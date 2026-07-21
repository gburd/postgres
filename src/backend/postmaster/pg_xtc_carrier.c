/*-------------------------------------------------------------------------
 * pg_xtc_carrier.c -- run a threaded PostgreSQL client backend as an
 *	xtc_proc (a cooperatively scheduled fiber on an xtc_loop) instead of a
 *	raw pthread.  OUT-OF-TREE, throwaway, gated behind USE_XTC_CARRIER.
 *
 *	The multithreaded-postgres tree already runs each B_BACKEND on its own
 *	OS thread (backend_thread_entry in launch_backend.c), having solved the
 *	PG-globals wall that blocked the earlier fork-based xtc spike.  This
 *	module changes only the *carrier*: instead of pg_thread_create() making
 *	a pthread, the backend's entry function runs as a fiber on an xtc loop
 *	hosted on a dedicated pthread.  Because the fiber runs the tree's own
 *	backend_thread_entry() verbatim, all the thread-per-session init is
 *	reused, and MyClientSocket->sock (the fd the tree dup()'d once) is the
 *	SAME fd registered into FeBeWaitSet -- so the read fd and the wait fd
 *	never diverge (the exact bug that stalled the fork spike).
 *
 *	The runtime seam is in waiteventset.c: while xtc_in_backend_fiber is
 *	true, WaitEventSetWaitBlock() waits on set->epoll_fd via
 *	xtc_proc_wait_fd() (yielding the fiber to the xtc loop) instead of a
 *	blocking epoll_wait() that would monopolize the carrier thread.
 *
 *	SINGLE CARRIER, SINGLE-LOOP: one xtc loop on one scheduler thread runs
 *	all backend fibers.  Concurrency is cooperative -- a fiber only yields
 *	at a wait point.  This is enough to prove "select 1 through xtc"; a
 *	real pool (many carriers) is future work.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_XTC_CARRIER

#include <pthread.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>

#include "miscadmin.h"
#include <sys/resource.h>
#include "storage/latch.h"
#include "libpq/pqsignal.h"	/* BlockSig */
#include "postmaster/pg_xtc_carrier.h"
#include "utils/backend_runtime.h"	/* PgCurrentWorkSnapshot, save/restore-lazy */

/* xtc public API */
#include "xtc.h"
#include "xtc_app.h"
#include "xtc_exec.h"		/* xtc_exec_loop, xtc_exec_n_loops */
#include "xtc_proc.h"
#include "xtc_async.h"		/* xtc_set_stack_size */

/*
 * Best-effort diagnostic write to a raw fd (STDERR) on crash/down/teardown
 * paths where the result is genuinely ignorable but glibc marks write() with
 * warn_unused_result (a plain (void) cast does not suppress it).  Consume the
 * result so those diagnostic writes compile warning-clean.
 */
static inline void
xtc_diag_write(int fd, const void *buf, size_t n)
{
	ssize_t		w = write(fd, buf, n);

	(void) w;
}

/*
 * PostgreSQL backends need a large stack (pg_thread.c uses 8 MiB for its
 * pthreads; max_stack_depth defaults to ~2 MB).  The xtc default fiber stack
 * is only 64 KiB, which overflows immediately in parser/planner recursion.
 * Match the tree's pthread stack.  Tune here if a deeper path faults.
 */
#define XTC_PG_FIBER_STACK	((size_t) 8 * 1024 * 1024)

static xtc_app_t *g_xtc_app;
static xtc_loop_t *g_xtc_loop;	/* loop 0 (single-loop mode, or the sup loop) */
static xtc_exec_t *g_xtc_exec;	/* the N-loop executor when n_loops > 1, else NULL */
static int	g_xtc_n_loops = 1;
static _Atomic unsigned g_xtc_next_loop;	/* round-robin cursor */
static pthread_t g_xtc_thread;
static volatile bool g_xtc_ready;
static pthread_mutex_t g_xtc_start_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Per-loop supervisor fibers (AGENTS_XTC item #7 Stage 1).  Each loop hosts
 * one long-lived supervisor fiber that xtc_monitor()s the backend fibers on
 * that loop, so an ABNORMAL fiber death -- one that faulted before publishing
 * its PMChild exit -- is observed (a DOWN with non-zero reason) and logged
 * loudly instead of silently leaving an occupied PMChild slot.  NORMAL exits
 * (reason 0) are corroborative only: the postmaster still owns reaping via
 * process_pm_pooled_logical_exit().  The supervisor NEVER reaps and NEVER
 * respawns -- it is an observer.  Stage 1 is observability + loud logging;
 * wiring an abnormal DOWN into the postmaster crash policy (ExitPostmaster)
 * is Stage 1b.
 *
 * The supervisor must run ON its loop to call xtc_monitor()/xtc_recv(); the
 * postmaster spawn thread (off-loop) hands it each new backend pid via a
 * cross-thread xtc_send() register message.
 */
#define XTC_PG_MAX_SUP_LOOPS 1024
static xtc_pid_t g_xtc_sup_pid[XTC_PG_MAX_SUP_LOOPS];

/*
 * Set true (and the postmaster latch kicked) when the supervisor observes a
 * GENUINE abnormal fiber crash -- one that died before reaching its clean
 * exit.  The postmaster polls this in its main loop (Stage 1b escalation);
 * benign teardown faults (findings 2c) never set it.
 */
static _Atomic uint32_t g_xtc_genuine_crash;

/*
 * Postmaster's process latch, captured once at carrier start (on the postmaster
 * thread, where MyLatch is the postmaster's own latch).  The supervisor kicks
 * it after flagging a genuine crash so the postmaster escalates on its very
 * next wake instead of waiting out DetermineSleepTime() (which can be tens of
 * seconds when the server is otherwise idle).
 */
static Latch *_Atomic g_xtc_postmaster_latch;

/* Debug-only fault-injection entry counter (PG_XTC_INJECT_CRASH). */
static _Atomic unsigned g_xtc_inject_entry_count;
static int	g_xtc_n_sups = 0;

/*
 * Supervisor spawn request (AGENTS_XTC #7 Stage 1, race-free variant).
 * The postmaster spawn thread sends this to a loop's supervisor; the
 * supervisor -- running ON that loop -- does an atomic xtc_proc_spawn_monitor()
 * (libxtc v1.3.0), so the monitor is established before the child can run and
 * even an instant-exiting backend delivers a real-reason DOWN (never NOPROC).
 * This closes the spawn/register race where a fast-crashing fiber could die
 * before an out-of-band register arrived, escaping observation.  A leading
 * magic distinguishes it from a libxtc DOWN signal in the supervisor mailbox.
 */
#define XTC_SUP_SPAWN_MAGIC 0x58545343u		/* 'XTSC' */
typedef struct xtc_sup_spawn_msg
{
	uint32_t	magic;
	xtc_carrier_entry_fn entry;
	void	   *entry_arg;
} xtc_sup_spawn_msg;

/*
 * The backend fiber entry function.  It is invariant across the process
 * (always the tree's backend_thread_entry), so we store it once rather than
 * heap-allocating a per-spawn {entry, arg} wrapper: the fiber is spawned with
 * entry_arg (a stable, launch_backend.c-owned BackendThreadStart *) as its
 * xtc arg and reads this pointer for the function.  This keeps the carrier
 * layer allocation-free -- important because it runs on a bare xtc fiber
 * before any PostgreSQL memory context exists, where palloc is unusable and
 * libxtc exposes no public general-purpose malloc (only xtc_free for buffers
 * it hands back).
 */
static xtc_carrier_entry_fn g_xtc_backend_entry;

/*
 * Number of carrier loops (each on its own OS thread).  A pool of loops lets
 * backend fibers run in parallel and avoids the single-loop lost-wakeup where
 * two or more fibers parked on one loop could starve each other.
 *
 * The DEFAULT is the system core count (sysconf(_SC_NPROCESSORS_ONLN)) -- this
 * is deliberate, not a placeholder: a pool sized to the cores is how the xtc
 * carrier is meant to run, keeps the tests representative, and maximizes the
 * work that libxtc's scheduler (and DST) can see.  Override with
 * PG_XTC_CARRIER_LOOPS only for tuning or to force a specific size in a test.
 */
static int
xtc_carrier_loop_count(void)
{
	const char *env = getenv("PG_XTC_CARRIER_LOOPS");
	long		ncpus;

	if (env != NULL && env[0] != '\0')
	{
		long		v = strtol(env, NULL, 10);

		if (v >= 1 && v <= 1024)
			return (int) v;
	}

	ncpus = sysconf(_SC_NPROCESSORS_ONLN);
	if (ncpus < 1)
		ncpus = 1;
	if (ncpus > 1024)
		ncpus = 1024;
	return (int) ncpus;
}

/*
 * True (in the carrier/scheduler thread) while a backend fiber is running,
 * so WaitEventSetWaitBlock() yields the fiber via xtc_proc_wait_fd() instead
 * of blocking the carrier in epoll_wait().  It is thread-local: only the
 * scheduler thread sets it, other threads (postmaster, other carriers) keep
 * their own copy false.
 */
__thread bool xtc_in_backend_fiber = false;

/*
 * Phase B no-steal tripwire: per-fiber affine-section nesting depth.
 *
 * XtcPgNoStealEnter/Leave bracket a span that holds OS-thread-affine state and
 * that the audit proved contains no cooperative yield (OpenSSL error-queue
 * span, sigprocmask window; the raw-spinlock and static-scratch spans are
 * safe-by-construction -- a running fiber is never stolen, and those spans
 * never reach a park -- so they rely on the park-boundary assert transitively
 * rather than an explicit bracket at every hot call site).  The fiber park
 * choke points assert this depth is zero (unless a future unpin has made the
 * fiber migratable), so a yield newly introduced inside a bracketed span fires
 * the assertion instead of silently reading foreign per-thread state after a
 * steal.
 *
 * It is a plain thread-local counter (not per-proc): a backend fiber runs to
 * its next park entirely on one carrier thread, and Enter/Leave are strictly
 * nested within that run, so the thread-local depth is the fiber's depth at
 * every park.  A bare fiber (the supervisor) never brackets, so its depth
 * stays zero.  The whole counter + check compile only in assert builds
 * (USE_ASSERT_CHECKING), so release and process builds are byte-for-byte
 * unchanged.
 */
#ifdef USE_ASSERT_CHECKING
static __thread int xtc_pg_affine_depth = 0;

void
xtc_pg_affine_section_enter(void)
{
	xtc_pg_affine_depth++;
}

void
xtc_pg_affine_section_leave(void)
{
	Assert(xtc_pg_affine_depth > 0);
	xtc_pg_affine_depth--;
}

int
xtc_pg_affine_section_depth(void)
{
	return xtc_pg_affine_depth;
}

/*
 * Reset the affine-section depth to 0.  Called at backend-fiber ENTRY and in
 * the fiber's at-exit cleanup so a FATAL/longjmp that escapes a bracketed
 * affine section (e.g. ereport(FATAL) on lost protocol sync inside error
 * recovery) cannot leak a non-zero depth onto the carrier OS thread, where the
 * NEXT fiber reusing that thread would trip the park-boundary assert
 * spuriously.  The depth is per-OS-thread (__thread), not per-fiber, so it must
 * be re-zeroed at each fiber boundary.  Balanced enter/leave keeps it at 0
 * within a fiber; this makes the unwinding/terminating paths crash-safe too.
 */
void
xtc_pg_affine_section_reset(void)
{
	xtc_pg_affine_depth = 0;
}
#endif							/* USE_ASSERT_CHECKING */

/* Scheduler thread: run the xtc app loop forever. */
static void *
xtc_carrier_sched_thread(void *arg)
{
	xtc_app_t  *app = arg;

	g_xtc_ready = true;
	xtc_app_run(app);			/* blocks until the app is stopped */
	return NULL;
}

/* Forward decl: the backend fiber body, spawned by the loop supervisor. */
static void xtc_carrier_proc(void *arg);

/*
 * Per-loop supervisor fiber (AGENTS_XTC #7 Stage 1).  Runs forever on its
 * loop, servicing two message kinds:
 *   - a spawn request (xtc_sup_spawn_msg): xtc_proc_spawn_monitor() the backend
 *     fiber on THIS loop -- one atomic call that establishes the monitor before
 *     the new fiber can run (race-free, and NOPROC cannot occur);
 *   - a DOWN signal (decoded via xtc_down_decode_ex into a self-describing
 *     xtc_down_info_t): a monitored backend exited.  KIND_CLEAN/KIND_NOPROC are
 *     normal/benign (the postmaster already reaps; log quietly).  KIND_SIGNAL
 *     is a genuine contained fault -- make it LOUD via a raw write (elog is
 *     unsafe from this bare fiber with no PG error stack) and escalate.
 *     KIND_EXIT is a non-zero app exit the postmaster reaps under its own
 *     restart policy -- log, do not escalate.
 *
 * The supervisor never reaps a PMChild slot and never respawns a backend --
 * exactly-once reaping and crash policy remain the postmaster's.
 */
static void
xtc_carrier_supervisor_proc(void *arg)
{
	/* arg encodes this supervisor's loop index (see start_supervisors). */
	int			my_loop_idx = (int) (intptr_t) arg;
	xtc_loop_t *my_loop = (g_xtc_exec != NULL && g_xtc_n_loops > 1)
		? xtc_exec_loop(g_xtc_exec, my_loop_idx) : g_xtc_loop;

	/*
	 * Install libxtc's R1 per-fiber fault guard on THIS loop thread.  The
	 * guard registers the SIGSEGV/SIGBUS/SIGFPE/SIGILL handler on an alternate
	 * signal stack; without it a synchronous fault in a backend fiber cannot
	 * be contained (no handler to unwind the one fiber and deliver its DOWN),
	 * which is the whole point of #7 Stage 1b crash containment.  The
	 * supervisor runs one fiber per loop, on that loop's thread, so this
	 * installs the guard exactly once per loop thread (the call is idempotent).
	 */
	(void) xtc_fault_guard_install();

	for (;;)
	{
		void	   *msg = NULL;
		size_t		len = 0;
		xtc_down_info_t di;
		int			rc;

		rc = xtc_recv(&msg, &len, -1);	/* block until a message arrives */
		if (rc != XTC_OK || msg == NULL)
			continue;

		if (xtc_down_decode_ex(msg, len, &di) == XTC_OK)
		{
			xtc_pid_t	down_pid = di.pid;

			/*
			 * A monitored backend/worker fiber exited.  libxtc v1.3.0 gives a
			 * self-describing DOWN: di.kind says HOW it ended, with the signal
			 * number and the app exit code in SEPARATE fields, so there is no
			 * range heuristic and no dependence on our proc_exit << 8 encoding:
			 *   XTC_DOWN_KIND_CLEAN  -> returned, or xtc_exit_self(0); quiet.
			 *   XTC_DOWN_KIND_NOPROC -> the monitor raced a clean exit (target
			 *                           already reaped); benign, the postmaster
			 *                           owns reaping.  (Atomic spawn_monitor now
			 *                           makes this case essentially unreachable
			 *                           -- the monitor is in place before the
			 *                           child runs -- but we still classify it.)
			 *   XTC_DOWN_KIND_SIGNAL -> a GENUINE crash: di.signal is the R1
			 *                           contained-fault signal (e.g. 11 ==
			 *                           SIGSEGV).  Escalate.
			 *   XTC_DOWN_KIND_EXIT   -> a non-zero application exit: di.exit_code
			 *                           is the code the fiber passed to
			 *                           xtc_exit_self (our backends pass
			 *                           backend_thread_exitstatus(code), i.e. the
			 *                           << 8 wait-status form, so di.exit_code
			 *                           carries that verbatim).  A NORMAL exit the
			 *                           postmaster reaps under its own restart
			 *                           policy -- NOT a fault.  Do NOT escalate.
			 * A bare xtc_exit_self(1) is now KIND_EXIT (exit_code 1), distinct
			 * from a signal-1 fault (KIND_SIGNAL, signal 1) -- the overloading
			 * we reported against v1.2.1 is gone.
			 */
			if (di.kind == XTC_DOWN_KIND_CLEAN)
			{
				static __thread int nlog = 0;

				if (nlog < 8)
				{
					char		buf[160];
					int			n;

					nlog++;
					n = snprintf(buf, sizeof(buf),
								 "xtc: supervisor observed normal backend fiber DOWN "
								 "pid=(loop=%u,local=%u,gen=%u)\n",
								 down_pid.loop_id, down_pid.local_id, down_pid.gen);
					if (n > 0)
					{
						if (n > (int) sizeof(buf))
							n = (int) sizeof(buf);
						xtc_diag_write(STDERR_FILENO, buf, (size_t) n);
					}
				}
			}
			else if (di.kind == XTC_DOWN_KIND_NOPROC)
			{
				/* Monitor raced a clean exit; benign, do NOT escalate. */
				static __thread int nnp = 0;

				if (nnp < 8)
				{
					char		buf[160];
					int			n;

					nnp++;
					n = snprintf(buf, sizeof(buf),
								 "xtc: supervisor observed benign monitor-race DOWN "
								 "(NOPROC) pid=(loop=%u,local=%u,gen=%u)\n",
								 down_pid.loop_id, down_pid.local_id, down_pid.gen);
					if (n > 0)
					{
						if (n > (int) sizeof(buf))
							n = (int) sizeof(buf);
						xtc_diag_write(STDERR_FILENO, buf, (size_t) n);
					}
				}
			}
			else if (di.kind == XTC_DOWN_KIND_SIGNAL)
			{
				/* An R1 contained-fault signal -> genuine crash. */
				static __thread int nabn = 0;

				if (nabn < 32)
				{
					char		buf[192];
					int			n;

					nabn++;
					n = snprintf(buf, sizeof(buf),
								 "xtc: SUPERVISOR observed GENUINE-CRASH backend fiber DOWN "
								 "pid=(loop=%u,local=%u,gen=%u) signal=%d\n",
								 down_pid.loop_id, down_pid.local_id,
								 down_pid.gen, di.signal);
					if (n > 0)
					{
						if (n > (int) sizeof(buf))
							n = (int) sizeof(buf);
						xtc_diag_write(STDERR_FILENO, buf, (size_t) n);
					}
				}

				/*
				 * Stage 1b escalation: a genuine backend-fiber crash.  Flag it,
				 * then kick the postmaster latch so it consumes the flag and
				 * drives its crash policy (ExitPostmaster under multithreaded
				 * mode) immediately, rather than only on its next idle-timeout
				 * wakeup.
				 */
				atomic_store(&g_xtc_genuine_crash, 1);
				{
					Latch	   *pml = atomic_load(&g_xtc_postmaster_latch);

					if (pml != NULL)
						SetLatch(pml);
				}
			}
			else
			{
				/*
				 * XTC_DOWN_KIND_EXIT: a non-zero application exit.  The
				 * postmaster reaps it and applies its own worker restart
				 * policy; do NOT escalate the runtime.  di.exit_code carries
				 * the backend_thread_exitstatus(code) value verbatim (<< 8
				 * wait-status form), so recover the proc_exit code with >> 8.
				 */
				static __thread int nappx = 0;

				if (nappx < 8)
				{
					char		buf[176];
					int			n;

					nappx++;
					n = snprintf(buf, sizeof(buf),
								 "xtc: supervisor observed non-zero exit DOWN "
								 "pid=(loop=%u,local=%u,gen=%u) exit_code=%d (postmaster reaps)\n",
								 down_pid.loop_id, down_pid.local_id,
								 down_pid.gen, di.exit_code >> 8);
					if (n > 0)
					{
						if (n > (int) sizeof(buf))
							n = (int) sizeof(buf);
						xtc_diag_write(STDERR_FILENO, buf, (size_t) n);
					}
				}
			}
		}
		else if (len == sizeof(xtc_sup_spawn_msg) &&
				 ((const xtc_sup_spawn_msg *) msg)->magic == XTC_SUP_SPAWN_MAGIC)
		{
			xtc_sup_spawn_msg sp;
			xtc_proc_opts_t po = {0};
			xtc_pid_t	bpid = XTC_PID_NONE;
			uint64_t	ref = 0;

			memcpy(&sp, msg, sizeof(sp));

			/* entry is invariant; store it once (no per-spawn wrapper). */
			g_xtc_backend_entry = sp.entry;
			po.name = "pg-backend";

			/*
			 * Atomic spawn+monitor (libxtc v1.3.0): establish the monitor
			 * BEFORE the child is made runnable, so there is no window in which
			 * the backend fiber exists but is unmonitored.  Even a backend that
			 * runs and exits immediately delivers a real-reason DOWN, never
			 * XTC_DOWN_KIND_NOPROC -- the monitor-race case disappears by
			 * construction instead of being classified after the fact.  The
			 * supervisor runs as an xtc_proc (see xtc_carrier_start_supervisors),
			 * so it satisfies spawn_monitor's requirement that the caller be a
			 * process.  entry_arg is the stable launch_backend.c-owned
			 * BackendThreadStart *; no heap allocation on this bare fiber.
			 */
			if (xtc_proc_spawn_monitor(my_loop, xtc_carrier_proc, sp.entry_arg,
									   &po, &bpid, &ref) == XTC_OK)
			{
				char		sbuf[96];
				int			sn;

				/* raw write: elog is unsafe from this bare fiber */
				sn = snprintf(sbuf, sizeof(sbuf),
							  "xtc: spawned backend fiber pid=(loop=%u,local=%u,gen=%u)\n",
							  bpid.loop_id, bpid.local_id, bpid.gen);
				if (sn > 0)
				{
					if (sn > (int) sizeof(sbuf))
						sn = (int) sizeof(sbuf);
					xtc_diag_write(STDERR_FILENO, sbuf, (size_t) sn);
				}
			}
		}

		/*
		 * Free the recv envelope with the library allocator's public
		 * deallocator (libxtc v1.2.0).  xtc_recv buffers come from libxtc's
		 * own allocator, which is not necessarily libc malloc/free, so
		 * xtc_free() -- not plain free() -- is the documented, thread-safe
		 * way to release them.
		 */
		xtc_free(msg);
	}
}

/* Spawn one supervisor fiber per loop; record their pids.  Best-effort:
 * a loop whose supervisor fails to spawn simply has no DOWN observation. */
static void
xtc_carrier_start_supervisors(void)
{
	g_xtc_n_sups = (g_xtc_n_loops < XTC_PG_MAX_SUP_LOOPS)
		? g_xtc_n_loops : XTC_PG_MAX_SUP_LOOPS;

	for (int i = 0; i < g_xtc_n_sups; i++)
	{
		xtc_loop_t *loop = (g_xtc_exec != NULL && g_xtc_n_loops > 1)
			? xtc_exec_loop(g_xtc_exec, i) : g_xtc_loop;
		xtc_proc_opts_t po = {0};
		xtc_pid_t	pid = XTC_PID_NONE;

		po.name = "pg-xtc-supervisor";
		if (xtc_proc_spawn(loop, xtc_carrier_supervisor_proc,
						   (void *) (intptr_t) i, &po, &pid) == XTC_OK)
			g_xtc_sup_pid[i] = pid;
		else
			g_xtc_sup_pid[i] = XTC_PID_NONE;
	}
}

/*
 * ---------------------------------------------------------------------------
 * Crash-safe affine-section depth reset at backend-fiber exit (Phase B).
 *
 * The fiber-ctx hook that used to chain libxtc's __xtc_fiber_ctx_save/restore
 * to repoint PG's six current-work roots on a coroutine switch has been
 * removed: while backend fibers are PINNED the manual wait-boundary seams
 * (xtc_pg_wait_fd, method_xtc.c, fd.c) already repoint the roots on every
 * cooperative park -- the only park class a backend fiber has -- so the hook
 * was no-op-equivalent, and its dependency on the now-hidden
 * __xtc_fiber_ctx_* symbols blocked linking against libxtc v1.25.0+.  The
 * hook's only unique coverage was involuntary preemption switches, which
 * cannot need a root repoint while pinned (resume-on-same-thread, same roots)
 * and are the migration case handled at Phase D.
 *
 * What DID live inside the removed hook's at-exit callback and is still
 * needed is the crash-safe affine-depth reset (assert-only): a FATAL/longjmp
 * that escapes a bracketed affine section could leave the per-OS-thread depth
 * non-zero and trip the next fiber's park-boundary assert.  It is re-homed
 * here as a standalone xtc_proc_at_exit callback, registered at fiber entry,
 * so the crash-safe reset survives every exit path independently of the hook.
 * ---------------------------------------------------------------------------
 */

#ifdef USE_ASSERT_CHECKING
/*
 * xtc_proc_at_exit callback (assert builds only): re-zero the per-OS-thread
 * affine-section depth when a backend fiber exits by ANY path (clean return,
 * xtc_exit_self, or a contained fault's recovery unwind).  If the fiber
 * terminated out of a bracketed affine section (FATAL/longjmp), the depth
 * could be left non-zero and trip the NEXT fiber's park-boundary assert on
 * this carrier thread.  Runs LIFO on every proc exit, so it closes that
 * window deterministically.  Fiber ENTRY also resets the depth (see
 * xtc_carrier_proc); this covers the exit-by-unwind path that skips the
 * clean-exit code.
 */
static void
xtc_pg_affine_reset_at_exit(void *arg)
{
	(void) arg;
	xtc_pg_affine_section_reset();
}
#endif

/* Fiber entry: run the tree's backend_thread_entry (all real init). */
static void
xtc_carrier_proc(void *arg)
{
	/*
	 * arg is the stable BackendThreadStart * (launch_backend.c owns its
	 * lifetime); the entry function is invariant and stored in
	 * g_xtc_backend_entry.  No wrapper struct, no heap free -- this fiber
	 * runs before any PostgreSQL memory context exists.
	 */
	xtc_carrier_entry_fn entry = g_xtc_backend_entry;
	void	   *entry_arg = arg;

	/*
	 * Do NOT elog() here: the fiber has no PG error stack / ErrorContext yet
	 * (backend_thread_entry sets those up).  An early ereport would call
	 * errstart -> exit() and tear down the shared postmaster.
	 */

	xtc_in_backend_fiber = true;

#ifdef USE_ASSERT_CHECKING
	/*
	 * Start this fiber with a clean affine-section depth.  The depth is
	 * per-OS-thread; a previous fiber on this carrier thread that terminated
	 * out of a bracketed section (FATAL/longjmp) may have left it non-zero.
	 * Re-zero at entry so the park-boundary assert reflects THIS fiber only.
	 */
	xtc_pg_affine_section_reset();

	/*
	 * Re-zero the affine depth again on ANY exit of this fiber, including the
	 * fault/recovery unwind that siglongjmps past the clean-exit path.  A
	 * FATAL/longjmp escaping a bracketed affine section could otherwise leave
	 * the per-OS-thread depth non-zero and trip the next fiber's park-boundary
	 * assert on this carrier thread.  xtc_proc_at_exit callbacks run LIFO on
	 * every proc exit (clean, xtc_exit_self, or recovery).  Assert-only.
	 */
	(void) xtc_proc_at_exit(xtc_pg_affine_reset_at_exit, NULL);
#endif

	/*
	 * Debug-only fault injection (AGENTS_XTC #7 Stage 1b test hook).  If
	 * PG_XTC_INJECT_CRASH=N is set, the Nth backend fiber to enter (1-based,
	 * counted across the whole pool) faults HERE.  libxtc v1.2.1 auto-arms the
	 * default recovery frame at the fiber's first scheduling point, so we yield
	 * once (xtc_proc_sleep(0)) before faulting -- matching a real backend, which
	 * always yields (socket recv, I/O) long before it could crash.  R1
	 * containment then turns the SIGSEGV into a DOWN(reason=11) to the
	 * supervisor, which flags a GENUINE crash so the postmaster terminates the
	 * runtime.  Never set in production.
	 */
	{
		const char *inj = getenv("PG_XTC_INJECT_CRASH");

		if (inj != NULL && inj[0] != '\0')
		{
			long		target = strtol(inj, NULL, 10);
			unsigned	nth = atomic_fetch_add(&g_xtc_inject_entry_count, 1) + 1;

			if (target > 0 && (unsigned) target == nth)
			{
				volatile uintptr_t addr = 0x10;

				xtc_diag_write(STDERR_FILENO,
							 "xtc: INJECT_CRASH faulting this fiber before clean exit\n",
							 56);
				/* Yield once so libxtc's auto-armed recovery frame is active. */
				(void) xtc_proc_sleep(0);
				*(volatile int *) addr = 1;	/* SIGSEGV -> contained -> DOWN reason=11 */
			}
		}
	}

	/*
	 * backend_thread_entry() runs the tree's full thread-per-session init
	 * and then BackendMain, which does not return (pg_unreachable()).  Its
	 * client-socket waits route through waiteventset.c's xtc intercept.
	 */
	entry(entry_arg);

	/* If it ever returns, leave the fiber cleanly. */
	xtc_in_backend_fiber = false;
	xtc_exit_self(0);
}

/*
 * Start the xtc scheduler on a dedicated thread inside the postmaster.
 * Idempotent; called from the postmaster before the first backend launch.
 * Returns 0 on success, -1 on failure.
 */
int
xtc_pg_carrier_start(void)
{
	int			rc = 0;
	sigset_t	save_mask;
	bool		masked = false;

	/*
	 * Capture the postmaster's process latch on the first call (this runs on
	 * the postmaster thread via the launch path, so MyLatch is the
	 * postmaster's own latch).  The supervisor uses it to wake the postmaster
	 * on a genuine crash.
	 */
	if (atomic_load(&g_xtc_postmaster_latch) == NULL && MyLatch != NULL)
		atomic_store(&g_xtc_postmaster_latch, MyLatch);

	pthread_mutex_lock(&g_xtc_start_lock);
	if (g_xtc_app != NULL)
	{
		pthread_mutex_unlock(&g_xtc_start_lock);
		return 0;				/* already started */
	}

	{
		xtc_app_opts_t opts = XTC_APP_OPTS_DEFAULT;

		/*
		 * Block all signals across the entire xtc bringup.  As of libxtc v1.4.2+
		 * (verified v1.9.0), libxtc creates its scheduler/loop/worker threads via
		 * __os_pthread_create_masked (sigfillset around pthread_create in
		 * src/os/os_thread.c), so THOSE threads already start with all signals
		 * blocked regardless of the creating thread's mask.  But we still create
		 * the carrier scheduler thread (g_xtc_thread) below with a RAW
		 * pthread_create, which inherits this thread's mask; and the postmaster
		 * runs ServerLoop with signals UNBLOCKED.  Without this block, that one
		 * thread (and anything xtc_app_start() might spawn on this calling
		 * thread) could start unblocked and the kernel could deliver a
		 * process-directed signal (e.g. SIGCHLD when a forked child exits) to it
		 * where MyProcPid==0 (no backend adopted) -- tripping Assert(MyProcPid)
		 * in wrapper_handler.  Core-proven historically: SIGCHLD hit the
		 * scheduler thread mid-pthread_create of a loop worker (that specific
		 * cascade is now covered by libxtc's masked create; this block remains as
		 * belt-and-suspenders for our own pthread_create and is the reason
		 * process-directed control signals stay with the postmaster main thread).
		 * Backend fibers re-block via backend_thread_entry and route their own
		 * interrupts through latches, not OS signals.  Mirrors pg_thread_create()
		 * and fork_process(), which block signals around thread/process creation
		 * for exactly this reason.
		 */
		sigprocmask(SIG_SETMASK, &BlockSig, &save_mask);
		masked = true;

		/* Big fiber stacks: PG backends recurse deeply (see above). */
		xtc_set_stack_size(XTC_PG_FIBER_STACK);

		/*
		 * Threaded fault policy: fail-stop FAST.  A synchronous fault in a
		 * session running inline on a pooled-affine carrier thread (not a
		 * libxtc-managed coro) cannot be contained -- libxtc's fault guard
		 * restores SIG_DFL and re-raises, and the whole shared-address-space
		 * process must die (correct: one fiber's memory corruption is not
		 * isolable).  But the default SIGSEGV disposition core-dumps the entire
		 * multithreaded process (dozens of carrier threads x large fiber
		 * stacks), which took ~20-70s to write -- during which the crashing
		 * client's socket stays open and it hangs.  Drop RLIMIT_CORE to 0 so
		 * the re-raised fault terminates the process instantly and closes all
		 * client sockets at once (fail-stop with no client hang).  Set
		 * PG_XTC_ALLOW_CORE=1 to keep cores when actively debugging a crash.
		 */
		if (getenv("PG_XTC_ALLOW_CORE") == NULL)
		{
			struct rlimit rl;

			if (getrlimit(RLIMIT_CORE, &rl) == 0)
			{
				rl.rlim_cur = 0;
				(void) setrlimit(RLIMIT_CORE, &rl);
			}
		}

		g_xtc_n_loops = xtc_carrier_loop_count();

		opts.name = "pg-xtc-carrier";
		opts.n_loops = g_xtc_n_loops;
		if (xtc_app_create(&opts, &g_xtc_app) != XTC_OK)
		{
			elog(LOG, "xtc: xtc_app_create failed");
			rc = -1;
			goto out;
		}
		g_xtc_loop = xtc_app_loop(g_xtc_app);
		/* NULL in single-loop mode; the N-loop executor otherwise. */
		g_xtc_exec = xtc_app_exec(g_xtc_app);
		if (g_xtc_exec != NULL)
			g_xtc_n_loops = xtc_exec_n_loops(g_xtc_exec);
		else
			g_xtc_n_loops = 1;

		if (xtc_app_start(g_xtc_app, NULL, 0) != XTC_OK)
		{
			elog(LOG, "xtc: xtc_app_start failed");
			rc = -1;
			goto out;
		}

		if (pthread_create(&g_xtc_thread, NULL, xtc_carrier_sched_thread,
						   g_xtc_app) != 0)
		{
			elog(LOG, "xtc: pthread_create for carrier scheduler failed");
			rc = -1;
			goto out;
		}
		while (!g_xtc_ready)
			pg_usleep(1000);

		/*
		 * Spawn the per-loop supervisor fibers now that the loops are running
		 * (AGENTS_XTC #7 Stage 1: observe abnormal backend fiber death).
		 */
		xtc_carrier_start_supervisors();

		elog(LOG, "xtc: carrier scheduler thread up (%d loop%s, %d supervisor%s)",
			 g_xtc_n_loops, g_xtc_n_loops == 1 ? "" : "s",
			 g_xtc_n_sups, g_xtc_n_sups == 1 ? "" : "s");
	}

out:
	/* Restore the postmaster's own (unblocked) mask on this thread. */
	if (masked)
		sigprocmask(SIG_SETMASK, &save_mask, NULL);
	pthread_mutex_unlock(&g_xtc_start_lock);
	return rc;
}

/*
 * Launch a threaded backend as an xtc fiber.  `entry` is the tree's
 * backend_thread_entry; `entry_arg` is its BackendThreadStart *.  Returns 0
 * on success (the fiber is spawned on a carrier loop), non-zero errno-like
 * value on failure so the caller can fall back / report.
 *
 * With a loop pool the fiber is placed round-robin across the executor loops,
 * so concurrent backends run on distinct loops and a fiber parked on one loop
 * cannot starve a sibling on another.
 */
int
xtc_pg_launch_backend_fiber(xtc_carrier_entry_fn entry, void *entry_arg)
{
	int			loop_idx = 0;

	if (xtc_pg_carrier_start() != 0)
		return EAGAIN;

	/* Pick a loop round-robin across the pool; loop 0 in single-loop mode. */
	if (g_xtc_exec != NULL && g_xtc_n_loops > 1)
		loop_idx = (int) (atomic_fetch_add(&g_xtc_next_loop, 1) %
						  (unsigned) g_xtc_n_loops);

	/*
	 * Hand the spawn to that loop's supervisor, which does an atomic
	 * xtc_proc_spawn_monitor() on-loop (libxtc v1.3.0), so the new backend
	 * fiber is monitored BEFORE it can run.  This closes the spawn/register
	 * race: a fiber that crashes immediately can no longer die unobserved (and
	 * cannot land in the NOPROC case).  The send is cross-thread (postmaster
	 * spawn thread -> supervisor mailbox) but only carries the entry/arg; the
	 * actual atomic spawn+monitor happens inside the supervisor.
	 */
	if (loop_idx < g_xtc_n_sups)
	{
		xtc_sup_spawn_msg sp;

		sp.magic = XTC_SUP_SPAWN_MAGIC;
		sp.entry = entry;
		sp.entry_arg = entry_arg;
		if (xtc_send(g_xtc_sup_pid[loop_idx], &sp, sizeof(sp)) != XTC_OK)
			return EAGAIN;
		return 0;
	}

	/*
	 * No supervisor for this loop (should not happen once supervisors are up)
	 * -- fall back to a direct spawn without monitoring, so a backend still
	 * launches.  Observability is lost for this one fiber only.
	 */
	{
		xtc_proc_opts_t po = {0};
		xtc_pid_t	pid = XTC_PID_NONE;
		xtc_loop_t *loop = (g_xtc_exec != NULL && g_xtc_n_loops > 1)
			? xtc_exec_loop(g_xtc_exec, loop_idx) : g_xtc_loop;

		g_xtc_backend_entry = entry;	/* invariant; see xtc_carrier_proc */
		po.name = "pg-backend";
		if (xtc_proc_spawn(loop, xtc_carrier_proc, entry_arg, &po, &pid) != XTC_OK)
			return EAGAIN;
		elog(LOG, "xtc: spawned backend fiber (unmonitored) pid=(loop=%u,local=%u,gen=%u)",
			 pid.loop_id, pid.local_id, pid.gen);
		return 0;
	}
}

/*
 * Backend exit seam (see header).  Called from backend_thread_finish() at the
 * point a pthread carrier would call pg_thread_exit().  The backend's
 * proc_exit cleanup has already run (socket closed, logical backend
 * unpublished, scheduler carrier unregistered, retained memory reclaimed,
 * PMChild thread-exit published, thread_start released).  All that remains is
 * to leave the fiber -- xtc_exit_self returns control to the carrier loop,
 * which reclaims the proc/task slot so the next backend gets a fresh one.
 */
void
xtc_pg_backend_fiber_exit(int code)
{
	{
		char		buf[96];
		int			n;
		xtc_pid_t	self = xtc_self();

		n = snprintf(buf, sizeof(buf),
					 "xtc: backend fiber exiting pid=(loop=%u,local=%u,gen=%u) code=%d\n",
					 self.loop_id, self.local_id, self.gen, code);
		if (n > 0)
		{
			if (n > (int) sizeof(buf))
				n = (int) sizeof(buf);
			xtc_diag_write(STDERR_FILENO, buf, (size_t) n);
		}
	}

	xtc_in_backend_fiber = false;

	xtc_exit_self(code);

	/* xtc_exit_self does not return; if it somehow does, do not fall back
	 * into the cleaned-up backend stack. */
	abort();
}

/*
 * Runtime seam used by waiteventset.c.  Yield the current backend fiber
 * until `fd` is ready for `interest_pg` (PG WL_* bits) or timeout.  Returns
 * WL_* bits that fired.  timeout_ms < 0 means wait forever.  Only called
 * when xtc_in_backend_fiber is true.
 */
#include "storage/waiteventset.h"	/* WL_* */
#include "xtc_io.h"					/* XTC_IO_* */

int
xtc_pg_wait_fd(int fd, int interest_pg, long timeout_ms)
{
	uint32_t	interest = 0;
	uint32_t	revents = 0;
	int64_t		timeout_ns = (timeout_ms < 0) ? -1 : (int64_t) timeout_ms * 1000000;
	int			out = 0;
	int			rc;
	PgCurrentWorkSnapshot snap;

	/*
	 * Phase B park-boundary tripwire (manual-seam twin of the fiber-ctx save
	 * hook's assert): this seam is only reached while xtc_in_backend_fiber, and
	 * both branches below park the fiber.  A bracketed thread-affine section
	 * must not be open across the park (see xtc_pg_affine_section_enter).  This
	 * seam runs unconditionally (unlike the global save hook, which a
	 * concurrent spawn can transiently revert to proc.c's), so it is the
	 * belt-and-suspenders check.  Gated on migratability: hard while pinned
	 * (dead), advisory once unpinned.
	 */
	Assert(xtc_pg_backend_fiber_is_migratable() ||
		   xtc_pg_affine_section_depth() == 0);

	if (fd < 0)
	{
		/* No fd: honor a finite timeout by sleeping the fiber. */
		if (timeout_ms >= 0)
		{
			PgCurrentWorkSnapshot sleep_snap;

			PgRuntimeSaveCurrentWork(&sleep_snap);
			xtc_proc_sleep(timeout_ns);
			PgRuntimeRestoreCurrentWork(&sleep_snap);
			return WL_TIMEOUT;
		}
		return WL_LATCH_SET;	/* caller re-checks */
	}

	if (interest_pg & WL_SOCKET_READABLE)
		interest |= XTC_IO_READABLE;
	if (interest_pg & WL_SOCKET_WRITEABLE)
		interest |= XTC_IO_WRITABLE;
	interest |= XTC_IO_HUP | XTC_IO_ERR;

	PgRuntimeSaveCurrentWork(&snap);
	rc = xtc_proc_wait_fd(fd, interest, timeout_ns, &revents);

	/*
	 * xtc_proc_wait_fd parked this fiber and the loop may have run other
	 * backend fibers on this OS thread meanwhile, clobbering PG's current-
	 * work thread-locals.  Restore ours before touching any PG state.
	 */
	PgRuntimeRestoreCurrentWork(&snap);

	if (rc != XTC_OK && rc != XTC_E_AGAIN)
		return WL_LATCH_SET;	/* treat as a wakeup; caller re-checks */

	if (revents & XTC_WAIT_TIMEOUT)
		out |= WL_TIMEOUT;
	if (revents & XTC_WAIT_MAILBOX)
		out |= WL_LATCH_SET;
	if (revents & XTC_IO_READABLE)
		out |= WL_SOCKET_READABLE;
	if (revents & XTC_IO_WRITABLE)
		out |= WL_SOCKET_WRITEABLE;
	if (revents & (XTC_IO_HUP | XTC_IO_ERR))
		out |= WL_SOCKET_READABLE;	/* let PG read and see EOF/err */
	return out;
}

/*
 * #7 Stage 1b: consume the genuine-crash flag set by a supervisor fiber when
 * it observed a backend fiber that crashed before reaching its clean exit.
 * Returns true once per crash; the postmaster then drives its crash policy.
 * Benign xtc_exit_self teardown faults (post-clean-exit) never set the flag.
 */
bool
xtc_pg_consume_genuine_crash(void)
{
	return atomic_exchange(&g_xtc_genuine_crash, 0) != 0;
}

/*
 * Whether the currently-running backend fiber may migrate (be stolen) across
 * carriers.  Backend fibers are pinned in this build -- the gated unpin has
 * not landed -- so this is unconditionally false.  See the header comment for
 * why the predicate exists now (no-migrate invariants written as tripwires).
 *
 * When the unpin lands this becomes the real per-fiber "is this fiber
 * steal-eligible" query; until then keeping it a single false keeps every
 * invariant that consults it dead (never fires) while pinned.
 */
bool
xtc_pg_backend_fiber_is_migratable(void)
{
	return false;
}

#endif							/* USE_XTC_CARRIER */
