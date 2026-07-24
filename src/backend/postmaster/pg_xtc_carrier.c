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
#include "postmaster/autovacuum.h"	/* autovacuum_max_workers */
#include "storage/latch.h"
#include "libpq/pqsignal.h"	/* BlockSig */
#include "postmaster/pg_xtc_carrier.h"
#include "utils/backend_runtime.h"	/* PgCurrentWorkSnapshot, save/restore-lazy */

/* xtc public API */
#include "xtc.h"
#include "xtc_app.h"
#include "xtc_exec.h"		/* xtc_exec_loop, xtc_exec_n_loops, xtc_exec_set_eager_rebalance */
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
/*
 * Upper bound on the worker/aux fiber executor loop pool when the pooled
 * protocol scheduler owns client backends.  The executor then runs only
 * long-lived worker fibers, so a small fixed pool suffices; this caps the pool
 * even if autovacuum_max_workers + max_worker_processes are set very high.
 */
#define XTC_PG_MAX_WORKER_FIBER_LOOPS 16
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
 * The pool is FIXED at startup and sized to roughly one loop per core, but
 * bounded so a very-large-core box does not create hundreds of carrier OS
 * threads.  Two callers create OS-thread pools under multithreaded=on: this
 * fiber executor (worker/aux fibers, and thread-per-session client backends
 * when pooled_protocol_carriers=0) and, independently, the pooled-protocol
 * carrier pool (backend_pooled_protocol_*).  Both must share the SAME budget,
 * or on a 384-vCPU box the two uncapped pools stack (e.g. 192 pooled carriers
 * PLUS 384 fiber-executor loops = 576+ OS threads) and the kernel's CFS
 * load-balancer (update_sg_lb_stats) dominates the profile -- measured as the
 * top CPU consumer, 10-11%, with the box 78-89%% idle, on the 2026-07-23 EC2
 * A/B.  So:
 *   - PG_XTC_CARRIER_LOOPS overrides everything (tuning / tests).
 *   - Else, when the pooled-protocol scheduler is active, the pooled carrier
 *     pool owns client-backend parallelism and this executor runs only a
 *     handful of long-lived worker fibers, so size it small and fixed to the
 *     worker concurrency (NOT the core count).
 *   - Else (thread-per-session, pooled_protocol_carriers==0), this executor IS
 *     the backend parallelism, so size to the core count, bounded by
 *     POOLED_PROTOCOL_CARRIER_AUTO_CEILING so the fiber pool obeys the same
 *     ceiling as the pooled pool.
 *
 * NECESSARY BUT NOT THE WHOLE CURE: this removes the fiber executor's
 * over-sizing (its 384->~15 contribution on the benchmark box).  The A/B
 * measured 4634 OS threads, which is NOT fully accounted for by any single
 * identifiable source (fiber executor n_loops+1, libxtc blocking pool capped
 * at 64, singleton lock-detector/preempt/slab-PSI helpers, pooled carriers
 * <= ceiling).  A follow-up must locate the remaining thread multiplier with a
 * thread-name histogram; do not treat this alone as the full thread-explosion
 * fix.
 *
 * ponytail: with many concurrent CPU-bound parallel workers (workers >> loops)
 * they serialize per loop -- a latency ceiling, not a deadlock (fibers are
 * cooperative).  Upgrade path if a workload proves it matters: elastic loop
 * growth for the worker-fiber pool.
 */
static int
xtc_carrier_loop_count(void)
{
	const char *env = getenv("PG_XTC_CARRIER_LOOPS");
	long		ncpus;
	int			loops;

	if (env != NULL && env[0] != '\0')
	{
		long		v = strtol(env, NULL, 10);

		if (v >= 1 && v <= 1024)
			return (int) v;
	}

	/*
	 * When the pooled-protocol scheduler is active, client backends run on the
	 * pooled carrier pool (backend_pooled_protocol_*), NOT on this fiber
	 * executor.  The executor then runs only a small, fixed set of long-lived
	 * worker/aux fibers (autovac launcher + workers, WAL writer, WAL
	 * summarizer, logical launcher, a few bgworkers -- on the order of ten,
	 * bounded by autovacuum_max_workers + max_worker_processes).  Sizing this
	 * pool to the core count would spin up hundreds of OS threads to run a
	 * dozen fibers -- exactly the thread explosion the 2026-07-23 A/B caught
	 * (fiber executor 384 loops stacked on 192 pooled carriers -> CFS
	 * load-balancer became the top CPU consumer).  Size it to the worker
	 * concurrency instead, floored small for parallelism / lost-wakeup safety
	 * and never larger than the carrier budget.
	 */
	if (PgRuntimePooledProtocolRequested())
	{
		int			carriers = PgRuntimePooledProtocolCarrierLimit();
		int			workers = 4;	/* floor: parallel workers, avoid single-loop starvation */

		if (autovacuum_max_workers > 0)
			workers += autovacuum_max_workers;
		if (max_worker_processes > 0)
			workers += max_worker_processes;
		if (workers > XTC_PG_MAX_WORKER_FIBER_LOOPS)
			workers = XTC_PG_MAX_WORKER_FIBER_LOOPS;
		if (carriers >= 1 && workers > carriers)
			workers = carriers;
		if (workers < 1)
			workers = 1;
		return workers;
	}

	/*
	 * Thread-per-session (pooled_protocol_carriers==0): this executor IS the
	 * client-backend parallelism, so size it to the cores, bounded by the same
	 * ceiling the pooled pool obeys so a very-large-core box cannot create an
	 * unbounded thread pool here either.
	 */
	ncpus = sysconf(_SC_NPROCESSORS_ONLN);
	if (ncpus < 1)
		ncpus = 1;
	loops = (int) ncpus;
	if (loops > POOLED_PROTOCOL_CARRIER_AUTO_CEILING)
		loops = POOLED_PROTOCOL_CARRIER_AUTO_CEILING;
	return loops;
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

/*
 * Assert-only cross-check that a wait-boundary seam restored the current-work
 * bridge to THIS fiber's own roots after a park.
 *
 * The seams snapshot the six roots on the fiber's stack before parking and
 * restore them on resume (PgRuntimeSaveCurrentWork / RestoreCurrentWork).
 * Because the snapshot lives on the fiber's stack it rides with the fiber
 * across a work-stealing steal, so the restore repoints the resuming thread's
 * bridge to the fiber's own roots regardless of which carrier loop it resumed
 * on.  This check confirms that invariant the moment it could first break: it
 * obtains the fiber's own carrier via xtc_proc_userdata() (which does NOT go
 * through the bridge, so it stays correct after a steal) and asserts the
 * just-restored carrier root equals it.  The carrier is the fiber-owned
 * identity token; because a seam restores all six roots ATOMICALLY from one
 * stack snapshot (PgRuntimeRestoreCurrentWork), a matching carrier means the
 * whole bridge came from this fiber's own snapshot -- a partial repoint that
 * left a non-carrier root stale is not possible through the seam.  The seam
 * additionally checks, at save time, that the snapshot it is about to ride
 * across the park already belongs to this fiber (see the GUC-amutex seam and
 * xtc_pg_wait_fd), which catches a leak that happened BEFORE the park.  While
 * pinned they always match (no migration); once the unpin lands this is the
 * tripwire that catches a wrong root-repoint before it can corrupt data across
 * sessions.  Compiled only in assert builds; a no-op elsewhere so process and
 * release builds are byte-for-byte unchanged.
 */
void
xtc_pg_verify_current_work_is_self(void)
{
	PgCarrier  *self_carrier;

	if (!xtc_in_backend_fiber)
		return;					/* not a backend fiber (bare supervisor) */

	self_carrier = (PgCarrier *) xtc_proc_userdata();
	if (self_carrier == NULL)
		return;					/* userdata not set (early boot / test) */

	/*
	 * The restored carrier root must be this fiber's own carrier.  Any other
	 * value means a park resumed with the bridge still pointing at whichever
	 * fiber last ran on this carrier thread -- the exact cross-session data
	 * corruption the seams exist to prevent.  Because a seam restores all six
	 * roots atomically from ONE stack snapshot, a matching carrier means the
	 * whole bridge was repointed to this fiber's own snapshot; a partial
	 * repoint that left a session/execution root stale cannot occur through the
	 * seam.  The complementary save-time check (xtc_pg_verify_snapshot_is_self)
	 * catches a bridge that was ALREADY leaked before the park was entered.
	 */
	Assert(CurrentPgCarrier == self_carrier);
}

/*
 * Save-time twin of xtc_pg_verify_current_work_is_self: assert that a snapshot
 * a seam is about to save and ride across a park already belongs to THIS
 * fiber.  The restore-time check proves the bridge came back to the fiber's
 * own carrier; this proves the bridge was the fiber's own to begin with, so a
 * leak that happened on an EARLIER unseamed boundary (before this park) is
 * caught here rather than being silently re-saved and restored.  Same
 * migration-only relevance, same assert-only cost; a no-op outside assert
 * builds.
 */
void
xtc_pg_verify_snapshot_is_self(const PgCurrentWorkSnapshot *snap)
{
	PgCarrier  *self_carrier;

	if (!xtc_in_backend_fiber || snap == NULL)
		return;

	self_carrier = (PgCarrier *) xtc_proc_userdata();
	if (self_carrier == NULL)
		return;					/* userdata not set (early boot / test) */

	Assert(snap->carrier == self_carrier);
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
			PgCarrier  *bc;

			memcpy(&sp, msg, sizeof(sp));

			/* entry is invariant; store it once (no per-spawn wrapper). */
			g_xtc_backend_entry = sp.entry;
			po.name = "pg-backend";

			/*
			 * Phase D: spawn migratable (work-stealable) iff the postmaster
			 * marked this fiber's carrier so (client backend, ssl_sni off; see
			 * xtc_carrier_migratable).  A zeroed po.migratable keeps the fiber
			 * pinned -- the pre-Phase-D behavior -- for every other child and
			 * whenever the flag is off, so the flip is scoped to exactly the
			 * fibers the design clears.  The decision rides on the fiber-owned
			 * carrier, so this spawn-time read matches the runtime
			 * xtc_pg_backend_fiber_is_migratable() view (both read the same
			 * carrier.migratable).
			 */
			bc = xtc_pg_backend_thread_start_carrier(sp.entry_arg);
			if (bc != NULL && bc->migratable)
				po.migratable = 1;

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
							  "xtc: spawned backend fiber pid=(loop=%u,local=%u,gen=%u) migratable=%d\n",
							  bpid.loop_id, bpid.local_id, bpid.gen, po.migratable);
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

	/*
	 * Publish this fiber's own PgCarrier root as the proc's userdata
	 * (libxtc v1.26.0).  The carrier is fiber-owned (it lives in the
	 * per-backend BackendThreadStart that is this fiber's entry arg), and
	 * xtc_proc userdata rides with the proc across a work-stealing steal, so
	 * xtc_proc_userdata() gives an O(1), migration-safe way to find THIS
	 * fiber's roots from any context -- crucially, without consulting the
	 * thread-local current-work bridge, which reflects whichever fiber last
	 * ran on the resuming carrier thread and may not be this one after a
	 * steal.  It backs xtc_pg_backend_fiber_is_migratable() (a correct
	 * per-fiber query even mid-migration) and the assert-only seam cross-check
	 * that the wait-boundary restore repointed the bridge to this fiber's own
	 * roots.  Neutral while pinned (nothing migrates, so the bridge already
	 * matches); load-bearing once the gated unpin flips migratable to 1.
	 */
	(void) xtc_proc_set_userdata(xtc_pg_backend_thread_start_carrier(entry_arg));

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

		/*
		 * Eager work-stealing rebalance (libxtc v1.27.0 df86fb8), threaded
		 * multi-loop carrier ONLY.  It makes migratable backend fibers actually
		 * get stolen: a loop whose run queue drains but that still owns
		 * fd-parked fibers steals a peer's runnable migratable proc before
		 * blocking in its own poller, and enqueuing migratable work nudges one
		 * idle peer's poller so it re-checks and steals promptly.  Opt-in
		 * (default off = ABI/behavior-neutral); we turn it on here, AFTER the
		 * exec exists and BEFORE any backend fiber spawns.  Process mode has no
		 * exec (g_xtc_exec == NULL) and single-loop mode has no peer to steal
		 * from (g_xtc_n_loops == 1), so both are left untouched.
		 */
		if (g_xtc_exec != NULL && g_xtc_n_loops > 1)
			xtc_exec_set_eager_rebalance(g_xtc_exec, 1);

		/*
		 * Idle-poll steal backoff (libxtc v1.31.0), threaded multi-loop carrier
		 * ONLY.  Complements eager-rebalance: when a worker's run queue is empty
		 * AND no peer has runnable work to steal, it grows its idle poll timeout
		 * (1ms -> 32ms across an idle streak) instead of re-scanning peers every
		 * millisecond; any real work resets it.  This targets the residual the
		 * 2026-07-24 metal A/B showed on idle-heavy tiny-query load (pgbench
		 * select@384: box ~97%% idle, update_sg_lb_stats/newidle balancer
		 * dominant from parked carriers repeatedly waking).  Opt-in (default off
		 * = neutral); observable via pg_stat_xtc_carriers.steal_backoff.  Same
		 * gating as eager-rebalance: no-op in process/single-loop mode.
		 */
		if (g_xtc_exec != NULL && g_xtc_n_loops > 1)
			xtc_exec_set_steal_backoff(g_xtc_exec, 1);

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

		elog(LOG, "xtc: carrier scheduler thread up (%d loop%s, %d supervisor%s, %s)",
			 g_xtc_n_loops, g_xtc_n_loops == 1 ? "" : "s",
			 g_xtc_n_sups, g_xtc_n_sups == 1 ? "" : "s",
			 PgRuntimePooledProtocolRequested()
			 ? "worker-fiber pool; client backends on pooled carriers"
			 : "thread-per-session backend pool");
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
 * cannot starve a sibling on another.  (The test-only PG_XTC_FORCE_LOOP hook
 * below can pin migratable backends to one loop to exercise work-stealing; it
 * is dormant while migration is disabled -- see xtc_carrier_migratable.)
 */
int
xtc_pg_launch_backend_fiber(xtc_carrier_entry_fn entry, void *entry_arg)
{
	int			loop_idx = 0;

	if (xtc_pg_carrier_start() != 0)
		return EAGAIN;

	/* Pick a loop round-robin across the pool; loop 0 in single-loop mode. */
	if (g_xtc_exec != NULL && g_xtc_n_loops > 1)
	{
		/*
		 * Test-only forced placement (DORMANT while migration is disabled).
		 * PG_XTC_FORCE_LOOP=N pins the MIGRATABLE client-backend fibers to loop
		 * N instead of round-robin, mirroring libxtc's own work-steal proof
		 * (test/m5/test_steal.c piles all tasks on loop 0 so idle peer loops
		 * must steal).  It only fires for migratable fibers, so while
		 * xtc_carrier_migratable() returns false (Phase D HOLD -- see
		 * launch_backend.c) NOTHING is forced and every fiber round-robins as
		 * before.  It is the single point a future steal-under-load test flips
		 * on once migration is re-enabled AND libxtc's Phase E wake-nudge lands
		 * (without the nudge an idle peer blocked in xtc_io_poll(-1) is not woken
		 * to steal, so forcing placement alone cannot force a local steal).
		 * Never set in production; ignored unless it names a valid loop.
		 */
		const char *force = getenv("PG_XTC_FORCE_LOOP");
		PgCarrier  *fiber_carrier = xtc_pg_backend_thread_start_carrier(entry_arg);
		bool		force_this = (force != NULL && force[0] != '\0' &&
								 fiber_carrier != NULL && fiber_carrier->migratable);

		if (force_this)
		{
			long		v = strtol(force, NULL, 10);

			if (v >= 0 && v < g_xtc_n_loops)
				loop_idx = (int) v;
			else
				loop_idx = (int) (atomic_fetch_add(&g_xtc_next_loop, 1) %
								  (unsigned) g_xtc_n_loops);
		}
		else
			loop_idx = (int) (atomic_fetch_add(&g_xtc_next_loop, 1) %
							  (unsigned) g_xtc_n_loops);
	}

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
		PgCarrier  *bc = xtc_pg_backend_thread_start_carrier(entry_arg);

		g_xtc_backend_entry = entry;	/* invariant; see xtc_carrier_proc */
		po.name = "pg-backend";
		/* Phase D: honor the same migratability decision as the monitored path. */
		if (bc != NULL && bc->migratable)
			po.migratable = 1;
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
	 * Phase B park-boundary tripwire (manual-seam twin of the removed
	 * fiber-ctx save hook's assert): this seam is only reached while
	 * xtc_in_backend_fiber, and both branches below park the fiber.  A
	 * bracketed thread-affine section (raw spinlock, OpenSSL error-queue span,
	 * sigprocmask window) holds per-OS-thread state that would be wrong if the
	 * fiber resumed on a different carrier, so it must never be open across a
	 * park.  The audit (MULTITHREADED_FIBER_WORKER_DESIGN.md section 4)
	 * established every affine span is yield-free, so the depth is always zero
	 * here -- for a PINNED and a MIGRATABLE fiber alike.  Assert it
	 * unconditionally: a park while an affine span is open is a bug whether or
	 * not the fiber can migrate (while pinned the resume is on the same thread
	 * so it would not corrupt, but it still signals a broken invariant; once
	 * migratable it is the exact cross-carrier corruption we must catch).  This
	 * seam runs unconditionally (unlike the removed global save hook, which a
	 * concurrent spawn could transiently revert), so it is the standing
	 * belt-and-suspenders check.
	 */
	Assert(xtc_pg_affine_section_depth() == 0);

	if (fd < 0)
	{
		/* No fd: honor a finite timeout by sleeping the fiber. */
		if (timeout_ms >= 0)
		{
			PgCurrentWorkSnapshot sleep_snap;

			PgRuntimeSaveCurrentWork(&sleep_snap);
			xtc_proc_sleep(timeout_ns);
			PgRuntimeRestoreCurrentWork(&sleep_snap);
			XtcPgVerifyCurrentWorkIsSelf();
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
	 * work thread-locals.  Restore ours before touching any PG state.  The
	 * snapshot lives on this fiber's stack, so it rode with the fiber if the
	 * park resumed on a different carrier loop (a work-steal) -- the restore
	 * therefore repoints the bridge to this fiber's own roots regardless of
	 * which loop resumed it.
	 */
	PgRuntimeRestoreCurrentWork(&snap);
	XtcPgVerifyCurrentWorkIsSelf();

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
 * Whether the currently-running backend fiber may migrate (be work-stolen)
 * across carriers.
 *
 * Phase D: this is now a real per-fiber query.  The migratability decision is
 * made once on the postmaster thread (xtc_carrier_migratable: client backend,
 * ssl_sni off) and recorded on the fiber-owned carrier root; the spawn site
 * reads the same flag to set xtc_proc_opts_t.migratable, so the runtime answer
 * here and the actual libxtc pinning cannot disagree.
 *
 * Resolved via xtc_proc_userdata() (the fiber's own PgCarrier, published at
 * fiber entry), NOT via the thread-local current-work bridge: after a steal the
 * bridge may still reflect whichever fiber last ran on the resuming thread
 * until the next seam restore, but userdata always tracks THIS fiber, so the
 * answer is correct even mid-migration.  Only meaningful while
 * xtc_in_backend_fiber; false otherwise (bare supervisor, non-fiber threads,
 * process mode).
 */
bool
xtc_pg_backend_fiber_is_migratable(void)
{
	PgCarrier  *self_carrier;

	if (!xtc_in_backend_fiber)
		return false;

	self_carrier = (PgCarrier *) xtc_proc_userdata();
	return self_carrier != NULL && self_carrier->migratable;
}

/*
 * Diagnostic: total number of tasks work-stolen across all carrier loops since
 * startup (summed xtc_loop_stats_t.steals).  Used by the forced-migration
 * stress test to PROVE fibers actually migrated (a nonzero total means at
 * least one parked-then-woken migratable fiber was rebalanced onto an idle
 * loop).  Returns 0 in single-loop mode (no peer to steal from) and is a
 * lock-free snapshot of relaxed atomics -- exactness across a running executor
 * is not guaranteed, which is fine for a "did any steal happen" probe.
 */
uint64
xtc_pg_carrier_total_steals(void)
{
	uint64		total = 0;

	if (g_xtc_exec == NULL || g_xtc_n_loops <= 1)
		return 0;

	for (int i = 0; i < g_xtc_n_loops; i++)
	{
		xtc_loop_stats_t st;

		if (xtc_exec_loop_stats(g_xtc_exec, i, &st) == XTC_OK)
			total += st.steals;
	}
	return total;
}

/*
 * Fill a caller-provided array with a lock-free snapshot of per-loop libxtc
 * scheduler stats (tasks_run, steals) for the pg_stat_xtc_carriers view
 * (fusion roadmap F0b).  Writes at most max_loops entries and returns the
 * number written (== the live loop count, capped at max_loops).  Returns 0
 * when the multi-loop executor is not running (single-loop / process mode).
 * The values are relaxed-atomic snapshots -- exact per-loop consistency across
 * a running executor is not guaranteed, which is fine for a monitoring view.
 */
int
xtc_pg_carrier_loop_stats(XtcPgLoopStat *out, int max_loops)
{
	int			n;

	if (out == NULL || max_loops <= 0)
		return 0;
	if (g_xtc_exec == NULL || g_xtc_n_loops <= 1)
		return 0;

	n = g_xtc_n_loops;
	if (n > max_loops)
		n = max_loops;

	for (int i = 0; i < n; i++)
	{
		xtc_loop_stats_t st;

		out[i].loop_id = i;
		if (xtc_exec_loop_stats(g_xtc_exec, i, &st) == XTC_OK)
		{
			out[i].tasks_run = st.tasks_run;
			out[i].steals = st.steals;
		}
		else
		{
			out[i].tasks_run = 0;
			out[i].steals = 0;
		}
	}
	return n;
}

/*
 * Runtime-scalar snapshot for pg_stat_xtc_carriers: the executor loop count and
 * the eager-rebalance / steal-backoff knob state.  Returns false when the
 * multi-loop executor is not running (caller shows the view empty).
 */
bool
xtc_pg_carrier_runtime_info(XtcPgCarrierRuntimeInfo *out)
{
	if (out == NULL)
		return false;
	if (g_xtc_exec == NULL || g_xtc_n_loops <= 1)
		return false;

	out->n_loops = g_xtc_n_loops;
	out->eager_rebalance = xtc_exec_get_eager_rebalance(g_xtc_exec) ? true : false;
	out->steal_backoff = xtc_exec_get_steal_backoff(g_xtc_exec) ? true : false;
	return true;
}

#endif							/* USE_XTC_CARRIER */
