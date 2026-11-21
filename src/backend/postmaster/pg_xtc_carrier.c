/*-------------------------------------------------------------------------
 * pg_xtc_carrier.c -- the libxtc carrier: run every postmaster child of a
 *	multithreaded server as an xtc_proc (a cooperatively scheduled fiber) on
 *	a fixed-size xtc executor, one OS thread per loop.  Compiled only with
 *	USE_XTC_CARRIER.
 *
 *	Each fiber runs backend_thread_entry() from launch_backend.c, which does
 *	the per-backend initialization.  For a client backend, MyClientSocket->sock
 *	is the fd registered in FeBeWaitSet, so the read fd and the wait fd are
 *	the same.
 *
 *	The runtime seam is in waiteventset.c: while xtc_pg_in_backend_fiber() is
 *	true, WaitEventSetWaitBlock() waits on set->epoll_fd via
 *	xtc_proc_wait_fd() (yielding the fiber to the xtc loop) instead of a
 *	blocking epoll_wait() that would monopolize the carrier thread.
 *
 *	Concurrency is cooperative: a fiber yields only at a wait point.  Each
 *	loop has a supervisor fiber that spawns the fibers placed on it and
 *	classifies their exits; a genuine crash fail-stops the server.
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
#include "xtc_exec.h"		/* xtc_exec_loop, xtc_exec_n_loops, xtc_exec_set_eager_rebalance */
#include "xtc_loop.h"
#include "xtc_proc.h"
#include "xtc_orc.h"		/* xtc_sup_* (F4: optional per-loop OTP supervisor) */
#include "xtc_version.h"		/* XTC_VERSION_* (installed from libxtc 2.2) */
/*
 * This file uses 2.2 API: xtc_sup_opts_t.on_child_down, XTC_DOWN_KIND_KILLED,
 * xtc_reg_put / xtc_reg_visit (backend_runtime_backend.c), and the 2.2 names
 * for the API 2.2 renamed.  Meson also requires xtc >= 2.2.0.
 */
#if !defined(XTC_VERSION_MAJOR) || \
	(XTC_VERSION_MAJOR == 2 && XTC_VERSION_MINOR < 2) || XTC_VERSION_MAJOR < 2
#error "PostgreSQL requires libxtc 2.2 or later"
#endif
#include "xtc_runtime.h"		/* xtc_runtime_set_stack_size */
#include "xtc_stats.h"		/* xtc_counter_* (fusion F1 runtime counters); xtc_runtime_tuning_check (F0a) */
#include "xtc_log.h"		/* xtc_log_set_default (fusion F0a: libxtc diagnostics -> server log) */
#include "xtc_dump.h"		/* xtc_dump (fusion F0d: full runtime dump on threaded crash) */
#include "xtc_tail.h"		/* xtc_tail_enable (v1.41.1 diagnostic timeline; PG_XTC_TAIL) */

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
 * Fusion F0a: libxtc default-log sink -> the PostgreSQL server log.
 *
 * When xtc_log_to_server is on, we install this as libxtc's default log sink
 * (xtc_log_set_default) at carrier bringup, so libxtc's internal diagnostics
 * reach the PG log instead of libxtc's own stderr fd.  In v1.32.0 the reachable
 * producer is the host-tuning advisor (xtc_runtime_tuning_check, which xtc_app_start
 * also runs): CPU-governor / intel_pstate / THP / swappiness / io_uring probes
 * -- exactly the host mis-tuning signal that shows up as the CFS-load-balancer
 * tax in our benchmarks.  libxtc streams no other internal log today; the sink
 * is harmless (captures nothing) if that changes it captures those too.
 *
 * The sink may run on libxtc's log-consumer context (not a PG backend with a
 * valid ereport/error stack), so it must NOT call ereport.  It writes the
 * already-formatted line (buf includes the trailing newline) to STDERR with an
 * [xtc <LEVEL>] prefix -- the logging collector captures it.  A plain write()
 * is async-signal-safe and thread-safe (same discipline as xtc_diag_write).
 * Returns 0 (never signals sink failure back to libxtc; a dropped diagnostic
 * line is harmless).
 */
static int
xtc_pg_log_sink(void *user, xtc_log_level_t lvl, const char *buf, size_t len)
{
	const char *tag;

	(void) user;
	switch (lvl)
	{
		case XTC_LOG_TRACE:	tag = "[xtc TRACE] "; break;
		case XTC_LOG_DEBUG:	tag = "[xtc DEBUG] "; break;
		case XTC_LOG_INFO:	tag = "[xtc INFO] "; break;
		case XTC_LOG_WARN:	tag = "[xtc WARN] "; break;
		case XTC_LOG_ERROR:	tag = "[xtc ERROR] "; break;
		case XTC_LOG_FATAL:	tag = "[xtc FATAL] "; break;
		default:			tag = "[xtc] "; break;
	}
	xtc_diag_write(STDERR_FILENO, tag, strlen(tag));
	if (buf != NULL && len > 0)
		xtc_diag_write(STDERR_FILENO, buf, len);
	return 0;
}

/*
 * PostgreSQL backends need a large stack (max_stack_depth defaults to ~2 MB).
 * The xtc default fiber stack is only 64 KiB, which overflows immediately in
 * parser/planner recursion.  Use 8 MiB, the usual platform thread stack.  Tune
 * here if a deeper path faults.
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
 * Per-loop supervisor fibers.  Each loop hosts one long-lived supervisor fiber
 * that xtc_monitor()s the backend fibers on that loop, so an ABNORMAL fiber
 * death -- one that faulted before publishing its PMChild exit -- is observed
 * (a DOWN with non-zero reason) and logged loudly instead of silently leaving
 * an occupied PMChild slot.  NORMAL exits (reason 0) are corroborative only:
 * the postmaster still owns reaping via process_pm_fiber_exit().  The
 * supervisor NEVER reaps and NEVER respawns -- it is an observer.  Stage 1 is
 * observability + loud logging; wiring an abnormal DOWN into the postmaster
 * crash policy (ExitPostmaster) is Stage 1b.
 *
 * The supervisor must run ON its loop to call xtc_monitor()/xtc_recv(); the
 * postmaster spawn thread (off-loop) hands it each new backend pid via a
 * cross-thread xtc_send() register message.
 */
#define XTC_PG_MAX_SUP_LOOPS 1024
static xtc_pid_t g_xtc_sup_pid[XTC_PG_MAX_SUP_LOOPS];

/*
 * F4 supervision-fusion (optional, GUC-gated by xtc_carrier_use_orc): one
 * libxtc xtc_orc OTP supervisor per carrier loop, standing up ALONGSIDE the
 * hand-rolled g_xtc_sup_pid[] supervisor fibers -- it does NOT replace them.
 * The handles are parallel to g_xtc_sup_pid[] (index i is loop i).  NULL when
 * the GUC is off (the default) or the supervisor failed to start on that loop.
 * See xtc_carrier_start_orc_supervisors() for why backend fibers are NOT
 * registered as children of these supervisors (the escalation invariant).
 */
static xtc_supervisor_t *g_xtc_orc_sup[XTC_PG_MAX_SUP_LOOPS];
static int	g_xtc_n_orc_sups = 0;

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

static int	g_xtc_n_sups = 0;

/*
 * Supervisor spawn request (race-free variant).
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
 * Upper bound on the default carrier loop count, so a very-large-core box does
 * not create hundreds of carrier OS threads (and their stack reservations)
 * that the kernel's CFS load balancer then spends its time on.
 */
#define XTC_PG_CARRIER_LOOPS_CEILING 256

/*
 * Number of carrier loops (each on its own OS thread).  A pool of loops lets
 * backend fibers run in parallel and avoids the single-loop lost-wakeup where
 * two or more fibers parked on one loop could starve each other.
 *
 * The pool is FIXED at startup and sized to one loop per core, bounded by
 * XTC_PG_CARRIER_LOOPS_CEILING.  PG_XTC_CARRIER_LOOPS overrides the default
 * (tuning and tests).
 *
 * shortcut: with many concurrent CPU-bound parallel workers (workers >> loops)
 * they serialize per loop -- a latency ceiling, not a deadlock (fibers are
 * cooperative).  Upgrade path if a workload proves it matters: elastic loop
 * growth.
 */
int
xtc_pg_carrier_loop_count(void)
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

	ncpus = sysconf(_SC_NPROCESSORS_ONLN);
	if (ncpus < 1)
		ncpus = 1;
	loops = (int) ncpus;
	if (loops > XTC_PG_CARRIER_LOOPS_CEILING)
		loops = XTC_PG_CARRIER_LOOPS_CEILING;
	return loops;
}

/*
 * Only backend procs publish userdata, before PG startup; supervisors leave it
 * NULL.  libxtc carries it across sibling switches and migration.  The borrowed
 * carrier pointer is cleared before its BackendThreadStart owner is freed.
 */
bool
xtc_pg_in_backend_fiber(void)
{
	return xtc_proc_userdata() != NULL;
}

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

	if (snap == NULL)
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
 * Per-loop supervisor fiber.  Runs forever on its loop, servicing two message
 * kinds:
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
		xtc_down_info_t di = XTC_DOWN_INFO_INIT;
		bool		from_library;
		int			rc;

#ifdef HAVE_XTC_RECV_MATCH_EX
		xtc_msg_info_t info = XTC_MSG_INFO_INIT;

		/* block until a message arrives; NULL predicate takes the first */
		rc = xtc_recv_match_ex(0, NULL, NULL, &msg, &len, &info, -1);

		/*
		 * A DOWN's byte layout is public, so a payload that decodes as a DOWN
		 * proves nothing by itself; only the library can set XTC_MSG_SYS.
		 * Without this check any proc could make the supervisor report a live
		 * backend as crashed and fail-stop the server.
		 */
		from_library = (info.flags & XTC_MSG_SYS) != 0;
#else
		rc = xtc_recv(&msg, &len, -1);	/* block until a message arrives */
		from_library = true;
#endif
		if (rc != XTC_OK || msg == NULL)
			continue;

		if (from_library && xtc_down_decode_ex(msg, len, &di) == XTC_OK)
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
			 *                           SIGSEGV).  Escalate.  libxtc 2.2
			 *                           (8a7e9c06) GUARANTEES this kind for any
			 *                           contained fault regardless of the code
			 *                           the fiber then passed to xtc_exit_self:
			 *                           our backends arm a recovery frame and
			 *                           xtc_exit_self(sig) on a fault, which
			 *                           v2.1.x MISREPORTED as KIND_EXIT(sig) and
			 *                           silently skipped this escalation (libxtc
			 *                           reply 2026-11-12 section 1).  The signal
			 *                           replaces any passed code in the DOWN.
			 *   XTC_DOWN_KIND_EXIT   -> a non-zero application exit: di.exit_code
			 *                           is the code the fiber passed to
			 *                           xtc_exit_self (our backends pass
			 *                           backend_thread_exitstatus(code), i.e. the
			 *                           << 8 wait-status form, so di.exit_code
			 *                           carries that verbatim).  A NORMAL exit the
			 *                           postmaster reaps under its own restart
			 *                           policy -- NOT a fault.  Do NOT escalate.
			 *   XTC_DOWN_KIND_KILLED -> (libxtc 2.2, =5) the fiber was terminated
			 *                           on purpose (xtc_exit_pid, a supervisor
			 *                           stop, a link/group kill); di.exit_code is
			 *                           the KILL REASON, not a proc_exit code.
			 *                           Deliberate teardown -- NOT a fault.  Do
			 *                           NOT escalate.  (NOCONNECTION and any
			 *                           future kind share this terminal branch.)
			 * A bare xtc_exit_self(1) is KIND_EXIT (exit_code 1), distinct from a
			 * signal-1 fault (KIND_SIGNAL, signal 1).
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
								 "pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ")\n",
								 down_pid.loop_id, down_pid.local_id, (uint64) down_pid.gen);
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
								 "(NOPROC) pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ")\n",
								 down_pid.loop_id, down_pid.local_id, (uint64) down_pid.gen);
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
								 "pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ") signal=%d\n",
								 down_pid.loop_id, down_pid.local_id,
								 (uint64) down_pid.gen, di.signal);
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
				/*
				 * F0d (fusion observability): dump the full libxtc runtime
				 * state -- every scheduler loop with its run-queue/steal
				 * stats, all procs, the faulting thread's C backtrace -- to
				 * the server log BEFORE we escalate the whole process down.
				 * A threaded genuine crash fail-stops the process (no
				 * core-dump hang), so this dump is the primary post-mortem
				 * for what the scheduler was doing at the crash.  Runs only
				 * on the genuine-crash path (never a hot path), from the
				 * supervisor fiber in normal context where xtc_dump's
				 * per-loop inspection locks are reliable.  STDERR_FILENO is
				 * the postmaster's stderr == the server log.
				 */
				xtc_dump(STDERR_FILENO);

				atomic_store(&g_xtc_genuine_crash, 1);
				{
					Latch	   *pml = atomic_load(&g_xtc_postmaster_latch);

					if (pml != NULL)
						SetLatch(pml);
				}
			}
			else if (di.kind == XTC_DOWN_KIND_EXIT)
			{
				/*
				 * XTC_DOWN_KIND_EXIT: a non-zero application exit.  The
				 * postmaster reaps it and applies its own worker restart
				 * policy; do NOT escalate the runtime.  di.exit_code carries
				 * the backend_thread_exitstatus(code) value verbatim (<< 8
				 * wait-status form), so recover the proc_exit code with >> 8.
				 *
				 * NB (libxtc 2.2, 8a7e9c06): a CONTAINED FAULT is now always
				 * reported as XTC_DOWN_KIND_SIGNAL regardless of the code the
				 * fiber passed to xtc_exit_self -- so a backend that faults
				 * inside its recovery frame and then calls xtc_exit_self(sig)
				 * no longer lands HERE as EXIT(sig) (the v2.1.x
				 * misclassification that silently skipped the genuine-crash
				 * fail-stop; libxtc reply 2026-11-12 section 1).  This branch
				 * is now reached only by a genuine application exit.
				 */
				static __thread int nappx = 0;

				if (nappx < 8)
				{
					char		buf[176];
					int			n;

					nappx++;
					n = snprintf(buf, sizeof(buf),
								 "xtc: supervisor observed non-zero exit DOWN "
								 "pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ") exit_code=%d (postmaster reaps)\n",
								 down_pid.loop_id, down_pid.local_id,
								 (uint64) down_pid.gen, di.exit_code >> 8);
					if (n > 0)
					{
						if (n > (int) sizeof(buf))
							n = (int) sizeof(buf);
						xtc_diag_write(STDERR_FILENO, buf, (size_t) n);
					}
				}
			}
			else
			{
				/*
				 * XTC_DOWN_KIND_KILLED (libxtc 2.2: xtc_exit_pid / a
				 * supervisor stop / a link or group-restart kill),
				 * XTC_DOWN_KIND_NOCONNECTION (a cross-process channel died --
				 * not produced on the in-process backend-fiber path), or any
				 * future kind.  A KILLED backend fiber was terminated on
				 * purpose (we or the runtime asked it to stop), so it is NOT a
				 * genuine crash: the postmaster owns the teardown, do NOT
				 * escalate.  Handle it explicitly rather than folding it into
				 * the EXIT branch -- di.exit_code here is the KILL REASON, not
				 * a proc_exit code, so the >> 8 recovery above would misread
				 * it.  An unrecognised kind is logged and treated
				 * conservatively as a non-escalating teardown; if a future
				 * libxtc adds a kind that must fail-stop, it will be a SIGNAL.
				 */
				static __thread int nkill = 0;

				if (nkill < 8)
				{
					char		buf[192];
					int			n;

					nkill++;
					n = snprintf(buf, sizeof(buf),
								 "xtc: supervisor observed terminated backend fiber DOWN "
								 "pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ") kind=%d reason=%d (postmaster reaps)\n",
								 down_pid.loop_id, down_pid.local_id,
								 (uint64) down_pid.gen, (int) di.kind, di.exit_code);
					if (n > 0)
					{
						if (n > (int) sizeof(buf))
							n = (int) sizeof(buf);
						xtc_diag_write(STDERR_FILENO, buf, (size_t) n);
					}
				}
			}
		}
		else if (!from_library &&
				 len == sizeof(xtc_sup_spawn_msg) &&
				 ((const xtc_sup_spawn_msg *) msg)->magic == XTC_SUP_SPAWN_MAGIC)
		{
			xtc_sup_spawn_msg sp;
			xtc_proc_opts_t po = XTC_PROC_OPTS_INIT;
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
			if (xtc_carrier_use_orc && my_loop_idx < g_xtc_n_orc_sups &&
				g_xtc_orc_sup[my_loop_idx] != NULL)
			{
				/*
				 * Hand the backend to this loop's xtc_orc supervisor as a
				 * TEMPORARY child: it is never restarted, and its exit reaches
				 * xtc_carrier_orc_child_down, which is then the escalation path.
				 * This fiber is a proc and is not the supervisor, so
				 * xtc_sup_add_child may run here (sup.c refuses only off-proc
				 * callers and the supervisor itself).  An add that fails or
				 * times out leaves the backend unstarted; the postmaster sees the
				 * launch never register and fails the connection as it would
				 * for a failed spawn.
				 */
				xtc_child_spec_t spec = XTC_CHILD_SPEC_INIT;
				int			arc;

				spec.name = "pg-backend";
				spec.fn = xtc_carrier_proc;
				spec.arg = sp.entry_arg;
				spec.policy = XTC_RESTART_TEMPORARY;
				spec.migratable = po.migratable;
				spec.stack_size = XTC_PG_FIBER_STACK;
				arc = xtc_sup_add_child(g_xtc_orc_sup[my_loop_idx], &spec, &bpid);
				if (arc == XTC_OK)
				{
					char		sbuf[128];
					int			sn;

					sn = snprintf(sbuf, sizeof(sbuf),
								  "xtc: orc added backend fiber pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ") migratable=%d\n",
								  bpid.loop_id, bpid.local_id, (uint64) bpid.gen, po.migratable);
					if (sn > 0)
						xtc_diag_write(STDERR_FILENO, sbuf,
									   Min((size_t) sn, sizeof(sbuf)));
				}
				else
				{
					char		sbuf[96];
					int			sn;

					sn = snprintf(sbuf, sizeof(sbuf),
								  "xtc: orc xtc_sup_add_child failed rc=%d on loop %d\n",
								  arc, my_loop_idx);
					if (sn > 0)
						xtc_diag_write(STDERR_FILENO, sbuf,
									   Min((size_t) sn, sizeof(sbuf)));
				}
			}
			else if (xtc_proc_spawn_monitor(my_loop, xtc_carrier_proc, sp.entry_arg,
											&po, &bpid, &ref) == XTC_OK)
			{
				char		sbuf[128];
				int			sn;

				/* raw write: elog is unsafe from this bare fiber */
				sn = snprintf(sbuf, sizeof(sbuf),
							  "xtc: spawned backend fiber pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ") migratable=%d\n",
							  bpid.loop_id, bpid.local_id, (uint64) bpid.gen, po.migratable);
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
		xtc_proc_opts_t po = XTC_PROC_OPTS_INIT;
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
 * F4 supervision-fusion: stand up one libxtc xtc_orc OTP supervisor per carrier
 * loop (GUC xtc_carrier_use_orc; default off).  Best-effort, exactly like
 * xtc_carrier_start_supervisors(): a loop whose supervisor fails to start just
 * gets a NULL handle.
 *
 * This runs ALONGSIDE the hand-rolled per-loop supervisor fibers
 * (xtc_carrier_supervisor_proc); it does NOT replace them.  The hand-rolled
 * supervisor + the XTC_SUP_SPAWN mailbox + the manual XTC_DOWN_KIND_* classifier
 * remain the UNCHANGED source of truth for backend-fiber crash detection and
 * Stage-1b escalation (SIGNAL DOWN -> g_xtc_genuine_crash + postmaster latch).
 *
 * WHY BACKEND FIBERS WERE NOT xtc_sup CHILDREN -- the durable design-doc
 * correction.  The original supervision-fusion design called for routing
 * backend fibers through xtc_sup_add_child and mapping the
 * SIGNAL-DOWN escalation "into the supervisor's DOWN handling".  Against the
 * libxtc pinned through 2026-11-04 that was NOT implementable and would have
 * SILENTLY LOST the escalation:
 *
 *   - xtc_orc's public API then exposed only AGGREGATE counters
 *     (xtc_sup_n_alive / n_restarts); it had NO per-child DOWN callback and no
 *     DOWN-forwarding hook.  The supervisor decoded each child's DOWN inside
 *     its own private xtc_recv loop purely to drive the restart decision and
 *     then DISCARDED the classification -- an external caller could not learn
 *     whether a specific child crashed (SIGNAL) or exited cleanly.
 *   - A caller-side follow-up xtc_monitor() on the pid xtc_sup_add_child
 *     returns is NOT a substitute: it races the supervisor's own monitor and
 *     was measured 1000/1000 LOST (XTC_DOWN_KIND_NOPROC) on a same-loop
 *     crashing child.
 *
 * Both facts were verified against libxtc v1.44.0 and reported upstream.
 * Because our crash policy is "a crashed backend may have corrupted the
 * shared address space, so FAIL-STOP the whole process", the
 * SIGNAL-vs-clean distinction is mandatory; until libxtc could report it per
 * child it had to stay on a monitor we own -- our own
 * xtc_proc_spawn_monitor() in xtc_carrier_supervisor_proc, which establishes
 * the monitor atomically before the child runs.
 *
 * THE PER-CHILD-DOWN GAP IS NOW CLOSED (libxtc 2.2, 8a7e9c06).  The audit
 * requested exactly this, and libxtc delivered it as
 * xtc_sup_opts_t.on_child_down (one hook per supervisor, NOT per child spec),
 * fired on the supervisor's own fiber BEFORE the restart decision, observe-only
 * and non-parking, with the child's spec (so spec->arg / spec->name identify
 * the child across restarts).  We install that hook here (xtc_carrier_orc_child_down)
 * so an xtc_sup child that goes DOWN with XTC_DOWN_KIND_SIGNAL drives the SAME
 * Stage-1b escalation (g_xtc_genuine_crash + postmaster latch) the hand-rolled
 * classifier does.  The mechanism is therefore wired and testable the moment a
 * child is added.
 *
 * These supervisors still own ZERO children today, deliberately.  Routing the
 * real backend spawn (xtc_pg_launch_backend_fiber) through xtc_sup_add_child is
 * a larger behavioural cutover -- it moves the spawn+monitor+DOWN plumbing off
 * the hand-rolled XTC_SUP_SPAWN mailbox onto the OTP supervisor -- and needs its
 * own A/B on check-threaded (neutral-or-better, no crash-containment
 * regression) before it can replace the escalation source of truth.  That is
 * the remaining step (F4 design step 2), intentionally NOT taken in this commit.
 * Until a child is added the hook never fires and changes no behaviour -- which
 * is exactly why it is safe to wire behind a default-off developer GUC.
 */

/*
 * on_child_down hook for the GUC-gated xtc_orc supervisors (libxtc 2.2).
 *
 * Runs on the supervisor's own fiber, once per DOWN of a child the supervisor
 * owns, BEFORE the supervisor decides whether to restart it.  It mirrors the
 * SIGNAL branch of the hand-rolled classifier (xtc_carrier_supervisor_proc):
 * an XTC_DOWN_KIND_SIGNAL is an R1 contained fault -- a genuine crash that may
 * have torn the shared address space -- so flag g_xtc_genuine_crash and kick
 * the postmaster latch, the identical Stage-1b escalation.  CLEAN / EXIT are a
 * normal end and KILLED is a deliberate teardown; none escalate (the postmaster
 * owns reaping), matching the hand-rolled classifier's non-escalating branches.
 *
 * The hook MUST NOT park (libxtc's contract): only an atomic store,
 * xtc_proc_wake, SetLatch or xtc_sup_stop are allowed -- no xtc_recv, sleep,
 * blocking I/O or xtc_amutex.  atomic_store + SetLatch satisfy that exactly.
 * Unlike the hand-rolled SIGNAL branch we do NOT call xtc_dump() here: xtc_dump
 * takes libxtc's per-loop inspection locks and is not a non-parking operation,
 * so it stays on the hand-rolled path (which runs in a plain xtc_recv loop, not
 * under the supervisor's restart-decision fiber).  The escalation itself --
 * the only load-bearing effect -- is byte-identical: atomic_store(&..., 1) then
 * SetLatch(postmaster).  spec->arg will carry the PgBackend/carrier identity
 * once backends are added as children; today it is unused (zero children).
 */
static void
xtc_carrier_orc_child_down(xtc_supervisor_t *sup,
						   const xtc_child_spec_t *spec,
						   const xtc_down_info_t *info, void *user)
{
	(void) sup;
	(void) spec;
	(void) user;

	if (info == NULL)
		return;

	if (info->kind == XTC_DOWN_KIND_SIGNAL)
	{
		/*
		 * A genuine contained-fault crash.  Escalate exactly as the
		 * hand-rolled classifier does: set the flag the postmaster polls, then
		 * kick its latch so it drives the crash policy immediately rather than
		 * waiting out DetermineSleepTime().  Store 1 (not info->signal) to keep
		 * g_xtc_genuine_crash's value byte-identical to the hand-rolled path;
		 * the consumer (xtc_pg_consume_genuine_crash) only tests != 0.  Both
		 * atomic_store and SetLatch are non-parking, honouring the hook
		 * contract.
		 */
		Latch	   *pml;
		char		buf[160];
		int			n;

		/* raw write: elog is unsafe from the supervisor fiber */
		n = snprintf(buf, sizeof(buf),
					 "xtc: ORC observed GENUINE-CRASH backend fiber DOWN pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ") signal=%d\n",
					 info->pid.loop_id, info->pid.local_id,
					 (uint64) info->pid.gen, info->signal);
		if (n > 0)
			xtc_diag_write(STDERR_FILENO, buf, Min((size_t) n, sizeof(buf)));

		atomic_store(&g_xtc_genuine_crash, 1);
		pml = atomic_load(&g_xtc_postmaster_latch);
		if (pml != NULL)
			SetLatch(pml);
	}
	/* CLEAN / EXIT: a normal end.  KILLED / NOCONNECTION: deliberate teardown.
	 * None escalate -- the postmaster owns reaping (matches the hand-rolled
	 * classifier's non-SIGNAL branches). */
}

static void
xtc_carrier_start_orc_supervisors(void)
{
	g_xtc_n_orc_sups = (g_xtc_n_loops < XTC_PG_MAX_SUP_LOOPS)
		? g_xtc_n_loops : XTC_PG_MAX_SUP_LOOPS;

	for (int i = 0; i < g_xtc_n_orc_sups; i++)
	{
		xtc_loop_t *loop = (g_xtc_exec != NULL && g_xtc_n_loops > 1)
			? xtc_exec_loop(g_xtc_exec, i) : g_xtc_loop;
		xtc_sup_opts_t opts = XTC_SUP_OPTS_INIT;
		xtc_supervisor_t *sup = NULL;

		/*
		 * ONE_FOR_ONE with the default restart intensity (max_restarts=3 /
		 * period=5s).  No exec is handed in: each supervisor is single-loop and
		 * owns only children placed on its own loop, so this mirrors the
		 * one-supervisor-per-loop topology of the hand-rolled fibers rather than
		 * a single exec-spanning supervisor.  No initial children (see the long
		 * comment above), so the child-spec array is empty.
		 */
		opts.strategy = XTC_SUP_ONE_FOR_ONE;

		/*
		 * Install the libxtc 2.2 per-child DOWN hook.  It covers the static
		 * children, the children added via xtc_sup_add_child, and (if we ever
		 * give the supervisor one) its root -- one registration for all.  With
		 * zero children today it never fires; it is wired now so the mechanism
		 * is in place and testable the moment a child is added (F4 step 2).
		 */
		opts.on_child_down = xtc_carrier_orc_child_down;
		opts.on_child_down_user = NULL;
		if (xtc_sup_start(loop, &opts, NULL, 0, &sup) == XTC_OK)
			g_xtc_orc_sup[i] = sup;
		else
			g_xtc_orc_sup[i] = NULL;
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
	PgCarrier  *carrier = xtc_pg_backend_thread_start_carrier(entry_arg);

	/*
	 * Do NOT elog() here: the fiber has no PG error stack / ErrorContext yet
	 * (backend_thread_entry sets those up).  An early ereport would call
	 * errstart -> exit() and tear down the shared postmaster.
	 */

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
	if (carrier == NULL || xtc_proc_set_userdata(carrier) != XTC_OK)
	{
		const char msg[] = "xtc: could not install backend fiber identity\n";

		xtc_diag_write(STDERR_FILENO, msg, sizeof(msg) - 1);
		/* No PG error stack yet: fail-stop, as in the quickdie path. */
		_exit(2);
	}

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
	 * backend_thread_entry() runs the tree's full thread-per-session init
	 * and then BackendMain, which does not return (pg_unreachable()).  Its
	 * client-socket waits route through waiteventset.c's xtc intercept.
	 */
	entry(entry_arg);

	/* If it ever returns, drop the borrowed owner before leaving the fiber. */
	(void) xtc_proc_set_userdata(NULL);
	xtc_exit_self(0);
}

/*
 * Start the xtc scheduler on a dedicated thread inside the postmaster.
 * Idempotent; called from PostmasterMain before the first child launch.
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
	 * the postmaster thread, so MyLatch is the postmaster's own latch).  The supervisor uses it to wake the postmaster
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
		xtc_app_opts_t opts = XTC_APP_OPTS_INIT;

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
		 * interrupts through latches, not OS signals.  Mirrors fork_process(),
		 * which blocks signals around process creation for exactly this reason.
		 */
		sigprocmask(SIG_SETMASK, &BlockSig, &save_mask);
		masked = true;

		/* Big fiber stacks: PG backends recurse deeply (see above). */
		xtc_runtime_set_stack_size(XTC_PG_FIBER_STACK);

		/*
		 * Fusion F0a: route libxtc's internal diagnostics into the PG server
		 * log when xtc_log_to_server is on.  Install the sink BEFORE
		 * xtc_app_start (which itself runs the host-tuning advisor by default in
		 * v1.32.0) so its advisories are captured, then trigger the advisor
		 * explicitly too (safe to call more than once).  Threaded-only; process
		 * mode never reaches carrier bringup.  Best-effort: a failed log-create
		 * just leaves libxtc's default stderr sink in place.  The xtc_log_t is
		 * process-lifetime (one carrier bringup per process); not destroyed.
		 */
		if (xtc_log_to_server)
		{
			xtc_log_opts_t logopts = XTC_LOG_OPTS_INIT;
			xtc_log_t  *pglog = NULL;

			logopts.sink = xtc_pg_log_sink;
			logopts.sink_user = NULL;
			logopts.floor = XTC_LOG_INFO;
			if (xtc_log_create(&logopts, &pglog) == XTC_OK && pglog != NULL)
			{
				(void) xtc_log_set_default(pglog);
				xtc_runtime_tuning_check();	/* fills libxtc's log ring with advisories */

				/*
				 * xtc_log is a RING BUFFER: xtc_log_write() (which
				 * xtc_runtime_tuning_check -> XTC_LOG_INFO_F feeds) only enqueues
				 * records; the sink callback fires only from xtc_log_drain()
				 * (there is no background drain thread, and we deliberately
				 * never destroy the process-lifetime log, so the destroy-time
				 * flush never runs).  Drain explicitly here so the advisories
				 * actually reach the sink -> the server log.  For the v1.32.0
				 * reachable producer (the one-shot bringup advisor) this single
				 * post-check drain is sufficient; if libxtc later streams
				 * continuous internal log lines, a periodic drain would be
				 * needed (revisit then).
				 */
				(void) xtc_log_drain(pglog);
			}
		}

		/*
		 * Threaded fault policy: fail-stop FAST.  A synchronous fault on a
		 * carrier thread cannot be contained -- libxtc's fault guard restores
		 * SIG_DFL and re-raises, and the whole shared-address-space process
		 * must die (correct: one fiber's memory corruption is not isolable).
		 * But the default SIGSEGV disposition core-dumps the entire
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

		g_xtc_n_loops = xtc_pg_carrier_loop_count();

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
		 * Diagnostic timeline (libxtc v1.41.1 xtc_tail): a bounded in-process
		 * ring of runtime events.  The SCHED source brackets the aio park with
		 * PARK (detail = op) and, on resume, RUN (detail = park-to-run latency in
		 * ns) -- so a fiber stranded on an I/O completion shows up as a PARK with
		 * NO matching RUN for that pid.  That is the direct test for the
		 * lost-completion-wake hang we are chasing with the libxtc team.
		 *
		 * OFF unless PG_XTC_TAIL is set: when a source is disabled xtc_tail is one
		 * branch and reads no clock, but we still gate it so production never even
		 * allocates the ring.  The ring holds 16384 records and overwrites
		 * oldest-first, so a busy run WILL wrap -- dump it promptly at the hang,
		 * and use xtc_tail_reset() before a load phase if you need the tightest
		 * window.
		 */
		{
			const char *tail = getenv("PG_XTC_TAIL");

			if (tail != NULL && tail[0] != '\0')
			{
				/*
				 * LOOP_POLL (libxtc >= 1.42.0) and WAKE (>= 1.43.0) ride the
				 * XTC_TAIL_SCHED mask -- they are event KINDS, not mask bits, so
				 * no extra flag is needed here.  Worth knowing what changed under
				 * us: in 1.42.0 LOOP_POLL fired on EVERY poll and, with 32 loops,
				 * consumed 87-93 % of the 16384-record ring, which made "loop L
				 * emitted no polls" unfalsifiable (evicted vs never happened).
				 * 1.43.0 makes it IDLE-ONLY (emitted only when a poll dispatched
				 * nothing, loop.c: n_out == 0) and adds xtc_tail_dropped(), so
				 * absence is interpretable again.  XTC_TAIL_WAKE records the
				 * DISPATCH side -- the missing link between "CQE reaped" and
				 * "task never ran".
				 */
				(void) xtc_tail_enable(XTC_TAIL_SCHED | XTC_TAIL_MSG);
			}
		}

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
		 * Spawn the per-loop supervisor fibers now that the loops are running,
		 * so abnormal backend fiber death is observed.
		 */
		xtc_carrier_start_supervisors();

		/*
		 * F4 supervision-fusion (optional, developer GUC xtc_carrier_use_orc;
		 * default off): ALSO stand up one libxtc xtc_orc OTP supervisor per loop.
		 * This is additive scaffolding -- the hand-rolled supervisors spawned
		 * just above remain the escalation source of truth and the backend spawn
		 * path is unchanged.  When the GUC is off (the default) this is skipped
		 * entirely, so the path is byte-for-byte the pre-F4 behaviour.  See
		 * xtc_carrier_start_orc_supervisors() for why these supervisors own no
		 * backend children today.
		 */
		if (xtc_carrier_use_orc)
			xtc_carrier_start_orc_supervisors();

		elog(LOG, "xtc: carrier scheduler thread up (%d loop%s, %d supervisor%s, %s)",
			 g_xtc_n_loops, g_xtc_n_loops == 1 ? "" : "s",
			 g_xtc_n_sups, g_xtc_n_sups == 1 ? "" : "s",
			 "thread-per-session backend pool");

		/*
		 * F4: report the optional xtc_orc supervisor count only when the GUC is
		 * on, so the common (GUC-off) startup log stays byte-for-byte unchanged.
		 * These supervisors own no children today (see
		 * xtc_carrier_start_orc_supervisors), so the line documents the scaffold's
		 * presence, not a child count.
		 */
		if (xtc_carrier_use_orc)
			elog(LOG, "xtc: xtc_orc supervisors up (%d supervisor%s); backend fibers are TEMPORARY children and on_child_down is the escalation path",
				 g_xtc_n_orc_sups, g_xtc_n_orc_sups == 1 ? "" : "s");
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
		int			rc = XTC_E_INVAL;
		int			attempt;

		sp.magic = XTC_SUP_SPAWN_MAGIC;
		sp.entry = entry;
		sp.entry_arg = entry_arg;

		/*
		 * Deliver the spawn to a LIVE supervisor without dropping the accepted
		 * connection.  Two failure modes under a large simultaneous connect
		 * burst, both of which the fork model never hits and neither should we:
		 *  - a supervisor whose spawn failed at startup has
		 *    g_xtc_sup_pid[i]==XTC_PID_NONE; xtc_send returns XTC_E_INVAL there
		 *    PERMANENTLY, so retrying that loop is futile -- rotate to the next
		 *    supervisor instead;
		 *  - xtc_send returns XTC_E_AGAIN (soft-full/contention) even with room;
		 *    the libxtc contract is to retry (the supervisor drains in us).
		 * Try each supervisor in turn (rotating), briefly backing off, until one
		 * accepts.  The bound is a safety valve against ALL supervisors wedged.
		 */
		for (attempt = 0; attempt < 4000; attempt++)
		{
			int			idx = (loop_idx + attempt) % g_xtc_n_sups;
			xtc_pid_t	sup = g_xtc_sup_pid[idx];

			if (!xtc_pid_is_none(sup))
			{
				rc = xtc_send(sup, &sp, sizeof(sp));
				if (rc == XTC_OK)
					return 0;
				/* XTC_E_AGAIN/INVAL: try the next supervisor; else hard-stop. */
				if (rc != XTC_E_AGAIN && rc != XTC_E_INVAL)
					break;
			}
			if (attempt >= g_xtc_n_sups)
				pg_usleep(attempt < g_xtc_n_sups + 16 ? 10 : 100);
		}
		elog(WARNING, "could not hand backend fiber spawn to any carrier supervisor after retries: rc=%d",
			 rc);
		return EAGAIN;
	}

	/*
	 * No supervisor for this loop (should not happen once supervisors are up)
	 * -- fall back to a direct spawn without monitoring, so a backend still
	 * launches.  Observability is lost for this one fiber only.
	 */
	{
		xtc_proc_opts_t po = XTC_PROC_OPTS_INIT;
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
		elog(LOG, "xtc: spawned backend fiber (unmonitored) pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ")",
			 pid.loop_id, pid.local_id, (uint64) pid.gen);
		return 0;
	}
}

/*
 * Backend exit seam (see header).  Called from backend_thread_finish() as the
 * last step of a fiber backend's exit.  The backend's
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
		char		buf[128];
		int			n;
		xtc_pid_t	self = xtc_self();

		n = snprintf(buf, sizeof(buf),
					 "xtc: backend fiber exiting pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ") code=%d\n",
					 self.loop_id, self.local_id, (uint64) self.gen, code);
		if (n > 0)
		{
			if (n > (int) sizeof(buf))
				n = (int) sizeof(buf);
			xtc_diag_write(STDERR_FILENO, buf, (size_t) n);
		}
	}

	/* Also covers early exits that retain the startup allocation. */
	(void) xtc_proc_set_userdata(NULL);

	xtc_exit_self(code);

	/* xtc_exit_self does not return; if it somehow does, do not fall back
	 * into the cleaned-up backend stack. */
	abort();
}

/*
 * Runtime seam used by waiteventset.c.  Yield the current backend fiber
 * until `fd` is ready for `interest_pg` (PG WL_* bits) or timeout.  Returns
 * WL_* bits that fired.  timeout_ms < 0 means wait forever.  Only called
 * when a libxtc proc is running (including waiteventset's non-backend fallback).
 * A hard FD-wait error terminates the process; callers may hold locks or own
 * wait-queue state for which PostgreSQL error unwinding is not safe.
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
	 * fiber-ctx save hook's assert): both branches below can park the fiber.  A
	 * bracketed thread-affine section (raw spinlock, OpenSSL error-queue span,
	 * sigprocmask window) holds per-OS-thread state that would be wrong if the
	 * fiber resumed on a different carrier, so it must never be open across a
	 * park.  An audit of every such span found each one yield-free (none calls
	 * a function that can park), so the depth is always zero
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
			/*
			 * Lazy restore, same reasoning as the fd-park resume below: this is
			 * a per-wake fiber-switch site, so an eager ~231-entry GUC rebind
			 * here would be paid on every timer wake.
			 */
			PgRuntimeRestoreCurrentWorkLazy(&sleep_snap);
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
	 *
	 * Use the LAZY restore.  This is a per-park/per-wake path -- the hottest
	 * fiber-switch site there is -- and the eager variant re-runs
	 * RebindSessionGUCVariablePointers(), a ~231-entry hash pass, on EVERY
	 * wake.  When a waiter's predicate is not yet true (an LWLock/buffer-lock
	 * retry loop re-parks immediately), that rebind becomes a 100%-CPU spin
	 * that never returns control to __xtc_loop_step, so this carrier thread
	 * stops calling xtc_io_poll and every fiber whose completion lives on this
	 * loop starves -- observed as a hard write-path wedge (0 tps) with waiters
	 * on Buffer/BufferExclusive and Lock/tuple whose pg_blocking_pids() is
	 * EMPTY, and one thread burning a core in
	 * BufferLockAcquire -> ProcSemaphoreWaitFiber -> xtc_pg_wait_fd ->
	 * PgRuntimeRestoreCurrentWork -> find_option.  The lazy variant restores
	 * the six roots and lets the ~130 derived caches and the session-rooted
	 * GUC arrays re-derive on first touch through the existing owner-token
	 * mechanism, which is exactly what the fiber-context-switch hook already
	 * relies on (see PgRuntimeRestoreCurrentWorkLazy's own contract).
	 */
	PgRuntimeRestoreCurrentWorkLazy(&snap);
	XtcPgVerifyCurrentWorkIsSelf();

	if (rc != XTC_OK && rc != XTC_E_AGAIN)
	{
		char		buf[128];
		int			n;

		/* No PG error callbacks or cleanup while caller-owned waits are live. */
		n = snprintf(buf, sizeof(buf),
					 "xtc: xtc_proc_wait_fd failed fd=%d rc=%d; terminating process\n",
					 fd, rc);
		if (n > 0)
			xtc_diag_write(STDERR_FILENO, buf, Min((size_t) n, sizeof(buf) - 1));
		_exit(2);
	}

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
 * answer is correct even mid-migration.  False without a live PG owner (bare
 * supervisor, non-fiber threads, process mode, or final exit after owner clear).
 */
bool
xtc_pg_backend_fiber_is_migratable(void)
{
	PgCarrier  *self_carrier;

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
bool
xtc_pg_carrier_send_forged_down(int signo)
{
	xtc_pid_t	sup;
	int			loop;

	/* The wire layout of a DOWN, as libxtc's proc.c sends it. */
	struct
	{
		uint8_t		kind;
		uint64_t	ref;
		xtc_pid_t	pid;
		int			reason;
		uint8_t		exit_kind;
	}			pg_attribute_packed() down;

	loop = xtc_exec_loop_id();
	if (loop < 0 || loop >= g_xtc_n_sups)
		return false;
	sup = g_xtc_sup_pid[loop];
	if (xtc_pid_is_none(sup))
		return false;

	down.kind = 'D';
	down.ref = 1;
	down.pid = xtc_self();
	down.reason = signo;
	down.exit_kind = (uint8_t) XTC_DOWN_KIND_SIGNAL;
	return xtc_send(sup, &down, sizeof(down)) == XTC_OK;
}

uint64
xtc_pg_carrier_total_steals(void)
{
	uint64		total = 0;

	if (g_xtc_exec == NULL || g_xtc_n_loops <= 1)
		return 0;

	for (int i = 0; i < g_xtc_n_loops; i++)
	{
		xtc_loop_stats_t st = XTC_LOOP_STATS_INIT;

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
		xtc_loop_stats_t st = XTC_LOOP_STATS_INIT;

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

/*
 * ---------------------------------------------------------------------------
 * Fusion F1: carrier/runtime hot-path counters (libxtc xtc_stats).
 *
 * These count PG-side runtime events that libxtc's per-loop
 * xtc_exec_loop_stats (surfaced by pg_stat_xtc_carriers) does NOT see, such
 * as sessions routed to a process-fallback backend.  Surfaced by the
 * pg_stat_xtc_runtime SRF/view.
 *
 * Each counter is a libxtc xtc_counter_t: per-CPU sharded, so an inc/add on
 * the hot path is one cache-line-local atomic add (see xtc_stats.h).  The
 * handles are created once, on the postmaster thread, via
 * xtc_pg_runtime_counters_register().  Before registration -- and forever in
 * process mode, where register() is never called -- g_xtc_runtime_counters[]
 * stays all-NULL and the inc/add helpers are a branch-and-return, so those
 * paths pay nothing.
 *
 * The names/order MUST match the XtcPgRuntimeCounter enum in the header.
 * ---------------------------------------------------------------------------
 */
static const char *const xtc_pg_runtime_counter_names[XTC_PG_RUNTIME_COUNTER_COUNT] = {
	[XTC_PG_RC_PROCESS_FALLBACKS] = "process_fallbacks",
};

static xtc_counter_t *g_xtc_runtime_counters[XTC_PG_RUNTIME_COUNTER_COUNT];
static bool g_xtc_runtime_counters_registered = false;

/*
 * Create the F1 runtime counters once.  Idempotent; called on the postmaster
 * thread before the first event is counted (see
 * postmaster_process_fallback_launch).  A create failure just leaves
 * that handle NULL -- the inc/add helper skips a NULL handle, so a partial
 * registration degrades to "that counter reads 0", never a crash.
 */
void
xtc_pg_runtime_counters_register(void)
{
	if (g_xtc_runtime_counters_registered)
		return;
	g_xtc_runtime_counters_registered = true;

	for (int i = 0; i < XTC_PG_RUNTIME_COUNTER_COUNT; i++)
	{
		char		name[64];

		snprintf(name, sizeof(name), "pg.runtime.%s",
				 xtc_pg_runtime_counter_names[i]);
		if (xtc_counter_create(name, &g_xtc_runtime_counters[i]) != XTC_OK)
			g_xtc_runtime_counters[i] = NULL;
	}
}

/*
 * Hot-path increment.  One cache-line-local atomic add when the counter is
 * registered; a NULL-guarded no-op otherwise (process mode, or before the
 * first registration).  `c` out of range is defensively ignored.
 */
void
xtc_pg_runtime_counter_add(XtcPgRuntimeCounter c, int64 delta)
{
	if ((unsigned) c >= XTC_PG_RUNTIME_COUNTER_COUNT)
		return;
	if (g_xtc_runtime_counters[c] != NULL)
		xtc_counter_add(g_xtc_runtime_counters[c], (int64_t) delta);
}

/*
 * Snapshot the counters for the pg_stat_xtc_runtime SRF.  Fills at most
 * `max` entries and returns the number written.  Returns 0 (empty view) when
 * the counters were never registered -- i.e. process mode or a threaded run
 * that never counted an event.  xtc_counter_read sums the per-CPU
 * shards, so this is the slow path by design; harmless for a monitoring view.
 */
int
xtc_pg_runtime_counters_snapshot(XtcPgRuntimeCounterStat *out, int max)
{
	int			n = 0;

	if (out == NULL || max <= 0)
		return 0;
	if (!g_xtc_runtime_counters_registered)
		return 0;

	for (int i = 0; i < XTC_PG_RUNTIME_COUNTER_COUNT && n < max; i++)
	{
		out[n].name = xtc_pg_runtime_counter_names[i];
		out[n].value = g_xtc_runtime_counters[i] != NULL
			? xtc_counter_read(g_xtc_runtime_counters[i]) : 0;
		n++;
	}
	return n;
}

/*
 * ---------------------------------------------------------------------------
 * Aux-worker restart-intensity tracking (supervision tree, detection+
 * escalation half).
 *
 * WHY THIS IS NOT xtc_orc, AND CANNOT BE: xtc_orc (libxtc's OTP supervisor,
 * xtc_orc.h) offers exactly this bookkeeping (xtc_sup_n_restarts, restart
 * intensity, escalation) -- but ONLY for a child it spawned itself.  Every
 * public xtc_orc counter is updated from a single call site inside its own
 * xtc_recv loop (upstream src/orc/sup.c __sup_entry, guarded by
 * __should_restart), reached only for a DOWN on a proc __spawn_child()
 * created.  There is no "adopt an already-running proc" call and no "record
 * an external restart event" call -- so an embedder that cannot hand it
 * spawn ownership gets NOTHING from it, not even observability.  We verified
 * this and reported it upstream.
 *
 * We cannot hand xtc_orc spawn ownership of an aux worker for two
 * independent reasons:
 *   1. PMChild slot assignment (AssignPostmasterChildSlot) and
 *      ActiveChildList mutation are postmaster-thread-affine; xtc_orc
 *      restarts a child from the SUPERVISOR'S OWN carrier-loop thread.
 *   2. Even where xtc_orc's internal classification is correct (v1.44.0
 *      fixed the spawn-then-monitor race we reported), its public API has
 *      no per-child DOWN detail.
 *
 * So restart OWNERSHIP for these singletons stays exactly where it already
 * safely is: PostgreSQL's own postmaster-thread-affine
 * LaunchMissingBackgroundProcesses() (postmaster.c), which relaunches a
 * missing walwriter/bgwriter/checkpointer every ServerLoop tick when
 * pmState allows it -- unchanged, and already correct for a fiber-backed
 * worker (reap_aux_or_backend_child routes both process and fiber/thread
 * exits through the same per-type cleanup_*_child()).
 *
 * What WAS missing is restart-INTENSITY tracking for that relaunch: nothing
 * previously noticed a worker dying and relaunching in a tight loop (a
 * "flapping" worker -- alive just long enough to publish a clean exit,
 * then dying again).  That is real OTP escalation and it is ours to add,
 * because the restart decision was always ours.  The algorithm below is
 * intentionally the SAME sliding-window design as xtc_orc's own
 * __record_restart/__intensity_exceeded (upstream src/orc/sup.c), so the
 * day per-child DOWN classification is added AND the PMChild thread-
 * affinity hazard has a safe answer, swapping this for a real
 * xtc_sup_start()-owned child is a mechanical mapping, not a re-derivation.
 *
 * Scope: ONE tracker per aux-worker family (an array indexed by BackendType
 * would generalize this trivially; only B_WAL_WRITER is wired to it today --
 * see xtc_pg_aux_worker_note_relaunch's caller in postmaster.c).  Detection
 * and escalation for a GENUINE CRASH remain on the existing, unrelated,
 * already-correct path: xtc_proc_spawn_monitor() + the per-loop supervisor's
 * XTC_DOWN_KIND_SIGNAL classifier (above in this file) -> xtc_dump ->
 * g_xtc_genuine_crash -> postmaster latch -> ExitPostmaster(1).  This
 * tracker answers a DIFFERENT question ("is this singleton flapping on
 * CLEAN exits") and must never suppress or duplicate that path.
 * ---------------------------------------------------------------------------
 */
#define XTC_PG_AUX_FLAP_MAX_RESTARTS 3	/* mirrors xtc_orc's own default */
#define XTC_PG_AUX_FLAP_PERIOD_NS (5LL * 1000 * 1000 * 1000)	/* 5s, mirrors xtc_orc's default */
#define XTC_PG_AUX_FLAP_WINDOW_CAP (XTC_PG_AUX_FLAP_MAX_RESTARTS + 4)

typedef struct XtcPgAuxFlapTracker
{
	int64		recent[XTC_PG_AUX_FLAP_WINDOW_CAP];
	int			n;
	int			total_restarts;
} XtcPgAuxFlapTracker;

static XtcPgAuxFlapTracker g_xtc_walwriter_flap;

/*
 * Record one relaunch of an aux-worker singleton and report whether it has
 * exceeded restart intensity (XTC_PG_AUX_FLAP_MAX_RESTARTS relaunches within
 * XTC_PG_AUX_FLAP_PERIOD_NS).  Called from the postmaster thread only (the
 * same thread that owns the relaunch decision), so no locking is needed --
 * mirrors xtc_orc's own single-threaded-supervisor assumption.
 *
 * Sliding-window algorithm identical in shape to upstream libxtc's
 * src/orc/sup.c __record_restart + __intensity_exceeded: keep a small ring
 * of recent relaunch timestamps; a relaunch is "in the window" if it is
 * newer than (now - period); if more than max_restarts are in the window,
 * intensity is exceeded.
 */
bool
xtc_pg_aux_worker_note_relaunch(const char *worker_name)
{
	XtcPgAuxFlapTracker *t = &g_xtc_walwriter_flap;
	int64		now = GetCurrentTimestamp();
	int			in_window;

	if (t->n < XTC_PG_AUX_FLAP_WINDOW_CAP)
		t->recent[t->n++] = now;
	else
	{
		memmove(&t->recent[0], &t->recent[1],
				(size_t) (XTC_PG_AUX_FLAP_WINDOW_CAP - 1) * sizeof(int64));
		t->recent[XTC_PG_AUX_FLAP_WINDOW_CAP - 1] = now;
	}
	t->total_restarts++;

	in_window = 0;
	for (int i = 0; i < t->n; i++)
	{
		/* GetCurrentTimestamp() is microseconds since the PG epoch. */
		if (t->recent[i] > now - (XTC_PG_AUX_FLAP_PERIOD_NS / 1000))
			in_window++;
	}

	if (in_window > XTC_PG_AUX_FLAP_MAX_RESTARTS)
	{
		elog(LOG,
			 "xtc: aux worker \"%s\" exceeded restart intensity (%d relaunches within %ld ms); escalating",
			 worker_name, in_window, (long) (XTC_PG_AUX_FLAP_PERIOD_NS / 1000000));
		return true;
	}
	return false;
}

#endif							/* USE_XTC_CARRIER */
