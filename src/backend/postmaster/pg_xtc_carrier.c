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
 *	loop has an xtc_orc supervisor whose TEMPORARY children are the fibers
 *	placed on it, and a spawner fiber that adds them; a genuine crash
 *	fail-stops the server.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_XTC_CARRIER

#include <pthread.h>
#include <sched.h>
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
#include "xtc_exec.h"
#include "xtc_loop.h"
#include "xtc_proc.h"
#include "xtc_orc.h"
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
 * Supervision.  Each loop runs two procs of ours:
 *
 *   - an xtc_orc supervisor (g_xtc_orc_sup[i]) whose children are the
 *     backend fibers placed on that loop.  Every child is TEMPORARY: the
 *     postmaster, not the supervisor, decides whether a child type is
 *     restarted.  The supervisor's on_child_down hook classifies each exit,
 *     and a contained fault (XTC_DOWN_KIND_SIGNAL) fail-stops the server.
 *   - a spawner fiber (g_xtc_spawner_pid[i]).  xtc_sup_add_child must be
 *     called from a proc, and the postmaster thread is not one, so the
 *     postmaster sends the spawner a request and the spawner adds the child.
 *     The spawner also pins its loop thread to a CPU at startup.
 *
 * Nothing restarts a backend fiber here and nothing reaps a PMChild slot:
 * exactly-once reaping and the restart policy stay with the postmaster.
 */
#define XTC_PG_MAX_LOOPS 1024
static xtc_supervisor_t *g_xtc_orc_sup[XTC_PG_MAX_LOOPS];
static xtc_pid_t g_xtc_spawner_pid[XTC_PG_MAX_LOOPS];

/*
 * Set (and the postmaster latch kicked) when a supervisor observes a genuine
 * crash: a backend fiber that died of a contained fault.  The postmaster polls
 * it in ServerLoop.
 */
static _Atomic uint32_t g_xtc_genuine_crash;

/*
 * The postmaster's process latch, captured at carrier start on the postmaster
 * thread.  on_child_down sets it after flagging a crash, so the postmaster
 * escalates at once instead of waiting out DetermineSleepTime().
 */
static Latch *_Atomic g_xtc_postmaster_latch;

/*
 * A spawn request from the postmaster thread to a loop's spawner fiber.  The
 * leading magic tells it apart from anything else in the spawner's mailbox.
 */
#define XTC_SPAWN_MAGIC 0x58545343u		/* 'XTSC' */
typedef struct xtc_spawn_msg
{
	uint32_t	magic;
	xtc_carrier_entry_fn entry;
	void	   *entry_arg;
} xtc_spawn_msg;

/*
 * The backend fiber entry function.  It is the same for every fiber (the
 * tree's backend_thread_entry), so it is stored once and each fiber is spawned
 * with only its BackendThreadStart * as the argument.  This keeps the spawner
 * allocation-free: it runs on a bare fiber before any memory context exists.
 */
static xtc_carrier_entry_fn g_xtc_backend_entry;

/*
 * pg_usleep_hook: on a backend fiber, park the fiber for the delay instead of
 * sleeping the carrier thread.  A thread sleep would stall every other fiber
 * on the loop, including the one the sleeper is often waiting for: the
 * checkpointer polls with 10 ms sleeps for WAL senders to reach STOPPING,
 * and a WAL sender on the same loop cannot get there while the loop sleeps.
 * The wait seam saves and restores this fiber's current work around the park.
 */
static bool
xtc_pg_usleep(long microsec)
{
	if (!xtc_pg_in_backend_fiber())
		return false;
	(void) xtc_pg_wait_fd(-1, 0, (microsec + 999) / 1000);
	return true;
}

/*
 * Number of carrier loops, one OS thread each, fixed for the life of the
 * server.  carrier_loops = 0 (the default) means one loop per CPU the process
 * may run on, which is also the most it can use once every loop is pinned to
 * its own CPU.
 */
int
xtc_pg_carrier_loop_count(void)
{
	int			ncpus = 0;

#ifdef __linux__
	cpu_set_t	allowed;

	if (sched_getaffinity(0, sizeof(allowed), &allowed) == 0)
		ncpus = CPU_COUNT(&allowed);
#endif
	if (carrier_loops > 0)
		return carrier_loops;
	if (ncpus < 1)
		ncpus = (int) sysconf(_SC_NPROCESSORS_ONLN);
	if (ncpus < 1)
		ncpus = 1;
	return Min(ncpus, XTC_PG_MAX_LOOPS);
}

/*
 * Pin the calling loop thread to the loop_idx-th CPU of the process's allowed
 * set (wrapping when there are more loops than CPUs).  The allowed set is the
 * one the postmaster started with, captured before any loop pinned itself, so
 * every loop sees the same set.  libxtc exports no affinity call, so this uses
 * pthread_setaffinity_np directly.  Failure only costs locality, so it is
 * reported and otherwise ignored.
 */
#ifdef __linux__
static cpu_set_t g_xtc_allowed_cpus;
static int	g_xtc_n_allowed_cpus;
#endif

static void
xtc_carrier_pin_loop_thread(int loop_idx)
{
#ifdef __linux__
	cpu_set_t	one;
	int			nth;
	int			cpu = -1;
	char		buf[96];
	int			n;

	if (!carrier_cpu_pinning || g_xtc_n_allowed_cpus < 1)
		return;
	nth = loop_idx % g_xtc_n_allowed_cpus;
	for (int c = 0; c < CPU_SETSIZE; c++)
	{
		if (CPU_ISSET(c, &g_xtc_allowed_cpus) && nth-- == 0)
		{
			cpu = c;
			break;
		}
	}
	if (cpu < 0)
		return;
	CPU_ZERO(&one);
	CPU_SET(cpu, &one);
	if (pthread_setaffinity_np(pthread_self(), sizeof(one), &one) == 0)
		n = snprintf(buf, sizeof(buf), "xtc: loop %d pinned to CPU %d\n",
					 loop_idx, cpu);
	else
		n = snprintf(buf, sizeof(buf), "xtc: could not pin loop %d to CPU %d\n",
					 loop_idx, cpu);
	if (n > 0)
		xtc_diag_write(STDERR_FILENO, buf, Min((size_t) n, sizeof(buf) - 1));
#else
	(void) loop_idx;
#endif
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

/* The backend fiber body, added to a loop's supervisor by its spawner. */
static void xtc_carrier_proc(void *arg);

/*
 * on_child_down hook of the per-loop xtc_orc supervisors.
 *
 * Runs on the supervisor's fiber once per DOWN of a backend fiber it owns,
 * before its (TEMPORARY, so empty) restart decision.  A contained fault
 * (XTC_DOWN_KIND_SIGNAL) is a genuine crash that may have damaged the shared
 * address space, so flag it and wake the postmaster, which fail-stops the
 * server.  A clean or non-zero application exit is a normal end the postmaster
 * reaps, and KILLED or NOCONNECTION is a deliberate teardown; none escalate.
 * The supervisor only passes on a DOWN the library sent (XTC_MSG_SYS), so a
 * forged message cannot reach this hook.
 *
 * NB: the hook must not park.  An atomic store, SetLatch, and a raw write are
 * all it does; elog is unsafe on this bare fiber.  libxtc's xtc_dump takes
 * per-loop inspection locks, so it cannot run here either.
 */
static void
xtc_carrier_child_down(xtc_supervisor_t *sup, const xtc_child_spec_t *spec,
					   const xtc_down_info_t *info, void *user)
{
	Latch	   *pml;
	char		buf[160];
	int			n;

	(void) sup;
	(void) spec;
	(void) user;

	if (info == NULL || info->kind != XTC_DOWN_KIND_SIGNAL)
		return;

	n = snprintf(buf, sizeof(buf),
				 "xtc: supervisor observed GENUINE-CRASH backend fiber DOWN "
				 "pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ") signal=%d\n",
				 info->pid.loop_id, info->pid.local_id,
				 (uint64) info->pid.gen, info->signal);
	if (n > 0)
		xtc_diag_write(STDERR_FILENO, buf, Min((size_t) n, sizeof(buf) - 1));

	atomic_store(&g_xtc_genuine_crash, 1);
	pml = atomic_load(&g_xtc_postmaster_latch);
	if (pml != NULL)
		SetLatch(pml);
}

/*
 * Per-loop spawner fiber.  It pins its loop thread to a CPU, installs
 * libxtc's fault guard on that thread, and then serves spawn requests from
 * the postmaster by adding each backend fiber to this loop's supervisor.
 *
 * An add that fails leaves the fiber unstarted.  Its PMChild never registers,
 * so the postmaster's launch-registration wait times out and the connection
 * fails as for any failed spawn; the failure is logged here.
 */
static void
xtc_carrier_spawner_proc(void *arg)
{
	int			my_loop_idx = (int) (intptr_t) arg;

	xtc_carrier_pin_loop_thread(my_loop_idx);

	/*
	 * The fault guard handles SIGSEGV, SIGBUS, SIGFPE, and SIGILL on an
	 * alternate stack, so a fault in one backend fiber ends that fiber with a
	 * SIGNAL DOWN instead of taking the thread down unobserved.  It is per OS
	 * thread, and this fiber is the one proc of ours that runs on every loop.
	 */
	(void) xtc_fault_guard_install();

	for (;;)
	{
		void	   *msg = NULL;
		size_t		len = 0;
		xtc_spawn_msg sp;
		xtc_child_spec_t spec = XTC_CHILD_SPEC_INIT;
		xtc_pid_t	bpid = XTC_PID_NONE;
		int			rc;
		char		buf[128];
		int			n;

		if (xtc_recv(&msg, &len, -1) != XTC_OK || msg == NULL)
			continue;
		if (len != sizeof(sp) ||
			((const xtc_spawn_msg *) msg)->magic != XTC_SPAWN_MAGIC)
		{
			xtc_free(msg);
			continue;
		}
		memcpy(&sp, msg, sizeof(sp));
		xtc_free(msg);

		g_xtc_backend_entry = sp.entry;
		spec.name = "pg-backend";
		spec.fn = xtc_carrier_proc;
		spec.arg = sp.entry_arg;
		spec.policy = XTC_RESTART_TEMPORARY;
		spec.stack_size = XTC_PG_FIBER_STACK;

		rc = xtc_sup_add_child(g_xtc_orc_sup[my_loop_idx], &spec, &bpid);
		if (rc == XTC_OK)
			n = snprintf(buf, sizeof(buf),
						 "xtc: spawned backend fiber pid=(loop=%u,local=%u,gen=" UINT64_FORMAT ")\n",
						 bpid.loop_id, bpid.local_id, (uint64) bpid.gen);
		else
			n = snprintf(buf, sizeof(buf),
						 "xtc: could not add backend fiber on loop %d: rc=%d\n",
						 my_loop_idx, rc);
		if (n > 0)
			xtc_diag_write(STDERR_FILENO, buf, Min((size_t) n, sizeof(buf) - 1));
	}
}

/*
 * Start one xtc_orc supervisor and one spawner fiber on every loop.  Called
 * once from xtc_pg_carrier_start, after the loops run.  Without both on every
 * loop the server cannot place or supervise fibers there, so any failure is
 * fatal to the carrier start.
 */
static int
xtc_carrier_start_supervision(void)
{
	for (int i = 0; i < g_xtc_n_loops; i++)
	{
		xtc_loop_t *loop = (g_xtc_exec != NULL)
			? xtc_exec_loop(g_xtc_exec, i) : g_xtc_loop;
		xtc_sup_opts_t opts = XTC_SUP_OPTS_INIT;
		xtc_proc_opts_t po = XTC_PROC_OPTS_INIT;

		opts.strategy = XTC_SUP_ONE_FOR_ONE;
		opts.on_child_down = xtc_carrier_child_down;
		if (xtc_sup_start(loop, &opts, NULL, 0, &g_xtc_orc_sup[i]) != XTC_OK)
		{
			elog(LOG, "xtc: could not start the supervisor on loop %d", i);
			return -1;
		}

		po.name = "pg-xtc-spawner";
		if (xtc_proc_spawn(loop, xtc_carrier_spawner_proc,
						   (void *) (intptr_t) i, &po,
						   &g_xtc_spawner_pid[i]) != XTC_OK)
		{
			elog(LOG, "xtc: could not start the spawner on loop %d", i);
			return -1;
		}
	}
	return 0;
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
#ifdef __linux__
		if (sched_getaffinity(0, sizeof(g_xtc_allowed_cpus),
							  &g_xtc_allowed_cpus) == 0)
			g_xtc_n_allowed_cpus = CPU_COUNT(&g_xtc_allowed_cpus);
#endif

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

		if (xtc_carrier_start_supervision() != 0)
		{
			rc = -1;
			goto out;
		}
		pg_usleep_hook = xtc_pg_usleep;

		elog(LOG, "xtc: carrier up (%d loop%s, each with an xtc_orc supervisor%s)",
			 g_xtc_n_loops, g_xtc_n_loops == 1 ? "" : "s",
			 carrier_cpu_pinning ? ", pinned to a CPU" : "");
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
 * once a loop's spawner has the request, or an errno value if none would take
 * it.
 *
 * Fibers are placed round-robin across the loops, so concurrent backends run
 * in parallel.  The send crosses threads (postmaster to spawner) and can see
 * XTC_E_AGAIN when the spawner's mailbox is momentarily full under a connect
 * burst; the libxtc contract is to retry, so try each loop in turn, backing
 * off briefly, before giving up.
 */
int
xtc_pg_launch_backend_fiber(xtc_carrier_entry_fn entry, void *entry_arg)
{
	xtc_spawn_msg sp;
	int			first;
	int			rc = XTC_E_INVAL;

	if (xtc_pg_carrier_start() != 0)
		return EAGAIN;

	sp.magic = XTC_SPAWN_MAGIC;
	sp.entry = entry;
	sp.entry_arg = entry_arg;
	first = (int) (atomic_fetch_add(&g_xtc_next_loop, 1) %
				   (unsigned) g_xtc_n_loops);

	for (int attempt = 0; attempt < 4000; attempt++)
	{
		int			idx = (first + attempt) % g_xtc_n_loops;

		rc = xtc_send(g_xtc_spawner_pid[idx], &sp, sizeof(sp));
		if (rc == XTC_OK)
			return 0;
		if (rc != XTC_E_AGAIN)
			break;
		if (attempt >= g_xtc_n_loops)
			pg_usleep(attempt < g_xtc_n_loops + 16 ? 10 : 100);
	}
	elog(WARNING, "could not hand a backend fiber to any carrier loop: rc=%d", rc);
	return EAGAIN;
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
 * Test hook (see header): a user message laid out like a library DOWN, sent to
 * this loop's spawner.  The spawner discards anything but a spawn request, and
 * the supervisor never receives it at all, so it must change nothing.
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

	loop = (g_xtc_exec != NULL) ? xtc_exec_loop_id() : 0;
	if (loop < 0 || loop >= g_xtc_n_loops)
		return false;
	sup = g_xtc_spawner_pid[loop];

	down.kind = 'D';
	down.ref = 1;
	down.pid = xtc_self();
	down.reason = signo;
	down.exit_kind = (uint8_t) XTC_DOWN_KIND_SIGNAL;
	return xtc_send(sup, &down, sizeof(down)) == XTC_OK;
}

/*
 * Total tasks stolen across all carrier loops since startup (summed
 * xtc_loop_stats_t.steals).  A lock-free snapshot of relaxed atomics; 0 in
 * single-loop mode.
 */
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
