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
#include "postmaster/pg_xtc_carrier.h"

/* xtc public API */
#include "xtc.h"
#include "xtc_app.h"
#include "xtc_exec.h"		/* xtc_exec_loop, xtc_exec_n_loops */
#include "xtc_proc.h"
#include "xtc_async.h"		/* xtc_set_stack_size */

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
 * Clean-exit discriminator (AGENTS_XTC #7 Stage 1b).  A backend fiber records
 * its pid here just before it calls xtc_exit_self() on the NORMAL exit path
 * (xtc_pg_backend_fiber_exit), i.e. after backend_thread_finish() has already
 * published its PMChild exit.  When the supervisor then sees an ABNORMAL DOWN
 * (reason=-11, the known benign SIGSEGV inside libxtc's xtc_exit_self during
 * teardown -- findings 2c) it checks this ring: a pid present here reached its
 * clean exit, so the fault is the benign post-exit teardown fault -> do NOT
 * escalate (the postmaster already reaped it).  A pid absent means the fiber
 * died mid-work before publishing -- a GENUINE crash that must escalate.
 *
 * Fixed-size lock-free ring: the fiber writes its slot with a release store,
 * the supervisor scans with acquire loads.  Both run on the same loop, and a
 * stale hit only risks under-escalating a genuinely-crashed pid that happens
 * to collide with a recently-clean pid in the same ring slot -- vanishingly
 * unlikely and, until libxtc fixes 2c, safer than false-escalating every
 * benign teardown fault into a whole-cluster crash.
 */
#define XTC_PG_CLEAN_RING 256
typedef struct xtc_clean_exit_rec
{
	_Atomic uint32_t loop_id;
	_Atomic uint32_t local_id;
	_Atomic uint32_t gen;
	_Atomic uint32_t valid;
} xtc_clean_exit_rec;
static xtc_clean_exit_rec g_xtc_clean_ring[XTC_PG_CLEAN_RING];
static _Atomic unsigned g_xtc_clean_next;

/* Record that `pid` reached its clean exit path (called from the fiber). */
static void
xtc_mark_clean_exit(xtc_pid_t pid)
{
	unsigned	i = atomic_fetch_add(&g_xtc_clean_next, 1) % XTC_PG_CLEAN_RING;
	xtc_clean_exit_rec *r = &g_xtc_clean_ring[i];

	atomic_store(&r->valid, 0);
	atomic_store(&r->loop_id, pid.loop_id);
	atomic_store(&r->local_id, pid.local_id);
	atomic_store(&r->gen, pid.gen);
	atomic_store(&r->valid, 1);
}

/* True if `pid` was recorded as having reached its clean exit path. */
static bool
xtc_saw_clean_exit(xtc_pid_t pid)
{
	for (int i = 0; i < XTC_PG_CLEAN_RING; i++)
	{
		xtc_clean_exit_rec *r = &g_xtc_clean_ring[i];

		if (atomic_load(&r->valid) == 1 &&
			atomic_load(&r->loop_id) == pid.loop_id &&
			atomic_load(&r->local_id) == pid.local_id &&
			atomic_load(&r->gen) == pid.gen)
			return true;
	}
	return false;
}

/*
 * Set true (and the postmaster latch kicked) when the supervisor observes a
 * GENUINE abnormal fiber crash -- one that died before reaching its clean
 * exit.  The postmaster polls this in its main loop (Stage 1b escalation);
 * benign teardown faults (findings 2c) never set it.
 */
static _Atomic uint32_t g_xtc_genuine_crash;

/* Debug-only fault-injection entry counter (PG_XTC_INJECT_CRASH). */
static _Atomic unsigned g_xtc_inject_entry_count;
static int	g_xtc_n_sups = 0;

/*
 * Supervisor spawn request (AGENTS_XTC #7 Stage 1, race-free variant).
 * The postmaster spawn thread sends this to a loop's supervisor; the
 * supervisor -- running ON that loop -- does xtc_proc_spawn() followed by
 * xtc_monitor() with no intervening yield, so the monitor is established
 * BEFORE the new backend fiber can run.  This closes the spawn/register race
 * where a fast-crashing fiber could die before an out-of-band register
 * arrived, escaping observation.  A leading magic distinguishes it from a
 * libxtc DOWN signal in the supervisor mailbox.
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
 *   - a spawn request (xtc_sup_spawn_msg): xtc_proc_spawn() the backend fiber
 *     on THIS loop and immediately xtc_monitor() it, with no yield between,
 *     so the monitor is in place before the new fiber can run (race-free);
 *   - a DOWN signal (decoded via xtc_down_decode): a monitored backend
 *     exited.  reason 0 is a normal exit (the postmaster already reaps it;
 *     log at DEBUG only).  A non-zero reason means the fiber faulted/aborted;
 *     make it LOUD via a raw write (elog is unsafe from this bare fiber with
 *     no PG error stack) so an abnormal fiber death is never silent.
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

	for (;;)
	{
		void	   *msg = NULL;
		size_t		len = 0;
		xtc_pid_t	down_pid;
		int			down_reason = 0;
		int			rc;

		rc = xtc_recv(&msg, &len, -1);	/* block until a message arrives */
		if (rc != XTC_OK || msg == NULL)
			continue;

		if (xtc_down_decode(msg, len, &down_pid, &down_reason) == XTC_OK)
		{
			/* A monitored backend fiber exited. */
			if (down_reason != 0)
			{
				static __thread int nabn = 0;
				bool		clean = xtc_saw_clean_exit(down_pid);

				if (nabn < 32)
				{
					char		buf[192];
					int			n;

					nabn++;
					n = snprintf(buf, sizeof(buf),
								 "xtc: SUPERVISOR observed %s backend fiber DOWN "
								 "pid=(loop=%u,local=%u,gen=%u) reason=%d\n",
								 clean ? "benign-teardown-fault(post-clean-exit)"
								 : "GENUINE-CRASH",
								 down_pid.loop_id, down_pid.local_id,
								 down_pid.gen, down_reason);
					if (n > 0)
					{
						if (n > (int) sizeof(buf))
							n = (int) sizeof(buf);
						(void) write(STDERR_FILENO, buf, (size_t) n);
					}
				}

				/*
				 * Stage 1b escalation.  Only a GENUINE crash (the fiber died
				 * before reaching its clean exit path) escalates: flag it and
				 * kick the postmaster, which polls the flag and drives the
				 * existing crash policy.  A benign teardown fault (findings 2c:
				 * SIGSEGV inside xtc_exit_self after a clean, already-reaped
				 * exit) must NOT escalate -- else every ~3/11 normal teardown
				 * would crash the whole cluster.  The postmaster already reaped
				 * it.
				 */
				if (!clean)
					atomic_store(&g_xtc_genuine_crash, 1);
			}
			else
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
						(void) write(STDERR_FILENO, buf, (size_t) n);
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

			memcpy(&sp, msg, sizeof(sp));

			/* entry is invariant; store it once (no per-spawn wrapper). */
			g_xtc_backend_entry = sp.entry;
			po.name = "pg-backend";

			/*
			 * Spawn on THIS supervisor's loop with the stable entry_arg
			 * (a launch_backend.c-owned BackendThreadStart *) as the fiber
			 * arg, then monitor with no yield in between: the new fiber
			 * cannot be scheduled until the supervisor next yields (at
			 * xtc_recv), by which point the monitor is registered.
			 * Race-free observation, and no heap allocation on this bare
			 * pre-PG-init fiber.
			 */
			if (xtc_proc_spawn(my_loop, xtc_carrier_proc, sp.entry_arg,
							   &po, &bpid) == XTC_OK)
			{
				uint64_t	ref = 0;
				char		sbuf[96];
				int			sn;

				(void) xtc_monitor(bpid, &ref);
				/* raw write: elog is unsafe from this bare fiber */
				sn = snprintf(sbuf, sizeof(sbuf),
							  "xtc: spawned backend fiber pid=(loop=%u,local=%u,gen=%u)\n",
							  bpid.loop_id, bpid.local_id, bpid.gen);
				if (sn > 0)
				{
					if (sn > (int) sizeof(sbuf))
						sn = (int) sizeof(sbuf);
					(void) write(STDERR_FILENO, sbuf, (size_t) sn);
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
	 * errstart -> exit() and tear down the shared postmaster.  A raw write is
	 * safe.
	 */
	{
		static const char m[] = "xtc: backend fiber entered; running backend_thread_entry\n";
		(void) write(STDERR_FILENO, m, sizeof(m) - 1);
	}

	xtc_in_backend_fiber = true;

	/*
	 * Debug-only fault injection (AGENTS_XTC #7 Stage 1b test hook).  If
	 * PG_XTC_INJECT_CRASH=N is set, the Nth backend fiber to enter (1-based,
	 * counted across the whole pool) faults HERE, before running any real work
	 * and before reaching its clean exit path.  Now that the loop supervisor
	 * spawns+monitors atomically (no spawn/register race), even an immediate
	 * fault is observed: libxtc R1 containment turns the SIGSEGV into a
	 * DOWN(reason=-11) to the supervisor, which (no clean-exit record for this
	 * pid) flags a GENUINE crash and the postmaster terminates the runtime.
	 * Never set in production.
	 */
	{
		const char *inj = getenv("PG_XTC_INJECT_CRASH");

		if (inj != NULL && inj[0] != '\0')
		{
			long		target = strtol(inj, NULL, 10);
			unsigned	nth = atomic_fetch_add(&g_xtc_inject_entry_count, 1) + 1;

			if (target > 0 && (unsigned) target == nth)
			{
				volatile int *crashp = NULL;

				(void) write(STDERR_FILENO,
							 "xtc: INJECT_CRASH faulting this fiber before clean exit\n",
							 56);
				*crashp = 42;	/* SIGSEGV -> contained -> DOWN reason=-11 */
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

	pthread_mutex_lock(&g_xtc_start_lock);
	if (g_xtc_app != NULL)
	{
		pthread_mutex_unlock(&g_xtc_start_lock);
		return 0;				/* already started */
	}

	{
		xtc_app_opts_t opts = XTC_APP_OPTS_DEFAULT;

		/* Big fiber stacks: PG backends recurse deeply (see above). */
		xtc_set_stack_size(XTC_PG_FIBER_STACK);

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
	 * Hand the spawn to that loop's supervisor, which does xtc_proc_spawn()
	 * + xtc_monitor() atomically on-loop (no yield between), so the new
	 * backend fiber is monitored BEFORE it can run.  This closes the
	 * spawn/register race: a fiber that crashes immediately can no longer die
	 * unobserved.  The send is cross-thread (postmaster spawn thread ->
	 * supervisor mailbox) but only carries the entry/arg; the actual spawn
	 * and monitor happen together inside the supervisor.
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
			(void) write(STDERR_FILENO, buf, (size_t) n);
		}
	}

	xtc_in_backend_fiber = false;

	/*
	 * Record that this fiber reached its clean exit path (AGENTS_XTC #7
	 * Stage 1b) BEFORE calling xtc_exit_self().  backend_thread_finish() has
	 * already published this backend's PMChild exit, so the postmaster will
	 * reap it normally.  If xtc_exit_self() then benign-faults during teardown
	 * (findings 2c), the supervisor sees this clean record and does not
	 * escalate.  A fiber that crashes earlier -- before reaching here -- has
	 * no record, so its abnormal DOWN is treated as a genuine crash.
	 */
	xtc_mark_clean_exit(xtc_self());

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
#include "utils/backend_runtime.h"	/* PgCurrentWorkSnapshot, save/restore */

int
xtc_pg_wait_fd(int fd, int interest_pg, long timeout_ms)
{
	uint32_t	interest = 0;
	uint32_t	revents = 0;
	int64_t		timeout_ns = (timeout_ms < 0) ? -1 : (int64_t) timeout_ms * 1000000;
	int			out = 0;
	int			rc;
	PgCurrentWorkSnapshot snap;

	if (fd < 0)
	{
		/* No fd: honor a finite timeout by sleeping the fiber. */
		if (timeout_ms >= 0)
		{
			PgCurrentWorkSnapshot snap;

			PgRuntimeSaveCurrentWork(&snap);
			xtc_proc_sleep(timeout_ns);
			PgRuntimeRestoreCurrentWork(&snap);
			return WL_TIMEOUT;
		}
		return WL_LATCH_SET;	/* caller re-checks */
	}

	if (interest_pg & WL_SOCKET_READABLE)
		interest |= XTC_IO_READABLE;
	if (interest_pg & WL_SOCKET_WRITEABLE)
		interest |= XTC_IO_WRITABLE;
	interest |= XTC_IO_HUP | XTC_IO_ERR;

	{
		/* Prove the xtc wait path fired (rate-limited, raw write: elog may
		 * be mid-command). */
		static __thread int nlog = 0;
		if (nlog < 8)
		{
			char		buf[128];
			int			n;

			nlog++;
			n = snprintf(buf, sizeof(buf),
						 "xtc: fiber wait_fd fd=%d interest=0x%x timeout_ms=%ld (via xtc_proc_wait_fd)\n",
						 fd, interest, timeout_ms);
			if (n > 0)
			{
				if (n > (int) sizeof(buf))
					n = (int) sizeof(buf);
				(void) write(STDERR_FILENO, buf, (size_t) n);
			}
		}
	}

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

#endif							/* USE_XTC_CARRIER */
