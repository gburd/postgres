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

typedef struct xtc_carrier_arg
{
	xtc_carrier_entry_fn entry;
	void	   *entry_arg;
}			xtc_carrier_arg;

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
static int	g_xtc_n_sups = 0;

/* Register message: "please xtc_monitor this backend pid (child_slot N)". */
typedef struct xtc_sup_register_msg
{
	xtc_pid_t	backend_pid;
	int			child_slot;
} xtc_sup_register_msg;

/*
 * Number of carrier loops (each on its own OS thread).  A pool of loops lets
 * backend fibers run in parallel and avoids the single-loop lost-wakeup where
 * two or more fibers parked on one loop could starve each other.  Defaults to
 * the CPU count; override with PG_XTC_CARRIER_LOOPS for tuning (calibration
 * knob -- the ideal count depends on core count and connection mix).
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

/*
 * Per-loop supervisor fiber (AGENTS_XTC #7 Stage 1).  Runs forever on its
 * loop, servicing two message kinds:
 *   - a register message (xtc_sup_register_msg): xtc_monitor() the backend
 *     pid so we get a DOWN when it exits;
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
	(void) arg;

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

				if (nabn < 32)
				{
					char		buf[160];
					int			n;

					nabn++;
					n = snprintf(buf, sizeof(buf),
								 "xtc: SUPERVISOR observed ABNORMAL backend fiber DOWN "
								 "pid=(loop=%u,local=%u,gen=%u) reason=%d\n",
								 down_pid.loop_id, down_pid.local_id,
								 down_pid.gen, down_reason);
					if (n > 0)
					{
						if (n > (int) sizeof(buf))
							n = (int) sizeof(buf);
						(void) write(STDERR_FILENO, buf, (size_t) n);
					}
				}
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
		else if (len == sizeof(xtc_sup_register_msg))
		{
			xtc_sup_register_msg reg;
			uint64_t	ref = 0;

			memcpy(&reg, msg, sizeof(reg));
			(void) xtc_monitor(reg.backend_pid, &ref);
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
		if (xtc_proc_spawn(loop, xtc_carrier_supervisor_proc, NULL, &po,
						   &pid) == XTC_OK)
			g_xtc_sup_pid[i] = pid;
		else
			g_xtc_sup_pid[i] = XTC_PID_NONE;
	}
}

/* Fiber entry: run the tree's backend_thread_entry (all real init). */
static void
xtc_carrier_proc(void *arg)
{
	xtc_carrier_arg *ca = arg;
	xtc_carrier_entry_fn entry = ca->entry;
	void	   *entry_arg = ca->entry_arg;

	free(ca);

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
	xtc_carrier_arg *ca;
	xtc_proc_opts_t po = {0};
	xtc_pid_t	pid = XTC_PID_NONE;
	xtc_loop_t *loop;
	int			loop_idx = 0;

	if (xtc_pg_carrier_start() != 0)
		return EAGAIN;

	ca = malloc(sizeof(*ca));
	if (ca == NULL)
		return ENOMEM;
	ca->entry = entry;
	ca->entry_arg = entry_arg;

	/* Pick a loop round-robin across the pool; loop 0 in single-loop mode. */
	if (g_xtc_exec != NULL && g_xtc_n_loops > 1)
	{
		loop_idx = (int) (atomic_fetch_add(&g_xtc_next_loop, 1) %
						  (unsigned) g_xtc_n_loops);
		loop = xtc_exec_loop(g_xtc_exec, loop_idx);
	}
	else
		loop = g_xtc_loop;

	po.name = "pg-backend";
	if (xtc_proc_spawn(loop, xtc_carrier_proc, ca, &po, &pid) != XTC_OK)
	{
		free(ca);
		return EAGAIN;
	}

	/*
	 * Ask this loop's supervisor to xtc_monitor() the new backend so an
	 * abnormal death is observed (AGENTS_XTC #7 Stage 1).  Best-effort and
	 * cross-thread (postmaster spawn thread -> supervisor mailbox): if the
	 * send is dropped, the only cost is a missing DOWN observation for this
	 * backend -- normal-exit reaping is unaffected (the postmaster owns it).
	 */
	if (loop_idx < g_xtc_n_sups)
	{
		xtc_sup_register_msg reg;

		reg.backend_pid = pid;
		reg.child_slot = -1;	/* not plumbed through this seam yet */
		(void) xtc_send(g_xtc_sup_pid[loop_idx], &reg, sizeof(reg));
	}

	elog(LOG, "xtc: spawned backend fiber pid=(loop=%u,local=%u,gen=%u)",
		 pid.loop_id, pid.local_id, pid.gen);
	return 0;
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

#endif							/* USE_XTC_CARRIER */
