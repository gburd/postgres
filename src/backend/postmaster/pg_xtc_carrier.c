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

#include "miscadmin.h"
#include "postmaster/pg_xtc_carrier.h"

/* xtc public API */
#include "xtc.h"
#include "xtc_app.h"
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
static xtc_loop_t *g_xtc_loop;
static pthread_t g_xtc_thread;
static volatile bool g_xtc_ready;
static pthread_mutex_t g_xtc_start_lock = PTHREAD_MUTEX_INITIALIZER;

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

		opts.name = "pg-xtc-carrier";
		opts.n_loops = 1;
		if (xtc_app_create(&opts, &g_xtc_app) != XTC_OK)
		{
			elog(LOG, "xtc: xtc_app_create failed");
			rc = -1;
			goto out;
		}
		g_xtc_loop = xtc_app_loop(g_xtc_app);

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
		elog(LOG, "xtc: carrier scheduler thread up (single-loop app)");
	}

out:
	pthread_mutex_unlock(&g_xtc_start_lock);
	return rc;
}

/*
 * Launch a threaded backend as an xtc fiber.  `entry` is the tree's
 * backend_thread_entry; `entry_arg` is its BackendThreadStart *.  Returns 0
 * on success (the fiber is spawned on the carrier loop), non-zero errno-like
 * value on failure so the caller can fall back / report.
 */
int
xtc_pg_launch_backend_fiber(xtc_carrier_entry_fn entry, void *entry_arg)
{
	xtc_carrier_arg *ca;
	xtc_proc_opts_t po = {0};
	xtc_pid_t	pid = XTC_PID_NONE;

	if (xtc_pg_carrier_start() != 0)
		return EAGAIN;

	ca = malloc(sizeof(*ca));
	if (ca == NULL)
		return ENOMEM;
	ca->entry = entry;
	ca->entry_arg = entry_arg;

	po.name = "pg-backend";
	if (xtc_proc_spawn(g_xtc_loop, xtc_carrier_proc, ca, &po, &pid) != XTC_OK)
	{
		free(ca);
		return EAGAIN;
	}

	elog(LOG, "xtc: spawned backend fiber pid=(loop=%u,local=%u,gen=%u)",
		 pid.loop_id, pid.local_id, pid.gen);
	return 0;
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

	if (fd < 0)
	{
		/* No fd: honor a finite timeout by sleeping the fiber. */
		if (timeout_ms >= 0)
		{
			xtc_proc_sleep(timeout_ns);
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

	rc = xtc_proc_wait_fd(fd, interest, timeout_ns, &revents);

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
