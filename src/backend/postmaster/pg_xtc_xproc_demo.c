/*-------------------------------------------------------------------------
 * pg_xtc_xproc_demo.c -- a self-contained, GUC-gated DEMONSTRATION of the
 *	libxtc xtc_xproc cross-fork watchdog.  OUT-OF-TREE, throwaway, gated
 *	behind USE_XTC_CARRIER and the developer GUC xtc_xproc_watchdog_demo
 *	(default off).
 *
 *	WHAT THIS IS.  libxtc's xtc_xproc extends the Erlang-style process
 *	relations (spawn / send / link / monitor) across a fork() boundary:
 *	a parent xtc_xproc_spawn()s a child that runs its own xtc runtime, then
 *	xtc_xproc_monitor()s it so the child's CRASH or EXIT surfaces as a normal
 *	xtc DOWN -- with the signal / exit code decoded from the child's
 *	waitpid status, or XTC_DOWN_KIND_NOCONNECTION if the control channel
 *	dies first.  This file demonstrates, end to end, how an in-tree
 *	xtc_xproc watchdog WOULD use that facility to restart a crashed child
 *	under a bounded restart intensity -- the OTP "supervisor" pattern.
 *
 *	WHAT THIS IS NOT.  This is a demo of correct API USAGE, not a change
 *	to PostgreSQL's crash policy.  The real threaded-backend crash policy
 *	is unchanged: a genuine backend-fiber crash still fail-stops the whole
 *	process (see pg_xtc_carrier.c's supervisor + xtc_pg_consume_genuine_
 *	crash, and ExitPostmaster).  The bounded local restart loop below runs
 *	ONLY against this demo's own throwaway child, ONLY when the developer
 *	GUC is on, and is NEVER wired into ExitPostmaster or the genuine-crash
 *	path.  When the GUC is off (the default) nothing here runs and the
 *	production build is byte-for-byte unchanged.
 *
 *	WHERE IT RUNS.  xtc_xproc_watchdog_demo_kick() is called once from
 *	xtc_pg_carrier_start() (pg_xtc_carrier.c), right after the per-loop
 *	supervisors are spawned, iff the GUC is on.  It spawns a single demo
 *	fiber on carrier loop 0 via xtc_proc_spawn (exactly as the supervisors
 *	are spawned) -- xtc_xproc_spawn must be called from a fiber on the loop, so
 *	the demo cannot run on the bare scheduler thread.  The demo fiber owns
 *	the whole dance and then exits; it never respawns the PG backend and
 *	never touches a PMChild slot.
 *
 *	PLATFORM.  xtc_xproc is POSIX-only (fork + socketpair + waitpid); on
 *	Windows / where the facility declines, xtc_xproc_spawn returns XTC_E_NOSYS
 *	and the demo logs "not supported here" and exits cleanly.  The demo
 *	uses xtc_xproc_spawn (a raw function pointer across fork), NOT the
 *	re-exec'd xtc_xproc_spawn_entry path, so it does NOT need the
 *	xtc_xproc_win_child_maybe main()-hook: that hook only matters for the
 *	re-exec entry path, which this POSIX-only demo deliberately does not
 *	exercise.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_XTC_CARRIER

#include <unistd.h>
#include <stdio.h>
#include <stdint.h>

#include "miscadmin.h"
#include "postmaster/pg_xtc_carrier.h"	/* carrier loop accessor for the kick */
#include "postmaster/pg_xtc_xproc_demo.h"

/* xtc public API */
#include "xtc.h"
#include "xtc_loop.h"
#include "xtc_proc.h"			/* xtc_proc_spawn, xtc_recv, xtc_proc_sleep, DOWN decode */
#include "xtc_xproc.h"			/* xtc_xproc_spawn / xtc_xproc_monitor / xtc_xproc_* (POSIX-only) */

/*
 * Bounded restart intensity for the watchdog DEMO: at most this many child
 * restarts within the window below.  Deliberately tiny -- this is the OTP
 * "max_restarts / max_seconds" shape in miniature, not a production policy.
 */
#define XTC_XPROC_DEMO_MAX_RESTARTS		3
#define XTC_XPROC_DEMO_WINDOW_NS		((int64_t) 10 * 1000 * 1000 * 1000)	/* 10 s */

/* The child sleeps this long, then exits abnormally so the DEMO can show the
 * monitor -> decode -> restart loop.  Short, so the demo finishes promptly. */
#define XTC_XPROC_DEMO_CHILD_SLEEP_NS	((int64_t) 200 * 1000 * 1000)	/* 200 ms */

/* A distinctive nonzero exit code the child uses, so the decoded
 * XTC_DOWN_KIND_EXIT.exit_code in the log is unmistakably ours. */
#define XTC_XPROC_DEMO_CHILD_EXIT_CODE	42

/*
 * Best-effort diagnostic write to STDERR.  The demo runs on a BARE xtc fiber
 * (spawned with xtc_proc_spawn, like the carrier supervisors): it has no PG
 * error stack / ErrorContext, so elog()/ereport() are unsafe here -- a raw
 * write is the correct idiom (mirrors pg_xtc_carrier.c's xtc_diag_write).
 */
static inline void
xtc_xproc_demo_write(const char *buf, int n)
{
	ssize_t		w;

	if (n <= 0)
		return;
	w = write(STDERR_FILENO, buf, (size_t) n);
	(void) w;
}

#define XTC_XPROC_DEMO_LOG(...) \
	do { \
		char		_b[256]; \
		int			_n = snprintf(_b, sizeof(_b), __VA_ARGS__); \
		if (_n > (int) sizeof(_b)) \
			_n = (int) sizeof(_b); \
		xtc_xproc_demo_write(_b, _n); \
	} while (0)

/*
 * Child body (POSIX: this runs in the fork'd child, which inherited the
 * parent's address space so this raw function pointer is meaningful -- the
 * xtc_xproc_spawn contract).  It is a trivial OTP "worker": sleep briefly, then
 * exit abnormally with a distinctive code so the parent watchdog observes an
 * XTC_DOWN_KIND_EXIT and demonstrates the restart path.  xtc_proc_sleep parks
 * the fiber (keeps the child loop live); xtc_exit_self delivers the DOWN.
 *
 * Note: xtc_xproc_spawn arranges the child side (xtc_xproc_child_main) internally;
 * this function is the root_fn that child_main runs as the child's root proc.
 */
static void
xtc_xproc_demo_child_root(void *arg)
{
	(void) arg;					/* no payload needed for this demo */

	(void) xtc_proc_sleep(XTC_XPROC_DEMO_CHILD_SLEEP_NS);

	/*
	 * Exit abnormally (nonzero code) so the parent's watchdog sees an
	 * abnormal DOWN and exercises the bounded-restart branch.  A real worker
	 * would return (clean) on success; we force the abnormal case because the
	 * whole point of the demo is the restart-after-crash pattern.
	 */
	(void) xtc_exit_self(XTC_XPROC_DEMO_CHILD_EXIT_CODE);
}

/*
 * Spawn + monitor one demo child; return XTC_OK and the handle/ref via *out_p
 * / *out_ref, or an xtc error.  Logs the child's OS pid.  Factored out so the
 * initial spawn and each bounded restart share one code path.
 */
static int
xtc_xproc_demo_spawn_one(xtc_loop_t *loop, xtc_xproc_t **out_p, uint64_t *out_ref)
{
	xtc_xproc_t *child = NULL;
	uint64_t	ref = 0;
	int			rc;

	*out_p = NULL;
	*out_ref = 0;

	/* 1) xtc_xproc_spawn: fork a child that runs xtc_xproc_demo_child_root under
	 * its own xtc runtime.  POSIX-only; XTC_E_NOSYS where unsupported. */
	rc = xtc_xproc_spawn(loop, "pg-xtc-xproc-demo-child",
					xtc_xproc_demo_child_root, NULL, 0, &child);
	if (rc != XTC_OK)
		return rc;

	/* 2) xtc_xproc_os_pid: log the child's real OS pid (watchdog visibility). */
	XTC_XPROC_DEMO_LOG("xtc_xproc demo: spawned child os_pid=%ld\n",
					   xtc_xproc_os_pid(child));

	/* 3) xtc_xproc_monitor: a one-way DOWN when the child exits/crashes or its
	 * control channel dies.  (xtc_xproc_monitor, not xtc_xproc_link: a monitor does NOT
	 * bind the watchdog's own fate to the child, which is what a supervisor
	 * wants.)  Must be called from a fiber -- we are one. */
	rc = xtc_xproc_monitor(child, &ref);
	if (rc != XTC_OK)
	{
		xtc_xproc_destroy(child);	/* 5) tear the child down on failure */
		return rc;
	}

	XTC_XPROC_DEMO_LOG("xtc_xproc demo: monitoring child (ref=" UINT64_FORMAT ")\n",
					   (uint64) ref);

	*out_p = child;
	*out_ref = ref;
	return XTC_OK;
}

/*
 * The watchdog DEMO fiber.  Runs the full xtc_xproc dance:
 *
 *   spawn+monitor -> block for the child's DOWN -> xtc_down_decode_ex ->
 *   on an abnormal DOWN (SIGNAL / nonzero EXIT / NOCONNECTION) re-spawn, up
 *   to XTC_XPROC_DEMO_MAX_RESTARTS within XTC_XPROC_DEMO_WINDOW_NS -> destroy.
 *
 * This is the in-tree xtc_xproc watchdog pattern in miniature.  It is scoped
 * to this demo's own child; it never touches the PG backend crash policy.
 */
static void
xtc_xproc_demo_proc(void *arg)
{
	xtc_loop_t *loop = (xtc_loop_t *) arg;
	xtc_xproc_t *child = NULL;
	uint64_t	mon_ref = 0;
	int			restarts = 0;
	int64_t		window_start_ns;
	int			rc;

	XTC_XPROC_DEMO_LOG("xtc_xproc demo: START (DEMO ONLY; PG crash policy is "
					   "unchanged/fail-stop).  max %d restarts / %d s.\n",
					   XTC_XPROC_DEMO_MAX_RESTARTS,
					   (int) (XTC_XPROC_DEMO_WINDOW_NS / 1000000000));

	window_start_ns = xtc_clock_real();

	rc = xtc_xproc_demo_spawn_one(loop, &child, &mon_ref);
	if (rc == XTC_E_NOSYS)
	{
		/* POSIX-only facility: Windows / unsupported build.  Log and exit. */
		XTC_XPROC_DEMO_LOG("xtc_xproc demo: xtc_xproc_spawn not supported here "
						   "(XTC_E_NOSYS); demo is POSIX-only, exiting.\n");
		return;
	}
	if (rc != XTC_OK)
	{
		XTC_XPROC_DEMO_LOG("xtc_xproc demo: initial xtc_xproc_spawn/monitor failed "
						   "rc=%d; exiting.\n", rc);
		return;
	}

	for (;;)
	{
		void	   *msg = NULL;
		size_t		len = 0;
		xtc_down_info_t di = XTC_DOWN_INFO_INIT;
		bool		abnormal;

		/* Block for the monitored child's DOWN message. */
		rc = xtc_recv(&msg, &len, -1);
		if (rc != XTC_OK || msg == NULL)
			continue;

		/* xtc_down_decode_ex: self-describing kind/signal/exit_code. */
		if (xtc_down_decode_ex(msg, len, &di) != XTC_OK)
		{
			/* Not a DOWN -- ignore (the demo child sends no other messages). */
			xtc_free(msg);
			continue;
		}
		xtc_free(msg);

		switch (di.kind)
		{
			case XTC_DOWN_KIND_SIGNAL:
				XTC_XPROC_DEMO_LOG("xtc_xproc demo: child DOWN kind=SIGNAL "
								   "signal=%d (abnormal)\n", di.signal);
				abnormal = true;
				break;
			case XTC_DOWN_KIND_EXIT:
				XTC_XPROC_DEMO_LOG("xtc_xproc demo: child DOWN kind=EXIT "
								   "exit_code=%d (abnormal iff nonzero)\n",
								   di.exit_code);
				abnormal = (di.exit_code != 0);
				break;
			case XTC_DOWN_KIND_NOCONNECTION:
				XTC_XPROC_DEMO_LOG("xtc_xproc demo: child DOWN kind=NOCONNECTION "
								   "(control channel died; abnormal)\n");
				abnormal = true;
				break;
			case XTC_DOWN_KIND_CLEAN:
				XTC_XPROC_DEMO_LOG("xtc_xproc demo: child DOWN kind=CLEAN "
								   "(normal exit); done.\n");
				abnormal = false;
				break;
			case XTC_DOWN_KIND_NOPROC:
			default:
				XTC_XPROC_DEMO_LOG("xtc_xproc demo: child DOWN kind=%d "
								   "(benign/other); done.\n", (int) di.kind);
				abnormal = false;
				break;
		}

		/* 5) The old handle has fired its monitor; tear it down. */
		xtc_xproc_destroy(child);
		child = NULL;

		if (!abnormal)
			break;				/* clean/benign: the watchdog is done */

		/*
		 * Bounded restart: this is the DEMO of what an in-tree xtc_xproc
		 * watchdog would do on an abnormal child DOWN.  Reset the window when
		 * it elapses (OTP sliding-window intensity), and give up (fail-stop
		 * THIS DEMO ONLY) once the intensity is exceeded.
		 */
		if (xtc_clock_real() - window_start_ns > XTC_XPROC_DEMO_WINDOW_NS)
		{
			restarts = 0;
			window_start_ns = xtc_clock_real();
		}
		if (restarts >= XTC_XPROC_DEMO_MAX_RESTARTS)
		{
			XTC_XPROC_DEMO_LOG("xtc_xproc demo: restart intensity exceeded "
							   "(%d in-window); giving up (DEMO fail-stop -- NOT "
							   "the PG crash policy).\n", restarts);
			break;
		}

		restarts++;
		XTC_XPROC_DEMO_LOG("xtc_xproc demo: RESTART %d/%d (watchdog pattern)\n",
						   restarts, XTC_XPROC_DEMO_MAX_RESTARTS);

		rc = xtc_xproc_demo_spawn_one(loop, &child, &mon_ref);
		if (rc != XTC_OK)
		{
			XTC_XPROC_DEMO_LOG("xtc_xproc demo: restart xtc_xproc_spawn/monitor "
							   "failed rc=%d; exiting.\n", rc);
			break;
		}
	}

	if (child != NULL)
		xtc_xproc_destroy(child);

	XTC_XPROC_DEMO_LOG("xtc_xproc demo: END (demo fiber exiting; no effect on "
					   "the PG backend crash policy).\n");
}

/*
 * Kick the demo once, from the carrier scheduler thread at carrier start.
 * No-op unless the GUC is on.  Spawns the demo fiber on loop 0 with
 * xtc_proc_spawn -- the same bare-fiber spawn the supervisors use -- because
 * xtc_xproc_spawn must be called from a fiber on the loop.  Best-effort: if the
 * spawn fails the demo simply does not run.
 */
void
xtc_xproc_watchdog_demo_kick(void)
{
	xtc_loop_t *loop;
	xtc_proc_opts_t po = XTC_PROC_OPTS_INIT;
	xtc_pid_t	pid = XTC_PID_NONE;

	if (!xtc_xproc_watchdog_demo)
		return;					/* GUC off: zero production impact */

	loop = xtc_pg_carrier_loop0();
	if (loop == NULL)
	{
		elog(LOG, "xtc_xproc demo: no carrier loop available; demo skipped");
		return;
	}

	po.name = "pg-xtc-xproc-demo";
	if (xtc_proc_spawn(loop, xtc_xproc_demo_proc, (void *) loop, &po, &pid) != XTC_OK)
		elog(LOG, "xtc_xproc demo: failed to spawn demo fiber; demo skipped");
	else
		elog(LOG, "xtc_xproc demo: demo fiber spawned (DEMO ONLY; PG crash "
			 "policy unchanged)");
}

#endif							/* USE_XTC_CARRIER */
