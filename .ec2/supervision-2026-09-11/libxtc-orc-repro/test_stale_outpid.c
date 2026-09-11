/*
 * Confirm: xtc_sup_add_child()'s out_pid is written ONCE (at add time) and
 * never updated when the supervisor restarts the child (sup.c:189 updates
 * c->pid internally; sup.c:548 copies it into out_pid only at the original
 * add_child call).  So a caller that keeps the pid across a PERMANENT/
 * TRANSIENT restart holds a STALE pid after the first restart.
 *
 * Demonstrate: register a PERMANENT child that exits immediately (forcing a
 * restart), capture out_pid once, wait for at least one restart to happen
 * (xtc_sup_n_restarts() > 0), then try to xtc_monitor() the ORIGINAL out_pid
 * -- if it is stale, the monitor targets a pid that no longer identifies the
 * (new) live child.
 */
#include "xtc.h"
#include "xtc_app.h"
#include "xtc_loop.h"
#include "xtc_proc.h"
#include "xtc_orc.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>

static xtc_supervisor_t *g_sup;
static _Atomic int g_run_count = 0;
static _Atomic int g_add_done = 0;
static xtc_pid_t g_original_pid;

static void
exit_after_delay(void *arg)
{
	(void) arg;
	int n = atomic_fetch_add(&g_run_count, 1) + 1;
	/* Sleep briefly so the pid is observably alive for a moment, then exit
	 * cleanly -- PERMANENT restarts unconditionally on ANY exit. */
	xtc_proc_sleep(5LL * 1000 * 1000);  /* 5ms */
	(void) n;
}

static void
runner(void *arg)
{
	xtc_child_spec_t spec;
	int rc;

	(void) arg;
	memset(&spec, 0, sizeof spec);
	spec.name = "restarter";
	spec.fn = exit_after_delay;
	spec.policy = XTC_RESTART_PERMANENT;   /* always restart */

	rc = xtc_sup_add_child(g_sup, &spec, &g_original_pid);
	printf("add_child rc=%d original_pid=(%u,%u,%u)\n", rc,
	       g_original_pid.loop_id, g_original_pid.local_id, g_original_pid.gen);
	atomic_store(&g_add_done, 1);
	xtc_exit_self(0);
}

/* Runs AFTER we have observed (from the main thread) that several restarts
 * have happened, so it must NOT be a proc pinned to the same loop with a
 * blocking wait inside it -- keep it a plain proc that does the one-shot
 * monitor check and exits immediately. */
static void
checker(void *arg)
{
	(void) arg;
	uint64_t ref;
	int rc = xtc_monitor(g_original_pid, &ref);
	printf("xtc_monitor(original_pid) rc=%d\n", rc);
	if (rc == XTC_OK)
	{
		void *msg; size_t sz; xtc_down_info_t di;
		rc = xtc_recv(&msg, &sz, 500LL * 1000 * 1000);
		if (rc == XTC_OK && xtc_down_decode_ex(msg, sz, &di) == XTC_OK)
		{
			printf("DOWN on original_pid: kind=%d reason=%d\n", di.kind, di.reason);
			if (di.kind == XTC_DOWN_KIND_NOPROC)
				printf("CONFIRMED STALE: original_pid's specific generation is already gone (NOPROC) -- the live child today has a DIFFERENT pid.gen that out_pid was never updated to.\n");
			else
				printf("original_pid still resolved to a real DOWN of kind=%d (not immediately conclusive either way from this call alone)\n", di.kind);
			xtc_free(msg);
		}
		else
			printf("no DOWN arrived on original_pid within 500ms (rc=%d)\n", rc);
	}
	xtc_exit_self(0);
}

int
main(void)
{
	xtc_app_opts_t opts = XTC_APP_OPTS_DEFAULT;
	xtc_app_t *app;
	xtc_loop_t *loop;
	xtc_sup_opts_t sup_opts = XTC_SUP_OPTS_DEFAULT;
	xtc_pid_t p;

	opts.name = "stale-outpid-probe";
	opts.n_loops = 1;
	xtc_app_create(&opts, &app);
	loop = xtc_app_loop(app);
	sup_opts.strategy = XTC_SUP_ONE_FOR_ONE;
	sup_opts.max_restarts = 1000000;
	sup_opts.period_ns = 1;
	xtc_sup_start(loop, &sup_opts, NULL, 0, &g_sup);
	xtc_app_start(app, NULL, 0);

	pthread_t th;
	pthread_create(&th, NULL, (void *(*)(void *)) xtc_app_run, app);
	usleep(50000);

	xtc_proc_spawn(loop, runner, NULL, NULL, &p);

	/* Poll from the MAIN thread (not a proc on the loop) so raw usleep here
	 * cannot block the loop's own OS thread -- unlike a poll loop placed
	 * inside a proc on the same single loop, which would starve the very
	 * child it is waiting on. */
	{
		int waited = 0;
		while (!atomic_load(&g_add_done) && waited < 2000) { usleep(10000); waited += 10; }
		waited = 0;
		while (atomic_load(&g_run_count) < 4 && waited < 5000) { usleep(10000); waited += 10; }
		printf("after %dms: run_count=%d n_restarts=%d\n",
		       waited, atomic_load(&g_run_count), xtc_sup_n_restarts(g_sup));
	}

	xtc_proc_spawn(loop, checker, NULL, NULL, &p);
	usleep(700000);

	xtc_sup_stop(g_sup);
	xtc_sup_join(g_sup, -1);
	xtc_app_stop(app);
	pthread_join(th, NULL);
	xtc_app_destroy(app);
	return 0;
}
