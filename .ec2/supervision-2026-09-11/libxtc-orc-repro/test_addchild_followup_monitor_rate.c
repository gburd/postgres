/*
 * How often does OUR OWN follow-up xtc_monitor() (registered on the pid
 * xtc_sup_add_child() hands back, AFTER the add_child round trip returns)
 * still race against a fast, no-yield crashing child -- under the FIXED
 * v1.44.0 library?  This is NOT the bug they fixed (that was sup.c's
 * INTERNAL spawn-then-monitor); this is our own SEPARATE external monitor,
 * added after add_child's ADD_CHILD/reply message round trip, which is
 * structurally a later, wider window no internal fix touches.
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

#define N_TRIALS 1000
static xtc_supervisor_t *g_sup;
static _Atomic int g_correct = 0, g_lost = 0;

static void
crash_child(void *arg) { (void) arg; *(volatile int *) 0x10 = 1; }

static void
guard(void *arg) { (void) arg; xtc_fault_guard_install(); xtc_exit_self(0); }

static void
runner(void *arg)
{
	(void) arg;
	for (int i = 0; i < N_TRIALS; i++)
	{
		xtc_child_spec_t spec;
		xtc_pid_t pid;
		uint64_t ref;
		int rc;

		memset(&spec, 0, sizeof spec);
		spec.name = "crash";
		spec.fn = crash_child;
		spec.policy = XTC_RESTART_TEMPORARY;
		rc = xtc_sup_add_child(g_sup, &spec, &pid);
		if (rc != XTC_OK) continue;

		rc = xtc_monitor(pid, &ref);   /* OUR OWN monitor, added AFTER add_child returns */
		if (rc != XTC_OK) continue;

		void *msg; size_t sz; xtc_down_info_t di;
		rc = xtc_recv(&msg, &sz, 2LL * 1000 * 1000 * 1000);
		if (rc != XTC_OK) continue;
		if (xtc_down_decode_ex(msg, sz, &di) != XTC_OK) { xtc_free(msg); continue; }
		if (di.kind == XTC_DOWN_KIND_SIGNAL) atomic_fetch_add(&g_correct, 1);
		else if (di.kind == XTC_DOWN_KIND_NOPROC) atomic_fetch_add(&g_lost, 1);
		xtc_free(msg);
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

	opts.name = "addchild-followup-rate";
	opts.n_loops = 1;
	xtc_app_create(&opts, &app);
	loop = xtc_app_loop(app);
	sup_opts.strategy = XTC_SUP_ONE_FOR_ONE;
	xtc_sup_start(loop, &sup_opts, NULL, 0, &g_sup);
	xtc_app_start(app, NULL, 0);

	pthread_t th;
	pthread_create(&th, NULL, (void *(*)(void *)) xtc_app_run, app);
	usleep(50000);
	xtc_proc_spawn(loop, guard, NULL, NULL, &p);
	usleep(20000);
	xtc_proc_spawn(loop, runner, NULL, NULL, &p);

	int waited = 0;
	while (waited < 60000) { usleep(10000); waited += 10;
		if (atomic_load(&g_correct) + atomic_load(&g_lost) >= N_TRIALS) break; }

	printf("SAME-LOOP add_child + OUR OWN follow-up monitor: correct(SIGNAL)=%d lost(NOPROC)=%d of %d\n",
	       atomic_load(&g_correct), atomic_load(&g_lost), N_TRIALS);

	xtc_sup_stop(g_sup);
	xtc_sup_join(g_sup, -1);
	xtc_app_stop(app);
	pthread_join(th, NULL);
	xtc_app_destroy(app);
	return 0;
}
