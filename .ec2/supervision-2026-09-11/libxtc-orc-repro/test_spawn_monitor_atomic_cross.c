/* Same sanity check but CROSS-LOOP placement, to confirm the atomic
 * primitive is safe there too (matches our real deployment: the
 * supervisor calls spawn_monitor ON the loop the child lands on, per
 * loop, so this should still be same-loop-relative -- but verify a
 * genuinely cross-loop caller too, for completeness). */
#include "xtc.h"
#include "xtc_app.h"
#include "xtc_loop.h"
#include "xtc_proc.h"
#include "xtc_exec.h"

#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>

#define N_TRIALS 3000
static _Atomic int g_correct = 0, g_lost = 0, g_other = 0;
static xtc_loop_t *g_loop0, *g_loop1;

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
		xtc_pid_t pid; uint64_t ref; int rc;
		/* Caller (on loop0) spawns onto loop1 -- true cross-thread. */
		rc = xtc_proc_spawn_monitor(g_loop1, crash_child, NULL, NULL, &pid, &ref);
		if (rc != XTC_OK) { printf("trial %d spawn_monitor rc=%d\n", i, rc); continue; }
		void *msg; size_t sz; xtc_down_info_t di;
		rc = xtc_recv(&msg, &sz, 2LL*1000*1000*1000);
		if (rc != XTC_OK) { printf("trial %d: no DOWN\n", i); continue; }
		xtc_down_decode_ex(msg, sz, &di);
		xtc_free(msg);
		if (di.kind == XTC_DOWN_KIND_SIGNAL) atomic_fetch_add(&g_correct, 1);
		else if (di.kind == XTC_DOWN_KIND_NOPROC) atomic_fetch_add(&g_lost, 1);
		else atomic_fetch_add(&g_other, 1);
	}
	xtc_exit_self(0);
}

int
main(void)
{
	xtc_app_opts_t opts = XTC_APP_OPTS_DEFAULT;
	xtc_app_t *app;
	xtc_pid_t p;

	opts.name = "atomic-sanity-cross";
	opts.n_loops = 2;
	xtc_app_create(&opts, &app);
	g_loop0 = xtc_app_loop(app);
	g_loop1 = xtc_exec_loop(xtc_app_exec(app), 1);
	xtc_app_start(app, NULL, 0);

	pthread_t th;
	pthread_create(&th, NULL, (void *(*)(void *)) xtc_app_run, app);
	usleep(50000);

	xtc_proc_spawn(g_loop0, guard, NULL, NULL, &p);
	xtc_proc_spawn(g_loop1, guard, NULL, NULL, &p);
	usleep(20000);

	xtc_proc_spawn(g_loop0, runner, NULL, NULL, &p);

	int waited = 0;
	while (waited < 60000)
	{
		usleep(10000); waited += 10;
		if (atomic_load(&g_correct) + atomic_load(&g_lost) + atomic_load(&g_other) >= N_TRIALS)
			break;
	}

	printf("CROSS-LOOP xtc_proc_spawn_monitor: correct(SIGNAL)=%d lost(NOPROC)=%d other=%d of %d\n",
	       atomic_load(&g_correct), atomic_load(&g_lost), atomic_load(&g_other), N_TRIALS);

	xtc_app_stop(app);
	pthread_join(th, NULL);
	xtc_app_destroy(app);
	return 0;
}
