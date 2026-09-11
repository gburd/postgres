/*
 * libxtc xtc_orc supervisor: clean, decisive repro of the spawn-then-
 * monitor classification race.
 *
 * Built against a PRISTINE libxtc checkout (verify with
 * `git -C <checkout> status --short` == empty before running).  This
 * program does not modify libxtc; it links against libxtc.a and drives
 * the PUBLIC xtc_orc/xtc_proc API only.
 *
 * Two DISTINCT things are measured, kept separate on purpose:
 *
 * (A) CLASSIFICATION LOSS: after xtc_sup_add_child() (the real API used
 *     to add a dynamic child to a running supervisor) returns a pid, can
 *     a caller that ALSO wants its own DOWN detail for that pid (because
 *     xtc_orc's public API has NO per-child terminate/DOWN callback --
 *     verified: xtc_orc.h exposes zero callback fields, only the
 *     aggregate counters xtc_sup_n_children/n_alive/n_restarts/alive)
 *     reliably get it via its own immediate xtc_monitor() call right
 *     after add_child returns?
 *
 *     MECHANISM (verified, not assumed): xtc_sup_add_child is a message
 *     ROUND TRIP -- it sends an ADD_CHILD control message to the
 *     supervisor's mailbox and BLOCKS (xtc_recv, a yield point) waiting
 *     for the reply.  Inside the supervisor (src/orc/sup.c
 *     __handle_add_child), the child is spawned (enqueued on its target
 *     loop) and the reply is sent AFTER that.  On a single loop, a
 *     cooperative scheduler runs enqueued work in the order it becomes
 *     runnable; the child was enqueued strictly before the reply, so it
 *     is eligible to run -- and, if it exits/crashes fast with no further
 *     yield, ALREADY GONE -- before the caller's xtc_recv for the reply
 *     returns and the caller can register ITS OWN monitor.  A monitor
 *     registered on an already-dead target always reports
 *     XTC_DOWN_KIND_NOPROC (this is documented, correct libxtc behavior
 *     in isolation) -- but it means the caller's classification of THAT
 *     child is lost: a genuine SIGSEGV reads identically to "already
 *     gone", with the real signal number unrecoverable.
 *
 * (B) SPURIOUS RESTART: does the SAME underlying race affect xtc_orc's
 *     OWN INTERNAL classification (the one __spawn_child's separate
 *     xtc_proc_spawn()+xtc_monitor() pair establishes, invisible to
 *     external callers)?  Measured via a behavioral proxy: a
 *     RESTART_TRANSIENT child that exits CLEANLY (reason 0) must NEVER
 *     be restarted (TRANSIENT restarts only on reason != 0).  If
 *     xtc_orc's OWN internal monitor also loses the race and reports the
 *     clean exit as NOPROC (reason -100000, which is != 0), a TRANSIENT
 *     child that exited perfectly cleanly gets wrongly restarted.  This
 *     is observable purely via xtc_sup_n_restarts(), with NO extra
 *     monitor added by the test.
 *
 * Both (A) and (B) are run in two placements:
 *   SAME-LOOP:  a single-loop supervisor (xtc_sup_opts_t.exec == NULL);
 *               children run on the supervisor's own loop.
 *   CROSS-LOOP: a multi-loop supervisor (opts.exec set) with the child
 *               spec explicitly targeting a DIFFERENT loop than the
 *               supervisor's own loop -- true concurrent OS threads.
 *
 * Usage: test_orc_spawn_monitor_race <mode> <placement> <trials>
 *   mode:       classify | restart
 *   placement:  same | cross
 *   trials:     integer trial count
 *
 * Build:
 *   cc -I<libxtc-checkout>/src/inc -O0 -g \
 *      -o test_orc_spawn_monitor_race test_orc_spawn_monitor_race.c \
 *      libxtc.a -lpthread -lrt -ldl -luring
 */
#include "xtc.h"
#include "xtc_app.h"
#include "xtc_loop.h"
#include "xtc_proc.h"
#include "xtc_orc.h"
#include "xtc_exec.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>

static xtc_supervisor_t *g_sup;
static _Atomic int g_ran = 0;
static _Atomic int g_correct = 0;   /* classify mode: DOWN kind == SIGNAL */
static _Atomic int g_lost = 0;      /* classify mode: DOWN kind == NOPROC */
static _Atomic int g_done = 0;

static const char *g_mode;         /* "classify" or "restart" */
static int g_placement_cross;      /* 0 = same-loop, 1 = cross-loop */
static int g_trials;

static void
crash_child(void *arg)
{
	(void) arg;
	atomic_fetch_add(&g_ran, 1);
	/* No yield: fault immediately.  R1's per-proc auto-armed default
	 * recovery frame (proc.c __proc_entry) contains this into a normal
	 * DOWN, so the process does not crash -- only this fiber "exits". */
	*(volatile int *) 0x10 = 1;
}

static void
clean_child(void *arg)
{
	(void) arg;
	atomic_fetch_add(&g_ran, 1);
	/* Returns immediately: a clean exit, DOWN reason 0. */
}

static void
guard_install_proc(void *arg)
{
	(void) arg;
	xtc_fault_guard_install();
	xtc_exit_self(0);
}

static void
runner(void *arg)
{
	(void) arg;
	for (int i = 0; i < g_trials; i++)
	{
		xtc_child_spec_t spec;
		xtc_pid_t pid;
		int rc;

		memset(&spec, 0, sizeof spec);
		spec.name = (strcmp(g_mode, "classify") == 0) ? "crash" : "cleanexit";
		spec.fn = (strcmp(g_mode, "classify") == 0) ? crash_child : clean_child;
		spec.policy = (strcmp(g_mode, "classify") == 0)
			? XTC_RESTART_TEMPORARY   /* classify: policy irrelevant, never restarts */
			: XTC_RESTART_TRANSIENT;  /* restart: must NOT restart on a clean exit */
		spec.loop = g_placement_cross ? 1 : 0;   /* only used when opts.exec != NULL */

		rc = xtc_sup_add_child(g_sup, &spec, &pid);
		if (rc != XTC_OK)
		{
			printf("trial %d: xtc_sup_add_child rc=%d\n", i, rc);
			continue;
		}

		if (strcmp(g_mode, "classify") == 0)
		{
			/*
			 * The caller's OWN follow-up monitor -- what any external
			 * observer needing per-child classification must do, since
			 * xtc_orc exposes no terminate/DOWN callback.  Called
			 * immediately, with NO other yielding call in between.
			 */
			uint64_t ref;
			rc = xtc_monitor(pid, &ref);
			if (rc != XTC_OK) { printf("trial %d: xtc_monitor rc=%d\n", i, rc); continue; }

			void *msg; size_t sz; xtc_down_info_t di;
			rc = xtc_recv(&msg, &sz, 2LL * 1000 * 1000 * 1000);
			if (rc != XTC_OK) { printf("trial %d: no DOWN arrived\n", i); continue; }
			if (xtc_down_decode_ex(msg, sz, &di) != XTC_OK) { xtc_free(msg); continue; }
			if (di.kind == XTC_DOWN_KIND_SIGNAL) atomic_fetch_add(&g_correct, 1);
			else if (di.kind == XTC_DOWN_KIND_NOPROC) atomic_fetch_add(&g_lost, 1);
			xtc_free(msg);
		}
	}
	atomic_store(&g_done, 1);
	xtc_exit_self(0);
}

int
main(int argc, char **argv)
{
	xtc_app_opts_t opts = XTC_APP_OPTS_DEFAULT;
	xtc_app_t *app;
	xtc_loop_t *loop0;
	xtc_sup_opts_t sup_opts = XTC_SUP_OPTS_DEFAULT;
	xtc_pid_t runner_pid;
	int rc;
	int before_children, before_restarts;

	if (argc < 4)
	{
		fprintf(stderr, "usage: %s <classify|restart> <same|cross> <trials>\n", argv[0]);
		return 2;
	}
	g_mode = argv[1];
	g_placement_cross = (strcmp(argv[2], "cross") == 0);
	g_trials = atoi(argv[3]);

	opts.name = "orc-race-repro";
	opts.n_loops = g_placement_cross ? 2 : 1;
	if (xtc_app_create(&opts, &app) != XTC_OK) { fprintf(stderr, "app_create failed\n"); return 1; }
	loop0 = xtc_app_loop(app);

	sup_opts.strategy = XTC_SUP_ONE_FOR_ONE;
	sup_opts.max_restarts = 1000000;   /* do not let intensity giveup mask the count */
	sup_opts.period_ns = 1;
	if (g_placement_cross)
		sup_opts.exec = xtc_app_exec(app);   /* multi-loop: enables cross-loop child placement */

	rc = xtc_sup_start(loop0, &sup_opts, NULL, 0, &g_sup);
	if (rc != XTC_OK) { fprintf(stderr, "xtc_sup_start rc=%d\n", rc); return 1; }

	if (xtc_app_start(app, NULL, 0) != XTC_OK) { fprintf(stderr, "app_start failed\n"); return 1; }

	pthread_t th;
	pthread_create(&th, NULL, (void *(*)(void *)) xtc_app_run, app);
	usleep(50000);

	/* Install the R1 fault guard on every loop thread the run will use. */
	{
		xtc_pid_t g0, g1;
		xtc_proc_spawn(loop0, guard_install_proc, NULL, NULL, &g0);
		if (g_placement_cross)
		{
			xtc_loop_t *loop1 = xtc_exec_loop(xtc_app_exec(app), 1);
			xtc_proc_spawn(loop1, guard_install_proc, NULL, NULL, &g1);
		}
		usleep(20000);
	}

	before_children = xtc_sup_n_children(g_sup);
	before_restarts = xtc_sup_n_restarts(g_sup);

	rc = xtc_proc_spawn(loop0, runner, NULL, NULL, &runner_pid);
	if (rc != XTC_OK) { fprintf(stderr, "spawn runner rc=%d\n", rc); return 1; }

	int waited_ms = 0;
	while (waited_ms < 60000)
	{
		usleep(20000);
		waited_ms += 20;
		if (atomic_load(&g_done) &&
		    xtc_sup_n_children(g_sup) - before_children >= g_trials &&
		    xtc_sup_n_alive(g_sup) == 0)
			break;
	}

	printf("mode=%s placement=%s trials=%d\n", g_mode, g_placement_cross ? "cross-loop" : "same-loop", g_trials);
	printf("waited_ms=%d children_ran=%d n_children_delta=%d n_alive=%d\n",
	       waited_ms, atomic_load(&g_ran),
	       xtc_sup_n_children(g_sup) - before_children, xtc_sup_n_alive(g_sup));

	if (strcmp(g_mode, "classify") == 0)
	{
		int correct = atomic_load(&g_correct);
		int lost = atomic_load(&g_lost);
		printf("RESULT classify: correct(SIGNAL)=%d lost(NOPROC)=%d of %d trials\n",
		       correct, lost, g_trials);
	}
	else
	{
		int restarts = xtc_sup_n_restarts(g_sup) - before_restarts;
		printf("RESULT restart: spurious_restarts=%d of %d TRANSIENT clean-exit trials (expected 0)\n",
		       restarts, g_trials);
	}

	xtc_sup_stop(g_sup);
	xtc_sup_join(g_sup, -1);
	xtc_app_stop(app);
	pthread_join(th, NULL);
	xtc_app_destroy(app);
	return 0;
}
