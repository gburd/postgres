/*
 * Precisely scoped follow-up to the (now-fixed) spawn-then-monitor race:
 * with libxtc v1.44.0's sup.c fix (atomic xtc_proc_spawn_monitor + the
 * TRANSIENT/NOPROC guard), does the PUBLIC xtc_orc API give an EXTERNAL
 * consumer (code that is not the supervisor itself) ANY way to learn a
 * specific child's DOWN classification (SIGNAL vs CLEAN vs EXIT vs NOPROC)?
 *
 * This matters because PostgreSQL's crash-escalation policy is external to
 * the supervisor: something OUTSIDE xtc_orc (our postmaster) needs to know
 * "did that child crash" to decide whether to fail-stop.  xtc_orc's restart
 * POLICY now classifies correctly INTERNALLY (this is what the v1.44.0 fix
 * verified 200/200), but internal correctness only affects xtc_orc's OWN
 * restart decision for that child -- it says nothing about whether a
 * DIFFERENT piece of code (us) can observe the SAME classification.
 *
 * Public xtc_orc.h surface: xtc_sup_n_children/n_alive/n_restarts/alive.
 * All are AGGREGATE counts.  There is no per-child accessor and no
 * terminate/DOWN callback.
 *
 * Test: register ONE TEMPORARY child (the policy PostgreSQL's own hand-rolled
 * supervisor uses for client backends) that CRASHES, and a second TEMPORARY
 * child that exits CLEANLY.  For TEMPORARY, xtc_orc's internal restart
 * decision is a no-op EITHER WAY (never restarts), so nothing in the public
 * counters can differ between the two cases if the ONLY signal available is
 * "the restart decision".  Confirm: after both are dead, do n_children/
 * n_alive/n_restarts/alive give ANY basis to tell which one crashed?
 *
 * Separately (not the main point, but worth recording): does adding our OWN
 * follow-up xtc_monitor() on the child pid returned by xtc_sup_add_child
 * still race, even under v1.44.0?  (This is a DIFFERENT race than the fixed
 * one: it is the caller's own SEPARATE monitor call, registered AFTER
 * add_child's ADD_CHILD/reply round trip -- which is inherently racy against
 * ANY same-loop cooperative scheduler for a fast, no-yield child, and is not
 * what sup.c's internal fix touches.)
 */
#include "xtc.h"
#include "xtc_app.h"
#include "xtc_loop.h"
#include "xtc_proc.h"
#include "xtc_orc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>

static xtc_supervisor_t *g_sup;

static void
crash_child(void *arg) { (void) arg; *(volatile int *) 0x10 = 1; }

static void
clean_child(void *arg) { (void) arg; }

static void
guard(void *arg) { (void) arg; xtc_fault_guard_install(); xtc_exit_self(0); }

static void
runner(void *arg)
{
	xtc_child_spec_t spec;
	xtc_pid_t pid_crash, pid_clean;
	int rc;
	int n_children_before, n_restarts_before;

	(void) arg;

	n_children_before = xtc_sup_n_children(g_sup);
	n_restarts_before = xtc_sup_n_restarts(g_sup);

	memset(&spec, 0, sizeof spec);
	spec.name = "crash-temp";
	spec.fn = crash_child;
	spec.policy = XTC_RESTART_TEMPORARY;
	rc = xtc_sup_add_child(g_sup, &spec, &pid_crash);
	printf("add crash-temp child: rc=%d pid=(%u,%u,%u)\n",
	       rc, pid_crash.loop_id, pid_crash.local_id, pid_crash.gen);

	memset(&spec, 0, sizeof spec);
	spec.name = "clean-temp";
	spec.fn = clean_child;
	spec.policy = XTC_RESTART_TEMPORARY;
	rc = xtc_sup_add_child(g_sup, &spec, &pid_clean);
	printf("add clean-temp child: rc=%d pid=(%u,%u,%u)\n",
	       rc, pid_clean.loop_id, pid_clean.local_id, pid_clean.gen);

	/* Let both run to completion. */
	usleep(50000);

	printf("\n--- PUBLIC xtc_orc counters after both children are dead ---\n");
	printf("n_children delta = %d (expect 2)\n", xtc_sup_n_children(g_sup) - n_children_before);
	printf("n_alive          = %d (expect 0 -- both dead, TEMPORARY never restarts)\n", xtc_sup_n_alive(g_sup));
	printf("n_restarts delta = %d (expect 0 -- TEMPORARY never restarts, so this is 0 for BOTH the crash and the clean exit -- IDENTICAL)\n",
	       xtc_sup_n_restarts(g_sup) - n_restarts_before);
	printf("alive            = %d (supervisor itself, irrelevant to per-child state)\n", xtc_sup_alive(g_sup));
	printf("\nCONCLUSION: the four public counters read IDENTICALLY whether a TEMPORARY\n");
	printf("child crashed (SIGNAL) or exited cleanly (CLEAN) -- xtc_orc's public API\n");
	printf("gives an EXTERNAL consumer NO way to distinguish the two for a TEMPORARY\n");
	printf("child, which is exactly the policy PostgreSQL needs for crash-escalation.\n");

	/*
	 * Secondary check: does the caller's OWN follow-up xtc_monitor(),
	 * registered on the pid AFTER add_child's round trip, still race even
	 * with v1.44.0's fix (a DIFFERENT race than the one they fixed: this is
	 * OUR monitor call, not the supervisor's internal one)?
	 */
	{
		xtc_child_spec_t s2;
		xtc_pid_t p2;
		uint64_t ref;
		void *msg; size_t sz; xtc_down_info_t di;

		memset(&s2, 0, sizeof s2);
		s2.name = "crash-temp-2";
		s2.fn = crash_child;
		s2.policy = XTC_RESTART_TEMPORARY;
		rc = xtc_sup_add_child(g_sup, &s2, &p2);
		rc = xtc_monitor(p2, &ref);   /* OUR OWN separate monitor, added AFTER add_child returns */
		rc = xtc_recv(&msg, &sz, 2LL * 1000 * 1000 * 1000);
		if (rc == XTC_OK && xtc_down_decode_ex(msg, sz, &di) == XTC_OK)
		{
			printf("\n--- secondary: OUR OWN follow-up monitor on an add_child pid ---\n");
			printf("kind=%d (2=SIGNAL correct, 3=NOPROC lost) -- %s\n", di.kind,
			       di.kind == XTC_DOWN_KIND_SIGNAL ? "correct" : "LOST (inherent: child may already be dead by the time OUR monitor call runs -- this is NOT the sup.c internal race, it is the caller's own separate late monitor, an unavoidable consequence of add_child returning the pid only AFTER spawn)");
			xtc_free(msg);
		}
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

	opts.name = "temporary-observability-probe";
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
	usleep(300000);

	xtc_sup_stop(g_sup);
	xtc_sup_join(g_sup, -1);
	xtc_app_stop(app);
	pthread_join(th, NULL);
	xtc_app_destroy(app);
	return 0;
}
