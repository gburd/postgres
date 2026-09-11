/* v1.44.0: does ATOMIC spawn+monitor classify a CONTAINED FAULT as SIGNAL on
 * OUR topology (supervisor + child SAME loop)?  Pre-fix: deterministic NOPROC.
 * Pattern copied from libxtc's own test/m8/test_proc.c flt_faulter(). */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include "xtc_app.h"
#include "xtc_loop.h"
#include "xtc_proc.h"

#define N 200
static int n_signal, n_noproc, n_clean, n_exit, n_other, n_nodown;

/* NO explicit arming: __proc_entry auto-arms a default recovery frame, so the
 * fault is contained and the DOWN carries KIND_SIGNAL (libxtc's own
 * flt_early_faulter relies on exactly this). */
static void faulter(void *arg)
{
	volatile uintptr_t addr = 0x10;
	(void)arg;
	*(volatile int *)addr = 1;              /* boom -> contained SIGSEGV */
}

static void probe(void *arg)
{
	xtc_loop_t *loop = (xtc_loop_t *)arg;
	int i;
	for (i = 0; i < N; i++) {
		xtc_pid_t pid; uint64_t ref;
		xtc_down_info_t di;
		void *msg = NULL; size_t sz = 0;
		if (xtc_proc_spawn_monitor(loop, faulter, NULL, NULL, &pid, &ref) != XTC_OK)
			continue;
		if (xtc_recv(&msg, &sz, 3000LL*1000*1000) != XTC_OK) { n_nodown++; continue; }
		memset(&di, 0, sizeof di);
		if (xtc_down_decode_ex(msg, sz, &di) != XTC_OK) { n_other++; continue; }
		switch (di.kind) {
		case XTC_DOWN_KIND_SIGNAL: n_signal++; break;
		case XTC_DOWN_KIND_NOPROC: n_noproc++; break;
		case XTC_DOWN_KIND_CLEAN:  n_clean++;  break;
		case XTC_DOWN_KIND_EXIT:   n_exit++;   break;
		default: n_other++; break;
		}
	}
	printf("RESULT same-loop atomic spawn_monitor, %d trials: SIGNAL=%d NOPROC=%d CLEAN=%d EXIT=%d other=%d nodown=%d\n",
	       N, n_signal, n_noproc, n_clean, n_exit, n_other, n_nodown);
	fflush(stdout);
	_exit(n_signal >= N * 95 / 100 ? 0 : 1);
}

int main(void)
{
	xtc_app_t *app = NULL; xtc_loop_t *loop; xtc_pid_t p;
	if (xtc_fault_guard_install() != XTC_OK) { puts("guard install failed"); return 2; }
	if (xtc_app_create(NULL, &app) != XTC_OK) { puts("app_create failed"); return 2; }
	loop = xtc_app_loop(app);
	if (loop == NULL) { puts("app_loop failed"); return 2; }
	if (xtc_proc_spawn(loop, probe, loop, NULL, &p) != XTC_OK) { puts("spawn failed"); return 2; }
	xtc_loop_run(loop);
	return 3;
}
