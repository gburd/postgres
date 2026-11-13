# F4 cutover: route backend fibers through the xtc_orc supervisor

Status: DESIGN (2026-11-13). The next structural fusion increment after the
2.2 adoption (on_child_down hook + classifier fix landed). Two-review +
A/B-on-check-threaded-pooled gated. Branch stabilize-v23-20261113.

## Goal

Make the GUC-gated libxtc `xtc_orc` supervisor OWN the client-backend fibers as
`XTC_RESTART_TEMPORARY` children, so its `on_child_down` hook becomes the live
crash-escalation source (SIGNAL -> g_xtc_genuine_crash + postmaster latch), and
retire the hand-rolled `xtc_carrier_supervisor_proc` + the `XTC_SUP_SPAWN_MAGIC`
mailbox + the manual DOWN classifier. This is the last step that turns the
`xtc_orc` scaffold (today: 4 supervisors, 0 children) into the real supervision
tree.

## What exists today (verified on this branch)

Two separate supervisor procs per loop:
- `xtc_carrier_supervisor_proc` (g_xtc_sup_pid[]): receives our custom
  XTC_SUP_SPAWN message and does the atomic `xtc_proc_spawn_monitor` +
  classifies the child's DOWN + escalates. THE source of truth today.
- libxtc `xtc_sup` (g_xtc_orc_sup[]): `xtc_sup_start(loop, opts, NULL, 0)` with
  `on_child_down = xtc_carrier_orc_child_down` installed, ZERO children.

The launch path `xtc_pg_launch_backend_fiber` runs on the POSTMASTER SPAWN
THREAD (postmaster_backend_thread_launch -> launch_backend.c:1043), NOT a fiber.
It picks a loop round-robin and cross-thread `xtc_send`s the spawn to that
loop's hand-rolled supervisor, which does the on-loop atomic spawn+monitor.

## The blocker: xtc_sup_add_child cannot be called from the spawn thread

`xtc_sup_add_child(sup, spec, &pid)` (libxtc 8a7e9c06 / src/orc/sup.c):
- returns XTC_E_INVAL if `xtc_pid_is_none(xtc_self())` -- it MUST run inside a
  proc, because it sends an internal ADD_CHILD message to the supervisor and
  AWAITS the reply (synchronous round trip).
- returns XTC_E_STATE if called from the supervisor's own proc or inside its
  hook.

Our launch runs on the postmaster thread (no `xtc_self()`), so it cannot call
`xtc_sup_add_child` directly. And the child must land in the supervisor's own
child TABLE (via its internal `__handle_add_child` -> `__spawn_child`) for
`on_child_down` to fire -- a plain `xtc_proc_spawn_monitor` (what our hand-rolled
path does) does NOT register the child with the supervisor, so the hook would
never see its DOWN. There is no off-thread / async add_child in 8a7e9c06.

## Options

### Option A (needs a libxtc API): async xtc_sup_cast_add_child
Ask libxtc for a fire-and-forget add that is callable from ANY thread and does
not await a reply (like `xtc_sup_cast` is to `xtc_sup_call`):
```c
int xtc_sup_cast_add_child(xtc_supervisor_t *sup, const xtc_child_spec_t *spec);
```
It would enqueue the ADD_CHILD to the supervisor's mailbox from the spawn
thread; the supervisor spawns+monitors+tables the child on its own loop exactly
as `xtc_sup_add_child` does, but the caller does not block for the pid. Our
launch path does not need the child pid back (it never did -- the hand-rolled
XTC_SUP_SPAWN message is already fire-and-forget), so the cast fits perfectly
and preserves the current retry/rotate-on-EAGAIN admission (cast returns
AGAIN/INVAL the same way `xtc_send` does today). This is the CLEAN cutover.
FILED: /tmp/libxtc-sup-cast-add-child-2026-11-13.md.

### Option B (no libxtc change, more code): a per-loop adder fiber
Spawn one long-lived "adder" fiber per loop at carrier start. The launch path
cross-thread `xtc_send`s the spawn to that adder fiber (as today), and the adder
-- being a fiber on the sup's loop -- calls `xtc_sup_add_child`. This keeps the
message-passing shape but adds a second proc per loop purely to bridge the
thread->fiber boundary, and the adder's synchronous `xtc_sup_add_child` await
serializes adds on that loop (a connect-burst bottleneck the cast avoids).
Strictly worse than A; only worth it if libxtc declines the cast.

### Option C (not viable): make launch run on a fiber
The postmaster spawn thread is not a carrier fiber and should not become one
(it is control-plane, a process-lifetime exception). Rejected.

## Recommended: Option A, gated behind the existing xtc_carrier_use_orc GUC

1. Keep the hand-rolled supervisor + XTC_SUP_SPAWN mailbox + classifier as the
   default (GUC off) and the fallback until the A/B proves A neutral-or-better.
2. When `xtc_carrier_use_orc` is on: `xtc_pg_launch_backend_fiber` builds a
   TEMPORARY `xtc_child_spec_t` (name "pg-backend", fn=xtc_carrier_proc,
   arg=entry_arg, policy=TEMPORARY, loop=loop_idx) and `xtc_sup_cast_add_child`s
   it to g_xtc_orc_sup[loop_idx], with the same 4000-attempt rotate-on-AGAIN/
   INVAL admission loop the hand-rolled path uses.
3. `on_child_down` (already installed) becomes the escalation source: its SIGNAL
   branch already sets g_xtc_genuine_crash + latch, byte-identical to the
   hand-rolled classifier. CLEAN/EXIT/KILLED stay quiet. Because 2.2 guarantees
   SIGNAL for contained faults, this is now reliable (unlike the v2.1.x hole).
4. The hand-rolled xtc_carrier_supervisor_proc is NOT started when the GUC is on
   (the orc sup replaces it). Keep its code until the A/B + 2 reviews pass, then
   a separate commit deletes it + the XTC_SUP_SPAWN mailbox + the classifier.

## Preserved invariants
- TEMPORARY children: a crashed backend is NEVER auto-restarted; it escalates to
  whole-process fail-stop (SIGNAL -> on_child_down -> latch). xtc_orc's value
  here is spawn+monitor atomicity + the child table/hook, not auto-restart.
- Process mode byte-for-byte (all under USE_XTC_CARRIER + GUC).
- The retry/rotate admission (never drop an accepted connection under a connect
  burst) is preserved via the cast's AGAIN/INVAL handling.
- The orphan-reaper handshake (autovac-worker start-timeout cancel) that the
  hand-rolled supervisor backs must be re-homed onto xtc_sup_stop of one child
  or kept as a separate mechanism; audit before deleting the hand-rolled sup.

## A/B plan (the gate)
check-threaded-pooled, connect-storm + steady OLTP, GUC off vs on:
neutral-or-better tps/latency, no crash-containment regression (the Phase 16/19
crash + escalation TAP green on both), no connect-burst admission regression.
Two independent reviews. Only then delete the hand-rolled path.

## Why not just measure first (the roadmap's "measure the beat-fork lever")
That measurement (LWLock-contention vs structural) is about the PERF lever and
needs a metal A/B. This cutover is a CORRECTNESS/architecture consolidation
(one supervisor, not two; the hook as the real escalation) whose A/B gate is
"neutral-or-better", not "faster". Both can proceed; this one is blocked only on
the libxtc cast API (Option A), so the immediate action is the libxtc request.
