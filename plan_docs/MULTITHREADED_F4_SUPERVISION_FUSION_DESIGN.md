# F4-SUP: dedup the hand-rolled carrier supervisor onto libxtc xtc_orc/xtc_svr

Status: DESIGN (2026-08-31). Ready to implement once Option A (sessions-as-fibers) is live
(pooled_protocol_fiber_sessions flipped on after the libxtc task->state fix), because the
supervisor's value scales with fibers being the normal backend carrier.  Two-review gated,
A/B neutral-or-better on check-threaded-pooled, fallback kept until proven.

## What we hand-roll today (pg_xtc_carrier.c)
A per-loop supervisor FIBER (`xtc_carrier_supervisor_proc`, ~490-750) that:
1. is spawned one-per-loop by `xtc_carrier_start_supervisors` (g_xtc_sup_pid[]);
2. receives cross-thread spawn requests (xtc_sup_spawn_msg / XTC_SUP_SPAWN_MAGIC) from
   `xtc_pg_launch_backend_fiber` and does the atomic `xtc_proc_spawn_monitor` on-loop so a
   backend fiber is monitored before it can run;
3. observes child DOWNs, classifies CLEAN / EXIT / SIGNAL via a kind+range heuristic, and
   on a genuine (R1-contained SIGNAL) crash sets `g_xtc_genuine_crash` + kicks the
   postmaster latch (Stage 1b escalation); benign/policy exits are logged, not escalated;
4. backs the orphan-reaper handshake for autovac-worker start-timeout cancel
   (carrier_orphan_start).
This is ~260 lines of bespoke supervisor + a hand-rolled spawn mailbox protocol + a manual
DOWN classifier -- i.e. a partial re-implementation of an OTP supervisor.

## What libxtc v1.40.3 gives (xtc_orc.h + xtc_svr.h)
- `xtc_sup_start(loop, opts, child_spec[], n, &sup)`: an OTP supervisor proc on a loop with
  a child-spec array (entry fn, arg, restart policy: permanent/transient/temporary) and a
  restart STRATEGY (one_for_one etc.), doing spawn+monitor+restart internally.
- `xtc_sup_add_child(sup, spec, &pid)`: add a child dynamically (our per-connection backend
  spawn maps here -- a temporary child, no auto-restart: a crashed client backend must NOT
  be auto-restarted, it must escalate).
- `xtc_sup_n_children / n_alive / n_restarts / alive`: the bookkeeping we track by hand.
- `xtc_svr_*` (gen_server): request/reply + monitor-DOWN handling in a behaviour callback,
  if we want the supervisor to also answer status calls (pg_stat_xtc_carriers could read
  n_alive/n_restarts live).

## Mapping (dedup plan)
| hand-rolled today                              | libxtc replacement                          |
|------------------------------------------------|---------------------------------------------|
| xtc_carrier_supervisor_proc (per loop)         | one xtc_sup per loop via xtc_sup_start       |
| g_xtc_sup_pid[] + xtc_carrier_start_supervisors| the xtc_supervisor_t* per loop               |
| xtc_sup_spawn_msg / XTC_SUP_SPAWN_MAGIC mailbox| xtc_sup_add_child(sup, temporary spec, &pid) |
| manual DOWN kind/range classifier              | child restart policy = TEMPORARY (no auto-  |
|                                                | restart); DOWN reason from the sup callback  |
| g_xtc_genuine_crash + SetLatch escalation      | the sup's on-child-terminate callback maps a |
|                                                | SIGNAL DOWN -> set g_xtc_genuine_crash+latch |
| orphan-reaper handshake (autovac cancel)       | xtc_sup child cancel / xtc_sup_stop of one   |

Restart policy is the crux and must be preserved EXACTLY: a client-backend fiber crash
must **fail-stop the whole process** (threaded crash policy), NOT be auto-restarted.  So
every backend child is a TEMPORARY child (restart=never) and the supervisor's terminate
callback re-implements the Stage-1b escalation (SIGNAL DOWN -> g_xtc_genuine_crash + latch).
xtc_orc's value here is NOT auto-restart (we forbid it for backends) but: the spawn+monitor
atomicity, the child registry/bookkeeping (n_alive/n_restarts for observability), and the
uniform DOWN plumbing -- replacing our bespoke mailbox + classifier.

## Why gated on Option A
Under the stackless pooled default, backends are NOT fibers (the carrier pthread runs them
inline), so the supervisor only monitors the aux-worker + carrier fibers -- a thin surface.
Once Option A makes every session a fiber, the supervisor monitors 100s-1000s of backend
fibers per loop, and the spawn/monitor/DOWN plumbing is on the hot connect/disconnect path
-- exactly where xtc_orc's tuned implementation earns its keep and where an A/B is
meaningful.  Implementing before that would be measuring the wrong (thin) surface.

## Increment steps (when unblocked)
1. Behind USE_XTC_CARRIER + a `xtc_carrier_use_orc` dev GUC (default off), stand up one
   xtc_sup per loop in xtc_pg_carrier_start instead of xtc_carrier_supervisor_proc; keep
   the old path when off.
2. Route xtc_pg_launch_backend_fiber through xtc_sup_add_child (TEMPORARY spec) instead of
   the XTC_SUP_SPAWN_MAGIC mailbox.  Preserve the retry/rotate-on-EAGAIN admission behavior.
3. Move the DOWN classifier + Stage-1b escalation into the sup terminate callback; keep
   g_xtc_genuine_crash semantics byte-identical (SIGNAL -> escalate; CLEAN/EXIT -> quiet).
4. Wire xtc_sup_n_alive/n_restarts into pg_stat_xtc_carriers (bonus observability).
5. A/B connect-storm + steady OLTP on check-threaded-pooled: neutral-or-better, no crash-
   containment regression (the Phase 16/19 crash + escalation TAP must stay green).  Two
   independent reviews.  Only then delete the hand-rolled supervisor + mailbox + classifier.

## Guardrails (same as every fusion increment)
Process mode byte-for-byte; keep the fallback until proven; do NOT let xtc_orc auto-restart
a backend child (crash escalation is the required policy); the process-lifetime exceptions
(postmaster/control-plane) stand.
