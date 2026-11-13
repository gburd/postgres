# F4-SUP: dedup the hand-rolled carrier supervisor onto libxtc xtc_orc/xtc_svr

> **SUPERSEDED / CORRECTION (2026-11-04), UPDATED (2026-11-13).** Steps 2-4
> below were NOT implementable as written against the libxtc pinned through
> 2026-11-04: `xtc_orc` exposed **no per-child DOWN classification** to an
> external caller.  Its public surface was only aggregate counters
> (`xtc_sup_n_alive`/`n_restarts`); the supervisor decoded each child's DOWN in
> its own private `xtc_recv` loop solely to drive the restart decision and then
> discarded it, and a caller-side follow-up `xtc_monitor()` on the
> `xtc_sup_add_child` pid raced the supervisor's own monitor (measured 1000/1000
> lost / `NOPROC` on a same-loop crashing child).  This is verified and filed in
> `plan_docs/phase16_audits/LIBXTC_ORC_NO_PER_CHILD_DOWN.md`.  Consequently,
> routing backend fibers through `xtc_sup_add_child` would have **silently lost**
> the SIGNAL-DOWN -> `g_xtc_genuine_crash` + postmaster-latch escalation, which
> is the mandatory fail-stop crash policy.
>
> **The per-child-DOWN gap is now CLOSED in libxtc 2.2 (`8a7e9c06`).**  The two
> API shapes the audit requested landed: `xtc_sup_opts_t.on_child_down` (one
> hook per supervisor, fired on the supervisor's own fiber BEFORE the restart
> decision, observe-only, non-parking, carrying the child's spec so
> `spec->arg`/`spec->name` identify the child across restarts) and the
> DOWN-classification fix (a contained fault is now always
> `XTC_DOWN_KIND_SIGNAL`).  See `/tmp/libxtc-reply-pgreq-2026-11-12.md`
> sections 1 and 2.
>
> **Adopted (2026-11-13, gated):** the `on_child_down` hook is installed on the
> GUC-gated (`xtc_carrier_use_orc`, default off) `xtc_orc` supervisors
> (`xtc_carrier_orc_child_down` in `pg_xtc_carrier.c`).  Its SIGNAL branch does
> the identical Stage-1b escalation as the hand-rolled classifier
> (`atomic_store(&g_xtc_genuine_crash, 1)` + `SetLatch(postmaster)`); CLEAN /
> EXIT / KILLED do not escalate.  The mechanism is wired and testable the moment
> a child is added.  It does NOT call `xtc_dump()` (not a non-parking op; stays
> on the hand-rolled path).
>
> **Remaining step (F4 step 2), intentionally NOT taken yet:** routing the real
> backend spawn (`xtc_pg_launch_backend_fiber`) through `xtc_sup_add_child`
> (TEMPORARY children).  That is a larger behavioural cutover -- it moves the
> spawn+monitor+DOWN plumbing off the hand-rolled `XTC_SUP_SPAWN` mailbox onto
> the OTP supervisor -- and must be A/B-measured on `check-threaded-pooled`
> (neutral-or-better, no crash-containment regression, two reviews) before it
> can replace the escalation source of truth.  Until then these supervisors own
> **zero** children and the hand-rolled `xtc_carrier_supervisor_proc` +
> `XTC_SUP_SPAWN` mailbox + manual `XTC_DOWN_KIND_*` classifier remain the
> UNCHANGED source of truth.  See `xtc_carrier_start_orc_supervisors()` and
> `xtc_carrier_orc_child_down()` in
> `src/backend/postmaster/pg_xtc_carrier.c` for the authoritative code comments.
> Step 5 (`n_alive`/`n_restarts` into `pg_stat_xtc_carriers`) is intentionally
> NOT wired: with zero children those counters are constant (1 / 0) and would be
> noise.  NOT compiled or tested; EC2 validation pending.

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
