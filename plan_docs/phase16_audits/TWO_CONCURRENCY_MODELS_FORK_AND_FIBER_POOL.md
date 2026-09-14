# Concurrency models: TWO only — fork, and fiber-per-session on a fixed-size thread pool

Date: 2026-09-14. Owner directive: there will be exactly TWO concurrency models — (1) the existing
fork model, and (2) fiber-per-session running on a fixed-size thread pool. "Thread-per-session" as a
distinct model/option is EXPUNGED.

## The model space today (three enum values) vs the target (two)
`PgRuntimeKind` (backend_runtime.h:134): `PG_RUNTIME_PROCESS`, `PG_RUNTIME_THREAD_PER_SESSION`,
`PG_RUNTIME_POOLED_PROTOCOL`. That is three; the directive wants two.

Terminology trap, resolved by reading the code:
- **carriers=0** does NOT mean "one OS thread per session." It routes every B_BACKEND through
  `xtc_pg_launch_backend_fiber` (pg_xtc_carrier.c:1187), which spawns a libxtc FIBER round-robin
  across the FIXED `g_xtc_exec` loop pool (`g_xtc_n_loops` = `xtc_carrier_loop_count()`, an N-loop
  executor = N OS threads), with work-stealing. So carriers=0 is ALREADY "fiber-per-session on a
  fixed-size thread pool" -- it is mislabeled `PG_RUNTIME_THREAD_PER_SESSION`.
- **carriers>0** is the STACKLESS inline carrier pool: a fixed set of carrier OS threads each running
  sessions inline to a stackless protocol-read boundary (the hand-rolled `backend_pooled_protocol_*`
  scheduler). Sessions here are NOT fibers. This is the layer the remove-hand-rolled directive deletes.

So the SURVIVING threaded model = the carriers=0 execution path (fiber per session on the fixed
`xtc_exec` loop pool), and the things to EXPUNGE are:
1. The "thread-per-session" NAME/framing and any user-facing option that selects a per-session OS
   thread (there is no true per-thread model to keep -- the fiber path is the keeper).
2. The stackless inline carrier pool (carriers>0, `backend_pooled_protocol_*` + PgRuntimeProtocolScheduler)
   -- already slated for deletion by REMOVE_HANDROLLED_LIBXTC_REIMPLEMENTATIONS.md.

## Target enum: two threaded-relevant states
- `PG_RUNTIME_PROCESS` (fork) -- keep.
- ONE fiber model: fiber-per-session on the fixed xtc_exec loop pool. Collapse
  `PG_RUNTIME_THREAD_PER_SESSION` + `PG_RUNTIME_POOLED_PROTOCOL` into a single kind (name it for what
  it is, e.g. `PG_RUNTIME_FIBER_POOL` / `PG_RUNTIME_THREADED`). The stackless-pool behaviour is deleted;
  the fiber-on-fixed-pool behaviour is the one threaded kind.

## Config surface (target)
- `multithreaded` (on/off) selects fork vs the fiber-pool model. (Keep the existing GUC.)
- The pool SIZE is a fixed-size thread-pool knob (today `pooled_protocol_carriers`, but its semantics
  flip: it sizes the fiber pool = number of exec loops/threads, NOT a count of stackless carriers).
  Rename/redocument to "fiber pool threads" once the stackless pool is gone. Default: auto = one
  loop/thread per core (bounded), which is the fixed-size pool.
- DELETE `pooled_protocol_fiber_sessions` (Option A staging flag) -- the fiber model becomes THE model,
  so a flag to opt into it is meaningless.
- DELETE `pooled_protocol_carrier_message_budget` + PG_STEP_YIELD_BUDGET (the stackless-pool
  time-slicing workaround) -- a fiber yields at every park; CPU time-slicing is xtc_exec_class's job.
- The `carriers=0`-means-thread-per-session special case is gone; there is no 0 sentinel meaning a
  different model.

## Sequencing (do NOT break process mode or the fiber path; each step A/B neutral-or-better)
1. IN FLIGHT (agent b0b3451b): the fiber session execution path parks IN PLACE via xtc_pg_wait_fd
   through plain PgSessionRun (committed 966d05a186 on optionA-fibers), NOT the stackless staging.
   This proves the surviving model's execution path is sound. Validate (in-place park, no PANIC at
   c=64..384, no interrupt/cancel/NOTIFY/timeout regression, gmake check).
2. Make the fiber-per-session-on-fixed-pool model the DEFAULT threaded model (was
   pooled_protocol_fiber_sessions=on): multithreaded=on => fiber pool, sized by the pool knob.
3. DELETE the stackless inline carrier pool + the whole hand-rolled scheduler
   (backend_pooled_protocol_*, PgRuntimeProtocolScheduler, WaitParkedReads, message-budget,
   PG_STEP_YIELD_BUDGET, the stackless PG_STEP_PARK_PROTOCOL_READ lease/resume + the postgres.c:7058
   PANIC path). This is the bulk of REMOVE_HANDROLLED_LIBXTC_REIMPLEMENTATIONS.md TARGET 1 -- a pure
   deletion once the fiber path is the default.
4. Collapse the enum to two kinds; rename the pool-size GUC; delete pooled_protocol_fiber_sessions +
   the message-budget GUC; expunge every "thread-per-session" name/comment/test that implies a third
   model. Update docs (MULTITHREADED_ARCHITECTURE.md, PLAN, AGENTS.md) to describe exactly two models.
5. Evaluate the fixed-pool bound (TARGET 2): is a session-count ceiling needed beyond n_loops
   work-stealing? Use xtc_pool/xtc_credit if so; do not re-hand-roll.

## Invariants preserved (deliberate process-lifetime exceptions stay)
Single-user, bootstrap, frontend utilities, postmaster/control-plane, crash-escalation remain
process-lifetime -- unchanged. Process mode stays a fully supported model, byte-for-byte. The expunge
is of the THIRD (thread-per-session) framing + the stackless pool, NOT of process mode.
