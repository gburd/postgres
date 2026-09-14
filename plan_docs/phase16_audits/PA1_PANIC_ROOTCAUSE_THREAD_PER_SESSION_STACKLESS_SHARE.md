# P-A1 root cause + fix: the c>=192 "could not lease protocol read park" PANIC is a symptom of thread-per-session sharing the pooled stackless scheduler unnecessarily

Date: 2026-09-14. Investigation by agent 5dfb48ae (design-complete, code not landed; ran out of turns).
Source-verified by the PM. This is the P-A1 root cause the agent didn't get to commit.

## The PANIC and its real precondition
`postgres.c:7058` `elog(PANIC, "could not lease protocol read park for same carrier resume")` fires
when `PgRuntimeProtocolSchedulerLeaseBackend` fails on a same-carrier resume. Historically observed
(~1/20) under migratable=1 + contended GUC + real cross-loop steals. Its ACTUAL precondition is a
fiber being work-stolen while parked in the shared STACKLESS `parked_protocol_queue`/`runnable_queue`.

That machinery exists on the thread-per-session path ONLY because thread-per-session was wired to
share the POOLED scheduler's stackless-park bookkeeping unnecessarily. A true fiber does NOT need it:
it holds its own stack and parks IN PLACE via `xtc_pg_wait_fd` (inside `WaitEventSetWait`), so it never
enters the shared queue and the lease-on-resume race cannot occur.

## The code path (verified in source)
`PostgresRunSession` (postgres.c:7441):
```c
if (PgRuntimeIsThreadBacked(CurrentPgRuntime) && IsExternalConnectionBackend(MyBackendType))
    PgSessionRunProtocolSchedulerStaging(session);   /* pg_noreturn: the stackless park + lease/resume */
PgSessionRun(session);                                /* plain for(;;) PgSessionStepUnprotected loop */
```
- `PgSessionRunProtocolSchedulerStaging` is `pg_noreturn` (postgres.c:216), so TODAY both
  thread-per-session AND pooled external conns go into the stackless staging and never reach
  `PgSessionRun`.
- `PgSessionRun` (postgres.c:5550) is the plain error-boundary + `for(;;) PgSessionStepUnprotected`
  loop -- exactly what PROCESS MODE uses, and exactly what a fiber holding its stack needs: it parks in
  place on socket read via `xtc_pg_wait_fd` and never touches the run-queue/lease/budget machinery.
- Predicates available: `PgRuntimeIsPooledProtocol(runtime)` and `PgRuntimeKindIsPooledProtocol(kind)`
  (backend_runtime.h:3449) distinguish `PG_RUNTIME_POOLED_PROTOCOL` from `PG_RUNTIME_THREAD_PER_SESSION`.

## The fix (a DELETION of a detour, not a lease patch -- satisfies the remove-hand-rolled directive)
Branch the guard so ONLY the pooled protocol scheduler uses the stackless staging; thread-per-session
(a real fiber) falls through to plain `PgSessionRun`:
```c
if (PgRuntimeIsPooledProtocol(CurrentPgRuntime) && IsExternalConnectionBackend(MyBackendType))
    PgSessionRunProtocolSchedulerStaging(session);   /* pooled only */
PgSessionRun(session);                                /* thread-per-session fiber + process mode */
```
Thread-per-session then NEVER enters `PgSessionRunProtocolSchedulerStaging` /
`PgRuntimeProtocolSchedulerLeaseBackend` / the stackless park -- so the c>=192 PANIC's precondition is
structurally gone, and the fiber path does not DEPEND on the doomed run-queue machinery (clean for the
later deletion). This is the contract-honest fix: use the libxtc fiber (stack + in-place xtc_pg_wait_fd
park) instead of the hand-rolled stackless park + lease.

## What is already established (agent 5dfb48ae, 8-core dev box)
- Fiber-per-session (carriers=0) runs under real load: 8 loops/8 supervisors, migratable=1 dominant,
  thousands of real cross-loop steals per loop (loop 0: tasks_run=91166, steals=12588).
- The PANIC did NOT reproduce at c=64..384 (`-S`, default, `-C`) on this box -- consistent with the
  precondition analysis (pure thread-per-session has no second pooled carrier draining the shared
  queue). It needs the historical migratable+contended-GUC+steal shape (test 014) to surface, OR a
  bigger box; the FIX removes the path regardless.
- Interrupt/cancel/NOTIFY/timeout delivery is INDEPENDENT of the staging machinery
  (`SendInterrupt`/`SetLatch`/`interrupt_wake_fd` work off `MyProc`/`MyLatch`, not `protocol_park`
  state) -- so falling through to `PgSessionRun` does not regress signal/interrupt delivery. VERIFY
  this with a test (interrupt/cancel/NOTIFY/timeout under thread-per-session after the change).

## What remains (next agent)
Implement the one-line branch change, rebuild, gdb-confirm the in-place `xtc_pg_wait_fd` park under
the post-fix code, run the c=64..384 sweep + a migration-stress workload (test 014's contended-GUC
shape) for no-PANIC + no interrupt/cancel/NOTIFY/timeout regression, `gmake check` 245/245, commit +
push to optionA-fibers. No libxtc bug report warranted -- the open question was entirely PG-side.
