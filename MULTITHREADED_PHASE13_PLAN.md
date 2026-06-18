# Phase 13 Scheduler-Aware Wait Boundary Plan

Phase 13 starts from the Phase 12 thread-per-session runtime. The immediate
goal is not pooled scheduling yet; it is to make waits explicit enough that a
later scheduler can suspend and resume logical backend work without changing
every caller at once.

## Signalling And Wakeup Boundary

Do not perform the full upstream-style latch-to-interrupt replacement as a
separate pre-Phase-13 refactor. The current branch already has the important
Phase 13 precondition: logical backend events can be represented as backend
interrupts, while latch wakeups remain the primitive for wait readiness.

Use this boundary for new Phase 13 work:

- logical backend event: use `SendInterrupt()`, `RaiseInterrupt()`, or a
  proc-signal reason that maps to the logical interrupt path;
- wait primitive readiness: use latch, condition variable, wait event set, or
  the Phase 13 wait-completion record;
- process lifecycle or external OS event: keep process signals/process-group
  signalling unless the target is explicitly known to be a logical backend;
- scheduler wakeup: route behind `PgBackendWakeForInterrupt()` or the wait
  completion layer, not by teaching arbitrary callers about carrier threads.

This keeps the branch aligned with the upstream direction without importing
the broad latch API replacement before the scheduler model exists. Once Phase
13 has real wait-completion records and a task requeue path, revisit whether a
larger upstream-style `Interrupt` abstraction should subsume more latch call
sites.

## Current Pre-Phase-13 Signalling State

Phase 12 now provides the alignment layer Phase 13 should build on:

- `SendInterrupt(PgBackend *, PgBackendInterruptType)` and
  `RaiseInterrupt(PgBackendInterruptType)` are the preferred APIs for logical
  backend interrupts;
- legacy `PgBackendRaiseInterrupt()` names remain compatibility wrappers;
- `SendBackendInterrupt()` and `SendProcSignal()` use local threaded wakeups
  when a target logical backend can be identified;
- proc-signal reasons that do not map to a backend interrupt can still use
  local slot-flag wakeups for threaded targets, preserving legacy semantics;
- Linux threaded latch wakeups use local wake fds rather than Unix signals.

Known non-goals before Phase 13:

- do not mechanically replace every `SetLatch()` call;
- do not add new interrupt enum values just to avoid a latch wake;
- do not route postmaster/crash/termination process control through logical
  backend interrupts;
- do not collapse condition variables, shared-memory queues, or wait event
  sets into the interrupt API before wait-completion ownership exists.

## Phase 13 Work Plan

1. Inventory blocking wait families used by the threaded core target.
   Classify each as:
   - scheduler-visible in Phase 13;
   - keep blocking in thread-per-session fallback;
   - defer until Phase 14/15 because it needs pooled carrier state or executor
     yield points.
2. Introduce a minimal wait-completion record:
   - owning `PgBackend`/`PgSession`/`PgExecution`;
   - wait kind and wait event info;
   - readiness state;
   - timeout/cancel/termination interaction;
   - requeue hook for future pooled carriers.
3. Put scheduler-aware wake routing behind narrow helpers:
   - `PgBackendWakeForInterrupt()` for logical backend interrupts;
   - a wait-completion wake helper for latch/CV/socket/lock readiness.
4. Convert one low-risk wait family first, preserving blocking fallback.
   Good candidates are latch waits or frontend socket waits because they have
   clear ownership and existing wait-event metadata.
5. Add focused coverage for cancellation, termination, timeout, abandoned
   client, reconnect, and process-mode behavior for the converted family.
6. Repeat family-by-family for:
   - latch waits;
   - frontend input;
   - frontend output;
   - condition variables;
   - lock waits;
   - timeout waits.
7. Reassess the broader upstream latch-to-interrupt architecture after at least
   one wait family has a working wait-completion record and scheduler requeue
   story. At that point, a larger refactor can be judged against real Phase 13
   mechanics rather than as an abstract cleanup.

## Current Implementation Status

The first wait-boundary slice is implemented:

- `PgWaitCompletion` records live beside `PgBackendWaitState`.
- `PgSuspend()` can publish the current backend/session/execution owner, wait
  kind, wait event info, wake mask, timeout, readiness state, and cancel/die
  interrupt flags while preserving the blocking callback fallback.
- `PgBackendWakeWaitCompletion()` records wait readiness and wakes the owning
  backend latch unless a future scheduler installs a requeue hook.
- `SendInterrupt()` marks published wait completions for query-cancel and
  proc-die delivery without changing the logical interrupt mailbox semantics.
- `WaitEventSetWait()` is the first representative wait-family entry point
  because `WaitLatch()`, `WaitLatchOrSocket()`, and frontend socket waits
  already flow through it.
- Focused backend-runtime coverage proves publication, existing-pending cancel
  seeding, later termination marking, readiness marking, and cleanup.
- Wait-completion publication is automatic for `PG_RUNTIME_THREAD_PER_SESSION`
  backends. Process-mode backends continue to use the direct blocking fallback
  path, while a narrow test/diagnostic override can force publication without a
  threaded runtime object.
- A focused threaded TAP test now observes real wait-completion records from
  another SQL session while a backend is blocked on frontend input
  (`ClientRead`), frontend output (`ClientWrite`), a latch wait (`PgSleep`), a
  condition variable wait (`TestBackendRuntimeConditionVariable`), and a
  heavyweight advisory lock wait (`advisory`).  The same test confirms
  `pg_stat_activity` reports the expected wait event and that query cancel
  wakes each active published wait.

The next Phase 13 slice should audit whether any remaining blocking family in
the threaded-world core target bypasses `WaitEventSetWait()` or the existing
lock/condition-variable paths, with timeout waits treated as part of the
representative latch/event-set coverage unless a distinct bypass appears.

## Validation Gate

After each wait-family conversion:

- `make check`
- `make check-threaded`
- `make check-threaded-workers` when worker/latch behavior is touched
- `make check-runtime-lifecycles` when runtime state shape changes
- focused TAP or isolation coverage for cancel/terminate/timeout while blocked
- `git diff --check`

Do not start Phase 14 pooled-carrier scheduling until the Phase 13 wait
boundary can suspend and resume at least one representative wait family through
the new wait-completion path while process mode and thread-per-session fallback
remain healthy.
