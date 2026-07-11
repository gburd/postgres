# Phase 13 Scheduler-Aware Wait Boundary Plan

Phase 13 starts from the Phase 12 thread-per-session runtime. The immediate
goal is not pooled scheduling yet; it is to make waits explicit enough that a
later scheduler can observe readiness and wake logical backend work without
changing every caller at once. The Phase 14/15 protocol scheduler must not infer
from this plan that arbitrary waits can detach a logical backend.

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
13 has real wait-completion records and observable wake routing, revisit whether
a larger upstream-style `Interrupt` abstraction should subsume more latch call
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
   - optional future wake handoff metadata. Any requeue hook is reserved for
     post-Phase-15 scheduler-boundary experiments, not Phase 14/15 deep-wait
     detachment.
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
   one wait family has a working wait-completion record and observable wake
   story. At that point, a larger refactor can be judged against real Phase 13
   mechanics rather than as an abstract cleanup.

## Current Implementation Status

The first wait-boundary slice is implemented:

- `PgWaitCompletion` records live beside `PgBackendWaitState`.
- `PgSuspend()` can publish the current backend/session/execution owner, wait
  kind, wait event info, wake mask, timeout, readiness state, and cancel/die
  interrupt flags while preserving the blocking callback fallback.
- `PgBackendWakeWaitCompletion()` records wait readiness and wakes the owning
  backend latch. A requeue hook may remain as experimental/future metadata, but
  Phase 14/15 must not install it for deep waits or treat it as carrier-release
  semantics.
- `SendInterrupt()` marks published wait completions for query-cancel and
  proc-die delivery without changing the logical interrupt mailbox semantics.
- `WaitEventSetWait()` is the first representative wait-family entry point
  because `WaitLatch()`, `WaitLatchOrSocket()`, and frontend socket waits
  already flow through it.
- `ProcWaitOnSemaphore()` publishes PGPROC-owned semaphore waits used by
  LWLocks, buffer content locks, CLOG group update, and ProcArray group update
  paths.  `ProcWakeSemaphore()` marks matching logical wait-completion records
  ready before falling back to the existing `PGSemaphoreUnlock()` wake.
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
  heavyweight advisory lock wait (`advisory`).  It also observes a real
  semaphore-backed LWLock wait (`TestBackendRuntimeLWLock`).  The same test
  confirms `pg_stat_activity` reports the expected wait event and that query
  cancel wakes each interruptible active published wait.

## Wait-Family Audit

The threaded-world core target now has scheduler-visible publication for the
blocking families that can park a regular backend:

| Family | Publication path | Evidence |
| --- | --- | --- |
| Event-set, latch, socket, frontend input/output, timeout | `WaitEventSetWait()` builds `PG_WAIT_KIND_EVENT_SET` and calls `PgSuspend()` | TAP observes `ClientRead`, `ClientWrite`, and `PgSleep`; unit coverage checks timeout metadata |
| Condition variables | `ConditionVariableTimedSleep()` waits through `WaitLatch()` | TAP observes `TestBackendRuntimeConditionVariable` |
| Heavyweight locks and signal waits | `ProcSleep()`/`ProcWaitForSignal()` wait through `WaitLatch()` | TAP observes advisory lock wait event `advisory` |
| PGPROC semaphores, including LWLocks, buffer content locks, CLOG group update, and ProcArray group update | `ProcWaitOnSemaphore()` builds `PG_WAIT_KIND_SEMAPHORE` and calls `PgSuspend()`; `ProcWakeSemaphore()` marks readiness before the legacy semaphore wake | TAP observes `TestBackendRuntimeLWLock`; source audit leaves only absorbed-wakeup balancing as raw semaphore operations |

Remaining direct platform `poll()`, `epoll_wait()`, and socket waits in regular
backend paths are under `WaitEventSetWait()`/`WaitLatchOrSocket()` or special
startup/authentication/control-plane paths.  Those special paths can continue
blocking in the thread-per-session fallback until a later phase gives startup
and control-plane work explicit scheduler tasks.

With this audit complete, Phase 13 has the wait-completion substrate and real
coverage needed before Phase 14 starts designing pooled-carrier scheduling.

## Validation Gate

After each wait-family conversion:

- `make check`
- `make check-threaded`
- `make check-threaded-workers` when worker/latch behavior is touched
- `make check-runtime-lifecycles` when runtime state shape changes
- focused TAP or isolation coverage for cancel/terminate/timeout while blocked
- `git diff --check`

Do not start Phase 14 protocol-boundary scheduling until the Phase 13 wait
boundary can publish and wake at least one representative wait family through
the new wait-completion path while process mode and thread-per-session fallback
remain healthy.

## Phase 13 Gate: MET (2026-07-09, re-validated on libxtc v1.9.0 with real fibers)

The Phase 14 precondition above is satisfied and re-verified after the runtime
became fiber-backed (client backends run as xtc fibers; io_method=xtc):

- Latch/timeout family: a backend blocked on pg_sleep publishes
  `Timeout/PgSleep` (observable from another session -- the
  WaitEventSetWait -> PgSuspend publication path), and pg_cancel_backend WAKES
  the blocked wait (verified: the wait returns and the backend leaves the active
  wait).  0 cores.
- Heavyweight-lock family: a backend blocked on a conflicting relation lock
  publishes `Lock/relation` (ProcSleep -> WaitLatch -> PgSuspend), and
  pg_cancel_backend wakes the lock wait.  0 cores.
- These exercise both the event-set/latch publication path and the
  ProcSleep/lock publication path with real fibers, cross-session observation,
  and cancel-driven wake -- exactly the representative-family + cancel-while-
  blocked gate.  Process mode and thread-per-session fallback remain healthy
  (non-cassert smoke 24/24; cassert smoke 0 cores/0 asserts).

The full Phase 13 TAP (t/004_phase13_wait_completion.pl) additionally covers
ClientRead/ClientWrite/ConditionVariable/advisory-lock/LWLock publication; run
it under `gmake check-threaded` on a disk-backed host (the meson-on-btrfs dev
env has the documented 001 background_psql harness hang; the direct probes above
substitute for spot validation here).  Phase 13 is complete; Phase 14 may begin
when taken up.
