# Multithreaded PostgreSQL Implementation Plan

This plan is intentionally ambitious, but it is staged so that each phase
leaves the tree in a coherent state. The first implementation target is native
thread-per-session PostgreSQL. Pooled scheduling comes later.

## Current Documentation Baseline

The branch starts with:

- root-level architecture documentation;
- a root-level agent guide;
- local reference material in `refs/`;
- no code changes yet.

The first code changes should be scaffolding and behavior-preserving refactors,
not a direct jump to thread launch.

## Phase 0: Reference Audit And Invariants

Status: documentation phase.

Tasks:

- Keep `refs/` as the local reference set.
- Extract the useful ideas from Heikki's threading branch:
  - global annotations;
  - logical interrupts;
  - thread launch mechanics;
  - extension module gating;
  - GUC handling experiments;
  - session resource owner.
- Identify high-risk backend invariants before moving code:
  - top-level error recovery;
  - transaction abort cleanup;
  - signal mask assumptions;
  - memory context current-state assumptions;
  - `PGPROC` and lock ownership;
  - fd ownership and virtual fd cache;
  - relcache/catcache invalidation behavior.

Deliverables:

- `AGENTS.md`
- `MULTITHREADED_ARCHITECTURE.md`
- `MULTITHREADED_PLAN.md`

Validation:

- Documentation review.
- No code behavior changed.

## Phase 1: Runtime And State Scaffolding

Goal: introduce vocabulary and object skeletons without changing behavior.

Likely changes:

- Add headers for runtime/session/backend/carrier concepts.
- Add current-context pointers with process-mode initialization.
- Wrap existing process globals through accessors only where it clarifies the
  migration path.
- Extend or embed the existing `Session` object rather than creating a
  competing concept too early.
- Introduce the broader object as `PgSession` and embed the existing `Session`
  object initially; do not rename `Session` or repurpose `CurrentSession` in
  the first scaffolding commit.
- Add comments documenting ownership boundaries.

Initial objects can be thin:

- `PgRuntime`
- `PgCarrier`
- `PgBackend`
- `PgSession`
- `PgConnection`
- `PgExecution`

Expected commit shape:

- one commit for type declarations and no-op initialization;
- one commit for connecting current process-mode startup to the skeleton;
- no broad call-site churn yet.

Validation:

- build succeeds;
- `make check` or equivalent smoke tests pass in process mode;
- no threaded mode exposed yet.

## Phase 2: Global Lifetime Annotation

Goal: create visibility into mutable global state before moving it.

Likely changes:

- Add lifetime annotation macros inspired by Heikki's branch.
- Add or adapt a static tool to find unclassified mutable globals.
- Start with annotations that do not change generated code.
- Classify globals by ownership:
  - runtime-global;
  - immutable singleton;
  - dynamic singleton;
  - session-local;
  - execution-local;
  - carrier-local;
  - connection-local;
  - shared-memory state.

Expected commit shape:

- one commit for annotation macros and tooling;
- several focused commits annotating subsystems.

Validation:

- process-mode tests pass;
- static tool can run and produce a useful report;
- new mutable globals require explicit classification.

## Phase 3: Main Loop Unwinding

Goal: split `PostgresMain()` into stateful pieces while preserving synchronous
process-mode behavior.

Likely changes:

- Move volatile loop locals into a session/main-loop state struct:
  - `send_ready_for_query`;
  - idle timeout enabled flags;
  - skip-until-sync state;
  - extended-query-message state where appropriate.
- Extract top-level initialization into a session bootstrap function.
- Extract top-level error recovery into a named function.
- Extract one command-cycle function.
- Add a process-mode runner that simply loops over the new step function.

Target shape:

```c
PgStepResult PgSessionStep(PgSession *session, PgStepBudget budget);
void PgSessionRun(PgSession *session);
```

`PgSessionStep()` is the protected public entrypoint. It must install the
session's top-level error boundary, or verify that the matching boundary is
already active. Any unprotected helper is private and must not be called by a
runtime, thread launcher, or scheduler.

Early `PgSessionStep()` may still block inside `ReadCommand()` and command
execution. That is acceptable in this phase. The objective is to make the state
explicit, isolate the loop, and make the error-boundary contract unambiguous.

Validation:

- process-mode regression tests pass;
- error recovery tests still behave correctly;
- extended query protocol still handles skip-until-sync correctly;
- cancellation during command read still works.
- no unhandled `ERROR` escapes past the protected step entrypoint.

## Phase 4: Logical Interrupts And Timeouts

Goal: replace process-signal-shaped backend communication with logical
interrupts that work for both processes and threads, and make timeout delivery
target logical backends rather than the whole process.

Likely changes:

- Add a backend interrupt mailbox.
- Convert signal handlers to set logical interrupt bits.
- Route cancellation, termination, config reload, notify catchup, procsignal
  barriers, and timeout events through the interrupt system.
- Split timeout registration from timeout delivery so timeout expiry records a
  target backend/execution and sets logical interrupt bits for that target.
- Preserve process-mode `SIGALRM` behavior as an implementation detail where
  useful, but do not make thread mode depend on per-backend Unix alarm signals.
- Keep `CHECK_FOR_INTERRUPTS()` as the common service point.
- Preserve existing signal delivery in process mode as an external transport.

Expected commit shape:

- one commit introducing interrupt types and mailboxes;
- focused commits replacing families of signal/procsignal uses.

Validation:

- cancellation tests;
- termination tests;
- config reload tests;
- LISTEN/NOTIFY behavior;
- statement, lock, transaction, idle-in-transaction, and idle-session timeouts;
- hot standby recovery conflict behavior where practical;
- process-mode regressions.

## Phase 5: Wait Boundary

Goal: centralize blocking waits behind a scheduler-aware boundary.

Likely changes:

- Introduce `PgWaitSpec` and `PgSuspend()` or equivalent.
- Adapt frontend command read waits.
- Adapt frontend output flush waits.
- Adapt latch and wait-event-set paths.
- Adapt condition variable and lock waits where feasible.
- Keep early native implementation blocking the current carrier.

Important rule:

All waits that can last an unbounded amount of time should become visible to
the runtime. Short bounded waits can remain local until later.

Validation:

- process-mode tests pass;
- cancellation while blocked still works;
- idle timeout and transaction timeout behavior remains correct;
- no lost wakeups in common wait paths.

## Phase 6: Backend Lifecycle And Exit

Goal: make backend termination logical so a threaded backend can exit without
terminating or corrupting the whole runtime.

Likely changes:

- Identify direct and indirect callers of `proc_exit`, `exit`, and fatal exit
  helpers in backend code.
- Split logical backend exit from process exit.
- Route `on_proc_exit`, `before_shmem_exit`, and `shmem_exit` callbacks through
  backend/runtime-aware cleanup.
- Ensure one logical backend can release resources and detach from shared state
  while other threaded backends continue.
- Preserve current process-mode behavior.
- Define which paths still escalate to runtime/process termination, especially
  `PANIC` and postmaster death.

Validation:

- normal client disconnect cleanup;
- `FATAL` during active transaction;
- repeated connect/disconnect stress in process mode;
- temporary file cleanup;
- DSM/DSA detach cleanup;
- callback ordering remains compatible in process mode.

## Phase 7: Extension Backend Model Gate

Goal: prevent unsafe extension loading in threaded mode and establish the route
for in-tree extensions.

Likely changes:

- Extend `Pg_magic_struct` with backend model metadata.
- Make default `PG_MODULE_MAGIC` process-only.
- Add explicit opt-in macros for threaded compatibility.
- Teach `dfmgr.c` to reject incompatible modules when threaded mode is active.
- Audit PL/pgSQL first.
- Define the minimum in-tree module allowlist for threaded regression tests,
  including PL/pgSQL, required encoding conversion modules, regression-test
  helper modules, and any module loaded automatically by the selected tests.
- Add per-session extension state APIs if needed by PL/pgSQL or bundled
  modules.

Suggested backend model levels:

- process only;
- thread-per-session safe;
- pooled-scheduler/task reentrant.

Validation:

- incompatible test extension is rejected in threaded mode;
- existing process mode loads extensions as before;
- PL/pgSQL loads and passes its tests when marked safe;
- the minimum in-tree module allowlist loads in threaded mode;
- contrib modules are migrated only after audit.

## Phase 8: Thread-Safety Floor

Goal: make enough backend-local state private to each logical backend that
thread-per-session launch is not sharing unsafe plain globals.

Required floor:

- current memory context state;
- current resource owner state;
- `MyProc` and `PGPROC` ownership state;
- `MyProcPort` and frontend protocol buffers;
- interrupt pending flags and interrupt holdoff counters;
- timeout pending flags and timeout registration state;
- GUC backing variables and GUC nesting state;
- error context and exception stack state;
- current transaction/session identity globals;
- temporary file and virtual fd owner state.

Likely changes:

- Use thread-local compatibility state where that preserves current
  process-per-session semantics.
- Prefer object-owned state where the ownership boundary is already clear.
- Add assertions that the current runtime/backend/session/execution pointers
  are initialized before backend-local state is accessed.
- Keep process-mode behavior unchanged.

Validation:

- process-mode full regression tests;
- static global report shows the required floor is no longer shared as plain
  mutable process globals;
- targeted tests for memory context, resource owner, GUC, interrupt, timeout,
  protocol, and fd cleanup behavior.

## Phase 9: Thread-Per-Session Runtime

Goal: run regular client backends as OS threads inside one server runtime.

Likely changes:

- Add PostgreSQL thread portability layer if not already present.
- Add `multithreaded` or equivalent experimental GUC.
- Add backend launch path that can choose process or thread.
- Initialize carrier-local state for each thread.
- Initialize current runtime/backend/session/execution pointers.
- Ensure signal masks and handlers are not incorrectly installed per thread.
- Preserve process-mode launch path.

Conservative scope:

- regular client backends first;
- auxiliary processes can remain processes initially;
- background workers can be gated off or process-only until audited;
- unsafe extensions rejected.

Validation:

- multiple concurrent SQL sessions in threaded mode;
- cancellation and termination of one threaded backend;
- connection startup and teardown;
- transaction abort and error recovery;
- basic isolation tests;
- PL/pgSQL smoke and regression tests;
- process-mode full test suite.

## Phase 10: State Migration From TLS To Objects

Goal: reduce reliance on thread-local globals so sessions can eventually move
between carriers.

Likely workstreams:

- GUC state into `PgSession` and transaction/execution state.
- memory context current state into `PgExecution` or carrier current pointers.
- resource owners split by session/transaction/task.
- protocol buffers into `PgConnection`.
- debug query string and statement metadata into `PgExecution`.
- interrupt holdoff/cancel holdoff counters into execution/backend state.
- fd cache into session-owned state plus runtime fd budget.
- cache state either session-owned or explicitly synchronized.

Validation:

- targeted tests per subsystem;
- process-mode and thread-per-session mode stay working;
- static global report shrinks over time.

## Phase 11: Pooled Carrier Scheduler

Goal: schedule logical session/execution tasks onto a smaller number of worker
threads.

Likely changes:

- Add runnable queues and wait queues.
- Add task budget handling.
- Make `PgSessionStep()` return when it reaches visible waits.
- Move blocking waits to scheduler registration.
- Wake blocked tasks through logical interrupts or wait completion.
- Keep thread-per-session mode available for debugging and fallback.

Initial limitations are acceptable:

- long executor calls may pin a carrier until they reach safe yield points;
- some subsystems may temporarily opt out and require a dedicated carrier;
- background workers can be migrated gradually.

Validation:

- many idle sessions on few carriers;
- many clients waiting on locks without many blocked OS threads;
- no lost wakeups under stress;
- cancellation of waiting and running tasks;
- process-mode and thread-per-session modes still work.

## Phase 12: Executor And Utility Yield Points

Goal: improve fairness and latency for long-running commands under pooled
scheduling.

Likely changes:

- add safe yield checks in long executor loops;
- add batch boundaries in sort/hash/materialize paths where safe;
- add COPY yield points;
- add utility command yield points for long operations;
- integrate with interrupt checks and budget accounting.

Validation:

- long query fairness tests;
- cancellation latency tests;
- no executor state corruption across yields;
- performance comparison against thread-per-session mode.

## Phase 13: Hardening

Goal: make threaded mode debuggable and credible.

Likely work:

- thread sanitizer runs where feasible;
- address sanitizer runs;
- stress tests for interrupts, waits, cancellation, and teardown;
- lock-order documentation for new runtime locks;
- debug views for runtime/backend/session/carrier state;
- crash and FATAL behavior tests;
- performance baselines.

## PL/pgSQL And In-Tree Modules Plan

PL/pgSQL should be the first nontrivial module to support threaded mode.

Approach:

- audit global and static state in `src/pl/plpgsql`;
- classify caches as session-local, runtime-global immutable, or synchronized;
- move mutable session caches into `PgSession` extension state or PL/pgSQL's own
  session-owned object;
- keep process mode behavior unchanged;
- mark PL/pgSQL thread-per-session safe only after tests pass.

Contrib modules should be handled after the mechanism is proven:

- start with simple stateless modules;
- reject or defer modules with background workers, process-global caches, or
  unsafe external library assumptions;
- document each opt-in.

## Test Strategy

Process mode remains the control group.

Minimum validation after major phases:

- build;
- core regression tests;
- targeted TAP tests for touched areas;
- isolation tests for lock/wait changes;
- extension load tests;
- PL/pgSQL tests once extension gating exists.

Threaded mode validation begins when thread launch exists:

- smoke test multiple concurrent clients;
- cancellation of running query;
- termination of idle and active backend;
- error recovery after `ERROR`;
- transaction abort cleanup;
- LISTEN/NOTIFY if logical interrupts have reached that path;
- PL/pgSQL smoke tests;
- repeated connect/disconnect stress.

Later scheduler validation:

- many idle sessions on limited carriers;
- lock wait queues with cancellation;
- output backpressure;
- timeouts while waiting;
- no lost wakeups across repeated suspend/resume cycles.

## Risk Register

### Top-Level Error Recovery

Risk: moving `PostgresMain()` state breaks `ERROR` recovery or protocol sync.

Mitigation:

- preserve the always-active top-level `sigsetjmp` boundary initially;
- make `PgSessionStep()` the protected public entrypoint and keep unprotected
  helpers private;
- extract recovery code with minimal semantic changes;
- add targeted protocol error tests.

### Hidden Mutable Globals

Risk: thread mode corrupts session state through unclassified globals.

Mitigation:

- annotate globals early;
- use static reports;
- reject unclassified mutable globals in new code;
- prefer thread-local transition wrappers before object migration.

### Lost Wakeups

Risk: replacing signals/latches introduces race conditions.

Mitigation:

- use atomic interrupt bits;
- document clear-before-check wait patterns;
- stress wait/cancel paths;
- borrow proven patterns from Heikki's interrupt work.

### Extension Unsafety

Risk: arbitrary extensions use mutable statics or unsafe libraries.

Mitigation:

- default extensions to process-only;
- add explicit module metadata;
- provide session-state APIs;
- migrate PL/pgSQL and selected in-tree modules first.

### `PGPROC` Ownership

Risk: `PGPROC` currently conflates backend identity, lock waiting, proc array
membership, and transaction visibility.

Mitigation:

- in early thread-per-session mode, keep one `PGPROC` per logical backend;
- only later split execution/transaction leasing from idle session identity.

### File Descriptor Accounting

Risk: per-session fd caches in one process exceed process-wide fd limits.

Mitigation:

- add runtime-level fd budget accounting before threaded mode is broadly used;
- keep virtual fd state session-local until sharing is audited.

### Cache Sharing

Risk: making relcache/catcache shared too early creates subtle races.

Mitigation:

- keep cache state session-local first;
- rely on existing invalidation semantics;
- optimize sharing later only with clear locking rules.

### Signal Semantics

Risk: process-level signals do not map cleanly to threaded backend identities.

Mitigation:

- signals become external delivery only;
- backend-to-backend communication uses logical interrupts;
- thread mode avoids per-backend Unix signal handling.

### Backend Exit

Risk: a threaded backend follows a process-exit path and terminates the whole
runtime or leaves backend-local resources attached.

Mitigation:

- split backend exit from process exit before thread launch;
- route exit callbacks through backend-aware cleanup;
- reserve process termination for process mode, postmaster death, or `PANIC`;
- stress repeated connect/disconnect and `FATAL` paths.

## Suggested Commit Sequence After Documentation

1. Add state object declarations and process-mode no-op initialization.
2. Add global lifetime annotations and initial static report tooling.
3. Extract `PostgresMain()` error recovery into a named helper.
4. Extract session bootstrap from `PostgresMain()`.
5. Extract one-command loop state into a session main-loop struct.
6. Add `PgSessionStep()` while process mode still loops synchronously.
7. Introduce logical interrupt structs and bridge signal handlers to them.
8. Convert timeout delivery to target logical backends.
9. Split backend exit cleanup from process exit.
10. Add extension backend model metadata and process-only default.
11. Establish the minimum in-tree module allowlist.
12. Make the thread-safety floor private through TLS or object ownership.
13. Add thread portability layer and backend launch switch.
14. Run first thread-per-session backend smoke tests.

Each commit should leave process mode buildable. Prefer temporary compatibility
wrappers to broad all-at-once rewrites.
