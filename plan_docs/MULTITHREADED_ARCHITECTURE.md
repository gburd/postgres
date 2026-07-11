# Multithreaded PostgreSQL Architecture

This document describes the target architecture for an experimental branch
that makes PostgreSQL capable of running user backends in a multithreaded
runtime. The intent is to aim for the "holy grail" shape discussed in the
PgConf.dev 2025 multithreading talk: make session and execution state explicit
so PostgreSQL can map work to processes, threads, or a later scheduler.

North star (revised 2026-07-11): the goal is not merely "threading works" with
libxtc as a swappable substrate, but to FUSE the runtime with libxtc deeply
enough that the threaded branch matches and then beats the fork model on
throughput.  Session-5 baselines put threaded at ~65 % of process (the cost is
our own per-command scheduling + current-work indirection layered on top of
libxtc, plus hand-rolled pthread plumbing duplicating libxtc facilities).  The
plan is therefore to adopt libxtc's OTP behaviours (xtc_svr, xtc_orc, xtc_pool,
xtc_fsm, the crash-aware registry, xtc_pg, xtc_xproc, xtc_credit) as first-class
runtime building blocks and to dedup PostgreSQL's Latch/LWLock/CV/AIO onto xtc
primitives where it is a net win -- one behaviour at a time, each A/B-measured
and kept only if neutral-or-better.  Process mode remains a permanently
supported backend model and the process-lifetime exceptions stand; this is
aggressive fusion, not a blind cutover.  See the "North star" section in
MULTITHREADED_PLAN.md for the concrete adoption list and the promoted Phase 18.

This is not a short upstream patch plan. It is a north-star design for a single
ambitious branch.

## Phase 14/15 Superseding Note

`MULTITHREADED_PROTOCOL_SCHEDULER_DESIGN.md` is the authoritative scheduler
design for Phase 14 and Phase 15. Those phases schedule only top-level frontend
protocol input before the next message type byte is consumed.

In Phase 14/15:

- deep waits remain observable but carrier-pinned;
- `PgSuspend()` publishes wait metadata and invokes the blocking callback on the
  same carrier;
- frontend output backpressure, COPY continuations, lock/LWLock/condition
  variable/latch detachment, executor/utility yields, and AIO/storage detach are
  deferred to Phase 17 or later;
- Phase 14 proves protocol park/resume correctness;
- Phase 15 proves the real bounded carrier pool and sessions-greater-than-
  carriers validation.

Older sections below that discuss broader run-to-wait scheduling are retained as
long-term architecture, not as Phase 14/15 implementation instructions.

## Goals

- Keep the existing multiprocess backend model working.
- Add a native thread-per-session mode as the first threaded runtime.
- Create an internal state model that separates runtime, carrier, backend,
  session, connection, and execution state.
- Unwind `PostgresMain()` enough that the backend main loop can be stepped,
  suspended, resumed, and eventually scheduled.
- Replace process-signal-oriented backend communication with logical
  interrupts and explicit wait/wakeup objects.
- Provide an extension compatibility model where existing third-party
  extensions can remain process-only, while audited in-tree modules can opt
  into threaded mode.
- Build toward a scheduler that can run many logical sessions/executions on a
  smaller pool of carriers.

## Non-Goals For The First Working Threaded Prototype

- Do not make every executor node independently schedulable.
- Do not require green-thread stack copying.
- Do not require shared relcache/catcache/fd-cache designs on day one.
- Do not make third-party C extensions thread-safe by default.
- Do not make WASM the primary target. The architecture should avoid native
  assumptions that would make a later host-driven runtime impossible, but the
  first implementation target is native PostgreSQL threading.
- Do not remove the multiprocess backend model.

## Existing Starting Points

PostgreSQL already has a small `Session` abstraction in
`src/include/access/session.h`. Today it exists to share a session-scoped DSM
and DSA area between a parallel query leader and workers. Its comments already
say it could include state that is currently global. This branch should treat
that as the seed of the broader session model unless implementation pressure
shows that a new type is cleaner.

The initial mechanical rule is: introduce the broader object as `PgSession` and
embed the existing `Session` inside it. Do not immediately rename the existing
type or overload `CurrentSession` with a different meaning. Once enough state
has moved, the older `Session` type can either be folded into `PgSession` or
renamed in a dedicated cleanup.

`PostgresMain()` in `src/backend/tcop/postgres.c` is currently doing too many
jobs:

- backend signal setup;
- base backend initialization;
- database/session initialization;
- frontend protocol startup messages;
- top-level error boundary;
- error recovery;
- per-command memory reset;
- idle state reporting and timeout setup;
- blocking command read;
- interrupt and config processing;
- protocol message dispatch;
- transaction and portal cleanup after errors.

The threaded architecture should split those responsibilities into stateful
session/runtime operations without changing behavior first.

Heikki Linnakangas's threading branch is the best reference implementation for
several near-term mechanics:

- global lifetime annotations;
- thread-local transitional globals;
- a `multithreaded` GUC;
- thread launch from backend launch paths;
- logical interrupts replacing many signal and latch uses;
- extension module gating for threaded compatibility.

This branch should reuse ideas and adapt code carefully, but should not merge
that branch wholesale.

## Core Object Model

### PgRuntime

`PgRuntime` represents a running PostgreSQL server runtime inside one address
space. In threaded mode, many user backends share one `PgRuntime`. In process
mode, each process may have a local runtime facade around existing shared
memory and process-private state.

Responsibilities:

- backend registry;
- logical backend id and cancel-key allocation;
- scheduler ownership;
- runtime-wide interrupt routing;
- host OS operations and wait integration;
- shared memory and DSM/DSA policy;
- global configuration defaults;
- postmaster/supervisor state where applicable;
- thread/process/carrier lifecycle.

`PgRuntime` is not a replacement for PostgreSQL shared memory. Shared memory
remains the compatibility boundary for process mode. In threaded mode, the
runtime may access the same data more directly, but the logical shared-memory
APIs should remain intact until there is a deliberate reason to change them.

### PgCarrier

`PgCarrier` is the physical execution vehicle.

Examples:

- a postmaster child process in process mode;
- an OS thread in thread-per-session mode;
- one worker in a future carrier pool;
- a later host-scheduler callback.

Carrier state should be small and physical:

- native thread/process handle;
- stack base and stack checks;
- current signal mask or thread interrupt mask;
- carrier-local scratch state;
- current runtime/backend/session/execution pointers;
- thread name and debugging identity.

Carrier state must not become a dumping ground for SQL session state. If a
value should survive movement to another carrier, it belongs in `PgBackend`,
`PgSession`, or `PgExecution`.

### PgBackend

`PgBackend` is the logical backend identity. It is what cancellation,
statistics, monitoring, and lock participation should target.

Responsibilities:

- stable backend id and pid-like externally visible identity;
- cancel key;
- backend type;
- activity/stats identity;
- interrupt mailbox;
- connection/session association;
- lifecycle state;
- ownership of, or reference to, `PGPROC` in early phases.

In the first thread-per-session implementation, a `PgBackend` may still own a
`PGPROC` for its whole lifetime. The target architecture should move toward
leasing transaction/lock participation to `PgExecution` so idle sessions do not
need all heavyweight transaction resources permanently.

### PgSession

`PgSession` is SQL session state.

Responsibilities:

- database, role, auth, and namespace state;
- session GUC values and reporting state;
- prepared statements;
- portals and cursors that outlive a single command;
- temp namespace and temp resource state;
- session memory contexts;
- session resource owner;
- session-local caches that cannot yet be shared safely;
- in-tree extension session state;
- session DSM/DSA state currently represented by `Session`.

The existing `Session` struct should either evolve into this object or be
embedded inside it during transition.

### PgConnection

`PgConnection` represents frontend/backend protocol transport.

Responsibilities:

- protocol version and message framing;
- input and output buffers;
- connection status;
- TLS/GSS/security state;
- socket fd or equivalent native transport handle;
- flush and backpressure state;
- protocol sync-loss state.

`Port` already holds some connection state. The long-term shape should make the
connection/session split explicit: a session usually has one connection, but
the architecture should not make that inseparable.

### PgExecution

`PgExecution` represents active work being run for a session.

Responsibilities:

- current transaction/query/portal execution state;
- current memory context;
- active resource owner;
- active snapshot and command id state;
- current statement timestamps and debug query string;
- error boundary state;
- interrupt holdoff/cancel holdoff/critical-section counters;
- leased `PGPROC` or lock participant state in the long-term model.

Many current globals are really execution state. Moving them to `PgExecution`
is the work that makes pooled scheduling credible.

### PgTask

`PgTask` is the scheduler-visible unit of work. Early code may not need a
separate public struct, but the concept matters.

Examples:

- complete session bootstrap;
- read and dispatch one protocol command;
- continue a portal execution;
- perform error recovery;
- run an autovacuum or background worker step;
- flush protocol output.

The scheduler should run tasks until they complete, hit a budget, or reach an
explicit wait point.

## Current Context Pointers

Transitional multithreading will need thread-local current pointers:

```c
CurrentRuntime
CurrentCarrier
CurrentBackend
CurrentSession
CurrentExecution
```

These should be treated as compatibility pointers, not the final place for
state. New code should prefer passing explicit objects where practical.

Existing globals can first become macros or accessors over these current
objects. That reduces churn while making ownership visible.

## Main Loop Unwinding

The desired replacement for `PostgresMain()` is a protected, stateful session
runner:

```c
PgStepResult PgSessionStep(PgSession *session, PgStepBudget budget);
```

The exact names may change, but the shape should remain:

- the caller owns the outer loop;
- the public step entrypoint installs or verifies the active top-level backend
  error boundary;
- unprotected implementation helpers are private and must not be scheduler
  entrypoints;
- the session owns state that currently lives in volatile locals;
- blocking reads and waits become explicit step results or scheduler waits;
- top-level error recovery remains active and reliable;
- process mode can still call a simple loop around the step function.

Likely session states:

- `PG_SESSION_NEW`
- `PG_SESSION_BOOTSTRAPPING`
- `PG_SESSION_READY`
- `PG_SESSION_READING_COMMAND`
- `PG_SESSION_DISPATCHING_COMMAND`
- `PG_SESSION_EXECUTING`
- `PG_SESSION_SKIPPING_UNTIL_SYNC`
- `PG_SESSION_ERROR_RECOVERY`
- `PG_SESSION_TERMINATING`
- `PG_SESSION_DONE`

The first implementation should not try to rewrite all backend work into
continuation-passing style. It should split the top-level loop and preserve the
current synchronous command execution model.

### Error Boundaries

The existing `sigsetjmp` boundary in `PostgresMain()` is intentionally always
active. The first refactor should preserve that property.

Target shape:

- `PgSessionStep()` is the protected public entrypoint used by future scheduler
  callers.
- Process/thread runners may install their own persistent top-level error
  boundary and call private unprotected helpers inside that boundary.
- `PgSessionStep()` installs the top-level boundary for the duration of the
  step, or asserts that the matching session boundary is already active.
- Internal helpers such as `PgSessionStepInternal()` may assume a boundary, but
  must not be exposed to runtime or scheduler callers.
- On `ERROR`, control transfers to `PgSessionRecoverError()`.
- Recovery state that is now stored in volatile locals moves into `PgSession`
  or `PgExecution`.
- `FATAL` tears down the logical backend.
- `PANIC` tears down the runtime, as today.

Do not attempt to replace PostgreSQL error handling with return codes as part
of the first main-loop split. A backend step may return a status after recovery,
but ordinary PostgreSQL backend code may continue to use `ereport(ERROR)`.

The public step contract is:

- no unhandled `ERROR` escapes past `PgSessionStep()`;
- `FATAL` becomes logical backend termination in threaded mode;
- the session state records whether recovery has completed, whether protocol
  sync has been lost, and whether the backend must terminate;
- process mode preserves current user-visible behavior.

## Scheduler Model

### Stage 1: Process Compatibility

The new objects exist, but process mode behaves as it does today. Each process
has one active carrier, backend, session, and execution path.

### Stage 2: Thread-Per-Session

Each connected client session runs on its own OS thread. This is intentionally
close to the current process-per-session model:

- OS scheduler handles fairness;
- most command execution remains synchronous;
- thread-local compatibility globals are acceptable;
- caches and fd state can remain per session where sharing would be risky;
- extension compatibility is gated.

This is the first credible threaded milestone.

### Stage 3: Protocol-Boundary Cooperative Scheduling

Phase 14/15 implement a deliberately narrow version of pooled scheduling.
Session work runs synchronously until it returns to the top-level frontend
protocol boundary. A session step may prepare a protocol-read park only when it
is about to wait for the next frontend message type byte and no byte has been
consumed. The carrier loop performs the actual detach only after the session
step has returned.

In this stage, a task runs until it:

- completes;
- hits a step budget;
- reaches the top-level frontend input park boundary.

Other waits remain carrier-pinned and observable. They must not detach a
logical backend until a later phase gives the wait site an explicit continuation
and cleanup contract.

### Stage 4: Finer-Grained Execution Scheduling

Long-running executor work can become more cooperative by adding yield points
at safe boundaries:

- between tuple batches;
- between plan node batches;
- during sort/hash build loops;
- during COPY loops;
- during utility command loops where safe.

This is later work. The top-level architecture should not depend on it for the
first threaded backend.

## Waits, Wakeups, And Interrupts

The architecture needs one logical wait path:

```c
PgWaitResult PgSuspend(PgWaitSpec *wait);
```

In native process/thread modes, `PgSuspend()` may block the current carrier
using `WaitEventSetWait()`, `WaitLatch()`, or platform APIs. In Phase 14/15 it
remains an observable-wait API: it may publish wait metadata, but it does not
detach the logical backend or run another task. Any later design that makes a
specific deep wait scheduler-yielding must define that wait's continuation,
cleanup, and extension-safety contract explicitly.

Important wait sources for observability and later scheduler-boundary design:

- frontend command reads;
- frontend output flush and backpressure;
- TLS/GSS handshakes;
- lock waits;
- condition variable waits;
- latch waits;
- process death and postmaster death checks;
- timeouts;
- DSM/shm_mq/pqmq waits;
- AIO/storage waits;
- background worker control waits.

Logical interrupts should replace signal-specific backend communication.
Signals can remain an external delivery mechanism in process mode, but signal
handlers should translate into logical interrupt bits. In thread mode, another
backend should be able to set interrupt bits atomically and wake the target
backend without sending a Unix signal to the process.

`CHECK_FOR_INTERRUPTS()` should remain the common safety point, but the state it
examines should move from process-global variables toward the current backend
or execution.

### Timeout Delivery

Timeout handling needs explicit treatment before thread launch. PostgreSQL
currently relies heavily on process-level signal machinery for timeout
delivery. In threaded mode, timeout expiry must target a logical backend or
execution, not the whole process.

Target model:

- timeout registration records the target backend/execution;
- expiry sets logical interrupt bits for that target;
- process mode may continue using existing signal transport internally;
- thread mode must not depend on per-backend `SIGALRM` handling;
- cancellation while blocked must be delivered through the same wait/wakeup
  path as other backend interrupts.

## Global State Migration

Every mutable global must eventually be classified.

Suggested categories:

- `pg_global`: true runtime-global state, safe to share with synchronization or
  immutable after startup.
- `static_singleton`: immutable lookup tables or constants.
- `dynamic_singleton`: runtime-level mutable singleton requiring explicit
  synchronization or startup-only mutation.
- `session_local`: state that belongs in `PgSession`.
- `execution_local`: state that belongs in `PgExecution`.
- `carrier_local`: physical thread/process state.
- `connection_local`: protocol transport state.
- `shared_memory`: state already protected by shared-memory concurrency rules.

Heikki's annotation work is a good starting point, but the final architecture
should not stop at thread-local globals. Thread-local storage is a transition
tool. The goal is explicit ownership.

New mutable globals should not be added without a lifetime classification.

## Thread-Safety Floor

Thread-per-session mode must not launch until a minimum set of backend-local
state has been separated from shared process globals or made thread-local as a
temporary bridge.

The required floor includes:

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

Thread-local storage is acceptable for the first thread-per-session milestone
where it preserves current process-per-session behavior. Pooled scheduling
requires moving this state into `PgSession`, `PgExecution`, `PgConnection`, or
`PgCarrier`.

## GUCs

GUCs are one of the hardest migration areas because many options are backed by
direct C variables.

Target model:

- runtime-global defaults live in `PgRuntime`;
- session values live in a session GUC context;
- transaction-local GUC nesting lives with transaction/execution state;
- reporting state lives with the session/connection;
- legacy direct variable access is wrapped through generated or handwritten
  accessors during transition.

Thread-per-session can use thread-local GUC storage as a bridge. Pooled
scheduling needs GUC state to be owned by `PgSession`/`PgExecution`, not by the
carrier thread.

Third-party extensions that define custom GUCs by writing process-global
variables should be rejected in threaded mode unless they opt into a safe API.

## Memory Contexts And Resource Owners

Memory contexts are currently navigated through process-global current state.
Threaded mode needs the current memory context to be at least carrier-local,
and pooled scheduling needs it to be execution-local.

Proposed ownership:

- runtime contexts for runtime-global state;
- backend contexts for logical backend identity;
- session contexts for session lifetime state;
- transaction contexts for transaction lifetime state;
- message/query contexts for command lifetime state;
- carrier contexts only for physical worker scratch.

Resource owners should follow the same split:

- session resource owner for resources that survive commands;
- transaction resource owner for transaction-scoped resources;
- task/query resource owner for short-lived work;
- runtime resource owner for runtime-global resources.

Heikki's `SessionResourceOwner` work is a useful reference.

## Caches And File Descriptors

The first threaded implementation should avoid making every backend cache a
shared concurrent data structure.

Conservative path:

- keep relcache/catcache/syscache state logically session-local until audited;
- keep virtual fd state logically session-local;
- add a runtime-level fd budget allocator because OS fd limits are shared by
  all threads in a process;
- preserve existing shared-memory invalidation semantics;
- make cache sharing a later optimization.

This costs memory, but it keeps the first threaded mode close to the process
model and reduces correctness risk.

## Extension Compatibility

Existing C extensions cannot be assumed thread-safe. The threaded runtime must
gate extension loading through module metadata.

Suggested model:

- default `PG_MODULE_MAGIC` means process-backend-compatible only;
- a new module magic field advertises backend model compatibility;
- audited modules can opt into thread-per-session compatibility;
- pooled protocol compatibility must split carrier-affine sessions from
  carrier-migratable sessions;
- a stricter later flag can advertise deep-wait/task reentrancy;
- `dfmgr.c` rejects incompatible modules when the runtime is threaded.

Possible flags:

```c
PG_BACKEND_MODEL_PROCESS
PG_BACKEND_MODEL_THREAD_PER_SESSION
PG_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE
PG_BACKEND_MODEL_POOLED_PROTOCOL_MIGRATABLE
PG_BACKEND_MODEL_TASK_REENTRANT
```

The current live `PG_BACKEND_MODEL_POOLED_SCHEDULER` marker is a transitional
generic level and is too coarse for Phase 15 migration claims. Protocol
scheduler work must not treat it as equivalent to either protocol-affine or
protocol-migratable support.

In-tree modules can be migrated deliberately. PL/pgSQL should be treated as a
first-class target, not as an arbitrary third-party extension. It should gain
session-owned cache/state APIs where needed and opt into threaded mode only
after audit.

The first threaded regression target also needs a minimum in-tree module set.
That set should include PL/pgSQL, required encoding conversion modules,
regression-test helper modules needed by the selected test suite, and any
module loaded automatically by core tests. The threaded test matrix should not
silently skip required in-tree modules just because extension gating exists.

The first threaded milestone may use a conservative allowlist, but the end-state
target for this plan is broader: all contrib extensions shipped in-tree should
support thread-per-session mode, carry explicit backend model metadata, and pass
their contrib regression tests in threaded mode. Any temporary exception should
be tracked as a release-blocking gap, not hidden behind the process-only default.

Third-party extension authors need replacement APIs for common unsafe patterns:

- per-session extension state;
- per-transaction extension state;
- thread-safe custom GUC state;
- shared runtime state with explicit locks;
- lifecycle hooks for session attach/detach and backend termination.

## Backend Launch And Supervision

Today backend launch is built around process creation. Threaded mode should
introduce a logical launch path:

```c
PgBackendLaunch(PgRuntime *runtime, PgBackendStart *start);
```

The launch implementation can choose:

- forked process;
- spawned process on Windows;
- OS thread;
- future scheduler task.

This should be a runtime decision, not scattered through postmaster code.

Early threaded mode can keep auxiliary processes in their current process model
while regular client backends move to threads. That is only the first
user-backend milestone. The final normal threaded server mode should run
in-tree server-owned workers, including autovacuum and other auxiliary worker
families, as runtime-owned threaded workers rather than forked subprocesses.

Single-user mode, bootstrap mode, frontend command-line utilities,
postmaster/control-plane process lifetime, and crash-escalation paths remain
process-lifetime exceptions. Worker roles should use an explicit worker runtime
owner rather than being silently treated as SQL sessions.

## Crash And Error Semantics

Threads remove the memory-corruption containment provided by process isolation.
This branch should be honest about that tradeoff.

Initial semantics:

- `ERROR`: recover the current execution/session as today.
- `FATAL`: terminate the logical backend/session.
- `PANIC`: terminate the runtime.
- segmentation faults and memory corruption remain process-fatal in threaded
  mode.

The branch should invest in assertions, sanitizers, and targeted stress tests,
but it should not pretend to preserve process-level crash isolation inside one
address space.

## Backend Exit And Cleanup

Backend exit paths are a precondition for thread launch. PostgreSQL has many
paths that assume terminating the current backend means terminating the current
process.

Threaded mode needs a logical backend exit path for:

- `proc_exit`;
- `exit`;
- `on_proc_exit` callbacks;
- `before_shmem_exit` callbacks;
- `shmem_exit` cleanup;
- resource owner cleanup;
- DSM/DSA detach;
- temporary file cleanup;
- connection teardown;
- thread/carrier return to the runtime.

The target rule is that `FATAL` and normal backend termination clean up one
logical backend/session and return control to the runtime. They must not tear
down the entire process unless the runtime is process-mode or the error level
requires `PANIC`.

## Portability

PostgreSQL should hide thread APIs behind its own portability layer rather than
sprinkling raw pthread calls through backend code.

The abstraction should cover:

- thread creation and join/detach;
- mutexes and condition variables;
- thread-local storage during transition;
- thread naming;
- signal mask behavior;
- platform-specific wakeups.

C11 threads are not assumed to be sufficient across PostgreSQL's supported
platforms. Heikki's branch and PostgreSQL's existing portability style should
guide this layer.

## Host-Driven Runtime Constraint

The architecture should keep a later host-driven runtime possible, but this is
not the main implementation target.

The practical constraint is simple: do not bury all progress behind an infinite
backend loop or unabstracted blocking waits. The same `PgSessionStep()` and
`PgSuspend()` boundaries needed for a pooled native scheduler are enough to
keep a future host-integrated port viable.

## Definition Of Done For The First Threaded Milestone

A credible first milestone is:

- process mode still passes normal regression tests;
- threaded mode can accept multiple client connections;
- each client session runs on a separate OS thread;
- backend cancellation and termination work through logical interrupts;
- PL/pgSQL works in threaded mode;
- unsafe third-party extensions are rejected by module metadata;
- common SQL regression tests pass in threaded mode with a conservative set of
  enabled extensions;
- the code has a clear path from thread-per-session to pooled carriers.
