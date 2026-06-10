# Multithreaded PostgreSQL Architecture

This document describes the target architecture for an experimental branch
that makes PostgreSQL capable of running user backends in a multithreaded
runtime. The intent is to aim for the "holy grail" shape discussed in the
PgConf.dev 2025 multithreading talk: make session and execution state explicit
so PostgreSQL can map work to processes, threads, or a later scheduler.

This is not a short upstream patch plan. It is a north-star design for a single
ambitious branch.

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

The desired replacement for `PostgresMain()` is a stateful session runner:

```c
PgStepResult PgSessionStep(PgSession *session, PgStepBudget budget);
```

The exact names may change, but the shape should remain:

- the caller owns the outer loop;
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

- `PgSessionRun()` or `PgTaskRunWithErrorBoundary()` installs the top-level
  boundary.
- On `ERROR`, control transfers to `PgSessionRecoverError()`.
- Recovery state that is now stored in volatile locals moves into `PgSession`
  or `PgExecution`.
- `FATAL` tears down the logical backend.
- `PANIC` tears down the runtime, as today.

Do not attempt to replace PostgreSQL error handling with return codes as part
of the first main-loop split.

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

### Stage 3: Run-To-Wait Cooperative Scheduling

Session work runs on a pool of carriers. A task runs until it:

- completes;
- hits a step budget;
- waits for frontend input;
- waits for output backpressure;
- waits on a lock;
- waits on a latch/interrupt;
- waits on timeout;
- waits on AIO or storage completion.

This stage requires explicit wait objects and wakeups. It does not require
executor stack capture or green threads.

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

In early native process/thread modes, `PgSuspend()` may block the current
carrier using `WaitEventSetWait()`, `WaitLatch()`, or platform APIs. In a later
pooled scheduler, it registers the wait, marks the task blocked, and runs
another task.

Important wait sources:

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
- a stricter later flag can advertise pooled-scheduler/task reentrancy;
- `dfmgr.c` rejects incompatible modules when the runtime is threaded.

Possible flags:

```c
PG_BACKEND_MODEL_PROCESS
PG_BACKEND_MODEL_THREAD_SESSION
PG_BACKEND_MODEL_TASK_REENTRANT
```

In-tree modules can be migrated deliberately. PL/pgSQL should be treated as a
first-class target, not as an arbitrary third-party extension. It should gain
session-owned cache/state APIs where needed and opt into threaded mode only
after audit.

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
while regular client backends move to threads. The target design should allow
auxiliary roles to become runtime tasks later, but that is not required for the
first user-backend milestone.

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
