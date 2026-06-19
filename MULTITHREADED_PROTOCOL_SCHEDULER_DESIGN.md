# Multithreaded Protocol-Boundary Scheduler Design

This document defines a conservative pooled-carrier scheduler design for the
multithreaded PostgreSQL branch. It deliberately does not assume that the
current Phase 14 prototype is the right implementation base. It is valid to
hard reset to a Phase 13 checkpoint and cherry-pick only the parts that match
this design.

The central decision is:

```text
Only top-level frontend protocol waits are scheduler-yielding in the first
pooled scheduler phase.
```

Backend execution, backend IO, lock waits, LWLock waits, condition variable
waits, checkpoint waits, executor waits, extension code, and most output flush
paths remain synchronous and carrier-pinned until each has an explicit
continuation design.

## Goals

- Run many mostly-idle client sessions on fewer physical carrier threads.
- Preserve PostgreSQL's existing synchronous execution model for active
  commands.
- Allow an idle session to be woken by frontend input, backend logical events,
  async `LISTEN`/`NOTIFY`, timeout expiry, termination, or postmaster death.
- Avoid any design that requires suspending and later resuming an arbitrary C
  call stack.
- Keep process mode and thread-per-session mode viable throughout the work.
- Retain Phase 13 wait-completion observability for deep waits, without
  treating those waits as carrier-yielding.
- Leave a clear path to later output-backpressure and selected deep-wait
  continuations.

## Non-Goals

- Do not make every `PgSuspend()` wait release a carrier.
- Do not make LWLocks, heavyweight locks, condition variables, checkpoint waits,
  storage IO, or `pg_sleep()` scheduler-yielding in Phase 14A.
- Do not introduce stack copying, fibers, or green-thread stack switching.
- Do not require executor-wide continuation-passing style.
- Do not make third-party C extensions pooled-scheduler reentrant by default.
- Do not assume that thread-per-session compatibility implies safe movement
  between carriers.
- Do not pin a carrier to a session for a whole transaction as a correctness
  rule.
- Do not remove thread-per-session mode.

## Definitions

### Carrier

A physical execution vehicle: an OS thread in native threaded modes, a process
in process mode, or a future host scheduler callback.

### Logical Backend

The backend identity that cancellation, stats, lock participation, interrupts,
timeouts, and `pg_stat_activity` target.

### Session

SQL session state: database, role, GUC state, prepared statements, portals,
temp state, transaction/session-owned resources, and frontend protocol state.

### Execution

The active command or transaction work currently running for a session.

### Observable Wait

A wait that publishes metadata and can be inspected or marked ready, but still
blocks the current carrier while the C stack remains live.

### Scheduler-Yielding Wait

A wait where the logical backend detaches from the carrier, returns normally to
the scheduler, and later resumes by re-entering a known top-level continuation.

### Carrier-Pinned Work

Any work that has a live PostgreSQL C stack that must continue in-place after a
wait returns.

## Core Invariant

```text
A detached logical backend must not have a live C stack on any carrier.
```

This invariant is stronger than "the backend is not currently running". It
means no deep PostgreSQL frame can be waiting to resume after carrier detach.

Therefore, the first scheduler-yielding boundary is only the top-level frontend
protocol wait before any new message byte has been consumed.

## Scheduler Boundary

The Phase 14A scheduler boundary is:

```text
ReadyForQuery / idle protocol loop -> attempt to read next frontend message
type byte -> no byte available -> park logical backend.
```

This boundary is safe because:

- the prior command has completed or the session is at an explicit
  idle-in-transaction protocol boundary;
- the backend has returned to the top-level session loop;
- no executor, lock manager, storage, or extension stack needs to resume;
- no partial frontend message has been consumed;
- the continuation is simply "re-enter the session step and try again".

The scheduler must not park after reading the frontend message type byte unless
the entire message read and dispatch path has an explicit resumable protocol
state. Phase 14A should avoid that problem.

## Wait Classification

### Scheduler-Yielding In Phase 14A

Only:

- top-level frontend input wait before consuming the next protocol message
  type byte.

The wait sources attached to this parked state are:

- frontend socket readable;
- backend logical interrupt/latch wake;
- async notify pending;
- catchup/config/proc-signal work;
- cancel/die;
- idle session timeout;
- idle-in-transaction session timeout;
- transaction timeout if active while idle;
- idle stats update timeout if needed;
- postmaster death.

### Observable But Carrier-Blocking In Phase 14A

- frontend output backpressure;
- latch waits below command execution;
- `pg_sleep()`;
- heavyweight/advisory lock waits;
- condition variable waits;
- semaphore-backed waits;
- LWLocks;
- buffer content locks;
- CLOG/ProcArray group update waits;
- checkpoint waits;
- storage IO and WAL flush waits;
- background worker and auxiliary control-plane waits.

These waits may publish Phase 13 wait-completion records. They may be
cancellable, visible in tests, and useful for diagnostics. They must not
enqueue a detached logical backend unless a later phase gives the wait site an
explicit continuation.

### Potential Later Scheduler Boundaries

- frontend output backpressure, once `PgConnection` owns enough output state to
  return `would block` and resume flushing;
- COPY protocol input/output states, if represented as explicit session
  continuations;
- selected lock waits, only with caller-specific retry/continuation state;
- selected executor loops, only at batch or node boundaries;
- AIO/storage completion, only where the upper stack can return to a known
  continuation.

## Idle Transaction Semantics

`idle in transaction` is still a top-level protocol boundary. It can be parked
without pinning a carrier, provided the logical backend/session owns all state
that survives the park:

- transaction state;
- snapshots and command ids;
- locks;
- `PGPROC` identity and wait-visible fields;
- resource owners;
- GUC nesting;
- timeout registrations;
- pending notifies;
- temp namespace/resources;
- session memory contexts;
- protocol state.

The carrier must not own any of that state as a condition of correctness. A
carrier may be preferred for locality, but it must be replaceable.

## LISTEN/NOTIFY

`LISTEN`/`NOTIFY` should be a first-class Phase 14A case.

A listening session parked at top-level protocol input is waiting for more than
client input. It must also wake when another backend commits a matching
`NOTIFY`.

Expected flow:

1. Session executes `LISTEN`.
2. Session reaches the idle protocol boundary and parks.
3. Another backend commits a matching `NOTIFY`.
4. The listening backend receives a logical notify/proc-signal interrupt.
5. The scheduler marks the parked backend runnable.
6. A carrier attaches the backend.
7. The top-level session step processes pending notify state.
8. `NotificationResponse` is sent to the client.
9. The session returns to the idle protocol boundary and may park again.

If notification output blocks, Phase 14A may keep the carrier pinned while
flushing. Output backpressure can become a later scheduler boundary only after
the output path is made explicitly resumable.

### Notify Delivery Eligibility

`LISTEN`/`NOTIFY` wakeup and `NotificationResponse` delivery are not the same
thing.

A parked backend may receive a notify-related logical wake while it is
`idle in transaction`. In that state PostgreSQL must not deliver async
notifications to the client yet. The scheduler must avoid turning this into a
busy loop.

Required behavior:

- if a parked backend is not in a transaction block, a notify wake makes it
  runnable so the top-level session step can process and flush notifications;
- if a parked backend is `idle in transaction`, a notify wake may mark
  backend/session state pending, but it must not repeatedly enqueue the backend
  solely to attempt delivery;
- when the client later sends `COMMIT`, `ROLLBACK`, or another command that
  changes transaction state, normal top-level processing decides whether
  pending notifications can be delivered;
- cancel, terminate, timeout, postmaster death, and frontend input remain
  serviceable wake reasons even while `idle in transaction`.

## Carrier Affinity And Grace Pinning

Correctness must not depend on a session returning to the same carrier after it
parks. Performance may benefit from preferring the same carrier.

Phase 14A should support soft carrier affinity:

- record the last carrier that ran a logical backend;
- when the backend wakes, prefer that carrier if it is idle or cheap to wake;
- allow any carrier to run the backend if the preferred carrier is busy;
- do not hold a carrier indefinitely for an idle session.

An optional optimization is a short grace pin:

- after `ReadyForQuery`, a carrier may wait briefly for the same session's next
  frontend message before returning to the general pool;
- the grace timeout should be short and configurable or at least easy to tune;
- the carrier must leave the grace state on scheduler pressure, shutdown,
  timeout, or logical interrupt;
- idle-in-transaction sessions must not monopolize carriers beyond the grace
  window.

This separates the correctness model from cache-locality policy:

```text
Correctness: parked sessions are movable.
Performance: recently active sessions have carrier affinity.
```

## Session Migration Compatibility

Idle protocol parking makes a session movable between carriers. That is a
weaker requirement than arbitrary task reentrancy, but it is still stronger
than thread-per-session compatibility.

A module or subsystem is thread-per-session compatible if it can run safely
when each session owns one stable carrier thread. It is session-migratable only
if it can also tolerate this sequence:

```text
statement N runs on carrier A
session parks at top-level protocol input
statement N+1 runs on carrier B
```

Phase 14A must distinguish those properties.

Required rule:

```text
A session may migrate only if all loaded modules and enabled subsystems are
session-migratable, or if their carrier-local state has explicit attach/detach
hooks.
```

Unsafe sessions must have a fallback:

- hard-affine to the original carrier;
- excluded from pooled scheduler mode;
- or rejected at module load/runtime configuration time.

The exact flag names can change, but the compatibility model needs levels like:

```c
PG_BACKEND_MODEL_THREAD_PER_SESSION
PG_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE
PG_BACKEND_MODEL_POOLED_PROTOCOL_MIGRATABLE
PG_BACKEND_MODEL_TASK_REENTRANT
```

`PG_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE` means the session may use the pooled
runtime but cannot migrate carriers. It can still benefit from common logical
interrupt and wait-observability machinery, but it does not contribute to the
"many sessions on fewer carriers" goal while idle.

`PG_BACKEND_MODEL_POOLED_PROTOCOL_MIGRATABLE` means the session can detach only
at the top-level protocol boundary and later run on any carrier.

Examples of state that must not be carrier-local for migratable sessions:

- session GUC backing variables;
- memory context current pointers;
- resource owner current pointers;
- error context and exception stack state;
- `MyProc`, `MyLatch`, and interrupt latch identity;
- frontend protocol buffers;
- extension session caches;
- custom GUC backing variables;
- callback-local "current session" caches.

This section is intentionally conservative. A compatibility mistake here
creates silent cross-session corruption, not just a scheduling inefficiency.

## Runtime Object Responsibilities

### PgRuntime

Owns the scheduler, runnable queues, carrier pool, backend registry, logical
interrupt routing, and runtime-wide policy.

For Phase 14A it should provide:

- scheduler initialization;
- carrier pool startup/shutdown;
- runnable queue;
- parked protocol-wait queue or indexed wait set;
- timeout tracking for parked sessions;
- socket readiness dispatch;
- logical wake dispatch;
- carrier affinity policy.

### PgCarrier

Owns physical execution state:

- native thread handle;
- scheduler wait latch or wake fd;
- carrier-local scratch memory context;
- currently attached backend/session/execution pointers;
- optional last-run metadata for diagnostics.

It must not own SQL session state.

### PgBackend

Owns logical backend identity:

- backend id and visible signal/stat pid;
- interrupt mailbox;
- cancel key;
- backend type;
- `PGPROC` ownership or lease in early phases;
- stats/activity identity;
- wait-completion publication state;
- parked scheduler state.

In Phase 14A, `PGPROC` can remain backend-lifetime. Later phases may move
toward execution leasing, but that is not required for the first protocol
scheduler.

### PgSession

Owns SQL session and protocol-loop state:

- idle/running/error recovery state;
- protocol sync state;
- session GUC state;
- prepared statements and portals;
- temp state;
- session memory contexts;
- current connection pointer.

### PgConnection

Owns frontend transport state:

- socket;
- TLS/GSS state;
- input buffer;
- output buffer;
- message framing state;
- connection lost/sync lost state.

Phase 14A requires one precise guarantee:

```text
The scheduler may park only when no frontend message read is active.
```

### PgExecution

Owns active command state. In Phase 14A, while an execution is active, the
backend is carrier-pinned.

## Attach/Detach Invariants

Carrier attach and detach must be specified as a contract, not inferred from
current-pointer side effects.

### On Attach

Before a carrier calls into `PgSessionStep()` for a backend:

- `CurrentPgRuntime`, `CurrentPgCarrier`, `CurrentPgBackend`,
  `CurrentPgSession`, `CurrentPgConnection`, and `CurrentPgExecution` identify
  the attached work;
- compatibility globals and TLS mirrors are reloaded from backend/session/
  connection/execution state;
- `MyProc` and `MyProcNumber` identify the logical backend's `PGPROC`;
- `MyLatch` and the backend interrupt latch point at a wake object valid for
  the attached carrier/backend combination;
- `FeBeWaitSet`, if present, references the current connection socket and the
  correct latch;
- memory-context current pointers are valid for the attached execution/session;
- resource-owner current pointers are valid for the attached execution/session;
- timeout state targets the logical backend, not the carrier;
- wait-event reporting points at the logical backend's wait state;
- the backend is in `RUNNING` scheduler state and in no parked/runnable queue.

### On Detach

Before a carrier returns to the scheduler after protocol parking:

- no PostgreSQL C stack below `PgSessionStep()` remains live for that backend;
- no frontend message read is active;
- no carrier-owned memory context is referenced by session/backend state;
- `FeBeWaitSet` is either detached from the carrier or known to be safe to
  reinstall on the next carrier;
- `MyLatch`/interrupt-latch ownership is represented by the parked scheduler
  wake record or by a backend-owned logical wake object;
- timeout registrations remain associated with the logical backend/session;
- `PGPROC`, locks, transaction state, and stats identity remain owned by the
  logical backend;
- current pointers on the carrier are cleared or set to scheduler-only values;
- TLS/global mirrors that could be observed by carrier scheduler code are not
  left pointing at the detached session;
- the backend is in `PARKED_PROTOCOL_READ` scheduler state and appears in
  exactly one parked wait structure.

### Forbidden States

The implementation should assert against:

- a backend in a scheduler queue while still attached to a carrier;
- a detached backend with an active frontend message read;
- a detached backend with a deep wait-completion record treated as runnable
  scheduler state;
- a carrier running scheduler code while `CurrentPgBackend` still points at a
  detached backend;
- two carriers simultaneously attached to the same backend;
- stale `FeBeWaitSet` latch entries after carrier migration.

## Scheduler State Machine

### Backend Scheduler States

Suggested states:

- `DETACHED`: backend is not owned by scheduler queues and is not running;
- `RUNNABLE`: backend is queued to run;
- `RUNNING`: backend is attached to a carrier;
- `PARKED_PROTOCOL_READ`: backend is detached at top-level protocol input;
- `EXITING`: backend is tearing down and must not be dispatched.

Avoid a generic `WAITING` state unless it is explicitly qualified. Deep
observable waits should not be represented as scheduler waiting.

### Step Results

`PgSessionStep()` should return a small set of scheduler-visible outcomes:

- `CONTINUE`: made progress and may be called again;
- `PARKED_PROTOCOL_READ`: detached safely at top-level frontend input;
- `ERROR_RECOVERED`: recovered from `ERROR`; scheduler may continue or exit
  depending on session state;
- `DONE`: session exited normally;
- `FATAL_EXIT`: logical backend is terminating.

The exact enum names can differ, but `PARKED_PROTOCOL_READ` should not be
named like a generic wait.

## Phase 14A Control Flow

### Carrier Loop

1. Pop a runnable backend.
2. Attach backend/session/connection/execution current pointers.
3. Run `PgSessionStep()` with a small budget.
4. If the step returns `PARKED_PROTOCOL_READ`, detach and wait for readiness.
5. If the step returns `CONTINUE`, requeue or continue based on budget.
6. If the step returns `DONE`/`FATAL_EXIT`, perform logical backend cleanup.
7. If no backend is runnable, wait on scheduler wake sources.

### Session Step

1. Preserve the top-level error boundary.
2. If resuming from a parked wake, service the wake reason before deciding
   whether to park again:
   - socket-readable wakes may proceed to the nonblocking message-byte probe;
   - socket-close/error wakes must detect disconnect;
   - logical interrupt wakes must run the top-level interrupt/config/catchup
     path that is legal while idle;
   - notify wakes must respect notify delivery eligibility;
   - timeout wakes must process the expired logical timeout;
   - proc-die and postmaster-death wakes must terminate promptly.
3. Finish prior-command idle work:
   - process pending notify if appropriate;
   - report stats;
   - report changed GUCs;
   - send `ReadyForQuery` if needed;
   - arm idle timeouts.
4. Mark command-read state.
5. Attempt nonblocking read of the next frontend message type byte.
6. If no byte is available:
   - publish a protocol-park wait;
   - clear/adjust interrupt holdoff as needed;
   - return `PARKED_PROTOCOL_READ`;
   - do not leave a partial message read active.
7. If a byte is available:
   - disable idle timers as today;
   - process interrupts;
   - finish reading and dispatch the message synchronously;
   - remain carrier-pinned until the command returns to top level.

## API Sketch

Names are provisional. The important point is to separate protocol parking from
generic wait publication.

### Protocol Park

```c
typedef enum PgProtocolParkWake
{
	PG_PROTOCOL_WAKE_SOCKET_READABLE = 1 << 0,
	PG_PROTOCOL_WAKE_SOCKET_CLOSED   = 1 << 1,
	PG_PROTOCOL_WAKE_INTERRUPT       = 1 << 2,
	PG_PROTOCOL_WAKE_TIMEOUT         = 1 << 3,
	PG_PROTOCOL_WAKE_POSTMASTER_DEATH = 1 << 4,
} PgProtocolParkWake;

typedef struct PgProtocolParkSpec
{
	PgBackend  *backend;
	PgSession  *session;
	PgConnection *connection;
	pgsocket	socket;
	TimestampTz timeout_at;
	uint32		wake_mask;
	uint32		wait_event_info;
	uint64		generation;
} PgProtocolParkSpec;

bool PgBackendParkProtocolRead(PgProtocolParkSpec *spec);
void PgBackendUnparkProtocolRead(PgBackend *backend, uint32 wake_events);
```

`PgBackendParkProtocolRead()` may detach the backend from the current carrier.
It must assert that:

- the backend is in pooled scheduler mode;
- no protocol message read is active;
- the session is in top-level command read state;
- no execution stack is active below `PgSessionStep()`;
- the backend is not already in a scheduler queue;
- the connection socket is valid or the wake is purely interrupt/timeout based.

### Observable Wait

`PgSuspend()` remains the wait-observability API:

```c
int PgSuspend(const PgWaitSpec *wait_spec,
			  PgSuspendCallback callback,
			  void *callback_arg);
```

In Phase 14A, `PgSuspend()` may publish a wait-completion record, but it must
not detach the logical backend. It invokes the callback on the same carrier and
returns to the same C stack.

### Scheduler Dispatch

```c
PgBackend *PgRuntimeSchedulerPopRunnable(PgRuntime *runtime,
										 PgCarrier *carrier);
void PgRuntimeSchedulerEnqueueRunnable(PgBackend *backend,
										PgSchedulerWakeReason reason);
void PgRuntimeSchedulerParkProtocolRead(PgBackend *backend,
										const PgProtocolParkSpec *spec);
uint32 PgRuntimeSchedulerWait(PgRuntime *runtime,
							  PgCarrier *carrier,
							  long max_wait);
```

The scheduler should only inspect scheduler records, not arbitrary
wait-completion records, when deciding which backend can detach and later run.

## Parked Wake Service Contract

A wake is not complete when the scheduler marks a backend runnable. It is
complete only after the reattached session has serviced or deliberately
deferred the wake reason.

The scheduler must record enough wake metadata for the session step to know why
it was resumed:

- socket readable;
- socket close/error;
- logical interrupt;
- notify;
- timeout;
- proc die;
- postmaster death;
- scheduler shutdown.

The session step must not immediately re-park after an interrupt, timeout, or
notify wake just because no frontend byte is readable. It must first run the
top-level service path for that wake reason. If the wake reason is not currently
serviceable, as with notify delivery during `idle in transaction`, the backend
may re-park only after recording that the pending event is deferred and should
not cause immediate requeue churn.

Each park record should carry a generation or sequence number. Wake handling
must compare and advance the generation so that:

- a wake racing with detach is not lost;
- a stale wake from a prior park does not run the backend repeatedly;
- deferred non-serviceable wakes do not create a scheduler busy loop;
- tests can assert that each wake is consumed exactly once.

## Protocol State Requirements

Protocol parking is legal only before a new frontend message starts.

The implementation needs an explicit connection/session predicate:

```c
bool PgConnectionCanParkBeforeMessage(PgConnection *connection);
```

This must be false when:

- a message type byte has been consumed but the body is incomplete;
- COPY input/output protocol is active unless COPY has an explicit continuation;
- SSL/GSS handshake state is mid-record;
- protocol sync has been lost;
- error recovery still needs to emit or consume protocol messages;
- `pq_is_reading_msg()` or an equivalent connection-local state says a message
  read is active.

The nonblocking read path should behave like:

```text
if type byte available:
    begin/finish normal synchronous message handling
else:
    park before message starts
```

It should not use a partial message as the scheduler continuation.

## Wake Sources For Parked Protocol Reads

The parked protocol wait must be woken by:

- frontend socket readability;
- socket hangup/error;
- logical backend interrupts;
- `PROCSIG_NOTIFY_INTERRUPT`;
- catchup/config invalidation;
- query cancel;
- proc die;
- timeout expiry;
- postmaster death;
- scheduler shutdown.

Wake handling should mark the backend runnable. Actual interrupt semantics
should remain in the normal top-level processing path where possible.

## Timeout Handling

Timeouts must target logical backends, not physical carriers.

For parked sessions, the scheduler should know the next relevant logical
timeout and wake the backend when it expires. On re-entry, existing timeout
processing should set the same user-visible behavior as thread-per-session
mode.

Timeouts to cover in Phase 14A:

- idle session timeout;
- idle-in-transaction session timeout;
- transaction timeout while idle;
- idle stats update timeout if retained;
- postmaster-death checks if represented as timeout-backed polling on a
  platform.

Statement and lock timeouts during active execution remain carrier-pinned and
use the normal backend path.

## Interrupt Handling

Logical interrupts must be independent of carrier identity.

When a backend is parked:

- setting an interrupt bit must wake the scheduler;
- the backend must become runnable;
- when reattached, top-level processing applies the interrupt;
- query cancel while idle remains a no-op where PostgreSQL expects that;
- proc die terminates the logical backend.

The scheduler should not process arbitrary backend interrupts itself beyond
deciding that the parked backend must run.

## Frontend Output

Phase 14A keeps frontend output carrier-pinned.

This includes:

- `ReadyForQuery`;
- `NotificationResponse`;
- query results;
- COPY output;
- error messages;
- notices.

If the socket blocks while flushing output, the carrier remains occupied. This
is acceptable for Phase 14A because output paths often have active protocol and
error semantics that are not yet represented as resumable state.

A later output-boundary phase can introduce:

- connection-owned output continuation state;
- `WOULD_BLOCK` returns from flush paths;
- scheduler parking on socket writable;
- clear handling of partial messages and errors.

## Deep Waits

Deep waits remain carrier-pinned.

Phase 13 wait-completion records should still publish:

- wait kind;
- wait event;
- socket if applicable;
- timeout metadata;
- interrupt/cancel readiness;
- owning backend/session/execution;
- diagnostic state.

But deep wait publication must not imply scheduler detach. A deep wait has a
live stack and must resume at the waiting call site.

The implementation should enforce this distinction with separate APIs or an
explicit flag:

```c
/* Observable, may block carrier. */
PgSuspend(...);

/* Scheduler-yielding, only legal at top-level protocol input. */
PgBackendParkProtocolRead(...);
```

Alternatively, `PgWaitSpec` can carry a capability flag:

```c
PG_WAIT_CAN_DETACH_CARRIER
```

Only the protocol-read boundary should set that flag in Phase 14A.

## Fairness And Scheduling Policy

The first policy should be simple and measurable.

### Runnable Order

Use FIFO runnable ordering unless profiling shows a clear reason to do
otherwise. A backend that wakes from parked protocol input goes to the runnable
queue tail. A backend that voluntarily yields after a step budget also goes to
the tail.

### Step Budget

The initial scheduler may run one frontend message per dispatch for pooled
mode:

```text
budget.max_messages = 1
```

This prevents one client with a long buffered script from monopolizing a
carrier. It also creates a clear measurement point. Later, the budget can be
raised or made adaptive for hot sessions.

### Grace Pinning Policy

Grace pinning should be optional and bounded. Suggested first policy:

- after a backend sends `ReadyForQuery`, the carrier may wait briefly on that
  backend's protocol read wake sources;
- if input arrives during the grace window, continue on the same carrier;
- if another runnable backend exists, skip or cut short the grace window;
- if the backend is idle-in-transaction, use a shorter grace window or no grace
  by default;
- if the carrier pool is undersupplied, favor pool utilization over affinity.

The policy should be instrumented with counters:

- grace waits attempted;
- grace waits hit;
- grace waits cut short by scheduler pressure;
- same-carrier resumes;
- cross-carrier resumes.

### Starvation Avoidance

Carrier affinity must not starve other runnable backends. A backend may prefer
its last carrier, but it cannot reserve that carrier while other runnable work
is waiting.

## Observability

The scheduler should expose enough internal state to prove the design:

- number of carriers;
- number of attached/running backends;
- number of parked protocol-read backends;
- runnable queue length;
- parked wake reason counts;
- same-carrier versus migrated resumes;
- protocol park/unpark counters;
- deep wait-completion counts by wait kind, explicitly not carrier-detached.

Test-only SQL functions can expose snapshots during development, but the target
architecture should not require test hooks to understand whether a backend is
parked or carrier-pinned.

## Error Handling

The top-level `sigsetjmp` boundary remains mandatory.

No unhandled `ERROR` may escape `PgSessionStep()`. On `ERROR`:

- recover using the existing top-level recovery path;
- preserve protocol sync-loss behavior;
- abort transaction state as today;
- clean per-command state;
- return a scheduler-visible recovered/error result only after recovery.

Scheduler park is not an error unwind. It must be a normal return from the
session step.

## Backend Exit

Logical backend exit must clean up one backend/session without necessarily
exiting the physical carrier.

Phase 14A needs a clear split:

- logical backend exit: close connection, release session resources, unregister
  backend, publish child exit state;
- carrier exit: terminate the physical thread during postmaster shutdown or
  pool resize;
- runtime exit: process-wide or postmaster-level shutdown.

If this split is not clean yet, it is better to keep a staging implementation
where each accepted client creates a carrier, while the design and tests focus
only on protocol parking semantics. Do not let that staging shape become the
target architecture.

## Carrier Pool

The target pooled runtime should eventually launch carriers independently of
client connections.

Phase 14A may be implemented in two steps:

1. Staging mode:
   - still launch one carrier per client;
   - prove protocol park/resume, logical wakeups, and state ownership;
   - use carrier affinity trivially because the home carrier exists.
2. Real pool mode:
   - launch a bounded carrier pool;
   - accepted clients create logical backend/session objects;
   - parked sessions do not own carriers;
   - active commands lease carriers;
   - backend exit does not imply carrier exit.

Staging mode is useful, but it is not Phase 14A completion. It is a precursor
that proves protocol parking mechanics before the true carrier pool exists.

Phase 14A completion requires real pool mode and at least one validation run
where runnable/parked client sessions outnumber carrier threads.

## Implementation Phases

### Phase 14A.0: Clean Baseline

- Start from the end of Phase 13 wait-completion work.
- Remove or disable generic scheduler requeue from deep waits.
- Ensure process mode and thread-per-session mode still pass their existing
  gates.
- Update documentation to distinguish observable waits from scheduler-yielding
  waits.

### Phase 14A.1: Protocol Park Primitive

- Add the nonblocking frontend message type-byte probe.
- Add an explicit protocol-park API.
- Teach `PgSessionStep()` to return `PARKED_PROTOCOL_READ`.
- Assert that no partial frontend message is active when parking.
- Add unit coverage for the message-read predicate.

### Phase 14A.2: Scheduler Queue And Socket Wake

- Add runnable and parked-protocol queues.
- Add socket readiness wait-set dispatch for parked protocol reads.
- Wake parked backends on frontend input or disconnect.
- Run one frontend message per dispatch initially.
- Add focused TAP coverage for multiple idle clients.

### Phase 14A.3: Logical Wake While Parked

- Wake parked sessions on cancel/die/config/catchup/proc-signal events.
- Add `LISTEN`/`NOTIFY` coverage.
- Add idle timeout and idle-in-transaction timeout coverage.
- Prove parked idle-in-transaction sessions preserve transaction and lock
  state.

### Phase 14A.4: Carrier Affinity

- Track last carrier.
- Prefer same-carrier resume when cheap.
- Add optional short grace pin.
- Add counters and benchmarks for bursty interactive clients.

### Phase 14A.5: Real Carrier Pool

- Decouple client accept from carrier creation.
- Run a bounded number of carriers.
- Ensure backend exit does not imply carrier exit.
- Add stress tests with sessions greater than carriers.

Phase 14A should not be considered complete until this phase is done. Earlier
staging phases may be committed as scaffolding, but documentation and test names
should not claim "pooled carrier scheduler complete" while there is still one
carrier per client connection.

### Phase 14B: Output Boundary, Optional

Only after Phase 14A is stable:

- represent output flush state in `PgConnection`;
- let output paths return `would block`;
- park on socket writable;
- add tests for partial output, errors, notices, notifications, and disconnects.

### Later Deep-Wait Phases

Each later deep wait must have its own design:

- exact call sites;
- continuation state;
- cleanup rules;
- retry semantics;
- error handling;
- extension safety story;
- tests proving no live C stack is detached.

## Rollback And Cherry-Pick Strategy

The cleanest development base is the end of Phase 13 wait observability:

```text
84601c25a7 Publish semaphore-backed wait completions
```

From there, rebuild Phase 14A around protocol parking.

Current handoff state:

- the abandoned generic scheduler direction is preserved on
  `abandoned/phase14-generic-scheduler-prototype`;
- the fresh implementation branch is `phase14-protocol-boundary-scheduler`;
- `multithreaded` has been moved back to `84601c25a7`;
- `phase14-protocol-boundary-scheduler` starts from `84601c25a7`;
- the only code commits kept so far are:
  - `e16777e3f8 Serialize threaded locale probes`;
  - `8d94030db6 Record wait completion socket metadata`;
- `make -s -C src/test/modules/test_backend_runtime check` currently fails
  during `temp-install` with an `initdb` bootstrap segmentation fault on both
  `84601c25a7` and `phase14-protocol-boundary-scheduler`, so treat that as a
  pre-existing baseline problem, not evidence against the two kept commits;
- this design document and the updated phase plan are intentionally
  uncommitted working-tree docs for the Phase 14 restart.

Likely keep/cherry-pick conceptually:

- wait-completion records and tests from Phase 13;
- nonblocking frontend message type-byte probe;
- `PgSessionStep()` returning a parked/top-level result;
- socket readiness dispatch for parked protocol waits;
- logical interrupt wake for parked protocol waits;
- timeout wake for parked idle sessions;
- scheduler queue primitives after renaming/narrowing states.

Likely drop or rewrite:

- generic scheduler requeue hooks installed on every wait-completion record;
- any design where `PgSuspend()` means detach;
- tests that present `pg_sleep`, advisory locks, or LWLocks as carrier-release
  evidence;
- carrier exit handoff machinery that exists only because the prototype still
  creates one carrier per client;
- broad PMChild/carrier ownership changes until logical backend exit and
  carrier exit are specified separately.

## Test Plan

### Required Correctness Tests

- multiple parked idle clients resume on frontend input;
- parked idle client exits cleanly on disconnect;
- parked idle client handles `pg_cancel_backend()` as PostgreSQL expects;
- parked idle client handles `pg_terminate_backend()`;
- parked idle client receives `LISTEN`/`NOTIFY`;
- parked idle-in-transaction client receiving `NOTIFY` does not spin and does
  not deliver until transaction state permits it;
- parked idle-in-transaction client resumes and preserves transaction state;
- parked idle-in-transaction timeout terminates the session;
- parked idle session timeout terminates the session;
- config/catchup/proc-signal wake does not get lost;
- postmaster shutdown wakes/exits parked sessions;
- process mode behavior is unchanged;
- thread-per-session behavior is unchanged.

### Negative Tests

- deep `pg_sleep()` wait publishes observability but does not detach;
- advisory lock wait publishes observability but does not detach;
- LWLock/semaphore wait publishes observability but does not detach;
- frontend output backpressure does not claim carrier release;
- a partially read protocol message cannot be parked.

### Stress Tests

- many idle clients with fewer carriers than sessions;
- bursty clients with short think time;
- idle-in-transaction clients holding locks while carriers serve other
  sessions;
- frequent `LISTEN`/`NOTIFY` wakeups;
- cancel/terminate races with frontend input readiness;
- timeout races with frontend input readiness;
- disconnect races while parked.

### Validation Gates

Before claiming Phase 14A:

- normal process-mode regression tests pass;
- thread-per-session tests pass;
- focused protocol scheduler TAP passes;
- at least one pooled scheduler stress test runs with more sessions than
  carriers;
- carrier attach/detach invariant assertions are enabled in development builds;
- lifecycle/global-state checks pass;
- no generated test output is part of commits;
- docs clearly distinguish observable waits from scheduler-yielding waits.

## Performance Expectations

Phase 14A should primarily improve scalability for many idle or think-time
heavy sessions. It should not be expected to reduce carrier usage for many
simultaneously active blocked queries.

Expected wins:

- fewer sleeping carrier threads for idle clients;
- lower memory and scheduler overhead under high idle connection counts;
- better behavior for `LISTEN` sessions that are mostly idle;
- room for future carrier-pool sizing and affinity policies.

Expected non-wins:

- active queries blocked on locks still occupy carriers;
- storage IO waits still occupy carriers;
- frontend output backpressure still occupies carriers;
- long executor work still occupies carriers.

Carrier affinity and grace pinning may improve latency for bursty interactive
sessions, but should be measured rather than assumed.

## Open Questions

- What is the first real carrier-pool sizing policy?
- Is `PGPROC` backend-lifetime sufficient for Phase 14A idle detach, or do
  specific idle states require additional ownership cleanup?
- Which latch should represent a parked backend: backend-owned, carrier-owned,
  scheduler-owned, or a logical wake object?
- How should `FeBeWaitSet` be represented while detached?
- Should parked protocol wait use Phase 13 wait-completion records, a separate
  scheduler wait record, or both?
- What is the minimum safe grace-pin timeout?
- How should NUMA and CPU affinity be represented, if at all?
- What is the exact PMChild lifecycle for a logical backend whose carrier is
  reused?
- Which modules and in-tree subsystems can claim
  `PG_BACKEND_MODEL_POOLED_PROTOCOL_MIGRATABLE` immediately, and which must
  start affine or process-only?

## Design Summary

The first pooled scheduler should schedule PostgreSQL sessions only where
PostgreSQL already has a natural continuation: the top-level frontend protocol
loop.

Everything else remains synchronous.

This gives a much smaller and more defensible milestone:

```text
Many idle sessions, fewer carriers, no arbitrary stack suspension.
```

Later phases can add additional scheduler-yielding boundaries one at a time,
but each must prove that the live C stack can be discarded or reconstructed.
