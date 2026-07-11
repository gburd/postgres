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

Phase boundary summary:

- Phase 14 proves the protocol park/resume foundation. A staging
  implementation may still have one carrier per client while it proves that
  top-level frontend input parking is correct.
- Phase 15 proves the real bounded carrier pool. This is where parked sessions
  stop owning carriers, active work leases carriers, and at least one validation
  run has more client sessions than carriers.
- Phase 17 or later owns output-yielding, COPY continuations, deep wait
  detachment, executor/utility yield points, AIO/storage detachment, and any
  stackful coroutine or task-reentrant extension model.

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

## Protocol Byte Probe Contract

Phase 14 requires a new frontend message type-byte primitive. Existing helpers
such as `pq_getbyte_if_available()` and `pq_startmsgread_getbyte()` are not the
contract, because one requires an active message read and the other starts and
consumes the message type byte.

The required primitive has explicit tri-state semantics:

```c
typedef enum PgProtocolByteResult
{
	PG_PROTOCOL_BYTE_NONE,
	PG_PROTOCOL_BYTE_AVAILABLE,
	PG_PROTOCOL_BYTE_EOF
} PgProtocolByteResult;

typedef struct PgProtocolByteProbe
{
	unsigned char type;
	uint32		transport_wait_events;
	bool		transport_buffered_input;
	uint64		transport_generation;
} PgProtocolByteProbe;

PgProtocolByteResult PgConnectionProbeMessageType(PgConnection *connection,
												  PgProtocolByteProbe *probe);
```

The exact names can change, but the semantics must not:

- `PG_PROTOCOL_BYTE_NONE` means no byte is currently available, no active
  message read has started, `comm_reading_msg` or its replacement remains false,
  no input buffer cursor moves, no type byte is consumed, and the backend may
  park at the protocol boundary. The returned `transport_wait_events` must say
  which transport readiness events can make progress.
- `PG_PROTOCOL_BYTE_AVAILABLE` means the type byte has been consumed and the
  message is now owned by the attached carrier until the complete message body
  is read, skipped, or recovered from by the normal synchronous protocol path.
  The backend must not detach again until it returns to a new top-level
  protocol boundary.
- `PG_PROTOCOL_BYTE_EOF` means the connection is closed or has a hard read
  error. The backend must service disconnect/error handling while attached; it
  must not park as though the connection were merely idle.

The primitive must also specify:

- SSL/GSS and handshake states: no-byte may only mean "would block before a new
  PostgreSQL message". A mid-record SSL/GSS state is not a parkable PostgreSQL
  protocol boundary unless that layer has its own explicit continuation. If
  SSL/GSS cannot report a stable `transport_wait_events` mask and generation,
  Phase 14 must disable protocol parking for that connection and keep it
  carrier-pinned at frontend reads.
- transport readiness: `transport_wait_events` may include socket readability
  and writability. SSL `WANT_WRITE` while reading must park on writability, not
  only readability. GSS or SSL buffered plaintext must be reported as
  `PG_PROTOCOL_BYTE_AVAILABLE` or `transport_buffered_input`, not hidden behind
  a socket-readiness wait that may never fire. `transport_buffered_input` is not
  a sleepable parked condition: the probe should return
  `PG_PROTOCOL_BYTE_AVAILABLE` if a type byte can be exposed immediately, or the
  scheduler must make the backend immediately runnable to re-probe without
  waiting for kernel socket readiness.
- EINTR/latch/proc-signal behavior: transient interrupts must not consume a byte
  or move buffer cursors. They should return no-byte only after recording the
  wake reason that must be serviced before re-parking.
- timeout behavior: timeout expiry observed during the probe must be surfaced as
  a wake/service reason, not hidden as an ordinary no-byte result.
- query-cancel holdoff behavior: the no-byte path must not return with
  query-cancel holdoff elevated. The byte-available path may use the existing
  holdoff shape, but it must remain carrier-pinned until the full message body
  is complete or the connection exits.
- error recovery: after an error, parking is legal only after protocol recovery
  has either restored sync or decided to terminate the connection.
- observability: tests must prove the no-byte path leaves message-read state and
  buffer cursors unchanged, leaves no active message read, restores holdoff
  counters, preserves transport generation, and prevents detach after
  byte-available until the body is complete.

## Wait Classification

### Scheduler-Yielding In Phase 14A

Only:

- top-level frontend input wait before consuming the next protocol message
  type byte.

The wait sources attached to this parked state are:

- frontend transport readiness, including socket readability or writability as
  required by SSL/GSS state;
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

Phase 14 should keep this minimal:

- record the last carrier that ran a logical backend if that is cheap and useful
  for diagnostics;
- do not implement grace pinning as a Phase 14 correctness feature;
- do not hold a carrier for an idle session in staging mode.

Phase 15 target pooled mode may add soft carrier affinity:

- when the backend wakes, prefer that carrier if it is idle or cheap to wake;
- allow any carrier to run the backend if the preferred carrier is busy;
- do not hold a carrier indefinitely for an idle session.

Phase 15 may also add a short grace pin after correctness is proven:

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
Performance: target pooled mode may prefer recently active sessions' carriers.
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
runtime but must resume on a compatible home carrier or carrier class. It can
detach at the top-level protocol boundary and release the carrier while parked,
but the scheduler must respect the affinity constraint before reattaching it.
This model exists because "pooled" and "migratable" are different promises.

`PG_BACKEND_MODEL_POOLED_PROTOCOL_MIGRATABLE` means the session can detach only
at the top-level protocol boundary and later run on any carrier.

`PG_BACKEND_MODEL_TASK_REENTRANT` is a later, stronger promise. It means code is
safe for selected deep-wait or task-level continuations, not merely top-level
protocol detach.

The current single `PG_BACKEND_MODEL_POOLED_SCHEDULER` shape is too coarse for
Phase 15 migration claims. Before Phase 15 can claim carrier migration, module
compatibility must be split into at least:

- process-only;
- thread-per-session;
- pooled protocol affine;
- pooled protocol migratable;
- later task reentrant.

Until that split exists, pooled protocol mode must treat modules that only claim
the old generic pooled scheduler model as non-migratable, or reject them from
pooled protocol mode.

Phase 14/15 must not set the runtime-required backend model to
`PG_BACKEND_MODEL_POOLED_SCHEDULER`. The live loader currently uses ordinal
compatibility for that generic marker, so using it as the requirement would
admit modules under a promise the protocol scheduler no longer defines. Until
the split enum exists, pooled protocol mode should require
`PG_BACKEND_MODEL_THREAD_PER_SESSION` plus a separate hard-affinity policy, or
remain disabled for extension-bearing sessions.

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
- runnable queue;
- parked protocol-wait queue or indexed wait set;
- timeout tracking for parked sessions;
- frontend transport readiness dispatch;
- logical wake dispatch;
- attach/detach assertions;
- optional last-carrier diagnostic metadata, without grace-pinning policy.

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

Phase 14 must introduce or expose narrow runtime APIs for this boundary before
the scheduler uses carrier migration:

```c
void PgCarrierAttachBackend(PgCarrier *carrier, PgBackend *backend,
							PgSession *session, PgConnection *connection);
void PgCarrierDetachBackend(PgCarrier *carrier, PgBackend *backend);
```

These APIs must own the rebinding/assertion work for `Current*`, hot buckets,
TLS mirrors, GUC/session globals, `MyProc`, `MyLatch`, `FeBeWaitSet`, memory
contexts, resource owners, timeout state, wait-event pointers, and the
scheduler-loop state where no backend is current. Scheduler code should not
assemble those bindings ad hoc.

The implementation should wrap the existing current-work bridge, such as
`PgRuntimeSetCurrentWork()` or its successor, instead of clearing individual
pointers by hand. The attach/detach APIs must update current roots, refresh hot
cells and bucket pointers, rebind compatibility mirrors, and invalidate or
validate cached fast-path slots as one atomic-looking operation from the
scheduler's point of view.

Debug builds should make hot-field ownership checkable:

- every hot slot used by scheduler-visible code should be associated with the
  currently attached backend/session/execution generation;
- `CurrentMemoryContext` must never point at a parked backend's
  `MessageContext` while a carrier is running scheduler code;
- scheduler-loop allocation must use a carrier/runtime context, not the last
  detached session;
- resource-owner and error-context current pointers must be null or
  scheduler-safe while no backend is attached.

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
- `CurrentMemoryContext`, resource owner, and error context point at
  scheduler-safe state or are null where allowed;
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

### Phase 14 Wake Object Acceptance Criteria

Protocol parking needs a wake object immediately. Phase 14 may keep `PGPROC`
backend-lifetime, but it must make these ownership decisions explicit before
claiming protocol parking works:

- `PGPROC` remains owned by the logical backend while parked, including locks,
  proc-array identity, wait-event fields, and signal/stat visibility.
- Phase 14A.1 must define a parked wake routing table before runnable/parked
  scheduler queues are introduced. The table must cover frontend transport
  readiness, connection close/error, `SendInterrupt()`, proc-signal fallback,
  `PGPROC->procLatch` users, timeout expiry, and postmaster death.
- The parked backend is woken through a backend-owned logical wake object or a
  scheduler-owned parked record that is independent of any carrier-local latch
  pointer. The chosen object must carry a park generation.
- `MyLatch`, `backend->interrupt_latch`, and `PGPROC->procLatch` ownership must
  be rebound on attach and redirected or represented while parked. A parked
  backend must not depend on the last carrier's stack or local wait object.
- `FeBeWaitSet` must either be recreated/rebound on every attach or represented
  as connection-owned state whose socket, latch, and postmaster-death entries
  are safe across detach.
- Socket readiness, latch/interrupt wake, timeout wake, and postmaster death
  must all converge on the parked scheduler record without requiring the backend
  to be attached already.
- If any wake path cannot target the parked generation reliably, that backend is
  not eligible for protocol parking yet and must remain carrier-pinned.
- Tests must cover wake delivery after detach, same-carrier resume, migrated
  resume if migration exists, and teardown while parked.

## Scheduler State Machine

### Backend Scheduler States

Suggested states:

- `DETACHED`: backend is not owned by scheduler queues and is not running;
- `RUNNABLE`: backend is queued to run;
- `RUNNING`: backend is attached to a carrier;
- `PARKING_PROTOCOL_READ`: backend is still attached, but `PgSessionStep()` has
  prepared a park request and is returning normally to the carrier loop;
- `PARKED_PROTOCOL_READ`: backend is detached by the carrier loop after the step
  stack has unwound;
- `EXITING`: backend is tearing down and must not be dispatched.

Avoid a generic `WAITING` state unless it is explicitly qualified. Deep
observable waits should not be represented as scheduler waiting.

### Step Results

`PgSessionStep()` should return a small set of scheduler-visible outcomes:

- `CONTINUE`: made progress and may be called again;
- `PARK_PROTOCOL_READ`: prepared a protocol-read park request at top-level
  frontend input. The backend is still attached until the carrier loop observes
  the result and detaches after `PgSessionStep()` has returned;
- `ERROR_RECOVERED`: recovered from `ERROR`; scheduler may continue or exit
  depending on session state;
- `DONE`: session exited normally;
- `FATAL_EXIT`: logical backend is terminating.

The exact enum names can differ, but the park result should not be named like a
generic wait and must not imply that detach already happened inside
`PgSessionStep()`.

## Phase 14A Control Flow

### Carrier Loop

1. Pop a runnable backend.
2. Attach backend/session/connection/execution current pointers.
3. Run `PgSessionStep()` with a small budget.
4. If the step returns `PARK_PROTOCOL_READ`, assert the `PgSessionStep()` stack
   has unwound, then commit the prepared park, detach, and wait for readiness.
5. If the step returns `CONTINUE`, requeue or continue based on budget.
6. If the step returns `DONE`/`FATAL_EXIT`, perform logical backend cleanup.
7. If no backend is runnable, wait on scheduler wake sources.

### Session Step

1. Preserve the top-level error boundary.
2. If resuming from a parked wake, service the wake reason before deciding
   whether to park again:
   - frontend transport readiness wakes may proceed to the nonblocking
     message-byte probe;
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
   - prepare a protocol-park request owned by the backend/session;
   - clear/adjust interrupt holdoff as needed;
   - return `PARK_PROTOCOL_READ`;
   - do not leave a partial message read active.
7. If a byte is available:
   - disable idle timers as today;
   - process interrupts;
   - finish reading and dispatch the message synchronously;
   - remain carrier-pinned until the command returns to top level.

## Idle Command-Read Compatibility

Parking at the protocol boundary must preserve the observable behavior of the
existing `DoingCommandRead` state.

Before a backend parks:

- the session must be in the same logical command-read state used by the
  thread-per-session idle read path;
- query cancel must remain ignored or deferred exactly as PostgreSQL expects
  while idle at command read;
- idle connection checks, idle stats handling, and idle timeout arming must see
  equivalent state to a carrier-pinned idle read;
- any query-cancel holdoff or interrupt holdoff used around the type-byte probe
  must be restored to a well-defined parked state.

When a parked backend is made runnable, service wake reasons in this order:

1. Handle postmaster death, proc die, and hard connection EOF/error first.
2. Reattach the backend and restore command-read-compatible `Current*`,
   `DoingCommandRead`, latch, timeout, and wait-event state.
3. Run top-level interrupt/config/catchup processing that is legal while idle.
   Query cancel while idle must preserve current PostgreSQL semantics.
4. Mark and service expired idle/transaction/client-check timeouts through the
   normal attached timeout path.
5. Service `LISTEN`/`NOTIFY` only if the current transaction state permits
   delivery.
6. If frontend transport readiness remains, run the protocol byte probe. A
   consumed type byte makes the backend carrier-pinned until the message body
   and command dispatch complete.
7. If no frontend byte is available and all serviceable wake reasons are
   consumed or explicitly deferred, re-park with a new generation.

An idle-in-transaction notify wake needs a deferred-notify marker. If
`ProcessNotifyInterrupt()` or the equivalent path cannot clear the pending
notification because the backend is idle in a transaction, the scheduler must
record the notify generation/reason as deferred. The same unprocessable notify
must not keep requeueing the backend until transaction state changes or a newer
notification generation arrives.

There are two separate notify states to track:

- the logical wake/signal generation that made the parked backend runnable;
- the backend's still-pending async notification work, which may remain pending
  while idle in a transaction.

Phase 14 must test both. Reparking after an idle-in-transaction notify wake is
legal only after the wake generation is marked serviced/deferred, while the
backend-level pending async notification remains for later delivery. A new
notify signal generation or a transaction-state change may make the backend
runnable again; the same deferred generation must not.

## API Sketch

Names are provisional. The important point is to separate protocol parking from
generic wait publication.

### Protocol Park

```c
typedef enum PgProtocolParkWake
{
	PG_PROTOCOL_WAKE_SOCKET_READABLE = 1 << 0,
	PG_PROTOCOL_WAKE_SOCKET_WRITEABLE = 1 << 1,
	PG_PROTOCOL_WAKE_SOCKET_CLOSED   = 1 << 2,
	PG_PROTOCOL_WAKE_INTERRUPT       = 1 << 3,
	PG_PROTOCOL_WAKE_NOTIFY          = 1 << 4,
	PG_PROTOCOL_WAKE_TIMEOUT         = 1 << 5,
	PG_PROTOCOL_WAKE_POSTMASTER_DEATH = 1 << 6,
} PgProtocolParkWake;

typedef struct PgProtocolParkSpec
{
	PgBackend  *backend;
	PgSession  *session;
	PgConnection *connection;
	pgsocket	socket;
	uint32		transport_wait_events;
	uint64		transport_generation;
	TimestampTz timeout_at;
	uint64		timeout_generation;
	uint32		wake_mask;
	uint32		wait_event_info;
	uint64		generation;
	uint64		notify_signal_generation;
	uint64		deferred_notify_generation;
} PgProtocolParkSpec;

bool PgBackendPrepareProtocolReadPark(PgProtocolParkSpec *spec);
void PgCarrierCommitProtocolReadPark(PgCarrier *carrier, PgBackend *backend);
void PgBackendUnparkProtocolRead(PgBackend *backend, uint32 wake_events);
```

`PgBackendPrepareProtocolReadPark()` is called while inside `PgSessionStep()`.
It records the park request and returns a step result, but it must not detach
the backend, clear `CurrentPgBackend`, enqueue the backend in a detached parked
queue, or otherwise make the carrier available to another backend while the
`PgSessionStep()` frame is still live.

It must assert that:

- the backend is in pooled scheduler mode;
- no protocol message read is active;
- the session is in top-level command read state;
- no execution stack is active below the current `PgSessionStep()` frame;
- the backend is not already in a scheduler queue;
- the connection transport has a valid wait mask or buffered-input generation,
  unless the wake is purely interrupt/timeout based.

`PgCarrierCommitProtocolReadPark()` is called only by the carrier loop after
`PgSessionStep()` has returned `PARK_PROTOCOL_READ`. It performs the actual
detach, rebinds or clears carrier current state, and moves the backend into the
parked protocol-read structure. This split protects the no-live-stack invariant:
the only live PostgreSQL frame at prepare time is the step frame that is about
to return, and the detach happens after it has unwound.

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

- frontend transport readable/writeable/buffered;
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

- frontend transport readiness from the park record's
  `transport_wait_events`, including socket readability, socket writability for
  SSL/GSS progress, and already-buffered decrypted bytes;
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

The scheduler must compare the park record's transport generation before acting
on readiness. If the connection's transport state has changed since the park was
prepared, the backend must be reattached to re-probe rather than treating stale
readiness as permission to consume protocol bytes.

## Timeout Handling

Timeouts must target logical backends, not physical carriers.

Detached scheduler code must not call timeout helpers that implicitly inspect
or mutate `CurrentPgBackend`. Phase 14 must choose one explicit mechanism:

```c
bool PgBackendTimeoutNextWake(PgBackend *backend, TimestampTz *wake_at);
void PgBackendTimeoutMarkExpired(PgBackend *backend, TimestampTz now);
void PgBackendTimeoutServiceAttached(PgBackend *backend);
```

The exact API can change, but the ownership rule cannot:

- computing the next parked wake may use backend-indexed timeout state captured
  before detach or an API that takes an explicit `PgBackend *`;
- marking a parked timeout expired may set backend-owned pending bits and
  enqueue a `PG_PROTOCOL_WAKE_TIMEOUT`;
- firing timeout handlers and raising user-visible timeout errors must happen
  only after the backend is attached and the normal top-level timeout path is
  active.

Parked timeout snapshots must carry a timeout generation. Any timeout enable,
disable, reschedule, service, frontend byte consumption, or backend reattach
that changes timeout state must advance that generation. When the scheduler
observes a timeout timestamp, it may only mark the parked timeout expired if the
park record's timeout generation still matches the backend's current timeout
generation. A stale timeout snapshot must wake/reprobe the backend at most; it
must not fire user-visible timeout behavior while detached.

An acceptable initial implementation is to snapshot the next idle timeout while
the backend is still attached during protocol park, let the scheduler sleep on
that timestamp, and reattach the backend before calling any current-backend
timeout code. What is not acceptable is a detached carrier inspecting or firing
another backend's logical timeout through thread-local `Current*` state.

For parked sessions, the scheduler should know the next relevant logical
timeout and wake the backend when it expires. On re-entry, existing timeout
processing should set the same user-visible behavior as thread-per-session
mode.

Timeouts to cover in Phase 14A:

- idle session timeout;
- idle-in-transaction session timeout;
- transaction timeout while idle;
- idle stats update timeout if retained;
- client connection check timeout if retained while parked;
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
PgBackendPrepareProtocolReadPark(...);
PgCarrierCommitProtocolReadPark(...);
```

Alternatively, `PgWaitSpec` can carry a capability flag:

```c
PG_WAIT_CAN_DETACH_CARRIER
```

Only the protocol-read commit path should set that flag in Phase 14A, and only
after `PgSessionStep()` has returned.

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

Before scheduler dispatch grows beyond staging, `PgStepResult` must have
explicit scheduler-visible outcomes for protocol park, normal logical backend
exit, and fatal logical backend exit. EOF, `Terminate`, and other normal session
end paths must be able to return `DONE` or a logical-exit result to the carrier
loop in pooled modes. They must not rely on non-returning `PgBackendExit(0)` if
the physical carrier is supposed to survive and run another backend.

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

Phase 15 also has to keep the postmaster child publication model split between
logical backend identity and physical carrier lifetime. Thread-backed PMChild
state exposes logical backend publication through `logical_backend` and
`logical_signal_pid`, while native thread exit remains a separate carrier
lifecycle report. A real carrier pool must preserve that separation so a parked
or migrated logical backend is not lost when a carrier exits and a carrier can
be reused without implying logical backend exit.
Phase 15 introduces a pooled-logical PMChild state for client sessions that
publish a logical backend without owning a dedicated process or native thread
carrier; signal and wake routing still target the published logical backend.

## Carrier Pool

The target pooled runtime should eventually launch carriers independently of
client connections.

Phase 14 may use staging mode:

- still launch one carrier per client;
- prove protocol park/resume, logical wakeups, and state ownership;
- use carrier affinity trivially because the home carrier exists;
- keep all deep waits and active command work carrier-pinned.

Staging mode is acceptable Phase 14 completion if protocol parking is correct
and the tests avoid claiming that sessions outnumber carriers. It is a
foundation, not the final pooled scheduler.

Phase 15 owns real pool mode:

- launch a bounded carrier pool;
- accepted clients create logical backend/session objects;
- parked sessions do not own carriers;
- active commands lease carriers;
- backend exit does not imply carrier exit;
- at least one validation run has runnable/parked client sessions outnumbering
  carrier threads.

## Implementation Phases

### Phase 14A.0: Clean Baseline

- Start from the end of Phase 13 wait-completion work.
- Remove, disable, or assert-unreachable generic scheduler requeue hooks from
  deep wait-completion records. Phase 14A.1 must not start while a deep
  wait-completion readiness path can enqueue detached scheduler work.
- Ensure process mode and thread-per-session mode still pass their existing
  gates.
- Update documentation to distinguish observable waits from scheduler-yielding
  waits.

### Phase 14A.1: Protocol Park Primitive

- Add the nonblocking frontend message type-byte probe.
- Add explicit prepare/commit protocol-park APIs.
- Add the transport wait mask/generation required by TLS/GSS-aware protocol
  parking, or explicitly disable protocol parking for SSL/GSS connections in
  Phase 14.
- Teach `PgSessionStep()` to return `PARK_PROTOCOL_READ` after preparing a park
  request, without detaching inside the step frame.
- Teach the carrier loop to commit the park and detach only after
  `PgSessionStep()` has returned.
- Extend `PgStepResult` with protocol park, normal exit, and fatal exit outcomes
  before scheduler dispatch relies on the step result.
- Assert that no partial frontend message is active when parking.
- Add unit coverage for the message-read predicate, receive-buffer cursor,
  query-cancel holdoff, and transport wait mask.

### Phase 14A.2: Scheduler Queue And Transport Wake

- Add runnable and parked-protocol queues.
- Add frontend transport readiness wait-set dispatch for parked protocol reads.
- Wake parked backends on frontend input or disconnect.
- Run one frontend message per dispatch initially.
- Add focused TAP coverage for multiple idle clients.
- Cover TLS/GSS transport wait masks, or prove those connections stay
  carrier-pinned in Phase 14.

### Phase 14A.3: Logical Wake While Parked

- Wake parked sessions on cancel/die/config/catchup/proc-signal events.
- Add `LISTEN`/`NOTIFY` coverage.
- Add idle timeout and idle-in-transaction timeout coverage.
- Add timeout snapshot generation validation coverage.
- Prove parked idle-in-transaction sessions preserve transaction and lock
  state.

### Phase 15.1: Real Carrier Pool

- Decouple client accept from carrier creation.
- Run a bounded number of carriers.
- Ensure backend exit does not imply carrier exit.
- Add stress tests with sessions greater than carriers.
- Add migration/affinity counters for same-carrier versus moved resumes.
- Add optional short grace pin only after scheduler pressure and shutdown escape
  conditions are tested.

Early Phase 15 foundation should keep the carrier object visibly separate from
the logical backend/session/connection/execution object group even where the
thread-per-session launcher still allocates them together. This avoids baking
the staging assumption into the runtime fixture that later pooled carriers must
reuse across logical sessions.

The runnable side should expose a carrier-facing lease primitive: an idle
carrier pops one runnable protocol backend and attaches the backend's logical
session/connection/execution state to itself. Staging mode may still drive this
from the same thread that parked the session, but tests should be able to prove
the lease path also works with a different resume carrier.

Real pool carriers must register with the protocol scheduler before leasing
work. Registration is bounded by the configured carrier limit and accounts
idle, active, rejected, leased, and released carriers so the pool can prove it
is serving sessions with fewer physical carriers than logical sessions.

Phase 14 staging phases may be committed as scaffolding, but documentation and
test names should not claim "pooled carrier scheduler complete" while there is
still one carrier per client connection. Phase 15 is not complete until the real
bounded pool and sessions-greater-than-carriers validation exist.

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

Current branch state:

- the abandoned generic scheduler direction is preserved on
  `abandoned/phase14-generic-scheduler-prototype`;
- the protocol-boundary implementation branch is
  `phase14-protocol-boundary-scheduler`;
- `multithreaded` has been moved back to `84601c25a7`;
- `phase14-protocol-boundary-scheduler` starts from `84601c25a7`;
- Phase 14A has been rebuilt around top-level protocol parking, with deep waits
  remaining observable but carrier-pinned;
- the historical `temp-install`/`initdb` bootstrap segmentation fault seen on
  the Phase 13 baseline has been fixed on `phase14-protocol-boundary-scheduler`;
  `make -C src/test/modules/test_backend_runtime check` is now expected to pass
  as part of Phase 14A verification;
- this design document and the updated phase plan are committed on
  `phase14-protocol-boundary-scheduler` and should be kept in sync as review
  feedback closes.

Likely keep/cherry-pick conceptually:

- wait-completion records and tests from Phase 13;
- nonblocking frontend message type-byte probe;
- `PgSessionStep()` returning a parked/top-level result;
- frontend transport readiness dispatch for parked protocol waits;
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

- bursty clients with short think time;
- frequent `LISTEN`/`NOTIFY` wakeups;
- cancel/terminate races with frontend input readiness;
- timeout races with frontend input readiness;
- disconnect races while parked.

These are Phase 15 target-pool stress tests, not Phase 14 staging evidence:

- many idle clients with fewer carriers than sessions;
- idle-in-transaction clients holding locks while carriers serve other
  sessions.

### Validation Gates

Before claiming Phase 14:

- normal process-mode regression tests pass;
- thread-per-session tests pass;
- focused protocol scheduler TAP passes;
- carrier attach/detach invariant assertions are enabled in development builds;
- protocol-byte probe tests prove no-byte leaves message state untouched and
  byte-available pins the backend until the full message is handled;
- byte-probe tests prove no-byte restores query-cancel holdoff and does not move
  receive-buffer cursors;
- transport-readiness tests prove TLS/GSS connections either park with the
  correct read/write/buffered wait mask or are excluded from Phase 14 parking;
- parked wake tests cover frontend input, disconnect, cancel, terminate,
  timeout, postmaster death, and `LISTEN`/`NOTIFY`;
- idle-in-transaction notify tests prove the deferred-notify marker prevents
  requeue churn;
- timeout tests prove stale parked timeout generations cannot fire detached
  timeout behavior;
- lifecycle/global-state checks pass;
- no generated test output is part of commits;
- docs clearly distinguish observable waits from scheduler-yielding waits.

Before claiming Phase 15:

- the runtime launches a bounded carrier pool independently of client sessions;
- parked sessions do not own carriers;
- at least one pooled scheduler stress test runs with more sessions than
  carriers;
- extension compatibility distinguishes protocol-affine from
  protocol-migratable sessions;
- migration/affinity counters distinguish same-carrier resumes from migrated
  resumes;
- negative deep-wait tests still prove `PgSuspend()` does not detach active C
  stacks.

## Performance Expectations

Target pooled mode should primarily improve scalability for many idle or
think-time-heavy sessions. Phase 14 staging proves correctness of the protocol
park/resume boundary; the measurable carrier-count wins arrive in Phase 15 when
parked sessions no longer own carriers. Neither phase should be expected to
reduce carrier usage for many simultaneously active blocked queries.

Expected Phase 15 wins:

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
Phase 14: protocol park/resume correctness, no arbitrary stack suspension.
Phase 15: many idle sessions on fewer carriers.
```

Later phases can add additional scheduler-yielding boundaries one at a time,
but each must prove that the live C stack can be discarded or reconstructed.
