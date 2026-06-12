# Phase 10 Thread-Per-Session Runtime Notes

Phase 10 is in progress. The goal is to run regular client backends as OS
threads inside one server runtime while preserving the process launch path.

## Launch Selection Scaffold

The first slice adds the explicit launch selector without starting backend
threads yet:

- `multithreaded` is a hidden `PGC_POSTMASTER` developer GUC, defaulting off;
- `PgRuntimeGetBackendLaunchModel()` returns `PG_BACKEND_LAUNCH_THREAD` only
  when `multithreaded` is enabled for a regular `B_BACKEND` launch;
- `postmaster_child_launch()` now consults the runtime selector before the fork
  path;
- the threaded branch currently logs a clear "not implemented yet" message and
  rejects the connection instead of silently forking.

Dead-end backends, auxiliary workers, background workers, and other
server-owned worker families remain process launches. WAL senders still begin
life as `B_BACKEND`; they need an explicit policy once the regular backend
thread launcher exists.

The next slice should add a backend thread portability layer and a real thread
launcher that can run `BackendMain()` with carrier-local runtime pointers.

## Thread Portability Slice

The second slice adds the minimal backend port wrapper for native carrier
threads:

- `PgThread` stores the native thread handle;
- `pg_thread_create()` creates a named carrier thread from a
  `void (*)(void *)` routine;
- `pg_thread_join()` and `pg_thread_detach()` cover the lifecycle operations
  needed by thread-per-session launch and teardown;
- `pg_thread_set_name()` sets a native thread name where this checkout can
  compile the platform support.

This wrapper deliberately does not expose mutexes, condition variables, or a
thread pool. Those belong in later runtime/scheduler layers once backend
threads can actually run.

The Windows implementation is a best-effort `_beginthreadex()` wrapper and has
not been validated in this macOS checkout.

## Explicit Backend Startup Mode Slice

The third slice starts separating backend startup from process inheritance:

- `BackendMain()` remains the existing postmaster-child process entrypoint;
- `BackendMainWithStartupData()` now takes explicit startup data, an explicit
  client socket, and a `BackendStartupMode`;
- process mode uses `BACKEND_STARTUP_PROCESS`, preserving the historical
  `SIGTERM`, `SIGALRM`, startup-packet timeout, and `_exit()` behavior before
  shared memory is touched;
- `BACKEND_STARTUP_THREAD` is named but still fails explicitly because startup
  timeout and termination still need to be routed through logical backend
  exit before it can safely run inside the postmaster address space.

This is intentionally not the thread launcher yet. It creates the call shape
the launcher needs and makes the remaining process-only startup semantics
visible.

## PMChild Carrier Identity Slice

The fourth slice starts separating postmaster supervision identity from process
identity:

- `PMChild` now records an explicit `PMChildCarrierKind`;
- process-backed children are initialized with `PM_CHILD_CARRIER_PROCESS`;
- process launch success records the PID through `PostmasterChildSetProcess()`;
- PID lookup and worker-notify helpers only match process-backed children;
- process signaling asserts that the target is process-backed.

This does not launch backend threads. It removes the assumption that every
postmaster child entry can be supervised only by a PID, which is a prerequisite
for recording a future thread handle and for keeping `waitpid()` cleanup from
accidentally consuming thread-backed logical children.

## Carrier-Aware Launch API Slice

The fifth slice moves the launch-model decision to a PMChild-aware API:

- `postmaster_child_launch_carrier()` takes the already allocated `PMChild`;
- process launch success records the PID in the `PMChild` before returning;
- threaded launch remains an explicit `ENOSYS` failure, but now fails at the
  API that can later record a thread carrier rather than at a PID-only return
  boundary;
- regular backends, auxiliary children, and background workers use the
  carrier-aware API;
- the old `postmaster_child_launch()` remains the process-only launcher for
  process-only callers such as syslogger startup.

This keeps current process behavior intact while making the next real thread
launcher patch local to `launch_backend.c` and `pmchild.c` instead of forcing
all postmaster call sites to reason about non-PID carriers.

## PMChild Thread Handle Slice

The sixth slice gives thread-backed children a native handle slot:

- `PMChild` now stores a `PgThread` alongside the process PID;
- `PostmasterChildIsThread()` identifies thread-backed entries;
- `PostmasterChildSetThread()` records the native thread handle and clears the
  process PID.

No caller sets thread-backed PMChild entries yet. This makes the eventual
thread launcher able to record its carrier without overloading `pid` or
inventing a transient side table.

## Thread Exit Reaper Slice

The seventh slice adds the postmaster-owned cleanup path for thread-backed
children:

- `PMChild` now records an atomic "thread exited" flag and a waitpid-style
  thread exit status;
- an exiting thread can call `PostmasterChildMarkThreadExited()` to publish its
  exit and wake the postmaster latch;
- the postmaster main loop scans for exited thread-backed children and calls
  `CleanupBackend()` itself;
- thread carriers therefore do not mutate `ActiveChildList` or release PMChild
  slots from inside the carrier thread.

This is a prerequisite for the first actual backend-thread launch. Without it,
the thread routine would either leak PMChild slots or perform postmaster-owned
list mutation from the wrong thread.

## Thread Carrier Reject Slice

The eighth slice creates the first regular backend carrier thread when
`multithreaded=on`:

- the postmaster duplicates the accepted client socket for the thread carrier;
- `pg_thread_create()` starts a backend-named carrier thread;
- the PMChild entry is marked as `PM_CHILD_CARRIER_THREAD`;
- the carrier thread currently sends a protocol-level rejection and closes its
  socket instead of entering `BackendMain()`;
- the thread reports normal exit through `PostmasterChildMarkThreadExited()`;
- the postmaster joins the completed thread and releases the PMChild slot
  through the thread-exit reaper.

This proves the launch, socket ownership, thread handle, latch wakeup, join,
and PMChild reaping path without yet running PostgreSQL backend startup inside
the postmaster address space. The remaining blockers for replacing the
rejection with `BackendMainWithStartupData(..., BACKEND_STARTUP_THREAD)` are
the thread-safe startup timeout/termination path, backend-local initialization
that does not mutate postmaster runtime globals, and backend exit that cannot
call process exit from the carrier thread.

## Thread Carrier Identity Slice

The ninth slice starts installing backend-local identity inside the temporary
rejecting carrier thread:

- the thread start payload now carries a copied `BackendStartupData`;
- the carrier thread initializes `MyBackendType`, `MyPMChildSlot`, and
  `MyClientSocket` before touching the client socket;
- connection timing state is initialized from the copied startup data, with
  `fork_end` standing in as the carrier-thread start timestamp for now;
- malformed thread-launch startup payloads fail before thread creation;
- the thread still rejects the connection before entering backend startup.

`MyClientSocket` currently points into the thread start payload and is cleared
before that payload is freed. That is acceptable only for the reject stub. The
first real `BackendMainWithStartupData(..., BACKEND_STARTUP_THREAD)` path must
move client-socket ownership into backend-local lifetime before the start
payload can be released.

## Thread Runtime State Slice

The tenth slice gives the carrier thread an explicit runtime/backend object
frame before it touches backend-local compatibility globals:

- `PG_RUNTIME_THREAD_PER_SESSION` and `PG_CARRIER_THREAD` distinguish the
  first threaded runtime from process mode;
- `InitializePgThreadRuntime()` initializes the shared thread-per-session
  runtime object and marks its extension backend model as
  `PG_BACKEND_MODEL_THREAD_PER_SESSION`;
- `PgThreadBackendRuntimeState` owns one carrier/backend/session/connection/
  execution object set for a carrier thread;
- `InitializePgThreadBackendRuntime()` installs TLS current pointers, a fresh
  backend-local exit state, interrupt mailbox, DSM mapping list, wait state,
  and optional interrupt latch for the carrier thread;
- the temporary rejecting backend carrier now initializes this runtime state
  before setting `MyBackendType`, `MyPMChildSlot`, and `MyClientSocket`;
- `test_backend_runtime` verifies the thread runtime state initializer without
  leaving the real process backend in threaded mode.

The threaded runtime still does not install a non-returning `exit_backend`
continuation. Replacing the reject stub with real backend startup therefore
still requires the next slice to add a thread-exit primitive and a carrier
exit continuation that reports PMChild exit without calling process `exit()`.

## Thread Carrier Exit Slice

The eleventh slice adds the non-returning carrier exit primitive needed by the
future backend-thread exit continuation:

- `pg_thread_exit()` terminates the current carrier thread without returning;
- `InitializePgThreadRuntime()` now receives `backend_thread_exit` as the
  runtime `exit_backend` continuation for threaded backend carriers;
- the backend carrier launch payload is tracked through carrier-local TLS;
- `backend_thread_finish()` publishes a waitpid-style exit status, wakes the
  postmaster latch, clears temporary connection state, frees the launch
  payload, and exits the thread;
- the temporary reject carrier now exits through this finalizer instead of
  returning from the thread routine;
- `test_backend_runtime` covers `pg_thread_exit()` by creating a joinable
  thread that exits explicitly.

The real `PgBackendExit()` path is still not safe to exercise from backend
startup. It currently checks process-child identity before invoking cleanup,
and `BackendInitialize(..., BACKEND_STARTUP_THREAD)` still stops before
startup-packet handling because timeout and termination delivery are not yet
thread-local.

## Thread Carrier Memory Context Slice

The twelfth slice initializes PostgreSQL memory-context roots inside the
carrier thread:

- the carrier thread calls `MemoryContextInit()` before installing backend
  runtime state;
- the carrier therefore has thread-local `TopMemoryContext`, `ErrorContext`,
  and `CurrentMemoryContext` before future backend startup can allocate
  `Port`, startup packet, or error-reporting state;
- `backend_thread_finish()` deletes the carrier thread's `TopMemoryContext`
  before exiting, so the temporary reject carrier does not leak the per-thread
  memory-context tree into the long-lived postmaster process.

This still does not run `BackendInitialize(..., BACKEND_STARTUP_THREAD)`.
Startup packet timeout/termination routing and the `PgBackendExit()` process
identity check remain the next blockers.

## Thread Startup Guard Slice

The thirteenth slice routes the carrier thread into the shared backend startup
entrypoint and stops at the explicit threaded-startup guard:

- the backend carrier now calls
  `BackendMainWithStartupData(..., BACKEND_STARTUP_THREAD)` instead of sending
  a hand-written protocol rejection;
- the carrier initializes `MyProcPid`, a thread-local `MyLatch`,
  `TopMemoryContext`, `ErrorContext`, thread runtime state, backend identity,
  connection timing, and startup timestamps before entering backend startup;
- the thread start payload copies the postmaster's current `session_timezone`
  and `log_timezone` pointers so early error reporting in a new TLS carrier can
  format timestamps before full GUC session-state adoption exists;
- `InitializePgThreadRuntime()` now prepares the shared thread runtime and
  exit continuation without switching the current carrier's
  `CurrentPgRuntime`; `InitializePgThreadBackendRuntime()` adopts that runtime
  inside the backend carrier thread;
- this keeps the postmaster supervisor in process runtime while backend
  carriers use the thread-exit continuation;
- the startup guard now reports the normal FATAL response through backend error
  reporting, exits through `PgBackendExit()`, publishes PMChild thread exit,
  wakes the postmaster latch, and lets the postmaster join/reap the carrier.

This proves more of the real startup path, but it deliberately still stops
before startup-packet timeout and termination handling. The next slices need to
replace the process-only startup timeout path, route startup termination through
logical backend exit, and begin adopting/copying broader GUC-backed session
state before the guard can move later in `BackendInitialize()`.

## Startup Metadata Boundary Slice

The fourteenth slice moves the threaded-startup guard to the startup-packet
timeout boundary:

- process mode still installs the historical startup SIGTERM handler,
  initializes SIGALRM-backed timeouts, and applies the startup signal mask
  immediately after `pq_init()`;
- thread mode now skips those process-global signal changes and continues
  through remote host/port resolution, `Port` metadata initialization, and
  optional connection receipt logging;
- the threaded guard now fires immediately before `RegisterTimeout()` and
  `enable_timeout_after()` would arm the process-global startup packet timeout;
- disabling the startup timeout and restoring `BlockSig` are explicitly
  process-mode operations.

This leaves a sharper next boundary: a threaded backend can perform early
connection metadata setup, but it still cannot read the startup packet until
startup timeout and startup termination have logical-backend equivalents that
do not use `_exit()` or process-wide SIGALRM delivery.

## Startup Packet Read Deadline Slice

The fifteenth slice lets the threaded carrier read and parse the startup
packet before stopping at the next unsafe boundary:

- process mode still uses the historical `STARTUP_PACKET_TIMEOUT` and
  SIGALRM-backed timeout machinery;
- thread mode records a per-`Port` client-read deadline for the startup packet
  window instead of arming a process-global SIGALRM timeout;
- `secure_read()` converts that per-connection deadline into a finite
  `WaitEventSetWait()` timeout and reports `ETIMEDOUT` when it expires;
- after startup-packet handling completes, thread mode clears the deadline and
  stops at a guarded FATAL before authentication, `InitProcess()`, or
  post-startup backend lifetime;
- `socket_close()` now explicitly closes the accepted socket for threaded
  backends during logical backend exit, while preserving the process-backend
  behavior of leaving the socket open until process death.

This moves the thread-per-session prototype through real startup packet
processing without using the process-level startup timeout. Authentication
remains deliberately guarded because its timeout path, `InitPostgres()`/
`InitProcess()` side effects, and post-startup session termination paths still
need their own thread-safe lifecycle work.

## Validation

- `gmake -C src/backend/postmaster launch_backend.o` passed;
- `gmake -C src/backend/postmaster pmchild.o postmaster.o` passed after the
  PMChild carrier identity slice;
- `gmake -C src/backend/postmaster pmchild.o postmaster.o` passed after the
  thread exit reaper slice;
- `gmake -C src/backend/postmaster launch_backend.o postmaster.o pmchild.o`
  passed after the thread carrier reject slice;
- `gmake -C src/backend/postmaster launch_backend.o postmaster.o pmchild.o`
  passed after the carrier-aware launch API slice;
- `gmake -C src/backend/postmaster pmchild.o postmaster.o launch_backend.o`
  passed after the PMChild thread handle slice;
- after the PMChild layout changed, an incremental temp-instance check exposed
  stale postmaster objects, so `gmake -C src/backend/postmaster clean` followed
  by `gmake -C src/backend -j8` was required and passed;
- `gmake -C src/backend/utils/init backend_runtime.o globals.o` passed;
- `gmake -C src/backend/utils/misc guc_tables.o` passed after regenerating
  `guc_tables.inc.c`;
- `gmake -C src/backend/tcop backend_startup.o` passed after the explicit
  startup-mode slice;
- full `gmake -C src/backend -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including the
  `pg_thread_create()`/`pg_thread_join()` smoke;
- temp install smoke with the default `multithreaded=off` showed `off` and ran
  `SELECT 1`;
- temp install smokes with `multithreaded=on` rejected a client connection
  with `Function not implemented` and logged the explicit threaded-launch stub
  message, including after the carrier-aware launch API slice.
- a temp install smoke with `multithreaded=on` after the thread carrier reject
  slice rejected two client connections with "threaded backend startup is not
  implemented yet"; `pg_ctl status` reported the postmaster still running, and
  normal fast shutdown completed.
- after the thread carrier identity slice,
  `gmake -C src/backend/postmaster launch_backend.o` passed;
- after the thread carrier identity slice, full `gmake -C src/backend -j8`
  passed;
- after the thread carrier identity slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the thread carrier
  identity slice rejected two client connections with "threaded backend startup
  is not implemented yet"; `pg_ctl status` reported the postmaster still
  running, and normal fast shutdown completed.
- after the thread runtime state slice,
  `gmake -C src/backend/utils/init backend_runtime.o` and
  `gmake -C src/backend/postmaster launch_backend.o` passed;
- after the thread runtime state slice, full `gmake -C src/backend -j8`
  passed;
- after the thread runtime state slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed, including the
  thread runtime state initializer check;
- a temp install smoke with `multithreaded=on` after the thread runtime state
  slice rejected two client connections with "threaded backend startup is not
  implemented yet"; `pg_ctl status` reported the postmaster still running, and
  normal fast shutdown completed.
- after the thread carrier exit slice, `gmake -C src/backend/port pg_thread.o`
  and `gmake -C src/backend/postmaster launch_backend.o` passed;
- after the thread carrier exit slice, full `gmake -C src/backend -j8` passed;
- after the thread carrier exit slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed, including
  explicit `pg_thread_exit()` join coverage;
- a temp install smoke with `multithreaded=on` after the thread carrier exit
  slice rejected two client connections with "threaded backend startup is not
  implemented yet"; `pg_ctl status` reported the postmaster still running, and
  normal fast shutdown completed.
- after the thread carrier memory context slice,
  `gmake -C src/backend/postmaster launch_backend.o` passed;
- after the thread carrier memory context slice, full `gmake -C src/backend -j8`
  passed;
- after the thread carrier memory context slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the thread carrier memory
  context slice rejected two client connections with "threaded backend startup
  is not implemented yet"; `pg_ctl status` reported the postmaster still
  running, and normal fast shutdown completed.
- while replacing the hand-written rejection with the startup guard, a live
  smoke initially exposed an uninitialized thread-local latch/timezone crash in
  `pq_init()` error reporting and a separate bug where preparing the thread
  runtime switched the postmaster's own `CurrentPgRuntime`; both were fixed in
  the startup guard slice.
- after the thread startup guard slice,
  `gmake -C src/backend/postmaster launch_backend.o` and
  `gmake -C src/backend/utils/init backend_runtime.o` passed;
- after the thread startup guard slice, full `gmake -C src/backend -j8`
  passed;
- after the thread startup guard slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the thread startup guard
  slice rejected two client connections with the guarded FATAL from
  `BackendInitialize(..., BACKEND_STARTUP_THREAD)`; `pg_ctl status` reported
  the postmaster still running between connections, no thread-exit continuation
  ran during postmaster shutdown, and normal fast shutdown completed.
- after the startup metadata boundary slice,
  `gmake -C src/backend/tcop backend_startup.o` passed;
- after the startup metadata boundary slice, full `gmake -C src/backend -j8`
  passed;
- a temp install smoke with `multithreaded=on` after the startup metadata
  boundary slice still rejected two client connections with the guarded FATAL,
  kept the postmaster running between connections, and completed normal fast
  shutdown.
- after the startup packet read deadline slice,
  `gmake -C src/backend/libpq be-secure.o`,
  `gmake -C src/backend/libpq pqcomm.o`, and
  `gmake -C src/backend/tcop backend_startup.o` passed;
- after the startup packet read deadline slice, full `gmake -C src/backend -j8`
  passed;
- after the startup packet read deadline slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the startup packet read
  deadline slice read real libpq startup packets for two client connections,
  rejected both with the guarded "threaded backend authentication is not
  implemented yet" FATAL, kept the postmaster running between connections, and
  completed normal fast shutdown;
- the same smoke held a silent Unix-socket client past
  `authentication_timeout='1s'`; the server logged `could not receive data from
  client: Operation timed out`, the client observed EOF after the logical
  backend exited, and `pg_ctl status` confirmed the postmaster remained alive.
