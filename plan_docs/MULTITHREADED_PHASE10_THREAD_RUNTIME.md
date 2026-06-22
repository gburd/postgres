# Phase 10 Thread-Per-Session Runtime Notes

Phase 10 is complete for the first thread-per-session target. Regular client
backends can run as OS threads inside one server runtime while preserving the
process launch path. Server-owned worker families remain deferred to Phase 11:
startup-time process workers are tolerated, late fork-without-exec worker
launches are blocked after backend threads exist, and at the Phase 10 gate
process-backed parallel workers were suppressed so callers fell back to
leader-only execution where PostgreSQL already supports that. Phase 11 later
replaces the Phase 10 autovacuum-worker and core parallel-worker deferrals
with thread carriers.

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

## InitProcess Boundary Slice

The sixteenth slice moves the guarded stop out of `BackendInitialize()` and to
the first shared-memory backend registration boundary:

- threaded startup now returns from `BackendInitialize()` after parsing a valid
  startup packet and filling `MyProcPort` startup state;
- `BackendMainWithStartupData()` stops immediately before `InitProcess()` when
  running in `BACKEND_STARTUP_THREAD` mode;
- the guarded FATAL now names the real next blocker: `InitProcess()`, PGPROC
  registration, and post-startup backend lifetime still assume process-backed
  backend identity;
- process mode still enters `InitProcess()`, `PostgresMain()`, authentication,
  and normal session execution unchanged.

This establishes the next concrete work item: make PGPROC registration and
exit cleanup safe for logical backends whose carrier is a thread and whose
process PID is shared with the postmaster and sibling backends.

## Logical Backend ID Slice

The seventeenth slice introduces a runtime-owned logical backend identity
before changing PGPROC registration:

- `PgBackend` now carries a `PgBackendId` assigned independently of
  `MyProcPid`;
- process and thread runtime initialization both assign backend ids from a
  runtime-wide atomic counter;
- `PgBackendGetId()` and `PgCurrentBackendId()` expose the identity without
  requiring callers to inspect runtime internals;
- `test_backend_runtime` verifies that two thread backend runtime states in
  one address space get nonzero, distinct logical backend ids.

This does not yet replace the many existing PID-backed fields. It creates the
object identity that later PGPROC, procsignal, latch, cancellation, and
monitoring work can target while preserving process-mode behavior.

## PGPROC Logical Identity Slice

The eighteenth slice carries the runtime-owned logical backend identity into
shared backend registration:

- `PgBackendId` now lives in a small shared header so runtime and shared-memory
  backend structures can refer to the same type without pulling in full runtime
  state;
- `PGPROC` now records `backendId` alongside the historical process `pid`;
- `InitProcess()` and `InitAuxiliaryProcess()` initialize `PGPROC.backendId`
  when a runtime/backend object already exists;
- process-mode `InitializePgProcessRuntime()` backfills `MyProc->backendId`
  because process backends create their runtime object in `BaseInit()` after
  `InitProcess()`;
- `ProcKill()` and `AuxiliaryProcKill()` clear the logical id when the PGPROC
  slot becomes reusable;
- `test_backend_runtime` verifies that a normal SQL backend has a nonzero
  `PgCurrentBackendId()` and a matching `MyProc->backendId`.

This still preserves `PGPROC.pid` for process-mode callers and for the many
subsystems that still signal or monitor by PID. The next boundary is to move
the threaded startup guard past `InitProcess()` and prove that PGPROC
allocation plus cleanup can run inside a backend carrier thread without
terminating the postmaster.

## Threaded InitProcess Slice

The nineteenth slice lets threaded startup cross the first shared-memory
backend registration boundary:

- `BACKEND_STARTUP_THREAD` now calls `InitProcess()` after startup-packet
  parsing;
- the thread carrier receives a regular `PGPROC` slot and shared latch before
  the guarded stop;
- the guarded FATAL now fires after PGPROC registration and before
  `PostgresMain()`, authentication, `InitPostgres()`, procsignal participation,
  or normal SQL execution;
- thread exit therefore exercises `ProcKill()` and returns the PGPROC slot to
  its freelist through the backend-exit path;
- the next blockers are now post-startup session lifecycle, authentication
  timeout/cancellation semantics, procsignal identity, and replacing PID-based
  latch wakeups for concurrent backend threads.

This is still only a boundary proof. Sequential threaded startup can register
and clean up a backend, but true concurrent threaded sessions still need the
remaining PID-to-logical-backend migrations before normal SQL execution can be
enabled.

## Session Bootstrap BaseInit Slice

The twentieth slice moves threaded startup into the unwrapped
`PgSessionBootstrap()` path and stops at the first `BaseInit()` subsystem that
is not yet thread-safe:

- `BackendMainWithStartupData(..., BACKEND_STARTUP_THREAD)` now enters
  `PostgresMain()` after `InitProcess()` instead of stopping immediately after
  PGPROC registration;
- `PgSessionBootstrap()` detects thread-per-session runtime state and skips
  backend signal handler installation, because `pqsignal()` changes
  process-global handlers shared with the postmaster and sibling backend
  threads;
- `BaseInit()` preserves existing thread runtime state instead of replacing it
  with process runtime state, while still initializing transaction state;
- the guarded FATAL now fires before `pgstat_initialize()` and later
  `BaseInit()` subsystems;
- an lldb smoke showed that entering `pgstat_attach_shmem()` without this
  guard dereferenced missing per-thread pgstat attachment state and crashed
  the postmaster process, so pgstat attachment is the next concrete subsystem
  to adapt.

This keeps the main-loop unwinding direction: startup now reaches the session
bootstrap function used by the stepped session runner, but still avoids
installing process-global signal state or entering pgstat/shared-cache
subsystems that have not been made backend-local.

## PGStat Shared Anchor Slice

The twenty-first slice adapts the first `BaseInit()` subsystem reached by
threaded startup:

- pgstat now keeps the shared stats control block in a process-wide
  `PG_GLOBAL_SHMEM` anchor rather than only in backend-local `pgStatLocal`;
- `StatsShmemRequest()` fills that process-wide anchor during shared-memory
  setup;
- `StatsShmemInit()` mirrors the process-wide anchor into the postmaster's
  local pgstat state for initialization;
- `pgstat_attach_shmem()` now initializes each backend-local
  `pgStatLocal.shmem` from the process-wide anchor before attaching the DSA
  and dshash state;
- the temporary pre-pgstat guard in `BaseInit()` is removed, so threaded
  startup now crosses pgstat initialization and the rest of `BaseInit()`.

The current guarded stop is after `BaseInit()` and before cancel-key
generation, `InitPostgres()`, authentication, procsignal participation, and
normal SQL execution. The next boundary is database/session initialization,
where the remaining process signal, cancel-key, procsignal, and authentication
timeout assumptions become visible.

## Cancel Key Boundary Slice

The twenty-second slice moves the guarded stop past backend cancel-key
generation:

- `PgSessionBootstrap()` still skips process-global signal-handler
  installation for thread carriers;
- thread carriers no longer call `sigprocmask(SIG_SETMASK, &UnBlockSig, NULL)`
  while entering session bootstrap;
- `MyCancelKey` and `MyCancelKeyLength` are already backend-local TLS state, so
  threaded startup can generate a normal cancellation key without sharing it
  with the postmaster or sibling backends;
- the guarded FATAL now fires after cancel-key generation and before
  `InitPostgres()`.

The next boundary is still larger than the preceding ones: `InitPostgres()`
registers the backend in procsignal state, starts database/session
initialization, performs authentication, and advertises cancellation metadata.
Those pieces need logical-backend identity and timeout/interrupt handling
rather than process-wide signal assumptions.

## ProcSignal Logical Identity Slice

The twenty-third slice moves the guarded stop into `InitPostgres()` after the
first procsignal registration boundary:

- `ProcSignalSlot` now carries `pss_backendId` beside the historical
  `pss_pid`;
- `ProcSignalInit()` records `PgCurrentBackendId()` in the slot while
  preserving existing PID publication for process-mode callers;
- `CleanupProcSignalState()` clears the logical id when the slot is released;
- threaded startup now crosses `InitProcessPhase2()`, pgstat backend status
  initialization, shared-invalidation backend initialization,
  `ProcSignalInit()`, and local data-checksum state initialization before the
  guarded stop;
- the guarded FATAL now fires before backend timeout registration, catalog
  initialization, authentication, and normal session startup.

This is not yet logical procsignal delivery. `SendProcSignal()` still uses PID
and `SIGUSR1`, so concurrent threaded backends would still need delivery by
logical backend/proc number plus latch wakeup rather than process signal.

## Timeout Registration Boundary Slice

The twenty-fourth slice moves the guarded stop past backend timeout handler
registration without installing process-global timeout delivery in the thread
carrier:

- timeout initialization is split into backend-local timeout state reset and
  process SIGALRM handler installation;
- process backends still use `InitializeTimeouts()`, preserving the historical
  `SIGALRM` handler setup;
- threaded backend carriers call `InitializeLogicalTimeouts()` from
  `PgSessionBootstrap()`, resetting their backend-local timeout table without
  changing the process signal handler shared with the postmaster and sibling
  backend threads;
- threaded startup now registers the regular backend timeout handlers in
  `InitPostgres()` and then stops before authentication timeout arming,
  catalog initialization, or normal session lifetime;
- the guarded FATAL explicitly records that handler registration is safe enough
  to cross, but timeout delivery still needs a logical backend timer path
  before authentication and later session execution can run.

The next blocker is not `RegisterTimeout()` itself. It is the first
`enable_timeout_after()` use in authentication, plus the broader question of
how logical backend timeouts should wake and interrupt one carrier without
using process-wide `setitimer()`/`SIGALRM`.

## Authentication Deadline Boundary Slice

The twenty-fifth slice lets threaded startup complete client authentication
without arming process-global timeout delivery:

- `IsUnderPostmaster` is now carrier-local TLS, so the postmaster can continue
  to see itself as the supervisor while backend carrier threads see themselves
  as postmaster-managed children;
- backend carrier threads now initialize the same local latch wait set that
  process children get from `InitPostmasterChild()`;
- `PerformAuthentication()` keeps process backends on the existing
  `STATEMENT_TIMEOUT`/`SIGALRM` authentication guard;
- threaded backends use the connection-local `Port` read deadline already
  checked by `secure_read()`, then clear it after authentication succeeds;
- threaded startup now crosses relation/cache setup, starts the initial
  transaction, runs HBA/client authentication, and stops immediately after
  successful authentication before role identity initialization.

Two lldb smokes shaped this slice. First, a backend carrier with shared
process-wide `IsUnderPostmaster=false` incorrectly entered standalone
`StartupXLOG()`. After making that flag carrier-local, catalog I/O during auth
then reached `WaitLatch()` before the carrier had initialized its latch wait
set. Both are now explicit carrier startup responsibilities.

The next boundary is role/session identity and database validation:
`InitializeSessionUserId()`, system-user initialization, `superuser()`, and
the connection-limit/database checks all need to run with backend-local
lifetime assumptions before the guard can move to the end of `InitPostgres()`.

## Role Identity GUC Boundary Slice

The twenty-sixth slice lets threaded startup initialize role/session identity
after authentication:

- threaded backend carriers now build their own GUC lookup table before
  `InitializeSessionUserId()` calls `SetConfigOption()`;
- the early GUC bridge initializes only `session_authorization` and `role`,
  because running full `InitializeGUCOptions()` inside a thread would reset
  shared postmaster/runtime GUC storage to boot defaults;
- role lookup, `SetAuthenticatedUserId()`, session authorization assignment,
  `SET ROLE NONE`, optional system-user initialization, `superuser()`, and
  `pgstat_bestart_security()` now complete before the guarded stop;
- the guarded FATAL now fires before database validation, connection-limit
  checks, per-database/per-role setting application, and post-startup session
  lifetime.

An lldb smoke without this bridge crashed in `find_option()` because
`guc_hashtab` is thread-local and had not been built for the backend carrier.
The bridge is deliberately narrow: it proves the role identity boundary while
leaving full per-session GUC state adoption as a Phase 10 blocker before
arbitrary SQL can run.

## Database Identity Boundary Slice

The twenty-seventh slice moves threaded startup through database identity and
storage-path setup:

- after role identity, threaded startup now runs the regular reserved-slot and
  replication privilege checks;
- the carrier looks up and locks the target database, rechecks that it still
  exists, sets `MyDatabaseId`, records it in `MyProc->databaseId`, and
  invalidates the startup catalog snapshot;
- the carrier validates the database directory and `PG_VERSION`, installs the
  database path, runs relcache phase 3, and initializes the ACL framework;
- the guarded FATAL now fires immediately before `CheckMyDatabase()`.

The next blocker is deliberately explicit. `CheckMyDatabase()` both applies
database-specific GUC state and calls `pg_perm_setlocale(LC_CTYPE, ...)`.
That is not a safe threaded-backend operation as-is, because it can mutate
process-global locale state shared with the postmaster and sibling carriers.
The next Phase 10 slice needs a thread-safe database GUC/locale policy before
startup settings and arbitrary SQL can run.

## Database GUC And Locale Boundary Slice

The twenty-eighth slice lets threaded startup cross `CheckMyDatabase()`:

- `server_encoding`'s GUC backing string is now session-local TLS, matching
  the already session-local `DatabaseEncoding`, `ClientEncoding`, and
  `MessageEncoding` state;
- the early threaded GUC bridge now initializes the `server_encoding` and
  `client_encoding` GUC records in addition to `session_authorization` and
  `role`;
- `CheckMyDatabase()` can set database encoding, record `server_encoding`,
  seed `client_encoding`, check CONNECT/database limits, and run collation
  version warnings in a backend carrier thread;
- threaded carriers do not call `pg_perm_setlocale(LC_CTYPE, ...)`; for now,
  they require the database LC_CTYPE to match the postmaster's current process
  LC_CTYPE and then update only backend-local message encoding state;
- the guarded FATAL now fires after database-specific GUC and locale
  validation, before startup-packet GUC options, `pg_db_role_setting`
  application, default session state initialization, session preload
  libraries, final pgstat startup, and transaction commit.

This is a conservative first locale policy. It supports the normal same-locale
cluster case without mutating process-global locale state from a backend
thread. A later threaded runtime needs a broader per-database locale strategy
before it can support clusters where database LC_CTYPE differs from the
postmaster process LC_CTYPE.

## Startup Options Boundary Slice

The twenty-ninth slice lets threaded startup process startup-packet GUC
options:

- `process_startup_options()` now runs for threaded backend carriers after
  database-specific GUC and locale validation;
- this covers command-line options embedded in the startup packet and
  additional startup-packet GUC pairs such as `application_name`;
- the guarded FATAL now fires after startup-packet options and before
  `pg_db_role_setting` entries, default session state initialization, session
  preload libraries, final pgstat startup, and transaction commit.

This is still not full GUC session adoption. Startup options set concrete
values supplied by the client, while `pg_db_role_setting` can apply arbitrary
database/user defaults and exposes the broader reset/default semantics that
still need a more complete per-session GUC initialization policy.

## pg_db_role_setting Boundary Slice

The thirtieth slice lets threaded startup apply database/user catalog settings:

- `process_settings()` now runs for threaded backend carriers after
  startup-packet GUC options;
- the early threaded GUC bridge initializes the `wal_consistency_checking` GUC
  record, because applying catalog settings can scan catalogs and dirty hint
  bits before arbitrary SQL has started;
- a process-mode-created `ALTER DATABASE postgres SET application_name = ...`
  setting is applied during threaded startup before the guard fires;
- the guarded FATAL now fires after startup-packet options and
  `pg_db_role_setting` application, before default session state
  initialization, `PostAuthDelay`, session preload libraries, final pgstat
  startup, transaction commit, and normal session lifetime.

An lldb smoke without the `wal_consistency_checking` bridge crashed in
`XLogRecordAssemble()` while catalog scans inside `ApplySetting()` dirtied hint
bits and consulted the uninitialized thread-local WAL consistency array. The
bridge is still intentionally narrow: it initializes only the GUC records that
the current threaded startup path can reach before the guard.

## Default Session State Boundary Slice

The thirty-first slice lets threaded startup initialize default session state:

- `PostAuthDelay`, if configured, is now reached after all startup and
  catalog-backed GUC settings are applied;
- `InitializeSearchPath()` runs for threaded backend carriers, installing the
  default namespace/search-path invalidation state;
- `InitializeClientEncoding()` completes backend-local client/server encoding
  conversion setup;
- `InitializeSession()` creates the legacy `CurrentSession` object, and
  `PgProcessRuntimeAttachSession()` attaches it to the current runtime session
  object;
- the guarded FATAL now fires before session-preload libraries, final pgstat
  publication, startup transaction commit, connection warnings, and normal
  session lifetime.

This proves the sequential default-session startup boundary. It does not yet
prove that the syscache callback registry used by `InitializeSearchPath()` is
ready for concurrent threaded SQL execution; that remains part of the broader
thread-safety floor before arbitrary SQL can run.

## Session Preload Boundary Slice

The thirty-second slice lets threaded startup reach session preload library
processing:

- threaded startup now runs `process_session_preload_libraries()` after
  default session state initialization;
- empty `session_preload_libraries` and `local_preload_libraries` lists cross
  the regular backend-start path in a carrier thread;
- nonempty preload lists still use the Phase 7 extension backend-model gate
  in `load_file()`/`dfmgr.c`, so process-only modules should be rejected in
  threaded mode unless they opt into a compatible backend model;
- the guarded FATAL now fires before `pgstat_bestart_final()`, startup
  transaction commit, connection warnings, normal-processing startup, and the
  query loop.

An attempted boundary after the full `pgstat_bestart_final()` was intentionally
backed out. A fast two-connection smoke could produce
`unsupported byval length: 0`, and a debugger-assisted run later saw an
autovacuum worker segfault followed by recovery waiting on a ProcSignalBarrier.
That made it necessary to split backend-status finalization from backend stats
entry/appname publication before crossing startup transaction commit.

## Final Backend Status Boundary Slice

The thirty-third slice splits final backend-status publication from backend
stats entry creation:

- `pgstat_bestart_final_status()` now finalizes the shared
  `PgBackendStatus` row by publishing the database id, user id, and
  non-starting backend state;
- the existing `pgstat_bestart_final()` keeps process-mode behavior by calling
  the status helper and then creating the backend stats entry and reporting
  `application_name`;
- threaded startup calls only the status helper and then stops at the guarded
  FATAL;
- the guarded FATAL now fires before backend stats entry creation,
  `application_name` reporting, startup transaction commit, connection
  warnings, normal-processing startup, and the query loop.

This keeps the visible backend-status row transition separate from the
unstable stats/appname path exposed by the failed full-finalization attempt.
The next boundary is to make backend stats entry lifecycle safe for
thread-backed backends whose carrier can overlap with process workers and
sibling backend carriers.

## Application Name Status Boundary Slice

The thirty-fourth slice lets threaded startup report `application_name` into
the finalized backend-status row:

- the threaded path now calls `pgstat_report_appname(application_name)` after
  `pgstat_bestart_final_status()`;
- explicit startup-packet `application_name` values therefore cross the
  backend-status publication path in a carrier thread;
- backend stats entry creation remains guarded, so the unstable
  `pgstat_create_backend()` lifecycle is still not reached by threaded
  startup;
- the guarded FATAL now fires before backend stats entry creation, startup
  transaction commit, connection warnings, normal-processing startup, and the
  query loop.

This narrows the previous pgstat blocker further: appname reporting into
`PgBackendStatus` is stable in the sequential two-client smoke, while backend
stats entry lifecycle remains the next concrete boundary.

## Backend Stats Entry And Startup Serialization Slice

The thirty-fifth slice lets threaded startup create the per-backend statistics
entry while adding an explicit temporary guard around concurrent threaded
startup:

- after final backend-status and `application_name` publication, threaded
  startup now calls `pgstat_create_backend(MyProcNumber)` for backend types
  that track backend statistics;
- the guarded FATAL now fires after backend stats entry creation and before
  startup transaction commit, connection warnings, normal-processing startup,
  and the query loop;
- the backend carrier holds a runtime-wide startup/session gate while it runs
  `BackendMainWithStartupData(..., BACKEND_STARTUP_THREAD)`;
- that gate is deliberately temporary: a 20-client concurrent startup smoke
  without it produced catalog/cache failures such as
  `could not find tuple for opclass 112`, showing that the current path can
  reach cache-heavy initialization before those backend-local caches have been
  isolated for same-address-space concurrency;
- `backend_thread_finish()` releases the gate before exiting the carrier
  thread, so the postmaster-owned join/reap path still controls PMChild slot
  release.

This is not the final thread-per-session concurrency model.  It is a safety
floor that makes the current threaded prototype deterministic while later
Phase 10 work moves catalog/cache, transaction commit, interrupt, and session
lifetime state onto thread-safe backend/session owners.  The gate must be
removed or narrowed before normal concurrent SQL execution can be considered
complete.

An attempted follow-up boundary after `CommitTransactionCommand()` was not
kept.  Five sequential clients could cross startup transaction commit and
connection-warning emission, but a 20-client concurrent smoke still produced
`could not find tuple for opclass 112` and `unsupported byval length: 0`
failures even with the temporary startup/session gate.  That means the next
commit boundary needs a stronger transaction-end and cache/lifetime policy,
not just a later guard in `InitPostgres()`.

## One-Step Session Boundary Slice

The thirty-sixth slice moves the guarded stop into the unwrapped session
runner after one protocol step:

- threaded startup now completes `PgSessionBootstrap()` and enters
  `PgSessionRun()`;
- `PgSessionRun()` uses the existing single-message step budget and calls
  `PgSessionStep()` before the threaded guard fires;
- a simple-query client can therefore receive `ReadyForQuery`, send one
  protocol message, execute the query, receive the result, and then see the
  guarded FATAL;
- the guard still prevents multi-step session lifetime, because repeated
  protocol-loop execution, idle waits, transaction cleanup after arbitrary
  commands, and backend-local cache teardown are not yet safe for concurrent
  threaded carriers.

This is the first boundary that proves the stepped main-loop shape with a real
SQL command in the threaded carrier. It still relies on the temporary
startup/session gate and is not a claim that arbitrary threaded sessions can
remain alive across multiple frontend messages.

## Two-Step Session Boundary Slice

The thirty-seventh slice lets threaded sessions reenter the main protocol loop
once before the temporary guard fires:

- `PgSessionRun()` now counts returned `PgSessionStep()` calls for threaded
  backend carriers and guards after two completed protocol steps;
- a normal one-shot `psql -c` connection can execute its query, receive
  `ReadyForQuery` on the second loop iteration, send `Terminate`, and exit
  cleanly without seeing the guard;
- a connection that sends two SQL messages executes both messages and then
  reaches the guarded FATAL before unbounded session lifetime begins;
- process-mode `PgSessionRun()` remains an unbounded loop.

This proves the first repeated main-loop reentry in a threaded backend carrier
and exercises clean client-driven session termination. It still does not remove
the temporary startup/session gate or make long-lived threaded sessions safe;
the next blocker is unbounded idle/read lifetime plus safe backend-local
cleanup after arbitrary command sequences.

## Bounded Session Lifetime Slice

The thirty-eighth slice replaces the two-step stop with a larger temporary
session guard and an explicit idle-read deadline:

- threaded `PgSessionRun()` now allows up to eight returned protocol steps
  before the guarded FATAL fires;
- before each threaded step, the runner arms the existing
  `Port.client_read_deadline` for a short temporary deadline, so a carrier that
  reaches idle client read cannot block forever during this guarded prototype;
- after a threaded step returns, the runner clears that temporary deadline
  before deciding whether the step-count guard should fire;
- one-shot clients still execute, receive `ReadyForQuery`, send `Terminate`,
  and exit cleanly;
- clients that keep sending messages can execute several commands before the
  bounded-session guard stops the carrier;
- process-mode `PgSessionRun()` remains unchanged and unbounded.

This moves the thread-per-session prototype from a two-message proof to a
bounded session-lifetime proof. It is still deliberately temporary: the step
count and idle deadline are guardrails around the current implementation, not
the intended final session policy. Removing them requires safe long-idle wait
ownership, cancellation/timeout routing, and cleanup after arbitrary command
sequences.

## Startup-Only Gate Slice

The thirty-ninth slice narrows the temporary threaded startup serialization
gate:

- `launch_backend.c` now exposes `ThreadedBackendStartupComplete()`, a no-op
  outside backend carrier threads;
- the threaded carrier still enters the startup gate before
  `BackendMainWithStartupData(..., BACKEND_STARTUP_THREAD)`;
- `PgSessionBootstrap()` releases that gate after session bootstrap and loop
  state initialization complete, immediately before `PgSessionRun()` begins
  processing frontend messages;
- the backend-thread exit finalizer still calls the same release helper, so
  FATAL exits before session bootstrap and normal exits after gate release both
  use the same safe cleanup path;
- post-bootstrap bounded session execution can now overlap across backend
  carrier threads.

This is the first proof that the same-address-space threaded backend path can
run multiple regular backend sessions concurrently after startup. It does not
remove the temporary startup gate; catalog/cache-heavy session bootstrap still
needs serialization until the remaining shared initialization state is moved
behind backend/session ownership.

## Unbounded Session Loop Slice

The fortieth slice removes the temporary protocol-step guard and idle-read
deadline from threaded session execution:

- `PgSessionRun()` now uses the same unbounded `PgSessionStep()` loop for
  process and threaded backends;
- thread-per-session carriers can remain idle in the normal client read path
  instead of being forced through the temporary one-second read deadline;
- long idle waits therefore occupy a carrier thread, which is acceptable for
  the Phase 10 thread-per-session target and remains a future scheduler concern
  rather than a reason to keep the temporary guard;
- `InitializeThreadedSessionGUCOptions()` now initializes `search_path` as
  part of the narrow threaded GUC bridge, because error-path testing exposed
  an uninitialized thread-local namespace search path during operator lookup;
- repeated commands, client-driven termination, SQL ERROR recovery, and
  transaction abort/rollback now run through the same stepped session loop in
  threaded mode.

An lldb-assisted smoke without the `search_path` bridge crashed the
postmaster in `nsphash_lookup()` via `fetch_search_path_array()` while a
threaded backend parsed `select 1/0`. Initializing the search-path GUC record
for the backend carrier fixed that namespace-cache crash without broadening
the threaded bridge to full GUC reinitialization.

This still does not complete Phase 10. The startup/session gate remains around
catalog/cache-heavy session bootstrap, and cancellation, termination, timeout
delivery, PL/pgSQL execution, extension rejection, and the Gate D validation
set still need to be proved against the unbounded session loop.

## Logical Signal PID And Threaded Wake Slice

The forty-first slice makes SQL-visible backend ids targetable in threaded
mode and proves cancel/terminate delivery across sibling backend threads:

- `pg_backend_pid()`, `BackendKeyData`, and `pg_stat_activity.pid` now publish
  `PgCurrentBackendSignalPid()`, which remains `MyProcPid` in process mode and
  becomes the logical backend id in thread-per-session mode;
- `BackendSignalPidGetProc()` and `BackendSignalPidIsActive()` resolve either
  a process-backed PID or a thread-backed logical backend id without changing
  raw `BackendPidGetProc()` callers;
- `ProcSignalSlot` now carries a logical backend interrupt mask and optional
  proc-die sender identity for thread-backed backends;
- `pg_cancel_backend()` and `pg_terminate_backend()` still use `kill()` for
  process-backed targets, but route thread-backed targets through
  `SendBackendInterrupt()`;
- `ProcessInterrupts()` now consumes pending logical interrupts from the
  current procsignal slot, so thread-delivered query-cancel and proc-die
  interrupts flow through the existing backend interrupt flags;
- Unix latches now remember the owning pthread and, where available, a direct
  owner wake fd so `SetLatch()` can wake a sibling backend thread that shares
  the same OS pid;
- macOS/kqueue wait sets register a direct `EVFILT_USER` latch wake event and
  record the owning kqueue fd on the latch, avoiding dependence on
  process-wide SIGURG delivery to wake an idle sibling thread;
- backend carrier threads initialize wait-event support before creating their
  local latch, so each carrier has its own wait primitive state.

Validation for this slice:

- clean backend rebuild and install after the `Latch` shared-struct layout
  change;
- threaded smoke with `multithreaded=on`: five concurrent sessions returned
  five distinct SQL-visible backend ids; `pg_cancel_backend()` cancelled a
  running `pg_sleep(30)`; `pg_terminate_backend(pid, 5000)` returned `t` for an
  idle backend and the server logged `terminating connection due to
  administrator command`;
- process-mode smoke with `multithreaded=off`: `select 42,
  pg_backend_pid() > 0` returned `42|t`;
- `gmake -C src/test/modules/test_backend_runtime check` passed.

An earlier incremental build after changing `Latch` layout crashed during
bootstrap in `PGSemaphoreReset()` because stale objects still used the old
`PGPROC.procLatch` size and corrupted the following semaphore field. Any
future change to shared-memory structs embedded in `PGPROC` or installed
backend headers should use a clean backend rebuild before trusting `initdb` or
threaded smoke results.

## Logical Timeout Wait Clamp Slice

The forty-second slice makes backend-local timeouts fire in threaded backends
without sending process signals to the postmaster:

- `InitializeLogicalTimeouts()` now selects logical timeout delivery, preserving
  the existing thread-local timeout queue while suppressing `setitimer()`;
- `WaitEventSetWaitInternal()` clamps each kernel wait to the next logical
  backend timeout and calls the timeout firing path when that deadline is due;
- signal-backed and logical timeout delivery now share the same due-timeout
  firing helper, so timeout order, indicators, target backend/execution, and
  repeating timeout rescheduling remain identical;
- `StatementTimeoutHandler()` and `LockTimeoutHandler()` now send Unix signals
  only for process-backed timeout targets. Thread-backed targets use the
  logical backend mailbox and latch wakeup, avoiding the earlier bug where
  statement timeout sent `SIGINT` to the postmaster pid.

Validation for this slice:

- `gmake -C src/backend/utils/misc timeout.o`,
  `gmake -C src/backend/storage/ipc waiteventset.o`, and
  `gmake -C src/backend/utils/init backend_runtime.o postinit.o` passed;
- full `gmake -j8` and `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- threaded `multithreaded=on` statement-timeout smoke cancelled
  `pg_sleep(5)` with `canceling statement due to statement timeout` and then
  returned `select 42` from a fresh connection, proving the postmaster stayed
  alive;
- threaded `multithreaded=on` idle-session and idle-in-transaction timeout
  smokes terminated idle clients with the expected timeout log messages and
  then returned `select 42` from a fresh connection;
- process-mode `multithreaded=off` statement-timeout smoke still cancelled
  `pg_sleep(5)` and left the server healthy;
- `gmake -C src/test/modules/test_backend_runtime check` passed.

This still does not complete Phase 10. Logical cancel, terminate, and timeout
delivery now work for regular threaded sessions, but PL/pgSQL execution,
extension rejection/acceptance behavior in a live threaded session, broader
session cleanup stress, and Gate D remained to be proved at this point.

## PL/pgSQL Thread-Per-Session Slice

The forty-third slice enables PL/pgSQL in the live thread-per-session runtime:

- PL/pgSQL's mutable module-scope session and backend state is now bridged
  through `PG_THREAD_LOCAL PG_GLOBAL_SESSION` storage for the
  thread-per-session runtime;
- `plpgsql` advertises `PG_BACKEND_MODEL_THREAD_PER_SESSION` in its module
  magic, so the extension backend-model gate can load it in threaded sessions;
- PL/pgSQL module initialization is now idempotent per backend thread, because
  `_PG_init()` runs only when the dynamic library is loaded into the process,
  while later backend threads still need their own custom GUC records,
  transaction callbacks, and rendezvous pointer;
- `InitializeThreadedSessionGUCOptions()` now initializes
  `dynamic_library_path`, because C-language function validation loads
  `plpgsql` through the dynamic loader before the wider per-session GUC bridge
  is complete;
- the extension backend-model regression now expects PL/pgSQL to load under
  the `thread-per-session` runtime model while default process-only test
  modules remain rejected.

Validation for this slice:

- full `gmake -j8` and `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- threaded `multithreaded=on` PL/pgSQL smoke created and executed a PL/pgSQL
  function and `DO` block, verified `dynamic_library_path = '$libdir'`, and
  verified `plpgsql.variable_conflict = error` in both the first loading
  backend thread and a later backend thread after the library was already
  loaded process-wide;
- threaded `multithreaded=on` concurrent PL/pgSQL smoke ran five sibling
  backend sessions through the same PL/pgSQL function and returned the
  expected `11`, `22`, `33`, `44`, and `55` results;
- threaded `multithreaded=on` live extension-model smoke loaded
  `test_ext_threaded`, `test_ext_backend_model`, and `plpgsql`, rejected the
  default process-only `test_ext` module with the expected backend-model
  mismatch, and returned `select 42` afterward;
- process-mode `gmake -C src/pl/plpgsql/src check` passed;
- the direct process-mode `pg_regress` run for `test_extensions`,
  `test_extdepend`, `test_ext_backend_model`, and
  `test_ext_backend_model_pooled` passed after patching the recreated macOS
  temp-install binaries;
- `gmake -C src/test/modules/test_backend_runtime check` passed.

This still does not complete Phase 10. PL/pgSQL and live extension-model
behavior now work in regular threaded sessions, but broader session cleanup
stress and Gate D remain to be proved before Phase 10 can close.

## Threaded Cleanup Stress Slice

The forty-fourth slice stabilizes broader threaded session cleanup stress and
blocks unsafe late process-worker launches after thread carriers exist:

- after the postmaster successfully creates any backend thread carrier,
  `postmaster_child_launch_carrier()` rejects later fork-without-exec process
  launches in threaded mode with `ENOSYS`;
- at Phase 10 close, autovacuum workers were temporarily disabled in threaded
  mode and logged a single notice when the launcher first tried to start one;
- at Phase 10 close, `InitializeParallelDSM()` suppressed process-backed
  parallel workers in threaded mode, so parallel query, parallel index build,
  and parallel vacuum callers fell back to leader-only execution instead of
  attempting dynamic background worker registration. Phase 11 supersedes this
  temporary restriction with thread-backed core parallel workers;
- backend carrier threads seed their thread-local `pg_global_prng_state`
  before normal session work can create DSM handles;
- the narrow threaded GUC bridge now initializes `default_tablespace` and
  `temp_tablespaces`, because table creation can reach index-build temp file
  setup before full per-session GUC adoption exists.

This slice deliberately did not implement threaded autovacuum, parallel
workers, or other server-owned worker families. Those belong to Phase 11.
The Phase 10 policy is stricter than "workers remain processes": startup-time
process workers that exist before regular backend threads are still tolerated,
but normal threaded server mode must not fork new server-owned subprocesses
after thread carriers have started.

Validation for this slice:

- `gmake -C src/backend/postmaster launch_backend.o autovacuum.o` passed;
- `gmake -C src/backend/utils/misc guc.o` passed;
- `gmake -C src/backend/access/transam parallel.o` passed;
- full `gmake -j8` and `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- threaded `multithreaded=on` DDL smoke created a table with a primary key,
  exercising the table/toast/index path that previously reached parallel DSM
  and temp-tablespace setup;
- threaded cleanup stress with 25 clean clients, 20 killed/abandoned clients,
  20 error-recovery clients, and 80 reconnects finished with 25 committed
  rows, zero leaked advisory locks, zero prepared statements in the checker
  session, zero active or idle-in-transaction sibling client backends, and a
  final successful `select 42`;
- the threaded stress observed the expected
  `autovacuum workers are disabled in multithreaded mode` log and no `PANIC`,
  `FATAL`, crash, terminated-server-process, or tuple/cache corruption log
  pattern;
- process-mode `multithreaded=off` smoke created, populated, vacuum-analyzed,
  and counted a table without logging the threaded autovacuum deferral;
- `gmake -C src/test/modules/test_backend_runtime check` passed.

## Threaded Runtime TAP Smoke Slice

The forty-fifth slice adds an in-tree TAP smoke for the live
thread-per-session runtime under `src/test/modules/test_backend_runtime`:

- the module's makefile and meson metadata now register
  `t/001_threaded_runtime.pl`;
- the TAP test starts a `multithreaded=on` temp instance;
- it verifies the threaded GUC state, DDL with a primary key, concurrent
  client sessions with distinct SQL-visible backend ids, active query
  cancellation, idle backend termination, SQL error recovery, PL/pgSQL
  execution, live process-only module rejection, abandoned idle-client
  advisory-lock cleanup, transaction-abort cleanup, repeated
  connect/disconnect, and final connection health;
- the test also rejects common crash/corruption log signatures after the
  threaded smoke.

This TAP test is still narrower than the full Gate D manual stress. It is a
compact smoke rather than the larger repeated connect/disconnect and
killed-client stress recorded above.

Validation for this slice:

- `PERL5LIB="$HOME/perl5/lib/perl5:$PWD/src/test/perl" perl -c
  src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed;
- direct TAP run passed with the temp install on `PATH`, `PG_REGRESS` set to
  `src/test/regress/pg_regress`, and local `IPC::Run` available through
  `PERL5LIB`;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP in this checkout because it is not configured
  with `--enable-tap-tests`.

The next slice broadened the TAP smoke's Gate D coverage:

- active `pg_sleep()` cancellation is driven by a direct background `psql`
  process and verifies the cancellation error reaches the client;
- idle backend termination verifies the logical backend id leaves
  `pg_stat_activity`;
- a SQL `ERROR` is followed by a fresh successful query;
- live process-only module rejection uses `LOAD 'test_backend_runtime'` and
  verifies both the backend-model mismatch and subsequent server health;
- abandoned idle-client cleanup now uses `BackgroundPsql` to hold an advisory
  lock while idle in transaction, then kills the client and verifies that the
  lock is released;
- transaction-abort cleanup verifies a transaction-scoped advisory lock is
  released after an error aborts the transaction;
- repeated connect/disconnect opens 30 fresh threaded client sessions and
  verifies each one can execute SQL.

Validation for this broader TAP slice:

- `PERL5LIB="$HOME/perl5/lib/perl5:$PWD/src/test/perl" perl -c
  src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed its SQL
  regression and skipped TAP in this checkout because it is not configured
  with `--enable-tap-tests`;
- direct TAP run passed with 23 tests.

## Gate D Validation

Gate D is complete using the plan's documented near-equivalent rule for local
platform/tooling issues that make literal `check-world` noisy on this macOS
checkout.

The process-mode control group was exercised by repeated full builds,
installs, module checks, and a top-level `gmake -j8 check-world` attempt. The
literal top-level command reached the expected broad process-mode suites but
hit local dynamic-library install-name problems unrelated to PostgreSQL
behavior:

- the first run failed when all 67 ECPG test executables aborted while looking
  for `/usr/local/pgsql/lib` dynamic libraries from build-tree install names;
- after patching the build-tree ECPG dynamic-library IDs to the temp install,
  `gmake -C src/interfaces/ecpg check` passed all 67 tests;
- a later `check-world` run showed the core `src/test/regress` suite passing
  all 245 tests and `src/test/isolation` passing all 129 tests before the run
  reached `src/test/modules/test_extensions`;
- `src/test/modules/test_extensions` then failed before SQL execution because
  the recreated temp-install `initdb` still referenced
  `/usr/local/pgsql/lib/libpq.5.dylib`;
- after patching the recreated temp-install binaries,
  direct `pg_regress` for `test_extensions`, `test_extdepend`,
  `test_ext_backend_model`, and `test_ext_backend_model_pooled` passed all 4
  tests.

The threaded-mode Gate D subset is covered by
`src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and the
larger manual smokes recorded above:

- multiple concurrent threaded client sessions with distinct logical backend
  ids;
- running-query cancellation and idle backend termination through logical
  backend ids;
- SQL `ERROR` recovery and transaction-abort advisory-lock cleanup;
- abandoned idle-client cleanup;
- repeated connect/disconnect;
- PL/pgSQL execution;
- incompatible process-only module rejection with post-error server health;
- DDL with primary-key index build;
- final connection health plus crash/corruption log signature checks.

The late-worker policy required by Gate D is implemented as follows:

- `postmaster_child_launch_carrier()` rejects late fork-without-exec process
  launches with `ENOSYS` after any backend thread carrier has started in
  threaded mode;
- at Gate D close, `do_start_worker()` disabled autovacuum worker starts in
  threaded mode and logged a single deferral notice. Phase 11 supersedes that
  deferral with an autovacuum worker thread carrier;
- at Gate D close, `InitializeParallelDSM()` suppressed process-backed
  parallel workers in threaded sessions, allowing existing callers to run
  leader-only where PostgreSQL supports that fallback. Phase 11 supersedes
  this deferral with thread-backed core parallel workers;
- all remaining in-tree server-owned worker families are explicitly Phase 11
  work, not Phase 10 regular client backend work.

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
- after the InitProcess boundary slice,
  `gmake -C src/backend/tcop backend_startup.o` passed;
- after the InitProcess boundary slice, full `gmake -C src/backend -j8` passed;
- after the InitProcess boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the InitProcess boundary
  slice read real libpq startup packets for two client connections, returned
  from `BackendInitialize()`, rejected both immediately before `InitProcess()`
  with the guarded "threaded backend shared-memory registration is not
  implemented yet" FATAL, kept the postmaster running between connections, and
  completed normal fast shutdown.
- after the logical backend ID slice,
  `gmake -C src/backend/utils/init backend_runtime.o` and
  `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed;
- after the logical backend ID slice, an initial temp-install regression run
  exposed stale backend object layout from the `PgBackend` struct change; a
  backend clean, generated-header recovery, and full `gmake -C src/backend -j8`
  rebuild passed and removed the bootstrap crash;
- after the logical backend ID slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed, including the
  distinct logical backend id regression.
- a temp install smoke with `multithreaded=on` after the logical backend ID
  slice still read real libpq startup packets for two client connections,
  rejected both immediately before `InitProcess()` with the guarded
  "threaded backend shared-memory registration is not implemented yet" FATAL,
  kept the postmaster running between connections, and completed normal fast
  shutdown.
- after the PGPROC logical identity slice,
  `gmake -C src/backend/storage/lmgr proc.o`,
  `gmake -C src/backend/utils/init backend_runtime.o`, and
  `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed;
- after the PGPROC logical identity slice, a backend clean, generated-header
  recovery, and full `gmake -C src/backend -j8` rebuild passed;
- after the PGPROC logical identity slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed, including
  the SQL-backend `PGPROC.backendId` regression.
- after the threaded InitProcess slice,
  `gmake -C src/backend/tcop backend_startup.o` passed;
- after the threaded InitProcess slice, full `gmake -C src/backend -j8`
  passed;
- after the threaded InitProcess slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the threaded InitProcess
  slice read real libpq startup packets for two client connections, entered
  `InitProcess()`, rejected both immediately after PGPROC registration with
  the guarded "threaded backend session execution is not implemented yet"
  FATAL, kept the postmaster running between connections, and completed normal
  fast shutdown.
- after the session bootstrap BaseInit slice,
  `gmake -C src/backend/utils/init postinit.o` and
  `gmake -C src/backend/tcop backend_startup.o postgres.o` passed;
- after the session bootstrap BaseInit slice, full `gmake -C src/backend -j8`
  passed and `gmake DESTDIR="$PWD/tmp_install" install` completed;
- an lldb smoke without the pre-pgstat guard stopped in
  `pgstat_attach_shmem()` on the backend carrier thread, confirming pgstat
  shared-memory attachment as the next boundary;
- after adding the pre-pgstat guard,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the session bootstrap
  BaseInit slice read real libpq startup packets for two client connections,
  entered `PostgresMain()`/`PgSessionBootstrap()`, preserved thread runtime
  state through transaction-state initialization, rejected both before pgstat
  initialization with the guarded "threaded backend base initialization is not
  implemented yet" FATAL, kept the postmaster running between connections, and
  completed normal fast shutdown.
- after the PGStat shared anchor slice,
  `gmake -C src/backend/utils/activity pgstat_shmem.o` and
  `gmake -C src/backend/utils/init postinit.o` passed;
- after the PGStat shared anchor slice, full `gmake -C src/backend -j8`
  passed and `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the PGStat shared anchor slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the PGStat shared anchor
  slice read real libpq startup packets for two client connections, entered
  `PostgresMain()`/`PgSessionBootstrap()`, completed `BaseInit()` including
  pgstat attachment through the process-wide pgstat anchor, rejected both after
  `BaseInit()` with the guarded "threaded backend database initialization is
  not implemented yet" FATAL, kept the postmaster running between connections,
  and completed normal fast shutdown.
- after the cancel key boundary slice, `gmake -C src/backend/tcop postgres.o`
  passed;
- after the cancel key boundary slice, full `gmake -C src/backend -j8` passed
  and `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the cancel key boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the cancel key boundary
  slice read real libpq startup packets for two client connections, completed
  `BaseInit()`, generated backend-local cancel keys, rejected both before
  `InitPostgres()` with the guarded "threaded backend database initialization
  is not implemented yet" FATAL, kept the postmaster running between
  connections, and completed normal fast shutdown.
- after the ProcSignal logical identity slice,
  `gmake -C src/backend/storage/ipc procsignal.o`,
  `gmake -C src/backend/utils/init postinit.o`, and
  `gmake -C src/backend/tcop postgres.o` passed;
- after the ProcSignal logical identity slice, full
  `gmake -C src/backend -j8` passed and
  `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the ProcSignal logical identity slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the ProcSignal logical
  identity slice read real libpq startup packets for two client connections,
  crossed `InitPostgres()` far enough to register ProcArray, pgstat backend
  status, shared-invalidation, and procsignal state, rejected both before
  timeout registration with the guarded "threaded backend database
  initialization is not implemented yet" FATAL, kept the postmaster running
  between connections, and completed normal fast shutdown.
- after the timeout registration boundary slice,
  `gmake -C src/backend/utils/misc timeout.o`,
  `gmake -C src/backend/tcop postgres.o`, and
  `gmake -C src/backend/utils/init postinit.o` passed;
- after the timeout registration boundary slice, full
  `gmake -C src/backend -j8` passed and
  `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the timeout registration boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the timeout registration
  boundary slice read real libpq startup packets for two client connections,
  crossed `InitPostgres()` far enough to register backend timeout handlers
  without installing `SIGALRM`, rejected both before authentication timeout
  arming with the guarded "threaded backend database initialization is not
  implemented yet" FATAL, kept the postmaster running between connections, and
  completed normal fast shutdown.
- after the authentication deadline boundary slice,
  `gmake -C src/backend/utils/init postinit.o` and
  `gmake -C src/backend/postmaster launch_backend.o` passed;
- after changing exported `IsUnderPostmaster` storage to TLS, a backend clean,
  generated-header recovery, full `gmake -C src/backend -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed;
- after the authentication deadline boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the authentication
  deadline boundary slice read real libpq startup packets for two client
  connections, completed catalog-backed trust authentication using the
  connection-local deadline rather than `SIGALRM`, rejected both immediately
  after authentication with the guarded "threaded backend database
  initialization is not implemented yet" FATAL, kept the postmaster running
  between connections, and completed normal fast shutdown.
- after the role identity GUC boundary slice,
  `gmake -C src/backend/utils/misc guc.o` and
  `gmake -C src/backend/utils/init postinit.o` passed;
- after the role identity GUC boundary slice, full
  `gmake -C src/backend -j8` passed and
  `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the role identity GUC boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the role identity GUC
  boundary slice read real libpq startup packets for two client connections,
  completed catalog-backed trust authentication, initialized role/session
  identity with thread-local GUC lookup state, rejected both before database
  validation with the guarded "threaded backend database initialization is not
  implemented yet" FATAL, kept the postmaster running between connections, and
  completed normal fast shutdown.
- after the database identity boundary slice,
  `gmake -C src/backend/utils/init postinit.o` passed;
- after the database identity boundary slice, full
  `gmake -C src/backend -j8` passed and
  `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the database identity boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the database identity
  boundary slice read real libpq startup packets for two client connections,
  completed authentication and role identity, crossed database lookup/lock,
  database path validation, relcache phase 3, and ACL setup, rejected both
  before `CheckMyDatabase()` with the guarded "threaded backend database
  initialization is not implemented yet" FATAL, kept the postmaster running
  between connections, and completed normal fast shutdown.
- after the database GUC and locale boundary slice,
  `gmake -C src/backend/utils/init postinit.o` and
  `gmake -C src/backend/utils/misc guc.o guc_tables.o` passed;
- after the database GUC and locale boundary slice, full
  `gmake -C src/backend -j8` passed and
  `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the database GUC and locale boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the database GUC and
  locale boundary slice read real libpq startup packets for two client
  connections, completed authentication, role identity, database identity,
  database-specific GUC setup, and locale validation, rejected both before
  startup options and `pg_db_role_setting` application with the guarded
  "threaded backend database initialization is not implemented yet" FATAL,
  kept the postmaster running between connections, and completed normal fast
  shutdown.
- after the startup options boundary slice,
  `gmake -C src/backend/utils/init postinit.o` passed;
- after the startup options boundary slice, full
  `gmake -C src/backend -j8` passed and
  `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the startup options boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke with `multithreaded=on` after the startup options
  boundary slice used psql connection strings with explicit
  `application_name` values, completed startup-packet GUC option processing,
  rejected both connections before `pg_db_role_setting` with the guarded
  "threaded backend database initialization is not implemented yet" FATAL,
  kept the postmaster running between connections, and completed normal fast
  shutdown.
- after the `pg_db_role_setting` boundary slice,
  `gmake -C src/backend/utils/misc guc.o` and
  `gmake -C src/backend/utils/init postinit.o` passed;
- after the `pg_db_role_setting` boundary slice, full
  `gmake -C src/backend -j8` passed and
  `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the `pg_db_role_setting` boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke created `ALTER DATABASE postgres SET application_name`
  in process mode, restarted with `multithreaded=on`, completed
  startup-packet GUC option processing and `pg_db_role_setting` application for
  two threaded client attempts, rejected both before default session state with
  the guarded "threaded backend database initialization is not implemented
  yet" FATAL, kept the postmaster running between connections, and completed
  normal fast shutdown.
- after the default session state boundary slice,
  `gmake -C src/backend/utils/init postinit.o` passed;
- after the default session state boundary slice, full
  `gmake -C src/backend -j8` passed and
  `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the default session state boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke created `ALTER DATABASE postgres SET application_name`
  in process mode, restarted with `multithreaded=on`, completed
  startup-packet GUC option processing, `pg_db_role_setting` application,
  default search-path/client-encoding initialization, and legacy session
  allocation for two threaded client attempts, rejected both before session
  preload libraries with the guarded "threaded backend database initialization
  is not implemented yet" FATAL, kept the postmaster running between
  connections, and completed normal fast shutdown.
- after the session preload boundary slice,
  `gmake -C src/backend/utils/init postinit.o` passed;
- after the session preload boundary slice, full
  `gmake -C src/backend -j8` passed and
  `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the session preload boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke created `ALTER DATABASE postgres SET application_name`
  in process mode, restarted with `multithreaded=on`, completed
  startup-packet GUC option processing, `pg_db_role_setting` application,
  default session state, and session-preload processing for two immediate
  threaded client attempts, rejected both before final pgstat startup with the
  guarded "threaded backend database initialization is not implemented yet"
  FATAL, kept the postmaster running after a one-second delay, and completed
  normal fast shutdown.
- diagnostic attempts to move the guard after `pgstat_bestart_final()` were
  not kept: one non-debug smoke returned `unsupported byval length: 0` on the
  second immediate threaded client, and a debugger-assisted run later saw an
  autovacuum worker segfault followed by recovery waiting on a
  ProcSignalBarrier. Backend stats entry creation and appname publication
  remain the next blocker.
- after the final backend status boundary slice,
  `gmake -C src/backend/utils/activity backend_status.o` and
  `gmake -C src/backend/utils/init postinit.o` passed;
- after the final backend status boundary slice, full
  `gmake -C src/backend -j8` passed and
  `gmake DESTDIR="$PWD/tmp_install" install` completed;
- after the final backend status boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke created `ALTER DATABASE postgres SET application_name`
  in process mode, restarted with `multithreaded=on`, completed final
  backend-status row publication for two immediate threaded client attempts,
  rejected both before backend stats entry creation with the guarded
  "threaded backend database initialization is not implemented yet" FATAL,
  kept the postmaster running after a one-second delay, and completed normal
  fast shutdown.
- after the application name status boundary slice,
  `gmake -C src/backend/utils/init postinit.o` passed;
- after the application name status boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed;
- a temp install smoke started with `multithreaded=on`, used two immediate
  clients with explicit startup-packet `application_name` values, completed
  final backend-status row publication and `application_name` reporting for
  both attempts, rejected both before backend stats entry creation with the
  guarded "threaded backend database initialization is not implemented yet"
  FATAL, kept the postmaster running after a one-second delay, and completed
  normal fast shutdown.
- before adding the threaded startup/session gate, a 20-client concurrent
  startup smoke with backend stats entry creation enabled produced
  `could not find tuple for opclass 112` failures and left a carrier stuck
  during shutdown, proving that same-address-space concurrent startup is not
  safe yet.
- after the backend stats entry and startup serialization slice,
  `gmake -C src/backend/postmaster launch_backend.o`,
  `gmake -C src/backend/utils/init postinit.o`, full
  `gmake -C src/backend -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed.
- after the backend stats entry and startup serialization slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed.
- a temp install smoke with `multithreaded=on` ran five sequential clients
  with explicit startup-packet `application_name` values, completed backend
  stats entry creation for each, rejected each before startup transaction
  commit with the guarded FATAL, found no `unsupported byval length`,
  opclass, segmentation, trap, or panic failures in the server log, kept the
  postmaster running, and completed normal fast shutdown.
- a temp install smoke with `multithreaded=on` ran 20 concurrent clients.
  The temporary startup/session gate serialized the unsafe threaded startup
  path; every client reached the guarded FATAL after backend stats entry
  creation, the server log contained no byval/opclass/crash failures, the
  postmaster stayed running, and normal fast shutdown completed.
- follow-up validation found that explicit `TopMemoryContext` teardown in the
  thread-exit continuation made the repeated-carrier boundary unstable:
  reverting an experimental launch-gate change and rerunning the 20-client
  smoke produced one intended guarded FATAL followed by
  `could not find tuple for opclass 112`; holding the startup gate through
  memory teardown changed the failure to `unsupported byval length: 0`.
  The committed boundary now deliberately leaves carrier memory-context
  ownership to the future thread-runtime lifecycle instead of destroying it in
  `backend_thread_finish()`. With that teardown removed, a fresh 20-client
  `multithreaded=on` smoke reached the intended guarded FATAL for all clients
  and found no byval/opclass/crash signatures in text logs or client stderr.
- after the thread-exit memory-teardown correction,
  `gmake -C src/backend/postmaster launch_backend.o`, full
  `gmake -C src/backend -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed.
- after the thread-exit memory-teardown correction,
  `gmake -C src/test/modules/test_backend_runtime check` passed.
- after the guarded thread-exit boundary was stabilized, moving the threaded
  guard past `CommitTransactionCommand()` passed
  `gmake -C src/backend/utils/init postinit.o`, full
  `gmake -C src/backend -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install`; a 20-client
  `multithreaded=on` smoke reached the new startup-transaction-commit guard
  for every client with no byval/opclass/crash signatures.
- moving the guard past `EmitConnectionWarnings()` establishes the current
  boundary at full `InitPostgres()` completion. `gmake -C
  src/backend/utils/init postinit.o`, full `gmake -C src/backend -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed; a 20-client
  `multithreaded=on` smoke reached the `InitPostgres completed` guard for
  every client with no byval/opclass/crash signatures.
- after the full `InitPostgres()` boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed.
- moving the guard back into `PgSessionBootstrap()` establishes the current
  boundary at return from `InitPostgres()` into `PostgresMain` setup.
  Threaded carriers now skip the process-backend-only
  `PostmasterContext` deletion because that pointer is runtime-global in the
  threaded server. `gmake -C src/backend/tcop postgres.o`,
  `gmake -C src/backend/utils/init postinit.o`, full
  `gmake -C src/backend -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed; a 20-client
  `multithreaded=on` smoke reached the `threaded backend startup reached
  PostgresMain` guard for every client with no byval/opclass/crash signatures.
- after the `PostgresMain` entry boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed.
- moving the guard to the end of `PgSessionBootstrap()` establishes the
  current boundary at completed session bootstrap, immediately before
  `PgSessionRun()` starts the main protocol loop. This covers normal-mode
  processing state, initial GUC reporting, optional log-disconnection callback
  registration, `pgstat_report_connect()`, backend-key transmission,
  `MessageContext` and row-description context allocation, login event trigger
  dispatch, and loop-state initialization. `gmake -C src/backend/tcop
  postgres.o`, full `gmake -C src/backend -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed; a 20-client
  `multithreaded=on` smoke reached the `threaded backend session bootstrap
  completed` guard for every client with no byval/opclass/crash signatures.
- after the session-bootstrap boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed.
- moving the guard into `PgSessionRun()` after one `PgSessionStep()` passed
  `gmake -C src/backend/tcop postgres.o`, full `gmake -C src/backend -j8`,
  and `gmake DESTDIR="$PWD/tmp_install" install`; a 20-client
  `multithreaded=on` smoke ran `SELECT <n>` through psql for every client,
  observed 20 guarded `threaded backend completed one protocol step` FATALs,
  saw 20 result rows, and found no byval/opclass/crash signatures.
- after the one-step session boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed.
- after the two-step session boundary slice, `git diff --check`,
  `gmake -C src/backend/tcop postgres.o`, full `gmake -C src/backend -j8`,
  and `gmake DESTDIR="$PWD/tmp_install" install` passed.
- a 20-client `multithreaded=on` smoke using one-shot `psql -c "select <n>"`
  connections returned 20 result rows, exited with status 0, produced no
  client stderr, produced no guarded FATALs, and found no
  byval/opclass/crash signatures.
- a 10-client `multithreaded=on` smoke using two SQL messages per connection
  returned 20 result rows and reached the expected `threaded backend completed
  two protocol steps` guard with no byval/opclass/crash signatures.
- after the two-step session boundary slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed.
- a process-mode temp-instance smoke with `multithreaded=off` returned
  `select 42` successfully with no client stderr.
- after the bounded session lifetime slice, `git diff --check`,
  `gmake -C src/backend/tcop postgres.o`, full `gmake -C src/backend -j8`,
  and `gmake DESTDIR="$PWD/tmp_install" install` passed.
- a 20-client `multithreaded=on` smoke using one-shot `psql -c "select <n>"`
  connections returned 20 result rows, exited with status 0, produced no
  client stderr, produced no bounded-session guarded FATALs, and found no
  byval/opclass/crash signatures.
- a five-client `multithreaded=on` smoke using ten SQL messages per connection
  returned 40 result rows before reaching the expected bounded-session guard
  and found no byval/opclass/crash signatures.
- a sequential idle-client `multithreaded=on` smoke connected three clients
  that sent no SQL for longer than the temporary deadline; each reached the
  `Operation timed out` read boundary, the server shut down cleanly, and no
  byval/opclass/crash signatures were found.
- after the bounded session lifetime slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed.
- a process-mode temp-instance smoke with `multithreaded=off` returned
  `select 42` successfully with no client stderr.
- after the startup-only gate slice, `git diff --check`,
  `gmake -C src/backend/postmaster launch_backend.o`,
  `gmake -C src/backend/tcop postgres.o`, full `gmake -C src/backend -j8`,
  and `gmake DESTDIR="$PWD/tmp_install" install` passed.
- a 20-client `multithreaded=on` smoke using one-shot `psql -c "select <n>"`
  connections returned 20 result rows, exited with status 0, produced no
  client stderr, produced no bounded-session guarded FATALs, and found no
  byval/opclass/crash signatures.
- a five-client `multithreaded=on` smoke using ten SQL messages per connection
  returned 40 result rows before reaching the expected bounded-session guard
  and found no byval/opclass/crash signatures.
- a concurrent idle-client `multithreaded=on` smoke connected five clients
  that sent no SQL for longer than the temporary deadline; all five reached
  the `Operation timed out` read boundary, the server shut down cleanly, and
  no byval/opclass/crash signatures were found.
- after the startup-only gate slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed.
- a process-mode temp-instance smoke with `multithreaded=off` returned
  `select 42` successfully with no client stderr.
- after the unbounded session loop slice, `git diff --check`,
  `gmake -C src/backend/tcop postgres.o`,
  `gmake -C src/backend/utils/misc guc.o`, full
  `gmake -C src/backend -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed.
- a 20-client `multithreaded=on` smoke using one-shot `psql -c "select <n>"`
  connections returned 20 result rows, exited with status 0, produced no
  client stderr, produced no temporary guard messages, and found no
  byval/opclass/crash signatures.
- a five-client `multithreaded=on` smoke using 25 SQL messages per connection
  returned 125 result rows, exited with status 0, produced no client stderr,
  produced no temporary guard messages, and found no byval/opclass/crash
  signatures.
- a 10-client `multithreaded=on` idle-wait smoke kept clients connected for
  two seconds before sending SQL; all clients returned their result rows with
  no timeout messages, no temporary guard messages, no client stderr, and no
  byval/opclass/crash signatures.
- after adding `search_path` to the threaded GUC bridge, a five-client
  `multithreaded=on` error-recovery smoke executed `select 1/0` outside and
  inside an explicit transaction, observed the expected 10 division-by-zero
  errors, returned all five post-error rows and all five post-rollback rows,
  and found no byval/opclass/crash signatures.
- a combined `multithreaded=on` smoke reran the one-shot, multi-command, and
  idle client patterns together and returned 20 one-shot rows, 125
  multi-command rows, and 10 idle rows with no temporary guard messages,
  timeout messages, client stderr, or byval/opclass/crash signatures.
- after the unbounded session loop slice,
  `gmake -C src/test/modules/test_backend_runtime check` passed.
