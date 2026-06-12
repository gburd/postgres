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
The next boundary is to make backend stats entry lifecycle and appname
publication safe for thread-backed backends whose carrier can overlap with
process workers and sibling backend carriers.

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
