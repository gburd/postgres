# Phase 12 State Migration Notes

Phase 12 is in progress. The goal is to reduce reliance on thread-local
globals so sessions and executions can eventually move between carriers.

## CurrentSession Compatibility Bridge

The first Phase 12 slice moves ownership of the legacy `CurrentSession`
allocation under the `PgSession` runtime object:

- `PgSession` now has a `legacy_session` pointer;
- `InitializeSession()` first asks the current `PgSession` for its legacy
  session and allocates one in `TopMemoryContext` only when the object does not
  already own one;
- `InitializeSession()` installs the new allocation back onto `PgSession`, so
  `CurrentSession` remains the compatibility pointer used by typcache and
  parallel-query session DSM code;
- `InitPostgres()` no longer needs a process-specific
  `PgProcessRuntimeAttachSession()` call after `InitializeSession()`.

An attempted stronger version embedded `Session` storage directly inside
`PgSession`. That made the object ownership cleaner, but it regressed threaded
parallel CTAS in `001_threaded_runtime.pl`: the test stalled after launching
parallel worker thread carriers for the `threaded_runtime_stress` CTAS. The
safe bridge keeps `Session` allocated in `TopMemoryContext` for now while still
making `PgSession` the owner of the compatibility pointer. A future Phase 12
slice can revisit embedded storage with a focused threaded parallel-query
debugging fixture.

Validation for this slice:

- touched-object builds passed for `session.o`, `backend_runtime.o`, and
  `postinit.o`;
- full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  reinstalling `src/test/modules/test_backend_runtime`;
- direct `prove` over
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  passed all 46 tests. This specifically reproved threaded CTAS, threaded
  parallel query, worker launch/shutdown/restart, cancellation, termination,
  PL/pgSQL, and crash escalation after the session bridge change;
- a direct process-mode temp-cluster smoke forced a two-worker parallel
  aggregate over a 200,000-row table. The plan contained `Gather Merge` with
  `Workers Planned: 2` and returned the expected row count, proving the
  per-session DSM handoff still works when parallel workers attach to the
  leader's session state.

## Interrupt Holdoff Compatibility Bridge

The second Phase 12 slice moves the three historical interrupt holdoff and
critical-section counters under `PgBackend`:

- `PgBackend` now owns a `PgBackendInterruptHoldoffState`;
- `InterruptHoldoffCount`, `QueryCancelHoldoffCount`, and `CritSectionCount`
  remain source-compatible lvalues through macros in `miscadmin.h`;
- the macros route through pointer accessors that return the current
  `PgBackend` fields;
- early startup paths before `CurrentPgBackend` is installed use fallback
  backend-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts any early fallback counts into the
  process backend object before clearing the fallback storage.

This keeps the existing `HOLD_INTERRUPTS()`, `RESUME_INTERRUPTS()`,
`HOLD_CANCEL_INTERRUPTS()`, `RESUME_CANCEL_INTERRUPTS()`,
`START_CRIT_SECTION()`, and `END_CRIT_SECTION()` call sites intact while
making the state part of the logical backend object. Later scheduler work can
therefore ask whether a backend may process interrupts without reading raw
thread-local counter storage.

Validation for this slice:

- a stale incremental build failed at link because many backend objects still
  referenced the old exported counter symbols. Following the local build notes,
  `gmake -C src/backend clean` plus generated-header recovery was used before
  the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`;
- direct `pg_regress` for `src/test/modules/test_backend_runtime` passed. The
  new `test_backend_interrupt_holdoffs_are_backend_local()` function switches
  `CurrentPgBackend` between two fake backend objects and proves the historical
  counter lvalues are distinct per backend;
- direct `prove` over
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  passed all 46 tests after the holdoff-counter migration.

## Debug Query String Execution Bridge

The third Phase 12 slice moves `debug_query_string` under `PgExecution`:

- `PgExecution` now owns a `PgExecutionDebugState`;
- `debug_query_string` remains a source-compatible lvalue macro in
  `tcopprot.h`;
- the macro routes through `PgCurrentDebugQueryStringRef()`, which returns the
  current execution's field;
- early paths before `CurrentPgExecution` is installed use fallback
  execution-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts the early fallback value into the
  process execution object before clearing fallback storage.

This preserves existing logging, error-reporting, parallel-worker, and
background-worker call sites while making the client statement string part of
the logical execution object. That is a small but important step toward
moving a session/execution between carriers without depending on raw TLS for
diagnostic statement state.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `postgres.o`, and
  `test_backend_runtime.o`;
- because `tcopprot.h` changed a former exported execution global into a
  compatibility macro, `gmake -C src/backend clean` plus generated-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_debug_query_string_is_execution_local()`, which switches
  `CurrentPgExecution` between fake executions and proves assignments through
  `debug_query_string` remain isolated per execution;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  after the debug-query-string migration.

## Connection Socket I/O Bridge

The fourth Phase 12 slice moves the internal socket protocol buffers and
message flags from raw connection TLS in `pqcomm.c` under `PgConnection`:

- `PgConnection` now owns a `PgConnectionSocketIOState`;
- the send buffer pointer, send buffer size, send cursor, receive buffer,
  receive cursor, communication-busy flag, and message-read flag are part of
  the connection object;
- `pqcomm.c` keeps its historical local names as macros, so socket protocol
  code remains source-local while storage is object-backed;
- early authentication paths before `BaseInit()` installs
  `CurrentPgConnection` use fallback connection-local storage in
  `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts that early fallback socket state into
  the process connection object.

This is the first bridge for frontend/backend protocol state. It does not yet
move exported connection pointers such as `PqCommMethods` or `FeBeWaitSet`;
those are shared with `pqmq`, WAL sender, SSL/GSS, and latch retargeting and
should move in a separate batch.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `pqcomm.o`, and
  `test_backend_runtime.o`;
- an incremental full build initially left stale backend objects with old
  `PgThreadBackendRuntimeState` layout assumptions. Threaded TAP then crashed
  during startup before readiness. The recovery was a backend clean plus
  generated-header recovery followed by a clean `gmake -j8`, matching the
  local build notes for runtime/header layout changes;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`;
- focused `test_backend_runtime` regression passed and includes
  `test_connection_socket_io_is_connection_local()`, which switches
  `CurrentPgConnection` between fake connections and proves socket I/O state
  is isolated per connection;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install.

## Connection Protocol Dispatch Bridge

The fifth Phase 12 slice moves the exported frontend/backend protocol dispatch
state under `PgConnection`:

- `PgConnection` now owns a `PgConnectionProtocolState`;
- `PqCommMethods` and `FeBeWaitSet` remain source-compatible lvalue macros in
  `libpq.h`;
- the macros route through `PgCurrentPqCommMethodsRef()` and
  `PgCurrentFeBeWaitSetRef()`, which return the current connection fields;
- early startup paths before `CurrentPgConnection` is installed use fallback
  connection-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts any early fallback protocol state into
  the process connection object before clearing the fallback storage;
- `pq_init()` initializes the current connection's protocol methods to the
  socket implementation, while existing shared-memory message queue redirection
  still assigns through `PqCommMethods`.

This completes the first connection protocol bridge by keeping the historical
call sites and exported names usable while removing the raw TLS backing storage
for protocol method selection and frontend/backend wait-set ownership.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `pqcomm.o`, and
  `test_backend_runtime.o`;
- because installed headers changed protocol globals into compatibility
  macros, `gmake -C src/backend clean` plus generated-header recovery was used
  before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`;
- focused `test_backend_runtime` regression passed and includes
  `test_connection_protocol_state_is_connection_local()`, which switches
  `CurrentPgConnection` between fake connections and proves `PqCommMethods`
  and `FeBeWaitSet` are isolated per connection;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install.

## Connection Identity Bridge

The sixth Phase 12 slice moves the widely visible connection identity fields
under `PgConnection`:

- `PgConnection` now owns a `PgConnectionIdentityState`;
- `MyProcPort`, `MyCancelKey`, and `MyCancelKeyLength` remain
  source-compatible macros in `miscadmin.h`;
- `MyProcPort` and `MyCancelKeyLength` remain assignable lvalues through
  `PgCurrentProcPortRef()` and `PgCurrentCancelKeyLengthRef()`;
- `MyCancelKey` now resolves to the current connection's cancel-key buffer;
- early authentication paths before `BaseInit()` installs
  `CurrentPgConnection` use fallback connection-local storage in
  `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts any early fallback port and cancel-key
  state into the process connection object before clearing the fallback
  storage;
- threaded backend runtime initialization stores the supplied `Port` directly
  in the thread's `PgConnection`.

This removes another historical connection TLS bucket while keeping the
existing call sites for logging, authentication, cancel-key publication, and
client I/O source-compatible. It also makes the frontend connection pointer
travel with the logical connection object instead of the carrier thread.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`,
  `postgres.o`, `test_backend_runtime.o`, and representative users of
  `MyProcPort`;
- because `miscadmin.h` and `backend_runtime.h` changed former exported
  connection globals into compatibility macros, `gmake -C src/backend clean`
  plus generated-header recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`;
- focused `test_backend_runtime` regression passed and includes
  `test_connection_identity_state_is_connection_local()`, which switches
  `CurrentPgConnection` between fake connections and proves `MyProcPort`,
  `MyCancelKey`, and `MyCancelKeyLength` are isolated per connection;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install.
- `gmake -C contrib -j8` passed after the header migration.

## Connection Interrupt Flag Bridge

The seventh Phase 12 slice moves the connection-loss and client-check flags
under `PgConnection`:

- `PgConnection` now owns a `PgConnectionInterruptState`;
- `CheckClientConnectionPending` and `ClientConnectionLost` remain
  source-compatible lvalue macros in `miscadmin.h`;
- the macros route through `PgCurrentCheckClientConnectionPendingRef()` and
  `PgCurrentClientConnectionLostRef()`, which return fields in the current
  connection object;
- early paths before `CurrentPgConnection` is installed use fallback
  connection-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts any early fallback connection
  interrupt state into the process connection object before clearing the
  fallback storage.

This removes the remaining widely visible connection interrupt TLS flags while
keeping client connection checks, broken-pipe handling, and logical interrupt
application source-compatible.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`,
  `pqcomm.o`, `postgres.o`, and `test_backend_runtime.o`;
- because `miscadmin.h` and `backend_runtime.h` changed former exported
  connection globals into compatibility macros, `gmake -C src/backend clean`
  plus generated-header recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`;
- focused `test_backend_runtime` regression passed and includes
  `test_connection_interrupt_state_is_connection_local()`, which switches
  `CurrentPgConnection` between fake connections and proves
  `CheckClientConnectionPending` and `ClientConnectionLost` are isolated per
  connection;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install;
- `gmake -C contrib -j8` passed after the header migration.

## Connection Frontend Protocol Bridge

The eighth Phase 12 slice moves the negotiated frontend/backend protocol
version under `PgConnection`:

- `PgConnectionProtocolState` now owns `frontend_protocol`;
- `FrontendProtocol` remains a source-compatible lvalue macro in
  `libpq-be.h`;
- the macro routes through `PgCurrentFrontendProtocolRef()`, which returns the
  current connection's protocol-version field;
- early startup paths before `CurrentPgConnection` is installed reuse the
  existing protocol-state fallback storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts early fallback protocol state into the
  process connection object before clearing fallback storage.

This keeps startup packet negotiation, protocol-version checks, error
formatting, and shared-memory message queue protocol redirection tied to the
logical connection object. `PqCommMethods`, `FeBeWaitSet`, and
`FrontendProtocol` now share the same object-backed protocol-state bucket.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`,
  `backend_startup.o`, `postgres.o`, `pqmq.o`, and `elog.o`;
- because installed headers changed another exported connection global into a
  compatibility macro, `gmake -C src/backend clean` plus generated-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`;
- focused `test_backend_runtime` regression passed and includes
  `test_connection_frontend_protocol_is_connection_local()`, which switches
  `CurrentPgConnection` between fake connections and proves `FrontendProtocol`
  is isolated per connection;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install;
- `gmake -C contrib -j8` passed after the header migration.

## Connection Startup State Bridge

The ninth Phase 12 slice moves backend startup connection state under
`PgConnection`:

- `PgConnection` now owns a `PgConnectionStartupState`;
- `ClientAuthInProgress` remains a source-compatible lvalue macro in
  `postmaster.h`;
- `MyClientSocket` remains a source-compatible lvalue macro in `postmaster.h`;
- the macros route through `PgCurrentClientAuthInProgressRef()` and
  `PgCurrentClientSocketRef()`, which return fields in the current connection
  object;
- early postmaster/backend startup paths before `CurrentPgConnection` is
  installed use fallback connection-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts early fallback startup state into the
  process connection object before clearing fallback storage.

This keeps authentication-log visibility and the inherited/reconstructed
client socket pointer tied to the logical connection rather than the carrier
thread. It also preserves the process-mode adapter shape in `BackendMain()`
and the temporary thread-start socket handoff in `launch_backend.c` while
moving the backing storage out of raw TLS.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`,
  `postmaster.o`, `launch_backend.o`, `backend_startup.o`, `postgres.o`, and
  `test_backend_runtime.o`;
- because `postmaster.h` and `backend_runtime.h` changed exported connection
  globals into compatibility macros, `gmake -C src/backend clean` plus
  generated-header recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`;
- focused `test_backend_runtime` regression passed and includes
  `test_connection_startup_state_is_connection_local()`, which switches
  `CurrentPgConnection` between fake connections and proves
  `ClientAuthInProgress` and `MyClientSocket` are isolated per connection;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install;
- `gmake -C contrib -j8` passed after the header migration.

## Client Connection Info Bridge

The tenth Phase 12 slice moves authenticated-client connection information
under `PgConnection`:

- `PgConnection` now owns a `PgConnectionClientConnectionInfoState`;
- `PgConnectionClientConnectionInfoState` is layout-compatible with
  `ClientConnectionInfo`, with static assertions in `miscinit.c`;
- `MyClientConnectionInfo` remains a source-compatible lvalue macro in
  `libpq-be.h`;
- the macro routes through `PgCurrentClientConnectionInfoRef()`, which returns
  the current connection's authenticated-client information bucket;
- early authentication paths before `CurrentPgConnection` is installed use
  fallback connection-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts early fallback client-connection
  information into the process connection object before clearing fallback
  storage.

This keeps authenticated identity and authentication-method state tied to the
logical connection object. It also preserves the existing serialization and
deserialization call sites for parallel workers, which continue to use the
`MyClientConnectionInfo` compatibility name while the backing storage moves
out of raw TLS.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `miscinit.o`,
  `postinit.o`, `auth.o`, `auth-oauth.o`, `parallel.o`, and
  `test_backend_runtime.o`;
- because installed headers changed another exported connection global into a
  compatibility macro, `gmake -C src/backend clean` plus generated-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`;
- focused `test_backend_runtime` regression passed and includes
  `test_client_connection_info_is_connection_local()`, which switches
  `CurrentPgConnection` between fake connections and proves
  `MyClientConnectionInfo` is isolated per connection;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install;
- `gmake -C contrib -j8` passed after the header migration.

## Backend Pending Interrupt State Bridge

The eleventh Phase 12 slice moves the historical backend pending-interrupt
flags under `PgBackend`:

- `PgBackend` now owns a `PgBackendPendingInterruptState`;
- `InterruptPending`, `QueryCancelPending`, `ProcDiePending`,
  `ProcDieSenderPid`, `ProcDieSenderUid`,
  `IdleInTransactionSessionTimeoutPending`, `TransactionTimeoutPending`,
  `IdleSessionTimeoutPending`, `ProcSignalBarrierPending`,
  `LogMemoryContextPending`, and `IdleStatsUpdateTimeoutPending` remain
  source-compatible lvalue macros in `miscadmin.h`;
- the macros route through `PgCurrentPendingInterruptStateRef()`, which
  returns the current logical backend's pending-interrupt bucket;
- early startup paths before `CurrentPgBackend` is installed use fallback
  backend-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts early fallback pending-interrupt
  state into the process backend object before clearing fallback storage.

This keeps the signal-era pending flags tied to the logical backend rather
than the carrier thread. The logical interrupt mailbox still feeds these
compatibility names in `PgCurrentBackendApplyInterrupts()`, but the final
consumer state is now part of `PgBackend`, matching the targetable
wait/wakeup model.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`,
  `postgres.o`, and `test_backend_runtime.o`;
- because `miscadmin.h` changed widely exported backend interrupt globals into
  compatibility macros, `gmake -C src/backend clean` plus generated-header
  recovery was used before the clean rebuild;
- `src/common` was cleaned and rebuilt after the first link attempt exposed a
  stale `scram-common_srv.o` reference to the removed `InterruptPending`
  symbol;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`,
  PL/pgSQL, `src/test/regress`, and `libpqwalreceiver`;
- focused `test_backend_runtime` regression passed and includes
  `test_backend_pending_interrupts_are_backend_local()`, which switches
  `CurrentPgBackend` between fake backends and proves all moved pending flags
  are isolated per backend;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after stale `regress.dylib` and `libpqwalreceiver.dylib` rebuilds;
- clean `gmake -C contrib -j8` passed after the header migration.

## Backend Core State Bridge

The twelfth Phase 12 slice moves core backend identity and lifecycle state
under `PgBackend`:

- `PgBackend` now owns a `PgBackendCoreState`;
- `ExitOnAnyError`, `MyProcPid`, `MyStartTime`, `MyStartTimestamp`,
  `MyLatch`, `MyPMChildSlot`, `OutputFileName`, `Mode`, and
  `IgnoreSystemIndexes` remain source-compatible lvalue macros in
  `miscadmin.h`;
- `MyBackendType` remains a source-compatible lvalue macro but now maps to the
  existing `PgBackend.backend_type` field;
- the macros route through `PgCurrent*Ref()` accessors that return the current
  logical backend's core-state fields;
- early startup paths before `CurrentPgBackend` is installed use fallback
  backend-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` and `InstallPgThreadBackendRuntimeState()`
  adopt early fallback core state into the logical backend object before
  clearing fallback storage;
- latch adoption preserves both current startup shapes: an early `MyLatch`
  value becomes the backend interrupt latch, while a backend initialized with
  an explicit interrupt latch mirrors that value back into `MyLatch`.

This removes another set of process-era backend globals from raw TLS and puts
backend identity, start time, latch ownership, error-exit policy, processing
mode, debug output file, and system-index override state on the logical
backend object. That makes the thread-per-session runtime less dependent on
carrier-local storage and gives later scheduler work one object to carry for
backend identity/lifecycle state.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`,
  `miscinit.o`, `main.o`, `launch_backend.o`, `backend_startup.o`,
  `postgres.o`, and `test_backend_runtime.o`;
- because `miscadmin.h` changed widely exported backend globals into
  compatibility macros, `gmake -C src/backend clean` plus generated-header
  recovery was used before the clean rebuild;
- the first clean link exposed a stale server-side `src/port` object
  (`pqsignal_srv.o`) that still referenced the removed `MyProcPid` symbol.
  Cleaning and rebuilding `src/port` fixed the stale reference;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`,
  PL/pgSQL, `src/test/regress`, and `libpqwalreceiver`;
- focused `test_backend_runtime` regression passed and includes
  `test_backend_core_state_is_backend_local()`, which switches
  `CurrentPgBackend` between fake backends and proves the moved core-state
  compatibility lvalues are isolated per backend;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install;
- clean `gmake -C contrib -j8` passed after the header migration.

## Execution Error State Bridge

The thirteenth Phase 12 slice moves the error-context and exception stacks
under `PgExecution`:

- `PgExecution` now owns a `PgExecutionErrorState`;
- `error_context_stack` and `PG_exception_stack` remain source-compatible
  lvalue macros in `elog.h`;
- the macros route through `PgCurrentErrorContextStackRef()` and
  `PgCurrentExceptionStackRef()`, which return the current logical
  execution's fields;
- early startup paths before `CurrentPgExecution` is installed use fallback
  execution-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` and `InstallPgThreadBackendRuntimeState()`
  adopt any early fallback error state into the logical execution object
  before clearing fallback storage;
- `ParallelContext` now stores the leader's context callback chain as
  `saved_error_context_stack`, avoiding a field-name collision with the new
  compatibility macro while preserving parallel error reporting semantics.

This removes the core `elog.c` error-recovery stacks from raw TLS while
keeping the existing `PG_TRY()`/`PG_CATCH()` macros and error context callback
call sites intact. Later scheduler work can therefore carry error recovery
state with the logical execution instead of depending on carrier-local
storage.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `elog.o`,
  `parallel.o`, `postgres.o`, and `test_backend_runtime.o`;
- because `elog.h` changed exported execution globals into compatibility
  macros, `gmake -C src/backend clean` plus generated-header recovery was used
  before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`,
  PL/pgSQL, `src/test/regress`, and `libpqwalreceiver`;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_error_state_is_execution_local()`, which switches
  `CurrentPgExecution` between fake executions and proves the moved
  compatibility lvalues are isolated per execution;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install;
- clean `gmake -C contrib -j8` passed after the header migration.

## Execution Memory Context Bridge

The fourteenth Phase 12 slice moves the core execution-owned memory context
pointers under `PgExecution`:

- `PgExecution` now owns a `PgExecutionMemoryContextState`;
- `CurrentMemoryContext` remains a source-compatible lvalue macro in
  `palloc.h`;
- `ErrorContext`, `MessageContext`, `TopTransactionContext`,
  `CurTransactionContext`, and `PortalContext` remain source-compatible
  lvalue macros in `memutils.h`;
- the macros route through `PgCurrentMemoryContextRef()`,
  `PgErrorContextRef()`, `PgMessageContextRef()`,
  `PgTopTransactionContextRef()`, `PgCurTransactionContextRef()`, and
  `PgPortalContextRef()`, which return the current logical execution's
  fields;
- early memory setup before `CurrentPgExecution` is installed uses fallback
  execution-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` and `InstallPgThreadBackendRuntimeState()`
  adopt any early fallback memory-context pointers into the logical execution
  object before clearing fallback storage.

This removes the allocator's most central execution globals from raw TLS
while keeping `MemoryContextSwitchTo()` and the standard context names source
compatible. It is a high-value scheduler-preparation step because allocation,
error recovery, message handling, transactions, and portal execution now hang
off the logical execution object rather than the carrier thread.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `mcxt.o`, and
  `test_backend_runtime.o`;
- because `palloc.h` and `memutils.h` changed widely exported execution
  globals into compatibility macros, `gmake -C src/backend clean` plus
  generated-header recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`,
  PL/pgSQL, `src/test/regress`, `libpqwalreceiver`, and
  `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_memory_contexts_are_execution_local()`, which switches
  `CurrentPgExecution` between fake executions and proves the moved
  compatibility lvalues are isolated per execution without allocating through
  fake contexts;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install;
- clean `gmake -C contrib -j8` passed after the header migration.

## Execution Resource Owner Bridge

The fifteenth Phase 12 slice moves the transaction resource-owner current
pointers under `PgExecution`:

- `PgExecution` now owns a `PgExecutionResourceOwnerState`;
- `CurrentResourceOwner`, `CurTransactionResourceOwner`, and
  `TopTransactionResourceOwner` remain source-compatible lvalue macros in
  `resowner.h`;
- the macros route through `PgCurrentResourceOwnerRef()`,
  `PgCurTransactionResourceOwnerRef()`, and
  `PgTopTransactionResourceOwnerRef()`, which return the current logical
  execution's resource-owner fields;
- early startup paths before `CurrentPgExecution` is installed use fallback
  execution-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` and `InstallPgThreadBackendRuntimeState()`
  adopt any early fallback resource-owner pointers into the logical execution
  object before clearing fallback storage;
- `AuxProcessResourceOwner` remains backend-local raw storage for now because
  auxiliary process ownership is tied to backend/process lifecycle, not active
  SQL execution/transaction ownership.

This puts the core transaction resource-owner cursors on the logical
execution object while preserving the historical names used throughout
transaction, portal, executor, cache, and extension-facing code. Later
scheduler work can therefore carry resource lifetime with the execution or
transaction boundary instead of reading carrier-local TLS. A future slice can
split longer-lived session owners from per-execution or per-transaction owners
where the resowner graph needs finer ownership than these current pointers.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `resowner.o`, and
  `test_backend_runtime.o`;
- because `resowner.h` changed exported execution globals into compatibility
  macros, `gmake -C src/backend clean` plus generated-header recovery was used
  before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`,
  PL/pgSQL, `src/test/regress`, `libpqwalreceiver`, and
  `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_resource_owners_are_execution_local()`, which switches
  `CurrentPgExecution` between fake executions and proves the moved
  compatibility lvalues are isolated per execution without treating the fake
  pointers as real resource owners;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install;
- clean `gmake -C contrib -j8` passed after the header migration.

## Session Database Identity Bridge

The sixteenth Phase 12 slice moves the current database identity and path
state under `PgSession`:

- `PgSession` now owns a `PgSessionDatabaseState`;
- `MyDatabaseId`, `MyDatabaseTableSpace`,
  `MyDatabaseHasLoginEventTriggers`, and `DatabasePath` remain
  source-compatible lvalue macros in `miscadmin.h`;
- the macros route through `PgCurrentMyDatabaseIdRef()`,
  `PgCurrentMyDatabaseTableSpaceRef()`,
  `PgCurrentMyDatabaseHasLoginEventTriggersRef()`, and
  `PgCurrentDatabasePathRef()`, which return the current logical session's
  database fields;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` and `InstallPgThreadBackendRuntimeState()`
  adopt any early fallback database identity into the logical session object
  before clearing fallback storage.

This moves the current database OID, tablespace OID, login-event-trigger flag,
and database directory path from carrier-local TLS to the logical session. That
matters because these fields are consulted by catalog/cache access, locking,
DDL, event triggers, replication/logical paths, and statistics. A future
scheduler can now keep database identity with the session when executions move
between carriers instead of relying on the carrier thread's historical global
storage.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`,
  `test_backend_runtime.o`, `miscinit.o`, and `postinit.o`;
- because `miscadmin.h` changed exported session globals into compatibility
  macros, `gmake -C src/backend clean` plus generated-header recovery was used
  before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`,
  PL/pgSQL, `src/test/regress`, `libpqwalreceiver`, and
  `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_database_state_is_session_local()`, which switches
  `CurrentPgSession` between fake sessions and proves the moved compatibility
  lvalues are isolated per session;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the clean rebuild and install;
- clean `gmake -C contrib -j8` passed after the header migration.

## Session DateStyle/DateOrder Bridge

The seventeenth Phase 12 slice moves the parsed `DateStyle` and `DateOrder`
backing fields under `PgSession`:

- `PgSession` now owns a `PgSessionDateTimeState`;
- `DateStyle` and `DateOrder` remain source-compatible lvalue macros in
  `miscadmin.h`;
- the macros route through `PgCurrentDateStyleRef()` and
  `PgCurrentDateOrderRef()`, which return the current logical session's parsed
  date/time formatting fields;
- zeroed logical session objects lazily initialize these fields to the
  historical defaults, `USE_ISO_DATES` and `DATEORDER_MDY`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` and `InstallPgThreadBackendRuntimeState()`
  adopt any early fallback date/time state into the logical session object
  before resetting fallback storage to the default values.

This slice was deliberately narrower than "all date/time GUCs". `IntervalStyle`
remained a direct `PG_THREAD_LOCAL PG_GLOBAL_SESSION` variable here because the
generated GUC table stores a direct pointer to its backing variable. An initial
attempt to move `IntervalStyle` through a dynamic lvalue macro caused the GUC
record to keep pointing at early fallback storage, and core regression then
showed widespread interval-format output diffs. Direct-pointer GUCs therefore
needed a separate Phase 12 GUC-table rebind/adoption mechanism before they
could safely move under `PgSession`.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`,
  `variable.o`, `date.o`, `timestamp.o`, `datetime.o`, and
  `test_backend_runtime.o`;
- because `miscadmin.h` changed exported session globals into compatibility
  macros, `gmake -C src/backend clean` plus generated-header recovery was used
  before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`,
  PL/pgSQL, `src/test/regress`, `libpqwalreceiver`, and
  `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_datetime_state_is_session_local()`, which switches
  `CurrentPgSession` between fake sessions and proves the moved compatibility
  lvalues are isolated per session and default-initialized;
- threaded runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after narrowing this slice to exclude direct-pointer `IntervalStyle`;
- clean `gmake -C contrib -j8` passed after the header migration.

## Session IntervalStyle Direct GUC Pointer Bridge

The eighteenth Phase 12 slice moves the `IntervalStyle` direct-pointer GUC
backing field under `PgSession` and introduces the first GUC pointer rebind
hook:

- `PgSessionDateTimeState` now also owns `interval_style`;
- `IntervalStyle` remains a source-compatible lvalue macro in `miscadmin.h`;
- the macro routes through `PgCurrentIntervalStyleRef()`, which returns the
  current logical session's interval formatting field;
- zeroed logical session objects lazily initialize `IntervalStyle` to the
  historical default, `INTSTYLE_POSTGRES`;
- `PgSetCurrentSession()` centralizes session activation for runtime-owned
  session switches and calls `RebindSessionGUCVariablePointers()`;
- `InitializePgProcessRuntime()` and `InstallPgThreadBackendRuntimeState()`
  now activate their session through `PgSetCurrentSession()`;
- `RebindSessionGUCVariablePointers()` refreshes the generated
  `IntervalStyle` GUC record's cached backing-variable pointer to the current
  `PgSession` field when the GUC table exists, and is a no-op before GUC
  initialization.

This establishes the pattern needed for other direct-pointer GUC backing
variables whose generated GUC records cache C-variable addresses in
`InitializeGUCVariablePointers()`. It is still a narrow bridge, not a complete
session-swappable GUC stack: the broader GUC table, nesting, source, and reset
state remain carrier-local/thread-local for now. Later Phase 12 work should
extend this rebind/adoption mechanism or move the whole GUC state bucket under
the logical session before pooled carrier scheduling depends on arbitrary
session migration.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`, `guc.o`,
  `test_backend_runtime.o`, `variable.o`, `date.o`, `timestamp.o`, and
  `datetime.o`;
- because `miscadmin.h` changed another exported session global into a
  compatibility macro, `gmake -C src/backend clean` plus generated-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake install DESTDIR="$PWD/tmp_install"` passed, followed by rebuilding and
  reinstalling `src/test/modules/test_backend_runtime`, PL/pgSQL,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed. The
  `test_session_datetime_state_is_session_local()` function now switches
  sessions through `PgSetCurrentSession()`, sets `IntervalStyle` through the
  GUC machinery, and proves the lvalue follows the active session after GUC
  pointer rebinding;
- direct threaded-runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  after setting `PERL5LIB="$HOME/perl5/lib/perl5:..."` for the local
  `IPC::Run` install and patching build-tree `src/test/regress/pg_regress` to
  use the temp-install `libpq.5.dylib`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after moving `IntervalStyle`, covering the interval/date formatting
  regression that the earlier stale-pointer attempt exposed;
- clean `gmake -C contrib -j8` passed after the header migration.

## Session Query-Memory Direct GUC Pointer Bridge

The nineteenth Phase 12 slice moves the direct-pointer query-memory GUC
backing fields under `PgSession`:

- `PgSession` now owns a `PgSessionQueryMemoryState`;
- `work_mem`, `hash_mem_multiplier`, `maintenance_work_mem`, and
  `max_parallel_maintenance_workers` remain source-compatible lvalue macros in
  `miscadmin.h`;
- the macros route through `PgCurrentWorkMemRef()`,
  `PgCurrentHashMemMultiplierRef()`, `PgCurrentMaintenanceWorkMemRef()`, and
  `PgCurrentMaxParallelMaintenanceWorkersRef()`, which return the active
  logical session's query-memory fields;
- zeroed logical session objects lazily initialize these fields to the
  historical defaults: `work_mem = 4096`, `hash_mem_multiplier = 2.0`,
  `maintenance_work_mem = 65536`, and
  `max_parallel_maintenance_workers = 2`;
- early startup paths before `CurrentPgSession` is installed use fallback
  storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` and `InstallPgThreadBackendRuntimeState()`
  adopt any early fallback query-memory state into the logical session object
  before resetting fallback storage to default values;
- `RebindSessionGUCVariablePointers()` now refreshes the generated GUC records
  for all four query-memory settings when the current session changes.

This extends the direct-pointer GUC bridge from a single date/time setting to a
cluster of planner/executor memory settings. It is still not a full
session-owned GUC subsystem: the broader GUC table, nesting, source, and reset
state remain carrier-local/thread-local for now. The bridge is enough to make
these direct backing variables session-owned and to preserve existing call-site
syntax while later Phase 12 work decides whether the whole GUC state bucket
moves under `PgSession`.

Two migration hazards were found and handled:

- local fields named after common GUCs collide with the new lvalue macros
  because expressions such as `state->work_mem` macro-expand. The local GIN
  build-state field was renamed to `accum_work_mem`;
- loadable modules can carry stale undefined references to the old exported
  global symbols. A first core `parallel_schedule` run failed when
  `libpqwalreceiver.dylib` still referenced `_work_mem`; a forced clean rebuild
  and reinstall of `src/backend/replication/libpqwalreceiver` removed the stale
  symbol and fixed the subscription/object-address failures.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`, `guc.o`,
  `gininsert.o`, and `test_backend_runtime.o`;
- clean full `gmake -j8` passed after the GIN local-field rename;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`, PL/pgSQL,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_query_memory_state_is_session_local()`, which switches
  sessions through `PgSetCurrentSession()`, sets the query-memory settings
  through the GUC machinery, and proves the lvalues follow the active session
  after GUC pointer rebinding;
- direct threaded-runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  with the local `PERL5LIB` for `IPC::Run`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests after the forced clean `libpqwalreceiver` rebuild and reinstall;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration.

## Session Planner Cost Direct GUC Pointer Bridge

The twentieth Phase 12 slice moves a planner cost and parallel-planner state
bucket under `PgSession`:

- `PgSession` now owns a `PgSessionPlannerCostState`;
- `seq_page_cost`, `random_page_cost`, `cpu_tuple_cost`,
  `cpu_index_tuple_cost`, `cpu_operator_cost`, `parallel_tuple_cost`,
  `parallel_setup_cost`, `recursive_worktable_factor`,
  `effective_cache_size`, `disable_cost`, `max_parallel_workers_per_gather`,
  `debug_parallel_query`, and `parallel_leader_participation` remain
  source-compatible lvalue macros in the optimizer headers;
- the macros route through `PgCurrent*Ref()` accessors that return the active
  logical session's planner-cost fields;
- zeroed logical session objects lazily initialize these fields to the
  historical defaults from `optimizer/cost.h`, plus `disable_cost = 1.0e10`,
  `max_parallel_workers_per_gather = 2`, `debug_parallel_query =
  DEBUG_PARALLEL_OFF`, and `parallel_leader_participation = true`;
- early startup paths before `CurrentPgSession` is installed use fallback
  storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early fallback
  planner-cost state into the logical session object;
- `RebindSessionGUCVariablePointers()` now refreshes the generated GUC records
  for the direct-pointer GUC-backed members of this bucket when the current
  session changes.

`disable_cost` is part of the same planner-cost state bucket but is not a GUC,
so it does not have a generated GUC record to rebind. The broad family of
planner `enable_*` switches was intentionally left for the separate planner
method slice below so validation stayed readable.

One additional macro-collision hazard was found and handled. The tablespace
reloptions struct had fields named `seq_page_cost` and `random_page_cost`,
which collided with the new lvalue macros in expressions such as
`spc->opts->seq_page_cost`. The struct fields were renamed to
`spc_seq_page_cost` and `spc_random_page_cost` while preserving the SQL
reloption names and reloption parser mappings.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `costsize.o`, `planner.o`, and `test_backend_runtime.o`;
- because exported optimizer headers changed direct globals into
  compatibility macros, `gmake -C src/backend clean` plus generated-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed after the tablespace reloption field rename;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`, PL/pgSQL,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_planner_cost_state_is_session_local()`, which switches
  sessions through `PgSetCurrentSession()`, sets the GUC-backed planner cost
  and parallel-planner settings through the GUC machinery, sets `disable_cost`
  directly, and proves the lvalues follow the active session after GUC pointer
  rebinding;
- direct threaded-runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  with the local `PERL5LIB` for `IPC::Run`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, covering plan-shape-sensitive regressions after moving planner cost
  state;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  optimizer header migration.

## Session Planner Method Direct GUC Pointer Bridge

The twenty-first Phase 12 slice moves the remaining optimizer-session planner
method and planner-tuning direct-pointer state under `PgSession`:

- `PgSession` now owns a `PgSessionPlannerMethodState`;
- the planner method switches `enable_seqscan`, `enable_indexscan`,
  `enable_indexonlyscan`, `enable_bitmapscan`, `enable_tidscan`,
  `enable_sort`, `enable_incremental_sort`, `enable_hashagg`,
  `enable_nestloop`, `enable_material`, `enable_memoize`,
  `enable_mergejoin`, `enable_hashjoin`, `enable_gathermerge`,
  `enable_partitionwise_join`, `enable_partitionwise_aggregate`,
  `enable_parallel_append`, `enable_parallel_hash`,
  `enable_partition_pruning`, `enable_presorted_aggregate`,
  `enable_async_append`, `enable_distinct_reordering`, `enable_geqo`,
  `enable_eager_aggregate`, `enable_group_by_reordering`, and
  `enable_self_join_elimination` remain source-compatible lvalue macros in
  the optimizer headers;
- planner scalar tuning state `cursor_tuple_fraction`,
  `constraint_exclusion`, `geqo_threshold`, `Geqo_effort`,
  `Geqo_pool_size`, `Geqo_generations`, `Geqo_selection_bias`,
  `Geqo_seed`, `min_eager_agg_group_size`,
  `min_parallel_table_scan_size`, `min_parallel_index_scan_size`,
  `from_collapse_limit`, and `join_collapse_limit` also routes through the
  current session;
- the non-GUC cached GEQO planner extension id
  `Geqo_planner_extension_id` moves with the same state bucket so the
  optimizer session TLS baseline is not left with a single GEQO outlier;
- zeroed logical session objects lazily initialize these fields to the
  historical GUC boot defaults, including the disabled defaults for
  partitionwise join and partitionwise aggregate and `-1` for
  `Geqo_planner_extension_id`;
- early startup paths before `CurrentPgSession` is installed use fallback
  storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback planner-method state into the logical session object;
- `RebindSessionGUCVariablePointers()` now refreshes the generated GUC
  records for every GUC-backed member of this bucket when the current session
  changes.

This completes the direct optimizer-session TLS migration: a static scan over
`src/backend/optimizer` and `src/include/optimizer` now finds no remaining
`PG_THREAD_LOCAL PG_GLOBAL_SESSION` definitions or declarations. It does not
complete all planner-adjacent GUC migration; non-optimizer modules still own
other session GUC globals, and the broader GUC stack/source/reset state
remains process-global for now.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `costsize.o`, `allpaths.o`, `pathkeys.o`, `planner.o`,
  `analyzejoins.o`, `initsplan.o`, `plancat.o`, `geqo_main.o`, and
  `test_backend_runtime.o`;
- because exported optimizer headers changed direct globals into
  compatibility macros, `gmake -C src/backend clean` plus generated-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`, PL/pgSQL,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_planner_method_state_is_session_local()`, which switches
  sessions through `PgSetCurrentSession()`, sets every GUC-backed member of
  this bucket through the GUC machinery, sets `Geqo_planner_extension_id`
  directly, and proves the lvalues follow the active session after GUC pointer
  rebinding;
- direct threaded-runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  with `PERL5LIB` including both `src/test/perl` and the local `IPC::Run`
  install path;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, covering plan-shape-sensitive regressions after moving planner method
  and tuning state;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  optimizer header migration;
- `git diff --check` passed;
- static scans found no remaining optimizer-session TLS definitions and no
  migrated planner-method member-name macro collisions outside the intended
  `_value` fields in `backend_runtime.c`.

## Session Tablespace Direct GUC Pointer Bridge

The twenty-second Phase 12 slice moves tablespace-related session state under
`PgSession`:

- `PgSession` now owns a `PgSessionTablespaceState`;
- `default_tablespace`, `temp_tablespaces`, and
  `allow_in_place_tablespaces` remain source-compatible lvalue macros in
  `commands/tablespace.h`;
- `binary_upgrade_next_pg_tablespace_oid` remains a source-compatible lvalue
  macro in `catalog/binary_upgrade.h`;
- the macros route through `PgCurrent*Ref()` accessors that return the active
  logical session's tablespace fields;
- zeroed logical session objects lazily initialize these fields to the
  historical defaults: NULL tablespace strings, `allow_in_place_tablespaces =
  false`, and `binary_upgrade_next_pg_tablespace_oid = InvalidOid`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback tablespace state into the logical session object;
- `RebindSessionGUCVariablePointers()` now refreshes the generated GUC
  records for `default_tablespace`, `temp_tablespaces`, and
  `allow_in_place_tablespaces` when the current session changes.

This moves the default/permitted tablespace selection state, temporary
tablespace search list, in-place tablespace override, and binary-upgrade
tablespace OID handoff out of raw session TLS. It keeps the existing
DDL/storage call sites source-compatible while making the state belong to the
logical SQL session. `binary_upgrade_next_pg_tablespace_oid` is not a GUC, but
it has the same session lifetime and was kept in the same tablespace bucket so
the tablespace module does not retain a single raw TLS outlier.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `tablespace.o`, `pg_upgrade_support.o`, and `test_backend_runtime.o`;
- because installed headers changed exported tablespace globals into
  compatibility macros, `gmake -C src/backend clean` plus generated-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`, PL/pgSQL,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_tablespace_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, sets the GUC-backed tablespace settings
  through the GUC machinery, sets `binary_upgrade_next_pg_tablespace_oid`
  directly, and proves the lvalues follow the active session after GUC pointer
  rebinding;
- direct threaded-runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  with `PERL5LIB` including both `src/test/perl` and the local `IPC::Run`
  install path;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including the core `tablespace` test;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct TLS definitions or extern
  declarations for `default_tablespace`, `temp_tablespaces`,
  `allow_in_place_tablespaces`, or `binary_upgrade_next_pg_tablespace_oid`.

## Session Binary-Upgrade State Bridge

The twenty-third Phase 12 slice moves the remaining binary-upgrade catalog
handoff state under `PgSession`:

- `PgSession` now owns a `PgSessionBinaryUpgradeState`;
- `binary_upgrade_next_pg_type_oid`,
  `binary_upgrade_next_array_pg_type_oid`,
  `binary_upgrade_next_mrng_pg_type_oid`,
  `binary_upgrade_next_mrng_array_pg_type_oid`,
  `binary_upgrade_next_heap_pg_class_oid`,
  `binary_upgrade_next_heap_pg_class_relfilenumber`,
  `binary_upgrade_next_index_pg_class_oid`,
  `binary_upgrade_next_index_pg_class_relfilenumber`,
  `binary_upgrade_next_toast_pg_class_oid`,
  `binary_upgrade_next_toast_pg_class_relfilenumber`,
  `binary_upgrade_next_pg_enum_oid`,
  `binary_upgrade_next_pg_authid_oid`, and
  `binary_upgrade_record_init_privs` remain source-compatible lvalue macros
  in `catalog/binary_upgrade.h`;
- the macros route through `PgCurrent*Ref()` accessors that return fields in
  the active logical session;
- zeroed logical session objects lazily initialize the OID fields to
  `InvalidOid`, relfilenumber fields to `InvalidRelFileNumber`, and
  `binary_upgrade_record_init_privs` to `false`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback binary-upgrade state into the logical session object.

`binary_upgrade_next_pg_tablespace_oid` remains in the tablespace state bucket
introduced by the previous slice because it is consumed by the tablespace
module and shares that bucket's lifetime. With this slice, the remaining
catalog/type/class/enum/authid binary-upgrade state no longer has raw
session-TLS backing storage.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`,
  `pg_upgrade_support.o`, `test_backend_runtime.o`, `pg_type.o`,
  `pg_enum.o`, `index.o`, `heap.o`, `aclchk.o`, `typecmds.o`, `user.o`, and
  `relcache.o`;
- because `catalog/binary_upgrade.h` changed exported session globals into
  compatibility macros, `gmake -C src/backend clean` plus generated-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`, PL/pgSQL,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_binary_upgrade_state_is_session_local()`, which switches
  sessions through `PgSetCurrentSession()`, verifies the default invalid/false
  values, assigns every moved binary-upgrade lvalue, and proves the values
  follow the active session;
- direct threaded-runtime TAP coverage passed for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  with `PERL5LIB` including both `src/test/perl` and the local `IPC::Run`
  install path;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, covering catalog/type/class/enum/authid DDL and init-privilege paths
  after the binary-upgrade header migration;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved `binary_upgrade_next_*` and
  `binary_upgrade_record_init_privs` names.

## Session Parser GUC State Bridge

The twenty-fourth Phase 12 slice moves the parser direct-pointer GUC state
under `PgSession`:

- `PgSession` now owns a `PgSessionParserState`;
- `Transform_null_equals` and `backslash_quote` remain source-compatible
  lvalue macros in `parser/parse_expr.h` and `parser/parser.h`;
- the macros route through `PgCurrentTransformNullEqualsRef()` and
  `PgCurrentBackslashQuoteRef()` accessors that return fields in the active
  logical session;
- zeroed logical session objects lazily initialize
  `Transform_null_equals` to `false` and `backslash_quote` to
  `BACKSLASH_QUOTE_SAFE_ENCODING`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback parser GUC state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `transform_null_equals` and `backslash_quote` whenever the active
  logical session changes;
- the scanner-private cached copy was renamed from `backslash_quote` to
  `scanner_backslash_quote` so it cannot collide with the compatibility macro.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `parse_expr.o`, `scan.o`, and `test_backend_runtime.o`;
- because exported parser GUC globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated-header recovery was used before
  the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`, PL/pgSQL,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_parser_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, sets `backslash_quote` and
  `transform_null_equals` through the GUC machinery, and proves both lvalues
  follow the active session after GUC pointer rebinding;
- direct threaded-runtime TAP coverage was attempted for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`,
  but this system Perl is missing `IPC::Run`, so both tests failed before
  starting PostgreSQL;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including the core `guc` and parser-heavy SQL tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for `Transform_null_equals` or `backslash_quote`.

## Session Vacuum And Analyze GUC State Bridge

The twenty-fifth Phase 12 slice moves vacuum/analyze maintenance state under
`PgSession`:

- `PgSession` now owns a `PgSessionVacuumState`;
- the exported vacuum/analyze GUC backing variables remain source-compatible
  lvalue macros in `miscadmin.h` and `commands/vacuum.h`;
- the macros route through `PgCurrent*Ref()` accessors that return fields in
  the active logical session;
- zeroed logical session objects lazily initialize the moved state to the
  historical defaults for vacuum cost, buffer usage, statistics target,
  freeze/failsafe ages, truncation, eager freeze failure rate, and cost-delay
  timing;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback vacuum/analyze state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `default_statistics_target`, `track_cost_delay_timing`,
  `vacuum_buffer_usage_limit`, `vacuum_cost_delay`, `vacuum_cost_limit`,
  `vacuum_cost_page_dirty`, `vacuum_cost_page_hit`,
  `vacuum_cost_page_miss`, `vacuum_failsafe_age`,
  `vacuum_freeze_min_age`, `vacuum_freeze_table_age`,
  `vacuum_max_eager_freeze_failure_rate`,
  `vacuum_multixact_failsafe_age`, `vacuum_multixact_freeze_min_age`,
  `vacuum_multixact_freeze_table_age`, and `vacuum_truncate` whenever the
  active logical session changes;
- the lower-case `vacuum_cost_delay` and `vacuum_cost_limit` runtime copies
  are also session-local lvalue macros, but they are not the generated GUC
  records' direct backing variables;
- internal reloption C fields were renamed to `relopt_*` variants so
  `AutoVacOpts` and `StdRdOptions` members cannot collide with the new
  compatibility macros. SQL reloption names are unchanged.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`,
  `guc.o`, `vacuum.o`, `analyze.o`, `vacuumparallel.o`,
  `autovacuum.o`, `reloptions.o`, and `test_backend_runtime.o`;
- because exported vacuum/analyze GUC globals changed into compatibility
  macros, `gmake -C src/backend clean` plus generated-header recovery was
  used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`, PL/pgSQL,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_vacuum_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, sets the vacuum/analyze values through the
  GUC machinery plus the lower-case runtime lvalues, and proves the values
  follow the active session after GUC pointer rebinding;
- direct threaded-runtime TAP coverage was attempted for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`,
  but this system Perl is missing `IPC::Run`, so both tests failed before
  starting PostgreSQL;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including `vacuum`, `vacuum_parallel`, `guc`, `reloptions`,
  `stats`, and PL/pgSQL coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved vacuum/analyze names.

## Session Buffer I/O GUC State Bridge

The twenty-sixth Phase 12 slice moves buffer/storage I/O tuning state under
`PgSession`:

- `PgSession` now owns a `PgSessionBufferIOState`;
- the exported buffer I/O GUC backing variables remain source-compatible
  lvalue macros in `storage/bufmgr.h`;
- the macros route through `PgCurrent*Ref()` accessors that return fields in
  the active logical session;
- zeroed logical session objects lazily initialize
  `zero_damaged_pages`, `track_io_timing`, `effective_io_concurrency`,
  `maintenance_io_concurrency`, `io_combine_limit`,
  `io_combine_limit_guc`, and `backend_flush_after` to their historical
  defaults;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback buffer I/O state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `backend_flush_after`, `effective_io_concurrency`,
  `io_combine_limit`, `maintenance_io_concurrency`, `track_io_timing`, and
  `zero_damaged_pages` whenever the active logical session changes;
- `io_combine_limit` is a session-local derived runtime value, while
  `io_combine_limit_guc` is the generated GUC record's direct backing
  variable. The existing assign hooks continue to derive the runtime value
  from `io_combine_limit_guc` and the runtime-wide `io_max_combine_limit`;
- internal tablespace reloption C fields were renamed to `spc_*` variants and
  the read-stream internal member was renamed to `stream_io_combine_limit` so
  the new compatibility macros cannot collide with struct members. SQL
  reloption names and read-stream behavior are unchanged.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `bufmgr.o`,
  `buf_init.o`, `freelist.o`, `localbuf.o`, `read_stream.o`, `md.o`,
  `guc.o`, `variable.o`, `spccache.o`, `reloptions.o`, `analyze.o`,
  `explain.o`, `heapam.o`, `vacuumlazy.o`, `xlogprefetcher.o`, and
  `test_backend_runtime.o`;
- the first normal `gmake -j8` attempt linked stale objects that still
  referenced the old TLS symbols, matching the stale-object warning in
  `AGENTS.md`; `gmake -C src/backend clean` plus generated-header recovery was
  used before the clean rebuild;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_buffer_io_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, sets the buffer I/O values through the GUC
  machinery, verifies the derived `io_combine_limit`, and proves the values
  follow the active session after GUC pointer rebinding;
- direct threaded-runtime TAP coverage was attempted for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`,
  but this system Perl is missing `IPC::Run`, so both tests failed before
  starting PostgreSQL;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including `guc`, `reloptions`, `vacuum`, `vacuum_parallel`,
  `stats`, and storage-heavy coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved buffer I/O names.

## Session Transaction Default GUC State Bridge

The twenty-seventh Phase 12 slice moves transaction default GUC state under
`PgSession`:

- `PgSession` now owns a `PgSessionXactDefaultState`;
- the exported transaction default GUC backing variables remain
  source-compatible lvalue macros in `access/xact.h`;
- the macros route through `PgCurrent*Ref()` accessors that return fields in
  the active logical session;
- zeroed logical session objects lazily initialize
  `DefaultXactIsoLevel`, `DefaultXactReadOnly`, `DefaultXactDeferrable`, and
  `synchronous_commit` to their historical defaults;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback transaction default state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `default_transaction_deferrable`, `default_transaction_isolation`,
  `default_transaction_read_only`, and `synchronous_commit` whenever the
  active logical session changes;
- execution transaction state such as `XactIsoLevel`, `XactReadOnly`,
  `XactDeferrable`, `xact_is_sampled`, `CheckXidAlive`, `bsysscan`, and
  `MyXactFlags` intentionally remains execution-local TLS for a later
  transaction/execution-state migration slice;
- the private `SubOpts` C field in `subscriptioncmds.c` was renamed to
  `synccommit` so the new lower-case compatibility macro cannot collide with
  that struct member. SQL subscription option names are unchanged.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `xact.o`, `guc.o`,
  `subscriptioncmds.o`, and `test_backend_runtime.o`;
- because exported transaction default GUC globals changed into compatibility
  macros, `gmake -C src/backend clean` plus generated-header recovery was
  used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_xact_defaults_are_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, sets the transaction default values through
  the GUC machinery, and proves the values follow the active session after
  GUC pointer rebinding;
- direct threaded-runtime TAP coverage was attempted for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`,
  but this system Perl is missing `IPC::Run`, so both tests failed before
  starting PostgreSQL;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including `transactions`, `guc`, `subscription`, and PL/pgSQL
  coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved transaction default names.

## Session Lock/Wait GUC State Bridge

The twenty-eighth Phase 12 slice moves lock/wait timeout and lock debug GUC
state under `PgSession`:

- `PgSession` now owns a `PgSessionLockWaitState`;
- the exported lock/wait GUC backing variables remain source-compatible lvalue
  macros in `storage/proc.h`, `storage/lock.h`, and `storage/lwlock.h`;
- the macros route through `PgCurrent*Ref()` accessors that return fields in
  the active logical session;
- zeroed logical session objects lazily initialize the timeout, lock logging,
  and lock debug fields to their historical defaults, including
  `deadlock_timeout = 1000`, zero-valued statement/lock/idle/transaction
  timeouts, `log_lock_waits = true`, and `log_lock_failures = false`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback lock/wait state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `deadlock_timeout`, `statement_timeout`, `lock_timeout`,
  `idle_in_transaction_session_timeout`, `transaction_timeout`,
  `idle_session_timeout`, `log_lock_waits`, and `log_lock_failures` whenever
  the active logical session changes;
- `LOCK_DEBUG` GUC state for `debug_deadlocks`, `trace_lock_oidmin`,
  `trace_lock_table`, `trace_locks`, `trace_lwlocks`, and `trace_userlocks`
  is included in `PgSessionLockWaitState` and in the GUC rebind/test path
  under `#ifdef LOCK_DEBUG`; this checkout is a non-`LOCK_DEBUG` build, so
  that coverage is static plus non-debug compile coverage here.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `proc.o`, `lock.o`,
  `lwlock.o`, `guc.o`, and `test_backend_runtime.o`;
- because exported lock/wait GUC globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header recovery
  was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_lock_wait_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, sets the lock/wait timeout and lock logging
  values through the GUC machinery, and proves the values follow the active
  session after GUC pointer rebinding;
- direct threaded-runtime TAP coverage was attempted for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`,
  but this system Perl is missing `IPC::Run`, so both tests failed before
  starting PostgreSQL;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including lock, timeout-adjacent GUC, subscription, and PL/pgSQL
  coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved lock/wait names.

## Session Logging/Debug GUC State Bridge

The twenty-ninth Phase 12 slice moves logging/debug GUC state under
`PgSession`:

- `PgSession` now owns a `PgSessionLoggingState`;
- `PgSessionLoggingState` owns debug print flags, parser/planner/executor
  statement statistics flags, log duration/min-severity/sample/temp/parameter
  settings, `Log_error_verbosity`, `client_min_messages`, `event_source`,
  `backtrace_functions`, the parsed `backtrace_function_list`,
  `log_min_messages_string`, and the derived
  `log_min_messages[BACKEND_NUM_TYPES]` array;
- the public names remain source-compatible lvalue macros in `utils/guc.h`
  and `utils/elog.h`;
- `log_min_messages` remains indexable through a pointer-returning macro, so
  existing `log_min_messages[MyBackendType]` call sites keep their source
  shape;
- `guc_tables.c` uses a private macro for the generated
  `log_min_messages_string` storage pointer, and `elog.c` uses a private macro
  for the parsed `backtrace_function_list` pointer;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback logging/debug state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for the moved debug, stats, logging, severity, sampling, temp-file, and
  backtrace options whenever the active logical session changes;
- `log_btree_build_stats` and the debug-node-test GUCs are guarded by their
  compile-time `BTREE_BUILD_STATS` and `DEBUG_NODE_TESTS_ENABLED` options in
  the rebind and test paths;
- `event_source` is bridged but is not mutated in the regression test because
  it is a `PGC_POSTMASTER` GUC.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `guc_tables.o`, `elog.o`, `csvlog.o`, `jsonlog.o`, `postgres.o`, and
  `test_backend_runtime.o`;
- because exported logging/debug GUC globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header recovery
  was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_logging_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, sets representative debug/logging values
  through the GUC machinery, exercises the `log_min_messages` assign path, and
  proves the values follow the active session after GUC pointer rebinding;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- direct threaded-runtime TAP coverage was attempted for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`,
  but this system Perl is missing `IPC::Run`, so both tests failed before
  starting PostgreSQL;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved logging/debug names.
