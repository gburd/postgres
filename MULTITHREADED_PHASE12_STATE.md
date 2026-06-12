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
