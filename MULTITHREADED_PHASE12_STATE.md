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
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_connection_reset_closed_state()`;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and
  patched macOS install names;
- full `gmake -j8` passed;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals.

## Catalog Execution State

The one-hundred-seventy-fifth Phase 12 slice moves a coherent catalog
transaction/execution batch under `PgExecution`:

- new `PgExecutionCatalogState` owns the carrier-independent pointer/scalar
  slots for uncommitted enum tracking, REINDEX suppression, and pending smgr
  relation delete/sync state;
- `pg_enum.c` now routes `uncommitted_enum_types` and
  `uncommitted_enum_values` through `PgCurrentUncommittedEnumTypesRef()` and
  `PgCurrentUncommittedEnumValuesRef()`;
- `index.c` now routes `currentlyReindexedHeap`,
  `currentlyReindexedIndex`, `pendingReindexedIndexes`, and
  `reindexingNestLevel` through `PgExecutionCatalogState`;
- `storage.c` now routes `pendingDeletes` and `pendingSyncHash` through
  `PgExecutionCatalogState`;
- `SerializedReindexState` local fields were renamed so the compatibility
  macros do not rewrite `sistate->field` references;
- `test_execution_catalog_state_is_execution_local()` switches fake execution
  objects and verifies the seven moved catalog state slots are isolated.

This removes seven raw `PG_GLOBAL_EXECUTION` declarations from the global
lifetime scan. The pointed-to hash tables and lists still use their existing
transaction-lifespan ownership: enum, reindex, and smgr cleanup continue to
clear or release the active state at transaction boundaries. This is not a
full execution memory-context ownership split.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `pg_enum.o`,
  `index.o`, `storage.o`, and `test_backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_execution_catalog_state_is_execution_local()`;
- `gmake check-runtime-lifecycles` passed with 126 fields classified;
- `gmake check-global-lifetimes` passed with 88 execution-local declarations
  and zero new unclassified mutable globals;
- `gmake -C contrib -j8` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and
  patched macOS install names.

## Scalar And GUC Lifecycle Classification

The next Gate E2 manifest-hardening slice classifies low-risk lifecycle rows
whose ownership is already covered by existing backend or GUC cleanup rules:

- backend command, logging scratch, and instrumentation buckets contain only
  inline counters/buffers or borrowed startup pointers and therefore need no
  separate dynamic destroy path;
- scalar session GUC buckets such as buffer I/O, transaction defaults, storage,
  command, replication, sort, and query memory state are whole-bucket adopted
  and have no independent allocations;
- string-backed session GUC buckets such as tablespaces, datetime, logging,
  miscellaneous preload settings, user/session settings, general role state,
  JIT provider, text search config, and connection settings rely on the
  existing GUC reset machinery for string ownership. Their runtime object
  fields are pointer slots plus derived scalars, and those slots must be
  rebound through `RebindSessionGUCVariablePointers()` after switching the
  current session;
- user identity state is owned by the existing session authorization and
  reset-role paths. The `system_user` pointer remains borrowed and must not be
  freed by the bucket.

This slice intentionally leaves rows pending where the bucket owns or borrows
memory contexts, hash tables, dlist/dclist heads, sockets, replication worker
state, AIO state, collation/regex/sequence/optimizer caches, or the legacy
`Session` allocation. Those rows still need concrete reset/destroy work or a
more precise owner split before Phase 12 can close.

Validation for this slice:

- `gmake check-runtime-lifecycles` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals.

## Session Cache Teardown

The next Gate E2 hardening slice gives `PgSessionResetClosedState()` and
`PgConnectionResetClosedState()` concrete reset coverage for resource-owning
runtime buckets:

- `PgConnectionResetClosedState()` now clears the borrowed `Port` pointer and
  cancel key from `PgConnection.identity`, in addition to the existing socket,
  protocol, startup, and security resets;
- `PgSessionResetClosedState()` frees the copied `database_path`, destroys the
  parser operator lookup hash, destroys the sequence hash and clears
  `last_used_seq`, frees the regex ctype cache list through a regex-owned
  helper, frees the planner-extension name array while leaving the borrowed
  strings alone, destroys the operator proof hash, and deletes the collation
  cache memory context;
- `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` now records those rows as owning real
  reset behavior instead of `GateE2 pending` placeholders.

This does not finish the whole destructor tree. It deliberately does not claim
ownership of GUC strings, legacy `Session`, backend worker buckets,
TopMemoryContext split work, or execution memory-context state. Those remain
Phase 12/Gate E2 blockers.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `regcomp.o`, and
  `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 134 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed after fixing
  an initial bootstrap crash from an unconditional `pfree(NULL)` in the new
  reset path;
- direct `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl`
  passed all 87 tests with the local `/Users/samwillis/perl5` `PERL5LIB`
  paths and an explicit `PG_REGRESS`.

## Borrowed Execution Scratch Classification

The next manifest-hardening slice clears the remaining simple scratch rows
that do not own dynamic storage:

- backend expression-interpreter state owns an inline reverse-dispatch table
  and borrows the opcode dispatch table pointer;
- execution node I/O state owns scalar location flags and borrows the tokenizer
  pointer used by node read/write routines;
- execution regex state borrows the active `pg_locale_t` selected by the regex
  compiler/executor. The locale object is owned by the collation/locale cache,
  not the execution bucket.

The remaining `GateE2 pending` rows after this slice are therefore the real
resource/lifetime blockers: backend identity and worker buckets, memory
manager and execution memory contexts, and the legacy `Session` bridge.

Validation for this slice:

- `gmake check-runtime-lifecycles` passed with 134 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals.

## Backend Core Lifetime Classification

The next manifest-hardening slice classifies `PgBackend.core` as backend-owned
identity and borrowed latch state rather than a destructor owner:

- `PgBackendCoreState` holds backend identity/scalar state, an inline
  `OutputFileName` buffer, and the backend PRNG state;
- its `Latch *` points at latch storage owned by the process/thread
  initialization path or by `PGPROC`, and must not be freed by the core bucket;
- the PMChild/thread synchronization concern is not hidden by this
  classification. It is covered by the existing PMChild helper contract:
  thread-backed PMChild signal routing, detach, exit publication, join retry,
  and race coverage are handled outside `PgBackend.core`.

This leaves the actual remaining Gate E2 lifecycle blockers focused on
backend worker/resource buckets, legacy `Session`, and the execution/carrier
memory-context ownership split.

Validation for this slice:

- `gmake check-runtime-lifecycles` passed with 134 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals.

## Backend Early-Adoption Resource Guard

The next Gate E2 hardening slice tightens the review concern about raw
copy/adopt of pointer-bearing backend buckets. `PgBackendAdoptEarlyState()` is
still the single process/thread adoption path, but the resource-bearing helper
functions now assert that fallback storage contains only scalar early state
before adoption:

- WAL sender, replication, logical replication, XLog, recovery, maintenance
  worker, autovacuum, repack, AIO, memory-manager, and utility buckets all
  reject early-owned pointers, memory contexts, lists, HTABs, DSM/DSA handles,
  file descriptors, StringInfo buffers, or equivalent opaque resources;
- scalar fields that genuinely can be touched before runtime installation are
  still adopted and the fallback bucket is reset through its normal
  initializer;
- list-head buckets are reinitialized in the installed backend object after
  adoption, so zeroed fallback storage and circular initialized list heads both
  remain valid empty states;
- replication and logical-replication fallback storage now has explicit
  nonzero sentinel defaults instead of relying on the first adoption reset to
  install those defaults.

This does not close the `GateE2 pending full destroy` rows. It narrows the
copy rule: early adoption is no longer allowed to transfer owned resources for
these buckets. Their normal runtime teardown still needs bucket-by-bucket
destructor proof before Phase 12 can close.

Validation for this slice:

- touched-object build passed for `backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 134 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl`
  passed all 87 tests after cleaning and rerunning a transient failed cluster
  that had left one defunct postmaster child during TAP shutdown.

## Subsystem-Owned Backend Cleanup Audit

The next lifecycle-audit slice narrows several remaining `GateE2 pending`
backend rows by recording the existing subsystem-owned cleanup paths instead
of waiting for a central `DestroyPgBackend()` tree:

- `PgBackend.replication` relies on existing WAL receiver and replication slot
  exit callbacks. `WalRcvDie()` disconnects the active `walreceiver_conn`, and
  slot cleanup/release paths handle `MyReplicationSlot`;
- `PgBackend.recovery` owns backend-local startup/recovery conflict state, with
  `ShutdownRecoveryTransactionEnvironment()` destroying the recovery lock hash
  tables and clearing the virtual transaction state;
- `PgBackend.repack` has paired cleanup on both sides: the leader-side
  `stop_repack_decoding_worker()` detaches DSM/MQ state and frees the worker
  object, while worker-side `RepackWorkerShutdown()` detaches the mapped DSM
  segment;
- `PgBackend.aio` uses the existing `pgaio_shutdown()` before-shmem-exit
  callback to drain in-flight IO and clear `pgaio_my_backend`; the io_uring
  context pointer is a borrowed shared method context, not a backend-owned
  allocation.

This is an audit classification, not a new destructor implementation. The
remaining backend rows that still cannot honestly close are the ones with
retained `TopMemoryContext` allocations or worker-specific contexts that need a
real thread-runtime teardown story: WAL sender, logical replication, XLog,
maintenance worker, autovacuum, memory manager, and utility.

The legacy `Session` bridge now has an explicit endpoint in the manifest:
`PgSession` owns the pointer slot, while `access/session.h` remains the
parallel-query DSM/DSA compatibility payload. `DetachSession()` owns DSM/DSA
detach for that payload. The endpoint is folding that payload into `PgSession`
once typmod/parallel-query sharing no longer needs the old object as a
separate compatibility shell. Its allocation still sits under
`TopMemoryContext`, so this remains tied to the memory-context ownership split.

`PgExecution.memory_contexts` is likewise narrowed but not closed. The object
owns the execution-local pointer slots; the pointed-to contexts are still
owned by the existing main-loop, transaction, portal, and memory-manager paths.
Those slots must not be shallow-copied between concurrently live executions.
The unresolved work is the carrier/session `TopMemoryContext` split and the
destroy/reset path for contexts retained after a thread-backed backend exits.

## Backend Utility Closed-State Reset

The next code slice adds `PgBackendResetClosedState()` and calls it from
`proc_exit()` after ordinary `on_proc_exit` callbacks and
`PgSessionResetClosedState()`. The first backend reset implementation is
deliberately narrow and covers only utility-bucket allocations with clear
backend ownership:

- injection-point cache HTABs allocated in `TopMemoryContext`;
- DCH/NUM formatting cache entries allocated in `TopMemoryContext`;
- backend-local libxml memory context;
- missing-attribute cache HTABs allocated in `TopMemoryContext`.

The reset does not claim the whole utility bucket. Extension sibling cache
entries live in `CacheMemoryContext` and have syscache callback registration
semantics; sequential hash scan state and resource-release callbacks are owned
by their subsystem protocols. Those remain in the utility row's Gate E2 note
until they get their own explicit cleanup story.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `ipc.o`, and
  `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles`, `gmake check-global-lifetimes`,
  `git diff --check`, and full `gmake -j8` passed;
- the regression module has a direct `test_backend_reset_closed_state()` check
  using real dynahash tables and a real memory context for the owned utility
  resources, and `gmake -C src/test/modules/test_backend_runtime check`
  passed;
- the direct threaded-runtime TAP script passed all 87 tests with the local
  Perl `IPC::Run` install.

## Backend Closed-State Resource Reset

The next Gate E2 hardening slice broadens `PgBackendResetClosedState()` beyond
the utility bucket. It now handles backend-owned resources in several
resource-bearing buckets after ordinary exit callbacks have run:

- WAL sender state: still-owned physical WAL readers, logical decoding
  contexts, uploaded-manifest context, `StringInfoData` buffers, replication
  command context, and lag tracker;
- replication state: stale WAL receiver connection pointers, lingering
  receiver file descriptors, and the WAL receiver reply buffer;
- logical replication state: worker WAL receiver connection pointers, stream
  `BufFile`, copy buffer, subtransaction array, slotsync strings, list/hash
  scratch, launcher DSA/dshash local mappings, and `ApplyContext`;
- XLog state: lingering open WAL file descriptor and WAL/index-redo memory
  contexts;
- maintenance-worker state: archiver error string, module-state shell, loaded
  library string, archiver file-list heap, and archive memory context after
  the archive module shutdown callback;
- autovacuum state: `AutovacMemCxt`, child database-list context pointer,
  scheduling array pointer, worker-info pointer after `FreeWorkerInfo()`, and
  database-list head reinitialization;
- AIO state: backend pointer, worker id, and borrowed io_uring context pointer
  after `pgaio_shutdown()` drains in-flight IO.

The same slice also clears stale pointers in subsystem-owned callbacks:
logical WAL sender now clears `logical_decoding_ctx` and `xlogreader` after
`FreeDecodingContext()`, WAL receiver clears `wrconn` after disconnect, and
logical replication worker exit clears `LogRepWorkerWalRcvConn` after
disconnect. This makes the generic backend reset safe to run after callback
teardown without double-disconnecting or double-freeing normal-path state.

The reset still does not claim ownership of shared control-plane state such as
WAL sender shared slots, replication slots, logical worker slots, or stream
filesets. Those remain owned by existing callback paths. The memory-manager
`TopMemoryContext` split remains a separate Gate E2 blocker.

The following utility pass then closes the residual utility row:

- `ResetExtensionSiblingCache()` frees private extension sibling cache nodes
  inside `extension.c`, leaving the backend-local syscache callback with an
  empty list;
- `ResetResourceReleaseCallbacks()` frees backend-local dynamic resource
  callback registrations inside `resowner.c`;
- `PgBackendResetClosedState()` clears dynahash sequential-scan tracking slots
  and levels along with the existing utility caches.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `pgarch.o`,
  `extension.o`, `resowner.o`, `walsender.o`, `walreceiver.o`, `launcher.o`,
  and `test_backend_runtime.o`;
- `gmake -C src/test/modules/test_backend_runtime check` passed with the
  expanded `test_backend_reset_closed_state()` fixture;
- `gmake check-runtime-lifecycles`, `gmake check-global-lifetimes`,
  `git diff --check`, and full `gmake -j8` passed;
- the direct threaded-runtime TAP script passed all 87 tests.

## Backend Exit-State Lifecycle Closure

The exit-state lifecycle row is now closed explicitly. `PgBackendExitState`
contains only inline callback arrays, stack indexes, and exit-in-progress
flags. `PgBackendExitCleanup()` and `shmem_exit()` drain and zero callback
indexes as they run callbacks, and `PgBackendInitializeExitState()` zeroes the
full inline bucket for early-state reset or fresh backend construction.

The regression module now extends `test_backend_exit_state_is_backend_local()`
to seed callback indexes, flags, and a callback slot, then proves
`PgBackendInitializeExitState()` clears them. This documents that the bucket
does not need an additional dynamic destructor.

This slice also closes a Gate E2 PMChild/thread synchronization failure found
by the threaded-runtime TAP. During startup-to-thread handoff, a process-era
auxiliary worker can exit while normal server activity is driven by thread
carrier latch events and socket readiness. If the postmaster later reaches
shutdown with that process child still unreaped, the old process PMChild can
block shutdown-state accounting and the TAP observes a defunct postmaster
child. `ServerLoop()` now handles postmaster control-plane work once per loop
before accepting sockets, and opportunistically drains process child exits in
threaded mode after thread carriers have started. Thread startup and exit
handoffs are likewise drained once per loop, keeping PMChild list mutation in
the postmaster while preventing stale process-era handoff children from
surviving into shutdown.

Validation for this slice:

- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed before the full validation run;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed with a fresh
  temp install;
- `gmake check-runtime-lifecycles` passed with 134 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests, including the postmaster-child-count checks and clean shutdown.

## Backend Memory-Manager Freelist Closure

The backend memory-manager lifecycle row is now narrowed to the state it
actually owns. `PgBackendMemoryManagerState` contains AllocSet context
freelists and the log-memory-context-in-progress flag; it does not own
`TopMemoryContext` itself.

`aset.c` now exposes `AllocSetFreeContextFreelists()`, which frees every
retained AllocSet context header/keeper block in an explicit freelist array.
`PgBackendResetClosedState()` calls that helper for the backend memory-manager
bucket and clears the log-memory-context flag. This gives the bucket a concrete
reset/destroy rule while keeping the broader retained `TopMemoryContext`
ownership/reclamation problem visible as separate Phase 12 work.

The regression module extends the backend memory-manager locality test by
creating and deleting a real small AllocSet context, proving it enters the
current backend freelist, flushing the freelists, and verifying both freelist
slots are empty. The closed-state reset test also verifies the memory-manager
log flag is cleared by `PgBackendResetClosedState()`.

Validation for this slice:

- touched-object builds passed for `aset.o`, `backend_runtime.o`, and
  `test_backend_runtime.o`;
- full `gmake -j8` passed;
- `git diff --check` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed with a fresh
  temp install;
- `gmake check-runtime-lifecycles` passed with 134 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests.

## Execution Debug Pointer Reset

The execution debug lifecycle row now has an explicit reset rule.
`debug_query_string` remains a borrowed pointer to query text owned elsewhere;
the execution bucket must clear it but must not free it. Existing
`PostgresMain()` command, error, and exit paths already clear the pointer when
the current command text may be clobbered. `PgExecutionResetClosedState()` now
also clears the execution object's debug pointer, and `PgBackendExitCleanup()`
calls it with the other object closed-state resets.

The regression module extends
`test_execution_debug_query_string_is_execution_local()` to set a fake
execution's debug pointer and prove `PgExecutionResetClosedState()` clears it.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `ipc.o`, and
  `test_backend_runtime.o`;
- full `gmake -j8` passed;
- `git diff --check` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed with a fresh
  temp install;
- `gmake check-runtime-lifecycles` passed with 134 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests.

## Runtime Lifecycle Manifest

The one-hundred-seventy-third Phase 12 slice makes the Gate E2 object-lifecycle
audit enforceable:

- `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` records the owner/lifetime,
  initializer, early-adoption behavior, reset/destroy status, and copy/adoption
  rule for every current field in `PgBackend`, `PgSession`, `PgConnection`,
  and `PgExecution`;
- `src/tools/runtime_lifecycle/check_runtime_lifecycles.pl` parses
  `src/include/utils/backend_runtime.h` and fails if the manifest is missing a
  runtime-object field, has a duplicate row, or has a stale row;
- `gmake check-runtime-lifecycles` runs that checker as a top-level validation
  target;
- rows marked as `GateE2 pending` are deliberate blockers, not completion
  claims. They identify buckets that still need concrete reset/destroy or
  ownership work before Phase 12 can close.

This does not complete the destructor model or `TopMemoryContext` reclamation.
It makes the lifecycle audit durable so later larger state-migration and
teardown slices cannot add or rename runtime object fields without updating
the Gate E2 classification.

Validation for this slice:

- `perl -c src/tools/runtime_lifecycle/check_runtime_lifecycles.pl` passed;
- direct `perl src/tools/runtime_lifecycle/check_runtime_lifecycles.pl
  --header src/include/utils/backend_runtime.h --manifest
  MULTITHREADED_RUNTIME_LIFECYCLE.tsv` passed with 125 fields classified;
- `gmake check-runtime-lifecycles` passed after regenerating the local
  configured `GNUmakefile`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed.
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_connection_reset_closed_state()`;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and
  patched macOS install names;
- full `gmake -j8` passed;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals.
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

## Session Miscellaneous GUC State Bridge

The thirtieth Phase 12 slice moves miscellaneous session GUC state under
`PgSession`:

- `PgSession` now owns a `PgSessionMiscGUCState`;
- `PgSessionMiscGUCState` owns `allowSystemTableMods`,
  `max_stack_depth`, the derived `max_stack_depth_bytes` value used by
  stack-depth checks, `session_preload_libraries_string`,
  `local_preload_libraries_string`, and `Dynamic_library_path`;
- the public names remain source-compatible lvalue macros in `miscadmin.h`
  and `fmgr.h`;
- `stack_depth.c` keeps `max_stack_depth_bytes` private to the stack-depth
  implementation, but the storage now lives in the active logical session via
  `PgCurrentMaxStackDepthBytesRef()`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback miscellaneous GUC state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `allow_system_table_mods`, `dynamic_library_path`,
  `local_preload_libraries`, `max_stack_depth`, and
  `session_preload_libraries` whenever the active logical session changes.

Validation for this slice:

- touched-object builds passed for `globals.o`, `miscinit.o`,
  `backend_runtime.o`, `guc.o`, `stack_depth.o`, `dfmgr.o`, and
  `test_backend_runtime.o`;
- because exported miscellaneous GUC globals changed into compatibility
  macros, `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_misc_guc_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, sets the moved values through the GUC
  machinery, verifies derived stack-depth bytes, and proves the values follow
  the active session after GUC pointer rebinding;
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
  declarations for the moved miscellaneous GUC names.

## Session Pgstat State Bridge

The thirty-first Phase 12 slice moves pgstat session state under `PgSession`:

- `PgSession` now owns a `PgSessionPgStatState`;
- `PgSessionPgStatState` owns `pgstat_track_counts`,
  `pgstat_track_functions`, `pgstat_fetch_consistency`,
  `pgstat_track_activities`, `pgStatSessionEndCause`, and the
  session-report timestamp formerly held in `pgLastSessionReportTime`;
- the public names remain source-compatible lvalue macros in `pgstat.h` and
  `utils/backend_status.h`;
- `pgLastSessionReportTime` remains private to `pgstat_database.c`, but its
  storage now lives in the active logical session through
  `PgCurrentPgStatLastSessionReportTimeRef()`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback pgstat state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `stats_fetch_consistency`, `track_activities`, `track_counts`, and
  `track_functions` whenever the active logical session changes.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `pgstat.o`, `pgstat_function.o`, `pgstat_database.o`,
  `backend_status.o`, `backend_progress.o`, `execExpr.o`, and
  `test_backend_runtime.o`;
- because exported pgstat globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_pgstat_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, sets representative pgstat tracking and
  session-end values through the GUC machinery or direct compatibility names,
  and proves the values follow the active session after GUC pointer rebinding;
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
  declarations for the moved pgstat names.

## Session Query-ID State Bridge

The thirty-second Phase 12 slice moves query-ID session state under
`PgSession`:

- `PgSession` now owns a `PgSessionQueryIdState`;
- `PgSessionQueryIdState` owns the `compute_query_id` direct-pointer GUC
  backing variable and the derived `query_id_enabled` flag;
- the public names remain source-compatible lvalue macros in
  `nodes/queryjumble.h`, so existing callers, including backend-launch
  parameter save/restore code, keep their source shape;
- early startup and backend-parameter restore paths before `CurrentPgSession`
  is installed use fallback session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback query-ID state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC record
  for `compute_query_id` whenever the active logical session changes.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`,
  `queryjumblefuncs.o`, `guc.o`, `test_backend_runtime.o`,
  `launch_backend.o`, and `explain.o`;
- because exported query-ID globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_query_id_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, changes `compute_query_id` through the GUC
  machinery, toggles `query_id_enabled`, and proves both values and
  `IsQueryIdEnabled()` follow the active session after GUC pointer rebinding;
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
  declarations for the moved query-ID names.

## Session Storage GUC State Bridge

The thirty-third Phase 12 slice moves storage direct-pointer GUC state under
`PgSession`:

- `PgSession` now owns a `PgSessionStorageGUCState`;
- `PgSessionStorageGUCState` owns the `ignore_checksum_failure` direct-pointer
  GUC backing variable and the `file_copy_method` direct-pointer GUC backing
  variable;
- the public names remain source-compatible lvalue macros in
  `storage/bufpage.h` and `storage/copydir.h`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback storage GUC state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `ignore_checksum_failure` and `file_copy_method` whenever the active
  logical session changes.

The backend runtime regression test uses a direct
`FILE_COPY_METHOD_CLONE` assignment for the alternate session value because
the `clone` spelling for the `file_copy_method` GUC option is platform
conditional. The public compatibility name is still exercised as an lvalue,
and the `copy` GUC path is exercised through the normal GUC machinery.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `bufpage.o`,
  `copydir.o`, `guc.o`, `test_backend_runtime.o`, `bufmgr.o`, and
  `storage.o`;
- because exported storage globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_storage_guc_state_is_session_local()`, which switches
  sessions through `PgSetCurrentSession()`, changes
  `ignore_checksum_failure` through the GUC machinery, changes
  `file_copy_method` through both the public lvalue compatibility name and the
  GUC machinery, and proves both values follow the active session after GUC
  pointer rebinding;
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
  declarations for the moved storage GUC names.

## Session User GUC State Bridge

The thirty-fourth Phase 12 slice moves user/role direct-pointer GUC state
under `PgSession`:

- `PgSession` now owns a `PgSessionUserGUCState`;
- `PgSessionUserGUCState` owns the `Password_encryption` direct-pointer GUC
  backing variable and the `createrole_self_grant` direct-pointer GUC backing
  variable;
- the same state bucket owns the derived `createrole_self_grant` assign-hook
  values that drive automatic self-grants during `CREATE ROLE`;
- the public names remain source-compatible lvalue macros in
  `commands/user.h`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback user GUC state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `password_encryption` and `createrole_self_grant` whenever the active
  logical session changes.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `user.o`, `guc.o`,
  `test_backend_runtime.o`, and `auth.o`;
- because exported user GUC globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_user_guc_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, changes `password_encryption` and
  `createrole_self_grant` through the GUC machinery, and proves both public
  backing values and the derived self-grant option flags follow the active
  session after GUC pointer rebinding;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including the `password` and `create_role` schedules;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- direct threaded-runtime TAP coverage was attempted for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`,
  but this system Perl is missing `IPC::Run`, so both tests failed before
  starting PostgreSQL;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved user GUC names.

## Session Command GUC State Bridge

The thirty-fifth Phase 12 slice moves command/trigger/notify direct-pointer
GUC state under `PgSession`:

- `PgSession` now owns a `PgSessionCommandGUCState`;
- `PgSessionCommandGUCState` owns the `SessionReplicationRole`,
  `event_triggers`, and `Trace_notify` direct-pointer GUC backing variables;
- the public names remain source-compatible lvalue macros in
  `commands/trigger.h`, `commands/event_trigger.h`, and `commands/async.h`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback command GUC state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `session_replication_role`, `event_triggers`, and `trace_notify`
  whenever the active logical session changes.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `trigger.o`,
  `event_trigger.o`, `async.o`, `guc.o`, and `test_backend_runtime.o`;
- representative consumers `rewriteHandler.o` and logical replication
  `worker.o` were checked after the public compatibility names changed;
- because exported command GUC globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_command_guc_state_is_session_local()`, which switches
  sessions through `PgSetCurrentSession()`, changes
  `session_replication_role`, `event_triggers`, and `trace_notify` through
  the GUC machinery, and proves the public backing values follow the active
  session after GUC pointer rebinding;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including trigger, event-trigger, PL/pgSQL, async/notify, and GUC
  coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- direct threaded-runtime TAP coverage was attempted for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`,
  but this system Perl is missing `IPC::Run`, so both tests failed before
  starting PostgreSQL;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved command GUC names.

## Session Replication GUC State Bridge

The thirty-sixth Phase 12 slice moves replication direct-pointer GUC state
under `PgSession`:

- `PgSession` now owns a `PgSessionReplicationGUCState`;
- `PgSessionReplicationGUCState` owns the `wal_sender_timeout`,
  `wal_sender_shutdown_timeout`, `log_replication_commands`,
  `wal_receiver_timeout`, `logical_decoding_work_mem`, and
  `debug_logical_replication_streaming` direct-pointer GUC backing variables;
- the public names remain source-compatible lvalue macros in
  `replication/walsender.h`, `replication/walreceiver.h`, and
  `replication/reorderbuffer.h`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback replication GUC state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for the moved replication GUCs whenever the active logical session changes;
- the local subscription option field for `wal_receiver_timeout` was renamed
  to avoid macro expansion on `object->field` while preserving the
  user-visible option and GUC name.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `walsender.o`,
  `walreceiver.o`, `reorderbuffer.o`, logical replication `worker.o` and
  `applyparallelworker.o`, `guc.o`, `guc_tables.o`, and
  `test_backend_runtime.o`;
- because exported replication GUC globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed after renaming the local
  `wal_receiver_timeout` subscription option field that collided with the new
  compatibility macro;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime`,
  `src/test/regress`, `libpqwalreceiver`, and `src/backend/snowball`;
- focused `test_backend_runtime` regression passed and includes
  `test_session_replication_guc_state_is_session_local()`, which switches
  sessions through `PgSetCurrentSession()`, changes all six moved replication
  GUCs through the GUC machinery, and proves the public backing values follow
  the active session after GUC pointer rebinding;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- direct `contrib/test_decoding` regression passed all 20 tests after
  rebuilding stale `pgoutput` and `pgrepack` output plugin dylibs against the
  current headers;
- direct `contrib/test_decoding` isolation regression passed all 14 tests;
- direct threaded-runtime TAP coverage was attempted for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`,
  but this system Perl is missing `IPC::Run`, so both tests failed before
  starting PostgreSQL.

## Session General GUC State Bridge

The thirty-seventh Phase 12 slice moves a broad direct-pointer GUC group under
`PgSession`:

- `PgSession` now owns a `PgSessionGeneralGUCState`;
- `PgSessionGeneralGUCState` owns the `AllowAlterSystem`, `row_security`,
  `check_function_bodies`, `current_role_is_superuser`, `temp_file_limit`,
  `num_temp_buffers`, `role_string`, `lo_compat_privileges`,
  `extra_float_digits`, `bytea_output`, `xmlbinary`,
  `quote_all_identifiers`, `plan_cache_mode`, `GinFuzzySearchLimit`, and
  `gin_pending_list_limit` direct-pointer GUC backing variables;
- the public names remain source-compatible lvalue macros in `utils/guc.h`,
  `utils/rls.h`, `storage/large_object.h`, `utils/float.h`,
  `utils/bytea.h`, `utils/xml.h`, `utils/builtins.h`,
  `utils/plancache.h`, and `access/gin.h`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback general GUC state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for the moved general GUCs whenever the active logical session changes;
- `array_nulls` and `xmloption` remained carrier-local/thread-local globals in
  this slice. A later Phase 12 slice moves them into the same
  `PgSessionGeneralGUCState` bucket after handling the `xmloption` identifier
  collision without a public macro;
- `application_name` and the TCP keepalive/user-timeout GUC variables remain
  deferred because their public names collide with `Port` struct fields and
  need the same kind of call-site migration.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `guc_tables.o`, `test_backend_runtime.o`, `float.o`, `bytea.o`, `xml.o`,
  `ruleutils.o`, `plancache.o`, `ginget.o`, `ginfast.o`, and `inv_api.o`;
- because exported general GUC globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime` and PL/pgSQL
  against the current headers;
- focused `test_backend_runtime` regression passed and includes
  `test_session_general_guc_state_is_session_local()`, which switches
  sessions through `PgSetCurrentSession()`, changes the moved general GUCs
  through the GUC machinery, and proves the public backing values follow the
  active session after GUC pointer rebinding;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including `guc`, `rowsecurity`, `gin`, `plancache`, `plpgsql`,
  `largeobject`, and `xml` coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved general GUC names.

## Session Access/WAL GUC State Bridge

The thirty-eighth Phase 12 slice moves access-method, TOAST, and WAL
direct-pointer GUC state under `PgSession`:

- `PgSession` now owns a `PgSessionAccessWalGUCState`;
- `PgSessionAccessWalGUCState` owns the
  `default_table_access_method`, `synchronize_seqscans`,
  `default_toast_compression`, `wal_compression`, `wal_init_zero`,
  `wal_recycle`, `wal_consistency_checking_string`,
  `wal_consistency_checking`, `CommitDelay`, `CommitSiblings`,
  `track_wal_io_timing`, and `wal_skip_threshold` direct-pointer GUC backing
  variables;
- debug-only `XLOG_DEBUG` and `trace_syncscan` are also covered by the same
  state bucket under their existing `WAL_DEBUG` and `TRACE_SYNCSCAN`
  compilation guards;
- the public names remain source-compatible lvalue macros in
  `access/tableam.h`, `access/toast_compression.h`, `access/xlog.h`,
  `access/syncscan.h`, and `catalog/storage.h`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback access/WAL GUC state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for the moved access/WAL GUCs whenever the active logical session changes.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `test_backend_runtime.o`, `xlog.o`, `tableam.o`, `toast_compression.o`,
  `syncscan.o`, and `storage.o`, with nearby WAL/table/storage consumers
  checked where they were already up to date;
- because exported access/WAL GUC globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime` against the
  current headers;
- focused `test_backend_runtime` regression passed and includes
  `test_session_access_wal_guc_state_is_session_local()`, which switches
  sessions through `PgSetCurrentSession()`, changes the moved access/WAL GUCs
  through the GUC machinery where portable values exist, directly exercises
  the compatibility lvalue storage for single-valid-value settings, and proves
  the public backing values follow the active session after GUC pointer
  rebinding;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including `guc`, `create_am`, `compression`, `compression_pglz`,
  `compression_lz4`, `largeobject`, and WAL-adjacent coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved access/WAL GUC names.

## Session JIT GUC State Bridge

The thirty-ninth Phase 12 slice moves JIT direct-pointer GUC state under
`PgSession`:

- `PgSession` now owns a `PgSessionJitGUCState`;
- `PgSessionJitGUCState` owns the `jit_enabled`, `jit_provider`,
  `jit_debugging_support`, `jit_dump_bitcode`, `jit_expressions`,
  `jit_profiling_support`, `jit_tuple_deforming`, `jit_above_cost`,
  `jit_inline_above_cost`, and `jit_optimize_above_cost` direct-pointer GUC
  backing variables;
- the public names remain source-compatible lvalue macros in `jit/jit.h`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback JIT GUC state into the logical session object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for the moved JIT GUCs whenever the active logical session changes;
- the provider callback table and provider load/failure markers remain
  `PG_GLOBAL_SESSION` TLS for a later JIT provider-state slice, because they
  are provider lifecycle state rather than direct GUC backing variables;
- LLVM-specific provider internals remain deferred in this checkout because it
  is configured with `with_llvm = no`; compile/runtime coverage for those
  internals needs an LLVM-enabled build.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `jit.o`, `guc.o`,
  `guc_tables.o`, and `test_backend_runtime.o`; `planner.o` was checked as a
  nearby JIT-GUC consumer and was already up to date after the clean rebuild;
- because exported JIT GUC globals changed into compatibility macros,
  `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime` against the
  current headers;
- focused `test_backend_runtime` regression passed and includes
  `test_session_jit_guc_state_is_session_local()`, which switches sessions
  through `PgSetCurrentSession()`, changes portable JIT GUCs through the GUC
  machinery, directly exercises the compatibility lvalue storage for
  postmaster/backend-start settings, and proves the public backing values
  follow the active session after GUC pointer rebinding;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for the moved JIT GUC names.

## Session Extension-Control And Sort GUC State Bridge

The fortieth Phase 12 slice moves extension-control and sort direct-pointer
GUC state under `PgSession`:

- `PgSessionMiscGUCState` now owns `Extension_control_path`;
- `PgSession` now owns a `PgSessionSortGUCState`;
- `PgSessionSortGUCState` owns `trace_sort` and, when compiled with
  `DEBUG_BOUNDED_SORT`, `optimize_bounded_sort`;
- the public names remain source-compatible lvalue macros in
  `commands/extension.h` and `utils/guc.h`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback extension-control and sort GUC state into the logical session
  object;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `extension_control_path`, `trace_sort`, and debug-only
  `optimize_bounded_sort` whenever the active logical session changes;
- debug-only bounded-sort state is compiled and tested only in builds that
  define `DEBUG_BOUNDED_SORT`.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `guc_tables.o`, `extension.o`, `tuplesort.o`, `tuplesortvariants.o`, and
  `test_backend_runtime.o`;
- because exported direct-pointer GUC globals changed into compatibility
  macros, `gmake -C src/backend clean` plus generated utility and node-header
  recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime` against the
  current headers;
- focused `test_backend_runtime` regression passed and includes coverage for
  `Extension_control_path` in
  `test_session_misc_guc_state_is_session_local()` and sort GUC state in
  `test_session_sort_guc_state_is_session_local()`;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including `tuplesort`, `guc`, and extension-loading coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for `Extension_control_path`, `trace_sort`, or
  `optimize_bounded_sort`.

## Session Text-Search And Timezone State Bridge

The forty-first Phase 12 slice moves text-search and timezone session
environment state under `PgSession`:

- `PgSessionDateTimeState` now owns the `TimeZone` and `log_timezone` string
  GUC backing variables plus the derived `session_timezone` and
  `log_timezone` `pg_tz` pointers;
- `PgSession` now owns a `PgSessionTextSearchState`;
- `PgSessionTextSearchState` owns `TSCurrentConfig` and the
  `TSCurrentConfigCache` derived cache value;
- the public names remain source-compatible lvalue macros in `pgtime.h` and
  `tsearch/ts_cache.h`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback timezone and text-search state into the logical session object;
- `InitializeThreadedSessionGUCOptions()` initializes
  `default_text_search_config`, `TimeZone`, and `log_timezone` for freshly
  created threaded logical sessions;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for `default_text_search_config`, `TimeZone`, and `log_timezone` whenever
  the active logical session changes;
- local backend-launch handoff fields were renamed away from
  `session_timezone` and `log_timezone`, because those public names are now
  compatibility macros;
- `search_path` remains deferred. Its string GUC backing variable can move
  mechanically, but its namespace-derived caches and invalidation behavior
  need a separate migration plan before session switching can be made
  trustworthy.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `guc_tables.o`, `ts_cache.o`, `pgtz.o`, and `test_backend_runtime.o`;
- because exported timezone and text-search globals changed into
  compatibility macros, `gmake -C src/backend clean` plus generated utility
  and node-header recovery was used before the clean rebuild;
- clean full `gmake -j8` passed after renaming the backend-launch handoff
  fields that collided with the new timezone compatibility macros;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime` against the
  current headers;
- focused `test_backend_runtime` regression passed and includes an extended
  datetime state test plus `test_session_text_search_state_is_session_local()`,
  which switches sessions through `PgSetCurrentSession()`, changes the
  text-search GUC through the GUC machinery, directly exercises the derived
  text-search cache storage, and proves both values follow the active session;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including text-search, GUC, and date/time coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for `TSCurrentConfig`, `session_timezone`, `log_timezone`,
  `timezone_string`, or `log_timezone_string`.

## Session Connection And Tcop GUC State Bridge

The forty-second Phase 12 slice moves connection-facing and tcop exported GUC
state under `PgSession`:

- `PgSession` now owns a `PgSessionConnectionGUCState`;
- `PgSessionConnectionGUCState` owns `application_name`,
  `tcp_keepalives_idle`, `tcp_keepalives_interval`,
  `tcp_keepalives_count`, `tcp_user_timeout`, `Log_disconnections`,
  `log_statement`, `PostAuthDelay`, the
  `restrict_nonsystem_relation_kind` string GUC backing variable, and the
  derived `restrict_nonsystem_relation_kind` flag value;
- the public names remain source-compatible lvalue macros in `utils/guc.h`
  and `tcop/tcopprot.h`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt any early
  fallback connection/tcop GUC state into the logical session object;
- `InitializeThreadedSessionGUCOptions()` initializes the moved connection
  and tcop GUC records for freshly created threaded logical sessions;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for the moved connection and tcop GUCs whenever the active logical session
  changes;
- the backend `Port` fields formerly named `application_name` and
  `tcp_user_timeout` were renamed to `startup_application_name` and
  `socket_tcp_user_timeout`, because the public GUC names are now
  compatibility macros and those common field names collided with installed
  headers.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `postinit.o`,
  `guc.o`, `guc_tables.o`, `postgres.o`, `backend_startup.o`, `pqcomm.o`,
  and `test_backend_runtime.o`;
- because exported connection/tcop GUC globals changed into compatibility
  macros in installed headers, `gmake -C src/backend clean` plus generated
  utility and node-header recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime` against the
  current headers;
- focused `test_backend_runtime` regression passed and includes
  `test_session_connection_guc_state_is_session_local()`, which switches
  sessions through `PgSetCurrentSession()`, changes the moved generated GUCs
  through the GUC machinery, exercises the relation-kind assign-hook derived
  flags, and proves all moved values follow the active session;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including `guc`, `rules`, `plpgsql`, `subscription`, and logging/
  connection-visible coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- `git diff --check` passed;
- static scans found no remaining direct session TLS definitions or extern
  declarations for `application_name`, the TCP keepalive/user-timeout GUCs,
  `Log_disconnections`, `log_statement`, `PostAuthDelay`, or
  `restrict_nonsystem_relation_kind`.

## Runtime Server/Config-File GUC State Bridge

The forty-third Phase 12 slice moves server/config-file identity GUC state
under `PgRuntime` rather than `PgSession`:

- `PgRuntime` now owns a `PgRuntimeServerGUCState`;
- `PgRuntimeServerGUCState` owns `cluster_name`, `ConfigFileName`,
  `HbaFileName`, `IdentFileName`, `HostsFileName`, and `external_pid_file`;
- the public names remain source-compatible lvalue macros in `utils/guc.h`;
- early startup paths before `CurrentPgRuntime` is installed use fallback
  runtime storage in `backend_runtime.c`;
- `InitializePgProcessRuntime()` adopts any early fallback server/config-file
  GUC state into the process runtime object;
- `InitializePgThreadRuntime()` copies the process runtime's server/config-file
  GUC state into the thread-per-session runtime. These are `PGC_POSTMASTER`
  server-owned values, so the current bridge treats their string values as
  runtime/server configuration rather than logical-session state;
- `InitializeThreadedSessionGUCOptions()` initializes the generated GUC records
  for these names when a threaded backend builds its local GUC table;
- `RebindSessionGUCVariablePointers()` now rebinds the generated GUC records
  for these runtime-owned settings when the current runtime/session binding is
  refreshed.

This removes another direct TLS bucket without pretending these values are
session-local. The focused test deliberately avoids using `SetConfigOption()`
to create two simultaneously different `PGC_POSTMASTER` string-GUC values,
because PostgreSQL's broader GUC record, source, reset, and allocation
bookkeeping is still global/carrier-local. Instead it proves that the public
lvalue compatibility names are runtime-backed, that rebinding makes
`GetConfigOption()` read from the active runtime's storage, and that switching
between runtime objects does not require raw exported TLS variables.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `guc_tables.o`, and `test_backend_runtime.o`;
- because `backend_runtime.h` and `utils/guc.h` changed installed runtime/GUC
  declarations, `gmake -C src/backend clean` plus generated utility and
  node-header recovery was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime` against the current
  headers;
- focused `test_backend_runtime` regression passed and includes
  `test_runtime_server_guc_state_is_runtime_local()`, which switches
  `CurrentPgRuntime` between fake runtimes, exercises the runtime-backed
  lvalue macros, and proves `GetConfigOption()` follows the active runtime
  after GUC pointer rebinding;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including `guc`, `cluster`, `subscription`, and PL/pgSQL coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definitions or extern
  declarations for `cluster_name`, `ConfigFileName`, `HbaFileName`,
  `IdentFileName`, `HostsFileName`, or `external_pid_file`.

## Session Prepared-Statement State Bridge

The forty-fourth Phase 12 slice moves SQL/protocol prepared-statement storage
under `PgSession`:

- `PgSession` now owns a `PgSessionPreparedStatementState`;
- `PgSessionPreparedStatementState` owns the `prepared_queries` hash table
  pointer used by `PREPARE`, `EXECUTE`, `DEALLOCATE`, extended-protocol
  prepared statements, and `pg_prepared_statements`;
- `prepare.c` keeps its existing logic through a local compatibility macro
  backed by `PgCurrentPreparedQueriesRef()`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt or initialize the
  prepared-statement bucket with the rest of the logical session object.

This removes the raw per-thread prepared-statement TLS bucket and makes
prepared statements explicitly logical-session state. The migration does not
make cached plans portable across sessions; it preserves PostgreSQL's existing
per-session prepared-statement behavior while making the ownership visible to
the future scheduler.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `prepare.o`, and
  `test_backend_runtime.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before the clean
  rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling `src/test/modules/test_backend_runtime` against the
  current headers;
- focused `test_backend_runtime` regression includes
  `test_session_prepared_statement_state_is_session_local()`, which switches
  fake sessions through `PgSetCurrentSession()` and proves the prepared-query
  pointer follows the active session object;
- the same regression schedule includes a SQL-level `PREPARE`, visibility
  check through `pg_prepared_statements`, `EXECUTE`, and `DEALLOCATE` smoke;
- core process-mode `src/test/regress` `parallel_schedule` passed all 245
  tests, including `prepare`, `plancache`, and PL/pgSQL coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definition for
  `prepared_queries`.

## Session Temporary-Table ON COMMIT State Bridge

The forty-fifth Phase 12 slice moves temporary-table `ON COMMIT` action state
under `PgSession`:

- `PgSession` now owns a `PgSessionOnCommitState`;
- `PgSessionOnCommitState` owns the `on_commits` list used by
  `register_on_commit_action()`, `remove_on_commit_action()`,
  `PreCommit_on_commit_actions()`, `AtEOXact_on_commit_actions()`, and
  `AtEOSubXact_on_commit_actions()`;
- `tablecmds.c` keeps its existing local logic through a compatibility macro
  backed by `PgCurrentOnCommitActionsRef()`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt or initialize the
  ON COMMIT bucket with the rest of the logical session object.

This keeps PostgreSQL's existing transaction and subtransaction cleanup
semantics intact while making temporary-table ON COMMIT registrations
explicitly logical-session state. It removes another raw per-thread session
bucket from table DDL.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `tablecmds.o`, and
  `test_backend_runtime.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode or threaded-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- focused `test_backend_runtime` regression includes
  `test_session_on_commit_state_is_session_local()`, which switches fake
  sessions through `PgSetCurrentSession()` and proves the ON COMMIT list
  pointer follows the active session object;
- the same regression schedule includes SQL-level `ON COMMIT DELETE ROWS` and
  `ON COMMIT DROP` smokes for temporary tables;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definition for
  `on_commits`.

## Session Sequence State Bridge

The forty-sixth Phase 12 slice moves SQL sequence session cache state under
`PgSession`:

- `PgSession` now owns a `PgSessionSequenceState`;
- `PgSessionSequenceState` owns the `seqhashtab` hash table used to remember
  per-session sequence cache entries and the `last_used_seq` pointer used by
  `lastval()`;
- `sequence.c` keeps its existing local logic through compatibility macros
  backed by `PgCurrentSequenceHashTableRef()` and
  `PgCurrentLastUsedSequenceRef()`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt or initialize the
  sequence bucket with the rest of the logical session object.

This preserves PostgreSQL's existing `nextval()`, `currval()`, `lastval()`,
and `DISCARD SEQUENCES` behavior while making the ownership of sequence cache
state explicit. Sequence relation state remains catalog/storage state; this
slice only migrates the logical-session cache that PostgreSQL already treated
as session lifetime.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `sequence.o`, and
  `test_backend_runtime.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode or threaded-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- focused `test_backend_runtime` regression includes
  `test_session_sequence_state_is_session_local()`, which switches fake
  sessions through `PgSetCurrentSession()` and proves both sequence state
  pointers follow the active session object;
- the same regression schedule includes SQL-level `nextval()`, `currval()`,
  `lastval()`, and `DISCARD SEQUENCES` smokes for a temporary sequence;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definitions for
  `seqhashtab` or `last_used_seq`.

## Session Parser Operator Lookup Cache Bridge

The forty-seventh Phase 12 slice moves the parser operator lookup cache under
`PgSession`:

- `PgSessionParserState` now owns `operator_lookup_cache`;
- `parse_oper.c` keeps its existing operator lookup and invalidation logic
  through a compatibility macro backed by `PgCurrentOperatorLookupCacheRef()`;
- early startup paths before `CurrentPgSession` is installed use fallback
  parser session storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt or initialize the
  operator cache pointer with the rest of parser session state.

The operator lookup cache maps operator name, input types, and search path to
resolved operator OIDs. PostgreSQL already treats this as session-lifetime
lookaside state that is flushed by syscache invalidation. This slice keeps the
existing callback behavior and makes the cache pointer part of the logical
session object rather than raw TLS.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `parse_oper.o`, and
  `test_backend_runtime.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode or threaded-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- focused `test_backend_runtime` regression includes the existing
  `test_session_parser_state_is_session_local()` check extended to prove the
  operator lookup cache pointer follows the active session object;
- the same regression schedule includes SQL-level binary, unary, and
  schema-qualified operator lookup smokes;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definition for
  `OprCacheHash`.

## Session Regex Ctype Cache Bridge

The forty-eighth Phase 12 slice moves the regular-expression ctype probe cache
under `PgSession`:

- `PgSession` now owns a `PgSessionRegexState`;
- `PgSessionRegexState` owns the `pg_ctype_cache_list` chain used by the
  regex compiler to cache ctype character-class probe results for a collation;
- `regc_pg_locale.c` keeps its existing local cache logic through a
  compatibility macro backed by `PgCurrentRegexCtypeCacheListRef()`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt or initialize the
  regex cache pointer with the rest of the logical session object.

The regex cache is malloc-managed because the regex code must return failure
rather than lose control on out-of-memory. This slice preserves that ownership
and failure behavior while moving the cache root out of raw session TLS.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o` and `regcomp.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode or threaded-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- focused `test_backend_runtime` regression includes SQL-level regex
  character-class smokes that compile alpha and digit classes and then reuse
  an alpha-class pattern in the same session;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  the core `regex` test;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definition for
  `pg_ctype_cache_list`.

## Session Large-Object Relation Handle Bridge

The forty-ninth Phase 12 slice moves large-object relation handle cache state
under `PgSession`:

- `PgSession` now owns a `PgSessionLargeObjectState`;
- `PgSessionLargeObjectState` owns the cached `pg_largeobject` heap relation
  and large-object index relation references used by `open_lo_relation()` and
  `close_lo_relation()`;
- `inv_api.c` keeps its existing relation-open, ownership-transfer, scan,
  update, and close logic through compatibility macros backed by
  `PgCurrentLargeObjectHeapRelationRef()` and
  `PgCurrentLargeObjectIndexRelationRef()`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt or initialize the
  large-object relation-handle bucket with the rest of the logical session
  object.

PostgreSQL already treats these relation references as backend/session-local
cache roots with cleanup at transaction end. This slice keeps transaction
resource-owner ownership unchanged while making the cached roots explicit
logical-session state.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o` and `inv_api.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode or threaded-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- focused `test_backend_runtime` regression includes
  `test_session_large_object_state_is_session_local()`, which switches fake
  sessions through `PgSetCurrentSession()` and proves both relation pointers
  follow the active session object;
- the same regression schedule includes SQL-level `lo_from_bytea()`,
  `lo_get()`, and `lo_unlink()` smokes;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  the core `largeobject` test;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definitions for
  `lo_heap_r` or `lo_index_r`.

## Session Async Listener-State Bridge

The fiftieth Phase 12 slice moves committed asynchronous notification listener
state under `PgSession`:

- `PgSession` now owns a `PgSessionAsyncState`;
- `PgSessionAsyncState` owns the local channel-name hash table used by
  `LISTEN`, `UNLISTEN`, `pg_listening_channels()`, and notification filtering;
- `PgSessionAsyncState` also owns the `amRegisteredListener` flag tracking
  whether the logical session has an entry in the shared notification listener
  array;
- `async.c` keeps its existing LISTEN/UNLISTEN commit, abort, frontend
  delivery, and cleanup logic through compatibility macros backed by
  `PgCurrentAsyncLocalChannelTableRef()` and
  `PgCurrentAsyncRegisteredListenerRef()`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt or initialize the
  async listener-state bucket with the rest of the logical session object.

This slice deliberately leaves transaction-local pending LISTEN/UNLISTEN and
NOTIFY queues as execution state, and leaves the exit-cleanup registration flag
as backend state. The moved state is the committed session listener membership
and its local lookup table.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o` and `async.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode or threaded-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- focused `test_backend_runtime` regression includes
  `test_session_async_state_is_session_local()`, which switches fake sessions
  through `PgSetCurrentSession()` and proves both the local channel table
  pointer and registered-listener flag follow the active session object;
- the same regression schedule includes SQL-level `LISTEN`,
  `pg_listening_channels()`, and `UNLISTEN *` smokes;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  the core `async` and `guc` tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definitions for
  `localChannelTable` or `amRegisteredListener`.

## Session Encoding And Conversion Bridge

The fifty-first Phase 12 slice moves client/database/message encoding and
conversion-function cache state under `PgSession`:

- `PgSession` now owns a `PgSessionEncodingState`;
- `PgSessionEncodingState` owns the conversion procedure list, active
  client-to-server/server-to-client conversion function pointers, the
  UTF8-to-server helper conversion pointer, client/database/message encoding
  descriptors, startup-complete flag, and pending startup client encoding;
- `mbutils.c` keeps its historical local names through compatibility macros
  backed by `PgCurrentEncodingConvProcListRef()`,
  `PgCurrentToServerConvProcRef()`, `PgCurrentToClientConvProcRef()`,
  `PgCurrentUtf8ToServerConvProcRef()`, `PgCurrentClientEncodingRef()`,
  `PgCurrentDatabaseEncodingRef()`, `PgCurrentMessageEncodingRef()`,
  `PgCurrentEncodingStartupCompleteRef()`, and
  `PgCurrentPendingClientEncodingRef()`;
- early paths before `CurrentPgSession` is installed use fallback
  session storage in `backend_runtime.c`;
- fallback encoding storage is lazily initialized to the same SQL_ASCII
  defaults the old static variables used. This preserves very early paths such
  as `postgres -V`, which can touch encoding state before normal backend
  session installation;
- process-mode and thread-runtime session installation adopt or initialize the
  encoding bucket with the rest of the logical session object.

This slice leaves the conversion cache allocation policy unchanged:
conversion lookup entries remain long-lived `TopMemoryContext` allocations and
rollback can still restore a previous conversion selection without a catalog
lookup. The change only moves the roots and selected encoding pointers from
raw session TLS onto the logical session object.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o` and `mbutils.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression includes
  `test_session_encoding_state_is_session_local()`, which switches fake
  sessions through `PgSetCurrentSession()` and proves all conversion cache,
  selected encoding, startup-complete, and pending-client-encoding fields
  follow the active session object;
- the same regression schedule includes SQL-level `SET LOCAL client_encoding`
  and `convert_to()`/`convert_from()` smokes;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  the core `encoding`, `euc_kr`, `conversion`, and JSON encoding tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definitions for
  `ConvProcList`, `ToServerConvProc`, `ToClientConvProc`,
  `Utf8ToServerConvProc`, `ClientEncoding`, `DatabaseEncoding`,
  `MessageEncoding`, `backend_startup_complete`, or
  `pending_client_encoding`.

## Session Temporary File State Bridge

The fifty-second Phase 12 slice moves temporary-file accounting and temporary
tablespace selection state under `PgSession`:

- `PgSession` now owns a `PgSessionTempFileState`;
- `PgSessionTempFileState` owns the session's total temporary-file byte
  accounting, temporary-file name counter, current transaction's temp
  tablespace OID array, number of selected temp tablespaces, and round-robin
  cursor for choosing the next temp tablespace;
- `fd.c` keeps its historical local names through compatibility macros backed
  by `PgCurrentTemporaryFilesSizeRef()`, `PgCurrentTempFileCounterRef()`,
  `PgCurrentTempTableSpaceOidsRef()`,
  `PgCurrentNumTempTableSpacesRef()`, and
  `PgCurrentNextTempTableSpaceRef()`;
- early paths before `CurrentPgSession` is installed use fallback
  session storage in `backend_runtime.c`;
- process-mode and thread-runtime session installation adopt or initialize the
  temporary-file bucket with the rest of the logical session object.

This slice preserves the existing lifetime split: temporary-file size
accounting and name generation remain session-lifetime state, while the temp
tablespace array is still transaction-owned by its caller and cleared by
`AtEOXact_Files()`. The object bridge moves only the roots and counters from
raw session TLS onto the logical session.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o` and `fd.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression includes
  `test_session_temp_file_state_is_session_local()`, which switches fake
  sessions through `PgSetCurrentSession()` and proves temp-file byte
  accounting, the file-name counter, temp tablespace OID roots, count, and
  round-robin cursor follow the active session object;
- the same helper exercises `SetTempTablespaces()`,
  `TempTablespacesAreSet()`, and `GetTempTablespaces()` through the
  object-backed state;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  the core `temp` and `tablespace` tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definitions for
  `temporary_files_size`, `tempFileCounter`, `tempTableSpaces`,
  `numTempTableSpaces`, or `nextTempTableSpace`.

## Session Array/XML Option GUC Completion Bridge

The fifty-third Phase 12 slice completes two direct-pointer GUC leftovers in
the general GUC bucket:

- `PgSessionGeneralGUCState` now owns `Array_nulls` and `xmloption` as
  `array_nulls_value` and `xmloption_value`;
- `utils/array.h` keeps `Array_nulls` as a source-compatible lvalue macro
  backed by `PgCurrentArrayNullsRef()`;
- `utils/xml.h` exposes `PgCurrentXmlOptionRef()` but deliberately does not
  define a public `xmloption` macro, because the identifier appears as a
  parameter and struct-field name in XML-related code;
- `xml.c` and `guc_tables.c` use file-local `xmloption` compatibility macros
  so existing call sites and generated GUC variable assignment still write to
  the active `PgSession`;
- early startup paths before `CurrentPgSession` is installed use fallback
  session-local storage in `backend_runtime.c`;
- `InitializeThreadedSessionGUCOptions()` and
  `RebindSessionGUCVariablePointers()` now initialize and rebind the generated
  `array_nulls` and `xmloption` GUC records for the active logical session.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `arrayfuncs.o`,
  `xml.o`, `guc.o`, and `guc_tables.o`;
- because `backend_runtime.h` and installed public headers changed,
  `gmake -C src/backend clean` plus generated utility and node-header recovery
  was used before trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression extends
  `test_session_general_guc_state_is_session_local()` to switch fake sessions
  through `PgSetCurrentSession()` and prove `Array_nulls` and `xmloption`
  follow the active session object;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  `arrays`, `xml`, and `guc`;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definitions or exported
  declarations for `Array_nulls` or `xmloption`.

## Session Random Function State Bridge

The fifty-fourth Phase 12 slice moves SQL random-function state under the
logical session object:

- `PgSessionRandomState` now owns `random()`/`random_normal()` PRNG state and
  the `setseed()` initialized flag;
- `PgSession` embeds `PgSessionRandomState` with the rest of the session-owned
  state buckets;
- `backend_runtime.c` provides `PgCurrentPseudoRandomStateRef()` and
  `PgCurrentPseudoRandomSeedSetRef()` accessors, with an early session-local
  fallback before a `PgSession` is installed;
- process-mode session initialization and thread-runtime session installation
  adopt or initialize the random state with the rest of the logical session;
- `pseudorandomfuncs.c` keeps its local `prng_state` and `prng_seed_set` names
  as source-compatible lvalue macros backed by the active `PgSession`.

This keeps the SQL-visible behavior of `setseed()` and the random-family
functions unchanged in process mode while making the mutable PRNG stream
belong to the active logical session rather than a carrier-local TLS global.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o` and
  `pseudorandomfuncs.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression includes
  `test_session_random_state_is_session_local()`, which switches fake sessions
  through `PgSetCurrentSession()`, seeds them independently through
  `setseed()`, calls the SQL `random()` function, and proves each PRNG stream
  follows the active session object;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  the core `random` test;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definitions for
  `pseudorandomfuncs.c`'s `prng_state` or `prng_seed_set`.

## Session Optimizer Cache State Bridge

The fifty-fifth Phase 12 slice moves two optimizer cache roots under the
logical session object:

- `PgSessionOptimizerState` now owns planner extension-name ID assignment
  state, including the name array, assigned count, and allocated count;
- `PgSessionOptimizerState` also owns the predicate-test btree proof lookup
  hash root used by `predtest.c`;
- `backend_runtime.c` provides accessors for those roots and carries an early
  session-local fallback before a `PgSession` is installed;
- process-mode session initialization and thread-runtime session installation
  adopt or initialize the optimizer state with the rest of the logical
  session;
- `extendplan.c` and `predtest.c` keep their local source names as
  source-compatible lvalue macros backed by the active `PgSession`.

This keeps planner-extension IDs and predicate proof-cache contents scoped to
the logical session instead of the carrier thread, while preserving process
mode's existing per-backend behavior.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `extendplan.o`, and
  `predtest.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression includes
  `test_session_optimizer_state_is_session_local()`, which switches fake
  sessions through `PgSetCurrentSession()`, proves planner extension IDs are
  assigned independently per session through `GetPlannerExtensionId()`, and
  proves the predicate proof-cache root follows the active session object;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  `predicate`, `planner_est`, partition-planning, and plancache coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS definitions for
  `PlannerExtensionNameArray`, `PlannerExtensionNamesAssigned`,
  `PlannerExtensionNamesAllocated`, or `OprProofCacheHash`.

## Session Plan Cache List State Bridge

The fifty-sixth Phase 12 slice moves the plan-cache saved-plan and cached
expression list heads under the logical session object:

- `PgSessionPlanCacheState` now owns `saved_plan_list` and
  `cached_expression_list`;
- `backend_runtime.c` provides `PgCurrentSavedPlanListRef()` and
  `PgCurrentCachedExpressionListRef()` accessors with lazy initialization, so
  zeroed test/future runtime `PgSession` objects get valid list heads on first
  use;
- the early fallback exists only before a process or thread backend session is
  installed;
- adoption deliberately initializes fresh list heads and asserts that the early
  fallback lists are empty, rather than copying `dlist_head` storage. Empty
  `dlist_head` values contain self-pointers, so a plain struct copy would leave
  the copied list head pointing back to the old storage;
- `plancache.c` keeps the historical `saved_plan_list` and
  `cached_expression_list` source names as source-compatible lvalue macros
  backed by the active `PgSession`.

This keeps saved plans and cached expressions scoped to the logical session
instead of the carrier thread, while preserving process mode's existing
per-backend behavior.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o` and `plancache.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression includes
  `test_session_plan_cache_state_is_session_local()`, which switches fake
  sessions through `PgSetCurrentSession()`, inserts distinct nodes into each
  session's saved-plan and cached-expression lists, and verifies that the list
  heads follow the active `PgSession`;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  the core `plancache` and `plpgsql` tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS declarations for
  `saved_plan_list` or `cached_expression_list`.

## Session Namespace State Bridge

The fifty-seventh Phase 12 slice moves namespace/search-path state under the
logical session object:

- `PgSessionNamespaceState` now owns the active and base search-path lists,
  active and base creation namespaces, pending temporary-namespace creation
  flags, the active path generation, and the namespace user;
- the same state bucket owns `namespace_search_path`, temporary namespace and
  temporary TOAST namespace OIDs, the temporary-namespace subtransaction ID,
  the search-path validity flags, the search-path cache memory context, and
  search-path cache roots;
- `namespace.c` keeps the private `nsphash_hash` and
  `SearchPathCacheEntry` types private by storing those roots as opaque
  pointers in `PgSessionNamespaceState` and exposing typed local helper macros
  only after the simplehash types are defined;
- `namespace_search_path` remains source-compatible for callers through
  `catalog/namespace.h`, but is backed by the active session state;
- the `search_path` GUC participates in `RebindSessionGUCVariablePointers()`
  through `PgCurrentNamespaceSearchPathRef()`. This was required because
  generated GUC records can be built before `BaseInit()` installs the process
  session; the first validation run caught the bug as an `initdb`
  post-bootstrap `information_schema` failure before SQL tests started.

This keeps namespace resolution, temporary namespace ownership, and
search-path cache contents scoped to the logical session instead of the
carrier thread, while preserving process mode's existing per-backend behavior.

Validation for this slice:

- touched-object builds passed for `namespace.o` and `backend_runtime.o`;
- because `backend_runtime.h` changed, `gmake -C src/backend clean` plus
  generated utility and node-header recovery was used before trusting
  process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression includes
  `test_session_namespace_state_is_session_local()`, which switches fake
  sessions through `PgSetCurrentSession()`, mutates active/base search-path
  state, temporary namespace state, `namespace_search_path`, and opaque
  search-path cache pointers, then verifies they follow the active
  `PgSession`;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  namespace, GUC, temporary-object, and PL/pgSQL coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS declarations for the
  moved namespace/search-path state.

## Session Locale State Bridge

The fifty-eighth Phase 12 slice moves locale/session-environment state under
the logical session object:

- `PgSessionLocaleState` now owns the `lc_messages`, `lc_monetary`,
  `lc_numeric`, `lc_time`, and `icu_validation_level` direct-pointer GUC
  backing variables;
- the same state bucket owns the `lc_time` localized day/month name arrays,
  the localeconv cache validity flag, the `lc_time` cache validity flag, the
  per-session cached `struct lconv` pointer, the database default locale
  pointer, and the collation-cache context/root/last-entry fast path;
- `pg_locale.h` keeps the historical exported names as source-compatible
  lvalue macros backed by the active `PgSession`;
- `pg_locale.c` keeps private `pg_locale_t` and simplehash details local by
  storing those roots as opaque pointers in `PgSessionLocaleState` and exposing
  typed local helper macros only inside the implementation file;
- `PGLC_localeconv()` now allocates the per-session `struct lconv` object on
  first use instead of relying on a function-static TLS object;
- the locale GUCs participate in `RebindSessionGUCVariablePointers()` through
  `PgCurrentLocaleMessagesRef()`, `PgCurrentLocaleMonetaryRef()`,
  `PgCurrentLocaleNumericRef()`, `PgCurrentLocaleTimeRef()`, and
  `PgCurrentIcuValidationLevelRef()`.

This keeps locale formatting caches, collation lookup caches, and direct
locale GUC storage scoped to the logical session instead of the carrier
thread, while preserving process mode's existing per-backend behavior.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `pg_locale.o`,
  `guc.o`, and `test_backend_runtime.o`;
- because `backend_runtime.h` and `pg_locale.h` changed, the backend clean
  plus generated utility and node-header recovery path was used before
  trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression includes
  `test_session_locale_state_is_session_local()`, which switches fake
  sessions through `PgSetCurrentSession()`, mutates locale GUC backing values,
  localized name arrays, localeconv flags, and opaque collation-cache pointers,
  then verifies they follow the active `PgSession`;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  money, formatting, locale, collation, GUC, and PL/pgSQL coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS declarations for the
  moved locale/session-environment state.

## Session User Identity State Bridge

The fifty-ninth Phase 12 slice moves user/security identity state under the
logical session object:

- `PgSessionUserIdentityState` now owns the authenticated user ID, session user
  ID, outer/current user IDs, `SYSTEM_USER` string, session-user superuser
  flag, security restriction context, and SET ROLE activity flag;
- `miscinit.c` keeps the historical private source names as lvalue macros
  backed by the active `PgSession`;
- `GetAuthenticatedUserId()`, `GetSessionUserId()`, `GetOuterUserId()`,
  `GetUserId()`, `GetUserIdAndSecContext()`,
  `SetUserIdAndSecContext()`, `SetUserIdAndContext()`,
  `GetCurrentRoleId()`, `SetSessionAuthorization()`, and
  `InitializeSystemUser()` now operate through the active session state;
- the early fallback identity state is adopted into the process or thread
  session when runtime/session objects are installed;
- `test_backend_runtime` seeds synthetic sessions with the current user
  identity for tests that exercise unrelated session-local state and set
  superuser-only GUCs. The identity-specific test deliberately leaves its fake
  sessions zeroed so it still verifies lazy default initialization.

This keeps authentication identity, SQL session identity, effective-user
identity, and security context scoped to the logical session instead of the
carrier thread, while preserving process mode's existing per-backend behavior.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `miscinit.o`, and
  `test_backend_runtime.o`;
- because `backend_runtime.h` changed, the backend clean plus generated
  utility and node-header recovery path was used before trusting process-mode
  runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression includes
  `test_session_user_identity_state_is_session_local()`, which switches fake
  sessions through `PgSetCurrentSession()`, mutates authenticated/session/
  outer/current user IDs, system-user strings, superuser flags, SET ROLE state,
  and security restriction context, then verifies those values follow the
  active `PgSession`;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  role, privilege, GUC, PL/pgSQL, subscription, and event-trigger coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct session TLS declarations for the
  migrated user/security identity state.

## Connection Security State Bridge

The sixtieth Phase 12 slice moves connection security scratch state under the
logical connection object:

- `PgConnectionSecurityState` now owns the SSL loaded-verify-locations flag
  and the GSS send, receive, and result buffers, cursor/length fields,
  consumed count, and max-packet-size state;
- `libpq.h` keeps `ssl_loaded_verify_locations` as a source-compatible lvalue
  macro backed by the active `PgConnection`;
- `be-secure-gssapi.c` keeps its historical private `PqGSS*` names as local
  lvalue macros backed by the active `PgConnection`;
- process-mode startup adopts any early security state into the process
  connection, matching the existing connection-state compatibility pattern;
- PAM authentication scratch state remained deliberately separate in this
  slice because its storage shape depends on PAM headers and callback
  lifetime, not on the SSL/GSS transport buffers moved here.

This keeps connection transport security state scoped to the logical
connection instead of the carrier thread, while preserving source compatibility
for the existing SSL/GSS implementation files.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `be-secure.o`, and
  `test_backend_runtime.o`;
- this checkout is configured with `with_ssl = no` and `with_gssapi = no`, so
  SSL/GSS-specific source files were covered here by static scans plus the
  full non-SSL/non-GSS build; compile coverage for those files still requires
  SSL/GSS-enabled configurations;
- because `backend_runtime.h` and `libpq.h` changed, the backend clean plus
  generated utility and node-header recovery path was used before trusting
  process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression includes
  `test_connection_security_state_is_connection_local()`, which switches fake
  connections through `CurrentPgConnection`, mutates SSL and GSS security
  fields, and verifies those values follow the active `PgConnection`;
- direct `test_backend_runtime` regression passed after reinstalling the test
  module into `tmp_install`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  connection, authentication, GUC, PL/pgSQL, subscription, and event-trigger
  coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining migrated SSL/GSS direct connection TLS
  declarations for `ssl_loaded_verify_locations` or the `PqGSS*` buffer and
  cursor state.

## PAM Connection Authentication State Bridge

The sixty-first Phase 12 slice completes the deferred PAM authentication
scratch-state move under the logical connection object:

- `PgConnectionSecurityState` now owns the Solaris fallback PAM password
  pointer, the current PAM authentication `Port *`, and the no-password flag
  used by `pam_passwd_conv_proc()`;
- `auth.c` keeps the historical private `pam_passwd`, `pam_port_cludge`, and
  `pam_no_password` names as local lvalue macros backed by the active
  `PgConnection`;
- the PAM conversation struct is now stack-local to `CheckPAMAuth()`, with
  `appdata_ptr` initialized directly from the password argument for the normal
  PAM callback path;
- the connection security state bucket now owns SSL, GSS, and PAM
  connection/authentication scratch state.

This removes the remaining PAM-specific connection TLS variables while
preserving the existing PAM callback contract and the Solaris fallback path.

Validation for this slice:

- touched-object builds passed for `auth.o` and `test_backend_runtime.o`;
- this checkout is configured without PAM, so PAM-specific source was covered
  here by static scans plus the full non-PAM build; compile/runtime coverage
  for the PAM branch still requires a PAM-enabled configuration;
- because `backend_runtime.h` changed, the backend clean plus generated
  utility and node-header recovery path was used before trusting process-mode
  runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and now verifies that the
  PAM password pointer, `Port *`, and no-password flag follow the active
  `PgConnection` as part of
  `test_connection_security_state_is_connection_local()`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  connection, password/authentication, GUC, PL/pgSQL, subscription, and
  event-trigger coverage;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct connection TLS declarations for the
  migrated PAM scratch state.

## Backend Default PRNG State Bridge

The sixty-second Phase 12 slice moves the exported backend default
`pg_global_prng_state` under the logical backend object:

- `PgBackendCoreState` now owns the backend default PRNG state;
- backend builds keep `pg_global_prng_state` as a source-compatible lvalue
  macro backed by `PgCurrentGlobalPrngStateRef()`;
- frontend builds keep a real `pg_global_prng_state` definition in
  `src/common/pg_prng.c`, so frontend tools such as `pg_test_fsync` still
  link without the backend runtime object model;
- the backend-runtime regression fixture now verifies that assignments through
  `pg_global_prng_state` follow the active `CurrentPgBackend`.

This removes another exported backend TLS bucket while preserving the existing
backend and frontend PRNG call sites. A zeroed fake backend also exposed a real
fixture requirement: DSM handle generation uses `pg_global_prng_state`, so fake
backend tests that call DSM creation must seed the fake backend's PRNG just as
real backend startup does.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `pg_prng.o`,
  `pg_prng_srv.o`, `pg_test_fsync`, and `test_backend_runtime.o`;
- because `backend_runtime.h` and `pg_prng.h` changed exported backend state,
  the backend clean plus generated utility and node-header recovery path was
  used before trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes the extended
  `test_backend_core_state_is_backend_local()` coverage for
  `pg_global_prng_state`;
- the existing DSM shutdown fixture was updated to seed its fake backend PRNG
  before `dsm_create()`, matching real backend startup and avoiding an
  all-zero PRNG state in DSM handle generation;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  header migration;
- static scans found no remaining direct
  `PG_THREAD_LOCAL PG_GLOBAL_BACKEND pg_prng_state pg_global_prng_state`
  declaration.

## SPI Execution State Bridge

The sixty-third Phase 12 slice moves exported SPI API variables and the
private SPI connection stack under the logical execution object:

- `PgExecution` now owns a `PgExecutionSPIState`;
- public `SPI_processed`, `SPI_tuptable`, and `SPI_result` remain
  source-compatible lvalue macros backed by `PgCurrentSPI*Ref()` accessors, so
  PL/pgSQL and extension source can keep using the historical API shape;
- private `_SPI_stack`, `_SPI_current`, `_SPI_stack_depth`, and
  `_SPI_connected` are local compatibility macros inside `spi.c`;
- `_SPI_connected` keeps its historical `-1` sentinel and is explicitly
  initialized for process, thread, and early execution states;
- `_SPI_connection` now has a struct tag so `backend_runtime.h` can forward
  declare the private stack element without including `spi_priv.h`;
- the backend-runtime regression fixture now switches `CurrentPgExecution`
  between fake executions and verifies that assignments through the public SPI
  API variables follow the active execution.

This removes another exported backend TLS bucket from a particularly important
extension-facing API. The bridge keeps source compatibility for in-tree and
third-party code that reads or assigns the public SPI result globals, while
making nested SPI state part of the execution object. PL/pgSQL needed a clean
rebuild and reinstall after the public SPI variables became macros; a stale
`plpgsql.dylib` still imported `_SPI_processed` and failed during `initdb`
post-bootstrap initialization before SQL tests could start.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `spi.o`, and
  `test_backend_runtime.o`;
- because `spi.h` and `backend_runtime.h` changed exported backend state, the
  backend clean plus generated utility and node-header recovery path was used
  before trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- rebuilding and reinstalling `src/pl/plpgsql/src` passed after the stale
  `_SPI_processed` import was detected;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_spi_state_is_execution_local()`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- direct PL/pgSQL regression over `plpgsql_array`, `plpgsql_cache`,
  `plpgsql_call`, `plpgsql_control`, `plpgsql_copy`, `plpgsql_domain`,
  `plpgsql_misc`, `plpgsql_record`, `plpgsql_simple`,
  `plpgsql_transaction`, `plpgsql_trap`, `plpgsql_trigger`, and
  `plpgsql_varprops` passed all 13 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public SPI header migration;
- static scans found no remaining direct TLS declarations for the migrated SPI
  API variables or private SPI connection-stack state.

## Active Portal Execution State Bridge

The sixty-fourth Phase 12 slice moves the exported `ActivePortal` pointer
under the logical execution object:

- `PgExecution` now owns a `PgExecutionPortalState`;
- public `ActivePortal` remains a source-compatible lvalue macro in
  `pquery.h`, backed by `PgCurrentActivePortalRef()`;
- early paths before `CurrentPgExecution` is installed use fallback
  execution-local storage in `backend_runtime.c`;
- process and thread runtime installation adopt any early active portal value
  into the installed execution object;
- the backend-runtime regression fixture now switches `CurrentPgExecution`
  between fake executions and verifies that assignments through `ActivePortal`
  follow the active execution.

This removes the portal executor's current-portal pointer from raw execution
TLS while preserving the existing portal execution, cursor, and catalog-helper
call sites. It is a small bridge, but it sits directly on the query execution
path and helps make command execution state explicit before future scheduler
work tries to suspend and resume executions.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `pquery.o`, and
  `test_backend_runtime.o`;
- because `pquery.h` and `backend_runtime.h` changed exported backend state,
  the backend clean plus generated utility and node-header recovery path was
  used before trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_active_portal_is_execution_local()`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  `portals` and `portals_p2`;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public `pquery.h` migration;
- static scans found no remaining direct exported TLS declaration for
  `ActivePortal`.

## Connection Output State Bridge

The sixty-fifth Phase 12 slice moves tcop connection output state under the
logical connection object:

- `PgConnection` now owns a `PgConnectionOutputState`;
- public `whereToSendOutput` remains a source-compatible lvalue macro in
  `tcopprot.h`, backed by `PgCurrentWhereToSendOutputRef()`;
- public `client_connection_check_interval` remains a source-compatible
  lvalue macro in `tcopprot.h`, backed by
  `PgCurrentClientConnectionCheckIntervalRef()`;
- early startup paths before `CurrentPgConnection` is installed use fallback
  connection-local storage initialized to the historical `DestDebug` default;
- process runtime installation adopts any early output state into the
  installed process connection;
- thread runtime initialization sets each connection output state to
  `DestDebug`;
- the backend-runtime regression fixture now switches `CurrentPgConnection`
  between fake connections and verifies that assignments through
  `whereToSendOutput` and `client_connection_check_interval` follow the active
  connection.

This removes another exported connection TLS bucket from the command dispatch
and interrupt paths while preserving the current source-level API shape for
backend call sites.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `postgres.o`, and
  `test_backend_runtime.o`;
- because `backend_runtime.h` and `tcopprot.h` changed exported backend state,
  the backend clean plus generated utility and node-header recovery path was
  used before trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_connection_output_state_is_connection_local()`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public `tcopprot.h` migration;
- static scans found no remaining direct exported TLS declaration for
  `whereToSendOutput` or `client_connection_check_interval`; the only broad
  declaration-pattern match was the existing
  `check_client_connection_check_interval()` GUC hook prototype.

## Connection Startup Timing Bridge

The sixty-sixth Phase 12 slice moves exported backend startup timing state
under the logical connection object:

- `PgConnectionStartupState` now owns `ConnectionTiming` alongside
  `ClientAuthInProgress` and `MyClientSocket`;
- public `conn_timing` remains a source-compatible lvalue macro in
  `backend_startup.h`, backed by `PgCurrentConnectionTimingRef()`;
- the `ConnectionTiming` type now lives in `backend_runtime.h`, so
  `PgConnectionStartupState` can embed it without depending on an incomplete
  struct;
- early startup paths before `CurrentPgConnection` is installed use fallback
  connection-local startup state initialized with the historical
  `ready_for_use = TIMESTAMP_MINUS_INFINITY` sentinel;
- process runtime installation adopts any early startup timing into the
  installed process connection and reinitializes the early fallback sentinel;
- thread runtime initialization sets every connection's startup timing sentinel
  to `TIMESTAMP_MINUS_INFINITY`;
- the backend-runtime regression fixture now extends
  `test_connection_startup_state_is_connection_local()` to mutate and verify
  every `conn_timing` field while switching `CurrentPgConnection` between fake
  connections.

This removes another exported connection TLS bucket from backend startup and
connection setup logging while preserving the existing source-level
`conn_timing.field` API used by backend launch, authentication, and
`PostgresMain()`.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `backend_startup.o`,
  `postgres.o`, `launch_backend.o`, `postinit.o`, and
  `test_backend_runtime.o`;
- because `backend_runtime.h` and `backend_startup.h` changed exported backend
  state, the backend clean plus generated utility and node-header recovery path
  was used before trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes the extended
  `test_connection_startup_state_is_connection_local()` coverage for
  `conn_timing`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public `backend_startup.h` migration;
- static scans found no remaining direct exported TLS declaration for
  `conn_timing`.

## Extended Query Transaction-Started Loop Flag Bridge

The sixty-seventh Phase 12 slice moves the extended-query protocol
transaction-started flag under the logical session loop state:

- `PgSessionLoopState` now owns `transaction_started`;
- the private `postgres.c` `xact_started` name remains as a local
  compatibility macro backed by
  `CurrentPgSession->loop_state.transaction_started`;
- `PgSessionLoopStateInit()` resets the flag alongside the other top-level
  protocol-loop flags;
- error recovery continues to clear the flag through the existing
  `xact_started = false` path, but the storage now belongs to `PgSession`;
- the backend-runtime regression fixture adds
  `test_session_loop_state_is_session_local()`, switching `CurrentPgSession`
  between fake sessions and verifying all loop flags, including
  `transaction_started`, follow the active session.

This removes one of the main `postgres.c` protocol-loop TLS flags and makes
extended-query transaction state part of the logical session instead of the
carrier thread.

Validation for this slice:

- touched-object builds passed for `postgres.o` and
  `test_backend_runtime.o`;
- incremental full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_session_loop_state_is_session_local()`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public `backend_runtime.h` session-state update;
- static scans found no remaining raw `xact_started` TLS declaration.

## Vacuum Execution State Bridge

The sixty-eighth Phase 12 slice moves vacuum cost accounting, failsafe, and
parallel-vacuum scratch state under the logical execution object:

- `PgExecution` now owns `PgExecutionVacuumState`;
- exported `VacuumCostBalance` and `VacuumCostActive` remain source-compatible
  lvalue macros backed by the active execution;
- exported parallel-vacuum state in `commands/vacuum.h`
  (`VacuumSharedCostBalance`, `VacuumActiveNWorkers`,
  `VacuumCostBalanceLocal`, `VacuumFailsafeActive`, and
  `parallel_vacuum_worker_delay_ns`) remains source-compatible lvalue macros
  backed by the active execution;
- `vacuumparallel.c` private worker scratch state
  (`pv_shared_cost_params` and `shared_params_generation_local`) is also
  backed by `PgExecutionVacuumState`, using local typed compatibility macros;
- runtime initialization zeroes this bucket for thread backends and adopts any
  early execution state into the installed process/thread execution object;
- the backend-runtime regression fixture adds
  `test_execution_vacuum_state_is_execution_local()`, switching
  `CurrentPgExecution` between fake executions and verifying all migrated
  vacuum fields follow the active execution.

This removes the main VACUUM execution-state TLS bucket from `globals.c`,
`vacuum.c`, and `vacuumparallel.c`. Session-level vacuum GUC backing storage
remains in `PgSessionVacuumState`; the migrated fields here are per active
execution/command.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `globals.o`,
  `vacuum.o`, `vacuumparallel.o`, `bufmgr.o`, `autovacuum.o`,
  `datachecksum_state.o`, `vacuumlazy.o`, and
  `test_backend_runtime.o`, including forced rebuilds of header-dependent
  users;
- because `backend_runtime.h`, `miscadmin.h`, and `commands/vacuum.h` changed
  exported backend state, the backend clean plus generated utility and
  node-header recovery path was used before trusting process-mode runtime
  tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_vacuum_state_is_execution_local()`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  the core `vacuum` regression test;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public header migration;
- static scans found no remaining raw TLS declarations for the migrated vacuum
  execution-state fields.

## Node Read/Write Execution State Bridge

The sixty-ninth Phase 12 slice moves node serialization/deserialization
scratch state under the logical execution object:

- `PgExecution` now owns `PgExecutionNodeIOState`;
- `outfuncs.c` `write_location_fields` remains a local compatibility macro
  backed by the active execution;
- `read.c` `pg_strtok_ptr` remains a local compatibility macro backed by the
  active execution;
- `readfuncs.h` `restore_location_fields` remains a source-compatible
  debug-build lvalue macro backed by the active execution;
- runtime initialization zeroes this bucket for thread backends and adopts any
  early execution state into the installed process/thread execution object;
- the backend-runtime regression fixture adds
  `test_execution_node_io_state_is_execution_local()`, switching
  `CurrentPgExecution` between fake executions and verifying node read/write
  state follows the active execution.

This removes the node read/write scratch TLS bucket from `outfuncs.c`,
`read.c`, and `readfuncs.h`. The state is per active execution because
`nodeToStringInternal()` and `stringToNodeInternal()` save and restore it
around a single serialization/deserialization call, including re-entrant use.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `outfuncs.o`,
  `read.o`, forced `readfuncs.o`, and `test_backend_runtime.o`;
- because `backend_runtime.h` and `readfuncs.h` changed installed backend
  headers, the backend clean plus generated utility and node-header recovery
  path was used before trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_node_io_state_is_execution_local()`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public header migration;
- static scans found no remaining raw TLS declarations for the migrated node
  read/write execution-state fields.

## Basebackup Execution State Bridge

The seventieth Phase 12 slice moves basebackup command scratch state under the
logical execution object:

- `PgExecution` now owns `PgExecutionBaseBackupState`;
- `basebackup.c` `backup_started_in_recovery`, `total_checksum_failures`, and
  `noverify_checksums` remain local compatibility macros backed by the active
  execution;
- runtime initialization zeroes this bucket for thread backends and adopts any
  early execution state into the installed process/thread execution object;
- the backend-runtime regression fixture adds
  `test_execution_basebackup_state_is_execution_local()`, switching
  `CurrentPgExecution` between fake executions and verifying the migrated
  basebackup fields follow the active execution.

This removes the private basebackup TLS bucket from `basebackup.c`. The state
belongs to the active execution because it is per base backup command: the
command records whether recovery was active when the backup started, the
backup-wide checksum failure count, and the command option controlling
checksum verification.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `basebackup.o`, and
  `test_backend_runtime.o`;
- because `backend_runtime.h` changed an installed backend header, the backend
  clean plus generated utility and node-header recovery path was used before
  trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_basebackup_state_is_execution_local()`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public header migration;
- static scans found no remaining raw TLS declarations for the migrated
  basebackup execution-state fields.

## Analyze Execution State Bridge

The seventy-first Phase 12 slice moves ANALYZE command scratch state under the
logical execution object:

- `PgExecution` now owns `PgExecutionAnalyzeState`;
- `analyze.c` now routes its command memory context and buffer access strategy
  through private `analyze_context` and `analyze_strategy` compatibility macros
  backed by the active execution;
- the previous local name `anl_context` could not remain a macro because
  `VacAttrStats` also has an `anl_context` struct field; the field name remains
  unchanged and stores the active execution's analyze context as before;
- runtime initialization zeroes this bucket for thread backends and adopts any
  early execution state into the installed process/thread execution object;
- the backend-runtime regression fixture adds
  `test_execution_analyze_state_is_execution_local()`, switching
  `CurrentPgExecution` between fake executions and verifying the migrated
  ANALYZE fields follow the active execution.

This removes the private ANALYZE TLS bucket from `analyze.c`. The state belongs
to the active execution because it is per ANALYZE command: the command
allocates a context for analysis data and carries the buffer access strategy
selected by `analyze_rel()`.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `analyze.o`, and
  `test_backend_runtime.o`;
- the first `analyze.o` compile caught a macro collision when `anl_context`
  was used as a compatibility macro and expanded inside `stats->anl_context`;
  the final patch uses private `analyze_context` and `analyze_strategy` macros
  and leaves the `VacAttrStats` field untouched;
- because `backend_runtime.h` changed an installed backend header, the backend
  clean plus generated utility and node-header recovery path was used before
  trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_analyze_state_is_execution_local()`;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  the core `vacuum` regression test that exercises ANALYZE;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public header migration;
- static scans found no remaining raw TLS declarations for the migrated
  ANALYZE execution-state fields.

## Extension Creation Execution State Bridge

The seventy-second Phase 12 slice moves extension creation command state under
the logical execution object:

- `PgExecution` now owns `PgExecutionExtensionState`;
- `creating_extension` and `CurrentExtensionObject` remain source-compatible
  lvalue macros in `commands/extension.h`, backed by the active execution;
- `extension.c` no longer defines exported raw TLS storage for those names;
- runtime initialization sets the current extension object to `InvalidOid` for
  process, thread, and early execution state and adopts any early state into
  the installed execution object;
- the backend-runtime regression fixture adds
  `test_execution_extension_state_is_execution_local()`, switching
  `CurrentPgExecution` between fake executions and verifying both exported
  extension creation lvalues follow the active execution.

This state is execution-scoped because it is only valid while running one
`CREATE EXTENSION` or `ALTER EXTENSION UPDATE` script. Dependency recording,
ACL initialization, deletion, and event-trigger code still use the historical
public names, but they now read the active execution instead of a raw TLS
global.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `extension.o`, and
  `test_backend_runtime.o`;
- because `extension.h` and `backend_runtime.h` changed installed backend
  headers and formerly exported execution globals became compatibility macros,
  the backend clean plus generated utility and node-header recovery path was
  used before trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- rebuilding and reinstalling `src/test/modules/test_extensions` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_extension_state_is_execution_local()`;
- focused `test_extensions` and `test_extdepend` regressions passed, covering
  extension creation, update scripts, extension membership dependencies,
  event-trigger interactions, and extension dependency deletion behavior;
- focused `test_ext_backend_model` and `test_ext_backend_model_pooled`
  regressions passed after the public header migration;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public header migration;
- static scans found no remaining raw TLS declarations for the migrated
  extension creation execution-state fields.

## Materialized View Maintenance Execution State Bridge

The seventy-third Phase 12 slice moves the materialized-view incremental
maintenance depth counter under the logical execution object:

- `PgExecution` now owns `PgExecutionMatViewState`;
- `matview.c` keeps its local `matview_maintenance_depth` name as a
  compatibility macro backed by the active execution;
- runtime initialization zeroes this bucket for thread backends and adopts any
  early execution state into the installed process/thread execution object;
- the backend-runtime regression fixture adds
  `test_execution_matview_state_is_execution_local()`, switching
  `CurrentPgExecution` between fake executions and verifying the maintenance
  depth follows the active execution.

This removes the private materialized-view maintenance TLS counter from
`matview.c`. The state is execution-scoped because it is a per-command nesting
guard used while refreshing or incrementally maintaining a materialized view.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `matview.o`, and
  `test_backend_runtime.o`;
- because `backend_runtime.h` changed an installed backend header, the backend
  clean plus generated utility and node-header recovery path was used before
  trusting process-mode runtime tests;
- clean full `gmake -j8` passed after the backend clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_execution_matview_state_is_execution_local()`;
- the first focused regression run caught the expected-output underline width
  and final blank-line fixture adjustments for the new test block;
- core `src/test/regress` `parallel_schedule` passed all 245 tests, including
  the core `matview` regression;
- clean `gmake -C contrib clean && gmake -C contrib -j8` passed after the
  public header migration;
- static scans found no remaining raw TLS declaration for the migrated
  materialized-view execution-state field.

## Gate E2 Global Lifetime Scan Enforcement

The seventy-fourth Phase 12 slice turns the global-lifetime scanner into an
explicit Gate E2 validation target:

- the top-level configured makefile now exposes `gmake check-global-lifetimes`;
- the target runs `src/tools/global_lifetime/scan_global_lifetimes.pl` against
  `src/tools/global_lifetime/global_lifetime_baseline.tsv`;
- `AGENTS.md`, `MULTITHREADED_PLAN.md`, and the scanner README name this target
  as the required Phase 12 exit-gate command;
- the frontend `pg_global_prng_state` declaration and definition are annotated
  as `PG_GLOBAL_RUNTIME`, while backend builds still route the name through the
  `PgBackend` object-backed accessor.

This does not complete the broader Gate E2 lifecycle, GUC, PMChild, startup,
or stress-test requirements. It makes the global-classification requirement
enforceable so later Phase 12 work cannot add unclassified mutable globals
without a failing target.

Validation for this slice:

- `./config.status GNUmakefile` regenerated the local configured makefile from
  `GNUmakefile.in`;
- `gmake check-global-lifetimes` passed, scanning 1923 declarations with the
  checked baseline and reporting zero new unclassified mutable globals;
- touched-object builds passed for frontend and backend PRNG objects via
  `gmake -C src/common pg_prng.o pg_prng_srv.o`;
- `git diff --check` passed.

## PMChild Thread-Backend Publication Boundary

The seventy-fifth Phase 12 slice starts closing the Gate E2
PMChild/thread-backed backend ownership blocker:

- `PMChild` keeps the raw `thread_backend` pointer behind PMChild-owned APIs;
- publishing and clearing the thread-backed logical backend now happens under a
  `PMChildThreadBackendMutex`;
- postmaster signal routing no longer dereferences `pmchild->thread_backend`
  directly and instead asks PMChild to raise the logical interrupt while the
  publication lock is held;
- background-worker notification wakeups use
  `PostmasterChildWakeThreadBackend()` instead of directly waking the raw
  pointer;
- `test_backend_runtime` adds `test_pmchild_thread_backend_signal_api()` to
  verify that a thread-backed PMChild publishes a stable logical signal id,
  delivers a logical interrupt through the protected API, and refuses interrupt
  or wakeup delivery after unpublishing the backend.

This does not complete all PMChild Gate E2 work. It removes the most direct
unsynchronized pointer handoff in postmaster signal and wakeup paths. Remaining
work still needs stress coverage for shutdown and termination races plus a
fuller ownership contract for thread exit publication, join, and PMChild slot
release.

Validation for this slice:

- touched-object builds passed for `pmchild.o`, `postmaster.o`,
  `launch_backend.o`, and `test_backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and includes
  `test_pmchild_thread_backend_signal_api()`;
- `gmake check-global-lifetimes` passed, reporting zero new unclassified
  mutable globals against the checked baseline;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- threaded TAP coverage for `001_threaded_runtime.pl` and
  `002_threaded_bgworker_crash.pl` did not reach PostgreSQL because the system
  Perl is missing `IPC::Run`, matching the existing local-build note in
  `AGENTS.md`.

## PMChild Thread-Exit Publication Boundary

The seventy-sixth Phase 12 slice tightens the same Gate E2 ownership area by
making thread-exit publication a single PMChild operation:

- `PostmasterChildPublishThreadExit()` now clears the thread-backed
  `PgBackend` pointer under the PMChild thread-backend lock, stores the
  waitpid-style thread exit status, publishes the `thread_exited` flag, and
  wakes the postmaster latch;
- `backend_thread_finish()` no longer sequences raw unpublish and exit-mark
  operations itself;
- the postmaster reap loop now logs `pg_thread_join()` failures instead of
  silently ignoring them;
- `test_pmchild_thread_backend_signal_api()` now verifies that the publish
  helper exposes the exit status once and rejects later interrupt/wakeup
  delivery after the backend pointer is unpublished.

This still does not complete the full Gate E2 lifecycle blocker. It establishes
the local PMChild handoff contract for exit publication and join observability;
remaining work must still define and test memory/resource cleanup for
backend/session/connection/execution state after thread exit.

Validation for this slice:

- touched-object builds passed for `pmchild.o`, `postmaster.o`,
  `launch_backend.o`, and `test_backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed;
- `gmake check-global-lifetimes` passed, reporting zero new unclassified
  mutable globals against the checked baseline;
- core `src/test/regress` `parallel_schedule` passed all 245 tests.

## Threaded Exit Top-Memory Accounting

The seventy-seventh Phase 12 slice starts addressing the Gate E2 teardown
accounting requirement for the currently retained carrier `TopMemoryContext`:

- `backend_thread_finish()` measures the exiting carrier's recursive
  `TopMemoryContext` allocation with `MemoryContextMemAllocated()`;
- `PostmasterChildPublishThreadExit()` carries that retained-memory value
  through the same PMChild exit-publication handoff as the exit status;
- `PostmasterChildHasExitedThread()` returns both the waitpid-style exit status
  and the retained top-memory total to the postmaster reaper;
- the postmaster logs retained top-memory bytes at `DEBUG2` before joining the
  exited thread;
- `test_pmchild_thread_backend_signal_api()` now verifies that PMChild exit
  accounting is published exactly once together with the exit status.

This is explicit accounting, not full cleanup. The branch still intentionally
does not delete thread-carrier `TopMemoryContext` at exit because that has been
observed to corrupt later carrier startup. Gate E2 remains open until the
retained memory and other backend/session/connection/execution resources are
either safely cleaned up or deliberately owned by a documented longer-lived
runtime object with stronger stress coverage.

Validation for this slice:

- touched-object builds passed for `pmchild.o`, `postmaster.o`,
  `launch_backend.o`, and `test_backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed;
- `gmake check-global-lifetimes` passed, reporting zero new unclassified
  mutable globals against the checked baseline;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- threaded TAP coverage for `001_threaded_runtime.pl` and
  `002_threaded_bgworker_crash.pl` still did not reach PostgreSQL because the
  system Perl is missing `IPC::Run`, matching the existing local-build note in
  `AGENTS.md`.

## Threaded Session GUC Rebind Adoption

The seventy-eighth Phase 12 slice starts closing the Gate E2 GUC startup
blocker by replacing the broad hard-coded GUC initialization whitelist in
`InitializeThreadedSessionGUCOptions()`:

- threaded startup now builds the per-thread generated GUC table, records each
  direct backing-variable pointer after `InitializeGUCVariablePointers()`,
  runs `RebindSessionGUCVariablePointers()`, and initializes every built-in GUC
  record whose backing pointer changed;
- this makes startup initialization follow the existing
  `PgSession`/runtime-backed GUC migration table instead of depending on a
  manually curated list of option names reached by the current smoke tests;
- thread-backed client and worker startup now build the GUC table before
  runtime installation, using the existing early fallback runtime accessors
  during `RebindSessionGUCVariablePointers()`. The later runtime install adopts
  that initialized early state into the logical session, and the later
  `InitPostgres()` threaded-backend call is a no-op if the table already
  exists;
- the only remaining hand-curated threaded startup compatibility list is for
  TLS dummy startup GUCs without explicit session accessors:
  `session_authorization`, `server_encoding`, and `client_encoding`.

This is not the full Gate E2 GUC closure. It covers systematic initialization
for built-in generated GUC records whose direct backing variables have already
been migrated to session/runtime state. Remaining work must still define and
test postmaster/runtime default adoption, complete assign-hook/reset/default
semantics, database/role/startup option behavior, extension/custom GUCs, and
GUC-heavy threaded stress coverage.

Validation for this slice:

- touched-object builds passed for `guc.o` and `launch_backend.o`;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- focused `test_backend_runtime` regression passed;
- `gmake check-global-lifetimes` passed, reporting zero new unclassified
  mutable globals against the checked baseline;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- manual threaded GUC smoke with `multithreaded = on` reached server startup,
  client connection, `SHOW multithreaded`, several `SET` paths, and
  `RESET ALL`. It confirmed that postmaster/runtime default adoption remains
  incomplete (`work_mem` stayed at the boot default rather than the configured
  value) and then hit an existing broader threaded worker/WAL crash in the
  background writer (`LogStandbySnapshot()` -> `XLogInsert()`) after the client
  sequence;
- threaded TAP coverage for `001_threaded_runtime.pl` and
  `002_threaded_bgworker_crash.pl` still did not reach PostgreSQL because the
  system Perl is missing `IPC::Run`, matching the existing local-build note in
  `AGENTS.md`.

## Threaded Startup Nondefault GUC Replay

The seventy-ninth Phase 12 slice extends the same Gate E2 GUC adoption path by
replaying the postmaster's serialized nondefault GUC state in threaded backend
startup:

- non-EXEC_BACKEND postmasters now write `global/config_exec_params` when
  `multithreaded` is enabled, while EXEC_BACKEND keeps its existing
  unconditional child-process write path;
- SIGHUP reloads also refresh the serialized nondefault GUC file for threaded
  non-EXEC_BACKEND postmasters, matching the existing EXEC_BACKEND refresh;
- after `InitializeThreadedSessionGUCOptions()` builds and rebinds the
  per-thread GUC table, `backend_thread_entry()` now calls
  `read_nondefault_variables()`, matching the existing process-backend
  `SubPostmasterMain()` replay path;
- because this happens before `InstallPgThreadBackendRuntimeState()`, replayed
  direct-pointer GUC values are written into the early fallback session/runtime
  buckets and then adopted into the thread's `PgSession`/runtime objects during
  runtime installation;
- this specifically moves configured postmaster/runtime defaults, such as
  `work_mem`, from boot defaults toward the same effective state that forked
  or EXEC_BACKEND children receive.

This still is not complete Gate E2 GUC closure. The replay uses PostgreSQL's
existing serialized nondefault format for built-in GUCs, but custom/extension
GUC behavior, reset/default edge cases, database/role settings, and broader
threaded stress coverage remain open.

Validation for this slice:

- touched-object and full builds passed for `postmaster.o`, `guc.o`,
  `launch_backend.o`, and full `gmake -j8`;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed, reporting zero new unclassified
  mutable globals against the checked baseline;
- manual threaded GUC smoke with `multithreaded = on` and `work_mem = '8MB'`
  confirmed `SHOW work_mem` starts at `8MB`, `SET work_mem = '9MB'` changes
  the threaded session value, and `RESET work_mem` returns to the configured
  `8MB` default. The smoke also confirmed the postmaster wrote
  `global/config_exec_params`;
- the same threaded smoke did not shut down cleanly under `pg_ctl -m immediate`:
  the postmaster received the immediate shutdown request, logged a logical
  replication launcher thread exit, then issued SIGKILL to recalcitrant
  children. That is retained as part of the Gate E2 threaded teardown blocker;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- focused `test_backend_runtime` regression passed.

## Threaded Auxiliary ProcDie Shutdown

The eightieth Phase 12 slice closes the immediate-shutdown hang found by the
previous threaded GUC replay smoke:

- in thread-backed auxiliary workers, the postmaster maps `SIGQUIT`,
  `SIGKILL`, and `SIGABRT` to the logical `PG_BACKEND_INTERRUPT_PROC_DIE`
  mailbox instead of delivering a process signal handler that can `_exit()`;
- `ProcessMainLoopInterrupts()` now treats `ProcDiePending` as an immediate
  `proc_exit(1)` request, which covers background writer and WAL writer
  thread carriers;
- `ProcessCheckpointerInterrupts()` and
  `ProcessAutoVacLauncherInterrupts()` now do the same for their custom
  interrupt dispatch loops;
- this lets background writer, checkpointer, autovacuum launcher, and WAL
  writer thread carriers retire through the existing thread-exit publication
  and postmaster reaping path during immediate shutdown.

This is still not full Gate E2 teardown completion. The branch still retains
carrier `TopMemoryContext` allocations and needs broader resource accounting,
abandoned-client coverage, repeated reconnect stress, and join/slot-release
race testing. It does remove a concrete clean-fast-shutdown blocker for
thread-backed in-tree auxiliary workers.

Validation for this slice:

- touched-object builds passed for `interrupt.o`, `checkpointer.o`, and
  `autovacuum.o`, with `bgwriter.o` and `walwriter.o` already up to date;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- direct threaded immediate-shutdown smoke with `multithreaded = on`, one
  client query, and thread-backed auxiliary workers passed: `pg_ctl -m
  immediate -w -t 8 stop` reported `server stopped` and the log did not
  contain `issuing SIGKILL to recalcitrant children`;
- a five-cycle threaded startup/query/immediate-shutdown loop also passed
  without `issuing SIGKILL to recalcitrant children` or `server does not shut
  down`, giving basic repeated teardown coverage for the same auxiliary worker
  set;
- `gmake check-global-lifetimes` passed, reporting zero new unclassified
  mutable globals against the checked baseline;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- focused `test_backend_runtime` regression passed.

## Threaded Startup Gate Policy

The eighty-first Phase 12 slice centralizes the temporary threaded startup
serialization gate policy:

- `backend_thread_run_worker()` now calls a single
  `backend_thread_requires_startup_gate()` helper instead of open-coding the
  two worker classes that can bypass the gate;
- the only worker classes currently allowed to bypass the gate remain AIO
  workers and the syslogger, matching the previously validated behavior;
- all other thread-backed workers remain inside the gate until their startup
  shared-state dependencies are isolated and covered by worker-specific
  catalog-startup stress.

An attempted broader bypass for non-session auxiliary workers was tested first:
background writer, checkpointer, WAL writer, archiver, startup, WAL receiver,
and WAL summarizer carriers were allowed to start outside the gate. A simple
threaded `select 1` startup/query/immediate-shutdown smoke passed, but a
threaded `select count(*) > 0 from pg_class` catalog smoke caused an abrupt
postmaster death with no normal PostgreSQL error log before shutdown could run.
The experiment was backed out to the conservative helper policy above.

This is not a full Gate E2 startup-gate closure. It makes the current critical
section auditable and prevents unreviewed worker classes from bypassing it,
but the remaining broad gate still needs worker-by-worker isolation before
Phase 13 scheduler-aware wait work can proceed.

Validation for this slice:

- touched-object build passed for `launch_backend.o`;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- a three-cycle threaded startup/`select 1`/immediate-shutdown smoke passed
  without `issuing SIGKILL to recalcitrant children` or `server does not shut
  down`;
- `gmake check-global-lifetimes` passed, reporting zero new unclassified
  mutable globals against the checked baseline;
- core `src/test/regress` `parallel_schedule` passed all 245 tests;
- focused `test_backend_runtime` regression passed;
- the broader bypass experiment failed the threaded `pg_class` catalog smoke
  and is recorded as the next startup-gate narrowing blocker.

## PMChild Thread-Join Retry Boundary

The eighty-second Phase 12 slice tightens the Gate E2 PMChild
join/reaping/slot-release contract:

- `process_pm_thread_exit()` now treats successful `pg_thread_join()` as the
  boundary before PMChild cleanup and slot release;
- if joining a thread-backed child fails, the postmaster logs the failure,
  calls `PostmasterChildRetryThreadExit()`, and leaves the PMChild entry on
  `ActiveChildList` instead of running child cleanup against an unjoined
  carrier;
- `PostmasterChildRetryThreadExit()` re-publishes the already stored
  waitpid-style exit status and retained top-memory payload, making the
  claimed exit report visible to a later postmaster loop;
- `test_pmchild_thread_backend_signal_api()` now verifies that a claimed
  thread-exit payload is one-shot, can be re-published for retry, and remains
  one-shot after the retry.

This still does not complete Gate E2 lifecycle cleanup. It closes a concrete
slot-reuse hazard after native thread join failure, but broader stress coverage
is still needed for normal disconnects, abandoned clients, administrator
termination, SQL `ERROR` recovery, repeated reconnects, and worker
launch/shutdown races.

Validation for this slice:

- touched-object builds passed for `pmchild.o`, `postmaster.o`, and full
  `gmake -j8`;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- focused `test_backend_runtime` regression passed and covers the retry
  publication path in `test_pmchild_thread_backend_signal_api()`;
- a three-cycle threaded startup/`select 1`/immediate-shutdown smoke passed
  without `issuing SIGKILL to recalcitrant children` or `server does not shut
  down`;
- `gmake check-global-lifetimes` passed, reporting zero new unclassified
  mutable globals against the checked baseline;
- core `src/test/regress` `parallel_schedule` passed all 245 tests.

## Threaded Custom Extension GUC Session Init

The eighty-third Phase 12 slice starts closing the Gate E2 custom/extension
GUC blocker:

- `PgSession` now owns `dynamic_library_inits`, a list of process-loaded
  dynamic library entries whose `_PG_init()` function has been invoked for the
  current logical session;
- `dfmgr.c` still keeps the loaded-library list runtime-global, matching the
  process-wide address space, but when thread-per-session mode reuses an
  already loaded module in a different session it calls `_PG_init()` again so
  that session's per-thread GUC table receives the module's custom GUC
  definitions;
- newly loaded modules are also marked initialized for the loading session;
- threaded runtime installation now calls
  `InitializeThreadedSessionRequiredGUCOptions()` after
  `PgSetCurrentSession()` and after `CurrentPgExecution` is installed. This
  initializes required string GUCs whose
  generated records can otherwise already point at fallback accessors before
  the changed-pointer initialization pass runs. At this point in the work, the
  list covered `search_path` and `dynamic_library_path`;
- the threaded runtime TAP fixture now validates custom GUC behavior through
  `LOAD` and `SHOW`, avoiding catalog-writing DDL while the remaining WAL
  insertion blocker is open.

This is not full Gate E2 GUC completion. It proves the first in-tree route for
custom extension GUC definitions in thread-per-session mode, including
placeholder conversion when a module is loaded in multiple sessions. Broader
custom GUC reset/default semantics, database/role/startup settings,
contrib-wide extension coverage, and GUC-heavy stress remain open. During
validation, a threaded `CREATE TABLE` smoke got past namespace lookup and then
crashed in `XLogInsert()` while accessing derived WAL GUC state; the next
slice tracks that separate Gate E2 blocker.

Validation for this slice:

- touched-object builds passed for `namespace.o`, `backend_runtime.o`,
  `guc.o`, `dfmgr.o`, and `test_backend_runtime_threaded.o`;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- a manual threaded custom-GUC smoke with `multithreaded = on`,
  `dynamic_shared_memory_type = posix`, `LOAD 'test_backend_runtime_threaded'`,
  and `SHOW test_backend_runtime_threaded.custom_guc` proved `session one`,
  `session two`, and then `default` across three separate sessions;
- `lldb` confirmed the earlier `CREATE FUNCTION` crash first came from missing
  `search_path`, then after fixing required string GUCs moved to the existing
  DDL/WAL path (`CREATE TABLE` -> `XLogInsert()`);
- focused `gmake -C src/test/modules/test_backend_runtime check` is not a
  valid control for this slice in the current checkout: it starts a plain
  process-mode temp cluster and fails at the existing
  `test_backend_thread_runtime_state()` threaded-runtime assertion before it
  reaches the new `LOAD`/`SHOW` fixture.

## Threaded Catalog-Writing DDL WAL GUC Bootstrap

The eighty-fourth Phase 12 slice fixes the first catalog-writing table DDL
crash found by the extension/custom GUC work:

- a threaded `CREATE TABLE threaded_gate_e2_smoke(id int)` crashed in
  `XLogInsert()` while executing `XLogPutNextOid()`;
- `lldb` showed the fault in the inlined `XLogRecordAssemble()` access to
  `wal_consistency_checking[rmid]`;
- `wal_consistency_checking` is a derived per-session bool array populated by
  the string GUC's assign hook, not the generated GUC table's direct string
  backing variable itself;
- the systematic GUC rebind pass correctly points the string record at
  `PgSession.access_wal_guc.wal_consistency_checking_string_value`, but a
  default NULL string value can leave the derived bool array unset until the
  assign hook runs for the actual session;
- `InitializeThreadedSessionRequiredGUCOptions()` now includes
  `wal_consistency_checking` along with `search_path` and
  `dynamic_library_path`, forcing the hook to create the per-session bool
  array before table DDL reaches WAL insertion;
- the threaded runtime TAP fixture now includes a basic
  `CREATE TABLE`/`INSERT`/`DROP TABLE` smoke so table DDL covers this required
  bootstrap path.

This is still not full Gate E2 DDL or GUC completion. It closes the immediate
`CREATE TABLE` WAL consistency pointer crash, but broader table DDL,
extension DDL, database/role/startup setting behavior, assign-hook
reset/default semantics, and GUC-heavy stress remain open.

Validation for this slice:

- `lldb` reproduced the pre-fix crash as `CREATE TABLE` ->
  `XLogPutNextOid()` -> `XLogInsert()`;
- touched-object builds covered `guc.o` and the threaded test module;
- full `gmake -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- install and threaded runtime module reinstall passed;
- a manual threaded smoke with `multithreaded = on`,
  `dynamic_shared_memory_type = posix`, and `CREATE TABLE`/`INSERT`/
  `DROP TABLE` passed after the fix.

## Threaded Database Role Startup GUC Coverage

The eighty-fifth Phase 12 slice adds focused coverage for the Gate E2
database/role/startup GUC requirement:

- the threaded runtime TAP fixture now creates a login role and installs GUC
  defaults through `ALTER DATABASE` and `ALTER ROLE`;
- the role connection verifies the database default `work_mem`, the role
  default `statement_timeout`, and the role default
  `default_statistics_target`;
- a separate role connection uses a libpq startup packet
  `options='-c lock_timeout=8s'` and verifies that the startup option is
  applied in the threaded session;
- the selected settings cover both string/time display behavior and
  direct-pointer GUC backing variables that have moved under `PgSession`.

This does not close all Gate E2 GUC work. It proves the basic catalog-backed
database/role setting path and startup option path for built-in GUCs in
threaded sessions. Reset/default edge cases, transaction-local GUC stack
behavior, broader assign-hook semantics, custom extension GUC stress, and
larger GUC-heavy threaded workloads remain open.

Validation for this slice:

- a manual threaded smoke with `multithreaded = on` verified `ALTER DATABASE`
  `work_mem`, `ALTER ROLE` `statement_timeout` and
  `default_statistics_target`, and startup `options=-c lock_timeout=8s`;
- direct Perl syntax/TAP validation for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` was
  blocked in this checkout because system Perl lacks `IPC::Run`, matching the
  local test notes;
- full `gmake -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals.

## Threaded GUC Reset And Stack Coverage

The eighty-sixth Phase 12 slice adds focused coverage for the Gate E2
reset/default and transaction-local GUC stack requirement:

- the threaded runtime TAP fixture now runs a role-backed threaded session
  through `SET`, `SET LOCAL`, `ROLLBACK`, and `RESET` for `work_mem`, proving
  rollback to the session value and reset to the catalog-backed database
  default;
- the fixture runs `SET LOCAL statement_timeout` through `COMMIT`, proving the
  role default is restored after the transaction-local value is discarded;
- the fixture runs a startup-packet `options=-c lock_timeout=8s` connection
  through session `SET` and `RESET`, proving `RESET` returns to the startup
  option source rather than the compiled default;
- the fixture now checks custom extension GUC stack behavior after
  per-session module initialization by running `SET`, `SET LOCAL`, `COMMIT`,
  and `RESET` for `test_backend_runtime_threaded.custom_guc`.

This does not close all Gate E2 GUC work. It covers the first reset/default
edge cases for built-in and custom GUCs, but broader assign-hook coverage,
extension-DDL/custom-GUC stress, and larger GUC-heavy threaded workloads remain
open before Phase 13 scheduler-aware wait work.

Validation for this slice:

- a manual threaded smoke with `multithreaded = on` verified database default,
  role default, startup-packet reset, and transaction-local built-in GUC stack
  behavior. The attempted unprivileged custom-GUC `LOAD` path failed with the
  expected library-access policy error, so durable custom-GUC stack coverage is
  kept in the superuser `LOAD` path in the TAP fixture;
- direct Perl syntax/TAP validation for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` remains
  blocked in this checkout because system Perl lacks `IPC::Run`, matching the
  local test notes;
- a manual threaded smoke with `multithreaded = on` verified the expected
  built-in GUC stack values (`3MB`, `7s`, `77`, `4MB`, `5MB`, `4MB`, `3MB`,
  `9s`, `7s`), startup-packet `lock_timeout` reset values (`8s`, `9s`,
  `8s`), and custom extension GUC stack values (`changed`, `local`,
  `changed`, `default`);
- full `gmake -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Threaded GUC Heavy Stress Coverage

The eighty-seventh Phase 12 slice adds the first concurrent GUC-heavy stress
coverage required by Gate E2:

- the threaded runtime TAP fixture now starts four simultaneous `psql`
  scripts against the threaded postmaster;
- each script loads `test_backend_runtime_threaded`, ensuring per-session
  custom GUC module initialization is exercised under concurrent startup;
- each script repeatedly updates built-in direct-pointer GUCs
  (`work_mem`, `default_statistics_target`, and `lock_timeout`), assign-hook
  GUCs (`search_path`, `bytea_output`, `IntervalStyle`, and
  `wal_consistency_checking`), and the custom extension GUC
  `test_backend_runtime_threaded.custom_guc`;
- each script verifies transaction-local `work_mem` and custom-GUC values
  inside the transaction, then verifies final session values after commit;
- the expected final values include worker-specific custom GUC text, so the
  stress catches cross-session leakage between concurrent thread-backed
  sessions.

This does not complete Gate E2. It closes the first larger GUC-heavy threaded
workload gap, including an assign-hook path for `wal_consistency_checking`,
but broader extension DDL, lifecycle teardown/resource cleanup, PMChild race
stress, and startup-gate narrowing remain open before Phase 13.

Validation for this slice:

- a manual threaded smoke with `multithreaded = on` ran four concurrent
  `psql` scripts with the same GUC-heavy workload and verified
  `local-N:16MB:local-N` plus
  `done-N:5MB:125:2025ms:stress-N-25` for workers 1 through 4;
- parser-only Perl syntax validation for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed
  using a temporary local `IPC::Run` stub, because this checkout's system Perl
  still lacks the real `IPC::Run` module required to execute TAP tests;
- full `gmake -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Threaded Temp Tablespace And Teardown Stress

The eighty-eighth Phase 12 slice fixes a temp-table startup/adoption crash and
adds broader Gate E2 teardown stress:

- a manual four-client abandoned-session smoke reproduced an abrupt postmaster
  death during concurrent threaded `CREATE TEMP TABLE`;
- lldb showed two client backend threads crashing in
  `PrepareTempTablespaces()` through `pstrdup(temp_tablespaces)`, where
  `temp_tablespaces` was still NULL for the session-local tablespace state;
- `InitializeThreadedSessionRequiredGUCOptions()` now includes
  `temp_tablespaces`, forcing the default string value and assign-hook state to
  be initialized before temp table DDL can call `PrepareTempTablespaces()`;
- the threaded runtime TAP fixture now starts four idle-in-transaction clients
  that hold advisory locks and create temp tables, kills those clients, and
  verifies the advisory locks are released;
- the fixture also starts four idle threaded client backends, terminates all
  of them through `pg_terminate_backend()`, and verifies they disappear from
  `pg_stat_activity`.

This still does not complete Gate E2 lifecycle cleanup. The branch still
retains carrier `TopMemoryContext` allocations and needs stronger resource
ownership or cleanup, PMChild race stress, extension-DDL coverage, and
startup-gate narrowing before Phase 13.

Validation for this slice:

- the pre-fix manual repro killed the postmaster with no PostgreSQL log entry;
  lldb captured the crash as `PrepareTempTablespaces()` ->
  `pstrdup(temp_tablespaces)` -> `strlen(NULL)`;
- touched-object build for `src/backend/utils/misc/guc.o` passed;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- after the fix, a manual threaded smoke with `multithreaded = on` verified
  four concurrent idle abandoned clients released advisory locks
  (`abandoned_locks_before=4`, `abandoned_locks_after=0`), four administrator
  terminations were accepted and reaped (`terminate_accepted=t`,
  `terminate_gone=t`), and the server still returned `SELECT 42`;
- parser-only Perl syntax validation for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed
  using a temporary local `IPC::Run` stub, because this checkout's system Perl
  still lacks the real `IPC::Run` module required to execute TAP tests;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Threaded Extension DDL Coverage

The eighty-ninth Phase 12 slice adds a real thread-compatible extension DDL
path for the threaded runtime fixture:

- `src/test/modules/test_backend_runtime` now installs
  `test_backend_runtime_threaded.control` and
  `test_backend_runtime_threaded--1.0.sql`;
- the extension script exposes the existing `test_backend_runtime_threaded`
  helper functions through `MODULE_PATHNAME`, using the same shared library
  that is marked with
  `PG_MODULE_MAGIC_BACKEND_MODEL_THREAD_PER_SESSION`;
- the threaded runtime TAP fixture now creates
  `test_backend_runtime_threaded` with `CREATE EXTENSION` instead of declaring
  the helper C functions ad hoc;
- the fixture verifies an extension-created custom-GUC helper function and
  confirms `_PG_init()` initialized the module's per-session custom GUC state;
- after all helper calls are complete, the fixture drops the extension and
  verifies that the threaded server remains usable.

This does not complete all Gate E2 extension work. It proves the focused
thread-compatible extension DDL route for an in-tree test module, but broader
contrib/in-tree extension coverage and full lifecycle/startup-gate blockers
remain open before Phase 13.

Validation for this slice:

- `gmake -C src/test/modules/test_backend_runtime all` passed;
- `gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install"
  install` passed;
- a manual threaded smoke with `multithreaded = on` verified
  `CREATE EXTENSION test_backend_runtime_threaded`, custom-GUC helper output
  (`default`, `t`), a thread-model background-worker helper call, and
  `DROP EXTENSION test_backend_runtime_threaded`;
- parser-only Perl syntax validation for
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed
  using a temporary local `IPC::Run` stub, because this checkout's system Perl
  still lacks the real `IPC::Run` module required to execute TAP tests;
- full `gmake -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Threaded Startup Gate Auxiliary Writer Narrowing

The ninetieth Phase 12 slice narrows the temporary threaded startup
serialization gate for the lowest-risk auxiliary writer classes:

- `backend_thread_requires_startup_gate()` now allows background writer,
  checkpointer, and WAL writer thread carriers to bypass the global startup
  mutex, joining the already-validated AIO worker and syslogger bypasses;
- those three classes share `AuxiliaryProcessMainCommon()`, which creates the
  auxiliary PGPROC, initializes procsignal/barrier state, creates the
  auxiliary resource owner, starts pgstat, sets normal processing mode, and
  then enters worker-specific loops without running database/session bootstrap;
- the remaining classes that were part of the earlier failed broad bypass
  experiment stay gated: archiver, startup, WAL receiver, WAL summarizer,
  background workers, autovacuum, slot sync, and regular client backend
  startup;
- the policy comment now requires worker-specific shared-state isolation and
  threaded catalog-startup stress before adding more worker classes to the
  bypass list.

This is not the full Gate E2 startup-gate closure. It removes serialization
from three auxiliary writer startup paths that do not perform session/database
bootstrap, but the regular backend bootstrap and several server worker
families remain gated until their catalog/cache/lifecycle dependencies are
isolated or otherwise proven.

Validation for this slice:

- touched-object build for `src/backend/postmaster/launch_backend.o` passed;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- a threaded startup/concurrent-catalog/fast-shutdown smoke with
  `multithreaded = on` and `dynamic_shared_memory_type = posix` passed with
  eight simultaneous clients running catalog scans and temp-table DDL while
  background writer, checkpointer, and WAL writer bypassed the startup gate;
- server log inspection for that smoke found no crash, no postmaster death,
  and no `issuing SIGKILL to recalcitrant children` shutdown escalation;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## PMChild Thread Slot-Reuse Scrub

The ninety-first Phase 12 slice tightens the PMChild/thread-backed backend
ownership contract for slot reuse:

- PMChild slot initialization and assignment now reset the carrier-visible
  `signal_pid` field alongside the existing PID, thread-backend pointer, and
  thread-exit fields;
- `ReleasePostmasterChildSlot()` now clears `pid`, `signal_pid`,
  `thread_backend`, the waitpid-style thread-exit status, retained top-memory
  accounting, and the thread-exited flag before returning a PMChild entry to
  its freelist;
- `PostmasterChildPublishThreadExit()` still preserves the exited logical
  backend id long enough for the postmaster reaper to log and join the native
  thread, so this does not remove the debugging value of the exit report;
- `test_pmchild_thread_backend_signal_api()` now seeds stale signal and
  thread-exit state, calls `PostmasterChildSetThread()`, and verifies the
  thread carrier starts with no visible signal id or pending exit report before
  `PostmasterChildSetThreadBackend()` publishes the logical backend.

This is not full Gate E2 PMChild completion. It removes one stale-metadata
slot-reuse hazard and strengthens unit coverage for the PMChild publication
boundary, but broader runtime stress is still needed for concurrent
termination, worker exit, postmaster signal routing, and slot release under
native thread join/retry.

Validation for this slice:

- touched-object builds passed for `src/backend/postmaster/pmchild.o` and
  `src/test/modules/test_backend_runtime/test_backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- a direct SQL smoke against a fresh temp cluster returned `t` for
  `CREATE EXTENSION test_backend_runtime; SELECT test_pmchild_thread_backend_signal_api();`;
- before the follow-up runtime-state test isolation fix, direct full-module
  `pg_regress test_backend_runtime` was unsuitable for this narrow check
  because it aborted at the existing `test_backend_thread_runtime_state()`
  control-path assertion before reaching the PMChild function;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Thread Runtime State Test Isolation

The ninety-second Phase 12 slice restores the focused backend-runtime
regression path used by Gate E2 PMChild and state-migration checks:

- `test_backend_thread_runtime_state()` now validates constructed
  `PgThreadBackendRuntimeState` object links without installing the fake
  thread backend into the live process-mode test backend;
- `test_backend_thread_ids_are_logical()` now compares assigned logical
  backend ids from two constructed thread-backend states instead of switching
  `CurrentPgBackend` twice inside the active SQL session;
- both helpers now assert that the current process-mode runtime, carrier,
  backend, session, connection, and execution pointers are unchanged by the
  fake state construction;
- this keeps the tests aligned with their purpose, which is to verify the
  thread-runtime object layout and logical-id assignment, while avoiding
  accidental GUC/runtime adoption in the regression backend.

This does not close the Gate E2 lifecycle blockers. It makes the ordinary
`test_backend_runtime` regression usable again as a validation control for
PMChild publication and state-migration unit coverage.

Validation for this slice:

- touched-object build passed for
  `src/test/modules/test_backend_runtime/test_backend_runtime.o`;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- full `gmake -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## PMChild Thread-Exit Signal ID Capture

The ninety-third Phase 12 slice tightens the Gate E2 PMChild exit-publication
boundary:

- `PMChild` now stores `thread_exit_signal_pid` as part of the thread-exit
  payload;
- `PostmasterChildPublishThreadExit()` captures the exiting logical backend id
  into that payload and clears live `signal_pid` under the same
  `PMChildThreadBackendMutex` section that clears `thread_backend`;
- the postmaster reaper consumes the captured id for retained-memory and
  join-failure logging, so diagnostics keep the exited logical backend id
  without advertising a dead thread as signalable;
- PMChild initialization, assignment, process-carrier reset, dead-end child
  allocation, and slot release all scrub the captured exit id together with
  the other thread-exit fields;
- `test_pmchild_thread_backend_signal_api()` now verifies that thread-exit
  publication clears the live signal id, preserves the captured exit id across
  the first report, and preserves it again after a join retry.

This is not full Gate E2 PMChild completion. It closes another stale-routing
window in the local PMChild API contract, but broader stress is still needed
for concurrent postmaster signal routing, administrator termination,
abandoned-client teardown, worker exit, and native thread join/retry races.

Validation for this slice:

- touched-object builds passed for `src/backend/postmaster/pmchild.o`,
  `src/backend/postmaster/postmaster.o`, and
  `src/test/modules/test_backend_runtime/test_backend_runtime.o`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- a manual threaded temp-cluster smoke with `multithreaded = on` passed 12
  reconnects, `pg_terminate_backend()` against a sleeping client backend,
  a post-termination `SELECT 42`, and clean `pg_ctl -m fast stop`;
- `git diff --check` passed.

## PMChild Thread Read-Side Synchronization

The ninety-fourth Phase 12 slice tightens the Gate E2 PMChild synchronization
contract on the read side:

- `PostmasterChildSignalPid()` now reads a thread-backed PMChild's live
  `signal_pid` while holding `PMChildThreadBackendMutex`;
- `PostmasterChildHasExitedThread()` now claims the thread-exited flag and
  then copies the waitpid-style exit status, retained top-memory accounting,
  and captured exit signal id while holding the same mutex;
- this matches the write-side protocol used by
  `PostmasterChildSetThreadBackend()` and
  `PostmasterChildPublishThreadExit()`, so postmaster signal routing and exit
  reaping no longer read PMChild thread-owned identity/payload fields outside
  the PMChild synchronization boundary.

This is still not the full PMChild Gate E2 closure. It narrows the local data
race surface around PMChild identity and exit payload fields, but broader
runtime stress is still needed for concurrent signal routing, administrator
termination, abandoned-client teardown, worker exit, and native thread
join/retry races.

Validation for this slice:

- touched-object build passed for `src/backend/postmaster/pmchild.o`;
- touched-object check for `src/backend/postmaster/postmaster.o` was already
  up to date against the new PMChild API;
- touched-object check for
  `src/test/modules/test_backend_runtime/test_backend_runtime.o` was already
  up to date against the unchanged PMChild helper test;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- a manual threaded temp-cluster smoke with `multithreaded = on` passed 16
  reconnects, `pg_terminate_backend()` against a sleeping client backend, a
  post-termination `SELECT 84`, clean `pg_ctl -m fast stop`, and log
  inspection for crash/escalation markers;
- `git diff --check` passed.

## Threaded Client Socket Handoff

The ninety-fifth Phase 12 slice closes a concrete Gate E2 teardown-resource
hole in regular threaded backend startup:

- `pq_init()` still copies the accepted socket descriptor into `Port`, but now
  marks the launch-time `ClientSocket` as consumed only after `socket_close()`
  has been registered as the `Port` exit callback;
- `backend_thread_finish()` closes `BackendThreadStart.client_sock.sock` only
  if it is still valid at thread exit, making it the backstop for startup
  failures before `Port` owns the descriptor;
- after a successful `pq_init()` handoff, the copied launch socket is invalid
  and `socket_close()` remains the sole owner of closing `MyProcPort->sock`;
- this avoids both an early-startup descriptor leak in threaded backends and a
  later double-close hazard after `Port` has taken ownership.

This is not full Gate E2 teardown completion. It resolves one descriptor
ownership edge for regular client backends; the broader retained
`TopMemoryContext` and backend/session/connection/execution resource ownership
model remains a Gate E2 blocker.

Validation for this slice:

- touched-object builds passed for `src/backend/libpq/pqcomm.o` and
  `src/backend/postmaster/launch_backend.o`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- a manual threaded temp-cluster smoke with `multithreaded = on` passed 24
  reconnects, `pg_terminate_backend()` against a sleeping client backend, a
  post-termination `SELECT 168`, clean `pg_ctl -m fast stop`, and log
  inspection for crash/escalation/assertion/bad-descriptor markers;
- `git diff --check` passed.

## Threaded Startup Gate Archiver Narrowing

The ninety-sixth Phase 12 slice narrows the temporary threaded startup
serialization gate for the WAL archiver:

- `backend_thread_requires_startup_gate()` now allows `B_ARCHIVER` thread
  carriers to bypass the global startup mutex, joining the already-validated
  AIO worker, syslogger, background writer, checkpointer, and WAL writer
  bypasses;
- archiver uses `AuxiliaryProcessMainCommon()`, which creates its auxiliary
  PGPROC, procsignal/barrier state, resource owner, pgstat state, and normal
  processing mode before archiver-specific work begins;
- archiver does not run database/session bootstrap or `InitPostgres()`, and
  its archiving-loop state is backend-local/thread-local while wakeup state is
  published through `PgArch` shared memory;
- the remaining gated classes stay gated: regular client backend startup,
  autovacuum launcher/workers, background workers, slot sync, startup, WAL
  receiver, and WAL summarizer.

This is not the full Gate E2 startup-gate closure. It removes serialization
from one more non-session auxiliary startup path with a small, explicit
ownership model. Further narrowing still needs worker-specific shared-state
isolation and catalog-startup stress coverage, especially for workers that run
database/session bootstrap or load arbitrary background-worker code.

Validation for this slice:

- touched-object build for `src/backend/postmaster/launch_backend.o` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- a manual threaded archiver temp-cluster smoke with `multithreaded = on`,
  `archive_mode = on`, and `dynamic_shared_memory_type = posix` verified the
  archiver appears in `pg_stat_activity`, logged `starting archiver thread
  carrier`, archived a forced WAL segment through `archive_command`, handled
  concurrent `pg_class` catalog scans, and stopped cleanly with `pg_ctl -m
  fast -w stop`;
- log inspection for that smoke found no crash, postmaster-death, shutdown
  escalation, assertion, or bad-descriptor markers;
- `git diff --check` passed.

## Threaded Startup Gate WAL Summarizer Narrowing

The ninety-seventh Phase 12 slice narrows the temporary threaded startup
serialization gate for the WAL summarizer:

- `backend_thread_requires_startup_gate()` now allows `B_WAL_SUMMARIZER`
  thread carriers to bypass the global startup mutex, joining the
  already-validated AIO worker, syslogger, archiver, background writer,
  checkpointer, and WAL writer bypasses;
- WAL summarizer uses `AuxiliaryProcessMainCommon()`, which creates its
  auxiliary PGPROC, procsignal/barrier state, resource owner, pgstat state,
  and normal processing mode before summarizer-specific work begins;
- WAL summarizer does not run database/session bootstrap or `InitPostgres()`;
- its sleep/progress state is backend-local/thread-local or published through
  `WalSummarizerCtl` shared memory, and its interrupt loop consumes the
  logical interrupt mailbox before checking shutdown/configuration state;
- the remaining gated classes stay gated: regular client backend startup,
  autovacuum launcher/workers, background workers, slot sync, startup, and WAL
  receiver.

This is not the full Gate E2 startup-gate closure. It removes serialization
from another non-session auxiliary startup path with a small, explicit
ownership model. Further narrowing still needs worker-specific shared-state
isolation and catalog-startup stress coverage, especially for workers that run
database/session bootstrap, connect to external systems, or load arbitrary
background-worker code.

Validation for this slice:

- touched-object build for `src/backend/postmaster/launch_backend.o` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- a manual threaded WAL summarizer temp-cluster smoke with
  `multithreaded = on`, `summarize_wal = on`, and
  `dynamic_shared_memory_type = posix` verified the WAL summarizer appears in
  `pg_stat_activity`, logged `starting walsummarizer thread carrier` and `WAL
  summarizer started`, generated WAL while concurrent `pg_class` catalog scans
  ran, created WAL summary files after `pg_switch_wal()` and `CHECKPOINT`, and
  stopped cleanly with `pg_ctl -m fast -w stop`;
- log inspection for that smoke found no crash, postmaster-death, shutdown
  escalation, assertion, or bad-descriptor markers;
- `git diff --check` passed.

## Threaded Startup Gate WAL Receiver Narrowing

The ninety-eighth Phase 12 slice narrows the temporary threaded startup
serialization gate for the WAL receiver:

- `backend_thread_requires_startup_gate()` now allows `B_WAL_RECEIVER` thread
  carriers to bypass the global startup mutex, joining the already-validated
  AIO worker, syslogger, archiver, WAL summarizer, background writer,
  checkpointer, and WAL writer bypasses;
- WAL receiver uses `AuxiliaryProcessMainCommon()`, which creates its
  auxiliary PGPROC, procsignal/barrier state, resource owner, pgstat state,
  and normal processing mode before receiver-specific work begins;
- WAL receiver does not run database/session bootstrap or `InitPostgres()`;
- its connection, receive-file, reply, and wakeup state is backend-local or
  published through `WalRcv` shared memory, and its streaming loop consumes
  the logical interrupt mailbox before checking shutdown/configuration state;
- the bypassed section is the common auxiliary startup section. The later
  `libpqwalreceiver` module load and physical streaming path remain outside
  the temporary startup gate and are covered by the threaded replication smoke;
- the remaining gated classes stay gated: regular client backend startup,
  autovacuum launcher/workers, background workers, slot sync, and startup.

This is not the full Gate E2 startup-gate closure. It removes serialization
from another non-session auxiliary startup path with a small, explicit
ownership model and a real threaded physical-replication validation path.
Further narrowing still needs worker-specific shared-state isolation and
catalog-startup stress coverage, especially for workers that run
database/session bootstrap or load arbitrary background-worker code.

Validation for this slice:

- touched-object build for `src/backend/postmaster/launch_backend.o` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- a manual physical-replication smoke with a process-mode primary and a
  threaded standby verified the standby WAL receiver appears in
  `pg_stat_activity`, logged `starting walreceiver thread carrier` and
  `started streaming WAL from primary`, streamed and replayed a 1000-row table
  created on the primary, handled concurrent standby `pg_class` catalog scans,
  and stopped both clusters cleanly with `pg_ctl -m fast -w stop`;
- the same smoke patched the temp-install `libpqwalreceiver.dylib` libpq
  install name before startup, matching this checkout's macOS test notes;
- log inspection for that smoke found no crash, postmaster-death, shutdown
  escalation, assertion, or bad-descriptor markers;
- `git diff --check` passed.

## Threaded Startup Gate Startup Process Narrowing

The ninety-ninth Phase 12 slice narrows the temporary threaded startup
serialization gate for the startup process:

- `backend_thread_requires_startup_gate()` now allows `B_STARTUP` thread
  carriers to bypass the global startup mutex, joining the already-validated
  AIO worker, syslogger, archiver, WAL receiver, WAL summarizer, background
  writer, checkpointer, and WAL writer bypasses;
- startup process uses `AuxiliaryProcessMainCommon()`, which creates its
  auxiliary PGPROC, procsignal/barrier state, resource owner, pgstat state,
  and normal processing mode before recovery-specific work begins;
- startup process does not run database/session bootstrap or `InitPostgres()`;
- the bypassed section is the common auxiliary startup section. Recovery
  replay and normal startup-state transitions happen after
  `ThreadedBackendStartupComplete()` and outside the temporary startup gate;
- its recovery loop consumes logical interrupts and keeps signal, promotion,
  shutdown, archive-recovery, and restore-command state backend-local or
  published through shared memory control structures;
- the remaining gated classes stay gated: regular client backend startup,
  autovacuum launcher/workers, background workers, and slot sync.

This is not the full Gate E2 startup-gate closure. It removes serialization
from another non-session auxiliary startup path with a small, explicit
ownership model and covers both clean startup and crash-recovery startup.
Further narrowing still needs worker-specific shared-state isolation and
catalog-startup stress coverage, especially for workers that run
database/session bootstrap or load arbitrary background-worker code.

Validation for this slice:

- touched-object build for `src/backend/postmaster/launch_backend.o` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- a manual threaded startup/crash-recovery smoke with `multithreaded = on`
  and `dynamic_shared_memory_type = posix` verified the startup process
  logged `starting startup thread carrier` on both initial startup and
  restart after an immediate stop, recovered a 1000-row table after crash
  recovery, and handled a post-recovery `pg_class` catalog scan;
- log inspection for that smoke found recovery markers and found no crash,
  postmaster-death, shutdown escalation, assertion, or bad-descriptor
  markers;
- `git diff --check` passed.

## Threaded Startup Gate Autovacuum Launcher Narrowing

The one-hundredth Phase 12 slice narrows the temporary threaded startup
serialization gate for the autovacuum launcher:

- `backend_thread_requires_startup_gate()` now allows
  `B_AUTOVAC_LAUNCHER` thread carriers to bypass the global startup mutex,
  joining the already-validated AIO worker, syslogger, startup process,
  archiver, WAL receiver, WAL summarizer, background writer, checkpointer,
  and WAL writer bypasses;
- this bypass is deliberately limited to the launcher. Autovacuum workers stay
  gated because they select a database, run `InitPostgres()` for that
  database, and perform catalog/table work;
- the launcher uses backend initialization to attach to shared memory and the
  autovacuum coordinator state, but it enters the no-database launcher loop
  before choosing any autovacuum target database or running user-table work;
- launcher loop state is backend-local/thread-local or protected by
  `AutovacuumLock` in `AutoVacuumShmem`, and the threaded launcher consumes
  the logical interrupt mailbox before checking shutdown/configuration state;
- the remaining gated classes stay gated: regular client backend startup,
  autovacuum workers, background workers, and slot sync.

This is not the full Gate E2 startup-gate closure. It removes serialization
from another in-tree server-owned worker family entry point while retaining
the gate for the database-connected autovacuum worker path. Further narrowing
still needs worker-specific shared-state isolation and catalog-startup stress
coverage, especially for workers that run database/session bootstrap or load
arbitrary background-worker code.

Validation for this slice:

- touched-object build for `src/backend/postmaster/launch_backend.o` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- a manual threaded autovacuum launcher temp-cluster smoke with
  `multithreaded = on`, `autovacuum = on`, `autovacuum_naptime = '1s'`, and
  `dynamic_shared_memory_type = posix` verified the autovacuum launcher
  appears in `pg_stat_activity`, logged `starting autovacuum launcher thread
  carrier` and `autovacuum launcher started`, handled eight concurrent
  `pg_class` catalog scans, ran `CREATE TABLE`/`INSERT`/`ANALYZE`, and stopped
  cleanly with `pg_ctl -m fast -w stop`;
- log inspection for that smoke found no crash, postmaster-death, shutdown
  escalation, assertion, or bad-descriptor markers;
- `git diff --check` passed.

## Threaded Startup Gate Autovacuum Worker Narrowing

The one-hundred-first Phase 12 slice narrows the temporary threaded startup
serialization gate for autovacuum workers:

- `backend_thread_requires_startup_gate()` now allows `B_AUTOVAC_WORKER`
  thread carriers to bypass the global startup mutex, joining the
  already-validated autovacuum launcher and other in-tree server-owned worker
  bypasses;
- this bypass covers the autovacuum worker's own startup path after the
  launcher has published a worker slot in `AutoVacuumShmem`;
- the worker claims its shared-memory worker entry under `AutovacuumLock`,
  clears `av_startingWorker`, wakes the launcher through the postmaster in
  threaded mode, connects to the selected database, forces worker-local GUC
  overrides after threaded `InitPostgres()`, then releases the temporary
  startup gate boundary before table work;
- worker-local vacuum state remains backend-local/thread-local, while
  shared scheduling and cost-balancing state stays protected by
  `AutovacuumLock` and `AutoVacuumShmem`;
- the remaining gated classes stay gated: regular client backend startup,
  background workers, and slot sync.

This is not the full Gate E2 startup-gate closure. It removes serialization
from the database-connected in-tree autovacuum worker path after a real
threaded autovacuum validation, but arbitrary background workers and the slot
sync worker still need their own startup ownership model and stress coverage.

Validation for this slice:

- touched-object build for `src/backend/postmaster/launch_backend.o` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8` passed;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- a manual threaded autovacuum worker temp-cluster smoke with
  `multithreaded = on`, `autovacuum = on`, `autovacuum_naptime = '1s'`,
  zero vacuum/analyze thresholds, and `dynamic_shared_memory_type = posix`
  created a table, inserted and deleted 5000 rows, waited until
  `pg_stat_user_tables.autovacuum_count` reached 1, verified the log contains
  `starting autovacuum worker thread carrier` and `automatic vacuum of table`,
  handled twelve concurrent `pg_class` catalog scans, and stopped cleanly with
  `pg_ctl -m fast -w stop`;
- log inspection for that smoke found no crash, postmaster-death, shutdown
  escalation, assertion, or bad-descriptor markers;
- `git diff --check` passed.

## Threaded Startup Gate Background Worker Bypass Probe

The one-hundred-second Phase 12 slice tested, but did not land, removing the
temporary threaded startup serialization gate for thread-compatible dynamic
background workers:

- a probe changed `backend_thread_requires_startup_gate()` to inspect the full
  `BackendThreadStart` record and allow `B_BG_WORKER` to bypass the global
  startup mutex when `BackgroundWorkerCanUseThreadCarrier()` accepted the
  copied worker definition;
- process-model background worker rejection still worked before carrier
  launch;
- a manual threaded temp-cluster smoke created
  `test_backend_runtime_threaded`, ran
  `test_backend_runtime_rejects_process_bgworker()`, and then lost the server
  connection during `test_backend_runtime_launch_thread_bgworker()`;
- the failure log reached `registering background worker
  "test_backend_runtime thread bgworker"`, `starting background worker thread
  carrier "test_backend_runtime thread bgworker"`, and `starting background
  worker thread carrier` before the disconnect;
- the behavior was reverted. All `B_BG_WORKER` startup remains behind the
  temporary startup gate, including explicitly thread-compatible dynamic
  workers.

This keeps background-worker startup as a Gate E2 blocker. The next attempt
needs a background-worker-specific shared-state fix and stress coverage for
dynamic worker start, restart, stop, and post-disconnect server usability
before `B_BG_WORKER` can leave the gate.

Validation for this probe:

- `gmake -C src/backend/postmaster launch_backend.o` passed before the failed
  smoke;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals before the failed smoke;
- `gmake -j8` and `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed before
  the failed smoke;
- `gmake -C src/test/modules/test_backend_runtime clean`, `gmake -C
  src/test/modules/test_backend_runtime all`, and `gmake -C
  src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install`
  passed before the failed smoke;
- the manual dynamic background-worker bypass smoke failed as described above,
  so the code path was reverted and documented rather than committed as a
  narrowing;
- after the revert, `gmake -C src/backend/postmaster launch_backend.o`,
  `gmake check-global-lifetimes`, and the direct `test_backend_runtime`
  `pg_regress` control passed with all `B_BG_WORKER` startup still gated.

## Threaded Startup Gate Background Worker Narrowing

The one-hundred-third Phase 12 slice fixes the failed dynamic background
worker bypass and removes thread-compatible background workers from the
temporary startup serialization gate:

- `PMChild` now has an explicit thread startup-complete flag, published by
  `ThreadedBackendStartupComplete()` and claimed by the postmaster main loop;
- thread-backed background workers are marked running in postmaster-private
  state as soon as the carrier launches, preventing duplicate launches, but
  the shared background-worker slot is not reported as started until the
  worker reaches the startup-complete boundary;
- this closes the race found in the previous probe where a dynamic waiter
  observed the worker as started, immediately called `TerminateBackgroundWorker()`,
  and delivered termination while the thread carrier was still in
  `InitProcess()`, `BaseInit()`, or background-worker function lookup;
- `backend_thread_requires_startup_gate()` now allows `B_BG_WORKER` carriers
  to bypass the global startup mutex. Process-model background workers remain
  rejected in threaded mode before carrier launch;
- the `test_backend_runtime_threaded` restart helper now accepts the
  documented `BGWH_STOPPED` transient when the first restartable worker run
  exits before the waiter observes the started state, then continues waiting
  for the restarted run.

This is not the full Gate E2 startup-gate closure. It removes serialization
from explicitly thread-compatible dynamic background workers with a concrete
postmaster startup-publication boundary, while regular client backend startup
and slot sync remained gated until the next worker-specific slot-sync
narrowing.

Validation for this slice:

- touched-object build for `src/backend/postmaster/launch_backend.o`,
  `pmchild.o`, and `postmaster.o` passed;
- full `gmake -j8` passed;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- a manual threaded dynamic background-worker smoke with `multithreaded = on`,
  `max_worker_processes = 8`, and `dynamic_shared_memory_type = posix`
  created `test_backend_runtime_threaded`, verified process-model background
  worker rejection, launched/stopped a thread-compatible dynamic worker,
  restarted a thread-compatible dynamic worker through a crash-and-restart
  cycle, and verified the server remained usable with a `pg_class` catalog
  query;
- the manual smoke log showed the expected registering, thread-carrier
  startup, restart-run, unregistering, and clean shutdown markers with no
  `FATAL`, `PANIC`, postmaster-death, or terminated-by-signal markers;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install.

## Threaded Startup Gate Slot Sync Worker Narrowing

The one-hundred-fourth Phase 12 slice removes the slot sync worker from the
temporary threaded startup serialization gate:

- `backend_thread_requires_startup_gate()` now allows `B_SLOTSYNC_WORKER`
  carriers to bypass the global startup mutex;
- the slot sync worker already reaches `ThreadedBackendStartupComplete()` only
  after `InitPostgres()` connects it to the local database and before it
  connects to the primary, giving the postmaster the same explicit startup
  publication boundary used by other thread-backed workers;
- slot sync wake/stop paths already use `SlotSyncCtx` procno/threaded state
  and latch wakeups for promotion rather than process-signal-only routing;
- this removes startup serialization from slot-sync startup while retaining the
  gate for regular client backend startup.

This is not the full Gate E2 startup-gate closure. It removes serialization
from a worker-specific path with an identified startup-complete boundary and a
real threaded physical standby validation. Regular client backend startup
remains gated until its remaining shared-state startup dependencies are
isolated and covered by concurrent catalog-startup stress.

Validation for this slice:

- touched-object build for `src/backend/postmaster/launch_backend.o` passed;
- full `gmake -j8` passed before the documentation update;
- `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed before the
  documentation update;
- a manual threaded primary/standby slot-sync smoke created a primary with
  physical and failover logical replication slots, started a physical standby
  with `multithreaded = on`, `dynamic_shared_memory_type = posix`,
  `hot_standby_feedback = on`, and `sync_replication_slots = on`, observed
  `starting slotsync worker thread carrier` and `slot sync worker started` in
  the standby log, waited until the standby reported the failover logical slot
  as synced, verified standby catalog usability with a `pg_class` query, and
  stopped both clusters cleanly;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- `git diff --check` passed.

## Threaded Startup Gate Regular Backend Removal

The one-hundred-fifth Phase 12 slice removes the remaining regular client
backend startup user from the temporary threaded startup serialization gate:

- `backend_thread_run_backend()` now consults the same
  `backend_thread_requires_startup_gate()` policy helper as worker carriers;
- `backend_thread_requires_startup_gate()` now allows `B_BACKEND`, leaving no
  backend type currently serialized by `ThreadedBackendStartupMutex`;
- a first no-gate startup stress exposed that the recursive VACUUM/ANALYZE
  guard was a function-local `static bool` in `vacuum()`, so concurrent
  sessions running `ANALYZE` could make unrelated sessions fail with
  `ANALYZE cannot be executed from VACUUM or ANALYZE`;
- that guard now lives in `PgExecutionVacuumState` behind
  `PgCurrentVacuumInProgressRef()`, matching the existing execution-local
  vacuum cost/failsafe state and preparing it for future carrier migration;
- the `test_execution_vacuum_state_is_execution_local()` regression helper now
  covers the recursive VACUUM/ANALYZE flag.

This closes the current Gate E2 startup-gate blocker. Future work should not
reintroduce broad startup serialization unless it names the shared-state
dependency that requires serialization and includes concurrent catalog-startup
stress that reaches the affected path.

Validation for this slice:

- touched-object builds for `src/backend/postmaster/launch_backend.o`,
  `src/backend/commands/vacuum.o`, and
  `src/backend/utils/init/backend_runtime.o` passed;
- `src/backend/postgres` relink passed;
- full `gmake -j8` passed before the final backend-path policy change, and
  `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed after the final
  policy change;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- the initial manual 32-connection threaded no-gate stress reproduced the
  shared `in_vacuum` leak with concurrent `ANALYZE`;
- after moving `in_vacuum` into `PgExecutionVacuumState`, the same manual
  32-connection threaded startup/catalog/temp-table/ANALYZE stress passed,
  verified catalog usability and table row count after the concurrent
  sessions, found no crash/corruption log markers, and stopped cleanly;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- `git diff --check` passed.

## PMChild Thread Detach Boundary

The one-hundred-sixth Phase 12 slice tightens the Gate E2 PMChild/teardown
boundary without claiming full memory cleanup:

- `PostmasterChildDetachThreadBackend()` now clears a thread-backed PMChild's
  live `thread_backend` pointer and `signal_pid` under the same PMChild mutex
  used for signal/wakeup routing;
- the detach helper preserves the exited logical backend id in
  `thread_exit_signal_pid`, so later exit publication and postmaster reaping
  can still report the backend that exited;
- `PostmasterChildPublishThreadExit()` now preserves an already-detached
  `thread_exit_signal_pid` instead of overwriting it with zero;
- `backend_thread_finish()` detaches the live backend pointer before final
  exit publication and retained-memory accounting;
- `test_pmchild_thread_backend_signal_api()` covers the detach-then-publish
  sequence.

This is not full Gate E2 teardown completion. An attempted implementation that
reset the exiting carrier's `TopMemoryContext` after `PgBackendExitCleanup()`
caused an abrupt postmaster exit during a parallel threaded reconnect smoke.
That failed validation is evidence that backend/session/connection/execution
memory ownership still needs systematic separation before the carrier can
reclaim its top memory tree safely. The branch therefore still reports retained
`TopMemoryContext` bytes through PMChild exit accounting.

Validation for this slice:

- touched-object builds for `src/backend/postmaster/pmchild.o`,
  `src/backend/postmaster/launch_backend.o`, and
  `src/test/modules/test_backend_runtime/test_backend_runtime.o` passed;
- `src/backend/postgres` relink passed;
- rebuilding and reinstalling `src/test/modules/test_backend_runtime` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed;
- a manual threaded reconnect smoke with `TopMemoryContext` reset enabled
  failed with an abrupt postmaster exit, so that reset path was not retained;
- the retained-memory detach path passed a manual threaded smoke with 16
  concurrent reconnecting sessions that created temp tables, ran `ANALYZE`,
  loaded `test_backend_runtime_threaded`, exercised per-session GUC values,
  terminated a sleeping backend through `pg_terminate_backend()`, verified the
  base table remained readable, and shut down cleanly with no crash or join
  failure markers.

## PMChild Publication Race Regression

The one-hundred-seventh Phase 12 slice adds focused PMChild synchronization
stress for Gate E2:

- `test_pmchild_thread_backend_publication_race()` creates a fake
  thread-backed PMChild and a fake thread-runtime backend, then starts native
  reader threads inside the regression backend;
- reader threads repeatedly call `PostmasterChildSignalPid()`,
  `PostmasterChildRaiseThreadInterrupt()`, and
  `PostmasterChildWakeThreadBackend()` while the owner thread cycles through
  live backend publication, explicit detach, exit publication, and exit report
  claiming;
- each detach-then-publish cycle verifies that the exit payload preserves the
  logical backend id and retained-memory accounting, while reader threads
  prove that the live signal path is sometimes targetable and eventually
  becomes untargetable after detach;
- the full `test_backend_runtime` regression now exercises the new race helper
  alongside the single-threaded PMChild API contract.

This is not full Gate E2 PMChild completion. It raises the floor for the local
PMChild helper API, but broader real-server stress is still required for
administrator termination, abandoned clients, worker exits, postmaster reaping,
and native thread join/retry paths.

Validation for this slice:

- `src/test/modules/test_backend_runtime/test_backend_runtime.o` and
  `test_backend_runtime.dylib` rebuilt successfully;
- reinstalling `src/test/modules/test_backend_runtime` into the temp install
  passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Threaded Backend FATAL Teardown Coverage

The one-hundred-eighth Phase 12 slice adds explicit backend `FATAL` teardown
coverage for Gate E2:

- `test_backend_runtime_threaded` now exposes
  `test_backend_runtime_emit_fatal()`, which raises backend-local `FATAL` from
  a normal SQL session;
- `t/001_threaded_runtime.pl` calls that helper through a background `psql`,
  captures the SQL-visible logical backend id first, waits for the expected
  `FATAL` message, verifies the backend leaves `pg_stat_activity`, and
  confirms the threaded server remains usable afterward.

This closes the focused FATAL-client-backend fixture gap, but it does not
close all Gate E2 teardown work. `TopMemoryContext` ownership/reclamation,
abandoned-client and termination stress at larger scale, worker-exit races,
and native thread join/retry coverage remain part of the Phase 12 gate.

Validation for this slice:

- `test_backend_runtime_threaded.o` and `test_backend_runtime_threaded.dylib`
  rebuilt successfully;
- reinstalling `src/test/modules/test_backend_runtime` into the temp install
  passed;
- direct full-module `pg_regress test_backend_runtime` passed all 1 test
  against the current temp install;
- a manual threaded cluster smoke created the extension, ran the FATAL helper
  through `psql`, observed the expected `FATAL`, verified the backend id
  disappeared from `pg_stat_activity`, verified `SELECT 42`, and found no
  crash or join-failure markers in the server log;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed;
- direct `prove t/001_threaded_runtime.pl` initially could not start because
  system Perl was missing `IPC::Run`. Installing `IPC::Run` and `IO::Tty` into
  `/Users/samwillis/perl5` made the TAP harness runnable with an explicit
  `PERL5LIB`.

## Threaded Reload Client Encoding Serialization

The one-hundred-ninth Phase 12 slice fixes a threaded GUC reload bug exposed
after making the TAP harness runnable:

- the direct threaded-runtime TAP reached its IO-worker reload smoke, then the
  postmaster wrote garbage for dynamic-default `client_encoding` into
  `global/config_exec_params`;
- the late thread-backed IO worker replayed that serialized file and died with
  `FATAL: invalid value for parameter "client_encoding"`, which shut down the
  threaded server before the rest of the TAP could run;
- `client_encoding` is now included in the required threaded session string
  GUC bootstrap list, so each thread-backed backend has initialized
  per-session string storage before nondefault replay can use it;
- `write_one_nondefault_variable()` now serializes `client_encoding` from the
  authoritative encoding state via `pg_get_client_encoding_name()` instead of
  dereferencing the generic string GUC backing pointer. This keeps
  dynamic-default reload dumps stable for late thread-backed workers.

Validation for this slice:

- `src/backend/utils/misc/guc.o` rebuilt successfully;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- reinstalling `src/test/modules/test_backend_runtime` into the temp install
  passed;
- direct threaded-runtime TAP passed all 74 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Backend Port Context Exit Cleanup

The one-hundred-thirteenth Phase 12 slice removes another concrete
connection-owned allocation group from the retained top-memory bucket during
threaded backend exit:

- `pq_init()` now allocates `Port` in a dedicated `PortContext` child of
  `TopMemoryContext`;
- backend startup allocates remote-host, remote-port, remote-hostname,
  database, user, option, and startup GUC strings in the same `PortContext`;
- `socket_close()` captures the `PortContext`, runs the existing socket path
  cleanup, clears `MyProcPort`, and deletes the context before returning from
  the backend's `on_proc_exit()` callback;
- the callback still runs after later-registered users such as
  `log_disconnections`, preserving the expected `Port` lifetime for normal
  disconnect logging while releasing the connection object before PMChild
  retained-memory accounting.

This does not solve all `TopMemoryContext` reclamation. Authentication and
HBA-adjacent data still needed a follow-up ownership pass, and the broader
threaded teardown model remained a Gate E2 blocker. It did move the core
connection object and startup packet strings out of implicit carrier lifetime
and into an explicit connection-owned cleanup path.

Validation for this slice:

- `gmake -C src/backend/libpq pqcomm.o` passed;
- `gmake -C src/backend/tcop backend_startup.o` passed;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- `gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install` passed;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Backend Authentication Connection Data Cleanup

The one-hundred-fourteenth Phase 12 slice extends `PortContext` ownership to
more authentication and HBA-adjacent connection data:

- `set_authn_id()` now stores `MyClientConnectionInfo.authn_id` in the current
  `PortContext` instead of `TopMemoryContext`, keeping the copied external
  authentication identity at connection lifetime;
- hostname verification now stores a forward-confirmed `remote_hostname` in
  `PortContext`, matching the reverse-lookup hostname path moved by the
  previous slice;
- the implicit reject `HbaLine` for unmatched pg_hba entries is allocated in
  `PortContext`, so even authentication-failure paths keep the temporary HBA
  record tied to the rejected connection rather than carrier top memory.

This still does not prove full threaded `TopMemoryContext` reclamation.
Provider-specific authentication scratch, SSL/GSS state, and other caches
needed separate lifetime decisions where they were not already
connection-owned. It did close the obvious remaining `Port`-reachable
auth/hostname allocations called out by the previous `PortContext` slice.

Validation for this slice:

- `gmake -C src/backend/libpq auth.o hba.o` passed;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- `gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install` passed;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Backend SSL/GSS Connection Identity Cleanup

The one-hundred-fifteenth Phase 12 slice extends `PortContext` ownership to
SSL/GSS connection identity state:

- GSS and SSPI authentication now allocate the `pg_gssinfo` workspace in
  `PortContext` when it is created from `auth.c`;
- GSS encrypted-transport startup now allocates its `pg_gssinfo` workspace in
  `PortContext`;
- `pg_GSS_checkauth()` now stores the display principal in `PortContext`,
  matching the authenticated identity string stored by `set_authn_id()`;
- OpenSSL peer common-name and distinguished-name strings now allocate in
  `PortContext` instead of `TopMemoryContext`.

This keeps the connection-owned SSL/GSS identity structures under the same
explicit cleanup boundary as `Port`: `socket_close()` releases external
SSL/GSS handles first, then deletes `PortContext`. It does not prove every
provider-specific authentication scratch allocation is connection-owned, and
the broader threaded teardown model remains a Gate E2 blocker.

Validation for this slice:

- `gmake -C src/backend/libpq auth.o` passed;
- static scans of `auth.c`, `be-secure-gssapi.c`, and `be-secure-openssl.c`
  found no remaining `TopMemoryContext` allocation for `pg_gssinfo`, the GSS
  principal string, or SSL peer certificate-name strings;
- full non-SSL/non-GSS `gmake -j8` passed;
- full non-SSL/non-GSS `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- `gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install` passed;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Mixed Threaded Backend Teardown Stress

The one-hundred-tenth Phase 12 slice strengthens Gate E2 real-server teardown
coverage:

- `t/001_threaded_runtime.pl` now starts a mixed batch of thread-backed
  backends that exit through backend-local `FATAL`, administrator
  `pg_terminate_backend()`, and abandoned-client disconnect while holding
  advisory locks and owning temp tables;
- the fixture captures the SQL-visible logical backend ids for the `FATAL` and
  terminated sessions, verifies all of those ids leave `pg_stat_activity`,
  verifies the abandoned-client advisory locks are released, and confirms the
  threaded server remains usable afterward;
- this runs in the same live threaded server as the worker-launch, extension,
  GUC, parallel-query, and reconnect checks, so it exercises PMChild
  publication, detach, exit reporting, reaping, and carrier reuse under a
  broader mixed teardown load.

This still does not close the full Gate E2 teardown/resource blocker.
`TopMemoryContext` ownership/reclamation and deliberately retained resource
boundaries remain unresolved, and larger/longer teardown stress is still
useful. It does raise the real-server coverage floor for the PMChild reaping
path before scheduler work.

Validation for this slice:

- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Session-Owned String GUC Bootstrap Scan

The one-hundred-eleventh Phase 12 slice replaces the post-runtime required
string-GUC whitelist with an ownership-based scan:

- `PgCurrentSessionOwnsPointer()` now exposes the runtime-level check for
  whether an arbitrary backing pointer is inside the installed `PgSession`;
- after `InstallPgThreadBackendRuntimeState()` installs the thread runtime,
  `InitializeThreadedSessionRequiredGUCOptions()` scans every built-in string
  GUC record and initializes any NULL backing storage owned by the current
  `PgSession`;
- this keeps process/runtime string globals out of the post-install bootstrap
  path while automatically covering future session-owned string GUCs, including
  the previously discovered `dynamic_library_path`, `search_path`,
  `temp_tablespaces`, and `wal_consistency_checking` cases;
- `client_encoding` remains the only explicit post-install compatibility
  exception because its authoritative state is the session encoding object
  rather than a direct `char *` field inside `PgSession`.

This does not close all Gate E2 GUC work. It does remove the growing
post-runtime required string-GUC list as a blocker and leaves the remaining GUC
work focused on broader default/reset/custom-GUC semantics and stress coverage.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/utils/misc guc.o` passed;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- `gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install` passed;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Backend Libpq Socket I/O Exit Cleanup

The one-hundred-twelfth Phase 12 slice removes two concrete connection-owned
allocations from the retained `TopMemoryContext` bucket during threaded backend
exit:

- `socket_close()` now frees the backend libpq frontend/backend `WaitEventSet`
  before closing the accepted socket and invalidating `MyProcPort->sock`;
- `socket_close()` now frees the dynamically sized backend libpq send buffer
  and clears its pointer, size, and cursor fields;
- the fixed-size receive buffer is already embedded in
  `PgConnectionSocketIOState`, so it does not require a separate release path;
- normal disconnect, abandoned-client exit, backend-local `FATAL`, and
  administrator termination all reach this same `on_proc_exit()` callback in
  the threaded TAP matrix.

This does not solve full `TopMemoryContext` reclamation. It is a scoped
resource-ownership cleanup: backend libpq socket I/O state that has clear
connection lifetime is now explicitly released before PMChild retained-memory
accounting instead of relying on carrier top-memory retention.

Validation for this slice:

- `gmake -C src/backend/libpq pqcomm.o` passed;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- `gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install` passed;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Backend Auxiliary Resource Owner Bridge

The one-hundred-sixteenth Phase 12 slice moves the auxiliary-process resource
owner pointer into explicit backend state:

- `PgBackend` now owns `aux_process_resource_owner`, keeping the pointer with
  backend-local runtime state instead of as standalone backend-local TLS;
- `AuxProcessResourceOwner` remains a source-compatible lvalue macro through
  `PgCurrentAuxProcessResourceOwnerRef()`, so existing auxiliary-process
  resource-owner lifecycle code can continue to assign and compare it without
  call-site churn;
- `backend_runtime.c` keeps a small early fallback pointer for code that runs
  before `CurrentPgBackend` is installed, then adopts that fallback into the
  process or thread backend during runtime installation;
- `CreateAuxProcessResourceOwner()` and `ReleaseAuxProcessResources()` keep
  their existing object lifecycle. This slice changes where the backend-local
  pointer is stored, not when the resource owner is created, released, or
  deleted.

This removes another raw backend-local TLS global from the Phase 12 migration
set. It does not change the broader auxiliary-worker lifecycle model or close
the full Gate E2 teardown/resource blocker.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/utils/resowner resowner.o` passed;
- full backend clean plus generated-header recovery was required after the
  installed-header change, because stale objects still referenced the old
  `_AuxProcessResourceOwner` symbol;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- `gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install` passed;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Backend PGPROC Pointer Bridge

The one-hundred-seventeenth Phase 12 slice moves the current backend's
`PGPROC` pointer into explicit backend state:

- `PgBackend` now owns `my_proc`, keeping the backend-local pointer to the
  shared-memory `PGPROC` object with the rest of backend runtime state;
- `MyProc` remains a source-compatible lvalue macro through
  `PgCurrentMyProcRef()`, so existing `MyProc = ...`, comparison, and
  dereference call sites do not need broad churn in this slice;
- `backend_runtime.c` keeps a small early fallback pointer for code that runs
  before `CurrentPgBackend` is installed, then adopts that fallback into the
  process or thread backend during runtime installation;
- `InitProcess()`, `InitAuxiliaryProcess()`, `ProcKill()`, and
  `AuxiliaryProcKill()` keep their existing `PGPROC` object lifecycle and
  shared-memory ownership rules. This slice changes where the backend-local
  pointer is stored, not when the `PGPROC` object is allocated, published, or
  released.

This removes another raw backend-local TLS global from the Phase 12 migration
set and narrows procarray/lock/signalling state's dependency on standalone TLS.
It does not move `MyProcNumber`, `ParallelLeaderProcNumber`, or the full
`PGPROC` lifecycle yet.

The focused PMChild publication race regression was also made deterministic
enough for this macOS checkout: after publishing a fake thread backend, the
writer waits briefly for a reader to observe the live backend before detaching
and publishing exit. The test still covers concurrent signal-id, interrupt,
wakeup, detach, exit-publication, and exit-claim behavior, but no longer
depends on the OS scheduling a reader inside a tiny publish/detach window.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/lmgr proc.o` passed;
- full backend clean plus generated-header recovery was required after the
  installed-header change, because stale objects still referenced the old
  `_MyProc` symbol;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change because its stale `plpgsql.dylib` referenced `_MyProc`;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change because its stale test module
  referenced `_MyProc`;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Backend ProcNumber Bridge

The one-hundred-eighteenth Phase 12 slice moves the current backend's proc
number fields into explicit backend state:

- `PgBackend` now owns `my_proc_number` and
  `parallel_leader_proc_number`, keeping the backend-local proc identity
  values with the rest of backend runtime state;
- `MyProcNumber` and `ParallelLeaderProcNumber` remain source-compatible
  lvalue names through `PgCurrentMyProcNumberRef()` and
  `PgCurrentParallelLeaderProcNumberRef()`, so existing procarray, lock, and
  parallel-worker call sites can continue assigning and reading them without a
  broad mechanical rewrite;
- process and thread backend runtime initialization explicitly sets both proc
  numbers to `INVALID_PROC_NUMBER`, so zeroed or freshly allocated backend
  objects do not accidentally masquerade as proc number 0;
- `backend_runtime.c` keeps small early fallback proc-number slots for code
  that writes before `CurrentPgBackend` is installed, then adopts those values
  into the process or thread backend during runtime installation;
- `InitProcess()`, `InitAuxiliaryProcess()`, `ProcKill()`, parallel-worker
  leader assignment, and the underlying `PGPROC`/procarray shared-memory
  lifecycle keep their existing behavior. This slice changes where the
  backend-local proc-number values are stored, not when shared proc state is
  allocated, published, or released.

This closes the immediate follow-up left by the `MyProc` pointer bridge for
backend proc identity storage. It still does not move the full `PGPROC`
lifecycle or procarray ownership model out of its current shared-memory
structures.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/lmgr proc.o` passed;
- `gmake -C src/backend/utils/init globals.o` passed;
- full backend clean plus generated-header recovery was required after the
  installed-header change, because stale objects and test modules still
  referenced the old `_MyProcNumber` and `_ParallelLeaderProcNumber` symbols
  or missed the new accessor symbols;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed.

## Backend Status Slot Pointer Bridge

The one-hundred-nineteenth Phase 12 slice moves the current backend's
backend-status slot pointer into explicit backend state:

- `PgBackend` now owns `my_beentry`, keeping the backend-local pointer to the
  shared-memory `PgBackendStatus` slot with the rest of backend runtime state;
- `MyBEEntry` remains a source-compatible lvalue name through
  `PgCurrentMyBEEntryRef()`, so activity/progress/status reporting code can
  continue assigning, testing, and dereferencing it without broad call-site
  churn;
- `backend_runtime.c` keeps a small early fallback pointer for code that runs
  before `CurrentPgBackend` is installed, then adopts that fallback into the
  process or thread backend during runtime installation;
- `pgstat_beinit()`, `pgstat_beshutdown_hook()`, and the shared
  `PgBackendStatus` array keep their existing lifecycle. This slice changes
  where the backend-local pointer is stored, not when the status slot is
  initialized, cleared, or read by monitoring code.

This removes another exported backend-local TLS global and keeps SQL-visible
backend activity identity attached to the logical backend object. It does not
change the shared-memory backend status protocol or the broader PMChild
teardown/reaping model.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/utils/activity backend_status.o backend_progress.o`
  passed;
- `gmake -C src/backend/commands analyze.o` passed;
- `gmake -C src/backend/access/heap vacuumlazy.o` passed;
- full backend clean plus generated-header recovery was required after the
  installed-header change, because stale objects could still reference the old
  `_MyBEEntry` symbol or miss the new accessor symbol;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 439 to 438;
- a static scan found no remaining raw `MyBEEntry` TLS declaration or exported
  `_MyBEEntry` symbol reference;
- `git diff --check` passed.

## Backend Background Worker Entry Bridge

The one-hundred-twentieth Phase 12 slice moves the current background worker's
registration entry pointer into explicit backend state:

- `PgBackend` now owns `my_bgworker_entry`, keeping the backend-local pointer
  to the process or thread worker's `BackgroundWorker` registration entry with
  the rest of backend runtime state;
- `MyBgworkerEntry` remains a source-compatible lvalue name through
  `PgCurrentMyBgworkerEntryRef()`, so background-worker code, worker tests,
  and logging paths can continue assigning and reading it without broad
  call-site churn;
- `backend_runtime.c` keeps a small early fallback pointer for code that runs
  before `CurrentPgBackend` is installed, then adopts that fallback into the
  process or thread backend during runtime installation;
- `StartBackgroundWorker()`, worker slots, and the shared bgworker state keep
  their existing lifecycle. This slice changes where the backend-local pointer
  is stored, not when background-worker registration or start-state publication
  happens.

This keeps worker identity attached to the logical backend object rather than
as standalone exported TLS. It does not make third-party background workers
thread-safe or change the process-mode bgworker registration contract.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/postmaster postmaster.o bgworker.o` passed;
- `gmake -C src/backend/utils/error elog.o` passed;
- `gmake -C src/backend/tcop postgres.o` passed;
- `gmake -C src/backend/access/transam parallel.o` passed;
- full backend clean plus generated-header recovery was required after the
  installed-header change, because stale objects could still reference the old
  `_MyBgworkerEntry` symbol or miss the new accessor symbol;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 438 to 437;
- a static scan found no remaining raw `MyBgworkerEntry` TLS declaration or
  exported `_MyBgworkerEntry` symbol reference;
- `git diff --check` passed.

## Backend Main-Loop Interrupt Flag Bridge

The one-hundred-twenty-first Phase 12 slice moves two generic background
main-loop interrupt flags into explicit backend pending-interrupt state:

- `PgBackendPendingInterruptState` now owns `config_reload_pending` and
  `shutdown_request_pending`, keeping cooperative config reload and shutdown
  requests with the logical backend's interrupt state;
- `ConfigReloadPending` and `ShutdownRequestPending` remain
  source-compatible lvalue names through `miscadmin.h` compatibility macros,
  so existing regular backend, auxiliary worker, replication worker, and
  contrib worker loops can continue checking and clearing them without broad
  call-site churn;
- `ProcessMainLoopInterrupts()`, `SignalHandlerForConfigReload()`,
  `SignalHandlerForShutdownRequest()`, and
  `PgCurrentBackendApplyInterrupts()` now read and write the backend-owned
  fields through those compatibility names;
- the worker-specific pending flags in `postmaster/interrupt.h`
  (`WakeupStopPending`, `AutoVacLauncherPending`, and
  `CheckpointerShutdownXLOGPending`) keep their existing standalone TLS
  storage for now.

This removes two more exported backend-local TLS symbols and keeps generic
worker shutdown/reload delivery attached to the same backend interrupt object
that already owns cancel, die, timeout, barrier, memory-context, and
idle-stats pending flags. It does not change signal delivery semantics or
make the remaining worker-specific flags scheduler-aware.

Validation for this slice:

- `gmake -C src/backend/postmaster interrupt.o` passed;
- `gmake -C src/backend/utils/init backend_runtime.o globals.o` passed;
- `gmake -C src/test/modules/test_backend_runtime clean` passed;
- `gmake -C src/test/modules/test_backend_runtime all` passed;
- full backend clean plus generated-header recovery was required after the
  installed-header change, because stale objects could still reference the old
  `_ConfigReloadPending` and `_ShutdownRequestPending` symbols;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 437 to 433;
- a static scan found no remaining raw `ConfigReloadPending` or
  `ShutdownRequestPending` TLS declarations or exported symbol references;
- `git diff --check` passed.

## Backend Worker-Specific Interrupt Flag Bridge

The one-hundred-twenty-second Phase 12 slice moves the remaining
worker-specific pending-interrupt flags from standalone backend-local TLS into
the same explicit backend pending-interrupt state:

- `PgBackendPendingInterruptState` now owns `wakeup_stop_pending`,
  `autovac_launcher_pending`, and `checkpointer_shutdown_xlog_pending`;
- `WakeupStopPending`, `AutoVacLauncherPending`, and
  `CheckpointerShutdownXLOGPending` remain source-compatible lvalue names
  through `miscadmin.h` compatibility macros;
- `PgCurrentBackendApplyInterrupts()` continues setting those flags from
  logical interrupt masks, while the archiver, autovac launcher, and
  checkpointer handlers and main loops read and clear the backend-owned fields
  through the existing names;
- the exported TLS definitions and declarations were removed from
  `postmaster/interrupt.h`, `postmaster/interrupt.c`, and
  `postmaster/checkpointer.c`.

This completes the current pending-interrupt flag bridge for the generic
main-loop, archiver, autovac launcher, and checkpointer flags. Logical
interrupt delivery semantics are unchanged, but those pending requests now
follow the logical backend object rather than the carrier thread.

Validation for this slice:

- `gmake -C src/backend/postmaster interrupt.o checkpointer.o autovacuum.o pgarch.o`
  passed;
- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/test/modules/test_backend_runtime clean` passed;
- `gmake -C src/test/modules/test_backend_runtime all` passed;
- the raw symbol scan found only the five compatibility macros in
  `src/include/miscadmin.h` and no remaining TLS declarations or exported
  symbol references for the moved flags;
- full backend clean plus generated-header recovery was required after the
  installed-header change, because stale objects could still reference the old
  exported flag symbols;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression and still reported TAP disabled by configure;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 433 to 427;
- `git diff --check` passed.

## Backend Exit In-Progress Flag Bridge

The one-hundred-twenty-third Phase 12 slice moves the exported backend exit
in-progress flags from standalone backend-local TLS into the existing
`PgBackendExitState` object:

- `PgBackendExitState` now owns `proc_exit_active` and `shmem_exit_active`;
- `proc_exit_inprogress` and `shmem_exit_inprogress` remain
  source-compatible lvalue names through `storage/ipc.h` compatibility macros;
- `PgBackendExitInProgress()` and `PgBackendShmemExitInProgress()` now read
  through those object-backed lvalues;
- `PgBackendExitCleanup()`, `shmem_exit()`, and `on_exit_reset()` set and
  clear the backend-owned fields through the existing names;
- the exported TLS definitions were removed from `storage/ipc/ipc.c`.

This is a Gate E2 lifecycle cleanup step: exit state now follows the logical
backend object that owns the callback stacks and cleanup indexes, instead of a
separate carrier-local mirror. The broader `TopMemoryContext` reclamation and
full threaded teardown model remain open, but another exit-path dependency on
raw backend-local TLS is gone.

Validation for this slice:

- `gmake -C src/backend/storage/ipc ipc.o` passed;
- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/test/modules/test_backend_runtime clean` passed;
- `gmake -C src/test/modules/test_backend_runtime all` passed after relinking
  `src/backend/postgres` so the new accessor symbol was visible to the test
  module;
- full backend clean plus generated-header recovery was required after the
  installed-header change, because stale objects could still reference the old
  exported flag symbols;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- direct threaded-runtime TAP passed all 87 tests with local
  `/Users/samwillis/perl5` `PERL5LIB` paths before and after
  `gmake -C src/test/modules/test_backend_runtime check` recreated
  `tmp_install`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new
  `test_backend_exit_state_is_backend_local()` helper, and still reported TAP
  disabled by configure;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 427 to 423;
- a static scan found only the two compatibility macros in
  `src/include/storage/ipc.h` and no remaining TLS declarations or exported
  symbol references for the moved flags;
- `git diff --check` passed.

## Backend Pgstat Pending State Bridge

The one-hundred-twenty-fourth Phase 12 slice moves a coherent pgstat pending
state family from standalone backend-local TLS into `PgBackend`:

- `PgBackendPgStatPendingState` now owns `PendingBgWriterStats`,
  `PendingCheckpointerStats`, `pgStatBlockReadTime`,
  `pgStatBlockWriteTime`, `pgStatActiveTime`, and
  `pgStatTransactionIdleTime`;
- those names remain source-compatible lvalues through `pgstat.h`
  compatibility macros and runtime accessors;
- early fallback state is adopted during process/thread runtime installation,
  matching the other backend-local compatibility bridges;
- the bgwriter, checkpointer, database pgstat, bufmgr, activity reporting, and
  timing call sites continue to use the existing names, but the storage now
  follows the logical backend object.

This is the first deliberately larger state-family batch after the narrower
exit-path bridge. It keeps related pgstat pending counters together because
they have the same owner, reset shape, and validation surface. It also removes
six more exported backend-local TLS definitions while preserving process-mode
source compatibility for in-tree callers.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/utils/activity pgstat_database.o pgstat_bgwriter.o pgstat_checkpointer.o`
  passed;
- representative dependent objects in buffer, WAL, checkpointer, ANALYZE,
  VACUUM, backend-status, and pgstat I/O code were rebuilt successfully;
- full backend clean plus generated-header recovery was required after the
  installed-header change, because stale objects could still reference the old
  exported pgstat symbols;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new
  `test_backend_pgstat_pending_state_is_backend_local()` helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 423 to 417;
- a static scan found only the six compatibility macros/accessor prototypes in
  `src/include/pgstat.h` and no remaining raw TLS declarations or exported
  symbol references for the moved pgstat pending state;
- `git diff --check` passed.

## Backend Pgstat Pending Accounting State Bridge

The one-hundred-twenty-fifth Phase 12 slice continues the larger pgstat
state-family migration by moving another coherent pending/accounting group
from standalone backend-local TLS into `PgBackendPgStatPendingState`:

- pending I/O stats: `PendingIOStats` and `have_iostats`;
- pending SLRU stats: `pending_SLRUStats` and `have_slrustats`;
- pending lock stats: `PendingLockStats` and `have_lockstats`;
- database transaction counters: `pgStatXactCommit` and
  `pgStatXactRollback`;
- function timing separation state: `total_func_time`;
- WAL previous-usage accounting state: `prevWalUsage`.

Those names remain source-compatible lvalues through `pgstat.h`
compatibility macros and runtime accessors. The new storage lives in the same
backend-owned pgstat pending bucket as the previous bgwriter/checkpointer and
database timing bridge, so the whole pending pgstat accounting family now
follows the logical backend object instead of separate carrier-local TLS.

The SLRU pending array needed its fixed element count in the runtime header.
`PGSTAT_SLRU_NUM_ELEMENTS` is now public in `pgstat.h`, with a static assert in
`pgstat_internal.h` to keep it synchronized with the internal `slru_names[]`
list.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/utils/activity clean` passed;
- `gmake -C src/backend/utils/activity pgstat_io.o pgstat_slru.o pgstat_lock.o pgstat_database.o pgstat_function.o pgstat_wal.o`
  passed;
- `gmake -C src/backend postgres` passed after the new accessor symbols were
  added;
- `gmake -C src/test/modules/test_backend_runtime clean` passed;
- `gmake -C src/test/modules/test_backend_runtime all` passed;
- full backend clean plus generated-header recovery was required after the
  installed-header and `PgBackend` layout changes;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the expanded
  `test_backend_pgstat_pending_state_is_backend_local()` helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 412 to 402;
- a static scan found only the ten compatibility macros/accessor references
  in `src/include/pgstat.h` and no remaining raw TLS declarations or exported
  symbol references for the moved pgstat pending/accounting state;
- `git diff --check` passed.

## Backend Instrumentation State Bridge

The one-hundred-twenty-sixth Phase 12 slice moves a coherent executor
instrumentation accounting family from standalone backend-local TLS into
`PgBackend`:

- `PgBackendInstrumentationState` now owns `pgBufferUsage`,
  `save_pgBufferUsage`, `pgWalUsage`, and `save_pgWalUsage`;
- those names remain source-compatible lvalues through `instrument.h`
  compatibility macros and runtime accessors;
- early fallback state is adopted during process/thread runtime installation,
  matching the other backend-local compatibility bridges;
- executor instrumentation, WAL usage accounting, `EXPLAIN`, and statistics
  call sites continue to use the existing names, but the storage now follows
  the logical backend object.

This keeps the active and saved buffer/WAL instrumentation counters together
because they are reset, saved, restored, and reported as one backend execution
accounting family. It also removes four more backend-local TLS definitions
from fixed executor instrumentation paths without changing in-tree source
syntax.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/executor instrument.o` passed;
- full backend clean plus generated-header recovery was required after the
  installed-header and `PgBackend` layout changes, because stale objects could
  still reference the old exported instrumentation symbols;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new
  `test_backend_instrumentation_state_is_backend_local()` helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 402 to 397;
- a static scan found only the four compatibility macros in
  `src/include/executor/instrument.h` and no remaining raw TLS declarations or
  exported symbol references for the moved instrumentation state;
- `git diff --check` passed.

## Backend Pgstat Backend/Fixed Flush State Bridge

The one-hundred-twenty-seventh Phase 12 slice continues the larger pgstat
state-family migration by moving backend-stat and fixed-flush state from
standalone backend-local TLS into `PgBackendPgStatPendingState`:

- backend-stat pending data: `PendingBackendStats` and
  `backend_has_iostats`;
- backend WAL previous-usage accounting: `prevBackendWalUsage`;
- fixed-stat flush and snapshot control flags: `pgstat_report_fixed`,
  `pgStatForceNextFlush`, and `force_stats_snapshot_clear`;
- assertion-only pgstat lifecycle flags: `pgstat_is_initialized` and
  `pgstat_is_shutdown`.

Those names remain source-compatible lvalues through `pgstat.h`
compatibility macros and runtime accessors. This keeps the remaining scalar
pgstat flush-control state with the same backend-owned pending-state bucket as
the bgwriter/checkpointer, I/O, SLRU, lock, transaction, timing, and WAL
pending accounting already moved in previous slices.

This slice deliberately leaves `pgStatPendingContext` and `pgStatPending` for
a follow-up batch. They are also backend-local pgstat pending state, but moving
the pending-entry memory context and list cleanly changes the pending-entry
list ownership surface and deserves a separate focused proof.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/utils/activity pgstat.o pgstat_backend.o pgstat_io.o pgstat_lock.o pgstat_slru.o pgstat_wal.o`
  passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after expanding the pgstat backend-local helper;
- full backend clean plus generated-header recovery was required after the
  installed-header and `PgBackend` layout changes, because stale objects could
  still reference the old pgstat symbols or miss the new accessor symbols;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the expanded
  `test_backend_pgstat_pending_state_is_backend_local()` helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 397 to 388;
- a static scan found only the eight compatibility macros in
  `src/include/pgstat.h` and no remaining raw TLS declarations or exported
  symbol references for the moved backend/fixed pgstat flush state;
- `git diff --check` passed.

## Backend Pgstat Pending Entry State Bridge

The one-hundred-twenty-eighth Phase 12 slice completes the current pgstat
pending-state bucket by moving the pending-entry context and pending-entry
list from standalone backend-local TLS into `PgBackendPgStatPendingState`:

- `PgBackendPgStatPendingState` now owns `pgStatPendingContext` and
  `pgStatPending`;
- `pgstat.c` keeps the existing private source names through local
  compatibility macros that call `PgCurrentPgStatPendingContextRef()` and
  `PgCurrentPgStatPendingListRef()`;
- `src/include/utils/pgstat_internal.h` exposes the accessors to backend
  runtime initialization and the focused runtime test module;
- `PgBackendInitializePgStatPendingState()` initializes the list head for each
  logical backend.

This slice is separate from the scalar pgstat flush-state bridge because
`pgStatPending` is a linked-list head, not a scalar value. The adoption path
asserts that the early pending-entry list is empty before copying early state
into a logical backend, then reinitializes the adopted backend's list head.
Copying a non-empty `dlist_head` would be incorrect because list nodes would
still point at the old list head. The zero-initialized early static state is
valid because PostgreSQL's `dlist_is_empty()` treats a NULL first pointer as
empty.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/utils/activity pgstat.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after expanding the pgstat backend-local helper to check the pending
  memory context and list-head address across backend switches;
- full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the expanded
  `test_backend_pgstat_pending_state_is_backend_local()` helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 388 to 386;
- a static scan found only the two private compatibility macros in
  `src/backend/utils/activity/pgstat.c` and no remaining raw TLS declarations
  for the moved pending-entry context/list state;
- `git diff --check` passed.

## Backend Storage Pending/SMgr State Bridge

The one-hundred-twenty-ninth Phase 12 slice moves a coherent storage-owned
backend-local state group from standalone backend-local TLS into
`PgBackendStorageState`:

- pending file-sync/unlink state from `sync.c`: `pendingOps`,
  `pendingUnlinks`, `pendingOpsCxt`, `sync_cycle_ctr`,
  `checkpoint_cycle_ctr`, and `sync_in_progress`;
- storage-manager relation state from `smgr.c`: `SMgrRelationHash` and
  `unpinned_relns`;
- magnetic-disk storage-manager allocation context from `md.c`: `MdCxt`.

The existing source names remain local compatibility macros in their owning
storage files, routed through `PgCurrentSync*Ref()`,
`PgCurrentSMgr*Ref()`, and `PgCurrentMdContextRef()` accessors. The new state
bucket is initialized for every thread-backed backend and adopted by the
process-mode backend during `InitializePgProcessRuntime()`.

This slice keeps related storage lifetime state together because pending fsync
bookkeeping, smgr relation entries, and md descriptor allocations all belong
to one logical backend's storage-manager view. The smgr unpinned relation list
has the same copied-list-head hazard as other moved `dlist_head` values:
copying a non-empty list head would leave node links pointing at the old list
head. The process-mode adoption path therefore asserts that no early smgr
relation hash/list exists before backend-runtime adoption and initializes the
logical backend's smgr list head.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/sync sync.o` passed;
- `gmake -C src/backend/storage/smgr smgr.o md.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after adding `test_backend_storage_state_is_backend_local()`;
- full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- full `gmake -j8` passed;
- full `gmake -j8 DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new
  `test_backend_storage_state_is_backend_local()` helper, and still reported
  TAP disabled by configure;
- direct threaded-runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 386 to 379;
- a static scan found only the nine private compatibility macros in
  `src/backend/storage/sync/sync.c`, `src/backend/storage/smgr/smgr.c`, and
  `src/backend/storage/smgr/md.c` and no remaining raw TLS declarations for
  the moved storage state;
- `git diff --check` passed.

## Backend File Descriptor/VFD State Bridge

The one-hundred-thirtieth Phase 12 slice extends the backend storage-state
bucket to cover file descriptor and virtual-file-descriptor bookkeeping from
`fd.c`:

- virtual descriptor cache state: `VfdCache` and `SizeVfdCache`;
- currently open VFD file count: `nfile`;
- assert-only temporary-file access state: `temporary_files_allowed`;
- transient allocated descriptor state: `numAllocatedDescs`,
  `maxAllocatedDescs`, and `allocatedDescs`;
- externally reserved descriptor count: `numExternalFDs`.

The existing `fd.c` source names remain private compatibility macros backed
by `PgCurrentVfdCacheRef()`, `PgCurrentSizeVfdCacheRef()`,
`PgCurrentNFileRef()`, `PgCurrentTemporaryFilesAllowedRef()`,
`PgCurrentNumAllocatedDescsRef()`, `PgCurrentMaxAllocatedDescsRef()`,
`PgCurrentAllocatedDescsRef()`, and `PgCurrentNumExternalFDsRef()`.
`PgBackendStorageState` stores the private `fd.c` pointer-typed arrays as
opaque `void *` fields so `Vfd` and `AllocateDesc` stay local to `fd.c`.

This slice also fixes a thread-runtime adoption gap exposed by moving
descriptor counts into `PgBackendStorageState`. Thread startup performs latch
and wait-set initialization before `InstallPgThreadBackendRuntimeState()`, and
those paths can reserve file descriptors through the early backend fallback
state. The install path now calls `PgBackendAdoptEarlyStorageState()` so that
early storage fallback state is copied into the thread-backed `PgBackend`
before SQL or worker code runs. Without that adoption, the threaded TAP server
could reach "ready to accept connections" and then exit immediately after
launching the logical replication launcher thread.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/file fd.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after expanding `test_backend_storage_state_is_backend_local()` to
  cover the fd/VFD fields;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- full `gmake -j8` passed;
- full `gmake DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the expanded
  `test_backend_storage_state_is_backend_local()` helper, and still reported
  TAP disabled by configure;
- the direct threaded-runtime TAP initially exposed the missing early storage
  adoption path by exiting before assertions; after adding
  `PgBackendAdoptEarlyStorageState()` to
  `InstallPgThreadBackendRuntimeState()`, direct threaded-runtime TAP passed
  all 87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and
  an explicit `PG_REGRESS` environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 379 to 371;
- a static scan found only the eight private compatibility macros in
  `src/backend/storage/file/fd.c` and no remaining raw TLS declarations for
  the moved fd/VFD state;
- `git diff --check` passed.

## Backend Deadlock Detector State Bridge

The one-hundred-thirty-first Phase 12 slice moves the deadlock detector's
backend-local workspace from standalone TLS into a new `PgBackendLockState`
bucket:

- cycle detection workspace: `visitedProcs` and `nVisitedProcs`;
- topo-sort workspace: `topoProcs`, `beforeConstraints`, and
  `afterConstraints`;
- wait-order expansion workspace: `waitOrders`, `nWaitOrders`, and
  `waitOrderProcs`;
- active and possible constraint arrays: `curConstraints`,
  `nCurConstraints`, `maxCurConstraints`, `possibleConstraints`,
  `nPossibleConstraints`, and `maxPossibleConstraints`;
- deadlock detail output state: `deadlockDetails` and `nDeadlockDetails`;
- autovacuum deadlock cancellation target: `blocking_autovacuum_proc`.

`deadlock.c` keeps its historical local source names through private
compatibility macros backed by `PgCurrentDeadlock*Ref()` and
`PgCurrentBlockingAutovacuumProcRef()`. `PgBackendLockState` stores the
private `EDGE`, `WAIT_ORDER`, `DEADLOCK_INFO`, and `PGPROC` pointer-typed
fields as opaque `void *` values, so the deadlock detector's private types
stay local to `deadlock.c`.

The lock-state bucket is initialized for every process and thread backend.
Like the other backend-owned buckets, it also has early fallback storage that
is adopted during process and thread runtime installation. The current early
fallback is expected to be zero for normal deadlock checking, but adopting it
keeps the runtime bridge consistent with the rest of Phase 12 and avoids
future pre-runtime writes being silently discarded.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/lmgr deadlock.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after adding `test_backend_lock_state_is_backend_local()`;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- full `gmake -j8` passed;
- full `gmake DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new
  `test_backend_lock_state_is_backend_local()` helper, and still reported TAP
  disabled by configure;
- direct threaded-runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 371 to 355;
- a static scan found only the seventeen private compatibility macros in
  `src/backend/storage/lmgr/deadlock.c` and no remaining raw TLS declarations
  for the moved deadlock detector state;
- `git diff --check` passed.

## Backend Local Buffer State Bridge

The one-hundred-thirty-second Phase 12 slice moves local-buffer backend state
from standalone TLS and function-local static storage into
`PgBackendBufferState`:

- exported local-buffer state: `NLocBuffer`, `LocalBufferDescriptors`,
  `LocalBufferBlockPointers`, and `LocalRefCount`;
- private `localbuf.c` state: `nextFreeLocalBufId`, `LocalBufHash`, and
  `NLocalPinnedBuffers`;
- `GetLocalBufferStorage()` allocation cursor/context state:
  `cur_block`, `next_buf_in_block`, `num_bufs_in_block`,
  `total_bufs_allocated`, and `LocalBufferContext`.

The exported source names remain compatibility macros in `storage/bufmgr.h`
and `storage/buf_internals.h`, while the private `localbuf.c` names remain
private compatibility macros backed by `PgCurrent*Ref()` accessors. Pointer
fields whose concrete types are private to buffer internals are stored as
opaque `void *` fields where needed.

Including the `GetLocalBufferStorage()` cursor matters for threaded mode:
those variables were function-local statics, not annotated TLS globals, so a
raw file-scope global scan alone would not make the hazard obvious. Without
this move, two thread-backed logical backends could share the same local
buffer allocation cursor/context.

The next buffer-manager slice moves `BackendWritebackContext` and the shared
buffer private-refcount state into the same `PgBackendBufferState` bucket.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/buffer localbuf.o bufmgr.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after adding `test_backend_buffer_state_is_backend_local()`;
- a raw scan found only the intended twelve compatibility macros in
  `src/include/storage/bufmgr.h`, `src/include/storage/buf_internals.h`, and
  `src/backend/storage/buffer/localbuf.c`;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- full `gmake -j8` passed;
- full `gmake DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new
  `test_backend_buffer_state_is_backend_local()` helper, and still reported
  TAP disabled by configure;
- direct threaded-runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 355 to 345;
- `git diff --check` passed.

## Backend Shared Buffer Pin State Bridge

The one-hundred-thirty-third Phase 12 slice extends `PgBackendBufferState` to
cover shared-buffer backend-local pin/writeback bookkeeping:

- `BackendWritebackContext`, used by shared-buffer writeback coalescing;
- `PinCountWaitBuf`, the `LockBufferForCleanup()` wait target;
- private refcount array and overflow hash state:
  `PrivateRefCountArrayKeys`, `PrivateRefCountArray`,
  `PrivateRefCountHash`, `PrivateRefCountOverflowed`,
  `PrivateRefCountClock`, `ReservedRefCountSlot`, and
  `PrivateRefCountEntryLast`;
- `MaxProportionalPins`, the backend's advisory shared-buffer pin budget.

The compatibility surface keeps `BackendWritebackContext` object-like because
existing buffer manager code takes its address. `PgCurrentBackendWritebackContextRef()`
therefore returns the per-backend `WritebackContext` storage and
`storage/buf_internals.h` exposes `BackendWritebackContext` as `*ref`. The
concrete `WritebackContext` and `PrivateRefCountEntry` types remain in the
buffer-manager internal header; the public runtime object stores pointers and
does not expose the buffer-manager internals to unrelated backend headers.

`InitBufferManagerAccess()` now allocates and resets the private refcount
arrays through `PgBackendBufferState`, recreates the overflow hash for the
current backend, and initializes the per-backend writeback context through the
runtime bridge. This keeps the hot refcount state backend-owned without
changing the existing buffer pin call sites.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/buffer bufmgr.o buf_init.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after expanding `test_backend_buffer_state_is_backend_local()`;
- a static scan found no remaining raw TLS declarations for
  `BackendWritebackContext`, `PinCountWaitBuf`, private refcount state, or
  `MaxProportionalPins`;
- a stale-doc scan found no remaining notes treating `BackendWritebackContext`
  as a deferred buffer-manager slice;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- full `gmake -j8` passed;
- full `gmake DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  change;
- `src/test/modules/test_backend_runtime` was cleaned, rebuilt, and
  reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the expanded
  `test_backend_buffer_state_is_backend_local()` helper, and still reported
  TAP disabled by configure;
- direct threaded-runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 345 to 334;
- `git diff --check` passed.

## Backend IPC And Sinval State Bridge

The one-hundred-thirty-fourth Phase 12 slice moves a backend IPC/cache
invalidation state group into a new `PgBackendIPCState` bucket:

- `MyProcSignalSlot`, the current backend's proc-signal slot pointer;
- `SharedInvalidMessageCounter`, the SQL/cache invalidation progress counter;
- `catchupInterruptPending`, the backend-local catchup interrupt flag;
- the recursive `ReceiveSharedInvalidMessages()` message buffer and its
  `nextmsg`/`nummsgs` cursor state.

`procsignal.c` keeps the private `ProcSignalSlot` type local by exposing
`MyProcSignalSlot` as a file-local compatibility macro backed by
`PgCurrentProcSignalSlotRef()`. `sinval.h` keeps the exported
`SharedInvalidMessageCounter` and `catchupInterruptPending` source names as
macros backed by `PgBackendIPCState`, preserving existing call sites in
namespace/object-address invalidation checks and tcop interrupt handling.

The recursive sinval receive buffer is intentionally part of this batch even
though it was function-local static TLS rather than an exported global. In a
threaded runtime with carrier migration, recursion cursor state and already
fetched invalidation messages must travel with the logical backend, not with a
physical carrier thread.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/ipc procsignal.o sinval.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after adding `test_backend_ipc_state_is_backend_local()`;
- a static scan found no remaining raw TLS declarations for
  `MyProcSignalSlot`, `SharedInvalidMessageCounter`,
  `catchupInterruptPending`, or the moved recursive sinval receive buffer and
  cursor state;
- because `backend_runtime.h`, `sinval.h`, and the `PgBackend` layout changed,
  `gmake -C src/backend clean` plus generated utility and node-header recovery
  was used before the clean rebuild;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed, followed by rebuilding
  and reinstalling PL/pgSQL and `src/test/modules/test_backend_runtime`;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new
  `test_backend_ipc_state_is_backend_local()` helper, and still reported TAP
  disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations now at 330;
- `git diff --check` passed.

## Backend Lock Manager Local State Bridge

The one-hundred-thirty-fifth Phase 12 slice extends `PgBackendLockState` to
cover the remaining backend-local lock-manager and lock-wait state outside the
deadlock detector:

- fast-path lock-group use counters, replacing the former fixed TLS array
  with backend-owned storage allocated and reset by `InitLockManagerAccess()`;
- relation-extension lock ownership state;
- the local lock hash table pointer;
- strong-lock acquisition progress state;
- awaited lock and awaited resource owner state used by deadlock timeout
  processing;
- the deadlock-timeout pending flag;
- condition-variable sleep target state;
- speculative insertion token state.

The public runtime object keeps private lock-manager implementation types
opaque where needed, and the touched lock-manager source files keep their
historical local names as compatibility macros backed by
`PgBackendLockState`. This removes another fixed backend-local state group
from standalone TLS while preserving the current lock-manager call-site
shape.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/lmgr lock.o proc.o condition_variable.o lmgr.o`
  passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after expanding `test_backend_lock_state_is_backend_local()`;
- a static scan found no remaining raw TLS declarations for the moved lock
  manager, condition-variable, deadlock-timeout, or speculative-insertion
  state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the expanded
  `test_backend_lock_state_is_backend_local()` helper, and still reported TAP
  disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 330 to 321.

## Backend Transaction State Bridge

The one-hundred-thirty-sixth Phase 12 slice moves a transaction/access-manager
backend-local state group into a new `PgBackendTransactionState` bucket:

- transaction-status single-entry cache state: cached XID, cached status, and
  cached commit LSN;
- two-phase backend state: the locked `GlobalTransaction` pointer and
  two-phase exit-callback registration flag;
- the private `TwoPhaseGetGXact()` lookup cache, which was a function-local
  static rather than an annotated TLS global;
- SLRU error-report state used after physical page I/O failures;
- multixact member cache state: cache list head, initialization flag, cache
  memory context, and the private debug string buffer used by
  `mxid_to_string()`.

The runtime state keeps private two-phase implementation pointers opaque and
keeps SLRU's private error enum local to `slru.c`. The multixact cache list
head is explicitly initialized in the backend transaction state initializer.
Early fallback adoption asserts that an initialized early multixact list is
empty before copying, because non-empty copied list heads would leave list
nodes linked to the old head address.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/access/transam transam.o twophase.o slru.o multixact.o`
  passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after adding `test_backend_transaction_state_is_backend_local()`;
- a static scan found no remaining raw TLS declarations or function-local
  statics for the moved transaction-status, two-phase, SLRU, or multixact
  state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new
  `test_backend_transaction_state_is_backend_local()` helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 321 to 312.

## Backend ProcArray Visibility State Bridge

The one-hundred-thirty-seventh Phase 12 slice extends
`PgBackendTransactionState` to cover ProcArray backend-local visibility and
XID-cache state:

- the `TransactionIdIsInProgress()` negative-result cache;
- the four `GlobalVisState` horizon caches for shared, catalog, regular data,
  and temporary relations;
- the `ComputeXidHorizonsResultLastXmin` recomputation throttle;
- the optional `XIDCACHE_DEBUG` counters.

The `GlobalVisState` storage definition moved from private `procarray.c` scope
to the runtime header so the backend runtime can own the state by value while
the existing `snapmgr.h` and `heapam.h` forward declarations remain valid.
`procarray.c` keeps the existing local names as compatibility macros backed by
the current logical backend's transaction-state bucket.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/ipc procarray.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after expanding `test_backend_transaction_state_is_backend_local()`;
- a static scan found no remaining raw TLS declarations for the moved
  ProcArray visibility/cache state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the expanded
  `test_backend_transaction_state_is_backend_local()` helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 312 to 297.

## Backend Activity And PgStat Shared-Ref State Bridge

The one-hundred-thirty-eighth Phase 12 slice moves a backend activity and
pgstat shared-entry reference-cache state group into runtime-owned backend
state:

- the backend-status local snapshot table pointer;
- the local backend snapshot count;
- the backend-status snapshot memory context;
- the pgstat shared-entry reference hash pointer;
- the pgstat shared-reference age counter;
- the pgstat shared-reference memory context;
- the pgstat entry-reference hash memory context.

The backend-status snapshot fields now live in a dedicated
`PgBackendActivityState` bucket. The pgstat shared-entry reference-cache
fields now live in `PgBackendPgStatPendingState` behind private pgstat
accessors and `pgstat_shmem.c` compatibility macros. The simplehash table type
stays private to `pgstat_shmem.c`; the runtime bucket stores it as an opaque
pointer. `pgStatLocal` deliberately remains standalone backend-local TLS for a
later dedicated pgstat-local slice because its type currently depends on
internal pgstat snapshot structures in `pgstat_internal.h`.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/utils/activity backend_status.o pgstat_shmem.o pgstat.o`
  passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after expanding the pgstat pending-state helper and adding
  `test_backend_activity_state_is_backend_local()`;
- a static scan found no remaining raw TLS declarations for the moved
  backend-status snapshot state or pgstat shared-entry reference-cache state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new
  `test_backend_activity_state_is_backend_local()` helper, and still reported
  TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 297 to 291.

## Backend LWLock State Bridge

The one-hundred-thirty-ninth Phase 12 slice extends `PgBackendLockState` to
cover the always-built LWLock backend-local state:

- the count of currently held LWLocks;
- the fixed held-LWLock handle array used for error-recovery release;
- the backend-local copy of the number of user-defined LWLock tranches.

`PgBackendLockState` owns the held-lock array by value so error-recovery
release keeps the same fixed-storage behavior. `lwlock.c` keeps its existing
local names as compatibility macros backed by runtime accessors. The optional
`LWLOCK_STATS` debug-only state remains a follow-up because its dummy stats
entry uses a private debug struct and that code is not built in this checkout.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/storage/lmgr lwlock.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after expanding `test_backend_lock_state_is_backend_local()`;
- a static scan found no remaining raw TLS declarations for the moved
  always-built LWLock state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the expanded
  `test_backend_lock_state_is_backend_local()` helper, and still reported TAP
  disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 291 to 288.

## Backend Utility State Bridge

The one-hundred-fortieth Phase 12 slice moves a backend utility/support state
group from standalone backend-local TLS into a new `PgBackendUtilityState`
bucket:

- dynahash active sequential-scan tracking: the active table array, nesting
  level array, and active scan count used to guard unsafe hash-table
  destruction during `hash_seq_search()`;
- the superuser one-entry cache: last role OID, cached superuser result, and
  syscache callback registration flag;
- the resource-owner release callback list pointer;
- optional `RESOWNER_STATS` lookup counters.

`dynahash.c`, `superuser.c`, and `resowner.c` keep their existing local source
names through runtime-backed compatibility macros. `ResourceReleaseCallbackItem`
remains private to `resowner.c`; the runtime stores the callback head as an
opaque pointer and `resowner.c` casts it through a file-local typed helper.
The early fallback state is adopted during process/thread runtime
installation so callback registration before runtime installation remains
attached to the logical backend.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/utils/hash dynahash.o` passed;
- `gmake -C src/backend/utils/misc superuser.o` passed;
- `gmake -C src/backend/utils/resowner resowner.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after adding `test_backend_utility_state_is_backend_local()`;
- a static scan found no remaining raw TLS declarations for the moved
  dynahash, superuser, resource-owner callback, or optional resource-owner
  stats state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 288 to 280;
- `gmake -C contrib -j8` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new utility-state helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment.

## Backend IPC DSM And Latch State Bridge

The one-hundred-forty-third Phase 12 slice extends `PgBackendIPCState` to cover
DSM initialization and local latch state:

- `dsm_init_done`;
- the DSM registry's `dsm_registry_dsa` and `dsm_registry_table`;
- the process-local `LatchWaitSet` used by `WaitLatch()`;
- `LocalLatchData`.

The owning source files keep their existing names through compatibility macros,
with `dsm_registry.c` preserving its private typed pointers by casting the
runtime-owned opaque fields locally.

This slice also records an early-runtime adoption invariant. Threaded backend
startup calls `InitProcessLocalLatch()` and `InitializeLatchWaitSet()` before
`InstallPgThreadBackendRuntimeState()`, so the early fallback `MyLatch` pointer
can be copied into `backend->core.latch` before `PgBackendIPCState` is adopted.
`PgBackendAdoptEarlyIPCState()` now retargets adopted `backend->core.latch` and
`backend->interrupt_latch` pointers from the early fallback latch to the
backend-owned `backend->ipc.local_latch_data` before clearing the fallback. A
direct threaded-runtime TAP run caught the missing retargeting as
`cannot wait on a latch owned by another process` during startup.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `dsm.o`,
  `dsm_registry.o`, `latch.o`, `miscinit.o`, and
  `test_backend_runtime.o`;
- a static scan found no remaining raw TLS declarations for the moved DSM,
  DSM-registry, latch wait-set, or local-latch state;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 249 to 244;
- `gmake -C contrib -j8` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header and `PgBackend` layout changes;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the expanded IPC-state helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment.

## Backend Timeout State Bridge

The one-hundred-forty-fourth Phase 12 slice moves timeout scheduler state into
a new `PgBackendTimeoutState` bucket:

- the registered timeout parameter table;
- the active-timeout queue;
- alarm enabled and signal-pending state;
- the pending signal due timestamp;
- firing-target backend/execution pointers;
- signal-backed vs logical timeout delivery mode.

`timeout.c` keeps the existing scheduler behavior through compatibility macros
over the current backend timeout bucket. `PgTimeoutParams` is now defined in
`utils/timeout.h` so `PgBackend` can own the fixed timeout arrays directly
without allocating a separate private timeout object.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `timeout.o`, and
  `test_backend_runtime.o`;
- a static scan found no remaining raw TLS declarations for the moved timeout
  scheduler state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 244 to 236;
- `gmake -C contrib -j8` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header and `PgBackend` layout changes;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new timeout-state helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment.

## Backend WAL Sender State Bridge

The one-hundred-forty-fifth Phase 12 slice moves a coherent WAL sender
backend-local state group into a new `PgBackendWalSenderState` bucket:

- exported WAL sender identity and wakeup flags: `MyWalSnd`,
  `am_walsender`, `am_cascading_walsender`, `am_db_walsender`, and
  `wake_wal_senders`;
- physical/logical streaming cursor state, including the replication
  `XLogReaderState`, timeline fields, local sent pointer, reply-processing
  timestamps, streaming-done flags, caught-up flag, and shutdown flags;
- replication command scratch state: uploaded incremental-backup manifest,
  replication command memory context, output/reply buffers, logical decoding
  context, and lag tracker.

The public WAL sender headers keep the old variable names as compatibility
macros over `PgCurrentWalSenderState()`. `walsender.c` keeps private state
access local to the source file through equivalent macros. The local WAL
sender sent pointer is intentionally named `local_sent_ptr` in `walsender.c`
so the compatibility macro cannot collide with the shared-memory
`WalSnd.sentPtr` field.

Validation for this slice:

- focused object builds passed for `backend_runtime.o`, `walsender.o`,
  `syncrep.o`, and `test_backend_runtime.o`;
- a static scan found no remaining raw TLS declarations for the moved WAL
  sender state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 236 to 202;
- `gmake -C contrib -j8` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header and `PgBackend` layout changes;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new WAL sender state helper, and
  still reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment.

## Backend Replication Receiver And Slot State Bridge

The one-hundred-forty-sixth Phase 12 slice moves a coherent replication
receiver and slot backend-local state group into a new
`PgBackendReplicationState` bucket:

- `MyReplicationSlot`;
- synchronous replication wait mode;
- WAL receiver connection, receive-file, timeline, segment, logstream,
  wakeup, reply-message, and primary-standby-xmin state.

The public replication slot header keeps `MyReplicationSlot` as a compatibility
macro over `PgCurrentReplicationState()`. `syncrep.c` and `walreceiver.c` keep
their existing local names through source-file-local macros over the same
backend-owned state. The runtime initializer preserves the non-zero sentinels
from the former standalone state: sync-rep wait mode starts at
`SYNC_REP_NO_WAIT`, WAL receiver receive file starts at `-1`, and
`primary_has_standby_xmin` starts true.

Validation for this slice:

- focused object builds passed for `backend_runtime.o`, `walreceiver.o`,
  `syncrep.o`, `slot.o`, and `test_backend_runtime.o`;
- a static scan found no remaining raw TLS declarations for the moved
  replication slot, sync-rep, or WAL receiver state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 202 to 193;
- `gmake -C contrib -j8` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header and `PgBackend` layout changes;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new replication-state helper, and
  still reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment.

## Backend Logical Replication Worker State Bridge

The one-hundred-forty-seventh Phase 12 slice moves a broad logical
replication worker state group into a new
`PgBackendLogicalReplicationState` bucket:

- exported apply-worker state: `ApplyContext`, `MyParallelShared`,
  `ParallelApplyMessagePending`, `LogRepWorkerWalRcvConn`, `MySubscription`,
  `MyLogicalRepWorker`, `in_remote_transaction`,
  `InitializingApplyWorker`, and `table_states_not_ready`;
- apply-worker private backend state for subscription validity,
  on-commit wakeups, remote/stream transaction tracking, skip LSN, stream
  file, and last flush position;
- logical replication launcher DSA/dshash attachment state and on-commit
  launcher wakeup flag;
- parallel apply transaction hash, worker pool, current stream worker, and
  subtransaction list pointers;
- table-sync copy buffer and sequence-sync sequence-info list;
- logical decoding `XLogLogicalInfo` cache and delayed-update flag;
- slot-sync shutdown flag and observed slot-sync configuration state.

The public logical replication worker headers keep the old variable names as
compatibility macros over `PgCurrentLogicalReplicationState()`. Source-file
private state keeps local compatibility macros in its owning file. The runtime
initializer preserves the former non-zero sentinels for invalid remote final
LSN, streamed transaction XID, skip LSN, and last flush position. The deeper
logical replication internals that still expose private layout
(`lsn_mapping`, `apply_error_callback_arg`, and `subxact_data`) remain
standalone backend-local TLS for a later focused slice. Slot-sync `sleep_ms`
also remains standalone for now because it has a private non-zero default tied
to slot-sync scheduling constants. That remaining state is moved by the
one-hundred-fifty-fifth Phase 12 completion slice below.

Validation for this slice:

- focused object builds passed for `backend_runtime.o`, logical replication
  `worker.o`, `launcher.o`, `applyparallelworker.o`, `tablesync.o`,
  `sequencesync.o`, `logicalctl.o`, `slotsync.o`, and
  `test_backend_runtime.o`;
- a static scan found no remaining raw TLS declarations for the moved logical
  replication worker, launcher, parallel-apply, table-sync, sequence-sync,
  logical-info, or slot-sync state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 193 to 148;
- `gmake -C contrib -j8` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header and `PgBackend` layout changes;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new logical replication state helper,
  and still reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment.

## Backend WAL/XLog State Bridge

The one-hundred-forty-eighth Phase 12 slice moves the main backend-local
WAL/XLog cache group into a new `PgBackendXLogState` bucket:

- local recovery and XLog-insert permission flags;
- exported transaction WAL pointers `ProcLastRecPtr`, `XactLastRecEnd`, and
  `XactLastCommitEnd`;
- the backend-local redo pointer, full-page-write cache, and private
  `LogwrtResult` copy;
- open WAL segment file/segment/timeline tracking;
- local min-recovery-point copies and update flag;
- local checksum state;
- WAL insertion-lock bookkeeping;
- WAL debug memory context.

The public transaction WAL pointers remain source-compatible macros in
`xlog.h` over `PgCurrentXLogState()`. Most private `xlog.c` names also use
local compatibility macros. The local redo pointer deliberately uses the
non-conflicting `XLogLocalRedoRecPtr` name instead of a `RedoRecPtr` macro,
because shared WAL structs also contain fields named `RedoRecPtr`, and an
object-like macro would corrupt `Insert->RedoRecPtr` and
`XLogCtl->RedoRecPtr` member references.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `xlog.o`, and
  `test_backend_runtime.o`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 148 to 128;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake -C contrib -j8` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header and `PgBackend` layout changes;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new XLog-state helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment.

## Backend Recovery/Startup/Standby State Bridge

The one-hundred-forty-ninth Phase 12 slice moves a recovery-owned
backend-local state group into a new `PgBackendRecoveryState` bucket:

- startup-process SIGHUP, shutdown, promote, and restore-command flags;
- startup-progress phase timestamp and progress-timeout flag;
- local hot-standby-active and promote-triggered caches;
- recovery conflict lock hash table pointers;
- standby deadlock, delay, and lock timeout flags;
- standby conflict wait backoff state.

`startup.c`, `standby.c`, and `xlogrecovery.c` keep their local names as
compatibility macros over `PgCurrentRecoveryState()`. The standby conflict
wait default is shared as `PG_BACKEND_STANDBY_INITIAL_WAIT_US`, so the early
fallback backend and initialized thread backends start with the same backoff
state.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `startup.o`,
  `standby.o`, `xlogrecovery.o`, and `test_backend_runtime.o`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 128 to 115;
- `src/test/modules/test_backend_runtime` now includes
  `test_backend_recovery_state_is_backend_local()`, covering startup flags,
  local recovery caches, recovery lock hash pointers, standby timeout flags,
  and standby wait backoff isolation across two fake logical backends.
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake -C contrib -j8` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header and `PgBackend` layout changes;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new recovery-state helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment.

## Backend Maintenance Worker State Bridge

The one-hundred-fiftieth Phase 12 slice moves a broad server-owned
maintenance-worker state group into a new `PgBackendMaintenanceWorkerState`
bucket:

- archiver module errdetail scratch, restart timing, callback pointers, module
  state, archive memory context, loaded library name, queued archive-status
  files, and stop flag;
- checkpointer active-checkpoint timing/progress fields and checkpoint/archive
  timeout timestamps;
- bgwriter standby-snapshot timestamp and LSN cache;
- WAL summarizer sleep/backoff state and summary-removal redo pointer cache;
- data-checksum worker abort flag, launcher-running flag, and current
  operation.

The public archive-module errdetail variable remains source-compatible through
`arch_module_check_errdetail_string`, now implemented as a macro over
`PgCurrentArchModuleCheckErrdetailStringRef()`. `pgarch.c` uses `PgArchFiles`
for the archiver queue pointer to avoid an object-like macro colliding with
the `arch_files[]` member inside the private queue struct. The data-checksum
worker uses explicit `DataChecksums*` local-state names rather than an
`operation` macro, because `operation` is also a shared-memory struct member.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `bgwriter.o`,
  `checkpointer.o`, `pgarch.o`, `walsummarizer.o`,
  `datachecksum_state.o`, `shell_archive.o`, `basic_archive.o`, and
  `test_backend_runtime.o`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 115 to 93;
- a backend clean, generated-header recovery, full `gmake -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake -C contrib -j8` and a clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `src/test/modules/test_backend_runtime` now includes
  `test_backend_maintenance_worker_state_is_backend_local()`, covering
  archiver, checkpointer, bgwriter, WAL summarizer, and data-checksum worker
  fields across two fake logical backends.

## Backend Autovacuum State Bridge

The one-hundred-fifty-first Phase 12 slice moves autovacuum launcher and
worker backend-local state into a dedicated `PgBackendAutovacuumState` bucket:

- worker-local cost storage-parameter overrides;
- launcher `SIGUSR2` wake flag;
- recent XID and multixact comparison points;
- per-database default freeze-age settings used by workers;
- autovacuum long-lived memory context;
- launcher database list and database-list memory context;
- Valgrind-preserved launcher database array pointer;
- worker `WorkerInfo` pointer.

The private `avl_dbase` and `WorkerInfoData` types remain private to
`autovacuum.c`; `backend_runtime.h` forward-declares their struct tags so the
compatibility macros in `autovacuum.c` stay typed and assignable without
exposing those struct layouts.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `autovacuum.o`, and
  `test_backend_runtime.o`;
- a backend clean, generated-header recovery, full `gmake -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 93 to 79;
- `gmake -C contrib -j8` and a clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `src/test/modules/test_backend_runtime` now includes
  `test_backend_autovacuum_state_is_backend_local()`, covering all moved
  autovacuum fields across two fake logical backends.

## Backend Repack State Bridge

The one-hundred-fifty-second Phase 12 slice moves backend-local repack leader
and worker state into a dedicated `PgBackendRepackState` bucket:

- the leader's current `DecodingWorker` pointer;
- the exported repack worker message-pending flag;
- the worker-role flag, current WAL segment, worker DSM segment pointer, and
  repacked heap/toast relfile locators.

`commands/repack.h` keeps `RepackMessagePending` as a source-compatible lvalue
macro over `PgCurrentRepackMessagePendingRef()`. The private
`DecodingWorker` layout remains local to `repack.c`; `backend_runtime.h`
forward-declares the struct tag and only stores the pointer. `repack_worker.c`
keeps its historical local names as file-local macros over
`PgCurrentRepackState()`.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `repack.o`,
  `repack_worker.o`, and `test_backend_runtime.o`;
- a full `gmake -j8` and `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 79 to 72;
- `gmake -C contrib -j8` and a clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `src/test/modules/test_backend_runtime` now includes
  `test_backend_repack_state_is_backend_local()`, covering all moved repack
  fields across two fake logical backends.

## Backend AIO State Bridge

The one-hundred-fifty-third Phase 12 slice moves backend-local asynchronous
I/O state into a dedicated `PgBackendAioState` bucket:

- the current backend's `PgAioBackend` pointer;
- the AIO method-worker id;
- the io_uring method context pointer.

`aio_internal.h` keeps `pgaio_my_backend` as a source-compatible lvalue macro
over `PgCurrentAioBackendRef()`. `method_worker.c` keeps `MyIoWorkerId` as a
file-local macro over `PgCurrentAioState()`, and `method_io_uring.c` does the
same for `pgaio_my_uring_context`. The private `PgAioUringContext` layout
remains local to `method_io_uring.c`; `backend_runtime.h` only
forward-declares the struct tag and stores the pointer.

The first incremental build linked stale AIO objects that still referenced
the removed `pgaio_my_backend` storage. A later temp-install run reached
`initdb` bootstrap but spun during post-bootstrap shutdown cleanup with stale
`PgBackend` layout assumptions in backend objects. The recovery was the
backend clean plus generated-header recovery documented in `AGENTS.md`,
followed by a clean full build and reinstall.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, AIO method objects,
  and `test_backend_runtime.o`;
- a backend clean, generated-header recovery, full `gmake -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 72 to 69;
- `gmake -C contrib -j8` and a clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `src/test/modules/test_backend_runtime` now includes
  `test_backend_aio_state_is_backend_local()`, covering all moved AIO fields
  across two fake logical backends.

## Backend Utility Command/Cache State Bridge

The one-hundred-fifty-fourth Phase 12 slice extends `PgBackendUtilityState`
to cover another command and utility cache group:

- the exported async notify interrupt-pending flag;
- async's backend-exit UNLISTEN cleanup registration flag;
- the extension sibling lookup cache head;
- the injection-point callback cache;
- the deprecated block-sampling API's shared reservoir state and initialized
  flag.

`commands/async.h` keeps `notifyInterruptPending` as a source-compatible
lvalue macro over `PgCurrentNotifyInterruptPendingRef()`. `async.c`,
`extension.c`, `injection_point.c`, and `sampling.c` keep their private
historical names as local macros over `PgBackendUtilityState`. The private
`ExtensionSiblingCache` layout remains local to `extension.c`, with
`backend_runtime.h` forward-declaring only the struct tag.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `async.o`,
  `extension.o`, `injection_point.o`, `sampling.o`, and
  `test_backend_runtime.o`;
- a backend clean, generated-header recovery, full `gmake -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 69 to 62;
- `gmake -C contrib -j8` and a clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `test_backend_utility_state_is_backend_local()` now covers the moved async,
  extension, injection-point, and sampling fields across two fake logical
  backends.

## Backend Logical Replication Worker State Completion

The one-hundred-fifty-fifth Phase 12 slice completes the remaining
logical-replication worker and slot-sync backend-local bridge work:

- the apply worker's `lsn_mapping` flush-position list;
- the apply error-context callback argument;
- the apply worker subtransaction serialization state;
- the slot-sync worker sleep/backoff interval.

`worker.c` and `slotsync.c` keep their historical source-local names as
macros over `PgBackendLogicalReplicationState`. The runtime initializer now
sets the former non-zero defaults for this state, including
`remote_attnum = -1`, invalid transaction/LSN sentinels, invalid
`subxact_last`, and `PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS`.

The runtime header deliberately avoids including private logical replication
headers. `backend_runtime.h` stores the relation pointer as
`struct LogicalRepRelMapEntry *` and the logical replication message enum as
an `int`, so generic backend include paths do not have to see
`logicalrelation.h`, `logicalproto.h`, or their private dependency graph.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, logical replication
  `worker.o` and `slotsync.o`, and `test_backend_runtime.o`;
- full `gmake -j8` and `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 62 to 58;
- `gmake -C contrib -j8` and a clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `test_backend_logical_replication_state_is_backend_local()` now covers the
  remaining logical replication worker and slot-sync fields across two fake
  logical backends.

## Backend Predicate Lock State Bridge

The one-hundred-fifty-sixth Phase 12 slice extends `PgBackendLockState` to
cover the remaining predicate-lock backend-local state in `predicate.c`:

- the local predicate-lock coalescing hash table;
- the current serializable transaction pointer;
- the current transaction write-tracking flag;
- the saved serializable transaction pointer used by parallel-query
  read-only-safe release.

`predicate.c` keeps the historical source-local names as macros over
`PgBackendLockState`. The private `SERIALIZABLEXACT` layout remains private
to predicate locking; `backend_runtime.h` stores those pointers as opaque
`void *` fields and `predicate.c` casts them locally.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `predicate.o`, and
  `test_backend_runtime.o`;
- backend clean plus generated-header recovery, full `gmake -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 58 to 54;
- `gmake -C contrib -j8` and a clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment. An immediately preceding run hit a transient macOS
  postmaster-child-count/shutdown race after completing the SQL assertions;
  the clean rerun passed without code changes;
- `test_backend_lock_state_is_backend_local()` now covers the moved
  predicate-lock fields across two fake logical backends.

## Backend Index WAL Redo State Bridge

The one-hundred-fifty-seventh Phase 12 slice extends `PgBackendXLogState` to
cover the remaining index-AM WAL redo operation memory contexts:

- nbtree redo operation context in `nbtxlog.c`;
- GIN redo operation context in `ginxlog.c`;
- GiST redo operation context in `gistxlog.c`;
- SP-GiST redo operation context in `spgxlog.c`.

Each redo file keeps the historical source-local `opCtx` name as a macro over
its own `PgBackendXLogState` field. The contexts remain local to their owning
redo modules while the storage now follows the logical backend instead of raw
backend-local TLS.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, the four index WAL
  redo objects, and `test_backend_runtime.o`;
- backend clean plus generated-header recovery, full `gmake -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 54 to 50;
- `gmake -C contrib -j8` and a clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `test_backend_xlog_state_is_backend_local()` now covers the moved index WAL
  redo contexts across two fake logical backends.

## Backend Memory Manager State Bridge

The one-hundred-fifty-eighth Phase 12 slice adds a dedicated
`PgBackendMemoryManagerState` bucket for backend-local memory-manager state:

- the allocation-set context freelists in `aset.c`;
- the memory-context logging reentrancy guard in `mcxt.c`.

`backend_runtime.h` exposes only the `AllocSetContext` struct tag, not the
allocation-set layout, so `aset.c` remains the only owner of allocation-set
internals. The state uses the normal early-backend fallback pattern because
memory allocation and memory-context logging can be reached before a full
`PgBackend` object is installed.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `aset.o`, `mcxt.o`,
  and `test_backend_runtime.o`;
- backend clean plus generated-header recovery, full `gmake -j8`, and
  `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 50 to 49. This is a
  net drop of one because the slice removes two raw memory-manager globals and
  adds one early backend fallback bucket;
- `gmake -C contrib -j8` and a clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed after updating
  the expected output for the new memory-manager state helper;
- direct threaded runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `test_backend_memory_manager_state_is_backend_local()` now covers the moved
  allocator freelists and logging guard across two fake logical backends.

## Backend Wait And Invalidation State Bridge

The one-hundred-fifty-ninth Phase 12 slice moves a wait/IPC-adjacent backend
state group under the logical backend object:

- wait-event reporting local storage and the current wait-event storage
  pointer now live in `PgBackendWaitState`;
- the local transaction ID counter used by shared-invalidation slot reuse now
  lives in `PgBackendIPCState`.

`wait_event.h` keeps the historical `my_wait_event_info` lvalue name as a
compatibility macro over the current backend's wait state. The standalone
`S_LOCK_TEST` compile path keeps its private test-only fallback storage
because it intentionally builds outside the backend runtime. The wait-state
early fallback follows the normal adoption pattern: default local wait storage
is retargeted to the adopted backend object, while an explicitly redirected
storage pointer is preserved.

`sinvaladt.c` keeps `nextLocalTransactionId` as a local compatibility macro
over `PgBackendIPCState`, so the slot startup/shutdown copy-back behavior is
unchanged while the counter now follows the logical backend.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `wait_event.o`,
  `sinvaladt.o`, and `test_backend_runtime.o`;
- backend clean plus generated-header recovery was required after the
  installed-header and `PgBackend` layout changes. The first clean rebuild
  failed at link against a stale `src/common` server object that still
  referenced `_my_wait_event_info`; cleaning `src/common` and rebuilding fixed
  the stale object;
- clean full `gmake -j8` passed after the `src/common` clean;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 49 to 47;
- `gmake -C contrib -j8` and a clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed after
  restoring the expected-file trailing blank line;
- direct threaded runtime TAP passed all 87 tests with the local
  `/Users/samwillis/perl5` `PERL5LIB` paths and an explicit `PG_REGRESS`
  environment;
- `test_backend_wait_state_is_backend_local()` now covers local and redirected
  wait-event storage across two fake logical backends, while
  `test_backend_ipc_state_is_backend_local()` covers the invalidation local
  transaction ID counter.

## Backend Command And Log State Bridge

The one-hundred-sixtieth Phase 12 slice moves tcop command-loop and elog
line-format state into explicit runtime objects:

- `DoingCommandRead` is now a compatibility macro over
  `PgSessionLoopState.doing_command_read`, because it describes the currently
  attached session's protocol read boundary rather than a process-global
  property;
- `userDoption`, `Save_r`, and `Save_t` now live in
  `PgBackendCommandState`;
- elog's formatted-session-start cache, per-backend log line number, and
  cached log-line PID now live in `PgBackendLogState`.

The command and log buckets are scalar-only. Their early fallback buckets can
be copied directly during process initialization or thread-runtime adoption.
The session-loop fallback follows the existing loop-state initializer and
preserves the `send_ready_for_query` default after copy-back, matching the
pre-existing `PgSessionLoopStateInit()` behavior.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `postgres.o`,
  `elog.o`, and `test_backend_runtime.o`;
- backend and `src/common` clean rebuild plus generated-header recovery
  passed, followed by clean full `gmake -j8`;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 47 to 44;
- `gmake -C contrib -j8` passed;
- PL/pgSQL was cleaned, rebuilt, and reinstalled after the installed-header
  layout change;
- `gmake -C src/test/modules/test_backend_runtime check` passed after
  restoring the expected-file trailing blank line;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `test_backend_command_log_state_is_backend_local()` now covers the
  command-read flag, `-D` option storage, usage snapshot fields, formatted
  start-time buffer, elog line counter, and cached elog PID across two fake
  logical backends and sessions.

## Backend PgStat Local State Bridge

The one-hundred-sixty-first Phase 12 slice moves `pgStatLocal`, the
backend-local cumulative statistics anchor, into
`PgBackendPgStatPendingState.local`. The existing `pgStatLocal` spelling is
now a compatibility macro over `PgCurrentPgStatLocalState()`, so the broad
pgstat implementation still reads naturally while the shared stats control
pointer, DSA attachment, shared dshash handle, and current statistics snapshot
follow the logical backend object.

This intentionally adds a temporary include edge from `backend_runtime.h` to
`pgstat_internal.h` so the `PgStat_LocalState` object can remain embedded
rather than allocated lazily per backend. That header coupling is acceptable
for Phase 12 migration speed, but it should be revisited with the broader
Gate E2 object-lifecycle/header-boundary audit.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `pgstat.o`, selected
  pgstat activity objects, and `test_backend_runtime.o`;
- `src/backend/utils/activity` built cleanly and a static search confirmed the
  standalone `PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgStat_LocalState pgStatLocal`
  definition and extern declaration were removed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 44 to 42;
- backend and `src/common` clean rebuild plus generated-header recovery
  passed, followed by clean full `gmake -j8`;
- `gmake DESTDIR="$PWD/tmp_install" install`, `gmake -C contrib -j8`, and a
  clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `test_backend_pgstat_pending_state_is_backend_local()` now also verifies
  `pgStatLocal.shmem`, `pgStatLocal.dsa`, `pgStatLocal.shared_hash`, and
  `pgStatLocal.snapshot.mode` across two fake logical backends.

## Backend Expression Interpreter State Bridge

The one-hundred-sixty-second Phase 12 slice moves the computed-goto
expression interpreter lookup state into a new `PgBackendExprInterpState`
bucket:

- `dispatch_table` is now a compatibility macro over the current backend's
  interpreter state;
- `reverse_dispatch_table` is now embedded in `PgBackendExprInterpState` and
  follows the logical backend;
- the local label-address table inside `ExecInterpExpr()` was renamed to
  `local_dispatch_table` so the compatibility macro only targets backend-owned
  state.

The reverse lookup table stores the opcode as an integer to avoid adding an
`executor/execExpr.h` include edge to `backend_runtime.h`. The runtime bucket
uses a fixed capacity, `PG_BACKEND_EXPR_INTERP_MAX_OPS`, and
`execExprInterp.c` asserts that `EEOP_LAST` fits. This keeps the bucket
copyable for process/thread early adoption and avoids a new teardown-owned
allocation.

Validation for this slice:

- touched-object builds passed for `execExprInterp.o`, `backend_runtime.o`,
  and `test_backend_runtime.o`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 42 to 41. The net
  drop is one because two standalone interpreter globals were replaced by one
  early backend fallback bucket;
- backend and `src/common` clean rebuild plus generated-header recovery
  passed, followed by clean full `gmake -j8`;
- `gmake DESTDIR="$PWD/tmp_install" install`, `gmake -C contrib -j8`, and a
  clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed after fixing
  the new expected-output column width and trailing blank line;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `test_backend_expr_interp_state_is_backend_local()` now verifies the
  dispatch table pointer and first reverse-lookup entry across two fake
  logical backends.

## Backend LWLock Stats State Bridge

The one-hundred-sixty-third Phase 12 slice completes the optional LWLock
debug-statistics state bridge by extending `PgBackendLockState` again:

- `lwlock_stats_htab` now follows the logical backend;
- the dummy stats entry used before the hash table exists is embedded in
  `PgBackendLockState`;
- the `LWLock stats` memory context pointer and exit-callback registration
  flag were moved out of `init_lwlock_stats()` function-local statics and into
  `PgBackendLockState`.

`lwlock.c` keeps the historical local names as compatibility macros over
runtime accessors. The stats key and entry types are now public runtime structs
only because the backend-runtime test needs to validate the fields in normal
builds where `LWLOCK_STATS` is not compiled.

Validation for this slice:

- touched-object builds passed for `lwlock.o`, `backend_runtime.o`, and
  `test_backend_runtime.o`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 41 to 39;
- backend and `src/common` clean rebuild plus generated-header recovery
  passed, followed by clean full `gmake -j8`;
- `gmake DESTDIR="$PWD/tmp_install" install`, `gmake -C contrib -j8`, and a
  clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `test_backend_lock_state_is_backend_local()` now verifies the LWLock stats
  hash pointer, dummy entry, memory context pointer, and exit-registration flag
  across two fake logical backends.

## Execution Snapshot And Combo CID State Bridge

The one-hundred-sixty-fourth Phase 12 slice moves a transaction/execution
visibility state batch into two new `PgExecution` buckets:

- `PgExecutionSnapshotState` owns the current, secondary, catalog, and historic
  snapshot pointers plus their reusable `SnapshotData` storage;
- `TransactionXmin`, `RecentXmin`, `FirstSnapshotSet`,
  `FirstXactSnapshot`, exported-snapshot list state, historic tuple-CID state,
  and the active/registered snapshot tracking structures now follow the
  logical execution;
- `PgExecutionComboCidState` owns the combo-CID hash, array pointer, and
  counters used while a transaction is active.

`snapmgr.h` preserves the exported `FirstSnapshotSet`, `TransactionXmin`, and
`RecentXmin` names as lvalue macros over runtime accessors. Private
`snapmgr.c` names are source-local compatibility macros. The registered
snapshot heap remains owned by `snapmgr.c`: the runtime bucket stores the
heap, while `snapmgr.c` lazily installs its private `xmin_cmp` comparator.
`combocid.c` keeps the combo-CID key/entry layouts private and stores the
array pointer opaquely in `PgExecutionComboCidState`.

The lifecycle rule for this slice is intentionally the current PostgreSQL
rule made explicit: the runtime bucket owns the live pointers/counters, while
transaction-allocated snapshot and combo-CID memory is still released by
`TopTransactionContext` and existing end-of-transaction cleanup. Early
adoption copies each bucket as a whole, then resets the early fallback bucket;
snapshot initialization restores the `FirstNormalTransactionId` defaults for
real runtime objects.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `snapmgr.o`,
  `combocid.o`, and `test_backend_runtime.o`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with execution-local declarations dropping from 154 to 134;
- backend and `src/common` clean rebuild plus generated-header recovery
  passed, followed by clean full `gmake -j8`;
- `gmake DESTDIR="$PWD/tmp_install" install`, `gmake -C contrib -j8`, and a
  clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `test_execution_snapshot_combo_state_is_execution_local()` now verifies the
  snapshot pointer/data fields, active and registered snapshot handles,
  exported-snapshot list, tuple-CID state, and combo-CID state across two fake
  logical executions.

## Execution XLog Insert State Bridge

The one-hundred-sixty-fifth Phase 12 slice moves the WAL record-construction
workspace in `xloginsert.c` into a new `PgExecutionXLogInsertState` bucket:

- the registered-buffer array pointer, allocation size, and highest registered
  block id;
- the main-data `XLogRecData` chain head/tail and accumulated byte count;
- the current insert flags, embedded header `XLogRecData`, and header scratch
  buffer;
- the registered-data array pointer/count/capacity;
- the in-progress `XLogBeginInsert()` guard and the workspace memory context.

`registered_buffer` stays private to `xloginsert.c`; the runtime object stores
that array through an opaque pointer, while `xloginsert.c` casts it back behind
source-local compatibility macros. The existing `XLogRecData` type is already
part of the WAL insertion interface, so the runtime object can store those
chain pointers and the header record directly.

The lifecycle rule for this slice is explicit: `InitXLogInsert()` still owns
workspace allocation and `XLogResetInsertion()` still resets the current
record. Early adoption is allowed only when no WAL insert is in progress. If
early initialization has installed the legacy `mainrdata_last` sentinel that
points at the early bucket's own `mainrdata_head`, adoption retargets that
self-pointer to the destination execution bucket before resetting the early
fallback state. All allocation pointers copied during adoption are then owned
by the destination `PgExecution`.

This slice deliberately does not move the hidden function-local fake-LSN
statics in `XLogGetFakeLSN()`. Those values are not part of the transient WAL
record-construction workspace and need a separate session/execution lifetime
decision before migration.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `xloginsert.o`, and
  `test_backend_runtime.o`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with execution-local declarations dropping from 134 to 121;
- backend and `src/common` clean rebuild plus generated-header recovery
  passed, followed by clean full `gmake -j8`;
- `gmake DESTDIR="$PWD/tmp_install" install`, `gmake -C contrib -j8`, and a
  clean PL/pgSQL rebuild/install passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment;
- `test_execution_xloginsert_state_is_execution_local()` now verifies the WAL
  insert workspace pointers, counters, flags, embedded header record, scratch
  buffer, and memory context across two fake logical executions.

## Execution Transaction Flag State Bridge

The one-hundred-sixty-sixth Phase 12 slice moves the simple exported
transaction execution state in `xact.c` into a new `PgExecutionXactState`
bucket:

- current transaction isolation level, read-only flag, and deferrable flag;
- the statement-sampling flag for the active transaction;
- logical-decoding concurrent-abort guard state, `CheckXidAlive` and
  `bsysscan`;
- `MyXactFlags`, the top-level transaction event flag word used by callers
  across the tree.

`xact.h` preserves the existing public names as lvalue macros over runtime
accessors. This avoids including `backend_runtime.h` from `xact.h`, which
would create a circular dependency because the runtime header already needs
transaction definitions. The private transaction-state stack, command-id
state, timestamps, transaction callback lists, and abort context remain in
`xact.c` for later migration because they need a broader lifecycle split than
this simple exported-state bridge.

The lifecycle rule for this slice is straightforward: process and thread
runtime initialization adopt any early fallback writes as a whole and reset
the early bucket to PostgreSQL's existing defaults, `XACT_READ_COMMITTED` and
`InvalidTransactionId` for `CheckXidAlive`. The bucket contains no lists,
memory contexts, hash tables, or opaque owned pointers.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `xact.o`, and
  `test_backend_runtime.o`;
- clean backend and `src/common` rebuild passed after regenerating backend
  utility outputs, node support, and generated header symlinks;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake -C contrib -j8` passed;
- clean PL/pgSQL rebuild and reinstall into `tmp_install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with execution-local declarations dropping from 121 to 108;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed
  with 87 tests;
- `test_execution_xact_state_is_execution_local()` now verifies transaction
  isolation/read-only/deferrable state, transaction sampling state,
  `CheckXidAlive`, `bsysscan`, and `MyXactFlags` across two fake logical
  executions.

## Execution GUC Error Scratch State Bridge

The one-hundred-sixty-seventh Phase 12 slice moves GUC/error-report scratch
state into a new `PgExecutionGUCErrorState` bucket:

- GUC check-hook error code and message/detail/hint strings;
- `pre_format_elog_string()` errno and text-domain scratch state used by the
  GUC check-hook reporting macros;
- config-file scanner line number, fatal-flex message, and fatal-flex jump
  target used while parsing configuration files.

`guc.h` keeps the public `GUC_check_errmsg_string`,
`GUC_check_errdetail_string`, and `GUC_check_errhint_string` names as lvalue
macros over runtime accessors. `guc.c`, `elog.c`, and `guc-file.l` use local
compatibility macros for their private scratch names, so existing call sites
and lexer actions continue to read and assign the same identifiers.

The lifecycle rule is whole-bucket copy/adopt plus zero reset. The moved
fields are scalars or borrowed pointers to ErrorContext strings, static flex
messages, text-domain strings, or stack-owned jump buffers. The bucket owns no
lists, memory contexts, hash tables, sockets, or opaque heap allocations, so
there is no destroy action beyond normal execution reset.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`,
  `guc-file.o`, `elog.o`, and `test_backend_runtime.o`;
- stale backend objects initially failed to link against the removed exported
  GUC check-hook variables, proving this installed-header change requires the
  documented clean rebuild path;
- clean backend and `src/common` rebuild passed after regenerating backend
  utility outputs, node support, and generated header symlinks;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- clean PL/pgSQL rebuild and reinstall into `tmp_install` passed after a
  stale `plpgsql.dylib` failed to load the removed GUC check-hook symbols;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with execution-local declarations dropping from 108 to 97;
- direct threaded TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed
  with 87 tests after patching local macOS `libpq.5.dylib` install names for
  `pg_regress` and temp-install frontend binaries;
- `test_execution_guc_error_state_is_execution_local()` now verifies all moved
  GUC check-hook, formatting, and config-file scanner scratch fields across
  two fake logical executions.

## Execution Misc Scratch State Bridge

The one-hundred-sixty-eighth Phase 12 slice moves a small set of
execution-scope scratch globals under `PgExecution`:

- array typanalyze callback scratch state, as
  `PgExecutionAnalyzeState.array_extra_data`;
- regular-expression locale lookup scratch state, as
  `PgExecutionRegexState.regex_locale`;
- the optional Valgrind command-loop error counter scratch state, as
  `PgExecutionValgrindState.old_error_count`;
- logical-decoding snapshot-builder exported-snapshot scratch state, as
  `PgExecutionSnapBuildState`.

The regex and snapshot-builder fields are intentionally opaque or borrowed:
the regex locale pointer keeps the `pg_locale_t` definition private to the
regex/locale boundary, while `saved_resource_owner_during_export` is a
borrowed `ResourceOwner` pointer restored by snapshot-builder export cleanup.
The array typanalyze field continues to be owned by the analyze callback path,
and the Valgrind field is a scalar counter.

The lifecycle rule is whole-bucket copy/adopt plus zero reset. The moved
fields are scalars or borrowed execution-scope pointers; the bucket owns no
lists, memory contexts, hash tables, sockets, or heap allocations. Thread
runtime install adopts the same early fallback buckets as process runtime
initialization, so there is no silent process/thread asymmetry for this slice.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `regcomp.o`,
  `array_typanalyze.o`, `postgres.o`, `snapbuild.o`, and
  `test_backend_runtime.o`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with execution-local declarations dropping from 97 to 95;
- clean backend and `src/common` rebuild passed after regenerating backend
  utility outputs, node support, and generated header symlinks;
- full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- clean PL/pgSQL rebuild and reinstall into `tmp_install` passed after the
  runtime header/layout change;
- `gmake -C contrib -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed after
  refreshing the expected terminal blank line for the new output shape;
- direct threaded TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed
  with 87 tests after patching local macOS `libpq.5.dylib` install names for
  `pg_regress` and temp-install frontend binaries;
- `test_execution_misc_scratch_state_is_execution_local()` now verifies the
  moved array typanalyze, regex locale, Valgrind, and snapshot-builder
  scratch fields across two fake logical executions.

## Backend Early Adoption Symmetry

The one-hundred-sixty-ninth Phase 12 slice closes the first concrete
object-model review concern by centralizing backend early fallback adoption in
`PgBackendAdoptEarlyState()`:

- process runtime initialization and thread backend installation now call the
  same backend adoption helper instead of maintaining separate manual lists;
- the shared helper covers the backend buckets that process mode already
  adopted but thread install had missed: WAL sender, replication, logical
  replication, XLog, recovery, maintenance-worker, autovacuum, repack, AIO,
  pending-interrupt, and interrupt-holdoff state;
- backend exit state intentionally remains outside this helper because it is
  owned by the `storage/ipc` backend-exit cleanup lifecycle and is adopted
  through `PgBackendAdoptEarlyExitState()`;
- `PgBackendAdoptEarlyAutovacuumState()` now asserts that the early
  autovacuum database list is empty before adoption and reinitializes the
  adopted backend's `database_list` head, avoiding copied empty-`dlist`
  self-pointers.

This is a Gate E2 hardening step, not the full lifecycle audit. It removes one
class of process/thread adoption drift and adds one explicit pointer/list copy
rule. The remaining Gate E2 object-model work still needs bucket-by-bucket
initializer, adoption, reset/destroy, owner/lifetime, and pointer/list/socket/
hash/memory-context classification.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o` and
  `test_backend_runtime.o`;
- `gmake -C src/test/modules/test_backend_runtime check` passed with the new
  `test_thread_install_adopts_backend_fallback_state()` regression helper;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and
  patched macOS install names;
- full `gmake -j8` passed;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals.

## Session And Execution Early Adoption Symmetry

The one-hundred-seventieth Phase 12 slice extends the Gate E2 adoption-list
hardening to `PgSession` and `PgExecution`:

- `PgSessionAdoptEarlyState()` now owns the complete list of session early
  fallback buckets;
- `PgExecutionAdoptEarlyState()` now owns the complete list of execution early
  fallback buckets;
- `InitializePgProcessRuntime()` and `InstallPgThreadBackendRuntimeState()`
  both call those aggregate helpers instead of maintaining parallel manual
  session/execution lists;
- `test_thread_install_adopts_session_execution_fallback_state()` verifies
  representative session and execution fallback values are adopted into a
  target object and the early fallback buckets are reset to their defaults.

This is another Gate E2 hardening step, not the full lifecycle audit. It
prevents newly migrated session/execution buckets from being added to only one
runtime path.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o` and
  `test_backend_runtime.o`;
- static scan of `backend_runtime.c` confirmed process and thread install now
  call only `PgSessionAdoptEarlyState()` and `PgExecutionAdoptEarlyState()`
  for session/execution early adoption;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and
  patched macOS install names;
- full `gmake -j8` passed;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals.

## Connection Early Adoption Symmetry

The one-hundred-seventy-first Phase 12 slice completes the current aggregate
early-adoption symmetry pass for the four runtime object families:

- `PgConnectionAdoptEarlyState()` now owns the complete list of connection
  early fallback buckets;
- process runtime initialization calls `PgConnectionAdoptEarlyState()` with no
  preserved port, matching the historical process behavior of adopting
  `MyProcPort` from early fallback state;
- thread backend installation calls the same helper while preserving the
  constructor-provided `Port`, so pre-install connection fallback writes are
  adopted and reset without losing the live frontend connection object;
- `test_thread_install_adopts_connection_fallback_state()` verifies identity,
  cancel key, socket I/O, protocol, output, interrupt, startup timing, client
  auth info, and security fallback adoption plus fallback reset.

This closes the concrete manual process/thread connection adoption-list
asymmetry. It does not close the broader Gate E2 lifecycle audit: connection
state still has pointer-bearing fields such as send buffers, wait sets,
security buffers, and borrowed authentication strings that need explicit
reset/destroy ownership classification before Phase 12 closes.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o` and
  `test_backend_runtime.o`;
- static scan of `backend_runtime.c` confirmed process and thread install now
  use aggregate helpers for backend, session, connection, and execution early
  adoption;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and
  patched macOS install names;
- full `gmake -j8` passed;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals.

## Backend Parallel State Bridge

The one-hundred-forty-second Phase 12 slice moves a parallel-query and
shared-memory message-queue backend-local state group into a new
`PgBackendParallelState` bucket:

- exported parallel worker state: `ParallelWorkerNumber`,
  `ParallelMessagePending`, and `InitializingParallelWorker`;
- private parallel context state in `parallel.c`: `MyFixedParallelState`,
  `pcxt_list`, `pcxt_list_initialized`, and `ParallelLeaderPid`;
- private shared-memory message queue state in `pqmq.c`: `pq_mq_handle`,
  `pq_mq_busy`, `pq_mq_parallel_leader_pid`, and
  `pq_mq_parallel_leader_proc_number`.

The private `FixedParallelState` and `shm_mq_handle` types stay private to
their owning source files. `PgBackendParallelState` stores those pointers as
opaque `void *` fields, and the owning files cast them through local
compatibility macros. The early fallback state is statically initialized with
the same sentinels as the former standalone TLS globals, especially
`ParallelWorkerNumber = -1`, because bootstrap reaches parallel-worker checks
before full backend runtime adoption.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `parallel.o`,
  `pqmq.o`, and `test_backend_runtime.o`;
- a static scan found no remaining raw TLS declarations for the moved
  parallel worker or pqmq state;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed after fixing the early
  parallel fallback sentinel;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 262 to 249;
- `gmake -C contrib -j8` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header and `PgBackend` layout changes;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the new parallel-state helper, and still
  reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment.

## Backend Utility Cache State Bridge

The one-hundred-forty-first Phase 12 slice extends `PgBackendUtilityState` to
cover a larger utility cache/scratch-state group:

- date/time token lookup caches in `datetime.c`;
- cached constants for degree-based trigonometric functions in `float.c`;
- date/time and numeric format-picture caches in `formatting.c`;
- the optional libxml allocation context in `xml.c`;
- the pass-by-reference missing-attribute datum cache in `heaptuple.c`.

The date/time and formatting cache entry types remain private to their owning
source files. `PgBackendUtilityState` stores those arrays as opaque pointers,
and the source files cast them through file-local compatibility macros. The
runtime header defines only the fixed cache sizes and scalar fields; owning
files assert that their local cache-size constants match the runtime storage.

Validation for this slice:

- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake -C src/backend/utils/adt datetime.o float.o formatting.o xml.o`
  passed;
- `gmake -C src/backend/access/common heaptuple.o` passed;
- `gmake -C src/test/modules/test_backend_runtime test_backend_runtime.o`
  passed after expanding `test_backend_utility_state_is_backend_local()`;
- a static scan found no remaining raw TLS declarations for the moved
  date/time, float, formatting, libxml-context, or missing-attribute cache
  state;
- a full backend clean plus generated-header recovery was run after the
  installed-header and `PgBackend` layout changes;
- clean full `gmake -j8` passed;
- `gmake DESTDIR="$PWD/tmp_install" install` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals, with backend-local declarations dropping from 280 to 262;
- `gmake -C contrib -j8` passed;
- PL/pgSQL and `src/test/modules/test_backend_runtime` were cleaned, rebuilt,
  and reinstalled after the installed-header change;
- `gmake -C src/test/modules/test_backend_runtime check` passed the
  process-mode regression, including the expanded utility-state helper, and
  still reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and an
  explicit `PG_REGRESS` environment.

## Connection Closed-State Reset

The one-hundred-seventy-second Phase 12 slice makes one Gate E2 lifecycle
cleanup boundary explicit for retained connection runtime objects:

- new `PgConnectionResetClosedState()` resets the connection socket I/O bucket,
  protocol dispatch/wait-set fields, startup client-auth pointer state, and
  security scratch fields after connection close;
- the helper frees the malloc-backed GSS send, receive, and result buffers
  allocated by `be-secure-gssapi.c`;
- it intentionally does not free the palloc-backed libpq send buffer or
  `WaitEventSet`, because `socket_close()` already owns those release calls;
- `socket_close()` now captures the current `PgConnection` and calls the reset
  helper after releasing the wait set, send buffer, secure/socket state, and
  `PortContext`;
- `test_connection_reset_closed_state()` exercises the reset directly with
  malloc-backed fake GSS buffers and verifies stale socket/protocol/startup/
  security pointers and counters are scrubbed.

This closes a concrete connection bucket reset/destroy rule from the Gate E2
object-lifecycle review. It is not the full threaded teardown solution:
carrier `TopMemoryContext` reclamation, a complete backend/session/connection/
execution destructor tree, and the remaining bucket-by-bucket lifecycle audit
remain Gate E2 blockers before Phase 12 can close.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `pqcomm.o`, and
  `test_backend_runtime.o`;

## Session Dynamic Library Init Cleanup

The one-hundred-seventy-fourth Phase 12 slice closes a concrete session reset
rule from the Gate E2 lifecycle manifest:

- `PgSession` now owns a `dynamic_library_context` for the per-session
  `dynamic_library_inits` replay list used by threaded extension `_PG_init()`
  handling;
- `remember_module_session_init()` now allocates replay-list cells in that
  session-owned context instead of `TopMemoryContext`;
- `PgSessionResetClosedState()` deletes the session-owned dynamic-library
  context and clears `dynamic_library_inits`, with a legacy fallback that frees
  pre-existing list cells if no context exists;
- `PgBackendExitCleanup()` calls `PgSessionResetClosedState()` after existing
  `on_proc_exit` callbacks, preserving callback access to session state while
  still releasing the per-session replay list before threaded backend teardown
  accounting;
- `test_session_reset_closed_state()` verifies both the session-context cleanup
  path and the legacy list-free fallback.

This does not close the full session destructor model. It converts one
list-bearing `PgSession` field from a pending lifecycle row into an explicit
reset/destroy path and keeps process mode behavior compatible.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `dfmgr.o`, `ipc.o`,
  and `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 125 fields classified;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_session_reset_closed_state()`;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths and
  patched macOS install names;
- full `gmake -j8` passed;
- `gmake -C contrib -j8` passed;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals.

## Async Execution State

The next Phase 12 state-migration slice moves LISTEN/NOTIFY transaction
scratch state into `PgExecutionAsyncState`:

- `pendingActions`, `pendingListenActions`, `pendingNotifies`,
  `queueHeadBeforeWrite`, `queueHeadAfterWrite`, `signalPids`,
  `signalProcnos`, and `tryAdvanceTail` are now fields in the current
  `PgExecution`;
- `async.c` keeps the historic local names as macros over runtime accessors,
  while `QueuePosition` is now an alias of the runtime queue-position layout;
- `PgExecutionAdoptEarlyState()` adopts and resets the async fallback bucket,
  so process runtime initialization and thread backend install remain
  symmetrical;
- `InitializePgThreadBackendRuntimeState()` explicitly initializes the async
  bucket before install;
- `test_thread_install_adopts_session_execution_fallback_state()` now verifies
  early async fallback adoption and reset behavior;
- `test_execution_async_state_is_execution_local()` verifies that two
  simulated logical executions keep pending async pointers, queue positions,
  signal work arrays, and cleanup flags isolated.

Ownership remains with existing async cleanup paths: pending action/notify
lists and hash state live in transaction contexts and are cleared by async
transaction cleanup, while the `SignalBackends()` workspace arrays are still
allocated under `TopMemoryContext` until the broader backend destructor model
is closed. The runtime object now owns the execution-local pointer slots and
queue-position values, not the pointed-to storage.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `async.o`, and
  `test_backend_runtime.o`;
- full `gmake -j8` passed after the final async object relink;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_execution_async_state_is_execution_local()`;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths, explicit
  `PG_REGRESS`, and patched macOS install names;
- `gmake check-runtime-lifecycles` passed with 127 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and 81 execution-local declarations;
- `gmake -C contrib -j8` passed;
- `git diff --check` passed.

## Transaction Scalar Execution State

The next Phase 12 transaction-state slice moves a larger scalar/pointer batch
from `xact.c` into `PgExecutionXactState`:

- top-level full transaction ID;
- parallel-current-XID count and borrowed pointer;
- inline unreported-XID storage, with a runtime/static assert that the array
  capacity matches `PGPROC_MAX_CACHED_SUBXIDS`;
- subtransaction and command ID counters;
- transaction, statement, and transaction-stop timestamps;
- prepare GID pointer;
- force-synchronous-commit flag;
- transaction abort context pointer.

`xact.c` keeps its historic local names as file-local compatibility macros
over runtime accessors. The serialized parallel-transaction state struct uses
renamed fields for the command ID and parallel-XID count so those macros do
not rewrite `result->field` or `tstate->field` references.

This is still not the full transaction lifecycle split. The private
`TransactionStateData` stack and transaction callback lists remain in `xact.c`
for a later batch because moving them cleanly needs either a private
transaction runtime-state boundary or a broader destructor/reset rule. The
moved pointers are explicitly borrowed: parallel-current-XID storage is owned
by serialized parallel transaction state, prepare GID follows existing
transaction prepare-state ownership, and `TransactionAbortContext` is owned by
existing transaction memory-context cleanup. The runtime object owns the
execution-local slots and inline unreported-XID array.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `xact.o`, and
  `test_backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  the expanded `test_execution_xact_state_is_execution_local()` coverage for
  the moved transaction fields;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths, explicit
  `PG_REGRESS`, patched macOS install names, and a freshly reinstalled
  `tmp_install`;
- `gmake check-runtime-lifecycles` passed with 127 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and 67 execution-local declarations;
- `gmake -C contrib -j8` passed;
- `git diff --check` passed.

## Transaction Cleanup Execution State

The next Phase 12 transaction-cleanup slice moves a coherent set of
transaction/subtransaction cleanup pointers and flags into
`PgExecutionTransactionCleanupState`:

- large-object descriptor cookie array, size, cleanup-needed flag, and memory
  context pointer from `be-fsstubs.c`;
- the transaction temporary-file cleanup-needed flag from `fd.c`;
- the pgstat subtransaction stack pointer from `pgstat_xact.c`;
- the RI fast-path batch cache pointer and callback-registered flag from
  `ri_triggers.c`.

This is a slot-ownership move, not a destructor rewrite. The moved pointer
slots now belong to the logical execution object, while the pointed-to storage
continues to be owned by existing PostgreSQL cleanup paths: large-object
descriptor state is released by the current large-object transaction cleanup,
temporary files are removed by the existing transaction cleanup path, pgstat
subtransaction status is allocated and popped by pgstat xact cleanup, and the
RI fast-path hash table remains tied to the current batch/transaction callback
behavior. The lifecycle manifest records these as borrowed pointer slots plus
copied scalar flags, with whole-bucket fallback adoption through
`PgExecutionAdoptEarlyState()`.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `be-fsstubs.o`,
  `fd.o`, `pgstat_xact.o`, `ri_triggers.o`, and
  `test_backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_execution_transaction_cleanup_state_is_execution_local()`;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths, explicit
  `PG_REGRESS`, patched macOS install names, and a freshly recreated
  `tmp_install`;
- `gmake check-runtime-lifecycles` passed with 128 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and 60 execution-local declarations;
- `gmake -C contrib -j8` passed;
- `git diff --check` passed.

## Execution Error And Replication Scratch State

The next Phase 12 execution-scratch slice moves reporting and logical
replication scratch slots into `PgExecution`:

- `PgExecutionErrorState` now owns the `elog.c` error-data stack, stack depth,
  recursion depth, saved timestamp cache, and formatted log-time buffer, in
  addition to the existing error/exception stack pointers;
- `PgExecutionReplicationScratchState` owns the event-trigger query-state
  pointer, replication-origin transaction state, logical apply error-context
  stack, logical apply message memory context, and logical streaming memory
  context.

This is another slot-ownership move, not a destructor rewrite. `ErrorData`
string contents and callback-owned context state remain managed by the
existing error cleanup paths. Event-trigger query state remains owned by the
existing command/event-trigger context lifecycle. Logical apply message and
streaming memory contexts remain owned by logical apply worker startup and
cleanup paths. Replication-origin transaction state is copied scalar state.
The lifecycle manifest records the copied-scalar and borrowed-pointer rules,
and both process initialization and threaded runtime installation reach the
new bucket through centralized `PgExecutionAdoptEarlyState()`.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `elog.o`,
  `event_trigger.o`, `origin.o`, `worker.o`, and
  `test_backend_runtime.o`;
- full `gmake -j8` passed after rebuilding stale backend objects that still
  referenced the removed logical apply, replication-origin, and runtime-layout
  symbols;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed, followed by
  rebuilding and reinstalling `src/test/modules/test_backend_runtime`;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_execution_reporting_replication_state_is_execution_local()`;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with the local `/Users/samwillis/perl5` `PERL5LIB` paths, explicit
  `PG_REGRESS`, patched macOS install names, and a freshly recreated
  `tmp_install`;
- `gmake check-runtime-lifecycles` passed with 129 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and 47 execution-local declarations;
- `gmake -C contrib -j8` passed;
- `git diff --check` passed.

## Catalog Cache Execution State

The next Phase 12 catalog-cache slice moves relcache and catcache execution
scratch slots into `PgExecutionCatalogCacheState`:

- catcache's create-in-progress stack pointer;
- relcache's `RelationBuildDesc()` in-progress list pointer, length, and
  allocated length;
- relcache's EOXact relation OID list, length, and overflow flag;
- relcache's EOXact tuple descriptor array pointer, next index, and allocated
  length.

This keeps source-local names in `catcache.c` and `relcache.c` as
compatibility macros over runtime accessors. The runtime object owns the
slots and inline EOXact OID list. The pointed-to catcache stack entries remain
borrowed stack objects, the relcache in-progress list remains allocated in
`CacheMemoryContext`, and the tuple descriptor array remains owned by existing
relcache EOXact cleanup.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `catcache.o`,
  `relcache.o`, and `test_backend_runtime.o`;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_execution_catalog_cache_state_is_execution_local()`;
- `gmake check-runtime-lifecycles` passed with 130 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and 38 execution-local declarations;
- `gmake -C contrib -j8` passed;
- direct threaded-runtime TAP initially exposed stale runtime-layout objects:
  `001_threaded_runtime.pl` saw one postmaster child and shutdown stuck until
  `launch_backend.o` and the touched runtime/cache/test objects were rebuilt.
  After reinstalling into `tmp_install`, direct
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with patched macOS install names;
- `git diff --check` passed.

## Relation Mapper Execution State

The next Phase 12 relation-mapper slice moves the active and pending relation
map update files into `PgExecutionRelMapState`:

- `active_shared_updates`;
- `active_local_updates`;
- `pending_shared_updates`;
- `pending_local_updates`.

The session-visible `shared_map` and `local_map` files remain session-local
state. The new execution bucket owns only transaction-local relation-map
updates that become visible at command-counter boundaries and are cleared or
committed by the existing relmapper CCI/EOXact paths. The map files are inline
scalar storage, so adoption copies them by value and then resets the early
fallback bucket. Parallel worker restore keeps the existing safety rule: it
errors if any active or pending map already exists before replacing the active
maps from serialized leader state.

This also reduces the remaining execution-local scan by four declarations
without changing relmapper behavior in process mode.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `relmapper.o`, and
  `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 131 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and 35 execution-local declarations;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_execution_relmap_state_is_execution_local()`;
- after forcing rebuild of `launch_backend.o` and the touched runtime/cache/test
  objects, full `gmake -j8`, `gmake -j8 install DESTDIR="$PWD/tmp_install"`,
  and reinstalling `src/test/modules/test_backend_runtime` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with patched macOS install names;
- `gmake -C contrib -j8` passed.

## Invalidation And Two-Phase Execution State

The next Phase 12 transaction-scratch slice moves cache invalidation and
two-phase prepare-record scratch into `PgExecution`:

- `InvalMessageArrays`;
- `transInvalInfo`;
- `inplaceInvalInfo`;
- the two-phase prepare `records` chain.

`PgExecutionInvalidationState` keeps the invalidation message arrays together
with the transaction and inplace-update control pointers. The message arrays
still point to storage owned by `TopTransactionContext`; transaction,
subtransaction, command-boundary, and inplace-update cleanup continue to own
message processing and pointer reset.

`PgExecutionTwoPhaseRecordState` keeps the in-memory state-file chunk chain
used while assembling a `PREPARE TRANSACTION` record. `EndPrepare()` still
clears the chain after WAL insertion, and transaction/error cleanup still owns
the allocated chunks if prepare aborts before that point.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `inval.o`,
  `twophase.o`, and `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 133 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and 33 execution-local declarations;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_execution_inval_twophase_state_is_execution_local()`;
- after forcing rebuild of `launch_backend.o` and the touched
  runtime/inval/twophase/test objects, full `gmake -j8`,
  `gmake -j8 install DESTDIR="$PWD/tmp_install"`, and reinstalling
  `src/test/modules/test_backend_runtime` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with patched macOS install names;
- `gmake -C contrib -j8` passed.

## Trigger Execution State And pg_prewarm Scratch

The next Phase 12 execution-scratch slice moves trigger execution state out of
plain execution-local TLS and removes one contrib execution-local scratch
global:

- `MyTriggerDepth`;
- the after-trigger transaction/query state previously held in `afterTriggers`;
- `pg_prewarm`'s read-mode `blockbuffer`.

`PgExecutionTriggerState` owns the trigger nesting depth and an opaque pointer
to the private `AfterTriggersData` object. The pointer remains opaque so
`backend_runtime.h` does not expose trigger-internal structs, lists, event
chunks, tuplestores, or callback items. `trigger.c` lazily allocates that
private object in `TopMemoryContext` for the current execution bridge; the
internal memory contexts, query stacks, transition tuplestores, event lists,
and callback lists remain owned and reset by the existing trigger
transaction/subtransaction cleanup paths.

`pg_prewarm` no longer needs runtime state for read-mode scratch. Its aligned
block buffer is now a stack local inside `pg_prewarm()`, because it is only
used while one function invocation reads blocks synchronously.

The remaining contrib `pg_plan_advice` advice-generation counter is not an
execution-local value: the exported API requests advice generation for future
planning until the caller revokes it. This slice therefore reclassifies
`pgpa_planner_generate_advice` as session-local instead of moving it into
`PgExecution`.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `trigger.o`,
  `pg_prewarm.o`, `pgpa_planner.o`, and `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 134 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and 32 execution-local declarations;
- direct `PG_GLOBAL_EXECUTION` scan now reports only the runtime bridge/early
  fallback objects plus the two `xact.c` transaction-stack roots;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- `gmake -C contrib -j8` passed after cleaning stale `pg_plan_advice` and
  `pg_prewarm` objects that still referenced prior exported-global symbols;
- after forcing rebuild of `launch_backend.o` and the touched
  runtime/trigger/contrib/test objects, full `gmake -j8`,
  `gmake -j8 install DESTDIR="$PWD/tmp_install"`, and reinstalling
  `src/test/modules/test_backend_runtime` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with patched macOS install names.

## Transaction Stack Execution State

The next Phase 12 transaction-stack slice moves the last non-runtime
`PG_GLOBAL_EXECUTION` declarations out of `xact.c`:

- `TopTransactionStateData`;
- `CurrentTransactionState`.

`TransactionStateData` remains private to `xact.c`. `PgExecutionXactState`
stores only opaque root/current pointers, and `xact.c` exposes its historical
names as compatibility macros over private helper functions. The top
transaction state is allocated lazily in `TopMemoryContext` for the current
execution bridge; subtransaction entries remain allocated by the existing
`PushTransaction()` path under `TopTransactionContext`.

This preserves the transaction manager's internal type boundary while making
the transaction-stack root and current pointer part of the explicit execution
object. The full teardown story is still tied to the broader Gate E2 destroy
audit: today the top transaction state has the same practical lifetime as the
old process-global static object, while subtransaction nodes continue to be
owned by transaction cleanup.

Validation for this slice:

- touched-object builds passed for `xact.o`, `backend_runtime.o`, and
  `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 134 fields classified after
  updating the lifecycle manifest for the new opaque transaction-stack
  pointers;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- direct `rg` confirmed no remaining non-runtime `PG_GLOBAL_EXECUTION`
  declarations under `src/backend/access/transam/xact.c`;
- full `gmake -j8` passed after explicitly rebuilding stale objects removed
  during forced layout validation;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  the expanded `test_execution_xact_state_is_execution_local()` checks for the
  top/current transaction-state pointers;
- `gmake -C contrib -j8` passed;
- forced object rebuild/install passed for `launch_backend.o`,
  `backend_runtime.o`, `xact.o`, and `test_backend_runtime.o`, followed by
  `gmake -j8 install DESTDIR="$PWD/tmp_install"` and reinstalling
  `src/test/modules/test_backend_runtime`;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with patched macOS install names.

## Thread Runtime Object Initialization Lists

The next Gate E2 hardening slice removes the large duplicated initialization
lists from `InitializePgThreadBackendRuntimeState()`:

- `PgBackendInitializeRuntimeObject()` wires a backend into its runtime,
  carrier, session, connection, and execution, then initializes every backend
  bucket through the same per-bucket helper functions used elsewhere;
- `PgSessionInitializeRuntimeObject()` wires the session and initializes the
  full session bucket set, including list-bearing state such as plan-cache
  lists;
- `PgConnectionInitializeRuntimeObject()` wires the connection, preserves the
  constructor `Port`, and initializes socket/protocol/output/startup/security
  buckets;
- `PgExecutionInitializeRuntimeObject()` wires the execution and initializes
  the execution bucket set.

Thread backend installation still calls `PgBackendAdoptEarlyState()`,
`PgSessionAdoptEarlyState()`, `PgConnectionAdoptEarlyState()`, and
`PgExecutionAdoptEarlyState()` after the constructor. That keeps early fallback
adoption centralized in the adoption helpers while avoiding a separate
constructor-side list that can drift as new runtime buckets are added.

Validation for this slice:

- touched-object build passed for `backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 134 fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `gmake -C src/test/modules/test_backend_runtime check` passed, covering the
  existing thread-install adoption tests for backend, session/execution, and
  connection fallback state;
- full `gmake -j8` passed;
- `gmake -C contrib -j8` passed;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  87 tests with patched macOS install names after cleaning a stranded failed
  `tmp_check`/`log` run.

## Session Timezone-Abbreviation Cache

The next session datetime slice moves `datetime.c`'s active
`TimeZoneAbbrevTable` pointer and recent timezone-abbreviation lookup cache
into `PgSessionDateTimeState`. The table pointer remains borrowed from the
`timezone_abbreviations` GUC extra value; the inline cache is session scratch
cleared by `InstallTimeZoneAbbrevs()` and `ClearTimeZoneAbbrevCache()`.
`datetime.c` keeps its existing local names through accessors over the current
session object, and `test_session_datetime_state_is_session_local()` now
proves the table pointer and cache entry do not leak between logical sessions.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `datetime.o`, and
  `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed after updating the lifecycle
  manifest with the borrowed-pointer and inline-cache copy rule;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- full `gmake -j8`, `test_backend_runtime` regression, contrib build, and
  direct threaded runtime TAP were run before commit.

## Session Logical Replication Caches

The next session-cache batch moves logical replication's remaining
session-local cache roots into `PgSessionLogicalReplicationState`:

- replication-origin's borrowed `ReplicationState` pointer;
- subscriber relation-map and partition-map memory contexts and hashes;
- `pgoutput` publication validity and relation sync hash;
- sync-worker relation-state validity.

The relation-map contexts own their hash tables and cache entries, and
`PgSessionResetClosedState()` deletes those contexts or fallback hashes on
session close. The `pgoutput` relation sync hash remains a session cache and is
destroyed on either output-plugin shutdown or closed-session reset. The
replication-origin pointer is different: it is a borrowed shared-memory slot
with refcount semantics, so cleanup remains owned by
`replorigin_session_reset()` and `ReplicationOriginExitCleanup()` rather than
being silently cleared by the generic session reset path.

Early adoption for this bucket asserts all pointer/hash/context slots are
empty. That keeps startup adoption explicit and prevents silently carrying
logical-replication caches that were accidentally populated before a real
session object existed. `test_session_datetime_state_is_session_local()` now
also switches fake sessions and verifies the logical-replication pointer/hash
slots do not leak between logical sessions.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `origin.o`,
  `relation.o`, `syncutils.o`, `pgoutput.o`, and `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 138 fields classified after
  adding the logical-replication lifecycle row;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 180 to 173;
- clean backend rebuild plus full `gmake -j8` passed after changing
  `PgSession` layout;
- `gmake -C contrib -j8`, `gmake -C src/test/modules/test_backend_runtime
  check`, and direct threaded runtime TAP all passed before commit.

## Session Catalog Lookup Caches

The next larger session-cache batch moves several catalog lookup and
formatting support roots into a single `PgSessionCatalogLookupState` bucket:

- attribute-options cache hash from `attoptcache.c`;
- relfilenumber-to-OID cache hash and its reusable scan-key array from
  `relfilenumbermap.c`;
- tablespace-options cache hash from `spccache.c`;
- event-trigger cache hash, cache context, and cache-state flag from
  `evtcache.c`;
- retained ruleutils SPI plans for rewrite/view lookups from `ruleutils.c`;
- ICU database-encoding converter slot from `pg_locale_icu.c`.

The ownership rule is explicit. Early fallback adoption transfers these cache
roots as a whole through `PgSessionAdoptEarlyState()` and zeros the fallback
bucket, so process and threaded install cannot drift. Closed-session reset
destroys hash roots, deletes the event-trigger cache context, frees retained
ruleutils SPI plans, closes the ICU converter through `pg_locale_icu.c`, and
clears the inline relfilenumber scan keys.

This is still a bridge over the broader memory-context blocker. Some entries
can point at allocations under `CacheMemoryContext`; this slice moves the
session roots and documents that the pointed allocation lifetime remains tied
to the later cache-memory-context split rather than claiming full reclamation
today. `test_session_catalog_lookup_state_is_session_local()` switches fake
sessions and verifies the moved pointer, context, plan, state, and inline
scan-key slots remain isolated.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `attoptcache.o`,
  `relfilenumbermap.o`, `spccache.o`, `evtcache.o`, `ruleutils.o`,
  `pg_locale_icu.o`, and `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 139 fields classified after
  adding the catalog-lookup lifecycle row;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 173 to 164;
- clean backend rebuild plus full `gmake -j8` passed after changing
  `PgSession` layout;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` and `gmake -C contrib -j8`
  passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed after updating
  the expected output for the added runtime test;
- direct threaded runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  88 tests.

## PL/pgSQL Session Extension State

The next Phase 12 in-tree extension batch moves PL/pgSQL's remaining
session-local globals behind an explicit per-session private-state pointer
owned by `PgSessionExtensionModuleState`:

- PL/pgSQL custom-GUC backing variables and assign-hook derived flags;
- compiler scratch state, datum arrays, error context name, current compile
  pointer, and compile memory-context pointer;
- parser identifier lookup mode and namespace stack;
- plugin rendezvous pointer and per-session initialization flag;
- shared simple-expression EState/resource-owner roots and econtext stack;
- session-wide cast expression and cast execution hash roots.

Core deliberately does not expose PL/pgSQL private structs. `PgSession` owns an
opaque `plpgsql_state` pointer plus a reset-callback list. PL/pgSQL allocates
its concrete `PLpgSQL_session_state` under the session dynamic-library context
on first use, registers `plpgsql_reset_session_state()`, and then uses
lvalue-compatible macros over that state so existing PL/pgSQL code keeps its
source-local names.

The lifecycle rule is explicit. Early fallback adoption transfers the opaque
private-state pointer and callback list through `PgSessionAdoptEarlyState()`.
Normal transaction cleanup remains owned by PL/pgSQL's xact/subxact callbacks.
Closed-session reset invokes registered module callbacks before deleting
`dynamic_library_context`; the PL/pgSQL callback releases any leftover simple
expression executor state, releases plan-cache refs from the shared simple
expression resource owner, frees cached cast expressions, destroys the cast
hash roots, and scrubs the session struct back to defaults.

This removes the direct PL/pgSQL `PG_GLOBAL_SESSION` declarations while
establishing the in-tree route for extension-owned session state without
making `backend_runtime.h` depend on PL/pgSQL internals.

Validation for this slice:

- a static scan found no remaining direct `PG_GLOBAL_SESSION` declarations
  under `src/pl/plpgsql/src`;
- touched-object builds passed for `backend_runtime.o`, the PL/pgSQL objects,
  and `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 140 fields classified after
  adding the extension-module lifecycle row;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- a clean backend rebuild plus full `gmake -j8` passed after changing
  `backend_runtime.h` and `plpgsql.h`;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"`, `gmake -C contrib -j8`,
  and `gmake -C src/pl/plpgsql/src all` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_session_extension_module_state_is_session_local()`;
- direct threaded runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` passed all
  88 tests.

## Runtime Constructor And Lifecycle Gate Hardening

The next Gate E2 hardening slice removes another manual process/thread
runtime drift point and strengthens the lifecycle checker:

- `InitializePgProcessRuntime()` now uses the same
  `PgBackendInitializeRuntimeObject()`, `PgSessionInitializeRuntimeObject()`,
  `PgConnectionInitializeRuntimeObject()`, and
  `PgExecutionInitializeRuntimeObject()` helpers as
  `InitializePgThreadBackendRuntimeState()`;
- process-mode setup and threaded setup still differ in the physical runtime
  object and connection `Port`, but object construction now has a single
  implementation path for backend/session/connection/execution fields;
- `gmake check-runtime-lifecycles` now verifies that manifest-referenced
  runtime lifecycle functions are defined in the checked runtime sources;
- the same checker now asserts the process and thread construction/adoption
  entrypoints call the required object constructor and top-level early-adoption
  helpers.

This directly addresses the Gate E2 adoption-list risk. Adding a field to
`PgBackend`, `PgSession`, `PgConnection`, or `PgExecution` still requires an
explicit manifest row, but the gate now also catches stale function names and
loss of the shared constructor/adoption shape.

This does not make the large `PgBackend` object the final ownership model.
`PgBackend` remains the Phase 12 consolidation bridge for backend-local state
while the branch proves initialization, early adoption, reset/destroy, and
copy rules bucket by bucket. Likewise, `PgSession.legacy_session` remains a
narrow compatibility endpoint for the existing `access/session.h` payload
until that payload is folded fully into `PgSession` or deliberately retained
as a subobject with this documented lifetime.

Validation for this slice:

- `gmake check-runtime-lifecycles` passed with 140 fields classified after the
  checker was tightened.

## Tcop Session Protocol State

The next Phase 12 session-state batch moves the remaining `postgres.c`
session-local command/protocol storage into a new `PgSessionTcopState` bucket:

- the unnamed prepared statement plan-source pointer;
- the interactive `-E` query echo switch;
- the interactive `-j` semicolon/newline mode switch;
- the reused `RowDescriptionContext` and `StringInfoData` buffer used by
  Describe/RowDescription messages.

`postgres.c` keeps its historical local names as lvalue macros over runtime
accessors. The switch accessors support early fallback storage because
single-user startup can parse `-E` and `-j` before a `PgSession` is installed.
`PgSessionAdoptEarlyTcopState()` transfers those scalar fallback values into
the session and asserts that plan/buffer/context pointers are still empty
before early adoption. Normal SQL startup creates the row-description context
under the installed session state.

The lifecycle rule is explicit. `PgSessionResetClosedState()` drops any
leftover unnamed cached plan and deletes the row-description context, clearing
the retained buffer afterward. The context/buffer and unnamed plan pointer are
session-owned pointer slots and must never be shallow-copied between live
sessions.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `postgres.o`, and
  `test_backend_runtime.o`;
- because `backend_runtime.h` changed installed backend-state declarations, the
  backend clean plus generated-header recovery path was used before the clean
  rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- `gmake -C contrib -j8` passed;
- `gmake check-runtime-lifecycles` passed with 141 fields classified after
  adding the `PgSession.tcop` lifecycle row;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `git diff --check` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_session_tcop_state_is_session_local()`;
- direct `prove` over
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  passed all 94 tests.

## RI Cache, Loaded Relmap, And Session Flag State

The next larger Phase 12 session-cache batch moves these remaining
session-owned globals behind `PgSession`:

- RI trigger constraint/query/compare cache roots and valid-entry list from
  `ri_triggers.c`;
- the `debug_discard_caches` GUC backing variable from `inval.c`;
- relation mapper loaded shared/local map files from `relmapper.c`;
- the `update_process_title` GUC backing variable from `ps_status.c`.

The active and pending relation-map transaction update files remain in
`PgExecutionRelMapState`; this slice only moves the loaded shared/local maps
that are session cache state. Existing source names remain lvalue macros over
runtime accessors, including the generated GUC-table bindings for
`debug_discard_caches` and `update_process_title`.

The lifecycle rule is explicit. `PgSessionResetClosedState()` destroys the RI
hash roots, reinitializes the RI valid-entry list, resets
`debug_discard_caches`, and resets the loaded relation maps to unloaded empty
state. The RI query cache may point to saved SPI plans whose memory remains
part of the broader SPI/cache-memory ownership split; that limitation is
recorded in `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` rather than hidden as an
implicit shallow-copy rule.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `ri_triggers.o`,
  `inval.o`, `relmapper.o`, `ps_status.o`, and `test_backend_runtime.o`;
- after the installed `backend_runtime.h` change, backend generated headers
  were regenerated and a clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` and `gmake -C contrib -j8`
  passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed with the new
  RI/session relmap helpers and still reported TAP disabled by configure;
- direct threaded-runtime TAP
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  passed all 94 tests with the local `IPC::Run` Perl dependency;
- `gmake check-runtime-lifecycles` passed with 145 fields classified after
  adding the `PgSession.ri_globals` and `PgSession.relmap` lifecycle rows;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 157 to 149;
- `git diff --check` passed.

## Xact Callback And SQL Backup Session State

The next Phase 12 session-state batch moves two utility/session-lifecycle
groups behind `PgSession`:

- add-on transaction and subtransaction callback list heads from `xact.c`;
- SQL-callable online backup state from `xlogfuncs.c` and the matching
  session backup status from `xlog.c`.

`xact.c`, `xlogfuncs.c`, and `xlog.c` keep their historical source-local
names as lvalue macros over runtime accessors. Early fallback storage remains
available before a `PgSession` is installed and is transferred by
`PgSessionAdoptEarlyState()`.

The lifecycle rule is explicit. `PgSessionResetClosedState()` calls
`ResetXactCallbackState()` with the closing session installed, freeing any
remaining callback list nodes allocated in `TopMemoryContext`. Callback
arguments remain owned by the registering subsystem. For SQL backup state,
closed-session reset aborts a running backup through `do_pg_abort_backup()`,
then deletes `backup_context` and clears the retained `backup_state`,
`tablespace_map`, and status fields. The backup context owns the backup
payload pointers; the status field is scalar session state mirrored with
shared WAL backup counters.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `xact.o`, `xlog.o`,
  `xlogfuncs.o`, and `test_backend_runtime.o`;
- `gmake check-runtime-lifecycles` passed with 143 fields classified after
  adding the `PgSession.xact_callbacks` and `PgSession.backup` lifecycle
  rows;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 161 to 157;
- `git diff --check` passed;
- because `backend_runtime.h` changed installed backend-state declarations, the
  backend clean plus generated-header recovery path was used before the clean
  rebuild;
- clean full `gmake -j8` passed;
- `gmake -j8 install DESTDIR="$PWD/tmp_install"` passed;
- `gmake -C contrib -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_session_xact_callback_state_is_session_local()` and
  `test_session_backup_state_is_session_local()`;
- direct `prove` over
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  passed all 94 tests.

## Central GUC Registry Session State

The next Phase 12 GUC-state batch moves the central `guc.c` registry and
transaction/reporting state behind `PgSession`:

- `GUCMemoryContext`;
- the copied built-in/custom GUC record array and count;
- the GUC name hash table;
- the non-default, stack, and report list heads;
- `reporting_enabled`;
- `GUCNestLevel`.

`guc.c` keeps its historical local names as lvalue macros over runtime
accessors. The generated built-in GUC table and custom extension definitions
therefore attach to the current logical session's registry instead of a raw
thread-local process bucket.

The lifecycle rule is explicit. `PgSessionResetClosedState()` deletes the
session's `GUCMemoryContext`, which owns the copied GUC records, hash table,
custom variable records, stack entries, and GUC list nodes, then reinitializes
the bucket. Early fallback adoption transfers the owning GUC context before
the GUC-backed datetime, text-search, and connection string buckets are copied,
so those copied string pointers stay owned by the destination session. After
that transfer, the detached early datetime/text-search/connection string
buckets are left uninitialized and NULL rather than allocating new strings
while runtime installation is still in progress. A fresh fallback GUC context
is only created on demand if later pre-session code explicitly allocates
through the GUC fallback again. This also prevents later GUC metadata paths
from freeing non-owned fallback default strings.

Threaded GUC setup, mutation, and display now run under a temporary
process-wide GUC critical section. This is a Phase 12 correctness bridge while
copied GUC metadata and check/assign/show hooks still include process-era
assumptions. It should be narrowed only after the remaining GUC-backed
session/execution globals have explicit owners and direct concurrent GUC
mutation/display smokes are in place.

Threaded nondefault replay skips `PGC_POSTMASTER` and `PGC_INTERNAL` records.
Those values are already present in runtime-global storage because thread
carriers share the postmaster address space. Replaying them through a session
GUC context can replace or free strings owned by the postmaster's GUC context,
while session/backend/user GUCs still need replay to match forked-child
semantics.

This slice also fixes a previously hidden shallow-copy hazard: fallback dlist
and dclist heads are retargeted when moved. The GUC non-default dlist and the
RI valid-entry dclist no longer retain self-pointers to the old fallback head
after adoption.

Validation of the threaded TAP exposed a separate teardown hazard in the
backend memory-manager bucket. Startup threads can return AllocSet contexts to
their backend-local freelists while the thread's `TopMemoryContext` tree is
still retained for post-exit accounting. Freeing those freelists during
threaded `PgBackendResetClosedState()` can therefore double-free retained
context headers. The reset path now remains destructive in process mode, but
threaded mode clears the freelist bucket and leaves full retained
`TopMemoryContext` reclamation as the explicit follow-up owner.

The same validation originally suggested widening the startup serialization
gate around thread-carrier startup, but the broad gate was rejected after TAP
validation showed that it can deadlock normal threaded startup behind worker
paths that have not reached `ThreadedBackendStartupComplete()`. Startup
serialization is now helper-controlled rather than unconditional: a backend
type may opt in through `backend_thread_requires_startup_gate()` only when it
has a named shared-state dependency and a stress test proving that the gate is
released. The remaining early fallback, GUC replay, runtime installation, and
backend initialization paths therefore remain Gate E2 audit targets instead of
being hidden behind a process-wide startup lock.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `guc.o`, and
  `test_backend_runtime.o`;
- clean full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed, including
  `test_session_guc_state_is_session_local()`;
- `gmake check-runtime-lifecycles` passed with 146 fields classified after
  adding the `PgSession.guc` lifecycle row;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 149 to 141;
- `git diff --check` passed.

## Central GUC Registry Follow-Up Hardening

Follow-up Gate E2 validation of the central GUC-registry batch found two
adjacent lifecycle bugs.

First, `CurrentPgRuntime` was still a process-global current binding while the
other current runtime objects were carrier/thread local. Installing a threaded
backend could therefore change the postmaster main thread's current-runtime
view. The binding is now carrier/thread local; the process and threaded
runtime objects remain runtime-global.

Second, `reserved_class_prefix` is runtime-global extension metadata, not
session GUC registry state. After `GUCMemoryContext` moved into `PgSession`,
`MarkGUCPrefixReserved()` was allocating runtime-global list cells in whichever
session loaded an extension module. A threaded backend FATAL could destroy
that session context, leaving PL/pgSQL's later GUC prefix reservation with a
list head pointing into freed memory. Reserved GUC prefix nodes now live in a
dedicated `TopMemoryContext` child and `MarkGUCPrefixReserved()` mutates that
global list under the temporary threaded GUC critical section.

Validation for this follow-up:

- the PL/pgSQL-after-FATAL crash was reproduced under lldb, with the fault in
  `MarkGUCPrefixReserved()` -> `lappend()` -> `repalloc()`;
- touched-object build for `guc.o` passed;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct `prove` over
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  passed all 94 tests after the fix.

## Portal And Regex Session Cache State

The next Phase 12 session-cache batch moves another coherent group of
session-owned cache roots out of standalone TLS and behind `PgSession`:

- `portalmem.c` now stores `TopPortalContext`, the portal hash table, and the
  unnamed-portal name counter in `PgSessionPortalManagerState`;
- `regexp.c` now stores the compiled-regexp cache context, fixed cached-entry
  array, cached-entry count, and existing regex ctype cache list in
  `PgSessionRegexState`.

Both source files keep their historic local names as lvalue macros over
runtime accessors. That keeps the behavioral code largely unchanged while the
owning storage moves with the logical session instead of the carrier thread.

The lifecycle rule is explicit. `PgSessionResetClosedState()` deletes the
compiled-regexp cache context before resetting the inline compiled-regexp
array/count and freeing the ctype cache list. It also deletes
`TopPortalContext`, which owns portal structs, portal contexts, hold contexts,
and the portal hash table, then clears the unnamed-portal counter. Early
fallback adoption moves both buckets as whole objects and reinitializes the
fallback storage; these pointer-bearing buckets must not be shallow-copied
between concurrently live sessions.

This leaves the heavier catalog cache groups as the main remaining
session-cache migration surface: `syscache.c`, `catcache.c`, `relcache.c`,
`typcache.c`, and the LLVM/JIT provider caches. Those should be handled in
larger subsystem batches because their reset rules involve invalidation,
cache-memory-context ownership, and backend startup ordering.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `portalmem.o`, and
  `regexp.o`, and postmaster startup-publication object coverage passed for
  `launch_backend.o`, `pmchild.o`, and `postmaster.o`;
- full `gmake -j8` passed;
- `test_session_regex_portal_state_is_session_local()` covers the new
  `PgSession` accessors for fake sessions;
- `gmake check-runtime-lifecycles` passed with 147 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 141 to 137;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct `prove` over
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` and
  `src/test/modules/test_backend_runtime/t/002_threaded_bgworker_crash.pl`
  passed all 94 tests after the startup-publication fix and after rejecting
  the broad startup gate.

## Thread Startup Publication Latch Race

The portal/regex validation pass also exposed a Gate E2 synchronization bug in
the startup handoff path. A startup-era thread carrier can reach
`ThreadedBackendStartupComplete()` before the postmaster has configured its
server-loop wait set and recorded a postmaster latch pointer for direct wakeup.
The previous PMChild publication helper asserted and called `SetLatch()` on
that pointer, so threaded normal startup could crash in `SetLatch(NULL)`
immediately after logging "starting startup thread carrier".

PMChild startup/exit publication is now NULL-latch tolerant. It always records
the atomic startup/exit state, wakes the supplied latch when one exists, and
falls back to the postmaster signal wake path otherwise. The postmaster server
loop now drains thread startup and exit publications before each blocking wait,
so a publication that happened before the wait set existed is consumed without
sleeping until the next timeout.

This is part of Gate E2 because it tightens PMChild/thread-backend
synchronization rather than adding a new scheduling feature.

## Syscache And Catcache Session Roots

The next Phase 12 cache-state batch moves the syscache and catcache root
storage behind `PgSessionCatalogLookupState`:

- `syscache.c` now stores the `SysCache[]` array, initialization flag, and
  relation/supporting-relation OID arrays in `PgSession`;
- `catcache.c` now stores the `CacheHdr` root pointer in `PgSession`.

Both files keep their historic local names as macros over runtime accessors.
The catcache entries, bucket arrays, copied tuples, and cache descriptors
still live under `CacheMemoryContext`; this slice moves the session-visible
roots that select a logical session's catalog-cache universe. It deliberately
does not move `CacheMemoryContext`, relcache roots, typcache roots, or
`funccache.c` yet. `funccache.c` needs an explicit iterator/destructor before
its hash root can move safely, because hash entries can own copied tuple
descriptors and language-specific cached-function state.

The lifecycle rule is explicit. Early fallback adoption moves the root arrays
and header pointer as part of `PgSessionCatalogLookupState`. Session reset
clears those roots after destroying the smaller catalog lookup hash/context
roots and retained ruleutils SPI plans; entry memory remains owned by the
broader `CacheMemoryContext` ownership split that is still a Phase 12 cache
work item. These root pointers must not be shallow-copied between concurrently
live sessions.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `syscache.o`,
  `catcache.o`, and `test_backend_runtime.o`;
- `test_session_catalog_lookup_state_is_session_local()` now covers syscache
  arrays, syscache OID-array counters, and the catcache header pointer across
  fake-session switching;
- `gmake check-runtime-lifecycles` passed with 147 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 137 to 130.

## Relcache Session Roots

The next Phase 12 cache-state batch moves the remaining relcache session root
storage behind `PgSessionCatalogLookupState`:

- `RelationIdCache` now resolves through `PgCurrentRelationIdCacheRef()`;
- `criticalRelcachesBuilt` and `criticalSharedRelcachesBuilt` remain visible
  to `relcache.c`, `catcache.c`, and `postinit.c` through the historical names,
  but those names are now `relcache.h` accessors into the current `PgSession`;
- `relcacheInvalsReceived` now resolves through
  `PgCurrentRelcacheInvalsReceivedRef()`;
- `OpClassCache` now resolves through `PgCurrentOpClassCacheRef()`.

This slice moves the session-owned root pointers and scalar build/invalidation
state that select the logical session's relcache universe. It deliberately does
not claim ownership of relation descriptors, opclass entry arrays, catcache
entries, tuple descriptors, or other allocations that still live under
`CacheMemoryContext` or existing relcache cleanup paths. Those allocations
remain part of the broader `CacheMemoryContext` split and explicit destructor
work required before Gate E2 can close.

The lifecycle rule is explicit. Early fallback adoption moves these fields as
part of the whole `PgSessionCatalogLookupState` bucket. Session reset clears
the root hashes, critical-cache flags, and invalidation counter after dependent
cache roots have been handled. The hash root pointers must not be shallow-copied
between concurrently live sessions.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `postinit.o`,
  `relcache.o`, `catcache.o`, `syscache.o`, and `test_backend_runtime.o`;
- `gmake -j8` passed after removing and rebuilding stale users of the former
  exported relcache critical-cache symbols;
- `test_session_catalog_lookup_state_is_session_local()` now covers relcache
  root hashes, critical-cache flags, and invalidation counter across
  fake-session switching;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` and
  `002_threaded_bgworker_crash.pl` with 94 tests;
- `gmake check-runtime-lifecycles` passed with 147 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 130 to 123.

## Typcache Session Roots

The next Phase 12 cache-state batch moves typcache root storage and counters
behind `PgSessionCatalogLookupState`:

- `TypeCacheHash` now resolves through `PgCurrentTypeCacheHashRef()`;
- `RelIdToTypeIdCacheHash` now resolves through
  `PgCurrentRelIdToTypeIdCacheHashRef()`;
- `firstDomainTypeEntry` now resolves through
  `PgCurrentFirstDomainTypeEntryRef()`;
- the typcache in-progress OID stack pointer, length, and capacity now resolve
  through `PgCurrentTypCacheInProgressList*` accessors;
- `RecordCacheHash`, `RecordCacheArray`, `RecordCacheArrayLen`, and
  `NextRecordTypmod` now resolve through record-cache accessors;
- `tupledesc_id_counter` now resolves through
  `PgCurrentTupleDescIdCounterRef()`, with
  `PgSessionInitializeCatalogLookupState()` preserving the historical
  `INVALID_TUPLEDESC_IDENTIFIER` starting value.

This slice removes typcache's direct `PG_GLOBAL_SESSION` declarations and
keeps `typcache.c` source-local names as runtime accessor macros. It moves the
session-visible root pointers and scalar counters only. The type-cache entries,
record-cache entries, tuple descriptors, domain constraint caches, in-progress
array allocation, and fmgr/hash subsidiary allocations continue to live under
`CacheMemoryContext` or existing typcache-specific reference-count rules. That
is intentional for this batch and remains part of the broader
`CacheMemoryContext` destructor split before Gate E2 can close.

The lifecycle rule is explicit. Early fallback adoption moves the whole
`PgSessionCatalogLookupState` bucket, including these typcache fields. Session
reset clears the typcache roots, stack pointer/counters, record-cache
array/counters, and resets the tupledesc counter to its historical sentinel.
The hash, domain-list, in-progress-array, and record-array pointers must not be
shallow-copied between concurrently live sessions.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`, `typcache.o`, and
  `test_backend_runtime.o`;
- `gmake -j8` passed;
- `test_session_catalog_lookup_state_is_session_local()` now covers typcache
  root hashes, domain-list pointer, in-progress stack pointer/counters,
  record-cache pointer/counters, and tupledesc counter across fake-session
  switching;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` and
  `002_threaded_bgworker_crash.pl` with 94 tests;
- `gmake check-runtime-lifecycles` passed with 147 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 123 to 112.
- `git diff --check` passed.

## Runtime Source Ownership Split

The next Phase 12 maintenance slice starts reducing `backend_runtime.c`
conflict concentration without changing behavior:

- cache/catalog compatibility accessors moved from
  `src/backend/utils/init/backend_runtime.c` to the fork-owned adjacent file
  `src/backend/utils/cache/backend_runtime_cache.c`;
- `backend_runtime.c` still owns root runtime construction, current-pointer
  installation, process/thread symmetry, and top-level adoption/reset
  orchestration;
- `src/backend/utils/init/backend_runtime_internal.h` exposes the small
  backend-private current-bucket helper needed by adjacent fork-owned runtime
  files, avoiding new installed-header surface for internal plumbing;
- `src/backend/utils/cache/Makefile` and `src/backend/utils/cache/meson.build`
  include the new source file;
- `check-runtime-lifecycles` now passes an explicit checked source list and the
  checker default source list includes `backend_runtime_cache.c`;
- `MULTITHREADED_RUNTIME_OWNERS.tsv` starts the symbol-level maintenance map
  from legacy globals to runtime object buckets, members, accessors, and owner
  source files.

This is a no-behavior-change organization proof. Future Phase 12 migrations
should put domain-specific accessors and simple lifecycle helpers in
fork-owned adjacent subsystem files, then extend the checked runtime source
list and owner map in the same commit. Semantic reset/destruction code should
remain handwritten near the subsystem that owns the resource and must still be
covered by `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`.

Validation for this slice:

- touched-object builds passed for `backend_runtime_cache.o` and
  `backend_runtime.o`;
- `perl -c src/tools/runtime_lifecycle/check_runtime_lifecycles.pl` passed;
- `gmake -j8` passed;
- `gmake check-runtime-lifecycles` passed with the explicit source list
  including `backend_runtime_cache.c`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations unchanged at 112;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` and
  `002_threaded_bgworker_crash.pl` with 94 tests.

## CacheMemoryContext Session Slot

The next Phase 12 cache-state slice moves the exported `CacheMemoryContext`
pointer slot behind `PgSessionCatalogLookupState`:

- `PgSessionCatalogLookupState` now owns `cache_memory_context`;
- `utils/memutils.h` and `utils/catcache.h` preserve the historical
  `CacheMemoryContext` lvalue name as a macro over `PgCacheMemoryContextRef()`;
- `backend_runtime_cache.c`, the cache-owned runtime bridge, implements the
  accessor instead of growing `backend_runtime.c`;
- `mcxt.c` no longer defines an independent session-local
  `CacheMemoryContext` TLS global;
- `PgSessionResetClosedState()` clears the active session cache-context slot after the
  dependent cache roots and retained plans handled earlier in the reset path
  have been cleared; non-current offline session reset can delete the context,
  but active backend teardown still leaves physical reclamation to the retained
  `TopMemoryContext` tree until that Gate E2 blocker is closed.

This is still a pointer-slot migration, not a full `TopMemoryContext` split.
The cache memory context remains an ordinary memory context under the existing
memory-context tree, but the pointer that selects a logical session's cache
allocation arena now follows `PgSession` alongside the syscache, catcache,
relcache, and typcache roots. `MULTITHREADED_RUNTIME_OWNERS.tsv` records the
symbol-level owner mapping for future rebases.

Validation for this slice:

- touched-object builds passed for `backend_runtime_cache.o`, `mcxt.o`,
  `backend_runtime.o`, and `test_backend_runtime.o`;
- a clean backend rebuild plus generated-header recovery and full `gmake -j8`
  passed after changing the installed `memutils.h`/`catcache.h` declarations;
- stale PL/pgSQL initially failed temp-install `initdb` by referencing the
  removed `_CacheMemoryContext` symbol, matching the documented installed
  header rebuild hazard; `gmake -C src/pl/plpgsql/src clean all` fixed it;
- `gmake check-runtime-lifecycles` passed with 147 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 112 to 109;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` and
  `002_threaded_bgworker_crash.pl` with 94 tests.

## Cached-Function Hash Session Root

The next Phase 12 cache-state slice moves `funccache.c`'s cached-function hash
root into `PgSessionFunctionManagerState`:

- `PgSessionFunctionManagerState` now owns both the existing fmgr C-function
  hash and `funccache.c`'s cached-function hash;
- `backend_runtime_cache.c` owns both `PgCurrentCFuncHashRef()` and
  `PgCurrentCachedFunctionHashRef()`, keeping function-manager cache accessors
  out of the central runtime/orchestration file;
- `funccache.c` keeps the semantic cleanup path through
  `DestroyCachedFunctionHash()`, because entries can own copied tuple
  descriptors and language-specific cached-function storage;
- session reset destroys the cached-function hash through that funccache-owned
  destructor and preserves the existing leak-on-active-use rule for recursive
  active invocations;
- `MULTITHREADED_RUNTIME_OWNERS.tsv` records the symbol-level owner mapping for
  both function-manager cache roots.

This closes the specific `funccache.c` blocker called out after the syscache,
catcache, relcache, typcache, and `CacheMemoryContext` root moves. It does not
solve the broader `TopMemoryContext` reclamation problem; cached-function
structs are still allocated in the existing memory-context tree, and full
active-backend memory-tree teardown remains part of Gate E2.

Validation for this slice:

- touched-object builds passed for `funccache.o`, `backend_runtime_cache.o`,
  `backend_runtime.o`, and `test_backend_runtime.o`;
- a stale `dfmgr.o` after the `PgSession` layout change crashed threaded TAP
  during `call_module_init_function()` by treating the old
  `dynamic_library_inits` offset as a `List`; the documented backend clean plus
  generated utility/node-header recovery fixed that stale-object failure;
- clean full `gmake -j8` passed after backend clean and generated-header
  recovery;
- `gmake -C src/pl/plpgsql/src clean all` and
  `gmake -C src/test/modules/test_backend_runtime clean all` passed after the
  installed runtime-header change;
- `gmake check-runtime-lifecycles` passed with 147 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 109 to 108;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` and
  `002_threaded_bgworker_crash.pl` with 94 tests using the local `IPC::Run`
  `PERL5LIB` and explicit `PG_REGRESS` harness environment.

## Pgstat Runtime Owner File

The next organizational slice keeps Phase 12 from concentrating all runtime
bridge code in `backend_runtime.c`:

- `src/backend/utils/activity/backend_runtime_pgstat.c` now owns the
  compatibility accessors for `PgSessionPgStatState` and
  `PgBackendPgStatPendingState`;
- `backend_runtime.c` retains only the private current-bucket helpers plus
  initialization, early adoption, process/thread construction symmetry, and
  top-level reset orchestration;
- `backend_runtime_internal.h` exposes the small backend-private current-bucket
  helpers needed by this adjacent owner file, without adding installed-header
  plumbing;
- `src/backend/utils/activity/Makefile`, `src/backend/utils/activity/meson.build`,
  the `check-runtime-lifecycles` top-level target, and the checker default
  source set include the new owner file;
- `MULTITHREADED_RUNTIME_OWNERS.tsv` now maps pgstat legacy symbols to their
  runtime bucket/member/accessor and owner source.

This is a no-behavior-change organization proof. It does not migrate new
globals or change pgstat lifecycle semantics; it makes the owner-file pattern
concrete before more Phase 12 bucket moves.

Validation for this slice:

- touched-object builds passed for `backend_runtime_pgstat.o` and
  `backend_runtime.o`;
- `perl -c src/tools/runtime_lifecycle/check_runtime_lifecycles.pl` passed;
- full `gmake -j8` passed;
- `gmake check-runtime-lifecycles` passed with the explicit source list
  including `backend_runtime_pgstat.c`;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and unchanged session-local count of 108;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` with 88
  tests and `002_threaded_bgworker_crash.pl` with 6 tests using the local
  `IPC::Run` `PERL5LIB` and explicit `PG_REGRESS` harness environment. An
  initial combined `prove` run saw `001_threaded_runtime.pl` lose the temp
  postmaster immediately after startup without reaching SQL; rerunning the two
  files individually passed cleanly.

## JIT Provider Session Cache

The next Phase 12 state-migration slice moves the provider-independent JIT
callback cache and load-status flags into `PgSession`:

- `PgSessionJitProviderState` owns the `JitProviderCallbacks` table plus the
  successful-load and failed-load booleans formerly stored as raw
  session-local TLS in `src/backend/jit/jit.c`;
- `src/backend/jit/backend_runtime_jit.c` owns the compatibility accessors,
  keeping JIT-specific bridge code out of `backend_runtime.c`;
- `jit.c` preserves the historical local names through macros over the current
  session bucket;
- `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` records that the bucket owns only the
  callback slots and flags. Dynamic library handles remain dfmgr-owned, and
  provider-private LLVM resources remain with the provider callbacks until the
  LLVM-specific cache migration is handled under `src/backend/jit/llvm`;
- `MULTITHREADED_RUNTIME_OWNERS.tsv` maps `provider`,
  `provider_successfully_loaded`, and `provider_failed_loading` to the new
  bucket and owner file;
- `test_backend_runtime` now checks that the provider callback table and flags
  are session-local across fake sessions.

This removes the provider-independent JIT TLS declarations from
`src/backend/jit/jit.c`. It deliberately does not move the LLVM provider's
larger type/module/context cache, because this checkout is configured without
LLVM and that work needs an LLVM-enabled compile/test pass.

Validation for this slice:

- touched-object builds passed for `backend_runtime_jit.o`, `jit.o`,
  `backend_runtime.o`, and `test_backend_runtime.o`;
- clean full `gmake -j8` passed after backend clean and generated-header
  recovery for the installed runtime-header change;
- `gmake -C src/pl/plpgsql/src clean all` and
  `gmake -C src/test/modules/test_backend_runtime clean all` passed;
- `gmake check-runtime-lifecycles` passed with 148 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 108 to 106;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` with 88
  tests and `002_threaded_bgworker_crash.pl` with 6 tests using the local
  `IPC::Run` `PERL5LIB` and explicit `PG_REGRESS` harness environment;
- `gmake -C contrib -j8` passed after the installed runtime-header change.

## LLVM Provider Session Cache

The next Phase 12 JIT state-migration slice moves the LLVM provider-private
session cache into `PgSession`:

- `PgSessionLLVMJitState` now owns LLVM type refs, template refs,
  `llvmjit_types.bc` module metadata, context reuse counters, target/triple
  data, the reusable LLVM context, thread-safe context, and ORC JIT instances;
- `src/include/jit/llvmjit_runtime.h` provides the lightweight LLVM-facing
  state definition and `PgCurrentLLVMJitState()` declaration without exposing
  the large `backend_runtime.h` installed header to LLVM C/C++ headers;
- `llvmjit.h` preserves the historical type/template names as macros over the
  current session's LLVM state bucket, while `llvmjit.c` preserves its private
  provider names through local macros;
- `src/backend/jit/backend_runtime_jit.c` owns the public
  `PgCurrentLLVMJitState()` accessor, matching the provider-independent JIT
  bridge pattern and keeping this domain accessor out of `backend_runtime.c`;
- `backend_runtime.c` initializes and adopts the LLVM bucket alongside other
  `PgSession` buckets, with a single early fallback bucket for pre-session
  provider use;
- `MULTITHREADED_RUNTIME_OWNERS.tsv` maps the public type/template refs and
  private `llvm_*` provider cache names to `PgSession.llvm_jit`;
- `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` records the bucket's pointer-heavy
  copy/adoption rule: LLVM handles, modules, contexts, target refs, and
  type/template refs must not be shallow-copied between concurrently live
  sessions, and provider callbacks remain responsible for normal shutdown and
  context release.

The LLVM-enabled validation exposed one additional required compatibility
fix. JIT-generated IR still emitted a reference to the historical
`CurrentMemoryContext` global, but this branch now exposes it as an accessor
over runtime-owned state. `l_mcxt_switch()` now emits a call to
`PgCurrentMemoryContextRef()` and load/stores through the returned slot instead
of asking LLVM to resolve a removed global symbol.

LLVM 21 also requires keeping PostgreSQL's short historical macros out of
LLVM headers. `llvmjit.h` now undefines `AM`, `PM`, `TZ`, and `Mode` before
including LLVM C headers, and `llvmjit_inline.cpp` keeps its local guard for
LLVM C++ headers. The current LLVM validation configuration is:

```sh
./configure --without-icu --disable-rpath --with-llvm LLVM_CONFIG=/opt/homebrew/opt/llvm@21/bin/llvm-config
```

Validation for this slice:

- clean backend rebuild plus generated utility/node-header recovery passed
  after enabling LLVM;
- full `gmake -j8` passed;
- clean `gmake -C src/backend/jit/llvm all` passed with LLVM 21.1.8 and
  rebuilt `llvmjit.dylib` plus `llvmjit_types.bc`;
- `gmake -C src/pl/plpgsql/src clean all` and
  `gmake -C src/test/modules/test_backend_runtime clean all` passed after the
  installed runtime-header change;
- `gmake check-runtime-lifecycles` passed with 149 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 97 to 61;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` with 88
  tests and `002_threaded_bgworker_crash.pl` with 6 tests using the local
  `IPC::Run` `PERL5LIB` and patched build-tree `pg_regress` install name;
- an explicit LLVM smoke passed: `LOAD 'llvmjit'`, forced zero JIT thresholds,
  and a table-backed aggregate produced a plan with JIT functions in both the
  leader and a parallel worker.

## Compatibility GUC Session State

The next Phase 12 state-migration slice moves another GUC compatibility batch
from raw session-local TLS into existing `PgSession` buckets:

- `PgSessionGeneralGUCState` now owns `default_with_oids`,
  `standard_conforming_strings`, `seed`'s display-only
  `phony_random_seed`, and `session_authorization`'s string slot;
- `PgSessionConnectionGUCState` now owns the ignored
  `ssl_renegotiation_limit` compatibility scalar;
- `PgSessionDateTimeState` now owns the `DateStyle` and
  `timezone_abbreviations` GUC string slots;
- `PgSessionEncodingState` now owns the `client_encoding` and
  `server_encoding` display string slots;
- `src/backend/utils/misc/backend_runtime_guc.c` owns the compatibility
  accessors, keeping this GUC bridge out of `backend_runtime.c`;
- `guc_tables.c` preserves the historical local names through macros over the
  current session buckets, and `RebindSessionGUCVariablePointers()` retargets
  every moved built-in GUC after a session switch;
- `MULTITHREADED_RUNTIME_OWNERS.tsv` maps all nine legacy symbols to their
  runtime bucket/member/accessor and owner source.

This removes nine raw session-local TLS declarations from `guc_tables.c`.
The moved string slots still rely on existing GUC allocation/reset ownership;
the runtime buckets own pointer slots and scalar values, not a new string
destructor path.

Validation for this slice:

- touched-object builds passed for `backend_runtime_guc.o`, `guc.o`,
  `guc_tables.o`, `backend_runtime.o`, and `test_backend_runtime.o`;
- clean full `gmake -j8` passed after backend clean and generated-header
  recovery for the installed runtime-header change;
- `gmake -C src/pl/plpgsql/src clean all` and
  `gmake -C src/test/modules/test_backend_runtime clean all` passed;
- `gmake check-runtime-lifecycles` passed with 148 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 106 to 97;
- the raw TLS declaration search for this migrated batch found no remaining
  matches in `guc_tables.c`;
- `gmake -C src/test/modules/test_backend_runtime check` passed after adding
  `test_session_compat_guc_state_is_session_local()`;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` with 88
  tests and `002_threaded_bgworker_crash.pl` with 6 tests using the local
  `IPC::Run` `PERL5LIB` and explicit `PG_REGRESS` harness environment;
- `gmake -C contrib -j8` passed after the installed runtime-header change.

## Legacy Session And Connection Scratch Cleanup

This Phase 12 Gate E2 cleanup removes three more raw compatibility globals
from plain TLS storage and gives their lifecycle rules explicit runtime owners:

- `CurrentSession` is now an lvalue macro over
  `PgCurrentLegacySessionRef()`, so the legacy `access/session.h` pointer slot
  lives in `PgSession.legacy_session` instead of a separate exported TLS
  variable;
- `ConnectionWarningsEmitted`, `ConnectionWarningMessages`, and
  `ConnectionWarningDetails` now live in `PgConnection.startup`, matching the
  fact that they are frontend-connection startup/authentication scratch;
- `ri_fastpath_xact_callback_registered` now lives in
  `PgSession.ri_globals`, alongside the other referential-integrity trigger
  session caches.

The list-bearing connection warning fields are reset by
`PgConnectionResetClosedState()`, which frees saved warning message/detail
lists and clears the connection-owned slots. `EmitConnectionWarnings()` also
clears the slots after freeing the lists so normal warning emission does not
leave dangling list pointers in the connection object. The RI fast-path
registration guard is scalar session state; it is initialized, adopted with
the rest of `PgSession.ri_globals`, and reset during
`PgSessionResetClosedState()`.

`MULTITHREADED_RUNTIME_OWNERS.tsv` now records symbol-level mappings for this
batch, and `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` documents the reset/copy
rules for the affected buckets.

Validation for this slice:

- touched-object builds passed for `postinit.o`, `backend_runtime.o`,
  `session.o`, `ri_triggers.o`, and `test_backend_runtime.o`;
- clean backend rebuild plus generated utility/node-header recovery passed;
- full LLVM-enabled `gmake -j8` passed;
- `gmake -C src/pl/plpgsql/src clean all` and
  `gmake -C src/test/modules/test_backend_runtime clean all` passed after the
  installed `access/session.h`/runtime-header change;
- `gmake check-runtime-lifecycles` passed with 149 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 61 to 58;
- the raw TLS declaration search for this migrated batch found no remaining
  matches;
- `gmake -C src/test/modules/test_backend_runtime check` passed after
  extending the RI/session and connection startup/reset tests;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` with 88
  tests and `002_threaded_bgworker_crash.pl` with 6 tests using the local
  `IPC::Run` `PERL5LIB` and patched build-tree `pg_regress` install name;
- `git diff --check` passed.

## Top Memory Context Pointer Slot

This Phase 12 state-migration slice moves the historical
`TopMemoryContext` pointer out of standalone session-local TLS and into the
existing `PgExecution.memory_contexts` bucket:

- `PgExecutionMemoryContextState` now owns the `top_context` pointer slot
  beside `CurrentMemoryContext`, `ErrorContext`, `MessageContext`,
  transaction context pointers, and `PortalContext`;
- `utils/memutils.h` preserves the historical `TopMemoryContext` lvalue name
  as a macro over `PgTopMemoryContextRef()`;
- `MemoryContextInit()` continues to create the same root context, but writes
  the pointer through the runtime-owned slot;
- `test_execution_memory_contexts_are_execution_local()` now verifies
  `TopMemoryContext` switches and resets with the rest of the execution
  memory-context pointer slots;
- `MULTITHREADED_RUNTIME_OWNERS.tsv` records the symbol-level mapping, and
  `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` documents that this is a pointer-slot
  migration only.

This does not close the broader Gate E2 `TopMemoryContext` reclamation
blocker. The pointed memory-context tree is still retained during
thread-backed backend exit and accounted through the existing PMChild exit
path until a safe deletion/reparenting model exists.

Validation for this slice:

- touched-object builds passed for `mcxt.o`, `backend_runtime.o`, and
  `test_backend_runtime.o`;
- the LLVM-enabled full `gmake -j8` build passed after the backend clean and
  generated-file recovery sequence;
- `gmake -C src/pl/plpgsql/src clean all`,
  `gmake -C src/test/modules/test_backend_runtime clean all`, and
  `gmake -C contrib -j8` passed after the installed header/runtime change;
- `gmake check-runtime-lifecycles` passed with 149 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and session-local declarations reduced from 58 to 56;
- the raw TLS declaration search found no remaining `TopMemoryContext`
  storage declaration;
- `gmake -C src/test/modules/test_backend_runtime check` passed after seeding
  fake execution memory-context slots in the test harness for allocation-heavy
  execution-local tests;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` with 88
  tests and `002_threaded_bgworker_crash.pl` with 6 tests using the local
  `IPC::Run` `PERL5LIB` and patched temporary-install dynamic library names.

## Worker SPI Wait-Event Cache Lifetime

This Phase 12 cleanup removes the last non-runtime raw
`PG_THREAD_LOCAL PG_GLOBAL_BACKEND` declaration outside
`src/backend/utils/init/backend_runtime.c` early-fallback storage:

- `worker_spi_wait_event_main` in `src/test/modules/worker_spi/worker_spi.c`
  is now `PG_GLOBAL_RUNTIME`;
- the value is a custom wait-event registry ID returned by
  `WaitEventExtensionNew("WorkerSpiMain")`, not per-backend mutable execution
  state;
- `WaitEventExtensionNew()` already rechecks the shared registry under the
  wait-event lock, so concurrent process/thread callers can converge on the
  same ID even if they race through the zero-cache fast path;
- this keeps opted-in in-tree test extension code from depending on raw
  backend-local TLS for shared registry metadata.

After this slice, the direct raw TLS scan for
`PG_THREAD_LOCAL PG_GLOBAL_BACKEND`, `PG_GLOBAL_SESSION`,
`PG_GLOBAL_CONNECTION`, and `PG_GLOBAL_EXECUTION` outside
`src/backend/utils/init/backend_runtime.c` has no matches. Future in-tree
module additions should use `PG_GLOBAL_RUNTIME` only for real runtime-wide
registry/cache values; backend/session/execution/connection state should use
an explicit runtime-object bucket.

Validation for this slice:

- `gmake -C src/test/modules/worker_spi worker_spi.o all` passed;
- `gmake -C src/test/modules/worker_spi check` refreshed the temp install
  and completed the non-TAP target for this non-`--enable-tap-tests` checkout;
- direct worker SPI TAP passed for `001_worker_spi.pl`; `002_worker_terminate.pl`
  skipped as expected because this checkout is configured without injection
  points;
- `gmake check-runtime-lifecycles` passed with 149 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- the focused raw TLS scan outside `backend_runtime.c` found no remaining
  matches.

## Win32 Socket Nonblocking Flag

This Phase 12 connection-state slice moves the Win32 socket emulation
nonblocking flag behind the `PgConnection.socket_io` bucket:

- `PgConnectionSocketIOState` now owns `win32_noblock`;
- `win32_port.h` preserves the historical `pgwin32_noblock` lvalue as a macro
  over `PgCurrentPgwin32NoBlockRef()`;
- `src/backend/port/win32/socket.c` no longer defines separate
  `PG_GLOBAL_CONNECTION` storage for the flag;
- the existing connection socket-I/O runtime tests now verify that the field
  switches with `CurrentPgConnection` and is cleared by
  `PgConnectionResetClosedState()`;
- `MULTITHREADED_RUNTIME_OWNERS.tsv` records the symbol-level mapping, and
  `MULTITHREADED_RUNTIME_LIFECYCLE.tsv` expands the socket-I/O bucket
  lifecycle note.

This is a best-effort Windows-port migration from the macOS build. The shared
runtime object, accessors, manifests, and non-Windows tests are validated here,
but the `win32_port.h` macro expansion and `src/backend/port/win32/socket.c`
compile path still need a Windows build before Phase 12 can claim platform
coverage for this specific symbol.

Validation for this slice:

- pending Windows build validation for `src/backend/port/win32/socket.c`;
- touched-object builds passed for `backend_runtime.o` and
  `test_backend_runtime.o`;
- `gmake -j8` passed on the LLVM-enabled macOS build;
- `gmake check-runtime-lifecycles` passed with 149 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and connection-local declarations reduced from 11 to 9;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` with 88
  tests and `002_threaded_bgworker_crash.pl` with 6 tests using the local
  `IPC::Run` `PERL5LIB` and patched temporary-install dynamic library names.

## Carrier GUC Critical-Section Depth

This Phase 12 carrier-state slice moves the temporary threaded GUC
critical-section reentrancy depth into the `PgCarrier` object:

- `PgCarrier` now owns `threaded_guc_mutex_depth`;
- `guc.c` preserves the local `ThreadedGUCMutexDepth` name as a macro over
  `PgCurrentThreadedGUCMutexDepthRef()`;
- the process-wide `ThreadedGUCMutex` remains runtime-global and unchanged;
- `test_carrier_threaded_guc_lock_depth_is_carrier_local()` verifies that two
  fake carriers do not share nested GUC lock depth.

This is an ownership cleanup for the current Gate E2 bridge, not the final GUC
architecture. Threaded GUC setup, mutation, and display still use the
temporary process-wide mutex while copied GUC metadata, direct-pointer
variables, check hooks, assign hooks, show hooks, database/role settings, and
custom extension GUC behavior continue to be audited. The systematic
per-session adoption/rebind model remains a Gate E2 blocker.

Validation for this slice:

- touched-object builds passed for `guc.o`, `backend_runtime.o`, and
  `test_backend_runtime.o`;
- the LLVM-enabled full `gmake -j8` build passed after force-rebuilding stale
  `src/backend/postmaster/launch_backend.o` for the
  `PgThreadBackendRuntimeState` layout change;
- `gmake check-runtime-lifecycles` passed with 149 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and carrier-local declarations reduced from 32 to 31;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` with 88
  tests and `002_threaded_bgworker_crash.pl` with 6 tests using the local
  `IPC::Run` `PERL5LIB` and patched temporary-install dynamic library names.

## Carrier Wait/Stack/Launch State

This Phase 12 carrier-state slice moves the targeted wait/stack/launch
carrier-local TLS globals into `PgCarrier`:

- `CurrentBackendThreadStart` is now an opaque `backend_thread_start` pointer
  in `PgCarrier`, reached through `PgCurrentBackendThreadStartRef()`;
- wait-event signal/self-pipe state (`waiting`, `signal_fd`,
  `selfpipe_readfd`, `selfpipe_writefd`, and `selfpipe_owner_pid`) now lives in
  `PgCarrier` behind IPC-owned accessors in
  `src/backend/storage/ipc/backend_runtime_ipc.c`;
- `stack_base_ptr` now lives in `PgCarrier` behind
  `PgCurrentStackBasePtrRef()` in
  `src/backend/utils/misc/backend_runtime_utility.c`;
- carrier initialization explicitly preserves the legacy `-1` descriptor
  sentinels for both process and thread carriers.

The backend-thread entry path installs the prebuilt thread carrier before
recording `backend_thread_start`, so early thread startup state does not fall
back to the process carrier before full `InstallPgThreadBackendRuntimeState()`.
`test_carrier_misc_state_is_carrier_local()` switches between fake carriers and
verifies that wait-event descriptors, wait state, stack base, and launch
record pointers remain carrier-local.

Validation for this slice:

- touched-object builds passed for `backend_runtime.o`,
  `backend_runtime_ipc.o`, `waiteventset.o`, `backend_runtime_utility.o`,
  `stack_depth.o`, and `launch_backend.o`;
- the LLVM-enabled full `gmake -j8` build passed;
- `gmake check-runtime-lifecycles` passed with 149 runtime fields classified;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals and carrier-local declarations reduced from 31 to 24;
- `gmake -C src/test/modules/test_backend_runtime clean all` passed after the
  main `postgres` binary was rebuilt with the new carrier accessors;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` with 88
  tests and `002_threaded_bgworker_crash.pl` with 6 tests using the local
  `IPC::Run` `PERL5LIB` and patched temporary-install dynamic library names.

## Gate E2 Maintainability Refactor Blocker

Before continuing with additional Gate E2 state migration or starting Phase 13
scheduler-aware wait work, do a no-behavior-change maintainability split of
the runtime bridge and backend-runtime tests. The current central files are
large enough to slow review and increase rebase conflict concentration:

- `src/backend/utils/init/backend_runtime.c` is still the core runtime
  orchestration file, but it should not keep accumulating per-subsystem
  lifecycle helpers and compatibility accessors;
- `src/test/modules/test_backend_runtime/test_backend_runtime.c` contains the
  focused C-level coverage for most migrated buckets, but its object families
  should be split into separate test sources before more buckets are added.

Existing fork-owned split files already point in the right direction:

- `src/backend/utils/cache/backend_runtime_cache.c` owns cache and
  function-manager compatibility accessors;
- `src/backend/utils/activity/backend_runtime_pgstat.c` owns pgstat
  compatibility accessors;
- `src/backend/utils/misc/backend_runtime_guc.c` owns GUC compatibility
  backing-variable accessors;
- `src/backend/jit/backend_runtime_jit.c` owns provider-independent and LLVM
  JIT runtime bridges.

The refactor should preserve behavior and test surface while making ownership
more explicit:

- keep `backend_runtime.c` focused on root process/thread runtime
  construction, current-pointer installation, process/thread symmetry,
  top-level adoption/reset orchestration, and unavoidable early fallback
  storage;
- move remaining owner-specific lifecycle helpers/accessors to adjacent
  fork-owned files, such as IPC, lmgr/lock, buffer/storage, and additional GUC
  bridge files where appropriate;
- update the lifecycle checker so every owner-adjacent `backend_runtime_*.c`
  file that contains manifest-referenced functions is part of the default
  checked source set, or introduce an explicit checked source manifest before
  adding more split files;
- split `test_backend_runtime.c` by object/lifetime family while keeping the
  same `test_backend_runtime` extension, SQL file, expected output, and TAP
  entry points. Suggested first files are backend/carrier, session,
  connection, execution, PMChild, and shared test helpers.

Acceptance criteria for the refactor slice:

- no intended behavior change;
- add lifecycle bucket definition files, X-macros, or an equivalent checked
  manifest for `PgBackend`, `PgSession`, `PgConnection`, and `PgExecution`.
  Use that as the single source of truth for constructor, early-adoption, and
  reset/destroy call lists before the remaining teardown work continues;
- implement this lifecycle framework as the next Gate E2 slice. Prefer
  checked `.def` bucket files that are included by the top-level runtime
  constructors/adoption/reset orchestration, so adding a bucket requires one
  manifest row and one checked bucket-definition row rather than updating
  several handwritten call lists by memory;
- make the lifecycle framework ergonomic enough for large-batch migration.
  Add small helper macros, templates, or declarative rule columns for common
  copied-scalar, zero-reset, whole-bucket copy/adopt, and destructor-call
  cases. The goal is to remove repetitive manual lifecycle boilerplate while
  keeping exceptional ordering and semantic cleanup explicit;
- keep semantic cleanup/destructor functions handwritten and close to the
  owning subsystem. The generated or macro-driven layer should cover only
  repetitive coverage and call-list mechanics;
- extend `check_runtime_lifecycles.pl` so it verifies the bucket definitions
  against `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`, confirms every referenced
  lifecycle function exists, and rejects process/thread constructor,
  adoption, or reset asymmetry that is not explicit;
- `gmake check-runtime-lifecycles` still verifies every manifest-referenced
  function after the split;
- `gmake check-global-lifetimes` still reports zero new unclassified mutable
  globals;
- `gmake -C src/test/modules/test_backend_runtime check` and direct
  backend-runtime TAP still pass;
- docs and `AGENTS.md` tell future agents where new runtime buckets and tests
  should go, so new work does not default back to the giant central files.

Lifecycle bucket definition framework slice completed:

- `src/backend/utils/init/backend_runtime_backend_buckets.def`,
  `backend_runtime_session_buckets.def`,
  `backend_runtime_connection_buckets.def`, and
  `backend_runtime_execution_buckets.def` now define one checked row for each
  `PgBackend`, `PgSession`, `PgConnection`, and `PgExecution` field;
- each row names constructor, early-adoption, and closed-reset expressions;
- `check_runtime_lifecycles.pl` validates the bucket definitions against the
  root-object fields and `MULTITHREADED_RUNTIME_LIFECYCLE.tsv`, and confirms
  every referenced runtime lifecycle function is present in the checked source
  set;
- `backend_runtime.c` includes the rows for constructor setup and early
  fallback adoption. Backend, connection, and execution closed-reset
  orchestration includes the same rows directly;
- session closed reset uses the separate ordered
  `backend_runtime_session_reset_buckets.def`, because its teardown order
  differs from early-adoption order. The reset file is checked by
  `check_runtime_lifecycles.pl`; semantic cleanup remains handwritten in named
  helper functions.

First refactor slice completed:

- `src/backend/libpq/backend_runtime_connection.c` now owns
  frontend/backend connection compatibility accessors;
- `src/backend/storage/buffer/backend_runtime_buffer.c` now owns the
  backend-local buffer compatibility accessors;
- `src/backend/storage/file/backend_runtime_file.c` now owns backend-local
  fd/storage compatibility accessors;
- `src/backend/storage/lmgr/backend_runtime_lmgr.c` now owns backend-local
  lock-manager compatibility accessors;
- `src/backend/storage/ipc/backend_runtime_ipc.c` now owns backend-local IPC,
  sinval, DSM, and latch compatibility accessors;
- `src/backend/utils/init/backend_runtime.c` still owns root object
  construction, early fallback storage, current-pointer installation, and the
  fallback-aware current-bucket selectors used by the split owner files;
- `check_runtime_lifecycles.pl` now scans the storage owner files by default,
  so future manifest-referenced lifecycle helpers can be split out without
  weakening the Gate E2 checker;
- `src/test/modules/test_backend_runtime/test_backend_runtime.c` has been
  split into object-family files:
  `test_backend_runtime_backend.c`, `test_backend_runtime_session.c`,
  `test_backend_runtime_connection.c`, and
  `test_backend_runtime_execution.c`, with shared declarations in
  `test_backend_runtime.h`.

Second refactor slice completed:

- `src/backend/utils/init/backend_runtime.c` now keeps fallback-aware
  connection bucket selectors, while exported connection compatibility
  accessors live in `src/backend/libpq/backend_runtime_connection.c`;
- `src/test/modules/test_backend_runtime/test_backend_runtime_session_guc.c`
  now owns the runtime/server GUC, session GUC, and GUC rebind tests, reducing
  `test_backend_runtime_session.c` to core session/cache/identity/reset tests.

Third refactor slice completed:

- the remaining session-owned GUC compatibility accessors for query-id,
  storage/user/command/replication GUCs, logical replication GUC state,
  access/WAL GUCs, JIT GUCs, sort/query-memory/planner-cost/planner-method
  GUCs moved from `src/backend/utils/init/backend_runtime.c` to
  `src/backend/utils/misc/backend_runtime_guc.c`;
- backend-local utility, formatting, sampling, superuser, and resource-owner
  compatibility accessors moved to
  `src/backend/utils/misc/backend_runtime_utility.c`;
- backend-local parallel-query compatibility accessors moved to
  `src/backend/access/transam/backend_runtime_parallel.c`;
- `backend_runtime.c` keeps only the small fallback-aware current-bucket
  selectors for those owners, reducing the central runtime file by roughly
  another thousand lines without changing behavior;
- `test_backend_runtime_backend.c` was split again so PMChild
  thread-backend publication tests live in `test_backend_runtime_pmchild.c`,
  interrupt/exit-state tests live in
  `test_backend_runtime_backend_interrupt.c`, and core backend identity,
  command/log, expression-interpreter, and latch interrupt tests live in
  `test_backend_runtime_backend_core.c`.

Validation for this refactor slice:

- touched-object builds passed for `backend_runtime.o`,
  `backend_runtime_guc.o`, `backend_runtime_utility.o`,
  `backend_runtime_parallel.o`, and the split `test_backend_runtime` module
  objects;
- full `gmake -j8` passed;
- `gmake -C src/test/modules/test_backend_runtime clean all` passed after
  the test split;
- `gmake check-runtime-lifecycles` passed with 149 runtime fields classified
  after adding the new utility and parallel owner files to the default checked
  source set;
- `gmake check-global-lifetimes` passed with zero new unclassified mutable
  globals;
- `gmake -C src/test/modules/test_backend_runtime check` passed;
- direct backend-runtime TAP passed for `001_threaded_runtime.pl` and
  `002_threaded_bgworker_crash.pl` using the local `IPC::Run` `PERL5LIB` and
  patched temporary-install dynamic library names. The direct TAP invocation
  also needs `PG_REGRESS=$PWD/src/test/regress/pg_regress` when run outside
  the configured TAP make target, and stale `tmp_check` TAP scratch
  directories must be removed before rerun after a failed harness setup.

Lifecycle ergonomics slice completed:

- `src/backend/utils/init/backend_runtime.c` now has local lifecycle helper
  macros for routine zero-initializer functions, whole-bucket early fallback
  copy/adopt plus reinitialization, and whole-bucket early fallback copy/adopt
  plus zero reset;
- a batch of simple `PgExecution` lifecycle helpers now uses those macros
  instead of handwritten near-identical functions. The converted buckets cover
  memory-context/resource-owner/portal whole-bucket adoption, plus vacuum,
  node I/O, basebackup, ANALYZE, combo CID, transaction cleanup, GUC error,
  async, catalog-cache, relmap, invalidation, two-phase record, trigger,
  regex, valgrind, and snapbuild zero-init/copy-adopt helpers;
- `check_runtime_lifecycles.pl` recognizes `PG_RUNTIME_DEFINE_*` helper macros
  as defining checked lifecycle functions, so the manifest and bucket
  definition validation stays strict after boilerplate moves behind macros.

Validation for this lifecycle ergonomics slice:

- `perl -c src/tools/runtime_lifecycle/check_runtime_lifecycles.pl` passed;
- `gmake -C src/backend/utils/init backend_runtime.o` passed;
- `gmake check-runtime-lifecycles` passed with 149 runtime fields classified.
