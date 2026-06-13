# Multithreaded PostgreSQL Agent Guide

This repository is an experimental branch for making PostgreSQL capable of
running backend sessions in a multithreaded runtime. The branch is allowed to
be ambitious and is not currently optimized for upstream patch shape.

Implementation is now underway. Keep the plan and architecture notes current as
the code evolves.

## Project Docs

- [MULTITHREADED_ARCHITECTURE.md](MULTITHREADED_ARCHITECTURE.md) describes the
  desired end-state architecture.
- [MULTITHREADED_PLAN.md](MULTITHREADED_PLAN.md) describes the staged
  implementation plan, validation strategy, and risk register.
- [MULTITHREADED_PHASE5_INTERRUPTS.md](MULTITHREADED_PHASE5_INTERRUPTS.md)
  records the logical interrupt boundary and recovery-conflict fixture
  decision.
- [MULTITHREADED_PHASE6_EXIT.md](MULTITHREADED_PHASE6_EXIT.md) records the
  current backend lifecycle/exit boundary and deferred thread-runtime proof.
- [MULTITHREADED_PHASE7_EXTENSIONS.md](MULTITHREADED_PHASE7_EXTENSIONS.md)
  records the extension backend-model gate and PL/pgSQL audit result.
- [MULTITHREADED_PHASE8_THREAD_SAFETY.md](MULTITHREADED_PHASE8_THREAD_SAFETY.md)
  records the first thread-local bridge for backend-local state and the
  remaining Phase 8 thread-safety floor.
- [MULTITHREADED_PHASE9_WAIT_BOUNDARY.md](MULTITHREADED_PHASE9_WAIT_BOUNDARY.md)
  records the current logical wait/suspend boundary work.
- [MULTITHREADED_PHASE10_THREAD_RUNTIME.md](MULTITHREADED_PHASE10_THREAD_RUNTIME.md)
  records the thread-per-session runtime work.
- [MULTITHREADED_PHASE11_WORKERS.md](MULTITHREADED_PHASE11_WORKERS.md)
  records the auxiliary worker thread-runtime work, starting with autovacuum
  workers.
- [MULTITHREADED_PHASE12_STATE.md](MULTITHREADED_PHASE12_STATE.md) records the
  state-migration bridge work that starts moving TLS/global state toward
  explicit runtime/session objects.
- [MULTITHREADED_THREADING_REVIEW.md](MULTITHREADED_THREADING_REVIEW.md)
  records the critical branch review findings and the Phase 12 exit-gate
  rationale.
- [refs/REFERENCES.md](refs/REFERENCES.md) lists external references.
- [refs/pgconf-2025-multithreading-transcript.md](refs/pgconf-2025-multithreading-transcript.md)
  is the local transcript of the PgConf.dev 2025 talk that motivates this work.

## Working Assumptions

- Use Heikki Linnakangas's multithreading branch as reference material, not as
  a base to merge wholesale.
- Preserve multiprocess PostgreSQL as a supported backend model.
- The first native threading target should be thread-per-session. The longer
  term target is an explicit scheduler that can map sessions/executions to a
  pool of carriers.
- Thread-per-session for regular client backends is not the final normal-mode
  target. Normal threaded server mode should eventually run in-tree
  server-owned workers, including autovacuum and auxiliary worker families, as
  threaded runtime-owned workers rather than forked subprocesses.
- Single-user mode, bootstrap mode, frontend command-line utilities,
  postmaster/control-plane process lifetime, and crash-escalation paths are
  deliberate process-lifetime exceptions.
- Do not overfit the design to WASM. Keep the main-loop and wait-boundary
  abstractions clean enough that a future host-driven runtime can use them.
- Existing third-party C extensions may be process-backend-only. That is an
  acceptable compatibility break for threaded mode.
- Existing third-party background workers may remain process-only or be
  rejected in threaded mode unless explicit worker-runtime metadata opts them
  in.
- In-tree modules and important bundled languages, especially PL/pgSQL, should
  have a plausible path to work in threaded mode.

## Source Orientation

Important current files:

- `src/backend/tcop/postgres.c`: `PostgresMain()`, the top-level backend loop,
  error recovery, command read, command dispatch, and `ProcessInterrupts()`.
- `src/include/access/session.h` and `src/backend/access/common/session.c`:
  existing `Session` abstraction for session-scoped DSM/DSA state. Treat this
  as a seed for the broader session object unless there is a strong reason not
  to.
- `src/include/miscadmin.h`: widely visible process/session globals and the
  interrupt macros.
- `src/backend/storage/ipc/procsignal.c`: process-signal-style backend
  communication.
- `src/backend/storage/ipc/latch.c` and `src/backend/storage/ipc/waiteventset.c`:
  wait/wakeup infrastructure.
- `src/backend/postmaster/launch_backend.c` and
  `src/backend/postmaster/postmaster.c`: backend launch and supervision.
- `src/backend/postmaster/autovacuum.c`,
  `src/backend/postmaster/auxprocess.c`,
  `src/backend/postmaster/bgworker.c`, and the individual auxiliary worker
  files under `src/backend/postmaster/`: worker launch, supervision, and
  server-owned worker lifecycles.
- `src/backend/replication/walreceiver.c`,
  `src/backend/replication/logical/launcher.c`,
  `src/backend/replication/logical/worker.c`, and
  `src/backend/storage/aio/method_worker.c`: replication and AIO worker
  lifecycles that must eventually use the threaded worker runtime.
- `src/include/fmgr.h` and `src/backend/utils/fmgr/dfmgr.c`: extension module
  ABI checks.
- `src/pl/plpgsql`: PL/pgSQL implementation.

## Development Rules For This Branch

- Keep documentation and code commits coherent. Prefer one conceptual change
  per commit.
- After each commit, push the current branch immediately unless the user has
  explicitly asked not to push.
- Before editing core code, read the surrounding implementation and current
  comments. PostgreSQL has many invariants that are documented only locally.
- Keep process-mode behavior working after each implementation phase.
- Use static annotations and tools to classify globals before moving large
  amounts of state.
- For Phase 12 state migration, prefer larger coherent batches when the state
  has the same owner and validation surface. Avoid one-variable commits unless
  the variable sits on a fragile lifecycle path where a narrow proof is needed.
- Do not attempt thread launch until the thread-safety floor is in place:
  backend-local globals must not be shared plain process globals, backend exit
  must not terminate the whole runtime, and timeout/interrupt delivery must be
  per logical backend.
- Before leaving Phase 12 or starting scheduler-aware wait work, run
  `gmake check-global-lifetimes` as part of Gate E2. A new mutable global must
  be annotated with an explicit `PG_GLOBAL_*` owner or deliberately accepted in
  `src/tools/global_lifetime/global_lifetime_baseline.tsv`.
- `AuxProcessResourceOwner` is now routed through `PgBackend` via
  `PgCurrentAuxProcessResourceOwnerRef()` and the `AuxProcessResourceOwner`
  lvalue macro. After changing `src/include/utils/resowner.h` or this backend
  runtime bridge, clean and rebuild backend objects before trusting link or TAP
  results; stale objects can still reference the removed
  `_AuxProcessResourceOwner` symbol.
- `MyProc` is now routed through `PgBackend` via `PgCurrentMyProcRef()` and
  the `MyProc` lvalue macro. After changing `src/include/storage/proc.h` or
  this backend runtime bridge, clean and rebuild backend objects and any
  extension modules under test before trusting link or TAP results; stale
  objects can still reference the removed `_MyProc` symbol. At minimum, clean
  and reinstall PL/pgSQL and `src/test/modules/test_backend_runtime` before
  rerunning their tests after a `MyProc` bridge change.
- `MyProcNumber` and `ParallelLeaderProcNumber` are now routed through
  `PgBackend` via `PgCurrentMyProcNumberRef()`,
  `PgCurrentParallelLeaderProcNumberRef()`, and the existing lvalue names in
  `src/include/storage/procnumber.h`. After changing that header or this
  backend runtime bridge, clean and rebuild backend objects and any extension
  modules under test before trusting link or TAP results; stale objects can
  still reference the removed `_MyProcNumber` or
  `_ParallelLeaderProcNumber` symbols, or miss the new accessor symbols. At
  minimum, clean and reinstall PL/pgSQL and
  `src/test/modules/test_backend_runtime` before testing.
- `MyBEEntry` is now routed through `PgBackend` via
  `PgCurrentMyBEEntryRef()` and the existing lvalue name in
  `src/include/utils/backend_status.h`. After changing that header or this
  backend runtime bridge, clean and rebuild backend objects and any extension
  modules under test before trusting link or TAP results; stale objects can
  still reference the removed `_MyBEEntry` symbol, or miss the new accessor
  symbol. At minimum, clean and reinstall
  `src/test/modules/test_backend_runtime` before testing.
- `MyBgworkerEntry` is now routed through `PgBackend` via
  `PgCurrentMyBgworkerEntryRef()` and the lvalue macro in
  `src/include/postmaster/bgworker.h`. After changing that header or this
  backend runtime bridge, clean and rebuild backend objects and any extension
  modules under test before trusting link or TAP results; stale objects can
  still reference the removed `_MyBgworkerEntry` symbol, or miss the new
  accessor symbol. At minimum, clean and reinstall
  `src/test/modules/test_backend_runtime`, `src/test/modules/worker_spi`,
  `src/test/modules/test_shm_mq`, and any worker modules under test.
- `ConfigReloadPending`, `ShutdownRequestPending`, `WakeupStopPending`,
  `AutoVacLauncherPending`, and `CheckpointerShutdownXLOGPending` are now
  fields in `PgBackendPendingInterruptState`, exposed through compatibility
  macros in `src/include/miscadmin.h`; their old exported TLS symbols were
  removed from `src/backend/postmaster/interrupt.c` and
  `src/backend/postmaster/checkpointer.c`. After changing this bridge, clean
  and rebuild backend objects and extension modules that include
  `postmaster/interrupt.h` or `miscadmin.h`; stale modules can still reference
  removed `_ConfigReloadPending`, `_ShutdownRequestPending`,
  `_WakeupStopPending`, `_AutoVacLauncherPending`, or
  `_CheckpointerShutdownXLOGPending` symbols. At minimum, clean and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, worker modules, and
  contrib modules under test before validating.
- `proc_exit_inprogress` and `shmem_exit_inprogress` are now fields in
  `PgBackendExitState`, exposed through compatibility macros in
  `src/include/storage/ipc.h`; the old exported TLS definitions were removed
  from `src/backend/storage/ipc/ipc.c`. After changing this bridge, clean and
  rebuild backend objects and extension modules that include `storage/ipc.h`;
  stale modules can still reference the removed `_proc_exit_inprogress` or
  `_shmem_exit_inprogress` symbols, or miss the
  `PgCurrentBackendExitStateRef()` accessor.
- `PendingBgWriterStats`, `PendingCheckpointerStats`,
  `PendingIOStats`, `pending_SLRUStats`, `PendingLockStats`,
  `PendingBackendStats`, `pgStatXactCommit`, `pgStatXactRollback`,
  `pgStatBlockReadTime`, `pgStatBlockWriteTime`, `pgStatActiveTime`,
  `pgStatTransactionIdleTime`, `total_func_time`, `prevWalUsage`,
  `prevBackendWalUsage`, `pgstat_report_fixed`, `pgStatForceNextFlush`,
  `force_stats_snapshot_clear`, `pgstat_is_initialized`,
  `pgstat_is_shutdown`, `pgStatPendingContext`, `pgStatPending`, and the
  related `have_*stats`/`backend_has_iostats` booleans are now fields in
  `PgBackendPgStatPendingState`, exposed through compatibility macros in
  `src/include/pgstat.h` and private macros/accessors in
  `src/backend/utils/activity/pgstat.c` and
  `src/include/utils/pgstat_internal.h`; the old exported/static TLS
  definitions were removed from pgstat implementation files.
  `PGSTAT_SLRU_NUM_ELEMENTS` is public only to size the runtime SLRU
  pending-state array and is asserted against `slru_names[]` in
  `src/include/utils/pgstat_internal.h`. The pending-entry list bridge assumes
  no early pgstat pending entries exist before backend-runtime adoption; copied
  non-empty `dlist_head` values would still point at the old list head, so the
  adoption path asserts that invariant and reinitializes the adopted head.
  After changing this bridge, clean and rebuild backend objects and extension
  modules that include `pgstat.h`; stale objects can still reference removed
  pgstat symbols or miss the new accessor symbols. At minimum, clean and
  reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib/test modules under
  pgstat coverage before validating.
- `pgBufferUsage`, `save_pgBufferUsage`, `pgWalUsage`, and
  `save_pgWalUsage` are now fields in `PgBackendInstrumentationState`,
  exposed through compatibility macros in `src/include/executor/instrument.h`;
  the old exported/static TLS definitions were removed from
  `src/backend/executor/instrument.c`. After changing this bridge, clean and
  rebuild backend objects and extension modules that include `instrument.h`;
  stale objects can still reference removed `_pgBufferUsage` or
  `_pgWalUsage` symbols, or miss the new accessor symbols. At minimum, clean
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Pending file-sync state (`pendingOps`, `pendingUnlinks`,
  `pendingOpsCxt`, `sync_cycle_ctr`, `checkpoint_cycle_ctr`, and
  `sync_in_progress`), storage-manager relation state (`SMgrRelationHash` and
  `unpinned_relns`), magnetic-disk storage-manager context (`MdCxt`), and
  file-descriptor/VFD state (`VfdCache`, `SizeVfdCache`, `nfile`,
  `temporary_files_allowed`, `numAllocatedDescs`, `maxAllocatedDescs`,
  `allocatedDescs`, and `numExternalFDs`) are now fields in
  `PgBackendStorageState`, exposed through private compatibility macros in
  `src/backend/storage/sync/sync.c`, `src/backend/storage/smgr/smgr.c`,
  `src/backend/storage/smgr/md.c`, and `src/backend/storage/file/fd.c`.
  The smgr adoption path asserts that no early smgr relation hash/list exists
  before backend-runtime adoption; copied non-empty `dlist_head` values would
  still point at the old list head. Threaded backend startup can reserve file
  descriptors before installing the backend runtime, so
  `InstallPgThreadBackendRuntimeState()` must adopt early storage state into
  the thread-backed `PgBackend`; losing that fallback fd state can make the
  threaded TAP postmaster exit immediately after launching worker threads.
  After changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Deadlock detector workspace state (`visitedProcs`, `nVisitedProcs`,
  `topoProcs`, `beforeConstraints`, `afterConstraints`, `waitOrders`,
  `nWaitOrders`, `waitOrderProcs`, `curConstraints`, `nCurConstraints`,
  `maxCurConstraints`, `possibleConstraints`, `nPossibleConstraints`,
  `maxPossibleConstraints`, `deadlockDetails`, `nDeadlockDetails`, and
  `blocking_autovacuum_proc`) is now owned by `PgBackendLockState`, exposed
  through private compatibility macros in `src/backend/storage/lmgr/deadlock.c`.
  `PgBackendLockState` intentionally uses opaque pointer fields so the private
  `deadlock.c` `EDGE`, `WAIT_ORDER`, and `DEADLOCK_INFO` types stay local to
  that source file. After changing this bridge, clean and rebuild backend
  objects because `PgBackend` layout and installed runtime headers changed; at
  minimum rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`,
  and contrib before validating.
- Local-buffer state (`NLocBuffer`, `LocalBufferDescriptors`,
  `LocalBufferBlockPointers`, `LocalRefCount`, `nextFreeLocalBufId`,
  `LocalBufHash`, `NLocalPinnedBuffers`, and the `GetLocalBufferStorage()`
  allocation cursor/context fields) is now owned by `PgBackendBufferState`.
  Exported local-buffer names are compatibility macros in `storage/bufmgr.h`
  and `storage/buf_internals.h`; private names remain compatibility macros in
  `src/backend/storage/buffer/localbuf.c`. Shared-buffer pin/writeback state
  (`BackendWritebackContext`, `PinCountWaitBuf`, the private refcount
  array/hash state, and `MaxProportionalPins`) is also owned by
  `PgBackendBufferState`; `BackendWritebackContext` remains object-like at call
  sites through a `storage/buf_internals.h` macro. After changing this bridge,
  clean and rebuild backend objects because `PgBackend` layout and installed
  buffer/runtime headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- IPC/sinval backend state (`MyProcSignalSlot`, `SharedInvalidMessageCounter`,
  `catchupInterruptPending`, and the recursive
  `ReceiveSharedInvalidMessages()` buffer/cursor state) is now owned by
  `PgBackendIPCState`. `procsignal.c` keeps `ProcSignalSlot` private through a
  file-local compatibility macro; `sinval.h` keeps the exported counter/flag
  names as compatibility macros. After changing this bridge, clean and rebuild
  backend objects because `PgBackend` layout and installed storage/runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- Lock-manager backend-local state now also lives in `PgBackendLockState`:
  fast-path local-use counters, relation-extension lock ownership,
  `LockMethodLocalHash`, strong-lock progress, awaited-lock/owner state,
  `got_deadlock_timeout`, condition-variable sleep target state, and
  speculative insertion token state. `lock.c`, `proc.c`,
  `condition_variable.c`, and `lmgr.c` keep local compatibility macros. After
  changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating.
- Always-built LWLock backend-local state now also lives in
  `PgBackendLockState`: `num_held_lwlocks`, the fixed `held_lwlocks` array,
  and `LocalNumUserDefinedTranches` are backed by runtime accessors while
  `lwlock.c` keeps the existing local source names. `LWLOCK_STATS` debug-only
  state remains a follow-up because its dummy stats entry uses a private debug
  struct and that code is not built in this checkout. After changing this
  bridge, clean and rebuild backend objects because `PgBackend` layout and
  installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- Transaction/access-manager backend-local state now lives in
  `PgBackendTransactionState`: transaction-status cache fields, two-phase
  locked-GXACT and exit-registration fields, the private `TwoPhaseGetGXact()`
  lookup cache, SLRU error-report fields, and multixact cache/debug-string
  state. This bridge deliberately includes function-local statics that do not
  appear in the raw `PG_THREAD_LOCAL` scan. The multixact list head must be
  initialized through the runtime state initializer, and early adoption asserts
  that any initialized early list is empty before copying. After changing this
  bridge, clean and rebuild backend objects because `PgBackend` layout and
  installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- ProcArray backend-local visibility/cache state now also lives in
  `PgBackendTransactionState`: the `TransactionIdIsInProgress()` negative
  cache, `GlobalVisState` horizon caches, the
  `ComputeXidHorizonsResultLastXmin` throttle, and `XIDCACHE_DEBUG` counters.
  `GlobalVisState` is defined in `utils/backend_runtime.h` so the runtime can
  store it by value while existing snapshot/heapam headers keep using forward
  declarations. After changing this bridge, clean and rebuild backend objects
  because `PgBackend` layout and installed runtime headers changed; at minimum
  rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and
  contrib before validating.
- Backend activity snapshot state now lives in `PgBackendActivityState`:
  `localBackendStatusTable`, `localNumBackends`, and
  `backendStatusSnapContext` are backed by runtime accessors while
  `backend_status.c` keeps the existing local source names. Pgstat
  shared-entry reference-cache state (`pgStatEntryRefHash`,
  `pgStatSharedRefAge`, `pgStatSharedRefContext`, and
  `pgStatEntryRefHashContext`) now lives in `PgBackendPgStatPendingState`
  behind private pgstat accessors and `pgstat_shmem.c` compatibility macros.
  The private simplehash type stays local to `pgstat_shmem.c` through an
  opaque runtime pointer. `pgStatLocal` remains standalone backend-local TLS
  for a later dedicated pgstat-local slice because its type depends on
  internal pgstat snapshot structures. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- Backend utility/support state now lives in `PgBackendUtilityState`:
  dynahash active sequential-scan tracking, the superuser one-entry cache,
  the resource-owner release callback pointer, and optional `RESOWNER_STATS`
  counters are backed by runtime accessors while `dynahash.c`,
  `superuser.c`, and `resowner.c` keep local source names. The private
  `ResourceReleaseCallbackItem` type stays local to `resowner.c`; the runtime
  stores the callback head as an opaque pointer and `resowner.c` casts it
  through a file-local typed helper. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
- Utility cache/scratch state now also lives in `PgBackendUtilityState`:
  date/time token caches, degree-trig cached constants, date/time and numeric
  format-picture caches, the optional libxml allocation context, and the
  missing-attribute datum cache are backed by runtime accessors while
  `datetime.c`, `float.c`, `formatting.c`, `xml.c`, and `heaptuple.c` keep
  local source names. Private cache entry types stay private to their owning
  files through opaque runtime pointer arrays and local casts. After changing
  this bridge, clean and rebuild backend objects because `PgBackend` layout
  and installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- Parallel worker and pqmq backend-local state now lives in
  `PgBackendParallelState`: `ParallelWorkerNumber`,
  `ParallelMessagePending`, `InitializingParallelWorker`, private parallel
  context tracking, and shared-memory message queue redirection state are
  backed by runtime accessors while `parallel.c` and `pqmq.c` keep local
  source names. Private `FixedParallelState` and `shm_mq_handle` types remain
  opaque outside their owning files. The early fallback parallel state must
  keep the legacy `ParallelWorkerNumber = -1` sentinel as a static
  initializer; bootstrap reaches `IsParallelWorker()` before full backend
  runtime adoption, and a zero fallback makes `initdb` believe it is in a
  parallel worker. After changing this bridge, clean and rebuild backend
  objects because `PgBackend` layout and installed runtime headers changed; at
  minimum rebuild and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`,
  and contrib before validating.
- DSM/latch IPC backend-local state now also lives in `PgBackendIPCState`:
  `dsm_init_done`, `dsm_registry_dsa`, `dsm_registry_table`, `LatchWaitSet`,
  and `LocalLatchData` are backed by runtime accessors while `dsm.c`,
  `dsm_registry.c`, `latch.c`, and `miscinit.c` keep local compatibility
  names. Threaded backend startup initializes `MyLatch` and `LatchWaitSet`
  before installing the backend runtime object, so early IPC adoption must
  retarget adopted `backend->core.latch` and `backend->interrupt_latch`
  pointers from the early fallback latch to the backend-owned latch. If this
  is missed, direct threaded TAP fails during startup with
  `cannot wait on a latch owned by another process`. After changing this
  bridge, clean and rebuild backend objects because `PgBackend` layout and
  installed runtime headers changed; at minimum rebuild and reinstall
  PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib before
  validating.
- Timeout scheduler backend-local state now lives in `PgBackendTimeoutState`:
  registered timeout parameters, the active timeout queue, alarm/signal
  pending flags, firing-target pointers, and signal-vs-logical delivery mode
  are backed by runtime accessors while `timeout.c` keeps local compatibility
  names. `PgTimeoutParams` is defined in `utils/timeout.h` so `PgBackend` can
  own the fixed timeout arrays directly. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
  Direct threaded TAP exercises logical timeout delivery and should be run.
- WAL sender backend-local state now lives in `PgBackendWalSenderState`.
  Public WAL sender flags and `MyWalSnd` are compatibility macros over
  `PgCurrentWalSenderState()`, while `walsender.c` uses private macros for the
  streaming cursor, timeline state, reply buffers, logical decoding context,
  replication command context, and lag tracker. Keep the local sent pointer
  named distinctly from `WalSnd.sentPtr` to avoid macro expansion inside
  shared-memory struct field references. After changing this bridge, clean and
  rebuild backend objects because `PgBackend` layout and installed runtime
  headers changed; at minimum rebuild and reinstall PL/pgSQL,
  `src/test/modules/test_backend_runtime`, and contrib before validating.
  Direct threaded TAP should be run.
- Replication receiver and slot backend-local state now lives in
  `PgBackendReplicationState`. `MyReplicationSlot` is a compatibility macro
  over `PgCurrentReplicationState()`, while `syncrep.c` and `walreceiver.c`
  keep local compatibility names for sync-rep wait mode and WAL receiver
  connection/file/logstream/wakeup/reply state. The runtime initializer sets
  non-zero sentinels: `sync_rep_wait_mode = SYNC_REP_NO_WAIT`,
  `walreceiver_recv_file = -1`, and
  `walreceiver_primary_has_standby_xmin = true`. Fake-backend tests that
  inspect untouched replication state must initialize those fields explicitly
  because raw `MemSet()` does not model `PgBackendInitializeReplicationState()`.
  After changing this bridge, clean and rebuild backend objects because
  `PgBackend` layout and installed runtime headers changed; at minimum rebuild
  and reinstall PL/pgSQL, `src/test/modules/test_backend_runtime`, and contrib
  before validating. Direct threaded TAP should be run.
- Treat `PMChild.thread_backend` as private PMChild-owned publication state.
  Postmaster code should use PMChild helper APIs for threaded backend
  interrupt, wakeup, and thread-exit publication rather than dereferencing or
  clearing the raw pointer outside PMChild.
- Treat `PMChild.signal_pid` as live carrier-visible routing/logging state.
  Thread exit publication must capture the exited logical backend id in the
  PMChild exit payload before clearing `signal_pid`, so the postmaster can log
  the exited backend without advertising a dead thread as signalable.
- Thread-backed PMChild signal-id reads and thread-exit payload reads must use
  PMChild helper APIs. They are protected by the same PMChild mutex as
  `thread_backend` publication and clearing.
- Use `PostmasterChildDetachThreadBackend()` when a thread carrier needs to
  stop advertising its live logical-backend pointer before final exit
  publication. It preserves the exited logical id for reaping/logging while
  preventing later signal routing from targeting a backend committed to
  teardown.
- `test_pmchild_thread_backend_publication_race()` in
  `src/test/modules/test_backend_runtime` is the focused C-level stress for
  the PMChild helper contract. Run the full `test_backend_runtime` regression
  after changing PMChild thread publication, detach, signal-id, interrupt,
  wakeup, or exit-payload behavior.
- For thread-backed PMChild reaping, successful `pg_thread_join()` is the
  boundary before child cleanup and slot release. If join fails, leave the
  PMChild active and re-publish the claimed thread-exit report for retry; do
  not release or reuse a slot whose native carrier was not joined.
- Threaded backend exit currently reports retained carrier `TopMemoryContext`
  bytes through PMChild exit accounting. Do not remove or bypass this
  accounting until thread-exit memory/resource cleanup has a stronger
  replacement. A direct attempt to reset the exiting carrier's top memory tree
  after `PgBackendExitCleanup()` crashed a parallel threaded reconnect smoke,
  so treat full `TopMemoryContext` reclamation as an unresolved Gate E2 blocker.
- `test_backend_runtime_emit_fatal()` in
  `test_backend_runtime_threaded` is the focused threaded backend `FATAL`
  fixture. Run it through
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` with the
  local TAP `PERL5LIB` paths documented below, so the check covers the
  expected `FATAL`, verifies the backend id leaves `pg_stat_activity`, and
  confirms the server remains usable.
- `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` contains
  the broader mixed teardown stress for Gate E2. It starts concurrent
  backend-local `FATAL`, `pg_terminate_backend()`, and abandoned-client
  sessions, then verifies logical backend ids leave `pg_stat_activity`,
  advisory locks are released, and the server remains usable. Keep this
  fixture current when changing PMChild exit publication, thread join/retry,
  backend teardown, or session resource cleanup.
- Threaded regular backend launch duplicates the accepted client socket into
  `BackendThreadStart.client_sock`. `pq_init()` marks that launch-time socket
  copy invalid only after `Port` owns the descriptor and `socket_close()` is
  registered. `backend_thread_finish()` is the backstop for closing a still
  valid copied socket if startup fails before that handoff.
- Backend libpq connection teardown is now part of the Gate E2 resource model:
  `socket_close()` frees the frontend/backend `WaitEventSet`, the dynamically
  sized send buffer, and the `PortContext` that owns `Port` plus most startup
  packet/remote-host/authentication strings and SSL/GSS connection identity
  structures before closing the accepted socket. Keep the threaded TAP teardown
  matrix current when changing backend libpq socket I/O or `Port` ownership
  state, because normal disconnect, abandoned clients, `FATAL`, and
  administrator termination all exercise this callback.
- Thread-backed auxiliary workers receive postmaster `SIGQUIT`, `SIGKILL`,
  and `SIGABRT` as logical `PG_BACKEND_INTERRUPT_PROC_DIE` mailbox events, not
  as process signal handlers that can `_exit()`. Any custom auxiliary
  interrupt loop that calls `PgCurrentBackendApplyInterrupts()` must explicitly
  handle `ProcDiePending`, or immediate shutdown can leave thread carriers
  waiting for SIGKILL escalation.
- The temporary threaded startup serialization gate currently has no remaining
  backend-type users. Regular client backend startup bypass is validated by a
  32-connection threaded startup/catalog/temp-table/ANALYZE stress after
  moving VACUUM/ANALYZE recursive execution state into `PgExecutionVacuumState`.
  Process-model background workers are still rejected in threaded mode.
  Thread-compatible dynamic background workers publish their shared bgworker
  started state only after the worker reaches
  `ThreadedBackendStartupComplete()`, so dynamic waiters cannot terminate the
  worker while `InitProcess()`, `BaseInit()`, or background-worker function
  lookup are still running. Background writer/checkpointer/WAL writer bypass
  was validated as a worker-specific narrowing because their common auxiliary
  startup does not run database/session bootstrap before entering the worker
  loop. The autovacuum launcher bypass is validated against the no-database
  launcher loop; autovacuum worker bypass is validated against a real
  database-connected autovacuum worker launch and table vacuum smoke. Startup
  process, archiver, WAL receiver, and WAL summarizer bypasses are validated
  separately because they use the same common auxiliary startup, publish
  wakeup/progress state through shared memory, and keep per-loop work state
  backend-local. WAL receiver's gate bypass covers
  `AuxiliaryProcessMainCommon()`; the later `libpqwalreceiver` load and
  streaming loop are validated by a threaded physical-replication smoke.
  Startup process bypass is validated by threaded normal-startup and
  crash-recovery smokes. Slot sync worker bypass is validated by a threaded
  physical standby smoke that synchronizes a failover logical slot from a
  primary and verifies standby catalog usability. Keep any future startup-gate
  reintroduction narrowly tied to a named shared-state dependency and covered
  by concurrent catalog-startup stress.
- Prefer introducing compatibility wrappers around current globals before
  changing all call sites.
- Be careful moving GUC backing variables behind dynamic lvalue macros. The
  generated GUC table stores direct pointers for many variables during
  `InitializeGUCVariablePointers()`. Variables written only by assign hooks,
  such as parsed `DateStyle`/`DateOrder`, can be moved independently, but
  direct-pointer GUCs need a GUC-table pointer rebind/adoption mechanism.
  Threaded startup now records the direct backing-variable pointers after
  `InitializeGUCVariablePointers()`, runs
  `RebindSessionGUCVariablePointers()`, and initializes every built-in GUC
  record whose backing pointer changed. Keep extending
  `RebindSessionGUCVariablePointers()` when moving more direct-pointer GUC
  backing variables under runtime/session/execution objects. Only the small
  TLS dummy startup compatibility list in
  `InitializeThreadedSessionCompatibilityGUCOptions()` should remain
  hand-curated until those dummy GUCs get explicit session accessors. When
  common GUC names become macros, local struct fields with the same names must
  be renamed because macro expansion also hits `object->field` expressions;
  this was observed for the local GIN build-state `work_mem` field and the
  `TableSpaceOpts` `seq_page_cost`/`random_page_cost` fields.
- Some string GUCs can still be unset after runtime installation because the
  generated GUC table may already point at early fallback accessors before the
  "changed pointer" pass runs. `InstallPgThreadBackendRuntimeState()` therefore
  calls `InitializeThreadedSessionRequiredGUCOptions()` after
  `PgSetCurrentSession()` and after installing `CurrentPgExecution`; the latter
  is required because GUC check hooks allocate through the current execution's
  memory context state. That pass now initializes any built-in string GUC whose
  backing pointer is owned by the installed `PgSession` and still has NULL
  string storage, so future session-owned string GUCs do not need to be added
  to a growing whitelist. `client_encoding` remains the only post-install
  compatibility exception because its authoritative state is the session
  encoding object rather than a direct `char *` field in `PgSession`.
- Custom extension GUCs in threaded sessions rely on per-session `_PG_init()`
  invocation for already-loaded dynamic libraries. `dfmgr.c` records loaded
  module init state in `PgSession.dynamic_library_inits`; when a second
  threaded session reuses a process-loaded module, `_PG_init()` must run again
  so that session's GUC table receives the custom GUC definitions. A focused
  custom-GUC smoke should use `LOAD 'test_backend_runtime_threaded'` plus
  `SHOW`, so it validates module/GUC behavior without depending on catalog
  writes.
- Threaded catalog-writing DDL previously crashed in `XLogInsert()` during
  `CREATE TABLE` because the derived `wal_consistency_checking` bool array was
  NULL in the installed `PgSession`. Keep the threaded
  `CREATE TABLE`/`INSERT`/`DROP TABLE` smoke in
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` when
  changing required GUC bootstrap or WAL GUC state.
- The same threaded runtime TAP fixture now covers database, role, and startup
  GUC adoption: `ALTER DATABASE postgres SET work_mem`, `ALTER ROLE ... SET
  statement_timeout`, `ALTER ROLE ... SET default_statistics_target`, and a
  startup-packet `options='-c lock_timeout=8s'` connection. Keep that matrix
  current when changing threaded GUC replay/adoption paths.
- Threaded backend startup must replay postmaster nondefault GUC state after
  `InitializeThreadedSessionGUCOptions()` and before
  `InstallPgThreadBackendRuntimeState()`. That ordering lets
  `read_nondefault_variables()` write configured built-in defaults into early
  fallback session/runtime GUC buckets, which runtime installation then adopts
  into the thread's `PgSession`/runtime objects. Moving runtime installation
  earlier can crash because some adoption paths allocate GUC strings before
  `GUCMemoryContext` exists.
  The replay depends on the postmaster write side too: non-`EXEC_BACKEND`
  postmasters must call `write_nondefault_variables()` when `multithreaded` is
  enabled, both after initial config load and after SIGHUP reloads. Without
  `global/config_exec_params`, threaded clients silently fall back to boot
  defaults such as `work_mem = 4MB`.
- Avoid broad mechanical churn unless it unlocks a specific migration step.
- Do not remove process isolation paths merely because threaded mode exists.

## Local Build And Test Notes

- This checkout is commonly built with GNU make on macOS. Use `gmake`, not the
  BSD `make`. In the Codex desktop shell, Homebrew's bin directory may be
  absent from `PATH`; if `gmake` is not found, use `/opt/homebrew/bin/gmake` or
  export `PATH="/opt/homebrew/bin:$PATH"` before building.
- After cleaning under `src/backend`, generated backend-side files can be
  missing while include-side `header-stamp` files and symlinks still exist. If
  the build fails with a missing header such as `utils/errcodes.h`, regenerate
  the backend-side utility outputs explicitly:

  ```sh
  gmake -C src/backend/utils fmgr-stamp errcodes.h probes.h guc_tables.inc.c pgstat_wait_event.c wait_event_funcs_data.c wait_event_types.h
  ```

  If include-side symlinks are also missing or stale, remove the stamp and
  rebuild the symlinks:

  ```sh
  rm -f src/include/utils/header-stamp
  gmake -C src/backend/utils generated-header-symlinks
  ```

  If the missing generated header is `nodes/nodetags.h`, use the equivalent
  node-header recovery:

  ```sh
  rm -f src/backend/nodes/node-support-stamp
  gmake -C src/backend/nodes node-support-stamp
  rm -f src/include/nodes/header-stamp
  gmake -C src/backend/nodes generated-header-symlinks
  ```

- After changing exported backend globals or their `PG_THREAD_LOCAL`
  declarations in installed headers, clean and rebuild any in-tree extension
  under test before trusting its regression result. At minimum, do this for
  PL/pgSQL when touching GUC backing variables used by PL/pgSQL:

  ```sh
  gmake -C src/pl/plpgsql/src clean
  gmake -C src/pl/plpgsql/src all
  gmake -C src/pl/plpgsql/src DESTDIR="$PWD/tmp_install" install
  ```

  A stale PL/pgSQL build after SPI exported-state changes can fail during
  `initdb` post-bootstrap initialization with `Symbol not found:
  _SPI_processed` while loading `plpgsql.dylib`. Treat that as a stale module
  build, not as a SQL regression: clean, rebuild, and reinstall PL/pgSQL into
  the current `tmp_install`.

  `pg_global_prng_state` is also exported through an installed common header
  and is referenced by some contrib/test modules, including `amcheck`,
  `auto_explain`, `tablefunc`, and several `src/test/modules` tests. Clean and
  reinstall any of those modules before testing them after PRNG TLS changes.

  Pending interrupt globals such as `InterruptPending` can be referenced from
  server-side common objects and loadable modules. After converting one of
  these exported names to an object-backed compatibility macro, rebuild
  `src/common`, PL/pgSQL, `src/test/regress`, and `libpqwalreceiver` before
  trusting `initdb` or core regression results:

  ```sh
  gmake -C src/common clean all
  gmake -C src/pl/plpgsql/src clean all DESTDIR="$PWD/tmp_install" install
  gmake -C src/test/regress clean all
  gmake -C src/backend/replication/libpqwalreceiver clean all DESTDIR="$PWD/tmp_install" install
  ```

  Memory-context globals exported through `palloc.h` or `memutils.h` are also
  referenced by backend loadable modules needed during `initdb`
  post-bootstrap initialization. After converting one of these names to an
  object-backed compatibility macro, rebuild and reinstall `src/backend/snowball`
  before trusting temp-instance tests. A stale `dict_snowball.dylib` fails
  `initdb` with `Symbol not found: _CurrentMemoryContext`.

  ```sh
  gmake -C src/backend/snowball clean all DESTDIR="$PWD/tmp_install" install
  ```

  Direct-pointer GUC globals exported through `miscadmin.h` can be referenced
  by backend loadable modules too. After converting one of these names to an
  object-backed compatibility macro, force-clean and reinstall
  `libpqwalreceiver` before trusting subscription tests or the core
  `parallel_schedule`; a stale `libpqwalreceiver.dylib` failed to load with
  `Symbol not found: _work_mem` after the query-memory GUC migration.

  ```sh
  gmake -C src/backend/replication/libpqwalreceiver clean all
  gmake -C src/backend/replication/libpqwalreceiver DESTDIR="$PWD/tmp_install" install
  ```

  Logical-decoding output plugins can also keep stale references to moved
  backend globals. After moving memory-context or replication GUC globals,
  clean and reinstall `pgoutput` and `pgrepack` before trusting
  `contrib/test_decoding`; stale copies have failed with
  `Symbol not found: _CurrentMemoryContext`.

  ```sh
  gmake -C src/backend/replication/pgoutput clean all DESTDIR="$PWD/tmp_install" install
  gmake -C src/backend/replication/pgrepack clean all DESTDIR="$PWD/tmp_install" install
  ```

  Core backend globals such as `MyProcPid` can also be referenced from
  server-side port objects. If a clean backend link fails with a removed
  backend-global symbol from `libpgport_srv.a`, clean and rebuild `src/port`
  as well:

  ```sh
  gmake -C src/port clean all
  ```

- After changing a contrib/test module header to expose `PG_THREAD_LOCAL`
  declarations, clean and rebuild every object in that module before running a
  threaded smoke. Stale objects can still see the old plain-global symbol while
  freshly compiled objects use TLS, producing crashes that look like missing
  initialization. This was observed in `pg_stash_advice` after changing its
  DSM attachment pointers in `pg_stash_advice.h`.

- If an installed header changes a global from plain storage to
  `PG_THREAD_LOCAL`, do not trust a purely incremental backend build. Stale
  backend objects can still compile and link but then crash during `initdb`
  post-bootstrap single-user startup. Use the backend clean plus generated-file
  recovery above, then rebuild with `gmake -j8`.

- If a shared-memory struct layout changes, especially `Latch`, `PGPROC`, or
  fields embedded immediately beside semaphores/latches, do not trust a purely
  incremental backend build. Stale objects can corrupt adjacent shared-memory
  fields; one observed failure after changing `Latch` was a bootstrap segfault
  in `PGSemaphoreReset()` because stale `proc.o` still used the old
  `PGPROC.procLatch` size. Use the backend clean plus generated-file recovery
  above, then rebuild with `gmake -j8`.

- If `src/include/utils/backend_runtime.h` changes the layout of embedded
  runtime structs such as `PgThreadBackendRuntimeState`, do not trust a purely
  incremental backend build. Stale objects can keep old field offsets while
  freshly compiled runtime code zeros or writes the new, larger struct. One
  observed failure after adding connection socket I/O state was threaded
  startup corrupting adjacent `BackendThreadStart` timezone fields and
  segfaulting in `StartupXLOG()` before readiness. Use the backend clean plus
  generated-file recovery above, then rebuild with `gmake -j8`.

- If `src/include/replication/worker_internal.h` changes the layout of
  `LogicalRepWorker`, clean and rebuild the whole logical replication backend
  directory before running logical replication smokes. Incremental builds in
  this checkout have left objects such as `syncutils.o`, `tablesync.o`, and
  `applyparallelworker.o` built against the previous struct layout:

  ```sh
  gmake -C src/backend/replication/logical clean
  gmake -j8
  gmake -j8 install DESTDIR="$PWD/tmp_install"
  ```

  A stale `syncutils.o` can read `LogicalRepWorker.userid` from the old offset
  and make table-sync workers fail during startup with errors like
  `role with OID 119 does not exist`, where `119` is the ASCII value of a
  subscription relation-state byte.

- If `PMChild` layout changes in `src/include/postmaster/postmaster.h`, do not
  trust an incremental build of postmaster objects. Stale postmaster objects can
  corrupt the PMChild freelists or crash auxiliary children during temp-instance
  startup. Use:

  ```sh
  gmake -C src/backend/postmaster clean
  gmake -C src/backend -j8
  ```

- If a shared enum in an installed or widely included header changes numeric
  values, do not trust a purely incremental backend build. For example,
  inserting a new `PMSignalReason` before existing values can leave stale
  objects such as `checkpointer.o` signaling one numeric reason while
  `postmaster.o` interprets another, causing shutdown hangs. Prefer appending
  new signal reasons to preserve existing values, and force rebuild affected
  objects or use a clean backend rebuild before testing.

- This checkout is currently configured with `with_gssapi = no`. A direct
  `gmake -C src/backend/libpq be-secure-gssapi.o` can fail before reaching
  project changes because the GSSAPI types and functions are unavailable in
  this configuration. For GSSAPI-only source annotations, use static lifetime
  scan coverage plus a full non-GSS build here, and use a GSSAPI-enabled build
  when compile coverage for that file is required.

- This checkout is currently configured with `with_ssl = no`. A direct
  `gmake -C src/backend/libpq be-secure-openssl.o` can fail before reaching
  project changes because OpenSSL support macros are not enabled in
  `pg_config.h`. For OpenSSL-only source annotations, use static lifetime scan
  coverage plus a full non-SSL build here, and use an SSL-enabled build when
  compile coverage for that file is required.

- This checkout is currently configured with `with_llvm = no`. Direct builds
  under `src/backend/jit/llvm` fail before reaching project changes because
  the LLVM Makefile requires an LLVM-enabled configuration. For LLVM-only
  source annotations, use static lifetime scan coverage plus a full non-LLVM
  build here, and use an LLVM-enabled build when compile or runtime JIT
  coverage for those files is required.

- This checkout is currently configured without `--enable-injection-points`.
  `src/test/modules/injection_points` intentionally skips checks in that
  configuration, and injection-point TAP/regression coverage requires a build
  configured with injection points enabled. For injection-point-only source
  annotations in this checkout, use object compile coverage where reachable,
  static lifetime scan coverage, and a full non-injection build/install.

- For manual temp-cluster smokes, especially threaded-mode smokes launched from
  this deep checkout path, use a short Unix socket directory under `/tmp` with
  `pg_ctl -o "-k /tmp/..."` or an explicit `unix_socket_directories` setting.
  Nested workspace paths can exceed the platform Unix socket path length before
  SQL starts.

- Some `gmake ... check` runs fail on macOS because temporary-install binaries
  still refer to `/usr/local/pgsql/lib/libpq.5.dylib`. Patch the temp install
  before running direct `pg_regress` commands:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/initdb" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/psql"
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/pg_ctl" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/bin/pg_basebackup" || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" "$PWD/tmp_install/usr/local/pgsql/lib/libpqwalreceiver.dylib" || true
  ```

  Tests that create subscriptions can reach `libpqwalreceiver.dylib`; patch it
  along with the frontend binaries after reinstalling or recreating
  `tmp_install`.

  Direct isolation runs can fail the same way from build-tree binaries. Patch
  `src/test/isolation/isolationtester` and
  `src/test/isolation/pg_isolation_regress` to the same temp-install
  `libpq.5.dylib` before rerunning them. Direct TAP runs that pass
  `PG_REGRESS="$PWD/src/test/regress/pg_regress"` can fail during
  `pg_regress --config-auth` with signal 6 for the same reason after
  rebuilding `src/test/regress`; patch `src/test/regress/pg_regress` too:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/test/regress/pg_regress
  ```

  `gmake -C src/test/regress check-tests` recreates `tmp_install`, so a
  previously patched `psql` can become unpatched again. If that target fails
  before SQL starts with a `dyld` `libpq.5.dylib` loader error, patch the new
  temp-install binaries and rerun the equivalent `pg_regress` command directly.

  Top-level `gmake check-world` and recursive targets such as
  `gmake -C src/test check` also recreate `tmp_install` on this checkout. They
  can therefore fail before SQL starts even if a previous temp install was
  patched. If the failure is a `dyld` lookup for
  `/usr/local/pgsql/lib/libpq.5.dylib`, patch the recreated temp install and run
  the reached test driver directly. For example, after a `check-world` failure
  in `src/test/isolation`, patch `psql`, `pg_ctl`, `pg_isolation_regress`, and
  `isolationtester`, then rerun:

  ```sh
  cd src/test/isolation
  PATH="$PWD/../../../tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/../../../tmp_install/initdb-template" \
  ./pg_isolation_regress --temp-instance=./tmp_check_iso --inputdir=. --outputdir=output_iso --bindir= --schedule=./isolation_schedule
  ```

  The core regression equivalent after patching is:

  ```sh
  cd src/test/regress
  PATH="$PWD/../../../tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/../../../tmp_install/initdb-template" \
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --schedule=./parallel_schedule
  ```

  If the same recursive target needs to be rerun, patch the build-tree binaries
  that are copied into each recreated temp install before rerunning. This has
  allowed recursive checks such as
  `gmake -C src/test/modules/test_extensions check` to run normally after an
  initial temp-install `initdb` loader failure:

  ```sh
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/initdb/initdb || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/psql/psql || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/pg_ctl/pg_ctl || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/bin/pg_basebackup/pg_basebackup || true
  install_name_tool -change /usr/local/pgsql/lib/libpq.5.dylib "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/backend/replication/libpqwalreceiver/libpqwalreceiver.dylib || true
  ```

  `gmake check-world` also builds ECPG test executables that can record
  `/usr/local/pgsql/lib/libecpg.6.dylib`, `libpgtypes.3.dylib`, and
  `libecpg_compat.3.dylib` from build-tree library IDs. If all ECPG tests abort
  with signal 6 and stderr says `Library not loaded: /usr/local/pgsql/lib/...`,
  patch the build-tree dynamic-library IDs before rerunning:

  ```sh
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libpq.5.dylib" src/interfaces/libpq/libpq.5.dylib
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libecpg.6.dylib" src/interfaces/ecpg/ecpglib/libecpg.6.dylib
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libpgtypes.3.dylib" src/interfaces/ecpg/pgtypeslib/libpgtypes.3.dylib
  install_name_tool -id "$PWD/tmp_install/usr/local/pgsql/lib/libecpg_compat.3.dylib" src/interfaces/ecpg/compatlib/libecpg_compat.3.dylib
  ```

  Also patch inter-library references in `src/interfaces/ecpg/ecpglib` and
  `src/interfaces/ecpg/compatlib`, and patch any already-built ECPG test
  executables if rerunning `src/interfaces/ecpg/test` without rebuilding them.

- Do not run temp-instance smokes that use `tmp_install` in parallel with
  recursive check targets that recreate `tmp_install`. In particular,
  `gmake -C src/test/modules/test_backend_runtime check` deletes and
  reinstalls `tmp_install`; any concurrent smoke using
  `$PWD/tmp_install/usr/local/pgsql/bin` can fail for environmental reasons
  before it reaches the code being tested.

- For focused process-mode regression checks, run the test driver directly with
  the temp install first on `PATH`, for example:

  ```sh
  cd src/test/regress
  PATH="$PWD/../../../tmp_install/usr/local/pgsql/bin:$PWD:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/../../../tmp_install/initdb-template" \
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --dbname=regression guc
  ```

  If `$PWD/../../../tmp_install/initdb-template` or the equivalent relative
  path for the current test directory does not exist, omit `INITDB_TEMPLATE`
  and let `pg_regress` run a fresh `initdb`. A missing template fails before
  SQL starts with a `cp ... initdb-template: No such file or directory` error.

  On macOS, Unix-domain socket paths are limited. Live smokes from this long
  checkout path can fail before SQL starts with `Unix-domain socket path ... is
  too long (maximum 103 bytes)`. Use a short `mktemp -d /tmp/...` directory for
  ad hoc temp clusters that need Unix sockets.

  Threaded temp clusters currently require the database locale to match the
  postmaster process locale. In this checkout the shell commonly reports
  `LC_CTYPE=C.UTF-8`, so direct threaded smokes should initialize clusters with
  `initdb --locale=C.UTF-8` rather than `--no-locale`; otherwise threaded
  client backends fail before SQL starts with
  `database locale is incompatible with threaded backend mode`.

  If killed threaded temp clusters leave SysV shared-memory IDs behind,
  follow-up `initdb` runs can fail during bootstrap with
  `could not create shared memory segment: No space left on device` even when
  disk space is fine. First confirm no PostgreSQL server processes from this
  checkout are still running, then inspect and remove stale segments with
  `ipcs -m` and `ipcrm -m <id>`.

  Many individual regression tests assume fixture objects created by earlier
  `parallel_schedule` groups. If a direct focused run fails with missing tables
  such as `onek` or `tenk1`, rerun with the schedule prefix that builds the
  fixture state, for example:

  ```sh
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --dbname=regression \
    test_setup copy copyselect copydml copyencoding insert insert_conflict \
    create_function_c create_misc create_operator create_procedure create_table create_type create_schema \
    create_index create_index_spgist create_view index_including index_including_gist \
    create_aggregate create_function_sql create_cast constraints triggers select vacuum sanity_check guc
  ```

  The `horology` test has its own date/time fixture dependencies. Run
  `date time timetz timestamp timestamptz interval` before `horology` in direct
  focused runs, matching `parallel_schedule`.

  The `select_parallel` test can produce plan-shape diffs if the direct run
  only includes `create_misc`; include the schedule prefix through
  `create_index`, `vacuum`, `guc`, and `sysviews` before `select_parallel`.

  The `privileges` test has an opening large-object cleanup query whose
  expected output assumes no matching leftover objects. In direct focused runs
  that include both files, run `privileges` before `largeobject`, or run them
  in separate temp instances.

  The `stats` test expects helper objects from `stats_ext`; include
  `stats_ext` before `stats` in direct focused runs.

  The `float8` test expects the permanent `FLOAT8_TBL` fixture from
  `test_setup` after it drops its temporary table. Direct focused runs should
  use at least:

  ```sh
  ./pg_regress --temp-instance=./tmp_check --inputdir=. --bindir= --dlpath=. --dbname=regression \
    test_setup float8
  ```

- `guc_privs` is not a core `src/test/regress` test. It lives under
  `src/test/modules/unsafe_tests`.
- `analyze` is not a core `src/test/regress` test file in this checkout. For
  focused sampling/ANALYZE validation, use a live temp-cluster smoke that
  creates a table, inserts enough rows, runs `ANALYZE`, and verifies visible
  `pg_stats` rows.
- `create_role` is not reliable as a standalone direct `pg_regress` test in
  this checkout. It appears late in `parallel_schedule` and assumes earlier
  fixture/public-schema state; for focused superuser/role-cache validation,
  prefer `roleattributes` plus a live temp-cluster role privilege smoke unless
  you are intentionally running the larger schedule prefix.
- The extension backend-model tests need the test extension module installed
  into the current temp install before direct `pg_regress` runs:

  ```sh
  gmake -C src/test/modules/test_extensions DESTDIR="$PWD/tmp_install" install
  ```

- The threaded backend-runtime TAP fixture uses
  `CREATE EXTENSION test_backend_runtime_threaded`. After changing
  `src/test/modules/test_backend_runtime/test_backend_runtime_threaded.c`, its
  extension control/SQL files, or the module Makefile/meson metadata, reinstall
  that module before manual threaded smokes:

  ```sh
  gmake -C src/test/modules/test_backend_runtime DESTDIR="$PWD/tmp_install" install
  ```

- The backend-runtime state/PMChild regression is expected to be runnable as a
  focused process-mode control after the same module install. The fake
  thread-runtime helper tests should construct `PgThreadBackendRuntimeState`
  objects without installing them into the active SQL backend:

  ```sh
  cd src/test/modules/test_backend_runtime
  PATH="$PWD/../../../../tmp_install/usr/local/pgsql/bin:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/../../../../tmp_install/usr/local/pgsql/lib" \
  ../../../../src/test/regress/pg_regress --temp-instance=./tmp_check \
    --inputdir=. --outputdir=output \
    --bindir="$PWD/../../../../tmp_install/usr/local/pgsql/bin" \
    --dlpath=. test_backend_runtime
  ```

- GUC custom-prefix smoke tests that preload `test_oat_hooks` need that module
  installed into the current temp install first:

  ```sh
  gmake -C src/test/modules/test_oat_hooks DESTDIR="$PWD/tmp_install" install
  ```

- Threaded runtime GUC stack coverage in
  `src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl` verifies
  built-in database/role/startup defaults, `SET LOCAL` rollback/commit
  behavior, `RESET` back to database and startup-packet sources, and custom
  extension GUC `SET LOCAL`/`RESET` semantics through the superuser `LOAD`
  path. Unprivileged `LOAD 'test_backend_runtime_threaded'` is expected to
  fail with the normal library-access policy error unless explicit load
  privileges are granted.

  Manual concurrent threaded GUC smokes should capture background client PIDs
  with `$!` and wait for that explicit list. Do not rely on `jobs -p` in the
  non-interactive zsh shell; it can produce an empty list, causing the harness
  to stop the temp postmaster before the background clients finish.

- Abandoned-client teardown smokes should leave the backend idle in
  transaction before killing the client, matching `background_psql` behavior.
  Do not use `SELECT pg_sleep(...)` as the wait point for that fixture: killing
  the frontend while the backend is inside `pg_sleep` can leave the advisory
  lock visible until the running query observes an interrupt or finishes,
  which tests a different path from idle-client abandonment.

- Direct logical replication parallel-apply smokes should use the upstream
  `src/test/subscription/t/015_stream.pl` interleaved transaction shape:
  start one large transaction, run and commit a second large transaction while
  the first remains open, then commit the first. A single large transaction
  followed by `pg_sleep()` can replicate successfully without proving the
  `STREAM_START`/parallel apply path. The parallel worker is pooled and can be
  hard to catch by polling `pg_stat_activity`; use the subscriber log marker
  for `logical replication parallel apply worker for subscription`, the final
  replicated row/default counts, and a postmaster child-process check as the
  primary smoke evidence.

- Manual threaded slot-sync smokes that use `pg_basebackup -R` should write
  the final `primary_conninfo` containing `dbname=postgres` into
  `postgresql.auto.conf`, not only `postgresql.conf`. The `-R` generated
  `primary_conninfo` in `postgresql.auto.conf` otherwise overrides the later
  config-file value and makes the slot sync worker restart with
  `replication slot synchronization requires "dbname" to be specified in
  "primary_conninfo"` before testing the intended threaded path.

- Threaded checkpointer/background-writer smokes should wait for the
  post-startup handoff. In threaded mode those workers intentionally start as
  processes before recovery forks the startup process, then exit and relaunch
  as thread carriers after `PM_RUN` and after another thread carrier exists.
  Good smoke evidence is one logical `checkpointer` and one logical
  `background writer` in `pg_stat_activity`, no OS child command containing
  `checkpointer` or `background writer` under the postmaster, a successful
  `CHECKPOINT`, and clean fast shutdown. In process-mode compatibility smokes,
  the same workers should still appear as OS child processes.

- Plain `multithreaded=on` temp clusters should complete
  `pg_ctl -m fast stop` cleanly. If a Phase 11 worker smoke hangs during fast
  stop, sample the postmaster before cleanup; a previous blocker was a
  thread-backed worker that consumed logical interrupts without routing
  shutdown requests through `ProcessMainLoopInterrupts()`.

- PostgreSQL TAP tests require the non-core Perl module `IPC::Run`. It is
  installed locally for this checkout under `/Users/samwillis/perl5`; direct
  `prove` invocations with system Perl need the local `PERL5LIB` paths. To
  reinstall or update it locally without relying on system Perl paths, use:

  ```sh
  PERL_MM_USE_DEFAULT=1 \
  PERL_MM_OPT="INSTALL_BASE=$HOME/perl5" \
  PERL_MB_OPT="--install_base $HOME/perl5" \
  cpan -T -i IPC::Run
  ```

  Keep
  `PERL5LIB="$HOME/perl5/lib/perl5:$HOME/perl5/lib/perl5/darwin-thread-multi-2level:$PWD/src/test/perl"`
  in direct TAP commands. This checkout is still configured without
  `--enable-tap-tests`, so recursive `gmake ... check` targets report `TAP
  tests not enabled`. Do not treat that configure-time message as a reason to
  skip TAP coverage; run the direct `prove` command with the local `PERL5LIB`
  path. Direct `prove` runs also need the same harness environment that
  `gmake check` supplies, especially `PG_REGRESS`; if `PG_REGRESS` is missing,
  `PostgreSQL::Test::Cluster->init` can call `system_or_bail()` with an
  undefined command and `prove` may report an empty skip reason before the
  server starts. A minimal direct environment is:

  ```sh
  PERL5LIB="$HOME/perl5/lib/perl5:$HOME/perl5/lib/perl5/darwin-thread-multi-2level:$PWD/src/test/perl" \
  PATH="$PWD/tmp_install/usr/local/pgsql/bin:$PATH" \
  DYLD_LIBRARY_PATH="$PWD/tmp_install/usr/local/pgsql/lib" \
  INITDB_TEMPLATE="$PWD/tmp_install/initdb-template" \
  PG_REGRESS="$PWD/src/test/regress/pg_regress" \
  prove -I src/test/perl src/test/modules/test_backend_runtime/t/001_threaded_runtime.pl
  ```

- In the managed Codex sandbox, PostgreSQL temp-instance tests can fail during
  `initdb` with `could not create shared memory segment: Operation not
  permitted` from `shmget()`. Treat that as a sandbox restriction, not a
  PostgreSQL regression. Rerun the same test outside the sandbox/with
  escalation, or force a POSIX DSM configuration when that is sufficient for
  the check.

- Repeated crash-debugging of threaded temp clusters on macOS can leave stale
  SysV shared-memory segments and semaphore sets even when no `postgres`
  process remains. If `initdb` fails with `shmget(...): No space left on
  device`, first confirm that no PostgreSQL server process is still running:

  ```sh
  ps -axo pid,ppid,stat,command | rg '[p]ostgres|[p]ostmaster|[i]nitdb' || true
  ```

  Prefer removing only detached shared-memory segments (`NATTCH` is zero in
  `ipcs -ma`). If there is no live server to preserve, clear stale IPC objects
  owned by the current user:

  ```sh
  for id in $(ipcs -ma | awk '$1 == "m" && $9 == 0 && $5 == "'$USER'" {print $2}'); do ipcrm -m "$id" || true; done
  for id in $(ipcs -s | awk '$5 == "'$USER'" {print $2}'); do ipcrm -s "$id" || true; done
  ```

- This shell is zsh. Cleanup commands with unmatched globs, such as
  `rm -rf tmp_check_*`, can fail with `no matches found` before the test command
  runs. Use a matched path, `find`, or enable null-glob behavior when cleaning
  optional TAP/regression scratch directories.

## Terminology

- Runtime: one server runtime inside an address space. In process mode, each
  backend process has its own private address space plus shared memory. In
  threaded mode, many backends share one address space.
- Carrier: the physical execution vehicle, such as an OS process, OS thread,
  or future host scheduler worker.
- Backend: a logical PostgreSQL backend identity visible to cancellation,
  statistics, lock ownership, and monitoring.
- Session: SQL session state for a client or pooled logical session.
- Execution: active transaction/query/portal work currently consuming backend
  resources.
- Connection: frontend/backend protocol transport. It is usually a socket in
  native PostgreSQL, but should not be architecturally identical to a session.
