# Phase 8 Thread-Safety Floor Notes

Phase 8 is complete for the non-Windows process-mode path on this checkout.
This note records the implementation slices that now use the explicit
`PG_THREAD_LOCAL` storage qualifier from
`src/include/utils/global_lifetime.h` as a compatibility bridge for
thread-per-session launch. Windows platform shim annotations are
code-review-only until they are built and tested on Windows.

## Completed Slice

The `PG_GLOBAL_*` annotations remain classification-only. Do not make those
macros expand to TLS. Backend-local state that needs process-per-session
semantics in the initial threaded runtime uses explicit `PG_THREAD_LOCAL`
storage until it can move behind an owned runtime, backend, session, or
execution object.

The following state now uses explicit `PG_THREAD_LOCAL` storage:

- current runtime carrier pointers: `CurrentPgCarrier`, `CurrentPgBackend`,
  `CurrentPgSession`, `CurrentPgConnection`, and `CurrentPgExecution`;
- legacy session pointer: `CurrentSession`;
- memory context globals: `CurrentMemoryContext`, `TopMemoryContext`,
  `ErrorContext`, `CacheMemoryContext`, `MessageContext`,
  `TopTransactionContext`, `CurTransactionContext`, `PortalContext`, and
  the memory-context logging recursion guard;
- allocation-set context freelists in `aset.c`: `context_freelists` caches
  deleted default/small allocation contexts for reuse by the current backend
  and must not be shared by concurrent threaded backends;
- resource owner globals: `CurrentResourceOwner`,
  `CurTransactionResourceOwner`, `TopTransactionResourceOwner`, and
  `AuxProcessResourceOwner`, plus the resource-release callback registry and
  optional resource-owner stats counters;
- `MyProc` and `got_deadlock_timeout`;
- deadlock detector workspace allocated by `InitDeadLockChecking()` for the
  current backend;
- tcop usage-stat snapshots used by `ResetUsage()` and `ShowUsage()`;
- lockfile and Unix socket cleanup lists owned by the postmaster or standalone
  runtime;
- PGPROC ownership structures: `ProcGlobal`, `AllProcsShmemPtr`,
  `FastPathLockArrayShmemPtr`, `AuxiliaryProcs`, and `PreparedXactProcs` as
  shared-memory state, plus the proc sizing/request globals as runtime state;
- procarray ownership structures: `procArray`, `allProcs`,
  `KnownAssignedXids`, and `KnownAssignedXidsValid` as shared-memory state,
  recovery-stream XID bookkeeping as runtime state, and backend-local
  transaction visibility caches as TLS state;
- hot-standby recovery-conflict state: recovery lock hash tables, wait backoff,
  and timeout-handler pending flags as backend-local TLS state;
- error stack state: `error_context_stack`, `PG_exception_stack`, `errordata`,
  `errordata_stack_depth`, and `recursion_depth`;
- timeout registration and pending-delivery state in `timeout.c`;
- virtual fd and temporary-file owner state in `fd.c`;
- portal manager session state;
- active portal execution state in `pquery.c`, plus immutable destination
  receiver templates and the permanent `None_Receiver` pointer in `dest.c`;
- logical apply-worker memory/error context state;
- regexp cache state in `regexp.c`: the regexp cache memory context, cached
  compiled-pattern count, and compiled-pattern array are session-local TLS
  state because compiled regexps are writable backend/session cache entries
  allocated under the current backend's memory contexts.
- SQL pseudorandom generator state in `pseudorandomfuncs.c`: `prng_state`
  and `prng_seed_set` are session-local TLS state because `setseed()` and
  later `random*()` calls are user-visible session behavior that must not be
  shared by concurrent threaded sessions.
- deprecated ANALYZE/FDW sampling API state in `sampling.c`: `oldrs` and
  `oldrs_initialized` are backend-local TLS state because the legacy API
  intentionally keeps one common random stream per backend process.
- superuser role lookup cache state in `superuser.c`: `last_roleid`,
  `last_roleid_is_super`, and `roleid_callback_registered` are backend-local
  TLS state because they cache one backend's syscache-backed role lookup and
  syscache invalidation callback registration.
- ACL role-membership cache state in `acl.c`: the cached role OIDs,
  membership lists, and current-database hash filter are session-local TLS
  state because they cache one backend/session's authorization lookups and
  syscache invalidation callback state.
- frontend protocol and connection state: `FrontendProtocol`, `MyProcPort`,
  `MyClientSocket`, `MyCancelKey`, `MyCancelKeyLength`, `PqCommMethods`,
  `FeBeWaitSet`, `whereToSendOutput`, `debug_query_string`, and the libpq
  send/receive buffers in `pqcomm.c`, GSSAPI transport buffers in
  `be-secure-gssapi.c`, and the connection setup timing record in
  `backend_startup.c`;
- authentication and TLS connection-startup state: HBA and ident parser
  context/list handles are runtime-global configuration state, authentication
  method names are immutable state, OpenSSL context/host/BIO-method and
  passphrase reload state are runtime-global SSL configuration state,
  `ssl_loaded_verify_locations` is connection-local TLS state, and PAM
  conversation scratch is connection-local TLS state.
- OAuth validator registration state in `auth-oauth.c`: the loaded validator
  module state, callback table, validator memory context, registered HBA
  options, and option-check flag are classified as runtime-global singleton
  state.  This preserves the current one-validator-per-process model and keeps
  threaded OAuth validator policy tied to the Phase 7 extension backend-model
  gate rather than copying validator state per backend.
- process-title storage in `ps_status.c`: argv/environ relocation state and
  the physical process-title buffer are runtime-global state. The
  `update_process_title` GUC remains session-local, but threaded mode must not
  let multiple logical backends race while clobbering one OS process title;
  Phase 9/10 should expose backend activity through a thread-aware reporting
  path and serialize or suppress physical process-title writes.
- shared-memory message-queue protocol state in `pqmq.c`: the active
  `shm_mq` handle, send-recursion guard, and parallel leader identity are
  backend-local TLS state for the current redirected backend.
- connection-startup warning state in `postinit.c`;
- interrupt pending flags and holdoff counters, including async notify, sinval
  catchup, config reload/shutdown, parallel query, parallel logical apply,
  slot sync, and repack interrupt flags;
- shared-invalidation state in `sinval.c` and `sinvaladt.c`: the shared SI
  queue pointer as shared-memory state, and the processed-message counter,
  recursive receive buffer/counters, and next local transaction ID as
  backend-local state;
- injection-point state in `injection_point.c`: the active injection-point
  table pointer is shared-memory state, while the loaded callback cache is
  backend-local TLS state because it stores per-backend `TopMemoryContext`
  callback lookups derived from the shared table.
- dynahash active sequential-scan tracking state in `dynahash.c`;
- parallel-query backend state in `parallel.c`: worker number,
  worker-initialization flag, fixed parallel state pointer, active parallel
  context list, and parallel leader PID copy;
- process-signal shared/backend state: `ProcSignal` as shared-memory state and
  `MyProcSignalSlot` as the current backend's slot pointer;
- postmaster-signal state in `pmsignal.c`: `PMSignalState` is shared-memory
  state for postmaster/child flags, `num_child_flags` is runtime-global
  postmaster sizing state, and `postmaster_possibly_dead` is runtime-global
  parent-death notification state;
- postmaster child-slot state in `pmchild.c`: child-slot pool sizing,
  freelists, the active child list, and the Valgrind-only child-array witness
  are runtime-global postmaster control-plane state. These remain
  process-mode supervision structures in this phase; later worker-runtime
  phases must decide which entries represent thread-owned in-tree workers
  rather than forked child processes.
- postmaster child-launch metadata in `launch_backend.c`:
  `child_process_kinds` is an immutable generated dispatch table for child
  names, main functions, and shared-memory attachment policy.
- postmaster supervisor state in `postmaster.c`: listen socket tables,
  special-child pointers, startup/shutdown/crash state, connection-gating
  state, signal-pending flags, the postmaster wait set, syslogger redirection
  and SSL/Bonjour service flags, postmaster-death watch handles, and IO-worker
  child tracking are runtime-global control-plane state. These remain
  process-lifetime supervisor state in Phase 8; later worker-runtime phases
  must replace forked in-tree worker launches with runtime-owned threaded
  workers where normal threaded mode requires it.
- connection authentication progress state: `ClientAuthInProgress` is
  connection-local TLS, so error visibility during startup authentication is
  isolated per frontend connection instead of shared across threaded backends.
- common default PRNG state: `pg_global_prng_state` is backend-local TLS, so
  sampling, DSM handle generation, spin-delay jitter, temporary tablespace
  selection, and other backend callers do not race on one shared state vector
  in threaded mode.
- common timing conversion state: the tick-to-nanosecond scale factors,
  timing-initialization flag, TSC enable flag, and TSC frequency cache are
  runtime-global timing infrastructure state. They remain shared process
  configuration in Phase 8, matching the already-classified
  `timing_clock_source` GUC backing variable.
- data-directory file-permission state: `pg_dir_create_mode`,
  `pg_file_create_mode`, and `pg_mode_mask` are runtime-global configuration
  derived from the data directory mode and shared by backend file-creation
  paths.
- runtime CPU feature state: `X86Features` is the process/runtime-wide CPU
  capability cache initialized by `set_x86_features()` and read by optimized
  common/backend code paths, including timing source selection.
- immutable encoding metadata: `pg_enc2gettext_tbl` is now a const pointer
  array of gettext encoding names. It is exported metadata read by backend
  encoding/NLS paths and should not be mutable shared state in threaded mode.
- common logging level state: `__pg_log_level` is runtime-global logging
  configuration for frontend/common logging users and shared code that includes
  `common/logging.h`; it is not backend/session-local state.
- frontend cancellation and print-loop state: `CancelRequested` and
  `cancel_pressed` are runtime-global frontend process flags used by client
  tools and psql-style output code, not backend/session-local state.
- command-line option parser compatibility state: `optarg`, `optind`,
  `opterr`, `optopt`, and `optreset` are runtime-global process state exposed
  for getopt-compatible frontend and utility option parsing. PostgreSQL's
  re-entrant `pg_getopt_ctx` remains available where concurrent parsing is
  needed.
- AIO worker method state in `method_worker.c`: `io_worker_submission_queue`
  and `io_worker_control` are shared-memory state used by submitters, the
  postmaster, and IO workers. `io_worker_queue_size` is runtime configuration,
  and `MyIoWorkerId` is backend-local TLS state for the running IO worker.
- io_uring AIO method state in `method_io_uring.c`: `pgaio_uring_contexts`
  points at the shared-memory array of per-backend io_uring contexts,
  `pgaio_my_uring_context` is backend-local TLS for the current submitter's
  ring, and `pgaio_uring_caps` is runtime-global capability probe state
  computed before shared-memory sizing/initialization.
- standalone spinlock test state in `s_lock.c`: `test_lock` exists only under
  `S_LOCK_TEST` and is runtime-global state for that standalone verification
  binary, not live backend runtime state.
- logical replication launcher and worker identity state in `launcher.c`:
  `LogicalRepCtx` is shared-memory state for the launcher and worker slots,
  while `MyLogicalRepWorker`, the local last-start-times DSA/dshash
  attachments, and the commit-time launcher wakeup flag are backend-local TLS
  state for the logical replication launcher, apply workers, and SQL backends
  that need to wake the launcher after subscription catalog changes.
- common logical replication apply-worker state in `worker.c`: the active
  walreceiver connection, subscription cache pointer and validity flag,
  remote-transaction tracking, skip-LSN state, flush-position bookkeeping,
  apply-error callback scratch, initialization flag, and commit-time worker
  wakeup list are backend-local TLS state for the logical apply worker or
  catalog-changing backend that owns them.  The apply worker's LSN mapping
  list is now explicitly initialized during logical worker startup because a
  TLS `dlist_head` cannot use the self-referential `DLIST_STATIC_INIT`
  initializer safely.
- streamed logical replication apply-worker scratch in `worker.c`: the active
  streamed-transaction flag, streamed XID, spool-file handle, parallel apply
  change counter, and streamed subtransaction table are backend-local TLS state
  for the apply worker currently receiving or replaying a streamed
  transaction.
- logical replication parallel apply-worker state in `applyparallelworker.c`:
  the leader apply worker's transaction-to-worker hash, active worker pool,
  current streamed-transaction worker cache, the parallel worker's shared DSM
  pointer, and the parallel worker's current subtransaction list are
  backend-local TLS state. The DSM records they point at remain shared state
  protected by the existing parallel apply synchronization.
- logical replication table synchronization state in `tablesync.c`:
  `table_states_not_ready` is backend-local TLS state for the logical apply
  worker's cached view of subscription relations that are not yet ready, and
  `copybuf` is backend-local TLS scratch for the table synchronization
  worker's COPY stream.
- logical replication sequence synchronization state in `sequencesync.c`:
  `seqinfos` is backend-local TLS state for the sequence synchronization
  worker's current batch of remote/local sequence metadata.
- logical decoding control state in `logicalctl.c`: `LogicalDecodingCtl` is
  shared-memory state protected by `LogicalDecodingControlLock`, while the
  exported `XLogLogicalInfo` cache and pending barrier-update flag are
  backend-local TLS state so each backend keeps the intended
  transaction-stable view of logical-info WAL logging after a process barrier.
- logical replication origin state in `origin.c`: `replication_states_ctl` and
  `replication_states` are shared-memory state protected by
  `ReplicationOriginLock` and per-origin LWLocks, while
  `replorigin_xact_state` is execution-local TLS state and
  `session_replication_state` is the session-local TLS handle for the current
  backend's acquired origin.
- logical replication relation-map state in `relation.c`:
  `LogicalRepRelMapContext`, `LogicalRepRelMap`, `LogicalRepPartMapContext`,
  and `LogicalRepPartMap` are session-local TLS caches for one logical
  replication backend's remote-to-local relation and partition mappings.
- logical replication slot synchronization state in `slotsync.c`:
  `SlotSyncCtx` is shared-memory control state protected by its spinlock,
  while the dynamic nap interval `sleep_ms` and current-process
  `syncing_slots` flag are backend-local TLS state for the slot-sync worker or
  manual `pg_sync_replication_slots()` caller.
- logical snapshot builder export state in `snapbuild.c`:
  `SavedResourceOwnerDuringExport` and `ExportInProgress` are execution-local
  TLS state used only while exporting a historic snapshot.
- logical synchronization relation-state cache in `syncutils.c`:
  `relation_states_validity` is session-local TLS state for one logical apply
  worker's cached view of pending table and sequence synchronization work.
- pgoutput publication/relation cache state in `pgoutput.c`:
  `publications_valid` and `RelationSyncCache` are session-local TLS state for
  one logical decoding/output plugin instance's publication and relation
  schema cache.
- synchronous replication wait/config state in `syncrep.c`: the parsed
  `SyncRepConfig` pointer and `announce_next_takeover` logging guard are
  runtime-global synchronous-replication state, while `SyncRepWaitMode` is
  backend-local TLS derived from the current backend's `synchronous_commit`
  setting.
- syslogger service state in `syslogger.c`: log rotation timing, EOF/rotation
  flags, active log-file handles, previous log file names, partial-message
  buffers, exported pipe descriptors, and Windows helper-thread state are
  runtime-global logging-service state. Threaded normal mode will still need a
  worker-runtime decision for whether syslogger remains a dedicated service
  thread or is folded into a runtime logging component.
- background-worker registration state in `bgworker.c`: the postmaster-private
  `BackgroundWorkerList` is runtime-global control-plane state, while
  `BackgroundWorkerData` is shared-memory state visible to postmaster and
  regular backends.  The current worker entry pointer, `MyBgworkerEntry`, is
  backend-local TLS state because each running worker backend has its own
  `BackgroundWorker` metadata and code paths such as error reporting,
  parallel worker initialization, and logical apply must not read another
  worker's entry in threaded mode.
- background-writer snapshot throttle state in `bgwriter.c`:
  `last_snapshot_ts` and `last_snapshot_lsn` are backend-local TLS state for
  the bgwriter logical worker. These values throttle standby snapshot logging
  by the active bgwriter and must not become shared scratch state for unrelated
  logical backends in threaded mode.
- checkpointer coordination and progress state in `checkpointer.c`:
  `CheckpointerShmem` is shared-memory state used by backends and the
  checkpointer to coordinate checkpoint requests, fsync requests, and
  completion counters. `ckpt_active`, `ShutdownXLOGPending`,
  `ckpt_start_time`, `ckpt_start_recptr`, `ckpt_cached_elapsed`,
  `last_checkpoint_time`, and `last_xlog_switch_time` are backend-local TLS
  state for the checkpointer logical worker.
- archiver coordination and archive-module state in `pgarch.c`: `PgArch` is
  shared-memory state used to wake the archiver and force directory scans,
  while `last_pgarch_start_time` is runtime-global postmaster restart-throttle
  state. The active archive module callbacks/state, archive memory context,
  ready-file queue, SIGTERM throttle, stop flag, and archive-module
  error-detail string are backend-local TLS state for the archiver logical
  worker.
- WAL summarizer coordination and worker-local state in `walsummarizer.c`:
  `WalSummarizerCtl` is shared-memory state used to publish summarizer
  progress, wake waiters, and expose the active summarizer proc number. The
  sleep backoff, pages-read counter, and last redo pointer considered for
  summary cleanup are backend-local TLS state for the WAL summarizer logical
  worker.
- startup-process signal and progress state in `startup.c`: SIGHUP,
  shutdown, promotion, restore-command, and startup-progress timeout flags,
  plus the active startup-progress phase timestamp, are backend-local TLS
  state for the startup logical worker.
- WAL receiver connection and stream state in `walreceiver.c`: the dynamically
  loaded WAL receiver function table is runtime-global, while the active
  receiver connection, receive segment file metadata, written/flushed stream
  positions, periodic wakeup schedule, and reusable reply message buffer are
  backend-local TLS state for the WAL receiver logical worker.
- online data-checksum worker state in `datachecksum_state.c`:
  `DataChecksumState` is shared-memory state used by SQL callers, the
  data-checksum launcher, and data-checksum workers to coordinate requested
  operations, launcher/worker status, and result handoff. The launcher abort
  flag, launcher-running cleanup flag, and active operation copy are
  backend-local TLS state for the data-checksum launcher or worker logical
  backend.
- autovacuum launcher/worker state in `autovacuum.c`: `AutoVacuumShmem` is
  shared-memory coordination state for the launcher, workers, and postmaster
  worker slots. The launcher signal flag, launcher database list/context,
  Valgrind database-list witness, local anti-wraparound scoring snapshots,
  default freeze ages, autovacuum memory context, current worker `WorkerInfo`
  pointer, and relation storage-parameter cost overrides are backend-local TLS
  state for the autovacuum launcher or worker logical backend.
- process signal-mask templates in `pqsignal.c`: `UnBlockSig`, `BlockSig`,
  and `StartupBlockSig` are runtime-global templates initialized by
  `pqinitmask()`.  They remain shared signal-mask templates; Phase 9/10 must
  still make blocked threaded backends wakeable without relying on
  process-directed Unix signals.
- wait-event wake channel state in `waiteventset.c`: the current
  `WaitEventSetWait()` blocking flag and the signalfd/self-pipe descriptors
  are carrier-local TLS state for the physical thread or process that can be
  woken by the existing latch implementation. This is only a compatibility
  bridge; Phase 9 must still replace process-directed signal wakeups with a
  logical-backend-aware wait/wakeup boundary.
- backend-status shared and local state in `backend_status.c`: the shared
  status arrays and backing string/security buffers are classified as shared
  memory, while `MyBEEntry` and the reader-side local status snapshot table,
  count, and memory context are backend-local TLS state.
- backend/session identity globals: `MyProcPid`, `MyStartTime`,
  `MyStartTimestamp`, `MyLatch`, `MyPMChildSlot`, `MyProcNumber`,
  `ParallelLeaderProcNumber`, `MyDatabaseId`, `MyDatabaseTableSpace`,
  `MyDatabaseHasLoginEventTriggers`, `DatabasePath`, `MyBackendType`, `Mode`,
  and `OutputFileName`;
- backend-local latch state: `LocalLatchData` backing the early `MyLatch`
  pointer and `LatchWaitSet` backing `WaitLatch()`;
- lock-manager and wait-reporting state: heavyweight-lock shared hash tables,
  fast-path strong-lock counters, the main LWLock array, LWLock tranche
  registry, and custom wait-event registry are classified as shared-memory or
  runtime state; local lock hash state, fast-path local-use counts, awaited
  lock cleanup state, condition-variable sleep target, held LWLock stack, and
  local tranche cache are backend-local TLS. `my_wait_event_info` is
  classified as backend-local but deliberately left as a plain compatibility
  pointer in this slice after an exported TLS pointer variant crashed during
  bootstrap on macOS; Phase 9 should move wait-event storage behind the
  thread-compatible wait/wakeup boundary rather than retrying that direct TLS
  pointer shape.
- predicate-lock/SSI state: shared predicate lock target/lock hashes,
  serializable-XID hash, `PredXact` list, conflict pool, finished-transaction
  list, serial control pointer, old-committed transaction pointer, and scratch
  partition lock are classified as shared-memory state; the serializable SLRU
  descriptor, scratch target hash, and serializable sizing cache are runtime
  state; `LocalPredicateLockHash`, `MySerializableXact`, `MyXactDidWrite`,
  and `SavedSerializableXact` are backend-local TLS state.
- speculative insertion lock state in `lmgr.c`: `speculativeInsertionToken` is
  a per-backend counter used while acquiring and releasing speculative
  insertion locks for uniqueness checks. In threaded mode it must follow the
  logical backend, not the carrier thread or whole runtime.
- IPC, backend-exit, and shared-memory setup state: `proc_exit_inprogress`
  and `shmem_exit_inprogress` are backend-local TLS compatibility mirrors of
  the active logical backend's exit state; the one-time `atexit()` registration,
  add-in shared-memory request accumulator, shmem callback/request lists, and
  shmem request state machine are runtime state; fixed shared-memory segment,
  allocator, and shmem index pointers are classified as shared-memory state.
- dynamic shared memory state in `dsm.c` and `dsm_registry.c`: `dsm_init_done`
  is backend-local TLS because each logical backend may lazily initialize DSM
  use, the preallocated DSM and control pointers are shared-memory state, DSM
  control handle/mapping metadata is runtime state, `DSMRegistryCtx` points at
  shared memory, registry entry type names are immutable, and the attached
  registry DSA/dshash objects are backend-local TLS because they are
  per-backend attachment descriptors over shared DSM contents. The
  per-backend DSM segment list is initialized with its owning `PgBackend`,
  not lazily in `dsm.c`, so a backend object is always born with a valid list
  head before DSM creation, attach, detach, or shutdown paths use it.
- storage-manager backend cache state: `MdCxt`, `SMgrRelationHash`, and
  `unpinned_relns` are backend-local TLS state owned by the current backend's
  smgr cache, while the smgr dispatch table and method count are immutable
  state.
- sync-manager pending fsync/unlink state in `sync.c`: `pendingOps`,
  `pendingUnlinks`, `pendingOpsCxt`, the fsync/checkpoint cycle counters, and
  the retry-in-progress flag are backend-local TLS state for the standalone or
  checkpointer-like owner that maintains the pending sync table. The sync
  handler table is immutable state.
- authenticated, session, and effective-user identity state in `miscinit.c`:
  `AuthenticatedUserId`, `SessionUserId`, `OuterUserId`, `CurrentUserId`,
  `SystemUser`, `SessionUserIsSuperuser`, `SecurityRestrictionContext`, and
  `SetRoleIsActive`;
- vacuum execution state: `VacuumCostBalance`, `VacuumCostActive`,
  `parallel_vacuum_worker_delay_ns`, `VacuumFailsafeActive`,
  `VacuumSharedCostBalance`, `VacuumActiveNWorkers`, and
  `VacuumCostBalanceLocal`.  The shared-cost pointer variables still point at
  DSM/parallel-vacuum shared state, but the cached pointer ownership is local
  to the current vacuum execution.
- parallel-vacuum execution state in `vacuumparallel.c`:
  `pv_shared_cost_params` and `shared_params_generation_local`, which cache
  the current parallel-vacuum cost-parameter generation for the active worker.
- ANALYZE execution state in `analyze.c`: `anl_context` and `vac_strategy`.
  These carry the current command's working memory context and buffer access
  strategy through sampling and index-statistics helpers.
- array typanalyze callback bridge state in `array_typanalyze.c`:
  `array_extra_data`, which points at the current ANALYZE command's
  array-element comparison/hash metadata while `compute_array_stats()` and its
  hash callbacks are running.
- size-formatting metadata in `dbsize.c`: `size_pretty_units` is an immutable
  lookup table used by `pg_size_pretty()` and `pg_size_bytes()`.
- debug libxml allocation context state in `xml.c`: `LibxmlContext` is
  backend-local TLS for the optional `USE_LIBXMLCONTEXT` allocator hook path,
  where libxml callbacks allocate into the active backend's top memory
  context.
- generated wait-event view metadata in `wait_event_funcs.c`:
  `waitEventData` is an immutable lookup table for `pg_get_wait_events()`.
- pending server-worker statistics in `pgstat_bgwriter.c` and
  `pgstat_checkpointer.c`: `PendingBgWriterStats` and
  `PendingCheckpointerStats` are backend-local TLS buffers for the logical
  bgwriter/checkpointer worker that accumulates deltas before flushing them to
  shared statistics.
- vacuum tuning GUC backing variables in `vacuum.c`: `vacuum_freeze_min_age`,
  `vacuum_freeze_table_age`, `vacuum_multixact_freeze_min_age`,
  `vacuum_multixact_freeze_table_age`, `vacuum_failsafe_age`,
  `vacuum_multixact_failsafe_age`, `vacuum_max_eager_freeze_failure_rate`,
  `track_cost_delay_timing`, `vacuum_truncate`, `vacuum_cost_delay`, and
  `vacuum_cost_limit`;
- transaction execution state in `xact.c`, including current transaction
  state, subtransaction/command counters, transaction timestamps, parallel
  current-XID state, unreported subtransaction XIDs, transaction abort context,
  transaction flags, logical-streaming system-scan state, and transaction
  sampling state;
- cumulative-statistics transaction stack state in `pgstat_xact.c`:
  `pgStatXactStack`, which is allocated in `TopTransactionContext`, tracks
  relation and dropped-object stats for the current transaction/subtransaction
  tree, and is cleared at transaction or prepared-transaction end;
- cumulative-statistics infrastructure state in `pgstat.c`: the current
  backend's `pgStatLocal` shared-memory handles and snapshot cache, the fixed
  stats flush flag, pending-stats memory context/list, forced-flush and
  snapshot-clear flags, and assertion-only initialization/shutdown guards are
  backend-local TLS state.  The built-in stats-kind descriptor table is
  immutable state, while the custom stats-kind descriptor registry remains
  runtime-global registration state constrained by the extension preload gate.
- cumulative-statistics shared-entry reference cache state in
  `pgstat_shmem.c`: each backend's shared-entry reference hash, reference-cache
  age, and attribution memory contexts are backend-local TLS state. They cache
  references to shared statistics entries owned by `pgStatLocal.shmem` and must
  not be shared between logical backends in threaded mode.
- cumulative database-statistics pending state in `pgstat_database.c`:
  backend-local pending I/O, active-time, idle-in-transaction-time, and
  commit/rollback counters use TLS until they are flushed to shared
  statistics.  Session disconnect cause and the last session report timestamp
  are session-local TLS state.
- cumulative per-kind pending-statistics state in `pgstat_backend.c`,
  `pgstat_function.c`, `pgstat_io.c`, `pgstat_lock.c`, `pgstat_slru.c`, and
  `pgstat_wal.c`: backend-local pending I/O, lock, SLRU, backend, function
  timing, and WAL baseline counters use TLS until the current backend flushes
  them to shared statistics.  Bgwriter and checkpointer pending counters are
  intentionally left for the server-owned worker runtime audit rather than
  treated as regular client-backend state.
- transaction-owned combo CID maps in `combocid.c` and relation storage
  pending-delete/sync cleanup queues in `storage.c`;
- WAL record construction state in `xloginsert.c`, including registered buffer
  and data arrays, main-data chain state, current insert flags, header scratch
  storage, and the WAL insertion memory context;
- WAL insertion position state in `xlog.c`: `ProcLastRecPtr`,
  `XactLastRecEnd`, and `XactLastCommitEnd`;
- WAL backend-local insertion/cache state in `xlog.c`: `RedoRecPtr`,
  `doPageWrites`, the private `LogwrtResult` copy, WAL insertion lock
  ownership (`MyLockNo` and `holdingAllLocks`), and the WAL debug memory
  context;
- WAL shared-memory/runtime handles in `xlog.c`: `XLogCtl` and the cached
  `WALInsertLocks` pointer as shared-memory state, plus
  `UsableBytesInSegment` as derived runtime state;
- WAL backend-local recovery/cache state in `xlog.c`: the cached recovery
  status and WAL insert permission state, the open WAL segment FD cache,
  min-recovery-point cache, and local data-checksum state;
- WAL control/checkpoint runtime state in `xlog.c`: checkpoint distance
  estimates, `CheckpointStats`, deferred WAL-consistency checking state,
  recovery full-page-write replay state, and the startup-only local
  `pg_control` transfer buffer.  The durable `pg_control` image pointer is
  classified as shared-memory state;
- server executable startup state in `main.c`: `progname` and the
  `reached_main` crash-handler guard are runtime-global startup state, while
  the dispatch option name table is immutable state;
- SQL backup session state in `xlog.c`: `sessionBackupState`, which tracks
  the session that started a SQL-callable backup;
- SQL backup function session state in `xlogfuncs.c`: `backup_state`,
  `tablespace_map`, and `backupcontext`, which carry data from
  `pg_backup_start()` to `pg_backup_stop()` in the same session;
- WAL recovery prefetch state in `xlogprefetcher.c`: the prefetch
  reconfiguration generation counter as runtime state and the recovery
  prefetch statistics block as shared-memory state;
- WAL recovery mode state in `xlogrecovery.c` and `xlogutils.c`: archive and
  standby mode flags, signal-file startup flags, checkpoint/redo start
  locations, `InRecovery`, `InRedo`, `standbyState`, and the consistency flag
  as runtime state.  The `XLogRecoveryCtl` pointer is classified as
  shared-memory state;
- WAL recovery replay state in `xlogrecovery.c` and `xlogutils.c`: startup
  process WAL reader/prefetcher pointers, WAL source/read bookkeeping,
  recovery receipt and backup-end state, consistency-check buffers,
  recovery-stop scratch state, and invalid-page replay bookkeeping as runtime
  state.  The local hot-standby and promotion caches use backend-local TLS, and
  `xlogSourceNames` is classified as immutable state;
- Wait-for-LSN state in `xlogwait.c`: `waitLSNState`, which points to the
  shared wait queues and per-backend wait records, as shared-memory state;
- base backup state in `basebackup.c` and `basebackup_target.c`: per-backup
  recovery/checksum/noverify state as execution-local TLS, backup exclusion
  directory names as immutable state, and the base-backup target registry as
  runtime state;
- bootstrap-mode state in `bootstrap.c` and `bootparse.y`: relation, tuple,
  type-cache, parser line, memory context, and deferred-index build state as
  runtime state.  Bootstrap mode remains a deliberate process-lifetime
  exception rather than a threaded client-backend path;
- WAL redo temporary memory contexts in GIN, GiST, btree, and SP-GiST redo
  modules;
- prepared-transaction state in `twophase.c`: `TwoPhaseState` as
  shared-memory state, `MyLockedGxact` and the exit-registration flag as
  backend-local state, and 2PC state-file assembly records as execution-local
  state;
- transaction characteristic GUC backing variables in `xact.c`: the
  session-local `DefaultXact*` defaults and the execution-local current
  `Xact*` isolation, read-only, and deferrable state;
- transaction callback lists in `xact.c`, now session-local TLS state;
- snapshot manager execution state in `snapmgr.c`, including current,
  secondary, catalog, historic, registered, active, exported, and first-xact
  snapshots, plus `TransactionXmin`, `RecentXmin`, tuple CID mapping, and
  `FirstSnapshotSet`;
- GUC manager state in `guc.c`: `GUCMemoryContext`, the session-local mutable
  `guc_variables` copy, `guc_hashtab`, `guc_nondef_list`, `guc_stack_list`,
  `guc_report_list`, `reporting_enabled`, and `GUCNestLevel`;
- GUC immutable lookup metadata in `guc.c` and `guc_tables.c`: unit hint
  strings, unit conversion tables, old-name mappings, and display-name tables
  for GUC contexts, sources, groups, and types. The custom-GUC reserved-prefix
  list remains runtime-global registration state governed by the extension
  backend-model gate.
- GUC config-file scanner state in `guc-file.l`: `ConfigFileLineno`,
  `GUC_flex_fatal_errmsg`, and `GUC_flex_fatal_jmp` are execution-local TLS
  state used while parsing one configuration file/include tree and handling
  scanner fatal-error recovery.
- GUC check-hook error state: `GUC_check_errcode_value`,
  `GUC_check_errmsg_string`, `GUC_check_errdetail_string`, and
  `GUC_check_errhint_string`;
- exported GUC backing variables that are heavily used by session-local code,
  including the timeout and lock-wait GUCs in `proc.c`, startup and resource
  GUCs in `globals.c` and `miscinit.c`, tcop logging/connection GUCs, RLS
  state, and the exported logging/debug GUCs in `guc_tables.c` including
  `check_function_bodies`;
- planner, analyze, GEQO, and JIT GUC backing variables, including
  `default_statistics_target`, the `jit_*` cost and feature toggles,
  `enable_geqo`, the GEQO tuning variables, planner cost constants, path
  enablement toggles, parallel planner toggles, partition-pruning toggles,
  collapse limits, `constraint_exclusion`, and the eager/distinct/self-join
  planner toggles;
- planner extension ID mapping state: the planner-extension name array and
  assigned/allocated counters in `extendplan.c`, GEQO's cached planner
  extension ID, `disable_cost`, and the predicate proof cache in `predtest.c`
  are session-local TLS state. Planner-extension IDs are explicitly not stable
  across backends today, and the proof cache is a per-session syscache-backed
  lookup cache.
- exported session-facing GUC backing variables in `guc_tables.c`, including
  `application_name`, `role_string`, `tcp_keepalives_idle`,
  `tcp_keepalives_interval`, `tcp_keepalives_count`, and `tcp_user_timeout`.
  `in_hot_standby_guc` remains deliberately separate because it reflects
  recovery/runtime state rather than per-session user state;
- session SQL-behavior GUC backing variables outside `guc_tables.c`, including
  `Array_nulls`, `backslash_quote`, `bytea_output`, `extra_float_digits`,
  `quote_all_identifiers`, `Transform_null_equals`, `xmlbinary`, and
  `xmloption`. The frontend `fe_utils` `quote_all_identifiers` variable is a
  separate client-side option and remains plain frontend state.
- session locale, authorization, and compatibility GUC backing variables in
  `guc_tables.c`: `client_encoding_string`, `datestyle_string`,
  `timezone_string`, `log_timezone_string`,
  `timezone_abbreviations_string`, `session_authorization_string`,
  `restrict_nonsystem_relation_kind_string`, `phony_random_seed`,
  `default_with_oids`, `standard_conforming_strings`, and
  `ssl_renegotiation_limit`;
- timezone and encoding state behind those GUCs, including
  `session_timezone`, `log_timezone`, and the `mbutils.c` encoding/conversion
  cache state for `ClientEncoding`, `DatabaseEncoding`, `MessageEncoding`,
  active conversion functions, pending startup client encoding, and cached
  conversion function lookup records.
- date/time token lookup state in `datetime.c`: exported month/day name tables
  are immutable state; the active timezone-abbreviation table and timezone
  abbreviation decode cache are session-local TLS because they depend on
  `timezone_abbreviations` and `TimeZone`; the static date and interval token
  lookup caches are backend-local TLS memoization over immutable token tables.
- date/time and numeric formatting state in `formatting.c`: fixed English,
  AD/BC, AM/PM, roman numeral, and ordinal lookup tables are immutable state;
  parsed date/time and numeric format-picture caches, entry counts, and aging
  counters are backend-local TLS state because they are writable cache
  metadata allocated under the current backend's `TopMemoryContext`.
- degree-based floating-point trigonometry state in `float.c`: the
  deliberately non-static degree input values are immutable state, while the
  lazily computed trigonometric constants and initialization flag are
  backend-local TLS cache state. This avoids runtime-global writes during
  threaded execution without changing the compiler-behavior guard described by
  `init_degree_constants()`.
- numeric, lock-name, and text-search lookup metadata: numeric constant
  templates in `numeric.c`, lock tag name tables in `lockfuncs.c`, and static
  parser/spell lookup strings in `wparser_def.c` and `spell.c` are immutable
  state. They are shared read-only metadata, not per-backend mutable cache
  state.
- locale GUC backing variables and derived locale cache state in
  `pg_locale.c`, including `locale_messages`, `locale_monetary`,
  `locale_numeric`, `locale_time`, `icu_validation_level`,
  `localized_abbrev_days`, `localized_full_days`,
  `localized_abbrev_months`, `localized_full_months`, the `lconv` cache,
  `default_locale`, `CollationCacheContext`, `CollationCache`, and the
  last-used collation cache entry. The fixed `c_locale` descriptor is
  immutable singleton state, while the ICU string converter in
  `pg_locale_icu.c` is session-local TLS as documented by the existing
  per-session converter comment;
- additional session USERSET GUC backing variables outside `guc_tables.c`:
  `default_toast_compression`, `trace_syncscan`, `Password_encryption`, and
  `createrole_self_grant`. The derived assign-hook state for
  `createrole_self_grant`, including the parsed role-grant options, is also
  session-local TLS state.
- command/session GUC backing variables outside `guc_tables.c`:
  `default_tablespace`, `temp_tablespaces`,
  `allow_in_place_tablespaces`, `SessionReplicationRole`,
  `event_triggers`, and `Extension_control_path`;
- extension command execution state in `extension.c`: `creating_extension` and
  `CurrentExtensionObject`.
- extension sibling lookup cache state in `extension.c`: `ext_sibling_list` is
  backend-local TLS state allocated under the current backend's
  `CacheMemoryContext` and invalidated by the current backend's syscache
  callback path.
- cached function execution table in `funccache.c`: `cfunc_hashtable` is a
  session-local TLS cache whose entries are allocated in `TopMemoryContext`
  and keyed by function OID, call context, argument types, and result
  descriptor.
- reloption cache state in `attoptcache.c` and `spccache.c`: the attribute
  options cache and tablespace options cache are session-local TLS hash tables
  allocated under `CacheMemoryContext` and invalidated by the current backend's
  syscache callback path.
- event-trigger cache state in `evtcache.c`: `EventTriggerCache`,
  `EventTriggerCacheContext`, and `EventTriggerCacheState` are session-local
  TLS state allocated under `CacheMemoryContext` and invalidated by the
  current backend's syscache callback path.
- relfilenumber map cache state in `relfilenumbermap.c`:
  `RelfilenumberMapHash` and the prebuilt `relfilenumber_skey` scan keys are
  session-local TLS state initialized under the current backend's
  `CacheMemoryContext`.
- type cache and record typmod cache state in `typcache.c`: the main type
  cache hash tables, domain-entry list, in-progress lookup stack,
  registered-record hash/array, next local record typmod, and tuple
  descriptor identifier counter are session-local TLS state. Shared
  record-typmod registry handles remain owned by `CurrentSession` and point
  to DSM/DSA-backed state used for parallel-query sharing.
- syscache wrapper state in `syscache.c`: `SysCache`, `CacheInitialized`, and
  the derived relation/supporting-relation OID lookup arrays are session-local
  TLS state initialized by the current backend's `InitCatalogCache()` path.
- invalidation dispatcher state in `inval.c`: transaction and inplace
  invalidation message arrays and stack pointers are execution-local TLS state
  owned by the current transaction/critical-section path, while syscache,
  relcache, and relsync callback registries are session-local TLS state
  registered by caches loaded in the current backend.
- relation mapper state in `relmapper.c`: loaded shared and local relation-map
  snapshots are session-local TLS cache state reloaded on relmap
  invalidation, while active and pending relation-map update buffers are
  execution-local TLS state owned by the current transaction or parallel worker
  restore path.
- catalog cache state in `catcache.c`: `CacheHdr` is session-local TLS state
  for the current backend's catalog-cache header and cache list, while
  `catcache_in_progress_stack` is execution-local TLS state used to mark
  in-progress cache entries dead when invalidations arrive during entry or
  list construction.
- relation cache state in `relcache.c`: `RelationIdCache`,
  `criticalRelcachesBuilt`, `criticalSharedRelcachesBuilt`,
  `relcacheInvalsReceived`, and `OpClassCache` are session-local TLS cache
  state for the current backend, while relation-build in-progress tracking,
  end-of-transaction relation cleanup lists, and deferred tuple-descriptor
  cleanup arrays are execution-local TLS state owned by the current
  transaction or relation-build path.
- event-trigger query execution state in `event_trigger.c`:
  `currentEventTriggerState`, the stack head for SQL-drop, table-rewrite, and
  DDL command collection state owned by the currently running utility command.
- after-trigger transaction-tree state in `trigger.c`: the `afterTriggers`
  struct owns deferred event lists, per-query trigger queues, subtransaction
  restore points, SET CONSTRAINTS state, and deferred batch callbacks for the
  current transaction tree.
- GIN session USERSET GUC backing variables: `GinFuzzySearchLimit` and
  `gin_pending_list_limit`.
- async notify tracing USERSET GUC backing variable: `Trace_notify`.
- async notification state in `async.c`: `asyncQueueControl` as shared-memory
  state, notification SLRU and global channel hash handles as runtime state,
  local LISTEN state as session-local state, and pending LISTEN/NOTIFY plus
  signal workspace as execution-local state;
- text-search session GUC/cache state in `ts_cache.c`: `TSCurrentConfig`,
  `TSCurrentConfigCache`, the parser/dictionary/config cache hash tables, and
  their last-used fast-path pointers.
- dynamic loader state in `dfmgr.c` and `fmgr.c`: `Dynamic_library_path` is a
  session GUC backing variable, `file_list`, `file_tail`, and the rendezvous
  variable hash are runtime-global dynamic-library state governed by the Phase
  7 extension backend-model gate, and `CFuncHash` is a session-local TLS cache
  for `pg_proc`-derived C function addresses.
- plan-cache mode session GUC backing variable: `plan_cache_mode`.
- table access method and synchronized-scan session GUC backing variables:
  `default_table_access_method` and `synchronize_seqscans`.
- generic rb-tree sentinel state in `rbtree.c`: the shared `RBTNIL` sentinel
  node is immutable singleton state used by all rb-tree instances.
- namespace/search-path session state in `namespace.c`: the
  `namespace_search_path` GUC backing variable, active/base search path
  derived state, temp namespace ownership state, and the search-path cache.
- large-object session/transaction state in `inv_api.c`: the
  `lo_compat_privileges` GUC backing variable and the cached
  `pg_largeobject` heap/index relation handles `lo_heap_r` and `lo_index_r`.
- large-object descriptor state in `be-fsstubs.c`: the open-descriptor cookie
  table, cookie-table size, cleanup-needed flag, and private large-object
  memory context are execution-local TLS state cleared at transaction end.
- sort session GUC backing variables in `tuplesort.c`: `trace_sort` and the
  debug-build `optimize_bounded_sort`.
- commit behavior session GUC backing variables: `synchronous_commit` in
  `xact.c`, plus `CommitDelay` and `CommitSiblings` in `xlog.c`.
- query/statistics session state: `compute_query_id`, `query_id_enabled`,
  `pgstat_fetch_consistency`, `pgstat_track_activities`,
  `pgstat_track_counts`, and `pgstat_track_functions`.
- executor instrumentation counters: `pgBufferUsage`, `pgWalUsage`, and the
  private parallel-query baseline copies in `instrument.c`. These counters
  accumulate one backend's buffer and WAL usage so callers can compute deltas
  around a query, plan node, or parallel-query section.
- expression interpreter dispatch lookup state in `execExprInterp.c`:
  `dispatch_table` and `reverse_dispatch_table` are backend-local TLS state
  under computed-goto dispatch. This avoids sharing the lazy
  `ExecInitInterpreter()` setup path between concurrent threaded backends.
- logging/error-reporting session state: `Log_error_verbosity`,
  `log_min_messages_string`, and the processed `backtrace_function_list`
  derived from `backtrace_functions`.
- logging/error-reporting backend and execution state in `elog.c`: formatted
  start-time buffers and log-line prefix counters are backend-local TLS state,
  formatted log-time buffers and saved timestamp/formatting state are
  execution-local TLS state, and syslog plus Windows backtrace initialization
  handles remain runtime-global logging state.
- guarded developer node-test GUC backing variables:
  `Debug_copy_parse_plan_trees`, `Debug_raw_expression_coverage_test`, and
  `Debug_write_read_parse_plan_trees`.
- node serialization/parser scratch state in `outfuncs.c` and `read.c`:
  `write_location_fields`, `pg_strtok_ptr`, and the
  `DEBUG_NODE_TESTS_ENABLED` `restore_location_fields` flag are execution-local
  TLS state saved and restored around one `nodeToString()` or `stringToNode()`
  operation.
- parser operator lookup state in `parse_oper.c`: `OprCacheHash` is a
  session-local TLS cache populated by the current backend and flushed through
  the current backend's syscache callback path. The recursive-CTE diagnostic
  string table in `parse_cte.c` is immutable metadata.
- regex locale state in `regc_pg_locale.c`: `pg_regex_locale` is active regex
  operation state, while `pg_ctype_cache_list` is a session-local ctype probe
  cache. Regex character-class and error strings are immutable metadata.
- fixed replication metadata: the libpq walreceiver callback table and logical
  replication conflict type-name table are immutable metadata.
- port-level semaphore and shared-memory attachment state: semaphore arrays
  stored in shared memory, OS shared-memory segment identifiers/addresses, and
  anonymous shared-memory backing pointers are classified as shared-memory
  state, while OS semaphore allocation counters and cleanup handle arrays are
  runtime-global startup/shutdown state.
- replication slot ownership state: `ReplicationSlotCtl` is shared memory,
  `MyReplicationSlot` is the current backend's slot pointer and uses TLS, and
  synchronized-standby-slot parsed configuration plus the oldest confirmed
  flush LSN cache are runtime-global state.
- statistics function argument descriptor tables in `attribute_stats.c`,
  `extended_stats_funcs.c`, and `relation_stats.c` are immutable metadata used
  to validate SQL-callable statistics update functions.
- storage and I/O session GUC backing variables:
  `backend_flush_after`, `effective_io_concurrency`, `file_copy_method`,
  `ignore_checksum_failure`, `io_combine_limit`,
  `io_combine_limit_guc`, `maintenance_io_concurrency`,
  `track_io_timing`, and `zero_damaged_pages`.
- temporary-file tablespace selection state in `fd.c`:
  `tempTableSpaces`, `numTempTableSpaces`, and `nextTempTableSpace`.
- lock-manager session GUC backing variables:
  `Debug_deadlocks`, `Trace_lock_oidmin`, `Trace_lock_table`,
  `Trace_locks`, `Trace_lwlocks`, `Trace_userlocks`, and
  `log_lock_failures`.
- WAL session GUC backing variables and derived session state:
  `XLOG_DEBUG`, `track_wal_io_timing`, `wal_compression`,
  `wal_consistency_checking`, `wal_consistency_checking_string`,
  `wal_init_zero`, and `wal_recycle`.
- extension hook registries exported through object access, EXPLAIN, executor,
  planner/path, parser, utility, row-security, logging, selectivity/cache,
  fmgr, authentication, SSL, and shared-memory startup APIs as runtime-global
  registration state. These hooks are intentionally shared by one runtime;
  threaded-mode mutation is governed by the Phase 7 extension backend-model
  gate rather than copied per session.
- EXPLAIN extension registries in `explain_state.c`: the extension-name and
  extension-option arrays plus assigned/allocated counters are runtime-global
  registration state. The per-command extension payload remains in
  `ExplainState`.
- security-label provider registry state in `seclabel.c`: the provider list
  registered by security-label modules is runtime-global registration state.
  Threaded-mode mutation/loading is governed by the Phase 7 extension
  backend-model gate.
- extensible-node and custom-scan method registries in `extensible.c`:
  `extensible_node_methods` and `custom_scan_methods` are runtime-global
  extension registration tables. Threaded-mode mutation/loading is governed by
  the Phase 7 extension backend-model gate.
- SPI API and connection-stack state in `spi.c`: `SPI_processed`,
  `SPI_tuptable`, `SPI_result`, `_SPI_stack`, `_SPI_current`,
  `_SPI_stack_depth`, and `_SPI_connected`. SPI exposes its result variables
  through the extension ABI and saves/restores them across nesting levels, but
  the state still belongs to one backend's current SPI call stack.
- final backend-facing USERSET/SUSET GUC backing variables and required
  derived state: `debug_discard_caches`,
  `debug_logical_replication_streaming`, `log_replication_commands`,
  `logical_decoding_work_mem`, `max_stack_depth`,
  `max_stack_depth_bytes`, `stack_base_ptr`, `update_process_title`,
  `wal_receiver_timeout`, `wal_sender_shutdown_timeout`,
  `wal_sender_timeout`, and `wal_skip_threshold`.
- REPACK concurrent decoding leader state in `repack.c`: `decoding_worker` is
  backend-local TLS state for the client backend that launched the decoding
  worker and owns the DSM, background-worker handle, and error queue for the
  current REPACK command.
- REPACK concurrent decoding worker state in `repack_worker.c`: the worker
  identity flag, current decoded WAL segment, DSM segment handle, and relation
  filter locators are backend-local TLS state for the REPACK decoding worker
  logical backend.
- JIT provider loader state in `jit.c`: the provider callback table and
  provider load success/failure cache are session-local TLS state. This
  matches the `jit_provider` session GUC and prevents threaded sessions from
  sharing one provider-load result or callback table.
- LLVM JIT provider state in `llvmjit.c`: type/function-reference caches,
  loaded bitcode module handles, session initialization state, module
  generation counters, context-use counters, target/triple/layout handles,
  thread-safe LLVM context handles, and ORC JIT instances are session-local
  TLS state. The separate `llvmjit_types.c` globals are bitcode-only template
  symbols and are classified as immutable template metadata rather than
  server runtime state.

The frontend utility `quote_all_identifiers` global is explicitly classified
as `PG_GLOBAL_DYNAMIC`, not as backend session state. The backend GUC backing
variable with the same name was already classified as `PG_THREAD_LOCAL`
`PG_GLOBAL_SESSION` in `ruleutils.c` and `builtins.h`; the frontend variable
is not part of backend threaded-session state.

The following GUC backing variables are now explicitly classified as
runtime-global, not thread-local, because they describe server build,
postmaster, shared-memory, or startup-computed runtime state:

- preset/runtime GUC backing variables in `guc_tables.c`: `assert_enabled`,
  `block_size`, `data_directory`, `debug_io_direct_string`,
  `effective_wal_level`, `exec_backend_enabled`, `huge_pages`,
  `huge_page_size`, `huge_pages_status`, `integer_datetimes`,
  `max_function_args`, `max_identifier_length`, `max_index_keys`,
  `num_os_semaphores`, `segment_size`, `server_encoding_string`,
  `server_version_num`, `server_version_string`,
  `shared_memory_size_in_huge_pages`, `shared_memory_size_mb`, and
  `wal_block_size`.
- postmaster/control-plane and auxiliary-writer GUC backing variables:
  `AuthenticationTimeout`, `BgWriterDelay`,
  `CheckPointCompletionTarget`, `CheckPointTimeout`, `CheckPointWarning`,
  `EnableSSL`, `ListenAddresses`, `Log_RotationAge`, `Log_RotationSize`,
  `Log_directory`, `Log_file_mode`, `Log_filename`,
  `Log_truncate_on_rotation`, `Logging_collector`, `PostPortNumber`,
  `PreAuthDelay`, `ReservedConnections`, `SuperuserReservedConnections`,
  `Unix_socket_directories`, `WalWriterDelay`, `WalWriterFlushAfter`,
  `bonjour_name`, `enable_bonjour`, `log_hostname`,
  `log_startup_progress_interval`, `remove_temp_files_after_crash`,
  `restart_after_crash`, `send_abort_for_crash`, and
  `send_abort_for_kill`.
- autovacuum launcher/worker GUC backing variables:
  `Log_autoanalyze_min_duration`, `Log_autovacuum_min_duration`,
  `autovacuum_analyze_score_weight`, `autovacuum_anl_scale`,
  `autovacuum_anl_thresh`, `autovacuum_freeze_max_age`,
  `autovacuum_freeze_score_weight`, `autovacuum_max_workers`,
  `autovacuum_multixact_freeze_max_age`,
  `autovacuum_multixact_freeze_score_weight`, `autovacuum_naptime`,
  `autovacuum_start_daemon`, `autovacuum_vac_cost_delay`,
  `autovacuum_vac_cost_limit`, `autovacuum_vac_ins_scale`,
  `autovacuum_vac_ins_thresh`, `autovacuum_vac_max_thresh`,
  `autovacuum_vac_scale`, `autovacuum_vac_thresh`,
  `autovacuum_vacuum_insert_score_weight`,
  `autovacuum_vacuum_score_weight`, `autovacuum_work_mem`, and
  `autovacuum_worker_slots`.
- shared storage, file, and AIO runtime GUC backing variables:
  `NBuffers`, `bgwriter_flush_after`, `bgwriter_lru_maxpages`,
  `bgwriter_lru_multiplier`, `checkpoint_flush_after`,
  `data_sync_retry`, `dynamic_shared_memory_type`, `file_extend_method`,
  `io_max_combine_limit`, `io_max_concurrency`, `io_max_workers`,
  `io_method`, `io_min_workers`, `io_worker_idle_timeout`,
  `io_worker_launch_interval`, `io_worker_queue_size`, `max_files_per_process`,
  `min_dynamic_shared_memory`, `recovery_init_sync_method`, and
  `shared_memory_type`.
- lock-manager sizing GUC backing variables:
  `max_locks_per_xact`, `max_predicate_locks_per_page`,
  `max_predicate_locks_per_relation`, and
  `max_predicate_locks_per_xact`.
- server-wide error-log destination and syslog GUC backing variables:
  `Log_destination`, `Log_destination_string`, `Log_line_prefix`,
  `syslog_facility`, `syslog_ident_str`, `syslog_sequence_numbers`, and
  `syslog_split_messages`.
- core WAL runtime GUC backing variables and derived runtime state:
  `CheckPointSegments`, `EnableHotStandby`, `XLOGbuffers`,
  `XLogArchiveCommand`, `XLogArchiveMode`, `XLogArchiveTimeout`,
  `data_checksums`, `fullPageWrites`, `log_checkpoints`,
  `max_slot_wal_keep_size_mb`, `max_wal_size_mb`, `min_wal_size_mb`,
  `wal_decode_buffer_size`, `wal_keep_size_mb`, `wal_level`,
  `wal_log_hints`, `wal_retrieve_retry_interval`, `wal_segment_size`, and
  `wal_sync_method`.
- WAL resource-manager registry state: `RmgrTable` is runtime-global.
  Custom resource-manager registration remains restricted to
  `shared_preload_libraries` initialization, before threaded sessions can run.
- relation-options registry state in `reloptions.c` is runtime-global:
  built-in option definition arrays, the derived parser table, custom option
  storage, and custom kind allocation counters. Contrib modules such as
  `bloom` use the global registration APIs, so threaded contrib support needs
  a runtime registration policy or lock rather than per-session copies.
- recovery and standby runtime GUC backing variables and derived recovery
  target state: `PrimaryConnInfo`, `PrimarySlotName`,
  `archiveCleanupCommand`, `ignore_invalid_pages`, `in_hot_standby_guc`,
  `log_recovery_conflict_waits`, `max_standby_archive_delay`,
  `max_standby_streaming_delay`, `recoveryEndCommand`,
  `recoveryRestoreCommand`, `recoveryTarget`, `recoveryTargetAction`,
  `recoveryTargetInclusive`, `recoveryTargetLSN`, `recoveryTargetName`,
  `recoveryTargetTLI`, `recoveryTargetTLIRequested`,
  `recoveryTargetTime`, `recoveryTargetTimeLineGoal`,
  `recoveryTargetXid`, `recovery_min_apply_delay`, `recovery_prefetch`,
  `recovery_target_lsn_string`, `recovery_target_name_string`,
  `recovery_target_string`, `recovery_target_time_string`,
  `recovery_target_timeline_string`, `recovery_target_xid_string`,
  `curFileTLI`, `expectedTLEs`, and `wal_receiver_create_temp_slot`.
- libpq, authentication, SSL, socket, and connection-startup runtime GUC
  backing variables: `SSLCipherList`, `SSLCipherSuites`, `SSLECDHCurve`,
  `SSLPreferServerCiphers`, `Trace_connection_negotiation`,
  `Unix_socket_group`, `Unix_socket_permissions`, `log_connections`,
  `log_connections_string`, `md5_password_warnings`,
  `oauth_validator_libraries_string`,
  `password_expiration_warning_threshold`, `pg_gss_accept_delegation`,
  `pg_krb_caseins_users`, `pg_krb_server_keyfile`,
  `scram_sha_256_iterations`, `ssl_ca_file`, `ssl_cert_file`,
  `ssl_crl_dir`, `ssl_crl_file`, `ssl_dh_params_file`, `ssl_key_file`,
  `ssl_library`, `ssl_max_protocol_version`, `ssl_min_protocol_version`,
  `ssl_passphrase_command`, `ssl_passphrase_command_supports_reload`, and
  `ssl_sni`.
- replication, WAL summarization, archive-library, notification queue, commit
  timestamp, prepared-transaction, and backend-status runtime GUC backing
  variables: `SyncRepStandbyNames`, `XLogArchiveLibrary`,
  `hot_standby_feedback`, `idle_replication_slot_timeout_secs`,
  `max_active_replication_origins`, `max_logical_replication_workers`,
  `max_notify_queue_pages`, `max_parallel_apply_workers_per_subscription`,
  `max_prepared_xacts`, `max_repack_replication_slots`,
  `max_replication_slots`, `max_sync_workers_per_subscription`,
  `max_wal_senders`, `pgstat_track_activity_query_size`, `summarize_wal`,
  `sync_replication_slots`, `synchronized_standby_slots`,
  `track_commit_timestamp`, `wal_receiver_status_interval`, and
  `wal_summary_keep_time`.
- timing runtime GUC backing variable: `timing_clock_source`. Although its
  GUC context is `PGC_SUSET`, the common timing conversion state is currently
  process-wide, so this variable remains runtime-global until the timing
  subsystem is given an explicit per-session or per-carrier abstraction.
- server start and configuration reload timestamps: `PgStartTime` and
  `PgReloadTime` are runtime-global state set by postmaster, standalone
  startup, configuration reload, or EXEC_BACKEND parameter restore, and are
  exposed to sessions as server/runtime metadata.

`ConfigureNames[]` is now classified as an immutable generated template. The
generator emits `NULL` backing-variable pointers into that template, and emits
`InitializeGUCVariablePointers()` beside it. Each backend session copies the
template into `guc_variables` during GUC initialization, then calls
`InitializeGUCVariablePointers()` to bind the copied records to the current
thread's backing variables before `guc.c` mutates stack, reset, report, and
source state. This removes the static-initializer blocker for TLS GUC backing
variables.

`CurrentTransactionState` cannot use a static initializer that points at
`TopTransactionStateData`, because both are thread-local objects. It is
initialized by `InitializeTransactionState()`, called from `main()` immediately
after memory-context initialization so bootstrap, check-only, single-user,
postmaster, and regular backend paths can safely inspect transaction nesting
before `BaseInit()`. `BaseInit()` also calls the same function idempotently for
normal backend startup.

`PostmasterContext` remains runtime-global. The GUC static-initializer
constraint is now removed for generated built-in GUC records, but many GUC
backing variables outside the exported first slice still need to be converted
or explicitly classified according to their real owner.

The global-lifetime scanner now skips generated Bison parser outputs and Flex
scanner outputs for the main SQL parser, replication command parser, synchronous
replication parser, and JSONPath parser. Those generated files contain
K&R-style helper definitions, function parameters, and immutable transition
tables that the heuristic scanner misidentified as top-level mutable globals.
This mirrors the existing `bootparse.c` generated-file exception and removes
noise without changing backend runtime state.

The scanner also skips the `checksum_block_internal.h` include-fragment, which
is deliberately included inside checksum function bodies, and recognizes
immutable const pointer objects without being confused by `*` tokens inside
their initializers. This removes static-analysis noise for local checksum
scratch variables, immutable compression/fork/statistics tables, and typedef
attribute tails without changing backend runtime state.

Any dynamically loaded module that references an exported global after it gains
`PG_THREAD_LOCAL` must be rebuilt against the updated headers. Stale modules can
still link but may crash because they use the old non-TLS symbol access pattern.
During validation this affected `test_ext_backend_model.dylib` and
`plpgsql.dylib`; cleaning and rebuilding those modules fixed the crashes.

## Completion Status

The required-floor audit for non-Windows backend state is complete. As of the
Windows platform shim pass, the static scanner baseline contains one remaining
non-Windows entry:

- `src/backend/utils/adt/tsrank.c`: `WordEntryPos pos;` inside the anonymous
  `DocRepresentation` typedef. This is not a top-level mutable global; it is a
  scanner artifact from a struct member declaration. A generic scanner fix was
  attempted and backed out because it did not remove this artifact cleanly
  without broadening the heuristic risk. The artifact is documented here rather
  than treated as Phase 8 mutable backend state.

The Windows platform shim annotations are best-effort ownership classifications
from code review on this macOS checkout. They are not a validated Windows
threaded-runtime design. In particular, the Windows signal queue, signal mask,
signal event, signal critical section, and timer communication/thread handles
are classified as carrier-local physical dispatch state; the signal handler
tables, initial signal pipe handoff, and NTDLL function pointers are
runtime-global; and the socket nonblocking compatibility flag is classified as
connection-local. A future Windows pass must build and test these annotations
on Windows and revisit the signal, timer, and socket shims before claiming
threaded Windows support.

After the final USERSET/SUSET GUC classification slice, the filtered static
report contains zero remaining unclassified generated GUC backing variables.
The plan-cache saved plan and cached expression list heads are now explicit
session-local TLS state initialized by `InitPlanCache()`, so they no longer
depend on self-referential `DLIST_STATIC_INIT` globals.
The authenticated/session/effective role identity variables in `miscinit.c` are
now session-local TLS state, preserving process-mode behavior while preventing
threaded backends from sharing one effective user/security context.
The ACL role-membership cache in `acl.c` is now session-local TLS. Its cached
role OIDs, membership lists, and current-database hash filter are populated
from syscache lookups and copied into `TopMemoryContext` for the current
backend/session, so threaded sessions must not share one mutable authorization
cache.
The GSSAPI transport buffers in `be-secure-gssapi.c` are now connection-local
TLS state, matching the existing libpq send/receive buffer bridge in
`pqcomm.c`.
The local latch backing object in `miscinit.c` and the cached `WaitLatch()`
wait set in `latch.c` are now backend-local TLS state, so the thread-local
`MyLatch` pointer no longer targets shared static storage before a backend
switches to its shared `PGPROC` latch.
The process-signal header in shared memory is now explicitly classified as
shared-memory state, while each backend's cached `MyProcSignalSlot` pointer is
backend-local TLS state.
The procarray shared-memory pointers and KnownAssignedXids arrays are now
explicit shared-memory state. Backend-local transaction visibility caches,
including the `GlobalVis*` states and `cachedXidIsNotInProgress`, use TLS,
while recovery-stream bookkeeping such as `latestObservedXid` remains
runtime-owned state.
The single-entry transaction-status cache in `transam.c` is now backend-local
TLS, matching the backend-private visibility cache model.
Prepared-transaction state in `twophase.c` now has explicit lifetimes:
`TwoPhaseState` is shared memory protected by `TwoPhaseStateLock`, while the
currently locked prepared transaction pointer and `before_shmem_exit`
registration flag are backend-local TLS. The state-file assembly chain used
while preparing a transaction is execution-local TLS.
Async notification state in `async.c` now has explicit lifetimes: the
notification queue control block is shared memory, the notification SLRU
descriptor and global channel DSA/dshash handles are runtime state, the local
LISTEN table and registered-listener flag are session-local TLS, and pending
LISTEN/NOTIFY action lists plus commit-time signaling workspace are
execution-local TLS.
Shared-invalidation state now has explicit lifetimes. The `shmInvalBuffer`
pointer in `sinvaladt.c` is shared-memory state. The processed-message counter
exported as `SharedInvalidMessageCounter`, the recursive receive buffer and
counters in `ReceiveSharedInvalidMessages()`, and `nextLocalTransactionId` are
backend-local TLS.
Dynahash active sequential-scan tracking in `dynahash.c` is now backend-local
TLS. It records the hash scans currently open in one backend and cannot be
shared by concurrently executing threaded backends.
Hot-standby recovery-conflict state in `standby.c` is now backend-local TLS.
This includes the recovery lock hash tables owned by the startup backend, the
per-wait exponential backoff counter, and the timeout-handler pending flags set
by standby timeout callbacks.
The resource-release callback registry in `resowner.c` is now backend-local
TLS. That preserves the current process-per-backend semantics for callbacks
registered by dynamically loaded code, while the broader extension threading
policy remains governed by the Phase 7 backend-model gate. Optional
`RESOWNER_STATS` counters use the same backend-local lifetime.
The memory-context logging recursion guard in `mcxt.c` is now backend-local
TLS, matching `LogMemoryContextPending` delivery to a specific backend.
The deadlock detector workspace in `deadlock.c` is now backend-local TLS,
matching the existing `InitDeadLockChecking()` per-backend allocation model.
This includes the waits-for traversal arrays, proposed wait-order workspace,
deadlock report details, and the cached blocking-autovacuum pointer.
The usage-stat snapshots in `postgres.c` are now backend-local TLS. They hold
the current backend's `ResetUsage()` baseline for later `ShowUsage()` calls.
The lockfile cleanup list in `miscinit.c` and Unix socket cleanup list in
`pqcomm.c` are now explicit runtime-global state. They are owned by postmaster
or standalone process lifetime and must not be replicated into regular client
backend threads.
The legacy `CurrentSession` pointer in `session.c` is now session-local TLS.
The `Session` object remains the existing per-session DSM/DSA owner; this slice
only prevents the current-session compatibility pointer from being shared by
multiple threaded backends.
Combo CID maps in `combocid.c` and pending relation storage cleanup queues in
`storage.c` are now execution-local TLS. They are allocated in transaction
contexts or TopMemoryContext for current-transaction cleanup and must not be
shared by concurrently executing threaded backends.
Parallel-query state in `parallel.c` is now backend-local TLS. The active
parallel-context list now uses lazy per-backend initialization instead of the
old self-referential static initializer, so parallel contexts are not shared
across threaded client backends.
Connection-startup warning state in `postinit.c` is now connection-local TLS.
It accumulates warnings for the current connection before emission and must not
be shared by simultaneous threaded connection startups.
WAL record construction state in `xloginsert.c` is now execution-local TLS.
The `mainrdata_last` pointer now gets its first per-backend value during
`InitXLogInsert()` instead of using a process-global self-referential static
initializer.
Sequence cache state in `sequence.c` is now session-local TLS. The
`seqhashtab` entries record sequences touched in the current session for
`nextval()`/`currval()` semantics, and `last_used_seq` backs `lastval()` for
that same session. They must not be shared across concurrently executing
threaded sessions.
Temporary-table `ON COMMIT` bookkeeping in `tablecmds.c` is now session-local
TLS. The list is explicitly described as backend-local because `ON COMMIT`
actions only apply to temp tables, and entries can survive transaction cleanup
for the lifetime of the current session.
Prepared statement storage in `prepare.c` is now session-local TLS. Named SQL
and protocol prepared statements are visible across commands in one session,
but their cached plans and hash table must not be shared by concurrent
threaded sessions.
Rule/view deparse SPI plan caches in `ruleutils.c` are now session-local TLS.
They hold saved SPI plans prepared by the current backend for
`pg_get_ruledef()` and `pg_get_viewdef()` lookups, while the query text
literals are immutable singleton data.
Materialized-view maintenance depth in `matview.c` is now execution-local TLS.
It is a short-lived counter used to permit internal DML while one backend is
refreshing a materialized view, and must not leak across concurrent threaded
executions.
Trigger nesting depth in `trigger.c` is now execution-local TLS. The
`MyTriggerDepth` counter is incremented only around trigger function calls,
restored in a `PG_FINALLY()` block, and backs `pg_trigger_depth()` for the
current execution.
Referential-integrity trigger caches in `ri_triggers.c` now have explicit
lifetimes. The constraint, query-plan, and comparison caches are
session-local TLS, matching their backend-private `TopMemoryContext`
allocation and syscache callback registration. The fast-path batch cache and
after-trigger batch callback flag are execution-local TLS because they are
allocated in `TopTransactionContext` and torn down at trigger-batch end or
transaction abort, while the xact/subxact callback registration guard is
session-local TLS alongside the session-local transaction callback registry.
Missing-attribute value cache state in `heaptuple.c` is now backend-local
TLS. The cache stores backend-private copies of pass-by-reference missing
column defaults in `TopMemoryContext`; sharing the mutable dynahash between
threaded backends would be unsafe and is not required for correctness.
Synchronized sequential scan state in `syncscan.c` is now explicitly
classified as shared-memory state. The `scan_locations` pointer targets the
shared LRU location table registered with `ShmemRequestStruct()` and protected
by `SyncScanLock`.
B-tree vacuum cycle state in `nbtutils.c` is now explicitly classified as
shared-memory state. The `btvacinfo` pointer targets the shared active-vacuum
table registered with `ShmemRequestStruct()` and protected by
`BtreeVacuumLock`.
SLRU saved I/O error details in `slru.c` are now backend-local TLS. Physical
SLRU read/write helpers save an error cause and `errno` for a later
`SlruReportIOError()` call in the same backend; sharing those mutable fields
between concurrently executing threaded backends would corrupt the reported
failure.
Multixact state in `multixact.c` now has explicit lifetimes. The multixact
SLRU descriptors are runtime-global configuration/handles, while
`MultiXactState`, `OldestMemberMXactId`, and `OldestVisibleMXactId` point at
shared memory registered during startup. The transaction-lifetime multixact
cache and its memory context are backend-local TLS with lazy list
initialization, so concurrent threaded backends do not share one row-lock
membership cache.
Core transaction SLRU state now has explicit lifetimes. The transaction,
subtransaction, and commit-timestamp SLRU descriptors are runtime-global
configuration/handles registered during startup. The commit-timestamp
last-value cache and activation flag point at shared memory registered with
`ShmemRequestStruct()` and protected by `CommitTsLock`.
Transaction ID and OID assignment state in `varsup.c` is now explicitly
classified as shared-memory state. The exported `TransamVariables` pointer
targets the `ShmemRequestStruct()` allocation that stores cluster-wide XID/OID
counters and wraparound limits guarded by their existing LWLocks.
Binary-upgrade assignment state in `binary_upgrade.h`, `aclchk.c`, `heap.c`,
`index.c`, `pg_enum.c`, `pg_type.c`, `tablespace.c`, `typecmds.c`, and
`user.c` is now session-local TLS state. These pg_upgrade support variables
are set by SQL-callable support functions and consumed by the next relevant
DDL operation in the same backend session; threaded sessions must not share
pending OID, relfilenumber, or initial-privilege assignment controls.
Catalog transaction-local state in `index.c` and `pg_enum.c` now uses
execution-local TLS. The system-index reindex state records the current
backend's active or pending reindex operation and is explicitly serialized
into parallel workers, while the enum uncommitted-type/value hash tables track
transaction-local enum safety for the current backend only.
Immutable catalog lookup tables in `heap.c` and `objectaddress.c` are now
explicitly classified: `SysAtt[]` is the fixed system-attribute descriptor
table, and `ObjectTypeMap[]` maps stable object-type strings to `ObjectType`
values.

Gate C has passed on this macOS checkout. The current validation includes a
literal top-level `gmake check-world` pass for the runnable checks in this
configuration, static global report checks, direct full core regression,
direct full isolation regression, extension load tests using the test-only
threaded backend model, and PL/pgSQL process-mode regression tests. TAP checks
are skipped because this checkout is not configured with TAP support.

## Validation So Far

Validation for this slice:

- explicit generated-header recovery for `src/backend/utils` and
  `src/backend/nodes`, followed by `gmake -j8`;
- incremental `gmake -j8` after moving transaction-state initialization into
  `main()`;
- clean `gmake -j8` after making `ConfigureNames[]` an immutable template and
  rebinding GUC backing-variable pointers at runtime;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header planner/JIT declarations to
  `PG_THREAD_LOCAL`;
- focused core GUC regression test: `guc`;
- fixture-backed planner/JIT regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc join
  aggregates incremental_sort plancache limit plpgsql copy2 temp domain
  rangefuncs prepare conversion truncate alter_table sequence polymorphism
  rowtypes returning largeobject with xml partition_merge partition_split
  partition_join partition_prune reloptions hash_part indexing
  partition_aggregate partition_info tuplesort explain memoize predicate numa
  eager_aggregate planner_est`;
- fixture-backed transaction regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc
  transactions`;
- fixture-backed exported session GUC regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc create_role
  roleattributes`;
- live temp-cluster smoke coverage for `application_name`, `role`,
  `tcp_keepalives_idle`, `tcp_keepalives_interval`, `tcp_keepalives_count`, and
  `tcp_user_timeout`;
- fixture-backed SQL-behavior GUC regression coverage:
  `test_setup boolean char name varchar text float4 float8 strings arrays copy
  copyselect copydml copyencoding insert insert_conflict create_function_c
  create_misc create_operator create_procedure create_table create_type
  create_schema create_index create_index_spgist create_view index_including
  index_including_gist create_aggregate create_function_sql create_cast
  constraints triggers select vacuum sanity_check xml`;
- live temp-cluster smoke coverage for `array_nulls`, `backslash_quote`,
  `bytea_output`, `extra_float_digits`, `quote_all_identifiers`,
  `transform_null_equals`, `xmlbinary`, and `xmloption`;
- fixture-backed vacuum GUC regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc`;
- live temp-cluster smoke coverage for `vacuum_truncate`,
  `vacuum_freeze_min_age`, `vacuum_freeze_table_age`, `vacuum_failsafe_age`,
  `vacuum_multixact_freeze_min_age`,
  `vacuum_multixact_freeze_table_age`,
  `vacuum_multixact_failsafe_age`,
  `vacuum_max_eager_freeze_failure_rate`, and
  `track_cost_delay_timing`;
- fixture-backed locale/authorization/encoding GUC regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc strings
  date time timetz timestamp timestamptz interval horology sysviews
  select_parallel`;
- focused `datetime.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and fixture-backed date/time plus `guc`
  regression coverage after classifying date/time token tables and caches. The
  direct `pg_regress` invocation ran the core fixture prefix plus `guc`, `date`,
  `time`, `timetz`, `timestamp`, `timestamptz`, `interval`, and `horology`, and
  passed all 35 tests.
- focused `formatting.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and fixture-backed numeric, money,
  date/time, and horology regression coverage after classifying immutable
  formatting lookup tables and backend-local format-picture caches. The direct
  `pg_regress` invocation ran the core fixture prefix plus `guc`, `numeric`,
  `money`, `date`, `time`, `timetz`, `timestamp`, `timestamptz`, `interval`,
  and `horology`, and passed all 37 tests.
- focused `float.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and direct temp-instance `test_setup` plus
  `float8` regression coverage after classifying degree-based trigonometry
  constants. Initial direct runs of `float8` alone and `float4 float8` failed
  because `float8` expects the permanent `FLOAT8_TBL` fixture from
  `test_setup` after dropping its temporary table; rerunning with
  `test_setup float8` passed.
- focused `numeric.o`, `regexp.o`, `lockfuncs.o`, `wparser_def.o`, and
  `spell.o` compile coverage, global-lifetime scanner coverage, incremental
  full rebuild/install, and fixture-backed `numeric`, `strings`, `tsearch`,
  `tsdicts`, and `advisory_lock` regression coverage after classifying numeric
  constants, regexp cache state, lock-name metadata, and text-search lookup
  strings. The direct `pg_regress` invocation ran the core fixture prefix plus
  those five tests and passed all 33 tests.
- focused `pseudorandomfuncs.o` compile coverage, global-lifetime scanner
  coverage, incremental full rebuild/install, and direct temp-instance
  `random` regression coverage after classifying SQL pseudorandom generator
  state as session-local TLS. The direct `pg_regress` invocation passed.
- focused `timestamp.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and direct temp-cluster smoke coverage for
  `pg_postmaster_start_time()` and `pg_conf_load_time()` after classifying
  server start/reload timestamps as runtime-global state. The smoke verified
  non-null start and reload timestamps, reloaded configuration, then verified
  the reload timestamp remained valid after reload.
- focused `sampling.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, fixture-prefix regression coverage through
  `vacuum`, and a direct temp-cluster ANALYZE smoke after classifying the
  deprecated sampling API state as backend-local TLS. The smoke created and
  populated a table, ran `ANALYZE`, and verified `pg_stats` rows were visible.
- focused `array_typanalyze.o` compile coverage, global-lifetime scanner
  coverage, incremental full rebuild/install, and direct temp-cluster array
  ANALYZE smoke after classifying the array typanalyze callback bridge as
  execution-local TLS. The smoke populated an `int[]` column, ran `ANALYZE`,
  and verified `pg_stats.most_common_elems` was produced for the array column.
- focused `superuser.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, direct `roleattributes` regression
  coverage, and a direct temp-cluster same-session role-cache invalidation
  smoke after classifying the superuser role lookup cache as backend-local
  TLS. The smoke created a role, observed `has_table_privilege()` change from
  false to true after `ALTER ROLE ... SUPERUSER`, then back to false after
  `ALTER ROLE ... NOSUPERUSER`.
- focused `acl.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and fixture-backed `roleattributes` plus
  `privileges` regression coverage after classifying the ACL role-membership
  cache as session-local TLS. The direct `pg_regress` invocation used the
  setup/create/constraint/trigger prefix needed by `privileges` and passed all
  32 tests.
- focused `guc.o` and `guc_tables.o` compile coverage, global-lifetime
  scanner coverage, incremental full rebuild/install, direct `guc` regression
  coverage, and a direct temp-cluster custom-GUC reserved-prefix smoke after
  classifying immutable GUC lookup metadata and runtime custom-prefix
  registration state. The smoke preloaded `test_oat_hooks`, verified its
  custom GUC appeared in `pg_settings`, and verified an unregistered variable
  under the reserved prefix was rejected.
- focused `guc-file.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and a direct temp-cluster configuration
  reload smoke after classifying config-file scanner state as execution-local
  TLS. The smoke loaded an included config file, reloaded a changed `work_mem`,
  then reloaded a syntax error and verified the prior setting remained active
  while the error was logged.
- focused `elog.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and a direct temp-cluster log-prefix smoke
  after classifying logging timestamp/formatting state. The smoke enabled
  `%m`, `%s`, `%l`, and `%p` in `log_line_prefix`, emitted two SQL errors, and
  verified both log entries included formatted timestamps, backend start time,
  line counters, and backend PID.
- focused `injection_point.o` compile coverage, global-lifetime scanner
  coverage, and incremental full rebuild/install after classifying the
  injection-point shared table and backend-local callback cache. This checkout
  is not configured with `--enable-injection-points`, so injection-point
  regression/TAP coverage requires a separate injection-enabled build.
- unsafe test module coverage for session authorization and GUC privileges:
  `rolenames setconfig alter_system_table guc_privs`;
- live temp-cluster smoke coverage for `client_encoding`, `DateStyle`,
  `TimeZone`, `log_timezone`, `timezone_abbreviations`,
  `restrict_nonsystem_relation_kind`, `seed`, `default_with_oids`,
  `standard_conforming_strings`, `ssl_renegotiation_limit`, and
  `session_authorization`;
- fixture-backed locale cache regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc numeric money
  date time timetz timestamp timestamptz interval horology collate`;
- live temp-cluster smoke coverage for `lc_messages`, `lc_monetary`,
  `lc_numeric`, `lc_time`, `icu_validation_level`, localized date formatting,
  numeric formatting, and money formatting. This build is configured
  `--without-icu`, so the ICU-specific collation regression file was not
  applicable;
- focused `pg_locale.o` and `pg_locale_icu.o` compile coverage,
  global-lifetime scanner coverage, incremental full rebuild/install, and a
  direct temp-cluster C/POSIX collation smoke after classifying the fixed
  `c_locale` descriptor as immutable singleton state and the ICU converter as
  session-local TLS. The smoke verified database collation metadata, `C` and
  `POSIX` collation catalog entries, and `COLLATE "C"` ordering. This local
  build is configured `--without-icu`; it still initializes the built-in
  `unicode|i|und` collation catalog entry, so the ICU converter classification
  has compile/static coverage here but not runtime conversion-path coverage;
- focused `dbsize.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and direct `dbsize` regression coverage
  after classifying the `pg_size_pretty()`/`pg_size_bytes()` unit table as
  immutable singleton state;
- focused `xml.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and fixture-backed `test_setup xml`
  regression coverage after classifying the optional `USE_LIBXMLCONTEXT`
  allocation context pointer as backend-local TLS. This local build is not a
  libxml-enabled debug build, so the allocator-hook classification has
  compile/static coverage here rather than direct `USE_LIBXMLCONTEXT` runtime
  coverage;
- focused `wait_event_funcs.o` compile coverage, global-lifetime scanner
  coverage, incremental full rebuild/install, and a live temp-cluster
  `pg_wait_events` smoke after classifying the generated wait-event metadata
  table as immutable singleton state. The smoke verified non-empty results,
  core wait-event type groups, and descriptions for representative named
  events;
- global-lifetime scanner coverage, backend clean plus generated-header
  recovery, full rebuild/install, and a live temp-cluster worker-stats smoke
  after classifying exported pending bgwriter/checkpointer stats as
  backend-local TLS. The smoke created write activity, forced two fast
  checkpoints, verified `pg_stat_checkpointer.num_requested` advanced, and
  verified `pg_stat_reset_shared('bgwriter')` and
  `pg_stat_reset_shared('checkpointer')` moved their reset timestamps;
- focused `dsm.o` and `dsm_registry.o` compile coverage, global-lifetime
  scanner coverage, incremental full rebuild/install, and
  `test_dsm_registry` regression coverage after classifying DSM control,
  preallocated DSM, and DSM registry attachment state. The regression creates
  named DSM, DSA, and dshash registry entries and verifies
  `pg_dsm_registry_allocations` reports the expected segment/area/hash
  entries;
- fixture-backed role/compression GUC regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc compression
  create_role strings portals`;
- live temp-cluster smoke coverage for `default_toast_compression`,
  `password_encryption`, and `createrole_self_grant`, including a non-superuser
  CREATEROLE self-grant check. The same smoke confirmed `trace_syncscan` is not
  registered in this default build because `TRACE_SYNCSCAN` is not enabled;
- fixture-backed command/session GUC regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc create_am
  oidjoins event_trigger tablespace`;
- test extension regression coverage for extension command state and backend
  model checks: `test_extensions`, `test_extdepend`,
  `test_ext_backend_model`, and `test_ext_backend_model_pooled`;
- live temp-cluster smoke coverage for `default_tablespace`,
  `temp_tablespaces`, `allow_in_place_tablespaces`,
  `session_replication_role`, `event_triggers`, and
  `extension_control_path`, including a custom extension loaded through a
  session-set control path and `$system` discovery for PL/pgSQL after clearing
  the path. Phase 11 later proved that threaded backend carriers must also
  initialize `extension_control_path` in `InitializeThreadedSessionGUCOptions()`
  before `CREATE EXTENSION` can safely search control directories. The local
  TAP harness could not run
  `t/001_extension_control_path.pl` because this macOS Perl does not have the
  required `IPC::Run` module installed;
- fixture-backed GIN regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc gin`;
- live temp-cluster smoke coverage for `gin_fuzzy_search_limit` and
  `gin_pending_list_limit`, including a GIN index reloption override for
  `gin_pending_list_limit`;
- fixture-backed async notify regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc async`;
- live temp-cluster smoke coverage for `trace_notify`, including `SET`,
  `SHOW`, `LISTEN`, and `NOTIFY`;
- focused `funccache.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, fixture-backed SQL-function/plancache
  regression coverage, and a direct temp-cluster SQL-function replacement
  smoke after classifying the cached-function hash table as session-local TLS.
- focused `attoptcache.o` and `spccache.o` compile coverage, global-lifetime
  scanner coverage, incremental full rebuild/install, standalone `reloptions`
  regression coverage, and a direct temp-cluster tablespace option
  create/alter/drop smoke after classifying reloption caches as
  session-local TLS.
- focused `evtcache.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and fixture-backed `event_trigger`
  regression coverage through the `create_am` dependency prefix after
  classifying event-trigger cache state as session-local TLS.
- focused `relfilenumbermap.o` compile coverage, global-lifetime scanner
  coverage, incremental full rebuild/install, and a live temp-cluster
  relfilenumber mapping smoke after classifying the relfilenumber map cache as
  session-local TLS. The smoke populated `pg_filenode_relation()`'s cache,
  rewrote the table with `VACUUM FULL`, verified the old filenumber no longer
  resolved, and verified the new filenumber mapped back to the relation.
- focused `typcache.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, early datatype plus `type_sanity`
  regression coverage, and fixture-backed type/row/domain/range regression
  coverage after classifying typcache and local record-typmod cache state as
  session-local TLS. An initial custom `type_sanity` schedule failed because
  it ran after artifact-producing DDL tests, and a second attempt confirmed
  `type_sanity` requires the standard early datatype fixtures; the final
  fixture-backed runs passed.
- focused `syscache.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, catalog sanity regression coverage, and
  DDL/invalidation-heavy regression coverage after classifying syscache wrapper
  state as session-local TLS. The catalog sanity group covered early datatype
  fixtures, `type_sanity`, `opr_sanity`, `misc_sanity`, and `oidjoins`; the
  DDL group covered create/alter/drop, plan-cache, domain, rowtype, range,
  dependency, and GUC paths.
- focused `inval.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, DDL/syscache invalidation regression
  coverage, and a live temp-cluster prepared-plan invalidation smoke after
  classifying invalidation dispatcher state. The smoke kept a prepared query
  across table rewrite-relevant DDL, index create/drop, `ANALYZE`, and data
  updates and verified correct results. An initial variant intentionally
  changed the prepared query's result type and hit PostgreSQL's expected
  `cached plan must not change result type` error, confirming invalidation
  occurred but not suitable as a passing smoke.
- focused `relmapper.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, fixture-backed cluster/alter/table-space
  regression coverage, and a live temp-cluster mapped-catalog smoke after
  classifying relation mapper state. The smoke resolved `pg_class` through the
  relation map, rewrote it with `VACUUM FULL`, verified the old mapped
  filenumber no longer resolved, and verified the new mapped filenumber
  resolved back to `pg_class`.
- focused `catcache.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, catalog sanity regression coverage,
  DDL/syscache invalidation regression coverage, and a live temp-cluster
  catcache rename/drop smoke after classifying catalog cache state. The smoke
  resolved a table name through `to_regclass()`, renamed it, verified the old
  name no longer resolved, verified the new name resolved, dropped it, and
  verified the dropped name no longer resolved.
- focused `relcache.o`, `catcache.o`, `seclabel.o`, and `postinit.o` compile
  coverage, global-lifetime scanner coverage, backend clean plus
  generated-header recovery, full rebuild/install, fixture-backed relation DDL
  regression coverage, and a live temp-cluster restart/rename/drop smoke after
  classifying relation cache state. An initial incremental rebuild/install
  reproduced the documented stale-object failure mode during `initdb`
  post-bootstrap startup; the backend clean/rebuild then passed. The regression
  group covered create/index/alter/temp/cluster/tablespace/event-trigger paths,
  and the smoke created a table and index, restarted the server, verified both
  still resolved, then verified rename/drop invalidation.
- focused `ts_cache.o` compile coverage, global-lifetime scanner coverage, and
  incremental full rebuild/install after classifying text-search parser,
  dictionary, and configuration caches as session-local TLS state;
- fixture-backed default-text-search regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc tsearch`;
- live temp-cluster smoke coverage for `default_text_search_config`, including
  repeated `SET`, `get_current_ts_config()`, and `to_tsvector()` calls that
  verified both English stemming and simple dictionary output;
- focused `dfmgr.o` and `fmgr.o` compile coverage, global-lifetime scanner
  coverage, incremental full rebuild/install, and focused backend-model
  extension regression coverage after classifying the dynamic-library list,
  rendezvous hash, and C function cache. Regression coverage included
  `test_ext_backend_model` and `test_ext_backend_model_pooled`;
- fixture-backed dynamic loader regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc`;
- live temp-cluster smoke coverage for `dynamic_library_path`, including an
  empty-path `LOAD 'plpgsql'` failure and a `$libdir` success;
- focused `plancache.o` compile coverage;
- fixture-backed plan-cache mode regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc plancache
  explain partition_prune subselect`;
- live temp-cluster smoke coverage for `plan_cache_mode`, including
  `PREPARE`, `EXECUTE`, `SET force_generic_plan`, `SET force_custom_plan`,
  `RESET`, and `DEALLOCATE`;
- plan-cache saved plan and cached expression list heads are explicitly
  initialized `PG_THREAD_LOCAL PG_GLOBAL_SESSION` state;
- focused `tableam.o` compile coverage;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header table access declarations to
  `PG_THREAD_LOCAL`;
- fixture-backed table access GUC regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc create_am`;
- live temp-cluster smoke coverage for `default_table_access_method` and
  `synchronize_seqscans`, including table creation through the default table
  access method and `SET`/`SHOW` coverage for synchronized scans;
- focused `namespace.o` compile coverage;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header namespace/search-path declarations to
  `PG_THREAD_LOCAL`;
- fixture-backed namespace/search-path regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc namespace
  temp plancache create_role privileges`;
- live temp-cluster smoke coverage for `search_path`, schema-qualified and
  unqualified lookup, temp namespace creation, and a second connection that
  did not inherit the first session's search path or temp namespace state;
- focused `inv_api.o` compile coverage;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header large-object declarations to
  `PG_THREAD_LOCAL`;
- fixture-backed large-object/GUC privilege regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc privileges
  largeobject`;
- live temp-cluster smoke coverage for `lo_compat_privileges` and large-object
  create/write/read/unlink behavior, including a second connection that did
  not inherit the first session's `lo_compat_privileges` setting;
- focused `be-fsstubs.o` compile coverage, full rebuild/install,
  global-lifetime scanner coverage, and fixture-backed `largeobject`
  regression coverage after classifying large-object descriptor state. The
  direct `pg_regress` invocation included the schedule prefix through
  `returning` and passed all 44 tests including `largeobject`;
- focused `tuplesort.o` and `tuplesortvariants.o` compile coverage;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header sort GUC declarations to
  `PG_THREAD_LOCAL`;
- fixture-backed sort GUC regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc limit
  tuplesort incremental_sort aggregates`;
- live temp-cluster smoke coverage for `trace_sort`, including a sorted query
  and a second connection that did not inherit the first session's setting.
  The guarded `optimize_bounded_sort` GUC is not exposed in this default build;
- focused `xact.o` and `xlog.o` compile coverage;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header commit GUC declarations to
  `PG_THREAD_LOCAL`;
- fixture-backed commit GUC regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc
  transactions`;
- live temp-cluster smoke coverage for `synchronous_commit`, `commit_delay`,
  and `commit_siblings`, including a commit path and a second connection that
  did not inherit the first session's settings;
- focused `queryjumblefuncs.o`, `pgstat.o`, `pgstat_function.o`,
  `backend_status.o`, `backend_progress.o`, `launch_backend.o`, and
  `execExpr.o` compile coverage;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header query/statistics declarations to
  `PG_THREAD_LOCAL`;
- fixture-backed query/statistics regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc explain
  stats_ext stats`;
- live temp-cluster smoke coverage for `compute_query_id`,
  `stats_fetch_consistency`, `track_activities`, `track_counts`, and
  `track_functions`, including `EXPLAIN (verbose)` query identifier output and
  a second connection that did not inherit the first session's settings;
- focused `elog.o` and `guc_tables.o` compile coverage;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header logging/error-reporting declarations to
  `PG_THREAD_LOCAL`;
- fixture-backed GUC regression coverage after the logging/error-reporting
  slice:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc`;
- live temp-cluster smoke coverage for `log_error_verbosity`,
  `log_min_messages`, and `backtrace_functions`, including a second
  connection that did not inherit the first session's settings;
- focused `guc_tables.o` compile coverage plus incremental `gmake -j8` after
  converting the `DEBUG_NODE_TESTS_ENABLED` developer node-test GUC
  declarations to `PG_THREAD_LOCAL`. These GUCs are not present in the default
  build, so validation for this slice is compile and static-scan coverage
  rather than runtime SQL coverage;
- focused `guc_tables.o` compile coverage plus incremental `gmake -j8` after
  classifying preset/runtime GUC backing variables as `PG_GLOBAL_RUNTIME`;
- focused `postmaster.o`, `syslogger.o`, `bgwriter.o`, `checkpointer.o`,
  `walwriter.o`, and `startup.o` compile coverage plus incremental
  `gmake -j8` after classifying postmaster/control-plane GUC backing variables
  as `PG_GLOBAL_RUNTIME`;
- focused `autovacuum.o` compile coverage plus incremental `gmake -j8` after
  classifying autovacuum launcher/worker GUC backing variables as
  `PG_GLOBAL_RUNTIME`;
- focused `bufmgr.o`, `bufpage.o`, `fd.o`, `copydir.o`, `dsm_impl.o`,
  `ipci.o`, `aio.o`, and `method_worker.o` compile coverage plus incremental
  `gmake -j8` after classifying storage and AIO GUC backing variables as
  either `PG_THREAD_LOCAL` session state or `PG_GLOBAL_RUNTIME` runtime state;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header storage and AIO declarations to
  `PG_THREAD_LOCAL` or `PG_GLOBAL_RUNTIME`;
- focused core GUC regression test after the storage/AIO slice: `guc`;
- focused `lock.o`, `lwlock.o`, and `predicate.o` compile coverage;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header lock-manager declarations to
  `PG_THREAD_LOCAL` or `PG_GLOBAL_RUNTIME`;
- focused core GUC regression test after the lock-manager slice: `guc`;
- focused `elog.o` and `guc_tables.o` compile coverage plus incremental
  `gmake -j8` after classifying logging-destination GUC backing variables as
  `PG_GLOBAL_RUNTIME`;
- focused `xlog.o` compile coverage;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting installed-header core WAL declarations to
  `PG_THREAD_LOCAL` or `PG_GLOBAL_RUNTIME`;
- focused core GUC regression test after the core WAL slice: `guc`;
- focused `xlogrecovery.o`, `xlogutils.o`, `xlogprefetcher.o`, `standby.o`,
  and `guc_tables.o` compile coverage plus incremental `gmake -j8` after
  classifying recovery and standby GUC backing variables and derived recovery
  target state as `PG_GLOBAL_RUNTIME`;
- focused core GUC regression test after the recovery and standby slice:
  `guc`;
- focused `be-secure.o`, `auth.o`, `crypt.o`, `auth-scram.o`,
  `auth-oauth.o`, `pqcomm.o`, and `backend_startup.o` compile coverage after
  classifying libpq, authentication, SSL, socket, and connection-startup GUC
  backing variables as `PG_GLOBAL_RUNTIME`;
- incremental `gmake -j8` and focused core GUC regression test after the
  libpq/authentication/SSL slice: `guc`;
- focused `pgarch.o`, `walsummarizer.o`, `launcher.o`, `slotsync.o`,
  `origin.o`, `slot.o`, `walsender.o`, `walreceiver.o`, `syncrep.o`,
  `async.o`, `twophase.o`, `commit_ts.o`, and `backend_status.o` compile
  coverage after classifying replication, WAL-capacity, notification queue,
  commit timestamp, prepared-transaction, and backend-status GUC backing
  variables as `PG_GLOBAL_RUNTIME`;
- incremental `gmake -j8` and focused core GUC regression test after the
  replication/WAL-capacity slice: `guc`;
- focused `inval.o`, `reorderbuffer.o`, `walsender.o`, `walreceiver.o`,
  `stack_depth.o`, `ps_status.o`, `storage.o`, `instr_time.o`, `string_utils.o`,
  `fd.o`, and `proc.o` compile coverage after classifying the final
  USERSET/SUSET GUC backing variables, frontend `quote_all_identifiers`
  singleton, temporary-file tablespace selection state, and shared PGPROC
  ownership annotations;
- backend clean plus generated-header recovery, followed by clean `gmake -j8`
  after converting final installed-header declarations to `PG_THREAD_LOCAL`
  or explicit runtime/dynamic classifications;
- focused `ps_status.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and focused core GUC regression coverage
  after classifying process-title storage as runtime-global state;
- focused core GUC regression test after the final USERSET/SUSET slice:
  `guc`;
- PL/pgSQL clean rebuild and temp-install reinstall after the final
  installed-header `PG_THREAD_LOCAL` changes;
- targeted isolation regression coverage:
  `read-only-anomaly read-only-anomaly-2 read-only-anomaly-3
  serializable-parallel-2`;
- unsafe test module GUC privilege regression test: `guc_privs`;
- `perl src/tools/global_lifetime/scan_global_lifetimes.pl --baseline
  src/tools/global_lifetime/global_lifetime_baseline.tsv`;
- `perl src/tools/global_lifetime/scan_global_lifetimes.pl --write-baseline
  src/tools/global_lifetime/global_lifetime_baseline.tsv`;
- regenerated `src/tools/global_lifetime/global_lifetime_baseline.tsv` so
  previously classified Phase 8 globals are no longer carried as stale
  unclassified debt;
- filtered static scan for the touched required-floor names;
- filtered non-TLS extern mismatch search for the planner/JIT/analyze,
  exported session, session SQL-behavior, and vacuum tuning GUC backing
  variables, plus the session locale/authorization/encoding and
  locale-cache, role/compression/syncscan GUC slices;
- `git diff --check`;
- extension backend-model regression tests:
  `test_extensions`, `test_extdepend`, `test_ext_backend_model`, and
  `test_ext_backend_model_pooled`;
- PL/pgSQL process-mode regression tests.
- focused `miscinit.o` compile coverage plus fixture-backed role/privilege
  regression coverage after classifying authenticated/session/effective role
  identity state.
- full non-GSS build coverage, static lifetime scan coverage, and
  process-mode connection smoke/regression coverage after classifying the
  GSSAPI transport buffers. Direct `be-secure-gssapi.o` compile coverage was
  not available in this checkout because it is configured with
  `with_gssapi = no`.
- focused `miscinit.o` and `latch.o` compile coverage plus process-mode
  connection smoke/regression coverage after classifying backend-local latch
  backing state.
- focused `procsignal.o` compile coverage plus process-mode connection
  smoke/regression coverage after classifying process-signal shared/backend
  state.
- focused `pqmq.o` compile coverage, full rebuild/install,
  global-lifetime scanner coverage, and fixture-backed `select_parallel`
  regression coverage after classifying shared-memory message-queue protocol
  state. A direct run with only `create_misc` produced unrelated plan-shape
  diffs because expected indexes were absent; the final direct `pg_regress`
  invocation included the schedule prefix through `sysviews` and passed all
  38 tests including `select_parallel`.
- focused `procarray.o` compile coverage plus transaction and snapshot
  regression coverage after classifying procarray shared/runtime/backend state.
- focused `standby.o` compile coverage plus process-mode recovery-conflict
  static scan coverage after classifying hot-standby recovery-conflict state.
- focused `resowner.o` compile coverage plus resource-owner static scan
  coverage after classifying the resource-release callback registry.
- focused `mcxt.o` compile coverage, memory-context static scan coverage, and
  process-mode query/PLpgSQL regression coverage after classifying the
  memory-context logging recursion guard.
- focused `deadlock.o` compile coverage plus lock/deadlock static scan
  coverage after classifying the deadlock detector workspace.
- focused `postgres.o` compile coverage plus process-mode stats GUC
  regression coverage after classifying usage-stat snapshots.
- focused `miscinit.o` compile coverage plus process-mode startup/regression
  smoke coverage after classifying the lockfile cleanup list.
- focused `pqcomm.o` compile coverage plus process-mode startup/regression
  smoke coverage after classifying the Unix socket cleanup list.
- focused `session.o` compile coverage plus full rebuild/process-mode
  startup/regression smoke coverage after classifying the legacy
  current-session pointer.
- focused `combocid.o` and `storage.o` compile coverage plus transaction and
  relation-storage regression coverage after classifying transaction-owned
  combo CID and pending storage cleanup state.
- focused `parallel.o` compile coverage plus full rebuild/process-mode
  parallel-query regression coverage after classifying per-backend parallel
  query state.
- focused `postinit.o` compile coverage plus process-mode startup/regression
  smoke coverage after classifying connection-startup warning state.
- focused `xloginsert.o` compile coverage plus WAL-writing transaction and
  relation-storage regression coverage after classifying WAL record
  construction state.
- focused `transam.o` compile coverage plus transaction visibility regression
  coverage after classifying the single-entry transaction-status cache.
- focused `twophase.o` compile coverage plus prepared-transaction regression
  coverage after classifying prepared-transaction shared/backend/execution
  state.
- focused `async.o` compile coverage plus async notify regression coverage
  after classifying notification shared/runtime/session/execution state.
- focused `sinval.o` and `sinvaladt.o` compile coverage plus cache
  invalidation regression coverage after classifying shared-invalidation
  shared/backend state.
- focused `dynahash.o` compile coverage plus hash-scan regression coverage
  after classifying active hash sequential-scan tracking state.
- focused `sequence.o` compile coverage plus sequence regression coverage
  after classifying session-local sequence cache and `lastval()` state.
- focused `tablecmds.o` compile coverage plus temp-table/alter-table
  regression coverage after classifying session-local `ON COMMIT`
  bookkeeping.
- focused `prepare.o` compile coverage plus prepared-statement regression
  coverage after classifying session-local prepared statement storage.
- focused `ruleutils.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and fixture-backed `create_view`/`rules`
  regression coverage after classifying rule/view deparse SPI plan caches. The
  direct schedule-prefix run covered 99 tests through `rules`, including
  `pg_get_ruledef()` and `pg_get_viewdef()` call sites.
- focused `matview.o` compile coverage plus materialized-view regression
  coverage after classifying execution-local materialized-view maintenance
  depth.
- focused `trigger.o` compile coverage plus trigger regression coverage after
  classifying execution-local trigger nesting depth.
- focused `ri_triggers.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and fixture-backed `foreign_key` plus
  `triggers` regression coverage after classifying referential-integrity
  trigger cache state. A direct `foreign_key triggers` run first exposed the
  expected missing `test_setup` public-schema grant dependency, then
  `test_setup foreign_key triggers` passed all three tests.
- focused `heaptuple.o` compile coverage plus fast-default regression
  coverage after classifying backend-local missing-attribute cache state.
- focused `syncscan.o` compile coverage plus process-mode sequential-scan
  regression coverage after classifying synchronized-scan shared-memory state.
- focused `nbtutils.o` compile coverage plus process-mode btree/vacuum
  regression coverage after classifying btree vacuum shared-memory state.
- focused `slru.o` compile coverage plus transaction and async-notify
  regression coverage after classifying backend-local SLRU saved-error state.
- focused `multixact.o` compile coverage plus process-mode multixact
  isolation and prepared-transaction regression coverage after classifying
  multixact shared/runtime/backend-local state.
- focused `clog.o`, `subtrans.o`, and `commit_ts.o` compile coverage plus
  transaction/subtransaction and commit-timestamp regression coverage after
  classifying core transaction SLRU runtime/shared state.
- focused `varsup.o` compile coverage plus transaction, subtransaction, OID,
  and commit-timestamp regression coverage after classifying
  `TransamVariables` as shared-memory state.
- focused `xlogrecovery.o` and `xlogutils.o` compile coverage, full
  rebuild/install, global-lifetime scanner coverage, and a process-mode
  immediate-stop/restart crash-recovery smoke after classifying WAL recovery
  mode state.
- focused `xlogrecovery.o` and `xlogutils.o` compile coverage, full
  rebuild/install, global-lifetime scanner coverage with no remaining
  `xlogrecovery.c` or `xlogutils.c` baseline entries, and a process-mode
  immediate-stop/restart crash-recovery smoke after classifying WAL recovery
  replay state.
- focused `xlogwait.o` compile coverage, full rebuild/install,
  global-lifetime scanner coverage, and a process-mode `WAIT FOR LSN` smoke
  using `MODE 'primary_flush'` after classifying Wait-for-LSN shared state.
- focused `basebackup.o` and `basebackup_target.o` compile coverage, full
  rebuild/install, global-lifetime scanner coverage, and a process-mode
  `pg_basebackup -X none` smoke after classifying base backup execution and
  target-registry state.
- focused `bootstrap.o` and `bootparse.o` compile coverage, full
  rebuild/install, global-lifetime scanner coverage with generated
  `bootparse.c` skipped, and an `initdb --no-sync` smoke after classifying
  bootstrap-mode runtime state.
- focused `aclchk.o`, `heap.o`, `index.o`, `pg_enum.o`, `pg_type.o`,
  `tablespace.o`, `typecmds.o`, and `user.o` compile coverage, backend clean
  plus generated-header recovery, full rebuild/install, and global-lifetime
  scanner coverage after classifying binary-upgrade assignment state. Runtime
  smoke coverage included `initdb --no-sync`, a `-b` binary-upgrade server
  smoke that set heap OID, heap relfilenumber, row type OID, array type OID,
  and role OID controls before consuming them with DDL, and a normal-mode
  catalog DDL smoke for table, enum type, and role creation with the TLS
  defaults unset. The scanner was also tightened so function prototypes with
  callback parameters are not retained as false unclassified globals.
- focused `index.o` and `pg_enum.o` compile coverage, incremental full
  rebuild/install, global-lifetime scanner coverage, and a process-mode
  catalog DDL smoke after classifying catalog transaction-local state. The SQL
  smoke created and altered an enum inside one transaction, inserted the new
  value before commit, created a table with a primary key and secondary index,
  ran `REINDEX TABLE`, and verified the table contents after reindexing.
- focused `heap.o` and `objectaddress.o` compile coverage plus
  global-lifetime scanner coverage after classifying immutable catalog lookup
  tables.
- focused compile coverage for hook-registry definition files, full
  configured rebuild/install, global-lifetime scanner coverage, and
  process-mode startup/query/EXPLAIN smoke after classifying exported
  extension hook registries. Direct `be-secure-openssl.o` subdir compile was
  not runnable in this checkout because the direct target lacks the OpenSSL
  include path; the configured top-level build covered the file.
- focused `explain_state.o` compile coverage, full rebuild/install,
  `pg_overexplain`, `pg_plan_advice`, and `auto_explain`
  clean/rebuild/install, global-lifetime scanner coverage, and process-mode
  EXPLAIN extension-option smoke after classifying EXPLAIN extension
  registries. The smoke loaded `pg_overexplain`, ran
  `EXPLAIN (DEBUG, RANGE_TABLE)`, loaded `pg_plan_advice`, ran
  `EXPLAIN (PLAN_ADVICE)`, loaded `auto_explain`, and validated
  `auto_explain.log_extension_options`.
- focused `seclabel.o` compile coverage, full rebuild/install,
  `dummy_seclabel` clean/rebuild/install, global-lifetime scanner coverage,
  and `dummy_seclabel` regression coverage after classifying the
  security-label provider registry. The first direct run failed when
  `CREATE SUBSCRIPTION` loaded an unpatched temp-install
  `libpqwalreceiver.dylib`; after patching its `libpq.5.dylib` install name,
  the direct `dummy_seclabel` regression passed.
- focused `spi.o` compile coverage, backend clean plus generated-header
  recovery, full rebuild/install, PL/pgSQL clean/rebuild/install,
  global-lifetime scanner coverage, and PL/pgSQL regression coverage after
  classifying SPI API and connection-stack state. The first
  `gmake -C src/pl/plpgsql/src check` run recreated `tmp_install` and failed
  before SQL started with the known macOS `libpq.5.dylib` loader error; after
  patching the recreated temp-install binaries, the equivalent direct
  `pg_regress` invocation passed all 13 PL/pgSQL tests.
- focused `instrument.o` compile coverage, backend clean plus
  generated-header recovery, full rebuild/install, `pg_stat_statements`
  clean/rebuild/install, global-lifetime scanner coverage, and process-mode
  instrumentation smoke coverage after classifying executor instrumentation
  counters. The runtime smoke preloaded `pg_stat_statements`, created the
  extension, ran `EXPLAIN (ANALYZE, BUFFERS, WAL)` against an insert, verified
  the table contents, and confirmed `pg_stat_statements` recorded the query.
- focused `execExprInterp.o` compile coverage, full rebuild/install,
  global-lifetime scanner coverage, and process-mode expression interpreter
  smoke coverage after moving computed-goto dispatch lookup state to
  backend-local TLS. The runtime smoke exercised prepared expression
  execution, CASE, scalar-array operations, array containment, JSONB
  expressions, aggregate filters/transitions, and `EXPLAIN (VERBOSE)` over the
  prepared plan.
- focused `analyze.o` compile coverage, global-lifetime scanner coverage, and
  process-mode ANALYZE smoke coverage after classifying ANALYZE execution
  state. `gmake -C src/test/regress check-tests TESTS="analyze"` recreated
  `tmp_install` and failed before SQL started with the known macOS
  `libpq.5.dylib` loader error; direct smoke coverage then patched the
  recreated temp-install binaries, ran inheritance-tree `ANALYZE VERBOSE`,
  `VACUUM (ANALYZE, VERBOSE)`, verified table stats visibility, and confirmed
  inherited `pg_stats` rows were loaded.
- focused `event_trigger.o` compile coverage, global-lifetime scanner
  coverage, and fixture-backed `event_trigger` regression coverage after
  classifying event-trigger query execution state. A direct `event_trigger`
  run failed because the test expects `heap2` from `create_am`; a direct
  `test_setup create_am event_trigger` run then had `event_trigger` pass but
  exposed `create_am`'s own `create_index` fixture dependency. The final direct
  `pg_regress` invocation included the schedule prefix through `create_index`
  and passed all 17 tests including `create_am` and `event_trigger`.
- focused `trigger.o` compile coverage, full rebuild/install,
  global-lifetime scanner coverage, and fixture-backed `triggers` regression
  coverage after classifying after-trigger transaction-tree state. The direct
  `pg_regress` invocation included the schedule prefix through `constraints`
  and passed all 24 tests including `triggers`.
- focused `vacuum.o` and `vacuumparallel.o` compile coverage,
  global-lifetime scanner coverage, backend clean/rebuild/install coverage,
  fixture-backed `vacuum`/`guc` regression coverage, and a live temp-cluster
  `VACUUM (VERBOSE, PARALLEL 2)` smoke after classifying vacuum cost-delay,
  failsafe, and parallel-vacuum cost pointer state. An incremental
  rebuild/install after changing exported TLS declarations in `vacuum.h`
  crashed during `initdb` post-bootstrap startup on macOS; following the
  documented backend clean plus generated-file recovery fixed it. The final
  direct `pg_regress` invocation included the schedule prefix through
  `vacuum` and `guc` and passed all 28 tests. The smoke created a table and
  index, set `vacuum_cost_delay = 1` and `vacuum_cost_limit = 10`, ran
  `VACUUM (VERBOSE, PARALLEL 2)`, and verified those settings remained visible
  as `1ms` and `10` in the session.
- focused `aset.o` compile coverage, incremental full rebuild/install,
  global-lifetime scanner coverage, and fixture-backed regression coverage
  after classifying allocation-set context freelists as backend-local TLS. A
  too-small direct `select` run first showed fixture/order drift because it
  skipped the documented schedule prefix; the final direct `pg_regress`
  invocation included the prefix through `create_index`, `triggers`, `select`,
  and `guc` and passed all 26 tests.
- focused `pgstat_xact.o` compile coverage, incremental full rebuild/install,
  global-lifetime scanner coverage, and fixture-backed `stats_ext` plus
  `stats` regression coverage after classifying cumulative-statistics
  transaction stack state. A standalone `stats` run failed because the test
  expects `test_setup`, `create_misc`, `create_table`, `create_index`, and the
  `check_estimated_rows()` helper from `stats_ext`; the final direct
  `pg_regress` invocation included those fixtures and passed all 30 tests.
- focused `pgstat_database.o` compile coverage, global-lifetime scanner
  coverage, backend clean plus generated-header recovery, full rebuild/install,
  and fixture-backed `stats_ext` plus `stats` regression coverage after
  classifying cumulative database-statistics pending counters and session-end
  state. The first direct `pg_regress` invocation failed before PostgreSQL
  started because the stale `INITDB_TEMPLATE` path no longer existed after
  recreating `tmp_install`; rerunning without the template shortcut performed
  a normal `initdb` and passed all 30 tests.
- focused `pgstat.o` compile coverage, global-lifetime scanner coverage,
  backend clean plus generated-header recovery, full rebuild/install, and
  fixture-backed `stats_ext` plus `stats` regression coverage after
  classifying cumulative-statistics infrastructure state.  The static
  self-referential `DLIST_STATIC_INIT` for `pgStatPending` was replaced with
  explicit `dlist_init()` in `pgstat_initialize()` before pending stats can be
  queued. The direct `pg_regress` invocation passed all 30 tests.
- focused `pgstat_shmem.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and direct temp-cluster statistics smoke
  after classifying shared-entry reference cache state. The smoke created and
  analyzed a table, forced stats flushes, checked relation and database stats
  visibility, reset the table counters through
  `pg_stat_reset_single_table_counters()`, and shut down cleanly.
- focused `backend_status.o` and `backend_progress.o` compile coverage,
  global-lifetime scanner coverage, backend clean plus generated-header
  recovery, full rebuild/install, and fixture-backed backend-status regression
  coverage after classifying backend-status shared-memory handles and
  backend-local status snapshot state. The direct `pg_regress` invocation
  included `privileges`, `misc_functions`, `sysviews`, `rules`, `guc`,
  `stats_ext`, and `stats` on top of the core fixture prefix and passed all
  34 tests.
- focused `pgstat_backend.o`, `pgstat_function.o`, `pgstat_io.o`,
  `pgstat_lock.o`, `pgstat_slru.o`, and `pgstat_wal.o` compile coverage,
  global-lifetime scanner coverage, incremental full rebuild/install, and
  fixture-backed backend-status/statistics regression coverage after
  classifying cumulative per-kind pending-statistics state. The direct
  `pg_regress` invocation included `privileges`, `misc_functions`, `sysviews`,
  `rules`, `guc`, `stats_ext`, and `stats` on top of the core fixture prefix
  and passed all 34 tests.
- focused configured libpq compile coverage for `auth.o`, `hba.o`, and
  `be-secure.o`, global-lifetime scanner coverage, backend clean plus
  generated-header recovery, full configured non-SSL rebuild/install, and a
  direct temp-instance `guc` regression smoke after classifying HBA/ident,
  PAM, and SSL authentication state. The temp-instance smoke exercised
  `initdb`, server startup, local authentication, `psql` connection, and SQL
  execution and passed. Auth-specific TAP tests were not run because this
  macOS Perl lacks `IPC::Run`; OpenSSL object compile coverage was not run
  because this checkout is configured with `with_ssl = no`, so
  `be-secure-openssl.c` requires an SSL-enabled build for meaningful compile
  validation.
- focused `auth-oauth.o`, `pqsignal.o`, `main.o`, and `rbtree.o` compile
  coverage, global-lifetime scanner coverage, incremental full rebuild/install,
  and a direct temp-instance startup/auth regression smoke after classifying
  OAuth validator singleton state, signal-mask templates, server executable
  startup state, and the rb-tree sentinel.
- focused `extension.o` and `repack.o` compile coverage and global-lifetime
  scanner coverage, incremental full rebuild/install, and a direct
  temp-cluster `pg_available_extensions` plus `CREATE EXTENSION plpgsql` smoke
  after classifying the extension sibling lookup cache and client-backend
  REPACK decoding-worker handle state.
- focused `repack_worker.o` compile coverage, global-lifetime scanner
  coverage, incremental full rebuild/install, focused core `guc` regression
  coverage, and direct temp-cluster `REPACK (CONCURRENTLY)` smoke after
  classifying REPACK decoding-worker state. The live smoke started a
  `wal_level=logical` cluster, repacked a primary-keyed permanent table,
  verified the updated row count and absence of leaked `repack_%` replication
  slots, and confirmed the server log registered, started, and exited a
  `REPACK decoding worker`.
- focused `jit.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and a direct temp-cluster
  `SELECT pg_jit_available()` smoke with JIT disabled after classifying the
  generic JIT provider loader cache as session-local TLS. This checkout is
  configured with `with_llvm = no`, so LLVM provider compile coverage remains
  for an LLVM-enabled build.
- global-lifetime scanner coverage, incremental full non-LLVM rebuild/install,
  and the same direct temp-cluster `SELECT pg_jit_available()` smoke after
  classifying LLVM provider state. This checkout is configured with
  `with_llvm = no`; direct `src/backend/jit/llvm` compile coverage and runtime
  JIT execution remain for an LLVM-enabled build.
- focused `extensible.o`, `outfuncs.o`, and `read.o` compile coverage,
  global-lifetime scanner coverage, incremental full rebuild/install, and
  direct temp-cluster node I/O smoke coverage through `EXPLAIN (VERBOSE,
  FORMAT JSON) SELECT 1` after classifying node registry and node
  serialization/parser scratch state.
- focused `geqo_main.o`, `costsize.o`, `extendplan.o`, and `predtest.o`
  compile coverage, global-lifetime scanner coverage, backend clean plus
  generated-header recovery, full rebuild/install, and direct temp-cluster
  planner smoke coverage through `EXPLAIN (VERBOSE) SELECT * FROM generate_series(1, 3) g`
  after classifying planner-extension ID mapping, `disable_cost`, and
  predicate proof cache state.
- focused AIO compile coverage for `aio.o`, `aio_init.o`, and
  `aio_target.o`, global-lifetime scanner coverage, backend clean plus
  generated-header recovery, full rebuild/install, and a direct temp-cluster
  AIO smoke after classifying the shared AIO control pointers, backend-local
  AIO state pointer, runtime method dispatch pointer, and immutable AIO method
  and target tables.
- focused `method_worker.o` compile coverage, global-lifetime scanner
  coverage, incremental full rebuild/install, focused core `guc` regression
  coverage, and direct temp-cluster worker-AIO smoke after classifying AIO
  worker method state. The live smoke used `io_method=worker`, verified two
  `io worker` backends were visible, created heap data large enough to
  exercise buffer IO, forced checkpoints and sequential scans, verified SQL
  results, and stopped the server with fast shutdown.
- focused `method_io_uring.o` compile coverage, global-lifetime scanner
  coverage, and incremental full rebuild/install after classifying io_uring
  AIO method state. Runtime io_uring coverage was not available on this macOS
  checkout because `IOMETHOD_IO_URING_ENABLED` is not active.
- focused `s_lock.o` compile coverage and global-lifetime scanner coverage
  after classifying the `S_LOCK_TEST` standalone test lock as runtime test
  binary state.
- focused WAL sender compile coverage for `walsender.o`, `syncrep.o`,
  `slot.o`, `postinit.o`, `backend_startup.o`, and related direct users,
  global-lifetime scanner coverage, backend clean plus generated-header
  recovery, full rebuild/install, and a direct temp-cluster replication-protocol
  smoke after classifying the shared WAL sender registry and backend-local WAL
  sender identity, wakeup, streaming, signal, logical decoding, and lag-tracker
  state.
- focused WAL receiver metadata compile coverage for `walreceiver.o`,
  `walreceiverfuncs.o`, and `slotfuncs.o`, global-lifetime scanner coverage,
  incremental full rebuild/install, and a direct temp-cluster replication
  smoke after classifying the shared WAL receiver control pointer, runtime WAL
  receiver function dispatch pointer, and immutable slot-sync reason names.
- focused buffer-manager compile coverage for `buf_init.o`, `buf_table.o`,
  `bufmgr.o`, `freelist.o`, and `localbuf.o`, global-lifetime scanner
  coverage, backend clean plus generated-header recovery, full rebuild/install,
  and direct temp-cluster shared/local-buffer smoke after classifying shared
  buffer control structures as shared-memory state and private refcount,
  backend writeback, and local-buffer structures as backend-local TLS.
- focused lock/wait compile coverage for `lock.o`, `lwlock.o`,
  `condition_variable.o`, `s_lock.o`, `wait_event.o`, and
  `wait_event_funcs.o`, global-lifetime scanner coverage, backend clean plus
  generated-header recovery, full rebuild/install, and direct temp-cluster
  concurrent lock smoke after classifying heavyweight-lock shared tables,
  backend-local local lock state, held LWLocks, condition-variable sleep state,
  spin-delay state, and wait-event registry state. The smoke used two
  concurrent backends, observed one `pg_stat_activity` relation-lock waiter,
  verified the waiter acquired the lock after release, and exercised advisory
  lock acquisition/release. A direct `PG_THREAD_LOCAL` conversion of
  `my_wait_event_info` was tested and rejected in this slice because it caused
  a bootstrap bus error on macOS; this variable remains classification-only
  until Phase 9 introduces the wait/wakeup boundary.
- focused `waiteventset.o` and `latch.o` compile coverage, global-lifetime
  scanner coverage, incremental full rebuild/install, isolation `timeouts`
  coverage, and a live temp-cluster `pg_cancel_backend()` smoke after
  classifying wait-event wake channel state as carrier-local TLS. The live
  smoke started one backend blocked in `pg_sleep(30)`, canceled it from another
  backend, and verified `ERROR: canceling statement due to user request`.
- focused `pmsignal.o`, `postmaster.o`, `launch_backend.o`, and `pmchild.o`
  compile coverage, global-lifetime scanner coverage, incremental full
  rebuild/install, focused core `guc` regression coverage, and direct
  temp-cluster startup/connection/shutdown smoke after classifying
  postmaster-signal state. The live smoke initialized a cluster, started the
  server, connected through `psql`, verified the current backend was visible
  in `pg_stat_activity`, and stopped the server with fast shutdown.
- focused `pmchild.o`, `postmaster.o`, and `launch_backend.o` compile
  coverage, global-lifetime scanner coverage, incremental full
  rebuild/install, focused core `guc` regression coverage, and direct
  temp-cluster startup/two-connection/shutdown smoke after classifying
  postmaster child-slot state. The live smoke verified a client backend was
  visible through `pg_stat_activity` and that a connected backend had a
  positive backend PID before fast shutdown.
- focused `launch_backend.o` compile coverage and global-lifetime scanner
  coverage after classifying the child-launch metadata table as immutable.
- focused `postmaster.o`, `launch_backend.o`, and `syslogger.o` compile
  coverage, global-lifetime scanner coverage, and incremental full
  rebuild/install after classifying postmaster supervisor state as
  runtime-global control-plane state. A direct temp-cluster startup/connection
  smoke verified that a Unix-socket backend appears in `pg_stat_activity`
  before fast shutdown.
- focused `postmaster.o`, `backend_startup.o`, `postgres.o`, and
  `postinit.o` compile coverage, global-lifetime scanner coverage, backend
  clean plus generated-header recovery, full rebuild/install, and direct
  temp-cluster connection-startup smoke after converting
  `ClientAuthInProgress` to connection-local TLS. The smoke verified
  `current_user` and current backend visibility in `pg_stat_activity`.
- global-lifetime scanner coverage after skipping generated Bison/Flex parser
  outputs that were producing false unclassified mutable-global records. The
  regenerated baseline dropped from 179 to 48 unclassified entries with no new
  unclassified mutable globals.
- global-lifetime scanner syntax and baseline coverage after skipping the
  checksum include-fragment, ignoring typedef attribute/closing tails, and
  recognizing const pointer objects as immutable based on their declaration
  prefix. The regenerated baseline dropped from 42 to 32 unclassified entries
  with no new unclassified mutable globals.
- focused `pg_prng.o`, `postmaster.o`, `dsm.o`, `s_lock.o`, `fd.o`,
  `xact.o`, and `postgres.o` compile coverage, global-lifetime scanner
  coverage, common/backend clean plus generated-header recovery, full
  rebuild/install, and direct temp-cluster PRNG smoke after converting
  `pg_global_prng_state` to backend-local TLS. The live smoke initialized a
  cluster, exercised `random()`, created and analyzed a temporary table, and
  stopped the server with fast shutdown.
- focused `instr_time.o` and `instrument.o` compile coverage,
  global-lifetime scanner coverage, incremental full rebuild/install, and
  direct temp-cluster timing smoke after classifying common timing conversion
  state as runtime-global. The live smoke initialized a cluster, exercised
  `pg_sleep`, verified `EXPLAIN ANALYZE` emitted timing data, and stopped the
  server with fast shutdown.
- focused `file_perm.o`, `fd.o`, `postmaster.o`, `syslogger.o`, and
  `basebackup.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and direct temp-cluster file-creation
  smoke after classifying data-directory file-permission state as
  runtime-global. The live smoke initialized a cluster, created a database,
  created and populated a heap table, forced a checkpoint, and stopped the
  server with fast shutdown.
- focused `pg_cpu_x86.o` and `instr_time.o` compile coverage,
  global-lifetime scanner coverage, incremental full rebuild/install, and
  direct temp-cluster timing smoke after classifying the runtime CPU feature
  cache as runtime-global. The live smoke verified `EXPLAIN ANALYZE` emitted
  timing data before fast shutdown.
- focused `encnames.o` and `mbutils.o` compile coverage, global-lifetime
  scanner coverage, incremental full rebuild/install, and direct temp-cluster
  encoding smoke after making the gettext encoding-name pointer table
  immutable. The live smoke verified database encoding lookup and client
  encoding changes before fast shutdown.
- focused `logging.o`, common logging users, `xlogreader.o`, and frontend
  `print.o`/`cancel.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, and installed-tool `--help` smoke for
  `pg_isready` and `pg_waldump` after classifying common logging level state
  as runtime-global.
- focused frontend `cancel.o`, `print.o`, `parallel_slot.o`, `query_utils.o`,
  psql object, and script object compile coverage, global-lifetime scanner
  coverage, incremental full rebuild/install, and installed `psql --help`
  smoke after classifying frontend cancellation/print-loop flags as
  runtime-global.
- focused `getopt.o`, frontend utility object, timezone, and isolationtester
  compile coverage, global-lifetime scanner coverage, incremental full
  rebuild/install, and installed getopt smoke for `pg_controldata --version`,
  `pg_waldump --help`, and `psql --help` after classifying command-line
  option parser compatibility globals as runtime-global.
- global-lifetime scanner coverage after best-effort Windows platform shim
  classification. This covered annotations for Windows signal emulation,
  socket compatibility state, timer helper-thread state, and dynamically loaded
  NTDLL function pointers. No Windows build or runtime test was run on this
  macOS checkout, so this validation is static/code-review-only for Windows.
  A focused macOS `gmake -C src/port win32ntdll.o` probe failed before compile
  on missing Windows SDK header `ntstatus.h`, as expected for this host.
- focused `syslogger.o`, `postmaster.o`, and `launch_backend.o` compile
  coverage, global-lifetime scanner coverage, incremental full
  rebuild/install, and direct temp-cluster `logging_collector=on` smoke after
  classifying syslogger service state. The live smoke started the collector,
  emitted a `RAISE LOG` marker from SQL, verified `current_logfiles`, verified
  the collected log file was non-empty, found the marker in that file, and
  stopped the server with fast shutdown.
- focused `bgworker.o`, `postmaster.o`, `elog.o`, `postgres.o`, `parallel.o`,
  and `applyparallelworker.o` compile coverage, global-lifetime scanner
  coverage, backend clean plus generated-header recovery, full rebuild/install,
  `worker_spi` clean rebuild/install, focused core `guc` regression coverage,
  and direct temp-cluster dynamic background-worker smoke after classifying
  background-worker registration state and `MyBgworkerEntry`. The live smoke
  created the `worker_spi` extension, launched a dynamic worker, waited for the
  worker-created schema to appear, verified the worker's `backend_type` in
  `pg_stat_activity`, and stopped the server with fast shutdown.
- focused `bgwriter.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, focused core `guc` regression coverage,
  and direct temp-cluster bgwriter smoke after classifying bgwriter snapshot
  throttle state. The live smoke verified the background writer was visible in
  `pg_stat_activity`, created a heap table, ran `CHECKPOINT`, checked the row
  count, and stopped the server with fast shutdown.
- focused `checkpointer.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, focused core `guc` regression coverage,
  and direct temp-cluster checkpoint smoke after classifying checkpointer
  shared/progress state. The live smoke verified the checkpointer was visible
  in `pg_stat_activity`, created a heap table, observed a current WAL LSN, ran
  `CHECKPOINT`, checked the row count, and stopped the server with fast
  shutdown.
- focused `pgarch.o` and `shell_archive.o` compile coverage, global-lifetime
  scanner coverage, backend clean plus generated-header recovery, full
  rebuild/install, focused core `guc` regression coverage, and direct
  temp-cluster `archive_mode=on` smoke after classifying archiver
  shared/module state. The live smoke verified the archiver was visible in
  `pg_stat_activity`, generated WAL, forced a WAL switch and checkpoint,
  verified at least one archived WAL file, and stopped the server with fast
  shutdown.
- focused `walsummarizer.o` compile coverage, global-lifetime scanner
  coverage, incremental full rebuild/install, focused core `guc` regression
  coverage, and direct temp-cluster `summarize_wal=on` smoke after classifying
  WAL summarizer shared/progress state. The live smoke verified the
  walsummarizer was visible in `pg_stat_activity`, verified
  `pg_get_wal_summarizer_state()` exposed a live summarizer PID, generated WAL,
  forced WAL switches and checkpoints, verified `pg_available_wal_summaries()`
  produced summary rows, checked final summarizer state, and stopped the
  server with fast shutdown.
- focused `startup.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, focused core `guc` regression coverage,
  and direct temp-cluster startup/connection/shutdown smoke after classifying
  startup-process signal and progress state. The live smoke initialized a
  cluster, started the server with startup progress logging enabled, connected
  through `psql`, verified postmaster start time, performed a heap
  create/insert/count round trip, and stopped the server with fast shutdown.
- focused `walreceiver.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, focused core `guc` regression coverage,
  and direct primary/standby streaming replication smoke after classifying WAL
  receiver connection and stream state. The live smoke initialized a primary,
  took a `pg_basebackup -R` standby, verified a visible `walreceiver` backend,
  replayed the initial table contents, inserted more rows on the primary,
  verified the standby caught up to the new row count, and stopped both
  servers with fast shutdown.
- focused `launcher.o`, `worker.o`, `applyparallelworker.o`, and
  `tablesync.o` compile coverage, global-lifetime scanner coverage, backend
  clean plus generated-header recovery, full rebuild/install, focused core
  `guc` regression coverage, and direct publisher/subscriber logical
  replication smoke after classifying logical replication launcher and worker
  identity state. The live smoke created a publication and subscription,
  verified visible `logical replication launcher` and
  `logical replication apply worker` backends on the subscriber, copied the
  initial table contents, applied additional publisher inserts, and stopped
  both servers with fast shutdown.
- focused `worker.o`, `launcher.o`, `applyparallelworker.o`, and
  `tablesync.o` compile coverage, global-lifetime scanner coverage, backend
  clean plus generated-header recovery, full rebuild/install, focused core
  `guc` regression coverage, and direct publisher/subscriber logical
  replication smoke after classifying common logical replication apply-worker
  state. The live smoke verified visible `logical replication launcher` and
  `logical replication apply worker` backends on the subscriber, copied 1000
  rows, applied a follow-up publisher insert to 1500 rows, and stopped both
  servers with fast shutdown.
- focused `worker.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, focused core `guc` regression coverage,
  and direct publisher/subscriber logical replication smoke with
  `streaming = on` and publisher-side `debug_logical_replication_streaming =
  immediate` after classifying streamed apply-worker scratch state. The live
  smoke verified visible `logical replication launcher` and `logical
  replication apply worker` backends on the subscriber, applied a streamed
  transaction containing 1200 rows and a savepoint/subtransaction segment, and
  stopped both servers with fast shutdown.
- focused `applyparallelworker.o`, `worker.o`, and `launcher.o` compile
  coverage, global-lifetime scanner coverage, backend clean plus
  generated-header recovery, full rebuild/install, focused core `guc`
  regression coverage, and direct publisher/subscriber logical replication
  smoke with `streaming = parallel` and publisher-side
  `debug_logical_replication_streaming = immediate` after classifying logical
  parallel apply-worker state. The live smoke held the publisher transaction
  open while polling the subscriber, verified visible `logical replication
  launcher`, `logical replication apply worker`, and `logical replication
  parallel worker` backends, applied a streamed transaction containing 1400
  rows and a savepoint/subtransaction segment, and stopped both servers with
  fast shutdown.
- focused `tablesync.o`, `worker.o`, `launcher.o`, and
  `applyparallelworker.o` compile coverage, global-lifetime scanner coverage,
  backend clean plus generated-header recovery, full rebuild/install, focused
  core `guc` regression coverage, and direct publisher/subscriber logical
  replication smoke after classifying logical table synchronization state. The
  live smoke created a subscription with `copy_data = true`, verified the
  subscriber log recorded the table synchronization worker, copied 2500
  preexisting publisher rows through the table sync path, applied a follow-up
  publisher insert to 2750 rows, and stopped both servers with fast shutdown.
- focused `sequencesync.o`, `worker.o`, and `launcher.o` compile coverage,
  global-lifetime scanner coverage, incremental full rebuild/install, focused
  core `guc` regression coverage, and direct publisher/subscriber logical
  replication smoke after classifying logical sequence synchronization state.
  The live smoke created a publication for all sequences, verified the
  subscriber log recorded the sequence synchronization worker, synchronized a
  sequence to `READY` at value 125, advanced the publisher sequence, refreshed
  sequences, verified the subscriber sequence reached value 150, and stopped
  both servers with fast shutdown.
- focused `logicalctl.o`, `xlog.o`, and `xact.o` compile coverage,
  global-lifetime scanner coverage, backend clean plus generated-header
  recovery, full rebuild/install, focused core `guc` regression coverage, and
  direct temp-cluster logical decoding smoke after classifying logical decoding
  control state. The smoke initialized a cluster with `wal_level = replica`,
  created a `pgoutput` logical replication slot, verified the slot metadata in
  `pg_replication_slots`, dropped the slot, and stopped the server with fast
  shutdown.
- focused `origin.o` compile coverage, caller coverage through clean
  rebuild/install after the exported `origin.h` declaration changed,
  global-lifetime scanner coverage, focused core `guc` regression coverage,
  and direct temp-cluster replication-origin smoke after classifying
  replication origin state. The smoke created an origin, verified session
  setup state before and after `pg_replication_origin_session_setup`, assigned
  transaction origin metadata, committed a write, verified session progress,
  reset the session origin, dropped the origin, and stopped the server with
  fast shutdown.
- focused `relation.o`, `worker.o`, and `tablesync.o` compile coverage,
  global-lifetime scanner coverage, incremental full rebuild/install, focused
  core `guc` regression coverage, and direct publisher/subscriber logical
  replication smoke after classifying logical relation-map caches. The smoke
  used a plain publisher table and a partitioned subscriber target, copied
  existing rows through table synchronization, applied follow-up inserts, and
  verified the subscriber partition counts were split across the relation-map
  and partition-map paths before stopping both servers with fast shutdown.
- focused `slotsync.o`, `backend_runtime.o`, and `postgres.o` compile
  coverage, global-lifetime scanner coverage, incremental full
  rebuild/install, focused core `guc` regression coverage, and direct
  temp-cluster `pg_sync_replication_slots()` entrypoint smoke after
  classifying slot synchronization state. The smoke verified that the manual
  sync SQL function reaches the expected primary-mode rejection and stops the
  server cleanly.
- focused `snapbuild.o`, `logical.o`, and `slotfuncs.o` compile coverage,
  global-lifetime scanner coverage, incremental full rebuild/install, focused
  core `guc` regression coverage, and direct temp-cluster replication-protocol
  logical slot smoke after classifying logical snapshot builder export state.
  The smoke issued `CREATE_REPLICATION_SLOT ... LOGICAL ... (SNAPSHOT
  'export')`, verified the exported snapshot name from the replication
  protocol response, dropped the slot, checked the server log for the exported
  logical decoding snapshot message, and stopped the server cleanly.
- focused `syncutils.o`, `worker.o`, `tablesync.o`, `launcher.o`, and
  `pgoutput.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, focused core `guc` regression coverage,
  and direct publisher/subscriber logical replication smoke after classifying
  synchronization relation-state and pgoutput cache state. The smoke used a
  plain publisher table and a partitioned subscriber target, copied existing
  rows through table synchronization, applied follow-up inserts through
  pgoutput, verified subscriber partition counts, and stopped both servers
  with fast shutdown.
- focused `syncrep.o`, `walsender.o`, `xact.o`, and `twophase.o` compile
  coverage, global-lifetime scanner coverage, backend clean plus
  generated-header recovery, full rebuild/install, focused core `guc`
  regression coverage, and direct temp-cluster synchronous-replication GUC
  smoke after classifying synchronous replication wait/config state. The
  smoke parsed `synchronous_standby_names = 'ANY 1 (*)'`, changed
  `synchronous_commit` through `remote_write`, `remote_apply`, and `off`,
  performed a non-blocking write with `synchronous_commit = off`, and avoided
  a standby-less synchronous commit that would intentionally wait forever.
- focused `datachecksum_state.o` compile coverage, global-lifetime scanner
  coverage, incremental full rebuild/install, focused core `guc` regression
  coverage, and direct temp-cluster data-checksum worker smoke after
  classifying online data-checksum worker state. The live smoke initialized a
  cluster with `--no-data-checksums`, created heap data, verified
  `data_checksums` started `off`, called `pg_enable_data_checksums(0, 100)`,
  observed a `datachecksums%` backend in `pg_stat_activity`, verified
  `data_checksums` reached `on`, checked the heap row count, and stopped the
  server with fast shutdown.
- focused `autovacuum.o` compile coverage, global-lifetime scanner coverage,
  incremental full rebuild/install, focused core `guc` regression coverage,
  and direct temp-cluster autovacuum launcher/worker smoke after classifying
  autovacuum launcher/worker state. The live smoke verified the autovacuum
  launcher was visible, created a table with aggressive autovacuum reloptions,
  generated update/delete churn, observed autovacuum/analyze stats for the
  table, checked autovacuum log output, and stopped the server with fast
  shutdown.
- focused IPC/shared-memory compile coverage for `ipc.o`, `ipci.o`, and
  `shmem.o`, global-lifetime scanner coverage, backend clean plus
  generated-header recovery, full rebuild/install, and direct temp-cluster
  lifecycle smoke after classifying backend-exit compatibility mirrors,
  shared-memory request/setup state, and fixed shared-memory pointers. The
  smoke initialized a cluster, started the server, executed SQL before and
  after `pg_terminate_backend()` against a sleeping backend, observed the
  expected FATAL disconnect for the terminated backend, verified the server
  stayed usable from a new connection, and shut down cleanly.
- focused storage-manager compile coverage for `md.o` and `smgr.o`,
  global-lifetime scanner coverage, incremental full rebuild/install, and
  direct temp-cluster smgr smoke after classifying the md/smgr backend cache
  state. The smoke created and extended heap/index/temp relations, checked
  relation storage size, vacuumed, checkpointed, truncated, inserted after
  truncate, dropped the relations, and shut down cleanly.
- focused sync-manager compile coverage for `sync.o`, global-lifetime scanner
  coverage, incremental full rebuild/install, and direct temp-cluster sync
  smoke with `fsync = on` after classifying pending sync state. The smoke
  created and extended a heap relation, forced checkpoints around insert,
  update, delete, and drop work, and shut down cleanly.
- focused tcop compile coverage for `backend_startup.o`, `dest.o`, and
  `pquery.o`, global-lifetime scanner coverage, backend clean plus
  generated-header recovery, full rebuild/install, and direct temp-cluster
  query/portal smoke after converting exported `conn_timing` and
  `ActivePortal` declarations to TLS. The smoke enabled
  `log_connections = 'setup_durations'`, verified the setup-duration log
  entry, exercised named cursor fetch/move paths, copied query output through
  the normal destination machinery, and shut down cleanly.
- focused predicate-lock compile coverage for `predicate.o`, global-lifetime
  scanner coverage, incremental full rebuild/install, and direct temp-cluster
  serializable transaction smoke after classifying SSI shared/runtime/backend
  state. The smoke created a table, ran a serializable transaction that read
  and wrote data, verified live `SIReadLock` entries in `pg_locks`, committed,
  queried afterward, dropped the table, and shut down cleanly.
- focused lock-manager compile coverage for `lmgr.o`, global-lifetime scanner
  coverage, incremental full rebuild/install, and direct temp-instance
  `insert_conflict` regression coverage after classifying speculative insertion
  token state.
- Gate C refresh after the Windows/static scanner cleanup: incremental
  non-Windows `gmake -j8`, global-lifetime scanner baseline coverage with the
  one documented `tsrank.c` typedef artifact, direct extension backend-model
  regression coverage for `test_extensions`, `test_extdepend`,
  `test_ext_backend_model`, and `test_ext_backend_model_pooled`, and direct
  PL/pgSQL process-mode regression coverage for all 13 PL/pgSQL tests.
  `gmake check` for the extension and PL/pgSQL targets recreated
  `tmp_install` and failed before SQL on the known macOS
  `/usr/local/pgsql/lib/libpq.5.dylib` loader path; direct reruns after
  `install_name_tool` patching passed.
- A literal top-level `gmake check-world` was attempted after the Gate C
  refresh. It recreated `tmp_install`, reached `src/test/isolation`, and failed
  before SQL because temp-installed `psql` still referenced
  `/usr/local/pgsql/lib/libpq.5.dylib`. `gmake -C src/test check` recreated
  `tmp_install` again and failed the same way. After patching the recreated
  temp install, the direct full isolation schedule passed all 129 tests and the
  direct core `parallel_schedule` regression run passed all 245 tests. This is
  a documented near-equivalent for the core process-mode part of Gate C on this
  macOS checkout, not a literal `check-world` pass.
- A later literal top-level `gmake check-world` attempt reached
  `src/test/isolation` and exposed a real DSM ownership bug during
  `multiple-row-versions`: parallel btree build creation of per-session DSM
  crashed because the backend-owned DSM list could be uninitialized in a
  constructed `PgBackend`. The fix initializes `dsm_segment_list` when
  `InitializePgProcessRuntime()` constructs the process-mode backend, removes
  the lazy initialization flag from `PgBackend`, and updates the
  `test_backend_runtime` fake backends to initialize their DSM lists
  explicitly. Focused `multiple-row-versions`, full isolation, and
  `test_backend_runtime` checks passed after the fix.
- After patching the macOS build-tree dynamic-library IDs for `libpq`,
  `libecpg`, `libpgtypes`, and `libecpg_compat`, a literal top-level
  `gmake check-world` completed successfully on this checkout. The run passed
  core isolation, core regression, all runnable `src/test/modules` checks,
  ECPG, contrib regression/isolation checks, and the other runnable make
  targets. TAP-only directories were skipped because this checkout is not
  configured with TAP support.

On macOS, the temp install still records `/usr/local/pgsql/lib/libpq.5.dylib`
in frontend binaries. The extension and PL/pgSQL checks above were run after
patching the temp-installed `initdb` or `psql` with `install_name_tool` to point
at `tmp_install/usr/local/pgsql/lib/libpq.5.dylib`; the unpatched failures were
dynamic loader failures before SQL tests ran.
