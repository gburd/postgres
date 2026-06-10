# Phase 8 Thread-Safety Floor Notes

Phase 8 is not complete yet. This note records the implementation slices that
now use the explicit `PG_THREAD_LOCAL` storage qualifier from
`src/include/utils/global_lifetime.h` as a compatibility bridge for
thread-per-session launch.

## Completed Slice

The `PG_GLOBAL_*` annotations remain classification-only. Do not make those
macros expand to TLS. Backend-local state that needs process-per-session
semantics in the initial threaded runtime uses explicit `PG_THREAD_LOCAL`
storage until it can move behind an owned runtime, backend, session, or
execution object.

The following state now uses explicit `PG_THREAD_LOCAL` storage:

- current runtime carrier pointers: `CurrentPgCarrier`, `CurrentPgBackend`,
  `CurrentPgSession`, `CurrentPgConnection`, and `CurrentPgExecution`;
- memory context globals: `CurrentMemoryContext`, `TopMemoryContext`,
  `ErrorContext`, `CacheMemoryContext`, `MessageContext`,
  `TopTransactionContext`, `CurTransactionContext`, `PortalContext`, and
  the memory-context logging recursion guard;
- resource owner globals: `CurrentResourceOwner`,
  `CurTransactionResourceOwner`, `TopTransactionResourceOwner`, and
  `AuxProcessResourceOwner`, plus the resource-release callback registry and
  optional resource-owner stats counters;
- `MyProc` and `got_deadlock_timeout`;
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
- logical apply-worker memory/error context state;
- regexp cache memory context;
- frontend protocol and connection state: `FrontendProtocol`, `MyProcPort`,
  `MyClientSocket`, `MyCancelKey`, `MyCancelKeyLength`, `PqCommMethods`,
  `FeBeWaitSet`, `whereToSendOutput`, `debug_query_string`, and the libpq
  send/receive buffers in `pqcomm.c` and GSSAPI transport buffers in
  `be-secure-gssapi.c`;
- interrupt pending flags and holdoff counters, including async notify, sinval
  catchup, config reload/shutdown, parallel query, parallel logical apply,
  slot sync, and repack interrupt flags;
- process-signal shared/backend state: `ProcSignal` as shared-memory state and
  `MyProcSignalSlot` as the current backend's slot pointer;
- backend/session identity globals: `MyProcPid`, `MyStartTime`,
  `MyStartTimestamp`, `MyLatch`, `MyPMChildSlot`, `MyProcNumber`,
  `ParallelLeaderProcNumber`, `MyDatabaseId`, `MyDatabaseTableSpace`,
  `MyDatabaseHasLoginEventTriggers`, `DatabasePath`, `MyBackendType`, `Mode`,
  and `OutputFileName`;
- backend-local latch state: `LocalLatchData` backing the early `MyLatch`
  pointer and `LatchWaitSet` backing `WaitLatch()`;
- authenticated, session, and effective-user identity state in `miscinit.c`:
  `AuthenticatedUserId`, `SessionUserId`, `OuterUserId`, `CurrentUserId`,
  `SystemUser`, `SessionUserIsSuperuser`, `SecurityRestrictionContext`, and
  `SetRoleIsActive`;
- vacuum execution state: `VacuumCostBalance` and `VacuumCostActive`;
- vacuum tuning GUC backing variables in `vacuum.c`: `vacuum_freeze_min_age`,
  `vacuum_freeze_table_age`, `vacuum_multixact_freeze_min_age`,
  `vacuum_multixact_freeze_table_age`, `vacuum_failsafe_age`,
  `vacuum_multixact_failsafe_age`, `vacuum_max_eager_freeze_failure_rate`,
  `track_cost_delay_timing`, and `vacuum_truncate`;
- transaction execution state in `xact.c`, including current transaction
  state, subtransaction/command counters, transaction timestamps, parallel
  current-XID state, unreported subtransaction XIDs, transaction abort context,
  transaction flags, logical-streaming system-scan state, and transaction
  sampling state;
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
- locale GUC backing variables and derived locale cache state in
  `pg_locale.c`, including `locale_messages`, `locale_monetary`,
  `locale_numeric`, `locale_time`, `icu_validation_level`,
  `localized_abbrev_days`, `localized_full_days`,
  `localized_abbrev_months`, `localized_full_months`, the `lconv` cache,
  `default_locale`, `CollationCacheContext`, `CollationCache`, and the
  last-used collation cache entry;
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
- GIN session USERSET GUC backing variables: `GinFuzzySearchLimit` and
  `gin_pending_list_limit`.
- async notify tracing USERSET GUC backing variable: `Trace_notify`.
- text-search session GUC/cache state: `TSCurrentConfig` and
  `TSCurrentConfigCache`.
- dynamic loader session GUC backing variable: `Dynamic_library_path`.
- plan-cache mode session GUC backing variable: `plan_cache_mode`.
- table access method and synchronized-scan session GUC backing variables:
  `default_table_access_method` and `synchronize_seqscans`.
- namespace/search-path session state in `namespace.c`: the
  `namespace_search_path` GUC backing variable, active/base search path
  derived state, temp namespace ownership state, and the search-path cache.
- large-object session/transaction state in `inv_api.c`: the
  `lo_compat_privileges` GUC backing variable and the cached
  `pg_largeobject` heap/index relation handles `lo_heap_r` and `lo_index_r`.
- sort session GUC backing variables in `tuplesort.c`: `trace_sort` and the
  debug-build `optimize_bounded_sort`.
- commit behavior session GUC backing variables: `synchronous_commit` in
  `xact.c`, plus `CommitDelay` and `CommitSiblings` in `xlog.c`.
- query/statistics session state: `compute_query_id`, `query_id_enabled`,
  `pgstat_fetch_consistency`, `pgstat_track_activities`,
  `pgstat_track_counts`, and `pgstat_track_functions`.
- logging/error-reporting session state: `Log_error_verbosity`,
  `log_min_messages_string`, and the processed `backtrace_function_list`
  derived from `backtrace_functions`.
- guarded developer node-test GUC backing variables:
  `Debug_copy_parse_plan_trees`, `Debug_raw_expression_coverage_test`, and
  `Debug_write_read_parse_plan_trees`.
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
- final backend-facing USERSET/SUSET GUC backing variables and required
  derived state: `debug_discard_caches`,
  `debug_logical_replication_streaming`, `log_replication_commands`,
  `logical_decoding_work_mem`, `max_stack_depth`,
  `max_stack_depth_bytes`, `stack_base_ptr`, `update_process_title`,
  `wal_receiver_timeout`, `wal_sender_shutdown_timeout`,
  `wal_sender_timeout`, and `wal_skip_threshold`.

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
  `io_worker_launch_interval`, `max_files_per_process`,
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

Any dynamically loaded module that references an exported global after it gains
`PG_THREAD_LOCAL` must be rebuilt against the updated headers. Stale modules can
still link but may crash because they use the old non-TLS symbol access pattern.
During validation this affected `test_ext_backend_model.dylib` and
`plpgsql.dylib`; cleaning and rebuilding those modules fixed the crashes.

## Remaining Phase 8 Work

Phase 8 still needs to cover at least:

- the rest of the required-floor audit from `MULTITHREADED_PLAN.md`.

After the final USERSET/SUSET GUC classification slice, the filtered static
report contains zero remaining unclassified generated GUC backing variables.
The plan-cache saved plan and cached expression list heads are now explicit
session-local TLS state initialized by `InitPlanCache()`, so they no longer
depend on self-referential `DLIST_STATIC_INIT` globals.
The authenticated/session/effective role identity variables in `miscinit.c` are
now session-local TLS state, preserving process-mode behavior while preventing
threaded backends from sharing one effective user/security context.
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

Before Phase 8 can be marked complete, Gate C must pass: `check-world`, static
global report checks, extension load tests using the test-only threaded backend
model, and PL/pgSQL process-mode regression tests. Gate C also fails if any
Phase 8 required-floor global remains unsafe and unclassified.

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
  the path. The local TAP harness could not run
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
- focused `ts_cache.o` compile coverage;
- fixture-backed default-text-search regression coverage:
  `test_setup copy copyselect copydml copyencoding insert insert_conflict
  create_function_c create_misc create_operator create_procedure create_table
  create_type create_schema create_index create_index_spgist create_view
  index_including index_including_gist create_aggregate create_function_sql
  create_cast constraints triggers select vacuum sanity_check guc tsearch`;
- live temp-cluster smoke coverage for `default_text_search_config`, including
  repeated `SET`, `SHOW`, `get_current_ts_config()`, and `to_tsvector()` calls;
- focused `dfmgr.o` compile coverage;
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
- focused `procarray.o` compile coverage plus transaction and snapshot
  regression coverage after classifying procarray shared/runtime/backend state.
- focused `standby.o` compile coverage plus process-mode recovery-conflict
  static scan coverage after classifying hot-standby recovery-conflict state.
- focused `resowner.o` compile coverage plus resource-owner static scan
  coverage after classifying the resource-release callback registry.
- focused `mcxt.o` compile coverage, memory-context static scan coverage, and
  process-mode query/PLpgSQL regression coverage after classifying the
  memory-context logging recursion guard.

On macOS, the temp install still records `/usr/local/pgsql/lib/libpq.5.dylib`
in frontend binaries. The extension and PL/pgSQL checks above were run after
patching the temp-installed `initdb` or `psql` with `install_name_tool` to point
at `tmp_install/usr/local/pgsql/lib/libpq.5.dylib`; the unpatched failures were
dynamic loader failures before SQL tests ran.
