/*-------------------------------------------------------------------------
 *
 * backend_runtime.c
 *	  Runtime/backend/session scaffolding for backend execution.
 *
 * The process-mode implementation uses static per-process objects. Later
 * phases can replace or extend these objects without changing callers that
 * already refer to the current runtime/backend/session/execution.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/init/backend_runtime.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gin.h"
#include "access/parallel.h"
#include "access/syncscan.h"
#include "access/tableam.h"
#include "access/toast_compression.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "archive/archive_module.h"
#include "catalog/binary_upgrade.h"
#include "catalog/storage.h"
#include "commands/async.h"
#include "commands/extension.h"
#include "commands/repack.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/vacuum.h"
#include "jit/jit.h"
#include "lib/dshash.h"
#include "libpq/crypt.h"
#include "miscadmin.h"
#include "nodes/queryjumble.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "parser/parser.h"
#include "parser/parse_expr.h"
#include "postmaster/pgarch.h"
#include "postmaster/interrupt.h"
#include "regex/regex.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/logicalworker.h"
#include "replication/slotsync.h"
#include "replication/walreceiver.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/buffile.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "storage/large_object.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "utils/backend_runtime.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"
#include "utils/plancache.h"
#include "utils/resowner.h"
#include "utils/rls.h"
#include "utils/xml.h"

PG_GLOBAL_RUNTIME PgRuntime *CurrentPgRuntime = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgCarrier *CurrentPgCarrier = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgBackend *CurrentPgBackend = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgSession *CurrentPgSession = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgConnection *CurrentPgConnection = NULL;
PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgExecution *CurrentPgExecution = NULL;

static PG_GLOBAL_RUNTIME PgRuntime process_runtime;
static PG_GLOBAL_RUNTIME PgRuntime thread_runtime;
static PG_GLOBAL_RUNTIME bool thread_runtime_initialized = false;
static PG_GLOBAL_RUNTIME bool backend_id_counter_initialized = false;
static PG_GLOBAL_RUNTIME pg_atomic_uint64 next_backend_id;
static PG_GLOBAL_RUNTIME PgRuntimeServerGUCState early_runtime_server_guc = {
	.initialized = true,
	.cluster_name_value = "",
	.config_file_name = NULL,
	.hba_file_name = NULL,
	.ident_file_name = NULL,
	.hosts_file_name = NULL,
	.external_pid_file_value = NULL
};
static PG_GLOBAL_CARRIER PgCarrier process_carrier;
static PG_GLOBAL_BACKEND PgBackend process_backend;
static PG_GLOBAL_SESSION PgSession process_session;
static PG_GLOBAL_CONNECTION PgConnection process_connection;
static PG_GLOBAL_EXECUTION PgExecution process_execution;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendCoreState early_backend_core = {
	.mode = InitProcessing
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendCommandState early_backend_command;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendLogState early_backend_log;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendExprInterpState early_backend_expr_interp;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND BackendType early_backend_type = B_INVALID;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PGPROC *early_my_proc = NULL;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND ProcNumber early_my_proc_number = INVALID_PROC_NUMBER;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND ProcNumber early_parallel_leader_proc_number = INVALID_PROC_NUMBER;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendStatus *early_my_beentry = NULL;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND BackgroundWorker *early_my_bgworker_entry = NULL;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND ResourceOwner early_aux_process_resource_owner = NULL;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendPgStatPendingState early_backend_pgstat_pending;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendInstrumentationState early_backend_instrumentation;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendBufferState early_backend_buffers;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendStorageState early_backend_storage;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendLockState early_backend_locks;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendIPCState early_backend_ipc;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendWaitState early_backend_wait_state;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendTransactionState early_backend_transaction;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendMemoryManagerState early_backend_memory_manager;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendTimeoutState early_backend_timeout;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendWalSenderState early_backend_walsender;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendReplicationState early_backend_replication = {
	.sync_rep_wait_mode = -1,
	.walreceiver_recv_file = -1,
	.walreceiver_primary_has_standby_xmin = true
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendLogicalReplicationState early_backend_logical_replication = {
	.apply_error_callback_arg.remote_attnum = -1,
	.apply_error_callback_arg.remote_xid = InvalidTransactionId,
	.apply_error_callback_arg.finish_lsn = InvalidXLogRecPtr,
	.subxact_data.subxact_last = InvalidTransactionId,
	.remote_final_lsn = InvalidXLogRecPtr,
	.stream_xid = InvalidTransactionId,
	.skip_xact_finish_lsn = InvalidXLogRecPtr,
	.last_flushpos = InvalidXLogRecPtr,
	.slotsync_sleep_ms = PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendXLogState early_backend_xlog = {
	.local_recovery_in_progress = true,
	.local_xlog_insert_allowed = -1,
	.proc_last_rec_ptr = InvalidXLogRecPtr,
	.xact_last_rec_end = InvalidXLogRecPtr,
	.xact_last_commit_end = InvalidXLogRecPtr,
	.redo_rec_ptr = InvalidXLogRecPtr,
	.open_log_file = -1,
	.local_min_recovery_point = InvalidXLogRecPtr,
	.update_min_recovery_point = true
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendRecoveryState early_backend_recovery = {
	.standby_wait_us = PG_BACKEND_STANDBY_INITIAL_WAIT_US
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendMaintenanceWorkerState early_backend_maintenance_worker = {
	.bgwriter_last_snapshot_lsn = InvalidXLogRecPtr,
	.walsummarizer_sleep_quanta = 1,
	.walsummarizer_redo_pointer_at_last_summary_removal = InvalidXLogRecPtr
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendAutovacuumState early_backend_autovacuum = {
	.av_storage_param_cost_delay = -1,
	.av_storage_param_cost_limit = -1
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendRepackState early_backend_repack = {
	.repacked_rel_locator.relNumber = InvalidOid,
	.repacked_rel_toast_locator.relNumber = InvalidOid
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendAioState early_backend_aio = {
	.my_io_worker_id = -1
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendActivityState early_backend_activity;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendUtilityState early_backend_utility;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendParallelState early_backend_parallel = {
	.worker_number = -1,
	.pq_mq_parallel_leader_proc_number = INVALID_PROC_NUMBER
};
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionIdentityState early_connection_identity;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionSocketIOState early_connection_socket_io;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionProtocolState early_connection_protocol;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionOutputState early_connection_output = {
	.where_to_send_output = DestDebug
};
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionInterruptState early_connection_interrupts;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionStartupState early_connection_startup = {
	.timing.ready_for_use = TIMESTAMP_MINUS_INFINITY
};
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionClientConnectionInfoState early_client_connection_info;
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnectionSecurityState early_connection_security;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionDatabaseState early_session_database;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionTablespaceState early_session_tablespace = {
	.initialized = true,
	.default_tablespace_name = NULL,
	.temp_tablespaces_names = NULL,
	.allow_in_place_tablespaces_value = false,
	.binary_upgrade_next_pg_tablespace_oid_value = InvalidOid
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionBinaryUpgradeState early_session_binary_upgrade = {
	.initialized = true,
	.binary_upgrade_next_pg_type_oid_value = InvalidOid,
	.binary_upgrade_next_array_pg_type_oid_value = InvalidOid,
	.binary_upgrade_next_mrng_pg_type_oid_value = InvalidOid,
	.binary_upgrade_next_mrng_array_pg_type_oid_value = InvalidOid,
	.binary_upgrade_next_heap_pg_class_oid_value = InvalidOid,
	.binary_upgrade_next_heap_pg_class_relfilenumber_value = InvalidRelFileNumber,
	.binary_upgrade_next_index_pg_class_oid_value = InvalidOid,
	.binary_upgrade_next_index_pg_class_relfilenumber_value = InvalidRelFileNumber,
	.binary_upgrade_next_toast_pg_class_oid_value = InvalidOid,
	.binary_upgrade_next_toast_pg_class_relfilenumber_value = InvalidRelFileNumber,
	.binary_upgrade_next_pg_enum_oid_value = InvalidOid,
	.binary_upgrade_next_pg_authid_oid_value = InvalidOid,
	.binary_upgrade_record_init_privs_value = false
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionDateTimeState early_session_datetime = {
	.initialized = true,
	.date_style = USE_ISO_DATES,
	.date_order = DATEORDER_MDY,
	.interval_style = INTSTYLE_POSTGRES,
	.timezone_string_value = "GMT",
	.log_timezone_string_value = "GMT",
	.session_timezone_value = NULL,
	.log_timezone_value = NULL
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionTextSearchState early_session_text_search = {
	.initialized = true,
	.current_config_value = "pg_catalog.simple",
	.current_config_cache = InvalidOid
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionConnectionGUCState early_session_connection_guc = {
	.initialized = true,
	.application_name_value = "",
	.tcp_keepalives_idle_value = 0,
	.tcp_keepalives_interval_value = 0,
	.tcp_keepalives_count_value = 0,
	.tcp_user_timeout_value = 0,
	.log_disconnections_value = false,
	.log_statement_value = 0,
	.post_auth_delay_seconds = 0,
	.restrict_nonsystem_relation_kind_string_value = "",
	.restrict_nonsystem_relation_kind_value = 0
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionParserState early_session_parser = {
	.initialized = true,
	.transform_null_equals_value = false,
	.backslash_quote_value = BACKSLASH_QUOTE_SAFE_ENCODING,
	.operator_lookup_cache = NULL
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionVacuumState early_session_vacuum = {
	.initialized = true,
	.vacuum_buffer_usage_limit_kb = 2048,
	.vacuum_cost_page_hit_value = 1,
	.vacuum_cost_page_miss_value = 2,
	.vacuum_cost_page_dirty_value = 20,
	.vacuum_cost_limit_value = 200,
	.vacuum_cost_delay_ms = 0,
	.default_statistics_target_value = 100,
	.vacuum_freeze_min_age_value = 50000000,
	.vacuum_freeze_table_age_value = 150000000,
	.vacuum_multixact_freeze_min_age_value = 5000000,
	.vacuum_multixact_freeze_table_age_value = 150000000,
	.vacuum_failsafe_age_value = 1600000000,
	.vacuum_multixact_failsafe_age_value = 1600000000,
	.track_cost_delay_timing_value = false,
	.vacuum_truncate_value = true,
	.vacuum_max_eager_freeze_failure_rate_value = 0.03,
	.local_vacuum_cost_delay_ms = 0,
	.local_vacuum_cost_limit_value = 200
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionBufferIOState early_session_buffer_io = {
	.initialized = true,
	.zero_damaged_pages_value = false,
	.track_io_timing_value = false,
	.effective_io_concurrency_value = DEFAULT_EFFECTIVE_IO_CONCURRENCY,
	.maintenance_io_concurrency_value = DEFAULT_MAINTENANCE_IO_CONCURRENCY,
	.io_combine_limit_value = DEFAULT_IO_COMBINE_LIMIT,
	.io_combine_limit_guc_value = DEFAULT_IO_COMBINE_LIMIT,
	.backend_flush_after_value = DEFAULT_BACKEND_FLUSH_AFTER
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionXactDefaultState early_session_xact_defaults = {
	.initialized = true,
	.default_xact_iso_level = XACT_READ_COMMITTED,
	.default_xact_read_only = false,
	.default_xact_deferrable = false,
	.synchronous_commit_value = SYNCHRONOUS_COMMIT_ON
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionLockWaitState early_session_lock_wait = {
	.initialized = true,
	.deadlock_timeout_ms = 1000,
	.statement_timeout_ms = 0,
	.lock_timeout_ms = 0,
	.idle_in_transaction_session_timeout_ms = 0,
	.transaction_timeout_ms = 0,
	.idle_session_timeout_ms = 0,
	.log_lock_waits_value = true,
	.log_lock_failures_value = false,
	.trace_lock_oidmin_value = FirstNormalObjectId,
	.trace_locks_value = false,
	.trace_userlocks_value = false,
	.trace_lock_table_value = 0,
	.debug_deadlocks_value = false,
	.trace_lwlocks_value = false
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionLoopState early_session_loop_state;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionLoggingState early_session_logging = {
	.initialized = true,
	.debug_print_plan_value = false,
	.debug_print_parse_value = false,
	.debug_print_raw_parse_value = false,
	.debug_print_rewritten_value = false,
	.debug_pretty_print_value = true,
#ifdef DEBUG_NODE_TESTS_ENABLED
	.debug_copy_parse_plan_trees_value = DEFAULT_DEBUG_COPY_PARSE_PLAN_TREES,
	.debug_write_read_parse_plan_trees_value = DEFAULT_DEBUG_WRITE_READ_PARSE_PLAN_TREES,
	.debug_raw_expression_coverage_test_value = DEFAULT_DEBUG_RAW_EXPRESSION_COVERAGE_TEST,
#endif
	.log_parser_stats_value = false,
	.log_planner_stats_value = false,
	.log_executor_stats_value = false,
	.log_statement_stats_value = false,
	.log_btree_build_stats_value = false,
	.event_source_value = NULL,
	.log_duration_value = false,
	.log_error_verbosity_value = PGERROR_DEFAULT,
	.log_parameter_max_length_value = -1,
	.log_parameter_max_length_on_error_value = 0,
	.log_min_error_statement_value = ERROR,
	.log_min_messages_values = {
#define PG_PROCTYPE(bktype, bkcategory, description, main_func, shmem_attach) \
		[bktype] = WARNING,
#include "postmaster/proctypelist.h"
#undef PG_PROCTYPE
	},
	.log_min_messages_string_value = NULL,
	.client_min_messages_value = NOTICE,
	.log_min_duration_sample_value = -1,
	.log_min_duration_statement_value = -1,
	.log_temp_files_value = -1,
	.log_statement_sample_rate_value = 1.0,
	.log_xact_sample_rate_value = 0,
	.backtrace_functions_value = NULL,
	.backtrace_function_list_value = NULL
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionMiscGUCState early_session_misc_guc = {
	.initialized = true,
	.allow_system_table_mods_value = false,
	.max_stack_depth_kb = 100,
	.max_stack_depth_bytes = 100 * (ssize_t) 1024,
	.session_preload_libraries_value = NULL,
	.local_preload_libraries_value = NULL,
	.dynamic_library_path_value = NULL,
	.extension_control_path_value = "$system"
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionPgStatState early_session_pgstat = {
	.initialized = true,
	.track_counts = true,
	.track_functions = TRACK_FUNC_OFF,
	.fetch_consistency = PGSTAT_FETCH_CONSISTENCY_CACHE,
	.track_activities = true,
	.session_end_cause = DISCONNECT_NORMAL,
	.last_session_report_time = 0
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionQueryIdState early_session_query_id = {
	.initialized = true,
	.compute_query_id_value = COMPUTE_QUERY_ID_AUTO,
	.query_id_enabled_value = false
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionStorageGUCState early_session_storage_guc = {
	.initialized = true,
	.ignore_checksum_failure_value = false,
	.file_copy_method_value = FILE_COPY_METHOD_COPY
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionUserGUCState early_session_user_guc = {
	.initialized = true,
	.password_encryption_value = PASSWORD_TYPE_SCRAM_SHA_256,
	.createrole_self_grant_value = "",
	.createrole_self_grant_enabled = false,
	.createrole_self_grant_options_specified = 0,
	.createrole_self_grant_options_admin = false,
	.createrole_self_grant_options_inherit = false,
	.createrole_self_grant_options_set = false
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionUserIdentityState early_session_user_identity = {
	.initialized = true,
	.authenticated_user_id = InvalidOid,
	.session_user_id = InvalidOid,
	.outer_user_id = InvalidOid,
	.current_user_id = InvalidOid,
	.system_user = NULL,
	.session_user_is_superuser = false,
	.security_restriction_context = 0,
	.set_role_is_active = false
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionCommandGUCState early_session_command_guc = {
	.initialized = true,
	.session_replication_role_value = SESSION_REPLICATION_ROLE_ORIGIN,
	.event_triggers_value = true,
	.trace_notify_value = false
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionReplicationGUCState early_session_replication_guc = {
	.initialized = true,
	.wal_sender_timeout_ms = 60 * 1000,
	.wal_sender_shutdown_timeout_ms = -1,
	.log_replication_commands_value = false,
	.wal_receiver_timeout_ms = 60 * 1000,
	.logical_decoding_work_mem_kb = 65536,
	.debug_logical_replication_streaming_value =
		DEBUG_LOGICAL_REP_STREAMING_BUFFERED
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionGeneralGUCState early_session_general_guc = {
	.initialized = true,
	.allow_alter_system_value = true,
	.row_security_value = true,
	.check_function_bodies_value = true,
	.current_role_is_superuser_value = false,
	.temp_file_limit_kb = -1,
	.num_temp_buffers_blocks = 1024,
	.role_string_value = "none",
	.lo_compat_privileges_value = false,
	.extra_float_digits_value = 1,
	.array_nulls_value = true,
	.bytea_output_value = BYTEA_OUTPUT_HEX,
	.xmlbinary_value = XMLBINARY_BASE64,
	.xmloption_value = XMLOPTION_CONTENT,
	.quote_all_identifiers_value = false,
	.plan_cache_mode_value = PLAN_CACHE_MODE_AUTO,
	.gin_fuzzy_search_limit_value = 0,
	.gin_pending_list_limit_value = 0
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionAccessWalGUCState early_session_access_wal_guc = {
	.initialized = true,
	.default_table_access_method_value = DEFAULT_TABLE_ACCESS_METHOD,
	.synchronize_seqscans_value = true,
	.default_toast_compression_value = DEFAULT_TOAST_COMPRESSION,
	.wal_compression_value = WAL_COMPRESSION_NONE,
	.wal_init_zero_value = true,
	.wal_recycle_value = true,
	.wal_consistency_checking_string_value = NULL,
	.wal_consistency_checking_value = NULL,
	.commit_delay_us = 0,
	.commit_siblings_value = 5,
	.track_wal_io_timing_value = false,
	.wal_skip_threshold_kb = 2048,
#ifdef WAL_DEBUG
	.xlog_debug_value = false,
#endif
#ifdef TRACE_SYNCSCAN
	.trace_syncscan_value = false,
#endif
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionJitGUCState early_session_jit_guc = {
	.initialized = true,
	.jit_enabled_value = false,
	.jit_provider_value = "llvmjit",
	.jit_debugging_support_value = false,
	.jit_dump_bitcode_value = false,
	.jit_expressions_value = true,
	.jit_profiling_support_value = false,
	.jit_tuple_deforming_value = true,
	.jit_above_cost_value = 100000,
	.jit_inline_above_cost_value = 500000,
	.jit_optimize_above_cost_value = 500000
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionSortGUCState early_session_sort_guc = {
	.initialized = true,
	.trace_sort_value = false,
#ifdef DEBUG_BOUNDED_SORT
	.optimize_bounded_sort_value = true,
#endif
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionQueryMemoryState early_session_query_memory = {
	.initialized = true,
	.work_mem_kb = 4096,
	.hash_mem_multiplier_value = 2.0,
	.maintenance_work_mem_kb = 65536,
	.max_parallel_maintenance_workers_value = 2
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionPlannerCostState early_session_planner_cost = {
	.initialized = true,
	.seq_page_cost_value = DEFAULT_SEQ_PAGE_COST,
	.random_page_cost_value = DEFAULT_RANDOM_PAGE_COST,
	.cpu_tuple_cost_value = DEFAULT_CPU_TUPLE_COST,
	.cpu_index_tuple_cost_value = DEFAULT_CPU_INDEX_TUPLE_COST,
	.cpu_operator_cost_value = DEFAULT_CPU_OPERATOR_COST,
	.parallel_tuple_cost_value = DEFAULT_PARALLEL_TUPLE_COST,
	.parallel_setup_cost_value = DEFAULT_PARALLEL_SETUP_COST,
	.recursive_worktable_factor_value = DEFAULT_RECURSIVE_WORKTABLE_FACTOR,
	.effective_cache_size_pages = DEFAULT_EFFECTIVE_CACHE_SIZE,
	.disable_cost_value = 1.0e10,
	.max_parallel_workers_per_gather_value = 2,
	.debug_parallel_query_value = DEBUG_PARALLEL_OFF,
	.parallel_leader_participation_value = true
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionPlannerMethodState early_session_planner_method = {
	.initialized = true,
	.enable_seqscan_value = true,
	.enable_indexscan_value = true,
	.enable_indexonlyscan_value = true,
	.enable_bitmapscan_value = true,
	.enable_tidscan_value = true,
	.enable_sort_value = true,
	.enable_incremental_sort_value = true,
	.enable_hashagg_value = true,
	.enable_nestloop_value = true,
	.enable_material_value = true,
	.enable_memoize_value = true,
	.enable_mergejoin_value = true,
	.enable_hashjoin_value = true,
	.enable_gathermerge_value = true,
	.enable_partitionwise_join_value = false,
	.enable_partitionwise_aggregate_value = false,
	.enable_parallel_append_value = true,
	.enable_parallel_hash_value = true,
	.enable_partition_pruning_value = true,
	.enable_presorted_aggregate_value = true,
	.enable_async_append_value = true,
	.enable_distinct_reordering_value = true,
	.enable_geqo_value = true,
	.enable_eager_aggregate_value = true,
	.enable_group_by_reordering_value = true,
	.enable_self_join_elimination_value = true,
	.cursor_tuple_fraction_value = DEFAULT_CURSOR_TUPLE_FRACTION,
	.constraint_exclusion_value = CONSTRAINT_EXCLUSION_PARTITION,
	.geqo_threshold_value = 12,
	.Geqo_effort_value = DEFAULT_GEQO_EFFORT,
	.Geqo_pool_size_value = 0,
	.Geqo_generations_value = 0,
	.Geqo_selection_bias_value = DEFAULT_GEQO_SELECTION_BIAS,
	.Geqo_seed_value = 0.0,
	.Geqo_planner_extension_id_value = -1,
	.min_eager_agg_group_size_value = 8.0,
	.min_parallel_table_scan_size_blocks = (8 * 1024 * 1024) / BLCKSZ,
	.min_parallel_index_scan_size_blocks = (512 * 1024) / BLCKSZ,
	.from_collapse_limit_value = 8,
	.join_collapse_limit_value = 8
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionPreparedStatementState early_session_prepared_statement;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionOnCommitState early_session_on_commit;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionSequenceState early_session_sequence;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionRegexState early_session_regex;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionLargeObjectState early_session_large_object;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionAsyncState early_session_async;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionEncodingState early_session_encoding;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionTempFileState early_session_temp_file = {
	.initialized = true,
	.num_temp_table_spaces = -1
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionRandomState early_session_random = {
	.initialized = true,
	.prng_seed_set = false
};
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionOptimizerState early_session_optimizer;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionPlanCacheState early_session_plan_cache;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionNamespaceState early_session_namespace;
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSessionLocaleState early_session_locale = {
	.initialized = true,
	.icu_validation_level_value = WARNING,
	.last_collation_cache_oid = InvalidOid
};
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendPendingInterruptState early_pending_interrupts;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackendInterruptHoldoffState early_interrupt_holdoffs;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionDebugState early_execution_debug;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionErrorState early_execution_error = {
	.errordata_stack_depth = -1
};
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionMemoryContextState early_execution_memory_contexts;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionResourceOwnerState early_execution_resource_owners;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionSPIState early_execution_spi = {
	.connected = -1
};
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionPortalState early_execution_portal;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionVacuumState early_execution_vacuum;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionNodeIOState early_execution_node_io;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionBaseBackupState early_execution_basebackup;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionAnalyzeState early_execution_analyze;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionExtensionState early_execution_extension;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionMatViewState early_execution_matview;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionSnapshotState early_execution_snapshot = {
	.transaction_xmin = FirstNormalTransactionId,
	.recent_xmin = FirstNormalTransactionId
};
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionComboCidState early_execution_combo_cid;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionXLogInsertState early_execution_xloginsert;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionXactState early_execution_xact = {
	.iso_level = XACT_READ_COMMITTED,
	.check_xid_alive = InvalidTransactionId
};
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionTransactionCleanupState early_execution_transaction_cleanup;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionReplicationScratchState early_execution_replication_scratch = {
	.replorigin_xact = {
		.origin = InvalidReplOriginId,
		.origin_lsn = InvalidXLogRecPtr,
		.origin_timestamp = 0
	}
};
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionGUCErrorState early_execution_guc_error;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionAsyncState early_execution_async;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionCatalogState early_execution_catalog = {
	.currently_reindexed_heap = InvalidOid,
	.currently_reindexed_index = InvalidOid
};
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionCatalogCacheState early_execution_catalog_cache;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionRelMapState early_execution_relmap;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionInvalidationState
			early_execution_invalidation;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionTwoPhaseRecordState
			early_execution_two_phase_records;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionTriggerState
			early_execution_trigger;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionRegexState early_execution_regex;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionValgrindState early_execution_valgrind;
static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecutionSnapBuildState early_execution_snapbuild;

StaticAssertDecl(PG_BACKEND_INTERRUPT_COUNT <= 32,
				 "PgBackendInterruptMask must fit all backend interrupts");
StaticAssertDecl(PG_EXECUTION_UNREPORTED_XIDS_CAPACITY == PGPROC_MAX_CACHED_SUBXIDS,
				 "PgExecution xact unreported XID storage must match PGPROC");

static void PgBackendInitializeIdCounter(void);
static PgBackendId PgBackendAssignId(void);
static void PgBackendWakeForInterrupt(PgBackend *backend);
static void PgRuntimeInitializeServerGUCState(PgRuntimeServerGUCState *server_guc);
static void PgRuntimeAdoptEarlyServerGUCState(PgRuntime *runtime);
static PgRuntimeServerGUCState *PgCurrentRuntimeServerGUCState(void);
static void PgConnectionAdoptEarlyIdentity(PgConnection *connection);
static void PgConnectionAdoptEarlySocketIO(PgConnection *connection);
static void PgConnectionAdoptEarlyProtocolState(PgConnection *connection);
static void PgConnectionInitializeOutputState(PgConnectionOutputState *output);
static void PgConnectionAdoptEarlyOutputState(PgConnection *connection);
static void PgConnectionInitializeStartupState(PgConnectionStartupState *startup);
static void PgConnectionAdoptEarlyInterruptState(PgConnection *connection);
static void PgConnectionAdoptEarlyStartupState(PgConnection *connection);
static void PgConnectionAdoptEarlyClientConnectionInfo(PgConnection *connection);
static void PgConnectionAdoptEarlySecurityState(PgConnection *connection);
static void PgSessionAdoptEarlyDatabaseState(PgSession *session);
static void PgSessionInitializeTablespaceState(PgSessionTablespaceState *tablespace);
static void PgSessionAdoptEarlyTablespaceState(PgSession *session);
static void PgSessionInitializeBinaryUpgradeState(PgSessionBinaryUpgradeState *binary_upgrade);
static void PgSessionAdoptEarlyBinaryUpgradeState(PgSession *session);
static void PgSessionInitializeDateTimeState(PgSessionDateTimeState *datetime);
static void PgSessionAdoptEarlyDateTimeState(PgSession *session);
static void PgSessionInitializeTextSearchState(PgSessionTextSearchState *text_search);
static void PgSessionAdoptEarlyTextSearchState(PgSession *session);
static void PgSessionInitializeConnectionGUCState(PgSessionConnectionGUCState *connection_guc);
static void PgSessionAdoptEarlyConnectionGUCState(PgSession *session);
static void PgSessionInitializeParserState(PgSessionParserState *parser);
static void PgSessionAdoptEarlyParserState(PgSession *session);
static void PgSessionInitializeVacuumState(PgSessionVacuumState *vacuum);
static void PgSessionAdoptEarlyVacuumState(PgSession *session);
static void PgSessionInitializeBufferIOState(PgSessionBufferIOState *buffer_io);
static void PgSessionAdoptEarlyBufferIOState(PgSession *session);
static void PgSessionInitializeXactDefaultState(PgSessionXactDefaultState *xact_defaults);
static void PgSessionAdoptEarlyXactDefaultState(PgSession *session);
static void PgSessionInitializeLockWaitState(PgSessionLockWaitState *lock_wait);
static void PgSessionAdoptEarlyLockWaitState(PgSession *session);
static void PgSessionInitializeLoggingState(PgSessionLoggingState *logging);
static void PgSessionAdoptEarlyLoggingState(PgSession *session);
static void PgSessionInitializeMiscGUCState(PgSessionMiscGUCState *misc_guc);
static void PgSessionAdoptEarlyMiscGUCState(PgSession *session);
static void PgSessionInitializePgStatState(PgSessionPgStatState *pgstat);
static void PgSessionAdoptEarlyPgStatState(PgSession *session);
static void PgSessionInitializeQueryIdState(PgSessionQueryIdState *query_id);
static void PgSessionAdoptEarlyQueryIdState(PgSession *session);
static void PgSessionInitializeStorageGUCState(PgSessionStorageGUCState *storage_guc);
static void PgSessionAdoptEarlyStorageGUCState(PgSession *session);
static void PgSessionInitializeUserGUCState(PgSessionUserGUCState *user_guc);
static void PgSessionAdoptEarlyUserGUCState(PgSession *session);
static void PgSessionInitializeUserIdentityState(PgSessionUserIdentityState *user_identity);
static void PgSessionAdoptEarlyUserIdentityState(PgSession *session);
static void PgSessionInitializeCommandGUCState(PgSessionCommandGUCState *command_guc);
static void PgSessionAdoptEarlyCommandGUCState(PgSession *session);
static void PgSessionInitializeReplicationGUCState(PgSessionReplicationGUCState *replication_guc);
static void PgSessionAdoptEarlyReplicationGUCState(PgSession *session);
static void PgSessionInitializeGeneralGUCState(PgSessionGeneralGUCState *general_guc);
static void PgSessionAdoptEarlyGeneralGUCState(PgSession *session);
static void PgSessionInitializeAccessWalGUCState(PgSessionAccessWalGUCState *access_wal_guc);
static void PgSessionAdoptEarlyAccessWalGUCState(PgSession *session);
static void PgSessionInitializeJitGUCState(PgSessionJitGUCState *jit_guc);
static void PgSessionAdoptEarlyJitGUCState(PgSession *session);
static void PgSessionInitializeSortGUCState(PgSessionSortGUCState *sort_guc);
static void PgSessionAdoptEarlySortGUCState(PgSession *session);
static void PgSessionInitializeQueryMemoryState(PgSessionQueryMemoryState *query_memory);
static void PgSessionAdoptEarlyQueryMemoryState(PgSession *session);
static void PgSessionInitializePlannerCostState(PgSessionPlannerCostState *planner_cost);
static void PgSessionAdoptEarlyPlannerCostState(PgSession *session);
static void PgSessionInitializePlannerMethodState(PgSessionPlannerMethodState *planner_method);
static void PgSessionAdoptEarlyPlannerMethodState(PgSession *session);
static void PgSessionInitializePreparedStatementState(PgSessionPreparedStatementState *prepared_statement);
static void PgSessionAdoptEarlyPreparedStatementState(PgSession *session);
static void PgSessionInitializeOnCommitState(PgSessionOnCommitState *on_commit);
static void PgSessionAdoptEarlyOnCommitState(PgSession *session);
static void PgSessionInitializeSequenceState(PgSessionSequenceState *sequence);
static void PgSessionAdoptEarlySequenceState(PgSession *session);
static void PgSessionInitializeRegexState(PgSessionRegexState *regex);
static void PgSessionAdoptEarlyRegexState(PgSession *session);
static void PgSessionInitializeLargeObjectState(PgSessionLargeObjectState *large_object);
static void PgSessionAdoptEarlyLargeObjectState(PgSession *session);
static void PgSessionInitializeAsyncState(PgSessionAsyncState *async);
static void PgSessionAdoptEarlyAsyncState(PgSession *session);
static void PgSessionInitializeEncodingState(PgSessionEncodingState *encoding);
static void PgSessionEnsureEncodingStateInitialized(PgSessionEncodingState *encoding);
static void PgSessionAdoptEarlyEncodingState(PgSession *session);
static void PgSessionInitializeTempFileState(PgSessionTempFileState *temp_file);
static void PgSessionAdoptEarlyTempFileState(PgSession *session);
static void PgSessionInitializeRandomState(PgSessionRandomState *random);
static void PgSessionAdoptEarlyRandomState(PgSession *session);
static void PgSessionInitializeOptimizerState(PgSessionOptimizerState *optimizer);
static void PgSessionAdoptEarlyOptimizerState(PgSession *session);
static void PgSessionInitializePlanCacheState(PgSessionPlanCacheState *plan_cache);
static void PgSessionAdoptEarlyPlanCacheState(PgSession *session);
static void PgSessionInitializeNamespaceState(PgSessionNamespaceState *namespace_state);
static void PgSessionAdoptEarlyNamespaceState(PgSession *session);
static void PgSessionInitializeLocaleState(PgSessionLocaleState *locale);
static void PgSessionAdoptEarlyLocaleState(PgSession *session);
static void PgBackendResetCoreState(PgBackendCoreState *core);
static void PgBackendInitializeCommandState(PgBackendCommandState *command);
static void PgBackendAdoptEarlyCommandState(PgBackend *backend);
static void PgBackendInitializeLogState(PgBackendLogState *log_state);
static void PgBackendAdoptEarlyLogState(PgBackend *backend);
static void PgBackendInitializeExprInterpState(PgBackendExprInterpState *expr_interp);
static void PgBackendAdoptEarlyExprInterpState(PgBackend *backend);
static void PgSessionInitializeLoopState(PgSessionLoopState *loop_state);
static void PgSessionAdoptEarlyLoopState(PgSession *session);
static void PgBackendInitializeProcNumberState(PgBackend *backend);
static void PgBackendAdoptEarlyCoreState(PgBackend *backend);
static void PgBackendAdoptEarlyMyProc(PgBackend *backend);
static void PgBackendAdoptEarlyProcNumberState(PgBackend *backend);
static void PgBackendAdoptEarlyMyBEEntry(PgBackend *backend);
static void PgBackendAdoptEarlyMyBgworkerEntry(PgBackend *backend);
static void PgBackendAdoptEarlyAuxProcessResourceOwner(PgBackend *backend);
static void PgBackendInitializePgStatPendingState(PgBackendPgStatPendingState *pgstat_pending);
static void PgBackendAdoptEarlyPgStatPendingState(PgBackend *backend);
static void PgBackendInitializeActivityState(PgBackendActivityState *activity);
static void PgBackendAdoptEarlyActivityState(PgBackend *backend);
static void PgBackendInitializeMemoryManagerState(PgBackendMemoryManagerState *memory_manager);
static void PgBackendAdoptEarlyMemoryManagerState(PgBackend *backend);
static void PgBackendInitializeUtilityState(PgBackendUtilityState *utility);
static void PgBackendAdoptEarlyUtilityState(PgBackend *backend);
static void PgBackendInitializeParallelState(PgBackendParallelState *parallel);
static void PgBackendAdoptEarlyParallelState(PgBackend *backend);
static void PgBackendInitializeInstrumentationState(PgBackendInstrumentationState *instrumentation);
static void PgBackendAdoptEarlyInstrumentationState(PgBackend *backend);
static void PgBackendInitializeBufferState(PgBackendBufferState *buffers);
static void PgBackendAdoptEarlyBufferState(PgBackend *backend);
static void PgBackendInitializeStorageState(PgBackendStorageState *storage);
static void PgBackendAdoptEarlyStorageState(PgBackend *backend);
static void PgBackendInitializeLockState(PgBackendLockState *locks);
static void PgBackendAdoptEarlyLockState(PgBackend *backend);
static void PgBackendInitializeIPCState(PgBackendIPCState *ipc);
static void PgBackendAdoptEarlyIPCState(PgBackend *backend);
static void PgBackendEnsureWaitStateInitialized(PgBackendWaitState *wait_state);
static void PgBackendInitializeWaitState(PgBackendWaitState *wait_state);
static void PgBackendAdoptEarlyWaitState(PgBackend *backend);
static void PgBackendInitializeTransactionState(PgBackendTransactionState *transaction);
static void PgBackendAdoptEarlyTransactionState(PgBackend *backend);
static void PgBackendInitializeTimeoutState(PgBackendTimeoutState *timeout);
static void PgBackendAdoptEarlyTimeoutState(PgBackend *backend);
static void PgBackendInitializeWalSenderState(PgBackendWalSenderState *walsender);
static void PgBackendAdoptEarlyWalSenderState(PgBackend *backend);
static void PgBackendInitializeReplicationState(PgBackendReplicationState *replication);
static void PgBackendAdoptEarlyReplicationState(PgBackend *backend);
static void PgBackendInitializeLogicalReplicationState(PgBackendLogicalReplicationState *logical_replication);
static void PgBackendAdoptEarlyLogicalReplicationState(PgBackend *backend);
static void PgBackendInitializeXLogState(PgBackendXLogState *xlog);
static void PgBackendAdoptEarlyXLogState(PgBackend *backend);
static void PgBackendInitializeRecoveryState(PgBackendRecoveryState *recovery);
static void PgBackendAdoptEarlyRecoveryState(PgBackend *backend);
static void PgBackendInitializeMaintenanceWorkerState(PgBackendMaintenanceWorkerState *maintenance_worker);
static void PgBackendAdoptEarlyMaintenanceWorkerState(PgBackend *backend);
static void PgBackendInitializeAutovacuumState(PgBackendAutovacuumState *autovacuum);
static void PgBackendAdoptEarlyAutovacuumState(PgBackend *backend);
static void PgBackendInitializeRepackState(PgBackendRepackState *repack);
static void PgBackendAdoptEarlyRepackState(PgBackend *backend);
static void PgBackendInitializeAioState(PgBackendAioState *aio);
static void PgBackendAdoptEarlyAioState(PgBackend *backend);
static void PgBackendAdoptEarlyPendingInterrupts(PgBackend *backend);
static void PgBackendAdoptEarlyInterruptHoldoffs(PgBackend *backend);
static BackendType *PgCurrentBackendTypeRef(void);
static void PgExecutionAdoptEarlyDebugState(PgExecution *execution);
static void PgExecutionInitializeErrorState(PgExecutionErrorState *error);
static void PgExecutionAdoptEarlyErrorState(PgExecution *execution);
static void PgExecutionAdoptEarlyMemoryContexts(PgExecution *execution);
static void PgExecutionAdoptEarlyResourceOwners(PgExecution *execution);
static void PgExecutionInitializeSPIState(PgExecutionSPIState *spi);
static void PgExecutionAdoptEarlySPIState(PgExecution *execution);
static void PgExecutionAdoptEarlyPortalState(PgExecution *execution);
static void PgExecutionInitializeVacuumState(PgExecutionVacuumState *vacuum);
static void PgExecutionAdoptEarlyVacuumState(PgExecution *execution);
static void PgExecutionInitializeNodeIOState(PgExecutionNodeIOState *node_io);
static void PgExecutionAdoptEarlyNodeIOState(PgExecution *execution);
static void PgExecutionInitializeBaseBackupState(PgExecutionBaseBackupState *basebackup);
static void PgExecutionAdoptEarlyBaseBackupState(PgExecution *execution);
static void PgExecutionInitializeAnalyzeState(PgExecutionAnalyzeState *analyze);
static void PgExecutionAdoptEarlyAnalyzeState(PgExecution *execution);
static void PgExecutionInitializeExtensionState(PgExecutionExtensionState *extension);
static void PgExecutionAdoptEarlyExtensionState(PgExecution *execution);
static void PgExecutionInitializeMatViewState(PgExecutionMatViewState *matview);
static void PgExecutionAdoptEarlyMatViewState(PgExecution *execution);
static void PgExecutionInitializeSnapshotState(PgExecutionSnapshotState *snapshot);
static void PgExecutionAdoptEarlySnapshotState(PgExecution *execution);
static void PgExecutionInitializeComboCidState(PgExecutionComboCidState *combo_cid);
static void PgExecutionAdoptEarlyComboCidState(PgExecution *execution);
static void PgExecutionInitializeXLogInsertState(PgExecutionXLogInsertState *xloginsert);
static void PgExecutionAdoptEarlyXLogInsertState(PgExecution *execution);
static void PgExecutionInitializeXactState(PgExecutionXactState *xact);
static void PgExecutionAdoptEarlyXactState(PgExecution *execution);
static void PgExecutionInitializeTransactionCleanupState(PgExecutionTransactionCleanupState *transaction_cleanup);
static void PgExecutionAdoptEarlyTransactionCleanupState(PgExecution *execution);
static void PgExecutionInitializeReplicationScratchState(PgExecutionReplicationScratchState *replication_scratch);
static void PgExecutionAdoptEarlyReplicationScratchState(PgExecution *execution);
static void PgExecutionInitializeGUCErrorState(PgExecutionGUCErrorState *guc_error);
static void PgExecutionAdoptEarlyGUCErrorState(PgExecution *execution);
static void PgExecutionInitializeAsyncState(PgExecutionAsyncState *async);
static void PgExecutionAdoptEarlyAsyncState(PgExecution *execution);
static void PgExecutionInitializeCatalogState(PgExecutionCatalogState *catalog);
static void PgExecutionAdoptEarlyCatalogState(PgExecution *execution);
static void PgExecutionInitializeCatalogCacheState(PgExecutionCatalogCacheState *catalog_cache);
static void PgExecutionAdoptEarlyCatalogCacheState(PgExecution *execution);
static void PgExecutionInitializeRelMapState(PgExecutionRelMapState *relmap);
static void PgExecutionAdoptEarlyRelMapState(PgExecution *execution);
static void PgExecutionInitializeInvalidationState(PgExecutionInvalidationState
												   *invalidation);
static void PgExecutionAdoptEarlyInvalidationState(PgExecution *execution);
static void PgExecutionInitializeTwoPhaseRecordState(PgExecutionTwoPhaseRecordState
													 *two_phase_records);
static void PgExecutionAdoptEarlyTwoPhaseRecordState(PgExecution *execution);
static void PgExecutionInitializeTriggerState(PgExecutionTriggerState *trigger);
static void PgExecutionAdoptEarlyTriggerState(PgExecution *execution);
static void PgExecutionInitializeRegexState(PgExecutionRegexState *regex);
static void PgExecutionAdoptEarlyRegexState(PgExecution *execution);
static void PgExecutionInitializeValgrindState(PgExecutionValgrindState *valgrind);
static void PgExecutionAdoptEarlyValgrindState(PgExecution *execution);
static void PgExecutionInitializeSnapBuildState(PgExecutionSnapBuildState *snapbuild);
static void PgExecutionAdoptEarlySnapBuildState(PgExecution *execution);
static PgBackendCoreState *PgCurrentCoreState(void);
static PgSessionDatabaseState *PgCurrentSessionDatabaseState(void);
static PgSessionTablespaceState *PgCurrentSessionTablespaceState(void);
static PgSessionBinaryUpgradeState *PgCurrentSessionBinaryUpgradeState(void);
static PgSessionDateTimeState *PgCurrentSessionDateTimeState(void);
static PgSessionTextSearchState *PgCurrentSessionTextSearchState(void);
static PgSessionConnectionGUCState *PgCurrentSessionConnectionGUCState(void);
static PgSessionParserState *PgCurrentSessionParserState(void);
static PgSessionVacuumState *PgCurrentSessionVacuumState(void);
static PgSessionBufferIOState *PgCurrentSessionBufferIOState(void);
static PgSessionXactDefaultState *PgCurrentSessionXactDefaultState(void);
static PgSessionLockWaitState *PgCurrentSessionLockWaitState(void);
static PgSessionLoggingState *PgCurrentSessionLoggingState(void);
static PgSessionMiscGUCState *PgCurrentSessionMiscGUCState(void);
static PgSessionPgStatState *PgCurrentSessionPgStatState(void);
static PgSessionQueryIdState *PgCurrentSessionQueryIdState(void);
static PgSessionStorageGUCState *PgCurrentSessionStorageGUCState(void);
static PgSessionUserGUCState *PgCurrentSessionUserGUCState(void);
static PgSessionUserIdentityState *PgCurrentSessionUserIdentityState(void);
static PgSessionCommandGUCState *PgCurrentSessionCommandGUCState(void);
static PgSessionReplicationGUCState *PgCurrentSessionReplicationGUCState(void);
static PgSessionGeneralGUCState *PgCurrentSessionGeneralGUCState(void);
static PgSessionAccessWalGUCState *PgCurrentSessionAccessWalGUCState(void);
static PgSessionJitGUCState *PgCurrentSessionJitGUCState(void);
static PgSessionSortGUCState *PgCurrentSessionSortGUCState(void);
static PgSessionQueryMemoryState *PgCurrentSessionQueryMemoryState(void);
static PgSessionPlannerCostState *PgCurrentSessionPlannerCostState(void);
static PgSessionPlannerMethodState *PgCurrentSessionPlannerMethodState(void);
static PgSessionPreparedStatementState *PgCurrentSessionPreparedStatementState(void);
static PgSessionOnCommitState *PgCurrentSessionOnCommitState(void);
static PgSessionSequenceState *PgCurrentSessionSequenceState(void);
static PgSessionRegexState *PgCurrentSessionRegexState(void);
static PgSessionLargeObjectState *PgCurrentSessionLargeObjectState(void);
static PgSessionAsyncState *PgCurrentSessionAsyncState(void);
static PgSessionEncodingState *PgCurrentSessionEncodingState(void);
static PgSessionTempFileState *PgCurrentSessionTempFileState(void);
static PgSessionRandomState *PgCurrentSessionRandomState(void);
static PgSessionOptimizerState *PgCurrentSessionOptimizerState(void);
static PgSessionPlanCacheState *PgCurrentSessionPlanCacheState(void);
static PgSessionNamespaceState *PgCurrentSessionNamespaceState(void);
static PgSessionLocaleState *PgCurrentSessionLocaleState(void);
static PgExecutionErrorState *PgCurrentExecutionErrorState(void);
static PgExecutionMemoryContextState *PgCurrentExecutionMemoryContexts(void);
static PgExecutionResourceOwnerState *PgCurrentExecutionResourceOwners(void);
static PgExecutionSPIState *PgCurrentExecutionSPIState(void);
static PgExecutionPortalState *PgCurrentExecutionPortalState(void);
static PgExecutionVacuumState *PgCurrentExecutionVacuumState(void);
static PgExecutionNodeIOState *PgCurrentExecutionNodeIOState(void);
static PgExecutionBaseBackupState *PgCurrentExecutionBaseBackupState(void);
static PgExecutionAnalyzeState *PgCurrentExecutionAnalyzeState(void);
static PgExecutionExtensionState *PgCurrentExecutionExtensionState(void);
static PgExecutionMatViewState *PgCurrentExecutionMatViewState(void);
static PgExecutionSnapshotState *PgCurrentExecutionSnapshotState(void);
static PgExecutionComboCidState *PgCurrentExecutionComboCidState(void);
static PgExecutionXLogInsertState *PgCurrentExecutionXLogInsertState(void);
static PgExecutionXactState *PgCurrentExecutionXactState(void);
static PgExecutionTransactionCleanupState *PgCurrentExecutionTransactionCleanupState(void);
static PgExecutionReplicationScratchState *PgCurrentExecutionReplicationScratchState(void);
static PgExecutionGUCErrorState *PgCurrentExecutionGUCErrorState(void);
static PgExecutionAsyncState *PgCurrentExecutionAsyncState(void);
static PgExecutionCatalogState *PgCurrentExecutionCatalogState(void);
static PgExecutionCatalogCacheState *PgCurrentExecutionCatalogCacheState(void);
static PgExecutionRelMapState *PgCurrentExecutionRelMapState(void);
static PgExecutionInvalidationState *PgCurrentExecutionInvalidationState(void);
static PgExecutionTwoPhaseRecordState *PgCurrentExecutionTwoPhaseRecordState(void);
static PgExecutionRegexState *PgCurrentExecutionRegexState(void);
static PgExecutionValgrindState *PgCurrentExecutionValgrindState(void);
static PgExecutionSnapBuildState *PgCurrentExecutionSnapBuildState(void);
static PgBackendPgStatPendingState *PgCurrentBackendPgStatPendingState(void);
static PgBackendInstrumentationState *PgCurrentBackendInstrumentationState(void);
static PgBackendBufferState *PgCurrentBackendBufferState(void);
static PgBackendTransactionState *PgCurrentBackendTransactionState(void);
static PgBackendPendingInterruptState *PgCurrentPendingInterrupts(void);
static PgBackendInterruptHoldoffState *PgCurrentInterruptHoldoffs(void);

static void
PgBackendInitializeIdCounter(void)
{
	if (backend_id_counter_initialized)
		return;

	pg_atomic_init_u64(&next_backend_id, 0);
	backend_id_counter_initialized = true;
}

static PgBackendId
PgBackendAssignId(void)
{
	PgBackendInitializeIdCounter();

	return pg_atomic_add_fetch_u64(&next_backend_id, 1);
}

static void
PgRuntimeInitializeServerGUCState(PgRuntimeServerGUCState *server_guc)
{
	Assert(server_guc != NULL);

	server_guc->initialized = true;
	server_guc->cluster_name_value = guc_strdup(FATAL, "");
	server_guc->config_file_name = NULL;
	server_guc->hba_file_name = NULL;
	server_guc->ident_file_name = NULL;
	server_guc->hosts_file_name = NULL;
	server_guc->external_pid_file_value = NULL;
}

static void
PgRuntimeAdoptEarlyServerGUCState(PgRuntime *runtime)
{
	Assert(runtime != NULL);

	if (!early_runtime_server_guc.initialized)
		PgRuntimeInitializeServerGUCState(&early_runtime_server_guc);

	runtime->server_guc = early_runtime_server_guc;
	PgRuntimeInitializeServerGUCState(&early_runtime_server_guc);
}

static void
PgConnectionAdoptEarlyIdentity(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->identity = early_connection_identity;
	MemSet(&early_connection_identity, 0, sizeof(early_connection_identity));
}

static void
PgConnectionAdoptEarlySocketIO(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->socket_io = early_connection_socket_io;
	MemSet(&early_connection_socket_io, 0, sizeof(early_connection_socket_io));
}

static void
PgConnectionAdoptEarlyProtocolState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->protocol = early_connection_protocol;
	MemSet(&early_connection_protocol, 0, sizeof(early_connection_protocol));
}

static void
PgConnectionInitializeOutputState(PgConnectionOutputState *output)
{
	Assert(output != NULL);

	MemSet(output, 0, sizeof(*output));
	output->where_to_send_output = DestDebug;
}

static void
PgConnectionAdoptEarlyOutputState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->output = early_connection_output;
	PgConnectionInitializeOutputState(&early_connection_output);
}

static void
PgConnectionAdoptEarlyInterruptState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->interrupts = early_connection_interrupts;
	MemSet(&early_connection_interrupts, 0, sizeof(early_connection_interrupts));
}

static void
PgConnectionInitializeStartupState(PgConnectionStartupState *startup)
{
	Assert(startup != NULL);

	MemSet(startup, 0, sizeof(*startup));
	startup->timing.ready_for_use = TIMESTAMP_MINUS_INFINITY;
}

static void
PgConnectionAdoptEarlyStartupState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->startup = early_connection_startup;
	PgConnectionInitializeStartupState(&early_connection_startup);
}

static void
PgConnectionAdoptEarlyClientConnectionInfo(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->client_connection_info = early_client_connection_info;
	MemSet(&early_client_connection_info, 0, sizeof(early_client_connection_info));
}

static void
PgConnectionAdoptEarlySecurityState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->security = early_connection_security;
	MemSet(&early_connection_security, 0, sizeof(early_connection_security));
}

void
PgConnectionResetClosedState(PgConnection *connection)
{
	PgConnectionSecurityState *security;

	Assert(connection != NULL);

	connection->identity.port = NULL;
	MemSet(connection->identity.cancel_key, 0,
		   sizeof(connection->identity.cancel_key));
	connection->identity.cancel_key_length = 0;

	/*
	 * socket_close() releases the palloc-backed send buffer and wait set.
	 * This reset makes the retained logical connection object stop pointing
	 * at resources that no longer exist.
	 */
	MemSet(&connection->socket_io, 0, sizeof(connection->socket_io));
	connection->protocol.comm_methods = NULL;
	connection->protocol.fe_be_wait_set = NULL;
	connection->protocol.frontend_protocol = 0;

	connection->startup.client_auth_in_progress = false;
	connection->startup.client_socket = NULL;

	/*
	 * GSSAPI connection buffers are malloc-backed in be-secure-gssapi.c.
	 * PAM fields are borrowed authentication-time pointers, so reset them but
	 * do not free them here.
	 */
	security = &connection->security;
	free(security->gss_send_buffer);
	free(security->gss_recv_buffer);
	free(security->gss_result_buffer);
	MemSet(security, 0, sizeof(*security));
}

void
PgConnectionAdoptEarlyState(PgConnection *connection,
							struct Port *preserved_port)
{
	Assert(connection != NULL);

	PgConnectionAdoptEarlyIdentity(connection);
	if (preserved_port != NULL)
		connection->identity.port = preserved_port;
	PgConnectionAdoptEarlySocketIO(connection);
	PgConnectionAdoptEarlyProtocolState(connection);
	PgConnectionAdoptEarlyOutputState(connection);
	PgConnectionAdoptEarlyInterruptState(connection);
	PgConnectionAdoptEarlyStartupState(connection);
	PgConnectionAdoptEarlyClientConnectionInfo(connection);
	PgConnectionAdoptEarlySecurityState(connection);
}

static void
PgSessionAdoptEarlyDatabaseState(PgSession *session)
{
	Assert(session != NULL);

	session->database = early_session_database;
	MemSet(&early_session_database, 0, sizeof(early_session_database));
}

static void
PgSessionInitializeTablespaceState(PgSessionTablespaceState *tablespace)
{
	Assert(tablespace != NULL);

	tablespace->initialized = true;
	tablespace->default_tablespace_name = NULL;
	tablespace->temp_tablespaces_names = NULL;
	tablespace->allow_in_place_tablespaces_value = false;
	tablespace->binary_upgrade_next_pg_tablespace_oid_value = InvalidOid;
}

static void
PgSessionAdoptEarlyTablespaceState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_tablespace.initialized)
		PgSessionInitializeTablespaceState(&early_session_tablespace);

	session->tablespace = early_session_tablespace;
	PgSessionInitializeTablespaceState(&early_session_tablespace);
}

static void
PgSessionInitializeBinaryUpgradeState(PgSessionBinaryUpgradeState *binary_upgrade)
{
	Assert(binary_upgrade != NULL);

	binary_upgrade->initialized = true;
	binary_upgrade->binary_upgrade_next_pg_type_oid_value = InvalidOid;
	binary_upgrade->binary_upgrade_next_array_pg_type_oid_value = InvalidOid;
	binary_upgrade->binary_upgrade_next_mrng_pg_type_oid_value = InvalidOid;
	binary_upgrade->binary_upgrade_next_mrng_array_pg_type_oid_value = InvalidOid;
	binary_upgrade->binary_upgrade_next_heap_pg_class_oid_value = InvalidOid;
	binary_upgrade->binary_upgrade_next_heap_pg_class_relfilenumber_value =
		InvalidRelFileNumber;
	binary_upgrade->binary_upgrade_next_index_pg_class_oid_value = InvalidOid;
	binary_upgrade->binary_upgrade_next_index_pg_class_relfilenumber_value =
		InvalidRelFileNumber;
	binary_upgrade->binary_upgrade_next_toast_pg_class_oid_value = InvalidOid;
	binary_upgrade->binary_upgrade_next_toast_pg_class_relfilenumber_value =
		InvalidRelFileNumber;
	binary_upgrade->binary_upgrade_next_pg_enum_oid_value = InvalidOid;
	binary_upgrade->binary_upgrade_next_pg_authid_oid_value = InvalidOid;
	binary_upgrade->binary_upgrade_record_init_privs_value = false;
}

static void
PgSessionAdoptEarlyBinaryUpgradeState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_binary_upgrade.initialized)
		PgSessionInitializeBinaryUpgradeState(&early_session_binary_upgrade);

	session->binary_upgrade = early_session_binary_upgrade;
	PgSessionInitializeBinaryUpgradeState(&early_session_binary_upgrade);
}

static void
PgSessionInitializeDateTimeState(PgSessionDateTimeState *datetime)
{
	Assert(datetime != NULL);

	datetime->initialized = true;
	datetime->date_style = USE_ISO_DATES;
	datetime->date_order = DATEORDER_MDY;
	datetime->interval_style = INTSTYLE_POSTGRES;
	datetime->timezone_string_value = guc_strdup(FATAL, "GMT");
	datetime->log_timezone_string_value = guc_strdup(FATAL, "GMT");
	datetime->session_timezone_value = pg_tzset("GMT");
	datetime->log_timezone_value = datetime->session_timezone_value;
}

static void
PgSessionAdoptEarlyDateTimeState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_datetime.initialized)
		PgSessionInitializeDateTimeState(&early_session_datetime);

	session->datetime = early_session_datetime;
	PgSessionInitializeDateTimeState(&early_session_datetime);
}

static void
PgSessionInitializeTextSearchState(PgSessionTextSearchState *text_search)
{
	Assert(text_search != NULL);

	text_search->initialized = true;
	text_search->current_config_value = guc_strdup(FATAL, "pg_catalog.simple");
	text_search->current_config_cache = InvalidOid;
}

static void
PgSessionAdoptEarlyTextSearchState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_text_search.initialized)
		PgSessionInitializeTextSearchState(&early_session_text_search);

	session->text_search = early_session_text_search;
	PgSessionInitializeTextSearchState(&early_session_text_search);
}

static void
PgSessionInitializeConnectionGUCState(PgSessionConnectionGUCState *connection_guc)
{
	Assert(connection_guc != NULL);

	connection_guc->initialized = true;
	connection_guc->application_name_value = guc_strdup(FATAL, "");
	connection_guc->tcp_keepalives_idle_value = 0;
	connection_guc->tcp_keepalives_interval_value = 0;
	connection_guc->tcp_keepalives_count_value = 0;
	connection_guc->tcp_user_timeout_value = 0;
	connection_guc->log_disconnections_value = false;
	connection_guc->log_statement_value = 0;
	connection_guc->post_auth_delay_seconds = 0;
	connection_guc->restrict_nonsystem_relation_kind_string_value =
		guc_strdup(FATAL, "");
	connection_guc->restrict_nonsystem_relation_kind_value = 0;
}

static void
PgSessionAdoptEarlyConnectionGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_connection_guc.initialized)
		PgSessionInitializeConnectionGUCState(&early_session_connection_guc);

	session->connection_guc = early_session_connection_guc;
	PgSessionInitializeConnectionGUCState(&early_session_connection_guc);
}

static void
PgSessionInitializeParserState(PgSessionParserState *parser)
{
	Assert(parser != NULL);

	parser->initialized = true;
	parser->transform_null_equals_value = false;
	parser->backslash_quote_value = BACKSLASH_QUOTE_SAFE_ENCODING;
	parser->operator_lookup_cache = NULL;
}

static void
PgSessionAdoptEarlyParserState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_parser.initialized)
		PgSessionInitializeParserState(&early_session_parser);

	session->parser = early_session_parser;
	PgSessionInitializeParserState(&early_session_parser);
}

static void
PgSessionInitializeVacuumState(PgSessionVacuumState *vacuum)
{
	Assert(vacuum != NULL);

	vacuum->initialized = true;
	vacuum->vacuum_buffer_usage_limit_kb = 2048;
	vacuum->vacuum_cost_page_hit_value = 1;
	vacuum->vacuum_cost_page_miss_value = 2;
	vacuum->vacuum_cost_page_dirty_value = 20;
	vacuum->vacuum_cost_limit_value = 200;
	vacuum->vacuum_cost_delay_ms = 0;
	vacuum->default_statistics_target_value = 100;
	vacuum->vacuum_freeze_min_age_value = 50000000;
	vacuum->vacuum_freeze_table_age_value = 150000000;
	vacuum->vacuum_multixact_freeze_min_age_value = 5000000;
	vacuum->vacuum_multixact_freeze_table_age_value = 150000000;
	vacuum->vacuum_failsafe_age_value = 1600000000;
	vacuum->vacuum_multixact_failsafe_age_value = 1600000000;
	vacuum->track_cost_delay_timing_value = false;
	vacuum->vacuum_truncate_value = true;
	vacuum->vacuum_max_eager_freeze_failure_rate_value = 0.03;
	vacuum->local_vacuum_cost_delay_ms = 0;
	vacuum->local_vacuum_cost_limit_value = 200;
}

static void
PgSessionAdoptEarlyVacuumState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_vacuum.initialized)
		PgSessionInitializeVacuumState(&early_session_vacuum);

	session->vacuum = early_session_vacuum;
	PgSessionInitializeVacuumState(&early_session_vacuum);
}

static void
PgSessionInitializeBufferIOState(PgSessionBufferIOState *buffer_io)
{
	Assert(buffer_io != NULL);

	buffer_io->initialized = true;
	buffer_io->zero_damaged_pages_value = false;
	buffer_io->track_io_timing_value = false;
	buffer_io->effective_io_concurrency_value = DEFAULT_EFFECTIVE_IO_CONCURRENCY;
	buffer_io->maintenance_io_concurrency_value = DEFAULT_MAINTENANCE_IO_CONCURRENCY;
	buffer_io->io_combine_limit_value = DEFAULT_IO_COMBINE_LIMIT;
	buffer_io->io_combine_limit_guc_value = DEFAULT_IO_COMBINE_LIMIT;
	buffer_io->backend_flush_after_value = DEFAULT_BACKEND_FLUSH_AFTER;
}

static void
PgSessionAdoptEarlyBufferIOState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_buffer_io.initialized)
		PgSessionInitializeBufferIOState(&early_session_buffer_io);

	session->buffer_io = early_session_buffer_io;
	PgSessionInitializeBufferIOState(&early_session_buffer_io);
}

static void
PgSessionInitializeXactDefaultState(PgSessionXactDefaultState *xact_defaults)
{
	Assert(xact_defaults != NULL);

	xact_defaults->initialized = true;
	xact_defaults->default_xact_iso_level = XACT_READ_COMMITTED;
	xact_defaults->default_xact_read_only = false;
	xact_defaults->default_xact_deferrable = false;
	xact_defaults->synchronous_commit_value = SYNCHRONOUS_COMMIT_ON;
}

static void
PgSessionAdoptEarlyXactDefaultState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_xact_defaults.initialized)
		PgSessionInitializeXactDefaultState(&early_session_xact_defaults);

	session->xact_defaults = early_session_xact_defaults;
	PgSessionInitializeXactDefaultState(&early_session_xact_defaults);
}

static void
PgSessionInitializeLockWaitState(PgSessionLockWaitState *lock_wait)
{
	Assert(lock_wait != NULL);

	lock_wait->initialized = true;
	lock_wait->deadlock_timeout_ms = 1000;
	lock_wait->statement_timeout_ms = 0;
	lock_wait->lock_timeout_ms = 0;
	lock_wait->idle_in_transaction_session_timeout_ms = 0;
	lock_wait->transaction_timeout_ms = 0;
	lock_wait->idle_session_timeout_ms = 0;
	lock_wait->log_lock_waits_value = true;
	lock_wait->log_lock_failures_value = false;
	lock_wait->trace_lock_oidmin_value = FirstNormalObjectId;
	lock_wait->trace_locks_value = false;
	lock_wait->trace_userlocks_value = false;
	lock_wait->trace_lock_table_value = 0;
	lock_wait->debug_deadlocks_value = false;
	lock_wait->trace_lwlocks_value = false;
}

static void
PgSessionAdoptEarlyLockWaitState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_lock_wait.initialized)
		PgSessionInitializeLockWaitState(&early_session_lock_wait);

	session->lock_wait = early_session_lock_wait;
	PgSessionInitializeLockWaitState(&early_session_lock_wait);
}

static void
PgSessionInitializeLoggingState(PgSessionLoggingState *logging)
{
	Assert(logging != NULL);

	logging->initialized = true;
	logging->debug_print_plan_value = false;
	logging->debug_print_parse_value = false;
	logging->debug_print_raw_parse_value = false;
	logging->debug_print_rewritten_value = false;
	logging->debug_pretty_print_value = true;
#ifdef DEBUG_NODE_TESTS_ENABLED
	logging->debug_copy_parse_plan_trees_value = DEFAULT_DEBUG_COPY_PARSE_PLAN_TREES;
	logging->debug_write_read_parse_plan_trees_value = DEFAULT_DEBUG_WRITE_READ_PARSE_PLAN_TREES;
	logging->debug_raw_expression_coverage_test_value = DEFAULT_DEBUG_RAW_EXPRESSION_COVERAGE_TEST;
#endif
	logging->log_parser_stats_value = false;
	logging->log_planner_stats_value = false;
	logging->log_executor_stats_value = false;
	logging->log_statement_stats_value = false;
	logging->log_btree_build_stats_value = false;
	logging->event_source_value = NULL;
	logging->log_duration_value = false;
	logging->log_error_verbosity_value = PGERROR_DEFAULT;
	logging->log_parameter_max_length_value = -1;
	logging->log_parameter_max_length_on_error_value = 0;
	logging->log_min_error_statement_value = ERROR;
	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
		logging->log_min_messages_values[i] = WARNING;
	logging->log_min_messages_string_value = NULL;
	logging->client_min_messages_value = NOTICE;
	logging->log_min_duration_sample_value = -1;
	logging->log_min_duration_statement_value = -1;
	logging->log_temp_files_value = -1;
	logging->log_statement_sample_rate_value = 1.0;
	logging->log_xact_sample_rate_value = 0;
	logging->backtrace_functions_value = NULL;
	logging->backtrace_function_list_value = NULL;
}

static void
PgSessionAdoptEarlyLoggingState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_logging.initialized)
		PgSessionInitializeLoggingState(&early_session_logging);

	session->logging = early_session_logging;
	PgSessionInitializeLoggingState(&early_session_logging);
}

static void
PgSessionInitializeMiscGUCState(PgSessionMiscGUCState *misc_guc)
{
	Assert(misc_guc != NULL);

	misc_guc->initialized = true;
	misc_guc->allow_system_table_mods_value = false;
	misc_guc->max_stack_depth_kb = 100;
	misc_guc->max_stack_depth_bytes = 100 * (ssize_t) 1024;
	misc_guc->session_preload_libraries_value = NULL;
	misc_guc->local_preload_libraries_value = NULL;
	misc_guc->dynamic_library_path_value = NULL;
	misc_guc->extension_control_path_value = "$system";
}

static void
PgSessionAdoptEarlyMiscGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_misc_guc.initialized)
		PgSessionInitializeMiscGUCState(&early_session_misc_guc);

	session->misc_guc = early_session_misc_guc;
	PgSessionInitializeMiscGUCState(&early_session_misc_guc);
}

static void
PgSessionInitializePgStatState(PgSessionPgStatState *pgstat)
{
	Assert(pgstat != NULL);

	pgstat->initialized = true;
	pgstat->track_counts = true;
	pgstat->track_functions = TRACK_FUNC_OFF;
	pgstat->fetch_consistency = PGSTAT_FETCH_CONSISTENCY_CACHE;
	pgstat->track_activities = true;
	pgstat->session_end_cause = DISCONNECT_NORMAL;
	pgstat->last_session_report_time = 0;
}

static void
PgSessionAdoptEarlyPgStatState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_pgstat.initialized)
		PgSessionInitializePgStatState(&early_session_pgstat);

	session->pgstat = early_session_pgstat;
	PgSessionInitializePgStatState(&early_session_pgstat);
}

static void
PgSessionInitializeQueryIdState(PgSessionQueryIdState *query_id)
{
	Assert(query_id != NULL);

	query_id->initialized = true;
	query_id->compute_query_id_value = COMPUTE_QUERY_ID_AUTO;
	query_id->query_id_enabled_value = false;
}

static void
PgSessionAdoptEarlyQueryIdState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_query_id.initialized)
		PgSessionInitializeQueryIdState(&early_session_query_id);

	session->query_id = early_session_query_id;
	PgSessionInitializeQueryIdState(&early_session_query_id);
}

static void
PgSessionInitializeStorageGUCState(PgSessionStorageGUCState *storage_guc)
{
	Assert(storage_guc != NULL);

	storage_guc->initialized = true;
	storage_guc->ignore_checksum_failure_value = false;
	storage_guc->file_copy_method_value = FILE_COPY_METHOD_COPY;
}

static void
PgSessionAdoptEarlyStorageGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_storage_guc.initialized)
		PgSessionInitializeStorageGUCState(&early_session_storage_guc);

	session->storage_guc = early_session_storage_guc;
	PgSessionInitializeStorageGUCState(&early_session_storage_guc);
}

static void
PgSessionInitializeUserGUCState(PgSessionUserGUCState *user_guc)
{
	Assert(user_guc != NULL);

	user_guc->initialized = true;
	user_guc->password_encryption_value = PASSWORD_TYPE_SCRAM_SHA_256;
	user_guc->createrole_self_grant_value = "";
	user_guc->createrole_self_grant_enabled = false;
	user_guc->createrole_self_grant_options_specified = 0;
	user_guc->createrole_self_grant_options_admin = false;
	user_guc->createrole_self_grant_options_inherit = false;
	user_guc->createrole_self_grant_options_set = false;
}

static void
PgSessionAdoptEarlyUserGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_user_guc.initialized)
		PgSessionInitializeUserGUCState(&early_session_user_guc);

	session->user_guc = early_session_user_guc;
	PgSessionInitializeUserGUCState(&early_session_user_guc);
}

static void
PgSessionInitializeUserIdentityState(PgSessionUserIdentityState *user_identity)
{
	Assert(user_identity != NULL);

	user_identity->authenticated_user_id = InvalidOid;
	user_identity->session_user_id = InvalidOid;
	user_identity->outer_user_id = InvalidOid;
	user_identity->current_user_id = InvalidOid;
	user_identity->system_user = NULL;
	user_identity->session_user_is_superuser = false;
	user_identity->security_restriction_context = 0;
	user_identity->set_role_is_active = false;
	user_identity->initialized = true;
}

static void
PgSessionAdoptEarlyUserIdentityState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_user_identity.initialized)
		PgSessionInitializeUserIdentityState(&early_session_user_identity);

	session->user_identity = early_session_user_identity;
	PgSessionInitializeUserIdentityState(&early_session_user_identity);
}

static void
PgSessionInitializeCommandGUCState(PgSessionCommandGUCState *command_guc)
{
	Assert(command_guc != NULL);

	command_guc->initialized = true;
	command_guc->session_replication_role_value =
		SESSION_REPLICATION_ROLE_ORIGIN;
	command_guc->event_triggers_value = true;
	command_guc->trace_notify_value = false;
}

static void
PgSessionAdoptEarlyCommandGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_command_guc.initialized)
		PgSessionInitializeCommandGUCState(&early_session_command_guc);

	session->command_guc = early_session_command_guc;
	PgSessionInitializeCommandGUCState(&early_session_command_guc);
}

static void
PgSessionInitializeReplicationGUCState(PgSessionReplicationGUCState *replication_guc)
{
	Assert(replication_guc != NULL);

	replication_guc->initialized = true;
	replication_guc->wal_sender_timeout_ms = 60 * 1000;
	replication_guc->wal_sender_shutdown_timeout_ms = -1;
	replication_guc->log_replication_commands_value = false;
	replication_guc->wal_receiver_timeout_ms = 60 * 1000;
	replication_guc->logical_decoding_work_mem_kb = 65536;
	replication_guc->debug_logical_replication_streaming_value =
		DEBUG_LOGICAL_REP_STREAMING_BUFFERED;
}

static void
PgSessionAdoptEarlyReplicationGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_replication_guc.initialized)
		PgSessionInitializeReplicationGUCState(&early_session_replication_guc);

	session->replication_guc = early_session_replication_guc;
	PgSessionInitializeReplicationGUCState(&early_session_replication_guc);
}

static void
PgSessionInitializeGeneralGUCState(PgSessionGeneralGUCState *general_guc)
{
	Assert(general_guc != NULL);

	general_guc->initialized = true;
	general_guc->allow_alter_system_value = true;
	general_guc->row_security_value = true;
	general_guc->check_function_bodies_value = true;
	general_guc->current_role_is_superuser_value = false;
	general_guc->temp_file_limit_kb = -1;
	general_guc->num_temp_buffers_blocks = 1024;
	general_guc->role_string_value = "none";
	general_guc->lo_compat_privileges_value = false;
	general_guc->extra_float_digits_value = 1;
	general_guc->array_nulls_value = true;
	general_guc->bytea_output_value = BYTEA_OUTPUT_HEX;
	general_guc->xmlbinary_value = XMLBINARY_BASE64;
	general_guc->xmloption_value = XMLOPTION_CONTENT;
	general_guc->quote_all_identifiers_value = false;
	general_guc->plan_cache_mode_value = PLAN_CACHE_MODE_AUTO;
	general_guc->gin_fuzzy_search_limit_value = 0;
	general_guc->gin_pending_list_limit_value = 0;
}

static void
PgSessionAdoptEarlyGeneralGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_general_guc.initialized)
		PgSessionInitializeGeneralGUCState(&early_session_general_guc);

	session->general_guc = early_session_general_guc;
	PgSessionInitializeGeneralGUCState(&early_session_general_guc);
}

static void
PgSessionInitializeAccessWalGUCState(PgSessionAccessWalGUCState *access_wal_guc)
{
	Assert(access_wal_guc != NULL);

	access_wal_guc->initialized = true;
	access_wal_guc->default_table_access_method_value =
		DEFAULT_TABLE_ACCESS_METHOD;
	access_wal_guc->synchronize_seqscans_value = true;
	access_wal_guc->default_toast_compression_value =
		DEFAULT_TOAST_COMPRESSION;
	access_wal_guc->wal_compression_value = WAL_COMPRESSION_NONE;
	access_wal_guc->wal_init_zero_value = true;
	access_wal_guc->wal_recycle_value = true;
	access_wal_guc->wal_consistency_checking_string_value = NULL;
	access_wal_guc->wal_consistency_checking_value = NULL;
	access_wal_guc->commit_delay_us = 0;
	access_wal_guc->commit_siblings_value = 5;
	access_wal_guc->track_wal_io_timing_value = false;
	access_wal_guc->wal_skip_threshold_kb = 2048;
#ifdef WAL_DEBUG
	access_wal_guc->xlog_debug_value = false;
#endif
#ifdef TRACE_SYNCSCAN
	access_wal_guc->trace_syncscan_value = false;
#endif
}

static void
PgSessionAdoptEarlyAccessWalGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_access_wal_guc.initialized)
		PgSessionInitializeAccessWalGUCState(&early_session_access_wal_guc);

	session->access_wal_guc = early_session_access_wal_guc;
	PgSessionInitializeAccessWalGUCState(&early_session_access_wal_guc);
}

static void
PgSessionInitializeJitGUCState(PgSessionJitGUCState *jit_guc)
{
	Assert(jit_guc != NULL);

	jit_guc->initialized = true;
	jit_guc->jit_enabled_value = false;
	jit_guc->jit_provider_value = "llvmjit";
	jit_guc->jit_debugging_support_value = false;
	jit_guc->jit_dump_bitcode_value = false;
	jit_guc->jit_expressions_value = true;
	jit_guc->jit_profiling_support_value = false;
	jit_guc->jit_tuple_deforming_value = true;
	jit_guc->jit_above_cost_value = 100000;
	jit_guc->jit_inline_above_cost_value = 500000;
	jit_guc->jit_optimize_above_cost_value = 500000;
}

static void
PgSessionAdoptEarlyJitGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_jit_guc.initialized)
		PgSessionInitializeJitGUCState(&early_session_jit_guc);

	session->jit_guc = early_session_jit_guc;
	PgSessionInitializeJitGUCState(&early_session_jit_guc);
}

static void
PgSessionInitializeSortGUCState(PgSessionSortGUCState *sort_guc)
{
	Assert(sort_guc != NULL);

	sort_guc->initialized = true;
	sort_guc->trace_sort_value = false;
#ifdef DEBUG_BOUNDED_SORT
	sort_guc->optimize_bounded_sort_value = true;
#endif
}

static void
PgSessionAdoptEarlySortGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_sort_guc.initialized)
		PgSessionInitializeSortGUCState(&early_session_sort_guc);

	session->sort_guc = early_session_sort_guc;
	PgSessionInitializeSortGUCState(&early_session_sort_guc);
}

static void
PgSessionInitializeQueryMemoryState(PgSessionQueryMemoryState *query_memory)
{
	Assert(query_memory != NULL);

	query_memory->initialized = true;
	query_memory->work_mem_kb = 4096;
	query_memory->hash_mem_multiplier_value = 2.0;
	query_memory->maintenance_work_mem_kb = 65536;
	query_memory->max_parallel_maintenance_workers_value = 2;
}

static void
PgSessionAdoptEarlyQueryMemoryState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_query_memory.initialized)
		PgSessionInitializeQueryMemoryState(&early_session_query_memory);

	session->query_memory = early_session_query_memory;
	PgSessionInitializeQueryMemoryState(&early_session_query_memory);
}

static void
PgSessionInitializePlannerCostState(PgSessionPlannerCostState *planner_cost)
{
	Assert(planner_cost != NULL);

	planner_cost->initialized = true;
	planner_cost->seq_page_cost_value = DEFAULT_SEQ_PAGE_COST;
	planner_cost->random_page_cost_value = DEFAULT_RANDOM_PAGE_COST;
	planner_cost->cpu_tuple_cost_value = DEFAULT_CPU_TUPLE_COST;
	planner_cost->cpu_index_tuple_cost_value = DEFAULT_CPU_INDEX_TUPLE_COST;
	planner_cost->cpu_operator_cost_value = DEFAULT_CPU_OPERATOR_COST;
	planner_cost->parallel_tuple_cost_value = DEFAULT_PARALLEL_TUPLE_COST;
	planner_cost->parallel_setup_cost_value = DEFAULT_PARALLEL_SETUP_COST;
	planner_cost->recursive_worktable_factor_value =
		DEFAULT_RECURSIVE_WORKTABLE_FACTOR;
	planner_cost->effective_cache_size_pages = DEFAULT_EFFECTIVE_CACHE_SIZE;
	planner_cost->disable_cost_value = 1.0e10;
	planner_cost->max_parallel_workers_per_gather_value = 2;
	planner_cost->debug_parallel_query_value = DEBUG_PARALLEL_OFF;
	planner_cost->parallel_leader_participation_value = true;
}

static void
PgSessionAdoptEarlyPlannerCostState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_planner_cost.initialized)
		PgSessionInitializePlannerCostState(&early_session_planner_cost);

	session->planner_cost = early_session_planner_cost;
	PgSessionInitializePlannerCostState(&early_session_planner_cost);
}

static void
PgSessionInitializePlannerMethodState(PgSessionPlannerMethodState *planner_method)
{
	Assert(planner_method != NULL);

	planner_method->initialized = true;
	planner_method->enable_seqscan_value = true;
	planner_method->enable_indexscan_value = true;
	planner_method->enable_indexonlyscan_value = true;
	planner_method->enable_bitmapscan_value = true;
	planner_method->enable_tidscan_value = true;
	planner_method->enable_sort_value = true;
	planner_method->enable_incremental_sort_value = true;
	planner_method->enable_hashagg_value = true;
	planner_method->enable_nestloop_value = true;
	planner_method->enable_material_value = true;
	planner_method->enable_memoize_value = true;
	planner_method->enable_mergejoin_value = true;
	planner_method->enable_hashjoin_value = true;
	planner_method->enable_gathermerge_value = true;
	planner_method->enable_partitionwise_join_value = false;
	planner_method->enable_partitionwise_aggregate_value = false;
	planner_method->enable_parallel_append_value = true;
	planner_method->enable_parallel_hash_value = true;
	planner_method->enable_partition_pruning_value = true;
	planner_method->enable_presorted_aggregate_value = true;
	planner_method->enable_async_append_value = true;
	planner_method->enable_distinct_reordering_value = true;
	planner_method->enable_geqo_value = true;
	planner_method->enable_eager_aggregate_value = true;
	planner_method->enable_group_by_reordering_value = true;
	planner_method->enable_self_join_elimination_value = true;
	planner_method->cursor_tuple_fraction_value = DEFAULT_CURSOR_TUPLE_FRACTION;
	planner_method->constraint_exclusion_value =
		CONSTRAINT_EXCLUSION_PARTITION;
	planner_method->geqo_threshold_value = 12;
	planner_method->Geqo_effort_value = DEFAULT_GEQO_EFFORT;
	planner_method->Geqo_pool_size_value = 0;
	planner_method->Geqo_generations_value = 0;
	planner_method->Geqo_selection_bias_value =
		DEFAULT_GEQO_SELECTION_BIAS;
	planner_method->Geqo_seed_value = 0.0;
	planner_method->Geqo_planner_extension_id_value = -1;
	planner_method->min_eager_agg_group_size_value = 8.0;
	planner_method->min_parallel_table_scan_size_blocks =
		(8 * 1024 * 1024) / BLCKSZ;
	planner_method->min_parallel_index_scan_size_blocks =
		(512 * 1024) / BLCKSZ;
	planner_method->from_collapse_limit_value = 8;
	planner_method->join_collapse_limit_value = 8;
}

static void
PgSessionAdoptEarlyPlannerMethodState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_planner_method.initialized)
		PgSessionInitializePlannerMethodState(&early_session_planner_method);

	session->planner_method = early_session_planner_method;
	PgSessionInitializePlannerMethodState(&early_session_planner_method);
}

static void
PgSessionInitializePreparedStatementState(PgSessionPreparedStatementState *prepared_statement)
{
	Assert(prepared_statement != NULL);

	prepared_statement->prepared_queries = NULL;
}

static void
PgSessionAdoptEarlyPreparedStatementState(PgSession *session)
{
	Assert(session != NULL);

	session->prepared_statement = early_session_prepared_statement;
	PgSessionInitializePreparedStatementState(&early_session_prepared_statement);
}

static void
PgSessionInitializeOnCommitState(PgSessionOnCommitState *on_commit)
{
	Assert(on_commit != NULL);

	on_commit->on_commits = NIL;
}

static void
PgSessionAdoptEarlyOnCommitState(PgSession *session)
{
	Assert(session != NULL);

	session->on_commit = early_session_on_commit;
	PgSessionInitializeOnCommitState(&early_session_on_commit);
}

static void
PgSessionInitializeSequenceState(PgSessionSequenceState *sequence)
{
	Assert(sequence != NULL);

	sequence->seqhashtab = NULL;
	sequence->last_used_seq = NULL;
}

static void
PgSessionAdoptEarlySequenceState(PgSession *session)
{
	Assert(session != NULL);

	session->sequence = early_session_sequence;
	PgSessionInitializeSequenceState(&early_session_sequence);
}

static void
PgSessionInitializeRegexState(PgSessionRegexState *regex)
{
	Assert(regex != NULL);

	regex->ctype_cache_list = NULL;
}

static void
PgSessionAdoptEarlyRegexState(PgSession *session)
{
	Assert(session != NULL);

	session->regex = early_session_regex;
	PgSessionInitializeRegexState(&early_session_regex);
}

static void
PgSessionInitializeLargeObjectState(PgSessionLargeObjectState *large_object)
{
	Assert(large_object != NULL);

	large_object->heap_relation = NULL;
	large_object->index_relation = NULL;
}

static void
PgSessionAdoptEarlyLargeObjectState(PgSession *session)
{
	Assert(session != NULL);

	session->large_object = early_session_large_object;
	PgSessionInitializeLargeObjectState(&early_session_large_object);
}

static void
PgSessionInitializeAsyncState(PgSessionAsyncState *async)
{
	Assert(async != NULL);

	async->local_channel_table = NULL;
	async->registered_listener = false;
}

static void
PgSessionAdoptEarlyAsyncState(PgSession *session)
{
	Assert(session != NULL);

	session->async = early_session_async;
	PgSessionInitializeAsyncState(&early_session_async);
}

static void
PgSessionInitializeEncodingState(PgSessionEncodingState *encoding)
{
	Assert(encoding != NULL);

	encoding->conv_proc_list = NIL;
	encoding->to_server_conv_proc = NULL;
	encoding->to_client_conv_proc = NULL;
	encoding->utf8_to_server_conv_proc = NULL;
	encoding->client_encoding = &pg_enc2name_tbl[PG_SQL_ASCII];
	encoding->database_encoding = &pg_enc2name_tbl[PG_SQL_ASCII];
	encoding->message_encoding = &pg_enc2name_tbl[PG_SQL_ASCII];
	encoding->backend_startup_complete = false;
	encoding->pending_client_encoding = PG_SQL_ASCII;
}

static void
PgSessionEnsureEncodingStateInitialized(PgSessionEncodingState *encoding)
{
	Assert(encoding != NULL);

	if (encoding->client_encoding == NULL)
		PgSessionInitializeEncodingState(encoding);
}

static void
PgSessionAdoptEarlyEncodingState(PgSession *session)
{
	Assert(session != NULL);

	PgSessionEnsureEncodingStateInitialized(&early_session_encoding);
	session->encoding = early_session_encoding;
	PgSessionInitializeEncodingState(&early_session_encoding);
}

static void
PgSessionInitializeTempFileState(PgSessionTempFileState *temp_file)
{
	Assert(temp_file != NULL);

	temp_file->initialized = true;
	temp_file->temporary_files_size = 0;
	temp_file->temp_file_counter = 0;
	temp_file->temp_table_spaces = NULL;
	temp_file->num_temp_table_spaces = -1;
	temp_file->next_temp_table_space = 0;
}

static void
PgSessionAdoptEarlyTempFileState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_temp_file.initialized)
		PgSessionInitializeTempFileState(&early_session_temp_file);

	session->temp_file = early_session_temp_file;
	PgSessionInitializeTempFileState(&early_session_temp_file);
}

static void
PgSessionInitializeRandomState(PgSessionRandomState *random)
{
	Assert(random != NULL);

	MemSet(&random->prng_state, 0, sizeof(random->prng_state));
	random->prng_seed_set = false;
	random->initialized = true;
}

static void
PgSessionAdoptEarlyRandomState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_random.initialized)
		PgSessionInitializeRandomState(&early_session_random);

	session->random = early_session_random;
	PgSessionInitializeRandomState(&early_session_random);
}

static void
PgSessionInitializeOptimizerState(PgSessionOptimizerState *optimizer)
{
	Assert(optimizer != NULL);

	optimizer->planner_extension_names = NULL;
	optimizer->planner_extension_names_assigned = 0;
	optimizer->planner_extension_names_allocated = 0;
	optimizer->opr_proof_cache_hash = NULL;
}

static void
PgSessionAdoptEarlyOptimizerState(PgSession *session)
{
	Assert(session != NULL);

	session->optimizer = early_session_optimizer;
	PgSessionInitializeOptimizerState(&early_session_optimizer);
}

static void
PgSessionInitializePlanCacheState(PgSessionPlanCacheState *plan_cache)
{
	Assert(plan_cache != NULL);

	dlist_init(&plan_cache->saved_plan_list);
	dlist_init(&plan_cache->cached_expression_list);
	plan_cache->initialized = true;
}

static void
PgSessionAdoptEarlyPlanCacheState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_plan_cache.initialized)
		PgSessionInitializePlanCacheState(&early_session_plan_cache);

	Assert(dlist_is_empty(&early_session_plan_cache.saved_plan_list));
	Assert(dlist_is_empty(&early_session_plan_cache.cached_expression_list));
	PgSessionInitializePlanCacheState(&session->plan_cache);
	PgSessionInitializePlanCacheState(&early_session_plan_cache);
}

static void
PgSessionInitializeNamespaceState(PgSessionNamespaceState *namespace_state)
{
	Assert(namespace_state != NULL);

	namespace_state->active_search_path = NIL;
	namespace_state->active_creation_namespace = InvalidOid;
	namespace_state->active_temp_creation_pending = false;
	namespace_state->active_path_generation = 1;
	namespace_state->base_search_path = NIL;
	namespace_state->base_creation_namespace = InvalidOid;
	namespace_state->base_temp_creation_pending = false;
	namespace_state->namespace_user = InvalidOid;
	namespace_state->base_search_path_valid = true;
	namespace_state->search_path_cache_valid = false;
	namespace_state->search_path_cache_context = NULL;
	namespace_state->my_temp_namespace = InvalidOid;
	namespace_state->my_temp_toast_namespace = InvalidOid;
	namespace_state->my_temp_namespace_subid = InvalidSubTransactionId;
	namespace_state->namespace_search_path_value = NULL;
	namespace_state->search_path_cache = NULL;
	namespace_state->last_search_path_cache_entry = NULL;
	namespace_state->initialized = true;
}

static void
PgSessionAdoptEarlyNamespaceState(PgSession *session)
{
	char	   *namespace_search_path_value;

	Assert(session != NULL);

	if (!early_session_namespace.initialized)
		PgSessionInitializeNamespaceState(&early_session_namespace);

	namespace_search_path_value = early_session_namespace.namespace_search_path_value;
	PgSessionInitializeNamespaceState(&session->namespace_state);
	session->namespace_state.namespace_search_path_value =
		namespace_search_path_value;
	PgSessionInitializeNamespaceState(&early_session_namespace);
}

static void
PgSessionInitializeLocaleState(PgSessionLocaleState *locale)
{
	Assert(locale != NULL);

	locale->locale_messages_value = NULL;
	locale->locale_monetary_value = NULL;
	locale->locale_numeric_value = NULL;
	locale->locale_time_value = NULL;
	locale->icu_validation_level_value = WARNING;
	memset(locale->localized_abbrev_days_values, 0,
		   sizeof(locale->localized_abbrev_days_values));
	memset(locale->localized_full_days_values, 0,
		   sizeof(locale->localized_full_days_values));
	memset(locale->localized_abbrev_months_values, 0,
		   sizeof(locale->localized_abbrev_months_values));
	memset(locale->localized_full_months_values, 0,
		   sizeof(locale->localized_full_months_values));
	locale->default_locale = NULL;
	locale->locale_conv_valid = false;
	locale->locale_time_valid = false;
	locale->current_locale_conv = NULL;
	locale->current_locale_conv_allocated = false;
	locale->collation_cache_context = NULL;
	locale->collation_cache = NULL;
	locale->last_collation_cache_oid = InvalidOid;
	locale->last_collation_cache_locale = NULL;
	locale->initialized = true;
}

static void
PgSessionAdoptEarlyLocaleState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_locale.initialized)
		PgSessionInitializeLocaleState(&early_session_locale);

	session->locale = early_session_locale;
	PgSessionInitializeLocaleState(&early_session_locale);
}

void
PgSessionAdoptEarlyState(PgSession *session)
{
	Assert(session != NULL);

	PgSessionAdoptEarlyLoopState(session);
	PgSessionAdoptEarlyDatabaseState(session);
	PgSessionAdoptEarlyTablespaceState(session);
	PgSessionAdoptEarlyBinaryUpgradeState(session);
	PgSessionAdoptEarlyDateTimeState(session);
	PgSessionAdoptEarlyTextSearchState(session);
	PgSessionAdoptEarlyConnectionGUCState(session);
	PgSessionAdoptEarlyParserState(session);
	PgSessionAdoptEarlyVacuumState(session);
	PgSessionAdoptEarlyBufferIOState(session);
	PgSessionAdoptEarlyXactDefaultState(session);
	PgSessionAdoptEarlyLockWaitState(session);
	PgSessionAdoptEarlyLoggingState(session);
	PgSessionAdoptEarlyMiscGUCState(session);
	PgSessionAdoptEarlyPgStatState(session);
	PgSessionAdoptEarlyQueryIdState(session);
	PgSessionAdoptEarlyStorageGUCState(session);
	PgSessionAdoptEarlyUserGUCState(session);
	PgSessionAdoptEarlyUserIdentityState(session);
	PgSessionAdoptEarlyCommandGUCState(session);
	PgSessionAdoptEarlyReplicationGUCState(session);
	PgSessionAdoptEarlyGeneralGUCState(session);
	PgSessionAdoptEarlyAccessWalGUCState(session);
	PgSessionAdoptEarlyJitGUCState(session);
	PgSessionAdoptEarlySortGUCState(session);
	PgSessionAdoptEarlyQueryMemoryState(session);
	PgSessionAdoptEarlyPlannerCostState(session);
	PgSessionAdoptEarlyPlannerMethodState(session);
	PgSessionAdoptEarlyPreparedStatementState(session);
	PgSessionAdoptEarlyOnCommitState(session);
	PgSessionAdoptEarlySequenceState(session);
	PgSessionAdoptEarlyRegexState(session);
	PgSessionAdoptEarlyLargeObjectState(session);
	PgSessionAdoptEarlyAsyncState(session);
	PgSessionAdoptEarlyEncodingState(session);
	PgSessionAdoptEarlyTempFileState(session);
	PgSessionAdoptEarlyRandomState(session);
	PgSessionAdoptEarlyOptimizerState(session);
	PgSessionAdoptEarlyPlanCacheState(session);
	PgSessionAdoptEarlyNamespaceState(session);
	PgSessionAdoptEarlyLocaleState(session);
}

static void
PgBackendResetCoreState(PgBackendCoreState *core)
{
	MemSet(core, 0, sizeof(*core));
	core->mode = InitProcessing;
}

static void
PgBackendInitializeProcNumberState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->my_proc_number = INVALID_PROC_NUMBER;
	backend->parallel_leader_proc_number = INVALID_PROC_NUMBER;
}

static void
PgBackendAdoptEarlyCoreState(PgBackend *backend)
{
	struct Latch *existing_interrupt_latch;

	Assert(backend != NULL);

	existing_interrupt_latch = backend->interrupt_latch;
	backend->core = early_backend_core;
	PgBackendResetCoreState(&early_backend_core);

	if (backend->core.latch == NULL)
		backend->core.latch = existing_interrupt_latch;
	else
		PgBackendSetInterruptLatch(backend, backend->core.latch);

	if (early_backend_type != B_INVALID)
	{
		backend->backend_type = early_backend_type;
		early_backend_type = B_INVALID;
	}
}

static void
PgBackendAdoptEarlyAuxProcessResourceOwner(PgBackend *backend)
{
	Assert(backend != NULL);

	if (early_aux_process_resource_owner != NULL)
	{
		backend->aux_process_resource_owner = early_aux_process_resource_owner;
		early_aux_process_resource_owner = NULL;
	}
}

static void
PgBackendAdoptEarlyMyProc(PgBackend *backend)
{
	Assert(backend != NULL);

	if (early_my_proc != NULL)
	{
		backend->my_proc = early_my_proc;
		early_my_proc = NULL;
	}
}

static void
PgBackendAdoptEarlyProcNumberState(PgBackend *backend)
{
	Assert(backend != NULL);

	if (early_my_proc_number != INVALID_PROC_NUMBER)
	{
		backend->my_proc_number = early_my_proc_number;
		early_my_proc_number = INVALID_PROC_NUMBER;
	}

	if (early_parallel_leader_proc_number != INVALID_PROC_NUMBER)
	{
		backend->parallel_leader_proc_number =
			early_parallel_leader_proc_number;
		early_parallel_leader_proc_number = INVALID_PROC_NUMBER;
	}
}

static void
PgBackendAdoptEarlyMyBEEntry(PgBackend *backend)
{
	Assert(backend != NULL);

	if (early_my_beentry != NULL)
	{
		backend->my_beentry = early_my_beentry;
		early_my_beentry = NULL;
	}
}

static void
PgBackendAdoptEarlyMyBgworkerEntry(PgBackend *backend)
{
	Assert(backend != NULL);

	if (early_my_bgworker_entry != NULL)
	{
		backend->my_bgworker_entry = early_my_bgworker_entry;
		early_my_bgworker_entry = NULL;
	}
}

static void
PgBackendInitializeCommandState(PgBackendCommandState *command)
{
	Assert(command != NULL);

	MemSet(command, 0, sizeof(*command));
}

static void
PgBackendAdoptEarlyCommandState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->command = early_backend_command;
	PgBackendInitializeCommandState(&early_backend_command);
}

static void
PgBackendInitializeLogState(PgBackendLogState *log_state)
{
	Assert(log_state != NULL);

	MemSet(log_state, 0, sizeof(*log_state));
}

static void
PgBackendAdoptEarlyLogState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->log_state = early_backend_log;
	PgBackendInitializeLogState(&early_backend_log);
}

static void
PgBackendInitializeExprInterpState(PgBackendExprInterpState *expr_interp)
{
	Assert(expr_interp != NULL);

	MemSet(expr_interp, 0, sizeof(*expr_interp));
}

static void
PgBackendAdoptEarlyExprInterpState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->expr_interp = early_backend_expr_interp;
	PgBackendInitializeExprInterpState(&early_backend_expr_interp);
}

static void
PgSessionInitializeLoopState(PgSessionLoopState *loop_state)
{
	Assert(loop_state != NULL);

	MemSet(loop_state, 0, sizeof(*loop_state));
	loop_state->send_ready_for_query = true;
}

static void
PgSessionAdoptEarlyLoopState(PgSession *session)
{
	Assert(session != NULL);

	session->loop_state = early_session_loop_state;
	if (!session->loop_state.send_ready_for_query)
		session->loop_state.send_ready_for_query = true;
	PgSessionInitializeLoopState(&early_session_loop_state);
}

static void
PgBackendInitializePgStatPendingState(PgBackendPgStatPendingState *pgstat_pending)
{
	Assert(pgstat_pending != NULL);

	MemSet(pgstat_pending, 0, sizeof(*pgstat_pending));
	dlist_init(&pgstat_pending->pending);
}

static void
PgBackendAdoptEarlyPgStatPendingState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(dlist_is_empty(&early_backend_pgstat_pending.pending));

	backend->pgstat_pending = early_backend_pgstat_pending;
	dlist_init(&backend->pgstat_pending.pending);
	PgBackendInitializePgStatPendingState(&early_backend_pgstat_pending);
}

static void
PgBackendInitializeActivityState(PgBackendActivityState *activity)
{
	Assert(activity != NULL);

	MemSet(activity, 0, sizeof(*activity));
}

static void
PgBackendAdoptEarlyActivityState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->activity = early_backend_activity;
	PgBackendInitializeActivityState(&early_backend_activity);
}

static void
PgBackendInitializeMemoryManagerState(PgBackendMemoryManagerState *memory_manager)
{
	Assert(memory_manager != NULL);

	MemSet(memory_manager, 0, sizeof(*memory_manager));
}

static void
PgBackendAdoptEarlyMemoryManagerState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_memory_manager.context_freelists[0].num_free == 0);
	Assert(early_backend_memory_manager.context_freelists[0].first_free == NULL);
	Assert(early_backend_memory_manager.context_freelists[1].num_free == 0);
	Assert(early_backend_memory_manager.context_freelists[1].first_free == NULL);

	backend->memory_manager = early_backend_memory_manager;
	PgBackendInitializeMemoryManagerState(&early_backend_memory_manager);
}

static void
PgBackendInitializeUtilityState(PgBackendUtilityState *utility)
{
	Assert(utility != NULL);

	MemSet(utility, 0, sizeof(*utility));
	utility->superuser_last_roleid = InvalidOid;
}

static void
PgBackendAdoptEarlyUtilityState(PgBackend *backend)
{
	int			i;

	Assert(backend != NULL);
	Assert(early_backend_utility.extension_sibling_list == NULL);
	Assert(early_backend_utility.injection_point_cache == NULL);
	Assert(early_backend_utility.resource_release_callbacks == NULL);
	Assert(early_backend_utility.libxml_context == NULL);
	Assert(early_backend_utility.missing_attr_cache == NULL);
	for (i = 0; i < PG_BACKEND_MAX_SEQ_SCANS; i++)
		Assert(early_backend_utility.seq_scan_tables[i] == NULL);
	for (i = 0; i < PG_BACKEND_MAX_DATE_FIELDS; i++)
	{
		Assert(early_backend_utility.date_cache[i] == NULL);
		Assert(early_backend_utility.delta_cache[i] == NULL);
	}
	for (i = 0; i < PG_BACKEND_FORMAT_CACHE_ENTRIES; i++)
	{
		Assert(early_backend_utility.dch_cache[i] == NULL);
		Assert(early_backend_utility.num_cache[i] == NULL);
	}

	backend->utility = early_backend_utility;
	PgBackendInitializeUtilityState(&early_backend_utility);
}

static void
PgBackendInitializeParallelState(PgBackendParallelState *parallel)
{
	Assert(parallel != NULL);

	MemSet(parallel, 0, sizeof(*parallel));
	parallel->worker_number = -1;
	parallel->pq_mq_parallel_leader_proc_number = INVALID_PROC_NUMBER;
}

static void
PgBackendAdoptEarlyParallelState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->parallel = early_backend_parallel;
	if (early_backend_parallel.context_list_initialized)
	{
		Assert(dlist_is_empty(&early_backend_parallel.context_list));
		dlist_init(&backend->parallel.context_list);
		backend->parallel.context_list_initialized = true;
	}
	PgBackendInitializeParallelState(&early_backend_parallel);
}

static void
PgBackendInitializeInstrumentationState(PgBackendInstrumentationState *instrumentation)
{
	Assert(instrumentation != NULL);

	MemSet(instrumentation, 0, sizeof(*instrumentation));
}

static void
PgBackendAdoptEarlyInstrumentationState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->instrumentation = early_backend_instrumentation;
	PgBackendInitializeInstrumentationState(&early_backend_instrumentation);
}

static void
PgBackendInitializeBufferState(PgBackendBufferState *buffers)
{
	Assert(buffers != NULL);

	MemSet(buffers, 0, sizeof(*buffers));
	buffers->reserved_ref_count_slot = -1;
	buffers->private_ref_count_entry_last = -1;
}

static void
PgBackendAdoptEarlyBufferState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->buffers = early_backend_buffers;
	PgBackendInitializeBufferState(&early_backend_buffers);
}

static void
PgBackendInitializeStorageState(PgBackendStorageState *storage)
{
	Assert(storage != NULL);

	MemSet(storage, 0, sizeof(*storage));
	dlist_init(&storage->smgr_unpinned_relations);
}

static void
PgBackendAdoptEarlyStorageState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_storage.smgr_relation_hash == NULL);
	Assert(dlist_is_empty(&early_backend_storage.smgr_unpinned_relations));

	backend->storage = early_backend_storage;
	dlist_init(&backend->storage.smgr_unpinned_relations);
	PgBackendInitializeStorageState(&early_backend_storage);
}

static void
PgBackendInitializeLockState(PgBackendLockState *locks)
{
	Assert(locks != NULL);

	MemSet(locks, 0, sizeof(*locks));
}

static void
PgBackendAdoptEarlyLockState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->locks = early_backend_locks;
	PgBackendInitializeLockState(&early_backend_locks);
}

static void
PgBackendInitializeIPCState(PgBackendIPCState *ipc)
{
	Assert(ipc != NULL);

	MemSet(ipc, 0, sizeof(*ipc));
}

static void
PgBackendAdoptEarlyIPCState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->ipc = early_backend_ipc;

	if (backend->core.latch == &early_backend_ipc.local_latch_data)
		backend->core.latch = &backend->ipc.local_latch_data;

	if (backend->interrupt_latch == &early_backend_ipc.local_latch_data)
		PgBackendSetInterruptLatch(backend, &backend->ipc.local_latch_data);

	PgBackendInitializeIPCState(&early_backend_ipc);
}

static void
PgBackendEnsureWaitStateInitialized(PgBackendWaitState *wait_state)
{
	Assert(wait_state != NULL);

	if (wait_state->my_wait_event_info == NULL)
		wait_state->my_wait_event_info = &wait_state->local_wait_event_info;
}

static void
PgBackendInitializeWaitState(PgBackendWaitState *wait_state)
{
	Assert(wait_state != NULL);

	MemSet(wait_state, 0, sizeof(*wait_state));
	wait_state->my_wait_event_info = &wait_state->local_wait_event_info;
	pg_atomic_init_u32(&wait_state->waiting, 0);
}

static void
PgBackendAdoptEarlyWaitState(PgBackend *backend)
{
	Assert(backend != NULL);

	PgBackendEnsureWaitStateInitialized(&early_backend_wait_state);
	backend->wait_state = early_backend_wait_state;

	if (backend->wait_state.my_wait_event_info ==
		&early_backend_wait_state.local_wait_event_info)
		backend->wait_state.my_wait_event_info =
			&backend->wait_state.local_wait_event_info;

	PgBackendInitializeWaitState(&early_backend_wait_state);
}

static void
PgBackendInitializeTransactionState(PgBackendTransactionState *transaction)
{
	Assert(transaction != NULL);

	MemSet(transaction, 0, sizeof(*transaction));
	transaction->cached_fetch_xid = InvalidTransactionId;
	transaction->two_phase_cached_fxid = InvalidFullTransactionId;
	transaction->procarray_cached_xid_not_in_progress = InvalidTransactionId;
	transaction->compute_xid_horizons_result_last_xmin = InvalidTransactionId;
	dclist_init(&transaction->multixact_cache);
}

static void
PgBackendAdoptEarlyTransactionState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(!early_backend_transaction.multixact_cache_initialized ||
		   dclist_is_empty(&early_backend_transaction.multixact_cache));

	backend->transaction = early_backend_transaction;
	if (backend->transaction.multixact_cache_initialized)
		dclist_init(&backend->transaction.multixact_cache);
	PgBackendInitializeTransactionState(&early_backend_transaction);
}

static void
PgBackendInitializeTimeoutState(PgBackendTimeoutState *timeout)
{
	Assert(timeout != NULL);

	MemSet(timeout, 0, sizeof(*timeout));
}

static void
PgBackendAdoptEarlyTimeoutState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->timeout = early_backend_timeout;
	PgBackendInitializeTimeoutState(&early_backend_timeout);
}

static void
PgBackendInitializeWalSenderState(PgBackendWalSenderState *walsender)
{
	Assert(walsender != NULL);

	MemSet(walsender, 0, sizeof(*walsender));
}

static void
PgBackendAdoptEarlyWalSenderState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_walsender.my_wal_snd == NULL);
	Assert(early_backend_walsender.xlogreader == NULL);
	Assert(early_backend_walsender.uploaded_manifest == NULL);
	Assert(early_backend_walsender.uploaded_manifest_mcxt == NULL);
	Assert(early_backend_walsender.output_message.data == NULL);
	Assert(early_backend_walsender.reply_message.data == NULL);
	Assert(early_backend_walsender.tmpbuf.data == NULL);
	Assert(early_backend_walsender.logical_decoding_ctx == NULL);
	Assert(early_backend_walsender.replication_cmd_context == NULL);
	Assert(early_backend_walsender.lag_tracker == NULL);

	backend->walsender = early_backend_walsender;
	PgBackendInitializeWalSenderState(&early_backend_walsender);
}

static void
PgBackendInitializeReplicationState(PgBackendReplicationState *replication)
{
	Assert(replication != NULL);

	MemSet(replication, 0, sizeof(*replication));
	replication->sync_rep_wait_mode = -1;
	replication->walreceiver_recv_file = -1;
	replication->walreceiver_primary_has_standby_xmin = true;
}

static void
PgBackendAdoptEarlyReplicationState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_replication.my_replication_slot == NULL);
	Assert(early_backend_replication.walreceiver_conn == NULL);
	Assert(early_backend_replication.walreceiver_recv_file == -1);
	Assert(early_backend_replication.walreceiver_reply_message.data == NULL);

	backend->replication = early_backend_replication;
	PgBackendInitializeReplicationState(&early_backend_replication);
}

static void
PgBackendInitializeLogicalReplicationState(PgBackendLogicalReplicationState *logical_replication)
{
	Assert(logical_replication != NULL);

	MemSet(logical_replication, 0, sizeof(*logical_replication));
	dlist_init(&logical_replication->lsn_mapping);
	logical_replication->apply_error_callback_arg.remote_attnum = -1;
	logical_replication->apply_error_callback_arg.remote_xid = InvalidTransactionId;
	logical_replication->apply_error_callback_arg.finish_lsn = InvalidXLogRecPtr;
	logical_replication->subxact_data.subxact_last = InvalidTransactionId;
	logical_replication->remote_final_lsn = InvalidXLogRecPtr;
	logical_replication->stream_xid = InvalidTransactionId;
	logical_replication->skip_xact_finish_lsn = InvalidXLogRecPtr;
	logical_replication->last_flushpos = InvalidXLogRecPtr;
	logical_replication->slotsync_sleep_ms = PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS;
}

static void
PgBackendAdoptEarlyLogicalReplicationState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(dlist_is_empty(&early_backend_logical_replication.lsn_mapping));
	Assert(early_backend_logical_replication.apply_error_callback_arg.rel ==
		   NULL);
	Assert(early_backend_logical_replication.apply_error_callback_arg.origin_name
		   == NULL);
	Assert(early_backend_logical_replication.subxact_data.subxacts == NULL);
	Assert(early_backend_logical_replication.apply_context == NULL);
	Assert(early_backend_logical_replication.my_parallel_shared == NULL);
	Assert(early_backend_logical_replication.logrep_worker_walrcv_conn == NULL);
	Assert(early_backend_logical_replication.my_subscription == NULL);
	Assert(early_backend_logical_replication.my_logical_rep_worker == NULL);
	Assert(early_backend_logical_replication.on_commit_wakeup_workers_subids ==
		   NIL);
	Assert(early_backend_logical_replication.stream_fd == NULL);
	Assert(early_backend_logical_replication.table_states_not_ready == NIL);
	Assert(early_backend_logical_replication.copybuf == NULL);
	Assert(early_backend_logical_replication.seqinfos == NIL);
	Assert(early_backend_logical_replication.slotsync_observed_primary_conninfo
		   == NULL);
	Assert(early_backend_logical_replication.slotsync_observed_primary_slotname
		   == NULL);
	Assert(early_backend_logical_replication.launcher_last_start_times_dsa == NULL);
	Assert(early_backend_logical_replication.launcher_last_start_times == NULL);
	Assert(early_backend_logical_replication.parallel_apply_txn_hash == NULL);
	Assert(early_backend_logical_replication.parallel_apply_worker_pool == NIL);
	Assert(early_backend_logical_replication.stream_apply_worker == NULL);
	Assert(early_backend_logical_replication.parallel_apply_subxactlist == NIL);

	backend->logical_replication = early_backend_logical_replication;
	dlist_init(&backend->logical_replication.lsn_mapping);
	PgBackendInitializeLogicalReplicationState(&early_backend_logical_replication);
}

static void
PgBackendInitializeXLogState(PgBackendXLogState *xlog)
{
	Assert(xlog != NULL);

	MemSet(xlog, 0, sizeof(*xlog));
	xlog->local_recovery_in_progress = true;
	xlog->local_xlog_insert_allowed = -1;
	xlog->proc_last_rec_ptr = InvalidXLogRecPtr;
	xlog->xact_last_rec_end = InvalidXLogRecPtr;
	xlog->xact_last_commit_end = InvalidXLogRecPtr;
	xlog->redo_rec_ptr = InvalidXLogRecPtr;
	xlog->open_log_file = -1;
	xlog->local_min_recovery_point = InvalidXLogRecPtr;
	xlog->update_min_recovery_point = true;
}

static void
PgBackendAdoptEarlyXLogState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_xlog.open_log_file == -1);
	Assert(early_backend_xlog.wal_debug_context == NULL);
	Assert(early_backend_xlog.btree_xlog_op_context == NULL);
	Assert(early_backend_xlog.gin_xlog_op_context == NULL);
	Assert(early_backend_xlog.gist_xlog_op_context == NULL);
	Assert(early_backend_xlog.spgist_xlog_op_context == NULL);

	backend->xlog = early_backend_xlog;
	PgBackendInitializeXLogState(&early_backend_xlog);
}

static void
PgBackendInitializeRecoveryState(PgBackendRecoveryState *recovery)
{
	Assert(recovery != NULL);

	MemSet(recovery, 0, sizeof(*recovery));
	recovery->standby_wait_us = PG_BACKEND_STANDBY_INITIAL_WAIT_US;
}

static void
PgBackendAdoptEarlyRecoveryState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_recovery.recovery_lock_hash == NULL);
	Assert(early_backend_recovery.recovery_lock_xid_hash == NULL);

	backend->recovery = early_backend_recovery;
	PgBackendInitializeRecoveryState(&early_backend_recovery);
}

static void
PgBackendInitializeMaintenanceWorkerState(PgBackendMaintenanceWorkerState *maintenance_worker)
{
	Assert(maintenance_worker != NULL);

	MemSet(maintenance_worker, 0, sizeof(*maintenance_worker));
	maintenance_worker->bgwriter_last_snapshot_lsn = InvalidXLogRecPtr;
	maintenance_worker->walsummarizer_sleep_quanta = 1;
	maintenance_worker->walsummarizer_redo_pointer_at_last_summary_removal =
		InvalidXLogRecPtr;
}

static void
PgBackendAdoptEarlyMaintenanceWorkerState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_maintenance_worker.arch_module_errdetail_string ==
		   NULL);
	Assert(early_backend_maintenance_worker.archive_callbacks == NULL);
	Assert(early_backend_maintenance_worker.archive_module_state == NULL);
	Assert(early_backend_maintenance_worker.archive_context == NULL);
	Assert(early_backend_maintenance_worker.loaded_archive_library == NULL);
	Assert(early_backend_maintenance_worker.pgarch_files == NULL);

	backend->maintenance_worker = early_backend_maintenance_worker;
	PgBackendInitializeMaintenanceWorkerState(&early_backend_maintenance_worker);
}

static void
PgBackendInitializeAutovacuumState(PgBackendAutovacuumState *autovacuum)
{
	Assert(autovacuum != NULL);

	MemSet(autovacuum, 0, sizeof(*autovacuum));
	autovacuum->av_storage_param_cost_delay = -1;
	autovacuum->av_storage_param_cost_limit = -1;
	dlist_init(&autovacuum->database_list);
}

static void
PgBackendAdoptEarlyAutovacuumState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_autovacuum.autovac_mem_cxt == NULL);
	Assert(dlist_is_empty(&early_backend_autovacuum.database_list));
	Assert(early_backend_autovacuum.database_list_cxt == NULL);
	Assert(early_backend_autovacuum.avl_dbase_array == NULL);
	Assert(early_backend_autovacuum.my_worker_info == NULL);

	backend->autovacuum = early_backend_autovacuum;
	dlist_init(&backend->autovacuum.database_list);
	PgBackendInitializeAutovacuumState(&early_backend_autovacuum);
}

static void
PgBackendInitializeRepackState(PgBackendRepackState *repack)
{
	Assert(repack != NULL);

	MemSet(repack, 0, sizeof(*repack));
	repack->repacked_rel_locator.relNumber = InvalidOid;
	repack->repacked_rel_toast_locator.relNumber = InvalidOid;
}

static void
PgBackendAdoptEarlyRepackState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_repack.decoding_worker == NULL);
	Assert(early_backend_repack.worker_dsm_segment == NULL);

	backend->repack = early_backend_repack;
	PgBackendInitializeRepackState(&early_backend_repack);
}

static void
PgBackendInitializeAioState(PgBackendAioState *aio)
{
	Assert(aio != NULL);

	MemSet(aio, 0, sizeof(*aio));
	aio->my_io_worker_id = -1;
}

static void
PgBackendAdoptEarlyAioState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_aio.my_backend == NULL);
	Assert(early_backend_aio.my_uring_context == NULL);

	backend->aio = early_backend_aio;
	PgBackendInitializeAioState(&early_backend_aio);
}

static void
PgBackendAdoptEarlyPendingInterrupts(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->pending_interrupts = early_pending_interrupts;
	MemSet(&early_pending_interrupts, 0, sizeof(early_pending_interrupts));
}

static void
PgBackendAdoptEarlyInterruptHoldoffs(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->interrupt_holdoffs.interrupt_holdoff_count =
		early_interrupt_holdoffs.interrupt_holdoff_count;
	backend->interrupt_holdoffs.query_cancel_holdoff_count =
		early_interrupt_holdoffs.query_cancel_holdoff_count;
	backend->interrupt_holdoffs.crit_section_count =
		early_interrupt_holdoffs.crit_section_count;

	early_interrupt_holdoffs.interrupt_holdoff_count = 0;
	early_interrupt_holdoffs.query_cancel_holdoff_count = 0;
	early_interrupt_holdoffs.crit_section_count = 0;
}

void
PgBackendAdoptEarlyState(PgBackend *backend)
{
	Assert(backend != NULL);

	PgBackendAdoptEarlyCoreState(backend);
	PgBackendAdoptEarlyCommandState(backend);
	PgBackendAdoptEarlyLogState(backend);
	PgBackendAdoptEarlyExprInterpState(backend);
	PgBackendAdoptEarlyMyProc(backend);
	PgBackendAdoptEarlyProcNumberState(backend);
	PgBackendAdoptEarlyMyBEEntry(backend);
	PgBackendAdoptEarlyMyBgworkerEntry(backend);
	PgBackendAdoptEarlyAuxProcessResourceOwner(backend);
	PgBackendAdoptEarlyPgStatPendingState(backend);
	PgBackendAdoptEarlyActivityState(backend);
	PgBackendAdoptEarlyMemoryManagerState(backend);
	PgBackendAdoptEarlyUtilityState(backend);
	PgBackendAdoptEarlyParallelState(backend);
	PgBackendAdoptEarlyInstrumentationState(backend);
	PgBackendAdoptEarlyBufferState(backend);
	PgBackendAdoptEarlyStorageState(backend);
	PgBackendAdoptEarlyLockState(backend);
	PgBackendAdoptEarlyIPCState(backend);
	PgBackendAdoptEarlyWaitState(backend);
	PgBackendAdoptEarlyTransactionState(backend);
	PgBackendAdoptEarlyTimeoutState(backend);
	PgBackendAdoptEarlyWalSenderState(backend);
	PgBackendAdoptEarlyReplicationState(backend);
	PgBackendAdoptEarlyLogicalReplicationState(backend);
	PgBackendAdoptEarlyXLogState(backend);
	PgBackendAdoptEarlyRecoveryState(backend);
	PgBackendAdoptEarlyMaintenanceWorkerState(backend);
	PgBackendAdoptEarlyAutovacuumState(backend);
	PgBackendAdoptEarlyRepackState(backend);
	PgBackendAdoptEarlyAioState(backend);
	PgBackendAdoptEarlyPendingInterrupts(backend);
	PgBackendAdoptEarlyInterruptHoldoffs(backend);
}

static void
PgExecutionAdoptEarlyDebugState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->debug.debug_query_string =
		early_execution_debug.debug_query_string;

	early_execution_debug.debug_query_string = NULL;
}

static void
PgExecutionInitializeErrorState(PgExecutionErrorState *error)
{
	Assert(error != NULL);

	MemSet(error, 0, sizeof(*error));
	error->errordata_stack_depth = -1;
}

static void
PgExecutionAdoptEarlyErrorState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->error = early_execution_error;
	PgExecutionInitializeErrorState(&early_execution_error);
}

static void
PgExecutionAdoptEarlyMemoryContexts(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->memory_contexts = early_execution_memory_contexts;
	MemSet(&early_execution_memory_contexts, 0,
		   sizeof(early_execution_memory_contexts));
}

static void
PgExecutionAdoptEarlyResourceOwners(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->resource_owners = early_execution_resource_owners;
	MemSet(&early_execution_resource_owners, 0,
		   sizeof(early_execution_resource_owners));
}

static void
PgExecutionInitializeSPIState(PgExecutionSPIState *spi)
{
	Assert(spi != NULL);

	MemSet(spi, 0, sizeof(*spi));
	spi->connected = -1;
}

static void
PgExecutionAdoptEarlySPIState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->spi = early_execution_spi;
	PgExecutionInitializeSPIState(&early_execution_spi);
}

static void
PgExecutionAdoptEarlyPortalState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->portal = early_execution_portal;
	MemSet(&early_execution_portal, 0, sizeof(early_execution_portal));
}

static void
PgExecutionInitializeVacuumState(PgExecutionVacuumState *vacuum)
{
	Assert(vacuum != NULL);

	MemSet(vacuum, 0, sizeof(*vacuum));
}

static void
PgExecutionAdoptEarlyVacuumState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->vacuum = early_execution_vacuum;
	PgExecutionInitializeVacuumState(&early_execution_vacuum);
}

static void
PgExecutionInitializeNodeIOState(PgExecutionNodeIOState *node_io)
{
	Assert(node_io != NULL);

	MemSet(node_io, 0, sizeof(*node_io));
}

static void
PgExecutionAdoptEarlyNodeIOState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->node_io = early_execution_node_io;
	PgExecutionInitializeNodeIOState(&early_execution_node_io);
}

static void
PgExecutionInitializeBaseBackupState(PgExecutionBaseBackupState *basebackup)
{
	Assert(basebackup != NULL);

	MemSet(basebackup, 0, sizeof(*basebackup));
}

static void
PgExecutionAdoptEarlyBaseBackupState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->basebackup = early_execution_basebackup;
	PgExecutionInitializeBaseBackupState(&early_execution_basebackup);
}

static void
PgExecutionInitializeAnalyzeState(PgExecutionAnalyzeState *analyze)
{
	Assert(analyze != NULL);

	MemSet(analyze, 0, sizeof(*analyze));
}

static void
PgExecutionAdoptEarlyAnalyzeState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->analyze = early_execution_analyze;
	PgExecutionInitializeAnalyzeState(&early_execution_analyze);
}

static void
PgExecutionInitializeExtensionState(PgExecutionExtensionState *extension)
{
	Assert(extension != NULL);

	extension->creating = false;
	extension->current_object = InvalidOid;
}

static void
PgExecutionAdoptEarlyExtensionState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->extension = early_execution_extension;
	PgExecutionInitializeExtensionState(&early_execution_extension);
}

static void
PgExecutionInitializeMatViewState(PgExecutionMatViewState *matview)
{
	Assert(matview != NULL);

	matview->maintenance_depth = 0;
}

static void
PgExecutionAdoptEarlyMatViewState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->matview = early_execution_matview;
	PgExecutionInitializeMatViewState(&early_execution_matview);
}

static void
PgExecutionInitializeSnapshotState(PgExecutionSnapshotState *snapshot)
{
	Assert(snapshot != NULL);

	MemSet(snapshot, 0, sizeof(*snapshot));
	snapshot->transaction_xmin = FirstNormalTransactionId;
	snapshot->recent_xmin = FirstNormalTransactionId;
}

static void
PgExecutionAdoptEarlySnapshotState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->snapshot = early_execution_snapshot;
	PgExecutionInitializeSnapshotState(&early_execution_snapshot);
}

static void
PgExecutionInitializeComboCidState(PgExecutionComboCidState *combo_cid)
{
	Assert(combo_cid != NULL);

	MemSet(combo_cid, 0, sizeof(*combo_cid));
}

static void
PgExecutionAdoptEarlyComboCidState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->combo_cid = early_execution_combo_cid;
	PgExecutionInitializeComboCidState(&early_execution_combo_cid);
}

static void
PgExecutionInitializeXLogInsertState(PgExecutionXLogInsertState *xloginsert)
{
	Assert(xloginsert != NULL);

	MemSet(xloginsert, 0, sizeof(*xloginsert));
}

static void
PgExecutionAdoptEarlyXLogInsertState(PgExecution *execution)
{
	Assert(execution != NULL);
	Assert(!early_execution_xloginsert.begininsert_called);

	execution->xloginsert = early_execution_xloginsert;
	if (execution->xloginsert.mainrdata_last ==
		(XLogRecData *) &early_execution_xloginsert.mainrdata_head)
		execution->xloginsert.mainrdata_last =
			(XLogRecData *) &execution->xloginsert.mainrdata_head;

	PgExecutionInitializeXLogInsertState(&early_execution_xloginsert);
}

static void
PgExecutionInitializeXactState(PgExecutionXactState *xact)
{
	Assert(xact != NULL);

	MemSet(xact, 0, sizeof(*xact));
	xact->iso_level = XACT_READ_COMMITTED;
	xact->check_xid_alive = InvalidTransactionId;
	xact->top_full_transaction_id = InvalidFullTransactionId;
}

static void
PgExecutionAdoptEarlyXactState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->xact = early_execution_xact;
	PgExecutionInitializeXactState(&early_execution_xact);
}

static void
PgExecutionInitializeTransactionCleanupState(PgExecutionTransactionCleanupState *transaction_cleanup)
{
	Assert(transaction_cleanup != NULL);

	MemSet(transaction_cleanup, 0, sizeof(*transaction_cleanup));
}

static void
PgExecutionAdoptEarlyTransactionCleanupState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->transaction_cleanup = early_execution_transaction_cleanup;
	PgExecutionInitializeTransactionCleanupState(&early_execution_transaction_cleanup);
}

static void
PgExecutionInitializeReplicationScratchState(PgExecutionReplicationScratchState *replication_scratch)
{
	Assert(replication_scratch != NULL);

	MemSet(replication_scratch, 0, sizeof(*replication_scratch));
	replication_scratch->replorigin_xact.origin = InvalidReplOriginId;
	replication_scratch->replorigin_xact.origin_lsn = InvalidXLogRecPtr;
	replication_scratch->replorigin_xact.origin_timestamp = 0;
}

static void
PgExecutionAdoptEarlyReplicationScratchState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->replication_scratch = early_execution_replication_scratch;
	PgExecutionInitializeReplicationScratchState(&early_execution_replication_scratch);
}

static void
PgExecutionInitializeGUCErrorState(PgExecutionGUCErrorState *guc_error)
{
	Assert(guc_error != NULL);

	MemSet(guc_error, 0, sizeof(*guc_error));
}

static void
PgExecutionAdoptEarlyGUCErrorState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->guc_error = early_execution_guc_error;
	PgExecutionInitializeGUCErrorState(&early_execution_guc_error);
}

static void
PgExecutionInitializeAsyncState(PgExecutionAsyncState *async)
{
	Assert(async != NULL);

	MemSet(async, 0, sizeof(*async));
}

static void
PgExecutionAdoptEarlyAsyncState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->async = early_execution_async;
	PgExecutionInitializeAsyncState(&early_execution_async);
}

static void
PgExecutionInitializeCatalogState(PgExecutionCatalogState *catalog)
{
	Assert(catalog != NULL);

	MemSet(catalog, 0, sizeof(*catalog));
	catalog->currently_reindexed_heap = InvalidOid;
	catalog->currently_reindexed_index = InvalidOid;
}

static void
PgExecutionAdoptEarlyCatalogState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->catalog = early_execution_catalog;
	PgExecutionInitializeCatalogState(&early_execution_catalog);
}

static void
PgExecutionInitializeCatalogCacheState(PgExecutionCatalogCacheState *catalog_cache)
{
	Assert(catalog_cache != NULL);

	MemSet(catalog_cache, 0, sizeof(*catalog_cache));
}

static void
PgExecutionAdoptEarlyCatalogCacheState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->catalog_cache = early_execution_catalog_cache;
	PgExecutionInitializeCatalogCacheState(&early_execution_catalog_cache);
}

static void
PgExecutionInitializeRelMapState(PgExecutionRelMapState *relmap)
{
	Assert(relmap != NULL);

	MemSet(relmap, 0, sizeof(*relmap));
}

static void
PgExecutionAdoptEarlyRelMapState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->relmap = early_execution_relmap;
	PgExecutionInitializeRelMapState(&early_execution_relmap);
}

static void
PgExecutionInitializeInvalidationState(PgExecutionInvalidationState
									   *invalidation)
{
	Assert(invalidation != NULL);

	MemSet(invalidation, 0, sizeof(*invalidation));
}

static void
PgExecutionAdoptEarlyInvalidationState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->invalidation = early_execution_invalidation;
	PgExecutionInitializeInvalidationState(&early_execution_invalidation);
}

static void
PgExecutionInitializeTwoPhaseRecordState(PgExecutionTwoPhaseRecordState
										 *two_phase_records)
{
	Assert(two_phase_records != NULL);

	MemSet(two_phase_records, 0, sizeof(*two_phase_records));
}

static void
PgExecutionAdoptEarlyTwoPhaseRecordState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->two_phase_records = early_execution_two_phase_records;
	PgExecutionInitializeTwoPhaseRecordState(&early_execution_two_phase_records);
}

static void
PgExecutionInitializeTriggerState(PgExecutionTriggerState *trigger)
{
	Assert(trigger != NULL);

	MemSet(trigger, 0, sizeof(*trigger));
}

static void
PgExecutionAdoptEarlyTriggerState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->trigger = early_execution_trigger;
	PgExecutionInitializeTriggerState(&early_execution_trigger);
}

static void
PgExecutionInitializeRegexState(PgExecutionRegexState *regex)
{
	Assert(regex != NULL);

	MemSet(regex, 0, sizeof(*regex));
}

static void
PgExecutionAdoptEarlyRegexState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->regex = early_execution_regex;
	PgExecutionInitializeRegexState(&early_execution_regex);
}

static void
PgExecutionInitializeValgrindState(PgExecutionValgrindState *valgrind)
{
	Assert(valgrind != NULL);

	MemSet(valgrind, 0, sizeof(*valgrind));
}

static void
PgExecutionAdoptEarlyValgrindState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->valgrind = early_execution_valgrind;
	PgExecutionInitializeValgrindState(&early_execution_valgrind);
}

static void
PgExecutionInitializeSnapBuildState(PgExecutionSnapBuildState *snapbuild)
{
	Assert(snapbuild != NULL);

	MemSet(snapbuild, 0, sizeof(*snapbuild));
}

static void
PgExecutionAdoptEarlySnapBuildState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->snapbuild = early_execution_snapbuild;
	PgExecutionInitializeSnapBuildState(&early_execution_snapbuild);
}

void
PgExecutionAdoptEarlyState(PgExecution *execution)
{
	Assert(execution != NULL);

	PgExecutionAdoptEarlyDebugState(execution);
	PgExecutionAdoptEarlyErrorState(execution);
	PgExecutionAdoptEarlyMemoryContexts(execution);
	PgExecutionAdoptEarlyResourceOwners(execution);
	PgExecutionAdoptEarlySPIState(execution);
	PgExecutionAdoptEarlyPortalState(execution);
	PgExecutionAdoptEarlyVacuumState(execution);
	PgExecutionAdoptEarlyNodeIOState(execution);
	PgExecutionAdoptEarlyBaseBackupState(execution);
	PgExecutionAdoptEarlyAnalyzeState(execution);
	PgExecutionAdoptEarlyExtensionState(execution);
	PgExecutionAdoptEarlyMatViewState(execution);
	PgExecutionAdoptEarlySnapshotState(execution);
	PgExecutionAdoptEarlyComboCidState(execution);
	PgExecutionAdoptEarlyXLogInsertState(execution);
	PgExecutionAdoptEarlyXactState(execution);
	PgExecutionAdoptEarlyTransactionCleanupState(execution);
	PgExecutionAdoptEarlyReplicationScratchState(execution);
	PgExecutionAdoptEarlyGUCErrorState(execution);
	PgExecutionAdoptEarlyAsyncState(execution);
	PgExecutionAdoptEarlyCatalogState(execution);
	PgExecutionAdoptEarlyCatalogCacheState(execution);
	PgExecutionAdoptEarlyRelMapState(execution);
	PgExecutionAdoptEarlyInvalidationState(execution);
	PgExecutionAdoptEarlyTwoPhaseRecordState(execution);
	PgExecutionAdoptEarlyTriggerState(execution);
	PgExecutionAdoptEarlyRegexState(execution);
	PgExecutionAdoptEarlyValgrindState(execution);
	PgExecutionAdoptEarlySnapBuildState(execution);
}

static void
PgBackendInitializeRuntimeObject(PgBackend *backend,
								 PgRuntime *runtime,
								 PgCarrier *carrier,
								 PgSession *session,
								 PgConnection *connection,
								 PgExecution *execution,
								 BackendType backend_type,
								 struct Latch *interrupt_latch)
{
	Assert(backend != NULL);

	backend->runtime = runtime;
	backend->id = PgBackendAssignId();
	backend->carrier = carrier;
	backend->session = session;
	backend->connection = connection;
	backend->execution = execution;
	backend->backend_type = backend_type;
	PgBackendInitializeProcNumberState(backend);
	PgBackendInitializeInterrupts(backend);
	PgBackendResetCoreState(&backend->core);
	PgBackendInitializeCommandState(&backend->command);
	PgBackendInitializeLogState(&backend->log_state);
	PgBackendInitializeExprInterpState(&backend->expr_interp);
	PgBackendInitializePgStatPendingState(&backend->pgstat_pending);
	PgBackendInitializeActivityState(&backend->activity);
	PgBackendInitializeMemoryManagerState(&backend->memory_manager);
	PgBackendInitializeUtilityState(&backend->utility);
	PgBackendInitializeParallelState(&backend->parallel);
	PgBackendInitializeInstrumentationState(&backend->instrumentation);
	PgBackendInitializeBufferState(&backend->buffers);
	PgBackendInitializeStorageState(&backend->storage);
	PgBackendInitializeLockState(&backend->locks);
	PgBackendInitializeIPCState(&backend->ipc);
	PgBackendInitializeWaitState(&backend->wait_state);
	PgBackendInitializeTransactionState(&backend->transaction);
	PgBackendInitializeTimeoutState(&backend->timeout);
	PgBackendInitializeWalSenderState(&backend->walsender);
	PgBackendInitializeReplicationState(&backend->replication);
	PgBackendInitializeLogicalReplicationState(&backend->logical_replication);
	PgBackendInitializeXLogState(&backend->xlog);
	PgBackendInitializeRecoveryState(&backend->recovery);
	PgBackendInitializeMaintenanceWorkerState(&backend->maintenance_worker);
	PgBackendInitializeAutovacuumState(&backend->autovacuum);
	PgBackendInitializeRepackState(&backend->repack);
	PgBackendInitializeAioState(&backend->aio);
	PgBackendSetInterruptLatch(backend, interrupt_latch);
	dlist_init(&backend->dsm_segment_list);
	PgBackendInitializeExitState(&backend->exit_state);
}

static void
PgSessionInitializeRuntimeObject(PgSession *session,
								 PgBackend *backend,
								 PgConnection *connection,
								 PgExecution *execution)
{
	Assert(session != NULL);

	session->backend = backend;
	session->connection = connection;
	session->execution = execution;
	MemSet(&session->database, 0, sizeof(session->database));
	PgSessionInitializeLoopState(&session->loop_state);
	PgSessionInitializeTablespaceState(&session->tablespace);
	PgSessionInitializeBinaryUpgradeState(&session->binary_upgrade);
	PgSessionInitializeDateTimeState(&session->datetime);
	PgSessionInitializeTextSearchState(&session->text_search);
	PgSessionInitializeConnectionGUCState(&session->connection_guc);
	PgSessionInitializeParserState(&session->parser);
	PgSessionInitializeVacuumState(&session->vacuum);
	PgSessionInitializeBufferIOState(&session->buffer_io);
	PgSessionInitializeXactDefaultState(&session->xact_defaults);
	PgSessionInitializeLockWaitState(&session->lock_wait);
	PgSessionInitializeLoggingState(&session->logging);
	PgSessionInitializeMiscGUCState(&session->misc_guc);
	PgSessionInitializePgStatState(&session->pgstat);
	PgSessionInitializeQueryIdState(&session->query_id);
	PgSessionInitializeStorageGUCState(&session->storage_guc);
	PgSessionInitializeUserGUCState(&session->user_guc);
	PgSessionInitializeUserIdentityState(&session->user_identity);
	PgSessionInitializeCommandGUCState(&session->command_guc);
	PgSessionInitializeReplicationGUCState(&session->replication_guc);
	PgSessionInitializeGeneralGUCState(&session->general_guc);
	PgSessionInitializeAccessWalGUCState(&session->access_wal_guc);
	PgSessionInitializeJitGUCState(&session->jit_guc);
	PgSessionInitializeSortGUCState(&session->sort_guc);
	PgSessionInitializeQueryMemoryState(&session->query_memory);
	PgSessionInitializePlannerCostState(&session->planner_cost);
	PgSessionInitializePlannerMethodState(&session->planner_method);
	PgSessionInitializePreparedStatementState(&session->prepared_statement);
	PgSessionInitializeOnCommitState(&session->on_commit);
	PgSessionInitializeSequenceState(&session->sequence);
	PgSessionInitializeRegexState(&session->regex);
	PgSessionInitializeLargeObjectState(&session->large_object);
	PgSessionInitializeAsyncState(&session->async);
	PgSessionInitializeEncodingState(&session->encoding);
	PgSessionInitializeTempFileState(&session->temp_file);
	PgSessionInitializeRandomState(&session->random);
	PgSessionInitializeOptimizerState(&session->optimizer);
	PgSessionInitializePlanCacheState(&session->plan_cache);
	PgSessionInitializeNamespaceState(&session->namespace_state);
	PgSessionInitializeLocaleState(&session->locale);
}

static void
PgConnectionInitializeRuntimeObject(PgConnection *connection,
									PgBackend *backend,
									PgSession *session,
									struct Port *port)
{
	Assert(connection != NULL);

	connection->backend = backend;
	connection->session = session;
	MemSet(&connection->identity, 0, sizeof(connection->identity));
	connection->identity.port = port;
	MemSet(&connection->socket_io, 0, sizeof(connection->socket_io));
	MemSet(&connection->protocol, 0, sizeof(connection->protocol));
	PgConnectionInitializeOutputState(&connection->output);
	MemSet(&connection->interrupts, 0, sizeof(connection->interrupts));
	PgConnectionInitializeStartupState(&connection->startup);
	MemSet(&connection->client_connection_info, 0,
		   sizeof(connection->client_connection_info));
	MemSet(&connection->security, 0, sizeof(connection->security));
}

static void
PgExecutionInitializeRuntimeObject(PgExecution *execution,
								   PgBackend *backend,
								   PgSession *session,
								   PgCarrier *carrier)
{
	Assert(execution != NULL);

	execution->backend = backend;
	execution->session = session;
	execution->carrier = carrier;
	PgExecutionInitializeErrorState(&execution->error);
	PgExecutionInitializeSPIState(&execution->spi);
	PgExecutionInitializeVacuumState(&execution->vacuum);
	PgExecutionInitializeNodeIOState(&execution->node_io);
	PgExecutionInitializeBaseBackupState(&execution->basebackup);
	PgExecutionInitializeAnalyzeState(&execution->analyze);
	PgExecutionInitializeExtensionState(&execution->extension);
	PgExecutionInitializeMatViewState(&execution->matview);
	PgExecutionInitializeSnapshotState(&execution->snapshot);
	PgExecutionInitializeComboCidState(&execution->combo_cid);
	PgExecutionInitializeXLogInsertState(&execution->xloginsert);
	PgExecutionInitializeXactState(&execution->xact);
	PgExecutionInitializeTransactionCleanupState(&execution->transaction_cleanup);
	PgExecutionInitializeReplicationScratchState(&execution->replication_scratch);
	PgExecutionInitializeGUCErrorState(&execution->guc_error);
	PgExecutionInitializeAsyncState(&execution->async);
	PgExecutionInitializeCatalogState(&execution->catalog);
	PgExecutionInitializeCatalogCacheState(&execution->catalog_cache);
	PgExecutionInitializeRelMapState(&execution->relmap);
	PgExecutionInitializeInvalidationState(&execution->invalidation);
	PgExecutionInitializeTwoPhaseRecordState(&execution->two_phase_records);
	PgExecutionInitializeTriggerState(&execution->trigger);
	PgExecutionInitializeRegexState(&execution->regex);
	PgExecutionInitializeValgrindState(&execution->valgrind);
	PgExecutionInitializeSnapBuildState(&execution->snapbuild);
}

void
InitializePgProcessRuntime(void)
{
	MemSet(&process_runtime, 0, sizeof(process_runtime));
	MemSet(&process_carrier, 0, sizeof(process_carrier));
	MemSet(&process_backend, 0, sizeof(process_backend));
	MemSet(&process_session, 0, sizeof(process_session));
	MemSet(&process_connection, 0, sizeof(process_connection));
	MemSet(&process_execution, 0, sizeof(process_execution));

	process_runtime.kind = PG_RUNTIME_PROCESS;
	process_runtime.current_carrier = &process_carrier;
	process_runtime.extension_backend_model = PG_BACKEND_MODEL_PROCESS;
	PgRuntimeAdoptEarlyServerGUCState(&process_runtime);

	process_carrier.kind = PG_CARRIER_PROCESS;
	process_carrier.runtime = &process_runtime;
	process_carrier.current_backend = &process_backend;
	process_carrier.current_session = &process_session;
	process_carrier.current_execution = &process_execution;

	process_backend.runtime = &process_runtime;
	process_backend.id = PgBackendAssignId();
	process_backend.carrier = &process_carrier;
	process_backend.session = &process_session;
	process_backend.connection = &process_connection;
	process_backend.execution = &process_execution;
	process_backend.backend_type = MyBackendType;
	PgBackendInitializeProcNumberState(&process_backend);
	PgBackendInitializeInterrupts(&process_backend);
	PgBackendAdoptEarlyState(&process_backend);
	PgBackendSetInterruptLatch(&process_backend, process_backend.core.latch);
	dlist_init(&process_backend.dsm_segment_list);
	PgBackendInitializeExitState(&process_backend.exit_state);
	PgBackendAdoptEarlyExitState(&process_backend.exit_state);

	process_session.backend = &process_backend;
	process_session.connection = &process_connection;
	process_session.execution = &process_execution;
	PgSessionAdoptEarlyState(&process_session);

	process_connection.backend = &process_backend;
	process_connection.session = &process_session;
	PgConnectionAdoptEarlyState(&process_connection, NULL);

	process_execution.backend = &process_backend;
	process_execution.session = &process_session;
	process_execution.carrier = &process_carrier;
	PgExecutionInitializeSPIState(&process_execution.spi);
	PgExecutionAdoptEarlyState(&process_execution);

	CurrentPgRuntime = &process_runtime;
	CurrentPgCarrier = &process_carrier;
	CurrentPgBackend = &process_backend;
	PgSetCurrentSession(&process_session);
	CurrentPgConnection = &process_connection;
	CurrentPgExecution = &process_execution;

	if (MyProc != NULL && MyProc->backendId == 0)
		MyProc->backendId = process_backend.id;
}

void
InitializePgThreadRuntime(PgBackendExitContinuation exit_backend)
{
	if (!thread_runtime_initialized)
	{
		MemSet(&thread_runtime, 0, sizeof(thread_runtime));

		thread_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
		thread_runtime.extension_backend_model =
			PG_BACKEND_MODEL_THREAD_PER_SESSION;
		if (process_runtime.server_guc.initialized)
			thread_runtime.server_guc = process_runtime.server_guc;
		else
			PgRuntimeInitializeServerGUCState(&thread_runtime.server_guc);
		PgBackendInitializeIdCounter();
		thread_runtime_initialized = true;
	}

	thread_runtime.exit_backend = exit_backend;
}

void
InitializePgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state,
									  BackendType backend_type,
									  struct Port *port,
									  struct Latch *interrupt_latch)
{
	Assert(state != NULL);
	Assert(thread_runtime_initialized);

	MemSet(state, 0, sizeof(*state));

	state->carrier.kind = PG_CARRIER_THREAD;
	state->carrier.runtime = &thread_runtime;
	state->carrier.current_backend = &state->backend;
	state->carrier.current_session = &state->session;
	state->carrier.current_execution = &state->execution;

	PgBackendInitializeRuntimeObject(&state->backend, &thread_runtime,
									 &state->carrier, &state->session,
									 &state->connection, &state->execution,
									 backend_type, interrupt_latch);
	PgSessionInitializeRuntimeObject(&state->session, &state->backend,
									 &state->connection, &state->execution);
	PgConnectionInitializeRuntimeObject(&state->connection, &state->backend,
										&state->session, port);
	PgExecutionInitializeRuntimeObject(&state->execution, &state->backend,
									   &state->session, &state->carrier);
}

void
InstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state)
{
	Assert(state != NULL);

	state->carrier.current_backend = &state->backend;
	state->carrier.current_session = &state->session;
	state->carrier.current_execution = &state->execution;
	PgBackendAdoptEarlyState(&state->backend);
	PgSessionAdoptEarlyState(&state->session);
	PgConnectionAdoptEarlyState(&state->connection,
								state->connection.identity.port);
	PgExecutionAdoptEarlyState(&state->execution);
	CurrentPgRuntime = &thread_runtime;
	CurrentPgCarrier = &state->carrier;
	CurrentPgBackend = &state->backend;
	PgSetCurrentSession(&state->session);
	CurrentPgConnection = &state->connection;
	CurrentPgExecution = &state->execution;
	InitializeThreadedSessionRequiredGUCOptions();
}

void
InitializePgThreadBackendRuntime(PgThreadBackendRuntimeState *state,
								 BackendType backend_type,
								 struct Port *port,
								 struct Latch *interrupt_latch)
{
	InitializePgThreadBackendRuntimeState(state, backend_type, port,
										  interrupt_latch);
	InstallPgThreadBackendRuntimeState(state);
}

void
PgSetCurrentSession(PgSession *session)
{
	CurrentPgSession = session;
	RebindSessionGUCVariablePointers();
}

bool
PgCurrentSessionOwnsPointer(const void *ptr)
{
	uintptr_t	address;
	uintptr_t	session_start;
	uintptr_t	session_end;

	if (CurrentPgSession == NULL || ptr == NULL)
		return false;

	address = (uintptr_t) ptr;
	session_start = (uintptr_t) CurrentPgSession;
	session_end = session_start + sizeof(PgSession);

	return address >= session_start && address < session_end;
}

static void
PgBackendDeleteMemoryContext(MemoryContext *context)
{
	if (context == NULL || *context == NULL)
		return;

	if (CurrentMemoryContext == *context)
		MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(*context);
	*context = NULL;
}

static void
PgBackendResetStringInfo(StringInfoData *buf)
{
	if (buf == NULL)
		return;

	if (buf->data != NULL)
		pfree(buf->data);
	MemSet(buf, 0, sizeof(*buf));
}

static void
PgBackendResetWalSenderClosedState(PgBackendWalSenderState *walsender)
{
	if (walsender == NULL)
		return;

	if (walsender->logical_decoding_ctx != NULL)
	{
		FreeDecodingContext(walsender->logical_decoding_ctx);
		walsender->logical_decoding_ctx = NULL;
		walsender->xlogreader = NULL;
	}
	else if (walsender->xlogreader != NULL)
	{
		XLogReaderFree(walsender->xlogreader);
		walsender->xlogreader = NULL;
	}

	PgBackendDeleteMemoryContext(&walsender->uploaded_manifest_mcxt);
	walsender->uploaded_manifest = NULL;

	PgBackendResetStringInfo(&walsender->output_message);
	PgBackendResetStringInfo(&walsender->reply_message);
	PgBackendResetStringInfo(&walsender->tmpbuf);

	PgBackendDeleteMemoryContext(&walsender->replication_cmd_context);

	if (walsender->lag_tracker != NULL)
	{
		pfree(walsender->lag_tracker);
		walsender->lag_tracker = NULL;
	}
}

static void
PgBackendResetReplicationClosedState(PgBackendReplicationState *replication)
{
	if (replication == NULL)
		return;

	if (replication->walreceiver_conn != NULL)
	{
		walrcv_disconnect(replication->walreceiver_conn);
		replication->walreceiver_conn = NULL;
	}

	if (replication->walreceiver_recv_file >= 0)
	{
		(void) close(replication->walreceiver_recv_file);
		replication->walreceiver_recv_file = -1;
	}

	PgBackendResetStringInfo(&replication->walreceiver_reply_message);
}

static void
PgBackendResetLogicalReplicationClosedState(PgBackendLogicalReplicationState *logical_replication)
{
	if (logical_replication == NULL)
		return;

	if (logical_replication->logrep_worker_walrcv_conn != NULL)
	{
		walrcv_disconnect(logical_replication->logrep_worker_walrcv_conn);
		logical_replication->logrep_worker_walrcv_conn = NULL;
	}

	if (logical_replication->stream_fd != NULL)
	{
		BufFileClose(logical_replication->stream_fd);
		logical_replication->stream_fd = NULL;
	}

	if (logical_replication->copybuf != NULL)
	{
		PgBackendResetStringInfo(logical_replication->copybuf);
		pfree(logical_replication->copybuf);
		logical_replication->copybuf = NULL;
	}

	if (logical_replication->subxact_data.subxacts != NULL)
	{
		pfree(logical_replication->subxact_data.subxacts);
		logical_replication->subxact_data.subxacts = NULL;
	}
	logical_replication->subxact_data.nsubxacts = 0;
	logical_replication->subxact_data.nsubxacts_max = 0;
	logical_replication->subxact_data.subxact_last = InvalidTransactionId;

	if (logical_replication->apply_error_callback_arg.origin_name != NULL)
	{
		pfree(logical_replication->apply_error_callback_arg.origin_name);
		logical_replication->apply_error_callback_arg.origin_name = NULL;
	}
	logical_replication->apply_error_callback_arg.rel = NULL;
	logical_replication->apply_error_callback_arg.remote_attnum = -1;
	logical_replication->apply_error_callback_arg.remote_xid = InvalidTransactionId;
	logical_replication->apply_error_callback_arg.finish_lsn = InvalidXLogRecPtr;

	if (logical_replication->slotsync_observed_primary_conninfo != NULL)
	{
		pfree(logical_replication->slotsync_observed_primary_conninfo);
		logical_replication->slotsync_observed_primary_conninfo = NULL;
	}
	if (logical_replication->slotsync_observed_primary_slotname != NULL)
	{
		pfree(logical_replication->slotsync_observed_primary_slotname);
		logical_replication->slotsync_observed_primary_slotname = NULL;
	}

	if (logical_replication->parallel_apply_txn_hash != NULL)
	{
		hash_destroy(logical_replication->parallel_apply_txn_hash);
		logical_replication->parallel_apply_txn_hash = NULL;
	}

	list_free(logical_replication->on_commit_wakeup_workers_subids);
	list_free(logical_replication->table_states_not_ready);
	list_free(logical_replication->seqinfos);
	list_free(logical_replication->parallel_apply_worker_pool);
	list_free(logical_replication->parallel_apply_subxactlist);

	PgBackendDeleteMemoryContext(&logical_replication->apply_context);

	dlist_init(&logical_replication->lsn_mapping);
	logical_replication->my_parallel_shared = NULL;
	logical_replication->my_subscription = NULL;
	logical_replication->my_subscription_valid = false;
	logical_replication->my_logical_rep_worker = NULL;
	logical_replication->on_commit_wakeup_workers_subids = NIL;
	logical_replication->table_states_not_ready = NIL;
	logical_replication->seqinfos = NIL;
	if (logical_replication->launcher_last_start_times != NULL)
	{
		dshash_detach(logical_replication->launcher_last_start_times);
		logical_replication->launcher_last_start_times = NULL;
	}
	if (logical_replication->launcher_last_start_times_dsa != NULL)
	{
		dsa_detach(logical_replication->launcher_last_start_times_dsa);
		logical_replication->launcher_last_start_times_dsa = NULL;
	}
	logical_replication->parallel_apply_worker_pool = NIL;
	logical_replication->stream_apply_worker = NULL;
	logical_replication->parallel_apply_subxactlist = NIL;
}

static void
PgBackendResetXLogClosedState(PgBackendXLogState *xlog)
{
	if (xlog == NULL)
		return;

	if (xlog->open_log_file >= 0)
	{
		(void) close(xlog->open_log_file);
		xlog->open_log_file = -1;
	}

	PgBackendDeleteMemoryContext(&xlog->wal_debug_context);
	PgBackendDeleteMemoryContext(&xlog->btree_xlog_op_context);
	PgBackendDeleteMemoryContext(&xlog->gin_xlog_op_context);
	PgBackendDeleteMemoryContext(&xlog->gist_xlog_op_context);
	PgBackendDeleteMemoryContext(&xlog->spgist_xlog_op_context);
}

static void
PgBackendResetMaintenanceWorkerClosedState(PgBackendMaintenanceWorkerState *maintenance_worker)
{
	if (maintenance_worker == NULL)
		return;

	if (maintenance_worker->arch_module_errdetail_string != NULL)
	{
		pfree(maintenance_worker->arch_module_errdetail_string);
		maintenance_worker->arch_module_errdetail_string = NULL;
	}
	if (maintenance_worker->archive_module_state != NULL)
	{
		pfree(maintenance_worker->archive_module_state);
		maintenance_worker->archive_module_state = NULL;
	}
	PgBackendDeleteMemoryContext(&maintenance_worker->archive_context);
	if (maintenance_worker->loaded_archive_library != NULL)
	{
		pfree(maintenance_worker->loaded_archive_library);
		maintenance_worker->loaded_archive_library = NULL;
	}
	PgArchResetFilesState(&maintenance_worker->pgarch_files);

	maintenance_worker->archive_callbacks = NULL;
}

static void
PgBackendResetAutovacuumClosedState(PgBackendAutovacuumState *autovacuum)
{
	if (autovacuum == NULL)
		return;

	PgBackendDeleteMemoryContext(&autovacuum->autovac_mem_cxt);
	autovacuum->database_list_cxt = NULL;
	autovacuum->avl_dbase_array = NULL;
	autovacuum->my_worker_info = NULL;
	dlist_init(&autovacuum->database_list);
}

static void
PgBackendResetAioClosedState(PgBackendAioState *aio)
{
	if (aio == NULL)
		return;

	aio->my_backend = NULL;
	aio->my_io_worker_id = -1;
	aio->my_uring_context = NULL;
}

static void
PgBackendResetMemoryManagerClosedState(PgBackendMemoryManagerState *memory_manager)
{
	if (memory_manager == NULL)
		return;

	AllocSetFreeContextFreelists(memory_manager->context_freelists,
								 PG_BACKEND_ALLOCSET_NUM_FREELISTS);
	memory_manager->log_memory_context_in_progress = false;
}

static void
PgBackendResetUtilityClosedState(PgBackendUtilityState *utility)
{
	int			i;

	if (utility == NULL)
		return;

	ResetExtensionSiblingCache();

	if (utility->injection_point_cache != NULL)
	{
		hash_destroy(utility->injection_point_cache);
		utility->injection_point_cache = NULL;
	}

	for (i = 0; i < utility->num_seq_scans; i++)
	{
		utility->seq_scan_tables[i] = NULL;
		utility->seq_scan_levels[i] = 0;
	}
	utility->num_seq_scans = 0;

	ResetResourceReleaseCallbacks();

	for (i = 0; i < utility->n_dch_cache; i++)
	{
		pfree(utility->dch_cache[i]);
		utility->dch_cache[i] = NULL;
	}
	utility->n_dch_cache = 0;
	utility->dch_counter = 0;

	for (i = 0; i < utility->n_num_cache; i++)
	{
		pfree(utility->num_cache[i]);
		utility->num_cache[i] = NULL;
	}
	utility->n_num_cache = 0;
	utility->num_counter = 0;

	if (utility->libxml_context != NULL)
	{
		MemoryContextDelete(utility->libxml_context);
		utility->libxml_context = NULL;
	}

	if (utility->missing_attr_cache != NULL)
	{
		hash_destroy(utility->missing_attr_cache);
		utility->missing_attr_cache = NULL;
	}
}

void
PgBackendResetClosedState(PgBackend *backend)
{
	if (backend == NULL)
		return;

	PgBackendResetWalSenderClosedState(&backend->walsender);
	PgBackendResetReplicationClosedState(&backend->replication);
	PgBackendResetLogicalReplicationClosedState(&backend->logical_replication);
	PgBackendResetXLogClosedState(&backend->xlog);
	PgBackendResetMaintenanceWorkerClosedState(&backend->maintenance_worker);
	PgBackendResetAutovacuumClosedState(&backend->autovacuum);
	PgBackendResetAioClosedState(&backend->aio);
	PgBackendResetMemoryManagerClosedState(&backend->memory_manager);
	PgBackendResetUtilityClosedState(&backend->utility);
}

MemoryContext
PgSessionGetDynamicLibraryMemoryContext(PgSession *session)
{
	Assert(session != NULL);

	if (session->dynamic_library_context == NULL)
		session->dynamic_library_context =
			AllocSetContextCreate(TopMemoryContext,
								  "dynamic library session state",
								  ALLOCSET_SMALL_SIZES);

	return session->dynamic_library_context;
}

void
PgSessionResetClosedState(PgSession *session)
{
	if (session == NULL)
		return;

	if (session->database.database_path != NULL)
	{
		pfree(session->database.database_path);
		session->database.database_path = NULL;
	}

	if (session->dynamic_library_context != NULL)
	{
		MemoryContextDelete(session->dynamic_library_context);
		session->dynamic_library_context = NULL;
	}
	else if (session->dynamic_library_inits != NIL)
		list_free(session->dynamic_library_inits);

	session->dynamic_library_inits = NIL;

	if (session->parser.operator_lookup_cache != NULL)
	{
		hash_destroy(session->parser.operator_lookup_cache);
		session->parser.operator_lookup_cache = NULL;
	}

	if (session->sequence.seqhashtab != NULL)
	{
		hash_destroy(session->sequence.seqhashtab);
		session->sequence.seqhashtab = NULL;
	}
	session->sequence.last_used_seq = NULL;

	pg_free_regex_ctype_cache_list(session->regex.ctype_cache_list);
	session->regex.ctype_cache_list = NULL;

	if (session->optimizer.planner_extension_names != NULL)
	{
		pfree(session->optimizer.planner_extension_names);
		session->optimizer.planner_extension_names = NULL;
	}
	session->optimizer.planner_extension_names_assigned = 0;
	session->optimizer.planner_extension_names_allocated = 0;
	if (session->optimizer.opr_proof_cache_hash != NULL)
	{
		hash_destroy(session->optimizer.opr_proof_cache_hash);
		session->optimizer.opr_proof_cache_hash = NULL;
	}

	if (session->locale.collation_cache_context != NULL)
	{
		MemoryContextDelete(session->locale.collation_cache_context);
		session->locale.collation_cache_context = NULL;
		session->locale.collation_cache = NULL;
		session->locale.last_collation_cache_oid = InvalidOid;
		session->locale.last_collation_cache_locale = NULL;
	}
}

void
PgExecutionResetClosedState(PgExecution *execution)
{
	if (execution == NULL)
		return;

	execution->debug.debug_query_string = NULL;
}

Session *
PgSessionGetLegacySession(PgSession *session)
{
	if (session == NULL)
		return NULL;

	return session->legacy_session;
}

void
PgSessionSetLegacySession(PgSession *session, Session *legacy_session)
{
	if (session == NULL)
		return;

	session->legacy_session = legacy_session;
}

Session *
PgCurrentLegacySession(void)
{
	return PgSessionGetLegacySession(CurrentPgSession);
}

static PgSessionDatabaseState *
PgCurrentSessionDatabaseState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_database;

	return &CurrentPgSession->database;
}

static PgRuntimeServerGUCState *
PgCurrentRuntimeServerGUCState(void)
{
	PgRuntimeServerGUCState *server_guc;

	if (CurrentPgRuntime == NULL)
		server_guc = &early_runtime_server_guc;
	else
		server_guc = &CurrentPgRuntime->server_guc;

	if (!server_guc->initialized)
		PgRuntimeInitializeServerGUCState(server_guc);

	return server_guc;
}

static PgSessionTablespaceState *
PgCurrentSessionTablespaceState(void)
{
	PgSessionTablespaceState *tablespace;

	if (CurrentPgSession == NULL)
		tablespace = &early_session_tablespace;
	else
		tablespace = &CurrentPgSession->tablespace;

	if (!tablespace->initialized)
		PgSessionInitializeTablespaceState(tablespace);

	return tablespace;
}

static PgSessionBinaryUpgradeState *
PgCurrentSessionBinaryUpgradeState(void)
{
	PgSessionBinaryUpgradeState *binary_upgrade;

	if (CurrentPgSession == NULL)
		binary_upgrade = &early_session_binary_upgrade;
	else
		binary_upgrade = &CurrentPgSession->binary_upgrade;

	if (!binary_upgrade->initialized)
		PgSessionInitializeBinaryUpgradeState(binary_upgrade);

	return binary_upgrade;
}

static PgSessionDateTimeState *
PgCurrentSessionDateTimeState(void)
{
	PgSessionDateTimeState *datetime;

	if (CurrentPgSession == NULL)
		datetime = &early_session_datetime;
	else
		datetime = &CurrentPgSession->datetime;

	if (!datetime->initialized)
		PgSessionInitializeDateTimeState(datetime);

	return datetime;
}

static PgSessionTextSearchState *
PgCurrentSessionTextSearchState(void)
{
	PgSessionTextSearchState *text_search;

	if (CurrentPgSession == NULL)
		text_search = &early_session_text_search;
	else
		text_search = &CurrentPgSession->text_search;

	if (!text_search->initialized)
		PgSessionInitializeTextSearchState(text_search);

	return text_search;
}

static PgSessionConnectionGUCState *
PgCurrentSessionConnectionGUCState(void)
{
	PgSessionConnectionGUCState *connection_guc;

	if (CurrentPgSession == NULL)
		connection_guc = &early_session_connection_guc;
	else
		connection_guc = &CurrentPgSession->connection_guc;

	if (!connection_guc->initialized)
		PgSessionInitializeConnectionGUCState(connection_guc);

	return connection_guc;
}

static PgSessionParserState *
PgCurrentSessionParserState(void)
{
	PgSessionParserState *parser;

	if (CurrentPgSession == NULL)
		parser = &early_session_parser;
	else
		parser = &CurrentPgSession->parser;

	if (!parser->initialized)
		PgSessionInitializeParserState(parser);

	return parser;
}

static PgSessionVacuumState *
PgCurrentSessionVacuumState(void)
{
	PgSessionVacuumState *vacuum;

	if (CurrentPgSession == NULL)
		vacuum = &early_session_vacuum;
	else
		vacuum = &CurrentPgSession->vacuum;

	if (!vacuum->initialized)
		PgSessionInitializeVacuumState(vacuum);

	return vacuum;
}

static PgSessionBufferIOState *
PgCurrentSessionBufferIOState(void)
{
	PgSessionBufferIOState *buffer_io;

	if (CurrentPgSession == NULL)
		return &early_session_buffer_io;

	buffer_io = &CurrentPgSession->buffer_io;
	if (!buffer_io->initialized)
		PgSessionInitializeBufferIOState(buffer_io);

	return buffer_io;
}

static PgSessionXactDefaultState *
PgCurrentSessionXactDefaultState(void)
{
	PgSessionXactDefaultState *xact_defaults;

	if (CurrentPgSession == NULL)
		xact_defaults = &early_session_xact_defaults;
	else
		xact_defaults = &CurrentPgSession->xact_defaults;

	if (!xact_defaults->initialized)
		PgSessionInitializeXactDefaultState(xact_defaults);

	return xact_defaults;
}

static PgSessionLockWaitState *
PgCurrentSessionLockWaitState(void)
{
	PgSessionLockWaitState *lock_wait;

	if (CurrentPgSession == NULL)
		lock_wait = &early_session_lock_wait;
	else
		lock_wait = &CurrentPgSession->lock_wait;

	if (!lock_wait->initialized)
		PgSessionInitializeLockWaitState(lock_wait);

	return lock_wait;
}

static PgSessionLoggingState *
PgCurrentSessionLoggingState(void)
{
	PgSessionLoggingState *logging;

	if (CurrentPgSession == NULL)
		logging = &early_session_logging;
	else
		logging = &CurrentPgSession->logging;

	if (!logging->initialized)
		PgSessionInitializeLoggingState(logging);

	return logging;
}

static PgSessionMiscGUCState *
PgCurrentSessionMiscGUCState(void)
{
	PgSessionMiscGUCState *misc_guc;

	if (CurrentPgSession == NULL)
		misc_guc = &early_session_misc_guc;
	else
		misc_guc = &CurrentPgSession->misc_guc;

	if (!misc_guc->initialized)
		PgSessionInitializeMiscGUCState(misc_guc);

	return misc_guc;
}

static PgSessionPgStatState *
PgCurrentSessionPgStatState(void)
{
	PgSessionPgStatState *pgstat;

	if (CurrentPgSession == NULL)
		pgstat = &early_session_pgstat;
	else
		pgstat = &CurrentPgSession->pgstat;

	if (!pgstat->initialized)
		PgSessionInitializePgStatState(pgstat);

	return pgstat;
}

static PgSessionQueryIdState *
PgCurrentSessionQueryIdState(void)
{
	PgSessionQueryIdState *query_id;

	if (CurrentPgSession == NULL)
		query_id = &early_session_query_id;
	else
		query_id = &CurrentPgSession->query_id;

	if (!query_id->initialized)
		PgSessionInitializeQueryIdState(query_id);

	return query_id;
}

static PgSessionStorageGUCState *
PgCurrentSessionStorageGUCState(void)
{
	PgSessionStorageGUCState *storage_guc;

	if (CurrentPgSession == NULL)
		storage_guc = &early_session_storage_guc;
	else
		storage_guc = &CurrentPgSession->storage_guc;

	if (!storage_guc->initialized)
		PgSessionInitializeStorageGUCState(storage_guc);

	return storage_guc;
}

static PgSessionUserGUCState *
PgCurrentSessionUserGUCState(void)
{
	PgSessionUserGUCState *user_guc;

	if (CurrentPgSession == NULL)
		user_guc = &early_session_user_guc;
	else
		user_guc = &CurrentPgSession->user_guc;

	if (!user_guc->initialized)
		PgSessionInitializeUserGUCState(user_guc);

	return user_guc;
}

static PgSessionUserIdentityState *
PgCurrentSessionUserIdentityState(void)
{
	PgSessionUserIdentityState *user_identity;

	if (CurrentPgSession == NULL)
		user_identity = &early_session_user_identity;
	else
		user_identity = &CurrentPgSession->user_identity;

	if (!user_identity->initialized)
		PgSessionInitializeUserIdentityState(user_identity);

	return user_identity;
}

PgSessionUserIdentityState *
PgCurrentUserIdentityState(void)
{
	return PgCurrentSessionUserIdentityState();
}

static PgSessionCommandGUCState *
PgCurrentSessionCommandGUCState(void)
{
	PgSessionCommandGUCState *command_guc;

	if (CurrentPgSession == NULL)
		command_guc = &early_session_command_guc;
	else
		command_guc = &CurrentPgSession->command_guc;

	if (!command_guc->initialized)
		PgSessionInitializeCommandGUCState(command_guc);

	return command_guc;
}

static PgSessionReplicationGUCState *
PgCurrentSessionReplicationGUCState(void)
{
	PgSessionReplicationGUCState *replication_guc;

	if (CurrentPgSession == NULL)
		replication_guc = &early_session_replication_guc;
	else
		replication_guc = &CurrentPgSession->replication_guc;

	if (!replication_guc->initialized)
		PgSessionInitializeReplicationGUCState(replication_guc);

	return replication_guc;
}

static PgSessionGeneralGUCState *
PgCurrentSessionGeneralGUCState(void)
{
	PgSessionGeneralGUCState *general_guc;

	if (CurrentPgSession == NULL)
		general_guc = &early_session_general_guc;
	else
		general_guc = &CurrentPgSession->general_guc;

	if (!general_guc->initialized)
		PgSessionInitializeGeneralGUCState(general_guc);

	return general_guc;
}

static PgSessionAccessWalGUCState *
PgCurrentSessionAccessWalGUCState(void)
{
	PgSessionAccessWalGUCState *access_wal_guc;

	if (CurrentPgSession == NULL)
		access_wal_guc = &early_session_access_wal_guc;
	else
		access_wal_guc = &CurrentPgSession->access_wal_guc;

	if (!access_wal_guc->initialized)
		PgSessionInitializeAccessWalGUCState(access_wal_guc);

	return access_wal_guc;
}

static PgSessionJitGUCState *
PgCurrentSessionJitGUCState(void)
{
	PgSessionJitGUCState *jit_guc;

	if (CurrentPgSession == NULL)
		jit_guc = &early_session_jit_guc;
	else
		jit_guc = &CurrentPgSession->jit_guc;

	if (!jit_guc->initialized)
		PgSessionInitializeJitGUCState(jit_guc);

	return jit_guc;
}

static PgSessionSortGUCState *
PgCurrentSessionSortGUCState(void)
{
	PgSessionSortGUCState *sort_guc;

	if (CurrentPgSession == NULL)
		sort_guc = &early_session_sort_guc;
	else
		sort_guc = &CurrentPgSession->sort_guc;

	if (!sort_guc->initialized)
		PgSessionInitializeSortGUCState(sort_guc);

	return sort_guc;
}

static PgSessionQueryMemoryState *
PgCurrentSessionQueryMemoryState(void)
{
	PgSessionQueryMemoryState *query_memory;

	if (CurrentPgSession == NULL)
		query_memory = &early_session_query_memory;
	else
		query_memory = &CurrentPgSession->query_memory;

	if (!query_memory->initialized)
		PgSessionInitializeQueryMemoryState(query_memory);

	return query_memory;
}

static PgSessionPlannerCostState *
PgCurrentSessionPlannerCostState(void)
{
	PgSessionPlannerCostState *planner_cost;

	if (CurrentPgSession == NULL)
		planner_cost = &early_session_planner_cost;
	else
		planner_cost = &CurrentPgSession->planner_cost;

	if (!planner_cost->initialized)
		PgSessionInitializePlannerCostState(planner_cost);

	return planner_cost;
}

static PgSessionPlannerMethodState *
PgCurrentSessionPlannerMethodState(void)
{
	PgSessionPlannerMethodState *planner_method;

	if (CurrentPgSession == NULL)
		planner_method = &early_session_planner_method;
	else
		planner_method = &CurrentPgSession->planner_method;

	if (!planner_method->initialized)
		PgSessionInitializePlannerMethodState(planner_method);

	return planner_method;
}

static PgSessionPreparedStatementState *
PgCurrentSessionPreparedStatementState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_prepared_statement;

	return &CurrentPgSession->prepared_statement;
}

static PgSessionOnCommitState *
PgCurrentSessionOnCommitState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_on_commit;

	return &CurrentPgSession->on_commit;
}

static PgSessionSequenceState *
PgCurrentSessionSequenceState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_sequence;

	return &CurrentPgSession->sequence;
}

static PgSessionRegexState *
PgCurrentSessionRegexState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_regex;

	return &CurrentPgSession->regex;
}

static PgSessionLargeObjectState *
PgCurrentSessionLargeObjectState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_large_object;

	return &CurrentPgSession->large_object;
}

static PgSessionAsyncState *
PgCurrentSessionAsyncState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_async;

	return &CurrentPgSession->async;
}

static PgSessionEncodingState *
PgCurrentSessionEncodingState(void)
{
	PgSessionEncodingState *encoding;

	if (CurrentPgSession == NULL)
		encoding = &early_session_encoding;
	else
		encoding = &CurrentPgSession->encoding;

	PgSessionEnsureEncodingStateInitialized(encoding);
	return encoding;
}

static PgSessionTempFileState *
PgCurrentSessionTempFileState(void)
{
	PgSessionTempFileState *temp_file;

	if (CurrentPgSession == NULL)
		temp_file = &early_session_temp_file;
	else
		temp_file = &CurrentPgSession->temp_file;

	if (!temp_file->initialized)
		PgSessionInitializeTempFileState(temp_file);

	return temp_file;
}

static PgSessionRandomState *
PgCurrentSessionRandomState(void)
{
	PgSessionRandomState *random;

	if (CurrentPgSession == NULL)
		random = &early_session_random;
	else
		random = &CurrentPgSession->random;

	if (!random->initialized)
		PgSessionInitializeRandomState(random);

	return random;
}

static PgSessionOptimizerState *
PgCurrentSessionOptimizerState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_optimizer;

	return &CurrentPgSession->optimizer;
}

static PgSessionPlanCacheState *
PgCurrentSessionPlanCacheState(void)
{
	PgSessionPlanCacheState *plan_cache;

	if (CurrentPgSession == NULL)
		plan_cache = &early_session_plan_cache;
	else
		plan_cache = &CurrentPgSession->plan_cache;

	if (!plan_cache->initialized)
		PgSessionInitializePlanCacheState(plan_cache);

	return plan_cache;
}

static PgSessionNamespaceState *
PgCurrentSessionNamespaceState(void)
{
	PgSessionNamespaceState *namespace_state;

	if (CurrentPgSession == NULL)
		namespace_state = &early_session_namespace;
	else
		namespace_state = &CurrentPgSession->namespace_state;

	if (!namespace_state->initialized)
		PgSessionInitializeNamespaceState(namespace_state);

	return namespace_state;
}

PgSessionNamespaceState *
PgCurrentNamespaceState(void)
{
	return PgCurrentSessionNamespaceState();
}

char **
PgCurrentNamespaceSearchPathRef(void)
{
	return &PgCurrentSessionNamespaceState()->namespace_search_path_value;
}

static PgSessionLocaleState *
PgCurrentSessionLocaleState(void)
{
	PgSessionLocaleState *locale;

	if (CurrentPgSession == NULL)
		locale = &early_session_locale;
	else
		locale = &CurrentPgSession->locale;

	if (!locale->initialized)
		PgSessionInitializeLocaleState(locale);

	return locale;
}

PgSessionLocaleState *
PgCurrentLocaleState(void)
{
	return PgCurrentSessionLocaleState();
}

char **
PgCurrentLocaleMessagesRef(void)
{
	return &PgCurrentSessionLocaleState()->locale_messages_value;
}

char **
PgCurrentLocaleMonetaryRef(void)
{
	return &PgCurrentSessionLocaleState()->locale_monetary_value;
}

char **
PgCurrentLocaleNumericRef(void)
{
	return &PgCurrentSessionLocaleState()->locale_numeric_value;
}

char **
PgCurrentLocaleTimeRef(void)
{
	return &PgCurrentSessionLocaleState()->locale_time_value;
}

int *
PgCurrentIcuValidationLevelRef(void)
{
	return &PgCurrentSessionLocaleState()->icu_validation_level_value;
}

Oid *
PgCurrentMyDatabaseIdRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_id;
}

Oid *
PgCurrentMyDatabaseTableSpaceRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_tablespace;
}

bool *
PgCurrentMyDatabaseHasLoginEventTriggersRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_has_login_event_triggers;
}

char **
PgCurrentDatabasePathRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_path;
}

char **
PgCurrentDefaultTablespaceRef(void)
{
	return &PgCurrentSessionTablespaceState()->default_tablespace_name;
}

char **
PgCurrentTempTablespacesRef(void)
{
	return &PgCurrentSessionTablespaceState()->temp_tablespaces_names;
}

bool *
PgCurrentAllowInPlaceTablespacesRef(void)
{
	return &PgCurrentSessionTablespaceState()->allow_in_place_tablespaces_value;
}

Oid *
PgCurrentBinaryUpgradeNextPgTablespaceOidRef(void)
{
	return &PgCurrentSessionTablespaceState()->binary_upgrade_next_pg_tablespace_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextPgTypeOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_pg_type_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextArrayPgTypeOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_array_pg_type_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextMrngPgTypeOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_mrng_pg_type_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextMrngArrayPgTypeOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_mrng_array_pg_type_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextHeapPgClassOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_heap_pg_class_oid_value;
}

RelFileNumber *
PgCurrentBinaryUpgradeNextHeapPgClassRelfilenumberRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_heap_pg_class_relfilenumber_value;
}

Oid *
PgCurrentBinaryUpgradeNextIndexPgClassOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_index_pg_class_oid_value;
}

RelFileNumber *
PgCurrentBinaryUpgradeNextIndexPgClassRelfilenumberRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_index_pg_class_relfilenumber_value;
}

Oid *
PgCurrentBinaryUpgradeNextToastPgClassOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_toast_pg_class_oid_value;
}

RelFileNumber *
PgCurrentBinaryUpgradeNextToastPgClassRelfilenumberRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_toast_pg_class_relfilenumber_value;
}

Oid *
PgCurrentBinaryUpgradeNextPgEnumOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_pg_enum_oid_value;
}

Oid *
PgCurrentBinaryUpgradeNextPgAuthidOidRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_next_pg_authid_oid_value;
}

bool *
PgCurrentBinaryUpgradeRecordInitPrivsRef(void)
{
	return &PgCurrentSessionBinaryUpgradeState()->binary_upgrade_record_init_privs_value;
}

int *
PgCurrentDateStyleRef(void)
{
	return &PgCurrentSessionDateTimeState()->date_style;
}

int *
PgCurrentDateOrderRef(void)
{
	return &PgCurrentSessionDateTimeState()->date_order;
}

int *
PgCurrentIntervalStyleRef(void)
{
	return &PgCurrentSessionDateTimeState()->interval_style;
}

char **
PgCurrentTimeZoneStringRef(void)
{
	return &PgCurrentSessionDateTimeState()->timezone_string_value;
}

char **
PgCurrentLogTimeZoneStringRef(void)
{
	return &PgCurrentSessionDateTimeState()->log_timezone_string_value;
}

pg_tz **
PgCurrentSessionTimeZoneRef(void)
{
	return &PgCurrentSessionDateTimeState()->session_timezone_value;
}

pg_tz **
PgCurrentLogTimeZoneRef(void)
{
	return &PgCurrentSessionDateTimeState()->log_timezone_value;
}

char **
PgCurrentTSCurrentConfigRef(void)
{
	return &PgCurrentSessionTextSearchState()->current_config_value;
}

Oid *
PgCurrentTSCurrentConfigCacheRef(void)
{
	return &PgCurrentSessionTextSearchState()->current_config_cache;
}

char **
PgCurrentClusterNameRef(void)
{
	return &PgCurrentRuntimeServerGUCState()->cluster_name_value;
}

char **
PgCurrentConfigFileNameRef(void)
{
	return &PgCurrentRuntimeServerGUCState()->config_file_name;
}

char **
PgCurrentHbaFileNameRef(void)
{
	return &PgCurrentRuntimeServerGUCState()->hba_file_name;
}

char **
PgCurrentIdentFileNameRef(void)
{
	return &PgCurrentRuntimeServerGUCState()->ident_file_name;
}

char **
PgCurrentHostsFileNameRef(void)
{
	return &PgCurrentRuntimeServerGUCState()->hosts_file_name;
}

char **
PgCurrentExternalPidFileRef(void)
{
	return &PgCurrentRuntimeServerGUCState()->external_pid_file_value;
}

char **
PgCurrentApplicationNameRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->application_name_value;
}

int *
PgCurrentTcpKeepalivesIdleRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->tcp_keepalives_idle_value;
}

int *
PgCurrentTcpKeepalivesIntervalRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->tcp_keepalives_interval_value;
}

int *
PgCurrentTcpKeepalivesCountRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->tcp_keepalives_count_value;
}

int *
PgCurrentTcpUserTimeoutRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->tcp_user_timeout_value;
}

bool *
PgCurrentLogDisconnectionsRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->log_disconnections_value;
}

int *
PgCurrentLogStatementRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->log_statement_value;
}

int *
PgCurrentPostAuthDelayRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->post_auth_delay_seconds;
}

char **
PgCurrentRestrictNonsystemRelationKindStringRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->restrict_nonsystem_relation_kind_string_value;
}

int *
PgCurrentRestrictNonsystemRelationKindRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->restrict_nonsystem_relation_kind_value;
}

HTAB **
PgCurrentPreparedQueriesRef(void)
{
	return &PgCurrentSessionPreparedStatementState()->prepared_queries;
}

List **
PgCurrentOnCommitActionsRef(void)
{
	return &PgCurrentSessionOnCommitState()->on_commits;
}

HTAB **
PgCurrentSequenceHashTableRef(void)
{
	return &PgCurrentSessionSequenceState()->seqhashtab;
}

struct SeqTableData **
PgCurrentLastUsedSequenceRef(void)
{
	return &PgCurrentSessionSequenceState()->last_used_seq;
}

bool *
PgCurrentTransformNullEqualsRef(void)
{
	return &PgCurrentSessionParserState()->transform_null_equals_value;
}

int *
PgCurrentBackslashQuoteRef(void)
{
	return &PgCurrentSessionParserState()->backslash_quote_value;
}

HTAB **
PgCurrentOperatorLookupCacheRef(void)
{
	return &PgCurrentSessionParserState()->operator_lookup_cache;
}

struct pg_ctype_cache **
PgCurrentRegexCtypeCacheListRef(void)
{
	return &PgCurrentSessionRegexState()->ctype_cache_list;
}

struct RelationData **
PgCurrentLargeObjectHeapRelationRef(void)
{
	return &PgCurrentSessionLargeObjectState()->heap_relation;
}

struct RelationData **
PgCurrentLargeObjectIndexRelationRef(void)
{
	return &PgCurrentSessionLargeObjectState()->index_relation;
}

HTAB **
PgCurrentAsyncLocalChannelTableRef(void)
{
	return &PgCurrentSessionAsyncState()->local_channel_table;
}

bool *
PgCurrentAsyncRegisteredListenerRef(void)
{
	return &PgCurrentSessionAsyncState()->registered_listener;
}

List **
PgCurrentEncodingConvProcListRef(void)
{
	return &PgCurrentSessionEncodingState()->conv_proc_list;
}

FmgrInfo **
PgCurrentToServerConvProcRef(void)
{
	return &PgCurrentSessionEncodingState()->to_server_conv_proc;
}

FmgrInfo **
PgCurrentToClientConvProcRef(void)
{
	return &PgCurrentSessionEncodingState()->to_client_conv_proc;
}

FmgrInfo **
PgCurrentUtf8ToServerConvProcRef(void)
{
	return &PgCurrentSessionEncodingState()->utf8_to_server_conv_proc;
}

const pg_enc2name **
PgCurrentClientEncodingRef(void)
{
	return &PgCurrentSessionEncodingState()->client_encoding;
}

const pg_enc2name **
PgCurrentDatabaseEncodingRef(void)
{
	return &PgCurrentSessionEncodingState()->database_encoding;
}

const pg_enc2name **
PgCurrentMessageEncodingRef(void)
{
	return &PgCurrentSessionEncodingState()->message_encoding;
}

bool *
PgCurrentEncodingStartupCompleteRef(void)
{
	return &PgCurrentSessionEncodingState()->backend_startup_complete;
}

int *
PgCurrentPendingClientEncodingRef(void)
{
	return &PgCurrentSessionEncodingState()->pending_client_encoding;
}

uint64 *
PgCurrentTemporaryFilesSizeRef(void)
{
	return &PgCurrentSessionTempFileState()->temporary_files_size;
}

long *
PgCurrentTempFileCounterRef(void)
{
	return &PgCurrentSessionTempFileState()->temp_file_counter;
}

Oid **
PgCurrentTempTableSpaceOidsRef(void)
{
	return &PgCurrentSessionTempFileState()->temp_table_spaces;
}

int *
PgCurrentNumTempTableSpacesRef(void)
{
	return &PgCurrentSessionTempFileState()->num_temp_table_spaces;
}

int *
PgCurrentNextTempTableSpaceRef(void)
{
	return &PgCurrentSessionTempFileState()->next_temp_table_space;
}

pg_prng_state *
PgCurrentPseudoRandomStateRef(void)
{
	return &PgCurrentSessionRandomState()->prng_state;
}

bool *
PgCurrentPseudoRandomSeedSetRef(void)
{
	return &PgCurrentSessionRandomState()->prng_seed_set;
}

const char ***
PgCurrentPlannerExtensionNameArrayRef(void)
{
	return &PgCurrentSessionOptimizerState()->planner_extension_names;
}

int *
PgCurrentPlannerExtensionNamesAssignedRef(void)
{
	return &PgCurrentSessionOptimizerState()->planner_extension_names_assigned;
}

int *
PgCurrentPlannerExtensionNamesAllocatedRef(void)
{
	return &PgCurrentSessionOptimizerState()->planner_extension_names_allocated;
}

HTAB **
PgCurrentOprProofCacheHashRef(void)
{
	return &PgCurrentSessionOptimizerState()->opr_proof_cache_hash;
}

dlist_head *
PgCurrentSavedPlanListRef(void)
{
	return &PgCurrentSessionPlanCacheState()->saved_plan_list;
}

dlist_head *
PgCurrentCachedExpressionListRef(void)
{
	return &PgCurrentSessionPlanCacheState()->cached_expression_list;
}

int *
PgCurrentVacuumBufferUsageLimitRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_buffer_usage_limit_kb;
}

int *
PgCurrentVacuumCostPageHitRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_cost_page_hit_value;
}

int *
PgCurrentVacuumCostPageMissRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_cost_page_miss_value;
}

int *
PgCurrentVacuumCostPageDirtyRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_cost_page_dirty_value;
}

int *
PgCurrentVacuumCostLimitRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_cost_limit_value;
}

double *
PgCurrentVacuumCostDelayRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_cost_delay_ms;
}

int *
PgCurrentDefaultStatisticsTargetRef(void)
{
	return &PgCurrentSessionVacuumState()->default_statistics_target_value;
}

int *
PgCurrentVacuumFreezeMinAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_freeze_min_age_value;
}

int *
PgCurrentVacuumFreezeTableAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_freeze_table_age_value;
}

int *
PgCurrentVacuumMultixactFreezeMinAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_multixact_freeze_min_age_value;
}

int *
PgCurrentVacuumMultixactFreezeTableAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_multixact_freeze_table_age_value;
}

int *
PgCurrentVacuumFailsafeAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_failsafe_age_value;
}

int *
PgCurrentVacuumMultixactFailsafeAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_multixact_failsafe_age_value;
}

bool *
PgCurrentTrackCostDelayTimingRef(void)
{
	return &PgCurrentSessionVacuumState()->track_cost_delay_timing_value;
}

bool *
PgCurrentVacuumTruncateRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_truncate_value;
}

double *
PgCurrentVacuumMaxEagerFreezeFailureRateRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_max_eager_freeze_failure_rate_value;
}

double *
PgCurrentLocalVacuumCostDelayRef(void)
{
	return &PgCurrentSessionVacuumState()->local_vacuum_cost_delay_ms;
}

int *
PgCurrentLocalVacuumCostLimitRef(void)
{
	return &PgCurrentSessionVacuumState()->local_vacuum_cost_limit_value;
}

bool *
PgCurrentZeroDamagedPagesRef(void)
{
	return &PgCurrentSessionBufferIOState()->zero_damaged_pages_value;
}

bool *
PgCurrentTrackIOTimingRef(void)
{
	return &PgCurrentSessionBufferIOState()->track_io_timing_value;
}

int *
PgCurrentEffectiveIOConcurrencyRef(void)
{
	return &PgCurrentSessionBufferIOState()->effective_io_concurrency_value;
}

int *
PgCurrentMaintenanceIOConcurrencyRef(void)
{
	return &PgCurrentSessionBufferIOState()->maintenance_io_concurrency_value;
}

int *
PgCurrentIOCombineLimitRef(void)
{
	return &PgCurrentSessionBufferIOState()->io_combine_limit_value;
}

int *
PgCurrentIOCombineLimitGUCRef(void)
{
	return &PgCurrentSessionBufferIOState()->io_combine_limit_guc_value;
}

int *
PgCurrentBackendFlushAfterRef(void)
{
	return &PgCurrentSessionBufferIOState()->backend_flush_after_value;
}

int *
PgCurrentDefaultXactIsoLevelRef(void)
{
	return &PgCurrentSessionXactDefaultState()->default_xact_iso_level;
}

bool *
PgCurrentDefaultXactReadOnlyRef(void)
{
	return &PgCurrentSessionXactDefaultState()->default_xact_read_only;
}

bool *
PgCurrentDefaultXactDeferrableRef(void)
{
	return &PgCurrentSessionXactDefaultState()->default_xact_deferrable;
}

int *
PgCurrentSynchronousCommitRef(void)
{
	return &PgCurrentSessionXactDefaultState()->synchronous_commit_value;
}

int *
PgCurrentDeadlockTimeoutRef(void)
{
	return &PgCurrentSessionLockWaitState()->deadlock_timeout_ms;
}

int *
PgCurrentStatementTimeoutRef(void)
{
	return &PgCurrentSessionLockWaitState()->statement_timeout_ms;
}

int *
PgCurrentLockTimeoutRef(void)
{
	return &PgCurrentSessionLockWaitState()->lock_timeout_ms;
}

int *
PgCurrentIdleInTransactionSessionTimeoutRef(void)
{
	return &PgCurrentSessionLockWaitState()->idle_in_transaction_session_timeout_ms;
}

int *
PgCurrentTransactionTimeoutRef(void)
{
	return &PgCurrentSessionLockWaitState()->transaction_timeout_ms;
}

int *
PgCurrentIdleSessionTimeoutRef(void)
{
	return &PgCurrentSessionLockWaitState()->idle_session_timeout_ms;
}

bool *
PgCurrentLogLockWaitsRef(void)
{
	return &PgCurrentSessionLockWaitState()->log_lock_waits_value;
}

bool *
PgCurrentLogLockFailuresRef(void)
{
	return &PgCurrentSessionLockWaitState()->log_lock_failures_value;
}

int *
PgCurrentTraceLockOidMinRef(void)
{
	return &PgCurrentSessionLockWaitState()->trace_lock_oidmin_value;
}

bool *
PgCurrentTraceLocksRef(void)
{
	return &PgCurrentSessionLockWaitState()->trace_locks_value;
}

bool *
PgCurrentTraceUserlocksRef(void)
{
	return &PgCurrentSessionLockWaitState()->trace_userlocks_value;
}

int *
PgCurrentTraceLockTableRef(void)
{
	return &PgCurrentSessionLockWaitState()->trace_lock_table_value;
}

bool *
PgCurrentDebugDeadlocksRef(void)
{
	return &PgCurrentSessionLockWaitState()->debug_deadlocks_value;
}

bool *
PgCurrentTraceLwlocksRef(void)
{
	return &PgCurrentSessionLockWaitState()->trace_lwlocks_value;
}

bool *
PgCurrentDebugPrintPlanRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_print_plan_value;
}

bool *
PgCurrentDebugPrintParseRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_print_parse_value;
}

bool *
PgCurrentDebugPrintRawParseRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_print_raw_parse_value;
}

bool *
PgCurrentDebugPrintRewrittenRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_print_rewritten_value;
}

bool *
PgCurrentDebugPrettyPrintRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_pretty_print_value;
}

#ifdef DEBUG_NODE_TESTS_ENABLED
bool *
PgCurrentDebugCopyParsePlanTreesRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_copy_parse_plan_trees_value;
}

bool *
PgCurrentDebugWriteReadParsePlanTreesRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_write_read_parse_plan_trees_value;
}

bool *
PgCurrentDebugRawExpressionCoverageTestRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_raw_expression_coverage_test_value;
}
#endif

bool *
PgCurrentLogParserStatsRef(void)
{
	return &PgCurrentSessionLoggingState()->log_parser_stats_value;
}

bool *
PgCurrentLogPlannerStatsRef(void)
{
	return &PgCurrentSessionLoggingState()->log_planner_stats_value;
}

bool *
PgCurrentLogExecutorStatsRef(void)
{
	return &PgCurrentSessionLoggingState()->log_executor_stats_value;
}

bool *
PgCurrentLogStatementStatsRef(void)
{
	return &PgCurrentSessionLoggingState()->log_statement_stats_value;
}

bool *
PgCurrentLogBtreeBuildStatsRef(void)
{
	return &PgCurrentSessionLoggingState()->log_btree_build_stats_value;
}

char **
PgCurrentEventSourceRef(void)
{
	return &PgCurrentSessionLoggingState()->event_source_value;
}

bool *
PgCurrentLogDurationRef(void)
{
	return &PgCurrentSessionLoggingState()->log_duration_value;
}

int *
PgCurrentLogErrorVerbosityRef(void)
{
	return &PgCurrentSessionLoggingState()->log_error_verbosity_value;
}

int *
PgCurrentLogParameterMaxLengthRef(void)
{
	return &PgCurrentSessionLoggingState()->log_parameter_max_length_value;
}

int *
PgCurrentLogParameterMaxLengthOnErrorRef(void)
{
	return &PgCurrentSessionLoggingState()->log_parameter_max_length_on_error_value;
}

int *
PgCurrentLogMinErrorStatementRef(void)
{
	return &PgCurrentSessionLoggingState()->log_min_error_statement_value;
}

int *
PgCurrentLogMinMessagesArrayRef(void)
{
	return PgCurrentSessionLoggingState()->log_min_messages_values;
}

char **
PgCurrentLogMinMessagesStringRef(void)
{
	return &PgCurrentSessionLoggingState()->log_min_messages_string_value;
}

int *
PgCurrentClientMinMessagesRef(void)
{
	return &PgCurrentSessionLoggingState()->client_min_messages_value;
}

int *
PgCurrentLogMinDurationSampleRef(void)
{
	return &PgCurrentSessionLoggingState()->log_min_duration_sample_value;
}

int *
PgCurrentLogMinDurationStatementRef(void)
{
	return &PgCurrentSessionLoggingState()->log_min_duration_statement_value;
}

int *
PgCurrentLogTempFilesRef(void)
{
	return &PgCurrentSessionLoggingState()->log_temp_files_value;
}

double *
PgCurrentLogStatementSampleRateRef(void)
{
	return &PgCurrentSessionLoggingState()->log_statement_sample_rate_value;
}

double *
PgCurrentLogXactSampleRateRef(void)
{
	return &PgCurrentSessionLoggingState()->log_xact_sample_rate_value;
}

char **
PgCurrentBacktraceFunctionsRef(void)
{
	return &PgCurrentSessionLoggingState()->backtrace_functions_value;
}

char **
PgCurrentBacktraceFunctionListRef(void)
{
	return &PgCurrentSessionLoggingState()->backtrace_function_list_value;
}

bool *
PgCurrentAllowSystemTableModsRef(void)
{
	return &PgCurrentSessionMiscGUCState()->allow_system_table_mods_value;
}

int *
PgCurrentMaxStackDepthRef(void)
{
	return &PgCurrentSessionMiscGUCState()->max_stack_depth_kb;
}

ssize_t *
PgCurrentMaxStackDepthBytesRef(void)
{
	return &PgCurrentSessionMiscGUCState()->max_stack_depth_bytes;
}

char **
PgCurrentSessionPreloadLibrariesRef(void)
{
	return &PgCurrentSessionMiscGUCState()->session_preload_libraries_value;
}

char **
PgCurrentLocalPreloadLibrariesRef(void)
{
	return &PgCurrentSessionMiscGUCState()->local_preload_libraries_value;
}

char **
PgCurrentDynamicLibraryPathRef(void)
{
	return &PgCurrentSessionMiscGUCState()->dynamic_library_path_value;
}

char **
PgCurrentExtensionControlPathRef(void)
{
	return &PgCurrentSessionMiscGUCState()->extension_control_path_value;
}

bool *
PgCurrentPgStatTrackCountsRef(void)
{
	return &PgCurrentSessionPgStatState()->track_counts;
}

int *
PgCurrentPgStatTrackFunctionsRef(void)
{
	return &PgCurrentSessionPgStatState()->track_functions;
}

int *
PgCurrentPgStatFetchConsistencyRef(void)
{
	return &PgCurrentSessionPgStatState()->fetch_consistency;
}

bool *
PgCurrentPgStatTrackActivitiesRef(void)
{
	return &PgCurrentSessionPgStatState()->track_activities;
}

SessionEndType *
PgCurrentPgStatSessionEndCauseRef(void)
{
	return &PgCurrentSessionPgStatState()->session_end_cause;
}

PgStat_Counter *
PgCurrentPgStatLastSessionReportTimeRef(void)
{
	return &PgCurrentSessionPgStatState()->last_session_report_time;
}

static PgBackendActivityState *
PgCurrentBackendActivityState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_activity;

	return &CurrentPgBackend->activity;
}

LocalPgBackendStatus **
PgCurrentLocalBackendStatusTableRef(void)
{
	return &PgCurrentBackendActivityState()->backend_status_table;
}

int *
PgCurrentLocalNumBackendsRef(void)
{
	return &PgCurrentBackendActivityState()->num_backends;
}

MemoryContext *
PgCurrentBackendStatusSnapContextRef(void)
{
	return &PgCurrentBackendActivityState()->backend_status_context;
}

static PgBackendMemoryManagerState *
PgCurrentBackendMemoryManagerState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_memory_manager;

	return &CurrentPgBackend->memory_manager;
}

PgBackendAllocSetFreeList *
PgCurrentAllocSetContextFreeLists(void)
{
	return PgCurrentBackendMemoryManagerState()->context_freelists;
}

bool *
PgCurrentLogMemoryContextInProgressRef(void)
{
	return &PgCurrentBackendMemoryManagerState()->log_memory_context_in_progress;
}

static PgBackendUtilityState *
PgCurrentBackendUtilityState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_utility;

	return &CurrentPgBackend->utility;
}

HTAB **
PgCurrentSeqScanTables(void)
{
	return PgCurrentBackendUtilityState()->seq_scan_tables;
}

int *
PgCurrentSeqScanLevels(void)
{
	return PgCurrentBackendUtilityState()->seq_scan_levels;
}

int *
PgCurrentNumSeqScansRef(void)
{
	return &PgCurrentBackendUtilityState()->num_seq_scans;
}

volatile sig_atomic_t *
PgCurrentNotifyInterruptPendingRef(void)
{
	return &PgCurrentBackendUtilityState()->notify_interrupt_pending;
}

bool *
PgCurrentAsyncUnlistenExitRegisteredRef(void)
{
	return &PgCurrentBackendUtilityState()->async_unlisten_exit_registered;
}

struct ExtensionSiblingCache **
PgCurrentExtensionSiblingListRef(void)
{
	return &PgCurrentBackendUtilityState()->extension_sibling_list;
}

HTAB **
PgCurrentInjectionPointCacheRef(void)
{
	return &PgCurrentBackendUtilityState()->injection_point_cache;
}

ReservoirStateData *
PgCurrentSamplingOldReservoirRef(void)
{
	return &PgCurrentBackendUtilityState()->sampling_old_reservoir;
}

bool *
PgCurrentSamplingOldReservoirInitializedRef(void)
{
	return &PgCurrentBackendUtilityState()->sampling_old_reservoir_initialized;
}

Oid *
PgCurrentSuperuserLastRoleIdRef(void)
{
	return &PgCurrentBackendUtilityState()->superuser_last_roleid;
}

bool *
PgCurrentSuperuserLastRoleIdIsSuperRef(void)
{
	return &PgCurrentBackendUtilityState()->superuser_last_roleid_is_super;
}

bool *
PgCurrentSuperuserRoleIdCallbackRegisteredRef(void)
{
	return &PgCurrentBackendUtilityState()->superuser_roleid_callback_registered;
}

void **
PgCurrentResourceReleaseCallbacksRef(void)
{
	return &PgCurrentBackendUtilityState()->resource_release_callbacks;
}

#ifdef RESOWNER_STATS
int *
PgCurrentResourceOwnerArrayLookupsRef(void)
{
	return &PgCurrentBackendUtilityState()->resource_owner_array_lookups;
}

int *
PgCurrentResourceOwnerHashLookupsRef(void)
{
	return &PgCurrentBackendUtilityState()->resource_owner_hash_lookups;
}
#endif

const void **
PgCurrentDateTokenCache(void)
{
	return PgCurrentBackendUtilityState()->date_cache;
}

const void **
PgCurrentDeltaTokenCache(void)
{
	return PgCurrentBackendUtilityState()->delta_cache;
}

bool *
PgCurrentDegreeConstsSetRef(void)
{
	return &PgCurrentBackendUtilityState()->degree_consts_set;
}

float8 *
PgCurrentDegreeSin30Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_sin_30;
}

float8 *
PgCurrentDegreeOneMinusCos60Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_one_minus_cos_60;
}

float8 *
PgCurrentDegreeAsin05Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_asin_0_5;
}

float8 *
PgCurrentDegreeAcos05Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_acos_0_5;
}

float8 *
PgCurrentDegreeAtan10Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_atan_1_0;
}

float8 *
PgCurrentDegreeTan45Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_tan_45;
}

float8 *
PgCurrentDegreeCot45Ref(void)
{
	return &PgCurrentBackendUtilityState()->degree_cot_45;
}

void **
PgCurrentDCHCache(void)
{
	return PgCurrentBackendUtilityState()->dch_cache;
}

int *
PgCurrentNumDCHCacheRef(void)
{
	return &PgCurrentBackendUtilityState()->n_dch_cache;
}

int *
PgCurrentDCHCounterRef(void)
{
	return &PgCurrentBackendUtilityState()->dch_counter;
}

void **
PgCurrentNUMCache(void)
{
	return PgCurrentBackendUtilityState()->num_cache;
}

int *
PgCurrentNumNUMCacheRef(void)
{
	return &PgCurrentBackendUtilityState()->n_num_cache;
}

int *
PgCurrentNUMCounterRef(void)
{
	return &PgCurrentBackendUtilityState()->num_counter;
}

MemoryContext *
PgCurrentLibxmlContextRef(void)
{
	return &PgCurrentBackendUtilityState()->libxml_context;
}

HTAB **
PgCurrentMissingAttrCacheRef(void)
{
	return &PgCurrentBackendUtilityState()->missing_attr_cache;
}

static PgBackendParallelState *
PgCurrentBackendParallelState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_parallel;

	return &CurrentPgBackend->parallel;
}

int *
PgCurrentParallelWorkerNumberRef(void)
{
	return &PgCurrentBackendParallelState()->worker_number;
}

volatile sig_atomic_t *
PgCurrentParallelMessagePendingRef(void)
{
	return &PgCurrentBackendParallelState()->message_pending;
}

bool *
PgCurrentInitializingParallelWorkerRef(void)
{
	return &PgCurrentBackendParallelState()->initializing_worker;
}

void **
PgCurrentFixedParallelStateRef(void)
{
	return &PgCurrentBackendParallelState()->fixed_parallel_state;
}

dlist_head *
PgCurrentParallelContextListRef(void)
{
	return &PgCurrentBackendParallelState()->context_list;
}

bool *
PgCurrentParallelContextListInitializedRef(void)
{
	return &PgCurrentBackendParallelState()->context_list_initialized;
}

pid_t *
PgCurrentParallelLeaderPidRef(void)
{
	return &PgCurrentBackendParallelState()->leader_pid;
}

void **
PgCurrentPqMqHandleRef(void)
{
	return &PgCurrentBackendParallelState()->pq_mq_handle;
}

bool *
PgCurrentPqMqBusyRef(void)
{
	return &PgCurrentBackendParallelState()->pq_mq_busy;
}

pid_t *
PgCurrentPqMqParallelLeaderPidRef(void)
{
	return &PgCurrentBackendParallelState()->pq_mq_parallel_leader_pid;
}

ProcNumber *
PgCurrentPqMqParallelLeaderProcNumberRef(void)
{
	return &PgCurrentBackendParallelState()->pq_mq_parallel_leader_proc_number;
}

int *
PgCurrentComputeQueryIdRef(void)
{
	return &PgCurrentSessionQueryIdState()->compute_query_id_value;
}

bool *
PgCurrentQueryIdEnabledRef(void)
{
	return &PgCurrentSessionQueryIdState()->query_id_enabled_value;
}

bool *
PgCurrentIgnoreChecksumFailureRef(void)
{
	return &PgCurrentSessionStorageGUCState()->ignore_checksum_failure_value;
}

int *
PgCurrentFileCopyMethodRef(void)
{
	return &PgCurrentSessionStorageGUCState()->file_copy_method_value;
}

int *
PgCurrentPasswordEncryptionRef(void)
{
	return &PgCurrentSessionUserGUCState()->password_encryption_value;
}

char **
PgCurrentCreateRoleSelfGrantRef(void)
{
	return &PgCurrentSessionUserGUCState()->createrole_self_grant_value;
}

bool *
PgCurrentCreateRoleSelfGrantEnabledRef(void)
{
	return &PgCurrentSessionUserGUCState()->createrole_self_grant_enabled;
}

unsigned *
PgCurrentCreateRoleSelfGrantOptionsSpecifiedRef(void)
{
	return &PgCurrentSessionUserGUCState()->createrole_self_grant_options_specified;
}

bool *
PgCurrentCreateRoleSelfGrantOptionsAdminRef(void)
{
	return &PgCurrentSessionUserGUCState()->createrole_self_grant_options_admin;
}

bool *
PgCurrentCreateRoleSelfGrantOptionsInheritRef(void)
{
	return &PgCurrentSessionUserGUCState()->createrole_self_grant_options_inherit;
}

bool *
PgCurrentCreateRoleSelfGrantOptionsSetRef(void)
{
	return &PgCurrentSessionUserGUCState()->createrole_self_grant_options_set;
}

int *
PgCurrentSessionReplicationRoleRef(void)
{
	return &PgCurrentSessionCommandGUCState()->session_replication_role_value;
}

bool *
PgCurrentEventTriggersRef(void)
{
	return &PgCurrentSessionCommandGUCState()->event_triggers_value;
}

bool *
PgCurrentTraceNotifyRef(void)
{
	return &PgCurrentSessionCommandGUCState()->trace_notify_value;
}

int *
PgCurrentWalSenderTimeoutRef(void)
{
	return &PgCurrentSessionReplicationGUCState()->wal_sender_timeout_ms;
}

int *
PgCurrentWalSenderShutdownTimeoutRef(void)
{
	return &PgCurrentSessionReplicationGUCState()->wal_sender_shutdown_timeout_ms;
}

bool *
PgCurrentLogReplicationCommandsRef(void)
{
	return &PgCurrentSessionReplicationGUCState()->log_replication_commands_value;
}

int *
PgCurrentWalReceiverTimeoutRef(void)
{
	return &PgCurrentSessionReplicationGUCState()->wal_receiver_timeout_ms;
}

int *
PgCurrentLogicalDecodingWorkMemRef(void)
{
	return &PgCurrentSessionReplicationGUCState()->logical_decoding_work_mem_kb;
}

int *
PgCurrentDebugLogicalReplicationStreamingRef(void)
{
	PgSessionReplicationGUCState *replication_guc;

	replication_guc = PgCurrentSessionReplicationGUCState();
	return &replication_guc->debug_logical_replication_streaming_value;
}

bool *
PgCurrentAllowAlterSystemRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->allow_alter_system_value;
}

bool *
PgCurrentRowSecurityRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->row_security_value;
}

bool *
PgCurrentCheckFunctionBodiesRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->check_function_bodies_value;
}

bool *
PgCurrentCurrentRoleIsSuperuserRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->current_role_is_superuser_value;
}

int *
PgCurrentTempFileLimitRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->temp_file_limit_kb;
}

int *
PgCurrentNumTempBuffersRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->num_temp_buffers_blocks;
}

char **
PgCurrentRoleStringRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->role_string_value;
}

bool *
PgCurrentLoCompatPrivilegesRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->lo_compat_privileges_value;
}

int *
PgCurrentExtraFloatDigitsRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->extra_float_digits_value;
}

bool *
PgCurrentArrayNullsRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->array_nulls_value;
}

int *
PgCurrentByteaOutputRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->bytea_output_value;
}

int *
PgCurrentXmlBinaryRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->xmlbinary_value;
}

int *
PgCurrentXmlOptionRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->xmloption_value;
}

bool *
PgCurrentQuoteAllIdentifiersRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->quote_all_identifiers_value;
}

int *
PgCurrentPlanCacheModeRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->plan_cache_mode_value;
}

int *
PgCurrentGinFuzzySearchLimitRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->gin_fuzzy_search_limit_value;
}

int *
PgCurrentGinPendingListLimitRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->gin_pending_list_limit_value;
}

char **
PgCurrentDefaultTableAccessMethodRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->default_table_access_method_value;
}

bool *
PgCurrentSynchronizeSeqscansRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->synchronize_seqscans_value;
}

int *
PgCurrentDefaultToastCompressionRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->default_toast_compression_value;
}

int *
PgCurrentWalCompressionRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->wal_compression_value;
}

bool *
PgCurrentWalInitZeroRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->wal_init_zero_value;
}

bool *
PgCurrentWalRecycleRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->wal_recycle_value;
}

char **
PgCurrentWalConsistencyCheckingStringRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->wal_consistency_checking_string_value;
}

bool **
PgCurrentWalConsistencyCheckingRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->wal_consistency_checking_value;
}

int *
PgCurrentCommitDelayRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->commit_delay_us;
}

int *
PgCurrentCommitSiblingsRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->commit_siblings_value;
}

bool *
PgCurrentTrackWalIoTimingRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->track_wal_io_timing_value;
}

int *
PgCurrentWalSkipThresholdRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->wal_skip_threshold_kb;
}

#ifdef WAL_DEBUG
bool *
PgCurrentXLogDebugRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->xlog_debug_value;
}
#endif

#ifdef TRACE_SYNCSCAN
bool *
PgCurrentTraceSyncscanRef(void)
{
	return &PgCurrentSessionAccessWalGUCState()->trace_syncscan_value;
}
#endif

bool *
PgCurrentJitEnabledRef(void)
{
	return &PgCurrentSessionJitGUCState()->jit_enabled_value;
}

char **
PgCurrentJitProviderRef(void)
{
	return &PgCurrentSessionJitGUCState()->jit_provider_value;
}

bool *
PgCurrentJitDebuggingSupportRef(void)
{
	return &PgCurrentSessionJitGUCState()->jit_debugging_support_value;
}

bool *
PgCurrentJitDumpBitcodeRef(void)
{
	return &PgCurrentSessionJitGUCState()->jit_dump_bitcode_value;
}

bool *
PgCurrentJitExpressionsRef(void)
{
	return &PgCurrentSessionJitGUCState()->jit_expressions_value;
}

bool *
PgCurrentJitProfilingSupportRef(void)
{
	return &PgCurrentSessionJitGUCState()->jit_profiling_support_value;
}

bool *
PgCurrentJitTupleDeformingRef(void)
{
	return &PgCurrentSessionJitGUCState()->jit_tuple_deforming_value;
}

double *
PgCurrentJitAboveCostRef(void)
{
	return &PgCurrentSessionJitGUCState()->jit_above_cost_value;
}

double *
PgCurrentJitInlineAboveCostRef(void)
{
	return &PgCurrentSessionJitGUCState()->jit_inline_above_cost_value;
}

double *
PgCurrentJitOptimizeAboveCostRef(void)
{
	return &PgCurrentSessionJitGUCState()->jit_optimize_above_cost_value;
}

bool *
PgCurrentTraceSortRef(void)
{
	return &PgCurrentSessionSortGUCState()->trace_sort_value;
}

#ifdef DEBUG_BOUNDED_SORT
bool *
PgCurrentOptimizeBoundedSortRef(void)
{
	return &PgCurrentSessionSortGUCState()->optimize_bounded_sort_value;
}
#endif

int *
PgCurrentWorkMemRef(void)
{
	return &PgCurrentSessionQueryMemoryState()->work_mem_kb;
}

double *
PgCurrentHashMemMultiplierRef(void)
{
	return &PgCurrentSessionQueryMemoryState()->hash_mem_multiplier_value;
}

int *
PgCurrentMaintenanceWorkMemRef(void)
{
	return &PgCurrentSessionQueryMemoryState()->maintenance_work_mem_kb;
}

int *
PgCurrentMaxParallelMaintenanceWorkersRef(void)
{
	return &PgCurrentSessionQueryMemoryState()->max_parallel_maintenance_workers_value;
}

double *
PgCurrentSeqPageCostRef(void)
{
	return &PgCurrentSessionPlannerCostState()->seq_page_cost_value;
}

double *
PgCurrentRandomPageCostRef(void)
{
	return &PgCurrentSessionPlannerCostState()->random_page_cost_value;
}

double *
PgCurrentCpuTupleCostRef(void)
{
	return &PgCurrentSessionPlannerCostState()->cpu_tuple_cost_value;
}

double *
PgCurrentCpuIndexTupleCostRef(void)
{
	return &PgCurrentSessionPlannerCostState()->cpu_index_tuple_cost_value;
}

double *
PgCurrentCpuOperatorCostRef(void)
{
	return &PgCurrentSessionPlannerCostState()->cpu_operator_cost_value;
}

double *
PgCurrentParallelTupleCostRef(void)
{
	return &PgCurrentSessionPlannerCostState()->parallel_tuple_cost_value;
}

double *
PgCurrentParallelSetupCostRef(void)
{
	return &PgCurrentSessionPlannerCostState()->parallel_setup_cost_value;
}

double *
PgCurrentRecursiveWorktableFactorRef(void)
{
	return &PgCurrentSessionPlannerCostState()->recursive_worktable_factor_value;
}

int *
PgCurrentEffectiveCacheSizeRef(void)
{
	return &PgCurrentSessionPlannerCostState()->effective_cache_size_pages;
}

Cost *
PgCurrentDisableCostRef(void)
{
	return (Cost *) &PgCurrentSessionPlannerCostState()->disable_cost_value;
}

int *
PgCurrentMaxParallelWorkersPerGatherRef(void)
{
	return &PgCurrentSessionPlannerCostState()->max_parallel_workers_per_gather_value;
}

int *
PgCurrentDebugParallelQueryRef(void)
{
	return &PgCurrentSessionPlannerCostState()->debug_parallel_query_value;
}

bool *
PgCurrentParallelLeaderParticipationRef(void)
{
	return &PgCurrentSessionPlannerCostState()->parallel_leader_participation_value;
}

bool *
PgCurrentEnableSeqscanRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_seqscan_value;
}

bool *
PgCurrentEnableIndexscanRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_indexscan_value;
}

bool *
PgCurrentEnableIndexonlyscanRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_indexonlyscan_value;
}

bool *
PgCurrentEnableBitmapscanRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_bitmapscan_value;
}

bool *
PgCurrentEnableTidscanRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_tidscan_value;
}

bool *
PgCurrentEnableSortRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_sort_value;
}

bool *
PgCurrentEnableIncrementalSortRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_incremental_sort_value;
}

bool *
PgCurrentEnableHashaggRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_hashagg_value;
}

bool *
PgCurrentEnableNestloopRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_nestloop_value;
}

bool *
PgCurrentEnableMaterialRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_material_value;
}

bool *
PgCurrentEnableMemoizeRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_memoize_value;
}

bool *
PgCurrentEnableMergejoinRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_mergejoin_value;
}

bool *
PgCurrentEnableHashjoinRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_hashjoin_value;
}

bool *
PgCurrentEnableGathermergeRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_gathermerge_value;
}

bool *
PgCurrentEnablePartitionwiseJoinRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_partitionwise_join_value;
}

bool *
PgCurrentEnablePartitionwiseAggregateRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_partitionwise_aggregate_value;
}

bool *
PgCurrentEnableParallelAppendRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_parallel_append_value;
}

bool *
PgCurrentEnableParallelHashRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_parallel_hash_value;
}

bool *
PgCurrentEnablePartitionPruningRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_partition_pruning_value;
}

bool *
PgCurrentEnablePresortedAggregateRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_presorted_aggregate_value;
}

bool *
PgCurrentEnableAsyncAppendRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_async_append_value;
}

bool *
PgCurrentEnableDistinctReorderingRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_distinct_reordering_value;
}

bool *
PgCurrentEnableGeqoRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_geqo_value;
}

bool *
PgCurrentEnableEagerAggregateRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_eager_aggregate_value;
}

bool *
PgCurrentEnableGroupByReorderingRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_group_by_reordering_value;
}

bool *
PgCurrentEnableSelfJoinEliminationRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->enable_self_join_elimination_value;
}

double *
PgCurrentCursorTupleFractionRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->cursor_tuple_fraction_value;
}

int *
PgCurrentConstraintExclusionRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->constraint_exclusion_value;
}

int *
PgCurrentGeqoThresholdRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->geqo_threshold_value;
}

int *
PgCurrentGeqoEffortRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->Geqo_effort_value;
}

int *
PgCurrentGeqoPoolSizeRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->Geqo_pool_size_value;
}

int *
PgCurrentGeqoGenerationsRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->Geqo_generations_value;
}

double *
PgCurrentGeqoSelectionBiasRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->Geqo_selection_bias_value;
}

double *
PgCurrentGeqoSeedRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->Geqo_seed_value;
}

int *
PgCurrentGeqoPlannerExtensionIdRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->Geqo_planner_extension_id_value;
}

double *
PgCurrentMinEagerAggGroupSizeRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->min_eager_agg_group_size_value;
}

int *
PgCurrentMinParallelTableScanSizeRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->min_parallel_table_scan_size_blocks;
}

int *
PgCurrentMinParallelIndexScanSizeRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->min_parallel_index_scan_size_blocks;
}

int *
PgCurrentFromCollapseLimitRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->from_collapse_limit_value;
}

int *
PgCurrentJoinCollapseLimitRef(void)
{
	return &PgCurrentSessionPlannerMethodState()->join_collapse_limit_value;
}

struct Port **
PgConnectionProcPortRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_identity.port;

	return &connection->identity.port;
}

struct Port **
PgCurrentProcPortRef(void)
{
	return PgConnectionProcPortRef(CurrentPgConnection);
}

uint8 *
PgConnectionCancelKey(PgConnection *connection)
{
	if (connection == NULL)
		return early_connection_identity.cancel_key;

	return connection->identity.cancel_key;
}

uint8 *
PgCurrentCancelKey(void)
{
	return PgConnectionCancelKey(CurrentPgConnection);
}

int *
PgConnectionCancelKeyLengthRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_identity.cancel_key_length;

	return &connection->identity.cancel_key_length;
}

int *
PgCurrentCancelKeyLengthRef(void)
{
	return PgConnectionCancelKeyLengthRef(CurrentPgConnection);
}

const char **
PgExecutionDebugQueryStringRef(PgExecution *execution)
{
	if (execution == NULL)
		return &early_execution_debug.debug_query_string;

	return &execution->debug.debug_query_string;
}

const char **
PgCurrentDebugQueryStringRef(void)
{
	return PgExecutionDebugQueryStringRef(CurrentPgExecution);
}

static PgExecutionErrorState *
PgCurrentExecutionErrorState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_error;

	return &CurrentPgExecution->error;
}

ErrorContextCallback **
PgCurrentErrorContextStackRef(void)
{
	return &PgCurrentExecutionErrorState()->context_stack;
}

sigjmp_buf **
PgCurrentExceptionStackRef(void)
{
	return &PgCurrentExecutionErrorState()->exception_stack;
}

ErrorData *
PgCurrentErrorDataArray(void)
{
	return PgCurrentExecutionErrorState()->errordata;
}

int *
PgCurrentErrorDataStackDepthRef(void)
{
	return &PgCurrentExecutionErrorState()->errordata_stack_depth;
}

int *
PgCurrentErrorRecursionDepthRef(void)
{
	return &PgCurrentExecutionErrorState()->recursion_depth;
}

struct timeval *
PgCurrentSavedTimevalRef(void)
{
	return &PgCurrentExecutionErrorState()->saved_timeval;
}

bool *
PgCurrentSavedTimevalSetRef(void)
{
	return &PgCurrentExecutionErrorState()->saved_timeval_set;
}

char *
PgCurrentFormattedLogTime(void)
{
	return PgCurrentExecutionErrorState()->formatted_log_time;
}

static PgExecutionMemoryContextState *
PgCurrentExecutionMemoryContexts(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_memory_contexts;

	return &CurrentPgExecution->memory_contexts;
}

MemoryContext *
PgCurrentMemoryContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->current_context;
}

bool *
PgCurrentDoingCommandReadRef(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_loop_state.doing_command_read;

	return &CurrentPgSession->loop_state.doing_command_read;
}

MemoryContext *
PgErrorContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->error_context;
}

MemoryContext *
PgMessageContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->message_context;
}

MemoryContext *
PgTopTransactionContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->top_transaction_context;
}

MemoryContext *
PgCurTransactionContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->cur_transaction_context;
}

MemoryContext *
PgPortalContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->portal_context;
}

static PgExecutionResourceOwnerState *
PgCurrentExecutionResourceOwners(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_resource_owners;

	return &CurrentPgExecution->resource_owners;
}

ResourceOwner *
PgCurrentResourceOwnerRef(void)
{
	return &PgCurrentExecutionResourceOwners()->current_owner;
}

ResourceOwner *
PgCurTransactionResourceOwnerRef(void)
{
	return &PgCurrentExecutionResourceOwners()->cur_transaction_owner;
}

ResourceOwner *
PgTopTransactionResourceOwnerRef(void)
{
	return &PgCurrentExecutionResourceOwners()->top_transaction_owner;
}

static PgExecutionSPIState *
PgCurrentExecutionSPIState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_spi;

	return &CurrentPgExecution->spi;
}

uint64 *
PgCurrentSPIProcessedRef(void)
{
	return &PgCurrentExecutionSPIState()->processed;
}

SPITupleTable **
PgCurrentSPITuptableRef(void)
{
	return &PgCurrentExecutionSPIState()->tuptable;
}

int *
PgCurrentSPIResultRef(void)
{
	return &PgCurrentExecutionSPIState()->result;
}

_SPI_connection **
PgCurrentSPIStackRef(void)
{
	return &PgCurrentExecutionSPIState()->stack;
}

_SPI_connection **
PgCurrentSPICurrentRef(void)
{
	return &PgCurrentExecutionSPIState()->current;
}

int *
PgCurrentSPIStackDepthRef(void)
{
	return &PgCurrentExecutionSPIState()->stack_depth;
}

int *
PgCurrentSPIConnectedRef(void)
{
	return &PgCurrentExecutionSPIState()->connected;
}

static PgExecutionPortalState *
PgCurrentExecutionPortalState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_portal;

	return &CurrentPgExecution->portal;
}

Portal *
PgCurrentActivePortalRef(void)
{
	return &PgCurrentExecutionPortalState()->active;
}

static PgExecutionVacuumState *
PgCurrentExecutionVacuumState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_vacuum;

	return &CurrentPgExecution->vacuum;
}

bool *
PgCurrentVacuumInProgressRef(void)
{
	return &PgCurrentExecutionVacuumState()->in_vacuum;
}

int *
PgCurrentVacuumCostBalanceRef(void)
{
	return &PgCurrentExecutionVacuumState()->cost_balance;
}

bool *
PgCurrentVacuumCostActiveRef(void)
{
	return &PgCurrentExecutionVacuumState()->cost_active;
}

pg_atomic_uint32 **
PgCurrentVacuumSharedCostBalanceRef(void)
{
	return &PgCurrentExecutionVacuumState()->shared_cost_balance;
}

pg_atomic_uint32 **
PgCurrentVacuumActiveNWorkersRef(void)
{
	return &PgCurrentExecutionVacuumState()->active_nworkers;
}

int *
PgCurrentVacuumCostBalanceLocalRef(void)
{
	return &PgCurrentExecutionVacuumState()->cost_balance_local;
}

bool *
PgCurrentVacuumFailsafeActiveRef(void)
{
	return &PgCurrentExecutionVacuumState()->failsafe_active;
}

int64 *
PgCurrentParallelVacuumWorkerDelayNsRef(void)
{
	return &PgCurrentExecutionVacuumState()->parallel_worker_delay_ns;
}

void **
PgCurrentParallelVacuumSharedCostParamsRef(void)
{
	return &PgCurrentExecutionVacuumState()->parallel_shared_cost_params;
}

uint32 *
PgCurrentParallelVacuumSharedParamsGenerationLocalRef(void)
{
	return &PgCurrentExecutionVacuumState()->parallel_shared_params_generation_local;
}

static PgExecutionNodeIOState *
PgCurrentExecutionNodeIOState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_node_io;

	return &CurrentPgExecution->node_io;
}

bool *
PgCurrentNodeWriteLocationFieldsRef(void)
{
	return &PgCurrentExecutionNodeIOState()->write_location_fields;
}

const char **
PgCurrentNodeReadStrtokPtrRef(void)
{
	return &PgCurrentExecutionNodeIOState()->strtok_ptr;
}

bool *
PgCurrentNodeRestoreLocationFieldsRef(void)
{
	return &PgCurrentExecutionNodeIOState()->restore_location_fields;
}

static PgExecutionBaseBackupState *
PgCurrentExecutionBaseBackupState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_basebackup;

	return &CurrentPgExecution->basebackup;
}

bool *
PgCurrentBaseBackupStartedInRecoveryRef(void)
{
	return &PgCurrentExecutionBaseBackupState()->backup_started_in_recovery;
}

long long int *
PgCurrentBaseBackupTotalChecksumFailuresRef(void)
{
	return &PgCurrentExecutionBaseBackupState()->total_checksum_failures;
}

bool *
PgCurrentBaseBackupNoVerifyChecksumsRef(void)
{
	return &PgCurrentExecutionBaseBackupState()->noverify_checksums;
}

static PgExecutionAnalyzeState *
PgCurrentExecutionAnalyzeState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_analyze;

	return &CurrentPgExecution->analyze;
}

MemoryContext *
PgCurrentAnalyzeContextRef(void)
{
	return &PgCurrentExecutionAnalyzeState()->context;
}

BufferAccessStrategy *
PgCurrentAnalyzeStrategyRef(void)
{
	return &PgCurrentExecutionAnalyzeState()->strategy;
}

void **
PgCurrentArrayAnalyzeExtraDataRef(void)
{
	return &PgCurrentExecutionAnalyzeState()->array_extra_data;
}

static PgExecutionExtensionState *
PgCurrentExecutionExtensionState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_extension;

	return &CurrentPgExecution->extension;
}

bool *
PgCurrentCreatingExtensionRef(void)
{
	return &PgCurrentExecutionExtensionState()->creating;
}

Oid *
PgCurrentExtensionObjectRef(void)
{
	return &PgCurrentExecutionExtensionState()->current_object;
}

static PgExecutionMatViewState *
PgCurrentExecutionMatViewState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_matview;

	return &CurrentPgExecution->matview;
}

int *
PgCurrentMatViewMaintenanceDepthRef(void)
{
	return &PgCurrentExecutionMatViewState()->maintenance_depth;
}

static PgExecutionSnapshotState *
PgCurrentExecutionSnapshotState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_snapshot;

	return &CurrentPgExecution->snapshot;
}

SnapshotData *
PgCurrentSnapshotDataRef(void)
{
	return &PgCurrentExecutionSnapshotState()->current_snapshot_data;
}

SnapshotData *
PgCurrentSecondarySnapshotDataRef(void)
{
	return &PgCurrentExecutionSnapshotState()->secondary_snapshot_data;
}

SnapshotData *
PgCurrentCatalogSnapshotDataRef(void)
{
	return &PgCurrentExecutionSnapshotState()->catalog_snapshot_data;
}

Snapshot *
PgCurrentSnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->current_snapshot;
}

Snapshot *
PgCurrentSecondarySnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->secondary_snapshot;
}

Snapshot *
PgCurrentCatalogSnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->catalog_snapshot;
}

Snapshot *
PgCurrentHistoricSnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->historic_snapshot;
}

TransactionId *
PgCurrentTransactionXminRef(void)
{
	return &PgCurrentExecutionSnapshotState()->transaction_xmin;
}

TransactionId *
PgCurrentRecentXminRef(void)
{
	return &PgCurrentExecutionSnapshotState()->recent_xmin;
}

HTAB **
PgCurrentTupleCidDataRef(void)
{
	return &PgCurrentExecutionSnapshotState()->tuplecid_data;
}

void **
PgCurrentActiveSnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->active_snapshot;
}

pairingheap *
PgCurrentRegisteredSnapshotsRef(void)
{
	return &PgCurrentExecutionSnapshotState()->registered_snapshots;
}

bool *
PgCurrentFirstSnapshotSetRef(void)
{
	return &PgCurrentExecutionSnapshotState()->first_snapshot_set;
}

Snapshot *
PgCurrentFirstXactSnapshotRef(void)
{
	return &PgCurrentExecutionSnapshotState()->first_xact_snapshot;
}

List **
PgCurrentExportedSnapshotsRef(void)
{
	return &PgCurrentExecutionSnapshotState()->exported_snapshots;
}

static PgExecutionComboCidState *
PgCurrentExecutionComboCidState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_combo_cid;

	return &CurrentPgExecution->combo_cid;
}

HTAB **
PgCurrentComboCidHashRef(void)
{
	return &PgCurrentExecutionComboCidState()->hash;
}

void **
PgCurrentComboCidsRef(void)
{
	return &PgCurrentExecutionComboCidState()->cids;
}

int *
PgCurrentUsedComboCidsRef(void)
{
	return &PgCurrentExecutionComboCidState()->used;
}

int *
PgCurrentSizeComboCidsRef(void)
{
	return &PgCurrentExecutionComboCidState()->size;
}

static PgExecutionXLogInsertState *
PgCurrentExecutionXLogInsertState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_xloginsert;

	return &CurrentPgExecution->xloginsert;
}

void **
PgCurrentXLogInsertRegisteredBuffersRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->registered_buffers;
}

int *
PgCurrentXLogInsertMaxRegisteredBuffersRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->max_registered_buffers;
}

int *
PgCurrentXLogInsertMaxRegisteredBlockIdRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->max_registered_block_id;
}

XLogRecData **
PgCurrentXLogInsertMainRDataHeadRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->mainrdata_head;
}

XLogRecData **
PgCurrentXLogInsertMainRDataLastRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->mainrdata_last;
}

uint64 *
PgCurrentXLogInsertMainRDataLenRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->mainrdata_len;
}

uint8 *
PgCurrentXLogInsertFlagsRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->curinsert_flags;
}

XLogRecData *
PgCurrentXLogInsertHeaderRecordDataRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->hdr_rdt;
}

char **
PgCurrentXLogInsertHeaderScratchRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->hdr_scratch;
}

XLogRecData **
PgCurrentXLogInsertRDatasRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->rdatas;
}

int *
PgCurrentXLogInsertNumRDatasRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->num_rdatas;
}

int *
PgCurrentXLogInsertMaxRDatasRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->max_rdatas;
}

bool *
PgCurrentXLogInsertBeginCalledRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->begininsert_called;
}

MemoryContext *
PgCurrentXLogInsertContextRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->context;
}

static PgExecutionXactState *
PgCurrentExecutionXactState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_xact;

	return &CurrentPgExecution->xact;
}

int *
PgCurrentXactIsoLevelRef(void)
{
	return &PgCurrentExecutionXactState()->iso_level;
}

bool *
PgCurrentXactReadOnlyRef(void)
{
	return &PgCurrentExecutionXactState()->read_only;
}

bool *
PgCurrentXactDeferrableRef(void)
{
	return &PgCurrentExecutionXactState()->deferrable;
}

bool *
PgCurrentXactIsSampledRef(void)
{
	return &PgCurrentExecutionXactState()->is_sampled;
}

TransactionId *
PgCurrentCheckXidAliveRef(void)
{
	return &PgCurrentExecutionXactState()->check_xid_alive;
}

bool *
PgCurrentBSysScanRef(void)
{
	return &PgCurrentExecutionXactState()->bsysscan_value;
}

int *
PgCurrentMyXactFlagsRef(void)
{
	return &PgCurrentExecutionXactState()->flags;
}

FullTransactionId *
PgCurrentXactTopFullTransactionIdRef(void)
{
	return &PgCurrentExecutionXactState()->top_full_transaction_id;
}

int *
PgCurrentNParallelCurrentXidsRef(void)
{
	return &PgCurrentExecutionXactState()->n_parallel_current_xids;
}

TransactionId **
PgCurrentParallelCurrentXidsRef(void)
{
	return &PgCurrentExecutionXactState()->parallel_current_xids;
}

int *
PgCurrentNUnreportedXidsRef(void)
{
	return &PgCurrentExecutionXactState()->n_unreported_xids;
}

TransactionId *
PgCurrentUnreportedXids(void)
{
	return PgCurrentExecutionXactState()->unreported_xids;
}

SubTransactionId *
PgCurrentSubTransactionIdCounterRef(void)
{
	return &PgCurrentExecutionXactState()->current_sub_transaction_id;
}

CommandId *
PgCurrentCommandIdCounterRef(void)
{
	return &PgCurrentExecutionXactState()->current_command_id;
}

bool *
PgCurrentCommandIdUsedRef(void)
{
	return &PgCurrentExecutionXactState()->current_command_id_used;
}

TimestampTz *
PgCurrentXactStartTimestampRef(void)
{
	return &PgCurrentExecutionXactState()->xact_start_timestamp;
}

TimestampTz *
PgCurrentStmtStartTimestampRef(void)
{
	return &PgCurrentExecutionXactState()->stmt_start_timestamp;
}

TimestampTz *
PgCurrentXactStopTimestampRef(void)
{
	return &PgCurrentExecutionXactState()->xact_stop_timestamp;
}

char **
PgCurrentPrepareGIDRef(void)
{
	return &PgCurrentExecutionXactState()->prepare_gid;
}

bool *
PgCurrentForceSyncCommitRef(void)
{
	return &PgCurrentExecutionXactState()->force_sync_commit;
}

MemoryContext *
PgCurrentTransactionAbortContextRef(void)
{
	return &PgCurrentExecutionXactState()->transaction_abort_context;
}

TransactionStateData **
PgCurrentTopTransactionStateDataRef(void)
{
	return &PgCurrentExecutionXactState()->top_transaction_state_data;
}

TransactionStateData **
PgCurrentTransactionStateRef(void)
{
	return &PgCurrentExecutionXactState()->current_transaction_state;
}

static PgExecutionTransactionCleanupState *
PgCurrentExecutionTransactionCleanupState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_transaction_cleanup;

	return &CurrentPgExecution->transaction_cleanup;
}

LargeObjectDesc ***
PgCurrentLargeObjectCookiesRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->lo_cookies;
}

int *
PgCurrentLargeObjectCookiesSizeRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->lo_cookies_size;
}

bool *
PgCurrentLargeObjectCleanupNeededRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->lo_cleanup_needed;
}

MemoryContext *
PgCurrentLargeObjectContextRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->lo_context;
}

bool *
PgCurrentHaveXactTemporaryFilesRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->have_xact_temporary_files;
}

PgStat_SubXactStatus **
PgCurrentPgStatXactStackRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->pgstat_xact_stack;
}

HTAB **
PgCurrentRIFastPathCacheRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->ri_fastpath_cache;
}

bool *
PgCurrentRIFastPathCallbackRegisteredRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->ri_fastpath_callback_registered;
}

static PgExecutionReplicationScratchState *
PgCurrentExecutionReplicationScratchState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_replication_scratch;

	return &CurrentPgExecution->replication_scratch;
}

EventTriggerQueryState **
PgCurrentEventTriggerQueryStateRef(void)
{
	return &PgCurrentExecutionReplicationScratchState()->event_trigger_query_state;
}

ReplOriginXactState *
PgCurrentReplOriginXactStateRef(void)
{
	return &PgCurrentExecutionReplicationScratchState()->replorigin_xact;
}

ErrorContextCallback **
PgCurrentApplyErrorContextStackRef(void)
{
	return &PgCurrentExecutionReplicationScratchState()->apply_error_context_stack;
}

MemoryContext *
PgCurrentApplyMessageContextRef(void)
{
	return &PgCurrentExecutionReplicationScratchState()->apply_message_context;
}

MemoryContext *
PgCurrentLogicalStreamingContextRef(void)
{
	return &PgCurrentExecutionReplicationScratchState()->logical_streaming_context;
}

static PgExecutionGUCErrorState *
PgCurrentExecutionGUCErrorState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_guc_error;

	return &CurrentPgExecution->guc_error;
}

int *
PgCurrentGUCCheckErrcodeValueRef(void)
{
	return &PgCurrentExecutionGUCErrorState()->check_errcode_value;
}

char **
PgCurrentGUCCheckErrmsgStringRef(void)
{
	return &PgCurrentExecutionGUCErrorState()->check_errmsg_string;
}

char **
PgCurrentGUCCheckErrdetailStringRef(void)
{
	return &PgCurrentExecutionGUCErrorState()->check_errdetail_string;
}

char **
PgCurrentGUCCheckErrhintStringRef(void)
{
	return &PgCurrentExecutionGUCErrorState()->check_errhint_string;
}

int *
PgCurrentFormatErrnumberRef(void)
{
	return &PgCurrentExecutionGUCErrorState()->format_errnumber;
}

const char **
PgCurrentFormatDomainRef(void)
{
	return &PgCurrentExecutionGUCErrorState()->format_domain;
}

unsigned int *
PgCurrentConfigFileLinenoRef(void)
{
	return &PgCurrentExecutionGUCErrorState()->config_file_lineno;
}

const char **
PgCurrentGUCFlexFatalErrmsgRef(void)
{
	return &PgCurrentExecutionGUCErrorState()->flex_fatal_errmsg;
}

sigjmp_buf **
PgCurrentGUCFlexFatalJmpRef(void)
{
	return &PgCurrentExecutionGUCErrorState()->flex_fatal_jmp;
}

static PgExecutionAsyncState *
PgCurrentExecutionAsyncState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_async;

	return &CurrentPgExecution->async;
}

struct ActionList **
PgCurrentPendingActionsRef(void)
{
	return &PgCurrentExecutionAsyncState()->pending_actions;
}

HTAB **
PgCurrentPendingListenActionsRef(void)
{
	return &PgCurrentExecutionAsyncState()->pending_listen_actions;
}

struct NotificationList **
PgCurrentPendingNotifiesRef(void)
{
	return &PgCurrentExecutionAsyncState()->pending_notifies;
}

PgExecutionAsyncQueuePosition *
PgCurrentQueueHeadBeforeWriteRef(void)
{
	return &PgCurrentExecutionAsyncState()->queue_head_before_write;
}

PgExecutionAsyncQueuePosition *
PgCurrentQueueHeadAfterWriteRef(void)
{
	return &PgCurrentExecutionAsyncState()->queue_head_after_write;
}

int32 **
PgCurrentSignalPidsRef(void)
{
	return &PgCurrentExecutionAsyncState()->signal_pids;
}

ProcNumber **
PgCurrentSignalProcnosRef(void)
{
	return &PgCurrentExecutionAsyncState()->signal_procnos;
}

bool *
PgCurrentTryAdvanceTailRef(void)
{
	return &PgCurrentExecutionAsyncState()->try_advance_tail;
}

static PgExecutionCatalogState *
PgCurrentExecutionCatalogState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_catalog;

	return &CurrentPgExecution->catalog;
}

HTAB **
PgCurrentUncommittedEnumTypesRef(void)
{
	return &PgCurrentExecutionCatalogState()->uncommitted_enum_types;
}

HTAB **
PgCurrentUncommittedEnumValuesRef(void)
{
	return &PgCurrentExecutionCatalogState()->uncommitted_enum_values;
}

Oid *
PgCurrentReindexedHeapRef(void)
{
	return &PgCurrentExecutionCatalogState()->currently_reindexed_heap;
}

Oid *
PgCurrentReindexedIndexRef(void)
{
	return &PgCurrentExecutionCatalogState()->currently_reindexed_index;
}

List **
PgCurrentPendingReindexedIndexesRef(void)
{
	return &PgCurrentExecutionCatalogState()->pending_reindexed_indexes;
}

int *
PgCurrentReindexingNestLevelRef(void)
{
	return &PgCurrentExecutionCatalogState()->reindexing_nest_level;
}

struct PendingRelDelete **
PgCurrentPendingRelDeletesRef(void)
{
	return &PgCurrentExecutionCatalogState()->pending_rel_deletes;
}

HTAB **
PgCurrentPendingSyncHashRef(void)
{
	return &PgCurrentExecutionCatalogState()->pending_sync_hash;
}

static PgExecutionCatalogCacheState *
PgCurrentExecutionCatalogCacheState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_catalog_cache;

	return &CurrentPgExecution->catalog_cache;
}

CatCInProgress **
PgCurrentCatCacheInProgressStackRef(void)
{
	return &PgCurrentExecutionCatalogCacheState()->catcache_in_progress_stack;
}

InProgressEnt **
PgCurrentRelcacheInProgressListRef(void)
{
	return &PgCurrentExecutionCatalogCacheState()->relcache_in_progress_list;
}

int *
PgCurrentRelcacheInProgressListLenRef(void)
{
	return &PgCurrentExecutionCatalogCacheState()->relcache_in_progress_list_len;
}

int *
PgCurrentRelcacheInProgressListMaxLenRef(void)
{
	return &PgCurrentExecutionCatalogCacheState()->relcache_in_progress_list_maxlen;
}

Oid *
PgCurrentRelcacheEOXactList(void)
{
	return PgCurrentExecutionCatalogCacheState()->relcache_eoxact_list;
}

int *
PgCurrentRelcacheEOXactListLenRef(void)
{
	return &PgCurrentExecutionCatalogCacheState()->relcache_eoxact_list_len;
}

bool *
PgCurrentRelcacheEOXactListOverflowedRef(void)
{
	return &PgCurrentExecutionCatalogCacheState()->relcache_eoxact_list_overflowed;
}

TupleDesc **
PgCurrentRelcacheEOXactTupleDescArrayRef(void)
{
	return &PgCurrentExecutionCatalogCacheState()->relcache_eoxact_tupledesc_array;
}

int *
PgCurrentRelcacheNextEOXactTupleDescNumRef(void)
{
	return &PgCurrentExecutionCatalogCacheState()->relcache_next_eoxact_tupledesc_num;
}

int *
PgCurrentRelcacheEOXactTupleDescArrayLenRef(void)
{
	return &PgCurrentExecutionCatalogCacheState()->relcache_eoxact_tupledesc_array_len;
}

static PgExecutionRelMapState *
PgCurrentExecutionRelMapState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_relmap;

	return &CurrentPgExecution->relmap;
}

PgExecutionRelMapFile *
PgCurrentRelMapActiveSharedUpdatesRef(void)
{
	return &PgCurrentExecutionRelMapState()->active_shared_updates;
}

PgExecutionRelMapFile *
PgCurrentRelMapActiveLocalUpdatesRef(void)
{
	return &PgCurrentExecutionRelMapState()->active_local_updates;
}

PgExecutionRelMapFile *
PgCurrentRelMapPendingSharedUpdatesRef(void)
{
	return &PgCurrentExecutionRelMapState()->pending_shared_updates;
}

PgExecutionRelMapFile *
PgCurrentRelMapPendingLocalUpdatesRef(void)
{
	return &PgCurrentExecutionRelMapState()->pending_local_updates;
}

static PgExecutionInvalidationState *
PgCurrentExecutionInvalidationState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_invalidation;

	return &CurrentPgExecution->invalidation;
}

PgExecutionInvalMessageArray *
PgCurrentInvalMessageArrays(void)
{
	return PgCurrentExecutionInvalidationState()->message_arrays;
}

struct TransInvalidationInfo **
PgCurrentTransInvalInfoRef(void)
{
	return &PgCurrentExecutionInvalidationState()->trans_info;
}

struct InvalidationInfo **
PgCurrentInplaceInvalInfoRef(void)
{
	return &PgCurrentExecutionInvalidationState()->inplace_info;
}

static PgExecutionTwoPhaseRecordState *
PgCurrentExecutionTwoPhaseRecordState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_two_phase_records;

	return &CurrentPgExecution->two_phase_records;
}

PgExecutionTwoPhaseRecordState *
PgCurrentTwoPhaseRecordStateRef(void)
{
	return PgCurrentExecutionTwoPhaseRecordState();
}

static PgExecutionTriggerState *
PgCurrentExecutionTriggerState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_trigger;

	return &CurrentPgExecution->trigger;
}

int *
PgCurrentTriggerDepthRef(void)
{
	return &PgCurrentExecutionTriggerState()->depth;
}

void **
PgCurrentAfterTriggersDataRef(void)
{
	return &PgCurrentExecutionTriggerState()->after_triggers_data;
}

static PgExecutionRegexState *
PgCurrentExecutionRegexState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_regex;

	return &CurrentPgExecution->regex;
}

void **
PgCurrentRegexLocaleRef(void)
{
	return &PgCurrentExecutionRegexState()->regex_locale;
}

static PgExecutionValgrindState *
PgCurrentExecutionValgrindState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_valgrind;

	return &CurrentPgExecution->valgrind;
}

unsigned int *
PgCurrentValgrindOldErrorCountRef(void)
{
	return &PgCurrentExecutionValgrindState()->old_error_count;
}

static PgExecutionSnapBuildState *
PgCurrentExecutionSnapBuildState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_snapbuild;

	return &CurrentPgExecution->snapbuild;
}

struct ResourceOwnerData **
PgCurrentSnapBuildSavedResourceOwnerDuringExportRef(void)
{
	return &PgCurrentExecutionSnapBuildState()->saved_resource_owner_during_export;
}

bool *
PgCurrentSnapBuildExportInProgressRef(void)
{
	return &PgCurrentExecutionSnapBuildState()->export_in_progress;
}

PgConnectionSocketIOState *
PgConnectionSocketIORef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_socket_io;

	return &connection->socket_io;
}

PgConnectionSocketIOState *
PgCurrentConnectionSocketIORef(void)
{
	return PgConnectionSocketIORef(CurrentPgConnection);
}

const PQcommMethods **
PgConnectionPqCommMethodsRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_protocol.comm_methods;

	return &connection->protocol.comm_methods;
}

const PQcommMethods **
PgCurrentPqCommMethodsRef(void)
{
	return PgConnectionPqCommMethodsRef(CurrentPgConnection);
}

WaitEventSet **
PgConnectionFeBeWaitSetRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_protocol.fe_be_wait_set;

	return &connection->protocol.fe_be_wait_set;
}

WaitEventSet **
PgCurrentFeBeWaitSetRef(void)
{
	return PgConnectionFeBeWaitSetRef(CurrentPgConnection);
}

uint32 *
PgConnectionFrontendProtocolRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_protocol.frontend_protocol;

	return &connection->protocol.frontend_protocol;
}

uint32 *
PgCurrentFrontendProtocolRef(void)
{
	return PgConnectionFrontendProtocolRef(CurrentPgConnection);
}

static CommandDest *
PgConnectionWhereToSendOutputRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_output.where_to_send_output;

	return &connection->output.where_to_send_output;
}

CommandDest *
PgCurrentWhereToSendOutputRef(void)
{
	return PgConnectionWhereToSendOutputRef(CurrentPgConnection);
}

static int *
PgConnectionClientConnectionCheckIntervalRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_output.client_connection_check_interval;

	return &connection->output.client_connection_check_interval;
}

int *
PgCurrentClientConnectionCheckIntervalRef(void)
{
	return PgConnectionClientConnectionCheckIntervalRef(CurrentPgConnection);
}

volatile sig_atomic_t *
PgConnectionCheckClientConnectionPendingRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_interrupts.check_client_connection_pending;

	return &connection->interrupts.check_client_connection_pending;
}

volatile sig_atomic_t *
PgCurrentCheckClientConnectionPendingRef(void)
{
	return PgConnectionCheckClientConnectionPendingRef(CurrentPgConnection);
}

volatile sig_atomic_t *
PgConnectionClientConnectionLostRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_interrupts.client_connection_lost;

	return &connection->interrupts.client_connection_lost;
}

volatile sig_atomic_t *
PgCurrentClientConnectionLostRef(void)
{
	return PgConnectionClientConnectionLostRef(CurrentPgConnection);
}

bool *
PgConnectionClientAuthInProgressRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_startup.client_auth_in_progress;

	return &connection->startup.client_auth_in_progress;
}

bool *
PgCurrentClientAuthInProgressRef(void)
{
	return PgConnectionClientAuthInProgressRef(CurrentPgConnection);
}

struct ClientSocket **
PgConnectionClientSocketRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_startup.client_socket;

	return &connection->startup.client_socket;
}

struct ClientSocket **
PgCurrentClientSocketRef(void)
{
	return PgConnectionClientSocketRef(CurrentPgConnection);
}

static ConnectionTiming *
PgConnectionTimingRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_startup.timing;

	return &connection->startup.timing;
}

ConnectionTiming *
PgCurrentConnectionTimingRef(void)
{
	return PgConnectionTimingRef(CurrentPgConnection);
}

void *
PgConnectionClientConnectionInfoRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_client_connection_info;

	return &connection->client_connection_info;
}

void *
PgCurrentClientConnectionInfoRef(void)
{
	return PgConnectionClientConnectionInfoRef(CurrentPgConnection);
}

PgConnectionSecurityState *
PgConnectionSecurityStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_security;

	return &connection->security;
}

PgConnectionSecurityState *
PgCurrentConnectionSecurityStateRef(void)
{
	return PgConnectionSecurityStateRef(CurrentPgConnection);
}

static PgBackendCoreState *
PgCurrentCoreState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_core;

	return &CurrentPgBackend->core;
}

PGPROC **
PgCurrentMyProcRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_my_proc;

	return &CurrentPgBackend->my_proc;
}

ProcNumber *
PgCurrentMyProcNumberRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_my_proc_number;

	return &CurrentPgBackend->my_proc_number;
}

ProcNumber *
PgCurrentParallelLeaderProcNumberRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_parallel_leader_proc_number;

	return &CurrentPgBackend->parallel_leader_proc_number;
}

PgBackendStatus **
PgCurrentMyBEEntryRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_my_beentry;

	return &CurrentPgBackend->my_beentry;
}

BackgroundWorker **
PgCurrentMyBgworkerEntryRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_my_bgworker_entry;

	return &CurrentPgBackend->my_bgworker_entry;
}

ResourceOwner *
PgCurrentAuxProcessResourceOwnerRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_aux_process_resource_owner;

	return &CurrentPgBackend->aux_process_resource_owner;
}

pg_prng_state *
PgCurrentGlobalPrngStateRef(void)
{
	return &PgCurrentCoreState()->global_prng_state;
}

static PgBackendCommandState *
PgCurrentBackendCommandState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_command;

	return &CurrentPgBackend->command;
}

const char **
PgCurrentUserDOptionRef(void)
{
	return &PgCurrentBackendCommandState()->user_d_option;
}

struct rusage *
PgCurrentUsageSaveRusageRef(void)
{
	return &PgCurrentBackendCommandState()->save_rusage;
}

struct timeval *
PgCurrentUsageSaveTimevalRef(void)
{
	return &PgCurrentBackendCommandState()->save_timeval;
}

static PgBackendLogState *
PgCurrentBackendLogState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_log;

	return &CurrentPgBackend->log_state;
}

char *
PgCurrentFormattedStartTimeBuffer(void)
{
	return PgCurrentBackendLogState()->formatted_start_time;
}

long *
PgCurrentLogLineNumberRef(void)
{
	return &PgCurrentBackendLogState()->line_number;
}

int *
PgCurrentLogLinePidRef(void)
{
	return &PgCurrentBackendLogState()->line_pid;
}

PgBackendExprInterpState *
PgCurrentExprInterpState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_expr_interp;

	return &CurrentPgBackend->expr_interp;
}

bool *
PgCurrentExitOnAnyErrorRef(void)
{
	return &PgCurrentCoreState()->exit_on_any_error;
}

int *
PgCurrentMyProcPidRef(void)
{
	return &PgCurrentCoreState()->proc_pid;
}

pg_time_t *
PgCurrentMyStartTimeRef(void)
{
	return &PgCurrentCoreState()->start_time;
}

TimestampTz *
PgCurrentMyStartTimestampRef(void)
{
	return &PgCurrentCoreState()->start_timestamp;
}

struct Latch **
PgCurrentMyLatchRef(void)
{
	return &PgCurrentCoreState()->latch;
}

int *
PgCurrentMyPMChildSlotRef(void)
{
	return &PgCurrentCoreState()->pm_child_slot;
}

char *
PgCurrentOutputFileNameRef(void)
{
	return PgCurrentCoreState()->output_file_name;
}

static BackendType *
PgCurrentBackendTypeRef(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_type;

	return &CurrentPgBackend->backend_type;
}

BackendType *
PgCurrentMyBackendTypeRef(void)
{
	return PgCurrentBackendTypeRef();
}

ProcessingMode *
PgCurrentProcessingModeRef(void)
{
	return &PgCurrentCoreState()->mode;
}

bool *
PgCurrentIgnoreSystemIndexesRef(void)
{
	return &PgCurrentCoreState()->ignore_system_indexes;
}

static PgBackendPgStatPendingState *
PgCurrentBackendPgStatPendingState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_pgstat_pending;

	return &CurrentPgBackend->pgstat_pending;
}

PgStat_LocalState *
PgCurrentPgStatLocalState(void)
{
	return &PgCurrentBackendPgStatPendingState()->local;
}

PgStat_BgWriterStats *
PgCurrentPendingBgWriterStatsRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->pending_bgwriter;
}

PgStat_CheckpointerStats *
PgCurrentPendingCheckpointerStatsRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->pending_checkpointer;
}

PgStat_PendingIO *
PgCurrentPendingIOStatsRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->io_stats;
}

bool *
PgCurrentHaveIOStatsRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->io_stats_pending;
}

PgStat_SLRUStats *
PgCurrentPendingSLRUStatsArray(void)
{
	return PgCurrentBackendPgStatPendingState()->slru_stats;
}

bool *
PgCurrentHaveSLRUStatsRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->slru_stats_pending;
}

PgStat_PendingLock *
PgCurrentPendingLockStatsRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->lock_stats;
}

bool *
PgCurrentHaveLockStatsRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->lock_stats_pending;
}

PgStat_BackendPending *
PgCurrentPendingBackendStatsRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->backend_stats;
}

bool *
PgCurrentBackendHasIOStatsRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->backend_io_stats_pending;
}

MemoryContext *
PgCurrentPgStatPendingContextRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->pending_context;
}

dlist_head *
PgCurrentPgStatPendingListRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->pending;
}

void **
PgCurrentPgStatEntryRefHashRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->entry_ref_hash;
}

int *
PgCurrentPgStatSharedRefAgeRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->shared_ref_age;
}

MemoryContext *
PgCurrentPgStatSharedRefContextRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->shared_ref_context;
}

MemoryContext *
PgCurrentPgStatEntryRefHashContextRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->entry_ref_hash_context;
}

WalUsage *
PgCurrentPgStatPrevBackendWalUsageRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->backend_wal_prev_usage;
}

bool *
PgCurrentPgStatReportFixedRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->report_fixed;
}

bool *
PgCurrentPgStatForceNextFlushRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->force_next_flush;
}

bool *
PgCurrentForceStatsSnapshotClearRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->force_snapshot_clear;
}

bool *
PgCurrentPgStatIsInitializedRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->is_initialized;
}

bool *
PgCurrentPgStatIsShutdownRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->is_shutdown;
}

int *
PgCurrentPgStatXactCommitRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->xact_commit;
}

int *
PgCurrentPgStatXactRollbackRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->xact_rollback;
}

PgStat_Counter *
PgCurrentPgStatBlockReadTimeRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->block_read_time;
}

PgStat_Counter *
PgCurrentPgStatBlockWriteTimeRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->block_write_time;
}

PgStat_Counter *
PgCurrentPgStatActiveTimeRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->active_time;
}

PgStat_Counter *
PgCurrentPgStatTransactionIdleTimeRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->transaction_idle_time;
}

instr_time *
PgCurrentPgStatTotalFuncTimeRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->func_total_time;
}

WalUsage *
PgCurrentPgStatPrevWalUsageRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->wal_prev_usage;
}

static PgBackendInstrumentationState *
PgCurrentBackendInstrumentationState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_instrumentation;

	return &CurrentPgBackend->instrumentation;
}

BufferUsage *
PgCurrentBufferUsageRef(void)
{
	return &PgCurrentBackendInstrumentationState()->buffer_usage;
}

BufferUsage *
PgCurrentSavedBufferUsageRef(void)
{
	return &PgCurrentBackendInstrumentationState()->saved_buffer_usage;
}

WalUsage *
PgCurrentWalUsageRef(void)
{
	return &PgCurrentBackendInstrumentationState()->wal_usage;
}

WalUsage *
PgCurrentSavedWalUsageRef(void)
{
	return &PgCurrentBackendInstrumentationState()->saved_wal_usage;
}

static PgBackendBufferState *
PgCurrentBackendBufferState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_buffers;

	return &CurrentPgBackend->buffers;
}

static MemoryContext
PgBackendBufferAllocationContext(void)
{
	if (TopMemoryContext != NULL)
		return TopMemoryContext;
	if (CurrentMemoryContext != NULL)
		return CurrentMemoryContext;

	elog(ERROR, "cannot allocate backend buffer state before memory contexts exist");
	return NULL;				/* keep compiler quiet */
}

int *
PgCurrentNLocBufferRef(void)
{
	return &PgCurrentBackendBufferState()->nlocbuffer;
}

void **
PgCurrentLocalBufferDescriptorsRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_descriptors;
}

void **
PgCurrentLocalBufferBlockPointersRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_block_pointers;
}

int32 **
PgCurrentLocalRefCountRef(void)
{
	return &PgCurrentBackendBufferState()->local_ref_count;
}

int *
PgCurrentNextFreeLocalBufIdRef(void)
{
	return &PgCurrentBackendBufferState()->next_free_local_buf_id;
}

HTAB **
PgCurrentLocalBufHashRef(void)
{
	return &PgCurrentBackendBufferState()->local_buf_hash;
}

int *
PgCurrentNLocalPinnedBuffersRef(void)
{
	return &PgCurrentBackendBufferState()->n_local_pinned_buffers;
}

char **
PgCurrentLocalBufferCurBlockRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_cur_block;
}

int *
PgCurrentLocalBufferNextBufInBlockRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_next_buf_in_block;
}

int *
PgCurrentLocalBufferNumBufsInBlockRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_num_bufs_in_block;
}

int *
PgCurrentLocalBufferTotalBufsAllocatedRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_total_bufs_allocated;
}

MemoryContext *
PgCurrentLocalBufferContextRef(void)
{
	return &PgCurrentBackendBufferState()->local_buffer_context;
}

BufferDesc **
PgCurrentPinCountWaitBufRef(void)
{
	return &PgCurrentBackendBufferState()->pin_count_wait_buf;
}

WritebackContext *
PgCurrentBackendWritebackContextRef(void)
{
	PgBackendBufferState *buffers = PgCurrentBackendBufferState();

	if (buffers->backend_writeback_context == NULL)
		buffers->backend_writeback_context =
			MemoryContextAllocZero(PgBackendBufferAllocationContext(),
								   sizeof(WritebackContext));

	return buffers->backend_writeback_context;
}

void **
PgCurrentPrivateRefCountArrayKeysRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_array_keys;
}

void **
PgCurrentPrivateRefCountArrayRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_array;
}

void **
PgCurrentPrivateRefCountHashRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_hash;
}

int32 *
PgCurrentPrivateRefCountOverflowedRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_overflowed;
}

uint32 *
PgCurrentPrivateRefCountClockRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_clock;
}

int *
PgCurrentReservedRefCountSlotRef(void)
{
	return &PgCurrentBackendBufferState()->reserved_ref_count_slot;
}

int *
PgCurrentPrivateRefCountEntryLastRef(void)
{
	return &PgCurrentBackendBufferState()->private_ref_count_entry_last;
}

uint32 *
PgCurrentMaxProportionalPinsRef(void)
{
	return &PgCurrentBackendBufferState()->max_proportional_pins;
}

static PgBackendStorageState *
PgCurrentBackendStorageState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_storage;

	return &CurrentPgBackend->storage;
}

void **
PgCurrentVfdCacheRef(void)
{
	return &PgCurrentBackendStorageState()->vfd_cache;
}

Size *
PgCurrentSizeVfdCacheRef(void)
{
	return &PgCurrentBackendStorageState()->size_vfd_cache;
}

int *
PgCurrentNFileRef(void)
{
	return &PgCurrentBackendStorageState()->nfile;
}

bool *
PgCurrentTemporaryFilesAllowedRef(void)
{
	return &PgCurrentBackendStorageState()->temporary_files_allowed;
}

int *
PgCurrentNumAllocatedDescsRef(void)
{
	return &PgCurrentBackendStorageState()->num_allocated_descs;
}

int *
PgCurrentMaxAllocatedDescsRef(void)
{
	return &PgCurrentBackendStorageState()->max_allocated_descs;
}

void **
PgCurrentAllocatedDescsRef(void)
{
	return &PgCurrentBackendStorageState()->allocated_descs;
}

int *
PgCurrentNumExternalFDsRef(void)
{
	return &PgCurrentBackendStorageState()->num_external_fds;
}

HTAB **
PgCurrentSyncPendingOpsRef(void)
{
	return &PgCurrentBackendStorageState()->sync_pending_ops;
}

List **
PgCurrentSyncPendingUnlinksRef(void)
{
	return &PgCurrentBackendStorageState()->sync_pending_unlinks;
}

MemoryContext *
PgCurrentSyncPendingOpsContextRef(void)
{
	return &PgCurrentBackendStorageState()->sync_pending_ops_context;
}

uint16 *
PgCurrentSyncCycleCounterRef(void)
{
	return &PgCurrentBackendStorageState()->sync_cycle_counter;
}

uint16 *
PgCurrentSyncCheckpointCycleCounterRef(void)
{
	return &PgCurrentBackendStorageState()->sync_checkpoint_cycle_counter;
}

bool *
PgCurrentSyncInProgressRef(void)
{
	return &PgCurrentBackendStorageState()->sync_in_progress;
}

HTAB **
PgCurrentSMgrRelationHashRef(void)
{
	return &PgCurrentBackendStorageState()->smgr_relation_hash;
}

dlist_head *
PgCurrentSMgrUnpinnedRelationsRef(void)
{
	return &PgCurrentBackendStorageState()->smgr_unpinned_relations;
}

MemoryContext *
PgCurrentMdContextRef(void)
{
	return &PgCurrentBackendStorageState()->md_context;
}

static PgBackendLockState *
PgCurrentBackendLockState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_locks;

	return &CurrentPgBackend->locks;
}

void **
PgCurrentFastPathLocalUseCountsRef(void)
{
	return &PgCurrentBackendLockState()->fast_path_local_use_counts;
}

PgBackendLWLockHandle *
PgCurrentHeldLWLocks(void)
{
	return PgCurrentBackendLockState()->held_lwlocks;
}

int *
PgCurrentNumHeldLWLocksRef(void)
{
	return &PgCurrentBackendLockState()->num_held_lwlocks;
}

HTAB **
PgCurrentLWLockStatsHashRef(void)
{
	return &PgCurrentBackendLockState()->lwlock_stats_htab;
}

PgBackendLWLockStats *
PgCurrentLWLockStatsDummy(void)
{
	return &PgCurrentBackendLockState()->lwlock_stats_dummy;
}

MemoryContext *
PgCurrentLWLockStatsContextRef(void)
{
	return &PgCurrentBackendLockState()->lwlock_stats_context;
}

bool *
PgCurrentLWLockStatsExitRegisteredRef(void)
{
	return &PgCurrentBackendLockState()->lwlock_stats_exit_registered;
}

int *
PgCurrentLocalNumUserDefinedLWLockTranchesRef(void)
{
	return &PgCurrentBackendLockState()->local_num_user_defined_lwlock_tranches;
}

bool *
PgCurrentRelationExtensionLockHeldRef(void)
{
	return &PgCurrentBackendLockState()->relation_extension_lock_held;
}

HTAB **
PgCurrentLockMethodLocalHashRef(void)
{
	return &PgCurrentBackendLockState()->lock_method_local_hash;
}

void **
PgCurrentStrongLockInProgressRef(void)
{
	return &PgCurrentBackendLockState()->strong_lock_in_progress;
}

void **
PgCurrentAwaitedLockRef(void)
{
	return &PgCurrentBackendLockState()->awaited_lock;
}

void **
PgCurrentAwaitedOwnerRef(void)
{
	return &PgCurrentBackendLockState()->awaited_owner;
}

volatile sig_atomic_t *
PgCurrentDeadlockTimeoutPendingRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_timeout_pending;
}

void **
PgCurrentConditionVariableSleepTargetRef(void)
{
	return &PgCurrentBackendLockState()->condition_variable_sleep_target;
}

uint32 *
PgCurrentSpeculativeInsertionTokenRef(void)
{
	return &PgCurrentBackendLockState()->speculative_insertion_token;
}

void **
PgCurrentDeadlockVisitedProcsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_visited_procs;
}

int *
PgCurrentDeadlockNVisitedProcsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_n_visited_procs;
}

void **
PgCurrentDeadlockTopoProcsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_topo_procs;
}

void **
PgCurrentDeadlockBeforeConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_before_constraints;
}

void **
PgCurrentDeadlockAfterConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_after_constraints;
}

void **
PgCurrentDeadlockWaitOrdersRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_wait_orders;
}

int *
PgCurrentDeadlockNWaitOrdersRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_n_wait_orders;
}

void **
PgCurrentDeadlockWaitOrderProcsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_wait_order_procs;
}

void **
PgCurrentDeadlockCurConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_cur_constraints;
}

int *
PgCurrentDeadlockNCurConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_n_cur_constraints;
}

int *
PgCurrentDeadlockMaxCurConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_max_cur_constraints;
}

void **
PgCurrentDeadlockPossibleConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_possible_constraints;
}

int *
PgCurrentDeadlockNPossibleConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_n_possible_constraints;
}

int *
PgCurrentDeadlockMaxPossibleConstraintsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_max_possible_constraints;
}

void **
PgCurrentDeadlockDetailsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_details;
}

int *
PgCurrentDeadlockNDetailsRef(void)
{
	return &PgCurrentBackendLockState()->deadlock_n_details;
}

void **
PgCurrentBlockingAutovacuumProcRef(void)
{
	return &PgCurrentBackendLockState()->blocking_autovacuum_proc;
}

HTAB **
PgCurrentLocalPredicateLockHashRef(void)
{
	return &PgCurrentBackendLockState()->local_predicate_lock_hash;
}

void **
PgCurrentMySerializableXactRef(void)
{
	return &PgCurrentBackendLockState()->my_serializable_xact;
}

bool *
PgCurrentMyXactDidWriteRef(void)
{
	return &PgCurrentBackendLockState()->my_xact_did_write;
}

void **
PgCurrentSavedSerializableXactRef(void)
{
	return &PgCurrentBackendLockState()->saved_serializable_xact;
}

static PgBackendIPCState *
PgCurrentBackendIPCState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_ipc;

	return &CurrentPgBackend->ipc;
}

void **
PgCurrentProcSignalSlotRef(void)
{
	return &PgCurrentBackendIPCState()->proc_signal_slot;
}

uint64 *
PgCurrentSharedInvalidMessageCounterRef(void)
{
	return &PgCurrentBackendIPCState()->shared_invalid_message_counter;
}

volatile sig_atomic_t *
PgCurrentCatchupInterruptPendingRef(void)
{
	return &PgCurrentBackendIPCState()->catchup_interrupt_pending;
}

void **
PgCurrentSharedInvalidationMessagesRef(void)
{
	return &PgCurrentBackendIPCState()->shared_invalidation_messages;
}

volatile int *
PgCurrentSharedInvalidationNextMsgRef(void)
{
	return &PgCurrentBackendIPCState()->shared_invalidation_next_msg;
}

volatile int *
PgCurrentSharedInvalidationNumMsgsRef(void)
{
	return &PgCurrentBackendIPCState()->shared_invalidation_num_msgs;
}

bool *
PgCurrentDsmInitDoneRef(void)
{
	return &PgCurrentBackendIPCState()->dsm_init_done;
}

void **
PgCurrentDsmRegistryDsaRef(void)
{
	return &PgCurrentBackendIPCState()->dsm_registry_dsa;
}

void **
PgCurrentDsmRegistryTableRef(void)
{
	return &PgCurrentBackendIPCState()->dsm_registry_table;
}

LocalTransactionId *
PgCurrentNextLocalTransactionIdRef(void)
{
	return &PgCurrentBackendIPCState()->next_local_transaction_id;
}

WaitEventSet **
PgCurrentLatchWaitSetRef(void)
{
	return &PgCurrentBackendIPCState()->latch_wait_set;
}

Latch *
PgCurrentLocalLatchData(void)
{
	return &PgCurrentBackendIPCState()->local_latch_data;
}

static PgBackendWaitState *
PgCurrentBackendWaitState(void)
{
	if (CurrentPgBackend == NULL)
	{
		PgBackendEnsureWaitStateInitialized(&early_backend_wait_state);
		return &early_backend_wait_state;
	}

	PgBackendEnsureWaitStateInitialized(&CurrentPgBackend->wait_state);
	return &CurrentPgBackend->wait_state;
}

uint32 **
PgCurrentMyWaitEventInfoRef(void)
{
	return &PgCurrentBackendWaitState()->my_wait_event_info;
}

uint32 *
PgCurrentLocalWaitEventInfoRef(void)
{
	return &PgCurrentBackendWaitState()->local_wait_event_info;
}

PgBackendTimeoutState *
PgCurrentTimeoutState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_timeout;

	return &CurrentPgBackend->timeout;
}

PgBackendWalSenderState *
PgCurrentWalSenderState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_walsender;

	return &CurrentPgBackend->walsender;
}

PgBackendReplicationState *
PgCurrentReplicationState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_replication;

	return &CurrentPgBackend->replication;
}

PgBackendLogicalReplicationState *
PgCurrentLogicalReplicationState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_logical_replication;

	return &CurrentPgBackend->logical_replication;
}

PgBackendXLogState *
PgCurrentXLogState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_xlog;

	return &CurrentPgBackend->xlog;
}

PgBackendRecoveryState *
PgCurrentRecoveryState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_recovery;

	return &CurrentPgBackend->recovery;
}

PgBackendMaintenanceWorkerState *
PgCurrentMaintenanceWorkerState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_maintenance_worker;

	return &CurrentPgBackend->maintenance_worker;
}

char **
PgCurrentArchModuleCheckErrdetailStringRef(void)
{
	return &PgCurrentMaintenanceWorkerState()->arch_module_errdetail_string;
}

PgBackendAutovacuumState *
PgCurrentAutovacuumState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_autovacuum;

	return &CurrentPgBackend->autovacuum;
}

PgBackendRepackState *
PgCurrentRepackState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_repack;

	return &CurrentPgBackend->repack;
}

volatile sig_atomic_t *
PgCurrentRepackMessagePendingRef(void)
{
	return &PgCurrentRepackState()->message_pending;
}

PgBackendAioState *
PgCurrentAioState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_aio;

	return &CurrentPgBackend->aio;
}

struct PgAioBackend **
PgCurrentAioBackendRef(void)
{
	return &PgCurrentAioState()->my_backend;
}

static PgBackendTransactionState *
PgCurrentBackendTransactionState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_transaction;

	return &CurrentPgBackend->transaction;
}

TransactionId *
PgCurrentCachedFetchXidRef(void)
{
	return &PgCurrentBackendTransactionState()->cached_fetch_xid;
}

int *
PgCurrentCachedFetchXidStatusRef(void)
{
	return &PgCurrentBackendTransactionState()->cached_fetch_xid_status;
}

XLogRecPtr *
PgCurrentCachedCommitLSNRef(void)
{
	return &PgCurrentBackendTransactionState()->cached_commit_lsn;
}

void **
PgCurrentTwoPhaseLockedGxactRef(void)
{
	return &PgCurrentBackendTransactionState()->two_phase_locked_gxact;
}

bool *
PgCurrentTwoPhaseExitRegisteredRef(void)
{
	return &PgCurrentBackendTransactionState()->two_phase_exit_registered;
}

FullTransactionId *
PgCurrentTwoPhaseCachedFxidRef(void)
{
	return &PgCurrentBackendTransactionState()->two_phase_cached_fxid;
}

void **
PgCurrentTwoPhaseCachedGxactRef(void)
{
	return &PgCurrentBackendTransactionState()->two_phase_cached_gxact;
}

int *
PgCurrentSlruErrorCauseRef(void)
{
	return &PgCurrentBackendTransactionState()->slru_error_cause;
}

int *
PgCurrentSlruErrnoRef(void)
{
	return &PgCurrentBackendTransactionState()->slru_errno_value;
}

dclist_head *
PgCurrentMultiXactCacheRef(void)
{
	return &PgCurrentBackendTransactionState()->multixact_cache;
}

bool *
PgCurrentMultiXactCacheInitializedRef(void)
{
	return &PgCurrentBackendTransactionState()->multixact_cache_initialized;
}

MemoryContext *
PgCurrentMultiXactContextRef(void)
{
	return &PgCurrentBackendTransactionState()->multixact_context;
}

char **
PgCurrentMultiXactDebugStringRef(void)
{
	return &PgCurrentBackendTransactionState()->multixact_debug_string;
}

TransactionId *
PgCurrentProcArrayCachedXidNotInProgressRef(void)
{
	return &PgCurrentBackendTransactionState()->procarray_cached_xid_not_in_progress;
}

struct GlobalVisState *
PgCurrentGlobalVisSharedRelsRef(void)
{
	return &PgCurrentBackendTransactionState()->global_vis_shared_rels;
}

struct GlobalVisState *
PgCurrentGlobalVisCatalogRelsRef(void)
{
	return &PgCurrentBackendTransactionState()->global_vis_catalog_rels;
}

struct GlobalVisState *
PgCurrentGlobalVisDataRelsRef(void)
{
	return &PgCurrentBackendTransactionState()->global_vis_data_rels;
}

struct GlobalVisState *
PgCurrentGlobalVisTempRelsRef(void)
{
	return &PgCurrentBackendTransactionState()->global_vis_temp_rels;
}

TransactionId *
PgCurrentComputeXidHorizonsResultLastXminRef(void)
{
	return &PgCurrentBackendTransactionState()->compute_xid_horizons_result_last_xmin;
}

long *
PgCurrentXidCacheByRecentXminRef(void)
{
	return &PgCurrentBackendTransactionState()->xidcache_by_recent_xmin;
}

long *
PgCurrentXidCacheByKnownXactRef(void)
{
	return &PgCurrentBackendTransactionState()->xidcache_by_known_xact;
}

long *
PgCurrentXidCacheByMyXactRef(void)
{
	return &PgCurrentBackendTransactionState()->xidcache_by_my_xact;
}

long *
PgCurrentXidCacheByLatestXidRef(void)
{
	return &PgCurrentBackendTransactionState()->xidcache_by_latest_xid;
}

long *
PgCurrentXidCacheByMainXidRef(void)
{
	return &PgCurrentBackendTransactionState()->xidcache_by_main_xid;
}

long *
PgCurrentXidCacheByChildXidRef(void)
{
	return &PgCurrentBackendTransactionState()->xidcache_by_child_xid;
}

long *
PgCurrentXidCacheByKnownAssignedRef(void)
{
	return &PgCurrentBackendTransactionState()->xidcache_by_known_assigned;
}

long *
PgCurrentXidCacheNoOverflowRef(void)
{
	return &PgCurrentBackendTransactionState()->xidcache_no_overflow;
}

long *
PgCurrentXidCacheSlowAnswerRef(void)
{
	return &PgCurrentBackendTransactionState()->xidcache_slow_answer;
}

static PgBackendPendingInterruptState *
PgCurrentPendingInterrupts(void)
{
	if (CurrentPgBackend == NULL)
		return &early_pending_interrupts;

	return &CurrentPgBackend->pending_interrupts;
}

PgBackendPendingInterruptState *
PgCurrentPendingInterruptStateRef(void)
{
	return PgCurrentPendingInterrupts();
}

static PgBackendInterruptHoldoffState *
PgCurrentInterruptHoldoffs(void)
{
	if (CurrentPgBackend == NULL)
		return &early_interrupt_holdoffs;

	return &CurrentPgBackend->interrupt_holdoffs;
}

volatile uint32 *
PgCurrentInterruptHoldoffCountRef(void)
{
	return &PgCurrentInterruptHoldoffs()->interrupt_holdoff_count;
}

volatile uint32 *
PgCurrentQueryCancelHoldoffCountRef(void)
{
	return &PgCurrentInterruptHoldoffs()->query_cancel_holdoff_count;
}

volatile uint32 *
PgCurrentCritSectionCountRef(void)
{
	return &PgCurrentInterruptHoldoffs()->crit_section_count;
}

PgBackendLaunchModel
PgRuntimeGetBackendLaunchModel(BackendType backend_type)
{
	if (PgRuntimeShouldThreadBackend(backend_type))
		return PG_BACKEND_LAUNCH_THREAD;

	return PG_BACKEND_LAUNCH_PROCESS;
}

bool
PgRuntimeShouldThreadBackend(BackendType backend_type)
{
	if (!multithreaded)
		return false;

	/*
	 * Phase 10 is scoped to regular client backends.  Phase 11 incrementally
	 * moves in-tree server-owned worker families onto the same carrier
	 * infrastructure as they get dedicated signal and lifecycle conversion.
	 */
	return backend_type == B_BACKEND ||
		backend_type == B_ARCHIVER ||
		backend_type == B_AUTOVAC_LAUNCHER ||
		backend_type == B_AUTOVAC_WORKER ||
		backend_type == B_BG_WRITER ||
		backend_type == B_CHECKPOINTER ||
		backend_type == B_LOGGER ||
		backend_type == B_STARTUP ||
		backend_type == B_WAL_RECEIVER ||
		backend_type == B_SLOTSYNC_WORKER ||
		backend_type == B_WAL_WRITER ||
		backend_type == B_WAL_SUMMARIZER;
}

PgBackendModel
PgRuntimeGetExtensionBackendModel(void)
{
	if (CurrentPgRuntime == NULL)
		return PG_BACKEND_MODEL_PROCESS;

	return CurrentPgRuntime->extension_backend_model;
}

void
PgRuntimeSetExtensionBackendModel(PgBackendModel backend_model)
{
	if (backend_model < PG_BACKEND_MODEL_PROCESS ||
		backend_model > PG_BACKEND_MODEL_POOLED_SCHEDULER)
		elog(ERROR, "invalid backend model: %d", backend_model);

	if (CurrentPgRuntime == NULL)
		return;

	check_loaded_modules_backend_model(backend_model);
	CurrentPgRuntime->extension_backend_model = backend_model;
}

void
PgBackendInitializeInterrupts(PgBackend *backend)
{
	if (backend == NULL)
		return;

	pg_atomic_init_u32(&backend->interrupts.pending_mask, 0);
	backend->interrupts.proc_die_sender_pid = 0;
	backend->interrupts.proc_die_sender_uid = 0;
}

void
PgBackendSetInterruptLatch(PgBackend *backend, struct Latch *interrupt_latch)
{
	if (backend == NULL)
		return;

	backend->interrupt_latch = interrupt_latch;
}

PgBackendId
PgBackendGetId(PgBackend *backend)
{
	if (backend == NULL)
		return 0;

	return backend->id;
}

PgBackendId
PgCurrentBackendId(void)
{
	return PgBackendGetId(CurrentPgBackend);
}

int
PgBackendGetSignalPid(PgBackend *backend)
{
	if (backend == NULL)
		return MyProcPid;

	if (backend->runtime != NULL &&
		backend->runtime->kind == PG_RUNTIME_THREAD_PER_SESSION)
	{
		if (backend->id > PG_INT32_MAX)
			elog(ERROR, "threaded backend identifier exceeds protocol range");

		return (int) backend->id;
	}

	return MyProcPid;
}

int
PgCurrentBackendSignalPid(void)
{
	return PgBackendGetSignalPid(CurrentPgBackend);
}

bool
PgBackendUsesProcessSignals(PgBackend *backend)
{
	if (backend == NULL || backend->runtime == NULL)
		return true;

	return backend->runtime->kind == PG_RUNTIME_PROCESS;
}

void
PgBackendWakeup(PgBackend *backend)
{
	if (backend == NULL)
		return;

	PgBackendWakeForInterrupt(backend);
}

void
PgBackendRaiseInterrupt(PgBackend *backend,
						PgBackendInterruptType interrupt_type)
{
	PgBackendInterruptMask interrupt_mask;

	if (backend == NULL)
		return;
	if (interrupt_type < 0 || interrupt_type >= PG_BACKEND_INTERRUPT_COUNT)
		return;

	interrupt_mask = PG_BACKEND_INTERRUPT_MASK(interrupt_type);
	pg_atomic_fetch_or_u32(&backend->interrupts.pending_mask, interrupt_mask);
	PgBackendWakeForInterrupt(backend);
}

static void
PgBackendWakeForInterrupt(PgBackend *backend)
{
	/*
	 * Process mode has one logical backend per address space, so waking the
	 * current backend must still arm the historical fast-path flag used by
	 * signal-era code. Non-current logical backends rely on the mailbox test in
	 * CHECK_FOR_INTERRUPTS() after their carrier wakes.
	 */
	if (backend == CurrentPgBackend)
		InterruptPending = true;

	if (backend->interrupt_latch != NULL)
		SetLatch(backend->interrupt_latch);
	else if (backend == CurrentPgBackend && MyLatch != NULL)
		SetLatch(MyLatch);
}

void
PgCurrentBackendRaiseInterrupt(PgBackendInterruptType interrupt_type)
{
	PgBackendRaiseInterrupt(CurrentPgBackend, interrupt_type);
}

void
PgBackendRaiseProcDieInterrupt(PgBackend *backend, int sender_pid,
							   int sender_uid)
{
	if (backend == NULL)
		return;

	if (backend->interrupts.proc_die_sender_pid == 0)
	{
		backend->interrupts.proc_die_sender_pid = sender_pid;
		backend->interrupts.proc_die_sender_uid = sender_uid;
	}

	PgBackendRaiseInterrupt(backend, PG_BACKEND_INTERRUPT_PROC_DIE);
}

void
PgCurrentBackendRaiseProcDieInterrupt(int sender_pid, int sender_uid)
{
	PgBackendRaiseProcDieInterrupt(CurrentPgBackend, sender_pid, sender_uid);
}

PgBackendInterruptMask
PgBackendConsumeInterrupts(PgBackend *backend)
{
	if (backend == NULL)
		return 0;

	return pg_atomic_exchange_u32(&backend->interrupts.pending_mask, 0);
}

bool
PgCurrentBackendHasPendingInterrupts(void)
{
	PgBackend  *backend = CurrentPgBackend;

	if (backend == NULL)
		return ProcSignalBackendInterruptsPending();

	return pg_atomic_read_u32(&backend->interrupts.pending_mask) != 0 ||
		ProcSignalBackendInterruptsPending();
}

void
PgBackendConsumeProcDieSender(PgBackend *backend, int *sender_pid,
							  int *sender_uid)
{
	if (sender_pid != NULL)
		*sender_pid = 0;
	if (sender_uid != NULL)
		*sender_uid = 0;

	if (backend == NULL)
		return;

	if (sender_pid != NULL)
		*sender_pid = backend->interrupts.proc_die_sender_pid;
	if (sender_uid != NULL)
		*sender_uid = backend->interrupts.proc_die_sender_uid;

	backend->interrupts.proc_die_sender_pid = 0;
	backend->interrupts.proc_die_sender_uid = 0;
}

void
PgCurrentBackendApplyInterrupts(void)
{
	PgBackendInterruptMask pending;
	int			proc_signal_sender_pid = 0;
	int			proc_signal_sender_uid = 0;

	pending = PgBackendConsumeInterrupts(CurrentPgBackend);
	pending |= ConsumeBackendInterruptsFromProcSignal(&proc_signal_sender_pid,
													  &proc_signal_sender_uid);
	if (pending == 0)
		return;

	/*
	 * The logical mailbox feeds the legacy per-backend pending flags below.
	 * Arm the legacy dispatcher as well, so callers that consume the mailbox
	 * immediately before CHECK_FOR_INTERRUPTS() still run ProcessInterrupts().
	 */
	InterruptPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL))
		QueryCancelPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PROC_DIE))
	{
		int			sender_pid;
		int			sender_uid;

		ProcDiePending = true;
		PgBackendConsumeProcDieSender(CurrentPgBackend, &sender_pid,
									  &sender_uid);
		if (sender_pid == 0 && proc_signal_sender_pid != 0)
		{
			sender_pid = proc_signal_sender_pid;
			sender_uid = proc_signal_sender_uid;
		}
		if (ProcDieSenderPid == 0)
		{
			ProcDieSenderPid = sender_pid;
			ProcDieSenderUid = sender_uid;
		}
	}

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CLIENT_CONNECTION_CHECK))
		CheckClientConnectionPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_IDLE_IN_TRANSACTION_SESSION_TIMEOUT))
		IdleInTransactionSessionTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_TRANSACTION_TIMEOUT))
		TransactionTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_IDLE_SESSION_TIMEOUT))
		IdleSessionTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_IDLE_STATS_UPDATE_TIMEOUT))
		IdleStatsUpdateTimeoutPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PROC_SIGNAL_BARRIER))
		ProcSignalBarrierPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_LOG_MEMORY_CONTEXT))
		LogMemoryContextPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CONFIG_RELOAD))
		ConfigReloadPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST))
		ShutdownRequestPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CATCHUP))
		catchupInterruptPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_NOTIFY))
		notifyInterruptPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PARALLEL_MESSAGE))
		ParallelMessagePending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_PARALLEL_APPLY_MESSAGE))
		ParallelApplyMessagePending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_SLOT_SYNC_MESSAGE))
		SlotSyncShutdownPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_REPACK_MESSAGE))
		RepackMessagePending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_WAKEUP_STOP))
		WakeupStopPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_AUTOVAC_LAUNCHER))
		AutoVacLauncherPending = true;

	if (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_CHECKPOINTER_SHUTDOWN_XLOG))
		CheckpointerShutdownXLOGPending = true;
}

int
PgSuspend(const PgWaitSpec *wait_spec, PgSuspendCallback callback,
		  void *callback_arg)
{
	PgBackend  *backend = CurrentPgBackend;
	int			result = 0;

	Assert(callback != NULL);

	if (backend != NULL && wait_spec != NULL)
	{
		backend->wait_state.spec = *wait_spec;
		pg_atomic_write_membarrier_u32(&backend->wait_state.waiting, 1);
	}

	PG_TRY();
	{
		result = callback(callback_arg);
	}
	PG_CATCH();
	{
		if (backend != NULL)
		{
			pg_atomic_write_u32(&backend->wait_state.waiting, 0);
			backend->wait_state.spec.kind = PG_WAIT_KIND_NONE;
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (backend != NULL)
	{
		pg_atomic_write_u32(&backend->wait_state.waiting, 0);
		backend->wait_state.spec.kind = PG_WAIT_KIND_NONE;
	}

	return result;
}
