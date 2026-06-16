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

#include <unistd.h>

#include "access/gin.h"
#include "access/parallel.h"
#include "access/session.h"
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
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/explain_state.h"
#include "commands/prepare.h"
#include "commands/repack.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
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
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "storage/large_object.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "tsearch/ts_cache.h"
#include "utils/backend_runtime.h"
#include "backend_runtime_internal.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/dsa.h"
#include "utils/elog.h"
#include "utils/float.h"
#include "utils/funccache.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"
#include "utils/plancache.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/rls.h"
#include "utils/typcache.h"
#include "utils/xml.h"

#define PG_TRGM_SIMILARITY_THRESHOLD_DEFAULT 0.3
#define PG_TRGM_WORD_SIMILARITY_THRESHOLD_DEFAULT 0.6
#define PG_TRGM_STRICT_WORD_SIMILARITY_THRESHOLD_DEFAULT 0.5
#define PG_PLAN_ADVICE_ALWAYS_EXPLAIN_SUPPLIED_ADVICE_DEFAULT true
#define AUTO_EXPLAIN_LOG_MIN_DURATION_DEFAULT (-1)
#define AUTO_EXPLAIN_LOG_PARAMETER_MAX_LENGTH_DEFAULT (-1)
#define AUTO_EXPLAIN_LOG_TIMING_DEFAULT true
#define AUTO_EXPLAIN_LOG_FORMAT_DEFAULT EXPLAIN_FORMAT_TEXT
#define AUTO_EXPLAIN_LOG_LEVEL_DEFAULT LOG
#define AUTO_EXPLAIN_SAMPLE_RATE_DEFAULT 1.0

PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgRuntime *CurrentPgRuntime = NULL;
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
static PG_GLOBAL_RUNTIME PgRuntimeExtensionModuleState early_runtime_extension_modules;
static PG_GLOBAL_CARRIER PgCarrier process_carrier;
static PG_GLOBAL_BACKEND PgBackend process_backend;
static PG_GLOBAL_SESSION PgSession process_session;
static PG_GLOBAL_CONNECTION PgConnection process_connection;
static PG_GLOBAL_EXECUTION PgExecution process_execution;
static PG_THREAD_LOCAL PG_GLOBAL_BACKEND PgBackend early_backend_fallback = {
	.core = {
		.mode = InitProcessing
	},
	.timeout = {0},
	.replication = {
		.sync_rep_wait_mode = -1,
		.walreceiver_recv_file = -1,
		.walreceiver_primary_has_standby_xmin = true
	},
	.logical_replication = {
		.apply_error_callback_arg.remote_attnum = -1,
		.apply_error_callback_arg.remote_xid = InvalidTransactionId,
		.apply_error_callback_arg.finish_lsn = InvalidXLogRecPtr,
		.subxact_data.subxact_last = InvalidTransactionId,
		.remote_final_lsn = InvalidXLogRecPtr,
		.stream_xid = InvalidTransactionId,
		.skip_xact_finish_lsn = InvalidXLogRecPtr,
		.last_flushpos = InvalidXLogRecPtr,
		.slotsync_sleep_ms = PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS
	},
	.xlog = {
		.local_recovery_in_progress = true,
		.local_xlog_insert_allowed = -1,
		.proc_last_rec_ptr = InvalidXLogRecPtr,
		.xact_last_rec_end = InvalidXLogRecPtr,
		.xact_last_commit_end = InvalidXLogRecPtr,
		.redo_rec_ptr = InvalidXLogRecPtr,
		.open_log_file = -1,
		.local_min_recovery_point = InvalidXLogRecPtr,
		.update_min_recovery_point = true
	},
	.recovery = {
		.standby_wait_us = PG_BACKEND_STANDBY_INITIAL_WAIT_US
	},
	.maintenance_worker = {
		.bgwriter_last_snapshot_lsn = InvalidXLogRecPtr,
		.walsummarizer_sleep_quanta = 1,
		.walsummarizer_redo_pointer_at_last_summary_removal = InvalidXLogRecPtr
	},
	.autovacuum = {
		.av_storage_param_cost_delay = -1,
		.av_storage_param_cost_limit = -1
	},
	.repack = {
		.repacked_rel_locator.relNumber = InvalidOid,
		.repacked_rel_toast_locator.relNumber = InvalidOid
	},
	.aio = {
		.my_io_worker_id = -1
	},
	.parallel = {
		.worker_number = -1,
		.pq_mq_parallel_leader_proc_number = INVALID_PROC_NUMBER
	},
	.my_proc_number = INVALID_PROC_NUMBER,
	.parallel_leader_proc_number = INVALID_PROC_NUMBER,
	.backend_type = B_INVALID
};

#define early_backend_core early_backend_fallback.core
#define early_backend_command early_backend_fallback.command
#define early_backend_log early_backend_fallback.log_state
#define early_backend_expr_interp early_backend_fallback.expr_interp
#define early_backend_type early_backend_fallback.backend_type
#define early_my_proc early_backend_fallback.my_proc
#define early_my_proc_number early_backend_fallback.my_proc_number
#define early_parallel_leader_proc_number early_backend_fallback.parallel_leader_proc_number
#define early_my_beentry early_backend_fallback.my_beentry
#define early_my_bgworker_entry early_backend_fallback.my_bgworker_entry
#define early_aux_process_resource_owner early_backend_fallback.aux_process_resource_owner
#define early_backend_pgstat_pending early_backend_fallback.pgstat_pending
#define early_backend_instrumentation early_backend_fallback.instrumentation
#define early_backend_buffers early_backend_fallback.buffers
#define early_backend_storage early_backend_fallback.storage
#define early_backend_locks early_backend_fallback.locks
#define early_backend_ipc early_backend_fallback.ipc
#define early_backend_wait_state early_backend_fallback.wait_state
#define early_backend_transaction early_backend_fallback.transaction
#define early_backend_memory_manager early_backend_fallback.memory_manager
#define early_backend_timeout early_backend_fallback.timeout
#define early_backend_walsender early_backend_fallback.walsender
#define early_backend_replication early_backend_fallback.replication
#define early_backend_logical_replication early_backend_fallback.logical_replication
#define early_backend_xlog early_backend_fallback.xlog
#define early_backend_recovery early_backend_fallback.recovery
#define early_backend_maintenance_worker early_backend_fallback.maintenance_worker
#define early_backend_autovacuum early_backend_fallback.autovacuum
#define early_backend_repack early_backend_fallback.repack
#define early_backend_aio early_backend_fallback.aio
#define early_backend_extension_modules early_backend_fallback.extension_modules
#define early_backend_activity early_backend_fallback.activity
#define early_backend_utility early_backend_fallback.utility
#define early_backend_parallel early_backend_fallback.parallel
#define early_pending_interrupts early_backend_fallback.pending_interrupts
#define early_interrupt_holdoffs early_backend_fallback.interrupt_holdoffs
static PG_THREAD_LOCAL PG_GLOBAL_CONNECTION PgConnection early_connection_fallback = {
	.output = {
		.where_to_send_output = DestDebug
	},
	.startup = {
		.timing.ready_for_use = TIMESTAMP_MINUS_INFINITY
	}
};
#define early_connection_identity early_connection_fallback.identity
#define early_connection_socket_io early_connection_fallback.socket_io
#define early_connection_protocol early_connection_fallback.protocol
#define early_connection_output early_connection_fallback.output
#define early_connection_interrupts early_connection_fallback.interrupts
#define early_connection_startup early_connection_fallback.startup
#define early_client_connection_info \
	early_connection_fallback.client_connection_info
#define early_client_connection_info_context \
	early_connection_fallback.client_connection_info_context
#define early_client_connection_info_authn_id_owned \
	early_connection_fallback.client_connection_info_authn_id_owned
#define early_connection_security early_connection_fallback.security
static PG_THREAD_LOCAL PG_GLOBAL_SESSION PgSession early_session_fallback = {
	.tablespace = {
		.initialized = true,
		.default_tablespace_name = NULL,
		.temp_tablespaces_names = NULL,
		.allow_in_place_tablespaces_value = false,
		.binary_upgrade_next_pg_tablespace_oid_value = InvalidOid
	},
	.binary_upgrade = {
		.initialized = true,
		.binary_upgrade_next_pg_type_oid_value = InvalidOid,
		.binary_upgrade_next_array_pg_type_oid_value = InvalidOid,
		.binary_upgrade_next_mrng_pg_type_oid_value = InvalidOid,
		.binary_upgrade_next_mrng_array_pg_type_oid_value = InvalidOid,
		.binary_upgrade_next_heap_pg_class_oid_value = InvalidOid,
		.binary_upgrade_next_heap_pg_class_relfilenumber_value =
			InvalidRelFileNumber,
		.binary_upgrade_next_index_pg_class_oid_value = InvalidOid,
		.binary_upgrade_next_index_pg_class_relfilenumber_value =
			InvalidRelFileNumber,
		.binary_upgrade_next_toast_pg_class_oid_value = InvalidOid,
		.binary_upgrade_next_toast_pg_class_relfilenumber_value =
			InvalidRelFileNumber,
		.binary_upgrade_next_pg_enum_oid_value = InvalidOid,
		.binary_upgrade_next_pg_authid_oid_value = InvalidOid,
		.binary_upgrade_record_init_privs_value = false
	},
	.datetime = {
		.initialized = true,
		.date_style = USE_ISO_DATES,
		.date_order = DATEORDER_MDY,
		.interval_style = INTSTYLE_POSTGRES,
		.datestyle_string_value = "ISO, MDY",
		.timezone_string_value = "GMT",
		.log_timezone_string_value = "GMT",
		.timezone_abbreviations_string_value = NULL,
		.session_timezone_value = NULL,
		.log_timezone_value = NULL,
		.timezone_abbrev_table = NULL
	},
	.text_search = {
		.initialized = true,
		.current_config_value = "pg_catalog.simple",
		.current_config_cache = InvalidOid
	},
	.connection_guc = {
		.initialized = true,
		.application_name_value = "",
		.ssl_renegotiation_limit_value = 0,
		.tcp_keepalives_idle_value = 0,
		.tcp_keepalives_interval_value = 0,
		.tcp_keepalives_count_value = 0,
		.tcp_user_timeout_value = 0,
		.log_disconnections_value = false,
		.log_statement_value = 0,
		.post_auth_delay_seconds = 0,
		.restrict_nonsystem_relation_kind_string_value = "",
		.restrict_nonsystem_relation_kind_value = 0
	},
	.parser = {
		.initialized = true,
		.transform_null_equals_value = false,
		.backslash_quote_value = BACKSLASH_QUOTE_SAFE_ENCODING,
		.operator_lookup_cache = NULL
	},
	.vacuum = {
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
	},
	.buffer_io = {
		.initialized = true,
		.zero_damaged_pages_value = false,
		.track_io_timing_value = false,
		.effective_io_concurrency_value = DEFAULT_EFFECTIVE_IO_CONCURRENCY,
		.maintenance_io_concurrency_value = DEFAULT_MAINTENANCE_IO_CONCURRENCY,
		.io_combine_limit_value = DEFAULT_IO_COMBINE_LIMIT,
		.io_combine_limit_guc_value = DEFAULT_IO_COMBINE_LIMIT,
		.backend_flush_after_value = DEFAULT_BACKEND_FLUSH_AFTER
	},
	.xact_defaults = {
		.initialized = true,
		.default_xact_iso_level = XACT_READ_COMMITTED,
		.default_xact_read_only = false,
		.default_xact_deferrable = false,
		.synchronous_commit_value = SYNCHRONOUS_COMMIT_ON
	},
	.lock_wait = {
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
	},
	.logging = {
		.initialized = true,
		.debug_print_plan_value = false,
		.debug_print_parse_value = false,
		.debug_print_raw_parse_value = false,
		.debug_print_rewritten_value = false,
		.debug_pretty_print_value = true,
#ifdef DEBUG_NODE_TESTS_ENABLED
		.debug_copy_parse_plan_trees_value = DEFAULT_DEBUG_COPY_PARSE_PLAN_TREES,
		.debug_write_read_parse_plan_trees_value =
			DEFAULT_DEBUG_WRITE_READ_PARSE_PLAN_TREES,
		.debug_raw_expression_coverage_test_value =
			DEFAULT_DEBUG_RAW_EXPRESSION_COVERAGE_TEST,
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
	},
	.misc_guc = {
		.initialized = true,
		.allow_system_table_mods_value = false,
		.max_stack_depth_kb = 100,
		.max_stack_depth_bytes = 100 * (ssize_t) 1024,
		.session_preload_libraries_value = NULL,
		.local_preload_libraries_value = NULL,
		.dynamic_library_path_value = NULL,
		.extension_control_path_value = "$system"
	},
	.pgstat = {
		.initialized = true,
		.track_counts = true,
		.track_functions = TRACK_FUNC_OFF,
		.fetch_consistency = PGSTAT_FETCH_CONSISTENCY_CACHE,
		.track_activities = true,
		.session_end_cause = DISCONNECT_NORMAL,
		.last_session_report_time = 0
	},
	.query_id = {
		.initialized = true,
		.compute_query_id_value = COMPUTE_QUERY_ID_AUTO,
		.query_id_enabled_value = false
	},
	.storage_guc = {
		.initialized = true,
		.ignore_checksum_failure_value = false,
		.file_copy_method_value = FILE_COPY_METHOD_COPY
	},
	.user_guc = {
		.initialized = true,
		.password_encryption_value = PASSWORD_TYPE_SCRAM_SHA_256,
		.createrole_self_grant_value = "",
		.createrole_self_grant_enabled = false,
		.createrole_self_grant_options_specified = 0,
		.createrole_self_grant_options_admin = false,
		.createrole_self_grant_options_inherit = false,
		.createrole_self_grant_options_set = false
	},
	.user_identity = {
		.initialized = true,
		.authenticated_user_id = InvalidOid,
		.session_user_id = InvalidOid,
		.outer_user_id = InvalidOid,
		.current_user_id = InvalidOid,
		.system_user = NULL,
		.session_user_is_superuser = false,
		.security_restriction_context = 0,
		.set_role_is_active = false
	},
	.command_guc = {
		.initialized = true,
		.session_replication_role_value = SESSION_REPLICATION_ROLE_ORIGIN,
		.event_triggers_value = true,
		.trace_notify_value = false
	},
	.replication_guc = {
		.initialized = true,
		.wal_sender_timeout_ms = 60 * 1000,
		.wal_sender_shutdown_timeout_ms = -1,
		.log_replication_commands_value = false,
		.wal_receiver_timeout_ms = 60 * 1000,
		.logical_decoding_work_mem_kb = 65536,
		.debug_logical_replication_streaming_value =
			DEBUG_LOGICAL_REP_STREAMING_BUFFERED
	},
	.general_guc = {
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
	},
	.access_wal_guc = {
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
	},
	.jit_guc = {
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
	},
	.sort_guc = {
		.initialized = true,
		.trace_sort_value = false,
#ifdef DEBUG_BOUNDED_SORT
		.optimize_bounded_sort_value = true,
#endif
	},
	.query_memory = {
		.initialized = true,
		.work_mem_kb = 4096,
		.hash_mem_multiplier_value = 2.0,
		.maintenance_work_mem_kb = 65536,
		.max_parallel_maintenance_workers_value = 2
	},
	.planner_cost = {
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
	},
	.planner_method = {
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
	},
	.extension_modules = {
		.auto_explain_log_min_duration = AUTO_EXPLAIN_LOG_MIN_DURATION_DEFAULT,
		.auto_explain_log_parameter_max_length =
			AUTO_EXPLAIN_LOG_PARAMETER_MAX_LENGTH_DEFAULT,
		.auto_explain_log_timing = AUTO_EXPLAIN_LOG_TIMING_DEFAULT,
		.auto_explain_log_format = AUTO_EXPLAIN_LOG_FORMAT_DEFAULT,
		.auto_explain_log_level = AUTO_EXPLAIN_LOG_LEVEL_DEFAULT,
		.auto_explain_sample_rate = AUTO_EXPLAIN_SAMPLE_RATE_DEFAULT,
		.pg_trgm_similarity_threshold = PG_TRGM_SIMILARITY_THRESHOLD_DEFAULT,
		.pg_trgm_word_similarity_threshold =
			PG_TRGM_WORD_SIMILARITY_THRESHOLD_DEFAULT,
		.pg_trgm_strict_word_similarity_threshold =
			PG_TRGM_STRICT_WORD_SIMILARITY_THRESHOLD_DEFAULT,
		.pg_plan_advice_always_explain_supplied_advice =
			PG_PLAN_ADVICE_ALWAYS_EXPLAIN_SUPPLIED_ADVICE_DEFAULT,
		.pg_stash_advice_stash_name = ""
	},
	.temp_file = {
		.initialized = true,
		.num_temp_table_spaces = -1
	},
	.random = {
		.initialized = true,
		.prng_seed_set = false
	},
	.locale = {
		.initialized = true,
		.icu_validation_level_value = WARNING,
		.last_collation_cache_oid = InvalidOid
	}
};

#define early_session_database early_session_fallback.database
#define early_session_tablespace early_session_fallback.tablespace
#define early_session_binary_upgrade early_session_fallback.binary_upgrade
#define early_session_datetime early_session_fallback.datetime
#define early_session_text_search early_session_fallback.text_search
#define early_session_connection_guc early_session_fallback.connection_guc
#define early_session_parser early_session_fallback.parser
#define early_session_vacuum early_session_fallback.vacuum
#define early_session_buffer_io early_session_fallback.buffer_io
#define early_session_xact_defaults early_session_fallback.xact_defaults
#define early_session_lock_wait early_session_fallback.lock_wait
#define early_session_loop_state early_session_fallback.loop_state
#define early_session_tcop early_session_fallback.tcop
#define early_session_logging early_session_fallback.logging
#define early_session_misc_guc early_session_fallback.misc_guc
#define early_session_guc early_session_fallback.guc
#define early_session_pgstat early_session_fallback.pgstat
#define early_session_query_id early_session_fallback.query_id
#define early_session_storage_guc early_session_fallback.storage_guc
#define early_session_user_guc early_session_fallback.user_guc
#define early_session_user_identity early_session_fallback.user_identity
#define early_session_command_guc early_session_fallback.command_guc
#define early_session_replication_guc early_session_fallback.replication_guc
#define early_session_logical_replication early_session_fallback.logical_replication
#define early_session_general_guc early_session_fallback.general_guc
#define early_session_access_wal_guc early_session_fallback.access_wal_guc
#define early_session_jit_guc early_session_fallback.jit_guc
#define early_session_jit_provider early_session_fallback.jit_provider_state
#define early_session_llvm_jit early_session_fallback.llvm_jit
#define early_session_sort_guc early_session_fallback.sort_guc
#define early_session_query_memory early_session_fallback.query_memory
#define early_session_planner_cost early_session_fallback.planner_cost
#define early_session_planner_method early_session_fallback.planner_method
#define early_session_function_manager early_session_fallback.function_manager
#define early_session_extension_modules early_session_fallback.extension_modules
#define early_session_catalog_lookup early_session_fallback.catalog_lookup
#define early_session_invalidation_callbacks early_session_fallback.invalidation_callbacks
#define early_session_ri_globals early_session_fallback.ri_globals
#define early_session_relmap early_session_fallback.relmap
#define early_session_prepared_statement early_session_fallback.prepared_statement
#define early_session_on_commit early_session_fallback.on_commit
#define early_session_sequence early_session_fallback.sequence
#define early_session_xact_callbacks early_session_fallback.xact_callbacks
#define early_session_backup early_session_fallback.backup
#define early_session_regex early_session_fallback.regex
#define early_session_portal_manager early_session_fallback.portal_manager
#define early_session_large_object early_session_fallback.large_object
#define early_session_async early_session_fallback.async
#define early_session_encoding early_session_fallback.encoding
#define early_session_temp_file early_session_fallback.temp_file
#define early_session_random early_session_fallback.random
#define early_session_optimizer early_session_fallback.optimizer
#define early_session_plan_cache early_session_fallback.plan_cache
#define early_session_namespace early_session_fallback.namespace_state
#define early_session_locale early_session_fallback.locale

static PG_THREAD_LOCAL PG_GLOBAL_EXECUTION PgExecution early_execution_fallback = {
	.error = {
		.errordata_stack_depth = -1
	},
	.spi = {
		.connected = -1
	},
	.snapshot = {
		.transaction_xmin = FirstNormalTransactionId,
		.recent_xmin = FirstNormalTransactionId
	},
	.xact = {
		.iso_level = XACT_READ_COMMITTED,
		.check_xid_alive = InvalidTransactionId
	},
	.replication_scratch = {
		.replorigin_xact = {
			.origin = InvalidReplOriginId,
			.origin_lsn = InvalidXLogRecPtr,
			.origin_timestamp = 0
		}
	},
	.catalog = {
		.currently_reindexed_heap = InvalidOid,
		.currently_reindexed_index = InvalidOid
	}
};

#define early_execution_debug early_execution_fallback.debug
#define early_execution_error early_execution_fallback.error
#define early_execution_memory_contexts early_execution_fallback.memory_contexts
#define early_execution_resource_owners early_execution_fallback.resource_owners
#define early_execution_spi early_execution_fallback.spi
#define early_execution_portal early_execution_fallback.portal
#define early_execution_vacuum early_execution_fallback.vacuum
#define early_execution_node_io early_execution_fallback.node_io
#define early_execution_basebackup early_execution_fallback.basebackup
#define early_execution_analyze early_execution_fallback.analyze
#define early_execution_extension early_execution_fallback.extension
#define early_execution_matview early_execution_fallback.matview
#define early_execution_snapshot early_execution_fallback.snapshot
#define early_execution_combo_cid early_execution_fallback.combo_cid
#define early_execution_xloginsert early_execution_fallback.xloginsert
#define early_execution_xact early_execution_fallback.xact
#define early_execution_transaction_cleanup early_execution_fallback.transaction_cleanup
#define early_execution_replication_scratch early_execution_fallback.replication_scratch
#define early_execution_guc_error early_execution_fallback.guc_error
#define early_execution_async early_execution_fallback.async
#define early_execution_catalog early_execution_fallback.catalog
#define early_execution_catalog_cache early_execution_fallback.catalog_cache
#define early_execution_relmap early_execution_fallback.relmap
#define early_execution_invalidation early_execution_fallback.invalidation
#define early_execution_two_phase_records early_execution_fallback.two_phase_records
#define early_execution_trigger early_execution_fallback.trigger
#define early_execution_regex early_execution_fallback.regex
#define early_execution_valgrind early_execution_fallback.valgrind
#define early_execution_snapbuild early_execution_fallback.snapbuild

StaticAssertDecl(PG_BACKEND_INTERRUPT_COUNT <= 32,
				 "PgBackendInterruptMask must fit all backend interrupts");
StaticAssertDecl(PG_EXECUTION_UNREPORTED_XIDS_CAPACITY == PGPROC_MAX_CACHED_SUBXIDS,
				 "PgExecution xact unreported XID storage must match PGPROC");

static void PgBackendInitializeIdCounter(void);
static PgBackendId PgBackendAssignId(void);
static void PgBackendWakeForInterrupt(PgBackend *backend);
static void PgRuntimeInitializeServerGUCState(PgRuntimeServerGUCState *server_guc);
static void PgRuntimeAdoptEarlyServerGUCState(PgRuntime *runtime);
static bool PgRuntimeServerGUCStateHasConfigPaths(PgRuntimeServerGUCState *server_guc);
static void PgRuntimeInitializeExtensionModuleState(PgRuntimeExtensionModuleState *extension_modules);
static MemoryContext PgRuntimeEnsureExtensionModuleMemoryContext(PgRuntimeExtensionModuleState *extension_modules);
static void PgRuntimeAdoptEarlyExtensionModuleState(PgRuntime *runtime);
PgRuntimeServerGUCState *PgCurrentRuntimeServerGUCState(void);
static void PgConnectionAdoptEarlyIdentity(PgConnection *connection);
static void PgConnectionAdoptEarlySocketIO(PgConnection *connection);
static void PgConnectionAdoptEarlyProtocolState(PgConnection *connection);
static void PgConnectionInitializeOutputState(PgConnectionOutputState *output);
static void PgConnectionAdoptEarlyOutputState(PgConnection *connection);
static void PgConnectionInitializeStartupState(PgConnectionStartupState *startup);
static void PgConnectionAdoptEarlyInterruptState(PgConnection *connection);
static void PgConnectionAdoptEarlyStartupState(PgConnection *connection);
static void PgConnectionAdoptEarlyClientConnectionInfo(PgConnection *connection);
static void PgConnectionAdoptEarlyClientConnectionInfoContext(PgConnection *connection);
static void PgConnectionAdoptEarlyClientConnectionInfoAuthnIdOwned(PgConnection *connection);
static void PgConnectionResetClientConnectionInfoClosedState(PgConnection *connection);
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
void PgSessionInitializeVacuumState(PgSessionVacuumState *vacuum);
static void PgSessionAdoptEarlyVacuumState(PgSession *session);
static void PgSessionInitializeBufferIOState(PgSessionBufferIOState *buffer_io);
static void PgSessionAdoptEarlyBufferIOState(PgSession *session);
static void PgSessionInitializeXactDefaultState(PgSessionXactDefaultState *xact_defaults);
static void PgSessionAdoptEarlyXactDefaultState(PgSession *session);
void PgSessionInitializeLockWaitState(PgSessionLockWaitState *lock_wait);
static void PgSessionAdoptEarlyLockWaitState(PgSession *session);
static void PgSessionInitializeLoggingState(PgSessionLoggingState *logging);
static void PgSessionAdoptEarlyLoggingState(PgSession *session);
static void PgSessionInitializeMiscGUCState(PgSessionMiscGUCState *misc_guc);
static void PgSessionAdoptEarlyMiscGUCState(PgSession *session);
static void PgSessionAdoptEarlyGUCState(PgSession *session);
void PgSessionInitializePgStatState(PgSessionPgStatState *pgstat);
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
static void PgSessionInitializeLogicalReplicationState(PgSessionLogicalReplicationState *logical_replication);
static void PgSessionAdoptEarlyLogicalReplicationState(PgSession *session);
static void PgMoveDListHead(dlist_head *dst, dlist_head *src);
static void PgMoveDCListHead(dclist_head *dst, dclist_head *src);
static void PgSessionInitializeGeneralGUCState(PgSessionGeneralGUCState *general_guc);
static void PgSessionAdoptEarlyGeneralGUCState(PgSession *session);
static void PgSessionInitializeAccessWalGUCState(PgSessionAccessWalGUCState *access_wal_guc);
static void PgSessionAdoptEarlyAccessWalGUCState(PgSession *session);
static void PgSessionInitializeJitGUCState(PgSessionJitGUCState *jit_guc);
static void PgSessionAdoptEarlyJitGUCState(PgSession *session);
static void PgSessionInitializeJitProviderState(PgSessionJitProviderState *jit_provider_state);
static void PgSessionAdoptEarlyJitProviderState(PgSession *session);
static void PgSessionInitializeLLVMJitState(PgSessionLLVMJitState *llvm_jit);
static void PgSessionAdoptEarlyLLVMJitState(PgSession *session);
static void PgSessionInitializeSortGUCState(PgSessionSortGUCState *sort_guc);
static void PgSessionAdoptEarlySortGUCState(PgSession *session);
static void PgSessionInitializeQueryMemoryState(PgSessionQueryMemoryState *query_memory);
static void PgSessionAdoptEarlyQueryMemoryState(PgSession *session);
static void PgSessionInitializePlannerCostState(PgSessionPlannerCostState *planner_cost);
static void PgSessionAdoptEarlyPlannerCostState(PgSession *session);
static void PgSessionInitializePlannerMethodState(PgSessionPlannerMethodState *planner_method);
static void PgSessionAdoptEarlyPlannerMethodState(PgSession *session);
static void PgSessionInitializeFunctionManagerState(PgSessionFunctionManagerState *function_manager);
static void PgSessionAdoptEarlyFunctionManagerState(PgSession *session);
static void PgSessionAdoptEarlyExtensionModuleState(PgSession *session);
static void PgSessionInitializeCatalogLookupState(PgSessionCatalogLookupState *catalog_lookup);
static void PgSessionAdoptEarlyCatalogLookupState(PgSession *session);
static void PgSessionAdoptEarlyInvalidationCallbackState(PgSession *session);
static void PgSessionInitializeRIGlobalsState(PgSessionRIGlobalsState *ri_globals);
static void PgSessionAdoptEarlyRIGlobalsState(PgSession *session);
static void PgSessionAdoptEarlyRelMapState(PgSession *session);
static void PgSessionInitializePreparedStatementState(PgSessionPreparedStatementState *prepared_statement);
static void PgSessionAdoptEarlyPreparedStatementState(PgSession *session);
static void PgSessionInitializeOnCommitState(PgSessionOnCommitState *on_commit);
static void PgSessionAdoptEarlyOnCommitState(PgSession *session);
static void PgSessionInitializeSequenceState(PgSessionSequenceState *sequence);
static void PgSessionAdoptEarlySequenceState(PgSession *session);
static void PgSessionInitializeXactCallbackState(PgSessionXactCallbackState *xact_callbacks);
static void PgSessionAdoptEarlyXactCallbackState(PgSession *session);
static void PgSessionInitializeBackupState(PgSessionBackupState *backup);
static void PgSessionAdoptEarlyBackupState(PgSession *session);
static void PgSessionAdoptEarlyRegexState(PgSession *session);
static void PgSessionAdoptEarlyPortalManagerState(PgSession *session);
void PgSessionInitializeLargeObjectState(PgSessionLargeObjectState *large_object);
static void PgSessionAdoptEarlyLargeObjectState(PgSession *session);
static void PgSessionInitializeAsyncState(PgSessionAsyncState *async);
static void PgSessionAdoptEarlyAsyncState(PgSession *session);
static void PgSessionEnsureEncodingStateInitialized(PgSessionEncodingState *encoding);
static void PgSessionAdoptEarlyEncodingState(PgSession *session);
void PgSessionInitializeTempFileState(PgSessionTempFileState *temp_file);
static void PgSessionAdoptEarlyTempFileState(PgSession *session);
static void PgSessionInitializeRandomState(PgSessionRandomState *random);
static void PgSessionAdoptEarlyRandomState(PgSession *session);
static void PgSessionInitializeOptimizerState(PgSessionOptimizerState *optimizer);
static void PgSessionAdoptEarlyOptimizerState(PgSession *session);
void PgSessionInitializePlanCacheState(PgSessionPlanCacheState *plan_cache);
static void PgSessionAdoptEarlyPlanCacheState(PgSession *session);
void PgSessionInitializeNamespaceState(PgSessionNamespaceState *namespace_state);
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
static void PgSessionInitializeTcopState(PgSessionTcopState *tcop);
static void PgSessionAdoptEarlyTcopState(PgSession *session);
static void PgBackendInitializeProcNumberState(PgBackend *backend);
static void PgBackendAdoptEarlyCoreState(PgBackend *backend);
static void PgBackendAdoptEarlyMyProc(PgBackend *backend);
static void PgBackendAdoptEarlyProcNumberState(PgBackend *backend);
static void PgBackendAdoptEarlyMyBEEntry(PgBackend *backend);
static void PgBackendAdoptEarlyMyBgworkerEntry(PgBackend *backend);
static void PgBackendAdoptEarlyAuxProcessResourceOwner(PgBackend *backend);
void PgBackendInitializePgStatPendingState(PgBackendPgStatPendingState *pgstat_pending);
static void PgBackendAdoptEarlyPgStatPendingState(PgBackend *backend);
static void PgBackendInitializeActivityState(PgBackendActivityState *activity);
static void PgBackendAdoptEarlyActivityState(PgBackend *backend);
static void PgBackendInitializeMemoryManagerState(PgBackendMemoryManagerState *memory_manager);
static void PgBackendAdoptEarlyMemoryManagerState(PgBackend *backend);
static void PgBackendInitializeUtilityState(PgBackendUtilityState *utility);
static void PgBackendAdoptEarlyUtilityState(PgBackend *backend);
void PgBackendInitializeParallelState(PgBackendParallelState *parallel);
static void PgBackendAdoptEarlyParallelState(PgBackend *backend);
static void PgBackendInitializeInstrumentationState(PgBackendInstrumentationState *instrumentation);
static void PgBackendAdoptEarlyInstrumentationState(PgBackend *backend);
void PgBackendInitializeBufferState(PgBackendBufferState *buffers);
static void PgBackendAdoptEarlyBufferState(PgBackend *backend);
static void PgBackendAdoptEarlyStorageState(PgBackend *backend);
static void PgBackendAdoptEarlyLockState(PgBackend *backend);
void PgBackendInitializeIPCState(PgBackendIPCState *ipc);
static void PgBackendAdoptEarlyIPCState(PgBackend *backend);
static void PgBackendEnsureWaitStateInitialized(PgBackendWaitState *wait_state);
void PgBackendInitializeWaitState(PgBackendWaitState *wait_state);
static void PgBackendAdoptEarlyWaitState(PgBackend *backend);
void PgBackendInitializeTransactionState(PgBackendTransactionState *transaction);
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
void PgBackendInitializeRecoveryState(PgBackendRecoveryState *recovery);
static void PgBackendAdoptEarlyRecoveryState(PgBackend *backend);
static void PgBackendInitializeMaintenanceWorkerState(PgBackendMaintenanceWorkerState *maintenance_worker);
static void PgBackendAdoptEarlyMaintenanceWorkerState(PgBackend *backend);
static void PgBackendInitializeAutovacuumState(PgBackendAutovacuumState *autovacuum);
static void PgBackendAdoptEarlyAutovacuumState(PgBackend *backend);
void PgBackendInitializeRepackState(PgBackendRepackState *repack);
static void PgBackendAdoptEarlyRepackState(PgBackend *backend);
static void PgBackendInitializeAioState(PgBackendAioState *aio);
static void PgBackendAdoptEarlyAioState(PgBackend *backend);
static void PgBackendAdoptEarlyExtensionModuleState(PgBackend *backend);
static void PgBackendAdoptEarlyPendingInterrupts(PgBackend *backend);
static void PgBackendAdoptEarlyInterruptHoldoffs(PgBackend *backend);
static BackendType *PgCurrentBackendTypeRef(void);
static void PgExecutionAdoptEarlyDebugState(PgExecution *execution);
static void PgExecutionAdoptEarlyErrorState(PgExecution *execution);
static void PgExecutionAdoptEarlyMemoryContexts(PgExecution *execution);
static void PgExecutionAdoptEarlyResourceOwners(PgExecution *execution);
static void PgExecutionAdoptEarlySPIState(PgExecution *execution);
static void PgExecutionAdoptEarlyPortalState(PgExecution *execution);
static void PgExecutionAdoptEarlyVacuumState(PgExecution *execution);
static void PgExecutionAdoptEarlyNodeIOState(PgExecution *execution);
static void PgExecutionAdoptEarlyBaseBackupState(PgExecution *execution);
static void PgExecutionAdoptEarlyAnalyzeState(PgExecution *execution);
static void PgExecutionAdoptEarlyExtensionState(PgExecution *execution);
static void PgExecutionAdoptEarlyMatViewState(PgExecution *execution);
static void PgExecutionAdoptEarlySnapshotState(PgExecution *execution);
static void PgExecutionAdoptEarlyComboCidState(PgExecution *execution);
static void PgExecutionAdoptEarlyXLogInsertState(PgExecution *execution);
static void PgExecutionAdoptEarlyXactState(PgExecution *execution);
static void PgExecutionAdoptEarlyTransactionCleanupState(PgExecution *execution);
static void PgExecutionAdoptEarlyReplicationScratchState(PgExecution *execution);
static void PgExecutionAdoptEarlyGUCErrorState(PgExecution *execution);
static void PgExecutionAdoptEarlyAsyncState(PgExecution *execution);
static void PgExecutionAdoptEarlyCatalogState(PgExecution *execution);
static void PgExecutionAdoptEarlyCatalogCacheState(PgExecution *execution);
static void PgExecutionAdoptEarlyRelMapState(PgExecution *execution);
static void PgExecutionAdoptEarlyInvalidationState(PgExecution *execution);
static void PgExecutionAdoptEarlyTwoPhaseRecordState(PgExecution *execution);
static void PgExecutionAdoptEarlyTriggerState(PgExecution *execution);
static void PgExecutionAdoptEarlyRegexState(PgExecution *execution);
static void PgExecutionAdoptEarlyValgrindState(PgExecution *execution);
static void PgExecutionAdoptEarlySnapBuildState(PgExecution *execution);
static PgBackendCoreState *PgCurrentCoreState(void);
PgSessionTcopState *PgCurrentSessionTcopState(void);
PgSessionDatabaseState *PgCurrentSessionDatabaseState(void);
PgSessionTablespaceState *PgCurrentSessionTablespaceState(void);
PgSessionBinaryUpgradeState *PgCurrentSessionBinaryUpgradeState(void);
PgSessionDateTimeState *PgCurrentSessionDateTimeState(void);
PgSessionTextSearchState *PgCurrentSessionTextSearchState(void);
PgSessionConnectionGUCState *PgCurrentSessionConnectionGUCState(void);
PgSessionParserState *PgCurrentSessionParserState(void);
PgSessionVacuumState *PgCurrentSessionVacuumState(void);
PgSessionBufferIOState *PgCurrentSessionBufferIOState(void);
PgSessionXactDefaultState *PgCurrentSessionXactDefaultState(void);
PgSessionLockWaitState *PgCurrentSessionLockWaitState(void);
PgSessionLoggingState *PgCurrentSessionLoggingState(void);
PgSessionMiscGUCState *PgCurrentSessionMiscGUCState(void);
PgSessionGUCState *PgCurrentSessionGUCState(void);
PgSessionPgStatState *PgCurrentSessionPgStatState(void);
PgSessionQueryIdState *PgCurrentSessionQueryIdState(void);
PgSessionStorageGUCState *PgCurrentSessionStorageGUCState(void);
PgSessionUserGUCState *PgCurrentSessionUserGUCState(void);
static PgSessionUserIdentityState *PgCurrentSessionUserIdentityState(void);
PgSessionCommandGUCState *PgCurrentSessionCommandGUCState(void);
PgSessionReplicationGUCState *PgCurrentSessionReplicationGUCState(void);
PgSessionLogicalReplicationState *PgCurrentSessionLogicalReplicationState(void);
PgSessionGeneralGUCState *PgCurrentSessionGeneralGUCState(void);
PgSessionAccessWalGUCState *PgCurrentSessionAccessWalGUCState(void);
PgSessionJitGUCState *PgCurrentSessionJitGUCState(void);
PgSessionJitProviderState *PgCurrentSessionJitProviderState(void);
PgSessionLLVMJitState *PgCurrentSessionLLVMJitState(void);
PgSessionSortGUCState *PgCurrentSessionSortGUCState(void);
PgSessionQueryMemoryState *PgCurrentSessionQueryMemoryState(void);
PgSessionPlannerCostState *PgCurrentSessionPlannerCostState(void);
PgSessionPlannerMethodState *PgCurrentSessionPlannerMethodState(void);
PgSessionFunctionManagerState *PgCurrentSessionFunctionManagerState(void);
PgSessionExtensionModuleState *PgCurrentSessionExtensionModuleState(void);
PgSessionCatalogLookupState *PgCurrentSessionCatalogLookupState(void);
PgSessionInvalidationCallbackState *PgCurrentSessionInvalidationCallbackState(void);
PgSessionRIGlobalsState *PgCurrentSessionRIGlobalsState(void);
PgSessionRelMapState *PgCurrentSessionRelMapState(void);
PgSessionPreparedStatementState *PgCurrentSessionPreparedStatementState(void);
PgSessionOnCommitState *PgCurrentSessionOnCommitState(void);
PgSessionSequenceState *PgCurrentSessionSequenceState(void);
PgSessionXactCallbackState *PgCurrentSessionXactCallbackState(void);
PgSessionBackupState *PgCurrentSessionBackupState(void);
PgSessionRegexState *PgCurrentSessionRegexState(void);
PgSessionPortalManagerState *PgCurrentSessionPortalManagerState(void);
PgSessionLargeObjectState *PgCurrentSessionLargeObjectState(void);
static PgSessionAsyncState *PgCurrentSessionAsyncState(void);
PgSessionEncodingState *PgCurrentSessionEncodingState(void);
PgSessionTempFileState *PgCurrentSessionTempFileState(void);
static PgSessionRandomState *PgCurrentSessionRandomState(void);
static PgSessionOptimizerState *PgCurrentSessionOptimizerState(void);
PgSessionPlanCacheState *PgCurrentSessionPlanCacheState(void);
PgSessionNamespaceState *PgCurrentSessionNamespaceState(void);
PgSessionLocaleState *PgCurrentSessionLocaleState(void);
PgExecutionErrorState *PgCurrentExecutionErrorState(void);
PgExecutionMemoryContextState *PgCurrentExecutionMemoryContexts(void);
PgExecutionResourceOwnerState *PgCurrentExecutionResourceOwners(void);
PgExecutionSPIState *PgCurrentExecutionSPIState(void);
static PgExecutionPortalState *PgCurrentExecutionPortalState(void);
PgExecutionVacuumState *PgCurrentExecutionVacuumState(void);
static PgExecutionNodeIOState *PgCurrentExecutionNodeIOState(void);
static PgExecutionBaseBackupState *PgCurrentExecutionBaseBackupState(void);
PgExecutionAnalyzeState *PgCurrentExecutionAnalyzeState(void);
static PgExecutionMatViewState *PgCurrentExecutionMatViewState(void);
PgExecutionSnapshotState *PgCurrentExecutionSnapshotState(void);
PgExecutionComboCidState *PgCurrentExecutionComboCidState(void);
PgExecutionXLogInsertState *PgCurrentExecutionXLogInsertState(void);
PgExecutionXactState *PgCurrentExecutionXactState(void);
static PgExecutionTransactionCleanupState *PgCurrentExecutionTransactionCleanupState(void);
static PgExecutionReplicationScratchState *PgCurrentExecutionReplicationScratchState(void);
PgExecutionGUCErrorState *PgCurrentExecutionGUCErrorState(void);
static PgExecutionAsyncState *PgCurrentExecutionAsyncState(void);
static PgExecutionCatalogState *PgCurrentExecutionCatalogState(void);
static PgExecutionCatalogCacheState *PgCurrentExecutionCatalogCacheState(void);
static PgExecutionRelMapState *PgCurrentExecutionRelMapState(void);
static PgExecutionInvalidationState *PgCurrentExecutionInvalidationState(void);
static PgExecutionTwoPhaseRecordState *PgCurrentExecutionTwoPhaseRecordState(void);
PgExecutionRegexState *PgCurrentExecutionRegexState(void);
static PgExecutionValgrindState *PgCurrentExecutionValgrindState(void);
static PgExecutionSnapBuildState *PgCurrentExecutionSnapBuildState(void);
PgBackendPgStatPendingState *PgCurrentBackendPgStatPendingState(void);
PgBackendInstrumentationState *PgCurrentBackendInstrumentationState(void);
PgBackendTransactionState *PgCurrentBackendTransactionState(void);
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

	/*
	 * Runtime server GUC strings describe address-space state selected during
	 * postmaster startup.  Auxiliary threads can initialize process runtime
	 * state more than once, so keep the early fallback as a persistent mirror
	 * rather than consuming it on first adoption.
	 */
	runtime->server_guc = early_runtime_server_guc;
}

static bool
PgRuntimeServerGUCStateHasConfigPaths(PgRuntimeServerGUCState *server_guc)
{
	return server_guc != NULL &&
		server_guc->initialized &&
		server_guc->config_file_name != NULL &&
		server_guc->config_file_name[0] != '\0';
}

static void
PgRuntimeInitializeExtensionModuleState(PgRuntimeExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	extension_modules->memory_context = NULL;
	extension_modules->pg_plan_advice_context = NULL;
	extension_modules->pg_plan_advice_advisor_hook_list = NIL;
	extension_modules->bloom_context = NULL;
	extension_modules->rendezvous_hash = NULL;
}

static MemoryContext
PgRuntimeEnsureExtensionModuleMemoryContext(PgRuntimeExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	if (extension_modules->memory_context == NULL)
	{
		if (CurrentPgRuntime != NULL &&
			CurrentPgRuntime->kind == PG_RUNTIME_THREAD_PER_SESSION)
			elog(ERROR,
				 "thread runtime extension module memory context is not initialized");

		extension_modules->memory_context =
			AllocSetContextCreate(TopMemoryContext,
								  "RuntimeExtensionModules",
								  ALLOCSET_DEFAULT_SIZES);
	}

	return extension_modules->memory_context;
}

static void
PgRuntimeAdoptEarlyExtensionModuleState(PgRuntime *runtime)
{
	Assert(runtime != NULL);

	runtime->extension_modules = early_runtime_extension_modules;
	PgRuntimeInitializeExtensionModuleState(&early_runtime_extension_modules);
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlyIdentity,
								   PgConnection, connection, identity,
								   early_connection_identity)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlySocketIO,
								   PgConnection, connection, socket_io,
								   early_connection_socket_io)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlyProtocolState,
								   PgConnection, connection, protocol,
								   early_connection_protocol)

static void
PgConnectionInitializeOutputState(PgConnectionOutputState *output)
{
	Assert(output != NULL);

	MemSet(output, 0, sizeof(*output));
	output->where_to_send_output = DestDebug;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgConnectionAdoptEarlyOutputState,
										PgConnection, connection, output,
										early_connection_output,
										PgConnectionInitializeOutputState)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlyInterruptState,
								   PgConnection, connection, interrupts,
								   early_connection_interrupts)

static void
PgConnectionInitializeStartupState(PgConnectionStartupState *startup)
{
	Assert(startup != NULL);

	MemSet(startup, 0, sizeof(*startup));
	startup->timing.ready_for_use = TIMESTAMP_MINUS_INFINITY;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgConnectionAdoptEarlyStartupState,
										PgConnection, connection, startup,
										early_connection_startup,
										PgConnectionInitializeStartupState)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlyClientConnectionInfo,
								   PgConnection, connection,
								   client_connection_info,
								   early_client_connection_info)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlyClientConnectionInfoContext,
								   PgConnection, connection,
								   client_connection_info_context,
								   early_client_connection_info_context)

static void
PgConnectionAdoptEarlyClientConnectionInfoAuthnIdOwned(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->client_connection_info_authn_id_owned =
		early_client_connection_info_authn_id_owned;
	early_client_connection_info_authn_id_owned = false;
}

static void
PgConnectionResetClientConnectionInfoClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

	if (connection->client_connection_info_authn_id_owned &&
		connection->client_connection_info.authn_id != NULL &&
		connection->client_connection_info_context == NULL)
		pfree((void *) connection->client_connection_info.authn_id);

	MemSet(&connection->client_connection_info, 0,
		   sizeof(connection->client_connection_info));
	connection->client_connection_info_authn_id_owned = false;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgConnectionAdoptEarlySecurityState,
								   PgConnection, connection, security,
								   early_connection_security)

static void
PgConnectionResetIdentityClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

	if (CurrentPgConnection == connection &&
		MyProcPort == connection->identity.port)
		MyProcPort = NULL;

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(connection->identity.port_context);
	connection->identity.port = NULL;
	MemSet(connection->identity.cancel_key, 0,
		   sizeof(connection->identity.cancel_key));
	connection->identity.cancel_key_length = 0;
}

static void
PgConnectionResetSocketIOClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

	/*
	 * socket_close() releases the palloc-backed send buffer and wait set.
	 * This reset makes the retained logical connection object stop pointing
	 * at resources that no longer exist.
	 */
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(connection->socket_io.socket_io_context);
	MemSet(&connection->socket_io, 0, sizeof(connection->socket_io));
}

static void
PgConnectionResetProtocolClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->protocol.comm_methods = NULL;
	connection->protocol.fe_be_wait_set = NULL;
	connection->protocol.frontend_protocol = 0;
}

static void
PgConnectionResetStartupClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

	connection->startup.client_auth_in_progress = false;
	connection->startup.client_socket = NULL;
	if (connection->startup.connection_warning_context != NULL)
	{
		if (CurrentMemoryContext == connection->startup.connection_warning_context)
			MemoryContextSwitchTo(TopMemoryContext);
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(connection->startup.connection_warning_context);
	}
	else
	{
		list_free_deep(connection->startup.connection_warning_messages);
		list_free_deep(connection->startup.connection_warning_details);
	}
	connection->startup.connection_warnings_emitted = false;
	connection->startup.connection_warning_messages = NIL;
	connection->startup.connection_warning_details = NIL;
}

static void
PgConnectionResetSecurityClosedState(PgConnection *connection)
{
	PgConnectionSecurityState *security;

	Assert(connection != NULL);

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
PgConnectionResetClosedState(PgConnection *connection)
{
	Assert(connection != NULL);

#define PG_CONNECTION_BUCKET(field, init, adopt, reset) \
	do { reset; } while (0);
#include "backend_runtime_connection_buckets.def"
#undef PG_CONNECTION_BUCKET
}

void
PgConnectionAdoptEarlyState(PgConnection *connection,
							struct Port *preserved_port)
{
	Assert(connection != NULL);

#define PG_CONNECTION_BUCKET(field, init, adopt, reset) \
	do { adopt; } while (0);
#include "backend_runtime_connection_buckets.def"
#undef PG_CONNECTION_BUCKET
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgSessionAdoptEarlyDatabaseState,
								   PgSession, session, database,
								   early_session_database)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyTablespaceState,
										  PgSession, session, tablespace,
										  early_session_tablespace,
										  PgSessionInitializeTablespaceState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyBinaryUpgradeState,
										  PgSession, session, binary_upgrade,
										  early_session_binary_upgrade,
										  PgSessionInitializeBinaryUpgradeState)

static void
PgSessionInitializeDateTimeState(PgSessionDateTimeState *datetime)
{
	Assert(datetime != NULL);

	datetime->initialized = true;
	datetime->date_style = USE_ISO_DATES;
	datetime->date_order = DATEORDER_MDY;
	datetime->interval_style = INTSTYLE_POSTGRES;
	datetime->datestyle_string_value = guc_strdup(FATAL, "ISO, MDY");
	datetime->timezone_string_value = guc_strdup(FATAL, "GMT");
	datetime->log_timezone_string_value = guc_strdup(FATAL, "GMT");
	datetime->timezone_abbreviations_string_value = NULL;
	datetime->session_timezone_value = pg_tzset("GMT");
	datetime->log_timezone_value = datetime->session_timezone_value;
	datetime->timezone_abbrev_table = NULL;
	MemSet(datetime->timezone_abbrev_cache, 0,
		   sizeof(datetime->timezone_abbrev_cache));
	datetime->current_time_cache_ts = 0;
	datetime->current_time_cache_timezone = NULL;
	MemSet(&datetime->current_time_cache_tm, 0,
		   sizeof(datetime->current_time_cache_tm));
	datetime->current_time_cache_fsec = 0;
	datetime->current_time_cache_tz = 0;
}

static void
PgSessionResetEarlyDateTimeState(PgSessionDateTimeState *datetime)
{
	Assert(datetime != NULL);

	datetime->initialized = false;
	datetime->date_style = USE_ISO_DATES;
	datetime->date_order = DATEORDER_MDY;
	datetime->interval_style = INTSTYLE_POSTGRES;
	datetime->datestyle_string_value = NULL;
	datetime->timezone_string_value = NULL;
	datetime->log_timezone_string_value = NULL;
	datetime->timezone_abbreviations_string_value = NULL;
	datetime->session_timezone_value = NULL;
	datetime->log_timezone_value = NULL;
	datetime->timezone_abbrev_table = NULL;
	MemSet(datetime->timezone_abbrev_cache, 0,
		   sizeof(datetime->timezone_abbrev_cache));
	datetime->current_time_cache_ts = 0;
	datetime->current_time_cache_timezone = NULL;
	MemSet(&datetime->current_time_cache_tm, 0,
		   sizeof(datetime->current_time_cache_tm));
	datetime->current_time_cache_fsec = 0;
	datetime->current_time_cache_tz = 0;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED_WITH_RESET(PgSessionAdoptEarlyDateTimeState,
													 PgSession, session,
													 datetime,
													 early_session_datetime,
													 PgSessionInitializeDateTimeState,
													 PgSessionResetEarlyDateTimeState)

static void
PgSessionInitializeTextSearchState(PgSessionTextSearchState *text_search)
{
	Assert(text_search != NULL);

	MemSet(text_search, 0, sizeof(*text_search));
	text_search->initialized = true;
	text_search->current_config_value = guc_strdup(FATAL, "pg_catalog.simple");
	text_search->current_config_cache = InvalidOid;
}

static void
PgSessionResetEarlyTextSearchState(PgSessionTextSearchState *text_search)
{
	Assert(text_search != NULL);

	MemSet(text_search, 0, sizeof(*text_search));
	text_search->initialized = false;
	text_search->current_config_cache = InvalidOid;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED_WITH_RESET(PgSessionAdoptEarlyTextSearchState,
													 PgSession, session,
													 text_search,
													 early_session_text_search,
													 PgSessionInitializeTextSearchState,
													 PgSessionResetEarlyTextSearchState)

static void
PgSessionInitializeConnectionGUCState(PgSessionConnectionGUCState *connection_guc)
{
	Assert(connection_guc != NULL);

	connection_guc->initialized = true;
	connection_guc->application_name_value = guc_strdup(FATAL, "");
	connection_guc->ssl_renegotiation_limit_value = 0;
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
PgSessionResetEarlyConnectionGUCState(PgSessionConnectionGUCState *connection_guc)
{
	Assert(connection_guc != NULL);

	connection_guc->initialized = false;
	connection_guc->application_name_value = NULL;
	connection_guc->ssl_renegotiation_limit_value = 0;
	connection_guc->tcp_keepalives_idle_value = 0;
	connection_guc->tcp_keepalives_interval_value = 0;
	connection_guc->tcp_keepalives_count_value = 0;
	connection_guc->tcp_user_timeout_value = 0;
	connection_guc->log_disconnections_value = false;
	connection_guc->log_statement_value = 0;
	connection_guc->post_auth_delay_seconds = 0;
	connection_guc->restrict_nonsystem_relation_kind_string_value = NULL;
	connection_guc->restrict_nonsystem_relation_kind_value = 0;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED_WITH_RESET(PgSessionAdoptEarlyConnectionGUCState,
													 PgSession, session,
													 connection_guc,
													 early_session_connection_guc,
													 PgSessionInitializeConnectionGUCState,
													 PgSessionResetEarlyConnectionGUCState)

static void
PgSessionInitializeParserState(PgSessionParserState *parser)
{
	Assert(parser != NULL);

	parser->initialized = true;
	parser->transform_null_equals_value = false;
	parser->backslash_quote_value = BACKSLASH_QUOTE_SAFE_ENCODING;
	parser->operator_lookup_cache = NULL;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyParserState,
										  PgSession, session, parser,
										  early_session_parser,
										  PgSessionInitializeParserState)

void
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyVacuumState,
										  PgSession, session, vacuum,
										  early_session_vacuum,
										  PgSessionInitializeVacuumState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyBufferIOState,
										  PgSession, session, buffer_io,
										  early_session_buffer_io,
										  PgSessionInitializeBufferIOState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyXactDefaultState,
										  PgSession, session, xact_defaults,
										  early_session_xact_defaults,
										  PgSessionInitializeXactDefaultState)

void
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyLockWaitState,
										  PgSession, session, lock_wait,
										  early_session_lock_wait,
										  PgSessionInitializeLockWaitState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyLoggingState,
										  PgSession, session, logging,
										  early_session_logging,
										  PgSessionInitializeLoggingState)

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
	misc_guc->update_process_title_value = DEFAULT_UPDATE_PROCESS_TITLE;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyMiscGUCState,
										  PgSession, session, misc_guc,
										  early_session_misc_guc,
										  PgSessionInitializeMiscGUCState)

void
PgSessionInitializeGUCState(PgSessionGUCState *guc)
{
	Assert(guc != NULL);

	guc->initialized = true;
	guc->memory_context = NULL;
	guc->variables = NULL;
	guc->num_variables = 0;
	guc->hash_table = NULL;
	dlist_init(&guc->nondef_list);
	slist_init(&guc->stack_list);
	slist_init(&guc->report_list);
	guc->reporting_enabled = false;
	guc->nest_level = 0;
}

static void
PgSessionAdoptEarlyGUCState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_guc.initialized)
		PgSessionInitializeGUCState(&early_session_guc);

	session->guc = early_session_guc;
	PgMoveDListHead(&session->guc.nondef_list,
					&early_session_guc.nondef_list);
	PgSessionInitializeGUCState(&early_session_guc);
}

void
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyPgStatState,
										  PgSession, session, pgstat,
										  early_session_pgstat,
										  PgSessionInitializePgStatState)

static void
PgSessionInitializeQueryIdState(PgSessionQueryIdState *query_id)
{
	Assert(query_id != NULL);

	query_id->initialized = true;
	query_id->compute_query_id_value = COMPUTE_QUERY_ID_AUTO;
	query_id->query_id_enabled_value = false;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyQueryIdState,
										  PgSession, session, query_id,
										  early_session_query_id,
										  PgSessionInitializeQueryIdState)

static void
PgSessionInitializeStorageGUCState(PgSessionStorageGUCState *storage_guc)
{
	Assert(storage_guc != NULL);

	storage_guc->initialized = true;
	storage_guc->ignore_checksum_failure_value = false;
	storage_guc->file_copy_method_value = FILE_COPY_METHOD_COPY;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyStorageGUCState,
										  PgSession, session, storage_guc,
										  early_session_storage_guc,
										  PgSessionInitializeStorageGUCState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyUserGUCState,
										  PgSession, session, user_guc,
										  early_session_user_guc,
										  PgSessionInitializeUserGUCState)

static void
PgSessionInitializeUserIdentityState(PgSessionUserIdentityState *user_identity)
{
	Assert(user_identity != NULL);

	user_identity->authenticated_user_id = InvalidOid;
	user_identity->session_user_id = InvalidOid;
	user_identity->outer_user_id = InvalidOid;
	user_identity->current_user_id = InvalidOid;
	user_identity->system_user = NULL;
	user_identity->system_user_context = NULL;
	user_identity->system_user_owned = false;
	user_identity->session_user_is_superuser = false;
	user_identity->security_restriction_context = 0;
	user_identity->set_role_is_active = false;
	user_identity->cached_role[0] = InvalidOid;
	user_identity->cached_role[1] = InvalidOid;
	user_identity->cached_role[2] = InvalidOid;
	user_identity->cached_roles[0] = NIL;
	user_identity->cached_roles[1] = NIL;
	user_identity->cached_roles[2] = NIL;
	user_identity->cached_db_hash = 0;
	user_identity->initialized = true;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyUserIdentityState,
										  PgSession, session, user_identity,
										  early_session_user_identity,
										  PgSessionInitializeUserIdentityState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyCommandGUCState,
										  PgSession, session, command_guc,
										  early_session_command_guc,
										  PgSessionInitializeCommandGUCState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyReplicationGUCState,
										  PgSession, session, replication_guc,
										  early_session_replication_guc,
										  PgSessionInitializeReplicationGUCState)

static void
PgSessionInitializeLogicalReplicationState(PgSessionLogicalReplicationState *logical_replication)
{
	Assert(logical_replication != NULL);

	MemSet(logical_replication, 0, sizeof(*logical_replication));
}

static void
PgSessionAdoptEarlyLogicalReplicationState(PgSession *session)
{
	Assert(session != NULL);
	Assert(early_session_logical_replication.session_replication_state == NULL);
	Assert(early_session_logical_replication.logical_rep_relmap_context == NULL);
	Assert(early_session_logical_replication.logical_rep_relmap == NULL);
	Assert(early_session_logical_replication.logical_rep_partmap_context == NULL);
	Assert(early_session_logical_replication.logical_rep_partmap == NULL);
	Assert(!early_session_logical_replication.pgoutput_publications_valid);
	Assert(early_session_logical_replication.pgoutput_relation_sync_cache == NULL);
	Assert(early_session_logical_replication.syncing_relations_state == 0);

	PgSessionInitializeLogicalReplicationState(&session->logical_replication);
	PgSessionInitializeLogicalReplicationState(&early_session_logical_replication);
}

static void
PgMoveDListHead(dlist_head *dst, dlist_head *src)
{
	Assert(dst != NULL);
	Assert(src != NULL);

	if (dlist_is_empty(src))
	{
		dlist_init(dst);
		return;
	}

	*dst = *src;
	dst->head.next->prev = &dst->head;
	dst->head.prev->next = &dst->head;
}

static void
PgMoveDCListHead(dclist_head *dst, dclist_head *src)
{
	Assert(dst != NULL);
	Assert(src != NULL);

	if (dclist_is_empty(src))
	{
		dclist_init(dst);
		return;
	}

	dst->dlist = src->dlist;
	dst->count = src->count;
	dst->dlist.head.next->prev = &dst->dlist.head;
	dst->dlist.head.prev->next = &dst->dlist.head;
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
	general_guc->default_with_oids_value = false;
	general_guc->standard_conforming_strings_value = true;
	general_guc->phony_random_seed_value = 0.0;
	general_guc->temp_file_limit_kb = -1;
	general_guc->num_temp_buffers_blocks = 1024;
	general_guc->role_string_value = "none";
	general_guc->session_authorization_string_value = NULL;
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyGeneralGUCState,
										  PgSession, session, general_guc,
										  early_session_general_guc,
										  PgSessionInitializeGeneralGUCState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyAccessWalGUCState,
										  PgSession, session, access_wal_guc,
										  early_session_access_wal_guc,
										  PgSessionInitializeAccessWalGUCState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyJitGUCState,
										  PgSession, session, jit_guc,
										  early_session_jit_guc,
										  PgSessionInitializeJitGUCState)

static void
PgSessionInitializeJitProviderState(PgSessionJitProviderState *jit_provider_state)
{
	Assert(jit_provider_state != NULL);

	MemSet(jit_provider_state, 0, sizeof(*jit_provider_state));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyJitProviderState,
										PgSession, session,
										jit_provider_state,
										early_session_jit_provider,
										PgSessionInitializeJitProviderState)

static void
PgSessionInitializeLLVMJitState(PgSessionLLVMJitState *llvm_jit)
{
	Assert(llvm_jit != NULL);

	MemSet(llvm_jit, 0, sizeof(*llvm_jit));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyLLVMJitState,
										PgSession, session, llvm_jit,
										early_session_llvm_jit,
										PgSessionInitializeLLVMJitState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlySortGUCState,
										  PgSession, session, sort_guc,
										  early_session_sort_guc,
										  PgSessionInitializeSortGUCState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyQueryMemoryState,
										  PgSession, session, query_memory,
										  early_session_query_memory,
										  PgSessionInitializeQueryMemoryState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyPlannerCostState,
										  PgSession, session, planner_cost,
										  early_session_planner_cost,
										  PgSessionInitializePlannerCostState)

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyPlannerMethodState,
										  PgSession, session, planner_method,
										  early_session_planner_method,
										  PgSessionInitializePlannerMethodState)

static void
PgSessionInitializeFunctionManagerState(PgSessionFunctionManagerState *function_manager)
{
	Assert(function_manager != NULL);

	function_manager->function_manager_context = NULL;
	function_manager->c_func_hash = NULL;
	function_manager->cached_function_hash = NULL;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyFunctionManagerState,
										PgSession, session,
										function_manager,
										early_session_function_manager,
										PgSessionInitializeFunctionManagerState)

void
PgSessionInitializeExtensionModuleState(PgSessionExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	extension_modules->plpgsql_state = NULL;
	extension_modules->plpython_procedure_cache = NULL;
	extension_modules->plpython_memory_context = NULL;
	extension_modules->plpython_reset_registered = false;
	extension_modules->plperl_memory_context = NULL;
	extension_modules->plperl_inited = false;
	extension_modules->plperl_interp_hash = NULL;
	extension_modules->plperl_proc_hash = NULL;
	extension_modules->plperl_active_interp = NULL;
	extension_modules->plperl_held_interp = NULL;
	extension_modules->plperl_use_strict = false;
	extension_modules->plperl_on_init = NULL;
	extension_modules->plperl_on_plperl_init = NULL;
	extension_modules->plperl_on_plperlu_init = NULL;
	extension_modules->plperl_ending = false;
	extension_modules->plperl_current_call_data = NULL;
	extension_modules->plperl_reset_registered = false;
	extension_modules->pltcl_memory_context = NULL;
	extension_modules->pltcl_start_proc = NULL;
	extension_modules->pltclu_start_proc = NULL;
	extension_modules->pltcl_hold_interp = NULL;
	extension_modules->pltcl_interp_hash = NULL;
	extension_modules->pltcl_proc_hash = NULL;
	extension_modules->pltcl_current_call_state = NULL;
	extension_modules->pltcl_reset_registered = false;
	extension_modules->plsample_memory_context = NULL;
	extension_modules->refint_foreign_plans = NULL;
	extension_modules->refint_num_foreign_plans = 0;
	extension_modules->refint_primary_plans = NULL;
	extension_modules->refint_num_primary_plans = 0;
	extension_modules->refint_reset_registered = false;
	extension_modules->auth_delay_milliseconds = 0;
	extension_modules->basebackup_to_shell_command = "";
	extension_modules->basebackup_to_shell_required_role = "";
	extension_modules->isn_weak = false;
	extension_modules->passwordcheck_min_password_length = 8;
	extension_modules->reset_callbacks = NIL;
	extension_modules->auto_explain_log_min_duration =
		AUTO_EXPLAIN_LOG_MIN_DURATION_DEFAULT;
	extension_modules->auto_explain_log_parameter_max_length =
		AUTO_EXPLAIN_LOG_PARAMETER_MAX_LENGTH_DEFAULT;
	extension_modules->auto_explain_log_analyze = false;
	extension_modules->auto_explain_log_verbose = false;
	extension_modules->auto_explain_log_buffers = false;
	extension_modules->auto_explain_log_io = false;
	extension_modules->auto_explain_log_wal = false;
	extension_modules->auto_explain_log_triggers = false;
	extension_modules->auto_explain_log_timing =
		AUTO_EXPLAIN_LOG_TIMING_DEFAULT;
	extension_modules->auto_explain_log_settings = false;
	extension_modules->auto_explain_log_format =
		AUTO_EXPLAIN_LOG_FORMAT_DEFAULT;
	extension_modules->auto_explain_log_level =
		AUTO_EXPLAIN_LOG_LEVEL_DEFAULT;
	extension_modules->auto_explain_log_nested_statements = false;
	extension_modules->auto_explain_sample_rate =
		AUTO_EXPLAIN_SAMPLE_RATE_DEFAULT;
	extension_modules->auto_explain_log_extension_options = NULL;
	extension_modules->auto_explain_extension_options = NULL;
	extension_modules->pg_trgm_similarity_threshold =
		PG_TRGM_SIMILARITY_THRESHOLD_DEFAULT;
	extension_modules->pg_trgm_word_similarity_threshold =
		PG_TRGM_WORD_SIMILARITY_THRESHOLD_DEFAULT;
	extension_modules->pg_trgm_strict_word_similarity_threshold =
		PG_TRGM_STRICT_WORD_SIMILARITY_THRESHOLD_DEFAULT;
	extension_modules->pg_plan_advice_advice = NULL;
	extension_modules->pg_plan_advice_always_store_advice_details = false;
	extension_modules->pg_plan_advice_always_explain_supplied_advice =
		PG_PLAN_ADVICE_ALWAYS_EXPLAIN_SUPPLIED_ADVICE_DEFAULT;
	extension_modules->pg_plan_advice_feedback_warnings = false;
	extension_modules->pg_plan_advice_trace_mask = false;
	extension_modules->pg_plan_advice_generate_advice = 0;
	extension_modules->pg_stash_advice_stash_name = "";
	extension_modules->sepgsql_context = NULL;
	extension_modules->sepgsql_avc_context = NULL;
	extension_modules->sepgsql_client_label_peer = NULL;
	extension_modules->sepgsql_client_label_pending = NIL;
	extension_modules->sepgsql_client_label_committed = NULL;
	extension_modules->sepgsql_client_label_func = NULL;
	memset(extension_modules->sepgsql_avc_slots, 0,
		   sizeof(extension_modules->sepgsql_avc_slots));
	extension_modules->sepgsql_avc_num_caches = 0;
	extension_modules->sepgsql_avc_lru_hint = 0;
	extension_modules->sepgsql_avc_threshold = 0;
	extension_modules->sepgsql_avc_unlabeled = NULL;
	memset(&extension_modules->pgcrypto_des, 0,
		   sizeof(extension_modules->pgcrypto_des));
	extension_modules->dblink_context = NULL;
	extension_modules->dblink_persistent_connection = NULL;
	extension_modules->dblink_remote_conn_hash = NULL;
	extension_modules->dblink_reset_registered = false;
	extension_modules->postgres_fdw_options_context = NULL;
	extension_modules->postgres_fdw_options = NULL;
	extension_modules->postgres_fdw_application_name = NULL;
	extension_modules->postgres_fdw_connection_hash = NULL;
	extension_modules->postgres_fdw_shippable_cache_hash = NULL;
	extension_modules->postgres_fdw_cursor_number = 0;
	extension_modules->postgres_fdw_prep_stmt_number = 0;
	extension_modules->postgres_fdw_xact_got_connection = false;
	extension_modules->postgres_fdw_read_only_level = 0;
	extension_modules->postgres_fdw_connection_callbacks_registered = false;
	extension_modules->postgres_fdw_shippable_callbacks_registered = false;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyExtensionModuleState,
										PgSession, session,
										extension_modules,
										early_session_extension_modules,
										PgSessionInitializeExtensionModuleState)

static void
PgSessionInitializeCatalogLookupState(PgSessionCatalogLookupState *catalog_lookup)
{
	Assert(catalog_lookup != NULL);

	MemSet(catalog_lookup, 0, sizeof(*catalog_lookup));
	catalog_lookup->typcache_tupledesc_id_counter =
		INVALID_TUPLEDESC_IDENTIFIER;
}

static void
PgSessionAdoptEarlyCatalogLookupState(PgSession *session)
{
	Assert(session != NULL);

	/*
	 * Early fallback state can be adopted before typcache paths have forced
	 * fallback initialization.  Keep copied sessions out of the reserved
	 * tupledesc-ID range.
	 */
	if (early_session_catalog_lookup.typcache_tupledesc_id_counter == 0)
		early_session_catalog_lookup.typcache_tupledesc_id_counter =
			INVALID_TUPLEDESC_IDENTIFIER;
	session->catalog_lookup = early_session_catalog_lookup;
	PgSessionInitializeCatalogLookupState(&early_session_catalog_lookup);
}

void
PgSessionInitializeInvalidationCallbackState(PgSessionInvalidationCallbackState *invalidation_callbacks)
{
	Assert(invalidation_callbacks != NULL);

	MemSet(invalidation_callbacks, 0, sizeof(*invalidation_callbacks));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyInvalidationCallbackState,
										PgSession, session,
										invalidation_callbacks,
										early_session_invalidation_callbacks,
										PgSessionInitializeInvalidationCallbackState)

static void
PgSessionInitializeRIGlobalsState(PgSessionRIGlobalsState *ri_globals)
{
	Assert(ri_globals != NULL);

	ri_globals->constraint_cache = NULL;
	ri_globals->query_cache = NULL;
	ri_globals->compare_cache = NULL;
	dclist_init(&ri_globals->constraint_cache_valid_list);
	ri_globals->fastpath_xact_callback_registered = false;
	ri_globals->debug_discard_caches_initialized = true;
	ri_globals->debug_discard_caches_value = DEFAULT_DEBUG_DISCARD_CACHES;
}

static void
PgSessionAdoptEarlyRIGlobalsState(PgSession *session)
{
	Assert(session != NULL);

	if (!early_session_ri_globals.debug_discard_caches_initialized)
		PgSessionInitializeRIGlobalsState(&early_session_ri_globals);

	session->ri_globals = early_session_ri_globals;
	PgMoveDCListHead(&session->ri_globals.constraint_cache_valid_list,
					 &early_session_ri_globals.constraint_cache_valid_list);
	PgSessionInitializeRIGlobalsState(&early_session_ri_globals);
}

void
PgSessionInitializeRelMapState(PgSessionRelMapState *relmap)
{
	Assert(relmap != NULL);

	MemSet(relmap, 0, sizeof(*relmap));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyRelMapState,
										PgSession, session, relmap,
										early_session_relmap,
										PgSessionInitializeRelMapState)

static void
PgSessionInitializePreparedStatementState(PgSessionPreparedStatementState *prepared_statement)
{
	Assert(prepared_statement != NULL);

	prepared_statement->prepared_queries = NULL;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyPreparedStatementState,
										PgSession, session,
										prepared_statement,
										early_session_prepared_statement,
										PgSessionInitializePreparedStatementState)

static void
PgSessionInitializeOnCommitState(PgSessionOnCommitState *on_commit)
{
	Assert(on_commit != NULL);

	on_commit->on_commits = NIL;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyOnCommitState,
										PgSession, session, on_commit,
										early_session_on_commit,
										PgSessionInitializeOnCommitState)

static void
PgSessionInitializeSequenceState(PgSessionSequenceState *sequence)
{
	Assert(sequence != NULL);

	sequence->seqhashtab = NULL;
	sequence->last_used_seq = NULL;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlySequenceState,
										PgSession, session, sequence,
										early_session_sequence,
										PgSessionInitializeSequenceState)

static void
PgSessionInitializeXactCallbackState(PgSessionXactCallbackState *xact_callbacks)
{
	Assert(xact_callbacks != NULL);

	xact_callbacks->xact_callbacks = NULL;
	xact_callbacks->subxact_callbacks = NULL;
	xact_callbacks->xact_callback_context = NULL;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyXactCallbackState,
										PgSession, session, xact_callbacks,
										early_session_xact_callbacks,
										PgSessionInitializeXactCallbackState)

static void
PgSessionInitializeBackupState(PgSessionBackupState *backup)
{
	Assert(backup != NULL);

	backup->backup_state = NULL;
	backup->tablespace_map = NULL;
	backup->backup_context = NULL;
	backup->session_backup_state = SESSION_BACKUP_NONE;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyBackupState,
										PgSession, session, backup,
										early_session_backup,
										PgSessionInitializeBackupState)

void
PgSessionInitializeRegexState(PgSessionRegexState *regex)
{
	Assert(regex != NULL);

	regex->regexp_cache_context = NULL;
	regex->num_cached_res = 0;
	MemSet(regex->cached_res, 0, sizeof(regex->cached_res));
	regex->ctype_cache_list = NULL;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyRegexState,
										PgSession, session, regex,
										early_session_regex,
										PgSessionInitializeRegexState)

void
PgSessionInitializePortalManagerState(PgSessionPortalManagerState *portal_manager)
{
	Assert(portal_manager != NULL);

	portal_manager->top_portal_context = NULL;
	portal_manager->portal_hash_table = NULL;
	portal_manager->unnamed_portal_count = 0;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyPortalManagerState,
										PgSession, session, portal_manager,
										early_session_portal_manager,
										PgSessionInitializePortalManagerState)

void
PgSessionInitializeLargeObjectState(PgSessionLargeObjectState *large_object)
{
	Assert(large_object != NULL);

	large_object->heap_relation = NULL;
	large_object->index_relation = NULL;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyLargeObjectState,
										PgSession, session, large_object,
										early_session_large_object,
										PgSessionInitializeLargeObjectState)

static void
PgSessionInitializeAsyncState(PgSessionAsyncState *async)
{
	Assert(async != NULL);

	async->local_channel_table = NULL;
	async->registered_listener = false;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyAsyncState,
										PgSession, session, async,
										early_session_async,
										PgSessionInitializeAsyncState)

void
PgSessionInitializeEncodingState(PgSessionEncodingState *encoding)
{
	Assert(encoding != NULL);

	encoding->conv_proc_list = NIL;
	encoding->encoding_cache_context = NULL;
	encoding->to_server_conv_proc = NULL;
	encoding->to_client_conv_proc = NULL;
	encoding->utf8_to_server_conv_proc = NULL;
	encoding->client_encoding_string_value = "SQL_ASCII";
	encoding->server_encoding_string_value = "SQL_ASCII";
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

void
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyTempFileState,
										  PgSession, session, temp_file,
										  early_session_temp_file,
										  PgSessionInitializeTempFileState)

static void
PgSessionInitializeRandomState(PgSessionRandomState *random)
{
	Assert(random != NULL);

	MemSet(&random->prng_state, 0, sizeof(random->prng_state));
	random->prng_seed_set = false;
	random->initialized = true;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyRandomState,
										  PgSession, session, random,
										  early_session_random,
										  PgSessionInitializeRandomState)

static void
PgSessionInitializeOptimizerState(PgSessionOptimizerState *optimizer)
{
	Assert(optimizer != NULL);

	optimizer->planner_extension_names = NULL;
	optimizer->planner_extension_names_assigned = 0;
	optimizer->planner_extension_names_allocated = 0;
	optimizer->opr_proof_cache_hash = NULL;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgSessionAdoptEarlyOptimizerState,
										PgSession, session, optimizer,
										early_session_optimizer,
										PgSessionInitializeOptimizerState)

void
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

void
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
	namespace_state->search_path_context = NULL;
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
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(early_session_namespace.search_path_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(
		early_session_namespace.search_path_cache_context);
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
	locale->locale_time_context = NULL;
	locale->default_locale = NULL;
	locale->locale_conv_valid = false;
	locale->locale_time_valid = false;
	locale->locale_conv_context = NULL;
	locale->current_locale_conv = NULL;
	locale->current_locale_conv_allocated = false;
	locale->collation_cache_context = NULL;
	locale->collation_cache = NULL;
	locale->last_collation_cache_oid = InvalidOid;
	locale->last_collation_cache_locale = NULL;
	locale->icu_converter = NULL;
	locale->initialized = true;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_INITIALIZED(PgSessionAdoptEarlyLocaleState,
										  PgSession, session, locale,
										  early_session_locale,
										  PgSessionInitializeLocaleState)

void
PgSessionAdoptEarlyState(PgSession *session)
{
	Assert(session != NULL);

#define PG_SESSION_BUCKET(field, init, adopt, reset) \
	do { adopt; } while (0);
#include "backend_runtime_session_buckets.def"
#undef PG_SESSION_BUCKET
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyCommandState,
										PgBackend, backend, command,
										early_backend_command,
										PgBackendInitializeCommandState)

static void
PgBackendInitializeLogState(PgBackendLogState *log_state)
{
	Assert(log_state != NULL);

	MemSet(log_state, 0, sizeof(*log_state));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyLogState,
										PgBackend, backend, log_state,
										early_backend_log,
										PgBackendInitializeLogState)

static void
PgBackendInitializeExprInterpState(PgBackendExprInterpState *expr_interp)
{
	Assert(expr_interp != NULL);

	MemSet(expr_interp, 0, sizeof(*expr_interp));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyExprInterpState,
										PgBackend, backend, expr_interp,
										early_backend_expr_interp,
										PgBackendInitializeExprInterpState)

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
PgSessionInitializeTcopState(PgSessionTcopState *tcop)
{
	Assert(tcop != NULL);

	MemSet(tcop, 0, sizeof(*tcop));
}

static void
PgSessionAdoptEarlyTcopState(PgSession *session)
{
	Assert(session != NULL);
	Assert(early_session_tcop.unnamed_stmt_psrc == NULL);
	Assert(early_session_tcop.row_description_context == NULL);
	Assert(early_session_tcop.row_description_buf.data == NULL);

	session->tcop = early_session_tcop;
	PgSessionInitializeTcopState(&early_session_tcop);
}

void
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyActivityState,
										PgBackend, backend, activity,
										early_backend_activity,
										PgBackendInitializeActivityState)

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
	Assert(early_backend_utility.async_global_channel_table == NULL);
	Assert(early_backend_utility.async_global_channel_dsa == NULL);
	Assert(early_backend_utility.extension_sibling_list == NULL);
	Assert(early_backend_utility.injection_point_cache == NULL);
	Assert(early_backend_utility.utility_cache_context == NULL);
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
	backend->utility = early_backend_utility;
	PgBackendInitializeUtilityState(&early_backend_utility);
}

void
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
	Assert(early_backend_parallel.message_context == NULL);

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

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyInstrumentationState,
										PgBackend, backend,
										instrumentation,
										early_backend_instrumentation,
										PgBackendInitializeInstrumentationState)

void
PgBackendInitializeBufferState(PgBackendBufferState *buffers)
{
	Assert(buffers != NULL);

	MemSet(buffers, 0, sizeof(*buffers));
	buffers->reserved_ref_count_slot = -1;
	buffers->private_ref_count_entry_last = -1;
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyBufferState,
										PgBackend, backend, buffers,
										early_backend_buffers,
										PgBackendInitializeBufferState)

void
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

void
PgBackendInitializeLockState(PgBackendLockState *locks)
{
	Assert(locks != NULL);

	MemSet(locks, 0, sizeof(*locks));
}

static void
PgBackendAdoptEarlyLockState(PgBackend *backend)
{
	Assert(backend != NULL);
	Assert(early_backend_locks.strong_lock_in_progress == NULL);
	Assert(early_backend_locks.awaited_lock == NULL);
	Assert(early_backend_locks.awaited_owner == NULL);
	Assert(early_backend_locks.blocking_autovacuum_proc == NULL);

	backend->locks = early_backend_locks;
	PgBackendInitializeLockState(&early_backend_locks);
}

void
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

void
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

void
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgBackendAdoptEarlyTimeoutState,
										PgBackend, backend, timeout,
										early_backend_timeout,
										PgBackendInitializeTimeoutState)

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
	Assert(early_backend_logical_replication.parallel_apply_message_context ==
		   NULL);

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

void
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
	Assert(early_backend_maintenance_worker.bgwriter_context == NULL);
	Assert(early_backend_maintenance_worker.walwriter_context == NULL);
	Assert(early_backend_maintenance_worker.checkpointer_context == NULL);
	Assert(early_backend_maintenance_worker.walsummarizer_context == NULL);

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

void
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
	Assert(early_backend_repack.message_context == NULL);

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

void
PgBackendInitializeExtensionModuleState(PgBackendExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	MemSet(extension_modules, 0, sizeof(*extension_modules));
	extension_modules->basic_archive_archive_directory = "";
}

static void
PgBackendAdoptEarlyExtensionModuleState(PgBackend *backend)
{
	Assert(backend != NULL);

	backend->extension_modules = early_backend_extension_modules;
	PgBackendInitializeExtensionModuleState(&early_backend_extension_modules);
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

#define PG_BACKEND_BUCKET(field, init, adopt, reset) \
	do { adopt; } while (0);
#include "backend_runtime_backend_buckets.def"
#undef PG_BACKEND_BUCKET
}

static void
PgExecutionAdoptEarlyDebugState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->debug.debug_query_string =
		early_execution_debug.debug_query_string;

	early_execution_debug.debug_query_string = NULL;
}

void
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgExecutionAdoptEarlyMemoryContexts,
								   PgExecution, execution, memory_contexts,
								   early_execution_memory_contexts)
PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgExecutionAdoptEarlyResourceOwners,
								   PgExecution, execution, resource_owners,
								   early_execution_resource_owners)

void
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

PG_RUNTIME_DEFINE_ADOPT_EARLY_ZERO(PgExecutionAdoptEarlyPortalState,
								   PgExecution, execution, portal,
								   early_execution_portal)

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeVacuumState,
							PgExecutionVacuumState, vacuum)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyVacuumState,
										PgExecution, execution, vacuum,
										early_execution_vacuum,
										PgExecutionInitializeVacuumState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeNodeIOState,
							PgExecutionNodeIOState, node_io)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyNodeIOState,
										PgExecution, execution, node_io,
										early_execution_node_io,
										PgExecutionInitializeNodeIOState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeBaseBackupState,
							PgExecutionBaseBackupState, basebackup)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyBaseBackupState,
										PgExecution, execution, basebackup,
										early_execution_basebackup,
										PgExecutionInitializeBaseBackupState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeAnalyzeState,
							PgExecutionAnalyzeState, analyze)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyAnalyzeState,
										PgExecution, execution, analyze,
										early_execution_analyze,
										PgExecutionInitializeAnalyzeState)

void
PgExecutionInitializeExtensionState(PgExecutionExtensionState *extension)
{
	Assert(extension != NULL);

	extension->creating = false;
	extension->current_object = InvalidOid;
	extension->auto_explain_nesting_level = 0;
	extension->auto_explain_current_query_sampled = false;
	extension->pgcrypto_debug_handler = NULL;
}

static void
PgExecutionAdoptEarlyExtensionState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->extension = early_execution_extension;
	PgExecutionInitializeExtensionState(&early_execution_extension);
}

void
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

void
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

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeComboCidState,
							PgExecutionComboCidState, combo_cid)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyComboCidState,
										PgExecution, execution, combo_cid,
										early_execution_combo_cid,
										PgExecutionInitializeComboCidState)

void
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

void
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

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeTransactionCleanupState,
							PgExecutionTransactionCleanupState,
							transaction_cleanup)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyTransactionCleanupState,
										PgExecution, execution,
										transaction_cleanup,
										early_execution_transaction_cleanup,
										PgExecutionInitializeTransactionCleanupState)

void
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

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeGUCErrorState,
							PgExecutionGUCErrorState, guc_error)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyGUCErrorState,
										PgExecution, execution, guc_error,
										early_execution_guc_error,
										PgExecutionInitializeGUCErrorState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeAsyncState,
							PgExecutionAsyncState, async)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyAsyncState,
										PgExecution, execution, async,
										early_execution_async,
										PgExecutionInitializeAsyncState)

void
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

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeCatalogCacheState,
							PgExecutionCatalogCacheState, catalog_cache)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyCatalogCacheState,
										PgExecution, execution, catalog_cache,
										early_execution_catalog_cache,
										PgExecutionInitializeCatalogCacheState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeRelMapState,
							PgExecutionRelMapState, relmap)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyRelMapState,
										PgExecution, execution, relmap,
										early_execution_relmap,
										PgExecutionInitializeRelMapState)

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeInvalidationState,
							PgExecutionInvalidationState, invalidation)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyInvalidationState,
										PgExecution, execution, invalidation,
										early_execution_invalidation,
										PgExecutionInitializeInvalidationState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeTwoPhaseRecordState,
							PgExecutionTwoPhaseRecordState,
							two_phase_records)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyTwoPhaseRecordState,
										PgExecution, execution,
										two_phase_records,
										early_execution_two_phase_records,
										PgExecutionInitializeTwoPhaseRecordState)

void
PgExecutionInitializeTriggerState(PgExecutionTriggerState *trigger)
{
	Assert(trigger != NULL);

	MemSet(trigger, 0, sizeof(*trigger));
}

PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyTriggerState,
										PgExecution, execution, trigger,
										early_execution_trigger,
										PgExecutionInitializeTriggerState)

PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeRegexState,
							PgExecutionRegexState, regex)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyRegexState,
										PgExecution, execution, regex,
										early_execution_regex,
										PgExecutionInitializeRegexState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeValgrindState,
							PgExecutionValgrindState, valgrind)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlyValgrindState,
										PgExecution, execution, valgrind,
										early_execution_valgrind,
										PgExecutionInitializeValgrindState)
PG_RUNTIME_DEFINE_ZERO_INIT(PgExecutionInitializeSnapBuildState,
							PgExecutionSnapBuildState, snapbuild)
PG_RUNTIME_DEFINE_ADOPT_EARLY_WITH_INIT(PgExecutionAdoptEarlySnapBuildState,
										PgExecution, execution, snapbuild,
										early_execution_snapbuild,
										PgExecutionInitializeSnapBuildState)

void
PgExecutionAdoptEarlyState(PgExecution *execution)
{
	Assert(execution != NULL);

#define PG_EXECUTION_BUCKET(field, init, adopt, reset) \
	do { adopt; } while (0);
#include "backend_runtime_execution_buckets.def"
#undef PG_EXECUTION_BUCKET
}

static void
PgRuntimeInitializeRuntimeObject(PgRuntime *runtime)
{
	Assert(runtime != NULL);

#define PG_RUNTIME_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "backend_runtime_runtime_buckets.def"
#undef PG_RUNTIME_BUCKET
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

#define PG_BACKEND_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "backend_runtime_backend_buckets.def"
#undef PG_BACKEND_BUCKET
}

static void
PgSessionInitializeRuntimeObject(PgSession *session,
								 PgBackend *backend,
								 PgConnection *connection,
								 PgExecution *execution)
{
	Assert(session != NULL);

#define PG_SESSION_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "backend_runtime_session_buckets.def"
#undef PG_SESSION_BUCKET
}

static void
PgConnectionInitializeRuntimeObject(PgConnection *connection,
									PgBackend *backend,
									PgSession *session,
									struct Port *port)
{
	Assert(connection != NULL);

#define PG_CONNECTION_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "backend_runtime_connection_buckets.def"
#undef PG_CONNECTION_BUCKET
}

static void
PgExecutionInitializeRuntimeObject(PgExecution *execution,
								   PgBackend *backend,
								   PgSession *session,
								   PgCarrier *carrier)
{
	Assert(execution != NULL);

#define PG_EXECUTION_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "backend_runtime_execution_buckets.def"
#undef PG_EXECUTION_BUCKET
}

static void
PgCarrierInitializeRuntimeObject(PgCarrier *carrier)
{
	bool		is_under_postmaster;

	is_under_postmaster = carrier->is_under_postmaster;
	MemSet(carrier, 0, sizeof(*carrier));
	carrier->is_under_postmaster = is_under_postmaster;

#define PG_CARRIER_BUCKET(field, init, adopt, reset) \
	do { init; } while (0);
#include "backend_runtime_carrier_buckets.def"
#undef PG_CARRIER_BUCKET
}

void
PgRuntimeResetAfterFork(void)
{
	PgBackendResetDsmStateAfterFork();

	CurrentPgRuntime = NULL;
	CurrentPgCarrier = NULL;
	CurrentPgBackend = NULL;
	CurrentPgSession = NULL;
	CurrentPgConnection = NULL;
	CurrentPgExecution = NULL;

	MemSet(&process_runtime, 0, sizeof(process_runtime));
	PgRuntimeInitializeRuntimeObject(&process_runtime);
	PgCarrierInitializeRuntimeObject(&process_carrier);
	PgBackendInitializeRuntimeObject(&process_backend, NULL, NULL, NULL,
									 NULL, NULL, B_INVALID, NULL);
	PgSessionInitializeRuntimeObject(&process_session, NULL, NULL, NULL);
	PgConnectionInitializeRuntimeObject(&process_connection, NULL, NULL,
										NULL);
	PgExecutionInitializeRuntimeObject(&process_execution, NULL, NULL, NULL);

	PgBackendInitializeRuntimeObject(&early_backend_fallback, NULL, NULL,
									 NULL, NULL, NULL, B_INVALID, NULL);
	early_backend_core.proc_pid = (int) getpid();
}

void
InitializePgProcessRuntime(void)
{
	MemSet(&process_runtime, 0, sizeof(process_runtime));
	PgRuntimeInitializeRuntimeObject(&process_runtime);
	PgCarrierInitializeRuntimeObject(&process_carrier);
	MemSet(&process_backend, 0, sizeof(process_backend));
	MemSet(&process_session, 0, sizeof(process_session));
	MemSet(&process_connection, 0, sizeof(process_connection));
	MemSet(&process_execution, 0, sizeof(process_execution));

	process_runtime.kind = PG_RUNTIME_PROCESS;
	process_runtime.current_carrier = &process_carrier;
	process_runtime.extension_backend_model = PG_BACKEND_MODEL_PROCESS;
	PgRuntimeAdoptEarlyServerGUCState(&process_runtime);
	PgRuntimeAdoptEarlyExtensionModuleState(&process_runtime);

	process_carrier.kind = PG_CARRIER_PROCESS;
	process_carrier.runtime = &process_runtime;
	process_carrier.current_backend = &process_backend;
	process_carrier.current_session = &process_session;
	process_carrier.current_execution = &process_execution;

	PgBackendInitializeRuntimeObject(&process_backend, &process_runtime,
									 &process_carrier, &process_session,
									 &process_connection, &process_execution,
									 MyBackendType, NULL);
	PgBackendAdoptEarlyState(&process_backend);
	PgBackendSetInterruptLatch(&process_backend, process_backend.core.latch);
	PgBackendAdoptEarlyExitState(&process_backend.exit_state);

	PgSessionInitializeRuntimeObject(&process_session, &process_backend,
									 &process_connection, &process_execution);
	PgSessionAdoptEarlyState(&process_session);

	PgConnectionInitializeRuntimeObject(&process_connection, &process_backend,
										&process_session, NULL);
	PgConnectionAdoptEarlyState(&process_connection, NULL);

	PgExecutionInitializeRuntimeObject(&process_execution, &process_backend,
									   &process_session, &process_carrier);
	PgExecutionAdoptEarlyState(&process_execution);

	CurrentPgRuntime = &process_runtime;
	CurrentPgCarrier = &process_carrier;
	CurrentPgBackend = &process_backend;
	CurrentPgConnection = &process_connection;
	CurrentPgExecution = &process_execution;
	PgSetCurrentSession(&process_session);

	if (MyProc != NULL && MyProc->backendId == 0)
		MyProc->backendId = process_backend.id;
}

void
InitializePgThreadRuntime(PgBackendExitContinuation exit_backend)
{
	if (!thread_runtime_initialized)
	{
		MemSet(&thread_runtime, 0, sizeof(thread_runtime));
		PgRuntimeInitializeRuntimeObject(&thread_runtime);

		thread_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
		thread_runtime.extension_backend_model =
			PG_BACKEND_MODEL_THREAD_PER_SESSION;
		if (PgRuntimeServerGUCStateHasConfigPaths(&process_runtime.server_guc))
			thread_runtime.server_guc = process_runtime.server_guc;
		else if (PgRuntimeServerGUCStateHasConfigPaths(&early_runtime_server_guc))
			thread_runtime.server_guc = early_runtime_server_guc;
		else if (process_runtime.server_guc.initialized)
			thread_runtime.server_guc = process_runtime.server_guc;
		else
			PgRuntimeInitializeServerGUCState(&thread_runtime.server_guc);
		thread_runtime.extension_modules = process_runtime.extension_modules;
		PgRuntimeEnsureExtensionModuleMemoryContext(&thread_runtime.extension_modules);
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
	PgCarrierInitializeRuntimeObject(&state->carrier);

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
	CurrentPgConnection = &state->connection;
	CurrentPgExecution = &state->execution;
	PgSetCurrentSession(&state->session);
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
	if (CurrentPgSession != NULL)
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

bool
PgCurrentOrEarlySessionOwnsPointer(const void *ptr)
{
	uintptr_t	address;
	uintptr_t	session_start;
	uintptr_t	session_end;

	if (ptr == NULL)
		return false;
	if (PgCurrentSessionOwnsPointer(ptr))
		return true;

	address = (uintptr_t) ptr;
	session_start = (uintptr_t) &early_session_fallback;
	session_end = session_start + sizeof(PgSession);

	return address >= session_start && address < session_end;
}

void
PgRuntimeDeleteOwnedMemoryContext(MemoryContext *context)
{
	Assert(context != NULL);

	if (*context == NULL)
		return;

	if (CurrentMemoryContext == *context)
		MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(*context);
	*context = NULL;
}

MemoryContext
PgSessionGetDynamicLibraryMemoryContext(PgSession *session)
{
	Assert(session != NULL);

	return PgRuntimeGetOwnedMemoryContext(&session->dynamic_library_context,
										  "dynamic library session state");
}

List **
PgCurrentSessionDynamicLibraryInitsRef(void)
{
	Assert(CurrentPgSession != NULL);

	return &CurrentPgSession->dynamic_library_inits;
}

Session *
PgSessionGetLegacySession(PgSession *session)
{
	if (session == NULL)
		return NULL;

	if (session->legacy_session == NULL)
	{
		Assert(session->legacy_session_context == NULL);
		(void) PgRuntimeGetOwnedMemoryContext(&session->legacy_session_context,
											  "legacy session compatibility state");
		session->legacy_session =
			MemoryContextAllocZero(session->legacy_session_context,
								   sizeof(Session));
	}

	return session->legacy_session;
}

Session *
PgCurrentLegacySession(void)
{
	if (CurrentPgSession == NULL)
	{
		if (TopMemoryContext == NULL)
			return process_session.legacy_session;

		return PgSessionGetLegacySession(&process_session);
	}

	return PgSessionGetLegacySession(CurrentPgSession);
}

Session **
PgCurrentLegacySessionRef(void)
{
	if (CurrentPgSession == NULL)
		return &process_session.legacy_session;

	return &CurrentPgSession->legacy_session;
}

PgSessionDatabaseState *
PgCurrentSessionDatabaseState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_database;

	return &CurrentPgSession->database;
}

PgRuntimeServerGUCState *
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

PgRuntimeExtensionModuleState *
PgCurrentRuntimeExtensionModuleState(void)
{
	if (CurrentPgRuntime == NULL)
		return &early_runtime_extension_modules;

	return &CurrentPgRuntime->extension_modules;
}

MemoryContext
PgCurrentRuntimeExtensionModuleMemoryContext(void)
{
	return PgRuntimeEnsureExtensionModuleMemoryContext(PgCurrentRuntimeExtensionModuleState());
}

MemoryContext *
PgCurrentPgPlanAdviceContextRef(void)
{
	return &PgCurrentRuntimeExtensionModuleState()->pg_plan_advice_context;
}

List **
PgCurrentPgPlanAdviceAdvisorHookListRef(void)
{
	PgRuntimeExtensionModuleState *extension_modules;

	extension_modules = PgCurrentRuntimeExtensionModuleState();
	return &extension_modules->pg_plan_advice_advisor_hook_list;
}

MemoryContext *
PgCurrentBloomContextRef(void)
{
	return &PgCurrentRuntimeExtensionModuleState()->bloom_context;
}

HTAB **
PgCurrentRendezvousHashRef(void)
{
	return &PgCurrentRuntimeExtensionModuleState()->rendezvous_hash;
}

PgSessionPgcryptoDesState *
PgCurrentPgcryptoDesState(void)
{
	return &PgCurrentSessionExtensionModuleState()->pgcrypto_des;
}

PgSessionTablespaceState *
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

PgSessionBinaryUpgradeState *
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

PgSessionDateTimeState *
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

PgSessionTextSearchState *
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

PgSessionConnectionGUCState *
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

PgSessionParserState *
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

PgSessionVacuumState *
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

PgSessionBufferIOState *
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

PgSessionXactDefaultState *
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

PgSessionLockWaitState *
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

PgSessionLoggingState *
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

PgSessionMiscGUCState *
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

PgSessionGUCState *
PgCurrentSessionGUCState(void)
{
	PgSessionGUCState *guc;

	if (CurrentPgSession == NULL)
		guc = &early_session_guc;
	else
		guc = &CurrentPgSession->guc;

	if (!guc->initialized)
		PgSessionInitializeGUCState(guc);

	return guc;
}

PgSessionPgStatState *
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

PgSessionQueryIdState *
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

PgSessionStorageGUCState *
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

PgSessionUserGUCState *
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

MemoryContext *
PgCurrentSystemUserContextRef(void)
{
	return &PgCurrentSessionUserIdentityState()->system_user_context;
}

PgSessionCommandGUCState *
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

PgSessionReplicationGUCState *
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

PgSessionLogicalReplicationState *
PgCurrentSessionLogicalReplicationState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_logical_replication;

	return &CurrentPgSession->logical_replication;
}

PgSessionGeneralGUCState *
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

PgSessionAccessWalGUCState *
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

PgSessionJitGUCState *
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

PgSessionJitProviderState *
PgCurrentSessionJitProviderState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_jit_provider;

	return &CurrentPgSession->jit_provider_state;
}

PgSessionLLVMJitState *
PgCurrentSessionLLVMJitState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_llvm_jit;

	return &CurrentPgSession->llvm_jit;
}

PgSessionSortGUCState *
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

PgSessionQueryMemoryState *
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

PgSessionPlannerCostState *
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

PgSessionPlannerMethodState *
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

PgSessionFunctionManagerState *
PgCurrentSessionFunctionManagerState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_function_manager;

	return &CurrentPgSession->function_manager;
}

PgSessionExtensionModuleState *
PgCurrentSessionExtensionModuleState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_extension_modules;

	return &CurrentPgSession->extension_modules;
}

PgSessionCatalogLookupState *
PgCurrentSessionCatalogLookupState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_catalog_lookup;

	return &CurrentPgSession->catalog_lookup;
}

PgSessionInvalidationCallbackState *
PgCurrentSessionInvalidationCallbackState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_invalidation_callbacks;

	return &CurrentPgSession->invalidation_callbacks;
}

PgSessionRIGlobalsState *
PgCurrentSessionRIGlobalsState(void)
{
	PgSessionRIGlobalsState *ri_globals;

	if (CurrentPgSession == NULL)
		ri_globals = &early_session_ri_globals;
	else
		ri_globals = &CurrentPgSession->ri_globals;

	if (!ri_globals->debug_discard_caches_initialized)
		PgSessionInitializeRIGlobalsState(ri_globals);

	return ri_globals;
}

PgSessionRelMapState *
PgCurrentSessionRelMapState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_relmap;

	return &CurrentPgSession->relmap;
}

PgSessionPreparedStatementState *
PgCurrentSessionPreparedStatementState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_prepared_statement;

	return &CurrentPgSession->prepared_statement;
}

PgSessionOnCommitState *
PgCurrentSessionOnCommitState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_on_commit;

	return &CurrentPgSession->on_commit;
}

PgSessionSequenceState *
PgCurrentSessionSequenceState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_sequence;

	return &CurrentPgSession->sequence;
}

PgSessionXactCallbackState *
PgCurrentSessionXactCallbackState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_xact_callbacks;

	return &CurrentPgSession->xact_callbacks;
}

PgSessionBackupState *
PgCurrentSessionBackupState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_backup;

	return &CurrentPgSession->backup;
}

PgSessionRegexState *
PgCurrentSessionRegexState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_regex;

	return &CurrentPgSession->regex;
}

PgSessionPortalManagerState *
PgCurrentSessionPortalManagerState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_portal_manager;

	return &CurrentPgSession->portal_manager;
}

PgSessionLargeObjectState *
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

PgSessionEncodingState *
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

PgSessionTempFileState *
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

PgSessionPlanCacheState *
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

PgSessionTcopState *
PgCurrentSessionTcopState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_tcop;

	return &CurrentPgSession->tcop;
}

PgSessionNamespaceState *
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

PgSessionLocaleState *
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

void **
PgCurrentBackendThreadStartRef(void)
{
	if (CurrentPgCarrier != NULL)
		return &CurrentPgCarrier->backend_thread_start;

	return &process_carrier.backend_thread_start;
}

bool *
PgCurrentIsUnderPostmasterRef(void)
{
	return &PgCurrentCarrierState()->is_under_postmaster;
}

PgCarrier *
PgCurrentCarrierState(void)
{
	if (CurrentPgCarrier == NULL)
		return &process_carrier;

	return CurrentPgCarrier;
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

PgBackendMemoryManagerState *
PgCurrentBackendMemoryManagerState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_memory_manager;

	return &CurrentPgBackend->memory_manager;
}

PgBackendUtilityState *
PgCurrentBackendUtilityState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_utility;

	return &CurrentPgBackend->utility;
}

PgBackendParallelState *
PgCurrentBackendParallelState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_parallel;

	return &CurrentPgBackend->parallel;
}

PgConnectionIdentityState *
PgConnectionIdentityStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_identity;

	return &connection->identity;
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

PgExecutionErrorState *
PgCurrentExecutionErrorState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_error;

	return &CurrentPgExecution->error;
}

PgExecutionMemoryContextState *
PgCurrentExecutionMemoryContexts(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_memory_contexts;

	return &CurrentPgExecution->memory_contexts;
}

bool *
PgCurrentDoingCommandReadRef(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_loop_state.doing_command_read;

	return &CurrentPgSession->loop_state.doing_command_read;
}


PgExecutionResourceOwnerState *
PgCurrentExecutionResourceOwners(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_resource_owners;

	return &CurrentPgExecution->resource_owners;
}

PgExecutionSPIState *
PgCurrentExecutionSPIState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_spi;

	return &CurrentPgExecution->spi;
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

PgExecutionVacuumState *
PgCurrentExecutionVacuumState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_vacuum;

	return &CurrentPgExecution->vacuum;
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

PgExecutionAnalyzeState *
PgCurrentExecutionAnalyzeState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_analyze;

	return &CurrentPgExecution->analyze;
}

PgExecutionExtensionState *
PgCurrentExecutionExtensionState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_extension;

	return &CurrentPgExecution->extension;
}

PgExecutionDebugHandler *
PgCurrentPgcryptoDebugHandlerRef(void)
{
	return &PgCurrentExecutionExtensionState()->pgcrypto_debug_handler;
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

PgExecutionSnapshotState *
PgCurrentExecutionSnapshotState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_snapshot;

	return &CurrentPgExecution->snapshot;
}

PgExecutionComboCidState *
PgCurrentExecutionComboCidState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_combo_cid;

	return &CurrentPgExecution->combo_cid;
}

PgExecutionXLogInsertState *
PgCurrentExecutionXLogInsertState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_xloginsert;

	return &CurrentPgExecution->xloginsert;
}

PgExecutionXactState *
PgCurrentExecutionXactState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_xact;

	return &CurrentPgExecution->xact;
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

MemoryContext
PgCurrentEventTriggerMemoryContext(void)
{
	PgExecutionReplicationScratchState *replication_scratch;

	replication_scratch = PgCurrentExecutionReplicationScratchState();

	return PgRuntimeGetOwnedMemoryContext(&replication_scratch->event_trigger_context,
										  "event trigger execution state");
}

MemoryContext *
PgCurrentEventTriggerMemoryContextRef(void)
{
	return &PgCurrentExecutionReplicationScratchState()->event_trigger_context;
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

PgExecutionGUCErrorState *
PgCurrentExecutionGUCErrorState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_guc_error;

	return &CurrentPgExecution->guc_error;
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

MemoryContext
PgCurrentAsyncSignalWorkspaceContext(void)
{
	PgExecutionAsyncState *async = PgCurrentExecutionAsyncState();

	return PgRuntimeGetOwnedMemoryContext(&async->signal_context,
										  "LISTEN/NOTIFY signal workspace");
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

MemoryContext
PgCurrentAfterTriggersMemoryContext(void)
{
	PgExecutionTriggerState *trigger;

	trigger = PgCurrentExecutionTriggerState();

	return PgRuntimeGetOwnedMemoryContext(&trigger->after_triggers_context,
										  "after trigger execution state");
}

MemoryContext *
PgCurrentAfterTriggersMemoryContextRef(void)
{
	return &PgCurrentExecutionTriggerState()->after_triggers_context;
}

PgExecutionRegexState *
PgCurrentExecutionRegexState(void)
{
	if (CurrentPgExecution == NULL)
		return &early_execution_regex;

	return &CurrentPgExecution->regex;
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
PgConnectionSocketIOStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_socket_io;

	return &connection->socket_io;
}

PgConnectionProtocolState *
PgConnectionProtocolStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_protocol;

	return &connection->protocol;
}

PgConnectionOutputState *
PgConnectionOutputStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_output;

	return &connection->output;
}

PgConnectionInterruptState *
PgConnectionInterruptStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_interrupts;

	return &connection->interrupts;
}

PgConnectionStartupState *
PgConnectionStartupStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_startup;

	return &connection->startup;
}

PgConnectionClientConnectionInfoState *
PgConnectionClientConnectionInfoStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_client_connection_info;

	return &connection->client_connection_info;
}

MemoryContext *
PgConnectionClientConnectionInfoContextRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_client_connection_info_context;

	return &connection->client_connection_info_context;
}

bool *
PgConnectionClientConnectionInfoAuthnIdOwnedRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_client_connection_info_authn_id_owned;

	return &connection->client_connection_info_authn_id_owned;
}

PgConnectionSecurityState *
PgConnectionRuntimeSecurityStateRef(PgConnection *connection)
{
	if (connection == NULL)
		return &early_connection_security;

	return &connection->security;
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

PgBackendPgStatPendingState *
PgCurrentBackendPgStatPendingState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_pgstat_pending;

	return &CurrentPgBackend->pgstat_pending;
}

PgBackendInstrumentationState *
PgCurrentBackendInstrumentationState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_instrumentation;

	return &CurrentPgBackend->instrumentation;
}

PgBackendBufferState *
PgCurrentBackendBufferState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_buffers;

	return &CurrentPgBackend->buffers;
}

MemoryContext
PgBackendBufferAllocationContext(void)
{
	PgBackendBufferState *buffers = PgCurrentBackendBufferState();

	if (TopMemoryContext != NULL)
		return PgRuntimeGetOwnedMemoryContextWithSizes(&buffers->buffer_context,
													   "BackendBufferContext",
													   ALLOCSET_DEFAULT_SIZES);
	if (CurrentMemoryContext != NULL)
		return CurrentMemoryContext;

	elog(ERROR, "cannot allocate backend buffer state before memory contexts exist");
	return NULL;				/* keep compiler quiet */
}

PgBackendStorageState *
PgCurrentBackendStorageState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_storage;

	return &CurrentPgBackend->storage;
}

PgBackendLockState *
PgCurrentBackendLockState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_locks;

	return &CurrentPgBackend->locks;
}

PgBackendIPCState *
PgCurrentBackendIPCState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_ipc;

	return &CurrentPgBackend->ipc;
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

PgBackendExtensionModuleState *
PgCurrentBackendExtensionModuleState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_extension_modules;

	return &CurrentPgBackend->extension_modules;
}

char **
PgCurrentBasicArchiveDirectoryRef(void)
{
	return &PgCurrentBackendExtensionModuleState()->basic_archive_archive_directory;
}

PgBackendTransactionState *
PgCurrentBackendTransactionState(void)
{
	if (CurrentPgBackend == NULL)
		return &early_backend_transaction;

	return &CurrentPgBackend->transaction;
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
