/*-------------------------------------------------------------------------
 *
 * backend_runtime_session.c
 *	  Runtime bridge accessors for session-owned compatibility state.
 *
 * This file owns session fallback state, session construction/adoption, and
 * session-facing compatibility accessors.  Top-level process/thread lifecycle
 * orchestration remains in backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/init/backend_runtime_session.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "access/gin.h"
#include "access/session.h"
#include "access/syncscan.h"
#include "access/tableam.h"
#include "access/toast_compression.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "archive/archive_module.h"
#include "catalog/binary_upgrade.h"
#include "commands/extension.h"
#include "commands/repack.h"
#include "commands/trigger.h"
#include "commands/vacuum.h"
#include "jit/jit.h"
#include "libpq/crypt.h"
#include "miscadmin.h"
#include "nodes/queryjumble.h"
#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "parser/parser.h"
#include "replication/logical.h"
#include "replication/reorderbuffer.h"
#include "replication/logicalworker.h"
#include "replication/slotsync.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/sinval.h"
#include "tsearch/ts_cache.h"
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
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/rls.h"
#include "utils/typcache.h"
#include "utils/xml.h"
#include "backend_runtime_internal.h"

static void PgSessionAdoptEarlyDatabaseState(PgSession *session);
static void PgSessionInitializeTablespaceState(PgSessionTablespaceState *tablespace);
static void PgSessionAdoptEarlyTablespaceState(PgSession *session);
static void PgSessionInitializeBinaryUpgradeState(PgSessionBinaryUpgradeState *binary_upgrade);
static void PgSessionAdoptEarlyBinaryUpgradeState(PgSession *session);
static void PgSessionAdoptEarlyDateTimeState(PgSession *session);
static void PgSessionInitializeTextSearchState(PgSessionTextSearchState *text_search);
static void PgSessionAdoptEarlyTextSearchState(PgSession *session);
static void PgSessionInitializeConnectionGUCState(PgSessionConnectionGUCState *connection_guc);
static void PgSessionAdoptEarlyConnectionGUCState(PgSession *session);
static void PgSessionAdoptEarlyParserState(PgSession *session);
static void PgSessionAdoptEarlyVacuumState(PgSession *session);
static void PgSessionInitializeBufferIOState(PgSessionBufferIOState *buffer_io);
static void PgSessionAdoptEarlyBufferIOState(PgSession *session);
static void PgSessionInitializeXactDefaultState(PgSessionXactDefaultState *xact_defaults);
static void PgSessionAdoptEarlyXactDefaultState(PgSession *session);
static void PgSessionAdoptEarlyLockWaitState(PgSession *session);
static void PgSessionInitializeLoggingState(PgSessionLoggingState *logging);
static void PgSessionAdoptEarlyLoggingState(PgSession *session);
static void PgSessionInitializeMiscGUCState(PgSessionMiscGUCState *misc_guc);
static void PgSessionAdoptEarlyMiscGUCState(PgSession *session);
static void PgSessionAdoptEarlyGUCState(PgSession *session);
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
static void PgSessionAdoptEarlyLargeObjectState(PgSession *session);
static void PgSessionInitializeAsyncState(PgSessionAsyncState *async);
static void PgSessionAdoptEarlyAsyncState(PgSession *session);
static void PgSessionEnsureEncodingStateInitialized(PgSessionEncodingState *encoding);
static void PgSessionAdoptEarlyEncodingState(PgSession *session);
static void PgSessionAdoptEarlyTempFileState(PgSession *session);
static void PgSessionInitializeRandomState(PgSessionRandomState *random);
static void PgSessionAdoptEarlyRandomState(PgSession *session);
static void PgSessionInitializeOptimizerState(PgSessionOptimizerState *optimizer);
static void PgSessionAdoptEarlyOptimizerState(PgSession *session);
static void PgSessionAdoptEarlyPlanCacheState(PgSession *session);
static void PgSessionAdoptEarlyNamespaceState(PgSession *session);
static void PgSessionAdoptEarlyLocaleState(PgSession *session);
static void PgSessionInitializeLoopState(PgSessionLoopState *loop_state);
static void PgSessionAdoptEarlyLoopState(PgSession *session);
static void PgSessionInitializeTcopState(PgSessionTcopState *tcop);
static void PgSessionAdoptEarlyTcopState(PgSession *session);
static PgSessionUserIdentityState *PgCurrentSessionUserIdentityState(void);
static char *PgSessionDefaultGUCString(const char *src);
static PG_THREAD_LOCAL PG_GLOBAL_SESSION bool
			use_static_guc_defaults_for_initialization = false;
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

bool
PgSessionSetStaticGUCDefaultsForInitialization(bool use_static)
{
	bool		previous = use_static_guc_defaults_for_initialization;

	use_static_guc_defaults_for_initialization = use_static;
	return previous;
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

static char *
PgSessionDefaultGUCString(const char *src)
{
	if (use_static_guc_defaults_for_initialization)
		return unconstify(char *, src);

	return guc_strdup(FATAL, src);
}

void
PgSessionInitializeDateTimeState(PgSessionDateTimeState *datetime)
{
	Assert(datetime != NULL);

	datetime->initialized = true;
	datetime->date_style = USE_ISO_DATES;
	datetime->date_order = DATEORDER_MDY;
	datetime->interval_style = INTSTYLE_POSTGRES;
	datetime->datestyle_string_value =
		PgSessionDefaultGUCString("ISO, MDY");
	datetime->timezone_string_value = PgSessionDefaultGUCString("GMT");
	datetime->log_timezone_string_value = PgSessionDefaultGUCString("GMT");
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
	text_search->current_config_value =
		PgSessionDefaultGUCString("pg_catalog.simple");
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
	connection_guc->application_name_value = PgSessionDefaultGUCString("");
	connection_guc->ssl_renegotiation_limit_value = 0;
	connection_guc->tcp_keepalives_idle_value = 0;
	connection_guc->tcp_keepalives_interval_value = 0;
	connection_guc->tcp_keepalives_count_value = 0;
	connection_guc->tcp_user_timeout_value = 0;
	connection_guc->log_disconnections_value = false;
	connection_guc->log_statement_value = 0;
	connection_guc->post_auth_delay_seconds = 0;
	connection_guc->restrict_nonsystem_relation_kind_string_value =
		PgSessionDefaultGUCString("");
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

/*
 * xtc-carrier: restore this thread's early_session_fallback to the pristine
 * state a freshly-spawned OS thread has (the static initializer at the top of
 * this file), so a carrier thread that hosts a second backend fiber behaves
 * exactly like a fresh pthread would for the first one.
 *
 * The three "adopt-with-reset" buckets (datetime, text_search,
 * connection_guc) are left .initialized = false after a backend adopts them
 * (PgSessionResetEarly*State).  The next backend on the SAME thread then reads
 * e.g. early_session_datetime before InitializeThreadedSessionGUCOptions()
 * has built a GUCMemoryContext, so re-initializing them here through
 * guc_strdup() would fault.  Force static GUC defaults (string literals, no
 * allocation) while re-initializing -- this is what InitializePgThread
 * BackendRuntimeState() already does for a session, and what the static
 * early_session_fallback initializer encodes.
 */
void
PgSessionResetEarlyFallbackForNewBackend(void)
{
	bool		static_guc_defaults;

	static_guc_defaults = PgSessionSetStaticGUCDefaultsForInitialization(true);
	PgSessionInitializeDateTimeState(&early_session_datetime);
	PgSessionInitializeTextSearchState(&early_session_text_search);
	PgSessionInitializeConnectionGUCState(&early_session_connection_guc);
	(void) PgSessionSetStaticGUCDefaultsForInitialization(static_guc_defaults);
}

void
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
	guc->variable_states = NULL;
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
	extension_modules->private_states = NIL;
	extension_modules->reset_callbacks = NIL;
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

void
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
	regex->cached_res = NULL;
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

void
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


PgSession *
PgCurrentOrEarlySession(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_fallback;

	return CurrentPgSession;
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
		PgSession  *process_session = PgProcessSessionState();

		if (TopMemoryContext == NULL)
			return process_session->legacy_session;

		return PgSessionGetLegacySession(process_session);
	}

	return PgSessionGetLegacySession(CurrentPgSession);
}

Session **
PgCurrentLegacySessionRef(void)
{
	if (CurrentPgSession == NULL)
		return &PgProcessSessionState()->legacy_session;

	return &CurrentPgSession->legacy_session;
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


PgSessionDatabaseState *
PgCurrentSessionDatabaseState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_database;

	return &CurrentPgSession->database;
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
	PG_RUNTIME_RETURN_INITIALIZED_SESSION_BUCKET(CurrentPgSessionXactDefaultRuntimeState,
												 xact_defaults,
												 early_session_xact_defaults,
												 PgSessionInitializeXactDefaultState);
}

PgSessionLockWaitState *
PgCurrentSessionLockWaitState(void)
{
	PG_RUNTIME_RETURN_INITIALIZED_SESSION_BUCKET(CurrentPgSessionLockWaitRuntimeState,
												 lock_wait,
												 early_session_lock_wait,
												 PgSessionInitializeLockWaitState);
}

PgSessionLoggingState *
PgCurrentSessionLoggingState(void)
{
	PG_RUNTIME_RETURN_INITIALIZED_SESSION_BUCKET(CurrentPgSessionLoggingRuntimeState,
												 logging,
												 early_session_logging,
												 PgSessionInitializeLoggingState);
}

PgSessionMiscGUCState *
PgCurrentSessionMiscGUCState(void)
{
	PG_RUNTIME_RETURN_INITIALIZED_SESSION_BUCKET(CurrentPgSessionMiscGUCRuntimeState,
												 misc_guc,
												 early_session_misc_guc,
												 PgSessionInitializeMiscGUCState);
}

PgSessionGUCState *
PgCurrentSessionGUCState(void)
{
	PG_RUNTIME_RETURN_INITIALIZED_SESSION_BUCKET(CurrentPgSessionGUCRuntimeState,
												 guc,
												 early_session_guc,
												 PgSessionInitializeGUCState);
}

PgSessionPgStatState *
PgCurrentSessionPgStatState(void)
{
	PG_RUNTIME_RETURN_INITIALIZED_SESSION_BUCKET(CurrentPgSessionPgStatRuntimeState,
												 pgstat,
												 early_session_pgstat,
												 PgSessionInitializePgStatState);
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
	PG_RUNTIME_RETURN_INITIALIZED_SESSION_BUCKET(CurrentPgSessionQueryMemoryRuntimeState,
												 query_memory,
												 early_session_query_memory,
												 PgSessionInitializeQueryMemoryState);
}

PgSessionPlannerCostState *
PgCurrentSessionPlannerCostState(void)
{
	PG_RUNTIME_RETURN_INITIALIZED_SESSION_BUCKET(CurrentPgSessionPlannerCostRuntimeState,
												 planner_cost,
												 early_session_planner_cost,
												 PgSessionInitializePlannerCostState);
}

PgSessionPlannerMethodState *
PgCurrentSessionPlannerMethodState(void)
{
	PG_RUNTIME_RETURN_INITIALIZED_SESSION_BUCKET(CurrentPgSessionPlannerMethodRuntimeState,
												 planner_method,
												 early_session_planner_method,
												 PgSessionInitializePlannerMethodState);
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

static PgSessionExtensionPrivateState *
PgSessionFindExtensionPrivateState(PgSessionExtensionModuleState *extension_modules,
								   const char *key)
{
	Assert(extension_modules != NULL);
	Assert(key != NULL);

	foreach_ptr(PgSessionExtensionPrivateState, private_state,
				extension_modules->private_states)
	{
		if (strcmp(private_state->key, key) == 0)
			return private_state;
	}

	return NULL;
}

void *
PgSessionGetExtensionPrivateState(const char *key)
{
	PgSessionExtensionPrivateState *private_state;

	private_state = PgSessionFindExtensionPrivateState(
		PgCurrentSessionExtensionModuleState(), key);

	return private_state != NULL ? private_state->state : NULL;
}

void *
PgSessionEnsureExtensionPrivateState(const char *key, Size size,
									 PgSessionExtensionPrivateStateCleanup cleanup)
{
	PgSessionExtensionModuleState *extension_modules;
	PgSessionExtensionPrivateState *private_state;
	MemoryContext alloc_context;
	MemoryContext old_context;

	Assert(key != NULL);
	Assert(size > 0);

	extension_modules = PgCurrentSessionExtensionModuleState();
	private_state = PgSessionFindExtensionPrivateState(extension_modules, key);
	if (private_state != NULL)
		return private_state->state;

	if (CurrentPgSession != NULL)
		alloc_context = PgSessionGetDynamicLibraryMemoryContext(CurrentPgSession);
	else
		alloc_context = TopMemoryContext;

	old_context = MemoryContextSwitchTo(alloc_context);
	private_state = palloc_object(PgSessionExtensionPrivateState);
	private_state->key = key;
	private_state->state = palloc0(size);
	private_state->cleanup = cleanup;
	extension_modules->private_states =
		lappend(extension_modules->private_states, private_state);
	MemoryContextSwitchTo(old_context);

	return private_state->state;
}

PgSessionCatalogLookupState *
PgCurrentSessionCatalogLookupState(void)
{
	PgSessionCatalogLookupState *catalog_lookup;

	catalog_lookup = CurrentPgSessionCatalogLookupRuntimeState;
	if (likely(catalog_lookup != NULL))
		return catalog_lookup;

	PG_RUNTIME_BRIDGE_COUNT_FALLBACK(session_catalog_lookup);
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

PgSessionLargeObjectState *
PgCurrentSessionLargeObjectState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_large_object;

	return &CurrentPgSession->large_object;
}

PgSessionAsyncState *
PgCurrentSessionAsyncState(void)
{
	if (CurrentPgSession == NULL)
		return &early_session_async;

	return &CurrentPgSession->async;
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

PgSessionRandomState *
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

PgSessionOptimizerState *
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

PgSessionLoopState *
PgCurrentSessionLoopState(void)
{
	if (likely(CurrentPgSessionLoopRuntimeState != NULL))
		return CurrentPgSessionLoopRuntimeState;

	if (CurrentPgSession == NULL)
		return &early_session_loop_state;

	return &CurrentPgSession->loop_state;
}

PgSessionNamespaceState *
PgCurrentSessionNamespaceState(void)
{
	PgSessionNamespaceState *namespace_state;

	if (likely(CurrentPgSessionNamespaceRuntimeState != NULL &&
			   CurrentPgSessionNamespaceRuntimeState->initialized))
		return CurrentPgSessionNamespaceRuntimeState;

	if (CurrentPgSession == NULL)
		namespace_state = &early_session_namespace;
	else
		namespace_state = &CurrentPgSession->namespace_state;

	if (!namespace_state->initialized)
		PgSessionInitializeNamespaceState(namespace_state);

	return namespace_state;
}



PgSessionDateTimeState *
PgCurrentSessionDateTimeState(void)
{
	PgSessionDateTimeState *datetime;

	if (likely(CurrentPgSessionDateTimeRuntimeState != NULL &&
			   CurrentPgSessionDateTimeRuntimeState->initialized))
		return CurrentPgSessionDateTimeRuntimeState;

	datetime = &PgCurrentOrEarlySession()->datetime;

	if (!datetime->initialized)
		PgSessionInitializeDateTimeState(datetime);

	return datetime;
}

PgSessionNamespaceState *
PgCurrentNamespaceState(void)
{
	return PgCurrentSessionNamespaceState();
}

char **
PgCurrentNamespaceSearchPathRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionNamespaceRuntimeState, PgCurrentSessionNamespaceState)->namespace_search_path_value;
}

PgSessionLocaleState *
PgCurrentSessionLocaleState(void)
{
	PgSessionLocaleState *locale;

	if (likely(CurrentPgSessionLocaleRuntimeState != NULL &&
			   CurrentPgSessionLocaleRuntimeState->initialized))
		return CurrentPgSessionLocaleRuntimeState;

	locale = &PgCurrentOrEarlySession()->locale;

	if (!locale->initialized)
		PgSessionInitializeLocaleState(locale);

	return locale;
}

PgSessionLocaleState *
PgCurrentLocaleState(void)
{
	return PgCurrentSessionLocaleState();
}

void **
PgCurrentIcuConverterRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLocaleRuntimeState, PgCurrentSessionLocaleState)->icu_converter;
}

char **
PgCurrentLocaleMessagesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLocaleRuntimeState, PgCurrentSessionLocaleState)->locale_messages_value;
}

char **
PgCurrentLocaleMonetaryRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLocaleRuntimeState, PgCurrentSessionLocaleState)->locale_monetary_value;
}

char **
PgCurrentLocaleNumericRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLocaleRuntimeState, PgCurrentSessionLocaleState)->locale_numeric_value;
}

char **
PgCurrentLocaleTimeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLocaleRuntimeState, PgCurrentSessionLocaleState)->locale_time_value;
}

int *
PgCurrentIcuValidationLevelRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLocaleRuntimeState, PgCurrentSessionLocaleState)->icu_validation_level_value;
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

MemoryContext *
PgCurrentDatabasePathContextRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_path_context;
}

bool *
PgCurrentDatabasePathOwnedRef(void)
{
	return &PgCurrentSessionDatabaseState()->database_path_owned;
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
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->date_style;
}

int *
PgCurrentDateOrderRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->date_order;
}

int *
PgCurrentIntervalStyleRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->interval_style;
}

char **
PgCurrentTimeZoneStringRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->timezone_string_value;
}

char **
PgCurrentLogTimeZoneStringRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->log_timezone_string_value;
}

pg_tz **
PgCurrentSessionTimeZoneRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->session_timezone_value;
}

pg_tz **
PgCurrentLogTimeZoneRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->log_timezone_value;
}

TimeZoneAbbrevTable **
PgCurrentTimeZoneAbbrevTableRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->timezone_abbrev_table;
}

PgSessionTzAbbrevCache *
PgCurrentTimeZoneAbbrevCache(void)
{
	return PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->timezone_abbrev_cache;
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

HTAB **
PgCurrentTSParserCacheHashRef(void)
{
	return &PgCurrentSessionTextSearchState()->parser_cache_hash;
}

TSParserCacheEntry **
PgCurrentTSLastUsedParserRef(void)
{
	return &PgCurrentSessionTextSearchState()->last_used_parser;
}

HTAB **
PgCurrentTSDictionaryCacheHashRef(void)
{
	return &PgCurrentSessionTextSearchState()->dictionary_cache_hash;
}

TSDictionaryCacheEntry **
PgCurrentTSLastUsedDictionaryRef(void)
{
	return &PgCurrentSessionTextSearchState()->last_used_dictionary;
}

HTAB **
PgCurrentTSConfigCacheHashRef(void)
{
	return &PgCurrentSessionTextSearchState()->config_cache_hash;
}

TSConfigCacheEntry **
PgCurrentTSLastUsedConfigRef(void)
{
	return &PgCurrentSessionTextSearchState()->last_used_config;
}

void **
PgCurrentPLpgSQLSessionStateRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plpgsql_state;
}

void **
PgCurrentPLpythonProcedureCacheRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plpython_procedure_cache;
}

MemoryContext *
PgCurrentPLpythonMemoryContextRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plpython_memory_context;
}

bool *
PgCurrentPLpythonResetRegisteredRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plpython_reset_registered;
}

MemoryContext *
PgCurrentPLperlMemoryContextRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_memory_context;
}

bool *
PgCurrentPLperlInitedRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_inited;
}

void **
PgCurrentPLperlInterpHashRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_interp_hash;
}

void **
PgCurrentPLperlProcHashRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_proc_hash;
}

void **
PgCurrentPLperlActiveInterpRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_active_interp;
}

void **
PgCurrentPLperlHeldInterpRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_held_interp;
}

bool *
PgCurrentPLperlUseStrictRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_use_strict;
}

char **
PgCurrentPLperlOnInitRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_on_init;
}

char **
PgCurrentPLperlOnPLperlInitRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_on_plperl_init;
}

char **
PgCurrentPLperlOnPLperluInitRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_on_plperlu_init;
}

bool *
PgCurrentPLperlEndingRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_ending;
}

void **
PgCurrentPLperlCurrentCallDataRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_current_call_data;
}

bool *
PgCurrentPLperlResetRegisteredRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plperl_reset_registered;
}

MemoryContext *
PgCurrentPLTclMemoryContextRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->pltcl_memory_context;
}

char **
PgCurrentPLTclStartProcRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->pltcl_start_proc;
}

char **
PgCurrentPLTclUStartProcRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->pltclu_start_proc;
}

void **
PgCurrentPLTclHoldInterpRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->pltcl_hold_interp;
}

void **
PgCurrentPLTclInterpHashRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->pltcl_interp_hash;
}

void **
PgCurrentPLTclProcHashRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->pltcl_proc_hash;
}

void **
PgCurrentPLTclCurrentCallStateRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->pltcl_current_call_state;
}

bool *
PgCurrentPLTclResetRegisteredRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->pltcl_reset_registered;
}

MemoryContext *
PgCurrentPLsampleMemoryContextRef(void)
{
	return &PgCurrentSessionExtensionModuleState()->plsample_memory_context;
}

void
PgSessionRegisterResetCallback(PgSessionResetCallback callback, void *arg)
{
	PgSessionExtensionModuleState *extension_modules;
	PgSessionResetCallbackItem *item;
	MemoryContext oldcontext;

	Assert(callback != NULL);

	extension_modules = PgCurrentSessionExtensionModuleState();

	if (CurrentPgSession != NULL)
		oldcontext = MemoryContextSwitchTo(PgSessionGetDynamicLibraryMemoryContext(CurrentPgSession));
	else
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	item = palloc_object(PgSessionResetCallbackItem);
	item->callback = callback;
	item->arg = arg;
	extension_modules->reset_callbacks =
		lappend(extension_modules->reset_callbacks, item);

	MemoryContextSwitchTo(oldcontext);
}

PgSessionInvalidationCallbackState *
PgCurrentInvalidationCallbackState(void)
{
	return PgCurrentSessionInvalidationCallbackState();
}

PgExecutionRelMapFile *
PgCurrentRelMapSharedMapRef(void)
{
	return &PgCurrentSessionRelMapState()->shared_map;
}

PgExecutionRelMapFile *
PgCurrentRelMapLocalMapRef(void)
{
	return &PgCurrentSessionRelMapState()->local_map;
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
