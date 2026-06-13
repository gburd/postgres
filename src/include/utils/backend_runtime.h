/*-------------------------------------------------------------------------
 *
 * backend_runtime.h
 *	  Runtime/backend/session scaffolding for backend execution.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/utils/backend_runtime.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BACKEND_RUNTIME_H
#define BACKEND_RUNTIME_H

#include "access/session.h"
#include "common/relpath.h"
#include "fmgr.h"
#include "lib/ilist.h"
#include "libpq/hba.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "utils/backend_id.h"
#include "utils/global_lifetime.h"
#include "utils/palloc.h"

typedef struct PgRuntime PgRuntime;
typedef struct PgCarrier PgCarrier;
typedef struct PgBackend PgBackend;
typedef struct PgSession PgSession;
typedef struct PgConnection PgConnection;
typedef struct PgExecution PgExecution;
typedef struct PQcommMethods PQcommMethods;
typedef struct WaitEventSet WaitEventSet;
struct ClientSocket;
typedef void (*PgBackendExitContinuation) (int code);
typedef int (*PgSuspendCallback) (void *callback_arg);

typedef enum PgRuntimeKind
{
	PG_RUNTIME_PROCESS,
	PG_RUNTIME_THREAD_PER_SESSION
} PgRuntimeKind;

typedef enum PgCarrierKind
{
	PG_CARRIER_PROCESS,
	PG_CARRIER_THREAD
} PgCarrierKind;

typedef enum PgBackendLaunchModel
{
	PG_BACKEND_LAUNCH_PROCESS,
	PG_BACKEND_LAUNCH_THREAD
} PgBackendLaunchModel;

/*
 * Budget for one invocation of PgSessionStep().  The process-mode runner uses
 * a single-message budget today; later schedulers can extend this contract
 * without changing the caller shape.
 */
typedef struct PgStepBudget
{
	int			max_messages;
} PgStepBudget;

typedef enum PgStepResult
{
	PG_STEP_CONTINUE,
	PG_STEP_ERROR_RECOVERED
} PgStepResult;

/*
 * Logical interrupts target a backend object first.  In process mode these are
 * bridged back to the historical volatile globals serviced by
 * CHECK_FOR_INTERRUPTS(); later backend models can route these bits without
 * depending on Unix signals as the in-process representation.
 */
typedef enum PgBackendInterruptType
{
	PG_BACKEND_INTERRUPT_QUERY_CANCEL,
	PG_BACKEND_INTERRUPT_PROC_DIE,
	PG_BACKEND_INTERRUPT_CLIENT_CONNECTION_CHECK,
	PG_BACKEND_INTERRUPT_IDLE_IN_TRANSACTION_SESSION_TIMEOUT,
	PG_BACKEND_INTERRUPT_TRANSACTION_TIMEOUT,
	PG_BACKEND_INTERRUPT_IDLE_SESSION_TIMEOUT,
	PG_BACKEND_INTERRUPT_IDLE_STATS_UPDATE_TIMEOUT,
	PG_BACKEND_INTERRUPT_PROC_SIGNAL_BARRIER,
	PG_BACKEND_INTERRUPT_LOG_MEMORY_CONTEXT,
	PG_BACKEND_INTERRUPT_RECOVERY_CONFLICT,
	PG_BACKEND_INTERRUPT_CONFIG_RELOAD,
	PG_BACKEND_INTERRUPT_SHUTDOWN_REQUEST,
	PG_BACKEND_INTERRUPT_CATCHUP,
	PG_BACKEND_INTERRUPT_NOTIFY,
	PG_BACKEND_INTERRUPT_PARALLEL_MESSAGE,
	PG_BACKEND_INTERRUPT_PARALLEL_APPLY_MESSAGE,
	PG_BACKEND_INTERRUPT_SLOT_SYNC_MESSAGE,
	PG_BACKEND_INTERRUPT_REPACK_MESSAGE,
	PG_BACKEND_INTERRUPT_WAKEUP_STOP,
	PG_BACKEND_INTERRUPT_AUTOVAC_LAUNCHER,
	PG_BACKEND_INTERRUPT_CHECKPOINTER_SHUTDOWN_XLOG,
	PG_BACKEND_INTERRUPT_LOG_ROTATE,
	PG_BACKEND_INTERRUPT_STARTUP_PROMOTE,
	PG_BACKEND_INTERRUPT_COUNT
} PgBackendInterruptType;

typedef uint32 PgBackendInterruptMask;

#define PG_BACKEND_INTERRUPT_MASK(interrupt_type) \
	(((PgBackendInterruptMask) 1) << (interrupt_type))

typedef struct PgBackendInterruptMailbox
{
	pg_atomic_uint32 pending_mask;
	volatile int proc_die_sender_pid;
	volatile int proc_die_sender_uid;
} PgBackendInterruptMailbox;

typedef enum PgWaitKind
{
	PG_WAIT_KIND_NONE,
	PG_WAIT_KIND_EVENT_SET
} PgWaitKind;

typedef struct PgWaitSpec
{
	PgWaitKind	kind;
	uint32		wait_event_info;
	uint32		wake_events;
	long		timeout;
} PgWaitSpec;

typedef struct PgBackendWaitState
{
	PgWaitSpec	spec;
	pg_atomic_uint32 waiting;
} PgBackendWaitState;

typedef struct PgBackendCoreState
{
	bool		exit_on_any_error;
	int			proc_pid;
	pg_time_t	start_time;
	TimestampTz start_timestamp;
	struct Latch *latch;
	int			pm_child_slot;
	char		output_file_name[MAXPGPATH];
	ProcessingMode mode;
	bool		ignore_system_indexes;
} PgBackendCoreState;

typedef struct PgExecutionDebugState
{
	const char *debug_query_string;
} PgExecutionDebugState;

typedef struct PgExecutionErrorState
{
	struct ErrorContextCallback *context_stack;
	sigjmp_buf *exception_stack;
} PgExecutionErrorState;

typedef struct PgExecutionMemoryContextState
{
	MemoryContext current_context;
	MemoryContext error_context;
	MemoryContext message_context;
	MemoryContext top_transaction_context;
	MemoryContext cur_transaction_context;
	MemoryContext portal_context;
} PgExecutionMemoryContextState;

typedef struct PgExecutionResourceOwnerState
{
	struct ResourceOwnerData *current_owner;
	struct ResourceOwnerData *cur_transaction_owner;
	struct ResourceOwnerData *top_transaction_owner;
} PgExecutionResourceOwnerState;

typedef struct PgSessionDatabaseState
{
	Oid			database_id;
	Oid			database_tablespace;
	bool		database_has_login_event_triggers;

	/*
	 * Path relative to DataDir of this database's primary directory, ie its
	 * directory in the default tablespace.
	 */
	char	   *database_path;
} PgSessionDatabaseState;

typedef struct PgSessionTablespaceState
{
	bool		initialized;
	char	   *default_tablespace_name;
	char	   *temp_tablespaces_names;
	bool		allow_in_place_tablespaces_value;
	Oid			binary_upgrade_next_pg_tablespace_oid_value;
} PgSessionTablespaceState;

typedef struct PgSessionBinaryUpgradeState
{
	bool		initialized;
	Oid			binary_upgrade_next_pg_type_oid_value;
	Oid			binary_upgrade_next_array_pg_type_oid_value;
	Oid			binary_upgrade_next_mrng_pg_type_oid_value;
	Oid			binary_upgrade_next_mrng_array_pg_type_oid_value;
	Oid			binary_upgrade_next_heap_pg_class_oid_value;
	RelFileNumber binary_upgrade_next_heap_pg_class_relfilenumber_value;
	Oid			binary_upgrade_next_index_pg_class_oid_value;
	RelFileNumber binary_upgrade_next_index_pg_class_relfilenumber_value;
	Oid			binary_upgrade_next_toast_pg_class_oid_value;
	RelFileNumber binary_upgrade_next_toast_pg_class_relfilenumber_value;
	Oid			binary_upgrade_next_pg_enum_oid_value;
	Oid			binary_upgrade_next_pg_authid_oid_value;
	bool		binary_upgrade_record_init_privs_value;
} PgSessionBinaryUpgradeState;

typedef struct PgSessionDateTimeState
{
	bool		initialized;
	int			date_style;
	int			date_order;
	int			interval_style;
} PgSessionDateTimeState;

typedef struct PgSessionParserState
{
	bool		initialized;
	bool		transform_null_equals_value;
	int			backslash_quote_value;
} PgSessionParserState;

typedef struct PgSessionVacuumState
{
	bool		initialized;
	int			vacuum_buffer_usage_limit_kb;
	int			vacuum_cost_page_hit_value;
	int			vacuum_cost_page_miss_value;
	int			vacuum_cost_page_dirty_value;
	int			vacuum_cost_limit_value;
	double		vacuum_cost_delay_ms;
	int			default_statistics_target_value;
	int			vacuum_freeze_min_age_value;
	int			vacuum_freeze_table_age_value;
	int			vacuum_multixact_freeze_min_age_value;
	int			vacuum_multixact_freeze_table_age_value;
	int			vacuum_failsafe_age_value;
	int			vacuum_multixact_failsafe_age_value;
	bool		track_cost_delay_timing_value;
	bool		vacuum_truncate_value;
	double		vacuum_max_eager_freeze_failure_rate_value;
	double		local_vacuum_cost_delay_ms;
	int			local_vacuum_cost_limit_value;
} PgSessionVacuumState;

typedef struct PgSessionBufferIOState
{
	bool		initialized;
	bool		zero_damaged_pages_value;
	bool		track_io_timing_value;
	int			effective_io_concurrency_value;
	int			maintenance_io_concurrency_value;
	int			io_combine_limit_value;
	int			io_combine_limit_guc_value;
	int			backend_flush_after_value;
} PgSessionBufferIOState;

typedef struct PgSessionXactDefaultState
{
	bool		initialized;
	int			default_xact_iso_level;
	bool		default_xact_read_only;
	bool		default_xact_deferrable;
	int			synchronous_commit_value;
} PgSessionXactDefaultState;

typedef struct PgSessionLockWaitState
{
	bool		initialized;
	int			deadlock_timeout_ms;
	int			statement_timeout_ms;
	int			lock_timeout_ms;
	int			idle_in_transaction_session_timeout_ms;
	int			transaction_timeout_ms;
	int			idle_session_timeout_ms;
	bool		log_lock_waits_value;
	bool		log_lock_failures_value;
	int			trace_lock_oidmin_value;
	bool		trace_locks_value;
	bool		trace_userlocks_value;
	int			trace_lock_table_value;
	bool		debug_deadlocks_value;
	bool		trace_lwlocks_value;
} PgSessionLockWaitState;

typedef struct PgSessionLoggingState
{
	bool		initialized;
	bool		debug_print_plan_value;
	bool		debug_print_parse_value;
	bool		debug_print_raw_parse_value;
	bool		debug_print_rewritten_value;
	bool		debug_pretty_print_value;
#ifdef DEBUG_NODE_TESTS_ENABLED
	bool		debug_copy_parse_plan_trees_value;
	bool		debug_write_read_parse_plan_trees_value;
	bool		debug_raw_expression_coverage_test_value;
#endif
	bool		log_parser_stats_value;
	bool		log_planner_stats_value;
	bool		log_executor_stats_value;
	bool		log_statement_stats_value;
	bool		log_btree_build_stats_value;
	char	   *event_source_value;
	bool		log_duration_value;
	int			log_error_verbosity_value;
	int			log_parameter_max_length_value;
	int			log_parameter_max_length_on_error_value;
	int			log_min_error_statement_value;
	int			log_min_messages_values[BACKEND_NUM_TYPES];
	char	   *log_min_messages_string_value;
	int			client_min_messages_value;
	int			log_min_duration_sample_value;
	int			log_min_duration_statement_value;
	int			log_temp_files_value;
	double		log_statement_sample_rate_value;
	double		log_xact_sample_rate_value;
	char	   *backtrace_functions_value;
	char	   *backtrace_function_list_value;
} PgSessionLoggingState;

typedef struct PgSessionMiscGUCState
{
	bool		initialized;
	bool		allow_system_table_mods_value;
	int			max_stack_depth_kb;
	ssize_t		max_stack_depth_bytes;
	char	   *session_preload_libraries_value;
	char	   *local_preload_libraries_value;
	char	   *dynamic_library_path_value;
} PgSessionMiscGUCState;

typedef struct PgSessionPgStatState
{
	bool		initialized;
	bool		track_counts;
	int			track_functions;
	int			fetch_consistency;
	bool		track_activities;
	SessionEndType session_end_cause;
	PgStat_Counter last_session_report_time;
} PgSessionPgStatState;

typedef struct PgSessionQueryIdState
{
	bool		initialized;
	int			compute_query_id_value;
	bool		query_id_enabled_value;
} PgSessionQueryIdState;

typedef struct PgSessionStorageGUCState
{
	bool		initialized;
	bool		ignore_checksum_failure_value;
	int			file_copy_method_value;
} PgSessionStorageGUCState;

typedef struct PgSessionUserGUCState
{
	bool		initialized;
	int			password_encryption_value;
	char	   *createrole_self_grant_value;
	bool		createrole_self_grant_enabled;
	unsigned	createrole_self_grant_options_specified;
	bool		createrole_self_grant_options_admin;
	bool		createrole_self_grant_options_inherit;
	bool		createrole_self_grant_options_set;
} PgSessionUserGUCState;

typedef struct PgSessionCommandGUCState
{
	bool		initialized;
	int			session_replication_role_value;
	bool		event_triggers_value;
	bool		trace_notify_value;
} PgSessionCommandGUCState;

typedef struct PgSessionReplicationGUCState
{
	bool		initialized;
	int			wal_sender_timeout_ms;
	int			wal_sender_shutdown_timeout_ms;
	bool		log_replication_commands_value;
	int			wal_receiver_timeout_ms;
	int			logical_decoding_work_mem_kb;
	int			debug_logical_replication_streaming_value;
} PgSessionReplicationGUCState;

typedef struct PgSessionGeneralGUCState
{
	bool		initialized;
	bool		allow_alter_system_value;
	bool		row_security_value;
	bool		check_function_bodies_value;
	bool		current_role_is_superuser_value;
	int			temp_file_limit_kb;
	int			num_temp_buffers_blocks;
	char	   *role_string_value;
	bool		lo_compat_privileges_value;
	int			extra_float_digits_value;
	int			bytea_output_value;
	int			xmlbinary_value;
	bool		quote_all_identifiers_value;
	int			plan_cache_mode_value;
	int			gin_fuzzy_search_limit_value;
	int			gin_pending_list_limit_value;
} PgSessionGeneralGUCState;

typedef struct PgSessionAccessWalGUCState
{
	bool		initialized;
	char	   *default_table_access_method_value;
	bool		synchronize_seqscans_value;
	int			default_toast_compression_value;
	int			wal_compression_value;
	bool		wal_init_zero_value;
	bool		wal_recycle_value;
	char	   *wal_consistency_checking_string_value;
	bool	   *wal_consistency_checking_value;
	int			commit_delay_us;
	int			commit_siblings_value;
	bool		track_wal_io_timing_value;
	int			wal_skip_threshold_kb;
#ifdef WAL_DEBUG
	bool		xlog_debug_value;
#endif
#ifdef TRACE_SYNCSCAN
	bool		trace_syncscan_value;
#endif
} PgSessionAccessWalGUCState;

typedef struct PgSessionQueryMemoryState
{
	bool		initialized;
	int			work_mem_kb;
	double		hash_mem_multiplier_value;
	int			maintenance_work_mem_kb;
	int			max_parallel_maintenance_workers_value;
} PgSessionQueryMemoryState;

typedef struct PgSessionPlannerCostState
{
	bool		initialized;
	double		seq_page_cost_value;
	double		random_page_cost_value;
	double		cpu_tuple_cost_value;
	double		cpu_index_tuple_cost_value;
	double		cpu_operator_cost_value;
	double		parallel_tuple_cost_value;
	double		parallel_setup_cost_value;
	double		recursive_worktable_factor_value;
	int			effective_cache_size_pages;
	double		disable_cost_value;
	int			max_parallel_workers_per_gather_value;
	int			debug_parallel_query_value;
	bool		parallel_leader_participation_value;
} PgSessionPlannerCostState;

typedef struct PgSessionPlannerMethodState
{
	bool		initialized;
	bool		enable_seqscan_value;
	bool		enable_indexscan_value;
	bool		enable_indexonlyscan_value;
	bool		enable_bitmapscan_value;
	bool		enable_tidscan_value;
	bool		enable_sort_value;
	bool		enable_incremental_sort_value;
	bool		enable_hashagg_value;
	bool		enable_nestloop_value;
	bool		enable_material_value;
	bool		enable_memoize_value;
	bool		enable_mergejoin_value;
	bool		enable_hashjoin_value;
	bool		enable_gathermerge_value;
	bool		enable_partitionwise_join_value;
	bool		enable_partitionwise_aggregate_value;
	bool		enable_parallel_append_value;
	bool		enable_parallel_hash_value;
	bool		enable_partition_pruning_value;
	bool		enable_presorted_aggregate_value;
	bool		enable_async_append_value;
	bool		enable_distinct_reordering_value;
	bool		enable_geqo_value;
	bool		enable_eager_aggregate_value;
	bool		enable_group_by_reordering_value;
	bool		enable_self_join_elimination_value;
	double		cursor_tuple_fraction_value;
	int			constraint_exclusion_value;
	int			geqo_threshold_value;
	int			Geqo_effort_value;
	int			Geqo_pool_size_value;
	int			Geqo_generations_value;
	double		Geqo_selection_bias_value;
	double		Geqo_seed_value;
	int			Geqo_planner_extension_id_value;
	double		min_eager_agg_group_size_value;
	int			min_parallel_table_scan_size_blocks;
	int			min_parallel_index_scan_size_blocks;
	int			from_collapse_limit_value;
	int			join_collapse_limit_value;
} PgSessionPlannerMethodState;

#define PG_CONNECTION_SEND_BUFFER_SIZE 8192
#define PG_CONNECTION_RECV_BUFFER_SIZE 8192
#define PG_CONNECTION_CANCEL_KEY_LENGTH 32

typedef struct PgConnectionIdentityState
{
	struct Port *port;
	uint8		cancel_key[PG_CONNECTION_CANCEL_KEY_LENGTH];
	int			cancel_key_length;
} PgConnectionIdentityState;

typedef struct PgConnectionSocketIOState
{
	char	   *send_buffer;
	int			send_buffer_size;
	size_t		send_pointer;
	size_t		send_start;
	char		recv_buffer[PG_CONNECTION_RECV_BUFFER_SIZE];
	int			recv_pointer;
	int			recv_length;
	bool		comm_busy;
	bool		comm_reading_msg;
} PgConnectionSocketIOState;

typedef struct PgConnectionProtocolState
{
	const PQcommMethods *comm_methods;
	WaitEventSet *fe_be_wait_set;
	uint32		frontend_protocol;
} PgConnectionProtocolState;

typedef struct PgConnectionInterruptState
{
	volatile sig_atomic_t check_client_connection_pending;
	volatile sig_atomic_t client_connection_lost;
} PgConnectionInterruptState;

typedef struct PgConnectionStartupState
{
	bool		client_auth_in_progress;
	struct ClientSocket *client_socket;
} PgConnectionStartupState;

typedef struct PgConnectionClientConnectionInfoState
{
	const char *authn_id;
	UserAuth	auth_method;
} PgConnectionClientConnectionInfoState;

/*
 * Main-loop state owned by PgSession. Some of this state used to be volatile
 * locals in PostgresMain(); keep the loop flags volatile because they must
 * survive the top-level longjmp used for backend error recovery.
 */
typedef struct PgSessionLoopState
{
	volatile bool send_ready_for_query;
	volatile bool idle_in_transaction_timeout_enabled;
	volatile bool idle_session_timeout_enabled;
	volatile bool doing_extended_query_message;
	volatile bool ignore_till_sync;
	volatile bool step_error_boundary_active;
} PgSessionLoopState;

struct PgRuntime
{
	PgRuntimeKind kind;
	PgCarrier  *current_carrier;
	PgBackendModel extension_backend_model;

	/*
	 * Optional continuation used after PgBackendExitCleanup().  Process mode
	 * leaves this NULL and falls through to exit().  A threaded runtime must
	 * install a handler that removes the logical backend from its scheduler
	 * without returning to the cleaned-up backend stack.
	 */
	PgBackendExitContinuation exit_backend;
};

struct PgCarrier
{
	PgCarrierKind kind;
	PgRuntime  *runtime;
	PgBackend  *current_backend;
	PgSession  *current_session;
	PgExecution *current_execution;
};

struct PgBackend
{
	PgBackendId id;
	PgRuntime  *runtime;
	PgCarrier  *carrier;
	PgSession  *session;
	PgConnection *connection;
	PgExecution *execution;
	PgBackendInterruptMailbox interrupts;
	struct Latch *interrupt_latch;
	PgBackendExitState exit_state;
	PgBackendCoreState core;
	PgBackendPendingInterruptState pending_interrupts;
	PgBackendInterruptHoldoffState interrupt_holdoffs;
	PgBackendWaitState wait_state;

	/* Backend-local dynamic shared memory mappings and detach callbacks. */
	dlist_head	dsm_segment_list;

	BackendType backend_type;
};

struct PgSession
{
	PgBackend  *backend;
	PgConnection *connection;
	PgExecution *execution;
	Session    *legacy_session;
	PgSessionLoopState loop_state;
	PgSessionDatabaseState database;
	PgSessionTablespaceState tablespace;
	PgSessionBinaryUpgradeState binary_upgrade;
	PgSessionDateTimeState datetime;
	PgSessionParserState parser;
	PgSessionVacuumState vacuum;
	PgSessionBufferIOState buffer_io;
	PgSessionXactDefaultState xact_defaults;
	PgSessionLockWaitState lock_wait;
	PgSessionLoggingState logging;
	PgSessionMiscGUCState misc_guc;
	PgSessionPgStatState pgstat;
	PgSessionQueryIdState query_id;
	PgSessionStorageGUCState storage_guc;
	PgSessionUserGUCState user_guc;
	PgSessionCommandGUCState command_guc;
	PgSessionReplicationGUCState replication_guc;
	PgSessionGeneralGUCState general_guc;
	PgSessionAccessWalGUCState access_wal_guc;
	PgSessionQueryMemoryState query_memory;
	PgSessionPlannerCostState planner_cost;
	PgSessionPlannerMethodState planner_method;
};

struct PgConnection
{
	PgBackend  *backend;
	PgSession  *session;
	PgConnectionIdentityState identity;
	PgConnectionSocketIOState socket_io;
	PgConnectionProtocolState protocol;
	PgConnectionInterruptState interrupts;
	PgConnectionStartupState startup;
	PgConnectionClientConnectionInfoState client_connection_info;
};

struct PgExecution
{
	PgBackend  *backend;
	PgSession  *session;
	PgCarrier  *carrier;
	PgExecutionDebugState debug;
	PgExecutionErrorState error;
	PgExecutionMemoryContextState memory_contexts;
	PgExecutionResourceOwnerState resource_owners;
};

typedef struct PgThreadBackendRuntimeState
{
	PgCarrier	carrier;
	PgBackend	backend;
	PgSession	session;
	PgConnection connection;
	PgExecution execution;
} PgThreadBackendRuntimeState;

extern PGDLLIMPORT PG_GLOBAL_RUNTIME PgRuntime *CurrentPgRuntime;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgCarrier *CurrentPgCarrier;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgBackend *CurrentPgBackend;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgSession *CurrentPgSession;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgConnection *CurrentPgConnection;
extern PGDLLIMPORT PG_THREAD_LOCAL PG_GLOBAL_CARRIER PgExecution *CurrentPgExecution;

extern MemoryContext *PgCurrentMemoryContextRef(void);
extern MemoryContext *PgErrorContextRef(void);
extern MemoryContext *PgMessageContextRef(void);
extern MemoryContext *PgTopTransactionContextRef(void);
extern MemoryContext *PgCurTransactionContextRef(void);
extern MemoryContext *PgPortalContextRef(void);
extern int *PgCurrentDeadlockTimeoutRef(void);
extern int *PgCurrentStatementTimeoutRef(void);
extern int *PgCurrentLockTimeoutRef(void);
extern int *PgCurrentIdleInTransactionSessionTimeoutRef(void);
extern int *PgCurrentTransactionTimeoutRef(void);
extern int *PgCurrentIdleSessionTimeoutRef(void);
extern bool *PgCurrentLogLockWaitsRef(void);
extern bool *PgCurrentLogLockFailuresRef(void);
extern int *PgCurrentTraceLockOidMinRef(void);
extern bool *PgCurrentTraceLocksRef(void);
extern bool *PgCurrentTraceUserlocksRef(void);
extern int *PgCurrentTraceLockTableRef(void);
extern bool *PgCurrentDebugDeadlocksRef(void);
extern bool *PgCurrentTraceLwlocksRef(void);
extern bool *PgCurrentDebugPrintPlanRef(void);
extern bool *PgCurrentDebugPrintParseRef(void);
extern bool *PgCurrentDebugPrintRawParseRef(void);
extern bool *PgCurrentDebugPrintRewrittenRef(void);
extern bool *PgCurrentDebugPrettyPrintRef(void);
#ifdef DEBUG_NODE_TESTS_ENABLED
extern bool *PgCurrentDebugCopyParsePlanTreesRef(void);
extern bool *PgCurrentDebugWriteReadParsePlanTreesRef(void);
extern bool *PgCurrentDebugRawExpressionCoverageTestRef(void);
#endif
extern bool *PgCurrentLogParserStatsRef(void);
extern bool *PgCurrentLogPlannerStatsRef(void);
extern bool *PgCurrentLogExecutorStatsRef(void);
extern bool *PgCurrentLogStatementStatsRef(void);
extern bool *PgCurrentLogBtreeBuildStatsRef(void);
extern char **PgCurrentEventSourceRef(void);
extern bool *PgCurrentLogDurationRef(void);
extern int *PgCurrentLogErrorVerbosityRef(void);
extern int *PgCurrentLogParameterMaxLengthRef(void);
extern int *PgCurrentLogParameterMaxLengthOnErrorRef(void);
extern int *PgCurrentLogMinErrorStatementRef(void);
extern int *PgCurrentLogMinMessagesArrayRef(void);
extern char **PgCurrentLogMinMessagesStringRef(void);
extern int *PgCurrentClientMinMessagesRef(void);
extern int *PgCurrentLogMinDurationSampleRef(void);
extern int *PgCurrentLogMinDurationStatementRef(void);
extern int *PgCurrentLogTempFilesRef(void);
extern double *PgCurrentLogStatementSampleRateRef(void);
extern double *PgCurrentLogXactSampleRateRef(void);
extern char **PgCurrentBacktraceFunctionsRef(void);
extern char **PgCurrentBacktraceFunctionListRef(void);
extern bool *PgCurrentAllowSystemTableModsRef(void);
extern int *PgCurrentMaxStackDepthRef(void);
extern ssize_t *PgCurrentMaxStackDepthBytesRef(void);
extern char **PgCurrentSessionPreloadLibrariesRef(void);
extern char **PgCurrentLocalPreloadLibrariesRef(void);
extern char **PgCurrentDynamicLibraryPathRef(void);
extern bool *PgCurrentPgStatTrackCountsRef(void);
extern int *PgCurrentPgStatTrackFunctionsRef(void);
extern int *PgCurrentPgStatFetchConsistencyRef(void);
extern bool *PgCurrentPgStatTrackActivitiesRef(void);
extern SessionEndType *PgCurrentPgStatSessionEndCauseRef(void);
extern PgStat_Counter *PgCurrentPgStatLastSessionReportTimeRef(void);
extern int *PgCurrentComputeQueryIdRef(void);
extern bool *PgCurrentQueryIdEnabledRef(void);
extern bool *PgCurrentIgnoreChecksumFailureRef(void);
extern int *PgCurrentFileCopyMethodRef(void);
extern int *PgCurrentPasswordEncryptionRef(void);
extern char **PgCurrentCreateRoleSelfGrantRef(void);
extern bool *PgCurrentCreateRoleSelfGrantEnabledRef(void);
extern unsigned *PgCurrentCreateRoleSelfGrantOptionsSpecifiedRef(void);
extern bool *PgCurrentCreateRoleSelfGrantOptionsAdminRef(void);
extern bool *PgCurrentCreateRoleSelfGrantOptionsInheritRef(void);
extern bool *PgCurrentCreateRoleSelfGrantOptionsSetRef(void);
extern int *PgCurrentSessionReplicationRoleRef(void);
extern bool *PgCurrentEventTriggersRef(void);
extern bool *PgCurrentTraceNotifyRef(void);
extern int *PgCurrentWalSenderTimeoutRef(void);
extern int *PgCurrentWalSenderShutdownTimeoutRef(void);
extern bool *PgCurrentLogReplicationCommandsRef(void);
extern int *PgCurrentWalReceiverTimeoutRef(void);
extern int *PgCurrentLogicalDecodingWorkMemRef(void);
extern int *PgCurrentDebugLogicalReplicationStreamingRef(void);
extern bool *PgCurrentAllowAlterSystemRef(void);
extern bool *PgCurrentRowSecurityRef(void);
extern bool *PgCurrentCheckFunctionBodiesRef(void);
extern bool *PgCurrentCurrentRoleIsSuperuserRef(void);
extern int *PgCurrentTempFileLimitRef(void);
extern int *PgCurrentNumTempBuffersRef(void);
extern char **PgCurrentRoleStringRef(void);
extern bool *PgCurrentLoCompatPrivilegesRef(void);
extern int *PgCurrentExtraFloatDigitsRef(void);
extern int *PgCurrentByteaOutputRef(void);
extern int *PgCurrentXmlBinaryRef(void);
extern bool *PgCurrentQuoteAllIdentifiersRef(void);
extern int *PgCurrentPlanCacheModeRef(void);
extern int *PgCurrentGinFuzzySearchLimitRef(void);
extern int *PgCurrentGinPendingListLimitRef(void);
extern char **PgCurrentDefaultTableAccessMethodRef(void);
extern bool *PgCurrentSynchronizeSeqscansRef(void);
extern int *PgCurrentDefaultToastCompressionRef(void);
extern int *PgCurrentWalCompressionRef(void);
extern bool *PgCurrentWalInitZeroRef(void);
extern bool *PgCurrentWalRecycleRef(void);
extern char **PgCurrentWalConsistencyCheckingStringRef(void);
extern bool **PgCurrentWalConsistencyCheckingRef(void);
extern int *PgCurrentCommitDelayRef(void);
extern int *PgCurrentCommitSiblingsRef(void);
extern bool *PgCurrentTrackWalIoTimingRef(void);
extern int *PgCurrentWalSkipThresholdRef(void);
#ifdef WAL_DEBUG
extern bool *PgCurrentXLogDebugRef(void);
#endif
#ifdef TRACE_SYNCSCAN
extern bool *PgCurrentTraceSyncscanRef(void);
#endif

extern void InitializePgProcessRuntime(void);
extern void InitializePgThreadRuntime(PgBackendExitContinuation exit_backend);
extern void InitializePgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state,
												 BackendType backend_type,
												 struct Port *port,
												 struct Latch *interrupt_latch);
extern void InstallPgThreadBackendRuntimeState(PgThreadBackendRuntimeState *state);
extern void InitializePgThreadBackendRuntime(PgThreadBackendRuntimeState *state,
											 BackendType backend_type,
											 struct Port *port,
											 struct Latch *interrupt_latch);
extern void PgSetCurrentSession(PgSession *session);
extern Session *PgSessionGetLegacySession(PgSession *session);
extern void PgSessionSetLegacySession(PgSession *session,
									   Session *legacy_session);
extern Session *PgCurrentLegacySession(void);
extern struct Port **PgConnectionProcPortRef(PgConnection *connection);
extern struct Port **PgCurrentProcPortRef(void);
extern uint8 *PgConnectionCancelKey(PgConnection *connection);
extern uint8 *PgCurrentCancelKey(void);
extern int *PgConnectionCancelKeyLengthRef(PgConnection *connection);
extern int *PgCurrentCancelKeyLengthRef(void);
extern const char **PgExecutionDebugQueryStringRef(PgExecution *execution);
extern const char **PgCurrentDebugQueryStringRef(void);
extern PgConnectionSocketIOState *PgConnectionSocketIORef(PgConnection *connection);
extern PgConnectionSocketIOState *PgCurrentConnectionSocketIORef(void);
extern const PQcommMethods **PgConnectionPqCommMethodsRef(PgConnection *connection);
extern const PQcommMethods **PgCurrentPqCommMethodsRef(void);
extern WaitEventSet **PgConnectionFeBeWaitSetRef(PgConnection *connection);
extern WaitEventSet **PgCurrentFeBeWaitSetRef(void);
extern uint32 *PgConnectionFrontendProtocolRef(PgConnection *connection);
extern uint32 *PgCurrentFrontendProtocolRef(void);
extern volatile sig_atomic_t *PgConnectionCheckClientConnectionPendingRef(PgConnection *connection);
extern volatile sig_atomic_t *PgCurrentCheckClientConnectionPendingRef(void);
extern volatile sig_atomic_t *PgConnectionClientConnectionLostRef(PgConnection *connection);
extern volatile sig_atomic_t *PgCurrentClientConnectionLostRef(void);
extern bool *PgConnectionClientAuthInProgressRef(PgConnection *connection);
extern bool *PgCurrentClientAuthInProgressRef(void);
extern struct ClientSocket **PgConnectionClientSocketRef(PgConnection *connection);
extern struct ClientSocket **PgCurrentClientSocketRef(void);
extern void *PgConnectionClientConnectionInfoRef(PgConnection *connection);
extern void *PgCurrentClientConnectionInfoRef(void);
extern PgBackendLaunchModel PgRuntimeGetBackendLaunchModel(BackendType backend_type);
extern bool PgRuntimeShouldThreadBackend(BackendType backend_type);
extern PgBackendModel PgRuntimeGetExtensionBackendModel(void);
extern void PgRuntimeSetExtensionBackendModel(PgBackendModel backend_model);
extern void PgBackendInitializeInterrupts(PgBackend *backend);
extern void PgBackendSetInterruptLatch(PgBackend *backend,
										struct Latch *interrupt_latch);
extern PgBackendId PgBackendGetId(PgBackend *backend);
extern PgBackendId PgCurrentBackendId(void);
extern int	PgBackendGetSignalPid(PgBackend *backend);
extern int	PgCurrentBackendSignalPid(void);
extern bool PgBackendUsesProcessSignals(PgBackend *backend);
extern void PgBackendWakeup(PgBackend *backend);
extern void PgBackendRaiseInterrupt(PgBackend *backend,
									 PgBackendInterruptType interrupt_type);
extern void PgBackendRaiseProcDieInterrupt(PgBackend *backend, int sender_pid,
										   int sender_uid);
extern void PgCurrentBackendRaiseInterrupt(PgBackendInterruptType interrupt_type);
extern void PgCurrentBackendRaiseProcDieInterrupt(int sender_pid,
												 int sender_uid);
extern PgBackendInterruptMask PgBackendConsumeInterrupts(PgBackend *backend);
extern void PgBackendConsumeProcDieSender(PgBackend *backend, int *sender_pid,
										  int *sender_uid);
extern bool PgCurrentBackendHasPendingInterrupts(void);
extern void PgCurrentBackendApplyInterrupts(void);
extern int	PgSuspend(const PgWaitSpec *wait_spec,
					  PgSuspendCallback callback, void *callback_arg);
extern PgStepResult PgSessionStep(PgSession *session, PgStepBudget budget);
pg_noreturn extern void PgSessionRun(PgSession *session);

#endif							/* BACKEND_RUNTIME_H */
