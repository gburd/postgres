/*-------------------------------------------------------------------------
 *
 * backend_runtime_guc.c
 *	  Runtime bridge accessors for session-owned GUC compatibility state.
 *
 * These accessors keep GUC backing variables mapped onto the current
 * PgSession while leaving runtime construction and top-level lifecycle
 * orchestration in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/misc/backend_runtime_guc.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "utils/backend_runtime.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

static PG_GLOBAL_RUNTIME PgRuntimeServerGUCState early_runtime_server_guc = {
	.initialized = true,
	.cluster_name_value = "",
	.config_file_name = NULL,
	.hba_file_name = NULL,
	.ident_file_name = NULL,
	.hosts_file_name = NULL,
	.external_pid_file_value = NULL
};

void
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

void
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

bool
PgRuntimeServerGUCStateHasConfigPaths(PgRuntimeServerGUCState *server_guc)
{
	return server_guc != NULL &&
		server_guc->initialized &&
		server_guc->config_file_name != NULL &&
		server_guc->config_file_name[0] != '\0';
}

PgRuntimeServerGUCState *
PgEarlyRuntimeServerGUCState(void)
{
	return &early_runtime_server_guc;
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

PgExecutionGUCErrorState *
PgCurrentExecutionGUCErrorState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionGUCErrorRuntimeState,
											   guc_error);
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

int *
PgCurrentSslRenegotiationLimitRef(void)
{
	return &PgCurrentSessionConnectionGUCState()->ssl_renegotiation_limit_value;
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

char **
PgCurrentDateStyleStringRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->datestyle_string_value;
}

char **
PgCurrentTimeZoneAbbreviationsStringRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionDateTimeRuntimeState, PgCurrentSessionDateTimeState)->timezone_abbreviations_string_value;
}

bool *
PgCurrentDefaultWithOidsRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->default_with_oids_value;
}

bool *
PgCurrentStandardConformingStringsRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->standard_conforming_strings_value;
}

double *
PgCurrentPhonyRandomSeedRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->phony_random_seed_value;
}

char **
PgCurrentSessionAuthorizationStringRef(void)
{
	return &PgCurrentSessionGeneralGUCState()->session_authorization_string_value;
}

char **
PgCurrentClientEncodingStringRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState)->client_encoding_string_value;
}

char **
PgCurrentServerEncodingStringRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionEncodingRuntimeState, PgCurrentSessionEncodingState)->server_encoding_string_value;
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

bool *
PgCurrentAllowSystemTableModsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionMiscGUCRuntimeState, PgCurrentSessionMiscGUCState)->allow_system_table_mods_value;
}

int *
PgCurrentMaxStackDepthRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionMiscGUCRuntimeState, PgCurrentSessionMiscGUCState)->max_stack_depth_kb;
}

ssize_t *
PgCurrentMaxStackDepthBytesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionMiscGUCRuntimeState, PgCurrentSessionMiscGUCState)->max_stack_depth_bytes;
}

char **
PgCurrentSessionPreloadLibrariesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionMiscGUCRuntimeState, PgCurrentSessionMiscGUCState)->session_preload_libraries_value;
}

char **
PgCurrentLocalPreloadLibrariesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionMiscGUCRuntimeState, PgCurrentSessionMiscGUCState)->local_preload_libraries_value;
}

char **
PgCurrentDynamicLibraryPathRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionMiscGUCRuntimeState, PgCurrentSessionMiscGUCState)->dynamic_library_path_value;
}

char **
PgCurrentExtensionControlPathRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionMiscGUCRuntimeState, PgCurrentSessionMiscGUCState)->extension_control_path_value;
}

bool *
PgCurrentUpdateProcessTitleRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionMiscGUCRuntimeState, PgCurrentSessionMiscGUCState)->update_process_title_value;
}

MemoryContext *
PgCurrentGUCMemoryContextRef(void)
{
	PgSessionGUCState *guc = PgCurrentSessionGUCState();

	if (CurrentPgSession == NULL &&
		guc->memory_context == NULL &&
		TopMemoryContext != NULL)
		(void) PgRuntimeGetOwnedMemoryContextWithSizes(&guc->memory_context,
													   "early GUC fallback state",
													   ALLOCSET_DEFAULT_SIZES);

	return &guc->memory_context;
}

/*
 * Non-allocating peek at the current GUC memory context.
 *
 * PgCurrentGUCMemoryContextRef() has an observation side effect: when no
 * session is installed yet it lazily creates the early-fallback context so
 * early GUC reads have somewhere to allocate.  That side effect makes it unsafe
 * for an invariant check that must see the cell as it stands -- notably
 * build_guc_variables()'s Assert(GUCMemoryContext == NULL), which reads through
 * the macro and would trip on the value the read itself just created.  This
 * peek returns the raw cell without allocating.
 */
MemoryContext
PgCurrentGUCMemoryContextPeek(void)
{
	return PgCurrentSessionGUCState()->memory_context;
}

struct config_generic **
PgCurrentGUCVariablesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionGUCRuntimeState, PgCurrentSessionGUCState)->variables;
}

struct config_generic_state **
PgCurrentGUCVariableStatesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionGUCRuntimeState, PgCurrentSessionGUCState)->variable_states;
}

int *
PgCurrentNumGUCVariablesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionGUCRuntimeState, PgCurrentSessionGUCState)->num_variables;
}

HTAB **
PgCurrentGUCHashTableRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionGUCRuntimeState, PgCurrentSessionGUCState)->hash_table;
}

dlist_head *
PgCurrentGUCNondefListRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionGUCRuntimeState, PgCurrentSessionGUCState)->nondef_list;
}

slist_head *
PgCurrentGUCStackListRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionGUCRuntimeState, PgCurrentSessionGUCState)->stack_list;
}

slist_head *
PgCurrentGUCReportListRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionGUCRuntimeState, PgCurrentSessionGUCState)->report_list;
}

bool *
PgCurrentGUCReportingEnabledRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionGUCRuntimeState, PgCurrentSessionGUCState)->reporting_enabled;
}

int *
PgCurrentGUCNestLevelRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionGUCRuntimeState, PgCurrentSessionGUCState)->nest_level;
}

int *
PgCurrentThreadedGUCMutexDepthRef(void)
{
	return &PgCurrentCarrierState()->threaded_guc_mutex_depth;
}

int *
PgCurrentThreadedRelOptionsMutexDepthRef(void)
{
	return &PgCurrentCarrierState()->threaded_reloptions_mutex_depth;
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

struct ReplicationState **
PgCurrentReplicationOriginSessionStateRef(void)
{
	return &PgCurrentSessionLogicalReplicationState()->session_replication_state;
}

MemoryContext *
PgCurrentLogicalRepRelMapContextRef(void)
{
	return &PgCurrentSessionLogicalReplicationState()->logical_rep_relmap_context;
}

HTAB **
PgCurrentLogicalRepRelMapRef(void)
{
	return &PgCurrentSessionLogicalReplicationState()->logical_rep_relmap;
}

MemoryContext *
PgCurrentLogicalRepPartMapContextRef(void)
{
	return &PgCurrentSessionLogicalReplicationState()->logical_rep_partmap_context;
}

HTAB **
PgCurrentLogicalRepPartMapRef(void)
{
	return &PgCurrentSessionLogicalReplicationState()->logical_rep_partmap;
}

bool *
PgCurrentPgOutputPublicationsValidRef(void)
{
	return &PgCurrentSessionLogicalReplicationState()->pgoutput_publications_valid;
}

HTAB **
PgCurrentPgOutputRelationSyncCacheRef(void)
{
	return &PgCurrentSessionLogicalReplicationState()->pgoutput_relation_sync_cache;
}

int *
PgCurrentLogicalRepSyncingRelationsStateRef(void)
{
	return &PgCurrentSessionLogicalReplicationState()->syncing_relations_state;
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
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionQueryMemoryRuntimeState, PgCurrentSessionQueryMemoryState)->work_mem_kb;
}

double *
PgCurrentHashMemMultiplierRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionQueryMemoryRuntimeState, PgCurrentSessionQueryMemoryState)->hash_mem_multiplier_value;
}

int *
PgCurrentMaintenanceWorkMemRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionQueryMemoryRuntimeState, PgCurrentSessionQueryMemoryState)->maintenance_work_mem_kb;
}

int *
PgCurrentMaxParallelMaintenanceWorkersRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionQueryMemoryRuntimeState, PgCurrentSessionQueryMemoryState)->max_parallel_maintenance_workers_value;
}

double *
PgCurrentSeqPageCostRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->seq_page_cost_value;
}

double *
PgCurrentRandomPageCostRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->random_page_cost_value;
}

double *
PgCurrentCpuTupleCostRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->cpu_tuple_cost_value;
}

double *
PgCurrentCpuIndexTupleCostRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->cpu_index_tuple_cost_value;
}

double *
PgCurrentCpuOperatorCostRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->cpu_operator_cost_value;
}

double *
PgCurrentParallelTupleCostRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->parallel_tuple_cost_value;
}

double *
PgCurrentParallelSetupCostRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->parallel_setup_cost_value;
}

double *
PgCurrentRecursiveWorktableFactorRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->recursive_worktable_factor_value;
}

int *
PgCurrentEffectiveCacheSizeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->effective_cache_size_pages;
}

Cost *
PgCurrentDisableCostRef(void)
{
	return (Cost *) &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->disable_cost_value;
}

int *
PgCurrentMaxParallelWorkersPerGatherRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->max_parallel_workers_per_gather_value;
}

int *
PgCurrentDebugParallelQueryRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->debug_parallel_query_value;
}

bool *
PgCurrentParallelLeaderParticipationRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerCostRuntimeState, PgCurrentSessionPlannerCostState)->parallel_leader_participation_value;
}

bool *
PgCurrentEnableSeqscanRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_seqscan_value;
}

bool *
PgCurrentEnableIndexscanRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_indexscan_value;
}

bool *
PgCurrentEnableIndexonlyscanRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_indexonlyscan_value;
}

bool *
PgCurrentEnableBitmapscanRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_bitmapscan_value;
}

bool *
PgCurrentEnableTidscanRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_tidscan_value;
}

bool *
PgCurrentEnableSortRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_sort_value;
}

bool *
PgCurrentEnableIncrementalSortRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_incremental_sort_value;
}

bool *
PgCurrentEnableHashaggRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_hashagg_value;
}

bool *
PgCurrentEnableNestloopRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_nestloop_value;
}

bool *
PgCurrentEnableMaterialRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_material_value;
}

bool *
PgCurrentEnableMemoizeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_memoize_value;
}

bool *
PgCurrentEnableMergejoinRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_mergejoin_value;
}

bool *
PgCurrentEnableHashjoinRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_hashjoin_value;
}

bool *
PgCurrentEnableGathermergeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_gathermerge_value;
}

bool *
PgCurrentEnablePartitionwiseJoinRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_partitionwise_join_value;
}

bool *
PgCurrentEnablePartitionwiseAggregateRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_partitionwise_aggregate_value;
}

bool *
PgCurrentEnableParallelAppendRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_parallel_append_value;
}

bool *
PgCurrentEnableParallelHashRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_parallel_hash_value;
}

bool *
PgCurrentEnablePartitionPruningRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_partition_pruning_value;
}

bool *
PgCurrentEnablePresortedAggregateRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_presorted_aggregate_value;
}

bool *
PgCurrentEnableAsyncAppendRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_async_append_value;
}

bool *
PgCurrentEnableDistinctReorderingRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_distinct_reordering_value;
}

bool *
PgCurrentEnableGeqoRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_geqo_value;
}

bool *
PgCurrentEnableEagerAggregateRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_eager_aggregate_value;
}

bool *
PgCurrentEnableGroupByReorderingRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_group_by_reordering_value;
}

bool *
PgCurrentEnableSelfJoinEliminationRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->enable_self_join_elimination_value;
}

double *
PgCurrentCursorTupleFractionRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->cursor_tuple_fraction_value;
}

int *
PgCurrentConstraintExclusionRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->constraint_exclusion_value;
}

int *
PgCurrentGeqoThresholdRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->geqo_threshold_value;
}

int *
PgCurrentGeqoEffortRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->Geqo_effort_value;
}

int *
PgCurrentGeqoPoolSizeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->Geqo_pool_size_value;
}

int *
PgCurrentGeqoGenerationsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->Geqo_generations_value;
}

double *
PgCurrentGeqoSelectionBiasRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->Geqo_selection_bias_value;
}

double *
PgCurrentGeqoSeedRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->Geqo_seed_value;
}

int *
PgCurrentGeqoPlannerExtensionIdRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->Geqo_planner_extension_id_value;
}

double *
PgCurrentMinEagerAggGroupSizeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->min_eager_agg_group_size_value;
}

int *
PgCurrentMinParallelTableScanSizeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->min_parallel_table_scan_size_blocks;
}

int *
PgCurrentMinParallelIndexScanSizeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->min_parallel_index_scan_size_blocks;
}

int *
PgCurrentFromCollapseLimitRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->from_collapse_limit_value;
}

int *
PgCurrentJoinCollapseLimitRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPlannerMethodRuntimeState, PgCurrentSessionPlannerMethodState)->join_collapse_limit_value;
}

int *
PgCurrentGUCCheckErrcodeValueRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionGUCErrorRuntimeState, PgCurrentExecutionGUCErrorState)->check_errcode_value;
}

char **
PgCurrentGUCCheckErrmsgStringRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionGUCErrorRuntimeState, PgCurrentExecutionGUCErrorState)->check_errmsg_string;
}

char **
PgCurrentGUCCheckErrdetailStringRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionGUCErrorRuntimeState, PgCurrentExecutionGUCErrorState)->check_errdetail_string;
}

char **
PgCurrentGUCCheckErrhintStringRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionGUCErrorRuntimeState, PgCurrentExecutionGUCErrorState)->check_errhint_string;
}

int *
PgCurrentFormatErrnumberRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionGUCErrorRuntimeState, PgCurrentExecutionGUCErrorState)->format_errnumber;
}

const char **
PgCurrentFormatDomainRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionGUCErrorRuntimeState, PgCurrentExecutionGUCErrorState)->format_domain;
}

unsigned int *
PgCurrentConfigFileLinenoRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionGUCErrorRuntimeState, PgCurrentExecutionGUCErrorState)->config_file_lineno;
}

const char **
PgCurrentGUCFlexFatalErrmsgRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionGUCErrorRuntimeState, PgCurrentExecutionGUCErrorState)->flex_fatal_errmsg;
}

sigjmp_buf **
PgCurrentGUCFlexFatalJmpRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionGUCErrorRuntimeState, PgCurrentExecutionGUCErrorState)->flex_fatal_jmp;
}
