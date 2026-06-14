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
#include "postgres.h"

#include "optimizer/cost.h"
#include "optimizer/geqo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

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
	return &PgCurrentSessionDateTimeState()->datestyle_string_value;
}

char **
PgCurrentTimeZoneAbbreviationsStringRef(void)
{
	return &PgCurrentSessionDateTimeState()->timezone_abbreviations_string_value;
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
	return &PgCurrentSessionEncodingState()->client_encoding_string_value;
}

char **
PgCurrentServerEncodingStringRef(void)
{
	return &PgCurrentSessionEncodingState()->server_encoding_string_value;
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
PgCurrentUpdateProcessTitleRef(void)
{
	return &PgCurrentSessionMiscGUCState()->update_process_title_value;
}

MemoryContext *
PgCurrentGUCMemoryContextRef(void)
{
	PgSessionGUCState *guc = PgCurrentSessionGUCState();

	if (CurrentPgSession == NULL &&
		guc->memory_context == NULL &&
		TopMemoryContext != NULL)
		guc->memory_context = AllocSetContextCreate(TopMemoryContext,
													"early GUC fallback state",
													ALLOCSET_DEFAULT_SIZES);

	return &guc->memory_context;
}

struct config_generic **
PgCurrentGUCVariablesRef(void)
{
	return &PgCurrentSessionGUCState()->variables;
}

int *
PgCurrentNumGUCVariablesRef(void)
{
	return &PgCurrentSessionGUCState()->num_variables;
}

HTAB **
PgCurrentGUCHashTableRef(void)
{
	return &PgCurrentSessionGUCState()->hash_table;
}

dlist_head *
PgCurrentGUCNondefListRef(void)
{
	return &PgCurrentSessionGUCState()->nondef_list;
}

slist_head *
PgCurrentGUCStackListRef(void)
{
	return &PgCurrentSessionGUCState()->stack_list;
}

slist_head *
PgCurrentGUCReportListRef(void)
{
	return &PgCurrentSessionGUCState()->report_list;
}

bool *
PgCurrentGUCReportingEnabledRef(void)
{
	return &PgCurrentSessionGUCState()->reporting_enabled;
}

int *
PgCurrentGUCNestLevelRef(void)
{
	return &PgCurrentSessionGUCState()->nest_level;
}

int *
PgCurrentThreadedGUCMutexDepthRef(void)
{
	return &PgCurrentCarrierState()->threaded_guc_mutex_depth;
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
