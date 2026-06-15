/*-------------------------------------------------------------------------
 *
 * backend_runtime_xact.c
 *	  Runtime bridge accessors for transaction-owned state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/access/transam/backend_runtime_xact.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../../utils/init/backend_runtime_internal.h"

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
