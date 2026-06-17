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
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "access/xact.h"
#include "utils/backend_runtime.h"
#include "../../utils/init/backend_runtime_internal.h"

PgExecutionXLogInsertState *
PgCurrentExecutionXLogInsertState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionXLogInsertRuntimeState,
											   xloginsert);
}

PgExecutionXactState *
PgCurrentExecutionXactState(void)
{
	if (likely(CurrentPgExecutionXactRuntimeState != NULL))
		return CurrentPgExecutionXactRuntimeState;

	return &PgCurrentOrEarlyExecution()->xact;
}

PgExecutionTransactionCleanupState *
PgCurrentExecutionTransactionCleanupState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionTransactionCleanupRuntimeState,
											   transaction_cleanup);
}

PgExecutionTwoPhaseRecordState *
PgCurrentExecutionTwoPhaseRecordState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionTwoPhaseRecordRuntimeState,
											   two_phase_records);
}

int *
PgCurrentDefaultXactIsoLevelRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionXactDefaultRuntimeState, PgCurrentSessionXactDefaultState)->default_xact_iso_level;
}

bool *
PgCurrentDefaultXactReadOnlyRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionXactDefaultRuntimeState, PgCurrentSessionXactDefaultState)->default_xact_read_only;
}

bool *
PgCurrentDefaultXactDeferrableRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionXactDefaultRuntimeState, PgCurrentSessionXactDefaultState)->default_xact_deferrable;
}

int *
PgCurrentSynchronousCommitRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionXactDefaultRuntimeState, PgCurrentSessionXactDefaultState)->synchronous_commit_value;
}

XactCallbackItem **
PgCurrentXactCallbacksRef(void)
{
	return &PgCurrentSessionXactCallbackState()->xact_callbacks;
}

SubXactCallbackItem **
PgCurrentSubXactCallbacksRef(void)
{
	return &PgCurrentSessionXactCallbackState()->subxact_callbacks;
}

MemoryContext
PgCurrentXactCallbackMemoryContext(void)
{
	return PgRuntimeGetOwnedMemoryContext(
		&PgCurrentSessionXactCallbackState()->xact_callback_context,
		"transaction callback session state");
}

int *
PgCurrentXactIsoLevelRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->iso_level;
}

bool *
PgCurrentXactReadOnlyRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->read_only;
}

bool *
PgCurrentXactDeferrableRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->deferrable;
}

bool *
PgCurrentXactIsSampledRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->is_sampled;
}

TransactionId *
PgCurrentCheckXidAliveRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->check_xid_alive;
}

bool *
PgCurrentBSysScanRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->bsysscan_value;
}

int *
PgCurrentMyXactFlagsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->flags;
}

FullTransactionId *
PgCurrentXactTopFullTransactionIdRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->top_full_transaction_id;
}

int *
PgCurrentNParallelCurrentXidsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->n_parallel_current_xids;
}

TransactionId **
PgCurrentParallelCurrentXidsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->parallel_current_xids;
}

int *
PgCurrentNUnreportedXidsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->n_unreported_xids;
}

TransactionId *
PgCurrentUnreportedXids(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->unreported_xids;
}

SubTransactionId *
PgCurrentSubTransactionIdCounterRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->current_sub_transaction_id;
}

CommandId *
PgCurrentCommandIdCounterRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->current_command_id;
}

bool *
PgCurrentCommandIdUsedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->current_command_id_used;
}

TimestampTz *
PgCurrentXactStartTimestampRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->xact_start_timestamp;
}

TimestampTz *
PgCurrentStmtStartTimestampRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->stmt_start_timestamp;
}

TimestampTz *
PgCurrentXactStopTimestampRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->xact_stop_timestamp;
}

char **
PgCurrentPrepareGIDRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->prepare_gid;
}

bool *
PgCurrentForceSyncCommitRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->force_sync_commit;
}

MemoryContext *
PgCurrentTransactionAbortContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->transaction_abort_context;
}

TransactionStateData **
PgCurrentTopTransactionStateDataRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->top_transaction_state_data;
}

TransactionStateData **
PgCurrentTransactionStateRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXactRuntimeState, PgCurrentExecutionXactState)->current_transaction_state;
}

PgExecutionTwoPhaseRecordState *
PgCurrentTwoPhaseRecordStateRef(void)
{
	return PgCurrentExecutionTwoPhaseRecordState();
}

TransactionId *
PgCurrentCachedFetchXidRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->cached_fetch_xid;
}

int *
PgCurrentCachedFetchXidStatusRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->cached_fetch_xid_status;
}

XLogRecPtr *
PgCurrentCachedCommitLSNRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->cached_commit_lsn;
}

void **
PgCurrentTwoPhaseLockedGxactRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->two_phase_locked_gxact;
}

bool *
PgCurrentTwoPhaseExitRegisteredRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->two_phase_exit_registered;
}

FullTransactionId *
PgCurrentTwoPhaseCachedFxidRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->two_phase_cached_fxid;
}

void **
PgCurrentTwoPhaseCachedGxactRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->two_phase_cached_gxact;
}

int *
PgCurrentSlruErrorCauseRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->slru_error_cause;
}

int *
PgCurrentSlruErrnoRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->slru_errno_value;
}

dclist_head *
PgCurrentMultiXactCacheRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->multixact_cache;
}

bool *
PgCurrentMultiXactCacheInitializedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->multixact_cache_initialized;
}

MemoryContext *
PgCurrentMultiXactContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->multixact_context;
}

char **
PgCurrentMultiXactDebugStringRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->multixact_debug_string;
}

TransactionId *
PgCurrentProcArrayCachedXidNotInProgressRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->procarray_cached_xid_not_in_progress;
}

struct GlobalVisState *
PgCurrentGlobalVisSharedRelsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->global_vis_shared_rels;
}

struct GlobalVisState *
PgCurrentGlobalVisCatalogRelsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->global_vis_catalog_rels;
}

struct GlobalVisState *
PgCurrentGlobalVisDataRelsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->global_vis_data_rels;
}

struct GlobalVisState *
PgCurrentGlobalVisTempRelsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->global_vis_temp_rels;
}

TransactionId *
PgCurrentComputeXidHorizonsResultLastXminRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->compute_xid_horizons_result_last_xmin;
}

long *
PgCurrentXidCacheByRecentXminRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->xidcache_by_recent_xmin;
}

long *
PgCurrentXidCacheByKnownXactRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->xidcache_by_known_xact;
}

long *
PgCurrentXidCacheByMyXactRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->xidcache_by_my_xact;
}

long *
PgCurrentXidCacheByLatestXidRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->xidcache_by_latest_xid;
}

long *
PgCurrentXidCacheByMainXidRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->xidcache_by_main_xid;
}

long *
PgCurrentXidCacheByChildXidRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->xidcache_by_child_xid;
}

long *
PgCurrentXidCacheByKnownAssignedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->xidcache_by_known_assigned;
}

long *
PgCurrentXidCacheNoOverflowRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->xidcache_no_overflow;
}

long *
PgCurrentXidCacheSlowAnswerRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendTransactionRuntimeState, PgCurrentBackendTransactionState)->xidcache_slow_answer;
}
