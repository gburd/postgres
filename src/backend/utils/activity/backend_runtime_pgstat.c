/*-------------------------------------------------------------------------
 *
 * backend_runtime_pgstat.c
 *	  Runtime bridge accessors for pgstat-owned backend/session state.
 *
 * These accessors keep legacy pgstat globals mapped onto the current runtime
 * objects while leaving runtime construction and top-level lifecycle
 * orchestration in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/activity/backend_runtime_pgstat.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "pgstat.h"
#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"
#include "../init/backend_runtime_internal.h"

bool *
PgCurrentPgStatTrackCountsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPgStatRuntimeState, PgCurrentSessionPgStatState)->track_counts;
}

int *
PgCurrentPgStatTrackFunctionsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPgStatRuntimeState, PgCurrentSessionPgStatState)->track_functions;
}

int *
PgCurrentPgStatFetchConsistencyRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPgStatRuntimeState, PgCurrentSessionPgStatState)->fetch_consistency;
}

bool *
PgCurrentPgStatTrackActivitiesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPgStatRuntimeState, PgCurrentSessionPgStatState)->track_activities;
}

SessionEndType *
PgCurrentPgStatSessionEndCauseRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPgStatRuntimeState, PgCurrentSessionPgStatState)->session_end_cause;
}

PgStat_Counter *
PgCurrentPgStatLastSessionReportTimeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionPgStatRuntimeState, PgCurrentSessionPgStatState)->last_session_report_time;
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

PgStat_SubXactStatus **
PgCurrentPgStatXactStackRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionTransactionCleanupRuntimeState, PgCurrentExecutionTransactionCleanupState)->pgstat_xact_stack;
}

PgStat_LocalState *
PgCurrentPgStatLocalStateSlow(void)
{
	PgBackendPgStatPendingState *pgstat_pending;

	pgstat_pending =
		PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState,
										PgCurrentBackendPgStatPendingState);
	if (likely(pgstat_pending->local != NULL))
		return pgstat_pending->local;

	Assert(TopMemoryContext != NULL);
	pgstat_pending->local =
		MemoryContextAllocZero(TopMemoryContext, sizeof(PgStat_LocalState));

	return pgstat_pending->local;
}

PgStat_LocalState *
PgCurrentPgStatLocalState(void)
{
	return PgCurrentPgStatLocalStateSlow();
}

PgStat_Snapshot *
PgCurrentPgStatSnapshot(void)
{
	PgStat_LocalState *local = PgCurrentPgStatLocalState();

	if (likely(local->snapshot != NULL))
		return local->snapshot;

	Assert(TopMemoryContext != NULL);
	local->snapshot =
		MemoryContextAllocZero(TopMemoryContext, sizeof(PgStat_Snapshot));

	return local->snapshot;
}

PgStat_Snapshot *
PgCurrentPgStatSnapshotIfAllocated(void)
{
	PgBackendPgStatPendingState *pgstat_pending;

	pgstat_pending = CurrentPgBackendPgStatPendingRuntimeState;
	if (unlikely(pgstat_pending == NULL || pgstat_pending->local == NULL))
		return NULL;

	return pgstat_pending->local->snapshot;
}

static PgBackendPgStatPendingColdState *
PgCurrentPgStatPendingColdState(void)
{
	PgBackendPgStatPendingState *pgstat_pending;

	pgstat_pending =
		PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState,
										PgCurrentBackendPgStatPendingState);
	if (likely(pgstat_pending->cold != NULL))
		return pgstat_pending->cold;

	pgstat_pending->cold = malloc(sizeof(PgBackendPgStatPendingColdState));
	if (unlikely(pgstat_pending->cold == NULL))
		elog(ERROR, "out of memory allocating pgstat pending cold state");
	MemSet(pgstat_pending->cold, 0, sizeof(PgBackendPgStatPendingColdState));

	return pgstat_pending->cold;
}

MemoryContext *
PgCurrentPgStatFixedSnapshotContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->fixed_snapshot_context;
}

PgStat_BgWriterStats *
PgCurrentPendingBgWriterStatsRef(void)
{
	return &PgCurrentPgStatPendingColdState()->pending_bgwriter;
}

PgStat_CheckpointerStats *
PgCurrentPendingCheckpointerStatsRef(void)
{
	return &PgCurrentPgStatPendingColdState()->pending_checkpointer;
}

PgStat_PendingIO *
PgCurrentPendingIOStatsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->io_stats;
}

bool *
PgCurrentHaveIOStatsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->io_stats_pending;
}

PgStat_SLRUStats *
PgCurrentPendingSLRUStatsArray(void)
{
	return PgCurrentPgStatPendingColdState()->slru_stats;
}

bool *
PgCurrentHaveSLRUStatsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->slru_stats_pending;
}

PgStat_PendingLock *
PgCurrentPendingLockStatsRef(void)
{
	return &PgCurrentPgStatPendingColdState()->lock_stats;
}

bool *
PgCurrentHaveLockStatsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->lock_stats_pending;
}

PgStat_BackendPending *
PgCurrentPendingBackendStatsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->backend_stats;
}

bool *
PgCurrentBackendHasIOStatsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->backend_io_stats_pending;
}

#undef PgCurrentBackendHasLockStatsRef
#ifndef PgCurrentBackendHasLockStatsRef
extern bool *PgCurrentBackendHasLockStatsRef(void);
#endif
bool *
PgCurrentBackendHasLockStatsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->backend_lock_stats_pending;
}

MemoryContext *
PgCurrentPgStatPendingContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->pending_context;
}

dlist_head *
PgCurrentPgStatPendingListRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->pending;
}

void **
PgCurrentPgStatEntryRefHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->entry_ref_hash;
}

int *
PgCurrentPgStatSharedRefAgeRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->shared_ref_age;
}

MemoryContext *
PgCurrentPgStatSharedRefContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->shared_ref_context;
}

MemoryContext *
PgCurrentPgStatEntryRefHashContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->entry_ref_hash_context;
}

WalUsage *
PgCurrentPgStatPrevBackendWalUsageRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->backend_wal_prev_usage;
}

bool *
PgCurrentPgStatReportFixedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->report_fixed;
}

bool *
PgCurrentPgStatForceNextFlushRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->force_next_flush;
}

bool *
PgCurrentForceStatsSnapshotClearRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->force_snapshot_clear;
}

bool *
PgCurrentPgStatIsInitializedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->is_initialized;
}

bool *
PgCurrentPgStatIsShutdownRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->is_shutdown;
}

int *
PgCurrentPgStatXactCommitRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->xact_commit;
}

int *
PgCurrentPgStatXactRollbackRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->xact_rollback;
}

PgStat_Counter *
PgCurrentPgStatBlockReadTimeRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->block_read_time;
}

PgStat_Counter *
PgCurrentPgStatBlockWriteTimeRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->block_write_time;
}

PgStat_Counter *
PgCurrentPgStatActiveTimeRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->active_time;
}

PgStat_Counter *
PgCurrentPgStatTransactionIdleTimeRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->transaction_idle_time;
}

instr_time *
PgCurrentPgStatTotalFuncTimeRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->func_total_time;
}

WalUsage *
PgCurrentPgStatPrevWalUsageRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendPgStatPendingRuntimeState, PgCurrentBackendPgStatPendingState)->wal_prev_usage;
}
