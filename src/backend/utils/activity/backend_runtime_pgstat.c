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
#include "postgres.h"

#include "pgstat.h"
#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"
#include "../init/backend_runtime_internal.h"

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

PgStat_LocalState *
PgCurrentPgStatLocalState(void)
{
	return &PgCurrentBackendPgStatPendingState()->local;
}

MemoryContext *
PgCurrentPgStatFixedSnapshotContextRef(void)
{
	return &PgCurrentBackendPgStatPendingState()->fixed_snapshot_context;
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
