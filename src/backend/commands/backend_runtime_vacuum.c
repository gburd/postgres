/*-------------------------------------------------------------------------
 *
 * backend_runtime_vacuum.c
 *	  Runtime bridge accessors for vacuum and analyze state.
 *
 * These accessors keep vacuum/analyze compatibility globals mapped onto the
 * current runtime objects while leaving runtime construction and early
 * fallback ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/commands/backend_runtime_vacuum.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "commands/vacuum.h"
#include "miscadmin.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgSessionVacuumState *
PgCurrentSessionVacuumState(void)
{
	PgSessionVacuumState *vacuum;

	if (likely(CurrentPgSessionVacuumRuntimeState != NULL &&
			   CurrentPgSessionVacuumRuntimeState->initialized))
		return CurrentPgSessionVacuumRuntimeState;

	vacuum = &PgCurrentOrEarlySession()->vacuum;
	if (!vacuum->initialized)
		PgSessionInitializeVacuumState(vacuum);

	return vacuum;
}

PgExecutionVacuumState *
PgCurrentExecutionVacuumState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionVacuumRuntimeState,
											   vacuum);
}

PgExecutionAnalyzeState *
PgCurrentExecutionAnalyzeState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionAnalyzeRuntimeState,
											   analyze);
}

int *
PgCurrentVacuumBufferUsageLimitRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_buffer_usage_limit_kb;
}

int *
PgCurrentVacuumCostPageHitRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_cost_page_hit_value;
}

int *
PgCurrentVacuumCostPageMissRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_cost_page_miss_value;
}

int *
PgCurrentVacuumCostPageDirtyRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_cost_page_dirty_value;
}

int *
PgCurrentVacuumCostLimitRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_cost_limit_value;
}

double *
PgCurrentVacuumCostDelayRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_cost_delay_ms;
}

int *
PgCurrentDefaultStatisticsTargetRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->default_statistics_target_value;
}

int *
PgCurrentVacuumFreezeMinAgeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_freeze_min_age_value;
}

int *
PgCurrentVacuumFreezeTableAgeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_freeze_table_age_value;
}

int *
PgCurrentVacuumMultixactFreezeMinAgeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_multixact_freeze_min_age_value;
}

int *
PgCurrentVacuumMultixactFreezeTableAgeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_multixact_freeze_table_age_value;
}

int *
PgCurrentVacuumFailsafeAgeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_failsafe_age_value;
}

int *
PgCurrentVacuumMultixactFailsafeAgeRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_multixact_failsafe_age_value;
}

bool *
PgCurrentTrackCostDelayTimingRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->track_cost_delay_timing_value;
}

bool *
PgCurrentVacuumTruncateRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_truncate_value;
}

double *
PgCurrentVacuumMaxEagerFreezeFailureRateRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->vacuum_max_eager_freeze_failure_rate_value;
}

double *
PgCurrentLocalVacuumCostDelayRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->local_vacuum_cost_delay_ms;
}

int *
PgCurrentLocalVacuumCostLimitRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionVacuumRuntimeState, PgCurrentSessionVacuumState)->local_vacuum_cost_limit_value;
}

bool *
PgCurrentVacuumInProgressRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionVacuumRuntimeState, PgCurrentExecutionVacuumState)->in_vacuum;
}

int *
PgCurrentVacuumCostBalanceRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionVacuumRuntimeState, PgCurrentExecutionVacuumState)->cost_balance;
}

bool *
PgCurrentVacuumCostActiveRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionVacuumRuntimeState, PgCurrentExecutionVacuumState)->cost_active;
}

pg_atomic_uint32 **
PgCurrentVacuumSharedCostBalanceRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionVacuumRuntimeState, PgCurrentExecutionVacuumState)->shared_cost_balance;
}

pg_atomic_uint32 **
PgCurrentVacuumActiveNWorkersRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionVacuumRuntimeState, PgCurrentExecutionVacuumState)->active_nworkers;
}

int *
PgCurrentVacuumCostBalanceLocalRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionVacuumRuntimeState, PgCurrentExecutionVacuumState)->cost_balance_local;
}

bool *
PgCurrentVacuumFailsafeActiveRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionVacuumRuntimeState, PgCurrentExecutionVacuumState)->failsafe_active;
}

int64 *
PgCurrentParallelVacuumWorkerDelayNsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionVacuumRuntimeState, PgCurrentExecutionVacuumState)->parallel_worker_delay_ns;
}

void **
PgCurrentParallelVacuumSharedCostParamsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionVacuumRuntimeState, PgCurrentExecutionVacuumState)->parallel_shared_cost_params;
}

uint32 *
PgCurrentParallelVacuumSharedParamsGenerationLocalRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionVacuumRuntimeState, PgCurrentExecutionVacuumState)->parallel_shared_params_generation_local;
}

MemoryContext *
PgCurrentAnalyzeContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAnalyzeRuntimeState, PgCurrentExecutionAnalyzeState)->context;
}

BufferAccessStrategy *
PgCurrentAnalyzeStrategyRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAnalyzeRuntimeState, PgCurrentExecutionAnalyzeState)->strategy;
}

void **
PgCurrentArrayAnalyzeExtraDataRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAnalyzeRuntimeState, PgCurrentExecutionAnalyzeState)->array_extra_data;
}
