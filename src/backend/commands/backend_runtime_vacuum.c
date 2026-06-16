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
#include "postgres.h"

#include "commands/vacuum.h"
#include "miscadmin.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

int *
PgCurrentVacuumBufferUsageLimitRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_buffer_usage_limit_kb;
}

int *
PgCurrentVacuumCostPageHitRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_cost_page_hit_value;
}

int *
PgCurrentVacuumCostPageMissRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_cost_page_miss_value;
}

int *
PgCurrentVacuumCostPageDirtyRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_cost_page_dirty_value;
}

int *
PgCurrentVacuumCostLimitRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_cost_limit_value;
}

double *
PgCurrentVacuumCostDelayRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_cost_delay_ms;
}

int *
PgCurrentDefaultStatisticsTargetRef(void)
{
	return &PgCurrentSessionVacuumState()->default_statistics_target_value;
}

int *
PgCurrentVacuumFreezeMinAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_freeze_min_age_value;
}

int *
PgCurrentVacuumFreezeTableAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_freeze_table_age_value;
}

int *
PgCurrentVacuumMultixactFreezeMinAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_multixact_freeze_min_age_value;
}

int *
PgCurrentVacuumMultixactFreezeTableAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_multixact_freeze_table_age_value;
}

int *
PgCurrentVacuumFailsafeAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_failsafe_age_value;
}

int *
PgCurrentVacuumMultixactFailsafeAgeRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_multixact_failsafe_age_value;
}

bool *
PgCurrentTrackCostDelayTimingRef(void)
{
	return &PgCurrentSessionVacuumState()->track_cost_delay_timing_value;
}

bool *
PgCurrentVacuumTruncateRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_truncate_value;
}

double *
PgCurrentVacuumMaxEagerFreezeFailureRateRef(void)
{
	return &PgCurrentSessionVacuumState()->vacuum_max_eager_freeze_failure_rate_value;
}

double *
PgCurrentLocalVacuumCostDelayRef(void)
{
	return &PgCurrentSessionVacuumState()->local_vacuum_cost_delay_ms;
}

int *
PgCurrentLocalVacuumCostLimitRef(void)
{
	return &PgCurrentSessionVacuumState()->local_vacuum_cost_limit_value;
}

bool *
PgCurrentVacuumInProgressRef(void)
{
	return &PgCurrentExecutionVacuumState()->in_vacuum;
}

int *
PgCurrentVacuumCostBalanceRef(void)
{
	return &PgCurrentExecutionVacuumState()->cost_balance;
}

bool *
PgCurrentVacuumCostActiveRef(void)
{
	return &PgCurrentExecutionVacuumState()->cost_active;
}

pg_atomic_uint32 **
PgCurrentVacuumSharedCostBalanceRef(void)
{
	return &PgCurrentExecutionVacuumState()->shared_cost_balance;
}

pg_atomic_uint32 **
PgCurrentVacuumActiveNWorkersRef(void)
{
	return &PgCurrentExecutionVacuumState()->active_nworkers;
}

int *
PgCurrentVacuumCostBalanceLocalRef(void)
{
	return &PgCurrentExecutionVacuumState()->cost_balance_local;
}

bool *
PgCurrentVacuumFailsafeActiveRef(void)
{
	return &PgCurrentExecutionVacuumState()->failsafe_active;
}

int64 *
PgCurrentParallelVacuumWorkerDelayNsRef(void)
{
	return &PgCurrentExecutionVacuumState()->parallel_worker_delay_ns;
}

void **
PgCurrentParallelVacuumSharedCostParamsRef(void)
{
	return &PgCurrentExecutionVacuumState()->parallel_shared_cost_params;
}

uint32 *
PgCurrentParallelVacuumSharedParamsGenerationLocalRef(void)
{
	return &PgCurrentExecutionVacuumState()->parallel_shared_params_generation_local;
}

MemoryContext *
PgCurrentAnalyzeContextRef(void)
{
	return &PgCurrentExecutionAnalyzeState()->context;
}

BufferAccessStrategy *
PgCurrentAnalyzeStrategyRef(void)
{
	return &PgCurrentExecutionAnalyzeState()->strategy;
}

void **
PgCurrentArrayAnalyzeExtraDataRef(void)
{
	return &PgCurrentExecutionAnalyzeState()->array_extra_data;
}
