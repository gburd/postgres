/*-------------------------------------------------------------------------
 *
 * backend_runtime_executor.c
 *	  Runtime bridge accessors for executor-owned execution state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/executor/backend_runtime_executor.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "executor/spi.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgExecutionSPIState *
PgCurrentExecutionSPIState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionSPIRuntimeState,
											   spi);
}

uint64 *
PgCurrentSPIProcessedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSPIRuntimeState, PgCurrentExecutionSPIState)->processed;
}

SPITupleTable **
PgCurrentSPITuptableRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSPIRuntimeState, PgCurrentExecutionSPIState)->tuptable;
}

int *
PgCurrentSPIResultRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSPIRuntimeState, PgCurrentExecutionSPIState)->result;
}

_SPI_connection **
PgCurrentSPIStackRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSPIRuntimeState, PgCurrentExecutionSPIState)->stack;
}

_SPI_connection **
PgCurrentSPICurrentRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSPIRuntimeState, PgCurrentExecutionSPIState)->current;
}

int *
PgCurrentSPIStackDepthRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSPIRuntimeState, PgCurrentExecutionSPIState)->stack_depth;
}

int *
PgCurrentSPIConnectedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSPIRuntimeState, PgCurrentExecutionSPIState)->connected;
}

BufferUsage *
PgCurrentBufferUsageRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendInstrumentationRuntimeState, PgCurrentBackendInstrumentationState)->buffer_usage;
}

BufferUsage *
PgCurrentSavedBufferUsageRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendInstrumentationRuntimeState, PgCurrentBackendInstrumentationState)->saved_buffer_usage;
}

WalUsage *
PgCurrentWalUsageRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendInstrumentationRuntimeState, PgCurrentBackendInstrumentationState)->wal_usage;
}

WalUsage *
PgCurrentSavedWalUsageRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendInstrumentationRuntimeState, PgCurrentBackendInstrumentationState)->saved_wal_usage;
}
