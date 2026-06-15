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
#include "postgres.h"

#include "executor/spi.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

uint64 *
PgCurrentSPIProcessedRef(void)
{
	return &PgCurrentExecutionSPIState()->processed;
}

SPITupleTable **
PgCurrentSPITuptableRef(void)
{
	return &PgCurrentExecutionSPIState()->tuptable;
}

int *
PgCurrentSPIResultRef(void)
{
	return &PgCurrentExecutionSPIState()->result;
}

_SPI_connection **
PgCurrentSPIStackRef(void)
{
	return &PgCurrentExecutionSPIState()->stack;
}

_SPI_connection **
PgCurrentSPICurrentRef(void)
{
	return &PgCurrentExecutionSPIState()->current;
}

int *
PgCurrentSPIStackDepthRef(void)
{
	return &PgCurrentExecutionSPIState()->stack_depth;
}

int *
PgCurrentSPIConnectedRef(void)
{
	return &PgCurrentExecutionSPIState()->connected;
}
