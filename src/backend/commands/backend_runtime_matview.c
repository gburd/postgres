/*-------------------------------------------------------------------------
 *
 * backend_runtime_matview.c
 *	  Runtime bridge accessors for materialized-view execution state.
 *
 * These accessors keep materialized-view compatibility globals mapped onto
 * the current PgExecution while leaving runtime construction and early
 * fallback ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/commands/backend_runtime_matview.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/matview.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgExecutionMatViewState *
PgCurrentExecutionMatViewState(void)
{
	return &PgCurrentOrEarlyExecution()->matview;
}

int *
PgCurrentMatViewMaintenanceDepthRef(void)
{
	return &PgCurrentExecutionMatViewState()->maintenance_depth;
}
