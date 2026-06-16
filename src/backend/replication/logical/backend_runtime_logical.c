/*-------------------------------------------------------------------------
 *
 * backend_runtime_logical.c
 *	  Runtime bridge accessors for logical-replication execution state.
 *
 * These accessors keep logical-replication compatibility globals mapped onto
 * the current PgExecution while leaving runtime construction and early
 * fallback ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/replication/logical/backend_runtime_logical.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../../utils/init/backend_runtime_internal.h"

ReplOriginXactState *
PgCurrentReplOriginXactStateRef(void)
{
	return &PgCurrentExecutionReplicationScratchState()->replorigin_xact;
}

ErrorContextCallback **
PgCurrentApplyErrorContextStackRef(void)
{
	return &PgCurrentExecutionReplicationScratchState()->apply_error_context_stack;
}

MemoryContext *
PgCurrentApplyMessageContextRef(void)
{
	return &PgCurrentExecutionReplicationScratchState()->apply_message_context;
}

MemoryContext *
PgCurrentLogicalStreamingContextRef(void)
{
	return &PgCurrentExecutionReplicationScratchState()->logical_streaming_context;
}

struct ResourceOwnerData **
PgCurrentSnapBuildSavedResourceOwnerDuringExportRef(void)
{
	return &PgCurrentExecutionSnapBuildState()->saved_resource_owner_during_export;
}

bool *
PgCurrentSnapBuildExportInProgressRef(void)
{
	return &PgCurrentExecutionSnapBuildState()->export_in_progress;
}
