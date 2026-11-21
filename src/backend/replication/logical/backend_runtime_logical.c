/*-------------------------------------------------------------------------
 *
 * backend_runtime_logical.c
 *	  Runtime bridge accessors for logical-replication state.
 *
 * These accessors map logical-replication compatibility globals onto their
 * current runtime owners; utils/init owns construction and early fallback.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/replication/logical/backend_runtime_logical.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../../utils/init/backend_runtime_internal.h"

PgExecutionReplicationScratchState *
PgCurrentExecutionReplicationScratchState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionReplicationScratchRuntimeState,
											   replication_scratch);
}

PgExecutionSnapBuildState *
PgCurrentExecutionSnapBuildState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionSnapBuildRuntimeState,
											   snapbuild);
}

bool *
PgCurrentReplicationOriginCleanupRegisteredRef(void)
{
	return &PgCurrentLogicalReplicationState()->origin_cleanup_registered;
}

ReplOriginXactState *
PgCurrentReplOriginXactStateRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionReplicationScratchRuntimeState, PgCurrentExecutionReplicationScratchState)->replorigin_xact;
}

ErrorContextCallback **
PgCurrentApplyErrorContextStackRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionReplicationScratchRuntimeState, PgCurrentExecutionReplicationScratchState)->apply_error_context_stack;
}

MemoryContext *
PgCurrentApplyMessageContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionReplicationScratchRuntimeState, PgCurrentExecutionReplicationScratchState)->apply_message_context;
}

MemoryContext *
PgCurrentLogicalStreamingContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionReplicationScratchRuntimeState, PgCurrentExecutionReplicationScratchState)->logical_streaming_context;
}

struct ResourceOwnerData **
PgCurrentSnapBuildSavedResourceOwnerDuringExportRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapBuildRuntimeState, PgCurrentExecutionSnapBuildState)->saved_resource_owner_during_export;
}

bool *
PgCurrentSnapBuildExportInProgressRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionSnapBuildRuntimeState, PgCurrentExecutionSnapBuildState)->export_in_progress;
}
