/*-------------------------------------------------------------------------
 *
 * backend_runtime_event_trigger.c
 *	  Runtime bridge accessors for event-trigger execution state.
 *
 * These accessors keep event-trigger compatibility globals mapped onto the
 * current PgExecution while leaving runtime construction and early fallback
 * ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/commands/backend_runtime_event_trigger.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "commands/event_trigger.h"
#include "../utils/init/backend_runtime_internal.h"

EventTriggerQueryState **
PgCurrentEventTriggerQueryStateRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionReplicationScratchRuntimeState, PgCurrentExecutionReplicationScratchState)->event_trigger_query_state;
}

MemoryContext
PgCurrentEventTriggerMemoryContext(void)
{
	PgExecutionReplicationScratchState *replication_scratch;

	replication_scratch = PgCurrentExecutionReplicationScratchState();

	return PgRuntimeGetOwnedMemoryContext(&replication_scratch->event_trigger_context,
										  "event trigger execution state");
}

MemoryContext *
PgCurrentEventTriggerMemoryContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionReplicationScratchRuntimeState, PgCurrentExecutionReplicationScratchState)->event_trigger_context;
}
