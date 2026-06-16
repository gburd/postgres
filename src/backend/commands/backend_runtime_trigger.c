/*-------------------------------------------------------------------------
 *
 * backend_runtime_trigger.c
 *	  Runtime bridge accessors for trigger execution state.
 *
 * These accessors keep trigger compatibility globals mapped onto the current
 * PgExecution while leaving runtime construction and early fallback ownership
 * in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/commands/backend_runtime_trigger.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/trigger.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgExecutionTriggerState *
PgCurrentExecutionTriggerState(void)
{
	return &PgCurrentOrEarlyExecution()->trigger;
}

int *
PgCurrentTriggerDepthRef(void)
{
	return &PgCurrentExecutionTriggerState()->depth;
}

void **
PgCurrentAfterTriggersDataRef(void)
{
	return &PgCurrentExecutionTriggerState()->after_triggers_data;
}

MemoryContext
PgCurrentAfterTriggersMemoryContext(void)
{
	PgExecutionTriggerState *trigger;

	trigger = PgCurrentExecutionTriggerState();

	return PgRuntimeGetOwnedMemoryContext(&trigger->after_triggers_context,
										  "after trigger execution state");
}

MemoryContext *
PgCurrentAfterTriggersMemoryContextRef(void)
{
	return &PgCurrentExecutionTriggerState()->after_triggers_context;
}
