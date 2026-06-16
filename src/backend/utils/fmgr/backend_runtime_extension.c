/*-------------------------------------------------------------------------
 *
 * backend_runtime_extension.c
 *	  Runtime bridge accessors for extension module state.
 *
 * These accessors keep extension and dynamic-library compatibility globals
 * mapped onto the current runtime/session/execution while leaving root
 * selection and lifecycle orchestration in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/fmgr/backend_runtime_extension.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../init/backend_runtime_internal.h"

MemoryContext
PgCurrentRuntimeExtensionModuleMemoryContext(void)
{
	return PgRuntimeEnsureExtensionModuleMemoryContext(PgCurrentRuntimeExtensionModuleState());
}

MemoryContext *
PgCurrentPgPlanAdviceContextRef(void)
{
	return &PgCurrentRuntimeExtensionModuleState()->pg_plan_advice_context;
}

List **
PgCurrentPgPlanAdviceAdvisorHookListRef(void)
{
	return &PgCurrentRuntimeExtensionModuleState()->pg_plan_advice_advisor_hook_list;
}

MemoryContext *
PgCurrentBloomContextRef(void)
{
	return &PgCurrentRuntimeExtensionModuleState()->bloom_context;
}

HTAB **
PgCurrentRendezvousHashRef(void)
{
	return &PgCurrentRuntimeExtensionModuleState()->rendezvous_hash;
}

PgSessionPgcryptoDesState *
PgCurrentPgcryptoDesState(void)
{
	return &PgCurrentSessionExtensionModuleState()->pgcrypto_des;
}

PgExecutionDebugHandler *
PgCurrentPgcryptoDebugHandlerRef(void)
{
	return &PgCurrentExecutionExtensionState()->pgcrypto_debug_handler;
}

bool *
PgCurrentCreatingExtensionRef(void)
{
	return &PgCurrentExecutionExtensionState()->creating;
}

Oid *
PgCurrentExtensionObjectRef(void)
{
	return &PgCurrentExecutionExtensionState()->current_object;
}
