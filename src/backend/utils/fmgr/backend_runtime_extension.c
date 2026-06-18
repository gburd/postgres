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
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

static PG_GLOBAL_RUNTIME PgRuntimeExtensionModuleState early_runtime_extension_modules;

void
PgRuntimeInitializeExtensionModuleState(PgRuntimeExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	extension_modules->memory_context = NULL;
	extension_modules->pg_plan_advice_context = NULL;
	extension_modules->pg_plan_advice_advisor_hook_list = NIL;
	extension_modules->bloom_context = NULL;
	extension_modules->rendezvous_hash = NULL;
}

MemoryContext
PgRuntimeEnsureExtensionModuleMemoryContext(PgRuntimeExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	if (extension_modules->memory_context == NULL)
	{
		if (CurrentPgRuntime != NULL &&
			CurrentPgRuntime->kind == PG_RUNTIME_THREAD_PER_SESSION)
			elog(ERROR,
				 "thread runtime extension module memory context is not initialized");

		extension_modules->memory_context =
			AllocSetContextCreate(TopMemoryContext,
								  "RuntimeExtensionModules",
								  ALLOCSET_DEFAULT_SIZES);
	}

	return extension_modules->memory_context;
}

void
PgRuntimeAdoptEarlyExtensionModuleState(PgRuntime *runtime)
{
	Assert(runtime != NULL);

	runtime->extension_modules = early_runtime_extension_modules;
	PgRuntimeInitializeExtensionModuleState(&early_runtime_extension_modules);
}

PgRuntimeExtensionModuleState *
PgCurrentRuntimeExtensionModuleState(void)
{
	if (CurrentPgRuntime == NULL)
		return &early_runtime_extension_modules;

	return &CurrentPgRuntime->extension_modules;
}

PgExecutionExtensionState *
PgCurrentExecutionExtensionState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionExtensionRuntimeState,
											   extension);
}

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
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionExtensionRuntimeState, PgCurrentExecutionExtensionState)->pgcrypto_debug_handler;
}

bool *
PgCurrentCreatingExtensionRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionExtensionRuntimeState, PgCurrentExecutionExtensionState)->creating;
}

Oid *
PgCurrentExtensionObjectRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionExtensionRuntimeState, PgCurrentExecutionExtensionState)->current_object;
}
