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

#define PG_PLAN_ADVICE_RUNTIME_STATE_KEY "pg_plan_advice.runtime"
#define BLOOM_RUNTIME_STATE_KEY "bloom.runtime"
#define RENDEZVOUS_RUNTIME_STATE_KEY "rendezvous.runtime"

typedef struct PgPlanAdviceRuntimeState
{
	MemoryContext context;
	List	   *advisor_hook_list;
} PgPlanAdviceRuntimeState;

typedef struct PgBloomRuntimeState
{
	MemoryContext context;
} PgBloomRuntimeState;

typedef struct PgRendezvousRuntimeState
{
	HTAB	   *hash;
} PgRendezvousRuntimeState;

void
PgRuntimeInitializeExtensionModuleState(PgRuntimeExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	extension_modules->memory_context = NULL;
	extension_modules->private_states = NIL;
}

MemoryContext
PgRuntimeEnsureExtensionModuleMemoryContext(PgRuntimeExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	if (extension_modules->memory_context == NULL)
	{
		if (PgRuntimeIsThreadBacked(CurrentPgRuntime))
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

static PgRuntimeExtensionPrivateState *
PgRuntimeFindExtensionPrivateState(PgRuntimeExtensionModuleState *extension_modules,
								   const char *key)
{
	Assert(extension_modules != NULL);
	Assert(key != NULL);

	foreach_ptr(PgRuntimeExtensionPrivateState, private_state,
				extension_modules->private_states)
	{
		if (strcmp(private_state->key, key) == 0)
			return private_state;
	}

	return NULL;
}

void *
PgRuntimeGetExtensionPrivateState(const char *key)
{
	PgRuntimeExtensionPrivateState *private_state;

	private_state = PgRuntimeFindExtensionPrivateState(
		PgCurrentRuntimeExtensionModuleState(), key);

	return private_state != NULL ? private_state->state : NULL;
}

void *
PgRuntimeEnsureExtensionPrivateState(const char *key, Size size,
									 PgExtensionPrivateStateCleanup cleanup)
{
	PgRuntimeExtensionModuleState *extension_modules;
	PgRuntimeExtensionPrivateState *private_state;
	MemoryContext old_context;

	Assert(key != NULL);
	Assert(size > 0);

	extension_modules = PgCurrentRuntimeExtensionModuleState();
	private_state = PgRuntimeFindExtensionPrivateState(extension_modules, key);
	if (private_state != NULL)
		return private_state->state;

	old_context = MemoryContextSwitchTo(
		PgRuntimeEnsureExtensionModuleMemoryContext(extension_modules));
	private_state = palloc_object(PgRuntimeExtensionPrivateState);
	private_state->key = key;
	private_state->state = palloc0(size);
	private_state->cleanup = cleanup;
	extension_modules->private_states =
		lappend(extension_modules->private_states, private_state);
	MemoryContextSwitchTo(old_context);

	return private_state->state;
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
	PgPlanAdviceRuntimeState *state;

	state = (PgPlanAdviceRuntimeState *)
		PgRuntimeEnsureExtensionPrivateState(PG_PLAN_ADVICE_RUNTIME_STATE_KEY,
											 sizeof(PgPlanAdviceRuntimeState),
											 NULL);
	return &state->context;
}

List **
PgCurrentPgPlanAdviceAdvisorHookListRef(void)
{
	PgPlanAdviceRuntimeState *state;

	state = (PgPlanAdviceRuntimeState *)
		PgRuntimeEnsureExtensionPrivateState(PG_PLAN_ADVICE_RUNTIME_STATE_KEY,
											 sizeof(PgPlanAdviceRuntimeState),
											 NULL);
	return &state->advisor_hook_list;
}

MemoryContext *
PgCurrentBloomContextRef(void)
{
	PgBloomRuntimeState *state;

	state = (PgBloomRuntimeState *)
		PgRuntimeEnsureExtensionPrivateState(BLOOM_RUNTIME_STATE_KEY,
											 sizeof(PgBloomRuntimeState),
											 NULL);
	return &state->context;
}

HTAB **
PgCurrentRendezvousHashRef(void)
{
	PgRendezvousRuntimeState *state;

	state = (PgRendezvousRuntimeState *)
		PgRuntimeEnsureExtensionPrivateState(RENDEZVOUS_RUNTIME_STATE_KEY,
											 sizeof(PgRendezvousRuntimeState),
											 NULL);
	return &state->hash;
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
