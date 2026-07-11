/*-------------------------------------------------------------------------
 *
 * backend_runtime_memory.c
 *	  Runtime bridge accessors for memory-manager and memory-context state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/mmgr/backend_runtime_memory.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

static MemoryContext *PgCurrentMemoryContextObjectRef(void);
static MemoryContext *PgMessageContextObjectRef(void);

void
PgRuntimeDeleteOwnedMemoryContext(MemoryContext *context)
{
	Assert(context != NULL);

	if (*context == NULL)
		return;

	if (CurrentMemoryContext == *context)
		MemoryContextSwitchTo(TopMemoryContext);
	MemoryContextDelete(*context);
	*context = NULL;
}

PgExecutionMemoryContextState *
PgCurrentExecutionMemoryContexts(void)
{
	PgExecutionMemoryContextState *memory_contexts;

	memory_contexts = CurrentPgExecutionMemoryContextRuntimeState;
	if (likely(memory_contexts != NULL))
		return memory_contexts;

	PG_RUNTIME_BRIDGE_COUNT_FALLBACK(memory_contexts);
	return &PgCurrentOrEarlyExecution()->memory_contexts;
}

PgBackendAllocSetFreeList *
PgCurrentAllocSetContextFreeLists(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendMemoryManagerRuntimeState, PgCurrentBackendMemoryManagerState)->context_freelists;
}

bool *
PgCurrentLogMemoryContextInProgressRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendMemoryManagerRuntimeState, PgCurrentBackendMemoryManagerState)->log_memory_context_in_progress;
}

MemoryContext *
PgTopMemoryContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->top_context;
}

MemoryContext *
PgCurrentMemoryContextRef(void)
{
	return PgCurrentMemoryContextObjectRef();
}

static MemoryContext *
PgCurrentMemoryContextObjectRef(void)
{
	PgExecutionMemoryContextState *memory_contexts =
		PgCurrentExecutionMemoryContexts();

	/*
	 * Bootstrap can reach fallback accessors before the hot current-cell table
	 * has been installed.  Keep the historical invariant established by
	 * MemoryContextInit(): once TopMemoryContext exists, CurrentMemoryContext
	 * must have somewhere valid to point.
	 */
	if (unlikely(memory_contexts->current_context == NULL &&
				 memory_contexts->top_context != NULL))
		memory_contexts->current_context = memory_contexts->top_context;

	return &memory_contexts->current_context;
}

void
PgSetCurrentMemoryContextObject(MemoryContext context)
{
	*PgCurrentMemoryContextObjectRef() = context;
}

MemoryContext *
PgErrorContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->error_context;
}

MemoryContext *
PgMessageContextRef(void)
{
	return PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgMessageContextHotRef,
											CurrentPgExecution,
											PgMessageContextObjectRef);
}

static MemoryContext *
PgMessageContextObjectRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->message_context;
}

MemoryContext *
PgTopTransactionContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->top_transaction_context;
}

MemoryContext *
PgCurTransactionContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->cur_transaction_context;
}

MemoryContext *
PgPortalContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->portal_context;
}
