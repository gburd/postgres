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
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "../init/backend_runtime_internal.h"

PgExecutionMemoryContextState *
PgCurrentExecutionMemoryContexts(void)
{
	return &PgCurrentOrEarlyExecution()->memory_contexts;
}

PgBackendAllocSetFreeList *
PgCurrentAllocSetContextFreeLists(void)
{
	return PgCurrentBackendMemoryManagerState()->context_freelists;
}

bool *
PgCurrentLogMemoryContextInProgressRef(void)
{
	return &PgCurrentBackendMemoryManagerState()->log_memory_context_in_progress;
}

MemoryContext *
PgTopMemoryContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->top_context;
}

MemoryContext *
PgCurrentMemoryContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->current_context;
}

MemoryContext *
PgErrorContextRef(void)
{
	return &PgCurrentExecutionMemoryContexts()->error_context;
}

MemoryContext *
PgMessageContextRef(void)
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
