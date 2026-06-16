/*-------------------------------------------------------------------------
 *
 * backend_runtime_async.c
 *	  Runtime bridge accessors for LISTEN/NOTIFY async state.
 *
 * These accessors keep async compatibility globals mapped onto the current
 * PgExecution while leaving runtime construction and early fallback ownership
 * in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/commands/backend_runtime_async.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "commands/async.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

HTAB **
PgCurrentAsyncLocalChannelTableRef(void)
{
	return &PgCurrentSessionAsyncState()->local_channel_table;
}

bool *
PgCurrentAsyncRegisteredListenerRef(void)
{
	return &PgCurrentSessionAsyncState()->registered_listener;
}

struct ActionList **
PgCurrentPendingActionsRef(void)
{
	return &PgCurrentExecutionAsyncState()->pending_actions;
}

HTAB **
PgCurrentPendingListenActionsRef(void)
{
	return &PgCurrentExecutionAsyncState()->pending_listen_actions;
}

struct NotificationList **
PgCurrentPendingNotifiesRef(void)
{
	return &PgCurrentExecutionAsyncState()->pending_notifies;
}

PgExecutionAsyncQueuePosition *
PgCurrentQueueHeadBeforeWriteRef(void)
{
	return &PgCurrentExecutionAsyncState()->queue_head_before_write;
}

PgExecutionAsyncQueuePosition *
PgCurrentQueueHeadAfterWriteRef(void)
{
	return &PgCurrentExecutionAsyncState()->queue_head_after_write;
}

MemoryContext
PgCurrentAsyncSignalWorkspaceContext(void)
{
	PgExecutionAsyncState *async = PgCurrentExecutionAsyncState();

	return PgRuntimeGetOwnedMemoryContext(&async->signal_context,
										  "LISTEN/NOTIFY signal workspace");
}

int32 **
PgCurrentSignalPidsRef(void)
{
	return &PgCurrentExecutionAsyncState()->signal_pids;
}

ProcNumber **
PgCurrentSignalProcnosRef(void)
{
	return &PgCurrentExecutionAsyncState()->signal_procnos;
}

bool *
PgCurrentTryAdvanceTailRef(void)
{
	return &PgCurrentExecutionAsyncState()->try_advance_tail;
}
