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
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "commands/async.h"
#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgExecutionAsyncState *
PgCurrentExecutionAsyncState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionAsyncRuntimeState,
											   async);
}

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
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAsyncRuntimeState, PgCurrentExecutionAsyncState)->pending_actions;
}

HTAB **
PgCurrentPendingListenActionsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAsyncRuntimeState, PgCurrentExecutionAsyncState)->pending_listen_actions;
}

struct NotificationList **
PgCurrentPendingNotifiesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAsyncRuntimeState, PgCurrentExecutionAsyncState)->pending_notifies;
}

PgExecutionAsyncQueuePosition *
PgCurrentQueueHeadBeforeWriteRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAsyncRuntimeState, PgCurrentExecutionAsyncState)->queue_head_before_write;
}

PgExecutionAsyncQueuePosition *
PgCurrentQueueHeadAfterWriteRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAsyncRuntimeState, PgCurrentExecutionAsyncState)->queue_head_after_write;
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
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAsyncRuntimeState, PgCurrentExecutionAsyncState)->signal_pids;
}

ProcNumber **
PgCurrentSignalProcnosRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAsyncRuntimeState, PgCurrentExecutionAsyncState)->signal_procnos;
}

bool *
PgCurrentTryAdvanceTailRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionAsyncRuntimeState, PgCurrentExecutionAsyncState)->try_advance_tail;
}
