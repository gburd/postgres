/*-------------------------------------------------------------------------
 *
 * backend_runtime_parallel.c
 *	  Runtime bridge accessors for backend-local parallel-query state.
 *
 * These accessors keep parallel-query compatibility globals mapped onto the
 * current PgBackend while leaving runtime construction and early fallback
 * ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/access/transam/backend_runtime_parallel.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../../utils/init/backend_runtime_internal.h"

int *
PgCurrentParallelWorkerNumberRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->worker_number;
}

volatile sig_atomic_t *
PgCurrentParallelMessagePendingRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->message_pending;
}

bool *
PgCurrentInitializingParallelWorkerRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->initializing_worker;
}

void **
PgCurrentFixedParallelStateRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->fixed_parallel_state;
}

dlist_head *
PgCurrentParallelContextListRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->context_list;
}

bool *
PgCurrentParallelContextListInitializedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->context_list_initialized;
}

pid_t *
PgCurrentParallelLeaderPidRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->leader_pid;
}

MemoryContext *
PgCurrentParallelMessageContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->message_context;
}

void **
PgCurrentPqMqHandleRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->pq_mq_handle;
}

bool *
PgCurrentPqMqBusyRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->pq_mq_busy;
}

pid_t *
PgCurrentPqMqParallelLeaderPidRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->pq_mq_parallel_leader_pid;
}

ProcNumber *
PgCurrentPqMqParallelLeaderProcNumberRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendParallelRuntimeState, PgCurrentBackendParallelState)->pq_mq_parallel_leader_proc_number;
}
