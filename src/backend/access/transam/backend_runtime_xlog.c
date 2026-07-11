/*-------------------------------------------------------------------------
 *
 * backend_runtime_xlog.c
 *	  Runtime bridge accessors for XLog-owned execution state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/access/transam/backend_runtime_xlog.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../../utils/init/backend_runtime_internal.h"

void **
PgCurrentXLogInsertRegisteredBuffersRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->registered_buffers;
}

int *
PgCurrentXLogInsertMaxRegisteredBuffersRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->max_registered_buffers;
}

int *
PgCurrentXLogInsertMaxRegisteredBlockIdRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->max_registered_block_id;
}

XLogRecData **
PgCurrentXLogInsertMainRDataHeadRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->mainrdata_head;
}

XLogRecData **
PgCurrentXLogInsertMainRDataLastRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->mainrdata_last;
}

uint64 *
PgCurrentXLogInsertMainRDataLenRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->mainrdata_len;
}

uint8 *
PgCurrentXLogInsertFlagsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->curinsert_flags;
}

XLogRecData *
PgCurrentXLogInsertHeaderRecordDataRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->hdr_rdt;
}

char **
PgCurrentXLogInsertHeaderScratchRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->hdr_scratch;
}

XLogRecData **
PgCurrentXLogInsertRDatasRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->rdatas;
}

int *
PgCurrentXLogInsertNumRDatasRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->num_rdatas;
}

int *
PgCurrentXLogInsertMaxRDatasRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->max_rdatas;
}

bool *
PgCurrentXLogInsertBeginCalledRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->begininsert_called;
}

MemoryContext *
PgCurrentXLogInsertContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->context;
}

uint64 *
PgCurrentGetXLogBufferCachedPageRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->get_xlog_buffer_cached_page;
}

char **
PgCurrentGetXLogBufferCachedPosRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionXLogInsertRuntimeState, PgCurrentExecutionXLogInsertState)->get_xlog_buffer_cached_pos;
}
