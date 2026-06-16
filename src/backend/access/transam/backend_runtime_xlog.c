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
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../../utils/init/backend_runtime_internal.h"

void **
PgCurrentXLogInsertRegisteredBuffersRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->registered_buffers;
}

int *
PgCurrentXLogInsertMaxRegisteredBuffersRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->max_registered_buffers;
}

int *
PgCurrentXLogInsertMaxRegisteredBlockIdRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->max_registered_block_id;
}

XLogRecData **
PgCurrentXLogInsertMainRDataHeadRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->mainrdata_head;
}

XLogRecData **
PgCurrentXLogInsertMainRDataLastRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->mainrdata_last;
}

uint64 *
PgCurrentXLogInsertMainRDataLenRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->mainrdata_len;
}

uint8 *
PgCurrentXLogInsertFlagsRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->curinsert_flags;
}

XLogRecData *
PgCurrentXLogInsertHeaderRecordDataRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->hdr_rdt;
}

char **
PgCurrentXLogInsertHeaderScratchRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->hdr_scratch;
}

XLogRecData **
PgCurrentXLogInsertRDatasRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->rdatas;
}

int *
PgCurrentXLogInsertNumRDatasRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->num_rdatas;
}

int *
PgCurrentXLogInsertMaxRDatasRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->max_rdatas;
}

bool *
PgCurrentXLogInsertBeginCalledRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->begininsert_called;
}

MemoryContext *
PgCurrentXLogInsertContextRef(void)
{
	return &PgCurrentExecutionXLogInsertState()->context;
}
