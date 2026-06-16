/*-------------------------------------------------------------------------
 *
 * backend_runtime_large_object.c
 *	  Runtime bridge accessors for large-object session state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/storage/large_object/backend_runtime_large_object.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../../utils/init/backend_runtime_internal.h"

struct RelationData **
PgCurrentLargeObjectHeapRelationRef(void)
{
	return &PgCurrentSessionLargeObjectState()->heap_relation;
}

struct RelationData **
PgCurrentLargeObjectIndexRelationRef(void)
{
	return &PgCurrentSessionLargeObjectState()->index_relation;
}

LargeObjectDesc ***
PgCurrentLargeObjectCookiesRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->lo_cookies;
}

int *
PgCurrentLargeObjectCookiesSizeRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->lo_cookies_size;
}

bool *
PgCurrentLargeObjectCleanupNeededRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->lo_cleanup_needed;
}

MemoryContext *
PgCurrentLargeObjectContextRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->lo_context;
}
