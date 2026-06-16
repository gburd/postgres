/*-------------------------------------------------------------------------
 *
 * backend_runtime_portal.c
 *	  Runtime bridge accessors for portal memory manager state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/mmgr/backend_runtime_portal.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../init/backend_runtime_internal.h"

Portal *
PgCurrentActivePortalRef(void)
{
	return &PgCurrentExecutionPortalState()->active;
}

MemoryContext *
PgCurrentTopPortalContextRef(void)
{
	return &PgCurrentSessionPortalManagerState()->top_portal_context;
}

HTAB **
PgCurrentPortalHashTableRef(void)
{
	return &PgCurrentSessionPortalManagerState()->portal_hash_table;
}

unsigned int *
PgCurrentUnnamedPortalCountRef(void)
{
	return &PgCurrentSessionPortalManagerState()->unnamed_portal_count;
}
