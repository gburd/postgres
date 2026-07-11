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
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../init/backend_runtime_internal.h"

PgSessionPortalManagerState *
PgCurrentSessionPortalManagerState(void)
{
	PG_RUNTIME_RETURN_CURRENT_SESSION_BUCKET(CurrentPgSessionPortalManagerRuntimeState,
											 portal_manager);
}

PgExecutionPortalState *
PgCurrentExecutionPortalState(void)
{
	if (likely(CurrentPgExecutionPortalRuntimeState != NULL))
		return CurrentPgExecutionPortalRuntimeState;

	return &PgCurrentOrEarlyExecution()->portal;
}

Portal *
PgCurrentActivePortalRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionPortalRuntimeState, PgCurrentExecutionPortalState)->active;
}

MemoryContext *
PgCurrentTopPortalContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionPortalManagerRuntimeState, PgCurrentSessionPortalManagerState)->top_portal_context;
}

HTAB **
PgCurrentPortalHashTableRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionPortalManagerRuntimeState, PgCurrentSessionPortalManagerState)->portal_hash_table;
}

unsigned int *
PgCurrentUnnamedPortalCountRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionPortalManagerRuntimeState, PgCurrentSessionPortalManagerState)->unnamed_portal_count;
}
