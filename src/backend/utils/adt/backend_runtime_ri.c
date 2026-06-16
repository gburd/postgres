/*-------------------------------------------------------------------------
 *
 * backend_runtime_ri.c
 *	  Runtime bridge accessors for RI trigger execution state.
 *
 * These accessors keep RI trigger compatibility globals mapped onto the
 * current PgExecution while leaving runtime construction and early fallback
 * ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/backend_runtime_ri.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../init/backend_runtime_internal.h"

PgSessionRIGlobalsState *
PgCurrentSessionRIGlobalsState(void)
{
	PgSessionRIGlobalsState *ri_globals;

	ri_globals = &PgCurrentOrEarlySession()->ri_globals;
	if (!ri_globals->debug_discard_caches_initialized)
		PgSessionInitializeRIGlobalsState(ri_globals);

	return ri_globals;
}

HTAB **
PgCurrentRIConstraintCacheRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->constraint_cache;
}

HTAB **
PgCurrentRIQueryCacheRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->query_cache;
}

HTAB **
PgCurrentRICompareCacheRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->compare_cache;
}

dclist_head *
PgCurrentRIConstraintCacheValidListRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->constraint_cache_valid_list;
}

bool *
PgCurrentRIFastPathXactCallbackRegisteredRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->fastpath_xact_callback_registered;
}

int *
PgCurrentDebugDiscardCachesRef(void)
{
	return &PgCurrentSessionRIGlobalsState()->debug_discard_caches_value;
}

HTAB **
PgCurrentRIFastPathCacheRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->ri_fastpath_cache;
}

bool *
PgCurrentRIFastPathCallbackRegisteredRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->ri_fastpath_callback_registered;
}
