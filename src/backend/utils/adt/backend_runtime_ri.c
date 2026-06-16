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
