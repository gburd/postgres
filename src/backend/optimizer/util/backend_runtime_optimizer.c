/*-------------------------------------------------------------------------
 *
 * backend_runtime_optimizer.c
 *	  Runtime bridge accessors for optimizer-owned session state.
 *
 * These accessors keep optimizer compatibility globals mapped onto the
 * current PgSession while leaving runtime construction and early fallback
 * ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/optimizer/util/backend_runtime_optimizer.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../../utils/init/backend_runtime_internal.h"

const char ***
PgCurrentPlannerExtensionNameArrayRef(void)
{
	return &PgCurrentSessionOptimizerState()->planner_extension_names;
}

int *
PgCurrentPlannerExtensionNamesAssignedRef(void)
{
	return &PgCurrentSessionOptimizerState()->planner_extension_names_assigned;
}

int *
PgCurrentPlannerExtensionNamesAllocatedRef(void)
{
	return &PgCurrentSessionOptimizerState()->planner_extension_names_allocated;
}

HTAB **
PgCurrentOprProofCacheHashRef(void)
{
	return &PgCurrentSessionOptimizerState()->opr_proof_cache_hash;
}
