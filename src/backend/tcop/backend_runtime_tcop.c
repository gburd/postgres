/*-------------------------------------------------------------------------
 *
 * backend_runtime_tcop.c
 *	  Runtime bridge accessors for top-level command loop state.
 *
 * These accessors keep tcop compatibility globals mapped onto the current
 * PgExecution while leaving runtime construction and early fallback ownership
 * in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/tcop/backend_runtime_tcop.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

unsigned int *
PgCurrentValgrindOldErrorCountRef(void)
{
	return &PgCurrentExecutionValgrindState()->old_error_count;
}
