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

bool *
PgCurrentDoingCommandReadRef(void)
{
	return &PgCurrentSessionLoopState()->doing_command_read;
}

CachedPlanSource **
PgCurrentUnnamedStmtPsrcRef(void)
{
	return &PgCurrentSessionTcopState()->unnamed_stmt_psrc;
}

bool *
PgCurrentEchoQueryRef(void)
{
	return &PgCurrentSessionTcopState()->echo_query;
}

bool *
PgCurrentUseSemiNewlineNewlineRef(void)
{
	return &PgCurrentSessionTcopState()->use_semi_newline_newline;
}

MemoryContext *
PgCurrentRowDescriptionContextRef(void)
{
	return &PgCurrentSessionTcopState()->row_description_context;
}

StringInfoData *
PgCurrentRowDescriptionBufRef(void)
{
	return &PgCurrentSessionTcopState()->row_description_buf;
}

unsigned int *
PgCurrentValgrindOldErrorCountRef(void)
{
	return &PgCurrentExecutionValgrindState()->old_error_count;
}
