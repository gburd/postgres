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
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgExecutionDebugState *
PgCurrentExecutionDebugState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionDebugRuntimeState,
											   debug);
}

PgExecutionValgrindState *
PgCurrentExecutionValgrindState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionValgrindRuntimeState,
											   valgrind);
}

const char **
PgCurrentDebugQueryStringRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionDebugRuntimeState, PgCurrentExecutionDebugState)->debug_query_string;
}

bool *
PgCurrentDoingCommandReadRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgSessionLoopRuntimeState, PgCurrentSessionLoopState)->doing_command_read;
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

const char **
PgCurrentUserDOptionRef(void)
{
	return &PgCurrentBackendCommandState()->user_d_option;
}

struct rusage *
PgCurrentUsageSaveRusageRef(void)
{
	return &PgCurrentBackendCommandState()->save_rusage;
}

struct timeval *
PgCurrentUsageSaveTimevalRef(void)
{
	return &PgCurrentBackendCommandState()->save_timeval;
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
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionValgrindRuntimeState, PgCurrentExecutionValgrindState)->old_error_count;
}
