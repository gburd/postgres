/*-------------------------------------------------------------------------
 *
 * backend_runtime_error.c
 *	  Runtime bridge accessors for error-reporting execution state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/error/backend_runtime_error.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "utils/elog.h"
#include "../init/backend_runtime_internal.h"

ErrorContextCallback **
PgCurrentErrorContextStackRef(void)
{
	return &PgCurrentExecutionErrorState()->context_stack;
}

sigjmp_buf **
PgCurrentExceptionStackRef(void)
{
	return &PgCurrentExecutionErrorState()->exception_stack;
}

ErrorData *
PgCurrentErrorDataArray(void)
{
	return PgCurrentExecutionErrorState()->errordata;
}

int *
PgCurrentErrorDataStackDepthRef(void)
{
	return &PgCurrentExecutionErrorState()->errordata_stack_depth;
}

int *
PgCurrentErrorRecursionDepthRef(void)
{
	return &PgCurrentExecutionErrorState()->recursion_depth;
}

struct timeval *
PgCurrentSavedTimevalRef(void)
{
	return &PgCurrentExecutionErrorState()->saved_timeval;
}

bool *
PgCurrentSavedTimevalSetRef(void)
{
	return &PgCurrentExecutionErrorState()->saved_timeval_set;
}

char *
PgCurrentFormattedLogTime(void)
{
	return PgCurrentExecutionErrorState()->formatted_log_time;
}
