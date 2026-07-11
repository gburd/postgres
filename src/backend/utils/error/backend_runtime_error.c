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
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "utils/elog.h"
#include "../init/backend_runtime_internal.h"

PgExecutionErrorState *
PgCurrentExecutionErrorState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionErrorRuntimeState,
											   error);
}

ErrorContextCallback **
PgCurrentErrorContextStackRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionErrorRuntimeState, PgCurrentExecutionErrorState)->context_stack;
}

sigjmp_buf **
PgCurrentExceptionStackRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionErrorRuntimeState, PgCurrentExecutionErrorState)->exception_stack;
}

ErrorData *
PgCurrentErrorDataArray(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionErrorRuntimeState, PgCurrentExecutionErrorState)->errordata;
}

int *
PgCurrentErrorDataStackDepthRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionErrorRuntimeState, PgCurrentExecutionErrorState)->errordata_stack_depth;
}

int *
PgCurrentErrorRecursionDepthRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionErrorRuntimeState, PgCurrentExecutionErrorState)->recursion_depth;
}

struct timeval *
PgCurrentSavedTimevalRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionErrorRuntimeState, PgCurrentExecutionErrorState)->saved_timeval;
}

bool *
PgCurrentSavedTimevalSetRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionErrorRuntimeState, PgCurrentExecutionErrorState)->saved_timeval_set;
}

char *
PgCurrentFormattedLogTime(void)
{
	return PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionErrorRuntimeState, PgCurrentExecutionErrorState)->formatted_log_time;
}

char *
PgCurrentFormattedStartTimeBuffer(void)
{
	return PgCurrentBackendLogState()->formatted_start_time;
}

long *
PgCurrentLogLineNumberRef(void)
{
	return &PgCurrentBackendLogState()->line_number;
}

int *
PgCurrentLogLinePidRef(void)
{
	return &PgCurrentBackendLogState()->line_pid;
}

bool *
PgCurrentDebugPrintPlanRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->debug_print_plan_value;
}

bool *
PgCurrentDebugPrintParseRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->debug_print_parse_value;
}

bool *
PgCurrentDebugPrintRawParseRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->debug_print_raw_parse_value;
}

bool *
PgCurrentDebugPrintRewrittenRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->debug_print_rewritten_value;
}

bool *
PgCurrentDebugPrettyPrintRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->debug_pretty_print_value;
}

#ifdef DEBUG_NODE_TESTS_ENABLED
bool *
PgCurrentDebugCopyParsePlanTreesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->debug_copy_parse_plan_trees_value;
}

bool *
PgCurrentDebugWriteReadParsePlanTreesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->debug_write_read_parse_plan_trees_value;
}

bool *
PgCurrentDebugRawExpressionCoverageTestRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->debug_raw_expression_coverage_test_value;
}
#endif

bool *
PgCurrentLogParserStatsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_parser_stats_value;
}

bool *
PgCurrentLogPlannerStatsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_planner_stats_value;
}

bool *
PgCurrentLogExecutorStatsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_executor_stats_value;
}

bool *
PgCurrentLogStatementStatsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_statement_stats_value;
}

bool *
PgCurrentLogBtreeBuildStatsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_btree_build_stats_value;
}

char **
PgCurrentEventSourceRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->event_source_value;
}

bool *
PgCurrentLogDurationRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_duration_value;
}

int *
PgCurrentLogErrorVerbosityRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_error_verbosity_value;
}

int *
PgCurrentLogParameterMaxLengthRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_parameter_max_length_value;
}

int *
PgCurrentLogParameterMaxLengthOnErrorRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_parameter_max_length_on_error_value;
}

int *
PgCurrentLogMinErrorStatementRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_min_error_statement_value;
}

int *
PgCurrentLogMinMessagesArrayRef(void)
{
	return PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_min_messages_values;
}

char **
PgCurrentLogMinMessagesStringRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_min_messages_string_value;
}

int *
PgCurrentClientMinMessagesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->client_min_messages_value;
}

int *
PgCurrentLogMinDurationSampleRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_min_duration_sample_value;
}

int *
PgCurrentLogMinDurationStatementRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_min_duration_statement_value;
}

int *
PgCurrentLogTempFilesRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_temp_files_value;
}

double *
PgCurrentLogStatementSampleRateRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_statement_sample_rate_value;
}

double *
PgCurrentLogXactSampleRateRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->log_xact_sample_rate_value;
}

char **
PgCurrentBacktraceFunctionsRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->backtrace_functions_value;
}

char **
PgCurrentBacktraceFunctionListRef(void)
{
	return &PG_RUNTIME_FAST_INITIALIZED_BUCKET_ACCESSOR(CurrentPgSessionLoggingRuntimeState, PgCurrentSessionLoggingState)->backtrace_function_list_value;
}
