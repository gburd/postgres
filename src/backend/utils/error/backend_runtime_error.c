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

bool *
PgCurrentDebugPrintPlanRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_print_plan_value;
}

bool *
PgCurrentDebugPrintParseRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_print_parse_value;
}

bool *
PgCurrentDebugPrintRawParseRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_print_raw_parse_value;
}

bool *
PgCurrentDebugPrintRewrittenRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_print_rewritten_value;
}

bool *
PgCurrentDebugPrettyPrintRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_pretty_print_value;
}

#ifdef DEBUG_NODE_TESTS_ENABLED
bool *
PgCurrentDebugCopyParsePlanTreesRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_copy_parse_plan_trees_value;
}

bool *
PgCurrentDebugWriteReadParsePlanTreesRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_write_read_parse_plan_trees_value;
}

bool *
PgCurrentDebugRawExpressionCoverageTestRef(void)
{
	return &PgCurrentSessionLoggingState()->debug_raw_expression_coverage_test_value;
}
#endif

bool *
PgCurrentLogParserStatsRef(void)
{
	return &PgCurrentSessionLoggingState()->log_parser_stats_value;
}

bool *
PgCurrentLogPlannerStatsRef(void)
{
	return &PgCurrentSessionLoggingState()->log_planner_stats_value;
}

bool *
PgCurrentLogExecutorStatsRef(void)
{
	return &PgCurrentSessionLoggingState()->log_executor_stats_value;
}

bool *
PgCurrentLogStatementStatsRef(void)
{
	return &PgCurrentSessionLoggingState()->log_statement_stats_value;
}

bool *
PgCurrentLogBtreeBuildStatsRef(void)
{
	return &PgCurrentSessionLoggingState()->log_btree_build_stats_value;
}

char **
PgCurrentEventSourceRef(void)
{
	return &PgCurrentSessionLoggingState()->event_source_value;
}

bool *
PgCurrentLogDurationRef(void)
{
	return &PgCurrentSessionLoggingState()->log_duration_value;
}

int *
PgCurrentLogErrorVerbosityRef(void)
{
	return &PgCurrentSessionLoggingState()->log_error_verbosity_value;
}

int *
PgCurrentLogParameterMaxLengthRef(void)
{
	return &PgCurrentSessionLoggingState()->log_parameter_max_length_value;
}

int *
PgCurrentLogParameterMaxLengthOnErrorRef(void)
{
	return &PgCurrentSessionLoggingState()->log_parameter_max_length_on_error_value;
}

int *
PgCurrentLogMinErrorStatementRef(void)
{
	return &PgCurrentSessionLoggingState()->log_min_error_statement_value;
}

int *
PgCurrentLogMinMessagesArrayRef(void)
{
	return PgCurrentSessionLoggingState()->log_min_messages_values;
}

char **
PgCurrentLogMinMessagesStringRef(void)
{
	return &PgCurrentSessionLoggingState()->log_min_messages_string_value;
}

int *
PgCurrentClientMinMessagesRef(void)
{
	return &PgCurrentSessionLoggingState()->client_min_messages_value;
}

int *
PgCurrentLogMinDurationSampleRef(void)
{
	return &PgCurrentSessionLoggingState()->log_min_duration_sample_value;
}

int *
PgCurrentLogMinDurationStatementRef(void)
{
	return &PgCurrentSessionLoggingState()->log_min_duration_statement_value;
}

int *
PgCurrentLogTempFilesRef(void)
{
	return &PgCurrentSessionLoggingState()->log_temp_files_value;
}

double *
PgCurrentLogStatementSampleRateRef(void)
{
	return &PgCurrentSessionLoggingState()->log_statement_sample_rate_value;
}

double *
PgCurrentLogXactSampleRateRef(void)
{
	return &PgCurrentSessionLoggingState()->log_xact_sample_rate_value;
}

char **
PgCurrentBacktraceFunctionsRef(void)
{
	return &PgCurrentSessionLoggingState()->backtrace_functions_value;
}

char **
PgCurrentBacktraceFunctionListRef(void)
{
	return &PgCurrentSessionLoggingState()->backtrace_function_list_value;
}
