/*-------------------------------------------------------------------------
 *
 * auto_explain.c
 *
 *
 * Copyright (c) 2008-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/auto_explain/auto_explain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "access/parallel.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "commands/explain_state.h"
#include "common/pg_prng.h"
#include "executor/instrument.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "parser/scansup.h"
#include "utils/backend_runtime.h"
#include "utils/guc.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC_EXT(
					.name = "auto_explain",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE
);

/*
 * Parsed form of one option from auto_explain.log_extension_options.
 */
typedef struct auto_explain_option
{
	char	   *name;
	char	   *value;
	NodeTag		type;
} auto_explain_option;

/*
 * Parsed form of the entirety of auto_explain.log_extension_options, stored
 * as GUC extra. The options[] array will have pointers into the string
 * following the array.
 */
typedef struct auto_explain_extension_options
{
	int			noptions;
	auto_explain_option options[FLEXIBLE_ARRAY_MEMBER];
	/* a null-terminated copy of the GUC string follows the array */
} auto_explain_extension_options;

#define AUTO_EXPLAIN_RUNTIME_STATE_KEY "auto_explain.runtime"
#define AUTO_EXPLAIN_SESSION_STATE_KEY "auto_explain.session"
#define AUTO_EXPLAIN_EXECUTION_STATE_KEY "auto_explain.execution"

typedef struct AutoExplainRuntimeState
{
	ExecutorStart_hook_type prev_ExecutorStart;
	ExecutorRun_hook_type prev_ExecutorRun;
	ExecutorFinish_hook_type prev_ExecutorFinish;
	ExecutorEnd_hook_type prev_ExecutorEnd;
} AutoExplainRuntimeState;

typedef struct AutoExplainSessionState
{
	bool		initialized;
	int			log_min_duration_value;
	int			log_parameter_max_length_value;
	bool		log_analyze_value;
	bool		log_verbose_value;
	bool		log_buffers_value;
	bool		log_io_value;
	bool		log_wal_value;
	bool		log_triggers_value;
	bool		log_timing_value;
	bool		log_settings_value;
	int			log_format_value;
	int			log_level_value;
	bool		log_nested_statements_value;
	double		sample_rate_value;
	char	   *log_extension_options_value;
	auto_explain_extension_options *extension_options;
} AutoExplainSessionState;

typedef struct AutoExplainExecutionState
{
	int			nesting_level;
	bool		current_query_sampled;
} AutoExplainExecutionState;

static AutoExplainRuntimeState *
auto_explain_runtime_state(void)
{
	return (AutoExplainRuntimeState *)
		PgRuntimeEnsureExtensionPrivateState(AUTO_EXPLAIN_RUNTIME_STATE_KEY,
											 sizeof(AutoExplainRuntimeState),
											 NULL);
}

static AutoExplainSessionState *
auto_explain_session_state(void)
{
	AutoExplainSessionState *state;

	state = (AutoExplainSessionState *)
		PgSessionEnsureExtensionPrivateState(AUTO_EXPLAIN_SESSION_STATE_KEY,
											 sizeof(AutoExplainSessionState),
											 NULL);
	if (!state->initialized)
	{
		state->log_min_duration_value = -1;
		state->log_parameter_max_length_value = -1;
		state->log_timing_value = true;
		state->log_format_value = EXPLAIN_FORMAT_TEXT;
		state->log_level_value = LOG;
		state->sample_rate_value = 1.0;
		state->initialized = true;
	}

	return state;
}

static AutoExplainExecutionState *
auto_explain_execution_state(void)
{
	return (AutoExplainExecutionState *)
		PgExecutionEnsureExtensionPrivateState(AUTO_EXPLAIN_EXECUTION_STATE_KEY,
											   sizeof(AutoExplainExecutionState),
											   NULL);
}

#define auto_explain_log_min_duration \
	(auto_explain_session_state()->log_min_duration_value)
#define auto_explain_log_parameter_max_length \
	(auto_explain_session_state()->log_parameter_max_length_value)
#define auto_explain_log_analyze \
	(auto_explain_session_state()->log_analyze_value)
#define auto_explain_log_verbose \
	(auto_explain_session_state()->log_verbose_value)
#define auto_explain_log_buffers \
	(auto_explain_session_state()->log_buffers_value)
#define auto_explain_log_io \
	(auto_explain_session_state()->log_io_value)
#define auto_explain_log_wal \
	(auto_explain_session_state()->log_wal_value)
#define auto_explain_log_triggers \
	(auto_explain_session_state()->log_triggers_value)
#define auto_explain_log_timing \
	(auto_explain_session_state()->log_timing_value)
#define auto_explain_log_settings \
	(auto_explain_session_state()->log_settings_value)
#define auto_explain_log_format \
	(auto_explain_session_state()->log_format_value)
#define auto_explain_log_level \
	(auto_explain_session_state()->log_level_value)
#define auto_explain_log_nested_statements \
	(auto_explain_session_state()->log_nested_statements_value)
#define auto_explain_sample_rate \
	(auto_explain_session_state()->sample_rate_value)
#define auto_explain_log_extension_options \
	(auto_explain_session_state()->log_extension_options_value)
#define nesting_level \
	(auto_explain_execution_state()->nesting_level)
#define current_query_sampled \
	(auto_explain_execution_state()->current_query_sampled)

#define prev_ExecutorStart \
	(auto_explain_runtime_state()->prev_ExecutorStart)
#define prev_ExecutorRun \
	(auto_explain_runtime_state()->prev_ExecutorRun)
#define prev_ExecutorFinish \
	(auto_explain_runtime_state()->prev_ExecutorFinish)
#define prev_ExecutorEnd \
	(auto_explain_runtime_state()->prev_ExecutorEnd)

static auto_explain_extension_options *
current_auto_explain_extension_options(void)
{
	return auto_explain_session_state()->extension_options;
}

static const struct config_enum_entry format_options[] = {
	{"text", EXPLAIN_FORMAT_TEXT, false},
	{"xml", EXPLAIN_FORMAT_XML, false},
	{"json", EXPLAIN_FORMAT_JSON, false},
	{"yaml", EXPLAIN_FORMAT_YAML, false},
	{NULL, 0, false}
};

static const struct config_enum_entry loglevel_options[] = {
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"debug", DEBUG2, true},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"log", LOG, false},
	{NULL, 0, false}
};

#define auto_explain_enabled() \
	(auto_explain_log_min_duration >= 0 && \
	 (nesting_level == 0 || auto_explain_log_nested_statements) && \
	 current_query_sampled)

static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc,
								ScanDirection direction,
								uint64 count);
static void explain_ExecutorFinish(QueryDesc *queryDesc);
static void explain_ExecutorEnd(QueryDesc *queryDesc);

static bool check_log_extension_options(char **newval, void **extra,
										GucSource source);
static void assign_log_extension_options(const char *newval, void *extra);
static void apply_extension_options(ExplainState *es,
									auto_explain_extension_options *ext);
static char *auto_explain_scan_literal(char **endp, char **nextp);
static int	auto_explain_split_options(char *rawstring,
									   auto_explain_option *options,
									   int maxoptions, char **errmsg);

/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variables. */
	DefineCustomIntVariable("auto_explain.log_min_duration",
							"Sets the minimum execution time above which plans will be logged.",
							"-1 disables logging plans. 0 means log all plans.",
							&auto_explain_log_min_duration,
							-1,
							-1, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("auto_explain.log_parameter_max_length",
							"Sets the maximum length of query parameter values to log.",
							"-1 means log values in full.",
							&auto_explain_log_parameter_max_length,
							-1,
							-1, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_BYTE,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("auto_explain.log_analyze",
							 "Use EXPLAIN ANALYZE for plan logging.",
							 NULL,
							 &auto_explain_log_analyze,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_settings",
							 "Log modified configuration parameters affecting query planning.",
							 NULL,
							 &auto_explain_log_settings,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_verbose",
							 "Use EXPLAIN VERBOSE for plan logging.",
							 NULL,
							 &auto_explain_log_verbose,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_buffers",
							 "Log buffers usage.",
							 NULL,
							 &auto_explain_log_buffers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_io",
							 "Log I/O statistics.",
							 NULL,
							 &auto_explain_log_io,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_wal",
							 "Log WAL usage.",
							 NULL,
							 &auto_explain_log_wal,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_triggers",
							 "Include trigger statistics in plans.",
							 "This has no effect unless log_analyze is also set.",
							 &auto_explain_log_triggers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("auto_explain.log_format",
							 "EXPLAIN format to be used for plan logging.",
							 NULL,
							 &auto_explain_log_format,
							 EXPLAIN_FORMAT_TEXT,
							 format_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("auto_explain.log_level",
							 "Log level for the plan.",
							 NULL,
							 &auto_explain_log_level,
							 LOG,
							 loglevel_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_nested_statements",
							 "Log nested statements.",
							 NULL,
							 &auto_explain_log_nested_statements,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("auto_explain.log_timing",
							 "Collect timing data, not just row counts.",
							 NULL,
							 &auto_explain_log_timing,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("auto_explain.log_extension_options",
							   "Extension EXPLAIN options to be added.",
							   NULL,
							   &auto_explain_log_extension_options,
							   NULL,
							   PGC_SUSET,
							   0,
							   check_log_extension_options,
							   assign_log_extension_options,
							   NULL);

	DefineCustomRealVariable("auto_explain.sample_rate",
							 "Fraction of queries to process.",
							 NULL,
							 &auto_explain_sample_rate,
							 1.0,
							 0.0,
							 1.0,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	MarkGUCPrefixReserved("auto_explain");

	/* Install hooks. */
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = explain_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = explain_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = explain_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = explain_ExecutorEnd;
}

/*
 * ExecutorStart hook: start up logging if needed
 */
static void
explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	/*
	 * At the beginning of each top-level statement, decide whether we'll
	 * sample this statement.  If nested-statement explaining is enabled,
	 * either all nested statements will be explained or none will.
	 *
	 * When in a parallel worker, we should do nothing, which we can implement
	 * cheaply by pretending we decided not to sample the current statement.
	 * If EXPLAIN is active in the parent session, data will be collected and
	 * reported back to the parent, and it's no business of ours to interfere.
	 */
	if (nesting_level == 0)
	{
		if (auto_explain_log_min_duration >= 0 && !IsParallelWorker())
			current_query_sampled = (pg_prng_double(&pg_global_prng_state) < auto_explain_sample_rate);
		else
			current_query_sampled = false;
	}

	if (auto_explain_enabled())
	{
		/* We're always interested in runtime */
		queryDesc->query_instr_options |= INSTRUMENT_TIMER;

		/* Enable per-node instrumentation iff log_analyze is required. */
		if (auto_explain_log_analyze && (eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		{
			if (auto_explain_log_timing)
				queryDesc->instrument_options |= INSTRUMENT_TIMER;
			else
				queryDesc->instrument_options |= INSTRUMENT_ROWS;
			if (auto_explain_log_buffers)
				queryDesc->instrument_options |= INSTRUMENT_BUFFERS;
			if (auto_explain_log_io)
				queryDesc->instrument_options |= INSTRUMENT_IO;
			if (auto_explain_log_wal)
				queryDesc->instrument_options |= INSTRUMENT_WAL;
		}
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
explain_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction,
					uint64 count)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
explain_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

/*
 * ExecutorEnd hook: log results if needed
 */
static void
explain_ExecutorEnd(QueryDesc *queryDesc)
{
	if (queryDesc->query_instr && auto_explain_enabled())
	{
		MemoryContext oldcxt;
		double		msec;

		/*
		 * Make sure we operate in the per-query context, so any cruft will be
		 * discarded later during ExecutorEnd.
		 */
		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);

		/* Log plan if duration is exceeded. */
		msec = INSTR_TIME_GET_MILLISEC(queryDesc->query_instr->total);
		if (msec >= auto_explain_log_min_duration)
		{
			ExplainState *es = NewExplainState();

			es->analyze = (queryDesc->instrument_options && auto_explain_log_analyze);
			es->verbose = auto_explain_log_verbose;
			es->buffers = (es->analyze && auto_explain_log_buffers);
			es->io = (es->analyze && auto_explain_log_io);
			es->wal = (es->analyze && auto_explain_log_wal);
			es->timing = (es->analyze && auto_explain_log_timing);
			es->summary = es->analyze;
			/* No support for MEMORY */
			/* es->memory = false; */
			es->format = auto_explain_log_format;
			es->settings = auto_explain_log_settings;

			apply_extension_options(es, current_auto_explain_extension_options());

			ExplainBeginOutput(es);
			ExplainQueryText(es, queryDesc);
			ExplainQueryParameters(es, queryDesc->params, auto_explain_log_parameter_max_length);
			ExplainPrintPlan(es, queryDesc);
			if (es->analyze && auto_explain_log_triggers)
				ExplainPrintTriggers(es, queryDesc);
			if (es->costs)
				ExplainPrintJITSummary(es, queryDesc);
			if (explain_per_plan_hook)
				(*explain_per_plan_hook) (queryDesc->plannedstmt,
										  NULL, es,
										  queryDesc->sourceText,
										  queryDesc->params,
										  queryDesc->estate->es_queryEnv);
			ExplainEndOutput(es);

			/* Remove last line break */
			if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
				es->str->data[--es->str->len] = '\0';

			/* Fix JSON to output an object */
			if (auto_explain_log_format == EXPLAIN_FORMAT_JSON)
			{
				es->str->data[0] = '{';
				es->str->data[es->str->len - 1] = '}';
			}

			/*
			 * Note: we rely on the existing logging of context or
			 * debug_query_string to identify just which statement is being
			 * reported.  This isn't ideal but trying to do it here would
			 * often result in duplication.
			 */
			ereport(auto_explain_log_level,
					(errmsg("duration: %.3f ms  plan:\n%s",
							msec, es->str->data),
					 errhidestmt(true)));
		}

		MemoryContextSwitchTo(oldcxt);
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * GUC check hook for auto_explain.log_extension_options.
 */
static bool
check_log_extension_options(char **newval, void **extra, GucSource source)
{
	char	   *rawstring;
	auto_explain_extension_options *result;
	auto_explain_option *options;
	int			maxoptions = 8;
	Size		rawstring_len;
	Size		allocsize;
	char	   *errmsg;

	/* NULL or empty string means no options. */
	if (*newval == NULL || (*newval)[0] == '\0')
	{
		*extra = NULL;
		return true;
	}

	rawstring_len = strlen(*newval) + 1;

retry:
	/* Try to allocate an auto_explain_extension_options object. */
	allocsize = offsetof(auto_explain_extension_options, options) +
		sizeof(auto_explain_option) * maxoptions +
		rawstring_len;
	result = (auto_explain_extension_options *) guc_malloc(LOG, allocsize);
	if (result == NULL)
		return false;

	/* Copy the string after the options array. */
	rawstring = (char *) &result->options[maxoptions];
	memcpy(rawstring, *newval, rawstring_len);

	/* Parse. */
	options = result->options;
	result->noptions = auto_explain_split_options(rawstring, options,
												  maxoptions, &errmsg);
	if (result->noptions < 0)
	{
		GUC_check_errdetail("%s", errmsg);
		guc_free(result);
		return false;
	}

	/*
	 * Retry with a larger array if needed.
	 *
	 * It should be impossible for this to loop more than once, because
	 * auto_explain_split_options tells us how many entries are needed.
	 */
	if (result->noptions > maxoptions)
	{
		maxoptions = result->noptions;
		guc_free(result);
		goto retry;
	}

	/* Validate each option against its registered check handler. */
	for (int i = 0; i < result->noptions; i++)
	{
		if (!GUCCheckExplainExtensionOption(options[i].name, options[i].value,
											options[i].type))
		{
			guc_free(result);
			return false;
		}
	}

	*extra = result;
	return true;
}

/*
 * GUC assign hook for auto_explain.log_extension_options.
 */
static void
assign_log_extension_options(const char *newval, void *extra)
{
	auto_explain_session_state()->extension_options = extra;
}

/*
 * Apply parsed extension options to an ExplainState.
 */
static void
apply_extension_options(ExplainState *es, auto_explain_extension_options *ext)
{
	if (ext == NULL)
		return;

	for (int i = 0; i < ext->noptions; i++)
	{
		auto_explain_option *opt = &ext->options[i];
		DefElem    *def;
		Node	   *arg;

		if (opt->value == NULL)
			arg = NULL;
		else if (opt->type == T_Integer)
			arg = (Node *) makeInteger(strtol(opt->value, NULL, 0));
		else if (opt->type == T_Float)
			arg = (Node *) makeFloat(opt->value);
		else
			arg = (Node *) makeString(opt->value);

		def = makeDefElem(opt->name, arg, -1);
		ApplyExtensionExplainOption(es, def, NULL);
	}
}

/*
 * auto_explain_scan_literal - In-place scanner for single-quoted string
 * literals.
 *
 * This is the single-quote analog of scan_quoted_identifier from varlena.c.
 */
static char *
auto_explain_scan_literal(char **endp, char **nextp)
{
	char	   *token = *nextp + 1;

	for (;;)
	{
		*endp = strchr(*nextp + 1, '\'');
		if (*endp == NULL)
			return NULL;		/* mismatched quotes */
		if ((*endp)[1] != '\'')
			break;				/* found end of literal */
		/* Collapse adjacent quotes into one quote, and look again */
		memmove(*endp, *endp + 1, strlen(*endp));
		*nextp = *endp;
	}
	/* *endp now points at the terminating quote */
	*nextp = *endp + 1;

	return token;
}

/*
 * auto_explain_split_options - Parse an option string into an array of
 * auto_explain_option structs.
 *
 * Much of this logic is similar to SplitIdentifierString and friends, but our
 * needs are different enough that we roll our own parsing logic. The goal here
 * is to accept the same syntax that the main parser would accept inside of
 * an EXPLAIN option list. While we can't do that perfectly without adding a
 * lot more code, the goal of this implementation is to be close enough that
 * users don't really notice the differences.
 *
 * The input string is modified in place (null-terminated, downcased, quotes
 * collapsed).  All name and value pointers in the output array refer into
 * this string, so the caller must ensure the string outlives the array.
 *
 * Returns the full number of options in the input string, but stores no
 * more than maxoptions into the caller-provided array. If a syntax error
 * occurs, returns -1 and sets *errmsg.
 */
static int
auto_explain_split_options(char *rawstring, auto_explain_option *options,
						   int maxoptions, char **errmsg)
{
	char	   *nextp = rawstring;
	int			noptions = 0;
	bool		done = false;

	*errmsg = NULL;

	while (scanner_isspace(*nextp))
		nextp++;				/* skip leading whitespace */

	if (*nextp == '\0')
		return 0;				/* empty string is fine */

	while (!done)
	{
		char	   *name;
		char	   *name_endp;
		char	   *value = NULL;
		char	   *value_endp = NULL;
		NodeTag		type = T_Invalid;

		/* Parse the option name. */
		name = scan_identifier(&name_endp, &nextp, ',', true);
		if (name == NULL || name_endp == name)
		{
			*errmsg = "option name missing or empty";
			return -1;
		}

		/* Skip whitespace after the option name. */
		while (scanner_isspace(*nextp))
			nextp++;

		/*
		 * Determine whether we have an option value.  A comma or end of
		 * string means no value; otherwise we have one.
		 */
		if (*nextp != '\0' && *nextp != ',')
		{
			if (*nextp == '\'')
			{
				/* Single-quoted string literal. */
				type = T_String;
				value = auto_explain_scan_literal(&value_endp, &nextp);
				if (value == NULL)
				{
					*errmsg = "unterminated single-quoted string";
					return -1;
				}
			}
			else if (isdigit((unsigned char) *nextp) ||
					 ((*nextp == '+' || *nextp == '-') &&
					  isdigit((unsigned char) nextp[1])))
			{
				char	   *endptr;
				long		intval;
				char		saved;

				/* Remember the start of the next token, and find the end. */
				value = nextp;
				while (*nextp && *nextp != ',' && !scanner_isspace(*nextp))
					nextp++;
				value_endp = nextp;

				/* Temporarily '\0'-terminate so we can use strtol/strtod. */
				saved = *value_endp;
				*value_endp = '\0';

				/*
				 * Integer, float, or neither?
				 *
				 * NB: Since we use strtol and strtod here rather than
				 * pg_strtoint64_safe, some syntax that would be accepted by
				 * the main parser is not accepted here, e.g. 100_000. On the
				 * plus side, strtol and strtod won't allocate, and
				 * pg_strtoint64_safe might. For now, it seems better to keep
				 * things simple here.
				 */
				errno = 0;
				intval = strtol(value, &endptr, 0);
				if (errno == 0 && *endptr == '\0' && endptr != value &&
					intval == (int) intval)
					type = T_Integer;
				else
				{
					type = T_Float;
					(void) strtod(value, &endptr);
					if (*endptr != '\0')
					{
						*value_endp = saved;
						*errmsg = "invalid numeric value";
						return -1;
					}
				}

				/* Remove temporary terminator. */
				*value_endp = saved;
			}
			else
			{
				/* Identifier, possibly double-quoted. */
				type = T_String;
				value = scan_identifier(&value_endp, &nextp, ',', true);
				if (value == NULL)
				{
					/*
					 * scan_identifier will return NULL if it finds an
					 * unterminated double-quoted identifier or it finds no
					 * identifier at all because the next character is
					 * whitespace or the separator character, here a comma.
					 * But the latter case is impossible here because the code
					 * above has skipped whitespace and checked for commas.
					 */
					*errmsg = "unterminated double-quoted string";
					return -1;
				}
			}
		}

		/* Skip trailing whitespace. */
		while (scanner_isspace(*nextp))
			nextp++;

		/* Expect comma or end of string. */
		if (*nextp == ',')
		{
			nextp++;
			while (scanner_isspace(*nextp))
				nextp++;
			if (*nextp == '\0')
			{
				*errmsg = "trailing comma in option list";
				return -1;
			}
		}
		else if (*nextp == '\0')
			done = true;
		else
		{
			*errmsg = "expected comma or end of option list";
			return -1;
		}

		/*
		 * Now safe to null-terminate the name and value.  We couldn't do this
		 * earlier because in the unquoted case, the null terminator position
		 * may coincide with a character that the scanning logic above still
		 * needed to read.
		 */
		*name_endp = '\0';
		if (value_endp != NULL)
			*value_endp = '\0';

		/* Always count this option, and store the details if there is room. */
		if (noptions < maxoptions)
		{
			options[noptions].name = name;
			options[noptions].type = type;
			options[noptions].value = value;
		}
		noptions++;
	}

	return noptions;
}
