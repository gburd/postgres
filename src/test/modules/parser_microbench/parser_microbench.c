/*-------------------------------------------------------------------------
 *
 * parser_microbench.c
 *	  Direct microbenchmark of raw_parser() with the dummy_grammar_ext
 *	  loaded.  Measures the steady-state per-parse cost without
 *	  pgbench's connect/network/execute overhead.
 *
 *	  Exposes one SQL function: parser_microbench(query text,
 *	  iterations int) RETURNS bigint.  Returns total nanoseconds for
 *	  N iterations of raw_parser() on the given query string.  Caller
 *	  divides for per-parse latency.
 *
 *	  Method:
 *	    1. Compile the query to a parser-state-cleared snapshot.
 *	    2. Time N parse cycles using clock_gettime(CLOCK_MONOTONIC).
 *	    3. Drop intermediate parse trees in a child memory context
 *	       so allocator pressure is consistent across runs.
 *
 *	  Loaded via shared_preload_libraries together with whichever
 *	  grammar extensions are being measured.  The function works
 *	  identically with no extensions loaded -- baseline number is
 *	  the static base_yyparse path.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * src/test/modules/parser_microbench/parser_microbench.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(parser_microbench);

Datum
parser_microbench(PG_FUNCTION_ARGS)
{
	text	   *query_text = PG_GETARG_TEXT_PP(0);
	int32		iterations = PG_GETARG_INT32(1);
	const char *query;
	struct timespec t0,
				t1;
	int64		ns_total;
	MemoryContext bench_ctx;
	MemoryContext old;
	int			i;

	if (iterations <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iterations must be positive")));

	query = text_to_cstring(query_text);

	bench_ctx = AllocSetContextCreate(CurrentMemoryContext,
									  "parser_microbench",
									  ALLOCSET_DEFAULT_SIZES);

	/*
	 * Warm the parser path: one untimed parse to populate any lazy-init state
	 * in the rebuilt .so / scanner.
	 */
	old = MemoryContextSwitchTo(bench_ctx);
	(void) raw_parser(query, RAW_PARSE_DEFAULT);
	MemoryContextReset(bench_ctx);
	MemoryContextSwitchTo(old);

	clock_gettime(CLOCK_MONOTONIC, &t0);
	for (i = 0; i < iterations; i++)
	{
		old = MemoryContextSwitchTo(bench_ctx);
		(void) raw_parser(query, RAW_PARSE_DEFAULT);
		MemoryContextReset(bench_ctx);
		MemoryContextSwitchTo(old);
	}
	clock_gettime(CLOCK_MONOTONIC, &t1);

	MemoryContextDelete(bench_ctx);

	ns_total = (int64) (t1.tv_sec - t0.tv_sec) * 1000000000LL +
		(int64) (t1.tv_nsec - t0.tv_nsec);

	PG_RETURN_INT64(ns_total);
}
