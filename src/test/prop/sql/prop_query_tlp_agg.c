/*-------------------------------------------------------------------------
 *
 * prop_query_tlp_agg.c
 *	  Query-level property tests: TLP specialised to aggregates.
 *
 * Core TLP (prop_query_tlp.c) partitions a table three ways over a predicate
 * and reconstructs the row multiset.  This area applies the same ternary
 * partition to *aggregates*, where the reconstruction law is arithmetic
 * rather than UNION ALL:
 *
 *	   count(*)        over t  ==  count(p) + count(not p) + count(p is null)
 *	   sum(e)          over t  ==  sum over the three partitions
 *	   max(e)          over t  ==  max of the three partition maxima
 *	   min(e)          over t  ==  min of the three partition minima
 *
 * Aggregation runs through hash/sorted/grouping executor paths that plain row
 * return does not, and the folding of partial aggregates (and of NULL groups)
 * is a classic wrong-results surface.  The oracle is still a tautology of SQL
 * semantics -- no invented reference value -- so any disagreement is a server
 * bug, not a flaky expectation.
 *
 * NULL handling is the subtle part and the reason this is worth a separate
 * area: SQL aggregates skip NULL inputs, and an empty partition yields NULL
 * (for sum/min/max) or 0 (for count).  We fold with coalesce and compare the
 * whole-table aggregate against the folded partitions as text so NULL == NULL
 * compares equal.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see pg_prop.h
 * and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_tlp_agg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_tlp_agg"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/*
 * Compare two single-value queries as text (so NULL compares equal to NULL)
 * and fail with both SQL strings on a mismatch.
 */
static void
require_same_scalar(pg_prop_tc *tc, pg_prop_conn *conn,
					const char *whole, const char *folded)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL;
	char	   *v1,
			   *v2;

	v1 = pg_prop_sql_scalar(conn, whole, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "whole-table aggregate failed: %s\n  SQL: %s",
					 e1 ? e1 : "(unknown)", whole);

	v2 = pg_prop_sql_scalar(conn, folded, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "folded-partition aggregate failed: %s\n  SQL: %s",
					 e2 ? e2 : "(unknown)", folded);

	/*
	 * max()/min() return SQL NULL for an empty or all-NULL input, which comes
	 * back as a C NULL pointer; the whole and folded forms must agree on NULL
	 * too (both NULL, or equal text).
	 */
	if ((v1 == NULL) != (v2 == NULL) ||
		(v1 != NULL && strcmp(v1, v2) != 0))
		pg_prop_fail(tc,
					 "TLP-aggregate mismatch:\n  whole:  %s = %s\n"
					 "  folded: %s = %s",
					 whole, v1 ? v1 : "NULL", folded, v2 ? v2 : "NULL");

	if (v1)
		pg_free(v1);
	if (v2)
		pg_free(v2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/* Build "<agg> FROM qg_t" and the folded three-partition form, then compare. */
static void
check_agg(pg_prop_tc *tc, pg_prop_conn *conn, const char *pred,
		  const char *whole_expr, const char *fold_fmt)
{
	StringInfoData whole;
	StringInfoData folded;

	initStringInfo(&whole);
	appendStringInfo(&whole, "SELECT %s FROM " QG_TABLE, whole_expr);

	initStringInfo(&folded);
	/* fold_fmt has three "%s" slots for the predicate (p, NOT p, p IS NULL). */
	appendStringInfo(&folded, fold_fmt, pred, pred, pred);

	require_same_scalar(tc, conn, whole.data, folded.data);

	pfree(whole.data);
	pfree(folded.data);
}

/* count(*) over t == count over the three partitions, summed. */
PG_PROP(count_folds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	check_agg(tc, conn, pred.data, "count(*)",
			  "SELECT (SELECT count(*) FROM " QG_TABLE " WHERE (%s)) "
			  "+ (SELECT count(*) FROM " QG_TABLE " WHERE NOT (%s)) "
			  "+ (SELECT count(*) FROM " QG_TABLE " WHERE (%s) IS NULL)");

	pfree(pred.data);
	pg_prop_disconnect(conn);
}

/* sum(a) over t == sum over the three partitions (coalesced for empties). */
PG_PROP(sum_folds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	/*
	 * sum() skips NULL inputs and returns NULL on an empty set; coalesce each
	 * partition's sum to 0 so the three fold additively to the whole-table
	 * sum, which is itself coalesced to 0 for an all-empty/all-NULL table.
	 */
	check_agg(tc, conn, pred.data, "coalesce(sum(a), 0)",
			  "SELECT coalesce((SELECT sum(a) FROM " QG_TABLE " WHERE (%s)), 0) "
			  "+ coalesce((SELECT sum(a) FROM " QG_TABLE " WHERE NOT (%s)), 0) "
			  "+ coalesce((SELECT sum(a) FROM " QG_TABLE " WHERE (%s) IS NULL), 0)");

	pfree(pred.data);
	pg_prop_disconnect(conn);
}

/* max(a) over t == max(a) over the three partitions UNION ALL'd. */
PG_PROP(max_folds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	/*
	 * max() ignores NULLs and yields NULL for an empty input.  Re-aggregating
	 * max over the three partitions concatenated is the whole-table max; this
	 * single aggregate over a UNION ALL avoids the planner pathology of
	 * greatest() over three correlated subqueries.
	 */
	check_agg(tc, conn, pred.data, "max(a)",
			  "SELECT max(a) FROM ("
			  "SELECT a FROM " QG_TABLE " WHERE (%s) "
			  "UNION ALL SELECT a FROM " QG_TABLE " WHERE NOT (%s) "
			  "UNION ALL SELECT a FROM " QG_TABLE " WHERE (%s) IS NULL) s");

	pfree(pred.data);
	pg_prop_disconnect(conn);
}

/* min(a) over t == min(a) over the three partitions UNION ALL'd. */
PG_PROP(min_folds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	check_agg(tc, conn, pred.data, "min(a)",
			  "SELECT min(a) FROM ("
			  "SELECT a FROM " QG_TABLE " WHERE (%s) "
			  "UNION ALL SELECT a FROM " QG_TABLE " WHERE NOT (%s) "
			  "UNION ALL SELECT a FROM " QG_TABLE " WHERE (%s) IS NULL) s");

	pfree(pred.data);
	pg_prop_disconnect(conn);
}
