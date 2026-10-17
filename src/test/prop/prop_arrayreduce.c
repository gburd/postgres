/*-------------------------------------------------------------------------
 *
 * prop_arrayreduce.c
 *	  Property-based tests for array aggregation and reduction functions.
 *
 * Covers the length/position/trim/aggregate reductions not exercised by the
 * "array", "arrayops", "arraycmp", "arrayconstruct" and "arrayslice" areas:
 * array_positions, trim_array, array_agg over unnest WITH ORDINALITY, the
 * array_length-is-NULL-on-empty versus cardinality-is-0 distinction, and
 * array_remove.  The oracle is each function's own contract -- where
 * positions land, that trim removes exactly k elements from the end, that
 * agg/unnest round-trips, and the empty/NULL length semantics -- so any "f"
 * result is a wrong-results bug in one of these reductions.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_arrayreduce.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "arrayreduce"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a NULL-free integer-array literal into buf as ARRAY[...]::int[], with
 * 0..6 elements each drawn from 0..5.  The cast gives an empty array
 * (ARRAY[]::int[]) a type.  All elements are in 0..5, so 7, 9 and 99 are
 * guaranteed-absent sentinels the properties below rely on; and because no
 * element is negative there is never a leading '-' to misparse in the list.
 * No NULL elements: the position/remove/aggregate properties rely on crisp
 * equality and a known-absent sentinel.
 */
static void
draw_intarr(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 6);
	int			i;

	appendStringInfoString(buf, "ARRAY[");
	for (i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, 0, 5));
	}
	appendStringInfoString(buf, "]::int[]");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on a well-formed
 * input, which is itself a bug -- so either is a failure, reported with the
 * SQL.  (Copied from prop_numeric.c's require_true.)
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	}
	if (res == NULL)
	{
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	}
	if (strcmp(res, "t") != 0)
	{
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	}
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * array_positions finds every occurrence.  Appending ARRAY[9,9] to a (9 is a
 * guaranteed-absent sentinel, elements of a are 0..5) places the only two 9s
 * at the last two positions, so array_positions must report exactly
 * {cardinality(a)+1, cardinality(a)+2}.
 */
PG_PROP(positions_finds_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT array_positions(%s || ARRAY[9,9], 9) "
					 "= ARRAY[cardinality(%s) + 1, cardinality(%s) + 2]",
					 a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_positions of an absent value is the empty array (not NULL).  99 is a
 * guaranteed-absent sentinel, so searching for it must yield ARRAY[]::int[].
 */
PG_PROP(positions_empty_when_absent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT array_positions(%s, 99) = ARRAY[]::int[]",
					 a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * trim_array shortens by exactly k: cardinality(trim_array(a, k)) =
 * cardinality(a) - k for every valid k in 0..cardinality(a).  k is drawn
 * after a's length is known so it never exceeds the array (trim_array errors
 * if k is out of range).
 */
PG_PROP(trim_array_shortens)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			card;
	int			k;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a);

	/* learn the cardinality so k stays in trim_array's legal range */
	{
		bool		failed = false;
		char	   *cerr = NULL;
		char	   *cres;
		StringInfoData cq;

		initStringInfo(&cq);
		appendStringInfo(&cq, "SELECT cardinality(%s)", a.data);
		cres = pg_prop_sql_scalar(conn, cq.data, &failed, &cerr);
		if (failed || cres == NULL)
			pg_prop_fail(tc, "cardinality probe failed: %s\n  SQL: %s",
						 cerr ? cerr : "(null)", cq.data);
		card = atoi(cres);
		pg_free(cres);
		if (cerr)
			pg_free(cerr);
		pfree(cq.data);
	}

	k = (int) pg_prop_draw_int(tc, 0, card);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(trim_array(%s, %d)) "
					 "= cardinality(%s) - %d",
					 a.data, k, a.data, k);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * trim_array removes from the END: trim_array(a, k) equals the prefix slice
 * a[1 : cardinality(a) - k].  k is drawn in range as above.  IS NOT DISTINCT
 * FROM covers the k = cardinality(a) case cleanly (both sides are the empty
 * array; neither is NULL, but the operator is sound regardless).
 */
PG_PROP(trim_array_prefix)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			card;
	int			k;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a);

	{
		bool		failed = false;
		char	   *cerr = NULL;
		char	   *cres;
		StringInfoData cq;

		initStringInfo(&cq);
		appendStringInfo(&cq, "SELECT cardinality(%s)", a.data);
		cres = pg_prop_sql_scalar(conn, cq.data, &failed, &cerr);
		if (failed || cres == NULL)
			pg_prop_fail(tc, "cardinality probe failed: %s\n  SQL: %s",
						 cerr ? cerr : "(null)", cq.data);
		card = atoi(cres);
		pg_free(cres);
		if (cerr)
			pg_free(cerr);
		pfree(cq.data);
	}

	k = (int) pg_prop_draw_int(tc, 0, card);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT trim_array(%s, %d) "
					 "IS NOT DISTINCT FROM (%s)[1 : cardinality(%s) - %d]",
					 a.data, k, a.data, a.data, k);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_agg(x ORDER BY ord) over unnest(a) WITH ORDINALITY reconstructs a.
 * The aggregate over 0 rows is SQL NULL, so coalesce it to '{}'::int[] for
 * the empty-array case; the comparison then holds for every a.
 */
PG_PROP(array_agg_unnest_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce("
					 "(SELECT array_agg(x ORDER BY ord) "
					 "FROM unnest(%s) WITH ORDINALITY AS u(x, ord)), "
					 "'{}'::int[]) = %s",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An empty array has a NULL length in each dimension: array_length('{}', 1)
 * IS NULL.  This is a fixed, input-independent fact; it documents the
 * empty-array length semantics the next property contrasts against
 * cardinality.
 */
PG_PROP(array_length_null_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT array_length('{}'::int[], 1) IS NULL");

	pg_prop_disconnect(conn);
}

/*
 * cardinality is 0 for the empty array where array_length is NULL:
 * cardinality(a) = coalesce(array_length(a, 1), 0).  For a non-empty 1-D
 * array both equal the length; for the empty array cardinality is 0 and
 * array_length is NULL, which the coalesce turns into 0.
 */
PG_PROP(cardinality_vs_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(%s) = coalesce(array_length(%s, 1), 0)",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_remove deletes every matching element.  Appending ARRAY[7] (7 is a
 * guaranteed-absent sentinel) adds exactly one 7; removing 7 must therefore
 * leave cardinality(a) elements -- the one appended 7 and nothing else is
 * removed.
 */
PG_PROP(array_remove_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(array_remove(%s || ARRAY[7], 7)) "
					 "= cardinality(%s)",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
