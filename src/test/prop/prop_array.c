/*-------------------------------------------------------------------------
 *
 * prop_array.c
 *	  Property-based tests for PostgreSQL array types and functions.
 *
 * Array in/out, subscripting, cardinality, and aggregation have had assorted
 * fixes; these check round-trips and the aggregate/unnest inverse over
 * integer arrays.
 *
 * The oracle is the array machinery's own algebra against SQL: text output
 * parsed back must equal the array, cardinality must agree with array_length
 * on a 1-D array, concatenation length is additive, an array contains itself,
 * element membership agrees with an explicit unnest scan, and aggregating the
 * unnest of an array (then unnesting and re-aggregating) yields the same
 * sorted array.  Any "f" (or an unexpected NULL/error) is a bug in the array
 * code, not a modelling slip -- the properties are stated so NULL and empty
 * edges stay crisp via coalesce()/IS NOT DISTINCT FROM.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_array.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "array"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a random SQL integer-array literal into buf as an ARRAY[...] expression
 * typed ::int[].  n is drawn in 0..8 elements, each a small int in -20..20;
 * with include_null set, elements may be SQL NULL (ARRAY[1, NULL, 3]).  The
 * ::int[] cast is always emitted so an empty array (ARRAY[]::int[]) and an
 * all-NULL array still carry a type.  Returns the element count n in case the
 * caller wants it.
 */
static int
draw_arr_opt(pg_prop_tc *tc, StringInfo buf, bool include_null)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 8);

	appendStringInfoString(buf, "ARRAY[");
	for (int i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		if (include_null && pg_prop_draw_int(tc, 0, 4) == 0)
			appendStringInfoString(buf, "NULL");
		else
			appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, -20, 20));
	}
	appendStringInfoString(buf, "]::int[]");

	return n;
}

/* Draw an integer array that may contain NULL elements. */
static int
draw_arr(pg_prop_tc *tc, StringInfo buf)
{
	return draw_arr_opt(tc, buf, true);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the array operation errored or produced NULL on valid
 * input, which is itself a bug -- so either is a failure, reported with the
 * SQL and any error text.
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
 * cardinality(A) = coalesce(array_length(A, 1), 0).  For a 1-D array these
 * agree: array_length is NULL for an empty array while cardinality is 0, so
 * the coalesce reconciles the empty case.
 */
PG_PROP(cardinality_matches_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr(tc, &a);

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
 * Aggregate/unnest inverse, order-insensitively: aggregating the unnest of A,
 * then unnesting that and re-aggregating, yields the same sorted array.  Both
 * sides sort with ORDER BY x, and IS NOT DISTINCT FROM makes the all-NULL and
 * empty cases (where either side can be NULL) compare equal.  unnest preserves
 * NULL elements, so the multiset of elements is what is being compared.
 */
PG_PROP(agg_unnest_roundtrip_sorted)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT array_agg(x ORDER BY x) FROM unnest(%s) AS x) "
					 "IS NOT DISTINCT FROM "
					 "(SELECT array_agg(y ORDER BY y) "
					 " FROM unnest((SELECT array_agg(x) FROM unnest(%s) AS x)) AS y)",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concatenation length is additive:
 *		coalesce(cardinality(A || B), 0)
 *			= coalesce(cardinality(A), 0) + coalesce(cardinality(B), 0).
 * cardinality is already 0 for an empty array, but the coalesce guards the
 * NULL that cardinality() returns if a concatenation ever yields NULL.
 */
PG_PROP(concat_length_additive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_arr(tc, &a);
	draw_arr(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(cardinality(%s || %s), 0) "
					 "= coalesce(cardinality(%s), 0) + coalesce(cardinality(%s), 0)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Text round-trip: A::text::int[] IS NOT DISTINCT FROM A.  Array output parsed
 * back must equal the array; IS NOT DISTINCT FROM handles the all-NULL and
 * empty edges where a plain = would yield NULL.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s)::text::int[] IS NOT DISTINCT FROM (%s)",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment is reflexive: A @> A.  Every array contains itself, including
 * the empty array.
 */
PG_PROP(contains_self)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);

	/*
	 * Containment uses element equality, and SQL NULL is never equal to NULL,
	 * so an array containing a NULL does not even contain itself.  @> is
	 * reflexive only for NULL-free arrays; draw one.
	 */
	draw_arr_opt(tc, &a, false);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s) @> (%s)", a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Element membership agrees with an explicit unnest scan:
 *		(e = ANY (A)) = EXISTS (SELECT 1 FROM unnest(A) u WHERE u = e).
 * A is drawn WITHOUT NULL elements here so e = ANY(A) is a crisp true/false
 * (a NULL element would let ANY return NULL on a no-match), keeping the
 * equality exact; e is a concrete small int.
 */
PG_PROP(element_membership_consistent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			e;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr_opt(tc, &a, false);	/* no NULL elements for a crisp ANY */
	e = (int) pg_prop_draw_int(tc, -20, 20);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%d = ANY (%s)) "
					 "= EXISTS (SELECT 1 FROM unnest(%s) u WHERE u = %d)",
					 e, a.data, a.data, e);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
