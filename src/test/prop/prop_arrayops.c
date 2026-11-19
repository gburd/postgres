/*-------------------------------------------------------------------------
 *
 * prop_arrayops.c
 *	  Property-based tests for array modification and conversion functions.
 *
 * Covers the array builtins not exercised by the "array" and "arrayslice"
 * areas: array_cat (||), array_append/array_prepend, array_position/
 * array_positions, array_remove, array_replace, cardinality,
 * array_to_string/string_to_array, and the array_agg/unnest round trip.
 * The oracle is each function's own contract -- length arithmetic, where an
 * appended/prepended element lands, that remove/replace actually eliminate a
 * value, and that string and aggregate conversions round-trip -- so any "f"
 * result is a wrong-results bug in one of these functions.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_arrayops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "arrayops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a NULL-free integer-array literal into buf as ARRAY[...]::int[], with
 * nmin..nmax elements each drawn from -5..5.  The cast makes an empty array
 * (ARRAY[]::int[]) carry a type.  Negative elements are parenthesised as
 * "(%d)" so a leading minus never misparses inside the ARRAY[] list.  No NULL
 * elements: the ordered/position properties below rely on crisp equality and
 * a known-absent sentinel.
 */
static void
draw_intarr(pg_prop_tc *tc, StringInfo buf, int nmin, int nmax)
{
	int			n = (int) pg_prop_draw_int(tc, nmin, nmax);
	int			i;

	appendStringInfoString(buf, "ARRAY[");
	for (i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(buf, "]::int[]");
}

/*
 * Draw an integer-array literal that may contain SQL NULL elements (~20% of
 * the time), for the NULL-aware properties.  Elements otherwise drawn from
 * -5..5 and parenthesised as above.  nmin..nmax bound the length.
 */
static void
draw_intarr_null(pg_prop_tc *tc, StringInfo buf, int nmin, int nmax)
{
	int			n = (int) pg_prop_draw_int(tc, nmin, nmax);
	int			i;

	appendStringInfoString(buf, "ARRAY[");
	for (i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		if (pg_prop_draw_int(tc, 0, 4) == 0)
			appendStringInfoString(buf, "NULL");
		else
			appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(buf, "]::int[]");
}

/*
 * Draw a NULL-free, non-negative integer-array literal (elements 0..5) into
 * buf, nmin..nmax elements.  Used by the array_to_string round trip so no '-'
 * sign ever appears in the delimited text.
 */
static void
draw_intarr_nonneg(pg_prop_tc *tc, StringInfo buf, int nmin, int nmax)
{
	int			n = (int) pg_prop_draw_int(tc, nmin, nmax);
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
 * cardinality(a || b) = cardinality(a) + cardinality(b).  "||" on two arrays
 * is array_cat; concatenation adds lengths exactly, including the empty-array
 * cases (cardinality of empty is 0).
 */
PG_PROP(cat_length_additive)
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
	draw_intarr(tc, &a, 0, 6);
	draw_intarr(tc, &b, 0, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(%s || %s) = "
					 "cardinality(%s) + cardinality(%s)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_append(a, 7) grows the length by one and places 7 at the new last
 * position: cardinality(array_append(a, 7)) = cardinality(a) + 1 and
 * array_append(a, 7)[cardinality(a) + 1] = 7.  (int arrays are 1-based, so
 * the appended element is at index cardinality(a) + 1.)
 */
PG_PROP(append_increments)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a, 0, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(array_append(%s, 7)) = cardinality(%s) + 1 "
					 "AND (array_append(%s, 7))[cardinality(%s) + 1] = 7",
					 a.data, a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_prepend(7, a) grows the length by one and places 7 at the first
 * position: cardinality(array_prepend(7, a)) = cardinality(a) + 1 and
 * array_prepend(7, a)[1] = 7.  (int arrays are 1-based.)
 */
PG_PROP(prepend_increments)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a, 0, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(array_prepend(7, %s)) = cardinality(%s) + 1 "
					 "AND (array_prepend(7, %s))[1] = 7",
					 a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_position(array_append(a, 99), 99) = cardinality(a) + 1.  99 cannot
 * occur in a (drawn from -5..5), so the only 99 is the appended one, which
 * sits at the new last position -- array_position finds exactly it.
 */
PG_PROP(position_finds_appended)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a, 0, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT array_position(array_append(%s, 99), 99) = "
					 "cardinality(%s) + 1",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_remove deletes every occurrence of its argument: after appending the
 * known-absent sentinel 99 and then removing 99, no 99 remains, so
 * array_position(..., 99) IS NULL.  (a is drawn from -5..5, so the only 99
 * is the one we appended.)
 */
PG_PROP(remove_eliminates)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a, 0, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT array_position("
					 "array_remove(array_append(%s, 99), 99), 99) IS NULL",
					 a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_replace swaps one value for another: after appending the known-absent
 * sentinel 99 and replacing 99 with 42, the 99 is gone and a 42 is present.
 * Both 99 and 42 are outside -5..5, so a contributes neither; the single 99
 * we appended becomes the single 42.
 */
PG_PROP(replace_swaps)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr(tc, &a, 0, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT array_position("
					 "array_replace(array_append(%s, 99), 99, 42), 99) IS NULL "
					 "AND array_position("
					 "array_replace(array_append(%s, 99), 99, 42), 42) IS NOT NULL",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_to_string / string_to_array round trip: for a non-empty array of
 * non-negative ints, string_to_array(array_to_string(a, ','), ',')::int[] = a.
 * Elements are 0..5 so no '-' complicates the delimited text, and the array
 * is non-empty (1..6 elements) so the empty-array corner (array_to_string of
 * empty is '', and string_to_array('', ',') is {''}) never arises.
 */
PG_PROP(to_string_from_string_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr_nonneg(tc, &a, 1, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT string_to_array(array_to_string(%s, ','), ',')::int[] = %s",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_agg over unnest WITH ORDINALITY reconstructs the array:
 *     array_agg(x ORDER BY ord) FROM unnest(a) WITH ORDINALITY AS u(x, ord)
 * returns a exactly, since ORDINALITY numbers the elements in array order and
 * array_agg(... ORDER BY ord) reassembles them in that order -- including
 * NULL elements, which unnest preserves.  Drawn with 1..6 elements so a is
 * non-empty (array_agg over 0 rows is NULL); draw_intarr_null exercises the
 * NULL-preserving path.
 */
PG_PROP(unnest_agg_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr_null(tc, &a, 1, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT array_agg(x ORDER BY ord) "
					 "FROM unnest(%s) WITH ORDINALITY AS u(x, ord)) "
					 "IS NOT DISTINCT FROM %s",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
