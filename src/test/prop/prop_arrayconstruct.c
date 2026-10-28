/*-------------------------------------------------------------------------
 *
 * prop_arrayconstruct.c
 *	  Property-based tests for array construction (constructors, dims, fill).
 *
 * Covers the array-construction surface not exercised by array/arrayops/
 * arrayslice/arraycmp: the ARRAY[...] constructor, ARRAY(subquery), array_fill,
 * array_dims/array_ndims/array_length, cardinality over multidim arrays, and
 * the dimensional effect of array_append.  The oracle is each construct's own
 * contract -- a constructed array equals the equivalent literal, array_fill
 * produces exactly the requested shape and value, and the dim/length/
 * cardinality functions report the true shape -- so any "f" result is a
 * wrong-results bug in array construction or introspection.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_arrayconstruct.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "arrayconstruct"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the construct errored or produced NULL on finite inputs,
 * which is itself a bug -- so either is a failure, reported with the SQL.
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
 * ARRAY(subquery) materialises its ordered result in order, so
 * ARRAY(SELECT v FROM (VALUES ...) ORDER BY v) over 1,2,3 equals the literal
 * constructor ARRAY[1,2,3].  Checks the subquery-array path against the plain
 * constructor.
 */
PG_PROP(array_subquery_eq_agg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT ARRAY(SELECT v FROM (VALUES (1),(2),(3)) t(v) "
				 "ORDER BY v) = ARRAY[1,2,3]");

	pg_prop_disconnect(conn);
}

/*
 * array_fill(7, ARRAY[k]) builds a 1-D array of exactly k copies of 7, so its
 * cardinality is k and every element (checked at index 1) is 7.  Drawn k in
 * 1..6.
 */
PG_PROP(array_fill_dims)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k = (int) pg_prop_draw_int(tc, 1, 6);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(array_fill(7, ARRAY[%d])) = %d "
					 "AND (array_fill(7, ARRAY[%d]))[1] = 7",
					 k, k, k);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A 1-D ARRAY[...] of m >= 1 elements has dimension string '[1:m]', since the
 * constructor assigns the default lower bound 1 and the length is m.  Elements
 * are drawn ints (parenthesised negatives); their values do not affect dims.
 */
PG_PROP(array_dims_1d)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			m = (int) pg_prop_draw_int(tc, 1, 6);
	int			i;
	StringInfoData arr;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&arr);
	appendStringInfoString(&arr, "ARRAY[");
	for (i = 0; i < m; i++)
	{
		if (i > 0)
			appendStringInfoString(&arr, ", ");
		appendStringInfo(&arr, "(%d)", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(&arr, "]");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT array_dims(%s) = '[1:' || %d || ']'",
					 arr.data, m);

	require_true(tc, conn, q.data);

	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_ndims reports the number of dimensions: 1 for a flat constructor and 2
 * for a nested one.  Constant checks -- the contract is about the shape of the
 * literal, independent of element values.
 */
PG_PROP(array_ndims)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT array_ndims(ARRAY[1,2,3]) = 1 "
				 "AND array_ndims(ARRAY[[1,2],[3,4]]) = 2");

	pg_prop_disconnect(conn);
}

/*
 * For a 2x3 nested constructor, array_length(.,1) is the outer dimension (2)
 * and array_length(.,2) is the inner dimension (3).  Constant check against a
 * known literal shape.
 */
PG_PROP(array_length_each_dim)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT array_length(ARRAY[[1,2,3],[4,5,6]], 1) = 2 "
				 "AND array_length(ARRAY[[1,2,3],[4,5,6]], 2) = 3");

	pg_prop_disconnect(conn);
}

/*
 * The ARRAY[a,b,c] constructor and the text literal '{a,b,c}'::int[] denote the
 * same array, so they compare equal.  Negatives are parenthesised in the
 * constructor ("(%d)"); the literal form writes '-' directly inside the braces.
 */
PG_PROP(array_constructor_eq_literal)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a = (int) pg_prop_draw_int(tc, -5, 5);
	int			b = (int) pg_prop_draw_int(tc, -5, 5);
	int			c = (int) pg_prop_draw_int(tc, -5, 5);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ARRAY[(%d),(%d),(%d)] = "
					 "('{' || %d || ',' || %d || ',' || %d || '}')::int[]",
					 a, b, c, a, b, c);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * cardinality counts every element across all dimensions, so a 3x2 nested
 * constructor has cardinality 6 (not its outer length 3).  Constant check.
 */
PG_PROP(cardinality_multidim)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT cardinality(ARRAY[[1,2],[3,4],[5,6]]) = 6");

	pg_prop_disconnect(conn);
}

/*
 * array_append extends a 1-D array by exactly one element, so appending to a
 * length-3 array yields array_length(.,1) = 4.  Constant check.
 */
PG_PROP(array_append_dim)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT array_length(array_append(ARRAY[1,2,3], 4), 1) = 4");

	pg_prop_disconnect(conn);
}
