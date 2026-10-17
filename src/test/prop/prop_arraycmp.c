/*-------------------------------------------------------------------------
 *
 * prop_arraycmp.c
 *	  Property-based tests for array comparison, containment, and overlap.
 *
 * Exercises the built-in anyarray operators @> (contains), <@ (contained by),
 * && (overlaps), = (equality), < (lexicographic order), and || (concat) over
 * integer arrays.  The oracle is each operator's own definition expressed a
 * second way: containment is element-set membership, overlap is a shared
 * element, equality is element-wise agreement, and lexicographic order has
 * fixed-constant anchors.  Any "f" (or an unexpected NULL/error) is a bug in
 * the array operator code, not a modelling slip -- NULL-array semantics are
 * subtle so the overlap/equality properties draw NULL-free elements.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_arraycmp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "arraycmp"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a random SQL integer-array literal into buf as ARRAY[...]::int[].  n is
 * drawn in 0..6 elements, each a small int in -5..5; with include_null set,
 * an element may be SQL NULL (ARRAY[1, NULL, 3]).  The ::int[] cast is always
 * emitted so an empty array (ARRAY[]::int[]) still carries a type.  Returns
 * the element count n.
 */
static int
draw_arr_opt(pg_prop_tc *tc, StringInfo buf, bool include_null)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 6);

	appendStringInfoString(buf, "ARRAY[");
	for (int i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		if (include_null && pg_prop_draw_int(tc, 0, 4) == 0)
			appendStringInfoString(buf, "NULL");
		else
			appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(buf, "]::int[]");

	return n;
}

/*
 * Draw a fixed-length integer-array literal of exactly m non-NULL elements.
 * Used where two arrays must share a length (unnest of two arrays) or where
 * NULL elements would make the operator return NULL instead of a crisp bool.
 */
static void
draw_arr_fixed(pg_prop_tc *tc, StringInfo buf, int m)
{
	appendStringInfoString(buf, "ARRAY[");
	for (int i = 0; i < m; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(buf, "]::int[]");
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
 * Containment is reflexive and symmetric at the ends: A @> A AND A <@ A.
 * Containment is set membership, and SQL NULL is never equal to NULL, so an
 * array holding a NULL would not even contain itself; draw NULL-free so both
 * directions are crisp true.
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
	draw_arr_opt(tc, &a, false);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s) @> (%s) AND (%s) <@ (%s)",
					 a.data, a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Every array contains any subset of its own elements, including the empty
 * array: A @> ARRAY[]::int[], and A @> (the first k elements of A).  The
 * subset is built with unnest + LIMIT k; @> is element-set containment, so
 * the order the subset comes out in does not matter and no ORDER BY is needed
 * for soundness.  A is drawn NULL-free so the elements compare crisply.
 */
PG_PROP(contains_subset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			n;
	int			k;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	n = draw_arr_opt(tc, &a, false);
	k = (n > 0) ? (int) pg_prop_draw_int(tc, 0, n) : 0;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s) @> ARRAY[]::int[] "
					 "AND (%s) @> (ARRAY(SELECT unnest(%s) LIMIT %d))",
					 a.data, a.data, a.data, k);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Overlap is symmetric: (A && B) = (B && A).  Draw NULL-free elements so &&
 * returns a crisp bool (a NULL element can make && return NULL on a
 * no-overlap), keeping the equality exact.
 */
PG_PROP(overlap_symmetric)
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
	draw_arr_opt(tc, &a, false);
	draw_arr_opt(tc, &b, false);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s) && (%s)) = ((%s) && (%s))",
					 a.data, b.data, b.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Overlap holds iff the two arrays share an element:
 *		(A && B) = EXISTS (SELECT 1 FROM unnest(A) x WHERE x = ANY (B)).
 * Both A and B are drawn NULL-free so && and the ANY scan agree with no
 * three-valued-logic slack.
 */
PG_PROP(overlap_iff_common)
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
	draw_arr_opt(tc, &a, false);
	draw_arr_opt(tc, &b, false);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) && (%s)) "
					 "= EXISTS (SELECT 1 FROM unnest(%s) x WHERE x = ANY (%s))",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Equality is element-wise, for two equal-length arrays:
 *		(A = B) = NOT EXISTS (SELECT 1 FROM unnest(A, B) AS u(x, y)
 *							   WHERE x IS DISTINCT FROM y).
 * unnest of two arrays requires equal length (otherwise it pads with NULL),
 * so A and B are drawn with the same length m of non-NULL elements; IS
 * DISTINCT FROM makes the comparison total.
 */
PG_PROP(equality_elementwise)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;
	int			m;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	m = (int) pg_prop_draw_int(tc, 0, 6);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_arr_fixed(tc, &a, m);
	draw_arr_fixed(tc, &b, m);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) = (%s)) "
					 "= NOT EXISTS (SELECT 1 FROM unnest(%s, %s) AS u(x, y) "
					 "WHERE x IS DISTINCT FROM y)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concatenation contains both operands' elements:
 *		(A || B) @> A AND (A || B) @> B.
 * Containment is element-set membership and A, B are NULL-free so both @>
 * tests are crisp; concat places every element of A and of B into the
 * result, so each is contained.
 */
PG_PROP(concat_contains_both)
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
	draw_arr_opt(tc, &a, false);
	draw_arr_opt(tc, &b, false);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s) || (%s)) @> (%s) AND ((%s) || (%s)) @> (%s)",
					 a.data, b.data, a.data, a.data, b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Array < is lexicographic: fixed anchors ARRAY[1,2] < ARRAY[1,3] (first
 * differing element decides) and ARRAY[1] < ARRAY[1,2] (a proper prefix is
 * less than its extension), plus a drawn case where P is a strict prefix of
 * P || [tail] with tail a non-empty NULL-free suffix, so P < P || tail must
 * hold.  All elements are NULL-free so the b-tree comparator sees total order.
 */
PG_PROP(array_lt_lexicographic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData p,
				tail,
				q;
	int			plen;
	int			tlen;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	plen = (int) pg_prop_draw_int(tc, 0, 4);
	tlen = (int) pg_prop_draw_int(tc, 1, 3);	/* non-empty tail */

	initStringInfo(&p);
	initStringInfo(&tail);
	draw_arr_fixed(tc, &p, plen);
	draw_arr_fixed(tc, &tail, tlen);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ARRAY[1,2]::int[] < ARRAY[1,3]::int[] "
					 "AND ARRAY[1]::int[] < ARRAY[1,2]::int[] "
					 "AND (%s) < ((%s) || (%s))",
					 p.data, p.data, tail.data);

	require_true(tc, conn, q.data);

	pfree(p.data);
	pfree(tail.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The empty array is contained in every array: ARRAY[]::int[] <@ A.  <@ is
 * the mirror of @>, and containment of the empty set is vacuously true, so
 * this holds even when A itself contains NULL elements.
 */
PG_PROP(empty_contained_in_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	/* <@ errors on NULL elements, so draw a NULL-free array of 0..6 elements */
	draw_arr_fixed(tc, &a, (int) pg_prop_draw_int(tc, 0, 6));

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ARRAY[]::int[] <@ (%s)", a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
