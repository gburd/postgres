/*-------------------------------------------------------------------------
 *
 * prop_arraysort.c
 *	  Property-based tests for array sorting and ordering.
 *
 * Sorting is expressed as array_agg(x ORDER BY x) over unnest(a): the ordered
 * aggregate materialises a sorted copy of the multiset of elements.  The
 * oracle is what a correct sort must satisfy -- the result is non-decreasing,
 * is a permutation of the input (same cardinality and sum), is idempotent,
 * DESC is the reverse of ASC, the first/last sorted elements are the min/max,
 * and lexicographic array ordering is consistent with element ordering.  Any
 * "f" result is a wrong-results bug in the ordered aggregate or in array
 * comparison, a concern distinct from the arrayops/arraycmp/arrayreduce/
 * arrayconstruct areas.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_arraysort.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "arraysort"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a NULL-free integer-array literal into buf as ARRAY[...]::int[], with
 * 0..6 elements each drawn from -5..5.  Each element is emitted parenthesized
 * as "(%d)" so a negative literal never misparses when substituted into a
 * larger expression (bare -N::int4 would parse as -(N::int4)).  The cast gives
 * an empty array (ARRAY[]::int[]) a type.  No NULL elements: the ordering
 * properties below rely on crisp comparison with no NULL sort placement.
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
		appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(buf, "]::int[]");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the ordered aggregate or array comparison errored or
 * produced NULL on a well-formed input, which is itself a bug -- so either is
 * a failure, reported with the SQL.  (Copied from prop_numeric.c's
 * require_true.)
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
 * The sorted array is non-decreasing.  s = array_agg(x ORDER BY x) over
 * unnest(a); bool_and over every adjacent pair s[i] <= s[i+1] must hold.  The
 * aggregate over 0 rows (empty a) makes s NULL and the subscripts set empty,
 * so coalesce the pairwise AND to true: a 0- or 1-element array is trivially
 * ordered.
 */
PG_PROP(sort_is_ordered)
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
					 "(SELECT bool_and(s[i] <= s[i+1]) "
					 "FROM generate_subscripts(s, 1) i "
					 "WHERE i < cardinality(s)), true) "
					 "FROM (SELECT array_agg(x ORDER BY x) s "
					 "FROM unnest(%s) x) q",
					 a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Sorting preserves the multiset: the sorted array is a permutation of a, so
 * it has the same cardinality and the same element sum.  (Cardinality and sum
 * together do not prove a permutation in general, but combined with
 * sort_is_ordered they are a strong, always-sound check against elements being
 * dropped, duplicated, or altered by the sort.)  coalesce handles the empty
 * array: cardinality 0 and sum NULL -> 0 on both sides.
 */
PG_PROP(sort_permutation)
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
					 "SELECT cardinality(s) = cardinality(%s) "
					 "AND coalesce((SELECT sum(e) FROM unnest(s) e), 0) "
					 "= coalesce((SELECT sum(e) FROM unnest(%s) e), 0) "
					 "FROM (SELECT coalesce("
					 "(SELECT array_agg(x ORDER BY x) FROM unnest(%s) x), "
					 "'{}'::int[]) s) q",
					 a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Sorting is idempotent: sorting an already-sorted array yields the same
 * array.  s = sorted(a); sorted(s) must equal s.  Both aggregates are
 * coalesced to '{}'::int[] so the empty case compares two empty arrays.
 */
PG_PROP(sort_idempotent)
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
					 "(SELECT array_agg(y ORDER BY y) FROM unnest(s) y), "
					 "'{}'::int[]) = s "
					 "FROM (SELECT coalesce("
					 "(SELECT array_agg(x ORDER BY x) FROM unnest(%s) x), "
					 "'{}'::int[]) s) q",
					 a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * DESC is the reverse of ASC: with asc = array_agg(x ORDER BY x) and desc =
 * array_agg(x ORDER BY x DESC), element i of desc equals element (card-i+1) of
 * asc for every i.  bool_and over all subscripts of desc checks the reversal;
 * coalesce to true covers the empty array (no subscripts).
 */
PG_PROP(desc_is_reverse)
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
					 "(SELECT bool_and(desc_a[i] = asc_a[cardinality(asc_a) - i + 1]) "
					 "FROM generate_subscripts(desc_a, 1) i), true) "
					 "FROM (SELECT "
					 "coalesce((SELECT array_agg(x ORDER BY x) "
					 "FROM unnest(%s) x), '{}'::int[]) asc_a, "
					 "coalesce((SELECT array_agg(x ORDER BY x DESC) "
					 "FROM unnest(%s) x), '{}'::int[]) desc_a) q",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The first element of the ascending sort is the minimum element.  Guarded to
 * non-empty a (cardinality > 0): over an empty array both the sorted array and
 * min() are NULL and the subscript [1] is undefined, so the property is
 * vacuously true -- the CASE returns true without indexing.
 */
PG_PROP(min_is_first_sorted)
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
					 "SELECT CASE WHEN cardinality(%s) = 0 THEN true "
					 "ELSE (SELECT array_agg(x ORDER BY x) FROM unnest(%s) x)[1] "
					 "= (SELECT min(e) FROM unnest(%s) e) END",
					 a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The last element of the ascending sort is the maximum element.  Guarded to
 * non-empty a as in min_is_first_sorted; the last index is cardinality(a).
 */
PG_PROP(max_is_last_sorted)
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
					 "SELECT CASE WHEN cardinality(%s) = 0 THEN true "
					 "ELSE (SELECT array_agg(x ORDER BY x) FROM unnest(%s) x)"
					 "[cardinality(%s)] "
					 "= (SELECT max(e) FROM unnest(%s) e) END",
					 a.data, a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Lexicographic array ordering is consistent with the first-differing
 * element: appending a strictly-larger element to a sorted array makes it
 * compare strictly greater.  s = sorted(a); 6 is a guaranteed-absent sentinel
 * (elements are -5..5), so s is a prefix of s || ARRAY[6] whose next element
 * (6) is larger than any element of s -- therefore s < s || ARRAY[6] always,
 * and s <= s || ARRAY[6] holds for every a (including the empty array, where
 * '{}' < ARRAY[6]).  Checking <= keeps the property sound at the empty
 * boundary while still exercising the comparison.
 */
PG_PROP(array_lt_consistent)
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
					 "SELECT s <= s || ARRAY[6] "
					 "FROM (SELECT coalesce("
					 "(SELECT array_agg(x ORDER BY x) FROM unnest(%s) x), "
					 "'{}'::int[]) s) q",
					 a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
