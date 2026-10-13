/*-------------------------------------------------------------------------
 *
 * prop_intarray.c
 *	  Property-based tests for the intarray contrib extension (int[] ops).
 *
 * intarray adds set/multiset operators on int[]; these check its sort/uniq
 * normalization and the +/-/&/# set operators against core-SQL oracles.
 *
 * intarray redefines operators on int[] (sort, uniq, + union, - difference,
 * & intersection, idx) that core SQL does not provide.  The oracle here is
 * core-SQL set machinery independent of intarray's own operators: multiset
 * sorting via array_agg(x ORDER BY x), distinct-sorting via
 * array_agg(DISTINCT x ORDER BY x), and element membership via core
 * "= ANY(array)".  Any "f" (or an unexpected NULL/error) is a bug in intarray,
 * not a modelling slip -- empty-array edges stay crisp via
 * coalesce(bool_and(...), true) and coalesce(..., '{}'::int[]).
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_intarray.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "intarray"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a random SQL integer-array literal into buf as an ARRAY[...] expression
 * typed ::int[].  n is drawn in 0..8 elements, each a small int in -10..10 so
 * duplicates are likely and the set operators have overlapping operands to
 * work on.  No NULL elements: intarray's operators are undefined for NULLs.
 * The ::int[] cast is always emitted so an empty array (ARRAY[]::int[]) still
 * carries a type.
 */
static void
draw_intarr(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 8);

	appendStringInfoString(buf, "ARRAY[");
	for (int i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, -10, 10));
	}
	appendStringInfoString(buf, "]::int[]");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means an intarray operator errored or produced NULL on valid
 * input, which is itself a bug -- so either is a failure, reported with the
 * SQL and any error text.  Mirrors require_true in prop_numeric.c.
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
 * Connect to the test cluster and ensure the intarray extension exists.  Each
 * property opens its own connection (the Tier-B pattern); CREATE EXTENSION IF
 * NOT EXISTS makes installing it idempotent across connections.  On any
 * failure the property is failed with the message.
 */
static pg_prop_conn *
connect_intarray(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "CREATE EXTENSION IF NOT EXISTS intarray",
						  &err))
		pg_prop_fail(tc, "setup failed (intarray): %s", err ? err : "");

	return conn;
}

/*
 * sort(A) is an ordered permutation of A: it has the same elements in
 * ascending order.  array_agg(x ORDER BY x) over unnest(A) is an independent
 * sort of the same multiset (core SQL, not intarray), so it must equal
 * sort(A).  This catches an out-of-order or element-dropping bug in intarray's
 * sort.
 */
PG_PROP(sort_is_ordered_permutation)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	draw_intarr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce((SELECT array_agg(x ORDER BY x) FROM unnest(%s) x), "
					 "'{}'::int[]) = sort(%s)",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * uniq(sort(A)) is the sorted distinct elements of A.  uniq removes only
 * ADJACENT duplicates, so it must be applied to sort(A); the oracle is
 * core-SQL array_agg(DISTINCT x ORDER BY x).  array_agg over no rows is NULL,
 * so the empty case is reconciled with coalesce(..., '{}'::int[]) -- intarray
 * yields '{}' for uniq(sort('{}')).
 */
PG_PROP(uniq_sort_is_distinct_sorted)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	draw_intarr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT uniq(sort(%s)) "
					 "= coalesce((SELECT array_agg(DISTINCT x ORDER BY x) "
					 "            FROM unnest(%s) x), '{}'::int[])",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The intarray union A + B contains every element of A (and, by symmetry of
 * the drawn operands over many cases, of B).  The oracle is core membership
 * "= ANY (A + B)" over each element of A via unnest -- independent of
 * intarray's own containment operators.  coalesce(bool_and(...), true) makes
 * an empty A pass (no rows to check).
 */
PG_PROP(union_contains_both)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_intarr(tc, &a);
	draw_intarr(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(bool_and(e = ANY (%s + %s)), true) "
					 "FROM unnest(%s) e",
					 a.data, b.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The intarray intersection A & B contains only elements present in BOTH
 * operands: every element of the intersection is a member of A and of B
 * (core "= ANY").  coalesce(bool_and(...), true) makes an empty intersection
 * pass.  A spurious element in the intersection fails here.
 */
PG_PROP(intersection_subset)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_intarr(tc, &a);
	draw_intarr(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(bool_and(e = ANY (%s) AND e = ANY (%s)), true) "
					 "FROM unnest(%s & %s) e",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The intarray difference A - B shares no element with B: every element of
 * A - B is NOT a member of B (core "= ANY").  coalesce(bool_and(...), true)
 * makes an empty difference pass.  A B-element leaking into the difference
 * fails here.
 */
PG_PROP(difference_removes_b)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_intarr(tc, &a);
	draw_intarr(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(bool_and(NOT (e = ANY (%s))), true) "
					 "FROM unnest(%s - %s) e",
					 b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * idx(A, v) returns the 1-based position of v in A, or 0 if v is absent.  v is
 * drawn in -12..12 (wider than the element range -10..10) so it is sometimes
 * outside the array.  When v is present, A[idx(A, v)] must be v; when absent,
 * idx must be 0.  The CASE picks the right check via core "= ANY".  Empty A is
 * covered: v is not ANY(A), so the ELSE branch requires idx = 0, which holds.
 */
PG_PROP(idx_finds_member)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;
	int			v;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	draw_intarr(tc, &a);
	v = (int) pg_prop_draw_int(tc, -12, 12);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN %d = ANY (%s) "
					 "THEN (%s)[idx(%s, %d)] = %d "
					 "ELSE idx(%s, %d) = 0 END",
					 v, a.data, a.data, a.data, v, v, a.data, v);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
