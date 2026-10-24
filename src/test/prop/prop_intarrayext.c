/*-------------------------------------------------------------------------
 *
 * prop_intarrayext.c
 *	  Property-based tests for intarray's own operator algebra (int[] ops).
 *
 * A companion to prop_intarray.c, which checks intarray against core-SQL
 * oracles.  This area instead exercises intarray's own operators as a closed
 * algebra: icount, sort, uniq, idx and the operators + - & | @> <@.  Each
 * property states a relation that intarray's operators must satisfy among
 * themselves (and against core cardinality/count), so a drop/dup/order bug or
 * a wrong containment answer surfaces as an "f".
 *
 * intarray requires non-NULL elements, so arrays are built with 0..6 small
 * positive ints (0..9); duplicates are likely so the set operators have real
 * work.  Any "f" (or an unexpected NULL/error) on such input is an intarray
 * bug, not a modelling slip -- empty-array edges stay crisp via
 * coalesce(..., true).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_intarrayext.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "intarrayext"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a random intarray literal into buf as an ARRAY[...] expression typed
 * ::int[].  n is drawn in 0..6 elements, each a small positive int in 0..9 so
 * duplicates are likely and a counterexample stays legible.  No NULL elements:
 * intarray's operators are undefined for NULLs.  The ::int[] cast is always
 * emitted so an empty array (ARRAY[]::int[]) still carries a type.
 */
static void
draw_intarr(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 6);

	appendStringInfoString(buf, "ARRAY[");
	for (int i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, 0, 9));
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
 * NOT EXISTS makes installing it idempotent across the thousands of per-case
 * connections.  On any failure the property is failed with the message.
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
 * icount(A) counts the elements of A.  With no NULL elements that must equal
 * core cardinality(A), which counts non-NULL elements.  A disagreement means
 * icount miscounts (e.g. drops or double-counts an element).
 */
PG_PROP(count_matches_cardinality)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	draw_intarr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT icount(%s) = cardinality(%s)",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * uniq(sort(A)) is the sorted DISTINCT elements of A, so its element count
 * must equal the number of distinct values in A.  icount of the normalized
 * array is compared with core count(DISTINCT x) over unnest(A).  uniq removes
 * only ADJACENT duplicates, hence it must be applied to sort(A); a dup left
 * behind (or a value wrongly collapsed) breaks the count.
 */
PG_PROP(sort_uniq_distinct)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	draw_intarr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT icount(uniq(sort(%s))) "
					 "= (SELECT count(DISTINCT x) FROM unnest(%s) x)",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The intarray union A | B contains both operands: (A | B) @> A and
 * (A | B) @> B, using intarray's own containment operator @>.  Union must not
 * drop an element of either side; a lost element fails one of the two @>.
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
					 "SELECT (%s | %s) @> %s AND (%s | %s) @> %s",
					 a.data, b.data, a.data, a.data, b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The intarray intersection A & B is contained in both operands:
 * (A & B) <@ A and (A & B) <@ B, using intarray's own containment operator
 * <@.  A spurious element in the intersection -- one not in both sides --
 * fails one of the two <@.
 */
PG_PROP(intersect_subset)
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
					 "SELECT (%s & %s) <@ %s AND (%s & %s) <@ %s",
					 a.data, b.data, a.data, a.data, b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The intarray push operator A + e appends element e, so the result must
 * contain e: (A + e) @> ARRAY[e].  e is drawn in 0..9 (the element range) so
 * it is often already present -- containment holds either way.  A push that
 * loses the appended value fails here.
 */
PG_PROP(add_element)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;
	int			e;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	draw_intarr(tc, &a);
	e = (int) pg_prop_draw_int(tc, 0, 9);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s + %d) @> ARRAY[%d]",
					 a.data, e, e);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The intarray difference A - e removes ALL occurrences of scalar e, so the
 * result must not contain e: NOT ((A - e) @> ARRAY[e]).  e is drawn in 0..9 so
 * it is often present in A, making the removal non-trivial.  A leftover copy
 * of e fails here.
 */
PG_PROP(remove_element)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;
	int			e;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	draw_intarr(tc, &a);
	e = (int) pg_prop_draw_int(tc, 0, 9);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT NOT ((%s - %d) @> ARRAY[%d])",
					 a.data, e, e);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * After pushing e, idx must locate it: idx(A + e, e) > 0.  idx returns the
 * 1-based position of e, or 0 if absent; since A + e just appended e, idx must
 * report a positive position.  e is drawn in 0..9.  A push/idx disagreement
 * (element present but idx = 0) fails here.
 */
PG_PROP(idx_finds)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;
	int			e;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	draw_intarr(tc, &a);
	e = (int) pg_prop_draw_int(tc, 0, 9);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT idx(%s + %d, %d) > 0",
					 a.data, e, e);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * sort(A) is non-decreasing: for the sorted array s, s[i] <= s[i+1] for every
 * adjacent pair.  bool_and over generate_subscripts(s,1) checks each pair;
 * coalesce(..., true) makes the empty/single-element case (no adjacent pair)
 * pass.  An out-of-order element in sort's output fails here.
 */
PG_PROP(sort_ordered)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;

	conn = connect_intarray(tc);

	initStringInfo(&a);
	draw_intarr(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce((SELECT bool_and(s[i] <= s[i+1]) "
					 "FROM generate_subscripts(s, 1) i "
					 "WHERE i < icount(s)), true) "
					 "FROM (SELECT sort(%s) AS s) q",
					 a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
