/*-------------------------------------------------------------------------
 *
 * prop_valuesrows.c
 *	  Property-based tests for VALUES lists, parallel unnest, and WITH
 *	  ORDINALITY as relation sources.
 *
 * These exercise the executor's relation-generating nodes: ValuesScan, the
 * SRF-in-FROM machinery (unnest, generate_series), WITH ORDINALITY, and the
 * array <-> relation round-trip.  The oracle is differential or a round-trip:
 * a VALUES list must produce the same rows as an equivalent UNION ALL;
 * parallel unnest of two arrays must pair elements positionally; array_agg of
 * an unnested array (ordered by ordinality) must reconstruct the array.  Any
 * disagreement is a wrong-results bug in those nodes.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_valuesrows.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "valuesrows"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is itself a failure, reported with the SQL.
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * Run two result-set queries and require their row-major renderings to match.
 * Both queries MUST carry a total ORDER BY so the comparison is a multiset
 * comparison over a deterministic row order.
 */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *q1, const char *q2)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *r1,
			   *r2;

	r1 = pg_prop_sql_rows(conn, q1, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "q1 failed: %s\n  SQL: %s", e1 ? e1 : "", q1);
	r2 = pg_prop_sql_rows(conn, q2, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "q2 failed: %s\n  SQL: %s", e2 ? e2 : "", q2);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc, "mismatch:\n  q1: %s\n  q2: %s\n  r1:\n%s\n  r2:\n%s",
					 q1, q2, r1, r2);
	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * A VALUES list equals the equivalent UNION ALL of single-row SELECTs.  Both
 * sides are the same three rows; the ValuesScan node must produce them just
 * as the Append-of-Result plan does.
 */
PG_PROP(values_eq_union_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_same_rows(tc, conn,
					  "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) v(n,s) ORDER BY n",
					  "SELECT 1,'a' UNION ALL SELECT 2,'b' UNION ALL SELECT 3,'c' ORDER BY 1");

	pg_prop_disconnect(conn);
}

/*
 * Parallel unnest of two arrays pairs elements positionally, so unnest of
 * ARRAY[1,2,3] and ARRAY['a','b','c'] yields the same (n,s) rows as the
 * equivalent VALUES list.
 */
PG_PROP(unnest_two_arrays_parallel)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_same_rows(tc, conn,
					  "SELECT * FROM unnest(ARRAY[1,2,3], ARRAY['a','b','c']) AS u(n,s) ORDER BY n",
					  "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) v(n,s) ORDER BY n");

	pg_prop_disconnect(conn);
}

/*
 * WITH ORDINALITY numbers rows 1..N in element order, so the ordinality
 * column equals the row_number() over that same order for every row.
 */
PG_PROP(unnest_ordinality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(ord = pos), true) FROM "
				 "(SELECT ord, row_number() OVER (ORDER BY ord) pos "
				 "FROM unnest(ARRAY[10,20,30]) WITH ORDINALITY AS u(x,ord)) s");

	pg_prop_disconnect(conn);
}

/*
 * unnest then re-aggregate round-trips: for a drawn int array a, array_agg of
 * the unnested elements ordered by ordinality reconstructs a exactly.  The
 * empty array is guarded with coalesce so a 0-row array_agg (SQL NULL)
 * compares equal to the empty-array literal.
 */
PG_PROP(array_unnest_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData arr;
	StringInfoData q;
	int			nelem;
	int			i;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* Build an int array literal with 0..6 elements, each a drawn int. */
	initStringInfo(&arr);
	appendStringInfoString(&arr, "ARRAY[");
	nelem = (int) pg_prop_draw_int(tc, 0, 6);
	for (i = 0; i < nelem; i++)
	{
		if (i > 0)
			appendStringInfoString(&arr, ",");
		appendStringInfo(&arr, "(%d)", (int) pg_prop_draw_int(tc, -1000, 1000));
	}
	appendStringInfoString(&arr, "]::int[]");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(array_agg(x ORDER BY ord), '{}'::int[]) = %s "
					 "FROM unnest(%s) WITH ORDINALITY AS u(x,ord)",
					 arr.data, arr.data);

	require_true(tc, conn, q.data);

	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A VALUES list with k rows has exactly k rows.  k is drawn 1..6 and the k
 * rows (each a single integer i) are built in C.
 */
PG_PROP(values_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			k;
	int			i;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	k = (int) pg_prop_draw_int(tc, 1, 6);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (SELECT count(*) FROM (VALUES ");
	for (i = 1; i <= k; i++)
	{
		if (i > 1)
			appendStringInfoString(&q, ",");
		appendStringInfo(&q, "(%d)", i);
	}
	appendStringInfo(&q, ") v) = %d", k);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * generate_series as a relation source equals the equivalent VALUES list.
 * Both yield the rows 1,2,3; the FunctionScan over generate_series and the
 * ValuesScan must agree row for row.
 */
PG_PROP(gen_series_eq_values)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_same_rows(tc, conn,
					  "SELECT g FROM generate_series(1,3) g ORDER BY g",
					  "SELECT * FROM (VALUES (1),(2),(3)) v(g) ORDER BY g");

	pg_prop_disconnect(conn);
}
