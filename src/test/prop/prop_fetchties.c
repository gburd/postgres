/*-------------------------------------------------------------------------
 *
 * prop_fetchties.c
 *	  Property-based tests for FETCH FIRST ... WITH TIES, LIMIT and OFFSET.
 *
 * The LIMIT/OFFSET executor node (nodeLimit.c) backs several surface syntaxes
 * that must agree: LIMIT k and FETCH FIRST k ROWS ONLY are the same thing, as
 * are OFFSET o FETCH NEXT l and OFFSET o LIMIT l.  FETCH FIRST k ROWS WITH
 * TIES additionally keeps every row tied with the k-th on the ORDER BY key, so
 * its result is a superset of the ONLY result and never pushes a strictly
 * larger key past the boundary.  A disagreement between two spellings, or a
 * WITH TIES set that drops below k rows or admits a larger key, is an executor
 * bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see pg_prop.h
 * and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_fetchties.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "fetchties"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Build ft_t(id int PRIMARY KEY, v int) with 0..30 rows: id is a dense 0-based
 * sequence (a total order), v is drawn from 0..3 so ties on v are frequent --
 * exactly what the WITH TIES boundary logic has to get right.  Rebuilt fresh
 * in each property, then ANALYZEd so the planner has stats.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 30);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ft_t; "
						   "CREATE TABLE ft_t(id int PRIMARY KEY, v int); ");
	for (int i = 0; i < nrows; i++)
		appendStringInfo(&ddl, "INSERT INTO ft_t VALUES (%d, %d); ",
						 i, (int) pg_prop_draw_int(tc, 0, 3));
	appendStringInfoString(&ddl, "ANALYZE ft_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * SQL error is itself a failure.  (Same shape as prop_numeric.c.)
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
 * Run two queries and require identical row-major result strings.  Both
 * queries must carry a TOTAL ORDER BY (ORDER BY v, id here) so the comparison
 * is deterministic.  (Same shape as the area spec's differential oracle.)
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
 * LIMIT k and FETCH FIRST k ROWS ONLY are the same construct; over a total
 * order (v, id) they must return exactly the same rows.  k in 0..30.
 */
PG_PROP(limit_eq_fetch_first)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k;
	StringInfoData q1,
				q2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	k = (int) pg_prop_draw_int(tc, 0, 30);

	initStringInfo(&q1);
	initStringInfo(&q2);
	appendStringInfo(&q1, "SELECT id FROM ft_t ORDER BY v, id LIMIT %d", k);
	appendStringInfo(&q2,
					 "SELECT id FROM ft_t ORDER BY v, id FETCH FIRST %d ROWS ONLY",
					 k);

	require_same_rows(tc, conn, q1.data, q2.data);

	pfree(q1.data);
	pfree(q2.data);
	pg_prop_disconnect(conn);
}

/*
 * FETCH FIRST k ROWS WITH TIES returns at least k rows (or all of them if the
 * table has fewer than k): ties can only add rows, never drop below the ONLY
 * count.  ORDER BY v only, so ties on v are what matter.  k in 1..10.
 */
PG_PROP(with_ties_superset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	k = (int) pg_prop_draw_int(tc, 1, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM "
					 "(SELECT v FROM ft_t ORDER BY v FETCH FIRST %d ROWS WITH TIES) s) "
					 ">= least(%d, (SELECT count(*) FROM ft_t))",
					 k, k);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * WITH TIES never admits a strictly larger ORDER BY key than ONLY: the max v
 * in the WITH TIES set equals the max v in the ONLY set (ties sit exactly at
 * the boundary, never above it).  coalesce(..., true) makes the empty-table
 * case (both maxima NULL) pass soundly.  ORDER BY v only.  k in 1..10.
 */
PG_PROP(with_ties_all_tied_at_boundary)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	k = (int) pg_prop_draw_int(tc, 1, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce("
					 "(SELECT max(v) FROM "
					 "(SELECT v FROM ft_t ORDER BY v FETCH FIRST %d ROWS WITH TIES) a) "
					 "= (SELECT max(v) FROM "
					 "(SELECT v FROM ft_t ORDER BY v FETCH FIRST %d ROWS ONLY) b), true)",
					 k, k);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * OFFSET o LIMIT l over a total order returns exactly greatest(0, least(l,
 * total - o)) rows.  o, l in 0..35 so both can exceed the row count.
 */
PG_PROP(offset_limit_total)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			o,
				l;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	o = (int) pg_prop_draw_int(tc, 0, 35);
	l = (int) pg_prop_draw_int(tc, 0, 35);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM "
					 "(SELECT id FROM ft_t ORDER BY v, id OFFSET %d LIMIT %d) s) "
					 "= greatest(0, least(%d, (SELECT count(*) FROM ft_t) - %d))",
					 o, l, l, o);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * LIMIT 0 returns no rows: the count is exactly 0, independent of the table.
 */
PG_PROP(limit_zero_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT (SELECT count(*) FROM "
						   "(SELECT id FROM ft_t ORDER BY v, id LIMIT 0) s) = 0");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * OFFSET o FETCH NEXT l ROWS ONLY and OFFSET o LIMIT l are the same construct;
 * over a total order they must return exactly the same rows (fixed o=2, l=3).
 */
PG_PROP(fetch_next_eq_offset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM ft_t ORDER BY v, id OFFSET 2 FETCH NEXT 3 ROWS ONLY",
					  "SELECT id FROM ft_t ORDER BY v, id OFFSET 2 LIMIT 3");

	pg_prop_disconnect(conn);
}
