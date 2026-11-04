/*-------------------------------------------------------------------------
 *
 * prop_aggdedup.c
 *	  Property-based tests for aggregate ALL/DISTINCT duplicate handling and
 *	  GROUP-wide counting identities.
 *
 * ALL is the default duplicate-handling for aggregates, so agg(ALL x) must
 * equal agg(x); DISTINCT within an aggregate must agree with an explicit
 * SELECT DISTINCT, and must be insensitive to a redundant inner ORDER BY;
 * per-group counts must sum to the whole-table count; and count(DISTINCT
 * composite) is bounded by count(*).  Each is checked against an independent
 * SQL oracle over a tiny table ad2_t(id, g, v) whose small value domain forces
 * heavy duplication, so any dedup/ALL mishandling shows up.
 *
 * This area is deliberately disjoint from "aggregate", "aggdistinct" and
 * "grouphaving": here the subject is specifically the ALL-vs-DISTINCT default
 * equivalence and the whole-vs-grouped counting identities, not DISTINCT
 * bounds (aggdistinct) nor HAVING/grouping rewrites (grouphaving).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_aggdedup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "aggdedup"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is a failure, reported with the SQL.
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
 * Differential result-set oracle (shape B): q1 and q2 must return identical
 * rows in identical order.  Both callers supply a TOTAL ORDER BY so the
 * row-major string compare behaves as a multiset compare.
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
 * Build ad2_t(id int PRIMARY KEY, g int, v int) with 0..30 rows.  id is a
 * dense surrogate key, g is drawn in [0, 3] (a few groups), and v is drawn in
 * [0, 4] (tiny value domain, so distinct values are far fewer than rows and
 * dedup is observable).  v is non-negative and never NULL, so count(v) =
 * count(*) and the ALL-vs-DISTINCT reasoning is clean.  Zero rows exercises
 * the empty-aggregate path.  ANALYZE lets the planner pick sort or hash
 * aggregation.  Rebuilt fresh in each property so cases are independent.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ad2_t; "
						   "CREATE TABLE ad2_t (id int PRIMARY KEY, "
						   "g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int r = 0; r < nrows; r++)
		appendStringInfo(&ddl, "INSERT INTO ad2_t VALUES (%d, %d, %d); ",
						 r,
						 (int) pg_prop_draw_int(tc, 0, 3),
						 (int) pg_prop_draw_int(tc, 0, 4));

	appendStringInfoString(&ddl, "ANALYZE ad2_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/*
 * ALL is the default duplicate-handling, so count(ALL v) must equal count(v):
 * writing ALL explicitly changes nothing.  Both ignore NULL (v is non-NULL
 * here anyway).  Empty table: 0 = 0.
 */
PG_PROP(count_all_eq_count_star)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(ALL v) = count(v) FROM ad2_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad2_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * sum(ALL v) must equal sum(v): ALL is the default, so making it explicit does
 * not change the sum.  Both are NULL over an empty table, and NULL = NULL
 * would be NULL -- but every input here has either both NULL (empty) or both
 * equal, so IS NOT DISTINCT FROM covers the empty case soundly.
 */
PG_PROP(sum_all_eq_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT sum(ALL v) IS NOT DISTINCT FROM sum(v) FROM ad2_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad2_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * Per-group count(DISTINCT v) equals the distinct count computed by a
 * correlated subquery over the distinct grouping keys.  IS NOT DISTINCT FROM
 * joins the correlation NULL-safely to match GROUP BY semantics (g is
 * non-NULL here, but the form stays sound if that changes).  Both sides ORDER
 * BY g, a total order over the distinct keys, so the row-major compare behaves
 * as a multiset compare.  Empty table: both return zero rows.
 */
PG_PROP(distinct_group_vs_subquery)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT g, count(DISTINCT v) FROM ad2_t "
					  "GROUP BY g ORDER BY g",
					  "SELECT g, (SELECT count(DISTINCT v) FROM ad2_t u "
					  "WHERE u.g IS NOT DISTINCT FROM o.g) "
					  "FROM (SELECT DISTINCT g FROM ad2_t) o ORDER BY g");

	pg_prop_sql_exec(conn, "DROP TABLE ad2_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * count(DISTINCT v) is a set cardinality and so cannot depend on a redundant
 * ORDER BY in a subquery that feeds it: ordering the input rows changes
 * nothing about the distinct set.  So count(DISTINCT v) over the table equals
 * the same over an ORDER BY v subquery.  Empty table: 0 = 0.
 */
PG_PROP(agg_distinct_order_irrelevant)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(DISTINCT v) FROM ad2_t) = "
				 "(SELECT count(DISTINCT v) FROM "
				 "(SELECT v FROM ad2_t ORDER BY v) s)");

	pg_prop_sql_exec(conn, "DROP TABLE ad2_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * Several aggregates computed together in one query must be internally
 * consistent: sum(v) equals sum(v) FILTER (WHERE true) (a trivially-true
 * filter removes nothing), and count(*) equals count(v) plus count(*) FILTER
 * (WHERE v IS NULL) (every row is either counted by count(v) or is a NULL-v
 * row; v is non-NULL here so count(v) = count(*) and the FILTER count is 0).
 * IS NOT DISTINCT FROM covers the empty-table sum (both NULL).
 */
PG_PROP(multiple_aggs_consistent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (sum(v) IS NOT DISTINCT FROM "
				 "sum(v) FILTER (WHERE true)) "
				 "AND count(*) = count(v) + "
				 "count(*) FILTER (WHERE v IS NULL) FROM ad2_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad2_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * Partitioning the table by g and summing the per-group counts recovers the
 * whole-table count: every row lands in exactly one group, so the group counts
 * sum to count(*).  coalesce(sum(cnt), 0) makes the empty table (no groups,
 * sum NULL) pass as 0 = 0.
 */
PG_PROP(group_all_rows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce((SELECT sum(cnt) FROM "
				 "(SELECT count(*) cnt FROM ad2_t GROUP BY g) s), 0) = "
				 "(SELECT count(*) FROM ad2_t)");

	pg_prop_sql_exec(conn, "DROP TABLE ad2_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * The number of distinct composite values (g, v) cannot exceed the number of
 * rows: DISTINCT only collapses duplicates.  count(DISTINCT (g,v)) <= count(*).
 * Empty table: 0 <= 0.
 */
PG_PROP(distinct_le_total)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(DISTINCT (g, v)) <= count(*) FROM ad2_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad2_t", &err);
	pg_prop_disconnect(conn);
}
