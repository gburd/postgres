/*-------------------------------------------------------------------------
 *
 * prop_windowagg.c
 *	  Property-based tests for window aggregates vs GROUP BY aggregates.
 *
 * A window aggregate with PARTITION BY g must compute, on every row of a
 * group, the same value a plain GROUP BY of that aggregate computes for the
 * group.  The two paths run entirely different executor nodes (WindowAgg vs
 * grouped Agg/HashAgg), so a divergence is a wrong-results bug in one of
 * them.  This area is deliberately distinct from the window/windowframes/
 * windowrank/windowexcl areas: it tests only the aggregate-equals-group
 * equivalence, not frame mechanics or ranking.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see pg_prop.h
 * and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_windowagg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "windowagg"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is itself a failure (the oracle expression is written to be
 * total: coalesce()/bool_and over an empty set yields "t").
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
 * Compare two result sets for exact row-major equality.  Both queries MUST
 * carry a total ORDER BY so the string compare is a sound multiset compare.
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
 * Build wa_t(id int PRIMARY KEY, g int, v int) with 0..25 rows, g in 0..3,
 * v in -5..5 and never NULL.  v is kept non-NULL so sum/avg/max/min are
 * exact and the window-vs-group equivalence has no NULL-handling asymmetry.
 * ANALYZE so the planner can pick its real plan for each path.  Rebuilt fresh
 * in every property.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS wa_t; "
						   "CREATE TABLE wa_t(id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int i = 0; i < nrows; i++)
		appendStringInfo(&ddl,
						 "INSERT INTO wa_t VALUES (%d, %d, %d); ",
						 i,
						 (int) pg_prop_draw_int(tc, 0, 3),
						 (int) pg_prop_draw_int(tc, -5, 5));

	appendStringInfoString(&ddl, "ANALYZE wa_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build failed: %s", err ? err : "");
	pfree(ddl.data);
}

/*
 * 1. sum(v) OVER (PARTITION BY g) equals the group's GROUP BY sum, on every
 *    row.  Join the per-row window result to the grouped sum on g and require
 *    they agree everywhere; coalesce makes the empty table pass.
 */
PG_PROP(window_sum_eq_group_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(ws = gs), true) FROM "
				 "(SELECT g, sum(v) OVER (PARTITION BY g) ws FROM wa_t) w "
				 "JOIN (SELECT g, sum(v) gs FROM wa_t GROUP BY g) a "
				 "USING (g)");

	pg_prop_disconnect(conn);
}

/*
 * 2. count(*) OVER (PARTITION BY g) equals the group's GROUP BY count, on
 *    every row.
 */
PG_PROP(window_count_eq_group_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(wc = gc), true) FROM "
				 "(SELECT g, count(*) OVER (PARTITION BY g) wc FROM wa_t) w "
				 "JOIN (SELECT g, count(*) gc FROM wa_t GROUP BY g) a "
				 "USING (g)");

	pg_prop_disconnect(conn);
}

/*
 * 3. avg(v) OVER (PARTITION BY g) equals the group's GROUP BY avg.  avg over
 *    integers yields numeric and both paths average the identical multiset,
 *    so the result is exact; IS NOT DISTINCT FROM compares them total (no NULL
 *    special-casing needed, but it keeps the oracle sound at any boundary).
 */
PG_PROP(window_avg_eq_group_avg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(wa IS NOT DISTINCT FROM ga), true) FROM "
				 "(SELECT g, avg(v) OVER (PARTITION BY g) wa FROM wa_t) w "
				 "JOIN (SELECT g, avg(v) ga FROM wa_t GROUP BY g) a "
				 "USING (g)");

	pg_prop_disconnect(conn);
}

/*
 * 4. max(v) and min(v) OVER (PARTITION BY g) equal the group's GROUP BY max
 *    and min, on every row.
 */
PG_PROP(window_max_min)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(wmax = gmax AND wmin = gmin), true) FROM "
				 "(SELECT g, max(v) OVER (PARTITION BY g) wmax, "
				 "min(v) OVER (PARTITION BY g) wmin FROM wa_t) w "
				 "JOIN (SELECT g, max(v) gmax, min(v) gmin FROM wa_t GROUP BY g) a "
				 "USING (g)");

	pg_prop_disconnect(conn);
}

/*
 * 5. sum(v) OVER () with no partition equals the grand total sum on every
 *    row.  coalesce guards the empty table (both the per-row set and the
 *    scalar subquery are then NULL/empty).
 */
PG_PROP(unpartitioned_window_eq_total)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(ws = (SELECT sum(v) FROM wa_t)), true) "
				 "FROM (SELECT sum(v) OVER () ws FROM wa_t) w");

	pg_prop_disconnect(conn);
}

/*
 * 6. A running row count within a group never exceeds the group's total row
 *    count: count(*) OVER (PARTITION BY g ORDER BY id ROWS UNBOUNDED PRECEDING)
 *    <= count(*) OVER (PARTITION BY g), on every row.  (A running sum would
 *    not be monotone because v can be negative, so count is used instead.)
 */
PG_PROP(running_count_le_total)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(rc <= tc), true) FROM "
				 "(SELECT count(*) OVER (PARTITION BY g ORDER BY id "
				 "ROWS UNBOUNDED PRECEDING) rc, "
				 "count(*) OVER (PARTITION BY g) tc FROM wa_t) w");

	pg_prop_disconnect(conn);
}

/*
 * 7. The distinct (g, window-sum) pairs equal the grouped (g, sum) rows.  A
 *    window sum partitioned by g produces one value per group repeated across
 *    its rows, so DISTINCT collapses it to exactly the GROUP BY result.  Both
 *    sides carry a total ORDER BY g (g is unique after the grouping/distinct),
 *    so this is a sound differential oracle.
 */
PG_PROP(distinct_in_window_vs_group)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT DISTINCT g, sum(v) OVER (PARTITION BY g) "
					  "FROM wa_t ORDER BY g",
					  "SELECT g, sum(v) FROM wa_t GROUP BY g ORDER BY g");

	pg_prop_disconnect(conn);
}
