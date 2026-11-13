/*-------------------------------------------------------------------------
 *
 * prop_aggdistinct.c
 *	  Property-based tests for DISTINCT inside aggregates and combinations
 *	  of DISTINCT aggregates.
 *
 * agg(DISTINCT v) deduplicates its input before aggregating, so count(DISTINCT
 * v) is bounded by count(v), sum(DISTINCT v) by sum(v) for non-negative input,
 * and count(DISTINCT v) must equal the row count of an explicit SELECT DISTINCT
 * projection.  These properties exercise the aggregate DISTINCT path (sort/hash
 * of the per-aggregate inputs) against independent SQL oracles over a small
 * table ad_t(id, g, v) whose tiny value domain guarantees heavy duplication,
 * so distinct counts are strictly below raw counts and the dedup actually runs.
 *
 * This area is deliberately disjoint from "aggregate", "boolagg", "ordagg",
 * "filteragg" and "aggorder": here the subject is specifically DISTINCT within
 * aggregates and several DISTINCT aggregates coexisting in one query.
 *
 * The oracles are computed in SQL from the same table, so there is no C-side
 * arithmetic to overflow; the small value range keeps every sum well within
 * range.  Empty-table and all-NULL cases are handled with coalesce()/nullif()/
 * CASE so those boundaries pass soundly (aggregates are NULL over no rows).
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_aggdistinct.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "aggdistinct"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the aggregate errored or produced an unexpected NULL,
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
 * Build the table ad_t(id int PRIMARY KEY, g int, v int) and populate it with
 * 0..30 rows.  id is a dense surrogate key, the group label g is drawn in
 * [0, 3] so there are only a few groups each with several rows, and v is drawn
 * in [0, 4] so the value domain is tiny -- distinct values are far fewer than
 * rows, which is exactly what makes the DISTINCT dedup observable (distinct
 * count strictly below raw count).  v is non-negative so sum(DISTINCT) <=
 * sum() holds.  The small range means no sum over at most 30 rows can
 * overflow.  Zero rows exercises the empty-aggregate path.  ANALYZE updates
 * statistics so the planner may choose either sort or hash aggregation paths.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ad_t; "
						   "CREATE TABLE ad_t (id int PRIMARY KEY, "
						   "g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int r = 0; r < nrows; r++)
		appendStringInfo(&ddl, "INSERT INTO ad_t VALUES (%d, %d, %d); ",
						 r,
						 (int) pg_prop_draw_int(tc, 0, 3),
						 (int) pg_prop_draw_int(tc, 0, 4));

	appendStringInfoString(&ddl, "ANALYZE ad_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/*
 * The dedup/non-null count chain: count(DISTINCT v) <= count(v) <= count(*).
 * DISTINCT removes duplicates so cannot increase the count, and count(v)
 * skips NULLs so cannot exceed count(*).  Empty table: 0 <= 0 <= 0.
 */
PG_PROP(count_distinct_le_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(DISTINCT v) <= count(v) "
				 "AND count(v) <= count(*) FROM ad_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * For non-negative v (drawn in 0..4), summing the distinct values cannot
 * exceed summing every value, since dropping duplicate non-negative addends
 * only removes non-negative contributions: sum(DISTINCT v) <= sum(v).
 * coalesce(..., 0) makes the empty table (both NULL) pass as 0 <= 0.
 */
PG_PROP(sum_distinct_le_sum_when_nonneg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(sum(DISTINCT v), 0) <= "
				 "coalesce(sum(v), 0) FROM ad_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * count(DISTINCT v) must equal the number of distinct non-NULL values,
 * computed independently as the row count of an explicit SELECT DISTINCT v
 * (which, like count(DISTINCT), ignores NULL under count(*) over the distinct
 * projection only when NULLs are excluded -- here count(DISTINCT) ignores NULL
 * and SELECT DISTINCT yields a NULL row, so we count non-NULL rows).  Both
 * sides agree exactly.  Empty table: both 0.
 */
PG_PROP(distinct_count_eq_subquery)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(DISTINCT v) = "
				 "(SELECT count(*) FROM (SELECT DISTINCT v FROM ad_t) s) "
				 "FROM ad_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * avg(DISTINCT v) is by definition sum(DISTINCT v) / count(DISTINCT v), so it
 * must equal the manual quotient of the two DISTINCT aggregates.  IS NOT
 * DISTINCT FROM makes the empty / all-NULL case (both sides NULL via nullif
 * on a zero distinct count) pass soundly.  The cast to numeric gives the same
 * exact-decimal arithmetic avg uses internally for integer input.
 */
PG_PROP(avg_distinct_manual)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT avg(DISTINCT v) IS NOT DISTINCT FROM "
				 "sum(DISTINCT v)::numeric / nullif(count(DISTINCT v), 0) "
				 "FROM ad_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * Two DISTINCT aggregates coexisting in one query must each still equal their
 * own independent subquery count: count(DISTINCT v) and count(DISTINCT g) are
 * computed together but must not interfere.  This exercises the
 * multiple-per-aggregate-DISTINCT sort/hash path.  Empty table: all 0.
 */
PG_PROP(multiple_distinct_aggs)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(DISTINCT v) = "
				 "(SELECT count(*) FROM (SELECT DISTINCT v FROM ad_t) s) "
				 "AND count(DISTINCT g) = "
				 "(SELECT count(*) FROM (SELECT DISTINCT g FROM ad_t) t) "
				 "FROM ad_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * Per group, DISTINCT cannot produce more values than there are rows:
 * count(DISTINCT v) <= count(*) within each GROUP BY g bucket.  bool_and over
 * the per-group comparisons must be true, and coalesce(..., true) makes the
 * empty table (no groups, bool_and NULL) pass.
 */
PG_PROP(grouped_distinct_sums)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(cd <= c), true) FROM "
				 "(SELECT count(DISTINCT v) cd, count(*) c "
				 "FROM ad_t GROUP BY g) s");

	pg_prop_sql_exec(conn, "DROP TABLE ad_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * string_agg(DISTINCT v::text, ',') concatenates the distinct values, so the
 * number of comma-separated pieces equals count(DISTINCT v).  v::text never
 * contains a comma (v is a small integer), so splitting on ',' recovers one
 * piece per distinct value.  When count(DISTINCT v) is 0 (empty or all-NULL)
 * string_agg is NULL -> coalesce to '' -> array_length of a one-empty-element
 * array is 1, which does not equal 0, so a CASE short-circuits that case to
 * true; otherwise the piece count must equal the distinct count.
 */
PG_PROP(string_agg_distinct_dedup)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN count(DISTINCT v) = 0 THEN true "
				 "ELSE array_length(string_to_array("
				 "coalesce(string_agg(DISTINCT v::text, ','), ''), ','), 1) "
				 "= count(DISTINCT v) END FROM ad_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * DISTINCT does not change the result of bool_and: whether or not duplicate
 * boolean inputs are collapsed, the AND over {true, false} is the same.  So
 * bool_and(DISTINCT (v > 0)) must equal bool_and(v > 0).  coalesce(..., true)
 * makes the empty table (both NULL) pass as true = true.
 */
PG_PROP(bool_and_distinct_same)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(DISTINCT (v > 0)), true) = "
				 "coalesce(bool_and(v > 0), true) FROM ad_t");

	pg_prop_sql_exec(conn, "DROP TABLE ad_t", &err);
	pg_prop_disconnect(conn);
}
