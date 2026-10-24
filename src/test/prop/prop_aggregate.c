/*-------------------------------------------------------------------------
 *
 * prop_aggregate.c
 *	  Property-based tests for the standard SQL aggregate functions.
 *
 * The standard aggregates sum(), avg(), min(), max() and count() must satisfy
 * exact relationships that hold for any column of values: the sum equals the
 * count times the average, the minimum is no greater than the average which
 * is no greater than the maximum, count(*) counts every row while count(col)
 * and sum()/avg()/min()/max() skip NULLs, and min()/max() are values actually
 * present in the column.  These properties check those relationships over a
 * randomly generated integer column.
 *
 * The oracles are computed in SQL, not C, so there is no risk of C-side
 * overflow and the type's own exact numeric arithmetic (avg(int) and sum(int)
 * return numeric/bigint) is the reference.  Comparisons use IS NOT DISTINCT
 * FROM so that the empty / all-NULL table -- where every aggregate but
 * count() is NULL -- is handled cleanly rather than yielding a NULL result.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_aggregate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "aggregate"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the aggregate errored or produced an unexpected NULL,
 * which is itself a failure, reported with the SQL.
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
 * Build a one-column integer table agg_t with a drawn number of rows (0..30).
 * Each value is a small integer in [-100, 100] -- small enough that no
 * aggregate over at most 30 of them can overflow -- or SQL NULL (~1 in 6), so
 * NULL-skipping is exercised.  The oracles are all derived in SQL from this
 * table, so nothing is returned; the counterexample shows the drawn values
 * through the generated INSERTs.  DROP TABLE IF EXISTS runs first; the harness
 * sets client_min_messages = warning so the "does not exist" NOTICE is
 * suppressed.  INSERTs are batched into one statement for speed.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 30);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS agg_t; "
						   "CREATE TABLE agg_t (v int); ");

	for (int r = 0; r < nrows; r++)
	{
		if (pg_prop_draw_int(tc, 0, 5) == 0)
			appendStringInfoString(&ddl, "INSERT INTO agg_t VALUES (NULL); ");
		else
			appendStringInfo(&ddl, "INSERT INTO agg_t VALUES (%d); ",
							 (int) pg_prop_draw_int(tc, -100, 100));
	}

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/*
 * avg(v) = sum(v) / count(v).  avg returns numeric; the exact identity divides
 * sum by the same count avg used, so it holds to the last digit (unlike
 * sum = count*avg, which rounds).  Empty / all-NULL: avg IS NULL.
 */
/*
 * avg(v) is sum(v)/count(v) by definition.  avg returns a finite-scale
 * numeric, so sum = count*avg does NOT hold exactly (the multiply reintroduces
 * rounding); the sound exact identity is avg = sum::numeric / count when there
 * is at least one non-NULL value, and avg IS NULL otherwise.
 */
PG_PROP(avg_equals_sum_over_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN count(v) > 0 "
				 "THEN avg(v) = sum(v)::numeric / count(v) "
				 "ELSE avg(v) IS NULL END FROM agg_t");

	pg_prop_sql_exec(conn, "DROP TABLE agg_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * min <= avg <= max.  Holds whenever there is at least one non-NULL row; when
 * there is none every aggregate is NULL and the comparison is NULL, so
 * coalesce(..., true) accepts the empty / all-NULL case.
 */
PG_PROP(min_le_avg_le_max)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(min(v) <= avg(v) AND avg(v) <= max(v), true) "
				 "FROM agg_t");

	pg_prop_sql_exec(conn, "DROP TABLE agg_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * count(*) counts every row; count(v) counts only non-NULL rows.  So
 * count(*) >= count(v), and the difference is exactly the number of NULL rows.
 */
PG_PROP(count_star_vs_count_col)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(*) >= count(v) AND (count(*) - count(v)) = "
				 "count(*) FILTER (WHERE v IS NULL) FROM agg_t");

	pg_prop_sql_exec(conn, "DROP TABLE agg_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * min() and max() return values that actually occur in the column.  Only
 * meaningful when there is at least one non-NULL value; on an empty or
 * all-NULL table min/max are NULL (and a NULL IN (empty set) is false, not
 * NULL), so gate the test on count(v) > 0.
 */
PG_PROP(min_max_in_table)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN count(v) > 0 "
				 "THEN (min(v) IN (SELECT v FROM agg_t)) AND "
				 "(max(v) IN (SELECT v FROM agg_t)) "
				 "ELSE true END FROM agg_t");

	pg_prop_sql_exec(conn, "DROP TABLE agg_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * sum() skips NULLs: the sum over the whole table equals the sum over the
 * NULL-filtered rows.  IS NOT DISTINCT FROM makes the empty / all-NULL case
 * (both sums NULL) pass.
 */
PG_PROP(sum_null_skipping)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT sum(v) IS NOT DISTINCT FROM "
				 "(SELECT sum(v) FROM agg_t WHERE v IS NOT NULL) FROM agg_t");

	pg_prop_sql_exec(conn, "DROP TABLE agg_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * count(DISTINCT v) <= count(v): distinct non-NULL values cannot outnumber
 * all non-NULL values.  Both are 0 on an empty / all-NULL table, so 0 <= 0.
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
				 "SELECT count(DISTINCT v) <= count(v) FROM agg_t");

	pg_prop_sql_exec(conn, "DROP TABLE agg_t", &err);
	pg_prop_disconnect(conn);
}
