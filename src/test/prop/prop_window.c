/*-------------------------------------------------------------------------
 *
 * prop_window.c
 *	  Property-based tests for SQL window functions.
 *
 * Window functions over an ordered partition must satisfy structural
 * invariants: row_number() is a permutation of 1..n, rank() <= row_number()
 * under the same ordering, dense_rank() is gapless (its maximum equals the
 * number of distinct order-key values), lag/lead shift by their offset, and a
 * running sum built with ROWS UNBOUNDED PRECEDING ends at the grand total.
 * These properties check each invariant over a generated table.
 *
 * The oracle is the invariant itself, expressed in SQL and reduced to a
 * single boolean: each per-row check is folded with bool_and(...) over the
 * windowed subquery and wrapped in coalesce(..., true) so the empty table
 * (n = 0) passes vacuously.  IS NOT DISTINCT FROM is used wherever a value may
 * be NULL (lag at the first row, the sum of an empty set).
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_window.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "window"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Value domain for the "v" column.  Kept small so that running sums of up to
 * 25 rows (|sum| <= 25 * 50 = 1250) cannot overflow int, and so a
 * counterexample table stays legible.
 */
#define V_MIN (-50)
#define V_MAX 50

/*
 * Build the test table agg_w(id int, v int) with a drawn number of rows in
 * [0, 25].  id is assigned 1..n explicitly so that ORDER BY id is a strict
 * total order (lag/lead are then fully deterministic); v is random in
 * [V_MIN, V_MAX] and may tie, so any ordering that must be total uses
 * ORDER BY v, id.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS agg_w; "
						   "CREATE TABLE agg_w (id int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int i = 1; i <= nrows; i++)
	{
		int			v = (int) pg_prop_draw_int(tc, V_MIN, V_MAX);

		appendStringInfo(&ddl, "INSERT INTO agg_w VALUES (%d, %d); ", i, v);
	}

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/* Drop the test table (best effort; ignore errors). */
static void
drop_table(pg_prop_conn *conn)
{
	char	   *err = NULL;

	(void) pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS agg_w", &err);
	if (err)
		pg_free(err);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the window query errored or produced NULL where the
 * invariant should have been decided, which is itself a failure, reported
 * with the SQL.
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
 * row_number() OVER (ORDER BY v, id) over n rows yields exactly {1..n}: its
 * maximum equals the row count and the number of distinct values equals the
 * row count, so the generated numbers are a permutation of 1..n.
 */
PG_PROP(row_number_is_permutation)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce((max(rn) = count(*) "
				 "AND count(DISTINCT rn) = count(*)), true) "
				 "FROM (SELECT row_number() OVER (ORDER BY v, id) rn "
				 "FROM agg_w) s");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * For every row, rank() <= row_number() under the same ordering.  rank() ties
 * share the lower number while row_number() is strictly increasing, so rank()
 * can only equal or trail row_number().
 */
PG_PROP(rank_le_row_number)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(rk <= rn), true) "
				 "FROM (SELECT rank() OVER w rk, row_number() OVER w rn "
				 "FROM agg_w WINDOW w AS (ORDER BY v)) s");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * dense_rank() is gapless: its maximum over ORDER BY v equals the number of
 * distinct v values.  (On the empty table max() is NULL, so coalesce it to 0,
 * which equals count(DISTINCT v) = 0.)
 */
PG_PROP(dense_rank_gapless)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce((SELECT max(dr) FROM "
				 "(SELECT dense_rank() OVER (ORDER BY v) dr FROM agg_w) s), 0) "
				 "= (SELECT count(DISTINCT v) FROM agg_w)");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * lag(v) OVER (ORDER BY id) at the row with id = k must equal the v of the row
 * with id = k - 1.  A self LEFT JOIN against the row at id - 1 is the oracle:
 * at the first row there is no id - 1, b.v is NULL, and lag is NULL, so
 * IS NOT DISTINCT FROM holds there too.
 */
PG_PROP(lag_lead_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(a.lagged IS NOT DISTINCT FROM b.v), true) "
				 "FROM (SELECT id, lag(v) OVER (ORDER BY id) lagged FROM agg_w) a "
				 "LEFT JOIN agg_w b ON b.id = a.id - 1");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * The running sum (sum(v) OVER ORDER BY id ROWS UNBOUNDED PRECEDING) at the
 * last row equals the grand total sum(v).  Both sides are NULL on the empty
 * table, so IS NOT DISTINCT FROM decides the empty case cleanly.
 */
PG_PROP(running_sum_last_equals_total)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT rs FROM "
				 "(SELECT id, sum(v) OVER (ORDER BY id "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) rs "
				 "FROM agg_w) s ORDER BY id DESC LIMIT 1) "
				 "IS NOT DISTINCT FROM (SELECT sum(v) FROM agg_w)");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * count(*) OVER () with no partition or frame reports the full row count n on
 * every row.  bool_and folds that over all rows; coalesce handles the empty
 * table.
 */
PG_PROP(count_over_equals_n)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(c = (SELECT count(*) FROM agg_w)), true) "
				 "FROM (SELECT count(*) OVER () c FROM agg_w) s");

	drop_table(conn);
	pg_prop_disconnect(conn);
}
