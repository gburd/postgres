/*-------------------------------------------------------------------------
 *
 * prop_windowdistinct.c
 *	  Property-based tests for window-aggregate correctness.
 *
 * These properties exercise windowed aggregates (running, partitioned, and
 * framed) and FILTER on window functions against invariants that must hold
 * for every row of every input: a running COUNT equals the row position, a
 * running SUM reaches the partition total at the last row, per-partition
 * min/max/avg bound each value, a FILTERed count never exceeds the full
 * count, and a 2-row frame sum never exceeds the whole-partition sum when all
 * values are non-negative.  A window-machinery bug (wrong frame edges, a
 * mishandled partition boundary, a dropped FILTER) breaks one of these, so an
 * "f" is a correctness bug, not a rounding artefact.
 *
 * The oracle shape (A): each property wraps a per-row window check in
 * bool_and(...) over a subquery and coalesces to true on empty input, so the
 * whole invariant collapses to a single scalar boolean that must be "t".
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_windowdistinct.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "windowdistinct"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the window machinery errored or produced NULL where the
 * invariant demands a definite answer, which is itself a bug -- so either is a
 * failure, reported with the SQL.
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
 * Build the table under test fresh for each case: wd_t(id, g, v) with 0..25
 * rows, a unique PRIMARY KEY id, a group g in 0..2 and a value v in 0..9.  No
 * NULLs: every property below assumes v is non-NULL and v >= 0.  ANALYZE so
 * the planner has stats (the window path is the same either way, but this
 * matches the real-use shape and keeps plans realistic).
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS wd_t; "
						   "CREATE TABLE wd_t (id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int i = 0; i < nrows; i++)
	{
		int			g = (int) pg_prop_draw_int(tc, 0, 2);
		int			v = (int) pg_prop_draw_int(tc, 0, 9);

		appendStringInfo(&ddl,
						 "INSERT INTO wd_t VALUES (%d, %d, %d); ",
						 i, g, v);
	}
	appendStringInfoString(&ddl, "ANALYZE wd_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/*
 * running_count_increases: a running COUNT(*) over ROWS UNBOUNDED PRECEDING,
 * taken in id order within each partition, equals the 1-based row position in
 * that partition.  The oracle is the same ordinal computed by row_number():
 * cnt = rn for every row.  Catches a frame or partition-boundary bug where
 * the running count fails to reset or miscounts at the first row.
 */
PG_PROP(running_count_increases)
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
						   "SELECT coalesce(bool_and(cnt = rn), true) FROM ("
						   "  SELECT count(*) OVER (PARTITION BY g ORDER BY id "
						   "                        ROWS UNBOUNDED PRECEDING) AS cnt, "
						   "         row_number() OVER (PARTITION BY g ORDER BY id) AS rn "
						   "  FROM wd_t) s");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * running_sum_last_eq_total: within each partition the running SUM(v) taken
 * in id order reaches the partition total exactly at the last row.  The check
 * compares, at the row where the running row-number equals the partition's
 * row count, the running sum against the whole-partition sum.  A frame bug
 * that drops or double-counts the final row breaks this.
 */
PG_PROP(running_sum_last_eq_total)
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
						   "SELECT coalesce(bool_and(rn <> cnt OR rsum = total), true) FROM ("
						   "  SELECT sum(v) OVER (PARTITION BY g ORDER BY id "
						   "                      ROWS UNBOUNDED PRECEDING) AS rsum, "
						   "         sum(v) OVER (PARTITION BY g) AS total, "
						   "         row_number() OVER (PARTITION BY g ORDER BY id) AS rn, "
						   "         count(*) OVER (PARTITION BY g) AS cnt "
						   "  FROM wd_t) s");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * window_min_le_value: the per-partition MIN(v) is <= v for every row in that
 * partition.  A trivial but load-bearing invariant for the partition-wide
 * min: a boundary bug that leaks another partition's rows into the frame
 * would push the min below (still passes) or above (fails) some value.
 */
PG_PROP(window_min_le_value)
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
						   "SELECT coalesce(bool_and(mn <= v), true) FROM ("
						   "  SELECT v, min(v) OVER (PARTITION BY g) AS mn "
						   "  FROM wd_t) s");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * window_max_ge_value: the per-partition MAX(v) is >= v for every row in that
 * partition.  The mirror of window_min_le_value; together they bracket every
 * value by its partition's extremes.
 */
PG_PROP(window_max_ge_value)
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
						   "SELECT coalesce(bool_and(mx >= v), true) FROM ("
						   "  SELECT v, max(v) OVER (PARTITION BY g) AS mx "
						   "  FROM wd_t) s");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * avg_between_minmax: the per-partition AVG(v) lies between the per-partition
 * MIN(v) and MAX(v).  The three aggregates share a partition frame, so this
 * cross-checks that avg is computed over the same row set as min/max; a frame
 * discrepancy among them breaks the BETWEEN.
 */
PG_PROP(avg_between_minmax)
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
						   "SELECT coalesce(bool_and(av BETWEEN mn AND mx), true) FROM ("
						   "SELECT avg(v) OVER w av, min(v) OVER w mn, max(v) OVER w mx "
						   "FROM wd_t "
						   "WINDOW w AS (PARTITION BY g)) s");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * filter_window: a FILTERed window count never exceeds the unfiltered window
 * count over the same partition.  count(*) FILTER (WHERE v > 5) counts a
 * subset of the rows count(*) counts, so the filtered value is always <= the
 * full value.  Catches a FILTER-on-window bug that lets excluded rows through.
 */
PG_PROP(filter_window)
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
						   "SELECT coalesce(bool_and(fcnt <= tcnt), true) FROM ("
						   "  SELECT count(*) FILTER (WHERE v > 5) "
						   "           OVER (PARTITION BY g) AS fcnt, "
						   "         count(*) OVER (PARTITION BY g) AS tcnt "
						   "  FROM wd_t) s");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * frame_sum_le_total: a 2-row trailing frame sum (1 PRECEDING .. CURRENT ROW)
 * never exceeds the whole-partition sum, given all v >= 0.  With non-negative
 * values a sub-frame's sum cannot exceed the full-partition sum, so this is a
 * sound bound; a frame-edge bug that over-counts rows into the trailing frame
 * breaks it.
 */
PG_PROP(frame_sum_le_total)
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
						   "SELECT coalesce(bool_and(fsum <= total), true) FROM ("
						   "  SELECT sum(v) OVER (PARTITION BY g ORDER BY id "
						   "                      ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS fsum, "
						   "         sum(v) OVER (PARTITION BY g) AS total "
						   "  FROM wd_t) s");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
