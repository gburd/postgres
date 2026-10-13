/*-------------------------------------------------------------------------
 *
 * prop_windowframes.c
 *	  Property-based tests for SQL window FRAME semantics.
 *
 * Distinct from the "window" area (which checks structural invariants of the
 * window functions themselves -- row_number/rank/dense_rank/lag/lead), this
 * area targets the window FRAME: the ROWS/RANGE/GROUPS clause that selects
 * which peer rows an aggregate sees.  A running sum built with ROWS UNBOUNDED
 * PRECEDING must end at the partition total; a CURRENT-ROW count must equal
 * the row's position; ROWS and RANGE must agree on a whole-partition frame;
 * first_value/last_value/nth_value over a fully-bounded ordered frame must
 * line up with min/max.  A failure means the frame boundary logic (the
 * WinSetMarkPosition / aggregate restart machinery) selected the wrong rows.
 *
 * The oracle is the invariant itself, expressed in SQL and reduced to a
 * single boolean: each per-row check is folded with bool_and(...) over a
 * windowed subquery and wrapped in coalesce(..., true) so an empty table
 * passes vacuously.  v is non-NULL so frame arithmetic is exact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_windowframes.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "windowframes"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Value domain for v.  Kept small (-5..5) and NEVER NULL so frame sums stay
 * exact and legible; |sum| <= 25 * 5 = 125 cannot overflow int.  g in 0..2
 * gives a few small partitions, each with several rows, so the frame logic
 * runs over partitions that actually have interior/edge rows to distinguish.
 */
#define V_MIN (-5)
#define V_MAX 5
#define G_MAX 2

/*
 * Build wf_t(id int PRIMARY KEY, g int, v int) with a drawn number of rows in
 * [0, 25].  id is 1..n so ORDER BY id is a strict total order within every
 * partition (frames and running aggregates are then fully deterministic); g
 * is drawn in 0..G_MAX; v is drawn in V_MIN..V_MAX and never NULL.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS wf_t; "
						   "CREATE TABLE wf_t(id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int i = 1; i <= nrows; i++)
	{
		int			g = (int) pg_prop_draw_int(tc, 0, G_MAX);
		int			v = (int) pg_prop_draw_int(tc, V_MIN, V_MAX);

		appendStringInfo(&ddl, "INSERT INTO wf_t VALUES (%d, %d, %d); ",
						 i, g, v);
	}
	appendStringInfoString(&ddl, "ANALYZE wf_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the window query produced NULL/errored where an invariant
 * demands true -- itself a bug -- so either is a failure, reported with SQL.
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
 * The last running sum (ROWS UNBOUNDED PRECEDING TO CURRENT ROW), taken at the
 * final row of each partition, equals that partition's total sum.  Picks the
 * last row via row_number() DESC = 1; if the frame boundary logic stops short
 * of CURRENT ROW the running sum would miss the final addend.
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
				 "SELECT coalesce(bool_and(rs = tot), true) FROM ("
				 "SELECT g, id, "
				 "sum(v) OVER (PARTITION BY g ORDER BY id "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) rs, "
				 "sum(v) OVER (PARTITION BY g) tot, "
				 "row_number() OVER (PARTITION BY g ORDER BY id DESC) rn "
				 "FROM wf_t) s WHERE rn = 1");

	pg_prop_disconnect(conn);
}

/*
 * A ROWS UNBOUNDED PRECEDING TO CURRENT ROW count equals the row's 1-based
 * position in its partition.  A frame that over- or under-counts the rows up
 * to and including the current row breaks this directly.
 */
PG_PROP(count_frame_equals_position)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(cnt = rn), true) FROM ("
				 "SELECT count(*) OVER (PARTITION BY g ORDER BY id "
				 "ROWS UNBOUNDED PRECEDING) cnt, "
				 "row_number() OVER (PARTITION BY g ORDER BY id) rn "
				 "FROM wf_t) s");

	pg_prop_disconnect(conn);
}

/*
 * On a BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING frame the whole
 * partition is in frame regardless of ROWS vs RANGE, so the two framings must
 * produce the identical aggregate on every row.  A difference means one
 * framing mis-identifies the frame bounds for the "whole partition" case.
 */
PG_PROP(range_vs_rows_full_frame)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(a = b), true) FROM ("
				 "SELECT sum(v) OVER (PARTITION BY g "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a, "
				 "sum(v) OVER (PARTITION BY g "
				 "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) b "
				 "FROM wf_t) s");

	pg_prop_disconnect(conn);
}

/*
 * first_value(v) over a whole-partition frame ordered by v is the partition
 * minimum of v.  The frame is fully bounded (UNBOUNDED PRECEDING..FOLLOWING)
 * and ordered ascending, so the first row in frame carries the smallest v;
 * compared to the frame-independent min(v) over the partition.
 */
PG_PROP(first_value_is_min_ordered)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(fv = mn), true) FROM ("
				 "SELECT first_value(v) OVER (PARTITION BY g ORDER BY v "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) fv, "
				 "min(v) OVER (PARTITION BY g) mn "
				 "FROM wf_t) s");

	pg_prop_disconnect(conn);
}

/*
 * last_value(v) over a whole-partition frame ordered by v is the partition
 * maximum of v.  This is the canonical trap: last_value defaults to a frame
 * ending at CURRENT ROW, so only an explicit UNBOUNDED FOLLOWING frame makes
 * the last-in-frame row the global max.  Compared to min/max's partition max.
 */
PG_PROP(last_value_full_frame_is_max_ordered)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(lv = mx), true) FROM ("
				 "SELECT last_value(v) OVER (PARTITION BY g ORDER BY v "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) lv, "
				 "max(v) OVER (PARTITION BY g) mx "
				 "FROM wf_t) s");

	pg_prop_disconnect(conn);
}

/*
 * nth_value(v, 1) and first_value(v) over the identical frame must agree on
 * every row: both name the first row of the frame.  A sound frame identity --
 * it holds for any frame, bounds, and ordering -- so it isolates whether the
 * two functions resolve the same frame-start row, independent of the data.
 */
PG_PROP(nth_value_1_is_first_value)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(nv IS NOT DISTINCT FROM fv), true) FROM ("
				 "SELECT nth_value(v, 1) OVER (PARTITION BY g ORDER BY id "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) nv, "
				 "first_value(v) OVER (PARTITION BY g ORDER BY id "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) fv "
				 "FROM wf_t) s");

	pg_prop_disconnect(conn);
}
