/*-------------------------------------------------------------------------
 *
 * prop_windownav.c
 *	  Property-based tests for SQL window NAVIGATION functions.
 *
 * Distinct from "window" (general structural invariants), "windowframes"
 * (ROWS/RANGE/GROUPS frame), "windowrank" (ranking family), "windowexcl"
 * (frame EXCLUDE) and "windowagg" (aggregates as window functions), this
 * area targets the navigation family: lag/lead and the positional readers
 * first_value/last_value/nth_value, plus row_number as the position oracle.
 * lag's default fires on the first row of a partition; lead/lag have exactly
 * (size-1) non-NULL results per partition; first_value/last_value over a full
 * frame are constant per partition and equal the min-id/max-id row's v;
 * nth_value(,1) equals first_value; and row_number is a dense 1..size bijection.
 * A failure means a navigation/position computation read the wrong row.
 *
 * The oracle is the invariant itself, expressed in SQL and reduced to a
 * single boolean: each per-row/per-partition check is folded with bool_and()
 * over a windowed subquery and wrapped in coalesce(..., true) so an empty
 * table passes vacuously.  v is non-NULL so lead/lag NULLs mark only partition
 * edges and positional reads are exact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_windownav.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "windownav"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Value domain for v.  Kept small (0..9) and NEVER NULL so lead/lag NULLs mark
 * only the partition edges and the positional reads (first/last/nth_value) are
 * exact.  g in 0..2 gives a few small partitions each with several rows, so the
 * navigation logic runs over partitions with real interior/edge structure.
 */
#define V_MIN 0
#define V_MAX 9
#define G_MAX 2

/*
 * Build wn_t(id int PRIMARY KEY, g int, v int) with a drawn number of rows in
 * [0, 25].  id is 1..n so ORDER BY id is a strict total order within every
 * partition (lead/lag and the full-frame positional reads are then fully
 * deterministic); g is drawn in 0..G_MAX; v is drawn in V_MIN..V_MAX and never
 * NULL.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS wn_t; "
						   "CREATE TABLE wn_t(id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int i = 1; i <= nrows; i++)
	{
		int			g = (int) pg_prop_draw_int(tc, 0, G_MAX);
		int			v = (int) pg_prop_draw_int(tc, V_MIN, V_MAX);

		appendStringInfo(&ddl, "INSERT INTO wn_t VALUES (%d, %d, %d); ",
						 i, g, v);
	}
	appendStringInfoString(&ddl, "ANALYZE wn_t; ");

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
 * lag(v, 1, -1) OVER (PARTITION BY g ORDER BY id) returns the default (-1) on
 * the first row of each partition and the previous row's v elsewhere.  So on
 * every row whose row_number() is 1, the lagged value must equal exactly the
 * default -1.  A default-handling or offset error on the partition's leading
 * edge would make the first row's lag something other than -1.
 */
PG_PROP(lag_default)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(CASE WHEN rn = 1 THEN lg = -1 ELSE true END), true) FROM ("
				 "SELECT lag(v, 1, -1) OVER w lg, row_number() OVER w rn "
				 "FROM (SELECT g, id, v FROM wn_t) t "
				 "WINDOW w AS (PARTITION BY g ORDER BY id)) s");

	pg_prop_disconnect(conn);
}

/*
 * lead(v) reads one row forward and so is non-NULL on every row of a partition
 * except the last.  Over a non-NULL v, the count of non-NULL lead values in a
 * partition must equal (partition size - 1).  A forward-offset or
 * partition-boundary error would leave the wrong number of non-NULL leads.
 */
PG_PROP(lead_lag_symmetry)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(nleads = sz - 1), true) FROM ("
				 "SELECT count(ld) nleads, count(*) sz FROM ("
				 "SELECT g, lead(v) OVER (PARTITION BY g ORDER BY id) ld "
				 "FROM wn_t) t GROUP BY g) s");

	pg_prop_disconnect(conn);
}

/*
 * first_value(v) over a full frame (UNBOUNDED PRECEDING .. UNBOUNDED
 * FOLLOWING) is constant across a partition and equals the v of the row with
 * the minimum id in that partition.  Comparing the per-row first_value with
 * the min-by-id v (computed independently via a correlated aggregate) catches
 * a frame-start or positional-read error.
 */
PG_PROP(first_value_constant)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(fv = minv), true) FROM ("
				 "SELECT first_value(v) OVER w fv, "
				 "(SELECT v FROM wn_t x WHERE x.g = t.g ORDER BY x.id LIMIT 1) minv "
				 "FROM (SELECT g, id, v FROM wn_t) t "
				 "WINDOW w AS (PARTITION BY g ORDER BY id "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) s");

	pg_prop_disconnect(conn);
}

/*
 * nth_value(v, 1) over a full frame equals first_value(v) over the same frame
 * on every row: both name the first row of the frame.  This is a differential
 * identity between two navigation functions, sound by construction; a
 * one-based-offset bug in nth_value would break it.
 */
PG_PROP(nth_value_1_eq_first)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(nv = fv), true) FROM ("
				 "SELECT nth_value(v, 1) OVER w nv, first_value(v) OVER w fv "
				 "FROM (SELECT g, id, v FROM wn_t) t "
				 "WINDOW w AS (PARTITION BY g ORDER BY id "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) s");

	pg_prop_disconnect(conn);
}

/*
 * last_value(v) over a full frame (UNBOUNDED PRECEDING .. UNBOUNDED FOLLOWING)
 * is constant across a partition and equals the v of the row with the maximum
 * id in that partition.  Without the explicit full frame last_value tracks the
 * default running frame and reads the current row; the full frame is what
 * makes it the partition's last.  A frame-end error would read the wrong row.
 */
PG_PROP(last_value_full_frame)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(lv = maxv), true) FROM ("
				 "SELECT last_value(v) OVER w lv, "
				 "(SELECT v FROM wn_t x WHERE x.g = t.g ORDER BY x.id DESC LIMIT 1) maxv "
				 "FROM (SELECT g, id, v FROM wn_t) t "
				 "WINDOW w AS (PARTITION BY g ORDER BY id "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)) s");

	pg_prop_disconnect(conn);
}

/*
 * lag(v) reads one row back and so is non-NULL on every row of a partition
 * except the first.  Over a non-NULL v, the count of non-NULL lag values in a
 * partition must equal (partition size - 1) -- the mirror of lead's count.  A
 * backward-offset or partition-boundary error would leave the wrong count.
 */
PG_PROP(lag_lead_inverse_interior)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(nlags = sz - 1), true) FROM ("
				 "SELECT count(lg) nlags, count(*) sz FROM ("
				 "SELECT g, lag(v) OVER (PARTITION BY g ORDER BY id) lg "
				 "FROM wn_t) t GROUP BY g) s");

	pg_prop_disconnect(conn);
}

/*
 * row_number() OVER (PARTITION BY g ORDER BY id) is a dense bijection onto
 * 1..size in every partition: its maximum equals the partition size and it
 * takes that many distinct values.  Both max(rn)=size and
 * count(distinct rn)=size must hold per partition.  A gap, a duplicate, or a
 * failure to reset at a partition boundary would break one of them.
 */
PG_PROP(row_number_dense)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(mrn = sz AND drn = sz), true) FROM ("
				 "SELECT max(rn) mrn, count(DISTINCT rn) drn, sz FROM ("
				 "SELECT g, row_number() OVER (PARTITION BY g ORDER BY id) rn, "
				 "count(*) OVER (PARTITION BY g) sz "
				 "FROM wn_t) t GROUP BY g, sz) u");

	pg_prop_disconnect(conn);
}
