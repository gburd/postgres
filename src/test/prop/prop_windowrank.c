/*-------------------------------------------------------------------------
 *
 * prop_windowrank.c
 *	  Property-based tests for SQL window RANKING functions.
 *
 * Distinct from the "window" area (structural invariants of the window
 * functions in general) and "windowframes" (the ROWS/RANGE/GROUPS frame),
 * this area targets the ranking family and the relationships between its
 * members: rank/dense_rank, percent_rank, cume_dist, ntile, and lag/lead.
 * dense_rank must never exceed rank; percent_rank and cume_dist must stay in
 * [0,1] with cume_dist >= percent_rank; ntile(4) must stay in [1,4]; the
 * minimum rank in a partition is 1; max(dense_rank) equals the count of
 * distinct order values; and lead(v) is non-NULL on every row but the
 * partition last.  A failure means a ranking peer-group or position
 * computation selected the wrong value.
 *
 * The oracle is the invariant itself, expressed in SQL and reduced to a
 * single boolean: each per-row/per-partition check is folded with
 * bool_and(...) over a windowed subquery and wrapped in coalesce(..., true)
 * so an empty table passes vacuously.  v is non-NULL so the peer grouping and
 * ranking are exact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_windowrank.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "windowrank"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Value domain for v.  Kept small (-5..5) and NEVER NULL so the ORDER BY peer
 * grouping is exact (ties occur often, exercising rank vs dense_rank).  g in
 * 0..2 gives a few small partitions each with several rows, so the ranking
 * logic runs over partitions with real interior/edge/tie structure.
 */
#define V_MIN (-5)
#define V_MAX 5
#define G_MAX 2

/*
 * Build wr_t(id int PRIMARY KEY, g int, v int) with a drawn number of rows in
 * [0, 25].  id is 1..n so ORDER BY ..., id is a strict total order within
 * every partition (ntile and lead are then fully deterministic); g is drawn
 * in 0..G_MAX; v is drawn in V_MIN..V_MAX and never NULL.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS wr_t; "
						   "CREATE TABLE wr_t(id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int i = 1; i <= nrows; i++)
	{
		int			g = (int) pg_prop_draw_int(tc, 0, G_MAX);
		int			v = (int) pg_prop_draw_int(tc, V_MIN, V_MAX);

		appendStringInfo(&ddl, "INSERT INTO wr_t VALUES (%d, %d, %d); ",
						 i, g, v);
	}
	appendStringInfoString(&ddl, "ANALYZE wr_t; ");

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
 * dense_rank() never exceeds rank() over the same partition/order.  Ties
 * advance rank by the group size but dense_rank by one, so dense_rank <= rank
 * must hold on every row; a peer-group miscount would push dense_rank past
 * rank.
 */
PG_PROP(rank_le_dense_rank_scaled)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(dr <= rk), true) FROM ("
				 "SELECT rank() OVER (PARTITION BY g ORDER BY v) rk, "
				 "dense_rank() OVER (PARTITION BY g ORDER BY v) dr "
				 "FROM wr_t) s");

	pg_prop_disconnect(conn);
}

/*
 * percent_rank() always lies in the closed unit interval [0, 1].  It is
 * (rank-1)/(nrows-1), which is 0 on the first peer group and 1 on the last; a
 * position/row-count error would push it outside [0,1].
 */
PG_PROP(percent_rank_unit_interval)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(pr BETWEEN 0 AND 1), true) FROM ("
				 "SELECT percent_rank() OVER (PARTITION BY g ORDER BY v) pr "
				 "FROM wr_t) s");

	pg_prop_disconnect(conn);
}

/*
 * cume_dist() lies in (0, 1] and is always >= percent_rank() over the same
 * window.  cume_dist counts rows up to and including the current peer group
 * (so it reaches 1 at the top), while percent_rank counts rows strictly
 * before it; a peer-boundary error would break the ordering between them or
 * push cume_dist outside [0,1].
 */
PG_PROP(cume_dist_unit_interval)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(cd BETWEEN 0 AND 1 AND cd >= pr), true) FROM ("
				 "SELECT cume_dist() OVER (PARTITION BY g ORDER BY v) cd, "
				 "percent_rank() OVER (PARTITION BY g ORDER BY v) pr "
				 "FROM wr_t) s");

	pg_prop_disconnect(conn);
}

/*
 * ntile(4) over a strict total order (v, id) assigns every row a bucket in
 * [1, 4].  A bucket-boundary arithmetic error (the classic remainder/division
 * off-by-one in the ntile bucket sizing) would produce a 0 or a 5.
 */
PG_PROP(ntile_bounded)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(nt BETWEEN 1 AND 4), true) FROM ("
				 "SELECT ntile(4) OVER (PARTITION BY g ORDER BY v, id) nt "
				 "FROM wr_t) s");

	pg_prop_disconnect(conn);
}

/*
 * The minimum rank() in every partition is 1.  Regardless of ties, the first
 * peer group always ranks 1; so the per-partition minimum of rank() must be
 * exactly 1.  A rank that failed to reset at a partition boundary would show a
 * minimum other than 1.
 */
PG_PROP(rank_first_is_one)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(mr = 1), true) FROM ("
				 "SELECT g, min(rk) mr FROM ("
				 "SELECT g, rank() OVER (PARTITION BY g ORDER BY v) rk "
				 "FROM wr_t) a GROUP BY g) b");

	pg_prop_disconnect(conn);
}

/*
 * The maximum dense_rank() in a partition equals the number of distinct order
 * values (v) in that partition.  dense_rank assigns consecutive integers
 * 1..k to the k distinct peer groups, so its partition maximum is exactly k =
 * count(distinct v).  Computed on two independent paths (windowed dense_rank
 * vs plain count(distinct)) and joined on g, this is a differential check: a
 * peer-group miscount in dense_rank breaks the equality.
 */
PG_PROP(dense_rank_counts_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(mdr = dc), true) FROM ("
				 "SELECT g, max(dr) mdr FROM ("
				 "SELECT g, dense_rank() OVER (PARTITION BY g ORDER BY v) dr "
				 "FROM wr_t) a GROUP BY g) m "
				 "JOIN (SELECT g, count(DISTINCT v) dc FROM wr_t GROUP BY g) d "
				 "USING (g)");

	pg_prop_disconnect(conn);
}

/*
 * lead(v) OVER (PARTITION BY g ORDER BY id) is non-NULL on exactly the rows
 * that are not the partition last; v itself is never NULL, so the count of
 * non-NULL lead values in each partition equals that partition's size minus
 * one.  A lead that reads the wrong offset, or fails to NULL the partition
 * boundary, breaks this count.
 */
PG_PROP(lag_lead_shift)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(nn = sz - 1), true) FROM ("
				 "SELECT g, count(ld) nn, count(*) sz FROM ("
				 "SELECT g, lead(v) OVER (PARTITION BY g ORDER BY id) ld "
				 "FROM wr_t) a GROUP BY g) b");

	pg_prop_disconnect(conn);
}
