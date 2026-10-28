/*-------------------------------------------------------------------------
 *
 * prop_windowexcl.c
 *	  Property-based tests for window frame EXCLUDE clauses and GROUPS mode.
 *
 * Distinct from the window/windowframes/windowrank areas, this one targets
 * the frame EXCLUSION options (EXCLUDE CURRENT ROW / GROUP / TIES / NO OTHERS)
 * and the GROUPS framing mode, where a frame counts peer groups rather than
 * rows or value ranges.  EXCLUDE CURRENT ROW over a full partition must drop
 * exactly this row's value; EXCLUDE NO OTHERS must match omitting the clause;
 * a GROUPS running count must be monotone and end at the partition size;
 * EXCLUDE GROUP must remove the whole peer group; EXCLUDE TIES can only remove
 * rows versus NO OTHERS.  A failure means the exclusion bookkeeping (the
 * frameheadpos/frametailpos plus the peer-group exclusion in nodeWindowAgg)
 * selected the wrong rows.
 *
 * The oracle is the invariant itself, expressed in SQL and reduced to a single
 * boolean: each per-row check is folded with bool_and(...) over a windowed
 * subquery and wrapped in coalesce(..., true) so an empty table passes
 * vacuously.  v is non-NULL and small (0..3) so peer groups actually form.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_windowexcl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "windowexcl"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Value domain for v.  Kept tiny (0..3) and NEVER NULL so many rows share a v
 * within a partition and GROUPS peer groups actually form; g in 0..2 gives a
 * few small partitions.  v non-NULL keeps the EXCLUDE arithmetic exact.
 */
#define V_MAX 3
#define G_MAX 2

/*
 * Build we_t(id int PRIMARY KEY, g int, v int) with a drawn number of rows in
 * [0, 25].  id is 1..n so ORDER BY id is a strict total order within every
 * partition; g is drawn in 0..G_MAX; v is drawn in 0..V_MAX, never NULL.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS we_t; "
						   "CREATE TABLE we_t(id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int i = 1; i <= nrows; i++)
	{
		int			g = (int) pg_prop_draw_int(tc, 0, G_MAX);
		int			v = (int) pg_prop_draw_int(tc, 0, V_MAX);

		appendStringInfo(&ddl, "INSERT INTO we_t VALUES (%d, %d, %d); ",
						 i, g, v);
	}
	appendStringInfoString(&ddl, "ANALYZE we_t; ");

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
 * EXCLUDE CURRENT ROW over a whole-partition frame drops exactly this row's
 * own value: sum(v) EXCLUDE CURRENT ROW = sum(v) over the full frame minus v.
 * The whole-partition frame makes the excluded set exactly {current row}, so
 * the two sums must differ by precisely v on every row.
 */
PG_PROP(exclude_current_row)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(sum_excl = sum_all - v), true) FROM ("
				 "SELECT v, "
				 "sum(v) OVER (PARTITION BY g "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING "
				 "EXCLUDE CURRENT ROW) sum_excl, "
				 "sum(v) OVER (PARTITION BY g "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) sum_all "
				 "FROM we_t) s");

	pg_prop_disconnect(conn);
}

/*
 * The default EXCLUDE NO OTHERS excludes nothing, so a frame carrying it must
 * produce the identical aggregate as the same frame with the clause omitted.
 * A difference means the parser/executor treats the default option as doing
 * something, which it must not.
 */
PG_PROP(exclude_no_others_default)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(a = b), true) FROM ("
				 "SELECT sum(v) OVER (PARTITION BY g ORDER BY id "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) a, "
				 "sum(v) OVER (PARTITION BY g ORDER BY id "
				 "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING "
				 "EXCLUDE NO OTHERS) b "
				 "FROM we_t) s");

	pg_prop_disconnect(conn);
}

/*
 * A GROUPS running count (UNBOUNDED PRECEDING TO CURRENT ROW, ordered by v)
 * counts whole peer groups up to and including this row's group, so taken at
 * the last peer group of a partition it equals the partition size.  The last
 * group is where row_number() DESC within the partition is 1; its running
 * count must be the full partition row count.
 */
PG_PROP(groups_mode_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(cnt = psize), true) FROM ("
				 "SELECT count(*) OVER (PARTITION BY g ORDER BY v "
				 "GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cnt, "
				 "count(*) OVER (PARTITION BY g) psize, "
				 "row_number() OVER (PARTITION BY g ORDER BY v DESC) rn "
				 "FROM we_t) s WHERE rn = 1");

	pg_prop_disconnect(conn);
}

/*
 * With GROUPS over the whole partition and EXCLUDE GROUP, the current row's
 * entire peer group is removed, so the frame count equals partition size minus
 * the size of this row's peer group.  Peer-group size is computed independently
 * as count(*) OVER (PARTITION BY g ORDER BY v RANGE CURRENT ROW), i.e. the set
 * of rows that are peers of this one under the v ordering.
 */
PG_PROP(exclude_group)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(cnt = psize - peers), true) FROM ("
				 "SELECT count(*) OVER (PARTITION BY g ORDER BY v "
				 "GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING "
				 "EXCLUDE GROUP) cnt, "
				 "count(*) OVER (PARTITION BY g) psize, "
				 "count(*) OVER (PARTITION BY g ORDER BY v "
				 "RANGE BETWEEN CURRENT ROW AND CURRENT ROW) peers "
				 "FROM we_t) s");

	pg_prop_disconnect(conn);
}

/*
 * EXCLUDE TIES removes the current row's peers (but keeps the current row), so
 * on any frame its count can only be less than or equal to the same frame with
 * EXCLUDE NO OTHERS -- excluding rows never adds any.  A strict inequality in
 * the other direction would mean exclusion invented rows.
 */
PG_PROP(exclude_ties_le)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(cnt_ties <= cnt_none), true) FROM ("
				 "SELECT count(*) OVER (PARTITION BY g ORDER BY v "
				 "GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING "
				 "EXCLUDE TIES) cnt_ties, "
				 "count(*) OVER (PARTITION BY g ORDER BY v "
				 "GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING "
				 "EXCLUDE NO OTHERS) cnt_none "
				 "FROM we_t) s");

	pg_prop_disconnect(conn);
}

/*
 * A ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING frame is the whole
 * partition, so sum(v) over it equals the frame-independent partition total
 * sum(v) OVER (PARTITION BY g) on every row.  A difference means the full-frame
 * bounds mis-identify the partition.
 */
PG_PROP(full_frame_sum_is_partition_total)
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
				 "sum(v) OVER (PARTITION BY g) b "
				 "FROM we_t) s");

	pg_prop_disconnect(conn);
}
