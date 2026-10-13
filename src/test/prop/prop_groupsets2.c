/*-------------------------------------------------------------------------
 *
 * prop_groupsets2.c
 *	  Property-based tests for GROUPING SETS with FILTER and grouping_id.
 *
 * These are scalar-boolean algebra properties about GROUPING SETS output that
 * are distinct from the UNION-ALL differential oracle in prop_groupingsets.c
 * and the ROLLUP/CUBE rewrites in prop_rollup2.c: the grand-total subtotal
 * equals the full row count, the grouping() bitmask stays in range, the
 * result-row count equals the sum of each set's distinct-group count, a
 * FILTERed count never exceeds the plain count within a set, GROUPING SETS
 * (()) always yields exactly one row, and ROLLUP has no more rows than CUBE.
 * Any "f" is a wrong-results bug in the grouping-sets executor.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_groupsets2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "groupsets2"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Build gs2_t(id int PRIMARY KEY, x int, y int, v int) with x,y in 0..2 and
 * small v, no NULLs in x or y.  Keeping x/y small guarantees most groups are
 * populated, and keeping them non-NULL makes a NULL in a grouping-set result
 * unambiguously the collapsed-column marker rather than real data.  ANALYZE
 * so the planner may pick any grouping-sets strategy.  0..24 rows covers the
 * empty table.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 24);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS gs2_t; "
						   "CREATE TABLE gs2_t (id int PRIMARY KEY, x int, y int, v int); ");
	for (int r = 0; r < nrows; r++)
	{
		int			x = (int) pg_prop_draw_int(tc, 0, 2);
		int			y = (int) pg_prop_draw_int(tc, 0, 2);
		int			v = (int) pg_prop_draw_int(tc, 0, 5);

		appendStringInfo(&ddl, "INSERT INTO gs2_t VALUES (%d, %d, %d, %d); ",
						 r, x, y, v);
	}
	appendStringInfoString(&ddl, "ANALYZE gs2_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build failed: %s\n  SQL: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is a failure.
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
 * In GROUPING SETS ((x),(y),()) the grand-total row -- the one from the empty
 * set () where both columns are collapsed -- aggregates every row, so its
 * count equals the whole table's row count.  grouping(x,y)=3 (both bits set)
 * identifies that row.  The outer CASE guards the empty table (where there is
 * no row to drive a scalar from and count(*)=0), returning true.
 */
PG_PROP(grouping_sets_subtotal_consistency)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN count(*)=0 THEN true "
				 "ELSE (SELECT c FROM (SELECT x,y,count(*) c, grouping(x,y) g "
				 "FROM gs2_t GROUP BY GROUPING SETS ((x),(y),())) s "
				 "WHERE g = 3 LIMIT 1) = (SELECT count(*) FROM gs2_t) END "
				 "FROM gs2_t");

	pg_prop_disconnect(conn);
}

/*
 * grouping(x,y) is a two-bit mask, so across any GROUPING SETS result it must
 * stay in {0,1,2,3}.  bool_and over the result rows must be true; coalesce
 * covers the empty-input case (bool_and over 0 rows is NULL).
 */
PG_PROP(grouping_id_bits)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(g BETWEEN 0 AND 3), true) "
				 "FROM (SELECT grouping(x,y) g FROM gs2_t "
				 "GROUP BY GROUPING SETS ((x),(y),())) s");

	pg_prop_disconnect(conn);
}

/*
 * The number of result rows for GROUPING SETS ((x),(y)) equals the sum over
 * each set of its distinct-group count: count(distinct x) + count(distinct y).
 * No set shares a group with another here (x-only groups vs y-only groups),
 * so the counts simply add.
 */
PG_PROP(groupingsets_rows_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT x FROM gs2_t GROUP BY GROUPING SETS ((x),(y))) s) "
				 "= (SELECT count(DISTINCT x) FROM gs2_t) "
				 "+ (SELECT count(DISTINCT y) FROM gs2_t)");

	pg_prop_disconnect(conn);
}

/*
 * Within any grouping set, a FILTERed count counts a subset of the rows the
 * plain count does, so count(*) FILTER (WHERE v>0) <= count(*).  bool_and over
 * every result row of GROUPING SETS ((x),()) must be true; coalesce covers the
 * empty-input case.
 */
PG_PROP(filter_in_groupingsets)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(cf <= c), true) FROM "
				 "(SELECT count(*) c, count(*) FILTER (WHERE v>0) cf "
				 "FROM gs2_t GROUP BY GROUPING SETS ((x),())) s");

	pg_prop_disconnect(conn);
}

/*
 * GROUP BY GROUPING SETS (()) is a single empty grouping set, which yields
 * exactly one row (the grand total) regardless of input -- even for an empty
 * table, where the aggregate still produces its one grand-total row.
 */
PG_PROP(empty_groupingset_one_row)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT count(*) FROM gs2_t GROUP BY GROUPING SETS (())) s) = 1");

	pg_prop_disconnect(conn);
}

/*
 * ROLLUP(x,y) is GROUPING SETS ((x,y),(x),()) and CUBE(x,y) is
 * ((x,y),(x),(y),()); ROLLUP's sets are a subset of CUBE's and no set emits
 * fewer rows under ROLLUP than CUBE, so ROLLUP's result-row count is at most
 * CUBE's.  Both subqueries return exactly one count, so this holds on every
 * input including the empty table.
 */
PG_PROP(rollup_subset_of_cube_rows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT x,y FROM gs2_t GROUP BY ROLLUP(x,y)) s) "
				 "<= (SELECT count(*) FROM "
				 "(SELECT x,y FROM gs2_t GROUP BY CUBE(x,y)) t)");

	pg_prop_disconnect(conn);
}
