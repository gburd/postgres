/*-------------------------------------------------------------------------
 *
 * prop_rollup2.c
 *	  Property-based tests for ROLLUP/CUBE grand-total and subtotal algebra.
 *
 * ROLLUP(x,y) and CUBE(x,y) emit, in addition to the detail groups, the
 * collapsed "subtotal" and "grand-total" rows that GROUP BY x,y does not.
 * These rows carry structural guarantees: the grand-total aggregates the
 * whole table, every x-subtotal aggregates exactly that x, CUBE emits a
 * superset of ROLLUP's groups, GROUPING() reports which columns collapsed,
 * and ROLLUP's detail rows match a plain GROUP BY x,y.  Each property is a
 * scalar boolean oracle: a wrong grouping-set result is a bug in the
 * grouping-sets executor, not a rounding artefact.  The data has no NULL in
 * x/y, so any NULL in a result column means "collapsed by the grouping set".
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_rollup2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "rollup2"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the grouping-set query errored or produced NULL where the
 * oracle demands a definite true, which is itself a bug -- so either is a
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
 * Build r2_t(id int PRIMARY KEY, x int, y int, v int) with 0..24 rows.  x and
 * y are each drawn from {0,1,2} with no NULL, so a NULL in any ROLLUP/CUBE
 * result column unambiguously marks a collapsed grouping column rather than a
 * data NULL.  v is a small integer so sums stay legible in a counterexample.
 * The table is rebuilt fresh in every property and ANALYZEd.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS r2_t; "
						   "CREATE TABLE r2_t(id int PRIMARY KEY, x int, y int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 24);
	for (int r = 0; r < nrows; r++)
	{
		int			x = (int) pg_prop_draw_int(tc, 0, 2);
		int			y = (int) pg_prop_draw_int(tc, 0, 2);
		int			v = (int) pg_prop_draw_int(tc, 0, 9);

		appendStringInfo(&ddl, "INSERT INTO r2_t VALUES (%d, %d, %d, %d); ",
						 r, x, y, v);
	}
	appendStringInfoString(&ddl, "ANALYZE r2_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * Property 1: the grand-total row of ROLLUP(x,y) (x and y both NULL, i.e. both
 * collapsed) aggregates the whole table: its sum(v) equals sum(v) over r2_t.
 * coalesce guards the empty table (no grand-total row exists) to true.
 */
PG_PROP(rollup_grand_total)
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
						   "SELECT coalesce("
						   "(SELECT g.s FROM "
						   "  (SELECT x, y, sum(v) s FROM r2_t GROUP BY ROLLUP(x,y)) g "
						   "  WHERE x IS NULL AND y IS NULL LIMIT 1) "
						   "IS NOT DISTINCT FROM (SELECT sum(v) FROM r2_t), true)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 2: each x-subtotal of ROLLUP(x,y) (y collapsed to NULL, x not NULL)
 * equals the plain GROUP BY x sum for that same x.  Join the ROLLUP subtotal
 * rows to GROUP BY x on x and require every pair to agree; bool_and over the
 * join, coalesce'd to true for the empty table (no subtotal rows).
 */
PG_PROP(rollup_subtotal_sums)
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
						   "SELECT coalesce("
						   "(SELECT bool_and(g.s = h.s) FROM "
						   "  (SELECT x, y, sum(v) s FROM r2_t GROUP BY ROLLUP(x,y)) g "
						   "  JOIN (SELECT x, sum(v) s FROM r2_t GROUP BY x) h "
						   "  ON g.x = h.x "
						   "  WHERE g.x IS NOT NULL AND g.y IS NULL), true)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 3: CUBE(x,y) emits at least as many groups as ROLLUP(x,y).  CUBE
 * enumerates all 4 subsets of {x,y} while ROLLUP enumerates only the 3 nested
 * prefixes, so for any data CUBE's group count >= ROLLUP's.  Both counts are
 * definite (count(*) over a grouped subquery is non-NULL), so no coalesce is
 * needed; the empty table gives 1 >= 1.
 */
PG_PROP(cube_more_rows_than_rollup)
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
						   "SELECT "
						   "(SELECT count(*) FROM "
						   "  (SELECT x, y FROM r2_t GROUP BY CUBE(x,y)) c) >= "
						   "(SELECT count(*) FROM "
						   "  (SELECT x, y FROM r2_t GROUP BY ROLLUP(x,y)) r)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 4: the detail rows of ROLLUP(x,y) (both x and y non-NULL, i.e.
 * nothing collapsed) match GROUP BY x,y exactly.  Since x/y are never NULL in
 * the data, ROLLUP's detail set is precisely the GROUP BY x,y set, so their
 * group counts are equal.  Both counts are definite; equal to 0 on empty.
 */
PG_PROP(rollup_detail_matches_groupby)
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
						   "SELECT "
						   "(SELECT count(*) FROM "
						   "  (SELECT x, y FROM r2_t GROUP BY ROLLUP(x,y)) g "
						   "  WHERE x IS NOT NULL AND y IS NOT NULL) = "
						   "(SELECT count(*) FROM "
						   "  (SELECT x, y FROM r2_t GROUP BY x, y) h)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 5: GROUPING(x,y) over CUBE(x,y) yields only values in {0,1,2,3}.
 * The 2-bit grouping bitmask (bit 1 for x collapsed, bit 0 for y collapsed)
 * cannot exceed 3 for two columns.  bool_and over the distinct grouping
 * values, coalesce'd to true for the empty table.
 */
PG_PROP(grouping_bits)
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
						   "SELECT coalesce("
						   "(SELECT bool_and(gb BETWEEN 0 AND 3) FROM "
						   "  (SELECT GROUPING(x,y) gb FROM r2_t GROUP BY CUBE(x,y)) s), "
						   "true)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 6: the grand-total row of ROLLUP(x,y) counts every row of the
 * table: its count(*) equals (SELECT count(*) FROM r2_t).  Analogous to
 * property 1 but on count rather than sum.  coalesce guards the empty table
 * (no grand-total row) to true.
 */
PG_PROP(rollup_total_count)
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
						   "SELECT coalesce("
						   "(SELECT c FROM "
						   "  (SELECT x, y, count(*) c FROM r2_t GROUP BY ROLLUP(x,y)) g "
						   "  WHERE x IS NULL AND y IS NULL LIMIT 1) "
						   "IS NOT DISTINCT FROM (SELECT count(*) FROM r2_t), true)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
