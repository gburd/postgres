/*-------------------------------------------------------------------------
 *
 * prop_groupingsets.c
 *	  Property-based tests for GROUPING SETS / ROLLUP / CUBE execution.
 *
 * By definition, GROUP BY GROUPING SETS (...) is the UNION ALL of the plain
 * GROUP BYs for each component set, with the columns collapsed by a set
 * returned as NULL.  ROLLUP and CUBE are shorthands for particular grouping
 * set lists.  The oracle rewrites each grouping-set query as that explicit
 * UNION ALL of plain GROUP BY queries and requires identical rows; a mismatch
 * means the grouping-sets executor disagrees with its own definition, which
 * is a wrong-results bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_groupingsets.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "groupingsets"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Build gs_t(id, x, y, v) with x,y in 0..2 and small v, no NULLs in x or y.
 * Keeping x/y small guarantees most groups are populated, and keeping them
 * non-NULL makes a NULL in a grouping-set result unambiguously the "all"
 * marker (a collapsed column) rather than real data.  ANALYZE so the planner
 * may choose any grouping-sets strategy.  0..24 rows covers the empty table.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 24);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS gs_t; "
						   "CREATE TABLE gs_t (id int PRIMARY KEY, x int, y int, v int); ");
	for (int r = 0; r < nrows; r++)
	{
		int			x = (int) pg_prop_draw_int(tc, 0, 2);
		int			y = (int) pg_prop_draw_int(tc, 0, 2);
		int			v = (int) pg_prop_draw_int(tc, 0, 5);

		appendStringInfo(&ddl, "INSERT INTO gs_t VALUES (%d, %d, %d, %d); ",
						 r, x, y, v);
	}
	appendStringInfoString(&ddl, "ANALYZE gs_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build failed: %s\n  SQL: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);
}

/*
 * Differential oracle: run both queries (each already carrying a total ORDER
 * BY) and require identical row-major output.  A SQL error on either side is
 * itself a failure.
 */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *q1, const char *q2)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *r1,
			   *r2;

	r1 = pg_prop_sql_rows(conn, q1, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "q1 failed: %s\n  SQL: %s", e1 ? e1 : "", q1);
	r2 = pg_prop_sql_rows(conn, q2, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "q2 failed: %s\n  SQL: %s", e2 ? e2 : "", q2);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc, "mismatch:\n  q1: %s\n  q2: %s\n  r1:\n%s\n  r2:\n%s",
					 q1, q2, r1, r2);
	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
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
 * ROLLUP(x, y) is GROUPING SETS ((x,y), (x), ()): the UNION ALL of grouping
 * by both columns, by x alone (y collapsed to NULL), and the grand total
 * (both NULL).  The ORDER BY on both sides is x then y with NULLS LAST, which
 * is a total order because distinct (x,y) groups cannot tie.
 */
PG_PROP(rollup_two)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT x, y, count(*), sum(v) FROM gs_t "
					  "GROUP BY ROLLUP(x, y) "
					  "ORDER BY x NULLS LAST, y NULLS LAST",
					  "( SELECT x, y, count(*), sum(v) FROM gs_t GROUP BY x, y "
					  "UNION ALL SELECT x, NULL::int, count(*), sum(v) FROM gs_t GROUP BY x "
					  "UNION ALL SELECT NULL::int, NULL::int, count(*), sum(v) FROM gs_t ) "
					  "ORDER BY x NULLS LAST, y NULLS LAST");

	pg_prop_disconnect(conn);
}

/*
 * CUBE(x, y) is GROUPING SETS ((x,y), (x), (y), ()): all four subsets of the
 * two columns.  Each UNION ALL arm selects NULL::int for the column its set
 * collapses.
 */
PG_PROP(cube_two)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT x, y, count(*), sum(v) FROM gs_t "
					  "GROUP BY CUBE(x, y) "
					  "ORDER BY x NULLS LAST, y NULLS LAST",
					  "( SELECT x, y, count(*), sum(v) FROM gs_t GROUP BY x, y "
					  "UNION ALL SELECT x, NULL::int, count(*), sum(v) FROM gs_t GROUP BY x "
					  "UNION ALL SELECT NULL::int, y, count(*), sum(v) FROM gs_t GROUP BY y "
					  "UNION ALL SELECT NULL::int, NULL::int, count(*), sum(v) FROM gs_t ) "
					  "ORDER BY x NULLS LAST, y NULLS LAST");

	pg_prop_disconnect(conn);
}

/*
 * An explicit GROUPING SETS list that is neither a ROLLUP nor a CUBE:
 * ((x), (y)).  Its definition is the UNION ALL of grouping by x alone (y
 * collapsed) and by y alone (x collapsed); no grand-total or all-columns arm.
 */
PG_PROP(grouping_sets_explicit)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT * FROM (SELECT x, y, count(*), sum(v) FROM gs_t "
					  "GROUP BY GROUPING SETS ((x), (y))) s "
					  "ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3, 4",
					  "SELECT * FROM "
					  "( SELECT x, NULL::int AS y, count(*), sum(v) FROM gs_t GROUP BY x "
					  "UNION ALL SELECT NULL::int AS x, y, count(*), sum(v) FROM gs_t GROUP BY y ) s "
					  "ORDER BY 1 NULLS LAST, 2 NULLS LAST, 3, 4");

	pg_prop_disconnect(conn);
}

/*
 * The GROUPING(x) function marks collapsed columns: it is 1 exactly on the
 * rows where the grouping set left x out.  Since x is never NULL in the data,
 * x comes back NULL in a ROLLUP(x) result iff it was collapsed, so
 * (GROUPING(x) = 1) must equal (x IS NULL) on every result row.  bool_and
 * over those rows must be true; coalesce covers the empty table (ROLLUP still
 * emits the grand-total row, but guard anyway).
 */
PG_PROP(grouping_function_marks_collapsed)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and((g = 1) = (x IS NULL)), true) "
				 "FROM (SELECT x, GROUPING(x) g FROM gs_t GROUP BY ROLLUP(x)) s");

	pg_prop_disconnect(conn);
}

/*
 * The grand-total row of ROLLUP(x, y) (both x and y NULL) aggregates every
 * row, so its count equals the whole table's row count.  ROLLUP emits the
 * grand-total row even for an empty table (with count 0), so this holds on
 * every input; coalesce guards against the row somehow being absent.
 */
PG_PROP(rollup_total_row_matches_ungrouped)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM gs_t) = coalesce("
				 "(SELECT c FROM (SELECT x, y, count(*) c FROM gs_t "
				 "GROUP BY ROLLUP(x, y)) s WHERE x IS NULL AND y IS NULL), 0)");

	pg_prop_disconnect(conn);
}
