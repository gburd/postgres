/*-------------------------------------------------------------------------
 *
 * prop_subqscalar.c
 *	  Property-based tests for scalar-subquery semantics.
 *
 * A scalar subquery must yield exactly one value: at most one row (else a
 * runtime error) and SQL NULL when no row is produced.  An aggregate scalar
 * subquery is the one shape that always returns a row even over zero input
 * rows, so count(*) over an empty filter is 0, not NULL.  These properties
 * pin that single-row/NULL contract and the correlated-scalar-in-SELECT and
 * scalar-in-WHERE forms; a disagreement is a scalar-subquery evaluation bug.
 * Distinct from prop_subquery.c (flattening/pull-up) and prop_corrsubq.c
 * (correlated decorrelation into grouped joins): here the focus is the scalar
 * contract itself -- one value, NULL on no rows -- not the rewrite.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_subqscalar.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "subqscalar"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Narrow domains: g in 0..3 keeps groups dense; v in -4..4 for aggregates. */
#define V_MIN (-4)
#define V_MAX 4

/* Draw an integer in [min, max], or SQL NULL (~15% of the time). */
static void
draw_opt(pg_prop_tc *tc, StringInfo buf, int min, int max)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, min, max));
}

/*
 * Rebuild ss_t fresh: id int PRIMARY KEY (0..n-1), g int in 0..3 (~15% NULL),
 * v int in -4..4 (~15% NULL).  0..25 rows.  ANALYZE so the planner has stats
 * to choose whichever scalar-subquery evaluation strategy it likes -- that
 * choice is what the oracle probes.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ss_t; "
						   "CREATE TABLE ss_t (id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int r = 0; r < nrows; r++)
	{
		StringInfoData g;
		StringInfoData v;

		initStringInfo(&g);
		initStringInfo(&v);
		draw_opt(tc, &g, 0, 3);
		draw_opt(tc, &v, V_MIN, V_MAX);
		appendStringInfo(&ddl, "INSERT INTO ss_t VALUES (%d, %s, %s); ",
						 r, g.data, v.data);
		pfree(g.data);
		pfree(v.data);
	}
	appendStringInfoString(&ddl, "ANALYZE ss_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * Shape A: run a boolean-valued SELECT and require "t".  A NULL result or a
 * SQL error is a failure, reported with the SQL.
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
 * Shape B: run two equivalent query forms and require their full result sets
 * (newline-joined, row-major) to be byte-identical.  Both queries must carry
 * a total ORDER BY so the comparison is a sound multiset compare.
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
 * 1. A scalar subquery over no rows is SQL NULL.  (SELECT v FROM ss_t WHERE
 * false) produces zero rows, and a scalar subquery that yields no row must
 * evaluate to NULL -- so the IS NULL test is true for every table.
 */
PG_PROP(scalar_empty_is_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT v FROM ss_t WHERE false) IS NULL");

	pg_prop_disconnect(conn);
}

/*
 * 2. An aggregate scalar subquery always returns exactly one row, even over
 * zero input rows -- count(*) over an empty filter is 0, not NULL.  This is
 * the single shape that distinguishes an aggregate scalar subquery from a
 * row-driven one, so the result must equal 0, never be NULL.
 */
PG_PROP(scalar_agg_always_one_row)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM ss_t WHERE false) = 0");

	pg_prop_disconnect(conn);
}

/*
 * 3. Correlated scalar max in SELECT == grouped LEFT JOIN max.  Per-outer-row
 * (SELECT max(u.v) ... WHERE u.g = t.g) equals the group's max from a LEFT
 * JOIN on g.  Both sides use = on g, so the NULL-g outer rows get no match on
 * either side (= is UNKNOWN against NULL) and yield NULL consistently; the
 * LEFT JOIN keeps every t.  max over an empty/all-NULL group is NULL, matching
 * the correlated form.  Total order by id.
 */
PG_PROP(correlated_max_in_select)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id, (SELECT max(u.v) FROM ss_t u WHERE u.g = t.g) m "
					  "FROM ss_t t ORDER BY id",
					  "SELECT t.id, mx.m FROM ss_t t "
					  "LEFT JOIN (SELECT g, max(v) m FROM ss_t GROUP BY g) mx "
					  "ON mx.g = t.g "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * 4. Scalar subquery in WHERE pulled into a derived column.  The uncorrelated
 * (SELECT avg(v) FROM ss_t) is one value for every outer row; comparing v to
 * it in WHERE must equal first materializing it as a derived column a and then
 * filtering v >= a.  avg over an empty/all-NULL table is NULL, so v >= NULL is
 * UNKNOWN on both sides and selects nothing -- consistent.  Total order by id.
 */
PG_PROP(scalar_in_where)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM ss_t WHERE v >= (SELECT avg(v) FROM ss_t) "
					  "ORDER BY id",
					  "SELECT id FROM "
					  "(SELECT id, v, (SELECT avg(v) FROM ss_t) a FROM ss_t) s "
					  "WHERE v >= a ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * 5. EXISTS == scalar count > 0.  A row exists for g=0 exactly when the
 * aggregate scalar count of such rows is positive.  count(*) is never NULL,
 * so the comparison is two-valued and the equality is total.
 */
PG_PROP(exists_vs_scalar_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT EXISTS (SELECT 1 FROM ss_t WHERE g = 0) "
				 "= ((SELECT count(*) FROM ss_t WHERE g = 0) > 0)");

	pg_prop_disconnect(conn);
}

/*
 * 6. A constant scalar subquery is its constant.  (SELECT 42) returns one row
 * of one column, so it must evaluate to 42.
 */
PG_PROP(scalar_subquery_const)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn, "SELECT (SELECT 42) = 42");

	pg_prop_disconnect(conn);
}

/*
 * 7. Nested scalar subqueries collapse to the innermost constant.  Three
 * levels of (SELECT (SELECT ...)) each return one row of one column, so the
 * whole thing must evaluate to 7.
 */
PG_PROP(nested_scalar)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn, "SELECT (SELECT (SELECT (SELECT 7))) = 7");

	pg_prop_disconnect(conn);
}
