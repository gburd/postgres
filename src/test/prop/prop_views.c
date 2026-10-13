/*-------------------------------------------------------------------------
 *
 * prop_views.c
 *	  Property-based tests for views and the rule system (src/backend/rewrite).
 *
 * A view is syntactic sugar over a query implemented by an ON SELECT rewrite
 * rule, and a simple updatable view maps INSERT/UPDATE/DELETE straight through
 * to its base relation.  The oracles here are therefore definitional: a view's
 * rows must equal its defining query (differential, shape B), and a change
 * made through a simple updatable view must appear in the base table (scalar,
 * shape A).  A materialized view instead snapshots its query at CREATE/REFRESH
 * time, so its stored value must equal the query result as of the last refresh.
 * Any disagreement is a rewrite-rule bug, not a legal result.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_views.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "views"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small integer domains keep each generated table legible in a counterexample
 * and cover the boundaries the view predicates key on (v > 0, g = 0): v and g
 * straddle zero, ids are a dense unique key.
 */
#define VAL_MIN (-5)
#define VAL_MAX 5

/*
 * Build a table named "name" shaped (id int PRIMARY KEY, g int, v int) with
 * 0..25 random rows (ids 1..nrows, g and v in [VAL_MIN, VAL_MAX]).  Drops any
 * prior table of that name first so each property starts fresh.  Fails the
 * case on any DDL/DML error (a build error is a harness problem, not a view
 * property, so surface it loudly).
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn, const char *name)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 25);

	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "DROP TABLE IF EXISTS %s CASCADE; "
					 "CREATE TABLE %s (id int PRIMARY KEY, g int, v int); ",
					 name, name);
	for (int r = 1; r <= nrows; r++)
		appendStringInfo(&ddl,
						 "INSERT INTO %s VALUES (%d, %d, %d); ",
						 name, r,
						 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX),
						 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
	appendStringInfo(&ddl, "ANALYZE %s; ", name);

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  SQL: %s",
					 err ? err : "", ddl.data);
	pfree(ddl.data);
}

/*
 * Shape B: two result-set queries must return byte-identical rows.  Each MUST
 * carry a total ORDER BY so the row-major render compares as an ordered list.
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
 * Shape A: run a boolean SELECT and require "t".  A NULL result or SQL error
 * is a failure, reported with the SQL.
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
 * Run a DDL/DML statement, failing the case on error.  Used for CREATE/DROP
 * VIEW and the through-view writes.
 */
static void
run_ddl(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "DDL failed: %s\n  SQL: %s", err ? err : "", sql);
}

/*
 * Property 1 (B): a view's rows equal its defining query.
 *
 * CREATE VIEW vw_p1_v1 AS SELECT id, v FROM vw_p1 WHERE v > 0; selecting from
 * the view must equal selecting the same projection+filter from the base.
 */
PG_PROP(view_eq_query)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn, "vw_p1");
	run_ddl(tc, conn, "DROP VIEW IF EXISTS vw_p1_v1; "
			"CREATE VIEW vw_p1_v1 AS SELECT id, v FROM vw_p1 WHERE v > 0;");

	require_same_rows(tc, conn,
					  "SELECT id, v FROM vw_p1_v1 ORDER BY id",
					  "SELECT id, v FROM vw_p1 WHERE v > 0 ORDER BY id");

	run_ddl(tc, conn, "DROP VIEW IF EXISTS vw_p1_v1;");
	run_ddl(tc, conn, "DROP TABLE IF EXISTS vw_p1 CASCADE;");
	pg_prop_disconnect(conn);
}

/*
 * Property 2 (B): a view on a view equals the composed query.
 *
 * vw_p2_v1 = rows of vw_p2 with v>0; vw_p2_v2 = ids from vw_p2_v1 with g=0.
 * The composed base query (v>0 AND g=0) must match vw_p2_v2.
 */
PG_PROP(view_of_view)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn, "vw_p2");
	run_ddl(tc, conn,
			"DROP VIEW IF EXISTS vw_p2_v2; "
			"DROP VIEW IF EXISTS vw_p2_v1; "
			"CREATE VIEW vw_p2_v1 AS SELECT * FROM vw_p2 WHERE v > 0; "
			"CREATE VIEW vw_p2_v2 AS SELECT id FROM vw_p2_v1 WHERE g = 0;");

	require_same_rows(tc, conn,
					  "SELECT id FROM vw_p2_v2 ORDER BY id",
					  "SELECT id FROM vw_p2 WHERE v > 0 AND g = 0 ORDER BY id");

	run_ddl(tc, conn, "DROP VIEW IF EXISTS vw_p2_v2; DROP VIEW IF EXISTS vw_p2_v1;");
	run_ddl(tc, conn, "DROP TABLE IF EXISTS vw_p2 CASCADE;");
	pg_prop_disconnect(conn);
}

/*
 * Property 3 (A): INSERT through a simple updatable view hits the base.
 *
 * vw_p3_v1 = SELECT id, v FROM vw_p3; inserting (9999, 5) through the view
 * must create exactly that row in the base table (id 9999 is above the 1..25
 * ids build_table uses, so it never collides with the PRIMARY KEY).
 */
PG_PROP(updatable_view_insert)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn, "vw_p3");
	run_ddl(tc, conn, "DROP VIEW IF EXISTS vw_p3_v1; "
			"CREATE VIEW vw_p3_v1 AS SELECT id, v FROM vw_p3;");
	run_ddl(tc, conn, "INSERT INTO vw_p3_v1 VALUES (9999, 5);");

	/* exactly one base row with id 9999 and v 5 (g defaults to NULL). */
	require_true(tc, conn,
				 "SELECT count(*) = 1 FROM vw_p3 "
				 "WHERE id = 9999 AND v = 5 AND g IS NULL");

	run_ddl(tc, conn, "DROP VIEW IF EXISTS vw_p3_v1;");
	run_ddl(tc, conn, "DROP TABLE IF EXISTS vw_p3 CASCADE;");
	pg_prop_disconnect(conn);
}

/*
 * Property 4 (A): UPDATE through a simple updatable view changes the base.
 *
 * Insert a known row (9999, 7) through the view, UPDATE its v to 0 through the
 * view, then require the base row's v is 0.  Using a self-inserted id avoids
 * depending on which random ids exist.
 */
PG_PROP(updatable_view_update)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn, "vw_p4");
	run_ddl(tc, conn, "DROP VIEW IF EXISTS vw_p4_v1; "
			"CREATE VIEW vw_p4_v1 AS SELECT id, v FROM vw_p4;");
	run_ddl(tc, conn, "INSERT INTO vw_p4_v1 VALUES (9999, 7);");
	run_ddl(tc, conn, "UPDATE vw_p4_v1 SET v = 0 WHERE id = 9999;");

	require_true(tc, conn,
				 "SELECT count(*) = 1 FROM vw_p4 WHERE id = 9999 AND v = 0");

	run_ddl(tc, conn, "DROP VIEW IF EXISTS vw_p4_v1;");
	run_ddl(tc, conn, "DROP TABLE IF EXISTS vw_p4 CASCADE;");
	pg_prop_disconnect(conn);
}

/*
 * Property 5 (A): DELETE through a simple updatable view removes from the base.
 *
 * Insert a known row (9999, 3) through the view, DELETE it through the view,
 * then require no base row with id 9999 remains.
 */
PG_PROP(updatable_view_delete)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn, "vw_p5");
	run_ddl(tc, conn, "DROP VIEW IF EXISTS vw_p5_v1; "
			"CREATE VIEW vw_p5_v1 AS SELECT id, v FROM vw_p5;");
	run_ddl(tc, conn, "INSERT INTO vw_p5_v1 VALUES (9999, 3);");
	run_ddl(tc, conn, "DELETE FROM vw_p5_v1 WHERE id = 9999;");

	require_true(tc, conn,
				 "SELECT count(*) = 0 FROM vw_p5 WHERE id = 9999");

	run_ddl(tc, conn, "DROP VIEW IF EXISTS vw_p5_v1;");
	run_ddl(tc, conn, "DROP TABLE IF EXISTS vw_p5 CASCADE;");
	pg_prop_disconnect(conn);
}

/*
 * Property 6 (A): a materialized view snapshots its query until REFRESH.
 *
 * mv.c = count(*) of vw_p6 at creation.  After inserting one extra row the mv
 * is stale (still equals the old count), and REFRESH makes it equal the new
 * count.  Both the staleness and the post-refresh equality are asserted so a
 * refresh that silently tracked live changes (a bug) would fail the first
 * check and an incorrect refresh would fail the second.
 */
PG_PROP(materialized_view_snapshot)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn, "vw_p6");
	run_ddl(tc, conn, "DROP MATERIALIZED VIEW IF EXISTS vw_p6_mv; "
			"CREATE MATERIALIZED VIEW vw_p6_mv AS SELECT count(*) AS c FROM vw_p6;");

	/* at creation the snapshot equals the live count. */
	require_true(tc, conn,
				 "SELECT (SELECT c FROM vw_p6_mv) = (SELECT count(*) FROM vw_p6)");

	/* insert one more row (id 9999 avoids the 1..25 PK range). */
	run_ddl(tc, conn, "INSERT INTO vw_p6 VALUES (9999, 0, 1);");

	/* now stale: the snapshot still equals the old (pre-insert) count. */
	require_true(tc, conn,
				 "SELECT (SELECT c FROM vw_p6_mv) = (SELECT count(*) - 1 FROM vw_p6)");

	/* refresh re-snapshots: the mv equals the new live count. */
	run_ddl(tc, conn, "REFRESH MATERIALIZED VIEW vw_p6_mv;");
	require_true(tc, conn,
				 "SELECT (SELECT c FROM vw_p6_mv) = (SELECT count(*) FROM vw_p6)");

	run_ddl(tc, conn, "DROP MATERIALIZED VIEW IF EXISTS vw_p6_mv;");
	run_ddl(tc, conn, "DROP TABLE IF EXISTS vw_p6 CASCADE;");
	pg_prop_disconnect(conn);
}
