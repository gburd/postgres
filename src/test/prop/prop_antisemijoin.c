/*-------------------------------------------------------------------------
 *
 * prop_antisemijoin.c
 *	  Property-based tests for semi-join / anti-join plan equivalences.
 *
 * EXISTS/IN produce semi-joins and NOT EXISTS/NOT IN produce anti-joins; the
 * executor may realise either with a hash join or a nested loop, and the
 * planner rewrites the subquery forms into one another.  The oracle is
 * differential: a semi/anti query must return the same rows however it is
 * phrased (EXISTS vs IN) and however it is executed (hashjoin vs nestloop),
 * and the semi and anti partitions must together reconstruct every row.  Any
 * disagreement is a planner or executor wrong-results bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_antisemijoin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "antisemijoin"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Keep k in a small non-null domain (0..4).  Non-null k is what makes
 * NOT IN sound here: NOT IN against a subquery that yields a NULL returns
 * UNKNOWN for every outer row, which would make the EXISTS/IN rewrites
 * diverge.  Holding b.k non-null keeps NOT EXISTS == NOT IN true.
 */
#define K_MIN 0
#define K_MAX 4

/*
 * Build as_a and as_b, each (id int PRIMARY KEY, k int) with 0..18 rows and
 * k in 0..4.  as_b is sometimes left empty (the anti-join-returns-everything
 * / semi-join-returns-nothing boundary).  ANALYZE both so the planner has
 * stats to choose among the plan shapes.
 */
static void
build_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			na;
	int			nb;
	bool		b_empty;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS as_a; "
						   "DROP TABLE IF EXISTS as_b; "
						   "CREATE TABLE as_a (id int PRIMARY KEY, k int); "
						   "CREATE TABLE as_b (id int PRIMARY KEY, k int); ");

	na = (int) pg_prop_draw_int(tc, 0, 18);
	for (int i = 0; i < na; i++)
		appendStringInfo(&ddl, "INSERT INTO as_a VALUES (%d, %d); ",
						 i, (int) pg_prop_draw_int(tc, K_MIN, K_MAX));

	/* as_b empty one case in four. */
	b_empty = (pg_prop_draw_int(tc, 0, 3) == 0);
	nb = b_empty ? 0 : (int) pg_prop_draw_int(tc, 0, 18);
	for (int i = 0; i < nb; i++)
		appendStringInfo(&ddl, "INSERT INTO as_b VALUES (%d, %d); ",
						 i, (int) pg_prop_draw_int(tc, K_MIN, K_MAX));

	appendStringInfoString(&ddl, "ANALYZE as_a; ANALYZE as_b; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build failed: %s\n  SQL: %s", err ? err : "", ddl.data);

	pfree(ddl.data);
}

/*
 * Shape B differential oracle: both queries must carry a total ORDER BY so
 * the row-major string compares as an ordered multiset.
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
 * Run one query twice -- once forcing a hash join, once forcing a nested loop
 * -- and require identical rows.  Resets the GUCs afterward.  The query must
 * carry a total ORDER BY (see require_same_rows).
 */
static void
run_two(pg_prop_tc *tc, pg_prop_conn *conn, const char *q)
{
	char	   *err = NULL;
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *r1,
			   *r2;

	/* Force hash join. */
	pg_prop_sql_exec(conn,
					 "SET enable_hashjoin = on; "
					 "SET enable_nestloop = off; "
					 "SET enable_mergejoin = off;", &err);
	r1 = pg_prop_sql_rows(conn, q, &f1, &e1);

	/* Force nested loop. */
	pg_prop_sql_exec(conn,
					 "SET enable_hashjoin = off; "
					 "SET enable_nestloop = on; "
					 "SET enable_mergejoin = off;", &err);
	r2 = pg_prop_sql_rows(conn, q, &f2, &e2);

	pg_prop_sql_exec(conn, "RESET ALL;", &err);

	if (f1)
		pg_prop_fail(tc, "hashjoin run failed: %s\n  SQL: %s", e1 ? e1 : "", q);
	if (f2)
		pg_prop_fail(tc, "nestloop run failed: %s\n  SQL: %s", e2 ? e2 : "", q);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc,
					 "hashjoin != nestloop:\n  SQL: %s\n  hash:\n%s\n  nestloop:\n%s",
					 q, r1, r2);

	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * EXISTS over a correlated equality is the same as IN over the (non-null)
 * key column.  k is 0..4 (non-null) so IN and EXISTS agree with no NULL
 * three-valued-logic surprise.
 */
PG_PROP(exists_eq_in_nonnull)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM as_a a "
					  "WHERE EXISTS (SELECT 1 FROM as_b b WHERE b.k = a.k) "
					  "ORDER BY id",
					  "SELECT id FROM as_a "
					  "WHERE k IN (SELECT k FROM as_b WHERE k IS NOT NULL) "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * NOT EXISTS is the same as NOT IN *only* when the inner key is non-null:
 * a NULL in as_b.k would make NOT IN return no rows regardless.  k is 0..4
 * (non-null), so NOT EXISTS == NOT IN holds.
 */
PG_PROP(not_exists_eq_not_in_nonnull)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM as_a a "
					  "WHERE NOT EXISTS (SELECT 1 FROM as_b b WHERE b.k = a.k) "
					  "ORDER BY id",
					  "SELECT id FROM as_a "
					  "WHERE k NOT IN (SELECT k FROM as_b) "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * The semi-join (EXISTS) must return the same rows whether executed as a hash
 * semi-join or a nested-loop semi-join.
 */
PG_PROP(semijoin_hash_vs_nestloop)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	run_two(tc, conn,
			"SELECT id FROM as_a a "
			"WHERE EXISTS (SELECT 1 FROM as_b b WHERE b.k = a.k) "
			"ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * The anti-join (NOT EXISTS) must return the same rows whether executed as a
 * hash anti-join or a nested-loop anti-join.
 */
PG_PROP(antijoin_hash_vs_nestloop)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	run_two(tc, conn,
			"SELECT id FROM as_a a "
			"WHERE NOT EXISTS (SELECT 1 FROM as_b b WHERE b.k = a.k) "
			"ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * The semi-join and the anti-join partition the outer rows: every row of as_a
 * either has a match (EXISTS) or has none (NOT EXISTS), never both and never
 * neither.  So (EXISTS rows) UNION ALL (NOT EXISTS rows) must reconstruct all
 * of as_a exactly.
 */
PG_PROP(semi_plus_anti_eq_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM as_a ORDER BY id",
					  "SELECT id FROM ("
					  "SELECT id FROM as_a a "
					  "WHERE EXISTS (SELECT 1 FROM as_b b WHERE b.k = a.k) "
					  "UNION ALL "
					  "SELECT id FROM as_a a "
					  "WHERE NOT EXISTS (SELECT 1 FROM as_b b WHERE b.k = a.k)"
					  ") s ORDER BY id");

	pg_prop_disconnect(conn);
}
