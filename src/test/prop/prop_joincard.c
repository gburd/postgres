/*-------------------------------------------------------------------------
 *
 * prop_joincard.c
 *	  Property-based tests for join cardinality and empty/identity-join laws.
 *
 * The oracle is relational algebra on two small random tables, jc_a and jc_b:
 * a cross join has exactly count(A)*count(B) rows, an equijoin can only be a
 * subset of that cross, joining with an empty relation produces no rows, a
 * LEFT JOIN against an empty relation preserves the left side exactly, an
 * inner join commutes as a multiset, CROSS JOIN equals JOIN ON true, and a
 * FULL JOIN keeps at least max(count(A), count(B)) rows.  Any violation is a
 * planner/executor join bug, since every law here is a theorem of the join
 * semantics regardless of data or plan shape.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_joincard.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "joincard"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Build jc_a and jc_b fresh.  Each is (id int PRIMARY KEY, k int) with 0..15
 * rows, k drawn from 0..3 and NULL ~15% of the time.  Small join keys force
 * many matches so an equijoin's cardinality is a non-trivial fraction of the
 * cross product; the id is a dense 0..n-1 key that gives every oracle a total
 * order to compare on.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn, const char *name)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "DROP TABLE IF EXISTS %s; "
					 "CREATE TABLE %s (id int PRIMARY KEY, k int); ",
					 name, name);

	nrows = (int) pg_prop_draw_int(tc, 0, 15);
	for (int i = 0; i < nrows; i++)
	{
		if (pg_prop_draw_int(tc, 0, 6) == 0)
			appendStringInfo(&ddl, "INSERT INTO %s VALUES (%d, NULL); ",
							 name, i);
		else
			appendStringInfo(&ddl, "INSERT INTO %s VALUES (%d, %d); ",
							 name, i, (int) pg_prop_draw_int(tc, 0, 3));
	}
	appendStringInfo(&ddl, "ANALYZE %s; ", name);

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build %s failed: %s\n  ddl: %s",
					 name, err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

static void
build_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	build_table(tc, conn, "jc_a");
	build_table(tc, conn, "jc_b");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is itself a failure, reported with the SQL.
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
 * Compare two result sets that must be identical multisets.  Both queries
 * MUST carry a total ORDER BY so the row-major string compares correctly.
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
 * A: count(A CROSS JOIN B) = count(A) * count(B).  The defining cardinality
 * of a cross product; holds for every table including the empty ones.
 */
PG_PROP(cross_join_card)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM jc_a a CROSS JOIN jc_b b) "
				 "= (SELECT count(*) FROM jc_a) * (SELECT count(*) FROM jc_b)");

	pg_prop_disconnect(conn);
}

/*
 * A: count(A JOIN B ON a.k=b.k) <= count(A) * count(B).  An equijoin is a
 * filtered subset of the cross product, so it can never have more rows.
 */
PG_PROP(inner_join_le_cross)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM jc_a a JOIN jc_b b ON a.k = b.k) "
				 "<= (SELECT count(*) FROM jc_a) * (SELECT count(*) FROM jc_b)");

	pg_prop_disconnect(conn);
}

/*
 * A: A JOIN (empty) is empty.  Inner-joining anything with a relation that
 * has no rows yields no rows, regardless of the ON condition.
 */
PG_PROP(join_empty_is_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM jc_a a "
				 "JOIN (SELECT * FROM jc_b WHERE false) b ON true) = 0");

	pg_prop_disconnect(conn);
}

/*
 * A: A LEFT JOIN (empty) keeps the left side.  Every left row survives with
 * NULL-extended right columns, so the count equals count(A).
 */
PG_PROP(left_join_empty_keeps_left)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM jc_a a "
				 "LEFT JOIN (SELECT * FROM jc_b WHERE false) b ON true) "
				 "= (SELECT count(*) FROM jc_a)");

	pg_prop_disconnect(conn);
}

/*
 * B: an inner join commutes as a multiset.  A JOIN B and B JOIN A, projected
 * to (a.id, b.id), must return identical rows in identical order.
 */
PG_PROP(join_commutes)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT a.id, b.id FROM jc_a a JOIN jc_b b ON a.k = b.k "
					  "ORDER BY a.id, b.id",
					  "SELECT a.id, b.id FROM jc_b b JOIN jc_a a ON a.k = b.k "
					  "ORDER BY a.id, b.id");

	pg_prop_disconnect(conn);
}

/*
 * B: CROSS JOIN equals JOIN ON true.  A cross product and an inner join whose
 * condition is a tautology are the same relation.
 */
PG_PROP(cross_eq_join_true)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT a.id, b.id FROM jc_a a CROSS JOIN jc_b b "
					  "ORDER BY a.id, b.id",
					  "SELECT a.id, b.id FROM jc_a a JOIN jc_b b ON true "
					  "ORDER BY a.id, b.id");

	pg_prop_disconnect(conn);
}

/*
 * A: count(A FULL JOIN B ON a.k=b.k) >= greatest(count(A), count(B)).  A full
 * outer join keeps every row of both inputs (matched together or NULL-
 * extended), so it cannot be smaller than either side.
 */
PG_PROP(full_join_card_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM jc_a a FULL JOIN jc_b b "
				 "ON a.k = b.k) "
				 ">= greatest((SELECT count(*) FROM jc_a), "
				 "(SELECT count(*) FROM jc_b))");

	pg_prop_disconnect(conn);
}
