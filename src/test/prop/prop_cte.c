/*-------------------------------------------------------------------------
 *
 * prop_cte.c
 *	  Property-based tests for common table expressions (WITH / WITH RECURSIVE).
 *
 * The oracle is differential: a CTE is a syntactic convenience and a
 * materialisation boundary, never a change in results.  An inlinable CTE must
 * return the same rows as the same CTE forced MATERIALIZED and as the
 * equivalent subquery; a recursive CTE walking a forest (parent is always a
 * strictly smaller id, or NULL for a root, so there are no cycles and the walk
 * always terminates) must visit exactly every node, each once.  Any
 * disagreement is a planner/executor bug in CTE handling, not a legal
 * difference.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_cte.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cte"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Differential result-set oracle: q1 and q2 must return identical rows in
 * identical order.  Both queries must carry a TOTAL ORDER BY (here on id, the
 * primary key) so the row-major string compares as a multiset.
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
 * Scalar boolean oracle: run a "SELECT <bool>" and require "t".  A NULL result
 * or a SQL error is itself a failure.
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
 * Build ct_t(id int PRIMARY KEY, parent int, v int) with 0..20 rows forming a
 * forest: ids are 1..n assigned densely, and each row's parent is either NULL
 * (a root) or a strictly smaller id.  parent < id guarantees acyclicity, so a
 * recursive walk from the roots always terminates and reaches every node.
 *
 * We draw parent in [0, id-1]; 0 encodes NULL (a root).  v is a small int in
 * [-5, 5].  ANALYZE so the planner has stats.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ct_t; "
						   "CREATE TABLE ct_t (id int PRIMARY KEY, parent int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 20);
	for (int id = 1; id <= nrows; id++)
	{
		int			p = (int) pg_prop_draw_int(tc, 0, id - 1);
		int			v = (int) pg_prop_draw_int(tc, -5, 5);

		if (p == 0)
			appendStringInfo(&ddl,
							 "INSERT INTO ct_t VALUES (%d, NULL, %d); ", id, v);
		else
			appendStringInfo(&ddl,
							 "INSERT INTO ct_t VALUES (%d, %d, %d); ", id, p, v);
	}

	appendStringInfoString(&ddl, "ANALYZE ct_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  SQL: %s",
					 err ? err : "", ddl.data);
	pfree(ddl.data);
}

/*
 * A non-recursive CTE returns the same rows whether the planner inlines it or
 * is forced to materialise it.  MATERIALIZED is a hint that changes execution
 * but never results.
 */
PG_PROP(inline_vs_materialized)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "WITH w AS (SELECT id, v FROM ct_t WHERE v > 0) "
					  "SELECT id FROM w ORDER BY id",
					  "WITH w AS MATERIALIZED (SELECT id, v FROM ct_t WHERE v > 0) "
					  "SELECT id FROM w ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * A simple CTE and the equivalent FROM-subquery must agree.  Inlining a
 * single-reference CTE is exactly this rewrite, so any difference is a bug.
 */
PG_PROP(cte_vs_subquery)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "WITH w AS (SELECT id FROM ct_t WHERE v <= 0) "
					  "SELECT id FROM w ORDER BY id",
					  "SELECT id FROM (SELECT id FROM ct_t WHERE v <= 0) s "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * A recursive CTE walking children down from the roots reaches exactly every
 * node.  Because every row's parent is a strictly smaller id (or NULL), every
 * node has a root ancestor, so the full table is reachable.
 */
PG_PROP(recursive_reaches_all_descendants)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "WITH RECURSIVE r AS ("
					  "SELECT id FROM ct_t WHERE parent IS NULL "
					  "UNION ALL "
					  "SELECT c.id FROM ct_t c JOIN r ON c.parent = r.id) "
					  "SELECT id FROM r ORDER BY id",
					  "SELECT id FROM ct_t ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * Every node's computed depth via a recursive CTE is >= 1, and a root (the
 * base case) has depth 1.  coalesce(...,true) makes the empty-forest case
 * (bool_and over 0 rows is NULL) pass soundly.
 */
PG_PROP(recursive_depth_nonneg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(depth >= 1), true) FROM ("
				 "WITH RECURSIVE r AS ("
				 "SELECT id, 1 AS depth FROM ct_t WHERE parent IS NULL "
				 "UNION ALL "
				 "SELECT c.id, r.depth + 1 FROM ct_t c JOIN r ON c.parent = r.id) "
				 "SELECT depth FROM r) s");

	pg_prop_disconnect(conn);
}

/*
 * The recursive walk visits each node exactly once.  UNION ALL (not UNION)
 * keeps every emitted row, so a duplicate visit would make the walk's count
 * exceed the table's count -- the equality catches it.  Since it's a forest,
 * each node has a unique parent chain to one root, so the counts must match.
 */
PG_PROP(recursive_count_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM ct_t) = "
				 "(SELECT count(*) FROM ("
				 "WITH RECURSIVE r AS ("
				 "SELECT id FROM ct_t WHERE parent IS NULL "
				 "UNION ALL "
				 "SELECT c.id FROM ct_t c JOIN r ON c.parent = r.id) "
				 "SELECT id FROM r) q)");

	pg_prop_disconnect(conn);
}

/*
 * A data-modifying CTE modifies its target exactly once: the DELETE ... CTE's
 * RETURNING row count must equal the number of rows that matched before the
 * delete.  We capture the pre-count on a scratch copy, run the DELETE-CTE, and
 * compare.  This exercises the "modify once, snapshot the target once"
 * semantics of data-modifying WITH.
 */
PG_PROP(data_modifying_cte_returning)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char	   *expected;
	bool		failed = false;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	if (!pg_prop_sql_exec(conn,
						  "DROP TABLE IF EXISTS ct_copy; "
						  "CREATE TABLE ct_copy AS SELECT * FROM ct_t;",
						  &err))
		pg_prop_fail(tc, "ct_copy build failed: %s", err ? err : "");

	expected = pg_prop_sql_scalar(conn,
								  "SELECT count(*) FROM ct_copy WHERE v > 0",
								  &failed, &err);
	if (failed || expected == NULL)
		pg_prop_fail(tc, "pre-count failed: %s", err ? err : "(null)");

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "WITH d AS (DELETE FROM ct_copy WHERE v > 0 "
						   "RETURNING id) SELECT count(*) FROM d");

	{
		char	   *got;
		bool		gf = false;
		char	   *ge = NULL;

		/*
		 * A data-modifying CTE must be the top-level statement, so run it
		 * directly and compare its RETURNING count in C to the pre-count.
		 */
		got = pg_prop_sql_scalar(conn, q.data, &gf, &ge);
		if (gf || got == NULL)
			pg_prop_fail(tc, "data-modifying CTE failed: %s",
						 ge ? ge : "(null)");
		if (strcmp(got, expected) != 0)
			pg_prop_fail(tc,
						 "RETURNING count %s != pre-count %s of v>0 rows",
						 got, expected);
		pg_free(got);
		if (ge)
			pg_free(ge);
	}

	pfree(q.data);
	pg_free(expected);
	if (err)
		pg_free(err);

	if (!pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS ct_copy;", &err))
		pg_prop_fail(tc, "ct_copy drop failed: %s", err ? err : "");

	pg_prop_disconnect(conn);
}
