/*-------------------------------------------------------------------------
 *
 * prop_recursivecte.c
 *	  Property-based tests for WITH RECURSIVE traversal semantics.
 *
 * Distinct from the "cte" area (which checks inlining/materialisation and a
 * single descendant walk): this area targets the recursive-worktable engine
 * itself -- UNION (dedup) vs UNION ALL, depth-limited recursion, and
 * transitive closure.  On a FOREST (parent is NULL or a strictly smaller id,
 * so no cycles) every recursion must terminate and the dedup/non-dedup and
 * capped/uncapped variants must agree.  Any disagreement, non-termination, or
 * missing edge is a bug in recursive-CTE execution, not a legal difference.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_recursivecte.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "recursivecte"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Differential result-set oracle: q1 and q2 must return identical rows in
 * identical order.  Both queries must carry a TOTAL ORDER BY so the row-major
 * string compares as a multiset.
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
 * Build rc_t(id int PRIMARY KEY, parent int) with 0..20 rows forming a FOREST:
 * ids are 1..n assigned densely, and each row's parent is either NULL (a root)
 * or a strictly smaller id.  parent < id guarantees acyclicity, so every
 * recursive walk terminates and the depth of any node is bounded by the row
 * count.
 *
 * We draw parent in [0, id-1]; 0 encodes NULL (a root).  ANALYZE so the
 * planner has stats.
 */
static void
build_forest(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS rc_t; "
						   "CREATE TABLE rc_t (id int PRIMARY KEY, parent int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 20);
	for (int id = 1; id <= nrows; id++)
	{
		int			p = (int) pg_prop_draw_int(tc, 0, id - 1);

		if (p == 0)
			appendStringInfo(&ddl,
							 "INSERT INTO rc_t VALUES (%d, NULL); ", id);
		else
			appendStringInfo(&ddl,
							 "INSERT INTO rc_t VALUES (%d, %d); ", id, p);
	}

	appendStringInfoString(&ddl, "ANALYZE rc_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "forest build failed: %s\n  SQL: %s",
					 err ? err : "", ddl.data);
	pfree(ddl.data);
}

/*
 * Property 1 (A): on a forest, UNION recursion from the roots reaches every
 * node.  Every id is connected to some root (follow parent links down to a
 * NULL), so the reachable set is all ids; its count equals the total row
 * count.  A dedup bug or a worktable-swap bug would drop or duplicate nodes.
 */
PG_PROP(union_terminates_on_forest)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_forest(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT (SELECT count(*) FROM ("
						   "  WITH RECURSIVE r AS ("
						   "    SELECT id FROM rc_t WHERE parent IS NULL"
						   "    UNION"
						   "    SELECT c.id FROM rc_t c JOIN r ON c.parent = r.id"
						   "  ) SELECT id FROM r"
						   ") s) = (SELECT count(*) FROM rc_t)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 2 (B): on a forest (no cycles, each node a single parent), UNION
 * (dedup) and UNION ALL produce the same set of ids.  With no shared
 * descendants reached by two paths and no cycles, dedup removes nothing, so
 * the two id multisets are identical.  A divergence means UNION ALL
 * re-enqueued a node it should not have, or UNION dropped one it should keep.
 */
PG_PROP(union_all_same_as_union_on_tree)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q1,
				q2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_forest(tc, conn);

	initStringInfo(&q1);
	appendStringInfoString(&q1,
						   "SELECT id FROM ("
						   "  WITH RECURSIVE r AS ("
						   "    SELECT id FROM rc_t WHERE parent IS NULL"
						   "    UNION"
						   "    SELECT c.id FROM rc_t c JOIN r ON c.parent = r.id"
						   "  ) SELECT id FROM r"
						   ") s ORDER BY id");

	initStringInfo(&q2);
	appendStringInfoString(&q2,
						   "SELECT id FROM ("
						   "  WITH RECURSIVE r AS ("
						   "    SELECT id FROM rc_t WHERE parent IS NULL"
						   "    UNION ALL"
						   "    SELECT c.id FROM rc_t c JOIN r ON c.parent = r.id"
						   "  ) SELECT id FROM r"
						   ") s ORDER BY id");

	require_same_rows(tc, conn, q1.data, q2.data);

	pfree(q1.data);
	pfree(q2.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 3 (A): walking from each node up to its root, the depth computed by
 * the recursion is 1 + number of proper ancestors, hence always >= 1 for
 * every node.  bool_and over all nodes must be true; coalesce(...,true) keeps
 * the empty forest sound (no rows -> vacuously true).  A depth-column bug in
 * the recursive term would make some node report depth < 1.
 */
PG_PROP(depth_matches_ancestors)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_forest(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT coalesce(bool_and(depth >= 1), true) FROM ("
						   "  WITH RECURSIVE r AS ("
						   "    SELECT id, parent, 1 AS depth FROM rc_t WHERE parent IS NULL"
						   "    UNION ALL"
						   "    SELECT c.id, c.parent, r.depth + 1"
						   "      FROM rc_t c JOIN r ON c.parent = r.id"
						   "  ) SELECT depth FROM r"
						   ") s");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 4 (A): the transitive closure of (ancestor, descendant) pairs
 * contains every direct parent-child edge.  Build the closure recursively from
 * the direct edges (parent IS NOT NULL) and chain through, then assert the
 * closure's pair count is at least the number of direct edges -- every direct
 * edge is itself an (ancestor, descendant) pair in the closure, so the count
 * can only be greater or equal.  A missing-base-row or missing-chaining bug
 * would drop pairs and break the bound.
 */
PG_PROP(transitive_closure_contains_edges)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_forest(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT (SELECT count(*) FROM ("
						   "  WITH RECURSIVE closure AS ("
						   "    SELECT parent AS ancestor, id AS descendant"
						   "      FROM rc_t WHERE parent IS NOT NULL"
						   "    UNION"
						   "    SELECT cl.ancestor, c.id"
						   "      FROM rc_t c JOIN closure cl ON c.parent = cl.descendant"
						   "  ) SELECT ancestor, descendant FROM closure"
						   ") cc) >= (SELECT count(*) FROM rc_t WHERE parent IS NOT NULL)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 5 (A): a depth-capped recursion (WHERE depth < 100) returns the
 * same node set as the uncapped recursion.  On a forest of at most 20 rows the
 * depth of any node is bounded by the row count (<< 100), so the cap never
 * fires and both must enumerate exactly the same ids.  Comparison is done in
 * C: both counts of distinct ids must be equal, and both must equal the full
 * row count (the cap must not hide anyone).  A premature-cutoff bug would make
 * the capped count smaller.
 */
PG_PROP(cycle_guard_terminates)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData capped,
				uncapped;
	char	   *rc,
			   *ru,
			   *rtot;
	bool		fc = false,
				fu = false,
				ft = false;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_forest(tc, conn);

	initStringInfo(&capped);
	appendStringInfoString(&capped,
						   "SELECT count(DISTINCT id) FROM ("
						   "  WITH RECURSIVE r AS ("
						   "    SELECT id, 1 AS depth FROM rc_t WHERE parent IS NULL"
						   "    UNION ALL"
						   "    SELECT c.id, r.depth + 1"
						   "      FROM rc_t c JOIN r ON c.parent = r.id"
						   "     WHERE r.depth < 100"
						   "  ) SELECT id FROM r"
						   ") s");

	initStringInfo(&uncapped);
	appendStringInfoString(&uncapped,
						   "SELECT count(DISTINCT id) FROM ("
						   "  WITH RECURSIVE r AS ("
						   "    SELECT id FROM rc_t WHERE parent IS NULL"
						   "    UNION"
						   "    SELECT c.id FROM rc_t c JOIN r ON c.parent = r.id"
						   "  ) SELECT id FROM r"
						   ") s");

	rc = pg_prop_sql_scalar(conn, capped.data, &fc, &err);
	if (fc)
		pg_prop_fail(tc, "capped query failed: %s\n  SQL: %s",
					 err ? err : "", capped.data);
	ru = pg_prop_sql_scalar(conn, uncapped.data, &fu, &err);
	if (fu)
		pg_prop_fail(tc, "uncapped query failed: %s\n  SQL: %s",
					 err ? err : "", uncapped.data);
	rtot = pg_prop_sql_scalar(conn, "SELECT count(*) FROM rc_t", &ft, &err);
	if (ft)
		pg_prop_fail(tc, "row-count query failed: %s", err ? err : "");

	if (rc == NULL || ru == NULL || rtot == NULL)
		pg_prop_fail(tc, "unexpected NULL count (capped=%s uncapped=%s total=%s)",
					 rc ? rc : "NULL", ru ? ru : "NULL", rtot ? rtot : "NULL");

	if (strcmp(rc, ru) != 0)
		pg_prop_fail(tc,
					 "depth cap changed the node set: capped=%s uncapped=%s",
					 rc, ru);
	if (strcmp(rc, rtot) != 0)
		pg_prop_fail(tc,
					 "recursion did not reach every node: reached=%s total=%s",
					 rc, rtot);

	pg_free(rc);
	pg_free(ru);
	pg_free(rtot);
	pfree(capped.data);
	pfree(uncapped.data);
	pg_prop_disconnect(conn);
}
