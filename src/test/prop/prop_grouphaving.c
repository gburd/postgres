/*-------------------------------------------------------------------------
 *
 * prop_grouphaving.c
 *	  Property-based tests for GROUP BY / HAVING grouped-aggregate rewrites.
 *
 * The oracle is differential: each property runs two queries that must return
 * the same rows, where one is the plain grouped form and the other an
 * equivalent rewrite (HAVING vs an outer WHERE on a grouped subquery, a
 * grouping expression vs a subquery that pre-computes it, per-group aggregate
 * vs a correlated-subquery form, HAVING true vs no HAVING, DISTINCT vs
 * GROUP BY with no aggregate).  A grouped answer must not depend on which
 * equivalent form the planner is handed, so any disagreement is a bug in
 * grouping, HAVING placement, or aggregate pushdown.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_grouphaving.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "grouphaving"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small domains keep each generated table legible in a counterexample and
 * keep cases fast while still covering empty/one-group/many-group shapes, a
 * low-cardinality grouping key (g in 0..4), signed values spanning zero, and
 * ~15% NULLs so NULL-grouping and NULL-aggregate behaviour is exercised.
 */
#define G_MIN 0
#define G_MAX 4
#define V_MIN (-5)
#define V_MAX 5

/* Draw v: SQL NULL ~15% of the time, else an int in [V_MIN, V_MAX]. */
static void
draw_v(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, V_MIN, V_MAX));
}

/*
 * Build gh_t(id int PRIMARY KEY, g int, v int) with 0..30 random rows.  g is
 * never NULL (0..4); v is NULL ~15% of the time.  Rebuilt fresh in each
 * property so cases are independent.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS gh_t; "
						   "CREATE TABLE gh_t (id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int r = 0; r < nrows; r++)
	{
		StringInfoData v;

		initStringInfo(&v);
		draw_v(tc, &v);
		appendStringInfo(&ddl, "INSERT INTO gh_t VALUES (%d, %d, %s); ",
						 r, (int) pg_prop_draw_int(tc, G_MIN, G_MAX), v.data);
		pfree(v.data);
	}
	appendStringInfoString(&ddl, "ANALYZE gh_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * Differential result-set oracle: q1 and q2 must return identical rows in
 * identical order.  Both callers supply a TOTAL ORDER BY so the row-major
 * string compare behaves as a multiset compare.
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
 * Scalar boolean oracle (shape A): a single SELECT must return "t".  A NULL
 * result or SQL error is a failure.  Used for the empty-input-sensitive
 * property where the two row-shapes legitimately differ on an empty table.
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
 * HAVING on a grouped result equals an outer WHERE applied to the same
 * grouping wrapped in a subquery.  The planner may push the predicate down or
 * leave it on the Agg node; both must select the same groups.
 */
PG_PROP(having_eq_outer_where)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT g, count(*) c FROM gh_t GROUP BY g "
					  "HAVING count(*) > 1 ORDER BY g",
					  "SELECT g, c FROM (SELECT g, count(*) c FROM gh_t "
					  "GROUP BY g) s WHERE s.c > 1 ORDER BY g");

	pg_prop_disconnect(conn);
}

/*
 * Grouping by an expression equals grouping by that expression pre-computed in
 * a subquery.  (v>0) is NULL when v is NULL, so NULLS FIRST pins the NULL
 * group deterministically in the total order.
 */
PG_PROP(group_by_expr_eq_subquery)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT (v>0) k, count(*) FROM gh_t GROUP BY (v>0) "
					  "ORDER BY k NULLS FIRST",
					  "SELECT k, count(*) FROM (SELECT (v>0) k FROM gh_t) s "
					  "GROUP BY k ORDER BY k NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * A per-group sum equals the same sum computed by a correlated subquery over
 * the distinct grouping keys.  IS NOT DISTINCT FROM makes the correlation
 * match the GROUP BY semantics for a NULL key (though g is non-NULL here, the
 * form stays sound if that ever changes).  Both sides return NULL for an
 * all-NULL-v group, so no coalesce is needed as long as both agree.
 */
PG_PROP(group_sum_eq_join_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT g, sum(v) FROM gh_t GROUP BY g "
					  "ORDER BY g NULLS FIRST",
					  "SELECT g, (SELECT sum(u.v) FROM gh_t u "
					  "WHERE u.g IS NOT DISTINCT FROM t.g) "
					  "FROM (SELECT DISTINCT g FROM gh_t) t "
					  "ORDER BY g NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * HAVING true is equivalent to no HAVING at all: a constant-true qual filters
 * nothing, so every group survives either way.
 */
PG_PROP(having_true_eq_no_having)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT g, count(*) FROM gh_t GROUP BY g HAVING true "
					  "ORDER BY g NULLS FIRST",
					  "SELECT g, count(*) FROM gh_t GROUP BY g "
					  "ORDER BY g NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * The empty grouping set GROUP BY () collapses the whole relation to exactly
 * one group, and -- unlike a column GROUP BY -- it emits that one row even for
 * an empty table.  So the number of groups is always exactly 1, on every
 * input.  (A bare constant such as GROUP BY (true) is rejected by the parser,
 * so the empty grouping set is the right way to express "one group".)
 */
PG_PROP(group_const_single_group)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT count(*) FROM gh_t GROUP BY ()) s) = 1");

	pg_prop_disconnect(conn);
}

/*
 * SELECT DISTINCT g equals GROUP BY g with no aggregate: both reduce to the
 * set of distinct grouping keys.  The planner may implement DISTINCT via a
 * grouping path, so the two must agree exactly.
 */
PG_PROP(distinct_eq_group_no_agg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT DISTINCT g FROM gh_t ORDER BY g NULLS FIRST",
					  "SELECT g FROM gh_t GROUP BY g ORDER BY g NULLS FIRST");

	pg_prop_disconnect(conn);
}
