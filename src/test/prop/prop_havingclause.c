/*-------------------------------------------------------------------------
 *
 * prop_havingclause.c
 *	  Property-based tests for HAVING-clause filtering semantics.
 *
 * The oracle is differential: HAVING filters whole groups by an aggregate
 * predicate, and that filtering must equal applying the same predicate as an
 * outer WHERE over a grouped subquery -- regardless of whether the planner
 * keeps the qual on the Agg node or pushes it down.  Two scalar properties
 * pin the group-count boundaries HAVING must honour: HAVING false yields no
 * groups, and HAVING on an aggregate with no GROUP BY always yields exactly
 * one group (the whole table).  Any disagreement is a bug in HAVING placement
 * or in the "no GROUP BY means one group" rule.  This area is distinct from
 * grouphaving (general GROUP BY/HAVING rewrites) and filteragg (FILTER on
 * aggregates): it isolates HAVING's group-filtering contract.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_havingclause.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "havingclause"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small domains keep a counterexample legible and cases fast while covering
 * empty/one-group/many-group shapes: a low-cardinality grouping key (g in
 * 0..4) and signed values spanning zero (v in -5..5).  v is non-NULL here so
 * sum(v) > 0 and count(*) comparisons have clean boundaries; NULL-aggregate
 * behaviour is the grouphaving area's job.
 */
#define G_MIN 0
#define G_MAX 4
#define V_MIN (-5)
#define V_MAX 5

/*
 * Build hc_t(id int PRIMARY KEY, g int, v int) with 0..30 random rows.  g in
 * 0..4, v in -5..5, both non-NULL.  Rebuilt fresh in each property so cases
 * are independent.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS hc_t; "
						   "CREATE TABLE hc_t (id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int r = 0; r < nrows; r++)
		appendStringInfo(&ddl, "INSERT INTO hc_t VALUES (%d, %d, (%d)); ",
						 r,
						 (int) pg_prop_draw_int(tc, G_MIN, G_MAX),
						 (int) pg_prop_draw_int(tc, V_MIN, V_MAX));
	appendStringInfoString(&ddl, "ANALYZE hc_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * Differential result-set oracle (shape B): q1 and q2 must return identical
 * rows in identical order.  Both callers supply a TOTAL ORDER BY (g is the
 * sole output column and is distinct per group, so ORDER BY g is total) so
 * the row-major string compare behaves as a multiset compare.
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
 * result or SQL error is a failure.  Used for the group-count boundary
 * properties where a differential row shape is awkward (empty result vs
 * single-group result).
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
 * HAVING sum(v) > 0 keeps exactly the groups whose grouped sum exceeds 0, the
 * same groups an outer WHERE on a grouped subquery keeps.  The predicate may
 * ride the Agg node or be pushed below; both must select the same groups.
 */
PG_PROP(having_filters_groups)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT g FROM hc_t GROUP BY g HAVING sum(v) > 0 "
					  "ORDER BY g",
					  "SELECT g FROM (SELECT g, sum(v) s FROM hc_t GROUP BY g) t "
					  "WHERE s > 0 ORDER BY g");

	pg_prop_disconnect(conn);
}

/*
 * HAVING count(*) >= 2 keeps groups with at least two rows, matching an outer
 * WHERE on the per-group count.
 */
PG_PROP(having_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT g FROM hc_t GROUP BY g HAVING count(*) >= 2 "
					  "ORDER BY g",
					  "SELECT g FROM (SELECT g, count(*) c FROM hc_t GROUP BY g) t "
					  "WHERE c >= 2 ORDER BY g");

	pg_prop_disconnect(conn);
}

/*
 * WHERE and HAVING compose: WHERE filters rows before grouping, HAVING filters
 * groups after.  The grouped-and-HAVING form must equal pushing the same
 * row filter and group filter through a grouped subquery.
 */
PG_PROP(having_and_where)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT g FROM hc_t WHERE v > -3 GROUP BY g "
					  "HAVING count(*) > 1 ORDER BY g",
					  "SELECT g FROM (SELECT g, count(*) c FROM hc_t "
					  "WHERE v > -3 GROUP BY g) t WHERE c > 1 ORDER BY g");

	pg_prop_disconnect(conn);
}

/*
 * HAVING false filters out every group: a constant-false qual survives no
 * group, so the result is always empty, for every input.
 */
PG_PROP(having_false_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT g FROM hc_t GROUP BY g HAVING false) s) = 0");

	pg_prop_disconnect(conn);
}

/*
 * HAVING true filters nothing: a constant-true qual keeps every group, so the
 * grouped result equals the same GROUP BY with no HAVING at all.
 */
PG_PROP(having_true_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT g FROM hc_t GROUP BY g HAVING true ORDER BY g",
					  "SELECT g FROM hc_t GROUP BY g ORDER BY g");

	pg_prop_disconnect(conn);
}

/*
 * HAVING with no GROUP BY groups the whole table into a single group, and that
 * one group row is emitted iff the group passes HAVING -- but the group itself
 * always exists.  With count(*) >= 0 (always true), the result is exactly one
 * group row on every input, including an empty table.  This pins the "no
 * GROUP BY means one implicit group" rule that HAVING filters over.
 */
PG_PROP(having_without_groupby)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT 1 FROM hc_t HAVING count(*) >= 0) s) = 1");

	pg_prop_disconnect(conn);
}
