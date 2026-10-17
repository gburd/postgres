/*-------------------------------------------------------------------------
 *
 * prop_orderbynulls.c
 *	  Property-based tests for ORDER BY NULLS FIRST/LAST ordering.
 *
 * The oracle is the SQL standard's rule that ASC orders NULLS LAST and DESC
 * orders NULLS FIRST by default, that NULLS placement and multi-key ordering
 * are independent of the plan (forced sort vs incremental-sort/index), and
 * that an ordered scan's first row agrees with min()/LIMIT 1.  A mismatch
 * means a sort or index-ordering path placed NULLs or ordered keys wrongly,
 * which is a wrong-results bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_orderbynulls.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "orderbynulls"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small integer domain for the sort keys, with ~20% NULL, so NULLS-placement
 * boundaries (all NULL, no NULL, ties on a) are hit often while the table
 * stays legible in a counterexample.
 */
#define VAL_MIN (-4)
#define VAL_MAX 4

/* Draw an integer in [VAL_MIN, VAL_MAX], or SQL NULL (~20% of the time). */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 4) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/*
 * Build ob_t(id int PRIMARY KEY, a int, b int) with 0..30 rows, a and b drawn
 * from the small NULL-bearing domain, an index on (a), and fresh statistics.
 * id is a dense 0..n-1 sequence so it is a guaranteed unique tiebreak for a
 * total order.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 30);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ob_t; "
						   "CREATE TABLE ob_t (id int PRIMARY KEY, a int, b int); ");

	for (int r = 0; r < nrows; r++)
	{
		StringInfoData a,
					b;

		initStringInfo(&a);
		initStringInfo(&b);
		draw_val(tc, &a);
		draw_val(tc, &b);
		appendStringInfo(&ddl, "INSERT INTO ob_t VALUES (%d, %s, %s); ",
						 r, a.data, b.data);
		pfree(a.data);
		pfree(b.data);
	}

	appendStringInfoString(&ddl,
						   "CREATE INDEX ob_t_a_idx ON ob_t (a); "
						   "ANALYZE ob_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  SQL: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);
}

/*
 * Differential result-set oracle: the two queries, each carrying a total
 * ORDER BY, must return identical rows in identical order.
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
 * a SQL error means the ordering oracle errored or produced NULL, which is
 * itself a failure.
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
 * Property 1: ASC default places NULLs last.  "ORDER BY a, id" must return
 * exactly the same rows in the same order as the explicit "ORDER BY a NULLS
 * LAST, id".  The id tiebreak makes both a total order.
 */
PG_PROP(nulls_last_default_asc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM ob_t ORDER BY a, id",
					  "SELECT id FROM ob_t ORDER BY a NULLS LAST, id");

	pg_prop_disconnect(conn);
}

/*
 * Property 2: DESC default places NULLs first.  "ORDER BY a DESC, id" must
 * equal the explicit "ORDER BY a DESC NULLS FIRST, id".
 */
PG_PROP(nulls_first_default_desc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM ob_t ORDER BY a DESC, id",
					  "SELECT id FROM ob_t ORDER BY a DESC NULLS FIRST, id");

	pg_prop_disconnect(conn);
}

/*
 * Property 3: the ordered answer must not depend on the plan.  The same
 * "ORDER BY a NULLS LAST, id" must return identical rows whether forced into a
 * seqscan + sort or into an index scan.  SET the planner GUCs around each run
 * and RESET ALL afterward so no state leaks to the next case.
 */
PG_PROP(sort_vs_index_scan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q = "SELECT id FROM ob_t ORDER BY a NULLS LAST, id";
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *r_sort,
			   *r_index;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	/* Forced seqscan + sort. */
	pg_prop_sql_exec(conn, "SET enable_indexscan = off; "
					 "SET enable_indexonlyscan = off; "
					 "SET enable_bitmapscan = off; "
					 "SET enable_seqscan = on", &err);
	r_sort = pg_prop_sql_rows(conn, q, &f1, &e1);

	/* Forced index scan. */
	pg_prop_sql_exec(conn, "SET enable_indexscan = on; "
					 "SET enable_indexonlyscan = on; "
					 "SET enable_seqscan = off; "
					 "SET enable_sort = off", &err);
	r_index = pg_prop_sql_rows(conn, q, &f2, &e2);

	pg_prop_sql_exec(conn, "RESET ALL", &err);

	if (f1)
		pg_prop_fail(tc, "forced-sort query failed: %s\n  SQL: %s",
					 e1 ? e1 : "", q);
	if (f2)
		pg_prop_fail(tc, "forced-index query failed: %s\n  SQL: %s",
					 e2 ? e2 : "", q);
	if (strcmp(r_sort, r_index) != 0)
		pg_prop_fail(tc, "plan changed ordered result:\n  sort:\n%s\n  index:\n%s",
					 r_sort, r_index);

	pg_free(r_sort);
	pg_free(r_index);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
	pg_prop_disconnect(conn);
}

/*
 * Property 4: multi-key NULLS ordering is stable and plan-independent.  The
 * three-key total order "a NULLS FIRST, b NULLS FIRST, id" must equal the same
 * ORDER BY applied over a subquery wrapper (which blocks a direct index path
 * and typically forces a sort), proving the two orderings agree.
 */
PG_PROP(multikey_stable)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM ob_t "
					  "ORDER BY a NULLS FIRST, b NULLS FIRST, id",
					  "SELECT id FROM (SELECT id, a, b FROM ob_t) s "
					  "ORDER BY a NULLS FIRST, b NULLS FIRST, id");

	pg_prop_disconnect(conn);
}

/*
 * Property 5: when any non-null a exists, the first row of "ORDER BY a NULLS
 * LAST, id" has a = min(a) over the non-null values.  Guarded with CASE on the
 * count of non-null a so an all-null or empty table passes soundly (true).
 */
PG_PROP(reverse_all_keys)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT count(a) FROM ob_t) = 0 THEN true "
				 "ELSE (SELECT a FROM ob_t ORDER BY a NULLS LAST, id LIMIT 1) "
				 "= (SELECT min(a) FROM ob_t) END");

	pg_prop_disconnect(conn);
}

/*
 * Property 6: LIMIT 1 of "ORDER BY a NULLS LAST, id" returns min(a) when a
 * non-null a exists.  IS NOT DISTINCT FROM compares safely; CASE on the
 * non-null count guards the empty / all-null table so it passes soundly.
 */
PG_PROP(limit1_is_min)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT count(a) FROM ob_t) = 0 THEN true "
				 "ELSE (SELECT a FROM ob_t ORDER BY a NULLS LAST, id LIMIT 1) "
				 "IS NOT DISTINCT FROM (SELECT min(a) FROM ob_t) END");

	pg_prop_disconnect(conn);
}
