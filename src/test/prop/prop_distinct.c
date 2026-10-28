/*-------------------------------------------------------------------------
 *
 * prop_distinct.c
 *	  Property-based tests for DISTINCT / DISTINCT ON / GROUP BY equivalences.
 *
 * DISTINCT, DISTINCT ON and GROUP BY are three spellings of the same
 * duplicate-elimination semantics, and the executor computes each by either a
 * hash aggregate or a sort+unique.  Those must all agree: a row that survives
 * one spelling or one strategy must survive the others, with NULL treated as a
 * single distinct value throughout.  Any disagreement is a planner/executor
 * bug.  Oracles are differential (two spellings must return identical rows) or
 * differential across enable_hashagg (hash distinct vs sort distinct).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_distinct.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "distinct"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small integer domains keep each case fast and keep collisions (so DISTINCT
 * actually removes something) frequent while still exercising NULL as a
 * distinct value.
 */
#define VAL_MIN 0
#define VAL_MAX 4

/* Draw an integer in [VAL_MIN, VAL_MAX], or SQL NULL (~15% of the time). */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/*
 * (Re)build di_t(id int PRIMARY KEY, a int, b int) with 0..30 random rows.
 * id is 0..nrows-1 so it is a total key; a and b are small ints or NULL.
 * Fresh in every property so cases never see each other's rows.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS di_t; "
						   "CREATE TABLE di_t (id int PRIMARY KEY, a int, b int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int r = 0; r < nrows; r++)
	{
		StringInfoData a,
					b;

		initStringInfo(&a);
		initStringInfo(&b);
		draw_val(tc, &a);
		draw_val(tc, &b);
		appendStringInfo(&ddl, "INSERT INTO di_t VALUES (%d, %s, %s); ",
						 r, a.data, b.data);
		pfree(a.data);
		pfree(b.data);
	}
	appendStringInfoString(&ddl, "ANALYZE di_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build di_t failed: %s\n  ddl: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);
}

/*
 * Differential result-set oracle: q1 and q2 must return identical rows in
 * identical order.  Each query MUST carry a total ORDER BY so the row-major
 * string compares as an ordered multiset.
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
 * Run one DISTINCT query two ways -- hash distinct (enable_hashagg = on) and
 * sort distinct (enable_hashagg = off) -- and require identical rows.  Both
 * strategies are optimisations that must never change results.  RESET ALL
 * after so a failed/longjmp'd case does not leak the GUC into the next.
 */
static void
run_two_distinct(pg_prop_tc *tc, pg_prop_conn *conn, const char *query)
{
	bool		fh = false,
				fs = false;
	char	   *eh = NULL,
			   *es = NULL,
			   *rh,
			   *rs,
			   *err = NULL;

	pg_prop_sql_exec(conn, "SET enable_hashagg = on", &err);
	rh = pg_prop_sql_rows(conn, query, &fh, &eh);
	pg_prop_sql_exec(conn, "SET enable_hashagg = off", &err);
	rs = pg_prop_sql_rows(conn, query, &fs, &es);
	pg_prop_sql_exec(conn, "RESET ALL", &err);

	if (fh)
		pg_prop_fail(tc, "hash distinct failed: %s\n  SQL: %s",
					 eh ? eh : "", query);
	if (fs)
		pg_prop_fail(tc, "sort distinct failed: %s\n  SQL: %s",
					 es ? es : "", query);
	if (strcmp(rh, rs) != 0)
		pg_prop_fail(tc,
					 "hash vs sort distinct mismatch:\n  SQL: %s\n"
					 "  hash:\n%s\n  sort:\n%s",
					 query, rh, rs);
	pg_free(rh);
	pg_free(rs);
	if (eh)
		pg_free(eh);
	if (es)
		pg_free(es);
	if (err)
		pg_free(err);
}

/*
 * SELECT DISTINCT a == SELECT a GROUP BY a.  DISTINCT on one column and
 * GROUP BY on that column must produce the same set of values, NULL included.
 */
PG_PROP(distinct_eq_groupby)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT DISTINCT a FROM di_t ORDER BY a NULLS FIRST",
					  "SELECT a FROM di_t GROUP BY a ORDER BY a NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * Multi-column SELECT DISTINCT a, b computed as a hash aggregate and as a
 * sort+unique must agree.
 */
PG_PROP(distinct_hash_vs_sort)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	run_two_distinct(tc, conn,
					 "SELECT DISTINCT a, b FROM di_t "
					 "ORDER BY a NULLS FIRST, b NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * DISTINCT ON (a): exactly one row per distinct a, and the ORDER BY starting
 * with a (then b) makes it the row with the smallest b for each a -- a
 * deterministic choice.  Hash vs sort must agree on that choice.
 */
PG_PROP(distinct_on_first)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	run_two_distinct(tc, conn,
					 "SELECT DISTINCT ON (a) a, b FROM di_t "
					 "ORDER BY a NULLS FIRST, b NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * The number of DISTINCT values of a equals the number of GROUP BY a groups.
 * NULL counts as one distinct value in both, so the counts must be identical.
 * Shape A: a scalar boolean oracle requiring "t".
 */
PG_PROP(distinct_count_eq_group_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	bool		failed = false;
	char	   *res;
	const char *sql =
		"SELECT (SELECT count(*) FROM (SELECT DISTINCT a FROM di_t) s) "
		"= (SELECT count(*) FROM (SELECT a FROM di_t GROUP BY a) t)";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "distinct count != group count (got %s) for: %s",
					 res, sql);
	pg_free(res);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/*
 * UNION (which de-duplicates) equals DISTINCT over the corresponding
 * UNION ALL.  Columns are aliased to x so both sides order by the same name.
 */
PG_PROP(union_distinct_eq_distinct_union_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT * FROM "
					  "(SELECT a AS x FROM di_t UNION SELECT b FROM di_t) s "
					  "ORDER BY x NULLS FIRST",
					  "SELECT DISTINCT x FROM "
					  "(SELECT a AS x FROM di_t UNION ALL SELECT b FROM di_t) s "
					  "ORDER BY x NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * DISTINCT is idempotent: applying it twice yields the same rows as once.
 */
PG_PROP(distinct_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT DISTINCT a FROM (SELECT DISTINCT a FROM di_t) s "
					  "ORDER BY a NULLS FIRST",
					  "SELECT DISTINCT a FROM di_t ORDER BY a NULLS FIRST");

	pg_prop_disconnect(conn);
}
