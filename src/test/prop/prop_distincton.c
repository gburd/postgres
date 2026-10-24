/*-------------------------------------------------------------------------
 *
 * prop_distincton.c
 *	  Property-based tests for DISTINCT ON row-selection semantics.
 *
 * DISTINCT ON (keys) keeps exactly one row per distinct value of the ON keys,
 * and when the query carries a leading ORDER BY on those keys it keeps the
 * first row in that order -- so ORDER BY key, tiebreak selects, per group, the
 * row with the smallest tiebreak.  This is distinct from plain DISTINCT (whole
 * output row de-dup) and from NULL distinctness: here the differential oracle
 * is against the per-group min()/GROUP BY that DISTINCT ON must coincide with,
 * plus scalar algebra (one row per group, subset of the table, count bounds).
 * Any disagreement is a planner/executor bug in the DISTINCT ON path.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_distincton.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "distincton"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small domains: g in 0..3 so there are at most a handful of groups and
 * collisions are frequent; v in 0..9 so per-group min(v) ties and distinct
 * values both occur.  No NULLs -- DISTINCT ON null handling is covered by the
 * nulldistinct area; here the leading ORDER BY on g (and the tiebreak) must be
 * a total order for the "picks first" oracle to be deterministic, so keeping g
 * and v non-null keeps every ORDER BY total without NULLS FIRST/LAST noise.
 */
#define G_MIN 0
#define G_MAX 3
#define V_MIN 0
#define V_MAX 9

/*
 * (Re)build do_t(id int PRIMARY KEY, g int, v int) with 0..30 random rows.
 * id is 0..nrows-1 so it is a total key (used as the final tiebreak and for
 * the two-key min(id) oracle); g and v are small non-null ints.  Fresh in
 * every property so cases never see each other's rows.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS do_t; "
						   "CREATE TABLE do_t (id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int r = 0; r < nrows; r++)
	{
		int			g = (int) pg_prop_draw_int(tc, G_MIN, G_MAX);
		int			v = (int) pg_prop_draw_int(tc, V_MIN, V_MAX);

		appendStringInfo(&ddl, "INSERT INTO do_t VALUES (%d, %d, %d); ",
						 r, g, v);
	}
	appendStringInfoString(&ddl, "ANALYZE do_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build do_t failed: %s\n  ddl: %s",
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
 * Scalar boolean oracle: run "SELECT <bool>" and require it returns "t".
 * A NULL result or a SQL error is a failure.
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
		pg_prop_fail(tc, "expected t, got %s for: %s", res, sql);
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * (A) DISTINCT ON (g) picks exactly one row per distinct g: the number of
 * rows kept equals the number of distinct g values.  The ORDER BY g makes
 * DISTINCT ON well-defined.
 */
PG_PROP(distinct_on_one_per_group)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT DISTINCT ON (g) g FROM do_t ORDER BY g) s) "
				 "= (SELECT count(DISTINCT g) FROM do_t)");

	pg_prop_disconnect(conn);
}

/*
 * (B) DISTINCT ON (g) with ORDER BY g, v picks the first row in that order
 * per group -- the row with the smallest v -- so (g, v) must equal the
 * per-group GROUP BY g / min(v).  Both sides carry a total ORDER BY g.
 */
PG_PROP(distinct_on_picks_first)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT DISTINCT ON (g) g, v FROM do_t ORDER BY g, v",
					  "SELECT g, min(v) FROM do_t GROUP BY g ORDER BY g");

	pg_prop_disconnect(conn);
}

/*
 * (A) Every (g, v) pair returned by DISTINCT ON (g) exists in the table: the
 * result selects real rows, never fabricated ones.  bool_and over the kept
 * rows; coalesce(..., true) so the empty-table case (zero kept rows ->
 * bool_and is NULL) passes soundly.
 */
PG_PROP(distinct_on_subset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and("
				 "EXISTS(SELECT 1 FROM do_t t WHERE t.g = s.g AND t.v = s.v))"
				 ", true) FROM "
				 "(SELECT DISTINCT ON (g) g, v FROM do_t ORDER BY g, v) s");

	pg_prop_disconnect(conn);
}

/*
 * (B) DISTINCT ON (g, v) with ORDER BY g, v, id keeps one row per (g, v) pair,
 * the one with the smallest id, so (g, v, id) must equal GROUP BY g, v /
 * min(id).  id is a PRIMARY KEY so g, v, id is a total order.
 */
PG_PROP(distinct_on_two_keys)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT DISTINCT ON (g, v) g, v, id FROM do_t "
					  "ORDER BY g, v, id",
					  "SELECT g, v, min(id) FROM do_t GROUP BY g, v "
					  "ORDER BY g, v");

	pg_prop_disconnect(conn);
}

/*
 * (A) The DISTINCT ON result can only shrink the table: the number of rows
 * kept is at most the number of rows in the table.  ORDER BY g, id is total
 * so the choice is deterministic (immaterial to the count, but keeps the
 * query well-defined).
 */
PG_PROP(distinct_on_count_le)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT DISTINCT ON (g) * FROM do_t ORDER BY g, id) s) "
				 "<= (SELECT count(*) FROM do_t)");

	pg_prop_disconnect(conn);
}

/*
 * (A) DISTINCT ON (g) keeps the same number of rows as SELECT DISTINCT g: both
 * collapse to one row per distinct g value, so their row counts must match.
 */
PG_PROP(distinct_on_vs_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT DISTINCT ON (g) g FROM do_t ORDER BY g) s) "
				 "= (SELECT count(*) FROM "
				 "(SELECT DISTINCT g FROM do_t) t)");

	pg_prop_disconnect(conn);
}
