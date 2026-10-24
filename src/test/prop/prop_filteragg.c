/*-------------------------------------------------------------------------
 *
 * prop_filteragg.c
 *	  Property-based tests for aggregate FILTER clauses and DISTINCT-in-agg.
 *
 * An aggregate with a FILTER (WHERE p) clause must agg exactly the rows a
 * CASE WHEN p THEN v END expression would feed it; the two spellings compile
 * to different executor paths (Agg filter qual vs a CASE in the transfn
 * input), so they are an independent oracle for each other.  DISTINCT inside
 * an aggregate deduplicates its input before the transfn runs; its result
 * must stay consistent with the plain aggregate (count(DISTINCT) <= count())
 * and with a manual sum/count.  A disagreement is a FILTER or DISTINCT-agg
 * executor bug, not a rounding artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_filteragg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "filteragg"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the aggregate errored or produced an unexpected NULL,
 * which is itself a failure, reported with the SQL.
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	}
	if (res == NULL)
	{
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	}
	if (strcmp(res, "t") != 0)
	{
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	}
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * Compare two result sets rendered row-major; a mismatch is a failure.  Both
 * queries must carry a TOTAL ORDER BY so the string compare is a multiset
 * compare (see area_spec.md).
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
 * Build fa_t(id int PRIMARY KEY, g int, v int) with 0..30 rows.  id is the
 * row ordinal, g is a group in 0..3, and v is a small int in [-5, 5] or SQL
 * NULL (~1 in 7, roughly 15%).  Small domains keep collisions, duplicate v's
 * (so DISTINCT does real work), and NULL/empty boundaries all exercised while
 * keeping a counterexample legible.  Rebuilt fresh in each property.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 30);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS fa_t; "
						   "CREATE TABLE fa_t (id int PRIMARY KEY, g int, v int); ");

	for (int r = 0; r < nrows; r++)
	{
		int			g = (int) pg_prop_draw_int(tc, 0, 3);

		if (pg_prop_draw_int(tc, 0, 6) == 0)
			appendStringInfo(&ddl, "INSERT INTO fa_t VALUES (%d, %d, NULL); ",
							 r, g);
		else
			appendStringInfo(&ddl, "INSERT INTO fa_t VALUES (%d, %d, %d); ",
							 r, g, (int) pg_prop_draw_int(tc, -5, 5));
	}

	appendStringInfoString(&ddl, "ANALYZE fa_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/*
 * FILTER equals CASE for sum: sum(v) FILTER (WHERE v>0) must equal
 * sum(CASE WHEN v>0 THEN v END).  Both aggregate exactly the rows where v>0.
 * Over 0 rows (or no positive v) both sides are NULL; IS NOT DISTINCT FROM
 * makes that pass.  Wrapped in scalar subselects so a single row is returned
 * even when fa_t is empty.
 */
PG_PROP(filter_eq_case)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT sum(v) FILTER (WHERE v>0) FROM fa_t) "
				 "IS NOT DISTINCT FROM "
				 "(SELECT sum(CASE WHEN v>0 THEN v END) FROM fa_t)");

	pg_prop_disconnect(conn);
}

/*
 * FILTER equals CASE for count: count(*) FILTER (WHERE v>0) must equal
 * count(CASE WHEN v>0 THEN 1 END).  count(expr) skips NULLs, so the CASE's
 * ELSE NULL arm drops exactly the rows the FILTER drops.  count() is never
 * NULL, so a plain = is sound even over 0 rows (both 0).
 */
PG_PROP(count_filter_eq_count_case)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FILTER (WHERE v>0) FROM fa_t) "
				 "= (SELECT count(CASE WHEN v>0 THEN 1 END) FROM fa_t)");

	pg_prop_disconnect(conn);
}

/*
 * FILTER predicates partition the rows: the three mutually exclusive and
 * exhaustive predicates v>0, v<=0, v IS NULL each count a disjoint subset,
 * and together they must count every row -- so their sum equals count(*).
 * All counts are non-NULL, so a plain = is sound even over 0 rows (0=0).
 */
PG_PROP(filter_partitions)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT "
				 "count(*) FILTER (WHERE v>0) "
				 "+ count(*) FILTER (WHERE v<=0) "
				 "+ count(*) FILTER (WHERE v IS NULL) "
				 "FROM fa_t) = (SELECT count(*) FROM fa_t)");

	pg_prop_disconnect(conn);
}

/*
 * DISTINCT never increases a count: count(DISTINCT v) <= count(v).  (The
 * analogous sum(DISTINCT v) <= sum(v) is FALSE with negatives, so only the
 * count form is asserted.)  Both counts are non-NULL and 0 over an empty
 * table, so 0<=0 holds.
 */
PG_PROP(distinct_count_le_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(DISTINCT v) FROM fa_t) "
				 "<= (SELECT count(v) FROM fa_t)");

	pg_prop_disconnect(conn);
}

/*
 * avg(DISTINCT v) is, by definition, the sum of the distinct values over
 * their count: avg(DISTINCT v) IS NOT DISTINCT FROM
 * sum(DISTINCT v)::numeric / nullif(count(DISTINCT v), 0).  nullif guards the
 * empty / all-NULL case: both sides are then NULL and IS NOT DISTINCT FROM
 * passes.  The exact identity divides the same sum by the same count avg
 * used, so it holds to the last digit.
 */
PG_PROP(avg_distinct_vs_manual)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT avg(DISTINCT v) FROM fa_t) "
				 "IS NOT DISTINCT FROM "
				 "(SELECT sum(DISTINCT v)::numeric "
				 "/ nullif(count(DISTINCT v), 0) FROM fa_t)");

	pg_prop_disconnect(conn);
}

/*
 * Per-group FILTER sum equals a correlated-subquery sum (differential).  The
 * grouped aggregate sum(v) FILTER (WHERE v>0) per g must match, group for
 * group, the sum of the same positive v's computed by a correlated subquery
 * over the distinct groups.  Both carry ORDER BY g (a total order over the
 * distinct groups) and render NULL as "\\N", so an empty group's NULL on each
 * side compares equal.  A disagreement is a grouped-FILTER executor bug.
 */
PG_PROP(filter_grouped)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT g, sum(v) FILTER (WHERE v>0) "
					  "FROM fa_t GROUP BY g ORDER BY g",
					  "SELECT g, (SELECT sum(u.v) FROM fa_t u "
					  "WHERE u.g = t.g AND u.v > 0) "
					  "FROM (SELECT DISTINCT g FROM fa_t) t ORDER BY g");

	pg_prop_disconnect(conn);
}

/*
 * string_agg(DISTINCT ...) emits no duplicate pieces: the number of
 * comma-separated elements it produces equals count(DISTINCT v).  Only
 * asserted when there is at least one distinct value, because an empty
 * string_agg is NULL -> coalesced to '' -> string_to_array('', ',') yields
 * {''} (length 1) which would spuriously fail against count 0.  The CASE
 * short-circuits that boundary to true.  v::text ORDER BY v::text makes the
 * aggregate deterministic.
 */
PG_PROP(string_agg_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT CASE WHEN count(DISTINCT v) = 0 THEN true "
				 "ELSE cardinality(string_to_array("
				 "coalesce(string_agg(DISTINCT v::text, ',' "
				 "ORDER BY v::text), ''), ',')) = count(DISTINCT v) END "
				 "FROM fa_t)");

	pg_prop_disconnect(conn);
}
