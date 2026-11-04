/*-------------------------------------------------------------------------
 *
 * prop_countsem.c
 *	  Property-based tests for COUNT semantics: count(*) vs count(col) vs
 *	  count(DISTINCT col) and their NULL handling.
 *
 * The oracle is the defined meaning of COUNT: count(*) counts rows,
 * count(col) counts non-NULL values of col, and count(DISTINCT col) counts
 * distinct non-NULL values -- so count(DISTINCT col) <= count(col) <=
 * count(*), the rows count(col) drops are exactly the NULL-col rows (count(*)
 * - count(col) = count(*) FILTER (WHERE col IS NULL)), count(DISTINCT col)
 * excludes NULL, and count of any non-NULL constant equals count(*).  A "f"
 * result is a wrong-results bug in COUNT, DISTINCT de-duplication, or the
 * aggregate's NULL skipping -- not a rounding artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_countsem.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "countsem"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means COUNT errored or produced NULL where it must not, which is
 * itself a bug -- so either is a failure, reported with the SQL.  count() is
 * never NULL (0 over empty input), so no coalesce is needed anywhere here.
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
 * Build cs2_t(id int PRIMARY KEY, a int, b int) with 0..30 rows: id is 0..n-1,
 * a and b each drawn in 0..4 with ~20% NULL independently.  The small 0..4
 * domain over up to 30 rows guarantees frequent duplicates so count(DISTINCT)
 * is a meaningful de-dup, and ~20% NULL in each column exercises both the
 * NULL-skipping of count(col) and the NULL exclusion of count(DISTINCT).
 * Fresh each property so a counterexample is self-contained; ANALYZE so plan
 * choice (hash vs sorted vs partial agg) is free to vary and the oracle holds.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS cs2_t; "
						   "CREATE TABLE cs2_t (id int PRIMARY KEY, a int, b int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int i = 0; i < nrows; i++)
	{
		appendStringInfo(&ddl, "INSERT INTO cs2_t VALUES (%d, ", i);
		if (pg_prop_draw_int(tc, 0, 4) == 0)
			appendStringInfoString(&ddl, "NULL, ");
		else
			appendStringInfo(&ddl, "%d, ", (int) pg_prop_draw_int(tc, 0, 4));
		if (pg_prop_draw_int(tc, 0, 4) == 0)
			appendStringInfoString(&ddl, "NULL); ");
		else
			appendStringInfo(&ddl, "%d); ", (int) pg_prop_draw_int(tc, 0, 4));
	}
	appendStringInfoString(&ddl, "ANALYZE cs2_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * count(*) equals the row count computed a second way (count(*) over a
 * one-column projection of the same rows).  A tautology that pins the simple
 * count(*) path: both must report the same number of rows.
 */
PG_PROP(count_star_eq_rows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM cs2_t) "
				 "= (SELECT count(*) FROM (SELECT 1 FROM cs2_t) s)");

	pg_prop_disconnect(conn);
}

/*
 * count(a) <= count(*).  count(a) counts only non-NULL a, which can never
 * exceed the total row count.
 */
PG_PROP(count_col_le_star)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(a) <= count(*) FROM cs2_t");

	pg_prop_disconnect(conn);
}

/*
 * count(DISTINCT a) <= count(a).  De-duplicating the non-NULL values can only
 * reduce (or keep) the count, never increase it.
 */
PG_PROP(count_distinct_le_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(DISTINCT a) <= count(a) FROM cs2_t");

	pg_prop_disconnect(conn);
}

/*
 * count(*) - count(a) = count(*) FILTER (WHERE a IS NULL).  The rows count(a)
 * drops are exactly the NULL-a rows, so the deficit equals the NULL count.
 */
PG_PROP(count_null_complement)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(*) - count(a) "
				 "= count(*) FILTER (WHERE a IS NULL) FROM cs2_t");

	pg_prop_disconnect(conn);
}

/*
 * count(DISTINCT (a,b)) <= count(*).  A row-value count(DISTINCT) over the
 * pair counts distinct (a,b) tuples (NULL tuples are skipped only when both
 * are NULL -- a row-value is NULL iff every element is NULL), which can never
 * exceed the row count.
 */
PG_PROP(count_distinct_pair_le)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(DISTINCT (a,b)) <= count(*) FROM cs2_t");

	pg_prop_disconnect(conn);
}

/*
 * count(a) and count(b) are each <= count(*), and count(a) partitions
 * count(*) with the NULL-a rows: count(a) + count(*) FILTER (WHERE a IS NULL)
 * = count(*).  Two per-column non-NULL counts bounded by the row count, and
 * the exact non-NULL/NULL split for a.
 */
PG_PROP(count_two_cols)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(a) <= count(*) "
				 "AND count(b) <= count(*) "
				 "AND count(a) + count(*) FILTER (WHERE a IS NULL) = count(*) "
				 "FROM cs2_t");

	pg_prop_disconnect(conn);
}

/*
 * count(DISTINCT a) excludes NULL: it equals the number of distinct non-NULL
 * a values computed independently via SELECT DISTINCT over the non-NULL rows.
 * A differential check that DISTINCT in the aggregate drops NULL exactly as a
 * SELECT DISTINCT ... WHERE a IS NOT NULL would.
 */
PG_PROP(count_distinct_nulls_excluded)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(DISTINCT a) FROM cs2_t) "
				 "= (SELECT count(*) FROM "
				 "(SELECT DISTINCT a FROM cs2_t WHERE a IS NOT NULL) s)");

	pg_prop_disconnect(conn);
}

/*
 * count(*) = count(1) = count(42): count of a non-NULL constant equals the
 * row count, since the constant is non-NULL in every row and so is never
 * skipped.  Pins that count(const) behaves as count(*), not as count of a
 * nullable column.
 */
PG_PROP(count_star_const)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(*) = count(1) AND count(1) = count(42) "
				 "FROM cs2_t");

	pg_prop_disconnect(conn);
}
