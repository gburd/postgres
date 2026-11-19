/*-------------------------------------------------------------------------
 *
 * prop_nulldistinct.c
 *	  Property-based tests for NULL distinctness in DISTINCT / GROUP BY /
 *	  UNIQUE NULLS [NOT] DISTINCT.
 *
 * SQL treats NULL as a single value for duplicate elimination (DISTINCT,
 * GROUP BY) but as distinct for aggregation (count(col) skips NULL) and, by
 * default, for UNIQUE constraints (NULLS DISTINCT, so many NULLs coexist).
 * PostgreSQL 15 added UNIQUE NULLS NOT DISTINCT, which collapses NULLs so at
 * most one NULL row is allowed.  These properties pin that algebra: all NULLs
 * form exactly one DISTINCT/GROUP group, count(col) excludes them, a default
 * UNIQUE admits many NULLs, and NULLS NOT DISTINCT rejects the second.  A
 * mismatch is a bug in grouping, aggregation, or index null handling.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_nulldistinct.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "nulldistinct"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small domain for a: 0..3 so collisions are frequent, with ~30% NULL to
 * exercise the single-null-group semantics hard.
 */
#define VAL_MIN 0
#define VAL_MAX 3

/* Draw a value for column a: SQL NULL ~30% of the time, else 0..3. */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 9) < 3)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/*
 * (Re)build nd_t(id int PRIMARY KEY, a int) with 0..25 random rows.  id is
 * 0..nrows-1 so it is a total key; a is 0..3 or NULL (~30% NULL).  Fresh in
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
						   "DROP TABLE IF EXISTS nd_t; "
						   "CREATE TABLE nd_t (id int PRIMARY KEY, a int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int r = 0; r < nrows; r++)
	{
		StringInfoData a;

		initStringInfo(&a);
		draw_val(tc, &a);
		appendStringInfo(&ddl, "INSERT INTO nd_t VALUES (%d, %s); ", r, a.data);
		pfree(a.data);
	}
	appendStringInfoString(&ddl, "ANALYZE nd_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build nd_t failed: %s\n  ddl: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);
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
 * Property 1 (shape A): SELECT DISTINCT a treats all NULLs as one value.  The
 * cardinality of SELECT DISTINCT a equals count(DISTINCT a) (which excludes
 * NULL) plus one if any NULL row exists.
 */
PG_PROP(distinct_nulls_one_group)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *sql =
		"SELECT (SELECT count(*) FROM (SELECT DISTINCT a FROM nd_t) s) "
		"= (SELECT count(DISTINCT a) FROM nd_t) "
		"+ (CASE WHEN EXISTS(SELECT 1 FROM nd_t WHERE a IS NULL) "
		"THEN 1 ELSE 0 END)";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_true(tc, conn, sql);
	pg_prop_disconnect(conn);
}

/*
 * Property 2 (shape A): GROUP BY a puts all NULLs in one group, so the group
 * count equals the SELECT DISTINCT a cardinality (property 1's LHS).
 */
PG_PROP(groupby_null_one_group)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *sql =
		"SELECT (SELECT count(*) FROM (SELECT a FROM nd_t GROUP BY a) s) "
		"= (SELECT count(*) FROM (SELECT DISTINCT a FROM nd_t) t)";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_true(tc, conn, sql);
	pg_prop_disconnect(conn);
}

/*
 * Property 3 (shape A): count(a) ignores NULLs, so it equals count(*) minus
 * the number of NULL a rows.
 */
PG_PROP(count_col_excludes_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *sql =
		"SELECT (SELECT count(a) FROM nd_t) "
		"= (SELECT count(*) FROM nd_t) "
		"- (SELECT count(*) FROM nd_t WHERE a IS NULL)";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_true(tc, conn, sql);
	pg_prop_disconnect(conn);
}

/*
 * Property 4: a default UNIQUE(a) constraint uses NULLS DISTINCT, so multiple
 * NULL rows are allowed.  Insert three NULL rows and verify all three persist.
 * Scratch table nd_u is built and dropped within the property.
 */
PG_PROP(unique_nulls_distinct_allows_dups)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *build =
		"DROP TABLE IF EXISTS nd_u; "
		"CREATE TABLE nd_u (a int UNIQUE); "
		"INSERT INTO nd_u VALUES (NULL); "
		"INSERT INTO nd_u VALUES (NULL); "
		"INSERT INTO nd_u VALUES (NULL); ";
	const char *sql = "SELECT (SELECT count(*) FROM nd_u) = 3";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn, build, &err))
		pg_prop_fail(tc, "build nd_u failed: %s", err ? err : "(no message)");

	require_true(tc, conn, sql);

	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS nd_u", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/*
 * Property 5: UNIQUE NULLS NOT DISTINCT collapses NULLs, so at most one NULL
 * row is allowed.  Insert one NULL (must succeed), then a second NULL (must
 * fail with a constraint violation), then verify exactly one NULL row remains.
 * This asserts the second insert returns false (failed), which is the whole
 * point of NULLS NOT DISTINCT.
 */
PG_PROP(unique_nulls_not_distinct_rejects)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	bool		ok;
	const char *build =
		"DROP TABLE IF EXISTS nd_u2; "
		"CREATE TABLE nd_u2 (a int UNIQUE NULLS NOT DISTINCT); ";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn, build, &err))
		pg_prop_fail(tc, "build nd_u2 failed: %s", err ? err : "(no message)");
	if (err)
	{
		pg_free(err);
		err = NULL;
	}

	/* First NULL must succeed. */
	if (!pg_prop_sql_exec(conn, "INSERT INTO nd_u2 VALUES (NULL)", &err))
		pg_prop_fail(tc, "first NULL insert failed under NULLS NOT DISTINCT: %s",
					 err ? err : "(no message)");
	if (err)
	{
		pg_free(err);
		err = NULL;
	}

	/* Second NULL must be rejected by the unique constraint. */
	ok = pg_prop_sql_exec(conn, "INSERT INTO nd_u2 VALUES (NULL)", &err);
	if (ok)
		pg_prop_fail(tc, "second NULL insert succeeded under NULLS NOT DISTINCT");
	if (err)
	{
		pg_free(err);
		err = NULL;
	}

	/*
	 * The failed insert aborts its own implicit transaction, so the table
	 * still holds exactly one NULL row.  pg_prop_sql_exec runs each statement
	 * in its own transaction, so the prior successful insert is durable.
	 */
	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM nd_u2 WHERE a IS NULL) = 1");

	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS nd_u2", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/*
 * Property 6 (shape B flavour via scalar count): SELECT DISTINCT ON (a) picks
 * one row per distinct a with NULL as a single group, so its cardinality
 * equals that of SELECT DISTINCT a.  The ORDER BY a NULLS FIRST, id is a total
 * order making the DISTINCT ON choice deterministic.
 */
PG_PROP(distinct_on_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *sql =
		"SELECT (SELECT count(*) FROM "
		"(SELECT DISTINCT ON (a) a FROM nd_t ORDER BY a NULLS FIRST, id) s) "
		"= (SELECT count(*) FROM (SELECT DISTINCT a FROM nd_t) t)";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_true(tc, conn, sql);
	pg_prop_disconnect(conn);
}
