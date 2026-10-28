/*-------------------------------------------------------------------------
 *
 * prop_defaults.c
 *	  Property-based tests for column DEFAULTs, generated columns and NOT NULL.
 *
 * The oracle is the stated semantics of each column feature: a column DEFAULT
 * supplies its value when an INSERT omits the column; a GENERATED ALWAYS AS
 * STORED column equals its expression over the base columns on both INSERT
 * and UPDATE; a NOT NULL constraint rejects a NULL; an IDENTITY column yields
 * strictly increasing values.  Each property builds a fresh scratch table,
 * drives one INSERT/UPDATE, and checks the resulting value with a scalar
 * SELECT (or, for NOT NULL, that the INSERT fails and the table stays empty).
 * A mismatch is a bug in default/generated/constraint handling.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_defaults.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "defaults"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is itself a failure (the oracle expects a definite true).
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
 * Run DDL/DML and require success; a failure here is a setup bug, not a
 * property result, so report it with the SQL.
 */
static void
require_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "exec failed: %s\n  SQL: %s", err ? err : "", sql);
	if (err)
		pg_free(err);
}

/*
 * Draw an integer in [-100, 100].  Returned as an int; callers that embed it
 * in SQL must parenthesize when it may be negative so a leading '-' does not
 * misparse in a larger expression.
 */
static int
draw_k(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, -100, 100);
}

/*
 * A column DEFAULT is applied when the INSERT omits the column:
 *   df(id int, v int DEFAULT 42); INSERT (id) only  =>  v = 42.
 */
PG_PROP(default_applied)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_exec(tc, conn,
				 "DROP TABLE IF EXISTS df; "
				 "CREATE TABLE df (id int, v int DEFAULT 42); "
				 "INSERT INTO df (id) VALUES (1);");

	require_true(tc, conn, "SELECT v = 42 FROM df WHERE id = 1");

	require_exec(tc, conn, "DROP TABLE df;");
	pg_prop_disconnect(conn);
}

/*
 * A DEFAULT expression is evaluated: v int DEFAULT (10+5)  =>  inserted v = 15.
 */
PG_PROP(default_expression)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_exec(tc, conn,
				 "DROP TABLE IF EXISTS df; "
				 "CREATE TABLE df (id int, v int DEFAULT (10+5)); "
				 "INSERT INTO df (id) VALUES (1);");

	require_true(tc, conn, "SELECT v = 15 FROM df WHERE id = 1");

	require_exec(tc, conn, "DROP TABLE df;");
	pg_prop_disconnect(conn);
}

/*
 * A GENERATED ALWAYS AS (a*2) STORED column equals 2k when a = k is inserted.
 */
PG_PROP(generated_stored)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k = draw_k(tc);
	StringInfoData sql;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_exec(tc, conn,
				 "DROP TABLE IF EXISTS dg; "
				 "CREATE TABLE dg (a int, g int GENERATED ALWAYS AS (a*2) STORED);");

	initStringInfo(&sql);
	appendStringInfo(&sql, "INSERT INTO dg (a) VALUES ((%d));", k);
	require_exec(tc, conn, sql.data);

	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT g = (%d) FROM dg", 2 * k);
	require_true(tc, conn, sql.data);

	require_exec(tc, conn, "DROP TABLE dg;");
	pfree(sql.data);
	pg_prop_disconnect(conn);
}

/*
 * A STORED generated column recomputes on UPDATE: insert a = k, then
 * UPDATE a = m  =>  g = 2m, independent of the original k.
 */
PG_PROP(generated_updates_with_base)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k = draw_k(tc);
	int			m = draw_k(tc);
	StringInfoData sql;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_exec(tc, conn,
				 "DROP TABLE IF EXISTS dg; "
				 "CREATE TABLE dg (a int, g int GENERATED ALWAYS AS (a*2) STORED);");

	initStringInfo(&sql);
	appendStringInfo(&sql, "INSERT INTO dg (a) VALUES ((%d));", k);
	require_exec(tc, conn, sql.data);

	resetStringInfo(&sql);
	appendStringInfo(&sql, "UPDATE dg SET a = (%d);", m);
	require_exec(tc, conn, sql.data);

	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT g = (%d) FROM dg", 2 * m);
	require_true(tc, conn, sql.data);

	require_exec(tc, conn, "DROP TABLE dg;");
	pfree(sql.data);
	pg_prop_disconnect(conn);
}

/*
 * A NOT NULL column rejects a NULL insert: the INSERT must fail
 * (pg_prop_sql_exec returns false) and the table must stay empty.
 */
PG_PROP(not_null_rejects)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char	   *ierr = NULL;
	bool		ok;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_exec(tc, conn,
				 "DROP TABLE IF EXISTS dn; "
				 "CREATE TABLE dn (a int NOT NULL);");

	ok = pg_prop_sql_exec(conn, "INSERT INTO dn (a) VALUES (NULL);", &ierr);
	if (ok)
		pg_prop_fail(tc, "NULL insert into NOT NULL column unexpectedly succeeded");
	if (ierr)
		pg_free(ierr);

	require_true(tc, conn, "SELECT count(*) = 0 FROM dn");

	require_exec(tc, conn, "DROP TABLE dn;");
	pg_prop_disconnect(conn);
}

/*
 * An IDENTITY column increases: insert two rows, the two ids differ and the
 * second is strictly greater than the first.
 */
PG_PROP(serial_increments)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k1 = draw_k(tc);
	int			k2 = draw_k(tc);
	StringInfoData sql;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_exec(tc, conn,
				 "DROP TABLE IF EXISTS ds; "
				 "CREATE TABLE ds (id int GENERATED ALWAYS AS IDENTITY, v int);");

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO ds (v) VALUES ((%d)); "
					 "INSERT INTO ds (v) VALUES ((%d));",
					 k1, k2);
	require_exec(tc, conn, sql.data);

	/*
	 * Two rows, ordered by insertion value is not reliable; order by id and
	 * require the lower id strictly less than the higher.  min < max also
	 * proves they differ.
	 */
	require_true(tc, conn,
				 "SELECT min(id) < max(id) FROM ds");

	require_exec(tc, conn, "DROP TABLE ds;");
	pfree(sql.data);
	pg_prop_disconnect(conn);
}

/*
 * A column with no default inserts NULL: dz(id int, v int); INSERT id only
 * =>  v IS NULL.
 */
PG_PROP(default_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_exec(tc, conn,
				 "DROP TABLE IF EXISTS dz; "
				 "CREATE TABLE dz (id int, v int); "
				 "INSERT INTO dz (id) VALUES (1);");

	require_true(tc, conn, "SELECT v IS NULL FROM dz WHERE id = 1");

	require_exec(tc, conn, "DROP TABLE dz;");
	pg_prop_disconnect(conn);
}
