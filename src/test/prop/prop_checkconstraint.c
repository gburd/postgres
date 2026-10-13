/*-------------------------------------------------------------------------
 *
 * prop_checkconstraint.c
 *      Property-based tests for CHECK and domain constraint enforcement.
 *
 * The oracle is the SQL standard semantics of constraint enforcement: a row
 * is rejected (INSERT fails) iff a CHECK predicate evaluates to FALSE, and is
 * accepted when the predicate is TRUE or UNKNOWN (NULL).  Domain CHECKs and
 * NOT NULL follow the same rule.  Each property builds a fresh scratch table
 * (and domain, where relevant) with a unique name, then asserts the exact
 * pg_prop_sql_exec outcome for allowed vs rejected rows.  A wrong outcome --
 * a violating row accepted, or a legal row rejected -- is a constraint bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      src/test/prop/prop_checkconstraint.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "checkconstraint"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run "sql" for side effects and require it to succeed (want==true) or to be
 * rejected by a constraint (want==false).  A mismatch is the property
 * failure: a violating row accepted, or a legal row wrongly rejected.
 */
static void
require_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql, bool want)
{
	char	   *err = NULL;
	bool		ok;

	ok = pg_prop_sql_exec(conn, sql, &err);
	if (ok != want)
	{
		pg_prop_fail(tc,
					 "expected %s but got %s for: %s\n  error: %s",
					 want ? "success" : "rejection",
					 ok ? "success" : "rejection",
					 sql,
					 err ? err : "(none)");
	}
	if (err)
		pg_free(err);
}

/* Setup DDL/DML that must succeed; a failure here is a harness/env problem. */
static void
must_setup(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "setup failed: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (err)
		pg_free(err);
}

/*
 * CHECK (v > 0): a violating insert (v = 0) must be rejected and a satisfying
 * insert (v = 5) must succeed.
 */
PG_PROP(check_rejects_violation)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	must_setup(tc, conn,
			   "DROP TABLE IF EXISTS cc_rv; "
			   "CREATE TABLE cc_rv (v int CHECK (v > 0));");

	require_exec(tc, conn, "INSERT INTO cc_rv VALUES (0)", false);
	require_exec(tc, conn, "INSERT INTO cc_rv VALUES (5)", true);

	must_setup(tc, conn, "DROP TABLE cc_rv;");
	pg_prop_disconnect(conn);
}

/*
 * CHECK (v > 0) must accept NULL: the predicate is UNKNOWN on NULL, and a
 * CHECK fails only on FALSE, so INSERT NULL succeeds.
 */
PG_PROP(check_allows_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	must_setup(tc, conn,
			   "DROP TABLE IF EXISTS cc_null; "
			   "CREATE TABLE cc_null (v int CHECK (v > 0));");

	require_exec(tc, conn, "INSERT INTO cc_null VALUES (NULL)", true);

	must_setup(tc, conn, "DROP TABLE cc_null;");
	pg_prop_disconnect(conn);
}

/*
 * A domain CHECK (VALUE > 0) enforces the same way as a table CHECK: a column
 * of that domain rejects values <= 0 and accepts positive values.
 */
PG_PROP(domain_constraint)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	must_setup(tc, conn,
			   "DROP TABLE IF EXISTS cc_dom; "
			   "DROP DOMAIN IF EXISTS cc_posint; "
			   "CREATE DOMAIN cc_posint AS int CHECK (VALUE > 0); "
			   "CREATE TABLE cc_dom (v cc_posint);");

	require_exec(tc, conn, "INSERT INTO cc_dom VALUES (0)", false);
	require_exec(tc, conn, "INSERT INTO cc_dom VALUES (5)", true);

	must_setup(tc, conn,
			   "DROP TABLE cc_dom; "
			   "DROP DOMAIN cc_posint;");
	pg_prop_disconnect(conn);
}

/*
 * CHECK (v BETWEEN 1 AND 10): a value inside the range is accepted; values
 * below (0) and above (11) the range are rejected.
 */
PG_PROP(multi_check)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	must_setup(tc, conn,
			   "DROP TABLE IF EXISTS cc_mc; "
			   "CREATE TABLE cc_mc (v int CHECK (v BETWEEN 1 AND 10));");

	require_exec(tc, conn, "INSERT INTO cc_mc VALUES (5)", true);
	require_exec(tc, conn, "INSERT INTO cc_mc VALUES (0)", false);
	require_exec(tc, conn, "INSERT INTO cc_mc VALUES (11)", false);

	must_setup(tc, conn, "DROP TABLE cc_mc;");
	pg_prop_disconnect(conn);
}

/*
 * CHECK (a + b > 0) over two columns: (3, -1) sums to 2 (> 0) and is
 * accepted; (1, -5) sums to -4 and is rejected.
 */
PG_PROP(check_on_expression)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	must_setup(tc, conn,
			   "DROP TABLE IF EXISTS cc_ce; "
			   "CREATE TABLE cc_ce (a int, b int, CHECK (a + b > 0));");

	require_exec(tc, conn, "INSERT INTO cc_ce VALUES (3, (-1))", true);
	require_exec(tc, conn, "INSERT INTO cc_ce VALUES (1, (-5))", false);

	must_setup(tc, conn, "DROP TABLE cc_ce;");
	pg_prop_disconnect(conn);
}

/*
 * A column that is both NOT NULL and CHECK (v > 0): a NULL is rejected by the
 * NOT NULL constraint (the CHECK alone would allow it), while a valid positive
 * value is accepted.
 */
PG_PROP(not_null_and_check)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	must_setup(tc, conn,
			   "DROP TABLE IF EXISTS cc_nn; "
			   "CREATE TABLE cc_nn (v int NOT NULL CHECK (v > 0));");

	require_exec(tc, conn, "INSERT INTO cc_nn VALUES (NULL)", false);
	require_exec(tc, conn, "INSERT INTO cc_nn VALUES (5)", true);

	must_setup(tc, conn, "DROP TABLE cc_nn;");
	pg_prop_disconnect(conn);
}
