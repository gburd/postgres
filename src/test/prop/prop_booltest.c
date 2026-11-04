/*-------------------------------------------------------------------------
 *
 * prop_booltest.c
 *	  Property-based tests for IS TRUE / IS FALSE / IS UNKNOWN boolean tests.
 *
 * The oracle is the defining expansion of each boolean test: IS TRUE is
 * coalesce(x = true, false), IS UNKNOWN is x IS NULL, and the three states
 * partition the input (exactly one holds for any bool-or-NULL x).  Each
 * property reconstructs a test from its definition in SQL and requires the
 * two sides to agree; a disagreement is a bug in the BooleanTest code.  These
 * tests never produce NULL -- that is the whole point of IS TRUE/FALSE/UNKNOWN
 * -- so a NULL oracle result is itself a failure.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_booltest.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "booltest"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Draw a bool-or-NULL literal: 'true', 'false', or NULL (~1/3 each). */
static void
draw_bool_or_null(pg_prop_tc *tc, StringInfo buf)
{
	int			k = (int) pg_prop_draw_int(tc, 0, 2);

	if (k == 0)
		appendStringInfoString(buf, "NULL::bool");
	else if (k == 1)
		appendStringInfoString(buf, "true");
	else
		appendStringInfoString(buf, "false");
}

/* Run a boolean SELECT and require "t"; NULL or error is a failure. */
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

/* IS TRUE equals coalesce(x = true, false). */
PG_PROP(is_true_definition)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_bool_or_null(tc, &x);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) IS TRUE) = coalesce((%s) = true, false)",
					 x.data, x.data);
	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* IS NOT TRUE is the complement of IS TRUE. */
PG_PROP(is_not_true_complement)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_bool_or_null(tc, &x);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) IS NOT TRUE) = (NOT ((%s) IS TRUE))",
					 x.data, x.data);
	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* IS UNKNOWN equals IS NULL for a boolean. */
PG_PROP(is_unknown_eq_is_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_bool_or_null(tc, &x);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) IS UNKNOWN) = ((%s) IS NULL)",
					 x.data, x.data);
	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The three states partition the input: exactly one of IS TRUE, IS FALSE,
 * IS UNKNOWN holds for any bool-or-NULL x.
 */
PG_PROP(exactly_one_state)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_bool_or_null(tc, &x);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%s) IS TRUE)::int + ((%s) IS FALSE)::int + "
					 "((%s) IS UNKNOWN)::int) = 1",
					 x.data, x.data, x.data);
	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* Double negation is identity, including for NULL: NOT NOT x = x (3VL). */
PG_PROP(double_negation)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_bool_or_null(tc, &x);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (NOT (NOT (%s))) IS NOT DISTINCT FROM (%s)",
					 x.data, x.data);
	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
