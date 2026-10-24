/*-------------------------------------------------------------------------
 *
 * prop_boolexpr.c
 *	  Property-based tests for boolean-expression algebra under 3VL.
 *
 * PostgreSQL booleans are three-valued (true, false, NULL/unknown).  The
 * laws of boolean algebra -- associativity, distributivity, absorption,
 * commutativity, identity/annihilator -- all survive the extension to 3VL
 * when AND/OR are given their Kleene truth tables, and this file checks each
 * law directly against the server's AND/OR evaluation.  Because a 3VL result
 * can itself be NULL, both sides of every law are compared with IS NOT
 * DISTINCT FROM (NULL-safe equality) rather than =; a disagreement is a bug
 * in boolean expression evaluation, not a NULL artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_boolexpr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "boolexpr"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw one three-valued boolean literal: 'true', 'false', or NULL (~1/3
 * each).  NULL is cast to bool so AND/OR resolve to the boolean operators
 * even when every operand is NULL.
 */
static void
draw_bool3(pg_prop_tc *tc, StringInfo buf)
{
	int			k = (int) pg_prop_draw_int(tc, 0, 2);

	if (k == 0)
		appendStringInfoString(buf, "true");
	else if (k == 1)
		appendStringInfoString(buf, "false");
	else
		appendStringInfoString(buf, "NULL::bool");
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

/* (a AND b) AND c  is not distinct from  a AND (b AND c). */
PG_PROP(and_associative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bool3(tc, &a);
	initStringInfo(&b);
	draw_bool3(tc, &b);
	initStringInfo(&c);
	draw_bool3(tc, &c);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%s) AND (%s)) AND (%s)) "
					 "IS NOT DISTINCT FROM ((%s) AND ((%s) AND (%s)))",
					 a.data, b.data, c.data, a.data, b.data, c.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* (a OR b) OR c  is not distinct from  a OR (b OR c). */
PG_PROP(or_associative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bool3(tc, &a);
	initStringInfo(&b);
	draw_bool3(tc, &b);
	initStringInfo(&c);
	draw_bool3(tc, &c);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%s) OR (%s)) OR (%s)) "
					 "IS NOT DISTINCT FROM ((%s) OR ((%s) OR (%s)))",
					 a.data, b.data, c.data, a.data, b.data, c.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* a AND (b OR c)  is not distinct from  (a AND b) OR (a AND c). */
PG_PROP(and_distributes_over_or)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bool3(tc, &a);
	initStringInfo(&b);
	draw_bool3(tc, &b);
	initStringInfo(&c);
	draw_bool3(tc, &c);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) AND ((%s) OR (%s))) "
					 "IS NOT DISTINCT FROM "
					 "(((%s) AND (%s)) OR ((%s) AND (%s)))",
					 a.data, b.data, c.data,
					 a.data, b.data, a.data, c.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* a OR (b AND c)  is not distinct from  (a OR b) AND (a OR c). */
PG_PROP(or_distributes_over_and)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bool3(tc, &a);
	initStringInfo(&b);
	draw_bool3(tc, &b);
	initStringInfo(&c);
	draw_bool3(tc, &c);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) OR ((%s) AND (%s))) "
					 "IS NOT DISTINCT FROM "
					 "(((%s) OR (%s)) AND ((%s) OR (%s)))",
					 a.data, b.data, c.data,
					 a.data, b.data, a.data, c.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Absorption, both forms, which hold in 3VL:
 *   a AND (a OR b) is not distinct from a
 *   a OR (a AND b) is not distinct from a
 */
PG_PROP(absorption)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bool3(tc, &a);
	initStringInfo(&b);
	draw_bool3(tc, &b);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%s) AND ((%s) OR (%s))) IS NOT DISTINCT FROM (%s)) "
					 "AND (((%s) OR ((%s) AND (%s))) IS NOT DISTINCT FROM (%s))",
					 a.data, a.data, b.data, a.data,
					 a.data, a.data, b.data, a.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Commutativity of both connectives:
 *   (a AND b) is not distinct from (b AND a)
 *   (a OR  b) is not distinct from (b OR  a)
 */
PG_PROP(and_commutative_or_commutative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bool3(tc, &a);
	initStringInfo(&b);
	draw_bool3(tc, &b);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%s) AND (%s)) IS NOT DISTINCT FROM ((%s) AND (%s))) "
					 "AND (((%s) OR (%s)) IS NOT DISTINCT FROM ((%s) OR (%s)))",
					 a.data, b.data, b.data, a.data,
					 a.data, b.data, b.data, a.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Identity and annihilator elements:
 *   a AND true  is not distinct from a   (true is AND-identity)
 *   a OR  false is not distinct from a   (false is OR-identity)
 *   a AND false = false                  (false annihilates AND)
 *   a OR  true  = true                   (true annihilates OR)
 * The annihilator results are never NULL, so plain = is sound there.
 */
PG_PROP(identity_annihilator)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bool3(tc, &a);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%s) AND true) IS NOT DISTINCT FROM (%s)) "
					 "AND (((%s) OR false) IS NOT DISTINCT FROM (%s)) "
					 "AND (((%s) AND false) = false) "
					 "AND (((%s) OR true) = true)",
					 a.data, a.data, a.data, a.data, a.data, a.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
