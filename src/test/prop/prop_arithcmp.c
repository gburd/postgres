/*-------------------------------------------------------------------------
 *
 * prop_arithcmp.c
 *	  Property-based tests for comparison-operator consistency.
 *
 * The oracle is the algebra the comparison operators must obey: trichotomy
 * (<, =, > partition any ordered pair), the derived operators (<=, >=, <>)
 * expressed from < and =, cross-type agreement (the same integer compares
 * equal across int4/int8/numeric/float8), and transitivity of <.  Each
 * property checks one of these identities in SQL and requires it to hold for
 * every drawn input; a failure is a bug in a comparison operator or in a
 * cross-type cast/coercion.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_arithcmp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "arithcmp"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw an int literal across the full int4 range (favouring boundaries), as
 * text, parenthesized if negative so a leading '-' does not misparse when
 * substituted into a cast like (%s)::int8.
 */
static void
draw_int(pg_prop_tc *tc, StringInfo buf)
{
	int			v = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	if (v < 0)
		appendStringInfo(buf, "(%d)", v);
	else
		appendStringInfo(buf, "%d", v);
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

/* Trichotomy: exactly one of a<b, a=b, a>b holds. */
PG_PROP(cmp_trichotomy)
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
	initStringInfo(&b);
	draw_int(tc, &a);
	draw_int(tc, &b);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s < %s)::int + (%s = %s)::int + (%s > %s)::int) = 1",
					 a.data, b.data, a.data, b.data, a.data, b.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* <= is < OR =. */
PG_PROP(le_eq_lt_or_eq)
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
	initStringInfo(&b);
	draw_int(tc, &a);
	draw_int(tc, &b);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s <= %s) = (%s < %s OR %s = %s)",
					 a.data, b.data, a.data, b.data, a.data, b.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* >= is the negation of <. */
PG_PROP(ge_is_not_lt)
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
	initStringInfo(&b);
	draw_int(tc, &a);
	draw_int(tc, &b);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s >= %s) = (NOT (%s < %s))",
					 a.data, b.data, a.data, b.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* <> is the negation of =. */
PG_PROP(ne_is_not_eq)
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
	initStringInfo(&b);
	draw_int(tc, &a);
	draw_int(tc, &b);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s <> %s) = (NOT (%s = %s))",
					 a.data, b.data, a.data, b.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* The same integer value compares equal across int4/int8/numeric/float8. */
PG_PROP(cross_type_cmp)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_int(tc, &a);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s)::int4 = (%s)::int8 "
					 "AND (%s)::int4 = (%s)::numeric "
					 "AND (%s)::int4::float8 = (%s)::float8",
					 a.data, a.data, a.data, a.data, a.data, a.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* Transitivity of <: a<b AND b<c implies a<c. */
PG_PROP(transitivity)
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
	initStringInfo(&b);
	initStringInfo(&c);
	draw_int(tc, &a);
	draw_int(tc, &b);
	draw_int(tc, &c);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT NOT (%s < %s AND %s < %s) OR (%s < %s)",
					 a.data, b.data, b.data, c.data, a.data, c.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
