/*-------------------------------------------------------------------------
 *
 * prop_cmpops.c
 *	  Property-based tests for BETWEEN / IS DISTINCT comparison constructs.
 *
 * The oracle is the standard's definition of each construct in terms of the
 * primitive comparison operators: BETWEEN desugars to a pair of >= / <=,
 * BETWEEN SYMMETRIC to a BETWEEN over the ordered endpoints, NOT BETWEEN to
 * the negation, and IS [NOT] DISTINCT FROM to <> extended with a defined
 * answer when operands are NULL.  Each property checks one such identity in
 * SQL and requires it to hold for every drawn integer; a failure is a bug in
 * the construct's transformation or in a cross-type cast/coercion.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_cmpops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cmpops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw an int4 literal (favouring boundaries), parenthesized if negative so a
 * leading '-' does not misparse when substituted into a larger expression or
 * a cast like (%s)::int8.
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

/*
 * BETWEEN desugars to a conjunction of range bounds:
 *   (x BETWEEN a AND b) = (x >= a AND x <= b).
 */
PG_PROP(between_eq_and)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&a);
	initStringInfo(&b);
	draw_int(tc, &x);
	draw_int(tc, &a);
	draw_int(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s BETWEEN %s AND %s) = (%s >= %s AND %s <= %s)",
					 x.data, a.data, b.data,
					 x.data, a.data, x.data, b.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * BETWEEN SYMMETRIC orders its endpoints first:
 *   (x BETWEEN SYMMETRIC a AND b) = (x BETWEEN least(a,b) AND greatest(a,b)).
 */
PG_PROP(between_symmetric)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&a);
	initStringInfo(&b);
	draw_int(tc, &x);
	draw_int(tc, &a);
	draw_int(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s BETWEEN SYMMETRIC %s AND %s) = "
					 "(%s BETWEEN least(%s, %s) AND greatest(%s, %s))",
					 x.data, a.data, b.data,
					 x.data, a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * NOT BETWEEN is the negation of BETWEEN:
 *   (x NOT BETWEEN a AND b) = NOT (x BETWEEN a AND b).
 */
PG_PROP(not_between)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&a);
	initStringInfo(&b);
	draw_int(tc, &x);
	draw_int(tc, &a);
	draw_int(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s NOT BETWEEN %s AND %s) = (NOT (%s BETWEEN %s AND %s))",
					 x.data, a.data, b.data,
					 x.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The same integer compares equal across types: int4 = numeric, int4 = int8,
 * and int4 widened to float8 = float8.  An int4 value is representable exactly
 * in each of these types, so every equality must hold.
 */
PG_PROP(cross_type_eq)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_int(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s)::int4 = (%s)::numeric "
					 "AND (%s)::int4 = (%s)::int8 "
					 "AND ((%s)::int4)::float8 = (%s)::float8",
					 x.data, x.data, x.data, x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For non-NULL operands, IS DISTINCT FROM is exactly <>.
 */
PG_PROP(is_distinct_vs_eq)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				y,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&y);
	draw_int(tc, &x);
	draw_int(tc, &y);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s IS DISTINCT FROM %s) = (%s <> %s)",
					 x.data, y.data, x.data, y.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Two NULLs are not distinct: IS NOT DISTINCT FROM is true and IS DISTINCT
 * FROM is false.  This is the whole point of the construct -- plain = / <>
 * would return NULL here.
 */
PG_PROP(is_not_distinct_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (NULL::int IS NOT DISTINCT FROM NULL::int) "
				 "AND (NOT (NULL::int IS DISTINCT FROM NULL::int))");

	pg_prop_disconnect(conn);
}

/*
 * An empty range (lower bound strictly above upper bound) contains nothing:
 * when a > b, (x BETWEEN a AND b) is false for every x.  We draw a strictly
 * greater than b by drawing two ints and taking greatest/least, forcing them
 * apart by +1 so the range is genuinely empty regardless of ties.
 */
PG_PROP(between_empty_range)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				p,
				r,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&p);
	initStringInfo(&r);
	draw_int(tc, &x);
	draw_int(tc, &p);
	draw_int(tc, &r);

	/*
	 * a := greatest(p,r) + 1, b := least(p,r), computed in int8 so adding 1 to
	 * INT32_MAX cannot overflow.  Then a > b holds unconditionally, so the
	 * range [a,b] is empty.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s BETWEEN "
					 "(greatest((%s)::int8, (%s)::int8) + 1) "
					 "AND least((%s)::int8, (%s)::int8)) = false",
					 x.data, p.data, r.data, p.data, r.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(p.data);
	pfree(r.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Transitivity of <: NOT (x < y AND y < z) OR (x < z).  Equivalently, if
 * x < y and y < z then x < z.  This is the material-implication form so the
 * single boolean is true for every drawn triple.
 */
PG_PROP(ternary_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				y,
				z,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&y);
	initStringInfo(&z);
	draw_int(tc, &x);
	draw_int(tc, &y);
	draw_int(tc, &z);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (NOT (%s < %s AND %s < %s)) OR (%s < %s)",
					 x.data, y.data, y.data, z.data, x.data, z.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(z.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
