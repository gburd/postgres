/*-------------------------------------------------------------------------
 *
 * prop_numscale.c
 *	  Property-based tests for NUMERIC scale/precision/typmod behaviour.
 *
 * Distinct from prop_numeric.c (arithmetic algebra): this file targets the
 * scale and precision machinery -- rounding a value when it is cast to
 * numeric(p,s), the scale() introspection function, trunc() at a given
 * scale, and the idempotence/round-trip of a typmod-constrained value.  The
 * oracle is each operation's own contract: a cast to numeric(p,s) must equal
 * round(x,s), truncation toward zero must never increase magnitude, and a
 * value already at scale s must survive a second cast and a text round-trip
 * unchanged.  A false result is a bug in apply_typmod / scale handling.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_numscale.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "numscale"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a decimal NUMERIC literal as "<int>.<frac>", parenthesized so a
 * leading sign never misparses when substituted into a larger expression.
 *
 * The integer part is kept modest (up to 6 digits, so < 10^6) and the
 * fractional part up to 6 digits, so the value always fits comfortably in
 * numeric(20,s) for s <= 5 (14 integer digits of headroom).  This keeps every
 * cast in these properties free of precision overflow, which is what we want:
 * we are testing rounding/scale, not the overflow error path.
 */
static void
draw_numeric(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 0, 6);
	int			nfrac = (int) pg_prop_draw_int(tc, 0, 6);
	bool		neg = pg_prop_draw_bool(tc);
	int			ndigits = 0;

	appendStringInfoString(buf, "(");
	if (neg)
		appendStringInfoChar(buf, '-');

	for (int i = 0; i < nint; i++)
	{
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
		ndigits++;
	}

	if (nfrac > 0)
	{
		appendStringInfoChar(buf, '.');
		for (int i = 0; i < nfrac; i++)
		{
			appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
			ndigits++;
		}
	}

	if (ndigits == 0)
		appendStringInfoChar(buf, '0');

	appendStringInfoString(buf, ")::numeric");
}

/*
 * Draw a NUMERIC literal forced non-negative (no sign), for properties whose
 * invariant is stated only for X >= 0.
 */
static void
draw_numeric_nonneg(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 0, 6);
	int			nfrac = (int) pg_prop_draw_int(tc, 0, 6);
	int			ndigits = 0;

	appendStringInfoString(buf, "(");
	for (int i = 0; i < nint; i++)
	{
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
		ndigits++;
	}
	if (nfrac > 0)
	{
		appendStringInfoChar(buf, '.');
		for (int i = 0; i < nfrac; i++)
		{
			appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
			ndigits++;
		}
	}
	if (ndigits == 0)
		appendStringInfoChar(buf, '0');
	appendStringInfoString(buf, ")::numeric");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operation errored or produced NULL on finite inputs,
 * which is itself a bug -- so either is a failure, reported with the SQL.
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
 * Casting to numeric(20,2) rounds to 2 fractional digits: X::numeric(20,2)
 * must equal round(X, 2).  Precision 20 is far larger than any drawn value
 * needs, so the cast cannot overflow and only the scale rounding is exercised
 * -- it must agree with the round() function to the digit.
 */
PG_PROP(cast_to_scale_rounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::numeric(20,2) = round(%s, 2)",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Rounding to s fractional digits yields a value whose scale is at most s:
 * scale(round(X, s)) <= s.  (It can be less -- trailing zeros are dropped --
 * so the bound is one-sided.)  s is drawn in 0..5.
 */
PG_PROP(scale_of_rounded)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			s;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	s = (int) pg_prop_draw_int(tc, 0, 5);
	initStringInfo(&x);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT scale(round(%s, %d)) <= %d",
					 x.data, s, s);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Truncation toward zero never increases magnitude: abs(trunc(X, s)) <=
 * abs(X).  trunc() drops digits beyond scale s toward zero, so the result's
 * absolute value can only shrink or stay equal.  s is drawn in 0..5.
 */
PG_PROP(trunc_le_abs)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			s;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	s = (int) pg_prop_draw_int(tc, 0, 5);
	initStringInfo(&x);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT abs(trunc(%s, %d)) <= abs(%s)",
					 x.data, s, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Casting to a fixed scale is idempotent: once a value is at scale 3, casting
 * it to numeric(20,3) again changes nothing.  (X::numeric(20,3))::numeric(20,3)
 * must equal X::numeric(20,3).
 */
PG_PROP(cast_scale_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s)::numeric(20,3))::numeric(20,3) "
					 "= (%s)::numeric(20,3)",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Round brackets truncation for X >= 0: trunc(X,s) <= round(X,s) <=
 * trunc(X,s) + 10^(-s).  For non-negative X, truncation drops the tail and
 * rounding can only push up by less than one unit in the last place, so the
 * rounded value lies in [trunc, trunc + ulp].  X is drawn non-negative; s in
 * 0..5.
 */
PG_PROP(round_trunc_relation)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			s;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	s = (int) pg_prop_draw_int(tc, 0, 5);
	initStringInfo(&x);
	draw_numeric_nonneg(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT trunc(%s, %d) <= round(%s, %d) "
					 "AND round(%s, %d) <= trunc(%s, %d) "
					 "+ (10::numeric ^ (-%d))",
					 x.data, s, x.data, s,
					 x.data, s, x.data, s, s);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A scale-constrained value survives a text round-trip: rendering
 * X::numeric(20,4) to text and parsing it back as numeric must reproduce the
 * same value.  (X::numeric(20,4))::text::numeric = X::numeric(20,4).
 */
PG_PROP(precision_preserved_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s)::numeric(20,4))::text::numeric "
					 "= (%s)::numeric(20,4)",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The scale of a value cast to numeric(20,s) is bounded by its typmod:
 * scale(X::numeric(20,s)) is between 0 and s.  The cast normalises to at most
 * s fractional digits and scale() is never negative, so the result sits in
 * [0, s].  s is drawn in 0..5.
 */
PG_PROP(scale_nonneg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			s;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	s = (int) pg_prop_draw_int(tc, 0, 5);
	initStringInfo(&x);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT scale((%s)::numeric(20,%d)) BETWEEN 0 AND %d",
					 x.data, s, s);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
