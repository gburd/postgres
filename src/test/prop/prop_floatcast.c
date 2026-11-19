/*-------------------------------------------------------------------------
 *
 * prop_floatcast.c
 *	  Property-based tests for float4/float8/numeric cast relationships.
 *
 * These check the IEEE-754 representability facts that PostgreSQL's binary
 * float casts rely on: float4 widens to float8 exactly, every int4 (and every
 * int8 below 2^52) is exact in float8, small decimals survive a float8<->
 * numeric round-trip within tolerance, numeric->float8 is monotone, and the
 * float4 narrowing cast stays within its ~7-significant-digit precision.  A
 * failure is a wrong-results bug in the float/numeric cast code, not a
 * rounding artefact: exact facts are asserted exactly, lossy ones only within
 * a stated tolerance.
 *
 * Distinct from prop_float/floatspecial/numcast/numericcmp: this file is
 * purely about the width/precision relationships *between* float4, float8 and
 * numeric, not arithmetic, specials, integer-numeric casts, or comparison.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_floatcast.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "floatcast"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the cast errored or produced NULL on finite inputs, which
 * is itself a bug -- so either is a failure, reported with the SQL.
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
 * Emit an integer literal safe for immediate casting.  A bare negative like
 * -2147483648::int4 parses as -(2147483648::int4), whose positive magnitude
 * overflows the target type before negation; wrapping any negative in
 * parentheses makes the cast apply to the already-negative value.  INT64_MIN
 * cannot be written as a positive literal at all (unused here, as the drawn
 * ranges exclude it, but handled for safety).
 */
static void
append_int_literal(StringInfo buf, int64 n)
{
	if (n == INT64_MIN)
		appendStringInfoString(buf, "(-9223372036854775807 - 1)");
	else if (n < 0)
		appendStringInfo(buf, "(%lld)", (long long) n);
	else
		appendStringInfo(buf, "%lld", (long long) n);
}

/*
 * Draw a small exact decimal literal "<int>.<frac>" with up to 4 fractional
 * digits, parenthesized (with a leading sign when negative) so it substitutes
 * cleanly into a larger expression.  The integer part is kept to a few digits
 * so the value stays small enough that float8 and float4 round-trips behave as
 * the properties expect.
 */
static void
draw_small_decimal(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 1, 4);
	int			nfrac = (int) pg_prop_draw_int(tc, 0, 4);
	bool		neg = pg_prop_draw_bool(tc);

	appendStringInfoChar(buf, '(');
	if (neg)
		appendStringInfoChar(buf, '-');
	for (int i = 0; i < nint; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	if (nfrac > 0)
	{
		appendStringInfoChar(buf, '.');
		for (int i = 0; i < nfrac; i++)
			appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	}
	appendStringInfoChar(buf, ')');
}

/*
 * 1. float4_to_float8_exact
 *
 * Widening a float4 value to float8 and narrowing it back is lossless: float8
 * exactly represents every float4, so v::float4::float8::float4 = v::float4
 * for a drawn finite v.  v is drawn as a small decimal literal.
 */
PG_PROP(float4_to_float8_exact)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_small_decimal(tc, &v);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT %s::float4::float8::float4 = %s::float4",
					 v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. int_to_float8_exact
 *
 * Every int4 has |n| < 2^31 < 2^53, so it is exactly representable in float8
 * and n::float8::int4 = n with no loss.
 */
PG_PROP(int_to_float8_exact)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_int_literal(&q, n);
	appendStringInfoString(&q, "::int4::float8::int4) = ");
	append_int_literal(&q, n);
	appendStringInfoString(&q, "::int4");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. float8_numeric_roundtrip_small
 *
 * A small exact decimal x, once cast to float8, may not be exactly
 * representable, so a float8->numeric->float8 round-trip is checked only
 * within tolerance: abs(x::float8::numeric::float8 - x::float8) < 1e-9.  The
 * comparison is anchored to x::float8 (not x) so the inexactness of the
 * decimal->binary step is not counted against the round-trip.
 */
PG_PROP(float8_numeric_roundtrip_small)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_small_decimal(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(%s::float8::numeric::float8 - %s::float8) "
					 "< 1e-9",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. numeric_to_float8_monotone
 *
 * The numeric->float8 cast is monotone: for two small numerics with a <= b,
 * a::float8 <= b::float8.  Two small decimals are drawn and ordered with
 * least()/greatest() so the <= always holds on the inputs; the property is
 * that casting preserves that order.
 */
PG_PROP(numeric_to_float8_monotone)
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
	draw_small_decimal(tc, &a);
	draw_small_decimal(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT least(%s::numeric, %s::numeric)::float8 "
					 "<= greatest(%s::numeric, %s::numeric)::float8",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. float4_precision_bound
 *
 * The float4 narrowing cast keeps ~7 significant decimal digits, so for a
 * small drawn float8 v, abs(v::float4::float8 - v) <= abs(v) * 1e-6 + 1e-6.
 * The relative term covers the ~2^-24 mantissa step for larger magnitudes and
 * the absolute term covers values near zero.
 */
PG_PROP(float4_precision_bound)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_small_decimal(tc, &v);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(%s::float8::float4::float8 - %s::float8) "
					 "<= abs(%s::float8) * 1e-6 + 1e-6",
					 v.data, v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. zero_casts
 *
 * Zero maps to zero across every width and type: 0::float4 = 0::float8, the
 * numeric zero casts to float8 zero, and a float8 zero narrows to int4 zero.
 * No inputs are drawn; this is a fixed sanity anchor for the zero element.
 */
PG_PROP(zero_casts)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT 0::float4 = 0::float8 "
				 "AND 0::numeric::float8 = 0::float8 "
				 "AND 0.0::float8::int4 = 0");

	pg_prop_disconnect(conn);
}

/*
 * 7. int8_to_float8_large_bound
 *
 * float8 exactly represents every integer with |n| <= 2^53; drawing n in
 * 0..2^52 stays well inside that, so n::float8::int8 = n with no loss even for
 * values far beyond the int4 range checked in int_to_float8_exact.
 */
PG_PROP(int8_to_float8_large_bound)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, 0, INT64CONST(4503599627370496));

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_int_literal(&q, n);
	appendStringInfoString(&q, "::int8::float8::int8) = ");
	append_int_literal(&q, n);
	appendStringInfoString(&q, "::int8");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
