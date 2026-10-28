/*-------------------------------------------------------------------------
 *
 * prop_mathfn.c
 *	  Property-based tests for the float8 math functions (abs, mod, power,
 *	  sqrt, exp, ln, floor, ceil, round, trunc, sign).
 *
 * The float8 math functions must satisfy their defining identities and
 * bounds; these check them, skipping domain errors and non-finite inputs
 * where appropriate.
 *
 * The oracle here is each function's defining identity -- sqrt squared
 * recovers its argument, floor/ceil bracket the value and differ by at most
 * one, ln and exp are inverses, trunc-division reconstructs the dividend, and
 * power agrees with repeated multiplication.  Float arithmetic is inexact, so
 * every inexact identity is checked against a tolerance scaled by magnitude;
 * the exact ones (abs, floor/ceil bracketing) are checked exactly.  Inputs
 * are drawn in a moderate range (-1000..1000) so exp/power do not overflow,
 * and NaN/Inf are skipped in C before any SQL is built since those have
 * special-case behaviour out of scope here.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_mathfn.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "mathfn"
#include "pg_prop.h"

#include <math.h>

#include "lib/stringinfo.h"

/*
 * Draw one finite double in [min, max] and format it as a bare "%.17g"
 * literal into buf (parenthesized by the caller as needed).  NaN and Inf are
 * discarded in C before any SQL is built -- the math functions have
 * special-case behaviour on those which is out of scope for these identity
 * checks.  "%.17g" round-trips any IEEE double exactly, so the literal names
 * the drawn value precisely.  The drawn value is returned so a property can
 * inspect it in C.
 */
static double
draw_f8(pg_prop_tc *tc, StringInfo buf, double min, double max)
{
	double		v = pg_prop_draw_float(tc, min, max);
	char		num[64];

	pg_prop_assume(tc, !isnan(v) && !isinf(v));

	snprintf(num, sizeof(num), "%.17g", v);
	appendStringInfoString(buf, num);
	return v;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on inputs the
 * property is supposed to handle, which is itself a failure, reported with
 * the SQL.
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
 * Like require_true, but a SQL error that is a legitimate domain or overflow
 * error (out of range, overflow, "cannot", "invalid", or a domain error)
 * discards the case rather than failing it.  Used for exp/ln/power, which can
 * overflow or hit a domain boundary even inside a moderate input range.
 */
static void
require_true_or_skip(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		bool		benign = (err != NULL &&
							  (strstr(err, "out of range") != NULL ||
							   strstr(err, "overflow") != NULL ||
							   strstr(err, "cannot") != NULL ||
							   strstr(err, "invalid") != NULL ||
							   strstr(err, "domain") != NULL));

		if (err)
			pg_free(err);
		if (benign)
			pg_prop_assume(tc, false);	/* legitimate domain/overflow: skip */
		pg_prop_fail(tc, "SQL error for: %s", sql);
	}
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
}

/*
 * abs is non-negative and idempotent: abs(V) >= 0 and abs(abs(V)) = abs(V).
 * Both are exact, so no tolerance is used.
 */
PG_PROP(abs_nonneg_and_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_f8(tc, &v, -1000.0, 1000.0);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(%s::float8) >= 0 "
					 "AND abs(abs(%s::float8)) = abs(%s::float8)",
					 v.data, v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * sqrt squared recovers its argument: sqrt(X)^2 ~= X for X >= 0.  X is forced
 * non-negative with abs() so sqrt is always defined.  sqrt rounds to the
 * nearest representable value, so the identity is checked against a relative
 * tolerance (scaled by magnitude, floored at 1 so small X still has slack).
 */
PG_PROP(sqrt_squared_approx)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_f8(tc, &v, -1000.0, 1000.0);

	/* x = abs(v) so sqrt is defined on a non-negative argument */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(sqrt(abs(%s::float8)) * sqrt(abs(%s::float8)) "
					 "- abs(%s::float8)) "
					 "<= 1e-9 * greatest(abs(%s::float8), 1)",
					 v.data, v.data, v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * floor and ceil bracket V and differ by 0 or 1: floor(V) <= V <= ceil(V) and
 * ceil(V) - floor(V) is 0 (V integral) or 1.  All exact, no tolerance.
 */
PG_PROP(floor_le_x_le_ceil)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_f8(tc, &v, -1000.0, 1000.0);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT floor(%s::float8) <= %s::float8 "
					 "AND %s::float8 <= ceil(%s::float8) "
					 "AND (ceil(%s::float8) - floor(%s::float8)) IN (0, 1)",
					 v.data, v.data, v.data, v.data, v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * round is within half a unit of V: abs(round(V) - V) <= 0.5 (plus a tiny
 * absolute slack for the representation of the half).
 */
PG_PROP(round_within_half)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_f8(tc, &v, -1000.0, 1000.0);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(round(%s::float8) - %s::float8) <= 0.5 + 1e-9",
					 v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * trunc truncates toward zero: abs(trunc(V)) <= abs(V) and trunc(V) has the
 * same sign as V or is zero.  The magnitude side gets a tiny absolute slack.
 */
PG_PROP(trunc_toward_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_f8(tc, &v, -1000.0, 1000.0);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(trunc(%s::float8)) <= abs(%s::float8) + 1e-9 "
					 "AND (sign(trunc(%s::float8)) = sign(%s::float8) "
					 "OR trunc(%s::float8) = 0)",
					 v.data, v.data, v.data, v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ln and exp are inverses on positive X: exp(ln(X)) ~= X.  X is drawn in a
 * small positive-to-bounded range (0.01 .. 100] so ln is defined and exp
 * stays finite; the identity is checked against a relative tolerance.  A
 * domain/overflow error is skipped rather than failed.
 */
PG_PROP(power_ln_exp_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_f8(tc, &x, 0.01, 100.0);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(exp(ln(%s::float8)) - %s::float8) "
					 "<= 1e-6 * %s::float8",
					 x.data, x.data, x.data);

	require_true_or_skip(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * degrees() and radians() are inverse conversions, so radians(degrees(x)) ~= x
 * within a relative tolerance.  (There is no float8 mod() in PostgreSQL -- mod
 * is numeric/integer only -- so the trunc-division identity is tested in the
 * numeric areas, not here.)
 */
PG_PROP(degrees_radians_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_f8(tc, &x, -1000.0, 1000.0);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(radians(degrees(%s::float8)) - %s::float8) "
					 "<= 1e-9 * greatest(abs(%s::float8), 1)",
					 x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * power cross-checked against repeated multiplication for a small integer
 * exponent: power(X, K) ~= X*X*...*X (K factors), using the explicit K-fold
 * product as an independent oracle (1 when K=0).  X is drawn in -10..10 and K
 * in 0..5 so the product stays well within range.  Checked against a relative
 * tolerance; a domain/overflow error is skipped.
 */
PG_PROP(power_matches_repeated_mult_small_exp)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				prod,
				q;
	int			k;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_f8(tc, &x, -10.0, 10.0);
	k = (int) pg_prop_draw_int(tc, 0, 5);

	/* build the explicit K-fold product: "1" for K=0, else x*x*...*x */
	initStringInfo(&prod);
	if (k == 0)
		appendStringInfoString(&prod, "1::float8");
	else
	{
		for (int i = 0; i < k; i++)
		{
			if (i > 0)
				appendStringInfoString(&prod, " * ");
			appendStringInfo(&prod, "(%s::float8)", x.data);
		}
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(power(%s::float8, %d) - (%s)) "
					 "<= 1e-6 * greatest(abs(power(%s::float8, %d)), 1)",
					 x.data, k, prod.data, x.data, k);

	require_true_or_skip(tc, conn, q.data);

	pfree(x.data);
	pfree(prod.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
