/*-------------------------------------------------------------------------
 *
 * prop_numericalg.c
 *	  Property-based tests for NUMERIC transcendental and integer algebra.
 *
 * Covers numeric algebra not exercised by the numeric/numround/numfmt/numcast
 * areas: the transcendental functions (sqrt, exp, ln, log, power) and the
 * integer helpers (mod, gcd, lcm) together with the scale/trunc/round
 * interplay.  The oracle is each function's defining identity -- sqrt(x)^2=x,
 * ln(exp(x))=x, log(b,b^e)=e, a=trunc(a/b)*b+mod(a,b), round-to-scale bound,
 * and gcd/lcm divisibility.  Transcendental identities carry a small tolerance
 * (results are rounded to a working scale); mod/gcd/lcm on integers are exact.
 * A violation is a bug in numeric.c's series/Newton routines or in the
 * gcd/lcm/mod paths, not a rounding artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_numericalg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "numericalg"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a small decimal NUMERIC literal as "<int>.<frac>", parenthesized so a
 * leading sign never misparses when substituted into a larger expression.
 * "small" (<=4 integer digits, <=4 fractional) keeps every transcendental and
 * integer identity far from overflow and keeps counterexamples legible.
 */
static void
draw_numeric(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 0, 4);
	int			nfrac = (int) pg_prop_draw_int(tc, 0, 4);
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

	/* ensure a valid numeric: at least one digit */
	if (ndigits == 0)
		appendStringInfoChar(buf, '0');

	appendStringInfoString(buf, ")::numeric");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on in-range inputs,
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
 * sqrt_sq: for x = abs(drawn numeric) >= 0, sqrt(x) squared returns x within a
 * relative-plus-absolute tolerance.  numeric sqrt rounds to a working scale,
 * so the only slack is the last-digit rounding, well inside 1e-9*(abs(x)+1).
 */
PG_PROP(sqrt_sq)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_numeric(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(sqrt(abs(%s)) * sqrt(abs(%s)) - abs(%s)) "
					 "< 1e-9::numeric * (abs(%s) + 1)",
					 a.data, a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ln_exp_inverse: ln(exp(x)) = x for x in [1, 20].  exp of larger x grows past
 * the comfortable working range, so x is kept <= 20; both are rounded to a
 * scale, hence the 1e-6*(x+1) tolerance.
 */
PG_PROP(ln_exp_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			x;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x = (int) pg_prop_draw_int(tc, 1, 20);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(ln(exp(%d::numeric)) - %d) "
					 "< 1e-6::numeric * (%d + 1)",
					 x, x, x);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * log_pow_inverse: log(b, b^e) = e for integer base b in [2, 9] and exponent e
 * in [0, 6].  b^e <= 9^6 = 531441, far from overflow; log rounds to a scale,
 * hence the 1e-6 tolerance.
 */
PG_PROP(log_pow_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			b,
				e;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	b = (int) pg_prop_draw_int(tc, 2, 9);
	e = (int) pg_prop_draw_int(tc, 0, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(log(%d::numeric, (%d::numeric) ^ %d) - %d) "
					 "< 1e-6::numeric",
					 b, b, e, e);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * mod_identity: the trunc-division identity a = trunc(a/b)*b + mod(a,b) for a
 * drawn numeric and a non-zero integer divisor b in [-20, -1] U [1, 20].  Exact
 * in principle, but a/b carries a fractional scale, so a 1e-9 tolerance absorbs
 * the display rounding of the quotient.
 */
PG_PROP(mod_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			b;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_numeric(tc, &a);

	/* b in 1..20 with a drawn sign; never zero */
	b = (int) pg_prop_draw_int(tc, 1, 20);
	if (pg_prop_draw_bool(tc))
		b = -b;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(%s - (trunc(%s / (%d)) * (%d) "
					 "+ mod(%s, (%d)))) < 1e-9::numeric",
					 a.data, a.data, b, b, a.data, b);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * round_scale: round(x, s) for s in [0, 4] has scale <= s and differs from x by
 * at most half a unit in the last kept place, i.e. 0.5 * 10^(-s).  Both parts
 * are exact for numeric, so this is an exact (no tolerance) check.  scale() of
 * the rounded value is compared to s.
 */
PG_PROP(round_scale)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			s;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x);

	s = (int) pg_prop_draw_int(tc, 0, 4);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT scale(round(%s, %d)) <= %d "
					 "AND abs(round(%s, %d) - %s) "
					 "<= 0.5::numeric * (10::numeric ^ (-%d))",
					 x.data, s, s,
					 x.data, s, x.data, s);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * gcd_divides_both: gcd(a, b) divides both a and b for positive integers a, b
 * in [1, 1000].  Exact integer property: a % gcd = 0 AND b % gcd = 0.
 */
PG_PROP(gcd_divides_both)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, 1, 1000);
	b = (int) pg_prop_draw_int(tc, 1, 1000);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%d::numeric %% gcd(%d::numeric, %d::numeric)) = 0 "
					 "AND (%d::numeric %% gcd(%d::numeric, %d::numeric)) = 0",
					 a, a, b,
					 b, a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * lcm_multiple: lcm(a, b) is a multiple of both a and b for positive integers
 * a, b in [1, 100].  Exact integer property: lcm %% a = 0 AND lcm %% b = 0.
 * lcm(a,b) <= 100*100 = 10000, far from overflow.
 */
PG_PROP(lcm_multiple)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, 1, 100);
	b = (int) pg_prop_draw_int(tc, 1, 100);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (lcm(%d::numeric, %d::numeric) %% %d::numeric) = 0 "
					 "AND (lcm(%d::numeric, %d::numeric) %% %d::numeric) = 0",
					 a, b, a,
					 a, b, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
