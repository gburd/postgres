/*-------------------------------------------------------------------------
 *
 * prop_numeric.c
 *	  Property-based tests for arbitrary-precision NUMERIC arithmetic.
 *
 * Target #9 in the design.  The 2024 numeric multiplication/division rewrite
 * (ca481d3c9ab, c4e44224cf6, 8dc28d7eb86, 9428c001f67) reworked carry
 * handling in mul_var, mul_var_short and div_var -- the base-NBASE^2
 * accumulation path and the "short" multiply paths that handle 1..6 NBASE
 * digits.  Carry/length bugs in those routines show up as broken algebraic
 * identities, so these properties check exact identities (commutativity,
 * distributivity, the a = q*b + r division identity, additive inverse) in SQL
 * against the numeric type itself, plus a sound bound on sqrt.
 *
 * The oracle here is the type's own algebra: numeric add/subtract/multiply
 * and trunc-division (div/mod) are exact at the operands' combined scale, so
 * these identities must hold with no tolerance.  Any "f" result is a bug in
 * the arithmetic, not a rounding artefact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_numeric.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "numeric"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a decimal NUMERIC literal as text into buf, parenthesized so a leading
 * sign never misparses when the literal is substituted into a larger
 * expression.
 *
 * Digit counts straddle the boundaries the mul rewrite cares about: one NBASE
 * digit is 4 decimal digits, and mul_var_short handles 1..6 NBASE digits
 * (~24 integer decimal digits), so the integer part is drawn up to 24 digits
 * to cross both the NBASE and NBASE^2 length thresholds.  Products of two
 * 24-digit values are ~48 digits, far below the numeric limit (10^131072), so
 * there is no overflow to gate on.
 */
static void
draw_numeric(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 0, 24);
	int			nfrac = (int) pg_prop_draw_int(tc, 0, 8);
	bool		neg = pg_prop_draw_bool(tc);
	int			ndigits = 0;

	appendStringInfoString(buf, "(");
	if (neg)
		appendStringInfoChar(buf, '-');

	/* integer part */
	for (int i = 0; i < nint; i++)
	{
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
		ndigits++;
	}

	/* fractional part */
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
 * Draw a NUMERIC literal forced non-negative (positive sign), for sqrt().
 */
static void
draw_numeric_nonneg(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 0, 24);
	int			nfrac = (int) pg_prop_draw_int(tc, 0, 8);
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
 * a SQL error means the arithmetic errored or produced NULL on finite inputs,
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
 * Multiplication commutes: X * Y = Y * X.  The base-NBASE^2 rewrite reorders
 * accumulation, so a carry bug would surface as asymmetry here.
 */
PG_PROP(mul_commutes)
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
	draw_numeric(tc, &x);
	draw_numeric(tc, &y);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s * %s) = (%s * %s)",
					 x.data, y.data, y.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The fundamental trunc-division identity: div(X, Y) * Y + mod(X, Y) = X.
 * div() is integer/trunc division for numeric and mod() its matching
 * remainder; this directly exercises div_var.  The CASE guards Y = 0 so no
 * division-by-zero error is expected -- if one still occurs, require_true
 * fails on it.
 */
PG_PROP(div_mod_identity)
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
	draw_numeric(tc, &x);
	draw_numeric(tc, &y);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN %s = 0 THEN true "
					 "ELSE div(%s, %s) * %s + mod(%s, %s) = %s END",
					 y.data,
					 x.data, y.data, y.data, x.data, y.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Additive inverse: (X + Y) - Y = X.  Numeric add/subtract are exact at the
 * operands' scale, so this holds with no rounding.
 */
PG_PROP(add_sub_inverse)
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
	draw_numeric(tc, &x);
	draw_numeric(tc, &y);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s + %s) - %s) = %s",
					 x.data, y.data, y.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Distributivity: X * (Y + Z) = X*Y + X*Z.  A carry/length bug in mul_var
 * often breaks this when one operand crosses a digit-count boundary while the
 * other does not, so the two sides accumulate carries differently.
 */
PG_PROP(mul_distributes_over_add)
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
	draw_numeric(tc, &x);
	draw_numeric(tc, &y);
	draw_numeric(tc, &z);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s * (%s + %s)) = (%s * %s + %s * %s)",
					 x.data, y.data, z.data,
					 x.data, y.data, x.data, z.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(z.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * sqrt bound: S = sqrt(abs(X)) is non-negative and, since numeric sqrt rounds
 * to a scale, S*S must not exceed abs(X) by more than a small relative-plus-
 * absolute tolerance.  This is a sound one-sided bound: an exact sqrt gives
 * S*S = X, and the only slack is the rounding of the last digit, well within
 * abs(X) * 1e-10 + 1e-10.  (The symmetric lower bound is scale-dependent and
 * fiddly to state soundly, so we assert only the clean upper side plus the
 * sign.)
 */
PG_PROP(sqrt_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric_nonneg(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT sqrt(abs(%s)) >= 0 "
					 "AND sqrt(abs(%s)) * sqrt(abs(%s)) "
					 "<= abs(%s) * (1 + 1e-10::numeric) + 1e-10::numeric",
					 x.data, x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
