/*-------------------------------------------------------------------------
 *
 * prop_numericdiv.c
 *	  Property-based tests for NUMERIC division/multiplication scale.
 *
 * Numeric multiplication is exact at the operands' combined scale, but
 * division (/) rounds to a selected display scale, so the division oracle
 * here is a rounded identity: a round-trip through multiply-then-divide, and
 * algebraic identities (commutativity, associativity, distributivity,
 * division by 1 and by self, sign of a quotient) compared at a common scale.
 * A broken carry in mul_var/div_var surfaces as one of these identities
 * failing even after rounding to a modest common scale, which is a bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_numericdiv.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "numericdiv"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a small-magnitude NUMERIC literal as '<int>.<frac>', parenthesized and
 * cast, so products and quotients stay comfortably representable and the
 * division rounding is dominated by a modest common scale.  The integer part
 * is 0..20 and the fractional part is 1..3 digits.  A random sign is applied.
 */
static void
draw_numeric(pg_prop_tc *tc, StringInfo buf)
{
	int			ipart = (int) pg_prop_draw_int(tc, 0, 20);
	int			nfrac = (int) pg_prop_draw_int(tc, 1, 3);
	bool		neg = pg_prop_draw_bool(tc);

	appendStringInfoString(buf, "(");
	if (neg)
		appendStringInfoChar(buf, '-');
	appendStringInfo(buf, "%d.", ipart);
	for (int i = 0; i < nfrac; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoString(buf, ")::numeric");
}

/*
 * Draw a non-zero NUMERIC literal as '<1..20>.<frac>' with a random sign.
 * The integer part starts at 1 so the magnitude is at least 1.0, keeping it
 * well away from zero for use as a divisor.
 */
static void
draw_numeric_nonzero(pg_prop_tc *tc, StringInfo buf)
{
	int			ipart = (int) pg_prop_draw_int(tc, 1, 20);
	int			nfrac = (int) pg_prop_draw_int(tc, 1, 3);
	bool		neg = pg_prop_draw_bool(tc);

	appendStringInfoString(buf, "(");
	if (neg)
		appendStringInfoChar(buf, '-');
	appendStringInfo(buf, "%d.", ipart);
	for (int i = 0; i < nfrac; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
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
 * Multiply/divide round-trip: round((X*Y)/Y, 6) = round(X, 6) for Y <> 0.
 * Multiplication is exact; the only slack is division's rounding, so rounding
 * both sides to a common scale (6) must recover X.
 */
PG_PROP(mul_div_roundtrip)
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
	draw_numeric_nonzero(tc, &y);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT round((%s * %s) / %s, 6) = round(%s, 6)",
					 x.data, y.data, y.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Division scale is sufficient: Y * (X / Y) reconstructs X to within a tiny
 * tolerance.  numeric division selects a display scale big enough that the
 * residual abs(Y*(X/Y) - X) is far below 1e-6.
 */
PG_PROP(div_scale_sufficient)
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
	draw_numeric_nonzero(tc, &y);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(%s * (%s / %s) - %s) < 1e-6::numeric",
					 y.data, x.data, y.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Multiplication commutes exactly: X * Y = Y * X.  No tolerance, since
 * numeric multiply is exact at the combined scale.
 */
PG_PROP(mul_commutative)
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
 * Multiplication associates: (X*Y)*Z = X*(Y*Z) within a common scale.
 * Multiply is exact, so this holds exactly, but we round both sides to 6 to
 * match the area's division-rounding convention and stay robust on small
 * magnitudes.
 */
PG_PROP(mul_associative)
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
	appendStringInfo(&q,
					 "SELECT round((%s * %s) * %s, 6) = round(%s * (%s * %s), 6)",
					 x.data, y.data, z.data, x.data, y.data, z.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(z.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Division by one is identity: X / 1 = X, exactly.
 */
PG_PROP(div_by_one)
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
	appendStringInfo(&q, "SELECT (%s / 1) = %s", x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Division by self is one: X / X = 1 for X <> 0.  The divisor is drawn
 * non-zero so no division-by-zero error is expected.
 */
PG_PROP(div_self_one)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric_nonzero(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s / %s) = 1", x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Multiplication distributes over addition: X*(Y+Z) = X*Y + X*Z within a
 * common scale.  Both sides are exact for numeric, but we round to 6 to match
 * the area convention.
 */
PG_PROP(mul_distributes)
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
	appendStringInfo(&q,
					 "SELECT round(%s * (%s + %s), 6) = round(%s * %s + %s * %s, 6)",
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
 * Sign of a quotient: (-X) / Y = -(X / Y) within a common scale, for Y <> 0.
 * numeric division truncates toward even rounding of the display scale
 * symmetrically about zero, so negating the dividend negates the rounded
 * quotient; comparing at scale 6 absorbs any last-digit rounding.
 */
PG_PROP(negative_div)
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
	draw_numeric_nonzero(tc, &y);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT round((-(%s)) / %s, 6) = round(-((%s) / %s), 6)",
					 x.data, y.data, x.data, y.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
