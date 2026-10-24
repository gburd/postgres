/*-------------------------------------------------------------------------
 *
 * prop_mathround.c
 *	  Property-based tests for the rounding-family functions and their
 *	  algebraic relationships: floor, ceil, round, trunc and sign.
 *
 * These check the ordering and integrality relationships that tie the
 * rounding family together -- floor(x) <= x <= ceil(x), round(x) landing on
 * one of the two bounding integers, trunc rounding toward zero, sign matching
 * the comparison trichotomy, and NUMERIC round-half-away-from-zero.  The
 * oracle is each relationship stated as a boolean SELECT that must be true for
 * every finite input; a false (or NULL/error) result is a bug in dtrunc/dround
 * or numeric_round/numeric_trunc, not a rounding artefact, because every
 * assertion here is exact at the type's own scale.  This area is deliberately
 * distinct from float/floatspecial (I/O and non-finite values), numround
 * (scale-parameterised numeric rounding bounds) and mathfn (transcendental
 * float8 functions): it is about the cross-function round/trunc/ceil/floor/
 * sign algebra over both float8 and numeric.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_mathround.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "mathround"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a finite decimal value as a single-quoted "<int>.<frac>" literal body
 * (e.g. '12.34'), both signs, with up to 12 integer and 6 fractional digits.
 * The caller appends the cast (::float8 or ::numeric).  Single-quoting plus a
 * trailing cast keeps a leading '-' from misparsing when substituted into a
 * larger expression, and the literal is identical whichever type it feeds, so
 * a float8 and numeric check see the same value.
 */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 1, 12);
	int			nfrac = (int) pg_prop_draw_int(tc, 0, 6);
	bool		neg = pg_prop_draw_bool(tc);

	appendStringInfoChar(buf, '\'');
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

	appendStringInfoChar(buf, '\'');
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on a finite input,
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
 * floor(x) <= x <= ceil(x), and ceil(x) - floor(x) is 0 (x integral) or 1.
 * The float8 oracle: floor never overshoots, ceil never undershoots, and the
 * two bounding integers are adjacent (or equal).
 */
PG_PROP(floor_le_ceil)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_val(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT floor(%s::float8) <= %s::float8 "
					 "AND %s::float8 <= ceil(%s::float8) "
					 "AND (ceil(%s::float8) - floor(%s::float8)) IN (0, 1)",
					 x.data, x.data, x.data, x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * round(x) lands on one of the two bounding integers: round(x) is floor(x) or
 * ceil(x).  True regardless of the half-rounding rule the platform uses.
 */
PG_PROP(round_between)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_val(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT round(%s::float8) IN (floor(%s::float8), ceil(%s::float8))",
					 x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * trunc rounds toward zero: for x >= 0 trunc(x) = floor(x); for x < 0
 * trunc(x) = ceil(x).  draw_val yields both signs, so this covers both
 * branches of the toward-zero rule.
 */
PG_PROP(trunc_toward_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_val(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN %s::float8 >= 0 "
					 "THEN trunc(%s::float8) = floor(%s::float8) "
					 "ELSE trunc(%s::float8) = ceil(%s::float8) END",
					 x.data, x.data, x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * sign(x) agrees with the comparison trichotomy: 1 if x > 0, -1 if x < 0,
 * else 0.  float8 sign.
 */
PG_PROP(sign_consistency)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_val(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT sign(%s::float8) = "
					 "CASE WHEN %s::float8 > 0 THEN 1 "
					 "WHEN %s::float8 < 0 THEN -1 ELSE 0 END",
					 x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * floor(x) and ceil(x) are already integers, so truncating them is a no-op:
 * floor(x) = trunc(floor(x)) AND ceil(x) = trunc(ceil(x)).
 */
PG_PROP(floor_ceil_integer)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_val(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT floor(%s::float8) = trunc(floor(%s::float8)) "
					 "AND ceil(%s::float8) = trunc(ceil(%s::float8))",
					 x.data, x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * NUMERIC round rounds half away from zero: round(0.5) = 1, round(1.5) = 2,
 * round(2.5) = 3, round(-0.5) = -1.  float8 round is libc-dependent (some
 * platforms round half to even), so this is asserted only for ::numeric,
 * where the rule is defined by PostgreSQL.  Constant checks, no drawn input.
 */
PG_PROP(round_half_away)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT round('0.5'::numeric) = 1 "
				 "AND round('1.5'::numeric) = 2 "
				 "AND round('2.5'::numeric) = 3 "
				 "AND round('-0.5'::numeric) = -1");

	pg_prop_disconnect(conn);
}

/*
 * The fractional part x - floor(x) lies in [0, 1): non-negative and strictly
 * below one.  float8; strict upper bound because floor removes exactly the
 * whole part.
 */
PG_PROP(floor_plus_frac)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_val(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s::float8 - floor(%s::float8)) >= 0 "
					 "AND (%s::float8 - floor(%s::float8)) < 1",
					 x.data, x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Rounding to a fixed scale is idempotent: round(round(x, 2), 2) =
 * round(x, 2).  Once rounded to 2 fractional digits, rounding again changes
 * nothing.  numeric, so the scale is exact and the equality has no tolerance.
 */
PG_PROP(double_round_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_val(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT round(round(%s::numeric, 2), 2) = round(%s::numeric, 2)",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
