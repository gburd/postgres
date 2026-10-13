/*-------------------------------------------------------------------------
 *
 * prop_numround.c
 *	  Property-based tests for NUMERIC rounding and scaling.
 *
 * NUMERIC round/trunc/ceil/floor and scale clamping (fixed in 1ff39f4ff2b)
 * have exact bound and ordering properties; these pin them down, distinct
 * from the arithmetic-identity numeric area.
 *
 * These exercise the rounding/scaling family -- round, trunc, ceil, floor,
 * scale -- with and without a scale argument.  NUMERIC is exact arbitrary
 * precision, so every property is stated as an exact equality or an exact
 * bound (e.g. half an ULP, 0.5 * power(10, -s)); no float tolerance is used.
 * This is distinct from the arithmetic-identity "numeric" area (+, -, *, /)
 * and from the float8 "mathfn" area.
 *
 * The oracle here is the exact decimal semantics of the type: floor/ceil
 * bracket a value and differ by 0 or 1, trunc moves toward zero, round(x, s)
 * lands within half an ULP of x at scale s, and all four are the identity on
 * integer-valued inputs.  Any "f" result is a bug, not a rounding artefact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_numround.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "numround"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a decimal NUMERIC literal as text into buf, parenthesized so a leading
 * sign never misparses when the literal is substituted into a larger
 * expression.
 *
 * Magnitudes are kept modest -- 0..12 integer digits and 0..8 fractional
 * digits with a random sign -- so counterexamples stay legible.  numeric has
 * no overflow concern at these sizes.  When nfrac is 0 the value is an exact
 * integer, which the integer-fixpoint property relies on.
 */
static void
draw_numeric(pg_prop_tc *tc, StringInfo buf, int nfrac)
{
	int			nint = (int) pg_prop_draw_int(tc, 0, 12);
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
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the rounding errored or produced NULL on a finite numeric,
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
 * floor and ceil bracket X and differ by 0 or 1:
 *   floor(X) <= X <= ceil(X), and ceil(X) - floor(X) IN (0, 1).
 */
PG_PROP(floor_le_x_le_ceil)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x, (int) pg_prop_draw_int(tc, 0, 8));

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT floor(%s) <= %s AND %s <= ceil(%s) "
					 "AND (ceil(%s) - floor(%s)) IN (0, 1)",
					 x.data, x.data, x.data, x.data,
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * trunc toward zero never increases magnitude and yields an integer:
 *   abs(trunc(X)) <= abs(X) and scale(trunc(X)) = 0.
 */
PG_PROP(trunc_abs_le_abs)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x, (int) pg_prop_draw_int(tc, 0, 8));

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(trunc(%s)) <= abs(%s) "
					 "AND scale(trunc(%s)) = 0",
					 x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * round(X, s) lands within half an ULP of X at scale s:
 *   abs(round(X, s) - X) <= 0.5 * power(10, -s).
 */
PG_PROP(round_scale_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			s = (int) pg_prop_draw_int(tc, 0, 6);
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x, (int) pg_prop_draw_int(tc, 0, 8));

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(round(%s, %d) - %s) "
					 "<= 0.5 * power(10::numeric, -%d)",
					 x.data, s, x.data, s);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * round(X, s) has scale <= s: it rounds to s fractional digits, and trailing
 * zeros may be dropped so the resulting scale can be smaller.
 */
PG_PROP(round_scale_is_scale)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			s = (int) pg_prop_draw_int(tc, 0, 6);
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x, (int) pg_prop_draw_int(tc, 0, 8));

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT scale(round(%s, %d)) <= %d",
					 x.data, s, s);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * trunc(X, s) toward zero at scale s never increases magnitude and discards
 * strictly less than one unit in the last place:
 *   abs(trunc(X, s)) <= abs(X) and abs(X - trunc(X, s)) < power(10, -s).
 */
PG_PROP(trunc_scale_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			s = (int) pg_prop_draw_int(tc, 0, 6);
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x, (int) pg_prop_draw_int(tc, 0, 8));

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(trunc(%s, %d)) <= abs(%s) "
					 "AND abs(%s - trunc(%s, %d)) < power(10::numeric, -%d)",
					 x.data, s, x.data,
					 x.data, x.data, s, s);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Negation symmetry of floor and ceil: floor(-X) = -ceil(X).  A clean exact
 * identity that any sign-handling bug in the two routines would break.
 */
PG_PROP(floor_ceil_negation_symmetry)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x, (int) pg_prop_draw_int(tc, 0, 8));

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT floor(-(%s)) = -ceil(%s)",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Rounding to scale s is idempotent: round(round(X, s), s) = round(X, s).
 * Once a value is at scale s, rounding again at the same scale changes
 * nothing.
 */
PG_PROP(round_idempotent_at_scale)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			s = (int) pg_prop_draw_int(tc, 0, 6);
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x, (int) pg_prop_draw_int(tc, 0, 8));

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT round(round(%s, %d), %d) = round(%s, %d)",
					 x.data, s, s, x.data, s);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * On an integer-valued numeric (drawn with no fractional part), floor, ceil,
 * round and trunc are all the identity:
 *   floor(N) = N AND ceil(N) = N AND round(N) = N AND trunc(N) = N.
 */
PG_PROP(ceil_floor_integer_fixpoint)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_numeric(tc, &n, 0);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT floor(%s) = %s AND ceil(%s) = %s "
					 "AND round(%s) = %s AND trunc(%s) = %s",
					 n.data, n.data, n.data, n.data,
					 n.data, n.data, n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
