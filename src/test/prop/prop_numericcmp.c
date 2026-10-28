/*-------------------------------------------------------------------------
 *
 * prop_numericcmp.c
 *	  Property-based tests for NUMERIC comparison, ordering and special values.
 *
 * Distinct from prop_numeric (arithmetic identities): this file pins down the
 * comparison semantics of the numeric type -- the total order over finite
 * values, the special rule that NaN sorts as the largest value and compares
 * equal to itself, scale-insensitive equality (0 = -0 = 0.00), and that
 * numeric-vs-int comparison agrees with int-vs-int.  A broken numeric_cmp,
 * least/greatest, abs or sign shows up as an "f" here.
 *
 * The oracle is the type's own documented comparison contract: these are
 * exact boolean facts true for every finite (and, where stated, NaN) input,
 * so any "f" or NULL result is a bug, not a rounding artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_numericcmp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "numericcmp"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a finite decimal NUMERIC literal as the text  '<int>.<frac>'::numeric
 * -- the sign lives inside the single-quoted string, so there is no leading-
 * minus misparse and no bare-literal overflow (the value is parsed by
 * numeric_in from text, never as an integer literal).  Scales straddle the
 * usual boundaries but stay far below the numeric limit, so no overflow.
 */
static void
draw_numeric(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 0, 12);
	int			nfrac = (int) pg_prop_draw_int(tc, 0, 6);
	bool		neg = pg_prop_draw_bool(tc);
	int			ndigits = 0;

	appendStringInfoChar(buf, '\'');
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

	appendStringInfoString(buf, "'::numeric");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a comparison errored or produced NULL on inputs where the
 * contract is total, which is itself a bug -- so either is a failure.
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
 * NaN sorts largest: 'NaN'::numeric > x for every finite x.  Numeric NaN is
 * defined to order above all finite values (mirroring the b-tree opclass used
 * for sorts and indexes), so this strict inequality must hold for any drawn
 * finite x.
 */
PG_PROP(numeric_nan_largest)
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
	appendStringInfo(&q, "SELECT 'NaN'::numeric > %s", x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * NaN equals NaN: 'NaN'::numeric = 'NaN'::numeric is TRUE.  Unlike float, the
 * numeric type defines NaN = NaN so NaN has a well-defined place in the total
 * order; this must be true (a constant check, but it guards that rule).
 */
PG_PROP(numeric_nan_eq_nan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn, "SELECT 'NaN'::numeric = 'NaN'::numeric");

	pg_prop_disconnect(conn);
}

/*
 * Total order (trichotomy): for finite A, B exactly one of A < B, A = B,
 * A > B holds.  Summing the three booleans as ints must equal 1 for any pair.
 */
PG_PROP(numeric_total_order)
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
	draw_numeric(tc, &a);
	draw_numeric(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s < %s)::int + (%s = %s)::int "
					 "+ (%s > %s)::int = 1",
					 a.data, b.data, a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * int-vs-numeric agrees with int-vs-int: for int n and numeric x,
 * (n::numeric < x) = (n < x).  The second comparison promotes n to numeric
 * via the implicit cast; both must agree, so the numeric comparison operator
 * selected for a mixed int/numeric comparison is consistent with the pure
 * numeric one.  n is drawn across the full int4 range; the negative literal
 * is cast as (%d) so INT_MIN does not misparse.
 */
PG_PROP(numeric_int_consistent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, -2147483648, 2147483647);
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%d)::numeric < %s) = ((%d)::int4 < %s)",
					 n, x.data, n, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * LEAST/GREATEST bound every argument: LEAST(a,b,c) <= each of a,b,c and
 * each <= GREATEST(a,b,c).  Holds for any finite numerics (no NULLs drawn),
 * so LEAST/GREATEST must straddle the inputs exactly.
 */
PG_PROP(greatest_least_numeric)
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
	draw_numeric(tc, &a);
	draw_numeric(tc, &b);
	draw_numeric(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT LEAST(%s, %s, %s) <= %s "
					 "AND LEAST(%s, %s, %s) <= %s "
					 "AND LEAST(%s, %s, %s) <= %s "
					 "AND %s <= GREATEST(%s, %s, %s) "
					 "AND %s <= GREATEST(%s, %s, %s) "
					 "AND %s <= GREATEST(%s, %s, %s)",
					 a.data, b.data, c.data, a.data,
					 a.data, b.data, c.data, b.data,
					 a.data, b.data, c.data, c.data,
					 a.data, a.data, b.data, c.data,
					 b.data, a.data, b.data, c.data,
					 c.data, a.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * abs/sign contract: abs(x) >= 0 and x = sign(x) * abs(x) for any finite x.
 * sign() returns -1, 0 or 1 for numeric; the product reconstructs x exactly
 * (abs and sign are exact), so this must hold with no tolerance.
 */
PG_PROP(abs_sign_numeric)
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
					 "SELECT abs(%s) >= 0 AND %s = sign(%s) * abs(%s)",
					 x.data, x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Zero and scale do not affect equality: 0 = -0 and 0.00 = 0.  Numeric
 * equality ignores both the sign of zero and the display scale, so these
 * constant facts must be TRUE (guards against a scale- or sign-sensitive
 * comparison path).
 */
PG_PROP(numeric_zero_signs)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT '0'::numeric = '-0'::numeric "
				 "AND '0.00'::numeric = '0'::numeric");

	pg_prop_disconnect(conn);
}

/*
 * least(a,b) <= greatest(a,b) for a drawn pair of finite numerics.  A weaker,
 * two-argument companion to greatest_least_numeric that still catches an
 * inverted comparison inside the min/max machinery.
 */
PG_PROP(min_max_order)
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
	draw_numeric(tc, &a);
	draw_numeric(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT least(%s, %s) <= greatest(%s, %s)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
