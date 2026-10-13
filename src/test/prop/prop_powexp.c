/*-------------------------------------------------------------------------
 *
 * prop_powexp.c
 *	  Property-based tests for power, exponential, logarithm and factorial.
 *
 * The oracle here is the algebra of these functions themselves: exponent
 * addition (b^(e1+e2) = b^e1 * b^e2), the exp/ln and log10/pow10 inverse
 * pairs, the factorial recurrence (n! = n*(n-1)!), and monotonicity of pow
 * and ln.  Integer power and factorial are exact so those identities use no
 * tolerance; the transcendental ones (sqrt, exp/ln, log) are checked within
 * a relative-plus-absolute tolerance.  A broken identity is a bug in the
 * numeric power/exp/ln/log/factorial implementation, not a rounding
 * artefact, because the bases/exponents are kept small enough that no
 * overflow or scale loss can occur (b<=5, e<=6, factorial n<=12).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_powexp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "powexp"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on finite inputs,
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
 * Exponent addition: b^(e1+e2) = b^e1 * b^e2 for integer base and exponents.
 * Numeric integer power is exact at these small magnitudes (b<=5, e<=5, so
 * the largest value is 5^10 ~= 9.8e6), so this holds with no tolerance.
 */
PG_PROP(pow_add_exponents)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			b,
				e1,
				e2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	b = (int) pg_prop_draw_int(tc, 2, 5);
	e1 = (int) pg_prop_draw_int(tc, 0, 5);
	e2 = (int) pg_prop_draw_int(tc, 0, 5);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%d::numeric ^ %d) = (%d::numeric ^ %d) * (%d::numeric ^ %d)",
					 b, e1 + e2, b, e1, b, e2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Identity exponents: b^0 = 1 and b^1 = b for every base.  Exact.
 */
PG_PROP(pow_zero_one)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	b = (int) pg_prop_draw_int(tc, 2, 9);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%d::numeric ^ 0) = 1 AND (%d::numeric ^ 1) = %d",
					 b, b, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Half power equals square root: x^0.5 == sqrt(x) within tolerance.  Both are
 * computed by numeric and rounded to a scale, so compare with a relative-
 * plus-absolute tolerance scaled by (x+1).
 */
PG_PROP(sqrt_pow_half)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			x;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x = (int) pg_prop_draw_int(tc, 1, 100);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs((%d::numeric ^ 0.5) - sqrt(%d::numeric)) "
					 "< 1e-6::numeric * (%d + 1)",
					 x, x, x);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * exp/ln inverse: exp(ln(x)) == x within tolerance.  ln and exp are rounded
 * to a scale, so compare with a relative-plus-absolute tolerance scaled by
 * (x+1).  x is kept in 1..20 so exp(ln(x)) stays well within range.
 */
PG_PROP(exp_ln_inverse)
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
					 "SELECT abs(exp(ln(%d::numeric)) - %d) < 1e-6::numeric * (%d + 1)",
					 x, x, x);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * log10 of a power of ten: log(10^e) == e.  log(numeric) is base 10; 10^e for
 * e<=6 is at most 1e6, exactly representable, so the result is e exactly.
 */
PG_PROP(log10_pow10)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			e;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	e = (int) pg_prop_draw_int(tc, 0, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT log(10::numeric ^ %d) = %d",
					 e, e);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Factorial recurrence: factorial(n) = n * factorial(n-1) for 1<=n<=12.  12!
 * is 479001600, inside int range; factorial(int) returns numeric and is
 * exact, so this holds with no tolerance.
 */
PG_PROP(factorial_recursive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 1, 12);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT factorial(%d) = %d * factorial(%d)",
					 n, n, n - 1);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Power is strictly monotone in the exponent: for e1 < e2 and base >= 2,
 * b^e1 < b^e2.  Exact integer power, so no tolerance.  e1 is drawn in 0..5
 * and e2 strictly above it up to 6.
 */
PG_PROP(pow_monotone)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			b,
				e1,
				e2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	b = (int) pg_prop_draw_int(tc, 2, 5);
	e1 = (int) pg_prop_draw_int(tc, 0, 5);
	e2 = (int) pg_prop_draw_int(tc, e1 + 1, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%d::numeric ^ %d) < (%d::numeric ^ %d)",
					 b, e1, b, e2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ln is strictly monotone: for x1 < x2, ln(x1) < ln(x2).  Both are rounded to
 * the same scale, and for integers 1..100 the gap between successive ln values
 * (>= ln(100)-ln(99) ~= 0.01) far exceeds the rounding of the final digit, so
 * the strict inequality is sound.  x1 is drawn in 1..99 and x2 strictly above.
 */
PG_PROP(ln_monotone)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			x1,
				x2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x1 = (int) pg_prop_draw_int(tc, 1, 99);
	x2 = (int) pg_prop_draw_int(tc, x1 + 1, 100);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ln(%d::numeric) < ln(%d::numeric)",
					 x1, x2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
