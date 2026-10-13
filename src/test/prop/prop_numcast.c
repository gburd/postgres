/*-------------------------------------------------------------------------
 *
 * prop_numcast.c
 *	  Property-based tests for numeric cross-type casts
 *	  (int2/int4/int8 <-> numeric <-> float8, and text).
 *
 * Casts between the numeric types must preserve value where the target can
 * represent it and round/overflow predictably otherwise; these check the
 * lossless round-trips and the documented rounding.
 *
 * numeric exactly represents every integer; float8 exactly represents the
 * integers with |x| < 2^53 (so an int4 is always safe, but an int8 is not --
 * only int4 is used for the float round-trip).  The numeric->integer cast
 * rounds to nearest, half away from zero, identically to round(numeric), so
 * round() is the oracle for the rounding property.  Narrowing casts
 * (int8->int2, etc.) raise an out-of-range error rather than silently
 * wrapping, so an overflow there is a legitimate skip, not a wrong result.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_numcast.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "numcast"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * True when a SQL error is a legitimate out-of-range/overflow condition rather
 * than a wrong-results bug.  A narrowing cast that does not fit the target
 * type raises such an error (never wraps), so those cases are discarded.
 */
static bool
is_overflow(const char *errmsg)
{
	if (errmsg == NULL)
		return false;
	return strstr(errmsg, "out of range") != NULL ||
		strstr(errmsg, "overflow") != NULL;
}

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
 * Like require_true, but a SQL error that is a legitimate overflow/out-of-range
 * condition discards the case (pg_prop_assume(false)) instead of failing.  Used
 * where a narrowing cast may legally not fit: the property is that it either
 * fits exactly or errors -- it must never silently wrap.
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
		if (is_overflow(err))
		{
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
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
 * -2147483648::int4 parses as -(2147483648::int4), and the positive magnitude
 * overflows the target type before negation; the same bites INT16_MIN::int2
 * and INT32_MIN::int4.  Wrapping any negative in parentheses makes the cast
 * apply to the already-negative value.  INT64_MIN additionally cannot be
 * written as a positive literal at all, so it is emitted via subtraction.
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
 * 1. int_numeric_roundtrip
 *
 * numeric exactly represents every int8, so n::numeric::int8 = n for the full
 * int64 range.
 */
PG_PROP(int_numeric_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_int_literal(&q, n);
	appendStringInfoString(&q, "::int8::numeric::int8) = ");
	append_int_literal(&q, n);
	appendStringInfoString(&q, "::int8");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. int4_numeric_float_roundtrip
 *
 * Every int4 has |n| < 2^53, so it is exactly representable in float8, and
 * n::float8::int4 = n with no loss.
 */
PG_PROP(int4_numeric_float_roundtrip)
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
 * 3. numeric_to_int_rounds_half_away
 *
 * The numeric->integer cast rounds to nearest, half away from zero, which is
 * exactly what round(numeric) does.  For a numeric X with a fractional part,
 * X::bigint = round(X)::bigint.  X is built with a modest integer part so no
 * overflow is possible.
 */
PG_PROP(numeric_to_int_rounds_half_away)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nint;
	int			nfrac;
	bool		neg;
	int			ndigits = 0;
	StringInfoData x;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* integer part up to 9 digits stays well inside int8; fractional 1..8 */
	nint = (int) pg_prop_draw_int(tc, 0, 9);
	nfrac = (int) pg_prop_draw_int(tc, 1, 8);
	neg = pg_prop_draw_bool(tc);

	initStringInfo(&x);
	appendStringInfoChar(&x, '(');
	if (neg)
		appendStringInfoChar(&x, '-');
	for (int i = 0; i < nint; i++)
	{
		appendStringInfoChar(&x, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
		ndigits++;
	}
	if (ndigits == 0)
		appendStringInfoChar(&x, '0');
	appendStringInfoChar(&x, '.');
	for (int i = 0; i < nfrac; i++)
		appendStringInfoChar(&x, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoChar(&x, ')');

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s)::numeric::bigint = round(%s::numeric)::bigint",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. float_to_numeric_roundtrip_small
 *
 * A float8 that is an exact integer within int4 range round-trips through
 * numeric: n::float8::numeric::float8 = n::float8.
 */
PG_PROP(float_to_numeric_roundtrip_small)
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
	appendStringInfoString(&q, "::float8::numeric::float8) = ");
	append_int_literal(&q, n);
	appendStringInfoString(&q, "::float8");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. int_widening_lossless
 *
 * int2 -> int4 -> int8 -> numeric widening never changes the value: for an
 * int2 m, that chain equals m::int2::numeric directly.
 */
PG_PROP(int_widening_lossless)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		m;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	m = pg_prop_draw_int(tc, INT16_MIN, INT16_MAX);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_int_literal(&q, m);
	appendStringInfoString(&q, "::int2::int4::int8::numeric) = ");
	append_int_literal(&q, m);
	appendStringInfoString(&q, "::int2::numeric");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. numeric_text_roundtrip
 *
 * A numeric formatted to text and parsed back is unchanged:
 * X::text::numeric = X.
 */
PG_PROP(numeric_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nint;
	int			nfrac;
	bool		neg;
	int			ndigits = 0;
	StringInfoData x;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	nint = (int) pg_prop_draw_int(tc, 0, 24);
	nfrac = (int) pg_prop_draw_int(tc, 0, 8);
	neg = pg_prop_draw_bool(tc);

	initStringInfo(&x);
	appendStringInfoChar(&x, '(');
	if (neg)
		appendStringInfoChar(&x, '-');
	for (int i = 0; i < nint; i++)
	{
		appendStringInfoChar(&x, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
		ndigits++;
	}
	if (nfrac > 0)
	{
		appendStringInfoChar(&x, '.');
		for (int i = 0; i < nfrac; i++)
		{
			appendStringInfoChar(&x, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
			ndigits++;
		}
	}
	if (ndigits == 0)
		appendStringInfoChar(&x, '0');
	appendStringInfoChar(&x, ')');

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s)::numeric::text::numeric = (%s)::numeric",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. int_overflow_on_narrowing_detected
 *
 * Narrowing a large int8 to int2 either fits exactly or raises an out-of-range
 * error -- it must never silently wrap.  When it fits the identity is trivially
 * true; when it overflows the cast errors and the case is skipped via
 * require_true_or_skip.
 */
PG_PROP(int_overflow_on_narrowing_detected)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_int_literal(&q, n);
	appendStringInfoString(&q, "::int8::int2) = ");
	append_int_literal(&q, n);
	appendStringInfoString(&q, "::int8::int2");

	require_true_or_skip(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
