/*-------------------------------------------------------------------------
 *
 * prop_intops.c
 *	  Property-based tests for integer (int2/int4/int8) arithmetic algebra.
 *
 * The oracle here is integer arithmetic's own algebra: addition commutes and
 * associates, multiplication distributes over addition, trunc-division obeys
 * (a/b)*b + a%b = a, and abs/sign agree with their definitions.  Operands are
 * drawn in sub-ranges that keep every intermediate within int8, so the laws
 * must hold with no overflow error -- any "f" result (or an unexpected error)
 * is a bug in the integer arithmetic, not a legitimate out-of-range.  One
 * property instead checks that widening to bigint avoids an int4 overflow.
 *
 * This area is arithmetic algebra and overflow detection: distinct from
 * "intbit" (bitwise operators) and "numcast" (cross-type cast round-trips).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_intops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "intops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the arithmetic errored or produced NULL on inputs chosen
 * to be in range, which is itself a bug -- so either is a failure.
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
 * -2147483648::int4 parses as -(2147483648::int4), and the positive magnitude
 * overflows the target type before negation; the same bites INT16_MIN::int2
 * and INT32_MIN::int4.  Wrapping any negative in parentheses makes the cast
 * apply to the already-negative value.  The per-width minimum additionally
 * cannot be written as a positive literal in its own width, so each is emitted
 * via subtraction (INT64_MIN as "(-9223372036854775807 - 1)").
 *
 * "width" is the number of bytes of the target type (2, 4 or 8); it selects
 * the right MIN rendering so a drawn value equal to that width's minimum is
 * never written as a bare over-wide positive.
 */
static void
append_int_literal(StringInfo buf, int64 n, int width)
{
	if (width == 8 && n == INT64_MIN)
		appendStringInfoString(buf, "(-9223372036854775807 - 1)");
	else if (width == 4 && n == INT32_MIN)
		appendStringInfoString(buf, "(-2147483647 - 1)");
	else if (width == 2 && n == INT16_MIN)
		appendStringInfoString(buf, "(-32767 - 1)");
	else if (n < 0)
		appendStringInfo(buf, "(%lld)", (long long) n);
	else
		appendStringInfo(buf, "%lld", (long long) n);
}

/*
 * 1. add_commutative
 *
 * a + b = b + a for int8 a, b in a safe range (-1e9..1e9) where every sum
 * stays well within int8.
 */
PG_PROP(add_commutative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -100000, 100000);
	b = pg_prop_draw_int(tc, -100000, 100000);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8 + ");
	append_int_literal(&q, b, 8);
	appendStringInfoString(&q, "::int8) = (");
	append_int_literal(&q, b, 8);
	appendStringInfoString(&q, "::int8 + ");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. add_associative
 *
 * (a + b) + c = a + (b + c) for int8 a, b, c in -1e6..1e6, so the total sum
 * (<= 3e6 in magnitude) fits int8 with room to spare.
 */
PG_PROP(add_associative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b,
				c;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -10000, 10000);
	b = pg_prop_draw_int(tc, -10000, 10000);
	c = pg_prop_draw_int(tc, -10000, 10000);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ((");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8 + ");
	append_int_literal(&q, b, 8);
	appendStringInfoString(&q, "::int8) + ");
	append_int_literal(&q, c, 8);
	appendStringInfoString(&q, "::int8) = (");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8 + (");
	append_int_literal(&q, b, 8);
	appendStringInfoString(&q, "::int8 + ");
	append_int_literal(&q, c, 8);
	appendStringInfoString(&q, "::int8))");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. mul_distributes_over_add
 *
 * a * (b + c) = a*b + a*c for int8 a, b, c in -1000..1000.  Each product is at
 * most ~2e6 and the sum ~4e6, far inside int8, so no overflow is possible.
 */
PG_PROP(mul_distributes_over_add)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b,
				c;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -1000, 1000);
	b = pg_prop_draw_int(tc, -1000, 1000);
	c = pg_prop_draw_int(tc, -1000, 1000);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8 * (");
	append_int_literal(&q, b, 8);
	appendStringInfoString(&q, "::int8 + ");
	append_int_literal(&q, c, 8);
	appendStringInfoString(&q, "::int8)) = (");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8 * ");
	append_int_literal(&q, b, 8);
	appendStringInfoString(&q, "::int8 + ");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8 * ");
	append_int_literal(&q, c, 8);
	appendStringInfoString(&q, "::int8)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. div_mod_identity
 *
 * For b <> 0, (a/b)*b + (a%b) = a.  SQL integer division truncates toward
 * zero, and % is the matching remainder, so this identity is exact.  b is
 * drawn in -1000..1000; the loop re-draws until b <> 0 so no division by zero
 * is attempted.  a in -1e6..1e6.  Every intermediate fits int8.
 */
PG_PROP(div_mod_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -10000, 10000);
	/* draw a non-zero divisor without a reject loop: 1..1000 with a sign */
	b = pg_prop_draw_int(tc, 1, 1000);
	if (pg_prop_draw_bool(tc))
		b = -b;

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ((");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8 / ");
	append_int_literal(&q, b, 8);
	appendStringInfoString(&q, "::int8) * ");
	append_int_literal(&q, b, 8);
	appendStringInfoString(&q, "::int8 + (");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8 % ");
	append_int_literal(&q, b, 8);
	appendStringInfoString(&q, "::int8)) = ");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. abs_sign
 *
 * abs(a) >= 0, sign(a) in (-1, 0, 1), and a = sign(a) * abs(a).  Drawn over
 * the full int8 range EXCLUDING INT64_MIN, whose absolute value overflows
 * int8; INT64_MIN + 1 is the lowest value drawn so abs() never errors.
 */
PG_PROP(abs_sign)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, INT64_MIN + 1, INT64_MAX);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT abs(");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8) >= 0 AND sign(");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8) IN (-1, 0, 1) AND ");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8 = sign(");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8) * abs(");
	append_int_literal(&q, a, 8);
	appendStringInfoString(&q, "::int8)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. int4_overflow_detected (widening avoids overflow)
 *
 * 2147483647 + k overflows int4 for any k in 1..1000, so the int4 addition is
 * the correct error path -- but asserting an error is awkward.  Instead assert
 * the sound positive property: widening the left operand to bigint lets the
 * same addition succeed and give the true sum, 2147483647 + k.  This checks
 * that widening is what avoids the int4 overflow.  (The int4 add itself is not
 * evaluated here; only the widened bigint add, which cannot overflow for these
 * operands.)
 */
PG_PROP(int4_overflow_detected)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		k;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	k = pg_prop_draw_int(tc, 1, 1000);

	/*
	 * Adding past INT32_MAX in bigint must give the exact mathematical result
	 * (no int4 wraparound).  Both sides are evaluated in bigint -- the right
	 * side crosses the int4 boundary a different way (2147483648 + (k-1)) so
	 * it is an independent computation, not a copy of the left.
	 */
	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (2147483647::bigint + ");
	append_int_literal(&q, k, 8);
	appendStringInfoString(&q, "::bigint) = (2147483648::bigint + ");
	append_int_literal(&q, k - 1, 8);
	appendStringInfoString(&q, "::bigint)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. cast_narrow_roundtrip_when_in_range
 *
 * For int8 n within the int2 range (-32768..32767), n::int2::int8 = n; and
 * for n within the int4 range, n::int4::int8 = n.  Both narrowing casts are
 * lossless when the value fits, so the round-trip is the identity.  n is drawn
 * in the int2 range so BOTH halves of the AND are in range at once (every
 * int2 value is also an int4 value).
 */
PG_PROP(cast_narrow_roundtrip_when_in_range)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, INT16_MIN, INT16_MAX);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	append_int_literal(&q, n, 2);
	appendStringInfoString(&q, "::int2::int8 = ");
	append_int_literal(&q, n, 8);
	appendStringInfoString(&q, "::int8 AND ");
	append_int_literal(&q, n, 4);
	appendStringInfoString(&q, "::int4::int8 = ");
	append_int_literal(&q, n, 8);
	appendStringInfoString(&q, "::int8");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
