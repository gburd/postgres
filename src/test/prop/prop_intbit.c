/*-------------------------------------------------------------------------
 *
 * prop_intbit.c
 *	  Property-based tests for integer bitwise operators and shifts.
 *
 * Integer bitwise operators and shifts have well-defined algebraic laws;
 * these check them on int4 and int8 across the full value range.
 *
 * The operators under test are the integer bitwise AND (&), OR (|), XOR (#),
 * NOT (~) and the arithmetic shifts (<<, >>) on int4 and int8.  Unlike
 * floating point, these are exact: De Morgan's laws, XOR self-inverse,
 * absorption, double complement and the shift/multiply correspondence all
 * hold with no tolerance, so any "f" result is a bug, not rounding.
 *
 * The pure-algebra properties (De Morgan, XOR, absorption, NOT, identity/
 * annihilator) draw the FULL int4/int8 range -- boundary values such as
 * INT32_MIN, INT64_MIN, 0 and -1 are exactly where two's-complement bit
 * tricks go wrong, so they are the point.  Only the shift properties bound
 * their inputs, so a << s cannot overflow int8 and turn a legitimate
 * algebraic identity into an overflow error.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_intbit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "intbit"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the bitwise op errored or produced NULL on finite inputs,
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
 * De Morgan over bitwise ops on int4: ~(a & b) = (~a) | (~b).  Drawn across
 * the full int4 range so INT32_MIN and -1 (all-ones) are exercised.
 */
PG_PROP(and_or_demorgan_int4)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	int			b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);
	b = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (~(((%d)::int4) & ((%d)::int4))) "
					 "= ((~((%d)::int4)) | (~((%d)::int4)))",
					 a, b, a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * XOR is its own inverse: a # a = 0.  Drawn across the full int4 range.
 */
PG_PROP(xor_self_zero_int4)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%d)::int4) # ((%d)::int4)) = 0::int4",
					 a, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * XOR by the same value twice is identity: (a # b) # b = a.  Drawn across the
 * full int8 range so INT64_MIN is exercised.
 */
PG_PROP(xor_twice_identity_int8)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	a;
	long long	b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (long long) pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);
	b = (long long) pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((((%lld)::int8) # ((%lld)::int8)) # ((%lld)::int8)) "
					 "= ((%lld)::int8)",
					 a, b, b, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Absorption laws on int4: a & (a | b) = a and a | (a & b) = a.  Drawn across
 * the full int4 range.
 */
PG_PROP(and_or_absorption_int4)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	int			b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);
	b = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%d)::int4) & (((%d)::int4) | ((%d)::int4))) "
					 "= ((%d)::int4) "
					 "AND (((%d)::int4) | (((%d)::int4) & ((%d)::int4))) "
					 "= ((%d)::int4)",
					 a, a, b, a,
					 a, a, b, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Double complement is identity: ~(~a) = a.  Drawn across the full int8
 * range.
 */
PG_PROP(not_involution_int8)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	a;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (long long) pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (~(~((%lld)::int8))) = ((%lld)::int8)",
					 a, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Left shift is multiply by a power of two: a << s = a * 2^s.  The 2^s
 * multiplier is built in C as (1LL << s) and both a (0..1000000) and s (0..10)
 * are bounded so a * (1 << s) stays well within int8 -- no overflow, so the
 * identity holds with no error.
 */
PG_PROP(shift_left_is_multiply_small)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	a;
	int			s;
	long long	m;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (long long) pg_prop_draw_int(tc, 0, 1000000);
	s = (int) pg_prop_draw_int(tc, 0, 10);
	m = 1LL << s;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%lld)::int8) << %d) "
					 "= (((%lld)::int8) * ((%lld)::int8))",
					 a, s, a, m);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Left then right shift by the same amount restores a: (a << s) >> s = a, when
 * no bits are lost to overflow.  Bounding a to -1000000..1000000 and s to
 * 0..10 guarantees a << s does not overflow int8, so the floor-division of the
 * right shift exactly undoes the left shift.
 */
PG_PROP(shift_right_left_floor_int8)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	a;
	int			s;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (long long) pg_prop_draw_int(tc, -1000000, 1000000);
	s = (int) pg_prop_draw_int(tc, 0, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((((%lld)::int8) << %d) >> %d) = ((%lld)::int8)",
					 a, s, s, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Identity and annihilator for AND on int4: a & -1 = a (since -1 is all-ones
 * in two's complement) and a & 0 = 0.  Drawn across the full int4 range.
 */
PG_PROP(and_identity_annihilator_int4)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%d)::int4) & ((-1)::int4)) = ((%d)::int4) "
					 "AND (((%d)::int4) & (0::int4)) = (0::int4)",
					 a, a, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
