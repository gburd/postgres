/*-------------------------------------------------------------------------
 *
 * prop_bitint.c
 *	  Property-based tests for bit-string <-> integer casts and identities.
 *
 * This area targets the int4 <-> bit(32) cast path and the bit-level
 * identities that must hold on the bit string produced from an integer: the
 * cast is a two's-complement reinterpretation, so a round trip must recover
 * the original int4 across the full range (including INT32_MIN and -1 where
 * sign handling goes wrong), length is always the declared width, and the
 * boolean algebra (AND/OR/NOT) plus the shift/multiply correspondence and a
 * popcount cross-check all hold exactly.  Any "f" is a cast or bit-op bug.
 *
 * Distinct from prop_bit (bit-string ops in isolation), prop_bitstringops
 * (set/get/shift on literal bit strings) and prop_intbit (integer bitwise
 * operators): here the subject is specifically the cast between int and
 * bit(N) and the equalities that bridge the two representations.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_bitint.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "bitint"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the cast errored or produced NULL on a finite integer,
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
 * Round trip: n::bit(32)::int = n for any int4 n across the full range.  The
 * int->bit cast lays down the two's-complement bits and the bit->int cast
 * reads them back, so the composition is the identity; INT32_MIN and -1 are
 * exactly where sign handling can go wrong, so the full range is drawn.  The
 * literal is substituted as "(%d)" so a negative never misparses.
 */
PG_PROP(int_to_bit_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%d)::int4::bit(32)::int4 = (%d)::int4",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Length is the declared width: length(n::bit(32)) = 32 for any int4 n.  The
 * cast must always yield exactly 32 bits regardless of the integer's value.
 */
PG_PROP(bit_length_32)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT length((%d)::int4::bit(32)) = 32", n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Idempotence of AND with self: (b & b) = b where b = n::bit(32).  Bitwise
 * AND of a value with itself is the value, so this must hold for the bits of
 * any integer.
 */
PG_PROP(and_with_self)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%d)::int4::bit(32) & (%d)::int4::bit(32)) "
					 "= (%d)::int4::bit(32)",
					 n, n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * OR with all-zero identity: (b | 0::int::bit(32)) = b.  The bit string of
 * integer 0 is the 32-bit all-zero string, which is the identity element for
 * bitwise OR, so OR-ing it with any value leaves the value unchanged.
 */
PG_PROP(or_with_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%d)::int4::bit(32) | (0)::int4::bit(32)) "
					 "= (%d)::int4::bit(32)",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Shift/multiply correspondence: (n::bit(32) << k)::int = (n * 2^k)::int for
 * small non-negative n and k, with the range guarded so n * 2^k stays below
 * 2^31 and no bit shifts out of the top (which would break the equality) and
 * no int4 overflow occurs.  n is 0..1000 and k is 0..10, and the case is only
 * asserted when n * 2^k < 2^31; otherwise the property trivially passes so a
 * guarded-out draw is not a failure.
 */
PG_PROP(shift_eq_mul)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	int			k;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 1000);
	k = (int) pg_prop_draw_int(tc, 0, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN (%d)::int8 * (2^%d)::int8 < 2147483648 "
					 "THEN ((%d)::int4::bit(32) << %d)::int4 "
					 "= ((%d) * (2^%d)::int4)::int4 "
					 "ELSE true END",
					 n, k, n, k, n, k);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Double complement is the identity: ~(~b) = b where b = n::bit(32).  Two
 * applications of bitwise NOT flip every bit back, recovering the original
 * bit string for any integer.
 */
PG_PROP(not_not_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ~(~((%d)::int4::bit(32))) = (%d)::int4::bit(32)",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Popcount cross-check: the number of 1 bits counted two independent ways
 * must agree.  The textual way is length(text) - length(text with 1s
 * removed); the per-bit way is sum(get_bit(b, i)) over i in 0..31.  Both count
 * the set bits of n::bit(32), so they must be equal for every integer -- a
 * disagreement means get_bit or the bit->text rendering is inconsistent with
 * the stored bits.
 */
PG_PROP(popcount_consistency)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT sum(get_bit((%d)::int4::bit(32), i)) "
					 "FROM generate_series(0, 31) i) "
					 "= length((%d)::int4::bit(32)::text) "
					 "- length(replace((%d)::int4::bit(32)::text, '1', ''))",
					 n, n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concatenation width: two 16-bit strings concatenate to exactly 32 bits.
 * length(a::bit(16) || b::bit(16)) = 32 for any two int4 values a, b, since
 * each half is a fixed 16-bit cast and || sums the widths.
 */
PG_PROP(bit_concat_int)
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
					 "SELECT length((%d)::int4::bit(16) "
					 "|| (%d)::int4::bit(16)) = 32",
					 a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
