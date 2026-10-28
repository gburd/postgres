/*-------------------------------------------------------------------------
 *
 * prop_bitstringops.c
 *	  Property-based tests for bit(n)/varbit string operations.
 *
 * Exercises the bit-string operators distinct from the "bit" area: string
 * concatenation (||), left/right shift (<<, >>), bitwise AND/OR/XOR of
 * equal-length operands, length(), position(), get_bit() and set_bit().  The
 * oracle is the algebra these operations must satisfy -- De Morgan over an
 * equal-length AND/OR/NOT, XOR-with-self is all zeros, set_bit then get_bit
 * reads back the written bit, and both concatenation and shifting have exact
 * length laws -- so any "f" result is a bug in varbit.c, not a tolerance.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_bitstringops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "bitstringops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a bit string literal B'<0/1 chars>' of length 1..16, drawing each
 * character independently as 0 or 1.  Returns the chosen length so callers
 * that need a matching length (equal-length bitwise ops) can draw patterns of
 * the same width.
 */
static int
draw_bitstring_len(pg_prop_tc *tc, StringInfo buf, int len)
{
	int			i;

	appendStringInfoString(buf, "B'");
	for (i = 0; i < len; i++)
		appendStringInfoChar(buf, pg_prop_draw_bool(tc) ? '1' : '0');
	appendStringInfoChar(buf, '\'');
	return len;
}

/*
 * Draw a bit string of a length chosen here (1..16).  Returns the length.
 */
static int
draw_bitstring(pg_prop_tc *tc, StringInfo buf)
{
	int			len = (int) pg_prop_draw_int(tc, 1, 16);

	return draw_bitstring_len(tc, buf, len);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operation errored or produced NULL on valid inputs,
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
 * Concatenation length law: length(a || b) = length(a) + length(b) for two
 * varbit values of independently drawn lengths.
 */
PG_PROP(concat_length)
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
	draw_bitstring(tc, &a);
	draw_bitstring(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(%s::varbit || %s::varbit) "
					 "= length(%s::varbit) + length(%s::varbit)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * De Morgan over equal-length bit(n): ~(a & b) = (~a) | (~b).  One length n
 * is drawn once, then two independent patterns of that length, so the AND/OR
 * and NOT all operate on operands of identical width.
 */
PG_PROP(and_or_demorgan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 1, 16);
	initStringInfo(&a);
	initStringInfo(&b);
	draw_bitstring_len(tc, &a, n);
	draw_bitstring_len(tc, &b, n);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ~(%s::bit(%d) & %s::bit(%d)) "
					 "= ((~ %s::bit(%d)) | (~ %s::bit(%d)))",
					 a.data, n, b.data, n,
					 a.data, n, b.data, n);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * XOR with self is all zeros: a # a has no 1 bits, so searching for a '1' in
 * its text rendering yields position 0 (not found).  This is sound for any
 * drawn a of any length.
 */
PG_PROP(xor_self_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bitstring(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT position('1' in (%s::varbit # %s::varbit)::text) = 0",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * set_bit/get_bit round-trip: after set_bit(a, k, 1) the bit at position k
 * reads back 1, for a drawn k in 0..length(a)-1.  The length is drawn here so
 * k can be bounded correctly.
 */
PG_PROP(setbit_getbit)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	int			k;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 1, 16);
	k = (int) pg_prop_draw_int(tc, 0, n - 1);
	initStringInfo(&a);
	draw_bitstring_len(tc, &a, n);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT get_bit(set_bit(%s::varbit, %d, 1), %d) = 1",
					 a.data, k, k);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Shifting preserves length: for a bit string a, length(a << 1) = length(a)
 * and length(a >> 1) = length(a).  Bit-string shift keeps the width and drops
 * or zero-fills, so the length is invariant.
 */
PG_PROP(shift_length_preserved)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bitstring(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(%s::varbit << 1) = length(%s::varbit) "
					 "AND length(%s::varbit >> 1) = length(%s::varbit)",
					 a.data, a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
