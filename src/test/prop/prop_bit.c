/*-------------------------------------------------------------------------
 *
 * prop_bit.c
 *	  Property-based tests for the bit and bit varying types.
 *
 * These exercise bit-string operations (concatenation, shifts, bitwise ops,
 * length) implemented in src/backend/utils/adt/varbit.c, checking
 * boolean-algebra and length identities over generated bit strings.
 *
 * The oracle is the bit-string algebra itself: concatenation length is
 * additive, De Morgan's laws hold for the bitwise operators, XOR of a value
 * with itself is the all-zeros string, shifts preserve length, the all-ones
 * mask is the AND identity and the all-zeros mask its annihilator, text I/O
 * round-trips, and get_bit observes what set_bit wrote.  Any "f" result is a
 * bug in varbit.c, not a representation artefact.
 *
 * The bitwise operators (~, &, |, #) require equal operand lengths, so the
 * properties that use them draw a single length L and then draw bodies of
 * exactly that length.  All-ones and all-zeros masks are derived from the
 * drawn value X itself (via ~(X & ~X) and X & ~X) so they match X's length
 * automatically rather than hardcoding a literal of a particular width.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_bit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "bit"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a bit-varying literal of a drawn length n in 1..40 into buf, emitted as
 * B'<bits>' where each bit is '0' or '1'.  Zero-length bit strings are legal
 * but awkward to use in the identities below, so lengths start at 1; the
 * engine favours the boundary, so length-1 strings get plenty of coverage.
 */
static void
draw_bits(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 1, 40);

	appendStringInfoString(buf, "B'");
	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 1)));
	appendStringInfoChar(buf, '\'');
}

/*
 * Draw a bit-varying literal of a SPECIFIC length len into buf.  Used by the
 * properties whose operators (~, &, |, #) require equal-length operands: the
 * caller draws L once with draw_bits_len_choice() and then draws each body of
 * exactly that length.
 */
static void
draw_bits_len(pg_prop_tc *tc, StringInfo buf, int len)
{
	appendStringInfoString(buf, "B'");
	for (int i = 0; i < len; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 1)));
	appendStringInfoChar(buf, '\'');
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operation errored or produced NULL on well-formed bit
 * strings, which is itself a bug -- so either is a failure, reported with the
 * SQL.
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
 * Concatenation length is additive: length(X || Y) = length(X) + length(Y).
 * length() of a bit string is its bit count, and || appends the bits of Y
 * after those of X, so no bits are gained or lost.
 */
PG_PROP(length_of_concat_additive)
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
	draw_bits(tc, &x);
	draw_bits(tc, &y);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(%s || %s) = length(%s) + length(%s)",
					 x.data, y.data, x.data, y.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * De Morgan's law for bit strings: NOT (X & Y) = (NOT X) | (NOT Y).  The
 * bitwise operators require equal-length operands, so draw a single length L
 * and then draw X and Y of exactly that length.
 */
PG_PROP(and_or_not_demorgan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	StringInfoData x,
				y,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	len = (int) pg_prop_draw_int(tc, 1, 40);

	initStringInfo(&x);
	initStringInfo(&y);
	draw_bits_len(tc, &x, len);
	draw_bits_len(tc, &y, len);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (~ (%s & %s)) = ((~ %s) | (~ %s))",
					 x.data, y.data, x.data, y.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(y.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * XOR of a value with itself is the all-zeros string of the same length:
 * X # X = all zeros.  The all-zeros mask is derived from X itself as X & ~X
 * so it matches X's length without building a separate literal.
 */
PG_PROP(xor_self_is_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_bits(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s # %s) = (%s & ~ %s)",
					 x.data, x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Shifts preserve length: for a shift s in 0..L, length(X << s) = length(X)
 * and length(X >> s) = length(X).  Bit-string shifts keep the width, filling
 * vacated positions with zeros rather than growing or shrinking the string.
 */
PG_PROP(shift_left_right_length_preserved)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	int			s;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	len = (int) pg_prop_draw_int(tc, 1, 40);
	s = (int) pg_prop_draw_int(tc, 0, len);

	initStringInfo(&x);
	draw_bits_len(tc, &x, len);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(%s << %d) = length(%s) "
					 "AND length(%s >> %d) = length(%s)",
					 x.data, s, x.data, x.data, s, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * AND identity and annihilator: X & (all ones) = X, and X & (all zeros) =
 * all zeros.  The masks are derived from X itself -- all zeros is X & ~X and
 * all ones is ~(X & ~X) -- so they automatically match X's length instead of
 * being hardcoded literals of a particular width.
 */
PG_PROP(and_identity_and_annihilator)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_bits(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s & (~ (%s & ~ %s))) = %s "
					 "AND (%s & (%s & ~ %s)) = (%s & ~ %s)",
					 x.data, x.data, x.data, x.data,
					 x.data, x.data, x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Text I/O round-trips: X::text::bit varying = X.  The bit-string output
 * function renders the value as '0'/'1' characters, and the input function
 * parses that back to the same value, so casting out to text and back in is
 * the identity.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_bits(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT %s::text::bit varying = %s",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * get_bit observes what set_bit wrote: for a position p in 0..L-1,
 * get_bit(set_bit(X, p, 1), p) = 1 and get_bit(set_bit(X, p, 0), p) = 0.
 * get_bit/set_bit are 0-indexed; this checks that a written bit reads back as
 * written regardless of X's prior content at p.
 */
PG_PROP(get_bit_set_bit_consistent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	int			p;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	len = (int) pg_prop_draw_int(tc, 1, 40);
	p = (int) pg_prop_draw_int(tc, 0, len - 1);

	initStringInfo(&x);
	draw_bits_len(tc, &x, len);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT get_bit(set_bit(%s, %d, 1), %d) = 1 "
					 "AND get_bit(set_bit(%s, %d, 0), %d) = 0",
					 x.data, p, p, x.data, p, p);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
