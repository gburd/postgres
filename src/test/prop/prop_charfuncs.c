/*-------------------------------------------------------------------------
 *
 * prop_charfuncs.c
 *	  Property-based tests for single-character SQL functions: chr(), ascii(),
 *	  and the one-character behaviour of length/octet_length/upper/lower/initcap.
 *
 * The oracle is the ASCII algebra of these functions: over the printable
 * ASCII range 33..126, chr() and ascii() are mutual inverses, a single ASCII
 * codepoint is exactly one character and one byte, upper/lower shift the ASCII
 * letter case by 32, and ASCII codepoint order is byte/collation order.  Each
 * property asserts an identity that must hold for every drawn codepoint; any
 * "f" result is a bug in these functions, not a rounding or locale artefact
 * (all inputs are single-byte ASCII, below any multibyte boundary).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_charfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "charfuncs"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on a valid ASCII
 * codepoint, which is itself a bug -- so either is a failure, reported with
 * the SQL.  Copied from prop_numeric.c's require_true.
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
 * ascii(chr(n)) = n for every printable ASCII codepoint n in 33..126.
 * chr()/ascii() must be mutual inverses on the single-byte ASCII range.
 */
PG_PROP(chr_ascii_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 33, 126);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ascii(chr(%d)) = %d", n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * length(chr(n)) = 1 for n in 33..126.  A single ASCII codepoint is exactly
 * one character.
 */
PG_PROP(chr_length_one)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 33, 126);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT length(chr(%d)) = 1", n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ascii(S) = ascii(left(S,1)) for a non-empty string S: ascii() reads only
 * the first character.  S is built from drawn printable ASCII codepoints so it
 * carries no quoting hazard, and is always at least one character long.
 */
PG_PROP(ascii_first_char)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	int			i;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* length 1..8; every char a printable ASCII codepoint 33..126 */
	len = (int) pg_prop_draw_int(tc, 1, 8);
	initStringInfo(&s);
	for (i = 0; i < len; i++)
	{
		int			n = (int) pg_prop_draw_int(tc, 33, 126);

		/* double single quotes to keep the SQL literal safe */
		if (n == '\'')
			appendStringInfoChar(&s, '\'');
		appendStringInfoChar(&s, (char) n);
	}

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ascii('%s') = ascii(left('%s', 1))",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For a lowercase letter codepoint n in 97..122, ascii(upper(chr(n))) = n - 32.
 * upper() shifts an ASCII lowercase letter to its uppercase counterpart, which
 * is exactly 32 codepoints lower.
 */
PG_PROP(upper_lower_ascii)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 97, 122);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ascii(upper(chr(%d))) = %d", n, n - 32);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * chr(a) || chr(b) is a two-character string whose first character's ascii()
 * is a.  Concatenating two single ASCII codepoints yields length 2 and
 * preserves the leading codepoint.
 */
PG_PROP(chr_concat)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, 33, 126);
	b = (int) pg_prop_draw_int(tc, 33, 126);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(chr(%d) || chr(%d)) = 2 "
					 "AND ascii(left(chr(%d) || chr(%d), 1)) = %d",
					 a, b, a, b, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * initcap() uppercases the first letter of a word: for a lowercase letter
 * codepoint n in 97..122, left(initcap(chr(n) || 'bc'), 1) = upper(chr(n)).
 * The literal suffix 'bc' is letters-only so no quoting hazard arises.
 */
PG_PROP(initcap_first_upper)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 97, 122);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT left(initcap(chr(%d) || 'bc'), 1) = upper(chr(%d))",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ASCII codepoint order is string order: for n1 < n2 in 33..126,
 * chr(n1) < chr(n2).  Draw n1 and n2, then compare the smaller codepoint's
 * character against the larger's; assume n1 != n2 is not needed because we
 * order the two draws and skip the equal case via a strict comparison.
 */
PG_PROP(ascii_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			x,
				y,
				n1,
				n2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x = (int) pg_prop_draw_int(tc, 33, 126);
	y = (int) pg_prop_draw_int(tc, 33, 126);
	/* equal codepoints make no strict ordering claim; discard that case */
	pg_prop_assume(tc, x != y);
	n1 = (x < y) ? x : y;
	n2 = (x < y) ? y : x;

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT chr(%d) < chr(%d)", n1, n2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * octet_length(chr(n)) = 1 for n in 33..126: every printable ASCII codepoint
 * encodes to a single byte in a UTF-8 (or any ASCII-superset) database.
 */
PG_PROP(octet_length_ascii)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 33, 126);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT octet_length(chr(%d)) = 1", n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
