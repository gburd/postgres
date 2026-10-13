/*-------------------------------------------------------------------------
 *
 * prop_strfunc.c
 *	  Property-based tests for assorted text/string functions.
 *
 * Covers string builtins not exercised by the "text" area (prop_text.c):
 * lpad, rpad, btrim/ltrim/rtrim, translate, chr/ascii, initcap, repeat,
 * left/right and strpos.  These check algebraic identities of each function
 * against independent oracles -- left(), right(), substr(), position() and
 * replace() -- so the functions cross-check one another rather than
 * re-deriving their semantics.
 *
 * Alphabets are ASCII, single-byte and quote-free, so length() counts
 * characters, the padding/trim/translate semantics are exact, and SQL
 * string-literal quoting is trivial.  Where a target length depends on the
 * operand's own length, it is built as a SQL expression (length('<S>') + e)
 * so the call and its oracle always see the same length.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_strfunc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "strfunc"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our alphabets never contain a quote, but do it defensively
 * so the generated SQL is always well-formed.
 */
static void
append_sql_literal(StringInfo buf, const char *s)
{
	appendStringInfoChar(buf, '\'');
	for (const char *p = s; *p != '\0'; p++)
	{
		if (*p == '\'')
			appendStringInfoChar(buf, '\'');
		appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * Draw 0..12 ASCII letters from "abc" into buf as a bare string.  Letters
 * only -- all single-byte and never a single quote -- so length() counts
 * characters exactly and quoting is trivial.
 */
static void
draw_letters(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] = "abc";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 2)]);
}

/*
 * Draw 0..12 characters from "ab" into buf as a bare string, so 'a' occurs
 * often at the edges -- for the btrim/ltrim/rtrim properties where trimming
 * 'a' needs to have teeth.
 */
static void
draw_ab(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] = "ab";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 1)]);
}

/*
 * Draw 0..12 characters from "abc " (letters plus space) into buf as a bare
 * string, so word boundaries occur -- for the initcap property.
 */
static void
draw_words(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] = "abc ";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 3)]);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the expression errored or produced NULL on well-formed
 * letter strings (usually an index slipped out of range), which is itself a
 * bug -- so either is a failure, reported with the SQL.  Modelled on
 * prop_numeric.c's require_true.
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
 * lpad()/rpad() produce a result of exactly the requested length:
 *   length(lpad(S, L, 'x')) = L AND length(rpad(S, L, 'x')) = L
 * for a target length L in [0, 20].  Both pad on their respective side when S
 * is shorter than L and truncate to L when S is longer, so the result length
 * is L regardless of length(S).
 */
PG_PROP(lpad_rpad_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			l;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	l = (int) pg_prop_draw_int(tc, 0, 20);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(lpad(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", %d, 'x')) = %d AND length(rpad(", l, l);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", %d, 'x')) = %d", l, l);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * When padding on the right (L >= length(S)), rpad leaves S as a prefix:
 *   left(rpad(S, length(S) + e, 'x'), length(S)) = S
 * for extra padding e in [0, 8].  The target length length(S) + e is built in
 * SQL so the call and the oracle agree on length(S); left() is the
 * independent oracle for "the first length(S) characters".
 */
PG_PROP(rpad_prefix_preserved_when_padding)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			e;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	e = (int) pg_prop_draw_int(tc, 0, 8);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT left(rpad(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", length(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") + %d, 'x'), length(", e);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * btrim() removes 'a' from both edges:
 *   left(btrim(S, 'a'), 1) <> 'a' AND right(btrim(S, 'a'), 1) <> 'a'
 * S is drawn from "ab" so leading/trailing 'a' runs occur often.  An empty
 * result makes left()/right() return '' which is <> 'a', so the all-'a' case
 * (btrim collapses to '') is naturally satisfied.
 */
PG_PROP(btrim_removes_edge_chars)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_ab(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT left(btrim(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a'), 1) <> 'a' AND right(btrim(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a'), 1) <> 'a'");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * btrim is the composition of one-sided trims:
 *   btrim(S, 'a') = ltrim(rtrim(S, 'a'), 'a')
 * S is drawn from "ab" so both edges exercise 'a' runs.  This cross-checks
 * btrim against ltrim and rtrim: trimming both ends at once must equal
 * trimming the right then the left.
 */
PG_PROP(ltrim_rtrim_compose_btrim)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_ab(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT btrim(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a') = ltrim(rtrim(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a'), 'a')");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * translate() with single-character from/to is a character map equivalent to
 * replace():
 *   translate(S, 'a', 'b') = replace(S, 'a', 'b')
 * replace() is the independent oracle: for a single-char from/to the two
 * agree exactly (every 'a' becomes 'b', everything else is left alone).  S is
 * drawn from "abc" so 'a' occurs and the map does real work.
 */
PG_PROP(translate_is_char_map)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT translate(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', 'b') = replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', 'b')");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * chr() and ascii() round-trip on printable ASCII:
 *   ascii(chr(p)) = p
 * for a drawn code point p in [32, 126].  chr() builds the one-character
 * string for the code point and ascii() reads it back, so composing them is
 * the identity.
 */
PG_PROP(chr_ascii_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			p;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	p = (int) pg_prop_draw_int(tc, 32, 126);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ascii(chr(%d)) = %d", p, p);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * repeat() multiplies length:
 *   length(repeat(S, n)) = length(S) * n
 * for a count n in [0, 10].  With a single-byte alphabet length() counts
 * characters, so repeating S n times gives exactly n copies' worth of
 * characters (0 for n = 0).
 */
PG_PROP(repeat_length_multiplies)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	n = (int) pg_prop_draw_int(tc, 0, 10);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(repeat(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", %d)) = length(", n);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") * %d", n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * initcap() is idempotent on its own output:
 *   initcap(initcap(S)) = initcap(S)
 * initcap uppercases the first letter of each word and lowercases the rest;
 * applying it again finds the same word boundaries and the same already-cased
 * letters, so the result is unchanged.  S is drawn from "abc " (letters plus
 * space) so word boundaries actually occur.
 */
PG_PROP(initcap_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_words(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT initcap(initcap(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = initcap(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * strpos() is consistent with substr()/position():
 *   when strpos(S, c) = 0 the char is absent (position(c in S) = 0);
 *   otherwise substr(S, strpos(S, c), 1) = c -- strpos points at an actual
 *   occurrence of c.
 * strpos() returns the 1-based index of the first match or 0 if absent; this
 * cross-checks it against substr() (the character it claims to point at) and
 * position() (the absence case).  c is fixed 'a' and S is drawn from "abc" so
 * both branches are exercised.
 */
PG_PROP(strpos_consistent_with_substring)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT CASE WHEN strpos(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a') = 0 THEN position('a' in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = 0 ELSE substr(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", strpos(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a'), 1) = 'a' END");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
