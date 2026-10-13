/*-------------------------------------------------------------------------
 *
 * prop_substring.c
 *	  Property-based tests for substring extraction and position functions.
 *
 * Covers the substring/position builtins not exercised by the "text" area
 * (prop_text.c) or the "strfunc" area (prop_strfunc.c): the SQL-standard
 * substring(S FROM m FOR L) and substring(S FROM 1) forms, the POSIX
 * substring(S FROM pattern) extraction, left()/right() edge cases
 * (including negative lengths) and starts_with().  The "text" area owns
 * substr()-split/overlay/split_part and the "strfunc" area owns
 * lpad/rpad/btrim/translate/chr/ascii/initcap/repeat/strpos, so none of
 * those are repeated here.
 *
 * The oracle is independent builtins -- left(), right(), substring() with
 * FROM/FOR, length(), position() and starts_with() -- and the identities
 * they must satisfy together, so the functions cross-check one another
 * rather than re-deriving their semantics.  Alphabets are ASCII single-byte
 * letters "abc", so length() counts characters exactly, SQL string-literal
 * quoting is trivial, and a single-letter POSIX pattern is a plain literal
 * regex (no metacharacters), making the regex-extraction oracle exact.
 * Indices are clamped server-side with least()/greatest() so the manual
 * oracles always see the same in-range index and no negative-length surprise
 * can arise; left() with a negative length is its own property.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_substring.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "substring"
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
 * Draw 0..14 ASCII letters from "abc" into buf as a bare string.  Letters
 * only -- all single-byte and never a single quote -- so length() counts
 * characters exactly, quoting is trivial, and a single letter used as a POSIX
 * pattern is a plain literal (no regex metacharacters).
 */
static void
draw_letters(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] = "abc";
	int			n = (int) pg_prop_draw_int(tc, 0, 14);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 2)]);
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
 * left() and right() reconstruct the whole string:
 *   left(S, k) || right(S, length(S) - k) = S
 * for a split point k in [0, length(S)].  left(S, k) is the first k
 * characters and right(S, m) the last m, so the two halves cover S exactly
 * and concatenate back to it.  k is drawn 0..16 and clamped server-side with
 * least(k, length(S)) so the split point is always in range (and the second
 * argument length(S) - k is never negative, which for right() would mean
 * something else entirely).
 */
PG_PROP(left_right_concat_reconstructs)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			k;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	k = (int) pg_prop_draw_int(tc, 0, 16);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT left(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", least(%d, length(", k);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "))) || right(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", length(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") - least(%d, length(", k);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "))) = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A negative length to left() drops characters from the end:
 *   left(S, -n) = substring(S from 1 for greatest(length(S) - n, 0))
 * for n in [0, length(S) + 2].  left(S, -n) returns all but the last n
 * characters (and the empty string once n reaches length(S)); the oracle
 * takes the first length(S) - n characters, clamped to >= 0 so both sides
 * agree on the over-trimmed case.  n is drawn 0..16 which comfortably covers
 * [0, length(S) + 2] since length(S) <= 14.
 */
PG_PROP(left_negative_drops_from_end)
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

	/*
	 * n must be >= 1: left(s, -0) is left(s, 0) = '' (SQL -0 == 0), which does
	 * not match the "drop the last n characters" oracle.  Only a genuinely
	 * negative length triggers the drop-from-end behaviour.
	 */
	n = (int) pg_prop_draw_int(tc, 1, 16);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT left(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", -%d) = substring(", n);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " from 1 for greatest(length(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") - %d, 0))", n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The length of a SQL-standard substring never exceeds the requested length
 * nor the available tail:
 *   length(substring(S from m for L)) <= L
 *   AND length(substring(S from m for L)) <= greatest(length(S) - m + 1, 0)
 * for a 1-based start m in [1, length(S) + 1] and length L in [0, length(S)].
 * This is a sound two-sided upper bound: substring(S from m for L) yields at
 * most L characters and at most the length(S) - m + 1 characters available
 * from m to the end (0 when m is past the end).  m is drawn 1..16 and clamped
 * server-side to [1, length(S) + 1]; L is drawn 0..16.
 */
PG_PROP(substring_from_for_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			m;
	int			l;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	m = (int) pg_prop_draw_int(tc, 1, 16);
	l = (int) pg_prop_draw_int(tc, 0, 16);

	initStringInfo(&q);

	/* extracted length <= requested length L */
	appendStringInfoString(&q, "SELECT length(substring(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " from least(greatest(%d, 1), length(", m);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") + 1) for %d)) <= %d", l, l);

	/* extracted length <= available tail from m */
	appendStringInfoString(&q, " AND length(substring(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " from least(greatest(%d, 1), length(", m);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") + 1) for %d)) <= greatest(length(", l);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") - least(greatest(%d, 1), length(", m);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") + 1) + 1, 0)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * substring from position 1 with no length is the whole string:
 *   substring(S from 1) = S
 * The SQL-standard substring with only a start of 1 and no FOR clause returns
 * everything from the first character onward, i.e. S itself.
 */
PG_PROP(substring_from_identity)
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
	appendStringInfoString(&q, "SELECT substring(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " from 1) = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * starts_with() agrees with a prefix comparison via left():
 *   starts_with(S, P) = (left(S, length(P)) = P)
 * left(S, length(P)) is the first length(P) characters of S, so S starts with
 * P exactly when that prefix equals P.  Both S and the short prefix P are
 * drawn from "abc" so P is a genuine candidate prefix often enough to exercise
 * both the true and false branches.  left() and length() are independent
 * oracles for starts_with().
 */
PG_PROP(starts_with_matches_left)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				p,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&p);
	draw_letters(tc, &s);
	draw_letters(tc, &p);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT starts_with(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ") = (left(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", length(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ")) = ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The POSIX substring(S from pattern) form extracts the matched text:
 *   when position(c in S) > 0, substring(S from c) = c;
 *   otherwise substring(S from c) IS NULL.
 * The pattern c is a single letter from "abc", which is a plain literal regex
 * (no metacharacters), so it matches exactly where that character occurs.
 * position() is the independent oracle for whether c occurs at all; when it
 * does the extraction returns that one character, and when it does not the
 * POSIX form returns NULL.
 */
PG_PROP(substring_regex_extracts_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	char		c;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	c = "abc"[pg_prop_draw_int(tc, 0, 2)];

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT CASE WHEN position('");
	appendStringInfoChar(&q, c);
	appendStringInfoString(&q, "' in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") > 0 THEN substring(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " from '");
	appendStringInfoChar(&q, c);
	appendStringInfoString(&q, "') = '");
	appendStringInfoChar(&q, c);
	appendStringInfoString(&q, "' ELSE substring(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " from '");
	appendStringInfoChar(&q, c);
	appendStringInfoString(&q, "') IS NULL END");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * SQL-standard substring FROM/FOR splits and rejoins to reconstruct S:
 *   substring(S from 1 for k) || substring(S from k + 1) = S
 * for a split point k in [0, length(S)].  substring(S from 1 for k) is the
 * first k characters and substring(S from k + 1) the rest, so they concatenate
 * back to S.  k is drawn 0..16 and clamped server-side with least(k,
 * length(S)) so the split point is always in range and both halves cover S
 * exactly.  This exercises the SQL-standard substring FROM/FOR form, distinct
 * from the substr() in the "text" area.
 */
PG_PROP(substring_concat_pieces)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			k;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	k = (int) pg_prop_draw_int(tc, 0, 16);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT substring(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " from 1 for least(%d, length(", k);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "))) || substring(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " from least(%d, length(", k);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) + 1) = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
