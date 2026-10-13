/*-------------------------------------------------------------------------
 *
 * prop_text.c
 *	  Property-based tests for text/string functions.
 *
 * The text/string functions in src/backend/utils/adt/varlena.c have had ~19
 * fixes over the years; these check algebraic identities of substring,
 * length, concat, overlay, replace and split against each other, so the
 * functions cross-check one another rather than re-deriving their semantics.
 *
 * The oracle is independent builtins -- length(), substr(), ||, reverse(),
 * upper()/lower(), replace(), position(), overlay(), split_part() and
 * string_to_array() -- and the identities they must satisfy together.
 * Alphabets are ASCII, single-byte, and quote-free, so length() counts
 * characters, reverse() is exact, and SQL string-literal quoting is trivial
 * (multibyte case folding is unicode_case's job, not this file's).
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_text.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "text"
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
 * Draw 0..16 characters of plain text from a 7-char alphabet "abcABC " into
 * buf as a bare string (no quoting).  Letters (mixed case) and a space only --
 * all single-byte ASCII and never a single quote -- so length() counts
 * characters exactly and quoting is trivial.
 */
static void
draw_text(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] = "abcABC ";
	int			n = (int) pg_prop_draw_int(tc, 0, 16);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 6)]);
}

/*
 * Draw 0..16 ASCII letters from "abcABC" (no space) into buf as a bare
 * string, for the case-folding length property where only letters are
 * interesting.
 */
static void
draw_letters(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] = "abcABC";
	int			n = (int) pg_prop_draw_int(tc, 0, 16);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 5)]);
}

/*
 * Draw 0..16 characters from the 2-letter alphabet "ab" into buf as a bare
 * string, so 'a' occurs often -- for the replace-removes-all property.
 */
static void
draw_ab(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] = "ab";
	int			n = (int) pg_prop_draw_int(tc, 0, 16);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 1)]);
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
 * Concatenation length is additive: length(A || B) = length(A) + length(B).
 * With a single-byte alphabet, length() counts characters, so the character
 * counts of two strings add exactly across concatenation.
 */
PG_PROP(length_of_concat_additive)
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
	draw_text(tc, &a);
	draw_text(tc, &b);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(");
	append_sql_literal(&q, a.data);
	appendStringInfoString(&q, " || ");
	append_sql_literal(&q, b.data);
	appendStringInfoString(&q, ") = length(");
	append_sql_literal(&q, a.data);
	appendStringInfoString(&q, ") + length(");
	append_sql_literal(&q, b.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Splitting and rejoining reconstructs the string:
 *   substr(S, 1, k) || substr(S, k+1) = S
 * for a split point k in [0, length(S)].  k is drawn in [0, 16] and clamped
 * server-side with least(k, length(S)) so the split point is always in range
 * and both halves cover S exactly.
 */
PG_PROP(substr_concat_reconstructs)
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
	draw_text(tc, &s);

	k = (int) pg_prop_draw_int(tc, 0, 16);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT substr(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", 1, least(%d, length(", k);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "))) || substr(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", least(%d, length(", k);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) + 1) = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * reverse() is its own inverse: reverse(reverse(S)) = S.  With a single-byte
 * ASCII-ish alphabet, reverse() is an exact character reversal, so applying
 * it twice yields the original.
 */
PG_PROP(reverse_involution)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT reverse(reverse(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ASCII case change preserves length:
 *   length(upper(S)) = length(S) AND length(lower(S)) = length(S)
 * for an ASCII-letter string S.  upper()/lower() on single-byte ASCII map
 * each character to exactly one character, so neither changes the length.
 */
PG_PROP(upper_lower_length_preserved_ascii)
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
	appendStringInfoString(&q, "SELECT length(upper(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") AND length(lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * replace() removes all occurrences: after replacing every 'a' with the empty
 * string, no 'a' remains, so position('a' in replace(S, 'a', '')) = 0.
 * position() returns 0 when the substring is not found.  S is drawn from "ab"
 * so 'a' occurs often and the property has teeth.
 */
PG_PROP(replace_removes_all_occurrences)
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
	appendStringInfoString(&q, "SELECT position('a' in replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', '')) = 0");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * overlay() matches a manual substring splice:
 *   overlay(S placing R from p for L)
 *     = substr(S, 1, p-1) || R || substr(S, p+L)
 * for a 1-based start p and length L.  p is drawn 0..24 then clamped
 * server-side to [1, length(S)+1] and L to >= 0, consistently on both sides,
 * so overlay() and the manual splice always see the same in-range p/L.  This
 * cross-checks overlay() against substr() and ||.
 */
PG_PROP(overlay_matches_manual)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				r,
				q;
	int			p;
	int			l;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&r);
	draw_text(tc, &s);
	draw_text(tc, &r);

	p = (int) pg_prop_draw_int(tc, 0, 24);
	l = (int) pg_prop_draw_int(tc, 0, 24);

	initStringInfo(&q);

	/* overlay side: clamp p to [1, length(S)+1], L to >= 0 */
	appendStringInfoString(&q, "SELECT overlay(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " placing ");
	append_sql_literal(&q, r.data);
	appendStringInfo(&q, " from least(greatest(%d, 1), length(", p);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") + 1) for greatest(%d, 0)) = ", l);

	/* manual splice side: identical clamps so both agree */
	appendStringInfoString(&q, "substr(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", 1, least(greatest(%d, 1), length(", p);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") + 1) - 1) || ");
	append_sql_literal(&q, r.data);
	appendStringInfoString(&q, " || substr(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", least(greatest(%d, 1), length(", p);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") + 1) + greatest(%d, 0))", l);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(r.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * split_part() round-trips against string_to_array():
 *   split_part(S, ',', i) = (string_to_array(S, ','))[i]
 * for i in [1, ntokens].  S is built by joining 1..4 letter-tokens with ','.
 * string_to_array() is an independent oracle, so this cross-checks
 * split_part()'s i-th-field semantics rather than re-deriving them.
 */
PG_PROP(split_part_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			ntokens;
	int			i;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/*
	 * build S = tok1,tok2,... with ntokens in [1, 4].  Each token must be
	 * NON-EMPTY: split_part() and string_to_array() agree only on non-empty
	 * input -- string_to_array('') is an empty array (so [1] is NULL) while
	 * split_part('',...) is the empty string, a documented difference.
	 */
	ntokens = (int) pg_prop_draw_int(tc, 1, 4);

	initStringInfo(&s);
	for (int t = 0; t < ntokens; t++)
	{
		static const char letters[] = "abcABC";
		int			len = (int) pg_prop_draw_int(tc, 1, 6);

		if (t > 0)
			appendStringInfoChar(&s, ',');
		for (int c = 0; c < len; c++)
			appendStringInfoChar(&s, letters[pg_prop_draw_int(tc, 0, 5)]);
	}

	/* pick which 1-based token to compare */
	i = (int) pg_prop_draw_int(tc, 1, ntokens);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT split_part(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", ',', %d) = (string_to_array(", i);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", ','))[%d]", i);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
