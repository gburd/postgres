/*-------------------------------------------------------------------------
 *
 * prop_regexmatch.c
 *	  Property-based tests for POSIX regex match operators and regexp_matches.
 *
 * The POSIX regex match operators (~, ~*, !~, !~*) and regexp_matches() are
 * checked against independent builtins (lower/left/right/position) and
 * against each other, using single-letter patterns and the simple anchors ^,
 * $, . only.  All text and all literal patterns come from a letters-only
 * alphabet, so no regex metacharacter leaks in except the ones a property
 * deliberately builds; a mismatch means a match operator or regexp_matches
 * disagrees with an independent oracle -- a regex-matching bug.  Distinct from
 * the regexp/regexfns/stringmatch areas, which check different oracles.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_regexmatch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "regexmatch"
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
 * Draw a bare string of length [min, max] from a letters-only alphabet.  No
 * regex metacharacter can appear, so the drawn value is always plain literal
 * text (and, when used as a pattern, a literal pattern).
 */
static void
draw_letters(pg_prop_tc *tc, StringInfo buf, const char *alphabet,
			 int min, int max)
{
	int			span = (int) strlen(alphabet) - 1;
	int			n = (int) pg_prop_draw_int(tc, min, max);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, span)]);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the match errored or produced NULL on non-NULL letter
 * strings, which is itself a bug -- so either is a failure, reported with the
 * SQL.  Modelled on prop_numeric.c's require_true.
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
 * 1. match_case_insensitive: the case-insensitive match "S ~* P" equals the
 * case-sensitive match of the folded operands, "lower(S) ~ lower(P)".  S is
 * drawn mixed-case over [a-cA-C] to exercise folding; P is a single literal
 * letter (also mixed-case), which is not a regex metacharacter, so both sides
 * perform a plain case-folded containment.
 */
PG_PROP(match_case_insensitive)
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
	draw_letters(tc, &s, "abcABC", 0, 12);
	draw_letters(tc, &p, "abcABC", 1, 1);	/* one literal letter */

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " ~* ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ") = (lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") ~ lower(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. not_match_complement: "S !~ P" is the boolean complement of "S ~ P", so
 * it equals "NOT (S ~ P)" for any S and P (both non-NULL letter strings).  P
 * is a single letter here, but the identity holds for any pattern; a mismatch
 * means the negated operator disagrees with its own base operator.
 */
PG_PROP(not_match_complement)
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
	draw_letters(tc, &s, "abc", 0, 12);
	draw_letters(tc, &p, "abc", 1, 1);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " !~ ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ") = (NOT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " ~ ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. anchored_start: a start anchor followed by one literal letter c, pattern
 * '^c', matches S exactly when S begins with c, i.e. left(S, 1) = c.  c is a
 * single [abc] letter (no metachar), so '^' || c is a pure "starts-with-c"
 * pattern; left(S, 1) is the independent oracle (and equals '' for empty S,
 * never matching c).
 */
PG_PROP(anchored_start)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&c);
	draw_letters(tc, &s, "abc", 0, 12);
	draw_letters(tc, &c, "abc", 1, 1);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " ~ ('^' || ");
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, ")) = (left(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 1) = ");
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. anchored_end: a single literal letter c followed by an end anchor,
 * pattern 'c$', matches S exactly when S ends with c, i.e. right(S, 1) = c.
 * Guarded to non-empty S: for empty S, right(S,1) = '' can never equal c, and
 * 'c$' never matches, so the identity still holds -- but we keep the explicit
 * guard so the oracle is obviously sound even on the empty case.
 */
PG_PROP(anchored_end)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&c);
	draw_letters(tc, &s, "abc", 1, 12);		/* non-empty S */
	draw_letters(tc, &c, "abc", 1, 1);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT CASE WHEN length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = 0 THEN true ELSE (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " ~ (");
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, " || '$')) = (right(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 1) = ");
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, ") END");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. dot_matches_any_single: for a non-empty S, the pattern '.' (dot matches
 * any single character) always finds a match, so "S ~ '.'" is true.  S is
 * drawn non-empty over [abc]; '.' is the only metachar here and it is built
 * directly, so there is nothing else to interpret.
 */
PG_PROP(dot_matches_any_single)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s, "abc", 1, 12);		/* non-empty S */

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " ~ '.')");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. empty_pattern_matches: the empty regex matches every string, so
 * "S ~ ''" is true for any S (including empty S).  S is drawn over [abc],
 * length 0..12.
 */
PG_PROP(empty_pattern_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s, "abc", 0, 12);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " ~ '')");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. literal_contains: for a single literal letter c (not a regex metachar),
 * the match "S ~ c" is a plain containment test, so it equals
 * "position(c in S) > 0" (position returns 0 when c is absent).
 */
PG_PROP(literal_contains)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&c);
	draw_letters(tc, &s, "abc", 0, 12);
	draw_letters(tc, &c, "abc", 1, 1);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " ~ ");
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, ") = (position(");
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, " in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") > 0)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 8. matches_returns_group: regexp_matches(S, '(a)') returns the matched
 * group text when 'a' occurs in S.  Fixing c = 'a' keeps the pattern a plain
 * single-group literal.  When position('a' in S) > 0 the captured group must
 * be exactly 'a'; when 'a' is absent, regexp_matches yields no row (the
 * scalar subquery is NULL) and the CASE short-circuits to true, so the oracle
 * is sound on both branches.
 */
PG_PROP(matches_returns_group)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s, "abc", 0, 12);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT CASE WHEN position('a' in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") > 0 THEN (SELECT (regexp_matches(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", '(a)'))[1]) = 'a' ELSE true END");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
