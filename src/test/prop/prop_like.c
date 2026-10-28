/*-------------------------------------------------------------------------
 *
 * prop_like.c
 *	  Property-based tests for LIKE/ILIKE pattern matching.
 *
 * LIKE/ILIKE and the % _ wildcards (src/backend/utils/adt/like_match.c) have
 * had matching and escaping fixes; these check matching against an
 * independent oracle built from the same strings, plus escaping and
 * case-insensitivity invariants.
 *
 * The oracle is independent builtins -- starts_with(), repeat(), length(),
 * lower() -- so the restricted pattern classes we generate (literal prefix
 * plus a single trailing %, a run of _, a wildcard-free pattern) are
 * cross-checked without re-deriving LIKE's own semantics.  Alphabets for the
 * text operand are letters only, so a wildcard never leaks into the text and
 * every % or _ under test comes from a pattern.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_like.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "like"
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
 * Draw 0..10 characters of plain text from a 3-letter alphabet "abc" into buf
 * as a bare string (no quoting).  Letters only -- never % or _ -- so the text
 * cannot accidentally contain a wildcard.
 */
static void
draw_text(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abc";
	int			n = (int) pg_prop_draw_int(tc, 0, 10);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 2)]);
}

/*
 * Draw 0..10 mixed-case characters from "abAB" into buf as a bare string, for
 * exercising case folding (ILIKE vs lower()+LIKE).  Letters only.
 */
static void
draw_mixed_text(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abAB";
	int			n = (int) pg_prop_draw_int(tc, 0, 10);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 3)]);
}

/*
 * Draw 0..8 mixed-case tokens into buf as a bare LIKE pattern: a literal
 * letter from "abAB", a '%', or a '_'.  For the case-insensitivity property.
 */
static void
draw_mixed_pattern(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abAB";
	int			n = (int) pg_prop_draw_int(tc, 0, 8);

	for (int i = 0; i < n; i++)
	{
		int			kind = (int) pg_prop_draw_int(tc, 0, 5);

		if (kind == 4)
			appendStringInfoChar(buf, '%');
		else if (kind == 5)
			appendStringInfoChar(buf, '_');
		else
			appendStringInfoChar(buf, letters[kind]);
	}
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the comparison errored or produced NULL on non-NULL
 * letter strings, which is itself a bug -- so either is a failure, reported
 * with the SQL.  Modelled on prop_numeric.c's require_true.
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
 * The single-% pattern matches every string.  Trivial, but catches gross
 * breakage of the "% matches any run including empty" rule.
 */
PG_PROP(percent_wildcard_matches_all)
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
	appendStringInfoString(&q, "SELECT ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE '%'");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A wildcard-free pattern matches exactly its own text.  Since S has no % or
 * _, "S LIKE S" matches iff S equals S, which it does.
 */
PG_PROP(literal_pattern_matches_itself)
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
	appendStringInfoString(&q, "SELECT ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * S always matches its own prefix followed by '%'.  The prefix length k is
 * drawn in [0, length(S)] and the substring is taken server-side so no C-side
 * substring logic can disagree with the backend's.
 */
PG_PROP(prefix_pattern_consistent)
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

	/* prefix length in [0, len(S)] */
	k = (int) pg_prop_draw_int(tc, 0, (int) strlen(s.data));

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE (substring(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " from 1 for %d) || '%%')", k);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * LIKE of a literal prefix plus a trailing '%' agrees with starts_with().
 * starts_with(text, prefix) is an independent builtin, so this genuinely
 * cross-checks LIKE's prefix semantics rather than re-deriving them.
 */
PG_PROP(like_prefix_matches_oracle)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				l,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&l);
	draw_text(tc, &s);			/* text */
	draw_text(tc, &l);			/* literal prefix (letters only) */

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE (");
	append_sql_literal(&q, l.data);
	appendStringInfoString(&q, " || '%')) = starts_with(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, l.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A pattern of exactly n underscores (and nothing else) matches S iff S has
 * length n.  length() is an independent builtin, so this cross-checks the '_'
 * (match exactly one char) semantics.
 */
PG_PROP(like_underscores_match_length)
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
	draw_text(tc, &s);

	n = (int) pg_prop_draw_int(tc, 0, 12);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " LIKE repeat('_', %d)) = (length(", n);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") = %d)", n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ILIKE is case-insensitive LIKE: "S ILIKE P" equals "lower(S) LIKE lower(P)".
 * Our pattern alphabet is letters + % + _, all case-fold safe (% and _ are
 * unaffected by lower()), so there are no escape subtleties.  Text and
 * pattern are drawn mixed-case to actually exercise the folding.
 */
PG_PROP(ilike_is_case_insensitive_like)
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
	draw_mixed_text(tc, &s);
	draw_mixed_pattern(tc, &p);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " ILIKE ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ") = (lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") LIKE lower(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An escaped wildcard is a literal.  With the default backslash escape and
 * modern standard_conforming_strings (so '100\%' in SQL is a literal
 * backslash followed by a percent), LIKE treats '\%' as a literal '%':
 * '100%' matches '100\%' but '100x' does not.  Fixed literals, no draws.
 */
PG_PROP(escape_literal_percent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT ('100%' LIKE '100\\%') AND NOT ('100x' LIKE '100\\%')");

	pg_prop_disconnect(conn);
}
