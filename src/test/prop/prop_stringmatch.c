/*-------------------------------------------------------------------------
 *
 * prop_stringmatch.c
 *	  Property-based tests for LIKE/ILIKE/SIMILAR TO/~ consistency.
 *
 * These cross-check the pattern-matching operators against *different*
 * oracles than the "like" area does: LIKE prefix/contains are checked against
 * position()/left(), ILIKE against lower()+LIKE, ~ (regex) against position()
 * for literal letters, and SIMILAR TO against plain equality for metachar-free
 * patterns.  All text and all patterns are built from the letters-only
 * alphabet [abc] (or [abcABC] for the case-folding property), so a % or _
 * never leaks into the operands and every operator sees only the literal
 * patterns the property intends.  A mismatch means one operator disagrees
 * with an independent builtin -- a matching bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_stringmatch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "stringmatch"
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
 * Draw a bare string of length [min, max] from a letters-only alphabet.  No %
 * or _ or any LIKE/SIMILAR/regex metacharacter can appear, so the drawn value
 * is always a plain literal both as text and as a pattern.
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
 * 1. like_prefix_eq_leftmatch: a literal prefix P followed by '%' matches S
 * exactly when the first length(P) characters of S equal P.  P is a plain
 * [abc] string (length 0..4) with no wildcards, so "P || '%'" is a pure
 * prefix pattern; left(S, length(P)) is an independent builtin.
 */
PG_PROP(like_prefix_eq_leftmatch)
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
	draw_letters(tc, &p, "abc", 0, 4);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE (");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " || '%')) = (left(");
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
 * 2. like_contains_eq_position: "'%' || P || '%'" matches S exactly when P
 * occurs somewhere in S.  P is a non-empty [abc] string (length 1..3) with no
 * wildcards, so the surrounding %s make a pure substring pattern; position(P
 * in S) > 0 is the independent oracle (position returns 0 when not found).
 */
PG_PROP(like_contains_eq_position)
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
	draw_letters(tc, &p, "abc", 1, 3);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE ('%' || ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " || '%')) = (position(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") > 0)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. ilike_eq_lower_like: ILIKE is case-insensitive LIKE, so "S ILIKE P"
 * equals "lower(S) LIKE lower(P)".  S and P are drawn mixed-case over
 * [abcABC] to actually exercise folding; P is a wildcard-free literal, so no
 * escaping or metachar subtlety is in play.
 */
PG_PROP(ilike_eq_lower_like)
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
	draw_letters(tc, &p, "abcABC", 0, 12);

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
 * 4. like_literal_eq_equals: with no wildcard characters, LIKE degrades to
 * equality, so "S LIKE P" equals "S = P".  P is a plain [abc] string
 * (length 0..12) with no % or _, so there is nothing for LIKE to match
 * loosely.
 */
PG_PROP(like_literal_eq_equals)
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
	draw_letters(tc, &p, "abc", 0, 12);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ") = (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " = ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. regex_literal_eq_contains: for a single literal letter c from [abc]
 * (none of which is a regex metacharacter), the POSIX regex match "S ~ c" is
 * a plain containment test, so it equals "position(c in S) > 0".
 */
PG_PROP(regex_literal_eq_contains)
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
	draw_letters(tc, &c, "abc", 1, 1);	/* exactly one literal letter */

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
 * 6. similar_to_literal: SIMILAR TO with a pattern that has no SIMILAR
 * metacharacters degrades to equality.  P is a plain [abc] string -- none of
 * a, b, c are SIMILAR metachars (% _ | * + ? { } ( ) [ ] \) -- so
 * "S SIMILAR TO P" equals "S = P".
 */
PG_PROP(similar_to_literal)
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
	draw_letters(tc, &p, "abc", 0, 12);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " SIMILAR TO ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ") = (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " = ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. not_like_complement: NOT LIKE is the boolean complement of LIKE, so
 * "S NOT LIKE P" equals "NOT (S LIKE P)" for any pattern.  Here P may contain
 * wildcards (literal letters, %, or _), since the identity holds whatever P
 * is as long as both operands are non-NULL (which letters-only strings
 * always are).
 */
PG_PROP(not_like_complement)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				p,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s, "abc", 0, 12);

	/*
	 * Build a pattern that may contain the wildcards %/_ as well as literal
	 * letters; only build a % or _ where deliberately chosen so no accidental
	 * wildcard leaks in.
	 */
	initStringInfo(&p);
	n = (int) pg_prop_draw_int(tc, 0, 8);
	for (int i = 0; i < n; i++)
	{
		int			kind = (int) pg_prop_draw_int(tc, 0, 4);

		if (kind == 3)
			appendStringInfoChar(&p, '%');
		else if (kind == 4)
			appendStringInfoChar(&p, '_');
		else
			appendStringInfoChar(&p, "abc"[kind]);
	}

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " NOT LIKE ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ") = (NOT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
