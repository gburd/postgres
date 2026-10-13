/*-------------------------------------------------------------------------
 *
 * prop_likepattern.c
 *	  Property-based tests for LIKE wildcard semantics (_ % ESCAPE).
 *
 * These isolate the structural meaning of LIKE's wildcards -- '_' matches
 * exactly one character, '%' matches any run including empty, and ESCAPE
 * turns a following metacharacter into a literal -- and cross-check each
 * against an independent builtin (length(), position(), left()).  This is a
 * different cut than the "like"/"stringmatch"/"regexmatch" areas: it is about
 * the wildcard algebra itself (how many chars a pattern consumes), not about
 * agreement between operators.  A mismatch means LIKE miscounts a wildcard --
 * a matching bug in like_match.c.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_likepattern.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "likepattern"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our alphabet never contains a quote, but do it defensively
 * so the generated SQL is always well-formed.
 */
static void
append_sql_literal(StringInfo buf, const char *s)
{
	const char *p;

	appendStringInfoChar(buf, '\'');
	for (p = s; *p != '\0'; p++)
	{
		if (*p == '\'')
			appendStringInfoChar(buf, '\'');
		appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * Draw 0..10 characters of plain text from the 3-letter alphabet "abc" into
 * buf as a bare string (no quoting).  Letters only -- never %, _, or \ -- so
 * the text can never contain a LIKE metacharacter, and every wildcard under
 * test comes from the pattern we build, not the operand.
 */
static void
draw_text(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abc";
	int			n = (int) pg_prop_draw_int(tc, 0, 10);
	int			i;

	for (i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 2)]);
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
 * A single '_' matches S iff S has length exactly 1.  length() is an
 * independent builtin, so this pins the "_ consumes exactly one character"
 * rule at its smallest instance.
 */
PG_PROP(underscore_matches_one)
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
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE '_') = (length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = 1)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The single-'%' pattern matches every string, the empty string included.
 * Pins "% matches any run including empty" at its base case.
 */
PG_PROP(percent_matches_all)
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
 * '%' || c || '%' matches S iff the single letter c occurs somewhere in S.
 * position(c in S) > 0 is an independent builtin, so this cross-checks the
 * "contains" idiom (a run before, a run after) against substring search.
 */
PG_PROP(percent_x_percent_contains)
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
	draw_text(tc, &s);

	c = "abc"[pg_prop_draw_int(tc, 0, 2)];

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " LIKE ('%%' || '%c' || '%%')) = (position('%c' in ",
					 c, c);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") > 0)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A pattern of exactly n underscores matches S iff S has length n.  Scales
 * the single-'_' rule to a run via repeat('_', n); length() is the oracle.
 */
PG_PROP(underscores_length)
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
 * A wildcard-free prefix P followed by '%' matches S iff S's first length(P)
 * characters equal P.  left(S, length(P)) = P is an independent builtin, so
 * this cross-checks LIKE's prefix handling.  P is drawn letters-only, so it
 * contains no wildcard and the only metacharacter is the trailing '%'.
 */
PG_PROP(prefix_percent)
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
	draw_text(tc, &s);
	draw_text(tc, &p);

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
 * An escaped '%' is a literal '%'.  With ESCAPE '\', the pattern '\%' matches
 * only the one-character string "%".  Our text S is drawn over [abc], so S is
 * never "%": the left side (S LIKE '\%' ESCAPE '\') is always false, and so
 * is the right side (S = '%'); they therefore agree.  This pins that ESCAPE
 * strips the wildcard meaning of the following '%' rather than matching an
 * arbitrary run.
 */
PG_PROP(escape_literal_percent)
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
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE '\\%' ESCAPE '\\') = (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " = '%')");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The empty pattern matches only the empty string: '' LIKE '' is true, and
 * for any S, (S LIKE '') equals (S = '').  Pins the base case where the
 * pattern consumes nothing.
 */
PG_PROP(empty_pattern_empty_string)
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
	appendStringInfoString(&q, "SELECT ('' LIKE '') AND ((");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE '') = (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " = ''))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * S always matches any of its own prefixes followed by '%'.  The prefix
 * length k is drawn in [0, length(S)] and the prefix itself is taken
 * server-side with left(), so no C-side substring logic can disagree with the
 * backend's.  Since S starts with its own first k characters, the match must
 * always succeed.
 */
PG_PROP(like_concat_split)
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

	k = (int) pg_prop_draw_int(tc, 0, (int) strlen(s.data));

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " LIKE (left(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", %d) || '%%')", k);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
