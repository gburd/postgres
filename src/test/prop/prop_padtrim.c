/*-------------------------------------------------------------------------
 *
 * prop_padtrim.c
 *	  Property-based tests for lpad/rpad and btrim/ltrim/rtrim/trim.
 *
 * These check the padding and trimming laws against independent oracles --
 * length(), left(), right() and construction of a known-trimmable string --
 * rather than re-deriving the functions' semantics.  The alphabet is [abc]
 * (single-byte, quote-free, and crucially excluding the pad char 'x' and the
 * space), so length() counts characters, a padded or trimmed region is
 * exactly the 'x'/space run we added, and a failure is a bug in the function.
 *
 * Distinct from the strfunc/textfns/textpos areas: here the laws are the
 * exact-length guarantee of pad, the suffix/prefix a pad preserves, the exact
 * recovery btrim gives on a known-trimmable string, the ltrim/rtrim
 * composition, trim's default (space) stripping, and pad's right-truncation.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_padtrim.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "padtrim"
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
 * Draw 0..10 characters from "abc" into buf as a bare string.  Letters only
 * -- all single-byte, never a quote, never the pad char 'x', never a space --
 * so length() counts characters and the pad/trim semantics are exact.
 */
static void
draw_letters(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] = "abc";
	int			n = (int) pg_prop_draw_int(tc, 0, 10);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 2)]);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the expression errored or produced NULL on well-formed
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
 * lpad() produces a result of exactly the requested length:
 *   length(lpad(S, n, 'x')) = n
 * for a target n in [0, 20].  lpad pads on the left when S is shorter than n
 * and truncates to n when S is longer, so the result length is n regardless
 * of length(S).
 */
PG_PROP(lpad_length)
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

	n = (int) pg_prop_draw_int(tc, 0, 20);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(lpad(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", %d, 'x')) = %d", n, n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * rpad() produces a result of exactly the requested length:
 *   length(rpad(S, n, 'x')) = n
 * for a target n in [0, 20].  Symmetric to lpad_length: rpad pads on the
 * right or truncates, so the result length is n regardless of length(S).
 */
PG_PROP(rpad_length)
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

	n = (int) pg_prop_draw_int(tc, 0, 20);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(rpad(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", %d, 'x')) = %d", n, n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Left-padding preserves the original as the suffix:
 *   right(lpad(S, length(S) + e, 'x'), length(S)) = S
 * for extra padding e in [0, 10], i.e. the target length is >= length(S).
 * The target is built in SQL as length('<S>') + e so the call and oracle
 * agree on length(S); right() is the independent oracle for "the last
 * length(S) characters", which after left-padding is exactly S.
 */
PG_PROP(lpad_preserves_suffix)
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

	e = (int) pg_prop_draw_int(tc, 0, 10);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT right(lpad(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", length(");
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
 * Right-padding preserves the original as the prefix:
 *   left(rpad(S, length(S) + e, 'x'), length(S)) = S
 * for extra padding e in [0, 10].  Symmetric to lpad_preserves_suffix: after
 * right-padding, the first length(S) characters are exactly S.  left() is the
 * independent oracle.
 */
PG_PROP(rpad_preserves_prefix)
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

	e = (int) pg_prop_draw_int(tc, 0, 10);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT left(rpad(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", length(");
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
 * btrim removes a known 'x' run from both ends exactly:
 *   btrim('xx' || S || 'xx', 'x') = S
 * S is drawn from [abc] so it contains no 'x'; the only 'x' characters are the
 * two we prepend and append.  btrim(_, 'x') must strip exactly those and leave
 * S, so the recovered value equals the original.  (An all-trim result on S
 * cannot over-trim since S has no 'x'.)
 */
PG_PROP(btrim_removes_both_ends)
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
	appendStringInfoString(&q, "SELECT btrim('xx' || ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || 'xx', 'x') = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * btrim is the composition of the two one-sided trims:
 *   btrim(S2, 'x') = ltrim(rtrim(S2, 'x'), 'x')
 * where S2 = 'xx' || S || 'xx' with S from [abc], so both edges carry 'x'
 * runs for the trims to bite on.  Trimming both ends at once must equal
 * trimming the right then the left; this cross-checks btrim against ltrim and
 * rtrim.
 */
PG_PROP(ltrim_rtrim_compose)
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
	appendStringInfoString(&q, "SELECT btrim('xx' || ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || 'xx', 'x') = ltrim(rtrim('xx' || ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || 'xx', 'x'), 'x')");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * trim() with no explicit character set strips leading/trailing spaces:
 *   trim('  ' || S || '  ') = S
 * S is drawn from [abc] so it contains no space; the only spaces are the two
 * we prepend and append.  Default trim must remove exactly those, recovering
 * S.  (S cannot be over-trimmed since it has no space.)
 */
PG_PROP(trim_default_spaces)
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
	appendStringInfoString(&q, "SELECT trim('  ' || ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || '  ') = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * When the target length is shorter than S, lpad truncates from the right,
 * keeping the first n characters:
 *   lpad(S, n, 'x') = left(S, n)
 * for n in [0, length(S)].  n is drawn in SQL as a fraction of length('<S>')
 * via a drawn offset so the call and oracle see the same length; left(S, n)
 * is the independent oracle for "the first n characters".  At n = length(S)
 * both sides are S, so the boundary is sound; below it, this exercises pad's
 * right-truncation.
 */
PG_PROP(lpad_truncates)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			slen;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	/* n in [0, length(S)]; strlen is the character length for [abc]. */
	slen = (int) strlen(s.data);
	n = (int) pg_prop_draw_int(tc, 0, slen);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT lpad(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", %d, 'x') = left(", n);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", %d)", n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
