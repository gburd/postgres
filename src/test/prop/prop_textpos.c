/*-------------------------------------------------------------------------
 *
 * prop_textpos.c
 *	  Property-based tests for text position/split/replace functions not
 *	  covered by text/strfunc/substring/textfns: position/strpos, split_part,
 *	  replace, translate, overlay, and repeat.
 *
 * The oracle is the SQL algebra these functions must satisfy: a found
 * position is a valid 1-based index and a not-found one is 0; replace that
 * deletes a character leaves none behind, and replace of an absent needle is
 * the identity; split_part on an absent delimiter yields the whole string as
 * part 1, and split on a known delimiter recovers the leading segment;
 * overlay inserting the empty string is the identity; translate composed with
 * its inverse map is the identity; and repeat multiplies length exactly.  All
 * strings are drawn from the quote-free [abc] alphabet (or [bc] where 'a' is
 * the delimiter) so SQL literal quoting is trivial and the only variable is
 * the function under test; a false result is a bug in that function, not a
 * quoting artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_textpos.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "textpos"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  The [abc]/[bc] alphabets never contain a quote, but do it
 * defensively so the generated SQL is always well-formed.
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
 * Draw 0..12 characters from a quote-free single-byte alphabet into buf as a
 * bare string (no quoting).  All ASCII, never a single quote, so length()
 * counts characters exactly and quoting is trivial.
 */
static void
draw_from(pg_prop_tc *tc, StringInfo buf, const char *alphabet)
{
	int			span = (int) strlen(alphabet) - 1;
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, span)]);
}

/* Draw over [abc]. */
static void
draw_abc(pg_prop_tc *tc, StringInfo buf)
{
	draw_from(tc, buf, "abc");
}

/* Draw over [bc] (no 'a'), for properties where 'a' is a delimiter. */
static void
draw_bc(pg_prop_tc *tc, StringInfo buf)
{
	draw_from(tc, buf, "bc");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on valid inputs,
 * which is itself a failure, reported with the SQL.
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
 * A position is either not-found (0) or a valid 1-based index: it lies in
 * [1, length(S) - length(P) + 1].  When P is longer than S the needle cannot
 * occur, so position is 0 and the first disjunct holds; otherwise the found
 * index can be at most the last place a P-length window fits in S.  P is
 * drawn 1..3 characters over [abc].
 */
PG_PROP(position_found_or_zero)
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
	draw_abc(tc, &s);

	initStringInfo(&p);
	{
		static const char alphabet[] = "abc";
		int			n = (int) pg_prop_draw_int(tc, 1, 3);

		for (int i = 0; i < n; i++)
			appendStringInfoChar(&p, alphabet[pg_prop_draw_int(tc, 0, 2)]);
	}

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT position(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = 0 OR (position(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") >= 1 AND position(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") <= length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") - length(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ") + 1)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Deleting a character removes every occurrence: after replace(S, 'a', ''),
 * position('a' in ...) is 0 (no 'a' remains).
 */
PG_PROP(replace_removes_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc(tc, &s);

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
 * Replacing an absent needle is the identity: 'z' never occurs in an [abc]
 * string, so replace(S, 'z', 'Q') = S.
 */
PG_PROP(replace_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'z', 'Q') = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * split_part on an absent delimiter yields the whole string as the single
 * part: 'x' never occurs in an [abc] string, so split_part(S, 'x', 1) = S.
 */
PG_PROP(split_part_rejoin)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT split_part(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'x', 1) = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * split_part recovers the leading segment: with S and T drawn over [bc] (no
 * 'a'), the string S || 'a' || T splits on 'a' into exactly [S, T], so
 * split_part(S || 'a' || T, 'a', 1) = S.  Because S has no 'a', the first
 * delimiter is the one we inserted.
 */
PG_PROP(split_part_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				t,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&t);
	draw_bc(tc, &s);
	draw_bc(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT split_part((");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || 'a' || ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, "), 'a', 1) = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Overlay inserting the empty string is the identity: overlay(S placing ''
 * from p for 0) inserts nothing at position p and deletes zero characters, so
 * the result is S.  p is drawn 1..length(S)+1 (every legal insertion point,
 * including just past the end).
 */
PG_PROP(overlay_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			slen;
	int			p;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc(tc, &s);
	slen = (int) strlen(s.data);
	p = (int) pg_prop_draw_int(tc, 1, slen + 1);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT overlay(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " placing '' from %d for 0) = ", p);
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * translate is invertible under a bijective map: translate(S, 'abc', 'xyz')
 * renames a->x, b->y, c->z, and translating the result back with the swapped
 * argument order restores S.  Over [abc] the map is a bijection, so the
 * composition is the identity.
 */
PG_PROP(translate_maps)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT translate(translate(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'abc', 'xyz'), 'xyz', 'abc') = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * repeat multiplies length exactly: length(repeat(S, k)) = length(S) * k for
 * k in 0..5 (k = 0 yields the empty string, length 0).
 */
PG_PROP(repeat_length)
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
	draw_abc(tc, &s);
	k = (int) pg_prop_draw_int(tc, 0, 5);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(repeat(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", %d)) = length(", k);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ") * %d", k);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
