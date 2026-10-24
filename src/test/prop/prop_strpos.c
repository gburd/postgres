/*-------------------------------------------------------------------------
 *
 * prop_strpos.c
 *	  Property-based tests for the precise semantics of position/strpos,
 *	  starts_with, and overlay.
 *
 * These functions have exact, documented contracts: strpos(S, P) and
 * position(P in S) are synonyms returning a 1-based index or 0 when absent;
 * the empty needle is found at position 1; starts_with(S, P) is true iff P is
 * a prefix of S; and overlay(S placing R from p for n) splices R over n
 * characters starting at p, so an interior replace of equal width preserves
 * length while a zero-width insert grows it by length(R).  The oracle is this
 * algebra expressed two ways (synonym equality, substring recovery, prefix
 * via left()), so a false result is a bug in the function, not a quoting
 * artefact: all strings are drawn from the quote-free [abc] alphabet, with
 * 'z' and 'XY' as sentinels guaranteed absent from that alphabet.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_strpos.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "strpos"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  The [abc] alphabet never contains a quote, but do it
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
 * Draw n..m characters from the quote-free [abc] alphabet into buf as a bare
 * string (no quoting).  All ASCII, never a single quote, so length() counts
 * characters exactly and quoting is trivial.
 */
static void
draw_abc(pg_prop_tc *tc, StringInfo buf, int min, int max)
{
	static const char alphabet[] = "abc";
	int			n = (int) pg_prop_draw_int(tc, min, max);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 2)]);
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
 * strpos and position are synonyms: strpos(S, P) = position(P in S) for every
 * S and P.  strpos takes its arguments (haystack, needle) while position uses
 * the "needle in haystack" syntax, so this also pins down the argument order.
 * P is drawn 0..3 characters over [abc] (including empty, which both must
 * handle identically).
 */
PG_PROP(strpos_eq_position)
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
	draw_abc(tc, &s, 0, 12);
	draw_abc(tc, &p, 0, 3);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT strpos(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ") = position(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A needle absent from the alphabet is never found: 'z' never occurs in an
 * [abc] string, so position('z' in S) = 0.
 */
PG_PROP(position_zero_when_absent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc(tc, &s, 0, 12);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT position('z' in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = 0");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The empty needle is found at position 1: position('' in S) = 1 for every S
 * (including the empty string, where the empty needle still reports 1).
 */
PG_PROP(position_empty_is_one)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc(tc, &s, 0, 12);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT position('' in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = 1");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * starts_with(S, P) is true iff P is a prefix of S, which is exactly
 * left(S, length(P)) = P: left() takes the leading length(P) characters, and
 * they equal P precisely when P prefixes S.  This holds for every P including
 * the empty string (left(S, 0) = '' = P).  P is drawn 0..3 over [abc].
 */
PG_PROP(starts_with_eq_prefix)
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
	draw_abc(tc, &s, 0, 12);
	draw_abc(tc, &p, 0, 3);

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
 * When position(P in S) = k > 0, the substring of S at k of width length(P)
 * is exactly P: substr(S, k, length(P)) = P.  This is the defining property
 * of a found position.  A CASE guards k = 0 (needle absent) so the property
 * is vacuously true there; when k > 0 the recovered substring must match.
 * P is drawn 1..3 over [abc].
 */
PG_PROP(position_found_substring)
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
	draw_abc(tc, &s, 0, 12);
	draw_abc(tc, &p, 1, 3);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT CASE WHEN position(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = 0 THEN true ELSE substr(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", position(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "), length(");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ")) = ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " END");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An interior equal-width overlay preserves length: overlay(S placing 'XY'
 * from p for 2) replaces the 2 characters at p with the 2-character 'XY', so
 * the length is unchanged.  p is drawn 1..length(S)-1, which keeps the whole
 * 2-wide window inside S (p + 2 - 1 <= length(S)), so no growth from running
 * past the end.  Guarded to length(S) >= 3 so such an interior p exists.
 */
PG_PROP(overlay_replaces)
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
	draw_abc(tc, &s, 3, 12);
	slen = (int) strlen(s.data);
	p = (int) pg_prop_draw_int(tc, 1, slen - 1);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(overlay(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " placing 'XY' from %d for 2)) = length(", p);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A zero-width overlay inserts: overlay(S placing 'XY' from p for 0) deletes
 * nothing and splices in the 2-character 'XY', so the length grows by 2.  p
 * is drawn 1..length(S)+1 (every legal insertion point, including just past
 * the end).
 */
PG_PROP(overlay_insert_grows)
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
	draw_abc(tc, &s, 0, 12);
	slen = (int) strlen(s.data);
	p = (int) pg_prop_draw_int(tc, 1, slen + 1);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(overlay(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, " placing 'XY' from %d for 0)) = length(", p);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") + 2");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A non-empty needle is found at position 1 in a string it prefixes:
 * strpos(P || S, P) = 1, because P is a prefix of P || S and the first match
 * is at the very start.  P is drawn 1..3 over [abc] (empty is excluded
 * because strpos('' prepended, '') is 1 too but trivially; the non-empty case
 * is the interesting prefix contract).
 */
PG_PROP(strpos_prefix)
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
	draw_abc(tc, &s, 0, 12);
	draw_abc(tc, &p, 1, 3);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT strpos((");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, " || ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "), ");
	append_sql_literal(&q, p.data);
	appendStringInfoString(&q, ") = 1");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(p.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
