/*-------------------------------------------------------------------------
 *
 * prop_regexfns.c
 *	  Property-based tests for the regexp text-processing functions.
 *
 * Exercises regexp_replace, regexp_match, regexp_split_to_array and
 * regexp_count (src/backend/utils/adt/regexp.c) against independent string
 * builtins (replace/length/position/array_to_string).  Strings are letters-
 * only over "abc" and every pattern/delimiter is a single literal letter
 * ('a' or 'z'), so no regex metacharacter is ever produced and the literal-
 * pattern semantics coincide exactly with the string-builtin oracles.  A "f"
 * means a regexp function disagrees with its string-builtin oracle, which is
 * a bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_regexfns.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "regexfns"
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
 * Draw 0..12 characters of plain text from the 3-letter alphabet "abc" into
 * buf as a bare string (no quoting).  Letters only -- never a regex
 * metacharacter -- so the literal-pattern oracles stay exact.
 */
static void
draw_text(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abc";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 2)]);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on non-NULL letter
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
 * Property 1 -- split_to_array_rejoin.
 *
 * The delimiter 'x' is not in the "abc" alphabet, so it never occurs in S:
 * regexp_split_to_array(S, 'x') cannot split and must yield the single-
 * element array ARRAY[S].  Rejoining with any separator therefore reproduces
 * S exactly, so array_to_string(regexp_split_to_array(S, 'x'), 'x') = S.
 */
PG_PROP(split_to_array_rejoin)
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
	appendStringInfoString(&q, "SELECT array_to_string(regexp_split_to_array(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'x'), 'x') = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 2 -- split_count_plus_one.
 *
 * Splitting S on the literal delimiter 'a' produces (occurrences of 'a') + 1
 * pieces.  The occurrence count is oracled by the length difference after
 * removing 'a' (each single-char removal shortens S by one), so
 * cardinality(regexp_split_to_array(S, 'a'))
 *   = (length(S) - length(replace(S, 'a', ''))) + 1.
 */
PG_PROP(split_count_plus_one)
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
	appendStringInfoString(&q, "SELECT cardinality(regexp_split_to_array(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a')) = ((length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") - length(replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', ''))) + 1)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 3 -- replace_global_removes.
 *
 * Global replace of the literal 'a' with the empty string removes every 'a',
 * so the result contains no 'a': position('a' in regexp_replace(S, 'a', '',
 * 'g')) = 0.
 */
PG_PROP(replace_global_removes)
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
	appendStringInfoString(&q, "SELECT position('a' in regexp_replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', '', 'g')) = 0");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 4 -- replace_first_only.
 *
 * Without the 'g' flag, regexp_replace(S, 'a', 'X') replaces at most the
 * first 'a'.  Using the empty replacement so the length oracle is clean:
 * removing exactly the first 'a' shortens S by one when S had an 'a', and
 * leaves it unchanged otherwise.  So
 *   length(regexp_replace(S, 'a', ''))
 *     = length(S) - (CASE WHEN position('a' in S) > 0 THEN 1 ELSE 0 END).
 */
PG_PROP(replace_first_only)
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
	appendStringInfoString(&q, "SELECT length(regexp_replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', '')) = (length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") - (CASE WHEN position('a' in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") > 0 THEN 1 ELSE 0 END))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 5 -- regexp_count_matches.
 *
 * regexp_count of the literal 'a' equals its number of occurrences, oracled
 * by the length difference after removing 'a' (each single-char removal
 * shortens S by one):
 *   regexp_count(S, 'a') = length(S) - length(replace(S, 'a', '')).
 */
PG_PROP(regexp_count_matches)
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
	appendStringInfoString(&q, "SELECT regexp_count(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a') = (length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") - length(replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', '')))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 6 -- match_returns_null_when_absent.
 *
 * 'z' is not in the "abc" alphabet S is drawn from, so there is no match and
 * regexp_match(S, 'z') must be SQL NULL: regexp_match(S, 'z') IS NULL.
 */
PG_PROP(match_returns_null_when_absent)
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
	appendStringInfoString(&q, "SELECT regexp_match(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'z') IS NULL");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 7 -- replace_identity_when_absent.
 *
 * Replacing a pattern that cannot occur leaves S unchanged.  'z' is not in the
 * "abc" alphabet S is drawn from, so regexp_replace(S, 'z', 'Q', 'g') = S.
 */
PG_PROP(replace_identity_when_absent)
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
	appendStringInfoString(&q, "SELECT regexp_replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'z', 'Q', 'g') = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
