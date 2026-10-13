/*-------------------------------------------------------------------------
 *
 * prop_regexp.c
 *	  Property-based tests for regular-expression functions.
 *
 * The regexp functions (src/backend/utils/adt/regexp.c) --
 * regexp_replace, regexp_count, regexp_split_to_array,
 * regexp_split_to_table, and the ~ match operator -- are checked here
 * against independent string builtins so the oracle is never the function
 * under test.  For a single-literal-char or single-delimiter pattern the
 * regexp semantics coincide exactly with replace()/position()/length()/
 * string_to_array(), which gives sound, tolerance-free equalities.
 *
 * Every generated string and pattern is letters-only ("abc"/"ab") plus the
 * fixed literal delimiters 'x' and 'z', so no regex metacharacter is ever
 * produced: the patterns are plain literals and the string-builtin oracles
 * are exact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_regexp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "regexp"
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
 * Draw 0..12 characters of plain text from a 3-letter alphabet "abc" into buf
 * as a bare string (no quoting).  Letters only -- never a regex metacharacter
 * -- so the text cannot accidentally contain a special regex token and the
 * literal-pattern oracles stay exact.
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
 * Draw a single literal char from "abc" into *c as a one-character bare
 * string, usable both as a one-char text and as a one-char literal pattern.
 */
static void
draw_char(pg_prop_tc *tc, char *c)
{
	static const char letters[] = "abc";

	c[0] = letters[pg_prop_draw_int(tc, 0, 2)];
	c[1] = '\0';
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
 * Global replace of a single literal char removes every occurrence and agrees
 * with replace().  For a literal single-char pattern c, regexp_replace(S, c,
 * '', 'g') must equal replace(S, c, '') -- replace() is the independent
 * oracle for a literal pattern -- which in turn contains no c.
 */
PG_PROP(replace_g_removes_all_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	char		c[2];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);
	draw_char(tc, c);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT regexp_replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, c);
	appendStringInfoString(&q, ", '', 'g') = replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, c);
	appendStringInfoString(&q, ", '')");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The ~ match operator with a single-literal-char pattern matches iff the
 * char occurs.  (S ~ c) must equal (position(c in S) > 0), with position()
 * the independent occurrence oracle for a literal char.
 */
PG_PROP(match_consistent_with_position)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	char		c[2];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);
	draw_char(tc, c);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " ~ ");
	append_sql_literal(&q, c);
	appendStringInfoString(&q, ") = (position(");
	append_sql_literal(&q, c);
	appendStringInfoString(&q, " in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") > 0)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Splitting on the empty pattern yields individual characters, and rejoining
 * them reconstructs the original.  array_to_string(regexp_split_to_array(S,
 * ''), '') must equal S.
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
	appendStringInfoString(&q, ", ''), '') = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Splitting on a literal delimiter produces as many pieces as the delimiter
 * induces.  The number of rows from regexp_split_to_table(S, 'x') must equal
 * cardinality(string_to_array(S, 'x')) -- string_to_array is the independent
 * oracle for literal-delimiter splitting.  S is built as "ab" tokens joined
 * by 'x', so 'x' occurs only as a delimiter.
 */
PG_PROP(split_count_matches_delimiter)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);

	/*
	 * Build a guaranteed non-empty 'x'-joined string.  On the empty string
	 * regexp_split_to_table yields one empty row while string_to_array yields
	 * an empty array, a documented difference that is not under test here; a
	 * non-empty string makes the two agree.
	 */
	{
		static const char letters[] = "ab";
		int			ntok = (int) pg_prop_draw_int(tc, 1, 6);

		for (int i = 0; i < ntok; i++)
		{
			int			tlen = (int) pg_prop_draw_int(tc, 1, 3);

			if (i > 0)
				appendStringInfoChar(&s, 'x');
			for (int j = 0; j < tlen; j++)
				appendStringInfoChar(&s, letters[pg_prop_draw_int(tc, 0, 1)]);
		}
	}

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (SELECT count(*) FROM regexp_split_to_table(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'x')) = cardinality(string_to_array(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'x'))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Replacing a pattern that cannot occur leaves S unchanged.  'z' is not in the
 * "abc" alphabet S is drawn from, so regexp_replace(S, 'z', 'Q', 'g') must
 * equal S.
 */
PG_PROP(replace_identity_on_nonmatch)
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

/*
 * regexp_count of a single literal char equals the number of occurrences,
 * oracled by the length difference after removing that char: each removed
 * single-char match shortens S by exactly 1, so length(S) - length(replace(S,
 * c, '')) is the exact occurrence count.
 */
PG_PROP(count_matches_occurrences)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	char		c[2];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);
	draw_char(tc, c);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT regexp_count(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, c);
	appendStringInfoString(&q, ") = (length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") - length(replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, c);
	appendStringInfoString(&q, ", '')))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
