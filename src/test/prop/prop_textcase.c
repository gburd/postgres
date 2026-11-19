/*-------------------------------------------------------------------------
 *
 * prop_textcase.c
 *	  Property-based tests for ASCII upper/lower/initcap case-folding laws.
 *
 * The oracle is the algebra of ASCII case folding: on the alphabet [a-zA-Z]
 * every character folds 1:1 (one lowercase <-> one uppercase, no multibyte
 * expansion), so upper/lower are idempotent, length-preserving, mutually
 * inverse on single letters, and distribute over concatenation.  A broken
 * identity here is a bug in the case-fold path for plain ASCII text, not a
 * locale or Unicode-expansion artefact -- these inputs never leave ASCII.
 *
 * Distinct from the unicode_case / strfunc / textfns areas: those cover
 * Unicode folding, assorted string builtins, and text utilities.  This area
 * asserts only the pure ASCII case-fold laws.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_textcase.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "textcase"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our alphabet is [a-zA-Z] and never contains a quote, but do
 * it defensively so the generated SQL is always well-formed.
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
 * Draw 0..12 ASCII letters from [a-zA-Z] into buf as a bare string.  Letters
 * only -- all single-byte, 1:1 case folding, never a quote -- so length()
 * counts characters exactly and quoting is trivial.
 */
static void
draw_letters(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] =
		"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 51)]);
}

/*
 * Draw a single lowercase ASCII letter [a-z] into buf as a bare string.
 */
static void
draw_lower_letter(pg_prop_tc *tc, StringInfo buf)
{
	appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 25)));
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the expression errored or produced NULL on well-formed
 * ASCII letter strings, which is itself a bug -- so either is a failure,
 * reported with the SQL.  Modelled on prop_numeric.c's require_true.
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
 * lower is idempotent: lower(lower(S)) = lower(S).  A second fold of
 * already-lowercased ASCII text must be a no-op.
 */
PG_PROP(lower_idempotent)
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
	appendStringInfoString(&q, "SELECT lower(lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * upper is idempotent: upper(upper(S)) = upper(S).
 */
PG_PROP(upper_idempotent)
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
	appendStringInfoString(&q, "SELECT upper(upper(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = upper(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Uppercasing then lowercasing equals just lowercasing: lower(upper(S)) =
 * lower(S).  For ASCII the uppercase of any letter lowercases back to the
 * same letter as the original did, so the intervening upper() is invisible.
 */
PG_PROP(upper_lower_roundtrip_lower)
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
	appendStringInfoString(&q, "SELECT lower(upper(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Case folding preserves length: length(upper(S)) = length(S) and
 * length(lower(S)) = length(S).  ASCII case folding is 1:1, so neither fold
 * adds or drops characters.
 */
PG_PROP(length_preserved)
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
 * initcap of a single ASCII word (letters only, no spaces) uppercases the
 * first character and lowercases the rest: initcap(W) = upper(left(W,1)) ||
 * lower(substr(W,2)), for length(W) >= 1.  Drawing a word of 1..12 letters
 * from [a-zA-Z] with no spaces means there is exactly one word boundary (the
 * start), so initcap's "first letter of each word" reduces to this.
 */
PG_PROP(initcap_lower_rest)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* a single word of >= 1 letters, no spaces */
	initStringInfo(&s);
	n = (int) pg_prop_draw_int(tc, 1, 12);
	{
		static const char alphabet[] =
			"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

		for (int i = 0; i < n; i++)
			appendStringInfoChar(&s, alphabet[pg_prop_draw_int(tc, 0, 51)]);
	}

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT initcap(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = upper(left(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 1)) || lower(substr(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 2))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Case folding is case-insensitive at the fold: lower(S) = lower(upper(S))
 * and upper(S) = upper(lower(S)).  Folding to one case first and then folding
 * again cannot change the final-case result for ASCII.
 */
PG_PROP(case_insensitive_eq)
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
	appendStringInfoString(&q, "SELECT lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = lower(upper(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) AND upper(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = upper(lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Case folding distributes over concatenation: lower(S || T) =
 * lower(S) || lower(T).  Folding is per-character and context-free for ASCII,
 * so splitting the fold across a join cannot change the result.
 */
PG_PROP(concat_distributes)
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
	draw_letters(tc, &s);
	draw_letters(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") = lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") || lower(");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For a lowercase ASCII letter c, upper actually changes it and the fold is
 * invertible: upper(c) <> c and lower(upper(c)) = c.  This pins down that the
 * [a-z] range genuinely has distinct uppercase images that round-trip back.
 */
PG_PROP(upper_lower_disjoint_for_letters)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&c);
	draw_lower_letter(tc, &c);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT upper(");
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, ") <> ");
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, " AND lower(upper(");
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, ")) = ");
	append_sql_literal(&q, c.data);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
