/*-------------------------------------------------------------------------
 *
 * prop_regexpreplace.c
 *	  Property-based tests for regexp_replace() with backreferences, flags
 *	  and capture groups.
 *
 * Exercises the substitution machinery of regexp_replace
 * (src/backend/utils/adt/regexp.c): the 'g' global flag, the 'i'
 * case-insensitive flag, anchors (^ and $), the whole-string ".*" match and
 * the backreference substitution path (\1, \2) over capture groups.  Each
 * property is a scalar boolean oracle built from independent string builtins
 * (replace/length/left/substr) or a hand-computed constant, so a "f" means
 * the replacement engine disagrees with an oracle that does not use regexes,
 * which is a bug.  Strings are letters-only over "abc" (length 0..12) so no
 * drawn character is ever a regex metacharacter.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_regexpreplace.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "regexpreplace"
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
 * Draw 0..12 characters of plain text from the 3-letter alphabet "abc" into
 * buf as a bare string (no quoting).  Letters only -- never a regex
 * metacharacter -- so the literal/anchored-pattern oracles stay exact.
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
 * Property 1 -- replace_all_g.
 *
 * The 'g' flag replaces every occurrence, so regexp_replace(S, 'a', 'X', 'g')
 * turns each 'a' into one 'X'.  The number of 'X' in the result therefore
 * equals the number of 'a' in S, oracled without regexes by the length
 * difference after removing each character with replace():
 *   length(R) - length(replace(R, 'X', '')) = length(S) - length(replace(S, 'a', ''))
 * where R = regexp_replace(S, 'a', 'X', 'g').  (S has no 'X', so counting 'X'
 * in R counts exactly the substitutions.)
 */
PG_PROP(replace_all_g)
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
	appendStringInfoString(&q, "SELECT (length(regexp_replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', 'X', 'g')) - length(replace(regexp_replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', 'X', 'g'), 'X', ''))) = (length(");
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
 * Property 2 -- replace_first_only.
 *
 * Without 'g', regexp_replace(S, 'a', 'X') replaces at most one 'a'.  Using
 * the empty replacement so the length oracle is clean: removing exactly the
 * first 'a' shortens S by one when S had an 'a', and leaves it unchanged
 * otherwise.  So the 'a' count decreases by 1 iff S contained an 'a':
 *   count_a(regexp_replace(S, 'a', '')) = count_a(S) - (1 if S has 'a' else 0)
 * where count_a(T) = length(T) - length(replace(T, 'a', '')).
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
	appendStringInfoString(&q, "SELECT (length(regexp_replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', '')) - length(replace(regexp_replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', ''), 'a', ''))) = ((length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") - length(replace(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 'a', ''))) - (CASE WHEN position('a' in ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") > 0 THEN 1 ELSE 0 END))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 3 -- backreference.
 *
 * Two capture groups (a)(b) with the replacement E'\2\1' swap the matched
 * fragments.  The constant case regexp_replace('ab', '(a)(b)', E'\2\1') = 'ba'
 * is checked alongside a drawn 2-character string c1||c2 from "abc":
 * regexp_replace(c1||c2, '(.)(.)', E'\2\1') = c2||c1.  '(.)(.)' matches any
 * two characters and the backreferences reverse them, oracled by the literal
 * concatenation c2||c1.  (In the C source each backslash is doubled so the
 * SQL text sees E'\2\1'.)
 */
PG_PROP(backreference)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData c1,
				c2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* draw exactly two characters from "abc" */
	initStringInfo(&c1);
	initStringInfo(&c2);
	{
		static const char letters[] = "abc";

		appendStringInfoChar(&c1, letters[pg_prop_draw_int(tc, 0, 2)]);
		appendStringInfoChar(&c2, letters[pg_prop_draw_int(tc, 0, 2)]);
	}

	initStringInfo(&q);
	/* constant case: literal (a)(b) swap */
	appendStringInfoString(&q, "SELECT regexp_replace('ab', '(a)(b)', E'\\\\2\\\\1') = 'ba'");
	/* drawn case: (.)(.) swaps the two drawn characters */
	appendStringInfoString(&q, " AND regexp_replace(");
	append_sql_literal(&q, c1.data);
	appendStringInfoString(&q, " || ");
	append_sql_literal(&q, c2.data);
	appendStringInfoString(&q, ", '(.)(.)', E'\\\\2\\\\1') = (");
	append_sql_literal(&q, c2.data);
	appendStringInfoString(&q, " || ");
	append_sql_literal(&q, c1.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(c1.data);
	pfree(c2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 4 -- case_insensitive_flag.
 *
 * The 'i' flag makes the match case-insensitive, so the lowercase pattern
 * 'abc' matches the uppercase subject 'ABC':
 *   regexp_replace('ABC', 'abc', 'X', 'i') = 'X'.
 * A constant check -- no draw needed -- that the 'i' flag reaches the matcher.
 */
PG_PROP(case_insensitive_flag)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT regexp_replace('ABC', 'abc', 'X', 'i') = 'X'");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 5 -- replace_empty_match.
 *
 * 'z' is not in the "abc" alphabet S is drawn from, so the pattern never
 * matches and a global replace leaves S unchanged:
 *   regexp_replace(S, 'z', 'X', 'g') = S.
 */
PG_PROP(replace_empty_match)
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
	appendStringInfoString(&q, ", 'z', 'X', 'g') = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 6 -- replace_whole.
 *
 * The anchored pattern '^.*$' matches the entire string in one go, so without
 * 'g' a non-empty S is replaced wholesale by the replacement:
 *   regexp_replace(S, '^.*$', 'Y') = 'Y' for non-empty S.
 * For an empty S the single empty match still yields 'Y', so the identity
 * holds for every drawn S; the CASE is left in only to document the
 * non-empty reasoning while remaining true at length 0.
 */
PG_PROP(replace_whole)
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
	appendStringInfoString(&q, ", '^.*$', 'Y') = 'Y'");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 7 -- anchored_replace.
 *
 * The '^' anchor restricts the match to the start of the string, so
 * regexp_replace(S, '^a', 'X') rewrites 'a' only when it is the first
 * character.  Oracled by the string builtins left()/substr():
 *   = CASE WHEN left(S,1) = 'a' THEN 'X' || substr(S,2) ELSE S END.
 * (substr(S,2) is the empty string when S is 'a' alone, so the single-char
 * and length-0 cases both come out right.)
 */
PG_PROP(anchored_replace)
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
	appendStringInfoString(&q, ", '^a', 'X') = (CASE WHEN left(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 1) = 'a' THEN 'X' || substr(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 2) ELSE ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " END)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 8 -- identity_no_match.
 *
 * The pattern 'zzz' uses only characters outside the "abc" alphabet, so it
 * never matches and a global replace is the identity:
 *   regexp_replace(S, 'zzz', 'Q', 'g') = S.
 */
PG_PROP(identity_no_match)
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
	appendStringInfoString(&q, ", 'zzz', 'Q', 'g') = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
