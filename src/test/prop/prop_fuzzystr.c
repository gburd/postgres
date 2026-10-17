/*-------------------------------------------------------------------------
 *
 * prop_fuzzystr.c
 *	  Property-based tests for the fuzzystrmatch contrib extension.
 *
 * fuzzystrmatch's levenshtein is a metric with well-known bounds; these check
 * the metric axioms and the identity/bound relationships.
 *
 * levenshtein(text, text) -> int is a proper metric: non-negative, symmetric,
 * zero iff equal, and it satisfies the triangle inequality.  It is also
 * bounded below by the length difference (an edit changes length by at most
 * one) and above by the longer operand's length (every character substituted).
 * These properties check each of those relationships against SQL as the
 * oracle; a "f" result is a bug in the metric, not a tolerance artefact.
 *
 * soundex/metaphone/dmetaphone also exist, but their outputs are opaque, so
 * only soundex determinism (property 7) is checked; the metric properties are
 * the high-value ones.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_fuzzystr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "fuzzystr"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Connect to the test cluster and ensure the fuzzystrmatch extension exists.
 * Each property opens its own connection (the Tier-B pattern); CREATE
 * EXTENSION IF NOT EXISTS makes the DDL idempotent against a reused backend.
 * On any failure the property is failed with the message.
 */
static pg_prop_conn *
connect_fuzzy(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "CREATE EXTENSION IF NOT EXISTS fuzzystrmatch",
						  &err))
		pg_prop_fail(tc, "setup failed (fuzzystrmatch): %s", err ? err : "");

	return conn;
}

/*
 * Draw 0..10 characters from the alphabet [a-z] into buf as a bare string (no
 * quoting).  The empty string is allowed: levenshtein('', '') = 0, which is
 * within every bound checked here.  Letters only, so no quoting-critical
 * character is ever generated.
 */
static void
draw_word(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 10);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 25)));
}

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our alphabet never contains a quote, but do it defensively
 * so the generated SQL is always well-formed.  Mirrors prop_like.c.
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
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on valid inputs,
 * which is itself a bug -- so either is a failure, reported with the SQL.
 * Mirrors require_true in prop_numeric.c.
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
 * Self distance is zero: levenshtein(S, S) = 0.  The identity axiom of the
 * metric -- a string is at distance zero from itself.
 */
PG_PROP(levenshtein_zero_iff_equal)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_fuzzy(tc);

	initStringInfo(&s);
	draw_word(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT levenshtein(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = 0");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Symmetry: levenshtein(S, T) = levenshtein(T, S).  The distance does not
 * depend on which argument comes first.
 */
PG_PROP(levenshtein_symmetric)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_fuzzy(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_word(tc, &s);
	draw_word(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT levenshtein(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") = levenshtein(");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Non-negativity and upper bound: 0 <= levenshtein(S, T) <= max(len(S),
 * len(T)).  The distance is never negative, and it never exceeds the longer
 * operand's length because the worst case substitutes every character.
 */
PG_PROP(levenshtein_nonneg_and_bounded)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_fuzzy(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_word(tc, &s);
	draw_word(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT levenshtein(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") BETWEEN 0 AND greatest(length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "), length(");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Triangle inequality: levenshtein(S, U) <= levenshtein(S, T) +
 * levenshtein(T, U).  The defining axiom that makes levenshtein a metric; a
 * path through T can never be shorter than the direct distance.
 */
PG_PROP(levenshtein_triangle_inequality)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				u,
				q;

	conn = connect_fuzzy(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	initStringInfo(&u);
	draw_word(tc, &s);
	draw_word(tc, &t);
	draw_word(tc, &u);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT levenshtein(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, u.data);
	appendStringInfoString(&q, ") <= levenshtein(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") + levenshtein(");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, u.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(u.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Length lower bound: levenshtein(S, T) >= abs(len(S) - len(T)).  A single
 * edit changes length by at most one, so the distance cannot be smaller than
 * the length difference.
 */
PG_PROP(levenshtein_length_lower_bound)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_fuzzy(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_word(tc, &s);
	draw_word(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT levenshtein(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") >= abs(length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") - length(");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Appending one character costs at most one edit: levenshtein(S, S || 'z') <=
 * 1.  Turning S into S with one extra trailing character is a single
 * insertion, so the distance is 0 or 1.
 */
PG_PROP(levenshtein_append_char_at_most_one)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_fuzzy(tc);

	initStringInfo(&s);
	draw_word(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT levenshtein(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || 'z') <= 1");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * soundex is deterministic: the same input always yields the same code.
 * soundex of an empty string may be '' which still equals itself, so an empty
 * word is harmless here.
 */
PG_PROP(soundex_deterministic)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_fuzzy(tc);

	initStringInfo(&s);
	draw_word(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT soundex(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = soundex(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
