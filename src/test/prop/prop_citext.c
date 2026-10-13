/*-------------------------------------------------------------------------
 *
 * prop_citext.c
 *	  Property-based tests for the citext case-insensitive text type.
 *
 * The citext contrib extension compares and hashes case-insensitively while
 * preserving the stored text; these check the case-insensitive equality and
 * ordering contract against lower()-folded plain text as the oracle.
 *
 * The oracle is independent of citext: lower()/upper() on plain text fold
 * case the same way citext's comparison does, so citext equality must equal
 * plain-text equality after lower(), and citext ordering must equal lower()
 * ordering.  Casting a citext back to text must return the original bytes
 * unchanged (folding happens only for comparison, not storage).  Inputs are
 * single-byte mixed-case ASCII ("abAB") so folding is non-trivial and
 * lower()/upper()/length() are exact oracles.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_citext.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "citext"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Connect to the test cluster and ensure the citext extension exists.  Each
 * property opens its own connection (the Tier-B pattern); CREATE EXTENSION IF
 * NOT EXISTS makes the DDL idempotent across connections.  On any failure the
 * property is failed with the message.
 */
static pg_prop_conn *
connect_citext(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "CREATE EXTENSION IF NOT EXISTS citext",
						  &err))
		pg_prop_fail(tc, "setup failed (citext): %s", err ? err : "");

	return conn;
}

/*
 * Draw 0..12 characters of mixed-case ASCII from "abAB" into buf as a bare
 * string (no quoting).  The alphabet mixes upper and lower case so case
 * folding is exercised, and every character is single-byte so length is exact
 * and lower()/upper() are the precise oracles.
 */
static void
draw_mixed(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abAB";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 3)]);
}

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
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means citext comparison/output errored or produced NULL on
 * valid single-byte strings, which is itself a bug -- so either is a failure,
 * reported with the SQL.  Mirrors require_true in prop_numeric.c.
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
 * citext equality folds case: a string equals both its uppercased and its
 * lowercased forms when compared as citext.  upper()/lower() are applied
 * server-side on plain text so no C-side folding can disagree:
 *
 *		S::citext = upper(S)::citext AND S::citext = lower(S)::citext
 */
PG_PROP(equality_is_case_insensitive)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_citext(tc);

	initStringInfo(&s);
	draw_mixed(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::citext = upper(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")::citext) AND (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::citext = lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")::citext)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The core contract: citext equality is exactly case-folded text equality.
 * For two strings S, T, comparing them as citext must agree with comparing
 * their lower()-folded plain text:
 *
 *		(S::citext = T::citext) = (lower(S) = lower(T))
 *
 * lower() on plain text is the independent oracle, so a folding bug in
 * citext's equality operator fails here.
 */
PG_PROP(equality_matches_lower_oracle)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_citext(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_mixed(tc, &s);
	draw_mixed(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::citext = ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, "::citext) = (lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = lower(");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * citext preserves the stored text on output: casting back to text returns
 * the original bytes, not a folded form (folding happens only for
 * comparison):
 *
 *		(S::citext)::text = S
 */
PG_PROP(preserves_stored_text)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_citext(tc);

	initStringInfo(&s);
	draw_mixed(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::citext)::text = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * citext ordering is case-insensitive, matching lower()-folded comparison.
 * For two strings S, T, the citext "<" operator must agree with "<" on their
 * lower()-folded plain text:
 *
 *		(S::citext < T::citext) = (lower(S) < lower(T))
 *
 * A sign or short-circuit bug in citext's comparison routine fails here.
 */
PG_PROP(ordering_matches_lower)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_citext(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_mixed(tc, &s);
	draw_mixed(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::citext < ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, "::citext) = (lower(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") < lower(");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concatenation then compare folds case: concatenating S and T and comparing
 * as citext must equal concatenating their uppercased forms and comparing as
 * citext (both fold to the same value):
 *
 *		(S || T)::citext = (upper(S) || upper(T))::citext
 */
PG_PROP(concat_is_case_insensitive)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_citext(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_mixed(tc, &s);
	draw_mixed(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ((");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ")::citext) = ((upper(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") || upper(");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, "))::citext)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * citext length equals the plain-text length: case folding does not change
 * the length for single-byte ASCII, so length() through citext must match
 * length() on the plain text:
 *
 *		length(S::citext) = length(S)
 */
PG_PROP(length_preserved)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_citext(tc);

	initStringInfo(&s);
	draw_mixed(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::citext) = length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
