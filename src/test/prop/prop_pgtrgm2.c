/*-------------------------------------------------------------------------
 *
 * prop_pgtrgm2.c
 *	  Property-based tests for additional pg_trgm similarity functions.
 *
 * Complements prop_pgtrgm.c (which covers similarity bounds, symmetry,
 * self-similarity, show_trgm cardinality and the <-> distance operator) by
 * exercising a distinct surface: word_similarity, strict_word_similarity,
 * show_trgm existence, and the % containment operator and its relationship to
 * pg_trgm.similarity_threshold.
 *
 * The oracle is pg_trgm's own stated contract -- every similarity flavour
 * ranges over [0,1], a non-empty string shows at least one trigram, the %
 * operator is exactly "similarity >= threshold", and appending a character can
 * never raise self-similarity above 1.  None of this re-derives the trigram
 * decomposition; it asserts the invariants it must satisfy, so a scoring or
 * operator bug surfaces as a broken invariant.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_pgtrgm2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "pgtrgm2"
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
 * Draw 0..12 characters from "[a-c] plus space" into buf as a bare string
 * (no quoting).  The small alphabet forces frequent trigram collisions
 * between S and T, so the similarity functions see non-trivial scores; the
 * space introduces word boundaries so the word_similarity path is exercised.
 */
static void
draw_str(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abc ";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);
	int			i;

	for (i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 3)]);
}

/* True if the bare string contains at least one non-space character. */
static bool
has_nonspace(const char *s)
{
	const char *p;

	for (p = s; *p != '\0'; p++)
		if (*p != ' ')
			return true;
	return false;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the similarity computation errored or produced NULL on
 * valid strings, which is itself a bug -- so either is a failure, reported
 * with the SQL.  Mirrors require_true in prop_numeric.c.
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * Connect to the test cluster and ensure the pg_trgm extension exists.  Each
 * property opens its own connection (the Tier-B pattern); CREATE EXTENSION IF
 * NOT EXISTS makes the DDL idempotent across the many per-case connections.
 */
static pg_prop_conn *
connect_trgm(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "CREATE EXTENSION IF NOT EXISTS pg_trgm",
						  &err))
		pg_prop_fail(tc, "setup failed (pg_trgm): %s", err ? err : "");

	return conn;
}

/*
 * similarity_unit: similarity(S, T) BETWEEN 0 AND 1 for any strings S, T.
 * A score outside the unit interval means the shared/total trigram ratio is
 * miscomputed.
 */
PG_PROP(similarity_unit)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_str(tc, &s);
	draw_str(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT similarity(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") BETWEEN 0 AND 1");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * similarity_self_one: for a string S with at least one trigram (any
 * non-space character), similarity(S, S) = 1 -- S shares every trigram with
 * itself.  Guarded to non-empty-of-trigram S; a whitespace-only or empty S
 * has no trigrams and self-similarity 0, so it is excluded (the property is
 * trivially passed by skipping the assert).
 */
PG_PROP(similarity_self_one)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	draw_str(tc, &s);

	if (has_nonspace(s.data))
	{
		initStringInfo(&q);
		appendStringInfoString(&q, "SELECT similarity(");
		append_sql_literal(&q, s.data);
		appendStringInfoString(&q, ", ");
		append_sql_literal(&q, s.data);
		appendStringInfoString(&q, ") = 1");

		require_true(tc, conn, q.data);
		pfree(q.data);
	}

	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * word_similarity_le_one: both word_similarity(S, T) and
 * strict_word_similarity(S, T) range over [0,1].  (word_similarity >=
 * similarity is not an invariant, so only the range bound is asserted.)
 */
PG_PROP(word_similarity_le_one)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_str(tc, &s);
	draw_str(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT word_similarity(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") BETWEEN 0 AND 1 AND strict_word_similarity(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") BETWEEN 0 AND 1");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * show_trgm_count: show_trgm(S) returns the trigram array.  Its cardinality
 * is always >= 0, and for a string with any non-space character the padding
 * guarantees at least one trigram, so cardinality > 0.  Both bounds hold for
 * every drawn S: the > 0 clause is only required when S has a non-space char.
 */
PG_PROP(show_trgm_count)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	draw_str(tc, &s);

	initStringInfo(&q);
	if (has_nonspace(s.data))
	{
		appendStringInfoString(&q, "SELECT cardinality(show_trgm(");
		append_sql_literal(&q, s.data);
		appendStringInfoString(&q, ")) > 0");
	}
	else
	{
		appendStringInfoString(&q, "SELECT cardinality(show_trgm(");
		append_sql_literal(&q, s.data);
		appendStringInfoString(&q, ")) >= 0");
	}

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * similarity_symmetric: similarity(S, T) = similarity(T, S).  The trigram
 * intersection and union are symmetric, so the ratio must be too; an
 * asymmetric result points at an ordering bug in the set operations.
 */
PG_PROP(similarity_symmetric)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_str(tc, &s);
	draw_str(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT similarity(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") = similarity(");
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
 * percent_operator_threshold: the % operator is exactly the thresholded
 * similarity.  (S % T) must equal (similarity(S, T) >= the current
 * pg_trgm.similarity_threshold GUC).  Reading the threshold via
 * current_setting keeps the oracle correct whatever the session default is.
 */
PG_PROP(percent_operator_threshold)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_str(tc, &s);
	draw_str(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " % ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") = (similarity(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q,
						   ") >= current_setting('pg_trgm.similarity_threshold')::float4)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * identical_percent: for a string S with any non-space character,
 * similarity(S, S) = 1, which is >= any valid threshold (the GUC is in
 * (0,1]), so S % S is true.  Guarded to a trigram-bearing S; a whitespace or
 * empty S has self-similarity 0 and may fall below threshold.
 */
PG_PROP(identical_percent)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	draw_str(tc, &s);

	if (has_nonspace(s.data))
	{
		initStringInfo(&q);
		appendStringInfoString(&q, "SELECT (");
		append_sql_literal(&q, s.data);
		appendStringInfoString(&q, " % ");
		append_sql_literal(&q, s.data);
		appendStringInfoString(&q, ")");

		require_true(tc, conn, q.data);
		pfree(q.data);
	}

	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * similarity_bounded_by_one_on_extend: appending a character to S can never
 * push similarity above its ceiling of 1.  For a trigram-bearing S,
 * similarity(S, S || 'x') is still in [0,1] -- extending one argument cannot
 * exceed full similarity.  Guarded to non-space S so a trigram set exists.
 */
PG_PROP(similarity_bounded_by_one_on_extend)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	draw_str(tc, &s);

	if (has_nonspace(s.data))
	{
		initStringInfo(&q);
		appendStringInfoString(&q, "SELECT similarity(");
		append_sql_literal(&q, s.data);
		appendStringInfoString(&q, ", ");
		append_sql_literal(&q, s.data);
		appendStringInfoString(&q, " || 'x') <= 1");

		require_true(tc, conn, q.data);
		pfree(q.data);
	}

	pfree(s.data);
	pg_prop_disconnect(conn);
}
