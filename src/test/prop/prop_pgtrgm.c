/*-------------------------------------------------------------------------
 *
 * prop_pgtrgm.c
 *	  Property-based tests for the pg_trgm trigram-similarity extension.
 *
 * pg_trgm computes trigram sets and a similarity in [0,1]; these check the
 * similarity bounds, symmetry, self-similarity, and consistency with
 * show_trgm, without reimplementing the trigram algorithm.
 *
 * The oracle is pg_trgm's own stated contract -- similarity ranges over the
 * unit interval, is symmetric, is 1 for a string against itself when the
 * string has any trigrams and 0 against the empty string, show_trgm's
 * cardinality is bounded by the padded-word trigram count, and the <->
 * distance operator is 1 - similarity.  None of this re-derives the trigram
 * decomposition itself; it asserts the invariants the decomposition must
 * satisfy, so a bug in trigram generation or similarity scoring shows up as a
 * broken invariant.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_pgtrgm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "pgtrgm"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our alphabets never contain a quote, but do it defensively
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
 * Draw 0..12 characters from "[a-z] plus space" into buf as a bare string
 * (no quoting).  The space is included so word boundaries occur inside the
 * generated text -- pg_trgm pads each word, so multi-word strings exercise
 * the inter-word padding and the full similarity path.
 */
static void
draw_word(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abcdefghijklmnopqrstuvwxyz ";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);
	int			i;

	for (i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 26)]);
}

/*
 * Draw a single lowercase [a-z] word of length 1..10 into buf (no spaces),
 * for the show_trgm cardinality property, which needs a single padded word.
 */
static void
draw_single_word(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abcdefghijklmnopqrstuvwxyz";
	int			n = (int) pg_prop_draw_int(tc, 1, 10);
	int			i;

	for (i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 25)]);
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
 * Connect to the test cluster and ensure the pg_trgm extension exists.  Each
 * property opens its own connection (the Tier-B pattern); CREATE EXTENSION IF
 * NOT EXISTS makes the DDL idempotent across connections and backends.  On
 * any failure the property is failed with the message.
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
 * similarity is in the unit interval: for any strings S, T,
 * similarity(S, T) BETWEEN 0 AND 1.  A score outside [0,1] means the shared/
 * total trigram ratio is miscomputed.
 */
PG_PROP(similarity_in_unit_interval)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_word(tc, &s);
	draw_word(tc, &t);

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
 * similarity is symmetric: similarity(S, T) = similarity(T, S).  The trigram
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
	draw_word(tc, &s);
	draw_word(tc, &t);

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
 * A string with at least one trigram is maximally similar to itself.
 * show_trgm(S) gives S's trigram array; when it is non-empty, S shares every
 * trigram with itself so similarity(S, S) = 1.  The CASE's ELSE handles a
 * too-short/empty string, which has no trigrams and whose self-similarity is
 * 0, so the property stays sound for every drawn S.
 */
PG_PROP(self_similarity_is_one_when_has_trigrams)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	draw_word(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT CASE WHEN cardinality(show_trgm(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) > 0 THEN similarity(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = 1 ELSE true END");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * similarity to the empty string is 0: no string shares a trigram with "",
 * which has none, so the ratio is exactly 0 (compared with = since it is
 * exact, not a rounding artefact).
 */
PG_PROP(similar_to_empty_is_zero)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	draw_word(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT similarity(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", '') = 0");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * show_trgm's cardinality is bounded by the padded-word trigram count.  For a
 * single lowercase word W of length L >= 1, pg_trgm pads with two leading
 * spaces and one trailing, which yields L + 1 positional trigrams.  pg_trgm
 * de-duplicates, so a word with repeated trigrams produces fewer distinct
 * ones; the exact count is therefore an UPPER bound.  Asserting
 * cardinality BETWEEN 1 AND length(W) + 1 is sound regardless of duplicates:
 * at least one trigram (L >= 1) and never more than L + 1.
 */
PG_PROP(show_trgm_count_matches_word_trigram_formula)
{
	pg_prop_conn *conn;
	StringInfoData w,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&w);
	draw_single_word(tc, &w);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT cardinality(show_trgm(");
	append_sql_literal(&q, w.data);
	appendStringInfoString(&q, ")) BETWEEN 1 AND length(");
	append_sql_literal(&q, w.data);
	appendStringInfoString(&q, ") + 1");

	require_true(tc, conn, q.data);

	pfree(w.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * word_similarity is also in the unit interval: word_similarity(S, T)
 * BETWEEN 0 AND 1.  (word_similarity >= similarity is not an invariant, so it
 * is deliberately not asserted here; only the range bound is.)
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
	draw_word(tc, &s);
	draw_word(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT word_similarity(");
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
 * The trigram distance operator <-> is 1 - similarity.  Both are reals, so a
 * small tolerance absorbs the float rounding of the two independent code
 * paths that compute them:
 *
 *		abs((S <-> T) - (1 - similarity(S, T))) < 1e-6
 */
PG_PROP(distance_is_one_minus_similarity)
{
	pg_prop_conn *conn;
	StringInfoData s,
				t,
				q;

	conn = connect_trgm(tc);

	initStringInfo(&s);
	initStringInfo(&t);
	draw_word(tc, &s);
	draw_word(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT abs((");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " <-> ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") - (1 - similarity(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, "))) < 1e-6");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
