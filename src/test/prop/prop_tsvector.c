/*-------------------------------------------------------------------------
 *
 * prop_tsvector.c
 *	  Property-based tests for tsvector structure.
 *
 * tsvector concatenation, setweight, strip, and length (tsvector_op.c) have
 * had fixes; these check structural invariants of tsvector independent of @@
 * matching (which the tsquery area covers).
 *
 * Everything is built through the 'simple' text search configuration, so a
 * lowercase ascii word maps to itself as a lexeme -- no stemming, no
 * stopwords -- which makes the distinct-lexeme oracles below exact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_tsvector.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "tsvector"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Tiny lexeme alphabet.  Four single-letter words keep the generated docs
 * legible in a counterexample and make duplicate words common, so the
 * distinct-count and union-bound properties below actually exercise dedup
 * (D with repeats) and overlap (D1, D2 sharing words).
 */
static const char *const lexemes[] = {"a", "b", "c", "d"};
#define NLEX ((int) (sizeof(lexemes) / sizeof(lexemes[0])))

/* Draw one lexeme from the alphabet. */
static const char *
draw_lexeme(pg_prop_tc *tc)
{
	return lexemes[pg_prop_draw_int(tc, 0, NLEX - 1)];
}

/*
 * Draw a document: 0..8 space-separated lowercase words, e.g. "a b a c".  The
 * raw words are emitted single-space-separated with no leading or trailing
 * space; the caller wraps them in to_tsvector('simple', '<D>').
 */
static void
draw_doc(pg_prop_tc *tc, StringInfo buf)
{
	int			nwords = (int) pg_prop_draw_int(tc, 0, 8);

	for (int i = 0; i < nwords; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ' ');
		appendStringInfoString(buf, draw_lexeme(tc));
	}
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on valid input,
 * which is itself a bug -- so either is a failure, reported with the SQL.
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
 * length(tsvector) is the distinct-lexeme count.  With the 'simple' config
 * each lowercase ascii word maps 1:1 to a lexeme, so the length must equal the
 * number of distinct words in D, counted independently by unnesting the raw
 * document on spaces and DISTINCT-counting.  draw_doc emits single-space
 * tokens with no empty slots, but w <> '' is filtered defensively so a stray
 * empty token (from any accidental double space) never inflates the oracle.
 */
PG_PROP(length_is_distinct_lexeme_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(to_tsvector('simple', '%s')) "
					 "= (SELECT count(DISTINCT w) "
					 "FROM unnest(string_to_array('%s', ' ')) w "
					 "WHERE w <> '')",
					 d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concatenation contains every lexeme of its left operand: for each word of
 * D1, the vector (V1 || V2) still matches a single-lexeme query for that word.
 * This checks that || drops no lexeme from the left side.  bool_and over zero
 * rows (empty D1) is NULL, so coalesce(..., true) treats the vacuous case as
 * holding.
 */
PG_PROP(concat_contains_both)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d1,
				d2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d1);
	initStringInfo(&d2);
	draw_doc(tc, &d1);
	draw_doc(tc, &d2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(bool_and("
					 "(to_tsvector('simple', '%s') || to_tsvector('simple', '%s')) "
					 "@@ to_tsquery('simple', w)), true) "
					 "FROM unnest(string_to_array('%s', ' ')) w "
					 "WHERE w <> ''",
					 d1.data, d2.data, d1.data);

	require_true(tc, conn, q.data);

	pfree(d1.data);
	pfree(d2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * strip() removes positions and weights but keeps the lexeme set, so the
 * stripped vector has the same length as the original.
 */
PG_PROP(strip_preserves_lexemes)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(strip(to_tsvector('simple', '%s'))) "
					 "= length(to_tsvector('simple', '%s'))",
					 d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * setweight() changes the weight label on positions but not the lexeme set,
 * so the length is unchanged after relabelling every position to weight 'A'.
 */
PG_PROP(setweight_preserves_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(setweight(to_tsvector('simple', '%s'), 'A')) "
					 "= length(to_tsvector('simple', '%s'))",
					 d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * tsvector text round-trip: a tsvector rendered to text and parsed back as a
 * tsvector equals the original.  Exercises tsvectorout/tsvectorin on the
 * structure this area builds (positions, dedup, ordering).
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_tsvector('simple', '%s')::text::tsvector "
					 "= to_tsvector('simple', '%s')",
					 d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concatenation is a union of lexeme sets: the distinct-lexeme count of
 * (V1 || V2) is at least each operand's count (nothing is lost) and at most
 * their sum (no lexeme is counted twice beyond the two sides).  Equality to
 * the max happens when one set contains the other; equality to the sum when
 * they are disjoint.
 */
PG_PROP(concat_length_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d1,
				d2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d1);
	initStringInfo(&d2);
	draw_doc(tc, &d1);
	draw_doc(tc, &d2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(v1 || v2) >= greatest(length(v1), length(v2)) "
					 "AND length(v1 || v2) <= length(v1) + length(v2) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v1, "
					 "to_tsvector('simple', '%s') AS v2) s",
					 d1.data, d2.data);

	require_true(tc, conn, q.data);

	pfree(d1.data);
	pfree(d2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
