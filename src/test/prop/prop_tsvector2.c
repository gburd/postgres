/*-------------------------------------------------------------------------
 *
 * prop_tsvector2.c
 *	  Property-based tests for tsvector @@ matching and editing semantics.
 *
 * Where prop_tsvector.c checks structural invariants (length, concat bounds,
 * text round-trip), this area checks how tsvector operations interact with @@
 * matching: concatenation keeps the left operand's matches, strip() keeps
 * lexeme matching while dropping positions, setweight() does not change
 * matching, ts_delete() removes a lexeme's matches, and a document matches a
 * query for any word it contains.  A violation here is a bug in tsvector_op.c
 * (concat/setweight/strip/ts_delete) or in @@ evaluation.
 *
 * Everything is built through the 'simple' text search configuration, so a
 * lowercase ascii word maps to itself as a lexeme -- no stemming, no
 * stopwords -- which makes "word present in the doc" exactly "lexeme in the
 * tsvector" and keeps the oracles below exact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_tsvector2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "tsvector2"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Tiny lexeme alphabet.  Four single-letter words keep generated docs legible
 * in a counterexample and make duplicate words common, so concat dedup and
 * ts_delete overlap are actually exercised.
 */
static const char *const lexemes[] = {"a", "b", "c", "d"};
#define NLEX ((int) (sizeof(lexemes) / sizeof(lexemes[0])))
#define MAXWORDS 8

/*
 * Draw a non-empty document into buf (space-separated words) and record each
 * drawn word in words[].  Returns the word count (1..MAXWORDS).  The doc is
 * never empty so at least one lexeme is always present, which every property
 * below relies on when it picks a "present word" w = words[i].
 */
static int
draw_doc(pg_prop_tc *tc, StringInfo buf, const char *words[])
{
	int			nwords = (int) pg_prop_draw_int(tc, 1, MAXWORDS);

	for (int i = 0; i < nwords; i++)
	{
		words[i] = lexemes[pg_prop_draw_int(tc, 0, NLEX - 1)];
		if (i > 0)
			appendStringInfoChar(buf, ' ');
		appendStringInfoString(buf, words[i]);
	}
	return nwords;
}

/* Draw one of the words that is present in the just-built doc. */
static const char *
draw_present_word(pg_prop_tc *tc, const char *words[], int nwords)
{
	return words[pg_prop_draw_int(tc, 0, nwords - 1)];
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
 * Concatenation keeps the left operand's matches: for a word w present in V1,
 * (V1 || V2) still matches to_tsquery('simple', w).  Since w is drawn from the
 * words of D1, V1 @@ w holds, and || must not drop it.
 */
PG_PROP(concat_contains_both)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *words1[MAXWORDS];
	const char *words2[MAXWORDS];
	const char *w;
	int			nwords;
	StringInfoData d1,
				d2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d1);
	initStringInfo(&d2);
	nwords = draw_doc(tc, &d1, words1);
	(void) draw_doc(tc, &d2, words2);
	w = draw_present_word(tc, words1, nwords);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (to_tsvector('simple', '%s') "
					 "|| to_tsvector('simple', '%s')) "
					 "@@ to_tsquery('simple', '%s')",
					 d1.data, d2.data, w);

	require_true(tc, conn, q.data);

	pfree(d1.data);
	pfree(d2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * length() is non-negative and equals the distinct-lexeme count.  With the
 * 'simple' config each lowercase word maps 1:1 to a lexeme, so the length must
 * match a DISTINCT count of the raw words (filtering any empty token
 * defensively).
 */
PG_PROP(length_nonneg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *words[MAXWORDS];
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	(void) draw_doc(tc, &d, words);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(to_tsvector('simple', '%s')) >= 0 "
					 "AND length(to_tsvector('simple', '%s')) "
					 "= (SELECT count(DISTINCT w) "
					 "FROM unnest(string_to_array('%s', ' ')) w "
					 "WHERE w <> '')",
					 d.data, d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * strip() drops positions but keeps lexemes, so for a non-phrase query (a
 * single lexeme) strip(V) @@ Q matches exactly when V @@ Q.  The two sides are
 * compared for a word w drawn from the doc.
 */
PG_PROP(strip_removes_positions)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *words[MAXWORDS];
	const char *w;
	int			nwords;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	nwords = draw_doc(tc, &d, words);
	w = draw_present_word(tc, words, nwords);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (strip(to_tsvector('simple', '%s')) "
					 "@@ to_tsquery('simple', '%s')) "
					 "= (to_tsvector('simple', '%s') "
					 "@@ to_tsquery('simple', '%s'))",
					 d.data, w, d.data, w);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * setweight() relabels weights but does not change which lexemes match, so
 * setweight(V, 'A') @@ Q matches exactly when V @@ Q for a word w drawn from
 * the doc.
 */
PG_PROP(setweight_preserves_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *words[MAXWORDS];
	const char *w;
	int			nwords;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	nwords = draw_doc(tc, &d, words);
	w = draw_present_word(tc, words, nwords);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (setweight(to_tsvector('simple', '%s'), 'A') "
					 "@@ to_tsquery('simple', '%s')) "
					 "= (to_tsvector('simple', '%s') "
					 "@@ to_tsquery('simple', '%s'))",
					 d.data, w, d.data, w);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ts_delete() removes a lexeme: after deleting 'a', the vector no longer
 * matches to_tsquery('simple', 'a').  Holds for every doc (ts_delete of an
 * absent lexeme is a no-op, and the absent lexeme still does not match).
 */
PG_PROP(delete_removes_lexeme)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *words[MAXWORDS];
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	(void) draw_doc(tc, &d, words);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT NOT (ts_delete(to_tsvector('simple', '%s'), 'a') "
					 "@@ to_tsquery('simple', 'a'))",
					 d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concatenation length is bounded by the sum of operand lengths; dedup of
 * shared lexemes can only reduce it.  length(V1 || V2) <= length(V1) +
 * length(V2).
 */
PG_PROP(concat_length_bound)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *words[MAXWORDS];
	StringInfoData d1,
				d2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d1);
	initStringInfo(&d2);
	(void) draw_doc(tc, &d1, words);
	(void) draw_doc(tc, &d2, words);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(v1 || v2) <= length(v1) + length(v2) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v1, "
					 "to_tsvector('simple', '%s') AS v2) s",
					 d1.data, d2.data);

	require_true(tc, conn, q.data);

	pfree(d1.data);
	pfree(d2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Self-match: a document matches a single-lexeme query for any word it
 * contains.  w is drawn from the doc's own words, so V @@ to_tsquery(w) must
 * hold.
 */
PG_PROP(self_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *words[MAXWORDS];
	const char *w;
	int			nwords;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	nwords = draw_doc(tc, &d, words);
	w = draw_present_word(tc, words, nwords);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_tsvector('simple', '%s') "
					 "@@ to_tsquery('simple', '%s')",
					 d.data, w);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
