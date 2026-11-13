/*-------------------------------------------------------------------------
 *
 * prop_tsquery.c
 *	  Property-based tests for full-text search tsquery/tsvector.
 *
 * tsquery parsing/normalization and the @@ match operator have had fixes;
 * these check tsquery text round-trip and the boolean semantics of @@ against
 * generated tsvectors.
 *
 * Everything is built through the 'simple' text search configuration, so a
 * lowercase ascii word maps to itself as a lexeme -- no stemming, no
 * stopwords -- which makes the membership/boolean oracles below exact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_tsquery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "tsquery"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Tiny lexeme alphabet.  Four single-letter words keep the generated docs and
 * queries legible in a counterexample and, more importantly, make matches
 * common: with only four possible words a random query term is likely to be
 * present in a random document, so the boolean properties actually exercise
 * both the true and false sides of @@.
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
 * Draw a document: 0..6 space-separated lexemes, e.g. "a b a c".  The raw
 * words are emitted; the caller wraps them in to_tsvector('simple', '<D>').
 */
static void
draw_doc(pg_prop_tc *tc, StringInfo buf)
{
	int			nwords = (int) pg_prop_draw_int(tc, 0, 6);

	for (int i = 0; i < nwords; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ' ');
		appendStringInfoString(buf, draw_lexeme(tc));
	}
}

/*
 * Draw a boolean tsquery expression over the alphabet using & | ! and
 * parentheses, e.g. "a & (b | !c)".  Depth-limited and always parseable:
 * parens are balanced here by construction and no operator is ever left
 * dangling, so to_tsquery('simple', '<q>') always succeeds.  The result
 * always contains at least one lexeme, so it never normalizes to the empty
 * query (which would make @@ return NULL).
 */
static void
draw_query(pg_prop_tc *tc, StringInfo buf, int depth)
{
	int			kind;

	/*
	 * At the depth cap only a leaf is allowed, guaranteeing termination and a
	 * non-empty query.
	 */
	if (depth >= 3)
		kind = 0;
	else
		kind = (int) pg_prop_draw_int(tc, 0, 3);

	switch (kind)
	{
		case 0:					/* leaf lexeme */
			appendStringInfoString(buf, draw_lexeme(tc));
			break;
		case 1:					/* negation: ! <sub> */
			appendStringInfoString(buf, "!");
			draw_query(tc, buf, depth + 1);
			break;
		case 2:					/* conjunction: (<l> & <r>) */
			appendStringInfoChar(buf, '(');
			draw_query(tc, buf, depth + 1);
			appendStringInfoString(buf, " & ");
			draw_query(tc, buf, depth + 1);
			appendStringInfoChar(buf, ')');
			break;
		default:				/* disjunction: (<l> | <r>) */
			appendStringInfoChar(buf, '(');
			draw_query(tc, buf, depth + 1);
			appendStringInfoString(buf, " | ");
			draw_query(tc, buf, depth + 1);
			appendStringInfoChar(buf, ')');
			break;
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
 * tsvector text round-trip: a tsvector rendered to text and parsed back as a
 * tsvector equals the original.  Exercises tsvectorout/tsvectorin.
 */
PG_PROP(tsvector_text_roundtrip)
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
					 "SELECT (v::text::tsvector = v) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v) s",
					 d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * tsquery text round-trip: to_tsquery normalizes the input, and rendering the
 * normalized query to text and parsing it back is stable.  Exercises
 * tsqueryout/tsqueryin and the normalization in to_tsquery.
 */
PG_PROP(tsquery_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData qexpr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&qexpr);
	draw_query(tc, &qexpr, 0);

	initStringInfo(&q);

	/*
	 * Compare the TEXT, not the tsquery = operator.  tsquery equality is
	 * structural (it compares the serialized operator tree, including the
	 * grouping of & and |), and text output does not preserve that: a & (a &
	 * a) and (a & a) & a print identically but are unequal values.  The sound
	 * round-trip is that parsing the printed form and reprinting reproduces
	 * the text.
	 */
	appendStringInfo(&q,
					 "SELECT (t::text::tsquery::text = t::text) "
					 "FROM (SELECT to_tsquery('simple', '%s') AS t) s",
					 qexpr.data);

	require_true(tc, conn, q.data);

	pfree(qexpr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A single-lexeme query matches a document iff that word is among the
 * document's words.  With the 'simple' config each lowercase ascii word is
 * its own lexeme, so the oracle is independent: '<L>' = ANY of the words of D
 * split on spaces.  Exercises the @@ operator on a one-term query.
 */
PG_PROP(single_lexeme_match_is_membership)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *l;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);
	l = draw_lexeme(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (to_tsvector('simple', '%s') @@ to_tsquery('simple', '%s')) "
					 "= ('%s' = ANY (string_to_array('%s', ' ')))",
					 d.data, l, l, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Conjunction: (D @@ (L1 & L2)) = ((D @@ L1) AND (D @@ L2)).  The AND-query's
 * match equals the AND of the two single-lexeme matches.
 */
PG_PROP(and_is_conjunction)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *l1,
			   *l2;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);
	l1 = draw_lexeme(tc);
	l2 = draw_lexeme(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (v @@ to_tsquery('simple', '%s & %s')) "
					 "= ((v @@ to_tsquery('simple', '%s')) "
					 "AND (v @@ to_tsquery('simple', '%s'))) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v) s",
					 l1, l2, l1, l2, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Disjunction: (D @@ (L1 | L2)) = ((D @@ L1) OR (D @@ L2)).  The OR-query's
 * match equals the OR of the two single-lexeme matches.
 */
PG_PROP(or_is_disjunction)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *l1,
			   *l2;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);
	l1 = draw_lexeme(tc);
	l2 = draw_lexeme(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (v @@ to_tsquery('simple', '%s | %s')) "
					 "= ((v @@ to_tsquery('simple', '%s')) "
					 "OR (v @@ to_tsquery('simple', '%s'))) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v) s",
					 l1, l2, l1, l2, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Negation: (D @@ !L) = NOT (D @@ L).  The '!L' query (a valid tsquery
 * meaning "not containing L") matches iff the positive query does not.
 */
PG_PROP(not_is_negation)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *l;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);
	l = draw_lexeme(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (v @@ to_tsquery('simple', '!%s')) "
					 "= NOT (v @@ to_tsquery('simple', '%s')) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v) s",
					 l, l, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
