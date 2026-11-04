/*-------------------------------------------------------------------------
 *
 * prop_tsquerylaw.c
 *	  Property-based tests for the boolean algebra of tsquery under @@.
 *
 * These check that the tsquery boolean connectives obey the laws of boolean
 * algebra when matched against a tsvector: & and | commute, an AND match
 * implies the corresponding OR match, ! negates, and de Morgan relates the
 * negation of a conjunction to the disjunction of the negations.  The oracle
 * is boolean algebra itself -- these equalities hold for every document and
 * every pair of lexemes -- so any "f" is a bug in tsquery normalization or in
 * the @@ executor, not an input artefact.  Distinct from prop_tsquery (which
 * decomposes @@ against SQL AND/OR), prop_tsvector/prop_tsrank.
 *
 * Everything is built through the 'simple' configuration, so each lowercase
 * ascii word is its own lexeme (no stemming, no stopwords), which keeps the
 * laws exact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see pg_prop.h
 * and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_tsquerylaw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "tsquerylaw"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Tiny lexeme alphabet.  Four single-letter words keep counterexamples
 * legible and make matches common: with only four possible words a random
 * query term is likely present in a random document, so the laws exercise
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
 * Commutativity of &: V @@ (a & b) = V @@ (b & a) for a drawn document V and
 * drawn lexemes a, b.
 */
PG_PROP(and_commutative_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *a,
			   *b;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);
	a = draw_lexeme(tc);
	b = draw_lexeme(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (v @@ to_tsquery('simple', '%s & %s')) "
					 "= (v @@ to_tsquery('simple', '%s & %s')) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v) s",
					 a, b, b, a, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Commutativity of |: V @@ (a | b) = V @@ (b | a).
 */
PG_PROP(or_commutative_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *a,
			   *b;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);
	a = draw_lexeme(tc);
	b = draw_lexeme(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (v @@ to_tsquery('simple', '%s | %s')) "
					 "= (v @@ to_tsquery('simple', '%s | %s')) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v) s",
					 a, b, b, a, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * AND implies OR: whenever V @@ (a & b), also V @@ (a | b).  A conjunction
 * match is strictly stronger than the matching disjunction, so the material
 * implication (NOT and) OR (or) must hold.
 */
PG_PROP(and_implies_or)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *a,
			   *b;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);
	a = draw_lexeme(tc);
	b = draw_lexeme(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (NOT (v @@ to_tsquery('simple', '%s & %s'))) "
					 "OR (v @@ to_tsquery('simple', '%s | %s')) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v) s",
					 a, b, a, b, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Negation: V @@ (!a) = NOT (V @@ a).  The '!a' query matches iff a is absent
 * from the document, which is exactly the negation of the positive match.
 */
PG_PROP(not_negates)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *a;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);
	a = draw_lexeme(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (v @@ to_tsquery('simple', '!%s')) "
					 "= NOT (v @@ to_tsquery('simple', '%s')) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v) s",
					 a, a, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * de Morgan: V @@ (!(a & b)) = V @@ ((!a) | (!b)).  Negating a conjunction is
 * the disjunction of the negations; both sides must match the same documents.
 */
PG_PROP(de_morgan_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *a,
			   *b;
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_doc(tc, &d);
	a = draw_lexeme(tc);
	b = draw_lexeme(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (v @@ to_tsquery('simple', '!(%s & %s)')) "
					 "= (v @@ to_tsquery('simple', '(!%s) | (!%s)')) "
					 "FROM (SELECT to_tsvector('simple', '%s') AS v) s",
					 a, b, a, b, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Single-lexeme match, constant anchor: 'a b' @@ 'a' is true and 'a b' @@ 'c'
 * is false.  A fixed positive and negative case pins the @@ operator so the
 * boolean laws above are not all vacuously satisfied by a @@ that always
 * returns the same value.
 */
PG_PROP(single_lexeme_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (to_tsvector('simple', 'a b') "
				 "@@ to_tsquery('simple', 'a')) "
				 "AND NOT (to_tsvector('simple', 'a b') "
				 "@@ to_tsquery('simple', 'c'))");

	pg_prop_disconnect(conn);
}

/*
 * tsquery text round-trip: a normalized tsquery rendered to text and parsed
 * back equals the original.  Fixed 'a & b' here; the structural = holds
 * because the printed form reparses to the same operator tree.
 */
PG_PROP(tsquery_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (to_tsquery('simple', 'a & b')::text::tsquery "
				 "= to_tsquery('simple', 'a & b'))");

	pg_prop_disconnect(conn);
}

/*
 * Empty document never positively matches: to_tsvector('simple','') @@
 * to_tsquery('simple','a') is false.  An empty tsvector contains no lexemes,
 * so a positive single-term query cannot match.
 */
PG_PROP(empty_doc_no_positive_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT NOT (to_tsvector('simple', '') "
				 "@@ to_tsquery('simple', 'a'))");

	pg_prop_disconnect(conn);
}
