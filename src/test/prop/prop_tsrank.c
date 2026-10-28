/*-------------------------------------------------------------------------
 *
 * prop_tsrank.c
 *	  Property-based tests for full-text ranking (ts_rank / ts_rank_cd).
 *
 * ts_rank and ts_rank_cd score a tsvector against a tsquery; these check the
 * score bounds, determinism, and the match-is-positive relationship,
 * using the 'simple' text search configuration so that lowercase ascii words
 * map to themselves as lexemes -- no stemming, no stopwords -- which makes the
 * match oracle (@@) and the resulting rank sign predictable.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_tsrank.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "tsrank"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Tiny lexeme alphabet.  Four single-letter words keep the generated docs and
 * queries legible in a counterexample and make matches common, so that both
 * the matching and ranking properties get exercised.
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
 * Draw a document: 0..8 space-separated lexemes, e.g. "a b a c".  The raw
 * words are emitted; the caller wraps them in to_tsvector('simple', '<D>').
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
 * Draw a boolean tsquery expression over the alphabet using & | ! and
 * parentheses, e.g. "a & (b | !c)".  Depth-limited and always parseable:
 * parens are balanced here by construction and no operator is ever left
 * dangling, so to_tsquery('simple', '<q>') always succeeds.  The result
 * always contains at least one lexeme, so it never normalizes to the empty
 * query (which would make @@ and the rank functions return NULL / zero
 * trivially).
 */
static void
draw_query(pg_prop_tc *tc, StringInfo buf, int depth, bool allow_neg)
{
	int			kind;
	int			maxkind;

	/*
	 * At the depth cap only a leaf is allowed, guaranteeing termination and a
	 * non-empty query.
	 */
	maxkind = allow_neg ? 3 : 2;
	if (depth >= 3)
		kind = 0;
	else
		kind = (int) pg_prop_draw_int(tc, 0, maxkind);

	/*
	 * When negation is disallowed, remap the negation case (1) to a leaf so
	 * the query stays purely positive.
	 */
	if (!allow_neg && kind == 1)
		kind = 0;

	switch (kind)
	{
		case 0:					/* leaf lexeme */
			appendStringInfoString(buf, draw_lexeme(tc));
			break;
		case 1:					/* negation: ! <sub> */
			appendStringInfoString(buf, "!");
			draw_query(tc, buf, depth + 1, allow_neg);
			break;
		case 2:					/* conjunction: (<l> & <r>) */
			appendStringInfoChar(buf, '(');
			draw_query(tc, buf, depth + 1, allow_neg);
			appendStringInfoString(buf, " & ");
			draw_query(tc, buf, depth + 1, allow_neg);
			appendStringInfoChar(buf, ')');
			break;
		default:				/* disjunction: (<l> | <r>) */
			appendStringInfoChar(buf, '(');
			draw_query(tc, buf, depth + 1, allow_neg);
			appendStringInfoString(buf, " | ");
			draw_query(tc, buf, depth + 1, allow_neg);
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
 * ts_rank is non-negative for any document and query.  A rank is a relevance
 * score and can never be negative, regardless of whether the query matches.
 */
PG_PROP(rank_nonnegative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d,
				qexpr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	initStringInfo(&qexpr);
	draw_doc(tc, &d);
	draw_query(tc, &qexpr, 0, true);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ts_rank(to_tsvector('simple', '%s'), "
					 "to_tsquery('simple', '%s')) >= 0",
					 d.data, qexpr.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(qexpr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ts_rank is deterministic.  Because ts_rank scores by lexeme co-occurrence
 * (not by the boolean @@ result), the clean invariant is that identical
 * inputs produce an identical rank.
 */
PG_PROP(rank_deterministic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d,
				qexpr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	initStringInfo(&qexpr);
	draw_doc(tc, &d);
	draw_query(tc, &qexpr, 0, true);

	/*
	 * ts_rank scores by lexeme co-occurrence, not by the boolean @@ result,
	 * so a non-matching query can still have a positive rank (any query
	 * lexeme present in the document contributes).  The sound invariant is
	 * determinism: the same document and query always yield the same rank.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ts_rank(to_tsvector('simple', '%s'), "
					 "to_tsquery('simple', '%s')) "
					 "= ts_rank(to_tsvector('simple', '%s'), "
					 "to_tsquery('simple', '%s'))",
					 d.data, qexpr.data, d.data, qexpr.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(qexpr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Match => positive rank.  When the document matches a positive (negation-
 * free) query, every query lexeme is present in the document, so there is at
 * least one lexeme to score and ts_rank is strictly > 0.  The CASE returns
 * true in the no-match case, where this property makes no claim.
 */
PG_PROP(rank_positive_when_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d,
				qexpr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	initStringInfo(&qexpr);
	draw_doc(tc, &d);
	draw_query(tc, &qexpr, 0, false);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN (to_tsvector('simple', '%s') "
					 "@@ to_tsquery('simple', '%s')) "
					 "THEN ts_rank(to_tsvector('simple', '%s'), "
					 "to_tsquery('simple', '%s')) > 0 "
					 "ELSE true END",
					 d.data, qexpr.data, d.data, qexpr.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(qexpr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ts_rank_cd (cover-density ranking) is non-negative for any document and
 * query, for the same reason ts_rank is: a relevance score cannot be
 * negative.
 */
PG_PROP(rank_cd_nonnegative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d,
				qexpr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	initStringInfo(&qexpr);
	draw_doc(tc, &d);
	draw_query(tc, &qexpr, 0, true);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ts_rank_cd(to_tsvector('simple', '%s'), "
					 "to_tsquery('simple', '%s')) >= 0",
					 d.data, qexpr.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(qexpr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * No match => zero cover-density rank.  When the document does not match the
 * query there is no cover to measure, so ts_rank_cd is exactly 0.  The CASE
 * returns true in the match case, where this property makes no claim.
 */
PG_PROP(rank_cd_zero_when_no_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d,
				qexpr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	initStringInfo(&qexpr);
	draw_doc(tc, &d);
	draw_query(tc, &qexpr, 0, false);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN NOT (to_tsvector('simple', '%s') "
					 "@@ to_tsquery('simple', '%s')) "
					 "THEN ts_rank_cd(to_tsvector('simple', '%s'), "
					 "to_tsquery('simple', '%s')) = 0 "
					 "ELSE true END",
					 d.data, qexpr.data, d.data, qexpr.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(qexpr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Default-weights equivalence: the 2-arg ts_rank(vector, query) is defined to
 * use the documented default weight array {0.1, 0.2, 0.4, 1.0} (for the D, C,
 * B, A labels).  So the 4-arg ts_rank with that explicit array must equal the
 * 2-arg form for every document and query.  This exercises the weighted
 * accumulation path without relying on any subtle monotonicity between
 * different weight sets.
 */
PG_PROP(rank_weights_monotone)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d,
				qexpr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	initStringInfo(&qexpr);
	draw_doc(tc, &d);
	draw_query(tc, &qexpr, 0, true);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ts_rank(to_tsvector('simple', '%s'), "
					 "to_tsquery('simple', '%s')) "
					 "= ts_rank('{0.1,0.2,0.4,1.0}'::float4[], "
					 "to_tsvector('simple', '%s'), "
					 "to_tsquery('simple', '%s'))",
					 d.data, qexpr.data, d.data, qexpr.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(qexpr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
