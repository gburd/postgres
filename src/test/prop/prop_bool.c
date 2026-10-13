/*-------------------------------------------------------------------------
 *
 * prop_bool.c
 *	  Property-based tests for boolean type input/output.
 *
 * boolean input (boolin, src/backend/utils/adt/bool.c) accepts many spellings
 * (t/true/yes/on/1 and f/false/no/off/0, case-insensitive, with surrounding
 * whitespace) and must map them consistently; these check that tolerance and
 * boolean algebra.  The oracle is the type's own semantics: every accepted
 * true spelling must equal true, every false spelling false, output is
 * canonical 'true'/'false', and the usual laws (double negation, De Morgan,
 * the false < true total order) hold.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_bool.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "bool"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * The documented true-ish spellings, case-insensitive: true, t, yes, y, on, 1.
 * Mixed case and a couple of whitespace-padded forms are included to exercise
 * boolin's case folding and leading/trailing whitespace trimming.  None
 * contain a single quote, so they embed into a '...' literal unescaped.
 */
static const char *const true_spellings[] = {
	"t", "T", "true", "TRUE", "True",
	"yes", "YES", "y", "on", "ON", "1",
	" true ", " 1 "
};

/*
 * The documented false-ish spellings, case-insensitive: false, f, no, n, off,
 * 0.  Same mixed-case and whitespace coverage as the true set.
 */
static const char *const false_spellings[] = {
	"f", "F", "false", "FALSE", "False",
	"no", "NO", "n", "off", "OFF", "0",
	" false ", " 0 "
};

#define N_TRUE_SPELLINGS	((int) (sizeof(true_spellings) / sizeof(true_spellings[0])))
#define N_FALSE_SPELLINGS	((int) (sizeof(false_spellings) / sizeof(false_spellings[0])))

/*
 * Draw one accepted spelling of "true".
 */
static const char *
draw_true_spelling(pg_prop_tc *tc)
{
	int			i = (int) pg_prop_draw_int(tc, 0, N_TRUE_SPELLINGS - 1);

	return true_spellings[i];
}

/*
 * Draw one accepted spelling of "false".
 */
static const char *
draw_false_spelling(pg_prop_tc *tc)
{
	int			i = (int) pg_prop_draw_int(tc, 0, N_FALSE_SPELLINGS - 1);

	return false_spellings[i];
}

/*
 * Draw a boolean literal: coin-flip the truth value, pick a spelling of it,
 * and emit it into buf as a single-quoted SQL literal '<spelling>'.  The
 * spellings contain only letters, digits and spaces -- no quotes -- so no
 * escaping is needed.  *expected receives the C bool the literal denotes.
 */
static void
draw_bool_literal(pg_prop_tc *tc, StringInfo buf, bool *expected)
{
	bool		want = pg_prop_draw_bool(tc);
	const char *spelling = want ? draw_true_spelling(tc) : draw_false_spelling(tc);

	appendStringInfoChar(buf, '\'');
	appendStringInfoString(buf, spelling);
	appendStringInfoChar(buf, '\'');

	*expected = want;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means boolin rejected an accepted spelling or the algebra
 * produced NULL on non-NULL inputs, which is itself a bug -- so either is a
 * failure, reported with the SQL.  Modelled on prop_numeric.c's require_true.
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
 * Every accepted true spelling parses to true.
 */
PG_PROP(true_spellings_parse_true)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *spelling;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	spelling = draw_true_spelling(tc);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT '%s'::boolean = true", spelling);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Every accepted false spelling parses to false.
 */
PG_PROP(false_spellings_parse_false)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *spelling;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	spelling = draw_false_spelling(tc);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT '%s'::boolean = false", spelling);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Text round-trip: boolean output is canonical 'true'/'false' and matches the
 * parsed value.  Draw a literal with a known expected bool and compare its
 * ::text rendering to the canonical word for that bool.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData b,
				q;
	bool		expected = false;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&b);
	draw_bool_literal(tc, &b, &expected);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s::boolean)::text = '%s'",
					 b.data, expected ? "true" : "false");

	require_true(tc, conn, q.data);

	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Double negation is identity: NOT (NOT B) = B.
 */
PG_PROP(not_involution)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData b,
				q;
	bool		expected = false;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&b);
	draw_bool_literal(tc, &b, &expected);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (NOT (NOT (%s::boolean))) = (%s::boolean)",
					 b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * De Morgan over the boolean type: NOT (P AND Q) = (NOT P) OR (NOT Q).
 */
PG_PROP(and_or_demorgan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData p,
				qq,
				q;
	bool		ep = false,
				eq = false;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&p);
	initStringInfo(&qq);
	draw_bool_literal(tc, &p, &ep);
	draw_bool_literal(tc, &qq, &eq);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (NOT ((%s::boolean) AND (%s::boolean))) "
					 "= ((NOT (%s::boolean)) OR (NOT (%s::boolean)))",
					 p.data, qq.data, p.data, qq.data);

	require_true(tc, conn, q.data);

	pfree(p.data);
	pfree(qq.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * boolin is case-insensitive: a true spelling parses the same whether upper-
 * or lower-cased.  (The spellings are already true-ish; folding case must not
 * change the parse.)
 */
PG_PROP(case_insensitive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *spelling;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	spelling = draw_true_spelling(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (upper('%s')::boolean) = (lower('%s')::boolean)",
					 spelling, spelling);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The boolean total order: false < true, not true < false, and false <> true.
 * A fixed check with no draws -- it anchors the ordering operators.
 */
PG_PROP(comparison_consistent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (false < true) AND NOT (true < false) "
				 "AND (false <> true)");

	pg_prop_disconnect(conn);
}
