/*-------------------------------------------------------------------------
 *
 * prop_collate.c
 *	  Property-based tests for COLLATE clauses and ordering determinism.
 *
 * The oracle is the agreement between a collation's comparison and a known
 * reference order.  The "C" collation is defined to be raw byte order, so a
 * COLLATE "C" comparison must equal the bytea comparison of the UTF-8 bytes;
 * COLLATE "C" must also be a total order (exactly one of <, =, > holds) and
 * reflexive on equality.  For pure-ASCII input, "C" and "ucs_basic" are both
 * codepoint order and must agree.  An ORDER BY under a fixed collation must be
 * deterministic across identical runs.  Any disagreement is a bug in collation
 * handling or sort stability, not an artefact of the test.
 *
 * Only built-in collations guaranteed present everywhere are used ("C" and
 * "ucs_basic"), and all input is drawn from the ASCII alphabet [a-cA-C] with
 * strings of length 0..8, so every property is sound regardless of the host's
 * ICU/libc locale support.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_collate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "collate"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Alphabet and length bounds for every drawn string. */
#define COLLATE_ALPHABET "abcABC"
#define STR_MIN 0
#define STR_MAX 8

/*
 * Draw a string from [a-cA-C], length 0..8, and append it to buf as a
 * single-quoted SQL text literal.  The alphabet contains no single quote, so
 * no escaping is needed; double any quote defensively in case the alphabet
 * ever grows.
 */
static void
draw_str_literal(pg_prop_tc *tc, StringInfo buf)
{
	size_t		len = 0;
	char	   *s = pg_prop_draw_from(tc, COLLATE_ALPHABET, STR_MIN, STR_MAX, &len);

	appendStringInfoChar(buf, '\'');
	for (size_t i = 0; i < len; i++)
	{
		if (s[i] == '\'')
			appendStringInfoChar(buf, '\'');
		appendStringInfoChar(buf, s[i]);
	}
	appendStringInfoChar(buf, '\'');
	pg_free(s);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the collation comparison errored or produced NULL, which
 * is itself a failure, reported with the SQL.  (Copied in spirit from
 * prop_numeric.c's require_true.)
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
 * Differential result-set oracle: two queries must return identical rows in
 * identical order.  (Copied from the area_spec skeleton.)
 */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *q1, const char *q2)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *r1,
			   *r2;

	r1 = pg_prop_sql_rows(conn, q1, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "q1 failed: %s\n  SQL: %s", e1 ? e1 : "", q1);
	r2 = pg_prop_sql_rows(conn, q2, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "q2 failed: %s\n  SQL: %s", e2 ? e2 : "", q2);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc, "mismatch:\n  q1: %s\n  q2: %s\n  r1:\n%s\n  r2:\n%s",
					 q1, q2, r1, r2);
	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * C collation is byte order: for two strings S, T,
 *   (S COLLATE "C" < T COLLATE "C") = (convert_to(S,'UTF8') < convert_to(T,'UTF8'))
 * where the right side is a bytea comparison.  "C" is defined as raw byte
 * (memcmp) order, so the text comparison under "C" must match the UTF-8 byte
 * comparison exactly, for every pair.
 */
PG_PROP(c_collation_is_byte_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				t,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&t);
	draw_str_literal(tc, &s);
	draw_str_literal(tc, &t);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s COLLATE \"C\" < %s COLLATE \"C\") "
					 "= (convert_to(%s, 'UTF8') < convert_to(%s, 'UTF8'))",
					 s.data, t.data, s.data, t.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Total order: for two strings exactly one of <, =, > holds under COLLATE "C".
 * The three indicators cast to int must sum to exactly 1.
 */
PG_PROP(collate_total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				t,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&t);
	draw_str_literal(tc, &s);
	draw_str_literal(tc, &t);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s COLLATE \"C\" < %s COLLATE \"C\")::int "
					 "+ (%s COLLATE \"C\" = %s COLLATE \"C\")::int "
					 "+ (%s COLLATE \"C\" > %s COLLATE \"C\")::int) = 1",
					 s.data, t.data, s.data, t.data, s.data, t.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Reflexivity: a string equals itself under COLLATE "C".
 */
PG_PROP(collate_equal_reflexive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_str_literal(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT %s COLLATE \"C\" = %s COLLATE \"C\"",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ORDER BY under a fixed collation is deterministic: the same query, run
 * twice over the same VALUES list, must return identical rows in identical
 * order.  A small VALUES list is built from drawn strings; ORDER BY x COLLATE
 * "C" is a stable, total order over distinct byte strings but may tie on
 * duplicates, so a tie-break on the ordinal keeps the comparison a true
 * total order and the oracle sound.
 */
PG_PROP(order_by_collate_deterministic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData vals,
				q;
	int			nrows;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	nrows = (int) pg_prop_draw_int(tc, 1, 12);

	initStringInfo(&vals);
	for (int i = 0; i < nrows; i++)
	{
		if (i > 0)
			appendStringInfoString(&vals, ", ");
		appendStringInfoChar(&vals, '(');
		draw_str_literal(tc, &vals);
		appendStringInfo(&vals, ", %d)", i);
	}

	/*
	 * Order primarily by the collated string; break ties on the ordinal so
	 * the ordering is total and the two runs are legitimately required to
	 * match.  Select the ordinal too so a reordering is visible.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT x, n FROM (VALUES %s) v(x, n) "
					 "ORDER BY x COLLATE \"C\", n",
					 vals.data);

	require_same_rows(tc, conn, q.data, q.data);

	pfree(vals.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For pure-ASCII [a-cA-C] input, "C" and "ucs_basic" are both codepoint
 * order, so a strict-less comparison under one must equal the same comparison
 * under the other:
 *   (S COLLATE "C" < T COLLATE "C") = (S COLLATE "ucs_basic" < T COLLATE "ucs_basic").
 */
PG_PROP(ucs_basic_eq_c_for_ascii)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				t,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&t);
	draw_str_literal(tc, &s);
	draw_str_literal(tc, &t);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s COLLATE \"C\" < %s COLLATE \"C\") "
					 "= (%s COLLATE \"ucs_basic\" < %s COLLATE \"ucs_basic\")",
					 s.data, t.data, s.data, t.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Uppercase sorts before lowercase in byte order: under "C", 'A' < 'a'.
 * This is a fixed fact of ASCII (0x41 < 0x61), so it takes no drawn input;
 * it guards against a "C" collation that is not actually raw byte order.
 */
PG_PROP(upper_lower_collate_c)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn, "SELECT 'A' COLLATE \"C\" < 'a' COLLATE \"C\"");

	pg_prop_disconnect(conn);
}
