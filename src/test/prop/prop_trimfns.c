/*-------------------------------------------------------------------------
 *
 * prop_trimfns.c
 *	  Property-based tests for the SQL TRIM(...) syntactic forms.
 *
 * The SQL standard TRIM(LEADING|TRAILING|BOTH <chars> FROM <s>) is parsed
 * into the btrim/ltrim/rtrim built-ins.  These properties pin down the three
 * directional variants and the two-forms-must-agree equivalence between the
 * TRIM syntax and the function-call built-ins: TRIM BOTH = btrim, TRIM
 * LEADING = ltrim, TRIM TRAILING = rtrim.  An oracle built from a known-
 * trimmable string (an 'x' sentinel not in the body alphabet) plus the
 * syntax-vs-function differential make every assertion sound.  A mismatch is
 * a parse-analysis or built-in bug, not a rounding artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_trimfns.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "trimfns"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Body alphabet.  'x' is deliberately excluded so it can serve as a trim
 * sentinel that never collides with the body content.
 */
#define TRIM_ALPHABET "abc"

/*
 * Draw a quoted SQL string literal over the body alphabet [abc] into buf.
 * The alphabet has no single quotes, so no escaping is needed.  Length 0..12.
 */
static void
draw_body(pg_prop_tc *tc, StringInfo buf)
{
	size_t		len = 0;
	char	   *s = pg_prop_draw_from(tc, TRIM_ALPHABET, 0, 12, &len);

	appendStringInfoChar(buf, '\'');
	appendBinaryStringInfo(buf, s, (int) len);
	appendStringInfoChar(buf, '\'');
	free(s);
}

/*
 * Draw a quoted SQL string literal that interleaves 'x' sentinels with body
 * characters from [abc], so trims have runs to bite on at both ends and in
 * the middle.  Each position is independently either an 'x' or a body char.
 * This produces the general S2 used by the equivalence properties.
 */
static void
draw_mixed(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	appendStringInfoChar(buf, '\'');
	for (int i = 0; i < n; i++)
	{
		if (pg_prop_draw_bool(tc))
			appendStringInfoChar(buf, 'x');
		else
			appendStringInfoChar(buf,
								 TRIM_ALPHABET[pg_prop_draw_int(tc, 0, 2)]);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the trim errored or produced NULL on finite text inputs,
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
 * TRIM(BOTH ' ' FROM '  ' || S || '  ') = S.  S is over [abc] (no spaces), so
 * the two leading and two trailing spaces are exactly what default trim must
 * strip, recovering S.  (S cannot be over-trimmed: it holds no spaces.)
 */
PG_PROP(trim_both_default)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_body(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT trim(BOTH ' ' FROM '  ' || %s || '  ') = %s",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * TRIM(LEADING 'x' FROM 'xxx' || S) = S.  S is over [abc] so it never starts
 * with 'x'; the three prepended sentinels are exactly what leading trim must
 * strip, leaving S untouched.
 */
PG_PROP(trim_leading)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_body(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT trim(LEADING 'x' FROM 'xxx' || %s) = %s",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * TRIM(TRAILING 'x' FROM S || 'xxx') = S.  S is over [abc] so it never ends
 * with 'x'; the three appended sentinels are exactly what trailing trim must
 * strip, leaving S untouched.
 */
PG_PROP(trim_trailing)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_body(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT trim(TRAILING 'x' FROM %s || 'xxx') = %s",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * TRIM(BOTH 'x' FROM 'xx' || S || 'xx') = S.  S is over [abc] so it has no
 * 'x' at either edge; the two prepended and two appended sentinels are
 * exactly what both-ends trim with the explicit 'x' set must strip,
 * recovering S.
 */
PG_PROP(trim_both_chars)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_body(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT trim(BOTH 'x' FROM 'xx' || %s || 'xx') = %s",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * btrim(S2, 'x') = TRIM(BOTH 'x' FROM S2) for an arbitrary mixed S2.  The two
 * spellings of both-ends trimming must agree on every input; S2 interleaves
 * 'x' sentinels with body chars so there are edge and interior runs to test.
 */
PG_PROP(btrim_eq_trim_both)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s2);
	draw_mixed(tc, &s2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT btrim(%s, 'x') = trim(BOTH 'x' FROM %s)",
					 s2.data, s2.data);

	require_true(tc, conn, q.data);

	pfree(s2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ltrim(S2, 'x') = TRIM(LEADING 'x' FROM S2) for an arbitrary mixed S2.  The
 * function and syntax forms of leading trim must agree on every input.
 */
PG_PROP(ltrim_eq_trim_leading)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s2);
	draw_mixed(tc, &s2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ltrim(%s, 'x') = trim(LEADING 'x' FROM %s)",
					 s2.data, s2.data);

	require_true(tc, conn, q.data);

	pfree(s2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * rtrim(S2, 'x') = TRIM(TRAILING 'x' FROM S2) for an arbitrary mixed S2.  The
 * function and syntax forms of trailing trim must agree on every input.
 */
PG_PROP(rtrim_eq_trim_trailing)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s2);
	draw_mixed(tc, &s2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT rtrim(%s, 'x') = trim(TRAILING 'x' FROM %s)",
					 s2.data, s2.data);

	require_true(tc, conn, q.data);

	pfree(s2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * btrim is idempotent: btrim(btrim(S2, 'x'), 'x') = btrim(S2, 'x').  A single
 * both-ends trim already removes every leading and trailing 'x', so a second
 * pass must be a no-op.  S2 is an arbitrary mixed string.
 */
PG_PROP(trim_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s2);
	draw_mixed(tc, &s2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT btrim(btrim(%s, 'x'), 'x') = btrim(%s, 'x')",
					 s2.data, s2.data);

	require_true(tc, conn, q.data);

	pfree(s2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
