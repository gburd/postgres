/*-------------------------------------------------------------------------
 *
 * prop_textord.c
 *	  Property-based tests for text comparison and ordering under COLLATE "C".
 *
 * The oracle is the algebra of a total order: under the byte-wise C collation,
 * text comparison is a strict total order on strings (trichotomy, reflexivity,
 * transitivity), the empty string is least, lexicographic order ignores length
 * (shorter is not automatically smaller), and appending a byte never decreases
 * a string's rank (strictly increases it).  Every comparison forces COLLATE
 * "C" so the result is deterministic and locale-independent; a broken identity
 * here is a bug in C-collation text comparison, not a locale artefact.
 *
 * Distinct from the collate / textcase areas: those cover collation selection
 * and case folding.  This area asserts only the ordering laws of COLLATE "C".
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_textord.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "textord"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our alphabet is [a-c] and never contains a quote, but do it
 * defensively so the generated SQL is always well-formed.
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
 * Draw 0..8 letters from [a-c] into buf as a bare string.  All single-byte,
 * never a quote, so quoting is trivial and the byte-wise C ordering is exactly
 * the ordering of these ASCII code points.
 */
static void
draw_letters(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 8);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the comparison errored or produced NULL on well-formed
 * [a-c] strings, which is itself a bug -- so either is a failure, reported
 * with the SQL.  Modelled on prop_numeric.c's require_true.
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
 * Trichotomy: for two strings S, T, exactly one of (<, =, >) holds under
 * COLLATE "C".  Counting the three mutually exclusive comparisons must sum to
 * exactly 1 -- a total order never leaves two strings incomparable or
 * simultaneously in two relations.
 */
PG_PROP(total_order_c)
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
	draw_letters(tc, &s);
	draw_letters(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" < ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, " COLLATE \"C\")::int + (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" = ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, " COLLATE \"C\")::int + (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" > ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, " COLLATE \"C\")::int = 1");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Prefix is <= its extension: S COLLATE "C" <= (S || 'a') COLLATE "C".  Under
 * C collation a string is never greater than itself with a byte appended --
 * either S is a proper prefix of (S || 'a') (so strictly less), or they are
 * equal only if the appended byte produced an identical string, which it
 * cannot since one is strictly longer; so <= always holds.
 */
PG_PROP(prefix_le)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" <= (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || 'a') COLLATE \"C\"");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The empty string is least: '' COLLATE "C" <= S COLLATE "C" for any S.  The
 * empty string is a prefix of every string, so it is never greater than any
 * string under C collation.
 */
PG_PROP(empty_least)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT '' COLLATE \"C\" <= ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\"");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Appending a byte strictly increases: (S || 'x') COLLATE "C" > S COLLATE "C".
 * S is a proper prefix of (S || 'x'), and under C collation a proper prefix is
 * always strictly less than its extension, so the extension is strictly
 * greater.  ('x' is outside [a-c] but still a plain single-byte ASCII letter,
 * which keeps the comparison byte-wise and quote-free.)
 */
PG_PROP(concat_monotone_prefix)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || 'x') COLLATE \"C\" > ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\"");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Ordering is lexicographic, not by length: 'b' COLLATE "C" > 'aa' COLLATE
 * "C".  A length-based order would rank the one-byte 'b' below the two-byte
 * 'aa'; C collation compares byte-wise, where 'b' (0x62) exceeds 'a' (0x61) at
 * the first byte, so 'b' is greater regardless of being shorter.  Constant
 * oracle -- no inputs drawn.
 */
PG_PROP(length_not_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT 'b' COLLATE \"C\" > 'aa' COLLATE \"C\"");

	pg_prop_disconnect(conn);
}

/*
 * Reflexivity: S COLLATE "C" = S, and S <= S, and S >= S.  Every string equals
 * itself and is both <= and >= itself under any total order.
 */
PG_PROP(reflexive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_letters(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" = ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" AND ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" <= ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" AND ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" >= ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\"");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Transitivity: for S, T, U, NOT (S < T AND T < U) OR (S < U) -- i.e. S < T
 * and T < U together imply S < U.  A total order is transitive; a comparison
 * bug that broke this would make sorts and index scans inconsistent.
 */
PG_PROP(transitivity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				t,
				u,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&t);
	initStringInfo(&u);
	draw_letters(tc, &s);
	draw_letters(tc, &t);
	draw_letters(tc, &u);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT NOT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" < ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, " COLLATE \"C\" AND ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, " COLLATE \"C\" < ");
	append_sql_literal(&q, u.data);
	appendStringInfoString(&q, " COLLATE \"C\") OR (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\" < ");
	append_sql_literal(&q, u.data);
	appendStringInfoString(&q, " COLLATE \"C\")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(u.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * least/greatest consistency: least(S, T) COLLATE "C" <= greatest(S, T)
 * COLLATE "C".  least and greatest pick the min and max under the same C
 * collation, so the chosen min can never exceed the chosen max.
 */
PG_PROP(min_max_consistency)
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
	draw_letters(tc, &s);
	draw_letters(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT least(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, " COLLATE \"C\") COLLATE \"C\" <= greatest(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " COLLATE \"C\", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, " COLLATE \"C\") COLLATE \"C\"");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
