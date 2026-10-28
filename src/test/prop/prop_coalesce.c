/*-------------------------------------------------------------------------
 *
 * prop_coalesce.c
 *	  Property-based tests for PostgreSQL's NULL-handling expressions.
 *
 * COALESCE, NULLIF, GREATEST, LEAST and CASE each have precise, specified
 * semantics: COALESCE returns the first non-NULL argument; NULLIF returns NULL
 * when its two arguments are equal (by SQL = semantics) and the first argument
 * otherwise; GREATEST and LEAST skip NULL arguments and return NULL only when
 * every argument is NULL; a searched CASE yields the result of its first true
 * WHEN, or the ELSE (NULL absent) when none matches.  These properties pin
 * those semantics down against explicit CASE / aggregate oracles.
 *
 * The oracle is an independent reconstruction of each operator's definition:
 * a hand-written searched CASE for COALESCE, the = operator for NULLIF, and
 * the max()/min() aggregates (which skip NULLs just like GREATEST/LEAST) over
 * a VALUES list.  Every result comparison uses IS NOT DISTINCT FROM so a NULL
 * result on both sides compares equal; the NULLIF oracle uses plain = to match
 * NULLIF's own equality semantics.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_coalesce.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "coalesce"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw one argument into buf: with probability ~1/4 the literal SQL NULL,
 * otherwise a small integer in [-50, 50].  Every argument is cast to ::int so
 * the expression's type resolves even when all arguments are NULL (otherwise
 * COALESCE/GREATEST/LEAST over bare NULLs raise "could not determine type of
 * NULL").
 */
static void
draw_arg(pg_prop_tc *tc, StringInfo buf)
{
	bool		is_null = (pg_prop_draw_int(tc, 0, 3) == 0);

	if (is_null)
		appendStringInfoString(buf, "NULL::int");
	else
		appendStringInfo(buf, "(%d)::int",
						 (int) pg_prop_draw_int(tc, -50, 50));
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the expression did not behave as the oracle demands,
 * which is itself a failure, reported with the SQL.
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
 * COALESCE returns the first non-NULL argument.  An explicit searched CASE
 * with one WHEN ... IS NOT NULL per argument computes exactly that; the two
 * must agree.  IS NOT DISTINCT FROM makes the all-NULL case (both sides NULL)
 * pass.
 */
PG_PROP(coalesce_first_nonnull)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a1,
				a2,
				a3,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a1);
	initStringInfo(&a2);
	initStringInfo(&a3);
	draw_arg(tc, &a1);
	draw_arg(tc, &a2);
	draw_arg(tc, &a3);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(%s, %s, %s) IS NOT DISTINCT FROM "
					 "CASE WHEN %s IS NOT NULL THEN %s "
					 "WHEN %s IS NOT NULL THEN %s "
					 "WHEN %s IS NOT NULL THEN %s END",
					 a1.data, a2.data, a3.data,
					 a1.data, a1.data,
					 a2.data, a2.data,
					 a3.data, a3.data);

	require_true(tc, conn, q.data);

	pfree(a1.data);
	pfree(a2.data);
	pfree(a3.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * COALESCE is idempotent and absorbs a leading NULL: coalesce(a, a) = a and
 * coalesce(NULL, a) = a, for any a (including a = NULL, handled by IS NOT
 * DISTINCT FROM).
 */
PG_PROP(coalesce_idempotent_absorb)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arg(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (coalesce(%s, %s) IS NOT DISTINCT FROM %s) "
					 "AND (coalesce(NULL::int, %s) IS NOT DISTINCT FROM %s)",
					 a.data, a.data, a.data,
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * NULLIF(a, b) is NULL when a = b and a otherwise.  The oracle uses plain =
 * (not IS NOT DISTINCT FROM) because NULLIF decides on SQL equality: if a or b
 * is NULL then a = b is NULL, the CASE WHEN is not true, and NULLIF returns a
 * -- which this oracle reproduces exactly.
 */
PG_PROP(nullif_semantics)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_arg(tc, &a);
	draw_arg(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT nullif(%s, %s) IS NOT DISTINCT FROM "
					 "CASE WHEN %s = %s THEN NULL ELSE %s END",
					 a.data, b.data,
					 a.data, b.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * GREATEST and LEAST skip NULL arguments and return NULL only when every
 * argument is NULL.  The max()/min() aggregates over a VALUES list of the same
 * arguments skip NULLs identically, so they are an exact independent oracle.
 */
PG_PROP(greatest_least_skip_nulls)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a1,
				a2,
				a3,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a1);
	initStringInfo(&a2);
	initStringInfo(&a3);
	draw_arg(tc, &a1);
	draw_arg(tc, &a2);
	draw_arg(tc, &a3);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT greatest(%s, %s, %s) IS NOT DISTINCT FROM "
					 "(SELECT max(v) FROM (VALUES (%s), (%s), (%s)) t(v)) "
					 "AND least(%s, %s, %s) IS NOT DISTINCT FROM "
					 "(SELECT min(v) FROM (VALUES (%s), (%s), (%s)) t(v))",
					 a1.data, a2.data, a3.data,
					 a1.data, a2.data, a3.data,
					 a1.data, a2.data, a3.data,
					 a1.data, a2.data, a3.data);

	require_true(tc, conn, q.data);

	pfree(a1.data);
	pfree(a2.data);
	pfree(a3.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * When at least one argument is non-NULL, GREATEST >= LEAST.  All-NULL yields
 * NULL on both sides, the comparison is NULL, and coalesce(..., true) makes
 * that case pass -- so the property holds unconditionally.
 */
PG_PROP(greatest_ge_least)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a1,
				a2,
				a3,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a1);
	initStringInfo(&a2);
	initStringInfo(&a3);
	draw_arg(tc, &a1);
	draw_arg(tc, &a2);
	draw_arg(tc, &a3);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(greatest(%s, %s, %s) >= "
					 "least(%s, %s, %s), true)",
					 a1.data, a2.data, a3.data,
					 a1.data, a2.data, a3.data);

	require_true(tc, conn, q.data);

	pfree(a1.data);
	pfree(a2.data);
	pfree(a3.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A searched CASE whose WHENs are all false yields its ELSE expression, or
 * NULL when no ELSE is present.  CASE WHEN false THEN 1 ELSE a END = a, and
 * CASE WHEN false THEN 1 END IS NULL.
 */
PG_PROP(case_exhaustive_matches_coalesce)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arg(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (CASE WHEN false THEN 1 ELSE %s END) "
					 "IS NOT DISTINCT FROM %s "
					 "AND (CASE WHEN false THEN 1 END) IS NULL",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
