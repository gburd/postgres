/*-------------------------------------------------------------------------
 *
 * prop_condexpr.c
 *	  Property-based tests for conditional-expression semantics.
 *
 * NULLIF, multi-branch simple CASE, GREATEST/LEAST NULL handling, ELSE-less
 * searched CASE, and nested COALESCE all compile to the same executor nodes
 * (NullIfExpr, CaseExpr, MinMaxExpr, CoalesceExpr).  Each property pins one
 * specified behaviour against an independent reconstruction of its definition,
 * so any "f" is a wrong-results bug in that machinery rather than a modelling
 * artefact: NULLIF reflexively nulls, NULLIF equals its = based CASE
 * expansion, a four-branch simple CASE selects the first match, GREATEST/LEAST
 * skip NULLs (and all-NULL yields NULL), an ELSE-less CASE with no true WHEN
 * is NULL, and COALESCE flattens.  Comparisons that can see NULL use IS NOT
 * DISTINCT FROM; the NULLIF oracle uses plain = to match NULLIF's own equality.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_condexpr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "condexpr"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw one int-or-NULL argument into buf: with probability ~1/4 the literal
 * SQL NULL (cast to ::int so an all-NULL expression still type-resolves),
 * otherwise a small integer in [-50, 50] emitted as "(%d)::int" so a leading
 * minus never misparses when substituted into a larger expression.
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
 * Draw a guaranteed non-NULL int argument into buf, for properties that
 * compare GREATEST/LEAST with interleaved NULLs against the NULL-free form.
 */
static void
draw_arg_nonnull(pg_prop_tc *tc, StringInfo buf)
{
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
 * NULLIF(a, a) IS NULL for any int-or-NULL a.  When a is non-NULL, a = a is
 * true so NULLIF returns NULL; when a is NULL, NULLIF(NULL, NULL) is NULL by
 * definition.  Either way the result is NULL, checked with IS NULL.
 */
PG_PROP(nullif_self_null)
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
	appendStringInfo(&q, "SELECT nullif(%s, %s) IS NULL", a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * NULLIF(a, b) equals its = based CASE expansion:
 *	 CASE WHEN a = b THEN NULL ELSE a END.
 * NULLIF decides on SQL = (not IS NOT DISTINCT FROM), so when a or b is NULL
 * the WHEN is unknown and NULLIF returns a -- the oracle reproduces that.  The
 * two sides are compared with IS NOT DISTINCT FROM so a NULL result matches.
 */
PG_PROP(coalesce_nullif_identity)
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
 * A four-branch simple CASE selects the first matching branch.  For a drawn
 * x in 0..3,
 *	 CASE x WHEN 0 THEN 10 WHEN 1 THEN 11 WHEN 2 THEN 12 ELSE 13 END
 * must equal 10 + least(x, 3) (10,11,12 for x=0,1,2; 13 for x=3 via ELSE).
 */
PG_PROP(case_multibranch)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			x = (int) pg_prop_draw_int(tc, 0, 3);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (CASE %d WHEN 0 THEN 10 WHEN 1 THEN 11 "
					 "WHEN 2 THEN 12 ELSE 13 END) = (10 + least(%d, 3))",
					 x, x);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * GREATEST and LEAST skip NULL arguments: with non-NULL a, b, c,
 *	 GREATEST(a, NULL, b, NULL, c) = GREATEST(a, b, c)
 * and likewise for LEAST.  The interleaved NULLs must have no effect.  Both
 * operands are non-NULL so plain = is sound here.
 */
PG_PROP(greatest_least_null_skip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	draw_arg_nonnull(tc, &a);
	draw_arg_nonnull(tc, &b);
	draw_arg_nonnull(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT greatest(%s, NULL::int, %s, NULL::int, %s) = "
					 "greatest(%s, %s, %s) "
					 "AND least(%s, NULL::int, %s, NULL::int, %s) = "
					 "least(%s, %s, %s)",
					 a.data, b.data, c.data,
					 a.data, b.data, c.data,
					 a.data, b.data, c.data,
					 a.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * GREATEST (and LEAST) over all-NULL arguments is NULL: every argument is
 * skipped, leaving nothing, and the specified result is NULL.
 */
PG_PROP(greatest_all_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT greatest(NULL::int, NULL::int) IS NULL "
						   "AND least(NULL::int, NULL::int) IS NULL");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A searched CASE with no ELSE and no true WHEN yields NULL.  A statically
 * false WHEN is NULL, and a WHEN whose predicate is false for every drawn a in
 * the small range (a > 1000000) is also never taken, so both CASEs are NULL.
 */
PG_PROP(case_else_omitted_null)
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
					 "SELECT (CASE WHEN false THEN 1 END) IS NULL "
					 "AND (CASE WHEN (%s > 1000000) THEN 1 END) IS NULL",
					 a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * COALESCE flattens: COALESCE(a, COALESCE(b, c)) returns the first non-NULL of
 * a, b, c, exactly as the flat COALESCE(a, b, c) does.  IS NOT DISTINCT FROM
 * makes the all-NULL case (both sides NULL) pass.
 */
PG_PROP(nested_coalesce)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	draw_arg(tc, &a);
	draw_arg(tc, &b);
	draw_arg(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(%s, coalesce(%s, %s)) "
					 "IS NOT DISTINCT FROM coalesce(%s, %s, %s)",
					 a.data, b.data, c.data,
					 a.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
