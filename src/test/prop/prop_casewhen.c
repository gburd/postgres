/*-------------------------------------------------------------------------
 *
 * prop_casewhen.c
 *	  Property-based tests for CASE / COALESCE / NULLIF / GREATEST / LEAST.
 *
 * These SQL constructs all compile to the same conditional-expression
 * machinery (CaseExpr, CoalesceExpr, MinMaxExpr, NullIfExpr in the executor),
 * and each has a textbook definition in terms of the others.  The oracle is
 * that equivalence: COALESCE must equal the first non-NULL via a nested CASE,
 * NULLIF must equal its CASE expansion, a searched and a simple CASE must
 * agree, GREATEST/LEAST must bound their arguments and ignore NULLs.  Any "f"
 * result is a wrong-results bug in that machinery, not a modelling artefact,
 * because every comparison that can see NULL uses IS NOT DISTINCT FROM.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_casewhen.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "casewhen"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Integer domain for drawn arguments. */
#define VAL_MIN (-20)
#define VAL_MAX 20

/*
 * Emit an int literal in [VAL_MIN, VAL_MAX], or SQL NULL (~25% of the time).
 * A negative is wrapped as "(%d)" so a leading minus never misparses when the
 * literal is substituted into a larger expression.
 */
static void
draw_int_or_null(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 3) == 0)
	{
		appendStringInfoString(buf, "NULL::int");
		return;
	}
	appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/* Emit a non-NULL int literal in [VAL_MIN, VAL_MAX], as "(%d)". */
static void
draw_int(pg_prop_tc *tc, StringInfo buf)
{
	appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the expression errored or produced NULL where the oracle
 * guarantees a definite true, which is itself a bug -- so either is a failure,
 * reported with the SQL.
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
 * A searched CASE and a simple CASE over the same scrutinee must agree.  Draw
 * x in 0..3 so some cases hit a WHEN arm and some fall to ELSE.
 */
PG_PROP(case_searched_equals_simple)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			x;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x = (int) pg_prop_draw_int(tc, 0, 3);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (CASE (%d) WHEN 0 THEN 'a' WHEN 1 THEN 'b' "
					 "ELSE 'z' END) "
					 "= (CASE WHEN (%d)=0 THEN 'a' WHEN (%d)=1 THEN 'b' "
					 "ELSE 'z' END)",
					 x, x, x);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * COALESCE(a,b,c) returns the first non-NULL argument (or NULL if all are
 * NULL), which is exactly a nested CASE on IS NOT NULL.  IS NOT DISTINCT FROM
 * makes the all-NULL case pass soundly.
 */
PG_PROP(coalesce_first_nonnull)
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
	draw_int_or_null(tc, &a);
	draw_int_or_null(tc, &b);
	draw_int_or_null(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT COALESCE(%s, %s, %s) IS NOT DISTINCT FROM "
					 "(CASE WHEN %s IS NOT NULL THEN %s "
					 "WHEN %s IS NOT NULL THEN %s ELSE %s END)",
					 a.data, b.data, c.data,
					 a.data, a.data, b.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * NULLIF(a,b) is defined as CASE WHEN a=b THEN NULL ELSE a END.  IS NOT
 * DISTINCT FROM covers the NULL-on-both-sides cases.
 */
PG_PROP(nullif_definition)
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
	draw_int_or_null(tc, &a);
	draw_int_or_null(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT NULLIF(%s, %s) IS NOT DISTINCT FROM "
					 "(CASE WHEN %s = %s THEN NULL ELSE %s END)",
					 a.data, b.data, a.data, b.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For non-NULL a,b,c: LEAST(a,b,c) <= each argument <= GREATEST(a,b,c).  A
 * sound total bound -- the min is below every element and the max above every
 * element with no ties or NULLs to worry about.
 */
PG_PROP(greatest_least_bounds)
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
	draw_int(tc, &a);
	draw_int(tc, &b);
	draw_int(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT LEAST(%s, %s, %s) <= %s AND %s <= GREATEST(%s, %s, %s) "
					 "AND LEAST(%s, %s, %s) <= %s AND %s <= GREATEST(%s, %s, %s) "
					 "AND LEAST(%s, %s, %s) <= %s AND %s <= GREATEST(%s, %s, %s)",
					 a.data, b.data, c.data, a.data, a.data, a.data, b.data, c.data,
					 a.data, b.data, c.data, b.data, b.data, a.data, b.data, c.data,
					 a.data, b.data, c.data, c.data, c.data, a.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * GREATEST and LEAST skip NULL arguments, so inserting a NULL among non-NULL
 * operands must not change the result.
 */
PG_PROP(greatest_ignores_null)
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
	draw_int(tc, &a);
	draw_int(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT GREATEST(%s, NULL::int, %s) = GREATEST(%s, %s) "
					 "AND LEAST(%s, NULL::int, %s) = LEAST(%s, %s)",
					 a.data, b.data, a.data, b.data,
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A CASE with no ELSE and no matching WHEN yields NULL.  One constant form
 * (WHEN false) and one drawn form (WHEN x > 1000, never true for x in the
 * domain) both must be NULL.
 */
PG_PROP(case_no_match_is_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			x;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x = (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (CASE WHEN false THEN 1 END) IS NULL "
					 "AND (CASE WHEN (%d) > 1000 THEN 1 END) IS NULL",
					 x);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * COALESCE(a,a) is a (idempotent), including when a is NULL.  IS NOT DISTINCT
 * FROM makes the NULL case sound.
 */
PG_PROP(coalesce_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_int_or_null(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT COALESCE(%s, %s) IS NOT DISTINCT FROM %s",
					 a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
