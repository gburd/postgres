/*-------------------------------------------------------------------------
 *
 * prop_coalescefn.c
 *	  Property-based tests for deeper COALESCE/NULLIF/GREATEST/LEAST laws.
 *
 * The first-match and NULL-skip semantics of these operators imply a layer of
 * algebraic laws beyond the one-step definitions checked in prop_coalesce:
 * COALESCE is associative and absorbs NULL on either side; NULLIF composes
 * with COALESCE so that COALESCE(NULLIF(a,b),b) collapses back to
 * COALESCE(a,b); GREATEST is idempotent and both GREATEST and LEAST ignore
 * NULL arguments entirely, so dropping a NULL argument changes nothing and
 * LEAST never exceeds GREATEST.  A failure of any of these is a bug in the
 * operator, not an artefact of the test.
 *
 * The oracle is each law stated as a SQL boolean that must be "t" for every
 * input: identities compared with IS NOT DISTINCT FROM (so NULL = NULL passes)
 * and ordering stated with <= over non-NULL draws.  No invented formulas --
 * every right-hand side is a rearrangement of the same operators or a direct
 * consequence of their published semantics.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_coalescefn.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "coalescefn"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw one int-or-NULL argument into buf: with probability ~30% the literal
 * SQL NULL, otherwise a small integer in [-50, 50].  Each argument is cast to
 * ::int so the expression type resolves even when every argument is NULL, and
 * a negative value is wrapped in parens as "(%d)::int" so a leading sign never
 * misparses when substituted into a larger expression.
 */
static void
draw_arg(pg_prop_tc *tc, StringInfo buf)
{
	bool		is_null = (pg_prop_draw_int(tc, 0, 9) < 3);

	if (is_null)
		appendStringInfoString(buf, "NULL::int");
	else
		appendStringInfo(buf, "(%d)::int",
						 (int) pg_prop_draw_int(tc, -50, 50));
}

/*
 * Draw a guaranteed-non-NULL int argument into buf, for the ordering and
 * idempotence laws that are only stated over non-NULL values.
 */
static void
draw_arg_nonnull(pg_prop_tc *tc, StringInfo buf)
{
	appendStringInfo(buf, "(%d)::int",
					 (int) pg_prop_draw_int(tc, -50, 50));
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the law did not hold as the oracle demands, which is
 * itself a failure, reported with the SQL.
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
 * COALESCE is associative: nesting the fold left or right over three
 * arguments yields the same value.  Both arrangements return the first
 * non-NULL of a, b, c (or NULL if all three are NULL), so they must agree;
 * IS NOT DISTINCT FROM makes the all-NULL case pass.
 */
PG_PROP(coalesce_associative)
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
					 "IS NOT DISTINCT FROM "
					 "coalesce(coalesce(%s, %s), %s)",
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
 * COALESCE absorbs a NULL on either side: coalesce(NULL, a) = a and
 * coalesce(a, NULL) = a for any a (including a = NULL, where both sides are
 * NULL and IS NOT DISTINCT FROM holds).
 */
PG_PROP(coalesce_absorbs_null)
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
					 "SELECT (coalesce(NULL::int, %s) IS NOT DISTINCT FROM %s) "
					 "AND (coalesce(%s, NULL::int) IS NOT DISTINCT FROM %s)",
					 a.data, a.data,
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * coalesce(a, b) returns a when a is non-NULL and b otherwise -- exactly the
 * value of CASE WHEN a IS NOT NULL THEN a ELSE b END.  The two must agree for
 * all a, b; IS NOT DISTINCT FROM handles the both-NULL case.
 */
PG_PROP(coalesce_first_nonnull_wins)
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
					 "SELECT coalesce(%s, %s) IS NOT DISTINCT FROM "
					 "(CASE WHEN %s IS NOT NULL THEN %s ELSE %s END)",
					 a.data, b.data,
					 a.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * NULLIF composes with COALESCE to cancel back to COALESCE:
 * coalesce(nullif(a, b), b) IS NOT DISTINCT FROM coalesce(a, b).
 *
 * Case a = b (both non-NULL): nullif(a,b)=NULL so the left is b, and the
 * right is a = b -- equal.  Case a <> b: nullif(a,b)=a so both sides are a.
 * Case a NULL: nullif(NULL,b)=NULL so the left is b, and coalesce(NULL,b)=b.
 * In every case the two sides match, so the law holds unconditionally.
 */
PG_PROP(nullif_coalesce_cancel)
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
					 "SELECT coalesce(nullif(%s, %s), %s) "
					 "IS NOT DISTINCT FROM coalesce(%s, %s)",
					 a.data, b.data, b.data,
					 a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * GREATEST is idempotent: greatest(a, a) = a for non-NULL a.  Drawn non-NULL
 * so the result is well defined; IS NOT DISTINCT FROM is used for uniformity.
 */
PG_PROP(greatest_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arg_nonnull(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT greatest(%s, %s) IS NOT DISTINCT FROM %s",
					 a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For non-NULL a, b, c the minimum never exceeds the maximum:
 * least(a, b, c) <= greatest(a, b, c).  All arguments are drawn non-NULL so
 * both sides are non-NULL and the comparison is a plain boolean.
 */
PG_PROP(least_le_greatest)
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
					 "SELECT least(%s, %s, %s) <= greatest(%s, %s, %s)",
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
 * GREATEST skips NULL arguments, so a NULL in the middle changes nothing:
 * greatest(a, NULL, b) = greatest(a, b) for non-NULL a, b.  Both are non-NULL
 * so the result is well defined; IS NOT DISTINCT FROM is used for uniformity.
 */
PG_PROP(greatest_null_skip)
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
	draw_arg_nonnull(tc, &a);
	draw_arg_nonnull(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT greatest(%s, NULL::int, %s) "
					 "IS NOT DISTINCT FROM greatest(%s, %s)",
					 a.data, b.data,
					 a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * COALESCE over nothing but NULLs is NULL: coalesce(NULL, NULL, NULL) IS NULL.
 * A fixed law with no drawn inputs -- it must hold on every run.
 */
PG_PROP(coalesce_all_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT coalesce(NULL::int, NULL::int, NULL::int) IS NULL");

	pg_prop_disconnect(conn);
}
