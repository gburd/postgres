/*-------------------------------------------------------------------------
 *
 * prop_nullsemantics.c
 *	  Property-based tests for SQL three-valued logic and NULL propagation.
 *
 * SQL boolean expressions evaluate in three-valued logic (true/false/unknown),
 * and NULL propagates through comparisons, IN, NOT IN, and boolean aggregates
 * in ways that are easy to get subtly wrong in the executor.  Each property
 * builds a scalar boolean oracle: a SELECT that must return "t" for every
 * drawn combination of bool-or-NULL and int-or-NULL operands.  Where both
 * sides of a comparison can themselves be NULL we compare with IS NOT DISTINCT
 * FROM (NULL-safe equality) so the oracle never collapses to NULL on a
 * boundary; a "t" is required, so an "f", NULL, or SQL error is a failure.
 *
 * A failure here is a bug in 3VL evaluation or NULL propagation, not a
 * tolerance artefact: these identities are exact for every input.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_nullsemantics.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "nullsemantics"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a boolean-or-NULL literal: one of "true", "false", "NULL::boolean".
 * The cast on NULL keeps the operand well-typed in any context.
 */
static void
draw_bool_null(pg_prop_tc *tc, StringInfo buf)
{
	int			k = (int) pg_prop_draw_int(tc, 0, 2);

	if (k == 0)
		appendStringInfoString(buf, "NULL::boolean");
	else if (k == 1)
		appendStringInfoString(buf, "true");
	else
		appendStringInfoString(buf, "false");
}

/*
 * Draw an integer-or-NULL literal from a small domain, ~25% NULL.  Negative
 * integers are emitted parenthesized and cast as "(%d)" so a leading sign
 * never misparses when substituted into a larger expression.
 */
static void
draw_int_null(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 3) == 0)
		appendStringInfoString(buf, "NULL::int");
	else
		appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, -5, 5));
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the oracle produced unknown or errored on inputs that must
 * yield true, which is itself a failure -- reported with the SQL.
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
 * De Morgan in 3VL: NOT (a AND b) is equivalent to (NOT a) OR (NOT b) for a,b
 * ranging over true/false/unknown.  Both sides can be NULL (when a or b is
 * NULL), so compare with IS NOT DISTINCT FROM.
 */
PG_PROP(and_or_demorgan_3vl)
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
	draw_bool_null(tc, &a);
	draw_bool_null(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (NOT (%s AND %s)) "
					 "IS NOT DISTINCT FROM ((NOT %s) OR (NOT %s))",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * IS NULL is the exact complement of IS NOT NULL: (x IS NULL) = NOT (x IS NOT
 * NULL).  Both IS NULL and IS NOT NULL always return a non-NULL boolean, so a
 * plain = is sound here.
 */
PG_PROP(is_null_complement)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_int_null(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s IS NULL) = (NOT (%s IS NOT NULL))",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Reflexive equality is NULL exactly when x is NULL, and true otherwise:
 * (x = x) IS NOT DISTINCT FROM (CASE WHEN x IS NULL THEN NULL ELSE true END).
 * The left side is NULL when x is NULL, so IS NOT DISTINCT FROM is required.
 */
PG_PROP(null_eq_is_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_int_null(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s = %s) IS NOT DISTINCT FROM "
					 "(CASE WHEN %s IS NULL THEN NULL ELSE true END)",
					 x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * IS NOT DISTINCT FROM equals coalesce(a = b, a IS NULL AND b IS NULL): a =
 * b is NULL when either operand is NULL, and in that case the second argument
 * supplies the NULL-safe answer (true only when both are NULL).  coalesce
 * never returns NULL here because its second argument is a non-NULL boolean,
 * so the two always-non-NULL sides compare with a plain =.
 */
PG_PROP(coalesce_vs_is_not_distinct)
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
	draw_int_null(tc, &a);
	draw_int_null(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s IS NOT DISTINCT FROM %s) = "
					 "coalesce(%s = %s, %s IS NULL AND %s IS NULL)",
					 a.data, b.data, a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * IN is the 3VL OR-of-equals: (x IN (l1, l2)) is equivalent to (x = l1 OR x =
 * l2), including when any operand is NULL (then both can be NULL/unknown).
 * Compare with IS NOT DISTINCT FROM because both sides can be NULL.
 */
PG_PROP(in_with_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				l1,
				l2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&l1);
	initStringInfo(&l2);
	draw_int_null(tc, &x);
	draw_int_null(tc, &l1);
	draw_int_null(tc, &l2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s IN (%s, %s)) IS NOT DISTINCT FROM "
					 "(%s = %s OR %s = %s)",
					 x.data, l1.data, l2.data,
					 x.data, l1.data, x.data, l2.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(l1.data);
	pfree(l2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The classic NOT IN NULL trap: when the list contains NULL and x does not
 * match any non-NULL item, the result is NULL (unknown), not true.  We draw
 * two distinct non-NULL integers x and k (redrawing k until x <> k) so x is
 * guaranteed not to equal the only non-NULL list element, then require
 * (x NOT IN (k, NULL)) IS NULL -- which is itself always a non-NULL boolean,
 * so a plain comparison to "t" is sound.
 */
PG_PROP(not_in_null_is_unknown)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			x,
				k;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x = (int) pg_prop_draw_int(tc, -5, 5);
	/* k != x without a reject loop: add a non-zero offset */
	k = x + (int) pg_prop_draw_int(tc, 1, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%d) NOT IN ((%d), NULL)) IS NULL",
					 x, k);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Constant 3VL truth-table check for AND/OR with unknown: true AND NULL is
 * unknown; false AND NULL is false (determined); true OR NULL is true
 * (determined); false OR NULL is unknown.  No drawn inputs -- a fixed sanity
 * oracle guarding the determining-vs-propagating cases of boolean 3VL.
 */
PG_PROP(boolean_agg_3vl)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (true AND NULL) IS NULL "
				 "AND (false AND NULL) = false "
				 "AND (true OR NULL) = true "
				 "AND (false OR NULL) IS NULL");

	pg_prop_disconnect(conn);
}
