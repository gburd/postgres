/*-------------------------------------------------------------------------
 *
 * prop_inlist.c
 *	  Property-based tests for IN / NOT IN / ANY / ALL list-membership algebra.
 *
 * The row-expression and scalar-array operators (IN, NOT IN, = ANY, > ALL,
 * > ANY) must all agree with their primitive definitions: IN is an OR-of-
 * equals, NOT IN is an AND-of-not-equals, = ANY(array) is the array form of
 * IN, and the ordered ANY/ALL forms reduce to a comparison against the
 * least/greatest list element.  A NULL in the list still produces the classic
 * three-valued-logic unknown rather than false.  A "f", NULL, or SQL error on
 * these scalar boolean oracles is a bug in ScalarArrayOpExpr / ArrayExpr
 * evaluation, not a tolerance artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_inlist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "inlist"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a non-null int literal from a small domain, parenthesized so a leading
 * sign never misparses when substituted into a larger expression.
 */
static void
draw_int(pg_prop_tc *tc, StringInfo buf)
{
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
 * IN equals = ANY(array): for a non-null x and a non-null list, (x IN (a,b,c))
 * is equivalent to (x = ANY(ARRAY[a,b,c])).  Both sides are non-NULL booleans
 * (no NULLs anywhere), so a plain = is sound and the result must be "t".
 */
PG_PROP(in_eq_any_array)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	draw_int(tc, &x);
	draw_int(tc, &a);
	draw_int(tc, &b);
	draw_int(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s IN (%s, %s, %s)) = "
					 "(%s = ANY(ARRAY[%s, %s, %s]))",
					 x.data, a.data, b.data, c.data,
					 x.data, a.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * IN is the OR-of-equals: for non-null x, a, b, (x IN (a, b)) is equivalent to
 * (x = a OR x = b).  No NULLs, so both sides are non-NULL booleans and a plain
 * = must yield "t".
 */
PG_PROP(in_eq_or_of_equals)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&a);
	initStringInfo(&b);
	draw_int(tc, &x);
	draw_int(tc, &a);
	draw_int(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s IN (%s, %s)) = "
					 "((%s = %s) OR (%s = %s))",
					 x.data, a.data, b.data,
					 x.data, a.data, x.data, b.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * NOT IN is the AND-of-not-equals: for non-null x, a, b, (x NOT IN (a, b)) is
 * equivalent to (x <> a AND x <> b).  No NULLs, so a plain = must yield "t".
 * The NOT IN is parenthesized so it binds as the left operand of =.
 */
PG_PROP(not_in_eq_and_of_ne)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&a);
	initStringInfo(&b);
	draw_int(tc, &x);
	draw_int(tc, &a);
	draw_int(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s NOT IN (%s, %s)) = "
					 "((%s <> %s) AND (%s <> %s))",
					 x.data, a.data, b.data,
					 x.data, a.data, x.data, b.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * > ALL reduces to a comparison against the greatest element: x is greater
 * than every list member iff it is greater than their maximum, so
 * (x > ALL(ARRAY[a,b,c])) = (x > greatest(a,b,c)) for non-null values.  All
 * non-NULL, so a plain = must yield "t".
 */
PG_PROP(all_gt)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	draw_int(tc, &x);
	draw_int(tc, &a);
	draw_int(tc, &b);
	draw_int(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s > ALL(ARRAY[%s, %s, %s])) = "
					 "(%s > greatest(%s, %s, %s))",
					 x.data, a.data, b.data, c.data,
					 x.data, a.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * > ANY reduces to a comparison against the least element: x is greater than
 * some list member iff it is greater than their minimum, so
 * (x > ANY(ARRAY[a,b,c])) = (x > least(a,b,c)) for non-null values.  All
 * non-NULL, so a plain = must yield "t".
 */
PG_PROP(any_gt)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	draw_int(tc, &x);
	draw_int(tc, &a);
	draw_int(tc, &b);
	draw_int(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s > ANY(ARRAY[%s, %s, %s])) = "
					 "(%s > least(%s, %s, %s))",
					 x.data, a.data, b.data, c.data,
					 x.data, a.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The IN-with-NULL unknown case: when x matches no non-NULL list item and the
 * list contains NULL, IN yields unknown (NULL), not false.  We build x and a
 * with x <> a via a non-zero offset (x = a + drawn 1..10), avoiding a reject
 * loop, so the only non-NULL list element a never equals x; then
 * (x IN (a, NULL)) IS NULL is itself always a non-NULL boolean, so a plain
 * comparison to "t" is sound.
 */
PG_PROP(in_null_unknown)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				x;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, -5, 5);
	/* x != a without a reject loop: add a non-zero offset */
	x = a + (int) pg_prop_draw_int(tc, 1, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%d) IN ((%d), NULL)) IS NULL",
					 x, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The NOT IN-with-NULL unknown case: same construction as in_null_unknown, but
 * for NOT IN.  When x matches no non-NULL item and the list has a NULL,
 * (x NOT IN (a, NULL)) is unknown (NULL), not true.  With x <> a built via a
 * non-zero offset, (x NOT IN (a, NULL)) IS NULL is a non-NULL boolean and must
 * be "t".
 */
PG_PROP(not_in_null_unknown)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				x;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, -5, 5);
	/* x != a without a reject loop: add a non-zero offset */
	x = a + (int) pg_prop_draw_int(tc, 1, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%d) NOT IN ((%d), NULL)) IS NULL",
					 x, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Empty scalar-array operators: = ANY over an empty array is vacuously false
 * (no element to match), and <> ALL over an empty array is vacuously true (the
 * universally-quantified claim over no elements holds).  No drawn inputs; a
 * fixed sanity oracle guarding the empty-array boundary of ScalarArrayOpExpr.
 */
PG_PROP(empty_any_false)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			x;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x = (int) pg_prop_draw_int(tc, -5, 5);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%d) = ANY(ARRAY[]::int[])) = false "
					 "AND ((%d) <> ALL(ARRAY[]::int[])) = true",
					 x, x);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
