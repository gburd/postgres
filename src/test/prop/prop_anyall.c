/*-------------------------------------------------------------------------
 *
 * prop_anyall.c
 *	  Property-based tests for scalar = ANY / = ALL array operators.
 *
 * These exercise "op ANY(array)" and "op ALL(array)" (ScalarArrayOpExpr)
 * against generated integer arrays, which is a different executor path than
 * IN-list or array-comparison lowering: the oracle rewrites each quantified
 * comparison as an equivalent unnest + EXISTS/aggregate, so a wrong answer in
 * the ANY/ALL evaluation (empty-array identity, min/max bound, membership) is
 * a bug.  Array elements are non-NULL (SQL NULL in ANY/ALL is a separate 3VL
 * concern) and empty arrays are handled explicitly.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_anyall.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "anyall"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Integer element domain: small, straddling 0 and both signs. */
#define EL_MIN (-5)
#define EL_MAX 5

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the comparison errored or produced NULL on inputs where
 * the property is defined to be true/false, which is itself a bug.
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
 * Draw a non-NULL int element in [EL_MIN, EL_MAX] into buf, parenthesized and
 * cast so a negative value never misparses when substituted into an array.
 */
static void
draw_el(pg_prop_tc *tc, StringInfo buf)
{
	appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, EL_MIN, EL_MAX));
}

/*
 * Build an int4[] literal "ARRAY[...]::int[]" with 0..8 non-NULL elements.
 * An empty draw yields "ARRAY[]::int[]", exercising the empty-array path.
 */
static void
draw_array(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 8);

	appendStringInfoString(buf, "ARRAY[");
	for (int i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		draw_el(tc, buf);
	}
	appendStringInfoString(buf, "]::int[]");
}

/* Draw a non-NULL scalar int in [EL_MIN, EL_MAX] into buf. */
static void
draw_scalar(pg_prop_tc *tc, StringInfo buf)
{
	appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, EL_MIN, EL_MAX));
}

/*
 * 1. (x = ANY(a)) agrees with set membership computed via unnest + EXISTS.
 *    Both are false for an empty array, so no empty guard is needed.
 */
PG_PROP(eq_any_is_member)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&x);
	draw_array(tc, &a);
	draw_scalar(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s = ANY(%s)) "
					 "= EXISTS(SELECT 1 FROM unnest(%s) e WHERE e = %s)",
					 x.data, a.data, a.data, x.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. (x > ALL(a)) equals (x > max(a)) for non-empty a; for empty a, x > ALL
 *    is vacuously true.  The CASE keeps the empty case sound (max() over 0
 *    rows is NULL, so comparing against it would be unusable there).
 */
PG_PROP(gt_all_is_gt_max)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&x);
	draw_array(tc, &a);
	draw_scalar(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN cardinality(%s) = 0 "
					 "THEN (%s > ALL(%s)) "
					 "ELSE (%s > ALL(%s)) "
					 "= (%s > (SELECT max(e) FROM unnest(%s) e)) END",
					 a.data,
					 x.data, a.data,
					 x.data, a.data,
					 x.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. (x < ANY(a)) equals (x < max(a)) for non-empty a; for empty a, x < ANY
 *    is false (there is nothing to beat).  Guarded by cardinality.
 */
PG_PROP(lt_any_is_lt_max)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&x);
	draw_array(tc, &a);
	draw_scalar(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN cardinality(%s) = 0 "
					 "THEN (%s < ANY(%s)) = false "
					 "ELSE (%s < ANY(%s)) "
					 "= (%s < (SELECT max(e) FROM unnest(%s) e)) END",
					 a.data,
					 x.data, a.data,
					 x.data, a.data,
					 x.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. (c = ALL(array of k copies of c)) is true: every element equals c.
 *    Build the array as k >= 1 copies of a single drawn value c.
 */
PG_PROP(eq_all_single_value)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			c = (int) pg_prop_draw_int(tc, EL_MIN, EL_MAX);
	int			k = (int) pg_prop_draw_int(tc, 1, 8);

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	appendStringInfoString(&a, "ARRAY[");
	for (int i = 0; i < k; i++)
	{
		if (i > 0)
			appendStringInfoString(&a, ", ");
		appendStringInfo(&a, "(%d)", c);
	}
	appendStringInfoString(&a, "]::int[]");

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%d) = ALL(%s)) = true", c, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. (x <> ALL(a)) = NOT (x = ANY(a)): non-membership is the negation of
 *    membership.  Holds for empty a too (both sides are true/true).
 */
PG_PROP(ne_all_absent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&x);
	draw_array(tc, &a);
	draw_scalar(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s <> ALL(%s)) = NOT (%s = ANY(%s))",
					 x.data, a.data, x.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. (x = ANY(ARRAY[]::int[])) is false: nothing to match in an empty array.
 */
PG_PROP(any_empty_false)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_scalar(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s = ANY(ARRAY[]::int[])) = false", x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. (x = ALL(ARRAY[]::int[])) is true: vacuously, all zero elements match.
 */
PG_PROP(all_empty_true)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_scalar(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s = ALL(ARRAY[]::int[])) = true", x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 8. (x >= ANY(a)) equals (x >= min(a)) for non-empty a; for empty a, x >= ANY
 *    is false.  Guarded by cardinality (min() over 0 rows is NULL).
 */
PG_PROP(ge_any_bound)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&x);
	draw_array(tc, &a);
	draw_scalar(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN cardinality(%s) = 0 "
					 "THEN (%s >= ANY(%s)) = false "
					 "ELSE (%s >= ANY(%s)) "
					 "= (%s >= (SELECT min(e) FROM unnest(%s) e)) END",
					 a.data,
					 x.data, a.data,
					 x.data, a.data,
					 x.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
