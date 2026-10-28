/*-------------------------------------------------------------------------
 *
 * prop_rowcompare.c
 *	  Property-based tests for row-value comparison and IS DISTINCT.
 *
 * The oracle is the defining expansion of each row-value construct: a
 * ROW(...) equality/ordering compares componentwise/lexicographically, and
 * ROW IS DISTINCT FROM is the NULL-aware negation of componentwise IS NOT
 * DISTINCT.  Each property reconstructs the row operator from its scalar
 * components in SQL and requires the two sides to agree; a disagreement is a
 * bug in the row-comparison code.  Where NULLs appear we compare with IS NOT
 * DISTINCT FROM so 3VL does not leak NULL into the boolean oracle.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_rowcompare.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "rowcompare"
#include "pg_prop.h"

#include "lib/stringinfo.h"

#define VAL_MIN (-4)
#define VAL_MAX 4

/* Draw a non-null int literal in [VAL_MIN, VAL_MAX], parenthesized if neg. */
static void
draw_int(pg_prop_tc *tc, StringInfo buf)
{
	int			v = (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX);

	if (v < 0)
		appendStringInfo(buf, "(%d)", v);
	else
		appendStringInfo(buf, "%d", v);
}

/* Draw an int-or-NULL literal (~15% NULL). */
static void
draw_int_or_null(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		draw_int(tc, buf);
}

/* Run a boolean SELECT and require "t"; NULL or error is a failure. */
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
 * Row equality is componentwise (all operands non-null): ROW(a,b) = ROW(c,d)
 * iff a=c AND b=d.
 */
PG_PROP(row_eq_componentwise)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	initStringInfo(&d);
	draw_int(tc, &a);
	draw_int(tc, &b);
	draw_int(tc, &c);
	draw_int(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (ROW(%s, %s) = ROW(%s, %s)) = (%s = %s AND %s = %s)",
					 a.data, b.data, c.data, d.data,
					 a.data, c.data, b.data, d.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Row less-than is lexicographic (all operands non-null): ROW(a,b) <
 * ROW(c,d) iff a<c OR (a=c AND b<d).
 */
PG_PROP(row_lt_lexicographic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	initStringInfo(&d);
	draw_int(tc, &a);
	draw_int(tc, &b);
	draw_int(tc, &c);
	draw_int(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (ROW(%s, %s) < ROW(%s, %s)) = "
					 "(%s < %s OR (%s = %s AND %s < %s))",
					 a.data, b.data, c.data, d.data,
					 a.data, c.data, a.data, c.data, b.data, d.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ROW IS DISTINCT FROM is the NULL-aware negation of componentwise IS NOT
 * DISTINCT: ROW(a,b) IS DISTINCT FROM ROW(c,d) equals NOT (a IS NOT DISTINCT
 * FROM c AND b IS NOT DISTINCT FROM d).  Operands drawn int-or-NULL.
 */
PG_PROP(row_is_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	initStringInfo(&d);
	draw_int_or_null(tc, &a);
	draw_int_or_null(tc, &b);
	draw_int_or_null(tc, &c);
	draw_int_or_null(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (ROW(%s, %s) IS DISTINCT FROM ROW(%s, %s)) = "
					 "(NOT (%s IS NOT DISTINCT FROM %s AND %s IS NOT DISTINCT FROM %s))",
					 a.data, b.data, c.data, d.data,
					 a.data, c.data, b.data, d.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A row is never distinct from itself, even with NULL components:
 * ROW(a,b,c) IS NOT DISTINCT FROM ROW(a,b,c) is always true.
 */
PG_PROP(row_eq_self)
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
					 "SELECT ROW(%s, %s, %s) IS NOT DISTINCT FROM ROW(%s, %s, %s)",
					 a.data, b.data, c.data, a.data, b.data, c.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A row-value IN list containing the row itself matches: for non-null a,b,
 * (a,b) IN ((c,d),(a,b)) is true.
 */
PG_PROP(in_rowvalues)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	initStringInfo(&d);
	draw_int(tc, &a);
	draw_int(tc, &b);
	draw_int(tc, &c);
	draw_int(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s, %s) IN ((%s, %s), (%s, %s))",
					 a.data, b.data, c.data, d.data, a.data, b.data);
	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
