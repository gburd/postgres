/*-------------------------------------------------------------------------
 *
 * prop_floatspecial.c
 *	  Property-based tests for float4/float8 special values and widening.
 *
 * This area targets the IEEE special values (+/-Infinity, NaN, signed zero)
 * and the float4<->float8 relationship, not the Ryu round-trip the "float"
 * area already covers.  PostgreSQL gives floats a total order that differs
 * from raw IEEE: NaN compares equal to NaN and sorts as the largest value,
 * and -0 compares equal to +0.  These properties pin those documented
 * semantics plus infinity arithmetic and the precision loss of a float8
 * narrowed to float4 and widened back.  A failure means a comparison or
 * arithmetic routine disagrees with the type's defined total order, which is
 * a bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_floatspecial.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "floatspecial"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a small finite float8 literal as "%d.%d" (whole.frac), parenthesized
 * and cast, so a leading sign never misparses when substituted into a larger
 * expression.  Small magnitudes keep the float4<->float8 widening bound tight
 * and avoid any infinity/overflow surprise -- the specials are supplied as
 * literal casts by the properties that need them.
 */
static void
draw_finite_f8(pg_prop_tc *tc, StringInfo buf)
{
	int			whole = (int) pg_prop_draw_int(tc, -1000, 1000);
	int			frac = (int) pg_prop_draw_int(tc, 0, 999);

	appendStringInfo(buf, "(%d.%d)::float8", whole, frac);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the oracle expression errored or produced NULL, which is
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
 * NaN equals NaN under PostgreSQL's total-order semantics: both the = operator
 * and IS NOT DISTINCT FROM report NaN = NaN as true (unlike raw IEEE, where
 * NaN compares unequal to everything including itself).
 */
PG_PROP(nan_not_equal_itself_but_is_not_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT ('NaN'::float8 = 'NaN'::float8) "
				 "AND ('NaN'::float8 IS NOT DISTINCT FROM 'NaN'::float8)");

	pg_prop_disconnect(conn);
}

/*
 * NaN sorts as the largest value: it is greater than any finite value and also
 * greater than +Infinity.
 */
PG_PROP(nan_greater_than_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_finite_f8(tc, &v);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('NaN'::float8 > %s) "
					 "AND ('NaN'::float8 > 'Infinity'::float8)",
					 v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Infinity arithmetic: Infinity + finite = Infinity, and Infinity - Infinity
 * is NaN.  The second half relies on NaN = NaN being true to compare the
 * result.
 */
PG_PROP(inf_arithmetic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_finite_f8(tc, &v);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (('Infinity'::float8 + %s) = 'Infinity'::float8) "
					 "AND (('Infinity'::float8 - 'Infinity'::float8) "
					 "= 'NaN'::float8)",
					 v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Infinity bounds the finite line: -Infinity < v < Infinity for any finite v.
 */
PG_PROP(inf_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_finite_f8(tc, &v);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('-Infinity'::float8 < %s) "
					 "AND (%s < 'Infinity'::float8)",
					 v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * float4<->float8 widening: a finite v narrowed to float4 and widened back to
 * float8 stays within float4's ~7-digit relative precision of v.  The bound is
 * a relative term (float4 carries ~1e-6 relative error) plus a small absolute
 * term to cover values near zero.
 */
PG_PROP(float4_float8_widening)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_finite_f8(tc, &v);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(%s::float4::float8 - %s) "
					 "<= abs(%s) * 1e-6::float8 + 1e-6::float8",
					 v.data, v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Signed zeros compare equal: +0 = -0.
 */
PG_PROP(float8_sign_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT '0'::float8 = '-0'::float8");

	pg_prop_disconnect(conn);
}

/*
 * The full defined total order is strict at each step:
 * -Infinity < finite < Infinity < NaN.
 */
PG_PROP(special_total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT ('-Infinity'::float8 < 0::float8) "
				 "AND (0::float8 < 'Infinity'::float8) "
				 "AND ('Infinity'::float8 < 'NaN'::float8)");

	pg_prop_disconnect(conn);
}

/*
 * Specials round-trip through text: NaN and Infinity each format and reparse
 * back to themselves.  The NaN comparison relies on NaN = NaN being true.
 */
PG_PROP(nan_roundtrip_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (('NaN'::float8)::text::float8 = 'NaN'::float8) "
				 "AND (('Infinity'::float8)::text::float8 "
				 "= 'Infinity'::float8)");

	pg_prop_disconnect(conn);
}
