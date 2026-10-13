/*-------------------------------------------------------------------------
 *
 * prop_scalarfn.c
 *	  Property-based tests for miscellaneous scalar functions.
 *
 * A grab-bag of scalar functions with clean algebraic contracts not covered
 * by the type-specific areas: num_nulls/num_nonnulls, mod/div sign rules, the
 * array form of width_bucket, the degrees/radians inverse, bit_count, and the
 * gcd*lcm identity.  Each property is a boolean the backend must answer "t".
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see pg_prop.h
 * and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_scalarfn.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "scalarfn"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Run a boolean SELECT and require it to return "t". */
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

/* Emit an int literal, parenthesizing negatives so a cast binds correctly. */
static void
append_int(StringInfo buf, int n)
{
	if (n < 0)
		appendStringInfo(buf, "(%d)", n);
	else
		appendStringInfo(buf, "%d", n);
}

/* An int argument that is NULL ~25% of the time. */
static void
append_int_or_null(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 3) == 0)
		appendStringInfoString(buf, "NULL::int");
	else
		append_int(buf, (int) pg_prop_draw_int(tc, -20, 20));
}

/* num_nulls(a,b,c) + num_nonnulls(a,b,c) = 3. */
PG_PROP(num_nulls_nonnulls)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	{
		StringInfoData args;

		/* same three args to both functions so the identity holds */
		initStringInfo(&args);
		append_int_or_null(tc, &args);
		appendStringInfoString(&args, ", ");
		append_int_or_null(tc, &args);
		appendStringInfoString(&args, ", ");
		append_int_or_null(tc, &args);

		appendStringInfo(&q,
						 "SELECT num_nulls(%s) + num_nonnulls(%s) = 3 "
						 "AND num_nulls(1,2,3) = 0 "
						 "AND num_nonnulls(1,2,3) = 3 "
						 "AND num_nulls(NULL::int, 1, NULL::int, 2) = 2",
						 args.data, args.data);
		pfree(args.data);
	}

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* mod(a,b) has the sign of a (or is zero): sign(mod(a,b)) IN (0, sign(a)). */
PG_PROP(mod_sign)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, -1000, 1000);
	b = (int) pg_prop_draw_int(tc, 1, 1000);
	if (pg_prop_draw_bool(tc))
		b = -b;

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT sign(mod(");
	append_int(&q, a);
	appendStringInfoString(&q, ", ");
	append_int(&q, b);
	appendStringInfoString(&q, ")) IN (0, sign(");
	append_int(&q, a);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* div(a,b) = trunc(a::numeric / b) for b <> 0. */
PG_PROP(div_func)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, -1000, 1000);
	b = (int) pg_prop_draw_int(tc, 1, 1000);
	if (pg_prop_draw_bool(tc))
		b = -b;

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT div(");
	append_int(&q, a);
	appendStringInfoString(&q, "::numeric, ");
	append_int(&q, b);
	appendStringInfoString(&q, ") = trunc(");
	append_int(&q, a);
	appendStringInfoString(&q, "::numeric / ");
	append_int(&q, b);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* width_bucket(array form) is monotone: v1 <= v2 => bucket(v1) <= bucket(v2). */
PG_PROP(width_bucket_array_monotone)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			v1,
				v2,
				t;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	v1 = (int) pg_prop_draw_int(tc, -10, 40);
	v2 = (int) pg_prop_draw_int(tc, -10, 40);
	if (v1 > v2)
	{
		t = v1;
		v1 = v2;
		v2 = t;
	}

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT width_bucket(");
	append_int(&q, v1);
	appendStringInfoString(&q, ", ARRAY[0,10,20,30]) <= width_bucket(");
	append_int(&q, v2);
	appendStringInfoString(&q, ", ARRAY[0,10,20,30])");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* radians(degrees(x)) = x within tolerance. */
PG_PROP(degrees_radians_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			whole,
				frac;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	whole = (int) pg_prop_draw_int(tc, -5, 5);
	frac = (int) pg_prop_draw_int(tc, 0, 9);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(radians(degrees('%d.%d'::float8)) "
					 "- '%d.%d'::float8) < 1e-9",
					 whole, frac, whole, frac);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* bit_count of 0xff is 8; of a zero byte is 0. */
PG_PROP(bit_count_bytea)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT bit_count(decode('ff', 'hex')) = 8 "
				 "AND bit_count(decode('00', 'hex')) = 0 "
				 "AND bit_count(decode('ffff', 'hex')) = 16");

	pg_prop_disconnect(conn);
}

/* gcd(a,b) * lcm(a,b) = a*b for positive a, b. */
PG_PROP(gcd_lcm_product)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, 1, 100);
	b = (int) pg_prop_draw_int(tc, 1, 100);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT gcd(%d, %d) * lcm(%d, %d) = %d * %d",
					 a, b, a, b, a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
