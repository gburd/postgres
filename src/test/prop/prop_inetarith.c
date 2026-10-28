/*-------------------------------------------------------------------------
 *
 * prop_inetarith.c
 *	  Property-based tests for inet arithmetic (inet +/- int, inet - inet).
 *
 * inet supports adding/subtracting an integer offset and subtracting two
 * inets to get the integer distance; these check the add/subtract inverse and
 * the distance/order consistency, skipping legitimate out-of-range.
 *
 * This is distinct from the "inet" area (prop_inet.c), which covers parsing,
 * containment, and accessors; here the target is the +/- arithmetic.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_inetarith.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "inetarith"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw an IPv4 host inet literal into buf as a quoted SQL literal cast to
 * inet, e.g. "'10.0.0.1'::inet".  No mask is attached (a bare host), so that
 * inet - inet is a well-defined integer address distance and inet +/- int
 * arithmetic stays within the 32-bit IPv4 address space.
 */
static void
draw_host_inet(pg_prop_tc *tc, StringInfo buf)
{
	int			a = (int) pg_prop_draw_int(tc, 0, 255);
	int			b = (int) pg_prop_draw_int(tc, 0, 255);
	int			c = (int) pg_prop_draw_int(tc, 0, 255);
	int			d = (int) pg_prop_draw_int(tc, 0, 255);

	appendStringInfo(buf, "'%d.%d.%d.%d'::inet", a, b, c, d);
}

/*
 * True if a SQL error message describes a legitimate out-of-range or
 * overflow condition -- adding/subtracting an offset near 0.0.0.0 or
 * 255.255.255.255 can run off the end of the address space, which is a
 * correct error, not a bug.  Such cases are skipped, not failed.
 */
static bool
is_overflow(const char *msg)
{
	if (msg == NULL)
		return false;
	return strstr(msg, "out of range") != NULL ||
		strstr(msg, "would overflow") != NULL ||
		strstr(msg, "cannot") != NULL ||
		strstr(msg, "result is out of range") != NULL;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a validly-generated value errored or produced NULL, which
 * is itself a bug -- so either is a failure, reported with the SQL.
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
 * Like require_true, but a SQL error that is_overflow() recognises as a
 * legitimate out-of-range/overflow is skipped (the case is assumed away)
 * rather than failed.  Any other error, a NULL, or a non-"t" result fails.
 */
static void
require_true_or_skip(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		if (is_overflow(err))
		{
			if (err)
				pg_free(err);
			pg_prop_assume(tc, false);
			return;				/* not reached */
		}
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
 * Adding then subtracting an integer offset returns the address:
 * (I + k) - k = I.  Near 0.0.0.0 or 255.255.255.255 the intermediate I + k
 * can run off the end of the address space, a legitimate out-of-range error
 * that is skipped rather than failed.
 */
PG_PROP(add_sub_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		k;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_host_inet(tc, &i);
	k = pg_prop_draw_int(tc, -1000000, 1000000);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s + %lld) - %lld = %s",
					 i.data, (long long) k, (long long) k, i.data);

	require_true_or_skip(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * inet - inet yields the integer distance; adding it back to I gives J:
 * I + (J - I) = J.  Both are IPv4 hosts, so the difference is always defined
 * and no out-of-range should occur; still skip overflow defensively.
 */
PG_PROP(diff_then_add)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData i,
				j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	initStringInfo(&j);
	draw_host_inet(tc, &i);
	draw_host_inet(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT %s + (%s - %s) = %s",
					 i.data, j.data, i.data, j.data);

	require_true_or_skip(tc, conn, q.data);

	pfree(i.data);
	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The integer difference's sign agrees with inet ordering:
 * (J - I) > 0 iff I < J, and (J - I) = 0 iff I = J.
 */
PG_PROP(diff_sign_matches_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData i,
				j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	initStringInfo(&j);
	draw_host_inet(tc, &i);
	draw_host_inet(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s - %s) > 0) = (%s < %s) "
					 "AND ((%s - %s) = 0) = (%s = %s)",
					 j.data, i.data, i.data, j.data,
					 j.data, i.data, i.data, j.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Adding a positive offset yields a greater address: I + k > I for k > 0,
 * when no overflow.  Near 255.255.255.255 the addition can run off the end,
 * a legitimate out-of-range error that is skipped rather than failed.
 */
PG_PROP(add_monotonic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		k;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_host_inet(tc, &i);
	k = pg_prop_draw_int(tc, 1, 1000000);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s + %lld) > %s",
					 i.data, (long long) k, i.data);

	require_true_or_skip(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Adding zero is the identity: I + 0 = I.
 */
PG_PROP(add_zero_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_host_inet(tc, &i);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT %s + 0 = %s", i.data, i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Addition associates with integer sum: (I + a) + b = I + (a + b), when no
 * overflow.  Either side can run off the address space near the ends, a
 * legitimate out-of-range error that is skipped rather than failed.
 */
PG_PROP(add_associates_with_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_host_inet(tc, &i);
	a = pg_prop_draw_int(tc, -100000, 100000);
	b = pg_prop_draw_int(tc, -100000, 100000);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s + %lld) + %lld) = (%s + (%lld + %lld))",
					 i.data, (long long) a, (long long) b,
					 i.data, (long long) a, (long long) b);

	require_true_or_skip(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
