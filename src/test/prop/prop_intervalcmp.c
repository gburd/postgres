/*-------------------------------------------------------------------------
 *
 * prop_intervalcmp.c
 *	  Property-based tests for interval comparison, ordering, and arithmetic.
 *
 * This area targets the interval type's algebra distinct from parsing: the
 * +/- operators, scalar multiply and divide, negation, and the total order
 * (interval_cmp / <,=,>) plus how it interacts with timestamp addition.  The
 * oracle is the type's own algebra: with day/hour/minute fields only (no
 * month ambiguity), interval add/subtract/negate are exact and scalar * and /
 * by small integers stay exact, so these identities must hold with no slack.
 * Any "f" result is a bug in interval arithmetic or comparison, not rounding.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_intervalcmp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "intervalcmp"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the arithmetic errored or produced NULL on finite inputs,
 * which is itself a bug -- so either is a failure, reported with the SQL.
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
 * Draw an interval field count: days -100..100, hours -50..50, mins -120..120.
 * Each returns a plain int the caller substitutes into make_interval().  The
 * ranges span zero and the boundaries the engine favours (min/max/0/+-1).
 */
static int
draw_days(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, -100, 100);
}

static int
draw_hours(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, -50, 50);
}

static int
draw_mins(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, -120, 120);
}

/*
 * add_commutative: make_interval(days => a) + make_interval(days => b) equals
 * make_interval(days => b) + make_interval(days => a).  Day-only intervals so
 * addition carries no month/day/time ambiguity; interval + interval must
 * commute exactly.
 */
PG_PROP(add_commutative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = draw_days(tc);
	b = draw_days(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (make_interval(days => (%d)) "
					 "+ make_interval(days => (%d))) "
					 "= (make_interval(days => (%d)) "
					 "+ make_interval(days => (%d)))",
					 a, b, b, a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * negate_inverse: I + (-I) = interval '0' for I = make_interval(days, hours).
 * Negation then addition of a value and its inverse must collapse exactly to
 * the zero interval.
 */
PG_PROP(negate_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			d,
				h;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	d = draw_days(tc);
	h = draw_hours(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (make_interval(days => (%d), hours => (%d)) "
					 "+ (- make_interval(days => (%d), hours => (%d)))) "
					 "= interval '0'",
					 d, h, d, h);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * scalar_mul_distributes: 2 * I = I + I.  Multiplying an interval by the
 * scalar 2 must equal adding it to itself; a carry bug in interval_mul would
 * break this.
 */
PG_PROP(scalar_mul_distributes)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			d,
				h,
				m;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	d = draw_days(tc);
	h = draw_hours(tc);
	m = draw_mins(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (2 * make_interval(days => (%d), hours => (%d), "
					 "mins => (%d))) "
					 "= (make_interval(days => (%d), hours => (%d), "
					 "mins => (%d)) "
					 "+ make_interval(days => (%d), hours => (%d), "
					 "mins => (%d)))",
					 d, h, m, d, h, m, d, h, m);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * div_mul_roundtrip: (I * 4) / 4 = I for I built from days and hours only.
 * Days and hours keep every intermediate integral, so multiplying by 4 then
 * dividing by 4 round-trips exactly with no fractional carry across fields.
 */
PG_PROP(div_mul_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			d,
				h;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	d = draw_days(tc);
	h = draw_hours(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((make_interval(days => (%d), hours => (%d)) "
					 "* 4) / 4) "
					 "= make_interval(days => (%d), hours => (%d))",
					 d, h, d, h);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * total_order: for two all-days intervals A and B, exactly one of A < B,
 * A = B, A > B holds.  Day-only intervals make the comparison well-defined
 * (no month/day/time field crossing), so the three relations must partition.
 */
PG_PROP(total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = draw_days(tc);
	b = draw_days(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (CASE WHEN A < B THEN 1 ELSE 0 END) "
					 "+ (CASE WHEN A = B THEN 1 ELSE 0 END) "
					 "+ (CASE WHEN A > B THEN 1 ELSE 0 END) = 1 "
					 "FROM (SELECT make_interval(days => (%d)) AS A, "
					 "make_interval(days => (%d)) AS B) s",
					 a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * add_to_timestamp_order: for day-only intervals A, B with A.days < B.days
 * (so A < B as intervals), ts + A < ts + B at a fixed timestamp.  Monotonicity
 * of timestamp + interval must respect the interval order.  A and B are built
 * so A's day count is strictly less than B's.
 */
PG_PROP(add_to_timestamp_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = draw_days(tc);
	b = draw_days(tc);

	/* force a strictly less than b */
	if (a > b)
	{
		int			t = a;

		a = b;
		b = t;
	}
	if (a == b)
		b = b + 1;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (timestamp '2000-06-15 12:00:00' "
					 "+ make_interval(days => (%d))) "
					 "< (timestamp '2000-06-15 12:00:00' "
					 "+ make_interval(days => (%d)))",
					 a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * zero_identity: I + interval '0' = I.  The zero interval is the additive
 * identity, so adding it must return the operand unchanged.
 */
PG_PROP(zero_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			d,
				h,
				m;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	d = draw_days(tc);
	h = draw_hours(tc);
	m = draw_mins(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (make_interval(days => (%d), hours => (%d), "
					 "mins => (%d)) + interval '0') "
					 "= make_interval(days => (%d), hours => (%d), "
					 "mins => (%d))",
					 d, h, m, d, h, m);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * mul_zero: I * 0 = interval '0'.  Scaling any interval by the scalar 0 must
 * annihilate every field to the zero interval.
 */
PG_PROP(mul_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			d,
				h,
				m;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	d = draw_days(tc);
	h = draw_hours(tc);
	m = draw_mins(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (make_interval(days => (%d), hours => (%d), "
					 "mins => (%d)) * 0) = interval '0'",
					 d, h, m);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
