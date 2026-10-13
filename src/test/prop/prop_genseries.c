/*-------------------------------------------------------------------------
 *
 * prop_genseries.c
 *	  Property-based tests for the generate_series set-returning function.
 *
 * generate_series has exact count/first/last/step invariants; these pin them
 * down across positive, negative, and degenerate steps.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_genseries.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "genseries"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means generate_series errored or produced NULL on valid inputs,
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
 * Draw a non-zero step in [-20, 20] \ {0}: draw a magnitude in 1..20 and a
 * random sign.  generate_series errors on step 0, so this never yields 0.
 */
static int64
draw_step(pg_prop_tc *tc)
{
	int64		mag = pg_prop_draw_int(tc, 1, 20);
	bool		neg = pg_prop_draw_bool(tc);

	return neg ? -mag : mag;
}

/*
 * The number of elements of generate_series(a, b, s) is exactly:
 *   if (s > 0 and a > b) or (s < 0 and a < b) then 0
 *   else floor(abs(b - a) / abs(s)) + 1
 * This is the core count invariant.  Compute both sides in SQL and compare.
 */
PG_PROP(count_matches_formula)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b,
				s;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -1000, 1000);
	b = pg_prop_draw_int(tc, -1000, 1000);
	s = draw_step(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM generate_series(%lld, %lld, %lld)) "
					 "= CASE WHEN (%lld > 0 AND %lld > %lld) OR (%lld < 0 AND %lld < %lld) THEN 0 "
					 "ELSE floor(abs(%lld - %lld) / abs(%lld))::int + 1 END",
					 (long long) a, (long long) b, (long long) s,
					 (long long) s, (long long) a, (long long) b,
					 (long long) s, (long long) a, (long long) b,
					 (long long) b, (long long) a, (long long) s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The first emitted value (in series order) is always the start a, when the
 * series is non-empty.  coalesce(..., true) makes an empty series pass
 * vacuously (the LIMIT 1 subselect is NULL, so the equality is NULL).
 */
PG_PROP(first_is_start_when_nonempty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b,
				s;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -1000, 1000);
	b = pg_prop_draw_int(tc, -1000, 1000);
	s = draw_step(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce((SELECT g FROM generate_series(%lld, %lld, %lld) g LIMIT 1) = %lld, true)",
					 (long long) a, (long long) b, (long long) s,
					 (long long) a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Every consecutive pair of elements differs by exactly the step s.  WITH
 * ORDINALITY gives a deterministic position to lag over.  bool_and over the
 * differences, coalesce'd to true so a 0- or 1-element series (no consecutive
 * pair) passes vacuously.
 */
PG_PROP(step_between_consecutive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b,
				s;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -1000, 1000);
	b = pg_prop_draw_int(tc, -1000, 1000);
	s = draw_step(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(bool_and(d = %lld), true) FROM ("
					 "SELECT g - lag(g) OVER (ORDER BY ord) AS d "
					 "FROM generate_series(%lld, %lld, %lld) WITH ORDINALITY AS t(g, ord)"
					 ") s WHERE d IS NOT NULL",
					 (long long) s,
					 (long long) a, (long long) b, (long long) s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Every element lies within [least(a, b), greatest(a, b)].  bool_and over the
 * BETWEEN test, coalesce'd to true so an empty series passes vacuously.
 */
PG_PROP(all_within_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b,
				s;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -1000, 1000);
	b = pg_prop_draw_int(tc, -1000, 1000);
	s = draw_step(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(bool_and(g BETWEEN least(%lld, %lld) AND greatest(%lld, %lld)), true) "
					 "FROM generate_series(%lld, %lld, %lld) g",
					 (long long) a, (long long) b, (long long) a, (long long) b,
					 (long long) a, (long long) b, (long long) s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * generate_series(a, b, s) and generate_series(b, a, -s) walk the same values
 * in opposite directions and so have equal sorted arrays -- but ONLY when
 * (b - a) is an exact multiple of s, otherwise the two walks land on different
 * grids and the last term differs.  Use step +/-1, where every integer in the
 * span is hit, so the reversal is always exact.  IS NOT DISTINCT FROM also
 * passes the empty case (both NULL).
 */
PG_PROP(reverse_step_reverses_series)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b,
				s;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -1000, 1000);
	b = pg_prop_draw_int(tc, -1000, 1000);
	s = pg_prop_draw_bool(tc) ? 1 : -1;	/* unit step: span is always a multiple */

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT array_agg(g ORDER BY g) FROM generate_series(%lld, %lld, %lld) g) "
					 "IS NOT DISTINCT FROM "
					 "(SELECT array_agg(g ORDER BY g) FROM generate_series(%lld, %lld, -(%lld)) g)",
					 (long long) a, (long long) b, (long long) s,
					 (long long) b, (long long) a, (long long) s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For step 1 and a <= b, the count is exactly b - a + 1.  a <= b is enforced
 * by swapping the drawn endpoints in C before building the SQL.
 */
PG_PROP(unit_step_count_is_span_plus_one)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b,
				tmp;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -1000, 1000);
	b = pg_prop_draw_int(tc, -1000, 1000);
	if (a > b)
	{
		tmp = a;
		a = b;
		b = tmp;
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM generate_series(%lld, %lld, 1)) = (%lld - %lld + 1)",
					 (long long) a, (long long) b,
					 (long long) b, (long long) a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
