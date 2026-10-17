/*-------------------------------------------------------------------------
 *
 * prop_genseries2.c
 *	  Property-based tests for generate_series over timestamps/numerics and
 *	  for generate_subscripts.
 *
 * These pin exact count/first/last/sum invariants of the timestamp and
 * numeric generate_series overloads, and the subscript range emitted by
 * generate_subscripts, which the integer-only prop_genseries.c does not
 * cover.  A wrong count, bound, or sum on valid input is a bug.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_genseries2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "genseries2"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on valid inputs,
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
 * A daily timestamp series from 2020-01-01 to 2020-01-01 + d days, stepping
 * one day, emits exactly d + 1 rows for d in 0..30.
 */
PG_PROP(ts_series_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	d = pg_prop_draw_int(tc, 0, 30);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM generate_series("
					 "TIMESTAMP '2020-01-01', "
					 "TIMESTAMP '2020-01-01' + make_interval(days => %lld), "
					 "interval '1 day')) = (%lld + 1)",
					 (long long) d, (long long) d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A numeric series from 0 to n stepping 0.5 emits floor(n / 0.5) + 1 = 2n + 1
 * rows for n in 0..10.
 */
PG_PROP(numeric_series_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, 0, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM generate_series("
					 "0::numeric, %lld::numeric, 0.5)) = (2 * %lld + 1)",
					 (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For an integer series with a <= b and default step 1, the minimum value is
 * the start a and the maximum is the end b.  Endpoints are drawn in -10..10
 * and swapped in C so a <= b.
 */
PG_PROP(series_first_last)
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

	a = pg_prop_draw_int(tc, -10, 10);
	b = pg_prop_draw_int(tc, -10, 10);
	if (a > b)
	{
		tmp = a;
		a = b;
		b = tmp;
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT min(g) FROM generate_series((%lld), (%lld)) g) = (%lld) "
					 "AND (SELECT max(g) FROM generate_series((%lld), (%lld)) g) = (%lld)",
					 (long long) a, (long long) b, (long long) a,
					 (long long) a, (long long) b, (long long) b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * generate_subscripts(arr, 1) over an int array of m elements (m in 0..6)
 * yields exactly the subscripts 1..m: m rows, and for m >= 1 the min is 1 and
 * the max is m.  The empty case (m = 0) is guarded: count = 0, and min is
 * NULL (CASE yields NULL), matched with IS NOT DISTINCT FROM.
 */
PG_PROP(subscripts_match_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		m,
				i;
	StringInfoData arr;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	m = pg_prop_draw_int(tc, 0, 6);

	/* build an int4 array literal ARRAY[...]::int[] with m elements */
	initStringInfo(&arr);
	appendStringInfoString(&arr, "ARRAY[");
	for (i = 0; i < m; i++)
		appendStringInfo(&arr, "%s%lld", i ? ", " : "", (long long) i);
	appendStringInfoString(&arr, "]::int[]");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM generate_subscripts(%s, 1)) = (%lld) "
					 "AND (SELECT min(s) FROM generate_subscripts(%s, 1) s) "
					 "IS NOT DISTINCT FROM (CASE WHEN %lld > 0 THEN 1 ELSE NULL END) "
					 "AND (SELECT max(s) FROM generate_subscripts(%s, 1) s) "
					 "IS NOT DISTINCT FROM (CASE WHEN %lld > 0 THEN %lld ELSE NULL END)",
					 arr.data, (long long) m,
					 arr.data, (long long) m,
					 arr.data, (long long) m, (long long) m);

	require_true(tc, conn, q.data);

	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A descending integer series generate_series(b, a, -1) with a <= b emits
 * exactly b - a + 1 rows.  Endpoints are drawn in -10..10 and swapped in C so
 * a <= b.
 */
PG_PROP(series_step_negative)
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

	a = pg_prop_draw_int(tc, -10, 10);
	b = pg_prop_draw_int(tc, -10, 10);
	if (a > b)
	{
		tmp = a;
		a = b;
		b = tmp;
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM generate_series((%lld), (%lld), (-1))) "
					 "= ((%lld) - (%lld) + 1)",
					 (long long) b, (long long) a,
					 (long long) b, (long long) a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * When the step points away from the end -- generate_series(0, 5, -1) -- the
 * series is empty, so its count is 0.  A fixed-input invariant, no draw.
 */
PG_PROP(series_empty_when_step_wrong)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM generate_series(0, 5, (-1))) = 0");

	pg_prop_disconnect(conn);
}

/*
 * The sum of generate_series(1, n) equals the triangular number
 * n * (n + 1) / 2 for n in 0..20.  For n = 0 the series is empty, so sum() is
 * NULL; coalesce to 0 matches the formula's 0.
 */
PG_PROP(series_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, 0, 20);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce((SELECT sum(g) FROM generate_series(1, %lld) g), 0) "
					 "= (%lld * (%lld + 1) / 2)",
					 (long long) n, (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
