/*-------------------------------------------------------------------------
 *
 * prop_timeops.c
 *	  Property-based tests for time / timetz / timestamp (without time zone).
 *
 * The oracle here is the types' own algebra: text round-trips, field
 * extraction, timestamp subtraction yielding an interval, date_trunc
 * idempotence, and the cast to date.  Each property is a SELECT of a boolean
 * that must be "t" for every drawn input; a NULL or SQL error is a bug.  We
 * use plain TIMESTAMP (no tz) to avoid DST ambiguity, and restrict day to
 * 1..28 so no month-length edge case can make a literal invalid.
 *
 * Distinct from the "date", "timestamptz", "interval" and "intervalparts"
 * areas: this targets the time and timestamp-without-tz types specifically.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_timeops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "timeops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operation errored or produced NULL on a valid input,
 * which is itself a bug -- so either is a failure, reported with the SQL.
 * Copied from prop_numeric.c's require_true (Shape A, scalar boolean oracle).
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
 * Draw a time-of-day and append a TIME literal "TIME '%02d:%02d:%02d'" to
 * buf.  The drawn h/m/s are returned via the out-params so a caller can also
 * build an extract() oracle from the same values.
 */
static void
draw_time(pg_prop_tc *tc, StringInfo buf, int *h, int *m, int *s)
{
	*h = (int) pg_prop_draw_int(tc, 0, 23);
	*m = (int) pg_prop_draw_int(tc, 0, 59);
	*s = (int) pg_prop_draw_int(tc, 0, 59);
	appendStringInfo(buf, "TIME '%02d:%02d:%02d'", *h, *m, *s);
}

/*
 * Draw a date-and-time and append a plain TIMESTAMP (no tz) literal
 * "TIMESTAMP '%04d-%02d-%02d %02d:%02d:%02d'" to buf.  Day is restricted to
 * 1..28 so every (year, month, day) is a valid calendar date.  Components are
 * returned so a caller can build make_date()/extract() oracles.
 */
static void
draw_timestamp(pg_prop_tc *tc, StringInfo buf,
			   int *y, int *mo, int *d, int *h, int *m, int *s)
{
	*y = (int) pg_prop_draw_int(tc, 2000, 2030);
	*mo = (int) pg_prop_draw_int(tc, 1, 12);
	*d = (int) pg_prop_draw_int(tc, 1, 28);
	*h = (int) pg_prop_draw_int(tc, 0, 23);
	*m = (int) pg_prop_draw_int(tc, 0, 59);
	*s = (int) pg_prop_draw_int(tc, 0, 59);
	appendStringInfo(buf, "TIMESTAMP '%04d-%02d-%02d %02d:%02d:%02d'",
					 *y, *mo, *d, *h, *m, *s);
}

/*
 * 1. time_text_roundtrip: (T)::text::time = T for a drawn TIME.
 */
PG_PROP(time_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t,
				q;
	int			h,
				m,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&t);
	draw_time(tc, &t, &h, &m, &s);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s)::text::time = %s)", t.data, t.data);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. timestamp_text_roundtrip: (TS)::text::timestamp = TS.
 */
PG_PROP(timestamp_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ts,
				q;
	int			y,
				mo,
				d,
				h,
				m,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ts);
	draw_timestamp(tc, &ts, &y, &mo, &d, &h, &m, &s);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s)::text::timestamp = %s)",
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. time_extract_fields: extract(hour from T)=h, extract(minute from T)=m,
 * extract(second from T)=s for the drawn h, m, s.
 */
PG_PROP(time_extract_fields)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t,
				q;
	int			h,
				m,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&t);
	draw_time(tc, &t, &h, &m, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(hour from %s) = %d "
					 "AND extract(minute from %s) = %d "
					 "AND extract(second from %s) = %d",
					 t.data, h, t.data, m, t.data, s);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. timestamp_minus_timestamp_interval: TS2 - TS1 is an interval, and
 * TS1 + (TS2 - TS1) = TS2 for two drawn timestamps.  The pg_typeof check
 * confirms the subtraction yields interval; the identity confirms the
 * round-trip through that interval.
 */
PG_PROP(timestamp_minus_timestamp_interval)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ts1,
				ts2,
				q;
	int			y,
				mo,
				d,
				h,
				m,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ts1);
	initStringInfo(&ts2);
	draw_timestamp(tc, &ts1, &y, &mo, &d, &h, &m, &s);
	draw_timestamp(tc, &ts2, &y, &mo, &d, &h, &m, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT pg_typeof(%s - %s) = 'interval'::regtype "
					 "AND (%s + (%s - %s)) = %s",
					 ts2.data, ts1.data,
					 ts1.data, ts2.data, ts1.data, ts2.data);

	require_true(tc, conn, q.data);

	pfree(ts1.data);
	pfree(ts2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. timestamp_plus_interval_roundtrip:
 * TS + make_interval(days=>d) - make_interval(days=>d) = TS for d in -100..100.
 * Adding then subtracting the same whole-day interval is an identity on a
 * plain (no-tz) timestamp, where there is no DST discontinuity.
 */
PG_PROP(timestamp_plus_interval_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ts,
				q;
	int			y,
				mo,
				d,
				h,
				m,
				s;
	int			days;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ts);
	draw_timestamp(tc, &ts, &y, &mo, &d, &h, &m, &s);
	days = (int) pg_prop_draw_int(tc, -100, 100);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s + make_interval(days => (%d)) "
					 "- make_interval(days => (%d))) = %s",
					 ts.data, days, days, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. time_order_total: for two drawn times A, B exactly one of <, =, > holds,
 * so the three comparisons' integer casts sum to 1.
 */
PG_PROP(time_order_total)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;
	int			h,
				m,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_time(tc, &a, &h, &m, &s);
	draw_time(tc, &b, &h, &m, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%s < %s)::int + (%s = %s)::int "
					 "+ (%s > %s)::int) = 1)",
					 a.data, b.data, a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. date_trunc_idempotent:
 * date_trunc('hour', date_trunc('hour', TS)) = date_trunc('hour', TS).
 * Truncating an already-truncated value is a no-op, so applying it twice
 * equals applying it once.
 */
PG_PROP(date_trunc_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ts,
				q;
	int			y,
				mo,
				d,
				h,
				m,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ts);
	draw_timestamp(tc, &ts, &y, &mo, &d, &h, &m, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT date_trunc('hour', date_trunc('hour', %s)) "
					 "= date_trunc('hour', %s)",
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 8. timestamp_cast_to_date: (TS)::date equals the date built from the drawn
 * year/month/day components: (TS)::date = make_date(y, mo, d).
 */
PG_PROP(timestamp_cast_to_date)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ts,
				q;
	int			y,
				mo,
				d,
				h,
				m,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ts);
	draw_timestamp(tc, &ts, &y, &mo, &d, &h, &m, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s)::date = make_date(%d, %d, %d)",
					 ts.data, y, mo, d);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
