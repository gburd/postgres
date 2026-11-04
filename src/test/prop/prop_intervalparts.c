/*-------------------------------------------------------------------------
 *
 * prop_intervalparts.c
 *	  Property-based tests for interval field extraction and justification
 *	  (extract/date_part on intervals, justify_hours/justify_days,
 *	  make_interval), in src/backend/utils/adt/timestamp.c.
 *
 * Interval field extraction and make_interval have precise relationships: the
 * fields passed to make_interval are stored verbatim and extract() returns
 * them back (within non-carrying ranges); justify_hours and justify_days
 * normalize carries (24h -> 1 day, 30d -> 1 month) without changing the value
 * an interval represents; and extract(epoch) of a time-only interval is the
 * exact field-weighted sum of seconds.  These properties check them.
 *
 * This is a distinct area from the existing "interval" tests (text round-trip,
 * add/subtract inverse, total order, justify_hours value-preservation): here
 * the subject is field extraction and the make_interval constructor.
 *
 * The oracle is the exact arithmetic of each field: make_interval stores the
 * named fields, extract() reports them, justify redistributes them within
 * fixed group sizes, and a day is exactly 86400 seconds for an interval's
 * epoch.  Each invariant is computed as a boolean in SQL and required "t".
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_intervalparts.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "intervalparts"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the expression errored or produced NULL on finite inputs,
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
 * 1. make_interval_extract_roundtrip
 *
 * For an interval built from a SINGLE field within a range that does not carry
 * into a coarser field, extract() of that field returns the exact input.  A
 * field count is drawn per non-carrying range (year, month, day, hour, minute,
 * second) and checked individually.  Days do not auto-carry to months (that
 * only happens under justify), so an arbitrary small day count is safe; hours
 * stay in -23..23, minutes/seconds in -59..59.
 */
PG_PROP(make_interval_extract_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y;
	int			mo;
	int			d;
	int			h;
	int			mi;
	int			s;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	y = (int) pg_prop_draw_int(tc, -5, 5);
	mo = (int) pg_prop_draw_int(tc, -11, 11);
	d = (int) pg_prop_draw_int(tc, -40, 40);
	h = (int) pg_prop_draw_int(tc, -23, 23);
	mi = (int) pg_prop_draw_int(tc, -59, 59);
	s = (int) pg_prop_draw_int(tc, -59, 59);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(year from make_interval(years => %d)) = %d "
					 "AND extract(month from make_interval(months => %d)) = %d "
					 "AND extract(day from make_interval(days => %d)) = %d "
					 "AND extract(hour from make_interval(hours => %d)) = %d "
					 "AND extract(minute from make_interval(mins => %d)) = %d "
					 "AND extract(second from make_interval(secs => %d)) = %d",
					 y, y, mo, mo, d, d, h, h, mi, mi, s, s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. justify_hours_normalizes_hours
 *
 * justify_hours moves whole 24-hour groups into the day field, so after
 * justify_hours the hour field is strictly within (-24, 24).
 */
PG_PROP(justify_hours_normalizes_hours)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			h;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	h = (int) pg_prop_draw_int(tc, -200, 200);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(extract(hour from "
					 "justify_hours(make_interval(hours => %d)))) < 24",
					 h);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. justify_days_normalizes_days
 *
 * justify_days moves whole 30-day groups into the month field, so after
 * justify_days the day field is strictly within (-30, 30).
 */
PG_PROP(justify_days_normalizes_days)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	d = (int) pg_prop_draw_int(tc, -400, 400);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(extract(day from "
					 "justify_days(make_interval(days => %d)))) < 30",
					 d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. justify_preserves_total_when_added
 *
 * justify_interval only redistributes the fields of an interval, so adding it
 * to a fixed timestamp yields the same instant as adding its justified form.
 * justify_hours redistributes 24-hour groups into days but never carries days
 * into months, so for a plain TIMESTAMP (no DST, where 1 day == 24 hours
 * exactly) adding the interval before or after justify_hours gives the same
 * instant.  (justify_interval is avoided here: its 30-day -> 1-month carry
 * would legitimately move the result, since 30 days and 1 month are different
 * calendar spans.)
 */
PG_PROP(justify_preserves_total_when_added)
{
	static const char *const ref = "TIMESTAMP '2000-06-15 00:00:00'";
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			d;
	int			h;
	int			m;
	int			s;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	d = (int) pg_prop_draw_int(tc, -40, 40);
	h = (int) pg_prop_draw_int(tc, -50, 50);
	m = (int) pg_prop_draw_int(tc, -120, 120);
	s = (int) pg_prop_draw_int(tc, -120, 120);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s + make_interval(days => %d, hours => %d, "
					 "mins => %d, secs => %d)) "
					 "= (%s + justify_hours(make_interval(days => %d, "
					 "hours => %d, mins => %d, secs => %d)))",
					 ref, d, h, m, s,
					 ref, d, h, m, s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. epoch_additive_for_time_fields
 *
 * For a time-only interval (days/hours/mins/secs, no months/years) the epoch
 * is exactly the field-weighted sum of seconds, with a day counted as 86400
 * seconds (intervals, unlike timestamps across DST, treat a day as a fixed
 * 86400s).
 */
PG_PROP(epoch_additive_for_time_fields)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			d;
	int			h;
	int			m;
	int			s;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	d = (int) pg_prop_draw_int(tc, -40, 40);
	h = (int) pg_prop_draw_int(tc, -50, 50);
	m = (int) pg_prop_draw_int(tc, -120, 120);
	s = (int) pg_prop_draw_int(tc, -120, 120);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(epoch from make_interval(days => %d, "
					 "hours => %d, mins => %d, secs => %d)) "
					 "= (%d*86400 + %d*3600 + %d*60 + %d)::numeric",
					 d, h, m, s,
					 d, h, m, s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. make_interval_additive
 *
 * Interval addition is componentwise, so make_interval(months => a) +
 * make_interval(months => b) equals make_interval(months => a + b).
 */
PG_PROP(make_interval_additive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	int			b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, -15, 15);
	b = (int) pg_prop_draw_int(tc, -15, 15);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT make_interval(months => %d) "
					 "+ make_interval(months => %d) "
					 "= make_interval(months => %d)",
					 a, b, a + b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. negate_make_interval
 *
 * Negating an interval negates every field, so -make_interval(y, mo, d, h)
 * equals make_interval with all those fields negated.
 */
PG_PROP(negate_make_interval)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y;
	int			mo;
	int			d;
	int			h;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	y = (int) pg_prop_draw_int(tc, -5, 5);
	mo = (int) pg_prop_draw_int(tc, -15, 15);
	d = (int) pg_prop_draw_int(tc, -40, 40);
	h = (int) pg_prop_draw_int(tc, -50, 50);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (- make_interval(years => %d, months => %d, "
					 "days => %d, hours => %d)) "
					 "= make_interval(years => %d, months => %d, "
					 "days => %d, hours => %d)",
					 y, mo, d, h,
					 -y, -mo, -d, -h);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
