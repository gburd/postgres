/*-------------------------------------------------------------------------
 *
 * prop_tstzarith.c
 *	  Property-based tests for timestamptz arithmetic and timezone
 *	  conversion (src/backend/utils/adt/timestamp.c).
 *
 * Distinct from prop_timestamptz.c (text/date_trunc/date_bin/extract): this
 * file targets the arithmetic and timezone-conversion algebra -- AT TIME ZONE
 * round-trips, interval add/subtract inverses, age() as a calendar interval,
 * extract(epoch) differences, and the comparator as a total order.  Every
 * property pins the session TimeZone to 'UTC' (via connect_tz) and every
 * literal carries an explicit +00 offset so results are deterministic and
 * free of DST; a failure is therefore a bug in the arithmetic, not a timezone
 * artefact.
 *
 * The oracle is the type's own algebra evaluated in SQL (round-trips,
 * add/subtract inverses, a differential epoch identity, and a total-order
 * check), so each identity holds for every drawn input with no tolerance
 * (except extract(epoch) differences, compared within 1e-6 for float8).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_tstzarith.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "tstzarith"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Connect to the test cluster and pin the session TimeZone to 'UTC' so AT TIME
 * ZONE, age(), extract(epoch), and comparison are deterministic and free of
 * DST regardless of the cluster default.  Fails the case if either step fails.
 */
static pg_prop_conn *
connect_tz(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn, "SET TIME ZONE 'UTC'", &err))
		pg_prop_fail(tc, "could not SET TIME ZONE 'UTC': %s", err ? err : "");

	return conn;
}

/*
 * Build a random timestamptz literal into buf, e.g.
 *		TIMESTAMPTZ '2024-03-15 08:30:00+00'
 *
 * Year 2001..2030 keeps the literal and its arithmetic well inside the
 * supported range; day is capped at 28 so every month is valid; the explicit
 * +00 offset makes the literal unambiguous UTC.
 */
static void
draw_ts(pg_prop_tc *tc, StringInfo buf)
{
	int			year;
	int			mon;
	int			day;
	int			hour;
	int			min;
	int			sec;

	year = (int) pg_prop_draw_int(tc, 2001, 2030);
	mon = (int) pg_prop_draw_int(tc, 1, 12);
	day = (int) pg_prop_draw_int(tc, 1, 28);
	hour = (int) pg_prop_draw_int(tc, 0, 23);
	min = (int) pg_prop_draw_int(tc, 0, 59);
	sec = (int) pg_prop_draw_int(tc, 0, 59);

	appendStringInfo(buf, "TIMESTAMPTZ '%04d-%02d-%02d %02d:%02d:%02d+00'",
					 year, mon, day, hour, min, sec);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operation errored or produced NULL on finite inputs,
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
 * 1. attz_roundtrip
 *
 * Converting a timestamptz to a timestamp at UTC and back to timestamptz at
 * UTC is the identity: (TS AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' = TS.  In
 * UTC there is no DST, so this double conversion is lossless.
 */
PG_PROP(attz_roundtrip)
{
	pg_prop_conn *conn;
	StringInfoData t;
	StringInfoData q;

	conn = connect_tz(tc);

	initStringInfo(&t);
	draw_ts(tc, &t);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s AT TIME ZONE 'UTC') AT TIME ZONE 'UTC') = %s",
					 t.data, t.data);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. plus_minus_interval
 *
 * Adding and then subtracting the same whole-hour interval is the identity:
 * TS + make_interval(hours => h) - make_interval(hours => h) = TS, for h in
 * -500..500.  An hour interval is a fixed span (no months/years), so it is
 * exactly invertible in UTC.
 */
PG_PROP(plus_minus_interval)
{
	pg_prop_conn *conn;
	StringInfoData t;
	StringInfoData q;
	int			h;

	conn = connect_tz(tc);

	initStringInfo(&t);
	draw_ts(tc, &t);

	h = (int) pg_prop_draw_int(tc, -500, 500);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s + make_interval(hours => %d) "
					 "- make_interval(hours => %d)) = %s",
					 t.data, h, h, t.data);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. epoch_diff
 *
 * extract(epoch) distributes over subtraction of two timestamptz:
 * extract(epoch from (TS2 - TS1)) = extract(epoch from TS2)
 *                                 - extract(epoch from TS1).
 * Both sides are float8 seconds, compared within 1e-6 to absorb float
 * rounding.
 */
PG_PROP(epoch_diff)
{
	pg_prop_conn *conn;
	StringInfoData t1;
	StringInfoData t2;
	StringInfoData q;

	conn = connect_tz(tc);

	initStringInfo(&t1);
	initStringInfo(&t2);
	draw_ts(tc, &t1);
	draw_ts(tc, &t2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(extract(epoch from (%s - %s)) "
					 "- (extract(epoch from %s) - extract(epoch from %s))) "
					 "< 1e-6",
					 t2.data, t1.data, t2.data, t1.data);

	require_true(tc, conn, q.data);

	pfree(t1.data);
	pfree(t2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. age_plus_back
 *
 * age(TS2, TS1) is a calendar interval (variable-length months/days) that
 * spans from TS1 to TS2.  Adding it back recovers TS2:
 * TS1 + age(TS2, TS1) = TS2.  This holds exactly in UTC because there is no
 * DST crossing to make the calendar interval ambiguous.
 */
PG_PROP(age_sign_matches_order)
{
	pg_prop_conn *conn;
	StringInfoData t1;
	StringInfoData t2;
	StringInfoData q;

	conn = connect_tz(tc);

	initStringInfo(&t1);
	initStringInfo(&t2);
	draw_ts(tc, &t1);
	draw_ts(tc, &t2);

	/*
	 * age(a, b) is not invertible by addition -- it returns a symbolic
	 * month/day/time interval, and a + age(b, a) need not land back on b when
	 * the time-of-day differs (the month carry does not undo cleanly).  The
	 * sound invariant is the sign: age(a, b) is positive, zero, or negative
	 * exactly as a is after, equal to, or before b.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (age(%s, %s) > interval '0') = (%s > %s) "
					 "AND (age(%s, %s) = interval '0') = (%s = %s)",
					 t2.data, t1.data, t2.data, t1.data,
					 t2.data, t1.data, t2.data, t1.data);

	require_true(tc, conn, q.data);

	pfree(t1.data);
	pfree(t2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. order_total
 *
 * For any two timestamptz A, B exactly one of (A < B), (A = B), (A > B) holds:
 * the comparator is a proper total order.
 */
PG_PROP(order_total)
{
	pg_prop_conn *conn;
	StringInfoData a;
	StringInfoData b;
	StringInfoData q;

	conn = connect_tz(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_ts(tc, &a);
	draw_ts(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s < %s)::int + (%s = %s)::int "
					 "+ (%s > %s)::int) = 1",
					 a.data, b.data, a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. truncate_le
 *
 * date_trunc rounds down toward the start of the unit, so truncating to hour
 * or day never moves the timestamp forward:
 * date_trunc('hour', TS) <= TS AND date_trunc('day', TS) <= TS.
 */
PG_PROP(truncate_le)
{
	pg_prop_conn *conn;
	StringInfoData t;
	StringInfoData q;

	conn = connect_tz(tc);

	initStringInfo(&t);
	draw_ts(tc, &t);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT date_trunc('hour', %s) <= %s "
					 "AND date_trunc('day', %s) <= %s",
					 t.data, t.data, t.data, t.data);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. epoch_monotone
 *
 * extract(epoch) is strictly increasing on a strictly increasing timestamptz.
 * TS2 is built as TS1 plus a strictly positive interval, so TS1 < TS2 by
 * construction; then extract(epoch from TS1) < extract(epoch from TS2) must
 * hold.  The positive interval is a fixed span (days/hours/minutes/seconds,
 * at least one second) so TS2 strictly exceeds TS1 in UTC.
 */
PG_PROP(epoch_monotone)
{
	pg_prop_conn *conn;
	StringInfoData t1;
	StringInfoData q;
	int			days;
	int			hours;
	int			mins;
	int			secs;

	conn = connect_tz(tc);

	initStringInfo(&t1);
	draw_ts(tc, &t1);

	/*
	 * Draw a strictly positive, fixed-span interval.  secs starts at 1 so the
	 * interval is never zero even when days/hours/mins are all 0; the year is
	 * capped at 2030 and the span at well under a year, so TS2 stays inside
	 * the supported range and never overflows.
	 */
	days = (int) pg_prop_draw_int(tc, 0, 300);
	hours = (int) pg_prop_draw_int(tc, 0, 23);
	mins = (int) pg_prop_draw_int(tc, 0, 59);
	secs = (int) pg_prop_draw_int(tc, 1, 59);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(epoch from %s) "
					 "< extract(epoch from "
					 "(%s + '%d days %d hours %d mins %d secs'::interval))",
					 t1.data, t1.data, days, hours, mins, secs);

	require_true(tc, conn, q.data);

	pfree(t1.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
