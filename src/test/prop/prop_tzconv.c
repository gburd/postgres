/*-------------------------------------------------------------------------
 *
 * prop_tzconv.c
 *	  Property-based tests for timezone conversion identities.
 *
 * AT TIME ZONE, make_timestamptz, and the timestamptz<->timestamp conversion
 * paths (src/backend/utils/adt/timestamp.c, datetime.c) implement the
 * instant<->wall-clock mapping.  These properties check the identities that
 * must hold for fixed, DST-free zones: AT TIME ZONE is its own inverse at a
 * fixed offset, converting to two fixed offsets differs by exactly the offset
 * delta, the epoch of an instant is independent of display zone, text I/O
 * round-trips under a UTC session, make_timestamptz recovers its fields, and
 * conversion to a fixed zone preserves instant order.  A false result is a
 * bug in the conversion, not a DST artefact (every zone used here is a fixed
 * offset or an Etc/GMT zone with no DST).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_tzconv.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "tzconv"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Connect to the test cluster and pin the session TimeZone to 'UTC' so that
 * text I/O, extract, and any implicit display conversion are deterministic.
 * The properties that compare against a named zone always name it explicitly
 * via AT TIME ZONE, so only the session default matters here.
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
		pg_prop_fail(tc, "could not set TIME ZONE to UTC: %s", err ? err : "");

	return conn;
}

/*
 * Build a random timestamptz literal into buf, e.g.
 *		TIMESTAMPTZ '2024-03-15 08:30:00+00'
 *
 * Shape A domain: year 2001..2030, month 1..12, day 1..28 (valid in every
 * month), hour 0..23, minute 0..59, second 0..59.  The explicit +00 offset
 * makes the literal an unambiguous instant.  The range is well inside the
 * supported timestamp range so the literal and the small shifts below never
 * overflow.
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
 * a SQL error means the conversion errored or produced NULL on finite inputs,
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
 * 1. attz_inverse_fixed_offset
 *
 * AT TIME ZONE is its own inverse at a fixed offset.  (TS AT TIME ZONE '+05')
 * converts the instant TS to the local wall clock in zone '+05', yielding a
 * plain timestamp; applying AT TIME ZONE '+05' to that timestamp reads it back
 * as an instant in '+05', recovering TS.  Zone '+05' is a fixed offset with no
 * DST, so this round-trip is exact for every instant:
 *		((TS AT TIME ZONE '+05') AT TIME ZONE '+05') = TS.
 */
PG_PROP(attz_inverse_fixed_offset)
{
	pg_prop_conn *conn;
	StringInfoData ts;
	StringInfoData q;

	conn = connect_tz(tc);

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s AT TIME ZONE '+05') AT TIME ZONE '+05') = %s",
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. offset_shifts_by_hours
 *
 * AT TIME ZONE converts an instant to the local wall clock in that zone,
 * producing a plain timestamp.  The wall clocks for two fixed offsets differ
 * by exactly the offset delta, so (TS AT TIME ZONE '+05') and
 * (TS AT TIME ZONE 'UTC') differ by 5 hours.  POSIX offset sign for a bare
 * '+05' is counter-intuitive (it can mean west of UTC), so we assert only the
 * magnitude, which is sound regardless of sign:
 *		abs(extract(epoch from ((TS AT '+05') - (TS AT 'UTC')))) = 5*3600.
 */
PG_PROP(offset_shifts_by_hours)
{
	pg_prop_conn *conn;
	StringInfoData ts;
	StringInfoData q;

	conn = connect_tz(tc);

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(extract(epoch from "
					 "((%s AT TIME ZONE '+05') - (%s AT TIME ZONE 'UTC')))) "
					 "= 5*3600",
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. epoch_invariant_under_zone
 *
 * The epoch of an instant does not depend on the display zone.  Converting an
 * instant TS to the wall clock in 'UTC' (a plain timestamp) and reinterpreting
 * that timestamp as an instant in the UTC session recovers the same instant,
 * so its epoch is unchanged:
 *		extract(epoch from (TS AT TIME ZONE 'UTC')::timestamptz)
 *			= extract(epoch from TS)
 * holds because the session TimeZone is pinned to UTC.
 */
PG_PROP(epoch_invariant_under_zone)
{
	pg_prop_conn *conn;
	StringInfoData ts;
	StringInfoData q;

	conn = connect_tz(tc);

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(epoch from "
					 "(%s AT TIME ZONE 'UTC')::timestamptz) "
					 "= extract(epoch from %s)",
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. roundtrip_text_utc
 *
 * Under a UTC session, formatting a timestamptz to text and parsing it back
 * yields the same instant: (TS)::text::timestamptz = TS.  This targets the
 * text I/O round-trip with a pinned display zone.
 */
PG_PROP(roundtrip_text_utc)
{
	pg_prop_conn *conn;
	StringInfoData ts;
	StringInfoData q;

	conn = connect_tz(tc);

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::timestamptz = %s",
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. make_timestamptz_roundtrip
 *
 * make_timestamptz(y, mo, d, h, mi, s, 'UTC') builds a valid instant from its
 * fields in UTC; reading the year field back in UTC must recover y:
 *		extract(year from make_timestamptz(...) AT TIME ZONE 'UTC') = y.
 * The field domain (year 2001..2030, day 1..28, etc.) is always a valid
 * calendar instant, so no error is expected.
 */
PG_PROP(make_timestamptz_roundtrip)
{
	pg_prop_conn *conn;
	StringInfoData q;
	int			year;
	int			mon;
	int			day;
	int			hour;
	int			min;
	int			sec;

	conn = connect_tz(tc);

	year = (int) pg_prop_draw_int(tc, 2001, 2030);
	mon = (int) pg_prop_draw_int(tc, 1, 12);
	day = (int) pg_prop_draw_int(tc, 1, 28);
	hour = (int) pg_prop_draw_int(tc, 0, 23);
	min = (int) pg_prop_draw_int(tc, 0, 59);
	sec = (int) pg_prop_draw_int(tc, 0, 59);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(year from "
					 "make_timestamptz(%d, %d, %d, %d, %d, %d, 'UTC') "
					 "AT TIME ZONE 'UTC') = %d",
					 year, mon, day, hour, min, sec, year);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. order_preserved_under_conversion
 *
 * Conversion to a fixed-offset zone is monotone: for TS1 < TS2,
 * (TS1 AT TIME ZONE '+05') < (TS2 AT TIME ZONE '+05').  TS2 is built as TS1
 * plus a strictly positive day/time interval (so TS1 < TS2 by construction,
 * and the span is exactly invertible with no month/year ambiguity).  Zone
 * '+05' is a fixed offset, so applying the same offset to both instants shifts
 * both wall clocks equally and preserves order.
 */
PG_PROP(order_preserved_under_conversion)
{
	pg_prop_conn *conn;
	StringInfoData ts1;
	StringInfoData ts2;
	StringInfoData q;
	int			days;
	int			hours;
	int			mins;
	int			secs;

	conn = connect_tz(tc);

	initStringInfo(&ts1);
	draw_ts(tc, &ts1);

	/* strictly positive day/time interval: at least one second */
	days = (int) pg_prop_draw_int(tc, 0, 1000);
	hours = (int) pg_prop_draw_int(tc, 0, 23);
	mins = (int) pg_prop_draw_int(tc, 0, 59);
	secs = (int) pg_prop_draw_int(tc, 1, 59);

	/* TS2 = TS1 + positive interval, well inside the supported range */
	initStringInfo(&ts2);
	appendStringInfo(&ts2,
					 "(%s + '%d days %d hours %d mins %d secs'::interval)",
					 ts1.data, days, hours, mins, secs);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s AT TIME ZONE '+05') < (%s AT TIME ZONE '+05')",
					 ts1.data, ts2.data);

	require_true(tc, conn, q.data);

	pfree(ts1.data);
	pfree(ts2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
