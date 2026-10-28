/*-------------------------------------------------------------------------
 *
 * prop_timestamptz.c
 *	  Property-based tests for timestamp/timestamptz
 *	  (src/backend/utils/adt/timestamp.c), focusing on timestamptz,
 *	  date_trunc, date_bin, and extract.
 *
 * timestamp[tz] has had ~24 fixes (date_bin rounding d163fdbfea8, date_trunc
 * on infinities, extract fields, overflow in +/- interval); these check
 * round-trips, date_trunc/date_bin idempotence and bounds, and extract
 * consistency.  To keep timezone effects deterministic, every property sets
 * the session TimeZone to 'UTC' first (see pg_prop_connect_utc), and every
 * literal carries an explicit +00 offset so it is unambiguous UTC.
 *
 * The oracle here is the type's own algebra evaluated in SQL: text round-trip,
 * date_trunc idempotence and its "never moves forward" bound, date_bin's
 * bucket containment, extract(epoch) monotonicity, the add/subtract inverse
 * for day/time-only intervals, and the comparator being a proper total order.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_timestamptz.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "timestamptz"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * True when a SQL error is a legitimate out-of-range/overflow condition rather
 * than a wrong-results bug.  Interval arithmetic and date_bin near the ends of
 * the supported range can legally overflow; those cases are discarded, not
 * reported.
 */
static bool
is_overflow(const char *errmsg)
{
	if (errmsg == NULL)
		return false;
	return strstr(errmsg, "out of range") != NULL ||
		strstr(errmsg, "overflow") != NULL;
}

/*
 * Connect to the test cluster and pin the session TimeZone to 'UTC' so that
 * date_trunc, text I/O, and extract are deterministic regardless of the
 * cluster's default.  Fails the case if either step fails.
 */
static pg_prop_conn *
pg_prop_connect_utc(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn, "SET TimeZone = 'UTC'", &err))
		pg_prop_fail(tc, "could not set TimeZone to UTC: %s", err ? err : "");

	return conn;
}

/*
 * Build a random timestamptz literal into buf, e.g.
 *		TIMESTAMPTZ '2024-03-15 08:30:00+00'
 *
 * The range is kept well inside the supported timestamp range (year
 * 2000..2050) so the literal itself never overflows and the identities under
 * test are only driven by the operation.  day is capped at 28 so every month
 * is valid.  The explicit +00 offset makes the literal unambiguous UTC.
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

	year = (int) pg_prop_draw_int(tc, 2000, 2050);
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
 * 1. text_roundtrip
 *
 * Formatting a timestamptz to text and parsing it back must yield the same
 * value: (T)::text::timestamptz = T.  This targets the text I/O round-trip.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	StringInfoData t;
	StringInfoData q;

	conn = pg_prop_connect_utc(tc);

	initStringInfo(&t);
	draw_ts(tc, &t);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::timestamptz = %s",
					 t.data, t.data);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* Valid date_trunc fields, sampled by index. */
static const char *const trunc_fields[] = {
	"microseconds", "second", "minute", "hour", "day", "month", "year"
};

/*
 * 2. date_trunc_idempotent
 *
 * date_trunc is idempotent: truncating to a unit and truncating again to the
 * same unit yields the same value.  date_trunc('<f>', T) =
 * date_trunc('<f>', date_trunc('<f>', T)).
 */
PG_PROP(date_trunc_idempotent)
{
	pg_prop_conn *conn;
	StringInfoData t;
	StringInfoData q;
	const char *f;

	conn = pg_prop_connect_utc(tc);

	f = trunc_fields[pg_prop_draw_int(tc, 0, lengthof(trunc_fields) - 1)];

	initStringInfo(&t);
	draw_ts(tc, &t);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT date_trunc('%s', %s) "
					 "= date_trunc('%s', date_trunc('%s', %s))",
					 f, t.data, f, f, t.data);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. date_trunc_not_after
 *
 * Truncation rounds down toward the start of the unit, so it never moves the
 * timestamp forward: date_trunc('<f>', T) <= T.
 */
PG_PROP(date_trunc_not_after)
{
	pg_prop_conn *conn;
	StringInfoData t;
	StringInfoData q;
	const char *f;

	conn = pg_prop_connect_utc(tc);

	f = trunc_fields[pg_prop_draw_int(tc, 0, lengthof(trunc_fields) - 1)];

	initStringInfo(&t);
	draw_ts(tc, &t);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT date_trunc('%s', %s) <= %s",
					 f, t.data, t.data);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. date_bin_in_bucket
 *
 * date_bin places T into a bucket whose start b satisfies b <= T < b + stride,
 * for a positive stride.  This directly targets the date_bin rounding fixes
 * (d163fdbfea8).  Overflow near the range ends is a legitimate error and the
 * case is discarded.
 *
 * date_bin signature: date_bin(stride interval, source timestamptz,
 * origin timestamptz).
 */
PG_PROP(date_bin_in_bucket)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t;
	StringInfoData o;
	StringInfoData q;
	char	   *result;
	bool		failed;
	int			stride;

	conn = pg_prop_connect_utc(tc);

	stride = (int) pg_prop_draw_int(tc, 1, 86400);

	initStringInfo(&t);
	draw_ts(tc, &t);

	initStringInfo(&o);
	draw_ts(tc, &o);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (b <= %s) AND (%s < b + ('%d seconds')::interval) "
					 "FROM (SELECT date_bin(('%d seconds')::interval, %s, %s) "
					 "AS b) s",
					 t.data, t.data, stride,
					 stride, t.data, o.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
	{
		if (is_overflow(err))
		{
			/* arithmetic ran off the supported range: legal, skip the case */
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pfree(t.data);
			pfree(o.data);
			pfree(q.data);
			pg_prop_disconnect(conn);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc,
					 "unexpected error on date_bin containment\n"
					 "  T: %s\n  O: %s\n  stride: %d s\n  error: %s",
					 t.data, o.data, stride, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "date_bin bucket does not contain T\n"
					 "  T: %s\n  O: %s\n  stride: %d s\n  result: %s",
					 t.data, o.data, stride, result ? result : "NULL");

	pg_free(result);
	pfree(t.data);
	pfree(o.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. extract_epoch_monotonic
 *
 * The ordering of two timestamps matches the ordering of their epoch seconds:
 * (T1 < T2) = (extract(epoch from T1) < extract(epoch from T2)).  This checks
 * that extract(epoch ...) is consistent with timestamptz comparison.
 */
PG_PROP(extract_epoch_monotonic)
{
	pg_prop_conn *conn;
	StringInfoData t1;
	StringInfoData t2;
	StringInfoData q;

	conn = pg_prop_connect_utc(tc);

	initStringInfo(&t1);
	initStringInfo(&t2);
	draw_ts(tc, &t1);
	draw_ts(tc, &t2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s < %s) "
					 "= (extract(epoch from %s) < extract(epoch from %s))",
					 t1.data, t2.data, t1.data, t2.data);

	require_true(tc, conn, q.data);

	pfree(t1.data);
	pfree(t2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. plus_minus_interval_inverse
 *
 * For a day/time-only interval I (no months/years, so it is exactly
 * invertible) and any timestamptz T, (T + I) - I must equal T.  Months and
 * years are deliberately excluded because they denote variable spans and are
 * not invertible this way.  Overflow near the range ends is a legitimate error
 * and the case is discarded.
 */
PG_PROP(plus_minus_interval_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t;
	StringInfoData q;
	char	   *result;
	bool		failed;
	int			days;
	int			hours;
	int			mins;
	int			secs;

	conn = pg_prop_connect_utc(tc);

	initStringInfo(&t);
	draw_ts(tc, &t);

	/* day/time-only interval -- no months/years, so exactly invertible */
	days = (int) pg_prop_draw_int(tc, 0, 1000);
	hours = (int) pg_prop_draw_int(tc, 0, 23);
	mins = (int) pg_prop_draw_int(tc, 0, 59);
	secs = (int) pg_prop_draw_int(tc, 0, 59);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s + '%d days %d hours %d mins %d secs'::interval "
					 "- '%d days %d hours %d mins %d secs'::interval) = %s",
					 t.data, days, hours, mins, secs,
					 days, hours, mins, secs, t.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
	{
		if (is_overflow(err))
		{
			/* arithmetic ran off the supported range: legal, skip the case */
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pfree(t.data);
			pfree(q.data);
			pg_prop_disconnect(conn);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc,
					 "unexpected error on (T + I - I) = T\n"
					 "  T: %s\n  I: %d days %d hours %d mins %d secs\n  error: %s",
					 t.data, days, hours, mins, secs, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "(T + I - I) != T for day/time interval\n"
					 "  T: %s\n  I: %d days %d hours %d mins %d secs\n  result: %s",
					 t.data, days, hours, mins, secs,
					 result ? result : "NULL");

	pg_free(result);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. comparison_total_order
 *
 * For any two timestamps T1, T2, exactly one of (T1 < T2), (T1 = T2),
 * (T1 > T2) must hold.  This checks that the timestamptz comparator is a
 * proper total order.
 */
PG_PROP(comparison_total_order)
{
	pg_prop_conn *conn;
	StringInfoData t1;
	StringInfoData t2;
	StringInfoData q;

	conn = pg_prop_connect_utc(tc);

	initStringInfo(&t1);
	initStringInfo(&t2);
	draw_ts(tc, &t1);
	draw_ts(tc, &t2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s < %s)::int + (%s = %s)::int "
					 "+ (%s > %s)::int) = 1",
					 t1.data, t2.data, t1.data, t2.data, t1.data, t2.data);

	require_true(tc, conn, q.data);

	pfree(t1.data);
	pfree(t2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
