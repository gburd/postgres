/*-------------------------------------------------------------------------
 *
 * prop_datecast.c
 *	  Property-based tests for date/timestamp/timestamptz cast relationships.
 *
 * This file targets the cross-type cast algebra -- the conversions between
 * date, timestamp (without time zone) and timestamptz -- as distinct from
 * date arithmetic (prop_date), field extraction (prop_dateparts), time-of-day
 * operations (prop_timeops) and truncation (prop_datetrunc).  A date casts to
 * timestamp as midnight of that day; a timestamp casts to date by dropping the
 * time; and under a UTC session the timestamp<->timestamptz conversion via AT
 * TIME ZONE 'UTC' is the identity.  make_date / make_time / make_timestamp are
 * the independent constructors used to build the operands.
 *
 * The oracle is the type's own cast algebra, evaluated in SQL as a boolean
 * that must be "t": each identity holds for every drawn date/time, so an "f"
 * (or a NULL / SQL error on valid inputs) is a cast bug, not a timezone
 * artefact.  Every property pins the session TimeZone to 'UTC' (via
 * connect_tz) so the timestamp<->timestamptz conversions are deterministic
 * and free of DST.
 *
 * Component ranges: year 2001..2030, month 1..12, day 1..28 (every month
 * valid, no invalid-date error), hour 0..23, minute 0..59, second 0..59.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_datecast.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "datecast"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Drawn date/time components.  Ranges per the spec: a day cap of 28 keeps
 * every month valid so no invalid-date error can arise from the literal, and
 * all fields stay well inside the supported range so no cast overflows.
 */
typedef struct dt_parts
{
	int			y;
	int			mo;
	int			d;
	int			h;
	int			mi;
	int			s;
}			dt_parts;

static void
draw_parts(pg_prop_tc *tc, dt_parts *p)
{
	p->y = (int) pg_prop_draw_int(tc, 2001, 2030);
	p->mo = (int) pg_prop_draw_int(tc, 1, 12);
	p->d = (int) pg_prop_draw_int(tc, 1, 28);
	p->h = (int) pg_prop_draw_int(tc, 0, 23);
	p->mi = (int) pg_prop_draw_int(tc, 0, 59);
	p->s = (int) pg_prop_draw_int(tc, 0, 59);
}

/*
 * Connect to the test cluster and pin the session TimeZone to 'UTC' so the
 * timestamp<->timestamptz conversions (AT TIME ZONE 'UTC', ::timestamptz) are
 * deterministic and free of DST regardless of the cluster default.  Fails the
 * case if either step fails.
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
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a cast errored or produced NULL on valid inputs, which is
 * itself a bug -- so either is a failure, reported with the SQL.
 */
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

/*
 * 1. date_to_timestamp
 *
 * Casting a date to timestamp yields midnight of that day: D::timestamp equals
 * the timestamp built from the same y-mo-d string with a 00:00:00 time.  Both
 * sides are formed independently (make_date vs. a text literal) so a wrong
 * time-of-day in the cast is caught.
 */
PG_PROP(date_to_timestamp)
{
	pg_prop_conn *conn = connect_tz(tc);
	dt_parts	p;
	StringInfoData q;

	draw_parts(tc, &p);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (make_date(%d,%d,%d))::timestamp "
					 "= ('%04d-%02d-%02d 00:00:00')::timestamp",
					 p.y, p.mo, p.d,
					 p.y, p.mo, p.d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. timestamp_to_date
 *
 * Casting a timestamp to date drops the time and returns the calendar day:
 * (make_timestamp(y,mo,d,h,mi,s))::date = make_date(y,mo,d) for the drawn
 * components.
 */
PG_PROP(timestamp_to_date)
{
	pg_prop_conn *conn = connect_tz(tc);
	dt_parts	p;
	StringInfoData q;

	draw_parts(tc, &p);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (make_timestamp(%d,%d,%d,%d,%d,%d))::date "
					 "= make_date(%d,%d,%d)",
					 p.y, p.mo, p.d, p.h, p.mi, p.s,
					 p.y, p.mo, p.d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. date_timestamp_roundtrip
 *
 * A date cast to timestamp (midnight) and back to date is the identity:
 * ((D)::timestamp)::date = D.  This exercises both cast directions against
 * each other.
 */
PG_PROP(date_timestamp_roundtrip)
{
	pg_prop_conn *conn = connect_tz(tc);
	dt_parts	p;
	StringInfoData q;

	draw_parts(tc, &p);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((make_date(%d,%d,%d))::timestamp)::date "
					 "= make_date(%d,%d,%d)",
					 p.y, p.mo, p.d,
					 p.y, p.mo, p.d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. timestamp_to_timestamptz
 *
 * Under a UTC session, promoting a timestamp to timestamptz and then reading
 * it back AT TIME ZONE 'UTC' is the identity:
 *		((TS)::timestamptz) AT TIME ZONE 'UTC' = TS.
 * The result of AT TIME ZONE 'UTC' on a timestamptz is a plain timestamp, so
 * the comparison is timestamp-vs-timestamp.
 */
PG_PROP(timestamp_to_timestamptz)
{
	pg_prop_conn *conn = connect_tz(tc);
	dt_parts	p;
	StringInfoData q;

	draw_parts(tc, &p);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((make_timestamp(%d,%d,%d,%d,%d,%d))::timestamptz) "
					 "AT TIME ZONE 'UTC' = make_timestamp(%d,%d,%d,%d,%d,%d)",
					 p.y, p.mo, p.d, p.h, p.mi, p.s,
					 p.y, p.mo, p.d, p.h, p.mi, p.s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. timestamptz_to_timestamp
 *
 * The reverse direction, stated as a round-trip through timestamptz under UTC:
 * taking a timestamp to timestamptz and back AT TIME ZONE 'UTC' recovers the
 * original timestamp.  (Equivalent to property 4 by UTC's symmetry; kept
 * separate to assert the reverse conversion path explicitly as the spec
 * requires.)
 */
PG_PROP(timestamptz_to_timestamp)
{
	pg_prop_conn *conn = connect_tz(tc);
	dt_parts	p;
	StringInfoData q;

	draw_parts(tc, &p);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((make_timestamp(%d,%d,%d,%d,%d,%d))::timestamptz) "
					 "AT TIME ZONE 'UTC') = make_timestamp(%d,%d,%d,%d,%d,%d)",
					 p.y, p.mo, p.d, p.h, p.mi, p.s,
					 p.y, p.mo, p.d, p.h, p.mi, p.s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. date_plus_time_eq_timestamp
 *
 * A date plus a time-of-day equals the timestamp with the same components:
 * make_date(y,mo,d) + make_time(h,mi,s) = make_timestamp(y,mo,d,h,mi,s).  This
 * ties the date+time composition operator to the direct timestamp constructor.
 */
PG_PROP(date_plus_time_eq_timestamp)
{
	pg_prop_conn *conn = connect_tz(tc);
	dt_parts	p;
	StringInfoData q;

	draw_parts(tc, &p);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT make_date(%d,%d,%d) + make_time(%d,%d,%d) "
					 "= make_timestamp(%d,%d,%d,%d,%d,%d)",
					 p.y, p.mo, p.d, p.h, p.mi, p.s,
					 p.y, p.mo, p.d, p.h, p.mi, p.s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. extract_consistency
 *
 * The year read from a date and from that date cast to timestamp must agree,
 * and both must equal the drawn year:
 *		extract(year from D::timestamp) = extract(year from D) = y.
 * The cast to timestamp must not perturb the calendar year.
 */
PG_PROP(extract_consistency)
{
	pg_prop_conn *conn = connect_tz(tc);
	dt_parts	p;
	StringInfoData q;

	draw_parts(tc, &p);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(year from (make_date(%d,%d,%d))::timestamp) "
					 "= extract(year from make_date(%d,%d,%d)) "
					 "AND extract(year from make_date(%d,%d,%d)) = %d",
					 p.y, p.mo, p.d,
					 p.y, p.mo, p.d,
					 p.y, p.mo, p.d, p.y);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 8. date_order_preserved
 *
 * The date->timestamp cast is strictly monotonic: for D1 < D2 built as
 * D2 = D1 + k days with k > 0, (D1)::timestamp < (D2)::timestamp.  k is kept
 * small (1..60) so D2 stays within the drawn-day neighbourhood and no cast
 * overflow arises; the day offset makes D1 < D2 by construction.
 */
PG_PROP(date_order_preserved)
{
	pg_prop_conn *conn = connect_tz(tc);
	dt_parts	p;
	int			k;
	StringInfoData q;

	draw_parts(tc, &p);
	k = (int) pg_prop_draw_int(tc, 1, 60);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (make_date(%d,%d,%d))::timestamp "
					 "< ((make_date(%d,%d,%d) + %d))::timestamp",
					 p.y, p.mo, p.d,
					 p.y, p.mo, p.d, k);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
