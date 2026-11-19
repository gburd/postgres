/*-------------------------------------------------------------------------
 *
 * prop_datetrunc.c
 *	  Property-based tests for date_trunc()/date_part() on plain TIMESTAMP
 *	  (src/backend/utils/adt/timestamp.c).
 *
 * Distinct from prop_date/prop_dateparts/prop_timeops/prop_tstzarith: this
 * file targets the truncation and field-extraction algebra of date_trunc()
 * and date_part() on TIMESTAMP WITHOUT TIME ZONE across the hour/day/month/
 * year units.  The oracle is the operations' own definition: truncation can
 * only move a timestamp earlier-or-equal, is idempotent, zeroes every field
 * below the unit, and coarser units land earlier-or-equal than finer ones;
 * date_part agrees with extract on the drawn field.  Each is TRUE for every
 * input, so an "f" result is a bug in the truncation/extraction code.
 *
 * Every property pins the session TimeZone to 'UTC' (via connect_tz) and uses
 * plain TIMESTAMP literals (no time zone, hence no DST), so results are
 * deterministic and free of timezone artefacts.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_datetrunc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "datetrunc"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Connect to the test cluster and pin the session TimeZone to 'UTC'.  Plain
 * TIMESTAMP has no time zone, but date_trunc on larger units and extract are
 * evaluated in session context; pinning UTC keeps every case deterministic
 * and free of DST regardless of the cluster default.  Fails the case on error.
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
 * Build a random plain TIMESTAMP (no time zone) literal into buf, e.g.
 *		TIMESTAMP '2024-03-15 08:30:00'
 *
 * Year 2001..2030 keeps the literal well inside the supported range; day is
 * capped at 28 so every month is valid.
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

	appendStringInfo(buf, "TIMESTAMP '%04d-%02d-%02d %02d:%02d:%02d'",
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
 * 1. trunc_le_original: truncating to any unit can only move a timestamp
 * earlier-or-equal to itself, for every unit {hour, day, month, year}.
 */
PG_PROP(trunc_le_original)
{
	pg_prop_conn *conn = connect_tz(tc);
	StringInfoData ts,
				q;

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT date_trunc('hour', %s) <= %s "
					 "AND date_trunc('day', %s) <= %s "
					 "AND date_trunc('month', %s) <= %s "
					 "AND date_trunc('year', %s) <= %s",
					 ts.data, ts.data,
					 ts.data, ts.data,
					 ts.data, ts.data,
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. trunc_idempotent: truncating an already-truncated value changes nothing,
 * for the hour/day/month units.
 */
PG_PROP(trunc_idempotent)
{
	pg_prop_conn *conn = connect_tz(tc);
	StringInfoData ts,
				q;

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT date_trunc('hour', date_trunc('hour', %s)) "
					 "= date_trunc('hour', %s) "
					 "AND date_trunc('day', date_trunc('day', %s)) "
					 "= date_trunc('day', %s) "
					 "AND date_trunc('month', date_trunc('month', %s)) "
					 "= date_trunc('month', %s)",
					 ts.data, ts.data,
					 ts.data, ts.data,
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. trunc_hour_zeroes_subhour: truncating to the hour zeroes the minute and
 * second fields.
 */
PG_PROP(trunc_hour_zeroes_subhour)
{
	pg_prop_conn *conn = connect_tz(tc);
	StringInfoData ts,
				q;

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(minute from date_trunc('hour', %s)) = 0 "
					 "AND extract(second from date_trunc('hour', %s)) = 0",
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. trunc_day_zeroes_time: truncating to the day leaves midnight, i.e. the
 * time-of-day component is 00:00:00.
 */
PG_PROP(trunc_day_zeroes_time)
{
	pg_prop_conn *conn = connect_tz(tc);
	StringInfoData ts,
				q;

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT date_trunc('day', %s)::time = '00:00:00'::time",
					 ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. trunc_month_first_day: truncating to the month lands on day 1.
 */
PG_PROP(trunc_month_first_day)
{
	pg_prop_conn *conn = connect_tz(tc);
	StringInfoData ts,
				q;

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(day from date_trunc('month', %s)) = 1",
					 ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. trunc_year_jan: truncating to the year lands on January 1st.
 */
PG_PROP(trunc_year_jan)
{
	pg_prop_conn *conn = connect_tz(tc);
	StringInfoData ts,
				q;

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(month from date_trunc('year', %s)) = 1 "
					 "AND extract(day from date_trunc('year', %s)) = 1",
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. trunc_coarser_le_finer: coarser truncation is earlier-or-equal than
 * finer, in a single chain year <= month <= day <= hour <= original.
 */
PG_PROP(trunc_coarser_le_finer)
{
	pg_prop_conn *conn = connect_tz(tc);
	StringInfoData ts,
				q;

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT date_trunc('year', %s) <= date_trunc('month', %s) "
					 "AND date_trunc('month', %s) <= date_trunc('day', %s) "
					 "AND date_trunc('day', %s) <= date_trunc('hour', %s) "
					 "AND date_trunc('hour', %s) <= %s",
					 ts.data, ts.data,
					 ts.data, ts.data,
					 ts.data, ts.data,
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 8. date_part_eq_extract: date_part() and extract() are the same operation
 * spelled two ways; they must agree on the drawn hour.
 */
PG_PROP(date_part_eq_extract)
{
	pg_prop_conn *conn = connect_tz(tc);
	StringInfoData ts,
				q;

	initStringInfo(&ts);
	draw_ts(tc, &ts);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT date_part('hour', %s) = extract(hour from %s)",
					 ts.data, ts.data);

	require_true(tc, conn, q.data);

	pfree(ts.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
