/*-------------------------------------------------------------------------
 *
 * prop_dateparts.c
 *	  Property-based tests for date arithmetic and field extraction.
 *
 * Exercises date operations distinct from the "date" area: date +/- integer
 * days, date - date yielding integer days, make_date(), extract() of
 * year/month/day/dow/doy, the text round-trip, and the total order of date
 * comparison.  The oracle is each operation's defining law -- adding then
 * subtracting the same offset is the identity, D1 + (D2 - D1) = D2, extracting
 * a field returns the component that built the date, day-of-week and
 * day-of-year lie in their fixed ranges, text output re-parses, and exactly
 * one of <,=,> holds for any pair -- so any "f" result is a bug in date.c,
 * not a tolerance.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_dateparts.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "dateparts"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a (year, month, day) triple into y, mo and d, with y 2000..2030,
 * mo 1..12 and d 1..28 (so every month is valid without leap/length cases).
 */
static void
draw_ymd(pg_prop_tc *tc, int *y, int *mo, int *d)
{
	*y = (int) pg_prop_draw_int(tc, 2000, 2030);
	*mo = (int) pg_prop_draw_int(tc, 1, 12);
	*d = (int) pg_prop_draw_int(tc, 1, 28);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operation errored or produced NULL on valid inputs,
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
 * Add/subtract of an integer-day offset is the identity:
 * (make_date(y,mo,d) + k) - k = make_date(y,mo,d), for k in -500..500.
 */
PG_PROP(date_plus_minus_int)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y,
				mo,
				d,
				k;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);
	k = (int) pg_prop_draw_int(tc, -500, 500);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (make_date(%d, %d, %d) + (%d)) - (%d) "
					 "= make_date(%d, %d, %d)",
					 y, mo, d, k, k, y, mo, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Date difference round-trip: D2 - D1 is an integer number of days and
 * D1 + (D2 - D1) = D2, for two independently drawn dates.
 */
PG_PROP(date_diff_days)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y1,
				mo1,
				d1,
				y2,
				mo2,
				d2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y1, &mo1, &d1);
	draw_ymd(tc, &y2, &mo2, &d2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT make_date(%d, %d, %d) "
					 "+ (make_date(%d, %d, %d) - make_date(%d, %d, %d)) "
					 "= make_date(%d, %d, %d)",
					 y1, mo1, d1,
					 y2, mo2, d2, y1, mo1, d1,
					 y2, mo2, d2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Field extraction returns the components that built the date:
 * extract(year from D) = y, extract(month from D) = mo, extract(day from D)
 * = d.
 */
PG_PROP(extract_year_month_day)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y,
				mo,
				d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(year from make_date(%d, %d, %d)) = %d "
					 "AND extract(month from make_date(%d, %d, %d)) = %d "
					 "AND extract(day from make_date(%d, %d, %d)) = %d",
					 y, mo, d, y,
					 y, mo, d, mo,
					 y, mo, d, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Range bounds: extract(dow from D) is in 0..6 and extract(doy from D) is in
 * 1..366 for any valid date.
 */
PG_PROP(dow_range)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y,
				mo,
				d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(dow from make_date(%d, %d, %d)) "
					 "BETWEEN 0 AND 6 "
					 "AND extract(doy from make_date(%d, %d, %d)) "
					 "BETWEEN 1 AND 366",
					 y, mo, d, y, mo, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Text round-trip: (make_date(y,mo,d))::text::date = make_date(y,mo,d).  The
 * output text must re-parse to the same date.
 */
PG_PROP(make_date_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y,
				mo,
				d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (make_date(%d, %d, %d))::text::date "
					 "= make_date(%d, %d, %d)",
					 y, mo, d, y, mo, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Date comparison is a total order: for two drawn dates A and B exactly one of
 * A < B, A = B, A > B holds.  The three mutually exclusive outcomes must sum
 * to exactly one true.
 */
PG_PROP(date_le_total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y1,
				mo1,
				d1,
				y2,
				mo2,
				d2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y1, &mo1, &d1);
	draw_ymd(tc, &y2, &mo2, &d2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (CASE WHEN make_date(%d, %d, %d) "
					 "< make_date(%d, %d, %d) THEN 1 ELSE 0 END "
					 "+ CASE WHEN make_date(%d, %d, %d) "
					 "= make_date(%d, %d, %d) THEN 1 ELSE 0 END "
					 "+ CASE WHEN make_date(%d, %d, %d) "
					 "> make_date(%d, %d, %d) THEN 1 ELSE 0 END) = 1",
					 y1, mo1, d1, y2, mo2, d2,
					 y1, mo1, d1, y2, mo2, d2,
					 y1, mo1, d1, y2, mo2, d2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
