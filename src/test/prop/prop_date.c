/*-------------------------------------------------------------------------
 *
 * prop_date.c
 *	  Property-based tests for the date type (src/backend/utils/adt/date.c).
 *
 * Date arithmetic, parsing, and the +/- integer and date-minus-date
 * operations have had overflow and rounding fixes; these check round-trips
 * and arithmetic identities over generated dates.
 *
 * The oracle here is the type's own algebra and I/O: a date formatted to text
 * and parsed back must equal the original; adding then subtracting an integer
 * number of days is the identity; date - date yields the integer day count
 * that reconstructs the second date; make_date and extract are mutual
 * inverses; the comparator is a total order; and casting through timestamp
 * (midnight) round-trips.  Each invariant is computed as a boolean in SQL and
 * the scalar result is required to be "t".
 *
 * Dates for the identity properties are drawn in a safe range (year
 * 2000..2050, day <= 28) so neither invalid-date nor overflow errors arise
 * from the literal itself; only large day offsets can legally overflow the
 * date range, and those cases are discarded via is_overflow rather than
 * reported.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_date.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "date"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * True when a SQL error is a legitimate out-of-range/overflow condition rather
 * than a wrong-results bug.  Adding a large integer number of days to a date
 * can legally run off the ends of the supported date range; those cases are
 * discarded, not reported.
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
 * Draw a valid DATE literal into buf, e.g.  DATE '2024-03-15'.
 *
 * The year is kept in 2000..2050 (well inside the supported range so no
 * arithmetic driven by the literal itself overflows), the month in 1..12, and
 * the day capped at 28 so every month is valid and no spurious invalid-date
 * error can arise.
 */
static void
draw_date(pg_prop_tc *tc, StringInfo buf)
{
	int			year;
	int			mon;
	int			day;

	year = (int) pg_prop_draw_int(tc, 2000, 2050);
	mon = (int) pg_prop_draw_int(tc, 1, 12);
	day = (int) pg_prop_draw_int(tc, 1, 28);

	appendStringInfo(buf, "DATE '%04d-%02d-%02d'", year, mon, day);
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
 * 1. text_roundtrip
 *
 * Formatting a date to text and parsing it back must yield the same date.
 * This exercises the date output and input paths against each other.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_date(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::date = %s", d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. add_sub_days_inverse
 *
 * Adding an integer number of days and then subtracting the same count must
 * return the original date: (D + k) - k = D.  A large k can legally run off
 * the date range, so an out-of-range error is discarded rather than reported.
 */
PG_PROP(add_sub_days_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d;
	StringInfoData q;
	int64		k;
	char	   *result;
	bool		failed;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_date(tc, &d);
	k = pg_prop_draw_int(tc, -100000, 100000);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s + %lld) - %lld = %s",
					 d.data, (long long) k, (long long) k, d.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
	{
		if (is_overflow(err))
		{
			/* arithmetic ran off the supported range: legal, skip the case */
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pfree(d.data);
			pfree(q.data);
			pg_prop_disconnect(conn);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc,
					 "unexpected error on (D + k) - k = D\n"
					 "  D: %s\n  k: %lld\n  error: %s",
					 d.data, (long long) k, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "(D + k) - k != D\n"
					 "  D: %s\n  k: %lld\n  result: %s",
					 d.data, (long long) k, result ? result : "NULL");

	pg_free(result);
	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. diff_then_add
 *
 * date - date yields an int4 number of days; adding it back to the first date
 * must reconstruct the second: D1 + (D2 - D1) = D2.  Both dates are in the
 * safe range so no overflow is expected.
 */
PG_PROP(diff_then_add)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d1;
	StringInfoData d2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d1);
	initStringInfo(&d2);
	draw_date(tc, &d1);
	draw_date(tc, &d2);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT %s + (%s - %s) = %s",
					 d1.data, d2.data, d1.data, d2.data);

	require_true(tc, conn, q.data);

	pfree(d1.data);
	pfree(d2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. monotonic_add
 *
 * Adding a strictly positive number of days must yield a strictly greater
 * date: (D + k) > D for k > 0.  A large k can legally overflow the date
 * range, so an out-of-range error is discarded rather than reported.
 */
PG_PROP(monotonic_add)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d;
	StringInfoData q;
	int64		k;
	char	   *result;
	bool		failed;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_date(tc, &d);
	k = pg_prop_draw_int(tc, 1, 100000);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s + %lld) > %s",
					 d.data, (long long) k, d.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
	{
		if (is_overflow(err))
		{
			/* arithmetic ran off the supported range: legal, skip the case */
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pfree(d.data);
			pfree(q.data);
			pg_prop_disconnect(conn);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc,
					 "unexpected error on (D + k) > D\n"
					 "  D: %s\n  k: %lld\n  error: %s",
					 d.data, (long long) k, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "(D + k) is not strictly greater than D for k > 0\n"
					 "  D: %s\n  k: %lld\n  result: %s",
					 d.data, (long long) k, result ? result : "NULL");

	pg_free(result);
	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. extract_reconstruct
 *
 * Reconstructing a date from its extracted year/month/day fields via
 * make_date (the independent constructor) must yield the original date.  This
 * exercises extract and make_date as mutual inverses.
 */
PG_PROP(extract_reconstruct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_date(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT make_date("
					 "extract(year from %s)::int, "
					 "extract(month from %s)::int, "
					 "extract(day from %s)::int) = %s",
					 d.data, d.data, d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. comparison_total_order
 *
 * For any two dates D1, D2, exactly one of (D1 < D2), (D1 = D2), (D1 > D2)
 * must hold.  This checks that the date comparator is a proper total order.
 */
PG_PROP(comparison_total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d1;
	StringInfoData d2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d1);
	initStringInfo(&d2);
	draw_date(tc, &d1);
	draw_date(tc, &d2);

	/* exactly one of <, =, > must be true */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ( ((%s) < (%s))::int "
					 "+ ((%s) = (%s))::int "
					 "+ ((%s) > (%s))::int ) = 1",
					 d1.data, d2.data,
					 d1.data, d2.data,
					 d1.data, d2.data);

	require_true(tc, conn, q.data);

	pfree(d1.data);
	pfree(d2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. timestamp_cast_roundtrip
 *
 * Casting a date to timestamp (midnight) and back must yield the same date:
 * (D::timestamp)::date = D.  This exercises the date<->timestamp conversion
 * paths against each other.
 */
PG_PROP(timestamp_cast_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&d);
	draw_date(tc, &d);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s::timestamp)::date = %s", d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
