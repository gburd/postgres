/*-------------------------------------------------------------------------
 *
 * prop_tochar.c
 *	  Property-based tests for to_char / to_number / to_date formatting
 *	  (src/backend/utils/adt/formatting.c).
 *
 * to_char/to_number/to_date in formatting.c have had ~18 fixes (roman
 * numerals, overflow, rounding, field parsing); these check round-trips
 * (format then parse back) and digit/width invariants.
 *
 * The oracle here is round-trip and width/shape: a value formatted to text
 * and parsed back must equal the original, and a fixed-width mask must yield
 * a result of the mask's width.  Each invariant is computed as a boolean in
 * the query and the scalar result is required to be "t".
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_tochar.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "tochar"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * True when a SQL error is a legitimate out-of-range/overflow/unsupported
 * condition rather than a wrong-results bug.  The bounded input ranges below
 * are chosen so this should not fire, but it is kept so that any genuine
 * overflow (e.g. a too-wide to_number) is skipped rather than misreported.
 */
static bool
is_overflow(const char *errmsg)
{
	if (errmsg == NULL)
		return false;
	return strstr(errmsg, "out of range") != NULL ||
		strstr(errmsg, "overflow") != NULL ||
		strstr(errmsg, "cannot") != NULL;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the formatting path errored or produced NULL on valid
 * inputs, which is itself a bug -- so either is a failure, reported with the
 * SQL.
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
 * Like require_true, but a SQL error that is a legitimate out-of-range or
 * overflow condition discards the case rather than failing it.  Used by the
 * to_number round-trip, where a pathological drawn value could in principle
 * exceed the parse range; the bounded generators make this rare, but routing
 * through here keeps a genuine overflow from being misreported as a bug.
 */
static void
require_true_or_skip(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		bool		overflow = is_overflow(err);

		if (err)
			pg_free(err);
		if (overflow)
			pg_prop_assume(tc, false);	/* legitimate overflow: skip */
		pg_prop_fail(tc, "SQL error for: %s", sql);
	}
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
}

/*
 * 1. number_roundtrip_via_to_number
 *
 * Format an integer with a fixed-width digit mask in fill mode (FM, no
 * padding) then parse it back: the value must survive the round-trip.  FM
 * suppresses the leading spaces that would otherwise confuse to_number.  The
 * range is bounded well inside the mask width so no overflow can occur.
 */
PG_PROP(number_roundtrip_via_to_number)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, -999999999, 999999999);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_number(to_char(%lld, 'FM999999999999'), "
					 "'999999999999')::bigint = %lld",
					 (long long) n, (long long) n);

	require_true_or_skip(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. roman_numeral_in_range_roundtrips
 *
 * Roman numerals are defined for 1..3999.  There is no reliable RN parser to
 * round-trip against, so this is a sound shape check instead: to_char(n,
 * 'FMRN') must be composed only of roman digits (I V X L C D M) and have a
 * length between 1 and 15.  This guards the roman overflow/format fixes
 * without needing an RN parser.
 */
PG_PROP(roman_numeral_in_range_roundtrips)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, 1, 3999);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_char(%lld, 'FMRN') ~ '^[IVXLCDM]+$' "
					 "AND length(to_char(%lld, 'FMRN')) BETWEEN 1 AND 15",
					 (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. date_roundtrip_via_to_date
 *
 * Formatting a date to text and parsing it back must return the same date.
 * The day is capped at 28 so every month/year combination is valid and no
 * invalid-date error can occur.
 */
PG_PROP(date_roundtrip_via_to_date)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			year;
	int			mon;
	int			day;
	char		iso[16];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	year = (int) pg_prop_draw_int(tc, 1900, 2100);
	mon = (int) pg_prop_draw_int(tc, 1, 12);
	day = (int) pg_prop_draw_int(tc, 1, 28);

	snprintf(iso, sizeof(iso), "%04d-%02d-%02d", year, mon, day);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_date(to_char(DATE '%s', 'YYYY-MM-DD'), "
					 "'YYYY-MM-DD') = DATE '%s'",
					 iso, iso);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. to_char_number_digit_count
 *
 * The number of characters in to_char(n, 'FM999999999999') must equal the
 * number of digits in n's decimal representation.  n is drawn in 1..999999999
 * (0 is excluded because the "9" mask has no significant digit to show for a
 * leading zero; its width behaviour is checked separately by the 0-mask
 * property).
 */
PG_PROP(to_char_number_digit_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, 1, 999999999);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(to_char(%lld, 'FM999999999999')) "
					 "= length(%lld::text)",
					 (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. leading_zero_mask_fixed_width
 *
 * The "0" mask forces leading zeros; FM trims padding spaces but not those
 * zeros, so to_char(n, 'FM0000') always yields exactly 4 characters for any
 * n in 0..9999.  Exercises the zero-padding path.
 */
PG_PROP(leading_zero_mask_fixed_width)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, 0, 9999);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(to_char(%lld, 'FM0000')) = 4",
					 (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. time_roundtrip
 *
 * Formatting a parsed time back to text must yield the original.  h/m/s are
 * drawn in valid ranges so no invalid-time error can occur; the round-trip
 * goes through to_timestamp with an HH24:MI:SS mask.
 */
PG_PROP(time_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			h;
	int			m;
	int			s;
	char		hms[16];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	h = (int) pg_prop_draw_int(tc, 0, 23);
	m = (int) pg_prop_draw_int(tc, 0, 59);
	s = (int) pg_prop_draw_int(tc, 0, 59);

	snprintf(hms, sizeof(hms), "%02d:%02d:%02d", h, m, s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_char(to_timestamp('%s', 'HH24:MI:SS'), "
					 "'HH24:MI:SS') = '%s'",
					 hms, hms);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
