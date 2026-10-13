/*-------------------------------------------------------------------------
 *
 * prop_toformat.c
 *	  Property-based tests for to_char / to_number / to_date / to_timestamp
 *	  formatting round-trips (src/backend/utils/adt/formatting.c).
 *
 * These check that a value formatted to text with a numeric/date mask and
 * parsed back equals the original, and that fixed-width and sign masks shape
 * the output the way the mask spells it.  A failure means a numeric digit
 * mask, a zero-pad width, a date/time field, or a sign was emitted or parsed
 * wrongly -- a formatting.c bug, not a rounding artefact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_toformat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "toformat"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * True when a SQL error is a legitimate out-of-range/overflow/unsupported
 * condition rather than a wrong-results bug.  The bounded input ranges below
 * are chosen so this should not fire, but it is kept so that any genuine
 * overflow (e.g. a to_number that cannot represent the formatted width) is
 * skipped rather than misreported.
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
 * Run a boolean-valued SELECT and require it to return "t".  A legitimate
 * out-of-range/overflow error discards the case (the mask or value could not
 * represent the number); any other error, a NULL, or an "f" is a failure.
 * Copied from the require_true_or_skip style in prop_tochar.c / prop_float.c.
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
 * 1. to_number_to_char_roundtrip
 *
 * For n in 0..999999, parsing back the FM-formatted value recovers n:
 * to_number(to_char(n, 'FM999999'), '999999') = n.  FM strips the padding
 * that would otherwise leave leading spaces for to_number to choke on.
 */
PG_PROP(to_number_to_char_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, 0, 999999);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_number(to_char((%lld), 'FM999999'), "
					 "'999999') = (%lld)",
					 (long long) n, (long long) n);

	require_true_or_skip(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. to_char_pads_width
 *
 * A '0' mask zero-pads to a fixed width, so for n in 0..999999 the formatted
 * text is always exactly 6 characters: length(to_char(n, '000000')) = 6.
 */
PG_PROP(to_char_pads_width)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, 0, 999999);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(to_char((%lld), 'FM000000')) = 6",
					 (long long) n);

	require_true_or_skip(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. to_date_roundtrip
 *
 * A zero-padded ISO date parsed by to_date and reformatted with the same mask
 * returns the identical string, for y 2000..2030, mo 1..12, d 1..28 (always a
 * valid day).  Components are zero-padded in C to match YYYY-MM-DD exactly.
 */
PG_PROP(to_date_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		y,
				mo,
				d;
	char		date[16];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	y = pg_prop_draw_int(tc, 2000, 2030);
	mo = pg_prop_draw_int(tc, 1, 12);
	d = pg_prop_draw_int(tc, 1, 28);

	snprintf(date, sizeof(date), "%04d-%02d-%02d",
			 (int) y, (int) mo, (int) d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_char(to_date('%s', 'YYYY-MM-DD'), "
					 "'YYYY-MM-DD') = '%s'",
					 date, date);

	require_true_or_skip(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. to_timestamp_roundtrip
 *
 * A zero-padded timestamp parsed by to_timestamp and reformatted with the
 * same mask returns the identical string.  Day capped at 28, hour 0..23,
 * minute/second 0..59, so every drawn value is valid and no DST/overflow
 * ambiguity arises.
 */
PG_PROP(to_timestamp_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		y,
				mo,
				d,
				h,
				mi,
				s;
	char		ts[32];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	y = pg_prop_draw_int(tc, 2000, 2030);
	mo = pg_prop_draw_int(tc, 1, 12);
	d = pg_prop_draw_int(tc, 1, 28);
	h = pg_prop_draw_int(tc, 0, 23);
	mi = pg_prop_draw_int(tc, 0, 59);
	s = pg_prop_draw_int(tc, 0, 59);

	snprintf(ts, sizeof(ts), "%04d-%02d-%02d %02d:%02d:%02d",
			 (int) y, (int) mo, (int) d, (int) h, (int) mi, (int) s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_char(to_timestamp('%s', "
					 "'YYYY-MM-DD HH24:MI:SS'), "
					 "'YYYY-MM-DD HH24:MI:SS') = '%s'",
					 ts, ts);

	require_true_or_skip(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. to_char_sign
 *
 * The S format forces an explicit sign; the FMS variant then formats a signed
 * integer that to_number parses back exactly, for n in -999999..999999:
 * to_number(to_char(n, 'FMS999999'), 'S999999') = n.  This exercises the sign
 * emit/parse path for both signs across zero.
 */
PG_PROP(to_char_sign)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, -999999, 999999);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_number(to_char((%lld), 'FMS999999'), "
					 "'S999999') = (%lld)",
					 (long long) n, (long long) n);

	require_true_or_skip(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. to_char_numeric_scale
 *
 * A three-decimal mask formats a numeric rounded to three places, and parsing
 * that back equals round(x, 3): to_number(to_char(x, 'FM999999.000'),
 * '999999.000') = round(x, 3).  x is drawn with up to six integer digits so
 * the mask always has room; FM strips the leading padding before to_number.
 */
PG_PROP(to_char_numeric_scale)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		ipart,
				fpart;
	bool		neg;
	char		x[32];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* up to 6 integer digits (fits '999999') and 4 fractional digits */
	ipart = pg_prop_draw_int(tc, 0, 999999);
	fpart = pg_prop_draw_int(tc, 0, 9999);
	neg = pg_prop_draw_bool(tc);

	snprintf(x, sizeof(x), "%s%d.%04d",
			 neg ? "-" : "", (int) ipart, (int) fpart);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_number(to_char((%s)::numeric, "
					 "'FM999999.000'), '999999.000') = "
					 "round((%s)::numeric, 3)",
					 x, x);

	require_true_or_skip(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. to_date_month_name
 *
 * Formatting a parsed date with 'MM' yields the zero-padded month number, for
 * mo 1..12: to_char(to_date('2020-<mo>-15', ...), 'MM') = lpad(mo::text, 2,
 * '0').  Isolates the month field through the parse/format round-trip against
 * an independent oracle built in SQL.
 */
PG_PROP(to_date_month_name)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		mo;
	char		date[16];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	mo = pg_prop_draw_int(tc, 1, 12);

	snprintf(date, sizeof(date), "2020-%02d-15", (int) mo);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_char(to_date('%s', 'YYYY-MM-DD'), 'MM') = "
					 "lpad((%lld)::text, 2, '0')",
					 date, (long long) mo);

	require_true_or_skip(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
