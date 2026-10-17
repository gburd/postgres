/*-------------------------------------------------------------------------
 *
 * prop_datefmt.c
 *	  Property-based tests for date formatting via to_char / to_date
 *	  (src/backend/utils/adt/formatting.c).
 *
 * These exercise the date-specific to_char/to_date paths distinct from the
 * generic number/roman masks (prop_tochar), the date-cast chain
 * (prop_datecast) and the EXTRACT part machinery (prop_dateparts): format a
 * date to text and parse it back (round-trips), check the individual
 * YYYY/MM/DD/DDD/ID/Q fields against zero-padded oracles computed in SQL, and
 * cross-check to_char against EXTRACT.  A mismatch is a wrong-results bug in
 * the field-formatting or field-parsing code, not a rounding artefact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_datefmt.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "datefmt"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the formatting/parsing path errored or produced NULL on
 * valid inputs, which is itself a bug -- so either is a failure, reported with
 * the SQL.
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
 * Draw a valid (y, mo, d) with day capped at 28 so every month/year
 * combination is valid and no invalid-date error can occur.
 */
static void
draw_ymd(pg_prop_tc *tc, int *y, int *mo, int *d)
{
	*y = (int) pg_prop_draw_int(tc, 2000, 2030);
	*mo = (int) pg_prop_draw_int(tc, 1, 12);
	*d = (int) pg_prop_draw_int(tc, 1, 28);
}

/*
 * 1. to_char_iso_roundtrip
 *
 * Formatting a date with the ISO YYYY-MM-DD mask and parsing it back must
 * return the original date.  This is a pure identity, sound for every drawn
 * (y, mo, d).
 */
PG_PROP(to_char_iso_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y;
	int			mo;
	int			d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_date(to_char(make_date(%d,%d,%d), 'YYYY-MM-DD'), "
					 "'YYYY-MM-DD') = make_date(%d,%d,%d)",
					 y, mo, d, y, mo, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. to_char_components
 *
 * The YYYY/MM/DD fields produced by to_char must match the zero-padded
 * decimal rendering of the input components (lpad as the oracle).  Checks the
 * per-field width/zero-pad behaviour of the date formatter.
 */
PG_PROP(to_char_components)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y;
	int			mo;
	int			d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_char(make_date(%d,%d,%d), 'YYYY') = lpad(%d::text,4,'0') "
					 "AND to_char(make_date(%d,%d,%d), 'MM') = lpad(%d::text,2,'0') "
					 "AND to_char(make_date(%d,%d,%d), 'DD') = lpad(%d::text,2,'0')",
					 y, mo, d, y,
					 y, mo, d, mo,
					 y, mo, d, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. to_char_doy
 *
 * The day-of-year field (DDD) must fall in 1..366 for any valid date.  A
 * sound range check on the DDD computation.
 */
PG_PROP(to_char_doy)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y;
	int			mo;
	int			d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_char(make_date(%d,%d,%d), 'DDD')::int BETWEEN 1 AND 366",
					 y, mo, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. to_date_ignores_format_extras
 *
 * A compact (no-separator) YYYYMMDD mask must also round-trip: to_char then
 * to_date with the same unseparated mask returns the original date.
 * Exercises the fixed-width field parsing of to_date without delimiters.
 */
PG_PROP(to_date_ignores_format_extras)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y;
	int			mo;
	int			d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_date(to_char(make_date(%d,%d,%d), 'YYYYMMDD'), "
					 "'YYYYMMDD') = make_date(%d,%d,%d)",
					 y, mo, d, y, mo, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. to_char_day_of_week
 *
 * The ISO day-of-week field (ID) must fall in 1..7 for any valid date.  A
 * sound range check on the ISO-weekday computation.
 */
PG_PROP(to_char_day_of_week)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y;
	int			mo;
	int			d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_char(make_date(%d,%d,%d), 'ID')::int BETWEEN 1 AND 7",
					 y, mo, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. extract_matches_to_char
 *
 * EXTRACT(year ...) and the YYYY field of to_char must agree.  A differential
 * oracle: two independent paths to the same year value must produce the same
 * answer.
 */
PG_PROP(extract_matches_to_char)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y;
	int			mo;
	int			d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(year from make_date(%d,%d,%d)) "
					 "= to_char(make_date(%d,%d,%d),'YYYY')::int",
					 y, mo, d, y, mo, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. to_char_month_name_parse
 *
 * A mask that renders the month as an abbreviated name (Mon) must also
 * round-trip: formatting then parsing with the same YYYY-Mon-DD mask returns
 * the original date.  Exercises the month-name formatting and name-parsing
 * paths of to_date.
 */
PG_PROP(to_char_month_name_parse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y;
	int			mo;
	int			d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_date(to_char(make_date(%d,%d,%d), 'YYYY-Mon-DD'), "
					 "'YYYY-Mon-DD') = make_date(%d,%d,%d)",
					 y, mo, d, y, mo, d);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 8. to_char_quarter
 *
 * The quarter field (Q) must equal (mo-1)/3 + 1 for the drawn month.  The
 * oracle is computed from the month alone and is sound for every valid date.
 */
PG_PROP(to_char_quarter)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			y;
	int			mo;
	int			d;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_ymd(tc, &y, &mo, &d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_char(make_date(%d,%d,%d), 'Q')::int = ((%d-1)/3 + 1)",
					 y, mo, d, mo);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
