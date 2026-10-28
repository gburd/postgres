/*-------------------------------------------------------------------------
 *
 * prop_intervalio.c
 *	  Property-based tests for interval text I/O (ISO 8601 and verbose).
 *
 * The oracle is round-trip identity through interval's textual
 * representation: an interval rendered to text and parsed back must equal
 * itself, and an ISO 8601 literal ('P1DT2H') must denote the same value
 * make_interval() builds from the same fields.  All intervals here are
 * month-free (days/hours/mins/secs only), so epoch extraction is exact
 * (a day is 86400s) and reconstruction from fields is lossless.  A broken
 * round-trip is a bug in interval_in/interval_out or the ISO/verbose
 * parsing, distinct from interval arithmetic, parts, and comparison.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_intervalio.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "intervalio"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means interval I/O errored or produced NULL on valid inputs,
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
 * Draw a month-free make_interval(...) expression into buf from small field
 * counts: days, hours, mins, secs.  Months are deliberately excluded so epoch
 * is exact and field reconstruction is lossless.  Returns the drawn fields
 * through the out-params so callers can also build an epoch expectation.
 */
static void
draw_make_interval(pg_prop_tc *tc, StringInfo buf,
				   int *d, int *h, int *m, int *s)
{
	*d = (int) pg_prop_draw_int(tc, -5, 5);
	*h = (int) pg_prop_draw_int(tc, -24, 24);
	*m = (int) pg_prop_draw_int(tc, -59, 59);
	*s = (int) pg_prop_draw_int(tc, -59, 59);

	appendStringInfo(buf,
					 "make_interval(days=>(%d), hours=>(%d), mins=>(%d), secs=>(%d))",
					 *d, *h, *m, *s);
}

/*
 * Property 1: text round-trip.  Rendering an interval to text and parsing it
 * back must yield the original interval.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData iv,
				q;
	int			d,
				h,
				m,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&iv);
	draw_make_interval(tc, &iv, &d, &h, &m, &s);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s)::text::interval = %s)",
					 iv.data, iv.data);

	require_true(tc, conn, q.data);

	pfree(iv.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 2: ISO 8601 round-trip.  An ISO interval literal must denote the
 * same value make_interval() builds from the same days/hours fields.
 */
PG_PROP(iso8601_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			d,
				h;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	d = (int) pg_prop_draw_int(tc, 0, 9);
	h = (int) pg_prop_draw_int(tc, 0, 23);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('P%dDT%dH'::interval = make_interval(days=>(%d), hours=>(%d)))",
					 d, h, d, h);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 3: justify_hours output is stable.  Rendering justify_hours(I) to
 * text and parsing it back must equal justify_hours(I).
 */
PG_PROP(justify_text_stable)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData iv,
				q;
	int			d,
				h,
				m,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&iv);
	draw_make_interval(tc, &iv, &d, &h, &m, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (justify_hours(%s)::text::interval = justify_hours(%s))",
					 iv.data, iv.data);

	require_true(tc, conn, q.data);

	pfree(iv.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 4: seconds literal round-trip.  interval '<n> seconds' must equal
 * make_interval(secs=><n>) for n in -3600..3600.
 */
PG_PROP(seconds_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, -3600, 3600);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (interval '%d seconds' = make_interval(secs=>(%d)))",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 5: negative interval round-trip.  With a strictly negative hours
 * field, rendering to text and parsing back must yield the original interval.
 */
PG_PROP(negative_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData iv,
				q;
	int			h;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	h = (int) pg_prop_draw_int(tc, -48, -1);

	initStringInfo(&iv);
	appendStringInfo(&iv, "make_interval(hours=>(%d))", h);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s)::text::interval = %s)",
					 iv.data, iv.data);

	require_true(tc, conn, q.data);

	pfree(iv.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 6: epoch is exact for month-free intervals.  extract(epoch from I)
 * must equal d*86400 + h*3600 + m*60 + s for the drawn fields (an interval day
 * is exactly 86400s; no months means no variable-length month to make this
 * approximate).
 */
PG_PROP(extract_epoch_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData iv,
				q;
	int			d,
				h,
				m,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&iv);
	draw_make_interval(tc, &iv, &d, &h, &m, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (extract(epoch from %s) = (%d::bigint*86400 + %d::bigint*3600 + %d::bigint*60 + %d::bigint))",
					 iv.data, d, h, m, s);

	require_true(tc, conn, q.data);

	pfree(iv.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 7: the zero interval.  interval '0' must equal make_interval() and
 * must equal the difference of an interval with itself.
 */
PG_PROP(zero_interval)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (interval '0' = make_interval() "
				 "AND interval '0' = (make_interval(hours=>1) - make_interval(hours=>1)))");

	pg_prop_disconnect(conn);
}

/*
 * Property 8: ISO zero output parses to the zero interval.  'P0D'::interval
 * must equal interval '0'.
 */
PG_PROP(iso_output_parse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn, "SELECT ('P0D'::interval = interval '0')");

	pg_prop_disconnect(conn);
}
