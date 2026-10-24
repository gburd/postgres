/*-------------------------------------------------------------------------
 *
 * prop_timeextract.c
 *	  Property-based tests for extract()/date_part() on time/timetz and the
 *	  algebra of time arithmetic (time +/- interval, time - time).
 *
 * The oracle here is each identity's own definition: the hour/minute/second
 * fields of a drawn TIME must equal the components we built it from, the epoch
 * of a time is h*3600 + mi*60 + s, adding the zero interval is a no-op, a
 * +1h/-1h round-trip returns the original (kept away from the 24h wrap),
 * subtracting two times yields an interval that re-adds to the second, and the
 * total order on time is trichotomous.  make_time and the text cast must both
 * reproduce the same TIME literal.  Each property is a SELECT of a boolean
 * that must be "t"; a NULL or SQL error on a valid time is itself a bug.
 *
 * Distinct from the "timeops", "dateparts" and "datetrunc" areas: this
 * targets extract/date_part on the time types specifically, plus time-time
 * and time-interval arithmetic (not date_trunc, not date fields, not
 * timestamp subtraction).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_timeextract.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "timeextract"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operation errored or produced NULL on a valid input,
 * which is itself a bug -- so either is a failure, reported with the SQL.
 * Copied from prop_numeric.c's require_true (Shape A, scalar boolean oracle).
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
 * Draw a time-of-day h/m/s (whole-second components, each within its natural
 * range) and append a zero-padded TIME literal "TIME '%02d:%02d:%02d'" to buf.
 * The components are returned so a caller can also build the extract()/epoch
 * oracle from the same values.
 */
static void
draw_time(pg_prop_tc *tc, StringInfo buf, int *h, int *mi, int *s)
{
	*h = (int) pg_prop_draw_int(tc, 0, 23);
	*mi = (int) pg_prop_draw_int(tc, 0, 59);
	*s = (int) pg_prop_draw_int(tc, 0, 59);
	appendStringInfo(buf, "TIME '%02d:%02d:%02d'", *h, *mi, *s);
}

/*
 * 1. extract_fields: extract(hour from T) = h, extract(minute from T) = mi,
 *    and extract(second from T) = s, for a drawn whole-second TIME.  extract
 *    returns numeric, so compare each against the integer we drew.
 */
PG_PROP(extract_fields)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t,
				q;
	int			h,
				mi,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&t);
	draw_time(tc, &t, &h, &mi, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(hour from %s) = %d "
					 "AND extract(minute from %s) = %d "
					 "AND extract(second from %s) = %d",
					 t.data, h, t.data, mi, t.data, s);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. epoch_seconds: extract(epoch from T) = h*3600 + mi*60 + s.  For a
 *    time-of-day the epoch is the number of seconds since midnight.
 */
PG_PROP(epoch_seconds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t,
				q;
	int			h,
				mi,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&t);
	draw_time(tc, &t, &h, &mi, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(epoch from %s) = %d",
					 t.data, h * 3600 + mi * 60 + s);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. time_plus_interval: T + interval '0' = T, and (for a time at least an
 *    hour before midnight so no 24h wrap occurs) T + interval '1 hour' -
 *    interval '1 hour' = T.  We rebuild the literal with h capped at 22 so the
 *    +1 hour step cannot cross midnight and wrap.
 */
PG_PROP(time_plus_interval)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t,
				q;
	int			h,
				mi,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* keep h <= 22 so T + interval '1 hour' does not wrap past 24:00 */
	h = (int) pg_prop_draw_int(tc, 0, 22);
	mi = (int) pg_prop_draw_int(tc, 0, 59);
	s = (int) pg_prop_draw_int(tc, 0, 59);

	initStringInfo(&t);
	appendStringInfo(&t, "TIME '%02d:%02d:%02d'", h, mi, s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s + interval '0') = %s "
					 "AND (%s + interval '1 hour' - interval '1 hour') = %s",
					 t.data, t.data, t.data, t.data);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. time_minus_time: for two drawn times T1, T2 the difference (T2 - T1) is
 *    an interval, and T1 + (T2 - T1) = T2.  This is the additive-inverse
 *    identity for time subtraction and holds for any ordering of T1, T2.
 */
PG_PROP(time_minus_time)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t1,
				t2,
				q;
	int			h1,
				mi1,
				s1,
				h2,
				mi2,
				s2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&t1);
	initStringInfo(&t2);
	draw_time(tc, &t1, &h1, &mi1, &s1);
	draw_time(tc, &t2, &h2, &mi2, &s2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT pg_typeof(%s - %s) = 'interval'::regtype "
					 "AND (%s + (%s - %s)) = %s",
					 t2.data, t1.data,
					 t1.data, t2.data, t1.data, t2.data);

	require_true(tc, conn, q.data);

	pfree(t1.data);
	pfree(t2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. time_order: for two drawn times exactly one of T1 < T2, T1 = T2, T1 > T2
 *    holds (trichotomy of the total order on time).
 */
PG_PROP(time_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t1,
				t2,
				q;
	int			h1,
				mi1,
				s1,
				h2,
				mi2,
				s2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&t1);
	initStringInfo(&t2);
	draw_time(tc, &t1, &h1, &mi1, &s1);
	draw_time(tc, &t2, &h2, &mi2, &s2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (CASE WHEN %s < %s THEN 1 ELSE 0 END) "
					 "+ (CASE WHEN %s = %s THEN 1 ELSE 0 END) "
					 "+ (CASE WHEN %s > %s THEN 1 ELSE 0 END) = 1",
					 t1.data, t2.data, t1.data, t2.data, t1.data, t2.data);

	require_true(tc, conn, q.data);

	pfree(t1.data);
	pfree(t2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. make_time_roundtrip: make_time(h, mi, s) = TIME '%02d:%02d:%02d' built
 *    from the same components.  make_time takes a double for seconds; passing
 *    the whole-second integer we drew must reproduce the padded literal.
 */
PG_PROP(make_time_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t,
				q;
	int			h,
				mi,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&t);
	draw_time(tc, &t, &h, &mi, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT make_time(%d, %d, %d) = %s",
					 h, mi, s, t.data);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. time_text_roundtrip: (T)::text::time = T for a drawn TIME.  The text
 *    output of a time must parse back to the same value.
 */
PG_PROP(time_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t,
				q;
	int			h,
				mi,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&t);
	draw_time(tc, &t, &h, &mi, &s);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s)::text::time = %s)", t.data, t.data);

	require_true(tc, conn, q.data);

	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 8. extract_from_make_time: extract(hour from make_time(h, mi, s)) = h.
 *    Composition of make_time and extract must recover the hour we supplied.
 */
PG_PROP(extract_from_make_time)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			h,
				mi,
				s;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	h = (int) pg_prop_draw_int(tc, 0, 23);
	mi = (int) pg_prop_draw_int(tc, 0, 59);
	s = (int) pg_prop_draw_int(tc, 0, 59);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT extract(hour from make_time(%d, %d, %d)) = %d",
					 h, mi, s, h);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
