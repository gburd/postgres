/*-------------------------------------------------------------------------
 *
 * prop_interval.c
 *	  Property-based tests for interval and timestamp arithmetic
 *	  (src/backend/utils/adt/timestamp.c, datetime.c).
 *
 * Target #8 in the design.  The interval/timestamp types have taken ~19 fixes
 * since 2023: overflow in timestamp + interval (4019285c064), date_bin
 * rounding (d163fdbfea8), interval field extraction (6be39d77a70), and the
 * infinite-interval rework (519fc1bd9e9).  That is a dense bug surface because
 * intervals carry three independent fields (months, days, microseconds) whose
 * mixed-sign and overflow interactions are easy to get subtly wrong.
 *
 * The oracle here is round-trip and algebraic identity, evaluated entirely in
 * SQL: an interval formatted to text and parsed back must equal the original;
 * a day/time interval added then subtracted must return the original
 * timestamp; the comparator must be a proper total order (including
 * infinities); and justify_hours must preserve the represented value.  Each
 * invariant is computed as a boolean in the query and the scalar result is
 * required to be "t".
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_interval.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "interval"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * True when a SQL error is a legitimate out-of-range/overflow condition rather
 * than a wrong-results bug.  Random field counts added to a timestamp near the
 * ends of its supported range can legally overflow; those cases are discarded,
 * not reported.
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
 * Build a random interval literal into buf, e.g.
 *		'1 years 2 mons -3 days 4 hours 5 mins 6.5 secs'::interval
 *
 * Field counts are kept small so counterexamples stay legible and so the
 * oracle itself does not drown in uninteresting overflow.  The whole interval
 * is randomly negated.  About 1 in 10 draws instead emits an infinite interval
 * to exercise the infinite-interval paths (519fc1bd9e9).
 *
 * When time_only is true, no year/month fields are drawn, so the interval is
 * exactly invertible under timestamp addition (months and years are not, by
 * design, because they denote variable spans).
 */
static void
draw_interval(pg_prop_tc *tc, StringInfo buf, bool allow_infinite, bool time_only)
{
	int			years;
	int			mons;
	int			days;
	int			hours;
	int			mins;
	int			secs;
	bool		frac;
	bool		negate;

	if (allow_infinite && pg_prop_draw_int(tc, 0, 9) == 0)
	{
		appendStringInfoString(buf,
							   pg_prop_draw_bool(tc)
							   ? "'infinity'::interval"
							   : "'-infinity'::interval");
		return;
	}

	years = time_only ? 0 : (int) pg_prop_draw_int(tc, -5, 5);
	mons = time_only ? 0 : (int) pg_prop_draw_int(tc, -15, 15);
	days = (int) pg_prop_draw_int(tc, -40, 40);
	hours = (int) pg_prop_draw_int(tc, -50, 50);
	mins = (int) pg_prop_draw_int(tc, -120, 120);
	secs = (int) pg_prop_draw_int(tc, -120, 120);
	frac = pg_prop_draw_bool(tc);
	negate = pg_prop_draw_bool(tc);

	appendStringInfoChar(buf, '\'');
	appendStringInfo(buf, "%d years %d mons %d days %d hours %d mins ",
					 years, mons, days, hours, mins);
	if (frac)
		appendStringInfo(buf, "%d.5 secs", secs);
	else
		appendStringInfo(buf, "%d secs", secs);
	appendStringInfoString(buf, "'::interval");

	if (negate)
	{
		/* Prepend unary minus; wrap so operator precedence is unambiguous. */
		char	   *inner = pstrdup(buf->data);

		resetStringInfo(buf);
		appendStringInfo(buf, "(- (%s))", inner);
		pfree(inner);
	}
}

/*
 * Build a random timestamp literal into buf, e.g.
 *		'2024-03-15 08:30:00'::timestamp
 *
 * The range is kept well inside the supported timestamp range (year
 * 1900..2100) so that only interval arithmetic, not the literal itself, drives
 * any overflow.  day is capped at 28 so every month is valid.  About 1 in 10
 * draws instead emits an infinite timestamp.
 */
static void
draw_timestamp(pg_prop_tc *tc, StringInfo buf, bool allow_infinite)
{
	int			year;
	int			mon;
	int			day;
	int			hour;
	int			min;
	int			sec;

	if (allow_infinite && pg_prop_draw_int(tc, 0, 9) == 0)
	{
		appendStringInfoString(buf,
							   pg_prop_draw_bool(tc)
							   ? "'infinity'::timestamp"
							   : "'-infinity'::timestamp");
		return;
	}

	year = (int) pg_prop_draw_int(tc, 1900, 2100);
	mon = (int) pg_prop_draw_int(tc, 1, 12);
	day = (int) pg_prop_draw_int(tc, 1, 28);
	hour = (int) pg_prop_draw_int(tc, 0, 23);
	min = (int) pg_prop_draw_int(tc, 0, 59);
	sec = (int) pg_prop_draw_int(tc, 0, 59);

	appendStringInfo(buf, "'%04d-%02d-%02d %02d:%02d:%02d'::timestamp",
					 year, mon, day, hour, min, sec);
}

/*
 * 1. interval_text_roundtrip
 *
 * Formatting a (finite) interval to text and parsing it back must yield the
 * same interval, under every IntervalStyle.  This targets the IntervalStyle
 * round-trip and mixed-sign formatting: the format and parse paths must agree.
 */
PG_PROP(interval_text_roundtrip)
{
	static const char *const styles[] = {
		"postgres", "postgres_verbose", "sql_standard", "iso_8601"
	};
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData e;
	StringInfoData q;
	const char *style;
	char	   *setsql;
	char	   *result;
	bool		failed;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* a non-infinite interval -- infinities are gated out for this one */
	initStringInfo(&e);
	draw_interval(tc, &e, false, false);

	/* exercise all four IntervalStyle output formats */
	style = styles[pg_prop_draw_int(tc, 0, lengthof(styles) - 1)];
	pg_prop_note(tc, "IntervalStyle = %s", style);

	setsql = psprintf("SET IntervalStyle = '%s'", style);
	if (!pg_prop_sql_exec(conn, setsql, &err))
		pg_prop_fail(tc, "could not set IntervalStyle '%s': %s",
					 style, err ? err : "");
	pfree(setsql);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s) = (%s)::text::interval", e.data, e.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
	{
		/*
		 * A valid interval that cannot parse its own formatted output is a
		 * round-trip bug, not a legitimate overflow.
		 */
		pg_prop_fail(tc,
					 "interval failed to round-trip through text "
					 "(IntervalStyle %s)\n  interval: %s\n  error: %s",
					 style, e.data, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "interval != (interval)::text::interval "
					 "(IntervalStyle %s)\n  interval: %s\n  result: %s",
					 style, e.data, result ? result : "NULL");

	pg_free(result);
	pfree(e.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. timestamp_plus_minus_interval
 *
 * For a day/time-only interval E (no month/year components) and any timestamp
 * T, (T + E - E) must equal T.  Months and years are deliberately excluded
 * because they denote variable spans and are not invertible this way.
 *
 * Targets 4019285c064 (overflow in timestamp + interval) and the add/subtract
 * inverse.  Timestamp and interval are both finite here; an overflow near the
 * range ends is a legitimate error and the case is discarded.
 */
PG_PROP(timestamp_plus_minus_interval)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData t;
	StringInfoData e;
	StringInfoData q;
	char	   *result;
	bool		failed;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&t);
	draw_timestamp(tc, &t, false);		/* non-infinite */

	initStringInfo(&e);
	draw_interval(tc, &e, false, true); /* non-infinite, day/time only */

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s + %s - %s) = %s",
					 t.data, e.data, e.data, t.data);

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
			pfree(e.data);
			pfree(q.data);
			pg_prop_disconnect(conn);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc,
					 "unexpected error on (T + E - E) = T\n"
					 "  T: %s\n  E: %s\n  error: %s",
					 t.data, e.data, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "(T + E - E) != T for day/time interval\n"
					 "  T: %s\n  E: %s\n  result: %s",
					 t.data, e.data, result ? result : "NULL");

	pg_free(result);
	pfree(t.data);
	pfree(e.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. interval_comparison_total_order
 *
 * For any two intervals E1, E2 (infinities included), exactly one of
 * (E1 < E2), (E1 = E2), (E1 > E2) must hold.  This checks that the interval
 * comparator is a proper total order, including across the infinite endpoints
 * introduced by 519fc1bd9e9.
 */
PG_PROP(interval_comparison_total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData e1;
	StringInfoData e2;
	StringInfoData q;
	char	   *result;
	bool		failed;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&e1);
	initStringInfo(&e2);
	draw_interval(tc, &e1, true, false);	/* infinities allowed */
	draw_interval(tc, &e2, true, false);

	/* exactly one of <, =, > must be true */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ( ((%s) < (%s))::int "
					 "+ ((%s) = (%s))::int "
					 "+ ((%s) > (%s))::int ) = 1",
					 e1.data, e2.data,
					 e1.data, e2.data,
					 e1.data, e2.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
		pg_prop_fail(tc,
					 "error evaluating interval total order\n"
					 "  E1: %s\n  E2: %s\n  error: %s",
					 e1.data, e2.data, err ? err : "(unknown)");

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "interval comparator is not a total order\n"
					 "  E1: %s\n  E2: %s\n  (exactly one of <,=,> expected) result: %s",
					 e1.data, e2.data, result ? result : "NULL");

	pg_free(result);
	pfree(e1.data);
	pfree(e2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. justify_interval_preserves_value
 *
 * justify_hours only redistributes the microsecond field into days without
 * changing the value an interval represents.  For a day/time-only interval E
 * and a fixed reference timestamp R, (R + E) must equal (R + justify_hours(E)).
 *
 * E is gated to day/time-only (no months/years) and justify_hours is used
 * rather than justify_interval, because justify_interval's day<->month
 * redistribution genuinely changes results across month boundaries and is not
 * a value-preserving identity under timestamp addition.
 */
PG_PROP(justify_interval_preserves_value)
{
	static const char *const ref = "'2000-06-15 12:00:00'::timestamp";
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData e;
	StringInfoData q;
	char	   *result;
	bool		failed;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&e);
	draw_interval(tc, &e, false, true); /* non-infinite, day/time only */

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s + (%s)) = (%s + justify_hours(%s))",
					 ref, e.data, ref, e.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
	{
		if (is_overflow(err))
		{
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pfree(e.data);
			pfree(q.data);
			pg_prop_disconnect(conn);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc,
					 "unexpected error on justify_hours identity\n"
					 "  E: %s\n  error: %s",
					 e.data, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "justify_hours changed the represented value\n"
					 "  E: %s\n  R + E != R + justify_hours(E); result: %s",
					 e.data, result ? result : "NULL");

	pg_free(result);
	pfree(e.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
