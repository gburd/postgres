/*-------------------------------------------------------------------------
 *
 * prop_cf_types.c
 *	  Value oracles for built-in functions around open commitfest bugs.
 *
 * Each property draws inputs around the trigger of one or more CF bug
 * fixes and checks the function's result against an oracle computed a
 * different way: a C model of the answer, an independent SQL formulation,
 * or an algebraic identity (round trip, monotonicity, row-order
 * independence).  A correct server satisfies every invariant by
 * construction; each property's comment says why.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_cf_types.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cf_types"
#include "pg_prop.h"

#include "lib/stringinfo.h"

static pg_prop_conn *
cfb3_connect(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

static void
cfb3_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "setup failed: %s\n  SQL: %s", err ? err : "", sql);
}

/* an error message that no input may provoke on a correct server */
static bool
cfb3_internal_error(const char *err)
{
	return err != NULL &&
		(strstr(err, "cache lookup failed") != NULL ||
		 strstr(err, "unexpected") != NULL ||
		 strstr(err, "unrecognized") != NULL ||
		 strstr(err, "TRAP") != NULL);
}

/*
 * Run a scalar query that must succeed; return its value (NULL for SQL NULL).
 */
static char *
cfb3_scalar(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v = pg_prop_sql_scalar(conn, sql, &failed, &err);

	if (failed)
		pg_prop_fail(tc, "query failed: %.300s\n  SQL: %.500s", err ? err : "", sql);
	return v;
}

/* ----------------------------------------------------------------
 * to_date / to_timestamp template fields   (CF 6792, 7411, 7416)
 * ----------------------------------------------------------------
 */

/* ISO-8601 weeks in ISO year y (52 or 53) */
static int
cfb3_iso_weeks(int64 y)
{
#define P(y) (((y) + (y) / 4 - (y) / 100 + (y) / 400) % 7)
	return (P(y) == 4 || P(y - 1) == 3) ? 53 : 52;
#undef P
}

static bool
cfb3_leap(int64 y)
{
	return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
}

/*
 * A template being assembled: the input string, the format, and the string
 * to_char(result, format) must reproduce if the input is accepted.
 */
typedef struct DtTpl
{
	StringInfoData in;
	StringInfoData fmt;
	StringInfoData exp;
	bool		fm;				/* FM on every numeric field */
	bool		valid;			/* every field drawn inside its domain */
	int			nfields;
} DtTpl;

/* note whether a drawn value lies in its field's domain */
static int64
dt_in(DtTpl *t, int64 v, int64 lo, int64 hi)
{
	if (v < lo || v > hi)
		t->valid = false;
	return v;
}

/* dt_seps[0] must stay the only whitespace separator, see dt_sep */
static const char *const dt_seps[] = {" ", "-", "/", ".", ":"};

/*
 * Separator before the next field.  Outside FX mode a format separator
 * consumes one separator character of the input, so after a space a minus
 * sign is eaten as a separator and "2024 -5" reads as day 5 -- documented
 * laxity, not a bug.  Fields that may get a negative value therefore get
 * a non-space separator ("nospace"); SSSSS, which a fixed server recovers
 * the swallowed minus for, may get any.
 */
static void
dt_sep(pg_prop_tc *tc, DtTpl *t, bool nospace)
{
	const char *sep;

	if (t->nfields++ == 0)
		return;
	sep = dt_seps[pg_prop_draw_int(tc, nospace ? 1 : 0, lengthof(dt_seps) - 1)];
	appendStringInfoString(&t->in, sep);
	appendStringInfoString(&t->fmt, sep);
	/* to_char emits the format's separator text verbatim */
	appendStringInfoString(&t->exp, sep);
}

/*
 * Append one numeric field "name" with input value v.  to_char pads the
 * field to "width" digits unless FM is given; "expect" is the value
 * to_char must render (== v except for the documented zero-means-unset
 * month/day, CF 6792).
 */
static void
dt_num(pg_prop_tc *tc, DtTpl *t, const char *name, int width,
	   int64 v, int64 expect)
{
	dt_sep(tc, t, v < 0);
	appendStringInfo(&t->fmt, "%s%s", t->fm ? "FM" : "", name);
	if (t->fm)
	{
		appendStringInfo(&t->in, "%lld", (long long) v);
		appendStringInfo(&t->exp, "%lld", (long long) expect);
	}
	else
	{
		appendStringInfo(&t->in, "%0*lld", width, (long long) v);
		appendStringInfo(&t->exp, "%0*lld", width, (long long) expect);
	}
}

/*
 * Draw a field value: mostly inside [lo, hi] (the range in which a round
 * trip is guaranteed for this year), sometimes anywhere in a wide window
 * around it that includes 0, negatives and 4+-digit values.  "bad_lo" ..
 * "bad_hi" is a range a correct server may accept without round-tripping
 * (e.g. ISO day 365..371 of a 52-week year), which is never drawn.
 */
static int64
dt_val(pg_prop_tc *tc, int64 lo, int64 hi, int64 wide, int64 bad_lo,
	   int64 bad_hi)
{
	int64		v;

	if (pg_prop_draw_int(tc, 0, 3) > 0)
		return pg_prop_draw_int(tc, lo, hi);
	v = pg_prop_draw_int(tc, -wide, wide);
	if (v >= bad_lo && v <= bad_hi)
		v = hi;
	return v;
}

static const char *const rm_good[] =
{"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII"};

/* roman strings that are not a month but start with one */
static const char *const rm_bad[] =
{"XIII", "IIII", "VV", "IIX", "XIV", "XX", "VIIII", "IXI", "XIX", "VIV", "IIV", "XIII"};

/* days in month m of Gregorian year y */
static int
cfb3_mdays(int64 y, int64 m)
{
	static const int md[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

	return (m == 2 && cfb3_leap(y)) ? 29 : md[m - 1];
}

/* which CF's fields a template exercises */
typedef enum DtFamily
{
	DT_RANGE,					/* CF 7411: DDD IDDD IW ID WW RM SSSSS */
	DT_Y_YYY,					/* CF 7416: Y,YYY millennia */
	DT_ZERO_MD					/* CF 6792: MM / DD, zero = unset */
} DtFamily;

/*
 * Build a to_date / to_timestamp template from a random sequence of fields
 * around family "fam" -- a year (YYYY, Y,YYY or IYYY), a day designation
 * (MM DD, RM [DD], DDD, WW, IDDD, IW [ID]), optionally SSSSS -- with random
 * separators and FM, each value drawn mostly in its domain but sometimes
 * far outside it (0, negative, 4+ digits, millennia past int, a roman
 * numeral with a month as a prefix).
 *
 * Invariant: when every field is inside its documented domain, to_X(s, f)
 * succeeds and to_char(to_X(s, f), f) = s.  Otherwise to_X may raise an
 * error, but if it returns a value that value must still render back as s:
 * silently reading "2024 999" (IYYY IDDD) as some other day, "XIII" as
 * December, "-1" (SSSSS) as 1, or 4294969320,024 (Y,YYY) as year 2024024
 * breaks the round trip.  The only out-of-domain values a correct server
 * maps elsewhere are the documented "zero month / zero day = unset" (CF
 * 6792), modelled as 1; in-domain ISO days/weeks past the end of a 52-week
 * ISO year, which roll into the next year, are never drawn.
 */
static void
dt_roundtrip(pg_prop_tc *tc, DtFamily fam)
{
	pg_prop_conn *conn;
	DtTpl		t;
	bool		ts;
	bool		iso;
	int			yfmt;
	int64		year;
	int			shape;
	StringInfoData q;
	bool		failed = false;
	char	   *err = NULL;
	char	   *got;

	initStringInfo(&t.in);
	initStringInfo(&t.fmt);
	initStringInfo(&t.exp);
	t.fm = pg_prop_draw_bool(tc);
	t.valid = true;
	t.nfields = 0;
	ts = pg_prop_draw_bool(tc);

	/* year: 0 = YYYY, 1 = Y,YYY, 2 = IYYY (ISO day fields follow) */
	if (fam == DT_Y_YYY)
		yfmt = 1;
	else if (fam == DT_ZERO_MD)
		yfmt = (int) pg_prop_draw_int(tc, 0, 1);
	else
		yfmt = (int) pg_prop_draw_int(tc, 0, 2);
	iso = (yfmt == 2);
	year = pg_prop_draw_int(tc, 1, 9999);
	if (yfmt == 0)
		dt_num(tc, &t, "YYYY", 4, year, year);
	else if (yfmt == 2)
		dt_num(tc, &t, "IYYY", 4, year, year);
	else
	{
		int64		mil = year / 1000;
		int64		rest = year % 1000;
		int64		shown = mil;

		if (fam == DT_Y_YYY && pg_prop_draw_bool(tc))
		{
			/* a millennium that aliases a valid one mod 2^32, or is huge */
			int64		k = pg_prop_draw_int(tc, -3, 3);

			t.valid = false;
			if (k == 0)
				shown = pg_prop_draw_int(tc, (int64) PG_INT32_MAX + 1,
										 PG_INT64_MAX / 10);
			else
				shown = mil + k * ((int64) 1 << 32);
		}
		dt_sep(tc, &t, false);
		appendStringInfoString(&t.fmt, "Y,YYY");
		appendStringInfo(&t.in, "%lld,%03lld", (long long) shown,
						 (long long) rest);
		appendStringInfo(&t.exp, "%lld,%03lld", (long long) shown,
						 (long long) rest);
	}

	/* day designation */
	shape = (int) pg_prop_draw_int(tc, 0, 3);
	if (iso)
	{
		int			weeks = cfb3_iso_weeks(year);

		if (shape <= 1)
		{
			int64		d = dt_in(&t, dt_val(tc, 1, weeks * 7, 2000,
											 weeks * 7 + 1, 371), 1, weeks * 7);

			dt_num(tc, &t, "IDDD", 3, d, d);
		}
		else
		{
			int64		w = dt_in(&t, dt_val(tc, 1, weeks, 200, weeks + 1, 53),
								  1, weeks);

			dt_num(tc, &t, "IW", 2, w, w);
			if (shape == 3)
			{
				int64		d = dt_in(&t, dt_val(tc, 1, 7, 20, 1, 7), 1, 7);

				dt_num(tc, &t, "ID", 1, d, d);
			}
		}
	}
	else if (fam != DT_RANGE || shape == 0)
	{
		/* MM DD; in DT_ZERO_MD zero month/day mean "unset", read as 1 */
		int64		mm;
		int64		dd;

		if (fam == DT_ZERO_MD)
		{
			mm = pg_prop_draw_int(tc, 0, 13);
			dd = pg_prop_draw_int(tc, 0, 32);
			if (mm > 12 || dd > cfb3_mdays(year, mm ? mm : 1))
				t.valid = false;
		}
		else
		{
			mm = pg_prop_draw_int(tc, 1, 12);
			dd = pg_prop_draw_int(tc, 1, cfb3_mdays(year, mm));
		}
		dt_num(tc, &t, "MM", 2, mm, mm ? mm : 1);
		dt_num(tc, &t, "DD", 2, dd, dd ? dd : 1);
	}
	else if (shape == 1)
	{
		/* RM [DD]: roman month, sometimes a non-month with a month prefix */
		bool		lower = pg_prop_draw_bool(tc);
		const char *r;

		if (pg_prop_draw_int(tc, 0, 2) == 0)
		{
			t.valid = false;
			r = rm_bad[pg_prop_draw_int(tc, 0, lengthof(rm_bad) - 1)];
		}
		else
			r = rm_good[pg_prop_draw_int(tc, 0, lengthof(rm_good) - 1)];
		dt_sep(tc, &t, false);
		appendStringInfoString(&t.fmt, lower ? "FMrm" : "FMRM");
		for (const char *c = r; *c; c++)
		{
			appendStringInfoChar(&t.in, lower ? pg_ascii_tolower(*c) : *c);
			appendStringInfoChar(&t.exp, lower ? pg_ascii_tolower(*c) : *c);
		}
		if (pg_prop_draw_bool(tc))
		{
			int64		dd = pg_prop_draw_int(tc, 1, 28);

			dt_num(tc, &t, "DD", 2, dd, dd);
		}
	}
	else if (shape == 2)
	{
		int			ndays = cfb3_leap(year) ? 366 : 365;
		int64		d = dt_in(&t, dt_val(tc, 1, ndays, 2000, ndays + 1, ndays + 1),
							  1, ndays);

		dt_num(tc, &t, "DDD", 3, d, d);
	}
	else
	{
		int64		w = dt_in(&t, dt_val(tc, 1, 53, 200, 1, 53), 1, 53);

		dt_num(tc, &t, "WW", 2, w, w);
	}

	/* seconds of day, to_timestamp only */
	if (ts && pg_prop_draw_bool(tc))
	{
		int64		sec = (fam == DT_RANGE) ?
			dt_in(&t, dt_val(tc, 0, 86399, 200000, 0, 86399), 0, 86399) :
			pg_prop_draw_int(tc, 0, 86399);

		dt_sep(tc, &t, false);
		appendStringInfo(&t.fmt, "%sSSSSS", t.fm ? "FM" : "");
		appendStringInfo(&t.in, "%lld", (long long) sec);
		appendStringInfo(&t.exp, "%lld", (long long) sec);
	}

	conn = cfb3_connect(tc);
	cfb3_exec(tc, conn, "SET timezone = 'UTC'");
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT to_char(%s('%s', '%s'), '%s')",
					 ts ? "to_timestamp" : "to_date",
					 t.in.data, t.fmt.data, t.fmt.data);
	got = pg_prop_sql_scalar(conn, q.data, &failed, &err);
	if (failed)
	{
		/* rejecting an out-of-domain input is allowed; nothing else is */
		if (t.valid || cfb3_internal_error(err))
			pg_prop_fail(tc, "%s rejected a valid input: %s\n  SQL: %s",
						 ts ? "to_timestamp" : "to_date", err ? err : "",
						 q.data);
	}
	else if (got == NULL || strcmp(got, t.exp.data) != 0)
		pg_prop_fail(tc,
					 "%s accepted an input that does not round-trip\n"
					 "  input:  '%s'\n  format: '%s'\n"
					 "  to_char of the result: '%s'\n"
					 "  expected:              '%s'\n  SQL: %s",
					 ts ? "to_timestamp" : "to_date", t.in.data, t.fmt.data,
					 got ? got : "(null)", t.exp.data, q.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7411: DDD / IDDD / IW / ID / WW / RM / SSSSS out of their domains
 * (0, 4+ digits, negatives, "XIII") must be rejected, not reinterpreted.
 */
PG_PROP(to_date_field_range_roundtrip)
{
	dt_roundtrip(tc, DT_RANGE);
}

/*
 * CF 7416: a Y,YYY millennium outside int must be rejected, not truncated
 * mod 2^32 (4294969320,024 -> year 2024024).
 */
PG_PROP(to_date_y_yyy_roundtrip)
{
	dt_roundtrip(tc, DT_Y_YYY);
}

/*
 * CF 6792 (tests-only patch): zero month / zero day are "unset" and read
 * as 1; every other MM/DD round-trips or is rejected.  Pins down existing
 * behaviour, so master is expected to pass.
 */
PG_PROP(to_date_zero_month_day)
{
	dt_roundtrip(tc, DT_ZERO_MD);
}

/* ----------------------------------------------------------------
 * datetime format cache: std vs lenient mode   (CF 7409)
 * ----------------------------------------------------------------
 */

#define FC_MAX_FMTS 48
#define FC_MAX_STEPS 120

/* format fields, separators legal in SQL/JSON std mode, and others */
static const char *const fc_fields[] = {"HH24", "MI", "SS", "YYYY", "MM", "DD"};
static const char *const fc_std_seps[] = {"-", ":", " ", ".", "/", ","};
static const char *const fc_lax_seps[] = {"z", "#", "@", "_", "!", "%"};

/* append s escaped for a SQL literal ('') */
static void
fc_sql_lit(StringInfo b, const char *s)
{
	appendStringInfoChar(b, '\'');
	for (; *s; s++)
	{
		if (*s == '\'')
			appendStringInfoChar(b, '\'');
		appendStringInfoChar(b, *s);
	}
	appendStringInfoChar(b, '\'');
}

/* append s as a double-quoted JSON / jsonpath string body */
static void
fc_json_str(StringInfo b, const char *s)
{
	appendStringInfoChar(b, '"');
	for (; *s; s++)
	{
		if (*s == '"' || *s == '\\')
			appendStringInfoChar(b, '\\');
		appendStringInfoChar(b, *s);
	}
	appendStringInfoChar(b, '"');
}

/* run one call, rendering the outcome (value or first error line) */
static char *
fc_outcome(pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	StringInfoData r;

	initStringInfo(&r);
	if (failed)
	{
		size_t		n = err ? strcspn(err, "\n") : 0;

		appendStringInfo(&r, "ERR %.*s", (int) n, err ? err : "");
		if (cfb3_internal_error(err))
			appendStringInfoString(&r, " [internal]");
	}
	else
		appendStringInfo(&r, "OK %s", v ? v : "(null)");
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	return r.data;
}

/*
 * datetime_format_cache_mode_independent  (CF 7409)
 *
 * The parsed-format cache (DCHCache, 20 entries, LRU) is keyed on the
 * format text and on whether it was parsed in SQL/JSON standard mode
 * (jsonpath .datetime()) or lenient mode (to_char / to_timestamp).
 * Draw a pool of up to 48 formats -- fields with separators, some legal
 * only in lenient mode ("HH24zMI"), some made distinct by a quoted
 * literal -- and a schedule of phases, each a run of calls in one mode
 * over sequential or random formats, so the cache fills, evicts and
 * recycles entries across modes.
 *
 * Invariant: each call's outcome (value or error message) is a function
 * of its arguments only.  The reference runs the same calls in two fresh
 * sessions, one per mode: a session that only ever parses in one mode
 * cannot reuse an entry across modes, so it gives the answer a correct
 * cache must reproduce in the mixed session.  On master a recycled entry
 * keeps its previous owner's mode flag, so .datetime("HH24zMI") hits a
 * lenient tree and accepts "12z34" instead of rejecting the separator.
 */
PG_PROP(datetime_format_cache_mode_independent)
{
	char	   *fmts[FC_MAX_FMTS];
	char	   *rendered[FC_MAX_FMTS];
	int			nfmt;
	int			step_fmt[FC_MAX_STEPS];
	bool		step_std[FC_MAX_STEPS];
	char	   *step_ref[FC_MAX_STEPS];
	int			nstep = 0;
	int			nphase;
	int			hh,
				mi;
	pg_prop_conn *lax;
	pg_prop_conn *std;
	pg_prop_conn *mixed;
	StringInfoData q;
	char		ts[64];

	nfmt = (int) pg_prop_draw_int(tc, 1, FC_MAX_FMTS);
	for (int i = 0; i < nfmt; i++)
	{
		StringInfoData f;
		int			nf = (int) pg_prop_draw_int(tc, 1, 3);

		initStringInfo(&f);
		if (pg_prop_draw_bool(tc))
			appendStringInfo(&f, "\"x%d\"", i);
		for (int k = 0; k < nf; k++)
		{
			if (k > 0)
			{
				if (pg_prop_draw_int(tc, 0, 2) == 0)
					appendStringInfoString(&f, fc_lax_seps[pg_prop_draw_int(tc, 0, lengthof(fc_lax_seps) - 1)]);
				else
					appendStringInfoString(&f, fc_std_seps[pg_prop_draw_int(tc, 0, lengthof(fc_std_seps) - 1)]);
			}
			appendStringInfoString(&f, fc_fields[pg_prop_draw_int(tc, 0, lengthof(fc_fields) - 1)]);
		}
		fmts[i] = f.data;
	}

	/* phases: a run of one mode over sequential (fresh) or random formats */
	nphase = (int) pg_prop_draw_int(tc, 1, 5);
	for (int p = 0; p < nphase && nstep < FC_MAX_STEPS; p++)
	{
		bool		is_std = pg_prop_draw_bool(tc);
		bool		seq = pg_prop_draw_bool(tc);
		int			start = (int) pg_prop_draw_int(tc, 0, nfmt - 1);
		int			len = (int) pg_prop_draw_int(tc, 1, 40);

		for (int k = 0; k < len && nstep < FC_MAX_STEPS; k++)
		{
			step_std[nstep] = is_std;
			step_fmt[nstep] = seq ? (start + k) % nfmt :
				(int) pg_prop_draw_int(tc, 0, nfmt - 1);
			nstep++;
		}
	}

	hh = (int) pg_prop_draw_int(tc, 0, 23);
	mi = (int) pg_prop_draw_int(tc, 0, 59);
	snprintf(ts, sizeof(ts), "timestamp '2024-03-04 %02d:%02d:07'", hh, mi);

	lax = cfb3_connect(tc);
	std = cfb3_connect(tc);
	mixed = cfb3_connect(tc);
	initStringInfo(&q);

	/* what each format renders to, so std mode has an input to parse */
	for (int i = 0; i < nfmt; i++)
	{
		bool		failed = false;
		char	   *err = NULL;

		resetStringInfo(&q);
		appendStringInfo(&q, "SELECT to_char(%s, ", ts);
		fc_sql_lit(&q, fmts[i]);
		appendStringInfoChar(&q, ')');
		rendered[i] = pg_prop_sql_scalar(lax, q.data, &failed, &err);
		if (failed || rendered[i] == NULL)
			pg_prop_fail(tc, "to_char failed: %s\n  SQL: %s",
						 err ? err : "", q.data);
	}

	/* reference outcomes from the single-mode sessions */
	for (int s = 0; s < nstep; s++)
	{
		int			i = step_fmt[s];
		StringInfoData j;

		initStringInfo(&j);
		resetStringInfo(&q);
		if (step_std[s])
		{
			fc_json_str(&j, rendered[i]);
			appendStringInfoString(&q, "SELECT jsonb_path_query_first(");
			fc_sql_lit(&q, j.data);
			resetStringInfo(&j);
			appendStringInfoString(&j, "$.datetime(");
			fc_json_str(&j, fmts[i]);
			appendStringInfoChar(&j, ')');
			appendStringInfoString(&q, "::jsonb, ");
			fc_sql_lit(&q, j.data);
			appendStringInfoString(&q, "::jsonpath)::text");
		}
		else
		{
			/* lenient parse and lenient render, both through the cache */
			appendStringInfoString(&q, "SELECT to_timestamp(");
			fc_sql_lit(&q, rendered[i]);
			appendStringInfoString(&q, ", ");
			fc_sql_lit(&q, fmts[i]);
			appendStringInfoString(&q, ")::text || ' ' || to_char(");
			appendStringInfo(&q, "%s, ", ts);
			fc_sql_lit(&q, fmts[i]);
			appendStringInfoChar(&q, ')');
		}
		pfree(j.data);
		step_ref[s] = pg_strdup(q.data);
	}
	cfb3_exec(tc, lax, "SET timezone = 'UTC'");
	cfb3_exec(tc, std, "SET timezone = 'UTC'");
	cfb3_exec(tc, mixed, "SET timezone = 'UTC'");
	for (int s = 0; s < nstep; s++)
	{
		char	   *want = fc_outcome(step_std[s] ? std : lax, step_ref[s]);
		char	   *got = fc_outcome(mixed, step_ref[s]);

		if (strcmp(want, got) != 0 || strstr(got, "[internal]") != NULL)
		{
			StringInfoData hist;

			initStringInfo(&hist);
			for (int k = Max(0, s - 40); k <= s; k++)
				appendStringInfo(&hist, " %s:%d", step_std[k] ? "std" : "lax",
								 step_fmt[k]);
			pg_prop_fail(tc,
						 "call %d gives a different outcome after %d cached "
						 "format calls in mixed modes\n"
						 "  SQL: %s\n  mixed session:      %s\n"
						 "  single-mode session: %s\n"
						 "  %d formats, last steps (mode:format#):%s",
						 s, s, step_ref[s], got, want, nfmt, hist.data);
		}
		pg_free(want);
		pg_free(got);
	}
	pg_prop_disconnect(mixed);
	pg_prop_disconnect(std);
	pg_prop_disconnect(lax);
}

/* ----------------------------------------------------------------
 * date_trunc on timestamptz near time-zone transitions   (CF 7243)
 * ----------------------------------------------------------------
 */

/* zones with offset changes that are not whole hours, or not at hh:00 */
static const struct
{
	const char *zone;
	int			year;
}			tz_odd[] = {
	{"Europe/Athens", 1916},	/* +1:34:52 -> +2 at 00:01 local */
	{"Europe/Dublin", 1916},	/* -0:25:21 -> +0:34:39 */
	{"Europe/Amsterdam", 1937}, /* +0:19:32 -> +0:20 */
	{"Europe/Moscow", 1919},
	{"Africa/Monrovia", 1972},	/* -0:44:30 -> 0 */
	{"Asia/Kathmandu", 1986},	/* +5:30 -> +5:45 */
	{"Asia/Singapore", 1981},	/* +7:30 -> +8 */
	{"Asia/Colombo", 1996},
	{"Asia/Pyongyang", 2015},	/* +9 -> +8:30 */
	{"Asia/Pyongyang", 2018},
	{"America/Caracas", 2007},	/* -4 -> -4:30 */
	{"America/Caracas", 2016},
	{"Australia/Lord_Howe", 2020},	/* 30-minute DST */
	{"Australia/Eucla", 2008},
	{"Pacific/Chatham", 2020},	/* transitions at 2:45 local */
	{"America/St_Johns", 2020},
	{"America/New_York", 2020}, /* hour-sized, at hh:00: control */
};

static const char *const trunc_units[] =
{"minute", "hour", "day", "week", "month", "quarter", "year", "decade", "second"};

/*
 * date_trunc_tz_wall_clock  (CF 7243)
 *
 * Pick a zone and a year (curated zones with odd transitions, or any zone
 * in pg_timezone_names and any year), locate its k-th offset change in
 * that year to the second, and draw t at a microsecond distance from it
 * (within a minute, a few hours, or two days) and a unit; sometimes use
 * the session time zone (2-argument form) instead of the explicit zone.
 *
 * Oracle: date_trunc(u, t, z) is the instant whose wall-clock time in z
 * is W = date_trunc(u, t AT TIME ZONE z), timestamp truncation being pure
 * calendar arithmetic.  The property collects every instant c with
 * c AT TIME ZONE z = W (W - o for each UTC offset o z uses within 16
 * hours, kept if it maps back to W) and, when exactly one exists (W is
 * neither skipped nor repeated by a transition), requires date_trunc to
 * return it.  Ambiguous or nonexistent wall times are left to the
 * server's resolution rule and not checked.  On master, minute and hour
 * truncation keep t's own offset even when W lies across a transition.
 */
PG_PROP(date_trunc_tz_wall_clock)
{
	pg_prop_conn *conn;
	StringInfoData q;
	StringInfoData zexpr;
	char	   *bad;
	const char *unit;
	int64		year;
	int64		k = pg_prop_draw_int(tc, 0, 3);
	int64		span;
	int64		off;
	bool		session_tz = pg_prop_draw_int(tc, 0, 3) == 0;

	/* sub-day units are where an offset change lands inside the period */
	if (pg_prop_draw_bool(tc))
		unit = pg_prop_draw_bool(tc) ? "minute" : "hour";
	else
		unit = trunc_units[pg_prop_draw_int(tc, 0, lengthof(trunc_units) - 1)];
	/* microseconds from the transition instant */
	switch (pg_prop_draw_int(tc, 0, 2))
	{
		case 0:
			span = INT64CONST(120000000);
			break;
		case 1:
			span = INT64CONST(3) * INT64CONST(3600000000);
			break;
		default:
			span = INT64CONST(48) * INT64CONST(3600000000);
			break;
	}
	off = pg_prop_draw_int(tc, -span, span);

	initStringInfo(&zexpr);
	if (pg_prop_draw_int(tc, 0, 3) > 0)
	{
		int			i = (int) pg_prop_draw_int(tc, 0, lengthof(tz_odd) - 1);

		appendStringInfo(&zexpr, "'%s'::text", tz_odd[i].zone);
		year = tz_odd[i].year + pg_prop_draw_int(tc, -1, 1) *
			(pg_prop_draw_int(tc, 0, 2) == 0);
	}
	else
	{
		appendStringInfo(&zexpr,
						 "(SELECT name FROM pg_timezone_names ORDER BY name "
						 "OFFSET %lld %% (SELECT count(*) FROM pg_timezone_names) "
						 "LIMIT 1)",
						 (long long) pg_prop_draw_int(tc, 0, 100000));
		year = pg_prop_draw_int(tc, 1880, 2037);
	}

	conn = cfb3_connect(tc);
	cfb3_exec(tc, conn, "SET timezone = 'UTC'");
	initStringInfo(&q);
	appendStringInfo(&q,
					 "WITH zz AS (SELECT %s AS z), "
					 "tr AS (SELECT g, row_number() OVER (ORDER BY g) - 1 AS rn, "
					 "              count(*) OVER () AS n "
					 "       FROM zz, generate_series(make_timestamptz(%lld,1,1,0,0,0,'UTC'), "
					 "                                make_timestamptz(%lld,1,1,0,0,0,'UTC'), "
					 "                                interval '1 hour') g "
					 "       WHERE (g AT TIME ZONE z) - (g AT TIME ZONE 'UTC') <> "
					 "             ((g + interval '1 hour') AT TIME ZONE z) - "
					 "             ((g + interval '1 hour') AT TIME ZONE 'UTC')), "
					 "hr AS (SELECT coalesce((SELECT g FROM tr WHERE rn = %lld %% n), "
					 "                       make_timestamptz(%lld,7,1,0,0,0,'UTC')) AS g), "
					 /* the first second of hour g at a new offset */
					 "base AS (SELECT coalesce(min(s), g) AS g "
					 "         FROM zz, hr, generate_series(g, g + interval '1 hour', "
					 "                                      interval '1 second') s "
					 "         WHERE (s AT TIME ZONE z) - (s AT TIME ZONE 'UTC') <> "
					 "               (g AT TIME ZONE z) - (g AT TIME ZONE 'UTC') "
					 "         GROUP BY g), "
					 "pt AS (SELECT z, g + interval '%lld microseconds' AS t FROM zz, base), "
					 "w AS (SELECT z, t, date_trunc('%s', t AT TIME ZONE z) AS w FROM pt), "
					 "x AS (SELECT w.*, %s AS d FROM w), "
					 "offs AS (SELECT DISTINCT (s AT TIME ZONE z) - (s AT TIME ZONE 'UTC') AS o "
					 "         FROM w, generate_series((w AT TIME ZONE 'UTC') - interval '16 hours', "
					 "                                 (w AT TIME ZONE 'UTC') + interval '16 hours', "
					 "                                 interval '10 minutes') s "
					 "         UNION SELECT (t AT TIME ZONE z) - (t AT TIME ZONE 'UTC') FROM pt "
					 "         UNION SELECT (d AT TIME ZONE z) - (d AT TIME ZONE 'UTC') FROM x), "
					 "c AS (SELECT DISTINCT (w - o) AT TIME ZONE 'UTC' AS c FROM w, offs "
					 "      WHERE ((w - o) AT TIME ZONE 'UTC') AT TIME ZONE z = w) "
					 "SELECT format('zone %%s, t = %%s (%%s local), wall-clock trunc %%s = %%s, "
					 "date_trunc gave %%s (%%s local)', "
					 "z, t, t AT TIME ZONE z, '%s', w, d, d AT TIME ZONE z) "
					 "FROM x "
					 "WHERE (SELECT count(*) FROM c) = 1 AND d <> (SELECT c FROM c)",
					 zexpr.data, (long long) year, (long long) year + 1,
					 (long long) k, (long long) year, (long long) off,
					 unit,
					 session_tz ?
					 "pg_temp.cfb3_trunc_session(t, z)" :
					 psprintf("date_trunc('%s', t, z)", unit),
					 unit);
	if (session_tz)
	{
		/* the 2-argument form, under SET timezone = z */
		StringInfoData f;

		initStringInfo(&f);
		appendStringInfo(&f,
						 "CREATE FUNCTION pg_temp.cfb3_trunc_session(t timestamptz, z text) "
						 "RETURNS timestamptz LANGUAGE plpgsql AS $$ "
						 "DECLARE r timestamptz; BEGIN "
						 "PERFORM set_config('timezone', z, true); "
						 "r := date_trunc('%s', t); "
						 "PERFORM set_config('timezone', 'UTC', true); "
						 "RETURN r; END $$", unit);
		cfb3_exec(tc, conn, f.data);
		pfree(f.data);
	}
	bad = cfb3_scalar(tc, conn, q.data);
	/* no SQL in the message: pg_prop_fail drops messages over ~1 kB */
	if (bad != NULL)
		pg_prop_fail(tc, "date_trunc disagrees with the wall-clock truncation (%s form)\n  %s",
					 session_tz ? "session time zone, 2-argument" : "explicit zone",
					 bad);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------
 * time / timetz +/- interval   (CF 7349)
 * ----------------------------------------------------------------
 */

#define CFB3_USECS_PER_DAY INT64CONST(86400000000)

/* render a time-of-day in [0, USECS_PER_DAY) as HH:MM:SS.ffffff */
static void
cfb3_tod(StringInfo b, int64 us)
{
	appendStringInfo(b, "%02d:%02d:%02d.%06d",
					 (int) (us / INT64CONST(3600000000)),
					 (int) (us / 60000000 % 60),
					 (int) (us / 1000000 % 60),
					 (int) (us % 1000000));
}

/*
 * time_interval_arith_mod_day  (CF 7349)
 *
 * Draw a time (or timetz with a drawn zone offset), and an interval with
 * random months and days and a time field anywhere in int64 -- biased to
 * its extremes, where t + span->time overflows -- and an operator (+/-).
 *
 * Model (C, exact): time +/- interval ignores months and days and wraps
 * modulo one day, so the result is (t +/- (span mod 86400000000)) mod
 * 86400000000, computed here without overflow; timetz keeps its zone.
 * Also cross-checked: (t + i) - i = t.  Both hold for every interval on
 * a correct server; on master the int64 sum overflows for large time
 * fields and returns a wrong time of day.
 */
PG_PROP(time_interval_arith_mod_day)
{
	pg_prop_conn *conn;
	int64		t = pg_prop_draw_int(tc, 0, CFB3_USECS_PER_DAY - 1);
	int64		span;
	int64		mons = pg_prop_draw_int(tc, -1000, 1000);
	int64		days = pg_prop_draw_int(tc, -100000, 100000);
	bool		minus = pg_prop_draw_bool(tc);
	bool		tz = pg_prop_draw_bool(tc);
	int64		tzmin = pg_prop_draw_int(tc, -15 * 60, 15 * 60);
	int64		r;
	StringInfoData lit;
	StringInfoData exp;
	StringInfoData q;
	char	   *got;

	switch (pg_prop_draw_int(tc, 0, 3))
	{
		case 0:
			span = pg_prop_draw_int(tc, PG_INT64_MAX - CFB3_USECS_PER_DAY,
									PG_INT64_MAX);
			break;
		case 1:
			span = pg_prop_draw_int(tc, PG_INT64_MIN,
									PG_INT64_MIN + CFB3_USECS_PER_DAY);
			break;
		case 2:
			span = pg_prop_draw_int(tc, -10 * CFB3_USECS_PER_DAY,
									10 * CFB3_USECS_PER_DAY);
			break;
		default:
			span = pg_prop_draw_int(tc, PG_INT64_MIN, PG_INT64_MAX);
			break;
	}
	/* -9223372036854775808 has no positive counterpart to subtract */
	r = span % CFB3_USECS_PER_DAY;	/* in (-day, day), no overflow */
	r = minus ? t - r : t + r;		/* in (-day, 2 day) */
	r %= CFB3_USECS_PER_DAY;
	if (r < 0)
		r += CFB3_USECS_PER_DAY;

	initStringInfo(&lit);
	appendStringInfo(&lit, "%s '", tz ? "timetz" : "time");
	cfb3_tod(&lit, t);
	if (tz)
		appendStringInfo(&lit, "%c%02d:%02d", tzmin < 0 ? '-' : '+',
						 (int) (llabs(tzmin) / 60), (int) (llabs(tzmin) % 60));
	appendStringInfoChar(&lit, '\'');

	initStringInfo(&exp);
	cfb3_tod(&exp, r);

	conn = cfb3_connect(tc);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN to_char(x::time, 'HH24:MI:SS.US') = '%s' "
					 "%s THEN NULL ELSE x::text END "
					 "FROM (SELECT %s %c (make_interval(months => %lld, days => %lld) "
					 "             + interval '%lld microseconds') AS x) s",
					 exp.data,
					 tz ? psprintf("AND x::text LIKE '%%%c%02d%s'",
								   tzmin < 0 ? '-' : '+',
								   (int) (llabs(tzmin) / 60),
								   llabs(tzmin) % 60 ?
								   psprintf(":%02d", (int) (llabs(tzmin) % 60)) : "") : "",
					 lit.data, minus ? '-' : '+',
					 (long long) mons, (long long) days, (long long) span);
	got = cfb3_scalar(tc, conn, q.data);
	if (got != NULL)
		pg_prop_fail(tc, "%s %c interval (months %lld, days %lld, time %lld us) "
					 "= %s, expected %s (time of day mod 24h)",
					 lit.data, minus ? '-' : '+', (long long) mons,
					 (long long) days, (long long) span, got, exp.data);

	/* (t + i) - i = t */
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN (%s + i) - i = %s THEN NULL "
					 "ELSE ((%s + i) - i)::text END "
					 "FROM (SELECT interval '%lld microseconds' AS i) s",
					 lit.data, lit.data, lit.data, (long long) span);
	got = cfb3_scalar(tc, conn, q.data);
	if (got != NULL)
		pg_prop_fail(tc, "(%s + i) - i = %s, i = %lld us", lit.data, got,
					 (long long) span);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------
 * SQL/JSON DEFAULT ... ON EMPTY / ON ERROR   (CF 7181, 7355, 7414)
 * ----------------------------------------------------------------
 */

static const char *const sj_types[] =
{"int", "bigint", "smallint", "text", "varchar", "bool", "numeric",
"pg_temp.cfb3_dnz", "pg_temp.cfb3_dnn"};

static const char *const sj_setup =
"CREATE DOMAIN pg_temp.cfb3_dnz AS int CHECK (VALUE <> 0); "
"CREATE DOMAIN pg_temp.cfb3_dnn AS int NOT NULL";

/* the outcome of a scalar query: "V:<text>", "N" (NULL) or "E" (error) */
static char *
sj_outcome(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	char	   *r;

	if (failed && cfb3_internal_error(err))
		pg_prop_fail(tc, "internal error: %.300s\n  SQL: %.500s", err, sql);
	if (failed)
		r = pg_strdup("E");
	else if (v == NULL)
		r = pg_strdup("N");
	else
		r = psprintf("V:%s", v);
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	return r;
}

/*
 * An int-valued DEFAULT sub-expression: constant, volatile, or (unless
 * "safe") one that may raise a cast, domain or division error.
 */
static void
sj_int_expr(pg_prop_tc *tc, StringInfo b, int depth, bool safe)
{
	int64		k = pg_prop_draw_int(tc, -3, 3);
	int64		c = pg_prop_draw_int(tc, 0, depth > 0 ? 8 : 4);

	if (safe && c >= 2 && c <= 5)
		c = (c == 4) ? 4 : 1;
	switch (c)
	{
		case 0:
			appendStringInfo(b, "(%lld)", (long long) k);
			break;
		case 1:
			appendStringInfo(b, "((random() * 0)::int + %lld)", (long long) k);
			break;
		case 2:
			/* a soft error from a cast inside the expression (CF 7355) */
			appendStringInfo(b, "(('a' || (random() * 0)::int)::int + %lld)",
							 (long long) k);
			break;
		case 3:
			/* a domain check (k = 0 fails) */
			appendStringInfo(b, "(%lld::pg_temp.cfb3_dnz + 0)", (long long) k);
			break;
		case 4:
			appendStringInfoString(b, !safe && pg_prop_draw_bool(tc) ?
								   "(NULL::pg_temp.cfb3_dnn + 0)" : "NULL::int");
			break;
		case 5:
			appendStringInfo(b, "(1 / ((random() * 0)::int + %lld))",
							 (long long) k);
			break;
		case 6:
			appendStringInfoString(b, "abs");
			appendStringInfoChar(b, '(');
			sj_int_expr(tc, b, depth - 1, safe);
			appendStringInfoChar(b, ')');
			break;
		default:
			appendStringInfoChar(b, '(');
			sj_int_expr(tc, b, depth - 1, safe);
			appendStringInfoString(b, pg_prop_draw_bool(tc) ? " + " : " * ");
			sj_int_expr(tc, b, depth - 1, safe);
			appendStringInfoChar(b, ')');
			break;
	}
}

/*
 * A DEFAULT expression.  "boolean": a boolean-valued one, literal or
 * computed, constant or volatile, built from error-free operands (CF
 * 7181).  Otherwise: an int or text one whose evaluation may raise an
 * error somewhere below its top node (CF 7355).
 */
static void
sj_default_expr(pg_prop_tc *tc, StringInfo b, bool boolean)
{
	if (boolean)
	{
		switch (pg_prop_draw_int(tc, 0, 3))
		{
			case 0:
				appendStringInfoString(b, pg_prop_draw_bool(tc) ? "true" : "false");
				break;
			case 1:
				appendStringInfoString(b, pg_prop_draw_bool(tc) ? "(random() < 2)" :
									   "(1 = 1)");
				break;
			default:
				appendStringInfoChar(b, '(');
				sj_int_expr(tc, b, 1, true);
				appendStringInfoString(b, pg_prop_draw_bool(tc) ? " < " : " = ");
				sj_int_expr(tc, b, 1, true);
				appendStringInfoChar(b, ')');
				break;
		}
		return;
	}
	switch (pg_prop_draw_int(tc, 0, 3))
	{
		case 0:
			appendStringInfo(b, "'%lld'", (long long) pg_prop_draw_int(tc, -2, 99));
			break;
		case 1:
			appendStringInfoString(b, "('x' || ");
			sj_int_expr(tc, b, 1, false);
			appendStringInfoChar(b, ')');
			break;
		default:
			sj_int_expr(tc, b, 2, false);
			break;
	}
}

/*
 * Draw a JSON_VALUE / JSON_QUERY / JSON_TABLE column, a RETURNING type
 * (including domains with NOT NULL and CHECK) and a DEFAULT expression,
 * with a document/path for which the DEFAULT is certainly taken: ON EMPTY
 * with a lax path to a missing key, or ON ERROR with a strict one.
 *
 * Oracle: the result then is the DEFAULT expression coerced to the
 * RETURNING type, so its outcome (value, NULL, or error) must equal that
 * of SELECT CAST((e) AS type) evaluated on its own.
 */
static void
sj_default_is_cast(pg_prop_tc *tc, bool boolean)
{
	pg_prop_conn *conn;
	const char *typ = sj_types[pg_prop_draw_int(tc, 0, lengthof(sj_types) - 1)];
	int			fn = (int) pg_prop_draw_int(tc, 0, 2);
	bool		on_error = pg_prop_draw_bool(tc);
	const char *quotes = "";
	StringInfoData e;
	StringInfoData doc;
	StringInfoData q;
	StringInfoData m;
	char	   *got;
	char	   *want;

	initStringInfo(&e);
	sj_default_expr(tc, &e, boolean);
	if (fn > 0)
	{
		switch (pg_prop_draw_int(tc, 0, 2))
		{
			case 1:
				quotes = " KEEP QUOTES";
				break;
			case 2:
				quotes = " OMIT QUOTES";
				break;
		}
	}
	/* a document without key "a" */
	initStringInfo(&doc);
	if (pg_prop_draw_bool(tc))
		appendStringInfoString(&doc, "{}");
	else
		appendStringInfo(&doc, "{\"b\": %lld}", (long long) pg_prop_draw_int(tc, 0, 9));

	initStringInfo(&q);
	if (fn < 2)
		appendStringInfo(&q,
						 "SELECT JSON_%s('%s'::jsonb, '%s $.a' RETURNING %s%s "
						 "DEFAULT %s ON %s)::text",
						 fn == 0 ? "VALUE" : "QUERY", doc.data,
						 on_error ? "strict" : "lax", typ, quotes, e.data,
						 on_error ? "ERROR" : "EMPTY");
	else
		appendStringInfo(&q,
						 "SELECT c::text FROM JSON_TABLE('%s'::jsonb, '$' "
						 "COLUMNS (c %s PATH '%s $.a'%s DEFAULT %s ON %s))",
						 doc.data, typ, on_error ? "strict" : "lax", quotes,
						 e.data, on_error ? "ERROR" : "EMPTY");
	/*
	 * The reference: the DEFAULT converted to the RETURNING type.  For a
	 * boolean DEFAULT and a type with a cast from boolean that is the cast;
	 * for one without (bigint, smallint, numeric and the int-based domains)
	 * the parser converts through jsonb, so the reference is the JSON
	 * boolean's conversion -- an error for a number type -- or NULL if the
	 * expression is NULL.  (A CAST cannot be written for those: the parser
	 * rejects it even in an untaken CASE branch.)  Either way the value the
	 * expression evaluates to decides the outcome, never a constant
	 * 'false'.
	 */
	conn = cfb3_connect(tc);
	cfb3_exec(tc, conn, sj_setup);
	initStringInfo(&m);
	if (boolean)
	{
		/* does the type (or its domain base) have a cast from boolean? */
		char	   *castq = psprintf("SELECT EXISTS (SELECT 1 FROM pg_cast WHERE castsource = 'bool'::regtype "
									 "AND casttarget = (SELECT CASE typtype WHEN 'd' THEN typbasetype ELSE oid END "
									 "FROM pg_type WHERE oid = '%s'::regtype)) "
									 "OR '%s'::regtype IN ('text'::regtype, 'varchar'::regtype)", typ, typ);
		bool		f = false;
		char	   *has = pg_prop_sql_scalar(conn, castq, &f, NULL);

		if (has && strcmp(has, "t") != 0)
			appendStringInfo(&m, "SELECT CASE WHEN (%s) IS NULL THEN NULL "
							 "ELSE JSON_VALUE(to_jsonb(%s), '$' RETURNING %s ERROR ON ERROR) END::text",
							 e.data, e.data, typ);
		if (has)
			pg_free(has);
		pfree(castq);
	}
	if (m.len == 0)
		appendStringInfo(&m, "SELECT CAST((%s) AS %s)::text", e.data, typ);

	got = sj_outcome(tc, conn, q.data);
	want = sj_outcome(tc, conn, m.data);
	if (strcmp(got, want) != 0)
		pg_prop_fail(tc, "DEFAULT ... ON %s is not the DEFAULT cast to %s\n"
					 "  %s  ->  %s\n  %s  ->  %s",
					 on_error ? "ERROR" : "EMPTY", typ, q.data, got,
					 m.data, want);
	pg_prop_disconnect(conn);
}

/*
 * sqljson_bool_default_is_cast  (CF 7181)
 *
 * Boolean DEFAULT expressions.  On master, for a non-integer RETURNING
 * type the parser replaces any boolean DEFAULT by the jsonb constant
 * 'false', whatever it evaluates to.
 */
PG_PROP(sqljson_bool_default_is_cast)
{
	sj_default_is_cast(tc, true);
}

/*
 * sqljson_default_error_is_cast  (CF 7355)
 *
 * DEFAULT expressions that may raise an error below their top node (a
 * cast, a domain check, a division).  On master the error is evaluated
 * in soft mode and silently becomes NULL unless the top node is itself a
 * coercion.
 */
PG_PROP(sqljson_default_error_is_cast)
{
	sj_default_is_cast(tc, false);
}

/*
 * sqljson_rows_independent  (CF 7414)
 *
 * Draw 2..8 rows (doc, path), each doc and path possibly SQL NULL, the
 * docs chosen to make the path empty, raise a strict-mode error, or
 * return a value; and one of JSON_VALUE / JSON_QUERY ... DEFAULT ON
 * EMPTY / ON ERROR or JSON_EXISTS ... TRUE ON ERROR.  Evaluate it over
 * all rows in one query.
 *
 * Oracle: a row's result depends on that row only, so each must equal
 * the same call evaluated alone on literals.  On master a row whose
 * context or path is SQL NULL inherits the previous row's ON EMPTY /
 * ON ERROR outcome instead of returning NULL.
 */
PG_PROP(sqljson_rows_independent)
{
	static const char *const docs[] =
	{"{}", "1", "{\"a\": 5}", "{\"a\": \"x\"}", "{\"b\": 2}", NULL};
	static const char *const paths[] = {"$.a", "strict $.a", "lax $.a", NULL};
	pg_prop_conn *conn;
	int			nrows = (int) pg_prop_draw_int(tc, 2, 8);
	int			fn = (int) pg_prop_draw_int(tc, 0, 2);
	bool		vol = pg_prop_draw_bool(tc);
	bool		path_col = pg_prop_draw_bool(tc);
	const char *rd[8];
	const char *rp[8];
	char		tail[200];
	StringInfoData q;
	StringInfoData one;
	char	   *all;
	char	   *expect;
	StringInfoData exp;
	bool		failed = false;
	char	   *err = NULL;

	if (fn == 2)
		snprintf(tail, sizeof(tail), "TRUE ON ERROR");
	else
		snprintf(tail, sizeof(tail), "%s DEFAULT %s ON EMPTY DEFAULT %s ON ERROR",
				 fn == 0 ? "RETURNING int" : "RETURNING text",
				 vol ? "((random() * 0)::int + 42)" : "42",
				 vol ? "((random() * 0)::int + 7)" : "7");
	if (fn == 1)
		snprintf(tail, sizeof(tail), "RETURNING text DEFAULT %s ON EMPTY DEFAULT %s ON ERROR",
				 vol ? "('e' || (random() * 0)::int)" : "'\"e\"'",
				 vol ? "('r' || (random() * 0)::int)" : "'\"r\"'");

	for (int i = 0; i < nrows; i++)
	{
		rd[i] = docs[pg_prop_draw_int(tc, 0, lengthof(docs) - 1)];
		rp[i] = path_col ? paths[pg_prop_draw_int(tc, 0, lengthof(paths) - 1)] :
			"strict $.a";
	}

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT string_agg(coalesce(r, 'NULL'), '|' ORDER BY i) FROM ("
					 "SELECT i, JSON_%s(x, %s %s)::text AS r FROM (VALUES ",
					 fn == 0 ? "VALUE" : fn == 1 ? "QUERY" : "EXISTS",
					 path_col ? "p" : "'strict $.a'", tail);
	for (int i = 0; i < nrows; i++)
	{
		appendStringInfo(&q, "%s(%d, ", i ? ", " : "", i);
		if (rd[i])
			appendStringInfo(&q, "'%s'::jsonb, ", rd[i]);
		else
			appendStringInfoString(&q, "NULL::jsonb, ");
		if (rp[i])
			appendStringInfo(&q, "'%s'::jsonpath)", rp[i]);
		else
			appendStringInfoString(&q, "NULL::jsonpath)");
	}
	appendStringInfoString(&q, ") v(i, x, p)) s");

	conn = cfb3_connect(tc);
	all = pg_prop_sql_scalar(conn, q.data, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "query failed: %.300s\n  SQL: %.600s", err ? err : "", q.data);

	initStringInfo(&exp);
	initStringInfo(&one);
	for (int i = 0; i < nrows; i++)
	{
		resetStringInfo(&one);
		appendStringInfo(&one, "SELECT coalesce(JSON_%s(%s%s%s, %s%s%s %s)::text, 'NULL')",
						 fn == 0 ? "VALUE" : fn == 1 ? "QUERY" : "EXISTS",
						 rd[i] ? "'" : "NULL::jsonb", rd[i] ? rd[i] : "",
						 rd[i] ? "'::jsonb" : "",
						 rp[i] ? "'" : "NULL::jsonpath", rp[i] ? rp[i] : "",
						 rp[i] ? "'::jsonpath" : "", tail);
		expect = pg_prop_sql_scalar(conn, one.data, &failed, &err);
		if (failed)
			pg_prop_fail(tc, "single-row query failed: %.300s\n  SQL: %.600s",
						 err ? err : "", one.data);
		appendStringInfo(&exp, "%s%s", i ? "|" : "", expect);
		pg_free(expect);
	}
	if (all == NULL || strcmp(all, exp.data) != 0)
		pg_prop_fail(tc, "per-row results depend on other rows\n"
					 "  together: %s\n  alone:    %s\n  SQL: %.700s",
					 all ? all : "(null)", exp.data, q.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------
 * window value functions vs the frame   (CF 7412)
 * ----------------------------------------------------------------
 */

/* frame bound kinds, in the order the grammar requires start <= end */
static const char *const wf_bound[] =
{"UNBOUNDED PRECEDING", "%d PRECEDING", "CURRENT ROW", "%d FOLLOWING",
"UNBOUNDED FOLLOWING"};
static const char *const wf_excl[] =
{"", " EXCLUDE TIES", " EXCLUDE TIES", " EXCLUDE GROUP",
" EXCLUDE CURRENT ROW", " EXCLUDE NO OTHERS"};
static const char *const wf_mode[] = {"ROWS", "ROWS", "RANGE", "GROUPS"};

static void
wf_append_bound(pg_prop_tc *tc, StringInfo b, int kind)
{
	if (kind == 1 || kind == 3)
		appendStringInfo(b, wf_bound[kind], (int) pg_prop_draw_int(tc, 0, 3));
	else
		appendStringInfoString(b, wf_bound[kind]);
}

/*
 * window_value_fns_match_frame  (CF 7412)
 *
 * Draw 1..14 rows (partition p, order key k with many ties, value x with
 * NULLs) and a frame: ROWS / RANGE / GROUPS, random start <= end bounds
 * with small offsets, and an exclusion; half the cases are ROWS ...
 * EXCLUDE TIES with an "n FOLLOWING" start or "n PRECEDING" end.
 *
 * Oracle: over one named window w, array_agg(x) OVER w lists exactly the
 * frame's rows in frame order (aggregates walk the frame row by row), so
 *   first_value(x) = a[1],  last_value(x) = a[cardinality(a)],
 *   nth_value(x, m) = a[m],
 * and with IGNORE NULLS the same against
 *   array_agg(x) FILTER (WHERE x IS NOT NULL) OVER w.
 * Both sides share the window, hence the sort order among peers.  On
 * master, with EXCLUDE TIES and a ROWS frame that starts after (or ends
 * before) a peer of the current row, the value functions skip to the
 * current row, reject it, and return NULL although rows remain.
 */
PG_PROP(window_value_fns_match_frame)
{
	pg_prop_conn *conn;
	int			nrows = (int) pg_prop_draw_int(tc, 1, 14);
	int			kmax = (int) pg_prop_draw_int(tc, 0, 4);
	bool		part = pg_prop_draw_bool(tc);
	bool		focus = pg_prop_draw_bool(tc);
	const char *mode;
	int			sk;
	int			ek;
	const char *excl;
	int			m = (int) pg_prop_draw_int(tc, 1, 4);
	StringInfoData win;
	StringInfoData q;
	char	   *bad;

	if (focus)
	{
		/* ROWS + EXCLUDE TIES, a frame that starts after or ends before us */
		mode = "ROWS";
		excl = " EXCLUDE TIES";
		if (pg_prop_draw_bool(tc))
		{
			sk = 3;
			ek = (int) pg_prop_draw_int(tc, 3, 4);
		}
		else
		{
			ek = 1;
			sk = (int) pg_prop_draw_int(tc, 0, 1);
		}
	}
	else
	{
		mode = wf_mode[pg_prop_draw_int(tc, 0, lengthof(wf_mode) - 1)];
		sk = (int) pg_prop_draw_int(tc, 0, 3);
		ek = (int) pg_prop_draw_int(tc, Max(sk, 1), 4);
		excl = wf_excl[pg_prop_draw_int(tc, 0, lengthof(wf_excl) - 1)];
	}

	initStringInfo(&win);
	appendStringInfo(&win, "%sORDER BY k %s BETWEEN ", part ? "PARTITION BY p " : "",
					 mode);
	wf_append_bound(tc, &win, sk);
	appendStringInfoString(&win, " AND ");
	wf_append_bound(tc, &win, ek);
	appendStringInfoString(&win, excl);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT format('row i=%s p=%s k=%s x=%s: frame %s (non-null %s); "
						   "first %s last %s nth(%s) %s; ignore nulls: first %s last %s nth %s', "
						   "i, p, k, x, a, an, fv, lv, m, nv, fvi, lvi, nvi) "
						   "FROM (SELECT i, p, k, x, ");
	appendStringInfo(&q, "%d AS m, ", m);
	appendStringInfo(&q,
					 "array_agg(x) OVER w AS a, "
					 "array_agg(x) FILTER (WHERE x IS NOT NULL) OVER w AS an, "
					 "first_value(x) OVER w AS fv, last_value(x) OVER w AS lv, "
					 "nth_value(x, %d) OVER w AS nv, "
					 "first_value(x) IGNORE NULLS OVER w AS fvi, "
					 "last_value(x) IGNORE NULLS OVER w AS lvi, "
					 "nth_value(x, %d) IGNORE NULLS OVER w AS nvi "
					 "FROM (VALUES ", m, m);
	for (int i = 0; i < nrows; i++)
	{
		int64		x = pg_prop_draw_int(tc, -1, 9);

		appendStringInfo(&q, "%s(%d, %d, %d, ", i ? ", " : "", i,
						 (int) pg_prop_draw_int(tc, 0, 1),
						 (int) pg_prop_draw_int(tc, 0, kmax));
		if (x < 0)
			appendStringInfoString(&q, "NULL::int)");
		else
			appendStringInfo(&q, "%d)", (int) x);
	}
	appendStringInfo(&q,
					 ") t(i, p, k, x) WINDOW w AS (%s)) s "
					 "WHERE fv IS DISTINCT FROM a[1] "
					 "   OR lv IS DISTINCT FROM a[cardinality(a)] "
					 "   OR nv IS DISTINCT FROM a[m] "
					 "   OR fvi IS DISTINCT FROM an[1] "
					 "   OR lvi IS DISTINCT FROM an[cardinality(an)] "
					 "   OR nvi IS DISTINCT FROM an[m] "
					 "ORDER BY i LIMIT 1", win.data);

	conn = cfb3_connect(tc);
	bad = cfb3_scalar(tc, conn, q.data);
	if (bad != NULL)
		pg_prop_fail(tc, "window value function disagrees with the frame\n"
					 "  WINDOW w AS (%s)\n  %s\n  SQL: %.900s",
					 win.data, bad, q.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------
 * citext split_part   (CF 7453)
 * ----------------------------------------------------------------
 */

/*
 * citext_split_part_like_core  (CF 7453)
 *
 * Draw an ASCII string s (letters of both cases and regex metacharacters,
 * so the delimiter must be matched literally), a delimiter d (empty,
 * random, or a case-flipped substring of s) and a field position n in
 * [-6, 6], zero included.
 *
 * Oracle: on ASCII, citext matching is matching after lower(), and lower()
 * preserves lengths, so the field citext's split_part returns, lowered,
 * must be core split_part(lower(s), lower(d), n) -- with the same outcome
 * when that raises an error (n = 0).  On master citext's split_part
 * returns '' for every negative position, splits an empty delimiter
 * between characters, and returns NULL instead of an error for n = 0.
 */
PG_PROP(citext_split_part_like_core)
{
	static const char alpha[] = "aAbBtT.*+?|()[]{}^$\\- ";
	pg_prop_conn *conn;
	size_t		slen;
	char	   *s = pg_prop_draw_from(tc, alpha, 0, 16, &slen);
	char	   *d;
	int64		n = pg_prop_draw_int(tc, -6, 6);
	StringInfoData q;
	StringInfoData m;
	char	   *got;
	char	   *want;

	switch (pg_prop_draw_int(tc, 0, 3))
	{
		case 0:
			d = pg_strdup("");
			break;
		case 1:
			{
				size_t		dl;

				d = pg_prop_draw_from(tc, alpha, 1, 3, &dl);
				break;
			}
		default:
			if (slen == 0)
				d = pg_strdup("t");
			else
			{
				size_t		off = (size_t) pg_prop_draw_int(tc, 0, slen - 1);
				size_t		len = (size_t) pg_prop_draw_int(tc, 1, Min(3, slen - off));

				d = pnstrdup(s + off, len);
				for (char *c = d; *c; c++)
					if (pg_prop_draw_bool(tc))
						*c = (*c >= 'a' && *c <= 'z') ? *c - 32 :
							(*c >= 'A' && *c <= 'Z') ? *c + 32 : *c;
			}
			break;
	}

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT lower(split_part(");
	fc_sql_lit(&q, s);
	appendStringInfoString(&q, "::citext, ");
	fc_sql_lit(&q, d);
	appendStringInfo(&q, "::citext, %lld))", (long long) n);
	initStringInfo(&m);
	appendStringInfoString(&m, "SELECT split_part(lower(");
	fc_sql_lit(&m, s);
	appendStringInfoString(&m, "), lower(");
	fc_sql_lit(&m, d);
	appendStringInfo(&m, "), %lld)", (long long) n);

	conn = cfb3_connect(tc);
	if (!pg_prop_require_extension(conn, "citext"))
		pg_prop_skip(tc, "citext not installed");
	got = sj_outcome(tc, conn, q.data);
	want = sj_outcome(tc, conn, m.data);
	if (strcmp(got, want) != 0)
		pg_prop_fail(tc, "citext split_part disagrees with core split_part on lowered input\n"
					 "  %s  ->  %s\n  %s  ->  %s", q.data, got, m.data, want);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------
 * GROUPING SETS over constant-reducible expressions   (CF 7438)
 * ----------------------------------------------------------------
 */

/* grouping expressions; several fold to constants (a, id are NOT NULL) */
static const char *const gs_exprs[] =
{"a IS NOT NULL", "id IS NOT NULL", "(b IS NULL) IS UNKNOWN", "1 + 1",
"a IS NULL", "b > 0", "a % 2", "b", "c", "upper(c)", "'k'::text",
"(b IS NOT NULL) IS NOT UNKNOWN"};

/*
 * Type-agnostic uses of a grouping expression in the target list.  The
 * scalar subquery's only column is g, so the expression's columns bind to
 * the outer query in both forms.
 */
static const char *const gs_uses[] =
{"(%s)", "(%s) IS NULL", "coalesce((%s)::text, '-')",
"CASE WHEN (%s) IS NULL THEN 'null' ELSE 'v' END",
"((%s)::text || 'x')",
"(SELECT count(*) FROM generate_series(0, 2) g WHERE (g = 0) = ((%s) IS NULL))"};

/* append template "tpl" with every %s replaced by "arg" */
static void
gs_subst(StringInfo b, const char *tpl, const char *arg)
{
	for (const char *p = tpl; *p; p++)
	{
		if (p[0] == '%' && p[1] == 's')
		{
			appendStringInfoString(b, arg);
			p++;
		}
		else
			appendStringInfoChar(b, *p);
	}
}

static char *
gs_digest(pg_prop_tc *tc, pg_prop_conn *conn, const char *guc, const char *q)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *d;

	cfb3_exec(tc, conn, "BEGIN");
	cfb3_exec(tc, conn, guc);
	d = pg_prop_sql_rowhash(conn, q, &failed, &err);
	pg_prop_sql_exec(conn, "ROLLBACK", NULL);
	if (failed || d == NULL)
		pg_prop_fail(tc, "query failed: %.300s\n  SQL: %.600s", err ? err : "", q);
	return d;
}

/*
 * grouping_sets_const_expr_as_column  (CF 7438)
 *
 * Draw a small table (id primary key, a NOT NULL, b and c nullable), 1..3
 * grouping expressions -- several of which the planner folds to a
 * constant (a IS NOT NULL, (b IS NULL) IS UNKNOWN, 1 + 1) -- a grouping
 * shape (ROLLUP, CUBE, or explicit GROUPING SETS with an empty set), a
 * target list using each expression in several forms (bare, under a
 * cast or CASE, in a scalar subquery), GROUPING(), an aggregate, an
 * optional HAVING on an expression being NULL, and hash or sort
 * aggregation.
 *
 * Oracle: the same query over a subquery that computes each expression
 * as a column (OFFSET 0 keeps it from being flattened) and groups by
 * those columns.  A grouping column is a grouping column whatever it is
 * computed from, so the two must return the same multiset of rows.  On
 * master, references to a constant-folded expression are not recognised
 * as the grouping column, so they are not nulled in the sets that omit
 * it.
 */
PG_PROP(grouping_sets_const_expr_as_column)
{
	pg_prop_conn *conn;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 6);
	int			nexpr = (int) pg_prop_draw_int(tc, 1, 3);
	int			ex[3];
	int			shape = (int) pg_prop_draw_int(tc, 0, 2);
	int			nuse = (int) pg_prop_draw_int(tc, 1, 4);
	int			having = (int) pg_prop_draw_int(tc, -1, nexpr - 1);
	const char *guc = pg_prop_draw_bool(tc) ? "SET LOCAL enable_hashagg = off" :
		"SET LOCAL enable_sort = off";
	StringInfoData setup;
	StringInfoData qa;
	StringInfoData qb;
	char	   *da;
	char	   *db;
	char		col[8];

	/*
	 * Distinct expressions: with a duplicate, query A's reference to it
	 * matches the first copy while B names one column, so they may
	 * legitimately differ.
	 */
	for (int i = 0; i < nexpr; i++)
	{
		int			pick = (int) pg_prop_draw_int(tc, 0, lengthof(gs_exprs) - 1 - i);

		for (int j = 0; j < (int) lengthof(gs_exprs); j++)
		{
			bool		used = false;

			for (int k = 0; k < i; k++)
				used |= (ex[k] == j);
			if (!used && pick-- == 0)
			{
				ex[i] = j;
				break;
			}
		}
	}

	initStringInfo(&setup);
	appendStringInfoString(&setup,
						   "CREATE TEMP TABLE cfb3_gs (id int PRIMARY KEY, a int NOT NULL, "
						   "b int, c text)");
	if (nrows > 0)
	{
		appendStringInfoString(&setup, "; INSERT INTO cfb3_gs VALUES ");
		for (int i = 0; i < nrows; i++)
		{
			int64		b = pg_prop_draw_int(tc, -2, 2);
			int64		c = pg_prop_draw_int(tc, -1, 2);

			appendStringInfo(&setup, "%s(%d, %d, ", i ? ", " : "", i + 1,
							 (int) pg_prop_draw_int(tc, 0, 3));
			if (b == -2)
				appendStringInfoString(&setup, "NULL, ");
			else
				appendStringInfo(&setup, "%d, ", (int) b);
			if (c < 0)
				appendStringInfoString(&setup, "NULL)");
			else
				appendStringInfo(&setup, "'%c')", "xyz"[c]);
		}
	}

	/*
	 * Query A groups by the expressions; query B by columns e0..e2 of a
	 * subquery computing them.  Build both from the same draws.
	 */
	initStringInfo(&qa);
	initStringInfo(&qb);
	appendStringInfoString(&qa, "SELECT ");
	appendStringInfoString(&qb, "SELECT ");
	for (int u = 0; u < nuse; u++)
	{
		int			e = (int) pg_prop_draw_int(tc, 0, nexpr - 1);
		const char *tpl = gs_uses[pg_prop_draw_int(tc, 0, lengthof(gs_uses) - 1)];

		snprintf(col, sizeof(col), "e%d", e);
		gs_subst(&qa, tpl, gs_exprs[ex[e]]);
		gs_subst(&qb, tpl, col);
		appendStringInfoString(&qa, ", ");
		appendStringInfoString(&qb, ", ");
	}
	appendStringInfoString(&qa, "GROUPING(");
	appendStringInfoString(&qb, "GROUPING(");
	for (int i = 0; i < nexpr; i++)
	{
		appendStringInfo(&qa, "%s%s", i ? ", " : "", gs_exprs[ex[i]]);
		appendStringInfo(&qb, "%se%d", i ? ", " : "", i);
	}
	appendStringInfoString(&qa, "), count(*), sum(a) FROM cfb3_gs GROUP BY ");
	appendStringInfoString(&qb, "), count(*), sum(a) FROM (SELECT id, a, b, c");
	for (int i = 0; i < nexpr; i++)
		appendStringInfo(&qb, ", %s AS e%d", gs_exprs[ex[i]], i);
	appendStringInfoString(&qb, " FROM cfb3_gs OFFSET 0) s GROUP BY ");

	if (shape < 2)
	{
		appendStringInfoString(&qa, shape == 0 ? "ROLLUP(" : "CUBE(");
		appendStringInfoString(&qb, shape == 0 ? "ROLLUP(" : "CUBE(");
		for (int i = 0; i < nexpr; i++)
		{
			appendStringInfo(&qa, "%s(%s)", i ? ", " : "", gs_exprs[ex[i]]);
			appendStringInfo(&qb, "%se%d", i ? ", " : "", i);
		}
		appendStringInfoChar(&qa, ')');
		appendStringInfoChar(&qb, ')');
	}
	else
	{
		/* one singleton set per expression, plus the empty set */
		appendStringInfoString(&qa, "GROUPING SETS (");
		appendStringInfoString(&qb, "GROUPING SETS (");
		for (int i = 0; i < nexpr; i++)
		{
			appendStringInfo(&qa, "((%s)), ", gs_exprs[ex[i]]);
			appendStringInfo(&qb, "(e%d), ", i);
		}
		appendStringInfoString(&qa, "())");
		appendStringInfoString(&qb, "())");
	}
	if (having >= 0)
	{
		appendStringInfo(&qa, " HAVING (%s) IS NULL", gs_exprs[ex[having]]);
		appendStringInfo(&qb, " HAVING e%d IS NULL", having);
	}
	appendStringInfoString(&qa, " ORDER BY ");
	appendStringInfoString(&qb, " ORDER BY ");
	for (int i = 1; i <= nuse + 3; i++)
	{
		appendStringInfo(&qa, "%s%d", i > 1 ? ", " : "", i);
		appendStringInfo(&qb, "%s%d", i > 1 ? ", " : "", i);
	}

	conn = cfb3_connect(tc);
	cfb3_exec(tc, conn, setup.data);
	cfb3_exec(tc, conn, "ANALYZE cfb3_gs");
	da = gs_digest(tc, conn, guc, qa.data);
	db = gs_digest(tc, conn, guc, qb.data);
	if (strcmp(da, db) != 0)
	{
		bool		failed;
		char	   *ra = pg_prop_sql_rows(conn, qa.data, &failed, NULL);
		char	   *rb = pg_prop_sql_rows(conn, qb.data, &failed, NULL);

		pg_prop_fail(tc, "grouping by an expression differs from grouping by it as a column\n"
					 "  %s\n  rows: %.200s\n  %s\n  rows: %.200s",
					 qa.data, ra ? ra : "?", qb.data, rb ? rb : "?");
	}
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------
 * JSON_SERIALIZE   (CF 6587)
 * ----------------------------------------------------------------
 */

/* a random JSON document, ASCII and non-ASCII strings, nested */
static void
js_value(pg_prop_tc *tc, StringInfo b, int depth)
{
	static const char *const strs[] =
	{"", "a", "x y", "\\\"q\\\"", "\\\\", "\\n", "\\u00e9", "é", "日本", "''"};
	int			n;

	switch (pg_prop_draw_int(tc, 0, depth > 0 ? 6 : 3))
	{
		case 0:
			appendStringInfo(b, "%lld", (long long) pg_prop_draw_int(tc, -1000, 100000));
			break;
		case 1:
			appendStringInfo(b, "\"%s\"", strs[pg_prop_draw_int(tc, 0, lengthof(strs) - 1)]);
			break;
		case 2:
			appendStringInfoString(b, pg_prop_draw_bool(tc) ? "true" : "null");
			break;
		case 3:
			appendStringInfo(b, "%lld.%02d", (long long) pg_prop_draw_int(tc, -99, 99),
							 (int) pg_prop_draw_int(tc, 0, 99));
			break;
		case 4:
		case 5:
			n = (int) pg_prop_draw_int(tc, 0, 3);
			appendStringInfoChar(b, '{');
			for (int i = 0; i < n; i++)
			{
				appendStringInfo(b, "%s \"k%d\" : ", i ? "," : "",
								 (int) pg_prop_draw_int(tc, 0, 3));
				js_value(tc, b, depth - 1);
			}
			appendStringInfoChar(b, '}');
			break;
		default:
			n = (int) pg_prop_draw_int(tc, 0, 3);
			appendStringInfoChar(b, '[');
			for (int i = 0; i < n; i++)
			{
				if (i)
					appendStringInfoString(b, pg_prop_draw_bool(tc) ? ", " : ",");
				js_value(tc, b, depth - 1);
			}
			appendStringInfoChar(b, ']');
			break;
	}
}

/*
 * json_serialize_is_text_cast  (CF 6587)
 *
 * Draw a JSON document, pass it to JSON_SERIALIZE as json or jsonb (a
 * literal or a column), with no RETURNING clause or RETURNING text,
 * varchar, varchar(n), char(n) or bytea.
 *
 * Oracle: JSON_SERIALIZE renders the value as text and coerces that to
 * the RETURNING type the way an assignment does (length overflow is an
 * error), bytea being the text in the server encoding.  So its outcome
 * must equal that of storing j::text into a column of that type, or of
 * convert_to(j::text, 'UTF8') for bytea.  On master a jsonb argument is
 * fed to the coercion as if it were json and comes out as '' or \x01.
 */
PG_PROP(json_serialize_is_text_cast)
{
	static const char *const rets[] =
	{"", "text", "varchar", "varchar(%d)", "char(%d)", "bytea"};
	pg_prop_conn *conn;
	StringInfoData doc;
	StringInfoData lit;
	StringInfoData q;
	StringInfoData m;
	bool		jsonb = pg_prop_draw_bool(tc);
	bool		as_col = pg_prop_draw_bool(tc);
	int			r = (int) pg_prop_draw_int(tc, 0, lengthof(rets) - 1);
	char		typ[32];
	char	   *got;
	char	   *want;

	initStringInfo(&doc);
	js_value(tc, &doc, 3);
	initStringInfo(&lit);
	fc_sql_lit(&lit, doc.data);
	appendStringInfo(&lit, "::%s", jsonb ? "jsonb" : "json");
	snprintf(typ, sizeof(typ), rets[r], (int) pg_prop_draw_int(tc, 1, 30));

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT JSON_SERIALIZE(%s%s%s)::text%s",
					 as_col ? "x" : lit.data,
					 r ? " RETURNING " : "", typ,
					 as_col ? psprintf(" FROM (VALUES (%s)) v(x)", lit.data) : "");
	initStringInfo(&m);
	conn = cfb3_connect(tc);
	if (r == 5)
		appendStringInfo(&m, "SELECT convert_to((%s)::text, 'UTF8')::text", lit.data);
	else
	{
		/* an assignment cast, as INSERT does it */
		resetStringInfo(&m);
		appendStringInfo(&m, "CREATE TEMP TABLE cfb3_js (v %s)", r ? typ : "text");
		cfb3_exec(tc, conn, m.data);
		resetStringInfo(&m);
		appendStringInfo(&m,
						 "WITH i AS (INSERT INTO cfb3_js SELECT (%s)::text RETURNING v) "
						 "SELECT v::text FROM i", lit.data);
	}
	got = sj_outcome(tc, conn, q.data);
	want = sj_outcome(tc, conn, m.data);
	if (strcmp(got, want) != 0)
		pg_prop_fail(tc, "JSON_SERIALIZE is not the text of its argument\n"
					 "  %s  ->  %s\n  %s  ->  %s", q.data, got, m.data, want);
	pg_prop_disconnect(conn);
}
