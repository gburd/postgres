/*-------------------------------------------------------------------------
 *
 * prop_textnum.c
 *	  Property-based tests for text <-> number parsing round-trips.
 *
 * The oracle is the identity that parsing a number's own text rendering must
 * reproduce the number: n = n::text::int4 for integers, X = X::text::numeric
 * for exact numerics, and v ~= (v::text)::float8 within IEEE tolerance for
 * float8.  These exercise the type input/output paths (int4in, numeric_in,
 * float8in and their *_out counterparts) plus the leading/trailing whitespace
 * and leading-sign handling of integer input.  A broken round-trip, a trimmed
 * digit, or a rejected valid literal is an input/output bug, not a rounding
 * artefact: exact round-trips are asserted exactly, the lossy float one only
 * within a stated relative-plus-absolute tolerance.
 *
 * Distinct from castchain/decimalfmt/floatcast: this file is purely about the
 * textual input/output parsers and their whitespace and sign conventions, not
 * cast composition, numeric formatting functions, or binary float<->numeric
 * width relationships.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_textnum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "textnum"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the parse errored or produced NULL on valid input, which
 * is itself a bug -- so either is a failure, reported with the SQL.
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
 * Append an int4-valued literal, parenthesizing negatives so that, when
 * substituted into a larger cast expression, the sign binds to the literal
 * rather than to a cast of the positive magnitude (which would overflow for
 * INT_MIN: bare -2147483648::int4 parses as -(2147483648::int4)).
 */
static void
append_int4(StringInfo buf, int n)
{
	if (n < 0)
		appendStringInfo(buf, "(%d)", n);
	else
		appendStringInfo(buf, "%d", n);
}

/*
 * int_text_roundtrip: for a drawn int4 n, n::text::int4 = n.  The core
 * identity of integer I/O: int4out followed by int4in must recover the value.
 */
PG_PROP(int_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	append_int4(&q, n);
	appendStringInfoString(&q, "::int4::text::int4 = ");
	append_int4(&q, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * text_int_leading_space: (' ' || n::text)::int4 = n.  Integer input trims
 * leading whitespace.  n is drawn >= 0 so there is no leading-sign / space
 * interaction to confuse the parser.
 */
PG_PROP(text_int_leading_space)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, INT32_MAX);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (' ' || ");
	append_int4(&q, n);
	appendStringInfoString(&q, "::int4::text)::int4 = ");
	append_int4(&q, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * text_int_trailing_space: (n::text || ' ')::int4 = n.  Integer input trims
 * trailing whitespace.  n drawn >= 0 to keep sign handling out of it.
 */
PG_PROP(text_int_trailing_space)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, INT32_MAX);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_int4(&q, n);
	appendStringInfoString(&q, "::int4::text || ' ')::int4 = ");
	append_int4(&q, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * numeric_text_roundtrip: for a numeric built as '<int>.<frac>',
 * (X::numeric)::text::numeric = X::numeric.  numeric_out then numeric_in is
 * exact: the text rendering carries full precision, so the recovered value
 * must equal the original exactly.
 */
PG_PROP(numeric_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			ipart = (int) pg_prop_draw_int(tc, 0, 2000000000);
	int			fpart = (int) pg_prop_draw_int(tc, 0, 999999);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/*
	 * Build X as a '<int>.<frac>' text literal cast to numeric.  Both parts
	 * are non-negative so the literal is a plain unsigned decimal; the point
	 * of the property is the text<->numeric round-trip, not sign handling.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%d.%d'::numeric)::text::numeric = '%d.%d'::numeric",
					 ipart, fpart, ipart, fpart);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * float_text_roundtrip_tol: for a float8 v built as '<int>.<frac>',
 * abs((v::text)::float8 - v) < 1e-9 * (abs(v) + 1).  float8out uses enough
 * significant digits to recover the exact binary value, so the round-trip
 * error must stay inside a tight relative-plus-absolute tolerance.
 */
PG_PROP(float_text_roundtrip_tol)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			ipart = (int) pg_prop_draw_int(tc, 0, 1000000000);
	int			fpart = (int) pg_prop_draw_int(tc, 0, 999999);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/*
	 * v is a non-negative '<int>.<frac>' float8.  The tolerance is relative
	 * to abs(v) with a +1 floor so very small v is still covered.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(('%d.%d'::float8)::text::float8 - '%d.%d'::float8) "
					 "< 1e-9::float8 * (abs('%d.%d'::float8) + 1)",
					 ipart, fpart, ipart, fpart, ipart, fpart);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * plus_sign_accepted: ('+' || n::text)::int4 = n for n >= 0.  Integer input
 * accepts an explicit leading '+' sign; prefixing one to a non-negative
 * rendering must not change the parsed value.
 */
PG_PROP(plus_sign_accepted)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, INT32_MAX);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ('+' || ");
	append_int4(&q, n);
	appendStringInfoString(&q, "::int4::text)::int4 = ");
	append_int4(&q, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * widen_narrow_roundtrip: (n::text)::int8::int4 = n for n in int4 range.
 * Rendering an int4, parsing it as the wider int8, then narrowing back must
 * recover n since every int4 value fits in int8.  This exercises both int I/O
 * parsers (int4out, int8in) and the int8->int4 narrowing cast on in-range
 * values.
 */
PG_PROP(widen_narrow_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_int4(&q, n);
	appendStringInfoString(&q, "::int4::text)::int8::int4 = ");
	append_int4(&q, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * bool_text: true::text::bool = true AND false::text::bool = false.  Boolean
 * output ('true'/'false') must parse back to the same value through boolin.
 * No input is drawn: the property is a fixed two-way round-trip assertion.
 */
PG_PROP(bool_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT true::text::bool = true "
				 "AND false::text::bool = false");

	pg_prop_disconnect(conn);
}
