/*-------------------------------------------------------------------------
 *
 * prop_jsonbroundtrip.c
 *	  Property-based tests for full jsonb value round-trips.
 *
 * The oracle is identity: a jsonb value J serialized to text (or pretty text,
 * or concatenated with an empty object) and parsed back must equal J.  Nested
 * objects and arrays with integer values are built in C and substituted into
 * SQL.  Any "f" is a serialize/parse asymmetry in jsonb I/O -- key ordering,
 * number canonicalization, whitespace, or structural handling -- which is a
 * bug.  Distinct from jsonformat/jsonbnum/jsonbtype: those check pretty-print
 * dedup, number canonicalization, and type-of extraction respectively; this
 * area asserts whole-value identity across text/pretty/concat/strip_nulls on
 * built nested shapes.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbroundtrip.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbroundtrip"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the round-trip errored or produced NULL, which is itself
 * a failure, reported with the SQL.  (Same shape as prop_numeric.c.)
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
 * Draw a signed integer and append it as a JSON number into buf.  Negative
 * values are not special inside a JSON object/array literal (no cast is
 * applied here), but the matching to_jsonb() cases parenthesize so a leading
 * '-' never misparses -- see draw_jsonb_scalar_arg.
 */
static void
append_json_int(pg_prop_tc *tc, StringInfo buf)
{
	int64		n = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	appendStringInfo(buf, INT64_FORMAT, n);
}

/*
 * Build a jsonb object literal with a-c keys and integer values, plus one
 * nested array of integers, into buf as a single-quoted SQL text literal cast
 * to jsonb.  Shape A: {"a":<n>,"b":<n>,...,"arr":[<n>,...]}.  Keys are a fixed
 * ASCII alphabet so no quoting hazard arises; values are plain JSON numbers so
 * the literal is always valid JSON.
 */
static void
build_jsonb_object(pg_prop_tc *tc, StringInfo buf)
{
	static const char *const keys[] = {"a", "b", "c"};
	int			nkeys = (int) pg_prop_draw_int(tc, 1, 3);
	int			narr = (int) pg_prop_draw_int(tc, 0, 4);
	int			i;

	appendStringInfoString(buf, "'{");
	for (i = 0; i < nkeys; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ',');
		appendStringInfo(buf, "\"%s\":", keys[i]);
		append_json_int(tc, buf);
	}
	/* one nested array of integers */
	if (nkeys > 0)
		appendStringInfoChar(buf, ',');
	appendStringInfoString(buf, "\"arr\":[");
	for (i = 0; i < narr; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ',');
		append_json_int(tc, buf);
	}
	appendStringInfoString(buf, "]}'::jsonb");
}

/*
 * Build a jsonb array literal of integers into buf as a single-quoted SQL
 * text literal cast to jsonb: '[<n>,<n>,...]'::jsonb.
 */
static void
build_jsonb_array(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 6);
	int			i;

	appendStringInfoString(buf, "'[");
	for (i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ',');
		append_json_int(tc, buf);
	}
	appendStringInfoString(buf, "]'::jsonb");
}

/*
 * J::text::jsonb = J for a built object.  Serializing a jsonb object to text
 * and parsing it back must reproduce the identical value.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_jsonb_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::jsonb = (%s)", j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * jsonb_pretty(J)::jsonb = J.  Pretty-printing adds whitespace/indentation
 * only; reparsing must recover J exactly.
 */
PG_PROP(pretty_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_jsonb_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT jsonb_pretty(%s)::jsonb = (%s)",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * (J || '{}'::jsonb)::text::jsonb = J.  Concatenating an empty object is a
 * no-op on an object value, so the full text round-trip must still equal J.
 */
PG_PROP(concat_empty_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_jsonb_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) || '{}'::jsonb)::text::jsonb = (%s)",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For J = {"o":{"i":<n>},"a":[1,2,3]}, J::text::jsonb = J.  A deliberately
 * nested object-in-object plus array value exercises recursive serialize and
 * parse together.
 */
PG_PROP(nested_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	appendStringInfoString(&j, "'{\"o\":{\"i\":");
	append_json_int(tc, &j);
	appendStringInfoString(&j, "},\"a\":[1,2,3]}'::jsonb");

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::jsonb = (%s)", j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For a jsonb array A, A::text::jsonb = A.  Array order and element values
 * must survive the text round-trip.
 */
PG_PROP(array_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	build_jsonb_array(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::jsonb = (%s)", a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Scalar round-trips: to_jsonb(<n>)::text::jsonb = to_jsonb(<n>), and the
 * literal scalars 'true' and 'null' round-trip through text too.  The integer
 * argument is parenthesized so a negative value binds to to_jsonb, not to the
 * surrounding cast.
 */
PG_PROP(scalar_roundtrips)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int64		n = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_jsonb((" INT64_FORMAT "))::text::jsonb "
					 "= to_jsonb((" INT64_FORMAT ")) "
					 "AND 'true'::jsonb::text::jsonb = 'true'::jsonb "
					 "AND 'null'::jsonb::text::jsonb = 'null'::jsonb",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Double pretty: jsonb_pretty(jsonb_pretty(J)::jsonb) = jsonb_pretty(J).
 * Pretty-printing then reparsing then pretty-printing again must be stable --
 * pretty output is a canonical rendering of the same value.
 */
PG_PROP(double_pretty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_jsonb_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_pretty(jsonb_pretty(%s)::jsonb) "
					 "= jsonb_pretty(%s)",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * strip_nulls round-trip: jsonb_strip_nulls(J)::text::jsonb =
 * jsonb_strip_nulls(J).  Whatever structure survives null-stripping must
 * itself round-trip through text unchanged.
 */
PG_PROP(strip_nulls_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_jsonb_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_strip_nulls(%s)::text::jsonb "
					 "= jsonb_strip_nulls(%s)",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
