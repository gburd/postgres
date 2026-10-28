/*-------------------------------------------------------------------------
 *
 * prop_jsontext.c
 *	  Property-based tests for the text-preserving json type
 *	  (src/backend/utils/adt/json*).
 *
 * Unlike jsonb, the json type stores the original text verbatim: insignificant
 * whitespace, object key order, and duplicate keys are all preserved exactly,
 * because json is validated-but-unparsed text rather than a normalized binary
 * tree.  These properties check that verbatim-preservation contract and the
 * json accessor/builder functions (json_array_length, ->, json_each,
 * json_build_array), using jsonb as the normalizing oracle where a value
 * comparison is wanted so formatting differences do not cause false failures.
 *
 * The headline distinction from prop_jsonb.c: where jsonb strips whitespace
 * and dedups keys "last wins", json keeps the bytes.  Property 1 (whitespace)
 * and property 7 (duplicate keys) exploit exactly that difference; the
 * remaining properties pin the json functions against the jsonb oracle.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsontext.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsontext"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * JSON string/key contents are drawn only from this alphabet.  Restricting to
 * [a-zA-Z0-9] means a generated value never contains a single quote (or a
 * backslash, or a double quote), so the whole JSON text can be dropped into a
 * single-quoted SQL literal with no escaping hazard.
 */
#define JSON_ALPHABET \
	"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

/* Draw a quoted JSON string of 0..4 safe characters into buf. */
static void
draw_json_string(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 4);

	appendStringInfoChar(buf, '"');
	for (int i = 0; i < n; i++)
	{
		int			idx = (int) pg_prop_draw_int(tc, 0, (int) strlen(JSON_ALPHABET) - 1);

		appendStringInfoChar(buf, JSON_ALPHABET[idx]);
	}
	appendStringInfoChar(buf, '"');
}

/* Draw one JSON scalar (integer, string, true, false, or null) into buf. */
static void
draw_json_scalar(pg_prop_tc *tc, StringInfo buf)
{
	switch ((int) pg_prop_draw_int(tc, 0, 4))
	{
		case 0:
			appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, -1000, 1000));
			break;
		case 1:
			draw_json_string(tc, buf);
			break;
		case 2:
			appendStringInfoString(buf, "true");
			break;
		case 3:
			appendStringInfoString(buf, "false");
			break;
		default:
			appendStringInfoString(buf, "null");
			break;
	}
}

/*
 * Draw a random, always-valid JSON text value into buf.
 *
 * The depth budget bounds nesting so a counterexample stays legible and the
 * recursion always terminates: at depth <= 0, or ~1/3 of the time otherwise,
 * emit a scalar; else emit an array or object with 0..3 children one level
 * shallower.  Separators are "exactly one space after each ':' and ','", so
 * the text carries deliberate insignificant whitespace -- json preserves it
 * verbatim (which the preservation property checks), while jsonb would strip
 * it.  Object keys are k0..k(n-1), always distinct within one object.
 */
static void
draw_json(pg_prop_tc *tc, StringInfo buf, int depth)
{
	int			nchildren;

	if (depth <= 0 || pg_prop_draw_int(tc, 0, 2) == 0)
	{
		draw_json_scalar(tc, buf);
		return;
	}

	if (pg_prop_draw_bool(tc))
	{
		/* array of 0..3 elements */
		nchildren = (int) pg_prop_draw_int(tc, 0, 3);
		appendStringInfoChar(buf, '[');
		for (int i = 0; i < nchildren; i++)
		{
			if (i > 0)
				appendStringInfoString(buf, ", ");
			draw_json(tc, buf, depth - 1);
		}
		appendStringInfoChar(buf, ']');
	}
	else
	{
		/* object of 0..3 members with distinct keys k0..k(n-1) */
		nchildren = (int) pg_prop_draw_int(tc, 0, 3);
		appendStringInfoChar(buf, '{');
		for (int i = 0; i < nchildren; i++)
		{
			if (i > 0)
				appendStringInfoString(buf, ", ");
			appendStringInfo(buf, "\"k%d\": ", i);
			draw_json(tc, buf, depth - 1);
		}
		appendStringInfoChar(buf, '}');
	}
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the json code errored or produced NULL on finite valid
 * JSON, which is itself a bug -- so either is a failure, reported with the
 * context (the JSON text or expression under test) and the SQL.
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql,
			 const char *context)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		pg_prop_fail(tc, "SQL error for %s\n  sql: %s\n  error: %s",
					 context, sql, err ? err : "(unknown)");
	}
	if (res == NULL)
	{
		pg_prop_fail(tc, "unexpected NULL result for %s\n  sql: %s",
					 context, sql);
	}
	if (strcmp(res, "t") != 0)
	{
		pg_prop_fail(tc, "invariant failed (got %s) for %s\n  sql: %s",
					 res, context, sql);
	}
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * The json type stores text verbatim.  draw_json injects exactly one space
 * after every ':' and ',', so the generated text carries insignificant
 * whitespace that jsonb would strip but json must keep.  Casting the json
 * value back to text must reproduce the original bytes exactly -- json does
 * NOT reformat.  Any "f" means json lost or rewrote the input text.
 */
PG_PROP(json_preserves_text_exactly)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	draw_json(tc, &j, 3);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::json)::text = '%s'",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Two routes to jsonb must agree as values.  Parsing text as json and then
 * casting that json to jsonb normalizes it; parsing the same text directly as
 * jsonb normalizes it too.  The intermediate json carries whitespace/order
 * the direct jsonb does not, but once both are jsonb the values must be
 * EQUAL.  A difference means the json->jsonb cast and the jsonb parser build
 * different normal forms from the same input.
 */
PG_PROP(json_vs_jsonb_normalization_differs_or_equal)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	draw_json(tc, &j, 3);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::json::jsonb) = ('%s'::jsonb)",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * json_array_length counts elements.  Build an array literal of exactly n
 * drawn scalars (n in 0..5) and assert json_array_length reports n.  Spaces
 * after commas are insignificant and must not change the count.
 */
PG_PROP(json_array_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, 5);
	StringInfoData arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&arr);
	appendStringInfoChar(&arr, '[');
	for (int i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(&arr, ", ");
		draw_json_scalar(tc, &arr);
	}
	appendStringInfoChar(&arr, ']');

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT json_array_length('%s'::json) = %d",
					 arr.data, n);

	require_true(tc, conn, q.data, arr.data);

	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The json field accessor agrees with the jsonb one.  Build an object with a
 * known first key "k0" and a drawn value, then compare json's -> 'k0' (which
 * returns json) cast to jsonb against jsonb's -> 'k0'.  IS NOT DISTINCT FROM
 * is used so a missing key (both NULL) also matches and formatting never
 * trips the comparison -- only the normalized values are compared.
 */
PG_PROP(json_object_field_matches_jsonb)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				obj,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* object {"k0": <scalar>} with a known key */
	initStringInfo(&v);
	draw_json_scalar(tc, &v);
	initStringInfo(&obj);
	appendStringInfo(&obj, "{\"k0\": %s}", v.data);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (('%s'::json -> 'k0')::jsonb) "
					 "IS NOT DISTINCT FROM ('%s'::jsonb -> 'k0')",
					 obj.data, obj.data);

	require_true(tc, conn, q.data, obj.data);

	pfree(v.data);
	pfree(obj.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The json and jsonb array builders agree.  json_build_array and
 * jsonb_build_array take the same scalar arguments; parsing the json builder's
 * result as jsonb must equal the jsonb builder's result.  Two independent
 * construction paths land on the same normalized value.  Arguments are drawn
 * as plain integers so they are valid SQL constants on both sides.
 */
PG_PROP(json_build_roundtrips_through_jsonb)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nargs = (int) pg_prop_draw_int(tc, 0, 4);
	StringInfoData args,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&args);
	for (int i = 0; i < nargs; i++)
	{
		if (i > 0)
			appendStringInfoString(&args, ", ");
		appendStringInfo(&args, "%d", (int) pg_prop_draw_int(tc, -1000, 1000));
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (json_build_array(%s)::jsonb) = "
					 "jsonb_build_array(%s)",
					 args.data, args.data);

	require_true(tc, conn, q.data, q.data);

	pfree(args.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * json_each yields one row per member.  Build an object with m DISTINCT keys
 * k0..k(m-1) (m in 0..5); json_each must return exactly m rows.  Distinct keys
 * keep the oracle simple -- the duplicate-key case (where json keeps both) is
 * exercised separately below.
 */
PG_PROP(json_each_count_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			m = (int) pg_prop_draw_int(tc, 0, 5);
	StringInfoData obj,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&obj);
	appendStringInfoChar(&obj, '{');
	for (int i = 0; i < m; i++)
	{
		StringInfoData v;

		if (i > 0)
			appendStringInfoString(&obj, ", ");
		initStringInfo(&v);
		draw_json_scalar(tc, &v);
		appendStringInfo(&obj, "\"k%d\": %s", i, v.data);
		pfree(v.data);
	}
	appendStringInfoChar(&obj, '}');

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM json_each('%s'::json)) = %d",
					 obj.data, m);

	require_true(tc, conn, q.data, obj.data);

	pfree(obj.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The headline json-vs-jsonb distinction: json preserves duplicate object
 * keys verbatim, jsonb dedups them.  For the fixed literal {"k": 1, "k": 2},
 * json_each returns BOTH members (2 rows) while jsonb_each keeps only the last
 * (1 row).  Fixed input, no draw -- this pins the behavioural contract itself.
 */
PG_PROP(json_duplicate_keys_preserved)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	q = "SELECT (SELECT count(*) FROM json_each('{\"k\": 1, \"k\": 2}'::json)) = 2 "
		"AND (SELECT count(*) FROM jsonb_each('{\"k\": 1, \"k\": 2}'::jsonb)) = 1";

	require_true(tc, conn, q, q);

	pg_prop_disconnect(conn);
}
