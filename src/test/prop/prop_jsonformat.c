/*-------------------------------------------------------------------------
 *
 * prop_jsonformat.c
 *	  Property-based tests for jsonb text formatting and canonicalization
 *	  (src/backend/utils/adt/jsonb.c, json.c: jsonb_pretty, the jsonb parser's
 *	  normal form, row_to_json, jsonb_build_array).
 *
 * jsonb stores a normalized binary tree, so its text output is a canonical
 * rendering: pretty-printing is a pure formatting layer that must reparse to
 * the identical value, and the parser must collapse duplicate keys (last
 * wins) and impose a fixed key order regardless of input order.  These
 * properties check those formatting/canonicalization contracts as exact
 * identities; any "f" is a bug in the renderer or the parser's normal form,
 * not a tolerance artefact.  This is distinct from prop_jsontext (verbatim
 * json), prop_jsonb (jsonb value ops) and prop_jsonbuild (builder algebra):
 * here the subject is the FORMATTING round-trip and canonical normal form.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonformat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonformat"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * JSON string/key contents are drawn only from this alphabet (letters and
 * digits), so a generated value never contains a single quote, double quote,
 * or backslash and the whole JSON text drops into a single-quoted SQL literal
 * with no escaping hazard.
 */
#define JSON_ALPHABET \
	"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

/* Draw a quoted JSON string of 0..4 safe characters into buf. */
static void
draw_json_string(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 4);
	int			i;

	appendStringInfoChar(buf, '"');
	for (i = 0; i < n; i++)
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
 * Draw a random, always-valid JSON text value into buf with DISTINCT object
 * keys (k0..k(n-1)), so no draw ever introduces an incidental duplicate key.
 *
 * The depth budget bounds nesting so a counterexample stays legible and the
 * recursion terminates: at depth <= 0, or ~1/3 of the time otherwise, emit a
 * scalar; else an array or object with 0..3 children one level shallower.
 * Separators carry a space after ':' and ',', which jsonb strips -- so the
 * input is deliberately not already in jsonb's canonical form.
 */
static void
draw_json(pg_prop_tc *tc, StringInfo buf, int depth)
{
	int			nchildren;
	int			i;

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
		for (i = 0; i < nchildren; i++)
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
		for (i = 0; i < nchildren; i++)
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
 * a SQL error means the formatting/parsing code errored or produced NULL on
 * finite valid JSON, which is itself a bug -- so either is a failure,
 * reported with the context (the JSON text or expression under test).
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
 * 1. pretty_reparse: pretty-printing is a lossless formatting layer.
 * jsonb_pretty(J) rewrites J with indentation and newlines; reparsing that
 * text as jsonb must yield exactly J back.  Any "f" means jsonb_pretty
 * emitted text that no longer parses to the same value -- a renderer bug.
 */
PG_PROP(pretty_reparse)
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
					 "SELECT jsonb_pretty('%s'::jsonb)::jsonb = '%s'::jsonb",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. jsonb_dedup_keys: a JSON object text with a duplicate key is normalized
 * by jsonb to keep the LAST occurrence.  Fixed input pins the contract:
 * '{"a":1,"a":2}'::jsonb equals '{"a":2}'::jsonb.  Any "f" means jsonb kept
 * the wrong duplicate (or neither).
 */
PG_PROP(jsonb_dedup_keys)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	q = "SELECT '{\"a\":1,\"a\":2}'::jsonb = '{\"a\":2}'::jsonb";

	require_true(tc, conn, q, q);

	pg_prop_disconnect(conn);
}

/*
 * 3. jsonb_key_order_normalized: jsonb imposes a canonical key order
 * independent of input order, so two objects with the same members written in
 * different orders are EQUAL.  Fixed input: '{"b":1,"a":2}' = '{"a":2,"b":1}'.
 * Any "f" means jsonb's normal form is sensitive to input key order.
 */
PG_PROP(jsonb_key_order_normalized)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	q = "SELECT '{\"b\":1,\"a\":2}'::jsonb = '{\"a\":2,\"b\":1}'::jsonb";

	require_true(tc, conn, q, q);

	pg_prop_disconnect(conn);
}

/*
 * 4. json_preserves_vs_jsonb: parsing a json value and casting it to jsonb
 * must land on the same value as parsing the identical text directly as
 * jsonb.  Both routes normalize to jsonb; the drawn integer n exercises the
 * numeric path through json's text form into the canonical binary tree.  Any
 * "f" means the json->jsonb cast and the jsonb parser disagree on the value.
 */
PG_PROP(json_preserves_vs_jsonb)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, -1000000, 1000000);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('{\"x\": %d}'::json)::jsonb = ('{\"x\": %d}'::jsonb)",
					 n, n);

	require_true(tc, conn, q.data, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. build_array_length: jsonb_build_array builds an array whose length is the
 * number of arguments.  Three fixed args plus a drawn n gives a 4-element
 * array, so jsonb_array_length must be 4 for every n.  Any "f" means the
 * builder dropped or duplicated an element.
 */
PG_PROP(build_array_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, -1000000, 1000000);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_array_length(jsonb_build_array(1, 2, 3, %d)) = 4",
					 n);

	require_true(tc, conn, q.data, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. row_to_json_roundtrip: row_to_json names anonymous ROW fields f1, f2,
 * ...; casting that json to jsonb and extracting field 'f1' must equal the
 * jsonb built directly from the first ROW value.  Draw an int n (first field)
 * and a short safe string s (second field, [a-c], no escaping hazard).  Any
 * "f" means row_to_json mislabeled or mis-rendered the first column.
 */
PG_PROP(row_to_json_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, -1000000, 1000000);
	char	   *s;
	size_t		slen;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	s = pg_prop_draw_from(tc, "abc", 0, 4, &slen);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (row_to_json(ROW((%d), '%s'))::jsonb -> 'f1') "
					 "= to_jsonb((%d))",
					 n, s, n);

	require_true(tc, conn, q.data, q.data);

	pfree(q.data);
	free(s);
	pg_prop_disconnect(conn);
}

/*
 * 7. jsonb_scalar_roundtrip: a jsonb scalar survives a text round-trip.
 * to_jsonb(n) rendered to text and reparsed as jsonb must equal to_jsonb(n).
 * Draw an int n to exercise the numeric scalar's canonical text form.  Any
 * "f" means the scalar's text output does not reparse to itself.
 */
PG_PROP(jsonb_scalar_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, -1000000, 1000000);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (to_jsonb((%d)))::text::jsonb = to_jsonb((%d))",
					 n, n);

	require_true(tc, conn, q.data, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 8. pretty_idempotent_text: jsonb_pretty is stable under reparse at the TEXT
 * level, not just the value level -- pretty-printing an already-pretty value
 * yields byte-identical text.  jsonb_pretty(J) = jsonb_pretty(jsonb_pretty(J)
 * ::jsonb).  Any "f" means the renderer is not idempotent (e.g. accumulates
 * or drops indentation across a round-trip).
 */
PG_PROP(pretty_idempotent_text)
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
					 "SELECT jsonb_pretty('%s'::jsonb) "
					 "= jsonb_pretty(jsonb_pretty('%s'::jsonb)::jsonb)",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
