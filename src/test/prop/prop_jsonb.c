/*-------------------------------------------------------------------------
 *
 * prop_jsonb.c
 *	  Property-based tests for the jsonb type (src/backend/utils/adt/jsonb*).
 *
 * jsonb normalizes on input: it deduplicates object keys keeping the last
 * value, drops insignificant whitespace, and canonicalizes numbers.  These
 * properties check that round-trips through text are stable, that the
 * normalization is idempotent (applying it again changes nothing), and that
 * structure is preserved -- the builder agrees with the parser, duplicate
 * keys resolve "last wins", and every value contains itself.
 *
 * The oracle is jsonb's own normalized algebra: because input is already
 * normalized, jsonb -> text -> jsonb is the identity, so these equalities
 * must hold exactly.  Any "f" (or a NULL/error on finite valid JSON) is a
 * normalization bug, not a representational artefact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonb.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonb"
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
 * shallower.  Objects sometimes repeat a key name so the "last value wins"
 * dedup path is exercised by the general round-trip properties too.
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
		/* object of 0..3 members, sometimes reusing a key to force dedup */
		bool		dup = pg_prop_draw_bool(tc);

		nchildren = (int) pg_prop_draw_int(tc, 0, 3);
		appendStringInfoChar(buf, '{');
		for (int i = 0; i < nchildren; i++)
		{
			int			key = (dup && i > 0) ? i - 1 : i;

			if (i > 0)
				appendStringInfoString(buf, ", ");
			appendStringInfo(buf, "\"k%d\": ", key);
			draw_json(tc, buf, depth - 1);
		}
		appendStringInfoChar(buf, '}');
	}
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means jsonb errored or produced NULL on finite valid JSON,
 * which is itself a bug -- so either is a failure, reported with the context
 * (the JSON text or expression under test) and the SQL.
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
 * jsonb -> text -> jsonb is stable.  Input is already normalized, so casting
 * out to text and back must reproduce the identical jsonb value; a difference
 * means the text output and the parser disagree about the normal form.
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
	draw_json(tc, &j, 3);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::jsonb)::text::jsonb = '%s'::jsonb",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Normalization is idempotent: two extra text round-trips do not change the
 * value.  jsonb normalizes on parse, so re-parsing already-normalized text
 * must be a fixed point.
 */
PG_PROP(normalization_idempotent)
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
					 "SELECT '%s'::jsonb = '%s'::jsonb::text::jsonb::text::jsonb",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Duplicate object keys resolve "last value wins": {"k": v1, "k": v2} must
 * equal {"k": v2}.  This is the documented dedup behaviour and a classic
 * edge; the two values are drawn independently so v1 != v2 is common.
 */
PG_PROP(object_key_dedup_last_wins)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v1,
				v2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v1);
	initStringInfo(&v2);
	draw_json_scalar(tc, &v1);
	draw_json_scalar(tc, &v2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT '{\"k\": %s, \"k\": %s}'::jsonb = '{\"k\": %s}'::jsonb",
					 v1.data, v2.data, v2.data);

	require_true(tc, conn, q.data, q.data);

	pfree(v1.data);
	pfree(v2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The builder agrees with the parser: jsonb_build_object('a', n, 'b', m)
 * equals the parse of the equivalent literal {"a": n, "b": m}.  Two
 * independent construction paths must land on the same normalized value.
 */
PG_PROP(build_matches_parse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, -1000000, 1000000);
	int			m = (int) pg_prop_draw_int(tc, -1000000, 1000000);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_build_object('a', %d, 'b', %d) = "
					 "'{\"a\": %d, \"b\": %d}'::jsonb",
					 n, m, n, m);

	require_true(tc, conn, q.data, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment is reflexive: every jsonb value contains itself under @>.
 */
PG_PROP(containment_reflexive)
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
	appendStringInfo(&q, "SELECT '%s'::jsonb @> '%s'::jsonb",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
