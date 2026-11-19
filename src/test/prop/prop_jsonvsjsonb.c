/*-------------------------------------------------------------------------
 *
 * prop_jsonvsjsonb.c
 *      Property-based tests for json vs jsonb type-difference semantics.
 *
 * json is textual: it preserves input whitespace, key order and duplicate
 * keys verbatim.  jsonb is a decomposed binary form: it discards whitespace,
 * sorts keys, keeps only the last of duplicate keys, and normalizes numbers.
 * These oracles pin down each documented difference and the json->jsonb
 * normalization bridge; a failure means the type's I/O or the cast changed a
 * guaranteed semantic (e.g. jsonb stopped deduping, or json stopped
 * preserving order), which is a bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      src/test/prop/prop_jsonvsjsonb.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonvsjsonb"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is itself a failure, reported with the SQL that produced it.
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
 * json preserves input key order as text, while jsonb sorts keys.  So the
 * json text round-trips byte-for-byte with the input order, and the jsonb of
 * the same input equals the jsonb of the key-sorted spelling.
 */
PG_PROP(json_preserves_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT '{\"b\":1,\"a\":2}'::json::text = '{\"b\":1,\"a\":2}' "
				 "AND '{\"b\":1,\"a\":2}'::jsonb = '{\"a\":2,\"b\":1}'::jsonb");

	pg_prop_disconnect(conn);
}

/*
 * jsonb keeps only the last of duplicate keys; a json carrying duplicates
 * cast to jsonb collapses to that same last-wins object.
 */
PG_PROP(json_keeps_duplicates_jsonb_dedups)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT '{\"a\":1,\"a\":2}'::jsonb = '{\"a\":2}'::jsonb "
				 "AND '{\"a\":1,\"a\":2}'::json::jsonb = '{\"a\":2}'::jsonb");

	pg_prop_disconnect(conn);
}

/*
 * Casting a json object to jsonb normalizes it to the same value jsonb would
 * build directly.  For any drawn integer n, '{"x": n}'::json::jsonb equals
 * jsonb_build_object('x', n).
 */
PG_PROP(json_to_jsonb_normalizes)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT '{\"x\": %d}'::json::jsonb "
					 "= jsonb_build_object('x', (%d)::int4)",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * jsonb numbers are compared by numeric value, so differently spelled but
 * numerically equal literals are equal: 1.0 = 1.00.
 */
PG_PROP(same_value_different_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT '1.0'::jsonb = '1.00'::jsonb");

	pg_prop_disconnect(conn);
}

/*
 * Array element order is significant and preserved in both json and jsonb, so
 * indexing a jsonb array yields the element at that input position.
 */
PG_PROP(json_array_order_preserved)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT ('[3,1,2]'::jsonb -> 0) = '3'::jsonb");

	pg_prop_disconnect(conn);
}

/*
 * jsonb ignores insignificant whitespace: a spaced-out object parses to the
 * same value as its compact spelling.
 */
PG_PROP(jsonb_whitespace_insensitive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT '{ \"a\" : 1 }'::jsonb = '{\"a\":1}'::jsonb");

	pg_prop_disconnect(conn);
}

/*
 * A built object serialized to json text round-trips to the same jsonb value:
 * O::json::text::jsonb = O::jsonb.  The json textual form carries all the
 * information jsonb needs, so the detour through text is lossless at the jsonb
 * level.
 */
PG_PROP(json_valid_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "WITH o AS (SELECT json_build_object("
				 "'a', 1, 'b', '[2,3]'::json, 'c', 'x') AS j) "
				 "SELECT (j::text)::jsonb = j::jsonb FROM o");

	pg_prop_disconnect(conn);
}

/*
 * row_to_json and to_jsonb agree on composite values once normalized through
 * jsonb: row_to_json(ROW(1,2))::jsonb = to_jsonb(ROW(1,2)), both {"f1":1,"f2":2}.
 */
PG_PROP(row_to_json_vs_jsonb)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT row_to_json(ROW(1,2))::jsonb = to_jsonb(ROW(1,2))");

	pg_prop_disconnect(conn);
}
