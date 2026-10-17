/*-------------------------------------------------------------------------
 *
 * prop_jsonconstruct.c
 *	  Property-based tests for jsonb construction functions.
 *
 * Exercises jsonb_build_object, jsonb_build_array, jsonb_object(text[]) and
 * to_jsonb() of a composite.  The oracle is each function's own documented
 * contract: a built object has exactly the keys passed, a built array
 * preserves element order and length, a looked-up value round-trips through
 * to_jsonb, and jsonb_object() pairs a flat text array into string values.
 * Any "f" result is a construction bug, not a formatting artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonconstruct.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonconstruct"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the construction errored or produced NULL on valid
 * inputs, which is itself a bug -- so either is a failure, reported with the
 * SQL.  (Copied from prop_numeric.c's require_true.)
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
 * Draw a short text value from a letters-only alphabet, single-quoted and
 * safe to substitute into SQL (the alphabet contains no quote to double).
 */
static void
draw_str(pg_prop_tc *tc, StringInfo buf)
{
	char	   *s;
	size_t		len = 0;

	s = pg_prop_draw_from(tc, "abc", 0, 6, &len);
	appendStringInfoChar(buf, '\'');
	appendStringInfoString(buf, s);
	appendStringInfoChar(buf, '\'');
	pg_free(s);
}

/*
 * build_object_keys: jsonb_build_object('a',1,'b',2,'c',3) has exactly the
 * three keys a, b, c -- so jsonb_object_keys returns 3 rows.
 */
PG_PROP(build_object_keys)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "jsonb_object_keys(jsonb_build_object('a',1,'b',2,'c',3))) = 3");

	pg_prop_disconnect(conn);
}

/*
 * build_array_order: jsonb_build_array(3,1,2) preserves element order, so
 * ->0, ->1, ->2 are '3', '1', '2' respectively.
 */
PG_PROP(build_array_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT jsonb_build_array(3,1,2) -> 0 = '3'::jsonb "
				 "AND jsonb_build_array(3,1,2) -> 1 = '1'::jsonb "
				 "AND jsonb_build_array(3,1,2) -> 2 = '2'::jsonb");

	pg_prop_disconnect(conn);
}

/*
 * build_object_lookup: for a drawn int n, looking up the key in
 * jsonb_build_object('k', n) yields to_jsonb(n).
 */
PG_PROP(build_object_lookup)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, -2147483648, 2147483647);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_build_object('k', (%d)::int) -> 'k' "
					 "= to_jsonb((%d)::int)",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * jsonb_object_from_array: jsonb_object() over a flat key/value text array
 * builds an object whose values are the text elements (as JSON strings).
 */
PG_PROP(jsonb_object_from_array)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT jsonb_object(ARRAY['a','1','b','2']) "
				 "= '{\"a\":\"1\",\"b\":\"2\"}'::jsonb");

	pg_prop_disconnect(conn);
}

/*
 * to_jsonb_composite: to_jsonb() of a one-row composite with named columns
 * (a int, b text) produces an object whose 'a' member round-trips to
 * to_jsonb(n).  A record literal has no field names, so a named subquery row
 * supplies them.
 */
PG_PROP(to_jsonb_composite)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, -2147483648, 2147483647);
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_str(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT to_jsonb(t) FROM "
					 "(SELECT (%d)::int AS a, %s::text AS b) t) -> 'a' "
					 "= to_jsonb((%d)::int)",
					 n, s.data, n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * build_array_length: jsonb_array_length agrees with the number of elements
 * passed to jsonb_build_array, including the empty array (length 0).
 */
PG_PROP(build_array_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT jsonb_array_length(jsonb_build_array(1,2,3,4,5)) = 5 "
				 "AND jsonb_array_length(jsonb_build_array()) = 0");

	pg_prop_disconnect(conn);
}

/*
 * nested_build: a nested jsonb_build_object is addressable with a path, so
 * jsonb_build_object('o', jsonb_build_object('i', n)) #> '{o,i}' = to_jsonb(n).
 */
PG_PROP(nested_build)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, -2147483648, 2147483647);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_build_object('o', "
					 "jsonb_build_object('i', (%d)::int)) #> '{o,i}' "
					 "= to_jsonb((%d)::int)",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * build_object_null_value: a NULL value in jsonb_build_object becomes JSON
 * null (not a dropped key), so ->'k' = 'null'::jsonb.
 */
PG_PROP(build_object_null_value)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT jsonb_build_object('k', NULL::int) -> 'k' "
				 "= 'null'::jsonb");

	pg_prop_disconnect(conn);
}
