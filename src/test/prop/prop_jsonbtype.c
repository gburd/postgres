/*-------------------------------------------------------------------------
 *
 * prop_jsonbtype.c
 *	  Property-based tests for jsonb type classification and scalar extraction.
 *
 * The oracle is jsonb's own type system: jsonb_typeof() must report the
 * JSON type of a value, and the scalar extraction operators (#>>, ->, ->>)
 * must round-trip a value back to the SQL value it was built from.  These
 * are exact identities with no tolerance -- a wrong typeof result, a lost
 * value, or a missing-key classification error is a bug in the jsonb type
 * code, not a representational artefact.  Properties cover scalar typeof,
 * object/array typeof, int/bool/text extraction round-trips, numeric value
 * preservation, nested typeof via key navigation, and element typeof via
 * array index navigation.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbtype.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbtype"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means jsonb produced the wrong classification/value (or errored
 * on valid input), which is itself a bug -- so either is a failure, reported
 * with the SQL.
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
 * Draw a string over the alphabet [abc] into buf as a single-quoted SQL text
 * literal body (no surrounding quotes added).  letters-only, so no quote
 * doubling is needed.
 */
static void
draw_abc(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 8);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
}

/*
 * jsonb_typeof reports the JSON type of each scalar kind: a number, a string,
 * a boolean, and the json null literal.
 */
PG_PROP(typeof_scalars)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_typeof(to_jsonb((%d))) = 'number'"
					 " AND jsonb_typeof(to_jsonb('%s'::text)) = 'string'"
					 " AND jsonb_typeof(to_jsonb(true)) = 'boolean'"
					 " AND jsonb_typeof('null'::jsonb) = 'null'",
					 n, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * jsonb_typeof of an empty object is 'object' and of an empty array is
 * 'array'.
 */
PG_PROP(typeof_object_array)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT jsonb_typeof('{}'::jsonb) = 'object'"
				 " AND jsonb_typeof('[]'::jsonb) = 'array'");

	pg_prop_disconnect(conn);
}

/*
 * An int survives a round-trip through a jsonb scalar: extracting the whole
 * document as text (#>> '{}') and casting back to int yields the original.
 * The negative literal is parenthesized before to_jsonb so a leading sign
 * never misparses, and INT32_MIN cannot overflow because the engine's widest
 * draw here is int4-ranged.
 */
PG_PROP(int_extract_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (to_jsonb((%d)) #>> '{}')::int = (%d)",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A boolean jsonb scalar extracts to the SQL text 'true' / 'false'.
 */
PG_PROP(bool_extract)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (to_jsonb(true) #>> '{}') = 'true'"
				 " AND (to_jsonb(false) #>> '{}') = 'false'");

	pg_prop_disconnect(conn);
}

/*
 * A text value survives a round-trip through a jsonb string scalar: building
 * it with to_jsonb and extracting the whole document as text (#>> '{}')
 * yields the original string.
 */
PG_PROP(text_extract_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (to_jsonb('%s'::text) #>> '{}') = '%s'",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * to_jsonb of an int equals the jsonb produced by parsing that int's text:
 * the numeric value and its canonical representation are preserved.
 */
PG_PROP(number_preserves_value)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT to_jsonb((%d)) = ((%d))::text::jsonb",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Navigating into a nested document by key classifies each child correctly,
 * and jsonb_typeof of a missing key is SQL NULL (not an error, not a type).
 */
PG_PROP(nested_typeof)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT jsonb_typeof('{\"a\": [1,2], \"b\": {}}'::jsonb -> 'a')"
				 " = 'array'"
				 " AND jsonb_typeof('{\"a\": [1,2], \"b\": {}}'::jsonb -> 'b')"
				 " = 'object'"
				 " AND jsonb_typeof('{\"a\": [1,2], \"b\": {}}'::jsonb -> 'nope')"
				 " IS NULL");

	pg_prop_disconnect(conn);
}

/*
 * Navigating into an array by index classifies each element by its JSON type.
 */
PG_PROP(array_elem_typeof)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT jsonb_typeof('[1,\"x\",true,null]'::jsonb -> 0)"
				 " = 'number'"
				 " AND jsonb_typeof('[1,\"x\",true,null]'::jsonb -> 1)"
				 " = 'string'"
				 " AND jsonb_typeof('[1,\"x\",true,null]'::jsonb -> 2)"
				 " = 'boolean'"
				 " AND jsonb_typeof('[1,\"x\",true,null]'::jsonb -> 3)"
				 " = 'null'");

	pg_prop_disconnect(conn);
}
