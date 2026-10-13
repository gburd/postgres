/*-------------------------------------------------------------------------
 *
 * prop_jsoneach.c
 *	  Property-based tests for jsonb set-returning expansion functions.
 *
 * Exercises jsonb_each / jsonb_object_keys / jsonb_each_text /
 * jsonb_array_elements and the build/aggregate inverses (jsonb_object_agg,
 * jsonb_agg, jsonb_build_object).  The oracle in every case is a round-trip
 * or a count identity: expanding a jsonb object or array into rows and
 * re-aggregating must reproduce the original value, and the number of keys or
 * elements expanded must equal what was put in.  A broken expansion (dropped,
 * duplicated, or mis-ordered key/element) breaks one of these identities, so
 * any "f" result is a bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsoneach.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsoneach"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the expansion errored or produced NULL on a well-formed
 * jsonb value, which is itself a bug -- so either is a failure, reported with
 * the SQL.  (Same contract as prop_numeric.c's require_true.)
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
 * Build a jsonb object literal with n distinct keys k0..k(n-1), each mapped to
 * a random small integer, into buf.  n in [0, 5].  Keys are ASCII "k<i>" so
 * they are distinct by construction and need no quote-escaping.  Returns n so
 * the caller can gate key-dependent properties (e.g. k0 present only n >= 1).
 */
static int
draw_object(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 5);

	appendStringInfoString(buf, "'{");
	for (int i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ",");
		appendStringInfo(buf, "\"k%d\": %d",
						 i, (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(buf, "}'::jsonb");
	return n;
}

/*
 * Build a jsonb array literal with m random small-integer elements, m in
 * [0, 5], into buf.  Returns m.
 */
static int
draw_array(pg_prop_tc *tc, StringInfo buf)
{
	int			m = (int) pg_prop_draw_int(tc, 0, 5);

	appendStringInfoString(buf, "'[");
	for (int i = 0; i < m; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ",");
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(buf, "]'::jsonb");
	return m;
}

/*
 * Property 1 -- object_keys_count: jsonb_object_keys emits exactly one row per
 * key, so count(*) over it equals the number of keys n in the object.
 */
PG_PROP(object_keys_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	n = draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM jsonb_object_keys(%s)) = %d",
					 j.data, n);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 2 -- each_reconstructs: expanding an object with jsonb_each and
 * re-aggregating with jsonb_object_agg(key, value) reproduces the original
 * object.  The empty object has no rows, so jsonb_object_agg is NULL there;
 * coalesce back to '{}' so the empty case round-trips soundly.
 */
PG_PROP(each_reconstructs)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	(void) draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce("
					 "(SELECT jsonb_object_agg(key, value) "
					 "FROM jsonb_each(%s)), '{}'::jsonb) = %s",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 3 -- each_text_values: for key k0, the text value jsonb_each_text
 * reports for that key equals the object's ->> 'k0'.  Only meaningful when the
 * object has at least one key (k0 present); for the empty object the property
 * is vacuously true, so guard with n >= 1.
 */
PG_PROP(each_text_values)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	n = draw_object(tc, &j);
	if (n < 1)
	{
		/* no k0 to probe; discard this case */
		pg_prop_assume(tc, false);
		pfree(j.data);
		pg_prop_disconnect(conn);
		return;
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT value FROM jsonb_each_text(%s) "
					 "WHERE key = 'k0') IS NOT DISTINCT FROM (%s ->> 'k0')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 4 -- keys_are_subset: every key reported by jsonb_object_keys is
 * actually present in the object (J ? key).  bool_and over all keys must be
 * true; coalesce(..., true) handles the empty object (no rows, NULL agg).
 */
PG_PROP(keys_are_subset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	(void) draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(bool_and(%s ? k), true) "
					 "FROM jsonb_object_keys(%s) k",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 5 -- object_keys_distinct: a jsonb object has no duplicate keys, so
 * jsonb_object_keys emits each key once -- count(*) equals count(DISTINCT key).
 * The empty object gives 0 = 0, which holds.
 */
PG_PROP(object_keys_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	(void) draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT count(*) = count(DISTINCT key) "
					 "FROM jsonb_object_keys(%s) AS key",
					 j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 6 -- array_elements_count: jsonb_array_elements emits exactly one
 * row per element, so count(*) over it equals the array length m.
 */
PG_PROP(array_elements_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			m;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	m = draw_array(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM jsonb_array_elements(%s)) = %d",
					 a.data, m);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 7 -- array_elements_roundtrip: expanding an array with
 * jsonb_array_elements and re-aggregating with jsonb_agg (which preserves
 * element order) reproduces the original array.  The empty array has no rows,
 * so jsonb_agg is NULL; coalesce back to '[]' so the empty case round-trips.
 */
PG_PROP(array_elements_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	(void) draw_array(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce("
					 "(SELECT jsonb_agg(e) FROM jsonb_array_elements(%s) e), "
					 "'[]'::jsonb) = %s",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 8 -- build_object_each: jsonb_build_object('a',1,'b',2) builds an
 * object with exactly the keys a and b.  Fixed-input sanity check that the
 * build path and the key-expansion path agree: two keys, both present.
 */
PG_PROP(build_object_each)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "jsonb_object_keys(jsonb_build_object('a',1,'b',2))) = 2 "
				 "AND jsonb_build_object('a',1,'b',2) ? 'a' "
				 "AND jsonb_build_object('a',1,'b',2) ? 'b'");

	pg_prop_disconnect(conn);
}
