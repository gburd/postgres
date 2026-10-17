/*-------------------------------------------------------------------------
 *
 * prop_jsonbkeys.c
 *	  Property-based tests for jsonb key/value iteration and extraction.
 *
 * Cross-checks the set-returning iterators (jsonb_object_keys, jsonb_each,
 * jsonb_each_text) against the scalar extraction and membership operators
 * (-> , ->> , ? , -) and the re-aggregation inverse (jsonb_object_agg).  The
 * oracle in every case is an agreement identity: the keys an iterator reports
 * must match the keys the object actually has, the value it reports for a key
 * must equal the value the arrow operator extracts, and expanding then
 * re-aggregating must reproduce the object.  Any disagreement -- a dropped,
 * duplicated, or mis-valued key -- breaks one of these, so any "f" is a bug.
 * Distinct from prop_jsoneach.c (iterator round-trips/counts over n in 0..5
 * and arrays) and prop_jsonbcontain.c (containment): here n is 1..5 so k0 is
 * always present and the key-level cross-checks below are never vacuous.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbkeys.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbkeys"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the iteration/extraction errored or produced NULL on a
 * well-formed jsonb object, which is itself a bug -- so either is a failure,
 * reported with the SQL.  (Same contract as prop_numeric.c's require_true.)
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
 * a random small integer, into buf.  n is drawn in [1, 5], so k0 is always
 * present and the key-level properties below (which probe 'k0') are never
 * vacuous.  Keys are ASCII "k<i>" so they are distinct by construction and
 * need no quote-escaping.  Returns n.
 */
static int
draw_object(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 1, 5);

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
 * Property 1 -- keys_match_each: jsonb_object_keys and jsonb_each enumerate the
 * same key set, so they emit the same number of rows for any object.  A
 * difference means one iterator drops or duplicates a key relative to the
 * other.
 */
PG_PROP(keys_match_each)
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
					 "SELECT (SELECT count(*) FROM jsonb_object_keys(%s)) "
					 "= (SELECT count(*) FROM jsonb_each(%s))",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 2 -- each_value_matches_arrow: the value jsonb_each reports for key
 * k0 equals what the -> operator extracts for 'k0'.  k0 is always present
 * (n >= 1), so the subquery yields exactly one row; a mismatch means the
 * iterator and the arrow operator disagree on a key's value.
 */
PG_PROP(each_value_matches_arrow)
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
					 "SELECT (SELECT value FROM jsonb_each(%s) WHERE key = 'k0') "
					 "= (%s -> 'k0')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 3 -- each_text_matches: the text value jsonb_each_text reports for
 * key k0 equals what the ->> operator extracts for 'k0'.  Both render the
 * value as text, so they must agree exactly.
 */
PG_PROP(each_text_matches)
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
					 "SELECT (SELECT value FROM jsonb_each_text(%s) WHERE key = 'k0') "
					 "= (%s ->> 'k0')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 4 -- keys_exist: every key jsonb_object_keys reports is actually
 * present in the object, i.e. J ? key holds for all of them.  bool_and over
 * all keys must be true; n >= 1 guarantees at least one key so the aggregate
 * is never NULL.
 */
PG_PROP(keys_exist)
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
					 "SELECT bool_and(%s ? k) FROM jsonb_object_keys(%s) k",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 5 -- values_reconstruct: expanding the object with jsonb_each and
 * re-aggregating with jsonb_object_agg(key, value) reproduces the original
 * object.  n >= 1 means there is always at least one row, so the aggregate is
 * never NULL and no coalesce is needed.
 */
PG_PROP(values_reconstruct)
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
					 "SELECT (SELECT jsonb_object_agg(key, value) "
					 "FROM jsonb_each(%s)) = %s",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Property 6 -- key_count_eq_n: the keys are distinct by construction, so
 * jsonb_object_keys emits exactly n rows for an object built with n distinct
 * keys.
 */
PG_PROP(key_count_eq_n)
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
 * Property 7 -- object_keys_distinct: a jsonb object has no duplicate keys, so
 * jsonb_object_keys emits each key once -- count(*) equals count(DISTINCT key).
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
 * Property 8 -- delete_reduces_keys: deleting the always-present key k0 with
 * the "- text" operator removes exactly one key, so the key count of (J - 'k0')
 * is n - 1.  n >= 1 guarantees k0 is present, so the delete always removes a
 * real key.
 */
PG_PROP(delete_reduces_keys)
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
					 "SELECT (SELECT count(*) FROM jsonb_object_keys(%s - 'k0')) = %d",
					 j.data, n - 1);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
