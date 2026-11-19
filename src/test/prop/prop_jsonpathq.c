/*-------------------------------------------------------------------------
 *
 * prop_jsonpathq.c
 *	  Property-based tests for jsonb SQL/JSON path QUERY functions.
 *
 * These exercise the jsonpath query evaluators -- jsonb_path_exists,
 * jsonb_path_query, jsonb_path_query_array, jsonb_path_match, and the @? / @@
 * operators -- as applied to small built-in jsonb objects.  The oracle is
 * each function's documented contract cross-checked against the plain jsonb
 * accessors it must agree with: an existence query on '$.a' agrees with the
 * key-exists operator ?, a single-match query returns the same value as ->,
 * a filter query selecting '$.arr[*]' returns every array element, a numeric
 * filter predicate '$.a > k' agrees with the plain integer comparison, the
 * @? / @@ operators agree with their function spellings, and a value filter
 * never selects more elements than the array holds.  Any "f" is a bug in the
 * jsonpath query evaluator, not a modelling slop on our side.  (Distinct from
 * areas "jsonpath" -- the jsonpath TYPE's parse/print -- and "jsonbpath" --
 * the text[]-path mutation functions jsonb_set/#-; this area is the jsonpath
 * QUERY functions and operators.)
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonpathq.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonpathq"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a jsonb object literal of the fixed shape {"a": <int>, "arr": [...]}
 * as text '{"a": 3, "arr": [1, 4]}'::jsonb.  Both members are always present:
 * "a" is a small int 0..9, "arr" is 0..5 small ints 0..9.  Values are tiny
 * ints so the literal is trivially valid jsonb with no quoting hazard, and
 * the jsonpath strings over it ('$.a', '$.arr[*]', ...) stay simple.
 */
static void
draw_object(pg_prop_tc *tc, StringInfo buf)
{
	int			narr = (int) pg_prop_draw_int(tc, 0, 5);
	int			i;

	appendStringInfo(buf, "'{\"a\": %d, \"arr\": [",
					 (int) pg_prop_draw_int(tc, 0, 9));
	for (i = 0; i < narr; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, 0, 9));
	}
	appendStringInfoString(buf, "]}'::jsonb");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on a valid input,
 * which is itself a bug -- so either is a failure, reported with the SQL.
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
 * path_exists_matches_contains_key: jsonb_path_exists(J, '$.a') agrees with
 * the key-exists operator (J ? 'a').  "a" is always present in J, so both
 * are true; the point is that the path evaluator and the accessor agree.
 */
PG_PROP(path_exists_matches_contains_key)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_exists(%s, '$.a') = (%s ? 'a')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * path_query_returns_value: jsonb_path_query(J, '$.a') returns exactly the
 * value J -> 'a'.  '$.a' has a single match (one scalar member), so the
 * set-returning function yields one row equal to the plain accessor.
 */
PG_PROP(path_query_returns_value)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_query(%s, '$.a') = (%s -> 'a')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * path_match_filter: for the numeric member "a", jsonb_path_match with the
 * predicate '$.a > k' agrees with the plain integer comparison
 * ((J ->> 'a')::int > k) for a drawn k.  k is drawn across and beyond the
 * value range of "a" (0..9) so both the true and false sides are exercised.
 */
PG_PROP(path_match_filter)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k = (int) pg_prop_draw_int(tc, -2, 11);
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_match(%s, '$.a > %d') "
					 "= ((%s ->> 'a')::int > %d)",
					 j.data, k, j.data, k);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_path_count: jsonb_path_query_array(J, '$.arr[*]') selects every
 * element of "arr", so its jsonb_array_length equals the length of J -> 'arr'.
 * This checks the wildcard array accessor returns the whole array, no more and
 * no fewer.
 */
PG_PROP(array_path_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_array_length("
					 "jsonb_path_query_array(%s, '$.arr[*]')) "
					 "= jsonb_array_length(%s -> 'arr')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * exists_operator_consistency: the @? operator is the operator spelling of
 * jsonb_path_exists, so (J @? '$.a') equals jsonb_path_exists(J, '$.a') for
 * every J.
 */
PG_PROP(exists_operator_consistency)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s @? '$.a') = jsonb_path_exists(%s, '$.a')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * match_operator_consistency: the @@ operator is the operator spelling of
 * jsonb_path_match, so (J @@ '$.a > k') agrees with jsonb_path_match(J,
 * '$.a > k').  IS NOT DISTINCT FROM so any NULL (should not arise for this
 * shape, but is sound either way) compares equal on both sides.
 */
PG_PROP(match_operator_consistency)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k = (int) pg_prop_draw_int(tc, -2, 11);
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s @@ '$.a > %d') "
					 "IS NOT DISTINCT FROM jsonb_path_match(%s, '$.a > %d')",
					 j.data, k, j.data, k);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * filter_selects_subset: a value filter can only drop elements, never add
 * them, so jsonb_path_query_array(J, '$.arr[*] ? (@ > k)') is never longer
 * than the whole array J -> 'arr'.  A sound one-sided bound (<=) regardless
 * of how many elements pass the filter for the drawn k.
 */
PG_PROP(filter_selects_subset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k = (int) pg_prop_draw_int(tc, -2, 11);
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	draw_object(tc, &j);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_array_length("
					 "jsonb_path_query_array(%s, '$.arr[*] ? (@ > %d)')) "
					 "<= jsonb_array_length(%s -> 'arr')",
					 j.data, k, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
