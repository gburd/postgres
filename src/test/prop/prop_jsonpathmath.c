/*-------------------------------------------------------------------------
 *
 * prop_jsonpathmath.c
 *	  Property-based tests for jsonpath arithmetic and filter methods.
 *
 * This area targets the jsonpath item methods and in-path arithmetic
 * (.size(), .type(), .abs(), .double(), @ comparisons in filters, and
 * expressions like '$.a + 1') rather than the bare member/index/existence
 * selection that prop_jsonpath / prop_jsonbpath / prop_jsonpathq cover.
 *
 * The oracle is an independent jsonb accessor computing the same answer by a
 * code path the jsonpath method evaluator does not share: jsonb_array_length,
 * J->>'a' cast to int/float8, abs()/to_jsonb() in SQL, and a subquery over
 * jsonb_array_elements for the filter counts.  Any "f" (or a NULL/error on
 * finite valid JSON where the property does not tolerate it) is a jsonpath
 * method bug.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonpathmath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonpathmath"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Integer domains for J.  "a" is drawn across a wide signed range (so abs()
 * crosses zero and the double() cast exercises large magnitudes), and array
 * elements are drawn -6..6 (a mix of signs including zero) so the sign-filter
 * properties see positive, negative, and zero elements.
 */
#define A_MIN (-1000000)
#define A_MAX 1000000
#define ARR_MIN (-6)
#define ARR_MAX 6

/*
 * Build the JSON text {"a": <int>, "arr": [<0..6 ints>]} into buf, and report
 * the drawn "a" value and the array through *a_out and the StringInfo arr_out
 * (a comma-joined list of the element values) so callers can form the oracle.
 * All values are integers rendered with %d, so the text is safe to drop into a
 * single-quoted SQL literal with no escaping.
 */
static void
draw_doc(pg_prop_tc *tc, StringInfo buf, int *a_out, StringInfo arr_out)
{
	int			a = (int) pg_prop_draw_int(tc, A_MIN, A_MAX);
	int			len = (int) pg_prop_draw_int(tc, 0, 6);

	*a_out = a;

	appendStringInfo(buf, "{\"a\": %d, \"arr\": [", a);
	for (int i = 0; i < len; i++)
	{
		int			e = (int) pg_prop_draw_int(tc, ARR_MIN, ARR_MAX);

		if (i > 0)
		{
			appendStringInfoString(buf, ", ");
			appendStringInfoString(arr_out, ", ");
		}
		appendStringInfo(buf, "%d", e);
		appendStringInfo(arr_out, "%d", e);
	}
	appendStringInfoString(buf, "]}");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a jsonpath method errored or produced NULL on finite valid
 * JSON, which is itself a bug -- so either is a failure, reported with the
 * JSON text under test and the SQL.
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
 * .size() on an array equals its length: jsonb_path_query_first(J, '$.arr.size()')
 * equals to_jsonb(jsonb_array_length(J->'arr')).  jsonb_array_length reaches
 * the length by a code path the .size() method does not share.
 */
PG_PROP(size_method)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	StringInfoData j,
				arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	initStringInfo(&arr);
	draw_doc(tc, &j, &a, &arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_query_first('%s'::jsonb, '$.arr.size()') "
					 "= to_jsonb(jsonb_array_length('%s'::jsonb -> 'arr'))",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * .type() reports the item kind: '$.a.type()' is '"number"' and
 * '$.arr.type()' is '"array"'.  "a" is always an integer and "arr" always an
 * array in the generated document, so both are exact constants.
 */
PG_PROP(type_method)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	StringInfoData j,
				arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	initStringInfo(&arr);
	draw_doc(tc, &j, &a, &arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_query_first('%s'::jsonb, '$.a.type()') "
					 "= '\"number\"'::jsonb "
					 "AND jsonb_path_query_first('%s'::jsonb, '$.arr.type()') "
					 "= '\"array\"'::jsonb",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Arithmetic in a path: '$.a + 1' equals to_jsonb(a + 1) where a is the drawn
 * integer.  A_MAX + 1 is far below int4 overflow, so (a + 1) is computed in C
 * and compared against jsonpath's own addition.
 */
PG_PROP(arithmetic_in_path)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	StringInfoData j,
				arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	initStringInfo(&arr);
	draw_doc(tc, &j, &a, &arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_query_first('%s'::jsonb, '$.a + 1') "
					 "= to_jsonb((%d))",
					 j.data, a + 1);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A '> 0' filter keeps exactly the positive elements: the length of
 * jsonb_path_query_array(J, '$.arr[*] ? (@ > 0)') equals the count of positive
 * array elements, computed independently by a subquery over
 * jsonb_array_elements.  Elements are integers so e::text::int is safe.
 */
PG_PROP(filter_gt)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	StringInfoData j,
				arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	initStringInfo(&arr);
	draw_doc(tc, &j, &a, &arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_array_length("
					 "jsonb_path_query_array('%s'::jsonb, '$.arr[*] ? (@ > 0)')) "
					 "= (SELECT count(*) FROM "
					 "jsonb_array_elements('%s'::jsonb -> 'arr') e "
					 "WHERE (e::text::int) > 0)",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An equality filter's existence agrees with membership: for a drawn k,
 * jsonb_path_exists(J, '$.arr[*] ? (@ == k)') is true exactly when k is an
 * element of arr, determined independently via jsonb_array_elements.  k is
 * drawn from the same -6..6 domain so both the present and absent branches are
 * exercised.
 */
PG_PROP(exists_filter)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	int			k = (int) pg_prop_draw_int(tc, ARR_MIN, ARR_MAX);
	StringInfoData j,
				arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	initStringInfo(&arr);
	draw_doc(tc, &j, &a, &arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_exists('%s'::jsonb, '$.arr[*] ? (@ == %d)') "
					 "= EXISTS (SELECT 1 FROM "
					 "jsonb_array_elements('%s'::jsonb -> 'arr') e "
					 "WHERE (e::text::int) = %d)",
					 j.data, k, j.data, k);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * .abs() agrees with SQL abs(): '$.a.abs()' equals to_jsonb(abs(a)).  "a" is
 * drawn in [-1000000, 1000000], well inside int4, so abs() does not overflow
 * and the comparison is exact.
 */
PG_PROP(abs_method)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	StringInfoData j,
				arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	initStringInfo(&arr);
	draw_doc(tc, &j, &a, &arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_query_first('%s'::jsonb, '$.a.abs()') "
					 "= to_jsonb(abs((%d)))",
					 j.data, a);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * .double() agrees with the float8 cast: '$.a.double()' equals
 * to_jsonb(a::float8).  "a" is a small integer, exactly representable in
 * float8, so both sides render the same numeric value.
 */
PG_PROP(double_method)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	StringInfoData j,
				arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	initStringInfo(&arr);
	draw_doc(tc, &j, &a, &arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_query_first('%s'::jsonb, '$.a.double()') "
					 "= to_jsonb((%d)::float8)",
					 j.data, a);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A filter never grows the array: the length of
 * jsonb_path_query_array(J, '$.arr[*] ? (@ >= 0)') is at most the length of
 * arr.  This is a sound one-sided bound for any array -- a filter can only
 * select a subset -- and holds even when arr is empty (0 <= 0).
 */
PG_PROP(min_filter_count_le)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a;
	StringInfoData j,
				arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	initStringInfo(&arr);
	draw_doc(tc, &j, &a, &arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_array_length("
					 "jsonb_path_query_array('%s'::jsonb, '$.arr[*] ? (@ >= 0)')) "
					 "<= jsonb_array_length('%s'::jsonb -> 'arr')",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
