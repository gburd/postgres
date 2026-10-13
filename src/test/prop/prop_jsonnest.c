/*-------------------------------------------------------------------------
 *
 * prop_jsonnest.c
 *	  Property-based tests for nested jsonb path navigation (#> and #>>).
 *
 * Exercises the path-extraction operators #> (returns jsonb at a text[] path)
 * and #>> (returns text at a path), plus jsonb_typeof and jsonb_set, against a
 * jsonb value we build in C with a known shape: {"o": {"i": <int>}, "arr":
 * [<ints>]}.  Because we know the shape and the exact values that went in, the
 * oracle is each operator's documented contract -- #> descends the named path
 * step by step, a missing key yields SQL NULL, a negative array index counts
 * from the end, composing two path steps equals the one combined path, typeof
 * reports the container kind, and set-then-get returns the value set -- so any
 * "f" (or a NULL/error where a value is expected) is a bug in path navigation,
 * not a modelling artefact.  (Distinct from areas "jsonb"/"jsonbops"/
 * "jsonbpath"/"jsonbagg2": those cover normalization, the raw ||/-/#-
 * mutation operators, the jsonb_set/#- path FUNCTIONS over literals, and the
 * builder/aggregate round-trips; this area is the #>/#>> READ navigation over
 * a C-built nested value.)
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonnest.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonnest"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operator produced NULL/errored on finite valid input,
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
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * Build a nested jsonb value {"o": {"i": ival}, "arr": [a0, a1, ...]} as a SQL
 * expression text into "j".  o.i is always present (one drawn int); arr holds
 * 0..5 drawn ints.  *iout receives the i value, *narr the array length, and
 * arrout[] the array elements, so a property can assert against the exact
 * values that went in.  All ints are drawn from a small signed domain and
 * substituted parenthesized as "(%lld)" so a negative value never misparses.
 *
 * The value is assembled with jsonb_build_object/jsonb_build_array so no
 * hand-rolled JSON literal escaping is needed and the result is already a
 * canonical jsonb.
 */
static void
build_nested(pg_prop_tc *tc, StringInfo j, long long *iout,
			 int *narr, long long arrout[6])
{
	long long	ival = pg_prop_draw_int(tc, -5, 5);
	int			n = (int) pg_prop_draw_int(tc, 0, 5);
	StringInfoData arr;
	int			k;

	*iout = ival;
	*narr = n;

	initStringInfo(&arr);
	appendStringInfoString(&arr, "jsonb_build_array(");
	for (k = 0; k < n; k++)
	{
		long long	e = pg_prop_draw_int(tc, -5, 5);

		arrout[k] = e;
		if (k > 0)
			appendStringInfoString(&arr, ", ");
		appendStringInfo(&arr, "(%lld)", e);
	}
	appendStringInfoChar(&arr, ')');

	appendStringInfo(j,
					 "jsonb_build_object('o', jsonb_build_object('i', (%lld)), "
					 "'arr', %s)",
					 ival, arr.data);

	pfree(arr.data);
}

/*
 * 1. nested_get: J #> '{o,i}' descends o then i, yielding the i value as
 * jsonb.  Oracle: equals to_jsonb(ival).
 */
PG_PROP(nested_get)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;
	long long	iv;
	int			n;
	long long	arr[6];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_nested(tc, &j, &iv, &n, arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s) #> '{o,i}' = to_jsonb((%lld))",
					 j.data, iv);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. nested_get_text: J #>> '{o,i}' returns the i value as text; cast back to
 * int it must equal the i value that went in.
 */
PG_PROP(nested_get_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;
	long long	iv;
	int			n;
	long long	arr[6];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_nested(tc, &j, &iv, &n, arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) #>> '{o,i}')::int = (%lld)",
					 j.data, iv);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. missing_path_null: a path through a nonexistent key yields SQL NULL.
 * o has only key "i", so J #> '{o,nope}' IS NULL must hold.
 */
PG_PROP(missing_path_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;
	long long	iv;
	int			n;
	long long	arr[6];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_nested(tc, &j, &iv, &n, arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s) #> '{o,nope}' IS NULL",
					 j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. array_index: when arr is non-empty, J #> '{arr,0}' is the first element.
 * Oracle: equals to_jsonb(arr[0]).  Guarded: an empty array has no element 0,
 * so skip (nothing to assert) rather than assert an unsound equality.
 */
PG_PROP(array_index)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;
	long long	iv;
	int			n;
	long long	arr[6];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_nested(tc, &j, &iv, &n, arr);

	if (n == 0)
	{
		pfree(j.data);
		pg_prop_disconnect(conn);
		return;
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s) #> '{arr,0}' = to_jsonb((%lld))",
					 j.data, arr[0]);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. negative_array_index: a negative index counts from the end, so when arr
 * is non-empty, J #> '{arr,-1}' is the last element.  Oracle: equals
 * to_jsonb(arr[n-1]).  Guarded on empty.
 */
PG_PROP(negative_array_index)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;
	long long	iv;
	int			n;
	long long	arr[6];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_nested(tc, &j, &iv, &n, arr);

	if (n == 0)
	{
		pfree(j.data);
		pg_prop_disconnect(conn);
		return;
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s) #> '{arr,-1}' = to_jsonb((%lld))",
					 j.data, arr[n - 1]);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. path_depth_consistency: navigating in two steps equals one combined path.
 * (J #> '{o}') #> '{i}' = J #> '{o,i}'.  A differential oracle: both sides
 * must agree for every input regardless of the drawn values.
 */
PG_PROP(path_depth_consistency)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;
	long long	iv;
	int			n;
	long long	arr[6];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_nested(tc, &j, &iv, &n, arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) #> '{o}') #> '{i}' = (%s) #> '{o,i}'",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. jsonb_typeof: the container/scalar kinds are reported correctly -- o is
 * an object, arr is an array, and o.i is a number.  All three must hold.
 */
PG_PROP(jsonb_typeof)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;
	long long	iv;
	int			n;
	long long	arr[6];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_nested(tc, &j, &iv, &n, arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_typeof((%s)->'o') = 'object' "
					 "AND jsonb_typeof((%s)->'arr') = 'array' "
					 "AND jsonb_typeof((%s) #> '{o,i}') = 'number'",
					 j.data, j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 8. set_deep: jsonb_set replacing o.i with 777 makes J #> '{o,i}' read back
 * as 777.  Set-then-get at a deep path returns the value set.
 */
PG_PROP(set_deep)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData j,
				q;
	long long	iv;
	int			n;
	long long	arr[6];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	build_nested(tc, &j, &iv, &n, arr);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_set((%s), '{o,i}', '777') #> '{o,i}' "
					 "= '777'::jsonb",
					 j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
