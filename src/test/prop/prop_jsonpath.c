/*-------------------------------------------------------------------------
 *
 * prop_jsonpath.c
 *	  Property-based tests for jsonpath (src/backend/utils/adt/jsonpath*).
 *
 * jsonpath has had ~22 correctness fixes; these check that jsonpath query
 * operators agree with independent jsonb accessors and that
 * predicate/existence semantics are consistent.
 *
 * The oracle is jsonb's own element/member accessors: the member accessor
 * (->), the array-element accessor (-> int), and jsonb_array_elements reach
 * the same values a jsonpath selects, by a code path jsonpath does not share.
 * So jsonb_path_query_first against a known path must agree with ->, a
 * numeric filter's match count must agree with an independent unnest count,
 * and $ must select the whole document.  Any "f" (or a NULL/error on finite
 * valid JSON where the property does not tolerate it) is a jsonpath bug.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonpath"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * JSON string/key contents are drawn only from this alphabet.  Restricting to
 * [a-zA-Z0-9] means a generated value never contains a single quote (or a
 * backslash, or a double quote), so the whole JSON text can be dropped into a
 * single-quoted SQL literal with no escaping hazard.  The jsonpath argument is
 * likewise passed as a bare single-quoted string.
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
 * shallower.  Keys are "k0".."k2".
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
		/* object of 0..3 members with keys "k0".."k2" */
		nchildren = (int) pg_prop_draw_int(tc, 0, 3);
		appendStringInfoChar(buf, '{');
		for (int i = 0; i < nchildren; i++)
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
 * a SQL error means jsonpath errored or produced NULL on finite valid JSON,
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
 * $ selects the whole document, so for any JSON value J,
 * jsonb_path_exists(J, '$') is true.  This is the most basic jsonpath
 * guarantee and must hold for every scalar, array, and object.
 */
PG_PROP(exists_root_always_true)
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
					 "SELECT jsonb_path_exists('%s'::jsonb, '$')",
					 j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The member-selecting jsonpath agrees with the member accessor: for an object
 * {"k0": V}, jsonb_path_query_first(J, '$.k0') equals J -> 'k0'.  The accessor
 * (->) reaches the member by a code path jsonpath does not share, so this
 * cross-checks jsonpath's object key resolution.  IS NOT DISTINCT FROM makes
 * the comparison NULL-safe even though J always has the key here.
 */
PG_PROP(path_query_first_matches_accessor)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_json_scalar(tc, &v);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_query_first('{\"k0\": %s}'::jsonb, '$.k0') "
					 "IS NOT DISTINCT FROM ('{\"k0\": %s}'::jsonb -> 'k0')",
					 v.data, v.data);

	require_true(tc, conn, q.data, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The index-selecting jsonpath agrees with the array-element accessor: for an
 * array of drawn scalars and an in-range index i,
 * jsonb_path_query_first(J, '$[i]') equals J -> i.  The accessor (-> int)
 * reaches the element independently of jsonpath's subscript handling.
 */
PG_PROP(array_index_matches_accessor)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len = (int) pg_prop_draw_int(tc, 1, 5);
	int			idx;
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	appendStringInfoChar(&j, '[');
	for (int i = 0; i < len; i++)
	{
		if (i > 0)
			appendStringInfoString(&j, ", ");
		draw_json_scalar(tc, &j);
	}
	appendStringInfoChar(&j, ']');

	idx = (int) pg_prop_draw_int(tc, 0, len - 1);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_query_first('%s'::jsonb, '$[%d]') "
					 "IS NOT DISTINCT FROM ('%s'::jsonb -> %d)",
					 j.data, idx, j.data, idx);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Existence is consistent with query: jsonb_path_exists(J, '$.k0') is true
 * exactly when jsonb_path_query_first(J, '$.k0') is not SQL NULL.  Note that a
 * present JSON null value makes query_first return 'null'::jsonb (not SQL
 * NULL) while exists is still true, so the two agree only against IS NOT NULL
 * -- which is the point.  J is an object that sometimes has k0 and sometimes
 * does not, so both branches are exercised.
 */
PG_PROP(exists_consistent_with_query)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	bool		present = pg_prop_draw_bool(tc);
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	appendStringInfoChar(&j, '{');
	if (present)
	{
		appendStringInfoString(&j, "\"k0\": ");
		draw_json_scalar(tc, &j);
	}
	appendStringInfoChar(&j, '}');

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_path_exists('%s'::jsonb, '$.k0') = "
					 "(jsonb_path_query_first('%s'::jsonb, '$.k0') IS NOT NULL)",
					 j.data, j.data);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A numeric filter selects correctly: for an array of small integers and a
 * threshold t, the number of elements jsonpath keeps with '$[*] ? (@ > t)'
 * equals the count computed independently by unnesting the array and filtering
 * in SQL.  jsonb_array_elements is the independent oracle; the array holds only
 * integers so e::text::int is safe.  This exercises jsonpath filter evaluation.
 */
PG_PROP(numeric_filter_selects_correctly)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len = (int) pg_prop_draw_int(tc, 1, 6);
	int			thresh = (int) pg_prop_draw_int(tc, -1000, 1000);
	StringInfoData j,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&j);
	appendStringInfoChar(&j, '[');
	for (int i = 0; i < len; i++)
	{
		if (i > 0)
			appendStringInfoString(&j, ", ");
		appendStringInfo(&j, "%d", (int) pg_prop_draw_int(tc, -1000, 1000));
	}
	appendStringInfoChar(&j, ']');

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM "
					 "jsonb_path_query('%s'::jsonb, '$[*] ? (@ > %d)') q) "
					 "= (SELECT count(*) FROM "
					 "jsonb_array_elements('%s'::jsonb) e "
					 "WHERE (e::text::int) > %d)",
					 j.data, thresh, j.data, thresh);

	require_true(tc, conn, q.data, j.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * lax and strict modes agree on a bare scalar selected by '$': for a scalar J,
 * jsonb_path_query_first(J, 'strict $') equals jsonb_path_query_first(J,
 * 'lax $').  The mode only changes how structural accessors treat missing
 * members and non-arrays; selecting the whole document is identical either
 * way.  IS NOT DISTINCT FROM keeps the comparison NULL-safe.
 */
PG_PROP(lax_strict_consistent_on_scalar)
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
					 "SELECT jsonb_path_query_first('%d'::jsonb, 'strict $') "
					 "IS NOT DISTINCT FROM "
					 "jsonb_path_query_first('%d'::jsonb, 'lax $')",
					 n, n);

	require_true(tc, conn, q.data, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
