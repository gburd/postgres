/*-------------------------------------------------------------------------
 *
 * prop_jsonbsub.c
 *	  Property-based tests for jsonb subscripting (J['key'], J[idx]).
 *
 * SQL-level subscripting of jsonb (container[expr]) is a separate access path
 * from the ->/->> operators and jsonb_set, implemented by jsonb's subscript
 * handler (jsonb_subscript_handler and the SubscriptingRef executor).  Each
 * property pins subscripting against the operator it is defined to mirror:
 * J['k'] must equal J -> 'k', an integer subscript must equal the numeric ->
 * element, a missing key or out-of-bounds index must be NULL, and an
 * assignment-style update via a built container must land the new value.  A
 * mismatch is a bug in the subscript handler, not in the operators.
 *
 * The oracle is the equivalence between the subscript syntax and the arrow
 * operators (and IS NULL for absent elements), checked with no tolerance.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbsub.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbsub"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the subscript access errored or disagreed with its oracle
 * on a well-formed input, which is itself a bug -- so either is a failure,
 * reported with the SQL.
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
 * Build a jsonb object literal {"k0":<int>,"k1":<int>} into buf as a cast
 * expression, with both keys present and small integer values.
 */
static void
draw_object(pg_prop_tc *tc, StringInfo buf)
{
	int			v0 = (int) pg_prop_draw_int(tc, -5, 5);
	int			v1 = (int) pg_prop_draw_int(tc, -5, 5);

	appendStringInfo(buf,
					 "('{\"k0\": %d, \"k1\": %d}'::jsonb)",
					 v0, v1);
}

/*
 * Build a jsonb array literal [<int>,...] with 1..6 small integer elements
 * into buf as a cast expression.  Always non-empty so element-0 access is
 * well-defined; *n receives the element count.
 */
static void
draw_array(pg_prop_tc *tc, StringInfo buf, int *n)
{
	int			len = (int) pg_prop_draw_int(tc, 1, 6);
	int			i;

	appendStringInfoString(buf, "('[");
	for (i = 0; i < len; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(buf, "]'::jsonb)");
	*n = len;
}

/*
 * subscript_eq_arrow: for an object J, J['k0'] = J -> 'k0'.  The text subscript
 * must select the same member value as the -> operator.
 */
PG_PROP(subscript_eq_arrow)
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
					 "SELECT %s['k0'] IS NOT DISTINCT FROM %s -> 'k0'",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_subscript_eq_arrow: for an array A, A[0] = A -> 0.  jsonb integer
 * subscripts are 0-based, matching the -> integer operator.  The array is
 * always non-empty, so element 0 exists.
 */
PG_PROP(array_subscript_eq_arrow)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_array(tc, &a, &n);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT %s[0] IS NOT DISTINCT FROM %s -> 0",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * subscript_assign: subscripting an object built by appending a new member
 * reads back the appended value.  (J || jsonb_build_object('k0', to_jsonb(99)))
 * sets k0 to 99, and subscripting that container must return 99.  This
 * simulates assignment through the subscript read path.
 */
PG_PROP(subscript_assign)
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
					 "SELECT (%s || jsonb_build_object('k0', to_jsonb(99)))['k0'] "
					 "= '99'::jsonb",
					 j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * missing_key_null: for an object J without key 'nope', J['nope'] IS NULL.
 * A subscript on an absent member yields SQL NULL, not an error.
 */
PG_PROP(missing_key_null)
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
	appendStringInfo(&q, "SELECT %s['nope'] IS NULL", j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * nested_subscript: for J = {"o": {"i": <n>}}, chained subscripting
 * J['o']['i'] returns to_jsonb(n).  This exercises the recursive descent of
 * the subscript executor through nested containers.
 */
PG_PROP(nested_subscript)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nval = (int) pg_prop_draw_int(tc, -5, 5);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('{\"o\": {\"i\": %d}}'::jsonb)['o']['i'] "
					 "= to_jsonb(%d)",
					 nval, nval);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_oob_null: for an array A, an out-of-bounds integer subscript A[1000]
 * IS NULL.  An index past the end yields SQL NULL, not an error.
 */
PG_PROP(array_oob_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_array(tc, &a, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT %s[1000] IS NULL", a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * subscript_text: for an object J whose k0 value is a scalar int, the subscript
 * value equals the ->> text parsed back to int and re-boxed:
 * J['k0'] = to_jsonb((J ->> 'k0')::int).  This pins the subscript result
 * against the text extraction path round-tripped through to_jsonb.
 */
PG_PROP(subscript_text)
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
					 "SELECT %s['k0'] = to_jsonb((%s ->> 'k0')::int)",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
