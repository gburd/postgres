/*-------------------------------------------------------------------------
 *
 * prop_jsonbpath.c
 *	  Property-based tests for jsonb path-manipulation functions.
 *
 * These exercise the jsonb mutation functions and operators that address a
 * value by a text[] path or by key: jsonb_set, jsonb_set with create_missing,
 * the #- (delete-at-path) operator, jsonb_strip_nulls, and || (concat).  The
 * oracle is each function's documented contract -- set-then-get returns the
 * value set, mutating one key leaves its siblings untouched, delete removes a
 * key, delete-then-reinsert round-trips to the original, concat lets the right
 * operand win on a conflicting key, strip_nulls drops null-valued members, and
 * containment is reflexive -- so any "f" is a bug in the function, not a
 * modelling slop on our side.  (Distinct from areas "jsonb"/"jsonbops"/
 * "jsonbuild"/"jsonpath": those cover normalization/containment, the raw
 * ||/-/#- operators on generated values, the build_* constructors, and the
 * jsonpath TYPE respectively; this area is the jsonb path-manipulation
 * FUNCTIONS over small built-in object literals.)
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbpath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbpath"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a jsonb object literal with 1..4 distinct keys k0..k3 and small int
 * values, as text '{"k0": 1, "k1": 2}'::jsonb.  k0 is always present so the
 * path '{k0}' always exists; nkeys-1 further keys are k1..k{nkeys-1}, all
 * distinct by construction.  Values are small ints 0..9 so the literal is
 * trivially valid jsonb with no quoting hazard.
 */
static void
draw_object(pg_prop_tc *tc, StringInfo buf)
{
	int			nkeys = (int) pg_prop_draw_int(tc, 1, 4);
	int			i;

	appendStringInfoString(buf, "'{");
	for (i = 0; i < nkeys; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "\"k%d\": %d",
						 i, (int) pg_prop_draw_int(tc, 0, 9));
	}
	appendStringInfoString(buf, "}'::jsonb");
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
 * set_then_get: jsonb_set(J, '{k0}', '99') replaces k0 with 99, so reading
 * k0 back yields exactly 99.  k0 is always present in J.
 */
PG_PROP(set_then_get)
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
					 "SELECT (jsonb_set(%s, '{k0}', '99'::jsonb) -> 'k0') "
					 "= '99'::jsonb",
					 j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * set_preserves_other_keys: replacing k0 does not change k1.  Uses IS NOT
 * DISTINCT FROM so the case where J has no k1 (both sides NULL) still passes.
 */
PG_PROP(set_preserves_other_keys)
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
					 "SELECT (jsonb_set(%s, '{k0}', '99'::jsonb) -> 'k1') "
					 "IS NOT DISTINCT FROM (%s -> 'k1')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * delete_path_removes: after J #- '{k0}' the key k0 no longer exists, so the
 * key-exists test ? 'k0' is false.
 */
PG_PROP(delete_path_removes)
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
	appendStringInfo(&q, "SELECT NOT ((%s #- '{k0}') ? 'k0')", j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * delete_then_set_roundtrip: removing k0 and re-adding its original value
 * (with create_missing = true) reconstructs J exactly.
 */
PG_PROP(delete_then_set_roundtrip)
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
					 "SELECT jsonb_set(%s #- '{k0}', '{k0}', "
					 "%s -> 'k0', true) = %s",
					 j.data, j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * insert_adds_new_key: jsonb_set with create_missing = true adds a key that
 * was absent.  'knew' is never a drawn key (keys are k0..k3), so it is always
 * new and must exist after the set.
 */
PG_PROP(insert_adds_new_key)
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
					 "SELECT (jsonb_set(%s, '{knew}', '7'::jsonb, true)) "
					 "? 'knew'",
					 j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * strip_nulls_removes_null_values: jsonb_strip_nulls drops object members
 * whose value is JSON null, and keeps the rest.  Fixed literal, so this is a
 * constant-input regression check for the function's core contract.
 */
PG_PROP(strip_nulls_removes_null_values)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT NOT (jsonb_strip_nulls('{\"a\":1,\"b\":null}'::jsonb) "
				 "? 'b') AND (jsonb_strip_nulls('{\"a\":1,\"b\":null}'::jsonb) "
				 "? 'a')");

	pg_prop_disconnect(conn);
}

/*
 * concat_right_wins: on a key present in both operands, || takes the right
 * operand's value, so (J || '{"k0": 1234}') -> 'k0' = 1234.  k0 is always in
 * J, exercising the conflict case.
 */
PG_PROP(concat_right_wins)
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
					 "SELECT ((%s || '{\"k0\": 1234}'::jsonb) -> 'k0') "
					 "= '1234'::jsonb",
					 j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * containment_self: every jsonb value contains itself and is contained by
 * itself, so J @> J AND J <@ J always holds.
 */
PG_PROP(containment_self)
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
	appendStringInfo(&q, "SELECT %s @> %s AND %s <@ %s",
					 j.data, j.data, j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
