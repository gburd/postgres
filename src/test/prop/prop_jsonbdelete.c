/*-------------------------------------------------------------------------
 *
 * prop_jsonbdelete.c
 *	  Property-based tests for the jsonb delete operators.
 *
 * These exercise the jsonb value-removal operators and their relationships:
 * "- text" (delete object key), "- int" (delete array element by index),
 * "- text[]" (delete several keys at once), and "#- text[]" (delete at path).
 * The oracle is each operator's documented contract and the identities that
 * tie them together -- deleting a present key makes the key-exists test false
 * while leaving siblings intact, deleting an array index shrinks the array by
 * one, a single-level path delete equals the key delete, delete-then-reinsert
 * round-trips to the original, and delete is idempotent -- so any "f" is a bug
 * in the operator, not a modelling slop on our side.  (Distinct from areas
 * "jsonbpath"/"jsonnest": those cover the jsonb path-manipulation FUNCTIONS
 * (jsonb_set/strip_nulls/concat) and nesting/structure respectively; this area
 * is specifically the delete OPERATORS and the algebra among them.)
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbdelete.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbdelete"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a jsonb object literal with n distinct keys k0..k{n-1} (n drawn 2..4)
 * and small int values, as text '{"k0": 1, "k1": 2}'::jsonb.  k0 and k1 are
 * always present (n >= 2), so the properties that reference them always hit a
 * present key.  Values are small ints 0..9 so the literal is trivially valid
 * jsonb with no quoting hazard, and the key "nope" is never drawn.
 */
static void
draw_object(pg_prop_tc *tc, StringInfo buf)
{
	int			nkeys = (int) pg_prop_draw_int(tc, 2, 4);
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
 * Draw a jsonb array literal of m elements (m drawn 1..5) with small int
 * values, as text '[1, 2, 3]'::jsonb, and report m via *mout so the oracle
 * can assert on the post-delete length.
 */
static void
draw_array(pg_prop_tc *tc, StringInfo buf, int *mout)
{
	int			m = (int) pg_prop_draw_int(tc, 1, 5);
	int			i;

	appendStringInfoString(buf, "'[");
	for (i = 0; i < m; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, 0, 9));
	}
	appendStringInfoString(buf, "]'::jsonb");
	*mout = m;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operator errored or produced NULL on a valid input,
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
 * delete_key_removes: after J - 'k0' the key k0 no longer exists, so the
 * key-exists test ? 'k0' is false.  k0 is always present in J.
 */
PG_PROP(delete_key_removes)
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
	appendStringInfo(&q, "SELECT NOT ((%s - 'k0') ? 'k0')", j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * delete_key_preserves_others: deleting k0 leaves k1 in place, so (J - 'k0')
 * still answers true to ? 'k1'.  k1 is always present (n >= 2).
 */
PG_PROP(delete_key_preserves_others)
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
	appendStringInfo(&q, "SELECT (%s - 'k0') ? 'k1'", j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * delete_array_of_keys: J - ARRAY['k0','k1'] removes both keys at once, so
 * neither key exists afterward.  Both keys are always present (n >= 2).
 */
PG_PROP(delete_array_of_keys)
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
					 "SELECT NOT ((%s - ARRAY['k0','k1']) ? 'k0') "
					 "AND NOT ((%s - ARRAY['k0','k1']) ? 'k1')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * delete_absent_key_noop: deleting a key that is not present changes nothing,
 * so J - 'nope' = J.  "nope" is never a drawn key (keys are k0..k3).
 */
PG_PROP(delete_absent_key_noop)
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
	appendStringInfo(&q, "SELECT (%s - 'nope') = %s", j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_delete_index: for a jsonb array of m >= 1 elements, A - 0 deletes the
 * element at index 0, so jsonb_array_length(A - 0) = m - 1.
 */
PG_PROP(array_delete_index)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			m = 0;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_array(tc, &a, &m);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT jsonb_array_length(%s - 0) = %d",
					 a.data, m - 1);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * path_delete_eq_key_delete: a single-level path delete and a key delete are
 * the same operation, so J #- '{k0}' = J - 'k0'.  k0 is always present.
 */
PG_PROP(path_delete_eq_key_delete)
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
	appendStringInfo(&q, "SELECT (%s #- '{k0}') = (%s - 'k0')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * delete_then_set_roundtrip: removing k0 and re-adding its original value
 * (with create_missing = true) reconstructs J exactly.  k0 is always present.
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
					 "SELECT jsonb_set(%s - 'k0', '{k0}', "
					 "%s -> 'k0', true) = %s",
					 j.data, j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * double_delete_idempotent: deleting the same key twice is the same as
 * deleting it once, so (J - 'k0') - 'k0' = J - 'k0'.
 */
PG_PROP(double_delete_idempotent)
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
	appendStringInfo(&q, "SELECT ((%s - 'k0') - 'k0') = (%s - 'k0')",
					 j.data, j.data);

	require_true(tc, conn, q.data);

	pfree(j.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
