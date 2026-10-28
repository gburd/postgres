/*-------------------------------------------------------------------------
 *
 * prop_jsonbuild.c
 *	  Property-based tests for jsonb construction/accessor functions
 *	  (src/backend/utils/adt/jsonfuncs.c, jsonb.c).
 *
 * The jsonb builder and accessor functions in jsonfuncs.c have had ~14 fixes
 * over their history.  These properties check builder/accessor inverses --
 * building an object or array and reading an element back must return what
 * went in -- and that the decomposition functions (jsonb_each,
 * jsonb_object_keys) agree with construction: the set of keys and the member
 * count of a built object must match the keys and count it was built from.
 *
 * The oracle is jsonb's own algebra of construction and extraction:
 * jsonb_build_object / jsonb_build_array produce a value whose members are
 * exactly their arguments, so -> , ->> , #> , jsonb_set, jsonb_each and
 * jsonb_object_keys must recover those arguments exactly.  Any "f" (or a
 * NULL/error on finite valid input) is a bug in the builder or accessor, not
 * a representational artefact.
 *
 * Distinct from prop_jsonb.c, which checks text round-trips, normalization
 * idempotency, key dedup, build-vs-parse and containment.  This area targets
 * the jsonb_build_*, jsonb_object, jsonb_set, ->/->>, jsonb_each and
 * jsonb_object_keys functions and their inverses.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbuild.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbuild"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Keys and string values are drawn only from [a-z].  Restricting the alphabet
 * means a generated key or value never contains a single quote (or a
 * backslash, or a double quote), so it can be dropped into a single-quoted SQL
 * literal with no escaping hazard.
 */
#define KEY_ALPHABET "abcdefghijklmnopqrstuvwxyz"

/* Draw a 1-3 letter [a-z] key (e.g. "ab") into buf, unquoted. */
static void
draw_key(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 1, 3);

	for (int i = 0; i < n; i++)
	{
		int			idx = (int) pg_prop_draw_int(tc, 0, (int) strlen(KEY_ALPHABET) - 1);

		appendStringInfoChar(buf, KEY_ALPHABET[idx]);
	}
}

/*
 * Draw a small JSON scalar as a *typed jsonb expression* into buf: a small
 * integer (to_jsonb(n::int)), a quoted [a-z] string ('"abc"'::jsonb), or one
 * of true/false/null ('true'::jsonb).  The result is a self-contained jsonb
 * value usable both as a jsonb_build_* argument and as the expected value in
 * an IS NOT DISTINCT FROM comparison.
 */
static void
draw_scalar(pg_prop_tc *tc, StringInfo buf)
{
	switch ((int) pg_prop_draw_int(tc, 0, 4))
	{
		case 0:
			appendStringInfo(buf, "to_jsonb(%d::int)",
							 (int) pg_prop_draw_int(tc, -1000, 1000));
			break;
		case 1:
			{
				int			n = (int) pg_prop_draw_int(tc, 1, 4);

				appendStringInfoString(buf, "'\"");
				for (int i = 0; i < n; i++)
				{
					int			idx = (int) pg_prop_draw_int(tc, 0, (int) strlen(KEY_ALPHABET) - 1);

					appendStringInfoChar(buf, KEY_ALPHABET[idx]);
				}
				appendStringInfoString(buf, "\"'::jsonb");
				break;
			}
		case 2:
			appendStringInfoString(buf, "'true'::jsonb");
			break;
		case 3:
			appendStringInfoString(buf, "'false'::jsonb");
			break;
		default:
			appendStringInfoString(buf, "'null'::jsonb");
			break;
	}
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a builder/accessor errored or produced NULL on finite
 * valid input, which is itself a bug -- so either is a failure, reported with
 * the SQL.
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
 * Builder/getter inverse for objects: jsonb_build_object(k, V) -> k equals V.
 * Building a one-member object from a key and value, then fetching that key
 * with ->, must recover the value exactly.
 */
PG_PROP(build_object_get_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData k,
				v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&k);
	initStringInfo(&v);
	draw_key(tc, &k);
	draw_scalar(tc, &v);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (jsonb_build_object('%s', %s) -> '%s') "
					 "IS NOT DISTINCT FROM (%s)",
					 k.data, v.data, k.data, v.data);

	require_true(tc, conn, q.data);

	pfree(k.data);
	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Array length matches construction: a jsonb_build_array of n elements has
 * jsonb_array_length n.  n ranges over 0..5; the empty case jsonb_build_array()
 * yields '[]' with length 0.
 */
PG_PROP(build_array_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, 5);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT jsonb_array_length(jsonb_build_array(");
	for (int i = 0; i < n; i++)
	{
		StringInfoData e;

		if (i > 0)
			appendStringInfoString(&q, ", ");
		initStringInfo(&e);
		draw_scalar(tc, &e);
		appendStringInfoString(&q, e.data);
		pfree(e.data);
	}
	appendStringInfo(&q, ")) = %d", n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Array element matches construction: the i-th element of a built array
 * equals the i-th input argument.  Draws n (1..5) scalars and an index i in
 * 0..n-1, keeping each element's expression so the expected value can be
 * compared with -> i.
 */
PG_PROP(array_element_matches_build)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 1, 5);
	int			idx = (int) pg_prop_draw_int(tc, 0, n - 1);
	StringInfoData q;
	StringInfoData elems[5];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	for (int i = 0; i < n; i++)
	{
		initStringInfo(&elems[i]);
		draw_scalar(tc, &elems[i]);
	}

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (jsonb_build_array(");
	for (int i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(&q, ", ");
		appendStringInfoString(&q, elems[i].data);
	}
	appendStringInfo(&q, ") -> %d) IS NOT DISTINCT FROM (%s)",
					 idx, elems[idx].data);

	require_true(tc, conn, q.data);

	for (int i = 0; i < n; i++)
		pfree(elems[i].data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Set-then-get inverse: build an object with a known key, jsonb_set that key
 * to a new value NV, then read it back with ->.  Setting a path and then
 * getting it must return the set value exactly.
 */
PG_PROP(set_then_get)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v0,
				nv,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v0);
	initStringInfo(&nv);
	draw_scalar(tc, &v0);
	draw_scalar(tc, &nv);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (jsonb_set(jsonb_build_object('k0', %s), "
					 "'{k0}', %s) -> 'k0') IS NOT DISTINCT FROM (%s)",
					 v0.data, nv.data, nv.data);

	require_true(tc, conn, q.data);

	pfree(v0.data);
	pfree(nv.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Decomposition agrees with construction (keys): for an object built from
 * distinct keys k0,k1,..., jsonb_object_keys returns exactly those keys as a
 * set.  Keys are generated by index (k0, k1, ...) so they are distinct and
 * none are dropped by dedup; both sides are sorted so order does not matter.
 */
PG_PROP(object_keys_matches_build)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			m = (int) pg_prop_draw_int(tc, 1, 4);
	StringInfoData obj,
				keys,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&obj);
	initStringInfo(&keys);
	appendStringInfoString(&obj, "jsonb_build_object(");
	for (int i = 0; i < m; i++)
	{
		StringInfoData v;

		if (i > 0)
		{
			appendStringInfoString(&obj, ", ");
			appendStringInfoString(&keys, ", ");
		}
		initStringInfo(&v);
		draw_scalar(tc, &v);
		appendStringInfo(&obj, "'k%d', %s", i, v.data);
		appendStringInfo(&keys, "'k%d'", i);
		pfree(v.data);
	}
	appendStringInfoString(&obj, ")");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT array_agg(key ORDER BY key) "
					 "FROM jsonb_object_keys(%s) key) "
					 "= (SELECT array_agg(k ORDER BY k) "
					 "FROM unnest(ARRAY[%s]) k)",
					 obj.data, keys.data);

	require_true(tc, conn, q.data);

	pfree(obj.data);
	pfree(keys.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Decomposition agrees with construction (count): an object built from m
 * distinct keys yields exactly m rows from jsonb_each (one row per member).
 * Keys are generated by index so they are distinct and none are dropped by
 * dedup.
 */
PG_PROP(each_count_matches_size)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			m = (int) pg_prop_draw_int(tc, 1, 4);
	StringInfoData obj,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&obj);
	appendStringInfoString(&obj, "jsonb_build_object(");
	for (int i = 0; i < m; i++)
	{
		StringInfoData v;

		if (i > 0)
			appendStringInfoString(&obj, ", ");
		initStringInfo(&v);
		draw_scalar(tc, &v);
		appendStringInfo(&obj, "'k%d', %s", i, v.data);
		pfree(v.data);
	}
	appendStringInfoString(&obj, ")");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM jsonb_each(%s)) = %d",
					 obj.data, m);

	require_true(tc, conn, q.data);

	pfree(obj.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Path extraction agrees with chained arrow access: for a nested object
 * J = {"k0": {"k1": V}}, J #> '{k0,k1}' equals (J -> 'k0') -> 'k1'.  #> and ->
 * are independent accessor paths and must land on the same value.
 */
PG_PROP(extract_path_matches_arrow)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				obj,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_scalar(tc, &v);

	initStringInfo(&obj);
	appendStringInfo(&obj,
					 "jsonb_build_object('k0', jsonb_build_object('k1', %s))",
					 v.data);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) #> '{k0,k1}') "
					 "IS NOT DISTINCT FROM (((%s) -> 'k0') -> 'k1')",
					 obj.data, obj.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(obj.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
