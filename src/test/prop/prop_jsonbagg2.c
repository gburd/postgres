/*-------------------------------------------------------------------------
 *
 * prop_jsonbagg2.c
 *	  Property-based tests for jsonb aggregate/builder round-trips.
 *
 * Exercises jsonb construction distinct from the jsonbuild/jsonagg areas:
 * to_jsonb() of scalars and arrays, jsonb_object_agg() over a built table,
 * jsonb_build_object(), jsonb_array_length() and jsonb_each().  The oracle is
 * a round-trip or a count identity -- aggregating key/value pairs then looking
 * one up returns the value that went in, extracting a scalar back from its
 * to_jsonb equals the original, the array's reported length equals the number
 * of elements, and the object has exactly as many keys as the (distinct-key)
 * table has rows -- so any "f" result is a bug in the jsonb builders, not a
 * tolerance.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbagg2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbagg2"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operation errored or produced NULL on valid inputs,
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
 * Build jb_t(k text, v int) with a drawn number of rows 0..12.  Keys are
 * DISTINCT (k0, k1, ...) and values are small ints.  Returns the row count so
 * properties can guard the empty case.
 */
static int
build_jb_t(pg_prop_tc *tc, pg_prop_conn *conn)
{
	int			nrows = (int) pg_prop_draw_int(tc, 0, 12);
	int			i;
	char	   *err = NULL;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfoString(&sql,
						   "DROP TABLE IF EXISTS jb_t; "
						   "CREATE TABLE jb_t(k text, v int); ");
	for (i = 0; i < nrows; i++)
	{
		int			v = (int) pg_prop_draw_int(tc, -5, 5);

		appendStringInfo(&sql,
						 "INSERT INTO jb_t VALUES ('k%d', (%d)); ",
						 i, v);
	}
	appendStringInfoString(&sql, "ANALYZE jb_t;");

	if (!pg_prop_sql_exec(conn, sql.data, &err))
		pg_prop_fail(tc, "build jb_t failed: %s\n  SQL: %s",
					 err ? err : "", sql.data);

	pfree(sql.data);
	return nrows;
}

/*
 * object_agg lookup: when k0 exists, jsonb_object_agg(k,v) -> 'k0' equals
 * to_jsonb of the v stored for 'k0'.  Guarded on nrows >= 1 so k0 is present;
 * with no rows the aggregate is NULL and there is nothing to look up, so the
 * empty case is skipped by building at least one row's worth of data only
 * when nrows >= 1 (otherwise the property trivially passes a true constant).
 */
PG_PROP(object_agg_lookup)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	nrows = build_jb_t(tc, conn);

	initStringInfo(&q);
	if (nrows >= 1)
		appendStringInfoString(&q,
							   "SELECT (jsonb_object_agg(k, v) -> 'k0') "
							   "= to_jsonb((SELECT v FROM jb_t WHERE k = 'k0')) "
							   "FROM jb_t");
	else
		appendStringInfoString(&q, "SELECT true");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Scalar round-trip through to_jsonb: for a drawn int n,
 * (to_jsonb(n) #>> '{}')::int = n.  #>> '{}' extracts the scalar as text.
 */
PG_PROP(to_jsonb_int_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, -2147483647, 2147483647);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (to_jsonb((%d)::int) #>> '{}')::int = (%d)::int",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Array length law: jsonb_array_length(to_jsonb(ARRAY[1,2,3])) = 3 (constant)
 * and for a drawn array of m ints it equals m.  m is 0..10.
 */
PG_PROP(array_to_jsonb_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			m;
	int			i;
	StringInfoData arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	m = (int) pg_prop_draw_int(tc, 0, 10);

	initStringInfo(&arr);
	appendStringInfoString(&arr, "ARRAY[");
	for (i = 0; i < m; i++)
	{
		int			v = (int) pg_prop_draw_int(tc, -5, 5);

		if (i > 0)
			appendStringInfoString(&arr, ", ");
		appendStringInfo(&arr, "(%d)", v);
	}
	appendStringInfoString(&arr, "]::int[]");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_array_length(to_jsonb(ARRAY[1,2,3])) = 3 "
					 "AND jsonb_array_length(to_jsonb(%s)) = %d",
					 arr.data, m);

	require_true(tc, conn, q.data);

	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Key count: the number of entries in jsonb_object_agg(k,v) equals the number
 * of (distinct-key) rows in jb_t.  coalesce handles the empty table, where the
 * aggregate is NULL and jsonb_each over NULL yields no rows (count 0 = 0).
 */
PG_PROP(object_agg_key_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	(void) build_jb_t(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT (SELECT count(*) FROM "
						   "jsonb_each(coalesce((SELECT jsonb_object_agg(k, v) "
						   "FROM jb_t), '{}'::jsonb))) "
						   "= (SELECT count(*) FROM jb_t)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * build_object round-trip: jsonb_build_object('a', n, 'b', m) -> 'a' equals
 * to_jsonb(n), for drawn ints n and m.  The 'a' slot must round-trip to its
 * input value.
 */
PG_PROP(jsonb_build_object_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n,
				m;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, -2147483647, 2147483647);
	m = (int) pg_prop_draw_int(tc, -2147483647, 2147483647);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (jsonb_build_object('a', (%d)::int, 'b', (%d)::int) "
					 "-> 'a') = to_jsonb((%d)::int)",
					 n, m, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
