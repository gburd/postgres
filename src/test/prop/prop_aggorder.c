/*-------------------------------------------------------------------------
 *
 * prop_aggorder.c
 *	  Property-based tests for ordered aggregates and aggregate ORDER BY.
 *
 * These check the ordering guarantees of aggregates that take an in-aggregate
 * ORDER BY or a WITHIN GROUP sort: array_agg/string_agg/json_agg emit their
 * elements in the sorted order requested, DESC is the reverse of ASC,
 * percentile_cont over a sorted group is monotone, and a redundant ORDER BY
 * in count() does not change the count.  A failure means the executor's
 * tuplesort-backed aggregate transition fed the final function values in the
 * wrong order (or dropped/duplicated them), which is a wrong-results bug.
 *
 * The oracle is the type's own ordering algebra over a fixed, NULL-free input
 * table (v is never NULL so every ordering is total and exact); each property
 * asserts a scalar boolean that must be true for every generated table.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_aggorder.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "aggorder"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Build ao_t(id int PRIMARY KEY, g int, v int) with 0..25 rows, g in 0..2 and
 * v in -5..5, never NULL so every ORDER BY over v is a total, exact order.
 * Rebuilt fresh in each property so cases do not leak state.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ao_t; "
						   "CREATE TABLE ao_t(id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int i = 0; i < nrows; i++)
	{
		int			g = (int) pg_prop_draw_int(tc, 0, 2);
		int			v = (int) pg_prop_draw_int(tc, -5, 5);

		appendStringInfo(&ddl, "INSERT INTO ao_t VALUES (%d, %d, (%d)); ",
						 i, g, v);
	}
	appendStringInfoString(&ddl, "ANALYZE ao_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  SQL: %s",
					 err ? err : "", ddl.data);

	pfree(ddl.data);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is itself a failure, reported with the SQL.
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
 * array_agg(v ORDER BY v) is non-decreasing: every adjacent pair a[i] <=
 * a[i+1].  coalesce over generate_subscripts makes the empty/one-element array
 * pass (bool_and over no rows is NULL -> true).
 */
PG_PROP(array_agg_sorted)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce((SELECT bool_and(a[i] <= a[i+1]) "
				 "FROM generate_subscripts(a, 1) i "
				 "WHERE i < cardinality(a)), true) "
				 "FROM (SELECT array_agg(v ORDER BY v) a FROM ao_t) s");

	pg_prop_disconnect(conn);
}

/*
 * string_agg(v::text, ',' ORDER BY v, id) is deterministic: computed twice it
 * must be identical.  (v, id) is a total order, so the output is fixed.  IS
 * NOT DISTINCT FROM handles the empty case (both sides NULL -> true).
 */
PG_PROP(string_agg_order_deterministic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT string_agg(v::text, ',' ORDER BY v, id) FROM ao_t) "
				 "IS NOT DISTINCT FROM "
				 "(SELECT string_agg(v::text, ',' ORDER BY v, id) FROM ao_t)");

	pg_prop_disconnect(conn);
}

/*
 * array_agg(v ORDER BY v DESC) is the element-wise reverse of
 * array_agg(v ORDER BY v): same cardinality and a[i] = b[card - i + 1] for
 * every i.  coalesce makes the empty case pass.
 */
PG_PROP(array_agg_desc_is_reverse_asc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT cardinality(a) IS NOT DISTINCT FROM cardinality(b) "
				 "AND coalesce((SELECT bool_and(a[i] = b[cardinality(b) - i + 1]) "
				 "FROM generate_subscripts(a, 1) i), true) "
				 "FROM (SELECT array_agg(v ORDER BY v DESC) a, "
				 "array_agg(v ORDER BY v) b FROM ao_t) s");

	pg_prop_disconnect(conn);
}

/*
 * The first element of json_agg(v ORDER BY v) equals min(v): the sorted JSON
 * array leads with the smallest value.  Guarded on non-empty (count > 0) so
 * the empty case trivially passes.
 */
PG_PROP(json_agg_order_first)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN count(*) = 0 THEN true "
				 "ELSE (json_agg(v ORDER BY v)::jsonb -> 0) = to_jsonb(min(v)) END "
				 "FROM ao_t");

	pg_prop_disconnect(conn);
}

/*
 * percentile_cont(ARRAY[0, 0.5, 1]) WITHIN GROUP (ORDER BY v) returns a
 * 3-element array (the min, median, max of a continuous interpolation) that
 * must be non-decreasing.  Guarded on non-empty; empty input yields NULL,
 * which coalesce turns into a pass.
 */
PG_PROP(percentile_within_group_sorted)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN p IS NULL THEN true "
				 "ELSE cardinality(p) = 3 AND p[1] <= p[2] AND p[2] <= p[3] END "
				 "FROM (SELECT percentile_cont(ARRAY[0, 0.5, 1]) "
				 "WITHIN GROUP (ORDER BY v) p FROM ao_t) s");

	pg_prop_disconnect(conn);
}

/*
 * A redundant ORDER BY inside count() does not change the result:
 * count(v ORDER BY v) = count(v).  Both count exactly one row even on empty
 * input (count over 0 rows is 0, not NULL), so no guard is needed.
 */
PG_PROP(count_order_irrelevant)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(v ORDER BY v) = count(v) FROM ao_t");

	pg_prop_disconnect(conn);
}
