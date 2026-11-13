/*-------------------------------------------------------------------------
 *
 * prop_jsonaggord.c
 *	  Property-based tests for jsonb aggregate ordering, filtering and keys.
 *
 * Exercises jsonb_agg()/jsonb_object_agg() behaviour distinct from the
 * jsonagg/jsonbagg2 areas: the ORDER BY inside the aggregate (first element
 * equals min), FILTER (WHERE ...) narrowing the aggregated set, DISTINCT
 * de-duplication, element-sum against sum(v), and per-group length.  The
 * oracle is a count/sum/extremum identity against the plain aggregate -- so a
 * mismatch is a bug in the jsonb aggregates, not a tolerance.  Every property
 * guards the empty / all-filtered-out case with coalesce so 0 rows passes
 * soundly.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonaggord.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonaggord"
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
 * Build ja_t(id int PRIMARY KEY, g int, v int) with a drawn number of rows
 * 0..20.  id runs 0..nrows-1 (a dense distinct PK), g in 0..2 (so grouping has
 * a few buckets), v in 0..9.  Returns the row count so properties can guard
 * the empty case.
 */
static int
build_ja_t(pg_prop_tc *tc, pg_prop_conn *conn)
{
	int			nrows = (int) pg_prop_draw_int(tc, 0, 20);
	int			i;
	char	   *err = NULL;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfoString(&sql,
						   "DROP TABLE IF EXISTS ja_t; "
						   "CREATE TABLE ja_t(id int PRIMARY KEY, g int, v int); ");
	for (i = 0; i < nrows; i++)
	{
		int			g = (int) pg_prop_draw_int(tc, 0, 2);
		int			v = (int) pg_prop_draw_int(tc, 0, 9);

		appendStringInfo(&sql,
						 "INSERT INTO ja_t VALUES (%d, %d, %d); ",
						 i, g, v);
	}
	appendStringInfoString(&sql, "ANALYZE ja_t;");

	if (!pg_prop_sql_exec(conn, sql.data, &err))
		pg_prop_fail(tc, "build ja_t failed: %s\n  SQL: %s",
					 err ? err : "", sql.data);

	pfree(sql.data);
	return nrows;
}

/*
 * Ordered first element: jsonb_agg(v ORDER BY v) -> 0 equals to_jsonb(min(v)).
 * The first element of a value-ordered array is the minimum.  Guarded on
 * nrows >= 1: over 0 rows the aggregate is NULL (-> 0 is NULL) and min(v) is
 * NULL too, so there is nothing to compare; skip to a true constant.
 */
PG_PROP(jsonb_agg_ordered_first)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	nrows = build_ja_t(tc, conn);

	initStringInfo(&q);
	if (nrows >= 1)
		appendStringInfoString(&q,
							   "SELECT (jsonb_agg(v ORDER BY v) -> 0) "
							   "= to_jsonb(min(v)) FROM ja_t");
	else
		appendStringInfoString(&q, "SELECT true");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Array length equals row count: jsonb_array_length(jsonb_agg(v)) = count(*).
 * coalesce handles the empty table, where jsonb_agg is NULL: coalesce(NULL, 0)
 * = count(*) = 0 holds soundly.
 */
PG_PROP(jsonb_agg_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	(void) build_ja_t(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT coalesce(jsonb_array_length(jsonb_agg(v)), 0) "
						   "= count(*) FROM ja_t");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Object_agg lookup by PK: jsonb_object_agg(id::text, v) looked up at the min
 * id's text equals to_jsonb of that row's v.  id is the PRIMARY KEY, so keys
 * are distinct and the lookup is unambiguous.  Guarded on nrows >= 1 so a
 * minimum id exists; the empty case skips to a true constant.
 */
PG_PROP(jsonb_object_agg_lookup)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	nrows = build_ja_t(tc, conn);

	initStringInfo(&q);
	if (nrows >= 1)
		appendStringInfoString(&q,
							   "SELECT (jsonb_object_agg(id::text, v) "
							   "-> (SELECT min(id)::text FROM ja_t)) "
							   "= to_jsonb((SELECT v FROM ja_t "
							   "WHERE id = (SELECT min(id) FROM ja_t))) "
							   "FROM ja_t");
	else
		appendStringInfoString(&q, "SELECT true");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * FILTER narrows the aggregate: the length of jsonb_agg(v) FILTER (WHERE v > 0)
 * equals count(*) FILTER (WHERE v > 0).  coalesce handles the all-filtered-out
 * case: with no surviving row the aggregate is NULL, coalesce(NULL, 0) = 0 =
 * the filtered count, so it passes soundly.
 */
PG_PROP(jsonb_agg_filter)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	(void) build_ja_t(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT coalesce(jsonb_array_length("
						   "jsonb_agg(v) FILTER (WHERE v > 0)), 0) "
						   "= count(*) FILTER (WHERE v > 0) FROM ja_t");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * DISTINCT de-duplicates: jsonb_array_length(jsonb_agg(DISTINCT v)) equals
 * count(DISTINCT v).  coalesce handles the empty table (NULL -> 0 = 0).
 */
PG_PROP(jsonb_agg_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	(void) build_ja_t(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT coalesce(jsonb_array_length("
						   "jsonb_agg(DISTINCT v)), 0) "
						   "= count(DISTINCT v) FROM ja_t");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Element sum equals sum(v): summing the integer elements of jsonb_agg(v)
 * equals sum(v) over the table.  Both sides coalesce to 0 on the empty table,
 * where jsonb_agg is NULL (defaulted to '[]') and sum(v) is NULL.
 */
PG_PROP(jsonb_agg_elements_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	(void) build_ja_t(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT (SELECT coalesce(sum(e::int), 0) "
						   "FROM jsonb_array_elements("
						   "coalesce((SELECT jsonb_agg(v) FROM ja_t), "
						   "'[]'::jsonb)) e) "
						   "= (SELECT coalesce(sum(v), 0) FROM ja_t)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Per-group length: within every g bucket, jsonb_array_length(jsonb_agg(v))
 * equals that group's count(*).  bool_and over the grouped subquery folds the
 * per-group checks into one boolean; coalesce(..., true) makes the no-group
 * (empty table) case pass soundly, since bool_and over 0 rows is NULL.
 */
PG_PROP(grouped_jsonb_agg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	(void) build_ja_t(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT coalesce(bool_and("
						   "jsonb_array_length(ja) = c), true) "
						   "FROM (SELECT jsonb_agg(v) ja, count(*) c "
						   "FROM ja_t GROUP BY g) s");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
