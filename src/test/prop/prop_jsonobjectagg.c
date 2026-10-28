/*-------------------------------------------------------------------------
 *
 * prop_jsonobjectagg.c
 *	  Property-based tests for jsonb_object_agg and json_object_agg.
 *
 * The object aggregates collect key/value pairs from rows into a JSON object,
 * one member per row.  Over a table whose keys are DISTINCT by construction
 * (k0..k(n-1)), there is no key collision, so every row contributes exactly
 * one member.  These properties check that structural fact against the source
 * rows -- member count, key count, value sum, a key lookup, the empty-input
 * NULL, a FILTER restriction -- plus the json(text)==jsonb agreement.  Any
 * mismatch means the aggregate summarised the rows wrongly, which is a bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonobjectagg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonobjectagg"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the aggregate errored or produced NULL where the property
 * expects a definite true/false, which is itself a bug -- so either is a
 * failure, reported with the SQL.  Mirrors require_true() in prop_numeric.c.
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
 * Build jo_t(id int PRIMARY KEY, k text, v int) with n rows, n drawn in
 * [0, 15].  Row i (1-based) has id = i, a DISTINCT key 'k'||(i-1) (so
 * 'k0'..'k(n-1)'), and a small integer v.  Keys are distinct by construction,
 * so an object aggregate has exactly one member per row with no dedup.  n may
 * be 0 (empty table) to exercise the empty-input case; the properties coalesce
 * for that.  ANALYZE after load.  This table name is distinct from the sibling
 * jsonagg areas' ja_t so the areas never collide.
 *
 * Returns the row count n.  pg_prop_fail()s on any DDL/DML error, since a
 * failure to build the fixture is not something the property can proceed past.
 */
static int
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			n;

	n = (int) pg_prop_draw_int(tc, 0, 15);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS jo_t; "
						   "CREATE TABLE jo_t (id int PRIMARY KEY, k text, v int); ");

	for (int i = 1; i <= n; i++)
	{
		int			v = (int) pg_prop_draw_int(tc, -8, 8);

		/* id = i, k = 'k'||(i-1) distinct per row, v a small int */
		appendStringInfo(&ddl,
						 "INSERT INTO jo_t VALUES (%d, 'k%d', %d); ",
						 i, i - 1, v);
	}

	appendStringInfoString(&ddl, "ANALYZE jo_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
	{
		pg_prop_fail(tc, "failed to build jo_t: %s\n  ddl: %s",
					 err ? err : "(no message)", ddl.data);
	}

	pfree(ddl.data);
	return n;
}

/* Drop the generated table.  Best-effort; ignores any error. */
static void
drop_table(pg_prop_conn *conn)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS jo_t", &err))
	{
		if (err)
			pg_free(err);
	}
}

/*
 * object_agg_member_count: jsonb_object_agg(k, v) with DISTINCT keys has
 * exactly one member per row, so counting its members (jsonb_each over the
 * object) equals the row count.  Over no rows the aggregate is SQL NULL;
 * coalesce it to '{}'::jsonb so the empty case counts 0 members and still
 * holds.
 */
PG_PROP(object_agg_member_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM jsonb_each("
				 "coalesce((SELECT jsonb_object_agg(k, v) FROM jo_t), "
				 "'{}'::jsonb))) "
				 "= (SELECT count(*) FROM jo_t)");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * object_agg_lookup: a key lookup recovers the row that supplied it.  For key
 * 'k0', the aggregated object's 'k0' member equals that row's v rendered as
 * jsonb -- but only when 'k0' exists (the table may be empty).  The CASE
 * guards on existence: with no 'k0' row the property is vacuously true.
 */
PG_PROP(object_agg_lookup)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN EXISTS (SELECT 1 FROM jo_t WHERE k = 'k0') "
				 "THEN (SELECT jsonb_object_agg(k, v) FROM jo_t) -> 'k0' "
				 "IS NOT DISTINCT FROM "
				 "(SELECT to_jsonb(v) FROM jo_t WHERE k = 'k0') "
				 "ELSE true END");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * json_eq_jsonb: the json(text) variant cast to jsonb equals the jsonb
 * variant -- the two aggregates build the same object, differing only in
 * textual vs normalised form, which the cast erases.  Over no rows both
 * aggregates are SQL NULL; coalesce both sides so the empty case compares
 * '{}'::jsonb = '{}'::jsonb and holds.
 */
PG_PROP(json_eq_jsonb)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce((SELECT json_object_agg(k, v) FROM jo_t)::jsonb, "
				 "'{}'::jsonb) "
				 "= coalesce((SELECT jsonb_object_agg(k, v) FROM jo_t), "
				 "'{}'::jsonb)");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * object_agg_keys_match: the number of keys in jsonb_object_agg(k, v) equals
 * the number of distinct k values in the table.  With DISTINCT keys by
 * construction count(DISTINCT k) = count(*), so no key is dropped by the
 * aggregate's last-wins dedup.  coalesce to '{}'::jsonb over no rows (0 keys).
 */
PG_PROP(object_agg_keys_match)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM jsonb_object_keys("
				 "coalesce((SELECT jsonb_object_agg(k, v) FROM jo_t), "
				 "'{}'::jsonb))) "
				 "= (SELECT count(DISTINCT k) FROM jo_t)");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * object_agg_values_sum: the sum of the values in jsonb_object_agg(k, v)
 * equals sum(v) over the rows.  Keys are distinct so every v survives as a
 * member; summing the member values (jsonb_each_text, cast to int) recovers
 * the row sum.  coalesce both the object (empty -> '{}') and the sums (empty
 * aggregate -> 0) so the empty case is 0 = 0.
 */
PG_PROP(object_agg_values_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT coalesce(sum(value::int), 0) "
				 "FROM jsonb_each_text("
				 "coalesce((SELECT jsonb_object_agg(k, v) FROM jo_t), "
				 "'{}'::jsonb))) "
				 "= (SELECT coalesce(sum(v), 0) FROM jo_t)");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * empty_object_agg_null: an object aggregate over zero rows is SQL NULL.  A
 * WHERE false scan feeds the aggregate no rows regardless of the generated
 * table contents, so the result must be NULL.
 */
PG_PROP(empty_object_agg_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT jsonb_object_agg(k, v) FROM jo_t WHERE false) "
				 "IS NULL");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * object_agg_filter: jsonb_object_agg(k, v) FILTER (WHERE v > 0) builds an
 * object only from the positive-v rows, so its member count equals the number
 * of rows with v > 0.  coalesce the filtered aggregate to '{}'::jsonb for the
 * case where no row has v > 0 (the FILTER then yields SQL NULL).
 */
PG_PROP(object_agg_filter)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM jsonb_each("
				 "coalesce((SELECT jsonb_object_agg(k, v) FILTER (WHERE v > 0) "
				 "FROM jo_t), '{}'::jsonb))) "
				 "= (SELECT count(*) FROM jo_t WHERE v > 0)");

	drop_table(conn);
	pg_prop_disconnect(conn);
}
