/*-------------------------------------------------------------------------
 *
 * prop_jsonagg.c
 *	  Property-based tests for the JSON aggregate functions.
 *
 * The JSON aggregates build composite JSON values from rows: json_agg and
 * jsonb_agg collect their argument into an array, one element per row in
 * aggregation order, while json_object_agg and jsonb_object_agg collect
 * key/value pairs into an object, one member per row.  These properties
 * check the structural relationships that must hold between the aggregated
 * result and the rows it was built from -- the array length (or object member
 * count) equals the row count, a key lookup recovers the row that supplied it,
 * and unnesting the aggregated array recovers the original multiset of values
 * -- plus the agree-with-jsonb identity for the json (text) variants.  All run
 * over a small generated table ja_t(id int, k text, v int).
 *
 * The oracle is the table itself: count(*), to_jsonb() on a single row, and
 * array_agg() of the raw column values are computed directly from the rows
 * and compared against what the JSON aggregate produced, so the aggregate's
 * answer is checked against the source data it summarised rather than against
 * a second copy of itself.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonagg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonagg"
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
 * Build the generated table ja_t(id int, k text, v int) with n rows, where n
 * is drawn in [1, 15].  Row i (1-based) has id = i, a DISTINCT key
 * 'k'||(i-1) (so 'k0'..'k(n-1)'), and a small integer v.  Keys are distinct
 * by construction, so jsonb_object_agg(k, v) has exactly one member per row
 * with no dedup.  n >= 1 guarantees 'k0' exists for the lookup property.
 *
 * Returns the row count n (always >= 1).  pg_prop_fail()s on any DDL/DML
 * error, since a failure to build the fixture is not something the property
 * can meaningfully proceed past.
 */
static int
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			n;

	/* n >= 1 so key 'k0' always exists (jsonb_object_agg_lookup needs it). */
	n = (int) pg_prop_draw_int(tc, 1, 15);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ja_t; "
						   "CREATE TABLE ja_t (id int, k text, v int); ");

	for (int i = 1; i <= n; i++)
	{
		int			v = (int) pg_prop_draw_int(tc, -8, 8);

		/* id = i, k = 'k'||(i-1) distinct per row, v a small int */
		appendStringInfo(&ddl,
						 "INSERT INTO ja_t VALUES (%d, 'k%d', %d); ",
						 i, i - 1, v);
	}

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
	{
		pg_prop_fail(tc, "failed to build ja_t: %s\n  ddl: %s",
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

	if (!pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS ja_t", &err))
	{
		if (err)
			pg_free(err);
	}
}

/*
 * jsonb_agg(v) produces an array with exactly one element per row.  Over no
 * rows the aggregate is SQL NULL, so coalesce the array length to 0; the
 * table always has >= 1 row here, but the coalesce keeps the invariant stated
 * for the empty-table case too.
 */
PG_PROP(jsonb_agg_length_matches_rows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(jsonb_array_length(jsonb_agg(v)), 0) "
				 "= (SELECT count(*) FROM ja_t) FROM ja_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * jsonb_object_agg(k, v) with DISTINCT keys has exactly one member per row:
 * counting the members (jsonb_each over the object) equals the row count.
 * Because the keys are distinct by construction there is no dedup, so no row
 * can be collapsed away.
 */
PG_PROP(jsonb_object_agg_key_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce("
				 "(SELECT count(*) FROM jsonb_each(jsonb_object_agg(k, v))), 0) "
				 "= (SELECT count(*) FROM ja_t) FROM ja_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * A key lookup recovers the row that supplied it: for the key 'k0' (always
 * present, since the table has >= 1 row), the aggregated object's 'k0' member
 * equals that row's v rendered as jsonb.  IS NOT DISTINCT FROM so the compare
 * is well-defined even if the value were NULL.
 */
PG_PROP(jsonb_object_agg_lookup)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (jsonb_object_agg(k, v) -> 'k0') "
				 "IS NOT DISTINCT FROM "
				 "(SELECT to_jsonb(v) FROM ja_t WHERE k = 'k0') FROM ja_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * json_agg and jsonb_agg agree on value: the json (text) variant, cast to
 * jsonb, equals the jsonb variant.  json preserves textual form while jsonb
 * normalises, but once json_agg's result is cast to jsonb the two must be
 * identical.  Both aggregate ORDER BY id so element order matches, and both
 * coalesce to 'null'::jsonb over no rows.
 */
PG_PROP(json_agg_matches_jsonb_agg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(json_agg(v ORDER BY id)::jsonb, 'null'::jsonb) "
				 "= coalesce(jsonb_agg(v ORDER BY id), 'null'::jsonb) FROM ja_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * Unnesting the aggregated array recovers the row values: the multiset of
 * elements in jsonb_agg(v) equals the multiset of v values from the rows.
 * Both sides are sorted into an int[] before comparing, so order does not
 * matter and duplicates are preserved.  Both coalesce to '{}'::int[] over no
 * rows.
 */
PG_PROP(jsonb_agg_elements_match_rows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce("
				 "(SELECT array_agg(e::int ORDER BY e::int) "
				 "FROM jsonb_array_elements(jsonb_agg(v)) e), '{}'::int[]) "
				 "= coalesce("
				 "(SELECT array_agg(v ORDER BY v) FROM ja_t), '{}'::int[]) "
				 "FROM ja_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * jsonb_agg(v ORDER BY id) preserves row order: the first element of the
 * aggregated array is the v of the smallest-id row.  IS NOT DISTINCT FROM for
 * a NULL-safe compare, and coalesce to true over no rows (where there is no
 * first element to check).
 */
PG_PROP(jsonb_agg_order_preserved)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce("
				 "(jsonb_agg(v ORDER BY id) -> 0) IS NOT DISTINCT FROM "
				 "(SELECT to_jsonb(v) FROM ja_t ORDER BY id LIMIT 1), true) "
				 "FROM ja_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}
