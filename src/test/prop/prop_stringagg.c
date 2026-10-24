/*-------------------------------------------------------------------------
 *
 * prop_stringagg.c
 *	  Property-based tests for the string_agg and array_agg aggregates.
 *
 * string_agg and array_agg collect ordered input; these check the agg/unnest
 * and agg/split inverses and length relationships over a generated table.
 *
 * The generated table sa_t(id int, v text) holds 0..20 rows with id = 1..n
 * (unique) and v a short [a-z] string containing no comma, so the comma-join
 * / string_to_array split round-trips exactly and every aggregate ordered BY
 * id is deterministic.  The oracle is each query's reference form: a split of
 * the joined aggregate against the array aggregate, cardinality against
 * count(*), string length against the sum of value lengths, an in-aggregate
 * ORDER BY against an externally-ordered aggregate, string_agg's NULL-skipping
 * against an explicit NOT NULL filter, and DISTINCT cardinality bounded by the
 * total.  IS NOT DISTINCT FROM / coalesce(...,0) make the empty table (all
 * aggregates NULL / arrays NULL) pass.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_stringagg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "stringagg"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the aggregate errored or produced NULL where the property
 * expected a boolean, which is itself a bug -- so either is a failure,
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
 * Append a short random [a-z] string literal (1..5 chars, no comma) to buf,
 * single-quoted for use as a text value.
 */
static void
append_value_literal(pg_prop_tc *tc, StringInfo buf)
{
	int			len = (int) pg_prop_draw_int(tc, 1, 5);

	appendStringInfoChar(buf, '\'');
	for (int i = 0; i < len; i++)
		appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 25)));
	appendStringInfoChar(buf, '\'');
}

/*
 * Build (and first drop) the table sa_t(id int, v text) with n rows, id =
 * 1..n assigned explicitly and v a short non-NULL [a-z] string.  Fails the
 * property if any DDL/DML errors.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn, int n)
{
	StringInfoData ddl;
	char	   *err = NULL;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS sa_t; "
						   "CREATE TABLE sa_t (id int, v text); ");

	for (int i = 1; i <= n; i++)
	{
		appendStringInfo(&ddl, "INSERT INTO sa_t VALUES (%d, ", i);
		append_value_literal(tc, &ddl);
		appendStringInfoString(&ddl, "); ");
	}

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s", err ? err : "(unknown)");

	pfree(ddl.data);
}

/*
 * Build the table with some NULL v rows for the NULL-skipping property.  id =
 * 1..n; v is NULL ~30% of the time, otherwise a short [a-z] string.
 */
static void
build_table_with_nulls(pg_prop_tc *tc, pg_prop_conn *conn, int n)
{
	StringInfoData ddl;
	char	   *err = NULL;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS sa_t; "
						   "CREATE TABLE sa_t (id int, v text); ");

	for (int i = 1; i <= n; i++)
	{
		appendStringInfo(&ddl, "INSERT INTO sa_t VALUES (%d, ", i);
		if (pg_prop_draw_int(tc, 0, 2) == 0)
			appendStringInfoString(&ddl, "NULL");
		else
			append_value_literal(tc, &ddl);
		appendStringInfoString(&ddl, "); ");
	}

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s", err ? err : "(unknown)");

	pfree(ddl.data);
}

/* Drop the generated table. */
static void
drop_table(pg_prop_conn *conn)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS sa_t", &err) && err)
		pg_free(err);
}

/*
 * The comma-join aggregate split back on ',' recovers the ordered values.
 * Because v contains no comma, string_to_array(string_agg(v, ',' ...), ',')
 * must equal array_agg(v ...); both are NULL on the empty table, so compare
 * with IS NOT DISTINCT FROM.
 */
PG_PROP(string_agg_split_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 20);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT string_to_array(string_agg(v, ',' ORDER BY id), ',') "
				 "IS NOT DISTINCT FROM array_agg(v ORDER BY id) "
				 "FROM sa_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * array_agg produces one element per row: cardinality(array_agg(v)) equals
 * count(*).  coalesce(...,0) handles the empty table, where the array is NULL.
 */
PG_PROP(array_agg_unnest_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 20);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT coalesce(cardinality(array_agg(v ORDER BY id)), 0) "
				 "= (SELECT count(*) FROM sa_t) FROM sa_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * The length of the empty-delimiter string_agg equals the sum of the value
 * lengths: concatenation adds no separators, so length(string_agg(v, ''))
 * equals sum(length(v)).  coalesce(...,0) handles the empty table.
 */
PG_PROP(string_agg_length_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 20);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT coalesce(length(string_agg(v, '' ORDER BY id)), 0) "
				 "= coalesce((SELECT sum(length(v)) FROM sa_t), 0) FROM sa_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * An in-aggregate ORDER BY matches an externally-ordered aggregate:
 * array_agg(v ORDER BY id) equals array_agg(v2) over a subquery already
 * ordered by id.  Both NULL on the empty table, so IS NOT DISTINCT FROM.
 */
PG_PROP(array_agg_order_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 20);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT array_agg(v ORDER BY id) IS NOT DISTINCT FROM "
				 "(SELECT array_agg(v2) FROM "
				 "(SELECT v AS v2 FROM sa_t ORDER BY id) s) "
				 "FROM sa_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * string_agg ignores NULL inputs: over a table that includes NULL v rows, the
 * aggregate over all rows equals the aggregate restricted to the non-NULL
 * rows.  Both NULL when every row is NULL / the table is empty, so IS NOT
 * DISTINCT FROM.
 */
PG_PROP(string_agg_skips_nulls)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 20);
	build_table_with_nulls(tc, conn, n);

	require_true(tc, conn,
				 "SELECT string_agg(v, ',' ORDER BY id) IS NOT DISTINCT FROM "
				 "(SELECT string_agg(v, ',' ORDER BY id) FROM sa_t "
				 "WHERE v IS NOT NULL) FROM sa_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * The distinct aggregate is no larger than the full one:
 * cardinality(array_agg(DISTINCT v)) <= cardinality(array_agg(v)).
 * coalesce(...,0) handles the empty table, where both arrays are NULL.
 */
PG_PROP(array_agg_distinct_le_total)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 20);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT coalesce(cardinality(array_agg(DISTINCT v)), 0) "
				 "<= coalesce(cardinality(array_agg(v)), 0) FROM sa_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}
