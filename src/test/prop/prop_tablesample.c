/*-------------------------------------------------------------------------
 *
 * prop_tablesample.c
 *	  Property-based tests for TABLESAMPLE and sampling invariants.
 *
 * The oracle is the definition of sampling itself: a sample is a subset of
 * the table (never invents rows), BERNOULLI(100) keeps every row and
 * BERNOULLI(0) keeps none, a REPEATABLE(seed) sample is deterministic across
 * two evaluations, and no sample is larger than the table.  REPEATABLE(seed)
 * is used everywhere so each run is reproducible; a failing "t" oracle means
 * the sampling method (TsmRoutine) returned a row not in the table, dropped
 * or duplicated rows at the 0/100 boundaries, or was non-deterministic for a
 * fixed seed -- any of which is a bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_tablesample.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "tablesample"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the query errored or produced NULL, which is itself a
 * failure, reported with the SQL.
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
 * Build ts_t(id int PRIMARY KEY, v int) with a FIXED number of rows: n is
 * drawn once (5..40) so each case has a deterministic, non-empty table.  ids
 * are 1..n and v = id * 2.  pg_prop_fail on any DDL/DML error.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 5, 40);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ts_t; "
						   "CREATE TABLE ts_t(id int PRIMARY KEY, v int); "
						   "INSERT INTO ts_t "
						   "SELECT g, g * 2 FROM generate_series(1, ");
	appendStringInfo(&ddl, "%d", n);
	appendStringInfoString(&ddl, ") g; ANALYZE ts_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
	if (err)
		pg_free(err);
}

/*
 * A BERNOULLI sample is a subset of the table: no sampled id is absent from
 * the table.  REPEATABLE(42) fixes the draw.
 */
PG_PROP(bernoulli_subset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT NOT EXISTS ("
				 "SELECT id FROM ts_t TABLESAMPLE BERNOULLI(50) REPEATABLE(42) "
				 "EXCEPT SELECT id FROM ts_t)");

	pg_prop_disconnect(conn);
}

/*
 * A SYSTEM sample is likewise a subset: every sampled id is in the table.
 */
PG_PROP(system_subset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT NOT EXISTS ("
				 "SELECT id FROM ts_t TABLESAMPLE SYSTEM(50) REPEATABLE(42) "
				 "EXCEPT SELECT id FROM ts_t)");

	pg_prop_disconnect(conn);
}

/*
 * BERNOULLI(100) returns every row: the sample count equals the table count.
 */
PG_PROP(sample_100_is_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI(100) "
				 "REPEATABLE(1)) = (SELECT count(*) FROM ts_t)");

	pg_prop_disconnect(conn);
}

/*
 * BERNOULLI(0) returns no rows: the sample count is zero.
 */
PG_PROP(sample_0_is_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI(0) "
				 "REPEATABLE(1)) = 0");

	pg_prop_disconnect(conn);
}

/*
 * REPEATABLE(seed) is deterministic: the same BERNOULLI(50) REPEATABLE(7)
 * sample twice yields the same id set (each direction of EXCEPT is empty).
 */
PG_PROP(repeatable_deterministic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT NOT EXISTS ("
				 "(SELECT id FROM ts_t TABLESAMPLE BERNOULLI(50) REPEATABLE(7)) "
				 "EXCEPT "
				 "(SELECT id FROM ts_t TABLESAMPLE BERNOULLI(50) REPEATABLE(7))) "
				 "AND NOT EXISTS ("
				 "(SELECT id FROM ts_t TABLESAMPLE BERNOULLI(50) REPEATABLE(7)) "
				 "EXCEPT "
				 "(SELECT id FROM ts_t TABLESAMPLE BERNOULLI(50) REPEATABLE(7)))");

	pg_prop_disconnect(conn);
}

/*
 * No sample is larger than the table: a BERNOULLI(37) count is <= the total.
 */
PG_PROP(sample_count_le_total)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM ts_t TABLESAMPLE BERNOULLI(37) "
				 "REPEATABLE(3)) <= (SELECT count(*) FROM ts_t)");

	pg_prop_disconnect(conn);
}
