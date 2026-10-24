/*-------------------------------------------------------------------------
 *
 * prop_returning.c
 *	  Property-based tests for data-modifying RETURNING, ON CONFLICT and MERGE.
 *
 * INSERT/UPDATE/DELETE ... RETURNING must emit exactly one output row per row
 * the statement actually inserted, updated or deleted; ON CONFLICT must either
 * update or skip the conflicting row (never a second insert); and a MERGE whose
 * source matches every target row must update every row and insert none.  The
 * oracle is the statement's own row accounting: the RETURNING count is compared
 * (in C) against an independently computed count of the affected rows, and the
 * post-state (row counts, specific values) is checked with scalar booleans.
 * Any disagreement is a bug in the executor's ModifyTable row handling, not a
 * legal difference.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_returning.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "returning"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Scalar boolean oracle: run a "SELECT <bool>" and require "t".  A NULL result
 * or a SQL error is itself a failure.  (Copied from prop_numeric.c.)
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
 * Run a scalar query expected to yield a non-NULL integer, and return it as a
 * long long.  A SQL error or a NULL result is a failure (every count query
 * here is count(*)/a scalar that is never NULL).
 */
static long long
scalar_int(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;
	long long	val;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	val = atoll(res);
	pg_free(res);
	if (err)
		pg_free(err);
	return val;
}

/*
 * Run a side-effecting statement (CREATE/DROP/DML) and fail on error.
 */
static void
exec_ok(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "exec failed: %s\n  SQL: %s", err ? err : "", sql);
	if (err)
		pg_free(err);
}

/*
 * Build rt_t(id int PRIMARY KEY, v int) with 0..20 rows: ids are 1..n assigned
 * densely (so they never collide with the generate_series ids offset by 1000),
 * and v is a small int in [-5, 5] so the v>0 / v<0 filters each land on a mix
 * of rows and the empty boundary.  ANALYZE so the planner has stats.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS rt_t; "
						   "CREATE TABLE rt_t (id int PRIMARY KEY, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 20);
	for (int id = 1; id <= nrows; id++)
	{
		int			v = (int) pg_prop_draw_int(tc, -5, 5);

		appendStringInfo(&ddl, "INSERT INTO rt_t VALUES (%d, %d); ", id, v);
	}

	appendStringInfoString(&ddl, "ANALYZE rt_t;");

	exec_ok(tc, conn, ddl.data);
	pfree(ddl.data);
}

/* Create a fresh scratch copy rt_c of rt_t (same columns + PK). */
static void
make_scratch(pg_prop_tc *tc, pg_prop_conn *conn)
{
	exec_ok(tc, conn,
			"DROP TABLE IF EXISTS rt_c; "
			"CREATE TABLE rt_c AS SELECT * FROM rt_t; "
			"ALTER TABLE rt_c ADD PRIMARY KEY (id);");
}

static void
drop_scratch(pg_prop_tc *tc, pg_prop_conn *conn)
{
	exec_ok(tc, conn, "DROP TABLE rt_c;");
}

/*
 * 1. INSERT ... RETURNING emits one row per inserted row.  Insert k new rows
 * (ids offset by 1000 so they never collide with the 1..20 ids) and count the
 * RETURNING output via a top-level data-modifying CTE; the count must equal k.
 */
PG_PROP(insert_returning_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			k;
	long long	returned;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	k = (int) pg_prop_draw_int(tc, 1, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "WITH ins AS ("
					 "INSERT INTO rt_c SELECT g + 1000, g FROM generate_series(1, %d) g "
					 "RETURNING id) SELECT count(*) FROM ins",
					 k);
	returned = scalar_int(tc, conn, q.data);
	pfree(q.data);

	if (returned != (long long) k)
		pg_prop_fail(tc, "INSERT RETURNING count %lld != k %d", returned, k);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 2. UPDATE ... RETURNING emits one row per updated row.  The RETURNING count
 * must equal the number of rows matching the WHERE filter, counted first.
 */
PG_PROP(update_returning_matches_filter)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	pre;
	long long	returned;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	pre = scalar_int(tc, conn, "SELECT count(*) FROM rt_c WHERE v > 0");
	returned = scalar_int(tc, conn,
						  "WITH u AS ("
						  "UPDATE rt_c SET v = v + 1 WHERE v > 0 RETURNING id) "
						  "SELECT count(*) FROM u");

	if (returned != pre)
		pg_prop_fail(tc, "UPDATE RETURNING count %lld != matching rows %lld",
					 returned, pre);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 3. DELETE ... RETURNING emits one row per deleted row.  The RETURNING count
 * must equal the number of rows matching the WHERE filter, counted first.
 */
PG_PROP(delete_returning_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	pre;
	long long	returned;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	pre = scalar_int(tc, conn, "SELECT count(*) FROM rt_c WHERE v < 0");
	returned = scalar_int(tc, conn,
						  "WITH d AS ("
						  "DELETE FROM rt_c WHERE v < 0 RETURNING id) "
						  "SELECT count(*) FROM d");

	if (returned != pre)
		pg_prop_fail(tc, "DELETE RETURNING count %lld != matching rows %lld",
					 returned, pre);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 4. INSERT ... ON CONFLICT DO UPDATE either updates the conflicting row or
 * inserts a new one, never both.  If rt_c has rows, INSERT a value colliding
 * with an existing id: the row count is unchanged and that row's v becomes 999.
 * If rt_c is empty, there is no existing id to collide with, so a fresh insert
 * grows the count by exactly 1 and the new row's v is 999.
 */
PG_PROP(upsert_insert_or_update)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	long long	pre;
	long long	target;
	long long	post;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	pre = scalar_int(tc, conn, "SELECT count(*) FROM rt_c");

	if (pre > 0)
	{
		/* Collide with the smallest existing id. */
		target = scalar_int(tc, conn, "SELECT min(id) FROM rt_c");

		initStringInfo(&q);
		appendStringInfo(&q,
						 "INSERT INTO rt_c VALUES (%lld, 999) "
						 "ON CONFLICT (id) DO UPDATE SET v = 999",
						 target);
		exec_ok(tc, conn, q.data);
		resetStringInfo(&q);

		post = scalar_int(tc, conn, "SELECT count(*) FROM rt_c");
		if (post != pre)
			pg_prop_fail(tc, "upsert-update changed row count %lld -> %lld",
						 pre, post);

		appendStringInfo(&q, "SELECT v = 999 FROM rt_c WHERE id = %lld", target);
		require_true(tc, conn, q.data);
		pfree(q.data);
	}
	else
	{
		/* Empty table: no conflict, a plain insert grows the count by 1. */
		exec_ok(tc, conn,
				"INSERT INTO rt_c VALUES (1, 999) "
				"ON CONFLICT (id) DO UPDATE SET v = 999");

		post = scalar_int(tc, conn, "SELECT count(*) FROM rt_c");
		if (post != pre + 1)
			pg_prop_fail(tc, "upsert-insert row count %lld -> %lld (expected +1)",
						 pre, post);

		require_true(tc, conn, "SELECT v = 999 FROM rt_c WHERE id = 1");
	}

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 5. INSERT ... ON CONFLICT DO NOTHING leaves a conflicting row entirely
 * unchanged: both the row count and that row's v are the same afterward.  When
 * rt_c is empty there is no row to conflict with, so this property only applies
 * to the non-empty case; an empty table is discarded.
 */
PG_PROP(upsert_do_nothing)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	long long	pre;
	long long	target;
	long long	oldv;
	long long	post;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	pre = scalar_int(tc, conn, "SELECT count(*) FROM rt_c");
	pg_prop_assume(tc, pre > 0);

	target = scalar_int(tc, conn, "SELECT min(id) FROM rt_c");

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT v FROM rt_c WHERE id = %lld", target);
	oldv = scalar_int(tc, conn, q.data);
	resetStringInfo(&q);

	appendStringInfo(&q,
					 "INSERT INTO rt_c VALUES (%lld, 999) "
					 "ON CONFLICT (id) DO NOTHING",
					 target);
	exec_ok(tc, conn, q.data);
	resetStringInfo(&q);

	post = scalar_int(tc, conn, "SELECT count(*) FROM rt_c");
	if (post != pre)
		pg_prop_fail(tc, "DO NOTHING changed row count %lld -> %lld", pre, post);

	appendStringInfo(&q, "SELECT v = %lld FROM rt_c WHERE id = %lld",
					 oldv, target);
	require_true(tc, conn, q.data);
	pfree(q.data);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 6. MERGE whose source matches every target row updates every row and inserts
 * none.  rt_c starts as an exact copy of rt_t, so a MERGE keyed on id matches
 * every row: WHEN MATCHED bumps v by 1, WHEN NOT MATCHED never fires.  After
 * the merge, every row's v equals its original v + 1 and the row count is
 * unchanged.  bool_and over 0 rows is NULL, so coalesce to true for the empty
 * case (which holds vacuously).
 */
PG_PROP(merge_matches_manual)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	pre;
	long long	post;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	pre = scalar_int(tc, conn, "SELECT count(*) FROM rt_c");

	exec_ok(tc, conn,
			"MERGE INTO rt_c t USING (SELECT id, v FROM rt_t) s ON t.id = s.id "
			"WHEN MATCHED THEN UPDATE SET v = s.v + 1 "
			"WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.v)");

	post = scalar_int(tc, conn, "SELECT count(*) FROM rt_c");
	if (post != pre)
		pg_prop_fail(tc, "MERGE changed row count %lld -> %lld", pre, post);

	/* Every row's v is its original (from rt_t) + 1; vacuously true if empty. */
	require_true(tc, conn,
				 "SELECT coalesce(bool_and(c.v = o.v + 1), true) "
				 "FROM rt_c c JOIN rt_t o USING (id)");

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}
