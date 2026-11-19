/*-------------------------------------------------------------------------
 *
 * prop_mergecmd.c
 *	  Property-based tests for MERGE command semantics (not RETURNING).
 *
 * MERGE joins a target and a source on a key and fires WHEN MATCHED /
 * WHEN NOT MATCHED actions.  The oracle here is the post-MERGE state: after a
 * MATCHED UPDATE every matched target row must carry the source value; after a
 * NOT MATCHED INSERT every source key must be present; after a full MERGE the
 * row count must equal |target-keys-before UNION source-keys|; a MATCHED DELETE
 * must remove exactly the matched rows; and a MERGE over an empty source must
 * leave the target untouched.  Each is computed independently (in SQL via
 * bool_and over the join, or in C from counts read with scalar queries) and
 * compared to the state MERGE actually produced.  Any disagreement is a bug in
 * the executor's MERGE row handling, not a legal difference.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_mergecmd.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "mergecmd"
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
 * here is a count(*) that is never NULL).
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
 * Build the target mc_tgt(id int PRIMARY KEY, v int) and source
 * mc_src(id int PRIMARY KEY, v int), each with 0..15 rows and ids 1..n.  The
 * two id ranges are drawn independently so they overlap partially: some ids
 * are in both (MATCHED), some only in the target, some only in the source
 * (NOT MATCHED).  v is small ([-5, 5]) and differs between the two tables so a
 * MATCHED UPDATE that copies s.v is observable.  ANALYZE for planner stats.
 */
static void
build_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	int			ntgt;
	int			nsrc;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS mc_tgt; "
						   "DROP TABLE IF EXISTS mc_src; "
						   "CREATE TABLE mc_tgt (id int PRIMARY KEY, v int); "
						   "CREATE TABLE mc_src (id int PRIMARY KEY, v int); ");

	ntgt = (int) pg_prop_draw_int(tc, 0, 15);
	for (int id = 1; id <= ntgt; id++)
		appendStringInfo(&ddl, "INSERT INTO mc_tgt VALUES (%d, %d); ",
						 id, (int) pg_prop_draw_int(tc, -5, 5));

	nsrc = (int) pg_prop_draw_int(tc, 0, 15);
	for (int id = 1; id <= nsrc; id++)
		appendStringInfo(&ddl, "INSERT INTO mc_src VALUES (%d, %d); ",
						 id, (int) pg_prop_draw_int(tc, -5, 5));

	appendStringInfoString(&ddl, "ANALYZE mc_tgt; ANALYZE mc_src;");

	exec_ok(tc, conn, ddl.data);
	pfree(ddl.data);
}

/* Create a fresh scratch copy mc_cp of mc_tgt (same columns + PK). */
static void
make_scratch(pg_prop_tc *tc, pg_prop_conn *conn)
{
	exec_ok(tc, conn,
			"DROP TABLE IF EXISTS mc_cp; "
			"CREATE TABLE mc_cp AS SELECT * FROM mc_tgt; "
			"ALTER TABLE mc_cp ADD PRIMARY KEY (id);");
}

static void
drop_scratch(pg_prop_tc *tc, pg_prop_conn *conn)
{
	exec_ok(tc, conn, "DROP TABLE mc_cp;");
}

/*
 * 1. WHEN MATCHED THEN UPDATE SET v = s.v copies the source value onto every
 * matched target row.  Afterward, joining the copy back to the source on id,
 * every joined row must satisfy copy.v = source.v.  bool_and over 0 joined
 * rows is NULL (no match at all), so coalesce to true -- that case holds
 * vacuously.
 */
PG_PROP(merge_matched_updates)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	make_scratch(tc, conn);

	exec_ok(tc, conn,
			"MERGE INTO mc_cp t USING mc_src s ON t.id = s.id "
			"WHEN MATCHED THEN UPDATE SET v = s.v");

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(c.v = s.v), true) "
				 "FROM mc_cp c JOIN mc_src s USING (id)");

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 2. WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v) adds every source
 * row whose id is absent from the target.  Afterward every source id must be
 * present in the copy.  bool_and over an empty source is NULL, so coalesce to
 * true -- the no-source case holds vacuously.
 */
PG_PROP(merge_not_matched_inserts)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	make_scratch(tc, conn);

	exec_ok(tc, conn,
			"MERGE INTO mc_cp t USING mc_src s ON t.id = s.id "
			"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)");

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(EXISTS "
				 "(SELECT 1 FROM mc_cp c WHERE c.id = s.id)), true) "
				 "FROM mc_src s");

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 3. A full MERGE (MATCHED UPDATE + NOT MATCHED INSERT) neither drops nor
 * duplicates keys: the copy's row count afterward must equal the number of
 * distinct ids in (copy-before UNION source).  The union count is read before
 * the MERGE (while the copy still holds its original ids) and compared in C to
 * the post-MERGE row count.
 */
PG_PROP(merge_both_clauses_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	expected;
	long long	post;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	make_scratch(tc, conn);

	expected = scalar_int(tc, conn,
						  "SELECT count(*) FROM "
						  "(SELECT id FROM mc_cp UNION SELECT id FROM mc_src) u");

	exec_ok(tc, conn,
			"MERGE INTO mc_cp t USING mc_src s ON t.id = s.id "
			"WHEN MATCHED THEN UPDATE SET v = s.v "
			"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)");

	post = scalar_int(tc, conn, "SELECT count(*) FROM mc_cp");
	if (post != expected)
		pg_prop_fail(tc,
					 "MERGE row count %lld != |copy UNION source| %lld",
					 post, expected);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 4. WHEN MATCHED THEN DELETE removes exactly the matched rows: the copy count
 * afterward must equal the count before minus the number of ids present in
 * both the copy and the source.  Both the pre-count and the matched-id count
 * are read before the MERGE and the arithmetic is checked in C.
 */
PG_PROP(merge_delete)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	pre;
	long long	matched;
	long long	post;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	make_scratch(tc, conn);

	pre = scalar_int(tc, conn, "SELECT count(*) FROM mc_cp");
	matched = scalar_int(tc, conn,
						 "SELECT count(*) FROM mc_cp c "
						 "WHERE EXISTS (SELECT 1 FROM mc_src s WHERE s.id = c.id)");

	exec_ok(tc, conn,
			"MERGE INTO mc_cp t USING mc_src s ON t.id = s.id "
			"WHEN MATCHED THEN DELETE");

	post = scalar_int(tc, conn, "SELECT count(*) FROM mc_cp");
	if (post != pre - matched)
		pg_prop_fail(tc,
					 "MERGE DELETE count %lld != pre %lld - matched %lld",
					 post, pre, matched);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 5. A MERGE over an empty source is a no-op: no key can match and none can be
 * NOT MATCHED (there are no source rows), so both the row count and the sum of
 * v must be unchanged.  The empty source is forced with USING (SELECT ... WHERE
 * false); the pre-state (count, coalesced sum) is captured and compared in C.
 * sum over 0 rows is NULL, so coalesce(sum, 0) keeps the empty-copy case sound.
 */
PG_PROP(merge_no_source_noop)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	pre_count;
	long long	pre_sum;
	long long	post_count;
	long long	post_sum;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	make_scratch(tc, conn);

	pre_count = scalar_int(tc, conn, "SELECT count(*) FROM mc_cp");
	pre_sum = scalar_int(tc, conn, "SELECT coalesce(sum(v), 0) FROM mc_cp");

	exec_ok(tc, conn,
			"MERGE INTO mc_cp t "
			"USING (SELECT 1 AS id, 1 AS v WHERE false) s ON t.id = s.id "
			"WHEN MATCHED THEN UPDATE SET v = s.v "
			"WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)");

	post_count = scalar_int(tc, conn, "SELECT count(*) FROM mc_cp");
	post_sum = scalar_int(tc, conn, "SELECT coalesce(sum(v), 0) FROM mc_cp");

	if (post_count != pre_count)
		pg_prop_fail(tc, "empty-source MERGE changed row count %lld -> %lld",
					 pre_count, post_count);
	if (post_sum != pre_sum)
		pg_prop_fail(tc, "empty-source MERGE changed sum(v) %lld -> %lld",
					 pre_sum, post_sum);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}
