/*-------------------------------------------------------------------------
 *
 * prop_updatesem.c
 *	  Property-based tests for UPDATE statement semantics.
 *
 * UPDATE must change exactly the matching rows and exactly the named columns,
 * leaving everything else alone.  The oracle is the statement's own arithmetic
 * accounting: pre-UPDATE sums/counts are captured with scalar queries, the
 * UPDATE is run, and the post-UPDATE sums/counts are compared in C against the
 * value the semantics require (sum grows by count, a filtered subset empties,
 * a join adds the matched delta, a no-match UPDATE is a no-op, RETURNING counts
 * the matched rows).  Any disagreement is a bug in the executor's ModifyTable
 * handling, not a legal difference.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_updatesem.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "updatesem"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a scalar query expected to yield a non-NULL integer, and return it as a
 * long long.  A SQL error or a NULL result is a failure (every scalar here is
 * count()/coalesce(sum())/min() guarded to be non-NULL).
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
 * Build us_t(id int PRIMARY KEY, g int, v int) with 0..20 rows: ids are 1..n
 * assigned densely, g is a small group key in [0, 3] so the g = 0 filter lands
 * on a mix of rows and the empty boundary, and v is a small int in [-5, 5] so
 * the v > 0 filter straddles zero.  ANALYZE so the planner has stats.
 */
static void
build_us_t(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS us_t; "
						   "CREATE TABLE us_t (id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 20);
	for (int i = 1; i <= nrows; i++)
		appendStringInfo(&ddl,
						 "INSERT INTO us_t VALUES (%d, %d, %d); ",
						 i,
						 (int) pg_prop_draw_int(tc, 0, 3),
						 (int) pg_prop_draw_int(tc, -5, 5));

	appendStringInfoString(&ddl, "ANALYZE us_t; ");
	exec_ok(tc, conn, ddl.data);
	pfree(ddl.data);
}

/*
 * Make a fresh scratch copy us_copy of us_t (no PK needed for these checks),
 * dropping any leftover from an earlier case first.
 */
static void
make_copy(pg_prop_tc *tc, pg_prop_conn *conn)
{
	exec_ok(tc, conn,
			"DROP TABLE IF EXISTS us_copy; "
			"CREATE TABLE us_copy AS SELECT * FROM us_t;");
}

static void
drop_scratch(pg_prop_tc *tc, pg_prop_conn *conn)
{
	exec_ok(tc, conn,
			"DROP TABLE IF EXISTS us_copy; "
			"DROP TABLE IF EXISTS us_src; "
			"DROP TABLE IF EXISTS us_t;");
}

/*
 * 1. UPDATE copy SET v = v + 1 touches every row, so the new sum must be the
 *    old sum plus one per row: new_sum = old_sum + count(*).
 */
PG_PROP(update_all_increments)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	oldsum,
				cnt,
				newsum;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_us_t(tc, conn);
	make_copy(tc, conn);

	oldsum = scalar_int(tc, conn, "SELECT coalesce(sum(v), 0) FROM us_copy");
	cnt = scalar_int(tc, conn, "SELECT count(*) FROM us_copy");

	exec_ok(tc, conn, "UPDATE us_copy SET v = v + 1");

	newsum = scalar_int(tc, conn, "SELECT coalesce(sum(v), 0) FROM us_copy");

	if (newsum != oldsum + cnt)
		pg_prop_fail(tc,
					 "SET v = v + 1 broke sum: old=%lld count=%lld new=%lld "
					 "(expected %lld)",
					 oldsum, cnt, newsum, oldsum + cnt);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 2. UPDATE copy SET v = 0 WHERE v > 0 clears exactly the positive rows: after
 *    it, no row has v > 0, and the total row count is unchanged (UPDATE never
 *    adds or removes rows).
 */
PG_PROP(update_where_subset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	oldcnt,
				newcnt,
				pos;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_us_t(tc, conn);
	make_copy(tc, conn);

	oldcnt = scalar_int(tc, conn, "SELECT count(*) FROM us_copy");

	exec_ok(tc, conn, "UPDATE us_copy SET v = 0 WHERE v > 0");

	pos = scalar_int(tc, conn, "SELECT count(*) FROM us_copy WHERE v > 0");
	newcnt = scalar_int(tc, conn, "SELECT count(*) FROM us_copy");

	if (pos != 0)
		pg_prop_fail(tc, "WHERE v > 0 UPDATE left %lld rows with v > 0", pos);
	if (newcnt != oldcnt)
		pg_prop_fail(tc, "UPDATE changed row count: old=%lld new=%lld",
					 oldcnt, newcnt);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 3. UPDATE copy c SET v = v + s.delta FROM us_src s WHERE c.id = s.id adds the
 *    source delta to exactly the ids present in both tables.  us_src holds ids
 *    overlapping us_copy (a random subset of 1..n) with random deltas; since
 *    us_src.id is unique here, each matched row's new v = old v + delta.  The
 *    oracle: the total increase over matched rows equals sum(delta over matched
 *    ids), and unmatched rows are unchanged.  Both are captured as one scalar:
 *    sum over the join of (new v - (old v + delta)) must be 0, and the overall
 *    sum must grow by exactly the matched-delta sum.
 */
PG_PROP(update_from_join)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData src;
	long long	oldsum,
				deltasum,
				newsum,
				mismatch;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_us_t(tc, conn);
	make_copy(tc, conn);

	n = (int) scalar_int(tc, conn, "SELECT count(*) FROM us_copy");

	/*
	 * Build us_src with unique ids overlapping us_copy: for each id 1..n,
	 * include it ~half the time with a random delta.  Unique ids keep the join
	 * one-to-one so each matched row gets exactly one delta.
	 */
	initStringInfo(&src);
	appendStringInfoString(&src,
						   "DROP TABLE IF EXISTS us_src; "
						   "CREATE TABLE us_src (id int, delta int); ");
	for (int i = 1; i <= n; i++)
		if (pg_prop_draw_bool(tc))
			appendStringInfo(&src, "INSERT INTO us_src VALUES (%d, %d); ",
							 i, (int) pg_prop_draw_int(tc, -5, 5));
	appendStringInfoString(&src, "ANALYZE us_src; ");
	exec_ok(tc, conn, src.data);
	pfree(src.data);

	/* record old copy sum and the sum of deltas over ids that will match */
	oldsum = scalar_int(tc, conn, "SELECT coalesce(sum(v), 0) FROM us_copy");
	deltasum = scalar_int(tc, conn,
						  "SELECT coalesce(sum(s.delta), 0) "
						  "FROM us_src s JOIN us_copy c ON c.id = s.id");

	/*
	 * Snapshot each copy row's pre-UPDATE v keyed by id, so after the UPDATE we
	 * can check that every matched row moved by exactly its delta and no
	 * unmatched row moved at all.  us_pre lives only for this check.
	 */
	exec_ok(tc, conn,
			"DROP TABLE IF EXISTS us_pre; "
			"CREATE TABLE us_pre AS SELECT id, v AS oldv FROM us_copy;");

	exec_ok(tc, conn,
			"UPDATE us_copy c SET v = v + s.delta "
			"FROM us_src s WHERE c.id = s.id");

	newsum = scalar_int(tc, conn, "SELECT coalesce(sum(v), 0) FROM us_copy");

	/*
	 * mismatch counts rows whose post value differs from the required value:
	 * matched rows must be oldv + delta, unmatched rows must be oldv.
	 */
	mismatch = scalar_int(tc, conn,
						  "SELECT count(*) "
						  "FROM us_copy c "
						  "JOIN us_pre p ON p.id = c.id "
						  "LEFT JOIN us_src s ON s.id = c.id "
						  "WHERE c.v <> p.oldv + coalesce(s.delta, 0)");

	exec_ok(tc, conn, "DROP TABLE IF EXISTS us_pre;");

	if (mismatch != 0)
		pg_prop_fail(tc,
					 "UPDATE ... FROM moved %lld rows by the wrong amount",
					 mismatch);
	if (newsum != oldsum + deltasum)
		pg_prop_fail(tc,
					 "UPDATE ... FROM broke sum: old=%lld deltasum=%lld "
					 "new=%lld (expected %lld)",
					 oldsum, deltasum, newsum, oldsum + deltasum);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 4. UPDATE copy SET v = v + 1, g = g + 10 changes both named columns on every
 *    row: new sum(v) = old sum(v) + count, and new sum(g) = old sum(g) +
 *    10*count.  Verifying both together confirms a multi-target SET applies
 *    every assignment.
 */
PG_PROP(update_multiple_set)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	oldv,
				oldg,
				cnt,
				newv,
				newg;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_us_t(tc, conn);
	make_copy(tc, conn);

	oldv = scalar_int(tc, conn, "SELECT coalesce(sum(v), 0) FROM us_copy");
	oldg = scalar_int(tc, conn, "SELECT coalesce(sum(g), 0) FROM us_copy");
	cnt = scalar_int(tc, conn, "SELECT count(*) FROM us_copy");

	exec_ok(tc, conn, "UPDATE us_copy SET v = v + 1, g = g + 10");

	newv = scalar_int(tc, conn, "SELECT coalesce(sum(v), 0) FROM us_copy");
	newg = scalar_int(tc, conn, "SELECT coalesce(sum(g), 0) FROM us_copy");

	if (newv != oldv + cnt)
		pg_prop_fail(tc,
					 "multi-SET v wrong: old=%lld count=%lld new=%lld "
					 "(expected %lld)",
					 oldv, cnt, newv, oldv + cnt);
	if (newg != oldg + 10 * cnt)
		pg_prop_fail(tc,
					 "multi-SET g wrong: old=%lld count=%lld new=%lld "
					 "(expected %lld)",
					 oldg, cnt, newg, oldg + 10 * cnt);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 5. UPDATE copy SET v = 999 WHERE id = -1 matches nothing (ids are 1..n), so
 *    it is a no-op: both count(*) and sum(v) are unchanged.
 */
PG_PROP(update_no_match_noop)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	oldcnt,
				oldsum,
				newcnt,
				newsum;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_us_t(tc, conn);
	make_copy(tc, conn);

	oldcnt = scalar_int(tc, conn, "SELECT count(*) FROM us_copy");
	oldsum = scalar_int(tc, conn, "SELECT coalesce(sum(v), 0) FROM us_copy");

	exec_ok(tc, conn, "UPDATE us_copy SET v = 999 WHERE id = (-1)");

	newcnt = scalar_int(tc, conn, "SELECT count(*) FROM us_copy");
	newsum = scalar_int(tc, conn, "SELECT coalesce(sum(v), 0) FROM us_copy");

	if (newcnt != oldcnt)
		pg_prop_fail(tc, "no-match UPDATE changed count: old=%lld new=%lld",
					 oldcnt, newcnt);
	if (newsum != oldsum)
		pg_prop_fail(tc, "no-match UPDATE changed sum: old=%lld new=%lld",
					 oldsum, newsum);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 6. WITH u AS (UPDATE copy SET v = v WHERE g = 0 RETURNING id) SELECT count(*)
 *    FROM u returns exactly one id per updated row, so the count must equal the
 *    pre-UPDATE count of g = 0 rows.  (v = v keeps values stable so the test
 *    isolates the RETURNING row count from any value change.)
 */
PG_PROP(update_returning_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	preg0,
				returned;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_us_t(tc, conn);
	make_copy(tc, conn);

	preg0 = scalar_int(tc, conn, "SELECT count(*) FROM us_copy WHERE g = 0");

	returned = scalar_int(tc, conn,
						  "WITH u AS (UPDATE us_copy SET v = v WHERE g = 0 "
						  "RETURNING id) SELECT count(*) FROM u");

	if (returned != preg0)
		pg_prop_fail(tc,
					 "RETURNING count %lld != pre-count of g=0 rows %lld",
					 returned, preg0);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}
