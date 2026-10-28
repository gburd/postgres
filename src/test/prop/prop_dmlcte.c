/*-------------------------------------------------------------------------
 *
 * prop_dmlcte.c
 *	  Property-based tests for data-modifying CTE semantics.
 *
 * A WITH clause may contain INSERT/UPDATE/DELETE ... RETURNING subqueries
 * whose effects feed the rest of the statement.  Two things make these
 * distinct from plain CTEs and plain RETURNING: all data-modifying CTEs in one
 * statement run against the single snapshot taken before the statement began,
 * so a sibling read of the modified table does NOT see the modifications; and
 * one data-modifying CTE's RETURNING rows can drive another (chained
 * INSERT ... SELECT FROM <delete-cte>).  The oracle is the statement's own row
 * accounting -- counts compared in C against independently computed
 * pre-statement counts -- plus the snapshot rule checked directly.  Any
 * disagreement is a bug in the executor's ModifyTable / CTE handling, not a
 * legal difference.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_dmlcte.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "dmlcte"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a scalar query expected to yield a non-NULL integer, and return it as a
 * long long.  A SQL error or a NULL result is a failure (every count query
 * here is count()/min() guarded to be non-NULL).  (Copied from
 * prop_returning.c.)
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
 * Build dc_t(id int PRIMARY KEY, v int) with 0..20 rows: ids are 1..n assigned
 * densely so the id+1000 / id+2000 offsets used by the insert CTEs never
 * collide with them, and v is a small int in [-5, 5] so the v < 0 and v >= 0
 * filters each land on a mix of rows and the empty boundary.  ANALYZE so the
 * planner has stats.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS dc_t; "
						   "CREATE TABLE dc_t (id int PRIMARY KEY, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 20);
	for (int id = 1; id <= nrows; id++)
	{
		int			v = (int) pg_prop_draw_int(tc, -5, 5);

		appendStringInfo(&ddl, "INSERT INTO dc_t VALUES (%d, %d); ", id, v);
	}

	appendStringInfoString(&ddl, "ANALYZE dc_t;");

	exec_ok(tc, conn, ddl.data);
	pfree(ddl.data);
}

/*
 * Create a fresh scratch copy dc_copy of dc_t.  CREATE TABLE AS has no primary
 * key, so add one explicitly; none of these properties strictly need it, but
 * it mirrors dc_t and makes an accidental duplicate id an error rather than
 * silent corruption of the count oracle.
 */
static void
make_scratch(pg_prop_tc *tc, pg_prop_conn *conn)
{
	exec_ok(tc, conn,
			"DROP TABLE IF EXISTS dc_copy; "
			"CREATE TABLE dc_copy AS SELECT * FROM dc_t; "
			"ALTER TABLE dc_copy ADD PRIMARY KEY (id);");
}

static void
drop_scratch(pg_prop_tc *tc, pg_prop_conn *conn)
{
	exec_ok(tc, conn, "DROP TABLE dc_copy;");
}

/*
 * 1. Two chained data-modifying CTEs: a DELETE feeds an INSERT.  The DELETE
 * removes every v < 0 row and RETURNING its ids; the INSERT consumes those ids
 * (offset by 1000 so they cannot collide with the surviving rows) and
 * RETURNING its own ids; the outer SELECT counts the insert's output.  The
 * executor must insert exactly as many rows as the delete removed, so the
 * insert count equals the pre-statement count of v < 0 rows, verified in C.
 */
PG_PROP(two_modifying_ctes)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	pre_neg;
	long long	inserted;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	pre_neg = scalar_int(tc, conn, "SELECT count(*) FROM dc_copy WHERE v < 0");

	inserted = scalar_int(tc, conn,
						  "WITH d AS ("
						  "DELETE FROM dc_copy WHERE v < 0 RETURNING id), "
						  "i AS ("
						  "INSERT INTO dc_copy SELECT id + 1000, 0 FROM d "
						  "RETURNING id) "
						  "SELECT count(*) FROM i");

	if (inserted != pre_neg)
		pg_prop_fail(tc,
					 "chained INSERT count %lld != DELETE count (v<0) %lld",
					 inserted, pre_neg);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 2. A data-modifying CTE runs against the pre-statement snapshot, so a
 * sibling read in the same statement does NOT see its effects.  The DELETE
 * empties dc_copy, but the outer SELECT count(*) FROM dc_copy -- reading the
 * same snapshot the statement started with -- still sees every original row.
 * So the statement result equals the original count (verified in C), and only
 * a FRESH statement afterward sees the table emptied (0 rows).
 */
PG_PROP(cte_sees_snapshot)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	orig;
	long long	seen;
	long long	after;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	orig = scalar_int(tc, conn, "SELECT count(*) FROM dc_copy");

	/* Outer read runs on the pre-DELETE snapshot: it still sees orig rows. */
	seen = scalar_int(tc, conn,
					  "WITH d AS (DELETE FROM dc_copy RETURNING id) "
					  "SELECT count(*) FROM dc_copy");
	if (seen != orig)
		pg_prop_fail(tc,
					 "snapshot read saw %lld rows, expected pre-DELETE %lld",
					 seen, orig);

	/* A fresh statement sees the committed effect: the table is empty. */
	after = scalar_int(tc, conn, "SELECT count(*) FROM dc_copy");
	if (after != 0)
		pg_prop_fail(tc, "after the WITH, table has %lld rows, expected 0",
					 after);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 3. UPDATE ... RETURNING in a CTE returns the NEW column values.  The UPDATE
 * adds 100 to v on every v >= 0 row and RETURNING v; those returned values are
 * the post-update values, so each is >= 100.  Checked as min(v) >= 100 over
 * the CTE output, but only when at least one v >= 0 row existed -- otherwise
 * the UPDATE touches no rows, the CTE is empty and min() is NULL, which is not
 * a counterexample.
 */
PG_PROP(update_returning_new)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	pre_nonneg;
	long long	minv;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	pre_nonneg = scalar_int(tc, conn,
							"SELECT count(*) FROM dc_copy WHERE v >= 0");
	pg_prop_assume(tc, pre_nonneg > 0);

	minv = scalar_int(tc, conn,
					  "WITH u AS ("
					  "UPDATE dc_copy SET v = v + 100 WHERE v >= 0 "
					  "RETURNING v) "
					  "SELECT min(v) FROM u");

	if (minv < 100)
		pg_prop_fail(tc,
					 "UPDATE RETURNING min(v) %lld < 100 (not the new values)",
					 minv);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 4. INSERT ... SELECT ... RETURNING in a CTE emits one row per inserted row.
 * The INSERT copies every dc_t row (ids offset by 2000 so they cannot collide
 * with dc_copy's 1..20 ids) and RETURNING a constant; the outer SELECT counts
 * that output.  The count must equal the number of source rows, dc_t's row
 * count, verified in C.
 */
PG_PROP(insert_select_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	src;
	long long	inserted;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	src = scalar_int(tc, conn, "SELECT count(*) FROM dc_t");

	inserted = scalar_int(tc, conn,
						  "WITH i AS ("
						  "INSERT INTO dc_copy SELECT id + 2000, v FROM dc_t "
						  "RETURNING 1) "
						  "SELECT count(*) FROM i");

	if (inserted != src)
		pg_prop_fail(tc, "INSERT SELECT count %lld != source rows %lld",
					 inserted, src);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * 5. DELETE ... RETURNING in a CTE returns one row per deleted row, and the
 * delete removes every row.  The outer SELECT counts the CTE output: it must
 * equal the original row count (verified in C), and a fresh read afterward
 * must see the table empty (also verified in C).
 */
PG_PROP(delete_all_returning)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long long	orig;
	long long	deleted;
	long long	after;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	make_scratch(tc, conn);

	orig = scalar_int(tc, conn, "SELECT count(*) FROM dc_copy");

	deleted = scalar_int(tc, conn,
						 "WITH d AS (DELETE FROM dc_copy RETURNING id) "
						 "SELECT count(*) FROM d");
	if (deleted != orig)
		pg_prop_fail(tc, "DELETE RETURNING count %lld != original rows %lld",
					 deleted, orig);

	after = scalar_int(tc, conn, "SELECT count(*) FROM dc_copy");
	if (after != 0)
		pg_prop_fail(tc, "after DELETE, table has %lld rows, expected 0",
					 after);

	drop_scratch(tc, conn);
	pg_prop_disconnect(conn);
}
