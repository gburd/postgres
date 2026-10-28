/*-------------------------------------------------------------------------
 *
 * prop_mvcc.c
 *	  Property-based tests for MVCC snapshot/visibility invariants.
 *
 * The oracle is MVCC's own transaction semantics driven over a single
 * connection with explicit transactions: a ROLLBACK must restore the prior
 * state, a COMMIT must persist it, a ROLLBACK TO savepoint must undo only the
 * work after the savepoint, a backend sees its own uncommitted writes, and a
 * REPEATABLE READ snapshot is stable across statements.  Any count or value
 * that does not match the transaction's defined outcome is a visibility bug.
 *
 * These are single-session properties: multi-session concurrency cannot be
 * driven deterministically from one connection, so each property exercises
 * the snapshot/visibility machinery a single backend relies on.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_mvcc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "mvcc"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Build mv_t(id int PRIMARY KEY, v int) fresh with a random number of rows in
 * [0, 20].  ids are the contiguous run 1..n so a property can pick an existing
 * id deterministically; v is a small signed int.  Rebuilt in every property so
 * no state leaks between cases.  Returns the row count built.
 */
static int
build_mv_t(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, 20);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS mv_t; "
						   "CREATE TABLE mv_t (id int PRIMARY KEY, v int); ");
	for (int i = 1; i <= n; i++)
		appendStringInfo(&ddl, "INSERT INTO mv_t VALUES (%d, %d); ",
						 i, (int) pg_prop_draw_int(tc, -5, 5));
	appendStringInfoString(&ddl, "ANALYZE mv_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build mv_t failed: %s", err ? err : "(no message)");

	pfree(ddl.data);
	return n;
}

/* Run SQL for side effects; a failure is a harness/setup bug, so fail hard. */
static void
exec_ok(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "exec failed: %s\n  SQL: %s", err ? err : "", sql);
}

/* Read a scalar integer; a NULL or error is a bug (these queries never NULL). */
static long
scalar_int(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;
	long		val;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	val = atol(res);
	pg_free(res);
	return val;
}

/* count(*) of mv_t. */
static long
count_mv_t(pg_prop_tc *tc, pg_prop_conn *conn)
{
	return scalar_int(tc, conn, "SELECT count(*) FROM mv_t");
}

/*
 * require_true (as in prop_numeric.c): a boolean SELECT must return "t"; a
 * NULL or SQL error is a failure.
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
 * ROLLBACK restores the row count.  Capture c0; BEGIN; INSERT k rows (k in
 * 0..5, with ids above the built range so no PK collision); the in-xact count
 * must be c0+k; ROLLBACK; the count must be c0 again.  A backend's own
 * uncommitted inserts are visible to itself, and ROLLBACK must discard them.
 */
PG_PROP(rollback_restores_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	int			k;
	long		c0;
	long		cmid;
	long		cafter;
	StringInfoData ins;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_mv_t(tc, conn);
	c0 = count_mv_t(tc, conn);
	k = (int) pg_prop_draw_int(tc, 0, 5);

	exec_ok(tc, conn, "BEGIN");
	initStringInfo(&ins);
	for (int i = 0; i < k; i++)
		appendStringInfo(&ins, "INSERT INTO mv_t VALUES (%d, 0); ", n + 1000 + i);
	if (ins.len > 0)
		exec_ok(tc, conn, ins.data);
	pfree(ins.data);

	cmid = count_mv_t(tc, conn);
	if (cmid != c0 + k)
	{
		exec_ok(tc, conn, "ROLLBACK");
		pg_prop_fail(tc, "in-xact count %ld != c0+k %ld (c0=%ld k=%d)",
					 cmid, c0 + k, c0, k);
	}

	exec_ok(tc, conn, "ROLLBACK");
	cafter = count_mv_t(tc, conn);
	if (cafter != c0)
		pg_prop_fail(tc, "post-rollback count %ld != c0 %ld", cafter, c0);

	pg_prop_disconnect(conn);
}

/*
 * COMMIT persists a delete.  Capture c0; BEGIN; DELETE one existing row (if
 * any); COMMIT; the count must be c0-1 when the table was non-empty, or c0
 * (== 0) when it was empty and no delete ran.
 */
PG_PROP(commit_persists_delete)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long		c0;
	long		cafter;
	long		expected;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_mv_t(tc, conn);
	c0 = count_mv_t(tc, conn);

	exec_ok(tc, conn, "BEGIN");
	if (c0 > 0)
	{
		exec_ok(tc, conn,
				"DELETE FROM mv_t WHERE id = (SELECT min(id) FROM mv_t)");
		expected = c0 - 1;
	}
	else
		expected = c0;			/* empty table: nothing to delete */
	exec_ok(tc, conn, "COMMIT");

	cafter = count_mv_t(tc, conn);
	if (cafter != expected)
		pg_prop_fail(tc, "post-commit count %ld != expected %ld (c0=%ld)",
					 cafter, expected, c0);

	pg_prop_disconnect(conn);
}

/*
 * Savepoint partial rollback.  BEGIN; INSERT A; SAVEPOINT s; INSERT B;
 * ROLLBACK TO s; COMMIT.  Only A survives, so the net change is +1.  ROLLBACK
 * TO must undo exactly the work after the savepoint, not before it.
 */
PG_PROP(savepoint_partial_rollback)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	long		c0;
	long		cafter;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_mv_t(tc, conn);
	c0 = count_mv_t(tc, conn);

	exec_ok(tc, conn, "BEGIN");
	{
		StringInfoData s;

		initStringInfo(&s);
		/* A persists; B is rolled back to the savepoint. */
		appendStringInfo(&s, "INSERT INTO mv_t VALUES (%d, 1); ", n + 2001);
		appendStringInfoString(&s, "SAVEPOINT s; ");
		appendStringInfo(&s, "INSERT INTO mv_t VALUES (%d, 2); ", n + 2002);
		appendStringInfoString(&s, "ROLLBACK TO s;");
		exec_ok(tc, conn, s.data);
		pfree(s.data);
	}
	exec_ok(tc, conn, "COMMIT");

	cafter = count_mv_t(tc, conn);
	if (cafter != c0 + 1)
		pg_prop_fail(tc, "post-savepoint count %ld != c0+1 %ld (c0=%ld)",
					 cafter, c0 + 1, c0);

	pg_prop_disconnect(conn);
}

/*
 * A backend sees its own in-xact UPDATE, and ROLLBACK restores the old value.
 * BEGIN; read old v; UPDATE v = v + 100 on an existing id; the same xact must
 * now read old+100; ROLLBACK; the value is back to old.  Vacuously passes when
 * the table is empty (nothing to update).
 */
PG_PROP(self_update_visible_in_xact)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	long		vold;
	long		vmid;
	long		vrestored;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_mv_t(tc, conn);
	if (n == 0)
	{
		/* no row to update: the property is vacuously satisfied */
		pg_prop_disconnect(conn);
		return;
	}

	vold = scalar_int(tc, conn, "SELECT v FROM mv_t WHERE id = 1");

	exec_ok(tc, conn, "BEGIN");
	exec_ok(tc, conn, "UPDATE mv_t SET v = v + 100 WHERE id = 1");
	vmid = scalar_int(tc, conn, "SELECT v FROM mv_t WHERE id = 1");
	if (vmid != vold + 100)
	{
		exec_ok(tc, conn, "ROLLBACK");
		pg_prop_fail(tc, "in-xact v %ld != old+100 %ld (old=%ld)",
					 vmid, vold + 100, vold);
	}
	exec_ok(tc, conn, "ROLLBACK");

	vrestored = scalar_int(tc, conn, "SELECT v FROM mv_t WHERE id = 1");
	if (vrestored != vold)
		pg_prop_fail(tc, "post-rollback v %ld != old %ld", vrestored, vold);

	pg_prop_disconnect(conn);
}

/*
 * A REPEATABLE READ snapshot is stable across statements.  BEGIN ISOLATION
 * LEVEL REPEATABLE READ; read the count twice; the two reads must agree.
 * Single-session this is trivially true, but it exercises the RR snapshot
 * path: a RR xact takes one snapshot at first query and reuses it.
 */
PG_PROP(repeatable_read_stable)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long		c1;
	long		c2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_mv_t(tc, conn);

	exec_ok(tc, conn, "BEGIN ISOLATION LEVEL REPEATABLE READ");
	c1 = count_mv_t(tc, conn);
	c2 = count_mv_t(tc, conn);
	exec_ok(tc, conn, "COMMIT");

	if (c1 != c2)
		pg_prop_fail(tc, "RR count unstable: c1 %ld != c2 %ld", c1, c2);

	pg_prop_disconnect(conn);
}

/*
 * count(*) is independent of row ordering.  Shape A: the plain count equals
 * the count of a subquery that imposes an ORDER BY.  Ordering is a
 * presentation concern and must never change the cardinality a snapshot sees.
 */
PG_PROP(count_consistent_across_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_mv_t(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM mv_t) "
				 "= (SELECT count(*) FROM (SELECT * FROM mv_t ORDER BY v) s)");

	pg_prop_disconnect(conn);
}
