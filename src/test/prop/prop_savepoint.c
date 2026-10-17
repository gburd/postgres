/*-------------------------------------------------------------------------
 *
 * prop_savepoint.c
 *	  Property-based tests for savepoints and subtransaction rollback.
 *
 * The oracle is PostgreSQL's own subtransaction semantics driven over a
 * single connection with explicit transactions and savepoints: ROLLBACK TO
 * must undo exactly the work done since the named savepoint (and nothing
 * before it), RELEASE must keep that work, a whole-transaction ROLLBACK must
 * discard everything, and after an error inside a subtransaction a ROLLBACK TO
 * must recover the session so later statements commit.  Any net row-count
 * delta that disagrees with the defined subtransaction outcome is a bug.
 *
 * This is distinct from the MVCC visibility properties: here the oracle is the
 * nesting/undo behaviour of subtransactions, not snapshot visibility.  Every
 * property captures the count before BEGIN, runs a fixed savepoint sequence,
 * captures the count after COMMIT/ROLLBACK, and compares the delta in C.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_savepoint.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "savepoint"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Build sv_t(id int PRIMARY KEY, v int) fresh with a random number of rows in
 * [0, 15].  ids are the contiguous run 1..n; v is a small signed int.  The
 * savepoint sequences below use ids at 1000+ offsets so no insert collides
 * with the built rows.  Rebuilt in every property so no state leaks between
 * cases.
 */
static void
build_sv_t(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, 15);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS sv_t; "
						   "CREATE TABLE sv_t (id int PRIMARY KEY, v int); ");
	for (int i = 1; i <= n; i++)
		appendStringInfo(&ddl, "INSERT INTO sv_t VALUES (%d, %d); ",
						 i, (int) pg_prop_draw_int(tc, -5, 5));
	appendStringInfoString(&ddl, "ANALYZE sv_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build sv_t failed: %s", err ? err : "(no message)");

	pfree(ddl.data);
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

/* count(*) of sv_t. */
static long
count_sv_t(pg_prop_tc *tc, pg_prop_conn *conn)
{
	return scalar_int(tc, conn, "SELECT count(*) FROM sv_t");
}

/*
 * ROLLBACK TO undoes work after the savepoint, keeps work before it.
 * BEGIN; INSERT A (1001); SAVEPOINT s1; INSERT B (1002); ROLLBACK TO s1;
 * COMMIT.  Only A survives: net +1.
 */
PG_PROP(savepoint_rollback_partial)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long		c0;
	long		cafter;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_sv_t(tc, conn);
	c0 = count_sv_t(tc, conn);

	exec_ok(tc, conn, "BEGIN");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (1001, 1)");
	exec_ok(tc, conn, "SAVEPOINT s1");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (1002, 2)");
	exec_ok(tc, conn, "ROLLBACK TO s1");
	exec_ok(tc, conn, "COMMIT");

	cafter = count_sv_t(tc, conn);
	if (cafter != c0 + 1)
		pg_prop_fail(tc, "net delta %ld != +1 (c0=%ld after=%ld)",
					 cafter - c0, c0, cafter);

	exec_ok(tc, conn, "DELETE FROM sv_t WHERE id >= 1000");
	pg_prop_disconnect(conn);
}

/*
 * Nested savepoints: ROLLBACK TO the inner savepoint undoes only its work.
 * BEGIN; SAVEPOINT s1; INSERT (2001); SAVEPOINT s2; INSERT (2002);
 * ROLLBACK TO s2; INSERT (2003); COMMIT.  2001 and 2003 survive, 2002 is
 * undone: net +2.
 */
PG_PROP(nested_savepoints)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long		c0;
	long		cafter;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_sv_t(tc, conn);
	c0 = count_sv_t(tc, conn);

	exec_ok(tc, conn, "BEGIN");
	exec_ok(tc, conn, "SAVEPOINT s1");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (2001, 1)");
	exec_ok(tc, conn, "SAVEPOINT s2");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (2002, 2)");
	exec_ok(tc, conn, "ROLLBACK TO s2");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (2003, 3)");
	exec_ok(tc, conn, "COMMIT");

	cafter = count_sv_t(tc, conn);
	if (cafter != c0 + 2)
		pg_prop_fail(tc, "net delta %ld != +2 (c0=%ld after=%ld)",
					 cafter - c0, c0, cafter);

	exec_ok(tc, conn, "DELETE FROM sv_t WHERE id >= 1000");
	pg_prop_disconnect(conn);
}

/*
 * RELEASE keeps the subtransaction's work.  BEGIN; INSERT (3001);
 * SAVEPOINT s1; INSERT (3002); RELEASE s1; COMMIT.  Both rows survive: net +2.
 * RELEASE merges the subtransaction up, it does not undo it.
 */
PG_PROP(release_savepoint_keeps)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long		c0;
	long		cafter;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_sv_t(tc, conn);
	c0 = count_sv_t(tc, conn);

	exec_ok(tc, conn, "BEGIN");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (3001, 1)");
	exec_ok(tc, conn, "SAVEPOINT s1");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (3002, 2)");
	exec_ok(tc, conn, "RELEASE s1");
	exec_ok(tc, conn, "COMMIT");

	cafter = count_sv_t(tc, conn);
	if (cafter != c0 + 2)
		pg_prop_fail(tc, "net delta %ld != +2 (c0=%ld after=%ld)",
					 cafter - c0, c0, cafter);

	exec_ok(tc, conn, "DELETE FROM sv_t WHERE id >= 1000");
	pg_prop_disconnect(conn);
}

/*
 * A savepoint is re-usable after ROLLBACK TO: the savepoint name survives the
 * rollback and work can continue past it.  BEGIN; INSERT (4001); SAVEPOINT s1;
 * INSERT (4002); ROLLBACK TO s1; INSERT (4003); COMMIT.  4001 and 4003
 * survive, 4002 is undone: net +2.
 */
PG_PROP(rollback_to_rereleases)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long		c0;
	long		cafter;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_sv_t(tc, conn);
	c0 = count_sv_t(tc, conn);

	exec_ok(tc, conn, "BEGIN");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (4001, 1)");
	exec_ok(tc, conn, "SAVEPOINT s1");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (4002, 2)");
	exec_ok(tc, conn, "ROLLBACK TO s1");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (4003, 3)");
	exec_ok(tc, conn, "COMMIT");

	cafter = count_sv_t(tc, conn);
	if (cafter != c0 + 2)
		pg_prop_fail(tc, "net delta %ld != +2 (c0=%ld after=%ld)",
					 cafter - c0, c0, cafter);

	exec_ok(tc, conn, "DELETE FROM sv_t WHERE id >= 1000");
	pg_prop_disconnect(conn);
}

/*
 * A whole-transaction ROLLBACK discards everything, savepoints included.
 * BEGIN; INSERT (5001); SAVEPOINT s1; INSERT (5002); ROLLBACK.  Nothing
 * persists: net 0.
 */
PG_PROP(full_rollback)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long		c0;
	long		cafter;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_sv_t(tc, conn);
	c0 = count_sv_t(tc, conn);

	exec_ok(tc, conn, "BEGIN");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (5001, 1)");
	exec_ok(tc, conn, "SAVEPOINT s1");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (5002, 2)");
	exec_ok(tc, conn, "ROLLBACK");

	cafter = count_sv_t(tc, conn);
	if (cafter != c0)
		pg_prop_fail(tc, "net delta %ld != 0 (c0=%ld after=%ld)",
					 cafter - c0, c0, cafter);

	exec_ok(tc, conn, "DELETE FROM sv_t WHERE id >= 1000");
	pg_prop_disconnect(conn);
}

/*
 * After an error inside a subtransaction, ROLLBACK TO recovers the session.
 * BEGIN; INSERT (6001); SAVEPOINT s1; INSERT a duplicate id (6001) to raise a
 * PK violation (which aborts the subtransaction); ROLLBACK TO s1 recovers;
 * INSERT (6002); COMMIT.  6001 and 6002 survive, the errored insert is
 * undone: net +2.  The duplicate INSERT's exec returns false; that is
 * expected, so continue past it to ROLLBACK TO.
 */
PG_PROP(subxact_error_rollback)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	long		c0;
	long		cafter;
	bool		dup_ok;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_sv_t(tc, conn);
	c0 = count_sv_t(tc, conn);

	exec_ok(tc, conn, "BEGIN");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (6001, 1)");
	exec_ok(tc, conn, "SAVEPOINT s1");

	/* Duplicate PK: this MUST fail, aborting the subtransaction. */
	err = NULL;
	dup_ok = pg_prop_sql_exec(conn, "INSERT INTO sv_t VALUES (6001, 9)", &err);
	if (dup_ok)
	{
		exec_ok(tc, conn, "ROLLBACK");
		pg_prop_fail(tc, "duplicate PK insert unexpectedly succeeded");
	}
	if (err)
		pg_free(err);

	/* ROLLBACK TO must recover the aborted subtransaction. */
	exec_ok(tc, conn, "ROLLBACK TO s1");
	exec_ok(tc, conn, "INSERT INTO sv_t VALUES (6002, 2)");
	exec_ok(tc, conn, "COMMIT");

	cafter = count_sv_t(tc, conn);
	if (cafter != c0 + 2)
		pg_prop_fail(tc, "net delta %ld != +2 (c0=%ld after=%ld)",
					 cafter - c0, c0, cafter);

	exec_ok(tc, conn, "DELETE FROM sv_t WHERE id >= 1000");
	pg_prop_disconnect(conn);
}
