/*-------------------------------------------------------------------------
 *
 * prop_foreignkey.c
 *	  Property-based tests for FOREIGN KEY enforcement and referential actions.
 *
 * The oracle is the referential-integrity contract itself: a FK constraint
 * must reject a child row that references a non-existent parent, must permit
 * a NULL FK column, and must apply the declared ON DELETE / ON UPDATE action
 * (CASCADE, RESTRICT, SET NULL) exactly when the referenced parent changes.
 * Each property builds a fresh parent/child pair, drives DDL/DML through
 * pg_prop_sql_exec, asserts the exec succeeds or fails as the contract
 * requires, and verifies the post-state row counts in C.  A deviation is a
 * referential-integrity bug (trigger firing, constraint check, or action
 * propagation), not a rounding artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_foreignkey.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "foreignkey"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Connect or fail.  Every property opens one connection and closes it at the
 * end; a failure/assume longjmps over the disconnect, and the driver's
 * per-case cleanup reclaims the handle.
 */
static pg_prop_conn *
connect_or_fail(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

/*
 * Run DDL/DML that MUST succeed; fail the case (with the SQL and error) if it
 * does not.  Used for setup and for operations the FK contract must allow.
 */
static void
exec_ok(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
	{
		pg_prop_fail(tc, "expected success but failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	}
	if (err)
		pg_free(err);
}

/*
 * Run DML that MUST be rejected by the FK constraint; fail the case if it
 * unexpectedly succeeds.  The error is discarded -- a rejection is the
 * expected outcome.
 */
static void
exec_rejected(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (pg_prop_sql_exec(conn, sql, &err))
	{
		pg_prop_fail(tc, "expected FK rejection but succeeded: %s", sql);
	}
	if (err)
		pg_free(err);
}

/*
 * Read a single integer count via a "SELECT count(*) ..." (never NULL: count
 * over zero rows is 0, so the scalar is always a well-defined integer).  Fail
 * the case on SQL error or NULL.
 */
static long
scalar_count(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;
	long		n;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "count query failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL count for: %s", sql);
	n = atol(res);
	pg_free(res);
	if (err)
		pg_free(err);
	return n;
}

/*
 * Build a fresh parent (fk_p) and child (fk_c) pair.  The child FK clause is
 * caller-supplied so each property declares the referential action it tests.
 * The parent id is a PRIMARY KEY so it can be referenced; the child has its
 * own serial-free id plus a pid FK column.
 */
static void
build_tables(pg_prop_tc *tc, pg_prop_conn *conn, const char *fk_clause)
{
	StringInfoData ddl;

	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "DROP TABLE IF EXISTS fk_c; "
					 "DROP TABLE IF EXISTS fk_p; "
					 "CREATE TABLE fk_p (id int PRIMARY KEY); "
					 "CREATE TABLE fk_c (id int, pid int %s); ",
					 fk_clause);
	exec_ok(tc, conn, ddl.data);
	pfree(ddl.data);
}

/* Drop both tables so a leaked schema cannot leak into the next case. */
static void
drop_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	exec_ok(tc, conn, "DROP TABLE IF EXISTS fk_c; DROP TABLE IF EXISTS fk_p;");
}

/*
 * fk_rejects_orphan: a FK rejects a child referencing a non-existent parent.
 * Insert parent id=1; inserting a child with a random pid that is NOT a parent
 * id must be rejected, while pid=1 (an existing parent) must succeed.
 */
PG_PROP(fk_rejects_orphan)
{
	pg_prop_conn *conn = connect_or_fail(tc);
	StringInfoData q;
	int			orphan;

	build_tables(tc, conn, "REFERENCES fk_p(id)");
	exec_ok(tc, conn, "INSERT INTO fk_p(id) VALUES (1)");

	/* A pid that is deliberately not the single parent id (1). */
	orphan = (int) pg_prop_draw_int(tc, 2, 1000);

	initStringInfo(&q);
	appendStringInfo(&q, "INSERT INTO fk_c(id, pid) VALUES (1, %d)", orphan);
	exec_rejected(tc, conn, q.data);
	pfree(q.data);

	/* Referencing the existing parent must be allowed. */
	exec_ok(tc, conn, "INSERT INTO fk_c(id, pid) VALUES (2, 1)");

	if (scalar_count(tc, conn, "SELECT count(*) FROM fk_c") != 1)
		pg_prop_fail(tc, "expected exactly 1 child row after orphan rejection");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * fk_allows_null: a nullable FK column accepts NULL (no parent required).
 * The parent may be empty or not; a NULL pid must insert regardless.
 */
PG_PROP(fk_allows_null)
{
	pg_prop_conn *conn = connect_or_fail(tc);

	build_tables(tc, conn, "REFERENCES fk_p(id)");

	/* Optionally populate a parent; it must not matter for a NULL FK. */
	if (pg_prop_draw_bool(tc))
		exec_ok(tc, conn, "INSERT INTO fk_p(id) VALUES (1)");

	exec_ok(tc, conn, "INSERT INTO fk_c(id, pid) VALUES (1, NULL)");

	if (scalar_count(tc, conn,
					 "SELECT count(*) FROM fk_c WHERE pid IS NULL") != 1)
		pg_prop_fail(tc, "expected 1 child row with NULL pid");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * cascade_delete: ON DELETE CASCADE removes children when the parent is
 * deleted.  Insert one parent and N (1..10) children referencing it, delete
 * the parent, and require the child count to be 0.
 */
PG_PROP(cascade_delete)
{
	pg_prop_conn *conn = connect_or_fail(tc);
	StringInfoData ins;
	int			nchild;

	build_tables(tc, conn, "REFERENCES fk_p(id) ON DELETE CASCADE");
	exec_ok(tc, conn, "INSERT INTO fk_p(id) VALUES (1)");

	nchild = (int) pg_prop_draw_int(tc, 1, 10);
	initStringInfo(&ins);
	for (int i = 0; i < nchild; i++)
		appendStringInfo(&ins, "INSERT INTO fk_c(id, pid) VALUES (%d, 1); ", i);
	exec_ok(tc, conn, ins.data);
	pfree(ins.data);

	if (scalar_count(tc, conn, "SELECT count(*) FROM fk_c") != nchild)
		pg_prop_fail(tc, "expected %d children before delete", nchild);

	exec_ok(tc, conn, "DELETE FROM fk_p WHERE id = 1");

	if (scalar_count(tc, conn, "SELECT count(*) FROM fk_c") != 0)
		pg_prop_fail(tc, "ON DELETE CASCADE left children behind");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * restrict_delete: ON DELETE RESTRICT blocks deleting a referenced parent.
 * With at least one child referencing the parent, DELETE of the parent must
 * be rejected and the parent must still be present.
 */
PG_PROP(restrict_delete)
{
	pg_prop_conn *conn = connect_or_fail(tc);
	StringInfoData ins;
	int			nchild;

	build_tables(tc, conn, "REFERENCES fk_p(id) ON DELETE RESTRICT");
	exec_ok(tc, conn, "INSERT INTO fk_p(id) VALUES (1)");

	nchild = (int) pg_prop_draw_int(tc, 1, 10);
	initStringInfo(&ins);
	for (int i = 0; i < nchild; i++)
		appendStringInfo(&ins, "INSERT INTO fk_c(id, pid) VALUES (%d, 1); ", i);
	exec_ok(tc, conn, ins.data);
	pfree(ins.data);

	exec_rejected(tc, conn, "DELETE FROM fk_p WHERE id = 1");

	if (scalar_count(tc, conn, "SELECT count(*) FROM fk_p WHERE id = 1") != 1)
		pg_prop_fail(tc, "ON DELETE RESTRICT allowed the parent to be deleted");
	if (scalar_count(tc, conn, "SELECT count(*) FROM fk_c") != nchild)
		pg_prop_fail(tc, "children changed despite blocked parent delete");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * set_null_delete: ON DELETE SET NULL nulls the child FK when the parent is
 * deleted.  After deleting the parent, every child must survive with pid NULL.
 */
PG_PROP(set_null_delete)
{
	pg_prop_conn *conn = connect_or_fail(tc);
	StringInfoData ins;
	int			nchild;

	build_tables(tc, conn, "REFERENCES fk_p(id) ON DELETE SET NULL");
	exec_ok(tc, conn, "INSERT INTO fk_p(id) VALUES (1)");

	nchild = (int) pg_prop_draw_int(tc, 1, 10);
	initStringInfo(&ins);
	for (int i = 0; i < nchild; i++)
		appendStringInfo(&ins, "INSERT INTO fk_c(id, pid) VALUES (%d, 1); ", i);
	exec_ok(tc, conn, ins.data);
	pfree(ins.data);

	exec_ok(tc, conn, "DELETE FROM fk_p WHERE id = 1");

	/* All children survive, all with pid now NULL. */
	if (scalar_count(tc, conn, "SELECT count(*) FROM fk_c") != nchild)
		pg_prop_fail(tc, "ON DELETE SET NULL removed children (expected %d)",
					 nchild);
	if (scalar_count(tc, conn,
					 "SELECT count(*) FROM fk_c WHERE pid IS NULL") != nchild)
		pg_prop_fail(tc, "ON DELETE SET NULL did not null all child pids");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * cascade_update: ON UPDATE CASCADE propagates a parent id change to the
 * children.  Insert parent id=1 and children referencing it, update the
 * parent id to a fresh value, and require every child's pid to follow.
 */
PG_PROP(cascade_update)
{
	pg_prop_conn *conn = connect_or_fail(tc);
	StringInfoData ins;
	StringInfoData q;
	int			nchild;
	int			newid;

	build_tables(tc, conn, "REFERENCES fk_p(id) ON UPDATE CASCADE");
	exec_ok(tc, conn, "INSERT INTO fk_p(id) VALUES (1)");

	nchild = (int) pg_prop_draw_int(tc, 1, 10);
	initStringInfo(&ins);
	for (int i = 0; i < nchild; i++)
		appendStringInfo(&ins, "INSERT INTO fk_c(id, pid) VALUES (%d, 1); ", i);
	exec_ok(tc, conn, ins.data);
	pfree(ins.data);

	/* A new parent id distinct from the old one (1). */
	newid = (int) pg_prop_draw_int(tc, 2, 1000);

	initStringInfo(&q);
	appendStringInfo(&q, "UPDATE fk_p SET id = %d WHERE id = 1", newid);
	exec_ok(tc, conn, q.data);
	pfree(q.data);

	/* No child should still reference the old id. */
	if (scalar_count(tc, conn, "SELECT count(*) FROM fk_c WHERE pid = 1") != 0)
		pg_prop_fail(tc, "ON UPDATE CASCADE left a child on the old parent id");

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT count(*) FROM fk_c WHERE pid = %d", newid);
	if (scalar_count(tc, conn, q.data) != nchild)
		pg_prop_fail(tc, "ON UPDATE CASCADE did not move all %d children to %d",
					 nchild, newid);
	pfree(q.data);

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}
