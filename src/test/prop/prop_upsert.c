/*-------------------------------------------------------------------------
 *
 * prop_upsert.c
 *	  Property-based tests for INSERT ... ON CONFLICT (upsert) semantics.
 *
 * ON CONFLICT must behave as a deterministic upsert: DO NOTHING leaves a
 * conflicting row untouched (count and value unchanged); DO UPDATE SET
 * v = EXCLUDED.v writes the proposed value; an EXCLUDED expression sees the
 * row the statement proposed to insert; a conflict WHERE clause gates the
 * update on the existing row; a brand-new key is inserted (count + 1); and a
 * single statement that proposes the same conflicting key twice is an error
 * ("ON CONFLICT DO UPDATE command cannot affect row a second time").  Each
 * post-state is read back with scalar queries and the invariant is checked in
 * C.  Any disagreement is a bug in the executor's ON CONFLICT handling, not a
 * legal difference.  This area is distinct from prop_returning (RETURNING row
 * accounting) and prop_mergecmd (MERGE); it only exercises the upsert state
 * transition.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_upsert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "upsert"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Read a single integer scalar; fail the case on SQL error or NULL (every
 * query here is COUNT(*) or a value from a known-present row, so neither is
 * expected).  Returns the parsed long.
 */
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
	if (err)
		pg_free(err);
	return val;
}

/*
 * Build up_copy(id int PRIMARY KEY, v int) as a copy of a freshly generated
 * up_t with 0..15 rows (ids 0..N-1, v drawn from a small signed domain that
 * includes negatives so the conflict-WHERE branch can be exercised).  The
 * per-property work table is up_copy; up_t is the generator source.
 *
 * Returns the row count (== number of distinct ids 0..count-1).  Guarantees a
 * non-empty table: if the draw produced 0 rows, one fresh row is inserted so
 * that properties needing an existing id always have one.
 */
static int
build_copy(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	nrows = (int) pg_prop_draw_int(tc, 0, 15);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS up_copy; "
						   "DROP TABLE IF EXISTS up_t; "
						   "CREATE TABLE up_t (id int PRIMARY KEY, v int); ");

	for (int i = 0; i < nrows; i++)
	{
		int			v = (int) pg_prop_draw_int(tc, -5, 5);

		appendStringInfo(&ddl, "INSERT INTO up_t VALUES (%d, (%d)); ", i, v);
	}

	appendStringInfoString(&ddl,
						   "CREATE TABLE up_copy (id int PRIMARY KEY, v int); "
						   "INSERT INTO up_copy SELECT * FROM up_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build failed: %s\n  ddl: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);

	/* Guard non-empty: ensure at least one existing id (id 0). */
	if (nrows == 0)
	{
		if (!pg_prop_sql_exec(conn, "INSERT INTO up_copy VALUES (0, 1);", &err))
			pg_prop_fail(tc, "seed insert failed: %s", err ? err : "");
		nrows = 1;
	}

	return nrows;
}

static void
drop_copy(pg_prop_conn *conn)
{
	char	   *err = NULL;

	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS up_copy; DROP TABLE IF EXISTS up_t;",
					 &err);
	if (err)
		pg_free(err);
}

/*
 * DO NOTHING on an existing id must leave both the row count and that row's v
 * unchanged.
 */
PG_PROP(conflict_do_nothing_keeps)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	int			id;
	long		cnt0,
				v0,
				cnt1,
				v1;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_copy(tc, conn);
	id = (int) pg_prop_draw_int(tc, 0, n - 1);

	cnt0 = scalar_int(tc, conn, "SELECT count(*) FROM up_copy");
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT v FROM up_copy WHERE id = %d", id);
	v0 = scalar_int(tc, conn, q.data);

	resetStringInfo(&q);
	appendStringInfo(&q,
					 "INSERT INTO up_copy VALUES (%d, 424242) ON CONFLICT DO NOTHING",
					 id);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "DO NOTHING failed: %s\n  SQL: %s",
					 err ? err : "", q.data);

	cnt1 = scalar_int(tc, conn, "SELECT count(*) FROM up_copy");
	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT v FROM up_copy WHERE id = %d", id);
	v1 = scalar_int(tc, conn, q.data);

	if (cnt1 != cnt0)
		pg_prop_fail(tc, "DO NOTHING changed count: %ld -> %ld (id %d)",
					 cnt0, cnt1, id);
	if (v1 != v0)
		pg_prop_fail(tc, "DO NOTHING changed v: %ld -> %ld (id %d)",
					 v0, v1, id);

	pfree(q.data);
	drop_copy(conn);
	pg_prop_disconnect(conn);
}

/*
 * DO UPDATE SET v = EXCLUDED.v on an existing id must set that row's v to the
 * proposed value and leave the row count unchanged.
 */
PG_PROP(conflict_do_update_sets)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	int			id;
	long		cnt0,
				cnt1,
				v1;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_copy(tc, conn);
	id = (int) pg_prop_draw_int(tc, 0, n - 1);

	cnt0 = scalar_int(tc, conn, "SELECT count(*) FROM up_copy");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "INSERT INTO up_copy VALUES (%d, 999) "
					 "ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
					 id);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "DO UPDATE failed: %s\n  SQL: %s",
					 err ? err : "", q.data);

	cnt1 = scalar_int(tc, conn, "SELECT count(*) FROM up_copy");
	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT v FROM up_copy WHERE id = %d", id);
	v1 = scalar_int(tc, conn, q.data);

	if (cnt1 != cnt0)
		pg_prop_fail(tc, "DO UPDATE changed count: %ld -> %ld (id %d)",
					 cnt0, cnt1, id);
	if (v1 != 999)
		pg_prop_fail(tc, "DO UPDATE SET v=EXCLUDED.v: expected 999, got %ld (id %d)",
					 v1, id);

	pfree(q.data);
	drop_copy(conn);
	pg_prop_disconnect(conn);
}

/*
 * A brand-new id with DO NOTHING is inserted: the row count increases by one
 * and the row carries the proposed value.
 */
PG_PROP(no_conflict_inserts)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	int			newid;
	long		cnt0,
				cnt1,
				v1;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_copy(tc, conn);
	newid = n + (int) pg_prop_draw_int(tc, 0, 5);	/* ids are 0..n-1, so >= n is new */

	cnt0 = scalar_int(tc, conn, "SELECT count(*) FROM up_copy");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "INSERT INTO up_copy VALUES (%d, 7) ON CONFLICT DO NOTHING",
					 newid);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "insert failed: %s\n  SQL: %s", err ? err : "", q.data);

	cnt1 = scalar_int(tc, conn, "SELECT count(*) FROM up_copy");
	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT v FROM up_copy WHERE id = %d", newid);
	v1 = scalar_int(tc, conn, q.data);

	if (cnt1 != cnt0 + 1)
		pg_prop_fail(tc, "new-id insert count: expected %ld, got %ld (id %d)",
					 cnt0 + 1, cnt1, newid);
	if (v1 != 7)
		pg_prop_fail(tc, "new-id insert value: expected 7, got %ld (id %d)",
					 v1, newid);

	pfree(q.data);
	drop_copy(conn);
	pg_prop_disconnect(conn);
}

/*
 * EXCLUDED refers to the proposed row: DO UPDATE SET v = EXCLUDED.v + 1 on a
 * proposed (id, 50) must leave v = 51, regardless of the old value.
 */
PG_PROP(excluded_refers_proposed)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	int			id;
	long		v1;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_copy(tc, conn);
	id = (int) pg_prop_draw_int(tc, 0, n - 1);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "INSERT INTO up_copy VALUES (%d, 50) "
					 "ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v + 1",
					 id);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "DO UPDATE failed: %s\n  SQL: %s",
					 err ? err : "", q.data);

	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT v FROM up_copy WHERE id = %d", id);
	v1 = scalar_int(tc, conn, q.data);

	if (v1 != 51)
		pg_prop_fail(tc, "EXCLUDED.v + 1: expected 51, got %ld (id %d)", v1, id);

	pfree(q.data);
	drop_copy(conn);
	pg_prop_disconnect(conn);
}

/*
 * A conflict WHERE gates the update on the EXISTING row: DO UPDATE SET v = 777
 * WHERE up_copy.v < 0 only fires when the pre-existing v is negative.  We set a
 * known old value on a chosen id (one draw decides whether it is negative) and
 * check both branches: negative -> 777, non-negative -> unchanged.
 */
PG_PROP(conflict_where_condition)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	int			id;
	int			oldv;
	long		v1;
	bool		neg;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_copy(tc, conn);
	id = (int) pg_prop_draw_int(tc, 0, n - 1);

	/* Force a known old v on this id so the WHERE branch is known. */
	neg = pg_prop_draw_bool(tc);
	oldv = neg ? -(int) pg_prop_draw_int(tc, 1, 100) : (int) pg_prop_draw_int(tc, 0, 100);

	initStringInfo(&q);
	appendStringInfo(&q, "UPDATE up_copy SET v = (%d) WHERE id = %d", oldv, id);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "setup update failed: %s\n  SQL: %s",
					 err ? err : "", q.data);

	resetStringInfo(&q);
	appendStringInfo(&q,
					 "INSERT INTO up_copy VALUES (%d, 0) "
					 "ON CONFLICT (id) DO UPDATE SET v = 777 WHERE up_copy.v < 0",
					 id);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "DO UPDATE WHERE failed: %s\n  SQL: %s",
					 err ? err : "", q.data);

	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT v FROM up_copy WHERE id = %d", id);
	v1 = scalar_int(tc, conn, q.data);

	if (neg)
	{
		if (v1 != 777)
			pg_prop_fail(tc,
						 "WHERE v<0 should update: old v=%d, expected 777, got %ld (id %d)",
						 oldv, v1, id);
	}
	else
	{
		if (v1 != oldv)
			pg_prop_fail(tc,
						 "WHERE v<0 should skip: old v=%d, expected unchanged, got %ld (id %d)",
						 oldv, v1, id);
	}

	pfree(q.data);
	drop_copy(conn);
	pg_prop_disconnect(conn);
}

/*
 * A single INSERT that proposes the same new key twice with ON CONFLICT DO
 * UPDATE is an error ("ON CONFLICT DO UPDATE command cannot affect row a
 * second time").  The statement must FAIL; pg_prop_sql_exec returning true
 * (success) is the bug.
 */
PG_PROP(multi_insert_last_wins)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	int			newid;
	StringInfoData q;
	bool		ok;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_copy(tc, conn);
	newid = n + (int) pg_prop_draw_int(tc, 0, 5);	/* a new key, so the two rows conflict with each other */

	initStringInfo(&q);
	appendStringInfo(&q,
					 "INSERT INTO up_copy VALUES (%d, 1), (%d, 2) "
					 "ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v",
					 newid, newid);

	ok = pg_prop_sql_exec(conn, q.data, &err);
	if (ok)
		pg_prop_fail(tc,
					 "expected ON CONFLICT DO UPDATE double-affect error, but statement succeeded\n  SQL: %s",
					 q.data);
	if (err)
		pg_free(err);

	pfree(q.data);
	drop_copy(conn);
	pg_prop_disconnect(conn);
}
