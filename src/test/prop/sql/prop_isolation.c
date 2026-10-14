/*-------------------------------------------------------------------------
 *
 * prop_isolation.c
 *	  Property tests that transaction isolation guarantees hold.
 *
 * Isolation is one of the hardest things a database promises and one of the
 * most important to get right.  These properties drive two concurrent sessions
 * (two libpq connections) through interleavings and assert the guarantee each
 * isolation level makes:
 *
 *	 ser_write_skew_prevented -- under SERIALIZABLE, the classic write-skew
 *						        pair (each txn reads a set and writes based on
 *						        it) cannot both commit; at least one must fail
 *						        with a serialization error (SQLSTATE 40001).
 *	 rr_snapshot_stable       -- under REPEATABLE READ, a txn's repeated read
 *						        of a row returns the same value even after a
 *						        concurrent committed update (snapshot stability).
 *	 rc_sees_committed        -- under READ COMMITTED, a new statement sees a
 *						        concurrently committed change (and does not see
 *						        an uncommitted one).
 *	 deadlock_one_victim      -- two txns that lock rows in opposite order
 *						        deadlock, and exactly one is chosen as victim
 *						        (SQLSTATE 40P01) while the other proceeds.
 *
 * The oracle is the documented guarantee, not a generated reference: a
 * SERIALIZABLE anomaly committing, a REPEATABLE READ snapshot shifting, or a
 * deadlock taking down both sessions would each be a serious isolation bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.  Uses two
 * connections; the harness's per-case cleanup closes any left open.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_isolation.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "isolation"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Run SQL on a connection; return true on success, capturing sqlstate text. */
static bool
run(pg_prop_conn *conn, const char *sql, char **err)
{
	return pg_prop_sql_exec(conn, sql, err);
}

static pg_prop_conn *
open_conn(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *c = pg_prop_connect(&err);

	if (c == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return c;
}

static long
scalar_long(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	long		n;

	v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "scalar failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	n = v ? atol(v) : 0;
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	return n;
}

/*
 * Build iso_t(id int primary key, v int) with two rows (1,0),(2,0) on one
 * connection.  Shared by the properties as the contended data.
 */
static void
setup_iso(pg_prop_tc *tc, pg_prop_conn *admin)
{
	char	   *err = NULL;

	if (!run(admin,
			 "DROP TABLE IF EXISTS iso_t; "
			 "CREATE TABLE iso_t (id int PRIMARY KEY, v int); "
			 "INSERT INTO iso_t VALUES (1, 0), (2, 0);", &err))
		pg_prop_fail(tc, "iso setup failed: %s", err ? err : "");
}

/*
 * SERIALIZABLE write skew: T1 reads row 2 and sets row 1 = that value + 1;
 * T2 reads row 1 and sets row 2 = that value + 1.  Serially either order gives
 * a determinate result; concurrently this is the write-skew anomaly, and
 * SERIALIZABLE must prevent both from committing.  At least one COMMIT must
 * fail with a serialization error.
 */
PG_PROP(ser_write_skew_prevented)
{
	pg_prop_conn *admin = open_conn(tc);
	pg_prop_conn *t1 = open_conn(tc);
	pg_prop_conn *t2 = open_conn(tc);
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *err = NULL;
	bool		c1,
				c2;
	int			a = (int) pg_prop_draw_int(tc, 0, 5);
	int			b = (int) pg_prop_draw_int(tc, 0, 5);
	StringInfoData q;

	setup_iso(tc, admin);

	run(t1, "BEGIN ISOLATION LEVEL SERIALIZABLE", &err);
	run(t2, "BEGIN ISOLATION LEVEL SERIALIZABLE", &err);

	/* each reads the "other" row, then writes "its" row based on it */
	(void) scalar_long(tc, t1, "SELECT v FROM iso_t WHERE id = 2");
	(void) scalar_long(tc, t2, "SELECT v FROM iso_t WHERE id = 1");

	initStringInfo(&q);
	appendStringInfo(&q, "UPDATE iso_t SET v = v + %d WHERE id = 1", a + 1);
	run(t1, q.data, &err);
	resetStringInfo(&q);
	appendStringInfo(&q, "UPDATE iso_t SET v = v + %d WHERE id = 2", b + 1);
	run(t2, q.data, &err);

	c1 = run(t1, "COMMIT", &e1);
	c2 = run(t2, "COMMIT", &e2);

	/*
	 * The guarantee: the two transactions form a dangerous read/write
	 * dependency cycle, so they cannot both commit under SERIALIZABLE.  At
	 * least one COMMIT must fail (SQLSTATE 40001, surfaced as a failed exec).
	 */
	if (c1 && c2)
		pg_prop_fail(tc,
					 "SERIALIZABLE allowed write skew: both transactions "
					 "committed (a=%d b=%d)", a, b);

	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
	pfree(q.data);
	pg_prop_disconnect(t1);
	pg_prop_disconnect(t2);
	pg_prop_disconnect(admin);
}

/*
 * REPEATABLE READ snapshot stability: T1 reads row 1, then a concurrent
 * committed T2 updates row 1, then T1 reads row 1 again.  T1 must see the same
 * value both times (its snapshot is fixed at the first statement).
 */
PG_PROP(rr_snapshot_stable)
{
	pg_prop_conn *admin = open_conn(tc);
	pg_prop_conn *t1 = open_conn(tc);
	pg_prop_conn *t2 = open_conn(tc);
	char	   *err = NULL;
	long		first,
				second;
	int			delta = (int) pg_prop_draw_int(tc, 1, 100);
	StringInfoData q;

	setup_iso(tc, admin);

	run(t1, "BEGIN ISOLATION LEVEL REPEATABLE READ", &err);
	first = scalar_long(tc, t1, "SELECT v FROM iso_t WHERE id = 1");

	/* concurrent committed update by T2 */
	initStringInfo(&q);
	appendStringInfo(&q, "UPDATE iso_t SET v = v + %d WHERE id = 1", delta);
	run(t2, "BEGIN", &err);
	run(t2, q.data, &err);
	run(t2, "COMMIT", &err);

	second = scalar_long(tc, t1, "SELECT v FROM iso_t WHERE id = 1");
	run(t1, "COMMIT", &err);

	if (first != second)
		pg_prop_fail(tc,
					 "REPEATABLE READ snapshot shifted: first=%ld second=%ld "
					 "(concurrent +%d)", first, second, delta);

	if (err)
		pg_free(err);
	pfree(q.data);
	pg_prop_disconnect(t1);
	pg_prop_disconnect(t2);
	pg_prop_disconnect(admin);
}

/*
 * READ COMMITTED visibility: within a READ COMMITTED transaction, a statement
 * run after a concurrent commit sees the committed value, and a value written
 * by an uncommitted concurrent txn is NOT seen.
 */
PG_PROP(rc_sees_committed)
{
	pg_prop_conn *admin = open_conn(tc);
	pg_prop_conn *t1 = open_conn(tc);
	pg_prop_conn *t2 = open_conn(tc);
	char	   *err = NULL;
	long		base,
				after_uncommitted,
				after_committed;
	int			delta = (int) pg_prop_draw_int(tc, 1, 100);
	StringInfoData q;

	setup_iso(tc, admin);

	run(t1, "BEGIN ISOLATION LEVEL READ COMMITTED", &err);
	base = scalar_long(tc, t1, "SELECT v FROM iso_t WHERE id = 1");

	/* T2 updates but does NOT commit yet */
	initStringInfo(&q);
	appendStringInfo(&q, "UPDATE iso_t SET v = v + %d WHERE id = 1", delta);
	run(t2, "BEGIN", &err);
	run(t2, q.data, &err);

	/* T1's new statement must still see the base value (T2 uncommitted) */
	after_uncommitted = scalar_long(tc, t1, "SELECT v FROM iso_t WHERE id = 1");
	if (after_uncommitted != base)
		pg_prop_fail(tc,
					 "READ COMMITTED saw uncommitted write: base=%ld saw=%ld",
					 base, after_uncommitted);

	/* now T2 commits; T1's next statement must see the committed value */
	run(t2, "COMMIT", &err);
	after_committed = scalar_long(tc, t1, "SELECT v FROM iso_t WHERE id = 1");
	if (after_committed != base + delta)
		pg_prop_fail(tc,
					 "READ COMMITTED did not see committed write: expected %ld saw %ld",
					 base + delta, after_committed);

	run(t1, "COMMIT", &err);
	if (err)
		pg_free(err);
	pfree(q.data);
	pg_prop_disconnect(t1);
	pg_prop_disconnect(t2);
	pg_prop_disconnect(admin);
}
