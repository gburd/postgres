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

/*
 * SERIALIZABLE phantom / predicate-lock anomaly: T1 and T2 each count the rows
 * matching a range predicate and then insert a new row that falls in the
 * *other* transaction's predicate.  Serially one would see the other's insert;
 * concurrently this is a write-skew-via-phantom, and SERIALIZABLE's predicate
 * locking must stop both from committing.  At least one COMMIT must fail with a
 * serialization error.
 */
PG_PROP(ser_phantom_prevented)
{
	pg_prop_conn *admin = open_conn(tc);
	pg_prop_conn *t1 = open_conn(tc);
	pg_prop_conn *t2 = open_conn(tc);
	char	   *err = NULL,
			   *e1 = NULL,
			   *e2 = NULL;
	bool		c1,
				c2;

	/* a table of integer "marks"; T1 works the even side, T2 the odd side */
	if (!run(admin,
			 "DROP TABLE IF EXISTS ph_t; "
			 "CREATE TABLE ph_t (id serial PRIMARY KEY, g int); "
			 "INSERT INTO ph_t (g) SELECT (i % 4) FROM generate_series(1, 8) i;",
			 &err))
		pg_prop_fail(tc, "phantom setup failed: %s", err ? err : "");

	run(t1, "BEGIN ISOLATION LEVEL SERIALIZABLE", &err);
	run(t2, "BEGIN ISOLATION LEVEL SERIALIZABLE", &err);

	/* each reads a predicate range (establishing a predicate lock) ... */
	(void) scalar_long(tc, t1, "SELECT count(*) FROM ph_t WHERE g = 0");
	(void) scalar_long(tc, t2, "SELECT count(*) FROM ph_t WHERE g = 2");

	/* ... then each inserts a phantom into the other's range */
	run(t1, "INSERT INTO ph_t (g) VALUES (2)", &err);
	run(t2, "INSERT INTO ph_t (g) VALUES (0)", &err);

	c1 = run(t1, "COMMIT", &e1);
	c2 = run(t2, "COMMIT", &e2);

	if (c1 && c2)
		pg_prop_fail(tc,
					 "SERIALIZABLE allowed a phantom anomaly: both committed");

	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
	if (err)
		pg_free(err);
	pg_prop_disconnect(t1);
	pg_prop_disconnect(t2);
	pg_prop_disconnect(admin);
}

/*
 * Lock conflicts are detected and bounded: a row locked by one session blocks
 * a conflicting write from another, and with a short lock_timeout that write
 * fails (SQLSTATE 55P03) rather than waiting forever.  This exercises the
 * row-lock machinery synchronously (no second thread): T2 holds the lock, T1
 * tries to take it with a timeout and must fail; after T2 commits, T1 can take
 * it.  A write that neither waits nor fails would be a lock-correctness bug.
 */
PG_PROP(row_lock_conflict_times_out)
{
	pg_prop_conn *admin = open_conn(tc);
	pg_prop_conn *t1 = open_conn(tc);
	pg_prop_conn *t2 = open_conn(tc);
	char	   *err = NULL;
	bool		blocked_update_ok;
	int			delta = (int) pg_prop_draw_int(tc, 1, 100);
	StringInfoData q;

	setup_iso(tc, admin);

	/* T2 locks row 1 for update and holds it (open transaction) */
	run(t2, "BEGIN", &err);
	(void) scalar_long(tc, t2, "SELECT v FROM iso_t WHERE id = 1 FOR UPDATE");

	/* T1, with a short lock_timeout, tries to update the same row: must fail */
	run(t1, "BEGIN", &err);
	run(t1, "SET lock_timeout = '300ms'", &err);
	initStringInfo(&q);
	appendStringInfo(&q, "UPDATE iso_t SET v = v + %d WHERE id = 1", delta);
	blocked_update_ok = run(t1, q.data, &err);
	if (blocked_update_ok)
		pg_prop_fail(tc,
					 "conflicting UPDATE succeeded while row was locked "
					 "(expected lock_timeout)");
	run(t1, "ROLLBACK", &err);

	/* T2 releases; now T1 can take the lock and update */
	run(t2, "COMMIT", &err);
	run(t1, "BEGIN", &err);
	run(t1, "SET lock_timeout = '5s'", &err);
	if (!run(t1, q.data, &err))
		pg_prop_fail(tc, "UPDATE failed after lock released: %s",
					 err ? err : "");
	run(t1, "COMMIT", &err);

	if (err)
		pg_free(err);
	pfree(q.data);
	pg_prop_disconnect(t1);
	pg_prop_disconnect(t2);
	pg_prop_disconnect(admin);
}

/*
 * Deadlock detection and victim selection: two sessions lock rows in opposite
 * order, so each ends up waiting on a lock the other holds -- a true deadlock.
 * PostgreSQL's deadlock detector must break it by aborting exactly ONE session
 * (SQLSTATE 40P01) while the other proceeds to commit.  This requires genuine
 * concurrency: both UPDATEs must be in flight at once, so they are dispatched
 * asynchronously (pg_prop_sql_send) and then awaited.  A deadlock that hangs
 * both, or aborts both, or aborts neither, is a detector bug.
 */
PG_PROP(deadlock_one_victim)
{
	pg_prop_conn *admin = open_conn(tc);
	pg_prop_conn *t1 = open_conn(tc);
	pg_prop_conn *t2 = open_conn(tc);
	char	   *err = NULL,
			   *ss1 = NULL,
			   *ss2 = NULL;
	bool		ok1,
				ok2;
	int			victims;

	setup_iso(tc, admin);

	run(t1, "BEGIN", &err);
	run(t2, "BEGIN", &err);

	/* T1 locks row 1; T2 locks row 2 (no conflict yet) */
	(void) scalar_long(tc, t1, "SELECT v FROM iso_t WHERE id = 1 FOR UPDATE");
	(void) scalar_long(tc, t2, "SELECT v FROM iso_t WHERE id = 2 FOR UPDATE");

	/*
	 * Now each asks for the row the other holds, both in flight at once.
	 * Dispatch both async so they are simultaneously blocked; the deadlock
	 * detector then fires.
	 */
	if (!pg_prop_sql_send(t1, "UPDATE iso_t SET v = v + 1 WHERE id = 2"))
		pg_prop_fail(tc, "could not dispatch T1 update");
	if (!pg_prop_sql_send(t2, "UPDATE iso_t SET v = v + 1 WHERE id = 1"))
		pg_prop_fail(tc, "could not dispatch T2 update");

	/* await both; exactly one must fail with 40P01 (deadlock_detected) */
	ok1 = pg_prop_sql_await(t1, &ss1, &err);
	ok2 = pg_prop_sql_await(t2, &ss2, &err);

	victims = 0;
	if (!ok1 && ss1 && strcmp(ss1, "40P01") == 0)
		victims++;
	if (!ok2 && ss2 && strcmp(ss2, "40P01") == 0)
		victims++;

	/* the surviving session commits; the victim's transaction is already aborted */
	run(ok1 ? t1 : t2, "COMMIT", &err);
	run(ok1 ? t1 : t2, "ROLLBACK", &err);	/* no-op if already committed */

	if (victims != 1)
		pg_prop_fail(tc,
					 "deadlock did not select exactly one victim: "
					 "t1 ok=%d ss=%s, t2 ok=%d ss=%s (victims=%d)",
					 (int) ok1, ss1 ? ss1 : "-", (int) ok2, ss2 ? ss2 : "-",
					 victims);

	if (ss1) pg_free(ss1);
	if (ss2) pg_free(ss2);
	if (err) pg_free(err);
	pg_prop_disconnect(t1);
	pg_prop_disconnect(t2);
	pg_prop_disconnect(admin);
}
