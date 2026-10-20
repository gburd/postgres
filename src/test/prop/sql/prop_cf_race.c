/*-------------------------------------------------------------------------
 *
 * prop_cf_race.c
 *	  Two-session interleavings from the open commitfest.
 *
 * Each property drives two (or three) connections through a drawn
 * interleaving with pg_prop_sql_send / pg_prop_sql_await, and checks an
 * invariant that must hold whatever the order: SERIALIZABLE transactions
 * that both commit have a serial order, a primary key stays unique, DDL that
 * races DDL either succeeds or fails with a user-facing error -- never an
 * internal one -- and the catalog it leaves behind is consistent.
 *
 *	 CF 7083  write skew through TID / TID-range / bitmap scans under SSI
 *	 CF 7197  duplicate primary key under SSI after a delete via another index
 *	 CF 5681  concurrent CREATE OR REPLACE / ALTER FUNCTION: internal error
 *	 CF 7303  ALTER SEQUENCE ... AS racing a read of the sequence (guard: the
 *			  XX001 did not reproduce on FreeBSD/arm64, io_method=worker)
 *	 CF 7353  CREATE INDEX on a partitioned table racing ATTACH PARTITION
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cf_race"
#include "pg_prop.h"

#include "lib/stringinfo.h"

static pg_prop_conn *
rc_connect(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

/*
 * Connect to a named database (creating it from template0 if absent), by
 * briefly pointing PGDATABASE at it around pg_prop_connect.  Used where a
 * property needs a connection in a database other than the one the driver
 * selected -- e.g. a cross-database xmin holder.  Returns NULL if the
 * database cannot be created.
 */
static pg_prop_conn *
rc_connect_db(pg_prop_tc *tc, const char *dbname)
{
	char	   *err = NULL;
	pg_prop_conn *admin = rc_connect(tc);
	bool		f = false;
	char	   *ex;
	StringInfoData q;
	const char *saved;
	pg_prop_conn *conn;

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT count(*) FROM pg_database WHERE datname = '%s'", dbname);
	ex = pg_prop_sql_scalar(admin, q.data, &f, NULL);
	if (!f && ex && strcmp(ex, "0") == 0)
	{
		resetStringInfo(&q);
		appendStringInfo(&q, "CREATE DATABASE %s TEMPLATE template0", dbname);
		if (!pg_prop_sql_exec(admin, q.data, &err))
			pg_prop_fail(tc, "CREATE DATABASE %s failed: %s", dbname, err ? err : "");
	}
	if (ex)
		pg_free(ex);
	pfree(q.data);
	pg_prop_disconnect(admin);

	saved = getenv("PGDATABASE");
	setenv("PGDATABASE", dbname, 1);
	conn = pg_prop_connect(&err);
	if (saved)
		setenv("PGDATABASE", saved, 1);
	else
		unsetenv("PGDATABASE");
	if (conn == NULL)
		pg_prop_fail(tc, "connect to %s failed: %s", dbname, err ? err : "");
	return conn;
}

static void
rc_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "statement failed: %s\n  SQL: %.1000s", err ? err : "", sql);
}

static char *
rc_scalar(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		f = false;
	char	   *err = NULL;
	char	   *v = pg_prop_sql_scalar(conn, sql, &f, &err);

	if (f || v == NULL)
		pg_prop_fail(tc, "query failed: %s\n  SQL: %s", err ? err : "", sql);
	return v;
}

/* backend pid of a connection */
static char *
rc_pid(pg_prop_tc *tc, pg_prop_conn *conn)
{
	return rc_scalar(tc, conn, "SELECT pg_backend_pid()");
}

/*
 * Wait (up to ~10 s) until backend "pid" is blocked on a lock; returns false
 * as soon as it is no longer running a statement (it did not need to block,
 * which is a legal interleaving).
 */
static bool
rc_wait_blocked(pg_prop_tc *tc, pg_prop_conn *obs, const char *pid)
{
	char		q[200];

	snprintf(q, sizeof(q),
			 "SELECT CASE WHEN cardinality(pg_blocking_pids(%s)) > 0 THEN 'b' "
			 "WHEN EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = %s AND state <> 'active') "
			 "THEN 'd' ELSE 'r' END",
			 pid, pid);
	for (int i = 0; i < 200; i++)
	{
		char	   *v;
		char		c;

		/* obs may be inside a transaction: its activity snapshot is cached */
		pg_free(rc_scalar(tc, obs, "SELECT pg_stat_clear_snapshot()"));
		v = rc_scalar(tc, obs, q);
		c = v[0];

		pg_free(v);
		if (c == 'b')
			return true;
		if (c == 'd' && i > 0)
			return false;
		pg_free(rc_scalar(tc, obs, "SELECT pg_sleep(0.05)"));
	}
	return false;
}

/* an error that only an internal bug produces */
static bool
rc_internal(const char *sqlstate, const char *err)
{
	return (sqlstate && (strcmp(sqlstate, "XX000") == 0 || strcmp(sqlstate, "XX001") == 0 ||
						 strcmp(sqlstate, "XX002") == 0)) ||
		(err && (strstr(err, "tuple concurrently updated") || strstr(err, "bogus") ||
				 strstr(err, "cache lookup failed") || strstr(err, "could not read block")));
}

static bool
rc_serfail(const char *sqlstate)
{
	return sqlstate && (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0);
}

/*
 * CF 7083: two SERIALIZABLE transactions each read two rows through a drawn
 * access path, then update the row the other read.  That is write skew: SSI
 * must abort one of them.  On master the TID scan and TID range scan paths
 * take no predicate lock, and both commit.
 *
 * Invariant: if both commit, the result equals some serial order -- here,
 * at least one transaction must have seen the other's write, which with
 * this schedule (both reads before both writes) is impossible, so at least
 * one must fail with a serialization failure.
 */
PG_PROP(ssi_scan_paths_prevent_write_skew)
{
	static const char *const paths[] = {"tid", "tidrange", "bitmap", "index", "seq"};
	pg_prop_conn *s1 = rc_connect(tc);
	pg_prop_conn *s2 = rc_connect(tc);
	int			path = (int) pg_prop_draw_int(tc, 0, lengthof(paths) - 1);
	int			nrows = (int) pg_prop_draw_int(tc, 2, 40);
	bool		first1 = pg_prop_draw_bool(tc);
	bool		commit1first = pg_prop_draw_bool(tc);
	const char *read_q;
	const char *gucs;
	char	   *st1 = NULL;
	char	   *st2 = NULL;
	char	   *e1 = NULL;
	char	   *e2 = NULL;
	bool		ok1;
	bool		ok2;
	bool		ok;
	StringInfoData s;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP TABLE IF EXISTS cfr_ws; CREATE TABLE cfr_ws (i int PRIMARY KEY, t text); "
					 "INSERT INTO cfr_ws SELECT g, 'v' FROM generate_series(1, %d) g", nrows);
	rc_exec(tc, s1, s.data);

	switch (path)
	{
		case 0:
			read_q = "SELECT * FROM cfr_ws WHERE ctid IN ('(0,1)', '(0,2)')";
			gucs = "SET LOCAL enable_seqscan = off; SET LOCAL enable_tidscan = on";
			break;
		case 1:
			read_q = "SELECT * FROM cfr_ws WHERE ctid >= '(0,1)' AND ctid <= '(0,2)'";
			gucs = "SET LOCAL enable_seqscan = off; SET LOCAL enable_tidscan = on";
			break;
		case 2:
			read_q = "SELECT * FROM cfr_ws WHERE i IN (1, 2)";
			gucs = "SET LOCAL enable_seqscan = off; SET LOCAL enable_indexscan = off";
			break;
		case 3:
			read_q = "SELECT * FROM cfr_ws WHERE i IN (1, 2)";
			gucs = "SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off";
			break;
		default:
			read_q = "SELECT * FROM cfr_ws WHERE i IN (1, 2)";
			gucs = "SET LOCAL enable_indexscan = off; SET LOCAL enable_bitmapscan = off";
			break;
	}

	rc_exec(tc, s1, "BEGIN ISOLATION LEVEL SERIALIZABLE");
	rc_exec(tc, s2, "BEGIN ISOLATION LEVEL SERIALIZABLE");
	rc_exec(tc, s1, gucs);
	rc_exec(tc, s2, gucs);
	rc_exec(tc, first1 ? s1 : s2, read_q);
	rc_exec(tc, first1 ? s2 : s1, read_q);

	/* each writes the row the other read but it did not write */
	ok1 = pg_prop_sql_send(s1, "UPDATE cfr_ws SET t = 'x1' WHERE i = 2");
	ok = ok1 && pg_prop_sql_await(s1, &st1, &e1);
	ok1 = ok;
	ok2 = pg_prop_sql_send(s2, "UPDATE cfr_ws SET t = 'x2' WHERE i = 1");
	ok = ok2 && pg_prop_sql_await(s2, &st2, &e2);
	ok2 = ok;

	if (commit1first)
	{
		if (ok1)
			ok1 = pg_prop_sql_send(s1, "COMMIT") && pg_prop_sql_await(s1, &st1, &e1);
		if (ok2)
			ok2 = pg_prop_sql_send(s2, "COMMIT") && pg_prop_sql_await(s2, &st2, &e2);
	}
	else
	{
		if (ok2)
			ok2 = pg_prop_sql_send(s2, "COMMIT") && pg_prop_sql_await(s2, &st2, &e2);
		if (ok1)
			ok1 = pg_prop_sql_send(s1, "COMMIT") && pg_prop_sql_await(s1, &st1, &e1);
	}
	if (!ok1)
		pg_prop_sql_exec(s1, "ROLLBACK", NULL);
	if (!ok2)
		pg_prop_sql_exec(s2, "ROLLBACK", NULL);

	if ((!ok1 && !rc_serfail(st1)) || (!ok2 && !rc_serfail(st2)))
		pg_prop_fail(tc, "unexpected error under SERIALIZABLE (%s scan): %s / %s",
					 paths[path], e1 ? e1 : "-", e2 ? e2 : "-");
	if (ok1 && ok2)
	{
		/* is the outcome serialisable?  both read old values of both rows */
		char	   *v = rc_scalar(tc, s1, "SELECT string_agg(t, ',' ORDER BY i) FROM cfr_ws WHERE i IN (1, 2)");

		pg_prop_fail(tc, "write skew: both SERIALIZABLE transactions committed after reading "
					 "through a %s scan; rows are now %s", paths[path], v);
	}
	rc_exec(tc, s1, "DROP TABLE cfr_ws");
	pfree(s.data);
	pg_prop_disconnect(s2);
	pg_prop_disconnect(s1);
}

/*
 * CF 7197: s1 (SERIALIZABLE) looks up k = 1 and finds it; s2 deletes that
 * row through a secondary index and commits; s1 inserts k = 1.  Whatever SSI
 * decides, the table must never hold two rows with k = 1, and s1 must not
 * see two.  Drawn: the uniqueness mechanism (PK, UNIQUE, EXCLUDE), ON
 * CONFLICT DO NOTHING vs plain INSERT, filler size.
 */
PG_PROP(ssi_primary_key_stays_unique)
{
	static const char *const keys[] = {
		"k int PRIMARY KEY, j int", "k int UNIQUE, j int", "k int, j int, EXCLUDE USING btree (k WITH =)",
	};
	pg_prop_conn *s1 = rc_connect(tc);
	pg_prop_conn *s2 = rc_connect(tc);
	int			key = (int) pg_prop_draw_int(tc, 0, lengthof(keys) - 1);
	bool		onconf = pg_prop_draw_bool(tc);
	int			fill = (int) pg_prop_draw_int(tc, 0, 3000);
	char	   *st = NULL;
	char	   *e = NULL;
	char	   *seen;
	char	   *n;
	bool		ok;
	StringInfoData s;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP TABLE IF EXISTS cfr_pk; CREATE TABLE cfr_pk (%s); "
					 "INSERT INTO cfr_pk VALUES (1, 1000000); "
					 "INSERT INTO cfr_pk SELECT g, g FROM generate_series(100, 100 + %d) g; "
					 "CREATE INDEX cfr_pk_j ON cfr_pk (j); ANALYZE cfr_pk",
					 keys[key], fill);
	rc_exec(tc, s1, s.data);

	rc_exec(tc, s1, "BEGIN ISOLATION LEVEL SERIALIZABLE; SET LOCAL enable_seqscan = off");
	rc_exec(tc, s2, "BEGIN ISOLATION LEVEL SERIALIZABLE; SET LOCAL enable_seqscan = off");
	rc_exec(tc, s1, "SELECT k, j FROM cfr_pk WHERE k = 1");
	rc_exec(tc, s2, "DELETE FROM cfr_pk WHERE j = 1000000");
	rc_exec(tc, s2, "COMMIT");
	ok = pg_prop_sql_send(s1, onconf ? "INSERT INTO cfr_pk VALUES (1, 2) ON CONFLICT DO NOTHING"
						  : "INSERT INTO cfr_pk VALUES (1, 2)") &&
		pg_prop_sql_await(s1, &st, &e);
	if (ok)
	{
		seen = rc_scalar(tc, s1, "SELECT count(*) FROM cfr_pk WHERE k = 1");
		if (strcmp(seen, "1") > 0)
			pg_prop_fail(tc, "transaction sees %s rows with k = 1 after its INSERT (%s)", seen, keys[key]);
		pg_free(seen);
		ok = pg_prop_sql_send(s1, "COMMIT") && pg_prop_sql_await(s1, &st, &e);
	}
	if (!ok)
	{
		pg_prop_sql_exec(s1, "ROLLBACK", NULL);
		if (!rc_serfail(st) && !(st && strcmp(st, "23505") == 0) && !(st && strcmp(st, "23P01") == 0))
			pg_prop_fail(tc, "unexpected error: %s", e ? e : "");
	}
	n = rc_scalar(tc, s1, "SELECT count(*) FROM cfr_pk WHERE k = 1");
	if (strcmp(n, "1") != 0 && strcmp(n, "0") != 0)
		pg_prop_fail(tc, "after commit the table holds %s rows with k = 1 (%s, %s)",
					 n, keys[key], onconf ? "ON CONFLICT DO NOTHING" : "INSERT");
	pg_free(n);
	rc_exec(tc, s1, "DROP TABLE cfr_pk");
	pfree(s.data);
	pg_prop_disconnect(s2);
	pg_prop_disconnect(s1);
}

/*
 * CF 5681: two sessions redefine (or ALTER) the same function concurrently.
 * One waits for the other; when it proceeds it must succeed or raise a
 * user-facing error -- never "tuple concurrently updated".  Drawn: the two
 * DDL statements, commit vs rollback of the first.
 */
PG_PROP(concurrent_function_ddl_is_not_internal)
{
	static const char *const ddl[] = {
		"CREATE OR REPLACE FUNCTION cfr_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + 1'",
		"CREATE OR REPLACE FUNCTION cfr_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1 + 2'",
		"ALTER FUNCTION cfr_f(int) IMMUTABLE",
		"ALTER FUNCTION cfr_f(int) COST 7",
		"ALTER FUNCTION cfr_f(int) SET search_path = public",
		"COMMENT ON FUNCTION cfr_f(int) IS 'x'",
	};
	pg_prop_conn *s1 = rc_connect(tc);
	pg_prop_conn *s2 = rc_connect(tc);
	const char *d1 = ddl[pg_prop_draw_int(tc, 0, lengthof(ddl) - 1)];
	const char *d2 = ddl[pg_prop_draw_int(tc, 0, lengthof(ddl) - 1)];
	bool		commit = pg_prop_draw_int(tc, 0, 3) != 0;
	char	   *pid2;
	char	   *st = NULL;
	char	   *e = NULL;
	bool		ok;

	rc_exec(tc, s1, "DROP FUNCTION IF EXISTS cfr_f(int); "
			"CREATE FUNCTION cfr_f(int) RETURNS int LANGUAGE sql AS 'SELECT $1'");
	pid2 = rc_pid(tc, s2);
	rc_exec(tc, s1, "BEGIN");
	rc_exec(tc, s1, d1);
	if (!pg_prop_sql_send(s2, d2))
		pg_prop_fail(tc, "could not dispatch");
	rc_wait_blocked(tc, s1, pid2);
	rc_exec(tc, s1, commit ? "COMMIT" : "ROLLBACK");
	ok = pg_prop_sql_await(s2, &st, &e);
	if (!ok && rc_internal(st, e))
		pg_prop_fail(tc, "concurrent function DDL raised an internal error: %s\n"
					 "  s1: %s (%s)\n  s2: %s", e ? e : "", d1, commit ? "COMMIT" : "ROLLBACK", d2);
	pg_free(pid2);
	pg_prop_sql_exec(s1, "DROP FUNCTION IF EXISTS cfr_f(int)", NULL);
	pg_prop_disconnect(s2);
	pg_prop_disconnect(s1);
}

/*
 * CF 7303: ALTER SEQUENCE ... AS <type> rewrites the sequence relfilenode;
 * a concurrent read of the sequence must see the old or new state, never
 * fail to read the relation.  Drawn: the sequence of AS types, the reads.
 */
PG_PROP(alter_sequence_as_races_reads)
{
	static const char *const types[] = {"int", "bigint", "smallint"};
	static const char *const reads[] = {
		"SELECT * FROM cfr_seq", "SELECT last_value FROM cfr_seq", "SELECT nextval('cfr_seq')",
		"SELECT * FROM pg_sequence_last_value('cfr_seq')",
	};
	pg_prop_conn *s1 = rc_connect(tc);
	pg_prop_conn *s2 = rc_connect(tc);
	int			rounds = (int) pg_prop_draw_int(tc, 5, 60);
	char	   *st = NULL;
	char	   *e = NULL;
	char		q[120];

	rc_exec(tc, s1, "DROP SEQUENCE IF EXISTS cfr_seq; CREATE SEQUENCE cfr_seq");
	for (int i = 0; i < rounds; i++)
	{
		snprintf(q, sizeof(q), "ALTER SEQUENCE cfr_seq AS %s",
				 types[pg_prop_draw_int(tc, 0, lengthof(types) - 1)]);
		if (!pg_prop_sql_send(s1, q))
			pg_prop_fail(tc, "could not dispatch");
		if (!pg_prop_sql_send(s2, reads[pg_prop_draw_int(tc, 0, lengthof(reads) - 1)]))
			pg_prop_fail(tc, "could not dispatch");
		if (!pg_prop_sql_await(s2, &st, &e) && rc_internal(st, e))
			pg_prop_fail(tc, "a read racing ALTER SEQUENCE ... AS failed: %s (round %d)", e ? e : "", i);
		if (!pg_prop_sql_await(s1, &st, &e) && rc_internal(st, e))
			pg_prop_fail(tc, "ALTER SEQUENCE ... AS racing a read failed: %s (round %d)", e ? e : "", i);
	}
	rc_exec(tc, s1, "DROP SEQUENCE cfr_seq");
	pg_prop_disconnect(s2);
	pg_prop_disconnect(s1);
}

/*
 * CF 7353: CREATE INDEX on a partitioned table while another session (in an
 * open transaction) attaches or renames a partition's index.  The CREATE
 * INDEX must succeed or fail with a user-facing error, and afterwards every
 * partition index is attached to at most one parent index.
 */
PG_PROP(create_index_races_index_attach)
{
	pg_prop_conn *s1 = rc_connect(tc);
	pg_prop_conn *s2 = rc_connect(tc);
	int			nparts = (int) pg_prop_draw_int(tc, 2, 4);
	int			action = (int) pg_prop_draw_int(tc, 0, 2);
	int			attached = (int) pg_prop_draw_int(tc, 1, nparts - 1);
	bool		commit = pg_prop_draw_int(tc, 0, 3) != 0;
	char	   *pid2;
	char	   *st = NULL;
	char	   *e = NULL;
	char	   *bad;
	StringInfoData s;

	initStringInfo(&s);
	appendStringInfoString(&s, "DROP TABLE IF EXISTS cfr_pia; CREATE TABLE cfr_pia (a int) PARTITION BY RANGE (a); ");
	for (int i = 0; i < nparts; i++)
		appendStringInfo(&s, "CREATE TABLE cfr_pia_%d PARTITION OF cfr_pia FOR VALUES FROM (%d) TO (%d); ",
						 i, i * 10, i * 10 + 10);
	appendStringInfoString(&s, "CREATE INDEX cfr_pia_old ON ONLY cfr_pia (a); ");
	for (int i = 0; i < nparts; i++)
		appendStringInfo(&s, "CREATE INDEX cfr_pia_%d_a ON cfr_pia_%d (a); ", i, i);
	for (int i = attached; i < nparts; i++)
		appendStringInfo(&s, "ALTER INDEX cfr_pia_old ATTACH PARTITION cfr_pia_%d_a; ", i);
	rc_exec(tc, s1, s.data);

	pid2 = rc_pid(tc, s2);
	rc_exec(tc, s1, "BEGIN");
	resetStringInfo(&s);
	if (action == 0)
		appendStringInfo(&s, "ALTER INDEX cfr_pia_old ATTACH PARTITION cfr_pia_%d_a",
						 (int) pg_prop_draw_int(tc, 0, attached - 1));
	else if (action == 1)
		appendStringInfo(&s, "ALTER INDEX cfr_pia_%d_a RENAME TO cfr_pia_renamed",
						 (int) pg_prop_draw_int(tc, attached, nparts - 1));
	else
		appendStringInfoString(&s, "SELECT count(*) FROM cfr_pia");
	rc_exec(tc, s1, s.data);
	if (!pg_prop_sql_send(s2, "CREATE INDEX cfr_pia_new ON cfr_pia (a)"))
		pg_prop_fail(tc, "could not dispatch");
	rc_wait_blocked(tc, s1, pid2);
	rc_exec(tc, s1, commit ? "COMMIT" : "ROLLBACK");
	if (!pg_prop_sql_await(s2, &st, &e) && rc_internal(st, e))
		pg_prop_fail(tc, "CREATE INDEX racing \"%s\" (%s) raised an internal error: %s",
					 s.data, commit ? "COMMIT" : "ROLLBACK", e ? e : "");

	bad = rc_scalar(tc, s1,
					"SELECT count(*) FROM (SELECT inhrelid FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
					"WHERE c.relkind = 'i' AND c.relname LIKE 'cfr_pia%' GROUP BY inhrelid HAVING count(*) > 1) x");
	if (strcmp(bad, "0") != 0)
		pg_prop_fail(tc, "%s partition indexes have more than one parent after CREATE INDEX raced \"%s\"",
					 bad, s.data);
	pg_free(bad);
	pg_free(pid2);
	rc_exec(tc, s1, "DROP TABLE cfr_pia");
	pfree(s.data);
	pg_prop_disconnect(s2);
	pg_prop_disconnect(s1);
}

/*
 * CF 6442: a recursing ALTER TABLE on the root of a partition tree (locks
 * top-down) racing ATTACH PARTITION deeper in the tree (locks the target,
 * then the new partition, then the ancestors through
 * generate_partition_qual: bottom-up) must not deadlock -- each is a single
 * DDL statement, no user-visible lock ordering is involved.
 *
 * The interleaving is forced, so the case is deterministic: a third session
 * holds a lock on the partition being attached; ATTACH then holds the
 * target and waits for it; the root ALTER then holds the root and waits for
 * the target; the third session commits.  ATTACH proceeds to lock the root
 * -- a cycle.  Drawn: tree depth, attach level, the root command, the lock
 * the third session holds.
 *
 * Known open: the CF 6442 entry carries only an injection-point TAP test
 * documenting this deadlock, no fix, so this property fails on master and
 * on the patched server alike.
 */
PG_PROP(root_alter_races_deep_attach_without_deadlock)
{
	static const char *const root_cmds[] = {
		"ALTER TABLE cfr_t0 ALTER COLUMN id SET DEFAULT 0",
		"ALTER TABLE cfr_t0 ALTER COLUMN id DROP DEFAULT",
		"ALTER TABLE cfr_t0 ALTER COLUMN id SET STATISTICS 50",
		"ALTER TABLE cfr_t0 ADD CONSTRAINT cfr_ck CHECK (id >= 0)",
	};
	static const char *const holds[] = {"ACCESS SHARE", "ROW EXCLUSIVE", "SHARE"};
	pg_prop_conn *s1 = rc_connect(tc);
	pg_prop_conn *s2 = rc_connect(tc);
	pg_prop_conn *s3 = rc_connect(tc);
	int			depth = (int) pg_prop_draw_int(tc, 2, 5);
	int			lvl = (int) pg_prop_draw_int(tc, 1, depth - 1);
	const char *root = root_cmds[pg_prop_draw_int(tc, 0, lengthof(root_cmds) - 1)];
	const char *hold = holds[pg_prop_draw_int(tc, 0, lengthof(holds) - 1)];
	char	   *pid1 = rc_pid(tc, s1);
	char	   *pid2 = rc_pid(tc, s2);
	char	   *st1 = NULL;
	char	   *st2 = NULL;
	char	   *e1 = NULL;
	char	   *e2 = NULL;
	bool		ok1;
	bool		ok2;
	StringInfoData s;
	char		att[200];

	initStringInfo(&s);
	appendStringInfoString(&s, "DROP TABLE IF EXISTS cfr_t0, cfr_new; "
						   "CREATE TABLE cfr_t0 (id int) PARTITION BY RANGE (id); ");
	for (int i = 1; i < depth; i++)
		appendStringInfo(&s, "CREATE TABLE cfr_t%d PARTITION OF cfr_t%d FOR VALUES FROM (0) TO (%d) "
						 "PARTITION BY RANGE (id); ", i, i - 1, 1000000 >> (i - 1));
	appendStringInfoString(&s, "CREATE TABLE cfr_new (id int)");
	rc_exec(tc, s1, s.data);
	snprintf(att, sizeof(att), "ALTER TABLE cfr_t%d ATTACH PARTITION cfr_new FOR VALUES FROM (%d) TO (%d)",
			 lvl, (1000000 >> lvl) / 2, 1000000 >> lvl);

	resetStringInfo(&s);
	appendStringInfo(&s, "LOCK TABLE cfr_new IN %s MODE", hold);
	rc_exec(tc, s3, "BEGIN");
	rc_exec(tc, s3, s.data);
	if (!pg_prop_sql_send(s2, att))
		pg_prop_fail(tc, "could not dispatch");
	rc_wait_blocked(tc, s3, pid2);
	if (!pg_prop_sql_send(s1, root))
		pg_prop_fail(tc, "could not dispatch");
	rc_wait_blocked(tc, s3, pid1);
	rc_exec(tc, s3, "COMMIT");

	ok2 = pg_prop_sql_await(s2, &st2, &e2);
	ok1 = pg_prop_sql_await(s1, &st1, &e1);
	if ((!ok1 && st1 && strcmp(st1, "40P01") == 0) || (!ok2 && st2 && strcmp(st2, "40P01") == 0))
		pg_prop_fail(tc, "deadlock between a root ALTER TABLE and a deeper ATTACH PARTITION\n"
					 "  root: %s\n  attach: %s\n  tree depth %d",
					 root, att, depth);
	if ((!ok1 && rc_internal(st1, e1)) || (!ok2 && rc_internal(st2, e2)))
		pg_prop_fail(tc, "internal error: %s / %s", e1 ? e1 : "-", e2 ? e2 : "-");

	pg_prop_sql_exec(s1, "DROP TABLE IF EXISTS cfr_t0, cfr_new", NULL);
	pg_free(pid1);
	pg_free(pid2);
	pfree(s.data);
	pg_prop_disconnect(s3);
	pg_prop_disconnect(s2);
	pg_prop_disconnect(s1);
}

/*
 * CF 6824: a heap rewrite (VACUUM FULL / CLUSTER / REPACK) or an index build
 * must not fail with "missing chunk number N for toast value" when a dead
 * heap tuple's TOAST has already been vacuumed away while the main tuple is
 * still held back by a concurrent transaction's xmin.  The rewrite copies
 * the dead tuple (it is not yet removable) and tries to detoast a value whose
 * chunks are gone.
 *
 * Precondition, from the patch's own recovery test:
 *   - a global xmin holder in another database (holds back the global horizon
 *     so the dead main tuple is copied, not removed);
 *   - a same-database REPEATABLE READ holder open across the DELETE and a
 *     VACUUM (PROCESS_TOAST false), so the main tuple survives that vacuum;
 *   - release the same-db holder, then VACUUM the toast table directly, which
 *     removes the now-dead toast chunks.
 * Then the rewrite / index build runs while the global holder is still open.
 *
 * Invariant: the operation succeeds or raises no missing/unexpected-chunk
 * error.  Drawn: the operation, value size (all out-of-line), row count,
 * which row is deleted.  Fails on master; passes with the CF 6824 patch.
 */
PG_PROP(rewrite_with_vacuumed_toast_of_held_tuple)
{
	static const char *const ops[] = {
		"VACUUM FULL cfr_rw",
		"CLUSTER cfr_rw USING cfr_rw_pkey",
		"CREATE INDEX cfr_rw_i ON cfr_rw ((cfr_rw IS NOT NULL))",
	};
	pg_prop_conn *work = rc_connect(tc);
	pg_prop_conn *gholder;
	pg_prop_conn *sholder;
	const char *op = ops[pg_prop_draw_int(tc, 0, lengthof(ops) - 1)];
	int			nrows = (int) pg_prop_draw_int(tc, 2, 12);
	int			len = (int) pg_prop_draw_int(tc, 2500, 8000);
	int			delid = (int) pg_prop_draw_int(tc, 1, nrows);
	StringInfoData s;
	char	   *toast;
	bool		f = false;
	char	   *st = NULL;
	char	   *err = NULL;

	/* global cross-database xmin holder, open for the whole case */
	gholder = rc_connect_db(tc, "pgprop_rw_holder");
	rc_exec(tc, gholder, "BEGIN");
	rc_exec(tc, gholder, "CREATE TEMP TABLE IF NOT EXISTS cfr_xid_holder (x int)");
	/* ensure it has a real XID so it holds back the global horizon */
	rc_exec(tc, gholder, "INSERT INTO cfr_xid_holder VALUES (1)");

	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP TABLE IF EXISTS cfr_rw; "
					 "CREATE TABLE cfr_rw (id int PRIMARY KEY, data text) WITH (autovacuum_enabled = off); "
					 "ALTER TABLE cfr_rw ALTER COLUMN data SET STORAGE EXTERNAL; "
					 "INSERT INTO cfr_rw SELECT g, repeat('x', %d) FROM generate_series(1, %d) g",
					 len, nrows);
	rc_exec(tc, work, s.data);
	toast = rc_scalar(tc, work,
					  "SELECT 'pg_toast.' || relname FROM pg_class "
					  "WHERE oid = (SELECT reltoastrelid FROM pg_class WHERE relname = 'cfr_rw')");

	/* same-db holder pins the dead main tuple across the no-toast vacuum */
	sholder = rc_connect(tc);
	rc_exec(tc, sholder, "BEGIN ISOLATION LEVEL REPEATABLE READ");
	rc_exec(tc, sholder, "SELECT 1");
	resetStringInfo(&s);
	appendStringInfo(&s, "DELETE FROM cfr_rw WHERE id = %d", delid);
	rc_exec(tc, work, s.data);
	rc_exec(tc, work, "VACUUM (PROCESS_TOAST false) cfr_rw");
	rc_exec(tc, sholder, "COMMIT");
	pg_prop_disconnect(sholder);

	/* now remove the dead toast chunks while the main tuple is still held */
	resetStringInfo(&s);
	appendStringInfo(&s, "VACUUM %s", toast);
	rc_exec(tc, work, s.data);

	/* the operation under test */
	if (!pg_prop_sql_send(work, op))
		pg_prop_fail(tc, "could not dispatch %s", op);
	if (!pg_prop_sql_await(work, &st, &err))
	{
		if (err && (strstr(err, "missing chunk") || strstr(err, "unexpected chunk")))
			pg_prop_fail(tc, "%s failed after the tuple's toast was vacuumed: %s\n"
						 "  %d rows, %d-byte values, deleted id %d",
						 op, err, nrows, len, delid);
		if (rc_internal(st, err))
			pg_prop_fail(tc, "%s raised an internal error: %s", op, err ? err : "");
	}

	rc_exec(tc, gholder, "COMMIT");
	pg_prop_disconnect(gholder);
	pg_prop_sql_exec(work, "DROP TABLE IF EXISTS cfr_rw", NULL);
	pg_free(toast);
	pfree(s.data);
	pg_prop_disconnect(work);
	(void) f;
}
