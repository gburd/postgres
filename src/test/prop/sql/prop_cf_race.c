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
