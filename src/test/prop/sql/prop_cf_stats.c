/*-------------------------------------------------------------------------
 *
 * prop_cf_stats.c
 *	  Statistics must agree with a model of what was done.
 *
 * pg_class.reltuples and the pg_stat_*_tables counters feed the planner and
 * autovacuum.  They are computed incrementally (per-transaction pending
 * counts flushed later, VACUUM/ANALYZE estimates), so they can drift from
 * the truth in ways no query result shows.  Each property here performs a
 * drawn sequence of operations whose effect on a statistic is known exactly,
 * and compares.
 *
 *	 CF 5777  reltuples after VACUUM FULL / CLUSTER of an index-less table
 *			  with recently-dead tuples
 *	 CF 6619  pg_stat_xact_user_tables counts only the current transaction
 *	 CF 7460  VACUUM / ANALYZE do not double-count pending relation stats
 *	 CF 6368  pg_get_indexdef of a WITHOUT OVERLAPS constraint index reads back
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cf_stats"
#include "pg_prop.h"

#include "lib/stringinfo.h"

static pg_prop_conn *
st_connect(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

static void
st_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "statement failed: %s\n  SQL: %.1000s", err ? err : "", sql);
}

static double
st_num(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		f = false;
	char	   *err = NULL;
	char	   *v = pg_prop_sql_scalar(conn, sql, &f, &err);
	double		d;

	if (f || v == NULL)
		pg_prop_fail(tc, "query failed: %s\n  SQL: %s", err ? err : "", sql);
	d = atof(v);
	pg_free(v);
	return d;
}

/*
 * CF 5777: after VACUUM FULL or CLUSTER, reltuples is the number of live
 * rows -- even when another session holds a snapshot that keeps some deleted
 * rows "recently dead" (they are copied, but are not live).  The patch's
 * symptom is index-less tables counting the dead copies.  Drawn: rows,
 * fraction deleted, whether the table has an index, VACUUM FULL vs CLUSTER,
 * whether an old snapshot is held.
 */
PG_PROP(reltuples_after_rewrite_counts_live_rows)
{
	pg_prop_conn *a = st_connect(tc);
	pg_prop_conn *snap = st_connect(tc);
	int			n = (int) pg_prop_draw_int(tc, 10, 5000);
	int			del_mod = (int) pg_prop_draw_int(tc, 2, 5);
	bool		index = pg_prop_draw_bool(tc);
	bool		hold = pg_prop_draw_int(tc, 0, 3) != 0;
	bool		cluster = index && pg_prop_draw_bool(tc);
	StringInfoData s;
	double		live;
	double		rt;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP TABLE IF EXISTS cfs_rt; CREATE TABLE cfs_rt (id int, pad text) "
					 "WITH (autovacuum_enabled = off); "
					 "INSERT INTO cfs_rt SELECT i, repeat('x', 20) FROM generate_series(1, %d) i", n);
	st_exec(tc, a, s.data);
	if (index)
		st_exec(tc, a, "CREATE INDEX cfs_rt_id ON cfs_rt (id)");

	/*
	 * An old snapshot that keeps the rows we are about to delete "recently
	 * dead".  It must not touch cfs_rt: a lock on it would block VACUUM FULL
	 * / CLUSTER (AccessExclusiveLock) forever.  Taking a snapshot on any
	 * other relation holds back the xmin horizon just the same.
	 */
	if (hold)
	{
		st_exec(tc, snap, "BEGIN ISOLATION LEVEL REPEATABLE READ");
		st_exec(tc, snap, "SELECT count(*) FROM pg_class");
	}
	resetStringInfo(&s);
	appendStringInfo(&s, "DELETE FROM cfs_rt WHERE id %% %d = 0", del_mod);
	st_exec(tc, a, s.data);

	st_exec(tc, a, cluster ? "CLUSTER cfs_rt USING cfs_rt_id" : "VACUUM FULL cfs_rt");
	live = st_num(tc, a, "SELECT count(*) FROM cfs_rt");
	rt = st_num(tc, a, "SELECT reltuples FROM pg_class WHERE relname = 'cfs_rt'");
	if (hold)
		st_exec(tc, snap, "COMMIT");

	if (rt != live)
		pg_prop_fail(tc, "after %s, reltuples = %.0f but the table has %.0f live rows\n"
					 "  %d rows, every %d-th deleted, index %s, old snapshot %s",
					 cluster ? "CLUSTER" : "VACUUM FULL", rt, live, n, del_mod,
					 index ? "yes" : "no", hold ? "held" : "none");

	st_exec(tc, a, "DROP TABLE cfs_rt");
	pfree(s.data);
	pg_prop_disconnect(snap);
	pg_prop_disconnect(a);
}

/*
 * CF 6619: inside a transaction, pg_stat_xact_user_tables reports only that
 * transaction's own activity.  Drawn: earlier committed transactions (with
 * and without stats flushed in between), then the counted transaction.
 */
PG_PROP(xact_stats_count_only_this_transaction)
{
	pg_prop_conn *c = st_connect(tc);
	int			prior = (int) pg_prop_draw_int(tc, 0, 3);
	int			mine_ins = (int) pg_prop_draw_int(tc, 0, 200);
	int			mine_del = (int) pg_prop_draw_int(tc, 0, 3);
	StringInfoData s;
	double		ins;
	double		upd;

	initStringInfo(&s);
	st_exec(tc, c, "DROP TABLE IF EXISTS cfs_x; CREATE TABLE cfs_x (id int) WITH (autovacuum_enabled = off)");
	for (int i = 0; i < prior; i++)
	{
		resetStringInfo(&s);
		appendStringInfo(&s, "INSERT INTO cfs_x SELECT generate_series(1, %d)",
						 (int) pg_prop_draw_int(tc, 1, 300));
		st_exec(tc, c, s.data);
		if (pg_prop_draw_bool(tc))
			st_exec(tc, c, "UPDATE cfs_x SET id = id + 1 WHERE id % 5 = 0");
		if (pg_prop_draw_bool(tc))
			st_exec(tc, c, "SELECT pg_stat_force_next_flush()");
	}

	st_exec(tc, c, "BEGIN");
	resetStringInfo(&s);
	appendStringInfo(&s, "INSERT INTO cfs_x SELECT generate_series(1, %d)", mine_ins);
	st_exec(tc, c, s.data);
	for (int i = 0; i < mine_del; i++)
		st_exec(tc, c, "DELETE FROM cfs_x WHERE id = -1");	/* touches nothing */
	ins = st_num(tc, c, "SELECT n_tup_ins FROM pg_stat_xact_user_tables WHERE relname = 'cfs_x'");
	upd = st_num(tc, c, "SELECT n_tup_upd FROM pg_stat_xact_user_tables WHERE relname = 'cfs_x'");
	st_exec(tc, c, "COMMIT");

	if (ins != mine_ins || upd != 0)
		pg_prop_fail(tc, "pg_stat_xact_user_tables inside the transaction: n_tup_ins %.0f (expected %d), "
					 "n_tup_upd %.0f (expected 0) after %d earlier committed transactions",
					 ins, mine_ins, upd, prior);
	st_exec(tc, c, "DROP TABLE cfs_x");
	pfree(s.data);
	pg_prop_disconnect(c);
}

/*
 * CF 7460: ANALYZE or VACUUM run before a session's pending stats are
 * flushed must not count those rows twice: n_live_tup after the flush equals
 * the live row count (no updates or deletes leave estimates aside), and
 * n_dead_tup equals the rows deleted.  Drawn: rows, deletes, which of
 * ANALYZE / VACUUM runs before the flush.
 */
PG_PROP(vacuum_analyze_do_not_double_count_pending)
{
	pg_prop_conn *c = st_connect(tc);
	int			n = (int) pg_prop_draw_int(tc, 1, 3000);
	int			del = (int) pg_prop_draw_int(tc, 0, n);
	int			op = (int) pg_prop_draw_int(tc, 0, 2);
	StringInfoData s;
	double		livet;
	double		deadt;

	initStringInfo(&s);
	st_exec(tc, c, "DROP TABLE IF EXISTS cfs_dc; CREATE TABLE cfs_dc (id int) WITH (autovacuum_enabled = off)");
	st_exec(tc, c, "SELECT pg_stat_force_next_flush()");
	resetStringInfo(&s);
	appendStringInfo(&s, "INSERT INTO cfs_dc SELECT generate_series(1, %d)", n);
	st_exec(tc, c, s.data);
	resetStringInfo(&s);
	appendStringInfo(&s, "DELETE FROM cfs_dc WHERE id <= %d", del);
	st_exec(tc, c, s.data);
	/* the operation that runs while this session's counts are still pending */
	st_exec(tc, c, op == 0 ? "ANALYZE cfs_dc" : op == 1 ? "VACUUM cfs_dc" : "VACUUM ANALYZE cfs_dc");
	st_exec(tc, c, "SELECT pg_stat_force_next_flush()");
	/* a fresh statement makes the flushed counts visible */
	st_exec(tc, c, "SELECT pg_stat_clear_snapshot()");
	livet = st_num(tc, c, "SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'cfs_dc'");
	deadt = st_num(tc, c, "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'cfs_dc'");

	if (livet > (n - del) || livet < 0)
		pg_prop_fail(tc, "n_live_tup = %.0f exceeds the %d live rows after %s ran with pending stats\n"
					 "  inserted %d, deleted %d", livet, n - del,
					 op == 0 ? "ANALYZE" : op == 1 ? "VACUUM" : "VACUUM ANALYZE", n, del);
	if (op != 0 && deadt > del)
		pg_prop_fail(tc, "n_dead_tup = %.0f exceeds the %d deleted rows after %s ran with pending stats",
					 deadt, del, op == 1 ? "VACUUM" : "VACUUM ANALYZE");
	st_exec(tc, c, "DROP TABLE cfs_dc");
	pfree(s.data);
	pg_prop_disconnect(c);
}

/*
 * CF 6368: the definition pg_get_indexdef prints for a WITHOUT OVERLAPS
 * constraint's index must be accepted when re-executed (pg_dump relies on
 * it).  Drawn: PK vs UNIQUE, range vs multirange, extra key columns.
 */
PG_PROP(without_overlaps_indexdef_reads_back)
{
	pg_prop_conn *c = st_connect(tc);
	bool		pk = pg_prop_draw_bool(tc);
	bool		multi = pg_prop_draw_bool(tc);
	bool		extra = pg_prop_draw_bool(tc);
	StringInfoData s;
	char	   *def;
	bool		f = false;
	char	   *err = NULL;

	if (!pg_prop_require_extension(c, "btree_gist"))
		pg_prop_skip(tc, "btree_gist not installed");
	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP TABLE IF EXISTS cfs_wo; CREATE TABLE cfs_wo (id int, k int, valid_at %s, "
					 "CONSTRAINT cfs_wo_key %s (id%s, valid_at WITHOUT OVERLAPS))",
					 multi ? "int4multirange" : "int4range", pk ? "PRIMARY KEY" : "UNIQUE",
					 extra ? ", k" : "");
	st_exec(tc, c, s.data);
	def = pg_prop_sql_scalar(c,
							 "SELECT pg_get_indexdef(conindid) FROM pg_constraint WHERE conname = 'cfs_wo_key'",
							 &f, &err);
	if (f || def == NULL)
		pg_prop_fail(tc, "pg_get_indexdef failed: %s", err ? err : "");
	/* re-run the definition under a different name */
	resetStringInfo(&s);
	appendStringInfo(&s, "%s", def);
	{
		char	   *p = strstr(s.data, "cfs_wo_key");

		if (p)
			memcpy(p, "cfs_wo_kez", 10);
	}
	if (!pg_prop_sql_exec(c, s.data, &err))
		pg_prop_fail(tc, "pg_get_indexdef's output for a WITHOUT OVERLAPS %s does not read back: %s\n  %s",
					 pk ? "PRIMARY KEY" : "UNIQUE", err ? err : "", def);
	pg_free(def);
	st_exec(tc, c, "DROP TABLE cfs_wo");
	pfree(s.data);
	pg_prop_disconnect(c);
}
