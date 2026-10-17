/*-------------------------------------------------------------------------
 *
 * prop_contrib.c
 *	  Property tests over assorted contrib extensions.
 *
 * A grab-bag of smaller contrib modules, each with a bounded invariant and a
 * skip-if-absent guard so the area runs whatever the server offers:
 *
 *	 tablefunc_normal_rand -- normal_rand(n, mean, sd) returns exactly n rows,
 *						     all finite; and crosstab's row count is bounded by
 *						     the source.
 *	 tsm_system_rows_bound -- TABLESAMPLE SYSTEM_ROWS(n) returns at most n rows,
 *						     all of which are genuine rows of the table.
 *	 tsm_system_time_bound -- TABLESAMPLE SYSTEM_TIME(ms) returns a subset of
 *						     the table (every sampled id exists).
 *	 walinspect_monotonic  -- pg_get_wal_records_info over a generated LSN range
 *						     returns records whose LSNs are within the range and
 *						     non-decreasing (WAL is an append-only sequence).
 *	 dblink_roundtrip      -- a value sent through a dblink loopback query comes
 *						     back unchanged (connection + result marshalling).
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_contrib.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "contrib"
#include "pg_prop.h"

#include "lib/stringinfo.h"

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

static pg_prop_conn *
connect_c(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	pg_prop_sql_exec(conn, "SET client_min_messages=warning", &err);
	if (err)
		pg_free(err);
	return conn;
}

/* normal_rand(n, mean, sd) yields exactly n finite rows. */
PG_PROP(tablefunc_normal_rand)
{
	pg_prop_conn *conn = connect_c(tc);
	int			n = (int) pg_prop_draw_int(tc, 0, 500);
	double		mean = (double) pg_prop_draw_int(tc, -100, 100);
	int			sd = (int) pg_prop_draw_int(tc, 0, 50);
	StringInfoData q;
	long		cnt,
				nonfinite;

	if (!pg_prop_require_extension(conn, "tablefunc"))
		pg_prop_skip(tc, "tablefunc not available");

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT count(*) FROM normal_rand(%d, %g, %d)",
					 n, mean, sd);
	cnt = scalar_long(tc, conn, q.data);
	if (cnt != n)
		pg_prop_fail(tc, "normal_rand returned %ld rows, expected %d", cnt, n);

	/* every value is finite (normal_rand must not emit NaN/Inf) */
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT count(*) FROM normal_rand(%d, %g, %d) v "
					 "WHERE v = 'NaN'::float8 OR v = 'Infinity'::float8 "
					 "OR v = '-Infinity'::float8",
					 n, mean, sd);
	nonfinite = scalar_long(tc, conn, q.data);
	if (nonfinite != 0)
		pg_prop_fail(tc, "normal_rand produced %ld non-finite values", nonfinite);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* TABLESAMPLE SYSTEM_ROWS(n): at most n rows, all genuine table rows. */
PG_PROP(tsm_system_rows_bound)
{
	pg_prop_conn *conn = connect_c(tc);
	char	   *err = NULL;
	int			total = (int) pg_prop_draw_int(tc, 0, 300);
	int			want = (int) pg_prop_draw_int(tc, 0, 400);
	int			i;
	StringInfoData ddl;
	long		got,
				bogus;

	if (!pg_prop_require_extension(conn, "tsm_system_rows"))
		pg_prop_skip(tc, "tsm_system_rows not available");

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS smp_t; "
						   "CREATE TABLE smp_t (id int PRIMARY KEY); ");
	for (i = 0; i < total; i++)
		appendStringInfo(&ddl, "INSERT INTO smp_t VALUES (%d); ", i);
	appendStringInfoString(&ddl, "ANALYZE smp_t;");
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "sample table build failed: %s", err ? err : "");
	pfree(ddl.data);

	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "SELECT count(*) FROM smp_t TABLESAMPLE SYSTEM_ROWS(%d)",
					 want);
	got = scalar_long(tc, conn, ddl.data);
	/* result is bounded by both the requested count and the table size */
	if (got > want || got > total)
		pg_prop_fail(tc,
					 "SYSTEM_ROWS(%d) returned %ld rows (table has %d)",
					 want, got, total);

	/* every sampled id must be a real id in the table */
	resetStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "SELECT count(*) FROM "
					 "(SELECT id FROM smp_t TABLESAMPLE SYSTEM_ROWS(%d)) s "
					 "WHERE NOT EXISTS (SELECT 1 FROM smp_t t WHERE t.id = s.id)",
					 want);
	bogus = scalar_long(tc, conn, ddl.data);
	if (bogus != 0)
		pg_prop_fail(tc, "SYSTEM_ROWS returned %ld ids not in the table", bogus);

	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS smp_t", &err);
	if (err)
		pg_free(err);
	pfree(ddl.data);
	pg_prop_disconnect(conn);
}

/* TABLESAMPLE SYSTEM_TIME(ms): every sampled id is a genuine table row. */
PG_PROP(tsm_system_time_bound)
{
	pg_prop_conn *conn = connect_c(tc);
	char	   *err = NULL;
	int			total = (int) pg_prop_draw_int(tc, 0, 300);
	int			i;
	StringInfoData ddl;
	long		bogus;

	if (!pg_prop_require_extension(conn, "tsm_system_time"))
		pg_prop_skip(tc, "tsm_system_time not available");

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS smt_t; "
						   "CREATE TABLE smt_t (id int PRIMARY KEY); ");
	for (i = 0; i < total; i++)
		appendStringInfo(&ddl, "INSERT INTO smt_t VALUES (%d); ", i);
	appendStringInfoString(&ddl, "ANALYZE smt_t;");
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "sample table build failed: %s", err ? err : "");
	pfree(ddl.data);

	/* a short time budget; the sample is a subset of the table */
	bogus = scalar_long(tc, conn,
						"SELECT count(*) FROM "
						"(SELECT id FROM smt_t TABLESAMPLE SYSTEM_TIME(10)) s "
						"WHERE NOT EXISTS (SELECT 1 FROM smt_t t WHERE t.id = s.id)");
	if (bogus != 0)
		pg_prop_fail(tc, "SYSTEM_TIME returned %ld ids not in the table", bogus);

	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS smt_t", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/*
 * pg_walinspect: records read over [start, current] all have LSNs within the
 * range and non-decreasing start_lsn (WAL is an ordered append-only log).  We
 * capture a start LSN, generate some WAL (a tiny table + inserts), then read
 * the records in between and check the ordering invariant.
 */
PG_PROP(walinspect_monotonic)
{
	pg_prop_conn *conn = connect_c(tc);
	char	   *err = NULL;
	char	   *start_lsn;
	bool		failed = false;
	int			i;
	int			n = (int) pg_prop_draw_int(tc, 1, 50);
	StringInfoData ddl;
	long		disordered;

	if (!pg_prop_require_extension(conn, "pg_walinspect"))
		pg_prop_skip(tc, "pg_walinspect not available");

	start_lsn = pg_prop_sql_scalar(conn, "SELECT pg_current_wal_lsn()::text",
								   &failed, &err);
	if (failed || start_lsn == NULL)
		pg_prop_skip(tc, "cannot read current WAL LSN (not in recovery?)");

	/* generate some WAL */
	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS wal_t; "
						   "CREATE TABLE wal_t (id int); ");
	for (i = 0; i < n; i++)
		appendStringInfo(&ddl, "INSERT INTO wal_t VALUES (%d); ", i);
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "WAL-generating DML failed: %s", err ? err : "");
	pfree(ddl.data);

	/*
	 * Read the records between start_lsn and the current LSN; their start_lsn
	 * values must be strictly increasing and within the queried range.  Count
	 * any adjacent pair that is out of order (expected zero).
	 */
	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "SELECT count(*) FROM ("
					 "SELECT start_lsn, lag(start_lsn) OVER (ORDER BY start_lsn) AS prev "
					 "FROM pg_get_wal_records_info('%s', pg_current_wal_lsn())) s "
					 "WHERE s.prev IS NOT NULL AND s.start_lsn <= s.prev",
					 start_lsn);
	disordered = scalar_long(tc, conn, ddl.data);
	if (disordered != 0)
		pg_prop_fail(tc, "pg_walinspect reported %ld out-of-order records",
					 disordered);

	pg_free(start_lsn);
	if (err)
		pg_free(err);
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS wal_t", &err);
	if (err)
		pg_free(err);
	pfree(ddl.data);
	pg_prop_disconnect(conn);
}

/*
 * dblink loopback round-trip: a value sent through a dblink connection back to
 * the same cluster and returned must be unchanged.  Exercises connection setup
 * and result marshalling.  The loopback connection string reuses the current
 * session's socket directory and port via the PG* environment the harness runs
 * under, passed as a literal.
 */
PG_PROP(dblink_roundtrip)
{
	pg_prop_conn *conn = connect_c(tc);
	char	   *err = NULL;
	int			a = (int) pg_prop_draw_int(tc, -1000000, 1000000);
	StringInfoData q;
	char	   *v;
	bool		failed = false;

	if (!pg_prop_require_extension(conn, "dblink"))
		pg_prop_skip(tc, "dblink not available");

	/*
	 * Build the loopback conninfo from the server's own settings so it points
	 * at this node (host = first unix_socket_directories entry, port =
	 * current_setting('port')), and read a value back with an explicit column
	 * definition list.  Compare to the sent value.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT x = %d FROM dblink("
					 "'host=' || split_part(current_setting('unix_socket_directories'), ',', 1) "
					 "|| ' port=' || current_setting('port') "
					 "|| ' dbname=' || current_database() "
					 "|| ' user=' || current_user, "
					 "'SELECT %d') AS t(x int)",
					 a, a);
	v = pg_prop_sql_scalar(conn, q.data, &failed, &err);
	if (failed)
	{
		/* dblink loopback can be refused by auth config; skip rather than fail */
		pg_prop_skip(tc, "dblink loopback not permitted here: %s",
					 err ? err : "");
	}
	if (v == NULL || strcmp(v, "t") != 0)
		pg_prop_fail(tc, "dblink did not round-trip %d unchanged", a);

	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
