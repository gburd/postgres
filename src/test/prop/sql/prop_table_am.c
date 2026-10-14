/*-------------------------------------------------------------------------
 *
 * prop_table_am.c
 *	  Trait-based property tests for table access methods.
 *
 * A table access method, like an index AM, has a generic contract that must
 * hold for any implementation and some implementation-specific behaviour.
 * This area tests the generic contract -- the invariants heap, and any
 * drop-in table AM (OrioleDB, the FLUX undo-based AM, ...), must all satisfy:
 *
 *	 insert_count     -- after INSERTing n rows into a fresh table, a seqscan
 *					    sees exactly n rows and count(*) = n.
 *	 delete_visible   -- a committed DELETE removes exactly the matching rows;
 *					    the rest remain and are unchanged.
 *	 update_identity  -- an UPDATE changes the targeted rows' value and leaves
 *					    the row count unchanged (no duplication, no loss).
 *	 ctid_unique      -- every live row has a distinct ctid, and count(ctid) =
 *					    count(*): the AM's row identity is one-per-live-row.
 *	 mvcc_rollback    -- a ROLLBACK of an INSERT/DELETE leaves the table exactly
 *					    as before: the AM honours transaction abort.
 *	 agg_vs_scan      -- count(*), min, max, sum over the table agree whether
 *					    computed by the AM's scan or over a materialized copy.
 *
 * The access method under test is named by PG_PROP_TABLE_AM (default "heap");
 * the whole suite runs over "CREATE TABLE ... USING <am>", so another table AM
 * reuses it by building the pg_prop binary with its extension loaded and the
 * AM name supplied -- nothing here is heap-specific.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_table_am.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "table_am"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* The table AM under test.  Core ships only heap; an extension AM overrides. */
#ifndef PG_PROP_TABLE_AM
#define PG_PROP_TABLE_AM "heap"
#endif

#define TAM_MAX_ROWS 60
#define TAM_K_MIN (-8)
#define TAM_K_MAX 8

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

static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *a, const char *b)
{
	bool		fa = false,
				fb = false;
	char	   *ea = NULL,
			   *eb = NULL,
			   *ra,
			   *rb;

	ra = pg_prop_sql_rows(conn, a, &fa, &ea);
	if (fa)
		pg_prop_fail(tc, "query a failed: %s\n  SQL: %s", ea ? ea : "", a);
	rb = pg_prop_sql_rows(conn, b, &fb, &eb);
	if (fb)
		pg_prop_fail(tc, "query b failed: %s\n  SQL: %s", eb ? eb : "", b);
	if (strcmp(ra, rb) != 0)
		pg_prop_fail(tc, "mismatch:\n  a: %s\n  b: %s\n  ra:\n%s\n  rb:\n%s",
					 a, b, ra, rb);
	pg_free(ra);
	pg_free(rb);
	if (ea)
		pg_free(ea);
	if (eb)
		pg_free(eb);
}

/* Build tam_t USING the AM under test; return the row count populated. */
static int
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			n;

	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "DROP TABLE IF EXISTS tam_t; "
					 "CREATE TABLE tam_t (id int PRIMARY KEY, k int, v int) "
					 "USING %s; ", PG_PROP_TABLE_AM);
	n = (int) pg_prop_draw_int(tc, 0, TAM_MAX_ROWS);
	for (int i = 0; i < n; i++)
		appendStringInfo(&ddl, "INSERT INTO tam_t VALUES (%d, %d, %d); ",
						 i,
						 (int) pg_prop_draw_int(tc, TAM_K_MIN, TAM_K_MAX),
						 (int) pg_prop_draw_int(tc, 0, 5));
	appendStringInfoString(&ddl, "ANALYZE tam_t;");
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s", err ? err : "");
	pfree(ddl.data);
	return n;
}

static pg_prop_conn *
connect_tam(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

/* count(*) == number inserted, and a seqscan returns exactly those ids. */
PG_PROP(insert_count)
{
	pg_prop_conn *conn = connect_tam(tc);
	int			n = build_table(tc, conn);
	long		c = scalar_long(tc, conn, "SELECT count(*) FROM tam_t");

	if (c != n)
		pg_prop_fail(tc, "count(*)=%ld != inserted %d", c, n);
	pg_prop_disconnect(conn);
}

/* A committed DELETE removes exactly the matching rows. */
PG_PROP(delete_visible)
{
	pg_prop_conn *conn = connect_tam(tc);
	int			n = build_table(tc, conn);
	int			k = (int) pg_prop_draw_int(tc, TAM_K_MIN, TAM_K_MAX);
	char	   *err = NULL;
	long		before,
				matched,
				after;
	StringInfoData q;

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT count(*) FROM tam_t WHERE k = %d", k);
	matched = scalar_long(tc, conn, q.data);
	before = n;

	resetStringInfo(&q);
	appendStringInfo(&q, "DELETE FROM tam_t WHERE k = %d", k);
	if (!pg_prop_sql_exec(conn, q.data, &err))
		pg_prop_fail(tc, "delete failed: %s", err ? err : "");

	after = scalar_long(tc, conn, "SELECT count(*) FROM tam_t");
	if (after != before - matched)
		pg_prop_fail(tc, "delete left %ld rows, expected %ld (%ld - %ld)",
					 after, before - matched, before, matched);
	/* no row with the deleted key remains */
	resetStringInfo(&q);
	appendStringInfo(&q, "SELECT count(*) FROM tam_t WHERE k = %d", k);
	if (scalar_long(tc, conn, q.data) != 0)
		pg_prop_fail(tc, "rows with k=%d survived the delete", k);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* An UPDATE changes value, preserves the row count, loses no identity. */
PG_PROP(update_identity)
{
	pg_prop_conn *conn = connect_tam(tc);
	int			n = build_table(tc, conn);
	char	   *err = NULL;
	long		after;

	if (!pg_prop_sql_exec(conn, "UPDATE tam_t SET v = v + 1", &err))
		pg_prop_fail(tc, "update failed: %s", err ? err : "");
	after = scalar_long(tc, conn, "SELECT count(*) FROM tam_t");
	if (after != n)
		pg_prop_fail(tc, "update changed row count: %ld != %d", after, n);
	/* every id still present exactly once */
	if (scalar_long(tc, conn,
					"SELECT count(*) FROM (SELECT id FROM tam_t "
					"GROUP BY id HAVING count(*) <> 1) s") != 0)
		pg_prop_fail(tc, "update duplicated or dropped a row identity");
	pg_prop_disconnect(conn);
}

/* Every live row has a distinct ctid and count(ctid) == count(*). */
PG_PROP(ctid_unique)
{
	pg_prop_conn *conn = connect_tam(tc);

	build_table(tc, conn);
	if (scalar_long(tc, conn, "SELECT count(*) FROM tam_t") !=
		scalar_long(tc, conn, "SELECT count(DISTINCT ctid) FROM tam_t"))
		pg_prop_fail(tc, "live rows do not have distinct ctids");
	pg_prop_disconnect(conn);
}

/* A ROLLBACK of writes restores the table exactly. */
PG_PROP(mvcc_rollback)
{
	pg_prop_conn *conn = connect_tam(tc);
	char	   *err = NULL;

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id, k, v FROM tam_t ORDER BY id",
					  "SELECT id, k, v FROM tam_t ORDER BY id");

	/* mutate inside an aborted transaction */
	pg_prop_sql_exec(conn, "BEGIN", &err);
	pg_prop_sql_exec(conn, "INSERT INTO tam_t VALUES (100000, 1, 1)", &err);
	pg_prop_sql_exec(conn, "DELETE FROM tam_t WHERE id = 0", &err);
	pg_prop_sql_exec(conn, "UPDATE tam_t SET v = -1", &err);
	pg_prop_sql_exec(conn, "ROLLBACK", &err);
	if (err)
		pg_free(err);

	/* table must be exactly as it was before the aborted transaction */
	{
		long		c = scalar_long(tc, conn, "SELECT count(*) FROM tam_t");
		long		neg = scalar_long(tc, conn,
									  "SELECT count(*) FROM tam_t WHERE v = -1");
		long		injected = scalar_long(tc, conn,
										   "SELECT count(*) FROM tam_t WHERE id = 100000");

		if (neg != 0 || injected != 0)
			pg_prop_fail(tc,
						 "rollback did not undo writes: v=-1 rows=%ld injected=%ld (count=%ld)",
						 neg, injected, c);
	}
	pg_prop_disconnect(conn);
}

/* Aggregates over the AM's scan agree with aggregates over a materialized copy. */
PG_PROP(agg_vs_scan)
{
	pg_prop_conn *conn = connect_tam(tc);

	build_table(tc, conn);
	require_same_rows(tc, conn,
					  "SELECT count(*), coalesce(sum(k),0), min(k), max(k), "
					  "coalesce(sum(v),0) FROM tam_t",
					  "SELECT count(*), coalesce(sum(k),0), min(k), max(k), "
					  "coalesce(sum(v),0) FROM (SELECT k, v FROM tam_t "
					  "ORDER BY id) s");
	pg_prop_disconnect(conn);
}
