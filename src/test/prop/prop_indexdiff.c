/*-------------------------------------------------------------------------
 *
 * prop_indexdiff.c
 *	  Property-based tests for index-vs-seqscan result equivalence.
 *
 * An index scan must return exactly the same rows as a sequential scan of the
 * same table for the same WHERE clause: an index is a performance structure,
 * never a different answer.  This differential oracle builds a small table,
 * creates one index per property (plain btree, GIN and GiST over an int via
 * contrib btree_gin/btree_gist, partial, expression, multicolumn), then forces
 * the planner onto the index path and onto a seqscan via enable_* GUCs and
 * requires the two result sets to be identical under a total ORDER BY.  Any
 * mismatch is a wrong-results bug in an index access method or the planner.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_indexdiff.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "indexdiff"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small integer domains: k in [-5, 5] with ~15% NULL exercises the =, BETWEEN
 * and IS NULL boundaries the AMs care about; v is a tiny non-negative value so
 * the partial-index predicate (v > 0) splits the table roughly in half.
 */
#define K_MIN (-5)
#define K_MAX 5

/*
 * Connect to the cluster and ensure the two contrib opclass extensions exist.
 * CREATE EXTENSION IF NOT EXISTS is idempotent, so this is safe to run on
 * every per-case connection.
 */
static pg_prop_conn *
connect_with_ext(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "CREATE EXTENSION IF NOT EXISTS btree_gin; "
						  "CREATE EXTENSION IF NOT EXISTS btree_gist",
						  &err))
		pg_prop_fail(tc, "setup failed (btree_gin/btree_gist): %s",
					 err ? err : "");

	return conn;
}

/*
 * Build id_t(id int PRIMARY KEY, k int, v int) with 0..40 rows, k in [-5, 5]
 * (~15% NULL), v a small non-negative value.  Fresh in every property so no
 * state leaks between cases.  id is 0..nrows-1 so it is a total-order key.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS id_t; "
						   "CREATE TABLE id_t (id int PRIMARY KEY, k int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 40);
	for (int i = 0; i < nrows; i++)
	{
		StringInfoData kval;

		initStringInfo(&kval);
		if (pg_prop_draw_int(tc, 0, 6) == 0)
			appendStringInfoString(&kval, "NULL");
		else
			appendStringInfo(&kval, "%d",
							 (int) pg_prop_draw_int(tc, K_MIN, K_MAX));

		appendStringInfo(&ddl, "INSERT INTO id_t VALUES (%d, %s, %d); ",
						 i, kval.data, (int) pg_prop_draw_int(tc, 0, 3));
		pfree(kval.data);
	}

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build failed: %s\n  SQL: %s", err ? err : "", ddl.data);
	pfree(ddl.data);
}

/* Shape B: two queries must return identical rows. */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *q1, const char *q2)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *r1,
			   *r2;

	r1 = pg_prop_sql_rows(conn, q1, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "q1 failed: %s\n  SQL: %s", e1 ? e1 : "", q1);
	r2 = pg_prop_sql_rows(conn, q2, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "q2 failed: %s\n  SQL: %s", e2 ? e2 : "", q2);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc, "mismatch:\n  q1: %s\n  q2: %s\n  r1:\n%s\n  r2:\n%s",
					 q1, q2, r1, r2);
	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * Run "select" first with index scans forced on and seqscan off, then with
 * seqscan forced and all index paths off, and require identical rows.  Resets
 * the GUCs afterwards.  bitmap controls whether the "index" side keeps bitmap
 * scans enabled (GIN produces only bitmap scans, so it must).
 */
static void
require_index_eq_seqscan(pg_prop_tc *tc, pg_prop_conn *conn,
						 const char *select, bool bitmap)
{
	char	   *err = NULL;
	char	   *idx;
	char	   *seq;
	bool		fi = false,
				fs = false;

	/* Force the index path: seqscan off; keep (bitmap)indexscan on. */
	if (bitmap)
		pg_prop_sql_exec(conn,
						 "SET enable_seqscan = off; "
						 "SET enable_indexscan = off; "
						 "SET enable_bitmapscan = on",
						 &err);
	else
		pg_prop_sql_exec(conn,
						 "SET enable_seqscan = off; "
						 "SET enable_indexscan = on; "
						 "SET enable_bitmapscan = off",
						 &err);
	idx = pg_prop_sql_rows(conn, select, &fi, &err);
	if (fi)
		pg_prop_fail(tc, "index-forced query failed: %s\n  SQL: %s",
					 err ? err : "", select);

	/* Force the seqscan path: all index paths off. */
	pg_prop_sql_exec(conn,
					 "SET enable_seqscan = on; "
					 "SET enable_indexscan = off; "
					 "SET enable_bitmapscan = off",
					 &err);
	seq = pg_prop_sql_rows(conn, select, &fs, &err);
	if (fs)
		pg_prop_fail(tc, "seqscan-forced query failed: %s\n  SQL: %s",
					 err ? err : "", select);

	pg_prop_sql_exec(conn, "RESET ALL", &err);

	if (strcmp(idx, seq) != 0)
		pg_prop_fail(tc,
					 "index != seqscan for query:\n  %s\n  index:\n%s\n  seqscan:\n%s",
					 select, idx, seq);

	pg_free(idx);
	pg_free(seq);
}

/*
 * 1. gin_equality: a GIN index over int (btree_gin opclass) answering k =
 * <const> must match a seqscan.  GIN yields only bitmap scans, so the index
 * side keeps bitmapscan enabled.
 */
PG_PROP(gin_equality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k;
	StringInfoData q;

	conn = connect_with_ext(tc);
	build_table(tc, conn);

	if (!pg_prop_sql_exec(conn,
						  "DROP INDEX IF EXISTS id_t_idx; "
						  "CREATE INDEX id_t_idx ON id_t USING gin (k); "
						  "ANALYZE id_t",
						  &err))
		pg_prop_fail(tc, "index build failed: %s", err ? err : "");

	k = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM id_t WHERE k = %d ORDER BY id",
					 k);

	require_index_eq_seqscan(tc, conn, q.data, true);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. gist_range: a GiST index over int (btree_gist opclass) answering
 * k BETWEEN lo AND hi must match a seqscan.
 */
PG_PROP(gist_range)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			lo,
				hi,
				t;
	StringInfoData q;

	conn = connect_with_ext(tc);
	build_table(tc, conn);

	if (!pg_prop_sql_exec(conn,
						  "DROP INDEX IF EXISTS id_t_idx; "
						  "CREATE INDEX id_t_idx ON id_t USING gist (k); "
						  "ANALYZE id_t",
						  &err))
		pg_prop_fail(tc, "index build failed: %s", err ? err : "");

	lo = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);
	hi = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);
	if (lo > hi)
	{
		t = lo;
		lo = hi;
		hi = t;
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM id_t WHERE k BETWEEN %d AND %d ORDER BY id",
					 lo, hi);

	require_index_eq_seqscan(tc, conn, q.data, false);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. partial_index: a partial btree indexed WHERE v > 0, used by a query whose
 * qual implies the predicate (k = <const> AND v > 0), must match a seqscan.
 */
PG_PROP(partial_index)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k;
	StringInfoData q;

	conn = connect_with_ext(tc);
	build_table(tc, conn);

	if (!pg_prop_sql_exec(conn,
						  "DROP INDEX IF EXISTS id_t_idx; "
						  "CREATE INDEX id_t_idx ON id_t (k) WHERE v > 0; "
						  "ANALYZE id_t",
						  &err))
		pg_prop_fail(tc, "index build failed: %s", err ? err : "");

	k = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM id_t WHERE k = %d AND v > 0 ORDER BY id",
					 k);

	require_index_eq_seqscan(tc, conn, q.data, false);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. expr_index: an expression index on (k*k), used by a query matching the
 * same expression (k*k = <square>), must match a seqscan.  const2 is a square
 * in [0, 25] so it is a value the expression can actually take.
 */
PG_PROP(expr_index)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			r;
	int			sq;
	StringInfoData q;

	conn = connect_with_ext(tc);
	build_table(tc, conn);

	if (!pg_prop_sql_exec(conn,
						  "DROP INDEX IF EXISTS id_t_idx; "
						  "CREATE INDEX id_t_idx ON id_t ((k * k)); "
						  "ANALYZE id_t",
						  &err))
		pg_prop_fail(tc, "index build failed: %s", err ? err : "");

	r = (int) pg_prop_draw_int(tc, 0, 5);
	sq = r * r;					/* a square in [0, 25] */

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM id_t WHERE k * k = %d ORDER BY id",
					 sq);

	require_index_eq_seqscan(tc, conn, q.data, false);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. multicol_btree: a multicolumn btree on (k, v), used by a prefix equality
 * (k = <const>) with ORDER BY k, v, id, must match a seqscan.  The ORDER BY is
 * a total order (id is unique) so the comparison is deterministic.
 */
PG_PROP(multicol_btree)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k;
	StringInfoData q;

	conn = connect_with_ext(tc);
	build_table(tc, conn);

	if (!pg_prop_sql_exec(conn,
						  "DROP INDEX IF EXISTS id_t_idx; "
						  "CREATE INDEX id_t_idx ON id_t (k, v); "
						  "ANALYZE id_t",
						  &err))
		pg_prop_fail(tc, "index build failed: %s", err ? err : "");

	k = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM id_t WHERE k = %d ORDER BY k, v, id",
					 k);

	require_index_eq_seqscan(tc, conn, q.data, false);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
