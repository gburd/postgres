/*-------------------------------------------------------------------------
 *
 * index_traits.c
 *	  Implementation of the trait-based index property harness.
 *
 * See index_traits.h for the trait vocabulary and the design.  The core
 * oracle throughout is differential: force the plan onto the index, force it
 * onto a sequential scan, and require identical rows.  An index is a
 * performance structure, never a different answer -- for every trait, the only
 * thing that changes is which qual/scan shape the trait exercises.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/index_traits.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "index_traits.h"

#define K_MIN (-8)
#define K_MAX 8
#define IDX_MAX_ROWS 60

/* Ask the catalog whether this AM claims a property (asc, can_order, ...). */
bool
am_claims(pg_prop_tc *tc, pg_prop_conn *conn, const char *amname,
		  const char *property)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	bool		yes;
	StringInfoData q;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(pg_indexam_has_property("
					 "(SELECT oid FROM pg_am WHERE amname = '%s'), '%s'), false)",
					 amname, property);
	v = pg_prop_sql_scalar(conn, q.data, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "property probe failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", q.data);
	yes = (v != NULL && strcmp(v, "t") == 0);
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	pfree(q.data);
	return yes;
}

/*
 * Index-level property probe (index_scan, bitmap_scan, backward_scan,
 * clusterable): these are properties of a built index, not the AM, so they are
 * queried via pg_index_has_property on an existing index (idxname).  NULL
 * (not-applicable) counts as false.
 */
static bool
index_claims(pg_prop_tc *tc, pg_prop_conn *conn, const char *idxname,
			 const char *property)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	bool		yes;
	StringInfoData q;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(pg_index_has_property('%s'::regclass, '%s'), false)",
					 idxname, property);
	v = pg_prop_sql_scalar(conn, q.data, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "index property probe failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", q.data);
	yes = (v != NULL && strcmp(v, "t") == 0);
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	pfree(q.data);
	return yes;
}

/*
 * Column-level property probe (asc, desc, orderable, search_nulls, returnable,
 * search_array) on column "col" of an existing index.
 */
static bool
col_claims(pg_prop_tc *tc, pg_prop_conn *conn, const char *idxname,
		   int col, const char *property)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	bool		yes;
	StringInfoData q;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(pg_index_column_has_property('%s'::regclass, %d, '%s'), false)",
					 idxname, col, property);
	v = pg_prop_sql_scalar(conn, q.data, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "column property probe failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", q.data);
	yes = (v != NULL && strcmp(v, "t") == 0);
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	pfree(q.data);
	return yes;
}

/* Build idx_t(id int PK, k int, v int, pl text) with a random row set. */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			n;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS idx_t; "
						   "CREATE TABLE idx_t (id int PRIMARY KEY, k int, "
						   "v int, pl text); ");
	n = (int) pg_prop_draw_int(tc, 0, IDX_MAX_ROWS);
	for (int i = 0; i < n; i++)
	{
		StringInfoData kv;

		initStringInfo(&kv);
		if (pg_prop_draw_int(tc, 0, 6) == 0)
			appendStringInfoString(&kv, "NULL");
		else
			appendStringInfo(&kv, "%d", (int) pg_prop_draw_int(tc, K_MIN, K_MAX));
		appendStringInfo(&ddl,
						 "INSERT INTO idx_t VALUES (%d, %s, %d, 'p%d'); ",
						 i, kv.data, (int) pg_prop_draw_int(tc, 0, 3), i);
		pfree(kv.data);
	}
	appendStringInfoString(&ddl, "ANALYZE idx_t;");
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s", err ? err : "");
	pfree(ddl.data);
}

/* Create idx_t_idx of the driver's AM over its colexpr; returns false on error
 * (some AM/column combinations are legitimately unsupported, in which case the
 * caller skips that trait rather than failing). */
static bool
build_index(pg_prop_conn *conn, const am_driver *d, const char *extra)
{
	StringInfoData ddl;
	char	   *err = NULL;
	bool		ok;

	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "DROP INDEX IF EXISTS idx_t_idx; "
					 "CREATE INDEX idx_t_idx ON idx_t USING %s (%s)%s;",
					 d->amname, d->colexpr, extra ? extra : "");
	ok = pg_prop_sql_exec(conn, ddl.data, &err);
	if (err)
		pg_free(err);
	pfree(ddl.data);
	return ok;
}

/*
 * Core differential: run "select" with the index path forced and again with a
 * seqscan forced, and require identical rows.  "want_bitmap" forces the bitmap
 * path (needed for AMs that produce only bitmap scans, e.g. gin/brin); else
 * the plain index scan.  Resets GUCs after.
 */
static void
index_eq_seqscan(pg_prop_tc *tc, pg_prop_conn *conn, const char *select,
				 bool want_bitmap)
{
	char	   *err = NULL;
	char	   *idx,
			   *seq;
	bool		fi = false,
				fs = false;

	if (want_bitmap)
		pg_prop_sql_exec(conn,
						 "SET enable_seqscan=off; SET enable_indexscan=off; "
						 "SET enable_bitmapscan=on", &err);
	else
		pg_prop_sql_exec(conn,
						 "SET enable_seqscan=off; SET enable_indexscan=on; "
						 "SET enable_bitmapscan=off", &err);
	idx = pg_prop_sql_rows(conn, select, &fi, &err);
	if (fi)
		pg_prop_fail(tc, "index-forced query failed: %s\n  SQL: %s",
					 err ? err : "", select);

	pg_prop_sql_exec(conn,
					 "SET enable_seqscan=on; SET enable_indexscan=off; "
					 "SET enable_bitmapscan=off", &err);
	seq = pg_prop_sql_rows(conn, select, &fs, &err);
	if (fs)
		pg_prop_fail(tc, "seqscan-forced query failed: %s\n  SQL: %s",
					 err ? err : "", select);

	pg_prop_sql_exec(conn, "RESET ALL", &err);

	if (strcmp(idx, seq) != 0)
		pg_prop_fail(tc,
					 "index != seqscan:\n  SQL: %s\n  index:\n%s\n  seqscan:\n%s",
					 select, idx, seq);
	pg_free(idx);
	pg_free(seq);
	if (err)
		pg_free(err);
}

/* ---- trait: exact (equality) ---------------------------------------- */
static void
trait_exact(pg_prop_tc *tc, pg_prop_conn *conn, const am_driver *d, bool bm)
{
	StringInfoData q;
	int			k = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);

	if (!build_index(conn, d, ""))
		return;
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM idx_t WHERE k = %d ORDER BY id", k);
	index_eq_seqscan(tc, conn, q.data, bm);
	pfree(q.data);
}

/* ---- trait: ordered (ranges, ORDER BY, backward) -------------------- */
static void
trait_ordered(pg_prop_tc *tc, pg_prop_conn *conn, const am_driver *d)
{
	StringInfoData q;
	int			lo = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);
	int			hi = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);
	int			t;

	if (lo > hi)
	{
		t = lo;
		lo = hi;
		hi = t;
	}
	if (!build_index(conn, d, ""))
		return;

	/* range qual */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM idx_t WHERE k BETWEEN %d AND %d "
					 "ORDER BY id", lo, hi);
	index_eq_seqscan(tc, conn, q.data, false);

	/* open-ended range */
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k FROM idx_t WHERE k >= %d ORDER BY id", lo);
	index_eq_seqscan(tc, conn, q.data, false);

	/* ORDER BY k satisfied by the index (ascending), vs sorted seqscan */
	resetStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT k FROM idx_t WHERE k IS NOT NULL "
						   "ORDER BY k, id");
	index_eq_seqscan(tc, conn, q.data, false);

	/* backward: ORDER BY k DESC */
	if (index_claims(tc, conn, "idx_t_idx", "backward_scan"))
	{
		resetStringInfo(&q);
		appendStringInfoString(&q,
							   "SELECT k FROM idx_t WHERE k IS NOT NULL "
							   "ORDER BY k DESC, id DESC");
		index_eq_seqscan(tc, conn, q.data, false);
	}
	pfree(q.data);
}

/* ---- trait: summarizing (BRIN-style lossy block ranges) ------------- */
static void
trait_summarizing(pg_prop_tc *tc, pg_prop_conn *conn, const am_driver *d)
{
	StringInfoData q;
	int			lo = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);
	int			hi = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);
	int			t;

	if (lo > hi)
	{
		t = lo;
		lo = hi;
		hi = t;
	}
	/* summarizing AMs only produce bitmap scans with recheck */
	if (!build_index(conn, d, ""))
		return;
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM idx_t WHERE k BETWEEN %d AND %d "
					 "ORDER BY id", lo, hi);
	index_eq_seqscan(tc, conn, q.data, true);
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k FROM idx_t WHERE k = %d ORDER BY id",
					 (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1));
	index_eq_seqscan(tc, conn, q.data, true);
	pfree(q.data);
}

/* ---- trait: multicol ------------------------------------------------ */
static void
trait_multicol(pg_prop_tc *tc, pg_prop_conn *conn, const am_driver *d)
{
	StringInfoData ddl;
	StringInfoData q;
	char	   *err = NULL;
	int			k = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);

	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "DROP INDEX IF EXISTS idx_t_idx; "
					 "CREATE INDEX idx_t_idx ON idx_t USING %s (%s, v);",
					 d->amname, d->colexpr);
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
	{
		if (err)
			pg_free(err);
		pfree(ddl.data);
		return;
	}
	pfree(ddl.data);
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM idx_t WHERE k = %d ORDER BY k, v, id",
					 k);
	index_eq_seqscan(tc, conn, q.data, false);
	pfree(q.data);
}

/* ---- trait: include (index-only scan) ------------------------------- */
static void
trait_include(pg_prop_tc *tc, pg_prop_conn *conn, const am_driver *d)
{
	StringInfoData ddl;
	StringInfoData q;
	char	   *err = NULL;
	int			k = (int) pg_prop_draw_int(tc, K_MIN - 1, K_MAX + 1);

	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "DROP INDEX IF EXISTS idx_t_idx; "
					 "CREATE INDEX idx_t_idx ON idx_t USING %s (%s) INCLUDE (v); "
					 "ANALYZE idx_t;",
					 d->amname, d->colexpr);
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
	{
		if (err)
			pg_free(err);
		pfree(ddl.data);
		return;
	}
	pfree(ddl.data);
	/* index-only scan path (SELECT only indexed+included cols) vs seqscan */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT k, v FROM idx_t WHERE k = %d ORDER BY k, v", k);
	index_eq_seqscan(tc, conn, q.data, false);
	pfree(q.data);
}

/* ---- trait: search_nulls -------------------------------------------- */
static void
trait_search_nulls(pg_prop_tc *tc, pg_prop_conn *conn, const am_driver *d)
{
	StringInfoData q;

	if (!build_index(conn, d, ""))
		return;
	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT id, k FROM idx_t WHERE k IS NULL ORDER BY id");
	index_eq_seqscan(tc, conn, q.data, false);
	resetStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT id, k FROM idx_t WHERE k IS NOT NULL "
						   "ORDER BY id");
	index_eq_seqscan(tc, conn, q.data, false);
	pfree(q.data);
}

/* ---- trait: unique -------------------------------------------------- */
static void
trait_unique(pg_prop_tc *tc, pg_prop_conn *conn, const am_driver *d)
{
	char	   *err = NULL;
	StringInfoData ddl;
	bool		dup_ok;

	/*
	 * A unique index must accept a table whose key column is already distinct
	 * and reject an attempt to insert a duplicate.  Build a fresh 3-row table
	 * with distinct keys, create the unique index (must succeed), then try to
	 * insert a duplicate key (must fail).
	 */
	pg_prop_sql_exec(conn,
					 "SET client_min_messages = warning; "
					 "DROP TABLE IF EXISTS uq_t; "
					 "CREATE TABLE uq_t (id int, k int); "
					 "INSERT INTO uq_t VALUES (1,10),(2,20),(3,30);", &err);
	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "CREATE UNIQUE INDEX uq_idx ON uq_t USING %s (k);",
					 d->amname);
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
	{
		/* AM claims can_unique but could not build on distinct keys: a bug */
		pg_prop_fail(tc, "unique index build on distinct keys failed: %s",
					 err ? err : "");
	}
	pfree(ddl.data);

	/* inserting a duplicate must be rejected */
	dup_ok = pg_prop_sql_exec(conn, "INSERT INTO uq_t VALUES (4,10);", &err);
	if (dup_ok)
		pg_prop_fail(tc, "unique index accepted a duplicate key");
	if (err)
		pg_free(err);
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS uq_t;", &err);
}

void
index_traits_check(pg_prop_tc *tc, const am_driver *d)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	bool		has_index_scan;
	bool		has_bitmap_scan;
	bool		has_search_nulls;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	if (d->setup_sql && !pg_prop_sql_exec(conn, d->setup_sql, &err))
		pg_prop_fail(tc, "AM setup failed: %s", err ? err : "");

	build_table(tc, conn);

	/*
	 * Build one index of the AM over column k so we can probe the index- and
	 * column-level properties (index_scan, bitmap_scan, search_nulls, ...),
	 * which are properties of a built index, not of the AM.  If the AM cannot
	 * index a plain int at all, there is nothing to test; return (vacuous).
	 */
	if (!build_index(conn, d, ""))
	{
		pg_prop_disconnect(conn);
		return;
	}
	has_index_scan = index_claims(tc, conn, "idx_t_idx", "index_scan");
	has_bitmap_scan = index_claims(tc, conn, "idx_t_idx", "bitmap_scan");
	has_search_nulls = col_claims(tc, conn, "idx_t_idx", 1, "search_nulls");

	/*
	 * Every searchable AM answers equality.  A bitmap-only AM (no plain index
	 * scan) must be exercised through the bitmap/recheck path.
	 */
	if (has_index_scan || has_bitmap_scan)
		trait_exact(tc, conn, d, !has_index_scan && has_bitmap_scan);

	/* ordered: declared by the AM (can_order) */
	if (am_claims(tc, conn, d->amname, "can_order"))
		trait_ordered(tc, conn, d);

	/*
	 * summarizing: a bitmap-only, non-ordered AM over a block range (BRIN).
	 * Lossy, so checked through the recheck (bitmap) path.
	 */
	if (has_bitmap_scan && !has_index_scan &&
		!am_claims(tc, conn, d->amname, "can_order"))
		trait_summarizing(tc, conn, d);

	if (am_claims(tc, conn, d->amname, "can_multi_col"))
		trait_multicol(tc, conn, d);

	if (am_claims(tc, conn, d->amname, "can_include"))
		trait_include(tc, conn, d);

	if (has_search_nulls)
		trait_search_nulls(tc, conn, d);

	if (am_claims(tc, conn, d->amname, "can_unique"))
		trait_unique(tc, conn, d);

	pg_prop_disconnect(conn);
}
