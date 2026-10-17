/*-------------------------------------------------------------------------
 *
 * prop_indexscan.c
 *	  Property-based tests for scan-path equivalence over a b-tree index.
 *
 * The oracle is differential across access paths: a query answered by a
 * b-tree index scan, by a bitmap heap scan over that index, and by a plain
 * sequential scan must return the identical rows in the identical order.  The
 * three paths are forced one at a time with enable_* planner GUCs.  A scan
 * path is a performance choice that must never change results, so any
 * disagreement -- a missed row at a NULL boundary, a wrong range endpoint, a
 * mishandled IN-list -- is an index-scan or bitmap-scan bug.
 *
 * Distinct from the "nbtree" area, which checks b-tree internals directly;
 * this exercises the planner/executor scan paths that read through the index.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_indexscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "indexscan"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* k is drawn in [-5, 5] with ~15% NULL; v is a small int. */
#define K_MIN (-5)
#define K_MAX 5

/*
 * Rebuild ix_t fresh for each property so a case never sees state from a
 * previous one.  40-row domain with k in [-5,5] ~15% NULL and small v keeps a
 * counterexample legible while covering the equality/range/NULL boundaries
 * the scan paths must agree on.  id is a PRIMARY KEY (unique, non-NULL) so it
 * serves as a total-order tiebreak in every query.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ix_t; "
						   "CREATE TABLE ix_t (id int PRIMARY KEY, k int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 40);
	for (int i = 0; i < nrows; i++)
	{
		appendStringInfo(&ddl, "INSERT INTO ix_t VALUES (%d, ", i);
		if (pg_prop_draw_int(tc, 0, 6) == 0)	/* ~15% NULL */
			appendStringInfoString(&ddl, "NULL");
		else
			appendStringInfo(&ddl, "%d", (int) pg_prop_draw_int(tc, K_MIN, K_MAX));
		appendStringInfo(&ddl, ", %d); ", (int) pg_prop_draw_int(tc, 0, 9));
	}

	appendStringInfoString(&ddl,
						   "CREATE INDEX ix_k ON ix_t (k); "
						   "ANALYZE ix_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);
}

/* The three scan-path GUC presets, applied one at a time. */
static const char *const scan_gucs[3] = {
	/* index scan only */
	"SET enable_seqscan=off; SET enable_bitmapscan=off; "
	"SET enable_indexscan=on; SET enable_indexonlyscan=on;",
	/* bitmap scan only */
	"SET enable_seqscan=off; SET enable_indexscan=off; "
	"SET enable_indexonlyscan=off; SET enable_bitmapscan=on;",
	/* sequential scan only */
	"SET enable_seqscan=on; SET enable_indexscan=off; "
	"SET enable_bitmapscan=off; SET enable_indexonlyscan=off;"
};

/*
 * Run "query" under each of the three scan-path presets and require the full
 * result set to be identical across all three (run1==run2, run1==run3).  The
 * query must carry a total ORDER BY so the row-major string compares as an
 * ordered set.  RESET ALL afterwards so no preset leaks to the next query.
 */
static void
run_three_scan(pg_prop_tc *tc, pg_prop_conn *conn, const char *query)
{
	char	   *rows[3] = {NULL, NULL, NULL};
	char	   *err = NULL;

	for (int i = 0; i < 3; i++)
	{
		bool		failed = false;

		if (!pg_prop_sql_exec(conn, scan_gucs[i], &err))
			pg_prop_fail(tc, "GUC set failed (path %d): %s", i + 1,
						 err ? err : "(no message)");

		rows[i] = pg_prop_sql_rows(conn, query, &failed, &err);
		if (failed)
			pg_prop_fail(tc, "query failed (path %d): %s\n  SQL: %s",
						 i + 1, err ? err : "(no message)", query);
	}

	pg_prop_sql_exec(conn, "RESET ALL;", &err);

	if (strcmp(rows[0], rows[1]) != 0)
		pg_prop_fail(tc,
					 "index scan != bitmap scan\n  SQL: %s\n"
					 "  index:  %s\n  bitmap: %s",
					 query, rows[0], rows[1]);
	if (strcmp(rows[0], rows[2]) != 0)
		pg_prop_fail(tc,
					 "index scan != seq scan\n  SQL: %s\n"
					 "  index: %s\n  seq:   %s",
					 query, rows[0], rows[2]);

	pg_free(rows[0]);
	pg_free(rows[1]);
	pg_free(rows[2]);
}

/* Draw a k constant in [K_MIN, K_MAX]. */
static int
draw_k(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, K_MIN, K_MAX);
}

/*
 * equality_scan: WHERE k = <const>.  The index equality path must return the
 * same rows as the bitmap and sequential paths.
 */
PG_PROP(equality_scan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM ix_t WHERE k = %d ORDER BY id",
					 draw_k(tc));
	run_three_scan(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * range_scan: WHERE k BETWEEN <lo> AND <hi>.  Exercises the b-tree range
 * scan's endpoint handling against bitmap and seq scans.  lo/hi are drawn
 * independently; an empty range (lo > hi) is a legal input all three agree on.
 */
PG_PROP(range_scan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM ix_t WHERE k BETWEEN %d AND %d "
					 "ORDER BY id",
					 draw_k(tc), draw_k(tc));
	run_three_scan(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * in_list_scan: WHERE k IN (a, b, c).  A multi-value IN becomes a
 * ScalarArrayOp the b-tree index scan handles natively; it must match the
 * bitmap and seq paths.
 */
PG_PROP(in_list_scan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM ix_t WHERE k IN (%d, %d, %d) "
					 "ORDER BY id",
					 draw_k(tc), draw_k(tc), draw_k(tc));
	run_three_scan(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * is_null_scan: WHERE k IS NULL.  The b-tree stores NULLs; the index path must
 * return exactly the NULL rows that the sequential scan does.
 */
PG_PROP(is_null_scan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q = "SELECT id, k, v FROM ix_t WHERE k IS NULL ORDER BY id";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	run_three_scan(tc, conn, q);

	pg_prop_disconnect(conn);
}

/*
 * inequality_scan: WHERE k > <const> OR k IS NULL.  Combines an open-ended
 * index range with the NULL path in one predicate; all three scans must agree
 * on the union.
 */
PG_PROP(inequality_scan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, k, v FROM ix_t WHERE k > %d OR k IS NULL "
					 "ORDER BY id",
					 draw_k(tc));
	run_three_scan(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * ordered_by_indexed_col: SELECT id, k WHERE k IS NOT NULL ORDER BY k, id.
 * The index scan can satisfy the ORDER BY k by walking the index in order,
 * while the seq/bitmap paths must sort.  (k, id) is a total order (id unique),
 * so index-ordered retrieval must produce exactly the sorted seqscan output.
 */
PG_PROP(ordered_by_indexed_col)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q = "SELECT id, k FROM ix_t WHERE k IS NOT NULL "
	"ORDER BY k, id";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	run_three_scan(tc, conn, q);

	pg_prop_disconnect(conn);
}
