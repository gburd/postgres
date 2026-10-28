/*-------------------------------------------------------------------------
 *
 * prop_sortlimit.c
 *	  Property-based tests for ORDER BY / LIMIT / OFFSET and top-N heapsort.
 *
 * The oracle is differential and algebraic: a top-N sort (ORDER BY ... LIMIT k)
 * must return exactly the k-row prefix of the corresponding full sort, LIMIT
 * must bound cardinality at least(k, n), OFFSET past the end must be empty,
 * ORDER BY must preserve the multiset of rows, and LIMIT ALL must equal no
 * LIMIT.  A disagreement is a bug in nodeLimit / tuplesort's bounded (top-N
 * heapsort) path, which is a performance optimisation that must never change
 * results.  Every LIMIT query carries a TOTAL order (v NULLS LAST, id) so the
 * row-major comparison is deterministic.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_sortlimit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "sortlimit"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* v domain: small ints -5..5, ~15% NULL. */
#define VAL_MIN (-5)
#define VAL_MAX 5

/*
 * Build sl_t(id int PRIMARY KEY, v int) with 0..30 rows.  id is a dense
 * sequence 0..n-1 so it is a unique total-order tiebreaker; v is drawn from
 * [-5, 5] with ~15% NULL.  Rebuilt fresh in every property so each case is
 * independent.  pg_prop_fail (not assume) on a build error: a failed DDL/DML
 * here is a harness problem, not a discardable input.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 30);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS sl_t; "
						   "CREATE TABLE sl_t (id int PRIMARY KEY, v int); ");
	for (int i = 0; i < nrows; i++)
	{
		if (pg_prop_draw_int(tc, 0, 6) == 0)
			appendStringInfo(&ddl, "INSERT INTO sl_t VALUES (%d, NULL); ", i);
		else
			appendStringInfo(&ddl, "INSERT INTO sl_t VALUES (%d, %d); ",
							 i, (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
	}
	appendStringInfoString(&ddl, "ANALYZE sl_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
	if (err)
		pg_free(err);
}

/* Differential result-set oracle: two queries must return identical rows. */
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

/* Scalar boolean oracle: a SELECT <bool> must return "t". */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * 1. top-N (ORDER BY ... LIMIT k) equals the k-prefix of the full sort.
 *
 * q1 is the direct top-N form (tuplesort takes its bounded heapsort path);
 * q2 wraps the full sort in a subquery and LIMITs the outer query, so the
 * inner sort is unbounded.  The total order (v NULLS LAST, id) makes both
 * deterministic, so require_same_rows is sound.  k drawn 0..30.
 */
PG_PROP(topn_eq_full_sort_limit)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k;
	StringInfoData q1,
				q2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	k = (int) pg_prop_draw_int(tc, 0, 30);

	initStringInfo(&q1);
	initStringInfo(&q2);
	appendStringInfo(&q1,
					 "SELECT id FROM sl_t ORDER BY v NULLS LAST, id LIMIT %d", k);
	appendStringInfo(&q2,
					 "SELECT id FROM (SELECT id FROM sl_t "
					 "ORDER BY v NULLS LAST, id) s LIMIT %d", k);

	require_same_rows(tc, conn, q1.data, q2.data);

	pfree(q1.data);
	pfree(q2.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. LIMIT bounds cardinality at least(k, n).
 *
 * Sound shape-A form: the count of a LIMIT k query equals least(k, total).
 * k drawn 0..35 so it straddles the row count (0..30).  Both subselects are
 * aggregates over the table, so each returns exactly one row even when the
 * table is empty -- no NULL-from-no-rows hazard.
 */
PG_PROP(limit_bounds_cardinality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	k = (int) pg_prop_draw_int(tc, 0, 35);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM "
					 "(SELECT id FROM sl_t ORDER BY v NULLS LAST, id LIMIT %d) s) "
					 "= least(%d, (SELECT count(*) FROM sl_t))", k, k);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. ORDER BY never changes cardinality.
 *
 * Sound shape-A form: the count over an ordered subquery equals the count of
 * the base table.  Both sides are aggregates, so one row each even when empty.
 */
PG_PROP(order_preserves_cardinality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT (SELECT count(*) FROM "
						   "(SELECT * FROM sl_t ORDER BY v) a) "
						   "= (SELECT count(*) FROM sl_t)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. OFFSET at or beyond the row count returns zero rows.
 *
 * Sound shape-A form: a LIMIT-less query with OFFSET = count yields an empty
 * set, so wrapping it in count(*) gives 0.  The outer count aggregate always
 * returns one row.
 */
PG_PROP(offset_beyond_end_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT (SELECT count(*) FROM "
						   "(SELECT id FROM sl_t ORDER BY id "
						   "OFFSET (SELECT count(*) FROM sl_t)) s) = 0");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. ORDER BY preserves the multiset of rows.
 *
 * Differential: the per-value counts of the ordered subquery equal those of
 * the base table.  Both sides are grouped and ORDER BY v NULLS FIRST so the
 * row-major strings compare equal; the NULL group is included and lines up.
 */
PG_PROP(sort_stable_cardinality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q1 =
		"SELECT v, count(*) FROM (SELECT v FROM sl_t ORDER BY v) s "
		"GROUP BY v ORDER BY v NULLS FIRST";
	const char *q2 =
		"SELECT v, count(*) FROM sl_t GROUP BY v ORDER BY v NULLS FIRST";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn, q1, q2);

	pg_prop_disconnect(conn);
}

/*
 * 6. LIMIT ALL equals no LIMIT.
 *
 * Differential: ORDER BY id (a total order on the PRIMARY KEY) with LIMIT ALL
 * returns the same rows in the same order as the same query with no LIMIT.
 */
PG_PROP(limit_all_eq_no_limit)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q1 = "SELECT id FROM sl_t ORDER BY id LIMIT ALL";
	const char *q2 = "SELECT id FROM sl_t ORDER BY id";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn, q1, q2);

	pg_prop_disconnect(conn);
}
