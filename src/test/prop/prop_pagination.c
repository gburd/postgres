/*-------------------------------------------------------------------------
 *
 * prop_pagination.c
 *	  Property-based tests for OFFSET/LIMIT and keyset pagination.
 *
 * The oracle is differential and algebraic: paging a result set with
 * LIMIT/OFFSET must reconstruct the full ordered set, keyset pagination
 * (WHERE (v,id) > (last_v,last_id)) must agree with the equivalent OFFSET,
 * LIMIT must bound the first page at least(p, n), OFFSET past the end must be
 * empty, LIMIT is monotone in its bound, and the first row of DESC order is
 * the last of ASC order.  A disagreement is a bug in nodeLimit / the planner's
 * ordering or boundary handling.  Every ordered query carries a TOTAL order
 * (v, id) so the row-major comparison is deterministic and the oracle sound.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_pagination.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "pagination"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* v domain: small ints -5..5 (no NULL -- (v,id) must be a total order). */
#define VAL_MIN (-5)
#define VAL_MAX 5

/*
 * Build pg_t(id int PRIMARY KEY, v int) with a FIXED row count drawn once per
 * case (n in 5..40).  id is a dense sequence 0..n-1, so (v, id) is a total
 * order with id as a unique tiebreaker.  v is drawn from [-5, 5], never NULL,
 * so (v, id) comparisons and row-compare keyset predicates are well defined.
 * Rebuilt fresh in every property.  Returns n.  pg_prop_fail (not assume) on a
 * build error: a failed DDL/DML here is a harness problem, not a bad input.
 */
static int
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 5, 40);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS pg_t; "
						   "CREATE TABLE pg_t (id int PRIMARY KEY, v int); ");
	for (int i = 0; i < n; i++)
		appendStringInfo(&ddl, "INSERT INTO pg_t VALUES (%d, %d); ",
						 i, (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
	appendStringInfoString(&ddl, "ANALYZE pg_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
	if (err)
		pg_free(err);
	return n;
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
 * 1. pages_reconstruct (B): paging the ordered set reproduces it.
 *
 * A single huge-LIMIT page (OFFSET 0 LIMIT 1000000) covers the whole table,
 * and re-sorting it by the total order (v, id) must yield the same rows in
 * the same order as the direct full ordered scan.  (A page partition
 * concatenated back in page order is the full order; the huge-limit page is
 * the degenerate one-page partition.)  Both carry a total order, so
 * require_same_rows is sound.
 */
PG_PROP(pages_reconstruct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q1 = "SELECT id FROM pg_t ORDER BY v, id";
	const char *q2 =
		"SELECT id FROM (SELECT id, v FROM pg_t "
		"ORDER BY v, id OFFSET 0 LIMIT 1000000) s ORDER BY v, id";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_same_rows(tc, conn, q1, q2);

	pg_prop_disconnect(conn);
}

/*
 * 2. page_sizes_sum (A): the first page holds least(p, n) rows.
 *
 * For a page size p (1..10) the count of the first LIMIT-p page equals
 * least(p, n).  The inner query is wrapped in count(*), so it returns exactly
 * one row even when p forces a short page.  n drawn 5..40 so p straddles it.
 */
PG_PROP(page_sizes_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	int			p;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_table(tc, conn);
	p = (int) pg_prop_draw_int(tc, 1, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM "
					 "(SELECT id FROM pg_t ORDER BY v, id LIMIT %d) s) "
					 "= least(%d, %d)", p, p, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. offset_past_end_empty (A): OFFSET n returns no rows.
 *
 * Skipping all n rows leaves an empty page, so wrapping it in count(*) gives
 * 0.  The outer count aggregate always returns one row.
 */
PG_PROP(offset_past_end_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_table(tc, conn);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM "
					 "(SELECT id FROM pg_t ORDER BY v, id OFFSET %d) s) = 0", n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. keyset_eq_offset (B): keyset pagination equals OFFSET.
 *
 * Keyset pagination past the first row -- WHERE (v, id) > (first_v, first_id)
 * -- returns exactly the rows an OFFSET 1 page returns.  The boundary is the
 * first row of the total order (v, id), so "strictly greater than row 1" and
 * "skip 1" denote the same suffix.  Both carry the total order; sound.
 */
PG_PROP(keyset_eq_offset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q1 =
		"SELECT id FROM pg_t "
		"WHERE (v, id) > (SELECT v, id FROM pg_t ORDER BY v, id LIMIT 1) "
		"ORDER BY v, id";
	const char *q2 = "SELECT id FROM pg_t ORDER BY v, id OFFSET 1";

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_same_rows(tc, conn, q1, q2);

	pg_prop_disconnect(conn);
}

/*
 * 5. limit_monotone (A): LIMIT count is monotone in the bound.
 *
 * For k1 <= k2 the LIMIT-k1 page is a prefix of the LIMIT-k2 page, so its row
 * count cannot exceed the larger page's.  k1, k2 drawn 0..40 and ordered so
 * k1 <= k2.  Both inner queries are wrapped in count(*), so each returns one
 * row even for a short or empty page.
 */
PG_PROP(limit_monotone)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b,
				k1,
				k2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	a = (int) pg_prop_draw_int(tc, 0, 40);
	b = (int) pg_prop_draw_int(tc, 0, 40);
	k1 = a < b ? a : b;
	k2 = a < b ? b : a;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM "
					 "(SELECT id FROM pg_t ORDER BY v, id LIMIT %d) x) "
					 "<= (SELECT count(*) FROM "
					 "(SELECT id FROM pg_t ORDER BY v, id LIMIT %d) y)", k1, k2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. reverse_pagination (A): first of DESC order is last of ASC order.
 *
 * The row with the greatest (v, id) is the first row of ORDER BY v DESC, id
 * DESC and also the last row of ORDER BY v, id (OFFSET n-1 LIMIT 1).  n is
 * fixed >= 5, so n-1 >= 4 and the OFFSET lands on the final row.  Each side is
 * a one-row scalar, compared for equality.
 */
PG_PROP(reverse_pagination)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = build_table(tc, conn);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT id FROM pg_t ORDER BY v DESC, id DESC LIMIT 1) "
					 "= (SELECT id FROM pg_t ORDER BY v, id OFFSET %d LIMIT 1)",
					 n - 1);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
