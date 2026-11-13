/*-------------------------------------------------------------------------
 *
 * prop_sortmerge.c
 *	  Property-based tests for sort-key ordering, multi-column sort, and
 *	  incremental-sort equivalence.
 *
 * The oracle is differential and algebraic: an incremental sort (enabled over
 * a leading indexed prefix) must return exactly the same rows in the same
 * order as a full sort; a multi-key DESC-mixed sort must be idempotent under a
 * subquery wrapper; ORDER BY must preserve the multiset; an index-ordered scan
 * must agree with a seqscan+sort; the first row of an ordered scan must carry
 * the minimum key; and ties on the leading key must be broken by id.  Every
 * query carries a TOTAL order (ending in id) so the row-major comparison is
 * deterministic.  A disagreement is a wrong-results bug in the incremental
 * sort / tuplesort / index-ordering paths, distinct from LIMIT top-N
 * (sortlimit) and NULLS placement (orderbynulls).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_sortmerge.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "sortmerge"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * a is drawn from a tiny domain (0..3) so there are few distinct values on the
 * indexed leading key -- this is what makes an incremental sort possible (sort
 * the leading indexed prefix for free, then sort each group by the trailing
 * keys).  b is drawn from -5..5 with ~15% NULL so NULLS placement interacts
 * with the trailing sort key.
 */
#define A_MIN 0
#define A_MAX 3
#define B_MIN (-5)
#define B_MAX 5

/* Draw b: an integer in [B_MIN, B_MAX], or SQL NULL (~15% of the time). */
static void
draw_b(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, B_MIN, B_MAX));
}

/*
 * Build sm_t(id int PRIMARY KEY, a int, b int) with 0..30 rows.  id is a dense
 * sequence 0..n-1 so it is a unique total-order tiebreaker; a is drawn from
 * [A_MIN, A_MAX] (few distinct values, so an incremental sort over the index
 * on (a) is possible); b is drawn from [B_MIN, B_MAX] with ~15% NULL.  A btree
 * index on (a) is created so the planner can produce an index-ordered scan on
 * the leading key, and ANALYZE primes the stats.  Rebuilt fresh in every
 * property so each case is independent.  pg_prop_fail (not assume) on a build
 * error: a failed DDL/DML here is a harness bug, not a discarded case.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 30);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS sm_t; "
						   "CREATE TABLE sm_t (id int PRIMARY KEY, a int, b int); ");

	for (int r = 0; r < nrows; r++)
	{
		StringInfoData b;

		initStringInfo(&b);
		draw_b(tc, &b);
		appendStringInfo(&ddl,
						 "INSERT INTO sm_t VALUES (%d, %d, %s); ",
						 r, (int) pg_prop_draw_int(tc, A_MIN, A_MAX), b.data);
		pfree(b.data);
	}

	appendStringInfoString(&ddl,
						   "CREATE INDEX sm_t_a_idx ON sm_t (a); "
						   "ANALYZE sm_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build failed: %s\n  ddl: %s",
					 err ? err : "(no message)", ddl.data);

	pfree(ddl.data);
}

/*
 * Differential result-set oracle: run two queries that must return identical
 * rows in identical order, and fail on any mismatch.  Both queries must carry
 * a TOTAL order (ending in id) so the row-major string compares as a multiset.
 */
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
 * Scalar boolean oracle: run a SELECT <bool> and require it returns "t".  A
 * NULL result or a SQL error is a failure (the oracle is written to never
 * yield NULL on any input via coalesce/CASE guards).
 */
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
 * incsort_vs_fullsort (B): ORDER BY a, b NULLS LAST, id -- a has an index and
 * few distinct values, so an incremental sort over the leading prefix is
 * possible.  The result must be identical with incremental sort enabled vs
 * disabled.  Incremental sort is a performance optimisation that must never
 * change the row order.
 */
PG_PROP(incsort_vs_fullsort)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q =
		"SELECT id, a, b FROM sm_t ORDER BY a, b NULLS LAST, id";
	char	   *on,
			   *off;
	bool		f1 = false,
				f2 = false;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	pg_prop_sql_exec(conn, "SET enable_incremental_sort = on", &err);
	on = pg_prop_sql_rows(conn, q, &f1, &err);
	if (f1)
		pg_prop_fail(tc, "incsort-on query failed: %s", err ? err : "");

	pg_prop_sql_exec(conn, "SET enable_incremental_sort = off", &err);
	off = pg_prop_sql_rows(conn, q, &f2, &err);
	if (f2)
		pg_prop_fail(tc, "incsort-off query failed: %s", err ? err : "");

	pg_prop_sql_exec(conn, "RESET ALL", &err);

	if (strcmp(on, off) != 0)
		pg_prop_fail(tc,
					 "incremental sort changed results\n  on:\n%s\n  off:\n%s",
					 on, off);

	pg_free(on);
	pg_free(off);
	pg_prop_disconnect(conn);
}

/*
 * multikey_desc_mix (B): a mixed ASC/DESC multi-key sort must be idempotent --
 * wrapping the projection in a subquery and re-sorting identically yields the
 * same rows in the same order.  Exercises the comparator for a DESC key with
 * explicit NULLS LAST alongside ASC keys.
 */
PG_PROP(multikey_desc_mix)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM sm_t "
					  "ORDER BY a, b DESC NULLS LAST, id",
					  "SELECT id FROM (SELECT id, a, b FROM sm_t) s "
					  "ORDER BY a, b DESC NULLS LAST, id");

	pg_prop_disconnect(conn);
}

/*
 * sort_nondestructive (B): ORDER BY does not change the multiset of rows.
 * Grouping the a-values coming out of an ORDER BY must match grouping them
 * straight from the table.  A sort that dropped or duplicated a row would
 * break this.
 */
PG_PROP(sort_nondestructive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT a, count(*) FROM (SELECT a FROM sm_t ORDER BY a) s "
					  "GROUP BY a ORDER BY a",
					  "SELECT a, count(*) FROM sm_t GROUP BY a ORDER BY a");

	pg_prop_disconnect(conn);
}

/*
 * first_key_dominant (A): the first row of ORDER BY a, id (ASC, NULLS LAST by
 * default -- non-NULL a sorts first) carries the minimum non-NULL a, i.e. it
 * agrees with min(a).  Guarded with CASE so the empty table and the all-NULL-a
 * table (where min(a) is NULL and the first ordered row is NULL) pass soundly:
 * only when a non-NULL a exists do we require the leading row to equal min(a).
 */
PG_PROP(first_key_dominant)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT count(a) FROM sm_t) = 0 THEN true "
				 "ELSE (SELECT a FROM sm_t ORDER BY a, id LIMIT 1) "
				 "IS NOT DISTINCT FROM (SELECT min(a) FROM sm_t) END");

	pg_prop_disconnect(conn);
}

/*
 * index_sort_matches (B): ORDER BY a (indexed) must return identical rows
 * whether the plan uses a forced index-ordered scan or a seqscan+sort.  A
 * total order (a, id) makes the comparison deterministic.  A mismatch is a
 * bug in the index-ordering path.
 */
PG_PROP(index_sort_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	const char *q = "SELECT id, a FROM sm_t ORDER BY a, id";
	char	   *idx,
			   *seq;
	bool		f1 = false,
				f2 = false;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	/* forced index scan */
	pg_prop_sql_exec(conn, "SET enable_seqscan = off", &err);
	pg_prop_sql_exec(conn, "SET enable_bitmapscan = off", &err);
	idx = pg_prop_sql_rows(conn, q, &f1, &err);
	if (f1)
		pg_prop_fail(tc, "index-scan query failed: %s", err ? err : "");

	/* seqscan + sort */
	pg_prop_sql_exec(conn, "RESET ALL", &err);
	pg_prop_sql_exec(conn, "SET enable_indexscan = off", &err);
	pg_prop_sql_exec(conn, "SET enable_indexonlyscan = off", &err);
	seq = pg_prop_sql_rows(conn, q, &f2, &err);
	if (f2)
		pg_prop_fail(tc, "seqscan query failed: %s", err ? err : "");

	pg_prop_sql_exec(conn, "RESET ALL", &err);

	if (strcmp(idx, seq) != 0)
		pg_prop_fail(tc,
					 "index-ordered scan != seqscan+sort\n  idx:\n%s\n  seq:\n%s",
					 idx, seq);

	pg_free(idx);
	pg_free(seq);
	pg_prop_disconnect(conn);
}

/*
 * stable_within_key (A): under ORDER BY a, id, rows with equal a must be
 * ordered by increasing id -- the trailing total-order key fully breaks ties
 * on the leading key.  Using a window LAG over the same ordering, every row
 * must either start a new a-group or have a strictly larger id than its
 * predecessor.  coalesce(..., true) makes the empty table pass soundly
 * (bool_and over 0 rows is NULL).
 */
PG_PROP(stable_within_key)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and("
				 "prev_id IS NULL OR prev_a <> a OR prev_id < id), true) "
				 "FROM (SELECT a, id, "
				 "lag(a) OVER (ORDER BY a, id) prev_a, "
				 "lag(id) OVER (ORDER BY a, id) prev_id "
				 "FROM sm_t) s");

	pg_prop_disconnect(conn);
}
