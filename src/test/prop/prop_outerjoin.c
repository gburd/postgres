/*-------------------------------------------------------------------------
 *
 * prop_outerjoin.c
 *	  Property-based tests for outer-join identities and reordering.
 *
 * Where prop_joinmethods checks that one join query is method-independent,
 * this targets the relational identities that relate different join SHAPES:
 * LEFT vs INNER row counts, LEFT = swapped RIGHT, FULL = LEFT + the B-only
 * antijoin, and the commutativity of an inner join's result.  The oracle is
 * differential (two forms must return identical ordered rows) or a scalar
 * boolean the backend must answer "t"; where a shape has distinct executor
 * paths per method, run_three forces each method and requires agreement.  A
 * mismatch is a wrong-results bug in outer-join handling (NULL-extension of
 * the unmatched side, full-join merge/hash state) or in join reordering.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_outerjoin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "outerjoin"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small key domain with ~20% NULLs is the point: a plain = join never matches
 * NULL k to NULL k, but an outer join still emits the unmatched (NULL-keyed)
 * side NULL-extended.  Values v give the generated schema a payload column so
 * a mis-extended row is visible in a counterexample.
 */
#define K_MIN (-3)
#define K_MAX 3
#define V_MIN (-2)
#define V_MAX 2

/* Draw a key value in [K_MIN, K_MAX], or SQL NULL (~20% of the time). */
static void
draw_key(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 4) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, K_MIN, K_MAX));
}

/*
 * Build one table "name"(id int PRIMARY KEY, k int, v int) with 0..12 rows.
 * id is a dense 0-based sequence (unique, so it is a sound total-order tie
 * breaker); k is drawn with NULLs; v is a small int.  Discards the case if
 * the DDL/DML is rejected, which should not happen for this fixed schema but
 * keeps a surprise from masquerading as a bug.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn, const char *name)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "DROP TABLE IF EXISTS %s; "
					 "CREATE TABLE %s (id int PRIMARY KEY, k int, v int); ",
					 name, name);

	nrows = (int) pg_prop_draw_int(tc, 0, 12);
	for (int r = 0; r < nrows; r++)
	{
		StringInfoData k;

		initStringInfo(&k);
		draw_key(tc, &k);
		appendStringInfo(&ddl,
						 "INSERT INTO %s VALUES (%d, %s, %d); ",
						 name, r, k.data,
						 (int) pg_prop_draw_int(tc, V_MIN, V_MAX));
		pfree(k.data);
	}

	appendStringInfo(&ddl, "ANALYZE %s; ", name);

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
	{
		pg_prop_note(tc, "table build rejected for %s: %s",
					 name, err ? err : "(no message)");
		pg_prop_note(tc, "ddl: %s", ddl.data);
		if (err)
			pg_free(err);
		pfree(ddl.data);
		pg_prop_assume(tc, false);	/* longjmps */
	}
	pfree(ddl.data);
}

/* Build both tables fresh for a case. */
static void
build_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	build_table(tc, conn, "oj_a");
	build_table(tc, conn, "oj_b");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is itself a failure, reported with the SQL.
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
 * Differential result-set oracle: run two queries that must return identical
 * ordered rows and fail on any disagreement.  Both queries MUST carry a total
 * ORDER BY so the newline-joined row-major rendering compares as a multiset.
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
 * Run "query" three ways -- hashjoin only, mergejoin only, nestloop only --
 * and require all three to produce the identical ordered row set.  RESET ALL
 * after the runs so the forced GUCs cannot leak out.  "query" must carry its
 * own total ORDER BY.
 */
static void
run_three(pg_prop_tc *tc, pg_prop_conn *conn, const char *query)
{
	char	   *err = NULL;
	char	   *run1;
	char	   *run2;
	char	   *run3;
	bool		f1,
				f2,
				f3;

	/* 1. hashjoin only */
	pg_prop_sql_exec(conn,
					 "SET enable_hashjoin = on; "
					 "SET enable_mergejoin = off; "
					 "SET enable_nestloop = off;",
					 &err);
	run1 = pg_prop_sql_rows(conn, query, &f1, &err);

	/* 2. mergejoin only */
	pg_prop_sql_exec(conn,
					 "SET enable_hashjoin = off; "
					 "SET enable_mergejoin = on; "
					 "SET enable_nestloop = off;",
					 &err);
	run2 = pg_prop_sql_rows(conn, query, &f2, &err);

	/* 3. nestloop only */
	pg_prop_sql_exec(conn,
					 "SET enable_hashjoin = off; "
					 "SET enable_mergejoin = off; "
					 "SET enable_nestloop = on;",
					 &err);
	run3 = pg_prop_sql_rows(conn, query, &f3, &err);

	/* Restore defaults regardless of outcome. */
	pg_prop_sql_exec(conn, "RESET ALL;", &err);

	if (f1 || f2 || f3)
		pg_prop_fail(tc, "query error for: %s\n  error: %s",
					 query, err ? err : "(unknown)");

	if (strcmp(run1, run2) != 0)
		pg_prop_fail(tc,
					 "hashjoin != mergejoin for: %s\n"
					 "  hashjoin:  %s\n  mergejoin: %s",
					 query, run1, run2);
	if (strcmp(run1, run3) != 0)
		pg_prop_fail(tc,
					 "hashjoin != nestloop for: %s\n"
					 "  hashjoin: %s\n  nestloop: %s",
					 query, run1, run3);

	pg_free(run1);
	pg_free(run2);
	pg_free(run3);
	if (err)
		pg_free(err);
}

/*
 * 1. A LEFT JOIN keeps every left row and adds the matched right rows, so it
 * can never return fewer rows than the INNER JOIN on the same ON condition:
 * the inner result is a subset of the left result.  Scalar boolean oracle.
 */
PG_PROP(left_join_count_ge_inner)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM oj_a a "
				 "LEFT JOIN oj_b b ON a.k = b.k) "
				 ">= (SELECT count(*) FROM oj_a a "
				 "JOIN oj_b b ON a.k = b.k)");

	pg_prop_disconnect(conn);
}

/*
 * 2. A LEFT JOIN B is exactly B RIGHT JOIN A: the right join preserves the
 * same (right-hand, here A) rows the left join preserves, with matches from
 * the other side filled identically.  Compared with the default plan, not the
 * three-method helper -- this is a rewrite identity, not an executor-method
 * check.  Both forms project (a.id, b.id) and carry a total order.
 */
PG_PROP(right_join_is_left_swapped)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT a.id, b.id FROM oj_a a "
					  "LEFT JOIN oj_b b ON a.k = b.k "
					  "ORDER BY a.id, coalesce(b.id, -1)",
					  "SELECT a.id, b.id FROM oj_b b "
					  "RIGHT JOIN oj_a a ON a.k = b.k "
					  "ORDER BY a.id, coalesce(b.id, -1)");

	pg_prop_disconnect(conn);
}

/*
 * 3. A FULL JOIN B equals (A LEFT JOIN B) UNION ALL (the B rows with no match
 * in A, NULL-extended on the A side).  The left join supplies every A row
 * plus its matches; the antijoin supplies exactly the unmatched B rows the
 * full join adds on top.  ORDER BY coalesce(a.id,-1), coalesce(b.id,-1) is a
 * total order because (a.id, b.id) is unique within each surviving row.
 */
PG_PROP(full_join_is_left_union_right_antijoin)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT aid, bid FROM ("
					  "SELECT a.id AS aid, b.id AS bid FROM oj_a a "
					  "FULL JOIN oj_b b ON a.k = b.k) s "
					  "ORDER BY coalesce(aid, -1), coalesce(bid, -1)",
					  "SELECT aid, bid FROM ("
					  "SELECT a.id AS aid, b.id AS bid FROM oj_a a "
					  "LEFT JOIN oj_b b ON a.k = b.k "
					  "UNION ALL "
					  "SELECT NULL::int AS aid, b.id AS bid FROM oj_b b "
					  "WHERE NOT EXISTS (SELECT 1 FROM oj_a a "
					  "WHERE a.k = b.k)) s "
					  "ORDER BY coalesce(aid, -1), coalesce(bid, -1)");

	pg_prop_disconnect(conn);
}

/*
 * 4. An INNER JOIN ON a.k = b.k must return the identical rows under all three
 * forced join methods.  This is the baseline executor-equivalence check for
 * the inner path; a shared key yields a cross product each method enumerates.
 */
PG_PROP(inner_join_method_invariant)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	run_three(tc, conn,
			  "SELECT a.id, b.id, a.v, b.v "
			  "FROM oj_a a JOIN oj_b b ON a.k = b.k "
			  "ORDER BY a.id, b.id");

	pg_prop_disconnect(conn);
}

/*
 * 5. A LEFT JOIN must likewise return the identical rows under all three
 * forced methods.  This exercises each method's NULL-extension of unmatched
 * left rows (hash left join, merge left join, nestloop with empty inner).
 */
PG_PROP(left_join_method_invariant)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	run_three(tc, conn,
			  "SELECT a.id, b.id, a.v, b.v "
			  "FROM oj_a a LEFT JOIN oj_b b ON a.k = b.k "
			  "ORDER BY a.id, coalesce(b.id, -1)");

	pg_prop_disconnect(conn);
}

/*
 * 6. An inner join's result is commutative: A JOIN B ON a.k = b.k returns the
 * same (a.id, b.id) pairs as B JOIN A ON a.k = b.k.  Compared on the default
 * plan -- this is the reordering identity, where a bug would reorder rows into
 * a different multiset.  ORDER BY the unique id pair gives a total order.
 */
PG_PROP(join_reorder_commutes)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT a.id, b.id FROM oj_a a "
					  "JOIN oj_b b ON a.k = b.k "
					  "ORDER BY a.id, b.id",
					  "SELECT a.id, b.id FROM oj_b b "
					  "JOIN oj_a a ON a.k = b.k "
					  "ORDER BY a.id, b.id");

	pg_prop_disconnect(conn);
}
