/*-------------------------------------------------------------------------
 *
 * prop_selfjoin.c
 *	  Property-based tests for self-join and join-cardinality invariants.
 *
 * A self-join of a table on an equi-key must return the same rows regardless
 * of which join method the planner picks, and its cardinality is fixed by the
 * key-group sizes (an equi-join on a key partitions rows into groups; the join
 * emits every ordered pair within a group, so count = sum of group_size^2).
 * These are exact combinatorial facts, so the oracles need no tolerance: a
 * method that returns different rows, or a count that disagrees with the
 * group-size formula, is a join bug (hashjoin/mergejoin/nestloop mismatch, a
 * NULL-key join that should not match, or a bad cardinality).
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_selfjoin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "selfjoin"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Build sj_t(id int PRIMARY KEY, k int): 0..20 rows, id dense from 0, k drawn
 * in 0..3 with ~15% NULL.  Small key domain forces many ties, so most cases
 * exercise non-trivial group sizes (the interesting cardinality regime); the
 * PRIMARY KEY gives a total order for the row-set oracles.  ANALYZE so the
 * planner has stats to pick among forced methods meaningfully.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS sj_t; "
						   "CREATE TABLE sj_t (id int PRIMARY KEY, k int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 20);
	for (int r = 0; r < nrows; r++)
	{
		if (pg_prop_draw_int(tc, 0, 6) == 0)
			appendStringInfo(&ddl, "INSERT INTO sj_t VALUES (%d, NULL); ", r);
		else
			appendStringInfo(&ddl, "INSERT INTO sj_t VALUES (%d, %d); ",
							 r, (int) pg_prop_draw_int(tc, 0, 3));
	}

	appendStringInfoString(&ddl, "ANALYZE sj_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);
}

/* --- Oracle B: differential result-set compare (from area_spec.md) --- */
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

/* --- Oracle A: scalar boolean compare (require_true, from prop_numeric.c) --- */
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
 * Force each of the three join methods in turn and require that "query"
 * returns identical rows under all three.  We leave the chosen method on and
 * disable the other two with SET enable_*; a RESET ALL between methods clears
 * the GUCs so each run starts from a known base.  nestloop can never be fully
 * disabled (the planner falls back to it), so forcing it means disabling only
 * hashjoin and mergejoin.  "query" must carry a TOTAL ORDER BY.
 */
static void
run_three(pg_prop_tc *tc, pg_prop_conn *conn, const char *query)
{
	static const char *setups[] = {
		/* hashjoin */
		"SET enable_hashjoin = on; SET enable_mergejoin = off; SET enable_nestloop = off;",
		/* mergejoin */
		"SET enable_hashjoin = off; SET enable_mergejoin = on; SET enable_nestloop = off;",
		/* nestloop */
		"SET enable_hashjoin = off; SET enable_mergejoin = off; SET enable_nestloop = on;"
	};
	char	   *err = NULL;
	char	   *base = NULL;

	for (int i = 0; i < 3; i++)
	{
		bool		f = false;
		char	   *rows;

		if (!pg_prop_sql_exec(conn, "RESET ALL;", &err))
			pg_prop_fail(tc, "RESET ALL failed: %s", err ? err : "");
		if (!pg_prop_sql_exec(conn, setups[i], &err))
			pg_prop_fail(tc, "GUC setup %d failed: %s", i, err ? err : "");

		rows = pg_prop_sql_rows(conn, query, &f, &err);
		if (f)
			pg_prop_fail(tc, "query failed (method %d): %s\n  SQL: %s",
						 i, err ? err : "", query);

		if (base == NULL)
			base = rows;
		else
		{
			if (strcmp(base, rows) != 0)
				pg_prop_fail(tc,
							 "join method %d differs:\n  SQL: %s\n  base:\n%s\n  this:\n%s",
							 i, query, base, rows);
			pg_free(rows);
		}
	}
	pg_free(base);
	pg_prop_sql_exec(conn, "RESET ALL;", &err);
	if (err)
		pg_free(err);
}

/*
 * 1. An equi self-join on k must return identical rows under hashjoin,
 * mergejoin and nestloop -- the join method is a performance choice that must
 * never change results.
 */
PG_PROP(equi_self_join_method_invariant)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	run_three(tc, conn,
			  "SELECT a.id, b.id FROM sj_t a JOIN sj_t b ON a.k = b.k "
			  "ORDER BY a.id, b.id");

	pg_prop_disconnect(conn);
}

/*
 * 2. An equi self-join on k partitions non-NULL rows into groups by k; the
 * join emits every ordered pair within a group, so its count equals the sum
 * over non-NULL k-groups of group_size^2.  NULL k never equi-joins, so it is
 * excluded.
 */
PG_PROP(join_count_eq_sum_of_squares)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM sj_t a JOIN sj_t b ON a.k = b.k) "
				 "= (SELECT coalesce(sum(c*c), 0) FROM "
				 "(SELECT count(*) c FROM sj_t WHERE k IS NOT NULL GROUP BY k) g)");

	pg_prop_disconnect(conn);
}

/*
 * 3. A CROSS JOIN forms every ordered pair of rows, so its count is the row
 * count squared.
 */
PG_PROP(cross_join_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM sj_t a CROSS JOIN sj_t b) "
				 "= (SELECT count(*) * count(*) FROM sj_t)");

	pg_prop_disconnect(conn);
}

/*
 * 4. A LEFT self-join keeps every row of the inner self-join plus any
 * left-side rows with no match (NULL-k rows, and -- for an inner equi-join --
 * none are dropped beyond those), so the LEFT count is always >= the INNER
 * count on the same ON condition.
 */
PG_PROP(left_self_join_ge)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM sj_t a LEFT JOIN sj_t b ON a.k = b.k) "
				 ">= (SELECT count(*) FROM sj_t a JOIN sj_t b ON a.k = b.k)");

	pg_prop_disconnect(conn);
}

/*
 * 5. The set of keys that appear in an equi self-join is exactly the set of
 * non-NULL k values present in the table (a key self-joins iff some row with
 * that key exists; NULL never joins).  Differential: the join's distinct keys
 * must equal the table's distinct non-NULL keys.
 */
PG_PROP(join_distinct_keys)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT DISTINCT a.k FROM sj_t a JOIN sj_t b ON a.k = b.k ORDER BY a.k",
					  "SELECT DISTINCT k FROM sj_t WHERE k IS NOT NULL ORDER BY k");

	pg_prop_disconnect(conn);
}

/*
 * 6. A self-join on "a.k IS NOT DISTINCT FROM b.k" treats NULL as a matchable
 * value (NULL IS NOT DISTINCT FROM NULL is true), so it groups ALL rows
 * including the NULL group; its count equals the sum over every k-group
 * (NULLs included) of group_size^2.
 */
PG_PROP(not_distinct_join_includes_nulls)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM sj_t a JOIN sj_t b "
				 "ON a.k IS NOT DISTINCT FROM b.k) "
				 "= (SELECT coalesce(sum(c*c), 0) FROM "
				 "(SELECT count(*) c FROM sj_t GROUP BY k) g)");

	pg_prop_disconnect(conn);
}
