/*-------------------------------------------------------------------------
 *
 * prop_joinmethods.c
 *	  Property-based tests for join-method equivalence (src/backend/executor).
 *
 * A join's result must not depend on the physical join method the planner
 * picks: hash, merge and nestloop are three implementations of the same
 * relational operator and must return the same rows.  These properties build
 * two random small tables and run the same join under each method forced on
 * (and the other two off), requiring identical row sets.  A disagreement is a
 * wrong-results bug in a join executor path -- NULL-key handling, hash batch
 * spill, or merge-join mark/restore.
 *
 * The oracle is differential: for a single generated pair of tables and a
 * single query, run1 (hashjoin) == run2 (mergejoin) == run3 (nestloop).  If a
 * method cannot be used for a given query the planner may fall back to
 * another; that is fine, because we only require the results to be equal, not
 * that a particular method was chosen.  The small key domain (-3..3 with ~20%
 * NULL) is the point: it forces NULL-key join behaviour, where these bugs
 * hide.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_joinmethods.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "joinmethods"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small integer domains keep each case fast and legible in a counterexample
 * while still covering the interactions where join bugs live: a key value
 * shared by several rows on both sides (so a method has to emit a cross
 * product for that key), and a NULL key (which never equi-matches but does
 * match under IS NOT DISTINCT FROM).
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
 * Build one table "name"(id int, k int, v int) with a random number of rows
 * (0..12).  id is a dense 0-based sequence so it is unique per table and can
 * serve as a tie-breaker in a total ORDER BY.  k is drawn by draw_key (with
 * NULLs); v is a small non-negative-or-negative int.  Discards the case
 * (via pg_prop_assume) if the DDL/DML is rejected, which should not happen
 * for this fixed schema but keeps a surprise from masquerading as a bug.
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
					 "CREATE TABLE %s (id int, k int, v int); ",
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
	build_table(tc, conn, "jm_a");
	build_table(tc, conn, "jm_b");
}

/*
 * Run "query" three ways -- hashjoin only, mergejoin only, nestloop only --
 * and require all three to produce the identical ordered row set.  Any SQL
 * error, or any disagreement, is a failure reported with the query and both
 * offending row dumps.  RESET ALL is issued after the runs so the forced
 * GUCs cannot leak out of this helper.
 *
 * "query" must carry its own total ORDER BY so pg_prop_sql_rows (which
 * compares the newline-joined row-major rendering) is comparing an ordered
 * multiset soundly.
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
 * Inner equi-join on the key: SELECT the matched rows and compare across
 * methods.  This is the straight hash/merge/nestloop inner path; a shared key
 * value yields a cross product that each method must enumerate identically.
 */
PG_PROP(inner_join_on_key)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	run_three(tc, conn,
			  "SELECT a.id, b.id, a.v, b.v "
			  "FROM jm_a a JOIN jm_b b ON a.k = b.k "
			  "ORDER BY a.id, b.id, a.v, b.v");

	pg_prop_disconnect(conn);
}

/*
 * Left outer join on the key: every left row survives, with NULLs filled in
 * where it has no match.  Exercises the outer-join handling of each method
 * (hash left join, merge left join, nestloop with the inner side empty).
 */
PG_PROP(left_join_on_key)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	run_three(tc, conn,
			  "SELECT a.id, b.id, a.v, b.v "
			  "FROM jm_a a LEFT JOIN jm_b b ON a.k = b.k "
			  "ORDER BY a.id, b.id, a.v, b.v");

	pg_prop_disconnect(conn);
}

/*
 * Full outer join on the key: unmatched rows from both sides survive, so
 * either id can be NULL.  The merge-join and hash-join full-join paths are
 * distinct code; nestloop cannot do a full join directly, so the planner
 * falls back -- which is fine, we only require equal results.  Order by
 * coalesce(id, -1) on both sides (NULL ids sort together) plus v on both
 * sides to make the ordering total.
 */
PG_PROP(full_join_on_key)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	run_three(tc, conn,
			  "SELECT a.id, b.id, a.v, b.v "
			  "FROM jm_a a FULL JOIN jm_b b ON a.k = b.k "
			  "ORDER BY coalesce(a.id, -1), coalesce(b.id, -1), "
			  "a.v, b.v");

	pg_prop_disconnect(conn);
}

/*
 * NULL-aware join key: ON a.k IS NOT DISTINCT FROM b.k treats NULL as equal
 * to NULL, so NULL-keyed rows on each side match each other.  This is the
 * case most likely to catch a method that mishandles NULL keys (equi-join
 * drops them; IS NOT DISTINCT FROM must keep them matched).
 */
PG_PROP(join_is_not_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	run_three(tc, conn,
			  "SELECT a.id, b.id, a.v, b.v "
			  "FROM jm_a a JOIN jm_b b ON a.k IS NOT DISTINCT FROM b.k "
			  "ORDER BY a.id, b.id, a.v, b.v");

	pg_prop_disconnect(conn);
}

/*
 * Inner equi-join with a residual filter: the ON clause joins on the key and
 * a WHERE restricts the result.  The filter can be pushed down or applied as
 * a join qual differently per method, so this checks that the filtered result
 * is still method-independent.
 */
PG_PROP(join_with_filter)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	run_three(tc, conn,
			  "SELECT a.id, b.id, a.v, b.v "
			  "FROM jm_a a JOIN jm_b b ON a.k = b.k "
			  "WHERE a.v > 0 OR b.v IS NULL "
			  "ORDER BY a.id, b.id, a.v, b.v");

	pg_prop_disconnect(conn);
}

/*
 * Anti-join: left rows that have no match on the right.  NOT EXISTS plans as
 * an anti-join, which hash, merge and nestloop each implement separately; a
 * NULL key on the right must not spuriously satisfy or defeat the anti-match.
 */
PG_PROP(anti_join)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	run_three(tc, conn,
			  "SELECT a.id "
			  "FROM jm_a a "
			  "WHERE NOT EXISTS (SELECT 1 FROM jm_b b WHERE b.k = a.k) "
			  "ORDER BY a.id");

	pg_prop_disconnect(conn);
}
