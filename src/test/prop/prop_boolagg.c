/*-------------------------------------------------------------------------
 *
 * prop_boolagg.c
 *	  Property-based tests for the quantifier aggregates, FILTER clauses and
 *	  DISTINCT/GROUP BY aggregate modifiers.
 *
 * EVERY() is the SQL-standard spelling of bool_and and SOME()/ANY() of
 * bool_or; count(*) FILTER (WHERE cond) counts only matching rows; and
 * aggregating over GROUP BY and then re-aggregating the per-group results must
 * reconcile with the ungrouped total.  These properties check the exact
 * relationships those features promise over a randomly generated two-column
 * table ba_t(g, v): the quantifier/count duality, that complementary FILTERs
 * partition the rows, that count(DISTINCT g) counts the distinct groups, and
 * that summing (or counting) per group adds back to the grand total.
 *
 * This area is deliberately disjoint from "aggregate" (sum/avg/min/max/count,
 * count-distinct bound) and "ordagg" (bool_and/bool_or, bit_and/bit_or,
 * percentile, mode): here the subject is EVERY/SOME, the FILTER clause,
 * count(DISTINCT) against an explicit distinct projection, and the
 * GROUP-BY-then-reaggregate identities.
 *
 * The oracles are computed in SQL from the same table, so there is no C-side
 * arithmetic to overflow; the small value range keeps every sum well within
 * range.  Empty-table cases are handled with coalesce() so the empty table
 * passes consistently with each oracle (aggregates are NULL over no rows).
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_boolagg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "boolagg"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the aggregate errored or produced an unexpected NULL,
 * which is itself a bug -- so either is a failure, reported with the SQL.
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	}
	if (res == NULL)
	{
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	}
	if (strcmp(res, "t") != 0)
	{
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	}
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * Build the table ba_t(g int, v int) and populate it with 0..25 rows.  The
 * group label g is drawn in [0, 3] so there are only a few groups and most
 * have several rows (so GROUP BY has something to reconcile), and v is drawn
 * in [-20, 20] so there is a healthy mix of positive, zero and negative values
 * for the quantifier and FILTER predicates to split on.  The range is small
 * enough that no sum over at most 25 rows can overflow.  Zero rows exercises
 * the empty-aggregate path.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ba_t; "
						   "CREATE TABLE ba_t (g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int r = 0; r < nrows; r++)
		appendStringInfo(&ddl, "INSERT INTO ba_t VALUES (%d, %d); ",
						 (int) pg_prop_draw_int(tc, 0, 3),
						 (int) pg_prop_draw_int(tc, -20, 20));

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/*
 * EVERY(v > 0) -- the SQL-standard spelling of bool_and -- is true iff no row
 * has v <= 0, which the count filter measures directly.  Empty table: EVERY is
 * NULL -> coalesce true, and count filter = 0 -> true, so the two sides agree.
 */
PG_PROP(every_equals_count_filter)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(every(v > 0), true) = "
				 "(count(*) FILTER (WHERE v <= 0) = 0) FROM ba_t");

	pg_prop_sql_exec(conn, "DROP TABLE ba_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * Two complementary FILTERs partition the rows: count(*) FILTER (WHERE v > 0)
 * plus count(*) FILTER (WHERE v <= 0) equals count(*).  Every row satisfies
 * exactly one of v > 0 / v <= 0, so the matched counts sum to the total.
 * Empty table: 0 + 0 = 0.
 */
PG_PROP(count_filter_partitions)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (count(*) FILTER (WHERE v > 0) + "
				 "count(*) FILTER (WHERE v <= 0)) = count(*) FROM ba_t");

	pg_prop_sql_exec(conn, "DROP TABLE ba_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * count(DISTINCT g) equals the number of distinct groups, computed
 * independently as the row count of an explicit SELECT DISTINCT.  The two must
 * agree exactly.  Empty table: both are 0.
 */
PG_PROP(count_distinct_matches_group_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(DISTINCT g) = "
				 "(SELECT count(*) FROM (SELECT DISTINCT g FROM ba_t) s) "
				 "FROM ba_t");

	pg_prop_sql_exec(conn, "DROP TABLE ba_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * GROUP BY g, sum v per group, then sum the group sums: the result equals the
 * grand total sum(v).  coalesce(..., 0) makes the empty table (both sides
 * NULL) pass as 0 = 0.
 */
PG_PROP(group_sums_add_to_total)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce((SELECT sum(gs) FROM "
				 "(SELECT sum(v) gs FROM ba_t GROUP BY g) s), 0) = "
				 "coalesce((SELECT sum(v) FROM ba_t), 0)");

	pg_prop_sql_exec(conn, "DROP TABLE ba_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * GROUP BY g, count rows per group, then sum the group counts: the result
 * equals the total row count count(*).  The per-group count sum is NULL on an
 * empty table (no groups), so coalesce(..., 0) matches count(*) = 0.
 */
PG_PROP(group_counts_add_to_total)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce((SELECT sum(gc) FROM "
				 "(SELECT count(*) gc FROM ba_t GROUP BY g) s), 0) = "
				 "(SELECT count(*) FROM ba_t)");

	pg_prop_sql_exec(conn, "DROP TABLE ba_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * The FILTERed sum of the positive values is non-negative and no greater than
 * the sum of absolute values: 0 <= sum(v) FILTER (WHERE v > 0) <= sum(abs(v)).
 * coalesce(..., 0) makes the empty / all-filtered-out case pass as 0 <= 0 <= 0.
 */
PG_PROP(filter_sum_le_total_abs)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(sum(v) FILTER (WHERE v > 0), 0) >= 0 "
				 "AND coalesce(sum(v) FILTER (WHERE v > 0), 0) <= "
				 "coalesce(sum(abs(v)), 0) FROM ba_t");

	pg_prop_sql_exec(conn, "DROP TABLE ba_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * De Morgan for the quantifier aggregates: NOT EVERY(p) = SOME(NOT p).  Here
 * p is v > 0, so NOT every(v > 0) must equal bool_or(NOT (v > 0)).  Empty
 * table: every and bool_or are both NULL, so coalesce(..., false) gives
 * false = false, which holds.
 */
PG_PROP(every_some_duality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(NOT every(v > 0), false) = "
				 "coalesce(bool_or(NOT (v > 0)), false) FROM ba_t");

	pg_prop_sql_exec(conn, "DROP TABLE ba_t", &err);
	pg_prop_disconnect(conn);
}
