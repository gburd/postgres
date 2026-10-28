/*-------------------------------------------------------------------------
 *
 * prop_minmaxagg.c
 *	  Property-based tests for the extremum and bitwise/boolean aggregates:
 *	  min, max, bool_and, bool_or, bit_and, bit_or.
 *
 * These aggregates promise order and bit-lattice identities over whatever
 * rows survive NULL skipping: min(v) <= max(v); min()/max() return values
 * actually present (and ignore NULLs); bool_and/bool_or obey De Morgan;
 * bit_and(v) is a submask of bit_or(v); and count(v) > 0 forces min/max
 * non-NULL.  A failure means an aggregate transition broke one of these, not
 * a rounding artefact -- there is none here.
 *
 * This area is deliberately disjoint from "aggregate" (sum/avg/min/max/count
 * numeric bounds), "boolagg" (EVERY/SOME, FILTER, GROUP BY reaggregation) and
 * "ordagg" (percentile/mode/ordered-set): here the subject is the extremum
 * and bit/bool lattice identities of min/max/bool_and/bool_or/bit_and/bit_or.
 *
 * The oracles are computed in SQL from the same table, so there is no C-side
 * arithmetic to overflow.  Empty / all-NULL cases are handled with coalesce(),
 * CASE and IS NOT DISTINCT FROM so an aggregate NULL passes soundly.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_minmaxagg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "minmaxagg"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the aggregate errored or produced an unexpected NULL,
 * which is itself a failure, reported with the SQL.
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
 * Build the table mm_t(id int PRIMARY KEY, v int) with 0..30 rows.  id is a
 * dense sequence so the primary key is unique by construction; v is drawn in
 * [-10, 10] -- small enough that no sum/product over at most 30 of them can
 * overflow int, and signed so bit_and/bit_or exercise the sign bits -- or SQL
 * NULL (~15%, 3 of 20), so NULL skipping is exercised.  Zero rows exercises
 * the empty-aggregate path.  The oracles are all derived in SQL from this
 * table; the counterexample shows the drawn values through the INSERTs.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 30);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS mm_t; "
						   "CREATE TABLE mm_t (id int PRIMARY KEY, v int); ");

	for (int r = 0; r < nrows; r++)
	{
		if (pg_prop_draw_int(tc, 0, 19) < 3)
			appendStringInfo(&ddl, "INSERT INTO mm_t VALUES (%d, NULL); ", r);
		else
			appendStringInfo(&ddl, "INSERT INTO mm_t VALUES (%d, %d); ",
							 r, (int) pg_prop_draw_int(tc, -10, 10));
	}

	appendStringInfoString(&ddl, "ANALYZE mm_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/*
 * min(v) <= max(v) over a non-empty column of values.  On an empty / all-NULL
 * table both are NULL, so coalesce the comparison to true (min <= max OR both
 * NULL): the empty case passes soundly.
 */
PG_PROP(min_le_max)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(min(v) <= max(v), true) FROM mm_t");

	pg_prop_sql_exec(conn, "DROP TABLE mm_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * min(v) is an actual value present in the column: either min is NULL (empty /
 * all-NULL) or it occurs among the v values.  coalesce(... IN ..., true)
 * accepts the NULL-min case; a non-NULL min must be found in the table.
 */
PG_PROP(min_in_table)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT min(v) FROM mm_t) IS NULL THEN true "
				 "ELSE ((SELECT min(v) FROM mm_t) IN (SELECT v FROM mm_t)) END");

	pg_prop_sql_exec(conn, "DROP TABLE mm_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * max(v) dominates every non-NULL value: bool_and(v <= max(v)) over the
 * non-NULL rows is true.  Over no non-NULL rows bool_and is NULL, so
 * coalesce(..., true) passes the empty / all-NULL case.
 */
PG_PROP(max_ge_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(v <= (SELECT max(v) FROM mm_t)), "
				 "true) FROM mm_t WHERE v IS NOT NULL");

	pg_prop_sql_exec(conn, "DROP TABLE mm_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * De Morgan over the quantifier aggregates: bool_and(p) = NOT bool_or(NOT p),
 * here with p = v > 0.  IS NOT DISTINCT FROM makes the empty / all-NULL case
 * (both sides NULL) pass; coalesce the whole comparison to true for the same
 * reason that NULL IS NOT DISTINCT FROM NULL is already true.
 */
PG_PROP(bool_and_or_duality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce("
				 "bool_and(v > 0) IS NOT DISTINCT FROM "
				 "NOT bool_or(NOT (v > 0)), true) FROM mm_t");

	pg_prop_sql_exec(conn, "DROP TABLE mm_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * bit_and(v) is a submask of bit_or(v): a bit set in the AND of all values is
 * necessarily set in the OR of all values, so (bit_and & bit_or) = bit_and.
 * Both aggregates skip NULLs; over no non-NULL rows both are NULL, so
 * coalesce the comparison to true for the empty / all-NULL case.
 */
PG_PROP(bit_and_submask_or)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce((bit_and(v) & bit_or(v)) = bit_and(v), true) "
				 "FROM mm_t WHERE v IS NOT NULL");

	pg_prop_sql_exec(conn, "DROP TABLE mm_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * When count(v) > 0 there is at least one non-NULL value, so min(v) and
 * max(v) are both non-NULL; when count(v) = 0 (empty / all-NULL) the CASE
 * returns true, accepting that min/max are NULL.
 */
PG_PROP(count_min_max_nonnull)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN count(v) > 0 "
				 "THEN min(v) IS NOT NULL AND max(v) IS NOT NULL "
				 "ELSE true END FROM mm_t");

	pg_prop_sql_exec(conn, "DROP TABLE mm_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * sum(v) is bracketed by count(v)*min(v) and count(v)*max(v) over the
 * non-NULL rows: every value is in [min, max], so their sum is in
 * [count*min, count*max].  Over no non-NULL rows sum/min/max are NULL and
 * count is 0, so coalesce the comparison to true for the empty case.
 */
PG_PROP(sum_between_count_times_minmax)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce("
				 "count(v) * min(v) <= sum(v) "
				 "AND sum(v) <= count(v) * max(v), true) "
				 "FROM mm_t WHERE v IS NOT NULL");

	pg_prop_sql_exec(conn, "DROP TABLE mm_t", &err);
	pg_prop_disconnect(conn);
}

/*
 * min() ignores NULLs: min over all rows equals min over the NULL-filtered
 * rows.  IS NOT DISTINCT FROM makes the empty / all-NULL case (both min NULL)
 * pass.
 */
PG_PROP(min_max_ignore_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT min(v) FROM mm_t) IS NOT DISTINCT FROM "
				 "(SELECT min(v) FROM mm_t WHERE v IS NOT NULL)");

	pg_prop_sql_exec(conn, "DROP TABLE mm_t", &err);
	pg_prop_disconnect(conn);
}
