/*-------------------------------------------------------------------------
 *
 * prop_countfilter.c
 *	  Property-based tests for COUNT/SUM with FILTER and boolean-to-int
 *	  aggregation.
 *
 * The oracle is the relationship between filtered aggregates and the
 * equivalent boolean-predicate aggregates over the same rows: count(*)
 * FILTER (WHERE p) must equal sum((p)::int) once NULL predicate rows are
 * accounted for (a NULL predicate drops the row from the FILTER count, and
 * (NULL)::int is NULL which sum skips -- so the two agree), count(*) must
 * partition exactly into its FILTER complements, and bool_or/bool_and over a
 * predicate must agree with the matching filtered counts.  A failure here is
 * a wrong-results bug in FILTER handling, boolean-to-int casting, or the
 * boolean aggregates' NULL skipping.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_countfilter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "countfilter"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the aggregate errored or produced NULL where it must not,
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
 * Build cf_t(id int PRIMARY KEY, v int) with 0..30 rows: id is 0..n-1, v is
 * drawn in -5..5 with ~15% NULL.  Fresh each property so a counterexample is
 * self-contained; ANALYZE so plan choice (hash vs sorted vs partial agg) is
 * free to vary and the oracle holds regardless.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS cf_t; "
						   "CREATE TABLE cf_t (id int PRIMARY KEY, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int i = 0; i < nrows; i++)
	{
		if (pg_prop_draw_int(tc, 0, 6) == 0)
			appendStringInfo(&ddl, "INSERT INTO cf_t VALUES (%d, NULL); ", i);
		else
			appendStringInfo(&ddl, "INSERT INTO cf_t VALUES (%d, %d); ",
							 i, (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(&ddl, "ANALYZE cf_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * count(*) FILTER (WHERE v > 0) = coalesce(sum((v > 0)::int), 0).
 *
 * A NULL v makes (v > 0) NULL, so the row is excluded from the FILTER count;
 * (NULL)::int is NULL and sum skips it -- so both sides treat NULL rows
 * identically.  coalesce guards the all-NULL / empty case where sum is NULL
 * but the FILTER count is 0.
 */
PG_PROP(count_filter_eq_sum_bool)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(sum((v > 0)::int), 0) "
				 "= count(*) FILTER (WHERE v > 0) FROM cf_t");

	pg_prop_disconnect(conn);
}

/*
 * count(*) = count(v) + count(*) FILTER (WHERE v IS NULL).  count(v) counts
 * non-NULL v; the FILTER counts the NULL v; together they are every row.
 */
PG_PROP(count_star_eq_count_plus_nulls)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(*) "
				 "= count(v) + count(*) FILTER (WHERE v IS NULL) FROM cf_t");

	pg_prop_disconnect(conn);
}

/*
 * coalesce(sum(v) FILTER (WHERE v > 0), 0) >= 0.  Summing only positive values
 * can never be negative; coalesce handles the no-positive-rows case (sum NULL).
 */
PG_PROP(sum_filter_positive_nonneg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(sum(v) FILTER (WHERE v > 0), 0) >= 0 FROM cf_t");

	pg_prop_disconnect(conn);
}

/*
 * The three FILTER complements partition every row exactly:
 * count(*) FILTER (WHERE v>0) + count(*) FILTER (WHERE v<=0)
 *   + count(*) FILTER (WHERE v IS NULL) = count(*).
 * Every row falls into exactly one of {v>0, v<=0, v IS NULL}.
 */
PG_PROP(filter_complement_partition)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT count(*) FILTER (WHERE v > 0) "
				 "+ count(*) FILTER (WHERE v <= 0) "
				 "+ count(*) FILTER (WHERE v IS NULL) "
				 "= count(*) FROM cf_t");

	pg_prop_disconnect(conn);
}

/*
 * avg(v) FILTER (WHERE v > 0) lies in [1, 5] whenever any v > 0 exists: the
 * values averaged are integers in 1..5, so their mean is in (0, 5], and being
 * >= 1 (the smallest positive integer) and <= 5.  The CASE guards the empty
 * case (no positive rows): avg is NULL then, so return true.
 */
PG_PROP(avg_filter_in_range)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN count(*) FILTER (WHERE v > 0) = 0 THEN true "
				 "ELSE avg(v) FILTER (WHERE v > 0) BETWEEN 1 AND 5 END "
				 "FROM cf_t");

	pg_prop_disconnect(conn);
}

/*
 * coalesce(bool_or(v > 0), false) = (count(*) FILTER (WHERE v > 0) > 0).
 * bool_or over the predicate is true iff at least one row satisfies it, which
 * is exactly the filtered count being positive.  coalesce handles the empty /
 * all-NULL case where bool_or is NULL and the count is 0 (false both sides).
 */
PG_PROP(bool_or_eq_count_gt_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_or(v > 0), false) "
				 "= (count(*) FILTER (WHERE v > 0) > 0) FROM cf_t");

	pg_prop_disconnect(conn);
}

/*
 * coalesce(bool_and(v > 0), true) = (count(*) FILTER (WHERE v <= 0) = 0).
 * bool_and over (v > 0) skips NULL-v rows, so it is true iff no non-NULL row
 * has v <= 0 -- i.e. the filtered count of v <= 0 is zero.  coalesce handles
 * the empty / all-NULL case (bool_and NULL, count 0: true both sides).
 */
PG_PROP(bool_and_eq_no_false)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(v > 0), true) "
				 "= (count(*) FILTER (WHERE v <= 0) = 0) FROM cf_t");

	pg_prop_disconnect(conn);
}
