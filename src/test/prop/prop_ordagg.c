/*-------------------------------------------------------------------------
 *
 * prop_ordagg.c
 *	  Property-based tests for ordered-set and boolean/bitwise aggregates.
 *
 * Ordered-set aggregates (percentile_cont, percentile_disc, mode) and the
 * boolean/bitwise aggregates (bool_and, bool_or, bit_and, bit_or) have
 * precise definitions; these check them over a generated integer column.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_ordagg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "ordagg"
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
 * Build the table oa_t(v int) and populate it with 0..25 rows of values in
 * [-50, 50].  The narrow value range makes duplicates likely, so mode() has a
 * meaningful most-frequent element and the discrete percentile has repeated
 * elements to choose among.  Zero rows exercise the empty-aggregate path.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS oa_t; "
						   "CREATE TABLE oa_t (v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int r = 0; r < nrows; r++)
		appendStringInfo(&ddl, "INSERT INTO oa_t VALUES (%d); ",
						 (int) pg_prop_draw_int(tc, -50, 50));

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/*
 * percentile_disc picks an actual element of the column.  So the discrete
 * median must be a value present in the column (empty table -> NULL IN (...)
 * -> coalesce true).
 */
PG_PROP(percentile_disc_is_a_value)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT count(*) FROM oa_t) = 0 THEN true ELSE "
				 "((SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY v) FROM oa_t) "
				 "IN (SELECT v FROM oa_t)) END");

	pg_prop_disconnect(conn);
}

/*
 * percentile_cont interpolates between elements, so for any fraction p in
 * [0,1] the result lies within [min(v), max(v)].
 */
PG_PROP(percentile_cont_within_minmax)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			pn;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	pn = (int) pg_prop_draw_int(tc, 0, 100);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce("
					 "percentile_cont(%d/100.0) WITHIN GROUP (ORDER BY v) "
					 "BETWEEN min(v) AND max(v), true) FROM oa_t",
					 pn);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * percentile_cont is non-decreasing in its fraction: for p1 <= p2 the
 * interpolated value at p1 must not exceed that at p2.
 */
PG_PROP(percentile_cont_monotonic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b,
				p1,
				p2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	a = (int) pg_prop_draw_int(tc, 0, 100);
	b = (int) pg_prop_draw_int(tc, 0, 100);
	p1 = (a <= b) ? a : b;
	p2 = (a <= b) ? b : a;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce("
					 "(percentile_cont(%d/100.0) WITHIN GROUP (ORDER BY v)) "
					 "<= (percentile_cont(%d/100.0) WITHIN GROUP (ORDER BY v)), "
					 "true) FROM oa_t",
					 p1, p2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The extreme fractions pin to the extreme values: percentile_cont(0) = min(v)
 * and percentile_cont(1) = max(v).
 */
PG_PROP(percentile_0_1_are_min_max)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce("
				 "(percentile_cont(0) WITHIN GROUP (ORDER BY v) = min(v)) "
				 "AND (percentile_cont(1) WITHIN GROUP (ORDER BY v) = max(v)), "
				 "true) FROM oa_t");

	pg_prop_disconnect(conn);
}

/*
 * mode() returns a most-frequent value.  Oracle: the count of rows equal to
 * the mode equals the maximum per-value count.  (Empty table -> mode is NULL,
 * both subqueries are NULL -> coalesce true.)
 */
PG_PROP(mode_is_most_frequent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT count(*) FROM oa_t) = 0 THEN true ELSE "
				 "((SELECT count(*) FROM oa_t WHERE v = "
				 "(SELECT mode() WITHIN GROUP (ORDER BY v) FROM oa_t)) "
				 "= (SELECT max(c) FROM "
				 "(SELECT count(*) c FROM oa_t GROUP BY v) g)) END");

	pg_prop_disconnect(conn);
}

/*
 * bool_and over a per-row predicate (v > 0) is true iff no row makes the
 * predicate false; cross-check it against a direct count of falsifying rows.
 * Empty table: bool_and is NULL -> coalesce true, and the count is 0 -> true,
 * so the two sides agree.
 */
PG_PROP(bool_and_or_definition)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(v > 0), true) = "
				 "(coalesce((SELECT count(*) FROM oa_t WHERE NOT (v > 0)), 0) = 0) "
				 "FROM oa_t");

	pg_prop_disconnect(conn);
}

/*
 * bit_or accumulates every bit any row has; bit_and keeps only bits all rows
 * share.  So bit_and(v) is a submask of bit_or(v): (bit_and & bit_or) =
 * bit_and.  (Empty table -> both NULL -> coalesce true.)
 */
PG_PROP(bit_and_or_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce((bit_and(v) & bit_or(v)) = bit_and(v), true) "
				 "FROM oa_t");

	pg_prop_disconnect(conn);
}
