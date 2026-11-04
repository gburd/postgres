/*-------------------------------------------------------------------------
 *
 * prop_percentilefn.c
 *	  Property-based tests for the percentile and mode ordered-set aggregates.
 *
 * percentile_cont, percentile_disc and mode() are ordered-set aggregates with
 * precise definitions: percentile_disc picks an actual column element,
 * percentile_cont interpolates within [min, max] and is monotone in the
 * fraction, and mode() returns a most-frequent value.  Each oracle restates
 * one of those definitions, so an "f" result is a bug in the aggregate.  This
 * area is kept distinct from ordagg/aggorder: it drills only the percentile
 * and mode functions, including the ARRAY-argument percentile form.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_percentilefn.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "percentilefn"
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
 * Build the table pf_t(id int PRIMARY KEY, v int) with 0..30 rows.  Values in
 * [-10, 10] with no NULLs: the narrow range makes duplicates likely, so
 * mode() has a meaningful most-frequent element and percentile_disc has
 * repeated elements to choose among.  Zero rows exercise the empty-aggregate
 * path (every property guards it).  ANALYZE so the planner has stats.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS pf_t; "
						   "CREATE TABLE pf_t (id int PRIMARY KEY, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int r = 0; r < nrows; r++)
		appendStringInfo(&ddl, "INSERT INTO pf_t VALUES (%d, %d); ",
						 r, (int) pg_prop_draw_int(tc, -10, 10));

	appendStringInfoString(&ddl, "ANALYZE pf_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/*
 * percentile_disc picks an actual element of the column, so the discrete
 * median must be a value present in the column.  (Empty table -> count 0 ->
 * CASE true.)
 */
PG_PROP(percentile_disc_is_member)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT count(*) FROM pf_t) = 0 THEN true ELSE "
				 "((SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY v) FROM pf_t) "
				 "IN (SELECT v FROM pf_t)) END");

	pg_prop_disconnect(conn);
}

/*
 * percentile_cont interpolates between elements, so for any fraction p in
 * [0,1] the result lies within [min(v), max(v)].  (Empty table -> count 0 ->
 * CASE true.)
 */
PG_PROP(percentile_cont_in_range)
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
					 "SELECT CASE WHEN (SELECT count(*) FROM pf_t) = 0 THEN true ELSE "
					 "((SELECT percentile_cont(%d/100.0) WITHIN GROUP (ORDER BY v) FROM pf_t) "
					 "BETWEEN (SELECT min(v) FROM pf_t) AND (SELECT max(v) FROM pf_t)) END",
					 pn);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The extreme fractions pin to the extreme values: percentile_cont(0) = min(v)
 * and percentile_cont(1) = max(v).  (Empty table -> count 0 -> CASE true.)
 */
PG_PROP(percentile_0_is_min)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT count(*) FROM pf_t) = 0 THEN true ELSE "
				 "(((SELECT percentile_cont(0) WITHIN GROUP (ORDER BY v) FROM pf_t) "
				 "= (SELECT min(v) FROM pf_t)) "
				 "AND ((SELECT percentile_cont(1) WITHIN GROUP (ORDER BY v) FROM pf_t) "
				 "= (SELECT max(v) FROM pf_t))) END");

	pg_prop_disconnect(conn);
}

/*
 * percentile_cont is non-decreasing in its fraction: for p1 <= p2 the
 * interpolated value at p1 must not exceed that at p2.  (Empty table ->
 * count 0 -> CASE true.)
 */
PG_PROP(percentile_cont_monotone)
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
					 "SELECT CASE WHEN (SELECT count(*) FROM pf_t) = 0 THEN true ELSE "
					 "((SELECT percentile_cont(%d/100.0) WITHIN GROUP (ORDER BY v) FROM pf_t) "
					 "<= (SELECT percentile_cont(%d/100.0) WITHIN GROUP (ORDER BY v) FROM pf_t)) END",
					 p1, p2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The lowest discrete fraction pins to the smallest value:
 * percentile_disc(0) = min(v).  (Empty table -> count 0 -> CASE true.)
 */
PG_PROP(percentile_disc_0_is_min)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT count(*) FROM pf_t) = 0 THEN true ELSE "
				 "((SELECT percentile_disc(0) WITHIN GROUP (ORDER BY v) FROM pf_t) "
				 "= (SELECT min(v) FROM pf_t)) END");

	pg_prop_disconnect(conn);
}

/*
 * mode() returns a most-frequent value.  Oracle: the count of rows equal to
 * the mode equals the maximum per-value count.  (Empty table -> count 0 ->
 * CASE true.)
 */
PG_PROP(mode_is_frequent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT count(*) FROM pf_t) = 0 THEN true ELSE "
				 "((SELECT count(*) FROM pf_t WHERE v = "
				 "(SELECT mode() WITHIN GROUP (ORDER BY v) FROM pf_t)) "
				 "= (SELECT max(c) FROM "
				 "(SELECT count(*) c FROM pf_t GROUP BY v) g)) END");

	pg_prop_disconnect(conn);
}

/*
 * The ARRAY form evaluates every fraction at once: percentile_cont(ARRAY
 * [0,0.5,1]) returns a 3-element array that, since the fractions ascend, must
 * be non-decreasing.  (Empty table -> the array aggregate is NULL -> count 0
 * -> CASE true.)
 */
PG_PROP(percentile_array)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN (SELECT count(*) FROM pf_t) = 0 THEN true ELSE "
				 "(SELECT array_length(a, 1) = 3 AND a[1] <= a[2] AND a[2] <= a[3] "
				 "FROM (SELECT percentile_cont(ARRAY[0,0.5,1]) "
				 "WITHIN GROUP (ORDER BY v) AS a FROM pf_t) s) END");

	pg_prop_disconnect(conn);
}
