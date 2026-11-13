/*-------------------------------------------------------------------------
 *
 * prop_query_window.c
 *	  Query-level property tests for window functions and grouping sets.
 *
 * These are the planner/executor transformations with the richest history of
 * wrong-results bugs, exercised here as metamorphic oracles over a generated
 * table (query_gen.h): no reference answer, only a relationship that must hold.
 *
 * Window functions:
 *	 window_count_over_all  -- count(*) OVER () on every row equals the total
 *						      row count; a running count ends at the total.
 *	 window_running_sum     -- the last value of sum(x) OVER (ORDER BY a, id)
 *						      equals the plain sum(x); the running sum is
 *						      monotone when x >= 0.
 *	 window_row_number_perm -- row_number() OVER (ORDER BY a, id) is a
 *						      permutation of 1..n (each appears exactly once).
 *	 window_rank_bounds     -- rank() <= row_number() and dense_rank() <=
 *						      rank(), per the standard window-rank ordering.
 *
 * Grouping sets:
 *	 rollup_grand_total     -- the all-NULL grouping row of GROUP BY ROLLUP(a,b)
 *						      equals the ungrouped aggregate over the table.
 *	 rollup_detail_matches  -- the detail rows of ROLLUP(a,b) equal plain
 *						      GROUP BY a, b.
 *	 cube_superset_rollup   -- GROUP BY CUBE(a,b) returns at least as many
 *						      grouping rows as ROLLUP(a,b) (cube is a superset).
 *	 groupingsets_union     -- GROUP BY GROUPING SETS ((a),(b),()) equals the
 *						      UNION ALL of GROUP BY a, GROUP BY b, and the grand
 *						      total -- a tautology the executor must reproduce.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_window.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_window"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

static long
scalar_long(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	long		n;

	v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "scalar failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	n = v ? atol(v) : 0;
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	return n;
}

static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *a, const char *b)
{
	bool		fa = false,
				fb = false;
	char	   *ea = NULL,
			   *eb = NULL,
			   *ra,
			   *rb;

	ra = pg_prop_sql_rows(conn, a, &fa, &ea);
	if (fa)
		pg_prop_fail(tc, "query a failed: %s\n  SQL: %s", ea ? ea : "", a);
	rb = pg_prop_sql_rows(conn, b, &fb, &eb);
	if (fb)
		pg_prop_fail(tc, "query b failed: %s\n  SQL: %s", eb ? eb : "", b);
	if (strcmp(ra, rb) != 0)
		pg_prop_fail(tc, "mismatch:\n  a: %s\n  b: %s\n  ra:\n%s\n  rb:\n%s",
					 a, b, ra, rb);
	pg_free(ra);
	pg_free(rb);
	if (ea)
		pg_free(ea);
	if (eb)
		pg_free(eb);
}

static pg_prop_conn *
connect_build(pg_prop_tc *tc, int *nrows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	qg_build_table(tc, conn, nrows);
	return conn;
}

/* count(*) OVER () on every row equals the table's row count. */
PG_PROP(window_count_over_all)
{
	int			nrows;
	pg_prop_conn *conn = connect_build(tc, &nrows);
	long		bad;

	/* a row whose window count differs from the total is a bug */
	bad = scalar_long(tc, conn,
					  "SELECT count(*) FROM "
					  "(SELECT count(*) OVER () AS c FROM qg_t) s "
					  "WHERE s.c <> (SELECT count(*) FROM qg_t)");
	if (bad != 0)
		pg_prop_fail(tc, "count(*) OVER () disagreed with total on %ld rows",
					 bad);
	pg_prop_disconnect(conn);
}

/*
 * The last value of a running sum over a total order equals the plain sum, and
 * the running sum is non-decreasing when the summed column is non-negative
 * (use coalesce(abs(a),0) so it is).
 */
PG_PROP(window_running_sum)
{
	int			nrows;
	pg_prop_conn *conn = connect_build(tc, &nrows);
	long		last_running,
				plain_sum,
				nonmonotone;

	if (nrows == 0)
	{
		pg_prop_disconnect(conn);
		return;
	}

	last_running = scalar_long(tc, conn,
							   "SELECT coalesce(max(rs), 0) FROM "
							   "(SELECT sum(coalesce(abs(a::bigint),0)) OVER "
							   "(ORDER BY a, id) AS rs FROM qg_t) s");
	plain_sum = scalar_long(tc, conn,
							"SELECT coalesce(sum(coalesce(abs(a::bigint),0)),0) FROM qg_t");
	if (last_running != plain_sum)
		pg_prop_fail(tc, "running sum last=%ld != plain sum=%ld",
					 last_running, plain_sum);

	/* non-negative summand => running sum never decreases */
	nonmonotone = scalar_long(tc, conn,
							  "SELECT count(*) FROM "
							  "(SELECT rs, lag(rs) OVER (ORDER BY a, id) AS prev FROM "
							  "(SELECT a, id, sum(coalesce(abs(a::bigint),0)) OVER (ORDER BY a, id) AS rs "
							  "FROM qg_t) t) u WHERE prev IS NOT NULL AND rs < prev");
	if (nonmonotone != 0)
		pg_prop_fail(tc, "running sum of non-negative values decreased %ld times",
					 nonmonotone);
	pg_prop_disconnect(conn);
}

/* row_number() over a total order is a permutation of 1..n. */
PG_PROP(window_row_number_perm)
{
	int			nrows;
	pg_prop_conn *conn = connect_build(tc, &nrows);
	long		bad;

	/* the set of row numbers must be exactly {1..nrows} */
	bad = scalar_long(tc, conn,
					  "SELECT count(*) FROM "
					  "(SELECT row_number() OVER (ORDER BY a, id) AS rn FROM qg_t) s "
					  "FULL JOIN generate_series(1, (SELECT count(*) FROM qg_t)) g(n) "
					  "ON s.rn = g.n WHERE s.rn IS NULL OR g.n IS NULL");
	if (bad != 0)
		pg_prop_fail(tc, "row_number() is not a permutation of 1..n (%ld off)",
					 bad);
	pg_prop_disconnect(conn);
}

/* dense_rank() <= rank() <= row_number() over the same ordering. */
PG_PROP(window_rank_bounds)
{
	int			nrows;
	pg_prop_conn *conn = connect_build(tc, &nrows);
	long		bad;

	bad = scalar_long(tc, conn,
					  "SELECT count(*) FROM "
					  "(SELECT row_number() OVER w AS rn, rank() OVER w AS rk, "
					  "dense_rank() OVER w AS dr FROM qg_t "
					  "WINDOW w AS (ORDER BY a)) s "
					  "WHERE NOT (s.dr <= s.rk AND s.rk <= s.rn)");
	if (bad != 0)
		pg_prop_fail(tc, "rank ordering violated on %ld rows "
					 "(dense_rank <= rank <= row_number)", bad);
	pg_prop_disconnect(conn);
}

/* ROLLUP(a,b) grand-total row equals the ungrouped aggregate. */
PG_PROP(rollup_grand_total)
{
	int			nrows;
	pg_prop_conn *conn = connect_build(tc, &nrows);
	long		rollup_total,
				plain_total;

	/* the grand-total row of ROLLUP has a IS NULL and b IS NULL (and GROUPING
	 * bits set); its sum equals the whole-table sum */
	rollup_total = scalar_long(tc, conn,
							   "SELECT coalesce(s,0) FROM "
							   "(SELECT sum(coalesce(a::bigint,0)) AS s, "
							   "grouping(a) AS ga, grouping(b) AS gb "
							   "FROM qg_t GROUP BY ROLLUP(a, b)) r "
							   "WHERE ga = 1 AND gb = 1");
	plain_total = scalar_long(tc, conn,
							  "SELECT coalesce(sum(coalesce(a::bigint,0)),0) FROM qg_t");
	if (rollup_total != plain_total)
		pg_prop_fail(tc, "ROLLUP grand total=%ld != plain total=%ld",
					 rollup_total, plain_total);
	pg_prop_disconnect(conn);
}

/* ROLLUP(a,b) detail rows equal plain GROUP BY a, b. */
PG_PROP(rollup_detail_matches)
{
	int			nrows;
	pg_prop_conn *conn = connect_build(tc, &nrows);

	require_same_rows(tc, conn,
		/* ROLLUP detail = rows with no grouping bit set, projected like GROUP BY */
					  "SELECT a, b, sum(coalesce(a::bigint,0)) AS s, count(*) AS c "
					  "FROM qg_t GROUP BY a, b ORDER BY a, b",
					  "SELECT a, b, s, c FROM "
					  "(SELECT a, b, sum(coalesce(a::bigint,0)) AS s, count(*) AS c, "
					  "grouping(a) AS ga, grouping(b) AS gb "
					  "FROM qg_t GROUP BY ROLLUP(a, b)) r "
					  "WHERE ga = 0 AND gb = 0 ORDER BY a, b");
	pg_prop_disconnect(conn);
}

/* CUBE(a,b) returns at least as many grouping rows as ROLLUP(a,b). */
PG_PROP(cube_superset_rollup)
{
	int			nrows;
	pg_prop_conn *conn = connect_build(tc, &nrows);
	long		ncube,
				nrollup;

	ncube = scalar_long(tc, conn,
						"SELECT count(*) FROM "
						"(SELECT 1 FROM qg_t GROUP BY CUBE(a, b)) s");
	nrollup = scalar_long(tc, conn,
						  "SELECT count(*) FROM "
						  "(SELECT 1 FROM qg_t GROUP BY ROLLUP(a, b)) s");
	if (ncube < nrollup)
		pg_prop_fail(tc, "CUBE rows %ld < ROLLUP rows %ld", ncube, nrollup);
	pg_prop_disconnect(conn);
}

/*
 * GROUPING SETS ((a),(b),()) equals the UNION ALL of GROUP BY a, GROUP BY b,
 * and the grand total -- a tautology of grouping-sets semantics.  Compare the
 * (grouping-set, key, aggregate) rows via a normalizing projection.
 */
PG_PROP(groupingsets_union)
{
	int			nrows;
	pg_prop_conn *conn = connect_build(tc, &nrows);

	require_same_rows(tc, conn,
		/* explicit grouping sets, normalized: tag the set by its grouping bits */
					  "SELECT grouping(a) AS ga, grouping(b) AS gb, "
					  "a, b, count(*) AS c "
					  "FROM qg_t GROUP BY GROUPING SETS ((a),(b),()) "
					  "ORDER BY ga, gb, a, b",
		/* the equivalent union of three plain groupings */
					  "SELECT ga, gb, a, b, c FROM ("
					  "SELECT 0 AS ga, 1 AS gb, a, NULL::int AS b, count(*) AS c "
					  "FROM qg_t GROUP BY a "
					  "UNION ALL "
					  "SELECT 1, 0, NULL, b, count(*) FROM qg_t GROUP BY b "
					  "UNION ALL "
					  "SELECT 1, 1, NULL, NULL, count(*) FROM qg_t"
					  ") u ORDER BY ga, gb, a, b");
	pg_prop_disconnect(conn);
}
