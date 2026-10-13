/*-------------------------------------------------------------------------
 *
 * prop_cubedist.c
 *	  Property-based tests for the cube extension's distance metric.
 *
 * Where prop_cube.c covers containment and the box algebra, this area targets
 * the metric surface of contrib/cube: cube_distance as a Euclidean metric
 * over point cubes, plus the coordinate accessors (cube_ll_coord,
 * cube_ur_coord) and cube_dim on single-point cubes.  A failure means
 * cube_distance is not a metric (negative, asymmetric, non-zero on a point to
 * itself, or violating the triangle inequality) or that a coordinate accessor
 * disagrees with the integer the point was built from -- each a bug in
 * contrib/cube's float8 math or coordinate indexing.
 *
 * Points are built from integer coordinates in -50..50 over dimension 1..3.
 * Integers are exact in float8, so each squared-difference sum is exact and
 * the only inexact step is the final sqrt; the Euclidean-match property
 * therefore uses a 1e-6 tolerance, and the triangle inequality a 1e-9 slack.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_cubedist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cubedist"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Dimension and coordinate domains. */
#define MAX_DIM 3
#define COORD_MIN (-50)
#define COORD_MAX 50

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a cube operation errored or produced NULL on valid
 * inputs, which is itself a bug -- so either is a failure, reported with the
 * SQL.  Mirrors require_true in prop_numeric.c / prop_cube.c.
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
 * Connect to the test cluster and ensure the cube extension exists.  Each
 * property opens its own connection (the Tier-B pattern); CREATE EXTENSION IF
 * NOT EXISTS makes it idempotent across the thousands of per-case
 * connections.  On any failure the property is failed with the message.
 */
static pg_prop_conn *
connect_cube(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "CREATE EXTENSION IF NOT EXISTS cube",
						  &err))
		pg_prop_fail(tc, "setup failed (cube): %s", err ? err : "");

	return conn;
}

/*
 * Draw a point of dimension d, filling coords[0..d-1] with integers in
 * [COORD_MIN, COORD_MAX], and emit the point as a cube literal into buf:
 *
 *		'(c1, c2, ..., cd)'::cube
 *
 * The text form is used (not cube(ARRAY[...])) because it is the most direct
 * point constructor and needs no per-element casting; negative coordinates
 * are harmless inside the single-quoted literal.  cube normalizes a d-coord
 * literal to a single point whose lower-left and upper-right corners coincide.
 */
static void
draw_point(pg_prop_tc *tc, StringInfo buf, int d, int *coords)
{
	int			k;

	appendStringInfoString(buf, "'(");
	for (k = 0; k < d; k++)
	{
		coords[k] = (int) pg_prop_draw_int(tc, COORD_MIN, COORD_MAX);
		if (k > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%d", coords[k]);
	}
	appendStringInfoString(buf, ")'::cube");
}

/*
 * cube_distance(A, B) >= 0 for any two same-dimension point cubes.
 */
PG_PROP(distance_nonneg)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, MAX_DIM);
	int			ca[MAX_DIM];
	int			cb[MAX_DIM];

	conn = connect_cube(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_point(tc, &a, d, ca);
	draw_point(tc, &b, d, cb);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT cube_distance(%s, %s) >= 0",
					 a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * cube_distance is symmetric: cube_distance(A, B) = cube_distance(B, A).
 * Integer coordinates make the squared-difference sum exact and sqrt
 * deterministic, so this holds with no tolerance.
 */
PG_PROP(distance_symmetric)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, MAX_DIM);
	int			ca[MAX_DIM];
	int			cb[MAX_DIM];

	conn = connect_cube(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_point(tc, &a, d, ca);
	draw_point(tc, &b, d, cb);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cube_distance(%s, %s) = cube_distance(%s, %s)",
					 a.data, b.data, b.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * cube_distance(A, A) = 0: the distance from a point to itself is exactly
 * zero.
 */
PG_PROP(distance_self_zero)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, MAX_DIM);
	int			ca[MAX_DIM];

	conn = connect_cube(tc);

	initStringInfo(&a);
	draw_point(tc, &a, d, ca);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT cube_distance(%s, %s) = 0",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Triangle inequality: cube_distance(A, C) <= cube_distance(A, B) +
 * cube_distance(B, C) + 1e-9.  The small slack absorbs the rounding of three
 * independent sqrt results so a tight (equal) triple on collinear points
 * cannot fail on a last-bit rounding difference.
 */
PG_PROP(triangle_inequality)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				c,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, MAX_DIM);
	int			ca[MAX_DIM];
	int			cb[MAX_DIM];
	int			cc[MAX_DIM];

	conn = connect_cube(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	draw_point(tc, &a, d, ca);
	draw_point(tc, &b, d, cb);
	draw_point(tc, &c, d, cc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cube_distance(%s, %s) <= "
					 "cube_distance(%s, %s) + cube_distance(%s, %s) + 1e-9",
					 a.data, c.data,
					 a.data, b.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Coordinate accessors on a point: for a single-point cube P and every
 * dimension k in 1..d, cube_ll_coord(P, k) = cube_ur_coord(P, k) = the k-th
 * coordinate the point was built from.  The expected integers are substituted
 * directly so the accessor is checked against the source value, not against
 * itself.
 */
PG_PROP(coord_access)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, MAX_DIM);
	int			ca[MAX_DIM];
	int			k;

	conn = connect_cube(tc);

	initStringInfo(&a);
	draw_point(tc, &a, d, ca);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ");
	for (k = 0; k < d; k++)
	{
		if (k > 0)
			appendStringInfoString(&q, " AND ");
		/* emit negative expected coords as (%d) so the comparison parses */
		appendStringInfo(&q,
						 "cube_ll_coord(%s, %d) = (%d) "
						 "AND cube_ur_coord(%s, %d) = (%d)",
						 a.data, k + 1, ca[k],
						 a.data, k + 1, ca[k]);
	}

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * cube_dim(P) = d: a point built from d coordinates reports dimension d.
 */
PG_PROP(dim_matches)
{
	pg_prop_conn *conn;
	StringInfoData a,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, MAX_DIM);
	int			ca[MAX_DIM];

	conn = connect_cube(tc);

	initStringInfo(&a);
	draw_point(tc, &a, d, ca);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT cube_dim(%s) = %d", a.data, d);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Euclidean match: cube_distance(A, B) equals the Euclidean distance computed
 * independently in SQL from the integer coordinates, within 1e-6.
 *
 *		abs(cube_distance(A, B) - sqrt(sum (ai - bi)^2)) < 1e-6
 *
 * The squared-difference sum is formed from the drawn integers (each as a
 * parenthesized float8 literal so a negative difference's square is exact),
 * giving an independent oracle: cube_distance must agree with the textbook
 * formula up to float8 sqrt rounding.
 */
PG_PROP(distance_matches_euclidean)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				sumsq,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, MAX_DIM);
	int			ca[MAX_DIM];
	int			cb[MAX_DIM];
	int			k;

	conn = connect_cube(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_point(tc, &a, d, ca);
	draw_point(tc, &b, d, cb);

	/* sum of squared coordinate differences, as a float8 expression */
	initStringInfo(&sumsq);
	for (k = 0; k < d; k++)
	{
		int			diff = ca[k] - cb[k];

		if (k > 0)
			appendStringInfoString(&sumsq, " + ");
		/* (diff)^2; parenthesize diff so a negative value squares correctly */
		appendStringInfo(&sumsq, "(%d)::float8 * (%d)::float8", diff, diff);
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(cube_distance(%s, %s) - sqrt(%s)) < 1e-6",
					 a.data, b.data, sumsq.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(sumsq.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
