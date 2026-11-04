/*-------------------------------------------------------------------------
 *
 * prop_geoseg.c
 *	  Property-based tests for geometric line-segment, line and circle ops.
 *
 * Target: src/backend/utils/adt/geo_ops.c, the lseg/line/circle operations
 * distinct from the point/box area covered by prop_geopoint.c.  Line segments
 * and circles carry length/radius/containment invariants: a segment's length
 * is the distance between its endpoints and its endpoints lie on it; a circle
 * contains its own center, its area is pi*r^2, its stored radius round-trips,
 * and a point lies in it exactly when its distance to the center is within the
 * radius.  These properties check those invariants with float tolerances where
 * sqrt/pi make arithmetic inexact, and with exact equality where the stored
 * integer radius is exact.
 *
 * Coordinates and radii are drawn as small integers and printed into lseg,
 * point and circle literals so that lengths and areas are near-exact (only the
 * sqrt/pi rounding needs tolerance) and the integer radius round-trips
 * exactly.
 *
 * The oracle here is an independent formula for each operation: segment length
 * against the point-distance operator; circle area against pi*r^2; point-in-
 * circle against the explicit distance-to-center test.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_geoseg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "geoseg"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw one small integer coordinate in [-100, 100].  Small integers keep the
 * literals exact: segment lengths and circle areas are then off from the
 * independent formula only by sqrt/pi rounding, and the integer radius is
 * stored exactly.
 */
static int
draw_coord(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, -100, 100);
}

/*
 * Draw one strictly-positive integer radius in [1, 50].  r > 0 keeps the
 * circle non-degenerate so it has an interior that contains its center.
 */
static int
draw_radius(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, 1, 50);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the geometric operation errored or produced NULL on finite
 * integer-coordinate inputs, which is itself a bug -- so either is a failure,
 * reported with the SQL.
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
 * A segment's length equals the distance between its endpoints: the independent
 * oracle is the point-distance operator over the two endpoints.  Both compute a
 * Euclidean distance via sqrt, so compare within a tolerance.
 */
PG_PROP(lseg_length_matches_endpoint_distance)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			x1,
				y1,
				x2,
				y2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x1 = draw_coord(tc);
	y1 = draw_coord(tc);
	x2 = draw_coord(tc);
	y2 = draw_coord(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(length('[(%d,%d),(%d,%d)]'::lseg) "
					 "- ('(%d,%d)'::point <-> '(%d,%d)'::point)) < 1e-6",
					 x1, y1, x2, y2,
					 x1, y1, x2, y2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A segment's length is non-negative: length([P,Q]) >= 0.  A length is a
 * magnitude, so it can never be negative regardless of endpoint ordering.
 */
PG_PROP(lseg_length_nonneg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			x1,
				y1,
				x2,
				y2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x1 = draw_coord(tc);
	y1 = draw_coord(tc);
	x2 = draw_coord(tc);
	y2 = draw_coord(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length('[(%d,%d),(%d,%d)]'::lseg) >= 0",
					 x1, y1, x2, y2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Both defining endpoints lie on the segment: P <@ [P,Q] and Q <@ [P,Q].  For
 * point/lseg, <@ means "point is on segment"; the endpoints are trivially on
 * it, and integer coordinates make the on-segment test exact.
 */
PG_PROP(lseg_endpoints_on_segment)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			x1,
				y1,
				x2,
				y2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	x1 = draw_coord(tc);
	y1 = draw_coord(tc);
	x2 = draw_coord(tc);
	y2 = draw_coord(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('(%d,%d)'::point <@ '[(%d,%d),(%d,%d)]'::lseg) "
					 "AND ('(%d,%d)'::point <@ '[(%d,%d),(%d,%d)]'::lseg)",
					 x1, y1, x1, y1, x2, y2,
					 x2, y2, x1, y1, x2, y2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A circle contains its own center: C @> center.  @> is circle-contains-point;
 * for a positive radius the center is strictly interior, so containment must
 * hold.
 */
PG_PROP(circle_center_inside)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			cx,
				cy,
				r;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	cx = draw_coord(tc);
	cy = draw_coord(tc);
	r = draw_radius(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT '<(%d,%d),%d>'::circle @> '(%d,%d)'::point",
					 cx, cy, r, cx, cy);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A circle's area equals pi * r^2: the independent oracle is pi()*r^2 computed
 * in float8.  area() and the formula both involve pi, so compare within a
 * tolerance.  The radius is cast to float8 so ^ is float power.
 */
PG_PROP(circle_area_matches_formula)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			cx,
				cy,
				r;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	cx = draw_coord(tc);
	cy = draw_coord(tc);
	r = draw_radius(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(area('<(%d,%d),%d>'::circle) "
					 "- (pi() * %d::float8 ^ 2)) < 1e-6",
					 cx, cy, r, r);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A circle's stored radius round-trips: radius('<(cx,cy),r>') = r.  The radius
 * is an integer and stored exactly, so this holds with no tolerance.
 */
PG_PROP(circle_radius_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			cx,
				cy,
				r;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	cx = draw_coord(tc);
	cy = draw_coord(tc);
	r = draw_radius(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT radius('<(%d,%d),%d>'::circle) = %d",
					 cx, cy, r, r);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A point lies in a circle exactly when its distance to the center is within
 * the radius: (P <@ C) = (dist(center, P) <= r).  The independent oracle is the
 * point-distance operator against the center; a tiny epsilon on the right-hand
 * side handles the boundary case (distance exactly r) consistently with the
 * server's inclusive containment.
 */
PG_PROP(point_in_circle_iff_distance_le_radius)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			px,
				py,
				cx,
				cy,
				r;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	px = draw_coord(tc);
	py = draw_coord(tc);
	cx = draw_coord(tc);
	cy = draw_coord(tc);
	r = draw_radius(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('(%d,%d)'::point <@ '<(%d,%d),%d>'::circle) "
					 "= (('(%d,%d)'::point <-> '(%d,%d)'::point) "
					 "<= %d::float8 + 1e-9)",
					 px, py, cx, cy, r,
					 cx, cy, px, py, r);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
