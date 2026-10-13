/*-------------------------------------------------------------------------
 *
 * prop_geopoint.c
 *	  Property-based tests for geometric point/line/box operations.
 *
 * Target: src/backend/utils/adt/geo_ops.c.  The geometric types (point, box,
 * line segment) carry exact invariants (box containment, point-in-box) and
 * metric invariants (distance symmetry, non-negativity, Euclidean distance),
 * which these properties check with float tolerances where arithmetic is
 * inexact and with exact equality where it is not.
 *
 * Coordinates are drawn as small integers (-100..100) and printed into point
 * and box literals so that areas and containment tests are exact and the
 * coordinate-range oracle (BETWEEN) matches the server's <@ exactly.
 * Distances still involve sqrt, so those are compared within a tolerance.
 *
 * The oracle here is an independent formula for each operation: distance is
 * checked against the Euclidean formula sqrt(dx^2+dy^2); box area against
 * |x2-x1|*|y2-y1|; point-in-box against an explicit coordinate-range test.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_geopoint.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "geopoint"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw one small integer coordinate in [-100, 100].  Small integers keep the
 * literals exact: box area and containment are then exact and the BETWEEN
 * oracle matches the server's inclusive <@ on boundaries.
 */
static int
draw_coord(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, -100, 100);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the geometric operation errored or produced NULL on
 * finite integer-coordinate inputs, which is itself a bug -- so either is a
 * failure, reported with the SQL.
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
 * Point distance is a non-negative, symmetric metric: (P <-> Q) >= 0 and
 * (P <-> Q) = (Q <-> P).  The reverse ordering recomputes the same distance,
 * so any asymmetry in the distance code surfaces here.
 */
PG_PROP(distance_nonneg_symmetric)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			px,
				py,
				qx,
				qy;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	px = draw_coord(tc);
	py = draw_coord(tc);
	qx = draw_coord(tc);
	qy = draw_coord(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('(%d,%d)'::point <-> '(%d,%d)'::point) >= 0 "
					 "AND ('(%d,%d)'::point <-> '(%d,%d)'::point) "
					 "= ('(%d,%d)'::point <-> '(%d,%d)'::point)",
					 px, py, qx, qy,
					 px, py, qx, qy,
					 qx, qy, px, py);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Distance of a point to itself is zero: (P <-> P) = 0.  Integer coordinates
 * make this exact, so no tolerance is needed.
 */
PG_PROP(distance_zero_iff_equal)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			px,
				py;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	px = draw_coord(tc);
	py = draw_coord(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('(%d,%d)'::point <-> '(%d,%d)'::point) = 0",
					 px, py, px, py);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Point distance equals the Euclidean distance sqrt(dx^2 + dy^2).  The
 * independent oracle is the Pythagorean formula computed in float8; sqrt makes
 * this inexact, so compare within a tolerance.  Coordinates are cast to
 * float8 so ^ is float power rather than integer exponentiation.
 */
PG_PROP(distance_matches_pythagoras)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			px,
				py,
				qx,
				qy;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	px = draw_coord(tc);
	py = draw_coord(tc);
	qx = draw_coord(tc);
	qy = draw_coord(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(('(%d,%d)'::point <-> '(%d,%d)'::point) "
					 "- sqrt((%d - %d)::float8 ^ 2 + (%d - %d)::float8 ^ 2)) < 1e-6",
					 px, py, qx, qy,
					 px, qx, py, qy);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A box contains both of its defining corner points: B @> corner1 and
 * B @> corner2.  @> is box-contains-point; integer coordinates make the
 * inclusive boundary test exact.
 */
PG_PROP(box_contains_its_corners)
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
					 "SELECT ('(%d,%d),(%d,%d)'::box @> '(%d,%d)'::point) "
					 "AND ('(%d,%d),(%d,%d)'::box @> '(%d,%d)'::point)",
					 x1, y1, x2, y2, x1, y1,
					 x1, y1, x2, y2, x2, y2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A box contains its own center point.  @@ applied to a box yields its center
 * point, which must lie inside (or on the boundary of) the box.
 */
PG_PROP(box_contains_center)
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
					 "SELECT ('(%d,%d),(%d,%d)'::box "
					 "@> (@@ '(%d,%d),(%d,%d)'::box))",
					 x1, y1, x2, y2, x1, y1, x2, y2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Box area is non-negative and equals |x2-x1| * |y2-y1|.  The independent
 * oracle is the width-times-height product in float8; area() uses the same
 * exact integer coordinates, so the two agree within a tiny tolerance.
 */
PG_PROP(box_area_nonneg_and_matches)
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
					 "SELECT area('(%d,%d),(%d,%d)'::box) >= 0 "
					 "AND abs(area('(%d,%d),(%d,%d)'::box) "
					 "- (abs(%d - %d) * abs(%d - %d))::float8) < 1e-6",
					 x1, y1, x2, y2,
					 x1, y1, x2, y2,
					 x2, x1, y2, y1);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Point-in-box agrees with the explicit coordinate-range test: (P <@ B) holds
 * exactly when P.x lies in the box's x-range and P.y in its y-range.  The
 * independent oracle is the BETWEEN test over least/greatest of the corner
 * coordinates; integer coordinates make BETWEEN exact, and <@ is inclusive on
 * the boundary, matching BETWEEN.
 */
PG_PROP(point_in_box_consistent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			px,
				py,
				x1,
				y1,
				x2,
				y2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	px = draw_coord(tc);
	py = draw_coord(tc);
	x1 = draw_coord(tc);
	y1 = draw_coord(tc);
	x2 = draw_coord(tc);
	y2 = draw_coord(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('(%d,%d)'::point <@ '(%d,%d),(%d,%d)'::box) "
					 "= (%d BETWEEN least(%d,%d) AND greatest(%d,%d) "
					 "AND %d BETWEEN least(%d,%d) AND greatest(%d,%d))",
					 px, py, x1, y1, x2, y2,
					 px, x1, x2, x1, x2,
					 py, y1, y2, y1, y2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
