/*-------------------------------------------------------------------------
 *
 * prop_boxops.c
 *	  Property-based tests for geometric box and polygon operations.
 *
 * Target: src/backend/utils/adt/geo_ops.c, the box/polygon operations distinct
 * from the point/lseg/circle areas covered by prop_geopoint.c and
 * prop_geoseg.c.  A box carries area/center/containment invariants: its area is
 * non-negative and equals the product of its side lengths; it contains its own
 * center and corners; the intersection of two overlapping boxes is contained in
 * each; the bounding box of two boxes covers both; and a polygon built from a
 * box encloses the same area.  width()/height() report the side lengths.
 *
 * The oracle here is an independent formula or a containment relation for each
 * operation: box area against abs(x2-x1)*abs(y2-y1); self-containment of center
 * and corners; intersection and bound_box containment; polygon(box) area
 * against box area.  Integer corners keep areas and side lengths exact, so only
 * the area comparisons need a tolerance.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_boxops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "boxops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw one small integer coordinate in [-50, 50].  Small integers keep the
 * literals exact: box areas and side lengths are then integer-valued and the
 * area comparisons are off from the independent formula only by any float
 * rounding inside area()/polygon().
 */
static int
draw_coord(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, -50, 50);
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
 * A box's area is non-negative: area(B) >= 0.  An area is a magnitude, so it
 * can never be negative regardless of corner ordering.  (The box type
 * normalises corners on input, but the invariant holds either way.)
 */
PG_PROP(area_nonneg)
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
					 "SELECT area('((%d,%d),(%d,%d))'::box) >= 0",
					 x1, y1, x2, y2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A box's area equals the product of its side lengths: area(B) =
 * abs(x2-x1)*abs(y2-y1).  The box type normalises corners on input, so the
 * identity is independent of which corner was given first; integer corners make
 * both sides exact, so the only slack is any float rounding in area().
 */
PG_PROP(area_formula)
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
					 "SELECT abs(area('((%d,%d),(%d,%d))'::box) "
					 "- (abs(%d - (%d)) * abs(%d - (%d)))::float8) < 1e-6",
					 x1, y1, x2, y2,
					 x2, x1, y2, y1);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A box contains its own center: B @> center(B).  @> is box-contains-point and
 * the center lies within the box's (inclusive) bounds, so containment must
 * hold for any box.
 */
PG_PROP(contains_center)
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
					 "SELECT '((%d,%d),(%d,%d))'::box "
					 "@> center('((%d,%d),(%d,%d))'::box)",
					 x1, y1, x2, y2,
					 x1, y1, x2, y2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A box contains its own defining corners: B @> point(x1,y1) AND
 * B @> point(x2,y2).  The corners lie on the box's boundary, and box
 * containment is inclusive, so both must be contained.
 */
PG_PROP(contains_corners)
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
					 "SELECT ('((%d,%d),(%d,%d))'::box @> point(%d, %d)) "
					 "AND ('((%d,%d),(%d,%d))'::box @> point(%d, %d))",
					 x1, y1, x2, y2, x1, y1,
					 x1, y1, x2, y2, x2, y2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * When two boxes overlap, their intersection is contained in each: when
 * B1 && B2, (B1 # B2) <@ B1 AND (B1 # B2) <@ B2.  # is box intersection and <@
 * is box-contained-in-box.  The intersection is only defined when the boxes
 * overlap, so the property is guarded by CASE WHEN B1 && B2 (the non-overlapping
 * case is vacuously true).
 */
PG_PROP(intersection_contained)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			ax1,
				ay1,
				ax2,
				ay2,
				bx1,
				by1,
				bx2,
				by2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	ax1 = draw_coord(tc);
	ay1 = draw_coord(tc);
	ax2 = draw_coord(tc);
	ay2 = draw_coord(tc);
	bx1 = draw_coord(tc);
	by1 = draw_coord(tc);
	bx2 = draw_coord(tc);
	by2 = draw_coord(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN "
					 "'((%d,%d),(%d,%d))'::box && '((%d,%d),(%d,%d))'::box "
					 "THEN ('((%d,%d),(%d,%d))'::box # '((%d,%d),(%d,%d))'::box) "
					 "<@ '((%d,%d),(%d,%d))'::box "
					 "AND ('((%d,%d),(%d,%d))'::box # '((%d,%d),(%d,%d))'::box) "
					 "<@ '((%d,%d),(%d,%d))'::box "
					 "ELSE true END",
					 ax1, ay1, ax2, ay2, bx1, by1, bx2, by2,
					 ax1, ay1, ax2, ay2, bx1, by1, bx2, by2,
					 ax1, ay1, ax2, ay2,
					 ax1, ay1, ax2, ay2, bx1, by1, bx2, by2,
					 bx1, by1, bx2, by2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The bounding box of two boxes covers both: bound_box(B1, B2) @> B1 AND @> B2.
 * bound_box is the smallest box covering both operands, so each operand is
 * contained in it; @> here is box-contains-box.
 */
PG_PROP(bound_box_contains)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			ax1,
				ay1,
				ax2,
				ay2,
				bx1,
				by1,
				bx2,
				by2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	ax1 = draw_coord(tc);
	ay1 = draw_coord(tc);
	ax2 = draw_coord(tc);
	ay2 = draw_coord(tc);
	bx1 = draw_coord(tc);
	by1 = draw_coord(tc);
	bx2 = draw_coord(tc);
	by2 = draw_coord(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (bound_box('((%d,%d),(%d,%d))'::box, "
					 "'((%d,%d),(%d,%d))'::box) @> '((%d,%d),(%d,%d))'::box) "
					 "AND (bound_box('((%d,%d),(%d,%d))'::box, "
					 "'((%d,%d),(%d,%d))'::box) @> '((%d,%d),(%d,%d))'::box)",
					 ax1, ay1, ax2, ay2, bx1, by1, bx2, by2,
					 ax1, ay1, ax2, ay2,
					 ax1, ay1, ax2, ay2, bx1, by1, bx2, by2,
					 bx1, by1, bx2, by2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A polygon built from a box encloses the same area: area(polygon(B)) =
 * area(B).  polygon(box) produces the four-corner rectangle, so the two area
 * computations must agree; integer corners make both exact up to any float
 * rounding, so compare within a tolerance.
 */
PG_PROP(box_to_polygon_contains_center)
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

	/*
	 * area(polygon) is not defined, but the polygon derived from a box must
	 * still cover the box's centre point: polygon(box) @> center(box).
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT polygon('((%d,%d),(%d,%d))'::box) "
					 "@> center('((%d,%d),(%d,%d))'::box)",
					 x1, y1, x2, y2,
					 x1, y1, x2, y2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A box's width and height equal its side lengths: width(B) = abs(x2-x1) AND
 * height(B) = abs(y2-y1).  The box type normalises corners on input, so each
 * side length is independent of input order; integer corners make both sides
 * exact, so compare within a tolerance to absorb any float rounding.
 */
PG_PROP(width_height)
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
					 "SELECT abs(width('((%d,%d),(%d,%d))'::box) "
					 "- abs(%d - (%d))::float8) < 1e-6 "
					 "AND abs(height('((%d,%d),(%d,%d))'::box) "
					 "- abs(%d - (%d))::float8) < 1e-6",
					 x1, y1, x2, y2, x2, x1,
					 x1, y1, x2, y2, y2, y1);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
