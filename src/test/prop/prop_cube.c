/*-------------------------------------------------------------------------
 *
 * prop_cube.c
 *	  Property-based tests for the cube contrib extension.
 *
 * cube represents points and boxes in N dimensions (contrib/cube); these
 * check text round-trip, containment reflexivity, union/intersection bounds,
 * and dimension/distance invariants.
 *
 * A cube is either a point in d-space or an axis-aligned box between a
 * lower-left and an upper-right corner.  Its coordinates are float8, but the
 * literals here draw small integers (-20..20) so every comparison is exact.
 * The oracle is cube's own algebra: a cube contains itself; a cube's text
 * output parses back equal; cube_union bounds both operands; cube_inter is
 * bounded by each operand; cube_distance is a non-negative, symmetric metric;
 * and each lower-left coordinate is <= its matching upper-right coordinate.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_cube.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cube"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a cube operation errored or produced NULL on valid
 * inputs, which is itself a bug -- so either is a failure, reported with the
 * SQL.  Mirrors require_true in prop_numeric.c.
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
 * NOT EXISTS makes it idempotent across connections.  On any failure the
 * property is failed with the message.
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
 * Draw a cube literal of dimension d into buf as a single-quoted, cast
 * expression: '...'::cube.  With probability 1/2 it is a point
 * '(x1,...,xd)'::cube; otherwise a box '(l1,...,ld),(h1,...,hd)'::cube with
 * each lo <= hi (cube normalizes internally, but keeping the literal clean
 * makes failures readable).  Coordinates are small integers in -20..20, which
 * float8 represents exactly, so every downstream comparison is exact.  The
 * literal only ever contains parentheses, commas, digits and minus signs, all
 * of which are fine inside single quotes.
 */
static void
draw_cube(pg_prop_tc *tc, StringInfo buf, int d)
{
	bool		is_box = pg_prop_draw_bool(tc);
	int			k;

	appendStringInfoChar(buf, '\'');

	if (!is_box)
	{
		/* point: (x1, x2, ..., xd) */
		appendStringInfoChar(buf, '(');
		for (k = 0; k < d; k++)
		{
			if (k > 0)
				appendStringInfoString(buf, ", ");
			appendStringInfo(buf, "%d",
							 (int) pg_prop_draw_int(tc, -20, 20));
		}
		appendStringInfoChar(buf, ')');
	}
	else
	{
		int			lo[4];
		int			hi[4];

		/* box: draw both corners, then normalize so each lo <= hi */
		for (k = 0; k < d; k++)
		{
			int			a = (int) pg_prop_draw_int(tc, -20, 20);
			int			b = (int) pg_prop_draw_int(tc, -20, 20);

			lo[k] = (a <= b) ? a : b;
			hi[k] = (a <= b) ? b : a;
		}

		appendStringInfoChar(buf, '(');
		for (k = 0; k < d; k++)
		{
			if (k > 0)
				appendStringInfoString(buf, ", ");
			appendStringInfo(buf, "%d", lo[k]);
		}
		appendStringInfoString(buf, "),(");
		for (k = 0; k < d; k++)
		{
			if (k > 0)
				appendStringInfoString(buf, ", ");
			appendStringInfo(buf, "%d", hi[k]);
		}
		appendStringInfoChar(buf, ')');
	}

	appendStringInfoString(buf, "'::cube");
}

/*
 * Text round-trip: a cube's text output parses back to the identical cube.
 *
 *		(C)::text::cube = C
 *
 * Any quoting or formatting bug in cube_out/cube_in breaks this.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	StringInfoData c,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, 4);

	conn = connect_cube(tc);

	initStringInfo(&c);
	draw_cube(tc, &c, d);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::cube = (%s)",
					 c.data, c.data);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment reflexivity: every cube contains itself and is contained by
 * itself.
 *
 *		C @> C AND C <@ C
 *
 * @> is "contains" and <@ is "contained by"; a boundary bug in the
 * containment test (strict vs. non-strict) breaks this.
 */
PG_PROP(contains_self)
{
	pg_prop_conn *conn;
	StringInfoData c,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, 4);

	conn = connect_cube(tc);

	initStringInfo(&c);
	draw_cube(tc, &c, d);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s) @> (%s) AND (%s) <@ (%s)",
					 c.data, c.data, c.data, c.data);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Dimension matches: cube_dim reports the dimension the literal was built
 * with.
 *
 *		cube_dim(C) = d
 *
 * cube_dim counts the coordinates; a point and a box of the same dimension
 * both report d.
 */
PG_PROP(dim_matches)
{
	pg_prop_conn *conn;
	StringInfoData c,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, 4);

	conn = connect_cube(tc);

	initStringInfo(&c);
	draw_cube(tc, &c, d);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT cube_dim(%s) = %d", c.data, d);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Union bounds both operands: the bounding box of A and B contains each.
 *
 *		cube_union(A, B) @> A AND cube_union(A, B) @> B
 *
 * cube_union requires same-dimension operands, so A and B are both drawn at
 * the one drawn dimension d.
 */
PG_PROP(union_contains_both)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, 4);

	conn = connect_cube(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_cube(tc, &a, d);
	draw_cube(tc, &b, d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cube_union(%s, %s) @> (%s) "
					 "AND cube_union(%s, %s) @> (%s)",
					 a.data, b.data, a.data,
					 a.data, b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * When two cubes overlap, their intersection is contained in each.  For
 * DISJOINT cubes cube_inter does not return an empty cube -- it returns a box
 * spanning the gap, contained in neither -- so the law is guarded by the
 * overlap operator &&.
 *
 *		A && B => cube_inter(A, B) <@ A AND cube_inter(A, B) <@ B
 *
 * cube_inter requires same-dimension operands, so A and B are both drawn at
 * the one drawn dimension d.
 */
PG_PROP(inter_contained_in_both)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, 4);

	conn = connect_cube(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_cube(tc, &a, d);
	draw_cube(tc, &b, d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN (%s) && (%s) "
					 "THEN cube_inter(%s, %s) <@ (%s) AND cube_inter(%s, %s) <@ (%s) "
					 "ELSE true END",
					 a.data, b.data,
					 a.data, b.data, a.data,
					 a.data, b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Distance is a non-negative, symmetric metric: cube_distance is >= 0 and
 * does not depend on operand order.
 *
 *		cube_distance(A, B) >= 0 AND cube_distance(A, B) = cube_distance(B, A)
 *
 * cube_distance requires same-dimension operands, so A and B are both drawn
 * at the one drawn dimension d.  Integer coordinates keep the Euclidean
 * distance's intermediate arithmetic exact, so the symmetric equality is
 * exact.
 */
PG_PROP(distance_nonneg_symmetric)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, 4);

	conn = connect_cube(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_cube(tc, &a, d);
	draw_cube(tc, &b, d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cube_distance(%s, %s) >= 0 "
					 "AND cube_distance(%s, %s) = cube_distance(%s, %s)",
					 a.data, b.data,
					 a.data, b.data, b.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Lower-left <= upper-right per coordinate: for every dimension k in 1..d the
 * lower-left corner coordinate does not exceed the upper-right one.
 *
 *		bool_and(cube_ll_coord(C, k) <= cube_ur_coord(C, k))
 *			FROM generate_series(1, d) k
 *
 * This is cube's internal normalization invariant surfaced through the
 * coordinate accessors; it must hold for points (where ll = ur) and boxes
 * alike.
 */
PG_PROP(ll_ur_bound_coordinates)
{
	pg_prop_conn *conn;
	StringInfoData c,
				q;
	int			d = (int) pg_prop_draw_int(tc, 1, 4);

	conn = connect_cube(tc);

	initStringInfo(&c);
	draw_cube(tc, &c, d);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT bool_and(cube_ll_coord(%s, k) <= cube_ur_coord(%s, k)) "
					 "FROM generate_series(1, %d) k",
					 c.data, c.data, d);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
