/*-------------------------------------------------------------------------
 *
 * prop_seg.c
 *	  Property-based tests for the seg contrib extension.
 *
 * seg represents a 1-D interval [lower, upper] with floating-point bounds and
 * error tolerances (contrib/seg).  These check text round-trip, containment
 * reflexivity, the lower <= upper invariant, overlap symmetry, and that
 * containment agrees with the stored bounds.  (seg has no union/intersection
 * operator, only comparison and containment.)  Integer-valued bounds are used
 * so the underlying float stores and compares exactly.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.  connect_seg() creates the extension.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_seg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "seg"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the seg operation errored or produced NULL on valid
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
 * Connect to the test cluster and ensure the seg extension exists.  Each
 * property opens its own connection (the Tier-B pattern); CREATE EXTENSION
 * IF NOT EXISTS is idempotent across connections.  On failure the property is
 * failed with the message.
 */
static pg_prop_conn *
connect_seg(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn, "CREATE EXTENSION IF NOT EXISTS seg", &err))
		pg_prop_fail(tc, "setup failed (seg): %s", err ? err : "");

	return conn;
}

/*
 * Draw a seg literal into buf as '<lo> .. <hi>'::seg, with two integers drawn
 * in -100..100 and ordered so lo <= hi.  The chosen lo and hi are returned
 * through *lo / *hi so a caller can build a point value inside the interval.
 * Integer bounds keep the underlying float exact for equality comparisons.
 */
static void
draw_seg(pg_prop_tc *tc, StringInfo buf, int *lo, int *hi)
{
	int			a = (int) pg_prop_draw_int(tc, -100, 100);
	int			b = (int) pg_prop_draw_int(tc, -100, 100);
	int			l,
				h;

	if (a <= b)
	{
		l = a;
		h = b;
	}
	else
	{
		l = b;
		h = a;
	}

	appendStringInfo(buf, "'%d .. %d'::seg", l, h);

	*lo = l;
	*hi = h;
}

/*
 * Text round-trip: (S)::text::seg = S.  seg_out formats the interval and
 * seg_in must parse that text back to the identical seg; integer-valued
 * bounds round-trip exactly through the float store.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;
	int			lo,
				hi;

	conn = connect_seg(tc);

	initStringInfo(&s);
	draw_seg(tc, &s, &lo, &hi);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::seg = (%s)", s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment reflexivity: S @> S AND S <@ S.  A seg always contains itself
 * and is contained in itself.
 */
PG_PROP(contains_self)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;
	int			lo,
				hi;

	conn = connect_seg(tc);

	initStringInfo(&s);
	draw_seg(tc, &s, &lo, &hi);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s) @> (%s) AND (%s) <@ (%s)",
					 s.data, s.data, s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Interval invariant: seg_lower(S) <= seg_upper(S).  Every seg's lower bound
 * is at most its upper bound.
 */
PG_PROP(lower_le_upper)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;
	int			lo,
				hi;

	conn = connect_seg(tc);

	initStringInfo(&s);
	draw_seg(tc, &s, &lo, &hi);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT seg_lower(%s) <= seg_upper(%s)",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * seg exposes comparison and containment operators but no union/intersection,
 * so overlap (&&) is checked for symmetry instead.
 */
PG_PROP(overlap_is_symmetric)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;
	int			alo,
				ahi,
				blo,
				bhi;

	conn = connect_seg(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_seg(tc, &a, &alo, &ahi);
	draw_seg(tc, &b, &blo, &bhi);

	/* seg has no union/intersection operator; && overlap is symmetric */
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s) && (%s)) = ((%s) && (%s))",
					 a.data, b.data, b.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Strict left/right (<< and >>) are mutually exclusive and imply no overlap:
 * if A << B (A entirely left of B) then not (A && B), and not (B << A).
 */
PG_PROP(strictly_left_excludes_overlap)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;
	int			alo,
				ahi,
				blo,
				bhi;

	conn = connect_seg(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_seg(tc, &a, &alo, &ahi);
	draw_seg(tc, &b, &blo, &bhi);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN (%s) << (%s) "
					 "THEN NOT ((%s) && (%s)) AND NOT ((%s) << (%s)) "
					 "ELSE true END",
					 a.data, b.data,
					 a.data, b.data, b.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Point containment: a degenerate single-point seg built from a value v with
 * lo <= v <= hi is contained in S.  v is drawn in C within the drawn seg's
 * integer bounds, so the point lies inside the interval and S @> it.
 */
PG_PROP(contains_point_seg)
{
	pg_prop_conn *conn;
	StringInfoData s,
				q;
	int			lo,
				hi,
				v;

	conn = connect_seg(tc);

	initStringInfo(&s);
	draw_seg(tc, &s, &lo, &hi);

	v = (int) pg_prop_draw_int(tc, lo, hi);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s) @> (%d || ' .. ' || %d)::seg",
					 s.data, v, v);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment is consistent with the stored bounds: A @> B iff A's lower is
 * <= B's lower and A's upper is >= B's upper.  seg_lower/seg_upper are the
 * independent oracle for the @> containment operator.
 */
PG_PROP(contains_matches_bounds)
{
	pg_prop_conn *conn;
	StringInfoData a,
				b,
				q;
	int			alo,
				ahi,
				blo,
				bhi;

	conn = connect_seg(tc);

	initStringInfo(&a);
	initStringInfo(&b);
	draw_seg(tc, &a, &alo, &ahi);
	draw_seg(tc, &b, &blo, &bhi);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) @> (%s)) "
					 "= (seg_lower(%s) <= seg_lower(%s) "
					 "AND seg_upper(%s) >= seg_upper(%s))",
					 a.data, b.data,
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
