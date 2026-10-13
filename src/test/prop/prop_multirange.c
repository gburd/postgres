/*-------------------------------------------------------------------------
 *
 * prop_multirange.c
 *	  Property-based tests for multirange types (int4multirange).
 *
 * Multiranges have accumulated many correctness fixes over the years in the
 * code that merges, normalizes, and combines them (union, intersection,
 * minus).  The defining invariant of the type is strong: a multirange is a
 * normalized, ordered set of non-overlapping, non-adjacent ranges.  Because
 * that canonical form is a true set, the set identities over int4multirange
 * are exact -- union commutes and is idempotent, intersection is contained in
 * both operands, difference removes exactly the overlap -- and so set algebra
 * is the natural property to assert against the type itself.
 *
 * The oracle here is the type's own set algebra.  The server normalizes and
 * merges on input, so overlapping or adjacent component ranges in a literal
 * get folded together; feeding deliberately overlapping components therefore
 * exercises the merge path, and the identities must still hold with no slack.
 * Unlike single ranges, multirange + * - never raise a "non-contiguous"
 * error -- representing a gap is the whole point of the type -- so these need
 * no CASE guards and are cleaner to state than their single-range cousins.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_multirange.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "multirange"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small integer bounds keep each generated multirange legible in a
 * counterexample and keep the overlap/adjacency interactions dense: with
 * bounds in [-8, 8] and up to three components, drawn ranges routinely
 * overlap, abut, and nest, which is exactly what the input normalizer and the
 * set operators must fold correctly.
 */
#define BND_MIN (-8)
#define BND_MAX 8

/*
 * Draw a random int4multirange literal into buf, e.g.
 *		'{[1,5), [8,10)}'::int4multirange
 * or the empty multirange '{}'::int4multirange.
 *
 * Draw 0..3 component ranges with small bounds.  A component is normally the
 * half-open range [lo, hi) with lo < hi, but occasionally an explicitly empty
 * component -- which the server drops on normalization, exercising that path.
 * Components are NOT forced disjoint: overlapping and adjacent inputs are
 * legal and get merged, so this deliberately feeds the merge logic.
 */
static void
draw_mr(pg_prop_tc *tc, StringInfo buf)
{
	int			ncomp = (int) pg_prop_draw_int(tc, 0, 3);
	bool		first = true;

	appendStringInfoChar(buf, '\'');
	appendStringInfoChar(buf, '{');
	for (int i = 0; i < ncomp; i++)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		/* ~1 in 5 components is an empty range, dropped on normalization. */
		if (pg_prop_draw_int(tc, 0, 4) == 0)
		{
			appendStringInfoString(buf, "empty");
		}
		else
		{
			int			lo = (int) pg_prop_draw_int(tc, BND_MIN, BND_MAX - 1);
			int			hi = (int) pg_prop_draw_int(tc, lo + 1, BND_MAX);

			appendStringInfo(buf, "[%d,%d)", lo, hi);
		}
	}
	appendStringInfoChar(buf, '}');
	appendStringInfoChar(buf, '\'');
	appendStringInfoString(buf, "::int4multirange");
}

/*
 * Draw a random int4range literal into buf, e.g. '[1,5)'::int4range or
 * 'empty'::int4range.  Used by the range-to-multirange roundtrip test.
 */
static void
draw_r(pg_prop_tc *tc, StringInfo buf)
{
	appendStringInfoChar(buf, '\'');
	if (pg_prop_draw_int(tc, 0, 5) == 0)
	{
		appendStringInfoString(buf, "empty");
	}
	else
	{
		int			lo = (int) pg_prop_draw_int(tc, BND_MIN, BND_MAX - 1);
		int			hi = (int) pg_prop_draw_int(tc, lo + 1, BND_MAX);

		appendStringInfo(buf, "[%d,%d)", lo, hi);
	}
	appendStringInfoChar(buf, '\'');
	appendStringInfoString(buf, "::int4range");
}

/*
 * Draw a scalar int4 element into buf, in a range a little wider than the
 * component bounds so it falls both inside and outside drawn ranges.
 */
static void
draw_elem(pg_prop_tc *tc, StringInfo buf)
{
	appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, BND_MIN - 2, BND_MAX + 2));
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a set operator errored or produced NULL on valid finite
 * input, which is itself a bug -- so either is a failure, reported with the
 * SQL that triggered it.
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
 * Union commutes and is idempotent: (M1 + M2 = M2 + M1) AND (M1 + M1 = M1).
 * Both follow from the canonical set form; a merge/ordering bug would break
 * commutativity, and a failure to collapse equal operands would break
 * idempotence.
 */
PG_PROP(union_is_commutative_and_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData m1,
				m2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&m1);
	initStringInfo(&m2);
	draw_mr(tc, &m1);
	draw_mr(tc, &m2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s + %s = %s + %s) AND (%s + %s = %s)",
					 m1.data, m2.data, m2.data, m1.data,
					 m1.data, m1.data, m1.data);

	require_true(tc, conn, q.data);

	pfree(m1.data);
	pfree(m2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Intersection is contained in both operands: (M1 * M2) <@ M1 AND (M1 * M2)
 * <@ M2.  Multirange intersection never errors (unlike single-range union),
 * so there is nothing to guard.
 */
PG_PROP(intersection_subset_of_both)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData m1,
				m2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&m1);
	initStringInfo(&m2);
	draw_mr(tc, &m1);
	draw_mr(tc, &m2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s * %s) <@ %s AND (%s * %s) <@ %s",
					 m1.data, m2.data, m1.data,
					 m1.data, m2.data, m2.data);

	require_true(tc, conn, q.data);

	pfree(m1.data);
	pfree(m2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Union is always defined and contains both operands: (M1 + M2) @> M1 AND
 * (M1 + M2) @> M2.  Single-range union errors when the result would be
 * non-contiguous; multirange union never does, which is the point of the
 * type.
 */
PG_PROP(union_contains_both)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData m1,
				m2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&m1);
	initStringInfo(&m2);
	draw_mr(tc, &m1);
	draw_mr(tc, &m2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s + %s) @> %s AND (%s + %s) @> %s",
					 m1.data, m2.data, m1.data,
					 m1.data, m2.data, m2.data);

	require_true(tc, conn, q.data);

	pfree(m1.data);
	pfree(m2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Difference removes exactly the overlap: NOT ((M1 - M2) && M2).  After
 * subtracting M2 from M1, the result shares no element with M2, so it does
 * not overlap M2.  A merge/split bug in minus would leave a sliver that still
 * overlaps.
 */
PG_PROP(minus_then_intersection_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData m1,
				m2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&m1);
	initStringInfo(&m2);
	draw_mr(tc, &m1);
	draw_mr(tc, &m2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT NOT ((%s - %s) && %s)",
					 m1.data, m2.data, m2.data);

	require_true(tc, conn, q.data);

	pfree(m1.data);
	pfree(m2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A counting-free inclusion/exclusion identity: ((M1 + M2) - M2) <@ M1 AND
 * M1 <@ (M1 + M2).  Removing M2 from the union leaves only elements that were
 * in M1, and M1 is always a subset of the union.  This ties union and
 * difference together without appealing to cardinality (which multiranges do
 * not expose directly).
 */
PG_PROP(inclusion_exclusion_cardinality_free)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData m1,
				m2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&m1);
	initStringInfo(&m2);
	draw_mr(tc, &m1);
	draw_mr(tc, &m2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s + %s) - %s) <@ %s AND %s <@ (%s + %s)",
					 m1.data, m2.data, m2.data, m1.data,
					 m1.data, m1.data, m2.data);

	require_true(tc, conn, q.data);

	pfree(m1.data);
	pfree(m2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A single range, lifted into a one-element multirange, contains exactly the
 * same elements: (R @> x) = (multirange(R) @> x) for a drawn element x.
 * multirange(anyrange) wraps one range as a multirange; the containment of
 * any point must be preserved by the lift.
 */
PG_PROP(range_roundtrip_into_multirange)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData r,
				x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&r);
	initStringInfo(&x);
	draw_r(tc, &r);
	draw_elem(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s @> %s) = (multirange(%s) @> %s)",
					 r.data, x.data, r.data, x.data);

	require_true(tc, conn, q.data);

	pfree(r.data);
	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
