/*-------------------------------------------------------------------------
 *
 * prop_rangeops.c
 *	  Property-based tests for int4range operator and function algebra.
 *
 * Target: the range type operator/function surface that the existing "range"
 * and "multirange" areas do not cover -- containment of bounds under '[)'
 * inclusivity, overlap (&&), adjacency (-|-), union (+), intersection (*),
 * difference (-), range_merge(), and lower()/upper() extraction.  The oracle
 * is the type's own algebra: every identity below is true for every input, so
 * an "f" result is a bug in the range code, not a modelling artefact.  Union
 * and difference are guarded with CASE because + over disjoint, non-adjacent
 * ranges raises an error rather than returning a value.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_rangeops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "rangeops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw an ordered pair lo <= hi in -10..10 and emit the int4range(lo, hi)
 * constructor text into buf (default '[)': lower inclusive, upper exclusive).
 * *lo and *hi receive the drawn bounds so a property can guard on lo < hi
 * (non-empty) versus lo == hi (empty).
 */
static void
draw_int4range(pg_prop_tc *tc, StringInfo buf, int *lo, int *hi)
{
	int			a = (int) pg_prop_draw_int(tc, -10, 10);
	int			b = (int) pg_prop_draw_int(tc, -10, 10);

	if (a > b)
	{
		int			t = a;

		a = b;
		b = t;
	}
	*lo = a;
	*hi = b;
	appendStringInfo(buf, "int4range((%d), (%d))", a, b);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the range algebra produced NULL or errored on inputs for
 * which the asserted identity should hold, which is itself a bug -- so either
 * is a failure, reported with the SQL.
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
 * contains_lower: under '[)' the lower bound is included.  For a non-empty
 * range (lo < hi), R @> lo.  When lo == hi the range is empty and contains
 * nothing, so we instead assert NOT (R @> lo): an empty range contains no
 * element.  Both halves are sound for every drawn pair.
 */
PG_PROP(contains_lower)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData r,
				q;
	int			lo,
				hi;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&r);
	draw_int4range(tc, &r, &lo, &hi);

	initStringInfo(&q);
	if (lo < hi)
		appendStringInfo(&q, "SELECT %s @> (%d)", r.data, lo);
	else
		appendStringInfo(&q, "SELECT NOT (%s @> (%d))", r.data, lo);

	require_true(tc, conn, q.data);

	pfree(r.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * excludes_upper: under '[)' the upper bound is excluded, so a non-empty range
 * does NOT contain hi: NOT (R @> hi).  When lo == hi the range is empty and
 * also contains nothing, so NOT (R @> hi) holds in that case too -- the
 * assertion is sound for every drawn pair without a guard.
 */
PG_PROP(excludes_upper)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData r,
				q;
	int			lo,
				hi;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&r);
	draw_int4range(tc, &r, &lo, &hi);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT NOT (%s @> (%d))", r.data, hi);

	require_true(tc, conn, q.data);

	pfree(r.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * intersection_contained: when A && B, their intersection A * B is contained
 * in each operand: (A * B) <@ A AND (A * B) <@ B.  Guarded with CASE on A && B
 * because * of non-overlapping ranges yields the empty range, which is <@
 * everything but is not the interesting case; the ELSE branch is trivially
 * true so the oracle is sound for every pair.
 */
PG_PROP(intersection_contained)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;
	int			lo,
				hi;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_int4range(tc, &a, &lo, &hi);
	draw_int4range(tc, &b, &lo, &hi);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN %s && %s "
					 "THEN (%s * %s) <@ %s AND (%s * %s) <@ %s "
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
 * union_defined_when_overlap_or_adjacent: + is defined exactly when the
 * operands overlap or are adjacent; the result then covers both operands.
 * CASE WHEN (A && B OR A -|- B) THEN (A + B) @> A AND (A + B) @> B ELSE true.
 * The guard is required because + over disjoint, non-adjacent ranges raises an
 * error; the ELSE branch keeps the oracle sound for those pairs.
 */
PG_PROP(union_defined_when_overlap_or_adjacent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;
	int			lo,
				hi;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_int4range(tc, &a, &lo, &hi);
	draw_int4range(tc, &b, &lo, &hi);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN (%s && %s OR %s -|- %s) "
					 "THEN (%s + %s) @> %s AND (%s + %s) @> %s "
					 "ELSE true END",
					 a.data, b.data, a.data, b.data,
					 a.data, b.data, a.data,
					 a.data, b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * difference_removes: when B @> A (B contains all of A), A - B is empty.
 * CASE WHEN B @> A THEN isempty(A - B) ELSE true END.  The guard confines the
 * assertion to the case where - is well-defined and the result is predictable
 * (A - B can error or split otherwise); the ELSE branch keeps it sound.
 */
PG_PROP(difference_removes)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;
	int			lo,
				hi;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_int4range(tc, &a, &lo, &hi);
	draw_int4range(tc, &b, &lo, &hi);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN %s @> %s "
					 "THEN isempty(%s - %s) ELSE true END",
					 b.data, a.data,
					 a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * merge_contains_both: range_merge(A, B) is the smallest range covering both
 * operands, so it always contains each: range_merge(A, B) @> A AND
 * range_merge(A, B) @> B.  range_merge is total (never errors on disjoint
 * ranges, unlike +), so no guard is needed.
 */
PG_PROP(merge_contains_both)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;
	int			lo,
				hi;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_int4range(tc, &a, &lo, &hi);
	draw_int4range(tc, &b, &lo, &hi);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT range_merge(%s, %s) @> %s "
					 "AND range_merge(%s, %s) @> %s",
					 a.data, b.data, a.data,
					 a.data, b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * elem_count_matches_width: for a non-empty '[)' range, the number of integers
 * it holds is upper(R) - lower(R), which must equal the drawn hi - lo.  This
 * is trivial but exercises the lower()/upper() bound extractors.  Guarded on
 * lo < hi because lower()/upper() of an empty range are NULL.
 */
PG_PROP(elem_count_matches_width)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData r,
				q;
	int			lo,
				hi;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&r);
	draw_int4range(tc, &r, &lo, &hi);

	initStringInfo(&q);
	if (lo < hi)
		appendStringInfo(&q,
						 "SELECT (upper(%s) - lower(%s)) = (%d)",
						 r.data, r.data, hi - lo);
	else
		appendStringInfoString(&q, "SELECT true");

	require_true(tc, conn, q.data);

	pfree(r.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * empty_when_lo_eq_hi: a '[)' range with equal bounds is empty.  For a drawn k,
 * isempty(int4range(k, k)).  This nails the degenerate boundary that the other
 * properties guard against.
 */
PG_PROP(empty_when_lo_eq_hi)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			k = (int) pg_prop_draw_int(tc, -10, 10);

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT isempty(int4range((%d), (%d)))", k, k);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
