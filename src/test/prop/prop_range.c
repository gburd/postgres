/*-------------------------------------------------------------------------
 *
 * prop_range.c
 *	  Property-based tests for range types (int4range).
 *
 * Range types have had many correctness fixes over the years -- in bound
 * comparison, empty-range handling, and the canonicalization of discrete
 * ranges (int4range is canonicalized to a half-open [lo, hi) form).  These
 * properties check the operator algebra (@>, <@, &&, -|-, +, *) against set
 * semantics using int4range, whose canonical form makes equality exact: two
 * int4ranges are equal iff they denote the same set, so an "=" oracle needs
 * no tolerance.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_range.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "range"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small element domain keeps counterexamples legible and makes overlap and
 * adjacency common enough that the guarded-union property exercises its
 * interesting branch regularly.
 */
#define ELEM_MIN (-8)
#define ELEM_MAX 8

/*
 * Draw a random int4range literal as a SQL expression into buf.  Four shapes,
 * chosen at random:
 *
 *   - a normal bounded range int4range(lo, hi).  int4range(a, b) is the
 *     half-open set [a, b); when a >= b this yields the empty range, which is
 *     a legal and interesting input, so we do not force lo < hi.
 *   - an explicitly empty range 'empty'::int4range.
 *   - unbounded below: int4range(NULL, hi).
 *   - unbounded above: int4range(lo, NULL).
 *
 * Every shape is a valid construction, so a SQL error on any of them is a bug
 * the caller reports, not an input to discard.
 */
static void
draw_range(pg_prop_tc *tc, StringInfo buf)
{
	int			shape = (int) pg_prop_draw_int(tc, 0, 3);
	int			lo;
	int			hi;

	switch (shape)
	{
		case 0:					/* explicitly empty */
			appendStringInfoString(buf, "'empty'::int4range");
			break;
		case 1:					/* unbounded below */
			hi = (int) pg_prop_draw_int(tc, ELEM_MIN, ELEM_MAX);
			appendStringInfo(buf, "int4range(NULL, %d)", hi);
			break;
		case 2:					/* unbounded above */
			lo = (int) pg_prop_draw_int(tc, ELEM_MIN, ELEM_MAX);
			appendStringInfo(buf, "int4range(%d, NULL)", lo);
			break;
		default:				/* normal bounded; order the ends so lo <= hi
								 * (int4range(a, b) raises an error when a > b;
								 * a == b is the empty range, which is fine) */
			lo = (int) pg_prop_draw_int(tc, ELEM_MIN, ELEM_MAX);
			hi = (int) pg_prop_draw_int(tc, ELEM_MIN, ELEM_MAX);
			if (lo > hi)
			{
				int			tmp = lo;

				lo = hi;
				hi = tmp;
			}
			appendStringInfo(buf, "int4range(%d, %d)", lo, hi);
			break;
	}
}

/*
 * Draw an element value for membership tests, drawn one past each bound so
 * values just outside the range domain are exercised too.
 */
static int
draw_elem(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, ELEM_MIN - 1, ELEM_MAX + 1);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operator produced NULL or errored on a valid range
 * construction, which is itself a bug -- so either is a failure, reported with
 * the SQL that triggered it.
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
 * Union contains both operands.  The union (+) of two ranges errors if the
 * result "would not be contiguous", so the CASE restricts the real check to
 * the overlapping (&&) or adjacent (-|-) cases where the union is a single
 * range, and returns true otherwise.  Within that guard, (R1 + R2) must
 * contain (@>) each of R1 and R2.
 */
PG_PROP(union_contains_both)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData r1,
				r2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&r1);
	initStringInfo(&r2);
	draw_range(tc, &r1);
	draw_range(tc, &r2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN %s && %s OR %s -|- %s "
					 "THEN (%s + %s) @> %s AND (%s + %s) @> %s "
					 "ELSE true END",
					 r1.data, r2.data, r1.data, r2.data,
					 r1.data, r2.data, r1.data,
					 r1.data, r2.data, r2.data);

	require_true(tc, conn, q.data);

	pfree(r1.data);
	pfree(r2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Intersection is contained in both operands: (R1 * R2) <@ R1 AND <@ R2.
 * Intersection never errors (unlike union, it is always a single range), and
 * when the operands are disjoint the result is the empty range, which is
 * contained in everything -- so this holds for all drawn R1, R2.
 */
PG_PROP(intersection_contained_in_both)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData r1,
				r2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&r1);
	initStringInfo(&r2);
	draw_range(tc, &r1);
	draw_range(tc, &r2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s * %s) <@ %s AND (%s * %s) <@ %s",
					 r1.data, r2.data, r1.data,
					 r1.data, r2.data, r2.data);

	require_true(tc, conn, q.data);

	pfree(r1.data);
	pfree(r2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Element containment agrees with singleton-range containment: for a range R
 * and an element x, (R @> x) must equal (R @> int4range(x, x+1)).  The range
 * int4range(x, x+1) canonicalizes to the half-open [x, x+1), the singleton
 * set containing exactly x, so this cross-checks the scalar "@> element"
 * operator against the range "@> range" operator -- two code paths that must
 * agree.
 */
PG_PROP(membership_matches_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData r,
				q;
	int			x;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&r);
	draw_range(tc, &r);
	x = draw_elem(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s @> %d) = (%s @> int4range(%d, %d))",
					 r.data, x, r.data, x, x + 1);

	require_true(tc, conn, q.data);

	pfree(r.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The empty range is the bottom element and the identity for union / zero for
 * intersection: for any R, 'empty' <@ R, R + 'empty' = R, and
 * R * 'empty' = 'empty'.  (R + 'empty' never triggers the non-contiguous
 * error because adding the empty range leaves R unchanged.)
 */
PG_PROP(empty_is_identity_and_bottom)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData r,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&r);
	draw_range(tc, &r);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('empty'::int4range <@ %s) "
					 "AND (%s + 'empty'::int4range = %s) "
					 "AND (%s * 'empty'::int4range = 'empty'::int4range)",
					 r.data, r.data, r.data, r.data);

	require_true(tc, conn, q.data);

	pfree(r.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment is reflexive and antisymmetric: R1 @> R1 always, and mutual
 * containment (R1 @> R2 AND R2 @> R1) holds exactly when R1 = R2.  The
 * antisymmetry check relies on int4range's canonical equality: two ranges are
 * equal iff they denote the same set, so "=" is the right-hand oracle.
 */
PG_PROP(contains_is_reflexive_and_antisymmetric)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData r1,
				r2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&r1);
	initStringInfo(&r2);
	draw_range(tc, &r1);
	draw_range(tc, &r2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s @> %s) "
					 "AND ((%s @> %s AND %s @> %s) = (%s = %s))",
					 r1.data, r1.data,
					 r1.data, r2.data, r2.data, r1.data,
					 r1.data, r2.data);

	require_true(tc, conn, q.data);

	pfree(r1.data);
	pfree(r2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
