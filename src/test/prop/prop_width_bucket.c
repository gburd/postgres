/*-------------------------------------------------------------------------
 *
 * prop_width_bucket.c
 *	  Property-based tests for the width_bucket() function.
 *
 * width_bucket(operand, low, high, count) assigns operand to one of count
 * equal-width buckets, returning 0 for below-low and count+1 for at-or-above-
 * high; recent fixes touched its boundary and NaN handling.  These check the
 * documented bucket semantics against an independent computation.
 *
 * The oracle is the documented definition itself, recomputed independently in
 * SQL: buckets are 1-based, the low bound is inclusive (bucket 1), the high
 * bound is exclusive (bucket count+1), anything strictly below low is 0, and
 * for an in-range operand the bucket is floor((op-low)/(high-low)*count)+1.
 * All arithmetic is done in numeric so the floor and division are exact and
 * no float rounding can mask or manufacture a discrepancy.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_width_bucket.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "width_bucket"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means width_bucket errored or produced NULL on finite inputs,
 * which is itself a bug -- so either is a failure, reported with the SQL.
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
 * Draw the three shared inputs for a width_bucket case: an integer operand in
 * [-50, 150], a pair of bounds ordered so low < high (bumping high when they
 * collide), and a count in [1, 20].  Integers keep the numeric arithmetic
 * exact and let the manual-formula oracle be checked without tolerance.
 */
static void
draw_inputs(pg_prop_tc *tc, int *operand, int *low, int *high, int *count)
{
	int			a;
	int			b;

	*operand = (int) pg_prop_draw_int(tc, -50, 150);

	a = (int) pg_prop_draw_int(tc, -50, 150);
	b = (int) pg_prop_draw_int(tc, -50, 150);
	if (a > b)
	{
		int			t = a;

		a = b;
		b = t;
	}
	if (a == b)
		b = b + 1;
	*low = a;
	*high = b;

	*count = (int) pg_prop_draw_int(tc, 1, 20);
}

/*
 * width_bucket always returns a value in [0, count+1]: a below-low operand is
 * clamped to 0, an at-or-above-high operand to count+1, everything else lands
 * in a numbered bucket.
 */
PG_PROP(result_in_range_0_to_count_plus_1)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			operand,
				low,
				high,
				count;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_inputs(tc, &operand, &low, &high, &count);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT width_bucket(%d::numeric, %d::numeric, "
					 "%d::numeric, %d) BETWEEN 0 AND %d + 1",
					 operand, low, high, count, count);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An operand strictly below low gets bucket 0.  low-1 < low, so it must fall
 * in the below-range overflow bucket.
 */
PG_PROP(below_low_is_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			operand,
				low,
				high,
				count;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_inputs(tc, &operand, &low, &high, &count);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT width_bucket((%d - 1)::numeric, %d::numeric, "
					 "%d::numeric, %d) = 0",
					 low, low, high, count);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An operand at or above high gets bucket count+1.  The high bound is
 * exclusive, so operand = high already falls in the overflow bucket, as does
 * high+1.
 */
PG_PROP(at_or_above_high_is_count_plus_1)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			operand,
				low,
				high,
				count;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_inputs(tc, &operand, &low, &high, &count);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT width_bucket(%d::numeric, %d::numeric, "
					 "%d::numeric, %d) = %d + 1 "
					 "AND width_bucket((%d + 1)::numeric, %d::numeric, "
					 "%d::numeric, %d) = %d + 1",
					 high, low, high, count, count,
					 high, low, high, count, count);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An operand exactly at low gets bucket 1: buckets are 1-based and the low
 * bound is inclusive.
 */
PG_PROP(at_low_is_bucket_1)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			operand,
				low,
				high,
				count;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_inputs(tc, &operand, &low, &high, &count);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT width_bucket(%d::numeric, %d::numeric, "
					 "%d::numeric, %d) = 1",
					 low, low, high, count);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * width_bucket is monotonic non-decreasing in the operand: for op1 <= op2,
 * bucket(op1) <= bucket(op2).  Two operands are drawn and ordered in C so the
 * precondition holds by construction.
 */
PG_PROP(monotonic_nondecreasing)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			operand,
				low,
				high,
				count;
	int			op1,
				op2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_inputs(tc, &operand, &low, &high, &count);

	op1 = (int) pg_prop_draw_int(tc, -50, 150);
	op2 = (int) pg_prop_draw_int(tc, -50, 150);
	if (op1 > op2)
	{
		int			t = op1;

		op1 = op2;
		op2 = t;
	}

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT width_bucket(%d::numeric, %d::numeric, "
					 "%d::numeric, %d) "
					 "<= width_bucket(%d::numeric, %d::numeric, "
					 "%d::numeric, %d)",
					 op1, low, high, count,
					 op2, low, high, count);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For an in-range operand (low <= op < high) the bucket equals the manual
 * definition floor((op-low)/(high-low)*count)+1.  The operand is drawn in
 * [low, high-1] so it is strictly in range, and the whole formula is computed
 * in numeric so the floor is exact and independent of width_bucket's own
 * arithmetic.
 */
PG_PROP(matches_manual_formula)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			operand,
				low,
				high,
				count;
	int			op;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_inputs(tc, &operand, &low, &high, &count);

	/* draw an operand strictly in range: low <= op <= high - 1 */
	op = (int) pg_prop_draw_int(tc, low, high - 1);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT width_bucket(%d::numeric, %d::numeric, "
					 "%d::numeric, %d) "
					 "= floor((%d::numeric - %d) * %d / (%d - %d)) + 1",
					 op, low, high, count,
					 op, low, count, high, low);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * width_bucket also supports descending bounds (high < low).  For an operand
 * strictly inside the open interval (low, high), the ascending and the
 * bound-swapped descending results mirror each other and sum to count+1:
 * width_bucket(op, low, high, count) + width_bucket(op, high, low, count)
 * = count + 1.  The operand is drawn in [low+1, high-1], which requires a span
 * of at least 2; draw_inputs can give span 1, so bounds are redrawn here with
 * a guaranteed span >= 2.
 */
PG_PROP(reverse_bounds_in_range)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			operand,
				low,
				high,
				count;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_inputs(tc, &operand, &low, &high, &count);
	if (high - low < 2)
		high = low + 2;

	/*
	 * width_bucket also accepts descending bounds (high < low).  The result
	 * stays in [0, count+1] for any operand, and an operand at or above the
	 * descending "low" (= the larger value) maps to bucket 0 while one at or
	 * below the descending "high" (= the smaller value) maps to count+1.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT width_bucket(%d::numeric, %d::numeric, "
					 "%d::numeric, %d) BETWEEN 0 AND %d + 1 "
					 "AND width_bucket((%d + 1)::numeric, %d::numeric, "
					 "%d::numeric, %d) = 0 "
					 "AND width_bucket((%d - 1)::numeric, %d::numeric, "
					 "%d::numeric, %d) = %d + 1",
					 operand, high, low, count, count,
					 high, high, low, count,
					 low, high, low, count, count);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
