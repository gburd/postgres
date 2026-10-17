/*-------------------------------------------------------------------------
 *
 * prop_jsonbmerge.c
 *	  Property-based tests for the jsonb concatenation/merge operator (||).
 *
 * These exercise the deep semantics of jsonb "||" that are distinct from the
 * containment, existence, and delete operators: object-key union with
 * right-hand precedence on shared keys, '{}' as a two-sided identity,
 * associativity over pairwise-disjoint objects, and the crucial SHALLOW rule
 * -- "||" replaces a shared nested value wholesale rather than deep-merging it.
 * It also covers array concatenation (append) and the object||array wrapping
 * rule.  The oracle is each behaviour's documented contract plus the key-count
 * identity for disjoint operands, so any "f" is a bug in the operator, not a
 * modelling slop on our side.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbmerge.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbmerge"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a jsonb object literal with n distinct keys named "k%d" starting at
 * index "base", with small int values, as text '{"k3": 7, "k4": 2}'::jsonb.
 * n is drawn 1..nmax.  By choosing non-overlapping [base, base+n) ranges for
 * two or three objects, the caller gets pairwise-disjoint key sets.  Values
 * are small ints 0..9 so the literal is trivially valid jsonb with no quoting
 * hazard.  Returns the number of keys emitted via *nout.
 */
static void
draw_object_from(pg_prop_tc *tc, StringInfo buf, int base, int nmax, int *nout)
{
	int			n = (int) pg_prop_draw_int(tc, 1, nmax);
	int			i;

	appendStringInfoString(buf, "'{");
	for (i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "\"k%d\": %d",
						 base + i, (int) pg_prop_draw_int(tc, 0, 9));
	}
	appendStringInfoString(buf, "}'::jsonb");
	*nout = n;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operator errored or produced NULL on a valid input,
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
 * concat_union_keys: (A || B) carries every key of A and every key of B.  A
 * and B are built with disjoint key ranges ([0, nA) and [100, 100+nB)), so k0
 * and k100 are always present, one from each side, and both must survive the
 * union.
 */
PG_PROP(concat_union_keys)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nA,
				nB;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_object_from(tc, &a, 0, 3, &nA);
	draw_object_from(tc, &b, 100, 3, &nB);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s || %s) ? 'k0' AND (%s || %s) ? 'k100'",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * concat_right_overrides: on a key shared by both operands the right-hand
 * value wins.  Build the two single-key objects with the same key but
 * different values and require the merged value to be the right one.
 */
PG_PROP(concat_right_overrides)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			lv = (int) pg_prop_draw_int(tc, 0, 9);
	int			rv = (int) pg_prop_draw_int(tc, 0, 9);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (jsonb_build_object('k', %d) || "
					 "jsonb_build_object('k', %d)) -> 'k' = '%d'::jsonb",
					 lv, rv, rv);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * concat_empty_identity: '{}'::jsonb is a two-sided identity for ||, so
 * A || '{}' = A and '{}' || A = A for any object A.
 */
PG_PROP(concat_empty_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nA;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_object_from(tc, &a, 0, 4, &nA);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s || '{}'::jsonb) = %s AND "
					 "('{}'::jsonb || %s) = %s",
					 a.data, a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * concat_associative_disjoint: for objects A, B, C with pairwise-disjoint key
 * sets ([0,nA), [100,100+nB), [200,200+nC)), (A||B)||C = A||(B||C).  Disjoint
 * keys avoid override order affecting the result, so the two groupings must be
 * byte-for-byte equal.
 */
PG_PROP(concat_associative_disjoint)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nA,
				nB,
				nC;
	StringInfoData a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	draw_object_from(tc, &a, 0, 3, &nA);
	draw_object_from(tc, &b, 100, 3, &nB);
	draw_object_from(tc, &c, 200, 3, &nC);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s || %s) || %s) = (%s || (%s || %s))",
					 a.data, b.data, c.data, a.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * concat_shallow_not_deep: || is SHALLOW -- a value shared under the same key
 * is replaced wholesale, never deep-merged.  Both operands have key "o" whose
 * value is a nested object with disjoint inner keys; the result's "o" must be
 * exactly the right operand's nested object, with no trace of the left inner
 * key.
 */
PG_PROP(concat_shallow_not_deep)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			la = (int) pg_prop_draw_int(tc, 0, 9);
	int			rb = (int) pg_prop_draw_int(tc, 0, 9);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('{\"o\": {\"a\": %d}}'::jsonb || "
					 "'{\"o\": {\"b\": %d}}'::jsonb) -> 'o' = "
					 "'{\"b\": %d}'::jsonb",
					 la, rb, rb);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_concat: concatenating two jsonb arrays appends element-wise, so
 * [a0..] || [b0..] equals the array with all left elements followed by all
 * right elements.  Built from small ints with no quoting hazard.
 */
PG_PROP(array_concat)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			x0 = (int) pg_prop_draw_int(tc, 0, 9);
	int			x1 = (int) pg_prop_draw_int(tc, 0, 9);
	int			y0 = (int) pg_prop_draw_int(tc, 0, 9);
	int			y1 = (int) pg_prop_draw_int(tc, 0, 9);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('[%d, %d]'::jsonb || '[%d, %d]'::jsonb) = "
					 "'[%d, %d, %d, %d]'::jsonb",
					 x0, x1, y0, y1, x0, x1, y0, y1);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * object_array_concat: concatenating an object with an array does not merge
 * key-wise; it wraps into an array.  We assert only the sound, documented
 * consequence -- the result's type is 'array' -- not the exact element layout.
 */
PG_PROP(object_array_concat)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nA;
	int			e0 = (int) pg_prop_draw_int(tc, 0, 9);
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_object_from(tc, &a, 0, 3, &nA);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_typeof(%s || '[%d]'::jsonb) = 'array'",
					 a.data, e0);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * concat_key_count: for disjoint objects A (nA keys) and B (nB keys), the
 * merged object has exactly nA + nB keys, since nothing is overwritten.
 * jsonb_object_keys returns one row per key, so count(*) must equal nA + nB.
 */
PG_PROP(concat_key_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nA,
				nB;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_object_from(tc, &a, 0, 4, &nA);
	draw_object_from(tc, &b, 100, 4, &nB);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM "
					 "jsonb_object_keys(%s || %s)) = %d",
					 a.data, b.data, nA + nB);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
