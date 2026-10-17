/*-------------------------------------------------------------------------
 *
 * prop_jsonbarray.c
 *	  Property-based tests for jsonb array operations.
 *
 * jsonb arrays are an ordered sequence container with their own access path:
 * jsonb_array_length, the -> integer element operator (including negative
 * indices counting from the end), || concatenation, jsonb_array_elements set
 * expansion, and jsonb_agg reconstruction.  Each property builds a jsonb array
 * of 0..6 known integers in C (computing the sum and contents on the C side)
 * and pins the server's answer against that C-known oracle: length equals the
 * built count, J -> i equals the i-th int, concatenation lengths add, the
 * expanded element sum equals the C sum, and agg round-trips the array.  A
 * mismatch is a bug in the jsonb array code, not in the oracle.
 *
 * The oracle is the C-side known contents of each built array (length, each
 * element, their sum), checked with no tolerance; disagreement is a failure.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbarray.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbarray"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Element domain: small ints keep counterexamples legible. */
#define ELEM_MIN (-5)
#define ELEM_MAX 5
/* 0..6 elements per the area spec. */
#define MAX_ELEMS 6

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a jsonb array operation errored or disagreed with the
 * C-known oracle on a well-formed input, which is itself a bug -- so either is
 * a failure, reported with the SQL.
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
 * Draw a jsonb array of m integers (0..MAX_ELEMS), each in [ELEM_MIN,
 * ELEM_MAX].  Emits a cast literal ('[...]'::jsonb) into buf; stores each drawn
 * element into vals[] and the count into *m.  Sum is the caller's to compute
 * from vals[] so it stays C-known.  An empty array ('[]'::jsonb) is a valid
 * draw and exercises the m = 0 boundary.
 */
static void
draw_array(pg_prop_tc *tc, StringInfo buf, int vals[MAX_ELEMS], int *m)
{
	int			n = (int) pg_prop_draw_int(tc, 0, MAX_ELEMS);
	int			i;

	appendStringInfoString(buf, "('[");
	for (i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		vals[i] = (int) pg_prop_draw_int(tc, ELEM_MIN, ELEM_MAX);
		appendStringInfo(buf, "%d", vals[i]);
	}
	appendStringInfoString(buf, "]'::jsonb)");
	*m = n;
}

/* Sum of the first m entries of vals[], computed in C. */
static long long
sum_vals(const int vals[MAX_ELEMS], int m)
{
	long long	s = 0;
	int			i;

	for (i = 0; i < m; i++)
		s += vals[i];
	return s;
}

/*
 * length_matches: jsonb_array_length(A) = m for an array A built from m ints.
 */
PG_PROP(length_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			vals[MAX_ELEMS];
	int			m;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_array(tc, &a, vals, &m);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT jsonb_array_length(%s) = %d", a.data, m);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * element_access: A -> i = to_jsonb(the i-th int) for every i in 0..m-1.  The
 * integer -> element operator must return the element the C side put at i.
 * A single SELECT bool-ANDs all m comparisons (true for the empty array).
 */
PG_PROP(element_access)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			vals[MAX_ELEMS];
	int			m;
	int			i;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_array(tc, &a, vals, &m);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT true");
	for (i = 0; i < m; i++)
		appendStringInfo(&q, " AND (%s -> %d) = to_jsonb(%d)",
						 a.data, i, vals[i]);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * negative_index: A -> -1 = to_jsonb(last int) for a non-empty A.  Negative
 * indices count from the end; -1 is the last element.  Discard the empty draw,
 * for which -1 is out of range.
 */
PG_PROP(negative_index)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			vals[MAX_ELEMS];
	int			m;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_array(tc, &a, vals, &m);

	pg_prop_assume(tc, m > 0);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s -> -1) = to_jsonb(%d)",
					 a.data, vals[m - 1]);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * concat_length: jsonb_array_length(A || B) = jsonb_array_length(A) +
 * jsonb_array_length(B).  Concatenating two arrays yields an array whose length
 * is the sum of the operand lengths.
 */
PG_PROP(concat_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;
	int			avals[MAX_ELEMS];
	int			bvals[MAX_ELEMS];
	int			am;
	int			bm;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_array(tc, &a, avals, &am);
	draw_array(tc, &b, bvals, &bm);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT jsonb_array_length(%s || %s) "
					 "= jsonb_array_length(%s) + jsonb_array_length(%s)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * elements_sum: the sum of jsonb_array_elements(A) equals the C-computed sum
 * of the built ints.  coalesce(sum(...), 0) makes the empty array sum to 0,
 * matching the C sum of zero elements.
 */
PG_PROP(elements_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			vals[MAX_ELEMS];
	int			m;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_array(tc, &a, vals, &m);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT coalesce(sum(e::int), 0) "
					 "FROM jsonb_array_elements(%s) e) = %lld",
					 a.data, sum_vals(vals, m));

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * agg_roundtrip: aggregating jsonb_array_elements(A) back with jsonb_agg
 * reconstructs A.  jsonb_agg over 0 rows is NULL, so coalesce to '[]'::jsonb
 * for the empty array, which must then equal A itself.
 */
PG_PROP(agg_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			vals[MAX_ELEMS];
	int			m;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_array(tc, &a, vals, &m);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT coalesce(jsonb_agg(e), '[]'::jsonb) "
					 "FROM jsonb_array_elements(%s) e) = %s",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * oob_null: an index past either end of A yields SQL NULL.  A -> 1000 and
 * A -> -1000 are both out of range for any array of at most MAX_ELEMS
 * elements, so both must be NULL (true for the empty array too).
 */
PG_PROP(oob_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			vals[MAX_ELEMS];
	int			m;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_array(tc, &a, vals, &m);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s -> 1000) IS NULL AND (%s -> (-1000)) IS NULL",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * contains_element: a non-empty A @> to_jsonb(ARRAY[first int]) -- the jsonb
 * containment operator must report that A contains its own first element as a
 * single-element array.  Discard the empty draw, which has no first element.
 */
PG_PROP(contains_element)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			vals[MAX_ELEMS];
	int			m;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_array(tc, &a, vals, &m);

	pg_prop_assume(tc, m > 0);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT %s @> to_jsonb(ARRAY[%d])",
					 a.data, vals[0]);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
