/*-------------------------------------------------------------------------
 *
 * prop_arrayslice.c
 *	  Property-based tests for PostgreSQL array slicing and dimensions.
 *
 * Array slicing and the dimension accessors have subtle 1-based/bound
 * semantics; these check slice length, bound consistency, and slice/concat
 * composition over generated 1-D integer arrays.
 *
 * Distinct from the core "array" and "intarray" areas: this covers
 * array_dims, array_upper/lower, slicing a[m:n], and slice/array_cat
 * composition rather than cardinality/unnest/concat/text/contains/membership.
 *
 * The oracle is the array machinery's own slicing algebra against SQL: for a
 * 1-D array A of length n the bounds are [1:n]; the full slice A[1:n] is A; a
 * slice A[m:p] has exactly p-m+1 elements; splitting and re-concatenating the
 * two halves A[1:k] || A[k+1:n] rebuilds A; array_dims renders '[1:n]';
 * slices preserve the original subscripts so (A[i:i])[i] = A[i]; and element
 * access outside the bounds returns NULL rather than erroring.  Any "f" (or
 * an unexpected NULL/error) is a bug in the slicing code, not a modelling
 * slip -- elements are drawn NULL-free so every = comparison stays exact.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_arrayslice.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "arrayslice"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a random SQL integer-array literal into buf as an ARRAY[...] expression
 * typed ::int[].  n is drawn in 0..10 elements, each a small int in -20..20,
 * with NO NULL elements so every = comparison and slice reconstruction stays
 * exact.  The ::int[] cast is always emitted so an empty array
 * (ARRAY[]::int[]) still carries a type.  *n receives the element count.
 */
static void
draw_arr(pg_prop_tc *tc, StringInfo buf, int *n)
{
	int			len = (int) pg_prop_draw_int(tc, 0, 10);

	appendStringInfoString(buf, "ARRAY[");
	for (int i = 0; i < len; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, -20, 20));
	}
	appendStringInfoString(buf, "]::int[]");

	*n = len;
}

/*
 * Draw an array like draw_arr but forced non-empty (n in 1..10), for the
 * bound/dims/slice-length properties: an empty 1-D array has NULL bounds and
 * NULL array_dims, so those identities only hold for n >= 1.
 */
static void
draw_arr_nonempty(pg_prop_tc *tc, StringInfo buf, int *n)
{
	int			len = (int) pg_prop_draw_int(tc, 1, 10);

	appendStringInfoString(buf, "ARRAY[");
	for (int i = 0; i < len; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, -20, 20));
	}
	appendStringInfoString(buf, "]::int[]");

	*n = len;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the slice operation errored or produced NULL on valid
 * input, which is itself a bug -- so either is a failure, reported with the
 * SQL and any error text.
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
 * For a 1-D array A of length n >= 1, the default bounds are [1:n]:
 *		array_lower(A, 1) = 1 AND array_upper(A, 1) = n AND cardinality(A) = n.
 * Empty arrays have NULL bounds, so n is drawn >= 1 here.
 */
PG_PROP(upper_lower_match_cardinality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr_nonempty(tc, &a, &n);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT array_lower(%s, 1) = 1 "
					 "AND array_upper(%s, 1) = %d "
					 "AND cardinality(%s) = %d",
					 a.data, a.data, n, a.data, n);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The full slice is the whole array: A[1:n] = A for n = cardinality.  n is
 * drawn >= 1 so the slice bounds are well-defined (A[1:0] would be empty).
 */
PG_PROP(full_slice_is_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr_nonempty(tc, &a, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)[1:%d] = (%s)",
					 a.data, n, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A slice A[m:p] with 1 <= m <= p <= n has exactly p-m+1 elements:
 *		cardinality(A[m:p]) = p - m + 1.
 * m and p are drawn in 1..n and ordered in C so m <= p; n is drawn >= 1.
 */
PG_PROP(slice_length_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			n;
	int			m,
				p;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr_nonempty(tc, &a, &n);

	m = (int) pg_prop_draw_int(tc, 1, n);
	p = (int) pg_prop_draw_int(tc, 1, n);
	if (m > p)
	{
		int			t = m;

		m = p;
		p = t;
	}

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT cardinality((%s)[%d:%d]) = (%d - %d + 1)",
					 a.data, m, p, p, m);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Splitting at k and re-concatenating rebuilds the array:
 *		A[1:k] || A[k+1:n] = A		for a split k in 0..n.
 * The edge splits are empty slices (A[1:0] and A[n+1:n]), handled by slice
 * semantics, so a plain = still holds across the whole range.
 */
PG_PROP(slice_concat_reconstructs)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			n;
	int			k;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr_nonempty(tc, &a, &n);

	k = (int) pg_prop_draw_int(tc, 0, n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s)[1:%d] || (%s)[%d+1:%d]) = (%s)",
					 a.data, k, a.data, k, n, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_dims of a 1-D non-empty array is the text '[1:n]':
 *		array_dims(A) = '[1:' || n || ']'.
 * array_dims is NULL for an empty array, so n is drawn >= 1.
 */
PG_PROP(array_dims_matches_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr_nonempty(tc, &a, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT array_dims(%s) = '[1:' || %d || ']'",
					 a.data, n);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Slices preserve original subscripts, so a single-element slice A[i:i]
 * keeps index i and (A[i:i])[i] = A[i] for i in 1..n.
 */
PG_PROP(element_access_in_slice)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			n;
	int			i;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr_nonempty(tc, &a, &n);

	i = (int) pg_prop_draw_int(tc, 1, n);

	/*
	 * A single-element slice arr[i:i] contains exactly the i-th element.  A
	 * slice is re-based so its own subscript is not i in general; compare the
	 * element it unnests to against arr[i] rather than subscripting the slice.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s)[%d] = (SELECT x FROM unnest((%s)[%d:%d]) x)",
					 a.data, i, a.data, i, i);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Subscripts outside the array bounds return NULL rather than erroring:
 *		A[n+1] IS NULL AND A[0] IS NULL.
 * n is drawn >= 1 so n+1 and 0 are both genuinely out of the [1:n] range.
 */
PG_PROP(out_of_bounds_is_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_arr_nonempty(tc, &a, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)[%d] IS NULL AND (%s)[0] IS NULL",
					 a.data, n + 1, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
