/*-------------------------------------------------------------------------
 *
 * prop_srf.c
 *	  Property-based tests for set-returning functions in various positions.
 *
 * SRFs (unnest, generate_series, string_to_table, regexp_split_to_table,
 * jsonb_array_elements) must agree whether placed in the target list or in
 * FROM, must count/sum consistently, and must handle empty input.  A failure
 * means an SRF produced a different row set depending on its syntactic
 * position, or miscounted, which is an executor bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_srf.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "srf"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error on valid inputs is itself a bug, so either is a failure.
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
 * Differential result-set oracle: two queries must return identical rows in
 * identical order.  Both queries MUST carry a total ORDER BY so the row-major
 * string compares as a multiset.
 */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *q1, const char *q2)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *r1,
			   *r2;

	r1 = pg_prop_sql_rows(conn, q1, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "q1 failed: %s\n  SQL: %s", e1 ? e1 : "", q1);
	r2 = pg_prop_sql_rows(conn, q2, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "q2 failed: %s\n  SQL: %s", e2 ? e2 : "", q2);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc, "mismatch:\n  q1: %s\n  q2: %s\n  r1:\n%s\n  r2:\n%s",
					 q1, q2, r1, r2);
	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * Build an int[] literal of m elements drawn from [-50, 50], appending each
 * element to "arr" (an ARRAY[...] body) and accumulating the exact sum in C.
 * Returns the sum; "arr" receives the comma-separated element list (no
 * brackets).  m may be 0 (empty list).
 */
static int64
draw_int_array(pg_prop_tc *tc, StringInfo arr, int m)
{
	int64		sum = 0;
	int			i;

	for (i = 0; i < m; i++)
	{
		int64		v = pg_prop_draw_int(tc, -50, 50);

		if (i > 0)
			appendStringInfoString(arr, ", ");
		/* cast as (%d) so a negative literal is not misparsed */
		appendStringInfo(arr, "(%lld)", (long long) v);
		sum += v;
	}
	return sum;
}

/*
 * string_to_table('a,b,c', ',') in FROM must yield exactly the elements of
 * the equivalent string_to_array unnested in the target list.  Build a random
 * comma-joined word of a-c letters so the split is non-trivial.
 */
PG_PROP(string_to_table_eq_split_array)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char	   *s;
	size_t		slen;
	StringInfoData q1,
				q2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* a string of a-c and commas; comma is a legal alphabet member here */
	s = pg_prop_draw_from(tc, "abc,", 0, 20, &slen);

	initStringInfo(&q1);
	initStringInfo(&q2);
	appendStringInfo(&q1,
					 "SELECT x FROM string_to_table('%s', ',') x ORDER BY x",
					 s);
	appendStringInfo(&q2,
					 "SELECT unnest(string_to_array('%s', ',')) ORDER BY 1",
					 s);

	require_same_rows(tc, conn, q1.data, q2.data);

	pfree(q1.data);
	pfree(q2.data);
	free(s);
	pg_prop_disconnect(conn);
}

/*
 * regexp_split_to_table(s, '') in FROM splits a string into its characters;
 * it must match regexp_split_to_array(s, '') unnested in the target list.
 * Draw a random a-c string so the character split is non-trivial.
 */
PG_PROP(regexp_split_to_table_eq_array)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char	   *s;
	size_t		slen;
	StringInfoData q1,
				q2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	s = pg_prop_draw_from(tc, "abc", 0, 20, &slen);

	initStringInfo(&q1);
	initStringInfo(&q2);
	appendStringInfo(&q1,
					 "SELECT x FROM regexp_split_to_table('%s', '') x ORDER BY x",
					 s);
	appendStringInfo(&q2,
					 "SELECT unnest(regexp_split_to_array('%s', '')) ORDER BY 1",
					 s);

	require_same_rows(tc, conn, q1.data, q2.data);

	pfree(q1.data);
	pfree(q2.data);
	free(s);
	pg_prop_disconnect(conn);
}

/*
 * sum(unnest(ARRAY[...])) over m drawn ints equals the total computed in C.
 * The empty array sums to SQL NULL, so coalesce to 0 and compare to the C sum
 * (which is 0 for m = 0): the empty case stays sound.
 */
PG_PROP(unnest_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			m;
	int64		sum;
	StringInfoData arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	m = (int) pg_prop_draw_int(tc, 0, 40);
	initStringInfo(&arr);
	sum = draw_int_array(tc, &arr, m);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce((SELECT sum(x) FROM unnest(ARRAY[%s]::int[]) x), 0) = %lld",
					 arr.data, (long long) sum);

	require_true(tc, conn, q.data);

	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * generate_series in the target list and in FROM must yield identical rows.
 * Draw a small ordered span so the series is non-trivial but bounded.
 */
PG_PROP(generate_series_in_target_eq_from)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b,
				tmp;
	StringInfoData q1,
				q2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, -20, 20);
	b = pg_prop_draw_int(tc, -20, 20);
	if (a > b)
	{
		tmp = a;
		a = b;
		b = tmp;
	}

	initStringInfo(&q1);
	initStringInfo(&q2);
	appendStringInfo(&q1,
					 "SELECT g FROM generate_series(%lld, %lld) g ORDER BY g",
					 (long long) a, (long long) b);
	appendStringInfo(&q2,
					 "SELECT generate_series(%lld, %lld) ORDER BY 1",
					 (long long) a, (long long) b);

	require_same_rows(tc, conn, q1.data, q2.data);

	pfree(q1.data);
	pfree(q2.data);
	pg_prop_disconnect(conn);
}

/*
 * jsonb_array_elements over an m-element JSON array yields exactly m rows.
 * Build the array as [v, v, ...] of m drawn ints; the count is m regardless
 * of values (m = 0 gives an empty array and count 0).
 */
PG_PROP(json_array_elements_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			m,
				i;
	StringInfoData js,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	m = (int) pg_prop_draw_int(tc, 0, 40);
	initStringInfo(&js);
	appendStringInfoChar(&js, '[');
	for (i = 0; i < m; i++)
	{
		int64		v = pg_prop_draw_int(tc, -50, 50);

		if (i > 0)
			appendStringInfoString(&js, ", ");
		appendStringInfo(&js, "%lld", (long long) v);
	}
	appendStringInfoChar(&js, ']');

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM jsonb_array_elements('%s'::jsonb)) = %d",
					 js.data, m);

	require_true(tc, conn, q.data);

	pfree(js.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * unnest(...) WITH ORDINALITY numbers rows 1..n in emission order, so the sum
 * of the ordinal column is n*(n+1)/2.  Build an m-element array; the ordinal
 * sum is the Gauss total of m, computed in C and compared.  m = 0 gives an
 * empty set whose ordinal sum is NULL, so coalesce to 0 (which equals the C
 * total 0).
 */
PG_PROP(unnest_ordinality_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			m;
	int64		ordsum;
	StringInfoData arr,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	m = (int) pg_prop_draw_int(tc, 0, 40);
	initStringInfo(&arr);
	(void) draw_int_array(tc, &arr, m);
	ordsum = (int64) m * (m + 1) / 2;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce((SELECT sum(ord) FROM unnest(ARRAY[%s]::int[]) "
					 "WITH ORDINALITY u(x, ord)), 0) = %lld",
					 arr.data, (long long) ordsum);

	require_true(tc, conn, q.data);

	pfree(arr.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A CROSS JOIN of two generate_series emits exactly a*b rows: for a in 0..5
 * and b in 0..5, generate_series(1, a) has a rows and generate_series(1, b)
 * has b rows (an empty side gives 0).  count(*) must equal a*b.
 */
PG_PROP(srf_cross_join_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a,
				b;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, 0, 5);
	b = pg_prop_draw_int(tc, 0, 5);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM generate_series(1, %lld) ga "
					 "CROSS JOIN generate_series(1, %lld) gb) = %lld",
					 (long long) a, (long long) b, (long long) (a * b));

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * unnest of an empty int[] yields zero rows.  This is a fixed boundary case:
 * count(*) over unnest('{}'::int[]) must be 0.
 */
PG_PROP(empty_array_unnest_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM unnest('{}'::int[])) = 0");

	pg_prop_disconnect(conn);
}
