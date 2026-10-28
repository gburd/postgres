/*-------------------------------------------------------------------------
 *
 * prop_arraynull.c
 *	  Property-based tests for arrays containing NULL elements and the
 *	  NULL-aware array operations.
 *
 * SQL arrays carry NULL as a first-class element value, distinct from an
 * empty array and from a NULL array.  The oracle here is the documented
 * NULL semantics of the array machinery: cardinality counts NULL elements,
 * array_remove(a, NULL) drops exactly the NULLs, array_position(a, NULL)
 * finds a NULL element, unnest yields one row per element (NULLs included),
 * array_to_string omits NULLs (or substitutes its third argument), coalesce
 * rewrites NULL elements in place, and ARRAY[NULL] is a one-element array
 * that is not the empty array.  Expected non-NULL counts are computed in C
 * from the known NULL positions, so a wrong count or a dropped/duplicated
 * NULL row is a bug in the array code, not an artefact of the test -- a
 * concern distinct from array/arrayops/arraycmp/arraysort.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_arraynull.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "arraynull"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw an int-array literal into buf as ARRAY[...]::int[], with 0..6 elements.
 * Each element is NULL with ~25% probability (drawing 0 out of 0..3), else an
 * integer from -5..5 emitted parenthesized as "(%d)" so a negative literal is
 * not misparsed when substituted into a larger expression.  The cast gives an
 * all-NULL or empty array a concrete element type.
 *
 * *total receives the element count and *nonnull the number of non-NULL
 * elements, so a property can compute the expected cardinality after NULL
 * removal in C rather than trusting a second SQL expression.
 */
static void
draw_intarr_null(pg_prop_tc *tc, StringInfo buf, int *total, int *nonnull)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 6);
	int			i;
	int			nn = 0;

	appendStringInfoString(buf, "ARRAY[");
	for (i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");
		if (pg_prop_draw_int(tc, 0, 3) == 0)
		{
			appendStringInfoString(buf, "NULL");
		}
		else
		{
			appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, -5, 5));
			nn++;
		}
	}
	appendStringInfoString(buf, "]::int[]");

	*total = n;
	*nonnull = nn;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the array operation errored or produced NULL on a
 * well-formed input, which is itself a bug -- so either is a failure, reported
 * with the SQL.  (Copied from prop_numeric.c's require_true.)
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
 * cardinality counts NULL elements: an array with known NULL positions has
 * cardinality equal to its total element count, NULLs included.  The expected
 * total is computed in C from the draw.
 */
PG_PROP(cardinality_counts_nulls)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			total,
				nonnull;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr_null(tc, &a, &total, &nonnull);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT cardinality(%s) = %d", a.data, total);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_remove(a, NULL) removes exactly the NULL elements: the cardinality of
 * the result equals the number of non-NULL elements, computed in C.  (NULLs
 * removed; non-NULLs preserved.)
 */
PG_PROP(array_remove_nulls)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			total,
				nonnull;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr_null(tc, &a, &total, &nonnull);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(array_remove(%s, NULL)) = %d",
					 a.data, nonnull);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_position finds a NULL element: for the fixed array ARRAY[1,NULL,2]
 * the first NULL is at position 2.  array_position accepts a NULL needle and
 * locates the NULL element rather than returning NULL or erroring.
 */
PG_PROP(contains_null_semantics)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT array_position(ARRAY[1,NULL,2]::int[], NULL) = 2");

	pg_prop_disconnect(conn);
}

/*
 * unnest yields one row per element, NULLs included: the number of rows from
 * unnest(a) equals cardinality(a).  Expressed as a scalar oracle so the empty
 * array yields count 0 (not a zero-row result): count(*) over the empty
 * unnest is 0, and cardinality of the empty array is 0.
 */
PG_PROP(unnest_preserves_nulls)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			total,
				nonnull;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr_null(tc, &a, &total, &nonnull);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT count(*) FROM unnest(%s) e) "
					 "= cardinality(%s)",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array_to_string omits NULL elements: for the fixed text array
 * ARRAY['a',NULL,'b'] joined on ',', the NULL is dropped and the result is
 * 'a,b'.
 */
PG_PROP(array_to_string_skips_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT array_to_string(ARRAY['a',NULL,'b']::text[], ',') "
				 "= 'a,b'");

	pg_prop_disconnect(conn);
}

/*
 * array_to_string with a third argument substitutes it for NULL elements
 * rather than omitting them: ARRAY['a',NULL,'b'] joined on ',' with null-text
 * 'X' is 'a,X,b'.
 */
PG_PROP(array_to_string_null_param)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT array_to_string(ARRAY['a',NULL,'b']::text[], ',', 'X') "
				 "= 'a,X,b'");

	pg_prop_disconnect(conn);
}

/*
 * coalesce rewrites each NULL element in place while preserving order and
 * length: array_agg(coalesce(e, -1) ORDER BY ord) over unnest WITH ORDINALITY
 * yields an array whose cardinality equals the input's and that contains no
 * NULLs.  The empty array makes array_agg return NULL, so coalesce the
 * cardinality to 0 and treat the no-element case as having zero NULLs.
 */
PG_PROP(coalesce_each)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;
	int			total,
				nonnull;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_intarr_null(tc, &a, &total, &nonnull);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT coalesce(cardinality(c), 0) = %d "
					 "AND coalesce(array_position(c, NULL) IS NULL, true) "
					 "FROM (SELECT array_agg(coalesce(e, -1) ORDER BY ord) c "
					 "FROM unnest(%s) WITH ORDINALITY u(e,ord)) s",
					 total, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An array holding one NULL is not the empty array: cardinality of
 * ARRAY[NULL]::int[] is 1 and cardinality of '{}'::int[] is 0.  (The two are
 * distinct values; ARRAY[NULL] is a one-element array.)
 */
PG_PROP(null_array_vs_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT cardinality(ARRAY[NULL]::int[]) = 1 "
				 "AND cardinality('{}'::int[]) = 0");

	pg_prop_disconnect(conn);
}
