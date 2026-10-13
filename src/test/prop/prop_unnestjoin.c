/*-------------------------------------------------------------------------
 *
 * prop_unnestjoin.c
 *	  Property-based tests for unnest() in joins and WITH ORDINALITY.
 *
 * unnest() expands an array into a set, optionally WITH ORDINALITY, and that
 * set is then joined (self-joined on a shared element) or re-aggregated.  The
 * oracle is the type/function algebra of unnest: the number of produced rows
 * equals the summed array cardinality, re-aggregating an ordinality-tagged
 * unnest reconstructs the original array, ordinals span 1..cardinality, empty
 * arrays yield no rows, and the summed elements agree whether summed over the
 * flattened set or per-row.  Any "f" is a wrong-results bug in array
 * expansion, ordinality, or the join over the expanded set.
 *
 * This area is kept distinct from arrayops/srf/valuesrows/lateral: it targets
 * unnest specifically as a set-returning expansion feeding joins, re-aggregation
 * and ordinality join keys, not generic array operators or generic SRFs.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_unnestjoin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "unnestjoin"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Element domain for the tag arrays.  Non-NULL elements only (the spec
 * requires it): a NULL element in a join key would make a.t = b.t three-valued
 * and the count arithmetic unsound, so we never emit NULL array elements.
 */
#define TAG_MIN 0
#define TAG_MAX 5

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the expansion/ordinality/join produced NULL or errored on
 * valid input, which is itself a bug -- so either is a failure, reported with
 * the SQL.  (Copied from prop_numeric.c's require_true.)
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
 * Build un_t(id int PRIMARY KEY, tags int[]) fresh with 0..12 rows.  Each
 * row's tags is an array of 0..4 non-NULL ints drawn from [TAG_MIN, TAG_MAX].
 * ids are 0..nrows-1 so they are unique.  Rebuilt in each property so no state
 * leaks between cases.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 12);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS un_t; "
						   "CREATE TABLE un_t(id int PRIMARY KEY, tags int[]); ");

	for (int r = 0; r < nrows; r++)
	{
		int			ntags = (int) pg_prop_draw_int(tc, 0, 4);

		appendStringInfo(&ddl, "INSERT INTO un_t VALUES (%d, ARRAY[", r);
		for (int e = 0; e < ntags; e++)
		{
			if (e > 0)
				appendStringInfoString(&ddl, ", ");
			appendStringInfo(&ddl, "%d",
							 (int) pg_prop_draw_int(tc, TAG_MIN, TAG_MAX));
		}
		/* empty ARRAY[] needs a type annotation */
		if (ntags == 0)
			appendStringInfoString(&ddl, "]::int[]); ");
		else
			appendStringInfoString(&ddl, "]); ");
	}

	appendStringInfoString(&ddl, "ANALYZE un_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/*
 * Property 1: the number of rows produced by unnest(tags) over the table
 * equals the sum of each row's cardinality(tags).  This is the defining count
 * identity of unnest-as-expansion.  coalesce guards the empty-table case where
 * sum(...) is NULL.
 */
PG_PROP(unnest_count_eq_sum_cardinality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT id, unnest(tags) t FROM un_t) s) "
				 "= (SELECT coalesce(sum(cardinality(tags)), 0) FROM un_t)");

	pg_prop_disconnect(conn);
}

/*
 * Property 2: re-aggregating an ordinality-tagged unnest reconstructs the
 * original array.  array_agg(t ORDER BY ord) over unnest(tags) WITH ORDINALITY
 * must rebuild tags exactly.  array_agg over an empty unnest is NULL, so
 * coalesce(reagg, '{}'::int[]) = tags handles the empty-array row.  bool_and
 * over 0 rows is NULL, coalesced to true so the empty-table case passes.
 */
PG_PROP(unnest_reagg_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(coalesce(reagg, '{}'::int[]) = tags), true) "
				 "FROM (SELECT id, tags, "
				 "(SELECT array_agg(t ORDER BY ord) "
				 "FROM unnest(tags) WITH ORDINALITY u(t, ord)) reagg "
				 "FROM un_t) s");

	pg_prop_disconnect(conn);
}

/*
 * Property 3: a self-join of the unnested tags on the shared element produces
 * at least as many rows as there are rows with a non-empty tags array -- every
 * tag matches itself, so each (id, t) tuple joins at least to its own
 * counterpart, and each row with cardinality > 0 contributes at least one such
 * self-match.  This bounds the self-join over the expanded set without
 * depending on duplicate multiplicities, so it is sound for any contents.
 * coalesce guards the empty-table/empty-join NULLs.
 */
PG_PROP(unnest_selfjoin_tag_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT id, unnest(tags) t FROM un_t) a "
				 "JOIN (SELECT id, unnest(tags) t FROM un_t) b ON a.t = b.t) "
				 ">= (SELECT coalesce(count(*), 0) FROM un_t "
				 "WHERE cardinality(tags) > 0)");

	pg_prop_disconnect(conn);
}

/*
 * Property 4: in unnest(tags) WITH ORDINALITY, the ordinal ranges 1..
 * cardinality(tags) for every produced row.  bool_and over 0 rows (empty
 * table or all-empty arrays) is NULL, coalesced to true.
 */
PG_PROP(ordinality_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and(ord BETWEEN 1 AND cardinality(tags)), true) "
				 "FROM (SELECT tags, ord FROM un_t, "
				 "unnest(tags) WITH ORDINALITY AS u(t, ord)) s");

	pg_prop_disconnect(conn);
}

/*
 * Property 5: a row with an empty tags array contributes no unnest rows, so
 * the total count of unnest(tags) WITH ORDINALITY rows equals the summed
 * cardinality (empty arrays add 0).  Same identity as property 1 but driven
 * through the ORDINALITY form, which cross-checks that ORDINALITY does not
 * invent a row for an empty array.  coalesce guards the empty-table NULL.
 */
PG_PROP(empty_array_no_rows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT id FROM un_t, unnest(tags) WITH ORDINALITY) s) "
				 "= (SELECT coalesce(sum(cardinality(tags)), 0) FROM un_t)");

	pg_prop_disconnect(conn);
}

/*
 * Property 6: the sum of all elements across the flattened unnest set equals
 * the sum over rows of (sum of that row's tags).  Summing the expansion must
 * agree with summing per row -- a differential check that unnest neither drops
 * nor duplicates elements.  Elements are non-NULL so no NULL arithmetic.  Both
 * coalesce layers guard the empty-set/empty-table NULLs.
 */
PG_PROP(unnest_sum_eq_array_sum)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT coalesce(sum(t), 0) FROM "
				 "(SELECT unnest(tags) t FROM un_t) s) "
				 "= (SELECT coalesce(sum("
				 "(SELECT coalesce(sum(e), 0) FROM unnest(tags) e)), 0) "
				 "FROM un_t)");

	pg_prop_disconnect(conn);
}
