/*-------------------------------------------------------------------------
 *
 * prop_rangeagg.c
 *	  Property-based tests for the range aggregates range_agg / range_merge
 *	  and multirange containment.
 *
 * range_agg folds a set of input ranges into one normalized multirange, and
 * range_merge collapses a multirange to its spanning range.  Both are set
 * operations whose results are exact: the aggregated multirange must contain
 * every member range (and every point inside a member), its merge span must
 * match min(lower)/max(upper) of the inputs, and merging can only reduce the
 * component count.  A failure of any of these is a bug in the aggregate merge
 * path, not a rounding artefact -- there is no slack to tolerate.
 *
 * This exercises range_agg / range_merge over a built table (distinct from
 * the pure-type algebra in prop_range / prop_multirange / prop_rangeops),
 * including the empty-input edge case where range_agg returns SQL NULL.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_rangeagg.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "rangeagg"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Integer bounds for lo/hi.  Kept small so the ranges overlap, abut and nest
 * densely, which is what makes range_agg actually merge components rather than
 * emit one component per row.
 */
#define VAL_MIN (-10)
#define VAL_MAX 10

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the aggregate errored or produced NULL on inputs where the
 * property guarded it with coalesce/CASE, which is itself a bug -- so either is
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
 * Build the rg_t table: id PRIMARY KEY, lo, hi with 0..15 rows and lo <= hi.
 * The lo <= hi ordering is enforced in C (swap when needed) so the generated
 * int4range(lo, hi, ...) literals never raise "range lower bound must be less
 * than or equal to range upper bound".  Rebuilt fresh in every property.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	char	   *err = NULL;
	StringInfoData ddl;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS rg_t; "
						   "CREATE TABLE rg_t(id int PRIMARY KEY, lo int, hi int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 15);
	for (int i = 0; i < nrows; i++)
	{
		int			a = (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX);
		int			b = (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX);
		int			lo = a < b ? a : b;
		int			hi = a < b ? b : a;

		appendStringInfo(&ddl,
						 "INSERT INTO rg_t VALUES (%d, (%d), (%d)); ",
						 i, lo, hi);
	}
	appendStringInfoString(&ddl, "ANALYZE rg_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  SQL: %s",
					 err ? err : "", ddl.data);

	pfree(ddl.data);
	if (err)
		pg_free(err);
}

/*
 * The aggregated multirange contains every member range: for each row r,
 * range_agg(all rows) @> int4range(r.lo, r.hi) must hold.  bool_and over all
 * rows, coalesced to true for the empty table (range_agg is NULL then and the
 * row-driven SELECT returns no rows).  The subselect names the aggregated
 * table "t" so each member is tested against the whole aggregate.
 */
PG_PROP(range_agg_covers)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and("
				 "(SELECT range_agg(int4range(t.lo, t.hi)) FROM rg_t t) "
				 "@> int4range(r.lo, r.hi)), true) FROM rg_t r");

	pg_prop_disconnect(conn);
}

/*
 * A point inside some input range is contained in the aggregated multirange.
 * Pick one row (the min id) and a value lo <= p <= hi inside it; the half-open
 * int4range(lo, hi) excludes hi, so test p = lo which is always a member.  The
 * CASE guards the empty table so the property passes soundly when there is no
 * row to pick.
 */
PG_PROP(multirange_contains_point)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	/*
	 * Use the inclusive '[]' aggregate so hi is a member too; pick p = lo of
	 * the lowest-id row.  int4range(lo, hi, '[]') with lo <= hi is never empty,
	 * so p = lo is always contained.
	 */
	require_true(tc, conn,
				 "SELECT CASE WHEN count(*) = 0 THEN true ELSE "
				 "(SELECT range_agg(int4range(lo, hi, '[]')) FROM rg_t) "
				 "@> (SELECT lo FROM rg_t ORDER BY id LIMIT 1) END "
				 "FROM rg_t");

	pg_prop_disconnect(conn);
}

/*
 * range_merge of the aggregated multirange is its spanning range, so its lower
 * bound equals min(lo) over the inputs.  Inclusive '[]' ranges are used so the
 * span's lower bound is exactly min(lo) (a half-open literal would normalize
 * the lower bound unchanged, but '[]' keeps it inclusive and unambiguous).
 * CASE guards the empty table.
 */
PG_PROP(range_merge_agg_bounds)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT CASE WHEN count(*) = 0 THEN true ELSE "
				 "lower(range_merge("
				 "(SELECT range_agg(int4range(lo, hi, '[]')) FROM rg_t t))) "
				 "= (SELECT min(lo) FROM rg_t) END "
				 "FROM rg_t");

	pg_prop_disconnect(conn);
}

/*
 * range_agg over an empty row set is SQL NULL.  WHERE false forces the empty
 * input regardless of how many rows the table holds; the result must be NULL.
 */
PG_PROP(multirange_is_empty_when_no_rows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT range_agg(int4range(t.lo, t.hi)) "
				 "FROM rg_t t WHERE false) IS NULL");

	pg_prop_disconnect(conn);
}

/*
 * unnest(range_agg(...)) yields the multirange's disjoint, non-adjacent
 * component ranges; merging can only reduce count, so the number of components
 * is at most the number of input rows.  coalesce guards the empty table where
 * range_agg is NULL and unnest yields no rows (count 0 <= 0 holds anyway).
 */
PG_PROP(unnest_multirange_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce("
				 "(SELECT count(*) FROM unnest("
				 "(SELECT range_agg(int4range(t.lo, t.hi)) FROM rg_t t))), 0) "
				 "<= (SELECT count(*) FROM rg_t)");

	pg_prop_disconnect(conn);
}

/*
 * Every integer in some input [lo, hi] (inclusive) is contained in the
 * inclusive '[]' aggregated multirange.  Draw an offset and clamp it to pick a
 * value v in [lo, hi] of the lowest-id row; the '[]' aggregate must contain v.
 * CASE guards the empty table.  The clamp (greatest/least against lo/hi) keeps
 * v inside the chosen row regardless of the drawn offset, so the assertion is
 * always true.
 */
PG_PROP(multirange_contains_all_elements)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			off = (int) pg_prop_draw_int(tc, 0, 2 * (VAL_MAX - VAL_MIN));

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	{
		StringInfoData q;

		initStringInfo(&q);
		appendStringInfo(&q,
						 "SELECT CASE WHEN count(*) = 0 THEN true ELSE "
						 "(SELECT range_agg(int4range(lo, hi, '[]')) FROM rg_t) @> "
						 "(SELECT least(greatest(lo + (%d), lo), hi) "
						 "FROM rg_t ORDER BY id LIMIT 1) END "
						 "FROM rg_t",
						 off);

		require_true(tc, conn, q.data);
		pfree(q.data);
	}

	pg_prop_disconnect(conn);
}
