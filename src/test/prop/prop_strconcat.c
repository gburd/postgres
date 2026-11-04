/*-------------------------------------------------------------------------
 *
 * prop_strconcat.c
 *	  Property-based tests for string_agg / array_to_string / array_agg
 *	  consistency.
 *
 * These check that the three string-concatenation aggregates agree: a
 * comma-joined string_agg equals array_to_string(array_agg(...)), the
 * empty-delimiter string_agg has one character per single-digit row, and the
 * comma-joined string round-trips back through string_to_array to the ordered
 * input rows.  Each is a differential oracle (two independent ways must agree)
 * or an exact identity, so any disagreement is a bug in one of the aggregates.
 *
 * The table sc_t(id int PRIMARY KEY, v int) holds 0..15 rows with id = 1..n
 * (unique) and v a single decimal digit 0..9, so v::text is exactly one
 * character and carries no comma; the comma-join / string_to_array split
 * round-trips exactly and every aggregate ordered BY id is deterministic.
 * coalesce(...,0) / IS NOT DISTINCT FROM make the empty table (all aggregates
 * NULL) pass soundly.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_strconcat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "strconcat"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the aggregate errored or produced NULL where the property
 * expected a boolean, which is itself a bug -- so either is a failure,
 * reported with the SQL.
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
 * identical order.  Both callers carry a total ORDER BY.  (Copied from the
 * area spec skeleton.)
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
 * Build (and first drop) the table sc_t(id int PRIMARY KEY, v int) with n
 * rows, id = 1..n assigned explicitly and v a single decimal digit 0..9 so
 * that v::text is exactly one character and contains no comma.  ANALYZE after
 * load.  Fails the property if any DDL/DML errors.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn, int n)
{
	StringInfoData ddl;
	char	   *err = NULL;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS sc_t; "
						   "CREATE TABLE sc_t (id int PRIMARY KEY, v int); ");

	for (int i = 1; i <= n; i++)
		appendStringInfo(&ddl, "INSERT INTO sc_t VALUES (%d, %d); ",
						 i, (int) pg_prop_draw_int(tc, 0, 9));

	appendStringInfoString(&ddl, "ANALYZE sc_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s", err ? err : "(unknown)");

	pfree(ddl.data);
}

/* Drop the generated table. */
static void
drop_table(pg_prop_conn *conn)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS sc_t", &err) && err)
		pg_free(err);
}

/*
 * string_agg equals array_to_string(array_agg(...)): collecting ordered
 * values and joining them with ',' must give the same string whether the join
 * happens inside string_agg or afterward over array_agg.  Both are NULL on the
 * empty table, so compare with IS NOT DISTINCT FROM.
 */
PG_PROP(string_agg_eq_array_to_string)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 15);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT string_agg(v::text, ',' ORDER BY id) "
				 "IS NOT DISTINCT FROM "
				 "array_to_string(array_agg(v::text ORDER BY id), ',') "
				 "FROM sc_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * The empty-delimiter string_agg has exactly one character per row: each
 * v::text is a single digit and '' adds no separators, so the length equals
 * count(*).  coalesce(...,0) handles the empty table, where the aggregate is
 * NULL and count(*) is 0.
 */
PG_PROP(string_agg_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 15);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT coalesce(length(string_agg(v::text, '' ORDER BY id)), 0) "
				 "= count(*) FROM sc_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * The comma-joined string_agg round-trips back to rows: splitting it on ','
 * with ordinality recovers each v::text in id order.  Both queries carry a
 * total ORDER BY over the position (id in the reference, the split ordinality
 * in the round-trip), so require_same_rows compares the pieces position by
 * position -- a scrambled order would be caught, not masked.  On the empty
 * table both sides return zero rows (string_agg is NULL, unnest of NULL
 * yields nothing), so the comparison is sound.
 */
PG_PROP(string_agg_pieces)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 15);
	build_table(tc, conn, n);

	require_same_rows(tc, conn,
					  "SELECT v::text AS x FROM sc_t ORDER BY id",
					  "SELECT q.x FROM "
					  "(SELECT u.x, u.ord FROM "
					  "(SELECT string_agg(v::text, ',' ORDER BY id) AS sa "
					  "FROM sc_t) s, "
					  "unnest(string_to_array(s.sa, ',')) "
					  "WITH ORDINALITY AS u(x, ord)) q ORDER BY q.ord");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * The comma-joined string_agg has exactly (count(*) - 1) commas when
 * non-empty, and 0 when empty: each single-digit value contributes no comma,
 * and the join inserts one delimiter between adjacent values.  The comma count
 * is length - length(string with commas stripped).  coalesce(...,0) handles
 * the empty table (string_agg NULL), and greatest(count(*)-1, 0) handles both
 * the empty and single-row cases.
 */
PG_PROP(concat_ws_eq_string_agg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 15);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT coalesce("
				 "length(string_agg(v::text, ',')) "
				 "- length(replace(string_agg(v::text, ','), ',', '')), 0) "
				 "= greatest(count(*) - 1, 0) FROM sc_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * array_agg produces one element per row: cardinality(array_agg(v)) equals
 * count(*).  coalesce(...,0) handles the empty table, where the array is NULL.
 */
PG_PROP(array_agg_cardinality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 15);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT coalesce(cardinality(array_agg(v ORDER BY id)), 0) "
				 "= count(*) FROM sc_t");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * An ordered string_agg is deterministic: computing it twice over the same
 * table with the same ORDER BY must give the identical string.  Both NULL on
 * the empty table, so IS NOT DISTINCT FROM.
 */
PG_PROP(string_agg_order_deterministic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 15);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT (SELECT string_agg(v::text, ',' ORDER BY id) FROM sc_t) "
				 "IS NOT DISTINCT FROM "
				 "(SELECT string_agg(v::text, ',' ORDER BY id) FROM sc_t)");

	drop_table(conn);
	pg_prop_disconnect(conn);
}

/*
 * string_agg over no rows is SQL NULL: an aggregate with no input produces
 * NULL, so the WHERE false form must be NULL on every table.
 */
PG_PROP(empty_string_agg_null)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = (int) pg_prop_draw_int(tc, 0, 15);
	build_table(tc, conn, n);

	require_true(tc, conn,
				 "SELECT (SELECT string_agg(v::text, ',') FROM sc_t WHERE false) "
				 "IS NULL");

	drop_table(conn);
	pg_prop_disconnect(conn);
}
