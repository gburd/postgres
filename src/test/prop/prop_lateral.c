/*-------------------------------------------------------------------------
 *
 * prop_lateral.c
 *	  Property-based tests for LATERAL and set-returning-function joins.
 *
 * A LATERAL join (and an implicitly-lateral SRF-in-FROM, or an SRF in the
 * target list) always has an equivalent non-lateral rewriting: a correlated
 * aggregate equals a grouped join, LATERAL unnest equals a CROSS JOIN unnest,
 * a LATERAL top-1 equals a correlated scalar subquery, VALUES equals a UNION
 * ALL of singletons.  The planner is free to rewrite LATERAL into a nestloop
 * or a hashed form; if any rewrite changes the rows returned, that is a bug.
 *
 * The oracle is differential (require_same_rows): the lateral form and its
 * hand-written non-lateral equivalent must return identical row multisets.
 * Both sides carry a total ORDER BY so the row-major render compares exactly.
 * The comma-LATERAL "drop the outer row when the lateral is empty" semantics
 * are handled by using the aggregate-subquery / EXISTS-restricted forms where
 * needed, so the two sides are genuinely equal on every generated table.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_lateral.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "lateral"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small int domain for a,b: 0..4 with ~15% NULL.  Narrow enough that many
 * rows collide on "a" (so the correlated/grouped forms actually have groups
 * with several members) and that generate_series(1, greatest(a,0)) stays tiny.
 */
#define VAL_MIN 0
#define VAL_MAX 4

/* Append an int in [VAL_MIN,VAL_MAX], or NULL ~15% of the time. */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/*
 * Rebuild lt_t(id int PRIMARY KEY, a int, b int) fresh with 0..20 rows.  id is
 * a dense 0..n-1 sequence so it is a unique total-order key; a,b are drawn by
 * draw_val.  pg_prop_fail on any DDL/DML error (the schema is fixed and legal,
 * so a failure here is a harness/server problem, not a discardable case).
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS lt_t; "
						   "CREATE TABLE lt_t (id int PRIMARY KEY, a int, b int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 20);
	for (int r = 0; r < nrows; r++)
	{
		appendStringInfo(&ddl, "INSERT INTO lt_t VALUES (%d, ", r);
		draw_val(tc, &ddl);
		appendStringInfoString(&ddl, ", ");
		draw_val(tc, &ddl);
		appendStringInfoString(&ddl, "); ");
	}
	appendStringInfoString(&ddl, "ANALYZE lt_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);
}

/*
 * Differential oracle: q1 and q2 must return identical row multisets (both
 * carry a total ORDER BY, so the row-major strings compare directly).  A SQL
 * error on either side, or a mismatch, is a failure printed with both queries.
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
 * Scalar boolean oracle (shape A): run a "SELECT <bool>" and require "t".  A
 * NULL result or a SQL error is a failure.
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
}

/*
 * Property 1: a correlated LATERAL aggregate equals a grouped left join.
 *
 * A comma-LATERAL with a correlated aggregate ("SELECT max(u.b) ... WHERE
 * u.a = t.a") always produces exactly one row per outer row -- max over an
 * empty set is SQL NULL, but it is still one row -- so the outer row is never
 * dropped.  The grouped form "GROUP BY a" with a LEFT JOIN therefore matches
 * it row for row (unmatched outer rows get a NULL m from the LEFT JOIN).
 */
PG_PROP(lateral_subquery_eq_join)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT t.id, s.m FROM lt_t t, "
					  "LATERAL (SELECT max(u.b) m FROM lt_t u WHERE u.a = t.a) s "
					  "ORDER BY t.id",
					  "SELECT t.id, mx.m FROM lt_t t "
					  "LEFT JOIN (SELECT a, max(b) m FROM lt_t GROUP BY a) mx "
					  "ON mx.a = t.a ORDER BY t.id");

	pg_prop_disconnect(conn);
}

/*
 * Property 2: LATERAL unnest equals CROSS JOIN unnest.  An SRF in FROM is
 * implicitly lateral, so both expand ARRAY[t.a, t.b] per outer row into the
 * same two rows.  NULLs in the array are preserved; order by t.id then e with
 * NULLS LAST so the total order is deterministic on both sides.
 */
PG_PROP(lateral_unnest_eq_unnest)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT t.id, e FROM lt_t t, "
					  "LATERAL unnest(ARRAY[t.a, t.b]) e "
					  "ORDER BY t.id, e NULLS LAST",
					  "SELECT t.id, e FROM lt_t t "
					  "CROSS JOIN unnest(ARRAY[t.a, t.b]) e "
					  "ORDER BY t.id, e NULLS LAST");

	pg_prop_disconnect(conn);
}

/*
 * Property 3: a LATERAL top-1 equals a correlated scalar subquery.
 *
 * Comma (CROSS) LATERAL drops the outer row when the lateral yields no rows,
 * so q1 keeps only rows whose "a" has at least one match.  The scalar-subquery
 * form keeps every outer row (with NULL where there is no match), so q2 is
 * restricted to EXISTS matches to make the two sides equal.  The inner
 * ORDER BY u.b, u.id is a total order (id is unique), so LIMIT 1 is
 * deterministic.
 */
PG_PROP(lateral_limit_topn)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT t.id, s.b FROM lt_t t, "
					  "LATERAL (SELECT u.b FROM lt_t u WHERE u.a = t.a "
					  "ORDER BY u.b, u.id LIMIT 1) s ORDER BY t.id",
					  "SELECT t.id, (SELECT u.b FROM lt_t u WHERE u.a = t.a "
					  "ORDER BY u.b, u.id LIMIT 1) FROM lt_t t "
					  "WHERE EXISTS (SELECT 1 FROM lt_t u WHERE u.a = t.a) "
					  "ORDER BY t.id");

	pg_prop_disconnect(conn);
}

/*
 * Property 4 (shape A): the count of rows produced by
 * (t CROSS JOIN LATERAL generate_series(1, greatest(a,0))) equals the sum
 * over rows of greatest(a,0).  generate_series(1, k) yields k rows for k >= 1
 * and 0 rows for k <= 0, so a row contributes greatest(a,0) rows; NULL a is
 * coalesced to 0.  Both sides are coalesced so the empty-table case is 0 = 0.
 */
PG_PROP(cross_join_lateral_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM lt_t t, "
				 "LATERAL generate_series(1, greatest(coalesce(t.a,0),0)) g) "
				 "= (SELECT coalesce(sum(greatest(coalesce(a,0),0)),0) FROM lt_t)");

	pg_prop_disconnect(conn);
}

/*
 * Property 5: a VALUES scan equals a UNION ALL of singletons.  No table is
 * needed; both forms produce the same three one-column rows and are given the
 * same total order.
 */
PG_PROP(values_scan_eq_union)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_same_rows(tc, conn,
					  "SELECT * FROM (VALUES (1),(2),(3)) v(x) ORDER BY x",
					  "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 ORDER BY 1");

	pg_prop_disconnect(conn);
}

/*
 * Property 6: an SRF in the target list equals a LATERAL SRF in FROM.  Under
 * modern (9.6+) semantics an SRF in the SELECT list expands exactly like a
 * LATERAL function call, so both yield the same (id, g) rows; a total order on
 * (id, g) makes the comparison deterministic.
 */
PG_PROP(srf_in_select_eq_lateral)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id, generate_series(1, greatest(coalesce(a,0),0)) AS g "
					  "FROM lt_t ORDER BY id, g",
					  "SELECT t.id, g FROM lt_t t, "
					  "LATERAL generate_series(1, greatest(coalesce(t.a,0),0)) g "
					  "ORDER BY t.id, g");

	pg_prop_disconnect(conn);
}
