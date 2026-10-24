/*-------------------------------------------------------------------------
 *
 * prop_exceptintersect.c
 *	  Property-based tests for EXCEPT / INTERSECT set operations.
 *
 * The oracle is differential: EXCEPT (distinct) is a NULL-aware antijoin,
 * INTERSECT (distinct) a NULL-aware semijoin, and EXCEPT ALL is bag
 * difference max(count_a - count_b, 0).  Each set-op result is compared to an
 * equivalent join-based reconstruction over the same two tables; a mismatch
 * means the set-op executor (hashing or sorting, NULL grouping) produced
 * wrong rows, which is a bug.  All comparisons use IS NOT DISTINCT FROM so
 * NULL is treated as a single groupable value, matching set-op semantics.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_exceptintersect.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "exceptintersect"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Small value domain with boundary negatives; ~15% NULL. */
#define VAL_MIN (-4)
#define VAL_MAX 4

/* Draw an int in [VAL_MIN, VAL_MAX] (parenthesized if negative) or NULL. */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	int			v;

	if (pg_prop_draw_int(tc, 0, 6) == 0)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}
	v = (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX);
	if (v < 0)
		appendStringInfo(buf, "(%d)", v);
	else
		appendStringInfo(buf, "%d", v);
}

/*
 * Build ei_a(id int, v int) and ei_b(id int, v int), each with 0..20 random
 * rows, v in [-4,4] ~15% NULL.  Rebuilt fresh for every case.
 */
static void
build_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			na;
	int			nb;
	int			r;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ei_a; "
						   "DROP TABLE IF EXISTS ei_b; "
						   "CREATE TABLE ei_a (id int, v int); "
						   "CREATE TABLE ei_b (id int, v int); ");

	na = (int) pg_prop_draw_int(tc, 0, 20);
	for (r = 0; r < na; r++)
	{
		StringInfoData v;

		initStringInfo(&v);
		draw_val(tc, &v);
		appendStringInfo(&ddl, "INSERT INTO ei_a VALUES (%d, %s); ", r, v.data);
		pfree(v.data);
	}

	nb = (int) pg_prop_draw_int(tc, 0, 20);
	for (r = 0; r < nb; r++)
	{
		StringInfoData v;

		initStringInfo(&v);
		draw_val(tc, &v);
		appendStringInfo(&ddl, "INSERT INTO ei_b VALUES (%d, %s); ", r, v.data);
		pfree(v.data);
	}

	appendStringInfoString(&ddl, "ANALYZE ei_a; ANALYZE ei_b; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "", ddl.data);
	pfree(ddl.data);
}

/* Compare two result-set queries row-major; both must carry a total order. */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *q1, const char *q2)
{
	bool		f1 = false;
	bool		f2 = false;
	char	   *e1 = NULL;
	char	   *e2 = NULL;
	char	   *r1;
	char	   *r2;

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

/* Run a boolean SELECT and require "t"; NULL or error is a failure. */
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
	if (err)
		pg_free(err);
}

/*
 * EXCEPT equals a NULL-aware distinct antijoin: the values in A that have no
 * IS NOT DISTINCT FROM match in B, deduplicated.
 */
PG_PROP(except_eq_antijoin)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT v FROM "
					  "(SELECT v FROM ei_a EXCEPT SELECT v FROM ei_b) s "
					  "ORDER BY v NULLS FIRST",
					  "SELECT v FROM "
					  "(SELECT DISTINCT a.v FROM ei_a a WHERE NOT EXISTS "
					  "(SELECT 1 FROM ei_b b WHERE b.v IS NOT DISTINCT FROM a.v)) s "
					  "ORDER BY v NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * INTERSECT equals a NULL-aware distinct semijoin: the values in A that have
 * at least one IS NOT DISTINCT FROM match in B, deduplicated.
 */
PG_PROP(intersect_eq_semijoin)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT v FROM "
					  "(SELECT v FROM ei_a INTERSECT SELECT v FROM ei_b) s "
					  "ORDER BY v NULLS FIRST",
					  "SELECT v FROM "
					  "(SELECT DISTINCT a.v FROM ei_a a WHERE EXISTS "
					  "(SELECT 1 FROM ei_b b WHERE b.v IS NOT DISTINCT FROM a.v)) s "
					  "ORDER BY v NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * EXCEPT ALL is bag difference: the multiplicity of each value equals
 * max(count_a - count_b, 0).  Compare per-value counts of EXCEPT ALL against
 * a FULL JOIN of the grouped counts (NULL-grouped via IS NOT DISTINCT FROM).
 */
PG_PROP(except_all_minus)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT v, count(*) AS c FROM "
					  "(SELECT v FROM ei_a EXCEPT ALL SELECT v FROM ei_b) s "
					  "GROUP BY v ORDER BY v NULLS FIRST",
					  "SELECT v, c FROM ("
					  "SELECT dv.v AS v, greatest("
					  "(SELECT count(*) FROM ei_a a WHERE a.v IS NOT DISTINCT FROM dv.v) "
					  "- (SELECT count(*) FROM ei_b b WHERE b.v IS NOT DISTINCT FROM dv.v), "
					  "0) AS c "
					  "FROM (SELECT DISTINCT v FROM ei_a) dv) m "
					  "WHERE c > 0 ORDER BY v NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * INTERSECT commutes as a set operation: A INTERSECT B and B INTERSECT A
 * produce the same set of values.
 */
PG_PROP(intersect_commutes_as_sets)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT v FROM "
					  "(SELECT v FROM ei_a INTERSECT SELECT v FROM ei_b) s "
					  "ORDER BY v NULLS FIRST",
					  "SELECT v FROM "
					  "(SELECT v FROM ei_b INTERSECT SELECT v FROM ei_a) s "
					  "ORDER BY v NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * A EXCEPT A is empty: a set minus itself has no rows, even with NULLs and
 * duplicates present.  A non-zero count would be a set-op bug.
 */
PG_PROP(except_self_empty)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT count(*) = 0 FROM "
				 "(SELECT v FROM ei_a EXCEPT SELECT v FROM ei_a) s");

	pg_prop_disconnect(conn);
}
