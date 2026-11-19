/*-------------------------------------------------------------------------
 *
 * prop_setopstrat.c
 *	  Property-based tests for set-operation strategy independence.
 *
 * UNION/INTERSECT/EXCEPT are specified by set semantics, not by the executor
 * strategy used to evaluate them.  PostgreSQL can implement a set operation
 * either by hashing (Setop/HashAggregate paths) or by sorting; which one it
 * picks is a cost decision gated by enable_hashagg.  The result rows must be
 * identical either way.  The oracle is differential: for each query we run it
 * once with enable_hashagg=on (hashed) and once with enable_hashagg=off
 * (sorted) and require identical rows.  Any disagreement is a bug in one of
 * the two set-operation implementations (or in NULL handling, since NULL is a
 * single distinct value for set-operation deduplication -- "not distinct").
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_setopstrat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "setopstrat"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small value domain: v in [-4, 4] with ~15% NULL, so the set operations
 * routinely hit duplicate values, empty intersections, and the NULL-as-a-
 * single-distinct-value case that distinguishes correct set-op dedup from
 * ordinary equality.
 */
#define VAL_MIN (-4)
#define VAL_MAX 4

/* Draw an integer in [VAL_MIN, VAL_MAX], or SQL NULL (~15% of the time). */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/*
 * Build so_a(id int, v int) and so_b(id int, v int), each with 0..20 random
 * rows.  Rebuilt fresh in every property so cases do not leak state.
 */
static void
build_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			na;
	int			nb;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS so_a; "
						   "DROP TABLE IF EXISTS so_b; "
						   "CREATE TABLE so_a (id int, v int); "
						   "CREATE TABLE so_b (id int, v int); ");

	na = (int) pg_prop_draw_int(tc, 0, 20);
	for (int i = 0; i < na; i++)
	{
		StringInfoData val;

		initStringInfo(&val);
		draw_val(tc, &val);
		appendStringInfo(&ddl, "INSERT INTO so_a VALUES (%d, %s); ", i, val.data);
		pfree(val.data);
	}

	nb = (int) pg_prop_draw_int(tc, 0, 20);
	for (int i = 0; i < nb; i++)
	{
		StringInfoData val;

		initStringInfo(&val);
		draw_val(tc, &val);
		appendStringInfo(&ddl, "INSERT INTO so_b VALUES (%d, %s); ", i, val.data);
		pfree(val.data);
	}

	appendStringInfoString(&ddl, "ANALYZE so_a; ANALYZE so_b; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);
}

/*
 * Run q with enable_hashagg=on (hashed set op) and again with
 * enable_hashagg=off (sorted set op); require identical rows.  RESET ALL
 * afterward so no planner GUC leaks into the next query or case.
 *
 * Each query already carries a TOTAL ORDER BY, so the row-major strings
 * compare as ordered multisets.
 */
static void
run_two(pg_prop_tc *tc, pg_prop_conn *conn, const char *q)
{
	char	   *err = NULL;
	bool		f1 = false;
	bool		f2 = false;
	char	   *e1 = NULL;
	char	   *e2 = NULL;
	char	   *r1;
	char	   *r2;

	if (!pg_prop_sql_exec(conn, "SET enable_hashagg = on", &err))
		pg_prop_fail(tc, "SET enable_hashagg=on failed: %s", err ? err : "");
	r1 = pg_prop_sql_rows(conn, q, &f1, &e1);

	if (!pg_prop_sql_exec(conn, "SET enable_hashagg = off", &err))
		pg_prop_fail(tc, "SET enable_hashagg=off failed: %s", err ? err : "");
	r2 = pg_prop_sql_rows(conn, q, &f2, &e2);

	pg_prop_sql_exec(conn, "RESET ALL", &err);

	if (f1)
		pg_prop_fail(tc, "hashed query failed: %s\n  SQL: %s", e1 ? e1 : "", q);
	if (f2)
		pg_prop_fail(tc, "sorted query failed: %s\n  SQL: %s", e2 ? e2 : "", q);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc,
					 "hashed != sorted set-op result\n  SQL: %s\n"
					 "  hashed (enable_hashagg=on):\n%s\n"
					 "  sorted (enable_hashagg=off):\n%s",
					 q, r1, r2);

	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * The distinct set operations (UNION/INTERSECT/EXCEPT) dedup to distinct
 * values of v, so v alone -- with NULLS FIRST pinning the single NULL value
 * -- is a total order over the result.
 */
PG_PROP(union_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	run_two(tc, conn,
			"(SELECT v FROM so_a UNION SELECT v FROM so_b) ORDER BY v NULLS FIRST");

	pg_prop_disconnect(conn);
}

PG_PROP(intersect_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	run_two(tc, conn,
			"(SELECT v FROM so_a INTERSECT SELECT v FROM so_b) ORDER BY v NULLS FIRST");

	pg_prop_disconnect(conn);
}

PG_PROP(except_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	run_two(tc, conn,
			"(SELECT v FROM so_a EXCEPT SELECT v FROM so_b) ORDER BY v NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * The ALL variants preserve multiplicity, so a value can repeat and v alone
 * is not a total order.  Grouping by v and emitting (v, count(*)) collapses
 * each value to one row, making (v) a total order again while still asserting
 * that both strategies produce the same multiset of values.
 */
PG_PROP(union_all_counts)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	run_two(tc, conn,
			"SELECT v, count(*) FROM "
			"(SELECT v FROM so_a UNION ALL SELECT v FROM so_b) s "
			"GROUP BY v ORDER BY v NULLS FIRST");

	pg_prop_disconnect(conn);
}

PG_PROP(intersect_all_counts)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	run_two(tc, conn,
			"SELECT v, count(*) FROM "
			"(SELECT v FROM so_a INTERSECT ALL SELECT v FROM so_b) s "
			"GROUP BY v ORDER BY v NULLS FIRST");

	pg_prop_disconnect(conn);
}

PG_PROP(except_all_counts)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);
	run_two(tc, conn,
			"SELECT v, count(*) FROM "
			"(SELECT v FROM so_a EXCEPT ALL SELECT v FROM so_b) s "
			"GROUP BY v ORDER BY v NULLS FIRST");

	pg_prop_disconnect(conn);
}
