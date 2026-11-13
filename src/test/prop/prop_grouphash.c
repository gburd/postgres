/*-------------------------------------------------------------------------
 *
 * prop_grouphash.c
 *	  Property-based tests for multi-column GROUP BY / DISTINCT strategy
 *	  independence.
 *
 * The oracle is differential: a grouping query that keys on several columns
 * (or several grouping expressions) must return identical rows whether the
 * executor hashes the groups (enable_hashagg = on) or sorts them
 * (enable_hashagg = off).  This targets the multi-key grouping paths
 * specifically -- composite hash keys, NULL groups across several keys,
 * DISTINCT over multiple columns, and count(DISTINCT) under grouping -- which
 * is distinct from prop_aggstrategies (single-key strategy independence) and
 * prop_grouphaving (equivalent SQL rewrites).  Any disagreement between the
 * two strategies is a wrong-results bug in multi-key grouping, not a
 * performance difference.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_grouphash.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "grouphash"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small domains keep each generated table legible in a counterexample and
 * keep cases fast while still covering the multi-key grouping boundaries: the
 * grouping keys a and b in 0..2 with ~15% NULL (so composite NULL groups and
 * mixed NULL/non-NULL keys are exercised on both strategies), and v small so
 * sum(v) and the (v>0) predicate have content without overflow concerns.
 */
#define K_MIN 0
#define K_MAX 2
#define V_MIN (-3)
#define V_MAX 3

/* Draw a grouping key: SQL NULL ~15% of the time, else an int in [K_MIN,K_MAX]. */
static void
draw_key(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, K_MIN, K_MAX));
}

/* Draw v: SQL NULL ~15% of the time, else an int in [V_MIN, V_MAX]. */
static void
draw_v(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, V_MIN, V_MAX));
}

/*
 * (Re)build gx_t(id int PRIMARY KEY, a int, b int, v int) fresh with 0..30
 * random rows.  a and b are grouping keys (0..2, ~15% NULL); v is small
 * (~15% NULL).  Rebuilt in each property so cases are independent.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS gx_t; "
						   "CREATE TABLE gx_t (id int PRIMARY KEY, a int, b int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int r = 0; r < nrows; r++)
	{
		StringInfoData aval;
		StringInfoData bval;
		StringInfoData vval;

		initStringInfo(&aval);
		initStringInfo(&bval);
		initStringInfo(&vval);
		draw_key(tc, &aval);
		draw_key(tc, &bval);
		draw_v(tc, &vval);
		appendStringInfo(&ddl, "INSERT INTO gx_t VALUES (%d, %s, %s, %s); ",
						 r, aval.data, bval.data, vval.data);
		pfree(aval.data);
		pfree(bval.data);
		pfree(vval.data);
	}
	appendStringInfoString(&ddl, "ANALYZE gx_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * Differential oracle: run the SAME query twice -- once forcing hash
 * aggregation (enable_hashagg = on) and once forcing sorted/grouped
 * aggregation (enable_hashagg = off) -- and require identical rows.  RESET ALL
 * after.  The query must already carry a TOTAL ORDER BY on all grouping keys
 * with NULLS FIRST; because the grouping keys are distinct per group that
 * makes the ordering total, so the row-major string compares as a multiset.
 */
static void
run_two(pg_prop_tc *tc, pg_prop_conn *conn, const char *query)
{
	char	   *err = NULL;
	bool		f1 = false,
				f2 = false;
	char	   *r1,
			   *r2;

	if (!pg_prop_sql_exec(conn, "SET enable_hashagg = on", &err))
		pg_prop_fail(tc, "SET enable_hashagg=on failed: %s", err ? err : "");
	r1 = pg_prop_sql_rows(conn, query, &f1, &err);
	if (f1)
		pg_prop_fail(tc, "hashagg query failed: %s\n  SQL: %s",
					 err ? err : "", query);

	if (!pg_prop_sql_exec(conn, "SET enable_hashagg = off", &err))
		pg_prop_fail(tc, "SET enable_hashagg=off failed: %s", err ? err : "");
	r2 = pg_prop_sql_rows(conn, query, &f2, &err);
	if (f2)
		pg_prop_fail(tc, "sort/groupagg query failed: %s\n  SQL: %s",
					 err ? err : "", query);

	pg_prop_sql_exec(conn, "RESET ALL", &err);

	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc,
					 "grouping result depends on strategy\n"
					 "  SQL: %s\n"
					 "  hashagg on:\n%s\n"
					 "  hashagg off:\n%s",
					 query, r1, r2);

	pg_free(r1);
	pg_free(r2);
	if (err)
		pg_free(err);
}

/*
 * Property 1: GROUP BY two plain columns with count(*) and sum(v).  The
 * grouping keys a and b both carry NULLs, so this exercises composite groups
 * where one or both keys are NULL on the hashed and sorted paths alike.
 */
PG_PROP(multikey_group_strategy)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	run_two(tc, conn,
			"SELECT a, b, count(*), sum(v) FROM gx_t GROUP BY a, b "
			"ORDER BY a NULLS FIRST, b NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * Property 2: GROUP BY a single grouping expression over two columns.  The
 * expression (a + coalesce(b,0)) is NULL exactly when a is NULL, so NULLS
 * FIRST pins that group; both strategies must compute the same expression
 * groups.
 */
PG_PROP(group_expr_strategy)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	run_two(tc, conn,
			"SELECT (a + coalesce(b,0)) k, count(*) FROM gx_t "
			"GROUP BY (a + coalesce(b,0)) ORDER BY k NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * Property 3: GROUP BY two boolean grouping expressions.  (v>0) is NULL when
 * v is NULL (so the first key spans true/false/NULL), while (a IS NULL) is a
 * total boolean; the composite exercises boolean hash keys and a NULL key
 * together.
 */
PG_PROP(group_bool_strategy)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	run_two(tc, conn,
			"SELECT (v>0) p, (a IS NULL) q, count(*) FROM gx_t "
			"GROUP BY (v>0), (a IS NULL) "
			"ORDER BY p NULLS FIRST, q NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * Property 4: SELECT DISTINCT over two columns.  DISTINCT may be implemented
 * by hashing or by sorting; both must yield the same distinct (a,b) pairs,
 * including the pairs with NULL in either column.
 */
PG_PROP(distinct_multikey)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	run_two(tc, conn,
			"SELECT DISTINCT a, b FROM gx_t "
			"ORDER BY a NULLS FIRST, b NULLS FIRST");

	pg_prop_disconnect(conn);
}

/*
 * Property 5: count(DISTINCT b) per group of a.  The inner DISTINCT and the
 * outer grouping can each be hashed or sorted; both strategies must agree on
 * the per-group distinct count, including for the NULL-a group and groups
 * whose b values include NULL (which count(DISTINCT) ignores).
 */
PG_PROP(group_count_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);

	run_two(tc, conn,
			"SELECT a, count(DISTINCT b) FROM gx_t GROUP BY a "
			"ORDER BY a NULLS FIRST");

	pg_prop_disconnect(conn);
}
