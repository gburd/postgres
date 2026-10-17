/*-------------------------------------------------------------------------
 *
 * prop_aggstrategies.c
 *	  Property-based tests for aggregate-strategy independence.
 *
 * An aggregation's result must not depend on which strategy the executor
 * picks: hash aggregation (enable_hashagg = on) and grouped/sorted
 * aggregation (enable_hashagg = off) must return identical rows.  The oracle
 * is differential -- the same GROUP BY query run under each strategy -- and
 * any disagreement is a wrong-results bug in nodeAgg, not a performance
 * difference, because the two paths are supposed to compute the same groups.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_aggstrategies.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "aggstrategies"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small integer domains keep the generated table legible in a counterexample
 * and keep each case fast while still exercising the grouping key boundaries:
 * g in -3..3 with ~15% NULL (so NULL grouping is tested on both paths), v
 * small and signed (so FILTER (WHERE v>0)/(v<0) and ordered array_agg have
 * both-sign content, ties, and NULLs).
 */
#define G_MIN (-3)
#define G_MAX 3
#define V_MIN (-5)
#define V_MAX 5

/* Draw g in [G_MIN, G_MAX], NULL ~15% of the time. */
static void
draw_g(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, G_MIN, G_MAX));
}

/* Draw v in [V_MIN, V_MAX], NULL ~15% of the time. */
static void
draw_v(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, V_MIN, V_MAX));
}

/*
 * (Re)build ag_t fresh with 0..30 random rows.  id is a serial-ish PRIMARY
 * KEY assigned here so the data is reproducible from the drawn choices.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ag_t; "
						   "CREATE TABLE ag_t (id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int r = 0; r < nrows; r++)
	{
		StringInfoData gval;
		StringInfoData vval;

		initStringInfo(&gval);
		initStringInfo(&vval);
		draw_g(tc, &gval);
		draw_v(tc, &vval);
		appendStringInfo(&ddl, "INSERT INTO ag_t VALUES (%d, %s, %s); ",
						 r, gval.data, vval.data);
		pfree(gval.data);
		pfree(vval.data);
	}
	appendStringInfoString(&ddl, "ANALYZE ag_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  SQL: %s",
					 err ? err : "", ddl.data);
	pfree(ddl.data);
}

/*
 * Differential oracle: run the same aggregate query twice, once forcing hash
 * aggregation (enable_hashagg = on) and once forcing grouped/sorted
 * aggregation (enable_hashagg = off).  Both must return identical rows.  Each
 * query must already carry a TOTAL ORDER BY on the grouping key(s) so the
 * row-major string compares as an ordered result; NULL groups are coalesced
 * to a sentinel in ORDER BY to give that a total order.  RESET ALL after.
 */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn, const char *query)
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
		pg_prop_fail(tc, "groupagg query failed: %s\n  SQL: %s",
					 err ? err : "", query);

	pg_prop_sql_exec(conn, "RESET ALL", &err);

	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc,
					 "aggregate result depends on strategy\n"
					 "  SQL: %s\n"
					 "  hashagg:\n%s\n"
					 "  groupagg:\n%s",
					 query, r1, r2);

	pg_free(r1);
	pg_free(r2);
	if (err)
		pg_free(err);
}

/*
 * Property 1: basic grouped aggregates (count/sum/avg/min/max) over g.
 * avg is cast to a fixed scale so the two strategies' textual rendering is
 * identical regardless of accumulation order.
 */
PG_PROP(basic_group)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_same_rows(tc, conn,
					  "SELECT g, count(*), sum(v), avg(v)::numeric(20,4), "
					  "min(v), max(v) "
					  "FROM ag_t GROUP BY g ORDER BY g NULLS FIRST");
	pg_prop_disconnect(conn);
}

/* Property 2: count(DISTINCT v) per group. */
PG_PROP(count_distinct_group)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_same_rows(tc, conn,
					  "SELECT g, count(DISTINCT v) "
					  "FROM ag_t GROUP BY g ORDER BY g NULLS FIRST");
	pg_prop_disconnect(conn);
}

/*
 * Property 3: grouping on two keys, one of them a boolean expression that
 * can itself be NULL (v is NULL -> v>0 is NULL).  Total order covers both.
 */
PG_PROP(group_two_keys)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_same_rows(tc, conn,
					  "SELECT g, (v>0) AS pos, count(*) "
					  "FROM ag_t GROUP BY g, (v>0) "
					  "ORDER BY g NULLS FIRST, pos NULLS FIRST");
	pg_prop_disconnect(conn);
}

/* Property 4: FILTER-ed aggregates per group. */
PG_PROP(filtered_agg)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_same_rows(tc, conn,
					  "SELECT g, count(*) FILTER (WHERE v>0), "
					  "sum(v) FILTER (WHERE v<0) "
					  "FROM ag_t GROUP BY g ORDER BY g NULLS FIRST");
	pg_prop_disconnect(conn);
}

/* Property 5: HAVING on the aggregate count. */
PG_PROP(having_clause)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_same_rows(tc, conn,
					  "SELECT g, count(*) FROM ag_t GROUP BY g "
					  "HAVING count(*) > 1 ORDER BY g NULLS FIRST");
	pg_prop_disconnect(conn);
}

/*
 * Property 6: ordered array_agg.  The ORDER BY inside array_agg makes the
 * array content deterministic, so both strategies must produce the same
 * arrays per group.
 */
PG_PROP(array_agg_ordered)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);
	require_same_rows(tc, conn,
					  "SELECT g, array_agg(v ORDER BY v) "
					  "FROM ag_t GROUP BY g ORDER BY g NULLS FIRST");
	pg_prop_disconnect(conn);
}
