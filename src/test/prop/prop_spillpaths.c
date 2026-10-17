/*-------------------------------------------------------------------------
 *
 * prop_spillpaths.c
 *	  Property-based tests for memory-bounded executor spill paths.
 *
 * A query's result must not depend on how much working memory the executor
 * is given.  With a tiny work_mem the sort spills to tape, the hash
 * aggregate and hash join spill their batches to disk, and SELECT DISTINCT /
 * UNION fall back to their external paths; with ample work_mem they all run
 * in memory.  The oracle is differential: run each query under work_mem =
 * '64kB' (spill-prone) and under work_mem = '64MB' (in-memory) and require
 * byte-identical, fully ordered result sets.  Any disagreement is a bug in a
 * spill path (tape merge, batch repartitioning, HashAgg disk spill, ...).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_spillpaths.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "spillpaths"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run "query" twice: once with a tiny work_mem that forces external sort /
 * hash spill / hashagg spill, once with ample work_mem that keeps everything
 * in memory.  RESET ALL between (and after) so no GUC leaks across the two
 * runs or to the next property.  Require the two result sets to be identical.
 * Every caller's query carries a TOTAL ORDER BY so the comparison is sound.
 */
static void
run_two_mem(pg_prop_tc *tc, pg_prop_conn *conn, const char *query)
{
	char	   *err = NULL;
	char	   *r_small;
	char	   *r_big;
	bool		fs = false;
	bool		fb = false;

	if (!pg_prop_sql_exec(conn, "SET work_mem = '64kB'", &err))
		pg_prop_fail(tc, "SET work_mem=64kB failed: %s", err ? err : "");
	r_small = pg_prop_sql_rows(conn, query, &fs, &err);
	if (fs)
		pg_prop_fail(tc, "spill-prone query failed: %s\n  SQL: %s",
					 err ? err : "", query);
	if (!pg_prop_sql_exec(conn, "RESET ALL", &err))
		pg_prop_fail(tc, "RESET ALL failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn, "SET work_mem = '64MB'", &err))
		pg_prop_fail(tc, "SET work_mem=64MB failed: %s", err ? err : "");
	r_big = pg_prop_sql_rows(conn, query, &fb, &err);
	if (fb)
		pg_prop_fail(tc, "in-memory query failed: %s\n  SQL: %s",
					 err ? err : "", query);
	if (!pg_prop_sql_exec(conn, "RESET ALL", &err))
		pg_prop_fail(tc, "RESET ALL failed: %s", err ? err : "");

	if (strcmp(r_small, r_big) != 0)
		pg_prop_fail(tc,
					 "spill vs in-memory mismatch:\n  SQL: %s\n"
					 "  work_mem=64kB:\n%s\n  work_mem=64MB:\n%s",
					 query, r_small, r_big);

	pg_free(r_small);
	pg_free(r_big);
	if (err)
		pg_free(err);
}

/*
 * Build sp_t(id int PRIMARY KEY, g int, v int) with 0..60 rows.  id is dense
 * 0..n-1; g in 0..9 (so GROUP BY g makes a handful of groups that fit many
 * rows each); v in -10..10 with ~10% NULL (so NULLS FIRST/LAST ordering and
 * NULL-in-aggregate behaviour are exercised).  ANALYZE so the planner has
 * stats and may actually choose hash/sort plans worth spilling.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 0, 60);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS sp_t; "
						   "CREATE TABLE sp_t(id int PRIMARY KEY, g int, v int); ");

	for (int i = 0; i < nrows; i++)
	{
		int			g = (int) pg_prop_draw_int(tc, 0, 9);

		appendStringInfo(&ddl, "INSERT INTO sp_t VALUES (%d, %d, ", i, g);
		if (pg_prop_draw_int(tc, 0, 9) == 0)
			appendStringInfoString(&ddl, "NULL); ");
		else
			appendStringInfo(&ddl, "%d); ",
							 (int) pg_prop_draw_int(tc, -10, 10));
	}
	appendStringInfoString(&ddl, "ANALYZE sp_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build sp_t failed: %s\n  DDL: %s",
					 err ? err : "", ddl.data);
	pfree(ddl.data);
}

/* Shared setup: connect and build the table.  Caller disconnects. */
static pg_prop_conn *
setup(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_table(tc, conn);
	return conn;
}

/* 1. External sort vs in-memory sort. */
PG_PROP(sort_spill)
{
	pg_prop_conn *conn = setup(tc);

	run_two_mem(tc, conn,
				"SELECT id, v FROM sp_t ORDER BY v NULLS LAST, id");
	pg_prop_disconnect(conn);
}

/* 2. HashAgg disk spill vs in-memory aggregate. */
PG_PROP(hashagg_spill)
{
	pg_prop_conn *conn = setup(tc);

	run_two_mem(tc, conn,
				"SELECT g, count(*), sum(v) FROM sp_t "
				"GROUP BY g ORDER BY g NULLS FIRST");
	pg_prop_disconnect(conn);
}

/* 3. DISTINCT hash/sort spill vs in-memory. */
PG_PROP(distinct_spill)
{
	pg_prop_conn *conn = setup(tc);

	run_two_mem(tc, conn,
				"SELECT DISTINCT g, (v > 0) FROM sp_t "
				"ORDER BY g NULLS FIRST, 2 NULLS FIRST");
	pg_prop_disconnect(conn);
}

/* 4. Hash join batch spill vs in-memory (bounded a side, full b side). */
PG_PROP(hashjoin_spill)
{
	pg_prop_conn *conn = setup(tc);

	run_two_mem(tc, conn,
				"SELECT a.id, b.id FROM sp_t a JOIN sp_t b ON a.g = b.g "
				"WHERE a.id <= 10 ORDER BY a.id, b.id");
	pg_prop_disconnect(conn);
}

/* 5. Set-operation (UNION) spill vs in-memory. */
PG_PROP(setop_spill)
{
	pg_prop_conn *conn = setup(tc);

	run_two_mem(tc, conn,
				"SELECT x FROM (SELECT v AS x FROM sp_t "
				"UNION SELECT v + 1 FROM sp_t) s ORDER BY x NULLS FIRST");
	pg_prop_disconnect(conn);
}

/* 6. GROUP BY with count(DISTINCT) (grouped-agg distinct) spill vs in-memory. */
PG_PROP(groupagg_distinct_spill)
{
	pg_prop_conn *conn = setup(tc);

	run_two_mem(tc, conn,
				"SELECT g, count(DISTINCT v) FROM sp_t "
				"GROUP BY g ORDER BY g NULLS FIRST");
	pg_prop_disconnect(conn);
}
