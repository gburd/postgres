/*-------------------------------------------------------------------------
 *
 * prop_corrsubq.c
 *	  Property-based tests for correlated-subquery decorrelation equivalences.
 *
 * A correlated subquery references the outer row inside its WHERE clause; the
 * planner routinely decorrelates such a subquery into a grouped join (one
 * aggregate per group, joined back on the correlation key).  That rewrite must
 * be result-preserving, so each property pairs the correlated form against the
 * decorrelated grouped-join form and requires identical rows.  Distinct from
 * prop_subquery.c (uncorrelated EXISTS/IN/CTE flattening): here the inner query
 * is correlated on a group key g, and the trap is the NULL group -- the join
 * back onto g must be NULL-safe (IS NOT DISTINCT FROM) so the NULL group's
 * aggregate reaches its outer rows just as the correlated scan would.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_corrsubq.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "corrsubq"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Narrow domains: g in 0..3 makes groups dense; v in -4..4 for aggregates. */
#define V_MIN (-4)
#define V_MAX 4

/* Draw an integer in [min, max], or SQL NULL (~15% of the time). */
static void
draw_opt(pg_prop_tc *tc, StringInfo buf, int min, int max)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, min, max));
}

/*
 * Rebuild cs_t fresh: id int PRIMARY KEY (0..n-1), g int in 0..3 (~15% NULL),
 * v int in -4..4 (~15% NULL).  0..25 rows.  ANALYZE so the planner has stats
 * to choose whichever decorrelation strategy it likes -- that choice is what
 * the differential oracle probes.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS cs_t; "
						   "CREATE TABLE cs_t (id int PRIMARY KEY, g int, v int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int r = 0; r < nrows; r++)
	{
		StringInfoData g;
		StringInfoData v;

		initStringInfo(&g);
		initStringInfo(&v);
		draw_opt(tc, &g, 0, 3);
		draw_opt(tc, &v, V_MIN, V_MAX);
		appendStringInfo(&ddl, "INSERT INTO cs_t VALUES (%d, %s, %s); ",
						 r, g.data, v.data);
		pfree(g.data);
		pfree(v.data);
	}
	appendStringInfoString(&ddl, "ANALYZE cs_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * Differential oracle: run two equivalent query forms and require their full
 * result sets (newline-joined, row-major) to be byte-identical.  Both queries
 * must carry a total ORDER BY so the comparison is a sound multiset compare.
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
 * 1. Correlated count == grouped-join count.  Per-outer-row count(*) of the
 * same-group rows equals the group count joined back on g.  count(*) is never
 * NULL (0 for an empty group, but a correlated group always has >=1 member --
 * the outer row itself), and the join is NULL-safe so the NULL group's count
 * reaches the NULL-g outer rows.
 */
PG_PROP(corr_count_eq_groupjoin)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id, (SELECT count(*) FROM cs_t u WHERE u.g IS NOT DISTINCT FROM t.g) AS c "
					  "FROM cs_t t ORDER BY id",
					  "SELECT t.id, gc.c FROM cs_t t "
					  "JOIN (SELECT g, count(*) AS c FROM cs_t GROUP BY g) gc "
					  "ON gc.g IS NOT DISTINCT FROM t.g "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * 2. Correlated EXISTS == IN, restricted to non-NULL v on both sides.  With
 * t.v IS NOT NULL and the IN subquery filtered to non-NULL v, the three-valued
 * UNKNOWN that a NULL would introduce is removed, so EXISTS(u.v = t.v) and
 * t.v IN (SELECT v ...) coincide exactly.
 */
PG_PROP(corr_exists_eq_in)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM cs_t t "
					  "WHERE t.v IS NOT NULL "
					  "AND EXISTS (SELECT 1 FROM cs_t u WHERE u.v = t.v) "
					  "ORDER BY id",
					  "SELECT id FROM cs_t t "
					  "WHERE t.v IS NOT NULL "
					  "AND t.v IN (SELECT v FROM cs_t WHERE v IS NOT NULL) "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * 3. Correlated max == grouped-join max.  Per-outer-row max(u.v) over the
 * same group equals the group's max joined back on g.  max(v) is SQL NULL when
 * every v in the group is NULL -- the same value the correlated subquery
 * yields -- so no coalesce is needed; the NULL-safe join carries the NULL
 * group's max to the NULL-g outer rows.
 */
PG_PROP(corr_max_eq_join)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id, (SELECT max(u.v) FROM cs_t u WHERE u.g IS NOT DISTINCT FROM t.g) AS m "
					  "FROM cs_t t ORDER BY id",
					  "SELECT t.id, gm.m FROM cs_t t "
					  "JOIN (SELECT g, max(v) AS m FROM cs_t GROUP BY g) gm "
					  "ON gm.g IS NOT DISTINCT FROM t.g "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * 4. Correlated NOT EXISTS == set complement via EXCEPT.  Rather than a
 * hand-rolled antijoin, the rows with NO same-group u having u.v > t.v are
 * exactly the full id set minus the rows that DO have such a u.  Both operands
 * are grounded in cs_t, so EXCEPT's set semantics are sound; q2 is the EXCEPT
 * form and must agree with the direct NOT EXISTS.
 */
PG_PROP(corr_not_exists_eq_except)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM cs_t t "
					  "WHERE NOT EXISTS "
					  "(SELECT 1 FROM cs_t u WHERE u.g = t.g AND u.v > t.v) "
					  "ORDER BY id",
					  "SELECT id FROM cs_t "
					  "EXCEPT "
					  "SELECT t.id FROM cs_t t "
					  "WHERE EXISTS "
					  "(SELECT 1 FROM cs_t u WHERE u.g = t.g AND u.v > t.v) "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * 5. Correlated sum with default == grouped LEFT JOIN sum with default.
 * coalesce((SELECT sum(u.v) ...),0) per outer row equals coalesce(gs.s,0) over
 * a LEFT JOIN to the per-group sum.  sum(v) is NULL for an all-NULL group;
 * coalesce folds both the all-NULL-group case and any unmatched LEFT-JOIN row
 * to 0 identically.  The NULL-safe join carries the NULL group's sum across.
 */
PG_PROP(scalar_subq_default)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id, coalesce((SELECT sum(u.v) FROM cs_t u WHERE u.g IS NOT DISTINCT FROM t.g), 0) AS s "
					  "FROM cs_t t ORDER BY id",
					  "SELECT t.id, coalesce(gs.s, 0) FROM cs_t t "
					  "LEFT JOIN (SELECT g, sum(v) AS s FROM cs_t GROUP BY g) gs "
					  "ON gs.g IS NOT DISTINCT FROM t.g "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}
