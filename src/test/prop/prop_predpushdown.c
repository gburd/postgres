/*-------------------------------------------------------------------------
 *
 * prop_predpushdown.c
 *	  Property-based tests for predicate pushdown / qual placement.
 *
 * A predicate's syntactic position must not change which rows a query
 * returns: whether a qual sits in a subquery, in the outer WHERE, in an
 * INNER JOIN's ON clause, below a UNION ALL, or below a GROUP BY on the
 * grouping key, the planner is free to move it but the result set must be
 * identical.  The oracle is differential (require_same_rows): two queries
 * that place the same qual differently must return identical, totally-
 * ordered rows.  Any mismatch is a qual-placement / pushdown bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_predpushdown.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "predpushdown"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small integer domains keep each case fast and keep a counterexample
 * legible, while still crossing the predicate boundaries (0, 4, NULL) the
 * properties pivot on.
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
 * Build pp_t(id int PRIMARY KEY, a int, b int) with 0..30 rows.  id is dense
 * (0..nrows-1) so it is a usable total-order key; a and b are drawn in
 * [-4,4] with ~15% NULL.  Rebuilt fresh in every property.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS pp_t; "
						   "CREATE TABLE pp_t (id int PRIMARY KEY, a int, b int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 30);
	for (int r = 0; r < nrows; r++)
	{
		appendStringInfo(&ddl, "INSERT INTO pp_t VALUES (%d, ", r);
		draw_val(tc, &ddl);
		appendStringInfoString(&ddl, ", ");
		draw_val(tc, &ddl);
		appendStringInfoString(&ddl, "); ");
	}
	appendStringInfoString(&ddl, "ANALYZE pp_t;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(no message)", ddl.data);
	pfree(ddl.data);
}

/* Shape B: two queries that must return identical, totally-ordered rows. */
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
 * A property body: connect, build the table, run the two formulations, done.
 * conn/err are local so each property is self-contained.
 */
#define PP_SETUP() \
	pg_prop_conn *conn; \
	char	   *err = NULL; \
	conn = pg_prop_connect(&err); \
	if (conn == NULL) \
		pg_prop_fail(tc, "connect failed: %s", err ? err : ""); \
	build_table(tc, conn)

/*
 * 1. A qual that lives in a subquery vs. the same qual folded into the outer
 *	  WHERE of a flattened query.  Subquery pullup must not change rows.
 */
PG_PROP(where_in_subquery_eq_outer)
{
	PP_SETUP();
	require_same_rows(tc, conn,
					  "SELECT id FROM (SELECT id, a FROM pp_t WHERE a > 0) s "
					  "WHERE s.a < 4 ORDER BY id",
					  "SELECT id FROM pp_t WHERE a > 0 AND a < 4 ORDER BY id");
	pg_prop_disconnect(conn);
}

/*
 * 2. For an INNER join, a qual in the ON clause and the same qual in WHERE are
 *	  equivalent; the planner moves it freely.
 */
PG_PROP(join_on_vs_where_inner)
{
	PP_SETUP();
	require_same_rows(tc, conn,
					  "SELECT t.id FROM pp_t t JOIN pp_t u "
					  "ON t.id = u.id AND t.a > 0 ORDER BY t.id",
					  "SELECT t.id FROM pp_t t JOIN pp_t u "
					  "ON t.id = u.id WHERE t.a > 0 ORDER BY t.id");
	pg_prop_disconnect(conn);
}

/*
 * 3. A qual above a UNION ALL vs. pushed into both arms.  Pushdown through a
 *	  set operation must distribute over both legs identically.
 */
PG_PROP(pushdown_through_union)
{
	PP_SETUP();
	require_same_rows(tc, conn,
					  "SELECT id FROM (SELECT id, a FROM pp_t "
					  "UNION ALL SELECT id, a FROM pp_t) s "
					  "WHERE s.a > 0 ORDER BY id",
					  "(SELECT id FROM pp_t WHERE a > 0 "
					  "UNION ALL SELECT id FROM pp_t WHERE a > 0) ORDER BY id");
	pg_prop_disconnect(conn);
}

/*
 * 4. A qual on a grouping key above a GROUP BY vs. pushed below the aggregate.
 *	  A restriction on the grouping column may legally move under the group.
 */
PG_PROP(pushdown_through_groupby)
{
	PP_SETUP();
	require_same_rows(tc, conn,
					  "SELECT g, c FROM (SELECT a g, count(*) c FROM pp_t GROUP BY a) s "
					  "WHERE s.g > 0 ORDER BY g",
					  "SELECT a g, count(*) c FROM pp_t WHERE a > 0 GROUP BY a ORDER BY a");
	pg_prop_disconnect(conn);
}

/*
 * 5. Constant folding of a qual expression must not change rows: a > (1+1-2)
 *	  is a > 0.
 */
PG_PROP(const_fold_eq)
{
	PP_SETUP();
	require_same_rows(tc, conn,
					  "SELECT id FROM pp_t WHERE a > (1+1-2) ORDER BY id",
					  "SELECT id FROM pp_t WHERE a > 0 ORDER BY id");
	pg_prop_disconnect(conn);
}

/*
 * 6. Removing a duplicate qual (a>0 AND a>0) must not change rows.
 */
PG_PROP(redundant_qual)
{
	PP_SETUP();
	require_same_rows(tc, conn,
					  "SELECT id FROM pp_t WHERE a > 0 AND a > 0 ORDER BY id",
					  "SELECT id FROM pp_t WHERE a > 0 ORDER BY id");
	pg_prop_disconnect(conn);
}
