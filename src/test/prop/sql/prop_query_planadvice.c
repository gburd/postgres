/*-------------------------------------------------------------------------
 *
 * prop_query_planadvice.c
 *	  Query-level property tests by plan-advice metamorphic testing.
 *
 * A query's result must not depend on how the planner chose to execute it.
 * The contrib/pg_plan_advice extension can steer specific planner decisions
 * (which scan method, which join method, which join order) through the
 * pg_plan_advice.advice GUC, so for a randomly generated query we run it
 * twice -- once with the planner free, once steered toward a different plan
 * shape -- and require the two result sets to be identical.
 *
 * Using plan advice rather than the blunt enable_* GUCs is deliberate: advice
 * names the exact relation/join to re-shape, so the alternate plan is a
 * targeted, legal variant of the same query rather than a global cost
 * distortion, and the extension's own validation rejects advice it cannot
 * honour (in which case the planner falls back to a valid plan that must still
 * agree).
 *
 * This is a metamorphic oracle: there is no reference answer, only the
 * invariant "the two runs agree".  Any disagreement is a wrong-results bug in
 * one of the execution paths (an index/bitmap scan that drops a row, a join
 * method that mis-handles a NULL key, a spill-to-disk path that corrupts a
 * batch).  These are exactly the bugs hardest to find by hand, because the
 * default plan is usually the well-tested one and the alternate paths fire
 * only on specific data.
 *
 * The one non-plan-shape knob we still flip is work_mem: a tiny value forces
 * the memory-bounded variants (external sort, hash/hashagg spill) that a small
 * test table would never otherwise reach.  That is a resource bound, not a
 * plan-shape override, and plan advice has no equivalent for it.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see pg_prop.h
 * and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_planadvice.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_planadvice"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/*
 * Connect and make pg_plan_advice available for the session.  LOAD makes the
 * pg_plan_advice.advice GUC settable; the extension need not be installed as
 * an object, only loaded into the backend.
 */
static pg_prop_conn *
connect_advice(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn, "LOAD 'pg_plan_advice'", &err))
		pg_prop_fail(tc, "LOAD pg_plan_advice failed: %s", err ? err : "");

	return conn;
}

/*
 * Run "sql" with the planner free, then again under the given plan advice (and
 * an optional extra SET such as a tiny work_mem), and require identical rows.
 * "advice" may be an empty string, in which case only the extra SET steers the
 * second run.  RESET ALL afterward so nothing leaks between cases.
 */
static void
require_plan_invariant(pg_prop_tc *tc, pg_prop_conn *conn,
					   const char *sql, const char *advice,
					   const char *extra_set)
{
	char	   *err = NULL;
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL;
	char	   *r1,
			   *r2;
	StringInfoData setup;

	/* Default plan. */
	if (!pg_prop_sql_exec(conn, "RESET ALL", &err))
		pg_prop_fail(tc, "RESET failed: %s", err ? err : "");
	r1 = pg_prop_sql_rows(conn, sql, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "default-plan query failed: %s\n  SQL: %s",
					 e1 ? e1 : "(unknown)", sql);

	/* Steered plan. */
	initStringInfo(&setup);
	if (advice[0] != '\0')
		appendStringInfo(&setup, "SET pg_plan_advice.advice = '%s'; ", advice);
	if (extra_set[0] != '\0')
		appendStringInfoString(&setup, extra_set);
	if (setup.len > 0 && !pg_prop_sql_exec(conn, setup.data, &err))
		pg_prop_fail(tc, "advice setup failed: %s\n  setup: %s",
					 err ? err : "", setup.data);
	pfree(setup.data);

	r2 = pg_prop_sql_rows(conn, sql, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "steered-plan query failed: %s\n  SQL: %s",
					 e2 ? e2 : "(unknown)", sql);

	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc,
					 "plan-dependent result for: %s\n"
					 "  advice: %s\n"
					 "  default plan rows:\n%s\n  steered plan rows:\n%s",
					 sql, advice, r1, r2);

	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * A filtered projection, steered to a sequential scan of the generated table.
 * The default plan may use any scan; forcing SEQ_SCAN must not change the
 * rows.  (If no index exists the advice is a no-op and the two runs trivially
 * agree, which is still a valid check of the SEQ_SCAN path.)
 */
PG_PROP(filter_plan_invariant)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData pred;
	StringInfoData q;

	conn = connect_advice(tc);
	qg_build_table(tc, conn, &nrows);

	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT id, a, b, c, f FROM " QG_TABLE
					 " WHERE (%s) ORDER BY id",
					 pred.data);

	require_plan_invariant(tc, conn, q.data,
						   "SEQ_SCAN(" QG_TABLE ")", "");

	pfree(pred.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A grouped aggregate run under a tiny work_mem to force the hashagg spill
 * (and external-sort) paths.  Grouping is a plan decision plan advice does not
 * cover, so the steering here is the work_mem bound; the result must not
 * depend on whether aggregation spilled to disk.
 */
PG_PROP(groupagg_plan_invariant)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData grp;
	StringInfoData q;

	conn = connect_advice(tc);
	qg_build_table(tc, conn, &nrows);

	initStringInfo(&grp);
	qg_draw_int_expr(tc, &grp);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT g, count(*), sum(a::bigint), min(b), max(b), "
					 "count(DISTINCT c) FROM "
					 "(SELECT (%s) AS g, a, b, c FROM " QG_TABLE ") s "
					 "GROUP BY g ORDER BY g",
					 grp.data);

	require_plan_invariant(tc, conn, q.data, "", "SET work_mem = '64kB';");

	pfree(grp.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A self-join steered to a nested-loop join.  The default plan may hash or
 * merge; forcing NESTED_LOOP_PLAIN on the inner relation must not change the
 * rows.  Join method is the richest source of result-dependent-on-plan bugs.
 */
PG_PROP(join_plan_invariant)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData pred;
	StringInfoData q;

	conn = connect_advice(tc);
	qg_build_table(tc, conn, &nrows);

	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	/*
	 * The generated predicate uses unqualified column names, so it is applied
	 * inside a single-table subquery (unambiguous there) and the filtered
	 * relation is self-joined under the alias t; the inner relation u is the
	 * join-method advice target.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT t.id, u.id FROM "
					 "(SELECT * FROM " QG_TABLE " WHERE (%s)) t "
					 "JOIN " QG_TABLE
					 " u ON (t.a IS NOT DISTINCT FROM u.b) "
					 "ORDER BY t.id, u.id",
					 pred.data);

	require_plan_invariant(tc, conn, q.data, "NESTED_LOOP_PLAIN(u)", "");

	pfree(pred.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The same self-join steered to a merge join (MERGE_JOIN_PLAIN).  A merge join
 * sorts both inputs and advances them in lockstep, a wholly different executor
 * path from nested-loop and hash joins and one with its own NULL-key and tie
 * handling; the rows must not change.  The equi-join key uses IS NOT DISTINCT
 * FROM so NULL keys participate, which is where merge-join bugs tend to hide.
 */
PG_PROP(mergejoin_plan_invariant)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData pred;
	StringInfoData q;

	conn = connect_advice(tc);
	qg_build_table(tc, conn, &nrows);

	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT t.id, u.id FROM "
					 "(SELECT * FROM " QG_TABLE " WHERE (%s)) t "
					 "JOIN " QG_TABLE
					 " u ON (t.a IS NOT DISTINCT FROM u.b) "
					 "ORDER BY t.id, u.id",
					 pred.data);

	require_plan_invariant(tc, conn, q.data, "MERGE_JOIN_PLAIN(u)", "");

	pfree(pred.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A three-way self-join steered to a specific join order.  The planner is
 * free to associate and commute the joins however it costs cheapest; forcing
 * a particular JOIN_ORDER must not change the rows.  Join reordering is a
 * distinct code path from join-method selection and has its own history of
 * wrong-results bugs (outer-join reordering, equivalence-class handling), so
 * it is worth steering explicitly.  Aliases x, y, z are joined on their keys;
 * the advice names a left-deep order over them.
 */
PG_PROP(join_order_plan_invariant)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData q;

	conn = connect_advice(tc);
	qg_build_table(tc, conn, &nrows);

	/*
	 * x JOIN y ON y.id = x.a joins on a possibly-NULL, non-unique key (so the
	 * join is interesting), then z on z.id = x.b.  All keys are coalesced into
	 * the id range only implicitly; non-matching rows simply drop, which is
	 * fine -- the invariant is that the surviving set is order-independent.
	 */
	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT x.id, y.id, z.id FROM " QG_TABLE " x "
						   "JOIN " QG_TABLE " y ON y.id = x.a "
						   "JOIN " QG_TABLE " z ON z.id = x.b "
						   "ORDER BY x.id, y.id, z.id");

	require_plan_invariant(tc, conn, q.data,
						   "JOIN_ORDER((x y) z)", "");

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * DISTINCT with ORDER BY ... LIMIT, run under a tiny work_mem to force the
 * sort/limit and hash-distinct spill paths.  ORDER BY the primary key keeps
 * the top-N result deterministic so the oracle stays sound.
 */
PG_PROP(distinct_limit_plan_invariant)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData q;
	int			lim;

	conn = connect_advice(tc);
	qg_build_table(tc, conn, &nrows);
	lim = (int) pg_prop_draw_int(tc, 0, QG_MAX_ROWS);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT DISTINCT id, a FROM " QG_TABLE
					 " ORDER BY id, a LIMIT %d",
					 lim);

	require_plan_invariant(tc, conn, q.data, "", "SET work_mem = '64kB';");

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Outer-join reordering: a three-way LEFT JOIN chain, steered to a specific
 * JOIN_ORDER, must return the same rows as the planner's free choice.  Outer-
 * join reordering is a distinct and historically bug-prone transformation
 * (the planner must respect outer-join identities and not reorder across a
 * nullable side incorrectly); forcing a legal order must not change the result.
 * Built over three self-joined copies of the generated table.
 */
PG_PROP(outer_join_order_invariant)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData q;

	conn = connect_advice(tc);
	qg_build_table(tc, conn, &nrows);

	/*
	 * x LEFT JOIN y on y.id = x.a, that LEFT JOIN z on z.id = x.b.  Project
	 * all three ids (NULL-extended on the right where no match), ordered for a
	 * stable comparison.  Steer a left-deep order over the aliases.
	 */
	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT x.id, y.id, z.id FROM " QG_TABLE " x "
						   "LEFT JOIN " QG_TABLE " y ON y.id = x.a "
						   "LEFT JOIN " QG_TABLE " z ON z.id = x.b "
						   "ORDER BY x.id, y.id NULLS FIRST, z.id NULLS FIRST");

	require_plan_invariant(tc, conn, q.data, "JOIN_ORDER((x y) z)", "");

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * GEQO equivalence: the genetic query optimizer searches the join-order space
 * heuristically instead of exhaustively, but it must never change the ANSWER
 * -- only which plan is chosen.  Run the same multi-way self-join with the
 * exhaustive planner (geqo off) and with GEQO forced (geqo on, threshold 2),
 * and require identical rows.  A difference would mean GEQO produced a plan
 * that computes the wrong result -- a serious optimizer bug.  Uses the plan-
 * invariant comparator with GEQO toggled via the extra SET.
 */
PG_PROP(geqo_equivalence)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData q;

	conn = connect_advice(tc);
	qg_build_table(tc, conn, &nrows);

	/* a 4-way self-join: well above geqo_threshold=2 when GEQO is forced */
	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT w.id, x.id, y.id, z.id FROM " QG_TABLE " w "
						   "JOIN " QG_TABLE " x ON x.id = w.a "
						   "JOIN " QG_TABLE " y ON y.id = w.b "
						   "JOIN " QG_TABLE " z ON z.id = x.a "
						   "ORDER BY w.id, x.id, y.id, z.id");

	/*
	 * Default run has geqo off (RESET ALL in require_plan_invariant); the
	 * steered run forces GEQO on with the minimum threshold so the genetic
	 * search is used for this join.  No plan advice -- just the GEQO toggle.
	 */
	require_plan_invariant(tc, conn, q.data, "",
						   "SET geqo = on; SET geqo_threshold = 2; "
						   "SET geqo_seed = 0.5;");

	pfree(q.data);
	pg_prop_disconnect(conn);
}
