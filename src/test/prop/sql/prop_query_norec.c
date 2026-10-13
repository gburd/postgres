/*-------------------------------------------------------------------------
 *
 * prop_query_norec.c
 *	  Query-level property tests using NoREC (non-optimising reference).
 *
 * NoREC (Rigger & Su, ESEC/FSE 2020) finds optimisation bugs by comparing a
 * query the planner can optimise against a semantically equivalent form it
 * cannot.  The classic transformation: the optimisable query
 *
 *	   SELECT count(*) FROM t WHERE p
 *
 * puts p where the planner may use an index, reorder, or push it down.  The
 * un-optimisable twin pulls p into the target list of an unrestricted scan,
 * where the planner must evaluate it per row with no shortcut:
 *
 *	   SELECT count(*) FROM (SELECT (p) IS TRUE AS t FROM t) s WHERE s.t
 *
 * Both count the rows for which p is TRUE, so the counts must match.  If they
 * differ, the optimisation applied to the first form changed the answer -- a
 * bug in predicate pushdown, index-qual translation, or scan filtering.
 *
 * Because both forms are evaluated by the same server, NoREC needs no external
 * reference database, and because the twin is deliberately opaque to the
 * optimiser, a mismatch points straight at the optimiser.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see pg_prop.h
 * and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_norec.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_norec"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/* Require two scalar queries to return the same value. */
static void
require_same_scalar(pg_prop_tc *tc, pg_prop_conn *conn,
					const char *opt, const char *noropt)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL;
	char	   *r1,
			   *r2;

	r1 = pg_prop_sql_scalar(conn, opt, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "optimisable query failed: %s\n  SQL: %s",
					 e1 ? e1 : "(unknown)", opt);
	r2 = pg_prop_sql_scalar(conn, noropt, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "non-optimisable query failed: %s\n  SQL: %s",
					 e2 ? e2 : "(unknown)", noropt);

	if (r1 == NULL || r2 == NULL || strcmp(r1, r2) != 0)
		pg_prop_fail(tc,
					 "NoREC mismatch (%s vs %s):\n  optimisable:      %s\n"
					 "  non-optimisable:  %s",
					 r1 ? r1 : "NULL", r2 ? r2 : "NULL", opt, noropt);

	if (r1)
		pg_free(r1);
	if (r2)
		pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * Core NoREC: count(*) WHERE p  vs  count of (p IS TRUE) over a bare scan.
 */
PG_PROP(count_matches_norec)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;
	StringInfoData opt;
	StringInfoData noropt;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);

	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	initStringInfo(&opt);
	appendStringInfo(&opt,
					 "SELECT count(*) FROM " QG_TABLE " WHERE (%s)",
					 pred.data);

	initStringInfo(&noropt);
	appendStringInfo(&noropt,
					 "SELECT count(*) FROM "
					 "(SELECT (%s) IS TRUE AS keep FROM " QG_TABLE ") s "
					 "WHERE s.keep",
					 pred.data);

	require_same_scalar(tc, conn, opt.data, noropt.data);

	pfree(pred.data);
	pfree(opt.data);
	pfree(noropt.data);
	pg_prop_disconnect(conn);
}

/*
 * NoREC with the predicate negated: the un-optimisable twin uses IS NOT TRUE
 * to count the complement, which must equal count(*) WHERE NOT p plus the NULL
 * rows.  Simpler sound form: count WHERE NOT p == count of (p IS FALSE).
 */
PG_PROP(count_false_matches_norec)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;
	StringInfoData opt;
	StringInfoData noropt;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);

	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	initStringInfo(&opt);
	appendStringInfo(&opt,
					 "SELECT count(*) FROM " QG_TABLE " WHERE NOT (%s)",
					 pred.data);

	initStringInfo(&noropt);
	appendStringInfo(&noropt,
					 "SELECT count(*) FROM "
					 "(SELECT (%s) IS FALSE AS keep FROM " QG_TABLE ") s "
					 "WHERE s.keep",
					 pred.data);

	require_same_scalar(tc, conn, opt.data, noropt.data);

	pfree(pred.data);
	pfree(opt.data);
	pfree(noropt.data);
	pg_prop_disconnect(conn);
}

/*
 * NoREC across a join: the optimisable form filters the inner relation of a
 * self-join with p in the WHERE clause, where the planner may push p below
 * the join or turn it into an index qual.  The un-optimisable twin forms the
 * full (unfiltered) join and counts (p IS TRUE) from the target list, so the
 * predicate cannot be pushed down or indexed.  Both count the join rows whose
 * left side satisfies p.  Join-filter interaction with pushdown is exactly
 * where NoREC found real optimiser bugs in other engines.
 */
PG_PROP(join_filter_matches_norec)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;
	StringInfoData opt;
	StringInfoData noropt;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);

	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	/*
	 * Optimisable: p applied to t in the WHERE of a t-JOIN-u self-join on a
	 * fixed inner row (u.id = 1), so the join cardinality is just the number
	 * of t rows satisfying p when such a u row exists, else zero.  p uses
	 * unqualified names so it is evaluated inside a single-table subquery over
	 * t where they are unambiguous.
	 */
	initStringInfo(&opt);
	appendStringInfo(&opt,
					 "SELECT count(*) FROM "
					 "(SELECT * FROM " QG_TABLE " WHERE (%s)) t "
					 "JOIN " QG_TABLE " u ON u.id = 1",
					 pred.data);

	/*
	 * Un-optimisable twin: the full join with no filter, counting (p IS TRUE)
	 * pulled into the projection so it is evaluated per surviving join row
	 * with no pushdown available.
	 */
	initStringInfo(&noropt);
	appendStringInfo(&noropt,
					 "SELECT count(*) FROM "
					 "(SELECT (t.p) AS keep FROM "
					 "(SELECT *, (%s) AS p FROM " QG_TABLE ") t "
					 "JOIN " QG_TABLE " u ON u.id = 1) s "
					 "WHERE s.keep IS TRUE",
					 pred.data);

	require_same_scalar(tc, conn, opt.data, noropt.data);

	pfree(pred.data);
	pfree(opt.data);
	pfree(noropt.data);
	pg_prop_disconnect(conn);
}

/*
 * NoREC over a sum: sum(a) WHERE p vs sum of a*(p IS TRUE)::int over a bare
 * scan.  Exercises aggregate pushdown interacting with the filter.  coalesce
 * so NULL a does not make the two forms disagree on NULL vs 0 (both treat a
 * missing contribution as 0).
 */
PG_PROP(sum_matches_norec)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;
	StringInfoData opt;
	StringInfoData noropt;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);

	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	initStringInfo(&opt);
	appendStringInfo(&opt,
					 "SELECT coalesce(sum(coalesce(a,0)),0) FROM " QG_TABLE
					 " WHERE (%s)",
					 pred.data);

	initStringInfo(&noropt);
	appendStringInfo(&noropt,
					 "SELECT coalesce(sum(CASE WHEN (%s) IS TRUE "
					 "THEN coalesce(a,0) ELSE 0 END),0) FROM " QG_TABLE,
					 pred.data);

	require_same_scalar(tc, conn, opt.data, noropt.data);

	pfree(pred.data);
	pfree(opt.data);
	pfree(noropt.data);
	pg_prop_disconnect(conn);
}
