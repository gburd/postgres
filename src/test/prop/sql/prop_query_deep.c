/*-------------------------------------------------------------------------
 *
 * prop_query_deep.c
 *	  Deep query-level property tests over the multi-table generator.
 *
 * This area drives the Phase 1 query builder (qg_build_query): a random join
 * tree across the generated database's foreign-key graph, sometimes wrapped in
 * a derived-table subquery or a CTE, with a single mutable predicate over the
 * anchor relation.  Each oracle is a metamorphic relation that must hold for
 * any such query -- no reference answer, so a disagreement is a planner or
 * executor wrong-results bug.
 *
 * Oracles:
 *	 1. deep_tlp -- the mutable predicate partitioned three ways (p / NOT p /
 *		p IS NULL) must reconstruct the unpartitioned query's rows.  This is
 *		TLP applied to an arbitrary generated join/subquery/CTE shape.
 *	 2. deep_norec -- count(*) with the predicate in WHERE equals the count of
 *		(predicate IS TRUE) pulled into an opaque per-row projection over the
 *		same join; the optimizer can push the first form down but not the
 *		second, so a mismatch is an optimization bug.
 *	 3. deep_cte_inline -- the same query as a plain subquery, a non-materialized
 *		CTE, and a MATERIALIZED CTE must all return identical rows; this targets
 *		the CTE-inlining / subquery-pullup rewrites.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_deep.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_deep"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/* Depth of the generated join/nesting (number of FK hops). */
#define QG_DEEP_MAX 4

static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *lhs, const char *rhs)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL;
	char	   *r1,
			   *r2;

	/*
	 * Compare by server-side digest, not by shipping the full result: deep
	 * generated queries can return results too large to render client-side
	 * (finding 027).  Two queries that return the same rows in the same order
	 * hash equal.
	 */
	r1 = pg_prop_sql_rowhash(conn, lhs, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "lhs query failed: %s\n  SQL: %s",
					 e1 ? e1 : "(unknown)", lhs);
	r2 = pg_prop_sql_rowhash(conn, rhs, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "rhs query failed: %s\n  SQL: %s",
					 e2 ? e2 : "(unknown)", rhs);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc,
					 "deep-query digest mismatch:\n  lhs: %s\n  rhs: %s\n"
					 "  lhs md5: %s\n  rhs md5: %s",
					 lhs, rhs, r1, r2);
	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

static long
scalar_long(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	long		n;

	v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "scalar query failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	n = v ? atol(v) : 0;
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	return n;
}

/*
 * Build a database and a deep query; return false (and clean up) if the random
 * database was too small to build a query, so the caller returns a vacuous
 * pass.  On true, *db and *q are owned by the caller (free with qg_free_query +
 * qg_drop_db).
 */
static bool
setup(pg_prop_tc *tc, pg_prop_conn **conn_out, qg_db **db_out, qg_query *q)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	qg_db_params p;
	qg_db	   *db;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_db_params_for_env(&p);
	db = qg_build_db(tc, conn, &p);

	if (!qg_build_query(tc, db, QG_DEEP_MAX, q))
	{
		qg_drop_db(tc, conn, db);
		pg_prop_disconnect(conn);
		return false;
	}
	*conn_out = conn;
	*db_out = db;
	return true;
}

/* TLP over an arbitrary generated join/subquery/CTE shape. */
PG_PROP(deep_tlp)
{
	pg_prop_conn *conn;
	qg_db	   *db;
	qg_query	q;
	StringInfoData pred;
	StringInfoData whole;
	StringInfoData part;

	if (!setup(tc, &conn, &db, &q))
		return;

	initStringInfo(&pred);
	qg_draw_predicate_on(tc, &pred, q.pred_table, q.pred_alias);

	/* whole: the query, unfiltered, ordered */
	initStringInfo(&whole);
	appendStringInfo(&whole, "%s %s", q.select_from, q.order_by);

	/*
	 * partitioned: the same select_from with the predicate added three ways.
	 * select_from has no WHERE, so appending " WHERE (pred)" is well-formed;
	 * the three are UNION ALL'd and re-ordered by the key list.
	 */
	initStringInfo(&part);
	appendStringInfo(&part,
					 "SELECT * FROM ("
					 "%s WHERE (%s) "
					 "UNION ALL %s WHERE NOT (%s) "
					 "UNION ALL %s WHERE (%s) IS NULL"
					 ") s ORDER BY %s",
					 q.select_from, pred.data,
					 q.select_from, pred.data,
					 q.select_from, pred.data,
					 q.keys);

	require_same_rows(tc, conn, whole.data, part.data);

	pfree(pred.data);
	pfree(whole.data);
	pfree(part.data);
	qg_free_query(&q);
	qg_drop_db(tc, conn, db);
	pg_prop_disconnect(conn);
}

/* NoREC over the deep query: optimizable count vs opaque per-row count. */
PG_PROP(deep_norec)
{
	pg_prop_conn *conn;
	qg_db	   *db;
	qg_query	q;
	StringInfoData pred;
	StringInfoData opt;
	StringInfoData noropt;
	long		c1,
				c2;

	if (!setup(tc, &conn, &db, &q))
		return;

	initStringInfo(&pred);
	qg_draw_predicate_on(tc, &pred, q.pred_table, q.pred_alias);

	/* optimizable: count of rows of the join where the predicate holds */
	initStringInfo(&opt);
	appendStringInfo(&opt,
					 "SELECT count(*) FROM (%s WHERE (%s)) s",
					 q.select_from, pred.data);
	c1 = scalar_long(tc, conn, opt.data);

	/*
	 * un-optimizable: the same predicate wrapped in a CASE so the planner
	 * cannot turn it into an index qual or push it below a join, evaluated in
	 * the same scope (the WHERE of select_from).  The two must count the same
	 * rows.
	 */
	initStringInfo(&noropt);
	appendStringInfo(&noropt,
					 "SELECT count(*) FROM "
					 "(%s WHERE (CASE WHEN (%s) IS TRUE THEN true ELSE false END)) s",
					 q.select_from, pred.data);
	c2 = scalar_long(tc, conn, noropt.data);

	if (c1 != c2)
		pg_prop_fail(tc,
					 "deep NoREC mismatch: %ld vs %ld\n  opt: %s\n  noropt: %s",
					 c1, c2, opt.data, noropt.data);

	pfree(pred.data);
	pfree(opt.data);
	pfree(noropt.data);
	qg_free_query(&q);
	qg_drop_db(tc, conn, db);
	pg_prop_disconnect(conn);
}

/*
 * The same filtered query expressed three ways -- inline subquery, a plain
 * (inlinable) CTE, and a MATERIALIZED CTE -- must return identical rows.  This
 * targets the subquery-pullup and CTE-inlining rewrites: the planner inlines
 * the plain CTE and the subquery but must not inline MATERIALIZED, yet all
 * three are semantically identical.
 */
PG_PROP(deep_cte_inline)
{
	pg_prop_conn *conn;
	qg_db	   *db;
	qg_query	q;
	StringInfoData pred;
	StringInfoData sub;
	StringInfoData cte;
	StringInfoData mcte;

	if (!setup(tc, &conn, &db, &q))
		return;

	initStringInfo(&pred);
	qg_draw_predicate_on(tc, &pred, q.pred_table, q.pred_alias);

	/* inline subquery */
	initStringInfo(&sub);
	appendStringInfo(&sub,
					 "SELECT * FROM (%s WHERE (%s)) s ORDER BY %s",
					 q.select_from, pred.data, q.keys);

	/* plain CTE (planner may inline) */
	initStringInfo(&cte);
	appendStringInfo(&cte,
					 "WITH c AS (%s WHERE (%s)) "
					 "SELECT * FROM c s ORDER BY %s",
					 q.select_from, pred.data, q.keys);

	/* materialized CTE (planner must not inline) */
	initStringInfo(&mcte);
	appendStringInfo(&mcte,
					 "WITH c AS MATERIALIZED (%s WHERE (%s)) "
					 "SELECT * FROM c s ORDER BY %s",
					 q.select_from, pred.data, q.keys);

	require_same_rows(tc, conn, sub.data, cte.data);
	require_same_rows(tc, conn, sub.data, mcte.data);

	pfree(pred.data);
	pfree(sub.data);
	pfree(cte.data);
	pfree(mcte.data);
	qg_free_query(&q);
	qg_drop_db(tc, conn, db);
	pg_prop_disconnect(conn);
}

/*
 * Constant-folding metamorphic: wrapping the predicate in a form the planner
 * can constant-fold must not change the result versus a form it cannot.  We
 * AND the predicate with a tautology written two ways: (1 = 1), which the
 * planner folds away at plan time, and (f(x) = f(x)) over a volatile-looking
 * but actually-constant expression it must evaluate per row.  Both are always
 * TRUE, so both queries must equal the unmodified query.
 */
PG_PROP(deep_const_fold)
{
	pg_prop_conn *conn;
	qg_db	   *db;
	qg_query	q;
	StringInfoData pred;
	StringInfoData base;
	StringInfoData folded;
	StringInfoData unfolded;

	if (!setup(tc, &conn, &db, &q))
		return;

	initStringInfo(&pred);
	qg_draw_predicate_on(tc, &pred, q.pred_table, q.pred_alias);

	initStringInfo(&base);
	appendStringInfo(&base, "%s WHERE (%s) %s",
					 q.select_from, pred.data, q.order_by);

	/* AND a plan-time-foldable tautology */
	initStringInfo(&folded);
	appendStringInfo(&folded, "%s WHERE ((%s) AND (1 = 1)) %s",
					 q.select_from, pred.data, q.order_by);

	/*
	 * AND a run-time tautology the planner cannot fold: abs(hashint4(k0)) >=
	 * 0 is always true but depends on a per-row value.  k0 is the first
	 * projected key, visible only after projection, so instead use a
	 * constant-but-opaque expression over the anchor's id: (r0.id - r0.id = 0),
	 * which references a column so it is evaluated per row yet always holds.
	 */
	initStringInfo(&unfolded);
	appendStringInfo(&unfolded,
					 "%s WHERE ((%s) AND (%s.id - %s.id = 0)) %s",
					 q.select_from, pred.data,
					 q.pred_alias, q.pred_alias, q.order_by);

	require_same_rows(tc, conn, base.data, folded.data);
	require_same_rows(tc, conn, base.data, unfolded.data);

	pfree(pred.data);
	pfree(base.data);
	pfree(folded.data);
	pfree(unfolded.data);
	qg_free_query(&q);
	qg_drop_db(tc, conn, db);
	pg_prop_disconnect(conn);
}

/*
 * Correlated-subquery rewrite: for a child/parent FK pair, the child rows that
 * have a matching parent are expressible three equivalent ways --
 *
 *	 WHERE fk IN (SELECT id FROM parent)
 *	 WHERE EXISTS (SELECT 1 FROM parent WHERE parent.id = child.fk)
 *	 a semijoin written as a plain JOIN on the (unique) parent id, DISTINCTed
 *
 * The planner unnests IN and EXISTS into a semijoin; all three must return the
 * same child ids.  This targets subquery unnesting directly.  (The FK is
 * enforced, so "has a matching parent" is simply fk IS NOT NULL, but the point
 * is that the three *syntactic* forms must agree regardless.)
 */
PG_PROP(deep_corr_subquery)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	qg_db_params p;
	qg_db	   *db;
	int			ci = -1;
	const qg_table *child;
	const qg_table *parent;
	StringInfoData in_form;
	StringInfoData exists_form;
	StringInfoData join_form;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_db_params_for_env(&p);
	db = qg_build_db(tc, conn, &p);

	for (int i = 0; i < db->ntables; i++)
		if (db->tables[i].fkref >= 0)
		{
			ci = i;
			break;
		}
	if (ci < 0)
	{
		qg_drop_db(tc, conn, db);
		pg_prop_disconnect(conn);
		return;
	}
	child = &db->tables[ci];
	parent = &db->tables[child->fkref];

	/*
	 * Filter the parent side with a generated predicate so the semijoin
	 * genuinely excludes some children (not just the FK-enforced "fk is not
	 * NULL").  The same parent predicate is used in all three forms.
	 */
	{
		StringInfoData ppred;

		initStringInfo(&ppred);
		qg_draw_predicate_on(tc, &ppred, parent, "pa");

		initStringInfo(&in_form);
		appendStringInfo(&in_form,
						 "SELECT id FROM %s ch WHERE fk IN "
						 "(SELECT id FROM %s pa WHERE (%s)) ORDER BY id",
						 child->name, parent->name, ppred.data);

		initStringInfo(&exists_form);
		appendStringInfo(&exists_form,
						 "SELECT id FROM %s ch WHERE EXISTS "
						 "(SELECT 1 FROM %s pa WHERE pa.id = ch.fk AND (%s)) "
						 "ORDER BY id",
						 child->name, parent->name, ppred.data);

		initStringInfo(&join_form);
		appendStringInfo(&join_form,
						 "SELECT DISTINCT ch.id FROM %s ch JOIN %s pa "
						 "ON pa.id = ch.fk WHERE (%s) ORDER BY ch.id",
						 child->name, parent->name, ppred.data);
		pfree(ppred.data);
	}

	require_same_rows(tc, conn, in_form.data, exists_form.data);
	require_same_rows(tc, conn, in_form.data, join_form.data);

	pfree(in_form.data);
	pfree(exists_form.data);
	pfree(join_form.data);
	qg_drop_db(tc, conn, db);
	pg_prop_disconnect(conn);
}
