/*-------------------------------------------------------------------------
 *
 * prop_query_diff.c
 *	  Full-configuration differential oracle.
 *
 * This is the strongest wrong-results oracle in the suite.  The idea, taken
 * from SQLancer's differential / query-plan-guidance mode (which found the
 * large majority of real DBMS bugs in that line of work): a correct database
 * returns the *same rows* for a query regardless of how the planner and
 * executor are forced to compute it.  We take one generated query over
 * adversarial, skewed, correlated data and run it under a battery of forced
 * configurations -- sequential vs index vs bitmap scans, nested-loop vs hash
 * vs merge joins, hash vs sort aggregation, GEQO on/off, JIT on/off, parallel
 * on/off, tiny vs large work_mem (forcing/avoiding spill) -- and require every
 * configuration that executes to produce a byte-identical result digest.
 *
 * Unlike the metamorphic oracles (TLP/NoREC), which rewrite the *query* and so
 * are blind to a bug that corrupts all rewrites identically, this oracle holds
 * the query fixed and varies only the *execution strategy*.  A disagreement is
 * therefore unambiguous: two execution paths computed different answers for the
 * same query over the same data -- a server bug, not an oracle artifact.
 *
 * The configurations are applied with SET LOCAL inside a transaction so they
 * cannot leak between runs.  Results are compared by server-side digest
 * (pg_prop_sql_rowhash), so arbitrarily large results cost O(1) client memory.
 * Every query carries a total ORDER BY so "same rows" means "same digest".
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_diff"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/*
 * Each configuration is a bundle of SET LOCAL statements forcing a particular
 * class of plan.  "" means "planner's free choice" (the reference).  We
 * deliberately include settings that force expensive, less-exercised paths
 * (tiny work_mem -> external sort / batched hash spill; disabled hashing ->
 * sort/merge everywhere), because the spill and sort paths are where executor
 * bugs hide.
 */
static const char *const configs[] =
{
	"",							/* reference: planner chooses freely */
	"SET LOCAL enable_indexscan=off; SET LOCAL enable_indexonlyscan=off;"
	"SET LOCAL enable_bitmapscan=off;",
	"SET LOCAL enable_seqscan=off;",
	"SET LOCAL enable_hashjoin=off; SET LOCAL enable_mergejoin=off;",
	"SET LOCAL enable_nestloop=off; SET LOCAL enable_mergejoin=off;",
	"SET LOCAL enable_nestloop=off; SET LOCAL enable_hashjoin=off;",
	"SET LOCAL enable_hashagg=off;",
	"SET LOCAL enable_sort=off;",
	"SET LOCAL geqo=on; SET LOCAL geqo_threshold=2; SET LOCAL geqo_seed=0.3;",
	"SET LOCAL geqo=off;",
	"SET LOCAL work_mem='64kB';",
	"SET LOCAL work_mem='256MB';",
	"SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;"
	"SET LOCAL min_parallel_table_scan_size=0;"
	"SET LOCAL max_parallel_workers_per_gather=4;",
	"SET LOCAL max_parallel_workers_per_gather=0;",
	"SET LOCAL jit=on; SET LOCAL jit_above_cost=0; SET LOCAL jit_inline_above_cost=0;"
	"SET LOCAL jit_optimize_above_cost=0;",
	"SET LOCAL jit=off;",

	/*
	 * The planner's proof-based rewrites, each switched off on its own.  These
	 * rewrites delete or restructure work only when they can prove it
	 * redundant (a join to a uniquely-keyed self, a GROUP BY order, a Memoize
	 * cache hit), so a wrong proof is a wrong answer.  Turning one off gives
	 * the unrewritten plan to compare against.
	 */
	"SET LOCAL enable_self_join_elimination=off;",
	"SET LOCAL enable_memoize=off;",
	"SET LOCAL enable_material=off;",
	"SET LOCAL enable_incremental_sort=off;",
	"SET LOCAL enable_eager_aggregate=off;",
	"SET LOCAL enable_group_by_reordering=off; SET LOCAL enable_distinct_reordering=off;",
	"SET LOCAL enable_presorted_aggregate=off;",
	"SET LOCAL enable_groupagg=off;",
	"SET LOCAL enable_gathermerge=off; SET LOCAL enable_parallel_hash=off;"
	"SET LOCAL enable_parallel_append=off;",
	"SET LOCAL enable_partitionwise_join=on; SET LOCAL enable_partitionwise_aggregate=on;",
	"SET LOCAL enable_partition_pruning=off;",
	/* join-search limits: no subquery/join flattening into one search */
	"SET LOCAL from_collapse_limit=1; SET LOCAL join_collapse_limit=1;",
	/* every rewrite at once off, nested loops only: the most naive plan */
	"SET LOCAL enable_self_join_elimination=off; SET LOCAL enable_memoize=off;"
	"SET LOCAL enable_hashjoin=off; SET LOCAL enable_mergejoin=off;"
	"SET LOCAL enable_hashagg=off; SET LOCAL enable_eager_aggregate=off;"
	"SET LOCAL enable_incremental_sort=off; SET LOCAL enable_indexscan=off;"
	"SET LOCAL enable_bitmapscan=off; SET LOCAL enable_indexonlyscan=off;",
};
#define N_CONFIGS ((int) (sizeof(configs) / sizeof(configs[0])))

/*
 * Run "query" under configuration setup and return the result digest, or NULL
 * on error with *errored set.  A SQL error under a forced config is not itself
 * a bug (e.g. a value that overflows, or a config making a plan impossible);
 * only a *disagreement in results* among the configs that succeeded is.  Each
 * run is its own transaction so the SET LOCALs are scoped and discarded.
 */
static char *
digest_under(pg_prop_conn *conn, const char *setup, const char *query,
			 bool *errored, char **errmsg)
{
	char	   *dig;
	bool		failed = false;
	char	   *err = NULL;

	*errored = false;
	*errmsg = NULL;

	if (!pg_prop_sql_exec(conn, "BEGIN", &err))
	{
		*errored = true;
		*errmsg = err;
		return NULL;
	}
	if (setup[0] != '\0' && !pg_prop_sql_exec(conn, setup, &err))
	{
		pg_prop_sql_exec(conn, "ROLLBACK", NULL);
		*errored = true;
		*errmsg = err;
		return NULL;
	}
	dig = pg_prop_sql_rowhash(conn, query, &failed, &err);
	pg_prop_sql_exec(conn, "ROLLBACK", NULL);

	if (failed)
	{
		if (dig)
			free(dig);
		*errored = true;
		*errmsg = err;
		return NULL;
	}
	return dig;
}

/*
 * Errors a correct server may raise or not depending on the plan: arithmetic
 * overflow and division by zero are evaluated per row, so a plan that filters
 * first never evaluates the expression on the offending row.  Anything else --
 * an unknown column, a syntax error, an internal error -- means the query
 * itself is malformed (a generator bug) or the server failed, and is never
 * waved through as "vacuous".
 */
static bool
plan_dependent_error(const char *msg)
{
	return msg != NULL &&
		(strstr(msg, "out of range") != NULL ||
		 strstr(msg, "division by zero") != NULL ||
		 strstr(msg, "overflow") != NULL);
}

/*
 * The core differential property: take the reference digest (first config that
 * executes), then require every other config that executes to agree.
 */
static void
run_diff(pg_prop_tc *tc, pg_prop_conn *conn, const char *query)
{
	char	   *ref;
	char	   *err = NULL;
	bool		errored;
	int			i;

	/*
	 * The reference is config[0], the planner's free choice.  If it errors,
	 * the case is vacuous only for a plan-dependent runtime error; anything
	 * else fails the case so a malformed generated query cannot pass silently.
	 */
	ref = digest_under(conn, configs[0], query, &errored, &err);
	if (errored)
	{
		if (plan_dependent_error(err))
		{
			pg_free(err);
			return;
		}
		pg_prop_fail(tc, "reference query failed: %s\n  query: %s",
					 err ? err : "", query);
	}
	pg_free(err);

	for (i = 1; i < N_CONFIGS; i++)
	{
		char	   *d = digest_under(conn, configs[i], query, &errored, &err);

		if (errored)
		{
			/*
			 * A forced config may legitimately hit a runtime error the
			 * reference plan avoided (see plan_dependent_error).  Any other
			 * error under a config whose reference succeeded is a bug: the
			 * same query must not become invalid because of a planner GUC.
			 */
			if (!plan_dependent_error(err))
				pg_prop_fail(tc,
							 "config[%d] errored where the reference succeeded: %s\n"
							 "  config: %s\n  query: %s",
							 i, err ? err : "", configs[i], query);
			pg_free(err);
			continue;
		}
		pg_free(err);
		if (strcmp(ref, d) != 0)
		{
			char	   *rc = pstrdup(ref);
			char	   *dc = pstrdup(d);

			free(ref);
			free(d);
			pg_prop_fail(tc,
						 "two execution strategies disagree on the same query.\n"
						 "  config[0] (free choice) -> %s\n"
						 "  config[%d] %s -> %s\n"
						 "  query: %s",
						 rc, i, configs[i], dc, query);
		}
		free(d);
	}
	free(ref);
}

/*
 * Single-table differential: SELECT every column WHERE <rich predicate> over
 * the adversarial single table, totally ordered.
 */
PG_PROP(diff_single_table_filter)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			nrows;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	qg_build_table(tc, conn, &nrows);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT id, a, b, c, d, f, g FROM " QG_TABLE
						   " WHERE ");
	qg_draw_predicate(tc, &q);
	appendStringInfoString(&q, " ORDER BY id, a, b, c, d, f, g NULLS FIRST");

	run_diff(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Aggregation differential: GROUP BY with a spread of aggregates (hash-vs-sort
 * aggregation and parallel partial aggregation are the executor code most
 * likely to disagree), full aggregate tuple ordered.
 */
PG_PROP(diff_group_aggregate)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			nrows;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	qg_build_table(tc, conn, &nrows);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT c, count(*), count(a), sum(a), avg(b)::numeric(30,6), "
						   "min(d), max(d), bool_or(g), string_agg(c, ',' ORDER BY c) "
						   "FROM " QG_TABLE " WHERE ");
	qg_draw_predicate(tc, &q);
	appendStringInfoString(&q, " GROUP BY c ORDER BY c NULLS FIRST");

	run_diff(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Deep multi-table differential: a generated multi-way join over the related
 * database, every key projected, totally ordered.  This is where GEQO,
 * join-method forcing, and spill all interact at once.
 */
PG_PROP(diff_deep_join)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	qg_db_params p;
	qg_db	   *db;
	qg_query	q;
	StringInfoData whole;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_db_params_for_env(&p);
	db = qg_build_db(tc, conn, &p);

	if (!qg_build_query(tc, db, 4, &q))
	{
		qg_drop_db(tc, conn, db);
		pg_prop_disconnect(conn);
		return;					/* db too small; vacuous */
	}

	initStringInfo(&whole);
	appendStringInfo(&whole, "%s %s", q.select_from, q.order_by);

	run_diff(tc, conn, whole.data);

	pfree(whole.data);
	qg_free_query(&q);
	qg_drop_db(tc, conn, db);
	pg_prop_disconnect(conn);
}


/*
 * The name of the last projected key ("k0" for a one-table chain, "kN" for an
 * N+1-table chain).  Properties that need a second key use this rather than
 * hard-coding k1, which does not exist when the chain has one table.
 */
static const char *
last_key(const qg_query *q, char *buf, size_t len)
{
	const char *p = strrchr(q->keys, 'k');

	snprintf(buf, len, "%s", p ? p : "k0");
	return buf;
}

/*
 * Shared setup for the shapes below: build the related database and one deep
 * join query over it.  Returns false (case vacuous) if the database is too
 * small to join.
 */
static bool
deep_setup(pg_prop_tc *tc, pg_prop_conn **connp, qg_db **dbp, qg_query *q)
{
	char	   *err = NULL;
	qg_db_params p;

	*connp = pg_prop_connect(&err);
	if (*connp == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	qg_db_params_for_env(&p);
	*dbp = qg_build_db(tc, *connp, &p);
	if (!qg_build_query(tc, *dbp, 4, q))
	{
		qg_drop_db(tc, *connp, *dbp);
		pg_prop_disconnect(*connp);
		return false;
	}
	return true;
}

static void
deep_teardown(pg_prop_tc *tc, pg_prop_conn *conn, qg_db *db, qg_query *q)
{
	qg_free_query(q);
	qg_drop_db(tc, conn, db);
	pg_prop_disconnect(conn);
}

/*
 * Aggregation over a deep join, grouped by the anchor's key.  This is the
 * input eager aggregation (pushing a partial aggregate below a join) and
 * GROUP BY reordering act on, and where hash vs sort vs presorted aggregation
 * all apply.  count(kN) for each joined key counts the NULL-extended rows of
 * the LEFT JOINs differently from count(*), so a wrong join-null placement
 * shows up as a different count.
 */
PG_PROP(diff_deep_aggregate)
{
	pg_prop_conn *conn;
	qg_db	   *db;
	qg_query	q;
	StringInfoData sql;
	StringInfoData aggs;
	char	   *keys;
	char	   *tok;
	char	   *save = NULL;
	char		lk[16];

	if (!deep_setup(tc, &conn, &db, &q))
		return;

	/* one count(kN) per projected key: "count(k0), count(k1), ..." */
	initStringInfo(&aggs);
	keys = pstrdup(q.keys);
	for (tok = strtok_r(keys, ", ", &save); tok; tok = strtok_r(NULL, ", ", &save))
		appendStringInfo(&aggs, "%scount(%s)", aggs.len ? ", " : "", tok);
	pfree(keys);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT k0, count(*), %s, count(DISTINCT %s) "
					 "FROM (%s) j GROUP BY k0 ORDER BY k0 NULLS FIRST",
					 aggs.data, last_key(&q, lk, sizeof(lk)), q.select_from);
	run_diff(tc, conn, sql.data);

	pfree(aggs.data);
	pfree(sql.data);
	deep_teardown(tc, conn, db, &q);
}

/*
 * Semijoin / antijoin over the deep join: rows of the anchor table that do (or
 * do not) have a match in the join, written with EXISTS, NOT EXISTS, IN and
 * NOT IN.  The planner turns these into semi/anti joins (and right-semi /
 * right-anti hash joins, unique-ified inner rels, ...), so each form exercises
 * a different conversion while the answer is fixed by the data.
 */
PG_PROP(diff_semi_anti_join)
{
	pg_prop_conn *conn;
	qg_db	   *db;
	qg_query	q;
	StringInfoData sql;
	const char *anchor;
	int			form;
	char		lk[16];

	if (!deep_setup(tc, &conn, &db, &q))
		return;
	last_key(&q, lk, sizeof(lk));

	anchor = q.pred_table->name;
	form = (int) pg_prop_draw_int(tc, 0, 3);
	initStringInfo(&sql);
	switch (form)
	{
		case 0:
			appendStringInfo(&sql,
							 "SELECT a.id FROM %s a WHERE EXISTS "
							 "(SELECT 1 FROM (%s) j WHERE j.k0 = a.id) ORDER BY a.id",
							 anchor, q.select_from);
			break;
		case 1:
			appendStringInfo(&sql,
							 "SELECT a.id FROM %s a WHERE NOT EXISTS "
							 "(SELECT 1 FROM (%s) j WHERE j.%s = a.id) ORDER BY a.id",
							 anchor, q.select_from, lk);
			break;
		case 2:
			appendStringInfo(&sql,
							 "SELECT a.id FROM %s a WHERE a.id IN "
							 "(SELECT j.k0 FROM (%s) j) ORDER BY a.id",
							 anchor, q.select_from);
			break;
		default:
			/* NOT IN with a possibly-NULL subquery column: three-valued */
			appendStringInfo(&sql,
							 "SELECT a.id FROM %s a WHERE a.id NOT IN "
							 "(SELECT j.%s FROM (%s) j) ORDER BY a.id",
							 anchor, lk, q.select_from);
			break;
	}
	run_diff(tc, conn, sql.data);

	pfree(sql.data);
	deep_teardown(tc, conn, db, &q);
}

/*
 * Window functions over the deep join: a partitioned running count, row
 * numbers, and a lag over a total order.  Window evaluation runs above the
 * join, so its input order and partitioning come from whatever plan produced
 * the join, and incremental sort / presorted paths feed it differently.
 */
PG_PROP(diff_deep_window)
{
	pg_prop_conn *conn;
	qg_db	   *db;
	qg_query	q;
	StringInfoData sql;

	if (!deep_setup(tc, &conn, &db, &q))
		return;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT %s, "
					 "count(*) OVER (PARTITION BY k0), "
					 "row_number() OVER (PARTITION BY k0 ORDER BY %s), "
					 "lag(k0) OVER (ORDER BY %s) "
					 "FROM (%s) j %s",
					 q.keys, q.keys, q.keys, q.select_from, q.order_by);
	run_diff(tc, conn, sql.data);

	pfree(sql.data);
	deep_teardown(tc, conn, db, &q);
}

/*
 * Deep join filtered by a generated predicate on the anchor.  The predicate
 * grammar (IS DISTINCT FROM, = ANY / <> ALL over arrays holding NULL,
 * cross-type casts, NULL tests) is where the planner's nullability and
 * strictness reasoning runs: outer-join reduction, qual pushdown below or
 * above a LEFT JOIN, and index-qual matching all depend on proving a
 * predicate strict.  A wrong proof changes the rows.
 */
PG_PROP(diff_deep_filtered)
{
	pg_prop_conn *conn;
	qg_db	   *db;
	qg_query	q;
	StringInfoData pred;
	StringInfoData sql;

	if (!deep_setup(tc, &conn, &db, &q))
		return;

	initStringInfo(&pred);
	qg_draw_predicate_on(tc, &pred, q.pred_table, q.pred_alias);
	initStringInfo(&sql);
	appendStringInfo(&sql, "%s WHERE %s %s", q.select_from, pred.data, q.order_by);
	run_diff(tc, conn, sql.data);

	pfree(pred.data);
	pfree(sql.data);
	deep_teardown(tc, conn, db, &q);
}
