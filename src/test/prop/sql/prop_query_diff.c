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
			 bool *errored)
{
	char	   *dig;
	bool		failed = false;
	char	   *err = NULL;

	*errored = false;

	if (!pg_prop_sql_exec(conn, "BEGIN", &err))
	{
		if (err)
			free(err);
		*errored = true;
		return NULL;
	}
	if (setup[0] != '\0' && !pg_prop_sql_exec(conn, setup, &err))
	{
		if (err)
			free(err);
		pg_prop_sql_exec(conn, "ROLLBACK", NULL);
		*errored = true;
		return NULL;
	}
	dig = pg_prop_sql_rowhash(conn, query, &failed, &err);
	pg_prop_sql_exec(conn, "ROLLBACK", NULL);

	if (failed)
	{
		if (err)
			free(err);
		if (dig)
			free(dig);
		*errored = true;
		return NULL;
	}
	return dig;
}

/*
 * The core differential property: take the reference digest (first config that
 * executes), then require every other config that executes to agree.
 */
static void
run_diff(pg_prop_tc *tc, pg_prop_conn *conn, const char *query)
{
	char	   *ref = NULL;
	bool		errored;
	int			i;
	int			ref_cfg = -1;

	for (i = 0; i < N_CONFIGS; i++)
	{
		ref = digest_under(conn, configs[i], query, &errored);
		if (!errored && ref != NULL)
		{
			ref_cfg = i;
			break;
		}
		if (ref)
		{
			free(ref);
			ref = NULL;
		}
	}
	if (ref == NULL)
		return;					/* query errors under every config: vacuous */

	for (i = ref_cfg + 1; i < N_CONFIGS; i++)
	{
		char	   *d = digest_under(conn, configs[i], query, &errored);

		if (errored || d == NULL)
		{
			if (d)
				free(d);
			continue;			/* config not applicable; skip */
		}
		if (strcmp(ref, d) != 0)
		{
			char	   *rc = pstrdup(ref);
			char	   *dc = pstrdup(d);

			free(ref);
			free(d);
			pg_prop_fail(tc,
						 "two execution strategies disagree on the same query.\n"
						 "  config[%d] %s -> %s\n"
						 "  config[%d] %s -> %s\n"
						 "  query: %s",
						 ref_cfg, configs[ref_cfg][0] ? configs[ref_cfg] : "(free choice)", rc,
						 i, configs[i], dc,
						 query);
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
