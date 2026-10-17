/*-------------------------------------------------------------------------
 *
 * prop_cf_planner.c
 *	  Plan differentials around open commitfest wrong-results bugs.
 *
 * Every property here draws a small schema and a query shape around the
 * trigger of one CF bug fix, then runs the same query under two settings
 * that may only change the plan (a planner GUC, an OFFSET 0 fence) and
 * requires the result digests to agree.  Each run is its own transaction
 * (SET LOCAL), the query carries an ORDER BY on a total key, and the
 * digest is md5 of the rendered rows.  Tables are temp tables, so they
 * vanish with the per-case connection even when a case fails.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_cf_planner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cf_planner"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* last schema/data setup of this case, printed with a counterexample */
static const char *cfp_last_setup = "";

static pg_prop_conn *
cfp_connect(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	cfp_last_setup = "";

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

static void
cfp_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "setup failed: %s\n  SQL: %s", err ? err : "", sql);
}

/* schema/data setup: like cfp_exec, and remembered for the failure message */
static void
cfp_setup(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	cfp_last_setup = sql;
	cfp_exec(tc, conn, sql);
}

/*
 * Run "query" in its own transaction after "setup" (SET LOCALs) and return
 * the result digest.  Any error fails the case: these queries are built so
 * that no plan can raise a data-dependent error.
 */
static char *
cfp_digest(pg_prop_tc *tc, pg_prop_conn *conn, const char *setup,
		   const char *query)
{
	char	   *err = NULL;
	bool		failed = false;
	char	   *dig;

	cfp_exec(tc, conn, "BEGIN");
	if (setup[0] != '\0')
		cfp_exec(tc, conn, setup);
	dig = pg_prop_sql_rowhash(conn, query, &failed, &err);
	pg_prop_sql_exec(conn, "ROLLBACK", NULL);
	if (failed || dig == NULL)
		pg_prop_fail(tc, "query failed: %s\n  setup: %s\n  query: %s",
					 err ? err : "", setup, query);
	return dig;
}

/* the first rows of "query" under "setup", for the failure message */
static char *
cfp_sample(pg_prop_conn *conn, const char *setup, const char *query)
{
	StringInfoData q;
	char	   *rows;
	bool		failed = false;

	pg_prop_sql_exec(conn, "BEGIN", NULL);
	if (setup[0] != '\0')
		pg_prop_sql_exec(conn, setup, NULL);
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT * FROM (%s) s LIMIT 30", query);
	rows = pg_prop_sql_rows(conn, q.data, &failed, NULL);
	pg_prop_sql_exec(conn, "ROLLBACK", NULL);
	pfree(q.data);
	return rows ? rows : pg_strdup("(unavailable)");
}

/*
 * The differential: "qa" under "sa" and "qb" under "sb" must return the same
 * rows in the same order.  qb may be NULL (same query as qa).
 */
static void
cfp_diff(pg_prop_tc *tc, pg_prop_conn *conn, const char *what,
		 const char *sa, const char *qa, const char *sb, const char *qb)
{
	char	   *da;
	char	   *db;

	if (qb == NULL)
		qb = qa;
	da = cfp_digest(tc, conn, sa, qa);
	db = cfp_digest(tc, conn, sb, qb);
	if (strcmp(da, db) != 0)
		pg_prop_fail(tc,
					 "%s: two plans of the same query disagree\n"
					 "  data: %s\n"
					 "  A setup: %s\n  A query: %s\n  A rows:\n%s\n"
					 "  B setup: %s\n  B query: %s\n  B rows:\n%s",
					 what, cfp_last_setup, sa, qa, cfp_sample(conn, sa, qa),
					 sb, qb, cfp_sample(conn, sb, qb));
	free(da);
	free(db);
}

/* nested-loop-only plan shape that makes Memoize the cheap inner path */
#define CFP_NESTLOOP_ONLY \
	"SET LOCAL enable_seqscan = off; SET LOCAL enable_material = off; " \
	"SET LOCAL enable_mergejoin = off; SET LOCAL enable_hashjoin = off; "

/*
 * CF 7175: Memoize whose cache key is an expression containing an outer
 * Param (t2.x + t0.p) while the same Param also filters the inner scan
 * elsewhere (t1.y = t0.p).  Two (t2.x, t0.p) pairs with the same key value
 * select different inner rows, but master keeps the Param in keyparamids and
 * never flushes the cache, so one entry serves both.
 *
 * Drawn: table size, column moduli, the key expression (+, -, *), the shape
 * of the extra use of the Param (=, <, <> on another column), and how many
 * outer rows drive the correlated subquery.  Invariant: enable_memoize only
 * changes the plan, so on and off must give the same per-outer-row counts.
 */
PG_PROP(memoize_buried_param_matches_no_memoize)
{
	static const char *const keyop[] = {"+", "-", "*"};
	static const char *const filt[] = {"=", "<", "<>", ">="};
	pg_prop_conn *conn = cfp_connect(tc);
	StringInfoData s;
	StringInfoData q;
	int			n = (int) pg_prop_draw_int(tc, 20, 2000);
	int			ma = (int) pg_prop_draw_int(tc, 1, 40);
	int			mb = (int) pg_prop_draw_int(tc, 1, 40);
	int			mc = (int) pg_prop_draw_int(tc, 1, 40);
	int			nout = (int) pg_prop_draw_int(tc, 2, 200);
	const char *op = keyop[pg_prop_draw_int(tc, 0, lengthof(keyop) - 1)];
	const char *fo = filt[pg_prop_draw_int(tc, 0, lengthof(filt) - 1)];
	bool		useb = pg_prop_draw_bool(tc);
	bool		agg = pg_prop_draw_bool(tc);
	bool		filtb = pg_prop_draw_bool(tc);

	initStringInfo(&s);
	appendStringInfo(&s,
					 "CREATE TEMP TABLE cfa1_m AS SELECT g AS id, g %% %d AS a, "
					 "g %% %d AS b, (g / 3) %% %d AS c FROM generate_series(0, %d) g; "
					 "CREATE INDEX ON cfa1_m (id); ANALYZE cfa1_m",
					 ma, mb, mc, n - 1);
	cfp_setup(tc, conn, s.data);

	/*
	 * Outer: t0 (rows id < nout).  Subquery: t2 drives a nested loop whose
	 * inner side is an index scan on t1.id = <t2.a op t0.P> with filter
	 * t1.Q fo t0.P, where P is the outer column buried in the key.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT %s FROM (SELECT t0.id, "
					 "(SELECT count(*) FROM cfa1_m t2 JOIN cfa1_m t1 "
					 "ON t1.id = t2.a %s t0.%s WHERE t1.%s %s t0.%s) AS c "
					 "FROM cfa1_m t0 WHERE t0.id < %d) s %s",
					 agg ? "sum(c)" : "id, c",
					 op, useb ? "b" : "c",
					 filtb ? "b" : "c", fo, useb ? "b" : "c",
					 nout, agg ? "" : "ORDER BY id");

	cfp_diff(tc, conn, "CF 7175 Memoize with a Param buried in the cache key",
			 CFP_NESTLOOP_ONLY, q.data,
			 CFP_NESTLOOP_ONLY "SET LOCAL enable_memoize = off;", NULL);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* append "INSERT INTO <tab> VALUES" with n rows of ncol ints in [0, dom], some NULL */
static void
cfp_rows(pg_prop_tc *tc, StringInfo b, const char *tab, int n, int ncol,
		 int dom, bool serial_first)
{
	appendStringInfo(b, "INSERT INTO %s VALUES ", tab);
	for (int i = 0; i < n; i++)
	{
		appendStringInfoString(b, i ? ", (" : "(");
		for (int c = 0; c < ncol; c++)
		{
			if (c > 0)
				appendStringInfoString(b, ", ");
			if (c == 0 && serial_first)
				appendStringInfo(b, "%d", i + 1);
			else if (pg_prop_draw_int(tc, 0, 9) == 0)
				appendStringInfoString(b, "NULL");
			else
				appendStringInfo(b, "%d", (int) pg_prop_draw_int(tc, 0, dom));
		}
		appendStringInfoChar(b, ')');
	}
	appendStringInfoString(b, "; ");
}

/*
 * CF 7384: a hashed SubPlan correlated to an outer query level, inside a join
 * OR clause from which extract_restriction_or_clauses() pulls a restriction on
 * one side.  The same SubPlan then sits in two quals with two SubPlanStates
 * sharing one PlanState; the first to rebuild its hash table clears chgParam,
 * and the other keeps probing a table built for an earlier outer row.
 *
 *	 SELECT s.id, EXISTS (SELECT 1 FROM p JOIN c ON c.id = p.cid
 *						  WHERE p.id = s.pid AND
 *								(c.owner = s.uid OR (p.pub AND c.K [NOT] IN
 *									(SELECT f.cid FROM f WHERE f.uid = s.uid))))
 *	 FROM s ORDER BY s.id
 *
 * Reference: the same query with "c.K IN (SELECT ...)" written
 * "c.K = ANY (ARRAY(SELECT ...))" (NOT IN as "<> ALL (ARRAY(...))").  Both
 * are the same ANY/ALL test with the same NULL semantics, but an ARRAY
 * sublink is never hashed.  Drawn: table contents (small domains, NULLs),
 * which c column is tested, IN vs NOT IN, EXISTS vs count(*), the plan
 * shape GUCs, and an extra OR arm.
 */
PG_PROP(hashed_subplan_rescan_matches_array_any)
{
	static const char *const setups[] = {
		"",
		"SET LOCAL enable_hashjoin = off; SET LOCAL enable_mergejoin = off;",
		"SET LOCAL enable_nestloop = off;",
		"SET LOCAL enable_indexscan = off; SET LOCAL enable_bitmapscan = off;",
	};
	pg_prop_conn *conn = cfp_connect(tc);
	StringInfoData s;
	StringInfoData qa;
	StringInfoData qb;
	int			dom = (int) pg_prop_draw_int(tc, 1, 12);
	int			nc = (int) pg_prop_draw_int(tc, 1, 12);
	int			np = (int) pg_prop_draw_int(tc, 1, 20);
	int			nf = (int) pg_prop_draw_int(tc, 0, 20);
	int			ns = (int) pg_prop_draw_int(tc, 1, 20);
	bool		notin = pg_prop_draw_int(tc, 0, 3) == 0;
	bool		cnt = pg_prop_draw_bool(tc);
	bool		extra = pg_prop_draw_bool(tc);
	const char *key = pg_prop_draw_bool(tc) ? "id" : "owner";
	const char *setup = setups[pg_prop_draw_int(tc, 0, lengthof(setups) - 1)];

	initStringInfo(&s);
	appendStringInfoString(&s,
						   "CREATE TEMP TABLE cfa1_hc (id int PRIMARY KEY, owner int); "
						   "CREATE TEMP TABLE cfa1_hp (id int PRIMARY KEY, cid int, pub bool); "
						   "CREATE TEMP TABLE cfa1_hf (cid int, uid int); "
						   "CREATE TEMP TABLE cfa1_hs (id int PRIMARY KEY, pid int, uid int); ");
	cfp_rows(tc, &s, "cfa1_hc", nc, 2, dom, true);
	appendStringInfoString(&s, "INSERT INTO cfa1_hp VALUES ");
	for (int i = 0; i < np; i++)
		appendStringInfo(&s, "%s(%d, %d, %s)", i ? ", " : "", i + 1,
						 (int) pg_prop_draw_int(tc, 0, nc + 1),
						 pg_prop_draw_int(tc, 0, 3) ? "true" : "false");
	appendStringInfoString(&s, "; ");
	if (nf > 0)
		cfp_rows(tc, &s, "cfa1_hf", nf, 2, dom, false);
	cfp_rows(tc, &s, "cfa1_hs", ns, 3, np + 1, true);
	appendStringInfoString(&s, "ANALYZE cfa1_hc, cfa1_hp, cfa1_hf, cfa1_hs");
	cfp_setup(tc, conn, s.data);

	initStringInfo(&qa);
	initStringInfo(&qb);
	for (int v = 0; v < 2; v++)
	{
		StringInfo	q = v == 0 ? &qa : &qb;

		appendStringInfo(q,
						 "SELECT s.id, %s FROM cfa1_hp p JOIN cfa1_hc c ON c.id = p.cid "
						 "WHERE p.id = s.pid AND (c.owner = s.uid %s OR (p.pub AND c.%s ",
						 cnt ? "(SELECT count(*)" : "EXISTS (SELECT 1",
						 extra ? "OR (NOT p.pub AND c.owner = s.pid)" : "",
						 key);
		if (v == 0)
			appendStringInfo(q, "%s (SELECT f.cid FROM cfa1_hf f WHERE f.uid = s.uid)",
							 notin ? "NOT IN" : "IN");
		else
			appendStringInfo(q, "%s (ARRAY(SELECT f.cid FROM cfa1_hf f WHERE f.uid = s.uid))",
							 notin ? "<> ALL" : "= ANY");
		appendStringInfoString(q, "))) AS r FROM cfa1_hs s ORDER BY s.id");
	}

	cfp_diff(tc, conn, "CF 7384 duplicated hashed SubPlan rescan",
			 setup, qa.data, setup, qb.data);

	pfree(s.data);
	pfree(qa.data);
	pfree(qb.data);
	pg_prop_disconnect(conn);
}

/*
 * Append a SQL string literal of 1..maxlen letters drawn from "alpha" (ASCII
 * only).  With "accents", 'a'/'A' may also come out as U+00E1/U+00C1, so
 * case- and accent-variants of one word collide under a level-1 collation.
 */
static void
cfp_word(pg_prop_tc *tc, StringInfo b, const char *alpha, int maxlen,
		 bool accents)
{
	size_t		len;
	char	   *w = pg_prop_draw_from(tc, alpha, 1, maxlen, &len);

	appendStringInfoChar(b, '\'');
	for (size_t i = 0; i < len; i++)
	{
		if (accents && (w[i] == 'a' || w[i] == 'A') && pg_prop_draw_bool(tc))
			appendStringInfoString(b, w[i] == 'a' ? "\xc3\xa1" : "\xc3\x81");
		else
			appendStringInfoChar(b, w[i]);
	}
	appendStringInfoChar(b, '\'');
	free(w);
}

/*
 * CF 7421: Memoize keyed on an outer expression whose collation is
 * nondeterministic while the join clause compares under a different
 * (deterministic) collation, e.g. i.x COLLATE "C" = o.x with o.x
 * case-insensitive.  Master hashes/compares cache keys with the outer
 * expression's collation, so 'ab' and 'AB' share one cache entry although
 * the clause tells them apart: wrong LEFT JOIN rows, wrong EXISTS / NOT
 * EXISTS (PG19 semi/anti-join Memoize).
 *
 * Drawn: the nondeterministic collation (case-, or case+accent-
 * insensitive), whether the outer collation comes from the column or a
 * COLLATE in a derived table, the clause collation, which side carries the
 * COLLATE, the alphabet and lengths of the strings (so case/accent
 * variants collide), table sizes, and the join shape.  Invariant:
 * enable_memoize only changes the plan.
 */
PG_PROP(memoize_collation_matches_no_memoize)
{
	static const char *const ndcoll[] = {
		"und-u-ks-level2",		/* case-insensitive */
		"und-u-ks-level1",		/* case- and accent-insensitive */
	};
	static const char *const dcoll[] = {"\"C\"", "\"POSIX\"", "\"und-x-icu\""};
	static const char *const alpha[] = {"aAbB", "aA", "abAB "};
	pg_prop_conn *conn = cfp_connect(tc);
	StringInfoData s;
	StringInfoData q;
	const char *nd = ndcoll[pg_prop_draw_int(tc, 0, lengthof(ndcoll) - 1)];
	const char *dc = dcoll[pg_prop_draw_int(tc, 0, lengthof(dcoll) - 1)];
	const char *al = alpha[pg_prop_draw_int(tc, 0, lengthof(alpha) - 1)];
	bool		accents = pg_prop_draw_bool(tc);
	int			maxlen = (int) pg_prop_draw_int(tc, 1, 3);
	int			no = (int) pg_prop_draw_int(tc, 5, 300);
	int			ni = (int) pg_prop_draw_int(tc, 1, 30);
	int			nfill = (int) pg_prop_draw_int(tc, 0, 2000);
	bool		colcoll = pg_prop_draw_bool(tc);	/* ci on the column */
	bool		inner_side = pg_prop_draw_bool(tc); /* COLLATE on inner side */
	int			shape = (int) pg_prop_draw_int(tc, 0, 3);
	const char *orel;
	const char *clause;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "CREATE COLLATION pg_temp.cfa1_nd (provider = icu, locale = '%s', "
					 "deterministic = false); "
					 "CREATE TEMP TABLE cfa1_co (id int, x text%s); "
					 "CREATE TEMP TABLE cfa1_cs (id int, x text COLLATE %s); "
					 "INSERT INTO cfa1_co VALUES ",
					 nd, colcoll ? " COLLATE pg_temp.cfa1_nd" : "", dc);
	for (int i = 0; i < no; i++)
	{
		appendStringInfo(&s, "%s(%d, ", i ? ", " : "", i);
		cfp_word(tc, &s, al, maxlen, accents);
		appendStringInfoChar(&s, ')');
	}
	appendStringInfoString(&s, "; INSERT INTO cfa1_cs VALUES ");
	for (int i = 0; i < ni; i++)
	{
		appendStringInfo(&s, "%s(%d, ", i ? ", " : "", i);
		cfp_word(tc, &s, al, maxlen, accents);
		appendStringInfoChar(&s, ')');
	}
	appendStringInfo(&s, "; INSERT INTO cfa1_cs SELECT g, 'z' || g "
					 "FROM generate_series(%d, %d) g; "
					 "CREATE INDEX ON cfa1_cs (x); ANALYZE cfa1_co, cfa1_cs",
					 ni, ni + nfill - 1);
	cfp_setup(tc, conn, s.data);

	/*
	 * The outer key o.x is nondeterministic, either by the column's collation
	 * or implicitly through a derived table's COLLATE.
	 */
	orel = colcoll ? "cfa1_co o" :
		"(SELECT id, x COLLATE pg_temp.cfa1_nd AS x FROM cfa1_co) o";

	/*
	 * The clause: compare under the deterministic collation.  With the
	 * COLLATE on the inner side the outer key keeps its nondeterministic
	 * collation (the bug); on the outer side the key carries the clause's
	 * collation (control).
	 */
	initStringInfo(&q);
	if (inner_side)
		appendStringInfo(&q, "i.x COLLATE %s = o.x", dc);
	else
		appendStringInfo(&q, "i.x = o.x COLLATE %s", dc);
	clause = pstrdup(q.data);
	resetStringInfo(&q);

	switch (shape)
	{
		case 0:
			appendStringInfo(&q,
							 "SELECT o.id, i.id FROM %s LEFT JOIN cfa1_cs i ON %s "
							 "ORDER BY 1, 2", orel, clause);
			break;
		case 1:
			appendStringInfo(&q,
							 "SELECT o.id FROM %s WHERE EXISTS "
							 "(SELECT 1 FROM cfa1_cs i WHERE %s) ORDER BY 1", orel, clause);
			break;
		case 2:
			appendStringInfo(&q,
							 "SELECT o.id FROM %s WHERE NOT EXISTS "
							 "(SELECT 1 FROM cfa1_cs i WHERE %s) ORDER BY 1", orel, clause);
			break;
		default:
			appendStringInfo(&q,
							 "SELECT o.id, i.id FROM %s JOIN cfa1_cs i ON %s "
							 "ORDER BY 1, 2", orel, clause);
			break;
	}

	cfp_diff(tc, conn, "CF 7421 Memoize cache key collation",
			 CFP_NESTLOOP_ONLY, q.data,
			 CFP_NESTLOOP_ONLY "SET LOCAL enable_memoize = off;", NULL);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A SQL/JSON query expression over context item CTX with PASSING argument
 * ARG, as a format with two %s (CTX, ARG), and text output.  None of these
 * is strict in ARG: a NULL PASSING value is a JSON null variable, and the
 * result does not depend on it at all in several of them.
 */
static const char *const cfp_jsonexpr[] = {
	"JSON_VALUE(%s, '$' PASSING %s AS p)",
	"JSON_VALUE(%s, 'strict $.a' PASSING %s AS p DEFAULT 'd' ON ERROR)",
	"JSON_VALUE(%s, '$.a' PASSING %s AS p DEFAULT 'd' ON EMPTY)",
	"JSON_EXISTS(%s, '$ ? ($p == null)' PASSING %s AS p)::text",
	"JSON_EXISTS(%s, '$ ? (@ == $p)' PASSING %s AS p)::text",
	"JSON_EXISTS(%s, '$.a' PASSING %s AS p)::text",
	"JSON_QUERY(%s, '$' PASSING %s AS p WITH WRAPPER)::text",
	"JSON_QUERY(%s, '$.a' PASSING %s AS p EMPTY OBJECT ON EMPTY)::text",
	"JSON_QUERY(%s, '$ ? (@ != $p)' PASSING %s AS p WITH CONDITIONAL WRAPPER)::text",
};

static const char *const cfp_jsonctx[] = {
	"jsonb '1'", "jsonb '{\"a\": 2}'", "jsonb '[1, null]'", "jsonb 'null'",
	"jsonb '{}'", "jsonb '\"s\"'",
};

/* append one drawn SQL/JSON expression with the given PASSING arg; ctx may be a column */
static void
cfp_draw_jsonexpr(pg_prop_tc *tc, StringInfo b, const char *arg,
				  const char *ctxcol)
{
	const char *fmt = cfp_jsonexpr[pg_prop_draw_int(tc, 0, lengthof(cfp_jsonexpr) - 1)];
	const char *ctx = (ctxcol && pg_prop_draw_int(tc, 0, 3) == 0) ? ctxcol :
		cfp_jsonctx[pg_prop_draw_int(tc, 0, lengthof(cfp_jsonctx) - 1)];

	appendStringInfo(b, fmt, ctx, arg);
}

/*
 * CF 7186, outer-join pull-up: a subquery whose target list holds a SQL/JSON
 * query function with a PASSING argument is pulled up under an outer join.
 * Master thinks JsonExpr is strict, so pullup_replace_vars() wraps no
 * PlaceHolderVar around it and evaluates it above the join, giving a non-NULL
 * value for null-extended rows.  Reference: the same query with OFFSET 0 in
 * the subquery, which blocks pull-up.
 *
 * Drawn: the JsonExpr (kind, path, ON EMPTY/ERROR, context item constant or
 * column), the join kind (LEFT / FULL / RIGHT) and condition (false, key
 * equality, equality plus a filter), and the table contents (with NULLs).
 */
PG_PROP(jsonexpr_pullup_matches_offset0_fence)
{
	static const char *const joins[] = {"LEFT JOIN", "FULL JOIN", "RIGHT JOIN"};
	static const char *const conds[] = {
		"false", "a.k = ss.y", "a.k = ss.y AND a.k > 1", "a.k IS NOT DISTINCT FROM ss.y",
	};
	pg_prop_conn *conn = cfp_connect(tc);
	StringInfoData s;
	StringInfoData e;
	StringInfoData qa;
	StringInfoData qb;
	int			na = (int) pg_prop_draw_int(tc, 1, 10);
	int			nb = (int) pg_prop_draw_int(tc, 1, 10);
	const char *jn = joins[pg_prop_draw_int(tc, 0, lengthof(joins) - 1)];
	const char *cond = conds[pg_prop_draw_int(tc, 0, lengthof(conds) - 1)];
	bool		wrap = pg_prop_draw_bool(tc);
	bool		lateral = pg_prop_draw_int(tc, 0, 3) == 0;

	initStringInfo(&s);
	appendStringInfoString(&s,
						   "CREATE TEMP TABLE cfa1_ja (k int); "
						   "CREATE TEMP TABLE cfa1_jb (y int, j jsonb); ");
	cfp_rows(tc, &s, "cfa1_ja", na, 1, 4, false);
	appendStringInfoString(&s, "INSERT INTO cfa1_jb VALUES ");
	for (int i = 0; i < nb; i++)
	{
		int			y = (int) pg_prop_draw_int(tc, -1, 4);

		appendStringInfo(&s, "%s(", i ? ", " : "");
		if (y < 0)
			appendStringInfoString(&s, "NULL");
		else
			appendStringInfo(&s, "%d", y);
		appendStringInfo(&s, ", %s)",
						 pg_prop_draw_int(tc, 0, 3) == 0 ? "NULL" :
						 cfp_jsonctx[pg_prop_draw_int(tc, 0, lengthof(cfp_jsonctx) - 1)]);
	}
	appendStringInfoString(&s, "; ANALYZE cfa1_ja, cfa1_jb");
	cfp_setup(tc, conn, s.data);

	/* the expression, optionally inside a strict operator */
	initStringInfo(&e);
	if (wrap)
		appendStringInfoString(&e, "'<' || ");
	cfp_draw_jsonexpr(tc, &e, lateral && pg_prop_draw_bool(tc) ? "a.k" : "y", "j");
	if (wrap)
		appendStringInfoString(&e, " || '>'");

	/*
	 * FULL JOIN needs a hash- or merge-joinable condition; "a.k > 1" and IS
	 * NOT DISTINCT FROM are not, and the server rightly rejects them.  (The
	 * outcome is plan-dependent: the patched planner keeps a qual that the
	 * buggy one dropped, so only the patched server raised the error.)
	 */
	if (strcmp(jn, "FULL JOIN") == 0 && strcmp(cond, "false") != 0)
		cond = "a.k = ss.y";

	initStringInfo(&qa);
	initStringInfo(&qb);
	for (int v = 0; v < 2; v++)
	{
		StringInfo	q = v == 0 ? &qa : &qb;

		if (lateral)
		{
			/* LATERAL only allows LEFT JOIN with a reference to a */
			appendStringInfo(q,
							 "SELECT a.k, ss.y, ss.v FROM cfa1_ja a LEFT JOIN LATERAL "
							 "(SELECT y, %s AS v FROM cfa1_jb%s) ss ON %s "
							 "ORDER BY 1 NULLS FIRST, 2 NULLS FIRST, 3 NULLS FIRST",
							 e.data, v ? " OFFSET 0" : "", cond);
		}
		else
			appendStringInfo(q,
							 "SELECT a.k, ss.y, ss.v FROM cfa1_ja a %s "
							 "(SELECT y, %s AS v FROM cfa1_jb%s) ss ON %s "
							 "ORDER BY 1 NULLS FIRST, 2 NULLS FIRST, 3 NULLS FIRST",
							 jn, e.data, v ? " OFFSET 0" : "", cond);
	}

	cfp_diff(tc, conn, "CF 7186 JsonExpr pulled up under an outer join",
			 "", qa.data, "", qb.data);

	pfree(s.data);
	pfree(e.data);
	pfree(qa.data);
	pfree(qb.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7186, inlining: a STRICT SQL function whose body is a SQL/JSON query
 * function of its arguments.  Master inlines it (believing the body strict),
 * which drops the strictness check, so a NULL argument yields non-NULL.
 * Invariant (definition of STRICT): the call is NULL whenever an argument is
 * NULL.  Drawn: the JsonExpr, whether the context item is the jsonb argument
 * or a constant, a strict wrapper, and the argument values.
 */
PG_PROP(strict_sql_function_with_jsonexpr_returns_null_on_null)
{
	pg_prop_conn *conn = cfp_connect(tc);
	StringInfoData s;
	char	   *v;
	bool		failed = false;
	char	   *err = NULL;
	bool		wrap = pg_prop_draw_bool(tc);
	int			nb = (int) pg_prop_draw_int(tc, 1, 10);

	initStringInfo(&s);
	appendStringInfoString(&s,
						   "CREATE FUNCTION pg_temp.cfa1_jf(int, jsonb) RETURNS text "
						   "STRICT IMMUTABLE LANGUAGE sql AS $$ SELECT ");
	if (wrap)
		appendStringInfoString(&s, "'<' || ");
	cfp_draw_jsonexpr(tc, &s, "$1", "$2");
	if (wrap)
		appendStringInfoString(&s, " || '>'");
	appendStringInfoString(&s, " $$; CREATE TEMP TABLE cfa1_jb (y int, j jsonb); "
						   "INSERT INTO cfa1_jb VALUES (NULL, NULL), (1, NULL), (NULL, '1')");
	for (int i = 0; i < nb; i++)
	{
		int			y = (int) pg_prop_draw_int(tc, -1, 4);

		if (y < 0)
			appendStringInfoString(&s, ", (NULL, ");
		else
			appendStringInfo(&s, ", (%d, ", y);
		appendStringInfo(&s, "%s)",
						 pg_prop_draw_int(tc, 0, 3) == 0 ? "NULL" :
						 cfp_jsonctx[pg_prop_draw_int(tc, 0, lengthof(cfp_jsonctx) - 1)]);
	}
	cfp_setup(tc, conn, s.data);

	v = pg_prop_sql_scalar(conn,
						   "SELECT count(*) FROM cfa1_jb "
						   "WHERE (y IS NULL OR j IS NULL) AND pg_temp.cfa1_jf(y, j) IS NOT NULL",
						   &failed, &err);
	if (failed || v == NULL)
		pg_prop_fail(tc, "query failed: %s\n  data: %s", err ? err : "", s.data);
	if (strcmp(v, "0") != 0)
	{
		char	   *rows = pg_prop_sql_rows(conn,
											"SELECT y, j, pg_temp.cfa1_jf(y, j) FROM cfa1_jb "
											"WHERE (y IS NULL OR j IS NULL) LIMIT 8",
											&failed, NULL);

		pg_prop_fail(tc,
					 "CF 7186 STRICT SQL function returned non-NULL for a NULL argument (%s rows)\n"
					 "  setup: %s\n  (y, j, f(y, j)):\n%s",
					 v, s.data, rows ? rows : "");
	}
	pg_free(v);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * Expression generator for CF 7400: a tree of COALESCE / CASE / NULLIF /
 * typmod casts / an inlinable identity SQL function over a NOT NULL column a
 * and a nullable column b of one type family, with typmods drawn so the
 * typmod the tree exposes can change when the planner reduces it.
 */
typedef struct CfpTyp
{
	const char *const *types;	/* spellings with and without typmod */
	int			ntypes;
	const char *const *lits;
	int			nlits;
	const char *fn;				/* identity SQL function for this family */
} CfpTyp;

static const char *const cfp_vtypes[] = {"varchar(5)", "varchar(10)", "varchar(20)", "varchar", "text"};
static const char *const cfp_vlits[] = {"'x'", "'yy'", "''"};
static const char *const cfp_ntypes[] = {"numeric(10)", "numeric(10,2)", "numeric(5,1)", "numeric"};
static const char *const cfp_nlits[] = {"1", "2.5", "0"};
static const CfpTyp cfp_typs[] = {
	{cfp_vtypes, lengthof(cfp_vtypes), cfp_vlits, lengthof(cfp_vlits), "pg_temp.cfa1_fv"},
	{cfp_ntypes, lengthof(cfp_ntypes), cfp_nlits, lengthof(cfp_nlits), "pg_temp.cfa1_fn"},
};

static void cfp_typmod_expr(pg_prop_tc *tc, StringInfo b, const CfpTyp *t, int depth);

/* a boolean test: the NOT NULL column's IS [NOT] NULL folds to a constant */
static void
cfp_typmod_cond(pg_prop_tc *tc, StringInfo b, const CfpTyp *t, int depth)
{
	switch (pg_prop_draw_int(tc, 0, 4))
	{
		case 0:
			appendStringInfoString(b, "a IS NULL");
			break;
		case 1:
			appendStringInfoString(b, "a IS NOT NULL");
			break;
		case 2:
			appendStringInfoString(b, pg_prop_draw_bool(tc) ? "false" : "true");
			break;
		case 3:
			appendStringInfoString(b, "b IS NULL");
			break;
		default:
			appendStringInfoChar(b, '(');
			cfp_typmod_expr(tc, b, t, depth - 1);
			appendStringInfoString(b, ") IS NULL");
			break;
	}
}

static void
cfp_typmod_expr(pg_prop_tc *tc, StringInfo b, const CfpTyp *t, int depth)
{
	/* depth > 3 marks the root: always a node the planner may reduce */
	int			k = depth > 3 ? (int) pg_prop_draw_int(tc, 3, 8) :
		(int) pg_prop_draw_int(tc, 0, depth <= 0 ? 2 : 8);
	const char *ty = t->types[pg_prop_draw_int(tc, 0, t->ntypes - 1)];

	if (depth > 3)
		depth = 3;

	switch (k)
	{
		case 0:
			appendStringInfoString(b, pg_prop_draw_bool(tc) ? "a" : "b");
			break;
		case 1:
			appendStringInfo(b, "null::%s", ty);
			break;
		case 2:
			appendStringInfo(b, "%s::%s", t->lits[pg_prop_draw_int(tc, 0, t->nlits - 1)], ty);
			break;
		case 3:
		case 4:
			{
				int			n = (int) pg_prop_draw_int(tc, 2, 3);

				appendStringInfoString(b, "COALESCE(");
				for (int i = 0; i < n; i++)
				{
					if (i > 0)
						appendStringInfoString(b, ", ");
					cfp_typmod_expr(tc, b, t, depth - 1);
				}
				appendStringInfoChar(b, ')');
			}
			break;
		case 5:
			{
				int			n = (int) pg_prop_draw_int(tc, 1, 2);

				appendStringInfoString(b, "CASE");
				for (int i = 0; i < n; i++)
				{
					appendStringInfoString(b, " WHEN ");
					cfp_typmod_cond(tc, b, t, depth);
					appendStringInfoString(b, " THEN ");
					cfp_typmod_expr(tc, b, t, depth - 1);
				}
				appendStringInfoString(b, " ELSE ");
				cfp_typmod_expr(tc, b, t, depth - 1);
				appendStringInfoString(b, " END");
			}
			break;
		case 6:
			appendStringInfoString(b, "NULLIF(");
			cfp_typmod_expr(tc, b, t, depth - 1);
			appendStringInfoString(b, ", ");
			cfp_typmod_expr(tc, b, t, depth - 1);
			appendStringInfoChar(b, ')');
			break;
		case 7:
			appendStringInfoChar(b, '(');
			cfp_typmod_expr(tc, b, t, depth - 1);
			appendStringInfo(b, ")::%s", ty);
			break;
		default:
			appendStringInfo(b, "%s(", t->fn);
			cfp_typmod_expr(tc, b, t, depth - 1);
			appendStringInfoChar(b, ')');
			break;
	}
}

/*
 * CF 7400 (part 1, still on master): ON CONFLICT inference simplifies the
 * index expression twice but the inference expression once, and reducing a
 * CASE / COALESCE / NULLIF (PG19 folds "a IS NULL" on a NOT NULL column) can
 * change the typmod the node exposes, so the two trees differ and no
 * arbiter index is found.
 *
 * Invariant: given a unique index on exactly expression E, both
 * INSERT ... ON CONFLICT ((E)) DO NOTHING and ... DO UPDATE must infer it
 * (they must not fail with "no unique or exclusion constraint matching"),
 * and a duplicate key inserted that way leaves one row.  The index is built
 * on an empty table, so it cannot fail on duplicates.
 */
PG_PROP(on_conflict_infers_index_on_same_expression)
{
	pg_prop_conn *conn = cfp_connect(tc);
	const CfpTyp *t = &cfp_typs[pg_prop_draw_int(tc, 0, lengthof(cfp_typs) - 1)];
	const char *ta = t->types[pg_prop_draw_int(tc, 0, t->ntypes - 1)];
	const char *tb = t->types[pg_prop_draw_int(tc, 0, t->ntypes - 1)];
	const char *va = t->lits[pg_prop_draw_int(tc, 0, t->nlits - 1)];
	const char *vb = pg_prop_draw_bool(tc) ? "NULL" :
		t->lits[pg_prop_draw_int(tc, 0, t->nlits - 1)];
	bool		upd = pg_prop_draw_bool(tc);
	StringInfoData e;
	StringInfoData s;
	char	   *err = NULL;

	initStringInfo(&e);
	cfp_typmod_expr(tc, &e, t, 4);

	initStringInfo(&s);
	appendStringInfo(&s,
					 "CREATE FUNCTION pg_temp.cfa1_fv(varchar) RETURNS varchar "
					 "IMMUTABLE LANGUAGE sql AS 'SELECT $1'; "
					 "CREATE FUNCTION pg_temp.cfa1_fn(numeric) RETURNS numeric "
					 "IMMUTABLE LANGUAGE sql AS 'SELECT $1'; "
					 "CREATE TEMP TABLE cfa1_oc (a %s NOT NULL, b %s)", ta, tb);
	cfp_setup(tc, conn, s.data);

	/*
	 * The index can still be rejected for reasons unrelated to the bug (an
	 * expression that folds to a constant is fine, but be safe): discard
	 * such a case rather than count it.
	 */
	resetStringInfo(&s);
	appendStringInfo(&s, "CREATE UNIQUE INDEX cfa1_oc_k ON cfa1_oc ((%s))", e.data);
	if (!pg_prop_sql_exec(conn, s.data, &err))
	{
		if (err && (strstr(err, "XX000") || strstr(err, "cache lookup") ||
					strstr(err, "unexpected")))
			pg_prop_fail(tc, "CREATE INDEX internal error: %s\n  SQL: %s", err, s.data);
		pg_prop_assume(tc, false);
	}

	for (int i = 0; i < 2; i++)
	{
		resetStringInfo(&s);
		appendStringInfo(&s,
						 "INSERT INTO cfa1_oc VALUES (%s, %s) ON CONFLICT ((%s)) %s",
						 va, vb, e.data,
						 upd ? "DO UPDATE SET b = EXCLUDED.b" : "DO NOTHING");
		if (!pg_prop_sql_exec(conn, s.data, &err))
			pg_prop_fail(tc,
						 "CF 7400 ON CONFLICT did not infer the unique index on the same expression\n"
						 "  error: %s  index: CREATE UNIQUE INDEX ON cfa1_oc ((%s))\n"
						 "  table: a %s NOT NULL, b %s\n  SQL: %s",
						 err ? err : "", e.data, ta, tb, s.data);
	}

	/* the key may be NULL (NULLs are distinct), so 1 or 2 rows */
	{
		bool		failed = false;
		char	   *n;

		resetStringInfo(&s);
		appendStringInfo(&s,
						 "SELECT count(*) = CASE WHEN (SELECT (%s) IS NULL FROM cfa1_oc LIMIT 1) "
						 "THEN 2 ELSE 1 END FROM cfa1_oc", e.data);
		n = pg_prop_sql_scalar(conn, s.data, &failed, &err);
		if (failed || n == NULL || strcmp(n, "t") != 0)
			pg_prop_fail(tc, "CF 7400 duplicate insert through ON CONFLICT left the wrong row count "
						 "(%s)\n  index expr: %s", failed ? (err ? err : "") : (n ? n : "NULL"),
						 e.data);
		pg_free(n);
	}

	pfree(e.data);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

static long
cfp_long(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	long		n;

	if (failed || v == NULL)
		pg_prop_fail(tc, "check failed: %s\n  SQL: %s", err ? err : "", sql);
	n = atol(v);
	pg_free(v);
	return n;
}

/*
 * CF 7448: eager aggregation (PG19) pushes a partial aggregate below a join
 * that still has a volatile clause to apply, so the clause runs once per
 * partial group instead of once per joined row.
 *
 * The volatile clause is nextval() of a fresh sequence inside a comparison
 * that is always true, so the query result is fixed, and the sequence counts
 * the evaluations.  Invariant (volatile functions are evaluated for every row
 * that needs them, and a volatile qual is never a pseudoconstant): every row
 * the join produces passed the clause, so the sequence advanced at least
 * once per joined row; and enable_eager_aggregate on/off give the same rows.
 *
 * Drawn: sizes, key and group moduli (so groups are large enough for eager
 * aggregation), where the volatile clause sits (ON with no Vars, ON over both
 * rels, WHERE with no Vars), the aggregates, the join method GUCs, a third
 * joined table, and min_eager_agg_group_size.
 */
PG_PROP(eager_agg_volatile_clause_runs_per_joined_row)
{
	static const char *const vclause[] = {
		"nextval('cfa1_vs') > 0",
		"nextval('cfa1_vs') + f.c > d.g - 1000000",
		"(nextval('cfa1_vs') > 0 OR f.c = d.g)",
	};
	static const char *const methods[] = {
		"",
		"SET LOCAL enable_hashjoin = off;",
		"SET LOCAL enable_hashjoin = off; SET LOCAL enable_mergejoin = off;",
		"SET LOCAL enable_nestloop = off; SET LOCAL enable_mergejoin = off;",
		"SET LOCAL enable_hashagg = off;",
	};
	static const char *const aggs[] = {
		"sum(f.c)", "count(*)", "count(f.c)", "max(f.c)", "min(f.c) + sum(f.c)",
	};
	pg_prop_conn *conn = cfp_connect(tc);
	StringInfoData s;
	StringInfoData q;
	StringInfoData j;
	StringInfoData setup;
	int			nf = (int) pg_prop_draw_int(tc, 100, 3000);
	int			kmod = (int) pg_prop_draw_int(tc, 1, 20);
	int			nd = (int) pg_prop_draw_int(tc, 1, 60);
	int			gmod = (int) pg_prop_draw_int(tc, 1, 5);
	int			cmod = (int) pg_prop_draw_int(tc, 1, 4);
	int			where = (int) pg_prop_draw_int(tc, 0, 3);	/* 0..2 ON, 3 WHERE */
	const char *vc = vclause[pg_prop_draw_int(tc, 0, lengthof(vclause) - 1)];
	const char *agg = aggs[pg_prop_draw_int(tc, 0, lengthof(aggs) - 1)];
	const char *meth = methods[pg_prop_draw_int(tc, 0, lengthof(methods) - 1)];
	bool		third = pg_prop_draw_int(tc, 0, 3) == 0;
	bool		lowmin = pg_prop_draw_bool(tc);
	long		joined;
	long		evals;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "CREATE TEMP TABLE cfa1_vf (k int, c int); "
					 "CREATE TEMP TABLE cfa1_vd (k int, g int); "
					 "CREATE TEMP TABLE cfa1_ve (g int, h int); "
					 "INSERT INTO cfa1_vf SELECT i %% %d, i %% %d FROM generate_series(1, %d) i; "
					 "INSERT INTO cfa1_vd SELECT i %% %d, i %% %d FROM generate_series(1, %d) i; "
					 "INSERT INTO cfa1_ve SELECT i %% %d, i FROM generate_series(1, %d) i; "
					 "CREATE TEMP SEQUENCE cfa1_vs; "
					 "ANALYZE cfa1_vf, cfa1_vd, cfa1_ve",
					 kmod, cmod, nf, kmod, gmod, nd, gmod, gmod);
	cfp_setup(tc, conn, s.data);

	/* the join, without its volatile clause */
	initStringInfo(&j);
	appendStringInfoString(&j, "cfa1_vf f JOIN cfa1_vd d ON f.k = d.k");
	if (third)
		appendStringInfoString(&j, " JOIN cfa1_ve e ON e.g = d.g");

	joined = cfp_long(tc, conn, psprintf("SELECT count(*) FROM %s", j.data));

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT d.g, ");
	appendStringInfoString(&q, agg);
	appendStringInfoString(&q, " FROM cfa1_vf f JOIN cfa1_vd d ON f.k = d.k");
	if (where < 3)
		appendStringInfo(&q, " AND %s", vc);
	if (third)
		appendStringInfoString(&q, " JOIN cfa1_ve e ON e.g = d.g");
	if (where == 3)
		appendStringInfo(&q, " WHERE %s", vc);
	appendStringInfoString(&q, " GROUP BY d.g ORDER BY d.g");

	initStringInfo(&setup);
	appendStringInfo(&setup, "SET LOCAL enable_eager_aggregate = on; %s%s",
					 meth, lowmin ? " SET LOCAL min_eager_agg_group_size = 0;" : "");

	/* eager aggregation allowed: count evaluations of the volatile clause */
	cfp_exec(tc, conn, "SELECT setval('cfa1_vs', 1, false)");
	free(cfp_digest(tc, conn, setup.data, q.data));
	evals = cfp_long(tc, conn,
					 "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM cfa1_vs");
	if (evals < joined)
	{
		char	   *plan;
		bool		failed = false;

		cfp_exec(tc, conn, "BEGIN");
		cfp_exec(tc, conn, setup.data);
		plan = pg_prop_sql_rows(conn, psprintf("EXPLAIN (COSTS OFF) %s", q.data),
								&failed, NULL);
		pg_prop_sql_exec(conn, "ROLLBACK", NULL);
		pg_prop_fail(tc,
					 "CF 7448 volatile clause evaluated %ld times for %ld joined rows\n"
					 "  data: %s\n  setup: %s\n  query: %s\n  plan:\n%s",
					 evals, joined, s.data, setup.data, q.data, plan ? plan : "");
	}

	/* and the same rows with eager aggregation off */
	{
		StringInfoData off;

		initStringInfo(&off);
		appendStringInfo(&off, "%s SET LOCAL enable_eager_aggregate = off;", setup.data);
		cfp_diff(tc, conn, "CF 7448 eager aggregation with a volatile clause",
				 setup.data, q.data, off.data, NULL);
		pfree(off.data);
	}

	pfree(s.data);
	pfree(q.data);
	pfree(j.data);
	pfree(setup.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7466: check_functional_grouping() accepts a WITHOUT OVERLAPS primary key
 * as proof that the other columns depend on the grouped key columns.  The key
 * is enforced by a GiST index whose equality need not be GROUP BY's btree
 * equality: with citext (btree_gist gist_text_ops through binary coercion)
 * 'a' and 'A' are distinct keys but one group, so the query is accepted and
 * returns one arbitrary title for two rows.
 *
 * Invariant: if the server accepts
 *	   SELECT <key cols>, <other cols> FROM t GROUP BY <key cols>
 * then the functional dependency holds, so the result equals
 *	   SELECT ... GROUP BY <key cols>, <other cols>
 * (a correct server either rejects the first form, which makes the case
 * vacuous, or accepts it only when the dependency is real).
 *
 * Drawn: the key column type (citext, or a text / int control), an extra
 * leading key column, the range type of the period, the non-key columns,
 * and rows with case variants and overlapping or disjoint periods; rows
 * that violate the key are dropped by ON CONFLICT DO NOTHING.
 */
PG_PROP(without_overlaps_pk_grouping_matches_full_grouping)
{
	static const char *const idtypes[] = {"citext", "citext", "text", "int"};
	static const char *const ids_txt[] = {"a", "A", "b", "B", "ab", "aB"};
	pg_prop_conn *conn = cfp_connect(tc);
	StringInfoData s;
	StringInfoData qa;
	StringInfoData qb;
	const char *idt;
	bool		extra = pg_prop_draw_bool(tc);
	bool		multi = pg_prop_draw_int(tc, 0, 3) == 0;
	bool		two = pg_prop_draw_bool(tc);
	int			n = (int) pg_prop_draw_int(tc, 1, 30);
	const char *keys;
	const char *others;
	char	   *err = NULL;
	bool		failed = false;
	char	   *da;
	char	   *db;

	if (!pg_prop_require_extension(conn, "btree_gist"))
		pg_prop_skip(tc, "btree_gist not installed");
	if (!pg_prop_require_extension(conn, "citext"))
		pg_prop_skip(tc, "citext not installed");
	idt = idtypes[pg_prop_draw_int(tc, 0, lengthof(idtypes) - 1)];

	initStringInfo(&s);
	appendStringInfo(&s,
					 "CREATE TEMP TABLE cfa1_tp (%sid %s, valid_at %s, title text NOT NULL, "
					 "n int, PRIMARY KEY (%sid, valid_at WITHOUT OVERLAPS)); "
					 "INSERT INTO cfa1_tp VALUES ",
					 extra ? "k int, " : "", idt, multi ? "int4multirange" : "int4range",
					 extra ? "k, " : "");
	for (int i = 0; i < n; i++)
	{
		int			lo = (int) pg_prop_draw_int(tc, 0, 8);
		int			len = (int) pg_prop_draw_int(tc, 1, 4);

		appendStringInfoString(&s, i ? ", (" : "(");
		if (extra)
			appendStringInfo(&s, "%d, ", (int) pg_prop_draw_int(tc, 0, 1));
		if (strcmp(idt, "int") == 0)
			appendStringInfo(&s, "%d", (int) pg_prop_draw_int(tc, 0, 3));
		else
			appendStringInfo(&s, "'%s'", ids_txt[pg_prop_draw_int(tc, 0, lengthof(ids_txt) - 1)]);
		if (multi)
			appendStringInfo(&s, ", '{[%d,%d)}'", lo, lo + len);
		else
			appendStringInfo(&s, ", '[%d,%d)'", lo, lo + len);
		appendStringInfo(&s, ", 't%d', %d)", i, (int) pg_prop_draw_int(tc, 0, 2));
	}
	appendStringInfoString(&s, " ON CONFLICT DO NOTHING; ANALYZE cfa1_tp");
	cfp_setup(tc, conn, s.data);

	keys = extra ? "k, id, valid_at" : "id, valid_at";
	others = two ? "title, n" : "title";

	initStringInfo(&qa);
	initStringInfo(&qb);
	appendStringInfo(&qa,
					 "SELECT %s, %s FROM cfa1_tp GROUP BY %s "
					 "ORDER BY %sid::text COLLATE \"C\", valid_at, title",
					 keys, others, keys, extra ? "k, " : "");
	appendStringInfo(&qb,
					 "SELECT %s, %s FROM cfa1_tp GROUP BY %s, %s "
					 "ORDER BY %sid::text COLLATE \"C\", valid_at, title",
					 keys, others, keys, others, extra ? "k, " : "");

	da = pg_prop_sql_rowhash(conn, qa.data, &failed, &err);
	if (failed)
	{
		/* rejecting the key-only grouping is always correct */
		if (err == NULL || strstr(err, "must appear in the GROUP BY clause") == NULL)
			pg_prop_fail(tc, "unexpected error: %s\n  query: %s", err ? err : "", qa.data);
		pg_prop_disconnect(conn);
		pfree(s.data);
		pfree(qa.data);
		pfree(qb.data);
		return;
	}
	db = pg_prop_sql_rowhash(conn, qb.data, &failed, &err);
	if (failed || db == NULL)
		pg_prop_fail(tc, "query failed: %s\n  query: %s", err ? err : "", qb.data);
	if (strcmp(da, db) != 0)
		pg_prop_fail(tc,
					 "CF 7466 GROUP BY a WITHOUT OVERLAPS primary key was accepted but the "
					 "columns are not functionally dependent on it\n"
					 "  data: %s\n  key-only grouping:\n%s\n  full grouping:\n%s\n"
					 "  A: %s\n  B: %s",
					 s.data, cfp_sample(conn, "", qa.data), cfp_sample(conn, "", qb.data),
					 qa.data, qb.data);
	free(da);
	free(db);
	pfree(s.data);
	pfree(qa.data);
	pfree(qb.data);
	pg_prop_disconnect(conn);
}
