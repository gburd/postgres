/*-------------------------------------------------------------------------
 *
 * prop_cf_exec.c
 *	  No-internal-error and model properties around open commitfest bug fixes
 *	  in the executor, PL/pgSQL and the plan cache.
 *
 * Each property draws a small schema and a statement sequence around the
 * trigger of one CF bug fix.  The common oracle is "a legal statement must
 * not raise an internal error": SQLSTATE class XX (elog(ERROR) without an
 * errcode), "cache lookup failed", or a backend crash (the harness then saves
 * the statement and exits 3).  Where the statement can also be given a
 * reference meaning, its result is compared with that reference.
 *
 * Objects are temp tables, or carry the cfa2_ prefix and are dropped by the
 * case that made them.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_cf_exec.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cf_exec"
#include "pg_prop.h"

#include "lib/stringinfo.h"

static pg_prop_conn *
cfe_connect(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

static void
cfe_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "setup failed: %s\n  SQL: %s", err ? err : "", sql);
}

/*
 * Run "sql" and report success; on error return the SQLSTATE and message.
 * pg_prop_sql_await does not run the crash tripwire, so after an error probe
 * the connection with a statement that carries "sql" in a dollar-quoted
 * literal: if the backend died, the tripwire logs it (and so the statement)
 * and exits 3.
 */
static bool
cfe_try(pg_prop_conn *conn, const char *sql, char **sqlstate, char **errmsg)
{
	bool		ok;
	StringInfoData probe;

	*sqlstate = NULL;
	*errmsg = NULL;
	if (!pg_prop_sql_send(conn, sql))
		ok = false;
	else
		ok = pg_prop_sql_await(conn, sqlstate, errmsg);
	if (!ok)
	{
		initStringInfo(&probe);
		appendStringInfo(&probe, "SELECT $cfe_probe$ liveness probe after:\n%s\n$cfe_probe$", sql);
		pg_prop_sql_exec(conn, probe.data, NULL);
		pfree(probe.data);
	}
	return ok;
}

/* is this error one a legal statement must never raise? */
static bool
cfe_internal(const char *sqlstate, const char *errmsg)
{
	if (sqlstate == NULL)
		return true;			/* no SQLSTATE at all: connection-level */
	if (strncmp(sqlstate, "XX", 2) == 0)
		return true;
	if (errmsg && strstr(errmsg, "cache lookup failed") != NULL)
		return true;
	return false;
}

/* run "sql"; fail the case on an internal error; return success */
static bool
cfe_try_noint(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql,
			  const char *ctx)
{
	char	   *ss;
	char	   *err;

	if (cfe_try(conn, sql, &ss, &err))
		return true;
	if (cfe_internal(ss, err))
		pg_prop_fail(tc, "internal error (SQLSTATE %s): %s\n  statement: %s\n"
					 "  context:\n%s",
					 ss ? ss : "none", err ? err : "", sql, ctx ? ctx : "");
	return false;
}

static char *
cfe_rows(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *r = pg_prop_sql_rows(conn, sql, &failed, &err);

	if (failed)
		pg_prop_fail(tc, "query failed: %s\n  SQL: %s", err ? err : "", sql);
	return r;
}

/* ----------------------------------------------------------------------
 * CF 7295: EXCLUDED.<virtual generated column> in ON CONFLICT.
 *
 * A virtual generated column g is, by definition, its generation expression
 * evaluated over the row it is read from.  So any INSERT ... ON CONFLICT
 * statement on a table with virtual columns must behave exactly like the same
 * statement on a plain table with the generation expressions written inline:
 * EXCLUDED.g -> E(EXCLUDED.b, EXCLUDED.c), t.g -> E(t.b, t.c), RETURNING g /
 * old.g / new.g likewise.  Both the RETURNING rows and the table contents
 * afterwards must agree, and both statements must succeed or fail together.
 *
 * The test table is drawn: column order permuted, a dropped column, one or two
 * virtual columns, partitioned (range / hash / list) or not, with partitions
 * created by PARTITION OF or built separately (own column order and a dropped
 * column) and attached -- the attribute maps the EXCLUDED projection must
 * survive.  Generation expressions include a bare Var and expressions using
 * tableoid that do not depend on its value (for the reference those are
 * written tableoid-free).
 *
 * EXCLUDED has no system columns, so EXCLUDED.g with g using tableoid may be
 * rejected; a correct server raises a user error (master: XX000 "variable not
 * found in subplan target lists", CF 7295).  That case is the one where only
 * "no internal error" is checked.
 * ----------------------------------------------------------------------
 */

/* '$' marks a column qualifier position; tf = tableoid-free equivalent */
typedef struct CfeGen
{
	const char *expr;
	const char *tf;
} CfeGen;

static const CfeGen cfe_gens[] = {
	{"$b * 2", NULL},
	{"$b + $c", NULL},
	{"coalesce($b, $c)", NULL},
	{"$c", NULL},				/* bare Var: matched without non-Var search */
	{"CASE WHEN $b > $c THEN $b ELSE $c END", NULL},
	{"nullif($b, $c)", NULL},
	{"$b - 3 * $c", NULL},
	{"$c + ($tableoid <> 0)::int", "$c + 1"},
	{"$b + $tableoid::int * 0", "$b"},
	{"CASE WHEN $tableoid IS NULL THEN $c ELSE $b END", "$b"},
};

#define CFE_KMAX 8

/* append template with '$' -> "qual." (or nothing for an empty qual) */
static void
cfe_expand(StringInfo b, const char *tmpl, const char *qual)
{
	appendStringInfoChar(b, '(');
	for (const char *p = tmpl; *p; p++)
	{
		if (*p == '$')
		{
			if (qual[0] != '\0')
				appendStringInfo(b, "%s.", qual);
		}
		else
			appendStringInfoChar(b, *p);
	}
	appendStringInfoChar(b, ')');
}

typedef struct CfeVg
{
	int			ngen;			/* 1 or 2 virtual columns g1, g2 */
	const CfeGen *gen[2];
	bool		excl_tableoid;	/* this statement read EXCLUDED.g with tableoid */
} CfeVg;

/* reference to virtual column i via qual: the column (test) or E inline (ref) */
static void
cfe_vg(StringInfo b, CfeVg *v, int i, bool ref, const char *qual)
{
	const CfeGen *g = v->gen[i];

	if (!ref)
	{
		if (qual[0] != '\0')
			appendStringInfo(b, "%s.", qual);
		appendStringInfo(b, "g%d", i + 1);
		if (strcmp(qual, "excluded") == 0 && g->tf != NULL)
			v->excl_tableoid = true;
	}
	else
		cfe_expand(b, g->tf ? g->tf : g->expr, qual);
}

/* an int-valued expression over EXCLUDED and the target row t */
static void
cfe_upsert_expr(StringInfo b, CfeVg *v, bool ref, int kind, int gi)
{
	switch (kind)
	{
		case 0:
			cfe_vg(b, v, gi, ref, "excluded");
			break;
		case 1:
			cfe_vg(b, v, gi, ref, "t");
			break;
		case 2:
			appendStringInfoString(b, "excluded.b");
			break;
		case 3:
			appendStringInfoString(b, "t.b + ");
			cfe_vg(b, v, gi, ref, "excluded");
			break;
		case 4:
			appendStringInfoString(b, "coalesce(");
			cfe_vg(b, v, gi, ref, "excluded");
			appendStringInfoString(b, ", -1)");
			break;
		case 5:
			appendStringInfoString(b, "(SELECT ");
			cfe_vg(b, v, gi, ref, "excluded");
			appendStringInfoString(b, ")");
			break;
		default:
			appendStringInfoString(b, "CASE WHEN ");
			cfe_vg(b, v, gi, ref, "excluded");
			appendStringInfoString(b, " > ");
			cfe_vg(b, v, gi, ref, "t");
			appendStringInfoString(b, " THEN ");
			cfe_vg(b, v, gi, ref, "excluded");
			appendStringInfoString(b, " ELSE t.c END");
			break;
	}
}

/* a boolean condition over EXCLUDED and t */
static void
cfe_upsert_cond(StringInfo b, CfeVg *v, bool ref, int kind, int gi)
{
	switch (kind)
	{
		case 0:
			cfe_vg(b, v, gi, ref, "excluded");
			appendStringInfoString(b, " IS NOT NULL");
			break;
		case 1:
			cfe_vg(b, v, gi, ref, "excluded");
			appendStringInfoString(b, " > ");
			cfe_vg(b, v, gi, ref, "t");
			break;
		case 2:
			cfe_vg(b, v, gi, ref, "t");
			appendStringInfoString(b, " IS DISTINCT FROM ");
			cfe_vg(b, v, gi, ref, "excluded");
			break;
		default:
			appendStringInfoString(b, "excluded.b >= 0");
			break;
	}
}

/* drawn choices of one upsert statement, rendered twice (test, ref) */
typedef struct CfeStmt
{
	int			nrows;
	int			key[4];
	int			bv[4];
	int			cv[4];			/* PG_INT32_MIN = NULL */
	int			colorder;		/* permutation of (a, b, c) in the column list */
	int			action;			/* 0 DO UPDATE, 1 DO SELECT, 2 DO SELECT FOR UPDATE */
	int			nset;			/* DO UPDATE: 1 = b, 2 = b and c, 3 = (b, c) = ROW */
	int			ekind[2];
	int			egi[2];
	bool		has_where;
	int			ckind;
	int			cgi;
	bool		returning;
	int			rgi;
} CfeStmt;

static const char *const cfe_perm3[6][3] = {
	{"a", "b", "c"}, {"a", "c", "b"}, {"b", "a", "c"},
	{"b", "c", "a"}, {"c", "a", "b"}, {"c", "b", "a"},
};

static void
cfe_val(StringInfo b, int v)
{
	if (v == PG_INT32_MIN)
		appendStringInfoString(b, "NULL");
	else
		appendStringInfo(b, "%d", v);
}

static void
cfe_render(StringInfo b, const CfeStmt *s, CfeVg *v, bool ref)
{
	const char *tbl = ref ? "cfa2_r" : "cfa2_v";

	if (s->returning)
		appendStringInfo(b, "TRUNCATE cfa2_ret_%s; WITH r AS (", ref ? "r" : "v");
	appendStringInfo(b, "INSERT INTO %s AS t (%s, %s, %s) VALUES ", tbl,
					 cfe_perm3[s->colorder][0], cfe_perm3[s->colorder][1],
					 cfe_perm3[s->colorder][2]);
	for (int i = 0; i < s->nrows; i++)
	{
		appendStringInfoString(b, i ? ", (" : "(");
		for (int j = 0; j < 3; j++)
		{
			char		col = cfe_perm3[s->colorder][j][0];

			if (j)
				appendStringInfoString(b, ", ");
			cfe_val(b, col == 'a' ? s->key[i] : col == 'b' ? s->bv[i] : s->cv[i]);
		}
		appendStringInfoChar(b, ')');
	}
	appendStringInfoString(b, " ON CONFLICT (a) ");
	if (s->action == 0)
	{
		appendStringInfoString(b, "DO UPDATE SET ");
		if (s->nset == 3)
		{
			appendStringInfoString(b, "(b, c) = ROW(");
			cfe_upsert_expr(b, v, ref, s->ekind[0], s->egi[0]);
			appendStringInfoString(b, ", ");
			cfe_upsert_expr(b, v, ref, s->ekind[1], s->egi[1]);
			appendStringInfoChar(b, ')');
		}
		else
		{
			appendStringInfoString(b, "b = ");
			cfe_upsert_expr(b, v, ref, s->ekind[0], s->egi[0]);
			if (s->nset == 2)
			{
				appendStringInfoString(b, ", c = ");
				cfe_upsert_expr(b, v, ref, s->ekind[1], s->egi[1]);
			}
		}
	}
	else
		appendStringInfo(b, "DO SELECT%s", s->action == 2 ? " FOR UPDATE" : "");
	if (s->has_where)
	{
		appendStringInfoString(b, " WHERE ");
		cfe_upsert_cond(b, v, ref, s->ckind, s->cgi);
	}
	if (s->returning)
	{
		appendStringInfoString(b, " RETURNING a, b, c, ");
		cfe_vg(b, v, s->rgi, ref, "");
		appendStringInfoString(b, ", ");
		cfe_vg(b, v, s->rgi, ref, "old");
		appendStringInfoString(b, ", ");
		cfe_vg(b, v, s->rgi, ref, "new");
		appendStringInfo(b, ") INSERT INTO cfa2_ret_%s SELECT * FROM r",
						 ref ? "r" : "v");
	}
}

static int
cfe_draw_val(pg_prop_tc *tc)
{
	return pg_prop_draw_int(tc, 0, 6) == 0 ? PG_INT32_MIN : (int) pg_prop_draw_int(tc, -3, 9);
}

/* column definitions of one (sub)table in a drawn order, with a dropped "x" */
static void
cfe_coldefs(pg_prop_tc *tc, StringInfo b, CfeVg *v, bool *has_x)
{
	const char *cols[6];
	int			n = 0;
	char		gdef[2][128];

	cols[n++] = "a int NOT NULL";
	cols[n++] = "b int";
	cols[n++] = "c int";
	for (int i = 0; i < v->ngen; i++)
	{
		StringInfoData e;

		initStringInfo(&e);
		cfe_expand(&e, v->gen[i]->expr, "");
		snprintf(gdef[i], sizeof(gdef[i]),
				 "g%d int GENERATED ALWAYS AS %s VIRTUAL", i + 1, e.data);
		pfree(e.data);
		cols[n++] = gdef[i];
	}
	*has_x = pg_prop_draw_bool(tc);
	if (*has_x)
		cols[n++] = "x int";
	/* Fisher-Yates over the drawn order */
	for (int i = n - 1; i > 0; i--)
	{
		int			j = (int) pg_prop_draw_int(tc, 0, i);
		const char *t = cols[i];

		cols[i] = cols[j];
		cols[j] = t;
	}
	for (int i = 0; i < n; i++)
		appendStringInfo(b, "%s%s", i ? ", " : "", cols[i]);
}

PG_PROP(excluded_virtual_generated_matches_inline_expression)
{
	pg_prop_conn *conn = cfe_connect(tc);
	CfeVg		v;
	StringInfoData s;
	StringInfoData log;
	bool		has_x;
	int			pkind;
	int			nparts = 0;
	int			nstmts;
	char	   *want;
	char	   *got;

	memset(&v, 0, sizeof(v));
	v.ngen = (int) pg_prop_draw_int(tc, 1, 2);
	for (int i = 0; i < v.ngen; i++)
		v.gen[i] = &cfe_gens[pg_prop_draw_int(tc, 0, lengthof(cfe_gens) - 1)];

	initStringInfo(&s);
	initStringInfo(&log);

	/* the test table: drawn layout, maybe partitioned */
	pkind = (int) pg_prop_draw_int(tc, 0, 3);
	appendStringInfoString(&s, "CREATE TEMP TABLE cfa2_v (");
	cfe_coldefs(tc, &s, &v, &has_x);
	appendStringInfoString(&s, ", PRIMARY KEY (a))");
	if (pkind == 1)
		appendStringInfoString(&s, " PARTITION BY RANGE (a)");
	else if (pkind == 2)
		appendStringInfoString(&s, " PARTITION BY HASH (a)");
	else if (pkind == 3)
		appendStringInfoString(&s, " PARTITION BY LIST (a)");
	appendStringInfoString(&s, "; ");
	if (has_x)
		appendStringInfoString(&s, "ALTER TABLE cfa2_v DROP COLUMN x; ");
	if (pkind != 0)
	{
		int			split = (int) pg_prop_draw_int(tc, 2, CFE_KMAX - 1);

		nparts = (int) pg_prop_draw_int(tc, 1, 3);
		if (pkind == 1)
			nparts = nparts < 2 ? 2 : nparts;
		for (int i = 0; i < nparts; i++)
		{
			StringInfoData bound;
			bool		separate = pg_prop_draw_bool(tc);

			initStringInfo(&bound);
			if (pkind == 1)
			{
				/* (MINVALUE, split), [split, split+1), ..., [.., MAXVALUE) */
				if (i == 0)
					appendStringInfo(&bound, "FROM (MINVALUE) TO (%d)", split);
				else if (i == nparts - 1)
					appendStringInfo(&bound, "FROM (%d) TO (MAXVALUE)", split + i - 1);
				else
					appendStringInfo(&bound, "FROM (%d) TO (%d)", split + i - 1, split + i);
			}
			else if (pkind == 2)
				appendStringInfo(&bound, "WITH (MODULUS %d, REMAINDER %d)", nparts, i);
			else if (i == nparts - 1)
				appendStringInfoString(&bound, "DEFAULT");
			else
				appendStringInfo(&bound, "IN (%d, %d)", i + 1, i + 1 + nparts);

			if (!separate)
				appendStringInfo(&s,
								 "CREATE TEMP TABLE cfa2_v%d PARTITION OF cfa2_v %s%s; ",
								 i, strcmp(bound.data, "DEFAULT") == 0 ? "" : "FOR VALUES ",
								 bound.data);
			else
			{
				bool		cx;

				appendStringInfo(&s, "CREATE TEMP TABLE cfa2_v%d (", i);
				cfe_coldefs(tc, &s, &v, &cx);
				appendStringInfoString(&s, "); ");
				if (cx)
					appendStringInfo(&s, "ALTER TABLE cfa2_v%d DROP COLUMN x; ", i);
				appendStringInfo(&s, "ALTER TABLE cfa2_v ATTACH PARTITION cfa2_v%d %s%s; ",
								 i, strcmp(bound.data, "DEFAULT") == 0 ? "" : "FOR VALUES ",
								 bound.data);
			}
			pfree(bound.data);
		}
	}
	/* the reference: a plain table, generated columns computed inline */
	appendStringInfoString(&s,
						   "CREATE TEMP TABLE cfa2_r (a int PRIMARY KEY, b int, c int); "
						   "CREATE TEMP TABLE cfa2_ret_v (a int, b int, c int, g int, og int, ng int); "
						   "CREATE TEMP TABLE cfa2_ret_r (a int, b int, c int, g int, og int, ng int); ");
	cfe_exec(tc, conn, s.data);
	appendStringInfoString(&log, s.data);

	/* initial rows, copied into the reference */
	resetStringInfo(&s);
	for (int k = 1; k <= CFE_KMAX; k++)
	{
		if (!pg_prop_draw_bool(tc))
			continue;
		appendStringInfo(&s, "INSERT INTO cfa2_v (a, b, c) VALUES (%d, ", k);
		cfe_val(&s, cfe_draw_val(tc));
		appendStringInfoString(&s, ", ");
		cfe_val(&s, cfe_draw_val(tc));
		appendStringInfoString(&s, "); ");
	}
	appendStringInfoString(&s, "INSERT INTO cfa2_r SELECT a, b, c FROM cfa2_v");
	cfe_exec(tc, conn, s.data);
	appendStringInfo(&log, "%s;\n", s.data);

	nstmts = (int) pg_prop_draw_int(tc, 1, 4);
	for (int n = 0; n < nstmts; n++)
	{
		CfeStmt		st;
		StringInfoData tq;
		StringInfoData rq;
		char	   *ss;
		char	   *err;
		bool		tok;
		bool		rok;

		memset(&st, 0, sizeof(st));
		st.nrows = (int) pg_prop_draw_int(tc, 1, 4);
		for (int i = 0; i < st.nrows; i++)
		{
			st.key[i] = (int) pg_prop_draw_int(tc, 1, CFE_KMAX);
			st.bv[i] = cfe_draw_val(tc);
			st.cv[i] = cfe_draw_val(tc);
		}
		st.colorder = (int) pg_prop_draw_int(tc, 0, 5);
		st.action = (int) pg_prop_draw_int(tc, 0, 3);
		if (st.action == 3)
			st.action = 0;		/* DO UPDATE twice as likely */
		st.nset = (int) pg_prop_draw_int(tc, 1, 3);
		for (int i = 0; i < 2; i++)
		{
			st.ekind[i] = (int) pg_prop_draw_int(tc, 0, 6);
			st.egi[i] = (int) pg_prop_draw_int(tc, 0, v.ngen - 1);
		}
		st.has_where = pg_prop_draw_bool(tc);
		st.ckind = (int) pg_prop_draw_int(tc, 0, 3);
		st.cgi = (int) pg_prop_draw_int(tc, 0, v.ngen - 1);
		st.returning = st.action != 0 || pg_prop_draw_bool(tc);
		st.rgi = (int) pg_prop_draw_int(tc, 0, v.ngen - 1);

		initStringInfo(&tq);
		initStringInfo(&rq);
		v.excl_tableoid = false;
		cfe_render(&tq, &st, &v, false);
		cfe_render(&rq, &st, &v, true);
		appendStringInfo(&log, "%s;\n", tq.data);

		tok = cfe_try(conn, tq.data, &ss, &err);
		if (!tok && cfe_internal(ss, err))
			pg_prop_fail(tc, "internal error (SQLSTATE %s): %s\n  statement: %s\n"
						 "  reference: %s\n  history:\n%s",
						 ss ? ss : "none", err ? err : "", tq.data, rq.data, log.data);
		if (!tok && v.excl_tableoid)
			continue;			/* EXCLUDED has no tableoid: a user error is right */
		rok = cfe_try_noint(tc, conn, rq.data, log.data);
		if (tok != rok)
			pg_prop_fail(tc, "virtual-column statement %s but its inline reference %s\n"
						 "  statement: %s\n  reference: %s\n  error: %s\n  history:\n%s",
						 tok ? "succeeded" : "failed", rok ? "succeeded" : "failed",
						 tq.data, rq.data, err ? err : "", log.data);
		if (!tok)
			continue;

		if (st.returning)
		{
			want = cfe_rows(tc, conn, "SELECT * FROM cfa2_ret_r ORDER BY 1, 2, 3, 4, 5, 6");
			got = cfe_rows(tc, conn, "SELECT * FROM cfa2_ret_v ORDER BY 1, 2, 3, 4, 5, 6");
			if (strcmp(want, got) != 0)
				pg_prop_fail(tc, "RETURNING differs from the inline-expression reference\n"
							 "  statement: %s\n  reference: %s\n  got:\n%s  want:\n%s"
							 "  history:\n%s", tq.data, rq.data, got, want, log.data);
			pg_free(want);
			pg_free(got);
		}

		resetStringInfo(&s);
		appendStringInfoString(&s, "SELECT a, b, c");
		for (int i = 0; i < v.ngen; i++)
		{
			appendStringInfoString(&s, ", ");
			cfe_vg(&s, &v, i, true, "");
		}
		appendStringInfoString(&s, " FROM cfa2_r ORDER BY a");
		want = cfe_rows(tc, conn, s.data);
		got = cfe_rows(tc, conn, v.ngen == 1 ?
					   "SELECT a, b, c, g1 FROM cfa2_v ORDER BY a" :
					   "SELECT a, b, c, g1, g2 FROM cfa2_v ORDER BY a");
		if (strcmp(want, got) != 0)
			pg_prop_fail(tc, "table contents differ from the inline-expression reference\n"
						 "  after: %s\n  reference: %s\n  got:\n%s  want:\n%s"
						 "  history:\n%s", tq.data, rq.data, got, want, log.data);
		pg_free(want);
		pg_free(got);
		pfree(tq.data);
		pfree(rq.data);
	}

	pfree(s.data);
	pfree(log.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 6825: an ON DELETE CASCADE on a self-referencing table, fired while a
 * BEFORE DELETE trigger on the same table invalidates the cascade's cached RI
 * plan (any DDL that bumps the relcache -- GRANT/REVOKE, ANALYZE, ...) and
 * then deletes from the table itself, re-entering the same RI query.  The
 * outer execution of the replaced plan must still finish.  On master it
 * crashes the backend.
 *
 * Invariant (a model, not just "no crash"): deleting a row removes exactly
 * its descendants in the parent_id forest, whatever the trigger does in
 * between, because the trigger's own DELETE removes a subset of those same
 * descendants.  The forest, the deleted root, the rows whose trigger
 * invalidates, the invalidating statement and the trigger's timing (whether
 * it deletes its own children before the cascade reaches them) are drawn.
 */
PG_PROP(self_ref_cascade_with_replanning_trigger_matches_model)
{
	static const char *const inval[] = {
		"GRANT SELECT ON cfa2_sr TO PUBLIC; REVOKE SELECT ON cfa2_sr FROM PUBLIC;",
		"ANALYZE cfa2_sr;",
		"COMMENT ON TABLE cfa2_sr IS 'x';",
		"ALTER TABLE cfa2_sr ALTER COLUMN parent_id SET STATISTICS 50;",
	};
	pg_prop_conn *conn = cfe_connect(tc);
	int			n = (int) pg_prop_draw_int(tc, 2, 24);
	int			parent[25];
	bool		gone[25];
	bool		fires[25];
	int			root;
	const char *iv = inval[pg_prop_draw_int(tc, 0, lengthof(inval) - 1)];
	StringInfoData s;
	StringInfoData model;
	char	   *got;
	bool		failed = false;
	char	   *err = NULL;
	bool		changed;

	/* a forest: each row's parent is an earlier row, or none */
	for (int i = 1; i <= n; i++)
	{
		parent[i] = (i > 1 && pg_prop_draw_int(tc, 0, 3) != 0)
			? (int) pg_prop_draw_int(tc, 1, i - 1) : 0;
		fires[i] = pg_prop_draw_int(tc, 0, 2) == 0;
		gone[i] = false;
	}
	root = (int) pg_prop_draw_int(tc, 1, n);

	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP TABLE IF EXISTS cfa2_sr; "
					 "CREATE TABLE cfa2_sr (id int PRIMARY KEY, "
					 "parent_id int REFERENCES cfa2_sr (id) ON DELETE CASCADE); "
					 "CREATE OR REPLACE FUNCTION cfa2_sr_bd() RETURNS trigger LANGUAGE plpgsql AS $f$ "
					 "BEGIN IF OLD.id = ANY (ARRAY[");
	{
		bool		first = true;

		for (int i = 1; i <= n; i++)
			if (fires[i])
			{
				appendStringInfo(&s, "%s%d", first ? "" : ",", i);
				first = false;
			}
		if (first)
			appendStringInfoString(&s, "0");
	}
	appendStringInfo(&s,
					 "]) THEN %s DELETE FROM cfa2_sr WHERE parent_id = OLD.id; END IF; "
					 "RETURN OLD; END $f$; "
					 "CREATE TRIGGER cfa2_sr_bd BEFORE DELETE ON cfa2_sr "
					 "FOR EACH ROW EXECUTE FUNCTION cfa2_sr_bd(); "
					 "INSERT INTO cfa2_sr VALUES ", iv);
	for (int i = 1; i <= n; i++)
	{
		appendStringInfo(&s, "%s(%d, ", i > 1 ? ", " : "", i);
		if (parent[i])
			appendStringInfo(&s, "%d)", parent[i]);
		else
			appendStringInfoString(&s, "NULL)");
	}
	cfe_exec(tc, conn, s.data);

	/* the statement under test; a crash makes the harness exit 3 */
	resetStringInfo(&s);
	appendStringInfo(&s, "DELETE FROM cfa2_sr WHERE id = %d", root);

	/*
	 * A user error (some invalidating statements are rejected inside a
	 * trigger on a table in use, e.g. ALTER TABLE) rolls the DELETE back, so
	 * the model is then "nothing deleted".  An internal error fails the case.
	 */
	if (cfe_try_noint(tc, conn, s.data, iv))
		gone[root] = true;
	do
	{
		changed = false;
		for (int i = 1; i <= n; i++)
			if (gone[root] && !gone[i] && parent[i] && gone[parent[i]])
				gone[i] = changed = true;
	} while (changed);
	initStringInfo(&model);
	for (int i = 1; i <= n; i++)
		if (!gone[i])
			appendStringInfo(&model, "%d\n", i);

	got = pg_prop_sql_rows(conn, "SELECT id FROM cfa2_sr ORDER BY id", &failed, &err);
	if (failed)
		pg_prop_fail(tc, "read-back failed: %s", err ? err : "");
	if (strcmp(got, model.data) != 0)
		pg_prop_fail(tc, "after DELETE id=%d with a trigger running \"%s\", "
					 "rows left differ from the cascade model\n  model:\n%s  table:\n%s",
					 root, iv, model.data, got);

	pg_free(got);
	cfe_exec(tc, conn, "DROP TABLE cfa2_sr; DROP FUNCTION cfa2_sr_bd()");
	pfree(s.data);
	pfree(model.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7094: a cast to "unknown" of a typed (non-literal) value must either work
 * or raise a user error, never XX000 "failed to find conversion function
 * from unknown to text".  Drawn: the source type and whether the value is a
 * literal, a column, or a PL/pgSQL assignment.
 */
PG_PROP(cast_to_unknown_is_not_internal)
{
	static const char *const srcs[] = {
		"'s'::text", "'s'::varchar", "1::int", "'s'::name", "'{1}'::int[]", "now()::text"
	};
	pg_prop_conn *conn = cfe_connect(tc);
	const char *src = srcs[pg_prop_draw_int(tc, 0, lengthof(srcs) - 1)];
	int			shape = (int) pg_prop_draw_int(tc, 0, 2);
	StringInfoData s;

	initStringInfo(&s);
	if (shape == 0)
		appendStringInfo(&s, "SELECT %s::unknown", src);
	else if (shape == 1)
		appendStringInfo(&s, "SELECT cast(v AS unknown) FROM (SELECT %s AS v) s", src);
	else
		appendStringInfo(&s, "DO $$ DECLARE a text; BEGIN a := %s::unknown; END $$", src);
	cfe_try_noint(tc, conn, s.data, "CF 7094 cast to unknown");
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7176: currtid2(view, tid) on a view must error with a proper SQLSTATE
 * (the view has no single underlying ctid) -- never XX000.  Drawn: the view
 * shape (GROUP BY ctid, DISTINCT, UNION, join, plain), the tid.
 */
PG_PROP(currtid2_on_view_is_not_internal)
{
	static const char *const views[] = {
		"SELECT ctid, a FROM cfa4_tid GROUP BY ctid, a",
		"SELECT DISTINCT ctid, a FROM cfa4_tid",
		"SELECT ctid, a FROM cfa4_tid UNION ALL SELECT ctid, a FROM cfa4_tid",
		"SELECT ctid, a FROM cfa4_tid",
		"SELECT x.ctid, x.a FROM cfa4_tid x JOIN cfa4_tid y USING (a)",
	};
	pg_prop_conn *conn = cfe_connect(tc);
	const char *v = views[pg_prop_draw_int(tc, 0, lengthof(views) - 1)];
	StringInfoData s;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP TABLE IF EXISTS cfa4_tid CASCADE; CREATE TABLE cfa4_tid (a int); "
					 "INSERT INTO cfa4_tid SELECT generate_series(1, %d); "
					 "CREATE VIEW cfa4_tid_v AS %s",
					 (int) pg_prop_draw_int(tc, 1, 20), v);
	cfe_exec(tc, conn, s.data);
	resetStringInfo(&s);
	appendStringInfo(&s, "SELECT currtid2('cfa4_tid_v'::text, '(0,%d)'::tid)",
					 (int) pg_prop_draw_int(tc, 1, 5));
	cfe_try_noint(tc, conn, s.data, v);
	cfe_exec(tc, conn, "DROP TABLE cfa4_tid CASCADE");
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7296: a set operation over zero-column inputs under GUCs that push the
 * planner to parallel plans must not crash (Assert nkeys > 0 in
 * tuplesort_begin_heap), and must return what the serial plan returns.
 * Drawn: set operation, sizes, parallel/cost GUCs.
 */
PG_PROP(zero_column_setop_parallel_matches_serial)
{
	static const char *const ops[] = {"UNION", "INTERSECT", "EXCEPT", "UNION ALL"};
	pg_prop_conn *conn = cfe_connect(tc);
	const char *op = ops[pg_prop_draw_int(tc, 0, lengthof(ops) - 1)];
	int			n1 = (int) pg_prop_draw_int(tc, 0, 3000);
	int			n2 = (int) pg_prop_draw_int(tc, 0, 3000);
	StringInfoData s;
	StringInfoData q;
	char	   *par;
	char	   *ser;
	bool		failed = false;
	char	   *err = NULL;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP TABLE IF EXISTS cfa4_z1, cfa4_z2; "
					 "CREATE TABLE cfa4_z1 AS SELECT g FROM generate_series(1, %d) g; "
					 "CREATE TABLE cfa4_z2 AS SELECT g FROM generate_series(1, %d) g; "
					 "ANALYZE cfa4_z1, cfa4_z2", n1, n2);
	cfe_exec(tc, conn, s.data);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT count(*) FROM (SELECT FROM cfa4_z1 %s SELECT FROM cfa4_z2) s", op);
	ser = pg_prop_sql_scalar(conn, q.data, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "serial plan failed: %s\n  %s", err ? err : "", q.data);

	resetStringInfo(&s);
	appendStringInfo(&s,
					 "SET cpu_tuple_cost = %d; SET min_parallel_table_scan_size = 1; "
					 "SET parallel_setup_cost = 0; SET parallel_tuple_cost = 0",
					 (int) pg_prop_draw_int(tc, 1, 2000));
	cfe_exec(tc, conn, s.data);
	par = pg_prop_sql_scalar(conn, q.data, &failed, &err);	/* crash -> harness exits 3 */
	cfe_exec(tc, conn, "RESET ALL");
	if (failed)
		pg_prop_fail(tc, "parallel-costed plan failed: %s\n  %s", err ? err : "", q.data);
	if (strcmp(par, ser) != 0)
		pg_prop_fail(tc, "zero-column %s: parallel-costed plan %s rows, serial %s\n  sizes %d, %d",
					 op, par, ser, n1, n2);
	pg_free(par);
	pg_free(ser);
	cfe_exec(tc, conn, "DROP TABLE cfa4_z1, cfa4_z2");
	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7097: an error raised inside an SQL-callable replication-slot function
 * and caught by a PL/pgSQL EXCEPTION block must release the slot; the next
 * slot operation in the session must work.  On master the slot stays
 * acquired (active = t) and the next acquire trips
 * Assert(MyReplicationSlot == NULL).  Needs wal_level = logical; skipped
 * otherwise.  Drawn: the failing call (advance to an invalid LSN, peek with
 * a bad option), the follow-up operation, how many times the pair repeats.
 */
PG_PROP(slot_function_error_releases_slot)
{
	static const char *const fail_calls[] = {
		"PERFORM pg_replication_slot_advance('cfa4_slot', '0/1')",
		"PERFORM count(*) FROM pg_logical_slot_peek_changes('cfa4_slot', NULL, NULL, 'no-such-option', 'x')",
		"PERFORM count(*) FROM pg_logical_slot_get_changes('cfa4_slot', NULL, NULL, 'include-xids', 'notabool')",
	};
	static const char *const follow[] = {
		"SELECT count(*) >= 0 FROM pg_logical_slot_peek_changes('cfa4_slot', NULL, NULL)",
		"SELECT count(*) >= 0 FROM pg_logical_slot_get_changes('cfa4_slot', NULL, NULL)",
		"SELECT pg_replication_slot_advance('cfa4_slot', pg_current_wal_lsn()) IS NOT NULL",
	};
	pg_prop_conn *conn = cfe_connect(tc);
	bool		failed = false;
	char	   *wl = pg_prop_sql_scalar(conn, "SHOW wal_level", &failed, NULL);
	int			reps = (int) pg_prop_draw_int(tc, 1, 3);
	StringInfoData s;
	char	   *act;

	if (wl == NULL || strcmp(wl, "logical") != 0)
	{
		if (wl)
			pg_free(wl);
		pg_prop_disconnect(conn);
		pg_prop_skip(tc, "needs wal_level = logical");
	}
	pg_free(wl);
	pg_prop_sql_exec(conn, "SELECT pg_drop_replication_slot('cfa4_slot') "
					 "FROM pg_replication_slots WHERE slot_name = 'cfa4_slot'", NULL);
	cfe_exec(tc, conn, "SELECT pg_create_logical_replication_slot('cfa4_slot', 'test_decoding')");

	initStringInfo(&s);
	for (int r = 0; r < reps; r++)
	{
		resetStringInfo(&s);
		appendStringInfo(&s, "DO $$ BEGIN %s; EXCEPTION WHEN OTHERS THEN NULL; END $$",
						 fail_calls[pg_prop_draw_int(tc, 0, lengthof(fail_calls) - 1)]);
		cfe_try_noint(tc, conn, s.data, "CF 7097 slot error inside an exception block");

		act = pg_prop_sql_scalar(conn,
								 "SELECT active_pid IS NOT DISTINCT FROM pg_backend_pid() "
								 "FROM pg_replication_slots WHERE slot_name = 'cfa4_slot'",
								 &failed, NULL);
		if (act && strcmp(act, "t") == 0)
			pg_prop_fail(tc, "slot still held by this session after the caught error\n  %s", s.data);
		if (act)
			pg_free(act);

		/* the next slot operation; a crash exits the harness */
		cfe_try_noint(tc, conn, follow[pg_prop_draw_int(tc, 0, lengthof(follow) - 1)],
					  "CF 7097 follow-up slot operation");
	}
	pg_prop_sql_exec(conn, "SELECT pg_drop_replication_slot('cfa4_slot')", NULL);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7459: a PL/pgSQL record assigned from a function returning a row keeps
 * TOAST pointers into the source table.  In a non-atomic DO block the source
 * row is deleted and the transaction committed; if VACUUM then removes the
 * dead row and its TOAST chunks before the record is read, the read raises
 * "missing chunk number 0 for toast value".  Invariant: a record's value is
 * the value that was read, whatever happens to the source row later.
 *
 * Interleaving (two sessions, as in the patch's isolation spec): session A
 * holds an advisory lock; session B runs the DO block, which deletes the
 * row, COMMITs, then blocks on the advisory lock; A runs VACUUM and releases
 * the lock; B then reads the record.  Drawn: value size (inline or toasted),
 * storage mode, DELETE vs UPDATE, and VACUUM vs no VACUUM (the control).
 */
PG_PROP(nonatomic_record_survives_source_row_removal)
{
	static const char *const storages[] = {"EXTERNAL", "EXTENDED"};
	pg_prop_conn *a = cfe_connect(tc);
	pg_prop_conn *b = cfe_connect(tc);
	int			len = pg_prop_draw_int(tc, 0, 3) == 0 ? (int) pg_prop_draw_int(tc, 1, 1500)
		: (int) pg_prop_draw_int(tc, 3000, 200000);
	const char *st = storages[pg_prop_draw_int(tc, 0, lengthof(storages) - 1)];
	bool		delete_row = pg_prop_draw_bool(tc);
	bool		vacuum = pg_prop_draw_int(tc, 0, 3) != 0;
	StringInfoData s;
	char	   *ss = NULL;
	char	   *err = NULL;
	bool		ok;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP TABLE IF EXISTS cfa4_t1 CASCADE; CREATE TABLE cfa4_t1 (id int, t text) "
					 "WITH (autovacuum_enabled = off); "
					 "ALTER TABLE cfa4_t1 ALTER COLUMN t SET STORAGE %s; "
					 "INSERT INTO cfa4_t1 SELECT 1, string_agg(md5(i::text), '') FROM generate_series(1, %d) i; "
					 "CREATE OR REPLACE FUNCTION cfa4_get() RETURNS cfa4_t1 LANGUAGE plpgsql AS "
					 "$f$ DECLARE r cfa4_t1; BEGIN SELECT * INTO r FROM cfa4_t1; RETURN r; END $f$",
					 st, len / 32 + 1);
	cfe_exec(tc, a, s.data);
	cfe_exec(tc, a, "SELECT pg_advisory_lock(74590)");

	resetStringInfo(&s);
	appendStringInfo(&s,
					 "DO $$ DECLARE r cfa4_t1; want int; BEGIN "
					 "SELECT length(t) INTO want FROM cfa4_t1; "
					 "r := cfa4_get(); "
					 "%s "
					 "COMMIT; "
					 "PERFORM pg_advisory_lock(74590); "
					 "IF length(r.t) IS DISTINCT FROM want THEN "
					 "RAISE EXCEPTION 'record value changed: %% vs %%', length(r.t), want; END IF; "
					 "PERFORM pg_advisory_unlock(74590); "
					 "END $$",
					 delete_row ? "DELETE FROM cfa4_t1;" : "UPDATE cfa4_t1 SET t = 'x';");
	if (!pg_prop_sql_send(b, s.data))
		pg_prop_fail(tc, "could not dispatch the DO block");

	/* let B reach the advisory lock (after its COMMIT), then clean the row away */
	cfe_exec(tc, a, "SELECT pg_sleep(0.2)");
	if (vacuum)
		cfe_exec(tc, a, "VACUUM cfa4_t1");
	cfe_exec(tc, a, "SELECT pg_advisory_unlock(74590)");

	ok = pg_prop_sql_await(b, &ss, &err);
	if (!ok && err && (strstr(err, "missing chunk") || strstr(err, "record value changed") ||
					   strstr(err, "unexpected chunk")))
		pg_prop_fail(tc, "a record read before COMMIT lost its value after %s: %s\n"
					 "  storage %s, %d bytes, %s",
					 vacuum ? "VACUUM" : "no VACUUM", err, st, len,
					 delete_row ? "DELETE" : "UPDATE");
	if (!ok && cfe_internal(ss, err))
		pg_prop_fail(tc, "internal error: %s", err ? err : "");

	cfe_exec(tc, a, "DROP TABLE cfa4_t1 CASCADE");
	pfree(s.data);
	pg_prop_disconnect(b);
	pg_prop_disconnect(a);
}

/*
 * CF 6903: a PL/pgSQL RETURN QUERY over a non-inlined SETOF function
 * returning a named composite; after ALTER TYPE ... ADD ATTRIBUTE widens the
 * composite (and the underlying table gains the column), the cached plan
 * must be invalidated -- the second call in the same session must return
 * what a fresh session returns.  On master the plan is not invalidated and
 * the call raises "structure of query does not match function result type".
 *
 * Invariant: same-session result after the DDL = fresh-session result.
 * Drawn: inlining blockers (SECURITY DEFINER / VOLATILE / plain), SRF
 * language (sql / plpgsql RETURN QUERY), the DDL pair, rows.
 */
PG_PROP(cached_return_query_sees_widened_composite)
{
	static const char *const attrs[] = {"STABLE SECURITY DEFINER", "VOLATILE", "STABLE", "SECURITY DEFINER"};
	pg_prop_conn *conn = cfe_connect(tc);
	pg_prop_conn *fresh;
	const char *attr = attrs[pg_prop_draw_int(tc, 0, lengthof(attrs) - 1)];
	bool		plpgsql_srf = pg_prop_draw_bool(tc);
	int			nrows = (int) pg_prop_draw_int(tc, 1, 5);
	StringInfoData s;
	char	   *a;
	char	   *b;
	bool		fa = false;
	bool		fb = false;
	char	   *ea = NULL;
	char	   *eb = NULL;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP FUNCTION IF EXISTS cfa4_caller() CASCADE; DROP FUNCTION IF EXISTS cfa4_srf() CASCADE; "
					 "DROP TABLE IF EXISTS cfa4_tbl; DROP TYPE IF EXISTS cfa4_ct CASCADE; "
					 "CREATE TYPE cfa4_ct AS (a int, b int); "
					 "CREATE TABLE cfa4_tbl (a int, b int); "
					 "INSERT INTO cfa4_tbl SELECT i, i * 10 FROM generate_series(1, %d) i; ", nrows);
	if (plpgsql_srf)
		appendStringInfo(&s,
						 "CREATE FUNCTION cfa4_srf() RETURNS SETOF cfa4_ct LANGUAGE plpgsql %s AS "
						 "$f$ BEGIN RETURN QUERY SELECT * FROM cfa4_tbl; END $f$; ", attr);
	else
		appendStringInfo(&s,
						 "CREATE FUNCTION cfa4_srf() RETURNS SETOF cfa4_ct LANGUAGE sql %s AS "
						 "$f$ SELECT * FROM cfa4_tbl $f$; ", attr);
	appendStringInfoString(&s,
						   "CREATE FUNCTION cfa4_caller() RETURNS SETOF cfa4_ct LANGUAGE plpgsql AS "
						   "$f$ BEGIN RETURN QUERY SELECT r.* FROM cfa4_srf() r; END $f$");
	cfe_exec(tc, conn, s.data);

	/* warm the plan cache in this session */
	pg_free(pg_prop_sql_rows(conn, "SELECT * FROM cfa4_caller() ORDER BY 1", &fa, NULL));

	cfe_exec(tc, conn, "ALTER TYPE cfa4_ct ADD ATTRIBUTE c int");
	cfe_exec(tc, conn, "ALTER TABLE cfa4_tbl ADD COLUMN c int DEFAULT 99");

	a = pg_prop_sql_rows(conn, "SELECT * FROM cfa4_caller() ORDER BY 1", &fa, &ea);
	fresh = cfe_connect(tc);
	b = pg_prop_sql_rows(fresh, "SELECT * FROM cfa4_caller() ORDER BY 1", &fb, &eb);
	if (fa != fb || (!fa && strcmp(a, b) != 0))
		pg_prop_fail(tc, "after ALTER TYPE ADD ATTRIBUTE the cached plan disagrees with a fresh session\n"
					 "  same session: %s\n  fresh session: %s\n  srf: %s %s",
					 fa ? (ea ? ea : "error") : a, fb ? (eb ? eb : "error") : b,
					 plpgsql_srf ? "plpgsql" : "sql", attr);
	if (a)
		pg_free(a);
	if (b)
		pg_free(b);
	pg_prop_disconnect(fresh);
	cfe_exec(tc, conn, "DROP FUNCTION cfa4_caller(); DROP FUNCTION cfa4_srf(); DROP TABLE cfa4_tbl; DROP TYPE cfa4_ct");
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7347 (on top of 7097): a temporary logical slot whose creation fails
 * inside a subtransaction (the output plugin fails to load; the error is
 * caught by an EXCEPTION block) must not be left behind: no slot of that
 * name may be listed and the name must be reusable.  Drawn: the failure,
 * how many attempts.  Needs wal_level = logical.  On master the second
 * attempt trips the CF 7097 assertion first, so validate on 7097 + 7347.
 */
PG_PROP(failed_temp_slot_creation_leaves_no_slot)
{
	/*
	 * Each fails while loading the output plugin, i.e. after the slot was
	 * created.  (An error raised after a successful creation is not a case:
	 * slot creation is not transactional, the slot is meant to survive.)
	 */
	static const char *const bad[] = {
		"PERFORM pg_create_logical_replication_slot('cfa4_tmp', 'no_such_plugin', true)",
		"PERFORM pg_create_logical_replication_slot('cfa4_tmp', 'plpgsql', true)",
	};
	pg_prop_conn *conn = cfe_connect(tc);
	bool		failed = false;
	char	   *wl = pg_prop_sql_scalar(conn, "SHOW wal_level", &failed, NULL);
	int			tries = (int) pg_prop_draw_int(tc, 1, 3);
	int			which = (int) pg_prop_draw_int(tc, 0, lengthof(bad) - 1);
	StringInfoData s;
	char	   *n;

	if (wl == NULL || strcmp(wl, "logical") != 0)
		pg_prop_skip(tc, "needs wal_level = logical");
	pg_free(wl);
	pg_prop_sql_exec(conn, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
					 "WHERE slot_name = 'cfa4_tmp' AND active_pid IS NULL", NULL);
	initStringInfo(&s);
	for (int i = 0; i < tries; i++)
	{
		resetStringInfo(&s);
		appendStringInfo(&s, "DO $$ BEGIN %s; EXCEPTION WHEN OTHERS THEN NULL; END $$", bad[which]);
		cfe_try_noint(tc, conn, s.data, "CF 7347 failed temporary slot creation");
	}
	n = pg_prop_sql_scalar(conn, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'cfa4_tmp'",
						   &failed, NULL);
	if (n && strcmp(n, "0") != 0)
		pg_prop_fail(tc, "a temporary slot whose creation was rolled back is still listed (%s)\n  %s",
					 n, bad[which]);
	if (n)
		pg_free(n);
	/* the name must be free for a real slot */
	cfe_try_noint(tc, conn, "SELECT 'init' FROM pg_create_logical_replication_slot('cfa4_tmp', 'test_decoding', true)",
				  "CF 7347 reuse of the name");
	pg_prop_sql_exec(conn, "SELECT pg_drop_replication_slot('cfa4_tmp')", NULL);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 6828: a physical slot created without reserving WAL has no restart_lsn;
 * with a finite max_slot_wal_keep_size its wal_status and safe_wal_size must
 * still be NULL (there is nothing to be safe about).  The setting is a
 * SIGHUP GUC, so the property sets it with ALTER SYSTEM and reloads, then
 * resets it.  Drawn: the keep size, reserve or not.
 */
PG_PROP(unreserved_slot_has_no_safe_wal_size)
{
	pg_prop_conn *conn = cfe_connect(tc);
	int			mb = (int) pg_prop_draw_int(tc, 1, 64);
	bool		reserve = pg_prop_draw_int(tc, 0, 3) == 0;
	StringInfoData s;
	char	   *r;
	bool		failed = false;

	initStringInfo(&s);
	appendStringInfo(&s, "ALTER SYSTEM SET max_slot_wal_keep_size = '%dMB'", mb);
	cfe_exec(tc, conn, s.data);
	cfe_exec(tc, conn, "SELECT pg_reload_conf()");
	pg_prop_sql_exec(conn, "SELECT pg_drop_replication_slot('cfa4_phys') FROM pg_replication_slots "
					 "WHERE slot_name = 'cfa4_phys'", NULL);
	resetStringInfo(&s);
	appendStringInfo(&s, "SELECT pg_create_physical_replication_slot('cfa4_phys', %s)",
					 reserve ? "true" : "false");
	cfe_exec(tc, conn, s.data);
	/* the reload is asynchronous: wait until this backend sees the value */
	for (int i = 0; i < 100; i++)
	{
		char	   *v = pg_prop_sql_scalar(conn, "SHOW max_slot_wal_keep_size", &failed, NULL);
		bool		done = v && strcmp(v, "-1") != 0;

		if (v)
			pg_free(v);
		if (done)
			break;
		cfe_exec(tc, conn, "SELECT pg_sleep(0.02)");
	}
	r = pg_prop_sql_scalar(conn,
						   "SELECT (restart_lsn IS NULL)::text || '|' || coalesce(wal_status, 'NULL') || '|' || "
						   "coalesce(safe_wal_size::text, 'NULL') FROM pg_replication_slots WHERE slot_name = 'cfa4_phys'",
						   &failed, NULL);
	pg_prop_sql_exec(conn, "SELECT pg_drop_replication_slot('cfa4_phys')", NULL);
	pg_prop_sql_exec(conn, "ALTER SYSTEM RESET max_slot_wal_keep_size", NULL);
	pg_prop_sql_exec(conn, "SELECT pg_reload_conf()", NULL);
	if (r && strncmp(r, "true|", 5) == 0 && strcmp(r, "true|NULL|NULL") != 0)
		pg_prop_fail(tc, "slot without restart_lsn reports restart_lsn-null|wal_status|safe_wal_size = %s "
					 "(max_slot_wal_keep_size %dMB)", r, mb);
	if (r)
		pg_free(r);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7432: a CALL whose error is caught by an exception block, in a
 * non-atomic loop, must not keep its CachedPlan pinned: the number of
 * CachedPlan contexts after the second round of N caught calls equals the
 * number after the first.  Drawn: N, the failing callee's shape.
 */
PG_PROP(caught_call_error_does_not_pin_plans)
{
	static const char *const bodies[] = {
		"RAISE EXCEPTION 'error %', x",
		"PERFORM 1 / (x - x)",
		"INSERT INTO cfa4_never_exists VALUES (x)",
	};
	pg_prop_conn *conn = cfe_connect(tc);
	int			n = (int) pg_prop_draw_int(tc, 2, 40);
	const char *body = bodies[pg_prop_draw_int(tc, 0, lengthof(bodies) - 1)];
	StringInfoData s;
	char	   *r;
	bool		failed = false;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "CREATE OR REPLACE PROCEDURE cfa4_err(x int) LANGUAGE plpgsql AS $p$ BEGIN %s; END $p$",
					 body);
	cfe_exec(tc, conn, s.data);
	resetStringInfo(&s);
	appendStringInfo(&s,
					 "CREATE OR REPLACE PROCEDURE cfa4_catch(INOUT kept int DEFAULT 0) LANGUAGE plpgsql AS $p$ "
					 "DECLARE b int; a int; BEGIN "
					 "FOR round IN 1..2 LOOP "
					 "b := (SELECT count(*) FROM pg_backend_memory_contexts WHERE name = 'CachedPlan'); "
					 "FOR i IN 1..%d LOOP BEGIN CALL cfa4_err(i); EXCEPTION WHEN OTHERS THEN NULL; END; END LOOP; "
					 "a := (SELECT count(*) FROM pg_backend_memory_contexts WHERE name = 'CachedPlan'); "
					 "END LOOP; kept := a - b; END $p$", n);
	cfe_exec(tc, conn, s.data);
	r = pg_prop_sql_scalar(conn, "CALL cfa4_catch()", &failed, NULL);
	if (r == NULL || failed)
		pg_prop_fail(tc, "CALL cfa4_catch() failed");
	if (strcmp(r, "0") != 0)
		pg_prop_fail(tc, "%s CachedPlans are still pinned after %d caught CALL errors (callee: %s)",
					 r, n, body);
	pg_free(r);
	pg_prop_sql_exec(conn, "DROP PROCEDURE cfa4_catch(int); DROP PROCEDURE cfa4_err(int)", NULL);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7404: streaming logical decoding of an in-progress transaction on a
 * table whose CHECK constraint is TOASTed in pg_constraint (and is followed
 * by more constraints in scan order) must decode, not fail with "unexpected
 * table_index_fetch_tuple call during logical decoding".  The failure needs
 * the relcache entry to be built during decoding, so the decoding session
 * is a fresh backend while the writing transaction is still open (as in the
 * report's reproducer).  Drawn: constraint size, constraints after it, the
 * volume written (enough or not to stream).
 *
 * A guard: neither this property nor the report's own script reproduced
 * the error on FreeBSD/arm64 at 45fc51f (both decode 10k+ changes).
 */
PG_PROP(streaming_decode_with_toasted_constraint)
{
	pg_prop_conn *conn = cfe_connect(tc);
	pg_prop_conn *w;
	pg_prop_conn *d;
	bool		failed = false;
	char	   *wl = pg_prop_sql_scalar(conn, "SHOW wal_level", &failed, NULL);
	int			big = (int) pg_prop_draw_int(tc, 100, 600);
	int			after = (int) pg_prop_draw_int(tc, 0, 3);
	int			rows = pg_prop_draw_bool(tc) ? (int) pg_prop_draw_int(tc, 2000, 12000)
		: (int) pg_prop_draw_int(tc, 1, 50);
	StringInfoData s;

	if (wl == NULL || strcmp(wl, "logical") != 0)
		pg_prop_skip(tc, "needs wal_level = logical");
	pg_free(wl);
	pg_prop_sql_exec(conn, "SELECT pg_drop_replication_slot('cfa4_ss') FROM pg_replication_slots "
					 "WHERE slot_name = 'cfa4_ss'", NULL);
	cfe_exec(tc, conn, "DROP TABLE IF EXISTS cfa4_nst, cfa4_fill");
	initStringInfo(&s);
	appendStringInfo(&s,
					 "DO $$ DECLARE extra text := ''; BEGIN "
					 "FOR k IN 1..%d LOOP extra := extra || format(', CONSTRAINT z_%%s CHECK (true)', k); END LOOP; "
					 "EXECUTE format('CREATE TABLE cfa4_nst (id int PRIMARY KEY, v text, "
					 "CONSTRAINT a_big CHECK (v <> ALL (ARRAY[%%s]))%%s)', "
					 "(SELECT string_agg(quote_literal(md5(i::text)), ',') FROM generate_series(1, %d) i), extra); "
					 "END $$; CREATE TABLE cfa4_fill (id int, pad text)",
					 after, big);
	cfe_exec(tc, conn, s.data);
	cfe_exec(tc, conn, "SELECT 'init' FROM pg_create_logical_replication_slot('cfa4_ss', 'test_decoding')");

	/* the writer: an open transaction touching cfa4_nst first */
	w = cfe_connect(tc);
	cfe_exec(tc, w, "SET logical_decoding_work_mem = '64kB'");
	cfe_exec(tc, w, "BEGIN");
	cfe_exec(tc, w, "INSERT INTO cfa4_nst VALUES (1, 'y')");
	resetStringInfo(&s);
	appendStringInfo(&s, "INSERT INTO cfa4_fill SELECT i, repeat('x', 200) FROM generate_series(1, %d) i", rows);
	cfe_exec(tc, w, s.data);

	/* the decoder: a fresh backend, cold relcache */
	d = cfe_connect(tc);
	cfe_exec(tc, d, "SET logical_decoding_work_mem = '64kB'");
	resetStringInfo(&s);
	appendStringInfo(&s, "streaming decode of an open transaction: CHECK over %d literals, "
					 "%d constraints after it, %d filler rows", big, after, rows);
	/* the bug is an elog(ERROR), XX000: cfe_try_noint fails the case on it */
	cfe_try_noint(tc, d,
				  "SELECT count(*) FROM pg_logical_slot_peek_changes('cfa4_ss', NULL, NULL, 'stream-changes', '1')",
				  s.data);
	pg_prop_disconnect(d);
	pg_prop_sql_exec(w, "ROLLBACK", NULL);
	pg_prop_disconnect(w);
	pg_prop_sql_exec(conn, "SELECT pg_drop_replication_slot('cfa4_ss')", NULL);
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS cfa4_nst, cfa4_fill", NULL);
	pfree(s.data);
	pg_prop_disconnect(conn);
}
