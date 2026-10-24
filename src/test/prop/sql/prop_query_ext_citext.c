/*-------------------------------------------------------------------------
 *
 * prop_query_ext_citext.c
 *	  Worked example: an extension augmenting the SQL query generator.
 *
 * This file is the reference for how a PostgreSQL extension plugs its own type
 * into the query-level property suite without writing a single oracle.  It
 * does three things, each through a published extension point (see pg_prop.h,
 * Extension points, and sql/query_gen.h):
 *
 *	 1. "query_gen.setup"     -- CREATE EXTENSION citext before the table is
 *							     built, so the citext type exists.
 *	 2. "query_gen.column"    -- add a citext column (ci) to the generated
 *							     table, with a value generator.
 *	 3. "query_gen.predicate" -- emit predicate leaves over ci (case-insensitive
 *							     equality and ordering), which the generator mixes
 *							     into the random predicates it builds.
 *
 * With those three hooks registered, every query-level area -- query_tlp,
 * query_tlp_agg, query_tlp_distinct, query_norec, query_cardinality, query_pqs,
 * query_planadvice -- now generates tables that contain a citext column and
 * predicates that exercise case-insensitive comparison, and checks all of its
 * existing invariants over them.  A wrong-results bug in citext's comparison
 * or hashing under a join, an aggregate, or a steered plan would be caught by
 * the same tautological oracles that cover the core types, with no new oracle
 * code here.
 *
 * It also registers one citext-specific area of its own (pattern (a)) to show
 * both styles live side by side in one file.
 *
 * This example is compiled into the pg_prop binary like any other query-gen
 * source.  A real out-of-tree extension would ship this file and arrange for
 * it to be compiled in (see src/test/prop/README, Extensions).
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_ext_citext.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_ext_citext"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/* ---- (1) setup: make citext available ------------------------------- */

static void
ext_citext_setup(pg_prop_tc *tc, const char *point, void *ctx)
{
	pg_prop_conn *conn = ctx;
	char	   *err = NULL;

	(void) point;
	if (!pg_prop_sql_exec(conn, "CREATE EXTENSION IF NOT EXISTS citext", &err))
		pg_prop_fail(tc, "citext setup failed: %s", err ? err : "");
}
PG_PROP_HOOK("query_gen.setup", ext_citext_setup);

/* ---- (2) column: a citext value generator --------------------------- */

/* Draw a short mixed-case [A-Ca-c] citext literal or SQL NULL, ~15% NULL. */
static void
draw_citext_or_null(pg_prop_tc *tc, StringInfo buf)
{
	int			len;
	int			i;

	if (pg_prop_draw_int(tc, 0, 6) == 0)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}
	len = (int) pg_prop_draw_int(tc, 0, 3);
	appendStringInfoString(buf, "'");
	for (i = 0; i < len; i++)
	{
		/* mix upper and lower so case-insensitivity actually matters */
		char		base = pg_prop_draw_bool(tc) ? 'A' : 'a';

		appendStringInfoChar(buf, (char) (base + pg_prop_draw_int(tc, 0, 2)));
	}
	appendStringInfoString(buf, "'::citext");
}

static void
ext_citext_column(pg_prop_tc *tc, const char *point, void *ctx)
{
	qg_schema  *s = ctx;

	(void) tc;
	(void) point;
	qg_add_column(s, "ci", "citext", draw_citext_or_null,
				  true /* text-like */ , false /* not numeric */ );
}
PG_PROP_HOOK("query_gen.column", ext_citext_column);

/* ---- (3) predicate: a case-insensitive leaf over ci ----------------- */

static void
ext_citext_predicate(pg_prop_tc *tc, const char *point, void *ctx)
{
	qg_pred_ctx *p = ctx;
	StringInfo	buf = p->buf;
	int			kind = (int) pg_prop_draw_int(tc, 0, 3);

	(void) point;
	switch (kind)
	{
		case 0:					/* ci = 'literal' (case-insensitive) */
			appendStringInfoString(buf, "ci = ");
			draw_citext_or_null(tc, buf);
			break;
		case 1:					/* ci <> 'literal' */
			appendStringInfoString(buf, "ci <> ");
			draw_citext_or_null(tc, buf);
			break;
		case 2:					/* ci IS NULL */
			appendStringInfoString(buf, "ci IS NULL");
			break;
		default:				/* ci < 'literal' (case-insensitive order) */
			appendStringInfoString(buf, "ci < ");
			draw_citext_or_null(tc, buf);
			break;
	}
}
PG_PROP_HOOK("query_gen.predicate", ext_citext_predicate);

/* ---- (a) a citext-specific area of this extension's own ------------- */

static pg_prop_conn *
connect_citext(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	if (!pg_prop_sql_exec(conn, "CREATE EXTENSION IF NOT EXISTS citext", &err))
		pg_prop_fail(tc, "CREATE EXTENSION citext failed: %s", err ? err : "");
	return conn;
}

/*
 * citext equality agrees with lower()-folded text equality: for any two short
 * strings, (x::citext = y::citext) must equal (lower(x) = lower(y)).  A
 * differential oracle -- two independent ways to decide the same question that
 * must always agree -- not an invented reference.
 */
PG_PROP(eq_matches_lower)
{
	pg_prop_conn *conn;
	StringInfoData x;
	StringInfoData y;
	StringInfoData sql;
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;

	conn = connect_citext(tc);

	initStringInfo(&x);
	initStringInfo(&y);
	draw_citext_or_null(tc, &x);
	draw_citext_or_null(tc, &y);
	/* strip the ::citext cast suffix for the text-side comparison */

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT (%s = %s) IS NOT DISTINCT FROM "
					 "(lower(%s::text) = lower(%s::text))",
					 x.data, y.data, x.data, y.data);

	v = pg_prop_sql_scalar(conn, sql.data, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "citext eq query failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql.data);
	if (v == NULL || strcmp(v, "t") != 0)
		pg_prop_fail(tc,
					 "citext equality disagrees with lower() folding\n  SQL: %s",
					 sql.data);

	pg_free(v);
	if (err)
		pg_free(err);
	pfree(x.data);
	pfree(y.data);
	pfree(sql.data);
	pg_prop_disconnect(conn);
}
