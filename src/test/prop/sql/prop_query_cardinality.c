/*-------------------------------------------------------------------------
 *
 * prop_query_cardinality.c
 *	  Query-level property tests: result-cardinality monotonicity laws.
 *
 * These are metamorphic relations on the *size* of a result set rather than
 * its contents, so they are cheap (one count per query) yet catch a broad
 * class of qualification and set-operation wrong-results bugs:
 *
 *	 1. Adding a conjunct never grows the result:
 *		  count(WHERE p)        >=  count(WHERE p AND q)
 *	 2. Adding a disjunct never shrinks it:
 *		  count(WHERE p)        <=  count(WHERE p OR q)
 *	 3. A predicate and its negation and its NULL-set tile the table exactly:
 *		  count(p) + count(NOT p) + count(p IS NULL)  ==  count(*)
 *	 4. DISTINCT never grows a projection:
 *		  count(DISTINCT e)     <=  count(all e)
 *	 5. UNION ALL is additive; UNION (set) is no larger:
 *		  count(A UNION ALL B)  ==  count(A) + count(B)
 *		  count(A UNION B)      <=  count(A UNION ALL B)
 *
 * Each is a theorem of relational algebra, so a violation is a server bug in
 * the planner's choice of plan or the executor's row counting -- not an oracle
 * that could be miscalibrated.  Counting rather than comparing contents also
 * lets these run against larger generated predicates without the comparison
 * cost of full row rendering.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_cardinality.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_cardinality"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/* Run a scalar COUNT query, failing the case on SQL error; return the count. */
static long
count_of(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *val;
	long		n;

	val = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "count query failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	n = atol(val);
	pg_free(val);
	if (err)
		pg_free(err);
	return n;
}

/* count(WHERE p) >= count(WHERE p AND q); and <= count(WHERE p OR q). */
PG_PROP(conjunct_shrinks_disjunct_grows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData p;
	StringInfoData q;
	StringInfoData sql;
	long		cp,
				cpq_and,
				cpq_or;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&p);
	qg_draw_predicate(tc, &p);
	initStringInfo(&q);
	qg_draw_predicate(tc, &q);
	initStringInfo(&sql);

	appendStringInfo(&sql, "SELECT count(*) FROM " QG_TABLE " WHERE (%s)", p.data);
	cp = count_of(tc, conn, sql.data);

	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(*) FROM " QG_TABLE
					 " WHERE (%s) AND (%s)", p.data, q.data);
	cpq_and = count_of(tc, conn, sql.data);

	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(*) FROM " QG_TABLE
					 " WHERE (%s) OR (%s)", p.data, q.data);
	cpq_or = count_of(tc, conn, sql.data);

	if (!(cpq_and <= cp))
		pg_prop_fail(tc,
					 "conjunct grew result: count(p AND q)=%ld > count(p)=%ld\n"
					 "  p: %s\n  q: %s", cpq_and, cp, p.data, q.data);
	if (!(cpq_or >= cp))
		pg_prop_fail(tc,
					 "disjunct shrank result: count(p OR q)=%ld < count(p)=%ld\n"
					 "  p: %s\n  q: %s", cpq_or, cp, p.data, q.data);

	pfree(p.data);
	pfree(q.data);
	pfree(sql.data);
	pg_prop_disconnect(conn);
}

/* count(p) + count(NOT p) + count(p IS NULL) == count(*). */
PG_PROP(partition_tiles_table)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData p;
	StringInfoData sql;
	long		total,
				cp,
				cnp,
				cnull;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&p);
	qg_draw_predicate(tc, &p);
	initStringInfo(&sql);

	total = count_of(tc, conn, "SELECT count(*) FROM " QG_TABLE);

	appendStringInfo(&sql, "SELECT count(*) FROM " QG_TABLE " WHERE (%s)", p.data);
	cp = count_of(tc, conn, sql.data);
	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(*) FROM " QG_TABLE " WHERE NOT (%s)", p.data);
	cnp = count_of(tc, conn, sql.data);
	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(*) FROM " QG_TABLE " WHERE (%s) IS NULL", p.data);
	cnull = count_of(tc, conn, sql.data);

	if (cp + cnp + cnull != total)
		pg_prop_fail(tc,
					 "partition does not tile: %ld + %ld + %ld != %ld\n  p: %s",
					 cp, cnp, cnull, total, p.data);

	pfree(p.data);
	pfree(sql.data);
	pg_prop_disconnect(conn);
}

/*
 * count(A UNION ALL B) == count(A) + count(B), and count(A UNION B) <= that,
 * where A = WHERE p and B = WHERE q over id projections.
 */
PG_PROP(union_cardinalities)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData p;
	StringInfoData q;
	StringInfoData sql;
	long		ca,
				cb,
				call_,
				cset;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&p);
	qg_draw_predicate(tc, &p);
	initStringInfo(&q);
	qg_draw_predicate(tc, &q);
	initStringInfo(&sql);

	appendStringInfo(&sql, "SELECT count(*) FROM " QG_TABLE " WHERE (%s)", p.data);
	ca = count_of(tc, conn, sql.data);
	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(*) FROM " QG_TABLE " WHERE (%s)", q.data);
	cb = count_of(tc, conn, sql.data);

	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT count(*) FROM ("
					 "SELECT id FROM " QG_TABLE " WHERE (%s) "
					 "UNION ALL SELECT id FROM " QG_TABLE " WHERE (%s)) s",
					 p.data, q.data);
	call_ = count_of(tc, conn, sql.data);

	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT count(*) FROM ("
					 "SELECT id FROM " QG_TABLE " WHERE (%s) "
					 "UNION SELECT id FROM " QG_TABLE " WHERE (%s)) s",
					 p.data, q.data);
	cset = count_of(tc, conn, sql.data);

	if (call_ != ca + cb)
		pg_prop_fail(tc,
					 "UNION ALL not additive: count=%ld != %ld + %ld\n"
					 "  p: %s\n  q: %s", call_, ca, cb, p.data, q.data);
	if (!(cset <= call_))
		pg_prop_fail(tc,
					 "UNION larger than UNION ALL: %ld > %ld\n  p: %s\n  q: %s",
					 cset, call_, p.data, q.data);

	pfree(p.data);
	pfree(q.data);
	pfree(sql.data);
	pg_prop_disconnect(conn);
}

/* count(DISTINCT e) <= count(all e) for a generated int projection e. */
PG_PROP(distinct_never_grows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData e;
	StringInfoData sql;
	long		call_,
				cdistinct;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&e);
	qg_draw_int_expr(tc, &e);
	initStringInfo(&sql);

	appendStringInfo(&sql, "SELECT count(%s) FROM " QG_TABLE, e.data);
	call_ = count_of(tc, conn, sql.data);
	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(DISTINCT %s) FROM " QG_TABLE, e.data);
	cdistinct = count_of(tc, conn, sql.data);

	if (!(cdistinct <= call_))
		pg_prop_fail(tc,
					 "DISTINCT grew count: count(distinct e)=%ld > count(e)=%ld\n"
					 "  e: %s", cdistinct, call_, e.data);

	pfree(e.data);
	pfree(sql.data);
	pg_prop_disconnect(conn);
}
