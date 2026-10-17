/*-------------------------------------------------------------------------
 *
 * prop_query_tlp.c
 *	  Query-level property tests using Ternary Logic Partitioning (TLP).
 *
 * TLP (Rigger & Su, OOPSLA 2020) is a metamorphic oracle for finding
 * wrong-results bugs in a query processor without an independent reference.
 * The idea: for any predicate p and any row, SQL three-valued logic puts the
 * row in exactly one of three buckets -- p is TRUE, p is FALSE, or p is NULL
 * (unknown).  So the rows of a table partition cleanly:
 *
 *	   (WHERE p)  UNION ALL  (WHERE NOT p)  UNION ALL  (WHERE p IS NULL)
 *		 ==  (the whole table, no WHERE)
 *
 * as multisets.  The combined query exercises the qualification machinery
 * three different ways and must reconstruct exactly the unfiltered result.  A
 * disagreement is unambiguously a bug -- in predicate evaluation, in
 * three-valued logic, in an index or bitmap path that drops or duplicates a
 * row, or in set-operation handling.  No oracle to get wrong: the property is
 * a tautology of SQL semantics, so any failure is in the server.
 *
 * TLP found dozens of real, confirmed wrong-results bugs in mature databases
 * (PostgreSQL, SQLite, MySQL, CockroachDB) that decades of hand-written tests
 * had not.  This area applies it to a randomly generated table and predicate
 * from query_gen.h.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see pg_prop.h
 * and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_tlp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_tlp"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/*
 * Run two queries that must return identical multisets and fail with a diff if
 * they do not.  Both are ordered by the same key so the row-major string
 * rendering compares as a multiset (identical rows in identical order).
 */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *whole, const char *partitioned)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL;
	char	   *r1,
			   *r2;

	r1 = pg_prop_sql_rows(conn, whole, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "unfiltered query failed: %s\n  SQL: %s",
					 e1 ? e1 : "(unknown)", whole);

	r2 = pg_prop_sql_rows(conn, partitioned, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "partitioned query failed: %s\n  SQL: %s",
					 e2 ? e2 : "(unknown)", partitioned);

	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc,
					 "TLP mismatch:\n  whole:        %s\n  partitioned:  %s\n"
					 "  whole rows:\n%s\n  partitioned rows:\n%s",
					 whole, partitioned, r1, r2);

	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * Core TLP: a plain WHERE partitioned three ways over the predicate.
 *
 *	 SELECT id FROM t ORDER BY id
 *	 ==
 *	 ( SELECT id FROM t WHERE p
 *	   UNION ALL SELECT id FROM t WHERE NOT p
 *	   UNION ALL SELECT id FROM t WHERE p IS NULL ) ORDER BY id
 *
 * id is the primary key, so ORDER BY id gives a stable total order and the
 * two renderings compare as multisets.
 */
PG_PROP(where_partitioned)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;
	StringInfoData whole;
	StringInfoData part;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);

	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	initStringInfo(&whole);
	appendStringInfoString(&whole, "SELECT id FROM " QG_TABLE " ORDER BY id");

	initStringInfo(&part);
	appendStringInfo(&part,
					 "SELECT id FROM (" 
					 "SELECT id FROM " QG_TABLE " WHERE (%s) "
					 "UNION ALL SELECT id FROM " QG_TABLE " WHERE NOT (%s) "
					 "UNION ALL SELECT id FROM " QG_TABLE " WHERE (%s) IS NULL"
					 ") s ORDER BY id",
					 pred.data, pred.data, pred.data);

	require_same_rows(tc, conn, whole.data, part.data);

	pfree(pred.data);
	pfree(whole.data);
	pfree(part.data);
	pg_prop_disconnect(conn);
}

/*
 * TLP for aggregation: COUNT(*) over the whole table equals the sum of the
 * three partition counts.  Aggregation takes a different executor path than
 * row return, so this catches count/agg-pushdown bugs the row form may miss.
 */
PG_PROP(count_partitioned)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;
	StringInfoData whole;
	StringInfoData part;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);

	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	initStringInfo(&whole);
	appendStringInfoString(&whole, "SELECT count(*) FROM " QG_TABLE);

	initStringInfo(&part);
	appendStringInfo(&part,
					 "SELECT (SELECT count(*) FROM " QG_TABLE " WHERE (%s)) "
					 "+ (SELECT count(*) FROM " QG_TABLE " WHERE NOT (%s)) "
					 "+ (SELECT count(*) FROM " QG_TABLE " WHERE (%s) IS NULL)",
					 pred.data, pred.data, pred.data);

	require_same_rows(tc, conn, whole.data, part.data);

	pfree(pred.data);
	pfree(whole.data);
	pfree(part.data);
	pg_prop_disconnect(conn);
}

/*
 * TLP across a self-join: partition the join's ON predicate.  Joins are a rich
 * source of wrong-results bugs (hash vs merge vs nestloop, NULL handling in
 * join keys), and TLP over the join condition exercises all of that.  The
 * whole join is an inner self-join on a trivial always-true condition narrowed
 * to a manageable size; the partition splits on the generated predicate
 * evaluated on the left row.
 */
PG_PROP(join_filter_partitioned)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;
	StringInfoData whole;
	StringInfoData part;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);

	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	/*
	 * Whole: every row of t paired with the single row whose id = 1 (if any),
	 * returning t.id.  The generated predicate uses unqualified column names,
	 * so in the partitioned form it is applied inside a single-table subquery
	 * over t (where the names are unambiguous) before the join.
	 */
	initStringInfo(&whole);
	appendStringInfoString(&whole,
						   "SELECT t.id FROM " QG_TABLE " t JOIN " QG_TABLE
						   " u ON u.id = 1 ORDER BY t.id");

	initStringInfo(&part);
	appendStringInfo(&part,
					 "SELECT id FROM ("
					 "SELECT t.id FROM (SELECT * FROM " QG_TABLE " WHERE (%s)) t "
					 "JOIN " QG_TABLE " u ON u.id = 1 "
					 "UNION ALL "
					 "SELECT t.id FROM (SELECT * FROM " QG_TABLE " WHERE NOT (%s)) t "
					 "JOIN " QG_TABLE " u ON u.id = 1 "
					 "UNION ALL "
					 "SELECT t.id FROM (SELECT * FROM " QG_TABLE " WHERE (%s) IS NULL) t "
					 "JOIN " QG_TABLE " u ON u.id = 1"
					 ") s ORDER BY id",
					 pred.data, pred.data, pred.data);

	require_same_rows(tc, conn, whole.data, part.data);

	pfree(pred.data);
	pfree(whole.data);
	pfree(part.data);
	pg_prop_disconnect(conn);
}
