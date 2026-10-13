/*-------------------------------------------------------------------------
 *
 * prop_query_tlp_distinct.c
 *	  Query-level property tests: TLP for DISTINCT and set operations.
 *
 * The three-way ternary partition of core TLP reconstructs a *multiset*.  Once
 * you add DISTINCT and the set operators (UNION, INTERSECT, EXCEPT), the laws
 * become set laws, which exercise a different executor layer: hashing and
 * sorting for duplicate elimination, and the set-operation node that core TLP
 * only touches through UNION ALL.
 *
 * The properties here are tautologies of set/multiset semantics over the same
 * ternary partition (WHERE p / WHERE NOT p / WHERE p IS NULL):
 *
 *	 1. DISTINCT distributes over the partition up to union:
 *		  SELECT DISTINCT id FROM t
 *		  == (DISTINCT id WHERE p) UNION (DISTINCT WHERE NOT p)
 *							       UNION (DISTINCT WHERE p IS NULL)
 *		(id is unique here, but the DISTINCT/UNION machinery still runs.)
 *
 *	 2. The three partitions are pairwise disjoint, so INTERSECT of any two is
 *		empty:  (WHERE p) INTERSECT (WHERE NOT p)  ==  no rows.
 *
 *	 3. EXCEPT recovers a partition's complement within the whole:
 *		  (all ids) EXCEPT (ids WHERE p) EXCEPT (ids WHERE NOT p)
 *		  ==  ids WHERE p IS NULL     (as a set)
 *
 * A disagreement is a bug in DISTINCT, in set-operation handling, or in the
 * hash/sort path chosen for either -- never in the oracle, which invents no
 * reference value.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_tlp_distinct.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_tlp_distinct"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/* As in prop_query_tlp.c: two row-returning queries must match as text. */
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

	r1 = pg_prop_sql_rows(conn, lhs, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "lhs query failed: %s\n  SQL: %s",
					 e1 ? e1 : "(unknown)", lhs);

	r2 = pg_prop_sql_rows(conn, rhs, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "rhs query failed: %s\n  SQL: %s",
					 e2 ? e2 : "(unknown)", rhs);

	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc,
					 "set-TLP mismatch:\n  lhs: %s\n  rhs: %s\n"
					 "  lhs rows:\n%s\n  rhs rows:\n%s",
					 lhs, rhs, r1, r2);

	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * SELECT DISTINCT id FROM t  ==  the three DISTINCT partitions UNIONed.
 * Both sides ordered by id for a stable comparison.
 */
PG_PROP(distinct_over_partition)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;
	StringInfoData lhs;
	StringInfoData rhs;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	initStringInfo(&lhs);
	appendStringInfoString(&lhs,
						   "SELECT id FROM (SELECT DISTINCT id FROM " QG_TABLE
						   ") s ORDER BY id");

	initStringInfo(&rhs);
	appendStringInfo(&rhs,
					 "SELECT id FROM ("
					 "SELECT DISTINCT id FROM " QG_TABLE " WHERE (%s) "
					 "UNION SELECT DISTINCT id FROM " QG_TABLE " WHERE NOT (%s) "
					 "UNION SELECT DISTINCT id FROM " QG_TABLE " WHERE (%s) IS NULL"
					 ") s ORDER BY id",
					 pred.data, pred.data, pred.data);

	require_same_rows(tc, conn, lhs.data, rhs.data);

	pfree(pred.data);
	pfree(lhs.data);
	pfree(rhs.data);
	pg_prop_disconnect(conn);
}

/*
 * The p and NOT-p partitions are disjoint: their INTERSECT is empty.  LHS is
 * the intersect; RHS is a query that returns no rows by construction.
 */
PG_PROP(partitions_disjoint)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;
	StringInfoData lhs;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	initStringInfo(&lhs);
	appendStringInfo(&lhs,
					 "SELECT id FROM ("
					 "SELECT id FROM " QG_TABLE " WHERE (%s) "
					 "INTERSECT SELECT id FROM " QG_TABLE " WHERE NOT (%s)"
					 ") s ORDER BY id",
					 pred.data, pred.data);

	require_same_rows(tc, conn, lhs.data,
					  "SELECT id FROM " QG_TABLE " WHERE false ORDER BY id");

	pfree(pred.data);
	pfree(lhs.data);
	pg_prop_disconnect(conn);
}

/*
 * EXCEPT recovers the p-IS-NULL partition as a set:
 *	 (DISTINCT all) EXCEPT (ids WHERE p) EXCEPT (ids WHERE NOT p)
 *	 ==  DISTINCT ids WHERE p IS NULL
 */
PG_PROP(except_recovers_null_partition)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	StringInfoData pred;
	StringInfoData lhs;
	StringInfoData rhs;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);

	initStringInfo(&lhs);
	appendStringInfo(&lhs,
					 "SELECT id FROM ("
					 "SELECT id FROM " QG_TABLE " "
					 "EXCEPT SELECT id FROM " QG_TABLE " WHERE (%s) "
					 "EXCEPT SELECT id FROM " QG_TABLE " WHERE NOT (%s)"
					 ") s ORDER BY id",
					 pred.data, pred.data);

	initStringInfo(&rhs);
	appendStringInfo(&rhs,
					 "SELECT DISTINCT id FROM " QG_TABLE " WHERE (%s) IS NULL "
					 "ORDER BY id",
					 pred.data);

	require_same_rows(tc, conn, lhs.data, rhs.data);

	pfree(pred.data);
	pfree(lhs.data);
	pfree(rhs.data);
	pg_prop_disconnect(conn);
}
