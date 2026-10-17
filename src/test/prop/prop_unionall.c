/*-------------------------------------------------------------------------
 *
 * prop_unionall.c
 *	  Property-based tests for UNION ALL / Append / MergeAppend semantics.
 *
 * UNION ALL is bag concatenation: it preserves every row and every row's
 * multiplicity, and the planner implements it with an Append (or, when a
 * common ORDER BY is pushed down, a MergeAppend that keeps the result
 * globally sorted).  These properties pin down the laws that distinguish
 * UNION ALL from the deduplicating set operators: additive count and sum,
 * doubled multiplicity for self-concatenation, non-decreasing output under a
 * common ORDER BY, multiset commutativity, and the empty/self edge cases.  A
 * failure means Append/MergeAppend dropped, duplicated, reordered or mis-
 * summed rows -- a wrong-results bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_unionall.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "unionall"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Narrow value domain so duplicates collide and multiplicity matters. */
#define VAL_MIN (-10)
#define VAL_MAX 10

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operation errored or produced NULL, which is itself a
 * bug -- so either is a failure, reported with the SQL.
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * Run two result-set queries and require identical rows in identical order.
 * Both queries MUST carry a total ORDER BY so the row-major string compares
 * as a multiset.
 */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *q1, const char *q2)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *r1,
			   *r2;

	r1 = pg_prop_sql_rows(conn, q1, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "q1 failed: %s\n  SQL: %s", e1 ? e1 : "", q1);
	r2 = pg_prop_sql_rows(conn, q2, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "q2 failed: %s\n  SQL: %s", e2 ? e2 : "", q2);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc, "mismatch:\n  q1: %s\n  q2: %s\n  r1:\n%s\n  r2:\n%s",
					 q1, q2, r1, r2);
	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/* Draw a value column cell: SQL NULL ~15% of the time, else [VAL_MIN,VAL_MAX]. */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/*
 * Build ua_a(id int, v int) and ua_b(id int, v int), each with a random
 * number of rows (0..20).  id is a dense 1..n key per table; v is drawn from
 * [VAL_MIN, VAL_MAX] with ~15% NULLs so both duplicate values and NULL
 * handling are exercised.  Rebuilt fresh in each property.
 */
static void
build_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			na;
	int			nb;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS ua_a; "
						   "DROP TABLE IF EXISTS ua_b; "
						   "CREATE TABLE ua_a (id int, v int); "
						   "CREATE TABLE ua_b (id int, v int); ");

	na = (int) pg_prop_draw_int(tc, 0, 20);
	for (int i = 0; i < na; i++)
	{
		StringInfoData vv;

		initStringInfo(&vv);
		draw_val(tc, &vv);
		appendStringInfo(&ddl, "INSERT INTO ua_a VALUES (%d, %s); ", i + 1, vv.data);
		pfree(vv.data);
	}

	nb = (int) pg_prop_draw_int(tc, 0, 20);
	for (int i = 0; i < nb; i++)
	{
		StringInfoData vv;

		initStringInfo(&vv);
		draw_val(tc, &vv);
		appendStringInfo(&ddl, "INSERT INTO ua_b VALUES (%d, %s); ", i + 1, vv.data);
		pfree(vv.data);
	}

	appendStringInfoString(&ddl, "ANALYZE ua_a; ANALYZE ua_b; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);

	pfree(ddl.data);
}

/* Drop both tables so no state leaks between cases. */
static void
drop_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn,
						  "DROP TABLE IF EXISTS ua_a; "
						  "DROP TABLE IF EXISTS ua_b; ",
						  &err))
		pg_prop_fail(tc, "table drop failed: %s", err ? err : "(unknown)");
}

/*
 * (A) count(A UNION ALL B) = count(A) + count(B).  UNION ALL concatenates
 * bags, so the Append must emit exactly every row of both inputs.
 */
PG_PROP(union_all_count_additive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT v FROM ua_a UNION ALL SELECT v FROM ua_b) s) "
				 "= ((SELECT count(*) FROM ua_a) "
				 "+ (SELECT count(*) FROM ua_b))");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * (A) sum(v) over (A UNION ALL B) = sum(A.v) + sum(B.v).  coalesce guards the
 * empty / all-NULL case where sum() is SQL NULL; UNION ALL preserving every
 * row means the summed total must be additive across the two inputs.
 */
PG_PROP(union_all_sum_additive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce((SELECT sum(v) FROM "
				 "(SELECT v FROM ua_a UNION ALL SELECT v FROM ua_b) s), 0) "
				 "= (coalesce((SELECT sum(v) FROM ua_a), 0) "
				 "+ coalesce((SELECT sum(v) FROM ua_b), 0))");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * (B) A UNION ALL A doubles every value's multiplicity: grouping over
 * (ua_a UNION ALL ua_a) must give exactly 2*count(*) per value compared with
 * grouping over ua_a alone.  ORDER BY v NULLS FIRST gives a total order over
 * the one grouping column so the row strings compare as a multiset.
 */
PG_PROP(union_all_preserves_multiplicity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT v, count(*) FROM "
					  "(SELECT v FROM ua_a UNION ALL SELECT v FROM ua_a) s "
					  "GROUP BY v ORDER BY v NULLS FIRST",
					  "SELECT v, 2*count(*) FROM ua_a "
					  "GROUP BY v ORDER BY v NULLS FIRST");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * (B->A) A UNION ALL with a common ORDER BY is planned as a MergeAppend and
 * its output must be globally sorted.  Checked soundly with a window lag():
 * for every row, the previous value (in NULLS LAST order) is NULL, is <= the
 * current value, or the current value is NULL (NULLs sort last) -- i.e. the
 * sequence is non-decreasing.  bool_and over the whole stream, coalesced to
 * true for the empty case.
 */
PG_PROP(mergeappend_ordered)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT coalesce(bool_and("
				 "prev IS NULL OR prev <= v OR v IS NULL), true) FROM "
				 "(SELECT v, lag(v) OVER (ORDER BY v NULLS LAST) prev FROM "
				 "(SELECT v FROM ua_a UNION ALL SELECT v FROM ua_b) s) q");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * (B) Append commutes as a multiset: (A UNION ALL B) and (B UNION ALL A) hold
 * the same values with the same multiplicities, even though the Append may
 * emit them in a different physical order.  Grouped by value with a total
 * ORDER BY v NULLS FIRST so the comparison is order-insensitive on rows but
 * exact on counts.
 */
PG_PROP(append_commutes_as_multiset)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT v, count(*) FROM "
					  "(SELECT v FROM ua_a UNION ALL SELECT v FROM ua_b) s "
					  "GROUP BY v ORDER BY v NULLS FIRST",
					  "SELECT v, count(*) FROM "
					  "(SELECT v FROM ua_b UNION ALL SELECT v FROM ua_a) s "
					  "GROUP BY v ORDER BY v NULLS FIRST");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * (A) An empty left branch is the identity for UNION ALL: concatenating an
 * empty relation with ua_b yields exactly count(ua_b) rows.  Guards the
 * Append's handling of a zero-row child.
 */
PG_PROP(empty_union_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "((SELECT v FROM ua_a WHERE false) "
				 "UNION ALL SELECT v FROM ua_b) s) "
				 "= (SELECT count(*) FROM ua_b)");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * (A) count(A UNION ALL A) = 2*count(A).  Self-concatenation doubles the row
 * count exactly; a MergeAppend/Append that deduplicated or dropped a child
 * would break this.
 */
PG_PROP(union_all_self_double_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT (SELECT count(*) FROM "
				 "(SELECT v FROM ua_a UNION ALL SELECT v FROM ua_a) s) "
				 "= (2 * (SELECT count(*) FROM ua_a))");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}
