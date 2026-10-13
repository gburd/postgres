/*-------------------------------------------------------------------------
 *
 * prop_setops.c
 *	  Property-based tests for SQL set operations (UNION/INTERSECT/EXCEPT).
 *
 * UNION, INTERSECT and EXCEPT must obey set algebra: UNION is commutative and
 * idempotent, INTERSECT yields a subset of both inputs, EXCEPT is disjoint
 * from its right operand, and the bare operators deduplicate while their ALL
 * variants preserve duplicates.  A De Morgan-ish relation ties the two
 * together: UNION equals SELECT DISTINCT over UNION ALL.  These properties
 * check those laws over two randomly generated integer tables.
 *
 * The oracle is set algebra itself, expressed in SQL: result SETS are compared
 * with the "symmetric difference is empty" idiom
 *		NOT EXISTS (X EXCEPT Y) AND NOT EXISTS (Y EXCEPT X)
 * which is independent of row order, and bag semantics are checked by COUNT.
 * The small value range (-10..10) and 0..20 rows per table make duplicates
 * likely, so the bag-vs-set distinction the ALL variants encode is actually
 * exercised.  Tables may also be empty, exercising the edge of each law.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_setops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "setops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Value domain small enough that duplicates collide across both tables. */
#define VAL_MIN (-10)
#define VAL_MAX 10

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the set operation errored or produced NULL, which is
 * itself a bug -- so either is a failure, reported with the SQL.
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	}
	if (res == NULL)
	{
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	}
	if (strcmp(res, "t") != 0)
	{
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	}
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * Build the two integer tables setops_a(v int) and setops_b(v int), each with
 * a random number of rows (0..20) drawn from [VAL_MIN, VAL_MAX].  Duplicates
 * are likely given the narrow value range, which is the point: the set-op
 * laws must hold whether the inputs are sets or bags, and the ALL variants
 * must keep the duplicates the bare operators drop.
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
						   "DROP TABLE IF EXISTS setops_a; "
						   "DROP TABLE IF EXISTS setops_b; "
						   "CREATE TABLE setops_a (v int); "
						   "CREATE TABLE setops_b (v int); ");

	na = (int) pg_prop_draw_int(tc, 0, 20);
	for (int i = 0; i < na; i++)
		appendStringInfo(&ddl, "INSERT INTO setops_a VALUES (%d); ",
						 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));

	nb = (int) pg_prop_draw_int(tc, 0, 20);
	for (int i = 0; i < nb; i++)
		appendStringInfo(&ddl, "INSERT INTO setops_b VALUES (%d); ",
						 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));

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
						  "DROP TABLE IF EXISTS setops_a; "
						  "DROP TABLE IF EXISTS setops_b; ",
						  &err))
		pg_prop_fail(tc, "table drop failed: %s", err ? err : "(unknown)");
}

/*
 * UNION commutes: (A UNION B) equals (B UNION A) as sets.  Checked with a
 * two-way symmetric-difference test so the comparison does not depend on the
 * order the two operands' rows happen to come out.
 */
PG_PROP(union_commutative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT NOT EXISTS ("
				 "(SELECT v FROM setops_a UNION SELECT v FROM setops_b) "
				 "EXCEPT "
				 "(SELECT v FROM setops_b UNION SELECT v FROM setops_a)) "
				 "AND NOT EXISTS ("
				 "(SELECT v FROM setops_b UNION SELECT v FROM setops_a) "
				 "EXCEPT "
				 "(SELECT v FROM setops_a UNION SELECT v FROM setops_b))");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * UNION is idempotent: (A UNION A) equals SELECT DISTINCT v FROM A.  UNION
 * deduplicates, so unioning A with itself must collapse to A's distinct values
 * and add nothing.
 */
PG_PROP(union_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT NOT EXISTS ("
				 "(SELECT v FROM setops_a UNION SELECT v FROM setops_a) "
				 "EXCEPT (SELECT DISTINCT v FROM setops_a)) "
				 "AND NOT EXISTS ("
				 "(SELECT DISTINCT v FROM setops_a) "
				 "EXCEPT (SELECT v FROM setops_a UNION SELECT v FROM setops_a))");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * INTERSECT yields a subset of both inputs: (A INTERSECT B) EXCEPT A is empty,
 * and likewise with B.  Every intersected value must appear in each operand's
 * distinct set.
 */
PG_PROP(intersect_subset_of_both)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT NOT EXISTS ("
				 "(SELECT v FROM setops_a INTERSECT SELECT v FROM setops_b) "
				 "EXCEPT SELECT v FROM setops_a) "
				 "AND NOT EXISTS ("
				 "(SELECT v FROM setops_a INTERSECT SELECT v FROM setops_b) "
				 "EXCEPT SELECT v FROM setops_b)");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * EXCEPT is disjoint from its right operand: (A EXCEPT B) INTERSECT B is
 * empty.  No value removed-against B can survive and still be in B.
 */
PG_PROP(except_disjoint_from_b)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT NOT EXISTS ("
				 "(SELECT v FROM setops_a EXCEPT SELECT v FROM setops_b) "
				 "INTERSECT SELECT v FROM setops_b)");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * UNION ALL preserves duplicates: the row count of (A UNION ALL B) equals
 * count(A) + count(B).  This is the bag-semantics counterpart to the
 * deduplicating UNION, and the small value range makes real duplicates likely.
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
				 "(SELECT v FROM setops_a UNION ALL SELECT v FROM setops_b) s) "
				 "= ((SELECT count(*) FROM setops_a) "
				 "+ (SELECT count(*) FROM setops_b))");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}

/*
 * UNION equals SELECT DISTINCT over UNION ALL: the deduplicating operator is
 * exactly the bag concatenation with duplicates removed.  This is the De
 * Morgan-ish bridge between the set and bag variants, checked set-wise with a
 * two-way symmetric-difference test.
 */
PG_PROP(union_is_distinct_union_all)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_tables(tc, conn);

	require_true(tc, conn,
				 "SELECT NOT EXISTS ("
				 "(SELECT v FROM setops_a UNION SELECT v FROM setops_b) "
				 "EXCEPT (SELECT DISTINCT v FROM "
				 "(SELECT v FROM setops_a UNION ALL SELECT v FROM setops_b) s)) "
				 "AND NOT EXISTS ("
				 "(SELECT DISTINCT v FROM "
				 "(SELECT v FROM setops_a UNION ALL SELECT v FROM setops_b) s) "
				 "EXCEPT (SELECT v FROM setops_a UNION SELECT v FROM setops_b))");

	drop_tables(tc, conn);
	pg_prop_disconnect(conn);
}
