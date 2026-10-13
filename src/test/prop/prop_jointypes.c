/*-------------------------------------------------------------------------
 *
 * prop_jointypes.c
 *	  Property-based tests for USING / NATURAL / ON join equivalences.
 *
 * The syntactic forms of a join (JOIN ... USING (k), NATURAL JOIN, JOIN ...
 * ON a.k = b.k) are defined to produce the same rows when they reference the
 * same columns.  These build two tables whose only common column is k and
 * check those equivalences differentially: any disagreement is a bug in how
 * the parser expands USING/NATURAL into join quals, or in the merged-column
 * projection that USING/NATURAL produce.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see pg_prop.h
 * and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jointypes.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jointypes"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a k value in [0,3] or SQL NULL (~15%).  A small domain with NULLs
 * exercises both matching and the NULL-key behaviour (a plain equi-join never
 * matches NULL to NULL).
 */
static void
draw_k(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, 0, 3));
}

/*
 * Build jt_a(ida, k, x) and jt_b(idb, k, y).  The id columns are named
 * differently so k is the ONLY column the two tables share -- that is what
 * makes NATURAL JOIN join on k alone.
 */
static void
build_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	char	   *err = NULL;
	StringInfoData ddl;
	int			na,
				nb,
				r;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS jt_a; DROP TABLE IF EXISTS jt_b; "
						   "CREATE TABLE jt_a (ida int PRIMARY KEY, k int, x int); "
						   "CREATE TABLE jt_b (idb int PRIMARY KEY, k int, y int); ");

	na = (int) pg_prop_draw_int(tc, 0, 15);
	for (r = 1; r <= na; r++)
	{
		appendStringInfo(&ddl, "INSERT INTO jt_a VALUES (%d, ", r);
		draw_k(tc, &ddl);
		appendStringInfo(&ddl, ", %d); ", (int) pg_prop_draw_int(tc, -5, 5));
	}
	nb = (int) pg_prop_draw_int(tc, 0, 15);
	for (r = 1; r <= nb; r++)
	{
		appendStringInfo(&ddl, "INSERT INTO jt_b VALUES (%d, ", r);
		draw_k(tc, &ddl);
		appendStringInfo(&ddl, ", %d); ", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(&ddl, "ANALYZE jt_a; ANALYZE jt_b;");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s", err ? err : "");
	pfree(ddl.data);
}

/* Two queries must return identical ordered rows. */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *q1, const char *q2)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL;
	char	   *r1,
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

/* USING (k) equals ON a.k = b.k for an inner join. */
PG_PROP(using_eq_on)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT ida, idb FROM jt_a a JOIN jt_b b USING (k) "
					  "ORDER BY ida, idb",
					  "SELECT ida, idb FROM jt_a a JOIN jt_b b ON a.k = b.k "
					  "ORDER BY ida, idb");

	pg_prop_disconnect(conn);
}

/* NATURAL JOIN joins on the only common column k, so it equals USING (k). */
PG_PROP(natural_eq_using)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT ida, idb FROM jt_a NATURAL JOIN jt_b "
					  "ORDER BY ida, idb",
					  "SELECT ida, idb FROM jt_a JOIN jt_b USING (k) "
					  "ORDER BY ida, idb");

	pg_prop_disconnect(conn);
}

/* USING (k) merges k into a single output column equal to a.k on an inner join. */
PG_PROP(using_single_k_column)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT k FROM jt_a a JOIN jt_b b USING (k) ORDER BY k",
					  "SELECT a.k FROM jt_a a JOIN jt_b b ON a.k = b.k "
					  "ORDER BY a.k");

	pg_prop_disconnect(conn);
}

/*
 * In a LEFT JOIN USING (k), the merged k column is the coalesce of the two
 * sides; since every left row is kept and the right k is NULL only where
 * there was no match (and the left k drove the match), the merged k equals the
 * left k.
 */
PG_PROP(left_join_using_coalesces)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT k FROM jt_a LEFT JOIN jt_b USING (k) "
					  "ORDER BY k NULLS LAST",
					  "SELECT a.k FROM jt_a a LEFT JOIN jt_b b ON a.k = b.k "
					  "ORDER BY a.k NULLS LAST");

	pg_prop_disconnect(conn);
}

/* CROSS JOIN equals JOIN ... ON true. */
PG_PROP(cross_eq_join_true)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	build_tables(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT ida, idb FROM jt_a a CROSS JOIN jt_b b "
					  "ORDER BY ida, idb",
					  "SELECT ida, idb FROM jt_a a JOIN jt_b b ON true "
					  "ORDER BY ida, idb");

	pg_prop_disconnect(conn);
}
