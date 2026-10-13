/*-------------------------------------------------------------------------
 *
 * prop_subquery.c
 *	  Property-based tests for subquery flattening and pull-up equivalences.
 *
 * The planner rewrites subqueries in many ways -- an EXISTS into a semijoin,
 * an IN into an EXISTS, a NOT EXISTS into an antijoin, a correlated scalar
 * subquery into a grouped join, a derived table or CTE into an inline scan.
 * Every such rewrite must be result-preserving: two semantically equivalent
 * forms of the same query must return exactly the same rows.  The oracle here
 * is differential -- two hand-written equivalent query forms, compared with
 * require_same_rows under a total ORDER BY -- so any disagreement is a
 * flattening/pull-up bug.
 *
 * NULL semantics are the trap in this area.  Property in_vs_exists restricts
 * the IN subquery to non-NULL values so that IN == EXISTS stays sound (a NULL
 * in an IN list changes three-valued-logic semantics and is a different, much
 * trickier property, deliberately kept out).  not_exists_vs_antijoin compares
 * NOT EXISTS against the set complement via EXCEPT rather than a hand-rolled
 * LEFT JOIN ... IS NULL antijoin, which is only equivalent when the join key
 * never produces a spurious NULL match.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_subquery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "subquery"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* Small integer domain for a and b; narrow so matches between rows are dense. */
#define VAL_MIN (-4)
#define VAL_MAX 4

/* Draw an integer in [VAL_MIN, VAL_MAX], or SQL NULL (~15% of the time). */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/*
 * Rebuild sq_t fresh: id int PRIMARY KEY (0..n-1), a int, b int, both in
 * [-4,4] with ~15% NULL.  0..25 rows.  ANALYZE so the planner has stats to
 * pick whichever flattening/pull-up strategy it likes -- that choice is
 * exactly what the differential oracle is probing.
 */
static void
build_table(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS sq_t; "
						   "CREATE TABLE sq_t (id int PRIMARY KEY, a int, b int); ");

	nrows = (int) pg_prop_draw_int(tc, 0, 25);
	for (int r = 0; r < nrows; r++)
	{
		StringInfoData a;
		StringInfoData b;

		initStringInfo(&a);
		initStringInfo(&b);
		draw_val(tc, &a);
		draw_val(tc, &b);
		appendStringInfo(&ddl, "INSERT INTO sq_t VALUES (%d, %s, %s); ",
						 r, a.data, b.data);
		pfree(a.data);
		pfree(b.data);
	}
	appendStringInfoString(&ddl, "ANALYZE sq_t; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "table build failed: %s\n  ddl: %s",
					 err ? err : "(unknown)", ddl.data);
	pfree(ddl.data);
}

/*
 * Differential oracle: run two equivalent query forms and require their full
 * result sets (newline-joined, row-major) to be byte-identical.  Both queries
 * must carry a total ORDER BY so the comparison is a sound multiset compare.
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

/*
 * 1. EXISTS == semijoin.  WHERE EXISTS (SELECT 1 ... u.a = t.b) selects each
 * t exactly once when at least one matching u exists; the DISTINCT join is the
 * explicit semijoin form.  These must agree for every table.
 */
PG_PROP(exists_vs_semijoin)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM sq_t t "
					  "WHERE EXISTS (SELECT 1 FROM sq_t u WHERE u.a = t.b) "
					  "ORDER BY id",
					  "SELECT DISTINCT t.id FROM sq_t t "
					  "JOIN sq_t u ON u.a = t.b "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * 2. IN == EXISTS, over a non-NULL IN subquery.  WHERE a IN (SELECT b ... b
 * IS NOT NULL) is equivalent to the correlated EXISTS form only once the IN
 * list has no NULL (a NULL in the list would turn a non-match into UNKNOWN
 * rather than false), so both sides restrict the inner b to non-NULL.
 */
PG_PROP(in_vs_exists)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM sq_t "
					  "WHERE a IN (SELECT b FROM sq_t WHERE b IS NOT NULL) "
					  "ORDER BY id",
					  "SELECT id FROM sq_t t "
					  "WHERE EXISTS (SELECT 1 FROM sq_t u "
					  "WHERE u.b = t.a AND u.b IS NOT NULL) "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * 3. NOT EXISTS == set complement.  Rather than a hand-rolled LEFT JOIN ...
 * IS NULL antijoin (unsound when the key can match NULL), compare NOT EXISTS
 * against the complement of the matching set computed with EXCEPT: the rows
 * NOT selected by the positive EXISTS are exactly the rows selected by NOT
 * EXISTS.  Both are grounded in the same table so EXCEPT's set semantics are
 * sound here.
 */
PG_PROP(not_exists_vs_antijoin)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM sq_t t "
					  "WHERE NOT EXISTS (SELECT 1 FROM sq_t u WHERE u.a = t.b) "
					  "ORDER BY id",
					  "SELECT id FROM sq_t "
					  "EXCEPT "
					  "SELECT t.id FROM sq_t t "
					  "WHERE EXISTS (SELECT 1 FROM sq_t u WHERE u.a = t.b) "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * 4. Correlated scalar subquery == grouped left join count.  (SELECT count(*)
 * ... u.a = t.a) per outer row equals count(u.*) from a LEFT JOIN grouped by
 * t.id: the LEFT JOIN keeps every t, count(u.*) counts only matching u rows
 * (0 when none), matching count(*) over the correlated set.  Both order by id.
 */
PG_PROP(scalar_subquery_pullup)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id, (SELECT count(*) FROM sq_t u WHERE u.a = t.a) AS c "
					  "FROM sq_t t ORDER BY id",
					  "SELECT t.id, count(u.*) FROM sq_t t "
					  "LEFT JOIN sq_t u ON u.a = t.a "
					  "GROUP BY t.id ORDER BY t.id");

	pg_prop_disconnect(conn);
}

/*
 * 5. Derived table flatten.  A subquery in FROM with its own predicate is
 * equivalent to the same scan with the predicates ANDed inline; the planner
 * routinely flattens the former into the latter.
 */
PG_PROP(derived_table_flatten)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "SELECT id FROM (SELECT id, a FROM sq_t WHERE a > 0) s "
					  "WHERE s.a < 4 ORDER BY id",
					  "SELECT id FROM sq_t WHERE a > 0 AND a < 4 ORDER BY id");

	pg_prop_disconnect(conn);
}

/*
 * 6. CTE == inline subquery.  A non-recursive, single-reference CTE is now
 * inlinable; the result must match the equivalent inline form with the CTE's
 * predicate ANDed into the base scan.
 */
PG_PROP(cte_vs_subquery)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build_table(tc, conn);

	require_same_rows(tc, conn,
					  "WITH w AS (SELECT id, a FROM sq_t WHERE a IS NOT NULL) "
					  "SELECT id FROM w WHERE a > 0 ORDER BY id",
					  "SELECT id FROM sq_t WHERE a IS NOT NULL AND a > 0 "
					  "ORDER BY id");

	pg_prop_disconnect(conn);
}
