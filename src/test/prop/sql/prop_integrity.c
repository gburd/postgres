/*-------------------------------------------------------------------------
 *
 * prop_integrity.c
 *	  Property tests using the integrity-checking contrib extensions as
 *	  oracles (amcheck, pg_visibility, pgstattuple, pageinspect).
 *
 * These extensions exist precisely to assert that an index or table is
 * internally well-formed, which makes them powerful property oracles: for ANY
 * table and index the generator can build, and after any sequence of DML the
 * generator can apply, the structure the server produced must pass its own
 * self-check.  A failure here is a storage- or access-method-level corruption
 * bug, not a wrong-results bug -- a different and valuable class.
 *
 *	 bt_index_check     -- a freshly built btree index over random data has no
 *					      structural corruption (ordering, sibling links, ...).
 *	 verify_heapam      -- a random heap, after inserts/updates/deletes, has no
 *					      heap/toast corruption (verify_heapam returns no rows).
 *	 vm_consistent      -- after VACUUM, pg_check_visible / pg_check_frozen
 *					      report no incorrectly-set visibility-map bits.
 *	 stattuple_bounds   -- pgstattuple's live/dead/free byte counts sum within
 *					      the table's size and are non-negative.
 *
 * Availability: each property requires its contrib extension and pg_prop_skips
 * if it is not installed, so the area runs whatever the server offers.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_integrity.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "integrity"
#include "pg_prop.h"

#include "lib/stringinfo.h"

#define INT_MAX_ROWS 200
#define INT_K_MIN (-20)
#define INT_K_MAX 20

static long
scalar_long(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	long		n;

	v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "scalar failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	n = v ? atol(v) : 0;
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	return n;
}

/* Build integ_t(id serial pk, k int, pl text) with random rows + churn. */
static void
build_and_churn(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, INT_MAX_ROWS);
	int			i;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "SET client_min_messages = warning; "
						   "DROP TABLE IF EXISTS integ_t; "
						   "CREATE TABLE integ_t (id int PRIMARY KEY, k int, pl text); ");
	for (i = 0; i < n; i++)
		appendStringInfo(&ddl,
						 "INSERT INTO integ_t VALUES (%d, %d, repeat('x', %d)); ",
						 i, (int) pg_prop_draw_int(tc, INT_K_MIN, INT_K_MAX),
						 (int) pg_prop_draw_int(tc, 0, 3000));
	/* churn: delete and update a fraction, so the heap has dead tuples */
	appendStringInfo(&ddl,
					 "DELETE FROM integ_t WHERE k = %d; "
					 "UPDATE integ_t SET k = k + 1 WHERE k < %d; ",
					 (int) pg_prop_draw_int(tc, INT_K_MIN, INT_K_MAX),
					 (int) pg_prop_draw_int(tc, INT_K_MIN, INT_K_MAX));
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "build/churn failed: %s", err ? err : "");
	pfree(ddl.data);
}

static pg_prop_conn *
connect_integ(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

/* A freshly built btree over random data passes amcheck's bt_index_check. */
PG_PROP(btree_amcheck)
{
	pg_prop_conn *conn = connect_integ(tc);
	char	   *err = NULL;

	if (!pg_prop_require_extension(conn, "amcheck"))
		pg_prop_skip(tc, "amcheck not available");
	build_and_churn(tc, conn);
	if (!pg_prop_sql_exec(conn,
						  "DROP INDEX IF EXISTS integ_idx; "
						  "CREATE INDEX integ_idx ON integ_t (k);", &err))
		pg_prop_fail(tc, "index build failed: %s", err ? err : "");
	/*
	 * bt_index_check returns void and raises on structural corruption; call
	 * it directly (not inside a dead subquery, which the planner would elide)
	 * so it actually executes.  A SQL error here is the corruption signal.
	 */
	if (!pg_prop_sql_exec(conn,
						  "SELECT bt_index_check('integ_idx'::regclass)", &err))
		pg_prop_fail(tc, "bt_index_check failed: %s", err ? err : "");
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/* A random heap after churn passes verify_heapam (no corruption rows). */
PG_PROP(heap_verify)
{
	pg_prop_conn *conn = connect_integ(tc);
	long		corrupt;

	if (!pg_prop_require_extension(conn, "amcheck"))
		pg_prop_skip(tc, "amcheck not available");
	build_and_churn(tc, conn);
	/* verify_heapam returns one row per corruption found; expect zero */
	corrupt = scalar_long(tc, conn,
						  "SELECT count(*) FROM verify_heapam('integ_t'::regclass)");
	if (corrupt != 0)
		pg_prop_fail(tc, "verify_heapam reported %ld corruption rows", corrupt);
	pg_prop_disconnect(conn);
}

/* After VACUUM, the visibility map has no incorrectly-set bits. */
PG_PROP(vm_consistent)
{
	pg_prop_conn *conn = connect_integ(tc);
	char	   *err = NULL;
	long		badvis,
				badfroz;

	if (!pg_prop_require_extension(conn, "pg_visibility"))
		pg_prop_skip(tc, "pg_visibility not available");
	build_and_churn(tc, conn);
	if (!pg_prop_sql_exec(conn, "VACUUM (FREEZE) integ_t;", &err))
		pg_prop_fail(tc, "vacuum failed: %s", err ? err : "");
	/* pg_check_visible / pg_check_frozen return the TIDs of wrongly-set bits */
	badvis = scalar_long(tc, conn,
						 "SELECT count(*) FROM pg_check_visible('integ_t'::regclass)");
	badfroz = scalar_long(tc, conn,
						  "SELECT count(*) FROM pg_check_frozen('integ_t'::regclass)");
	if (badvis != 0)
		pg_prop_fail(tc, "pg_check_visible found %ld wrong VM bits", badvis);
	if (badfroz != 0)
		pg_prop_fail(tc, "pg_check_frozen found %ld wrong frozen bits", badfroz);
	pg_prop_disconnect(conn);
}

/* pgstattuple's byte accounting is non-negative and bounded by table size. */
PG_PROP(stattuple_bounds)
{
	pg_prop_conn *conn = connect_integ(tc);
	long		bad;

	if (!pg_prop_require_extension(conn, "pgstattuple"))
		pg_prop_skip(tc, "pgstattuple not available");
	build_and_churn(tc, conn);
	/*
	 * table_len >= live+dead+free bytes, and no component is negative.  One
	 * query returns the count of violations (expected zero).
	 */
	bad = scalar_long(tc, conn,
					  "SELECT count(*) FROM pgstattuple('integ_t'::regclass) s "
					  "WHERE s.table_len < s.tuple_len + s.dead_tuple_len + s.free_space "
					  "OR s.tuple_len < 0 OR s.dead_tuple_len < 0 OR s.free_space < 0");
	if (bad != 0)
		pg_prop_fail(tc, "pgstattuple byte accounting inconsistent");
	pg_prop_disconnect(conn);
}
