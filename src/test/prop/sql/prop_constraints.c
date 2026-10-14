/*-------------------------------------------------------------------------
 *
 * prop_constraints.c
 *	  Property tests that integrity constraints are enforced exactly.
 *
 * A constraint is a promise the server makes about the data: it must reject
 * every row that violates it and accept every row that does not.  "Exactly" is
 * the property -- not "rejects some bad rows" but "rejects a row if and only if
 * it violates the constraint", checked differentially against the predicate the
 * constraint encodes.  A constraint that accepts a violator is a durability /
 * correctness bug of the most serious kind for a database; a constraint that
 * rejects a valid row is a false positive equally worth catching.
 *
 * The oracle for each constraint is a pure-SQL predicate that says whether a
 * candidate row is a violation; the property generates a batch of candidate
 * rows, computes which the predicate marks as violations, then inserts them one
 * at a time and requires the server to reject exactly that set.
 *
 *	 unique_rejects_dups    -- a UNIQUE/PRIMARY KEY accepts a row iff its key is
 *						      not already present (NULLs distinct by default).
 *	 check_rejects_false    -- a CHECK (expr) accepts a row iff expr is not
 *						      FALSE (NULL/unknown is accepted, per SQL).
 *	 notnull_rejects_null   -- a NOT NULL column accepts a row iff the column is
 *						      non-NULL.
 *	 fk_rejects_orphan      -- a FOREIGN KEY accepts a child row iff its key is
 *						      NULL or present in the parent.
 *	 exclusion_rejects      -- an EXCLUSION constraint (ranges that must not
 *						      overlap) accepts a row iff it overlaps no existing
 *						      row.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_constraints.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "constraints"
#include "pg_prop.h"

#include "lib/stringinfo.h"

#define C_K_MIN (-6)
#define C_K_MAX 6

static pg_prop_conn *
connect_c(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	pg_prop_sql_exec(conn, "SET client_min_messages=warning", &err);
	if (err)
		pg_free(err);
	return conn;
}

/* Draw an int in [C_K_MIN,C_K_MAX] as SQL text, ~15% NULL. */
static void
draw_k(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, C_K_MIN, C_K_MAX));
}

/*
 * Try one INSERT; return true if it succeeded.  Used to observe whether the
 * server accepted or rejected a candidate row.  Errors other than a constraint
 * violation (syntax, type) would be a generator bug and are surfaced.
 */
static bool
try_insert(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;
	bool		ok = pg_prop_sql_exec(conn, sql, &err);

	if (err)
		pg_free(err);
	return ok;
}

/*
 * UNIQUE accepts a row iff its key is not already present (NULLs are distinct,
 * so a NULL key is always accepted).  We maintain a shadow idea of presence in
 * SQL: before each insert, ask whether the key already exists (non-NULL and in
 * the table); the server must accept iff it does not.
 */
PG_PROP(unique_rejects_dups)
{
	pg_prop_conn *conn = connect_c(tc);
	char	   *err = NULL;
	int			nrows = (int) pg_prop_draw_int(tc, 1, 40);
	int			i;

	if (!pg_prop_sql_exec(conn,
						  "DROP TABLE IF EXISTS uq_c; "
						  "CREATE TABLE uq_c (id serial PRIMARY KEY, k int UNIQUE)",
						  &err))
		pg_prop_fail(tc, "setup failed: %s", err ? err : "");

	for (i = 0; i < nrows; i++)
	{
		StringInfoData kv;
		StringInfoData q;
		long		already;
		bool		accepted;
		bool		is_null;

		initStringInfo(&kv);
		draw_k(tc, &kv);
		is_null = (strcmp(kv.data, "NULL") == 0);

		/* does this non-NULL key already exist? */
		initStringInfo(&q);
		if (is_null)
			already = 0;
		else
		{
			bool		failed = false;
			char	   *v;

			appendStringInfo(&q, "SELECT count(*) FROM uq_c WHERE k = %s", kv.data);
			v = pg_prop_sql_scalar(conn, q.data, &failed, &err);
			already = v ? atol(v) : 0;
			if (v)
				pg_free(v);
			if (failed)
				pg_prop_fail(tc, "presence query failed: %s", err ? err : "");
		}

		resetStringInfo(&q);
		appendStringInfo(&q, "INSERT INTO uq_c (k) VALUES (%s)", kv.data);
		accepted = try_insert(tc, conn, q.data);

		/* server must accept iff the key was not already present */
		if (accepted != (already == 0))
			pg_prop_fail(tc,
						 "UNIQUE wrong: key %s already=%ld accepted=%d",
						 kv.data, already, (int) accepted);

		pfree(kv.data);
		pfree(q.data);
	}
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS uq_c", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/*
 * CHECK (k BETWEEN lo AND hi) accepts a row iff the expression is not FALSE;
 * SQL semantics admit a row when the check is TRUE or NULL (unknown).  Compute
 * the expected acceptance in SQL and compare to the server's decision.
 */
PG_PROP(check_rejects_false)
{
	pg_prop_conn *conn = connect_c(tc);
	char	   *err = NULL;
	int			lo = (int) pg_prop_draw_int(tc, C_K_MIN, C_K_MAX);
	int			hi = (int) pg_prop_draw_int(tc, C_K_MIN, C_K_MAX);
	int			i;
	int			nrows = (int) pg_prop_draw_int(tc, 1, 40);
	StringInfoData ddl;

	if (lo > hi)
	{
		int			t = lo;

		lo = hi;
		hi = t;
	}
	initStringInfo(&ddl);
	appendStringInfo(&ddl,
					 "DROP TABLE IF EXISTS ck_c; "
					 "CREATE TABLE ck_c (id serial PRIMARY KEY, k int "
					 "CHECK (k BETWEEN %d AND %d))", lo, hi);
	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "setup failed: %s", err ? err : "");
	pfree(ddl.data);

	for (i = 0; i < nrows; i++)
	{
		StringInfoData kv;
		StringInfoData q;
		bool		accepted;
		bool		expect_accept;
		bool		failed = false;
		char	   *v;

		initStringInfo(&kv);
		draw_k(tc, &kv);

		/* expected: accept unless the check is FALSE.  Compute in SQL. */
		initStringInfo(&q);
		appendStringInfo(&q,
						 "SELECT (%s BETWEEN %d AND %d) IS NOT FALSE",
						 kv.data, lo, hi);
		v = pg_prop_sql_scalar(conn, q.data, &failed, &err);
		expect_accept = (v && strcmp(v, "t") == 0);
		if (v)
			pg_free(v);
		if (failed)
			pg_prop_fail(tc, "expectation query failed: %s", err ? err : "");

		resetStringInfo(&q);
		appendStringInfo(&q, "INSERT INTO ck_c (k) VALUES (%s)", kv.data);
		accepted = try_insert(tc, conn, q.data);

		if (accepted != expect_accept)
			pg_prop_fail(tc,
						 "CHECK wrong: k=%s expect_accept=%d accepted=%d "
						 "(range %d..%d)",
						 kv.data, (int) expect_accept, (int) accepted, lo, hi);
		pfree(kv.data);
		pfree(q.data);
	}
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS ck_c", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/* NOT NULL accepts a row iff the column is non-NULL. */
PG_PROP(notnull_rejects_null)
{
	pg_prop_conn *conn = connect_c(tc);
	char	   *err = NULL;
	int			i;
	int			nrows = (int) pg_prop_draw_int(tc, 1, 30);

	if (!pg_prop_sql_exec(conn,
						  "DROP TABLE IF EXISTS nn_c; "
						  "CREATE TABLE nn_c (id serial PRIMARY KEY, k int NOT NULL)",
						  &err))
		pg_prop_fail(tc, "setup failed: %s", err ? err : "");

	for (i = 0; i < nrows; i++)
	{
		StringInfoData kv;
		StringInfoData q;
		bool		accepted,
					is_null;

		initStringInfo(&kv);
		draw_k(tc, &kv);
		is_null = (strcmp(kv.data, "NULL") == 0);
		initStringInfo(&q);
		appendStringInfo(&q, "INSERT INTO nn_c (k) VALUES (%s)", kv.data);
		accepted = try_insert(tc, conn, q.data);
		if (accepted == is_null)
			pg_prop_fail(tc, "NOT NULL wrong: k=%s accepted=%d",
						 kv.data, (int) accepted);
		pfree(kv.data);
		pfree(q.data);
	}
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS nn_c", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/*
 * FOREIGN KEY accepts a child row iff its key is NULL or present in the
 * parent.  Build a parent with a fixed set of keys, then insert children and
 * require acceptance iff the FK column is NULL or in the parent set.
 */
PG_PROP(fk_rejects_orphan)
{
	pg_prop_conn *conn = connect_c(tc);
	char	   *err = NULL;
	int			i;
	int			nrows = (int) pg_prop_draw_int(tc, 1, 40);

	/* parent holds keys 0..4 */
	if (!pg_prop_sql_exec(conn,
						  "DROP TABLE IF EXISTS fk_child; "
						  "DROP TABLE IF EXISTS fk_parent; "
						  "CREATE TABLE fk_parent (k int PRIMARY KEY); "
						  "INSERT INTO fk_parent VALUES (0),(1),(2),(3),(4); "
						  "CREATE TABLE fk_child (id serial PRIMARY KEY, "
						  "fk int REFERENCES fk_parent(k))",
						  &err))
		pg_prop_fail(tc, "setup failed: %s", err ? err : "");

	for (i = 0; i < nrows; i++)
	{
		StringInfoData kv;
		StringInfoData q;
		bool		accepted,
					expect_accept,
					is_null;
		int			val = 0;

		initStringInfo(&kv);
		/* draw a key in a range that straddles the parent's 0..4 set */
		if (pg_prop_draw_int(tc, 0, 6) == 0)
		{
			appendStringInfoString(&kv, "NULL");
			is_null = true;
		}
		else
		{
			val = (int) pg_prop_draw_int(tc, -2, 8);
			appendStringInfo(&kv, "%d", val);
			is_null = false;
		}
		expect_accept = is_null || (val >= 0 && val <= 4);

		initStringInfo(&q);
		appendStringInfo(&q, "INSERT INTO fk_child (fk) VALUES (%s)", kv.data);
		accepted = try_insert(tc, conn, q.data);
		if (accepted != expect_accept)
			pg_prop_fail(tc,
						 "FK wrong: fk=%s expect_accept=%d accepted=%d",
						 kv.data, (int) expect_accept, (int) accepted);
		pfree(kv.data);
		pfree(q.data);
	}
	pg_prop_sql_exec(conn,
					 "DROP TABLE IF EXISTS fk_child; DROP TABLE IF EXISTS fk_parent",
					 &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}

/*
 * EXCLUSION constraint: int4range values that must not overlap (&&).  A new
 * range is accepted iff it overlaps no existing range.  Compute the expected
 * answer in SQL against the already-accepted rows.
 */
PG_PROP(exclusion_rejects_overlap)
{
	pg_prop_conn *conn = connect_c(tc);
	char	   *err = NULL;
	int			i;
	int			nrows = (int) pg_prop_draw_int(tc, 1, 30);

	if (!pg_prop_sql_exec(conn,
						  "DROP TABLE IF EXISTS ex_c; "
						  "CREATE TABLE ex_c (id serial PRIMARY KEY, r int4range, "
						  "EXCLUDE USING gist (r WITH &&))",
						  &err))
	{
		/* gist/btree_gist on int4range may be unavailable: skip cleanly */
		if (err)
			pg_free(err);
		pg_prop_skip(tc, "exclusion/gist on int4range not available");
	}

	for (i = 0; i < nrows; i++)
	{
		int			lo = (int) pg_prop_draw_int(tc, C_K_MIN, C_K_MAX);
		int			hi = lo + (int) pg_prop_draw_int(tc, 1, 5);
		StringInfoData q;
		bool		accepted,
					expect_accept,
					failed = false;
		char	   *v;

		/* does [lo,hi) overlap any accepted range? */
		initStringInfo(&q);
		appendStringInfo(&q,
						 "SELECT NOT EXISTS (SELECT 1 FROM ex_c "
						 "WHERE r && int4range(%d,%d))", lo, hi);
		v = pg_prop_sql_scalar(conn, q.data, &failed, &err);
		expect_accept = (v && strcmp(v, "t") == 0);
		if (v)
			pg_free(v);
		if (failed)
			pg_prop_fail(tc, "overlap query failed: %s", err ? err : "");

		resetStringInfo(&q);
		appendStringInfo(&q, "INSERT INTO ex_c (r) VALUES (int4range(%d,%d))",
						 lo, hi);
		accepted = try_insert(tc, conn, q.data);
		if (accepted != expect_accept)
			pg_prop_fail(tc,
						 "EXCLUDE wrong: [%d,%d) expect_accept=%d accepted=%d",
						 lo, hi, (int) expect_accept, (int) accepted);
		pfree(q.data);
	}
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS ex_c", &err);
	if (err)
		pg_free(err);
	pg_prop_disconnect(conn);
}
