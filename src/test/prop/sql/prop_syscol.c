/*-------------------------------------------------------------------------
 *
 * prop_syscol.c
 *	  System columns must identify the row they came from.
 *
 * Every other query oracle in the suite compares user columns.  None of them
 * project ctid, tableoid or xmin, so any bug that leaves a tuple's system
 * columns wrong while its data is right is invisible to them -- and such bugs
 * are real: ExecForceStoreHeapTuple() drops tts_tid on a buffer slot, so an
 * ordering-operator index scan whose distances are lossy (GiST circle_ops /
 * poly_ops, SP-GiST poly_ops) returns ctid = (4294967295,0) for every row it
 * routes through the reorder queue, and SELECT ... FOR UPDATE then hands that
 * TID to heap_lock_tuple() (an assertion failure, or P_NEW relation extension
 * without assertions).  See CF 7371.
 *
 * The invariant here, checked over a randomized trigger space:
 *
 *	 For every row R returned by any query over table T, the system columns R
 *	 reports must name a live row of T holding exactly R's data:
 *		 R.ctid  is a valid TID, and  T WHERE ctid = R.ctid  has R's id and data;
 *		 R.tableoid = T's oid (or the oid of the partition/child that holds it);
 *		 R.xmin  equals that row's xmin.
 *
 * Each case builds a table of geometric values with an ordering-capable index
 * (GiST or SP-GiST, with an opclass whose distances may be exact or lossy),
 * then draws a query shape that reads system columns through a path the
 * planner is pushed onto with plan advice-free GUCs (seqscan off):
 *
 *	 - KNN:  ORDER BY v <-> point LIMIT n   (n drawn; n = 1 never reorders)
 *	 - KNN with a qual on top, or under a subquery / CTE
 *	 - KNN ... FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE   (the lock path)
 *	 - UPDATE ... RETURNING ctid / DELETE ... RETURNING ctid, routed through a
 *	   KNN subquery
 *	 - INSERT ... RETURNING ctid into a table with a BEFORE ROW trigger that
 *	   modifies NEW (the trigger path goes through ExecForceStoreHeapTuple too)
 *
 * The result is captured into a temp table first and verified against the
 * base table afterwards, so no verification qual can be pushed into the scan
 * under test.  A lock-path case that raises an error is also a failure: a
 * correct server locks the rows the query returned.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "syscol"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* the index/opclass/type combinations that support ORDER BY <-> point */
typedef struct KnnAm
{
	const char *am;				/* gist | spgist */
	const char *opclass;		/* "" = default */
	const char *type;			/* column type */
	bool		lossy;			/* distance needs recheck -> reorder queue */
} KnnAm;

static const KnnAm knn_ams[] = {
	{"gist", "", "point", false},
	{"gist", "", "box", false},
	{"gist", "", "circle", true},
	{"gist", "", "polygon", true},
	{"spgist", "", "point", false},
	{"spgist", "kd_point_ops", "point", false},
	{"spgist", "", "box", false},
	{"spgist", "", "polygon", true},
};
#define N_KNN_AMS ((int) lengthof(knn_ams))

static void
sc_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "setup failed: %s\n  SQL: %s", err ? err : "", sql);
}

static long
sc_count(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	long		n;

	if (failed || v == NULL)
		pg_prop_fail(tc, "check failed: %s\n  SQL: %s", err ? err : "", sql);
	n = atol(v);
	pg_free(v);
	return n;
}

/* append a literal geometric value of the given type around (x, y) */
static void
sc_geom(StringInfo b, const char *type, int x, int y, int sz)
{
	if (strcmp(type, "point") == 0)
		appendStringInfo(b, "point(%d, %d)", x, y);
	else if (strcmp(type, "box") == 0)
		appendStringInfo(b, "box(point(%d, %d), point(%d, %d))", x, y, x + sz, y + sz);
	else if (strcmp(type, "circle") == 0)
		appendStringInfo(b, "circle(point(%d, %d), %d.5)", x, y, sz);
	else						/* polygon: a triangle, so bbox distance != true distance */
		appendStringInfo(b, "polygon(path '((%d,%d),(%d,%d),(%d,%d))')",
						 x, y, x + sz, y + sz, x + sz, y);
}

/*
 * Build sc_t(id int primary key, g <type>, v int) with n rows of drawn
 * geometry and an ordering-capable index; return the chosen AM.
 */
static const KnnAm *
sc_build(pg_prop_tc *tc, pg_prop_conn *conn)
{
	const KnnAm *k = &knn_ams[pg_prop_draw_int(tc, 0, N_KNN_AMS - 1)];
	int			n = (int) pg_prop_draw_int(tc, 2, 300);
	int			grid = (int) pg_prop_draw_int(tc, 1, 50);
	StringInfoData s;

	initStringInfo(&s);
	appendStringInfo(&s,
					 "DROP TABLE IF EXISTS sc_t CASCADE; "
					 "CREATE TABLE sc_t (id int PRIMARY KEY, g %s, v int); "
					 "INSERT INTO sc_t VALUES ", k->type);
	for (int i = 1; i <= n; i++)
	{
		/* clustered coordinates with ties, so distances collide */
		int			x = (int) pg_prop_draw_int(tc, 0, grid);
		int			y = (int) pg_prop_draw_int(tc, 0, grid);
		int			sz = (int) pg_prop_draw_int(tc, 0, 9);

		appendStringInfo(&s, "%s(%d, ", i > 1 ? ", " : "", i);
		sc_geom(&s, k->type, x, y, sz);
		appendStringInfo(&s, ", %d)", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfo(&s, "; CREATE INDEX sc_t_g ON sc_t USING %s (g %s); ",
					 k->am, k->opclass);
	/* sometimes dead tuples / HOT chains in the heap under the index */
	if (pg_prop_draw_bool(tc))
		appendStringInfoString(&s, "UPDATE sc_t SET v = v + 1 WHERE id % 3 = 0; ");
	if (pg_prop_draw_bool(tc))
		appendStringInfoString(&s, "DELETE FROM sc_t WHERE id % 7 = 0; ");
	sc_exec(tc, conn, s.data);
	/* VACUUM cannot share a multi-statement (implicit-transaction) string */
	sc_exec(tc, conn, "VACUUM ANALYZE sc_t");
	pfree(s.data);
	return k;
}

/*
 * Verify the captured result sc_res(c tid, toid oid, xm text, id int, v int)
 * against sc_t: every row's system columns must name the live row with the
 * same id and data.
 */
static void
sc_verify(pg_prop_tc *tc, pg_prop_conn *conn, const char *what)
{
	long		total = sc_count(tc, conn, "SELECT count(*) FROM sc_res");
	long		bad_tid = sc_count(tc, conn,
								   "SELECT count(*) FROM sc_res WHERE c IS NULL "
								   "OR c = '(4294967295,0)'::tid OR c::text LIKE '(4294967295,%'");
	long		not_found = sc_count(tc, conn,
									 "SELECT count(*) FROM sc_res r WHERE NOT EXISTS "
									 "(SELECT 1 FROM sc_t t WHERE t.ctid = r.c "
									 "AND t.id = r.id AND t.v IS NOT DISTINCT FROM r.v)");
	long		bad_oid = sc_count(tc, conn,
								   "SELECT count(*) FROM sc_res WHERE toid IS DISTINCT FROM "
								   "'sc_t'::regclass::oid");
	long		bad_xmin = sc_count(tc, conn,
									"SELECT count(*) FROM sc_res r WHERE NOT EXISTS "
									"(SELECT 1 FROM sc_t t WHERE t.id = r.id "
									"AND t.xmin::text = r.xm)");

	if (bad_tid || not_found || bad_oid || bad_xmin)
	{
		bool		failed = false;
		char	   *sample = pg_prop_sql_rows(conn,
											  "SELECT c, toid, xm, id, v FROM sc_res ORDER BY id LIMIT 8",
											  &failed, NULL);

		pg_prop_fail(tc,
					 "%s returned rows whose system columns do not identify them\n"
					 "  rows=%ld  invalid ctid=%ld  ctid->wrong/missing row=%ld  "
					 "wrong tableoid=%ld  wrong xmin=%ld\n  sample (ctid, tableoid, xmin, id, v):\n%s",
					 what, total, bad_tid, not_found, bad_oid, bad_xmin,
					 sample ? sample : "(unavailable)");
	}
}

/* draw a KNN ORDER BY clause: v <-> point [ASC], tie-broken by id */
static void
sc_orderby(pg_prop_tc *tc, StringInfo b)
{
	appendStringInfo(b, "g <-> point(%d, %d)",
					 (int) pg_prop_draw_int(tc, -5, 55),
					 (int) pg_prop_draw_int(tc, -5, 55));
}

/*
 * The read path: capture a KNN query's system columns, possibly wrapped in a
 * subquery or CTE and possibly with a qual, then verify them.
 */
PG_PROP(knn_scan_syscols_identify_row)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			lim;
	int			shape;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	sc_build(tc, conn);

	lim = (int) pg_prop_draw_int(tc, 1, 40);
	shape = (int) pg_prop_draw_int(tc, 0, 3);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SET enable_seqscan = off; SET enable_bitmapscan = off; "
						   "DROP TABLE IF EXISTS sc_res; "
						   "CREATE TEMP TABLE sc_res AS ");
	switch (shape)
	{
		case 0:				/* plain KNN */
			appendStringInfoString(&q,
								   "SELECT ctid AS c, tableoid AS toid, xmin::text AS xm, id, v "
								   "FROM sc_t ORDER BY ");
			sc_orderby(tc, &q);
			appendStringInfo(&q, " LIMIT %d", lim);
			break;
		case 1:				/* KNN with a qual the index does not evaluate */
			appendStringInfoString(&q,
								   "SELECT ctid AS c, tableoid AS toid, xmin::text AS xm, id, v "
								   "FROM sc_t WHERE v >= -3 ORDER BY ");
			sc_orderby(tc, &q);
			appendStringInfo(&q, " LIMIT %d", lim);
			break;
		case 2:				/* KNN under a subquery, system columns projected through */
			appendStringInfoString(&q,
								   "SELECT c, toid, xm, id, v FROM (SELECT ctid AS c, tableoid AS toid, "
								   "xmin::text AS xm, id, v FROM sc_t ORDER BY ");
			sc_orderby(tc, &q);
			appendStringInfo(&q, " LIMIT %d) s", lim);
			break;
		default:			/* KNN in a materialized CTE */
			appendStringInfoString(&q,
								   "WITH k AS MATERIALIZED (SELECT ctid AS c, tableoid AS toid, "
								   "xmin::text AS xm, id, v FROM sc_t ORDER BY ");
			sc_orderby(tc, &q);
			appendStringInfo(&q, " LIMIT %d) SELECT * FROM k", lim);
			break;
	}
	appendStringInfoString(&q, "; RESET enable_seqscan; RESET enable_bitmapscan");
	sc_exec(tc, conn, q.data);
	sc_verify(tc, conn, "a KNN index scan");

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The lock path: SELECT ... ORDER BY <-> LIMIT n FOR <strength>.  The rows
 * must be lockable (the query must succeed) and must be the rows they say.
 */
PG_PROP(knn_row_lock_locks_the_returned_rows)
{
	static const char *const strength[] = {
		"FOR UPDATE", "FOR NO KEY UPDATE", "FOR SHARE", "FOR KEY SHARE"
	};
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	const char *st;
	int			lim;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	sc_build(tc, conn);

	st = strength[pg_prop_draw_int(tc, 0, lengthof(strength) - 1)];
	lim = (int) pg_prop_draw_int(tc, 1, 40);

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "BEGIN; SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off; "
						   "DROP TABLE IF EXISTS sc_res; "
						   "CREATE TEMP TABLE sc_res AS "
						   "SELECT ctid AS c, tableoid AS toid, xmin::text AS xm, id, v "
						   "FROM sc_t ORDER BY ");
	sc_orderby(tc, &q);
	appendStringInfo(&q, " LIMIT %d %s", lim, st);

	if (!pg_prop_sql_exec(conn, q.data, &err))
	{
		pg_prop_sql_exec(conn, "ROLLBACK", NULL);
		pg_prop_fail(tc,
					 "row-locking KNN query failed: %s\n  SQL: %s",
					 err ? err : "", q.data);
	}
	sc_exec(tc, conn, "COMMIT");
	sc_verify(tc, conn, "a row-locking KNN index scan");

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The write path: UPDATE / DELETE whose target rows come from a KNN subquery,
 * RETURNING system columns, and INSERT ... RETURNING through a BEFORE ROW
 * trigger that rewrites NEW (the trigger code stores the modified tuple with
 * ExecForceStoreHeapTuple).  RETURNING ctid must name the new (or, for DELETE,
 * the just-deleted) version.
 */
PG_PROP(dml_returning_syscols_identify_row)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			kind;
	int			lim;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	sc_build(tc, conn);

	kind = (int) pg_prop_draw_int(tc, 0, 2);
	lim = (int) pg_prop_draw_int(tc, 1, 40);

	initStringInfo(&q);
	if (kind == 2)
	{
		/* trigger that modifies NEW: INSERT ... RETURNING */
		sc_exec(tc, conn,
				"CREATE OR REPLACE FUNCTION sc_bump() RETURNS trigger LANGUAGE plpgsql AS "
				"$$ BEGIN NEW.v := coalesce(NEW.v, 0) + 100; RETURN NEW; END $$; "
				"CREATE TRIGGER sc_bump BEFORE INSERT OR UPDATE ON sc_t "
				"FOR EACH ROW EXECUTE FUNCTION sc_bump()");
		appendStringInfo(&q,
						 "DROP TABLE IF EXISTS sc_res; CREATE TEMP TABLE sc_res "
						 "(c tid, toid oid, xm text, id int, v int); "
						 "WITH ins AS (INSERT INTO sc_t SELECT id + 100000, g, v FROM sc_t "
						 "ORDER BY id LIMIT %d "
						 "RETURNING ctid, tableoid, xmin::text, id, v) "
						 "INSERT INTO sc_res SELECT * FROM ins", lim);
	}
	else
	{
		appendStringInfoString(&q,
							   "SET enable_seqscan = off; SET enable_bitmapscan = off; "
							   "DROP TABLE IF EXISTS sc_res; CREATE TEMP TABLE sc_res "
							   "(c tid, toid oid, xm text, id int, v int); ");
		appendStringInfo(&q,
						 "WITH m AS (%s sc_t %s WHERE id IN (SELECT id FROM sc_t ORDER BY ",
						 kind == 0 ? "UPDATE" : "DELETE FROM",
						 kind == 0 ? "SET v = v + 1" : "");
		sc_orderby(tc, &q);
		appendStringInfo(&q,
						 " LIMIT %d) RETURNING ctid, tableoid, xmin::text, id, v) "
						 "INSERT INTO sc_res SELECT * FROM m; "
						 "RESET enable_seqscan; RESET enable_bitmapscan", lim);
	}
	sc_exec(tc, conn, q.data);

	if (kind == 1)
	{
		/*
		 * DELETE: the returned ctids named rows that are now gone, so check
		 * only that they were valid and distinct (each deleted version once).
		 */
		long		bad = sc_count(tc, conn,
								   "SELECT count(*) FROM sc_res WHERE c IS NULL "
								   "OR c::text LIKE '(4294967295,%'");
		long		dup = sc_count(tc, conn,
								   "SELECT count(*) - count(DISTINCT c) FROM sc_res");

		if (bad || dup)
			pg_prop_fail(tc, "DELETE ... RETURNING ctid: %ld invalid, %ld duplicate", bad, dup);
	}
	else
		sc_verify(tc, conn, kind == 0 ? "UPDATE ... RETURNING" : "INSERT ... RETURNING via a modifying trigger");

	sc_exec(tc, conn, "DROP TRIGGER IF EXISTS sc_bump ON sc_t");
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Tuple routing into a postgres_fdw foreign partition, RETURNING ctid.  A
 * foreign-table partition whose columns match the parent gets a virtual
 * result slot, and the remote row comes back through ExecForceStoreHeapTuple()
 * -- whose virtual-slot branch also never set tts_tid (the second path in CF
 * 7371).  The returned ctid must name the row on the remote side.
 *
 * The "remote" is a loopback server in the same cluster.  Partitions are drawn:
 * some foreign (matching or permuted column order, which forces a conversion
 * map and a heap slot instead), some local; rows are routed by a drawn key.
 */
PG_PROP(fdw_routed_insert_returning_ctid)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s;
	int			nparts;
	int			nrows;
	long		bad;
	int			kinds[4];

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	if (!pg_prop_require_extension(conn, "postgres_fdw"))
		pg_prop_skip(tc, "postgres_fdw not installed");

	nparts = (int) pg_prop_draw_int(tc, 1, 4);
	nrows = (int) pg_prop_draw_int(tc, 1, 40);

	initStringInfo(&s);
	appendStringInfoString(&s,
						   "DROP TABLE IF EXISTS sc_par CASCADE; "
						   "DROP SERVER IF EXISTS sc_loop CASCADE; "
						   "DO $$ BEGIN EXECUTE format('CREATE SERVER sc_loop FOREIGN DATA WRAPPER postgres_fdw "
						   "OPTIONS (dbname %L, port %L)', current_database(), current_setting('port')); END $$; "
						   "DO $$ BEGIN EXECUTE format('CREATE USER MAPPING FOR CURRENT_USER SERVER sc_loop OPTIONS (user %L)', current_user); END $$; "
						   "CREATE TABLE sc_par (a int, b text) PARTITION BY LIST (a); ");
	for (int i = 0; i < nparts; i++)
	{
		int			kind = (int) pg_prop_draw_int(tc, 0, 2);

		kinds[i] = kind;
		appendStringInfo(&s, "DROP TABLE IF EXISTS sc_rem%d; ", i);
		if (kind == 2)
			appendStringInfo(&s, "CREATE TABLE sc_par%d PARTITION OF sc_par FOR VALUES IN (%d); ", i, i);
		else
		{
			/* kind 0: same column order (virtual slot); 1: permuted (conversion map) */
			appendStringInfo(&s, "CREATE TABLE sc_rem%d (%s); ", i,
							 kind == 0 ? "a int, b text" : "b text, a int");
			appendStringInfo(&s,
							 "CREATE FOREIGN TABLE sc_par%d PARTITION OF sc_par FOR VALUES IN (%d) "
							 "SERVER sc_loop OPTIONS (table_name 'sc_rem%d'); ", i, i, i);
		}
	}
	/*
	 * Commit the DDL before the insert: the loopback connection is a separate
	 * session and cannot see uncommitted remote tables.  (A multi-statement
	 * string is one implicit transaction.)
	 */
	sc_exec(tc, conn, s.data);
	resetStringInfo(&s);
	appendStringInfoString(&s,
						   "DROP TABLE IF EXISTS sc_res; CREATE TEMP TABLE sc_res (a int, b text, c tid, foreign_part bool); "
						   "WITH ins AS (INSERT INTO sc_par VALUES ");
	for (int r = 0; r < nrows; r++)
		appendStringInfo(&s, "%s(%d, 'r%d')", r ? ", " : "",
						 (int) pg_prop_draw_int(tc, 0, nparts - 1), r);
	appendStringInfoString(&s,
						   " RETURNING a, b, ctid, tableoid) "
						   "INSERT INTO sc_res SELECT a, b, ctid, "
						   "(SELECT relkind = 'f' FROM pg_class WHERE oid = ins.tableoid) FROM ins");
	if (!pg_prop_sql_exec(conn, s.data, &err))
		pg_prop_fail(tc, "routed insert failed: %s", err ? err : "");

	/*
	 * Every returned ctid must name the row: in the remote table for a foreign
	 * partition, in the partition itself for a local one.
	 */
	resetStringInfo(&s);
	appendStringInfoString(&s, "SELECT count(*) FROM sc_res r WHERE NOT (");
	for (int i = 0; i < nparts; i++)
	{
		if (i > 0)
			appendStringInfoString(&s, " OR ");
		if (kinds[i] == 2)
			appendStringInfo(&s,
							 "(r.a = %d AND EXISTS (SELECT 1 FROM sc_par%d x "
							 "WHERE x.ctid = r.c AND x.b = r.b))", i, i);
		else
			appendStringInfo(&s,
							 "(r.a = %d AND EXISTS (SELECT 1 FROM sc_rem%d x "
							 "WHERE x.ctid = r.c AND x.b = r.b))", i, i);
	}
	appendStringInfoChar(&s, ')');
	bad = sc_count(tc, conn, s.data);
	if (bad)
	{
		bool		failed = false;
		char	   *sample = pg_prop_sql_rows(conn,
											  "SELECT a, b, c, foreign_part FROM sc_res ORDER BY b LIMIT 8",
											  &failed, NULL);

		pg_prop_fail(tc, "routed INSERT ... RETURNING ctid: %ld of %d rows report a ctid "
					 "that does not name them\n  sample (a, b, ctid, foreign):\n%s",
					 bad, nrows, sample ? sample : "");
	}

	sc_exec(tc, conn, "DROP TABLE sc_par CASCADE; DROP SERVER sc_loop CASCADE");
	for (int i = 0; i < nparts; i++)
	{
		resetStringInfo(&s);
		appendStringInfo(&s, "DROP TABLE IF EXISTS sc_rem%d", i);
		sc_exec(tc, conn, s.data);
	}
	pfree(s.data);
	pg_prop_disconnect(conn);
}
