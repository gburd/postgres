/*-------------------------------------------------------------------------
 *
 * prop_query_multi.c
 *	  Query-level property tests over a multi-table generated database.
 *
 * The other query areas generate one table (qg_t) and self-join it.  This area
 * uses the multi-table model (qg_build_db): a small random database of 2..N
 * related tables with declared foreign keys and correlated data, so the
 * oracles run over real joins across distinct relations with a real referential
 * structure for the planner to exploit.
 *
 * The oracles are the same tautologies as elsewhere, lifted to the join:
 *
 *	 1. TLP over a join predicate: partitioning the WHERE of a join three ways
 *		on a generated predicate (p / NOT p / p IS NULL, applied to one side)
 *		must reconstruct the unpartitioned join's row multiset.
 *
 *	 2. Inner-join cardinality is bounded by the cross product and is symmetric:
 *		|A JOIN B ON k| == |B JOIN A ON k| and <= |A| * |B|.
 *
 *	 3. A foreign-key inner join never exceeds the child's row count on the
 *		many-to-one side: every child row matches at most one parent (the FK
 *		references the parent's primary key), so the equi-join on fk = id
 *		returns exactly the child rows whose fk is non-NULL.
 *
 * All are theorems, so any disagreement is a planner/executor wrong-results
 * bug, not an oracle miscalibration.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_multi.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_multi"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/* Two row-returning queries must render identically (ordered multiset). */
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

	r1 = pg_prop_sql_rowhash(conn, lhs, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "lhs query failed: %s\n  SQL: %s",
					 e1 ? e1 : "(unknown)", lhs);
	r2 = pg_prop_sql_rowhash(conn, rhs, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "rhs query failed: %s\n  SQL: %s",
					 e2 ? e2 : "(unknown)", rhs);

	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc,
					 "multi-table digest mismatch:\n  lhs: %s\n  rhs: %s\n"
					 "  lhs md5: %s\n  rhs md5: %s",
					 lhs, rhs, r1, r2);
	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

static long
count_of(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	long		n;

	v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "count query failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	n = v ? atol(v) : 0;
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
	return n;
}

/*
 * Find a child table (one with a FK) in the generated database, or return -1.
 * Many properties need an actual join edge; if the random database produced
 * none this case is vacuously satisfied.
 */
static int
find_child(const qg_db *db)
{
	for (int i = 0; i < db->ntables; i++)
		if (db->tables[i].fkref >= 0)
			return i;
	return -1;
}

/*
 * TLP over a join: the join of child to parent, partitioned three ways on a
 * predicate over the child, reconstructs the unpartitioned join.
 */
PG_PROP(join_tlp)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	qg_db	   *db;
	qg_db_params p;
	int			ci;
	const qg_table *child;
	const qg_table *parent;
	StringInfoData pred;
	StringInfoData whole;
	StringInfoData part;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_db_params_for_env(&p);
	db = qg_build_db(tc, conn, &p);

	ci = find_child(db);
	if (ci < 0)
	{
		qg_drop_db(tc, conn, db);
		pg_prop_disconnect(conn);
		return;
	}
	child = &db->tables[ci];
	parent = &db->tables[child->fkref];

	initStringInfo(&pred);
	qg_draw_predicate_on(tc, &pred, child, "ch");

	/* whole: child JOIN parent ON ch.fk = pa.id, return ch.id ordered */
	initStringInfo(&whole);
	appendStringInfo(&whole,
					 "SELECT ch.id FROM %s ch JOIN %s pa ON ch.fk = pa.id "
					 "ORDER BY ch.id",
					 child->name, parent->name);

	/* partitioned: the predicate over ch split three ways, UNION ALL'd */
	initStringInfo(&part);
	appendStringInfo(&part,
					 "SELECT id FROM ("
					 "SELECT ch.id FROM %s ch JOIN %s pa ON ch.fk = pa.id "
					 "WHERE (%s) "
					 "UNION ALL "
					 "SELECT ch.id FROM %s ch JOIN %s pa ON ch.fk = pa.id "
					 "WHERE NOT (%s) "
					 "UNION ALL "
					 "SELECT ch.id FROM %s ch JOIN %s pa ON ch.fk = pa.id "
					 "WHERE (%s) IS NULL"
					 ") s ORDER BY id",
					 child->name, parent->name, pred.data,
					 child->name, parent->name, pred.data,
					 child->name, parent->name, pred.data);

	require_same_rows(tc, conn, whole.data, part.data);

	pfree(pred.data);
	pfree(whole.data);
	pfree(part.data);
	qg_drop_db(tc, conn, db);
	pg_prop_disconnect(conn);
}

/*
 * Inner-join cardinality is commutative and bounded by the cross product:
 *	 |A JOIN B ON a.fk = b.id| == |B JOIN A ON a.fk = b.id|  and  <= |A|*|B|.
 */
PG_PROP(join_card_commutes_and_bounded)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	qg_db	   *db;
	qg_db_params p;
	int			ci;
	const qg_table *child;
	const qg_table *parent;
	StringInfoData sql;
	long		ab,
				ba,
				na,
				nb;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_db_params_for_env(&p);
	db = qg_build_db(tc, conn, &p);

	ci = find_child(db);
	if (ci < 0)
	{
		qg_drop_db(tc, conn, db);
		pg_prop_disconnect(conn);
		return;
	}
	child = &db->tables[ci];
	parent = &db->tables[child->fkref];

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT count(*) FROM %s a JOIN %s b ON a.fk = b.id",
					 child->name, parent->name);
	ab = count_of(tc, conn, sql.data);
	resetStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT count(*) FROM %s b JOIN %s a ON a.fk = b.id",
					 parent->name, child->name);
	ba = count_of(tc, conn, sql.data);

	/*
	 * The cross-product bound is over what the query actually scans, which
	 * includes any inheritance child of either table -- so read the sizes from
	 * the server, not from the generator's per-table row counts.
	 */
	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(*) FROM %s", child->name);
	na = count_of(tc, conn, sql.data);
	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(*) FROM %s", parent->name);
	nb = count_of(tc, conn, sql.data);

	if (ab != ba)
		pg_prop_fail(tc,
					 "join cardinality not commutative: %ld vs %ld\n"
					 "  child=%s parent=%s", ab, ba, child->name, parent->name);
	if (ab > na * nb)
		pg_prop_fail(tc,
					 "join exceeds cross product: %ld > %ld*%ld\n"
					 "  child=%s parent=%s", ab, na, nb, child->name, parent->name);

	pfree(sql.data);
	qg_drop_db(tc, conn, db);
	pg_prop_disconnect(conn);
}

/*
 * A FK equi-join to the parent's primary key returns exactly the child rows
 * whose fk is non-NULL (each matches exactly one parent, since fk references a
 * unique id and the FK is enforced so no orphans exist).  ONLY on both sides:
 * the uniqueness and the FK are properties of the declared tables, not of any
 * inheritance child the generator may have attached, whose rows a plain scan
 * would also return.
 */
PG_PROP(fk_join_eq_nonnull_children)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	qg_db	   *db;
	qg_db_params p;
	int			ci;
	const qg_table *child;
	const qg_table *parent;
	StringInfoData sql;
	long		joined,
				nonnull;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_db_params_for_env(&p);
	db = qg_build_db(tc, conn, &p);

	ci = find_child(db);
	if (ci < 0)
	{
		qg_drop_db(tc, conn, db);
		pg_prop_disconnect(conn);
		return;
	}
	child = &db->tables[ci];
	parent = &db->tables[child->fkref];

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT count(*) FROM ONLY %s ch JOIN ONLY %s pa ON ch.fk = pa.id",
					 child->name, parent->name);
	joined = count_of(tc, conn, sql.data);
	resetStringInfo(&sql);
	appendStringInfo(&sql, "SELECT count(*) FROM ONLY %s WHERE fk IS NOT NULL",
					 child->name);
	nonnull = count_of(tc, conn, sql.data);

	if (joined != nonnull)
		pg_prop_fail(tc,
					 "FK join count %ld != non-NULL child fk count %ld\n"
					 "  child=%s parent=%s",
					 joined, nonnull, child->name, parent->name);

	pfree(sql.data);
	qg_drop_db(tc, conn, db);
	pg_prop_disconnect(conn);
}
