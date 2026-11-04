/*-------------------------------------------------------------------------
 *
 * prop_cf_ddl2.c
 *	  Catalog and data invariants after DDL: partitioning, constraints,
 *	  RLS, matviews, publications, tablespaces, temp schemas, ACLs.
 *
 * Each property drives a randomized DDL sequence around the trigger of an
 * open commitfest bug fix and checks an invariant that holds on a correct
 * server:
 *
 *	 partindex_rebuild_preserves_props (CF 6440)
 *		ALTER COLUMN TYPE / SET EXPRESSION on a partitioned table rebuilds
 *		every index of the partition tree that uses the column; a rebuild is
 *		an implementation detail, so every user-visible property of every
 *		index (name, comment, reloptions, tablespace, per-column statistics
 *		target, replica identity, cluster mark, extension dependency,
 *		constraint comment) must be the same afterwards.
 *
 *	 refresh_concurrently_matches_query (CF 6579)
 *		A REFRESH MATERIALIZED VIEW CONCURRENTLY that succeeds leaves exactly
 *		the rows of the defining query; one that fails changes nothing.
 *
 *	 fdw_partitioned_remote_dml_matches_local (CF 6770)
 *		UPDATE/DELETE through postgres_fdw on a remote partitioned or
 *		inheritance table changes the same rows as on a local copy.
 *
 *	 partition_autoname_constraint_never_collides (CF 6842)
 *		An auto-named ADD CHECK / SET NOT NULL on a partition root never
 *		fails with a duplicate-name error, whatever schemas and constraint
 *		names the partitions have.
 *
 *	 revoke_keeps_grant_chains_rooted (CF 6961)
 *		After any GRANT/REVOKE/membership sequence, every non-owner grantor
 *		in an ACL directly holds the grant option it granted from.
 *
 *	 alter_publication_reported_to_event_triggers (CF 7055)
 *		An ALTER PUBLICATION that changes the publication is listed by
 *		pg_event_trigger_ddl_commands() at ddl_command_end.
 *
 *	 publication_table_list_order_independent (CF 7210)
 *		The same TABLE / TABLE ONLY mention list in two orders, when both are
 *		accepted, publishes the same tables.
 *
 *	 temporal_fk_validation_matches_model (CF 7241)
 *		Every way of validating a PERIOD foreign key against existing rows
 *		accepts iff the rows are covered (range_agg model).
 *
 *	 rollback_set_tablespace_keeps_indexes_consistent (CF 7312)
 *		After a (sub)transaction with SET TABLESPACE rolls back, index scans
 *		and the seqscan agree and nothing errors.
 *
 *	 drop_temp_schema_keeps_catalog_consistent (CF 7321)
 *		After DROP SCHEMA pg_temp_N, new temp objects never reference a
 *		missing namespace.
 *
 *	 pending_detach_partition_consistent (CF 7351)
 *		A partition left pending detach is still covered by FOR ALL TABLES,
 *		and all sessions agree on its publication membership.
 *
 *	 create_like_only_touches_new_table (CF 7379)
 *		CREATE [TEMP] TABLE ... (LIKE ... INCLUDING COMMENTS) never changes
 *		another table, whatever search_path says.
 *
 *	 on_conflict_where_respects_rls (CF 7403)
 *		A function in ON CONFLICT DO UPDATE/SELECT WHERE never receives a
 *		value from a row hidden by RLS.
 *
 *	 fk_references_only_unique_constraints (CF 7464)
 *		An accepted (temporal) FK is backed by a PK / UNIQUE constraint of
 *		the referenced table, never by an EXCLUDE constraint.
 *
 *	 without_overlaps_unique_range_eq_multirange (CF 7465)
 *		UNIQUE [NULLS NOT DISTINCT] (.., x WITHOUT OVERLAPS) accepts the same
 *		rows for range and single-range multirange columns.
 *
 * All objects use the prefix "cfc2_"; each case drops what it created, and
 * starts by dropping anything a failed earlier case left behind.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_cf_ddl2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cf_ddl2"
#include "pg_prop.h"

#include "lib/stringinfo.h"

static pg_prop_conn *
c2_connect(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	pg_prop_sql_exec(conn, "SET client_min_messages = warning", NULL);
	return conn;
}

/* run DDL/DML that must succeed on a correct server */
static void
c2_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "statement failed: %s\n  SQL: %s", err ? err : "", sql);
}

/* run a statement whose failure is acceptable; returns success */
static bool
c2_try(pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;
	bool		ok = pg_prop_sql_exec(conn, sql, &err);

	if (err)
		pg_free(err);
	return ok;
}

static char *
c2_rows(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *r = pg_prop_sql_rows(conn, sql, &failed, &err);

	if (failed)
		pg_prop_fail(tc, "query failed: %s\n  SQL: %s", err ? err : "", sql);
	return r;
}

static long
c2_count(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	long		n;

	if (failed)
		pg_prop_fail(tc, "query failed: %s\n  SQL: %s", err ? err : "", sql);
	n = v ? atol(v) : 0;
	if (v)
		pg_free(v);
	return n;
}

/* ----------------------------------------------------------------------
 * CF 6440: partitioned index rebuild preserves descendant properties
 * ----------------------------------------------------------------------
 */

typedef struct PiKind
{
	const char *def;			/* column list etc. after ON cfc2_r */
	bool		constraint;		/* ADD CONSTRAINT ... UNIQUE */
	bool		uses_g;			/* references the generated column */
	bool		replident;		/* usable as REPLICA IDENTITY */
	bool		clusterable;	/* CLUSTER ON allowed (btree, not partial) */
	int			exprcol;		/* 1-based expression column, 0 = none */
	int			ncols;
} PiKind;

static const PiKind pi_kinds[] = {
	{"UNIQUE (id, a)", true, false, true, true, 0, 2},
	{"(id, a)", false, false, true, true, 0, 2},	/* UNIQUE INDEX */
	{"(b, (a * 3))", false, false, false, true, 2, 2},
	{"((a + 1)) WHERE b > 0", false, false, false, false, 1, 1},
	{"USING hash (a)", false, false, false, false, 0, 1},
	{"(id, g)", false, true, true, true, 0, 2},		/* UNIQUE INDEX */
	{"((g + a), id)", false, true, false, true, 1, 2},
};

/* the catalog facts about every index in cfc2_r's tree that must survive */
#define PI_SNAPSHOT \
	"SELECT pg_partition_root(i.indexrelid)::regclass::text, t.relname, c.relname, " \
	" coalesce(obj_description(c.oid, 'pg_class'), '-'), " \
	" coalesce(c.reloptions::text, '-'), coalesce(ts.spcname, 'pg_default'), " \
	" (SELECT string_agg(coalesce(a.attstattarget::text, '-'), ',' ORDER BY a.attnum) " \
	"    FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0), " \
	" i.indisreplident, i.indisclustered, t.relreplident, " \
	" coalesce((SELECT string_agg(e.extname, ',' ORDER BY e.extname) FROM pg_depend d " \
	"    JOIN pg_extension e ON e.oid = d.refobjid WHERE d.classid = 'pg_class'::regclass " \
	"    AND d.objid = c.oid AND d.refclassid = 'pg_extension'::regclass AND d.deptype = 'x'), '-'), " \
	" coalesce((SELECT co.conname || ':' || coalesce(obj_description(co.oid, 'pg_constraint'), '-') " \
	"    FROM pg_constraint co WHERE co.conindid = c.oid AND co.conrelid = t.oid), '-') " \
	"FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid " \
	"JOIN pg_class t ON t.oid = i.indrelid " \
	"LEFT JOIN pg_tablespace ts ON ts.oid = c.reltablespace " \
	"WHERE i.indrelid IN (SELECT relid FROM pg_partition_tree('cfc2_r')) " \
	"ORDER BY 1, 2, 3"

static const char *pi_cleanup =
"DROP TABLE IF EXISTS cfc2_r CASCADE; "
"DROP TABLE IF EXISTS cfc2_r_l0, cfc2_r_l1, cfc2_r_l2, cfc2_r_m, cfc2_r_m0, cfc2_r_m1 CASCADE";

PG_PROP(partindex_rebuild_preserves_props)
{
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	bool		has_g = pg_prop_draw_bool(tc);
	bool		use_ts = pg_prop_draw_bool(tc);
	int			nleaf = (int) pg_prop_draw_int(tc, 1, 3);
	bool		has_mid = pg_prop_draw_bool(tc);
	int			nmid = has_mid ? (int) pg_prop_draw_int(tc, 1, 2) : 0;
	int			nidx = (int) pg_prop_draw_int(tc, 1, 3);
	int			kinds[3];
	int			alter;
	char	   *map;
	char	   *before;
	char	   *after;
	char	   *line;
	char	   *save = NULL;
	int			nmut = 0;

	c2_exec(tc, conn, pi_cleanup);
	/* CREATE/DROP TABLESPACE refuse to run in a multi-statement string */
	c2_try(conn, "DROP TABLESPACE IF EXISTS cfc2_ts");
	if (use_ts)
	{
		c2_exec(tc, conn, "SET allow_in_place_tablespaces = on");
		c2_exec(tc, conn, "CREATE TABLESPACE cfc2_ts LOCATION ''");
	}

	initStringInfo(&s);
	appendStringInfo(&s,
					 "CREATE TABLE cfc2_r (id int NOT NULL, a int NOT NULL, b int NOT NULL%s) "
					 "PARTITION BY RANGE (id); ",
					 has_g ? ", g int GENERATED ALWAYS AS (b * 2) STORED NOT NULL" : "");
	for (int i = 0; i < nleaf; i++)
	{
		if (pg_prop_draw_bool(tc))
			appendStringInfo(&s,
							 "CREATE TABLE cfc2_r_l%d PARTITION OF cfc2_r "
							 "FOR VALUES FROM (%d) TO (%d); ", i, i * 100, i * 100 + 100);
		else
		{
			/* different physical column order, with a dropped column */
			appendStringInfo(&s,
							 "CREATE TABLE cfc2_r_l%d (x int, b int NOT NULL%s, a int NOT NULL, "
							 "id int NOT NULL); ALTER TABLE cfc2_r_l%d DROP COLUMN x; "
							 "ALTER TABLE cfc2_r ATTACH PARTITION cfc2_r_l%d "
							 "FOR VALUES FROM (%d) TO (%d); ",
							 i, has_g ? ", g int GENERATED ALWAYS AS (b * 2) STORED NOT NULL" : "",
							 i, i, i * 100, i * 100 + 100);
		}
	}
	if (has_mid)
	{
		appendStringInfoString(&s,
							   "CREATE TABLE cfc2_r_m PARTITION OF cfc2_r FOR VALUES FROM (900) TO (1000) "
							   "PARTITION BY RANGE (id); ");
		for (int i = 0; i < nmid; i++)
			appendStringInfo(&s,
							 "CREATE TABLE cfc2_r_m%d PARTITION OF cfc2_r_m "
							 "FOR VALUES FROM (%d) TO (%d); ", i, 900 + i * 50, 950 + i * 50);
	}
	/* a few rows, spread over the partitions */
	appendStringInfo(&s,
					 "INSERT INTO cfc2_r (id, a, b) SELECT p * 100 + k, k %% 4, k "
					 "FROM generate_series(0, %d) p, generate_series(0, %d) k; ",
					 nleaf - 1, (int) pg_prop_draw_int(tc, 0, 5));
	if (has_mid)
		appendStringInfoString(&s, "INSERT INTO cfc2_r (id, a, b) VALUES (901, 1, 1), (902, 2, 2); ");

	for (int k = 0; k < nidx; k++)
	{
		int			kind;

		kind = (int) pg_prop_draw_int(tc, 0, lengthof(pi_kinds) - 1);
		if (pi_kinds[kind].uses_g && !has_g)
			kind -= 5;			/* the first five kinds don't use g */
		kinds[k] = kind;
		if (pi_kinds[kind].constraint)
			appendStringInfo(&s, "ALTER TABLE cfc2_r ADD CONSTRAINT cfc2_rx%d %s; ",
							 k, pi_kinds[kind].def);
		else
			appendStringInfo(&s, "CREATE %sINDEX cfc2_rx%d ON cfc2_r %s; ",
							 pi_kinds[kind].replident ? "UNIQUE " : "", k,
							 pi_kinds[kind].def);
	}
	c2_exec(tc, conn, s.data);

	/*
	 * Every index of the tree: "<root ordinal>\t<table>\t<relkind>\t<index>".
	 * Mutate each one's user-settable properties at random.
	 */
	map = c2_rows(tc, conn,
				  "SELECT substr(pg_partition_root(i.indexrelid)::regclass::text, 8), "
				  "quote_ident(t.relname), t.relkind, quote_ident(c.relname) "
				  "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
				  "JOIN pg_class t ON t.oid = i.indrelid "
				  "WHERE i.indrelid IN (SELECT relid FROM pg_partition_tree('cfc2_r')) "
				  "ORDER BY 1, 2");
	for (line = strtok_r(map, "\n", &save); line; line = strtok_r(NULL, "\n", &save))
	{
		char	   *f[4];
		char	   *fs = NULL;
		const PiKind *pk;
		bool		leaf;

		f[0] = strtok_r(line, "\t", &fs);
		f[1] = strtok_r(NULL, "\t", &fs);
		f[2] = strtok_r(NULL, "\t", &fs);
		f[3] = strtok_r(NULL, "\t", &fs);
		if (!f[0] || !f[1] || !f[2] || !f[3])
			continue;
		pk = &pi_kinds[kinds[atoi(f[0]) % nidx]];
		leaf = (f[2][0] == 'r');

		resetStringInfo(&s);
		if (pg_prop_draw_int(tc, 0, 3) == 0)
			appendStringInfo(&s, "COMMENT ON INDEX %s IS 'c%d'; ", f[3], nmut);
		if (pk->exprcol > 0 && pg_prop_draw_int(tc, 0, 2) == 0)
			appendStringInfo(&s, "ALTER INDEX %s ALTER COLUMN %d SET STATISTICS %d; ",
							 f[3], pk->exprcol, (int) pg_prop_draw_int(tc, 0, 1000));
		if (leaf && pg_prop_draw_int(tc, 0, 3) == 0)
			appendStringInfo(&s, "ALTER INDEX %s SET (fillfactor = %d); ",
							 f[3], (int) pg_prop_draw_int(tc, 50, 99));
		if (use_ts && pg_prop_draw_int(tc, 0, 3) == 0)
			appendStringInfo(&s, "ALTER INDEX %s SET TABLESPACE %s; ", f[3],
							 pg_prop_draw_bool(tc) ? "cfc2_ts" : "pg_default");
		if (pk->replident && pg_prop_draw_int(tc, 0, 2) == 0)
			appendStringInfo(&s, "ALTER TABLE %s REPLICA IDENTITY USING INDEX %s; ",
							 f[1], f[3]);
		if (pk->clusterable && leaf && pg_prop_draw_int(tc, 0, 3) == 0)
			appendStringInfo(&s, "ALTER TABLE %s CLUSTER ON %s; ", f[1], f[3]);
		if (pg_prop_draw_int(tc, 0, 4) == 0)
			appendStringInfo(&s, "ALTER INDEX %s DEPENDS ON EXTENSION plpgsql; ", f[3]);
		if (pk->constraint && pg_prop_draw_int(tc, 0, 3) == 0)
			appendStringInfo(&s, "COMMENT ON CONSTRAINT %s ON %s IS 'k%d'; ",
							 f[3], f[1], nmut);
		/* rename last: the statements above use the current name */
		if (pg_prop_draw_int(tc, 0, 2) == 0)
			appendStringInfo(&s, "ALTER INDEX %s RENAME TO cfc2_n%d; ", f[3], nmut);
		nmut++;
		if (s.len > 0)
			c2_exec(tc, conn, s.data);
	}
	pg_free(map);

	before = c2_rows(tc, conn, PI_SNAPSHOT);

	/* the rebuild-forcing DDL */
	alter = (int) pg_prop_draw_int(tc, 0, has_g ? 2 : 1);
	resetStringInfo(&s);
	if (use_ts && pg_prop_draw_bool(tc))
		appendStringInfoString(&s, "SET default_tablespace = cfc2_ts; ");
	switch (alter)
	{
		case 0:
			appendStringInfoString(&s, "ALTER TABLE cfc2_r ALTER COLUMN a TYPE int");
			break;
		case 1:
			appendStringInfoString(&s, "ALTER TABLE cfc2_r ALTER COLUMN a TYPE bigint");
			break;
		default:
			appendStringInfoString(&s, "ALTER TABLE cfc2_r ALTER COLUMN g SET EXPRESSION AS (b * 3)");
			break;
	}
	pg_prop_note(tc, "rebuild DDL: %s", s.data);
	c2_exec(tc, conn, s.data);
	c2_exec(tc, conn, "RESET default_tablespace");

	after = c2_rows(tc, conn, PI_SNAPSHOT);
	if (strcmp(before, after) != 0)
		pg_prop_fail(tc,
					 "index rebuild changed index properties of the partition tree\n"
					 "  DDL: %s\n"
					 "  (root, table, index, comment, reloptions, tablespace, stattargets, "
					 "indisreplident, indisclustered, relreplident, extdeps, constraint)\n"
					 "  before:\n%s  after:\n%s",
					 s.data, before, after);

	c2_exec(tc, conn, pi_cleanup);
	if (use_ts)
		c2_exec(tc, conn, "DROP TABLESPACE cfc2_ts");
	pg_free(before);
	pg_free(after);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 6579: REFRESH MATERIALIZED VIEW CONCURRENTLY == the defining query
 * ----------------------------------------------------------------------
 */

/* append a drawn value for a column of the given kind (0 int, 1 text) */
static void
mv_value(pg_prop_tc *tc, StringInfo b, int kind, int dom)
{
	int			v = (int) pg_prop_draw_int(tc, -1, dom);

	if (v < 0)
		appendStringInfoString(b, "NULL");
	else if (kind == 0)
		appendStringInfo(b, "%d", v);
	else
		appendStringInfo(b, "'t%d'", v);
}

/* multiset digest of a relation/query's rows */
static char *
mv_digest(pg_prop_tc *tc, pg_prop_conn *conn, const char *from)
{
	StringInfoData q;
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT count(*) || ':' || coalesce(md5(string_agg(x::text, '|' ORDER BY x::text)), '') "
					 "FROM (%s) x", from);
	v = pg_prop_sql_scalar(conn, q.data, &failed, &err);
	if (failed || v == NULL)
		pg_prop_fail(tc, "digest failed: %s\n  SQL: %s", err ? err : "", q.data);
	pfree(q.data);
	return v;
}

/*
 * After every REFRESH MATERIALIZED VIEW CONCURRENTLY: if it succeeded, the
 * matview holds exactly the multiset of rows its query returns now; if it
 * failed, the matview is unchanged.  (A unique index may make the refresh
 * fail; it may never make it silently drop or merge rows.)  Base tables get
 * duplicate rows, NULLs in indexed and non-indexed columns, and a random set
 * of unique indexes on the matview.
 */
PG_PROP(refresh_concurrently_matches_query)
{
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	int			ncols = (int) pg_prop_draw_int(tc, 1, 4);
	int			dom = (int) pg_prop_draw_int(tc, 0, 4);
	int			kinds[4];
	int			nidx;
	int			rounds = (int) pg_prop_draw_int(tc, 1, 4);
	bool		filtered = pg_prop_draw_bool(tc);
	const char *query;

	c2_exec(tc, conn, "DROP TABLE IF EXISTS cfc2_mf CASCADE");
	initStringInfo(&s);
	appendStringInfoString(&s, "CREATE TABLE cfc2_mf (");
	for (int c = 0; c < ncols; c++)
	{
		kinds[c] = (int) pg_prop_draw_int(tc, 0, 1);
		appendStringInfo(&s, "%sc%d %s", c ? ", " : "", c, kinds[c] ? "text" : "int");
	}
	appendStringInfoString(&s, ")");
	c2_exec(tc, conn, s.data);

	/* initial rows */
	resetStringInfo(&s);
	appendStringInfoString(&s, "INSERT INTO cfc2_mf VALUES ");
	{
		int			n = (int) pg_prop_draw_int(tc, 1, 8);

		for (int r = 0; r < n; r++)
		{
			appendStringInfoString(&s, r ? ", (" : "(");
			for (int c = 0; c < ncols; c++)
			{
				if (c)
					appendStringInfoString(&s, ", ");
				mv_value(tc, &s, kinds[c], dom);
			}
			appendStringInfoChar(&s, ')');
		}
	}
	/* may violate a unique index created below; then just start empty */
	c2_try(conn, s.data);

	query = filtered ? "SELECT * FROM cfc2_mf WHERE c0::text IS DISTINCT FROM '0'"
		: "SELECT * FROM cfc2_mf";
	resetStringInfo(&s);
	appendStringInfo(&s, "CREATE MATERIALIZED VIEW cfc2_mv AS %s", query);
	c2_exec(tc, conn, s.data);

	/* unique indexes on drawn column subsets; at least one must succeed */
	nidx = 0;
	for (int k = 0; k < 3 && (nidx == 0 || pg_prop_draw_bool(tc)); k++)
	{
		bool		first = true;

		resetStringInfo(&s);
		appendStringInfo(&s, "CREATE UNIQUE INDEX cfc2_mv_u%d ON cfc2_mv (", k);
		for (int c = 0; c < ncols; c++)
		{
			if (pg_prop_draw_bool(tc) || (first && c == ncols - 1))
			{
				appendStringInfo(&s, "%sc%d", first ? "" : ", ", c);
				first = false;
			}
		}
		appendStringInfo(&s, ")%s",
						 pg_prop_draw_int(tc, 0, 3) == 0 ? " NULLS NOT DISTINCT" : "");
		if (c2_try(conn, s.data))
			nidx++;
	}
	if (nidx == 0)
	{
		/* initial data violates every drawn index: empty it and retry once */
		c2_exec(tc, conn, "TRUNCATE cfc2_mf; REFRESH MATERIALIZED VIEW cfc2_mv; "
				"CREATE UNIQUE INDEX cfc2_mv_u9 ON cfc2_mv (c0)");
	}

	for (int r = 0; r < rounds; r++)
	{
		int			nmod = (int) pg_prop_draw_int(tc, 1, 4);
		char	   *before;
		char	   *after;
		char	   *want;
		char	   *err = NULL;
		bool		ok;

		/* mutate the base table: duplicate existing rows, insert, update, delete */
		for (int m = 0; m < nmod; m++)
		{
			resetStringInfo(&s);
			switch ((int) pg_prop_draw_int(tc, 0, 3))
			{
				case 0:
					appendStringInfo(&s,
									 "INSERT INTO cfc2_mf SELECT * FROM cfc2_mf "
									 "ORDER BY ctid LIMIT %d OFFSET %d",
									 (int) pg_prop_draw_int(tc, 1, 3),
									 (int) pg_prop_draw_int(tc, 0, 6));
					break;
				case 1:
					appendStringInfoString(&s, "INSERT INTO cfc2_mf VALUES (");
					for (int c = 0; c < ncols; c++)
					{
						if (c)
							appendStringInfoString(&s, ", ");
						mv_value(tc, &s, kinds[c], dom);
					}
					appendStringInfoChar(&s, ')');
					break;
				case 2:
					{
						int			c = (int) pg_prop_draw_int(tc, 0, ncols - 1);

						appendStringInfo(&s, "UPDATE cfc2_mf SET c%d = ", c);
						mv_value(tc, &s, kinds[c], dom);
						appendStringInfo(&s, " WHERE ctid IN (SELECT ctid FROM cfc2_mf "
										 "ORDER BY ctid LIMIT %d OFFSET %d)",
										 (int) pg_prop_draw_int(tc, 1, 2),
										 (int) pg_prop_draw_int(tc, 0, 6));
					}
					break;
				default:
					appendStringInfo(&s,
									 "DELETE FROM cfc2_mf WHERE ctid IN (SELECT ctid FROM cfc2_mf "
									 "ORDER BY ctid LIMIT %d OFFSET %d)",
									 (int) pg_prop_draw_int(tc, 1, 2),
									 (int) pg_prop_draw_int(tc, 0, 6));
					break;
			}
			c2_exec(tc, conn, s.data);
		}

		before = mv_digest(tc, conn, "SELECT * FROM cfc2_mv");
		ok = pg_prop_sql_exec(conn, "REFRESH MATERIALIZED VIEW CONCURRENTLY cfc2_mv", &err);
		after = mv_digest(tc, conn, "SELECT * FROM cfc2_mv");
		want = mv_digest(tc, conn, query);
		if (ok && strcmp(after, want) != 0)
		{
			char	   *got = c2_rows(tc, conn, "SELECT * FROM cfc2_mv ORDER BY 1 LIMIT 20");
			char	   *exp = c2_rows(tc, conn,
									  filtered ? "SELECT * FROM cfc2_mf WHERE c0::text IS DISTINCT FROM '0' ORDER BY 1 LIMIT 20"
									  : "SELECT * FROM cfc2_mf ORDER BY 1 LIMIT 20");

			pg_prop_fail(tc,
						 "REFRESH MATERIALIZED VIEW CONCURRENTLY succeeded but the matview "
						 "differs from its query (round %d)\n  matview %s:\n%s  query %s:\n%s",
						 r, after, got, want, exp);
		}
		if (!ok && strcmp(before, after) != 0)
			pg_prop_fail(tc, "failed REFRESH CONCURRENTLY (%s) changed the matview",
						 err ? err : "");
		if (err)
			pg_free(err);
		pg_free(before);
		pg_free(after);
		pg_free(want);
	}

	c2_exec(tc, conn, "DROP TABLE cfc2_mf CASCADE");
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 6770: UPDATE/DELETE through postgres_fdw on a remote partitioned table
 * ----------------------------------------------------------------------
 */

/*
 * A foreign table whose remote side is a partitioned (or inheritance) table
 * must behave like the remote table itself: an UPDATE or DELETE through it
 * changes exactly the rows its WHERE clause selects.  The model is a plain
 * local table holding the same rows, to which the same statement is applied
 * directly.  The WHERE clause sometimes contains a non-shippable volatile
 * call so postgres_fdw cannot push the statement down and must address
 * remote rows one by one (by ctid -- which is not unique across partitions).
 */
PG_PROP(fdw_partitioned_remote_dml_matches_local)
{
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	StringInfoData w;
	int			nparts = (int) pg_prop_draw_int(tc, 1, 4);
	bool		inherit = pg_prop_draw_bool(tc);
	int			nrows = (int) pg_prop_draw_int(tc, 1, 12);
	int			nstmts = (int) pg_prop_draw_int(tc, 1, 3);

	if (!pg_prop_require_extension(conn, "postgres_fdw"))
		pg_prop_skip(tc, "postgres_fdw not installed");

	initStringInfo(&s);
	initStringInfo(&w);
	c2_exec(tc, conn,
			"DROP FOREIGN TABLE IF EXISTS cfc2_ft; "
			"DROP TABLE IF EXISTS cfc2_fr, cfc2_fl CASCADE; "
			"DROP SERVER IF EXISTS cfc2_loop CASCADE");
	c2_exec(tc, conn,
			"DO $$ BEGIN EXECUTE format('CREATE SERVER cfc2_loop FOREIGN DATA WRAPPER postgres_fdw "
			"OPTIONS (dbname %L, port %L)', current_database(), current_setting('port')); "
			"EXECUTE format('CREATE USER MAPPING FOR CURRENT_USER SERVER cfc2_loop "
			"OPTIONS (user %L)', current_user); END $$");

	if (inherit)
	{
		appendStringInfoString(&s, "CREATE TABLE cfc2_fr (a int, b int); ");
		for (int p = 0; p < nparts; p++)
			appendStringInfo(&s,
							 "CREATE TABLE cfc2_fr%d (CHECK (a = %d)) INHERITS (cfc2_fr); ", p, p);
	}
	else
	{
		appendStringInfoString(&s, "CREATE TABLE cfc2_fr (a int, b int) PARTITION BY LIST (a); ");
		for (int p = 0; p < nparts; p++)
			appendStringInfo(&s,
							 "CREATE TABLE cfc2_fr%d PARTITION OF cfc2_fr FOR VALUES IN (%d); ", p, p);
	}
	appendStringInfoString(&s, "CREATE TABLE cfc2_fl (a int, b int); ");
	c2_exec(tc, conn, s.data);

	/* the same rows on both sides; rows go straight into their partition */
	for (int r = 0; r < nrows; r++)
	{
		int			a = (int) pg_prop_draw_int(tc, 0, nparts - 1);
		int			b = (int) pg_prop_draw_int(tc, 0, 5);

		resetStringInfo(&s);
		appendStringInfo(&s,
						 "INSERT INTO cfc2_fr%d VALUES (%d, %d); INSERT INTO cfc2_fl VALUES (%d, %d)",
						 a, a, b, a, b);
		c2_exec(tc, conn, s.data);
	}
	c2_exec(tc, conn,
			"CREATE FOREIGN TABLE cfc2_ft (a int, b int) SERVER cfc2_loop "
			"OPTIONS (table_name 'cfc2_fr')");

	for (int k = 0; k < nstmts; k++)
	{
		char	   *got;
		char	   *want;
		char	   *ng;
		char	   *nw;

		resetStringInfo(&w);
		switch ((int) pg_prop_draw_int(tc, 0, 3))
		{
			case 0:
				appendStringInfo(&w, "a = %d", (int) pg_prop_draw_int(tc, 0, nparts - 1));
				break;
			case 1:
				appendStringInfo(&w, "b = %d", (int) pg_prop_draw_int(tc, 0, 5));
				break;
			case 2:
				appendStringInfo(&w, "a = %d AND b <= %d",
								 (int) pg_prop_draw_int(tc, 0, nparts - 1),
								 (int) pg_prop_draw_int(tc, 0, 5));
				break;
			default:
				appendStringInfo(&w, "b > %d", (int) pg_prop_draw_int(tc, 0, 5));
				break;
		}
		if (pg_prop_draw_int(tc, 0, 3) != 0)
			appendStringInfoString(&w, " AND random() <= 1");	/* not shippable */

		resetStringInfo(&s);
		if (pg_prop_draw_bool(tc))
			appendStringInfo(&s,
							 "WITH m AS (UPDATE %%s SET b = b + 10 WHERE %s RETURNING 1) "
							 "SELECT count(*) FROM m", w.data);
		else
			appendStringInfo(&s,
							 "WITH m AS (DELETE FROM %%s WHERE %s RETURNING 1) "
							 "SELECT count(*) FROM m", w.data);
		{
			char		buf[1024];
			bool		failed = false;
			char	   *err = NULL;

			snprintf(buf, sizeof(buf), s.data, "cfc2_ft");
			pg_prop_note(tc, "stmt %d: %s", k, buf);
			ng = pg_prop_sql_scalar(conn, buf, &failed, &err);
			if (failed)
				pg_prop_fail(tc, "foreign DML failed: %s\n  SQL: %s", err ? err : "", buf);
			snprintf(buf, sizeof(buf), s.data, "cfc2_fl");
			nw = pg_prop_sql_scalar(conn, buf, &failed, &err);
			if (failed)
				pg_prop_fail(tc, "local DML failed: %s\n  SQL: %s", err ? err : "", buf);
		}
		got = c2_rows(tc, conn, "SELECT a, b FROM cfc2_fr ORDER BY a, b");
		want = c2_rows(tc, conn, "SELECT a, b FROM cfc2_fl ORDER BY a, b");
		if (strcmp(got, want) != 0 || strcmp(ng ? ng : "", nw ? nw : "") != 0)
			pg_prop_fail(tc,
						 "DML through a foreign table on a remote %s table differs from "
						 "the same DML on a local table\n  WHERE %s\n"
						 "  rows reported: foreign %s, local %s\n"
						 "  remote now (a, b):\n%s  expected:\n%s",
						 inherit ? "inheritance" : "partitioned", w.data,
						 ng ? ng : "?", nw ? nw : "?", got, want);
		pg_free(got);
		pg_free(want);
	}

	c2_exec(tc, conn,
			"DROP FOREIGN TABLE cfc2_ft; DROP TABLE cfc2_fr, cfc2_fl CASCADE; "
			"DROP SERVER cfc2_loop CASCADE");
	pfree(s.data);
	pfree(w.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 6842: auto-named constraints on a partition tree spanning schemas
 * ----------------------------------------------------------------------
 */

/*
 * An auto-named ALTER TABLE root ADD CHECK / SET NOT NULL / ADD NOT NULL
 * picks the name itself, so it must never fail with "constraint already
 * exists" (42710): the server is free to pick any unused name, whatever
 * names the partitions' existing constraints have and whatever schemas the
 * partitions live in.  Afterwards every partition has the new constraint
 * (inherited: same name for CHECK, a NOT NULL on the column for NOT NULL).
 * The partitions' pre-existing constraints are drawn to take exactly the
 * names the root would pick, or numbered variants of them.
 */
PG_PROP(partition_autoname_constraint_never_collides)
{
	static const char *const schemas[] = {"public", "cfc2_s1", "cfc2_s2"};
	static const char *const ddl[] = {
		"ADD CHECK (a > -50)",
		"ADD CHECK (a + b > -50)",
		"ALTER COLUMN a SET NOT NULL",
		"ADD NOT NULL a",
		"ALTER COLUMN b SET NOT NULL",
	};
	static const char *const suffix[] = {
		"_a_check", "_check", "_a_not_null", "_b_not_null", "_b_check", "_a_check1",
		"_a_b_check",
	};
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	const char *rootschema = schemas[pg_prop_draw_int(tc, 0, 1)];
	bool		mid = pg_prop_draw_bool(tc);
	int			nleaf = (int) pg_prop_draw_int(tc, 1, 3);
	int			d = (int) pg_prop_draw_int(tc, 0, lengthof(ddl) - 1);
	char		leafq[4][64];
	char		parentq[64];
	char	   *err = NULL;
	long		missing;

	initStringInfo(&s);
	c2_exec(tc, conn,
			"DROP TABLE IF EXISTS public.cfc2_pp, cfc2_s1.cfc2_pp CASCADE; "
			"DROP SCHEMA IF EXISTS cfc2_s1, cfc2_s2 CASCADE; "
			"DROP TABLE IF EXISTS public.cfc2_pp_mid, public.cfc2_pp_0, public.cfc2_pp_1, "
			"public.cfc2_pp_2 CASCADE; "
			"CREATE SCHEMA cfc2_s1; CREATE SCHEMA cfc2_s2");
	appendStringInfo(&s, "CREATE TABLE %s.cfc2_pp (a int, b int) PARTITION BY RANGE (a); ",
					 rootschema);
	if (mid)
	{
		snprintf(parentq, sizeof(parentq), "%s.cfc2_pp_mid",
				 schemas[pg_prop_draw_int(tc, 0, 2)]);
		appendStringInfo(&s,
						 "CREATE TABLE %s PARTITION OF %s.cfc2_pp "
						 "FOR VALUES FROM (0) TO (1000) PARTITION BY RANGE (a); ",
						 parentq, rootschema);
	}
	else
		snprintf(parentq, sizeof(parentq), "%s.cfc2_pp", rootschema);
	for (int i = 0; i < nleaf; i++)
	{
		const char *ls = schemas[pg_prop_draw_int(tc, 0, 2)];
		bool		samename = strcmp(ls, rootschema) != 0 && pg_prop_draw_bool(tc);
		char		cand[64];

		/* a leaf may share the root's relname in another schema (once) */
		snprintf(cand, sizeof(cand), "%s.cfc2_pp", ls);
		for (int j = 0; j < i; j++)
			if (strcmp(leafq[j], cand) == 0)
				samename = false;
		if (samename)
			strlcpy(leafq[i], cand, sizeof(leafq[i]));
		else
			snprintf(leafq[i], sizeof(leafq[i]), "%s.cfc2_pp_%d", ls, i);
		appendStringInfo(&s, "CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d); ",
						 leafq[i], parentq, i * 10, i * 10 + 10);
	}
	appendStringInfo(&s, "INSERT INTO %s.cfc2_pp SELECT g, g FROM generate_series(0, %d) g; ",
					 rootschema, nleaf * 10 - 1);
	c2_exec(tc, conn, s.data);

	/* pre-existing constraints on partitions, named like the root would */
	for (int i = 0; i < nleaf; i++)
	{
		int			nc = (int) pg_prop_draw_int(tc, 0, 2);

		for (int k = 0; k < nc; k++)
		{
			const char *suf = suffix[pg_prop_draw_int(tc, 0, lengthof(suffix) - 1)];

			resetStringInfo(&s);
			switch ((int) pg_prop_draw_int(tc, 0, 2))
			{
				case 0:
					appendStringInfo(&s, "ALTER TABLE %s ADD CONSTRAINT cfc2_pp%s CHECK (a > -%d)",
									 leafq[i], suf, (int) pg_prop_draw_int(tc, 1, 99));
					break;
				case 1:
					appendStringInfo(&s, "ALTER TABLE %s ADD CONSTRAINT cfc2_pp%s CHECK (a IS NOT NULL)",
									 leafq[i], suf);
					break;
				default:
					appendStringInfo(&s, "ALTER TABLE %s ADD CHECK (a > -%d)",
									 leafq[i], (int) pg_prop_draw_int(tc, 1, 99));
					break;
			}
			/* duplicate names on the same leaf are legitimately refused */
			c2_try(conn, s.data);
		}
	}

	resetStringInfo(&s);
	appendStringInfo(&s, "ALTER TABLE %s.cfc2_pp %s", rootschema, ddl[d]);
	if (!pg_prop_sql_exec(conn, s.data, &err))
	{
		char	   *cons;
		char	   *sql = pg_strdup(s.data);

		resetStringInfo(&s);
		appendStringInfo(&s,
						 "SELECT conrelid::regclass, conname, contype FROM pg_constraint "
						 "WHERE conrelid IN (SELECT relid FROM pg_partition_tree('%s.cfc2_pp')) "
						 "ORDER BY 1, 2", rootschema);
		cons = c2_rows(tc, conn, s.data);

		pg_prop_fail(tc, "auto-named constraint DDL on the partition root failed: %s"
					 "  SQL: %s\n  existing constraints:\n%s",
					 err ? err : "", sql, cons);
	}

	/* every partition inherited it */
	resetStringInfo(&s);
	if (d <= 1)
		appendStringInfo(&s,
						 "SELECT count(*) FROM pg_partition_tree('%s.cfc2_pp') t "
						 "WHERE NOT EXISTS (SELECT 1 FROM pg_constraint c, pg_constraint r "
						 "WHERE r.conrelid = '%s.cfc2_pp'::regclass AND r.contype = 'c' "
						 "AND r.conislocal AND c.conrelid = t.relid AND c.contype = 'c' "
						 "AND c.conname = r.conname AND (c.conrelid = r.conrelid OR c.coninhcount > 0))",
						 rootschema, rootschema);
	else
		appendStringInfo(&s,
						 "SELECT count(*) FROM pg_partition_tree('%s.cfc2_pp') t "
						 "WHERE NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_attribute a "
						 "ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1] "
						 "WHERE c.conrelid = t.relid AND c.contype = 'n' AND a.attname = '%c')",
						 rootschema, d == 4 ? 'b' : 'a');
	missing = c2_count(tc, conn, s.data);
	if (missing)
		pg_prop_fail(tc, "%ld partitions lack the constraint added by ALTER TABLE ... %s",
					 missing, ddl[d]);

	c2_exec(tc, conn,
			"DROP TABLE IF EXISTS public.cfc2_pp, cfc2_s1.cfc2_pp CASCADE; "
			"DROP SCHEMA IF EXISTS cfc2_s1, cfc2_s2 CASCADE");
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 6961: REVOKE keeps every grant chain rooted at the owner
 * ----------------------------------------------------------------------
 */

#define ACL_NROLES 4

/* entries (grantor, grantee, privilege) whose grantor no longer holds the option */
#define ACL_ORPHANS \
	"SELECT coalesce(string_agg(format('%s granted %s to %s', a.grantor::regrole, " \
	"  a.privilege_type, CASE a.grantee WHEN 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END), " \
	"  '; '), '') " \
	"FROM pg_class c, aclexplode(c.relacl) a " \
	"WHERE c.oid = 'cfc2_acl'::regclass AND a.grantor <> c.relowner " \
	"AND NOT EXISTS (SELECT 1 FROM aclexplode(c.relacl) b WHERE b.grantee = a.grantor " \
	"  AND b.privilege_type = a.privilege_type AND b.is_grantable)"

static const char *acl_cleanup =
"RESET SESSION AUTHORIZATION; DROP TABLE IF EXISTS cfc2_acl; "
"DROP ROLE IF EXISTS cfc2_r0, cfc2_r1, cfc2_r2, cfc2_r3";

/*
 * A grant made by a non-owner grantor is only legitimate while the grantor
 * holds the grant option directly in the ACL; REVOKE without CASCADE must
 * refuse to break that ("dependent privileges exist") and REVOKE ... CASCADE
 * must remove the dependants.  So after any sequence of GRANT / REVOKE /
 * role-membership / SUPERUSER changes, issued by drawn roles, every ACL entry
 * whose grantor is not the owner is backed by a grant-option entry for the
 * same privilege held by that grantor.  (This is also what pg_dump needs to
 * recreate the ACL.)  Statements may legitimately fail (permissions); the
 * invariant is checked after every one.
 */
PG_PROP(revoke_keeps_grant_chains_rooted)
{
	static const char *const privs[] = {"SELECT", "UPDATE"};
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	StringInfoData log;
	int			nsteps = (int) pg_prop_draw_int(tc, 1, 16);
	bool		holder[2][ACL_NROLES];

	initStringInfo(&s);
	initStringInfo(&log);
	/* "no privileges were granted" warnings are expected noise here */
	c2_exec(tc, conn, "SET client_min_messages = error");
	c2_exec(tc, conn, acl_cleanup);
	c2_exec(tc, conn,
			"CREATE ROLE cfc2_r0; CREATE ROLE cfc2_r1; CREATE ROLE cfc2_r2; CREATE ROLE cfc2_r3; "
			"CREATE TABLE cfc2_acl (a int); ALTER TABLE cfc2_acl OWNER TO cfc2_r0");

	/* random memberships (r_i member of r_j), often of the owner */
	{
		int			nm = (int) pg_prop_draw_int(tc, 0, 3);

		for (int m = 0; m < nm; m++)
		{
			int			of = pg_prop_draw_bool(tc) ? 0 : (int) pg_prop_draw_int(tc, 0, ACL_NROLES - 1);
			int			who = (int) pg_prop_draw_int(tc, 1, ACL_NROLES - 1);

			resetStringInfo(&s);
			appendStringInfo(&s, "GRANT cfc2_r%d TO cfc2_r%d%s", of, who,
							 pg_prop_draw_int(tc, 0, 3) == 0 ? " WITH INHERIT FALSE" : "");
			if (of != who && c2_try(conn, s.data))
				appendStringInfo(&log, "%s;\n", s.data);
		}
	}

	/*
	 * holder[p][r]: r was (last we know) granted privs[p] WITH GRANT OPTION.
	 * Only steers the generator toward actors that can extend a chain; the
	 * invariant is computed from the catalog, not from this.
	 */
	memset(holder, 0, sizeof(holder));
	for (int p = 0; p < (int) lengthof(privs); p++)
		holder[p][0] = true;	/* the owner */

	for (int k = 0; k < nsteps; k++)
	{
		int			pi = (int) pg_prop_draw_int(tc, 0, lengthof(privs) - 1);
		const char *priv = privs[pi];
		int			actor = (int) pg_prop_draw_int(tc, 0, ACL_NROLES - 1);
		int			target = (int) pg_prop_draw_int(tc, 1, ACL_NROLES - 1);
		int			op = (int) pg_prop_draw_int(tc, 0, 5);
		bool		go = false;
		char	   *orph;

		/* usually let a role that holds the option act */
		if (op <= 3 && pg_prop_draw_int(tc, 0, 3) != 0)
		{
			int			start = actor;

			while (!holder[pi][actor])
			{
				actor = (actor + 1) % ACL_NROLES;
				if (actor == start)
					break;
			}
		}
		resetStringInfo(&s);
		switch (op)
		{
			case 0:
			case 1:
				go = pg_prop_draw_int(tc, 0, 2) != 0;
				appendStringInfo(&s, "SET SESSION AUTHORIZATION cfc2_r%d; "
								 "GRANT %s ON cfc2_acl TO cfc2_r%d%s",
								 actor, priv, target, go ? " WITH GRANT OPTION" : "");
				break;
			case 2:
			case 3:
				appendStringInfo(&s, "SET SESSION AUTHORIZATION cfc2_r%d; "
								 "REVOKE %s%s ON cfc2_acl FROM cfc2_r%d %s",
								 actor,
								 pg_prop_draw_bool(tc) ? "GRANT OPTION FOR " : "",
								 priv, target,
								 pg_prop_draw_int(tc, 0, 2) != 0 ? "RESTRICT" : "CASCADE");
				break;
			case 4:
				appendStringInfo(&s, "GRANT cfc2_r%d TO cfc2_r%d",
								 pg_prop_draw_bool(tc) ? 0 : (int) pg_prop_draw_int(tc, 0, ACL_NROLES - 1),
								 target);
				break;
			default:
				appendStringInfo(&s, "ALTER ROLE cfc2_r%d %sSUPERUSER", target,
								 pg_prop_draw_bool(tc) ? "" : "NO");
				break;
		}
		if (c2_try(conn, s.data))
		{
			appendStringInfo(&log, "%s;\n", s.data);
			if (go)
				holder[pi][target] = true;
		}
		else
			appendStringInfo(&log, "%s;  -- failed\n", s.data);
		c2_exec(tc, conn, "RESET SESSION AUTHORIZATION");

		orph = pg_prop_sql_scalar(conn, ACL_ORPHANS, NULL, NULL);
		if (orph && orph[0] != '\0')
		{
			char	   *acl = pg_prop_sql_scalar(conn,
												 "SELECT relacl::text FROM pg_class WHERE oid = 'cfc2_acl'::regclass",
												 NULL, NULL);

			pg_prop_fail(tc,
						 "ACL has grants whose grantor does not hold the grant option "
						 "(broken grant chain): %s\n  relacl: %s\n  statements:\n%s",
						 orph, acl ? acl : "?", log.data);
		}
		if (orph)
			pg_free(orph);
	}

	c2_exec(tc, conn, acl_cleanup);
	pfree(s.data);
	pfree(log.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 7055: every effective ALTER PUBLICATION is reported to event triggers
 * ----------------------------------------------------------------------
 */

/* everything ALTER PUBLICATION can change about cfc2_ep */
#define EP_STATE \
	"SELECT concat_ws(' | ', " \
	" (SELECT format('%s %s %s %s %s %s %s %s %s', puballtables, puballsequences, pubinsert, " \
	"   pubupdate, pubdelete, pubtruncate, pubviaroot, pubgencols, pubowner) " \
	"   FROM pg_publication WHERE pubname = 'cfc2_ep'), " \
	" (SELECT string_agg(format('%s/%s/%s/%s', prrelid::regclass, prexcept, " \
	"   pg_get_expr(prqual, prrelid), prattrs), ',' ORDER BY prrelid::regclass::text) " \
	"   FROM pg_publication_rel r JOIN pg_publication p ON p.oid = r.prpubid " \
	"   WHERE p.pubname = 'cfc2_ep'), " \
	" (SELECT string_agg(pnnspid::regnamespace::text, ',' ORDER BY pnnspid::regnamespace::text) " \
	"   FROM pg_publication_namespace n JOIN pg_publication p ON p.oid = n.pnpubid " \
	"   WHERE p.pubname = 'cfc2_ep'))"

static const char *ep_cleanup =
"DROP EVENT TRIGGER IF EXISTS cfc2_evt; DROP FUNCTION IF EXISTS cfc2_etf(); "
"DROP PUBLICATION IF EXISTS cfc2_ep; DROP TABLE IF EXISTS cfc2_etlog, cfc2_et0, cfc2_et1, cfc2_et2; "
"DROP SCHEMA IF EXISTS cfc2_es0, cfc2_es1 CASCADE";

/*
 * pg_event_trigger_ddl_commands() at ddl_command_end lists the commands the
 * statement executed.  So an ALTER PUBLICATION that succeeds and changes the
 * publication's catalog state (its flags, its table list with row filters and
 * column lists, its schema list) must be reported at least once.  A random
 * starting publication and a random sequence of ALTER PUBLICATION forms (ADD,
 * DROP, SET of tables / schemas, SET (publish = ...), SET ALL TABLES [EXCEPT],
 * SET ALL SEQUENCES) are applied; statements may fail legitimately.
 */
PG_PROP(alter_publication_reported_to_event_triggers)
{
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	int			nsteps = (int) pg_prop_draw_int(tc, 1, 8);

	initStringInfo(&s);
	c2_exec(tc, conn, "SET client_min_messages = error");
	c2_exec(tc, conn, ep_cleanup);
	c2_exec(tc, conn,
			"CREATE TABLE cfc2_et0 (a int, b int); CREATE TABLE cfc2_et1 (a int, b int); "
			"CREATE TABLE cfc2_et2 (a int, b int); "
			"CREATE SCHEMA cfc2_es0; CREATE TABLE cfc2_es0.t (a int); "
			"CREATE SCHEMA cfc2_es1; CREATE TABLE cfc2_es1.t (a int); "
			"CREATE TABLE cfc2_etlog (tag text, otype text, ident text); "
			"CREATE FUNCTION cfc2_etf() RETURNS event_trigger LANGUAGE plpgsql AS $$ "
			"DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP "
			"INSERT INTO cfc2_etlog VALUES (r.command_tag, r.object_type, r.object_identity); "
			"END LOOP; END $$; "
			"CREATE EVENT TRIGGER cfc2_evt ON ddl_command_end WHEN TAG IN ('ALTER PUBLICATION') "
			"EXECUTE FUNCTION cfc2_etf()");

	switch ((int) pg_prop_draw_int(tc, 0, 3))
	{
		case 0:
			c2_exec(tc, conn, "CREATE PUBLICATION cfc2_ep");
			break;
		case 1:
			c2_exec(tc, conn, "CREATE PUBLICATION cfc2_ep FOR TABLE cfc2_et0, cfc2_et1, cfc2_et2");
			break;
		case 2:
			c2_exec(tc, conn, "CREATE PUBLICATION cfc2_ep FOR TABLES IN SCHEMA cfc2_es0, cfc2_es1, "
					"TABLE cfc2_et0");
			break;
		default:
			c2_exec(tc, conn, "CREATE PUBLICATION cfc2_ep FOR ALL TABLES");
			break;
	}

	for (int k = 0; k < nsteps; k++)
	{
		int			t1 = (int) pg_prop_draw_int(tc, 0, 2);
		int			t2 = (int) pg_prop_draw_int(tc, 0, 2);
		int			sc = (int) pg_prop_draw_int(tc, 0, 1);
		char	   *before;
		char	   *after;
		long		reported;
		char	   *err = NULL;
		bool		ok;

		resetStringInfo(&s);
		appendStringInfoString(&s, "ALTER PUBLICATION cfc2_ep ");
		switch ((int) pg_prop_draw_int(tc, 0, 10))
		{
			case 0:
				appendStringInfo(&s, "ADD TABLE cfc2_et%d%s", t1,
								 pg_prop_draw_bool(tc) ? " WHERE (a > 0)" : "");
				break;
			case 1:
				appendStringInfo(&s, "DROP TABLE cfc2_et%d", t1);
				break;
			case 2:
				appendStringInfo(&s, "DROP TABLE cfc2_et%d, cfc2_et%d", t1, t2);
				break;
			case 3:
				appendStringInfo(&s, "SET TABLE cfc2_et%d", t1);
				break;
			case 4:
				appendStringInfo(&s, "SET TABLE cfc2_et%d (a), cfc2_et%d", t1, t2);
				break;
			case 5:
				appendStringInfo(&s, "ADD TABLES IN SCHEMA cfc2_es%d", sc);
				break;
			case 6:
				appendStringInfo(&s, "DROP TABLES IN SCHEMA cfc2_es%d", sc);
				break;
			case 7:
				appendStringInfo(&s, "SET TABLES IN SCHEMA cfc2_es%d", sc);
				break;
			case 8:
				appendStringInfo(&s, "SET (publish = '%s')",
								 pg_prop_draw_bool(tc) ? "insert" : "insert, update, delete");
				break;
			case 9:
				if (pg_prop_draw_bool(tc))
					appendStringInfo(&s, "SET ALL TABLES EXCEPT (TABLE cfc2_et%d)", t1);
				else
					appendStringInfoString(&s, "SET ALL TABLES");
				break;
			default:
				appendStringInfoString(&s, "SET ALL SEQUENCES");
				break;
		}

		c2_exec(tc, conn, "TRUNCATE cfc2_etlog");
		before = pg_prop_sql_scalar(conn, EP_STATE, NULL, NULL);
		ok = pg_prop_sql_exec(conn, s.data, &err);
		after = pg_prop_sql_scalar(conn, EP_STATE, NULL, NULL);
		reported = c2_count(tc, conn, "SELECT count(*) FROM cfc2_etlog");
		if (ok && reported == 0 &&
			strcmp(before ? before : "", after ? after : "") != 0)
			pg_prop_fail(tc,
						 "ALTER PUBLICATION changed the publication but "
						 "pg_event_trigger_ddl_commands() reported nothing\n"
						 "  SQL: %s\n  before: %s\n  after:  %s",
						 s.data, before ? before : "", after ? after : "");
		if (err)
			pg_free(err);
		if (before)
			pg_free(before);
		if (after)
			pg_free(after);
	}

	c2_exec(tc, conn, ep_cleanup);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 7210: a publication's table list is order-independent
 * ----------------------------------------------------------------------
 */

static const char *pl_cleanup =
"DROP PUBLICATION IF EXISTS cfc2_pl0, cfc2_pl1; "
"DROP TABLE IF EXISTS cfc2_pt0, cfc2_pt1, cfc2_pt2, cfc2_pt3, cfc2_pt4 CASCADE";

/*
 * A publication's table list is a set of mentions; "t" adds t and its
 * descendants, "ONLY t" adds t alone.  If the server accepts a list, the
 * tables it publishes cannot depend on the order of the mentions.  The same
 * drawn mention list over a drawn inheritance / partition forest is given in
 * two orders (via CREATE PUBLICATION or ALTER PUBLICATION SET TABLE); when
 * both are accepted, the published-table sets must match.
 *
 * Accept-vs-reject is deliberately not compared: the CF 7210 patch rejects
 * "TABLE parent, TABLE child" ("conflicting ONLY specifications") yet accepts
 * "TABLE child, TABLE parent", because a child added by a parent's recursion
 * is in relids but not in relids_recursing.  That is arguably a patch bug,
 * but this property targets the silent first-mention-wins bug.
 */
PG_PROP(publication_table_list_order_independent)
{
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	int			ntab = (int) pg_prop_draw_int(tc, 2, 5);
	int			nment = (int) pg_prop_draw_int(tc, 1, 5);
	int			ment[5];
	int			only[5];
	int			perm[5];
	bool		ok[2];
	char	   *set[2];
	char	   *stmt[2];
	char	   *err[2] = {NULL, NULL};

	initStringInfo(&s);
	c2_exec(tc, conn, "SET client_min_messages = error");
	c2_exec(tc, conn, pl_cleanup);

	/* table 0 is a root; table i > 0 inherits from (or partitions) an earlier one */
	if (pg_prop_draw_int(tc, 0, 3) == 0)
	{
		appendStringInfoString(&s, "CREATE TABLE cfc2_pt0 (a int) PARTITION BY LIST (a); ");
		for (int i = 1; i < ntab; i++)
			appendStringInfo(&s, "CREATE TABLE cfc2_pt%d PARTITION OF cfc2_pt0 FOR VALUES IN (%d); ",
							 i, i);
	}
	else
	{
		appendStringInfoString(&s, "CREATE TABLE cfc2_pt0 (a int); ");
		for (int i = 1; i < ntab; i++)
		{
			int			par = (int) pg_prop_draw_int(tc, -1, i - 1);

			if (par < 0)
				appendStringInfo(&s, "CREATE TABLE cfc2_pt%d (a int); ", i);
			else
				appendStringInfo(&s, "CREATE TABLE cfc2_pt%d () INHERITS (cfc2_pt%d); ", i, par);
		}
	}
	c2_exec(tc, conn, s.data);

	for (int m = 0; m < nment; m++)
	{
		ment[m] = (int) pg_prop_draw_int(tc, 0, ntab - 1);
		only[m] = (int) pg_prop_draw_int(tc, 0, 2);	/* 0 plain, 1 ONLY, 2 "*" */
		perm[m] = m;
	}
	/* a drawn permutation (Fisher-Yates); default reverse when it shrinks */
	for (int m = nment - 1; m > 0; m--)
	{
		int			j = (int) pg_prop_draw_int(tc, 0, m);
		int			t = perm[m];

		perm[m] = perm[j];
		perm[j] = t;
	}

	for (int p = 0; p < 2; p++)
	{
		bool		via_alter = pg_prop_draw_bool(tc);

		resetStringInfo(&s);
		if (via_alter)
			appendStringInfo(&s, "CREATE PUBLICATION cfc2_pl%d; ALTER PUBLICATION cfc2_pl%d SET ", p, p);
		else
			appendStringInfo(&s, "CREATE PUBLICATION cfc2_pl%d FOR ", p);
		for (int m = 0; m < nment; m++)
		{
			int			x = p == 0 ? m : perm[m];

			appendStringInfo(&s, "%sTABLE %scfc2_pt%d%s", m ? ", " : "",
							 only[x] == 1 ? "ONLY " : "", ment[x],
							 only[x] == 2 ? " *" : "");
		}
		stmt[p] = pg_strdup(s.data);
		/* the two-statement string is one transaction: a failure leaves no pub */
		ok[p] = pg_prop_sql_exec(conn, s.data, &err[p]);
		resetStringInfo(&s);
		appendStringInfo(&s,
						 "SELECT coalesce(string_agg(prrelid::regclass::text, ',' "
						 "ORDER BY prrelid::regclass::text), '') FROM pg_publication_rel r "
						 "JOIN pg_publication p ON p.oid = r.prpubid WHERE p.pubname = 'cfc2_pl%d'", p);
		set[p] = pg_prop_sql_scalar(conn, s.data, NULL, NULL);
	}

	if (ok[0] && ok[1] && strcmp(set[0] ? set[0] : "", set[1] ? set[1] : "") != 0)
		pg_prop_fail(tc,
					 "the same publication table list in two orders gave different results\n"
					 "  A: %s\n     %s, tables {%s}%s%s\n  B: %s\n     %s, tables {%s}%s%s",
					 stmt[0], ok[0] ? "accepted" : "rejected", set[0] ? set[0] : "",
					 err[0] ? " " : "", err[0] ? err[0] : "",
					 stmt[1], ok[1] ? "accepted" : "rejected", set[1] ? set[1] : "",
					 err[1] ? " " : "", err[1] ? err[1] : "");

	c2_exec(tc, conn, pl_cleanup);
	pg_free(stmt[0]);
	pg_free(stmt[1]);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 7241: temporal FK validation keeps PERIOD semantics on every path
 * ----------------------------------------------------------------------
 */

static const char *tf_cleanup =
"DROP TABLE IF EXISTS cfc2_tf, cfc2_tf1, cfc2_tp CASCADE";

/* rows of cfc2_tf1 that violate FOREIGN KEY (parent_id, PERIOD valid_at) */
#define TF_VIOLATIONS \
	"SELECT count(*) FROM cfc2_tf1 c WHERE c.parent_id IS NOT NULL AND c.valid_at IS NOT NULL " \
	"AND NOT coalesce((SELECT range_agg(p.valid_at) FROM cfc2_tp p WHERE p.id = c.parent_id) " \
	"  @> c.valid_at, false)"

/*
 * A temporal foreign key (parent_id, PERIOD valid_at) holds for a row iff the
 * row's valid_at is covered by the union of the referenced rows' ranges.
 * However the constraint comes to be checked against existing rows -- ADD
 * (validated), ADD NOT VALID + VALIDATE, ADD NOT ENFORCED + ALTER ENFORCED,
 * ATTACH PARTITION of a populated table (cloned or reparented constraint) --
 * the server accepts iff no row violates that, per a SQL model with
 * range_agg.  Parent rows are drawn as runs of adjacent or gapped ranges so
 * that coverage often needs several parent rows.
 */
PG_PROP(temporal_fk_validation_matches_model)
{
	static const char *const pathname[] = {
		"ADD CONSTRAINT", "ADD NOT VALID + VALIDATE", "ADD NOT ENFORCED + ALTER ENFORCED",
		"ATTACH PARTITION (cloned FK)", "ATTACH PARTITION (reparented NOT VALID FK)",
	};
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	int			path = (int) pg_prop_draw_int(tc, 0, 4);
	int			nchild = (int) pg_prop_draw_int(tc, 1, 4);
	long		viol;
	char	   *err = NULL;
	bool		ok;
	const char *fk = "FOREIGN KEY (parent_id, PERIOD valid_at) REFERENCES cfc2_tp (id, PERIOD valid_at)";

	initStringInfo(&s);
	c2_exec(tc, conn, tf_cleanup);
	c2_exec(tc, conn,
			"CREATE TABLE cfc2_tp (id int4range, valid_at int4range, "
			"CONSTRAINT cfc2_tp_pk PRIMARY KEY (id, valid_at WITHOUT OVERLAPS))");

	/* parents: per id 0..2, a run of non-overlapping ranges */
	for (int id = 0; id < 3; id++)
	{
		int			n = (int) pg_prop_draw_int(tc, 0, 3);
		int			pos = (int) pg_prop_draw_int(tc, 0, 4);

		for (int k = 0; k < n; k++)
		{
			int			len = (int) pg_prop_draw_int(tc, 1, 5);

			resetStringInfo(&s);
			appendStringInfo(&s, "INSERT INTO cfc2_tp VALUES ('[%d,%d)', '[%d,%d)')",
							 id, id + 1, pos, pos + len);
			c2_exec(tc, conn, s.data);
			pos += len + (int) pg_prop_draw_int(tc, 0, 2);	/* 0 = adjacent */
		}
	}

	/* the referencing table that will hold the rows being validated */
	if (path >= 3)
		c2_exec(tc, conn,
				"CREATE TABLE cfc2_tf (p int, parent_id int4range, valid_at int4range, "
				"CONSTRAINT cfc2_tf_fk FOREIGN KEY (parent_id, PERIOD valid_at) "
				"REFERENCES cfc2_tp (id, PERIOD valid_at)) PARTITION BY LIST (p); "
				"CREATE TABLE cfc2_tf1 (LIKE cfc2_tf)");
	else
		c2_exec(tc, conn, "CREATE TABLE cfc2_tf1 (p int, parent_id int4range, valid_at int4range)");
	for (int r = 0; r < nchild; r++)
	{
		int			pid = (int) pg_prop_draw_int(tc, -1, 3);	/* -1 NULL, 3 no parent */
		int			lo = (int) pg_prop_draw_int(tc, 0, 14);
		int			len = (int) pg_prop_draw_int(tc, 1, 6);

		resetStringInfo(&s);
		if (pid < 0)
			appendStringInfo(&s, "INSERT INTO cfc2_tf1 VALUES (1, NULL, '[%d,%d)')", lo, lo + len);
		else
			appendStringInfo(&s, "INSERT INTO cfc2_tf1 VALUES (1, '[%d,%d)', '[%d,%d)')",
							 pid, pid + 1, lo, lo + len);
		c2_exec(tc, conn, s.data);
	}
	viol = c2_count(tc, conn, TF_VIOLATIONS);

	resetStringInfo(&s);
	switch (path)
	{
		case 0:
			appendStringInfo(&s, "ALTER TABLE cfc2_tf1 ADD CONSTRAINT cfc2_tf_fk %s", fk);
			break;
		case 1:
			c2_exec(tc, conn, psprintf("ALTER TABLE cfc2_tf1 ADD CONSTRAINT cfc2_tf_fk %s NOT VALID", fk));
			appendStringInfoString(&s, "ALTER TABLE cfc2_tf1 VALIDATE CONSTRAINT cfc2_tf_fk");
			break;
		case 2:
			c2_exec(tc, conn, psprintf("ALTER TABLE cfc2_tf1 ADD CONSTRAINT cfc2_tf_fk %s NOT ENFORCED", fk));
			appendStringInfoString(&s, "ALTER TABLE cfc2_tf1 ALTER CONSTRAINT cfc2_tf_fk ENFORCED");
			break;
		default:
			/* path 4: an equivalent NOT VALID FK already on the partition */
			if (path == 4)
				c2_exec(tc, conn, psprintf("ALTER TABLE cfc2_tf1 ADD CONSTRAINT cfc2_tf_fk %s NOT VALID", fk));
			appendStringInfoString(&s, "ALTER TABLE cfc2_tf ATTACH PARTITION cfc2_tf1 FOR VALUES IN (1)");
			break;
	}
	ok = pg_prop_sql_exec(conn, s.data, &err);
	if (ok != (viol == 0))
	{
		char	   *parents = c2_rows(tc, conn, "SELECT id, valid_at FROM cfc2_tp ORDER BY 1, 2");
		char	   *rows = c2_rows(tc, conn, "SELECT parent_id, valid_at FROM cfc2_tf1 ORDER BY 1, 2");

		pg_prop_fail(tc,
					 "temporal FK via %s was %s, but %ld existing rows violate it\n"
					 "  SQL: %s%s%s\n  referenced (id, valid_at):\n%s  referencing (parent_id, valid_at):\n%s",
					 pathname[path], ok ? "accepted" : "rejected", viol, s.data,
					 err ? "\n  error: " : "", err ? err : "", parents, rows);
	}

	c2_exec(tc, conn, tf_cleanup);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 7312: rolled-back SET TABLESPACE leaves indexes consistent
 * ----------------------------------------------------------------------
 */

/*
 * The index-scan answer must equal the seqscan answer for each probe range.
 * Values: committed rows are 1..n, rows written inside the transaction under
 * test are >= 100000, rows written after it are negative, so a stale index
 * entry left behind by a rollback is visible as a wrong count.
 */
static void
ts_check_indexes(pg_prop_tc *tc, pg_prop_conn *conn, const char *log)
{
	static const char *const probes[] = {
		"a < 0", "a >= 100000", "a BETWEEN 1 AND 99999", "a > -1000000000",
	};

	for (int p = 0; p < (int) lengthof(probes); p++)
	{
		char		q[512];
		char	   *seq;
		char	   *idx;
		bool		failed = false;
		char	   *err = NULL;

		snprintf(q, sizeof(q),
				 "SET enable_seqscan = off; SET enable_bitmapscan = %s; SET enable_indexscan = on; "
				 "SET enable_indexonlyscan = %s",
				 p % 2 ? "on" : "off", p < 2 ? "on" : "off");
		c2_exec(tc, conn, q);
		snprintf(q, sizeof(q), "SELECT count(*) || '/' || coalesce(sum(a), 0) FROM cfc2_tb WHERE %s",
				 probes[p]);
		idx = pg_prop_sql_scalar(conn, q, &failed, &err);
		if (failed)
			pg_prop_fail(tc, "index scan failed after rolled-back SET TABLESPACE: %s"
						 "  probe: %s\n  sequence:\n%s", err ? err : "", probes[p], log);
		c2_exec(tc, conn, "RESET enable_seqscan; RESET enable_bitmapscan; RESET enable_indexscan; "
				"RESET enable_indexonlyscan; SET enable_indexscan = off; SET enable_bitmapscan = off; "
				"SET enable_indexonlyscan = off");
		seq = pg_prop_sql_scalar(conn, q, &failed, &err);
		c2_exec(tc, conn, "RESET enable_indexscan; RESET enable_bitmapscan; RESET enable_indexonlyscan");
		if (failed)
			pg_prop_fail(tc, "seqscan failed: %s", err ? err : "");
		if (strcmp(idx ? idx : "", seq ? seq : "") != 0)
			pg_prop_fail(tc,
						 "index scan and seqscan disagree after SET TABLESPACE in a "
						 "rolled-back (sub)transaction\n  probe %s: index %s, seqscan %s"
						 "  (count/sum)\n  sequence:\n%s",
						 probes[p], idx ? idx : "", seq ? seq : "", log);
		pg_free(idx);
		pg_free(seq);
	}
}

/*
 * A random transaction over a table with indexes: SET TABLESPACE moves
 * (there and back), INSERT/DELETE/TRUNCATE, CREATE INDEX / REINDEX,
 * SAVEPOINT / ROLLBACK TO / RELEASE, ending in COMMIT or ROLLBACK; then more
 * rows are inserted (reusing freed TIDs).  Rolling back must undo the index
 * changes together with the heap's, so afterwards index scans and the
 * seqscan agree on every probe and nothing errors.
 */
PG_PROP(rollback_set_tablespace_keeps_indexes_consistent)
{
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	StringInfoData log;
	int			n0 = (int) pg_prop_draw_int(tc, 1, 400);
	int			nops = (int) pg_prop_draw_int(tc, 1, 6);
	int			nsp = 0;
	bool		in_ts = false;
	int			next_hi = 100000;

	initStringInfo(&s);
	initStringInfo(&log);
	c2_exec(tc, conn, "DROP TABLE IF EXISTS cfc2_tb");
	c2_try(conn, "DROP TABLESPACE IF EXISTS cfc2_ts");
	c2_exec(tc, conn, "SET allow_in_place_tablespaces = on");
	c2_exec(tc, conn, "CREATE TABLESPACE cfc2_ts LOCATION ''");

	appendStringInfo(&s,
					 "CREATE TABLE cfc2_tb (a int, b text); "
					 "INSERT INTO cfc2_tb SELECT g, 'x' || g FROM generate_series(1, %d) g; "
					 "CREATE INDEX cfc2_tb_a ON cfc2_tb %s (a)",
					 n0, pg_prop_draw_int(tc, 0, 3) == 0 ? "USING hash" : "");
	if (pg_prop_draw_bool(tc))
		appendStringInfoString(&s, "; CREATE INDEX cfc2_tb_ab ON cfc2_tb (a, b)");
	c2_exec(tc, conn, s.data);
	appendStringInfo(&log, "%s;\nBEGIN;\n", s.data);
	c2_exec(tc, conn, "BEGIN");

	for (int k = 0; k < nops; k++)
	{
		int			op = (int) pg_prop_draw_int(tc, 0, 9);

		resetStringInfo(&s);
		switch (op)
		{
			case 0:
			case 1:
			case 2:
				appendStringInfo(&s, "ALTER TABLE cfc2_tb SET TABLESPACE %s",
								 in_ts ? "pg_default" : "cfc2_ts");
				in_ts = !in_ts;
				break;
			case 3:
			case 4:
				{
					int			m = (int) pg_prop_draw_int(tc, 1, 1500);

					appendStringInfo(&s, "INSERT INTO cfc2_tb SELECT g, 'y' FROM generate_series(%d, %d) g",
									 next_hi, next_hi + m - 1);
					next_hi += m;
				}
				break;
			case 5:
				appendStringInfo(&s, "DELETE FROM cfc2_tb WHERE a %% %d = 0",
								 (int) pg_prop_draw_int(tc, 2, 5));
				break;
			case 6:
				appendStringInfoString(&s, pg_prop_draw_bool(tc) ? "TRUNCATE cfc2_tb"
									   : "REINDEX INDEX cfc2_tb_a");
				break;
			case 7:
				appendStringInfo(&s, "SAVEPOINT s%d", nsp++);
				break;
			case 8:
				if (nsp > 0)
					appendStringInfo(&s, "ROLLBACK TO SAVEPOINT s%d", nsp - 1);
				else
					appendStringInfo(&s, "SAVEPOINT s%d", nsp++);
				break;
			default:
				appendStringInfoString(&s, "CREATE INDEX ON cfc2_tb (a)");
				break;
		}
		/*
		 * in_ts is only a generator hint (a rollback may undo a move); a
		 * "move" to the current tablespace is a legal no-op.
		 */
		appendStringInfo(&log, "%s;\n", s.data);
		c2_exec(tc, conn, s.data);
	}
	resetStringInfo(&s);
	appendStringInfoString(&s, pg_prop_draw_int(tc, 0, 3) == 0 ? "COMMIT" : "ROLLBACK");
	appendStringInfo(&log, "%s;\n", s.data);
	c2_exec(tc, conn, s.data);

	resetStringInfo(&s);
	appendStringInfo(&s, "INSERT INTO cfc2_tb SELECT -g, 'z' FROM generate_series(1, %d) g",
					 (int) pg_prop_draw_int(tc, 1, 600));
	appendStringInfo(&log, "%s;\n", s.data);
	c2_exec(tc, conn, s.data);

	ts_check_indexes(tc, conn, log.data);

	c2_exec(tc, conn, "DROP TABLE cfc2_tb");
	c2_exec(tc, conn, "DROP TABLESPACE cfc2_ts");
	pfree(s.data);
	pfree(log.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 7321: dropping the session's temp schema leaves a consistent catalog
 * ----------------------------------------------------------------------
 */

/* catalog rows whose namespace does not exist */
#define TN_ORPHANS \
	"SELECT (SELECT count(*) FROM pg_class c WHERE NOT EXISTS " \
	"  (SELECT 1 FROM pg_namespace n WHERE n.oid = c.relnamespace)) + " \
	"(SELECT count(*) FROM pg_type t WHERE NOT EXISTS " \
	"  (SELECT 1 FROM pg_namespace n WHERE n.oid = t.typnamespace)) + " \
	"(SELECT count(*) FROM pg_proc p WHERE NOT EXISTS " \
	"  (SELECT 1 FROM pg_namespace n WHERE n.oid = p.pronamespace))"

/*
 * A superuser may DROP SCHEMA pg_temp_N CASCADE on its own temp schema.
 * Whatever temp objects existed, and wherever the drop happens (same or
 * earlier transaction, inside a savepoint that is or is not rolled back),
 * temp objects created afterwards must live in a schema that exists: no
 * pg_class / pg_type / pg_proc row may reference a missing namespace, and
 * pg_my_temp_schema(), when set, must name an existing schema.
 *
 * (pg_identify_object() on such an orphan also crashes the server; that
 * probe is deliberately not made here because the server is shared.)
 */
PG_PROP(drop_temp_schema_keeps_catalog_consistent)
{
	static const char *const mk[] = {
		"CREATE TEMP TABLE cfc2_tt%d (f1 int)",
		"CREATE TEMP VIEW cfc2_tt%d AS SELECT 1 AS x",
		"CREATE TEMP SEQUENCE cfc2_tt%d",
		"CREATE FUNCTION pg_temp.cfc2_tt%d() RETURNS int LANGUAGE sql AS 'SELECT 1'",
		"CREATE TEMP TABLE cfc2_tt%d (f1 serial PRIMARY KEY, f2 text)",
	};
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	StringInfoData log;
	int			nobj = 0;
	int			nsteps = (int) pg_prop_draw_int(tc, 1, 7);
	bool		in_txn = false;
	int			nsp = 0;
	long		orph;
	long		bad_temp;

	initStringInfo(&s);
	initStringInfo(&log);

	for (int k = 0; k < nsteps; k++)
	{
		resetStringInfo(&s);
		switch ((int) pg_prop_draw_int(tc, 0, 6))
		{
			case 0:
			case 1:
				appendStringInfo(&s, mk[pg_prop_draw_int(tc, 0, lengthof(mk) - 1)], nobj++);
				break;
			case 2:
			case 3:
				/* drop my own temp schema, if I have one */
				appendStringInfoString(&s,
									   "DO $$ BEGIN IF pg_my_temp_schema() <> 0 AND EXISTS (SELECT 1 FROM "
									   "pg_namespace WHERE oid = pg_my_temp_schema()) THEN EXECUTE format("
									   "'DROP SCHEMA %I CASCADE', pg_my_temp_schema()::regnamespace); "
									   "END IF; END $$");
				break;
			case 4:
				appendStringInfoString(&s, in_txn ? "COMMIT" : "BEGIN");
				in_txn = !in_txn;
				nsp = 0;
				break;
			case 5:
				if (in_txn)
					appendStringInfo(&s, "SAVEPOINT s%d", nsp++);
				else
					appendStringInfo(&s, mk[0], nobj++);
				break;
			default:
				if (in_txn && nsp > 0)
					appendStringInfo(&s, "ROLLBACK TO SAVEPOINT s%d", nsp - 1);
				else if (in_txn)
				{
					appendStringInfoString(&s, "ROLLBACK");
					in_txn = false;
				}
				else
					appendStringInfo(&s, mk[pg_prop_draw_int(tc, 0, lengthof(mk) - 1)], nobj++);
				break;
		}
		appendStringInfo(&log, "%s;\n", s.data);
		c2_exec(tc, conn, s.data);
	}
	/* always create one more temp object at the end */
	resetStringInfo(&s);
	appendStringInfo(&s, mk[pg_prop_draw_int(tc, 0, lengthof(mk) - 1)], nobj++);
	appendStringInfo(&log, "%s;\n", s.data);
	c2_exec(tc, conn, s.data);

	orph = c2_count(tc, conn, TN_ORPHANS);
	bad_temp = c2_count(tc, conn,
						"SELECT count(*) FROM (SELECT pg_my_temp_schema() AS t) x "
						"WHERE t <> 0 AND NOT EXISTS (SELECT 1 FROM pg_namespace WHERE oid = t)");
	if (orph || bad_temp)
		pg_prop_fail(tc,
					 "after dropping the temp schema: %ld catalog rows reference a missing "
					 "namespace; pg_my_temp_schema() %s\n  sequence:\n%s",
					 orph, bad_temp ? "names a dropped schema" : "ok", log.data);

	if (in_txn)
		c2_exec(tc, conn, "COMMIT");
	pfree(s.data);
	pfree(log.data);
	/* the temp schema's contents go away with the session */
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 7351: a partition whose DETACH CONCURRENTLY was interrupted
 * ----------------------------------------------------------------------
 */

static const char *dp_cleanup =
"DROP PUBLICATION IF EXISTS cfc2_dpa, cfc2_dpr, cfc2_dpt; "
"DROP TABLE IF EXISTS cfc2_dp, cfc2_dp0, cfc2_dp1, cfc2_dp2, cfc2_dp1_l0, cfc2_dp1_l1 CASCADE";

/* UPDATE probe: "ok" or the SQLSTATE-free error text */
static char *
dp_update_probe(pg_prop_conn *conn, const char *rel)
{
	char		q[256];
	char	   *err = NULL;

	snprintf(q, sizeof(q), "UPDATE %s SET b = b WHERE false", rel);
	if (pg_prop_sql_exec(conn, q, &err))
		return pg_strdup("ok");
	return err ? err : pg_strdup("error");
}

/*
 * Leave a drawn partition (leaf, or a partitioned partition with its own
 * leaves, possibly with a different column order) pending detach by letting
 * DETACH PARTITION ... CONCURRENTLY time out while another session holds the
 * parent open.  The partition is then "detached" for every purpose except
 * that FINALIZE has not run.  Invariants:
 *
 *  - once the detach's first transaction has committed, a session that
 *    looked at the partition before it and a fresh session agree on whether
 *    UPDATE of the partition is allowed (its publication membership);
 *  - a FOR ALL TABLES publication publishes every table: each cfc2_dp leaf
 *    is in pg_get_publication_tables() itself or through one of its current
 *    pg_partition_ancestors() (publish_via_partition_root on or off);
 *  - with PG_PROP_CF_CRASH=1 only (it takes the shared server down on an
 *    unfixed build): INSERT into the partition computing an identity
 *    default does not crash.
 */
PG_PROP(pending_detach_partition_consistent)
{
	pg_prop_conn *conn = c2_connect(tc);
	pg_prop_conn *other = c2_connect(tc);
	pg_prop_conn *fresh;
	StringInfoData s;
	int			nparts = (int) pg_prop_draw_int(tc, 2, 3);
	int			victim = (int) pg_prop_draw_int(tc, 0, nparts - 1);
	bool		victim_parted = pg_prop_draw_bool(tc);
	bool		permuted = pg_prop_draw_bool(tc);
	bool		identity = pg_prop_draw_bool(tc);
	bool		via_root = pg_prop_draw_bool(tc);
	bool		pub_root = pg_prop_draw_bool(tc);
	bool		all_tables = pg_prop_draw_bool(tc);
	const char *crash = getenv("PG_PROP_CF_CRASH");
	char	   *err = NULL;
	char		victim_name[32];
	char		probe_rel[32];
	char	   *before_other;
	char	   *after_other;
	char	   *after_fresh;
	char	   *uncovered;

	initStringInfo(&s);
	c2_exec(tc, conn, dp_cleanup);
	appendStringInfo(&s, "CREATE TABLE cfc2_dp (a int, b int%s) PARTITION BY LIST (a); ",
					 identity ? ", c bigint GENERATED BY DEFAULT AS IDENTITY" : "");
	for (int p = 0; p < nparts; p++)
	{
		if (p == victim && victim_parted)
		{
			appendStringInfo(&s,
							 "CREATE TABLE cfc2_dp%d PARTITION OF cfc2_dp FOR VALUES IN (%d) "
							 "PARTITION BY LIST (b); "
							 "CREATE TABLE cfc2_dp%d_l0 PARTITION OF cfc2_dp%d FOR VALUES IN (0); ",
							 p, p, p, p);
			if (permuted)
				appendStringInfo(&s,
								 "CREATE TABLE cfc2_dp%d_l1 (%sb int, a int); "
								 "ALTER TABLE cfc2_dp%d ATTACH PARTITION cfc2_dp%d_l1 DEFAULT; ",
								 p, identity ? "c bigint NOT NULL, " : "", p, p);
			else
				appendStringInfo(&s, "CREATE TABLE cfc2_dp%d_l1 PARTITION OF cfc2_dp%d DEFAULT; ",
								 p, p);
		}
		else if (permuted && p == victim)
			appendStringInfo(&s,
							 "CREATE TABLE cfc2_dp%d (%sb int, a int); "
							 "ALTER TABLE cfc2_dp ATTACH PARTITION cfc2_dp%d FOR VALUES IN (%d); ",
							 p, identity ? "c bigint NOT NULL, " : "", p, p);
		else
			appendStringInfo(&s, "CREATE TABLE cfc2_dp%d PARTITION OF cfc2_dp FOR VALUES IN (%d); ",
							 p, p);
	}
	appendStringInfo(&s, "INSERT INTO cfc2_dp (a, b) SELECT g %% %d, g %% 2 FROM generate_series(0, 9) g; ",
					 nparts);
	/* FOR ALL TABLES makes UPDATE without replica identity fail everywhere */
	if (all_tables)
		appendStringInfo(&s, "CREATE PUBLICATION cfc2_dpa FOR ALL TABLES WITH (publish_via_partition_root = %s); ",
						 via_root ? "true" : "false");
	if (pub_root || !all_tables)
		appendStringInfo(&s, "CREATE PUBLICATION cfc2_dpr FOR TABLE cfc2_dp WITH (publish_via_partition_root = %s); ",
						 via_root ? "true" : "false");
	c2_exec(tc, conn, s.data);

	snprintf(victim_name, sizeof(victim_name), "cfc2_dp%d", victim);
	if (victim_parted)
		snprintf(probe_rel, sizeof(probe_rel), "cfc2_dp%d_l%d", victim,
				 (int) pg_prop_draw_int(tc, 0, 1));
	else
		strlcpy(probe_rel, victim_name, sizeof(probe_rel));

	/* the other session builds its relcache entry for the partition first */
	before_other = dp_update_probe(other, probe_rel);
	pg_free(before_other);

	/* interrupt the detach: the other session holds the parent open */
	c2_exec(tc, other, "BEGIN; SELECT count(*) FROM cfc2_dp");
	c2_exec(tc, conn, "SET lock_timeout = '150ms'");
	resetStringInfo(&s);
	appendStringInfo(&s, "ALTER TABLE cfc2_dp DETACH PARTITION %s CONCURRENTLY", victim_name);
	if (pg_prop_sql_exec(conn, s.data, &err))
	{
		/* it did not wait (nothing to wait for): no pending state to test */
		c2_exec(tc, other, "COMMIT");
		c2_exec(tc, conn, "RESET lock_timeout");
		c2_exec(tc, conn, dp_cleanup);
		pg_prop_disconnect(other);
		pg_prop_disconnect(conn);
		pg_prop_assume(tc, false);
	}
	if (err)
		pg_free(err);
	c2_exec(tc, conn, "RESET lock_timeout");
	c2_exec(tc, other, "COMMIT");

	/*
	 * The timeout may also hit the first phase's lock wait (say, autovacuum
	 * on the partition), leaving nothing pending: discard such a case.
	 */
	if (c2_count(tc, conn,
				 "SELECT count(*) FROM pg_inherits WHERE inhdetachpending AND "
				 "inhparent = 'cfc2_dp'::regclass") != 1)
	{
		c2_exec(tc, conn, dp_cleanup);
		pg_prop_disconnect(other);
		pg_prop_disconnect(conn);
		pg_prop_assume(tc, false);
	}

	/* 2. a session with a pre-detach relcache entry agrees with a fresh one */
	fresh = c2_connect(tc);
	after_other = dp_update_probe(other, probe_rel);
	after_fresh = dp_update_probe(fresh, probe_rel);
	if ((strcmp(after_other, "ok") == 0) != (strcmp(after_fresh, "ok") == 0))
		pg_prop_fail(tc,
					 "after DETACH CONCURRENTLY of %s was interrupted, two sessions disagree on "
					 "UPDATE %s (publications: %s%s, via_root=%s)\n"
					 "  session that saw it before: %s\n  fresh session: %s",
					 victim_name, probe_rel, all_tables ? "ALL TABLES" : "",
					 (pub_root || !all_tables) ? " FOR TABLE root" : "",
					 via_root ? "true" : "false", after_other, after_fresh);
	pg_free(after_other);
	pg_free(after_fresh);
	pg_prop_disconnect(fresh);

	/* 1. FOR ALL TABLES covers every leaf */
	uncovered = !all_tables ? NULL : pg_prop_sql_scalar(conn,
								   "SELECT string_agg(c.relname, ',') FROM pg_class c "
								   "WHERE c.relname LIKE 'cfc2_dp%' AND c.relkind = 'r' "
								   "AND NOT EXISTS (SELECT 1 FROM pg_get_publication_tables('cfc2_dpa') g "
								   "  WHERE g.relid = c.oid OR g.relid IN "
								   "  (SELECT relid FROM pg_partition_ancestors(c.oid)))",
								   NULL, NULL);
	if (uncovered != NULL)
		pg_prop_fail(tc,
					 "FOR ALL TABLES publication (publish_via_partition_root = %s) does not "
					 "publish %s, a %s partition pending detach (or its leaf)",
					 via_root ? "true" : "false", uncovered,
					 victim_parted ? "partitioned" : "leaf");

	/* 3. identity default through the pending partition (crashes unfixed servers) */
	if (identity && crash != NULL && strcmp(crash, "1") == 0)
	{
		resetStringInfo(&s);
		appendStringInfo(&s, "INSERT INTO %s (a, b) VALUES (%d, 0)", probe_rel, victim);
		c2_try(conn, s.data);
	}

	c2_exec(tc, conn, dp_cleanup);
	pfree(s.data);
	pg_prop_disconnect(other);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 7379: CREATE TABLE ... (LIKE ...) only touches the table it creates
 * ----------------------------------------------------------------------
 */

/* everything about the permanent cfc2_l* tables a LIKE could clobber */
#define LK_REL "c.relname LIKE 'cfc2_l%' AND c.relnamespace::regnamespace::text IN ('public', 'cfc2_ls')"
#define LK_SNAPSHOT \
	"SELECT c.relnamespace::regnamespace, c.relname, d.objsubid, d.description " \
	"FROM pg_description d JOIN pg_class c ON d.classoid = 'pg_class'::regclass AND d.objoid = c.oid " \
	"WHERE " LK_REL " " \
	"UNION ALL SELECT co.connamespace::regnamespace, co.conname, 0, " \
	"  c.relname || ':' || coalesce(obj_description(co.oid, 'pg_constraint'), '-') " \
	"FROM pg_constraint co JOIN pg_class c ON c.oid = co.conrelid WHERE " LK_REL " " \
	"UNION ALL SELECT c.relnamespace::regnamespace, c.relname, c.relnatts, c.relkind::text " \
	"FROM pg_class c WHERE " LK_REL " " \
	"ORDER BY 1, 2, 3, 4"

static const char *lk_cleanup =
"DROP TABLE IF EXISTS public.cfc2_lsrc, public.cfc2_lt CASCADE; "
"DROP SCHEMA IF EXISTS cfc2_ls, cfc2_ln CASCADE";

/*
 * CREATE [TEMP] TABLE t (LIKE src INCLUDING ...) creates t and must leave
 * every other table alone, whatever search_path says and whatever
 * same-named tables exist in schemas that precede the target's.  The
 * permanent cfc2_l* tables' comments, constraints (with their comments) and
 * shape are snapshotted before and after; they must not change, and the
 * statement must not fail.  The new table carries src's comments when
 * INCLUDING COMMENTS is given.
 */
PG_PROP(create_like_only_touches_new_table)
{
	static const char *const paths[] = {
		"public, pg_temp", "pg_temp, public", "cfc2_ls, public, pg_temp",
		"public, cfc2_ls, pg_temp", "cfc2_ls, pg_temp, public",
	};
	static const char *const incl[] = {
		"INCLUDING COMMENTS", "INCLUDING ALL", "INCLUDING COMMENTS INCLUDING CONSTRAINTS",
		"INCLUDING COMMENTS INCLUDING INDEXES", "INCLUDING ALL EXCLUDING INDEXES",
	};
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	const char *path = paths[pg_prop_draw_int(tc, 0, lengthof(paths) - 1)];
	const char *inc = incl[pg_prop_draw_int(tc, 0, lengthof(incl) - 1)];
	bool		temp = pg_prop_draw_int(tc, 0, 3) != 0;
	bool		qualify = pg_prop_draw_int(tc, 0, 3) == 0;
	char	   *before;
	char	   *after;
	char	   *err = NULL;
	long		srcc;
	long		newc;

	initStringInfo(&s);
	c2_exec(tc, conn, lk_cleanup);
	c2_exec(tc, conn,
			"CREATE SCHEMA cfc2_ls; "
			"CREATE TABLE public.cfc2_lsrc (a int CONSTRAINT cfc2_lk_c CHECK (a > 0), b text); "
			"COMMENT ON COLUMN public.cfc2_lsrc.a IS 'src a'; "
			"COMMENT ON CONSTRAINT cfc2_lk_c ON public.cfc2_lsrc IS 'src check'; "
			"CREATE INDEX cfc2_lsrc_b ON public.cfc2_lsrc (b); "
			"COMMENT ON INDEX cfc2_lsrc_b IS 'src index'");
	if (pg_prop_draw_bool(tc))
		c2_exec(tc, conn, "COMMENT ON COLUMN public.cfc2_lsrc.b IS 'src b'");

	/* unrelated tables named like the target, in drawn schemas */
	for (int k = 0; k < 2; k++)
	{
		const char *sch = k == 0 ? "public" : "cfc2_ls";

		if (!pg_prop_draw_int(tc, 0, 3))
			continue;
		resetStringInfo(&s);
		appendStringInfo(&s, "CREATE TABLE %s.cfc2_lt (a int, b text, c int); ", sch);
		if (pg_prop_draw_bool(tc))
			appendStringInfo(&s, "COMMENT ON COLUMN %s.cfc2_lt.a IS 'unrelated %s a'; ", sch, sch);
		if (pg_prop_draw_bool(tc))
			appendStringInfo(&s,
							 "ALTER TABLE %s.cfc2_lt ADD CONSTRAINT cfc2_lk_c CHECK (c <> 0); "
							 "COMMENT ON CONSTRAINT cfc2_lk_c ON %s.cfc2_lt IS 'unrelated %s check'; ",
							 sch, sch, sch);
		c2_exec(tc, conn, s.data);
	}

	before = c2_rows(tc, conn, LK_SNAPSHOT);
	resetStringInfo(&s);
	if (temp)
		appendStringInfo(&s, "BEGIN; SET LOCAL search_path = %s; CREATE TEMP TABLE %scfc2_lt "
						 "(LIKE public.cfc2_lsrc %s)", path, qualify ? "pg_temp." : "", inc);
	else
		/* a permanent target goes into a fresh schema that comes first */
		appendStringInfo(&s, "BEGIN; CREATE SCHEMA cfc2_ln; SET LOCAL search_path = cfc2_ln, %s; "
						 "CREATE TABLE %scfc2_lt (LIKE public.cfc2_lsrc %s)",
						 path, qualify ? "cfc2_ln." : "", inc);
	if (!pg_prop_sql_exec(conn, s.data, &err))
	{
		pg_prop_sql_exec(conn, "ROLLBACK", NULL);
		pg_prop_fail(tc, "CREATE TABLE ... LIKE failed: %s  SQL: %s", err ? err : "", s.data);
	}
	after = c2_rows(tc, conn, LK_SNAPSHOT);
	srcc = c2_count(tc, conn,
					"SELECT count(*) FROM pg_description WHERE objoid = 'public.cfc2_lsrc'::regclass "
					"AND objsubid > 0");
	newc = c2_count(tc, conn,
					"SELECT count(*) FROM pg_description d JOIN pg_class c ON c.oid = d.objoid "
					"WHERE c.relname = 'cfc2_lt' AND d.objsubid > 0 AND "
					"(c.relpersistence = 't' OR c.relnamespace::regnamespace::text = 'cfc2_ln')");
	c2_exec(tc, conn, "ROLLBACK");

	/* the snapshot covers public and cfc2_ls only, never the new table */
	if (strcmp(before, after) != 0)
		pg_prop_fail(tc,
					 "%s with search_path = %s changed other tables\n"
					 "  before:\n%s  after:\n%s", s.data, path, before, after);
	if (newc != srcc)
		pg_prop_fail(tc,
					 "%s: new table has %ld column comments, source has %ld",
					 s.data, newc, srcc);

	c2_exec(tc, conn, lk_cleanup);
	pg_free(before);
	pg_free(after);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 7403: ON CONFLICT ... WHERE never sees rows RLS hides
 * ----------------------------------------------------------------------
 */

static const char *rl_cleanup =
"RESET ROLE; DROP TABLE IF EXISTS cfc2_doc; DROP FUNCTION IF EXISTS cfc2_leak(text); "
"DROP SEQUENCE IF EXISTS cfc2_leakseq; DROP ROLE IF EXISTS cfc2_rb";

/*
 * A user function called from a query run as a role subject to RLS must
 * never receive a value from a row that the role's policies hide from it.
 * The leak function bumps a sequence (non-transactional, so it survives the
 * statement's error) whenever it sees a secret of a row the role cannot
 * see.  Rows are drawn visible/hidden by owner; policies for SELECT /
 * UPDATE / INSERT are drawn (permissive or restrictive, by owner or by a
 * level column); the statement is INSERT ... ON CONFLICT (did) DO UPDATE ...
 * WHERE leak(existing.secret) or DO SELECT WHERE leak(...), on a drawn key.
 * Whether the statement succeeds or fails is not checked -- only that no
 * hidden secret reached the function.
 */
PG_PROP(on_conflict_where_respects_rls)
{
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	int			nrows = (int) pg_prop_draw_int(tc, 1, 6);
	int			key;
	bool		do_select = pg_prop_draw_bool(tc);
	bool		levels = pg_prop_draw_bool(tc);
	long		leaked;

	initStringInfo(&s);
	c2_exec(tc, conn, "SET client_min_messages = error");
	c2_exec(tc, conn, rl_cleanup);
	c2_exec(tc, conn,
			"CREATE ROLE cfc2_rb; "
			"CREATE TABLE cfc2_doc (did int PRIMARY KEY, owner text, lvl int, secret text); "
			"GRANT ALL ON cfc2_doc TO cfc2_rb; "
			"CREATE SEQUENCE cfc2_leakseq; GRANT ALL ON cfc2_leakseq TO cfc2_rb; "
			"CREATE FUNCTION cfc2_leak(text) RETURNS bool LANGUAGE plpgsql COST 0.0000001 AS "
			"$$ BEGIN IF $1 LIKE 'hidden%' THEN PERFORM nextval('cfc2_leakseq'); END IF; "
			"RETURN true; END $$; "
			"GRANT EXECUTE ON FUNCTION cfc2_leak(text) TO cfc2_rb");

	/*
	 * Row i is visible to cfc2_rb iff owner = cfc2_rb (or, with levels, lvl
	 * <= 1); its secret says which, so the leak function needs no catalog
	 * knowledge.
	 */
	for (int r = 0; r < nrows; r++)
	{
		bool		vis = pg_prop_draw_bool(tc);

		resetStringInfo(&s);
		appendStringInfo(&s, "INSERT INTO cfc2_doc VALUES (%d, '%s', %d, '%s%d')",
						 r, vis ? "cfc2_rb" : "other", vis ? 1 : 2,
						 vis ? "visible" : "hidden", r);
		c2_exec(tc, conn, s.data);
	}

	resetStringInfo(&s);
	appendStringInfoString(&s, "ALTER TABLE cfc2_doc ENABLE ROW LEVEL SECURITY; ");
	{
		const char *vis = levels ? "lvl <= 1" : "owner = current_user";

		/* SELECT visibility is always restricted: that defines "hidden" */
		appendStringInfo(&s, "CREATE POLICY cfc2_p_sel ON cfc2_doc FOR SELECT USING (%s); ", vis);
		if (pg_prop_draw_bool(tc))
			appendStringInfoString(&s, "CREATE POLICY cfc2_p_ins ON cfc2_doc FOR INSERT WITH CHECK (true); ");
		else
			appendStringInfo(&s, "CREATE POLICY cfc2_p_ins ON cfc2_doc FOR INSERT WITH CHECK (%s); ", vis);
		switch ((int) pg_prop_draw_int(tc, 0, 2))
		{
			case 0:
				appendStringInfo(&s, "CREATE POLICY cfc2_p_upd ON cfc2_doc FOR UPDATE USING (%s); ", vis);
				break;
			case 1:
				appendStringInfo(&s, "CREATE POLICY cfc2_p_upd ON cfc2_doc FOR UPDATE USING (%s) "
								 "WITH CHECK (true); ", vis);
				break;
			default:
				appendStringInfo(&s, "CREATE POLICY cfc2_p_upd ON cfc2_doc FOR UPDATE USING (true); "
								 "CREATE POLICY cfc2_p_updr ON cfc2_doc AS RESTRICTIVE FOR UPDATE "
								 "USING (%s); ", vis);
				break;
		}
	}
	c2_exec(tc, conn, s.data);

	key = (int) pg_prop_draw_int(tc, 0, nrows);	/* nrows = no conflict */
	resetStringInfo(&s);
	appendStringInfo(&s, "SET ROLE cfc2_rb; INSERT INTO cfc2_doc VALUES (%d, 'cfc2_rb', 1, 'new') "
					 "ON CONFLICT (did) ", key);
	if (do_select)
		appendStringInfoString(&s, "DO SELECT WHERE cfc2_leak(cfc2_doc.secret) RETURNING did");
	else
		appendStringInfo(&s, "DO UPDATE SET secret = %s WHERE cfc2_leak(cfc2_doc.secret)",
						 pg_prop_draw_bool(tc) ? "EXCLUDED.secret" : "cfc2_doc.secret || 'x'");
	c2_try(conn, s.data);
	c2_exec(tc, conn, "RESET ROLE");

	leaked = c2_count(tc, conn,
					  "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM cfc2_leakseq");
	if (leaked)
	{
		char	   *rows = c2_rows(tc, conn, "SELECT * FROM cfc2_doc ORDER BY did");

		pg_prop_fail(tc,
					 "a function in ON CONFLICT ... WHERE received the secret of a row hidden "
					 "by RLS (%ld time(s))\n  SQL: %s\n  table:\n%s", leaked, s.data, rows);
	}

	c2_exec(tc, conn, rl_cleanup);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 7464: a (temporal) FK only ever references a PK / UNIQUE constraint
 * ----------------------------------------------------------------------
 */

static const char *xf_cleanup =
"DROP TABLE IF EXISTS cfc2_xf, cfc2_xp CASCADE";

/*
 * A foreign key references a unique key of the referenced table: whenever
 * ADD FOREIGN KEY succeeds, its conindid is a unique index, and for a PERIOD
 * FK one backing a PRIMARY KEY / UNIQUE constraint declared WITHOUT
 * OVERLAPS.  (A plain FK may use a bare unique index, as it always could.)  The referenced table
 * gets a drawn set of keys -- PRIMARY KEY / UNIQUE with or without WITHOUT
 * OVERLAPS, EXCLUDE (id WITH =, valid_at WITH &&), a plain unique index --
 * and the FK a drawn form (PERIOD or not, explicit column list or not).
 * Rejecting the FK is always allowed.
 */
PG_PROP(fk_references_only_unique_constraints)
{
	static const char *const keys[] = {
		"CONSTRAINT cfc2_xk0 PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)",
		"CONSTRAINT cfc2_xk1 UNIQUE (id, valid_at WITHOUT OVERLAPS)",
		"CONSTRAINT cfc2_xk2 PRIMARY KEY (id, valid_at)",
		"CONSTRAINT cfc2_xk3 UNIQUE (id, valid_at)",
		"CONSTRAINT cfc2_xk4 EXCLUDE USING gist (id WITH =, valid_at WITH &&)",
		"CONSTRAINT cfc2_xk5 EXCLUDE USING gist (valid_at WITH &&, id WITH =)",
	};
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	int			nkeys = 0;
	bool		used[lengthof(keys)];
	bool		period = pg_prop_draw_int(tc, 0, 3) != 0;
	bool		collist = pg_prop_draw_bool(tc);
	bool		pk_drawn = false;
	char	   *err = NULL;
	long		bad;

	initStringInfo(&s);
	memset(used, 0, sizeof(used));
	c2_exec(tc, conn, "SET client_min_messages = error");
	if (!pg_prop_require_extension(conn, "btree_gist"))
		pg_prop_skip(tc, "btree_gist not installed");
	c2_exec(tc, conn, xf_cleanup);

	appendStringInfoString(&s, "CREATE TABLE cfc2_xp (id int4range, valid_at int4range");
	for (int k = 0; k < 3; k++)
	{
		int			i = (int) pg_prop_draw_int(tc, 0, lengthof(keys) - 1);

		if (used[i] || (i == 2 && pk_drawn) || (i == 0 && pk_drawn) || (k > 0 && !pg_prop_draw_bool(tc)))
			continue;
		used[i] = true;
		if (i == 0 || i == 2)
			pk_drawn = true;
		appendStringInfo(&s, ", %s", keys[i]);
		nkeys++;
	}
	appendStringInfoString(&s, ")");
	c2_exec(tc, conn, s.data);
	if (nkeys == 0 || pg_prop_draw_bool(tc))
		c2_exec(tc, conn, "CREATE UNIQUE INDEX cfc2_xpu ON cfc2_xp (id, valid_at)");

	/* parents: per id, a run of non-overlapping ranges */
	for (int id = 0; id < 2; id++)
	{
		int			pos = 0;
		int			n = (int) pg_prop_draw_int(tc, 1, 3);

		for (int k = 0; k < n; k++)
		{
			int			len = (int) pg_prop_draw_int(tc, 1, 4);

			resetStringInfo(&s);
			appendStringInfo(&s, "INSERT INTO cfc2_xp VALUES ('[%d,%d)', '[%d,%d)')",
							 id, id + 1, pos, pos + len);
			c2_exec(tc, conn, s.data);
			pos += len;
		}
	}
	c2_exec(tc, conn,
			"CREATE TABLE cfc2_xf (id int, parent_id int4range, valid_at int4range); "
			"INSERT INTO cfc2_xf SELECT 1, id, valid_at FROM cfc2_xp");

	resetStringInfo(&s);
	appendStringInfo(&s, "ALTER TABLE cfc2_xf ADD CONSTRAINT cfc2_xf_fk FOREIGN KEY (parent_id, %svalid_at) "
					 "REFERENCES cfc2_xp%s",
					 period ? "PERIOD " : "",
					 collist ? (period ? " (id, PERIOD valid_at)" : " (id, valid_at)") : "");
	if (!pg_prop_sql_exec(conn, s.data, &err))
	{
		/* rejecting is always allowed; the bug is accepting */
		pg_free(err);
		c2_exec(tc, conn, xf_cleanup);
		pfree(s.data);
		pg_prop_disconnect(conn);
		return;
	}

	/*
	 * A plain FK may reference any unique index (even one with no
	 * constraint); a PERIOD FK needs a WITHOUT OVERLAPS PK / UNIQUE.
	 */
	bad = c2_count(tc, conn,
				   "SELECT count(*) FROM pg_constraint f JOIN pg_index i ON i.indexrelid = f.conindid "
				   "WHERE f.conname = 'cfc2_xf_fk' AND NOT (i.indisunique AND (NOT f.conperiod OR "
				   "  EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid "
				   "  AND k.conrelid = f.confrelid AND k.contype IN ('p', 'u') AND k.conperiod)))");
	if (bad)
	{
		char	   *ref = pg_prop_sql_scalar(conn,
											 "SELECT conindid::regclass || ' ' || pg_get_indexdef(conindid) "
											 "FROM pg_constraint WHERE conname = 'cfc2_xf_fk'", NULL, NULL);

		pg_prop_fail(tc,
					 "foreign key accepted that is not backed by a primary key / unique "
					 "constraint%s\n  SQL: %s\n  references index: %s",
					 period ? " using WITHOUT OVERLAPS" : "", s.data, ref ? ref : "?");
	}

	c2_exec(tc, conn, xf_cleanup);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/* ----------------------------------------------------------------------
 * CF 7465: UNIQUE NULLS NOT DISTINCT WITHOUT OVERLAPS, range == multirange
 * ----------------------------------------------------------------------
 */

/*
 * A single-range multirange behaves like that range under = and &&, so a
 * UNIQUE [NULLS NOT DISTINCT] (id, valid_at WITHOUT OVERLAPS) constraint
 * must accept or reject the same rows whether the columns are int4range or
 * int4multirange (the latter's GiST index is lossy and goes through the
 * recheck path).  The same drawn row sequence, with NULLs in either column
 * and overlapping / adjacent / disjoint ranges, is inserted one row at a
 * time into both tables (plain INSERT or ON CONFLICT DO NOTHING); every
 * outcome and the final contents must agree.
 */
PG_PROP(without_overlaps_unique_range_eq_multirange)
{
	pg_prop_conn *conn = c2_connect(tc);
	StringInfoData s;
	StringInfoData log;
	bool		nnd = pg_prop_draw_int(tc, 0, 3) != 0;
	bool		pk = !nnd && pg_prop_draw_int(tc, 0, 3) == 0;
	int			nrows = (int) pg_prop_draw_int(tc, 1, 10);
	char	   *a;
	char	   *b;

	initStringInfo(&s);
	initStringInfo(&log);
	c2_exec(tc, conn, "SET client_min_messages = error");
	c2_exec(tc, conn, "DROP TABLE IF EXISTS cfc2_wr, cfc2_wm");
	for (int t = 0; t < 2; t++)
	{
		resetStringInfo(&s);
		appendStringInfo(&s, "CREATE TABLE cfc2_w%c (n int, id int4%s, valid_at int4%s, "
						 "CONSTRAINT cfc2_w%c_k %s (id, valid_at WITHOUT OVERLAPS))",
						 t ? 'm' : 'r', t ? "multirange" : "range", t ? "multirange" : "range",
						 t ? 'm' : 'r',
						 pk ? "PRIMARY KEY" : nnd ? "UNIQUE NULLS NOT DISTINCT" : "UNIQUE");
		c2_exec(tc, conn, s.data);
	}

	for (int r = 0; r < nrows; r++)
	{
		int			id = (int) pg_prop_draw_int(tc, -1, 2);	/* -1 = NULL */
		int			lo = (int) pg_prop_draw_int(tc, -1, 8);	/* -1 = NULL */
		int			len = (int) pg_prop_draw_int(tc, 1, 4);
		bool		upsert = pg_prop_draw_int(tc, 0, 3) == 0;
		bool		ok[2];
		char		idr[32];
		char		vr[32];

		if (id < 0)
			strlcpy(idr, "NULL", sizeof(idr));
		else
			snprintf(idr, sizeof(idr), "'[%d,%d)'", id, id + 1);
		if (lo < 0)
			strlcpy(vr, "NULL", sizeof(vr));
		else
			snprintf(vr, sizeof(vr), "'[%d,%d)'", lo, lo + len);

		for (int t = 0; t < 2; t++)
		{
			const char *ty = t ? "int4multirange" : "int4range";

			resetStringInfo(&s);
			/* int4multirange(range) builds the one-range multirange */
			appendStringInfo(&s, "INSERT INTO cfc2_w%c VALUES (%d, %s(%s::int4range), %s(%s::int4range))%s",
							 t ? 'm' : 'r', r, t ? ty : "", idr, t ? ty : "", vr,
							 upsert ? " ON CONFLICT DO NOTHING" : "");
			ok[t] = c2_try(conn, s.data);
		}
		appendStringInfo(&log, "(%s, %s)%s: range %s, multirange %s\n", idr, vr,
						 upsert ? " ON CONFLICT DO NOTHING" : "",
						 ok[0] ? "ok" : "rejected", ok[1] ? "ok" : "rejected");
		if (ok[0] != ok[1])
			pg_prop_fail(tc,
						 "%s (id, valid_at WITHOUT OVERLAPS): range and multirange columns "
						 "disagree on a row\n%s",
						 pk ? "PRIMARY KEY" : nnd ? "UNIQUE NULLS NOT DISTINCT" : "UNIQUE",
						 log.data);
	}
	a = c2_rows(tc, conn, "SELECT n FROM cfc2_wr ORDER BY n");
	b = c2_rows(tc, conn, "SELECT n FROM cfc2_wm ORDER BY n");
	if (strcmp(a, b) != 0)
		pg_prop_fail(tc, "%s (id, valid_at WITHOUT OVERLAPS): range and multirange tables "
					 "hold different rows\n%s  range rows n:\n%s  multirange rows n:\n%s",
					 pk ? "PRIMARY KEY" : nnd ? "UNIQUE NULLS NOT DISTINCT" : "UNIQUE",
					 log.data, a, b);

	c2_exec(tc, conn, "DROP TABLE cfc2_wr, cfc2_wm");
	pg_free(a);
	pg_free(b);
	pfree(s.data);
	pfree(log.data);
	pg_prop_disconnect(conn);
}
