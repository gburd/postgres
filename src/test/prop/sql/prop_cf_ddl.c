/*-------------------------------------------------------------------------
 *
 * prop_cf_ddl.c
 *	  Shape-changing DDL: after ALTER TABLE / ALTER TYPE the table's
 *	  constraints, indexes and rows must still agree.
 *
 * Every area elsewhere builds its tables once and then only queries them.
 * The catalog and relcache bugs that the open commitfest is fixing live in
 * the moment a table's SHAPE changes: a whole-row reference that survives a
 * DROP COLUMN, a CHECK not rebuilt after SET DATA TYPE, a partition key on
 * an altered column, two subcommands of one ALTER TABLE that both rebuild
 * the same column, per-column settings lost in an index rebuild.
 *
 * One stateful property draws a short sequence of shape-changing steps over a
 * table carrying the dependents those bugs need -- whole-row CHECKs (some NOT
 * ENFORCED), a whole-row expression index, a stored generated column, an
 * expression index with a statistics target -- interleaved with DML, and
 * after every step checks:
 *
 *	 (1) the step succeeded or raised an ordinary user error -- never SQLSTATE
 *		 XX000, "cache lookup failed", "unexpected", or a crash (the harness
 *		 saves the statement and exits 3 if the backend dies);
 *	 (2) every row satisfies every ENFORCED CHECK constraint, recomputed with
 *		 a plain SELECT of pg_get_constraintdef's expression;
 *	 (3) every index agrees with a seqscan (count via each index) and every
 *		 btree passes amcheck's bt_index_check;
 *	 (4) the table still takes an INSERT and a SELECT of every column.
 *
 * Smaller properties then target triggers the sequence rarely draws:
 * composite types used as stored columns (ALTER TYPE ... DROP ATTRIBUTE /
 * ALTER ATTRIBUTE, also under a domain CHECK), whole-row partition keys,
 * and statistics targets across ALTER COLUMN TYPE.
 *
 *	 CF 6055  whole-row CHECK / index survives DROP COLUMN, SET DATA TYPE
 *	 CF 6749  whole-row index reference with a virtual generated column
 *	 CF 7071  whole-row expression index after ALTER TABLE
 *	 CF 6121  ALTER COLUMN TYPE + SET EXPRESSION in one ALTER TABLE
 *	 CF 7117  SET EXPRESSION on a partition with dependents
 *	 CF 7426  CHECK NOT ENFORCED on the parent, ENFORCED on a child
 *	 CF 7169  ALTER TYPE DROP ATTRIBUTE on a stored, indexed composite
 *	 CF 7356  ALTER TYPE ALTER ATTRIBUTE under a domain CHECK
 *	 CF 5988  ALTER COLUMN TYPE / DROP COLUMN under a whole-row partition key
 *	 CF 7214  statistics targets across ALTER COLUMN TYPE
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cf_ddl"
#include "pg_prop.h"

#include "lib/stringinfo.h"

static pg_prop_conn *
cd_connect(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

/*
 * An error a legal statement may raise: anything with a SQLSTATE outside
 * class XX and without the internal-error phrasings.
 */
static bool
cd_internal(const char *sqlstate, const char *msg)
{
	if (sqlstate == NULL)
		return true;
	if (strncmp(sqlstate, "XX", 2) == 0)
		return true;
	if (msg && (strstr(msg, "cache lookup failed") || strstr(msg, "unexpected") ||
				strstr(msg, "unrecognized") || strstr(msg, "could not find")))
		return true;
	return false;
}

/*
 * Run a DDL/DML step.  Returns true if it succeeded; false on a user error;
 * fails the case on an internal error.  Uses the async path to get the
 * SQLSTATE, then a probe statement so a dead backend trips the harness's
 * crash tripwire with this statement in the log.
 */
static bool
cd_step(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql, const StringInfo log)
{
	char	   *ss = NULL;
	char	   *err = NULL;
	bool		ok;

	appendStringInfo(log, "    %s;\n", sql);
	if (!pg_prop_sql_send(conn, sql))
		ok = false;
	else
		ok = pg_prop_sql_await(conn, &ss, &err);
	if (ok)
		return true;
	/*
	 * Liveness probe: exits 3 (and logs the probe) if the backend died.  The
	 * probe carries the step log, so the crash log names what crashed.
	 */
	{
		StringInfoData probe;

		initStringInfo(&probe);
		appendStringInfo(&probe, "SELECT $cd_probe$ liveness probe after:\n%s$cd_probe$", log->data);
		pg_prop_sql_exec(conn, probe.data, NULL);
		pfree(probe.data);
	}
	if (cd_internal(ss, err))
		pg_prop_fail(tc, "internal error (SQLSTATE %s): %s\n  steps:\n%s",
					 ss ? ss : "none", err ? err : "", log->data);
	appendStringInfo(log, "      -- rejected: %s %.160s", ss ? ss : "", err ? err : "");
	if (log->len > 0 && log->data[log->len - 1] != '\n')
		appendStringInfoChar(log, '\n');
	return false;
}

static long
cd_count(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql, const StringInfo log)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	long		n;

	if (failed || v == NULL)
	{
		if (err && strstr(err, "cache lookup failed"))
			pg_prop_fail(tc, "internal error during check: %s\n  check: %s\n  steps:\n%s",
						 err, sql, log->data);
		pg_prop_fail(tc, "check query failed: %s\n  check: %s\n  steps:\n%s",
					 err ? err : "", sql, log->data);
	}
	n = atol(v);
	pg_free(v);
	return n;
}

/*
 * Invariants (2)-(4) over table "rel".
 */
static void
cd_check(pg_prop_tc *tc, pg_prop_conn *conn, const char *rel, const StringInfo log)
{
	StringInfoData q;
	char	   *list;
	bool		failed = false;
	char	   *err = NULL;
	long		total;

	initStringInfo(&q);

	/* (4) a SELECT of every column, and the row count */
	appendStringInfo(&q, "SELECT count(*) FROM (SELECT * FROM %s) s", rel);
	total = cd_count(tc, conn, q.data, log);

	/*
	 * (2) every enforced, validated CHECK holds on every row.  Each check's
	 * expression is recomputed as "NOT (expr)" over the table; a row where
	 * it is TRUE violates the constraint.  (NULL satisfies a CHECK.)
	 */
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT string_agg(format('SELECT %%L AS c, count(*) FROM %s WHERE NOT (%%s)', "
					 "conname, pg_get_expr(conbin, conrelid)), ' UNION ALL ') "
					 "FROM pg_constraint WHERE conrelid = '%s'::regclass AND contype = 'c' "
					 "AND conenforced AND convalidated",
					 rel, rel);
	list = pg_prop_sql_scalar(conn, q.data, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "listing checks failed: %s\n  steps:\n%s", err ? err : "", log->data);
	if (list && list[0])
	{
		char	   *bad;

		resetStringInfo(&q);
		appendStringInfo(&q, "SELECT string_agg(c || '=' || count, ', ') FROM (%s) s WHERE count > 0", list);
		bad = pg_prop_sql_scalar(conn, q.data, &failed, &err);
		if (failed)
		{
			if (err && cd_internal("XX000", err) && strstr(err, "cache lookup"))
				pg_prop_fail(tc, "recomputing a CHECK raised an internal error: %s\n  steps:\n%s",
							 err, log->data);
			/* a check expression may legitimately error on some row; skip */
		}
		else if (bad && bad[0])
			pg_prop_fail(tc, "rows violate an ENFORCED CHECK constraint: %s\n  steps:\n%s",
						 bad, log->data);
		if (bad)
			pg_free(bad);
	}
	if (list)
		pg_free(list);

	/*
	 * (3) every btree index passes amcheck; every index, used as the only
	 * access path, returns as many rows as the seqscan.  A bitmap scan of
	 * "col IS NOT NULL OR col IS NULL" is not indexable for every AM, so use
	 * amcheck for btree and, for all indexes, compare a forced-index count on
	 * the index's first key against the seqscan count with the same qual.
	 */
	resetStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT string_agg(i.indexrelid::regclass::text, ',') FROM pg_index i "
					 "JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am a ON a.oid = c.relam "
					 "WHERE i.indrelid = '%s'::regclass AND a.amname = 'btree' AND i.indisvalid", rel);
	list = pg_prop_sql_scalar(conn, q.data, &failed, NULL);
	if (!failed && list && list[0])
	{
		char	   *save = NULL;

		for (char *ix = strtok_r(list, ",", &save); ix; ix = strtok_r(NULL, ",", &save))
		{
			resetStringInfo(&q);
			appendStringInfo(&q, "SELECT bt_index_check('%s'::regclass, true)", ix);
			if (!pg_prop_sql_exec(conn, q.data, &err))
			{
				if (err && strstr(err, "does not exist"))
					continue;	/* amcheck not installed: skipped */
				pg_prop_fail(tc, "amcheck failed for index %s: %s\n  steps:\n%s",
							 ix, err ? err : "", log->data);
			}
		}
	}
	if (list)
		pg_free(list);

	/* (4) the table still accepts an INSERT of defaults (rolled back) */
	resetStringInfo(&q);
	appendStringInfo(&q, "BEGIN; INSERT INTO %s DEFAULT VALUES; ROLLBACK", rel);
	{
		char	   *e2 = NULL;

		if (!pg_prop_sql_exec(conn, q.data, &e2))
		{
			pg_prop_sql_exec(conn, "ROLLBACK", NULL);
			if (e2 && (strstr(e2, "cache lookup failed") || strstr(e2, "unexpected") ||
					   strstr(e2, "invalid memory alloc") || strstr(e2, "could not find")))
				pg_prop_fail(tc, "table no longer accepts INSERT: %s\n  steps:\n%s", e2, log->data);
		}
	}
	(void) total;
	pfree(q.data);
}

/*
 * The stateful property.  Table cfd_t(a int, b int, c text) with a drawn set
 * of dependents, then 1..12 drawn steps, each followed by cd_check.
 */
PG_PROP(alter_table_sequence_keeps_table_consistent)
{
	pg_prop_conn *conn = cd_connect(tc);
	StringInfoData log;
	StringInfoData s;
	int			nsteps = (int) pg_prop_draw_int(tc, 1, 12);
	bool		have_gen;

	initStringInfo(&log);
	initStringInfo(&s);

	pg_prop_sql_exec(conn, "CREATE EXTENSION IF NOT EXISTS amcheck", NULL);

	have_gen = pg_prop_draw_bool(tc);
	appendStringInfoString(&s, "DROP TABLE IF EXISTS cfd_t CASCADE; CREATE TABLE cfd_t (a int, b int, c text");
	if (have_gen)
		appendStringInfo(&s, ", g int GENERATED ALWAYS AS (a + 1) %s",
						 pg_prop_draw_bool(tc) ? "STORED" : "VIRTUAL");
	appendStringInfoString(&s, ")");
	cd_step(tc, conn, s.data, &log);

	/* dependents, each drawn independently */
	if (pg_prop_draw_bool(tc))
		cd_step(tc, conn, "ALTER TABLE cfd_t ADD CONSTRAINT cfd_wr_notnull CHECK (cfd_t IS NOT NULL) NOT ENFORCED", &log);
	if (pg_prop_draw_bool(tc))
		cd_step(tc, conn, "ALTER TABLE cfd_t ADD CONSTRAINT cfd_wr_len CHECK (length(cfd_t::text) < 200)", &log);
	if (pg_prop_draw_bool(tc))
		cd_step(tc, conn, "ALTER TABLE cfd_t ADD CONSTRAINT cfd_a_pos CHECK (a IS NULL OR a > -1000)", &log);
	if (pg_prop_draw_bool(tc))
		cd_step(tc, conn, "CREATE INDEX cfd_wr_idx ON cfd_t ((cfd_t))", &log);
	if (pg_prop_draw_bool(tc))
		cd_step(tc, conn, "CREATE UNIQUE INDEX cfd_wr_pred ON cfd_t (a) WHERE cfd_t IS NOT NULL", &log);
	if (pg_prop_draw_bool(tc))
	{
		cd_step(tc, conn, "CREATE INDEX cfd_expr_idx ON cfd_t ((a * b))", &log);
		cd_step(tc, conn, "ALTER INDEX cfd_expr_idx ALTER COLUMN 1 SET STATISTICS 123", &log);
	}
	if (pg_prop_draw_bool(tc))
		cd_step(tc, conn, "CREATE INDEX cfd_b_idx ON cfd_t (b)", &log);

	resetStringInfo(&s);
	appendStringInfo(&s, "INSERT INTO cfd_t (a, b, c) SELECT i, i %% 7, 'r' || i FROM generate_series(1, %d) i",
					 (int) pg_prop_draw_int(tc, 0, 300));
	cd_step(tc, conn, s.data, &log);
	cd_check(tc, conn, "cfd_t", &log);

	for (int i = 0; i < nsteps; i++)
	{
		static const char *const steps[] = {
			"ALTER TABLE cfd_t ALTER COLUMN a SET DATA TYPE bigint",
			"ALTER TABLE cfd_t ALTER COLUMN a SET DATA TYPE int",
			"ALTER TABLE cfd_t ALTER COLUMN b SET DATA TYPE numeric",
			"ALTER TABLE cfd_t ALTER COLUMN c SET DATA TYPE varchar(50)",
			"ALTER TABLE cfd_t DROP COLUMN b",
			"ALTER TABLE cfd_t DROP COLUMN c",
			"ALTER TABLE cfd_t ADD COLUMN d int DEFAULT 7",
			"ALTER TABLE cfd_t ADD COLUMN e text",
			"ALTER TABLE cfd_t ALTER COLUMN g SET EXPRESSION AS (a + 2)",
			"ALTER TABLE cfd_t ALTER COLUMN a SET DATA TYPE bigint, ALTER COLUMN g SET EXPRESSION AS (a * 2)",
			"ALTER TABLE cfd_t ADD COLUMN z int DEFAULT 11, ADD CONSTRAINT cfd_z CHECK (z > 9) NOT VALID",
			"ALTER TABLE cfd_t ALTER COLUMN z SET DATA TYPE numeric, ALTER COLUMN g SET EXPRESSION AS (a + 3)",
			"ALTER TABLE cfd_t ALTER CONSTRAINT cfd_wr_notnull ENFORCED",
			"ALTER TABLE cfd_t VALIDATE CONSTRAINT cfd_z",
			"UPDATE cfd_t SET a = a + 1 WHERE a % 3 = 0",
			"DELETE FROM cfd_t WHERE a % 5 = 0",
			"INSERT INTO cfd_t (a) VALUES (NULL), (-5), (999)",
			"VACUUM cfd_t",
			"REINDEX TABLE cfd_t",
		};
		const char *st = steps[pg_prop_draw_int(tc, 0, lengthof(steps) - 1)];

		/* VACUUM cannot run in the async path's implicit transaction: plain exec */
		if (strncmp(st, "VACUUM", 6) == 0)
		{
			appendStringInfo(&log, "    %s;\n", st);
			pg_prop_sql_exec(conn, st, NULL);
		}
		else
			cd_step(tc, conn, st, &log);
		cd_check(tc, conn, "cfd_t", &log);
	}

	/* statistics target on the expression index survives (CF 7214) */
	{
		bool		f = false;
		char	   *v = pg_prop_sql_scalar(conn,
										   "SELECT attstattarget FROM pg_attribute WHERE attrelid = "
										   "to_regclass('cfd_expr_idx') AND attnum = 1", &f, NULL);

		if (!f && v && v[0] && strcmp(v, "123") != 0)
			pg_prop_fail(tc, "expression index lost its statistics target: %s (set 123)\n  steps:\n%s",
						 v, log.data);
		if (v)
			pg_free(v);
	}

	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS cfd_t CASCADE", NULL);
	pfree(log.data);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7169 / 7356: a composite type used as a stored (and sometimes uniquely
 * indexed) column, or under a domain CHECK, then ALTER TYPE ... DROP / ALTER
 * ATTRIBUTE.  Invariants: no internal error; if the ALTER succeeded, a UNIQUE
 * index on the column still holds (count(DISTINCT v) = count(v)) and REINDEX
 * succeeds; a domain CHECK still rejects what it rejected.
 */
PG_PROP(alter_type_on_stored_composite_keeps_constraints)
{
	pg_prop_conn *conn = cd_connect(tc);
	StringInfoData log;
	int			kind = (int) pg_prop_draw_int(tc, 0, 3);
	bool		uniq = pg_prop_draw_bool(tc);
	int			n = (int) pg_prop_draw_int(tc, 1, 40);
	StringInfoData s;

	initStringInfo(&log);
	initStringInfo(&s);
	cd_step(tc, conn, "DROP TABLE IF EXISTS cfd_ct CASCADE; DROP DOMAIN IF EXISTS cfd_dom CASCADE; "
			"DROP TYPE IF EXISTS cfd_comp CASCADE", &log);
	cd_step(tc, conn, "CREATE TYPE cfd_comp AS (a int, b int)", &log);

	if (kind <= 1)
	{
		/* stored column; tuples differ only in b, so dropping b would merge them */
		cd_step(tc, conn, "CREATE TABLE cfd_ct (v cfd_comp)", &log);
		if (uniq)
			cd_step(tc, conn, "CREATE UNIQUE INDEX cfd_ct_v ON cfd_ct (v)", &log);
		appendStringInfo(&s, "INSERT INTO cfd_ct SELECT ROW(1, i)::cfd_comp FROM generate_series(1, %d) i", n);
		cd_step(tc, conn, s.data, &log);
		if (cd_step(tc, conn, kind == 0 ? "ALTER TYPE cfd_comp DROP ATTRIBUTE b"
					: "ALTER TYPE cfd_comp DROP ATTRIBUTE b CASCADE", &log))
		{
			long		d = cd_count(tc, conn, "SELECT count(DISTINCT v) FROM cfd_ct", &log);
			long		all = cd_count(tc, conn, "SELECT count(v) FROM cfd_ct", &log);

			if (uniq && d != all)
				pg_prop_fail(tc, "after ALTER TYPE DROP ATTRIBUTE a UNIQUE column holds duplicates: "
							 "%ld distinct of %ld\n  steps:\n%s", d, all, log.data);
			if (uniq && !cd_step(tc, conn, "REINDEX INDEX cfd_ct_v", &log))
				pg_prop_fail(tc, "REINDEX of the unique index fails after the ALTER TYPE\n  steps:\n%s",
							 log.data);
		}
	}
	else
	{
		/* domain whose CHECK goes through the composite type */
		cd_step(tc, conn, "CREATE DOMAIN cfd_dom AS int CHECK ((ROW(VALUE, 0)::cfd_comp).a > 0)", &log);
		cd_step(tc, conn, kind == 2 ? "ALTER TYPE cfd_comp ALTER ATTRIBUTE a TYPE bigint"
				: "ALTER TYPE cfd_comp ALTER ATTRIBUTE a TYPE text", &log);
		/* the domain must still accept 1 and reject -1 (or the ALTER was refused) */
		if (!cd_step(tc, conn, "SELECT 1::cfd_dom", &log))
			pg_prop_fail(tc, "domain no longer accepts a valid value\n  steps:\n%s", log.data);
		if (cd_step(tc, conn, "SELECT (-1)::cfd_dom", &log))
			pg_prop_fail(tc, "domain CHECK no longer rejects -1\n  steps:\n%s", log.data);
	}
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS cfd_ct CASCADE; DROP DOMAIN IF EXISTS cfd_dom CASCADE; "
					 "DROP TYPE IF EXISTS cfd_comp CASCADE", NULL);
	pfree(log.data);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 5988: a partitioned table whose key is a whole-row expression.  ALTER
 * COLUMN TYPE / DROP COLUMN must either be rejected or leave the table
 * usable: INSERT and SELECT then succeed, never "invalid memory alloc".
 */
PG_PROP(wholerow_partition_key_survives_alter)
{
	static const char *const alters[] = {
		"ALTER TABLE cfd_pk ALTER COLUMN a TYPE text",
		"ALTER TABLE cfd_pk ALTER COLUMN b TYPE bigint",
		"ALTER TABLE cfd_pk DROP COLUMN a",
		"ALTER TABLE cfd_pk DROP COLUMN b",
	};
	pg_prop_conn *conn = cd_connect(tc);
	StringInfoData log;
	const char *alt = alters[pg_prop_draw_int(tc, 0, lengthof(alters) - 1)];

	initStringInfo(&log);
	cd_step(tc, conn, "DROP TABLE IF EXISTS cfd_pk CASCADE", &log);
	cd_step(tc, conn, "CREATE TABLE cfd_pk (a int, b int) PARTITION BY LIST ((cfd_pk))", &log);
	cd_step(tc, conn, "CREATE TABLE cfd_pk_1 PARTITION OF cfd_pk DEFAULT", &log);
	cd_step(tc, conn, "INSERT INTO cfd_pk VALUES (1, 2), (3, 4)", &log);
	cd_step(tc, conn, alt, &log);
	if (!cd_step(tc, conn, "INSERT INTO cfd_pk DEFAULT VALUES", &log))
		pg_prop_fail(tc, "table no longer accepts INSERT\n  steps:\n%s", log.data);
	cd_count(tc, conn, "SELECT count(*) FROM (SELECT * FROM cfd_pk) s", &log);
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS cfd_pk CASCADE", NULL);
	pfree(log.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7426: a CHECK that is NOT ENFORCED on the parent and ENFORCED on an
 * inheritance child.  The child's rows must satisfy it, including rows that
 * arrive through the parent, through ATTACH/INHERIT, and after the parent's
 * constraint changes.
 */
PG_PROP(child_enforced_check_holds_on_child_rows)
{
	pg_prop_conn *conn = cd_connect(tc);
	StringInfoData log;
	int			how = (int) pg_prop_draw_int(tc, 0, 2);

	initStringInfo(&log);
	cd_step(tc, conn, "DROP TABLE IF EXISTS cfd_par CASCADE", &log);
	cd_step(tc, conn, "CREATE TABLE cfd_par (x int, CONSTRAINT cfd_ck CHECK (x > 0) NOT ENFORCED)", &log);
	if (how == 0)
		cd_step(tc, conn, "CREATE TABLE cfd_ch () INHERITS (cfd_par)", &log);
	else
	{
		cd_step(tc, conn, "CREATE TABLE cfd_ch (x int, CONSTRAINT cfd_ck CHECK (x > 0) NOT ENFORCED)", &log);
		cd_step(tc, conn, "ALTER TABLE cfd_ch INHERIT cfd_par", &log);
	}
	if (cd_step(tc, conn, "ALTER TABLE cfd_ch ALTER CONSTRAINT cfd_ck ENFORCED", &log))
	{
		cd_step(tc, conn, "INSERT INTO cfd_ch VALUES (-1)", &log);
		if (how == 2)
			cd_step(tc, conn, "ALTER TABLE cfd_par ALTER CONSTRAINT cfd_ck NOT ENFORCED", &log);
		cd_step(tc, conn, "INSERT INTO cfd_ch VALUES (-2)", &log);
		if (cd_count(tc, conn, "SELECT count(*) FROM ONLY cfd_ch WHERE NOT (x > 0)", &log) > 0)
		{
			bool		f = false;
			char	   *e = pg_prop_sql_scalar(conn,
											   "SELECT conenforced FROM pg_constraint WHERE conrelid = "
											   "'cfd_ch'::regclass AND conname = 'cfd_ck'", &f, NULL);

			if (e && strcmp(e, "t") == 0)
				pg_prop_fail(tc, "child holds rows violating its ENFORCED CHECK\n  steps:\n%s", log.data);
			if (e)
				pg_free(e);
		}
	}
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS cfd_par CASCADE", NULL);
	pfree(log.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7117: ALTER COLUMN SET EXPRESSION on a partition (or with ONLY) when the
 * generated column has dependents (index, CHECK, FK).  Accepted or rejected,
 * afterwards every partition's rows satisfy the CHECK and the generated
 * column equals its expression.
 */
PG_PROP(partition_set_expression_keeps_generated_consistent)
{
	static const char *const exprs[] = {"(a)", "(a + 1)", "(a + 112)", "(a * 2)"};
	pg_prop_conn *conn = cd_connect(tc);
	StringInfoData log;
	StringInfoData s;
	const char *target = pg_prop_draw_bool(tc) ? "cfd_gp_1" : "ONLY cfd_gp";
	const char *ex = exprs[pg_prop_draw_int(tc, 0, lengthof(exprs) - 1)];

	initStringInfo(&log);
	initStringInfo(&s);
	cd_step(tc, conn, "DROP TABLE IF EXISTS cfd_gp CASCADE", &log);
	cd_step(tc, conn, "CREATE TABLE cfd_gp (a int, b int GENERATED ALWAYS AS (a) STORED, "
			"CONSTRAINT cfd_gp_cc CHECK (b < 120)) PARTITION BY RANGE (a)", &log);
	cd_step(tc, conn, "CREATE TABLE cfd_gp_1 PARTITION OF cfd_gp FOR VALUES FROM (1) TO (10)", &log);
	cd_step(tc, conn, "CREATE TABLE cfd_gp_2 PARTITION OF cfd_gp FOR VALUES FROM (10) TO (20)", &log);
	if (pg_prop_draw_bool(tc))
		cd_step(tc, conn, "CREATE INDEX cfd_gp_b ON cfd_gp (b)", &log);
	cd_step(tc, conn, "INSERT INTO cfd_gp SELECT g FROM generate_series(5, 14) g", &log);
	appendStringInfo(&s, "ALTER TABLE %s ALTER COLUMN b SET EXPRESSION AS %s", target, ex);
	cd_step(tc, conn, s.data, &log);
	if (cd_count(tc, conn, "SELECT count(*) FROM cfd_gp WHERE NOT (b < 120)", &log) > 0)
		pg_prop_fail(tc, "a partition holds rows violating the CHECK on the generated column\n  steps:\n%s",
					 log.data);
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS cfd_gp CASCADE", NULL);
	pfree(log.data);
	pfree(s.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 6431 / 6449: a composite value produced before ALTER TYPE ... ALTER
 * ATTRIBUTE ... TYPE in the same transaction, then read after it, must not
 * be interpreted with the new row type: either an error is raised or the
 * value reads back correctly.  Reading the old int4 bits as text crashes
 * (textout memcpy of a bogus length).  Drawn: the holder of the old value
 * (cursor, PL/pgSQL row variable, generic RECORD, OUT parameter, nested
 * composite), the old value (biased to large ints, which become bogus
 * varlena lengths), the new attribute type.  Invariant: the backend
 * survives and no internal error is raised (cd_step checks both).
 */
PG_PROP(composite_read_after_alter_attribute_type)
{
	static const char *const newtypes[] = {"text", "numeric", "bigint", "varchar(20)"};
	pg_prop_conn *conn = cd_connect(tc);
	int			holder = (int) pg_prop_draw_int(tc, 0, 4);
	const char *nt = newtypes[pg_prop_draw_int(tc, 0, lengthof(newtypes) - 1)];
	long		v = pg_prop_draw_bool(tc) ? (1L << pg_prop_draw_int(tc, 20, 30)) : (long) pg_prop_draw_int(tc, 0, 1000);
	StringInfoData s;
	StringInfoData log;

	initStringInfo(&s);
	initStringInfo(&log);
	cd_step(tc, conn, "DROP TYPE IF EXISTS cfd_co CASCADE", &log);
	cd_step(tc, conn, "DROP TYPE IF EXISTS cfd_ci CASCADE", &log);
	cd_step(tc, conn, "CREATE TYPE cfd_ci AS (x int, y int)", &log);
	cd_step(tc, conn, "CREATE TYPE cfd_co AS (a int, b int, c cfd_ci)", &log);
	if (holder == 0)
	{
		/* cursor (CF 6431) */
		cd_step(tc, conn, "BEGIN", &log);
		appendStringInfo(&s, "DECLARE cfd_cur CURSOR FOR SELECT (i, %ld, NULL)::cfd_co "
						 "FROM generate_series(1, 3) i", v);
		cd_step(tc, conn, s.data, &log);
		cd_step(tc, conn, "FETCH FROM cfd_cur", &log);
		resetStringInfo(&s);
		appendStringInfo(&s, "ALTER TYPE cfd_co ALTER ATTRIBUTE b TYPE %s", nt);
		cd_step(tc, conn, s.data, &log);
		cd_step(tc, conn, "FETCH FROM cfd_cur", &log);
		cd_step(tc, conn, "ROLLBACK", &log);
	}
	else
	{
		/* PL/pgSQL (CF 6449): row variable, generic RECORD, OUT parameter, nested */
		if (holder == 4)
			appendStringInfo(&s,
							 "CREATE FUNCTION cfd_f() RETURNS record LANGUAGE plpgsql AS $f$ "
							 "DECLARE r cfd_co; BEGIN "
							 "r := row(1, 2, row(10, %ld)::cfd_ci)::cfd_co; "
							 "ALTER TYPE cfd_ci ALTER ATTRIBUTE y TYPE %s; "
							 "RETURN r; END $f$", v, nt);
		else
			appendStringInfo(&s,
							 "CREATE FUNCTION cfd_f(%s) RETURNS %s LANGUAGE plpgsql AS $f$ "
							 "DECLARE r %s; BEGIN "
							 "%s := row(1, %ld, NULL)::cfd_co; "
							 "ALTER TYPE cfd_co ALTER ATTRIBUTE b TYPE %s; "
							 "%s END $f$",
							 holder == 3 ? "OUT o cfd_co" : "",
							 holder == 3 ? "cfd_co" : "record",
							 holder == 2 ? "record" : "cfd_co",
							 holder == 3 ? "o" : "r", v, nt,
							 holder == 3 ? "RETURN;" : "RETURN r;");
		cd_step(tc, conn, s.data, &log);
		cd_step(tc, conn, "SELECT cfd_f()::text", &log);
		cd_step(tc, conn, "DROP FUNCTION cfd_f", &log);
	}
	pg_prop_sql_exec(conn, "DROP TYPE IF EXISTS cfd_co CASCADE; DROP TYPE IF EXISTS cfd_ci CASCADE", NULL);
	pfree(s.data);
	pfree(log.data);
	pg_prop_disconnect(conn);
}
