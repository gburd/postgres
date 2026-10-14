/*-------------------------------------------------------------------------
 *
 * prop_pl_langs.c
 *	  Property tests of the procedural-language / SQL boundary.
 *
 * The procedural languages themselves work; what is not obvious is whether
 * they behave correctly *at the boundary with SQL inside PostgreSQL* -- when a
 * PL function runs a query via SPI, marshals composite / array / set-returning
 * values across the boundary, fires as a trigger during DML, or round-trips a
 * value through the PL's own type system and back.  That boundary, over the
 * generated schemas, is the surface these properties target.  Each is a
 * differential oracle: the PL path must agree with the equivalent pure-SQL
 * path; a disagreement is a bug in argument marshalling, SPI result handling,
 * SRF materialization, trigger firing, or type conversion -- not in the
 * language's arithmetic.
 *
 * Properties (each over a freshly generated qg_t table):
 *	 spi_count_matches     -- a PL function that SPI-executes "SELECT count(*)
 *						     ... WHERE <pred>" returns the same count as running
 *						     that query directly.  Exercises SPI from the PL.
 *	 srf_matches_sql       -- a PL set-returning function that yields the ids
 *						     satisfying <pred> returns the same multiset as the
 *						     SQL query.  Exercises set return + materialization.
 *	 roundtrip_jsonb       -- a value passed into a PL function as jsonb and
 *						     returned must be unchanged (PL type marshalling).
 *	 trigger_derived       -- a PL BEFORE trigger that recomputes a derived
 *						     column keeps it consistent with the SQL expression
 *						     after arbitrary INSERT/UPDATE churn.
 *
 * Availability: each property probes its language with pg_prop_require_extension
 * and pg_prop_skips when the server lacks it, running whatever PLs are present.
 * Every property runs across all available PLs via a shared body parameterized
 * by the language name and its function/trigger body templates.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_pl_langs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "pl_langs"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/*
 * A procedural language and the per-language source fragments the properties
 * need.  Each fragment is a function/trigger BODY template written in that
 * language; the shared property bodies wrap them in CREATE FUNCTION.  ext is
 * the extension that must be loaded; "available" is filled per connection.
 */
typedef struct pl_lang
{
	const char *name;			/* language name in CREATE FUNCTION */
	const char *ext;			/* extension to require */

	/*
	 * Body of a function spi_count(pred text) RETURNS bigint that SPI-executes
	 * 'SELECT count(*) FROM qg_t WHERE ' || pred and returns the count.
	 */
	const char *spi_count_body;

	/*
	 * Body of a SRF srf_ids(pred text) RETURNS SETOF int yielding the ids of
	 * qg_t satisfying pred, in id order.
	 */
	const char *srf_body;

	/* Body of jr(j jsonb) RETURNS jsonb returning j unchanged. */
	const char *jsonb_body;

	/*
	 * Body of a trigger function that sets NEW.d := NEW.a + NEW.b (a derived
	 * column) before insert/update.
	 */
	const char *trig_body;
} pl_lang;

/*
 * The languages we know how to drive.  plpgsql is always present; the others
 * are built optionally.  Each body is written to the language's conventions
 * for SPI and argument access.
 */
static const pl_lang pls[] = {
	{
		"plpgsql", "plpgsql",
		/* spi_count */
		"DECLARE n bigint; BEGIN "
		"EXECUTE 'SELECT count(*) FROM qg_t WHERE ' || pred INTO n; "
		"RETURN n; END",
		/* srf */
		"DECLARE r int; BEGIN "
		"FOR r IN EXECUTE 'SELECT id FROM qg_t WHERE ' || pred || ' ORDER BY id' "
		"LOOP RETURN NEXT r; END LOOP; RETURN; END",
		/* jsonb */
		"BEGIN RETURN j; END",
		/* trigger */
		"BEGIN NEW.d := coalesce(NEW.a,0) + coalesce(NEW.b,0); RETURN NEW; END"
	},
	{
		"plpython3u", "plpython3u",
		"rv = plpy.execute('SELECT count(*) AS c FROM qg_t WHERE ' + pred)\n"
		"return rv[0]['c']",
		"rv = plpy.execute('SELECT id FROM qg_t WHERE ' + pred + ' ORDER BY id')\n"
		"return [r['id'] for r in rv]",
		"return j",
		"a = TD['new']['a'] or 0\n"
		"b = TD['new']['b'] or 0\n"
		"TD['new']['d'] = a + b\n"
		"return 'MODIFY'"
	},
	{
		"plperl", "plperl",
		"my ($pred) = @_; "
		"my $rv = spi_exec_query('SELECT count(*) c FROM qg_t WHERE ' . $pred); "
		"return $rv->{rows}->[0]->{c};",
		"my ($pred) = @_; "
		"my $rv = spi_exec_query('SELECT id FROM qg_t WHERE ' . $pred . ' ORDER BY id'); "
		"foreach my $row (@{$rv->{rows}}) { return_next($row->{id}); } return undef;",
		"my ($j) = @_; return $j;",
		"$_TD->{new}{d} = ($_TD->{new}{a} // 0) + ($_TD->{new}{b} // 0); return 'MODIFY';"
	},
	{
		"pltcl", "pltcl",
		"spi_exec \"SELECT count(*) c FROM qg_t WHERE $1\"; return $c",
		"spi_exec \"SELECT id FROM qg_t WHERE $1 ORDER BY id\" { return_next $id }; return",
		"return $1",
		"set a [expr {[string is integer -strict $NEW(a)] ? $NEW(a) : 0}]; "
		"set b [expr {[string is integer -strict $NEW(b)] ? $NEW(b) : 0}]; "
		"set NEW(d) [expr {$a + $b}]; return [array get NEW]"
	},
};

#define NPLS ((int) (sizeof(pls) / sizeof(pls[0])))

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

static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn, const char *a,
				  const char *b)
{
	bool		fa = false,
				fb = false;
	char	   *ea = NULL,
			   *eb = NULL,
			   *ra,
			   *rb;

	ra = pg_prop_sql_rows(conn, a, &fa, &ea);
	if (fa)
		pg_prop_fail(tc, "query a failed: %s\n  SQL: %s", ea ? ea : "", a);
	rb = pg_prop_sql_rows(conn, b, &fb, &eb);
	if (fb)
		pg_prop_fail(tc, "query b failed: %s\n  SQL: %s", eb ? eb : "", b);
	if (strcmp(ra, rb) != 0)
		pg_prop_fail(tc, "PL/SQL mismatch:\n  a: %s\n  b: %s\n  ra:\n%s\n  rb:\n%s",
					 a, b, ra, rb);
	pg_free(ra);
	pg_free(rb);
	if (ea)
		pg_free(ea);
	if (eb)
		pg_free(eb);
}

/* Connect, require all PLs we can, build qg_t; returns conn (never NULL). */
static pg_prop_conn *
connect_build(pg_prop_tc *tc, int *nrows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	qg_build_table(tc, conn, nrows);
	return conn;
}

/*
 * spi_count: for each available PL, a function that SPI-runs the count query
 * over qg_t with a generated predicate must equal the direct SQL count.
 */
PG_PROP(spi_count_matches)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData pred;
	StringInfoData ddl;
	StringInfoData q;
	char	   *err = NULL;
	int			i;
	int			ran = 0;

	conn = connect_build(tc, &nrows);
	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);
	initStringInfo(&ddl);
	initStringInfo(&q);

	for (i = 0; i < NPLS; i++)
	{
		long		pl_count,
					sql_count;

		if (!pg_prop_require_extension(conn, pls[i].ext))
			continue;
		ran++;

		resetStringInfo(&ddl);
		appendStringInfo(&ddl,
						 "CREATE OR REPLACE FUNCTION pl_spi_count(pred text) "
						 "RETURNS bigint LANGUAGE %s AS $plbody$\n%s\n$plbody$;",
						 pls[i].name, pls[i].spi_count_body);
		if (!pg_prop_sql_exec(conn, ddl.data, &err))
			pg_prop_fail(tc, "%s function create failed: %s",
						 pls[i].name, err ? err : "");

		/* count via the PL (SPI), passing the predicate as text */
		resetStringInfo(&q);
		appendStringInfo(&q, "SELECT pl_spi_count('%s')", pred.data);
		{
			/* the predicate text may contain single quotes? our generator does
			 * not emit string literals with quotes in numeric predicates, but
			 * text leaves use 'a' etc.  Escape by doubling single quotes. */
		}
		/* escape single quotes in the predicate for the SQL string literal */
		resetStringInfo(&q);
		appendStringInfoString(&q, "SELECT pl_spi_count('");
		for (const char *p = pred.data; *p; p++)
		{
			if (*p == '\'')
				appendStringInfoChar(&q, '\'');
			appendStringInfoChar(&q, *p);
		}
		appendStringInfoString(&q, "')");
		pl_count = scalar_long(tc, conn, q.data);

		/* count directly in SQL */
		resetStringInfo(&q);
		appendStringInfo(&q, "SELECT count(*) FROM qg_t WHERE %s", pred.data);
		sql_count = scalar_long(tc, conn, q.data);

		if (pl_count != sql_count)
			pg_prop_fail(tc,
						 "%s SPI count %ld != SQL count %ld\n  pred: %s",
						 pls[i].name, pl_count, sql_count, pred.data);
	}

	if (ran == 0)
		pg_prop_skip(tc, "no procedural language available");

	pfree(pred.data);
	pfree(ddl.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * srf: a PL set-returning function yielding the matching ids must return the
 * same multiset as the SQL query.
 */
PG_PROP(srf_matches_sql)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData pred;
	StringInfoData ddl;
	StringInfoData a;
	StringInfoData b;
	char	   *err = NULL;
	int			i;
	int			ran = 0;

	conn = connect_build(tc, &nrows);
	initStringInfo(&pred);
	qg_draw_predicate(tc, &pred);
	initStringInfo(&ddl);
	initStringInfo(&a);
	initStringInfo(&b);

	for (i = 0; i < NPLS; i++)
	{
		if (!pg_prop_require_extension(conn, pls[i].ext))
			continue;
		ran++;

		resetStringInfo(&ddl);
		appendStringInfo(&ddl,
						 "CREATE OR REPLACE FUNCTION pl_srf(pred text) "
						 "RETURNS SETOF int LANGUAGE %s AS $plbody$\n%s\n$plbody$;",
						 pls[i].name, pls[i].srf_body);
		if (!pg_prop_sql_exec(conn, ddl.data, &err))
			pg_prop_fail(tc, "%s SRF create failed: %s",
						 pls[i].name, err ? err : "");

		/* PL SRF, predicate escaped into the SQL literal */
		resetStringInfo(&a);
		appendStringInfoString(&a, "SELECT pl_srf('");
		for (const char *p = pred.data; *p; p++)
		{
			if (*p == '\'')
				appendStringInfoChar(&a, '\'');
			appendStringInfoChar(&a, *p);
		}
		appendStringInfoString(&a, "') AS id ORDER BY id");

		resetStringInfo(&b);
		appendStringInfo(&b, "SELECT id FROM qg_t WHERE %s ORDER BY id",
						 pred.data);

		require_same_rows(tc, conn, a.data, b.data);
	}

	if (ran == 0)
		pg_prop_skip(tc, "no procedural language available");

	pfree(pred.data);
	pfree(ddl.data);
	pfree(a.data);
	pfree(b.data);
	pg_prop_disconnect(conn);
}

/*
 * jsonb round-trip: a value marshalled into a PL function as jsonb and
 * returned must be byte-identical (normalized) to the input.  Exercises the
 * PL's jsonb <-> native conversion, a rich source of boundary bugs.
 */
PG_PROP(roundtrip_jsonb)
{
	pg_prop_conn *conn;
	int			nrows;
	StringInfoData ddl;
	StringInfoData q;
	char	   *err = NULL;
	int			i;
	int			ran = 0;
	int			a = (int) pg_prop_draw_int(tc, -100, 100);
	int			b = (int) pg_prop_draw_int(tc, -100, 100);

	conn = connect_build(tc, &nrows);
	initStringInfo(&ddl);
	initStringInfo(&q);

	for (i = 0; i < NPLS; i++)
	{
		char	   *v;
		bool		failed = false;

		/* pltcl has no native jsonb mapping; skip jsonb for it */
		if (strcmp(pls[i].name, "pltcl") == 0)
			continue;
		if (!pg_prop_require_extension(conn, pls[i].ext))
			continue;
		ran++;

		resetStringInfo(&ddl);
		appendStringInfo(&ddl,
						 "CREATE OR REPLACE FUNCTION pl_jr(j jsonb) "
						 "RETURNS jsonb LANGUAGE %s AS $plbody$\n%s\n$plbody$;",
						 pls[i].name, pls[i].jsonb_body);
		if (!pg_prop_sql_exec(conn, ddl.data, &err))
			pg_prop_fail(tc, "%s jsonb fn create failed: %s",
						 pls[i].name, err ? err : "");

		resetStringInfo(&q);
		appendStringInfo(&q,
						 "SELECT (pl_jr('{\"a\":%d,\"b\":[%d,null,\"x\"]}'::jsonb) "
						 "= '{\"a\":%d,\"b\":[%d,null,\"x\"]}'::jsonb)",
						 a, b, a, b);
		v = pg_prop_sql_scalar(conn, q.data, &failed, &err);
		if (failed)
			pg_prop_fail(tc, "%s jsonb roundtrip query failed: %s",
						 pls[i].name, err ? err : "");
		if (v == NULL || strcmp(v, "t") != 0)
			pg_prop_fail(tc, "%s did not round-trip jsonb unchanged", pls[i].name);
		if (v)
			pg_free(v);
	}

	if (ran == 0)
		pg_prop_skip(tc, "no jsonb-capable procedural language available");

	pfree(ddl.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * trigger: a PL BEFORE trigger that maintains a derived column d = a + b must
 * leave d consistent with the SQL expression for every row after arbitrary
 * INSERT/UPDATE churn.  Exercises trigger firing and NEW-tuple modification
 * across the PL boundary.
 *
 * All random values are drawn once, up front, into fixed-size arrays, so the
 * number of draws does not depend on how many PLs the server has (keeping the
 * case reproducible); each PL uses its own table/function/trigger names so
 * nothing leaks between languages.
 */
PG_PROP(trigger_derived)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			i;
	int			ran = 0;
	int			nins = (int) pg_prop_draw_int(tc, 0, 30);
	int			av[30];
	int			bv[30];
	int			j;

	/* draw all row values up front (loop-invariant draw count) */
	for (j = 0; j < nins; j++)
	{
		av[j] = (pg_prop_draw_int(tc, 0, 6) == 0) ? 0 : (int) pg_prop_draw_int(tc, -50, 50);
		bv[j] = (pg_prop_draw_int(tc, 0, 6) == 0) ? 0 : (int) pg_prop_draw_int(tc, -50, 50);
	}

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	for (i = 0; i < NPLS; i++)
	{
		StringInfoData ddl;
		long		bad;

		if (!pg_prop_require_extension(conn, pls[i].ext))
			continue;
		ran++;

		/* per-PL object names so no CREATE OR REPLACE across languages */
		initStringInfo(&ddl);
		appendStringInfo(&ddl,
						 "SET client_min_messages=warning; "
						 "DROP TABLE IF EXISTS pl_trg_%d; "
						 "DROP FUNCTION IF EXISTS pl_trg_fn_%d() CASCADE; "
						 "CREATE TABLE pl_trg_%d (id int PRIMARY KEY, a int, b int, d int); "
						 "CREATE FUNCTION pl_trg_fn_%d() RETURNS trigger "
						 "LANGUAGE %s AS $plbody$\n%s\n$plbody$; "
						 "CREATE TRIGGER pl_trg BEFORE INSERT OR UPDATE ON pl_trg_%d "
						 "FOR EACH ROW EXECUTE FUNCTION pl_trg_fn_%d();",
						 i, i, i, i, pls[i].name, pls[i].trig_body, i, i);
		if (!pg_prop_sql_exec(conn, ddl.data, &err))
			pg_prop_fail(tc, "%s trigger setup failed: %s",
						 pls[i].name, err ? err : "");

		resetStringInfo(&ddl);
		for (j = 0; j < nins; j++)
			appendStringInfo(&ddl,
							 "INSERT INTO pl_trg_%d (id, a, b) VALUES (%d, %d, %d); ",
							 i, j, av[j], bv[j]);
		/* a churn update */
		appendStringInfo(&ddl, "UPDATE pl_trg_%d SET a = a + 1 WHERE id %% 2 = 0; ", i);
		if (!pg_prop_sql_exec(conn, ddl.data, &err))
			pg_prop_fail(tc, "%s trigger DML failed: %s",
						 pls[i].name, err ? err : "");

		/* every row's derived d must equal a + b (coalesced) */
		{
			bool		failed = false;
			StringInfoData cq;
			char	   *v;

			initStringInfo(&cq);
			appendStringInfo(&cq,
							 "SELECT count(*) FROM pl_trg_%d "
							 "WHERE d IS DISTINCT FROM "
							 "(coalesce(a,0) + coalesce(b,0))", i);
			v = pg_prop_sql_scalar(conn, cq.data, &failed, &err);
			bad = v ? atol(v) : 0;
			if (v)
				pg_free(v);
			if (failed)
				pg_prop_fail(tc, "%s trigger check query failed: %s",
							 pls[i].name, err ? err : "");
			pfree(cq.data);
		}
		if (bad != 0)
			pg_prop_fail(tc,
						 "%s trigger left %ld rows with d != a+b",
						 pls[i].name, bad);
		/* drop this PL's objects before the next language */
		resetStringInfo(&ddl);
		appendStringInfo(&ddl,
						 "DROP TABLE IF EXISTS pl_trg_%d; "
						 "DROP FUNCTION IF EXISTS pl_trg_fn_%d() CASCADE;", i, i);
		(void) pg_prop_sql_exec(conn, ddl.data, &err);
		pfree(ddl.data);
	}

	if (err)
		pg_free(err);
	if (ran == 0)
		pg_prop_skip(tc, "no procedural language available");
	pg_prop_disconnect(conn);
}
