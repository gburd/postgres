/*-------------------------------------------------------------------------
 *
 * prop_pl_langs.c
 *	  Property tests across the procedural languages (PL/pgSQL, PL/Python,
 *	  PL/Perl, PL/Tcl).
 *
 * Each procedural language can define a function body that computes a value;
 * for a pure, deterministic computation the language used must not change the
 * answer.  This area generates the same simple integer computation in each
 * available PL and requires it to equal the pure-SQL computation of the same
 * expression -- a differential oracle across language implementations, where a
 * disagreement is a bug in a PL's argument marshalling, integer handling, or
 * return conversion.
 *
 * Availability: a PL is a loadable extension (plpgsql is always present;
 * plpython3u, plperl, pltcl are present only if the server was built with
 * them).  Each property probes its language with pg_prop_require_extension and
 * calls pg_prop_skip when it is not installed, so the area runs whatever the
 * server offers and cleanly skips the rest -- it never fails merely because a
 * PL is absent.
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

/* Compare a scalar query's value to an expected integer, as text. */
static void
require_scalar_eq(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql,
				  long expected)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *v;
	long		got;

	v = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "PL query failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql);
	got = v ? atol(v) : 0;
	if (got != expected)
		pg_prop_fail(tc, "PL result %ld != expected %ld\n  SQL: %s",
					 got, expected, sql);
	if (v)
		pg_free(v);
	if (err)
		pg_free(err);
}

/* Draw two small ints and the pure-SQL value of a*b + (a - b). */
static void
draw_inputs(pg_prop_tc *tc, int *a, int *b, long *expect)
{
	*a = (int) pg_prop_draw_int(tc, -1000, 1000);
	*b = (int) pg_prop_draw_int(tc, -1000, 1000);
	*expect = (long) (*a) * (*b) + ((long) (*a) - (long) (*b));
}

/* PL/pgSQL: always present (it is an un-droppable default language). */
PG_PROP(plpgsql_matches_sql)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	long		expect;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	if (!pg_prop_require_extension(conn, "plpgsql"))
		pg_prop_skip(tc, "plpgsql not available");

	if (!pg_prop_sql_exec(conn,
						  "CREATE OR REPLACE FUNCTION pl_f(a int, b int) "
						  "RETURNS bigint LANGUAGE plpgsql AS "
						  "$$ BEGIN RETURN a::bigint * b + (a - b); END $$;",
						  &err))
		pg_prop_fail(tc, "plpgsql function create failed: %s", err ? err : "");

	draw_inputs(tc, &a, &b, &expect);
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT pl_f(%d, %d)", a, b);
	require_scalar_eq(tc, conn, q.data, expect);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* PL/Python (plpython3u). */
PG_PROP(plpython_matches_sql)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	long		expect;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	if (!pg_prop_require_extension(conn, "plpython3u"))
		pg_prop_skip(tc, "plpython3u not available");

	if (!pg_prop_sql_exec(conn,
						  "CREATE OR REPLACE FUNCTION pl_f(a int, b int) "
						  "RETURNS bigint LANGUAGE plpython3u AS "
						  "$$ return a * b + (a - b) $$;",
						  &err))
		pg_prop_fail(tc, "plpython function create failed: %s", err ? err : "");

	draw_inputs(tc, &a, &b, &expect);
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT pl_f(%d, %d)", a, b);
	require_scalar_eq(tc, conn, q.data, expect);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* PL/Perl. */
PG_PROP(plperl_matches_sql)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	long		expect;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	if (!pg_prop_require_extension(conn, "plperl"))
		pg_prop_skip(tc, "plperl not available");

	if (!pg_prop_sql_exec(conn,
						  "CREATE OR REPLACE FUNCTION pl_f(a int, b int) "
						  "RETURNS bigint LANGUAGE plperl AS "
						  "$$ my ($a, $b) = @_; return $a * $b + ($a - $b); $$;",
						  &err))
		pg_prop_fail(tc, "plperl function create failed: %s", err ? err : "");

	draw_inputs(tc, &a, &b, &expect);
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT pl_f(%d, %d)", a, b);
	require_scalar_eq(tc, conn, q.data, expect);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/* PL/Tcl. */
PG_PROP(pltcl_matches_sql)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a,
				b;
	long		expect;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	if (!pg_prop_require_extension(conn, "pltcl"))
		pg_prop_skip(tc, "pltcl not available");

	if (!pg_prop_sql_exec(conn,
						  "CREATE OR REPLACE FUNCTION pl_f(a int, b int) "
						  "RETURNS bigint LANGUAGE pltcl AS "
						  "$$ return [expr {$1 * $2 + ($1 - $2)}] $$;",
						  &err))
		pg_prop_fail(tc, "pltcl function create failed: %s", err ? err : "");

	draw_inputs(tc, &a, &b, &expect);
	initStringInfo(&q);
	appendStringInfo(&q, "SELECT pl_f(%d, %d)", a, b);
	require_scalar_eq(tc, conn, q.data, expect);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
