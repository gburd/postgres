/*-------------------------------------------------------------------------
 *
 * prop_boolcast.c
 *	  Property-based tests for boolean text I/O and cast variants.
 *
 * The boolean type accepts many spellings on input ('true'/'yes'/'on'/'1'
 * and 'false'/'no'/'off'/'0', case-insensitively) and casts to and from int4
 * (true<->1, false<->0).  boolin/boolout and the bool<->int4 casts implement
 * this; a bug there shows up as a spelling that no longer parses to the right
 * value, a broken bool<->int round-trip, or a text round-trip that does not
 * recover the input.  These oracles are exact fixed-input identities (plus a
 * drawn 0/1 for the int->bool direction), so any "f" is a bug, not a tolerance
 * artefact.  This area is distinct from booltest (IS TRUE/FALSE/UNKNOWN) and
 * boolexpr (AND/OR/NOT short-circuit algebra): it targets only I/O and casts.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_boolcast.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "boolcast"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a cast or I/O conversion errored or produced NULL on an
 * input that must succeed, which is itself a bug -- so either is a failure,
 * reported with the SQL.  (Copied from prop_numeric.c's require_true.)
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	}
	if (res == NULL)
	{
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	}
	if (strcmp(res, "t") != 0)
	{
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	}
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * All the accepted "true" and "false" spellings parse to the same boolean
 * value.  'true'/'yes'/'on'/'1' are true; 'false'/'no'/'off'/'0' are false.
 * Exercises boolin's spelling table directly.
 */
PG_PROP(bool_text_variants)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT 'true'::bool = 't'::bool "
				 "AND 'yes'::bool = 'true'::bool "
				 "AND 'on'::bool = 'true'::bool "
				 "AND '1'::bool = 'true'::bool "
				 "AND 'false'::bool = 'f'::bool "
				 "AND 'no'::bool = 'false'::bool "
				 "AND 'off'::bool = 'false'::bool "
				 "AND '0'::bool = 'false'::bool");

	pg_prop_disconnect(conn);
}

/*
 * bool -> int4 cast: true is 1, false is 0.  Exercises the bool->int4 cast.
 */
PG_PROP(bool_to_int)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT 'true'::bool::int = 1 "
				 "AND 'false'::bool::int = 0");

	pg_prop_disconnect(conn);
}

/*
 * int4 -> bool cast: 0 is false, nonzero is true.  Drawn n in {0,1} keeps the
 * oracle on the two values PostgreSQL round-trips cleanly; the generic rule
 * (0 false, nonzero true) is checked against the n=0 vs n<>0 split.
 */
PG_PROP(int_to_bool)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_bool(tc) ? 1 : 0;

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (CASE WHEN %d = 0 THEN false ELSE true END) "
					 "= ((%d)::int::bool)",
					 n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * bool text round-trip: boolout renders 'true'/'false', and reparsing the
 * output recovers the value.  b::text::bool = b for both booleans.
 */
PG_PROP(bool_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT 'true'::bool::text = 'true' "
				 "AND 'false'::bool::text = 'false' "
				 "AND 'true'::bool::text::bool = 'true'::bool "
				 "AND 'false'::bool::text::bool = 'false'::bool");

	pg_prop_disconnect(conn);
}

/*
 * NOT toggles: NOT true is false, NOT false is true.
 */
PG_PROP(not_toggles)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT (NOT 'true'::bool) = 'false'::bool "
				 "AND (NOT 'false'::bool) = 'true'::bool");

	pg_prop_disconnect(conn);
}

/*
 * AND/OR identity elements: (b AND true) = b and (b OR false) = b for both
 * booleans.
 */
PG_PROP(bool_and_or_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT ('true'::bool AND 'true'::bool) = 'true'::bool "
				 "AND ('false'::bool AND 'true'::bool) = 'false'::bool "
				 "AND ('true'::bool OR 'false'::bool) = 'true'::bool "
				 "AND ('false'::bool OR 'false'::bool) = 'false'::bool");

	pg_prop_disconnect(conn);
}

/*
 * Input is case-insensitive: 'TRUE'/'False'/'YES' parse to the expected
 * values.  Exercises boolin's case folding.
 */
PG_PROP(case_insensitive_input)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT 'TRUE'::bool = 'true'::bool "
				 "AND 'False'::bool = 'false'::bool "
				 "AND 'YES'::bool = 'true'::bool");

	pg_prop_disconnect(conn);
}
