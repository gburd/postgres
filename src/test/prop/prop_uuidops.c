/*-------------------------------------------------------------------------
 *
 * prop_uuidops.c
 *	  Property-based tests for uuid ordering, text round-trip and comparison.
 *
 * The oracle is the uuid type's own algebra: output is the canonical
 * lowercase 8-4-4-4-12 text, comparison is a total order equal to the
 * lexicographic order of that canonical text, and text <-> uuid round-trips
 * exactly.  A failure means uuid in/out or comparison disagrees with the
 * documented canonical form -- a bug in the type, not a tolerance artefact.
 * This complements the "uuid" area (byte-oriented) by driving uuids built
 * from drawn hex nibbles in the canonical dashed form, and by asserting the
 * order-matches-text and nil-is-minimum laws.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_uuidops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "uuidops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw 32 hex nibbles (0-9a-f) and format them into the canonical dashed
 * 8-4-4-4-12 uuid form into "buf" (at least 37 bytes).  Lowercase hex.
 */
static void
draw_uuid_canon(pg_prop_tc *tc, char *buf)
{
	static const char hexdig[16] = "0123456789abcdef";
	int			i;
	int			p = 0;

	for (i = 0; i < 32; i++)
	{
		if (i == 8 || i == 12 || i == 16 || i == 20)
			buf[p++] = '-';
		buf[p++] = hexdig[(int) pg_prop_draw_int(tc, 0, 15)];
	}
	buf[p] = '\0';
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is itself a bug (uuid I/O or comparison should never error or
 * yield NULL on these finite, valid inputs), so either is a failure.
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
 * text_roundtrip: (U)::text::uuid = U.  Casting a uuid out to text and back
 * must reproduce the same uuid.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		u[37];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_canon(tc, u);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::uuid)::text::uuid = '%s'::uuid",
					 u, u);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * case_insensitive: the lower-case and upper-case hex forms of one uuid parse
 * to the same uuid.  Build the upper-case form in C so the two literals
 * differ only in letter case.
 */
PG_PROP(case_insensitive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		lo[37];
	char		up[37];
	int			i;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_canon(tc, lo);
	for (i = 0; lo[i] != '\0'; i++)
	{
		if (lo[i] >= 'a' && lo[i] <= 'f')
			up[i] = (char) (lo[i] - 'a' + 'A');
		else
			up[i] = lo[i];
	}
	up[i] = '\0';

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT '%s'::uuid = '%s'::uuid",
					 lo, up);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * total_order: for two drawn uuids A, B exactly one of A < B, A = B, A > B
 * holds.  (num of true among the three predicates) = 1.
 */
PG_PROP(total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		a[37];
	char		b[37];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_canon(tc, a);
	draw_uuid_canon(tc, b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (('%s'::uuid < '%s'::uuid)::int "
					 "+ ('%s'::uuid = '%s'::uuid)::int "
					 "+ ('%s'::uuid > '%s'::uuid)::int) = 1",
					 a, b, a, b, a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * self_equal: U = U AND NOT (U <> U).  Reflexivity of equality and its
 * agreement with the <> operator.
 */
PG_PROP(self_equal)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		u[37];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_canon(tc, u);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::uuid = '%s'::uuid) "
					 "AND NOT ('%s'::uuid <> '%s'::uuid)",
					 u, u, u, u);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * order_matches_text: (A < B) = (A::text < B::text).  uuid comparison is the
 * lexicographic order of the canonical lowercase hex text; since output is
 * always canonical lowercase, the uuid order must agree with text order.
 */
PG_PROP(order_matches_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		a[37];
	char		b[37];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_canon(tc, a);
	draw_uuid_canon(tc, b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::uuid < '%s'::uuid) "
					 "= (('%s'::uuid)::text < ('%s'::uuid)::text)",
					 a, b, a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * nil_is_min: the nil uuid (all zero) is <= any drawn uuid.  The all-zero
 * value is the minimum of the total order.
 */
PG_PROP(nil_is_min)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		u[37];
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_canon(tc, u);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT '00000000-0000-0000-0000-000000000000'::uuid "
					 "<= '%s'::uuid",
					 u);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
