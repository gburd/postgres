/*-------------------------------------------------------------------------
 *
 * prop_concatop.c
 *      Property-based tests for the || concatenation operator.
 *
 * The || operator is overloaded across text, array, jsonb and bytea, each
 * with its own implementation but a shared algebra: concatenation is
 * associative and length-additive (text/array/bytea), has an empty identity
 * element, and for jsonb objects is right-biased on key conflicts.  The
 * oracle is that algebra asserted directly in SQL; any "f" is a bug in one of
 * the concat implementations, not a rounding or representation artefact.
 *
 * Distinct from textfns/arrayops/jsonbops: those exercise per-type function
 * families; here the single cross-type operator and its associativity are
 * the target.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *      src/test/prop/prop_concatop.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "concatop"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operator errored or produced NULL on finite inputs,
 * which is itself a bug -- so either is a failure, reported with the SQL.
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
 * Draw a single-quoted text literal over the alphabet a-c, length 0..8.  No
 * embedded quotes are possible from this alphabet, so no doubling is needed.
 */
static void
draw_text(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 8);
	int			i;

	appendStringInfoChar(buf, '\'');
	for (i = 0; i < n; i++)
		appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
	appendStringInfoChar(buf, '\'');
}

/*
 * Draw an int4 array literal ARRAY[...]::int[] with 0..6 elements in -5..5.
 */
static void
draw_int_array(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 6);
	int			i;

	appendStringInfoString(buf, "ARRAY[");
	for (i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ',');
		appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, -5, 5));
	}
	appendStringInfoString(buf, "]::int[]");
}

/*
 * Draw a bytea literal decode('<hex>','hex') with 0..8 bytes.
 */
static void
draw_bytea(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 8);
	int			i;

	appendStringInfoString(buf, "decode('");
	for (i = 0; i < n; i++)
		appendStringInfo(buf, "%02x", (int) pg_prop_draw_int(tc, 0, 255));
	appendStringInfoString(buf, "','hex')");
}

/*
 * text || is associative: (a || b) || c = a || (b || c).
 */
PG_PROP(text_concat_associative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	draw_text(tc, &a);
	draw_text(tc, &b);
	draw_text(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s || %s) || %s) = (%s || (%s || %s))",
					 a.data, b.data, c.data, a.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * text || is length-additive: length(a || b) = length(a) + length(b).
 */
PG_PROP(text_concat_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_text(tc, &a);
	draw_text(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(%s || %s) = length(%s) + length(%s)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array || is associative: (a || b) || c = a || (b || c).
 */
PG_PROP(array_concat_associative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	initStringInfo(&c);
	draw_int_array(tc, &a);
	draw_int_array(tc, &b);
	draw_int_array(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s || %s) || %s) = (%s || (%s || %s))",
					 a.data, b.data, c.data, a.data, b.data, c.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array || is cardinality-additive:
 * cardinality(a || b) = cardinality(a) + cardinality(b).
 */
PG_PROP(array_concat_cardinality)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_int_array(tc, &a);
	draw_int_array(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(%s || %s) "
					 "= cardinality(%s) + cardinality(%s)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * jsonb || on objects is right-biased on key conflicts: the right operand's
 * value wins.  This is a fixed-shape oracle (no drawn inputs needed) that
 * pins the documented semantics.
 */
PG_PROP(jsonb_concat_right_wins)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT ((jsonb_build_object('k', 1) "
				 "|| jsonb_build_object('k', 2)) -> 'k') = '2'::jsonb");

	pg_prop_disconnect(conn);
}

/*
 * bytea || is octet-length-additive:
 * octet_length(a || b) = octet_length(a) + octet_length(b).
 */
PG_PROP(bytea_concat_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_bytea(tc, &a);
	draw_bytea(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT octet_length(%s || %s) "
					 "= octet_length(%s) + octet_length(%s)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * text || has the empty string as a two-sided identity:
 * a || '' = a AND '' || a = a.
 */
PG_PROP(text_concat_empty_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_text(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s || '') = %s AND ('' || %s) = %s",
					 a.data, a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * array || has the empty array as a two-sided identity:
 * a || ARRAY[]::int[] = a AND ARRAY[]::int[] || a = a.
 */
PG_PROP(array_concat_empty_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_int_array(tc, &a);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s || ARRAY[]::int[]) = %s "
					 "AND (ARRAY[]::int[] || %s) = %s",
					 a.data, a.data, a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
