/*-------------------------------------------------------------------------
 *
 * prop_uuid.c
 *	  Property-based tests for the uuid type.
 *
 * uuid in/out parsing (dashes, braces, case) and ordering have had fixes;
 * these check that parsing is tolerant and canonicalizing, and that
 * comparison is a total order consistent with the byte string.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_uuid.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "uuid"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw 16 random bytes and render them as a 32-character lowercase hex string
 * (no dashes) into "hex", a caller-provided buffer of at least 33 bytes.  Each
 * byte is drawn with pg_prop_draw_int(0, 255) and printed "%02x", so the
 * result is always fixed-width and lowercase.
 */
static void
draw_uuid_hex(pg_prop_tc *tc, char *hex)
{
	int			i;

	for (i = 0; i < 16; i++)
	{
		int			b = (int) pg_prop_draw_int(tc, 0, 255);

		snprintf(hex + i * 2, 3, "%02x", b);
	}
	hex[32] = '\0';
}

/*
 * Insert dashes into a 32-char hex string to form the canonical 8-4-4-4-12
 * dashed UUID text, appended to buf.  Dashes go after hex positions 8, 12, 16
 * and 20.
 */
static void
append_dashed(StringInfo buf, const char *hex)
{
	appendBinaryStringInfo(buf, hex, 8);
	appendStringInfoChar(buf, '-');
	appendBinaryStringInfo(buf, hex + 8, 4);
	appendStringInfoChar(buf, '-');
	appendBinaryStringInfo(buf, hex + 12, 4);
	appendStringInfoChar(buf, '-');
	appendBinaryStringInfo(buf, hex + 16, 4);
	appendStringInfoChar(buf, '-');
	appendBinaryStringInfo(buf, hex + 20, 12);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a validly-generated value parsed to NULL or errored, which
 * is itself a bug -- so either is a failure, reported with the SQL.
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
 * Text round-trip: parsing a canonical dashed uuid and printing it yields the
 * lowercase canonical form.  Since the drawn literal U is already lowercase
 * canonical, (U::uuid)::text must equal lower(U).  This exercises uuid_in then
 * uuid_out.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[33];
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_hex(tc, hex);
	initStringInfo(&d);
	append_dashed(&d, hex);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::uuid)::text = lower('%s')",
					 d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Dash-insensitive parse: uuid_in accepts both the 32-hex form without dashes
 * and the standard dashed form, and they denote the same value.  So H::uuid =
 * D::uuid where H and D come from the same bytes.
 */
PG_PROP(dash_insensitive_parse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[33];
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_hex(tc, hex);
	initStringInfo(&d);
	append_dashed(&d, hex);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::uuid) = ('%s'::uuid)",
					 hex, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Case-insensitive parse: uuid_in treats the hex digits case-insensitively, so
 * the lowercase dashed form and its uppercase version denote the same value.
 * upper() produces the uppercase form in SQL so the C side keeps one encoding.
 */
PG_PROP(case_insensitive_parse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[33];
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_hex(tc, hex);
	initStringInfo(&d);
	append_dashed(&d, hex);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::uuid) = (upper('%s')::uuid)",
					 d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Brace-form parse: uuid_in accepts the {dashed} braced form and it denotes
 * the same value as the bare dashed form.
 */
PG_PROP(brace_form_parse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[33];
	StringInfoData d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_hex(tc, hex);
	initStringInfo(&d);
	append_dashed(&d, hex);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::uuid) = ('{%s}'::uuid)",
					 d.data, d.data);

	require_true(tc, conn, q.data);

	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Total order: for two independently drawn uuids exactly one of <, =, > holds,
 * so the sum of those three booleans cast to int is 1.  This checks uuid
 * comparison is a total order (trichotomy).
 */
PG_PROP(comparison_total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex1[33],
				hex2[33];
	StringInfoData d1,
				d2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_hex(tc, hex1);
	draw_uuid_hex(tc, hex2);
	initStringInfo(&d1);
	initStringInfo(&d2);
	append_dashed(&d1, hex1);
	append_dashed(&d2, hex2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (('%s'::uuid < '%s'::uuid)::int "
					 "+ ('%s'::uuid = '%s'::uuid)::int "
					 "+ ('%s'::uuid > '%s'::uuid)::int) = 1",
					 d1.data, d2.data, d1.data, d2.data, d1.data, d2.data);

	require_true(tc, conn, q.data);

	pfree(d1.data);
	pfree(d2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Order matches the hex string: uuid comparison must agree with the
 * lexicographic comparison of the canonical lowercase 32-char hex (no dashes).
 * Both hex strings are lowercase fixed-width, so SQL text "<" is the right
 * oracle, and the dashed and raw forms are built from the same bytes.
 */
PG_PROP(order_matches_hex_string)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex1[33],
				hex2[33];
	StringInfoData d1,
				d2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_uuid_hex(tc, hex1);
	draw_uuid_hex(tc, hex2);
	initStringInfo(&d1);
	initStringInfo(&d2);
	append_dashed(&d1, hex1);
	append_dashed(&d2, hex2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::uuid < '%s'::uuid) = ('%s' < '%s')",
					 d1.data, d2.data, hex1, hex2);

	require_true(tc, conn, q.data);

	pfree(d1.data);
	pfree(d2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
