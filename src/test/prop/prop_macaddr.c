/*-------------------------------------------------------------------------
 *
 * prop_macaddr.c
 *	  Property-based tests for the macaddr and macaddr8 types.
 *
 * macaddr/macaddr8 parsing accepts several input formats (colon, dash,
 * dotted, bare) and must canonicalize them identically; comparison is a
 * byte-order total order.  These check format tolerance, text round-trip,
 * and ordering.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_macaddr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "macaddr"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw 6 random bytes and render them as a 12-character lowercase hex string
 * (no separators) into "hex", a caller-provided buffer of at least 13 bytes.
 * Each byte is drawn with pg_prop_draw_int(0, 255) and printed "%02x", so the
 * result is always fixed-width and lowercase -- the common value from which
 * every input form (colon/dash/dotted/bare) is built, so equality across
 * forms is the expected answer.
 */
static void
draw_mac6(pg_prop_tc *tc, char *hex)
{
	int			i;

	for (i = 0; i < 6; i++)
	{
		int			b = (int) pg_prop_draw_int(tc, 0, 255);

		snprintf(hex + i * 2, 3, "%02x", b);
	}
	hex[12] = '\0';
}

/*
 * Draw 8 random bytes and render them as a 16-character lowercase hex string
 * (no separators) into "hex", a caller-provided buffer of at least 17 bytes.
 * This is the EUI-64 value a macaddr8 is built from.
 */
static void
draw_mac8(pg_prop_tc *tc, char *hex)
{
	int			i;

	for (i = 0; i < 8; i++)
	{
		int			b = (int) pg_prop_draw_int(tc, 0, 255);

		snprintf(hex + i * 2, 3, "%02x", b);
	}
	hex[16] = '\0';
}

/*
 * Colon form of a 12-char hex string: xx:xx:xx:xx:xx:xx, appended to buf.
 * Separators go between each pair of hex characters.
 */
static void
append_colon6(StringInfo buf, const char *hex)
{
	int			i;

	for (i = 0; i < 6; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ':');
		appendBinaryStringInfo(buf, hex + i * 2, 2);
	}
}

/*
 * Dash form of a 12-char hex string: xx-xx-xx-xx-xx-xx, appended to buf.
 */
static void
append_dash6(StringInfo buf, const char *hex)
{
	int			i;

	for (i = 0; i < 6; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, '-');
		appendBinaryStringInfo(buf, hex + i * 2, 2);
	}
}

/*
 * Dotted (Cisco) form of a 12-char hex string: xxxx.xxxx.xxxx, appended to
 * buf.  Four hex characters per group, three groups separated by dots.
 */
static void
append_dotted6(StringInfo buf, const char *hex)
{
	int			i;

	for (i = 0; i < 3; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, '.');
		appendBinaryStringInfo(buf, hex + i * 4, 4);
	}
}

/*
 * Colon form of a 16-char hex string (8 bytes, EUI-64):
 * xx:xx:xx:xx:xx:xx:xx:xx, appended to buf.
 */
static void
append_colon8(StringInfo buf, const char *hex)
{
	int			i;

	for (i = 0; i < 8; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ':');
		appendBinaryStringInfo(buf, hex + i * 2, 2);
	}
}

/*
 * Dash form of a 16-char hex string (8 bytes, EUI-64):
 * xx-xx-xx-xx-xx-xx-xx-xx, appended to buf.
 */
static void
append_dash8(StringInfo buf, const char *hex)
{
	int			i;

	for (i = 0; i < 8; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, '-');
		appendBinaryStringInfo(buf, hex + i * 2, 2);
	}
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
 * Text round-trip: parsing a canonical colon-form macaddr and printing it
 * yields a value that re-parses equal.  (C::macaddr)::text::macaddr =
 * C::macaddr.  This exercises macaddr_in then macaddr_out then macaddr_in.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[13];
	StringInfoData c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac6(tc, hex);
	initStringInfo(&c);
	append_colon6(&c, hex);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::macaddr)::text::macaddr = '%s'::macaddr",
					 c.data, c.data);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Colon equals dash: the colon-separated and dash-separated input forms of
 * the same six bytes denote the same macaddr, since macaddr_in canonicalizes
 * both to the same value.
 */
PG_PROP(colon_equals_dash)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[13];
	StringInfoData c,
				d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac6(tc, hex);
	initStringInfo(&c);
	initStringInfo(&d);
	append_colon6(&c, hex);
	append_dash6(&d, hex);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::macaddr) = ('%s'::macaddr)",
					 c.data, d.data);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Colon equals dotted: the Cisco dotted form (xxxx.xxxx.xxxx) of the same six
 * bytes denotes the same macaddr as the colon form.
 */
PG_PROP(colon_equals_dotted)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[13];
	StringInfoData c,
				dot,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac6(tc, hex);
	initStringInfo(&c);
	initStringInfo(&dot);
	append_colon6(&c, hex);
	append_dotted6(&dot, hex);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::macaddr) = ('%s'::macaddr)",
					 c.data, dot.data);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(dot.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Colon equals bare: the bare 12-hex form (no separators) of the same six
 * bytes denotes the same macaddr as the colon form.
 */
PG_PROP(colon_equals_bare)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[13];
	StringInfoData c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac6(tc, hex);
	initStringInfo(&c);
	append_colon6(&c, hex);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::macaddr) = ('%s'::macaddr)",
					 c.data, hex);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Total order: for two independently drawn macaddrs exactly one of <, =, >
 * holds, so the sum of those three booleans cast to int is 1.  This checks
 * macaddr comparison is a total order (trichotomy).
 */
PG_PROP(comparison_total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex1[13],
				hex2[13];
	StringInfoData c1,
				c2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac6(tc, hex1);
	draw_mac6(tc, hex2);
	initStringInfo(&c1);
	initStringInfo(&c2);
	append_colon6(&c1, hex1);
	append_colon6(&c2, hex2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (('%s'::macaddr < '%s'::macaddr)::int "
					 "+ ('%s'::macaddr = '%s'::macaddr)::int "
					 "+ ('%s'::macaddr > '%s'::macaddr)::int) = 1",
					 c1.data, c2.data, c1.data, c2.data, c1.data, c2.data);

	require_true(tc, conn, q.data);

	pfree(c1.data);
	pfree(c2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Order matches the hex string: macaddr comparison must agree with the
 * lexicographic comparison of the canonical lowercase 12-char hex (no
 * separators).  Both hex strings are lowercase fixed-width, so SQL text "<" is
 * the right oracle, and the colon and bare forms are built from the same
 * bytes.
 */
PG_PROP(order_matches_hex)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex1[13],
				hex2[13];
	StringInfoData c1,
				c2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac6(tc, hex1);
	draw_mac6(tc, hex2);
	initStringInfo(&c1);
	initStringInfo(&c2);
	append_colon6(&c1, hex1);
	append_colon6(&c2, hex2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::macaddr < '%s'::macaddr) = ('%s' < '%s')",
					 c1.data, c2.data, hex1, hex2);

	require_true(tc, conn, q.data);

	pfree(c1.data);
	pfree(c2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * macaddr8 text round-trip: parsing a canonical colon-form macaddr8 and
 * printing it yields a value that re-parses equal.  Exercises macaddr8_in,
 * macaddr8_out, macaddr8_in.
 */
PG_PROP(macaddr8_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[17];
	StringInfoData c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac8(tc, hex);
	initStringInfo(&c);
	append_colon8(&c, hex);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::macaddr8)::text::macaddr8 = '%s'::macaddr8",
					 c.data, c.data);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * macaddr8 colon equals dash: the colon-separated and dash-separated input
 * forms of the same eight bytes denote the same macaddr8, since macaddr8_in
 * canonicalizes both to the same value.
 */
PG_PROP(macaddr8_colon_equals_dash)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[17];
	StringInfoData c,
				d,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac8(tc, hex);
	initStringInfo(&c);
	initStringInfo(&d);
	append_colon8(&c, hex);
	append_dash8(&d, hex);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('%s'::macaddr8) = ('%s'::macaddr8)",
					 c.data, d.data);

	require_true(tc, conn, q.data);

	pfree(c.data);
	pfree(d.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
