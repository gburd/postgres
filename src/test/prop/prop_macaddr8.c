/*-------------------------------------------------------------------------
 *
 * prop_macaddr8.c
 *	  Property-based tests for macaddr8 (EUI-64) algebra and conversions.
 *
 * These cover the macaddr8-specific operations that prop_macaddr.c does not:
 * trunc() (zeroing the trailing five bytes), macaddr8_set7bit() (idempotent
 * OR of 0x02 into the first byte), the EUI-48 -> EUI-64 insertion of ff:fe
 * when a 6-byte macaddr is cast to macaddr8, and that cast's reversibility.
 * The oracle is each function's documented definition in mac8.c; a failure
 * is a bug in trunc/set7bit/the conversion operators, not a rounding effect.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_macaddr8.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "macaddr8"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw 8 random bytes and render them as a 16-character lowercase hex string
 * (no separators) into "hex", a caller-provided buffer of at least 17 bytes.
 * Each byte is drawn with pg_prop_draw_int(0, 255) and printed "%02x", so the
 * result is always fixed-width and lowercase.  This is the EUI-64 value a
 * macaddr8 is built from.
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
 * Draw 6 random bytes and render them as a 12-character lowercase hex string
 * (no separators) into "hex", a caller-provided buffer of at least 13 bytes.
 * This is the EUI-48 value a macaddr is built from.
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
 * Colon form of an n-byte hex string (2n hex characters): xx:xx:...:xx,
 * appended to buf.  Separators go between each pair of hex characters.
 */
static void
append_colon(StringInfo buf, const char *hex, int nbytes)
{
	int			i;

	for (i = 0; i < nbytes; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ':');
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
 * text_roundtrip: a drawn macaddr8 survives a text round trip.
 * (M)::text::macaddr8 = M.  Exercises macaddr8_in, macaddr8_out, macaddr8_in.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[17];
	StringInfoData m,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac8(tc, hex);
	initStringInfo(&m);
	append_colon(&m, hex, 8);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::macaddr8)::text::macaddr8 = '%s'::macaddr8",
					 m.data, m.data);

	require_true(tc, conn, q.data);

	pfree(m.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * total_order: for two independently drawn macaddr8s exactly one of <, =, >
 * holds, so the sum of those three booleans cast to int is 1 (trichotomy).
 */
PG_PROP(total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex1[17],
				hex2[17];
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac8(tc, hex1);
	draw_mac8(tc, hex2);
	initStringInfo(&a);
	initStringInfo(&b);
	append_colon(&a, hex1, 8);
	append_colon(&b, hex2, 8);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (('%s'::macaddr8 < '%s'::macaddr8)::int "
					 "+ ('%s'::macaddr8 = '%s'::macaddr8)::int "
					 "+ ('%s'::macaddr8 > '%s'::macaddr8)::int) = 1",
					 a.data, b.data, a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * trunc_zeros_tail: trunc(M) keeps the first three bytes and zeroes the last
 * five.  Checked against the reconstructed value (first 3 bytes followed by
 * five zero bytes), so this pins the exact manufacturer-prefix semantics of
 * macaddr8_trunc rather than just the printed suffix.
 */
PG_PROP(trunc_zeros_tail)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[17];
	StringInfoData m,
				pfx,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac8(tc, hex);
	initStringInfo(&m);
	append_colon(&m, hex, 8);

	/* first three bytes in colon form, then five explicit zero bytes */
	initStringInfo(&pfx);
	append_colon(&pfx, hex, 3);
	appendStringInfoString(&pfx, ":00:00:00:00:00");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT trunc('%s'::macaddr8) = '%s'::macaddr8",
					 m.data, pfx.data);

	require_true(tc, conn, q.data);

	pfree(m.data);
	pfree(pfx.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * set7bit_idempotent: macaddr8_set7bit ORs 0x02 into the first byte, so
 * applying it twice equals applying it once.
 */
PG_PROP(set7bit_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[17];
	StringInfoData m,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac8(tc, hex);
	initStringInfo(&m);
	append_colon(&m, hex, 8);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT macaddr8_set7bit(macaddr8_set7bit('%s'::macaddr8)) "
					 "= macaddr8_set7bit('%s'::macaddr8)",
					 m.data, m.data);

	require_true(tc, conn, q.data);

	pfree(m.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * self_equal: a drawn macaddr8 equals itself (reflexivity of equality).
 */
PG_PROP(self_equal)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[17];
	StringInfoData m,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac8(tc, hex);
	initStringInfo(&m);
	append_colon(&m, hex, 8);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT '%s'::macaddr8 = '%s'::macaddr8",
					 m.data, m.data);

	require_true(tc, conn, q.data);

	pfree(m.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * macaddr_to_macaddr8: casting a 6-byte macaddr to macaddr8 inserts ff:fe as
 * the 4th and 5th bytes (the standard EUI-48 -> EUI-64 mapping), and that
 * cast is reversible, so (MA::macaddr8)::macaddr = MA.  The round trip pins
 * both halves of the conversion at once.
 */
PG_PROP(macaddr_to_macaddr8)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex[13];
	StringInfoData ma,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	draw_mac6(tc, hex);
	initStringInfo(&ma);
	append_colon(&ma, hex, 6);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT position(':ff:fe:' in ('%s'::macaddr8)::text) > 0 "
					 "AND (('%s'::macaddr)::macaddr8)::macaddr = '%s'::macaddr",
					 ma.data, ma.data, ma.data);

	require_true(tc, conn, q.data);

	pfree(ma.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * order_matches_bytes: for two macaddr8s that differ only in the first byte,
 * the one with the smaller first byte compares less.  Draw b1 < b1' and share
 * the remaining seven bytes so the comparison is decided at byte one.
 */
PG_PROP(order_matches_bytes)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	char		hex1[17],
				hex2[17];
	int			b1,
				b1p,
				t;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* draw two distinct first bytes with b1 < b1p */
	b1 = (int) pg_prop_draw_int(tc, 0, 254);
	b1p = (int) pg_prop_draw_int(tc, b1 + 1, 255);

	/* build two 8-byte hex strings sharing the seven trailing bytes */
	snprintf(hex1, 3, "%02x", b1);
	snprintf(hex2, 3, "%02x", b1p);
	for (t = 1; t < 8; t++)
	{
		int			byte = (int) pg_prop_draw_int(tc, 0, 255);

		snprintf(hex1 + t * 2, 3, "%02x", byte);
		memcpy(hex2 + t * 2, hex1 + t * 2, 2);
	}
	hex1[16] = '\0';
	hex2[16] = '\0';

	initStringInfo(&a);
	append_colon(&a, hex1, 8);

	initStringInfo(&b);
	append_colon(&b, hex2, 8);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT '%s'::macaddr8 < '%s'::macaddr8",
					 a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
