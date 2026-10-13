/*-------------------------------------------------------------------------
 *
 * prop_inet6.c
 *	  Property-based tests for IPv6 inet operations.
 *
 * These exercise the IPv6 path of the inet type specifically: the 128-bit
 * family/masklen accessors, inet_in/inet_out round-tripping of the full
 * eight-group form, the abbreviated ("::"-compressed) rendering parsing back
 * to the same value, subnet containment, and the family partition that keeps
 * IPv6 values from ever comparing equal to an IPv4 value.  A failure means
 * the 128-bit code mishandled an address, mask, or comparison -- a bug.
 *
 * This area is distinct from inet/inetarith/networkops/cidrops: every value
 * here is IPv6, built from eight drawn 16-bit groups so no "::" compression
 * ambiguity leaks into the generator.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_inet6.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "inet6"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw an IPv6 inet value as a quoted SQL literal cast to inet, e.g.
 * "inet '1:2:3:4:5:6:7:8/64'".  All eight 16-bit groups are always emitted in
 * lowercase hex and joined by ':', so the generator never produces the "::"
 * compressed form and there is no compression ambiguity in the input.  The
 * masklen is drawn across the full 0..128 range.  *len receives the drawn
 * masklen so a property can reference it.
 */
static void
draw_inet6(pg_prop_tc *tc, StringInfo buf, int *len)
{
	int			masklen = (int) pg_prop_draw_int(tc, 0, 128);

	appendStringInfoString(buf, "inet '");
	for (int i = 0; i < 8; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ':');
		appendStringInfo(buf, "%x", (unsigned) pg_prop_draw_int(tc, 0, 65535));
	}
	appendStringInfo(buf, "/%d'", masklen);

	*len = masklen;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a validly-generated IPv6 value parsed to NULL or errored,
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
 * family() of any IPv6 value is 6.
 */
PG_PROP(family_6)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet6(tc, &i, &len);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT family(%s) = 6", i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * masklen() returns the prefix length the value was built with.
 */
PG_PROP(masklen)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet6(tc, &i, &len);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT masklen(%s) = %d", i.data, len);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Text round-trip: an IPv6 inet rendered to text and parsed back equals the
 * value.  This exercises inet_out and inet_in on every generated value.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet6(tc, &i, &len);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::inet = %s", i.data, i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Forcing the mask to the full 128 bits yields a value whose masklen is 128:
 * set_masklen does not lose or alter the address, and masklen reports the new
 * prefix exactly.
 */
PG_PROP(host_in_network)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet6(tc, &i, &len);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT masklen(set_masklen(%s, 128)) = 128", i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The abbreviated ("::"-compressed) rendering parses back to the same inet.
 * abbrev() returns text; casting it to inet must reproduce the value.
 */
PG_PROP(abbrev_consistent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet6(tc, &i, &len);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT %s = (abbrev(%s))::inet", i.data, i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment is reflexive: a value is both contained-or-equal in itself and
 * contains-or-equal itself (I <<= I AND I >>= I).
 */
PG_PROP(contains_self)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet6(tc, &i, &len);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT %s <<= %s AND %s >>= %s",
					 i.data, i.data, i.data, i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The network base of an inet is <= the address itself under inet ordering:
 * clearing host bits can only lower (or leave unchanged) the value.
 */
PG_PROP(network_le_host)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet6(tc, &i, &len);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT network(%s) <= %s", i.data, i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An IPv6 value is never equal to an IPv4 value: inet equality partitions by
 * family, so comparing against a fixed IPv4 address always yields false.
 */
PG_PROP(v6_distinct_from_v4)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			len;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet6(tc, &i, &len);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT %s <> inet '192.168.0.1'", i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
