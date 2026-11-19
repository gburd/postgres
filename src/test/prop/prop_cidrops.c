/*-------------------------------------------------------------------------
 *
 * prop_cidrops.c
 *	  Property-based tests for the cidr type: normalization and containment.
 *
 * The cidr type differs from inet in that it zeroes host bits (a cidr is a
 * network, not an address): 'a.b.c.d/len'::cidr normalizes to the /len
 * network of a.b.c.d.  These properties check that normalization, the network
 * containment hierarchy (a /len contains the matching /(len+1) and every host
 * in the network), masklen/family accessors, text round-trip, and the
 * network<=broadcast ordering.  A failure means cidr I/O, network(),
 * set_masklen(), masklen(), or the containment operators mis-handle an IPv4
 * address/masklen pair -- a correctness bug in the network type.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_cidrops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cidrops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw an IPv4 "a.b.c.d/len" string (no quotes, no cast) into buf, with each
 * octet 0..255 and len 0..32.  Also return len via *len so properties that
 * need the numeric prefix length (len, len+1) can build their SQL.
 */
static void
draw_ipslash(pg_prop_tc *tc, StringInfo buf, int *len)
{
	int			a = (int) pg_prop_draw_int(tc, 0, 255);
	int			b = (int) pg_prop_draw_int(tc, 0, 255);
	int			c = (int) pg_prop_draw_int(tc, 0, 255);
	int			d = (int) pg_prop_draw_int(tc, 0, 255);
	int			l = (int) pg_prop_draw_int(tc, 0, 32);

	appendStringInfo(buf, "%d.%d.%d.%d/%d", a, b, c, d, l);
	*len = l;
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
 * A cidr with host bits set normalizes to its /len network: casting the
 * ip/len string to cidr equals rebuilding the cidr from network() of the same
 * value as an inet.  This is the defining property of cidr -- it zeroes host
 * bits -- so both sides must denote the same network.
 */
PG_PROP(cidr_normalizes)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ip,
				q;
	int			len;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ip);
	draw_ipslash(tc, &ip, &len);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT set_masklen(('%s')::inet, %d)::cidr = "
					 "network(set_masklen(('%s')::inet, %d))::cidr",
					 ip.data, len, ip.data, len);

	require_true(tc, conn, q.data);

	pfree(ip.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The /len network contains the host it was built from: the normalized cidr
 * contains-or-equals ('ip/32')::inet (the host address with a full mask).
 */
PG_PROP(cidr_contains_hosts)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ip,
				q;
	int			len;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ip);
	draw_ipslash(tc, &ip, &len);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT set_masklen(('%s')::inet, %d)::cidr >>= "
					 "(split_part('%s', '/', 1) || '/32')::inet",
					 ip.data, len, ip.data);

	require_true(tc, conn, q.data);

	pfree(ip.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * masklen() of the normalized cidr equals the drawn prefix length.
 */
PG_PROP(masklen_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ip,
				q;
	int			len;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ip);
	draw_ipslash(tc, &ip, &len);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT masklen(set_masklen(('%s')::inet, %d)::cidr) = %d",
					 ip.data, len, len);

	require_true(tc, conn, q.data);

	pfree(ip.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A /len network contains the matching /(len+1) sub-network: the two cidrs
 * built from the same address by set_masklen to len and len+1 are in the
 * containment relation (>>=) for every len < 32.  (At len = 32 there is no
 * narrower network, so the property is vacuously skipped by regenerating a
 * len in 0..31 for this check.)
 */
PG_PROP(broader_contains_narrower)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ip,
				q;
	int			a,
				b,
				c,
				d,
				len;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* draw len in 0..31 so len+1 is a valid IPv4 prefix (<= 32) */
	a = (int) pg_prop_draw_int(tc, 0, 255);
	b = (int) pg_prop_draw_int(tc, 0, 255);
	c = (int) pg_prop_draw_int(tc, 0, 255);
	d = (int) pg_prop_draw_int(tc, 0, 255);
	len = (int) pg_prop_draw_int(tc, 0, 31);

	initStringInfo(&ip);
	appendStringInfo(&ip, "%d.%d.%d.%d/%d", a, b, c, d, len);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT set_masklen(('%s')::inet, %d)::cidr "
					 ">>= set_masklen(('%s')::inet, %d)::cidr",
					 ip.data, len, ip.data, len + 1);

	require_true(tc, conn, q.data);

	pfree(ip.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Text round-trip: a cidr rendered to text and parsed back equals the value.
 * Exercises both cidr_out and cidr_in on every generated value.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ip,
				q;
	int			len;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ip);
	draw_ipslash(tc, &ip, &len);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (set_masklen(('%s')::inet, %d)::cidr)::text::cidr = "
					 "set_masklen(('%s')::inet, %d)::cidr",
					 ip.data, len, ip.data, len);

	require_true(tc, conn, q.data);

	pfree(ip.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * family() of an IPv4 cidr is 4.
 */
PG_PROP(family_4)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ip,
				q;
	int			len;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ip);
	draw_ipslash(tc, &ip, &len);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT family(set_masklen(('%s')::inet, %d)::cidr) = 4",
					 ip.data, len);

	require_true(tc, conn, q.data);

	pfree(ip.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The cidr's ordering as inet is consistent: the network address is <= the
 * broadcast address of the same network.
 */
PG_PROP(cidr_le_inet_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ip,
				q;
	int			len;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ip);
	draw_ipslash(tc, &ip, &len);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT network(('%s')::inet) <= broadcast(('%s')::inet)",
					 ip.data, ip.data);

	require_true(tc, conn, q.data);

	pfree(ip.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Host-count/masklen bound: the number of addresses in a /len network,
 * 2^(32-len), is at least 1, and forcing the mask to /32 with set_masklen
 * yields a single-host network whose masklen is 32.
 */
PG_PROP(host_count_bound)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData ip,
				q;
	int			len;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&ip);
	draw_ipslash(tc, &ip, &len);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (2::numeric ^ (32 - %d)) >= 1 "
					 "AND masklen(set_masklen(('%s')::inet, 32)) = 32",
					 len, ip.data);

	require_true(tc, conn, q.data);

	pfree(ip.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
