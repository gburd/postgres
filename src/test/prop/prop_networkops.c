/*-------------------------------------------------------------------------
 *
 * prop_networkops.c
 *	  Property-based tests for inet/cidr network functions.
 *
 * Exercises the network-address functions distinct from the inet/inetarith
 * areas: host(), network(), broadcast(), netmask(), masklen(),
 * set_masklen(), family(), the text round-trip, and the containment
 * operators (<<=, >>=).  The oracle is each function's defining law --
 * set_masklen reports back the mask it set, network() has the host's own
 * masklen, an IPv4 address is family 4, broadcast is never below network,
 * text output re-parses to the same value, and every value contains itself --
 * so any "f" result is a bug in network.c, not a tolerance.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_networkops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "networkops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw an IPv4 host literal "a.b.c.d/len" into buf, each octet 0..255 and the
 * mask length 0..32.  No quoting is needed since the characters are all
 * digits and dots/slash; the caller wraps it in inet '...'.
 */
static void
draw_inet(pg_prop_tc *tc, StringInfo buf)
{
	int			a = (int) pg_prop_draw_int(tc, 0, 255);
	int			b = (int) pg_prop_draw_int(tc, 0, 255);
	int			c = (int) pg_prop_draw_int(tc, 0, 255);
	int			d = (int) pg_prop_draw_int(tc, 0, 255);
	int			len = (int) pg_prop_draw_int(tc, 0, 32);

	appendStringInfo(buf, "%d.%d.%d.%d/%d", a, b, c, d, len);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on valid inputs,
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
 * set_masklen reports back the mask it set: masklen(set_masklen(X, m)) = m,
 * for a drawn m in 0..32.  The host part of X is irrelevant to this, so m is
 * drawn independently of X's own mask.
 */
PG_PROP(host_in_its_network)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			m;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	m = (int) pg_prop_draw_int(tc, 0, 32);
	initStringInfo(&x);
	draw_inet(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT masklen(set_masklen(inet '%s', %d)) = %d",
					 x.data, m, m);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * network() keeps the host's mask length: masklen(network(inet 'a.b.c.d/len'))
 * = len.  network() zeroes the host bits but does not change the mask width.
 */
PG_PROP(network_masklen)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			a = 0,
				b = 0,
				c = 0,
				d = 0,
				len = 0;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = (int) pg_prop_draw_int(tc, 0, 255);
	b = (int) pg_prop_draw_int(tc, 0, 255);
	c = (int) pg_prop_draw_int(tc, 0, 255);
	d = (int) pg_prop_draw_int(tc, 0, 255);
	len = (int) pg_prop_draw_int(tc, 0, 32);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT masklen(network(inet '%d.%d.%d.%d/%d')) = %d",
					 a, b, c, d, len, len);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Every IPv4 inet is family 4: family(inet 'a.b.c.d/len') = 4.
 */
PG_PROP(family_is_4)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_inet(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT family(inet '%s') = 4", x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The broadcast address is never below the network address:
 * broadcast(X) >= network(X) as an inet comparison.  broadcast sets the host
 * bits to 1 and network sets them to 0, so broadcast is always at or above.
 */
PG_PROP(broadcast_ge_network)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_inet(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT broadcast(inet '%s') >= network(inet '%s')",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Text round-trip: (inet 'a.b.c.d/len')::text::inet = inet 'a.b.c.d/len'.
 * The output text must re-parse to exactly the same value.
 */
PG_PROP(abbrev_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_inet(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (inet '%s')::text::inet = inet '%s'",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment is reflexive: inet X <<= X and X >>= X both hold.  Every
 * network/host value contains and is contained by itself.
 */
PG_PROP(contains_self)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_inet(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (inet '%s' <<= inet '%s') "
					 "AND (inet '%s' >>= inet '%s')",
					 x.data, x.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
