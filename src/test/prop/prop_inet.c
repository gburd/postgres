/*-------------------------------------------------------------------------
 *
 * prop_inet.c
 *	  Property-based tests for the inet/cidr network types.
 *
 * inet/cidr parsing, the containment operators, and host/network/masklen
 * accessors have had correctness fixes; these check text round-trips and the
 * subnet containment order over generated IPv4 and IPv6 values.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_inet.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "inet"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a valid inet literal into buf as a quoted SQL literal cast to inet,
 * e.g. "'10.0.0.1/24'::inet".
 *
 * IPv4 is drawn ~70% of the time (bias toward the common family while still
 * exercising IPv6).  IPv4 is "A.B.C.D" with a masklen 0..32, and the "/M" is
 * sometimes omitted to generate a host (no mask).  IPv6 uses the full 8-group
 * form "a:b:c:d:e:f:g:h/M" with each hextet drawn 0..65535 and M in 0..128;
 * writing all eight groups avoids any "::" compression ambiguity in the test
 * generator itself.
 */
static void
draw_inet(pg_prop_tc *tc, StringInfo buf)
{
	bool		ipv4 = (pg_prop_draw_int(tc, 0, 9) < 7);

	appendStringInfoChar(buf, '\'');
	if (ipv4)
	{
		int			a = (int) pg_prop_draw_int(tc, 0, 255);
		int			b = (int) pg_prop_draw_int(tc, 0, 255);
		int			c = (int) pg_prop_draw_int(tc, 0, 255);
		int			d = (int) pg_prop_draw_int(tc, 0, 255);

		appendStringInfo(buf, "%d.%d.%d.%d", a, b, c, d);
		if (pg_prop_draw_bool(tc))
			appendStringInfo(buf, "/%d", (int) pg_prop_draw_int(tc, 0, 32));
	}
	else
	{
		for (int i = 0; i < 8; i++)
		{
			if (i > 0)
				appendStringInfoChar(buf, ':');
			appendStringInfo(buf, "%x", (unsigned) pg_prop_draw_int(tc, 0, 65535));
		}
		appendStringInfo(buf, "/%d", (int) pg_prop_draw_int(tc, 0, 128));
	}
	appendStringInfoString(buf, "'::inet");
}

/*
 * Draw a valid cidr value into buf.
 *
 * A cidr literal errors if any host bit is set, so instead of writing
 * 'A.B.C.D/M'::cidr directly we let the server clear the host bits: network()
 * of an inet returns the cidr of the containing network, which is always a
 * valid cidr with host bits zero.  So this emits "network('...'::inet)".
 */
static void
draw_cidr(pg_prop_tc *tc, StringInfo buf)
{
	appendStringInfoString(buf, "network(");
	draw_inet(tc, buf);
	appendStringInfoChar(buf, ')');
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
 * Text round-trip: an inet rendered to text and parsed back equals the value.
 * This exercises both inet_out and inet_in on every generated value.
 */
PG_PROP(text_roundtrip_inet)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet(tc, &i);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::inet = %s", i.data, i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Splitting an inet into its host address plus its masklen and recombining
 * returns the original: set_masklen(host(I)::inet, masklen(I)) = I.  host()
 * drops the mask; set_masklen reattaches it.
 */
PG_PROP(host_masklen_reconstruct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet(tc, &i);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT set_masklen(host(%s)::inet, masklen(%s)) = %s",
					 i.data, i.data, i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The network of an inet contains the inet (>>= is contains-or-equals), and
 * network() yields a valid cidr: round-tripping it through cidr and back to
 * inet is a no-op.
 */
PG_PROP(network_contains_host)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet(tc, &i);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT network(%s) >>= %s "
					 "AND network(%s) = network(%s)::cidr::inet",
					 i.data, i.data, i.data, i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Containment is a partial order consistent with masklen: a network that
 * strictly contains another must have a shorter prefix.  So if C1 >>= C2 and
 * C1 <> C2 then masklen(C1) < masklen(C2); otherwise the implication is
 * vacuously true.
 */
PG_PROP(containment_order_consistent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData c1,
				c2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&c1);
	initStringInfo(&c2);
	draw_cidr(tc, &c1);
	draw_cidr(tc, &c2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN %s >>= %s AND %s <> %s "
					 "THEN masklen(%s) < masklen(%s) ELSE true END",
					 c1.data, c2.data, c1.data, c2.data,
					 c1.data, c2.data);

	require_true(tc, conn, q.data);

	pfree(c1.data);
	pfree(c2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The broadcast address of an inet lies within its network.
 */
PG_PROP(broadcast_in_network)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet(tc, &i);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT network(%s) >>= broadcast(%s)",
					 i.data, i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * family() is 4 or 6, and it is 4 exactly when the address is within the IPv4
 * space (contained-or-equal in 0.0.0.0/0).
 */
PG_PROP(family_matches_version)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData i,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&i);
	draw_inet(tc, &i);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT family(%s) IN (4, 6) "
					 "AND (family(%s) = 4) = (%s <<= '0.0.0.0/0'::inet)",
					 i.data, i.data, i.data);

	require_true(tc, conn, q.data);

	pfree(i.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
