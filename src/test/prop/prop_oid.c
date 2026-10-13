/*-------------------------------------------------------------------------
 *
 * prop_oid.c
 *	  Property-based tests for the oid type and oidvector
 *	  (src/backend/utils/adt/oid.c).
 *
 * oid is an unsigned 32-bit identifier.  Its text I/O prints and parses the
 * full unsigned range (values above 2^31 must appear as unsigned decimals,
 * not negatives, and the range wraps/overflows at 2^32), and oidvector input
 * parses a space-separated list of oids -- both have had fixes.  These
 * properties check unsigned text round-trip, that output is the unsigned form,
 * that comparison is a total order under *unsigned* ordering, and that an
 * oidvector round-trips through text and preserves its length.
 *
 * The oracle is the type's own text I/O and comparison: an oid formatted to
 * text and parsed back must equal the original across the whole range
 * (including 0 and 4294967295), the printed form of a value above 2^31 must be
 * its unsigned decimal, comparison must be a total order with unsigned
 * semantics, and an oidvector cast to text and back must be unchanged and keep
 * its element count.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_oid.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "oid"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw one oid value.  oid is unsigned 32-bit, so the full range
 * 0..4294967295 is drawn -- the region above 2^31 is the whole point, since a
 * signed misinterpretation would print or compare those values as negatives.
 */
static uint32
draw_oid(pg_prop_tc *tc)
{
	return (uint32) pg_prop_draw_int(tc, 0, UINT32_MAX);
}

/*
 * Draw an oidvector literal into buf as a single-quoted, space-separated list
 * of k oids in 0..6 elements, cast to oidvector.  The literal contains only
 * digits and spaces, so single-quoting is safe.  Returns k via *k_out.
 */
static void
draw_oidvector(pg_prop_tc *tc, StringInfo buf, int *k_out)
{
	int			k = (int) pg_prop_draw_int(tc, 0, 6);

	*k_out = k;

	appendStringInfoChar(buf, '\'');
	for (int i = 0; i < k; i++)
	{
		if (i > 0)
			appendStringInfoChar(buf, ' ');
		appendStringInfo(buf, "%u", draw_oid(tc));
	}
	appendStringInfoString(buf, "'::oidvector");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the query errored or produced NULL on valid inputs, which
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
 * Text round-trip: an oid formatted to text and parsed back equals the
 * original, across the whole range including 0 and 4294967295.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	uint32		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = draw_oid(tc);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%u::oid)::text::oid = %u::oid", n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Unsigned text: oid prints as an unsigned decimal.  For n > 2147483647 the
 * output must be the unsigned form (e.g. 4000000000, not -294967296).  The
 * expected text is built in C with %u, which is the unsigned decimal, and
 * compared against the oid's own ::text output.
 */
PG_PROP(unsigned_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	uint32		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = draw_oid(tc);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%u::oid)::text = '%u'::text", n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Comparison is a total order: for any two oids exactly one of <, =, > holds,
 * so the three predicates sum to 1.
 */
PG_PROP(comparison_total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	uint32		o1,
				o2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	o1 = draw_oid(tc);
	o2 = draw_oid(tc);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%u::oid < %u::oid)::int "
					 "+ (%u::oid = %u::oid)::int "
					 "+ (%u::oid > %u::oid)::int) = 1",
					 o1, o2, o1, o2, o1, o2);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Ordering is unsigned: a value above 2^31 compares greater than a small
 * value.  A signed misinterpretation would make the large value negative and
 * thus smaller, so this directly targets unsigned-comparison correctness.
 */
PG_PROP(order_is_unsigned)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	uint32		s,
				l;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	s = (uint32) pg_prop_draw_int(tc, 0, 1000);
	l = (uint32) pg_prop_draw_int(tc, INT64_C(2147483648), UINT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%u::oid > %u::oid)", l, s);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * oidvector text round-trip: an oidvector cast to text and parsed back equals
 * the original.  Exercises oidvector output and input (space-separated oids).
 */
PG_PROP(oidvector_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_oidvector(tc, &v, &k);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::oidvector = (%s)",
					 v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * oidvector length: an oidvector built from k oids casts to oid[] with
 * cardinality k.  The empty oidvector (k = 0) has cardinality 0, so coalesce
 * guards the NULL that cardinality() returns for an empty array.
 */
PG_PROP(oidvector_length_matches)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_oidvector(tc, &v, &k);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT coalesce(cardinality((%s)::oid[]), 0) = %d",
					 v.data, k);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
