/*-------------------------------------------------------------------------
 *
 * prop_bitlogic.c
 *	  Property-based tests for int4 bitwise-logic identities.
 *
 * The oracle is Boolean algebra over two's-complement int4: idempotence,
 * absorption, De Morgan's laws, XOR's self-inverse / commutativity /
 * associativity, the shift round-trip, double complement, the identity
 * elements of AND/OR/XOR, and the ~n = -n-1 two's-complement relation.  Each
 * is an exact algebraic law that must hold for every int4 input (shifts are
 * range-guarded, and the complement relation is evaluated in bigint to dodge
 * the one INT_MIN overflow), so any "f" result is a bug in the integer
 * bitwise operators, not a numeric artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_bitlogic.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "bitlogic"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw an int4 and render it as an SQL literal into buf, parenthesized so a
 * leading minus never misparses in a larger expression and so INT_MIN is
 * emitted as "(-2147483647 - 1)" rather than a bare -2147483648 (which would
 * parse as -(2147483648) and overflow int4).
 */
static void
draw_int4(pg_prop_tc *tc, StringInfo buf)
{
	int64		n = pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	if (n == INT32_MIN)
		appendStringInfoString(buf, "((-2147483647 - 1)::int4)");
	else
		appendStringInfo(buf, "((%lld)::int4)", (long long) n);
}

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
 * n & n = n.  AND is idempotent.
 */
PG_PROP(and_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_int4(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s & %s) = %s", n.data, n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * n | n = n.  OR is idempotent.
 */
PG_PROP(or_idempotent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_int4(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s | %s) = %s", n.data, n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * n # n = 0.  XOR is its own inverse, so a value XORed with itself clears.
 */
PG_PROP(xor_self_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_int4(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s # %s) = 0", n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Absorption: n & (n | m) = n AND n | (n & m) = n.
 */
PG_PROP(and_or_absorption)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				m,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	initStringInfo(&m);
	draw_int4(tc, &n);
	draw_int4(tc, &m);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s & (%s | %s)) = %s "
					 "AND (%s | (%s & %s)) = %s",
					 n.data, n.data, m.data, n.data,
					 n.data, n.data, m.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(m.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * De Morgan: ~(n & m) = (~n) | (~m) AND ~(n | m) = (~n) & (~m).
 */
PG_PROP(de_morgan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				m,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	initStringInfo(&m);
	draw_int4(tc, &n);
	draw_int4(tc, &m);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (~(%s & %s)) = ((~%s) | (~%s)) "
					 "AND (~(%s | %s)) = ((~%s) & (~%s))",
					 n.data, m.data, n.data, m.data,
					 n.data, m.data, n.data, m.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(m.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * XOR commutes and associates: n # m = m # n AND (n # m) # k = n # (m # k).
 */
PG_PROP(xor_commutative_assoc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				m,
				k,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	initStringInfo(&m);
	initStringInfo(&k);
	draw_int4(tc, &n);
	draw_int4(tc, &m);
	draw_int4(tc, &k);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s # %s) = (%s # %s) "
					 "AND ((%s # %s) # %s) = (%s # (%s # %s))",
					 n.data, m.data, m.data, n.data,
					 n.data, m.data, k.data, n.data, m.data, k.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(m.data);
	pfree(k.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Shift round-trip: for small non-negative n (0..1000) and k (0..10), when
 * n << k still fits in int4, (n << k) >> k = n.  n is drawn in 0..1000 and k
 * in 0..10, so the maximum left-shifted value is 1000 << 10 = 1024000, well
 * within int4; no overflow is possible and the right shift exactly undoes the
 * left shift because no high bits are lost.
 */
PG_PROP(shift_inverse_small)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, 1000);
	int			k = (int) pg_prop_draw_int(tc, 0, 10);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%d::int4 << %d) >> %d) = %d::int4",
					 n, k, k, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Double complement: ~(~n) = n.  NOT is an involution.
 */
PG_PROP(not_not)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_int4(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (~(~%s)) = %s", n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Identity elements: n & 0 = 0 AND n | 0 = n AND n # 0 = n.  0 annihilates
 * under AND and is the identity under OR and XOR.
 */
PG_PROP(and_zero)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_int4(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s & 0) = 0 "
					 "AND (%s | 0) = %s "
					 "AND (%s # 0) = %s",
					 n.data, n.data, n.data, n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Two's-complement relation: ~n = -n - 1.  In int4 this overflows for
 * n = INT_MIN (since -INT_MIN is not representable), so it is evaluated in
 * bigint: (~n)::bigint = (-(n::bigint)) - 1.  ~n is computed first in int4
 * (always well-defined) and then widened, so no int4 overflow can occur for
 * any input, including INT_MIN.
 */
PG_PROP(complement_relation)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_int4(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (~%s)::bigint = (-(%s::bigint)) - 1",
					 n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
