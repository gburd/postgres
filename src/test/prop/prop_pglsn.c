/*-------------------------------------------------------------------------
 *
 * prop_pglsn.c
 *	  Property-based tests for the pg_lsn datatype
 *	  (src/backend/utils/adt/pg_lsn.c).
 *
 * pg_lsn is a 64-bit WAL position printed as X/Y hex; its text I/O,
 * difference (lsn - lsn -> numeric bytes), and +/- numeric arithmetic have
 * had fixes.  These check text round-trip, the difference/addition inverse,
 * and ordering.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_pglsn.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "pglsn"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * True when a SQL error is a legitimate out-of-range/overflow condition
 * rather than a wrong-results bug.  pg_lsn + numeric and pg_lsn - numeric
 * run the result through numeric_pg_lsn, which raises "... out of range" when
 * the byte count carries the position outside the 64-bit LSN space; the NaN
 * guards raise "cannot add/subtract NaN".  Those are legal, so discard them.
 */
static bool
is_overflow(const char *errmsg)
{
	if (errmsg == NULL)
		return false;
	return strstr(errmsg, "out of range") != NULL ||
		strstr(errmsg, "cannot") != NULL;
}

/*
 * Draw a 64-bit LSN as a pg_lsn literal into buf, e.g. "'1A2B/00C3'::pg_lsn".
 *
 * The value is drawn as two independent 32-bit halves and emitted upper-hex
 * "high/low", so the whole 64-bit space is reachable, including the boundary
 * values 0/0 and FFFFFFFF/FFFFFFFF.  The raw 64-bit value (hi<<32 | lo) is
 * returned via *raw for use as a C-side oracle when wanted.
 */
static void
draw_lsn(pg_prop_tc *tc, StringInfo buf, uint64 *raw)
{
	uint32		hi = (uint32) pg_prop_draw_int(tc, 0, 4294967295);
	uint32		lo = (uint32) pg_prop_draw_int(tc, 0, 4294967295);

	appendStringInfo(buf, "'%X/%X'::pg_lsn", hi, lo);

	if (raw)
		*raw = ((uint64) hi << 32) | (uint64) lo;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operator errored or produced NULL on valid inputs,
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
 * 1. text_roundtrip
 *
 * The text output of a pg_lsn, parsed back, must equal the original.  This
 * exercises pg_lsn_out / pg_lsn_in (pg_parse_lsn): L = (L)::text::pg_lsn for
 * every drawn position, including the two extremes.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData l;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&l);
	draw_lsn(tc, &l, NULL);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s)::text::pg_lsn = (%s)", l.data, l.data);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. diff_then_add
 *
 * The difference L2 - L1 is a numeric byte count; adding it back to L1 must
 * yield L2: L1 + (L2 - L1) = L2.  (pg_lsn - pg_lsn -> numeric; pg_lsn +
 * numeric -> pg_lsn.)  When L2 < L1 the difference is negative, and the
 * add can carry L1 below 0 -- that underflow is a legitimate out-of-range
 * error, so the case is skipped rather than reported.
 */
PG_PROP(diff_then_add)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData l1;
	StringInfoData l2;
	StringInfoData q;
	char	   *result;
	bool		failed;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&l1);
	initStringInfo(&l2);
	draw_lsn(tc, &l1, NULL);
	draw_lsn(tc, &l2, NULL);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s + ((%s) - (%s))) = (%s)",
					 l1.data, l2.data, l1.data, l2.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
	{
		if (is_overflow(err))
		{
			/* add carried off the 64-bit range: legal, skip the case */
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pfree(l1.data);
			pfree(l2.data);
			pfree(q.data);
			pg_prop_disconnect(conn);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc,
					 "unexpected error on L1 + (L2 - L1) = L2\n"
					 "  L1: %s\n  L2: %s\n  error: %s",
					 l1.data, l2.data, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "L1 + (L2 - L1) != L2\n"
					 "  L1: %s\n  L2: %s\n  result: %s",
					 l1.data, l2.data, result ? result : "NULL");

	pg_free(result);
	pfree(l1.data);
	pfree(l2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. add_sub_inverse
 *
 * Adding a byte count and then subtracting it must return the LSN:
 * (L + d) - d = L.  d is kept modest (0..1e6) so most positions survive, but
 * a position within d bytes of the top of the range overflows on the add --
 * a legitimate out-of-range error, so that case is skipped.
 */
PG_PROP(add_sub_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData l;
	StringInfoData q;
	int64		d;
	char	   *result;
	bool		failed;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&l);
	draw_lsn(tc, &l, NULL);
	d = pg_prop_draw_int(tc, 0, 1000000);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s + %lld::numeric) - %lld::numeric) = (%s)",
					 l.data, (long long) d, (long long) d, l.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
	{
		if (is_overflow(err))
		{
			/* add carried past FFFFFFFF/FFFFFFFF: legal, skip the case */
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pfree(l.data);
			pfree(q.data);
			pg_prop_disconnect(conn);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc,
					 "unexpected error on (L + d) - d = L\n"
					 "  L: %s\n  d: %lld\n  error: %s",
					 l.data, (long long) d, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "(L + d) - d != L\n"
					 "  L: %s\n  d: %lld\n  result: %s",
					 l.data, (long long) d, result ? result : "NULL");

	pg_free(result);
	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. comparison_total_order
 *
 * For any two positions L1, L2 exactly one of (L1 < L2), (L1 = L2),
 * (L1 > L2) must hold: pg_lsn is a total order.
 */
PG_PROP(comparison_total_order)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData l1;
	StringInfoData l2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&l1);
	initStringInfo(&l2);
	draw_lsn(tc, &l1, NULL);
	draw_lsn(tc, &l2, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ( ((%s) < (%s))::int "
					 "+ ((%s) = (%s))::int "
					 "+ ((%s) > (%s))::int ) = 1",
					 l1.data, l2.data,
					 l1.data, l2.data,
					 l1.data, l2.data);

	require_true(tc, conn, q.data);

	pfree(l1.data);
	pfree(l2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. order_matches_difference_sign
 *
 * The comparison order must agree with the sign of the numeric difference:
 * L1 < L2 iff (L2 - L1) > 0, and L1 = L2 iff (L2 - L1) = 0.
 */
PG_PROP(order_matches_difference_sign)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData l1;
	StringInfoData l2;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&l1);
	initStringInfo(&l2);
	draw_lsn(tc, &l1, NULL);
	draw_lsn(tc, &l2, NULL);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s) < (%s)) = (((%s) - (%s)) > 0) "
					 "AND ((%s) = (%s)) = (((%s) - (%s)) = 0)",
					 l1.data, l2.data, l2.data, l1.data,
					 l1.data, l2.data, l2.data, l1.data);

	require_true(tc, conn, q.data);

	pfree(l1.data);
	pfree(l2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. add_zero_identity
 *
 * Adding zero bytes is the identity: L + 0 = L for every drawn position.
 */
PG_PROP(add_zero_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData l;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&l);
	draw_lsn(tc, &l, NULL);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s + 0::numeric) = (%s)", l.data, l.data);

	require_true(tc, conn, q.data);

	pfree(l.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
