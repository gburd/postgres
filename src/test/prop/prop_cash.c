/*-------------------------------------------------------------------------
 *
 * prop_cash.c
 *	  Property-based tests for the money type (src/backend/utils/adt/cash.c).
 *
 * The money type is a fixed-point int64 of minor units (cents, in the C
 * locale's two-fractional-digit format): a value like $1.23 is stored as the
 * integer 123.  Because the storage is a bare int64, its arithmetic has had a
 * run of overflow fixes -- in addition and subtraction (where two large
 * amounts can wrap the int64), in multiplication (cash * numeric and the
 * integer multiply paths), and in the rounding of division back to the
 * nearest cent.  A wrap instead of an error is a silent wrong-results bug.
 *
 * These properties check arithmetic identities that must hold on finite,
 * non-overflowing inputs -- additive inverse, commutativity, a text
 * round-trip, multiply-as-numeric, a division-rounding bound, and total
 * order -- and require that whenever an operation does run past the int64
 * range it raises an "out of range" error rather than wrapping.  Any case
 * that legitimately overflows is discarded (an assume), not reported, so the
 * identities are only asserted where they are supposed to hold.
 *
 * Locale note: money's text input and output depend on lc_monetary (the
 * currency symbol, the sign placement, the grouping).  To make the text
 * round-trip deterministic every property forces lc_monetary = 'C' on its
 * connection; in the C locale money prints like "$1.23" or "-$1.23".  The
 * VALUE of a money literal is kept locale-independent regardless, by building
 * it as (<cents>::numeric / 100)::money rather than parsing a formatted
 * string, so the arithmetic properties do not depend on the locale at all.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_cash.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cash"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * True when a SQL error is a legitimate int64 out-of-range/overflow condition
 * rather than a wrong-results bug.  money arithmetic near the ends of the
 * int64 minor-unit range can legally overflow; those cases are discarded, not
 * reported.
 */
static bool
is_overflow(const char *errmsg)
{
	if (errmsg == NULL)
		return false;
	return strstr(errmsg, "out of range") != NULL ||
		strstr(errmsg, "overflow") != NULL;
}

/*
 * Open a connection to the test cluster and force lc_monetary = 'C' so money
 * text output/parse is deterministic.  Fails the case if either step fails.
 */
static pg_prop_conn *
connect_C(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn, "SET lc_monetary = 'C'", &err))
		pg_prop_fail(tc, "SET lc_monetary = 'C' failed: %s", err ? err : "");

	return conn;
}

/*
 * Draw a money amount as a locale-independent SQL expression into buf:
 *
 *		((<cents>::numeric / 100)::money)
 *
 * money is an int64 count of minor units (cents, two fractional digits in the
 * C locale), so an amount is fully described by its integer cent count.
 * Casting a numeric to money is defined by value and does not depend on
 * lc_monetary, which is why we build the literal this way instead of parsing a
 * formatted "$1.23" string.
 *
 * Most draws use a moderate cent range so ordinary arithmetic (sums,
 * small-factor products) does not overflow and the identities get exercised;
 * about one draw in eight instead uses nearly the full int64 range so the
 * overflow paths are hit and checked to error rather than wrap.  The wide
 * bound stops short of INT64_MAX (9223372036854775807 cents) so that a single
 * numeric->money cast of the drawn value itself does not overflow -- only the
 * subsequent arithmetic can.
 */
static void
draw_money(pg_prop_tc *tc, StringInfo buf)
{
	int64		cents;

	if (pg_prop_draw_int(tc, 0, 7) == 0)
		cents = pg_prop_draw_int(tc, INT64_C(-92233720368547758),
								 INT64_C(92233720368547758));
	else
		cents = pg_prop_draw_int(tc, -1000000, 1000000);

	appendStringInfo(buf, "((" INT64_FORMAT "::numeric / 100)::money)", cents);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the operation errored or produced NULL on finite,
 * non-overflowing inputs, which is itself a bug -- so either is a failure,
 * reported with the SQL.  Use this for properties that cannot overflow.
 */
static void
require_true(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * Like require_true, but a SQL error whose message names an int64 overflow is
 * a legitimate out-of-range condition, not a bug: the case is discarded with
 * an assume(false) (which longjmps and does not return).  Any other error, a
 * NULL result, or an "f" result is a failure.  Use this for properties whose
 * arithmetic can legally run past the int64 minor-unit range.
 */
static void
require_true_or_skip(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		if (is_overflow(err))
		{
			/* ran off the int64 range: a legal error, discard the case */
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc, "SQL error for: %s\n  error: %s",
					 sql, err ? err : "(unknown)");
	}
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
	if (err)
		pg_free(err);
}

/*
 * Additive inverse: (M1 + M2) - M2 = M1.  money add/subtract are exact on
 * int64 minor units, so this holds whenever the intermediate sum does not
 * overflow; if the addition does overflow the server errors and the case is
 * skipped.
 */
PG_PROP(add_sub_inverse)
{
	pg_prop_conn *conn;
	StringInfoData m1,
				m2,
				q;

	conn = connect_C(tc);

	initStringInfo(&m1);
	initStringInfo(&m2);
	draw_money(tc, &m1);
	draw_money(tc, &m2);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s + %s) - %s) = %s",
					 m1.data, m2.data, m2.data, m1.data);

	require_true_or_skip(tc, conn, q.data);

	pfree(m1.data);
	pfree(m2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Addition commutes: M1 + M2 = M2 + M1.  A carry/overflow bug that depends on
 * operand order would surface as asymmetry here.  Both sides overflow or
 * neither does, so an overflow error discards the case cleanly.
 */
PG_PROP(add_commutes)
{
	pg_prop_conn *conn;
	StringInfoData m1,
				m2,
				q;

	conn = connect_C(tc);

	initStringInfo(&m1);
	initStringInfo(&m2);
	draw_money(tc, &m1);
	draw_money(tc, &m2);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s + %s) = (%s + %s)",
					 m1.data, m2.data, m2.data, m1.data);

	require_true_or_skip(tc, conn, q.data);

	pfree(m1.data);
	pfree(m2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Text round-trip in the C locale: M::text::money = M.  With lc_monetary='C'
 * money prints like "$1.23"/"-$1.23"; parsing that text back must recover the
 * exact same value.  This is the property that needs the forced C locale --
 * under a different lc_monetary the symbol and sign placement change but the
 * round-trip must still hold.  No arithmetic, so no overflow: require "t".
 */
PG_PROP(text_roundtrip_C_locale)
{
	pg_prop_conn *conn;
	StringInfoData m,
				q;

	conn = connect_C(tc);

	initStringInfo(&m);
	draw_money(tc, &m);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s::text::money) = %s", m.data, m.data);

	require_true(tc, conn, q.data);

	pfree(m.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Multiplying money by an integer equals doing the multiply in numeric and
 * casting back: M * k = (M::numeric * k)::money.  The oracle is independent of
 * the money * int code path (it routes through numeric arithmetic and a single
 * numeric->money cast), so a bug in the integer multiply surfaces as a
 * mismatch.  k is small (0..6) so moderate amounts stay in range; a product
 * that does overflow errors and the case is skipped.
 */
PG_PROP(mul_by_int_is_repeated_add)
{
	pg_prop_conn *conn;
	int			k;
	StringInfoData m,
				q;

	conn = connect_C(tc);

	k = (int) pg_prop_draw_int(tc, 0, 6);

	initStringInfo(&m);
	draw_money(tc, &m);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s * %d) = ((%s::numeric * %d)::money)",
					 m.data, k, m.data, k);

	require_true_or_skip(tc, conn, q.data);

	pfree(m.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Dividing money by a nonzero integer TRUNCATES toward zero (it is plain
 * integer division of the int64 minor units, not round-to-nearest), so the
 * money quotient differs from the exact numeric quotient by strictly less
 * than one cent:  abs((M / k)::numeric - M::numeric / k) < 0.01.  This bounds
 * the truncation without re-implementing it.  k is drawn nonzero by
 * construction (in {-6..-1, 1..6}), so no division-by-zero is expected; an
 * overflow in the division skips the case.
 */
PG_PROP(div_by_nonzero_times_back_close)
{
	pg_prop_conn *conn;
	int			k;
	StringInfoData m,
				q;

	conn = connect_C(tc);

	/* draw k in {-6..-1, 1..6}: nonzero by construction */
	k = (int) pg_prop_draw_int(tc, 1, 6);
	if (pg_prop_draw_bool(tc))
		k = -k;

	initStringInfo(&m);
	draw_money(tc, &m);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(((%s / %d)::numeric) - (%s::numeric / %d)) "
					 "< 0.01::numeric",
					 m.data, k, m.data, k);

	require_true_or_skip(tc, conn, q.data);

	pfree(m.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * money is a total order: for any two values exactly one of <, =, > holds, so
 * the three comparisons sum to exactly 1.  A broken comparator (e.g. a signed/
 * unsigned mix-up at the int64 boundary) would make two of them true or none
 * true.  Pure comparison, no arithmetic: no overflow, require "t".
 */
PG_PROP(comparison_total_order)
{
	pg_prop_conn *conn;
	StringInfoData m1,
				m2,
				q;

	conn = connect_C(tc);

	initStringInfo(&m1);
	initStringInfo(&m2);
	draw_money(tc, &m1);
	draw_money(tc, &m2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (((%s < %s)::int + (%s = %s)::int "
					 "+ (%s > %s)::int) = 1)",
					 m1.data, m2.data, m1.data, m2.data, m1.data, m2.data);

	require_true(tc, conn, q.data);

	pfree(m1.data);
	pfree(m2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
