/*-------------------------------------------------------------------------
 *
 * prop_safemath.c
 *	  Property-based tests for integer/numeric overflow detection
 *	  (src/backend/utils/adt/int.c, int8.c, numeric.c; src/include/common/int.h).
 *
 * PostgreSQL promises that integer arithmetic either returns the exact result
 * or raises "integer out of range" -- it must never wrap silently.  The
 * properties here assert the POSITIVE side of that contract: when a value or
 * operation is widened to a type with the headroom to hold it (int2->int8,
 * int4->int8, int8->numeric), the widened result equals the exact
 * mathematical answer.  A silent wrap in the narrow type, or a wrong result
 * after widening, breaks one of these identities; that is a correctness bug.
 *
 * These are deliberately positive invariants rather than "the server must
 * error here" checks, so there is no skip/overflow gating to make the run
 * flaky.  Values are drawn near the type boundaries (sqrt(INT_MAX), INT*_MAX,
 * INT*_MIN) because that is where wrap bugs live.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_safemath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "safemath"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the widened arithmetic errored or produced NULL on inputs
 * that are supposed to be in range, which is itself a bug -- so either is a
 * failure, reported with the SQL.  (Copied from prop_numeric.c's require_true.)
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
 * 1. int4_mul_overflow_widens
 *
 * For int4 a, b drawn near sqrt(INT_MAX) (~46340), the product a*b may exceed
 * int4 but always fits int8.  Computed in bigint it must equal the exact
 * product -- cross-checked against the numeric product, which has no range at
 * all.  a and b are emitted parenthesized and cast so a negative draw never
 * misparses, then cast to bigint before multiplying so the multiply itself
 * happens in int8 (no int4 wrap, no int4 overflow error).
 */
PG_PROP(int4_mul_overflow_widens)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			a;
	int			b;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* near +-sqrt(INT_MAX): products straddle the int4 boundary */
	a = (int) pg_prop_draw_int(tc, -46341, 46341);
	b = (int) pg_prop_draw_int(tc, -46341, 46341);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%d)::int4::bigint * (%d)::int4::bigint) "
					 "= ((%d)::numeric * (%d)::numeric)",
					 a, b, a, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. int2_add_boundary
 *
 * 32767::int2 + 1 overflows int2, but the same addition widened to bigint
 * yields exactly 32768.  Asserting the bigint form confirms the correct value
 * is reachable the moment the type has headroom; a silent int2 wrap would land
 * at -32768 instead.
 */
PG_PROP(int2_add_boundary)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn, "SELECT (32767::bigint + 1) = 32768");

	pg_prop_disconnect(conn);
}

/*
 * 3. numeric_no_overflow
 *
 * numeric has no fixed range, so a large product can never overflow.  Its
 * only obligation is to be exact: a*b must equal b*a.  Drawing wide integer
 * values (up to ~18 decimal digits each, so products reach ~36 digits, far
 * past int8) exercises the arbitrary-precision multiply with no range gating.
 */
PG_PROP(numeric_no_overflow)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a;
	StringInfoData b;
	StringInfoData q;
	int			i;
	int			na;
	int			nb;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* build two large decimal integer literals digit by digit */
	na = (int) pg_prop_draw_int(tc, 1, 18);
	nb = (int) pg_prop_draw_int(tc, 1, 18);

	initStringInfo(&a);
	initStringInfo(&b);
	appendStringInfoString(&a, "(");
	for (i = 0; i < na; i++)
		appendStringInfoChar(&a, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoString(&a, ")::numeric");
	appendStringInfoString(&b, "(");
	for (i = 0; i < nb; i++)
		appendStringInfoChar(&b, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoString(&b, ")::numeric");

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s * %s) = (%s * %s)",
					 a.data, b.data, b.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. int8_near_max_add
 *
 * Staying strictly under INT64_MAX must never overflow: for k in 1..1000,
 * (INT64_MAX - k)::bigint + k = INT64_MAX.  This confirms addition that lands
 * exactly on the maximum succeeds and is exact (no spurious overflow error,
 * no wrap).
 */
PG_PROP(int8_near_max_add)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			k;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	k = (int) pg_prop_draw_int(tc, 1, 1000);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((9223372036854775807 - %d)::bigint + %d) "
					 "= 9223372036854775807",
					 k, k);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. abs_min_overflows_widens
 *
 * abs(INT32_MIN) overflows int4 (its magnitude is one past INT32_MAX), but
 * widening to bigint first makes the magnitude representable:
 * abs((-2147483648)::int4::bigint) = 2147483648.  The negative literal is
 * parenthesized so it is parsed as the value -2147483648, not -(2147483648).
 */
PG_PROP(abs_min_overflows_widens)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT abs((-2147483648)::int4::bigint) = 2147483648");

	pg_prop_disconnect(conn);
}

/*
 * 6. div_by_type_min
 *
 * INT32_MIN / -1 overflows int4 (the quotient is one past INT32_MAX), but in
 * bigint the quotient fits: (-2147483648)::bigint / (-1) = 2147483648.  The
 * negated operands are parenthesized so each parses as its intended value.
 */
PG_PROP(div_by_type_min)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT ((-2147483648)::bigint / (-1)) = 2147483648");

	pg_prop_disconnect(conn);
}

/*
 * 7. mul_identity_safe
 *
 * The multiplicative identities a*1 = a and a*0 = 0 can never overflow for any
 * int8 a, including INT64_MIN and INT64_MAX.  INT64_MIN is written as a
 * subtraction (-9223372036854775807 - 1) because 9223372036854775808 is not a
 * valid int8 literal; the draw spans the full int8 range so both boundaries
 * are exercised.
 */
PG_PROP(mul_identity_safe)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int64		a;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	a = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	initStringInfo(&q);
	if (a == INT64_MIN)
		appendStringInfoString(&q,
							   "SELECT ((-9223372036854775807 - 1)::bigint * 1) "
							   "= (-9223372036854775807 - 1)::bigint "
							   "AND ((-9223372036854775807 - 1)::bigint * 0) = 0");
	else
		appendStringInfo(&q,
						 "SELECT ((%lld)::bigint * 1) = (%lld)::bigint "
						 "AND ((%lld)::bigint * 0) = 0",
						 (long long) a, (long long) a, (long long) a);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
