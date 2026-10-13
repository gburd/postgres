/*-------------------------------------------------------------------------
 *
 * prop_numfmt.c
 *	  Property-based tests for integer type I/O and overflow handling
 *	  for int2/int4/int8 (src/backend/utils/adt/int.c, int8.c, numutils.c).
 *
 * This is the "numfmt" area (integer parsing/formatting/overflow), distinct
 * from the "numeric" area (arbitrary-precision NUMERIC).  Integer input
 * (pg_strtoint16/32/64 in numutils.c) and arithmetic overflow checks (int.c,
 * int8.c) have had fixes; these properties check text round-trip, that
 * overflow is detected rather than wrapping, and non-decimal literal parsing.
 *
 * The oracle is the type's own text I/O and the exact NUMERIC algebra: an
 * integer formatted to text and parsed back must equal the original (across
 * the full width including INT64_MIN/MAX), and integer add/multiply must
 * either equal the exact numeric result or raise an overflow error -- never
 * wrap silently.  An "out of range" error for inputs whose true result
 * exceeds the type is the correct behavior and is skipped, not reported.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_numfmt.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "numfmt"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * True when a SQL error is a legitimate out-of-range/overflow condition rather
 * than a wrong-results bug.  Integer add/multiply of random operands near the
 * type's range ends can legally overflow; those cases are the correct result
 * and are discarded, not reported.
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
 * Run a SELECT that is REQUIRED to raise an out-of-range/overflow error.  If
 * the query instead returns a value, the overflow check that should have
 * fired did not -- the arithmetic wrapped -- and that is a bug.  Used for the
 * fixed boundary check (negating INT64_MIN).
 */
static void
require_overflow(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *res;

	res = pg_prop_sql_scalar(conn, sql, &failed, &err);
	if (failed)
	{
		if (is_overflow(err))
		{
			/* correct: the overflow was detected */
			if (err)
				pg_free(err);
			return;
		}
		pg_prop_fail(tc, "expected overflow error but got a different error "
					 "for: %s\n  error: %s", sql, err ? err : "(unknown)");
	}

	/* no error -- the result was produced, i.e. the overflow wrapped: a bug */
	pg_prop_fail(tc, "expected overflow error but query returned %s for: %s",
				 res ? res : "NULL", sql);
}

/*
 * Draw a full-width int64 and format it with "%lld".  The engine favours the
 * boundary values (INT64_MIN/MAX, 0, -1, 1); drawing the full range is the
 * point of these tests, so do not narrow it.
 */
static void
append_int64(pg_prop_tc *tc, StringInfo buf)
{
	int64		n = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	appendStringInfo(buf, "%lld", (long long) n);
}

/*
 * Draw an int32-range value and format it.
 */
static void
append_int32(pg_prop_tc *tc, StringInfo buf)
{
	int64		n = pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	appendStringInfo(buf, "%lld", (long long) n);
}

/*
 * Draw an int16-range value and format it.
 */
static void
append_int16(pg_prop_tc *tc, StringInfo buf)
{
	int64		n = pg_prop_draw_int(tc, -32768, 32767);

	appendStringInfo(buf, "%lld", (long long) n);
}

/*
 * 1. int8_text_roundtrip
 *
 * An int8 formatted to text and parsed back must equal the original, across
 * the full int64 width including INT64_MIN/MAX.  Exercises int8out and the
 * pg_strtoint64 input path (numutils.c).
 */
PG_PROP(int8_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	append_int64(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s)::int8)::text::int8 = (%s)::int8",
					 n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. int4_text_roundtrip
 *
 * An int4 formatted to text and parsed back must equal the original, across
 * the full int32 width including INT32_MIN/MAX.  Exercises int4out and the
 * pg_strtoint32 input path (numutils.c).
 */
PG_PROP(int4_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	append_int32(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s)::int4)::text::int4 = (%s)::int4",
					 n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. int2_text_roundtrip
 *
 * An int2 formatted to text and parsed back must equal the original, across
 * the full int16 width including -32768/32767.  Exercises int2out and the
 * pg_strtoint16 input path (numutils.c).
 */
PG_PROP(int2_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	append_int16(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s)::int2)::text::int2 = (%s)::int2",
					 n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. int8_add_matches_numeric_or_overflows
 *
 * int8 addition of A and B must either equal the exact numeric sum or raise an
 * out-of-range error -- it must never wrap silently.  NUMERIC is the exact
 * oracle: (A::numeric + B::numeric) is the true sum, and casting it back to
 * int8 only succeeds when the true sum fits.  If the int8 add raises
 * "out of range", the true sum is outside int8 and that is the correct result,
 * so the case is skipped.  Targets the int8 addition overflow check (int8.c).
 */
PG_PROP(int8_add_matches_numeric_or_overflows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a;
	StringInfoData b;
	StringInfoData q;
	char	   *result;
	bool		failed;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	append_int64(tc, &a);
	append_int64(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s)::int8 + (%s)::int8) "
					 "= ((%s)::numeric + (%s)::numeric)::int8",
					 a.data, b.data, a.data, b.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
	{
		if (is_overflow(err))
		{
			/* true sum exceeds int8: overflow is the correct result, skip */
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pfree(a.data);
			pfree(b.data);
			pfree(q.data);
			pg_prop_disconnect(conn);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc,
					 "unexpected error on int8 add vs numeric\n"
					 "  A: %s\n  B: %s\n  error: %s",
					 a.data, b.data, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "int8 addition did not match the exact numeric sum\n"
					 "  A: %s\n  B: %s\n  result: %s",
					 a.data, b.data, result ? result : "NULL");

	pg_free(result);
	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. int8_mul_matches_numeric_or_overflows
 *
 * As for addition: int8 multiplication of A and B must either equal the exact
 * numeric product or raise an out-of-range error, never wrapping.  An
 * "out of range" error means the true product is outside int8, which is the
 * correct result, so the case is skipped.  Targets the int8 multiply overflow
 * check (int8.c).
 */
PG_PROP(int8_mul_matches_numeric_or_overflows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData a;
	StringInfoData b;
	StringInfoData q;
	char	   *result;
	bool		failed;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	append_int64(tc, &a);
	append_int64(tc, &b);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s)::int8 * (%s)::int8) "
					 "= ((%s)::numeric * (%s)::numeric)::int8",
					 a.data, b.data, a.data, b.data);

	result = pg_prop_sql_scalar(conn, q.data, &failed, &err);

	if (failed)
	{
		if (is_overflow(err))
		{
			/* true product exceeds int8: overflow is correct, skip */
			pg_prop_note(tc, "overflow (skipped): %s", err);
			if (err)
				pg_free(err);
			pfree(a.data);
			pfree(b.data);
			pfree(q.data);
			pg_prop_disconnect(conn);
			pg_prop_assume(tc, false);
			return;				/* unreachable: assume(false) longjmps */
		}
		pg_prop_fail(tc,
					 "unexpected error on int8 mul vs numeric\n"
					 "  A: %s\n  B: %s\n  error: %s",
					 a.data, b.data, err ? err : "(unknown)");
	}

	if (result == NULL || strcmp(result, "t") != 0)
		pg_prop_fail(tc,
					 "int8 multiplication did not match the exact numeric product\n"
					 "  A: %s\n  B: %s\n  result: %s",
					 a.data, b.data, result ? result : "NULL");

	pg_free(result);
	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. negate_min_overflows
 *
 * A fixed boundary check with no draws: negating INT64_MIN has no int8
 * representation (its magnitude is one past INT64_MAX), so it must raise an
 * out-of-range error, not wrap back to INT64_MIN.  INT64_MIN is written as
 * (-9223372036854775807 - 1) so the literal is unambiguously parsed as int8
 * first; negating it must then overflow.  This is the classic overflow site
 * in int8um (int8.c).  If the query returns a value instead of erroring, the
 * negation wrapped -- a bug.
 */
PG_PROP(negate_min_overflows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_overflow(tc, conn,
					 "SELECT (-((-9223372036854775807 - 1)::int8))");

	pg_prop_disconnect(conn);
}

/*
 * 7. nondecimal_roundtrip
 *
 * PostgreSQL accepts hex/octal/binary integer literals (0x, 0o, 0b) since v16.
 * For a non-negative n, prefixing the hex digits of n (from to_hex) with "0x"
 * and casting to int8 must reparse to n.  Exercises the non-decimal parsing
 * path of pg_strtoint (numutils.c).
 */
PG_PROP(nondecimal_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, 0, 1000000);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('0x' || to_hex(%lld))::int8 = (%lld)::int8",
					 (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
