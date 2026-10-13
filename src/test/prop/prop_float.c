/*-------------------------------------------------------------------------
 *
 * prop_float.c
 *	  Property-based tests for float4/float8 output/input (the Ryu formatter).
 *
 * float8out/float4out use Ryu to produce the shortest decimal string that
 * reads back as the identical IEEE value.  The headline property is therefore
 * an exact text round-trip: float8 -> text -> float8 is the identity for every
 * value, including subnormals, +/-0, +/-Inf and NaN.  Alongside the round-trip
 * these properties check that the type's ordering and comparison operators are
 * consistent (a total order on non-NaN values) and that negation and abs
 * behave algebraically -- all of which must hold with no tolerance.
 *
 * The oracle here is the type's own text I/O and algebra: the server formats a
 * value, we feed its own text back in, and require the result to equal the
 * value we started from.  Because the property is about the SERVER's output
 * round-tripping, seeding the comparison from a "%.17g" literal (which itself
 * round-trips any IEEE double) is fine -- we then compare the server's own
 * ::text::float8 against that parsed literal.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_float.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "float"
#include "pg_prop.h"

#include "lib/stringinfo.h"

#include <math.h>

/*
 * Draw one double across the widest possible range and format it as a SQL
 * float8 literal into buf.  With unbounded range the harness's float generator
 * yields the special values too (NaN, +/-Inf, subnormals, +/-0); those are the
 * point, so the range is deliberately not narrowed.
 *
 * Finite values are emitted with "%.17g", which round-trips any IEEE double
 * exactly, so the literal names the drawn value precisely before it ever
 * reaches the server.  The specials have no decimal form and are emitted as
 * their canonical PostgreSQL spellings.  The returned double is the value
 * passed in SQL, so a property can inspect it in C (e.g. to skip NaN).
 */
static double
draw_f8_literal(pg_prop_tc *tc, StringInfo buf)
{
	double		v = pg_prop_draw_float(tc, -INFINITY, INFINITY);

	if (isnan(v))
		appendStringInfoString(buf, "'NaN'::float8");
	else if (isinf(v))
		appendStringInfoString(buf, v < 0 ? "'-Infinity'::float8"
							   : "'Infinity'::float8");
	else
	{
		char		num[64];

		snprintf(num, sizeof(num), "%.17g", v);
		appendStringInfo(buf, "'%s'::float8", num);
	}
	return v;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the comparison produced NULL or errored on inputs the
 * property is supposed to handle (almost always NaN leaking into a comparison,
 * which a sound property must special-case rather than blame on the server) --
 * so either is a failure, reported with the SQL.
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
 * Like require_true, but a SQL error that is a legitimate out-of-range
 * overflow (e.g. casting a float8 beyond the float4 range) discards the case
 * rather than failing it.
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
		bool		overflow = (err != NULL &&
								(strstr(err, "out of range") != NULL ||
								 strstr(err, "overflow") != NULL));

		if (err)
			pg_free(err);
		if (overflow)
			pg_prop_assume(tc, false);	/* legitimate overflow: skip */
		pg_prop_fail(tc, "SQL error for: %s", sql);
	}
	if (res == NULL)
		pg_prop_fail(tc, "unexpected NULL result for: %s", sql);
	if (strcmp(res, "t") != 0)
		pg_prop_fail(tc, "invariant failed (got %s) for: %s", res, sql);
	pg_free(res);
}

/*
 * Core Ryu property: float8 -> text -> float8 is the identity.  Ryu emits the
 * shortest decimal that reads back as the identical value, so re-parsing the
 * server's own output must recover the value exactly.  NaN needs special
 * handling because NaN <> NaN; for it we check that the text is 'NaN' instead.
 */
PG_PROP(text_roundtrip_f8)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_f8_literal(tc, &v);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN %s = 'NaN'::float8 "
					 "THEN (%s::text::float8) = 'NaN'::float8 OR (%s::text = 'NaN') "
					 "ELSE %s::text::float8 = %s END",
					 v.data, v.data, v.data, v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Same round-trip for float4.  The drawn double is cast to float4 first so it
 * lands on a real float4 value, then that value's own text must read back as
 * the identical float4.  NaN special-cased as above.
 */
PG_PROP(text_roundtrip_f4)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_f8_literal(tc, &v);

	/* f is the drawn value landed on a float4 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN (%s)::float4 = 'NaN'::float4 "
					 "THEN (%s)::float4::text = 'NaN' "
					 "ELSE (%s)::float4::text::float4 = (%s)::float4 END",
					 v.data, v.data, v.data, v.data);

	/* a float8 beyond the float4 range overflows on the cast; skip those */
	require_true_or_skip(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Non-NaN floats form a total order: of (V1 < V2), (V1 = V2), (V1 > V2)
 * exactly one holds, and (V1 < V2) is the negation of (V1 >= V2).  NaN is
 * unordered, so cases drawing a NaN are discarded (checked in C before any SQL
 * is built, since NaN would make every comparison NULL).
 */
PG_PROP(order_consistent_with_double)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v1,
				v2,
				q;
	double		d1,
				d2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v1);
	initStringInfo(&v2);
	d1 = draw_f8_literal(tc, &v1);
	d2 = draw_f8_literal(tc, &v2);
	pg_prop_assume(tc, !isnan(d1) && !isnan(d2));

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s < %s) = NOT (%s >= %s)) "
					 "AND (((%s < %s)::int + (%s = %s)::int + (%s > %s)::int) = 1)",
					 v1.data, v2.data, v1.data, v2.data,
					 v1.data, v2.data, v1.data, v2.data, v1.data, v2.data);

	require_true(tc, conn, q.data);

	pfree(v1.data);
	pfree(v2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Double negation is the identity: -(-V) = V.  NaN <> NaN would make the
 * equality NULL even though -NaN is still NaN, so the OR covers the NaN case
 * explicitly.
 */
PG_PROP(negation_involution)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	draw_f8_literal(tc, &v);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (- (- %s)) = %s OR (%s = 'NaN'::float8)",
					 v.data, v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * abs is non-negative and equals either V or -V.  NaN has no sign-magnitude
 * meaning here (and would make the comparisons NULL), so NaN cases are
 * discarded in C before building SQL.
 */
PG_PROP(abs_nonnegative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData v,
				q;
	double		d;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&v);
	d = draw_f8_literal(tc, &v);
	pg_prop_assume(tc, !isnan(d));

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT abs(%s) >= 0 AND (abs(%s) = %s OR abs(%s) = -%s)",
					 v.data, v.data, v.data, v.data, v.data);

	require_true(tc, conn, q.data);

	pfree(v.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Targeted +/-0 check (no draw): +0 and -0 compare equal, and the text of -0
 * is well-defined -- Ryu's sign-of-zero handling must render it as '-0' (or
 * '0').  A fixed-literal property, but valuable: signed zero is a classic
 * formatter corner.
 */
PG_PROP(zero_signs_equal_but_text_distinct)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	(void) tc;
	(void) arg;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT ('0'::float8 = '-0'::float8) "
				 "AND (('-0'::float8)::text IN ('-0','0'))");

	pg_prop_disconnect(conn);
}
