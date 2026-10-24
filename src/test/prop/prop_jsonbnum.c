/*-------------------------------------------------------------------------
 *
 * prop_jsonbnum.c
 *	  Property-based tests for jsonb numeric value preservation.
 *
 * jsonb stores numbers as the arbitrary-precision NUMERIC type, so a number
 * put into a jsonb value and extracted again (#>> '{}', ->>, ->) must come
 * back with its full value and precision -- unlike IEEE float, 2^53+1 and a
 * fractional numeric survive intact, and jsonb number comparison orders by
 * numeric value.  A lost digit, a float round-trip, or a wrong comparison
 * here is a bug in jsonb's numeric handling, distinct from jsonbtype
 * (type dispatch) and jsonconstruct (document building).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_jsonbnum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "jsonbnum"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a decimal NUMERIC literal as text, formatted "<int>.<frac>" and
 * parenthesized so a leading sign never misparses when substituted into a
 * larger expression.  Always emits an integer digit and a fractional part so
 * the literal exercises both integer and scaled numeric paths through jsonb.
 */
static void
draw_numeric(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 1, 12);
	int			nfrac = (int) pg_prop_draw_int(tc, 1, 8);
	bool		neg = pg_prop_draw_bool(tc);

	appendStringInfoString(buf, "(");
	if (neg)
		appendStringInfoChar(buf, '-');
	for (int i = 0; i < nint; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoChar(buf, '.');
	for (int i = 0; i < nfrac; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoString(buf, ")");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means jsonb produced the wrong answer or errored on a finite
 * input, which is itself a bug -- so either is a failure, reported with SQL.
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
 * An integer put into jsonb and extracted as text equals its own ::text:
 * to_jsonb(<n>) #>> '{}' = <n>::text.  Draws the full int4 range so the
 * boundary values (INT_MIN/MAX, 0, -1) are tried.
 */
PG_PROP(int_preserved)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n = pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (to_jsonb((%lld)::int) #>> '{}') = (%lld)::int::text",
					 (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * jsonb preserves numeric value: a scaled numeric cast to jsonb and back is
 * equal (as numeric) to the original.
 * (('x'::numeric)::jsonb #>> '{}')::numeric = 'x'::numeric.
 */
PG_PROP(numeric_preserved)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((to_jsonb(%s::numeric)) #>> '{}')::numeric = %s::numeric",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A negated integer extracts as its negation:
 * (to_jsonb((-n)::int) #>> '{}')::int = -n for drawn n >= 0.  The negative is
 * emitted parenthesized as (-n) so it is cast cleanly rather than as -(..).
 */
PG_PROP(negative_preserved)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n = pg_prop_draw_int(tc, 0, INT32_MAX);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (to_jsonb((-%lld)::int) #>> '{}')::int = (-%lld)",
					 (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * jsonb numbers are arbitrary precision, so an integer beyond the exact range
 * of IEEE double (2^53 + 1 = 9007199254740993) round-trips without loss --
 * unlike a float, which would collapse it to 2^53.
 */
PG_PROP(large_int_preserved)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT ((to_jsonb(9007199254740993::numeric)) #>> '{}')::numeric "
						   "= 9007199254740993::numeric");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * jsonb comparison orders numbers by value: for drawn a < b,
 * to_jsonb(a) < to_jsonb(b), plus the fixed anchor that 1 < 2 and 2 > 1.
 * a and b are drawn ordered to keep the property sound.
 */
PG_PROP(jsonb_number_compare)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		a = pg_prop_draw_int(tc, INT32_MIN, INT32_MAX - 1);
	int64		b = pg_prop_draw_int(tc, a + 1, INT32_MAX);
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (to_jsonb(1) < to_jsonb(2)) "
					 "AND (to_jsonb(2) > to_jsonb(1)) "
					 "AND (to_jsonb((%lld)::int) < to_jsonb((%lld)::int))",
					 (long long) a, (long long) b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Indexing a jsonb scalar array returns the element as a jsonb number equal to
 * to_jsonb of that value: ('[1,2,3]'::jsonb -> 1) = to_jsonb(2).
 */
PG_PROP(scalar_array_number)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT ('[1,2,3]'::jsonb -> 1) = to_jsonb(2)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A numeric placed into a jsonb object and extracted as text (->> 'n') casts
 * back to the same numeric value: (jsonb_build_object('n', x) ->> 'n')::numeric
 * = x::numeric.
 */
PG_PROP(numeric_in_object)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (jsonb_build_object('n', %s::numeric) ->> 'n')::numeric "
					 "= %s::numeric",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Zero forms: to_jsonb(0) equals '0'::jsonb, and to_jsonb(0.0::numeric)
 * extracts to either '0' or '0.0' -- jsonb may keep the trailing scale of a
 * scaled zero, so both renderings are accepted.
 */
PG_PROP(zero_forms)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfoString(&q,
						   "SELECT (to_jsonb(0) = '0'::jsonb) "
						   "AND ((to_jsonb(0.0::numeric) #>> '{}') IN ('0', '0.0'))");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
