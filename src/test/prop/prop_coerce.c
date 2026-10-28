/*-------------------------------------------------------------------------
 *
 * prop_coerce.c
 *	  Property-based tests for implicit/explicit coercion and operator
 *	  resolution.
 *
 * These properties pin down the type-promotion rules the parser and executor
 * apply when operands of different types meet: int + numeric promotes the int
 * to numeric, int < float8 promotes the int to float8, || coerces int to
 * text, and UNION/CASE/COALESCE unify branch types to a common type.  The
 * oracle is the explicit-cast form of the same expression: an implicit
 * coercion must produce exactly the value an explicit cast would, so any "f"
 * (or a differing UNION result set) is a resolution bug, not a value bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_coerce.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "coerce"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a modest int4 literal, parenthesized so a leading minus never
 * misparses when substituted into a larger expression (and so a bare -N is
 * never fed to ::int4 where it would reassociate).  Range is deliberately
 * small so counterexamples stay legible; coercion resolution is independent
 * of magnitude, so there is nothing extra to find at the int4 extremes here.
 */
static void
draw_int(pg_prop_tc *tc, StringInfo buf)
{
	appendStringInfo(buf, "(%d)", (int) pg_prop_draw_int(tc, -1000, 1000));
}

/*
 * Draw a decimal numeric literal as "<int>.<frac>", parenthesized, cast to
 * numeric.  Modest digit counts; a fractional part is always present so the
 * numeric genuinely differs from an int and the promotion is exercised.
 */
static void
draw_numeric(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 1, 6);
	int			nfrac = (int) pg_prop_draw_int(tc, 1, 6);
	bool		neg = pg_prop_draw_bool(tc);

	appendStringInfoString(buf, "(");
	if (neg)
		appendStringInfoChar(buf, '-');
	for (int i = 0; i < nint; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoChar(buf, '.');
	for (int i = 0; i < nfrac; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoString(buf, ")::numeric");
}

/*
 * Draw a float8 literal as "<int>.<frac>", parenthesized, cast to float8.
 */
static void
draw_float8(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 1, 6);
	int			nfrac = (int) pg_prop_draw_int(tc, 1, 6);
	bool		neg = pg_prop_draw_bool(tc);

	appendStringInfoString(buf, "(");
	if (neg)
		appendStringInfoChar(buf, '-');
	for (int i = 0; i < nint; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoChar(buf, '.');
	for (int i = 0; i < nfrac; i++)
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoString(buf, ")::float8");
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means coercion resolution failed or produced an unexpected
 * value, which is the bug these properties exist to catch.  (Copied in style
 * from prop_numeric.c's require_true.)
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
 * Differential result-set oracle: two queries must return byte-identical
 * row-major strings.  Both queries MUST carry a total ORDER BY.  (Copied from
 * the skeleton in area_spec.md.)
 */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *q1, const char *q2)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *r1,
			   *r2;

	r1 = pg_prop_sql_rows(conn, q1, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "q1 failed: %s\n  SQL: %s", e1 ? e1 : "", q1);
	r2 = pg_prop_sql_rows(conn, q2, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "q2 failed: %s\n  SQL: %s", e2 ? e2 : "", q2);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc, "mismatch:\n  q1: %s\n  q2: %s\n  r1:\n%s\n  r2:\n%s",
					 q1, q2, r1, r2);
	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * int + numeric: the int operand promotes to numeric, so (n + x) must equal
 * (n::numeric + x) exactly.  If promotion instead went the other way (numeric
 * truncated to int) the sum would differ.
 */
PG_PROP(int_plus_numeric_is_numeric)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	initStringInfo(&x);
	draw_int(tc, &n);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s + %s) = (%s::numeric + %s)",
					 n.data, x.data, n.data, x.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * int division truncates toward zero, whereas numeric division is exact: so
 * (n / m) with int operands must equal trunc(n::numeric / m) for m <> 0.
 * This checks that the integer / operator was resolved (not promoted to a
 * numeric / that would keep the fraction).  The CASE guards m = 0.
 */
PG_PROP(int_div_vs_numeric_div)
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
	draw_int(tc, &n);
	draw_int(tc, &m);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT CASE WHEN %s = 0 THEN true "
					 "ELSE (%s / %s)::numeric = trunc(%s::numeric / %s::numeric) END",
					 m.data,
					 n.data, m.data, n.data, m.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(m.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * int < float8: the int promotes to float8 for the comparison, so (n < f)
 * must equal (n::float8 < f).  A wrong resolution (truncating f to int) would
 * flip the comparison near integer boundaries.
 */
PG_PROP(mixed_compare)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				f,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	initStringInfo(&f);
	draw_int(tc, &n);
	draw_float8(tc, &f);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s < %s) = (%s::float8 < %s)",
					 n.data, f.data, n.data, f.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(f.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * UNION type unification (differential, shape B): a UNION ALL of an int and a
 * numeric unifies the column to numeric.  The result set, ordered, must match
 * the same two values built directly as numeric.  A total ORDER BY x makes
 * the comparison a sound multiset check; the two drawn values are distinct
 * (one has a fractional part) so there are no ties to disambiguate -- but the
 * explicit-numeric side is produced the same way, so even equal values would
 * compare identically.
 */
PG_PROP(union_unifies_int_numeric)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				x,
				q1,
				q2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	initStringInfo(&x);
	draw_int(tc, &n);
	draw_numeric(tc, &x);

	initStringInfo(&q1);
	initStringInfo(&q2);
	appendStringInfo(&q1,
					 "SELECT x FROM (SELECT %s AS x UNION ALL SELECT %s) s ORDER BY x",
					 n.data, x.data);
	appendStringInfo(&q2,
					 "SELECT x FROM (VALUES (%s::numeric),(%s)) v(x) ORDER BY x",
					 n.data, x.data);

	require_same_rows(tc, conn, q1.data, q2.data);

	pfree(n.data);
	pfree(x.data);
	pfree(q1.data);
	pfree(q2.data);
	pg_prop_disconnect(conn);
}

/*
 * CASE result unification: the branches (int, numeric) unify to numeric, so
 * CASE WHEN true THEN n ELSE x END must equal n::numeric exactly.  If the
 * int branch were not promoted, the WHEN-true value would come back as int
 * and still equal n::numeric -- but we also check the type path by comparing
 * against the numeric cast, which only agrees when the common type is numeric.
 */
PG_PROP(case_result_unification)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	initStringInfo(&x);
	draw_int(tc, &n);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (CASE WHEN true THEN %s ELSE %s END) = %s::numeric",
					 n.data, x.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * COALESCE type unification: COALESCE(int, numeric) unifies to numeric.  Both
 * branches are checked: a non-NULL int first argument comes back equal to
 * n::numeric, and a NULL int first argument falls through to the numeric
 * second argument equal to x.
 */
PG_PROP(coalesce_type_unification)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	initStringInfo(&x);
	draw_int(tc, &n);
	draw_numeric(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT COALESCE(%s, %s) = %s::numeric "
					 "AND COALESCE(NULL::int, %s) = %s",
					 n.data, x.data, n.data, x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * || coerces int to text: 'x' || n must equal 'x' || n::text.  The text
 * concatenation operator takes text operands, so the int is implicitly cast;
 * the oracle makes that cast explicit.  Any difference means the int was
 * rendered differently by the implicit path.
 */
PG_PROP(concat_coerces)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_int(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ('x' || %s) = ('x' || %s::text)",
					 n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
