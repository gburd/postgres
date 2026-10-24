/*-------------------------------------------------------------------------
 *
 * prop_decimalfmt.c
 *	  Property-based tests for NUMERIC decimal text I/O (input/output).
 *
 * This exercises the textual edges of numeric_in/numeric_out: parsing of
 * trailing zeros, leading zeros, signs and scientific-notation exponents, and
 * the round-trip through ::text.  The oracle is parse/print identity: a value
 * written out and read back must equal itself, and the display scale implied
 * by the input text must be preserved.  A failure means numeric's text codec
 * dropped or added a digit, mis-set the display scale, or mis-parsed an
 * exponent -- distinct from the arithmetic covered by numeric/numscale/
 * numround/numericcmp/numfmt.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_decimalfmt.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "decimalfmt"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a non-negative decimal magnitude as the raw text "<int>.<frac>" into
 * buf (no sign, no cast).  Sign is handled by the caller so a property can
 * build both "X" and "-X" from the same magnitude.  Digit counts stay small
 * (0..12 integer, 0..8 fractional) -- the point here is text formatting, not
 * carry handling -- and at least one digit is always emitted so the result is
 * a valid numeric literal.
 */
static void
draw_magnitude(pg_prop_tc *tc, StringInfo buf)
{
	int			nint = (int) pg_prop_draw_int(tc, 0, 12);
	int			nfrac = (int) pg_prop_draw_int(tc, 0, 8);
	int			ndigits = 0;

	for (int i = 0; i < nint; i++)
	{
		appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
		ndigits++;
	}
	if (nfrac > 0)
	{
		appendStringInfoChar(buf, '.');
		for (int i = 0; i < nfrac; i++)
		{
			appendStringInfoChar(buf, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
			ndigits++;
		}
	}
	if (ndigits == 0)
		appendStringInfoChar(buf, '0');
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the text codec errored or produced NULL on a valid
 * literal, which is itself a bug -- so either is a failure, reported with the
 * SQL.  (Same shape as prop_numeric.c's require_true.)
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
 * text_roundtrip: a drawn magnitude cast to numeric, out to text, and back to
 * numeric equals itself.  Exercises numeric_out followed by numeric_in on a
 * value with an arbitrary trailing-fraction scale.
 */
PG_PROP(text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_magnitude(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (('%s'::numeric)::text)::numeric = '%s'::numeric",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * trailing_zeros_equal: trailing fractional zeros do not change value --
 * '1.50' = '1.5' and '1.500' = '1.5'.  numeric_in must parse them to the same
 * value even though their display scales differ.
 */
PG_PROP(trailing_zeros_equal)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT '1.50'::numeric = '1.5'::numeric "
				 "AND '1.500'::numeric = '1.5'::numeric");

	pg_prop_disconnect(conn);
}

/*
 * scale_from_text: the display scale is taken from the literal's fractional
 * digit count -- scale('1.50') = 2 and scale('1.5') = 1 -- so trailing zeros
 * in the text are preserved as scale even though they do not change value.
 */
PG_PROP(scale_from_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT scale('1.50'::numeric) = 2 "
				 "AND scale('1.5'::numeric) = 1");

	pg_prop_disconnect(conn);
}

/*
 * exponent_roundtrip: scientific notation parses to the same value as the
 * expanded form -- '1.5e3' = 1500 and '2.5E2' = 250 -- covering both exponent
 * marker cases.
 */
PG_PROP(exponent_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT '1.5e3'::numeric = 1500::numeric "
				 "AND '2.5E2'::numeric = 250::numeric");

	pg_prop_disconnect(conn);
}

/*
 * leading_zeros_equal: leading integer zeros do not change value --
 * '007.5' = '7.5'.  numeric_in must strip them on parse.
 */
PG_PROP(leading_zeros_equal)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn, "SELECT '007.5'::numeric = '7.5'::numeric");

	pg_prop_disconnect(conn);
}

/*
 * negative_roundtrip: for a drawn non-negative magnitude X, prefixing a '-'
 * on the literal yields the arithmetic negation -- ('-' || X) = -(X).  The
 * magnitude is non-negative by construction, so the sign is unambiguous.
 */
PG_PROP(negative_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_magnitude(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT '-%s'::numeric = -('%s'::numeric)",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * normalize_text: feeding a value's own text representation back in is
 * idempotent -- (X::text)::numeric = X::numeric -- so numeric_out emits no
 * trailing-zero noise beyond the value's scale that would read back differently.
 */
PG_PROP(normalize_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_magnitude(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (('%s'::numeric)::text)::numeric = '%s'::numeric",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * plus_zero_identity: adding '0.0' leaves the value unchanged --
 * X + '0.0' = X.  Addition is exact, so the only way this fails is a text
 * parse that misreads the drawn literal.
 */
PG_PROP(plus_zero_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&x);
	draw_magnitude(tc, &x);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ('%s'::numeric + '0.0'::numeric) = '%s'::numeric",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
