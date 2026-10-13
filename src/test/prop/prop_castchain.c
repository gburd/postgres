/*-------------------------------------------------------------------------
 *
 * prop_castchain.c
 *	  Property-based tests for cross-type cast chains and text round-trips.
 *
 * Casts and text I/O between the common built-in types form an algebra that
 * must round-trip: a value cast out to another type and back, or serialized
 * to text and parsed again, has to come back unchanged.  These properties
 * check that identity for bool<->int, int<->text, numeric<->text, the
 * varchar/char/text family, oid<->int, and bytea<->text(hex), covering type
 * I/O and the implicit/explicit cast paths that a formula-based oracle would
 * not.  Any "f" result is a cast or I/O bug.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_castchain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "castchain"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a cast errored or produced NULL on a value it should
 * accept, which is itself a bug -- so either is a failure, reported with the
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
 * Draw an int4 as text, parenthesizing a negative so a leading '-' does not
 * misparse when substituted into a larger expression (bare -N::int4 parses as
 * -(N::int4), which overflows at INT_MIN).  Kept inside int4 range.
 */
static void
draw_int4_lit(pg_prop_tc *tc, StringInfo buf)
{
	int			n = (int) pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	if (n < 0)
		appendStringInfo(buf, "(%d)", n);
	else
		appendStringInfo(buf, "%d", n);
}

/*
 * Draw a short string over the alphabet [abc], length 0..maxlen, with no
 * trailing space -- so a char(n) blank-pad/rtrim round-trip is sound.  The
 * literal is emitted single-quoted and ready to substitute.
 */
static void
draw_abc_lit(pg_prop_tc *tc, StringInfo buf, int maxlen)
{
	int			len = (int) pg_prop_draw_int(tc, 0, maxlen);

	appendStringInfoChar(buf, '\'');
	for (int i = 0; i < len; i++)
		appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
	appendStringInfoChar(buf, '\'');
}

/*
 * bool<->int round-trip.  The constants pin the mapping (true->1, false->0 and
 * back), and for a drawn bool b, b::int::bool = b.
 */
PG_PROP(bool_int_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	const char *b;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	b = pg_prop_draw_bool(tc) ? "true" : "false";

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT true::int = 1 AND false::int = 0 "
					 "AND 1::bool = true AND 0::bool = false "
					 "AND (%s::int::bool) = %s",
					 b, b);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * int4 text round-trip: n::text::int4 = n for a drawn int4 n across the full
 * range (including INT_MIN via the parenthesized negative).
 */
PG_PROP(int_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_int4_lit(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s::int4)::text::int4) = (%s::int4)",
					 n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * numeric text round-trip: X built as "<int>.<frac>" has
 * (X::numeric)::text::numeric = X::numeric.  numeric text I/O is exact, so
 * this holds with no tolerance.
 */
PG_PROP(numeric_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData x,
				q;
	int			nint = (int) pg_prop_draw_int(tc, 1, 12);
	int			nfrac = (int) pg_prop_draw_int(tc, 1, 8);
	bool		neg = pg_prop_draw_bool(tc);

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* Build "(<int>.<frac>)" with at least one digit on each side. */
	initStringInfo(&x);
	appendStringInfoChar(&x, '(');
	if (neg)
		appendStringInfoChar(&x, '-');
	for (int i = 0; i < nint; i++)
		appendStringInfoChar(&x, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoChar(&x, '.');
	for (int i = 0; i < nfrac; i++)
		appendStringInfoChar(&x, (char) ('0' + pg_prop_draw_int(tc, 0, 9)));
	appendStringInfoChar(&x, ')');

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s::numeric)::text::numeric) = (%s::numeric)",
					 x.data, x.data);

	require_true(tc, conn, q.data);

	pfree(x.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * varchar<->text round-trip: for S over [abc] length 0..10,
 * S::varchar::text = S and S::varchar(20)::text = S.  (len <= 10 < 20, so the
 * length-qualified varchar never truncates.)
 */
PG_PROP(varchar_text_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc_lit(tc, &s, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s::varchar::text) = %s "
					 "AND (%s::varchar(20)::text) = %s",
					 s.data, s.data, s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * char(n) blank-pad / rtrim round-trip: for S over [abc] with no trailing
 * space, rtrim(S::char(20)) = S.  char(n) right-pads with blanks to width n;
 * rtrim strips them back.  Since S has no trailing space, nothing of S is
 * lost, so the padding is exactly recovered.  (len <= 10 < 20, no truncation.)
 */
PG_PROP(text_bpchar_trailing_space)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc_lit(tc, &s, 10);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT rtrim(%s::char(20)) = %s",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * oid<->int round-trip: for a drawn n in 0..1000000, n::oid::int8 = n.  oid is
 * an unsigned 32-bit type, so keeping n in a small non-negative range avoids
 * the modular wrap at 2^32 and the int8 compare is exact.
 */
PG_PROP(oid_int_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData q;
	int			n = (int) pg_prop_draw_int(tc, 0, 1000000);

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%d::oid::int8) = %d::int8", n, n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * bytea<->text(hex) round-trip: for S over [abc],
 * decode(encode(S::bytea, 'hex'), 'hex') = S::bytea.  encode/decode are exact
 * inverses on bytea, so hex serialization and parsing must recover the bytes.
 */
PG_PROP(bytea_hex_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc_lit(tc, &s, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT decode(encode(%s::bytea, 'hex'), 'hex') "
					 "= %s::bytea",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * int / numeric text consistency: for a drawn int4 n, n::numeric::text =
 * n::text.  An integer promoted to numeric renders with no decimal point, so
 * both paths produce the identical digit string.
 */
PG_PROP(int_numeric_text_consistent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_int4_lit(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s::int4)::numeric::text) = ((%s::int4)::text)",
					 n.data, n.data);

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
