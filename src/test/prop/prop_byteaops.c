/*-------------------------------------------------------------------------
 *
 * prop_byteaops.c
 *	  Property-based tests for bytea operations and codecs.
 *
 * bytea's operators and codecs form a small exact algebra: concatenation is
 * length-additive, substring()/substr() partition a value cleanly, get_byte
 * reads back what set_byte wrote, and encode/decode is the identity for every
 * encoding.  None of these admit any tolerance, so a "t" is required for all
 * inputs; the targets are varlena.c (bytea length, concat, substring,
 * get_byte/set_byte) and encode.c (hex/base64/escape codecs).
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_byteaops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "byteaops"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a byte string of 0..20 bytes and append it as decode('<hexpairs>',
 * 'hex') into buf, where <hexpairs> is an even-length run of hex digits (two
 * per byte).  Drawing the byte count directly keeps the hex length predictable
 * (always 2 * nbytes) so length-based oracles can compute the exact count.
 *
 * *nbytes receives the number of bytes, so a property can bound indices and
 * split points against the real length without a round-trip to SQL.
 */
static void
draw_bytea(pg_prop_tc *tc, StringInfo buf, int *nbytes)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 20);
	int			i;

	appendStringInfoString(buf, "decode('");
	for (i = 0; i < n; i++)
	{
		int			b = (int) pg_prop_draw_int(tc, 0, 255);

		appendStringInfo(buf, "%02x", b);
	}
	appendStringInfoString(buf, "', 'hex')");

	*nbytes = n;
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means an operator or codec errored or produced NULL on valid
 * inputs, which is itself a bug -- so either is a failure, reported with the
 * SQL.
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
 * Concatenation is length-additive: octet_length(A || B) = octet_length(A) +
 * octet_length(B).  Appending two bytea values yields exactly the sum of their
 * byte counts, with no truncation or padding.
 */
PG_PROP(concat_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			na,
				nb;
	StringInfoData a,
				b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	initStringInfo(&b);
	draw_bytea(tc, &a, &na);
	draw_bytea(tc, &b, &nb);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT octet_length(%s || %s) "
					 "= octet_length(%s) + octet_length(%s)",
					 a.data, b.data, a.data, b.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Hex round-trip: decode(encode(A, 'hex'), 'hex') = A.  encode renders each
 * byte as two hex digits and decode parses them back, so this is the identity
 * on bytea with no exceptions.
 */
PG_PROP(hex_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			na;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bytea(tc, &a, &na);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT decode(encode(%s, 'hex'), 'hex') = %s",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Base64 round-trip: decode(encode(A, 'base64'), 'base64') = A.  base64 is a
 * pure codec, so encode-then-decode must reproduce every input exactly.
 */
PG_PROP(base64_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			na;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bytea(tc, &a, &na);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT decode(encode(%s, 'base64'), 'base64') = %s",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * set_byte then get_byte reads back the written value: for non-empty A and a
 * valid index k in 0..octet_length(A)-1, get_byte(set_byte(A, k, 65), k) = 65.
 * k is drawn in the parent and clamped in SQL to the last valid index so it
 * can never run past the end regardless of the generated byte count.
 */
PG_PROP(getbyte_setbyte)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			na;
	int64		k;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bytea(tc, &a, &na);

	/* Empty bytea has no valid index; nothing to assert. */
	if (na == 0)
	{
		pfree(a.data);
		pg_prop_disconnect(conn);
		return;
	}

	k = pg_prop_draw_int(tc, 0, na - 1);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT get_byte(set_byte(%s, %lld, 65), %lld) = 65",
					 a.data, (long long) k, (long long) k);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * substring length is doubly bounded: octet_length(substring(A from s for l))
 * <= l AND <= greatest(octet_length(A) - s + 1, 0).  A substring can never be
 * longer than the requested length, nor longer than the bytes available from
 * the start position; s is drawn in 1..nbytes+1 and l in 0..nbytes.
 */
PG_PROP(substring_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			na;
	int64		s,
				l;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bytea(tc, &a, &na);
	s = pg_prop_draw_int(tc, 1, na + 1);
	l = pg_prop_draw_int(tc, 0, na);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT octet_length(substring(%s from %lld for %lld)) <= %lld "
					 "AND octet_length(substring(%s from %lld for %lld)) "
					 "<= greatest(octet_length(%s) - %lld + 1, 0)",
					 a.data, (long long) s, (long long) l, (long long) l,
					 a.data, (long long) s, (long long) l,
					 a.data, (long long) s);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Split-and-rejoin reconstructs: substring(A from 1 for k) ||
 * substring(A from k+1) = A for any split point k in 0..octet_length(A).  k is
 * drawn in the parent in 0..nbytes, so it already lies in range.
 */
PG_PROP(substr_concat_whole)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			na;
	int64		k;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bytea(tc, &a, &na);
	k = pg_prop_draw_int(tc, 0, na);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT substring(%s from 1 for %lld) "
					 "|| substring(%s from %lld) = %s",
					 a.data, (long long) k,
					 a.data, (long long) (k + 1), a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Escape round-trip: decode(encode(A, 'escape'), 'escape') = A.  The 'escape'
 * format leaves printable non-backslash bytes as-is and octal-escapes the rest
 * (doubling backslashes), a fiddly path whose boundary bugs show up here as a
 * failed round-trip.
 */
PG_PROP(escape_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			na;
	StringInfoData a,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&a);
	draw_bytea(tc, &a, &na);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT decode(encode(%s, 'escape'), 'escape') = %s",
					 a.data, a.data);

	require_true(tc, conn, q.data);

	pfree(a.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
