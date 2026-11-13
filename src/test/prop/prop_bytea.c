/*-------------------------------------------------------------------------
 *
 * prop_bytea.c
 *	  Property-based tests for bytea and encode()/decode().
 *
 * encode()/decode() (hex, base64, escape) and bytea operations are codecs
 * with exact round-trip properties plus length/concatenation laws; this
 * exercises all three encodings over arbitrary bytes.  The targets are
 * src/backend/utils/adt/encode.c (the hex/base64/escape codecs) and
 * varlena.c (bytea length, concatenation and substring).
 *
 * The oracle is the type's own algebra: for every encoding, decode . encode
 * is the identity on bytea; length() of a bytea is its byte count;
 * concatenation adds lengths; and splitting a bytea at any point and
 * rejoining the pieces reconstructs it.  None of these admit any tolerance,
 * so any "f" result is a bug in the codec or in the bytea operators.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_bytea.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "bytea"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw 0..128 arbitrary bytes and append a bytea literal for them into buf,
 * in the hex input format: '\x<hexdigits>'::bytea with two hex chars per
 * byte.  The leading sequence is a literal backslash followed by 'x', which
 * is how bytea's hex input is written under the default
 * standard_conforming_strings=on (the backslash is a literal character in the
 * single-quoted string, not an escape).  The empty bytea '\x'::bytea is a
 * valid and interesting case and is included (nbytes may be 0).
 *
 * *nbytes receives the number of bytes drawn, so a property can compare SQL
 * length() against the exact count it generated.
 */
static void
draw_bytea_literal(pg_prop_tc *tc, StringInfo buf, size_t *nbytes)
{
	size_t		len;
	unsigned char *bytes = pg_prop_draw_bytes(tc, 0, 128, &len);

	appendStringInfoString(buf, "'\\x");
	for (size_t i = 0; i < len; i++)
		appendStringInfo(buf, "%02x", bytes[i]);
	appendStringInfoString(buf, "'::bytea");

	*nbytes = len;
	pg_free(bytes);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the codec or operator errored or produced NULL on valid
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
 * Hex round-trip: decode(encode(B, 'hex'), 'hex') = B for every byte string.
 * encode() renders each byte as two lowercase hex digits and decode() parses
 * them back, so this is the identity on bytea with no exceptions.
 */
PG_PROP(hex_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	size_t		nbytes;
	StringInfoData b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&b);
	draw_bytea_literal(tc, &b, &nbytes);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT decode(encode(%s, 'hex'), 'hex') = %s",
					 b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Base64 round-trip: decode(encode(B, 'base64'), 'base64') = B.  base64 is a
 * pure codec, so encode-then-decode must reproduce every input exactly.
 */
PG_PROP(base64_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	size_t		nbytes;
	StringInfoData b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&b);
	draw_bytea_literal(tc, &b, &nbytes);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT decode(encode(%s, 'base64'), 'base64') = %s",
					 b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Escape round-trip: decode(encode(B, 'escape'), 'escape') = B.  The 'escape'
 * format leaves printable non-backslash bytes as-is and octal-escapes the
 * rest (and doubles backslashes), which is a known fiddly path -- a boundary
 * bug there would show up as a failed round-trip here.
 */
PG_PROP(escape_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	size_t		nbytes;
	StringInfoData b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&b);
	draw_bytea_literal(tc, &b, &nbytes);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT decode(encode(%s, 'escape'), 'escape') = %s",
					 b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Length matches the drawn byte count: length(B) = <nbytes>.  For bytea,
 * length() returns the number of bytes, which must equal the count this
 * property generated into the literal.
 */
PG_PROP(length_matches_byte_count)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	size_t		nbytes;
	StringInfoData b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&b);
	draw_bytea_literal(tc, &b, &nbytes);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT length(%s) = %zu", b.data, nbytes);

	require_true(tc, conn, q.data);

	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concatenation is length-additive: length(B1 || B2) = length(B1) +
 * length(B2).  Appending two bytea values must produce exactly the sum of
 * their lengths, with no truncation or padding.
 */
PG_PROP(concat_length_additive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	size_t		n1,
				n2;
	StringInfoData b1,
				b2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&b1);
	initStringInfo(&b2);
	draw_bytea_literal(tc, &b1, &n1);
	draw_bytea_literal(tc, &b2, &n2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(%s || %s) = length(%s) + length(%s)",
					 b1.data, b2.data, b1.data, b2.data);

	require_true(tc, conn, q.data);

	pfree(b1.data);
	pfree(b2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Split-and-rejoin reconstructs: substr(B, 1, k) || substr(B, k+1) = B for
 * any split point k in 0..length(B).  The split point is clamped in SQL with
 * least(<drawnint>, length(B)) so the drawn index can never run past the end
 * and the oracle matches regardless of the generated byte count.
 */
PG_PROP(substr_concat_reconstructs)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	size_t		nbytes;
	int64		k;
	StringInfoData b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&b);
	draw_bytea_literal(tc, &b, &nbytes);
	k = pg_prop_draw_int(tc, 0, 128);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT substr(%s, 1, least(%lld, length(%s))) "
					 "|| substr(%s, least(%lld, length(%s)) + 1) = %s",
					 b.data, (long long) k, b.data,
					 b.data, (long long) k, b.data, b.data);

	require_true(tc, conn, q.data);

	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Hex encoding length: length(encode(B, 'hex')) = 2 * <nbytes>.  The hex
 * encoding emits exactly two characters per input byte, so the encoded text
 * length is twice the byte count with no exceptions.
 */
PG_PROP(hex_encode_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	size_t		nbytes;
	StringInfoData b,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&b);
	draw_bytea_literal(tc, &b, &nbytes);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT length(encode(%s, 'hex')) = %zu",
					 b.data, (size_t) 2 * nbytes);

	require_true(tc, conn, q.data);

	pfree(b.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
