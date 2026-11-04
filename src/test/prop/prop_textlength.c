/*-------------------------------------------------------------------------
 *
 * prop_textlength.c
 *	  Property-based tests for the text length family of functions.
 *
 * The oracle here is the algebra relating length/char_length (character
 * counts), octet_length (byte count), bit_length (= 8 * octet_length), and
 * their behaviour under concatenation, substr, lpad and the bytea cast.  For
 * single-byte ASCII input these have exact, server-encoding-independent
 * relationships, so any "f" result is a bug in a length routine, not an
 * encoding artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_textlength.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "textlength"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Alphabet is single-byte ASCII [a-c]; it contains no single quote, so a
 * drawn string is safe to drop between single quotes with no escaping.
 */
#define TL_ALPHABET "abc"
#define TL_MIN 0
#define TL_MAX 15

/*
 * Draw a string from TL_ALPHABET and append it as a single-quoted SQL literal.
 */
static void
draw_str_literal(pg_prop_tc *tc, StringInfo buf)
{
	size_t		len = 0;
	char	   *s = pg_prop_draw_from(tc, TL_ALPHABET, TL_MIN, TL_MAX, &len);

	appendStringInfoChar(buf, '\'');
	appendBinaryStringInfo(buf, s, (int) len);
	appendStringInfoChar(buf, '\'');
	pg_free(s);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means a length function errored or produced NULL on finite
 * input, which is itself a bug -- so either is a failure, reported with the
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
 * length() and char_length() are documented synonyms: both count characters.
 */
PG_PROP(length_eq_char_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_str_literal(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT length(%s) = char_length(%s)",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * For single-byte ASCII [a-c], every character is exactly one byte, so the
 * byte count equals the character count.
 */
PG_PROP(octet_eq_char_ascii)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_str_literal(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT octet_length(%s) = char_length(%s)",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * bit_length() is defined as 8 times the byte length.
 */
PG_PROP(bit_length_8x)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_str_literal(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT bit_length(%s) = 8 * octet_length(%s)",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Concatenation is additive in character length: length(S || T) =
 * length(S) + length(T).
 */
PG_PROP(concat_length_additive)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				t,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	initStringInfo(&t);
	draw_str_literal(tc, &s);
	draw_str_literal(tc, &t);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(%s || %s) = length(%s) + length(%s)",
					 s.data, t.data, s.data, t.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A prefix is never longer than the string: length(substr(S, 1, k)) =
 * least(k, length(S)) for k in 0..20 (k spans past TL_MAX so the clamp is
 * exercised).  substr with a negative/zero count returns the empty string,
 * and least() matches since length is non-negative.
 */
PG_PROP(length_substr)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			k = (int) pg_prop_draw_int(tc, 0, 20);
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_str_literal(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT length(substr(%s, 1, %d)) = least(%d, length(%s))",
					 s.data, k, k, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The empty string has zero characters, zero bytes, and zero bits.  No input
 * is drawn: this is a fixed boundary check.
 */
PG_PROP(empty_lengths)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn,
				 "SELECT length('') = 0 AND octet_length('') = 0 "
				 "AND bit_length('') = 0");

	pg_prop_disconnect(conn);
}

/*
 * lpad(S, n, 'x') produces a string of exactly n characters for n >= 0,
 * padding or truncating as needed.  n spans 0..20, past TL_MAX, so both the
 * pad and truncate cases are exercised.  (Padding is covered elsewhere; this
 * is a cross-check against the length family.)
 */
PG_PROP(length_after_pad)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			n = (int) pg_prop_draw_int(tc, 0, 20);
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_str_literal(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT length(lpad(%s, %d, 'x')) = %d",
					 s.data, n, n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Casting ASCII text to bytea preserves the byte count: for single-byte
 * ASCII [a-c] the text and its bytea form have the same number of bytes.
 */
PG_PROP(octet_length_bytea)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_str_literal(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT octet_length((%s)::bytea) = octet_length(%s)",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
