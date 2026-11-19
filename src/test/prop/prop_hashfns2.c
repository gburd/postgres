/*-------------------------------------------------------------------------
 *
 * prop_hashfns2.c
 *	  Property-based tests for hash/digest determinism and consistency.
 *
 * The text hash (hashtext) and the digests (md5, sha256) are pure functions:
 * the same input must always produce the same output, and equal values must
 * produce equal results.  That contract is what hash joins, hash indexes and
 * every content-addressed use of a digest rely on; a non-stable hash
 * (uninitialized padding, stray global state) silently corrupts lookups.
 * These properties check determinism (same input, same output), equality
 * consistency (equal values hash equally), and the fixed output widths of the
 * digests (md5 is 32 hex chars, sha256 is 64), each via a scalar boolean
 * oracle.  This area is distinct from "hashfn": it covers hashtext plus the
 * md5/sha256 digests rather than the per-type hash_* functions.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_hashfns2.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "hashfns2"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw 0..16 characters from the quote-free single-byte alphabet "abc" into
 * buf as a bare string (no quoting).  Letters only -- all ASCII, never a
 * single quote -- so the literal quoting is trivial and the only variable is
 * the function under test.
 */
static void
draw_text(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abc";
	int			n = (int) pg_prop_draw_int(tc, 0, 16);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 2)]);
}

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  The [abc] alphabet never contains a quote, but do it
 * defensively so the generated SQL is always well-formed.  Modelled on
 * prop_hashfn.c.
 */
static void
append_sql_literal(StringInfo buf, const char *s)
{
	appendStringInfoChar(buf, '\'');
	for (const char *p = s; *p != '\0'; p++)
	{
		if (*p == '\'')
			appendStringInfoChar(buf, '\'');
		appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the hash/digest errored or produced NULL on a finite
 * input, which is itself a bug -- so either is a failure, reported with the
 * SQL.  Modelled on prop_numeric.c's require_true.
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
 * hashint_deterministic: hashtext applied to the text rendering of a drawn
 * int4 is stable.  hashint4 is not relied on here (hashtext is the exposed,
 * portable surface); rendering the integer to text and hashing it twice must
 * agree, which is the determinism half of the hash contract on integer-shaped
 * input over the full INT32 range.
 */
PG_PROP(hashint_deterministic)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT hashtext((%lld)::int4::text) "
					 "= hashtext((%lld)::int4::text)",
					 (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * hashtext_equal_inputs: hashing the same string twice gives the same value.
 * The headline determinism property on text -- a non-stable hash would break
 * this and corrupt every hash lookup.
 */
PG_PROP(hashtext_equal_inputs)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT hashtext(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = hashtext(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * hash_equal_values_equal_hash: equal values hash equally.  S and S || '' are
 * the same string (concatenating the empty string is the identity), built two
 * different ways, so their hashes must match.  This exercises the
 * equal-values-hash-equally contract where the equality is proven by
 * construction rather than asserted by the test.
 */
PG_PROP(hash_equal_values_equal_hash)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT hashtext(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = hashtext(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || '')");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * hash_int_consistency: the same integer rendered to text the same way hashes
 * the same.  (n)::int4::text produced twice is byte-identical, so hashtext of
 * the two renderings must agree -- determinism checked through the cast path
 * rather than a literal.
 */
PG_PROP(hash_int_consistency)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT hashtext((%lld)::int4::text) "
					 "= hashtext(((%lld)::int4)::text)",
					 (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * hash_distributes_nothing: hashtext is a function, so repeated application to
 * the same input always agrees.  We deliberately assert ONLY determinism:
 * two different strings MAY collide, so nothing is claimed about distinctness.
 * Keeping it to self-equality keeps the oracle sound for every input.
 */
PG_PROP(hash_distributes_nothing)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT hashtext(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = hashtext(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * md5_deterministic_and_hex: md5(S) is stable and always 32 lowercase hex
 * characters.  The determinism and the fixed output shape are both bugs if
 * broken, so both are checked in one oracle: md5(S) = md5(S) AND the result
 * matches ^[0-9a-f]{32}$.
 */
PG_PROP(md5_deterministic_and_hex)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT md5(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = md5(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") AND md5(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") ~ '^[0-9a-f]{32}$'");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * sha256_length: sha256 produces 32 bytes, so its hex encoding is always 64
 * characters, independent of the input length.  A width bug in the digest or
 * its encoding would change this count.
 */
PG_PROP(sha256_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(encode(sha256(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::bytea), 'hex')) = 64");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * hashtext_stable_across_calls: hashtext is stable across separate
 * evaluations.  Computing it in two independent subqueries and comparing the
 * results catches any per-call state that leaks between invocations -- the
 * result must be identical regardless of how many times it is evaluated.
 */
PG_PROP(hashtext_stable_across_calls)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (SELECT hashtext(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = (SELECT hashtext(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
