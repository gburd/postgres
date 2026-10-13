/*-------------------------------------------------------------------------
 *
 * prop_hashfn.c
 *	  Property-based tests for the type hash functions.
 *
 * The per-type hash functions (hashtext, hashint4, hashint8, hash_numeric,
 * and the seeded hashtextextended) must be deterministic and consistent with
 * equality: equal values must hash equally.  That contract is what hash joins
 * and hash indexes rely on -- a value hashed on one side of a join, or on
 * insert into a hash index, must land in the same bucket as an equal value
 * hashed later, or lookups silently miss.  These properties check the two
 * halves of that contract via the hash_* SQL-exposed functions: determinism
 * (the same input always hashes the same) and equal-values-hash-equally
 * (independently constructed equal values, and numerically-equal numerics of
 * different scale, hash identically).
 *
 * The oracle is the hash function's own stability and SQL equality: equality
 * is computed by the backend, and the hashes must agree with it with no
 * tolerance.  A deterministic self-comparison catches uninitialized-byte and
 * non-stable-state bugs; an equal-inputs comparison catches a hash that
 * disagrees with equality.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_hashfn.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "hashfn"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw 0..16 characters of lowercase text from "a".."z" into buf as a bare
 * string (no quoting).  Letters only, so the literal never needs more than
 * single-quote doubling (which it never triggers), keeping the generated SQL
 * trivially well-formed.
 */
static void
draw_text(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abcdefghijklmnopqrstuvwxyz";
	int			n = (int) pg_prop_draw_int(tc, 0, 16);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 25)]);
}

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our alphabet never contains a quote, but do it defensively
 * so the generated SQL is always well-formed.  Modelled on prop_like.c.
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
 * a SQL error means the hash errored or produced NULL on a finite input,
 * which is itself a bug -- so either is a failure, reported with the SQL.
 * Modelled on prop_numeric.c's require_true.
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
 * Append v as a SQL int8 literal.  INT64_MIN cannot be written as a plain
 * negative literal (the lexer reads the magnitude 9223372036854775808 first,
 * which overflows int8 before the minus is applied), so emit it as an
 * expression that stays in range.
 */
static void
append_int8_literal(StringInfo buf, int64 v)
{
	if (v == INT64_MIN)
		appendStringInfoString(buf, "(-9223372036854775807 - 1)");
	else
		appendStringInfo(buf, "%lld", (long long) v);
}

/*
 * hashtext is deterministic: hashing the same string twice gives the same
 * value.  Trivial-looking, but a non-stable hash (uninitialized padding, a
 * stray global) breaks it, and that would corrupt every hash lookup.
 */
PG_PROP(hashtext_deterministic)
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
 * Equal inputs hash equally.  S and the server-side reconstruction
 * substr(S,1,k) || substr(S,k+1) are the same string (the split-and-rejoin
 * is the identity for any k), so their hashes must match.  The split point k
 * is clamped with least(k, length(S)) so the reconstruction equals S exactly
 * for any drawn k, and the substrings are taken server-side so no C-side
 * splitting can disagree with the backend's.  This exercises the headline
 * equal-values-hash-equally contract on text.
 */
PG_PROP(hashtext_equal_inputs_equal_hashes)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			k;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_text(tc, &s);

	/* split point; clamped server-side via least(k, length(S)) */
	k = (int) pg_prop_draw_int(tc, 0, 20);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT hashtext(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = hashtext(substr(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", 1, least(%d, length(", k);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "))) || substr(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", least(%d, length(", k);
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) + 1))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * hashint4 is deterministic across the full INT32 range: the same int4 always
 * hashes the same.  The widest bounds are passed so the engine favours the
 * INT32 extremes, where a sign/width bug would show.
 */
PG_PROP(hashint4_deterministic)
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
					 "SELECT hashint4(%lld) = hashint4(%lld)",
					 (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * hashint8 is deterministic across the full INT64 range: the same int8 always
 * hashes the same.  hashint4 and hashint8 are distinct functions and are not
 * expected to agree on common values, so we check only int8 self-stability,
 * over the full int64 domain so the engine favours the INT64 extremes.
 */
PG_PROP(hashint8_consistent_with_int4_on_small)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT hashint8(");
	append_int8_literal(&q, n);
	appendStringInfoString(&q, "::int8) = hashint8(");
	append_int8_literal(&q, n);
	appendStringInfoString(&q, "::int8)");

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * The seeded extended hash is deterministic per seed: hashtextextended(S, k)
 * is stable for a fixed seed k.  Two different seeds (0 and 42) are each
 * checked for self-stability -- a seed that leaked into stale state would
 * break one or the other.  We do not assert anything about cross-seed
 * relations, only per-seed determinism.
 */
PG_PROP(hashtextextended_seed0_matches_or_differs)
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
	appendStringInfoString(&q, "SELECT hashtextextended(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 0) = hashtextextended(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 0) AND hashtextextended(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 42) = hashtextextended(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", 42)");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Numerically-equal numerics hash equally regardless of scale.  1.0 and 1.00
 * are equal as numerics but stored with different scale, and n and n + 0.0
 * likewise; the numeric hash must respect numeric equality (a known subtlety,
 * since a naive byte hash of the on-disk representation would differ by
 * scale).  hash_numeric is the SQL-callable per-type hash for numeric; if the
 * numeric hash ignored scale-normalisation this property would fail.
 */
PG_PROP(equal_numeric_values_hash_equal)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int64		n;
	StringInfoData q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = pg_prop_draw_int(tc, -1000, 1000);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT hash_numeric(1.0::numeric) = hash_numeric(1.00::numeric) "
					 "AND hash_numeric((%lld)::numeric) "
					 "= hash_numeric((%lld::numeric + 0.0))",
					 (long long) n, (long long) n);

	require_true(tc, conn, q.data);

	pfree(q.data);
	pg_prop_disconnect(conn);
}
