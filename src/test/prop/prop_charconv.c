/*-------------------------------------------------------------------------
 *
 * prop_charconv.c
 *	  Property-based tests for character-type interconversion.
 *
 * Casts among text, varchar, bpchar (char(n)) and name must preserve string
 * content within each type's capacity: name holds up to NAMEDATALEN-1 (63)
 * bytes, varchar with no length limit behaves as text, char(n) blank-pads to
 * n and strips that padding on the way back to text, and varchar(n) truncates
 * to n characters.  The oracle is the identity/round-trip itself: a cast
 * chain that should return the original string but does not is a bug in the
 * type I/O or the cast implementation, not a rounding artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_charconv.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "charconv"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Draw a string over the alphabet [abc] of length in [min_len, max_len] and
 * append it as a quoted SQL literal to buf.  The alphabet is letters only, so
 * no quote-doubling is needed; the trailing-space-free guarantee comes for
 * free since the alphabet contains no spaces, which keeps the char(n) padding
 * properties sound (strip of blank padding recovers the original exactly).
 */
static void
draw_abc_literal(pg_prop_tc *tc, StringInfo buf, int min_len, int max_len)
{
	static const char alpha[] = "abc";
	size_t		len = 0;
	char	   *s;

	s = pg_prop_draw_from(tc, alpha, (size_t) min_len, (size_t) max_len, &len);

	appendStringInfoChar(buf, '\'');
	appendBinaryStringInfo(buf, s, (int) len);
	appendStringInfoChar(buf, '\'');

	pg_free(s);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the cast chain errored or returned NULL on finite inputs,
 * which is itself a bug -- so either is a failure, reported with the SQL.
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
 * text -> name -> text round-trips.  A [abc] string of length <= 10 is well
 * within NAMEDATALEN-1 (63), so name stores it whole and casting back to text
 * recovers the original: S::name::text = S.
 */
PG_PROP(text_name_roundtrip)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc_literal(tc, &s, 0, 10);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s::name::text) = %s", s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * varchar with no length limit is content-identical to text, so a round-trip
 * through it changes nothing: S::varchar::text = S.
 */
PG_PROP(varchar_unlimited_eq_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc_literal(tc, &s, 0, 10);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s::varchar::text) = %s", s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * char(n) blank-pads to n characters; casting back to text strips that
 * trailing blank padding.  With a [abc] string (no trailing spaces of its
 * own) of length <= 10 stored in char(20), the strip recovers S exactly:
 * (S::char(20))::text = S.
 */
PG_PROP(char_strips_on_text)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc_literal(tc, &s, 0, 10);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT ((%s::char(20))::text) = %s", s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * name truncates to NAMEDATALEN-1 (63) bytes.  A string of 70 'a's cast to
 * name must therefore have length exactly 63.  This property takes no drawn
 * input; it is a fixed boundary check that fails if NAMEDATALEN or the name
 * truncation rule changes.
 */
PG_PROP(name_truncates_at_63)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	require_true(tc, conn, "SELECT length(repeat('a', 70)::name) = 63");

	pg_prop_disconnect(conn);
}

/*
 * Casting text to text is the identity: S::text = S.  A trivial but sound
 * anchor -- it fails only if text input/output stops being faithful.
 */
PG_PROP(text_text_identity)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc_literal(tc, &s, 0, 10);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (%s::text) = %s", s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * varchar(n) truncates to n characters.  For S of length >= 3,
 * (S::varchar(3))::text keeps exactly the first 3 characters, which is
 * left(S, 3).  S is drawn length 3..10 so the truncation is always exercised.
 */
PG_PROP(varchar_n_text_prefix)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc_literal(tc, &s, 3, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((%s::varchar(3))::text) = left(%s, 3)",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * name equality agrees with text equality.  name uses C-collation byte
 * ordering, so for two short [abc] strings (both well within 63 bytes and
 * free of trailing spaces) equality of their name casts matches equality of
 * the strings themselves: (S::name = T::name) = (S = T).  Stated as an
 * equality rather than ordering so it is sound regardless of collation.
 */
PG_PROP(name_comparison)
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
	draw_abc_literal(tc, &s, 0, 10);
	draw_abc_literal(tc, &t, 0, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s::name = %s::name) = (%s = %s)",
					 s.data, t.data, s.data, t.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A longer cast chain exercising several conversions at once: char(10) pads S
 * to 10, ::text strips the padding back to S, ::name stores it whole (<= 63
 * bytes), and a final ::text recovers S.  With no trailing spaces in S the
 * whole chain is the identity: ((S::char(10))::text)::name::text = S.  S is
 * drawn length 0..10 so char(10) never truncates.
 */
PG_PROP(char_name_text_chain)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	draw_abc_literal(tc, &s, 0, 10);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((((%s::char(10))::text)::name)::text) = %s",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
