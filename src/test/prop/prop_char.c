/*-------------------------------------------------------------------------
 *
 * prop_char.c
 *	  Property-based tests for char(n)/varchar(n) length semantics.
 *
 * bpchar (char(n)) blank-pads and blank-trims on comparison; varchar(n)
 * rejects over-length input; these check the documented padding/trimming and
 * length-limit semantics (src/backend/utils/adt/varchar.c).
 *
 * The oracle is independent builtins -- length(), char_length(), rtrim() --
 * together with the type's own cast and comparison rules.  Two subtleties
 * drive the queries here:
 *
 *	 - length()/char_length() on a bpchar IGNORE trailing blanks and return
 *	   the trimmed length, so padding is observed by casting the bpchar to
 *	   text first ((x::char(n))::text keeps the n-width padding) and taking
 *	   the length of that.
 *
 *	 - an EXPLICIT cast '...'::varchar(n) or '...'::char(n) TRUNCATES silently
 *	   (no error); the length-violation error path fires only on assignment /
 *	   INSERT.  So over-length explicit casts are checked for truncation to n,
 *	   not for an error.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_char.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "char"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our alphabets never contain a quote, but do it defensively
 * so the generated SQL is always well-formed.
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
 * Draw 0..maxlen characters from the alphabet "ab " (two letters plus a
 * space) into buf as a bare string (no quoting).  Including the space means
 * generated strings routinely carry trailing blanks, which is exactly what
 * the bpchar blank-pad / blank-trim semantics must handle.
 */
static void
draw_str(pg_prop_tc *tc, StringInfo buf, int maxlen)
{
	static const char alphabet[] = "ab ";
	int			n = (int) pg_prop_draw_int(tc, 0, maxlen);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 2)]);
}

/*
 * Draw a run of 0..maxlen non-space characters from "ab" into buf.  Used for
 * over-length strings whose truncation must be observable: a trailing space
 * that got dropped by truncation would be indistinguishable from blank
 * trimming, so these strings never contain a space.
 */
static void
draw_str_nonspace(pg_prop_tc *tc, StringInfo buf, int maxlen)
{
	static const char letters[] = "ab";
	int			n = (int) pg_prop_draw_int(tc, 0, maxlen);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 1)]);
}

/*
 * Draw a declared length n in [1, 20].
 */
static int
draw_len(pg_prop_tc *tc)
{
	return (int) pg_prop_draw_int(tc, 1, 20);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the query errored or produced NULL on the generated
 * inputs, which is itself a bug -- so either is a failure, reported with the
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
 * char(n) blank-pads its value to exactly n characters.  length() on the
 * bpchar itself would ignore the trailing blanks, so cast to text first --
 * ((S::char(n))::text) preserves the n-width padding -- and require its
 * length to be n.  S is drawn no longer than n so no truncation is involved.
 */
PG_PROP(bpchar_blank_pads_to_n)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = draw_len(tc);

	initStringInfo(&s);
	draw_str(tc, &s, n);		/* length(S) <= n */

	initStringInfo(&q);

	/*
	 * char(n) pads its value to n characters of STORAGE.  That padding is only
	 * observable via octet_length (the raw stored bytes): length(),
	 * char_length(), a ::text cast, and even || all trim trailing blanks by
	 * bpchar's SQL semantics.  S is drawn from a single-byte ASCII alphabet,
	 * so n padded characters are exactly n bytes.
	 */
	appendStringInfoString(&q, "SELECT octet_length(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, "::char(%d)) = %d", n, n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * char(n) comparison ignores trailing blanks: S and S with three extra
 * trailing spaces compare equal as char(n).  S is kept short enough that both
 * operands fit in n (length(S) + 3 <= n), so the extra spaces are genuine
 * trailing blanks rather than truncated padding.  This is the documented
 * bpchar blank-insensitive comparison.
 */
PG_PROP(bpchar_trailing_blanks_ignored_in_compare)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	/* need room for S plus three trailing spaces within n */
	n = (int) pg_prop_draw_int(tc, 3, 20);

	initStringInfo(&s);
	draw_str(tc, &s, n - 3);	/* length(S) <= n - 3 */

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, "::char(%d)) = (", n);
	/* same string with three extra trailing spaces */
	appendStringInfoChar(&q, '\'');
	for (const char *p = s.data; *p != '\0'; p++)
	{
		if (*p == '\'')
			appendStringInfoChar(&q, '\'');
		appendStringInfoChar(&q, *p);
	}
	appendStringInfoString(&q, "   '");
	appendStringInfo(&q, "::char(%d))", n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * varchar within its limit stores the string unchanged -- no padding, no
 * trimming.  S is drawn no longer than n, so (S::varchar(n))::text must equal
 * S exactly.
 */
PG_PROP(varchar_roundtrip_within_limit)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = draw_len(tc);

	initStringInfo(&s);
	draw_str(tc, &s, n);		/* length(S) <= n */

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, "::varchar(%d))::text = ", n);
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * An explicit cast of an over-length string to varchar(n) TRUNCATES silently
 * to n characters (it does NOT error -- the error path is only on
 * assignment/INSERT).  S is drawn strictly longer than n and from non-space
 * letters so the truncation is observable (a dropped trailing blank would be
 * ambiguous with trimming).  length() on the varchar is its real length, so
 * require it to equal n.
 */
PG_PROP(varchar_explicit_cast_truncates)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			n;
	int			extra;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = draw_len(tc);

	/* S strictly longer than n: length in [n+1, n+5] */
	extra = (int) pg_prop_draw_int(tc, 1, 5);

	initStringInfo(&s);
	draw_str_nonspace(tc, &s, n + extra);

	/*
	 * draw_str_nonspace draws 0..n+extra; force the length strictly above n
	 * by padding with 'a' if the draw came up short.
	 */
	while ((int) strlen(s.data) <= n)
		appendStringInfoChar(&s, 'a');

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT length(");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, "::varchar(%d)) = %d", n, n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Casting S to varchar(n) and then to char(n) equals casting S directly to
 * char(n), for S fitting in n.  bpchar's padding is reached by the same final
 * value regardless of the intermediate varchar hop.
 */
PG_PROP(bpchar_cast_from_varchar_consistent)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = draw_len(tc);

	initStringInfo(&s);
	draw_str(tc, &s, n);		/* length(S) <= n */

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT ((");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, "::varchar(%d))::char(%d)) = (", n, n);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, "::char(%d))", n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Trailing blanks do not affect a char(n) value: S::char(n) equals
 * rtrim(S)::char(n), for S fitting in n.  rtrim() is an independent builtin,
 * so this cross-checks that bpchar's stored value is blank-canonical.
 */
PG_PROP(bpchar_rtrim_equivalence)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	n = draw_len(tc);

	initStringInfo(&s);
	draw_str(tc, &s, n);		/* length(S) <= n */

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, "::char(%d)) = (rtrim(", n);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ")::char(%d))", n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
