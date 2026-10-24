/*-------------------------------------------------------------------------
 *
 * prop_varchartypmod.c
 *	  Property-based tests for varchar(n)/char(n) typmod length semantics.
 *
 * An explicit cast to varchar(n) truncates to n characters when the input is
 * longer and leaves it unchanged when it fits; char(n) blank-pads to exactly
 * n characters and compares/trims blank-insensitively.  The oracle is the
 * independent builtins left(), length(), char_length(), rtrim() plus the
 * type's own cast and comparison rules, so any disagreement is a bug in the
 * typmod coercion path (src/backend/utils/adt/varchar.c).
 *
 * This file keeps S over [abc] with NO trailing spaces, so every truncation
 * or padding effect is unambiguous (a dropped or added blank can never be
 * confused with a character of S).  That is deliberately distinct from
 * prop_char.c, whose alphabet includes a space to exercise blank trimming.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_varchartypmod.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "varchartypmod"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  The [abc] alphabet never contains a quote, but do it
 * defensively so the generated SQL is always well-formed.
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
 * Draw a string of length 0..12 from the alphabet [abc] into buf as a bare
 * string (no quoting).  No space is ever produced, so S has no trailing
 * blanks and truncation/padding effects are always observable.  Returns the
 * drawn length.
 */
static int
draw_str(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] = "abc";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 2)]);
	return n;
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
 * varchar truncation: for n <= length(S), the explicit cast S::varchar(n)
 * truncates to the first n characters, i.e. equals left(S, n).  n is drawn in
 * [0, length(S)] so the cast is always in truncation (or exact-fit) range.
 */
PG_PROP(varchar_truncates_on_cast)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			len;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	len = draw_str(tc, &s);

	/* varchar(n) requires n >= 1; skip length-0 draws (nothing to truncate) */
	if (len == 0)
	{
		pfree(s.data);
		pg_prop_disconnect(conn);
		return;
	}
	n = (int) pg_prop_draw_int(tc, 1, len);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, "::varchar(%d))::text = left(", n);
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, ", %d)", n);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * varchar no-truncate when it fits: for n >= length(S), S::varchar(n) equals
 * S unchanged (no padding for varchar).  n is drawn in
 * [length(S), length(S)+5], clamped to a minimum of 1 since varchar(n)
 * requires n >= 1.
 */
PG_PROP(varchar_no_truncate_when_fits)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			len;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	len = draw_str(tc, &s);

	n = len + (int) pg_prop_draw_int(tc, 0, 5);
	if (n < 1)
		n = 1;					/* varchar(n) requires n >= 1 */

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
 * char(n) blank-pads in storage, but the cast back to text strips the trailing
 * padding (unlike a raw bpchar, which keeps it internally).  So for S with no
 * trailing spaces and length <= n, the round-trip (S::char(n))::text recovers
 * S exactly.  S is single-byte ASCII with no trailing space; n is drawn in
 * [length(S), 20] (clamped to >= 1 for the empty-S case).
 */
PG_PROP(char_pads_to_n)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			len;
	int			n;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	len = draw_str(tc, &s);

	n = (int) pg_prop_draw_int(tc, len < 1 ? 1 : len, 20);

	/*
	 * char(n) blank-pads in storage, but casting back to text strips the
	 * trailing padding, so for S (no trailing spaces, length <= n) the
	 * round-trip S::char(n)::text recovers S exactly.
	 */
	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfo(&q, "::char(%d))::text = ", n);
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * char trim recovers the original: since S over [abc] has no trailing space,
 * rtrim(S::char(20)) equals S.  char(20) pads S with trailing blanks to width
 * 20; rtrim strips exactly those blanks, and because S itself ends in no
 * blank the recovered string is S exactly.  (length(S) <= 12 <= 20, so no
 * truncation.)
 */
PG_PROP(char_trim_recovers)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	(void) draw_str(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT rtrim((");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::char(20))) = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * char(n) comparison ignores trailing blanks: S::char(5) equals
 * (S || ' ')::char(5) whenever length(S) < 5, so both operands pad to width 5
 * and differ only in trailing blanks, which bpchar equality ignores.  S is
 * redrawn until it is short enough (length < 5) rather than rejecting cases.
 */
PG_PROP(char_compares_ignore_trailing)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			len;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	len = draw_str(tc, &s);

	/* keep S short enough that both operands fit within char(5) */
	if (len >= 5)
		s.data[s.len = 4] = '\0';

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::char(5)) = ((");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || ' ')::char(5))");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * varchar does not pad: S::varchar(20)::text equals S for any S of length
 * <= 12 (well within 20), confirming varchar stores the value verbatim with
 * no trailing-blank padding, unlike char(n).
 */
PG_PROP(varchar_text_equal)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	(void) draw_str(tc, &s);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, "::varchar(20))::text = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
