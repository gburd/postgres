/*-------------------------------------------------------------------------
 *
 * prop_textfns.c
 *	  Property-based tests for text functions not covered by text/strfunc/
 *	  substring/stringagg: concat, concat_ws, format, reverse, quote_literal,
 *	  md5, and the encode/decode hex round-trip.
 *
 * The oracle is the SQL algebra these functions must satisfy: reverse is an
 * involution and length-preserving; concat treats NULL as empty while
 * concat_ws skips NULL arguments; format('%s', S) is just S; quote_literal
 * over a quote-free string is S wrapped in single quotes; encode/decode hex
 * composed with convert_to/convert_from round-trips exactly; and a hex md5 is
 * always 32 characters.  Each identity is checked with a quote-free [abc]
 * alphabet so SQL literal quoting is trivial and the only variable is the
 * function under test; a false result is a bug in that function, not a
 * quoting or encoding artefact.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_textfns.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "textfns"
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
 * Draw 0..12 characters from the quote-free single-byte alphabet "abc" into
 * buf as a bare string (no quoting).  All ASCII, never a single quote, so
 * length() counts characters exactly, reverse() is exact, and quoting is
 * trivial.
 */
static void
draw_text(pg_prop_tc *tc, StringInfo buf)
{
	static const char alphabet[] = "abc";
	int			n = (int) pg_prop_draw_int(tc, 0, 12);

	for (int i = 0; i < n; i++)
		appendStringInfoChar(buf, alphabet[pg_prop_draw_int(tc, 0, 2)]);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the function errored or produced NULL on valid inputs,
 * which is itself a failure, reported with the SQL.
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
 * reverse is an involution: reverse(reverse(S)) = S.
 */
PG_PROP(reverse_involution)
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
	appendStringInfoString(&q, "SELECT reverse(reverse(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * concat_ws skips NULL arguments: concat_ws(',', S, NULL, T) equals
 * concat_ws(',', S, T).  A NULL in the middle must not emit a stray separator.
 */
PG_PROP(concat_ws_skips_null)
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
	draw_text(tc, &s);
	draw_text(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT concat_ws(',', ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", NULL, ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") = concat_ws(',', ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * concat treats NULL as empty: concat(S, NULL, T) = S || T.  The dropped NULL
 * contributes nothing, so the result is just the two literals concatenated.
 */
PG_PROP(concat_null_is_empty)
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
	draw_text(tc, &s);
	draw_text(tc, &t);

	initStringInfo(&q);
	appendStringInfoString(&q, "SELECT concat(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ", NULL, ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ") = (");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || ");
	append_sql_literal(&q, t.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(t.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * format('%s', S) is the identity on text: the %s conversion inserts the
 * argument verbatim.
 */
PG_PROP(format_s_equals_value)
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
	appendStringInfoString(&q, "SELECT format('%s', ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * quote_literal wraps a value in single quotes and doubles any embedded
 * quote.  For a quote-free [abc] string there is nothing to double, so the
 * result is exactly '''' || S || '''' (SQL '''' is one literal quote char;
 * in this C source each is written as a four-single-quote run inside the
 * literal below).  Comparing against the SQL-built expected keeps the oracle
 * independent of any C-side rendering.
 */
PG_PROP(quote_literal_roundtrip)
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
	appendStringInfoString(&q, "SELECT quote_literal(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ") = ('''' || ");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, " || '''')");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * encode/decode hex round-trips through the byte representation:
 * convert_from(decode(encode(convert_to(S,'UTF8'),'hex'),'hex'),'UTF8') = S.
 * convert_to turns S into its UTF-8 bytes, encode renders those as hex text,
 * decode parses them back to the same bytes, and convert_from reads UTF-8,
 * so the whole composition is the identity.
 */
PG_PROP(encode_decode_hex_roundtrip)
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
	appendStringInfoString(&q,
						   "SELECT convert_from(decode(encode(convert_to(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q,
						   ", 'UTF8'), 'hex'), 'hex'), 'UTF8') = ");
	append_sql_literal(&q, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * A hex md5 digest is always 32 characters, independent of the input length.
 */
PG_PROP(md5_length)
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
	appendStringInfoString(&q, "SELECT length(md5(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = 32");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * reverse is length-preserving: length(reverse(S)) = length(S).  Reordering
 * characters never adds or drops one.
 */
PG_PROP(reverse_length_preserved)
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
	appendStringInfoString(&q, "SELECT length(reverse(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")) = length(");
	append_sql_literal(&q, s.data);
	appendStringInfoString(&q, ")");

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
