/*-------------------------------------------------------------------------
 *
 * prop_textsplit.c
 *	  Property-based tests for text splitting/joining functions.
 *
 * Exercises split_part(), string_to_array(), string_to_table() and
 * array_to_string() against one another.  These functions share the delimiter
 * scan that chops a string into fields; a mismatch between the array, scalar
 * and set-returning forms (or between split and its join inverse) is a bug in
 * that shared field logic.  The oracle is each function's agreement with the
 * others, so no invented formula is asserted -- only identities.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_textsplit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "textsplit"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error is itself a failure, reported with the SQL.  (Copied from
 * prop_numeric.c's require_true.)
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
 * Differential result-set oracle: two queries must return identical rows in
 * identical order.  Both callers below carry a total ORDER BY.  (Copied from
 * the area spec skeleton.)
 */
static void
require_same_rows(pg_prop_tc *tc, pg_prop_conn *conn,
				  const char *q1, const char *q2)
{
	bool		f1 = false,
				f2 = false;
	char	   *e1 = NULL,
			   *e2 = NULL,
			   *r1,
			   *r2;

	r1 = pg_prop_sql_rows(conn, q1, &f1, &e1);
	if (f1)
		pg_prop_fail(tc, "q1 failed: %s\n  SQL: %s", e1 ? e1 : "", q1);
	r2 = pg_prop_sql_rows(conn, q2, &f2, &e2);
	if (f2)
		pg_prop_fail(tc, "q2 failed: %s\n  SQL: %s", e2 ? e2 : "", q2);
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc, "mismatch:\n  q1: %s\n  q2: %s\n  r1:\n%s\n  r2:\n%s",
					 q1, q2, r1, r2);
	pg_free(r1);
	pg_free(r2);
	if (e1)
		pg_free(e1);
	if (e2)
		pg_free(e2);
}

/*
 * Draw a comma-joined string S of 1..5 pieces, each 1..3 chars from the
 * alphabet [a-c].  The alphabet excludes the delimiter ',' and the letter
 * 'q' (used as the "absent delimiter" below), so a piece can never contain a
 * delimiter and the piece count is exactly (number of commas) + 1.  Each
 * piece has length >= 1, so there are no empty leading/trailing fields and
 * the split/join round-trip is unambiguous.  Returns the number of pieces.
 */
static int
draw_pieces(pg_prop_tc *tc, StringInfo buf)
{
	int			npieces = (int) pg_prop_draw_int(tc, 1, 5);
	int			i,
				j;

	for (i = 0; i < npieces; i++)
	{
		int			plen = (int) pg_prop_draw_int(tc, 1, 3);

		if (i > 0)
			appendStringInfoChar(buf, ',');
		for (j = 0; j < plen; j++)
			appendStringInfoChar(buf,
								 (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
	}
	return npieces;
}

/*
 * 1. split_part(S, ',', k) = (string_to_array(S, ','))[k] for every part
 * number k in 1..5.  The scalar and array forms must pick the same field;
 * array indexing is 1-based and matches split_part's 1-based part number.
 * For k past the last part both yield SQL NULL/'' -- but we only check k up
 * to npieces where both are a real field, and for k beyond we rely on
 * properties 5/6; here the CASE compares the two forms with IS NOT DISTINCT
 * FROM so a NULL array element (k > npieces) matches split_part's '' only
 * when both agree.  To keep it sound we bound k to the drawn piece count.
 */
PG_PROP(split_part_matches_array)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;
	int			npieces;
	int			k;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	npieces = draw_pieces(tc, &s);
	k = (int) pg_prop_draw_int(tc, 1, npieces);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT split_part('%s', ',', %d) "
					 "= (string_to_array('%s', ','))[%d]",
					 s.data, k, s.data, k);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 2. cardinality(string_to_array(S, ',')) = (count of ',' in S) + 1.  Each
 * drawn piece is non-empty and contains no comma, so the number of fields is
 * exactly one more than the number of delimiters.  The comma count is derived
 * independently as length(S) - length(replace(S, ',', '')).
 */
PG_PROP(string_to_array_length)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	(void) draw_pieces(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT cardinality(string_to_array('%s', ',')) "
					 "= (length('%s') - length(replace('%s', ',', ''))) + 1",
					 s.data, s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 3. string_to_table(S, ',') WITH ORDINALITY must yield the same ordered
 * fields as unnest(string_to_array(S, ',')) WITH ORDINALITY.  Both forms
 * split on the same delimiter; the set-returning function and the array
 * expansion must agree field-for-field.  A total ORDER BY ord makes the
 * row-major comparison a true ordered comparison.
 */
PG_PROP(split_table_eq_array)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q1,
				q2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	(void) draw_pieces(tc, &s);

	initStringInfo(&q1);
	appendStringInfo(&q1,
					 "SELECT t FROM string_to_table('%s', ',') "
					 "WITH ORDINALITY AS st(t, ord) ORDER BY ord",
					 s.data);

	initStringInfo(&q2);
	appendStringInfo(&q2,
					 "SELECT t FROM unnest(string_to_array('%s', ',')) "
					 "WITH ORDINALITY AS u(t, ord) ORDER BY ord",
					 s.data);

	require_same_rows(tc, conn, q1.data, q2.data);

	pfree(s.data);
	pfree(q1.data);
	pfree(q2.data);
	pg_prop_disconnect(conn);
}

/*
 * 4. array_to_string(string_to_array(S, ','), ',') = S.  Splitting then
 * re-joining on the same delimiter is the identity when no field is NULL and
 * no field contains the delimiter -- guaranteed here because every drawn
 * piece is a non-empty run of [a-c] with no comma, so there are no ambiguous
 * empty edge fields.
 */
PG_PROP(array_to_string_inverse)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	(void) draw_pieces(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT array_to_string(string_to_array('%s', ','), ',') "
					 "= '%s'",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 5. split_part(S, ',', 1000) = ''.  A part number beyond the last field is
 * the empty string, never NULL and never an error.  1000 is far past the
 * maximum of 5 fields.
 */
PG_PROP(split_part_out_of_range)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	(void) draw_pieces(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT split_part('%s', ',', 1000) = ''",
					 s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 6. split_part(S, ',', -1) counts from the end, so it must equal the last
 * field, which is (string_to_array(S, ','))[cardinality(...)].  This ties the
 * negative-index scalar form back to the array form, catching an off-by-one
 * in the reverse scan.
 */
PG_PROP(split_part_negative)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	(void) draw_pieces(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT split_part('%s', ',', -1) "
					 "= (string_to_array('%s', ','))"
					 "[cardinality(string_to_array('%s', ','))]",
					 s.data, s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * 7. split_part(S, 'q', 1) = S when 'q' does not occur in S.  The alphabet is
 * [a-c] so 'q' is always absent; with no delimiter present the whole string
 * is field 1.
 */
PG_PROP(empty_delimiter_whole)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData s,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&s);
	(void) draw_pieces(tc, &s);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT split_part('%s', 'q', 1) = '%s'",
					 s.data, s.data);

	require_true(tc, conn, q.data);

	pfree(s.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
