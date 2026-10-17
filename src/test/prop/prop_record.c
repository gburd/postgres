/*-------------------------------------------------------------------------
 *
 * prop_record.c
 *	  Property-based tests for composite/record type I/O and comparison.
 *
 * record_in/record_out and record comparison (src/backend/utils/adt/rowtypes.c)
 * have had quoting and NULL-handling fixes; these check anonymous-record text
 * round-trip and that record comparison is lexicographic over the fields.
 *
 * record_out must quote and escape field values that contain the record
 * delimiters -- commas, parentheses, double quotes, backslashes, spaces -- and
 * record_in must parse that text back to the identical record; a quoting bug
 * breaks the text round-trip.  Record comparison (record_cmp and the
 * record_lt/eq/gt operators) walks the fields left to right, so it must be a
 * lexicographic total order; a short-circuit or sign bug breaks that.
 *
 * The oracle is independent of rowtypes.c: round-trip compares a value to
 * itself through text::composite, and the ordering properties compare the
 * record operators to an explicit lexicographic expression built from the
 * scalar int comparisons.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_record.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "record"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Alphabet for text fields.  It deliberately includes exactly the characters
 * record_out must quote/escape -- comma, double quote, backslash, open and
 * close parens, and space -- alongside a few letters.  These are the
 * round-trip-critical characters; if they are drawn into a field value,
 * record_out has to quote the field and record_in has to parse it back.
 */
static const char text_alphabet[] = ",\"\\() ab";

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the record I/O or comparison errored or produced NULL on
 * valid inputs, which is itself a bug -- so either is a failure, reported with
 * the SQL.  Mirrors require_true in prop_numeric.c.
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
 * Connect to the test cluster and create the composite types this area needs.
 * Each property opens its own connection (the Tier-B pattern), so the types
 * are created per connection; CASCADE + IF EXISTS makes the DDL idempotent
 * against a connection that reused a backend.  On any failure the property is
 * failed with the message.
 */
static pg_prop_conn *
connect_and_setup(pg_prop_tc *tc)
{
	pg_prop_conn *conn;
	char	   *err = NULL;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "DROP TYPE IF EXISTS prop_rec2 CASCADE; "
						  "CREATE TYPE prop_rec2 AS (a int, b int)",
						  &err))
		pg_prop_fail(tc, "setup failed (prop_rec2): %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "DROP TYPE IF EXISTS prop_rec3 CASCADE; "
						  "CREATE TYPE prop_rec3 AS (a int, b int, c int)",
						  &err))
		pg_prop_fail(tc, "setup failed (prop_rec3): %s", err ? err : "");

	if (!pg_prop_sql_exec(conn,
						  "DROP TYPE IF EXISTS prop_rect CASCADE; "
						  "CREATE TYPE prop_rect AS (a text, b text)",
						  &err))
		pg_prop_fail(tc, "setup failed (prop_rect): %s", err ? err : "");

	return conn;
}

/*
 * Draw one integer field value into buf as a SQL expression: either a small
 * integer (possibly negative) cast to int, or NULL.  NULL fields exercise the
 * NULL-handling path in record_out/record_in.
 */
static void
draw_int_field(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_bool(tc))
		appendStringInfoString(buf, "NULL::int");
	else
		appendStringInfo(buf, "(%d)::int",
						 (int) pg_prop_draw_int(tc, -5, 5));
}

/*
 * Draw one non-NULL integer field value into buf, for the ordering
 * properties (comparison of NULL fields is three-valued and would not match
 * the two-valued lexicographic oracle).
 */
static void
draw_int_field_nonnull(pg_prop_tc *tc, StringInfo buf)
{
	appendStringInfo(buf, "(%d)::int", (int) pg_prop_draw_int(tc, -3, 3));
}

/*
 * Draw one text field value into buf as a single-quoted SQL literal, or NULL.
 * The literal is built from text_alphabet (the quoting-critical characters),
 * and single quotes are doubled defensively even though the alphabet has
 * none, so the literal is always well-formed.
 */
static void
draw_text_field(pg_prop_tc *tc, StringInfo buf)
{
	char	   *s;
	size_t		len;

	if (pg_prop_draw_bool(tc))
	{
		appendStringInfoString(buf, "NULL::text");
		return;
	}

	s = pg_prop_draw_from(tc, text_alphabet, 0, 6, &len);

	appendStringInfoChar(buf, '\'');
	for (size_t i = 0; i < len; i++)
	{
		if (s[i] == '\'')
			appendStringInfoChar(buf, '\'');
		appendStringInfoChar(buf, s[i]);
	}
	appendStringInfoString(buf, "'::text");

	pg_free(s);
}

/*
 * Text round-trip through record_out/record_in for an all-int composite:
 *
 *		(ROW(v1,...)::prop_rec3)::text::prop_rec3
 *			IS NOT DISTINCT FROM ROW(v1,...)::prop_rec3
 *
 * The ncols-wide prop_rec3 fixes the shape so the cast is well-typed; building
 * the three literals once and substituting them on both sides makes it a
 * genuine out-then-in round-trip rather than a tautology.  IS NOT DISTINCT
 * FROM so an all-NULL row still compares equal.  (ncols is 2..3 fields of
 * prop_rec3's three; extra fields are filled with NULL so the shape matches.)
 */
PG_PROP(text_roundtrip_int_record)
{
	pg_prop_conn *conn;
	StringInfoData f[3];
	StringInfoData row,
				q;
	int			ncols = (int) pg_prop_draw_int(tc, 2, 3);
	int			i;

	conn = connect_and_setup(tc);

	initStringInfo(&row);
	appendStringInfoString(&row, "ROW(");
	for (i = 0; i < 3; i++)
	{
		initStringInfo(&f[i]);
		if (i < ncols)
			draw_int_field(tc, &f[i]);
		else
			appendStringInfoString(&f[i], "NULL::int");

		if (i > 0)
			appendStringInfoString(&row, ", ");
		appendStringInfoString(&row, f[i].data);
	}
	appendStringInfoString(&row, ")::prop_rec3");

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s)::text::prop_rec3 IS NOT DISTINCT FROM (%s)",
					 row.data, row.data);

	require_true(tc, conn, q.data);

	for (i = 0; i < 3; i++)
		pfree(f[i].data);
	pfree(row.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Text round-trip for a two-field text composite whose field values are drawn
 * from the quoting-critical alphabet (comma, quote, backslash, parens,
 * space).  This is the high-value property: it forces record_out to quote and
 * escape those characters and record_in to parse the quoted text back:
 *
 *		(ROW(v1,v2)::prop_rect)::text::prop_rect
 *			IS NOT DISTINCT FROM ROW(v1,v2)::prop_rect
 *
 * IS NOT DISTINCT FROM so an all-NULL row still compares equal.
 */
PG_PROP(text_roundtrip_text_record)
{
	pg_prop_conn *conn;
	StringInfoData f1,
				f2,
				row,
				q;

	conn = connect_and_setup(tc);

	initStringInfo(&f1);
	initStringInfo(&f2);
	draw_text_field(tc, &f1);
	draw_text_field(tc, &f2);

	initStringInfo(&row);
	appendStringInfo(&row, "ROW(%s, %s)::prop_rect", f1.data, f2.data);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (%s)::text::prop_rect IS NOT DISTINCT FROM (%s)",
					 row.data, row.data);

	require_true(tc, conn, q.data);

	pfree(f1.data);
	pfree(f2.data);
	pfree(row.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Record comparison is lexicographic over the fields.  For two two-int
 * records R1 = ROW(a1,b1), R2 = ROW(a2,b2):
 *
 *		(R1 < R2) = ((a1 < a2) OR (a1 = a2 AND b1 < b2))
 *
 * The right side is an independent lexicographic oracle built from the scalar
 * int comparisons, so a short-circuit or sign bug in record_cmp fails here.
 * Fields are non-NULL to keep both sides two-valued.
 */
PG_PROP(comparison_lexicographic)
{
	pg_prop_conn *conn;
	StringInfoData a1,
				b1,
				a2,
				b2,
				q;

	conn = connect_and_setup(tc);

	initStringInfo(&a1);
	initStringInfo(&b1);
	initStringInfo(&a2);
	initStringInfo(&b2);
	draw_int_field_nonnull(tc, &a1);
	draw_int_field_nonnull(tc, &b1);
	draw_int_field_nonnull(tc, &a2);
	draw_int_field_nonnull(tc, &b2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (ROW(%s, %s)::prop_rec2 < ROW(%s, %s)::prop_rec2) "
					 "= ((%s < %s) OR (%s = %s AND %s < %s))",
					 a1.data, b1.data, a2.data, b2.data,
					 a1.data, a2.data, a1.data, a2.data, b1.data, b2.data);

	require_true(tc, conn, q.data);

	pfree(a1.data);
	pfree(b1.data);
	pfree(a2.data);
	pfree(b2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * Record comparison is a total order: for any two non-NULL two-int records
 * exactly one of <, =, > holds, so the three boolean-to-int results sum to 1:
 *
 *		(R1<R2)::int + (R1=R2)::int + (R1>R2)::int = 1
 *
 * A missing or overlapping case in record_cmp (e.g. a value that is neither
 * less, equal, nor greater, or two of them firing at once) breaks this.
 */
PG_PROP(comparison_total_order)
{
	pg_prop_conn *conn;
	StringInfoData a1,
				b1,
				a2,
				b2,
				q;

	conn = connect_and_setup(tc);

	initStringInfo(&a1);
	initStringInfo(&b1);
	initStringInfo(&a2);
	initStringInfo(&b2);
	draw_int_field_nonnull(tc, &a1);
	draw_int_field_nonnull(tc, &b1);
	draw_int_field_nonnull(tc, &a2);
	draw_int_field_nonnull(tc, &b2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT ((ROW(%s, %s)::prop_rec2 < ROW(%s, %s)::prop_rec2)::int "
					 "+ (ROW(%s, %s)::prop_rec2 = ROW(%s, %s)::prop_rec2)::int "
					 "+ (ROW(%s, %s)::prop_rec2 > ROW(%s, %s)::prop_rec2)::int) = 1",
					 a1.data, b1.data, a2.data, b2.data,
					 a1.data, b1.data, a2.data, b2.data,
					 a1.data, b1.data, a2.data, b2.data);

	require_true(tc, conn, q.data);

	pfree(a1.data);
	pfree(b1.data);
	pfree(a2.data);
	pfree(b2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
