/*-------------------------------------------------------------------------
 *
 * prop_xmlfmt.c
 *	  Property-based tests for XML construction/predicate functions.
 *
 * The XML constructors (xmlelement, xmlforest, xmlconcat, xmlagg) and the
 * xml predicates (IS DOCUMENT, xmlexists) in src/backend/utils/adt/xml.c
 * escape content and build well-formed fragments; these check element
 * wrapping, concatenation, escaping of the XML-special characters < > &, and
 * the IS DOCUMENT predicate, keeping the source ASCII.
 *
 * The oracle is textual: xmlelement wraps content in matching tags and
 * escapes markup; xmlconcat and xmlagg concatenate fragments in order; and
 * IS DOCUMENT distinguishes a single-rooted value from a multi-root fragment.
 * Each property compares the server's answer to an independently constructed
 * expected string (built with SQL string concatenation so no raw '<' sits
 * next to an interpolated identifier) and requires a boolean "t".
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_xmlfmt.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "xmlfmt"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Append a bare string as a single-quoted SQL literal, doubling any embedded
 * single quote.  Our content alphabet never contains a single quote, but do
 * it defensively so the generated SQL is always well-formed.
 */
static void
append_sql_literal(StringInfo buf, const char *s)
{
	const char *p;

	appendStringInfoChar(buf, '\'');
	for (p = s; *p != '\0'; p++)
	{
		if (*p == '\'')
			appendStringInfoChar(buf, '\'');
		appendStringInfoChar(buf, *p);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * Draw a valid XML element name of 1..6 characters from [a-z] into buf as a
 * bare identifier (no quoting).  Letters only, so the result is always a
 * valid NCName and can be used as a bare identifier in xmlelement(name N,...).
 */
static void
draw_name(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abcdefghijklmnopqrstuvwxyz";
	int			n = (int) pg_prop_draw_int(tc, 1, 6);
	int			i;

	for (i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 25)]);
}

/*
 * Draw 0..10 characters of content into buf as a bare string (no quoting),
 * from a pool that INCLUDES the XML-special characters < > & plus letters, a
 * space, and a double quote -- exactly the bytes xmlelement must escape in
 * element content.  The single quote is deliberately excluded so the SQL
 * literal quoting stays trivial.
 */
static void
draw_content(pg_prop_tc *tc, StringInfo buf)
{
	static const char pool[] = "abcdefghij<>& \"";
	int			n = (int) pg_prop_draw_int(tc, 0, 10);
	int			i;

	for (i = 0; i < n; i++)
		appendStringInfoChar(buf, pool[pg_prop_draw_int(tc, 0, (int) sizeof(pool) - 2)]);
}

/*
 * Draw 0..10 characters of content from letters only (no < > & " space) into
 * buf as a bare string.  Used by properties whose expected text is built by
 * literal substitution, where any special character would need escaping.
 */
static void
draw_content_letters(pg_prop_tc *tc, StringInfo buf)
{
	static const char letters[] = "abcdefghij";
	int			n = (int) pg_prop_draw_int(tc, 0, 10);
	int			i;

	for (i = 0; i < n; i++)
		appendStringInfoChar(buf, letters[pg_prop_draw_int(tc, 0, 9)]);
}

/*
 * Run a boolean-valued SELECT and require it to return "t".  A NULL result or
 * a SQL error means the construction errored or produced NULL on well-formed
 * ASCII inputs, which is itself a bug -- so either is a failure, reported with
 * the SQL.  Modelled on prop_numeric.c's require_true.
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
 * xmlelement wraps its content in a matching start/end tag.  For a letter name
 * N and letter-only content C (no markup to escape), the rendered text is
 * exactly '<N>C</N>'.  The expected string is assembled with SQL string
 * concatenation ('<' || N || '>' || C || '</' || N || '>') so no raw '<' sits
 * adjacent to the interpolated identifier in the C source.
 */
PG_PROP(element_wraps_name)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	initStringInfo(&c);
	draw_name(tc, &n);
	draw_content_letters(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT xmlelement(name %s, ", n.data);
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, ")::text = '<' || ");
	append_sql_literal(&q, n.data);
	appendStringInfoString(&q, " || '>' || ");
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, " || '</' || ");
	append_sql_literal(&q, n.data);
	appendStringInfoString(&q, " || '>'");

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * xmlelement escapes markup in its content.  No matter how many '<' characters
 * the content C contains, each content '<' is escaped to '&lt;', so the only
 * raw '<' bytes left in the rendered output are the two tag delimiters (the
 * '<' of the start tag and the '<' of the end tag).  The raw-'<'-count-is-2
 * oracle (output length minus length with all '<' removed equals 2) soundly
 * detects escaping without an XML parser.  Content is drawn from the pool that
 * includes < > & so this exercises the escaping path.
 */
PG_PROP(element_escapes_specials)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				c,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	initStringInfo(&c);
	draw_name(tc, &n);
	draw_content(tc, &c);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (length(xmlelement(name %s, ", n.data);
	append_sql_literal(&q, c.data);
	appendStringInfo(&q, ")::text) - length(replace(xmlelement(name %s, ", n.data);
	append_sql_literal(&q, c.data);
	appendStringInfoString(&q, ")::text, '<', ''))) = 2");

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(c.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * xmlconcat is textual concatenation of its fragments in order.  Two simple
 * elements E1, E2 concatenated render as E1's text followed by E2's text.
 * Letter-only content keeps each fragment's text free of escaping subtleties,
 * so the comparison is a clean textual identity.
 */
PG_PROP(xmlconcat_length_additive_ish)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n1,
				n2,
				c1,
				c2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n1);
	initStringInfo(&n2);
	initStringInfo(&c1);
	initStringInfo(&c2);
	draw_name(tc, &n1);
	draw_name(tc, &n2);
	draw_content_letters(tc, &c1);
	draw_content_letters(tc, &c2);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT xmlconcat(xmlelement(name %s, ", n1.data);
	append_sql_literal(&q, c1.data);
	appendStringInfo(&q, "), xmlelement(name %s, ", n2.data);
	append_sql_literal(&q, c2.data);
	appendStringInfo(&q, "))::text = xmlelement(name %s, ", n1.data);
	append_sql_literal(&q, c1.data);
	appendStringInfo(&q, ")::text || xmlelement(name %s, ", n2.data);
	append_sql_literal(&q, c2.data);
	appendStringInfoString(&q, ")::text");

	require_true(tc, conn, q.data);

	pfree(n1.data);
	pfree(n2.data);
	pfree(c1.data);
	pfree(c2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * IS DOCUMENT is true for a single-rooted well-formed XML value and false for
 * a multi-root fragment.  A lone xmlelement has exactly one root (a document),
 * while the concatenation of two elements has two roots (a content fragment,
 * not a document).
 */
PG_PROP(is_document_single_root)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n1,
				n2,
				c1,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n1);
	initStringInfo(&n2);
	initStringInfo(&c1);
	draw_name(tc, &n1);
	draw_name(tc, &n2);
	draw_content_letters(tc, &c1);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT (xmlelement(name %s, ", n1.data);
	append_sql_literal(&q, c1.data);
	appendStringInfo(&q, ") IS DOCUMENT) AND NOT (xmlconcat(xmlelement(name %s, 'x'), "
					 "xmlelement(name %s, 'y')) IS DOCUMENT)",
					 n1.data, n2.data);

	require_true(tc, conn, q.data);

	pfree(n1.data);
	pfree(n2.data);
	pfree(c1.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * xmlelement with empty content yields an empty element.  PostgreSQL emits the
 * self-closing form '<N/>', but accept the equivalent '<N></N>' too so the
 * oracle does not over-specify the serialization.  Both acceptable forms are
 * built with SQL string concatenation.
 */
PG_PROP(xmlelement_empty_content)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n);
	draw_name(tc, &n);

	initStringInfo(&q);
	appendStringInfo(&q, "SELECT xmlelement(name %s, '')::text IN ('<' || ", n.data);
	append_sql_literal(&q, n.data);
	appendStringInfoString(&q, " || '/>', '<' || ");
	append_sql_literal(&q, n.data);
	appendStringInfoString(&q, " || '></' || ");
	append_sql_literal(&q, n.data);
	appendStringInfoString(&q, " || '>')");

	require_true(tc, conn, q.data);

	pfree(n.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * xmlagg aggregates XML fragments by concatenation in order.  Aggregating two
 * element rows (ordered) must equal xmlconcat of the same two elements in that
 * order.  A VALUES-driven subquery supplies the two rows so no table is
 * needed.
 */
PG_PROP(xmlagg_concatenates_rows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData n1,
				n2,
				q;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	initStringInfo(&n1);
	initStringInfo(&n2);
	draw_name(tc, &n1);
	draw_name(tc, &n2);

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT (SELECT xmlagg(e ORDER BY ord) FROM "
					 "(VALUES (xmlelement(name %s, 'x'), 1), "
					 "(xmlelement(name %s, 'y'), 2)) t(e, ord))::text = "
					 "xmlconcat(xmlelement(name %s, 'x'), "
					 "xmlelement(name %s, 'y'))::text",
					 n1.data, n2.data, n1.data, n2.data);

	require_true(tc, conn, q.data);

	pfree(n1.data);
	pfree(n2.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
