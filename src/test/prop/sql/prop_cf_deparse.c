/*-------------------------------------------------------------------------
 *
 * prop_cf_deparse.c
 *	  Deparse round trip: what ruleutils prints must read back as the same
 *	  thing.
 *
 * pg_get_viewdef / pg_get_ruledef are how pg_dump writes views and rules,
 * so a deparse that drops a needed parenthesis, quote or cast silently
 * changes a view's meaning across a dump and restore -- or makes the dump
 * unrestorable.  The open commitfest has five such bugs.
 *
 * The property draws a view body from a small grammar of the expressions
 * those bugs involve, creates the view, deparses it (pretty and not), creates
 * a second view from the deparsed text, and requires:
 *	 (1) the deparsed text is accepted (a syntax error is a failure);
 *	 (2) both views return the same rows over drawn table data;
 *	 (3) deparsing the second view gives the same text (a fixed point).
 *
 *	 CF 7056  window names that are unreserved keywords (rows, range, groups)
 *	 CF 7441  sublink tests on OR/AND/arithmetic, pretty-printed
 *	 CF 7451  x IS DOCUMENT as an operand
 *	 CF 7452  operators on ROW(...)::record
 *	 CF 7442  an UPDATE/DELETE target alias named "set", in a rule
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "cf_deparse"
#include "pg_prop.h"

#include "lib/stringinfo.h"

static pg_prop_conn *
dp_connect(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

static void
dp_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "setup failed: %s\n  SQL: %.1500s", err ? err : "", sql);
}

/* append one drawn select-list expression over cfd_dp(a bool, b bool, c int, d int, x xml) */
static void
dp_expr(pg_prop_tc *tc, StringInfo b)
{
	static const char *const bools[] = {"a", "b", "(a OR b)", "(a AND b)", "(NOT a)"};
	static const char *const ints[] = {"c", "d", "(c + d)", "(c * 2)"};

	switch ((int) pg_prop_draw_int(tc, 0, 7))
	{
		case 0:				/* CF 7441: (bool expr) IN (sublink) */
			appendStringInfo(b, "(%s IN (SELECT %s))",
							 bools[pg_prop_draw_int(tc, 0, lengthof(bools) - 1)],
							 pg_prop_draw_bool(tc) ? "false" : "true");
			break;
		case 1:				/* CF 7441: (int expr) IN / = ANY sublink */
			appendStringInfo(b, "(%s %s (SELECT %d))",
							 ints[pg_prop_draw_int(tc, 0, lengthof(ints) - 1)],
							 pg_prop_draw_bool(tc) ? "IN" : "= ANY",
							 (int) pg_prop_draw_int(tc, 0, 5));
			break;
		case 2:				/* CF 7441: sublink as a comparison operand */
			appendStringInfo(b, "((SELECT %d) %s c)",
							 (int) pg_prop_draw_int(tc, 0, 5), pg_prop_draw_bool(tc) ? "<" : "=");
			break;
		case 3:				/* CF 7451: IS DOCUMENT as an operand */
			appendStringInfo(b, "(a %s (x IS %sDOCUMENT))",
							 pg_prop_draw_bool(tc) ? "=" : "<>", pg_prop_draw_bool(tc) ? "NOT " : "");
			break;
		case 4:				/* CF 7451: IS DOCUMENT under NOT / in a CASE */
			appendStringInfoString(b, pg_prop_draw_bool(tc) ? "(NOT (x IS DOCUMENT))"
								   : "(CASE WHEN x IS DOCUMENT THEN 1 ELSE 0 END)");
			break;
		case 5:				/* CF 7452: operator on ROW()::record with a NULL field */
			appendStringInfo(b, "(ROW(c, %s)::record %s ROW(c, %s)::record)",
							 pg_prop_draw_bool(tc) ? "NULL::int" : "d",
							 pg_prop_draw_bool(tc) ? "=" : "<>",
							 pg_prop_draw_bool(tc) ? "NULL::int" : "d");
			break;
		case 6:				/* CF 7452: plain row comparison (different semantics) */
			appendStringInfo(b, "(ROW(c, %s) = ROW(c, %s))",
							 pg_prop_draw_bool(tc) ? "NULL::int" : "d",
							 pg_prop_draw_bool(tc) ? "NULL::int" : "d");
			break;
		default:
			appendStringInfoString(b, ints[pg_prop_draw_int(tc, 0, lengthof(ints) - 1)]);
			break;
	}
}

/*
 * Compare two views' rows and require the deparse of the second to equal the
 * deparse of the first.
 */
static void
dp_roundtrip(pg_prop_tc *tc, pg_prop_conn *conn, const char *body, bool pretty)
{
	StringInfoData s;
	char	   *def;
	char	   *def2;
	char	   *r1;
	char	   *r2;
	bool		f = false;
	char	   *err = NULL;

	initStringInfo(&s);
	pg_prop_sql_exec(conn, "DROP VIEW IF EXISTS cfd_v2, cfd_v1", NULL);
	appendStringInfo(&s, "CREATE VIEW cfd_v1 AS %s", body);
	if (!pg_prop_sql_exec(conn, s.data, &err))
	{
		/* the drawn body itself is not valid SQL (e.g. a type mismatch): vacuous */
		pg_free(err);
		pfree(s.data);
		return;
	}
	resetStringInfo(&s);
	appendStringInfo(&s, "SELECT pg_get_viewdef('cfd_v1'::regclass, %s)", pretty ? "true" : "false");
	def = pg_prop_sql_scalar(conn, s.data, &f, &err);
	if (f || def == NULL)
		pg_prop_fail(tc, "pg_get_viewdef failed: %s\n  view: %s", err ? err : "", body);

	resetStringInfo(&s);
	appendStringInfo(&s, "CREATE VIEW cfd_v2 AS %s", def);
	if (!pg_prop_sql_exec(conn, s.data, &err))
		pg_prop_fail(tc, "the deparsed view (pretty=%s) does not read back: %s\n"
					 "  original: %s\n  deparsed: %s",
					 pretty ? "true" : "false", err ? err : "", body, def);

	r1 = pg_prop_sql_rows(conn, "SELECT * FROM cfd_v1 ORDER BY 1", &f, &err);
	if (f)
		pg_prop_fail(tc, "querying the original view failed: %s", err ? err : "");
	r2 = pg_prop_sql_rows(conn, "SELECT * FROM cfd_v2 ORDER BY 1", &f, &err);
	if (f)
		pg_prop_fail(tc, "querying the re-created view failed: %s", err ? err : "");
	if (strcmp(r1, r2) != 0)
		pg_prop_fail(tc, "the re-created view returns different rows (pretty=%s)\n"
					 "  original: %s\n  deparsed: %s\n  original rows:\n%s  re-created rows:\n%s",
					 pretty ? "true" : "false", body, def, r1, r2);

	resetStringInfo(&s);
	appendStringInfo(&s, "SELECT pg_get_viewdef('cfd_v2'::regclass, %s)", pretty ? "true" : "false");
	def2 = pg_prop_sql_scalar(conn, s.data, &f, &err);
	if (!f && def2 && strcmp(def, def2) != 0)
		pg_prop_fail(tc, "deparse is not a fixed point (pretty=%s)\n  first:  %s\n  second: %s",
					 pretty ? "true" : "false", def, def2);

	pg_free(def);
	if (def2)
		pg_free(def2);
	pg_free(r1);
	pg_free(r2);
	pfree(s.data);
}

/*
 * Expressions (CF 7441, 7451, 7452) over a drawn table: up to 4 columns of
 * drawn expressions, deparsed pretty and not.
 */
PG_PROP(view_expressions_survive_deparse)
{
	pg_prop_conn *conn = dp_connect(tc);
	int			nrows = (int) pg_prop_draw_int(tc, 0, 12);
	int			ncols = (int) pg_prop_draw_int(tc, 1, 4);
	StringInfoData s;
	StringInfoData body;

	initStringInfo(&s);
	appendStringInfoString(&s,
						   "DROP VIEW IF EXISTS cfd_v2, cfd_v1; DROP TABLE IF EXISTS cfd_dp; "
						   "CREATE TABLE cfd_dp (a bool, b bool, c int, d int, x xml); ");
	for (int i = 0; i < nrows; i++)
	{
		static const char *const bv[] = {"true", "false", "NULL"};
		static const char *const xv[] = {"'<r/>'", "'text'", "NULL", "'<a>1</a>'"};

		appendStringInfo(&s, "INSERT INTO cfd_dp VALUES (%s, %s, %s, %s, %s); ",
						 bv[pg_prop_draw_int(tc, 0, 2)], bv[pg_prop_draw_int(tc, 0, 2)],
						 pg_prop_draw_int(tc, 0, 4) == 0 ? "NULL" : psprintf("%d", (int) pg_prop_draw_int(tc, -2, 5)),
						 pg_prop_draw_int(tc, 0, 4) == 0 ? "NULL" : psprintf("%d", (int) pg_prop_draw_int(tc, -2, 5)),
						 xv[pg_prop_draw_int(tc, 0, 3)]);
	}
	dp_exec(tc, conn, s.data);

	initStringInfo(&body);
	appendStringInfoString(&body, "SELECT c AS k");
	for (int i = 0; i < ncols; i++)
	{
		appendStringInfoString(&body, ", ");
		dp_expr(tc, &body);
		appendStringInfo(&body, " AS e%d", i);
	}
	appendStringInfoString(&body, " FROM cfd_dp");

	dp_roundtrip(tc, conn, body.data, pg_prop_draw_bool(tc));

	pg_prop_sql_exec(conn, "DROP VIEW IF EXISTS cfd_v2, cfd_v1; DROP TABLE cfd_dp", NULL);
	pfree(s.data);
	pfree(body.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7056: windows named after unreserved keywords, referenced from another
 * window definition and from OVER.
 */
PG_PROP(keyword_window_names_survive_deparse)
{
	static const char *const names[] = {"rows", "range", "groups", "partition", "w", "order_"};
	static const char *const clauses[] = {
		"PARTITION BY v", "ORDER BY v", "ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW",
	};
	pg_prop_conn *conn = dp_connect(tc);
	const char *base = names[pg_prop_draw_int(tc, 0, lengthof(names) - 1)];
	StringInfoData body;

	initStringInfo(&body);
	appendStringInfo(&body,
					 "SELECT v, count(*) OVER w2 AS n FROM generate_series(1, %d) s(v) "
					 "WINDOW \"%s\" AS (%s), w2 AS (\"%s\"%s)",
					 (int) pg_prop_draw_int(tc, 1, 6), base,
					 clauses[pg_prop_draw_int(tc, 0, 0)], base,
					 pg_prop_draw_bool(tc) ? " ORDER BY v" : "");
	dp_roundtrip(tc, conn, body.data, pg_prop_draw_bool(tc));
	pg_prop_sql_exec(conn, "DROP VIEW IF EXISTS cfd_v2, cfd_v1", NULL);
	(void) clauses;
	pfree(body.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7442: a rule whose action UPDATEs or DELETEs with a target alias named
 * after a keyword ("set").  pg_get_ruledef's output must re-create the rule.
 */
PG_PROP(rule_target_alias_survives_deparse)
{
	static const char *const aliases[] = {"set", "\"set\"", "t", "using"};
	pg_prop_conn *conn = dp_connect(tc);
	const char *al = aliases[pg_prop_draw_int(tc, 0, lengthof(aliases) - 1)];
	bool		upd = pg_prop_draw_bool(tc);
	StringInfoData s;
	char	   *def;
	bool		f = false;
	char	   *err = NULL;

	initStringInfo(&s);
	dp_exec(tc, conn, "DROP TABLE IF EXISTS cfd_rt CASCADE; CREATE TABLE cfd_rt (a int, b int)");
	if (upd)
		appendStringInfo(&s, "CREATE RULE cfd_rt_r AS ON INSERT TO cfd_rt DO ALSO "
						 "UPDATE cfd_rt AS %s SET b = %s.b + 1 WHERE %s.a = new.a", al, al, al);
	else
		appendStringInfo(&s, "CREATE RULE cfd_rt_r AS ON INSERT TO cfd_rt DO ALSO "
						 "DELETE FROM cfd_rt AS %s WHERE %s.a = new.a + 100", al, al);
	if (!pg_prop_sql_exec(conn, s.data, &err))
	{
		/* alias not allowed by the grammar: vacuous */
		pg_free(err);
		goto done;
	}
	def = pg_prop_sql_scalar(conn, "SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE rulename = 'cfd_rt_r'",
							 &f, &err);
	if (f || def == NULL)
		pg_prop_fail(tc, "pg_get_ruledef failed: %s", err ? err : "");
	dp_exec(tc, conn, "DROP RULE cfd_rt_r ON cfd_rt");
	if (!pg_prop_sql_exec(conn, def, &err))
		pg_prop_fail(tc, "the deparsed rule does not read back: %s\n  original: %s\n  deparsed: %s",
					 err ? err : "", s.data, def);
	pg_free(def);
done:
	pg_prop_sql_exec(conn, "DROP TABLE IF EXISTS cfd_rt CASCADE", NULL);
	pfree(s.data);
	pg_prop_disconnect(conn);
}
