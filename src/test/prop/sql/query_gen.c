/*-------------------------------------------------------------------------
 *
 * query_gen.c
 *	  Shared SQL query generator for the query-level property areas.
 *
 * See query_gen.h for the contract.  The generator produces well-typed,
 * parseable SQL text only; it never decides what is correct -- that is each
 * area's oracle.  Keeping generation here means the TLP, NoREC and GUC-flip
 * areas all draw from the same distribution of tables and predicates, so a
 * counterexample found by one is reproducible shape-for-shape in the others.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/query_gen.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "query_gen.h"

/* Small domains keep counterexamples legible and cases fast. */
#define QG_VAL_MIN (-5)
#define QG_VAL_MAX 5

/* Draw an int in [QG_VAL_MIN, QG_VAL_MAX] as SQL text, ~15% NULL. */
static void
draw_int_or_null(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d",
						 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX));
}

/* Draw a short [a-c] string literal or SQL NULL, ~15% NULL. */
static void
draw_text_or_null(pg_prop_tc *tc, StringInfo buf)
{
	int			len;
	int			i;

	if (pg_prop_draw_int(tc, 0, 6) == 0)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}
	len = (int) pg_prop_draw_int(tc, 0, 3);
	appendStringInfoChar(buf, '\'');
	for (i = 0; i < len; i++)
		appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
	appendStringInfoChar(buf, '\'');
}

/* Draw a float8 literal (small, one decimal) or SQL NULL, ~15% NULL. */
static void
draw_float_or_null(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d.%d",
						 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX),
						 (int) pg_prop_draw_int(tc, 0, 9));
}

void
qg_build_table(pg_prop_tc *tc, pg_prop_conn *conn, int *nrows)
{
	char	   *err = NULL;
	StringInfoData ddl;
	int			n;
	int			r;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS " QG_TABLE "; "
						   "CREATE TABLE " QG_TABLE
						   " (id int PRIMARY KEY, a int, b int, c text, f float8); ");

	n = (int) pg_prop_draw_int(tc, 0, QG_MAX_ROWS);
	for (r = 1; r <= n; r++)
	{
		appendStringInfo(&ddl, "INSERT INTO " QG_TABLE " VALUES (%d, ", r);
		draw_int_or_null(tc, &ddl);
		appendStringInfoString(&ddl, ", ");
		draw_int_or_null(tc, &ddl);
		appendStringInfoString(&ddl, ", ");
		draw_text_or_null(tc, &ddl);
		appendStringInfoString(&ddl, ", ");
		draw_float_or_null(tc, &ddl);
		appendStringInfoString(&ddl, "); ");
	}

	/* ANALYZE so the planner has stats and will actually consider plans. */
	appendStringInfoString(&ddl, "ANALYZE " QG_TABLE ";");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "generator DDL/DML failed: %s", err ? err : "");

	pfree(ddl.data);
	*nrows = n;
}

/* One leaf comparison against a column: "a <op> <const>" or an IS NULL test. */
static void
draw_leaf(pg_prop_tc *tc, StringInfo buf)
{
	static const char *const cmp[] = {"=", "<>", "<", "<=", ">", ">="};
	int			kind = (int) pg_prop_draw_int(tc, 0, 6);
	int			col = (int) pg_prop_draw_int(tc, 0, 2);	/* a, b, or f */
	const char *colname = (col == 0) ? "a" : (col == 1) ? "b" : "f";

	switch (kind)
	{
		case 0:					/* col IS NULL */
			appendStringInfo(buf, "%s IS NULL", colname);
			break;
		case 1:					/* col IS NOT NULL */
			appendStringInfo(buf, "%s IS NOT NULL", colname);
			break;
		case 2:					/* c = 'text' / c comparison */
			appendStringInfoString(buf, "c ");
			appendStringInfoString(buf, cmp[pg_prop_draw_int(tc, 0, 5)]);
			appendStringInfoChar(buf, ' ');
			draw_text_or_null(tc, buf);
			break;
		case 3:					/* col IN (list) */
			appendStringInfo(buf, "%s IN (", colname);
			{
				int			m = (int) pg_prop_draw_int(tc, 1, 3);
				int			i;

				for (i = 0; i < m; i++)
				{
					if (i > 0)
						appendStringInfoString(buf, ", ");
					if (col == 2)
						draw_float_or_null(tc, buf);
					else
						draw_int_or_null(tc, buf);
				}
			}
			appendStringInfoChar(buf, ')');
			break;
		case 4:					/* col BETWEEN lo AND hi */
			appendStringInfo(buf, "%s BETWEEN %d AND %d", colname,
							 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX),
							 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX));
			break;
		default:				/* col <cmp> const */
			appendStringInfo(buf, "%s %s ", colname, cmp[pg_prop_draw_int(tc, 0, 5)]);
			if (col == 2)
				draw_float_or_null(tc, buf);
			else
				draw_int_or_null(tc, buf);
			break;
	}
}

static void
draw_predicate_rec(pg_prop_tc *tc, StringInfo buf, int depth)
{
	int			kind;

	if (depth >= 3)
		kind = 0;
	else
		kind = (int) pg_prop_draw_int(tc, 0, 4);

	switch (kind)
	{
		case 0:					/* leaf */
			draw_leaf(tc, buf);
			break;
		case 1:					/* NOT (sub) */
			appendStringInfoString(buf, "NOT (");
			draw_predicate_rec(tc, buf, depth + 1);
			appendStringInfoChar(buf, ')');
			break;
		case 2:					/* (l AND r) */
			appendStringInfoChar(buf, '(');
			draw_predicate_rec(tc, buf, depth + 1);
			appendStringInfoString(buf, " AND ");
			draw_predicate_rec(tc, buf, depth + 1);
			appendStringInfoChar(buf, ')');
			break;
		case 3:					/* (l OR r) */
			appendStringInfoChar(buf, '(');
			draw_predicate_rec(tc, buf, depth + 1);
			appendStringInfoString(buf, " OR ");
			draw_predicate_rec(tc, buf, depth + 1);
			appendStringInfoChar(buf, ')');
			break;
		default:				/* leaf */
			draw_leaf(tc, buf);
			break;
	}
}

void
qg_draw_predicate(pg_prop_tc *tc, StringInfo buf)
{
	draw_predicate_rec(tc, buf, 0);
}

void
qg_draw_int_expr(pg_prop_tc *tc, StringInfo buf)
{
	int			kind = (int) pg_prop_draw_int(tc, 0, 4);

	switch (kind)
	{
		case 0:
			appendStringInfoString(buf, "a");
			break;
		case 1:
			appendStringInfoString(buf, "b");
			break;
		case 2:
			appendStringInfo(buf, "%d",
							 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX));
			break;
		case 3:
			/* coalesce so NULLs don't dominate; a + b */
			appendStringInfoString(buf, "(coalesce(a,0) + coalesce(b,0))");
			break;
		default:
			appendStringInfoString(buf, "(coalesce(a,0) * 2)");
			break;
	}
}
