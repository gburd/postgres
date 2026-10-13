/*-------------------------------------------------------------------------
 *
 * prop_query_pqs.c
 *	  Query-level property tests using Pivoted Query Synthesis (PQS).
 *
 * PQS (Rigger & Su, ESEC/FSE 2020) is the oracle that found the most
 * previously-unknown wrong-results bugs in mature databases.  Unlike TLP and
 * NoREC, which compare two queries, PQS checks a single query against a single
 * known-true fact, so it can catch a bug that corrupts *both* sides of a
 * metamorphic pair identically.
 *
 * The method:
 *	 1. Pick a random "pivot" row that actually exists in the table and read
 *		back its column values.
 *	 2. Synthesise a WHERE predicate that is, by construction, guaranteed to
 *		evaluate to TRUE for the pivot row -- every leaf is rectified against
 *		the pivot's own values (a < pivot.a+1, c = pivot.c, x IS NULL when the
 *		pivot's x is NULL, and so on) and the leaves are combined only with AND
 *		and with OR-of-true, which preserve truth for the pivot.
 *	 3. Run SELECT id ... WHERE <predicate>.  The pivot's id MUST appear in the
 *		result, because the predicate is true for it.  If it is missing, the
 *		query processor dropped a row it must return -- a wrong-results bug.
 *
 * The containment check ("pivot is in the result") is a fact about one row we
 * computed ourselves from the stored data, so there is no reference query to
 * get wrong.  The synthesis is where the care is: a single mis-rectified leaf
 * would make the predicate possibly-false for the pivot and produce a false
 * alarm, so the rectification below is deliberately conservative.
 *
 * Tier B: runs SQL against the cluster the TAP driver started.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_query_pqs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "query_pqs"
#include "pg_prop.h"

#include "lib/stringinfo.h"
#include "query_gen.h"

/* The pivot row's values; a NULL C pointer means the column IS NULL. */
typedef struct PivotRow
{
	long		id;
	char	   *a;				/* int as text, or NULL */
	char	   *b;				/* int as text, or NULL */
	char	   *c;				/* text value (unquoted), or NULL */
	char	   *f;				/* float as text, or NULL */
} PivotRow;

/* Read one column of the pivot row; returns a palloc'd string or NULL. */
static char *
read_col(pg_prop_tc *tc, pg_prop_conn *conn, long id, const char *col)
{
	bool		failed = false;
	char	   *err = NULL;
	char	   *val;
	StringInfoData sql;

	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT %s FROM " QG_TABLE " WHERE id = %ld",
					 col, id);
	val = pg_prop_sql_scalar(conn, sql.data, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "pivot read failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql.data);
	pfree(sql.data);
	if (err)
		pg_free(err);
	return val;					/* NULL means SQL NULL */
}

/*
 * Append a leaf predicate on an integer column that is TRUE for the pivot.
 *	 - pivot value is NULL  -> "col IS NULL"
 *	 - pivot value is V     -> one of: col = V, col <> V+k, col >= V,
 *							    col <= V, col IN (V, ...), col BETWEEN <=V AND >=V
 * All strictly rectified so the leaf cannot be false (or NULL) for the pivot.
 */
static void
append_int_leaf(pg_prop_tc *tc, StringInfo buf, const char *col, const char *val)
{
	long		v;
	int			kind;

	if (val == NULL)
	{
		appendStringInfo(buf, "%s IS NULL", col);
		return;
	}
	v = atol(val);
	kind = (int) pg_prop_draw_int(tc, 0, 5);
	switch (kind)
	{
		case 0:
			appendStringInfo(buf, "%s = %ld", col, v);
			break;
		case 1:
			/* col <> (something that is not v) */
			appendStringInfo(buf, "%s <> %ld", col, v + 1 + pg_prop_draw_int(tc, 0, 3));
			break;
		case 2:
			appendStringInfo(buf, "%s >= %ld", col, v - pg_prop_draw_int(tc, 0, 3));
			break;
		case 3:
			appendStringInfo(buf, "%s <= %ld", col, v + pg_prop_draw_int(tc, 0, 3));
			break;
		case 4:
			/* IN list that definitely contains v */
			appendStringInfo(buf, "%s IN (%ld, %ld, %ld)",
							 col, v - 1, v, v + 2);
			break;
		default:
			appendStringInfo(buf, "%s BETWEEN %ld AND %ld",
							 col, v - pg_prop_draw_int(tc, 0, 3),
							 v + pg_prop_draw_int(tc, 0, 3));
			break;
	}
}

/* Append a text leaf on column c that is TRUE for the pivot. */
static void
append_text_leaf(pg_prop_tc *tc, StringInfo buf, const char *val)
{
	if (val == NULL)
	{
		appendStringInfoString(buf, "c IS NULL");
		return;
	}
	if (pg_prop_draw_bool(tc))
		appendStringInfo(buf, "c = '%s'", val);
	else
		appendStringInfo(buf, "c >= '%s'", val);	/* c >= itself is true */
}

/*
 * Append one rectified leaf over a randomly chosen column.  Every branch is
 * guaranteed TRUE for the pivot.
 */
static void
append_true_leaf(pg_prop_tc *tc, StringInfo buf, const PivotRow *pv)
{
	int			col = (int) pg_prop_draw_int(tc, 0, 3);	/* a, b, c, f */

	switch (col)
	{
		case 0:
			append_int_leaf(tc, buf, "a", pv->a);
			break;
		case 1:
			append_int_leaf(tc, buf, "b", pv->b);
			break;
		case 2:
			append_text_leaf(tc, buf, pv->c);
			break;
		default:
			/* f: only an equality or IS NULL; avoid float range edge cases. */
			if (pv->f == NULL)
				appendStringInfoString(buf, "f IS NULL");
			else
				appendStringInfo(buf, "f = %s", pv->f);
			break;
	}
}

/*
 * Build a predicate TRUE for the pivot by combining rectified leaves with
 * truth-preserving connectives: AND of true leaves stays true; (true OR
 * anything) stays true, so a right-hand OR branch may be any leaf at all.
 */
static void
build_true_predicate(pg_prop_tc *tc, StringInfo buf, const PivotRow *pv,
					 int depth)
{
	int			kind;

	if (depth >= 3)
	{
		append_true_leaf(tc, buf, pv);
		return;
	}
	kind = (int) pg_prop_draw_int(tc, 0, 3);
	switch (kind)
	{
		case 0:					/* single true leaf */
			append_true_leaf(tc, buf, pv);
			break;
		case 1:					/* (true AND true) */
			appendStringInfoChar(buf, '(');
			build_true_predicate(tc, buf, pv, depth + 1);
			appendStringInfoString(buf, " AND ");
			build_true_predicate(tc, buf, pv, depth + 1);
			appendStringInfoChar(buf, ')');
			break;
		default:				/* (true OR anything-true) -- keep both true so
								 * NULL from a bad RHS can't swallow the row */
			appendStringInfoChar(buf, '(');
			build_true_predicate(tc, buf, pv, depth + 1);
			appendStringInfoString(buf, " OR ");
			append_true_leaf(tc, buf, pv);
			appendStringInfoChar(buf, ')');
			break;
	}
}

/*
 * PQS core: pick a pivot, synthesise a predicate true for it, require its id
 * in the result.
 */
PG_PROP(pivot_is_contained)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	long		pivot_id;
	PivotRow	pv = {0};
	StringInfoData pred;
	StringInfoData sql;
	bool		failed = false;
	char	   *hit;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);

	/* No rows -> nothing to pivot on; this case is vacuously satisfied. */
	if (nrows == 0)
	{
		pg_prop_disconnect(conn);
		return;
	}

	/* ids are dense 1..nrows, so draw one directly. */
	pivot_id = (long) pg_prop_draw_int(tc, 1, nrows);
	pv.id = pivot_id;
	pv.a = read_col(tc, conn, pivot_id, "a");
	pv.b = read_col(tc, conn, pivot_id, "b");
	pv.c = read_col(tc, conn, pivot_id, "c");
	pv.f = read_col(tc, conn, pivot_id, "f");

	initStringInfo(&pred);
	build_true_predicate(tc, &pred, &pv, 0);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT id FROM " QG_TABLE " WHERE (%s) AND id = %ld",
					 pred.data, pivot_id);

	hit = pg_prop_sql_scalar(conn, sql.data, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "PQS query failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql.data);

	if (hit == NULL)
		pg_prop_fail(tc,
					 "PQS: pivot row %ld dropped by a predicate true for it\n"
					 "  pivot: a=%s b=%s c=%s f=%s\n  predicate: %s",
					 pivot_id,
					 pv.a ? pv.a : "NULL", pv.b ? pv.b : "NULL",
					 pv.c ? pv.c : "NULL", pv.f ? pv.f : "NULL",
					 pred.data);

	pg_free(hit);
	if (pv.a) pg_free(pv.a);
	if (pv.b) pg_free(pv.b);
	if (pv.c) pg_free(pv.c);
	if (pv.f) pg_free(pv.f);
	if (err) pg_free(err);
	pfree(pred.data);
	pfree(sql.data);
	pg_prop_disconnect(conn);
}

/*
 * PQS over a self-join: the pivot joined to itself on id must survive a
 * predicate true for it applied on the left side.  Exercises the join path
 * with a single-row-guaranteed outcome.
 */
PG_PROP(pivot_survives_join)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	int			nrows;
	long		pivot_id;
	PivotRow	pv = {0};
	StringInfoData pred;
	StringInfoData sql;
	bool		failed = false;
	char	   *hit;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	qg_build_table(tc, conn, &nrows);
	if (nrows == 0)
	{
		pg_prop_disconnect(conn);
		return;
	}

	pivot_id = (long) pg_prop_draw_int(tc, 1, nrows);
	pv.id = pivot_id;
	pv.a = read_col(tc, conn, pivot_id, "a");
	pv.b = read_col(tc, conn, pivot_id, "b");
	pv.c = read_col(tc, conn, pivot_id, "c");
	pv.f = read_col(tc, conn, pivot_id, "f");

	initStringInfo(&pred);
	build_true_predicate(tc, &pred, &pv, 0);

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT t.id FROM "
					 "(SELECT * FROM " QG_TABLE " WHERE (%s)) t "
					 "JOIN " QG_TABLE " u ON u.id = t.id WHERE t.id = %ld",
					 pred.data, pivot_id);

	hit = pg_prop_sql_scalar(conn, sql.data, &failed, &err);
	if (failed)
		pg_prop_fail(tc, "PQS join query failed: %s\n  SQL: %s",
					 err ? err : "(unknown)", sql.data);

	if (hit == NULL)
		pg_prop_fail(tc,
					 "PQS/join: pivot row %ld dropped from self-join\n"
					 "  pivot: a=%s b=%s c=%s f=%s\n  predicate: %s",
					 pivot_id,
					 pv.a ? pv.a : "NULL", pv.b ? pv.b : "NULL",
					 pv.c ? pv.c : "NULL", pv.f ? pv.f : "NULL",
					 pred.data);

	pg_free(hit);
	if (pv.a) pg_free(pv.a);
	if (pv.b) pg_free(pv.b);
	if (pv.c) pg_free(pv.c);
	if (pv.f) pg_free(pv.f);
	if (err) pg_free(err);
	pfree(pred.data);
	pfree(sql.data);
	pg_prop_disconnect(conn);
}
