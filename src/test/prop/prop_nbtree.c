/*-------------------------------------------------------------------------
 *
 * prop_nbtree.c
 *	  Property-based tests for nbtree index scans (src/backend/access/nbtree).
 *
 * Target #2 in the design.  The skip-scan work (92fe23d93aa and the ~50
 * follow-up fixes: "premature array advancement", "NULL tuple values always
 * advance skip arrays", "commute strategy for DESC keys", row-compare
 * robustness) makes nbtree scan-key handling a dense bug surface.
 *
 * The oracle is differential: the same query answered by a forced index scan
 * and by a forced sequential scan must return the same rows.  The index is
 * randomly shaped (column count, ASC/DESC, NULLS FIRST/LAST) and the query
 * mixes equality, inequality, IN, and IS NULL, which is what drives skip
 * arrays and array-key preprocessing.  Backward scans are exercised by a
 * random ORDER BY direction.
 *
 * Tier B: runs SQL against the cluster the TAP driver started; see pg_prop.h
 * and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_nbtree.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "nbtree"
#include "pg_prop.h"

#include "lib/stringinfo.h"

#define VAL_MIN (-5)
#define VAL_MAX 5
#define NCOLS 3					/* columns a, b, c */

static const char *const colnames[NCOLS] = {"a", "b", "c"};

/* Draw an integer in [VAL_MIN,VAL_MAX] or NULL (~1 in 7). */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/*
 * Create a table (a, b, c), a randomly shaped multi-column btree index over a
 * prefix of its columns, and random rows.  Returns false (via assume) if the
 * schema is rejected.
 */
static void
build(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			ncols;
	int			nrows;

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS nb_t; "
						   "CREATE TABLE nb_t (a int, b int, c int); ");

	/* index over the first ncols columns, each ASC/DESC and NULLS first/last */
	ncols = (int) pg_prop_draw_int(tc, 1, NCOLS);
	appendStringInfoString(&ddl, "CREATE INDEX nb_i ON nb_t (");
	for (int i = 0; i < ncols; i++)
	{
		if (i > 0)
			appendStringInfoString(&ddl, ", ");
		appendStringInfo(&ddl, "%s %s NULLS %s", colnames[i],
						 pg_prop_draw_bool(tc) ? "ASC" : "DESC",
						 pg_prop_draw_bool(tc) ? "FIRST" : "LAST");
	}
	appendStringInfoString(&ddl, "); ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
	{
		pg_prop_note(tc, "schema rejected: %s", err ? err : "");
		if (err)
			pg_free(err);
		pfree(ddl.data);
		pg_prop_assume(tc, false);
	}
	pfree(ddl.data);

	/* random rows */
	{
		StringInfoData ins;

		nrows = (int) pg_prop_draw_int(tc, 0, 60);
		initStringInfo(&ins);
		for (int r = 0; r < nrows; r++)
		{
			appendStringInfoString(&ins, "INSERT INTO nb_t VALUES (");
			for (int col = 0; col < NCOLS; col++)
			{
				if (col > 0)
					appendStringInfoString(&ins, ", ");
				draw_val(tc, &ins);
			}
			appendStringInfoString(&ins, "); ");
		}
		if (ins.len > 0 && !pg_prop_sql_exec(conn, ins.data, &err))
		{
			pg_prop_note(tc, "insert rejected: %s", err ? err : "");
			if (err)
				pg_free(err);
			pfree(ins.data);
			pg_prop_assume(tc, false);
		}
		pfree(ins.data);
	}

	/* keep the planner honest: tiny tables otherwise never use the index */
	pg_prop_sql_exec(conn, "ANALYZE nb_t", &err);
}

/* Draw a predicate that leans on index scan-key machinery. */
static void
draw_predicate(pg_prop_tc *tc, StringInfo where)
{
	static const char *ops[] = {"=", "<", "<=", ">", ">=", "<>"};
	int			nterms = (int) pg_prop_draw_int(tc, 1, 4);

	for (int i = 0; i < nterms; i++)
	{
		const char *col = colnames[pg_prop_draw_int(tc, 0, NCOLS - 1)];
		int			kind = (int) pg_prop_draw_int(tc, 0, 5);

		if (i > 0)
			appendStringInfoString(where, pg_prop_draw_bool(tc) ? " AND " : " OR ");

		switch (kind)
		{
			case 0:
				appendStringInfo(where, "%s IS NULL", col);
				break;
			case 1:
				appendStringInfo(where, "%s IS NOT NULL", col);
				break;
			case 2:				/* = ANY, drives array keys / skip scan */
				appendStringInfo(where, "%s = ANY (ARRAY[%d, %d, %d])", col,
								 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX),
								 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX),
								 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
				break;
			case 3:				/* row comparison, (a,b) < (x,y) */
				appendStringInfo(where, "(a, b) %s (%d, %d)",
								 pg_prop_draw_bool(tc) ? "<" : ">",
								 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX),
								 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
				break;
			default:
				appendStringInfo(where, "%s %s %d", col,
								 ops[pg_prop_draw_int(tc, 0, lengthof(ops) - 1)],
								 (int) pg_prop_draw_int(tc, VAL_MIN - 1, VAL_MAX + 1));
				break;
		}
	}
}

PG_PROP(indexscan_matches_seqscan)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData where;
	StringInfoData q;
	const char *dir;
	char	   *idx;
	char	   *seq;
	bool		f1,
				f2;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	build(tc, conn);

	initStringInfo(&where);
	draw_predicate(tc, &where);

	/* a random scan direction exercises backward index scans */
	dir = pg_prop_draw_bool(tc) ? "ASC" : "DESC";

	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT a, b, c FROM nb_t WHERE %s "
					 "ORDER BY a %s, b %s, c %s, (a IS NULL), (b IS NULL), (c IS NULL)",
					 where.data, dir, dir, dir);

	/* forced index scan */
	pg_prop_sql_exec(conn,
					 "SET enable_seqscan = off; SET enable_bitmapscan = off; "
					 "SET enable_indexscan = on; SET enable_indexonlyscan = on",
					 &err);
	idx = pg_prop_sql_rows(conn, q.data, &f1, &err);

	/* forced sequential scan: the trusted oracle */
	pg_prop_sql_exec(conn,
					 "SET enable_seqscan = on; SET enable_indexscan = off; "
					 "SET enable_indexonlyscan = off; SET enable_bitmapscan = off",
					 &err);
	seq = pg_prop_sql_rows(conn, q.data, &f2, &err);

	if (f1 || f2)
		pg_prop_fail(tc, "query error on WHERE %s: %s", where.data,
					 err ? err : "(unknown)");

	if (strcmp(idx, seq) != 0)
		pg_prop_fail(tc,
					 "index scan != seq scan for WHERE %s (ORDER BY %s)\n"
					 "  index: %s\n  seq:   %s",
					 where.data, dir, idx, seq);

	pg_free(idx);
	pg_free(seq);
	pfree(where.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
