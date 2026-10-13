/*-------------------------------------------------------------------------
 *
 * prop_partprune.c
 *	  Property-based tests for partition pruning (src/backend/partitioning).
 *
 * Target #1 in the design, and the one most likely to turn up a wrong-results
 * bug: three such fixes landed in the three months before this was written
 * (709dfd27f14, 6e5d5680b55, b9d9e9bcc57), all involving multi-column RANGE
 * keys, MINVALUE/MAXVALUE bounds, the DEFAULT partition, and NULLs.
 *
 * The oracle is differential: for a randomly generated partitioned table and
 * a plain (unpartitioned) table holding exactly the same rows, any query must
 * return the same rows from both.  Pruning is a performance optimisation that
 * must never change results, so the partitioned answer is also compared with
 * enable_partition_pruning turned off.  Any disagreement is a pruning bug.
 *
 * Tier B: this runs SQL against the cluster the TAP driver started; see
 * pg_prop.h and t/002_sql_props.pl.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_partprune.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "partprune"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/*
 * Small integer domains keep the generated schema legible in a counterexample
 * and keep each case fast, while still covering the boundary interactions
 * (a value exactly on a bound, below the first bound, above the last, NULL)
 * where the recent bugs lived.
 */
#define VAL_MIN (-4)
#define VAL_MAX 4

/* Draw an integer in [VAL_MIN, VAL_MAX], or SQL NULL (~15% of the time). */
static void
draw_val(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
}

/*
 * Build a RANGE-partitioned table on (a, b) with a random set of
 * non-overlapping partitions plus (usually) a DEFAULT partition, and a plain
 * table with the same columns.  Populate both with the same random rows.
 * Returns false (after noting why) if the random bounds could not form a
 * legal partition set, so the caller can discard the case.
 */
static bool
build_tables(pg_prop_tc *tc, pg_prop_conn *conn)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			nparts;
	int			lo;
	bool		ok = true;

	initStringInfo(&ddl);

	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS pp_part; "
						   "DROP TABLE IF EXISTS pp_plain; "
						   "CREATE TABLE pp_part (a int, b int) PARTITION BY RANGE (a, b); "
						   "CREATE TABLE pp_plain (a int, b int); ");

	/*
	 * Build the partition set.  Two shapes, chosen at random:
	 *
	 * 1. Columns laid end to end along a (each partition a distinct a-range),
	 *    optionally with MINVALUE/MAXVALUE on the outer bounds.
	 *
	 * 2. Several partitions that SHARE an a-range but split on b -- e.g.
	 *    FROM (3, MINVALUE) TO (3, 0) and FROM (3, 0) TO (3, MAXVALUE).  This
	 *    multi-column-prefix shape is what a prefix-only qual (a = 3 AND b op k)
	 *    must prune correctly; it is the shape behind 6e5d5680b55 and
	 *    b9d9e9bcc57.  Mixing a run of these with plain a-ranges stresses the
	 *    boundary between the b-split block and its neighbours.
	 */
	nparts = (int) pg_prop_draw_int(tc, 1, 6);
	lo = VAL_MIN;
	for (int i = 0; i < nparts; i++)
	{
		bool		first = (i == 0);
		bool		last = (i == nparts - 1);
		bool		split_block = (pg_prop_draw_int(tc, 0, 2) == 0);

		if (split_block)
		{
			/*
			 * A block of 2-3 partitions sharing a-value `lo`, split on b at
			 * random cut points, spanning MINVALUE..MAXVALUE on b.  Advances a
			 * by 1 so the next partition starts cleanly above the block.
			 */
			int			sub = (int) pg_prop_draw_int(tc, 2, 3);
			int			cut = (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX);
			int			cut2 = cut + (int) pg_prop_draw_int(tc, 1, 3);

			appendStringInfo(&ddl,
							 "CREATE TABLE pp_p%d PARTITION OF pp_part "
							 "FOR VALUES FROM (%d, MINVALUE) TO (%d, %d); ",
							 i, lo, lo, cut);
			if (sub == 3)
			{
				appendStringInfo(&ddl,
								 "CREATE TABLE pp_p%d_b PARTITION OF pp_part "
								 "FOR VALUES FROM (%d, %d) TO (%d, %d); ",
								 i, lo, cut, lo, cut2);
				appendStringInfo(&ddl,
								 "CREATE TABLE pp_p%d_c PARTITION OF pp_part "
								 "FOR VALUES FROM (%d, %d) TO (%d, MAXVALUE); ",
								 i, lo, cut2, lo);
			}
			else
				appendStringInfo(&ddl,
								 "CREATE TABLE pp_p%d_b PARTITION OF pp_part "
								 "FOR VALUES FROM (%d, %d) TO (%d, MAXVALUE); ",
								 i, lo, cut, lo);
			lo = lo + 1;
		}
		else
		{
			int			width = (int) pg_prop_draw_int(tc, 1, 3);
			int			hi = lo + width;

			appendStringInfo(&ddl,
							 "CREATE TABLE pp_p%d PARTITION OF pp_part FOR VALUES FROM (", i);
			if (first && pg_prop_draw_bool(tc))
				appendStringInfoString(&ddl, "MINVALUE, MINVALUE");
			else
				appendStringInfo(&ddl, "%d, MINVALUE", lo);
			appendStringInfoString(&ddl, ") TO (");
			if (last && pg_prop_draw_bool(tc))
				appendStringInfoString(&ddl, "MAXVALUE, MAXVALUE");
			else
				appendStringInfo(&ddl, "%d, MINVALUE", hi);
			appendStringInfoString(&ddl, "); ");
			lo = hi;
		}
	}

	/* Usually add a DEFAULT partition -- its mispruning was the recent bug. */
	if (pg_prop_draw_int(tc, 0, 4) != 0)
		appendStringInfoString(&ddl,
							   "CREATE TABLE pp_def PARTITION OF pp_part DEFAULT; ");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
	{
		/*
		 * A random bound set can be rejected by the backend (e.g. an empty
		 * range, or a partition that collides with another once MINVALUE is
		 * involved).  That is not a pruning bug; discard the case.
		 */
		pg_prop_note(tc, "schema rejected: %s", err ? err : "(no message)");
		pg_prop_note(tc, "ddl: %s", ddl.data);
		if (err)
			pg_free(err);
		pfree(ddl.data);
		pg_prop_assume(tc, false);
		return false;			/* unreachable: assume(false) longjmps */
	}
	pfree(ddl.data);

	/* Insert the same random rows into both tables. */
	{
		StringInfoData ins;
		int			nrows = (int) pg_prop_draw_int(tc, 0, 24);

		initStringInfo(&ins);
		for (int r = 0; r < nrows; r++)
		{
			StringInfoData vals;

			initStringInfo(&vals);
			draw_val(tc, &vals);
			appendStringInfoString(&vals, ", ");
			draw_val(tc, &vals);

			appendStringInfo(&ins, "INSERT INTO pp_part VALUES (%s); ", vals.data);
			appendStringInfo(&ins, "INSERT INTO pp_plain VALUES (%s); ", vals.data);
			pfree(vals.data);
		}
		if (ins.len > 0 && !pg_prop_sql_exec(conn, ins.data, &err))
		{
			/*
			 * A row can fall outside every partition when there is no DEFAULT.
			 * That is a legal INSERT failure, not a bug; discard the case.
			 */
			pg_prop_note(tc, "insert rejected (likely no partition for row): %s",
						 err ? err : "");
			if (err)
				pg_free(err);
			ok = false;
		}
		pfree(ins.data);
	}

	if (!ok)
		pg_prop_assume(tc, false);
	return ok;
}

/* Draw a random boolean predicate over (a, b). */
static void
draw_predicate(pg_prop_tc *tc, StringInfo where)
{
	static const char *ops[] = {"=", "<>", "<", "<=", ">", ">="};

	/*
	 * One case in three, emit the shape the recent pruning bugs needed: a
	 * prefix-only equality on the first key column combined with an
	 * inequality or IS NOT NULL on the second (e.g. a = 3 AND b >= -7).  With
	 * the b-split partition blocks above, this forces pruning to reason about
	 * a partial key prefix across a MAXVALUE/MINVALUE boundary -- the trigger
	 * for 6e5d5680b55 / 709dfd27f14 / b9d9e9bcc57.
	 */
	if (pg_prop_draw_int(tc, 0, 2) == 0)
	{
		appendStringInfo(where, "a = %d AND ",
						 (int) pg_prop_draw_int(tc, VAL_MIN - 1, VAL_MAX + 1));
		if (pg_prop_draw_bool(tc))
			appendStringInfo(where, "b %s %d",
							 ops[pg_prop_draw_int(tc, 2, lengthof(ops) - 1)],
							 (int) pg_prop_draw_int(tc, VAL_MIN - 1, VAL_MAX + 1));
		else
			appendStringInfoString(where, "b IS NOT NULL");
		return;
	}

	{
		int			nterms = (int) pg_prop_draw_int(tc, 1, 3);

		for (int i = 0; i < nterms; i++)
		{
			int			kind = (int) pg_prop_draw_int(tc, 0, 4);
			const char *col = pg_prop_draw_bool(tc) ? "a" : "b";

			if (i > 0)
				appendStringInfoString(where, pg_prop_draw_bool(tc) ? " AND " : " OR ");

			switch (kind)
			{
				case 0:			/* col IS NULL */
					appendStringInfo(where, "%s IS NULL", col);
					break;
				case 1:			/* col IS NOT NULL */
					appendStringInfo(where, "%s IS NOT NULL", col);
					break;
				case 2:			/* col op const */
					appendStringInfo(where, "%s %s %d", col,
									 ops[pg_prop_draw_int(tc, 0, lengthof(ops) - 1)],
									 (int) pg_prop_draw_int(tc, VAL_MIN - 1, VAL_MAX + 1));
					break;
				case 3:			/* col IN (list) */
					appendStringInfo(where, "%s IN (%d, %d)", col,
									 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX),
									 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
					break;
				default:		/* col = const (equality prefix) */
					appendStringInfo(where, "%s = %d", col,
									 (int) pg_prop_draw_int(tc, VAL_MIN, VAL_MAX));
					break;
			}
		}
	}
}

PG_PROP(pruning_preserves_rows)
{
	pg_prop_conn *conn;
	char	   *err = NULL;
	StringInfoData where;
	StringInfoData q;
	char	   *part_on;
	char	   *part_off;
	char	   *plain;
	bool		f1,
				f2,
				f3;

	conn = pg_prop_connect(&err);
	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");

	if (!build_tables(tc, conn))
	{
		pg_prop_disconnect(conn);
		return;					/* unreachable; build_tables assume()d */
	}

	initStringInfo(&where);
	draw_predicate(tc, &where);

	/*
	 * ORDER BY a canonical key so the row multisets compare order-insensitively
	 * (NULLS treated consistently).  Select both columns so a mis-pruned row
	 * shows up fully in the counterexample.
	 */
	initStringInfo(&q);
	appendStringInfo(&q,
					 "SELECT a, b FROM pp_part WHERE %s ORDER BY a, b, (a IS NULL), (b IS NULL)",
					 where.data);

	/* partitioned, pruning on (default) */
	pg_prop_sql_exec(conn, "SET enable_partition_pruning = on", &err);
	part_on = pg_prop_sql_rows(conn, q.data, &f1, &err);

	/* partitioned, pruning off: the trusted answer */
	pg_prop_sql_exec(conn, "SET enable_partition_pruning = off", &err);
	part_off = pg_prop_sql_rows(conn, q.data, &f2, &err);

	/* plain table, same rows: an independent oracle */
	{
		StringInfoData pq;

		initStringInfo(&pq);
		appendStringInfo(&pq,
						 "SELECT a, b FROM pp_plain WHERE %s ORDER BY a, b, (a IS NULL), (b IS NULL)",
						 where.data);
		plain = pg_prop_sql_rows(conn, pq.data, &f3, &err);
		pfree(pq.data);
	}

	if (f1 || f2 || f3)
		pg_prop_fail(tc, "query error on WHERE %s: %s", where.data,
					 err ? err : "(unknown)");

	if (strcmp(part_on, part_off) != 0)
		pg_prop_fail(tc,
					 "pruning changed results for WHERE %s\n"
					 "  pruning on:  %s\n  pruning off: %s",
					 where.data, part_on, part_off);
	if (strcmp(part_on, plain) != 0)
		pg_prop_fail(tc,
					 "partitioned != plain for WHERE %s\n"
					 "  partitioned: %s\n  plain:       %s",
					 where.data, part_on, plain);

	pg_free(part_on);
	pg_free(part_off);
	pg_free(plain);
	pfree(where.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}
