/*-------------------------------------------------------------------------
 *
 * prop_cf_index.c
 *	  An index scan must return exactly the rows a sequential scan returns.
 *
 * Every property here builds a small table over a drawn (type, access
 * method, opclass[, options]) combination, fills it with drawn values that
 * deliberately include the edge values index code forgets about (NaN and
 * +/-Infinity coordinates, empty strings and multiranges, NULLs, values with
 * gaps), creates the index before or after loading (build path vs insertion
 * path), then runs a drawn indexable predicate under three plans:
 *
 *	 seq     enable_indexscan/bitmapscan/indexonlyscan = off
 *	 index   enable_seqscan/bitmapscan = off   (plain or index-only scan)
 *	 bitmap  enable_seqscan/indexscan/indexonlyscan = off
 *
 * and requires the same rows (ORDER BY id, digested) and the same count from
 * all three.  That holds on a correct server by construction: an index is a
 * pure access path, so it may never change which rows satisfy a qual.  A
 * GUC the operator depends on (pg_trgm thresholds) is set identically in all
 * three plans.
 *
 * Open commitfest bugs this targets (each property names its CF):
 *	 7370  NaN box poisons GiST/BRIN union keys; GiST KNN with NaN
 *	 7192  GiST multirange <@ multirange drops leaf matches with gaps
 *	 7382  pg_trgm GIN/GiST lose rows at similarity_threshold = 0
 *	 7385  pg_trgm GiST union drops SIGNKEY from all-true keys
 *	 7413  ALTER COLUMN SET STORAGE on a column whose index key type differs
 *	 7107  cube distance functions return 0 instead of NaN
 *
 * Object prefix: cfb1_.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/sql/prop_cf_index.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <math.h>

#define PG_PROP_AREA "cf_index"
#include "pg_prop.h"

#include "lib/stringinfo.h"

/* ---- shared helpers ---------------------------------------------------- */

static void
cf_exec(pg_prop_tc *tc, pg_prop_conn *conn, const char *sql)
{
	char	   *err = NULL;

	if (!pg_prop_sql_exec(conn, sql, &err))
		pg_prop_fail(tc, "statement failed: %s\n  SQL: %.2000s",
					 err ? err : "", sql);
}

static pg_prop_conn *
cf_connect(pg_prop_tc *tc)
{
	char	   *err = NULL;
	pg_prop_conn *conn = pg_prop_connect(&err);

	if (conn == NULL)
		pg_prop_fail(tc, "connect failed: %s", err ? err : "");
	return conn;
}

#define N_PLANS 3
static const char *const plan_name[N_PLANS] = {"seq", "index", "bitmap"};
static const char *const plan_gucs[N_PLANS] = {
	"SET LOCAL enable_indexscan = off; SET LOCAL enable_bitmapscan = off; "
	"SET LOCAL enable_indexonlyscan = off",
	"SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off",
	"SET LOCAL enable_seqscan = off; SET LOCAL enable_indexscan = off; "
	"SET LOCAL enable_indexonlyscan = off",
};

/*
 * Run "query" in its own transaction under plan p plus the extra GUCs, and
 * return either its digest (rowhash) or, when "scalar", its single value.
 * An error fails the case: the seq plan of a legal query must not error, and
 * an index path must not raise an error the seq plan did not.
 */
static char *
cf_run(pg_prop_tc *tc, pg_prop_conn *conn, int p, const char *gucs,
	   const char *query, bool scalar)
{
	StringInfoData s;
	char	   *out;
	char	   *err = NULL;
	bool		failed = false;

	initStringInfo(&s);
	appendStringInfo(&s, "BEGIN; %s; %s", plan_gucs[p], gucs ? gucs : "");
	cf_exec(tc, conn, s.data);
	if (scalar)
		out = pg_prop_sql_scalar(conn, query, &failed, &err);
	else
		out = pg_prop_sql_rowhash(conn, query, &failed, &err);
	pg_prop_sql_exec(conn, "ROLLBACK", NULL);
	if (failed)
		pg_prop_fail(tc, "query failed under the %s plan: %s\n  GUCs: %s\n  query: %s",
					 plan_name[p], err ? err : "", gucs ? gucs : "", query);
	pfree(s.data);
	return out ? out : pg_strdup("(null)");
}

/* EXPLAIN of query under plan p, for the failure message */
static char *
cf_explain(pg_prop_conn *conn, int p, const char *gucs, const char *query)
{
	StringInfoData s;
	char	   *out;
	bool		failed = false;

	initStringInfo(&s);
	appendStringInfo(&s, "BEGIN; %s; %s", plan_gucs[p], gucs ? gucs : "");
	pg_prop_sql_exec(conn, s.data, NULL);
	resetStringInfo(&s);
	appendStringInfo(&s, "EXPLAIN (COSTS OFF) %s", query);
	out = pg_prop_sql_rows(conn, s.data, &failed, NULL);
	pg_prop_sql_exec(conn, "ROLLBACK", NULL);
	pfree(s.data);
	return out ? out : pg_strdup("(explain failed)");
}

/*
 * The core check.  "from_where" is "FROM <table> WHERE <qual>"; the table
 * has an int column id.  The row set (ids, ordered) and the count must agree
 * across the three plans.
 */
static void
cf_diff(pg_prop_tc *tc, pg_prop_conn *conn, const char *gucs,
		const char *from_where, const char *setup_note, const char *dump_sql)
{
	StringInfoData rows,
				cnt;
	char	   *ref_rows,
			   *ref_cnt;

	initStringInfo(&rows);
	initStringInfo(&cnt);
	appendStringInfo(&rows, "SELECT id %s ORDER BY id", from_where);
	appendStringInfo(&cnt, "SELECT count(*) %s", from_where);

	ref_rows = cf_run(tc, conn, 0, gucs, rows.data, false);
	ref_cnt = cf_run(tc, conn, 0, gucs, cnt.data, true);
	for (int p = 1; p < N_PLANS; p++)
	{
		char	   *r = cf_run(tc, conn, p, gucs, rows.data, false);
		char	   *c = cf_run(tc, conn, p, gucs, cnt.data, true);

		if (strcmp(r, ref_rows) != 0 || strcmp(c, ref_cnt) != 0)
		{
			char	   *n_ref = cf_run(tc, conn, 0, gucs, cnt.data, true);
			char	   *n_this = cf_run(tc, conn, p, gucs,
										psprintf("SELECT count(*) FROM (%s) s", rows.data),
										true);
			char	   *ex = cf_explain(conn, p, gucs, rows.data);
			char	   *dump = dump_sql ? cf_run(tc, conn, 0, NULL, dump_sql, true) : NULL;

			pg_prop_fail(tc,
						 "the %s plan disagrees with a sequential scan\n"
						 "  seq: %s rows (count(*) %s);  %s: %s rows (count(*) %s)\n"
						 "  GUCs: %s\n  query: %s\n  setup: %s\n  data: %s\n  %s plan:\n%s",
						 plan_name[p], n_ref, ref_cnt, plan_name[p], n_this, c,
						 gucs ? gucs : "", rows.data,
						 setup_note ? setup_note : "", dump ? dump : "",
						 plan_name[p], ex);
		}
		free(r);
		free(c);
	}
	free(ref_rows);
	free(ref_cnt);
	pfree(rows.data);
	pfree(cnt.data);
}

/* ---- geometric types: 7370 --------------------------------------------- */

/*
 * A coordinate: usually a small integer on a grid (so values collide and
 * boxes overlap), sometimes an IEEE edge value.  nan_pct is drawn per case so
 * some cases have no edge values at all and some are full of them.
 */
static void
geo_coord(pg_prop_tc *tc, StringInfo b, int grid, int edge_pct)
{
	if ((int) pg_prop_draw_int(tc, 1, 100) <= edge_pct)
	{
		static const char *const edge[] = {"NaN", "Infinity", "-Infinity", "NaN", "-0"};

		appendStringInfoString(b, edge[pg_prop_draw_int(tc, 0, lengthof(edge) - 1)]);
	}
	else
		appendStringInfo(b, "%d", (int) pg_prop_draw_int(tc, -2, grid));
}

static void
geo_point(pg_prop_tc *tc, StringInfo b, int grid, int edge_pct)
{
	appendStringInfoChar(b, '(');
	geo_coord(tc, b, grid, edge_pct);
	appendStringInfoChar(b, ',');
	geo_coord(tc, b, grid, edge_pct);
	appendStringInfoChar(b, ')');
}

/* append a quoted literal of the given geometric type */
static void
geo_value(pg_prop_tc *tc, StringInfo b, const char *type, int grid, int edge_pct)
{
	appendStringInfo(b, "%s '", type);
	if (strcmp(type, "point") == 0)
		geo_point(tc, b, grid, edge_pct);
	else if (strcmp(type, "box") == 0)
	{
		geo_point(tc, b, grid, edge_pct);
		appendStringInfoChar(b, ',');
		geo_point(tc, b, grid, edge_pct);
	}
	else if (strcmp(type, "polygon") == 0)
	{
		int			nv = (int) pg_prop_draw_int(tc, 1, 5);

		appendStringInfoChar(b, '(');
		for (int i = 0; i < nv; i++)
		{
			if (i > 0)
				appendStringInfoChar(b, ',');
			geo_point(tc, b, grid, edge_pct);
		}
		appendStringInfoChar(b, ')');
	}
	else						/* circle: radius must not be negative */
	{
		appendStringInfoChar(b, '<');
		geo_point(tc, b, grid, edge_pct);
		appendStringInfoChar(b, ',');
		if ((int) pg_prop_draw_int(tc, 1, 100) <= edge_pct)
			appendStringInfoString(b, pg_prop_draw_bool(tc) ? "NaN" : "Infinity");
		else
			appendStringInfo(b, "%d", (int) pg_prop_draw_int(tc, 0, grid));
		appendStringInfoChar(b, '>');
	}
	appendStringInfoChar(b, '\'');
}

typedef struct GeoAm
{
	const char *am;
	const char *opclass;
	const char *type;
	bool		nan_ok;			/* can this opclass index NaN at all? */
	bool		knn;			/* supports ORDER BY col <-> point */
} GeoAm;

static const GeoAm geo_ams[] = {
	{"gist", "box_ops", "box", true, true},
	{"gist", "point_ops", "point", true, true},
	{"gist", "poly_ops", "polygon", true, true},
	{"gist", "circle_ops", "circle", true, true},
	{"brin", "box_inclusion_ops", "box", true, false},
	{"spgist", "box_ops", "box", true, true},
	{"spgist", "kd_point_ops", "point", true, true},
	/* quad_point_ops raises "getQuadrant: impossible case" on a NaN point */
	{"spgist", "quad_point_ops", "point", false, true},
	{"spgist", "poly_ops", "polygon", true, true},
};

/* binary operators "col OP <rhs type>" per column type */
typedef struct GeoOp
{
	const char *ltype;
	const char *op;
	const char *rtype;
} GeoOp;

static const GeoOp geo_ops[] = {
	{"box", "&&", "box"}, {"box", "&<", "box"}, {"box", "&<|", "box"},
	{"box", "&>", "box"}, {"box", "<<", "box"}, {"box", "<<|", "box"},
	{"box", "<@", "box"}, {"box", ">>", "box"}, {"box", "@>", "box"},
	{"box", "|&>", "box"}, {"box", "|>>", "box"}, {"box", "~=", "box"},
	{"box", "@>", "point"},
	{"point", "<<", "point"}, {"point", "<<|", "point"}, {"point", ">>", "point"},
	{"point", "|>>", "point"}, {"point", "<^", "point"}, {"point", ">^", "point"},
	{"point", "~=", "point"}, {"point", "<@", "box"}, {"point", "<@", "circle"},
	{"point", "<@", "polygon"},
	{"polygon", "&&", "polygon"}, {"polygon", "&<", "polygon"},
	{"polygon", "&<|", "polygon"}, {"polygon", "&>", "polygon"},
	{"polygon", "<<", "polygon"}, {"polygon", "<<|", "polygon"},
	{"polygon", "<@", "polygon"}, {"polygon", ">>", "polygon"},
	{"polygon", "@>", "polygon"}, {"polygon", "|&>", "polygon"},
	{"polygon", "|>>", "polygon"}, {"polygon", "~=", "polygon"},
	{"circle", "&&", "circle"}, {"circle", "&<", "circle"},
	{"circle", "&<|", "circle"}, {"circle", "&>", "circle"},
	{"circle", "<<", "circle"}, {"circle", "<<|", "circle"},
	{"circle", "<@", "circle"}, {"circle", ">>", "circle"},
	{"circle", "@>", "circle"}, {"circle", "|&>", "circle"},
	{"circle", "|>>", "circle"}, {"circle", "~=", "circle"},
};

static const GeoAm *
geo_pick_am(pg_prop_tc *tc, const char *am)
{
	const GeoAm *cand[lengthof(geo_ams)];
	int			n = 0;

	for (int i = 0; i < (int) lengthof(geo_ams); i++)
		if (strcmp(geo_ams[i].am, am) == 0)
			cand[n++] = &geo_ams[i];
	return cand[pg_prop_draw_int(tc, 0, n - 1)];
}

/*
 * Build cfb1_geo(id, g) for the drawn opclass: some rows loaded before the
 * index is built, some inserted after (both the build and the insertion paths
 * compute union keys), with drawn edge values and NULLs.
 */
static void
geo_build(pg_prop_tc *tc, pg_prop_conn *conn, const GeoAm *ga, int *grid_out,
		  int *edge_out, StringInfo note)
{
	int			grid = (int) pg_prop_draw_int(tc, 1, 40);
	static const int edge_pcts[] = {0, 1, 3, 10, 30};
	int			edge = ga->nan_ok ? edge_pcts[pg_prop_draw_int(tc, 0, lengthof(edge_pcts) - 1)] : 0;
	int			nbefore = (int) pg_prop_draw_int(tc, 0, 1500);
	int			nafter = (int) pg_prop_draw_int(tc, 0, 600);
	int			null_pct = (int) pg_prop_draw_int(tc, 0, 2) * 5;
	int			id = 0;
	StringInfoData s;

	initStringInfo(&s);
	appendStringInfo(&s, "DROP TABLE IF EXISTS cfb1_geo; "
					 "CREATE TABLE cfb1_geo (id int, g %s)", ga->type);
	cf_exec(tc, conn, s.data);

	for (int phase = 0; phase < 2; phase++)
	{
		int			n = phase == 0 ? nbefore : nafter;

		if (phase == 1)
		{
			resetStringInfo(&s);
			appendStringInfo(&s, "CREATE INDEX cfb1_geo_g ON cfb1_geo USING %s (g %s)",
							 ga->am, ga->opclass);
			cf_exec(tc, conn, s.data);
		}
		/*
		 * Mostly bulk rows generated server-side (a drawn number of copies of
		 * a few drawn values, plus a grid sweep), with drawn literal rows mixed
		 * in so edge values land in different pages / subtrees.
		 */
		while (n > 0)
		{
			int			chunk = (int) Min(n, pg_prop_draw_int(tc, 1, 400));

			resetStringInfo(&s);
			if (pg_prop_draw_int(tc, 0, 3) == 0)
			{
				/* a grid sweep of chunk distinct small values */
				if (strcmp(ga->type, "point") == 0)
					appendStringInfo(&s, "INSERT INTO cfb1_geo SELECT %d + 1 + i, point(i %% %d, i / %d) "
									 "FROM generate_series(0, %d) i",
									 id, grid + 1, grid + 1, chunk - 1);
				else if (strcmp(ga->type, "box") == 0)
					appendStringInfo(&s, "INSERT INTO cfb1_geo SELECT %d + 1 + i, "
									 "box(point(i %% %d, i / %d), point(i %% %d + 1, i / %d + 1)) "
									 "FROM generate_series(0, %d) i",
									 id, grid + 1, grid + 1, grid + 1, grid + 1, chunk - 1);
				else if (strcmp(ga->type, "polygon") == 0)
					appendStringInfo(&s, "INSERT INTO cfb1_geo SELECT %d + 1 + i, "
									 "polygon(box(point(i %% %d, i / %d), point(i %% %d + 1, i / %d + 1))) "
									 "FROM generate_series(0, %d) i",
									 id, grid + 1, grid + 1, grid + 1, grid + 1, chunk - 1);
				else
					appendStringInfo(&s, "INSERT INTO cfb1_geo SELECT %d + 1 + i, "
									 "circle(point(i %% %d, i / %d), 0.5) "
									 "FROM generate_series(0, %d) i",
									 id, grid + 1, grid + 1, chunk - 1);
				id += chunk;
			}
			else
			{
				int			nlit = (int) Min(chunk, pg_prop_draw_int(tc, 1, 12));

				appendStringInfoString(&s, "INSERT INTO cfb1_geo VALUES ");
				for (int i = 0; i < nlit; i++)
				{
					appendStringInfo(&s, "%s(%d, ", i ? ", " : "", ++id);
					if ((int) pg_prop_draw_int(tc, 1, 100) <= null_pct)
						appendStringInfoString(&s, "NULL");
					else
						geo_value(tc, &s, ga->type, grid, edge);
					appendStringInfoChar(&s, ')');
				}
				/* and copies of the last literal row to fill the chunk */
				if (chunk > nlit)
					appendStringInfo(&s, "; INSERT INTO cfb1_geo SELECT %d + i, g FROM cfb1_geo "
									 ", generate_series(1, %d) i WHERE id = %d",
									 id, chunk - nlit, id);
				id += chunk - nlit;
			}
			cf_exec(tc, conn, s.data);
			n -= chunk;
		}
	}
	appendStringInfo(note, "%s %s on %s, %d rows before + %d after CREATE INDEX, grid %d, edge %d%%, null %d%%",
					 ga->am, ga->opclass, ga->type, nbefore, nafter, grid, edge, null_pct);
	*grid_out = grid;
	*edge_out = edge;
	pfree(s.data);
}

/* distinct values and their multiplicity, for failure messages */
#define GEO_DUMP \
	"SELECT string_agg(format('%s x%s', v, n), '; ' ORDER BY n DESC, v) FROM " \
	"(SELECT g::text AS v, count(*) AS n FROM cfb1_geo GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 15) s"

static void
geo_scan_check(pg_prop_tc *tc, const char *am)
{
	pg_prop_conn *conn = cf_connect(tc);
	const GeoAm *ga = geo_pick_am(tc, am);
	const GeoOp *cand[lengthof(geo_ops)];
	int			ncand = 0;
	int			grid,
				edge;
	int			nq;
	StringInfoData note,
				q;

	initStringInfo(&note);
	initStringInfo(&q);
	geo_build(tc, conn, ga, &grid, &edge, &note);

	for (int i = 0; i < (int) lengthof(geo_ops); i++)
		if (strcmp(geo_ops[i].ltype, ga->type) == 0)
			cand[ncand++] = &geo_ops[i];

	nq = (int) pg_prop_draw_int(tc, 1, 4);
	for (int k = 0; k < nq; k++)
	{
		const GeoOp *op = cand[pg_prop_draw_int(tc, 0, ncand - 1)];

		resetStringInfo(&q);
		appendStringInfo(&q, "FROM cfb1_geo WHERE g %s ", op->op);
		/* the probe may carry edge values too, even if the data does not */
		geo_value(tc, &q, op->rtype, grid + 2,
				  ga->nan_ok ? Max(edge, (int) pg_prop_draw_int(tc, 0, 1) * 10) : 0);
		cf_diff(tc, conn, NULL, q.data, note.data, GEO_DUMP);
	}
	cf_exec(tc, conn, "DROP TABLE cfb1_geo");
	pfree(note.data);
	pfree(q.data);
	pg_prop_disconnect(conn);
}

/*
 * CF 7370 (GiST): a box/point/polygon/circle with a NaN coordinate makes
 * box_ops union keys NaN, and no comparison is true against a NaN bound, so
 * a whole subtree is skipped; point ~= NaN and point <@ polygon/circle with
 * NaN points also disagreed with the heap operators.
 */
PG_PROP(gist_geo_scan_equals_seqscan)
{
	geo_scan_check(tc, "gist");
}

/* CF 7370 (BRIN): bound_box() puts NaN into the range summary. */
PG_PROP(brin_box_scan_equals_seqscan)
{
	geo_scan_check(tc, "brin");
}

/*
 * Not a CF entry: the same invariant for SP-GiST, which 7370 does not touch.
 * Kept as a neighbour so a failure here is reported separately.
 */
PG_PROP(spgist_geo_scan_equals_seqscan)
{
	geo_scan_check(tc, "spgist");
}

/*
 * CF 7370 (GiST KNN, patch 0004): ORDER BY col <-> point LIMIT k through the
 * index must return the k smallest distances a sort returns.  Compared as the
 * sorted multiset of distances rounded to 1e-6, so ties (any of the tied rows
 * may be returned) and last-bit differences between the index's distance and
 * the operator's cannot cause a false alarm.  NaN distances sort after all
 * numbers and NULLs last in both.  On master a NaN key fails an assertion or
 * raises "inconsistent point values".
 */
PG_PROP(knn_order_equals_sort)
{
	pg_prop_conn *conn = cf_connect(tc);
	const GeoAm *ga;
	int			grid,
				edge;
	int			lim;
	StringInfoData note,
				pt,
				q;
	char	   *ref,
			   *got;

	do
		ga = &geo_ams[pg_prop_draw_int(tc, 0, lengthof(geo_ams) - 1)];
	while (!ga->knn);

	initStringInfo(&note);
	initStringInfo(&pt);
	initStringInfo(&q);
	geo_build(tc, conn, ga, &grid, &edge, &note);

	geo_value(tc, &pt, "point", grid + 2,
			  ga->nan_ok ? Max(edge, (int) pg_prop_draw_int(tc, 0, 1) * 10) : 0);
	lim = (int) pg_prop_draw_int(tc, 1, 2200);
	appendStringInfo(&q,
					 "SELECT d FROM (SELECT round((g <-> %s)::numeric, 6) AS d FROM cfb1_geo "
					 "ORDER BY g <-> %s LIMIT %d) s ORDER BY d",
					 pt.data, pt.data, lim);

	ref = cf_run(tc, conn, 0, NULL, q.data, false);
	got = cf_run(tc, conn, 1, NULL, q.data, false);
	if (strcmp(ref, got) != 0)
	{
		char	   *ex = cf_explain(conn, 1, NULL, q.data);

		pg_prop_fail(tc, "KNN index scan returned different distances than a sort\n"
					 "  query: %s\n  setup: %s\n  index plan:\n%s",
					 q.data, note.data, ex);
	}
	free(ref);
	free(got);
	cf_exec(tc, conn, "DROP TABLE cfb1_geo");
	pg_prop_disconnect(conn);
}

/* ---- ranges and multiranges: 7192 -------------------------------------- */

/* append one int4 range literal body like [3,9) or "empty", bounds on a grid */
static void
rng_body(pg_prop_tc *tc, StringInfo b, int grid, int *cursor)
{
	int			lo,
				hi;

	if (pg_prop_draw_int(tc, 0, 15) == 0)
	{
		appendStringInfoString(b, "empty");
		return;
	}
	lo = *cursor + (int) pg_prop_draw_int(tc, 0, grid);
	hi = lo + (int) pg_prop_draw_int(tc, 1, grid + 1);
	*cursor = hi + 1;			/* the next range starts after a gap */
	appendStringInfo(b, "%c", pg_prop_draw_bool(tc) ? '[' : '(');
	if (pg_prop_draw_int(tc, 0, 12) == 0)
		;						/* -infinity */
	else
		appendStringInfo(b, "%d", lo);
	appendStringInfoChar(b, ',');
	if (pg_prop_draw_int(tc, 0, 12) == 0)
		;						/* +infinity */
	else
		appendStringInfo(b, "%d", hi);
	appendStringInfo(b, "%c", pg_prop_draw_bool(tc) ? ']' : ')');
}

/* a multirange with 0..4 ranges, usually with gaps between them */
static void
mr_value(pg_prop_tc *tc, StringInfo b, int grid)
{
	int			n = (int) pg_prop_draw_int(tc, 0, 4);
	int			cursor = (int) pg_prop_draw_int(tc, -grid, 4 * grid);

	appendStringInfoString(b, "'{");
	for (int i = 0; i < n; i++)
	{
		if (i > 0)
			appendStringInfoChar(b, ',');
		rng_body(tc, b, grid, &cursor);
	}
	appendStringInfoString(b, "}'::int4multirange");
}

static void
rng_value(pg_prop_tc *tc, StringInfo b, int grid)
{
	int			cursor = (int) pg_prop_draw_int(tc, -grid, 4 * grid);

	appendStringInfoChar(b, '\'');
	rng_body(tc, b, grid, &cursor);
	appendStringInfoString(b, "'::int4range");
}

/*
 * CF 7192: GiST multirange_ops stores a leaf multirange as its union range,
 * and the leaf test for "mr <@ query-multirange" compared that union range
 * against the query, so an indexed multirange with gaps that is contained by
 * a query with matching gaps was dropped before the heap recheck.  Drawn:
 * gist multirange_ops or gist/spgist range_ops (neighbours), the full
 * operator set against both multirange and range right-hand sides, and
 * probes that are sometimes an exact copy of a stored value (the trigger).
 */
PG_PROP(gist_multirange_scan_equals_seqscan)
{
	static const char *const mr_ops[] = {
		"&&", "&<", "&>", "-|-", "<<", ">>", "<@", "@>", "="
	};
	pg_prop_conn *conn = cf_connect(tc);
	/* 0 gist multirange_ops (twice as likely), 1 gist range_ops, 2 spgist range_ops */
	int			kind = (int) Max(0, pg_prop_draw_int(tc, -1, 2));
	const char *coltype = kind == 0 ? "int4multirange" : "int4range";
	int			grid = (int) pg_prop_draw_int(tc, 1, 30);
	int			nrows = (int) pg_prop_draw_int(tc, 1, 1500);
	int			nlit = (int) pg_prop_draw_int(tc, 1, 40);
	bool		index_first = pg_prop_draw_bool(tc);
	StringInfoData s,
				note;
	int			nq;

	initStringInfo(&s);
	initStringInfo(&note);
	appendStringInfo(&s, "DROP TABLE IF EXISTS cfb1_mr; CREATE TABLE cfb1_mr (id int, g %s)", coltype);
	cf_exec(tc, conn, s.data);
	if (index_first)
	{
		resetStringInfo(&s);
		appendStringInfo(&s, "CREATE INDEX cfb1_mr_g ON cfb1_mr USING %s (g)",
						 kind == 2 ? "spgist" : "gist");
		cf_exec(tc, conn, s.data);
	}
	/* drawn literal rows (with gaps, empties, infinities, NULLs) ... */
	resetStringInfo(&s);
	appendStringInfoString(&s, "INSERT INTO cfb1_mr VALUES ");
	for (int i = 1; i <= nlit; i++)
	{
		appendStringInfo(&s, "%s(%d, ", i > 1 ? ", " : "", i);
		if (pg_prop_draw_int(tc, 0, 20) == 0)
			appendStringInfoString(&s, "NULL");
		else if (kind == 0)
			mr_value(tc, &s, grid);
		else
			rng_value(tc, &s, grid);
		appendStringInfoChar(&s, ')');
	}
	cf_exec(tc, conn, s.data);
	/* ... and server-generated filler so the tree has internal pages */
	resetStringInfo(&s);
	if (kind == 0)
		appendStringInfo(&s,
						 "INSERT INTO cfb1_mr SELECT %d + i, int4multirange(int4range(i %% 97 * %d, i %% 97 * %d + %d), "
						 "int4range(i %% 97 * %d + %d, i %% 97 * %d + %d)) FROM generate_series(1, %d) i",
						 nlit, grid, grid, 1 + grid / 3, grid, 2 + grid / 2, grid, 3 + grid, nrows);
	else
		appendStringInfo(&s,
						 "INSERT INTO cfb1_mr SELECT %d + i, int4range(i %% 97 * %d, i %% 97 * %d + %d) "
						 "FROM generate_series(1, %d) i",
						 nlit, grid, grid, 1 + grid, nrows);
	cf_exec(tc, conn, s.data);
	if (!index_first)
	{
		resetStringInfo(&s);
		appendStringInfo(&s, "CREATE INDEX cfb1_mr_g ON cfb1_mr USING %s (g)",
						 kind == 2 ? "spgist" : "gist");
		cf_exec(tc, conn, s.data);
	}
	appendStringInfo(&note, "%s on %s, %d literal + %d filler rows, grid %d, index %s load",
					 kind == 2 ? "spgist range_ops" : kind == 1 ? "gist range_ops" : "gist multirange_ops",
					 coltype, nlit, nrows, grid, index_first ? "before" : "after");

	nq = (int) pg_prop_draw_int(tc, 1, 6);
	for (int k = 0; k < nq; k++)
	{
		/* index -1 = "<@" again: contained-by is the operator 7192 broke */
		int			opi = (int) pg_prop_draw_int(tc, -2, lengthof(mr_ops) - 1);
		const char *op = opi < 0 ? "<@" : mr_ops[opi];
		int			rhs = (int) pg_prop_draw_int(tc, 0, 3);

		resetStringInfo(&s);
		if (rhs == 0)
		{
			/* an exact copy of a stored value, cast to the column type */
			appendStringInfo(&s,
							 "FROM cfb1_mr WHERE g %s (SELECT g FROM cfb1_mr WHERE id = %d)",
							 op, (int) pg_prop_draw_int(tc, 1, nlit));
		}
		else if (rhs == 1 || kind != 0)
		{
			/* same type as the column (spgist range_ops has no mr operators) */
			appendStringInfo(&s, "FROM cfb1_mr WHERE g %s ", op);
			if (kind == 0)
				mr_value(tc, &s, grid);
			else
				rng_value(tc, &s, grid);
		}
		else
		{
			/* multirange column vs range right-hand side */
			if (strcmp(op, "=") == 0)
				op = "<@";
			appendStringInfo(&s, "FROM cfb1_mr WHERE g %s ", op);
			rng_value(tc, &s, grid);
		}
		cf_diff(tc, conn, NULL, s.data, note.data,
				"SELECT string_agg(format('%s x%s', v, n), '; ' ORDER BY n DESC, v) FROM "
				"(SELECT g::text AS v, count(*) AS n FROM cfb1_mr WHERE id <= 40 GROUP BY 1 "
				"ORDER BY 2 DESC, 1 LIMIT 15) s");
	}
	cf_exec(tc, conn, "DROP TABLE cfb1_mr");
	pfree(s.data);
	pfree(note.data);
	pg_prop_disconnect(conn);
}

/* ---- pg_trgm: 7382, 7385 ----------------------------------------------- */

/*
 * A short text from a small alphabet of word pieces, so trigrams are shared
 * between rows; sometimes empty, sometimes punctuation only (no trigrams),
 * sometimes a long all-digit run (sets most signature bits -> ALLISTRUE).
 */
static void
trgm_text(pg_prop_tc *tc, StringInfo b)
{
	static const char *const words[] = {
		"warsaw", "szczecin", "war", "saw", "ab", "abc", "x", "zz", "test", "Warsaw",
		"krakow", "lodz", "a", "q"
	};
	int			kind = (int) pg_prop_draw_int(tc, 0, 20);

	appendStringInfoChar(b, '\'');
	if (kind == 0)
		;						/* '' */
	else if (kind == 1)
		appendStringInfoString(b, " ,.- ");	/* no trigrams */
	else if (kind == 2)
	{
		/* a long digit string: many distinct trigrams */
		int			n = (int) pg_prop_draw_int(tc, 10, 120);

		for (int i = 1; i <= n; i++)
			appendStringInfo(b, "%s%d", i > 1 ? " " : "", i);
	}
	else
	{
		int			nw = (int) pg_prop_draw_int(tc, 1, 4);

		for (int i = 0; i < nw; i++)
			appendStringInfo(b, "%s%s", i ? " " : "",
							 words[pg_prop_draw_int(tc, 0, lengthof(words) - 1)]);
	}
	appendStringInfoChar(b, '\'');
}

/* drawn threshold: the legal minimum 0 and maximum 1 are heavily weighted */
static double
trgm_threshold(pg_prop_tc *tc)
{
	static const double fixed[] = {0.0, 0.0, 1.0, 0.3, 0.6};
	int			k = (int) pg_prop_draw_int(tc, 0, lengthof(fixed));

	if (k < (int) lengthof(fixed))
		return fixed[k];
	return (double) pg_prop_draw_int(tc, 0, 100) / 100.0;
}

/*
 * Build cfb1_trgm(id, t) with a drawn pg_trgm index.  Returns the index
 * description in note.  Rows: drawn literals (empty, trigram-less, NULL,
 * long, shared words), plus server-side filler so the index has internal
 * pages, loaded before and/or after CREATE INDEX (GIN with fastupdate keeps
 * post-build rows in the pending list).
 */
static void
trgm_build(pg_prop_tc *tc, pg_prop_conn *conn, StringInfo note)
{
	static const int siglens[] = {0, 1, 4, 12, 64, 2024};
	bool		gin = pg_prop_draw_bool(tc);
	int			siglen = gin ? 0 : siglens[pg_prop_draw_int(tc, 0, lengthof(siglens) - 1)];
	bool		fastupdate = pg_prop_draw_bool(tc);
	int			nfill = (int) pg_prop_draw_int(tc, 0, 1500);
	int			nlit = (int) pg_prop_draw_int(tc, 1, 30);
	int			split = (int) pg_prop_draw_int(tc, 0, nlit);	/* literals before the index */
	bool		fill_after = pg_prop_draw_bool(tc);
	StringInfoData s,
				idx;

	initStringInfo(&s);
	initStringInfo(&idx);
	if (gin)
		appendStringInfo(&idx, "CREATE INDEX cfb1_trgm_i ON cfb1_trgm USING gin (t gin_trgm_ops) "
						 "WITH (fastupdate = %s)", fastupdate ? "on" : "off");
	else if (siglen == 0)
		appendStringInfoString(&idx, "CREATE INDEX cfb1_trgm_i ON cfb1_trgm USING gist (t gist_trgm_ops)");
	else
		appendStringInfo(&idx, "CREATE INDEX cfb1_trgm_i ON cfb1_trgm USING gist (t gist_trgm_ops(siglen = %d))",
						 siglen);

	cf_exec(tc, conn, "DROP TABLE IF EXISTS cfb1_trgm; CREATE TABLE cfb1_trgm (id int, t text)");
	for (int phase = 0; phase < 2; phase++)
	{
		int			from = phase == 0 ? 1 : split + 1;
		int			to = phase == 0 ? split : nlit;

		if (phase == 1)
			cf_exec(tc, conn, idx.data);
		if (to >= from)
		{
			resetStringInfo(&s);
			appendStringInfoString(&s, "INSERT INTO cfb1_trgm VALUES ");
			for (int i = from; i <= to; i++)
			{
				appendStringInfo(&s, "%s(%d, ", i > from ? ", " : "", i);
				if (pg_prop_draw_int(tc, 0, 10) == 0)
					appendStringInfoString(&s, "NULL");
				else
					trgm_text(tc, &s);
				appendStringInfoChar(&s, ')');
			}
			cf_exec(tc, conn, s.data);
		}
		if (nfill > 0 && (phase == 1) == fill_after)
		{
			resetStringInfo(&s);
			appendStringInfo(&s,
							 "INSERT INTO cfb1_trgm SELECT 1000 + i, CASE WHEN i %% 23 = 0 THEN NULL "
							 "WHEN i %% 17 = 0 THEN '' ELSE 'w' || (i %% 97) || ' k' || (i %% 13) END "
							 "FROM generate_series(1, %d) i", nfill);
			cf_exec(tc, conn, s.data);
		}
	}
	appendStringInfo(note, "%s; %d literal rows (%d before the index), %d filler rows %s",
					 idx.data, nlit, split, nfill, fill_after ? "after" : "before");
	pfree(s.data);
	pfree(idx.data);
}

#define TRGM_DUMP \
	"SELECT string_agg(format('%s x%s', coalesce(quote_literal(v), 'NULL'), n), '; ' ORDER BY n DESC, v) FROM " \
	"(SELECT t AS v, count(*) AS n FROM cfb1_trgm GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 15) s"

/*
 * CF 7382: pg_trgm similarity operators (%, <%, %>, <<%, %>>) are true when
 * similarity >= threshold, and 0 is a legal threshold under which every
 * non-NULL row matches.  GIN only visited rows sharing a trigram with the
 * query, and both GIN and GiST internal pages rejected everything for a
 * query with no trigrams.  Drawn: GIN (fastupdate on/off) or GiST (siglen),
 * all three thresholds, operator, query text (incl. '' and trigram-less).
 * Also LIKE / ILIKE / regex / = for neighbouring coverage of the opclass.
 */
PG_PROP(trgm_scan_equals_seqscan)
{
	static const char *const sim_ops[] = {"%", "<%", "%>", "<<%", "%>>"};
	pg_prop_conn *conn;
	StringInfoData note,
				q,
				gucs;
	int			nq;

	conn = cf_connect(tc);
	if (!pg_prop_require_extension(conn, "pg_trgm"))
		pg_prop_skip(tc, "pg_trgm not installed");

	initStringInfo(&note);
	initStringInfo(&q);
	initStringInfo(&gucs);
	trgm_build(tc, conn, &note);

	nq = (int) pg_prop_draw_int(tc, 1, 5);
	for (int k = 0; k < nq; k++)
	{
		int			shape = (int) pg_prop_draw_int(tc, 0, 7);

		resetStringInfo(&gucs);
		appendStringInfo(&gucs,
						 "SET LOCAL pg_trgm.similarity_threshold = %g; "
						 "SET LOCAL pg_trgm.word_similarity_threshold = %g; "
						 "SET LOCAL pg_trgm.strict_word_similarity_threshold = %g",
						 trgm_threshold(tc), trgm_threshold(tc), trgm_threshold(tc));
		resetStringInfo(&q);
		appendStringInfoString(&q, "FROM cfb1_trgm WHERE ");
		if (shape <= 4)
		{
			/* t OP q  or  q OP t  (the commutator pairs are both indexable) */
			const char *op = sim_ops[pg_prop_draw_int(tc, 0, lengthof(sim_ops) - 1)];

			if (strcmp(op, "%") == 0 || strcmp(op, "%>") == 0 || strcmp(op, "%>>") == 0)
			{
				appendStringInfo(&q, "t %s ", op);
				trgm_text(tc, &q);
			}
			else
			{
				trgm_text(tc, &q);
				appendStringInfo(&q, " %s t", op);
			}
		}
		else if (shape == 5)
		{
			appendStringInfoString(&q, "t = ");
			trgm_text(tc, &q);
		}
		else
		{
			static const char *const pat_ops[] = {"LIKE", "ILIKE", "~", "~*"};
			const char *op = pat_ops[pg_prop_draw_int(tc, 0, lengthof(pat_ops) - 1)];
			bool		like = op[0] == 'L' || op[0] == 'I';

			appendStringInfo(&q, "t %s '%s", op, like ? "%" : "");
			/* a fragment of a word; the quotes of trgm_text are stripped */
			{
				StringInfoData w;

				initStringInfo(&w);
				trgm_text(tc, &w);
				w.data[w.len - 1] = '\0';
				appendStringInfo(&q, "%.6s%s'", w.data + 1, like ? "%" : "");
				pfree(w.data);
			}
		}
		cf_diff(tc, conn, gucs.data, q.data, note.data, TRGM_DUMP);
	}
	cf_exec(tc, conn, "DROP TABLE cfb1_trgm");
	pfree(note.data);
	pfree(q.data);
	pfree(gucs.data);
	pg_prop_disconnect(conn);
}
