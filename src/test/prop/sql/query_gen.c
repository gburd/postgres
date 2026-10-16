/*-------------------------------------------------------------------------
 *
 * query_gen.c
 *	  Shared SQL query generator for the query-level property areas.
 *
 * See query_gen.h for the contract.  The generator produces well-typed,
 * parseable SQL text only; it never decides what is correct -- that is each
 * area's oracle.  Keeping generation here means the TLP, NoREC, cardinality,
 * PQS and plan-advice areas all draw from the same distribution of tables and
 * predicates, so a counterexample found by one is reproducible shape-for-shape
 * in the others.
 *
 * The table shape is a schema assembled once per process: core registers a
 * fixed set of typed columns, and the "query_gen.column" / "query_gen.predicate"
 * extension points let a contrib type add its own columns and predicate leaves
 * (see pg_prop.h).  Everything downstream -- row generation, predicate drawing,
 * the oracles -- is schema-driven, so adding a type needs no oracle changes.
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

/*
 * Value domains deliberately favour the cases that break planners and
 * executors, not uniform small noise: boundary integers (INT_MIN/MAX, 0, +-1),
 * heavy NULL, and skew (one value dominating) so selectivity estimates diverge
 * from reality and plan choice flips.  Legible counterexamples still shrink
 * because the engine prefers the small/boundary draws.
 */
#define QG_VAL_MIN (-5)
#define QG_VAL_MAX 5

/* INT_MIN / INT_MAX as SQL text (INT_MIN written to avoid the unary-minus
 * literal-overflow trap). */
#define SQL_INT_MIN "(-2147483647 - 1)"
#define SQL_INT_MAX "2147483647"

/* ---- core column value generators ------------------------------------ */

/*
 * Draw an int as SQL text with an adversarial, skewed distribution:
 *   ~18% NULL,
 *   ~22% a boundary value (INT_MIN, INT_MAX, 0, -1, 1),
 *   ~35% the skew value 0 (so one value dominates -> bad selectivity est),
 *   else a small value in [-5,5].
 */
static void
draw_int_or_null(pg_prop_tc *tc, StringInfo buf)
{
	int			roll = (int) pg_prop_draw_int(tc, 0, 99);

	if (roll < 18)
		appendStringInfoString(buf, "NULL");
	else if (roll < 40)
	{
		static const char *const edge[] =
		{SQL_INT_MIN, SQL_INT_MAX, "0", "(-1)", "1"};

		appendStringInfoString(buf, edge[pg_prop_draw_int(tc, 0, 4)]);
	}
	else if (roll < 75)
		appendStringInfoString(buf, "0");	/* skew: 0 dominates */
	else
		appendStringInfo(buf, "%d",
						 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX));
}

/*
 * Draw a text literal, adversarial: ~18% NULL, ~15% empty string, often a
 * repeated short run (skew + collation edge cases), occasionally a value with
 * a trailing space or an upper-case letter (case/space sensitivity).
 */
static void
draw_text_or_null(pg_prop_tc *tc, StringInfo buf)
{
	int			roll = (int) pg_prop_draw_int(tc, 0, 99);
	int			len;
	int			i;

	if (roll < 18)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}
	if (roll < 33)
	{
		appendStringInfoString(buf, "''");	/* empty string (distinct from NULL) */
		return;
	}
	if (roll < 55)
	{
		appendStringInfoString(buf, "'a'");	/* skew: 'a' dominates */
		return;
	}
	len = (int) pg_prop_draw_int(tc, 0, 3);
	appendStringInfoChar(buf, '\'');
	for (i = 0; i < len; i++)
	{
		/* mostly a-c; occasionally an uppercase letter or a trailing space */
		int			k = (int) pg_prop_draw_int(tc, 0, 9);

		if (k == 0)
			appendStringInfoChar(buf, (char) ('A' + pg_prop_draw_int(tc, 0, 2)));
		else if (k == 1)
			appendStringInfoChar(buf, ' ');
		else
			appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
	}
	appendStringInfoChar(buf, '\'');
}

/* float8: adversarial -- NULL, +-0.0, +-Infinity, NaN, or a small value. */
static void
draw_float_or_null(pg_prop_tc *tc, StringInfo buf)
{
	int			roll = (int) pg_prop_draw_int(tc, 0, 99);

	if (roll < 15)
		appendStringInfoString(buf, "NULL");
	else if (roll < 35)
	{
		static const char *const edge[] =
		{"'0'::float8", "'-0'::float8", "'Infinity'::float8",
			"'-Infinity'::float8", "'NaN'::float8"};

		appendStringInfoString(buf, edge[pg_prop_draw_int(tc, 0, 4)]);
	}
	else
		appendStringInfo(buf, "%d.%d",
						 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX),
						 (int) pg_prop_draw_int(tc, 0, 9));
}

/* numeric: NULL, boundary (0, huge, tiny-scale), or a scaled value. */
static void
draw_numeric_or_null(pg_prop_tc *tc, StringInfo buf)
{
	int			roll = (int) pg_prop_draw_int(tc, 0, 99);

	if (roll < 15)
		appendStringInfoString(buf, "NULL");
	else if (roll < 30)
	{
		static const char *const edge[] =
		{"0", "0.00", "999.99", "-999.99", "'NaN'::numeric"};

		appendStringInfoString(buf, edge[pg_prop_draw_int(tc, 0, 4)]);
	}
	else
		appendStringInfo(buf, "%d.%02d",
						 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX),
						 (int) pg_prop_draw_int(tc, 0, 99));
}

/* boolean literal or SQL NULL (balanced three-valued). */
static void
draw_bool_or_null(pg_prop_tc *tc, StringInfo buf)
{
	int			k = (int) pg_prop_draw_int(tc, 0, 2);

	appendStringInfoString(buf, k == 0 ? "NULL" : k == 1 ? "true" : "false");
}

/* ---- schema assembly ------------------------------------------------- */

void
qg_add_column(qg_schema *s, const char *name, const char *sqltype,
			  qg_value_fn draw, bool is_text_like, bool is_numeric_like)
{
	if (s->ncols >= QG_MAX_COLS)
		return;					/* stay bounded; silently drop extras */
	s->cols[s->ncols].name = name;
	s->cols[s->ncols].sqltype = sqltype;
	s->cols[s->ncols].draw = draw;
	s->cols[s->ncols].is_text_like = is_text_like;
	s->cols[s->ncols].is_numeric_like = is_numeric_like;
	s->ncols++;
}

/*
 * The process-wide schema, built lazily on first use.  id (the primary key)
 * is implicit and not in cols[].  Core columns a, b, c, f are kept with their
 * historical names and types so PQS and the hand-written join predicates stay
 * valid; d (numeric) and g (bool) widen the type coverage.
 */
static qg_schema the_schema;
static bool schema_built = false;

static const qg_schema *
build_schema(pg_prop_tc *tc)
{
	if (schema_built)
		return &the_schema;

	the_schema.ncols = 0;
	qg_add_column(&the_schema, "a", "int", draw_int_or_null, false, true);
	qg_add_column(&the_schema, "b", "int", draw_int_or_null, false, true);
	qg_add_column(&the_schema, "c", "text", draw_text_or_null, true, false);
	qg_add_column(&the_schema, "f", "float8", draw_float_or_null, false, true);
	qg_add_column(&the_schema, "d", "numeric(5,2)", draw_numeric_or_null, false, true);
	qg_add_column(&the_schema, "g", "boolean", draw_bool_or_null, false, false);

	/* let extensions add their own columns */
	pg_prop_run_hooks(tc, "query_gen.column", &the_schema);

	schema_built = true;
	return &the_schema;
}

const qg_schema *
qg_current_schema(pg_prop_tc *tc)
{
	return build_schema(tc);
}

/* ---- table build ----------------------------------------------------- */

void
qg_build_table(pg_prop_tc *tc, pg_prop_conn *conn, int *nrows)
{
	char	   *err = NULL;
	StringInfoData ddl;
	const qg_schema *s = build_schema(tc);
	int			n;
	int			r;
	int			col;

	/*
	 * Let extensions run one-time-per-case setup DDL (typically CREATE
	 * EXTENSION) so any types their columns use exist before the table is
	 * built.  The ctx is the live connection; a contributor runs idempotent
	 * DDL (CREATE EXTENSION IF NOT EXISTS).
	 */
	pg_prop_run_hooks(tc, "query_gen.setup", conn);

	initStringInfo(&ddl);
	appendStringInfoString(&ddl,
						   "DROP TABLE IF EXISTS " QG_TABLE "; "
						   "CREATE TABLE " QG_TABLE " (id int PRIMARY KEY");
	for (col = 0; col < s->ncols; col++)
		appendStringInfo(&ddl, ", %s %s", s->cols[col].name, s->cols[col].sqltype);
	appendStringInfoString(&ddl, "); ");

	n = (int) pg_prop_draw_int(tc, 0, QG_MAX_ROWS);
	for (r = 1; r <= n; r++)
	{
		appendStringInfo(&ddl, "INSERT INTO " QG_TABLE " VALUES (%d", r);
		for (col = 0; col < s->ncols; col++)
		{
			appendStringInfoString(&ddl, ", ");
			s->cols[col].draw(tc, &ddl);
		}
		appendStringInfoString(&ddl, "); ");
	}

	/*
	 * Under the deep-hunt profiles, sometimes append a server-generated bulk
	 * of rows after the drawn ones.  A planner does not consider an index for
	 * a 40-row table, so without volume the index, bitmap, parallel and spill
	 * paths a differential oracle wants to compare never get chosen.  The bulk
	 * is skewed the same way the drawn values are (a dominant 0, NULLs, the
	 * int extremes) and deterministic in the drawn size, so cases replay.
	 */
	if (qg_deep_requested() && pg_prop_draw_bool(tc))
	{
		int			bulk = (int) pg_prop_draw_int(tc, 500, 20000);

		appendStringInfo(&ddl,
						 "INSERT INTO " QG_TABLE " (id, a, b, c, d, f, g) "
						 "SELECT %d + i, "
						 "CASE WHEN i %% 10 < 6 THEN 0 WHEN i %% 10 = 6 THEN NULL "
						 "WHEN i %% 97 = 0 THEN 2147483647 WHEN i %% 89 = 0 THEN (-2147483647 - 1) "
						 "ELSE (hashint4(i) %% 1000) END, "
						 "CASE WHEN i %% 7 = 0 THEN NULL ELSE hashint4(i * 31) %% 50 END, "
						 "CASE WHEN i %% 11 = 0 THEN NULL WHEN i %% 3 = 0 THEN 'a' "
						 "ELSE chr(97 + abs(hashint4(i)) %% 3) || repeat('b', i %% 3) END, "
						 "CASE WHEN i %% 13 = 0 THEN NULL ELSE ((hashint4(i * 7) %% 99999) / 100.0)::numeric(5,2) END, "
						 "CASE WHEN i %% 17 = 0 THEN NULL WHEN i %% 101 = 0 THEN 'NaN'::float8 "
						 "ELSE (hashint4(i * 13) %% 1000) / 10.0 END, "
						 "CASE WHEN i %% 5 = 0 THEN NULL ELSE i %% 2 = 0 END "
						 "FROM generate_series(1, %d) i; ",
						 n, bulk);
		n += bulk;
	}

	/*
	 * Secondary indexes, drawn per case: without them the planner has a single
	 * path for a one-table query (seqscan), and a differential oracle compares
	 * a plan with itself.  Single-column, composite, partial and expression
	 * indexes open index, index-only, bitmap-AND/OR and presorted paths.
	 */
	if (pg_prop_draw_bool(tc))
		appendStringInfoString(&ddl, "CREATE INDEX ON " QG_TABLE " (a); ");
	if (pg_prop_draw_bool(tc))
		appendStringInfoString(&ddl, "CREATE INDEX ON " QG_TABLE " (b, a); ");
	if (pg_prop_draw_bool(tc))
		appendStringInfoString(&ddl, "CREATE INDEX ON " QG_TABLE " (c); ");
	if (pg_prop_draw_int(tc, 0, 2) == 0)
		appendStringInfoString(&ddl, "CREATE INDEX ON " QG_TABLE " (d) WHERE d IS NOT NULL; ");
	if (pg_prop_draw_int(tc, 0, 2) == 0)
		appendStringInfoString(&ddl, "CREATE INDEX ON " QG_TABLE " ((coalesce(a, 0)::bigint + 1)); ");

	/* ANALYZE so the planner has stats and will actually consider plans. */
	appendStringInfoString(&ddl, "ANALYZE " QG_TABLE ";");

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "generator DDL/DML failed: %s", err ? err : "");

	pfree(ddl.data);
	*nrows = n;
}

/* ---- predicate drawing ----------------------------------------------- */

/* Pick a core numeric-like column name (a, b, or f) for a leaf. */
static const char *
pick_num_col(pg_prop_tc *tc)
{
	int			col = (int) pg_prop_draw_int(tc, 0, 2);

	return (col == 0) ? "a" : (col == 1) ? "b" : "f";
}

/*
 * One leaf comparison. The grammar is deliberately rich: besides the plain
 * comparisons it emits three-valued-logic edge forms (IS DISTINCT FROM, IS
 * NULL/NOT NULL), CASE expressions, and cross-type comparisons -- exactly the
 * type-interaction and NULL-handling code where planner/executor bugs hide.
 */
static void
draw_leaf(pg_prop_tc *tc, StringInfo buf)
{
	static const char *const cmp[] = {"=", "<>", "<", "<=", ">", ">="};
	int			have_ext = pg_prop_hooks_count("query_gen.predicate") > 0 ? 1 : 0;
	/* kinds 0..10 are built-in; 11 is the extension leaf when present. */
	int			n_builtin = 11;
	int			kind = (int) pg_prop_draw_int(tc, 0, n_builtin - 1 + have_ext);
	const char *colname;

	switch (kind)
	{
		case 0:					/* col IS NULL */
			appendStringInfo(buf, "%s IS NULL", pick_num_col(tc));
			break;
		case 1:					/* col IS NOT NULL */
			appendStringInfo(buf, "%s IS NOT NULL", pick_num_col(tc));
			break;
		case 2:					/* c comparison against text */
			appendStringInfoString(buf, "c ");
			appendStringInfoString(buf, cmp[pg_prop_draw_int(tc, 0, 5)]);
			appendStringInfoChar(buf, ' ');
			draw_text_or_null(tc, buf);
			break;
		case 3:					/* col IN (list) */
			colname = pick_num_col(tc);
			appendStringInfo(buf, "%s IN (", colname);
			{
				int			m = (int) pg_prop_draw_int(tc, 1, 3);
				int			i;

				for (i = 0; i < m; i++)
				{
					if (i > 0)
						appendStringInfoString(buf, ", ");
					if (strcmp(colname, "f") == 0)
						draw_float_or_null(tc, buf);
					else
						draw_int_or_null(tc, buf);
				}
			}
			appendStringInfoChar(buf, ')');
			break;
		case 4:					/* col BETWEEN lo AND hi */
			appendStringInfo(buf, "%s BETWEEN %d AND %d", pick_num_col(tc),
							 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX),
							 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX));
			break;
		case 5:					/* numeric column d compared, or bool g */
			if (pg_prop_draw_bool(tc))
				appendStringInfo(buf, "d %s %d.%02d",
								 cmp[pg_prop_draw_int(tc, 0, 5)],
								 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX),
								 (int) pg_prop_draw_int(tc, 0, 99));
			else
				appendStringInfo(buf, "g IS %s",
								 pg_prop_draw_bool(tc) ? "TRUE" : "NOT TRUE");
			break;
		case 7:					/* col IS [NOT] DISTINCT FROM const (3VL edge) */
			colname = pick_num_col(tc);
			appendStringInfo(buf, "%s IS %sDISTINCT FROM ", colname,
							 pg_prop_draw_bool(tc) ? "" : "NOT ");
			if (strcmp(colname, "f") == 0)
				draw_float_or_null(tc, buf);
			else
				draw_int_or_null(tc, buf);
			break;
		case 8:					/* CASE expression used as a boolean */
			appendStringInfoString(buf, "(CASE WHEN ");
			appendStringInfo(buf, "a %s ", cmp[pg_prop_draw_int(tc, 0, 5)]);
			draw_int_or_null(tc, buf);
			appendStringInfoString(buf, " THEN b ELSE ");
			draw_int_or_null(tc, buf);
			appendStringInfo(buf, " END) %s ", cmp[pg_prop_draw_int(tc, 0, 5)]);
			draw_int_or_null(tc, buf);
			break;
		case 9:					/* cross-type: int column vs numeric/text cast */
			if (pg_prop_draw_bool(tc))
				appendStringInfo(buf, "a::numeric %s %d.%02d",
								 cmp[pg_prop_draw_int(tc, 0, 5)],
								 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX),
								 (int) pg_prop_draw_int(tc, 0, 99));
			else
				appendStringInfo(buf, "a::text %s c",
								 cmp[pg_prop_draw_int(tc, 0, 5)]);
			break;
		case 10:				/* COALESCE in the comparison (NULL coalescing) */
			appendStringInfo(buf, "coalesce(a, b, %d) %s ",
							 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX),
							 cmp[pg_prop_draw_int(tc, 0, 5)]);
			draw_int_or_null(tc, buf);
			break;
		case 11:				/* extension-contributed leaf */
			{
				qg_pred_ctx pctx;
				int			n = pg_prop_hooks_count("query_gen.predicate");

				pctx.schema = &the_schema;
				pctx.buf = buf;
				pg_prop_run_hook_n(tc, "query_gen.predicate",
								   (int) pg_prop_draw_int(tc, 0, n - 1), &pctx);
			}
			break;
		default:				/* col <cmp> const */
			colname = pick_num_col(tc);
			appendStringInfo(buf, "%s %s ", colname, cmp[pg_prop_draw_int(tc, 0, 5)]);
			if (strcmp(colname, "f") == 0)
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
	build_schema(tc);			/* ensure hooks counted before drawing */
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
			/* coalesce so NULLs don't dominate; a + b, bigint-safe vs INT_MAX */
			appendStringInfoString(buf, "(coalesce(a::bigint,0) + coalesce(b::bigint,0))");
			break;
		default:
			appendStringInfoString(buf, "(coalesce(a::bigint,0) * 2)");
			break;
	}
}

/* ---- multi-table database model (Phase 0) ---------------------------- *
 *
 * Everything below builds a small random database of related tables for the
 * deep oracles.  It is independent of the single-table generator above; the
 * existing areas do not use it.
 */

/* A low-cardinality int column: tiny domain, so values repeat heavily. */
static void
draw_lowcard_or_null(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 9) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, 0, 2));
}

/* A high-cardinality int column: wide domain, so values are near-unique. */
static void
draw_highcard_or_null(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 19) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d", (int) pg_prop_draw_int(tc, 0, 100000));
}

/*
 * TOAST pressure.  toast_profile selects how often this text column's value is
 * large enough to be stored out-of-line/compressed (threshold ~2KB): 0 never,
 * 1 ~10%, 2 ~50%, 3 always.  Large values are a repeated short run so they
 * also compress, exercising the compressed-TOAST path; the value is still
 * deterministic given the draws so a counterexample replays.
 */
static int qg_toast_profile = 0;

static void
draw_toast_text_or_null(pg_prop_tc *tc, StringInfo buf)
{
	int			roll;
	bool		big;
	int			reps;
	int			i;

	if (pg_prop_draw_int(tc, 0, 9) == 0)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}
	roll = (int) pg_prop_draw_int(tc, 0, 9);
	switch (qg_toast_profile)
	{
		case 0:
			big = false;
			break;
		case 1:
			big = (roll == 0);
			break;
		case 2:
			big = (roll < 5);
			break;
		default:
			big = true;
			break;
	}
	appendStringInfoChar(buf, '\'');
	if (big)
	{
		/* ~3KB of a repeated unit -> past the 2KB TOAST threshold */
		reps = 600 + (int) pg_prop_draw_int(tc, 0, 400);
		for (i = 0; i < reps; i++)
			appendStringInfoString(buf, "xy");
	}
	else
	{
		int			len = (int) pg_prop_draw_int(tc, 0, 4);

		for (i = 0; i < len; i++)
			appendStringInfoChar(buf, (char) ('a' + pg_prop_draw_int(tc, 0, 2)));
	}
	appendStringInfoChar(buf, '\'');
}

void
qg_db_params_default(qg_db_params *p)
{
	p->min_tables = 2;
	p->max_tables = 3;
	p->min_cols = 2;
	p->max_cols = 4;
	p->min_rows = 0;
	p->max_rows = 30;
	p->allow_fk = true;
	p->allow_triggers = false;
	p->allow_inherit = false;
	p->toast_profile = 0;
	p->cardinality_bias = 0;
}

void
qg_db_params_deep(qg_db_params *p)
{
	p->min_tables = 3;
	p->max_tables = QG_MAX_TABLES;
	p->min_cols = 4;
	p->max_cols = 32;			/* wide tables */
	p->min_rows = 0;
	p->max_rows = 2000;			/* volume extremes: some empty, some large */
	p->allow_fk = true;
	p->allow_triggers = true;	/* result-neutral triggers in the deep hunt */
	p->allow_inherit = true;
	p->toast_profile = 2;
	p->cardinality_bias = 0;
}

/*
 * A leaner deep profile: still multi-table with FKs, subquery/unnest shapes,
 * TOAST and triggers, but with bounded table and row counts so the per-case
 * DDL cost and memory stay low enough to run tens of thousands of iterations
 * unattended on a modest box.  This is the profile the opt-in high-iteration
 * hunt should use; qg_db_params_deep() is the "everything maximal" profile for
 * a short, heavily-resourced run.
 */
void
qg_db_params_deep_fast(qg_db_params *p)
{
	p->min_tables = 2;
	p->max_tables = 6;
	p->min_cols = 3;
	p->max_cols = 10;
	p->min_rows = 0;
	p->max_rows = 200;
	p->allow_fk = true;
	p->allow_triggers = true;
	p->allow_inherit = true;
	p->toast_profile = 1;		/* ~10% large values: TOAST path, less bulk */
	p->cardinality_bias = 0;
}

/*
 * Choose the profile for a query area honouring the PG_PROP_DEEP environment
 * variable: unset/0 -> the fast, deterministic default (committed TAP run);
 * "fast" -> the leaner deep profile; "full" or any other non-empty value ->
 * the maximal deep profile.  Areas call this instead of hard-coding a profile
 * so the same binary runs the quick suite and the deep hunt.
 */
/* true when PG_PROP_DEEP asks for one of the deep-hunt profiles */
bool
qg_deep_requested(void)
{
	const char *v = getenv("PG_PROP_DEEP");

	return !(v == NULL || v[0] == '\0' || strcmp(v, "0") == 0);
}

void
qg_db_params_for_env(qg_db_params *p)
{
	const char *v = getenv("PG_PROP_DEEP");

	if (v == NULL || v[0] == '\0' || strcmp(v, "0") == 0)
		qg_db_params_default(p);
	else if (strcmp(v, "fast") == 0)
		qg_db_params_deep_fast(p);
	else
		qg_db_params_deep(p);
}

/*
 * Fill one table's schema with id (implicit PK), an optional FK column, and
 * ncols drawn data columns of mixed types and cardinalities.  Column names are
 * c0, c1, ...  Returns the FK column index (or -1) via *fkcol.
 */
static void
build_table_schema(pg_prop_tc *tc, qg_table *t, int ncols, int fkref,
				   int cardinality_bias)
{
	char	   *namebuf;
	int			i;

	t->schema.ncols = 0;
	t->fkcol = -1;
	t->fkref = fkref;

	if (fkref >= 0)
	{
		/* FK column references parent's id; correlated at populate time. */
		qg_add_column(&t->schema, "fk", "int", draw_int_or_null, false, true);
		t->fkcol = 0;
	}

	for (i = 0; i < ncols; i++)
	{
		int			kind = (int) pg_prop_draw_int(tc, 0, 5);
		qg_value_fn draw;
		const char *type;
		bool		textlike = false;
		bool		numlike = false;

		/* a cardinality bias favours low-card columns for grouping stress */
		if (cardinality_bias == 1 && pg_prop_draw_bool(tc))
			kind = 0;

		switch (kind)
		{
			case 0:				/* low-cardinality int */
				draw = draw_lowcard_or_null;
				type = "int";
				numlike = true;
				break;
			case 1:				/* high-cardinality int */
				draw = draw_highcard_or_null;
				type = "int";
				numlike = true;
				break;
			case 2:				/* small int */
				draw = draw_int_or_null;
				type = "int";
				numlike = true;
				break;
			case 3:				/* text */
				draw = draw_text_or_null;
				type = "text";
				textlike = true;
				break;
			case 4:				/* numeric */
				draw = draw_numeric_or_null;
				type = "numeric(6,2)";
				numlike = true;
				break;
			default:			/* TOAST-eligible text */
				draw = draw_toast_text_or_null;
				type = "text";
				textlike = true;
				break;
		}
		namebuf = psprintf("c%d", i);
		qg_add_column(&t->schema, namebuf, type, draw, textlike, numlike);
	}
}

/*
 * Emit SET STORAGE for the varlena (text/numeric) columns of a table, cycling
 * through PLAIN/MAIN/EXTERNAL/EXTENDED so the TOAST storage machinery is
 * exercised in all its modes.  PLAIN forbids out-of-line storage (so a large
 * value would error) -- so only choose PLAIN for columns the generator keeps
 * small; the TOAST-eligible text columns (draw == draw_toast_text_or_null) get
 * EXTERNAL (uncompressed out-of-line) or EXTENDED (compressed), never PLAIN.
 */
static void
emit_storage_modes(pg_prop_tc *tc, StringInfo ddl, const qg_table *t)
{
	int			col;

	for (col = 0; col < t->schema.ncols; col++)
	{
		const qg_column *c = &t->schema.cols[col];
		const char *mode;
		bool		toastable = c->is_text_like || !c->is_numeric_like;

		/* only varlena columns have a meaningful STORAGE setting */
		if (!c->is_text_like &&
			strncmp(c->sqltype, "numeric", 7) != 0)
			continue;

		if (c->draw == draw_toast_text_or_null)
			mode = pg_prop_draw_bool(tc) ? "EXTERNAL" : "EXTENDED";
		else
		{
			static const char *const modes[] = {"PLAIN", "MAIN",
				"EXTERNAL", "EXTENDED"};

			mode = modes[pg_prop_draw_int(tc, 0, 3)];
		}
		(void) toastable;
		appendStringInfo(ddl, "ALTER TABLE %s ALTER COLUMN %s SET STORAGE %s; ",
						 t->name, c->name, mode);
	}
}

qg_db *
qg_build_db(pg_prop_tc *tc, pg_prop_conn *conn, const qg_db_params *p)
{
	qg_db	   *db = palloc0(sizeof(qg_db));
	StringInfoData ddl;
	char	   *err = NULL;
	int			nt;
	int			ti;

	qg_toast_profile = p->toast_profile;

	nt = (int) pg_prop_draw_int(tc, p->min_tables, p->max_tables);
	if (nt < 1)
		nt = 1;
	if (nt > QG_MAX_TABLES)
		nt = QG_MAX_TABLES;
	db->ntables = nt;

	initStringInfo(&ddl);

	/*
	 * A result-neutral trigger function: it fires on write but returns the
	 * row unchanged, so it changes nothing a SELECT can observe while still
	 * forcing the relation to carry trigger metadata and the executor to run
	 * the trigger-firing path (which disables some optimisations, e.g.
	 * tuple-routing and HOT shortcuts).  Created idempotently.
	 */
	if (p->allow_triggers)
		appendStringInfoString(&ddl,
							   "CREATE OR REPLACE FUNCTION qg_noop_trig() "
							   "RETURNS trigger LANGUAGE plpgsql AS "
							   "$$ BEGIN RETURN NEW; END $$; ");

	for (ti = 0; ti < nt; ti++)
	{
		qg_table   *t = &db->tables[ti];
		int			ncols;
		int			fkref;
		int			col;

		snprintf(t->name, QG_NAME_LEN, "qg_t%d", ti);

		/* a child may reference any earlier table (so parents build first) */
		fkref = -1;
		if (p->allow_fk && ti > 0 && pg_prop_draw_bool(tc))
			fkref = (int) pg_prop_draw_int(tc, 0, ti - 1);

		ncols = (int) pg_prop_draw_int(tc, p->min_cols, p->max_cols);
		build_table_schema(tc, t, ncols, fkref, p->cardinality_bias);

		/* DROP first (reverse order handled by qg_drop_db; here just reset) */
		appendStringInfo(&ddl, "DROP TABLE IF EXISTS %s CASCADE; ", t->name);
		appendStringInfo(&ddl, "CREATE TABLE %s (id int PRIMARY KEY", t->name);
		for (col = 0; col < t->schema.ncols; col++)
			appendStringInfo(&ddl, ", %s %s",
							 t->schema.cols[col].name,
							 t->schema.cols[col].sqltype);
		/* declare the FK (references the parent's dense id) */
		if (t->fkref >= 0)
			appendStringInfo(&ddl,
							 ", FOREIGN KEY (fk) REFERENCES %s(id)",
							 db->tables[t->fkref].name);
		appendStringInfoString(&ddl, "); ");

		/* 0b: vary TOAST storage modes on the varlena columns */
		emit_storage_modes(tc, &ddl, t);

		/*
		 * 0c: attach a result-neutral BEFORE INSERT/UPDATE row trigger to some
		 * tables, so the relation carries trigger metadata and the write path
		 * runs the trigger.  Result-neutral, so SELECT oracles stay sound.
		 */
		if (p->allow_triggers && pg_prop_draw_bool(tc))
			appendStringInfo(&ddl,
							 "CREATE TRIGGER %s_trig BEFORE INSERT OR UPDATE ON %s "
							 "FOR EACH ROW EXECUTE FUNCTION qg_noop_trig(); ",
							 t->name, t->name);
	}

	/* populate parents before children (ascending ti == build order) */
	for (ti = 0; ti < nt; ti++)
	{
		qg_table   *t = &db->tables[ti];
		int			n = (int) pg_prop_draw_int(tc, p->min_rows, p->max_rows);
		int			parent_rows = (t->fkref >= 0) ? db->tables[t->fkref].nrows : 0;
		int			r;
		int			col;

		t->nrows = n;
		for (r = 1; r <= n; r++)
		{
			appendStringInfo(&ddl, "INSERT INTO %s VALUES (%d", t->name, r);
			for (col = 0; col < t->schema.ncols; col++)
			{
				appendStringInfoString(&ddl, ", ");
				if (col == t->fkcol)
				{
					/*
					 * Correlated FK: draw from the parent's actual key range
					 * 1..parent_rows, with a slice of NULLs.  (The FK is
					 * enforced, so no orphans -- a referenced id always
					 * exists.)  If the parent is empty, the only satisfiable
					 * value is NULL.
					 */
					if (parent_rows == 0 || pg_prop_draw_int(tc, 0, 4) == 0)
						appendStringInfoString(&ddl, "NULL");
					else
						appendStringInfo(&ddl, "%d",
										 (int) pg_prop_draw_int(tc, 1, parent_rows));
				}
				else
					t->schema.cols[col].draw(tc, &ddl);
			}
			appendStringInfoString(&ddl, "); ");
		}

		/*
		 * Secondary indexes.  Without them the planner has nothing to choose
		 * between on a single relation (seqscan is the only path), so most of
		 * a differential oracle's forced configurations collapse onto the same
		 * plan.  An index on the FK and on one or two data columns (sometimes
		 * composite, sometimes partial) opens index, index-only, bitmap and
		 * merge-join-on-presorted paths, plus Memoize over a parameterized
		 * inner index scan.
		 */
		if (t->fkcol >= 0 && pg_prop_draw_int(tc, 0, 3) != 0)
			appendStringInfo(&ddl, "CREATE INDEX ON %s (%s); ", t->name,
							 t->schema.cols[t->fkcol].name);
		for (col = 0; col < t->schema.ncols; col++)
		{
			int			k;

			/*
			 * Text-like columns can carry TOAST-sized values that exceed the
			 * btree tuple limit, so only scalar columns are indexed.
			 */
			if (col == t->fkcol || t->schema.cols[col].is_text_like ||
				pg_prop_draw_int(tc, 0, 2) != 0)
				continue;
			k = (int) pg_prop_draw_int(tc, 0, 2);
			if (k == 1 && (col + 1 >= t->schema.ncols ||
						   t->schema.cols[col + 1].is_text_like))
				k = 0;
			if (k == 0)
				appendStringInfo(&ddl, "CREATE INDEX ON %s (%s); ", t->name,
								 t->schema.cols[col].name);
			else if (k == 1)
				appendStringInfo(&ddl, "CREATE INDEX ON %s (%s, %s); ", t->name,
								 t->schema.cols[col].name,
								 t->schema.cols[col + 1].name);
			else
				appendStringInfo(&ddl,
								 "CREATE INDEX ON %s (%s) WHERE %s IS NOT NULL; ",
								 t->name, t->schema.cols[col].name,
								 t->schema.cols[col].name);
		}

		/*
		 * Inheritance child.  Some tables get a child holding a few rows that
		 * duplicate the parent's ids (a parent PRIMARY KEY does not constrain
		 * its children).  A query over the parent scans both, so a planner
		 * proof that leans on the parent's unique index -- self-join
		 * elimination, uniqueness-based join removal, DISTINCT elision -- is
		 * unsound there, while "ONLY parent" must still see only the parent.
		 */
		if (p->allow_inherit && n > 0 && pg_prop_draw_int(tc, 0, 2) == 0)
		{
			int			m = (int) pg_prop_draw_int(tc, 1, n < 5 ? n : 5);

			appendStringInfo(&ddl, "CREATE TABLE %s_c () INHERITS (%s); ",
							 t->name, t->name);
			for (r = 1; r <= m; r++)
			{
				appendStringInfo(&ddl, "INSERT INTO %s_c VALUES (%d", t->name,
								 (int) pg_prop_draw_int(tc, 1, n));
				for (col = 0; col < t->schema.ncols; col++)
				{
					appendStringInfoString(&ddl, ", ");
					if (col == t->fkcol)
						appendStringInfoString(&ddl, "NULL");	/* FK not inherited */
					else
						t->schema.cols[col].draw(tc, &ddl);
				}
				appendStringInfoString(&ddl, "); ");
			}
			appendStringInfo(&ddl, "ANALYZE %s_c; ", t->name);
		}
		appendStringInfo(&ddl, "ANALYZE %s; ", t->name);
	}

	if (!pg_prop_sql_exec(conn, ddl.data, &err))
		pg_prop_fail(tc, "database DDL/DML failed: %s", err ? err : "");

	pfree(ddl.data);
	return db;
}

void
qg_drop_db(pg_prop_tc *tc, pg_prop_conn *conn, qg_db *db)
{
	StringInfoData ddl;
	char	   *err = NULL;
	int			ti;

	initStringInfo(&ddl);
	/* drop children before parents: reverse build order, CASCADE for safety */
	for (ti = db->ntables - 1; ti >= 0; ti--)
		appendStringInfo(&ddl, "DROP TABLE IF EXISTS %s CASCADE; ",
						 db->tables[ti].name);
	(void) pg_prop_sql_exec(conn, ddl.data, &err);
	if (err)
		pg_free(err);
	pfree(ddl.data);
	pfree(db);
}

/*
 * Schema- and alias-aware predicate leaf/tree over a specific table.  Mirrors
 * draw_leaf/draw_predicate_rec but qualifies every column with "alias." and
 * only references columns that exist in this table's schema.
 */
static void
draw_leaf_on(pg_prop_tc *tc, StringInfo buf, const qg_table *t,
			 const char *alias)
{
	static const char *const cmp[] = {"=", "<>", "<", "<=", ">", ">="};
	const qg_column *c;
	int			ci;

	if (t->schema.ncols == 0)
	{
		/* no data columns: fall back to a key predicate */
		appendStringInfo(buf, "%s.id %s %d", alias,
						 cmp[pg_prop_draw_int(tc, 0, 5)],
						 (int) pg_prop_draw_int(tc, 0, 10));
		return;
	}
	ci = (int) pg_prop_draw_int(tc, 0, t->schema.ncols - 1);
	c = &t->schema.cols[ci];

	switch ((int) pg_prop_draw_int(tc, 0, 3))
	{
		case 0:
			appendStringInfo(buf, "%s.%s IS NULL", alias, c->name);
			break;
		case 1:
			appendStringInfo(buf, "%s.%s IS NOT NULL", alias, c->name);
			break;
		default:
			appendStringInfo(buf, "%s.%s %s ", alias, c->name,
							 cmp[pg_prop_draw_int(tc, 0, 5)]);
			if (c->is_text_like)
				draw_text_or_null(tc, buf);
			else
				draw_int_or_null(tc, buf);
			break;
	}
}

static void
draw_predicate_on_rec(pg_prop_tc *tc, StringInfo buf, const qg_table *t,
					  const char *alias, int depth)
{
	int			kind = (depth >= 2) ? 0 : (int) pg_prop_draw_int(tc, 0, 4);

	switch (kind)
	{
		case 1:
			appendStringInfoString(buf, "NOT (");
			draw_predicate_on_rec(tc, buf, t, alias, depth + 1);
			appendStringInfoChar(buf, ')');
			break;
		case 2:
			appendStringInfoChar(buf, '(');
			draw_predicate_on_rec(tc, buf, t, alias, depth + 1);
			appendStringInfoString(buf, " AND ");
			draw_predicate_on_rec(tc, buf, t, alias, depth + 1);
			appendStringInfoChar(buf, ')');
			break;
		case 3:
			appendStringInfoChar(buf, '(');
			draw_predicate_on_rec(tc, buf, t, alias, depth + 1);
			appendStringInfoString(buf, " OR ");
			draw_predicate_on_rec(tc, buf, t, alias, depth + 1);
			appendStringInfoChar(buf, ')');
			break;
		default:
			draw_leaf_on(tc, buf, t, alias);
			break;
	}
}

void
qg_draw_predicate_on(pg_prop_tc *tc, StringInfo buf, const qg_table *t,
					 const char *alias)
{
	draw_predicate_on_rec(tc, buf, t, alias, 0);
}

/* ---- deep query builder (Phase 1) ------------------------------------ */

/*
 * Build a left-deep join chain starting at table "start", following FK edges:
 * each step joins in the table that "start"'s current frontier references (its
 * parent) or a child that references the frontier.  Aliases are r0, r1, ...
 * The chain is at most max_joins long and stops when no further FK edge is
 * available.  Appends the "<tbl> r0 JOIN <tbl> r1 ON ..." text to "from" and
 * records each alias's table in used[] / aliases[].
 *
 * Returns the number of tables in the chain (>= 1).
 */
static int
build_join_chain(pg_prop_tc *tc, const qg_db *db, int start, int max_joins,
				 StringInfo from, const qg_table *used[], char aliases[][QG_NAME_LEN])
{
	int			n = 0;
	int			cur = start;

	used[0] = &db->tables[cur];
	snprintf(aliases[0], QG_NAME_LEN, "r0");
	appendStringInfo(from, "%s r0", db->tables[cur].name);
	n = 1;

	while (n <= max_joins)
	{
		const qg_table *frontier = used[n - 1];
		int			nexttab = -1;
		bool		frontier_is_child = false;

		/* prefer the frontier's own parent (frontier.fk -> parent.id) */
		if (frontier->fkref >= 0)
		{
			nexttab = frontier->fkref;
			frontier_is_child = true;
		}
		else
		{
			/* else find any child that references the frontier */
			for (int i = 0; i < db->ntables; i++)
				if (db->tables[i].fkref >= 0 &&
					&db->tables[db->tables[i].fkref] == frontier)
				{
					nexttab = i;
					frontier_is_child = false;
					break;
				}
		}
		if (nexttab < 0)
			break;

		used[n] = &db->tables[nexttab];
		snprintf(aliases[n], QG_NAME_LEN, "r%d", n);

		/*
		 * The join member is not always a plain relation.  The planner's most
		 * fragile code is in pulling up subqueries, translating appendrel
		 * (UNION ALL / inheritance) members, and placing lateral references --
		 * so the member is drawn from those shapes too.  Every shape exposes
		 * the columns id and fk under the member alias, so the join clause,
		 * the key projection and the predicates stay unchanged.
		 *
		 *   0  plain relation (inner or LEFT join, as before)
		 *   1  ONLY relation (excludes any inheritance child)
		 *   2  derived table with DISTINCT (subquery with sorted paths)
		 *   3  UNION ALL of two filtered halves of the relation (appendrel)
		 *   4  LATERAL subquery correlated to the previous member, OFFSET 0
		 *      so it is not flattened
		 *   5  LATERAL UNION ALL whose first arm is itself a DISTINCT subquery
		 *      and whose correlation references the previous member
		 */
		{
			const char *tn = db->tables[nexttab].name;
			const char *prev = aliases[n - 1];
			const char *jt = pg_prop_draw_bool(tc) ? "" : "LEFT";
			int			shape = (int) pg_prop_draw_int(tc, 0, 5);
			/* derived shapes expose id, plus fk when the table has one */
			const char *cols = db->tables[nexttab].fkcol >= 0 ? "id, fk" : "id";
			const char *xcols = db->tables[nexttab].fkcol >= 0 ? "x.id, x.fk" : "x.id";

			switch (shape)
			{
				case 1:
					appendStringInfo(from, " %s JOIN ONLY %s %s ON ", jt, tn,
									 aliases[n]);
					break;
				case 2:
					appendStringInfo(from,
									 " %s JOIN (SELECT DISTINCT %s FROM %s) %s ON ",
									 jt, cols, tn, aliases[n]);
					break;
				case 3:
					appendStringInfo(from,
									 " %s JOIN (SELECT %s FROM %s WHERE id %% 2 = 0 "
									 "UNION ALL SELECT %s FROM %s WHERE id %% 2 <> 0) %s ON ",
									 jt, cols, tn, cols, tn, aliases[n]);
					break;
				case 4:
				case 5:
					{
						/*
						 * The correlation is the FK equality itself, moved
						 * inside the lateral subquery; the outer ON is TRUE.
						 * Same rows as the plain join, different tree.
						 */
						StringInfoData corr;

						initStringInfo(&corr);
						if (frontier_is_child)
							appendStringInfo(&corr, "%s.fk = x.id", prev);
						else
							appendStringInfo(&corr, "x.fk = %s.id", prev);
						if (shape == 4)
							appendStringInfo(from,
											 " %s JOIN LATERAL (SELECT %s FROM %s x "
											 "WHERE %s OFFSET 0) %s ON true",
											 jt, xcols, tn, corr.data, aliases[n]);
						else
							appendStringInfo(from,
											 " %s JOIN LATERAL (SELECT %s FROM "
											 "(SELECT DISTINCT %s FROM %s) x WHERE %s "
											 "AND x.id %% 2 = 0 "
											 "UNION ALL SELECT %s FROM %s x WHERE %s "
											 "AND x.id %% 2 <> 0) %s ON true",
											 jt, xcols, cols, tn, corr.data,
											 xcols, tn, corr.data, aliases[n]);
						pfree(corr.data);
						n++;
						continue;	/* join condition already placed */
					}
				default:
					appendStringInfo(from, " %s JOIN %s %s ON ", jt, tn,
									 aliases[n]);
					break;
			}
		}
		if (frontier_is_child)
			/* frontier.fk = new.id */
			appendStringInfo(from, "%s.fk = %s.id", aliases[n - 1], aliases[n]);
		else
			/* new.fk = frontier.id */
			appendStringInfo(from, "%s.fk = %s.id", aliases[n], aliases[n - 1]);
		n++;
	}
	return n;
}

bool
qg_build_query(pg_prop_tc *tc, const qg_db *db, int max_depth, qg_query *q)
{
	const qg_table *used[QG_MAX_TABLES];
	char		aliases[QG_MAX_TABLES][QG_NAME_LEN];
	StringInfoData from;
	StringInfoData keys;
	StringInfoData sel;
	int			start;
	int			nchain;
	int			maxj;
	int			i;

	if (db->ntables == 0)
		return false;

	start = (int) pg_prop_draw_int(tc, 0, db->ntables - 1);
	maxj = (max_depth < 1) ? 1 : max_depth;
	if (maxj > db->ntables - 1)
		maxj = db->ntables - 1;
	if (maxj < 0)
		maxj = 0;

	initStringInfo(&from);
	nchain = build_join_chain(tc, db, start, maxj, &from, used, aliases);

	/*
	 * Project each joined table's id as the stable key list, aliased k0, k1,
	 * ... so the output column names are unique (two relations both have a
	 * column named id) and an outer query that wraps select_from can order by
	 * and reference the keys by those names.
	 */
	initStringInfo(&keys);		/* SELECT target list: r0.id AS k0, ... */
	{
		StringInfoData keynames;	/* k0, k1, ... for ORDER BY / outer refs */

		initStringInfo(&keynames);
		for (i = 0; i < nchain; i++)
		{
			if (i > 0)
			{
				appendStringInfoString(&keys, ", ");
				appendStringInfoString(&keynames, ", ");
			}
			appendStringInfo(&keys, "%s.id AS k%d", aliases[i], i);
			appendStringInfo(&keynames, "k%d", i);
		}

		/* the mutable predicate ranges over the anchor r0 */
		q->pred_table = used[0];
		snprintf(q->pred_alias, QG_NAME_LEN, "%s", aliases[0]);

		/*
		 * The builder returns the plain join tree: select_from is always
		 * "SELECT <keys> FROM <join>" with the mutable predicate ranging over
		 * the anchor alias r0.  Wrapping in a derived table or CTE is left to
		 * the oracles that want it (deep_cte_inline builds the inline / CTE /
		 * materialized forms on top of this), which keeps string assembly free
		 * of nested-WITH and alias collisions.  keys holds the k-named output
		 * columns, so an outer wrapper orders by and selects them by name.
		 */
		initStringInfo(&sel);
		appendStringInfo(&sel, "SELECT %s FROM %s", keys.data, from.data);
		q->select_from = pstrdup(sel.data);
		q->keys = pstrdup(keynames.data);
		q->order_by = psprintf("ORDER BY %s", keynames.data);
		pfree(keynames.data);
	}

	pfree(from.data);
	pfree(keys.data);
	pfree(sel.data);
	return true;
}

void
qg_free_query(qg_query *q)
{
	if (q->select_from)
		pfree(q->select_from);
	if (q->order_by)
		pfree(q->order_by);
	if (q->keys)
		pfree(q->keys);
	q->select_from = q->order_by = q->keys = NULL;
}
