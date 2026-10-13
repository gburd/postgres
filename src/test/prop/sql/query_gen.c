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

/* Small domains keep counterexamples legible and cases fast. */
#define QG_VAL_MIN (-5)
#define QG_VAL_MAX 5

/* ---- core column value generators ------------------------------------ */

/* int in [QG_VAL_MIN, QG_VAL_MAX] or SQL NULL, ~15% NULL. */
static void
draw_int_or_null(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d",
						 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX));
}

/* short [a-c] string literal or SQL NULL, ~15% NULL. */
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

/* float8 literal (small, one decimal) or SQL NULL, ~15% NULL. */
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

/* numeric literal with a scale, or SQL NULL -- exercises exact decimal paths. */
static void
draw_numeric_or_null(pg_prop_tc *tc, StringInfo buf)
{
	if (pg_prop_draw_int(tc, 0, 6) == 0)
		appendStringInfoString(buf, "NULL");
	else
		appendStringInfo(buf, "%d.%02d",
						 (int) pg_prop_draw_int(tc, QG_VAL_MIN, QG_VAL_MAX),
						 (int) pg_prop_draw_int(tc, 0, 99));
}

/* boolean literal or SQL NULL. */
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

/* One leaf comparison against a column, or an IS NULL test, or an ext leaf. */
static void
draw_leaf(pg_prop_tc *tc, StringInfo buf)
{
	static const char *const cmp[] = {"=", "<>", "<", "<=", ">", ">="};
	int			next_kinds = pg_prop_hooks_count("query_gen.predicate") > 0 ? 8 : 7;
	int			kind = (int) pg_prop_draw_int(tc, 0, next_kinds - 1);
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
		case 7:					/* extension-contributed leaf */
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
			/* coalesce so NULLs don't dominate; a + b */
			appendStringInfoString(buf, "(coalesce(a,0) + coalesce(b,0))");
			break;
		default:
			appendStringInfoString(buf, "(coalesce(a,0) * 2)");
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
	p->allow_triggers = false;	/* 0c enables */
	p->toast_profile = 2;
	p->cardinality_bias = 0;
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
