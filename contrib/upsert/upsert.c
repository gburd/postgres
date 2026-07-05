/*-------------------------------------------------------------------------
 *
 * upsert.c
 *	  Grammar-extension demonstrator: add an UPSERT statement that lowers
 *	  to INSERT ... ON CONFLICT (...) DO UPDATE.
 *
 *	  UPSERT INTO t (c1, ..., cN) VALUES (v1, ..., vN) ON (k1, ..., kM)
 *
 *	  is rewritten at parse time into the InsertStmt that the planner and
 *	  executor already understand:
 *
 *	  INSERT INTO t (c1, ..., cN) VALUES (v1, ..., vN)
 *	    ON CONFLICT (k1, ..., kM)
 *	    DO UPDATE SET c = excluded.c   -- for every c not in {k1..kM}
 *
 *	  If every written column is a conflict column there is nothing to
 *	  update, so the rewrite degrades to ON CONFLICT (...) DO NOTHING.
 *
 *	  The extension adds one keyword (UPSERT) and one production to the
 *	  in-process composed grammar via parser_extension.h.  It executes no
 *	  SQL itself -- it only produces a parse tree.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/upsert/upsert.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/lockoptions.h"
#include "parser/parser_extension.h"
#include "utils/builtins.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/*
 * Build the ON CONFLICT (...) DO UPDATE/NOTHING targetList: one
 * "<col> = excluded.<col>" ResTarget for every written column whose name
 * is NOT among the conflict columns.
 */
static List *
upsert_make_update_set(List *cols, List *conflict_cols)
{
	List	   *set_list = NIL;
	ListCell   *lc;

	foreach(lc, cols)
	{
		ResTarget  *col = lfirst_node(ResTarget, lc);
		const char *colname = col->name;
		bool		is_conflict_col = false;
		ListCell   *kc;

		if (colname == NULL)
			continue;

		foreach(kc, conflict_cols)
		{
			ResTarget  *kcol = lfirst_node(ResTarget, kc);

			if (kcol->name != NULL && strcmp(kcol->name, colname) == 0)
			{
				is_conflict_col = true;
				break;
			}
		}
		if (is_conflict_col)
			continue;

		/* SET <colname> = excluded.<colname> */
		{
			ColumnRef  *excluded = makeNode(ColumnRef);
			ResTarget  *rt = makeNode(ResTarget);

			excluded->fields = list_make2(makeString(pstrdup("excluded")),
										  makeString(pstrdup(colname)));
			excluded->location = -1;

			rt->name = pstrdup(colname);
			rt->indirection = NIL;
			rt->val = (Node *) excluded;
			rt->location = -1;
			set_list = lappend(set_list, rt);
		}
	}
	return set_list;
}

/*
 * Build the InferClause (conflict target) from the ON (...) column list.
 */
static InferClause *
upsert_make_infer(List *conflict_cols)
{
	InferClause *infer = makeNode(InferClause);
	List	   *elems = NIL;
	ListCell   *lc;

	foreach(lc, conflict_cols)
	{
		ResTarget  *col = lfirst_node(ResTarget, lc);
		IndexElem  *ie;

		if (col->name == NULL)
			continue;
		ie = makeNode(IndexElem);
		ie->name = pstrdup(col->name);
		ie->expr = NULL;
		ie->indexcolname = NULL;
		ie->collation = NIL;
		ie->opclass = NIL;
		ie->opclassopts = NIL;
		ie->ordering = SORTBY_DEFAULT;
		ie->nulls_ordering = SORTBY_NULLS_DEFAULT;
		ie->location = -1;
		elems = lappend(elems, ie);
	}

	infer->indexElems = elems;
	infer->whereClause = NULL;
	infer->conname = NULL;
	infer->location = -1;
	return infer;
}

/*
 * upsert_reduce
 *	  Reduce callback for:
 *	    upsert_stmt ::= UPSERT INTO qualified_name LPAREN insert_column_list
 *	                    RPAREN VALUES LPAREN expr_list RPAREN ON LPAREN
 *	                    insert_column_list RPAREN
 *	  RHS indices (0-based):
 *	    0 UPSERT  1 INTO  2 qualified_name  3 LPAREN  4 insert_column_list
 *	    5 RPAREN  6 VALUES  7 LPAREN  8 expr_list  9 RPAREN  10 ON
 *	    11 LPAREN 12 insert_column_list (conflict cols) 13 RPAREN
 *
 *	  Per the host-reduce ABI, rhs_values[i] is the symbol's value by value:
 *	  read each non-terminal directly as its declared %type pointer.
 */
static void
upsert_reduce(void *user_data, void *extra_arg, int nrhs,
			  const void *const *rhs_values, const int *rhs_locs,
			  void *lhs_out)
{
	RangeVar   *relation;
	List	   *cols;
	List	   *values;
	List	   *conflict_cols;
	List	   *update_set;
	SelectStmt *valstmt;
	InsertStmt *ins;
	OnConflictClause *onconflict;

	(void) user_data;
	(void) extra_arg;
	(void) rhs_locs;

	if (nrhs != 14)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("upsert: expected 14 RHS symbols, got %d", nrhs)));

	relation = (RangeVar *) rhs_values[2];
	cols = (List *) rhs_values[4];
	values = (List *) rhs_values[8];
	conflict_cols = (List *) rhs_values[12];

	/* VALUES (...) source: a single-row VALUES SelectStmt. */
	valstmt = makeNode(SelectStmt);
	valstmt->valuesLists = list_make1(values);

	/* ON CONFLICT (conflict_cols) DO UPDATE SET non-conflict = excluded.*  */
	update_set = upsert_make_update_set(cols, conflict_cols);

	onconflict = makeNode(OnConflictClause);
	onconflict->infer = upsert_make_infer(conflict_cols);
	onconflict->lockStrength = LCS_NONE;
	onconflict->whereClause = NULL;
	onconflict->location = -1;
	if (update_set == NIL)
	{
		/* Every written column is a conflict column: nothing to update. */
		onconflict->action = ONCONFLICT_NOTHING;
		onconflict->targetList = NIL;
	}
	else
	{
		onconflict->action = ONCONFLICT_UPDATE;
		onconflict->targetList = update_set;
	}

	ins = makeNode(InsertStmt);
	ins->relation = relation;
	ins->cols = cols;
	ins->selectStmt = (Node *) valstmt;
	ins->onConflictClause = onconflict;
	ins->returningClause = NULL;
	ins->withClause = NULL;
	ins->override = OVERRIDING_NOT_SET;

	*(Node **) lhs_out = (Node *) ins;
}

/*
 * Forwarder reduce for stmt ::= upsert_stmt.  This is a unit production;
 * Lime eliminates type-preserving unit reductions, so in practice this
 * callback does not fire and the upsert_stmt value flows into the stmt
 * slot unchanged.  We register a pass-through anyway (read the single RHS
 * value by value per the host-reduce ABI and write it to the LHS) so the
 * behaviour is correct whether or not the reduce runs.
 */
static void
upsert_forward(void *user_data, void *extra_arg, int nrhs,
			   const void *const *rhs_values, const int *rhs_locs,
			   void *lhs_out)
{
	(void) user_data;
	(void) extra_arg;
	(void) nrhs;
	(void) rhs_locs;
	*(Node **) lhs_out = (Node *) rhs_values[0];
}

/* Grammar fragment: one keyword, one production threaded into stmt. */
void
_PG_init(void)
{
	PgGrammarExtension *ext;
	char	   *err = NULL;
	static const char *upsert_rhs[] = {
		"UPSERT", "INTO", "qualified_name", "LPAREN", "insert_column_list",
		"RPAREN", "VALUES", "LPAREN", "expr_list", "RPAREN", "ON", "LPAREN",
		"insert_column_list", "RPAREN", NULL
	};

	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("upsert must be loaded via shared_preload_libraries"),
				 errhint("Add upsert to shared_preload_libraries in "
						 "postgresql.conf and restart the postmaster.")));

	ext = pg_grammar_ext_create("upsert", "1.0");

	/* UPSERT leads a statement; it does not collide with any base verb. */
	pg_grammar_ext_add_token(ext, "UPSERT", "upsert", UNRESERVED_KEYWORD);

	pg_grammar_ext_add_type(ext, "upsert_stmt", "Node *");

	/* upsert_stmt bubbles up to the base start symbol. */
	{
		static const char *stmt_rhs[] = {"upsert_stmt", NULL};

		pg_grammar_ext_add_rule(ext, "stmt", stmt_rhs, upsert_forward, NULL);
	}
	pg_grammar_ext_add_rule(ext, "upsert_stmt", upsert_rhs,
							upsert_reduce, NULL);

	if (!pg_grammar_ext_register(ext, &err))
		ereport(WARNING,
				(errmsg("upsert: register() failed: %s",
						err ? err : "(no detail)")));
	else
		ereport(LOG,
				(errmsg("upsert: registered (UPSERT -> INSERT ... ON CONFLICT "
						"DO UPDATE)")));
}
