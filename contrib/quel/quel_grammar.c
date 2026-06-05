/*-------------------------------------------------------------------------
 *
 * quel_grammar.c
 *	  Reduce-callback implementations for the QUEL grammar extension.
 *
 * Each QUEL statement form has a builder function that constructs
 * the equivalent PostgreSQL parse-tree node.  The result flows
 * back through pg_grammar_ext_dispatch_reduce into the rebuilt
 * parser, which deposits it on the parse stack.  When the parse
 * completes, raw_parser() returns the node list as if the user
 * had typed an equivalent SQL statement.
 *
 * The mapping QUEL -> PostgreSQL:
 *
 *   RETRIEVE   -> SelectStmt
 *   REPLACE    -> UpdateStmt
 *   APPEND     -> InsertStmt
 *   DELETE     -> DeleteStmt
 *   CREATE     -> CreateStmt
 *   DESTROY    -> DropStmt
 *   COPY       -> CopyStmt
 *   DEFINE V.  -> ViewStmt
 *   REMOVE V.  -> DropStmt
 *   INDEX      -> IndexStmt
 *   HELP       -> (NOTICE-only, returns empty list)
 *   RANGE      -> (state update only, returns NULL)
 *
 * Tuple variables declared by RANGE are session-scoped via
 * quel_rangetab.  Subsequent statements that reference `e.name`
 * resolve `e` to its bound relation by consulting the table.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/quel/quel_grammar.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "parser/parser.h"
#include "parser/scanner.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/lsyscache.h"

#include "quel_grammar.h"

/* ------------------------------------------------------------------------- */
/* Helpers                                                                   */
/* ------------------------------------------------------------------------- */

RangeVar *
quel_resolve_tuple_var(const char *tvname, int location)
{
	const char *rel;
	RangeVar   *rv;

	rel = quel_rangetab_lookup(tvname);
	if (rel == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("QUEL tuple variable \"%s\" is not bound at character %d",
						tvname, location + 1),
				 errhint("Issue a RANGE OF %s IS <relation> first.",
						 tvname)));
	}
	rv = makeRangeVar(NULL, pstrdup(rel), location);
	rv->alias = makeAlias(pstrdup(tvname), NIL);
	return rv;
}

Node *
quel_make_column_ref(const char *tvname, const char *colname, int location)
{
	ColumnRef  *cref = makeNode(ColumnRef);

	cref->fields = list_make2(makeString(pstrdup(tvname)),
							  makeString(pstrdup(colname)));
	cref->location = location;
	return (Node *) cref;
}

/*
 * walk_target_for_tvars
 *	  Recursively walk a target-list expression collecting tuple-var
 *	  references.  Used to compute the implied FROM clause for
 *	  RETRIEVE statements where Berkeley QUEL omits explicit FROM.
 */
static void
walk_node_for_tvars(Node *node, List **out)
{
	if (node == NULL)
		return;

	if (IsA(node, ColumnRef))
	{
		ColumnRef  *cref = (ColumnRef *) node;
		List	   *fields = cref->fields;

		if (list_length(fields) >= 2)
		{
			Node	   *first = linitial(fields);

			if (IsA(first, String))
			{
				const char *tvname = strVal(first);
				ListCell   *lc;
				bool		seen = false;

				foreach(lc, *out)
				{
					if (strcmp(strVal(lfirst(lc)), tvname) == 0)
					{
						seen = true;
						break;
					}
				}
				if (!seen)
					*out = lappend(*out, makeString(pstrdup(tvname)));
			}
		}
	}
	else if (IsA(node, A_Expr))
	{
		A_Expr	   *e = (A_Expr *) node;

		walk_node_for_tvars(e->lexpr, out);
		walk_node_for_tvars(e->rexpr, out);
	}
	else if (IsA(node, ResTarget))
	{
		walk_node_for_tvars(((ResTarget *) node)->val, out);
	}
	else if (IsA(node, List))
	{
		ListCell   *lc;

		foreach(lc, (List *) node)
			walk_node_for_tvars(lfirst(lc), out);
	}
}

List *
quel_implied_from_clause(List *target_list, Node *where_clause)
{
	List	   *seen_tvars = NIL;
	List	   *result = NIL;
	ListCell   *lc;

	walk_node_for_tvars((Node *) target_list, &seen_tvars);
	walk_node_for_tvars(where_clause, &seen_tvars);

	foreach(lc, seen_tvars)
	{
		const char *tvname = strVal(lfirst(lc));
		const char *rel = quel_rangetab_lookup(tvname);
		RangeVar   *rv;

		if (rel == NULL)
			continue;			/* unresolved; analyzer will catch */

		rv = makeRangeVar(NULL, pstrdup(rel), -1);
		rv->alias = makeAlias(pstrdup(tvname), NIL);
		result = lappend(result, rv);
	}

	return result;
}

/* ------------------------------------------------------------------------- */
/* Reduce-callback implementations                                           */
/*                                                                           */
/* These are sketches.  Each one needs the precise rule shape that           */
/* contrib/quel's quel.c declares; the values arriving via rhs_values        */
/* have types that match the rule's RHS symbols.  The current 12-rule        */
/* contrib/quel registers minimal RHS forms; full QUEL needs the rules       */
/* in .agent/notes/quel-full-implementation-plan.md to be added before       */
/* these builders fire end-to-end.                                           */
/*                                                                           */
/* For now they are stubs that build the right SHAPE of node but pull        */
/* concrete values from a TODO.  When the rule shapes are finalized          */
/* (Phase A of the plan), these stubs become full implementations.           */
/* ------------------------------------------------------------------------- */

void
quel_apply_range(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	const char *tvname;
	const char *relation;
	const core_YYSTYPE *tv_slot;
	const core_YYSTYPE *rel_slot;

	if (nrhs < 5)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("QUEL RANGE: expected 5 RHS symbols, got %d", nrhs)));

	/*
	 * RHS shape per quel.c: K_QUEL_RANGE K_QUEL_OF IDENT K_QUEL_IS IDENT [0]
	 * [1]       [2]   [3]       [4]
	 *
	 * IDENT slots carry core_YYSTYPE union; the lowercased string sits in the
	 * .str member.
	 */
	tv_slot = (const core_YYSTYPE *) rhs_values[2];
	rel_slot = (const core_YYSTYPE *) rhs_values[4];
	tvname = tv_slot ? tv_slot->str : NULL;
	relation = rel_slot ? rel_slot->str : NULL;

	if (tvname == NULL || relation == NULL)
	{
		ereport(WARNING,
				(errmsg("QUEL RANGE: missing tuple-var or relation "
						"name (tv=%p rel=%p)",
						tv_slot, rel_slot)));
		return;
	}

	quel_rangetab_set(tvname, relation);

	ereport(NOTICE,
			(errmsg("QUEL RANGE: %s is now bound to %s",
					tvname, relation)));
	(void) rhs_locs;
}

Node *
quel_build_retrieve(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	SelectStmt *stmt = makeNode(SelectStmt);

	(void) rhs_values;
	(void) rhs_locs;

	/*
	 * Phase B work: extract target list, optional INTO clause, UNIQUE flag,
	 * optional FROM, WHERE, and BY (sort) clauses from rhs_values.  Build the
	 * SelectStmt.
	 *
	 * For this initial scaffold, return an empty SelectStmt that select *
	 * from a placeholder relation, so the reduce path is exercised end-to-end
	 * without crashing.  When the full grammar is written, this body becomes:
	 *
	 * stmt->targetList   = extract_target_list(rhs_values[N]);
	 * stmt->intoClause   = extract_into_clause(rhs_values[M]);
	 * stmt->distinctClause = extract_unique(rhs_values[K]); stmt->fromClause
	 * = quel_implied_from_clause(...) ?? extract_from(rhs_values[F]);
	 * stmt->whereClause  = extract_where(rhs_values[W]); stmt->sortClause   =
	 * extract_by_clause(rhs_values[B]);
	 */

	if (nrhs > 0)
		stmt->targetList = NIL;
	stmt->fromClause = NIL;
	stmt->whereClause = NULL;
	stmt->sortClause = NIL;

	return (Node *) stmt;
}

Node *
quel_build_replace(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	UpdateStmt *stmt = makeNode(UpdateStmt);

	(void) rhs_values;
	(void) rhs_locs;
	(void) nrhs;

	/*
	 * Phase B work: build UpdateStmt: stmt->relation    =
	 * quel_resolve_tuple_var(tvname, loc); stmt->targetList  =
	 * extract_set_clauses(rhs_values[T]); stmt->whereClause =
	 * extract_where(rhs_values[W]);
	 */
	return (Node *) stmt;
}

Node *
quel_build_append(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	InsertStmt *stmt = makeNode(InsertStmt);

	(void) rhs_values;
	(void) rhs_locs;
	(void) nrhs;
	return (Node *) stmt;
}

Node *
quel_build_delete(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	DeleteStmt *stmt = makeNode(DeleteStmt);

	(void) rhs_values;
	(void) rhs_locs;
	(void) nrhs;
	return (Node *) stmt;
}

Node *
quel_build_create(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	CreateStmt *stmt = makeNode(CreateStmt);

	(void) rhs_values;
	(void) rhs_locs;
	(void) nrhs;
	return (Node *) stmt;
}

Node *
quel_build_destroy(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	DropStmt   *stmt = makeNode(DropStmt);

	stmt->removeType = OBJECT_TABLE;
	stmt->behavior = DROP_RESTRICT;
	(void) rhs_values;
	(void) rhs_locs;
	(void) nrhs;
	return (Node *) stmt;
}

Node *
quel_build_copy(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	CopyStmt   *stmt = makeNode(CopyStmt);

	(void) rhs_values;
	(void) rhs_locs;
	(void) nrhs;
	return (Node *) stmt;
}

Node *
quel_build_define_view(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	ViewStmt   *stmt = makeNode(ViewStmt);

	(void) rhs_values;
	(void) rhs_locs;
	(void) nrhs;
	return (Node *) stmt;
}

Node *
quel_build_remove_view(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	DropStmt   *stmt = makeNode(DropStmt);

	stmt->removeType = OBJECT_VIEW;
	(void) rhs_values;
	(void) rhs_locs;
	(void) nrhs;
	return (Node *) stmt;
}

Node *
quel_build_index(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	IndexStmt  *stmt = makeNode(IndexStmt);

	(void) rhs_values;
	(void) rhs_locs;
	(void) nrhs;
	return (Node *) stmt;
}

Node *
quel_build_help(const void *const *rhs_values, const int *rhs_locs, int nrhs)
{
	(void) rhs_values;
	(void) rhs_locs;
	(void) nrhs;

	ereport(NOTICE,
			(errmsg("QUEL HELP: documentation lookup not yet wired"),
			 errhint("Berkeley POSTGRES Reference Manual at "
					 "http://db.cs.berkeley.edu/postgres.html")));

	return NULL;
}

/* ------------------------------------------------------------------------- */
/* Phase B builders -- construct PG parse-tree nodes from rhs_values         */
/* ------------------------------------------------------------------------- */

/*
 * quel_build_attr_simple: quel_attr ::= IDENT or bare_label_keyword.
 *
 * Single-element ColumnRef.  rhs_values[0] points at the token slot
 * which for IDENT is val.str, for bare_label_keyword is whatever the
 * base grammar's bare_label_keyword rule assigned (a char * to the
 * canonical keyword spelling).
 *
 * Both bare_label_keyword and IDENT carry a String * via the same
 * core_YYSTYPE.str slot in the QUEL extension's view, so we can
 * extract identically.
 */
Node *
quel_build_attr_simple(const void *const *rhs_values, const int *rhs_locs,
					   int nrhs)
{
	ColumnRef  *cref;
	const core_YYSTYPE *slot;
	const char *colname;

	Assert(nrhs == 1);
	slot = (const core_YYSTYPE *) rhs_values[0];
	colname = slot ? slot->str : NULL;

	if (colname == NULL)
		return NULL;

	cref = makeNode(ColumnRef);
	cref->fields = list_make1(makeString(pstrdup(colname)));
	cref->location = rhs_locs[0];
	return (Node *) cref;
}

/*
 * quel_build_attr_qualified: quel_attr ::= IDENT DOT IDENT (or
 * IDENT DOT bare_label_keyword).  Two-part column reference.
 */
Node *
quel_build_attr_qualified(const void *const *rhs_values,
						  const int *rhs_locs, int nrhs)
{
	ColumnRef  *cref;
	const core_YYSTYPE *tv_slot;
	const core_YYSTYPE *col_slot;
	const char *tvname;
	const char *colname;

	Assert(nrhs == 3);
	tv_slot = (const core_YYSTYPE *) rhs_values[0];
	/* rhs_values[1] is the DOT token -- ignore. */
	col_slot = (const core_YYSTYPE *) rhs_values[2];
	tvname = tv_slot ? tv_slot->str : NULL;
	colname = col_slot ? col_slot->str : NULL;

	if (tvname == NULL || colname == NULL)
		return NULL;

	cref = makeNode(ColumnRef);
	cref->fields = list_make2(makeString(pstrdup(tvname)),
							  makeString(pstrdup(colname)));
	cref->location = rhs_locs[0];
	return (Node *) cref;
}

/*
 * quel_build_attr_list_single: quel_attr_list ::= quel_attr.
 * Wrap the single attr in a list.
 */
List *
quel_build_attr_list_single(const void *const *rhs_values,
							const int *rhs_locs, int nrhs)
{
	Node	   *attr;

	Assert(nrhs == 1);
	attr = *(Node *const *) rhs_values[0];

	if (attr == NULL)
		return NIL;
	return list_make1(attr);
}

/*
 * quel_build_attr_list_cons: quel_attr_list ::= quel_attr_list COMMA
 * quel_attr.  Append the new attr to the existing list.
 */
List *
quel_build_attr_list_cons(const void *const *rhs_values,
						  const int *rhs_locs, int nrhs)
{
	List	   *prev;
	Node	   *attr;

	Assert(nrhs == 3);
	prev = *(List *const *) rhs_values[0];
	/* rhs_values[1] is COMMA -- ignore. */
	attr = *(Node *const *) rhs_values[2];

	if (attr == NULL)
		return prev;
	return lappend(prev, attr);
}

/*
 * quel_make_resTarget_list: wrap a list of ColumnRef nodes as ResTarget
 * entries so they can serve as a SelectStmt::targetList.
 */
static List *
quel_make_resTarget_list(List *colrefs)
{
	List	   *targets = NIL;
	ListCell   *lc;

	foreach(lc, colrefs)
	{
		Node	   *col = (Node *) lfirst(lc);
		ResTarget  *rt = makeNode(ResTarget);

		rt->name = NULL;
		rt->indirection = NIL;
		rt->val = col;
		rt->location = -1;
		targets = lappend(targets, rt);
	}
	return targets;
}

/*
 * quel_synthesize_from: walk the rangetab and build a List of RangeVar
 * nodes for every bound tuple variable.  Berkeley QUEL omits the FROM
 * clause; we synthesise it from session state.  We prune to ONLY the
 * tuple variables that actually appear in the target_list or where
 * clause -- otherwise an unreferenced bound tuple var (a common
 * case where multiple RANGE statements have been issued in a
 * session) joins as a CARTESIAN PRODUCT and the plan blows up.
 *
 * Implementation: walk target_list + where_clause for ColumnRef nodes
 * whose first field is a registered tuple-variable name.  Build the
 * RangeVar list from the matching tuple-vars only.
 */
static List *
quel_synthesize_from(List *target_list, Node *where_clause)
{
	List	   *seen_tvars = NIL;
	List	   *from = NIL;
	ListCell   *lc;

	walk_node_for_tvars((Node *) target_list, &seen_tvars);
	walk_node_for_tvars(where_clause, &seen_tvars);

	foreach(lc, seen_tvars)
	{
		const char *tvname = strVal(lfirst(lc));
		const char *backing = quel_rangetab_lookup(tvname);
		RangeVar   *rv;

		if (backing == NULL)
			continue;
		rv = makeRangeVar(NULL, pstrdup(backing), -1);
		rv->alias = makeAlias(pstrdup(tvname), NIL);
		from = lappend(from, rv);
	}
	return from;
}

/*
 * quel_build_retrieve_simple: retrieve (attr_list).
 *
 * Build a SelectStmt with target list = attr_list, FROM clause
 * synthesised from the session's tuple-variable table.
 */
Node *
quel_build_retrieve_simple(const void *const *rhs_values,
						   const int *rhs_locs, int nrhs)
{
	SelectStmt *sel;
	List	   *attr_list;
	List	   *target_list;

	Assert(nrhs == 4);			/* RETRIEVE LPAREN list RPAREN */
	attr_list = *(List *const *) rhs_values[2];

	target_list = quel_make_resTarget_list(attr_list);
	sel = makeNode(SelectStmt);
	sel->targetList = target_list;
	sel->fromClause = quel_synthesize_from(target_list, NULL);
	sel->whereClause = NULL;
	sel->op = SETOP_NONE;
	return (Node *) sel;
}

/*
 * quel_build_retrieve_where: retrieve (attr_list) WHERE a_expr.
 *
 * Same as retrieve_simple but with whereClause populated from the
 * a_expr slot.  a_expr's value is already a Node * (the base
 * grammar's a_expr type) so we just thread it through.
 */
Node *
quel_build_retrieve_where(const void *const *rhs_values,
						  const int *rhs_locs, int nrhs)
{
	SelectStmt *sel;
	List	   *attr_list;
	Node	   *whereClause;
	List	   *target_list;

	Assert(nrhs == 6);			/* RETRIEVE ( list ) WHERE expr */
	attr_list = *(List *const *) rhs_values[2];
	whereClause = *(Node *const *) rhs_values[5];

	target_list = quel_make_resTarget_list(attr_list);
	sel = makeNode(SelectStmt);
	sel->targetList = target_list;
	sel->fromClause = quel_synthesize_from(target_list, whereClause);
	sel->whereClause = whereClause;
	sel->op = SETOP_NONE;
	return (Node *) sel;
}

/*
 * quel_resolve_target_relation: turn the IDENT in a REPLACE/APPEND/
 * DELETE statement into a RangeVar.  The IDENT may name either a
 * tuple variable bound by RANGE (resolves to its backing relation)
 * or a relation directly.  Berkeley QUEL doesn't distinguish; we
 * look up first via rangetab, fall back to treating IDENT as a
 * direct relation name.
 */
static RangeVar *
quel_resolve_target_relation(const core_YYSTYPE *slot, int location)
{
	const char *name;
	const char *backing;
	RangeVar   *rv;

	name = slot ? slot->str : NULL;
	if (name == NULL)
		return NULL;

	backing = quel_rangetab_lookup(name);
	if (backing != NULL)
	{
		/*
		 * tuple-var binding -- use the backing relation, alias as the
		 * tuple-var name so the WHERE clause's references resolve.
		 */
		rv = makeRangeVar(NULL, pstrdup(backing), location);
		rv->alias = makeAlias(pstrdup(name), NIL);
	}
	else
	{
		/* direct relation reference. */
		rv = makeRangeVar(NULL, pstrdup(name), location);
	}
	return rv;
}

/*
 * quel_build_replace_simple: replace IDENT (set_clause_list).
 *
 * Build an UpdateStmt whose targetList comes directly from the
 * base grammar's set_clause_list (already a List<ResTarget>).
 */
Node *
quel_build_replace_simple(const void *const *rhs_values,
						  const int *rhs_locs, int nrhs)
{
	UpdateStmt *upd;
	const core_YYSTYPE *target_slot;
	List	   *set_clauses;

	Assert(nrhs == 5);			/* REPLACE IDENT ( set_clause_list ) */
	target_slot = (const core_YYSTYPE *) rhs_values[1];
	set_clauses = *(List *const *) rhs_values[3];

	upd = makeNode(UpdateStmt);
	upd->relation = quel_resolve_target_relation(target_slot, rhs_locs[1]);
	upd->targetList = set_clauses;
	upd->whereClause = NULL;
	upd->fromClause = NIL;
	return (Node *) upd;
}

/*
 * quel_build_replace_where: replace IDENT (set_clause_list)
 *                            WHERE a_expr.
 */
Node *
quel_build_replace_where(const void *const *rhs_values,
						 const int *rhs_locs, int nrhs)
{
	UpdateStmt *upd;
	const core_YYSTYPE *target_slot;
	List	   *set_clauses;
	Node	   *whereClause;

	Assert(nrhs == 7);			/* REPLACE IDENT ( list ) WHERE expr */
	target_slot = (const core_YYSTYPE *) rhs_values[1];
	set_clauses = *(List *const *) rhs_values[3];
	whereClause = *(Node *const *) rhs_values[6];

	upd = makeNode(UpdateStmt);
	upd->relation = quel_resolve_target_relation(target_slot, rhs_locs[1]);
	upd->targetList = set_clauses;
	upd->whereClause = whereClause;
	upd->fromClause = NIL;
	return (Node *) upd;
}

/*
 * quel_build_append_full: append to IDENT (set_clause_list).
 *
 * Berkeley QUEL's APPEND uses 'name = value' pairs in parens, like
 * MySQL's INSERT INTO r SET name='alice', salary=5000 syntax.  PG's
 * InsertStmt expects (cols, valueList) instead.  We split the
 * set_clause_list into two parallel lists:
 *
 *   cols       = ResTarget list with each .name set, .val unused
 *   valueList  = a SelectStmt with a values_clause containing the
 *                expressions
 *
 * One row of values; matches the single-tuple INSERT shape.
 */
Node *
quel_build_append_full(const void *const *rhs_values,
					   const int *rhs_locs, int nrhs)
{
	InsertStmt *ins;
	SelectStmt *valSel;
	const core_YYSTYPE *target_slot;
	List	   *set_clauses;
	List	   *cols = NIL;
	List	   *valExprs = NIL;
	ListCell   *lc;

	Assert(nrhs == 6);			/* APPEND TO IDENT ( set_clause_list ) */
	target_slot = (const core_YYSTYPE *) rhs_values[2];
	set_clauses = *(List *const *) rhs_values[4];

	foreach(lc, set_clauses)
	{
		ResTarget  *src = lfirst_node(ResTarget, lc);
		ResTarget  *colTarget;

		/* Column-name part. */
		colTarget = makeNode(ResTarget);
		colTarget->name = src->name;
		colTarget->indirection = NIL;
		colTarget->val = NULL;
		colTarget->location = src->location;
		cols = lappend(cols, colTarget);

		/* Expression part. */
		valExprs = lappend(valExprs, src->val);
	}

	valSel = makeNode(SelectStmt);
	valSel->valuesLists = list_make1(valExprs);
	valSel->op = SETOP_NONE;

	ins = makeNode(InsertStmt);
	ins->relation = quel_resolve_target_relation(target_slot, rhs_locs[2]);
	ins->cols = cols;
	ins->selectStmt = (Node *) valSel;
	ins->override = OVERRIDING_NOT_SET;
	return (Node *) ins;
}

/*
 * quel_build_delete_simple: delete_quel IDENT.
 */
Node *
quel_build_delete_simple(const void *const *rhs_values,
						 const int *rhs_locs, int nrhs)
{
	DeleteStmt *del;
	const core_YYSTYPE *target_slot;

	Assert(nrhs == 2);			/* DELETE_QUEL IDENT */
	target_slot = (const core_YYSTYPE *) rhs_values[1];

	del = makeNode(DeleteStmt);
	del->relation = quel_resolve_target_relation(target_slot, rhs_locs[1]);
	del->whereClause = NULL;
	return (Node *) del;
}

/*
 * quel_build_delete_where: delete_quel IDENT WHERE a_expr.
 */
Node *
quel_build_delete_where(const void *const *rhs_values,
						const int *rhs_locs, int nrhs)
{
	DeleteStmt *del;
	const core_YYSTYPE *target_slot;
	Node	   *whereClause;

	Assert(nrhs == 4);			/* DELETE_QUEL IDENT WHERE a_expr */
	target_slot = (const core_YYSTYPE *) rhs_values[1];
	whereClause = *(Node *const *) rhs_values[3];

	del = makeNode(DeleteStmt);
	del->relation = quel_resolve_target_relation(target_slot, rhs_locs[1]);
	del->whereClause = whereClause;
	return (Node *) del;
}

/*
 * quel_build_retrieve_by: retrieve (attr_list) BY sortby_list.
 *
 * SelectStmt with sortClause populated from the base grammar's
 * sortby_list (already a List<SortBy>).
 */
Node *
quel_build_retrieve_by(const void *const *rhs_values,
					   const int *rhs_locs, int nrhs)
{
	SelectStmt *sel;
	List	   *attr_list;
	List	   *sortClause;
	List	   *target_list;

	Assert(nrhs == 6);			/* RETRIEVE ( list ) BY sortby_list */
	attr_list = *(List *const *) rhs_values[2];
	sortClause = *(List *const *) rhs_values[5];

	target_list = quel_make_resTarget_list(attr_list);
	sel = makeNode(SelectStmt);
	sel->targetList = target_list;
	sel->fromClause = quel_synthesize_from(target_list, NULL);
	sel->whereClause = NULL;
	sel->sortClause = sortClause;
	sel->op = SETOP_NONE;
	return (Node *) sel;
}

/*
 * quel_build_retrieve_where_by: retrieve (attr_list) WHERE a_expr
 *                                BY sortby_list.
 */
Node *
quel_build_retrieve_where_by(const void *const *rhs_values,
							 const int *rhs_locs, int nrhs)
{
	SelectStmt *sel;
	List	   *attr_list;
	Node	   *whereClause;
	List	   *sortClause;
	List	   *target_list;

	Assert(nrhs == 8);			/* RETRIEVE ( list ) WHERE expr BY sort */
	attr_list = *(List *const *) rhs_values[2];
	whereClause = *(Node *const *) rhs_values[5];
	sortClause = *(List *const *) rhs_values[7];

	target_list = quel_make_resTarget_list(attr_list);
	sel = makeNode(SelectStmt);
	sel->targetList = target_list;
	sel->fromClause = quel_synthesize_from(target_list, whereClause);
	sel->whereClause = whereClause;
	sel->sortClause = sortClause;
	sel->op = SETOP_NONE;
	return (Node *) sel;
}
