/*-------------------------------------------------------------------------
 *
 * matview_ivm.c
 *	  incremental maintenance of materialized views
 *
 * A materialized view created WITH (incremental_view_maintenance) is
 * compiled once, when it is created: its query is checked against what
 * incremental maintenance supports, and a maintenance program (an
 * IvmProgram node) is stored in pg_matview_ivm.  Later stages read that
 * program instead of analysing the query again.
 *
 * Supported so far: a single plain table, an optional WHERE clause, GROUP
 * BY plain columns of that table (or no GROUP BY), and a target list of
 * grouping columns and aggregates from a fixed list.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/matview_ivm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/table.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_matview_ivm.h"
#include "commands/defrem.h"
#include "commands/matview_ivm.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/syscache.h"


static Query *rewrite_view_query(Query *viewQuery);
static Oid	check_view_query(Query *query);
static IvmProgram * build_program(Query *query, Oid baserel);
static IvmAgg * build_agg(Aggref *aggref);
static Oid	get_agg_sortop(Oid aggfnoid);
static void check_group_key_type(Oid baserel, AttrNumber attno);
static void store_program(Oid matviewOid, IvmProgram * program);
static void set_relhasivm(Oid relid);
static void ivm_unsupported(const char *fmt, ...) pg_attribute_printf(1, 2);


/*
 * Raise the error for a query that cannot be incrementally maintained,
 * with the given detail.
 */
static void
ivm_unsupported(const char *fmt, ...)
{
	va_list		args;
	char		detail[1024];

	va_start(args, fmt);
	vsnprintf(detail, sizeof(detail), fmt, args);
	va_end(args);

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("materialized view cannot be incrementally maintained"),
			 errdetail_internal("%s", detail)));
}

/*
 * CompileIncrementalMatView
 *		Check the query of a new incrementally maintained materialized view,
 *		build its maintenance program and store it.
 *
 * viewQuery is the analysed, unrewritten query stored as the view's _RETURN
 * rule.  Called while the view is being created, before it is populated.
 */
void
CompileIncrementalMatView(Oid matviewOid, Query *viewQuery)
{
	Query	   *query;
	Oid			baserel;
	IvmProgram *program;
	ObjectAddress myself;
	AclResult	aclresult;

	query = rewrite_view_query(viewQuery);
	baserel = check_view_query(query);

	/*
	 * Maintenance will run inside other users' transactions on the base
	 * table, and an error in it aborts them, so creating the view needs the
	 * MAINTAIN privilege on (or ownership of) the base table.
	 */
	aclresult = pg_class_aclcheck(baserel, GetUserId(), ACL_MAINTAIN);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, get_relkind_objtype(RELKIND_RELATION),
					   get_rel_name(baserel));

	/*
	 * Keep the base table's writers out until this transaction commits: one
	 * that started before the view existed would make changes with no capture
	 * set up for them.  The lock must also exclude itself, because we update
	 * the table's pg_class row below and concurrent creators of views on the
	 * same table would otherwise race on it. ShareRowExclusiveLock, as CREATE
	 * TRIGGER takes, does both.
	 */
	LockRelationOid(baserel, ShareRowExclusiveLock);

	program = build_program(query, baserel);

	store_program(matviewOid, program);

	/*
	 * The view's _RETURN rule already depends on everything the query
	 * references; record the same for the program, so that dropping or
	 * replacing what it uses is refused while the program is stored.
	 */
	ObjectAddressSet(myself, RelationRelationId, matviewOid);
	recordDependencyOnExpr(&myself, (Node *) query, NIL, DEPENDENCY_NORMAL);

	set_relhasivm(baserel);
}

/*
 * Run the rewriter over a copy of the view query, so that row security
 * policies and rules of the base table are applied as they would be by
 * REFRESH.
 */
static Query *
rewrite_view_query(Query *viewQuery)
{
	Query	   *copied_query;
	List	   *rewritten;

	copied_query = copyObject(viewQuery);
	AcquireRewriteLocks(copied_query, true, false);
	rewritten = QueryRewrite(copied_query);

	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for CREATE MATERIALIZED VIEW");

	return linitial_node(Query, rewritten);
}

/*
 * Check that a rewritten view query has a shape the compiler supports, and
 * return its base relation.
 */
static Oid
check_view_query(Query *query)
{
	Oid			baserel = InvalidOid;
	int			nrels = 0;
	int			rti = 0;
	ListCell   *lc;

	Assert(query->commandType == CMD_SELECT);

	if (query->setOperations)
		ivm_unsupported("Set operations are not supported.");
	if (query->cteList)
		ivm_unsupported("WITH clauses are not supported.");
	if (query->distinctClause)
		ivm_unsupported(query->hasDistinctOn ?
						"DISTINCT ON is not supported." :
						"DISTINCT is not supported.");
	if (query->havingQual)
		ivm_unsupported("HAVING is not supported.");
	if (query->sortClause)
		ivm_unsupported("ORDER BY is not supported.");
	if (query->limitCount || query->limitOffset)
		ivm_unsupported("LIMIT and OFFSET are not supported.");
	if (query->hasWindowFuncs)
		ivm_unsupported("Window functions are not supported.");
	if (query->groupingSets)
		ivm_unsupported("Grouping sets are not supported.");
	if (query->hasTargetSRFs)
		ivm_unsupported("Set-returning functions are not supported.");
	if (query->hasSubLinks)
		ivm_unsupported("Subqueries are not supported.");
	if (query->hasForUpdate || query->rowMarks)
		ivm_unsupported("FOR UPDATE and FOR SHARE are not supported.");

	foreach(lc, query->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		rti++;
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				if (rte->relkind != RELKIND_RELATION)
					ivm_unsupported("Base relation \"%s\" is not a table.",
									get_rel_name(rte->relid));
				if (rte->tablesample)
					ivm_unsupported("TABLESAMPLE is not supported.");
				if (get_rel_persistence(rte->relid) != RELPERSISTENCE_PERMANENT)
					ivm_unsupported("Base table \"%s\" is not permanent.",
									get_rel_name(rte->relid));
				baserel = rte->relid;
				nrels++;
				break;
			case RTE_GROUP:
				/* the grouping step of a GROUP BY query */
				break;
			case RTE_SUBQUERY:
				if (OidIsValid(rte->relid))
					ivm_unsupported("Views in FROM are not supported, but \"%s\" is a view.",
									get_rel_name(rte->relid));
				ivm_unsupported("Subqueries in FROM are not supported.");
				break;
			case RTE_JOIN:
				ivm_unsupported("Joins are not supported.");
				break;
			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
				ivm_unsupported("Functions in FROM are not supported.");
				break;
			case RTE_VALUES:
				ivm_unsupported("VALUES in FROM is not supported.");
				break;
			default:
				ivm_unsupported("This kind of FROM item is not supported.");
				break;
		}
	}

	if (nrels != 1)
		ivm_unsupported("Incremental maintenance requires exactly one base table.");

	/*
	 * The single base table must be the only item in FROM; with a GROUP BY
	 * the RTE_GROUP entry follows it in the range table.
	 */
	if (list_length(query->jointree->fromlist) != 1 ||
		!IsA(linitial(query->jointree->fromlist), RangeTblRef))
		ivm_unsupported("Incremental maintenance requires exactly one base table.");

	if (contain_mutable_functions((Node *) query))
		ivm_unsupported("Functions in an incrementally maintained materialized view must be immutable.");

	return baserel;
}

/*
 * Return the varno of the base relation in a query that check_view_query
 * accepted.
 */
static int
base_varno(Query *query)
{
	return castNode(RangeTblRef, linitial(query->jointree->fromlist))->rtindex;
}

/*
 * Rewrite Vars of the base relation to varno 1, and reject system columns.
 */
static bool
check_base_vars_walker(Node *node, int *varno)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup != 0 || var->varno != *varno)
			elog(ERROR, "unexpected Var in incrementally maintained materialized view");
		if (var->varattno <= 0)
			ivm_unsupported("System columns and whole-row references are not supported.");
		return false;
	}
	return expression_tree_walker(node, check_base_vars_walker, varno);
}

static Node *
to_base_varno(Node *node, int varno)
{
	node = copyObject(node);
	(void) check_base_vars_walker(node, &varno);
	if (varno != 1)
		ChangeVarNodes(node, varno, 1, 0);
	return node;
}

/*
 * Build the maintenance program of an accepted query.
 */
static IvmProgram *
build_program(Query *query, Oid baserel)
{
	IvmProgram *program = makeNode(IvmProgram);
	int			varno = base_varno(query);
	bool		all_invertible = true;
	ListCell   *lc;

	program->baserel = baserel;
	if (query->jointree->quals)
		program->qual = to_base_varno(query->jointree->quals, varno);

	/* GROUP BY keys must be plain columns of the base table. */
	foreach(lc, query->groupClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);
		Node	   *expr = flatten_group_exprs(NULL, query, (Node *) tle->expr);
		Var		   *var;

		if (!IsA(expr, Var))
			ivm_unsupported("GROUP BY expressions are not supported.");
		var = castNode(Var, to_base_varno(expr, varno));
		check_group_key_type(baserel, var->varattno);
		program->groupcols = lappend_int(program->groupcols, var->varattno);
	}

	/* Each output column is a grouping column or an aggregate. */
	foreach(lc, query->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Node	   *expr = (Node *) tle->expr;

		if (tle->resjunk)
			continue;

		if (tle->ressortgroupref != 0)
		{
			/* a GROUP BY key (ORDER BY was rejected earlier) */
			int			pos = 0;
			ListCell   *gc;

			foreach(gc, query->groupClause)
			{
				pos++;
				if (lfirst_node(SortGroupClause, gc)->tleSortGroupRef ==
					tle->ressortgroupref)
					break;
			}
			if (gc == NULL)
				elog(ERROR, "ORDER/GROUP BY expression not found in list");
			program->viewcols = lappend_int(program->viewcols, pos);
		}
		else if (IsA(expr, Aggref))
		{
			IvmAgg	   *agg = build_agg((Aggref *) expr);

			if (agg->arg)
				agg->arg = (Expr *) to_base_varno((Node *) agg->arg, varno);
			if (!agg->invertible)
				all_invertible = false;
			program->aggs = lappend(program->aggs, agg);
			program->viewcols = lappend_int(program->viewcols,
											-list_length(program->aggs));
		}
		else
		{
			/*
			 * A second output column naming a grouping column carries no
			 * ressortgroupref; match it to the key by expression.
			 */
			Node	   *flat = flatten_group_exprs(NULL, query, expr);
			int			pos = 0;
			ListCell   *gc;

			if (IsA(flat, Var) && ((Var *) flat)->varno == varno)
			{
				foreach(gc, program->groupcols)
				{
					pos++;
					if (lfirst_int(gc) == ((Var *) flat)->varattno)
						break;
				}
				if (gc != NULL)
				{
					program->viewcols = lappend_int(program->viewcols, pos);
					continue;
				}
			}
			ivm_unsupported("Column \"%s\" must be a grouping column or an aggregate.",
							tle->resname);
		}
	}

	program->ivmclass = all_invertible ? IVM_CLASS_SELF_MAINTAINABLE :
		IVM_CLASS_READS_BASE;

	/*
	 * The view's rows are identified by their group key, so every GROUP BY
	 * key must be an output column.
	 */
	for (int i = 1; i <= list_length(program->groupcols); i++)
	{
		if (!list_member_int(program->viewcols, i))
		{
			AttrNumber	attno = list_nth_int(program->groupcols, i - 1);

			ivm_unsupported("Grouping column \"%s\" must be an output column.",
							get_attname(baserel, attno, false));
		}
	}

	return program;
}

/*
 * Check an aggregate call and describe how it is maintained.
 */
static IvmAgg *
build_agg(Aggref *aggref)
{
	IvmAgg	   *agg = makeNode(IvmAgg);
	bool		invertible;

	if (aggref->aggkind != AGGKIND_NORMAL)
		ivm_unsupported("Aggregate function %s is not supported.",
						format_procedure(aggref->aggfnoid));
	if (aggref->aggdistinct)
		ivm_unsupported("DISTINCT in an aggregate is not supported.");
	if (aggref->aggorder)
		ivm_unsupported("ORDER BY in an aggregate is not supported.");
	if (aggref->aggfilter)
		ivm_unsupported("FILTER in an aggregate is not supported.");

	switch (aggref->aggfnoid)
	{
			/* count(*) and count(x) */
		case F_COUNT_:
		case F_COUNT_ANY:
			/* sum and avg with an exact inverse transition function */
		case F_SUM_INT2:
		case F_SUM_INT4:
		case F_SUM_INT8:
		case F_SUM_NUMERIC:
		case F_SUM_INTERVAL:
		case F_AVG_INT2:
		case F_AVG_INT4:
		case F_AVG_INT8:
		case F_AVG_NUMERIC:
		case F_AVG_INTERVAL:
			invertible = true;
			break;

			/*
			 * Aggregates that the planner could compute from an index
			 * ordering (min, max, bool_and, bool_or, every: those with an
			 * aggsortop) are recomputed per group when an input goes away.
			 */
		default:
			if (!OidIsValid(get_agg_sortop(aggref->aggfnoid)))
				ivm_unsupported("Aggregate function %s is not supported.",
								format_procedure(aggref->aggfnoid));
			invertible = false;
			break;
	}

	agg->aggfnoid = aggref->aggfnoid;
	agg->aggtype = aggref->aggtype;
	agg->aggcollid = aggref->aggcollid;
	agg->inputcollid = aggref->inputcollid;
	agg->invertible = invertible;

	if (aggref->args != NIL)
	{
		if (list_length(aggref->args) != 1)
			ivm_unsupported("Aggregate function %s is not supported.",
							format_procedure(aggref->aggfnoid));
		agg->arg = linitial_node(TargetEntry, aggref->args)->expr;
	}

	return agg;
}

/*
 * Return the sort operator of an aggregate (pg_aggregate.aggsortop), or
 * InvalidOid if it has none.
 */
static Oid
get_agg_sortop(Oid aggfnoid)
{
	HeapTuple	tuple;
	Oid			sortop;

	tuple = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for aggregate %u", aggfnoid);
	sortop = ((Form_pg_aggregate) GETSTRUCT(tuple))->aggsortop;
	ReleaseSysCache(tuple);

	return sortop;
}

/*
 * A group key is looked up by value in the view, so equal keys must have
 * identical bytes: the type needs a default btree opclass whose equality is
 * image equality (BTEQUALIMAGE_PROC returns true), as btree deduplication
 * requires.  That excludes numeric, floats, interval, and text under a
 * nondeterministic collation.
 */
static void
check_group_key_type(Oid baserel, AttrNumber attno)
{
	Oid			typid;
	int32		typmod;
	Oid			collid;
	Oid			opclass;
	Oid			opfamily;
	Oid			opcintype;
	Oid			equalimageproc;

	get_atttypetypmodcoll(baserel, attno, &typid, &typmod, &collid);

	opclass = GetDefaultOpClass(typid, BTREE_AM_OID);
	if (OidIsValid(opclass))
	{
		opfamily = get_opclass_family(opclass);
		opcintype = get_opclass_input_type(opclass);
		equalimageproc = get_opfamily_proc(opfamily, opcintype, opcintype,
										   BTEQUALIMAGE_PROC);
		if (OidIsValid(equalimageproc) &&
			DatumGetBool(OidFunctionCall1Coll(equalimageproc, collid,
											  ObjectIdGetDatum(opcintype))))
			return;
	}

	if (OidIsValid(collid) && !get_collation_isdeterministic(collid))
		ivm_unsupported("Grouping column \"%s\" uses nondeterministic collation \"%s\", which cannot be used as an incremental group key.",
						get_attname(baserel, attno, false),
						get_collation_name(collid));
	ivm_unsupported("Grouping column \"%s\" has type %s, which cannot be used as an incremental group key.",
					get_attname(baserel, attno, false),
					format_type_be(typid));
}

/*
 * Insert the pg_matview_ivm row of a new view.
 */
static void
store_program(Oid matviewOid, IvmProgram * program)
{
	Relation	rel;
	HeapTuple	tuple;
	Datum		values[Natts_pg_matview_ivm] = {0};
	bool		nulls[Natts_pg_matview_ivm] = {0};
	char	   *programstr;

	programstr = nodeToString(program);

#ifdef USE_ASSERT_CHECKING
	{
		/* the stored form must read back as the same program */
		Node	   *readback = stringToNode(programstr);

		Assert(equal(readback, program));
		pfree(readback);
	}
#endif

	values[Anum_pg_matview_ivm_ivmrelid - 1] = ObjectIdGetDatum(matviewOid);
	values[Anum_pg_matview_ivm_ivmclass - 1] = Int16GetDatum(program->ivmclass);
	values[Anum_pg_matview_ivm_ivmbaserels - 1] =
		PointerGetDatum(buildoidvector(&program->baserel, 1));
	values[Anum_pg_matview_ivm_ivmprogram - 1] =
		CStringGetTextDatum(programstr);

	rel = table_open(MatViewIvmRelationId, RowExclusiveLock);
	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	CatalogTupleInsert(rel, tuple);
	heap_freetuple(tuple);
	table_close(rel, RowExclusiveLock);

	pfree(programstr);
}

/*
 * Mark a base table as having an incrementally maintained view, and make
 * every backend rebuild its relcache entry so it sees that.
 */
static void
set_relhasivm(Oid relid)
{
	Relation	pg_class;
	HeapTuple	tuple;
	Form_pg_class classForm;

	pg_class = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	classForm = (Form_pg_class) GETSTRUCT(tuple);

	if (!classForm->relhasivm)
	{
		classForm->relhasivm = true;
		CatalogTupleUpdate(pg_class, &tuple->t_self, tuple);
	}
	else
		CacheInvalidateRelcacheByTuple(tuple);

	heap_freetuple(tuple);
	table_close(pg_class, RowExclusiveLock);
}

/*
 * RemoveMatViewIvmEntry
 *		Delete the pg_matview_ivm row of a materialized view being dropped,
 *		if it has one.
 */
void
RemoveMatViewIvmEntry(Oid matviewOid)
{
	Relation	rel;
	HeapTuple	tuple;

	tuple = SearchSysCache1(MATVIEWIVMRELID, ObjectIdGetDatum(matviewOid));
	if (!HeapTupleIsValid(tuple))
		return;

	rel = table_open(MatViewIvmRelationId, RowExclusiveLock);
	CatalogTupleDelete(rel, &tuple->t_self);
	ReleaseSysCache(tuple);
	table_close(rel, RowExclusiveLock);
}
