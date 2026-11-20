/*-------------------------------------------------------------------------
 *
 * pg_fts_customscan.c
 *	  CustomScan providers for pg_fts:
 *	    1. COUNT pushdown -- answer  SELECT count(*) ... WHERE col @@@ q
 *	       from the bm25 index (VM-based bulk count) instead of a bitmap heap
 *	       scan, ~3x faster on a common term.
 *	    (later stages add a parallel ranked top-k CustomScan)
 *
 * The providers are installed by _PG_init via create_upper_paths_hook (count)
 * and set_rel_pathlist_hook (ranked).  They are strictly additive: a candidate
 * CustomPath is only *added* alongside the normal paths, so if anything about
 * the shape is unsupported we simply add nothing and the planner uses the
 * ordinary plan.  Nothing here changes results -- only the mechanism.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "pg_fts.h"

/* engine entry point implemented in pg_fts_am_scan.c (via pg_fts_am.c) */
extern int64 bm25_count_visible_oid(Oid indexoid, FtsQuery q);

PG_FUNCTION_INFO_V1(pg_fts_customscan_dummy);	/* keeps the file non-empty for old toolchains */
Datum
pg_fts_customscan_dummy(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

/* ---- saved previous hooks (chain, do not clobber) ---- */
static create_upper_paths_hook_type prev_upper_paths_hook = NULL;

/* cached OID of the @@@ (ftsdoc, ftsquery) operator; resolved lazily */
static Oid	fts_match_op = InvalidOid;

/* ===== count-pushdown CustomScan: path/plan/exec ===== */

typedef struct FtsCountScanState
{
	CustomScanState css;
	Oid			indexoid;
	FtsQuery	query;
	bool		done;
} FtsCountScanState;

static Plan *FtsCountPlanCustomPath(PlannerInfo *root, RelOptInfo *rel,
									struct CustomPath *best_path, List *tlist,
									List *clauses, List *custom_plans);
static Node *FtsCountCreateScanState(CustomScan *cscan);
static void FtsCountBeginScan(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *FtsCountExecScan(CustomScanState *node);
static void FtsCountEndScan(CustomScanState *node);
static void FtsCountReScan(CustomScanState *node);

static const CustomPathMethods fts_count_path_methods = {
	.CustomName = "FtsCount",
	.PlanCustomPath = FtsCountPlanCustomPath,
};

static const CustomScanMethods fts_count_scan_methods = {
	.CustomName = "FtsCount",
	.CreateCustomScanState = FtsCountCreateScanState,
};

static const CustomExecMethods fts_count_exec_methods = {
	.CustomName = "FtsCount",
	.BeginCustomScan = FtsCountBeginScan,
	.ExecCustomScan = FtsCountExecScan,
	.EndCustomScan = FtsCountEndScan,
	.ReScanCustomScan = FtsCountReScan,
};

/*
 * Resolve the @@@ operator OID in the extension's schema.  Returns InvalidOid
 * if pg_fts's SQL objects are not installed in this database's search path
 * (then the pushdown simply never triggers).
 */
static Oid
fts_lookup_match_op(void)
{
	if (OidIsValid(fts_match_op))
		return fts_match_op;
	/* @@@ (ftsdoc, ftsquery) */
	fts_match_op = OpernameGetOprid(list_make1(makeString("@@@")),
									TypenameGetTypid("ftsdoc"),
									TypenameGetTypid("ftsquery"));
	return fts_match_op;
}

/*
 * If the RestrictInfo list contains exactly one clause of the form
 *   <indexable expr> @@@ <FtsQuery Const>
 * covered by a bm25 index on `rel`, return the index OID and the query Const;
 * else InvalidOid.
 */
static Oid
fts_find_pushdown_index(PlannerInfo *root, RelOptInfo *rel,
						List *baserestrictinfo, FtsQuery *query_out)
{
	Oid			matchop = fts_lookup_match_op();
	RangeTblEntry *rte;
	Relation	heap;
	ListCell   *lc;
	OpExpr	   *matchclause = NULL;
	int			nquals = 0;

	if (!OidIsValid(matchop))
		return InvalidOid;
	if (rel->reloptkind != RELOPT_BASEREL || rel->rtekind != RTE_RELATION)
		return InvalidOid;

	/* need exactly one qual, and it must be the @@@ operator */
	foreach(lc, baserestrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
		OpExpr	   *op;

		nquals++;
		if (!IsA(ri->clause, OpExpr))
			return InvalidOid;
		op = (OpExpr *) ri->clause;
		if (op->opno != matchop || list_length(op->args) != 2)
			return InvalidOid;
		matchclause = op;
	}
	if (nquals != 1 || matchclause == NULL)
		return InvalidOid;

	/* the right-hand side must be a plan-time constant FtsQuery */
	{
		Node	   *rhs = (Node *) lsecond(matchclause->args);

		if (!IsA(rhs, Const) || ((Const *) rhs)->constisnull)
			return InvalidOid;
		*query_out = (FtsQuery) DatumGetPointer(((Const *) rhs)->constvalue);
	}

	/* find a bm25 index on this rel whose expression matches the LHS */
	rte = planner_rt_fetch(rel->relid, root);
	if (rte->rtekind != RTE_RELATION)
		return InvalidOid;
	heap = table_open(rte->relid, AccessShareLock);
	{
		List	   *indexoidlist = RelationGetIndexList(heap);
		ListCell   *ic;
		Oid			found = InvalidOid;
		Node	   *lhs = (Node *) linitial(matchclause->args);

		foreach(ic, indexoidlist)
		{
			Oid			indexoid = lfirst_oid(ic);
			Relation	ind = index_open(indexoid, AccessShareLock);

			if (ind->rd_rel->relam == get_index_am_oid("bm25", true) &&
				ind->rd_indexprs != NIL &&
				equal(linitial(ind->rd_indexprs), lhs))
				found = indexoid;
			index_close(ind, AccessShareLock);
			if (OidIsValid(found))
				break;
		}
		list_free(indexoidlist);
		table_close(heap, AccessShareLock);
		return found;
	}
}

/*
 * create_upper_paths_hook: at the GROUP/AGG stage, if the query is a bare
 * COUNT(*) over a single base rel whose only qual is `col @@@ q` with a bm25
 * index, add a CustomScan path that answers the count from the index.
 */
static void
fts_create_upper_paths(PlannerInfo *root, UpperRelationKind stage,
					   RelOptInfo *input_rel, RelOptInfo *output_rel,
					   void *extra)
{
	Query	   *parse = root->parse;
	RelOptInfo *baserel;
	Oid			indexoid;
	FtsQuery	query;
	CustomPath *cpath;

	if (prev_upper_paths_hook)
		prev_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	if (stage != UPPERREL_GROUP_AGG)
		return;
	/* bare aggregate: exactly one COUNT(*), no GROUP BY / HAVING / DISTINCT / window / set-op */
	if (parse->groupClause || parse->groupingSets || parse->havingQual ||
		parse->distinctClause || parse->hasWindowFuncs || parse->setOperations ||
		parse->hasDistinctOn || list_length(parse->targetList) != 1)
		return;
	if (list_length(parse->rtable) != 1)
		return;
	{
		TargetEntry *te = (TargetEntry *) linitial(parse->targetList);
		Aggref	   *agg;

		if (!IsA(te->expr, Aggref))
			return;
		agg = (Aggref *) te->expr;
		/* count(*) : COUNT with no args, no FILTER, no DISTINCT, no ORDER BY */
		if (agg->aggfnoid != F_COUNT_ ||
			agg->args != NIL || agg->aggfilter != NULL ||
			agg->aggdistinct != NIL || agg->aggorder != NIL)
			return;
	}

	/* the single base rel */
	if (bms_num_members(input_rel->relids) != 1)
		return;
	baserel = find_base_rel(root, bms_singleton_member(input_rel->relids));
	indexoid = fts_find_pushdown_index(root, baserel, baserel->baserestrictinfo,
									   &query);
	if (!OidIsValid(indexoid))
		return;

	/* build the CustomPath -- rows=1 */
	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = output_rel;
	cpath->path.pathtarget = output_rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.rows = 1;
	cpath->path.startup_cost = baserel->pages;	/* rough: one index bulk-count */
	cpath->path.total_cost = baserel->pages + 1;
	cpath->flags = 0;
	cpath->custom_paths = NIL;
	cpath->custom_private = list_make2(makeInteger((int) indexoid),
									   makeConst(INTERNALOID, -1, InvalidOid,
												 sizeof(void *),
												 PointerGetDatum(query),
												 false, false));
	cpath->methods = &fts_count_path_methods;
	add_path(output_rel, (Path *) cpath);
}

static Plan *
FtsCountPlanCustomPath(PlannerInfo *root, RelOptInfo *rel,
					   struct CustomPath *best_path, List *tlist,
					   List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = NIL;
	cscan->scan.scanrelid = 0;	/* no base rel scanned at exec time */
	cscan->custom_scan_tlist = tlist;
	cscan->custom_private = best_path->custom_private;
	cscan->methods = &fts_count_scan_methods;
	return &cscan->scan.plan;
}

static Node *
FtsCountCreateScanState(CustomScan *cscan)
{
	FtsCountScanState *st = (FtsCountScanState *) newNode(sizeof(FtsCountScanState),
														 T_CustomScanState);
	Const	   *qc;

	st->css.methods = &fts_count_exec_methods;
	st->indexoid = (Oid) intVal(linitial(cscan->custom_private));
	qc = (Const *) lsecond(cscan->custom_private);
	st->query = (FtsQuery) DatumGetPointer(qc->constvalue);
	st->done = false;
	return (Node *) st;
}

static void
FtsCountBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	/* nothing to set up beyond the tuple slot the executor made */
}

static TupleTableSlot *
FtsCountExecScan(CustomScanState *node)
{
	FtsCountScanState *st = (FtsCountScanState *) node;
	TupleTableSlot *slot = node->ss.ps.ps_ResultTupleSlot;
	int64		c;

	if (st->done)
		return NULL;
	st->done = true;

	c = bm25_count_visible_oid(st->indexoid, st->query);

	ExecClearTuple(slot);
	slot->tts_values[0] = Int64GetDatum(c);
	slot->tts_isnull[0] = false;
	ExecStoreVirtualTuple(slot);
	return slot;
}

static void
FtsCountEndScan(CustomScanState *node)
{
}

static void
FtsCountReScan(CustomScanState *node)
{
	((FtsCountScanState *) node)->done = false;
}

/* ===== module init ===== */

void		_PG_init(void);

void
_PG_init(void)
{
	RegisterCustomScanMethods(&fts_count_scan_methods);

	prev_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = fts_create_upper_paths;
}
