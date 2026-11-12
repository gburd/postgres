/*-------------------------------------------------------------------------
 *
 * matview_ivm_maintain.c
 *	  applying captured base-table changes to incrementally maintained
 *	  materialized views at commit
 *
 * At commit, for each base table this transaction changed, every
 * incrementally maintained view over it is brought up to date before the
 * commit record is written.  This first version maintains each view by
 * recomputing only the groups the transaction touched: it collects the
 * group keys of the captured rows that pass the view's WHERE clause,
 * deletes the view's rows for those keys, and inserts the rows the view's
 * query now produces for them.  A table changed outside the executor
 * (TRUNCATE) gets its views recomputed entirely.
 *
 * Recomputing reads the base table, so concurrent maintainers of one view
 * must be ordered: each takes ExclusiveLock on the view, held until its
 * commit is visible, and reads with a snapshot taken after the lock.  Plain
 * readers of the view are not blocked.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/matview_ivm_maintain.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "common/int.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_matview_ivm.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/matview_ivm.h"
#include "executor/executor.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_relation.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tuplestore.h"


/* one group key: values of the view's grouping columns */
typedef struct IvmGroupKey
{
	Datum	   *values;
	bool	   *isnull;
} IvmGroupKey;

/* what one view needs from the commit pass */
typedef struct IvmViewWork
{
	Oid			viewoid;
	IvmProgram *program;
	bool		recompute;		/* recompute all of it */
	List	   *keys;			/* list of IvmGroupKey *, without duplicates */
} IvmViewWork;


static IvmProgram *load_program(Oid viewoid);
static void collect_keys(IvmViewWork *work, Relation baserel,
						 Tuplestorestate *rows);
static void maintain_view(IvmViewWork *work);


/*
 * Read the stored maintenance program of a view.
 */
static IvmProgram *
load_program(Oid viewoid)
{
	HeapTuple	tuple;
	Datum		datum;
	char	   *str;
	IvmProgram *program;

	tuple = SearchSysCache1(MATVIEWIVMRELID, ObjectIdGetDatum(viewoid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for incremental view %u", viewoid);
	datum = SysCacheGetAttrNotNull(MATVIEWIVMRELID, tuple,
								   Anum_pg_matview_ivm_ivmprogram);
	str = TextDatumGetCString(datum);
	program = castNode(IvmProgram, stringToNode(str));
	pfree(str);
	ReleaseSysCache(tuple);

	return program;
}

/*
 * Are two group keys equal?  Group key types have image equality (the
 * compiler checked their opclass), so compare bytes.
 */
static bool
keys_equal(IvmGroupKey *a, IvmGroupKey *b, TupleDesc desc, List *groupcols)
{
	int			i = 0;
	ListCell   *lc;

	foreach(lc, groupcols)
	{
		Form_pg_attribute att = TupleDescAttr(desc, lfirst_int(lc) - 1);

		if (a->isnull[i] != b->isnull[i])
			return false;
		if (!a->isnull[i] &&
			!datum_image_eq(a->values[i], b->values[i],
							att->attbyval, att->attlen))
			return false;
		i++;
	}
	return true;
}

/*
 * Add the group keys of the captured rows that pass the view's WHERE
 * clause to work->keys.
 *
 * shortcut: a linear duplicate check per row; use a hash table once the
 * number of affected groups per transaction is large.
 */
static void
collect_keys(IvmViewWork *work, Relation baserel, Tuplestorestate *rows)
{
	TupleDesc	desc = RelationGetDescr(baserel);
	List	   *groupcols = work->program->groupcols;
	int			nkeys = list_length(groupcols);
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	ExprState  *qual = NULL;

	if (rows == NULL)
		return;

	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	if (work->program->qual)
		qual = ExecPrepareQual(make_ands_implicit((Expr *) work->program->qual),
							   estate);
	slot = MakeSingleTupleTableSlot(desc, &TTSOpsMinimalTuple);

	tuplestore_rescan(rows);
	while (tuplestore_gettupleslot(rows, true, false, slot))
	{
		IvmGroupKey *key;
		ListCell   *lc;
		bool		dup = false;
		int			i = 0;

		CHECK_FOR_INTERRUPTS();

		/* rows the WHERE clause filters out do not touch the view */
		if (qual)
		{
			econtext->ecxt_scantuple = slot;
			if (!ExecQual(qual, econtext))
			{
				ResetExprContext(econtext);
				continue;
			}
			ResetExprContext(econtext);
		}

		/* a view without GROUP BY has one group, with the empty key */
		key = palloc_object(IvmGroupKey);
		key->values = palloc_array(Datum, Max(nkeys, 1));
		key->isnull = palloc_array(bool, Max(nkeys, 1));
		slot_getallattrs(slot);
		foreach(lc, groupcols)
		{
			AttrNumber	attno = lfirst_int(lc);
			Form_pg_attribute att = TupleDescAttr(desc, attno - 1);

			key->isnull[i] = slot->tts_isnull[attno - 1];
			key->values[i] = key->isnull[i] ? (Datum) 0 :
				datumCopy(slot->tts_values[attno - 1], att->attbyval,
						  att->attlen);
			i++;
		}

		foreach(lc, work->keys)
		{
			if (keys_equal(lfirst(lc), key, desc, groupcols))
			{
				dup = true;
				break;
			}
		}
		if (!dup)
			work->keys = lappend(work->keys, key);
	}

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
}

/*
 * Return the view's query, rewritten, with an added condition restricting
 * the base table to the given group key (or to nothing extra, if key is
 * NULL).  The condition uses the grouping columns' default btree equality
 * operators, and IS NULL for null key values.
 */
static Query *
view_query_for_key(Relation viewrel, IvmProgram *program,
				   TupleDesc basedesc, IvmGroupKey *key)
{
	RewriteRule *rule;
	Query	   *query;
	List	   *rewritten;
	List	   *conds = NIL;
	int			varno;
	int			i = 0;
	ListCell   *lc;

	rule = viewrel->rd_rules->rules[0];
	query = copyObject(linitial_node(Query, rule->actions));
	AcquireRewriteLocks(query, true, false);
	rewritten = QueryRewrite(query);
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for incremental view maintenance");
	query = linitial_node(Query, rewritten);

	if (key == NULL || program->groupcols == NIL)
		return query;

	varno = castNode(RangeTblRef, linitial(query->jointree->fromlist))->rtindex;
	foreach(lc, program->groupcols)
	{
		AttrNumber	attno = lfirst_int(lc);
		Form_pg_attribute att = TupleDescAttr(basedesc, attno - 1);
		Var		   *var;

		var = makeVar(varno, attno, att->atttypid, att->atttypmod,
					  att->attcollation, 0);
		if (key->isnull[i])
		{
			NullTest   *nt = makeNode(NullTest);

			nt->arg = (Expr *) var;
			nt->nulltesttype = IS_NULL;
			nt->argisrow = false;
			nt->location = -1;
			conds = lappend(conds, nt);
		}
		else
		{
			Oid			opclass = GetDefaultOpClass(att->atttypid, BTREE_AM_OID);
			Oid			opfamily = get_opclass_family(opclass);
			Oid			opcintype = get_opclass_input_type(opclass);
			Oid			eqop = get_opfamily_member(opfamily, opcintype,
												   opcintype,
												   BTEqualStrategyNumber);
			Const	   *c;

			if (!OidIsValid(eqop))
				elog(ERROR, "missing equality operator for type %u",
					 att->atttypid);
			c = makeConst(att->atttypid, att->atttypmod, att->attcollation,
						  att->attlen, key->values[i], false, att->attbyval);
			conds = lappend(conds,
							make_opclause(eqop, BOOLOID, false,
										  (Expr *) var, (Expr *) c,
										  InvalidOid, att->attcollation));
		}
		i++;
	}

	if (query->jointree->quals)
		conds = lappend(conds, query->jointree->quals);
	query->jointree->quals = (Node *) make_ands_explicit(conds);

	return query;
}

/*
 * Run a query and return its result rows in a tuplestore.
 */
static Tuplestorestate *
run_query(Query *query)
{
	PlannedStmt *plan;
	QueryDesc  *qd;
	DestReceiver *dest;
	Tuplestorestate *result;

	result = tuplestore_begin_heap(false, false, work_mem);
	dest = CreateTuplestoreDestReceiver();
	SetTuplestoreDestReceiverParams(dest, result, CurrentMemoryContext,
									false, NULL, NULL);

	plan = pg_plan_query(query, "", 0, NULL, NULL);
	qd = CreateQueryDesc(plan, "", GetActiveSnapshot(), InvalidSnapshot,
						 dest, NULL, NULL, 0);
	ExecutorStart(qd, 0);
	ExecutorRun(qd, ForwardScanDirection, 0);
	ExecutorFinish(qd);
	ExecutorEnd(qd);
	FreeQueryDesc(qd);
	dest->rDestroy(dest);

	return result;
}

/*
 * Does a view row have one of the given group keys?  The view's grouping
 * columns are found through program->viewcols.
 */
static bool
row_has_key(TupleTableSlot *slot, IvmProgram *program, List *keys,
			TupleDesc basedesc)
{
	int			nkeys = list_length(program->groupcols);
	Datum	   *values = palloc_array(Datum, Max(nkeys, 1));
	bool	   *isnull = palloc_array(bool, Max(nkeys, 1));
	IvmGroupKey rowkey = {values, isnull};
	int			col = 0;
	bool		result = false;
	ListCell   *lc;

	slot_getallattrs(slot);
	foreach(lc, program->viewcols)
	{
		int			pos = lfirst_int(lc);

		/* first view column for each grouping column wins */
		if (pos > 0)
		{
			values[pos - 1] = slot->tts_values[col];
			isnull[pos - 1] = slot->tts_isnull[col];
		}
		col++;
	}

	foreach(lc, keys)
	{
		if (keys_equal(lfirst(lc), &rowkey, basedesc, program->groupcols))
		{
			result = true;
			break;
		}
	}
	pfree(values);
	pfree(isnull);
	return result;
}

/*
 * Delete the view rows with the given group keys (all rows if keys is NIL
 * and all is true), keeping its indexes up to date: index entries of a
 * deleted heap row are removed by VACUUM, as for any DELETE.
 */
static void
delete_view_rows(Relation viewrel, IvmProgram *program, List *keys,
				 bool all, TupleDesc basedesc)
{
	TableScanDesc scan;
	TupleTableSlot *slot;

	slot = table_slot_create(viewrel, NULL);
	scan = table_beginscan(viewrel, GetActiveSnapshot(), 0, NULL, SO_NONE);
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		CHECK_FOR_INTERRUPTS();
		if (all || program->groupcols == NIL ||
			row_has_key(slot, program, keys, basedesc))
			simple_table_tuple_delete(viewrel, &slot->tts_tid,
									  GetActiveSnapshot());
	}
	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);
}

/*
 * Insert rows into the view, with index entries.
 */
static void
insert_view_rows(Relation viewrel, Tuplestorestate *rows)
{
	EState	   *estate;
	ResultRelInfo *rri;
	TupleTableSlot *slot;
	TupleTableSlot *readslot;
	CommandId	cid = GetCurrentCommandId(true);

	estate = CreateExecutorState();
	rri = makeNode(ResultRelInfo);
	InitResultRelInfo(rri, viewrel, 0, NULL, 0);
	ExecOpenIndices(rri, false);

	slot = table_slot_create(viewrel, NULL);
	readslot = MakeSingleTupleTableSlot(RelationGetDescr(viewrel),
										&TTSOpsMinimalTuple);
	tuplestore_rescan(rows);
	while (tuplestore_gettupleslot(rows, true, false, readslot))
	{
		List	   *recheck;

		CHECK_FOR_INTERRUPTS();
		ExecCopySlot(slot, readslot);
		table_tuple_insert(viewrel, slot, cid, 0, NULL);
		recheck = ExecInsertIndexTuples(rri, estate, 0, slot, NIL, NULL);
		list_free(recheck);
	}

	ExecDropSingleTupleTableSlot(readslot);
	ExecDropSingleTupleTableSlot(slot);
	ExecCloseIndices(rri);
	FreeExecutorState(estate);
}

/*
 * Bring one view up to date.
 */
static void
maintain_view(IvmViewWork *work)
{
	Relation	viewrel;
	Relation	baserel;
	TupleDesc	basedesc;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	/*
	 * Order this maintainer after every earlier one: the lock is held until
	 * our commit is visible, and the snapshot taken below sees theirs.
	 */
	viewrel = table_open(work->viewoid, ExclusiveLock);

	/* an unpopulated view is filled by REFRESH, not maintained */
	if (!RelationIsPopulated(viewrel))
	{
		table_close(viewrel, NoLock);
		return;
	}

	baserel = table_open(work->program->baserel, AccessShareLock);
	basedesc = RelationGetDescr(baserel);

	/* run the view's query as its owner, as REFRESH does */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(viewrel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	CommandCounterIncrement();
	PushActiveSnapshot(GetLatestSnapshot());

	if (work->recompute)
	{
		delete_view_rows(viewrel, work->program, NIL, true, basedesc);
		insert_view_rows(viewrel,
						 run_query(view_query_for_key(viewrel, work->program,
													  basedesc, NULL)));
	}
	else if (work->keys != NIL)
	{
		ListCell   *lc;

		delete_view_rows(viewrel, work->program, work->keys, false, basedesc);
		foreach(lc, work->keys)
		{
			Tuplestorestate *rows;

			rows = run_query(view_query_for_key(viewrel, work->program,
												basedesc, lfirst(lc)));
			insert_view_rows(viewrel, rows);
			tuplestore_end(rows);
		}
	}

	PopActiveSnapshot();

	AtEOXact_GUC(false, save_nestlevel);
	SetUserIdAndSecContext(save_userid, save_sec_context);

	table_close(baserel, NoLock);
	table_close(viewrel, NoLock);
}

/*
 * Return the incrementally maintained views over a base table, in OID
 * order, which is also the order their locks are taken in.
 */
static List *
views_over(Oid baserelid)
{
	Relation	ivmrel;
	SysScanDesc scan;
	HeapTuple	tuple;
	List	   *result = NIL;

	ivmrel = table_open(MatViewIvmRelationId, AccessShareLock);
	scan = systable_beginscan(ivmrel, InvalidOid, false, NULL, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_matview_ivm form = (Form_pg_matview_ivm) GETSTRUCT(tuple);

		for (int i = 0; i < form->ivmbaserels.dim1; i++)
			if (form->ivmbaserels.values[i] == baserelid)
				result = lappend_oid(result, form->ivmrelid);
	}
	systable_endscan(scan);
	table_close(ivmrel, AccessShareLock);

	list_sort(result, list_oid_cmp);
	return result;
}

/*
 * list_sort comparator for IvmViewWork, by view OID.
 */
static int
work_cmp(const ListCell *a, const ListCell *b)
{
	Oid			va = ((IvmViewWork *) lfirst(a))->viewoid;
	Oid			vb = ((IvmViewWork *) lfirst(b))->viewoid;

	return pg_cmp_u32(va, vb);
}

/*
 * PreCommit_MaintainMatViews
 *		Apply this transaction's captured changes to the incrementally
 *		maintained views over the tables it changed.
 *
 * Called in the pre-commit loop of CommitTransaction and PrepareTransaction,
 * after deferred triggers.  Returns true if it did any work, so the caller
 * can run the loop again.
 */
bool
PreCommit_MaintainMatViews(void)
{
	List	   *changed;
	List	   *works = NIL;
	ListCell   *lc;

	changed = IvmTakeCapturedChanges();
	if (changed == NIL)
		return false;

	foreach(lc, changed)
	{
		IvmCapturedTable *ct = lfirst(lc);
		Relation	baserel = table_open(ct->relid, AccessShareLock);
		ListCell   *vc;

		foreach(vc, views_over(ct->relid))
		{
			IvmViewWork *work = palloc0_object(IvmViewWork);

			work->viewoid = lfirst_oid(vc);
			work->program = load_program(work->viewoid);
			work->recompute = ct->recompute;
			if (!work->recompute)
			{
				collect_keys(work, baserel, ct->olds);
				collect_keys(work, baserel, ct->news);
			}
			works = lappend(works, work);
		}
		table_close(baserel, NoLock);
	}

	/*
	 * Maintain the views in OID order, which is the order their locks are
	 * taken in, so two committing transactions that touch the same views
	 * cannot wait for each other in a cycle.  Views read one table only, so
	 * each view appears once.
	 */
	list_sort(works, work_cmp);
	foreach(lc, works)
		maintain_view(lfirst(lc));

	/* close the stores' temporary files before commit releases them */
	foreach(lc, changed)
	{
		IvmCapturedTable *ct = lfirst(lc);

		if (ct->olds)
			tuplestore_end(ct->olds);
		if (ct->news)
			tuplestore_end(ct->news);
	}

	return works != NIL;
}
