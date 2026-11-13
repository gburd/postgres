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
 * distinct group keys of the captured rows that pass the view's WHERE
 * clause, deletes the view's rows for those keys, and inserts the rows the
 * view's query now produces for them.  A table changed outside the
 * executor (TRUNCATE) gets its views recomputed entirely.
 *
 * The keys without NULLs are handed to the planner as a named tuplestore,
 * the same mechanism trigger transition tables use, and joined to the
 * view's query and to the view, so each view costs one recompute query and
 * one lookup query however many groups changed, and the planner can use
 * the base table's and the view's indexes.  Keys with a NULL can't be
 * joined with equality; each gets its own pair of queries using IS NULL.
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
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_matview_ivm.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "common/int.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/matview_ivm.h"
#include "executor/executor.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/injection_point.h"
#include "utils/lsyscache.h"
#include "utils/queryenvironment.h"
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

/* the group keys with one hash value; nearly always a single key */
typedef struct IvmKeyBucket
{
	uint32		hash;			/* hash key; must be first */
	List	   *keys;			/* list of IvmGroupKey * */
} IvmKeyBucket;

/* what one view needs from the commit pass */
typedef struct IvmViewWork
{
	Oid			viewoid;
	IvmProgram *program;
	bool		recompute;		/* recompute all of it */
	List	   *keys;			/* list of IvmGroupKey *, without duplicates */
	HTAB	   *keyhash;		/* IvmKeyBucket by key hash, for deduplication */
} IvmViewWork;

/* the name the planner and executor know the key tuplestore by */
#define IVM_KEYS_ENR_NAME	"pg_ivm_keys"


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
 * Hash a group key, consistently with keys_equal().
 */
static uint32
key_hash(IvmGroupKey *key, TupleDesc desc, List *groupcols)
{
	uint32		hash = 0;
	int			i = 0;
	ListCell   *lc;

	foreach(lc, groupcols)
	{
		Form_pg_attribute att = TupleDescAttr(desc, lfirst_int(lc) - 1);
		uint32		h = 0;

		if (!key->isnull[i])
			h = datum_image_hash(key->values[i], att->attbyval, att->attlen);
		hash = hash_combine(hash, h);
		i++;
	}
	return hash;
}

/*
 * Add one group key to work->keys unless an equal key is already there.
 * Takes ownership of key.
 */
static void
add_key(IvmViewWork *work, IvmGroupKey *key, TupleDesc desc)
{
	List	   *groupcols = work->program->groupcols;
	uint32		hash = key_hash(key, desc, groupcols);
	IvmKeyBucket *bucket;
	bool		found;
	ListCell   *lc;

	if (work->keyhash == NULL)
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(uint32);
		ctl.entrysize = sizeof(IvmKeyBucket);
		ctl.hcxt = CurrentMemoryContext;
		work->keyhash = hash_create("incremental view group keys", 256, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	bucket = hash_search(work->keyhash, &hash, HASH_ENTER, &found);
	if (!found)
		bucket->keys = NIL;
	foreach(lc, bucket->keys)
	{
		if (keys_equal(lfirst(lc), key, desc, groupcols))
			return;
	}
	bucket->keys = lappend(bucket->keys, key);
	work->keys = lappend(work->keys, key);
}

/*
 * Add the group keys of the captured rows that pass the view's WHERE
 * clause to work->keys.
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
		add_key(work, key, desc);
	}

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);
}

/*
 * Does a group key have a NULL in it?
 */
static bool
key_has_null(IvmGroupKey *key, int nkeys)
{
	for (int i = 0; i < nkeys; i++)
		if (key->isnull[i])
			return true;
	return false;
}

/*
 * Build the named tuplestore holding the keys without NULLs, one row per
 * key with one column per grouping column, and register it in a new query
 * environment.  Returns NULL if there are no such keys.
 */
static QueryEnvironment *
make_key_env(IvmViewWork *work, TupleDesc basedesc)
{
	List	   *groupcols = work->program->groupcols;
	int			nkeys = list_length(groupcols);
	TupleDesc	keydesc;
	Tuplestorestate *store;
	EphemeralNamedRelation enr;
	QueryEnvironment *env;
	double		ntuples = 0;
	int			i = 0;
	ListCell   *lc;

	keydesc = CreateTemplateTupleDesc(nkeys);
	foreach(lc, groupcols)
	{
		Form_pg_attribute att = TupleDescAttr(basedesc, lfirst_int(lc) - 1);

		TupleDescInitEntry(keydesc, ++i, NameStr(att->attname),
						   att->atttypid, att->atttypmod, 0);
		TupleDescInitEntryCollation(keydesc, i, att->attcollation);
	}
	TupleDescFinalize(keydesc);

	store = tuplestore_begin_heap(false, false, work_mem);
	foreach(lc, work->keys)
	{
		IvmGroupKey *key = lfirst(lc);

		if (key_has_null(key, nkeys))
			continue;
		tuplestore_putvalues(store, keydesc, key->values, key->isnull);
		ntuples++;
	}
	if (ntuples == 0)
	{
		tuplestore_end(store);
		return NULL;
	}

	enr = palloc0_object(EphemeralNamedRelationData);
	enr->md.name = IVM_KEYS_ENR_NAME;
	enr->md.reliddesc = InvalidOid;
	enr->md.tupdesc = keydesc;
	enr->md.enrtype = ENR_NAMED_TUPLESTORE;
	enr->md.enrtuples = ntuples;
	enr->reldata = store;
	env = create_queryEnv();
	register_ENR(env, enr);
	return env;
}

/*
 * Make "var = value" with the default btree equality operator of var's type,
 * the operator the group key's distinctness was judged by at CREATE.
 */
static Expr *
make_key_eq(Var *var, Expr *value)
{
	Oid			opclass = GetDefaultOpClass(var->vartype, BTREE_AM_OID);
	Oid			opfamily = get_opclass_family(opclass);
	Oid			opcintype = get_opclass_input_type(opclass);
	Oid			eqop = get_opfamily_member(opfamily, opcintype, opcintype,
										   BTEqualStrategyNumber);

	if (!OidIsValid(eqop))
		elog(ERROR, "missing equality operator for type %u", var->vartype);
	return make_opclause(eqop, BOOLOID, false, (Expr *) var, value,
						 InvalidOid, var->varcollid);
}

/*
 * Restrict a query to group keys.  keyvars are the Vars of the query that
 * hold the grouping columns, in groupcols order.  With env set, join the
 * query to the key tuplestore registered there; the keys are distinct, so
 * the join doesn't duplicate rows.  Otherwise restrict it to the one key
 * given, using IS NULL for its NULL columns, or, with no key either, leave
 * it alone.
 */
static void
restrict_to_keys(Query *query, List *keyvars, QueryEnvironment *env,
				 IvmGroupKey *key)
{
	List	   *conds = NIL;
	int			keyrti = 0;
	int			i = 0;
	ListCell   *lc;

	if (env == NULL && key == NULL)
		return;

	if (env)
	{
		EphemeralNamedRelationMetadata md =
			get_visible_ENR_metadata(env, IVM_KEYS_ENR_NAME);
		RangeTblEntry *rte = makeNode(RangeTblEntry);
		RangeTblRef *rtr = makeNode(RangeTblRef);
		TupleDesc	keydesc = md->tupdesc;

		rte->rtekind = RTE_NAMEDTUPLESTORE;
		rte->enrname = md->name;
		rte->enrtuples = md->enrtuples;
		rte->eref = makeAlias(md->name, NIL);
		for (int j = 0; j < keydesc->natts; j++)
		{
			Form_pg_attribute att = TupleDescAttr(keydesc, j);

			rte->eref->colnames = lappend(rte->eref->colnames,
										  makeString(pstrdup(NameStr(att->attname))));
			rte->coltypes = lappend_oid(rte->coltypes, att->atttypid);
			rte->coltypmods = lappend_int(rte->coltypmods, att->atttypmod);
			rte->colcollations = lappend_oid(rte->colcollations,
											 att->attcollation);
		}
		rte->inFromCl = true;
		query->rtable = lappend(query->rtable, rte);
		keyrti = list_length(query->rtable);
		rtr->rtindex = keyrti;
		query->jointree->fromlist = lappend(query->jointree->fromlist, rtr);
	}

	foreach(lc, keyvars)
	{
		Var		   *var = copyObject(lfirst(lc));

		if (env)
		{
			Var		   *keyvar = makeVar(keyrti, i + 1, var->vartype,
										 var->vartypmod, var->varcollid, 0);

			conds = lappend(conds, make_key_eq(var, (Expr *) keyvar));
		}
		else if (key->isnull[i])
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
			int16		typlen;
			bool		typbyval;
			Const	   *c;

			get_typlenbyval(var->vartype, &typlen, &typbyval);
			c = makeConst(var->vartype, var->vartypmod, var->varcollid,
						  typlen, key->values[i], false, typbyval);
			conds = lappend(conds, make_key_eq(var, (Expr *) c));
		}
		i++;
	}

	if (query->jointree->quals)
		conds = lappend(conds, query->jointree->quals);
	query->jointree->quals = (Node *) make_ands_explicit(conds);
}

/*
 * Return the view's query, rewritten.
 */
static Query *
view_query(Relation viewrel)
{
	RewriteRule *rule = viewrel->rd_rules->rules[0];
	Query	   *query = copyObject(linitial_node(Query, rule->actions));
	List	   *rewritten;

	AcquireRewriteLocks(query, true, false);
	rewritten = QueryRewrite(query);
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for incremental view maintenance");
	return linitial_node(Query, rewritten);
}

/*
 * Return the view's query restricted to the given keys (see
 * restrict_to_keys).
 */
static Query *
view_query_for_keys(Relation viewrel, IvmProgram *program, TupleDesc basedesc,
					QueryEnvironment *env, IvmGroupKey *key)
{
	Query	   *query = view_query(viewrel);
	int			varno;
	List	   *keyvars = NIL;
	ListCell   *lc;

	varno = castNode(RangeTblRef, linitial(query->jointree->fromlist))->rtindex;
	foreach(lc, program->groupcols)
	{
		AttrNumber	attno = lfirst_int(lc);
		Form_pg_attribute att = TupleDescAttr(basedesc, attno - 1);

		keyvars = lappend(keyvars, makeVar(varno, attno, att->atttypid,
										   att->atttypmod, att->attcollation,
										   0));
	}
	restrict_to_keys(query, keyvars, env, key);
	return query;
}

/*
 * Return "SELECT ctid FROM view" restricted to the given keys (see
 * restrict_to_keys).  The view's grouping columns are found through
 * program->viewcols; the first view column for each grouping column is
 * used.
 */
static Query *
view_rows_for_keys(Relation viewrel, IvmProgram *program,
				   QueryEnvironment *env, IvmGroupKey *key)
{
	Query	   *query = makeNode(Query);
	ParseState *pstate = make_parsestate(NULL);
	ParseNamespaceItem *nsitem;
	RangeTblRef *rtr = makeNode(RangeTblRef);
	TupleDesc	viewdesc = RelationGetDescr(viewrel);
	int			nkeys = list_length(program->groupcols);
	Var		  **keyvars = palloc0_array(Var *, Max(nkeys, 1));
	List	   *keyvarlist = NIL;
	Var		   *ctid;
	int			col = 0;
	ListCell   *lc;

	nsitem = addRangeTableEntryForRelation(pstate, viewrel, AccessShareLock,
										   NULL, false, false);
	nsitem->p_perminfo->requiredPerms = ACL_SELECT;
	foreach(lc, program->viewcols)
	{
		int			pos = lfirst_int(lc);

		col++;
		if (pos > 0 && keyvars[pos - 1] == NULL)
		{
			Form_pg_attribute att = TupleDescAttr(viewdesc, col - 1);

			keyvars[pos - 1] = makeVar(1, col, att->atttypid, att->atttypmod,
									   att->attcollation, 0);
			nsitem->p_perminfo->selectedCols =
				bms_add_member(nsitem->p_perminfo->selectedCols,
							   col - FirstLowInvalidHeapAttributeNumber);
		}
	}
	for (int i = 0; i < nkeys; i++)
		keyvarlist = lappend(keyvarlist, keyvars[i]);

	query->commandType = CMD_SELECT;
	query->canSetTag = true;
	query->rtable = pstate->p_rtable;
	query->rteperminfos = pstate->p_rteperminfos;
	rtr->rtindex = 1;
	query->jointree = makeFromExpr(list_make1(rtr), NULL);
	ctid = makeVar(1, SelfItemPointerAttributeNumber, TIDOID, -1,
				   InvalidOid, 0);
	query->targetList = list_make1(makeTargetEntry((Expr *) ctid, 1,
												   pstrdup("ctid"), false));
	restrict_to_keys(query, keyvarlist, env, key);
	return query;
}

/*
 * Run a query and return its result rows in a tuplestore.
 */
static Tuplestorestate *
run_query(Query *query, QueryEnvironment *env)
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
						 dest, NULL, env, 0);
	ExecutorStart(qd, 0);
	ExecutorRun(qd, ForwardScanDirection, 0);
	ExecutorFinish(qd);
	ExecutorEnd(qd);
	FreeQueryDesc(qd);
	dest->rDestroy(dest);

	return result;
}

/*
 * Delete the view rows whose ctids a query returns.  Index entries of a
 * deleted row are removed by VACUUM, as for any DELETE.
 */
static void
delete_view_rows(Relation viewrel, Query *query, QueryEnvironment *env)
{
	Tuplestorestate *tids = run_query(query, env);
	TupleDesc	desc = CreateTemplateTupleDesc(1);
	TupleTableSlot *slot;

	TupleDescInitEntry(desc, 1, "ctid", TIDOID, -1, 0);
	TupleDescFinalize(desc);
	slot = MakeSingleTupleTableSlot(desc, &TTSOpsMinimalTuple);
	tuplestore_rescan(tids);
	while (tuplestore_gettupleslot(tids, true, false, slot))
	{
		bool		isnull;
		ItemPointer tid = DatumGetItemPointer(slot_getattr(slot, 1, &isnull));

		CHECK_FOR_INTERRUPTS();
		simple_table_tuple_delete(viewrel, tid, GetActiveSnapshot());
	}
	ExecDropSingleTupleTableSlot(slot);
	tuplestore_end(tids);
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
 * Replace the view's rows for some group keys (see restrict_to_keys) by
 * the rows its query now produces for them.  The lookup of the old rows
 * runs to completion before the first insert, so it can't see the new ones.
 */
static void
recompute_keys(Relation viewrel, IvmProgram *program, TupleDesc basedesc,
			   QueryEnvironment *env, IvmGroupKey *key)
{
	Tuplestorestate *rows;

	delete_view_rows(viewrel, view_rows_for_keys(viewrel, program, env, key),
					 env);
	rows = run_query(view_query_for_keys(viewrel, program, basedesc, env, key),
					 env);
	insert_view_rows(viewrel, rows);
	tuplestore_end(rows);
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

	if (work->recompute || work->program->groupcols == NIL)
	{
		/* the whole view, which is a single group if there's no GROUP BY */
		if (work->recompute || work->keys != NIL)
			recompute_keys(viewrel, work->program, basedesc, NULL, NULL);
	}
	else if (work->keys != NIL)
	{
		int			nkeys = list_length(work->program->groupcols);
		QueryEnvironment *env = make_key_env(work, basedesc);
		ListCell   *lc;

		if (env)
			recompute_keys(viewrel, work->program, basedesc, env, NULL);
		foreach(lc, work->keys)
		{
			IvmGroupKey *key = lfirst(lc);

			if (key_has_null(key, nkeys))
				recompute_keys(viewrel, work->program, basedesc, NULL, key);
		}
	}

	PopActiveSnapshot();

	INJECTION_POINT("matview-ivm-maintained", NULL);

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
