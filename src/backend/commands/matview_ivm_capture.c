/*-------------------------------------------------------------------------
 *
 * matview_ivm_capture.c
 *	  capture of changes to base tables of incrementally maintained
 *	  materialized views
 *
 * Every row change to a table that an incrementally maintained materialized
 * view reads is recorded here, by the transaction making it: the old row of
 * an UPDATE or DELETE with weight -1, the new row of an INSERT or UPDATE
 * with weight +1.  Capture is called from the AFTER ROW trigger entry points
 * of the executor (ExecAR*Triggers), which every DML path reaches, but it
 * does not depend on triggers: it runs whether or not the table has any,
 * and whatever session_replication_role says.
 *
 * Changes are kept in backend-local tuplestores, one per base table and
 * subtransaction, and never WAL-logged.  A subtransaction's stores are
 * appended to its parent's on commit and thrown away on abort.  Operations
 * that change a table without going through the executor (TRUNCATE) mark
 * the table's changes as needing a full recompute instead.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/matview_ivm_capture.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_matview_ivm.h"
#include "commands/matview_ivm.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/tuplestore.h"


/*
 * The changes one (sub)transaction made to one base table.
 */
typedef struct IvmChangeSet
{
	Oid			relid;			/* hash key */
	SubTransactionId subid;		/* (sub)transaction that made them */
	Tuplestorestate *olds;		/* rows removed (weight -1) */
	Tuplestorestate *news;		/* rows added (weight +1) */
	TupleDesc	desc;			/* row type of the stored rows */
	int64		nold;
	int64		nnew;
	bool		recompute;		/* changed outside the executor */
	struct IvmChangeSet *outer; /* the same table's set in the parent */
} IvmChangeSet;

/*
 * All change sets of the current transaction, keyed by table.  Each entry
 * points to the set of the innermost subtransaction that has changed that
 * table; ->outer leads to the enclosing subtransactions' sets.
 */
typedef struct IvmChangeEntry
{
	Oid			relid;			/* hash key; must be first */
	IvmChangeSet *set;
} IvmChangeEntry;

static HTAB *ivm_changes = NULL;
static MemoryContext ivm_context = NULL;


static IvmChangeSet *get_change_set(Relation rel);
static void mark_recompute(IvmChangeSet *set);
static bool relation_feeds_views(Relation rel);


/*
 * Does this relation have an incrementally maintained materialized view?
 * relhasivm is set when such a view is created and never cleared, so it can
 * be stale; confirm against pg_matview_ivm.
 *
 * shortcut: a full scan of pg_matview_ivm per call; cache the answer in the
 * relcache entry once capture is on a hot path (patch 3 needs the views
 * anyway).
 */
static bool
relation_feeds_views(Relation rel)
{
	Relation	ivmrel;
	SysScanDesc scan;
	HeapTuple	tuple;
	Oid			relid = RelationGetRelid(rel);
	bool		found = false;

	if (!rel->rd_rel->relhasivm)
		return false;

	ivmrel = table_open(MatViewIvmRelationId, AccessShareLock);
	scan = systable_beginscan(ivmrel, InvalidOid, false, NULL, 0, NULL);
	while (!found && HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_matview_ivm form = (Form_pg_matview_ivm) GETSTRUCT(tuple);

		for (int i = 0; i < form->ivmbaserels.dim1; i++)
		{
			if (form->ivmbaserels.values[i] == relid)
			{
				found = true;
				break;
			}
		}
	}
	systable_endscan(scan);
	table_close(ivmrel, AccessShareLock);

	return found;
}

/*
 * Should changes to the result relation be captured?  The answer is cached
 * in the ResultRelInfo for the statement.
 */
static bool
capture_needed(ResultRelInfo *relinfo)
{
	if (relinfo->ri_IvmCapture == IVM_CAPTURE_UNKNOWN)
		relinfo->ri_IvmCapture =
			relation_feeds_views(relinfo->ri_RelationDesc) ?
			IVM_CAPTURE_YES : IVM_CAPTURE_NO;
	return relinfo->ri_IvmCapture == IVM_CAPTURE_YES;
}

/*
 * Return the current subtransaction's change set for a table, creating it
 * (and the transaction's hash table) if needed.
 */
static IvmChangeSet *
get_change_set(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	SubTransactionId subid = GetCurrentSubTransactionId();
	IvmChangeSet *set;
	bool		found;
	MemoryContext oldcxt;

	if (ivm_changes == NULL)
	{
		HASHCTL		ctl;

		ivm_context = AllocSetContextCreate(TopTransactionContext,
											"incremental view changes",
											ALLOCSET_DEFAULT_SIZES);
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(IvmChangeEntry);
		ctl.hcxt = ivm_context;
		ivm_changes = hash_create("incremental view changes", 16, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	{
		IvmChangeEntry *entry;

		entry = (IvmChangeEntry *) hash_search(ivm_changes, &relid,
											   HASH_ENTER, &found);
		if (found && entry->set->subid == subid)
			return entry->set;

		oldcxt = MemoryContextSwitchTo(ivm_context);
		set = palloc0_object(IvmChangeSet);
		set->relid = relid;
		set->subid = subid;
		set->outer = found ? entry->set : NULL;
		/* kept for merging at subtransaction end, when catalogs are off */
		set->desc = CreateTupleDescCopy(RelationGetDescr(rel));
		entry->set = set;
		MemoryContextSwitchTo(oldcxt);
	}

	return set;
}

/*
 * Add one row to a tuplestore of a change set, creating the tuplestore in
 * the change-set context.
 *
 * The tuplestore's temporary files belong to the top transaction's resource
 * owner, not the current statement's portal, since the rows must outlive
 * the statement.  A subtransaction's sets are freed explicitly when it
 * aborts.
 */
static void
add_row(Tuplestorestate **store, TupleTableSlot *slot)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(ivm_context);
	ResourceOwner oldowner = CurrentResourceOwner;

	CurrentResourceOwner = TopTransactionResourceOwner;
	if (*store == NULL)
		*store = tuplestore_begin_heap(false, false, work_mem);
	tuplestore_puttupleslot(*store, slot);
	CurrentResourceOwner = oldowner;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * IvmCaptureRow
 *		Record one row change to a result relation, if any incrementally
 *		maintained materialized view reads it.
 *
 * oldslot is the row removed (UPDATE, DELETE) or NULL, newslot the row added
 * (INSERT, UPDATE) or NULL.  An UPDATE that leaves the row byte-for-byte the
 * same in every column is not recorded.
 */
void
IvmCaptureRow(ResultRelInfo *relinfo, TupleTableSlot *oldslot,
			  TupleTableSlot *newslot)
{
	IvmChangeSet *set;

	if (!capture_needed(relinfo))
		return;

	if (oldslot && newslot)
	{
		TupleDesc	desc = RelationGetDescr(relinfo->ri_RelationDesc);
		bool		same = true;

		slot_getallattrs(oldslot);
		slot_getallattrs(newslot);
		for (int i = 0; i < desc->natts && same; i++)
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);

			if (att->attisdropped)
				continue;
			if (oldslot->tts_isnull[i] != newslot->tts_isnull[i])
				same = false;
			else if (!oldslot->tts_isnull[i] &&
					 !datum_image_eq(oldslot->tts_values[i],
									 newslot->tts_values[i],
									 att->attbyval, att->attlen))
				same = false;
		}
		if (same)
			return;
	}

	set = get_change_set(relinfo->ri_RelationDesc);
	if (set->recompute)
		return;
	if (oldslot)
	{
		add_row(&set->olds, oldslot);
		set->nold++;
	}
	if (newslot)
	{
		add_row(&set->news, newslot);
		set->nnew++;
	}
}

/*
 * Mark a change set as needing a recompute, dropping its rows.
 */
static void
mark_recompute(IvmChangeSet *set)
{
	set->recompute = true;
	if (set->olds)
		tuplestore_clear(set->olds);
	if (set->news)
		tuplestore_clear(set->news);
	set->nold = set->nnew = 0;
}

/*
 * IvmCaptureRecompute
 *		Record that a table was changed outside the executor, so that its
 *		incrementally maintained views must be recomputed rather than
 *		maintained from row changes.
 *
 * Rows captured so far in this transaction are discarded: the recompute
 * covers them.
 */
void
IvmCaptureRecompute(Relation rel)
{
	if (!relation_feeds_views(rel))
		return;

	mark_recompute(get_change_set(rel));
}

/*
 * Free one change set's storage.
 */
static void
free_change_set(IvmChangeSet *set)
{
	if (set->olds)
		tuplestore_end(set->olds);
	if (set->news)
		tuplestore_end(set->news);
	FreeTupleDesc(set->desc);
	pfree(set);
}

/*
 * Append the rows of src to dst, creating dst's stores as needed.
 */
static void
append_store(Tuplestorestate **dst, Tuplestorestate *src, TupleDesc desc)
{
	TupleTableSlot *slot;

	if (src == NULL)
		return;
	slot = MakeSingleTupleTableSlot(desc, &TTSOpsMinimalTuple);
	tuplestore_rescan(src);
	while (tuplestore_gettupleslot(src, true, false, slot))
		add_row(dst, slot);
	ExecDropSingleTupleTableSlot(slot);
}

/*
 * AtEOSubXact_IvmCapture
 *		At subtransaction end, merge the subtransaction's change sets into
 *		its parent's (commit) or throw them away (abort).
 */
void
AtEOSubXact_IvmCapture(bool isCommit, SubTransactionId mySubid,
					   SubTransactionId parentSubid)
{
	HASH_SEQ_STATUS seq;
	IvmChangeEntry *entry;

	if (ivm_changes == NULL)
		return;

	hash_seq_init(&seq, ivm_changes);
	while ((entry = (IvmChangeEntry *) hash_seq_search(&seq)) != NULL)
	{
		IvmChangeSet *set = entry->set;
		IvmChangeSet *outer = set->outer;
		Oid			relid = set->relid;

		if (set->subid != mySubid)
			continue;

		if (!isCommit)
		{
			/* the subtransaction's changes were rolled back */
			entry->set = outer;
			free_change_set(set);
		}
		else if (outer == NULL || outer->subid != parentSubid)
		{
			/* the parent had no set for this table: this one becomes it */
			set->subid = parentSubid;
		}
		else
		{
			/* fold into the parent's set */
			if (set->recompute)
				mark_recompute(outer);
			else if (!outer->recompute)
			{
				append_store(&outer->olds, set->olds, set->desc);
				append_store(&outer->news, set->news, set->desc);
				outer->nold += set->nold;
				outer->nnew += set->nnew;
			}
			entry->set = outer;
			free_change_set(set);
		}

		if (entry->set == NULL)
			(void) hash_search(ivm_changes, &relid, HASH_REMOVE, NULL);
	}
}

/*
 * AtEOXact_IvmCapture
 *		At transaction end, forget all captured changes.  Their memory goes
 *		with TopTransactionContext.
 */
void
AtEOXact_IvmCapture(bool isCommit)
{
	ivm_changes = NULL;
	ivm_context = NULL;
}

/*
 * IvmCapturedCounts
 *		Report the rows captured so far in this transaction for a table, as
 *		seen from the current subtransaction.  For tests.
 */
bool
IvmCapturedCounts(Oid relid, int64 *nold, int64 *nnew, bool *recompute)
{
	IvmChangeEntry *entry;

	*nold = *nnew = 0;
	*recompute = false;
	if (ivm_changes == NULL)
		return false;
	entry = (IvmChangeEntry *) hash_search(ivm_changes, &relid, HASH_FIND,
										   NULL);
	if (entry == NULL)
		return false;
	for (IvmChangeSet *set = entry->set; set != NULL; set = set->outer)
	{
		*nold += set->nold;
		*nnew += set->nnew;
		if (set->recompute)
			*recompute = true;
	}
	return true;
}
