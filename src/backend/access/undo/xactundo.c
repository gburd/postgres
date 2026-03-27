/*-------------------------------------------------------------------------
 *
 * xactundo.c
 *	  Management of undo record sets for transactions
 *
 * Undo records that need to be applied after a transaction or
 * subtransaction abort should be inserted using the functions defined
 * in this file; thus, every table or index access method that wants to
 * use undo for post-abort cleanup should invoke these interfaces.
 *
 * The reason for this design is that we want to pack all of the undo
 * records for a single transaction into one place, regardless of the
 * AM which generated them. That way, we can apply the undo actions
 * which pertain to that transaction in the correct order; namely,
 * backwards as compared with the order in which the records were
 * generated.
 *
 * We may use up to three undo record sets per transaction, one per
 * persistence level (permanent, unlogged, temporary). We assume that
 * it's OK to apply the undo records for each persistence level
 * independently of the others. This is safe since the modifications
 * must necessarily touch disjoint sets of pages.
 *
 * This design follows the EDB undo-record-set branch architecture
 * (xactundo.c) adapted for the physical undo approach used here.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/xactundo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/undo.h"
#include "access/relundo_worker.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/xact.h"
#include "access/xactundo.h"
#include "access/relundo.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* Per-relation UNDO tracking for rollback */
typedef struct PerRelUndoEntry
{
	Oid			relid;			/* Relation OID */
	RelUndoRecPtr start_urec_ptr;	/* First UNDO record for this relation */
	struct PerRelUndoEntry *next;
}			PerRelUndoEntry;

/* Per-subtransaction backend-private undo state. */
typedef struct XactUndoSubTransaction
{
	SubTransactionId nestingLevel;
	UndoRecPtr	start_location[NUndoPersistenceLevels];
	struct XactUndoSubTransaction *next;
}			XactUndoSubTransaction;

/* Backend-private undo state. */
typedef struct XactUndoData
{
	bool		has_undo;		/* has this xact generated any undo? */
	XactUndoSubTransaction *subxact;	/* current subtransaction state */

	/*
	 * Per-persistence-level record sets. These are created lazily on first
	 * use and destroyed at transaction end.
	 */
	UndoRecordSet *record_set[NUndoPersistenceLevels];

	/* Tracking for the most recent undo insertion per persistence level. */
	UndoRecPtr	last_location[NUndoPersistenceLevels];

	/* Per-relation UNDO tracking for rollback */
	PerRelUndoEntry *relundo_list;	/* List of relations with per-relation UNDO */
}			XactUndoData;

static XactUndoData XactUndo;
static XactUndoSubTransaction XactUndoTopState;

static void ResetXactUndo(void);
static void CollapseXactUndoSubTransactions(void);
static void ApplyPerRelUndo(void);
static UndoPersistenceLevel GetUndoPersistenceLevel(char relpersistence);

/*
 * XactUndoShmemSize
 *		How much shared memory do we need for transaction undo state?
 *
 * Currently no shared memory is needed -- all state is backend-private.
 * This function exists for forward compatibility with the architecture
 * where an UndoRequestManager will be added later.
 */
Size
XactUndoShmemSize(void)
{
	return 0;
}

/*
 * XactUndoShmemInit
 *		Initialize shared memory for transaction undo state.
 *
 * Currently a no-op; provided for the unified UndoShmemInit() pattern.
 */
void
XactUndoShmemInit(void)
{
	/* Nothing to do yet. */
}

/*
 * InitializeXactUndo
 *		Per-backend initialization for transaction undo.
 */
void
InitializeXactUndo(void)
{
	ResetXactUndo();
}

/*
 * GetUndoPersistenceLevel
 *		Map relation persistence character to UndoPersistenceLevel.
 */
static UndoPersistenceLevel
GetUndoPersistenceLevel(char relpersistence)
{
	switch (relpersistence)
	{
		case RELPERSISTENCE_PERMANENT:
			return UNDOPERSISTENCE_PERMANENT;
		case RELPERSISTENCE_UNLOGGED:
			return UNDOPERSISTENCE_UNLOGGED;
		case RELPERSISTENCE_TEMP:
			return UNDOPERSISTENCE_TEMP;
		default:
			elog(ERROR, "unrecognized relpersistence: %c", relpersistence);
			return UNDOPERSISTENCE_PERMANENT;	/* keep compiler quiet */
	}
}

/*
 * PrepareXactUndoData
 *		Prepare to insert a transactional undo record.
 *
 * Finds or creates the appropriate per-persistence-level UndoRecordSet
 * for the current transaction and adds the record to it.
 *
 * Returns the UndoRecPtr where the record will be inserted (or
 * InvalidUndoRecPtr if undo is disabled).
 */
UndoRecPtr
PrepareXactUndoData(XactUndoContext * ctx, char persistence,
					uint16 record_type, Relation rel,
					BlockNumber blkno, OffsetNumber offset,
					HeapTuple oldtuple)
{
	int			nestingLevel = GetCurrentTransactionNestLevel();
	UndoPersistenceLevel plevel = GetUndoPersistenceLevel(persistence);
	TransactionId xid = GetCurrentTransactionId();
	UndoRecordSet *uset;
	UndoRecPtr *sub_start_location;

	/* Remember that we've done something undo-related. */
	XactUndo.has_undo = true;

	/*
	 * If we've entered a subtransaction, spin up a new XactUndoSubTransaction
	 * so that we can track the start locations for the subtransaction
	 * separately from any parent (sub)transactions.
	 */
	if (nestingLevel > XactUndo.subxact->nestingLevel)
	{
		XactUndoSubTransaction *subxact;
		int			i;

		subxact = MemoryContextAlloc(UndoContext ? UndoContext : TopMemoryContext,
									 sizeof(XactUndoSubTransaction));
		subxact->nestingLevel = nestingLevel;
		subxact->next = XactUndo.subxact;
		XactUndo.subxact = subxact;

		for (i = 0; i < NUndoPersistenceLevels; ++i)
			subxact->start_location[i] = InvalidUndoRecPtr;
	}

	/*
	 * Make sure we have an UndoRecordSet of the appropriate type open for
	 * this persistence level.  These record sets are always associated with
	 * the toplevel transaction, not a subtransaction, to avoid fragmentation.
	 */
	uset = XactUndo.record_set[plevel];
	if (uset == NULL)
	{
		uset = UndoRecordSetCreate(xid, GetCurrentTransactionUndoRecPtr());
		XactUndo.record_set[plevel] = uset;
	}

	/* Remember persistence level for InsertXactUndoData. */
	ctx->plevel = plevel;
	ctx->uset = uset;

	/* Add the record to the record set. */
	UndoRecordAddTuple(uset, record_type, rel, blkno, offset, oldtuple);

	/*
	 * If this is the first undo for this persistence level in this
	 * subtransaction, record the start location. The actual UndoRecPtr is not
	 * known until insertion, so we use a sentinel for now and the caller will
	 * update it after InsertXactUndoData.
	 */
	sub_start_location = &XactUndo.subxact->start_location[plevel];
	if (!UndoRecPtrIsValid(*sub_start_location))
		*sub_start_location = (UndoRecPtr) 1;	/* will be set properly */

	return InvalidUndoRecPtr;	/* actual ptr assigned during insert */
}

/*
 * InsertXactUndoData
 *		Insert the prepared undo data into the undo log.
 *
 * This performs the actual write of the accumulated records.
 */
void
InsertXactUndoData(XactUndoContext * ctx)
{
	UndoRecordSet *uset = ctx->uset;
	UndoRecPtr	ptr;

	Assert(uset != NULL);

	ptr = UndoRecordSetInsert(uset);
	if (UndoRecPtrIsValid(ptr))
	{
		XactUndo.last_location[ctx->plevel] = ptr;

		/* Fix up subtransaction start location if needed */
		if (XactUndo.subxact->start_location[ctx->plevel] == (UndoRecPtr) 1)
			XactUndo.subxact->start_location[ctx->plevel] = ptr;
	}
}

/*
 * CleanupXactUndoInsertion
 *		Clean up after an undo insertion cycle.
 *
 * Note: does NOT free the record set -- that happens at xact end.
 * This just resets the per-insertion buffer so the set can accumulate
 * more records.
 */
void
CleanupXactUndoInsertion(XactUndoContext * ctx)
{
	/* Nothing to do currently; the record set buffer is reusable. */
}

/*
 * GetCurrentXactUndoRecPtr
 *		Get the most recent undo record pointer for a persistence level.
 */
UndoRecPtr
GetCurrentXactUndoRecPtr(UndoPersistenceLevel plevel)
{
	return XactUndo.last_location[plevel];
}

/*
 * AtCommit_XactUndo
 *		Post-commit cleanup of the undo state.
 *
 * On commit, undo records are no longer needed for rollback.
 * Free all record sets and reset state.
 *
 * NB: This code MUST NOT FAIL, since it is run as a post-commit step.
 */
void
AtCommit_XactUndo(void)
{
	int			i;

	if (!XactUndo.has_undo)
		return;

	/* Free all per-persistence-level record sets. */
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		if (XactUndo.record_set[i] != NULL)
		{
			UndoRecordSetFree(XactUndo.record_set[i]);
			XactUndo.record_set[i] = NULL;
		}
	}

	ResetXactUndo();
}

/*
 * AtAbort_XactUndo
 *		Post-abort cleanup of the undo state.
 *
 * On abort, we need to apply the undo chain to roll back changes.
 * The actual undo application is triggered by xact.c before calling
 * this function.  Here we apply per-relation UNDO and clean up the record sets.
 */
void
AtAbort_XactUndo(void)
{
	int			i;

	elog(LOG, "AtAbort_XactUndo: entered, has_undo=%d, relundo_list=%p",
		 XactUndo.has_undo, XactUndo.relundo_list);

	if (!XactUndo.has_undo && XactUndo.relundo_list == NULL)
		return;

	/* Collapse all subtransaction state. */
	CollapseXactUndoSubTransactions();

	/*
	 * Apply per-relation UNDO chains before cleaning up.
	 * This must happen before we reset state so we have the relation list.
	 */
	ApplyPerRelUndo();

	/* Free all per-persistence-level record sets. */
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		if (XactUndo.record_set[i] != NULL)
		{
			UndoRecordSetFree(XactUndo.record_set[i]);
			XactUndo.record_set[i] = NULL;
		}
	}

	ResetXactUndo();
}

/*
 * AtSubCommit_XactUndo
 *		Subtransaction commit: merge sub undo state into parent.
 */
void
AtSubCommit_XactUndo(int level)
{
	XactUndoSubTransaction *subxact = XactUndo.subxact;
	int			i;

	if (subxact == NULL || subxact->nestingLevel != level)
		return;

	/* Merge start locations into parent. */
	XactUndo.subxact = subxact->next;
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		if (UndoRecPtrIsValid(subxact->start_location[i]) &&
			!UndoRecPtrIsValid(XactUndo.subxact->start_location[i]))
		{
			XactUndo.subxact->start_location[i] =
				subxact->start_location[i];
		}
	}

	if (subxact != &XactUndoTopState)
		pfree(subxact);
}

/*
 * AtSubAbort_XactUndo
 *		Subtransaction abort: apply undo for this sub-level, clean up.
 */
void
AtSubAbort_XactUndo(int level)
{
	XactUndoSubTransaction *subxact = XactUndo.subxact;

	if (subxact == NULL || subxact->nestingLevel != level)
		return;

	/*
	 * TODO: Apply undo for just this subtransaction's records. For now, the
	 * records remain in the record set and will be applied at toplevel abort.
	 */

	XactUndo.subxact = subxact->next;
	if (subxact != &XactUndoTopState)
		pfree(subxact);
}

/*
 * AtProcExit_XactUndo
 *		Process exit cleanup for transaction undo.
 */
void
AtProcExit_XactUndo(void)
{
	int			i;

	/* Free any lingering record sets. */
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		if (XactUndo.record_set[i] != NULL)
		{
			UndoRecordSetFree(XactUndo.record_set[i]);
			XactUndo.record_set[i] = NULL;
		}
	}

	ResetXactUndo();
}

/*
 * ResetXactUndo
 *		Reset all backend-private undo state for the next transaction.
 */
static void
ResetXactUndo(void)
{
	int			i;

	XactUndo.has_undo = false;

	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		XactUndo.record_set[i] = NULL;
		XactUndo.last_location[i] = InvalidUndoRecPtr;
	}

	/* Reset subtransaction stack to the top level. */
	XactUndo.subxact = &XactUndoTopState;
	XactUndoTopState.nestingLevel = 1;
	XactUndoTopState.next = NULL;
	for (i = 0; i < NUndoPersistenceLevels; i++)
		XactUndoTopState.start_location[i] = InvalidUndoRecPtr;

	/* Reset per-relation UNDO list */
	XactUndo.relundo_list = NULL;
}

/*
 * CollapseXactUndoSubTransactions
 *		Collapse all subtransaction state into the top level.
 */
static void
CollapseXactUndoSubTransactions(void)
{
	/* If XactUndo hasn't been initialized yet, nothing to collapse */
	if (XactUndo.subxact == NULL)
		return;

	while (XactUndo.subxact != &XactUndoTopState)
	{
		XactUndoSubTransaction *subxact = XactUndo.subxact;
		int			i;

		XactUndo.subxact = subxact->next;

		/* Propagate start locations upward. */
		for (i = 0; i < NUndoPersistenceLevels; i++)
		{
			if (UndoRecPtrIsValid(subxact->start_location[i]) &&
				!UndoRecPtrIsValid(XactUndo.subxact->start_location[i]))
			{
				XactUndo.subxact->start_location[i] =
					subxact->start_location[i];
			}
		}

		pfree(subxact);
	}
}

/*
 * RegisterPerRelUndo
 *		Register a per-relation UNDO chain for rollback on abort.
 *
 * Called by table AMs that use per-relation UNDO when they insert their
 * first UNDO record for a relation in the current transaction.
 */
void
RegisterPerRelUndo(Oid relid, RelUndoRecPtr start_urec_ptr)
{
	PerRelUndoEntry *entry;

	elog(LOG, "RegisterPerRelUndo: called for relid=%u, start_urec_ptr=%lu",
		 relid, (unsigned long) start_urec_ptr);

	/* Initialize XactUndo if this is the first time it's being used */
	if (XactUndo.subxact == NULL)
	{
		XactUndo.subxact = &XactUndoTopState;
		XactUndoTopState.nestingLevel = 1;
		XactUndoTopState.next = NULL;
		for (int i = 0; i < NUndoPersistenceLevels; i++)
			XactUndoTopState.start_location[i] = InvalidUndoRecPtr;
	}

	/* Mark that we have UNDO so commit/abort cleanup happens correctly */
	XactUndo.has_undo = true;

	/* Check if this relation is already registered and update the pointer */
	for (entry = XactUndo.relundo_list; entry != NULL; entry = entry->next)
	{
		if (entry->relid == relid)
		{
			/* Update to the latest UNDO pointer for rollback */
			entry->start_urec_ptr = start_urec_ptr;
			elog(DEBUG1, "RegisterPerRelUndo: updated relation %u to UNDO pointer %lu",
				 relid, (unsigned long) start_urec_ptr);
			return;
		}
	}

	/* Add new entry to the list. Use CurTransactionContext for proper cleanup. */
	entry = (PerRelUndoEntry *) MemoryContextAlloc(CurTransactionContext,
												   sizeof(PerRelUndoEntry));
	entry->relid = relid;
	entry->start_urec_ptr = start_urec_ptr;
	entry->next = XactUndo.relundo_list;
	XactUndo.relundo_list = entry;

	elog(DEBUG1, "RegisterPerRelUndo: registered relation %u with start UNDO pointer %lu",
		 relid, (unsigned long) start_urec_ptr);
}

/*
 * GetPerRelUndoPtr
 *		Return the current (latest) UNDO record pointer for a relation,
 *		or InvalidRelUndoRecPtr if the relation has no registered UNDO.
 *
 * Used by table AMs to chain UNDO records: each new UNDO record's
 * urec_prevundorec is set to the previous record pointer.
 */
RelUndoRecPtr
GetPerRelUndoPtr(Oid relid)
{
	PerRelUndoEntry *entry;

	for (entry = XactUndo.relundo_list; entry != NULL; entry = entry->next)
	{
		if (entry->relid == relid)
			return entry->start_urec_ptr;
	}

	return InvalidRelUndoRecPtr;
}

/*
 * ApplyPerRelUndo
 *		Apply per-relation UNDO chains for all registered relations.
 *
 * Called during transaction abort to roll back changes made via
 * per-relation UNDO. Queue work for background UNDO workers.
 *
 * Per-relation UNDO cannot be applied synchronously during ROLLBACK
 * because we cannot safely access the catalog (IsTransactionState()
 * returns false during TRANS_ABORT state, causing relation_open() to
 * assert-fail).
 *
 * Instead, we queue the work for background UNDO workers that will
 * apply the UNDO chains asynchronously in a proper transaction context.
 * This matches the ZHeap architecture where UNDO application is
 * deferred to background processes.
 */
static void
ApplyPerRelUndo(void)
{
	PerRelUndoEntry *entry;
	TransactionId xid = GetCurrentTransactionIdIfAny();

	if (XactUndo.relundo_list == NULL)
	{
		elog(DEBUG1, "ApplyPerRelUndo: no per-relation UNDO to apply");
		return;					/* No per-relation UNDO to apply */
	}

	elog(LOG, "ApplyPerRelUndo: queuing UNDO work for background workers");

	for (entry = XactUndo.relundo_list; entry != NULL; entry = entry->next)
	{
		elog(LOG, "Queuing UNDO work: database %u, relation %u, UNDO ptr %lu",
			 MyDatabaseId, entry->relid, (unsigned long) entry->start_urec_ptr);

		RelUndoQueueAdd(MyDatabaseId, entry->relid, entry->start_urec_ptr, xid);
	}

	/* Start a worker if one isn't already running */
	StartRelUndoWorker(MyDatabaseId);
}
