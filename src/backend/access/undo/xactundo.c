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

#include "access/atm.h"
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

/* GUC: UNDO bytes threshold for instant abort via ATM */
int			undo_instant_abort_threshold = 65536;

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
	PerRelUndoEntry *relundo_list;	/* List of relations with per-relation
									 * UNDO */
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
 * The API is AM-agnostic: callers pass an RM ID, RM-specific info,
 * a relation OID, and an opaque payload.
 *
 * Returns the UndoRecPtr where the record will be inserted (or
 * InvalidUndoRecPtr if undo is disabled).
 */
UndoRecPtr
PrepareXactUndoData(XactUndoContext *ctx, char persistence,
					uint8 rmid, uint16 info, Oid reloid,
					const char *payload, Size payload_len)
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

	/* Add the record to the record set using generic payload API. */
	UndoRecordAddPayload(uset, rmid, info, reloid, payload, payload_len);

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
 * PrepareXactUndoDataParts
 *		Like PrepareXactUndoData, but with scatter-gather payload.
 *
 * Used when the payload is in two non-contiguous pieces (e.g., a fixed
 * header struct followed by variable-length tuple data).  Avoids the
 * need to assemble an intermediate contiguous buffer.
 */
UndoRecPtr
PrepareXactUndoDataParts(XactUndoContext *ctx, char persistence,
						 uint8 rmid, uint16 info, Oid reloid,
						 const char *part1, Size part1_len,
						 const char *part2, Size part2_len)
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
	 * this persistence level.
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

	/* Add the record using scatter-gather payload API. */
	UndoRecordAddPayloadParts(uset, rmid, info, reloid,
							  part1, part1_len, part2, part2_len);

	/*
	 * If this is the first undo for this persistence level in this
	 * subtransaction, record the start location.
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
 * Also updates the transaction-level undo record pointer (undoRecPtr
 * in TransactionState) so that subsequent UNDO records chain correctly.
 */
void
InsertXactUndoData(XactUndoContext *ctx)
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

		/*
		 * Update the per-transaction undo pointer in TransactionState so
		 * that the next UndoRecordSetCreate (if called directly by heap AM
		 * or other subsystems) picks up the correct chain pointer.
		 */
		SetCurrentTransactionUndoRecPtr(ptr);
	}
}

/*
 * CleanupXactUndoInsertion
 *		Clean up after an undo insertion cycle.
 *
 * Resets the record set's buffer position and record count so it can
 * accumulate more records.  Does NOT free the record set -- that
 * happens at transaction end (AtCommit_XactUndo / AtAbort_XactUndo).
 *
 * The record set's prev_undo_ptr is preserved across resets (it was
 * updated by UndoRecordSetInsert), so subsequent records chain
 * correctly through the undo log.
 */
void
CleanupXactUndoInsertion(XactUndoContext *ctx)
{
	if (ctx->uset != NULL)
		UndoRecordSetReset(ctx->uset);
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
 * UNDO pages are managed by shared_buffers and flushed by the
 * checkpointer — no per-commit fdatasync is needed.  We only
 * flush the deferred WAL allocation records so recovery can
 * reconstruct the UNDO log insert pointer.
 *
 * NB: This code MUST NOT FAIL, since it is run as a post-commit step.
 */
void
AtCommit_XactUndo(void)
{
	int			i;

	/*
	 * Always release the recycled UNDO record memory context, even if
	 * XactUndo.has_undo is false.  Some callers (pruneheap.c, nbtree_undo.c)
	 * still call UndoRecordSetCreate/Free directly, which caches a memory
	 * context in UndoRecordReusableContext without setting has_undo.  If we
	 * skip this cleanup, the cached context's parent pointer becomes dangling
	 * after the transaction's memory contexts are destroyed.
	 */
	UndoRecordSetResetCache();

	if (!XactUndo.has_undo)
	{
		/* Flush any deferred WAL even if has_undo is false */
		UndoWalBatchFlush();
		return;
	}

	/*
	 * Flush deferred UNDO allocation WAL records.  With shared_buffers
	 * routing, there's no need for per-commit fdatasync — dirty UNDO
	 * buffers are flushed by the checkpointer like regular heap pages.
	 * We still need to flush the allocation metadata WAL so recovery can
	 * reconstruct the insert pointer.
	 */
	UndoWalBatchFlush();

	/* Sync all dirty UNDO log files to disk before finishing commit. */
	UndoLogSync();

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
 *
 * We close cached UNDO log fds without fsyncing -- on abort, we don't
 * need the UNDO data to be durable (it will be replayed from WAL if
 * needed after a crash).
 */
void
AtAbort_XactUndo(void)
{
	int			i;

	/* Always clean up the recycled context; see AtCommit_XactUndo. */
	UndoRecordSetResetCache();

	if (!XactUndo.has_undo && XactUndo.relundo_list == NULL)
	{
		/* Discard any deferred WAL — nothing committed needs it */
		UndoWalBatchReset();
		return;
	}

	/*
	 * Flush deferred UNDO allocation WAL records before UNDO replay.
	 * If we crash during abort, recovery needs WAL to reconstruct the
	 * UNDO log insert pointer so it can replay the UNDO chain.
	 */
	UndoWalBatchFlush();

	/* Collapse all subtransaction state. */
	CollapseXactUndoSubTransactions();

	/*
	 * Apply per-relation UNDO chains before cleaning up. This must happen
	 * before we reset state so we have the relation list.
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

	/* Close cached UNDO log fds (no fsync needed on abort). */
	UndoLogCloseFiles();

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
 *
 * For per-relation UNDO, we apply the subtransaction's records synchronously
 * by queuing work for the background UNDO worker.  This ensures that tuples
 * inserted/modified by the aborting subtransaction are physically restored
 * before control returns to the caller.
 *
 * The sLog entries for this subtransaction are cleaned up by the
 * SubXactCallback registered in recno_slog.c.
 */
void
AtSubAbort_XactUndo(int level)
{
	XactUndoSubTransaction *subxact = XactUndo.subxact;
	int			i;

	if (subxact == NULL || subxact->nestingLevel != level)
		return;

	/*
	 * Apply per-relation UNDO for records generated during this
	 * subtransaction.  We iterate the record sets and apply records whose
	 * UndoRecPtr is at or after this subtransaction's start_location.
	 *
	 * For each persistence level where this subtransaction generated UNDO
	 * records, queue the work for the per-relation UNDO worker to apply
	 * them synchronously.  The parent transaction's records (before the
	 * subtransaction's start_location) are preserved.
	 */
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		UndoRecPtr	sub_start = subxact->start_location[i];

		if (!UndoRecPtrIsValid(sub_start))
			continue;

		/*
		 * Queue subtransaction UNDO work.  The per-relation UNDO entries
		 * registered during this subtransaction will be applied by the
		 * background worker.  Since this is a subtransaction abort (not a
		 * full transaction abort), we can't use ATM -- the parent
		 * transaction is still running.
		 *
		 * We iterate the relundo_list and queue entries that were
		 * registered at or after this subtransaction's nesting level.
		 * For simplicity, we queue all registered relations -- the UNDO
		 * worker will only apply records in the [sub_start, last_location]
		 * range for each persistence level.
		 */
		if (XactUndo.relundo_list != NULL)
		{
			PerRelUndoEntry *entry;
			TransactionId xid = GetCurrentTransactionIdIfAny();

			for (entry = XactUndo.relundo_list; entry != NULL;
				 entry = entry->next)
			{
				RelUndoQueueAdd(MyDatabaseId, entry->relid,
								entry->start_urec_ptr, xid);
			}

			StartRelUndoWorker(MyDatabaseId);

			elog(DEBUG1,
				 "AtSubAbort_XactUndo: applied sync UNDO for subtransaction level %d",
				 level);
		}

		/*
		 * Reset the last_location to what it was before this subtransaction,
		 * so that if the parent transaction continues and then aborts, only
		 * the parent's records are applied (the subtransaction's records
		 * have already been applied).
		 */
		if (subxact->next != NULL &&
			UndoRecPtrIsValid(subxact->next->start_location[i]))
			XactUndo.last_location[i] = subxact->next->start_location[i];
	}

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

	/* Close any cached UNDO log fds before process exit. */
	UndoLogCloseFiles();

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

	/*
	 * Add new entry to the list. Use CurTransactionContext for proper
	 * cleanup.
	 */
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
 * EstimatePerRelUndoBytes
 *		Estimate total UNDO bytes for all registered per-relation UNDO entries.
 *
 * Since the per-relation UNDO chain records are variable-length, we use a
 * conservative estimate based on the number of entries and the average
 * per-relation UNDO record size.  Each PerRelUndoEntry corresponds to at
 * least one UNDO record header plus payload.
 */
static Size
EstimatePerRelUndoBytes(void)
{
	PerRelUndoEntry *entry;
	int			nentries = 0;

	for (entry = XactUndo.relundo_list; entry != NULL; entry = entry->next)
		nentries++;

	/*
	 * Estimate: each per-relation UNDO entry implies at least one UNDO record
	 * (header + payload).  Use 256 bytes as a conservative average per entry.
	 */
	return (Size) nentries * 256;
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
 * For small transactions (below undo_instant_abort_threshold bytes),
 * we queue work for background UNDO workers that apply the UNDO chains
 * synchronously.  For large transactions (at or above the threshold),
 * we use ATM-style instant abort: the transaction's XID is added to the
 * Aborted Transaction Map and the Logical Revert background worker
 * applies the UNDO chain asynchronously, making ROLLBACK O(1).
 *
 * When undo_instant_abort_threshold is 0, ATM instant abort is always
 * used (the default behavior for CTR).
 */
static void
ApplyPerRelUndo(void)
{
	PerRelUndoEntry *entry;
	TransactionId xid = GetCurrentTransactionIdIfAny();
	bool		need_worker = false;
	bool		use_atm;
	Size		estimated_bytes;

	if (XactUndo.relundo_list == NULL)
	{
		elog(DEBUG1, "ApplyPerRelUndo: no per-relation UNDO to apply");
		return;
	}

	/*
	 * Decide whether to use ATM instant abort or synchronous rollback based
	 * on the estimated UNDO size and the threshold GUC.
	 *
	 * threshold == 0 means always use ATM (instant abort for all sizes).
	 */
	estimated_bytes = EstimatePerRelUndoBytes();
	use_atm = (undo_instant_abort_threshold == 0 ||
			   (int) estimated_bytes >= undo_instant_abort_threshold);

	elog(LOG, "ApplyPerRelUndo: processing per-relation UNDO entries "
		 "(estimated %zu bytes, threshold %d, %s)",
		 estimated_bytes, undo_instant_abort_threshold,
		 use_atm ? "ATM instant abort" : "sync rollback");

	for (entry = XactUndo.relundo_list; entry != NULL; entry = entry->next)
	{
		if (use_atm)
		{
			/*
			 * ATM-style instant abort.  ATMAddAborted() writes a WAL record
			 * and adds an entry to the shared-memory Aborted Transaction Map.
			 * The Logical Revert background worker will apply the UNDO chain
			 * asynchronously, making ROLLBACK O(1).
			 */
			if (ATMAddAborted(xid, MyDatabaseId, entry->relid,
							  entry->start_urec_ptr))
			{
				elog(DEBUG1,
					 "ATM: deferred rollback for relation %u (UNDO ptr %lu)",
					 entry->relid,
					 (unsigned long) entry->start_urec_ptr);
				continue;
			}

			/*
			 * ATM is full -- fall back to queuing work for the synchronous
			 * per-relation UNDO worker.
			 */
			elog(LOG,
				 "ATM full, queuing UNDO work: database %u, relation %u, "
				 "UNDO ptr %lu",
				 MyDatabaseId, entry->relid,
				 (unsigned long) entry->start_urec_ptr);
		}

		/*
		 * Synchronous rollback path: queue work for the background per-relation
		 * UNDO worker.  Used for small transactions (below threshold) or as a
		 * fallback when the ATM is full.
		 */
		RelUndoQueueAdd(MyDatabaseId, entry->relid,
						entry->start_urec_ptr, xid);
		need_worker = true;
	}

	if (need_worker)
		StartRelUndoWorker(MyDatabaseId);
}
