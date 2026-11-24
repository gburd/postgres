/*-------------------------------------------------------------------------
 *
 * xactundo.c
 *	  Management of undo record sets for transactions
 *
 * Undo records that need to be applied after a transaction or
 * subtransaction abort should be inserted using the functions defined
 * in this file; every consumer that wants undo for post-abort cleanup should
 * use these interfaces.
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
 * All of a transaction's UNDO records at one persistence level go into the
 * same UndoRecordSet in the order they were written, whatever objects they
 * describe, and rollback applies them newest first.
 *
 * SUBTRANSACTION TRACKING:
 *
 * Subtransaction state is tracked using a dynamically-grown array allocated
 * in TopMemoryContext.  The array starts at INITIAL_SUBXACT_CAPACITY (64)
 * slots and doubles when needed via repalloc().  The array persists across
 * transactions within the same backend to avoid repeated allocation for
 * steady-state workloads.  It lives in TopMemoryContext rather than
 * CurTransactionContext so that growing it in a SubXactCallback allocates
 * nothing per subtransaction.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/xactundo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "access/undo.h"
#include "access/undo_xlog.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/xact.h"
#include "access/xactundo.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xloginsert.h"
#include "catalog/pg_class.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/injection_point.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Initial capacity for the dynamically-grown subtransaction stack.
 * Covers 99.9% of workloads without needing reallocation.  The stack
 * doubles when needed, so there is no artificial upper limit.
 */
#define INITIAL_SUBXACT_CAPACITY	64

/* Per-subtransaction backend-private undo state (array element). */
typedef struct XactUndoSubTransactionState
{
	SubTransactionId nestingLevel;
	UndoRecPtr	start_location[NUndoPersistenceLevels];

	/*
	 * Snapshot of the parent's last_batch_lsn at the time this subtransaction
	 * started.  On subtransaction abort, after applying this subtransaction's
	 * UNDO chain, we restore XactUndo.last_batch_lsn to these saved values so
	 * the parent's subsequent abort (or further subtransactions) only covers
	 * the parent's own records and does not double-apply already-reversed
	 * batches.
	 */
	XLogRecPtr	last_batch_lsn[NUndoPersistenceLevels];
} XactUndoSubTransactionState;

/* Backend-private undo state. */
typedef struct XactUndoData
{
	bool		has_undo;		/* has this xact generated any undo? */
	int			subxact_depth;	/* 0 = top-level, 1+ = savepoints */
	int			subxact_capacity;	/* allocated slots in subxact_stack */

	/* Dynamically-grown subtransaction stack (TopMemoryContext). */
	XactUndoSubTransactionState *subxact_stack;

	/*
	 * Per-persistence-level record sets. These are created lazily on first
	 * use and destroyed at transaction end.
	 */
	UndoRecordSet *record_set[NUndoPersistenceLevels];

	/* Tracking for the most recent undo insertion per persistence level. */
	UndoRecPtr	last_location[NUndoPersistenceLevels];

	/*
	 * WAL-based UNDO chain heads.  When UNDO records are routed through WAL
	 * via XLOG_UNDO_BATCH, this tracks the LSN of the most recent batch per
	 * persistence level.  Used for rollback chain walking.
	 */
	XLogRecPtr	last_batch_lsn[NUndoPersistenceLevels];
} XactUndoData;

static XactUndoData XactUndo;
static bool subxact_callback_registered = false;

/*
 * Per-backend copy of the current transaction's UNDO chain heads, for the
 * checkpointer (see LogXactUndoInflight).  Only the owning backend writes its
 * slot, under the slot's spinlock; xid is invalid when it has none.
 */
typedef struct XactUndoInflightSlot
{
	slock_t		mutex;
	xl_undo_inflight_xact chain;
} XactUndoInflightSlot;

static XactUndoInflightSlot *XactUndoInflight = NULL;

static void PublishXactUndoChain(TransactionId xid);

/*
 * Compile-time guard: xl_xact_prepare.last_batch_lsn[3] must match
 * NUndoPersistenceLevels.  Both headers are available here; xact.h avoids
 * including undodefs.h to keep its include footprint minimal.
 */
StaticAssertDecl(NUndoPersistenceLevels == 3,
				 "xl_xact_prepare.last_batch_lsn array size (3) must match NUndoPersistenceLevels");

static void ResetXactUndo(void);
static void CollapseXactUndoSubTransactions(void);
static UndoPersistenceLevel GetUndoPersistenceLevel(char relpersistence);
static void EnsureSubxactStackCapacity(void);
static void XactUndo_SubXactCallback(SubXactEvent event, SubTransactionId mySubid,
									 SubTransactionId parentSubid, void *arg);

/* Convenience macro: pointer to current subtransaction state. */
#define CURRENT_SUBXACT() (&XactUndo.subxact_stack[XactUndo.subxact_depth])

/*
 * XactUndoShmemSize
 *		How much shared memory do we need for transaction undo state?
 *
 * One XactUndoInflightSlot per backend.
 */
Size
XactUndoShmemSize(void)
{
	return mul_size(MaxBackends, sizeof(XactUndoInflightSlot));
}

/*
 * XactUndoShmemRequest
 *		Register the in-flight chain slots with the shared memory framework.
 */
void
XactUndoShmemRequest(void)
{
	ShmemRequestStruct(.name = "UNDO In-flight Chains",
					   .size = XactUndoShmemSize(),
					   .ptr = (void **) &XactUndoInflight);
}

/*
 * XactUndoShmemInit
 *		Initialize shared memory for transaction undo state.
 */
void
XactUndoShmemInit(void)
{
	for (int i = 0; i < MaxBackends; i++)
	{
		SpinLockInit(&XactUndoInflight[i].mutex);
		XactUndoInflight[i].chain.xid = InvalidTransactionId;
	}
}

/*
 * PublishXactUndoChain
 *		Make this backend's chain heads visible to LogXactUndoInflight().
 *
 * xid invalid clears the slot.  Called in critical sections: no palloc.
 */
static void
PublishXactUndoChain(TransactionId xid)
{
	XactUndoInflightSlot *slot;

	if (XactUndoInflight == NULL || MyProcNumber < 0 ||
		MyProcNumber >= MaxBackends)
		return;
	slot = &XactUndoInflight[MyProcNumber];
	if (!TransactionIdIsValid(xid) && !TransactionIdIsValid(slot->chain.xid))
		return;

	SpinLockAcquire(&slot->mutex);
	slot->chain.xid = xid;
	for (int i = 0; i < NUndoPersistenceLevels; i++)
		slot->chain.last_batch_lsn[i] = XactUndo.last_batch_lsn[i];
	SpinLockRelease(&slot->mutex);
}

/*
 * LogXactUndoInflight
 *		WAL-log the UNDO chains of every transaction in flight.
 *
 * Called by the checkpointer after the checkpoint's redo pointer has been
 * determined.  Redo starts at that pointer and so does not see the batches a
 * transaction wrote before it; this record tells recovery about them.  A
 * transaction that commits or aborts after we read its slot is still listed;
 * its commit or abort record, which follows this one, removes the entry
 * again during redo.
 */
void
LogXactUndoInflight(void)
{
	xl_undo_inflight *rec;
	uint32		n = 0;

	if (XactUndoInflight == NULL)
		return;

	rec = palloc(SizeOfUndoInflight +
				 MaxBackends * sizeof(xl_undo_inflight_xact));
	for (int i = 0; i < MaxBackends; i++)
	{
		XactUndoInflightSlot *slot = &XactUndoInflight[i];

		SpinLockAcquire(&slot->mutex);
		if (TransactionIdIsValid(slot->chain.xid))
			rec->xacts[n++] = slot->chain;
		SpinLockRelease(&slot->mutex);
	}

	/* Unimportant, so that an idle system still skips checkpoints. */
	if (n > 0)
	{
		rec->nxacts = n;
		XLogBeginInsert();
		XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT);
		XLogRegisterData(rec, SizeOfUndoInflight +
						 n * sizeof(xl_undo_inflight_xact));
		(void) XLogInsert(RM_UNDO_ID, XLOG_UNDO_INFLIGHT);
	}
	pfree(rec);
}

/*
 * InitializeXactUndo
 *		Per-backend initialization for transaction undo.
 */
void
InitializeXactUndo(void)
{
	/* Ensure the dynamic subxact stack is allocated */
	EnsureSubxactStackCapacity();

	ResetXactUndo();

	/*
	 * Register callback to track subtransaction lifecycle. Do this lazily on
	 * first transaction to ensure it's registered for the backend that will
	 * actually use UNDO.
	 */
	if (!subxact_callback_registered)
	{
		RegisterSubXactCallback(XactUndo_SubXactCallback, NULL);
		subxact_callback_registered = true;
	}
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
	UndoRecordSet *uset;
	XactUndoSubTransactionState *cur;
	UndoRecPtr *sub_start_location;

	/* Remember that we've done something undo-related. */
	XactUndo.has_undo = true;

	/* The current (sub)transaction must have an xid; see below. */
	(void) GetCurrentTransactionId();

	/*
	 * Load the UndoRecordSetInsert() injection points into this backend's
	 * local cache now, while we are guaranteed not to be inside a critical
	 * section (PrepareXactUndoData may itself palloc).  UndoRecordSetInsert()
	 * runs inside the caller's crit section and can only fire these via the
	 * palloc-free INJECTION_POINT_CACHED variant.
	 */
	INJECTION_POINT_LOAD("undo-batch-before-wal-insert");
	INJECTION_POINT_LOAD("undo-batch-after-wal-insert");

	/*
	 * If we've entered a subtransaction deeper than what's currently tracked,
	 * push a new entry onto the subxact_stack.  This handles the case where
	 * PrepareXactUndoData is called for the first time in a subtransaction
	 * that was started before the SubXactCallback fired (e.g., if the
	 * callback hadn't been registered yet when the subtransaction began).
	 */
	cur = CURRENT_SUBXACT();
	if (nestingLevel > (int) cur->nestingLevel)
	{
		int			i;

		XactUndo.subxact_depth++;
		EnsureSubxactStackCapacity();

		cur = CURRENT_SUBXACT();
		cur->nestingLevel = nestingLevel;
		for (i = 0; i < NUndoPersistenceLevels; ++i)
		{
			cur->start_location[i] = InvalidUndoRecPtr;
			cur->last_batch_lsn[i] = XactUndo.last_batch_lsn[i];
		}
	}

	/*
	 * Make sure we have an UndoRecordSet of the appropriate type open for
	 * this persistence level.  These record sets are always associated with
	 * the toplevel transaction, not a subtransaction, to avoid fragmentation.
	 *
	 * The set, and so every batch it writes, is stamped with the top-level
	 * xid even when the first record comes from a subtransaction.  Crash
	 * recovery tracks loser chains by that xid and forgets them at the top
	 * level's commit or abort record; a subxid stamp would outlive the commit
	 * and get a committed transaction's UNDO applied.
	 */
	uset = XactUndo.record_set[plevel];
	if (uset == NULL)
	{
		uset = UndoRecordSetCreate(GetTopTransactionId());
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
	sub_start_location = &cur->start_location[plevel];
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
InsertXactUndoData(XactUndoContext *ctx)
{
	UndoRecordSet *uset = ctx->uset;
	UndoRecPtr	ptr;
	int			saved_delay = 0;

	Assert(uset != NULL);

	/*
	 * Hold off a checkpoint's redo-pointer phase while the batch is inserted
	 * and published for LogXactUndoInflight(): a batch that precedes the redo
	 * pointer must be visible to the checkpointer when it reads the slots.
	 */
	if (MyProc != NULL)
	{
		saved_delay = MyProc->delayChkptFlags;
		MyProc->delayChkptFlags |= DELAY_CHKPT_START;
	}

	ptr = UndoRecordSetInsert(uset);
	if (UndoRecPtrIsValid(ptr))
	{
		XactUndoSubTransactionState *cur = CURRENT_SUBXACT();

		XactUndo.last_location[ctx->plevel] = ptr;

		/*
		 * Track the WAL LSN of the most recent UNDO batch for this
		 * persistence level.  This is used during rollback to walk the UNDO
		 * chain backward through WAL.
		 *
		 * The pointer must be a valid LSN; the caller must have produced an
		 * RM_UNDO_ID XLOG_UNDO_BATCH record via UndoRecordSetInsert.  We do
		 * not validate the rmid here because UndoValidateBatchLSN reads from
		 * the on-disk WAL via read_local_xlog_page, which waits on flush and
		 * cannot be safely called from the WAL-insertion hot path.  The
		 * rollback path at AtAbort_XactUndo validates before applying.
		 */
		Assert(XLogRecPtrIsValid(uset->last_batch_lsn));
		XactUndo.last_batch_lsn[ctx->plevel] = uset->last_batch_lsn;
		PublishXactUndoChain(uset->xid);

		/* Fix up subtransaction start location if needed */
		if (cur->start_location[ctx->plevel] == (UndoRecPtr) 1)
			cur->start_location[ctx->plevel] = ptr;
	}
	else
	{
		XactUndoSubTransactionState *cur = CURRENT_SUBXACT();

		/*
		 * The batch was empty or elided, so no record pointer exists to fix
		 * up the sentinel with.  Leaving the (UndoRecPtr) 1 sentinel in place
		 * would make UndoRecPtrIsValid() treat it as a real start location
		 * and leak it into subxact->parent merges and rollback chain walks;
		 * reset it so this level records no start location.
		 */
		if (cur->start_location[ctx->plevel] == (UndoRecPtr) 1)
			cur->start_location[ctx->plevel] = InvalidUndoRecPtr;
	}

	if (MyProc != NULL)
		MyProc->delayChkptFlags = saved_delay;
}

/*
 * CleanupXactUndoInsertion
 *		Clean up after an undo insertion cycle.
 *
 * Resets the record set's buffer position and record count so it can
 * accumulate more records.  Does NOT free the record set -- that
 * happens at transaction end (AtCommit_XactUndo / AtAbort_XactUndo).
 */
void
CleanupXactUndoInsertion(XactUndoContext *ctx)
{
	if (ctx->uset != NULL)
		UndoRecordSetReset(ctx->uset);
}

/*
 * GetCurrentXactLastBatchLSN
 *		Get the WAL LSN of the most recent UNDO batch for a persistence level.
 *
 * Used during transaction abort to start the WAL-based UNDO chain walk.
 */
XLogRecPtr
GetCurrentXactLastBatchLSN(UndoPersistenceLevel plevel)
{
	return XactUndo.last_batch_lsn[plevel];
}

/*
 * AtCommit_XactUndo
 *		Post-commit cleanup of the undo state.
 *
 * On commit, undo records are no longer needed for rollback.
 * Free all record sets and reset state.
 *
 * The records are already in WAL, made durable by the commit record's
 * flush, so nothing needs writing here.
 *
 * NB: This code MUST NOT FAIL, since it is run as a post-commit step.
 */
void
AtCommit_XactUndo(void)
{
	int			i;


	if (!XactUndo.has_undo)
		return;

	/*
	 * Free all per-persistence-level record sets.
	 *
	 * We can safely call UndoRecordSetFree() during commit because we're in
	 * CurTransactionContext, not BumpContext (which is only used during
	 * abort). The record sets are allocated in CurTransactionContext and will
	 * be freed when that context is destroyed at transaction end.
	 */
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		if (XactUndo.record_set[i] != NULL)
		{
			UndoRecordSetFree(XactUndo.record_set[i]);
			XactUndo.record_set[i] = NULL;
		}
	}

	/* Release WAL retention hold acquired in UndoRecordSetInsert(). */
	UndoClearBatchLSN();

	ResetXactUndo();
}

/*
 * AtPrepare_XactUndo
 *		End-of-PREPARE cleanup of the backend-private undo state.
 *
 * PREPARE TRANSACTION hands ownership of this transaction's cluster-wide UNDO
 * to the 2PC state: the permanent chain-head LSN was durably saved in
 * xl_xact_prepare (twophase.c gxact->undo_batch_lsn), its WAL is pinned while
 * the xact stays prepared, and ROLLBACK PREPARED applies that LSN
 * synchronously via ApplyUndoChainFromWAL().
 *
 * The in-memory UndoRecordSets, however, live in TopTransactionContext, which
 * PrepareTransaction() destroys (AtCommit_Memory) right after this.  We must
 * therefore drop the backend-private references to them here, or the dangling
 * XactUndo.record_set[] pointers get freed at backend exit (AtProcExit_XactUndo
 * -> UndoRecordSetFree on already-freed memory) and crash.  We only NULL the
 * pointers (via ResetXactUndo); the record sets themselves are freed by
 * TopTransactionContext destruction.
 *
 * Unlike AtCommit_XactUndo, we do NOT call UndoClearBatchLSN(): the WAL
 * retention hold must persist for as long as the transaction stays prepared.
 */
void
AtPrepare_XactUndo(void)
{
	/*
	 * Drop the backend-private record-set references (memory freed with
	 * TopTransactionContext); keep the WAL retention hold.
	 */
	ResetXactUndo();
}

/*
 * AtAbort_XactUndo
 *		Post-abort cleanup of the undo state.
 *
 * Apply the transaction's UNDO chain and clean up the record sets.  The
 * batches are already in WAL, so a crash during the rollback leaves them for
 * the recovery undo phase.
 */
void
AtAbort_XactUndo(void)
{
	int			i;
	bool		lsn_safely_held = false;	/* true once inline UNDO completed */


	/* Always clean up the recycled context; see AtCommit_XactUndo. */
	UndoRecordSetResetCache();

	if (!XactUndo.has_undo)
		return;

	INJECTION_POINT("undo-xact-abort-before-apply", NULL);

	/* Collapse all subtransaction state. */
	CollapseXactUndoSubTransactions();

	/*
	 * UNDO application strategy: apply the transaction's cluster-wide UNDO
	 * chain synchronously in this backend, here at abort.
	 *
	 * The BumpContext issue (pfree crashes during abort) is avoided by: -
	 * Creating a temporary AllocSetContext for inline UNDO application -
	 * ApplyUndoChainFromWAL already avoids pfree on its allocations -
	 * Switching back to the abort context afterward
	 */
	{
		XLogRecPtr	perm_lsn =
			XactUndo.last_batch_lsn[UNDOPERSISTENCE_PERMANENT];

		if (XLogRecPtrIsValid(perm_lsn))
		{
			/*
			 * The UNDO batch records were inserted into the WAL buffers
			 * during this transaction but may not yet be flushed to disk. The
			 * inline apply path reads them back via the local XLog reader,
			 * which would otherwise busy-wait (pg_usleep) for the walwriter
			 * to flush past the batch LSN -- adding multi-second latency to
			 * every rollback. Flush WAL up to the end of the last record this
			 * backend wrote so the batch is immediately readable. We must not
			 * flush to GetXLogInsertRecPtr(): under concurrency that global
			 * insert position can point into the middle of a record another
			 * backend has reserved but not finished copying, so XLogFlush
			 * would fail with "xlog flush request is not satisfied".
			 * XactLastRecEnd is always a valid record boundary and, since
			 * this backend wrote the UNDO batch, is guaranteed to be at or
			 * past the batch end. This mirrors the commit-path flush in
			 * RecordTransactionCommit.
			 */
			XLogFlush(XactLastRecEnd);

			{
				/*
				 * Apply UNDO inline.  Use a dedicated AllocSetContext to
				 * avoid BumpContext pfree issues.
				 */
				MemoryContext undo_ctx;
				MemoryContext old_ctx;
				int			saved_trans_state;
				bool		undo_applied = false;
				bool		valid_batch;

				undo_ctx = AllocSetContextCreate(TopMemoryContext,
												 "Inline UNDO Apply",
												 ALLOCSET_DEFAULT_SIZES);
				old_ctx = MemoryContextSwitchTo(undo_ctx);

				/*
				 * Validate the batch LSN points to an actual UNDO record
				 * before attempting inline application.  Stale LSNs from
				 * chain_prev tracking anomalies can point to non-UNDO WAL
				 * records, which would cause "not an UNDO batch" warnings.
				 */
				valid_batch = UndoValidateBatchLSN(perm_lsn);
#ifdef USE_INJECTION_POINTS
				/* Force the invalid-batch early exit for testing. */
				if (IS_INJECTION_POINT_ATTACHED("inline-undo-invalid-batch"))
					valid_batch = false;
#endif
				if (!valid_batch)
				{
					elog(DEBUG1, "inline UNDO: last_batch_lsn %X/%08X is not "
						 "a valid UNDO batch",
						 LSN_FORMAT_ARGS(perm_lsn));
					undo_applied = false;
					MemoryContextSwitchTo(old_ctx);
					MemoryContextDelete(undo_ctx);
					goto inline_undo_done;
				}

				/*
				 * AbortTransaction() has already advanced the transaction
				 * state to TRANS_ABORT, but the relcache, locks, and resource
				 * owner are all still live.  UNDO appliers open relations,
				 * which asserts IsTransactionState(); temporarily present
				 * TRANS_INPROGRESS for the duration of the inline apply and
				 * always restore the real state afterward.
				 */
				saved_trans_state = EnterInlineUndoApplyState();
				PG_TRY();
				{
					undo_applied = ApplyUndoChainFromWAL(perm_lsn);
				}
				PG_CATCH();
				{
					/*
					 * If inline UNDO throws an error, release any content
					 * locks the failed chain left held before unwinding.
					 */
					BufferLockReleaseAll();
					LeaveInlineUndoApplyState(saved_trans_state);
					FlushErrorState();
					undo_applied = false;
				}
				PG_END_TRY();
				LeaveInlineUndoApplyState(saved_trans_state);

				MemoryContextSwitchTo(old_ctx);
				MemoryContextDelete(undo_ctx);

		inline_undo_done:
				if (!undo_applied)
				{
					/*
					 * Inline UNDO failed (WAL recycled, wrong record type, or
					 * chain walk aborted).  Retain the UNDO WAL retention
					 * slot so the WAL segment holding our UNDO data is not
					 * recycled; the slot is cleared at backend exit.
					 *
					 * The error that is aborting us may have been raised
					 * while this backend held buffer content locks, which
					 * resource-owner cleanup releases only later.  The
					 * appliers lock pages, possibly those very ones, so
					 * release the stragglers first.  LWLockReleaseAll does
					 * not cover them.
					 */
					BufferLockReleaseAll();
					elog(WARNING, "inline UNDO failed for xid %u",
						 GetCurrentTransactionId());
				}
				else
				{
					lsn_safely_held = true; /* UNDO fully applied, WAL can be
											 * recycled */
					ereport(DEBUG2,
							(errmsg("inline UNDO applied for xid %u",
									GetCurrentTransactionId())));
				}
			}
		}
	}

	INJECTION_POINT("undo-xact-abort-after-apply", NULL);

	/* Free all per-persistence-level record sets. */
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		if (XactUndo.record_set[i] != NULL)
		{
			UndoRecordSetFree(XactUndo.record_set[i]);
			XactUndo.record_set[i] = NULL;
		}
	}

	/*
	 * Release the WAL retention hold if inline UNDO completed: nothing needs
	 * the UNDO records any more.
	 *
	 * Otherwise the slot keeps the hold, but only until this backend's next
	 * transaction commits or aborts, or the backend exits; both clear it.
	 * Nothing re-applies the chain in the meantime.  The abort record is
	 * already written, so crash recovery does not treat the transaction as a
	 * loser either.
	 */
	if (lsn_safely_held)
		UndoClearBatchLSN();
	else
		elog(DEBUG1, "retaining UNDO WAL retention slot (inline UNDO failed)");

	ResetXactUndo();
}

/*
 * AtSubCommit_XactUndo
 *		Subtransaction commit: merge sub undo state into parent.
 */
void
AtSubCommit_XactUndo(int level)
{
	XactUndoSubTransactionState *cur;
	XactUndoSubTransactionState *parent;
	int			i;


	if (XactUndo.subxact_depth <= 0)
		return;

	cur = CURRENT_SUBXACT();
	if ((int) cur->nestingLevel != level)
		return;

	parent = &XactUndo.subxact_stack[XactUndo.subxact_depth - 1];

	/*
	 * Merge start locations into parent.
	 *
	 * Invariant: all UNDO records for this transaction, regardless of nesting
	 * level, are stored in a single chain per persistence level (one
	 * UndoRecordSet).  start_location tracks the earliest record the
	 * subtransaction generated.  Since records are strictly append-only, the
	 * parent's start location is always earlier than the subtransaction's if
	 * it exists.  We only update the parent's start when it is not yet set
	 * (the parent wrote no UNDO before this subtransaction).
	 */
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		if (UndoRecPtrIsValid(cur->start_location[i]) &&
			!UndoRecPtrIsValid(parent->start_location[i]))
		{
			parent->start_location[i] = cur->start_location[i];
		}
	}

	XactUndo.subxact_depth--;
}

/*
 * AtSubAbort_XactUndo
 *		Subtransaction abort: apply undo for this sub-level, clean up.
 *
 * We apply the batches the subtransaction wrote synchronously, before
 * control returns to the caller; the parent's batches are left alone.
 */
void
AtSubAbort_XactUndo(int level)
{
	XactUndoSubTransactionState *cur;
	XactUndoSubTransactionState *parent;
	int			i;


	if (XactUndo.subxact_depth <= 0)
		return;

	cur = CURRENT_SUBXACT();
	if ((int) cur->nestingLevel != level)
		return;

	parent = &XactUndo.subxact_stack[XactUndo.subxact_depth - 1];

	/*
	 * For each persistence level where this subtransaction wrote UNDO, apply
	 * the batches newer than the parent's chain head; the parent's records
	 * are preserved.
	 */
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		UndoRecPtr	sub_start = cur->start_location[i];

		if (!UndoRecPtrIsValid(sub_start))
			continue;

		/*
		 * Eagerly roll back the cluster-wide UNDO this subtransaction wrote.
		 *
		 * If the subtransaction advanced XactUndo.last_batch_lsn[i] beyond
		 * the value saved at subtransaction start, it wrote UNDO batches that
		 * must now be reversed.  We must apply them here rather than leave
		 * them to the top-level abort: a subtransaction can abort while its
		 * parent goes on to COMMIT, in which case the top level never aborts
		 * and the UNDO would never run, leaking the aborted subtransaction's
		 * writes as if committed.
		 *
		 * Apply only the batches strictly newer than the parent's saved head
		 * (cur->last_batch_lsn[i]) via the bounded chain walk, so the
		 * parent's and earlier subtransactions' batches are preserved.
		 * Afterward restore last_batch_lsn[i] to the parent's value so a
		 * later parent abort walks only the parent's batches and does not
		 * double-apply ours.
		 *
		 * BumpContext safety: subtransaction abort may run under a
		 * BumpContext that does not support pfree(), so apply inside a
		 * dedicated AllocSetContext and present an in-progress transaction
		 * state for the relation opens, mirroring the inline path in
		 * AtAbort_XactUndo().
		 */
		if (XLogRecPtrIsValid(XactUndo.last_batch_lsn[i]) &&
			XactUndo.last_batch_lsn[i] != cur->last_batch_lsn[i])
		{
			XLogRecPtr	sub_head = XactUndo.last_batch_lsn[i];
			XLogRecPtr	parent_head = cur->last_batch_lsn[i];

			/*
			 * Invariant: the current head must be strictly newer (larger)
			 * than the parent's saved head.  If this fires, a sibling
			 * subtransaction improperly modified last_batch_lsn after its own
			 * abort, which would cause us to skip or double-apply batches.
			 */
			Assert(!XLogRecPtrIsValid(parent_head) ||
				   sub_head > parent_head);

			/*
			 * Flush WAL so the subtransaction's batches are readable by the
			 * local XLog reader without busy-waiting on the walwriter.  See
			 * the matching flush in AtAbort_XactUndo() for why XactLastRecEnd
			 * is the correct, always-valid record boundary to flush to.
			 */
			XLogFlush(XactLastRecEnd);

			if (UndoValidateBatchLSN(sub_head))
			{
				MemoryContext undo_ctx;
				MemoryContext old_ctx;
				int			saved_trans_state;

				undo_ctx = AllocSetContextCreate(TopMemoryContext,
												 "Subxact UNDO Apply",
												 ALLOCSET_DEFAULT_SIZES);
				old_ctx = MemoryContextSwitchTo(undo_ctx);

				/*
				 * Release buffer content locks the failed subtransaction may
				 * still hold -- an error raised mid-split leaves both halves
				 * locked -- before the appliers lock pages again.  See
				 * AtAbort_XactUndo.
				 */
				BufferLockReleaseAll();
				saved_trans_state = EnterInlineUndoApplyState();
				PG_TRY();
				{
					ApplyUndoChainFromWALBounded(sub_head, parent_head);
				}
				PG_CATCH();
				{
					/*
					 * Release any content locks the failed subxact UNDO chain
					 * left held before unwinding.
					 */
					BufferLockReleaseAll();
					LeaveInlineUndoApplyState(saved_trans_state);
					FlushErrorState();
				}
				PG_END_TRY();
				LeaveInlineUndoApplyState(saved_trans_state);

				MemoryContextSwitchTo(old_ctx);
				MemoryContextDelete(undo_ctx);
			}
			else
				elog(DEBUG1, "subxact UNDO: head %X/%08X is not a valid UNDO "
					 "batch, skipping eager rollback",
					 LSN_FORMAT_ARGS(sub_head));

			XactUndo.last_batch_lsn[i] = parent_head;
			PublishXactUndoChain(GetTopTransactionIdIfAny());
		}

		/*
		 * Reset the last_location to what it was before this subtransaction,
		 * so that if the parent transaction continues and then aborts, only
		 * the parent's records are applied (the subtransaction's records have
		 * already been applied).
		 */
		if (UndoRecPtrIsValid(parent->start_location[i]))
			XactUndo.last_location[i] = parent->start_location[i];
	}

	XactUndo.subxact_depth--;
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

	/* Release any WAL retention hold (in case process exits mid-transaction). */
	UndoClearBatchLSN();

	ResetXactUndo();
}

/*
 * XactUndo_SubXactCallback
 *		Subtransaction callback to manage UNDO subtransaction state.
 *
 * This ensures the UNDO subsystem properly tracks all subtransactions,
 * including those created by ROLLBACK TO SAVEPOINT.
 *
 * The subtransaction state is stored in a dynamically-grown array in
 * TopMemoryContext.  In the common case (depth < capacity), no allocation
 * occurs.  Growth via repalloc is safe here because it operates on a
 * TopMemoryContext allocation, not a per-subtransaction context.
 */
static void
XactUndo_SubXactCallback(SubXactEvent event, SubTransactionId mySubid,
						 SubTransactionId parentSubid, void *arg)
{
	int			i;

	/* These parameters are mandated by the callback signature. */
	(void) parentSubid;
	(void) arg;

	switch (event)
	{
		case SUBXACT_EVENT_START_SUB:


			/*
			 * A new subtransaction is starting.  Push an entry onto the
			 * dynamically-grown stack, extending it if needed.
			 */
			XactUndo.subxact_depth++;
			EnsureSubxactStackCapacity();
			{
				XactUndoSubTransactionState *s = CURRENT_SUBXACT();

				s->nestingLevel = mySubid;
				for (i = 0; i < NUndoPersistenceLevels; ++i)
				{
					s->start_location[i] = InvalidUndoRecPtr;
					/* Save parent's last_batch_lsn for restore on abort */
					s->last_batch_lsn[i] = XactUndo.last_batch_lsn[i];
				}
			}
			break;

		case SUBXACT_EVENT_COMMIT_SUB:

			/*
			 * Subtransaction is committing. Merge its UNDO state into parent.
			 */
			AtSubCommit_XactUndo(mySubid);
			break;

		case SUBXACT_EVENT_ABORT_SUB:

			/*
			 * Subtransaction is aborting. Apply UNDO and clean up.
			 */
			AtSubAbort_XactUndo(mySubid);
			break;

		case SUBXACT_EVENT_PRE_COMMIT_SUB:
			/* Nothing to do at pre-commit */
			break;
	}
}

/*
 * EnsureSubxactStackCapacity
 *		Ensure the subxact_stack has room for the current depth.
 *
 * If the stack hasn't been allocated yet, allocate it with the initial
 * capacity.  If we've exceeded the current capacity, double it.
 * The stack lives in TopMemoryContext so it persists across transactions
 * within the same backend.
 */
static void
EnsureSubxactStackCapacity(void)
{
	if (XactUndo.subxact_stack == NULL)
	{
		/* First-time allocation */
		XactUndo.subxact_capacity = INITIAL_SUBXACT_CAPACITY;
		XactUndo.subxact_stack = (XactUndoSubTransactionState *)
			MemoryContextAllocZero(TopMemoryContext,
								   XactUndo.subxact_capacity *
								   sizeof(XactUndoSubTransactionState));
	}
	else if (XactUndo.subxact_depth >= XactUndo.subxact_capacity)
	{
		int			new_capacity = XactUndo.subxact_capacity * 2;

		XactUndo.subxact_stack = (XactUndoSubTransactionState *)
			repalloc(XactUndo.subxact_stack,
					 new_capacity * sizeof(XactUndoSubTransactionState));
		/* Zero the newly-allocated portion */
		memset(&XactUndo.subxact_stack[XactUndo.subxact_capacity], 0,
			   (new_capacity - XactUndo.subxact_capacity) *
			   sizeof(XactUndoSubTransactionState));
		XactUndo.subxact_capacity = new_capacity;
	}
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
	XactUndo.subxact_depth = 0;

	/*
	 * The subxact_stack allocation persists across transactions (it's in
	 * TopMemoryContext).  We just reset the depth and initialize slot 0.
	 */
	if (XactUndo.subxact_stack != NULL)
	{
		XactUndo.subxact_stack[0].nestingLevel = 1;
		for (i = 0; i < NUndoPersistenceLevels; i++)
		{
			XactUndo.subxact_stack[0].start_location[i] = InvalidUndoRecPtr;
			XactUndo.subxact_stack[0].last_batch_lsn[i] = InvalidXLogRecPtr;
		}
	}

	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		XactUndo.record_set[i] = NULL;
		XactUndo.last_location[i] = InvalidUndoRecPtr;
		XactUndo.last_batch_lsn[i] = InvalidXLogRecPtr;
	}

	/*
	 * The chain is now the commit's, the abort's or the 2PC state's; none of
	 * those needs the in-flight record.
	 */
	PublishXactUndoChain(InvalidTransactionId);
}

/*
 * CollapseXactUndoSubTransactions
 *		Collapse all subtransaction state into the top level.
 */
static void
CollapseXactUndoSubTransactions(void)
{
	while (XactUndo.subxact_depth > 0)
	{
		/*
		 * Merge current level into parent by calling AtSubCommit_XactUndo
		 * with the current level's nestingLevel.
		 */
		AtSubCommit_XactUndo(
							 XactUndo.subxact_stack[XactUndo.subxact_depth].nestingLevel);
	}
}
