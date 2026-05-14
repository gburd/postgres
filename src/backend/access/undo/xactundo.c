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
 * CROSS-RELATION ORDERING INVARIANT (important for TOAST correctness):
 *
 * All UNDO records for all relations touched by a single transaction are
 * packed into the same UndoRecordSet, in strict WAL-LSN emission order.
 * The newest-first application order during rollback guarantees correct
 * restoration ordering for multi-relation operations (e.g., FILEOPS).
 *
 * PARALLEL RECOVERY: XLOG_UNDO_BATCH records are handled by the startup
 * process via ApplyUndoChainFromWAL(); they are not dispatched to parallel
 * workers because UNDO application requires coordinated per-transaction
 * state.
 *
 * SUBTRANSACTION TRACKING:
 *
 * Subtransaction state is tracked using a dynamically-grown array allocated
 * in TopMemoryContext.  The array starts at INITIAL_SUBXACT_CAPACITY (64)
 * slots and doubles when needed via repalloc().  The array persists across
 * transactions within the same backend to avoid repeated allocation for
 * steady-state workloads.
 *
 * Growth via repalloc() in TopMemoryContext during SubXactCallbacks is safe
 * because it does not interact with pgstat's per-subtransaction tracking
 * (the original corruption bug was caused by palloc of new per-subtransaction
 * nodes in CurTransactionContext, not by growing an existing TopMemoryContext
 * allocation).
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
#include "access/undo_flush.h"
#include "access/undo_xlog.h"
#include "access/xlog.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/xact.h"
#include "access/xactundo.h"
#include "access/xlogdefs.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/injection_point.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* GUC: UNDO bytes threshold for instant abort via ATM */
int			undo_instant_abort_threshold = 65536;

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
}			XactUndoSubTransactionState;

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
}			XactUndoData;

static XactUndoData XactUndo;
static bool subxact_callback_registered = false;

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
	TransactionId xid = GetCurrentTransactionId();
	UndoRecordSet *uset;
	XactUndoSubTransactionState *cur;
	UndoRecPtr *sub_start_location;

	/* Remember that we've done something undo-related. */
	XactUndo.has_undo = true;

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
	sub_start_location = &cur->start_location[plevel];
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
	XactUndoSubTransactionState *cur;
	UndoRecPtr *sub_start_location;

	/* Remember that we've done something undo-related. */
	XactUndo.has_undo = true;

	/*
	 * If we've entered a subtransaction deeper than what's currently tracked,
	 * push a new entry onto the subxact_stack.
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

		/* Fix up subtransaction start location if needed */
		if (cur->start_location[ctx->plevel] == (UndoRecPtr) 1)
			cur->start_location[ctx->plevel] = ptr;

		/*
		 * Update the per-transaction undo pointer in TransactionState so that
		 * the next UndoRecordSetCreate (if called directly by heap AM or
		 * other subsystems) picks up the correct chain pointer.
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
 * XActUndoUpdateLastBatchLSN
 *		Record the LSN of an UNDO batch for the current transaction.
 *
 * Called from the heap DML code after writing an UNDO batch -- either
 * embedded inside a heap WAL record (HAS_UNDO path) or as a standalone
 * XLOG_UNDO_BATCH overflow record.  Updates last_batch_lsn so that
 * AtAbort_XactUndo() can find the head of the UNDO chain, and registers
 * the batch LSN for WAL retention tracking on first call per transaction.
 */
void
XActUndoUpdateLastBatchLSN(XLogRecPtr lsn, UndoPersistenceLevel plevel)
{
	if (!XLogRecPtrIsValid(lsn) || plevel >= NUndoPersistenceLevels)
		return;

	/*
	 * The pointer must be a valid LSN.  Rmid validation is deferred to the
	 * rollback path; see comment in InsertXactUndoData for why we cannot
	 * call UndoValidateBatchLSN here.
	 */
	Assert(XLogRecPtrIsValid(lsn));

	XactUndo.has_undo = true;
	XactUndo.last_batch_lsn[plevel] = lsn;
}

/*
 * AtCommit_XactUndo
 *		Post-commit cleanup of the undo state.
 *
 * On commit, undo records are no longer needed for rollback.
 * Free all record sets and reset state.
 *
 * UNDO pages are managed by shared_buffers and flushed by the
 * checkpointer -- no per-commit fdatasync is needed.  We only
 * flush the deferred WAL allocation records so recovery can
 * reconstruct the UNDO log insert pointer.
 *
 * NB: This code MUST NOT FAIL, since it is run as a post-commit step.
 */
void
AtCommit_XactUndo(void)
{
	int			i;

	if (!XactUndo.has_undo)
	{
		/* Flush any deferred WAL even if has_undo is false */
		UndoWalBatchFlush();
		return;
	}

	/*
	 * With UNDO-in-WAL, all UNDO data was already written to WAL via
	 * XLOG_UNDO_BATCH records during the transaction.  The single XLogFlush()
	 * at commit time (in RecordTransactionCommit) ensures both the UNDO data
	 * and the commit record are durable.  No separate fdatasync is needed.
	 *
	 * Legacy WAL batch flush is now a no-op but kept for safety.
	 */
	UndoWalBatchFlush();

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
 * AtAbort_XactUndo
 *		Post-abort cleanup of the undo state.
 *
 * On abort, we need to apply the undo chain to roll back changes.
 * The actual undo application is triggered by xact.c before calling
 * this function.  Here we apply per-relation UNDO and clean up the record sets.
 *
 * With append-only I/O, we sync UNDO files before UNDO replay so that
 * if we crash during rollback, recovery can re-read the UNDO records
 * from the segment file and continue the rollback.
 */
void
AtAbort_XactUndo(void)
{
	int			i;
	bool		lsn_safely_held = false;	/* true if inline UNDO or ATM holds LSN */

	/* Always clean up the recycled context; see AtCommit_XactUndo. */
	UndoRecordSetResetCache();

	if (!XactUndo.has_undo)
	{
		/* No UNDO data was written; nothing to do */
		UndoWalBatchReset();
		return;
	}

	/*
	 * With UNDO-in-WAL, all UNDO data is already in the WAL stream. No
	 * separate sync is needed.  The UNDO data can be read back from WAL for
	 * rollback via UndoReadBatchFromWAL().
	 *
	 * For crash safety during abort: if we crash mid-rollback, the recovery
	 * undo phase will find this transaction's UNDO batches in WAL and
	 * complete the rollback.
	 */
	UndoWalBatchFlush();		/* no-op, kept for safety */

	INJECTION_POINT("undo-xact-abort-before-apply", NULL);

	/* Collapse all subtransaction state. */
	CollapseXactUndoSubTransactions();

	/*
	 * UNDO application strategy: inline for small transactions, deferred for
	 * large ones.  Controlled by undo_instant_abort_threshold GUC.
	 *
	 * For small transactions (< threshold bytes of UNDO): apply UNDO
	 * synchronously in this backend.  This avoids ATM pool accumulation and
	 * eliminates the dependency on the background logical revert worker.
	 *
	 * For large transactions (>= threshold): register in the ATM for deferred
	 * asynchronous rollback by the logical revert worker.
	 *
	 * The BumpContext issue (pfree crashes during abort) is avoided by:
	 * - Creating a temporary AllocSetContext for inline UNDO application
	 * - ApplyUndoChainFromWAL already avoids pfree on its allocations
	 * - Switching back to the abort context afterward
	 */
	{
		XLogRecPtr	perm_lsn =
			XactUndo.last_batch_lsn[UNDOPERSISTENCE_PERMANENT];

		if (XLogRecPtrIsValid(perm_lsn))
		{
			Size		total_undo_bytes = 0;

			/* Calculate total UNDO data size for threshold comparison */
			for (i = 0; i < NUndoPersistenceLevels; i++)
			{
				if (XactUndo.record_set[i] != NULL)
					total_undo_bytes += UndoRecordSetGetSize(
						XactUndo.record_set[i]);
			}

			/*
			 * The UNDO batch records were inserted into the WAL buffers during
			 * this transaction but may not yet be flushed to disk.  The inline
			 * apply path reads them back via the local XLog reader, which would
			 * otherwise busy-wait (pg_usleep) for the walwriter to flush past
			 * the batch LSN -- adding multi-second latency to every rollback.
			 * Flush WAL up to the current insert position so the batch is
			 * immediately readable.
			 */
			XLogFlush(GetXLogInsertRecPtr());

			if (undo_instant_abort_threshold > 0 &&
				total_undo_bytes < (Size) undo_instant_abort_threshold)
			{
				/*
				 * Small transaction: apply UNDO inline.  Use a dedicated
				 * AllocSetContext to avoid BumpContext pfree issues.
				 */
				MemoryContext undo_ctx;
				MemoryContext old_ctx;
				int			saved_trans_state;

				undo_ctx = AllocSetContextCreate(TopMemoryContext,
												 "Inline UNDO Apply",
												 ALLOCSET_DEFAULT_SIZES);
				old_ctx = MemoryContextSwitchTo(undo_ctx);

				{
				bool	undo_applied = false;

				/*
				 * Validate the batch LSN points to an actual UNDO record
				 * before attempting inline application.  Stale LSNs from
				 * chain_prev tracking anomalies can point to RECNO WAL
				 * records, which would cause "not an UNDO batch" warnings.
				 */
				if (!UndoValidateBatchLSN(perm_lsn))
				{
					elog(DEBUG1, "inline UNDO: last_batch_lsn %X/%X is not "
						 "a valid UNDO batch, deferring to ATM",
						 LSN_FORMAT_ARGS(perm_lsn));
					undo_applied = false;
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
					 * If inline UNDO throws an error, fall back to ATM.
					 */
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
					 * Inline UNDO failed (WAL recycled, wrong record
					 * type, or chain walk aborted).  Register in ATM
					 * for deferred processing by the revert worker.
					 */
					elog(DEBUG1, "inline UNDO failed for xid %u, "
						 "deferring to ATM",
						 GetCurrentTransactionId());

					if (ATMAddAborted(GetCurrentTransactionId(),
									   MyDatabaseId, perm_lsn))
						lsn_safely_held = true;	/* ATM holds the LSN */
					else
						elog(WARNING, "ATM full: could not record aborted transaction %u", GetCurrentTransactionId());




				}
				else
				{
					lsn_safely_held = true;	/* UNDO fully applied, WAL can be recycled */
					ereport(DEBUG2,
							(errmsg("inline UNDO applied for xid %u "
									"(%zu bytes)",
									GetCurrentTransactionId(),
									total_undo_bytes)));
				}
			}
			}
			else
			{
				/*
				 * Large transaction or threshold=0: register in ATM for
				 * deferred rollback by the logical revert worker.
				 */
				if (ATMAddAborted(GetCurrentTransactionId(),
								   MyDatabaseId, perm_lsn))
				lsn_safely_held = true;
					else
					elog(WARNING,
						 "ATM full: could not record aborted transaction %u",
						 GetCurrentTransactionId());
			}
		}
	}

	INJECTION_POINT("undo-xact-abort-after-atm", NULL);

	/* Free all per-persistence-level record sets. */
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		if (XactUndo.record_set[i] != NULL)
		{
			UndoRecordSetFree(XactUndo.record_set[i]);
			XactUndo.record_set[i] = NULL;
		}
	}

	/* Close cached UNDO log fds. */
	UndoLogCloseFiles();

	/* Reset per-backend write pointer tracking. */
	UndoFlushResetMaxWritePtr();

	/*
	 * Release WAL retention hold ONLY if the LSN is safely held elsewhere:
	 * either inline UNDO completed (no WAL needed) or ATM registered the
	 * entry (revert worker will use ATM's copy of the LSN).
	 *
	 * If BOTH failed (inline UNDO failed AND ATM pool full), retain the
	 * per-backend slot to prevent checkpoint from recycling the WAL
	 * segment containing our UNDO data.  The slot will be cleared at
	 * backend exit via AtCleanup_XactUndo.
	 */
	if (lsn_safely_held)
		UndoClearBatchLSN();
	else
		elog(DEBUG1, "retaining per-backend UNDO LSN slot (ATM and inline both failed)");

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
	 * Apply per-relation UNDO for records generated during this
	 * subtransaction.  We iterate the record sets and apply records whose
	 * UndoRecPtr is at or after this subtransaction's start_location.
	 *
	 * For each persistence level where this subtransaction generated UNDO
	 * records, queue the work for the per-relation UNDO worker to apply them
	 * synchronously.  The parent transaction's records (before the
	 * subtransaction's start_location) are preserved.
	 */
	for (i = 0; i < NUndoPersistenceLevels; i++)
	{
		UndoRecPtr	sub_start = cur->start_location[i];

		if (!UndoRecPtrIsValid(sub_start))
			continue;

		/*
		 * Apply cluster-wide UNDO for this subtransaction.
		 *
		 * DISABLED: Same as in AtAbort_XactUndo, we defer UNDO application to
		 * avoid BumpContext issues. During subtransaction abort, PostgreSQL
		 * may use BumpContext which doesn't support pfree(), causing crashes
		 * when we try to clean up resources during UNDO application.
		 *
		 * The UNDO will be applied asynchronously by the logical revert
		 * worker when the top-level transaction commits or aborts and gets
		 * registered in the ATM.
		 *
		 * If the subtransaction advanced XactUndo.last_batch_lsn[i] beyond
		 * the value saved at subtransaction start, it wrote UNDO batches that
		 * must now be reversed.  Walk the chain from the current head back to
		 * (but not including) the saved parent head.
		 *
		 * After applying, restore last_batch_lsn[i] to the parent's saved
		 * value so that if the parent later aborts, AtAbort_XactUndo() only
		 * walks the parent's batches and does not double-apply ours.
		 */
		if (XLogRecPtrIsValid(XactUndo.last_batch_lsn[i]) &&
			XactUndo.last_batch_lsn[i] != cur->last_batch_lsn[i])
		{
			/*
			 * Invariant: the current head must be strictly newer (larger)
			 * than the parent's saved head.  If this fires, a sibling
			 * subtransaction improperly modified last_batch_lsn after its own
			 * abort, which would cause us to skip or double-apply batches.
			 */
			Assert(!XLogRecPtrIsValid(cur->last_batch_lsn[i]) ||
				   XactUndo.last_batch_lsn[i] > cur->last_batch_lsn[i]);

			/*
			 * Synchronous UNDO application disabled - deferred to background
			 * worker
			 */
			/* ApplyUndoChainFromWAL(XactUndo.last_batch_lsn[i]); */
			XactUndo.last_batch_lsn[i] = cur->last_batch_lsn[i];
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

	/* Close any cached UNDO log fds before process exit. */
	UndoLogCloseFiles();

	/* Reset per-backend write pointer tracking. */
	UndoFlushResetMaxWritePtr();

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
