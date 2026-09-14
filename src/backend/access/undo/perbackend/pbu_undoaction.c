/*-------------------------------------------------------------------------
 *
 * undoaction.c
 *	  execute undo actions
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/access/undo/undoaction.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/perbackend/pbu_compat.h"

#include "access/table.h"
#include "access/perbackend/pbu_undoaction_xlog.h"
#include "access/perbackend/pbu_undolog.h"
#include "access/perbackend/pbu_undorequest.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlog_internal.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/relfilenumbermap.h"
#include "utils/syscache.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "access/perbackend/pbu_undodiscard.h"
#include "access/undormgr.h"

/*
 * PrefetchUndoPages - Prefetch undo pages
 *
 * Prefetch undo pages, if prefetch_pages are behind prefetch_target
 */
static void
PrefetchUndoPages(RelFileNode rnode, int prefetch_target, int *prefetch_pages,
				  BlockNumber to_blkno, BlockNumber from_blkno,
				  char persistence)
{
	int			nprefetch;
	BlockNumber startblock;
	BlockNumber lastprefetched;

	/* Calculate last prefetched page in the previous iteration. */
	lastprefetched = from_blkno - *prefetch_pages;

	/* We have already prefetched all the pages of the transaction's undo. */
	if (lastprefetched <= to_blkno)
		return;

	/* Calculate number of blocks to be prefetched. */
	nprefetch =
		Min(prefetch_target - *prefetch_pages, lastprefetched - to_blkno);

	/* Where to start prefetch. */
	startblock = lastprefetched - nprefetch;

	while (nprefetch--)
	{
		PrefetchSharedBuffer(smgropen(rnode, INVALID_PROC_NUMBER),
							 MAIN_FORKNUM, startblock++);
		(*prefetch_pages)++;
	}
}

/*
 * UndoRecordBulkFetch  - Read undo records in bulk
 *
 * Read undo records between from_urecptr and to_urecptr until we exhaust the
 * the memory size specified by undo_apply_size.  If we could not read all the
 * records till to_urecptr then the caller should consume current set of records
 * and call this function again.
 *
 * from_urecptr		- Where to start fetching the undo records.  If we can not
 *					  read all the records because of memory limit then this
 *					  will be set to the previous undo record pointer from where
 *					  we need to start fetching on next call. Otherwise it will
 *					  be set to InvalidUndoRecPtr.
 * to_urecptr		- Last undo record pointer to be fetched.
 * undo_apply_size	- Memory segment limit to collect undo records.
 * nrecords			- Number of undo records read.
 * one_page			- Caller is applying undo only for one block not for
 *					  complete transaction.  If this is set true then instead of
 *					  following transaction undo chain using prevlen we will
 *					  follow the block prev chain of the block so that we can
 *					  avoid reading many unnecessary undo records of the
 *					  transaction.
 */
UndoRecInfo *
UndoRecordBulkFetch(UndoRecPtr *from_urecptr, UndoRecPtr to_urecptr,
					int undo_apply_size, int *nrecords, bool one_page)
{
	RelFileNode rnode;
	UndoRecPtr	urecptr,
				prev_urec_ptr;
	BlockNumber blkno;
	BlockNumber to_blkno;
	Buffer		buffer = InvalidBuffer;
	UnpackedUndoRecord *uur = NULL;
	UndoRecInfo *urp_array;
	int			urp_array_size = 1024;
	int			urp_index = 0;
	int			prefetch_target = 0;
	int			prefetch_pages = 0;
	Size		total_size = 0;
	TransactionId xid = InvalidTransactionId;

	/*
	 * In one_page mode we are fetching undo only for one page instead of
	 * fetching all the undo of the transaction.  Basically, we are fetching
	 * interleaved undo records.  So it does not make sense to do any prefetch
	 * in that case.
	 */
	if (!one_page)
		prefetch_target = target_prefetch_pages;

	/*
	 * Allocate initial memory to hold the undo record info, we can expand it
	 * if needed.
	 */
	urp_array = (UndoRecInfo *) palloc(sizeof(UndoRecInfo) * urp_array_size);
	urecptr = *from_urecptr;

	prev_urec_ptr = InvalidUndoRecPtr;
	*from_urecptr = InvalidUndoRecPtr;

	/* Read undo chain backward until we reach to the first undo record.  */
	do
	{
		BlockNumber from_blkno;
		UndoLogControl *log;
		UndoPersistence persistence;
		int			size;
		int			logno;

		logno = UndoRecPtrGetLogNo(urecptr);
		log = UndoLogGet(logno);
		persistence = log->meta.persistence;

		UndoRecPtrAssignRelFileNode(rnode, urecptr);
		to_blkno = UndoRecPtrGetBlockNum(to_urecptr);
		from_blkno = UndoRecPtrGetBlockNum(urecptr);

		/* Allocate memory for next undo record. */
		uur = palloc0(sizeof(UnpackedUndoRecord));

		/*
		 * If next undo record pointer to be fetched is not on the same block
		 * then release the old buffer and reduce the prefetch_pages count by
		 * one as we have consumed one page. Otherwise, just set the old
		 * buffer into the new undo record so that UndoGetOneRecord don't read
		 * the buffer again.
		 */
		blkno = UndoRecPtrGetBlockNum(urecptr);
		if (!UndoRecPtrIsValid(prev_urec_ptr) ||
			UndoRecPtrGetLogNo(prev_urec_ptr) != logno ||
			UndoRecPtrGetBlockNum(prev_urec_ptr) != blkno)
		{
			/* Release the previous buffer */
			if (BufferIsValid(buffer))
			{
				UnlockReleaseBuffer(buffer);
				buffer = InvalidBuffer;
			}

			if (prefetch_pages > 0)
				prefetch_pages--;
		}
		else
			uur->uur_buffer = buffer;

		/*
		 * If prefetch_pages are half of the prefetch_target then it's time to
		 * prefetch again.
		 */
		if (prefetch_pages < prefetch_target / 2)
			PrefetchUndoPages(rnode, prefetch_target, &prefetch_pages, to_blkno,
							  from_blkno, persistence);

		/*
		 * In one_page mode it's possible that the undo of the transaction
		 * might have been applied by worker and undo got discarded. Prevent
		 * discard worker from discarding undo data while we are reading it.
		 * See detail comment in UndoFetchRecord.  In normal mode we are
		 * holding transaction undo action lock so it can not be discarded.
		 */
		if (one_page)
		{
			LWLockAcquire(&log->discard_lock, LW_SHARED);

			if (!UndoRecordIsValid(urecptr))
				break;

			/* Read the undo record. */
			UndoGetOneRecord(uur, urecptr, rnode, persistence, true);
			LWLockRelease(&log->discard_lock);
		}
		else
			UndoGetOneRecord(uur, urecptr, rnode, persistence, true);

		/*
		 * Remember the buffer, so that next time we can call UndoGetOneRecord
		 * with the same buffer if we are reading the undo from the same
		 * buffer.
		 */
		buffer = uur->uur_buffer;
		uur->uur_buffer = InvalidBuffer;

		/*
		 * As soon as the transaction id is changed we can stop fetching the
		 * undo record.  Ideally, to_urecptr should control this but while
		 * reading undo only for a page we don't know what is the end undo
		 * record pointer for the transaction.
		 */
		if (one_page)
		{
			if (!TransactionIdIsValid(xid))
				xid = uur->uur_xid;
			else if (xid != uur->uur_xid)
				break;
		}

		/* Remember the previous undo record pointer. */
		prev_urec_ptr = urecptr;

		/*
		 * Calculate the previous undo record pointer of the transaction.  If
		 * we are reading undo only for a page then follow the blkprev chain
		 * of the page.  Otherwise, calculate the previous undo record pointer
		 * using transaction's current undo record pointer and the prevlen.
		 */
		if (one_page)
			urecptr = uur->uur_blkprev;
		else if (prev_urec_ptr == to_urecptr || uur->uur_info & UREC_INFO_TRANSACTION)
			urecptr = InvalidUndoRecPtr;
		else
			urecptr = UndoGetPrevUndoRecptr(prev_urec_ptr, uur->uur_prevurp,
											&buffer);

		/* We have consumed all elements of the urp_array so expand its size. */
		if (urp_index >= urp_array_size)
		{
			urp_array_size *= 2;
			urp_array =
				repalloc(urp_array, sizeof(UndoRecInfo) * urp_array_size);
		}

		/* Add entry in the urp_array */
		urp_array[urp_index].index = urp_index;
		urp_array[urp_index].urp = prev_urec_ptr;
		urp_array[urp_index].uur = uur;
		urp_index++;

		/* We have fetched all the undo records for the transaction. */
		if (!UndoRecPtrIsValid(urecptr) || (prev_urec_ptr == to_urecptr))
			break;

		/*
		 * Including current record, if we have crossed the memory limit then
		 * stop processing more records.  Remember to set the from_urecptr so
		 * that on next call we can resume fetching undo records where we left
		 * it.
		 */
		size = UnpackedUndoRecordSize(uur);
		total_size += size;

		if (total_size >= undo_apply_size)
		{
			*from_urecptr = urecptr;
			break;
		}
	} while (true);

	/* Release the last buffer. */
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	*nrecords = urp_index;

	return urp_array;
}

/*
 * undo_record_comparator
 *
 * qsort comparator to handle undo record for applying undo actions of the
 * transaction.
 */
static int
undo_record_comparator(const void *left, const void *right)
{
	UnpackedUndoRecord *luur = ((UndoRecInfo *) left)->uur;
	UnpackedUndoRecord *ruur = ((UndoRecInfo *) right)->uur;

	if (luur->uur_rmid < ruur->uur_rmid)
		return -1;
	else if (luur->uur_rmid > ruur->uur_rmid)
		return 1;
	else if (luur->uur_reloid < ruur->uur_reloid)
		return -1;
	else if (luur->uur_reloid > ruur->uur_reloid)
		return 1;
	else if (luur->uur_block == ruur->uur_block)
	{
		/*
		 * If records are for the same block then maintain their existing
		 * order by comparing their index in the array.  Because for single
		 * block we need to maintain the order for applying undo action.
		 */
		if (((UndoRecInfo *) left)->index < ((UndoRecInfo *) right)->index)
			return -1;
		else
			return 1;
	}
	else if (luur->uur_block < ruur->uur_block)
		return -1;
	else
		return 1;
}

/*
 * execute_undo_actions - Execute the undo actions
 *
 * xid - Transaction id that is getting rolled back.
 * from_urecptr - undo record pointer from where to start applying undo action.
 * to_urecptr	- undo record pointer upto which point apply undo action.
 * nopartial	- true if rollback is for complete transaction.
 *
 * Returns true if every record in the requested range was applied, and false
 * if at least one record was skipped (UNDO_APPLY_SKIPPED) or failed
 * (UNDO_APPLY_ERROR), so the transaction's rollback is NOT complete.  A false
 * return obliges the caller to arrange for the remaining work to be done
 * later: the AM apply callbacks skip unconditionally while InRecovery is set
 * (they need syscache/relcache and the index AMs, which the startup process
 * does not have), so the crash-recovery driver must re-queue the transaction
 * for a post-recovery worker rather than treat the chain as reverted.  See
 * PbuPerformUndoRecovery().
 *
 * A return of true after an already-applied transaction header short-circuit
 * (uur_progress != 0) is correct: the rollback is complete, just not by us.
 */
bool
execute_undo_actions(FullTransactionId full_xid, UndoRecPtr from_urecptr,
					 UndoRecPtr to_urecptr, bool nopartial)
{
	UnpackedUndoRecord *uur = NULL;
	UndoRecInfo *urp_array;
	UndoRecPtr	urec_ptr;
	ForkNumber	prev_fork = InvalidForkNumber;
	BlockNumber prev_block = InvalidBlockNumber;
	int			undo_apply_size = maintenance_work_mem * 1024L;
	bool		all_applied = true;
	TransactionId xid PG_USED_FOR_ASSERTS_ONLY = XidFromFullTransactionId(full_xid);

	/* 'from' and 'to' pointers must be valid. */
	Assert(from_urecptr != InvalidUndoRecPtr);
	Assert(to_urecptr != InvalidUndoRecPtr);

	urec_ptr = from_urecptr;

	if (nopartial)
	{
		/*
		 * It is important here to fetch the latest undo record and validate
		 * if the actions are already executed.  The reason is that it is
		 * possible that discard worker or backend might try to execute the
		 * rollback request which is already executed.  For ex., after discard
		 * worker fetches the record and found that this transaction need to
		 * be rolledback, backend might concurrently execute the actions and
		 * remove the request from rollback hash table. The similar problem
		 * can happen if the discard worker first pushes the request, the undo
		 * worker processed it and backend tries to process it some later
		 * point.
		 */
		uur = UndoFetchRecord(to_urecptr, InvalidBlockNumber, InvalidOffsetNumber,
							  InvalidTransactionId, NULL, NULL);

		/* already processed. */
		if (uur == NULL)
			return true;

		/*
		 * We don't need to execute the undo actions if they are already
		 * executed.
		 */
		if (uur->uur_progress != 0)
		{
			UndoRecordRelease(uur);
			return true;
		}

		Assert(xid == uur->uur_xid);

		UndoRecordRelease(uur);
		uur = NULL;
	}

	/*
	 * Fetch the multiple undo records which can fit into uur_segment; sort
	 * them in order of reloid and block number then apply them together
	 * page-wise. Repeat this until we get invalid undo record pointer.
	 */
	do
	{
		int			prev_rmid = -1;
		Oid			prev_reloid = InvalidOid;
		bool		blk_chain_complete;
		int			i;
		int			nrecords;
		int			last_index = 0;
		int			prefetch_pages = 0;

		/*
		 * If urec_ptr is not valid means we have complete all undo actions
		 * for this transaction, otherwise we need to fetch the next batch of
		 * the undo records.
		 */
		if (!UndoRecPtrIsValid(urec_ptr))
			break;

		/*
		 * Fetch multiple undo record in bulk.  This will return the array of
		 * undo record which will holds undo record pointers and the pointers
		 * to the actual unpacked undo record.   This will also update the
		 * number of undo records it has copied in the urp_array.  Also, for
		 * prefetching the target block ahead of applying undo actions it will
		 * update undo_blkinfo which will contains the information of the data
		 * blocks for which undo actions are going to applied for this undo
		 * record batch.
		 */
		urp_array = UndoRecordBulkFetch(&urec_ptr, to_urecptr, undo_apply_size,
										&nrecords, false);
		if (nrecords == 0)
			break;

		Assert(TransactionIdEquals(xid, urp_array[0].uur->uur_xid));

		/* Sort the undo record array in order of target blocks. */
		qsort((void *) urp_array, nrecords, sizeof(UndoRecInfo),
			  undo_record_comparator);

		if (nopartial && !UndoRecPtrIsValid(urec_ptr))
			blk_chain_complete = true;
		else
			blk_chain_complete = false;

		/*
		 * Now we have urp_array which is sorted in the block order so
		 * traverse this array and apply the undo action block by block.
		 */
		for (i = last_index; i < nrecords; i++)
		{
			UnpackedUndoRecord *uur = urp_array[i].uur;

			/*
			 * If this undo is not for the same block then apply all undo
			 * actions for the previous block.
			 */
			if (prev_rmid >= 0 &&
				(prev_rmid != uur->uur_rmid ||
				 prev_reloid != uur->uur_reloid ||
				 prev_fork != uur->uur_fork ||
				 prev_block != uur->uur_block))
			{
				if (!execute_undo_actions_page(urp_array, last_index, i - 1,
											   prev_reloid, full_xid, prev_block,
											   blk_chain_complete))
					all_applied = false;
				last_index = i;

				/* We have consumed one prefetched page. */
				if (prefetch_pages > 0)
					prefetch_pages--;
			}

			prev_rmid = uur->uur_rmid;
			prev_reloid = uur->uur_reloid;
			prev_fork = uur->uur_fork;
			prev_block = uur->uur_block;
		}

		/* Apply the last set of the actions. */
		if (!execute_undo_actions_page(urp_array, last_index, i - 1,
									   prev_reloid, full_xid, prev_block,
									   blk_chain_complete))
			all_applied = false;

		/* Free all undo records. */
		for (i = 0; i < nrecords; i++)
			UndoRecordRelease(urp_array[i].uur);

		/*
		 * Free urp array and undo_blkinfo array for the current batch of undo
		 * records.
		 */
		pfree(urp_array);
	} while (true);

	/*
	 * Set undo action apply progress as completed in the transaction header
	 * if this is a main transaction.
	 */
	if (nopartial)
	{
		/*
		 * During crash recovery (the ARIES undo phase), the whole transaction
		 * chain is applied once, before WAL insertion is re-enabled and before
		 * the shared rollback hash table is populated for this xid.  The
		 * transaction-header progress marker and its CLR
		 * (XLOG_UNDO_APPLY_PROGRESS) exist to make an interrupted LIVE abort
		 * idempotent and to coordinate with the discard worker; neither applies
		 * here.  Attempting the progress update in recovery is also unsafe: it
		 * would take the InRecovery undo-buffer read path (no exclusive buffer
		 * lock) and then MarkBufferDirty, tripping the buffer-lock assert, and
		 * it would emit WAL and touch the RollbackHT that has no entry for this
		 * xid.  So in recovery we skip the progress/CLR bookkeeping entirely;
		 * the per-record apply above already reverted the change, and the
		 * end-of-recovery checkpoint provides durability.
		 *
		 * Note that in recovery the per-record apply above has typically NOT
		 * reverted anything: the AM callbacks return UNDO_APPLY_SKIPPED while
		 * InRecovery is set, which all_applied has recorded.  Marking progress
		 * here would therefore be a lie as well as unsafe; the caller uses our
		 * false return to re-queue the transaction for post-recovery apply,
		 * which then does reach the progress/CLR bookkeeping below.
		 */
		if (InRecovery)
			return all_applied;

		/*
		 * Do not mark the transaction header as fully applied when some record
		 * was skipped or failed: the marker is what makes a later retry a
		 * no-op, so setting it now would strand the un-reverted remainder.
		 * The caller decides whether to re-queue.
		 */
		if (!all_applied)
			return false;

		/*
		 * Prepare and update the progress of the undo action apply in the
		 * transaction header.
		 */
		PrepareUpdateUndoActionProgress(NULL, to_urecptr, 1);

		START_CRIT_SECTION();

		/* Update the progress in the transaction header. */
		UndoRecordUpdateTransInfo(0);

		/* WAL log the undo apply progress. */
		{
			XLogRecPtr	lsn;
			xl_undoapply_progress xlrec;

			xlrec.urec_ptr = to_urecptr;
			xlrec.progress = 1;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, sizeof(xlrec));

			RegisterUndoLogBuffers(2);
			lsn = XLogInsert(RM_UNDOACTION_ID, XLOG_UNDO_APPLY_PROGRESS);
			UndoLogBuffersSetLSN(lsn);
		}

		END_CRIT_SECTION();
		UnlockReleaseUndoBuffers();

		/*
		 * Undo action is applied so delete the hash table entry.
		 */
		Assert(TransactionIdIsValid(xid));
		RollbackHTRemoveEntry(full_xid, to_urecptr);
	}

	return all_applied;
}

/*
 * PbuAtAbort_ApplyUndo
 *		Roll back the current transaction's per-backend UNDO at abort.
 *
 * Called from AbortTransaction() (xact.c) in the inline-undo-apply window,
 * alongside (but independent of) the per-relation engine's AtAbort_XactUndo().
 * The two engines coexist: this path only fires when the per-backend engine
 * produced UNDO for this transaction, which is recorded separately on the
 * TransactionState (pbuUndoStartPtr) via SetCurrentUndoLocation().  No access
 * method writes per-backend UNDO in this phase, so pbuUndoStartPtr is always
 * invalid here and this returns immediately; the dispatch path is live and
 * correct for when FLUX migrates to per-backend UNDO (Phase 8).
 *
 * The rollback is registered in the shared rollback hash table so the discard
 * worker will not race us.  If the request cannot be pushed to a background
 * undo worker (e.g. single-user mode, or workers disabled), the backend
 * applies its own UNDO synchronously via execute_undo_actions().
 */
void
PbuAtAbort_ApplyUndo(void)
{
	FullTransactionId full_xid;
	UndoRecPtr	start_urec_ptr;
	UndoRecPtr	from_urec_ptr;
	bool		pushed;

	start_urec_ptr = (UndoRecPtr) GetCurrentTransactionPbuUndoStart();

	/* Nothing to do if this transaction produced no per-backend UNDO. */
	if (!UndoRecPtrIsValid(start_urec_ptr))
		return;

	full_xid = GetTopFullTransactionIdIfAny();
	if (!FullTransactionIdIsValid(full_xid))
		return;

	from_urec_ptr = (UndoRecPtr) GetCurrentTransactionPbuUndoLatest();
	if (!UndoRecPtrIsValid(from_urec_ptr))
		from_urec_ptr = start_urec_ptr;

	/*
	 * Register the rollback request.  RegisterRollbackReq computes the chain's
	 * end pointer from start_urec_ptr, records the request so the discard
	 * worker skips it, and returns true if it was pushed to a background undo
	 * worker.  It returns false in single-user mode.
	 */
	pushed = RegisterRollbackReq(InvalidUndoRecPtr, start_urec_ptr,
								 MyDatabaseId, full_xid);

	/*
	 * If the request was not handed to a background worker, this backend must
	 * apply the UNDO itself, synchronously, before the transaction finishes
	 * aborting.  from = latest record, to = chain start, nopartial = true
	 * (whole transaction).
	 */
	if (!pushed)
		(void) execute_undo_actions(full_xid, from_urec_ptr, start_urec_ptr, true);
}

/*
 * execute_undo_actions_page - Execute the undo actions for a page
 *
 *	urp_array - array of undo records (along with their location) for which undo
 *				action needs to be applied.
 *	first_idx - index in the urp_array of the first undo action to be applied
 *	last_idx  - index in the urp_array of the first undo action to be applied
 *	reloid	- OID of relation on which undo actions needs to be applied.
 *	blkno	- block number on which undo actions needs to be applied.
 *						 blk_chain_complete - indicates whether the undo chain for block is
 *						 complete.
 *
 *	returns true if every record in [first_idx, last_idx] was applied, false if
 *	any record was skipped (UNDO_APPLY_SKIPPED) or errored.  A false return means
 *	the rollback of those records is still outstanding; the caller must not treat
 *	the transaction as reverted.  Unlike the previous behaviour, an
 *	UNDO_APPLY_ERROR does not stop the loop: the remaining records of the batch
 *	are still attempted (they may target unrelated pages), and the false return
 *	carries the incompleteness up.
 */
bool
execute_undo_actions_page(UndoRecInfo *urp_array, int first_idx, int last_idx,
						  Oid reloid, FullTransactionId full_xid, BlockNumber blkno,
						  bool blk_chain_complete)
{
	int			i;
	bool		all_applied = true;
	TransactionId xid = XidFromFullTransactionId(full_xid);

	Assert(urp_array != NULL);

	(void) reloid;			/* per-record uur_reloid is used instead */
	(void) blkno;
	(void) blk_chain_complete;

	/*
	 * Dispatch each record to the owning AM's undo-apply callback via the
	 * pluggable UNDO resource-manager API (access/undormgr.h).  Every AM or
	 * subsystem that writes per-backend UNDO registers an UndoRmgrData with an
	 * rm_undo callback keyed by the rmid stored in each record's header; the
	 * callback opens the target relation, reapplies the compensating change to
	 * the page and emits its own CLR WAL.
	 *
	 * No consumer registers a per-backend UNDO rmgr in this phase (that is
	 * Phase 8, when FLUX migrates to per-backend UNDO), so GetUndoRmgr()
	 * returns NULL and there is nothing to apply.  The dispatch path itself is
	 * live and correct for when the first consumer appears.
	 *
	 * The record subtype the callback switches on lives in uur_type, NOT
	 * uur_info (the latter is the UREC_INFO_* structural flag bits); we
	 * therefore pass rec->uur_type as the callback's "info" argument.  Passing
	 * uur_info here was a dormant dispatch bug.  (FILEOPS was briefly the first
	 * per-backend UNDO consumer but has since moved its crash-recovery revert
	 * to the common WAL stream; FLUX/ZHEAP/RECNO are the live consumers now.)
	 */
	for (i = first_idx; i <= last_idx; i++)
	{
		UnpackedUndoRecord *rec = urp_array[i].uur;
		const UndoRmgrData *undormgr;
		const char *payload;
		Size		payload_len;
		UndoApplyResult result;

		undormgr = GetUndoRmgr(rec->uur_rmid);
		if (undormgr == NULL || undormgr->rm_undo == NULL)
		{
			/*
			 * No registered handler for this rmid.  This should not happen
			 * once a consumer is attached; treat it as "nothing to apply" so
			 * an unattached engine (this phase) is a safe no-op.
			 */
			continue;
		}

		payload = rec->uur_payload.data;
		payload_len = rec->uur_payload.len;

		result = undormgr->rm_undo(rec->uur_rmid,
								   rec->uur_type,
								   xid,
								   rec->uur_reloid,
								   payload,
								   payload_len,
								   urp_array[i].urp);

		/*
		 * Both SKIPPED and ERROR leave this record's change in place, so the
		 * transaction is not fully reverted either way.  Report that to the
		 * caller instead of silently returning success.
		 *
		 * SKIPPED is the interesting case for crash recovery: every AM undo
		 * callback returns it unconditionally while InRecovery is set, so a
		 * whole loser transaction comes back SKIPPED from the ARIES undo phase
		 * and must be re-queued (PbuPerformUndoRecovery).  SKIPPED is also
		 * returned for genuinely nothing-to-do cases (the relation was dropped,
		 * the block was truncated away); re-queueing those is harmless, the
		 * retry skips them again and the request is then dropped.
		 */
		if (result != UNDO_APPLY_SUCCESS)
			all_applied = false;
	}

	return all_applied;
}
