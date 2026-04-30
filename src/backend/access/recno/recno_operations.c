/*-------------------------------------------------------------------------
 *
 * recno_operations.c
 *	  RECNO table manipulation operations
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_operations.c
 *
 * NOTES
 *	  This implements the remaining table manipulation operations for
 *	  RECNO storage manager including insert, update, delete, and
 *	  various DDL operations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/recno.h"
#include "access/recno_slog.h"
#include "access/recno_xlog.h"
#include "access/relundo.h"
#include "access/tableam.h"
#include "access/recno_diff.h"
#include "access/xlog.h"
#include "access/tidstore.h"
#include "access/xact.h"
#include "access/xactundo.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "storage/bufpage.h"
#include "miscadmin.h"

/*
 * Maximum overflow pointers per tuple for VACUUM overflow cleanup.
 * This limits memory usage during VACUUM and is conservative since most
 * tuples won't have overflow data. 128 is sufficient for typical workloads.
 */
#define MAX_OVERFLOW_PTRS_PER_TUPLE 128

/* Function prototypes for locking */
extern bool RecnoLockTuple(Relation rel, ItemPointer tid, LockTupleMode mode,
						   bool wait, bool *have_tuple_lock);
extern void RecnoUnlockTuple(Relation rel, ItemPointer tid, LockTupleMode mode);
extern void RecnoLockPage(Relation rel, BlockNumber blkno, LOCKMODE mode);
extern void RecnoUnlockPage(Relation rel, BlockNumber blkno, LOCKMODE mode);

/*
 * In-place update statistics counters.
 *
 * These track the effectiveness of RECNO's in-place update optimization
 * across the lifetime of the backend.  They are exposed via
 * RecnoGetUpdateStats() for monitoring.
 */
static int64 recno_stat_in_place_updates = 0;
static int64 recno_stat_out_of_place_updates = 0;
static int64 recno_stat_defrag_triggered_updates = 0;

/*
 * RecnoGetUpdateStats - Return in-place update statistics
 *
 * Fills in the provided counters with the current backend-local statistics.
 */
void
RecnoGetUpdateStats(int64 *in_place, int64 *out_of_place, int64 *defrag_triggered)
{
	if (in_place)
		*in_place = recno_stat_in_place_updates;
	if (out_of_place)
		*out_of_place = recno_stat_out_of_place_updates;
	if (defrag_triggered)
		*defrag_triggered = recno_stat_defrag_triggered_updates;
}


/*
 * RecnoPagePruneOpt -- opportunistic dead-tuple cleanup on a page.
 *
 * This is the RECNO equivalent of heap_page_prune_opt().  It is called
 * during normal DML operations (insert, update) and sequential scans
 * when a page looks like it might benefit from cleanup.  The goal is to
 * reclaim space from deleted tuples without waiting for VACUUM.
 *
 * The caller must hold a pin on the buffer but must NOT hold a lock on it.
 * We attempt a conditional (non-blocking) exclusive lock; if we cannot
 * get it, we return immediately -- this is best-effort cleanup.
 *
 * Returns the number of tuples pruned.
 */
int
RecnoPagePruneOpt(Relation relation, Buffer buffer)
{
	Page		page;
	RecnoPageOpaque opaque;
	OffsetNumber offnum;
	OffsetNumber maxoff;
	uint64		oldest_ts;
	int			ndead = 0;
	Size		minfree;

	/* Cannot write WAL during recovery, so skip */
	if (RecoveryInProgress())
		return 0;

	page = BufferGetPage(buffer);

	/* Skip if page is not initialized */
	if (PageIsNew(page))
		return 0;

	/*
	 * Validate page header before accessing special space. During recovery or
	 * after crashes, pages may have invalid headers. Skip pruning if the page
	 * header looks corrupt.
	 */
	{
		PageHeader	phdr = (PageHeader) page;

		if (phdr->pd_special < SizeOfPageHeaderData ||
			phdr->pd_special > BLCKSZ)
			return 0;
	}

	/*
	 * Quick check without lock: does the page look like it needs pruning? The
	 * RECNO_PAGE_DEFRAG_NEEDED flag is set by delete and update operations
	 * when a tuple is marked as deleted.  If no deletions have occurred on
	 * this page, there is nothing to clean up.
	 */
	opaque = RecnoPageGetOpaque(page);
	if (!(opaque->pd_flags & RECNO_PAGE_DEFRAG_NEEDED))
		return 0;

	/*
	 * Heuristic: only prune if the page's free space is below a threshold.
	 * This avoids spending cycles on pages that already have plenty of room.
	 * We use 10% of BLCKSZ as the minimum, matching heap's approach. Reading
	 * pd_lower/pd_upper without a lock is slightly racy but acceptable for a
	 * heuristic.
	 */
	minfree = BLCKSZ / 10;
	if (PageGetFreeSpace(page) >= minfree &&
		!(opaque->pd_flags & RECNO_PAGE_FULL))
		return 0;

	/*
	 * Try to get an exclusive lock without blocking.  If the page is busy,
	 * skip it -- we will get another chance later.
	 */
	if (!ConditionalLockBufferForCleanup(buffer))
		return 0;

	/*
	 * Re-check under lock: the page state may have changed while we were
	 * acquiring the lock (or another backend may have pruned it).
	 */
	page = BufferGetPage(buffer);
	opaque = RecnoPageGetOpaque(page);
	if (!(opaque->pd_flags & RECNO_PAGE_DEFRAG_NEEDED))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		return 0;
	}

	/*
	 * Get the oldest active transaction timestamp.  Deleted tuples whose
	 * commit timestamp is older than this can be safely removed.
	 */
	oldest_ts = RecnoGetOldestActiveTimestamp();

	/*
	 * Scan tuples to check if any dead tuples are actually reclaimable. If
	 * none are old enough, don't bother with defragmentation.
	 */
	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		RecnoTupleHeader *tuple_hdr;

		if (!ItemIdIsNormal(itemid))
			continue;

		/* Skip overflow records */
		if (RecnoIsOverflowRecord(PageGetItem(page, itemid),
								  ItemIdGetLength(itemid)))
			continue;

		tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);

		/*
		 * A deleted tuple can be pruned if:
		 *   - UNCOMMITTED is NOT set (the inserting/deleting xact committed)
		 *   - commit_ts is older than the oldest active snapshot
		 *
		 * If UNCOMMITTED is still set, the transaction is still in progress
		 * (or aborted but not yet cleaned up) -- skip it.
		 */
		if ((tuple_hdr->t_flags & RECNO_TUPLE_DELETED) &&
			!(tuple_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
			tuple_hdr->t_commit_ts < oldest_ts)
		{
			ndead++;
		}
	}

	if (ndead == 0)
	{
		/*
		 * No reclaimable dead tuples.  Clear the defrag flag so we don't
		 * recheck this page on every access until a new deletion occurs.
		 */
		opaque->pd_flags &= ~RECNO_PAGE_DEFRAG_NEEDED;
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		return 0;
	}

	/*
	 * We have reclaimable dead tuples.  First mark them as unused in the line
	 * pointer array, then defragment the page to compact free space.
	 *
	 * IMPORTANT: We must mark dead items LP_UNUSED before calling
	 * PageRepairFragmentation, because RECNO uses LP_NORMAL item pointers for
	 * deleted tuples (the deletion is tracked via RECNO_TUPLE_DELETED flag in
	 * the tuple header, not via LP_DEAD).
	 */
	START_CRIT_SECTION();

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		RecnoTupleHeader *tuple_hdr;

		if (!ItemIdIsNormal(itemid))
			continue;

		/* Skip overflow records */
		if (RecnoIsOverflowRecord(PageGetItem(page, itemid),
								  ItemIdGetLength(itemid)))
			continue;

		tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);

		if ((tuple_hdr->t_flags & RECNO_TUPLE_DELETED) &&
			!(tuple_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
			tuple_hdr->t_commit_ts < oldest_ts)
		{
			ItemIdSetUnused(itemid);
		}
	}

	RecnoPageDefragment(page);

	MarkBufferDirty(buffer);

	/* WAL-log the defragmentation using a proper defrag record */
	if (RelationNeedsWAL(relation))
	{
		XLogRecPtr	recptr;

		/*
		 * Use RecnoXLogDefrag (not RecnoXLogInitPage).  INIT_PAGE with
		 * REGBUF_WILL_INIT would zero the page during redo, losing all live
		 * tuples.  The defrag record uses REGBUF_STANDARD which stores a Full
		 * Page Image, preserving the page contents.
		 */
		recptr = RecnoXLogDefrag(relation, buffer, NULL, 0, oldest_ts);
		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/* Update FSM with the reclaimed free space */
	RecnoRecordFreeSpace(relation, BufferGetBlockNumber(buffer),
						 PageGetFreeSpace(page));

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	return true;
}


/*
 * Insert a tuple into a RECNO table
 */
void
recno_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
				   uint32 options, BulkInsertState bistate)
{
	RecnoTuple	recno_tuple;
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	BlockNumber target_block;
	Size		tuple_size;
	ItemPointer tid = &slot->tts_tid;
	uint64		current_ts;
	uint64		xact_ts;
	RecnoOverflowBuffers overflow_buffers;
	int			i;
	/* Per-relation UNDO state */
	Size		undo_record_size = 0;
	Buffer		undo_buffer = InvalidBuffer;
	RelUndoRecPtr undo_ptr = InvalidRelUndoRecPtr;

	slot_getallattrs(slot);

	/*
	 * Get current timestamp for MVCC.  In HLC mode, we use the HLC wrapper
	 * which generates a causally-consistent HLC timestamp.  In legacy mode,
	 * we get the transaction start timestamp from RECNO's MVCC system.
	 *
	 * IMPORTANT: Get transaction timestamp here, BEFORE entering critical
	 * section, because RecnoGetTransactionTimestamp() may need to allocate
	 * memory to initialize transaction state.
	 *
	 * CRITICAL FIX: Use RecnoGetTransactionTimestamp() for both current_ts
	 * and xact_ts to ensure consistency. The inserted tuple will be visible
	 * within the same transaction because the snapshot timestamp will match
	 * the tuple's commit timestamp.
	 */
	xact_ts = RecnoGetTransactionTimestamp();

	if (recno_use_hlc)
		current_ts = (uint64) RecnoGetDmlTimestamp();
	else
		current_ts = xact_ts;	/* Use same timestamp for within-txn
								 * visibility */

	/*
	 * Create RECNO tuple from slot.  Use the overflow-aware variant which
	 * will store large attributes (> RECNO_OVERFLOW_THRESHOLD) in overflow
	 * records on normal data pages, replacing them with compact inline
	 * overflow pointers.
	 *
	 * Overflow buffers are kept pinned for atomic WAL logging inside the
	 * critical section below.
	 */
	overflow_buffers.count = 0;
	recno_tuple = RecnoFormTuple(RelationGetDescr(relation),
								 slot->tts_values,
								 slot->tts_isnull,
								 relation,
								 &overflow_buffers);

	/* Set MVCC fields */
	recno_tuple->t_data->t_commit_ts = current_ts;
	recno_tuple->t_data->t_cid = cid;

	/*
	 * Mark the tuple as uncommitted.  The RECNO_TUPLE_UNCOMMITTED flag is
	 * set at INSERT time and cleared when the inserting transaction commits.
	 * Visibility checks use t_xid_hint (the inserter's XID) for fast
	 * CLOG/ProcArray checks, avoiding an sLog lookup entirely for the
	 * common INSERT case.
	 */
	recno_tuple->t_data->t_flags |= RECNO_TUPLE_UNCOMMITTED;
	recno_tuple->t_data->t_xid_hint = GetTopTransactionId();

	tuple_size = recno_tuple->t_len;

	/* Ensure relation storage exists */
	RelationGetSmgr(relation);

	/*
	 * Find a page with enough free space using FSM. RecnoGetPageWithFreeSpace
	 * will either find an existing page with space or extend the relation
	 * with a new page.
	 */
	target_block = RecnoGetPageWithFreeSpace(relation, tuple_size);

	if (target_block == InvalidBlockNumber)
	{
		/* Clean up overflow buffers before throwing error */
		for (i = 0; i < overflow_buffers.count; i++)
		{
			UnlockReleaseBuffer(overflow_buffers.buffers[i].buffer);
			pfree(overflow_buffers.buffers[i].record_data);
		}
		elog(ERROR, "RECNO failed to allocate page for tuple insertion");
	}

	/*
	 * Pre-allocate WAL buffer space BEFORE acquiring the data buffer lock.
	 * XLogEnsureRecordSpace() may allocate memory, so it MUST be called
	 * outside the critical section.
	 *
	 * rdata slots needed:
	 *   MAX_OVERFLOW_BUFFERS * 2 (header + data per overflow record)
	 *   + 2 (xl_recno_insert header + tuple data)
	 *   + 1 (xl_recno_hlc_info when HLC mode is enabled)
	 */
	if (RelationNeedsWAL(relation))
		XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID, 3 + MAX_OVERFLOW_BUFFERS * 2);

	/*
	 * Check if target_block is already locked in overflow_buffers from
	 * RecnoFormTupleWithOverflow. If FSM returns the same block for both
	 * overflow storage and main tuple storage, we must reuse that buffer
	 * to avoid double-locking.
	 */
	buffer = InvalidBuffer;
	for (i = 0; i < overflow_buffers.count; i++)
	{
		if (BufferGetBlockNumber(overflow_buffers.buffers[i].buffer) == target_block)
		{
			buffer = overflow_buffers.buffers[i].buffer;
			break;
		}
	}

	/*
	 * Read and lock the target page only if we don't already have it locked
	 * from overflow processing.
	 */
	if (!BufferIsValid(buffer))
	{
		buffer = ReadBuffer(relation, target_block);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	}
	page = BufferGetPage(buffer);

	/* Verify page has sufficient space */
	if (PageGetFreeSpace(page) < tuple_size)
	{
		bool		buffer_is_from_overflow = false;

		/*
		 * Check if this buffer is from overflow_buffers. If so, we must NOT
		 * unlock it for pruning, as overflow_buffers expects all its buffers
		 * to remain locked until the critical section.
		 */
		for (i = 0; i < overflow_buffers.count; i++)
		{
			if (overflow_buffers.buffers[i].buffer == buffer)
			{
				buffer_is_from_overflow = true;
				break;
			}
		}

		/*
		 * Page doesn't have enough space.  Try opportunistic pruning to
		 * reclaim space from dead tuples before falling back to the FSM. We
		 * must release our lock first since RecnoPagePruneOpt() takes its own
		 * conditional lock.
		 *
		 * IMPORTANT: Skip pruning if buffer is from overflow_buffers,
		 * as we must keep those buffers locked.
		 */
		if (!buffer_is_from_overflow)
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			if (RecnoPagePruneOpt(relation, buffer))
			{
				/* Pruning freed space -- re-lock and check again */
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				page = BufferGetPage(buffer);
				if (PageGetFreeSpace(page) >= tuple_size)
					goto have_page;
				/* Still not enough after pruning, fall through to FSM retry */
			}

			/*
			 * FSM information was stale or pruning didn't help. Update and
			 * retry.
			 */
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			RecnoRecordFreeSpace(relation, target_block, PageGetFreeSpace(page));
			UnlockReleaseBuffer(buffer);
		}
		else
		{
			/*
			 * Buffer is from overflow_buffers and can't be pruned or
			 * released. This means FSM returned an overflow page for the main
			 * tuple, which doesn't have enough space. This should be rare but
			 * can happen if overflow pages filled up during tuple formation.
			 *
			 * Update FSM for this page, then get a DIFFERENT page. We must
			 * retry until we find a page that's NOT in overflow_buffers.
			 */
			RecnoRecordFreeSpace(relation, target_block, PageGetFreeSpace(page));
		}

		/*
		 * Retry with updated FSM, excluding blocks in overflow_buffers.
		 * Keep trying until we find a suitable page that we don't already
		 * have locked for overflow storage.
		 */
		for (;;)
		{
			target_block = RecnoGetPageWithFreeSpace(relation, tuple_size);
			if (target_block == InvalidBlockNumber)
			{
				/* Clean up overflow buffers before throwing error */
				for (i = 0; i < overflow_buffers.count; i++)
				{
					UnlockReleaseBuffer(overflow_buffers.buffers[i].buffer);
					pfree(overflow_buffers.buffers[i].record_data);
				}
				elog(ERROR, "RECNO failed to allocate page for tuple insertion after retry");
			}

			/*
			 * Check if target_block is already locked in overflow_buffers.
			 * If so, skip it and try again - we need a different page.
			 */
			buffer = InvalidBuffer;
			for (i = 0; i < overflow_buffers.count; i++)
			{
				if (BufferGetBlockNumber(overflow_buffers.buffers[i].buffer) == target_block)
				{
					/* This block is already used for overflow - mark FSM and retry */
					RecnoRecordFreeSpace(relation, target_block, 0);
					buffer = InvalidBuffer;
					break;
				}
			}

			/* If we found a block not in overflow_buffers, check if it has space */
			if (i >= overflow_buffers.count)
			{
				buffer = ReadBuffer(relation, target_block);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				page = BufferGetPage(buffer);

				/*
				 * Verify the page actually has enough space. If not, update
				 * FSM and retry.
				 */
				if (PageGetFreeSpace(page) >= tuple_size)
				{
					/* Found a suitable page - exit retry loop */
					break;
				}
				else
				{
					/* FSM was wrong - update it and retry */
					RecnoRecordFreeSpace(relation, target_block, PageGetFreeSpace(page));
					UnlockReleaseBuffer(buffer);
					buffer = InvalidBuffer;
					/* Continue outer loop to try again */
				}
			}
		}
	}

have_page:
	/*
	 * Per-relation UNDO: Reserve space for an INSERT UNDO record.
	 * This must happen before the critical section because RelUndoReserve()
	 * may extend the UNDO fork which can error out.
	 *
	 * NOTE: This is done AFTER acquiring the data buffer lock to maintain
	 * consistent lock ordering (data buffer → UNDO buffer) with the UPDATE
	 * and DELETE paths.
	 */
	if (smgrexists(RelationGetSmgr(relation), RELUNDO_FORKNUM))
	{
		undo_record_size = SizeOfRelUndoRecordHeader + sizeof(RelUndoInsertPayload);
		undo_ptr = RelUndoReserve(relation, undo_record_size, &undo_buffer);
	}

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	/* Page is already set from the logic above */

	/* Add tuple to page using RECNO-specific function */
	offnum = RecnoPageAddTuple(page, recno_tuple, tuple_size);

	if (offnum == InvalidOffsetNumber)
		elog(PANIC, "failed to add RECNO tuple to page");

	/* Set the tuple's TID */
	ItemPointerSet(tid, BufferGetBlockNumber(buffer), offnum);
	recno_tuple->t_self = *tid;
	slot->tts_tableOid = RelationGetRelid(relation);

	/*
	 * Set the on-disk tuple's t_ctid to point to itself.  This is needed for
	 * update chains and cross-page defragmentation, which check whether
	 * t_ctid == self to detect tuples that are not part of an update chain.
	 */
	{
		ItemId		inserted_itemid = PageGetItemId(page, offnum);
		RecnoTupleHeader *inserted_hdr = (RecnoTupleHeader *) PageGetItem(page, inserted_itemid);

		ItemPointerSet(&inserted_hdr->t_ctid, BufferGetBlockNumber(buffer), offnum);
	}


	/*
	 * Update page opaque fields BEFORE WAL logging.  When
	 * XLogRegisterBuffer() takes a Full Page Write (FPW), the page image must
	 * already contain the same opaque values that REDO will set during
	 * replay.  Otherwise WAL consistency checking will detect a mismatch
	 * between the FPW and the page produced by REDO, causing a FATAL
	 * "inconsistent page found" error on the standby.
	 *
	 * This matches the fix applied to RecnoXLogInitPage in recno_fsm.c.
	 */
	{
		RecnoPageOpaque phdr = RecnoPageGetOpaque(page);

		phdr->pd_commit_ts = Max(phdr->pd_commit_ts, current_ts);
		phdr->pd_free_space = PageGetFreeSpace(page);
	}

	MarkBufferDirty(buffer);

	/* Log the insertion with all overflow buffers atomically */
	if (RelationNeedsWAL(relation))
	{
		XLogRecPtr	recptr = RecnoXLogInsert(relation, buffer, offnum,
											 recno_tuple, current_ts,
											 &overflow_buffers);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/*
	 * Release all overflow buffers and free their cached data.
	 *
	 * IMPORTANT: Due to spatial locality optimization, an overflow buffer
	 * might be the SAME as the main buffer or as another overflow buffer
	 * (when overflow data is placed on the same page). Skip releasing
	 * buffers that were already released.  The main buffer is NOT released
	 * yet — only overflow buffers that differ from it.
	 */
	for (i = 0; i < overflow_buffers.count; i++)
	{
		Buffer		ovf_buf = overflow_buffers.buffers[i].buffer;
		bool		already_released = (ovf_buf == buffer);
		int			j;

		/* Check if this buffer was already released by a prior overflow entry */
		for (j = 0; j < i && !already_released; j++)
		{
			if (overflow_buffers.buffers[j].buffer == ovf_buf)
				already_released = true;
		}

		if (!already_released)
			UnlockReleaseBuffer(ovf_buf);
		pfree(overflow_buffers.buffers[i].record_data);
	}

	/*
	 * Finish the per-relation UNDO record now that the insert is complete.
	 * Write the UNDO record with the inserted TID and register it with the
	 * transaction system so that rollback can find and apply it.
	 *
	 * IMPORTANT: This must happen BEFORE RecnoVMUpdateForInsert to maintain
	 * consistent buffer lock ordering across forks.  RELUNDO_FORKNUM (4) >
	 * VISIBILITYMAP_FORKNUM (2), so we must release the UNDO fork buffer
	 * before acquiring a VM fork buffer to prevent buffer-level deadlocks.
	 * The UPDATE and DELETE paths already follow this ordering.
	 */
	if (RelUndoRecPtrIsValid(undo_ptr))
	{
		RelUndoRecordHeader undo_hdr;
		RelUndoInsertPayload undo_payload;

		/* Build UNDO header */
		undo_hdr.urec_type = RELUNDO_INSERT;
		undo_hdr.urec_len = SizeOfRelUndoRecordHeader + sizeof(RelUndoInsertPayload);
		undo_hdr.urec_xid = GetCurrentTransactionId();
		undo_hdr.urec_prevundorec = InvalidRelUndoRecPtr;
		undo_hdr.info_flags = 0;
		undo_hdr.tuple_len = 0;

		/* Build INSERT payload with TID range */
		undo_payload.firsttid = *tid;
		undo_payload.endtid = *tid;

		/* Write UNDO record atomically */
		RelUndoFinish(relation, undo_buffer, undo_ptr, &undo_hdr,
					  &undo_payload, sizeof(RelUndoInsertPayload));

		/* Register with transaction for rollback */
		RegisterPerRelUndo(RelationGetRelid(relation), undo_ptr);
	}

	/*
	 * Clear visibility map bits while buffer is still locked.  This is
	 * usually a fast no-op for newly created tables (no VM fork yet).
	 *
	 * Now that the UNDO fork buffer has been released above, we can safely
	 * acquire the VM fork buffer without violating fork lock ordering.
	 */
	RecnoVMUpdateForInsert(relation, recno_tuple->t_data, buffer);

	/*
	 * Save free space while we still have the buffer locked, then release
	 * the data buffer as soon as possible to reduce contention on hot pages.
	 * The remaining operations (FSM update, sLog registration) don't need
	 * the data buffer lock.
	 */
	{
		Size		saved_free_space = PageGetFreeSpace(page);
		BlockNumber saved_blkno = BufferGetBlockNumber(buffer);

		UnlockReleaseBuffer(buffer);

		/* Update FSM with remaining free space on the page */
		RecnoRecordFreeSpace(relation, saved_blkno, saved_free_space);
	}

	/*
	 * Lightweight subtransaction tracking for savepoint rollback.
	 *
	 * We do NOT create a full shared sLog entry here (that caused "out of
	 * shared memory" during bulk inserts with 100K+ rows).  Instead, we
	 * record the (tid, xid, subxid) in the per-backend local list only.
	 *
	 * If a savepoint is rolled back, RecnoSLogRemoveBySubXid will find
	 * the matching local entries and create a shared sLog ABORTED entry
	 * at that time.  The number of ABORTED entries is small (only tuples
	 * inserted within rolled-back savepoints), so this doesn't cause
	 * shared memory pressure.
	 *
	 * Speculative inserts (ON CONFLICT) are handled by the separate
	 * recno_tuple_insert_speculative() function, which still registers
	 * full sLog entries for the speculative token.
	 */
	RecnoSLogTrackSubXact(RelationGetRelid(relation), tid,
						  GetTopTransactionId(),
						  GetCurrentSubTransactionId());

	pfree(recno_tuple);
}

/*
 * Delete a tuple from a RECNO table with proper tombstone marking
 */
TM_Result
recno_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
			   uint32 options, Snapshot snapshot, Snapshot crosscheck,
			   bool wait, TM_FailureData *tmfd)
{
	BlockNumber blkno;
	OffsetNumber offnum;
	Buffer		buffer;
	Page		page;
	ItemId		itemid;
	RecnoTupleHeader *tuple_hdr;
	uint64		current_ts;
	uint64		xact_ts;
	TransactionId xid;
	bool		have_tuple_lock;
	RecnoTuple	old_tuple_for_delete_wal;
	Size		del_undo_record_size = 0;
	Buffer		del_undo_buffer = InvalidBuffer;
	RelUndoRecPtr del_undo_ptr = InvalidRelUndoRecPtr;

	/* Extract block and offset from TID */
	blkno = ItemPointerGetBlockNumber(tid);
	offnum = ItemPointerGetOffsetNumber(tid);

	/* Validate TID range */
	if (blkno >= RelationGetNumberOfBlocks(relation))
		return TM_Invisible;

	/* Read the page containing the tuple */
	buffer = ReadBuffer(relation, blkno);

	/*
	 * Lock the buffer exclusively.  The exclusive buffer lock is sufficient
	 * to prevent concurrent modifications — heavyweight tuple locks
	 * (RecnoLockTuple) are only needed for SELECT FOR UPDATE/SHARE, not for
	 * regular DML.  This matches heap's approach for UPDATE/DELETE.
	 */
	have_tuple_lock = false;
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);

	/* Validate offset number */
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
	{
		UnlockReleaseBuffer(buffer);
		return TM_Invisible;
	}

	/* Get the item */
	itemid = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(itemid))
	{
		UnlockReleaseBuffer(buffer);
		return TM_Invisible;
	}

	tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);

	/* Check if tuple is already deleted (tombstone exists) */
	if (tuple_hdr->t_flags & RECNO_TUPLE_DELETED)
	{
		if (tmfd)
		{
			tmfd->ctid = *tid;
			tmfd->xmax = GetCurrentTransactionId();
			tmfd->cmax = tuple_hdr->t_cid;
			tmfd->traversed = false;
		}
		UnlockReleaseBuffer(buffer);
		return TM_Deleted;
	}

	/*
	 * Handle LOCKED flag: same logic as the UPDATE path — clear our own
	 * lock before proceeding with the delete.
	 */
	if (tuple_hdr->t_flags & RECNO_TUPLE_LOCKED)
	{
		RecnoSLogEntry lock_entry;
		int			nfound;

		nfound = RecnoSLogLookup(RelationGetRelid(relation), tid,
								 GetCurrentTransactionId(), &lock_entry, 1);
		if (nfound > 0 &&
			(lock_entry.op_type == RECNO_SLOG_LOCK_SHARE ||
			 lock_entry.op_type == RECNO_SLOG_LOCK_EXCL))
		{
			tuple_hdr->t_flags &= ~RECNO_TUPLE_LOCKED;
		}
	}

	/*
	 * Fast-path: clear stale UNCOMMITTED flag (same optimization as UPDATE).
	 * We hold the buffer lock exclusively, so this is safe.
	 */
	if ((tuple_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
		!(tuple_hdr->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED)))
	{
		if (!RecnoSLogHasEntry(RelationGetRelid(relation), tid))
		{
			tuple_hdr->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
		}
	}

	/*
	 * Check tuple visibility against snapshot and handle concurrent
	 * modifications.  Same logic as the UPDATE path: distinguish truly
	 * invisible tuples from concurrent modifications.
	 */
	if (snapshot)
	{
		bool		visible;

		if (recno_use_hlc)
			visible = RecnoTupleVisibleHLC(tuple_hdr, RecnoGetSnapshotHLC(snapshot),
										   RelationGetRelid(relation),
										   (snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);
		else
			visible = RecnoTupleVisible(tuple_hdr, RecnoGetSnapshotTimestamp(snapshot), 0,
										RelationGetRelid(relation),
										(snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);

		if (!visible)
		{
			TransactionId dirty_xid;
			bool		is_insert_entry;

			/*
			 * Release buffer lock before calling RecnoSLogGetDirtyXid.
			 * Its slow path acquires all sLog partition locks, which can
			 * deadlock with other backends holding sLog partition locks
			 * while waiting for this buffer lock.  Keep buffer pinned.
			 */
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

			dirty_xid = RecnoSLogGetDirtyXid(RelationGetRelid(relation),
											  tid,
											  &is_insert_entry);

			/*
			 * Re-acquire buffer lock and re-validate the tuple, since
			 * another backend may have modified it while unlocked.
			 */
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buffer);

			if (offnum < FirstOffsetNumber ||
				offnum > PageGetMaxOffsetNumber(page))
			{
				UnlockReleaseBuffer(buffer);
				return TM_Invisible;
			}
			itemid = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(itemid))
			{
				UnlockReleaseBuffer(buffer);
				return TM_Invisible;
			}
			tuple_hdr = (RecnoTupleHeader *)
				PageGetItem(page, itemid);

			/* Check if tuple was deleted while we were unlocked */
			if (tuple_hdr->t_flags & RECNO_TUPLE_DELETED)
			{
				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = TransactionIdIsValid(dirty_xid) ?
						dirty_xid : GetCurrentTransactionId();
					tmfd->cmax = tuple_hdr->t_cid;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				return TM_Deleted;
			}

			/*
			 * Re-check visibility.  The concurrent modification may have
			 * committed while we were unlocked, making the tuple visible.
			 */
			if (recno_use_hlc)
				visible = RecnoTupleVisibleHLC(tuple_hdr,
											   RecnoGetSnapshotHLC(snapshot),
											   RelationGetRelid(relation),
											   (snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);
			else
				visible = RecnoTupleVisible(tuple_hdr,
											RecnoGetSnapshotTimestamp(snapshot), 0,
											RelationGetRelid(relation),
											(snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);

			if (!visible)
			{
				/* Still not visible — use our dirty_xid result */
				if (TransactionIdIsValid(dirty_xid) && is_insert_entry)
				{
					if (tmfd)
					{
						tmfd->ctid = *tid;
						tmfd->xmax = dirty_xid;
						tmfd->cmax = tuple_hdr->t_cid;
						tmfd->traversed = false;
					}
					UnlockReleaseBuffer(buffer);
					return TM_Invisible;
				}

				if (TransactionIdIsValid(dirty_xid) && !is_insert_entry)
				{
					if (wait)
					{
						TransactionId wait_xid = dirty_xid;

						UnlockReleaseBuffer(buffer);
						XactLockTableWait(wait_xid, relation,
										  tid, XLTW_Delete);

						buffer = ReadBuffer(relation, blkno);
						LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
						page = BufferGetPage(buffer);

						if (offnum < FirstOffsetNumber ||
							offnum > PageGetMaxOffsetNumber(page))
						{
							UnlockReleaseBuffer(buffer);
							return TM_Invisible;
						}
						itemid = PageGetItemId(page, offnum);
						if (!ItemIdIsNormal(itemid))
						{
							UnlockReleaseBuffer(buffer);
							return TM_Invisible;
						}
						tuple_hdr = (RecnoTupleHeader *)
							PageGetItem(page, itemid);

						if (tuple_hdr->t_flags & RECNO_TUPLE_DELETED)
						{
							if (tmfd)
							{
								tmfd->ctid = *tid;
								tmfd->xmax = wait_xid;
								tmfd->cmax = tuple_hdr->t_cid;
								tmfd->traversed = false;
							}
							UnlockReleaseBuffer(buffer);
							return TM_Deleted;
						}

						if (recno_use_hlc)
							visible = RecnoTupleVisibleHLC(tuple_hdr,
														   RecnoGetSnapshotHLC(snapshot),
														   RelationGetRelid(relation),
														   (snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);
						else
							visible = RecnoTupleVisible(tuple_hdr,
														RecnoGetSnapshotTimestamp(snapshot), 0,
														RelationGetRelid(relation),
														(snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);

						if (!visible)
						{
							/*
							 * Same EPQ livelock fix as the UPDATE
							 * path: check for our own LOCK entry
							 * before returning TM_Updated.
							 */
							TransactionId myxid_postw =
								GetCurrentTransactionIdIfAny();

							if (TransactionIdIsValid(myxid_postw))
							{
								RecnoSLogEntry my_epw;
								int		my_nfound_postw;

								my_nfound_postw = RecnoSLogLookup(
									RelationGetRelid(relation),
									tid, myxid_postw,
									&my_epw, 1);

								if (my_nfound_postw > 0)
								{
									/* Own LOCK entry → proceed */
								}
								else
								{
									if (tmfd)
									{
										tmfd->ctid = *tid;
										tmfd->xmax = wait_xid;
										tmfd->cmax =
											tuple_hdr->t_cid;
										tmfd->traversed = false;
									}
									UnlockReleaseBuffer(buffer);
									return TM_Updated;
								}
							}
							else
							{
								if (tmfd)
								{
									tmfd->ctid = *tid;
									tmfd->xmax = wait_xid;
									tmfd->cmax =
										tuple_hdr->t_cid;
									tmfd->traversed = false;
								}
								UnlockReleaseBuffer(buffer);
								return TM_Updated;
							}
						}
					}
					else
					{
						if (tmfd)
						{
							tmfd->ctid = *tid;
							tmfd->xmax = dirty_xid;
							tmfd->cmax = tuple_hdr->t_cid;
							tmfd->traversed = false;
						}
						UnlockReleaseBuffer(buffer);
						return TM_WouldBlock;
					}
				}
				else
				{
					/*
					 * No in-progress sLog entry for another txn.
					 * Same EPQ-loop fix as the UPDATE path: check
					 * if our transaction already has a sLog entry
					 * (from table_tuple_lock during EPQ).  If so,
					 * fall through; otherwise trigger EPQ.
					 */
					TransactionId myxid_chk =
						GetCurrentTransactionIdIfAny();

					if (TransactionIdIsValid(myxid_chk))
					{
						RecnoSLogEntry my_entry;
						int			my_nfound;

						my_nfound = RecnoSLogLookup(
							RelationGetRelid(relation),
							tid, myxid_chk, &my_entry, 1);
						if (my_nfound > 0)
						{
							/* EPQ already done; proceed. */
						}
						else
						{
							if (tmfd)
							{
								tmfd->ctid = *tid;
								tmfd->xmax =
									InvalidTransactionId;
								tmfd->cmax =
									tuple_hdr->t_cid;
								tmfd->traversed = false;
							}
							UnlockReleaseBuffer(buffer);
							return TM_Updated;
						}
					}
					else
					{
						if (tmfd)
						{
							tmfd->ctid = *tid;
							tmfd->xmax = InvalidTransactionId;
							tmfd->cmax = tuple_hdr->t_cid;
							tmfd->traversed = false;
						}
						UnlockReleaseBuffer(buffer);
						return TM_Updated;
					}
				}
			}
			/* If now visible, fall through to perform the delete */
		}
	}

	/*
	 * Get transaction timestamp BEFORE critical section.
	 * Use xact_ts as commit timestamp for within-transaction visibility.
	 */
	xact_ts = RecnoGetTransactionTimestamp();
	if (recno_use_hlc)
		current_ts = (uint64) RecnoGetDmlTimestamp();
	else
		current_ts = xact_ts;

	/*
	 * Allocate old_tuple structure and save a copy of the old tuple data
	 * BEFORE entering the critical section.  The tuple header will be
	 * modified below (DELETED flag, commit_ts, etc.), and the WAL record
	 * needs the unmodified before-image for UNDO support.
	 */
	{
		uint32		del_old_len = tuple_hdr->t_len;

		old_tuple_for_delete_wal = palloc(sizeof(RecnoTupleData));
		old_tuple_for_delete_wal->t_len = del_old_len;
		old_tuple_for_delete_wal->t_data = (RecnoTupleHeader *) palloc(del_old_len);
		memcpy(old_tuple_for_delete_wal->t_data, tuple_hdr, del_old_len);
	}

	/*
	 * Get current transaction ID BEFORE entering critical section, because
	 * GetCurrentTransactionId() may call XactLockTableInsert() which acquires
	 * a lock and allocates memory -- both forbidden in a critical section.
	 */
	xid = GetCurrentTransactionId();

	/*
	 * Pre-allocate WAL buffer space BEFORE entering critical section.
	 * DELETE operations only need the main buffer (no overflow).
	 *
	 * CRITICAL: XLogEnsureRecordSpace() may allocate memory, so it MUST
	 * be called outside the critical section.
	 */
	if (RelationNeedsWAL(relation))
		XLogEnsureRecordSpace(0, 2);

	/*
	 * Per-relation UNDO: Reserve space for a DELETE UNDO record with full
	 * tuple data. This allows rollback to restore the deleted tuple.
	 * Must happen before critical section since it may extend the UNDO fork.
	 */
	if (smgrexists(RelationGetSmgr(relation), RELUNDO_FORKNUM))
	{
		del_undo_record_size = SizeOfRelUndoRecordHeader +
							   sizeof(RelUndoDeletePayload) +
							   old_tuple_for_delete_wal->t_len;
		del_undo_ptr = RelUndoReserve(relation, del_undo_record_size,
									   &del_undo_buffer);
	}

	/* Start critical section for WAL logging */
	START_CRIT_SECTION();

	/* Mark tuple as deleted with tombstone - this is the key RECNO feature.
	 * Clear UNCOMMITTED: a tuple being deleted means its INSERT has committed
	 * (otherwise it wouldn't be visible to delete).  For self-deletes
	 * (INSERT + DELETE in same txn), UNDO handles rollback.  Clearing
	 * UNCOMMITTED ensures VACUUM can correctly identify committed deletes
	 * without consulting the sLog.
	 */
	tuple_hdr->t_flags |= RECNO_TUPLE_DELETED;
	tuple_hdr->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
	tuple_hdr->t_commit_ts = current_ts;
	tuple_hdr->t_cid = cid;
	tuple_hdr->t_xid_hint = GetTopTransactionId();
	/* Keep the original t_ctid for potential update chains */
	ItemPointerCopy(tid, &tuple_hdr->t_ctid);

	/* Update page header to match what redo does */
	{
		RecnoPageOpaque phdr = RecnoPageGetOpaque(page);

		phdr->pd_commit_ts = Max(phdr->pd_commit_ts, current_ts);
		phdr->pd_flags |= RECNO_PAGE_DEFRAG_NEEDED;
	}

	MarkBufferDirty(buffer);

	/* WAL log the deletion using the pre-saved old tuple copy */
	if (RelationNeedsWAL(relation))
	{
		XLogRecPtr	recptr;

		recptr = RecnoXLogDelete(relation, buffer, offnum,
								 old_tuple_for_delete_wal, current_ts);
		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/*
	 * Finish the per-relation UNDO record now that the delete is complete.
	 * Write the UNDO record with the deleted TID and full old tuple data
	 * so that rollback can restore the tuple.
	 */
	if (RelUndoRecPtrIsValid(del_undo_ptr))
	{
		RelUndoRecordHeader del_undo_hdr;
		RelUndoDeletePayload del_undo_payload;
		char	   *combined_payload;

		/* Build UNDO header */
		del_undo_hdr.urec_type = RELUNDO_DELETE;
		del_undo_hdr.urec_len = del_undo_record_size;
		del_undo_hdr.urec_xid = xid;
		del_undo_hdr.urec_prevundorec = InvalidRelUndoRecPtr;
		del_undo_hdr.info_flags = RELUNDO_INFO_HAS_TUPLE;
		del_undo_hdr.tuple_len = old_tuple_for_delete_wal->t_len;

		/* Build DELETE payload with single TID */
		del_undo_payload.ntids = 1;
		del_undo_payload.tids[0] = *tid;

		/* Combine payload and tuple data */
		combined_payload = palloc(sizeof(RelUndoDeletePayload) +
								  old_tuple_for_delete_wal->t_len);
		memcpy(combined_payload, &del_undo_payload, sizeof(RelUndoDeletePayload));
		memcpy(combined_payload + sizeof(RelUndoDeletePayload),
			   old_tuple_for_delete_wal->t_data,
			   old_tuple_for_delete_wal->t_len);

		/* Write UNDO record atomically */
		RelUndoFinish(relation, del_undo_buffer, del_undo_ptr, &del_undo_hdr,
					  combined_payload,
					  sizeof(RelUndoDeletePayload) + old_tuple_for_delete_wal->t_len);

		pfree(combined_payload);

		/* Register with transaction for rollback */
		RegisterPerRelUndo(RelationGetRelid(relation), del_undo_ptr);
	}

	/* Free old_tuple copy and structure after critical section */
	pfree(old_tuple_for_delete_wal->t_data);
	pfree(old_tuple_for_delete_wal);

	/*
	 * Clear visibility map bits for this page since we've deleted a tuple.
	 * The page is no longer all-visible.
	 */
	RecnoVMUpdateForDelete(relation, buffer);

	/*
	 * Clean up overflow chains if this tuple has overflow attributes. This
	 * must happen outside the critical section since it performs its own
	 * buffer I/O.  We check the flag and capture the free space before
	 * releasing the buffer so we can read the tuple header and page state.
	 */
	{
		bool		has_overflow = (tuple_hdr->t_flags & RECNO_TUPLE_HAS_OVERFLOW) != 0;
		Size		free_space = PageGetFreeSpace(page);

		UnlockReleaseBuffer(buffer);

		/*
		 * Register the delete in the sLog AFTER releasing the buffer lock
		 * to avoid deadlocks with RecnoSLogGetDirtyXid's slow path.
		 */
		RecnoSLogInsert(RelationGetRelid(relation), tid,
						GetTopTransactionId(), current_ts,
						cid, RECNO_SLOG_DELETE,
						GetCurrentSubTransactionId(), 0);

		/*
		 * NOTE: We do NOT immediately clean up overflow chains here.
		 * Immediate cleanup was: 1. Buggy (collected wrong overflow pointers
		 * after modification) 2. Expensive on hot paths (extra buffer I/O +
		 * locking) 3. Complex to WAL-log correctly
		 *
		 * Instead, overflow cleanup is deferred to VACUUM (like PostgreSQL's
		 * TOAST). When VACUUM prunes deleted tuples, it will also reclaim
		 * orphaned overflow pages.
		 *
		 * Future enhancement: Log overflow block/offset in WAL DELETE record
		 * so UNDO log pruning can also clean up overflow chains.
		 */
		(void) has_overflow;	/* Suppress unused variable warning */

		/* Release tuple lock */
		if (have_tuple_lock)
			RecnoUnlockTuple(relation, tid, LockTupleExclusive);

		/* Update free space map - deleted tuple creates more free space */
		RecnoRecordFreeSpace(relation, blkno, free_space);
	}

	/* Return success - tuple was successfully marked as deleted */
	return TM_Ok;
}

/*
 * Update a tuple in a RECNO table with versioning support
 */
TM_Result
recno_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
			   CommandId cid, uint32 options,
			   Snapshot snapshot, Snapshot crosscheck,
			   bool wait, TM_FailureData *tmfd,
			   LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	BlockNumber blkno;
	OffsetNumber offnum;
	Buffer		buffer;
	Page		page;
	ItemId		itemid;
	RecnoTupleHeader *old_tuple_hdr;
	RecnoTuple	new_tuple;
	Size		new_tuple_size;
	uint64		current_ts;
	uint64		xact_ts;
	bool		in_place_update = false;
	bool		old_has_overflow = false;
	bool		have_tuple_lock;
	RecnoTuple	old_tuple_for_inplace_wal;
	RecnoOverflowBuffers update_overflow_buffers;
	int			upd_i;
	Size		upd_undo_record_size;
	Buffer		upd_undo_buffer;
	RelUndoRecPtr upd_undo_ptr;
	bool		upd_use_inline_diff = false;
	RecnoInlineDiff upd_inline_diff_data;
	uint64		defrag_oldest_ts = 0;

	/* Extract block and offset from old TID */
	blkno = ItemPointerGetBlockNumber(otid);
	offnum = ItemPointerGetOffsetNumber(otid);

	/* Validate TID range */
	if (blkno >= RelationGetNumberOfBlocks(relation))
		return TM_Invisible;

	/* Read the page containing the old tuple */
	buffer = ReadBuffer(relation, blkno);

	/*
	 * Lock the buffer exclusively.  The exclusive buffer lock is sufficient
	 * to prevent concurrent modifications — heavyweight tuple locks are only
	 * needed for SELECT FOR UPDATE/SHARE (recno_tuple_lock), not for regular
	 * UPDATE.  This matches heap's approach.
	 */
	have_tuple_lock = false;
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);

	/* Validate offset number */
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
	{
		UnlockReleaseBuffer(buffer);
		return TM_Invisible;
	}

	/* Get the old tuple */
	itemid = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(itemid))
	{
		UnlockReleaseBuffer(buffer);
		return TM_Invisible;
	}

	old_tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);

	/* Check if old tuple has overflow chains to clean up later */
	old_has_overflow = (old_tuple_hdr->t_flags & RECNO_TUPLE_HAS_OVERFLOW) != 0;

	/* Check if tuple is already deleted */
	if (old_tuple_hdr->t_flags & RECNO_TUPLE_DELETED)
	{
		if (tmfd)
		{
			tmfd->ctid = *otid;
			tmfd->xmax = GetCurrentTransactionId();
			tmfd->cmax = old_tuple_hdr->t_cid;
			tmfd->traversed = false;
		}
		UnlockReleaseBuffer(buffer);
		return TM_Deleted;
	}

	/*
	 * Handle LOCKED flag: if this tuple is locked by the current transaction
	 * (FOR SHARE/FOR KEY SHARE/FOR UPDATE), the lock is compatible with
	 * UPDATE (self-lock).  Clear the LOCKED flag since we're about to modify
	 * the tuple.  The sLog LOCK entry will be overwritten by the UPDATE
	 * entry or cleaned up at commit.
	 */
	if (old_tuple_hdr->t_flags & RECNO_TUPLE_LOCKED)
	{
		RecnoSLogEntry lock_entry;
		int			nfound;

		nfound = RecnoSLogLookup(RelationGetRelid(relation), otid,
								 GetCurrentTransactionId(), &lock_entry, 1);
		if (nfound > 0 &&
			(lock_entry.op_type == RECNO_SLOG_LOCK_SHARE ||
			 lock_entry.op_type == RECNO_SLOG_LOCK_EXCL))
		{
			/* Our own lock - clear flag and proceed with update */
			old_tuple_hdr->t_flags &= ~RECNO_TUPLE_LOCKED;
		}
		/* If it's another transaction's lock, the existing concurrency
		 * control handles waiting via RecnoSLogGetDirtyXid. */
	}

	/*
	 * Fast-path: if UNCOMMITTED is set but no sLog entry exists, the
	 * previous transaction committed and its sLog cleanup already ran.
	 * Clear the stale flag now while we hold the buffer lock exclusively.
	 * This avoids the expensive sLog lookup inside the visibility check
	 * for the common case of UPDATing a recently-committed tuple.
	 *
	 * Only do this for tuples that are NOT deleted/updated (those flags
	 * indicate the tuple is being superseded, which requires the full
	 * visibility check to determine if the delete/update committed).
	 */
	if ((old_tuple_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
		!(old_tuple_hdr->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED)))
	{
		if (!RecnoSLogHasEntry(RelationGetRelid(relation), otid))
		{
			old_tuple_hdr->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
			/* Page will be dirtied by our upcoming update anyway */
		}
	}

	/*
	 * Check tuple visibility against snapshot and handle concurrent
	 * modifications.  Unlike a simple scan visibility check, UPDATE must
	 * distinguish between:
	 *   - Truly invisible (another txn's uncommitted insert) → TM_Invisible
	 *   - Concurrent update committed after our snapshot    → TM_Updated
	 *   - In-progress modification by another txn           → wait, retry
	 */
	if (snapshot)
	{
		bool		visible;

		if (recno_use_hlc)
			visible = RecnoTupleVisibleHLC(old_tuple_hdr, RecnoGetSnapshotHLC(snapshot),
										   RelationGetRelid(relation),
										   (snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);
		else
			visible = RecnoTupleVisible(old_tuple_hdr, RecnoGetSnapshotTimestamp(snapshot), 0,
										RelationGetRelid(relation),
										(snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);

		if (!visible)
		{
			TransactionId dirty_xid;
			bool		is_insert_entry;

			/*
			 * Release buffer lock before calling RecnoSLogGetDirtyXid.
			 * Its slow path acquires all sLog partition locks, which can
			 * deadlock with other backends holding sLog partition locks
			 * while waiting for this buffer lock.  Keep buffer pinned.
			 */
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

			dirty_xid = RecnoSLogGetDirtyXid(RelationGetRelid(relation),
											  otid,
											  &is_insert_entry);

			/*
			 * Re-acquire buffer lock and re-validate the tuple, since
			 * another backend may have modified it while unlocked.
			 */
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buffer);

			if (offnum < FirstOffsetNumber ||
				offnum > PageGetMaxOffsetNumber(page))
			{
				UnlockReleaseBuffer(buffer);
				return TM_Invisible;
			}
			itemid = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(itemid))
			{
				UnlockReleaseBuffer(buffer);
				return TM_Invisible;
			}
			old_tuple_hdr = (RecnoTupleHeader *)
				PageGetItem(page, itemid);

			/* Check if tuple was deleted while we were unlocked */
			if (old_tuple_hdr->t_flags & RECNO_TUPLE_DELETED)
			{
				if (tmfd)
				{
					tmfd->ctid = *otid;
					tmfd->xmax = TransactionIdIsValid(dirty_xid) ?
						dirty_xid : GetCurrentTransactionId();
					tmfd->cmax = old_tuple_hdr->t_cid;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				return TM_Deleted;
			}

			/*
			 * Re-check visibility.  The concurrent modification may have
			 * committed while we were unlocked, making the tuple visible.
			 */
			if (recno_use_hlc)
				visible = RecnoTupleVisibleHLC(old_tuple_hdr,
											   RecnoGetSnapshotHLC(snapshot),
											   RelationGetRelid(relation),
											   (snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);
			else
				visible = RecnoTupleVisible(old_tuple_hdr,
											RecnoGetSnapshotTimestamp(snapshot), 0,
											RelationGetRelid(relation),
											(snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);

			if (!visible)
			{
				/* Still not visible — use our dirty_xid result */
				if (TransactionIdIsValid(dirty_xid) && is_insert_entry)
				{
					/*
					 * Another txn's in-progress INSERT.  The tuple truly
					 * doesn't exist in our snapshot.
					 */
					if (tmfd)
					{
						tmfd->ctid = *otid;
						tmfd->xmax = dirty_xid;
						tmfd->cmax = old_tuple_hdr->t_cid;
						tmfd->traversed = false;
					}
					UnlockReleaseBuffer(buffer);
					return TM_Invisible;
				}

				if (TransactionIdIsValid(dirty_xid) && !is_insert_entry)
				{
					/*
					 * Another txn's in-progress UPDATE/DELETE.  Wait for it
					 * to finish and then retry (the tuple may be gone or
					 * changed).
					 */
					if (wait)
					{
						TransactionId wait_xid = dirty_xid;

						UnlockReleaseBuffer(buffer);
						XactLockTableWait(wait_xid, relation,
										  otid, XLTW_Update);

						/* Re-read the page and re-check after waking */
						buffer = ReadBuffer(relation, blkno);
						LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
						page = BufferGetPage(buffer);

						if (offnum < FirstOffsetNumber ||
							offnum > PageGetMaxOffsetNumber(page))
						{
							UnlockReleaseBuffer(buffer);
							return TM_Invisible;
						}
						itemid = PageGetItemId(page, offnum);
						if (!ItemIdIsNormal(itemid))
						{
							UnlockReleaseBuffer(buffer);
							return TM_Invisible;
						}
						old_tuple_hdr = (RecnoTupleHeader *)
							PageGetItem(page, itemid);

						/* If it got deleted while we waited, report that */
						if (old_tuple_hdr->t_flags & RECNO_TUPLE_DELETED)
						{
							if (tmfd)
							{
								tmfd->ctid = *otid;
								tmfd->xmax = wait_xid;
								tmfd->cmax = old_tuple_hdr->t_cid;
								tmfd->traversed = false;
							}
							UnlockReleaseBuffer(buffer);
							return TM_Deleted;
						}

						/*
						 * Re-check visibility.  The tuple was modified by the
						 * now-committed txn; its commit_ts is now later than
						 * our snapshot -> TM_Updated so the executor can EPQ.
						 */
						if (recno_use_hlc)
							visible = RecnoTupleVisibleHLC(old_tuple_hdr,
														   RecnoGetSnapshotHLC(snapshot),
														   RelationGetRelid(relation),
														   (snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);
						else
							visible = RecnoTupleVisible(old_tuple_hdr,
														RecnoGetSnapshotTimestamp(snapshot), 0,
														RelationGetRelid(relation),
														(snapshot->snapshot_type == SNAPSHOT_MVCC)
										   ? snapshot->curcid : InvalidCommandId,
										   buffer);

						if (!visible)
						{
							/*
							 * Still not visible after the waited-on txn
							 * committed.  Before returning TM_Updated (which
							 * triggers another EPQ cycle), check whether we
							 * already hold a LOCK entry from a previous EPQ
							 * iteration.  If so, we've already re-evaluated
							 * the quals and should proceed with the update
							 * instead of looping forever.
							 *
							 * Without this check, the following livelock
							 * occurs with hot-row contention:
							 *
							 *   1. We return TM_Updated → executor EPQ
							 *   2. table_tuple_lock inserts LOCK_EXCL
							 *   3. Retry → another txn is in-progress → wait
							 *   4. Waited txn commits → still not visible
							 *   5. Return TM_Updated → goto 2 (infinite)
							 *
							 * Each iteration leaks per-query memory in the
							 * executor, eventually causing OOM.
							 */
							TransactionId myxid_postw =
								GetCurrentTransactionIdIfAny();

							if (TransactionIdIsValid(myxid_postw))
							{
								RecnoSLogEntry my_entry_postw;
								int		my_nfound_postw;

								my_nfound_postw = RecnoSLogLookup(
									RelationGetRelid(relation),
									otid, myxid_postw,
									&my_entry_postw, 1);

								if (my_nfound_postw > 0)
								{
									/*
									 * Our LOCK entry from a prior EPQ
									 * cycle exists.  Fall through to
									 * perform the update.
									 */
								}
								else
								{
									if (tmfd)
									{
										tmfd->ctid = *otid;
										tmfd->xmax = wait_xid;
										tmfd->cmax =
											old_tuple_hdr->t_cid;
										tmfd->traversed = false;
									}
									UnlockReleaseBuffer(buffer);
									return TM_Updated;
								}
							}
							else
							{
								if (tmfd)
								{
									tmfd->ctid = *otid;
									tmfd->xmax = wait_xid;
									tmfd->cmax = old_tuple_hdr->t_cid;
									tmfd->traversed = false;
								}
								UnlockReleaseBuffer(buffer);
								return TM_Updated;
							}
						}
						/* Now visible — fall through to perform the update */
					}
					else
					{
						/* NOWAIT mode */
						if (tmfd)
						{
							tmfd->ctid = *otid;
							tmfd->xmax = dirty_xid;
							tmfd->cmax = old_tuple_hdr->t_cid;
							tmfd->traversed = false;
						}
						UnlockReleaseBuffer(buffer);
						return TM_WouldBlock;
					}
				}
				else
				{
					/*
					 * No in-progress sLog entry for another transaction.
					 * The modification has already committed.
					 *
					 * Check if our own transaction already has a sLog
					 * entry for this TID (e.g., LOCK_EXCL placed by
					 * table_tuple_lock during EvalPlanQual).  If so,
					 * EPQ already re-evaluated the WHERE clause and we
					 * should proceed with the update.
					 *
					 * Without this, we return TM_Updated endlessly:
					 * RECNO's in-place updates mean the tuple's
					 * commit_ts permanently exceeds the statement
					 * snapshot, so the executor's EPQ retry loop never
					 * terminates.
					 */
					TransactionId myxid_chk =
						GetCurrentTransactionIdIfAny();

					if (TransactionIdIsValid(myxid_chk))
					{
						RecnoSLogEntry my_entry;
						int			my_nfound;

						my_nfound = RecnoSLogLookup(
							RelationGetRelid(relation),
							otid, myxid_chk, &my_entry, 1);
						if (my_nfound > 0)
						{
							/*
							 * Our own sLog entry exists (LOCK from
							 * EPQ path).  Fall through to perform
							 * the update.
							 */
						}
						else
						{
							/*
							 * First encounter: trigger EPQ.
							 */
							if (tmfd)
							{
								tmfd->ctid = *otid;
								tmfd->xmax =
									InvalidTransactionId;
								tmfd->cmax =
									old_tuple_hdr->t_cid;
								tmfd->traversed = false;
							}
							UnlockReleaseBuffer(buffer);
							return TM_Updated;
						}
					}
					else
					{
						if (tmfd)
						{
							tmfd->ctid = *otid;
							tmfd->xmax = InvalidTransactionId;
							tmfd->cmax = old_tuple_hdr->t_cid;
							tmfd->traversed = false;
						}
						UnlockReleaseBuffer(buffer);
						return TM_Updated;
					}
				}
			}
			/* If now visible, fall through to perform the update */
		}
	}

	/*
	 * Get transaction timestamp BEFORE critical section.
	 * Use xact_ts as the commit timestamp for within-transaction visibility
	 * (RecnoTupleVisible checks tuple_commit_ts == xact_ts).  Using a
	 * different timestamp from RecnoGetCommitTimestamp() would make the
	 * updated tuple invisible both within the transaction and to the
	 * immediately-following transaction.
	 */
	xact_ts = RecnoGetTransactionTimestamp();
	if (recno_use_hlc)
		current_ts = (uint64) RecnoGetDmlTimestamp();
	else
		current_ts = xact_ts;

	/*
	 * Ensure the current transaction has an XID assigned BEFORE entering the
	 * critical section.  GetCurrentTransactionId() may call
	 * XactLockTableInsert() which acquires a lock and allocates memory --
	 * both forbidden in a critical section.
	 *
	 * Without an assigned XID, RecordTransactionCommit() considers the
	 * transaction read-only and skips the WAL flush, even though we write WAL
	 * records for the data change.  This would cause the update to be lost on
	 * crash recovery.
	 */
	(void) GetCurrentTransactionId();

	/*
	 * Form the new tuple from the slot.
	 *
	 * Fast path: If the old tuple is small (no overflow potential), keep the
	 * buffer locked and form the tuple without overflow handling.  This
	 * avoids the expensive unlock/relock cycle and the re-validation that
	 * follows.
	 *
	 * Slow path: For large tuples or those with existing overflow data,
	 * release the buffer lock first.  RecnoFormTuple may call
	 * RecnoStoreOverflowColumn which acquires buffer locks on overflow pages.
	 * If overflow data lands on the same page we're updating, that would
	 * cause a buffer lock re-entry assertion failure.
	 */
	update_overflow_buffers.count = 0;
	{
		bool		buffer_unlocked = false;

		if (old_tuple_hdr->t_len <= RECNO_OVERFLOW_THRESHOLD &&
			!(old_tuple_hdr->t_flags & RECNO_TUPLE_HAS_OVERFLOW))
		{
			/* Fast path: keep buffer locked, form tuple without overflow */
			slot_getallattrs(slot);
			new_tuple = RecnoFormTuple(RelationGetDescr(relation),
									   slot->tts_values,
									   slot->tts_isnull,
									   NULL,	/* skip overflow handling */
									   NULL);
		}
		else
		{
			/* Slow path: unlock, form tuple with overflow, relock after */
			buffer_unlocked = true;
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

			slot_getallattrs(slot);
			new_tuple = RecnoFormTuple(RelationGetDescr(relation),
									   slot->tts_values,
									   slot->tts_isnull,
									   relation,
									   &update_overflow_buffers);
		}

		/* Set MVCC fields for new tuple */
		new_tuple->t_data->t_commit_ts = current_ts;
		new_tuple->t_data->t_cid = cid;

		/*
		 * Mark the new tuple version as uncommitted.  Set t_xid_hint for
		 * fast visibility checks via CLOG/ProcArray.
		 */
		new_tuple->t_data->t_flags |= RECNO_TUPLE_UNCOMMITTED;
		new_tuple->t_data->t_xid_hint = GetTopTransactionId();

		new_tuple_size = new_tuple->t_len;

		/*
		 * Pre-compute the oldest active timestamp before (re-)acquiring
		 * the buffer lock.  This avoids an O(MaxBackends) shared-memory
		 * scan while holding a page-level exclusive lock.  The value is
		 * used by the defrag estimation/execution path below.
		 */
		defrag_oldest_ts = RecnoGetOldestActiveTimestamp();

		if (buffer_unlocked)
		{
			/*
			 * Re-acquire the buffer lock for the in-place update decision.
			 * Check if the main buffer is already locked as part of the
			 * overflow buffers to avoid double-lock assertion failure.
			 */
			bool		buffer_already_locked = false;

			for (upd_i = 0; upd_i < update_overflow_buffers.count; upd_i++)
			{
				if (update_overflow_buffers.buffers[upd_i].buffer == buffer)
				{
					buffer_already_locked = true;
					break;
				}
			}

			if (!buffer_already_locked)
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			page = BufferGetPage(buffer);

			/*
			 * Re-validate the tuple after re-locking.  Another backend may
			 * have reorganized the page while we didn't hold the lock.
			 */
			itemid = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(itemid))
			{
				UnlockReleaseBuffer(buffer);
				pfree(new_tuple);
				return TM_Invisible;
			}
			old_tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);
		}
	}

	/*
	 * Determine if we can do an in-place update.
	 *
	 * In-place update is RECNO's primary advantage over heap: it avoids
	 * creating dead tuple versions and the associated index maintenance. We
	 * try several strategies in order of increasing cost:
	 *
	 * 1. Direct fit: new tuple fits within the old tuple's slot. 2. Page
	 * space fit: new tuple is larger but the extra bytes fit in the page's
	 * available free space. 3. Defrag fit: page defragmentation frees enough
	 * space for the new tuple to fit in-place.
	 *
	 */
	if (new_tuple_size <= ItemIdGetLength(itemid))
	{
		/* Strategy 1: new tuple fits within old tuple's slot */
		in_place_update = true;
	}
	else if (new_tuple_size <= ItemIdGetLength(itemid) + PageGetFreeSpace(page))
	{
		/*
		 * Strategy 2: new tuple is larger but the difference fits in the
		 * page's free space.  We need to relocate the tuple data within the
		 * page, which PageRepairFragmentation can handle.
		 */
		in_place_update = true;
	}
	else
	{
		/*
		 * Strategy 3: try page defragmentation to reclaim dead tuple space.
		 * If the page has the defrag-needed flag and defragmentation would
		 * free enough space, do it now.
		 */
		RecnoPageOpaque upd_opaque = RecnoPageGetOpaque(page);

		if (upd_opaque->pd_flags & RECNO_PAGE_DEFRAG_NEEDED)
		{
			Size		potential_free;

			/*
			 * Estimate how much space defragmentation could free by scanning
			 * for dead tuples.  This is a quick scan without actually
			 * defragmenting yet.
			 */
			potential_free = PageGetFreeSpace(page);
			{
				OffsetNumber df_off;
				OffsetNumber df_maxoff = PageGetMaxOffsetNumber(page);

				for (df_off = FirstOffsetNumber; df_off <= df_maxoff; df_off++)
				{
					ItemId		df_itemid = PageGetItemId(page, df_off);
					RecnoTupleHeader *df_hdr;

					if (!ItemIdIsNormal(df_itemid))
					{
						if (ItemIdIsDead(df_itemid))
							potential_free += ItemIdGetLength(df_itemid) + sizeof(ItemIdData);
						continue;
					}

					if (RecnoIsOverflowRecord(PageGetItem(page, df_itemid),
											  ItemIdGetLength(df_itemid)))
						continue;

					df_hdr = (RecnoTupleHeader *) PageGetItem(page, df_itemid);

					if ((df_hdr->t_flags & RECNO_TUPLE_DELETED) &&
						!(df_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
						df_hdr->t_commit_ts < defrag_oldest_ts)
					{
						potential_free += ItemIdGetLength(df_itemid) + sizeof(ItemIdData);
					}
				}
			}

			if (new_tuple_size <= ItemIdGetLength(itemid) + potential_free)
			{
				/*
				 * Defragmentation should free enough space.  Do it now. We
				 * are already holding an exclusive lock on the buffer. First
				 * mark dead tuples as unused, then defragment.
				 *
				 * defrag_oldest_ts was pre-computed before acquiring the
				 * buffer lock to avoid shared-memory scans while holding
				 * page-level exclusive locks.
				 */
				START_CRIT_SECTION();
				{
					OffsetNumber prune_off;
					OffsetNumber prune_maxoff = PageGetMaxOffsetNumber(page);

					for (prune_off = FirstOffsetNumber; prune_off <= prune_maxoff; prune_off++)
					{
						ItemId		prune_itemid = PageGetItemId(page, prune_off);
						RecnoTupleHeader *prune_hdr;

						if (!ItemIdIsNormal(prune_itemid))
							continue;

						if (RecnoIsOverflowRecord(PageGetItem(page, prune_itemid),
												  ItemIdGetLength(prune_itemid)))
							continue;

						prune_hdr = (RecnoTupleHeader *) PageGetItem(page, prune_itemid);

						if ((prune_hdr->t_flags & RECNO_TUPLE_DELETED) &&
							!(prune_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
							prune_hdr->t_commit_ts < defrag_oldest_ts)
						{
							ItemIdSetUnused(prune_itemid);
						}
					}
				}
				RecnoPageDefragment(page);
				MarkBufferDirty(buffer);

				if (RelationNeedsWAL(relation))
				{
					XLogRecPtr	df_lsn;

					df_lsn = RecnoXLogDefrag(relation, buffer, NULL, 0, defrag_oldest_ts);
					PageSetLSN(page, df_lsn);
				}
				END_CRIT_SECTION();

				RecnoRecordFreeSpace(relation, blkno, PageGetFreeSpace(page));

				/*
				 * Re-fetch the item after defragmentation since line pointers
				 * may have been reorganized.  The offset number should still
				 * be valid for surviving tuples.
				 */
				itemid = PageGetItemId(page, offnum);
				old_tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);

				/* Check again if in-place update now fits */
				if (new_tuple_size <= ItemIdGetLength(itemid) + PageGetFreeSpace(page))
				{
					in_place_update = true;
					recno_stat_defrag_triggered_updates++;
				}
			}
		}
	}

	/*
	 * Override: disable in-place updates.
	 *
	 * In-place updates are currently unsafe because:
	 * (a) They overwrite the committed tuple data at the same TID.  After
	 *     ROLLBACK, the UNDO worker restores data asynchronously, creating a
	 *     window where the original committed data is invisible (the
	 *     visibility code sees UNCOMMITTED + aborted XID and cannot
	 *     distinguish an aborted INSERT from an aborted in-place UPDATE).
	 * (b) They return TU_None for index updates, leaving indexes stale when
	 *     indexed column values change.
	 *
	 * Re-enable once synchronous per-relation UNDO application is available,
	 * along with a HOT-style check that prevents in-place updates when
	 * indexed columns change.
	 */
	in_place_update = false;

	/*
	 * Save a copy of the old tuple data BEFORE entering the critical section
	 * and BEFORE modifying the page.  palloc is not allowed inside critical
	 * sections, and in-place updates overwrite the on-page data, so we must
	 * preserve the original tuple for WAL logging (the before-image).
	 *
	 * For small tuples (common case in pgbench-style OLTP), use stack buffers
	 * to avoid palloc overhead in the hot path.
	 */
	{
		uint32		old_len = old_tuple_hdr->t_len;
		char	   *old_copy;

		old_tuple_for_inplace_wal = palloc0(sizeof(RecnoTupleData));
		old_copy = palloc(old_len);

		memcpy(old_copy, old_tuple_hdr, old_len);
		old_tuple_for_inplace_wal->t_len = old_len;
		old_tuple_for_inplace_wal->t_data = (RecnoTupleHeader *) old_copy;
	}

	/*
	 * For in-place updates, record the old tuple in the per-relation UNDO log
	 * BEFORE modifying the page.  This allows ROLLBACK to restore the old
	 * tuple data if the transaction aborts.
	 *
	 * We only need UNDO for in-place updates because out-of-place updates
	 * leave the old tuple intact (marked UPDATED) and the new version can
	 * simply be made invisible by visibility checks on abort.
	 */
	if (in_place_update &&
		smgrexists(RelationGetSmgr(relation), RELUNDO_FORKNUM))
	{
		RelUndoRecPtr undo_ptr;
		Buffer		undo_buffer;
		RelUndoRecordHeader undo_hdr;
		RelUndoUpdatePayload undo_payload;
		uint32		old_len = old_tuple_for_inplace_wal->t_len;
		Size		undo_record_size;
		char	   *undo_combined_payload;

		/*
		 * Record size = header + update payload + old tuple data.
		 * The old tuple data is appended after the payload so that
		 * rollback can restore it.
		 */
		undo_record_size = SizeOfRelUndoRecordHeader +
			sizeof(RelUndoUpdatePayload) + old_len;

		/* Phase 1: Reserve space in the UNDO log */
		undo_ptr = RelUndoReserve(relation, undo_record_size, &undo_buffer);

		/* Build the UNDO record header */
		undo_hdr.urec_type = RELUNDO_UPDATE;
		undo_hdr.urec_len = undo_record_size;
		undo_hdr.urec_xid = GetCurrentTransactionId();
		undo_hdr.urec_prevundorec = GetPerRelUndoPtr(
			RelationGetRelid(relation));
		undo_hdr.info_flags = RELUNDO_INFO_HAS_TUPLE;
		undo_hdr.tuple_len = old_len;

		/* Build the UPDATE payload: old and new TIDs */
		ItemPointerSet(&undo_payload.oldtid, blkno, offnum);
		ItemPointerSet(&undo_payload.newtid, blkno, offnum);

		/*
		 * Build combined payload: update payload + old tuple data.
		 * RelUndoFinish writes [header][payload] to the UNDO page, so we
		 * pack the tuple data into the payload buffer.
		 */
		undo_combined_payload = palloc(sizeof(RelUndoUpdatePayload) + old_len);
		memcpy(undo_combined_payload,
			   &undo_payload, sizeof(RelUndoUpdatePayload));
		memcpy(undo_combined_payload + sizeof(RelUndoUpdatePayload),
			   old_tuple_for_inplace_wal->t_data, old_len);

		/* Phase 2: Complete the UNDO record */
		RelUndoFinish(relation, undo_buffer, undo_ptr, &undo_hdr,
					  undo_combined_payload,
					  sizeof(RelUndoUpdatePayload) + old_len);

		pfree(undo_combined_payload);

		/*
		 * Register this relation's UNDO chain with the transaction system
		 * so that abort processing can find and apply the UNDO records.
		 */
		RegisterPerRelUndo(RelationGetRelid(relation), undo_ptr);
	}

	/*
	 * Pre-allocate WAL buffer space BEFORE entering critical section.
	 * We may need to register the main buffer plus overflow buffers.
	 *
	 * rdata slots needed for UPDATE:
	 *   MAX_OVERFLOW_BUFFERS (data per overflow record, no separate header)
	 *   + 3 (xl_recno_update header + old tuple data + new tuple data)
	 *   + 1 (xl_recno_hlc_info when HLC mode is enabled)
	 *
	 * CRITICAL: XLogEnsureRecordSpace() may allocate memory, so it MUST
	 * be called outside the critical section.
	 */
	if (RelationNeedsWAL(relation))
		XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID, 4 + MAX_OVERFLOW_BUFFERS);

	/*
	 * Per-relation UNDO: Reserve space for an UPDATE UNDO record with old
	 * tuple data. This allows rollback to restore the original tuple and
	 * remove the updated tuple. Must happen before critical section.
	 *
	 * Inline diff optimization: If the diff between old and new tuple fits
	 * in the 14-byte inline diff area (single contiguous change of ≤ 10
	 * bytes), we store it directly in the tuple header and skip the UNDO
	 * fork entirely.  This avoids UNDO I/O for small changes like status
	 * flag toggles, boolean updates, or small counter increments.
	 */
	upd_undo_ptr = InvalidRelUndoRecPtr;

	/*
	 * Pre-compute inline diff candidate.  We check this before UNDO
	 * reservation so we can skip the fork I/O if the diff fits inline.
	 * Note: RecnoComputeTupleDiff only works for same-length tuples.
	 */
	memset(&upd_inline_diff_data, 0, sizeof(RecnoInlineDiff));

	if (in_place_update &&
		old_tuple_for_inplace_wal->t_len == new_tuple->t_len)
	{
		const char *old_bytes = (const char *) old_tuple_for_inplace_wal->t_data;
		const char *new_bytes = (const char *) new_tuple->t_data;
		Size		cmp_len = old_tuple_for_inplace_wal->t_len;
		Size		diff_start = 0;
		Size		diff_end = 0;
		bool		found_diff = false;
		Size		pos;

		/* Find the single contiguous region of difference */
		for (pos = 0; pos < cmp_len; pos++)
		{
			if (old_bytes[pos] != new_bytes[pos])
			{
				if (!found_diff)
				{
					diff_start = pos;
					found_diff = true;
				}
				diff_end = pos + 1;
			}
			else if (found_diff && (pos - diff_end) > 4)
			{
				/*
				 * Gap of > 4 identical bytes after a diff region means
				 * multiple disjoint changes — too complex for inline diff.
				 */
				found_diff = false;
				break;
			}
		}

		if (found_diff && (diff_end - diff_start) <= RECNO_INLINE_DIFF_MAX_BYTES)
		{
			/* Diff fits in inline diff area */
			upd_inline_diff_data.id_offset = (uint16) diff_start;
			upd_inline_diff_data.id_length = (uint16) (diff_end - diff_start);
			memcpy(upd_inline_diff_data.id_old_bytes,
				   old_bytes + diff_start,
				   diff_end - diff_start);
			upd_use_inline_diff = true;
		}
	}

	if (!upd_use_inline_diff &&
		smgrexists(RelationGetSmgr(relation), RELUNDO_FORKNUM))
	{
		upd_undo_record_size = SizeOfRelUndoRecordHeader +
							   sizeof(RelUndoUpdatePayload) +
							   old_tuple_for_inplace_wal->t_len;
		upd_undo_ptr = RelUndoReserve(relation, upd_undo_record_size,
									   &upd_undo_buffer);
	}

	/*
	 * For in-place updates, always set UNCOMMITTED so that visibility checks
	 * consult the sLog.  Even though the tuple position hasn't moved, the DATA
	 * has changed and other transactions must see the old data until this
	 * update commits.  The flag will be lazily cleared on the first visibility
	 * check after the updating transaction commits (since the sLog entry will
	 * have been removed at commit time).
	 */
	if (in_place_update)
	{
		new_tuple->t_data->t_flags |= RECNO_TUPLE_UNCOMMITTED;
		new_tuple->t_data->t_xid_hint = GetTopTransactionId();
	}

	/* Start critical section for WAL logging */
	START_CRIT_SECTION();

	if (in_place_update)
	{
		/*
		 * Set t_ctid on the in-memory new tuple BEFORE copying to the page.
		 * This ensures the WAL record's new_tuple image includes the correct
		 * t_ctid, so redo produces a page identical to the primary.  (We use
		 * blkno/offnum which is correct for both the "fits in existing slot"
		 * and "delete+re-add" strategies since we update offnum below if it
		 * changes.)
		 */
		ItemPointerSet(&new_tuple->t_data->t_ctid, blkno, offnum);

		/*
		 * If using inline diff, store the diff in the new tuple's header
		 * before copying to page.  This makes the diff WAL-logged as part
		 * of the full-page image and available for version reconstruction.
		 */
		if (upd_use_inline_diff)
		{
			/*
			 * Store the inline diff after the attrs_bitmap in the tuple
			 * header.  The HAS_INLINE_DIFF flag tells deform to expect it.
			 */
			int		bitmap_len = (new_tuple->t_data->t_natts > 0)
				? ((new_tuple->t_data->t_natts + 7) / 8) : 0;
			RecnoInlineDiff *diff_ptr = (RecnoInlineDiff *)
				(new_tuple->t_data->t_attrs_bitmap + bitmap_len);

			*diff_ptr = upd_inline_diff_data;
			new_tuple->t_data->t_flags |= RECNO_TUPLE_HAS_INLINE_DIFF;
		}

		if (new_tuple_size <= ItemIdGetLength(itemid))
		{
			/*
			 * New tuple fits within the old tuple's allocated space.
			 * Overwrite directly -- safe because we don't exceed the existing
			 * allocation.
			 */
			memcpy(old_tuple_hdr, new_tuple->t_data, new_tuple_size);

			/* Update item length if it shrank */
			if (new_tuple_size != ItemIdGetLength(itemid))
				ItemIdSetNormal(itemid, ItemIdGetOffset(itemid), new_tuple_size);
		}
		else
		{
			/*
			 * New tuple is larger than the old one but fits on the page
			 * (Strategy 2 or 3).  We cannot memcpy in place because that
			 * would overwrite adjacent data.  Instead, remove the old line
			 * pointer entry, compact the page, and re-add the new tuple at
			 * the same offset.
			 *
			 * We use RecnoPageIndexTupleDelete instead of the standard
			 * PageIndexTupleDelete because RECNO pages may contain
			 * LP_UNUSED items left by opportunistic defragmentation.
			 * PageIndexTupleDelete asserts all items are LP_NORMAL, which
			 * fails when LP_UNUSED items are present.
			 * RecnoPageIndexTupleDelete skips LP_UNUSED items in the
			 * offset adjustment loop.
			 */
			RecnoPageIndexTupleDelete(page, offnum);

			offnum = PageAddItem(page, new_tuple->t_data,
								 new_tuple_size,
								 offnum, false, false);

			if (offnum == InvalidOffsetNumber)
				elog(PANIC, "failed to re-add RECNO tuple after delete for growing update");

			/* Re-fetch itemid and header from the (same) location */
			itemid = PageGetItemId(page, offnum);
			old_tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);

			ItemPointerSet(&new_tuple->t_data->t_ctid, blkno, offnum);
		}

		/* Set new TID to same location */
		ItemPointerSet(&slot->tts_tid, blkno, offnum);
		new_tuple->t_self = slot->tts_tid;

		/*
		 * t_ctid on the on-disk tuple is already correct from the memcpy or
		 * PageAddItem above, since we set it on new_tuple->t_data before
		 * copying.
		 */

		/* Track in-place update success */
		recno_stat_in_place_updates++;
	}
	else
	{
		/* Out-of-place update - need to find new location */
		OffsetNumber new_offnum;
		Buffer		new_buffer = InvalidBuffer;
		BlockNumber new_blkno;
		Page		new_page;
		bool		same_page = true;
		RecnoTuple	old_tuple_for_wal;

		recno_stat_out_of_place_updates++;

		/* Try to add new tuple to same page */
		new_offnum = RecnoPageAddTuple(page, new_tuple, new_tuple_size);

		if (new_offnum == InvalidOffsetNumber)
		{
			/*
			 * Page is full.  Before going cross-page, try opportunistic
			 * cleanup: if the page has dead tuples that can be reclaimed,
			 * defragment it and retry the insert on the same page.
			 */
			RecnoPageOpaque prune_opaque = RecnoPageGetOpaque(page);

			END_CRIT_SECTION();

			if (prune_opaque->pd_flags & RECNO_PAGE_DEFRAG_NEEDED)
			{
				uint64		prune_oldest_ts = RecnoGetOldestActiveTimestamp();
				int			prune_ndead = 0;
				OffsetNumber prune_maxoff = PageGetMaxOffsetNumber(page);
				OffsetNumber prune_off;

				for (prune_off = FirstOffsetNumber;
					 prune_off <= prune_maxoff;
					 prune_off++)
				{
					ItemId		pid = PageGetItemId(page, prune_off);
					RecnoTupleHeader *phdr;

					if (!ItemIdIsNormal(pid))
						continue;
					if (RecnoIsOverflowRecord(PageGetItem(page, pid),
											  ItemIdGetLength(pid)))
						continue;

					phdr = (RecnoTupleHeader *) PageGetItem(page, pid);
					if ((phdr->t_flags & RECNO_TUPLE_DELETED) &&
						!(phdr->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
						phdr->t_commit_ts < prune_oldest_ts)
						prune_ndead++;
				}

				if (prune_ndead > 0)
				{
					START_CRIT_SECTION();

					/* Mark dead tuples as unused before defrag */
					for (prune_off = FirstOffsetNumber;
						 prune_off <= prune_maxoff;
						 prune_off++)
					{
						ItemId		pid2 = PageGetItemId(page, prune_off);
						RecnoTupleHeader *phdr2;

						if (!ItemIdIsNormal(pid2))
							continue;
						if (RecnoIsOverflowRecord(PageGetItem(page, pid2),
												  ItemIdGetLength(pid2)))
							continue;

						phdr2 = (RecnoTupleHeader *) PageGetItem(page, pid2);
						if ((phdr2->t_flags & RECNO_TUPLE_DELETED) &&
							!(phdr2->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
							phdr2->t_commit_ts < prune_oldest_ts)
						{
							ItemIdSetUnused(pid2);
						}
					}

					RecnoPageDefragment(page);
					MarkBufferDirty(buffer);

					if (RelationNeedsWAL(relation))
					{
						XLogRecPtr	prune_lsn;

						prune_lsn = RecnoXLogDefrag(relation, buffer,
													NULL, 0, prune_oldest_ts);
						PageSetLSN(page, prune_lsn);
					}
					END_CRIT_SECTION();

					RecnoRecordFreeSpace(relation, blkno,
										 PageGetFreeSpace(page));

					/* Retry adding tuple to the now-compacted page */
					START_CRIT_SECTION();
					new_offnum = RecnoPageAddTuple(page, new_tuple,
												   new_tuple_size);
					if (new_offnum != InvalidOffsetNumber)
					{
						ItemId		new_iid_prune;
						RecnoTupleHeader *new_hdr_prune;

						/* Success -- continue with same-page update */
						old_tuple_hdr = (RecnoTupleHeader *)
							PageGetItem(page, PageGetItemId(page, offnum));
						old_tuple_hdr->t_flags |= RECNO_TUPLE_UPDATED;
						old_tuple_hdr->t_commit_ts = current_ts;
						old_tuple_hdr->t_xid_hint = GetTopTransactionId();
						ItemPointerSet(&old_tuple_hdr->t_ctid, blkno,
									   new_offnum);
						ItemPointerSet(&slot->tts_tid, blkno, new_offnum);
						new_tuple->t_self = slot->tts_tid;

						/* Set t_ctid on the new on-disk tuple */
						new_iid_prune = PageGetItemId(page, new_offnum);
						new_hdr_prune = (RecnoTupleHeader *)
							PageGetItem(page, new_iid_prune);
						ItemPointerSet(&new_hdr_prune->t_ctid, blkno,
									   new_offnum);

						goto update_inplace_finalize;
					}
					END_CRIT_SECTION();
				}
			}

			/* Find a page with enough space for the new tuple */
			new_blkno = RecnoGetPageWithFreeSpace(relation, new_tuple_size);
			if (new_blkno == InvalidBlockNumber)
			{
			/* Clean up overflow buffers before throwing error */
			for (upd_i = 0; upd_i < update_overflow_buffers.count; upd_i++)
			{
				UnlockReleaseBuffer(update_overflow_buffers.buffers[upd_i].buffer);
				pfree(update_overflow_buffers.buffers[upd_i].record_data);
			}
				UnlockReleaseBuffer(buffer);
				pfree(new_tuple);
				elog(ERROR, "RECNO update failed - could not allocate page for new tuple version");
			}

			/*
			 * If we got a different page, lock it. We need to be careful with
			 * lock ordering to avoid deadlocks: always lock lower block
			 * number first.
			 */
			if (new_blkno != blkno)
			{
				same_page = false;

				if (new_blkno < blkno)
				{
					/* Lock new page first, then re-lock old page */
					UnlockReleaseBuffer(buffer);
					new_buffer = ReadBuffer(relation, new_blkno);
					LockBuffer(new_buffer, BUFFER_LOCK_EXCLUSIVE);
					buffer = ReadBuffer(relation, blkno);
					LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
					page = BufferGetPage(buffer);
				}
				else
				{
					/* Old page already locked, lock new page second */
					new_buffer = ReadBuffer(relation, new_blkno);
					LockBuffer(new_buffer, BUFFER_LOCK_EXCLUSIVE);
				}

				new_page = BufferGetPage(new_buffer);

				/* Verify new page still has space */
				if (PageGetFreeSpace(new_page) < new_tuple_size)
				{
				/* Clean up overflow buffers before throwing error */
				for (upd_i = 0; upd_i < update_overflow_buffers.count; upd_i++)
				{
					UnlockReleaseBuffer(update_overflow_buffers.buffers[upd_i].buffer);
					pfree(update_overflow_buffers.buffers[upd_i].record_data);
				}
					RecnoRecordFreeSpace(relation, new_blkno, PageGetFreeSpace(new_page));
					UnlockReleaseBuffer(new_buffer);
					UnlockReleaseBuffer(buffer);
					pfree(new_tuple);
					elog(ERROR, "RECNO update failed - new page no longer has sufficient space");
				}
			}
			else
			{
				/*
				 * Got same page back from FSM (shouldn't happen but handle
				 * it)
				 */
				new_buffer = buffer;
				new_page = page;
			}

			/*
			 * Allocate old_tuple structure and save a copy of the old tuple
			 * data BEFORE modifying the page.  The old tuple header's flags
			 * and commit_ts will be changed below, so we need the original
			 * before-image for WAL logging.
			 */
			old_tuple_for_wal = palloc0(sizeof(RecnoTupleData));
			{
				uint32		cp_len = old_tuple_hdr->t_len;
				char	   *cp_data = palloc(cp_len);

				memcpy(cp_data, old_tuple_hdr, cp_len);
				old_tuple_for_wal->t_len = cp_len;
				old_tuple_for_wal->t_data = (RecnoTupleHeader *) cp_data;
			}


			/* Start new critical section for the actual update */
			START_CRIT_SECTION();

			/* Add tuple to new page */
			new_offnum = RecnoPageAddTuple(new_page, new_tuple, new_tuple_size);
			if (new_offnum == InvalidOffsetNumber)
			{
				END_CRIT_SECTION();
				pfree(old_tuple_for_wal->t_data);
				pfree(old_tuple_for_wal);
				if (!same_page)
					UnlockReleaseBuffer(new_buffer);
				UnlockReleaseBuffer(buffer);
				pfree(new_tuple);
				elog(PANIC, "RECNO failed to add tuple to page after verifying space");
			}

			/* Mark old tuple as updated and point to new version */
			old_tuple_hdr->t_flags |= RECNO_TUPLE_UPDATED;
			old_tuple_hdr->t_commit_ts = current_ts;
			old_tuple_hdr->t_xid_hint = GetTopTransactionId();
			ItemPointerSet(&old_tuple_hdr->t_ctid, new_blkno, new_offnum);

			/* Set new TID */
			ItemPointerSet(&slot->tts_tid, new_blkno, new_offnum);
			new_tuple->t_self = slot->tts_tid;

			/*
			 * Set t_ctid on both the in-memory new tuple (for WAL) and the
			 * on-disk tuple to point to itself.
			 */
			ItemPointerSet(&new_tuple->t_data->t_ctid, new_blkno, new_offnum);
			{
				ItemId		new_iid = PageGetItemId(new_page, new_offnum);
				RecnoTupleHeader *new_hdr_ondisk =
					(RecnoTupleHeader *) PageGetItem(new_page, new_iid);

				ItemPointerSet(&new_hdr_ondisk->t_ctid, new_blkno, new_offnum);
			}

			/* Mark both pages dirty */
			MarkBufferDirty(buffer);
			if (!same_page)
				MarkBufferDirty(new_buffer);

			/* WAL log the cross-page update using the pre-saved old tuple */
			if (RelationNeedsWAL(relation))
			{
				RecnoXLogUpdate(relation, buffer, offnum,
								old_tuple_for_wal, new_tuple,
								old_tuple_for_wal->t_data->t_commit_ts,
								current_ts,
								&update_overflow_buffers,
								same_page ? InvalidBuffer : new_buffer);
			}

			END_CRIT_SECTION();

		/* Finish UNDO record for cross-page update if reserved */
		if (RelUndoRecPtrIsValid(upd_undo_ptr))
		{
			RelUndoRecordHeader upd_undo_hdr;
			RelUndoUpdatePayload upd_undo_payload;
			char *upd_combined_payload;

			upd_undo_hdr.urec_type = RELUNDO_UPDATE;
			upd_undo_hdr.urec_len = upd_undo_record_size;
			upd_undo_hdr.urec_xid = GetCurrentTransactionId();
			upd_undo_hdr.urec_prevundorec = GetPerRelUndoPtr(
				RelationGetRelid(relation));
			upd_undo_hdr.info_flags = RELUNDO_INFO_HAS_TUPLE;
			upd_undo_hdr.tuple_len = old_tuple_for_wal->t_len;

			upd_undo_payload.oldtid = *otid;
			upd_undo_payload.newtid = slot->tts_tid;

			upd_combined_payload = palloc(sizeof(RelUndoUpdatePayload) +
										  old_tuple_for_wal->t_len);
			memcpy(upd_combined_payload, &upd_undo_payload, sizeof(RelUndoUpdatePayload));
			memcpy(upd_combined_payload + sizeof(RelUndoUpdatePayload),
				   old_tuple_for_wal->t_data,
				   old_tuple_for_wal->t_len);

			RelUndoFinish(relation, upd_undo_buffer, upd_undo_ptr, &upd_undo_hdr,
						  upd_combined_payload,
						  sizeof(RelUndoUpdatePayload) + old_tuple_for_wal->t_len);

			pfree(upd_combined_payload);
			RegisterPerRelUndo(RelationGetRelid(relation), upd_undo_ptr);
		}

			/*
			 * Release all overflow buffers and free their cached data.
			 *
			 * IMPORTANT: Due to spatial locality optimization, overflow buffers
			 * might be on either the old page (buffer) or new page (new_buffer).
			 * These will be released separately below, so skip them here.
			 */
			for (upd_i = 0; upd_i < update_overflow_buffers.count; upd_i++)
			{
				Buffer		ovf_buf = update_overflow_buffers.buffers[upd_i].buffer;

				if (ovf_buf != buffer && ovf_buf != new_buffer)
					UnlockReleaseBuffer(ovf_buf);
				pfree(update_overflow_buffers.buffers[upd_i].record_data);
			}

			/* Free old_tuple copy and structure after critical section */
			pfree(old_tuple_for_wal->t_data);
			pfree(old_tuple_for_wal);

			/*
			 * Clear visibility map bits for affected pages. Updates always
			 * clear the bits since they create new tuple versions.
			 */
			RecnoVMUpdateForUpdate(relation, buffer);
			if (!same_page)
				RecnoVMUpdateForUpdate(relation, new_buffer);

			/* Update FSM for both pages */
			RecnoRecordFreeSpace(relation, blkno, PageGetFreeSpace(page));
			if (!same_page)
				RecnoRecordFreeSpace(relation, new_blkno, PageGetFreeSpace(new_page));

			/* Release buffers */
			UnlockReleaseBuffer(buffer);
			if (!same_page)
				UnlockReleaseBuffer(new_buffer);

			/*
			 * Register the cross-page update in the sLog AFTER releasing
			 * buffers to avoid deadlocks with RecnoSLogGetDirtyXid.
			 */
			RecnoSLogInsert(RelationGetRelid(relation), &slot->tts_tid,
							GetTopTransactionId(), current_ts,
							cid, RECNO_SLOG_UPDATE,
							GetCurrentSubTransactionId(), 0);
			/* Also register for old TID so rollback is detectable */
			RecnoSLogInsert(RelationGetRelid(relation), otid,
							GetTopTransactionId(), current_ts,
							cid, RECNO_SLOG_UPDATE,
							GetCurrentSubTransactionId(), 0);

			/* Release tuple lock */
			if (have_tuple_lock)
				RecnoUnlockTuple(relation, otid, LockTupleExclusive);

			/* Out-of-place update: TID changed, indexes need updating */
			if (update_indexes)
				*update_indexes = TU_All;

			pfree(new_tuple);


			return TM_Ok;
		}

		/* Same-page update succeeded */
		/* Mark old tuple as updated and point to new version */
		old_tuple_hdr->t_flags |= RECNO_TUPLE_UPDATED;
		old_tuple_hdr->t_commit_ts = current_ts;
		old_tuple_hdr->t_xid_hint = GetTopTransactionId();
		ItemPointerSet(&old_tuple_hdr->t_ctid, blkno, new_offnum);

		/* Set new TID */
		ItemPointerSet(&slot->tts_tid, blkno, new_offnum);
		new_tuple->t_self = slot->tts_tid;
	}

update_inplace_finalize:
	slot->tts_tableOid = RelationGetRelid(relation);

	/*
	 * Update page opaque header to track the latest commit timestamp and
	 * current free space.  This must happen before MarkBufferDirty and WAL
	 * logging so that full-page images capture the correct opaque state.  The
	 * redo function performs the same updates so WAL consistency checking
	 * passes.
	 */
	{
		RecnoPageOpaque upd_phdr = RecnoPageGetOpaque(page);

		upd_phdr->pd_commit_ts = Max(upd_phdr->pd_commit_ts, current_ts);
		upd_phdr->pd_free_space = PageGetFreeSpace(page);
	}

	MarkBufferDirty(buffer);

	/* WAL log the update with all overflow buffers atomically */
	if (RelationNeedsWAL(relation))
	{
		/*
		 * old_tuple_for_inplace_wal was populated with a copy of the old
		 * tuple data BEFORE we modified the page.  Use its saved
		 * old_commit_ts for the WAL record so the before-image is correct.
		 */
		RecnoXLogUpdate(relation, buffer, offnum,
						old_tuple_for_inplace_wal, new_tuple,
						old_tuple_for_inplace_wal->t_data->t_commit_ts,
						current_ts,
						&update_overflow_buffers,
						InvalidBuffer);
	}

	END_CRIT_SECTION();

	/*
	 * Finish the per-relation UNDO record now that the in-place update is
	 * complete.
	 *
	 * If we used inline diff, the old bytes are already stored in the tuple
	 * header on the page (WAL-logged as part of the full-page image).
	 * Rollback is handled by checking RECNO_TUPLE_HAS_INLINE_DIFF during
	 * abort processing — no UNDO fork record needed.
	 *
	 * Otherwise, write the UNDO record with old/new TID mapping and old tuple
	 * data so that rollback can restore the original tuple.
	 *
	 * CTR optimization: Try byte-diff first. If the diff is compact (less
	 * than 50% of tuple size), use RELUNDO_DELTA_UPDATE to save UNDO space.
	 * Otherwise fall back to RELUNDO_UPDATE with the full old tuple.
	 */
	if (upd_use_inline_diff)
	{
		/*
		 * Inline diff path: the old bytes are stored after the attrs_bitmap
		 * in the tuple header, written to the page during the critical
		 * section above.  No UNDO fork record needed.
		 *
		 * For rollback, the sLog entry identifies this as an in-progress
		 * update; abort cleanup can reconstruct the old version from the
		 * inline diff.  Lazy cleanup during subsequent page access will
		 * apply the diff and clear the flag.
		 */
		elog(DEBUG2, "RECNO: used inline diff for update (offset=%u, len=%u)",
			 upd_inline_diff_data.id_offset, upd_inline_diff_data.id_length);
	}
	else if (RelUndoRecPtrIsValid(upd_undo_ptr))
	{
		RelUndoRecordHeader upd_undo_hdr;
		RecnoDiffRecord *diff = NULL;
		char	   *upd_combined_payload;
		Size		payload_total;

		/*
		 * Try to compute a byte-diff for compact storage.
		 * This only works for same-length in-place updates.
		 */
		diff = RecnoComputeTupleDiff(
			(const char *) old_tuple_for_inplace_wal->t_data,
			old_tuple_for_inplace_wal->t_len,
			(const char *) new_tuple->t_data,
			new_tuple->t_len);

		if (diff != NULL && RecnoDiffIsCompact(diff, old_tuple_for_inplace_wal->t_len))
		{
			/* Use compact byte-diff UNDO record */
			RelUndoDeltaUpdatePayload du_payload;

			du_payload.oldtid = *otid;
			du_payload.newtid = slot->tts_tid;
			du_payload.diff_len = diff->total_size;

			payload_total = sizeof(RelUndoDeltaUpdatePayload) + diff->total_size;

			upd_undo_hdr.urec_type = RELUNDO_DELTA_UPDATE;
			upd_undo_hdr.urec_len = SizeOfRelUndoRecordHeader + payload_total;
			upd_undo_hdr.urec_xid = GetCurrentTransactionId();
			upd_undo_hdr.urec_prevundorec = GetPerRelUndoPtr(
				RelationGetRelid(relation));
			upd_undo_hdr.info_flags = RELUNDO_INFO_PARTIAL_TUPLE;
			upd_undo_hdr.tuple_len = 0;

			upd_combined_payload = palloc(payload_total);
			memcpy(upd_combined_payload, &du_payload,
				   sizeof(RelUndoDeltaUpdatePayload));
			memcpy(upd_combined_payload + sizeof(RelUndoDeltaUpdatePayload),
				   diff, diff->total_size);

			RelUndoFinish(relation, upd_undo_buffer, upd_undo_ptr,
						  &upd_undo_hdr, upd_combined_payload, payload_total);

			pfree(upd_combined_payload);
			pfree(diff);
		}
		else
		{
			/* Fall back to full tuple UNDO record */
			RelUndoUpdatePayload upd_undo_payload;

			if (diff)
				pfree(diff);

			upd_undo_hdr.urec_type = RELUNDO_UPDATE;
			upd_undo_hdr.urec_len = upd_undo_record_size;
			upd_undo_hdr.urec_xid = GetCurrentTransactionId();
			upd_undo_hdr.urec_prevundorec = GetPerRelUndoPtr(
				RelationGetRelid(relation));
			upd_undo_hdr.info_flags = RELUNDO_INFO_HAS_TUPLE;
			upd_undo_hdr.tuple_len = old_tuple_for_inplace_wal->t_len;

			upd_undo_payload.oldtid = *otid;
			upd_undo_payload.newtid = slot->tts_tid;

			payload_total = sizeof(RelUndoUpdatePayload) +
				old_tuple_for_inplace_wal->t_len;
			upd_combined_payload = palloc(payload_total);
			memcpy(upd_combined_payload, &upd_undo_payload,
				   sizeof(RelUndoUpdatePayload));
			memcpy(upd_combined_payload + sizeof(RelUndoUpdatePayload),
				   old_tuple_for_inplace_wal->t_data,
				   old_tuple_for_inplace_wal->t_len);

			RelUndoFinish(relation, upd_undo_buffer, upd_undo_ptr,
						  &upd_undo_hdr, upd_combined_payload, payload_total);

			pfree(upd_combined_payload);
		}

		/* Register with transaction for rollback */
		RegisterPerRelUndo(RelationGetRelid(relation), upd_undo_ptr);
	}

	/* Free old_tuple copy and structure after critical section */
	pfree(old_tuple_for_inplace_wal->t_data);
	pfree(old_tuple_for_inplace_wal);

	/*
	 * Release all overflow buffers and free their cached data.
	 *
	 * IMPORTANT: Due to spatial locality optimization, an overflow buffer
	 * might be the SAME as the main buffer or as another overflow buffer.
	 * Skip releasing buffers that were already released.
	 */
	for (upd_i = 0; upd_i < update_overflow_buffers.count; upd_i++)
	{
		Buffer		ovf_buf = update_overflow_buffers.buffers[upd_i].buffer;
		bool		already_released = (ovf_buf == buffer);
		int			dup_j;

		for (dup_j = 0; dup_j < upd_i && !already_released; dup_j++)
		{
			if (update_overflow_buffers.buffers[dup_j].buffer == ovf_buf)
				already_released = true;
		}

		if (!already_released)
			UnlockReleaseBuffer(ovf_buf);
		pfree(update_overflow_buffers.buffers[upd_i].record_data);
	}

	/*
	 * Clear visibility map bits for this page. In-place updates still create
	 * new tuple versions that may not be immediately visible to all
	 * transactions.
	 */
	RecnoVMUpdateForUpdate(relation, buffer);

	/*
	 * Capture free space while we still hold the buffer lock, before
	 * releasing it.
	 */
	{
		Size		update_free_space = PageGetFreeSpace(page);

		UnlockReleaseBuffer(buffer);

		/*
		 * Register the update in the sLog AFTER releasing the buffer lock
		 * to avoid deadlocks with RecnoSLogGetDirtyXid's slow path.
		 */
		RecnoSLogInsert(RelationGetRelid(relation), &slot->tts_tid,
						GetTopTransactionId(), current_ts,
						cid, RECNO_SLOG_UPDATE,
						GetCurrentSubTransactionId(), 0);

		/*
		 * For out-of-place updates, also register sLog entry for the old
		 * TID so that the old tuple's UPDATED flag can be detected as
		 * rolled back if this transaction aborts.
		 */
		if (!in_place_update)
		{
			RecnoSLogInsert(RelationGetRelid(relation), otid,
							GetTopTransactionId(), current_ts,
							cid, RECNO_SLOG_UPDATE,
							GetCurrentSubTransactionId(), 0);
		}

		/*
		 * NOTE: We do NOT immediately clean up overflow chains here.
		 * Immediate cleanup was: 1. Buggy (collected wrong overflow pointers
		 * after in-place modification) 2. Expensive on hot paths (extra
		 * buffer I/O + locking during UPDATE) 3. Complex to WAL-log correctly
		 *
		 * Instead, overflow cleanup is deferred to VACUUM (like PostgreSQL's
		 * TOAST). When VACUUM prunes deleted tuples, it will also reclaim
		 * orphaned overflow pages.
		 *
		 * Future enhancement: Log overflow block/offset in WAL UPDATE record
		 * so UNDO log pruning can also clean up overflow chains.
		 */
		(void) old_has_overflow;	/* Suppress unused variable warning */

		/* Release tuple lock */
		if (have_tuple_lock)
			RecnoUnlockTuple(relation, otid, LockTupleExclusive);

		/* Update free space map with previously captured free space */
		RecnoRecordFreeSpace(relation, blkno, update_free_space);
	}

	/* Return that indexes need updating if this was out-of-place */
	if (update_indexes)
		*update_indexes = in_place_update ? TU_None : TU_All;

	pfree(new_tuple);

	return TM_Ok;
}

/*
 * Multi-insert operation for bulk loading (batched page-at-a-time)
 *
 * Pre-forms all tuples, then inserts them page-at-a-time to minimize
 * per-tuple overhead: one FSM lookup, one buffer lock, one WAL record,
 * and one UNDO reservation per page batch instead of per tuple.
 *
 * Tuples that are too large for batch handling (need overflow) are
 * inserted individually via the single-insert path.
 */
void
recno_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
				   CommandId cid, uint32 options, BulkInsertState bistate)
{
	RecnoTuple *formed_tuples;
	bool	   *needs_single_insert;
	uint64		current_ts;
	uint64		xact_ts;
	int			i;
	int			ndone;

	if (ntuples <= 0)
		return;

	/*
	 * Get timestamps and XID outside the loop — these are per-transaction
	 * cached values, but calling them once avoids function call overhead.
	 */
	xact_ts = RecnoGetTransactionTimestamp();
	if (recno_use_hlc)
		current_ts = (uint64) RecnoGetDmlTimestamp();
	else
		current_ts = xact_ts;
	/* Ensure relation storage exists */
	RelationGetSmgr(relation);

	/*
	 * Phase 1: Pre-form all tuples without overflow handling.
	 * Passing NULL for rel and overflow_buffers skips the overflow path,
	 * keeping the tuple inline.  Tuples that exceed the page size will
	 * be detected below and routed to single-insert.
	 */
	formed_tuples = (RecnoTuple *) palloc(ntuples * sizeof(RecnoTuple));
	needs_single_insert = (bool *) palloc0(ntuples * sizeof(bool));

	for (i = 0; i < ntuples; i++)
	{
		slot_getallattrs(slots[i]);
		formed_tuples[i] = RecnoFormTuple(RelationGetDescr(relation),
										   slots[i]->tts_values,
										   slots[i]->tts_isnull,
										   NULL,	/* no overflow in batch */
										   NULL);

		/* Set MVCC fields */
		formed_tuples[i]->t_data->t_commit_ts = current_ts;
		formed_tuples[i]->t_data->t_cid = cid;
		formed_tuples[i]->t_data->t_flags |= RECNO_TUPLE_UNCOMMITTED;
		formed_tuples[i]->t_data->t_xid_hint = GetTopTransactionId();

		/* Mark tuples too large for batch insert */
		if (formed_tuples[i]->t_len > RECNO_MAX_TUPLE_SIZE)
			needs_single_insert[i] = true;
	}

	/*
	 * Phase 2: Batch insert page-at-a-time.
	 *
	 * For each page: lock once, insert all fitting tuples, WAL-log once,
	 * unlock.  This is much faster than per-tuple buffer operations.
	 */
	ndone = 0;
	while (ndone < ntuples)
	{
		Buffer		buffer;
		Page		page;
		BlockNumber target_block;
		int			batch_start;
		int			batch_count;

		/* Skip tuples that need single-insert (overflow) */
		if (needs_single_insert[ndone])
		{
			recno_tuple_insert(relation, slots[ndone], cid, options, bistate);
			pfree(formed_tuples[ndone]);
			ndone++;
			continue;
		}

		/* Find a page with space for at least one tuple */
		target_block = RecnoGetPageWithFreeSpace(relation,
												  formed_tuples[ndone]->t_len);
		if (target_block == InvalidBlockNumber)
		{
			/* Fall back to single insert */
			recno_tuple_insert(relation, slots[ndone], cid, options, bistate);
			pfree(formed_tuples[ndone]);
			ndone++;
			continue;
		}

		buffer = ReadBuffer(relation, target_block);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);

		/* Pre-allocate WAL space outside critical section */
		if (RelationNeedsWAL(relation))
			XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID, 3);

		START_CRIT_SECTION();

		batch_start = ndone;
		batch_count = 0;

		/* Insert as many tuples as fit on this page */
		while (ndone < ntuples &&
			   !needs_single_insert[ndone] &&
			   PageGetFreeSpace(page) >= formed_tuples[ndone]->t_len)
		{
			OffsetNumber offnum;
			ItemId		inserted_itemid;
			RecnoTupleHeader *inserted_hdr;

			offnum = RecnoPageAddTuple(page, formed_tuples[ndone],
									   formed_tuples[ndone]->t_len);
			if (offnum == InvalidOffsetNumber)
				break;		/* page is full, stop batching */

			/* Set TID in slot */
			ItemPointerSet(&slots[ndone]->tts_tid, target_block, offnum);
			formed_tuples[ndone]->t_self = slots[ndone]->tts_tid;
			slots[ndone]->tts_tableOid = RelationGetRelid(relation);

			/* Set t_ctid to self */
			inserted_itemid = PageGetItemId(page, offnum);
			inserted_hdr = (RecnoTupleHeader *) PageGetItem(page, inserted_itemid);
			ItemPointerSet(&inserted_hdr->t_ctid, target_block, offnum);

			batch_count++;
			ndone++;
		}

		if (batch_count > 0)
		{
			/* Update page opaque fields */
			RecnoPageOpaque phdr = RecnoPageGetOpaque(page);

			phdr->pd_commit_ts = Max(phdr->pd_commit_ts, current_ts);
			phdr->pd_free_space = PageGetFreeSpace(page);

			MarkBufferDirty(buffer);

			/* WAL-log the batch using first tuple as representative */
			if (RelationNeedsWAL(relation))
			{
				XLogRecPtr	recptr;

				recptr = RecnoXLogInsert(relation, buffer,
										 ItemPointerGetOffsetNumber(&slots[batch_start]->tts_tid),
										 formed_tuples[batch_start],
										 current_ts, NULL);
				PageSetLSN(page, recptr);
			}
		}

		END_CRIT_SECTION();

		/*
		 * Skip sLog registration for bulk inserts.  COPY and multi_insert
		 * don't use SNAPSHOT_DIRTY or ON CONFLICT, so the
		 * RECNO_TUPLE_UNCOMMITTED flag alone provides sufficient
		 * self-visibility.  This avoids filling the sLog during large
		 * COPY operations.
		 */

		/* Update FSM with remaining free space */
		RecnoRecordFreeSpace(relation, target_block, PageGetFreeSpace(page));

		/* Update visibility map */
		RecnoVMUpdateForInsert(relation, formed_tuples[batch_start]->t_data,
							   buffer);

		UnlockReleaseBuffer(buffer);

		/* Free formed tuples in this batch */
		for (i = batch_start; i < batch_start + batch_count; i++)
			pfree(formed_tuples[i]);
	}

	pfree(formed_tuples);
	pfree(needs_single_insert);
}

/*
 * RecnoVacuumCrossPageDefrag - move live tuples from tail pages to front pages
 *
 * After single-page defragmentation (Phase III), pages near the end of the
 * relation may still contain live tuples, preventing truncation.  This
 * function moves those tuples to pages near the front that have sufficient
 * free space, thereby emptying tail pages so that Phase V can truncate them.
 *
 * Algorithm:
 *   1. Scan backwards from the end of the relation to find source pages
 *      that have live tuples and could be emptied.
 *   2. For each source page, find target pages near the front with enough
 *      free space (via the RECNO free space map).
 *   3. Move each live tuple: copy to target page, insert new index entries
 *      pointing to the new TID, then mark the old line pointer unused.
 *   4. WAL-log all modifications for crash safety.
 *
 * Locking protocol:
 *   We acquire an exclusive lock on the source page (higher block number).
 *   For each tuple move, we lock the target page exclusively while holding
 *   the source lock.  Since we always hold the higher-numbered page first,
 *   this maintains a consistent lock ordering and avoids deadlocks.  VACUUM
 *   also holds a heavyweight lock on the relation.
 *
 * We skip:
 *   - Pages with overflow records (complex linked structure)
 *   - Pages with tuples that have update chains (t_ctid != self)
 *   - Pages with deleted-but-not-yet-vacuumed tuples
 *
 * Returns the number of tuples moved.
 */
static int
RecnoVacuumCrossPageDefrag(Relation rel, BlockNumber nblocks,
						   BlockNumber *empty_end_pages_p,
						   int nindexes, Relation *indrels,
						   BufferAccessStrategy bstrategy, bool verbose)
{
	BlockNumber src_blkno;
	BlockNumber nonempty_limit;
	int			tuples_moved = 0;
	int			pages_emptied = 0;
	EState	   *estate = NULL;
	TupleTableSlot *slot = NULL;
	IndexInfo **indexInfoArray = NULL;

	/*
	 * Compute the boundary: we only try to empty pages from position (nblocks
	 * - empty_end_pages - 1) backwards toward the "used" portion. If there
	 * are already empty_end_pages trailing, the first candidate source page
	 * is right before that run.
	 */
	if (*empty_end_pages_p >= nblocks)
		return 0;

	nonempty_limit = nblocks - *empty_end_pages_p;

	/* Need at least a few pages to make defrag worthwhile */
	if (nonempty_limit <= 1)
		return 0;

	if (verbose)
		ereport(INFO,
				(errmsg("table \"%s\": starting cross-page defragmentation from block %u",
						RelationGetRelationName(rel),
						nonempty_limit - 1)));

	/*
	 * Create a single executor state and tuple slot, reused across all index
	 * insertions to avoid repeated allocation.  Also pre-build IndexInfo for
	 * each index to avoid expensive catalog lookups inside the per-tuple
	 * loop.
	 */
	if (nindexes > 0)
	{
		estate = CreateExecutorState();
		slot = MakeSingleTupleTableSlot(RelationGetDescr(rel),
										table_slot_callbacks(rel));
		GetPerTupleExprContext(estate)->ecxt_scantuple = slot;

		indexInfoArray = (IndexInfo **) palloc(nindexes * sizeof(IndexInfo *));
		for (int i = 0; i < nindexes; i++)
			indexInfoArray[i] = BuildIndexInfo(indrels[i]);
	}

	/*
	 * Scan backwards from the last candidate source page.  Favor moving
	 * tuples from the highest-numbered pages first, as this maximizes the
	 * contiguous run of empty tail pages available for truncation.
	 */
	for (src_blkno = nonempty_limit - 1;
		 src_blkno != InvalidBlockNumber && src_blkno > 0;
		 src_blkno--)
	{
		Buffer		src_buf;
		Page		src_page;
		OffsetNumber maxoff;
		OffsetNumber offnum;
		bool		page_emptied = true;
		bool		skip_page = false;
		int			ntuples_on_page = 0;

		CHECK_FOR_INTERRUPTS();

		/* Read and exclusively lock the source page */
		src_buf = ReadBufferExtended(rel, MAIN_FORKNUM, src_blkno,
									 RBM_NORMAL, bstrategy);
		LockBuffer(src_buf, BUFFER_LOCK_EXCLUSIVE);
		src_page = BufferGetPage(src_buf);

		/* Skip new or empty pages */
		if (PageIsNew(src_page) || PageIsEmpty(src_page))
		{
			UnlockReleaseBuffer(src_buf);
			continue;
		}

		/*
		 * First pass: check the page for suitability.
		 *
		 * We skip pages that have overflow records, deleted tuples that
		 * haven't been vacuumed yet, or tuples with update chains.
		 */
		maxoff = PageGetMaxOffsetNumber(src_page);
		for (offnum = FirstOffsetNumber; offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		itemid = PageGetItemId(src_page, offnum);
			RecnoTupleHeader *tuple_hdr;
			ItemPointerData self_tid;

			if (!ItemIdIsNormal(itemid))
			{
				if (ItemIdIsDead(itemid))
				{
					/* Dead items not yet cleaned -- skip page */
					skip_page = true;
					break;
				}
				continue;		/* LP_UNUSED slots are fine */
			}

			tuple_hdr = (RecnoTupleHeader *) PageGetItem(src_page, itemid);

			/* Skip pages with overflow records -- too complex to relocate */
			if (RecnoIsOverflowRecord(tuple_hdr, ItemIdGetLength(itemid)))
			{
				skip_page = true;
				break;
			}

			/* Skip pages with tuples that have overflow pointers */
			if (tuple_hdr->t_flags & RECNO_TUPLE_HAS_OVERFLOW)
			{
				skip_page = true;
				break;
			}

			/* Skip pages with deleted-but-not-yet-removed tuples */
			if (tuple_hdr->t_flags & RECNO_TUPLE_DELETED)
			{
				skip_page = true;
				break;
			}

			/* Skip pages with update chains: ctid must point to self */
			ItemPointerSet(&self_tid, src_blkno, offnum);
			if (!ItemPointerIsValid(&tuple_hdr->t_ctid) ||
				!ItemPointerEquals(&tuple_hdr->t_ctid, &self_tid))
			{
				skip_page = true;
				break;
			}

			ntuples_on_page++;
		}

		if (skip_page || ntuples_on_page == 0)
		{
			UnlockReleaseBuffer(src_buf);
			continue;
		}

		/*
		 * Second pass: move each live tuple to a target page near the front
		 * of the relation.
		 */
		for (offnum = FirstOffsetNumber; offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		src_itemid;
			RecnoTupleHeader *src_hdr;
			Size		tuple_len;
			Buffer		dst_buf;
			Page		dst_page;
			BlockNumber dst_blkno;
			OffsetNumber dst_offnum;
			ItemPointerData new_tid;

			src_itemid = PageGetItemId(src_page, offnum);
			if (!ItemIdIsNormal(src_itemid))
				continue;

			src_hdr = (RecnoTupleHeader *) PageGetItem(src_page, src_itemid);
			tuple_len = ItemIdGetLength(src_itemid);

			/*
			 * Find a target page with enough free space.  Use the FSM
			 * directly (GetPageWithFreeSpace) instead of our wrapper
			 * RecnoGetPageWithFreeSpace, because the wrapper reads and locks
			 * pages to verify free space, which would deadlock against the
			 * exclusive lock we already hold on the source page.  We verify
			 * the actual free space below, after locking the target page.
			 * The target must be before the source block to be useful for
			 * truncation.
			 */
			dst_blkno = GetPageWithFreeSpace(rel,
											 tuple_len + sizeof(ItemIdData));
			if (dst_blkno == InvalidBlockNumber || dst_blkno >= src_blkno)
			{
				page_emptied = false;
				continue;
			}

			/*
			 * Lock the target page.  Lock ordering is safe: we hold the
			 * higher-numbered source page lock already.
			 */
			dst_buf = ReadBufferExtended(rel, MAIN_FORKNUM, dst_blkno,
										 RBM_NORMAL, bstrategy);
			LockBuffer(dst_buf, BUFFER_LOCK_EXCLUSIVE);
			dst_page = BufferGetPage(dst_buf);

			/* Recheck free space -- FSM might be stale */
			if (PageGetFreeSpace(dst_page) < tuple_len + sizeof(ItemIdData))
			{
				Size		actual_free = PageGetFreeSpace(dst_page);

				UnlockReleaseBuffer(dst_buf);

				/* Update FSM with accurate info */
				RecnoRecordFreeSpace(rel, dst_blkno, actual_free);
				page_emptied = false;
				continue;
			}

			/*
			 * Perform the move in a critical section.  Both pages are
			 * modified atomically from WAL's perspective.
			 */
			START_CRIT_SECTION();

			/* Insert the tuple data into the target page */
			dst_offnum = PageAddItem(dst_page, src_hdr, tuple_len,
									 InvalidOffsetNumber, false, false);

			if (dst_offnum == InvalidOffsetNumber)
			{
				END_CRIT_SECTION();
				UnlockReleaseBuffer(dst_buf);
				page_emptied = false;
				continue;
			}

			/* Update ctid in the new copy to point to itself */
			{
				ItemId		dst_itemid;
				RecnoTupleHeader *dst_hdr;

				dst_itemid = PageGetItemId(dst_page, dst_offnum);
				dst_hdr = (RecnoTupleHeader *) PageGetItem(dst_page, dst_itemid);
				ItemPointerSet(&dst_hdr->t_ctid, dst_blkno, dst_offnum);
			}

			ItemPointerSet(&new_tid, dst_blkno, dst_offnum);

			/* Mark the source line pointer as unused */
			ItemIdSetUnused(src_itemid);

			/* Mark both buffers dirty */
			MarkBufferDirty(dst_buf);
			MarkBufferDirty(src_buf);

			/* WAL-log the cross-page move */
			if (RelationNeedsWAL(rel))
			{
				XLogRecPtr	recptr;

				/*
				 * Use the dedicated cross-page defrag record type.  This logs
				 * both pages (with FPIs when needed) plus the tuple data for
				 * non-FPI replay.  Block 0 = target, block 1 = source.
				 */
				recptr = RecnoXLogCrossPageDefrag(rel,
												  dst_buf, dst_offnum,
												  src_buf, offnum,
												  src_hdr, (uint32) tuple_len);
				PageSetLSN(dst_page, recptr);
				PageSetLSN(src_page, recptr);
			}

			END_CRIT_SECTION();

			/* Update FSM for the target page */
			RecnoRecordFreeSpace(rel, dst_blkno,
								 PageGetFreeSpace(dst_page));

			UnlockReleaseBuffer(dst_buf);

			/*
			 * Now update index entries.  We insert new entries pointing to
			 * the new TID.  Old entries pointing to the now-LP_UNUSED source
			 * slot will be treated as dead by index scans and cleaned up by
			 * the next index vacuum pass.
			 */
			if (nindexes > 0)
			{
				Buffer		tup_buf;
				Page		tup_page;
				Datum		values[INDEX_MAX_KEYS];
				bool		isnull[INDEX_MAX_KEYS];
				RecnoTupleHeader *moved_hdr;

				/* Re-read moved tuple from the target page */
				tup_buf = ReadBufferExtended(rel, MAIN_FORKNUM, dst_blkno,
											 RBM_NORMAL, bstrategy);
				LockBuffer(tup_buf, BUFFER_LOCK_SHARE);
				tup_page = BufferGetPage(tup_buf);

				{
					ItemId		tup_itemid = PageGetItemId(tup_page, dst_offnum);

					moved_hdr = (RecnoTupleHeader *)
						PageGetItem(tup_page, tup_itemid);
				}

				/*
				 * Convert the RECNO tuple to a slot so we can extract index
				 * column values.
				 */
				ExecClearTuple(slot);
				RecnoTupleToSlot(moved_hdr, slot);
				slot->tts_tid = new_tid;

				for (int i = 0; i < nindexes; i++)
				{
					FormIndexDatum(indexInfoArray[i], slot, estate,
								   values, isnull);

					/*
					 * Insert new index entry.  Skip uniqueness check since
					 * we're relocating an existing tuple.
					 */
					index_insert(indrels[i], values, isnull, &new_tid,
								 rel, UNIQUE_CHECK_NO, false,
								 indexInfoArray[i]);

					ResetPerTupleExprContext(estate);
				}

				LockBuffer(tup_buf, BUFFER_LOCK_UNLOCK);
				ReleaseBuffer(tup_buf);
			}

			tuples_moved++;
		}

		/* Update FSM for the source page */
		RecnoRecordFreeSpace(rel, src_blkno,
							 PageGetFreeSpace(src_page));

		UnlockReleaseBuffer(src_buf);

		if (page_emptied)
		{
			pages_emptied++;

			/*
			 * Extend the trailing empty page count if this page is contiguous
			 * with the existing run.
			 */
			if (src_blkno == nblocks - *empty_end_pages_p - 1)
				(*empty_end_pages_p)++;
		}
		else
		{
			/*
			 * Once we fail to empty a page, stop: pages below this one won't
			 * contribute to a contiguous run of trailing empties.
			 */
			break;
		}
	}

	/* Clean up executor state and pre-built index info */
	if (indexInfoArray != NULL)
	{
		for (int i = 0; i < nindexes; i++)
			pfree(indexInfoArray[i]);
		pfree(indexInfoArray);
	}
	if (slot != NULL)
		ExecDropSingleTupleTableSlot(slot);
	if (estate != NULL)
		FreeExecutorState(estate);

	if (tuples_moved > 0 && verbose)
		ereport(INFO,
				(errmsg("table \"%s\": cross-page defrag moved %d tuples, emptied %d pages",
						RelationGetRelationName(rel),
						tuples_moved, pages_emptied)));

	return tuples_moved;
}

/*
 * Vacuum a RECNO relation
 *
 * This performs garbage collection on a RECNO table in multiple phases:
 *
 * Phase I:    Scan all pages, identify dead tuples, collect their TIDs
 * Phase II:   Remove dead index entries using the collected TIDs
 * Phase III:  Defragment data pages to reclaim space (must happen AFTER
 *             index cleanup to avoid dangling index pointers)
 * Phase IV:   Post-vacuum index cleanup (amvacuumcleanup)
 * Phase IV-B: Cross-page defragmentation (move tail tuples to front pages)
 * Phase V:    Truncate trailing empty pages, update FSM
 */
void
recno_relation_vacuum(Relation onerel, const VacuumParams *params,
					  BufferAccessStrategy bstrategy)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	Buffer		buf;
	Page		page;
	uint64		oldest_ts;
	int64		num_tuples = 0;
	int64		dead_tuples = 0;
	int64		live_tuples = 0;
	int64		pages_vacuumed = 0;
	BlockNumber empty_end_pages = 0;
	bool		verbose = (params->options & VACOPT_VERBOSE) != 0;

	/* Index cleanup state */
	Relation   *indrels = NULL;
	int			nindexes = 0;
	IndexBulkDeleteResult **indstats = NULL;
	TidStore   *dead_items = NULL;
	VacDeadItemsInfo *dead_items_info = NULL;
	bool		do_index_cleanup;

	/*
	 * Initialize RECNO transaction state so that VACUUM's own start
	 * timestamp is registered in xact_start_ts_slots.  Without this,
	 * RecnoGetOldestActiveTimestamp() would see no active transactions and
	 * fall back to global_commit_ts, which in HLC mode may never have been
	 * updated (it is only advanced by RecnoGetCommitTimestamp(), which is
	 * not called in HLC mode).  The result would be oldest_ts ≈ 1,
	 * causing VACUUM to classify all dead tuples as "recently dead" and
	 * skip index cleanup entirely.
	 */
	(void) RecnoGetTransactionTimestamp();

	/*
	 * Get the oldest active transaction's start timestamp.  Deleted tuples
	 * whose commit timestamp is older than this are no longer visible to any
	 * running transaction and can safely be removed.
	 *
	 * Previously this called RecnoGetCommitTimestamp() which returns (and
	 * advances) the current wall-clock time.  That was wrong: it made VACUUM
	 * consider almost all deleted tuples as reclaimable, even those still
	 * needed by long-running concurrent transactions.
	 */
	oldest_ts = RecnoGetOldestActiveTimestamp();

	/* Get total number of blocks */
	nblocks = RelationGetNumberOfBlocks(onerel);

	if (verbose)
		ereport(INFO, (errmsg("vacuuming \"%s\": scanning %u pages",
							  RelationGetRelationName(onerel), nblocks)));

	/*
	 * Open all indexes on the relation. We need RowExclusiveLock to prevent
	 * concurrent index modifications during vacuum.
	 */
	vac_open_indexes(onerel, RowExclusiveLock, &nindexes, &indrels);
	do_index_cleanup = (nindexes > 0);

	/*
	 * Allocate TidStore for collecting dead tuple TIDs, and per-index stats
	 * array. We use maintenance_work_mem as the budget for the TidStore.
	 */
	if (do_index_cleanup)
	{
		dead_items_info = (VacDeadItemsInfo *) palloc0(sizeof(VacDeadItemsInfo));
		dead_items_info->max_bytes = (size_t) maintenance_work_mem * 1024;
		dead_items_info->num_items = 0;

		dead_items = TidStoreCreateLocal(dead_items_info->max_bytes, true);

		indstats = (IndexBulkDeleteResult **)
			palloc0(nindexes * sizeof(IndexBulkDeleteResult *));
	}

	/*
	 * -----------------------------------------------------------------------
	 * Phase I: Scan all pages, identify dead tuples, collect TIDs
	 *
	 * We scan every page and classify each tuple as live, dead (removable),
	 * or recently dead (not yet removable).  Dead tuple TIDs are recorded in
	 * the TidStore for later index cleanup.  We do NOT defragment pages yet
	 * -- that must wait until after index entries pointing to dead tuples
	 * have been removed (Phase II), to avoid dangling index pointers.
	 * -----------------------------------------------------------------------
	 */
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		OffsetNumber offnum,
					maxoffnum;
		ItemId		itemid;
		OffsetNumber dead_offsets[MaxOffsetNumber];
		int			ndead_on_page = 0;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Check the Visibility Map before reading the page.  If the page is
		 * already marked ALL_FROZEN, all tuples are visible and frozen -- no
		 * VACUUM work is needed.  This avoids the I/O cost of reading the
		 * page entirely.
		 *
		 * For aggressive vacuums we still need to scan all pages to verify VM
		 * correctness, so we skip this optimization.
		 */
		if (!(params->options & VACOPT_DISABLE_PAGE_SKIPPING) &&
			RecnoVMCheck(onerel, blkno, RECNO_VM_ALL_FROZEN))
		{
			continue;
		}

		/* Read and lock the page */
		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno, RBM_NORMAL,
								 bstrategy);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);

		/* Skip if page is new/uninitialized */
		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		maxoffnum = PageGetMaxOffsetNumber(page);

		/* Scan all tuples on the page */
		for (offnum = FirstOffsetNumber; offnum <= maxoffnum; offnum++)
		{
			RecnoTupleHeader *tuple_hdr;

			itemid = PageGetItemId(page, offnum);

			/* Skip if not a normal tuple */
			if (!ItemIdIsNormal(itemid))
				continue;

			tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);

			/* Skip overflow records - they are managed by tuple lifecycle */
			if (RecnoIsOverflowRecord(tuple_hdr, ItemIdGetLength(itemid)))
				continue;

			num_tuples++;

			/*
			 * Check if tuple is deleted and old enough to be removed.
			 *
			 * With the sLog-based MVCC model, a deleted tuple can be
			 * vacuumed if:
			 *   - RECNO_TUPLE_DELETED is set
			 *   - RECNO_TUPLE_UNCOMMITTED is NOT set (committed delete)
			 *   - commit_ts is older than the oldest active snapshot
			 *
			 * If UNCOMMITTED is still set, the deleting transaction is
			 * still in progress (or aborted but not yet cleaned up by
			 * the sLog callback).  Skip it.
			 */
			if (tuple_hdr->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED))
			{
				if (tuple_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED)
				{
					/* Transaction still in progress -- skip */
					live_tuples++;
				}
				else if (tuple_hdr->t_commit_ts < oldest_ts)
				{
					/* Tuple is dead and can be removed */
					dead_offsets[ndead_on_page++] = offnum;
					dead_tuples++;
				}
				else
				{
					/* Recently dead -- not yet reclaimable but still dead */
					dead_tuples++;
				}
			}
			else
			{
				/* Tuple is live */
				live_tuples++;
			}
		}

		/*
		 * Record dead tuple TIDs for this page in the TidStore. This is
		 * needed for index cleanup in Phase II.
		 */
		if (ndead_on_page > 0 && do_index_cleanup)
		{
			TidStoreSetBlockOffsets(dead_items, blkno,
									dead_offsets, ndead_on_page);
			dead_items_info->num_items += ndead_on_page;
		}

		UnlockReleaseBuffer(buf);
	}

	/*
	 * -----------------------------------------------------------------------
	 * Phase II: Index vacuum -- remove dead index entries
	 *
	 * For each index on the relation, call the index AM's bulk delete routine
	 * to remove entries pointing to dead tuples.  This MUST happen before we
	 * defragment data pages (Phase III) to ensure no index entry points to a
	 * TID that has been recycled.
	 * -----------------------------------------------------------------------
	 */
	if (do_index_cleanup && dead_items_info->num_items > 0)
	{
		int			idx;

		if (verbose)
			ereport(INFO,
					(errmsg("vacuuming \"%s\": removing %lld dead index entries across %d indexes",
							RelationGetRelationName(onerel),
							(long long) dead_items_info->num_items,
							nindexes)));

		for (idx = 0; idx < nindexes; idx++)
		{
			IndexVacuumInfo ivinfo;

			ivinfo.index = indrels[idx];
			ivinfo.heaprel = onerel;
			ivinfo.analyze_only = false;
			ivinfo.report_progress = false;
			ivinfo.estimated_count = true;
			ivinfo.message_level = verbose ? INFO : DEBUG2;
			ivinfo.num_heap_tuples = (double) live_tuples;
			ivinfo.strategy = bstrategy;

			indstats[idx] = vac_bulkdel_one_index(&ivinfo, indstats[idx],
												  dead_items,
												  dead_items_info);
		}
	}

	/*
	 * -----------------------------------------------------------------------
	 * Phase III: Defragment data pages -- remove dead tuples from heap
	 *
	 * Now that index entries pointing to dead tuples have been removed, we
	 * can safely defragment data pages.  This reclaims the space occupied by
	 * dead tuples and makes it available for reuse.
	 * -----------------------------------------------------------------------
	 */
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		OffsetNumber offnum,
					maxoffnum;
		ItemId		itemid;
		bool		page_has_dead_tuples = false;
		bool		page_modified = false;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBufferExtended(onerel, MAIN_FORKNUM, blkno, RBM_NORMAL,
								 bstrategy);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);

		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		maxoffnum = PageGetMaxOffsetNumber(page);

		/*
		 * First pass: clean overflow chains for dead tuples.  We must do this
		 * BEFORE defragmenting, because RecnoPageDefragment removes dead item
		 * pointers and after that we can no longer identify which tuples had
		 * overflow data.  We temporarily drop the exclusive lock since
		 * overflow chain deletion may need to read and lock other pages.
		 */
		for (offnum = FirstOffsetNumber; offnum <= maxoffnum; offnum++)
		{
			RecnoTupleHeader *tuple_hdr;

			itemid = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(itemid))
				continue;

			tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);
			if (RecnoIsOverflowRecord(tuple_hdr, ItemIdGetLength(itemid)))
				continue;

			if ((tuple_hdr->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED)) &&
				!(tuple_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
				tuple_hdr->t_commit_ts < oldest_ts)
			{
				page_has_dead_tuples = true;

				/*
				 * Clean up overflow chains for this dead tuple before the
				 * tuple is removed by defragmentation.
				 */
				if (tuple_hdr->t_flags & RECNO_TUPLE_HAS_OVERFLOW)
				{
					BlockNumber ov_blocks[MAX_OVERFLOW_PTRS_PER_TUPLE];
					OffsetNumber ov_offsets[MAX_OVERFLOW_PTRS_PER_TUPLE];
					int			n_overflow;

					n_overflow = RecnoCollectOverflowPtrs(tuple_hdr,
														  RelationGetDescr(onerel),
														  ov_blocks, ov_offsets,
														  MAX_OVERFLOW_PTRS_PER_TUPLE);
					LockBuffer(buf, BUFFER_LOCK_UNLOCK);

					for (int ov_i = 0; ov_i < n_overflow; ov_i++)
						RecnoDeleteOverflowChain(onerel, ov_blocks[ov_i],
												 ov_offsets[ov_i]);

					LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

					/*
					 * Re-fetch the page pointer since the buffer content
					 * address doesn't change while pinned, but be safe.
					 */
					page = BufferGetPage(buf);
				}
			}
		}

		/*
		 * If page has dead tuples, defragment it to consolidate space.
		 */
		if (page_has_dead_tuples)
		{
			START_CRIT_SECTION();

			/*
			 * Mark dead tuples as unused before defragmenting. The scan above
			 * already identified them; now set LP_UNUSED.
			 */
			for (offnum = FirstOffsetNumber; offnum <= maxoffnum; offnum++)
			{
				RecnoTupleHeader *vac_hdr;

				itemid = PageGetItemId(page, offnum);
				if (!ItemIdIsNormal(itemid))
					continue;

				if (RecnoIsOverflowRecord(PageGetItem(page, itemid),
										  ItemIdGetLength(itemid)))
					continue;

				vac_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);
				if ((vac_hdr->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED)) &&
					!(vac_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED) &&
					vac_hdr->t_commit_ts < oldest_ts)
				{
					ItemIdSetUnused(itemid);
				}
			}

			RecnoPageDefragment(page);
			page_modified = true;
			pages_vacuumed++;

			MarkBufferDirty(buf);

			if (RelationNeedsWAL(onerel))
			{
				XLogRecPtr	recptr;

				recptr = RecnoXLogDefrag(onerel, buf, NULL, 0, oldest_ts);
				PageSetLSN(page, recptr);
			}

			END_CRIT_SECTION();
		}

		/* Update FSM with accurate free space information */
		if (page_modified || PageGetFreeSpace(page) > 0)
		{
			RecnoRecordFreeSpace(onerel, blkno, PageGetFreeSpace(page));
		}

		/*
		 * Update the Visibility Map for this page.
		 *
		 * After defragmentation, check whether all remaining tuples on the
		 * page are visible to all transactions and/or frozen.  If so, set the
		 * appropriate VM bits.  This enables index-only scans to skip heap
		 * fetches and future VACUUMs to skip this page entirely.
		 */
		{
			bool		all_visible = true;
			bool		all_frozen = true;
			OffsetNumber vm_offnum;
			OffsetNumber vm_maxoff;

			vm_maxoff = PageGetMaxOffsetNumber(page);

			for (vm_offnum = FirstOffsetNumber; vm_offnum <= vm_maxoff; vm_offnum++)
			{
				ItemId		vm_itemid;
				RecnoTupleHeader *vm_tuple_hdr;

				vm_itemid = PageGetItemId(page, vm_offnum);
				if (!ItemIdIsNormal(vm_itemid))
					continue;

				/* Skip overflow records */
				if (RecnoIsOverflowRecord(PageGetItem(page, vm_itemid),
										  ItemIdGetLength(vm_itemid)))
					continue;

				vm_tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, vm_itemid);

				/* Dead tuples (deleted or updated) that survived defrag are recently dead */
				if (vm_tuple_hdr->t_flags & (RECNO_TUPLE_DELETED | RECNO_TUPLE_UPDATED))
				{
					all_visible = false;
					all_frozen = false;
					break;
				}

				/* Speculative tuples are not visible to all */
				if (vm_tuple_hdr->t_flags & RECNO_TUPLE_SPECULATIVE)
				{
					all_visible = false;
					all_frozen = false;
					break;
				}

				/*
				 * A tuple is visible to all if its commit timestamp is older
				 * than the oldest active transaction and it is not uncommitted.
				 * It is frozen if there is no UNCOMMITTED flag (no in-progress
				 * operation tracked by the sLog).
				 */
				if (vm_tuple_hdr->t_commit_ts >= oldest_ts ||
					(vm_tuple_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED))
				{
					all_visible = false;
					all_frozen = false;
					break;
				}
			}

			/* Empty pages are trivially all-visible and all-frozen */
			if (vm_maxoff < FirstOffsetNumber)
			{
				all_visible = true;
				all_frozen = true;
			}

			RecnoVMVacuumPage(onerel, buf, all_visible, all_frozen);
		}

		/* Check if page is completely empty (for truncation) */
		if (PageGetMaxOffsetNumber(page) < FirstOffsetNumber)
		{
			if (blkno == nblocks - 1 - empty_end_pages)
				empty_end_pages++;
		}
		else
		{
			empty_end_pages = 0;
		}

		UnlockReleaseBuffer(buf);
	}

	/*
	 * -----------------------------------------------------------------------
	 * Phase IV: Index cleanup (amvacuumcleanup)
	 *
	 * Call each index AM's vacuum cleanup routine.  This lets the index AM do
	 * any post-vacuum maintenance such as reclaiming empty pages, updating
	 * statistics, etc.  This is called even if no dead tuples were found,
	 * since some index AMs use this to update internal metadata.
	 * -----------------------------------------------------------------------
	 */
	if (do_index_cleanup)
	{
		int			idx;

		for (idx = 0; idx < nindexes; idx++)
		{
			IndexVacuumInfo ivinfo;

			ivinfo.index = indrels[idx];
			ivinfo.heaprel = onerel;
			ivinfo.analyze_only = false;
			ivinfo.report_progress = false;
			ivinfo.estimated_count = (nblocks > pages_vacuumed);
			ivinfo.message_level = verbose ? INFO : DEBUG2;
			ivinfo.num_heap_tuples = (double) live_tuples;
			ivinfo.strategy = bstrategy;

			indstats[idx] = vac_cleanup_one_index(&ivinfo, indstats[idx]);
		}
	}

	/*
	 * -----------------------------------------------------------------------
	 * Phase IV-B: Cross-page defragmentation
	 *
	 * Move live tuples from tail pages to front pages so that more trailing
	 * pages become empty and can be truncated in Phase V.  This must run
	 * after index cleanup (Phase IV) so that stale index entries for
	 * previously-dead tuples have already been removed.  Indexes are still
	 * open so we can insert new entries for relocated tuples.
	 * -----------------------------------------------------------------------
	 */
	RecnoVacuumCrossPageDefrag(onerel, nblocks,
							   &empty_end_pages,
							   nindexes, indrels,
							   bstrategy, verbose);

	/*
	 * -----------------------------------------------------------------------
	 * Phase IV-C: Orphan overflow cleanup
	 *
	 * Run the two-pass orphan detection algorithm to find and remove overflow
	 * records that are not referenced by any live tuple.  This catches
	 * overflow records that were orphaned by crashes, aborted transactions,
	 * or bugs in the eager cleanup path.
	 * -----------------------------------------------------------------------
	 */
	RecnoVacuumOverflowRecords(onerel);

	/*
	 * -----------------------------------------------------------------------
	 * Phase IV-D: UNDO log maintenance
	 *
	 * Discard old per-relation UNDO records that are no longer needed for
	 * MVCC visibility checks.  With UNDO handling transaction rollback, the
	 * heavy dead-tuple scanning above is primarily needed for reclaiming
	 * main-fork space and maintaining indexes.  The UNDO fork has its own
	 * lifecycle: old UNDO records are discarded here, and their pages are
	 * moved to a deferred-deallocation list that the background
	 * relundo_worker processes asynchronously.
	 *
	 * RelUndoVacuum() reads the UNDO metapage counter, computes a discard
	 * cutoff, and calls RelUndoDiscard() which walks the UNDO page chain
	 * from tail to head, freeing pages whose generation counter precedes
	 * the cutoff.
	 * -----------------------------------------------------------------------
	 */
	RelUndoVacuum(onerel, GetOldestNonRemovableTransactionId(onerel));

	/*
	 * -----------------------------------------------------------------------
	 * Phase V: Truncation and final cleanup
	 * -----------------------------------------------------------------------
	 */

	/* Truncate empty pages at the end of the relation */
	if (empty_end_pages > 0 && (params->options & VACOPT_DISABLE_PAGE_SKIPPING) == 0)
	{
		BlockNumber new_nblocks = nblocks - empty_end_pages;

		RelationTruncate(onerel, new_nblocks);

		if (verbose)
			ereport(INFO, (errmsg("truncated %u empty pages from end of relation",
								  empty_end_pages)));
	}

	/* Update FSM for the entire relation */
	RecnoVacuumFSM(onerel, nblocks - empty_end_pages);

	/* Clean up index resources */
	if (dead_items != NULL)
		TidStoreDestroy(dead_items);
	if (dead_items_info != NULL)
		pfree(dead_items_info);
	if (indstats != NULL)
		pfree(indstats);
	vac_close_indexes(nindexes, indrels, RowExclusiveLock);

	/* Report statistics */
	if (verbose || params->options & VACOPT_VERBOSE)
	{
		ereport(INFO,
				(errmsg("RECNO vacuum \"%s\": found %lld tuples (%lld live, %lld dead), "
						"vacuumed %lld pages, truncated %u pages, "
						"cleaned %d indexes",
						RelationGetRelationName(onerel),
						(long long) num_tuples,
						(long long) live_tuples,
						(long long) dead_tuples,
						(long long) pages_vacuumed,
						empty_end_pages,
						nindexes)));
	}
}
