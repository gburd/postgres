/*-------------------------------------------------------------------------
 *
 * flux_operations.c
 *	  FLUX table manipulation operations
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_operations.c
 *
 * NOTES
 *	  This implements the remaining table manipulation operations for
 *	  FLUX storage manager including insert, update, delete, and
 *	  various DDL operations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/flux.h"
#include "access/flux_dirtymap.h"
#include "access/slog.h"
#include "access/twophase.h"
#include "access/twophase_rmgr.h"
#include "access/flux_undo.h"
#include "access/flux_xlog.h"
#include "access/tableam.h"
#include "access/undobuffer.h"
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
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/latch.h"
#include "storage/read_stream.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/injection_point.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "storage/bufpage.h"
#include "miscadmin.h"

/*
 * Maximum overflow pointers per tuple for VACUUM overflow cleanup.
 * This limits memory usage during VACUUM and is conservative since most
 * tuples won't have overflow data. 128 is sufficient for typical workloads.
 */
#define MAX_OVERFLOW_PTRS_PER_TUPLE 128

/* Function prototypes for locking */
extern bool FluxLockTuple(Relation rel, ItemPointer tid, LockTupleMode mode,
						   bool wait, bool *have_tuple_lock);
extern void FluxUnlockTuple(Relation rel, ItemPointer tid, LockTupleMode mode);
extern void FluxLockPage(Relation rel, BlockNumber blkno, LOCKMODE mode);
extern void FluxUnlockPage(Relation rel, BlockNumber blkno, LOCKMODE mode);

/* sLog transaction callback prototypes */
static void FluxSLogXactCallback(XactEvent event, void *arg);
static void FluxSLogSubXactCallback(SubXactEvent event,
									 SubTransactionId mySubid,
									 SubTransactionId parentSubid,
									 void *arg);


/*
 * In-place update statistics counters.
 *
 * These track the effectiveness of FLUX's in-place update optimization
 * across the lifetime of the backend.  They are exposed via
 * FluxGetUpdateStats() for monitoring.
 */
static int64 flux_stat_in_place_updates = 0;
static int64 flux_stat_out_of_place_updates = 0;
static int64 flux_stat_defrag_triggered_updates = 0;

/* Whether sLog transaction callbacks have been registered for this backend */
static bool flux_slog_callbacks_registered = false;

/*
 * GUC: skip commit-time page re-visits (lazy clear of UNCOMMITTED flags).
 *
 * Default off.  Clearing FLUX_TUPLE_UNCOMMITTED is a pure hint (commit
 * visibility comes from CLOG via heap-shaped xmin/xmax); the lazy path clears
 * the flag at the next visibility check, while the eager path visits each
 * modified page at PRE_COMMIT so later readers can skip the sLog fast-path
 * lookup.
 */
bool		flux_lazy_uncommitted_clear = false;

/*
 * FluxGetUpdateStats - Return in-place update statistics
 *
 * Fills in the provided counters with the current backend-local statistics.
 */
void
FluxGetUpdateStats(int64 *in_place, int64 *out_of_place, int64 *defrag_triggered)
{
	if (in_place)
		*in_place = flux_stat_in_place_updates;
	if (out_of_place)
		*out_of_place = flux_stat_out_of_place_updates;
	if (defrag_triggered)
		*defrag_triggered = flux_stat_defrag_triggered_updates;
}


/*
 * FluxPagePruneOpt -- opportunistic dead-tuple cleanup on a page.
 *
 * This is the FLUX equivalent of heap_page_prune_opt().  It is called
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
FluxPagePruneOpt(Relation relation, Buffer buffer)
{
	Page		page;
	FluxPageOpaque opaque;
	OffsetNumber offnum;
	OffsetNumber maxoff;
	uint64		oldest_ts;
	TransactionId oldest_xmin_prune;
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
	 * FLUX_PAGE_DEFRAG_NEEDED flag is set by delete and update operations
	 * when a tuple is marked as deleted.  If no deletions have occurred on
	 * this page, there is nothing to clean up.
	 */
	opaque = FluxPageGetOpaque(page);
	if (!(FluxPageGetFlags(opaque) & FLUX_PAGE_DEFRAG_NEEDED))
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
		!(FluxPageGetFlags(opaque) & FLUX_PAGE_FULL))
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
	opaque = FluxPageGetOpaque(page);
	if (!(FluxPageGetFlags(opaque) & FLUX_PAGE_DEFRAG_NEEDED))
	{
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		return 0;
	}

	/*
	 * oldest_xmin_prune is the reclamation gate (XID horizon, FluxTupleDeadToAll
	 * below).  oldest_ts is NOT a decision input here: it is carried only into
	 * the DEFRAG WAL record's cosmetic page commit-ts word (FluxXLogDefrag ->
	 * FluxPageSetCommitTs).
	 */
	oldest_ts = FluxGetOldestActiveTimestamp();
	oldest_xmin_prune = FluxGetOldestXminHorizon(relation);
	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		FluxTupleHeader *tuple_hdr;

		if (!ItemIdIsNormal(itemid))
			continue;

		/* Skip overflow records */
		if (FluxIsOverflowRecordInline(PageGetItem(page, itemid),
								  ItemIdGetLength(itemid)))
			continue;

		tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

		/*
		 * A deleted tuple can be pruned if: - UNCOMMITTED is NOT set (the
		 * inserting/deleting xact committed) - commit_ts is older than the
		 * oldest active snapshot
		 *
		 * If UNCOMMITTED is still set, the transaction is still in progress
		 * (or aborted but not yet cleaned up) -- skip it.
		 */
		if (FluxTupleDeadToAll(tuple_hdr, oldest_xmin_prune))
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
		FluxPageClearFlag(opaque, FLUX_PAGE_DEFRAG_NEEDED);
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		return 0;
	}

	/*
	 * We have reclaimable dead tuples.  Mark them LP_DEAD (reclaiming their
	 * storage) and defragment the page to compact free space.
	 *
	 * CRITICAL: opportunistic pruning must set LP_DEAD, never LP_UNUSED.  A
	 * deleted FLUX tuple may still be referenced by index entries that only
	 * VACUUM (after index bulk-delete) is allowed to remove.  LP_DEAD reclaims
	 * the tuple's storage while reserving its line pointer so the TID cannot be
	 * recycled by a later INSERT (PageAddItemExtended only reuses LP_UNUSED
	 * slots).  Recycling a TID whose index entries still exist would let an
	 * index scan return the unrelated tuple later placed in that slot.  Only
	 * VACUUM Phase III converts LP_DEAD -> LP_UNUSED, after Phase II has removed
	 * the corresponding index entries.  This mirrors heap pruning, which sets
	 * LP_DEAD and defers LP_UNUSED to post-index-cleanup VACUUM.
	 */
	START_CRIT_SECTION();

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		FluxTupleHeader *tuple_hdr;

		if (!ItemIdIsNormal(itemid))
			continue;

		/* Skip overflow records */
		if (FluxIsOverflowRecordInline(PageGetItem(page, itemid),
								  ItemIdGetLength(itemid)))
			continue;

		tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

		if (FluxTupleDeadToAll(tuple_hdr, oldest_xmin_prune))
		{
			ItemIdSetDead(itemid);
		}
	}

	FluxPageDefragment(page);

	MarkBufferDirty(buffer);

	/* WAL-log the defragmentation using a proper defrag record */
	if (RelationNeedsWAL(relation))
	{
		XLogRecPtr	recptr;

		/*
		 * Use FluxXLogDefrag (not FluxXLogInitPage).  INIT_PAGE with
		 * REGBUF_WILL_INIT would zero the page during redo, losing all live
		 * tuples.  The defrag record uses REGBUF_STANDARD which stores a Full
		 * Page Image, preserving the page contents.
		 */
		recptr = FluxXLogDefrag(relation, buffer, NULL, 0, oldest_ts);
		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/* Update FSM with the reclaimed free space */
	FluxRecordFreeSpace(relation, BufferGetBlockNumber(buffer),
						 PageGetFreeSpace(page));

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	return true;
}


/*
 * Insert a tuple into a FLUX table
 */
void
flux_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
				   uint32 options, BulkInsertState bistate)
{
	FluxTuple	flux_tuple;
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	BlockNumber target_block;
	Size		tuple_size;
	ItemPointer tid = &slot->tts_tid;
	uint64		current_ts;
	FluxOverflowBuffers overflow_buffers;
	FluxLogicalImage insert_logical_img;
	Buffer		undo_buffer = InvalidBuffer;
	RelUndoRecPtr undo_ptr = InvalidRelUndoRecPtr;
	Size		saveFreeSpace;
	int			i;
	Datum		toast_values[MaxTupleAttributeNumber];
	bool		toast_isnull[MaxTupleAttributeNumber];
	ToastAttrInfo toast_attr[MaxTupleAttributeNumber];
	ToastTupleContext ttc;
	bool		toasted = false;

	slot_getallattrs(slot);

	/*
	 * Get current timestamp for the page-level commit-ts bookkeeping word.
	 *
	 * IMPORTANT: Get transaction timestamp here, BEFORE entering critical
	 * section, because FluxGetTransactionTimestamp() may need to allocate
	 * memory to initialize transaction state.
	 *
	 * CRITICAL FIX: Use FluxGetTransactionTimestamp() for both current_ts
	 * and xact_ts to ensure consistency. The inserted tuple will be visible
	 * within the same transaction because the snapshot timestamp will match
	 * the tuple's commit timestamp.
	 */
	(void) FluxGetTransactionTimestamp();

	current_ts = (uint64) FluxGetDmlTimestamp();

	/*
	 * Create FLUX tuple from slot.  Use the overflow-aware variant which
	 * will store large attributes (> FLUX_OVERFLOW_THRESHOLD) in overflow
	 * records on normal data pages, replacing them with compact inline
	 * overflow pointers.
	 *
	 * Overflow buffers are kept pinned for atomic WAL logging inside the
	 * critical section below.
	 */
	overflow_buffers.count = 0;

	/*
	 * FLUX has no on-page overflow: TOAST any wide varlena columns into the
	 * relation's standard heap TOAST table before forming the tuple, exactly
	 * like heap.  On return toast_values[] holds the (possibly externalized)
	 * datums that FluxFormTuple stores verbatim.
	 */
	memcpy(toast_values, slot->tts_values,
		   RelationGetDescr(relation)->natts * sizeof(Datum));
	memcpy(toast_isnull, slot->tts_isnull,
		   RelationGetDescr(relation)->natts * sizeof(bool));
	flux_toast_tuple(relation, toast_values, toast_isnull, NULL, NULL,
					 &ttc, toast_attr, &toasted, 0);

	flux_tuple = FluxFormTuple(RelationGetDescr(relation),
								 toast_values,
								 toast_isnull,
								 relation,
								 &overflow_buffers);

	/* Set MVCC fields (heap-shaped xmin/xmax) */
	flux_tuple->t_data->t_commit_ts = 0;	/* t_xmax = InvalidTransactionId */

	/*
	 * Stamp the inserter XID as t_xmin.  Visibility resolves t_xmin against
	 * CLOG + the reader's snapshot (FluxTupleSatisfiesMVCC), so an
	 * uncommitted insert is automatically invisible to other snapshots.  The
	 * FLUX_TUPLE_UNCOMMITTED flag is kept for the sLog write-conflict and
	 * defrag paths, but it no longer drives read visibility.
	 */
	flux_tuple->t_data->t_flags |= FLUX_TUPLE_UNCOMMITTED;
	flux_tuple->t_data->t_xmin = GetCurrentTransactionId();  /* subxid: heap-shaped, so savepoint rollback marks it aborted in CLOG */

	/*
	 * WS-PVS1: the version-chain head lives in the fixed header field
	 * t_verptr (counted by FLUX_TUPLE_OVERHEAD), so every tuple carries it
	 * from birth with no on-page growth.  Stamp InvalidRelUndoRecPtr here; the
	 * first UPDATE replaces it with a real chain head.  Readers treat an
	 * invalid head as "no history" (flux_pvs.c), so a never-updated row is
	 * unaffected.  Because the slot width never changes on UPDATE, the first
	 * UPDATE is always a same-length overwrite (Strategy 1 / CAS fast path).
	 */
	flux_tuple->t_data->t_flags |= FLUX_TUPLE_HAS_VERSION_PTR;
	FluxTupleSetVersionPtr(flux_tuple->t_data, flux_tuple->t_len,
							InvalidRelUndoRecPtr);
	tuple_size = flux_tuple->t_len;

	/* Ensure relation storage exists */
	RelationGetSmgr(relation);

	/*
	 * Find a page with enough free space.
	 *
	 * Like heap (RelationGetBufferForTuple), we first try the page we last
	 * inserted into, cached per-relation in the relcache via
	 * RelationSetTargetBlock().  Only if we have no cached target do we ask
	 * the FSM.  This avoids an fsm_search() + RecordPageWithFreeSpace() on
	 * every append: on an append-heavy table (e.g. pgbench_history) every
	 * backend otherwise pounds the same tail FSM pages, saturating the FSM
	 * buffer-header spinlock (LockBufHdr) at high concurrency.  The cache is
	 * only a hint -- a stale or too-full target just falls through to the FSM
	 * below and to the have_page free-space recheck.
	 *
	 * The cached-target fast path is used only when the tuple has no overflow
	 * data: with overflow, FluxFormTupleWithOverflow has already chosen and
	 * locked pages and the target_block/overflow-buffer interplay below
	 * assumes the FSM was consulted, so that path stays on the FSM.
	 *
	 * Account for fill factor: reserve space for future in-place updates.
	 */
	saveFreeSpace = RelationGetTargetPageFreeSpace(relation,
												   FLUX_DEFAULT_FILLFACTOR);
	{
		target_block = InvalidBlockNumber;
		if (overflow_buffers.count == 0)
			target_block = RelationGetTargetBlock(relation);

		if (target_block == InvalidBlockNumber)
			target_block = FluxGetPageWithFreeSpace(relation,
													 tuple_size + saveFreeSpace);
	}

	if (target_block == InvalidBlockNumber)
	{
		/* Clean up overflow buffers before throwing error */
		for (i = 0; i < overflow_buffers.count; i++)
		{
			UnlockReleaseBuffer(overflow_buffers.buffers[i].buffer);
			pfree(overflow_buffers.buffers[i].record_data);
		}
		elog(ERROR, "FLUX failed to allocate page for tuple insertion");
	}

	/*
	 * Pre-allocate WAL buffer space BEFORE acquiring the data buffer lock.
	 * XLogEnsureRecordSpace() may allocate memory, so it MUST be called
	 * outside the critical section.
	 *
	 * rdata slots needed: MAX_OVERFLOW_BUFFERS * 2 (header + data per
	 * overflow record) + 2 (xl_flux_insert header + tuple data)
	 */
	if (RelationNeedsWAL(relation))
		XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID, 3 + MAX_OVERFLOW_BUFFERS * 2);

	/*
	 * Check if target_block is already locked in overflow_buffers from
	 * FluxFormTupleWithOverflow. If FSM returns the same block for both
	 * overflow storage and main tuple storage, we must reuse that buffer to
	 * avoid double-locking.
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

	/*
	 * Verify the page has room for the tuple PLUS the fillfactor reservation,
	 * so in-place UPDATE growth stays possible (heap applies the same
	 * targetFreeSpace = len + saveFreeSpace bar).  A cached target block or a
	 * stale FSM answer that lacks the slack falls through to FSM retry /
	 * relation extension below.
	 */
	if (PageGetFreeSpace(page) < tuple_size + saveFreeSpace)
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
		 * must release our lock first since FluxPagePruneOpt() takes its own
		 * conditional lock.
		 *
		 * IMPORTANT: Skip pruning if buffer is from overflow_buffers, as we
		 * must keep those buffers locked.
		 */
		if (!buffer_is_from_overflow)
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			if (FluxPagePruneOpt(relation, buffer))
			{
				/* Pruning freed space -- re-lock and check again */
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				page = BufferGetPage(buffer);
				if (PageGetFreeSpace(page) >= tuple_size + saveFreeSpace)
					goto have_page;
				/* Still not enough after pruning, fall through to FSM retry */
			}

			/*
			 * FSM information was stale or pruning didn't help. Update and
			 * retry.
			 */
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			FluxRecordFreeSpace(relation, target_block, PageGetFreeSpace(page));
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
			FluxRecordFreeSpace(relation, target_block, PageGetFreeSpace(page));
		}

		/*
		 * Retry with updated FSM, excluding blocks in overflow_buffers. Keep
		 * trying until we find a suitable page that we don't already have
		 * locked for overflow storage.
		 */
		for (;;)
		{
			target_block = FluxGetPageWithFreeSpace(relation,
													 tuple_size + saveFreeSpace);
			if (target_block == InvalidBlockNumber)
			{
				/* Clean up overflow buffers before throwing error */
				for (i = 0; i < overflow_buffers.count; i++)
				{
					UnlockReleaseBuffer(overflow_buffers.buffers[i].buffer);
					pfree(overflow_buffers.buffers[i].record_data);
				}
				elog(ERROR, "FLUX failed to allocate page for tuple insertion after retry");
			}

			/*
			 * Check if target_block is already locked in overflow_buffers. If
			 * so, skip it and try again - we need a different page.
			 */
			buffer = InvalidBuffer;
			for (i = 0; i < overflow_buffers.count; i++)
			{
				if (BufferGetBlockNumber(overflow_buffers.buffers[i].buffer) == target_block)
				{
					/*
					 * This block is already used for overflow - mark FSM and
					 * retry
					 */
					FluxRecordFreeSpace(relation, target_block, 0);
					buffer = InvalidBuffer;
					break;
				}
			}

			/*
			 * If we found a block not in overflow_buffers, check if it has
			 * space
			 */
			if (i >= overflow_buffers.count)
			{
				buffer = ReadBuffer(relation, target_block);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				page = BufferGetPage(buffer);

				/*
				 * Verify the page actually has enough space. If not, update
				 * FSM and retry.
				 */
				if (PageGetFreeSpace(page) >= tuple_size + saveFreeSpace)
				{
					/* Found a suitable page - exit retry loop */
					break;
				}
				else
				{
					/* FSM was wrong - update it and retry */
					FluxRecordFreeSpace(relation, target_block, PageGetFreeSpace(page));
					UnlockReleaseBuffer(buffer);
					buffer = InvalidBuffer;
					/* Continue outer loop to try again */
				}
			}
		}
	}

have_page:

	/*
	 * SSI: check for rw-conflict in.  An INSERT may conflict with a
	 * concurrent serializable transaction that holds a relation-level or
	 * page-level predicate lock (e.g., from a range scan that would have
	 * included this new tuple).  Pass NULL tid since the tuple doesn't exist
	 * yet — only relation-level and page-level locks are checked.
	 */
	CheckForSerializableConflictIn(relation, NULL, BufferGetBlockNumber(buffer));

	/*
	 * Ensure the current transaction has an XID assigned BEFORE entering the
	 * critical section.  GetCurrentTransactionId() may call
	 * XactLockTableInsert() which acquires a lock and allocates memory --
	 * both forbidden in a critical section.  Most inserts already have an XID
	 * by now (assigned during the unique-index check), but inserting a NULL
	 * into a UNIQUE column skips that check, so the first assignment can
	 * otherwise land inside the crit section below.
	 *
	 * An assigned XID is also required for correctness: WAL records without
	 * an attached xid cannot be decoded into a logical replication stream
	 * (ReorderBuffer groups changes by xid and emits no commit record for
	 * InvalidTransactionId), and RecordTransactionCommit() would treat the
	 * transaction as read-only and skip the WAL flush.
	 */
	(void) GetCurrentTransactionId();

	/*
	 * Final free-space check before entering the critical section.
	 * PageGetFreeSpace may have been optimistic (alignment, line pointer
	 * overhead).  If the page can't actually fit the tuple, release it,
	 * update FSM, and extend the relation instead.  This prevents the PANIC
	 * that would otherwise fire inside the critical section.
	 */
	offnum = FluxPageAddTuple(page, flux_tuple, tuple_size);
	if (offnum == InvalidOffsetNumber)
	{
		/* Page too full despite FSM claim — record actual free space */
		FluxRecordFreeSpace(relation, BufferGetBlockNumber(buffer),
							 PageGetFreeSpace(page));
		UnlockReleaseBuffer(buffer);

		/* Extend the relation to get a guaranteed-empty page */
		buffer = ReadBuffer(relation, P_NEW);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);
		FluxInitPage(page, BufferGetPageSize(buffer));

		offnum = FluxPageAddTuple(page, flux_tuple, tuple_size);
		if (offnum == InvalidOffsetNumber)
			elog(ERROR, "failed to add FLUX tuple to new empty page (tuple_size=%zu)",
				 (Size) tuple_size);
	}

	/*
	 * Prepare the heap-format logical-decoding image before entering the
	 * critical section (it calls palloc/heap_form_tuple, which are forbidden
	 * inside a crit section).  No-op unless the relation is logically logged.
	 */
	FluxXLogPrepareLogicalImage(relation, flux_tuple, &insert_logical_img);

	/*
	 * Per-relation UNDO: reserve space for an INSERT UNDO record before the
	 * critical section, because RelUndoReserve() may extend the UNDO fork and
	 * error out.  This is done after acquiring the data buffer lock to keep a
	 * consistent lock ordering (data buffer -> UNDO buffer) with the UPDATE
	 * and DELETE paths.
	 */
	if (smgrexists(RelationGetSmgr(relation), RELUNDO_FORKNUM))
		undo_ptr = RelUndoReserve(relation,
								  SizeOfRelUndoRecordHeader +
								  sizeof(RelUndoInsertPayload),
								  &undo_buffer);

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	/* Set the tuple's TID */
	ItemPointerSet(tid, BufferGetBlockNumber(buffer), offnum);
	flux_tuple->t_self = *tid;
	slot->tts_tableOid = RelationGetRelid(relation);
	/*
	 * Set the on-disk tuple's t_ctid to point to itself.  This is needed for
	 * update chains and cross-page defragmentation, which check whether
	 * t_ctid == self to detect tuples that are not part of an update chain.
	 */
	{
		ItemId		inserted_itemid = PageGetItemId(page, offnum);
		FluxTupleHeader *inserted_hdr = (FluxTupleHeader *) PageGetItem(page, inserted_itemid);

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
	 * This matches the fix applied to FluxXLogInitPage in flux_fsm.c.
	 */
	{
		FluxPageOpaque phdr = FluxPageGetOpaque(page);

		FluxPageSetCommitTs(phdr, Max(FluxPageGetCommitTs(phdr), current_ts));
	}

	MarkBufferDirty(buffer);

	/* Log the insertion with all overflow buffers atomically */
	if (RelationNeedsWAL(relation))
	{
		XLogRecPtr	recptr = FluxXLogInsert(relation, buffer, offnum,
											 flux_tuple, current_ts,
											 &overflow_buffers,
											 &insert_logical_img,
											 false);

		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	FluxXLogReleaseLogicalImage(&insert_logical_img);

	/*
	 * Release all overflow buffers and free their cached data.
	 *
	 * IMPORTANT: Due to spatial locality optimization, an overflow buffer
	 * might be the SAME as the main buffer or as another overflow buffer
	 * (when overflow data is placed on the same page). Skip releasing buffers
	 * that were already released.  The main buffer is NOT released yet —
	 * only overflow buffers that differ from it.
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
	 * IMPORTANT: This must happen BEFORE FluxVMUpdateForInsert to maintain
	 * consistent buffer lock ordering across forks.  The UPDATE and DELETE
	 * paths already follow this ordering.
	 */

	/*
	 * Per-relation UNDO: write the INSERT record now that the insert is
	 * complete and register it so rollback can find and reverse it.  The
	 * record stores the inserted TID range; rollback marks the line pointer
	 * unused.
	 */
	if (RelUndoRecPtrIsValid(undo_ptr))
	{
		RelUndoRecordHeader undo_hdr;
		RelUndoInsertPayload undo_payload;

		undo_hdr.urec_type = RELUNDO_INSERT;
		undo_hdr.urec_len = SizeOfRelUndoRecordHeader +
			sizeof(RelUndoInsertPayload);
		undo_hdr.urec_xid = GetCurrentTransactionId();
		undo_hdr.urec_prevundorec = GetPerRelUndoPtr(RelationGetRelid(relation));
		undo_hdr.info_flags = 0;
		undo_hdr.tuple_len = 0;

		undo_payload.firsttid = *tid;
		undo_payload.endtid = *tid;

		RelUndoFinish(relation, undo_buffer, undo_ptr, &undo_hdr,
					  &undo_payload, sizeof(RelUndoInsertPayload));

		RegisterPerRelUndo(RelationGetRelid(relation), undo_ptr);
	}

	/*
	 * Clear visibility map bits while buffer is still locked.  This is
	 * usually a fast no-op for newly created tables (no VM fork yet).
	 */
	FluxVMUpdateForInsert(relation, flux_tuple->t_data, buffer);

	/*
	 * Save free space while we still have the buffer locked, then release the
	 * data buffer as soon as possible to reduce contention on hot pages. The
	 * remaining operations (FSM update, sLog registration) don't need the
	 * data buffer lock.
	 */
	{
		Size		saved_free_space = PageGetFreeSpace(page);
		BlockNumber saved_blkno = BufferGetBlockNumber(buffer);

		UnlockReleaseBuffer(buffer);

		/* Update FSM with remaining free space on the page */
		FluxRecordFreeSpace(relation, saved_blkno, saved_free_space);

		/*
		 * Cache this block as the next insert target (heap's
		 * RelationSetTargetBlock pattern) so the following append skips the
		 * FSM search entirely.  Only on the no-overflow path -- the overflow
		 * path deliberately stays on the FSM (see the target-block lookup
		 * above).  If the page is now too full for the next tuple, the
		 * have_page free-space recheck falls back to the FSM.
		 */
		if (overflow_buffers.count == 0)
			RelationSetTargetBlock(relation, saved_blkno);
	}

	/*
	 * Lightweight subtransaction tracking for savepoint rollback.
	 *
	 * We do NOT create a full shared sLog entry here by default (that caused
	 * "out of shared memory" during bulk inserts with 100K+ rows). Instead,
	 * we record (tid, xid, subxid) in the per-backend local list only.  If a
	 * savepoint is rolled back, SLogTupleRemoveBySubXid will find the
	 * matching local entries and create a shared sLog ABORTED entry at that
	 * time.
	 *
	 * Speculative inserts (ON CONFLICT) are handled by the separate
	 * flux_tuple_insert_speculative() function, which still registers full
	 * sLog entries for the speculative token.
	 */
	FluxEnsureSLogCallbacks();
	SLogTupleTrackLocalOnly(RelationGetRelid(relation), tid,
							GetTopTransactionId(),
							GetCurrentSubTransactionId());

	FluxFreeTuple(flux_tuple);

	/* Release toasting temporaries now that the tuple is durably stored. */
	if (toasted)
		flux_toast_cleanup(&ttc);

	pgstat_count_heap_insert(relation, 1);
}

/*
 * Delete a tuple from a FLUX table with proper tombstone marking
 */
TM_Result
flux_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
				   uint32 options, Snapshot snapshot, Snapshot crosscheck,
				   bool wait, TM_FailureData *tmfd)
{
	BlockNumber blkno;
	OffsetNumber offnum;
	Buffer		buffer;
	Page		page;
	ItemId		itemid;
	FluxTupleHeader *tuple_hdr;
	uint64		current_ts;
	bool		have_tuple_lock;
	FluxTuple	old_tuple_for_delete_wal;
	FluxLogicalImage delete_logical_img;
	Buffer		del_undo_buffer = InvalidBuffer;
	RelUndoRecPtr del_undo_ptr = InvalidRelUndoRecPtr;
	TransactionId del_hint_xid;

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
	 * (FluxLockTuple) are only needed for SELECT FOR UPDATE/SHARE, not for
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

	tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

	/*
	 * Check if tuple is already deleted (tombstone exists).  A DELETED flag
	 * can mean either a committed delete OR an in-progress delete by a
	 * concurrent transaction (which now also leaves UNCOMMITTED set).
	 * Distinguish the two: if another in-progress transaction owns the
	 * delete, wait for it and retry, matching heap's behavior where the
	 * second DELETE blocks behind the first. Only report TM_Deleted once the
	 * delete is genuinely committed (or ours).
	 */
	if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
	{
		TransactionId del_xid = InvalidTransactionId;
		bool		del_is_insert = false;

		if (tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED)
			del_xid = SLogTupleGetDirtyXid(RelationGetRelid(relation),
										   tid, &del_is_insert);

		if (wait && TransactionIdIsValid(del_xid) &&
			!TransactionIdIsCurrentTransactionId(del_xid) &&
			!del_is_insert)
		{
			TransactionId wait_xid = del_xid;

			UnlockReleaseBuffer(buffer);
			XactLockTableWait(wait_xid, relation, tid, XLTW_Delete);

			/* Re-read after waking; the delete committed or aborted. */
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
			tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

			/*
			 * If the delete committed, the tombstone remains: report it as
			 * deleted so the executor can re-evaluate via EPQ.  If it
			 * aborted, the before-image was restored (DELETED cleared) and we
			 * fall through to perform our own delete.
			 */
			if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
			{
				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = wait_xid;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				return TM_Deleted;
			}
		}
		else
		{
			if (tmfd)
			{
				tmfd->ctid = *tid;
				tmfd->xmax = GetCurrentTransactionId();
				tmfd->cmax = InvalidCommandId;
				tmfd->traversed = false;
			}
			UnlockReleaseBuffer(buffer);
			return TM_Deleted;
		}
	}

	/*
	 * Handle LOCKED flag: same logic as the UPDATE path — clear our own
	 * lock before proceeding with the delete.
	 */
	if (tuple_hdr->t_flags & FLUX_TUPLE_LOCKED)
	{
		SLogTupleOp lock_entry;
		int			nfound;

		nfound = SLogTupleLookupFiltered(RelationGetRelid(relation), tid,
										 GetCurrentTransactionId(), &lock_entry, 1);
		if (nfound > 0 &&
			(lock_entry.op_type == SLOG_OP_LOCK_SHARE ||
			 lock_entry.op_type == SLOG_OP_LOCK_EXCL))
		{
			tuple_hdr->t_flags &= ~FLUX_TUPLE_LOCKED;
		}
	}

	/*
	 * Fast-path: clear stale UNCOMMITTED flag (same optimization as UPDATE).
	 * We hold the buffer lock exclusively, so this is safe.
	 */
	if ((tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED) &&
		!(tuple_hdr->t_flags & (FLUX_TUPLE_DELETED | FLUX_TUPLE_UPDATED)))
	{
		if (!SLogTupleHasEntry(RelationGetRelid(relation), tid))
		{
			tuple_hdr->t_flags &= ~FLUX_TUPLE_UNCOMMITTED;
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

		visible = FluxTupleVisibleToSnapshotDual(tuple_hdr, snapshot,
												  RelationGetRelid(relation),
												  buffer);

		if (!visible)
		{
			TransactionId dirty_xid;
			bool		is_insert_entry;

			/*
			 * Lock-free: SLogTupleGetDirtyXid reads the seqlock-guarded sLog flat hash with
			 * EBR.  No need to release buffer lock.
			 */
			dirty_xid = SLogTupleGetDirtyXid(RelationGetRelid(relation),
											 tid,
											 &is_insert_entry);

			/* Check if tuple was deleted by another transaction */
			if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
			{
				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = TransactionIdIsValid(dirty_xid) ?
						dirty_xid : GetCurrentTransactionId();
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				return TM_Deleted;
			}

			/*
			 * Buffer lock was never released (wait-free sLog read), so the
			 * tuple cannot have changed.  Proceed with dirty_xid.
			 */
			{
				if (TransactionIdIsValid(dirty_xid) && is_insert_entry)
				{
					if (tmfd)
					{
						tmfd->ctid = *tid;
						tmfd->xmax = dirty_xid;
						tmfd->cmax = InvalidCommandId;
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
						tuple_hdr = (FluxTupleHeader *)
							PageGetItem(page, itemid);

						if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
						{
							if (tmfd)
							{
								tmfd->ctid = *tid;
								tmfd->xmax = wait_xid;
								tmfd->cmax = InvalidCommandId;
								tmfd->traversed = false;
							}
							UnlockReleaseBuffer(buffer);
							return TM_Deleted;
						}

						visible = FluxTupleVisibleToSnapshotDual(tuple_hdr, snapshot,
																  RelationGetRelid(relation),
																  buffer);

						if (!visible)
						{
							/*
							 * Same EPQ livelock fix as the UPDATE path: check
							 * for our own LOCK entry before returning
							 * TM_Updated.
							 */
							TransactionId myxid_postw =
								GetCurrentTransactionIdIfAny();

							if (TransactionIdIsValid(myxid_postw))
							{
								SLogTupleOp my_epw;
								int			my_nfound_postw;

								my_nfound_postw = SLogTupleLookupFiltered(
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
										tmfd->cmax = InvalidCommandId;
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
									tmfd->cmax = InvalidCommandId;
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
							tmfd->cmax = InvalidCommandId;
							tmfd->traversed = false;
						}
						UnlockReleaseBuffer(buffer);
						return TM_WouldBlock;
					}
				}
				else
				{
					/*
					 * No in-progress sLog entry for another txn. Same
					 * EPQ-loop fix as the UPDATE path: check if our
					 * transaction already has a sLog entry (from
					 * table_tuple_lock during EPQ).  If so, fall through;
					 * otherwise trigger EPQ.
					 */
					TransactionId myxid_chk =
						GetCurrentTransactionIdIfAny();

					if (TransactionIdIsValid(myxid_chk))
					{
						SLogTupleOp my_entry;
						int			my_nfound;

						my_nfound = SLogTupleLookupFiltered(
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
								tmfd->cmax = InvalidCommandId;
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
							tmfd->cmax = InvalidCommandId;
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
	 * Same as the UPDATE path: even when visibility returned "true", check
	 * for in-progress modifications by another transaction.  Block if found.
	 */
	if (tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED)
	{
		TransactionId dirty_xid;
		bool		is_insert_entry;

		/* Lock-free: no buffer unlock needed */
		dirty_xid = SLogTupleGetDirtyXid(RelationGetRelid(relation),
										 tid, &is_insert_entry);

		if (!TransactionIdIsValid(dirty_xid))
		{
			/*
			 * No in-flight transaction is modifying this tuple.  The
			 * UNCOMMITTED flag is stale (left over from a committed
			 * transaction whose cleanup callback already ran).  Clear it
			 * opportunistically to prevent future visibility re-checks.
			 */
			tuple_hdr->t_flags &= ~FLUX_TUPLE_UNCOMMITTED;
			MarkBufferDirty(buffer);
		}
		else if (TransactionIdIsValid(dirty_xid) &&
				 !TransactionIdIsCurrentTransactionId(dirty_xid) &&
				 !is_insert_entry)
		{
			if (wait)
			{
				TransactionId wait_xid = dirty_xid;

				UnlockReleaseBuffer(buffer);
				XactLockTableWait(wait_xid, relation, tid, XLTW_Delete);

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
				tuple_hdr = (FluxTupleHeader *)
					PageGetItem(page, itemid);

				if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
				{
					if (tmfd)
					{
						tmfd->ctid = *tid;
						tmfd->xmax = wait_xid;
						tmfd->cmax = InvalidCommandId;
						tmfd->traversed = false;
					}
					UnlockReleaseBuffer(buffer);
					return TM_Deleted;
				}

				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = wait_xid;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				return TM_Updated;
			}
			else
			{
				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = dirty_xid;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				return TM_WouldBlock;
			}
		}
	}

	/*
	 * Authoritative write-write / lock-conflict gate (mirrors the UPDATE
	 * path's final gate).  The three checks above use SLogTupleGetDirtyXid,
	 * which only detects in-progress WRITERS and is blind to lock-only
	 * markers -- so a SELECT ... FOR UPDATE/FOR SHARE locker (which leaves a
	 * SLOG_OP_LOCK_EXCL/LOCK_SHARE marker on a committed, non-deleted,
	 * non-UNCOMMITTED tuple) would sail straight through and DELETE the row
	 * the locker is protecting.  Probe with SLogTupleGetWriteConflictXid,
	 * which additionally reports a locker whose recorded LockTupleMode
	 * conflicts with ours under the real heavyweight matrix.
	 *
	 * LockTupleMode: LockTupleExclusive (AccessExclusiveLock), matching heap
	 * DELETE ("we need the strongest one").  A DELETE destroys the key, so it
	 * must conflict with FOR KEY SHARE FK lockers too; unlike UPDATE (which
	 * uses NoKeyExclusive precisely to stay compatible with a KeyShare FK
	 * lock), waiting on a KeyShare locker here is CORRECT, not spurious --
	 * the FK machinery guarantees that locker releases before the referenced
	 * row can go away.
	 *
	 * Self-wait / EPQ safety: SLogTupleGetWriteConflictXid skips our own xid
	 * internally, so XactLockTableWait never receives GetTopTransactionId()
	 * (no lmgr self-wait assertion).  A lock this transaction took during EPQ
	 * re-evaluation is our own marker and is likewise skipped.  Before
	 * sleeping we acquire the heavyweight LOCKTAG_TUPLE lock so two writers
	 * racing the same tuple queue FIFO instead of mutually XactLockTableWait
	 * deadlocking (matches heap_delete's "establish our priority").
	 */
	{
		TransactionId conflict_xid;
		bool		conflict_is_insert = false;

		conflict_xid = SLogTupleGetWriteConflictXid(RelationGetRelid(relation),
													tid, LockTupleExclusive,
													&conflict_is_insert);

		if (TransactionIdIsValid(conflict_xid) &&
			!TransactionIdIsCurrentTransactionId(conflict_xid) &&
			!conflict_is_insert)
		{
			if (wait)
			{
				TransactionId wait_xid = conflict_xid;

				UnlockReleaseBuffer(buffer);
				if (!have_tuple_lock)
					FluxLockTuple(relation, tid, LockTupleExclusive,
								   true, &have_tuple_lock);
				XactLockTableWait(wait_xid, relation, tid, XLTW_Delete);

				/* Re-read and re-classify after the conflicter finished. */
				buffer = ReadBuffer(relation, blkno);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				page = BufferGetPage(buffer);

				if (offnum < FirstOffsetNumber ||
					offnum > PageGetMaxOffsetNumber(page))
				{
					UnlockReleaseBuffer(buffer);
					if (have_tuple_lock)
						FluxUnlockTuple(relation, tid, LockTupleExclusive);
					return TM_Invisible;
				}
				itemid = PageGetItemId(page, offnum);
				if (!ItemIdIsNormal(itemid))
				{
					UnlockReleaseBuffer(buffer);
					if (have_tuple_lock)
						FluxUnlockTuple(relation, tid, LockTupleExclusive);
					return TM_Invisible;
				}
				tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

				/*
				 * If the conflicter was a writer that deleted the tuple,
				 * report TM_Deleted for EPQ.  A locker leaves the tuple
				 * intact -- we hold the tuple lock and now own the FIFO
				 * slot, so fall through to perform the delete.  We do NOT
				 * re-probe for further lockers: any locker that queued
				 * after us is behind us on LOCKTAG_TUPLE and will not be
				 * granted until we release, so re-waiting could livelock.
				 */
				if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
				{
					if (tmfd)
					{
						tmfd->ctid = *tid;
						tmfd->xmax = wait_xid;
						tmfd->cmax = InvalidCommandId;
						tmfd->traversed = false;
					}
					UnlockReleaseBuffer(buffer);
					if (have_tuple_lock)
						FluxUnlockTuple(relation, tid, LockTupleExclusive);
					return TM_Deleted;
				}
			}
			else
			{
				/* NOWAIT / SKIP LOCKED */
				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = conflict_xid;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				return TM_WouldBlock;
			}
		}
	}

	/*
	 * Get transaction timestamp BEFORE critical section. Use xact_ts as
	 * commit timestamp for within-transaction visibility.
	 */
	(void) FluxGetTransactionTimestamp();
	current_ts = (uint64) FluxGetDmlTimestamp();

	/*
	 * Allocate old_tuple structure and save a copy of the old tuple data
	 * BEFORE entering the critical section.  The tuple header will be
	 * modified below (DELETED flag, commit_ts, etc.), and the WAL record
	 * needs the unmodified before-image for UNDO support.
	 */
	{
		uint32		del_old_len = ItemIdGetLength(itemid);

		old_tuple_for_delete_wal = palloc(sizeof(FluxTupleData));
		old_tuple_for_delete_wal->t_len = del_old_len;
		old_tuple_for_delete_wal->t_data = (FluxTupleHeader *) palloc(del_old_len);
		memcpy(old_tuple_for_delete_wal->t_data, tuple_hdr, del_old_len);
	}

	/*
	 * Prepare the heap-format logical-decoding image of the deleted tuple
	 * before the critical section (palloc/heap_form_tuple are forbidden
	 * inside).  No-op unless the relation is logically logged.
	 */
	FluxXLogPrepareLogicalImage(relation, old_tuple_for_delete_wal,
								 &delete_logical_img);

	/*
	 * SSI: check for rw-conflict in.  If a concurrent serializable
	 * transaction read this tuple (holds a SIREAD lock on it), our delete
	 * creates an rw-antidependency that may form a dangerous structure.
	 */
	CheckForSerializableConflictIn(relation, tid, BufferGetBlockNumber(buffer));

	/*
	 * Pre-allocate WAL buffer space BEFORE entering critical section. DELETE
	 * operations only need the main buffer (no overflow).
	 *
	 * CRITICAL: XLogEnsureRecordSpace() may allocate memory, so it MUST be
	 * called outside the critical section.
	 */
	if (RelationNeedsWAL(relation))
		XLogEnsureRecordSpace(0, 2);

	/*
	 * Per-relation UNDO: reserve space for a DELETE UNDO record with full
	 * tuple data so rollback can restore the deleted tuple.  Must happen
	 * before the critical section since it may extend the UNDO fork.
	 */
	if (smgrexists(RelationGetSmgr(relation), RELUNDO_FORKNUM))
		del_undo_ptr = RelUndoReserve(relation,
									  SizeOfRelUndoRecordHeader +
									  sizeof(RelUndoDeletePayload) +
									  old_tuple_for_delete_wal->t_len,
									  &del_undo_buffer);

	/*
	 * Assign our top-level XID for the t_xmin stamp BEFORE the critical
	 * section: GetTopTransactionId() allocates a fresh XID on first use,
	 * which calls XactLockTableInsert -> LockAcquire -> palloc, forbidden
	 * inside a critical section.  Precompute here and write only the bare
	 * field below, matching the surrounding "compute before crit, write
	 * inside" pattern (del_undo_ptr, XLogEnsureRecordSpace, logical image).
	 */
	del_hint_xid = GetCurrentTransactionId();	/* subxid: heap-shaped t_xmax */

	/* Start critical section for WAL logging */
	START_CRIT_SECTION();

	/*
	 * Mark tuple as deleted with tombstone - this is the key FLUX feature.
	 * Set UNCOMMITTED so concurrent writers/readers consult the sLog while
	 * the delete is in flight: a second DELETE or UPDATE on this TID must
	 * detect the in-progress delete (via SLogTupleGetDirtyXid) and block on
	 * it, rather than racing ahead.  The flag is cleared at commit by
	 * flux_stamp_tuple_committed; VACUUM sees a clean committed delete via
	 * the xmax XID + CLOG.
	 *
	 * Heap-shaped: set t_xmax to the deleter XID (leaving t_xmin, the
	 * inserter, untouched).  Visibility resolves t_xmax against CLOG + the
	 * reader's snapshot: an old snapshot that predates the delete's commit
	 * still sees the row; a snapshot after it does not.
	 */
	tuple_hdr->t_flags |= FLUX_TUPLE_DELETED;
	tuple_hdr->t_flags |= FLUX_TUPLE_UNCOMMITTED;
	/* Fresh xmax being stamped: drop any stale XMAX_COMMITTED CLOG hint. */
	tuple_hdr->t_flags &= ~FLUX_TUPLE_XMAX_COMMITTED;
	FluxTupleSetXmax(tuple_hdr, del_hint_xid);
	/* Keep the original t_ctid for potential update chains */
	ItemPointerCopy(tid, &tuple_hdr->t_ctid);

	/* Update page header to match what redo does */
	{
		FluxPageOpaque phdr = FluxPageGetOpaque(page);

		FluxPageSetCommitTs(phdr, Max(FluxPageGetCommitTs(phdr), current_ts));
		FluxPageSetFlag(phdr, FLUX_PAGE_DEFRAG_NEEDED);
	}

	MarkBufferDirty(buffer);

	/* WAL log the deletion using the pre-saved old tuple copy */
	if (RelationNeedsWAL(relation))
	{
		XLogRecPtr	recptr;

		recptr = FluxXLogDelete(relation, buffer, offnum,
								 old_tuple_for_delete_wal,
								 (uint64) del_hint_xid,
								 &delete_logical_img);
		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	FluxXLogReleaseLogicalImage(&delete_logical_img);

	/*
	 * Per-relation UNDO: write the DELETE record now that the delete is
	 * complete.  The record stores the deleted TID plus the full old tuple so
	 * rollback can restore it in place.
	 */
	if (RelUndoRecPtrIsValid(del_undo_ptr))
	{
		RelUndoRecordHeader del_undo_hdr;
		RelUndoDeletePayload del_undo_payload;

		del_undo_hdr.urec_type = RELUNDO_DELETE;
		del_undo_hdr.urec_len = (uint16)
			(SizeOfRelUndoRecordHeader + sizeof(RelUndoDeletePayload) +
			 old_tuple_for_delete_wal->t_len);
		del_undo_hdr.urec_xid = GetCurrentTransactionId();
		del_undo_hdr.urec_prevundorec =
			GetPerRelUndoPtr(RelationGetRelid(relation));
		del_undo_hdr.info_flags = RELUNDO_INFO_HAS_TUPLE;
		del_undo_hdr.tuple_len = (uint16) old_tuple_for_delete_wal->t_len;

		del_undo_payload.ntids = 1;
		del_undo_payload.tids[0] = *tid;

		RelUndoFinishWithTuple(relation, del_undo_buffer, del_undo_ptr,
							   &del_undo_hdr,
							   &del_undo_payload, sizeof(RelUndoDeletePayload),
							   (const char *) old_tuple_for_delete_wal->t_data,
							   old_tuple_for_delete_wal->t_len);

		RegisterPerRelUndo(RelationGetRelid(relation), del_undo_ptr);
	}

	/*
	 * Clear visibility map bits for this page since we've deleted a tuple.
	 * The page is no longer all-visible.
	 */
	FluxVMUpdateForDelete(relation, buffer);

	/*
	 * Clean up overflow chains if this tuple has overflow attributes. This
	 * must happen outside the critical section since it performs its own
	 * buffer I/O.  We check the flag and capture the free space before
	 * releasing the buffer so we can read the tuple header and page state.
	 */
	{
		bool		has_overflow = (tuple_hdr->t_flags & FLUX_TUPLE_HAS_OVERFLOW) != 0;
		Size		free_space = PageGetFreeSpace(page);

		UnlockReleaseBuffer(buffer);

		/*
		 * Ensure xact/subxact callbacks are registered before any sLog
		 * operation.  This is critical for savepoint rollback: without the
		 * SubXactCallback, ROLLBACK TO SAVEPOINT won't restore tuples.
		 */
		FluxEnsureSLogCallbacks();

		/*
		 * Register the delete in the sLog AFTER releasing the buffer lock to
		 * avoid deadlocks with SLogTupleGetDirtyXid's slow path.
		 */
		SLogTupleInsert(RelationGetRelid(relation), tid,
						GetTopTransactionId(), SLOG_OP_DELETE,
						GetCurrentSubTransactionId(), cid, current_ts, 0,
						LockTupleNoKeyExclusive);

		/*
		 * Store before-image for savepoint rollback.  The tracked key was
		 * just created by SLogTupleInsert above.  We stash the original tuple
		 * data (saved before the critical section) so that ROLLBACK TO
		 * SAVEPOINT can physically restore the tuple.
		 *
		 * For DELETE, the before-image captures the original flags and
		 * commit_ts so we can undo the DELETED flag and timestamp.
		 */
		SLogTupleStoreBeforeImage(RelationGetRelid(relation), tid,
								  GetTopTransactionId(),
								  (const char *) old_tuple_for_delete_wal->t_data,
								  old_tuple_for_delete_wal->t_len,
								  old_tuple_for_delete_wal->t_data->t_flags,
								  old_tuple_for_delete_wal->t_data->t_commit_ts,
								  relation->rd_locator,
								  relation->rd_rel->relpersistence);

		/* Free old_tuple copy now that before-image has been stored */
		FluxFreeTuple(old_tuple_for_delete_wal);

		/* Mark this block dirty for the scan-path sLog bypass */
		FluxDirtyMapMark(RelationGetRelid(relation), blkno);

		/*
		 * NOTE: We do NOT immediately clean up overflow chains here.
		 * Immediate cleanup was: 1. Buggy (collected wrong overflow pointers
		 * after modification) 2. Expensive on hot paths (extra buffer I/O +
		 * locking) 3. Complex to WAL-log correctly
		 *
		 * Instead, overflow cleanup is deferred to VACUUM.  When VACUUM prunes
		 * deleted tuples, it will also reclaim orphaned overflow pages.
		 *
		 * Future enhancement: Log overflow block/offset in WAL DELETE record
		 * so UNDO log pruning can also clean up overflow chains.
		 */
		(void) has_overflow;	/* Suppress unused variable warning */

		/* Release tuple lock */
		if (have_tuple_lock)
			FluxUnlockTuple(relation, tid, LockTupleExclusive);

		/* Update free space map - deleted tuple creates more free space */
		FluxRecordFreeSpace(relation, blkno, free_space);
	}

	/* Return success - tuple was successfully marked as deleted */
	pgstat_count_heap_delete(relation);
	return TM_Ok;
}

/*
 * Release a set of overflow buffers collected during a force-shrink update.
 *
 * FluxStoreOverflowColumn leaves each overflow page locked, dirty, and
 * unlogged, handing the buffers back to the caller for atomic WAL logging on
 * the success path.  On any error path before that logging happens, the caller
 * must release them here so no dirty unlogged page is pinned into transaction
 * abort.  Skips main_buffer (released separately by the caller) and any buffer
 * that appears more than once (two overflow columns can land on one page).
 */
static void
flux_release_update_overflow_buffers(FluxOverflowBuffers *bufs,
									  Buffer main_buffer)
{
	int			i;

	for (i = 0; i < bufs->count; i++)
	{
		Buffer		ovf_buf = bufs->buffers[i].buffer;
		bool		already_released = (ovf_buf == main_buffer);
		int			j;

		for (j = 0; j < i && !already_released; j++)
		{
			if (bufs->buffers[j].buffer == ovf_buf)
				already_released = true;
		}
		if (!already_released)
			UnlockReleaseBuffer(ovf_buf);
		pfree(bufs->buffers[i].record_data);
	}
	bufs->count = 0;
}

/*
 * Update a tuple in a FLUX table with versioning support
 *
 * FLUX_RELEASE_TUPLOCK releases the heavyweight tuple lock that competing
 * updaters acquire before XactLockTableWait (see the wait sites below).  The
 * lock is held continuously from acquisition through the recheck/update so it
 * serializes racing updaters into a FIFO queue (matching heap_update); it must
 * be released on every function-exit path so it never leaks past commit.  The
 * have_tuple_lock guard makes the macro a no-op on paths that never acquired.
 */
#define FLUX_RELEASE_TUPLOCK() \
	do { \
		if (have_tuple_lock) \
		{ \
			FluxUnlockTuple(relation, otid, LockTupleNoKeyExclusive); \
			have_tuple_lock = false; \
		} \
	} while (0)

/*
 * flux_indexed_attr_changed
 *
 * Self-compute whether any INDEXED attribute changed value between the old
 * on-page tuple at 'otid' and the new row in 'slot'.  Stock PostgreSQL no
 * longer passes the modified-attrs set into tuple_update, so FLUX derives it
 * here exactly like heap does internally (HeapDetermineColumnsInfo): deform
 * the old on-page tuple, deform the new slot, and value-compare each indexed
 * attribute with datum_image_eq.
 *
 * Returns true if any indexed attribute differs, forcing the OUT-OF-PLACE
 * (delete + insert at new TID) update path so secondary indexes are rebuilt.
 * Whole-row (attr 0) and non-tableOID system attributes in the indexed set
 * are treated as changed (force out-of-place), matching heap's conservative
 * direction.  If the old tuple can't be read (gone/abnormal), we conservatively
 * return true so index maintenance still happens.
 */
static bool
flux_indexed_attr_changed(Relation relation, ItemPointer otid,
						  TupleTableSlot *slot)
{
	Bitmapset  *indexed;
	Bitmapset  *summarized;
	TupleDesc	tupdesc = RelationGetDescr(relation);
	BlockNumber blkno = ItemPointerGetBlockNumber(otid);
	OffsetNumber offnum = ItemPointerGetOffsetNumber(otid);
	Buffer		buffer;
	Page		page;
	ItemId		itemid;
	FluxTupleData old_tup;
	Datum		old_values[MaxTupleAttributeNumber];
	bool		old_isnull[MaxTupleAttributeNumber];
	bool		changed = false;
	int			attidx = -1;

	/*
	 * The full set of attributes referenced by ANY index on the relation:
	 * HOT_BLOCKING (ordinary/key indexes) plus SUMMARIZED (BRIN and friends).
	 * Heap's HeapDetermineColumnsInfo uses the same bitmaps; taking their union
	 * means "any indexed column changed" and lets FLUX force the out-of-place
	 * TU_All path, which rebuilds every index (summarizing included) at the new
	 * TID -- so FLUX never needs TU_Summarizing.
	 */
	indexed = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_HOT_BLOCKING);
	summarized = RelationGetIndexAttrBitmap(relation, INDEX_ATTR_BITMAP_SUMMARIZED);
	indexed = bms_add_members(indexed, summarized);
	bms_free(summarized);
	if (indexed == NULL)
		return false;			/* no indexed columns -> always in place */

	/* New values live in the slot; make sure they are all deformed. */
	slot_getallattrs(slot);

	/* Read the old on-page tuple. */
	buffer = ReadBuffer(relation, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
	{
		UnlockReleaseBuffer(buffer);
		bms_free(indexed);
		return true;			/* can't read old tuple -> be conservative */
	}
	itemid = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(itemid))
	{
		UnlockReleaseBuffer(buffer);
		bms_free(indexed);
		return true;
	}

	old_tup.t_len = ItemIdGetLength(itemid);
	old_tup.t_data = (FluxTupleHeader *) PageGetItem(page, itemid);
	ItemPointerSet(&old_tup.t_self, blkno, offnum);

	/*
	 * Deform only as far as the highest ordinary indexed column.  Indexed
	 * columns are usually the leading attributes, so on a wide table this
	 * avoids deforming (walking + fetching) every trailing non-indexed column
	 * just to compare a few keys -- one of the two per-update overheads called
	 * out in the write-gap diagnosis.  Whole-row (attrnum 0) and system
	 * (attrnum < 0) references are handled in the loop below without reading
	 * old_values[], so they do not raise the bound.
	 */
	{
		int			max_indexed_natts = 0;
		int			probe = -1;

		while ((probe = bms_next_member(indexed, probe)) >= 0)
		{
			AttrNumber	probenum = probe + FirstLowInvalidHeapAttributeNumber;

			if (probenum > max_indexed_natts)
				max_indexed_natts = probenum;
		}
		FluxDeformTupleUpTo(relation, &old_tup, tupdesc, old_values, old_isnull,
							max_indexed_natts);
	}

	while (!changed && (attidx = bms_next_member(indexed, attidx)) >= 0)
	{
		AttrNumber	attrnum = attidx + FirstLowInvalidHeapAttributeNumber;
		Form_pg_attribute att;
		Datum		old_val,
					new_val;
		bool		old_null,
					new_null;

		/* Whole-row reference: treat as changed (force all-index). */
		if (attrnum == 0)
		{
			changed = true;
			break;
		}

		/*
		 * System attributes other than tableOID cannot be compared meaningfully
		 * for an updated row: treat as changed.  tableOID never changes.
		 */
		if (attrnum < 0)
		{
			if (attrnum != TableOidAttributeNumber)
				changed = true;
			continue;
		}

		old_val = old_values[attrnum - 1];
		old_null = old_isnull[attrnum - 1];
		new_val = slot->tts_values[attrnum - 1];
		new_null = slot->tts_isnull[attrnum - 1];

		/* Null-ness differs -> changed. */
		if (old_null != new_null)
		{
			changed = true;
			break;
		}
		/* Both null -> unchanged. */
		if (new_null)
			continue;

		att = TupleDescAttr(tupdesc, attrnum - 1);
		if (!datum_image_eq(old_val, new_val, att->attbyval, att->attlen))
		{
			changed = true;
			break;
		}
	}

	UnlockReleaseBuffer(buffer);
	bms_free(indexed);
	return changed;
}

TM_Result
flux_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
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
	FluxTupleHeader *old_tuple_hdr;
	FluxTuple	new_tuple;
	Size		new_tuple_size;
	uint64		current_ts;
	bool		old_has_overflow = false;
	bool		have_tuple_lock = false;
	FluxTuple	old_tuple_for_inplace_wal;
	FluxLogicalImage update_old_img;
	FluxLogicalImage update_new_img;
	FluxOverflowBuffers update_overflow_buffers;
	int			upd_i;
	uint64		defrag_oldest_ts = 0;
	TransactionId defrag_oldest_xmin = InvalidTransactionId;
	bool		force_shrink_attempted = false;
	Buffer		upd_undo_buffer = InvalidBuffer;
	RelUndoRecPtr upd_undo_ptr = InvalidRelUndoRecPtr;
	Datum		upd_toast_values[MaxTupleAttributeNumber];
	bool		upd_toast_isnull[MaxTupleAttributeNumber];
	ToastAttrInfo upd_toast_attr[MaxTupleAttributeNumber];
	ToastTupleContext upd_ttc;
	bool		upd_toasted = false;

	/* Extract block and offset from old TID */
	blkno = ItemPointerGetBlockNumber(otid);
	offnum = ItemPointerGetOffsetNumber(otid);

	/* Validate TID range */
	if (blkno >= RelationGetNumberOfBlocks(relation))
		return TM_Invisible;

	/*
	 * OUT-OF-PLACE (heap-like) UPDATE for index-key changes.
	 *
	 * FLUX does not implement the RowID/gen index contract: secondary indexes
	 * carry plain heap-style TID entries.  If we kept the tuple in place while
	 * an indexed column changed, the executor would insert a new (newkey, TID)
	 * index entry while the old (oldkey, TID) entry still pointed at the same
	 * live TID -- the A->B->A duplicate bug, since there is no RowID/gen to
	 * disambiguate and no stale-entry recheck.
	 *
	 * Stock PostgreSQL no longer tells the AM which columns changed, so FLUX
	 * self-computes it (flux_indexed_attr_changed, matching heap's internal
	 * HeapDetermineColumnsInfo): when any indexed column changed, store the new
	 * version at a NEW TID (DELETE old + INSERT new, zheap's non-in-place
	 * update) and set *update_indexes = TU_All so the executor inserts fresh
	 * entries in every index at the new TID.  The old tuple's index entries
	 * then die with its TID and are reclaimed by ordinary VACUUM, exactly like
	 * heap.  Non-indexed-column UPDATEs fall through to the in-place path below
	 * and set TU_None (the zheap win: update without touching indexes).  FLUX
	 * never uses TU_Summarizing: a key-changing update always moves the TID, so
	 * TU_All already covers summarizing indexes.
	 */
	if (flux_indexed_attr_changed(relation, otid, slot))
	{
		TM_Result	del_res;

		del_res = flux_tuple_delete(relation, otid, cid, options,
									snapshot, crosscheck, wait, tmfd);
		if (del_res != TM_Ok)
			return del_res;

		/*
		 * Insert the new version.  flux_tuple_insert sets slot->tts_tid to the
		 * new location and TOASTs wide columns.  The old version remains
		 * reachable to older snapshots via the UNDO before-image the delete
		 * recorded; on ROLLBACK both the delete and the insert are undone.
		 */
		flux_tuple_insert(relation, slot, cid, options, NULL);

		if (update_indexes != NULL)
			*update_indexes = TU_All;
		if (lockmode != NULL)
			*lockmode = LockTupleExclusive;
		return TM_Ok;
	}

	/*
	 * In-place UPDATE: no indexed column changed, so the row keeps its TID and
	 * no index maintenance is needed.  Default the out-param to TU_None here so
	 * every TM_Ok exit of the in-place path below reports "no index update"
	 * (the caller does not pre-initialize *update_indexes; it reads it only on
	 * TM_Ok).
	 */
	if (update_indexes != NULL)
		*update_indexes = TU_None;

	/*
	 * --------------------------------------------------------------- FAST
	 * PATH: Same-size CAS update under SHARE_EXCLUSIVE buffer lock.
	 *
	 * For simple same-size updates (e.g. balance += delta in TPC-B), we can
	 * avoid the fully exclusive buffer lock by using a share-exclusive lock
	 * combined with a single-attempt per-tuple CAS trylock (t_writer).  This
	 * still allows concurrent readers while serializing writers on the same
	 * page.
	 *
	 * NOTE on t_writer contention (investigated, no change made): FluxTuple-
	 * WriterTryLock is a SINGLE compare-and-swap, not a spin loop.  On failure
	 * the backend does not retry -- it drops SHARE_EXCLUSIVE and falls to the
	 * exclusive slow path below, which is a queued, sleeping lock.  Moreover
	 * BUFFER_LOCK_SHARE_EXCLUSIVE already conflicts with itself (bufmgr.c
	 * BufferLockAttempt), so at most one CAS writer per buffer runs at a time;
	 * additional hot-row writers queue+sleep on the buffer content lock, not
	 * on t_writer.  There is therefore no t_writer spin storm to convert to a
	 * queued lock: the hot-row path ALREADY degrades to fair queued locks
	 * (BUFFER_LOCK_SHARE_EXCLUSIVE, then LOCKTAG_TUPLE + XactLockTableWait in
	 * the slow path).  The measured hot-row throughput decline (peak ~c8 then
	 * fall) is dominated by "Lock : tuple" heavyweight waits + per-update
	 * UNDO/sLog/WAL work under that lock -- the same serialization heap/zheap
	 * incur on a hot row -- NOT by spinning.  Switching the fast path to
	 * BUFFER_LOCK_EXCLUSIVE would not change this (both modes serialize one
	 * writer per buffer) and would regress the scattered case by blocking
	 * concurrent plain-SHARE readers.
	 *
	 * FIXME(flux hot-row scaling): the useful lever is reducing per-update
	 * work under the serialized lock (fold UNDO/sLog/WAL work, shrink the
	 * critical section) or a group-update / delta-accumulation scheme, not a
	 * writer-lock mode change.  Deferred: any such change is a correctness-
	 * sensitive redesign (lost-update + self-wait hazards) and must be
	 * benchmark-driven, not speculative.  See TASK B analysis in the handoff.
	 *
	 * Eligibility requirements: - New tuple must be the same on-disk size as
	 * the old tuple - Old tuple must not have overflow data - Old tuple must
	 * not be deleted, locked, or uncommitted - No speculative insertion -
	 * Must be a simple UPDATE (not HOT-chain following) - Snapshot visibility
	 * must be trivially true (committed tuple) - Relation must need WAL (for
	 * crash safety)
	 *
	 * If any condition fails, we fall through to the exclusive-lock path.
	 * ---------------------------------------------------------------
	 */
	{
		FluxTuple	cas_new_tuple;
		Size		cas_new_size;

		/* Form the new tuple speculatively (no overflow handling) */
		slot_getallattrs(slot);
		cas_new_tuple = FluxFormTuple(RelationGetDescr(relation),
									   slot->tts_values,
									   slot->tts_isnull,
									   NULL,	/* no overflow */
									   NULL);

		cas_new_size = cas_new_tuple->t_len;

		/* Attempt the CAS fast path */
		buffer = ReadBuffer(relation, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE_EXCLUSIVE);
		page = BufferGetPage(buffer);

		if (offnum >= FirstOffsetNumber &&
			offnum <= PageGetMaxOffsetNumber(page))
		{
			itemid = PageGetItemId(page, offnum);

			if (ItemIdIsNormal(itemid) &&
				cas_new_size <= ItemIdGetLength(itemid))
			{
				Size		cas_target_size;

				old_tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

				/*
				 * WS-PVS1: the version-chain head lives in the fixed header
				 * field t_verptr, so both the on-page tuple and the freshly
				 * formed new tuple carry it inside their header with no
				 * trailing growth.  Same-column-width updates are therefore
				 * naturally the same length, so this stays on the CAS fast
				 * path with no first-time growth dispatch.
				 */
				cas_target_size = cas_new_size;

				/*
				 * Cheap in-page eligibility gate: committed, not deleted, not
				 * locked, no overflow, same size for direct memcpy.  This is a
				 * pure buffer-domain check (no sLog probe).
				 *
				 * We deliberately do NOT probe the sLog for write-write
				 * conflicts here.  The flag gate is not a reliable conflict
				 * signal -- a committed in-place UPDATE rewinds t_commit_ts and
				 * clears FLUX_TUPLE_UNCOMMITTED, and a reader/third writer may
				 * clear a still-live flag as "stale" -- so any pre-lock sLog
				 * probe would have to be repeated authoritatively after we own
				 * t_writer anyway (state can change between the probe and the
				 * trylock).  The authoritative committed-update and
				 * in-progress-writer probes therefore run once, under the
				 * t_writer lock below; the pre-lock duplicates were redundant
				 * seqlock reads on the common no-conflict path.  Taking the
				 * trylock speculatively is harmless: no WAL or page change
				 * happens until revalidation passes, and a lost race just
				 * releases t_writer and falls to the exclusive path.
				 */
				if (!(old_tuple_hdr->t_flags & (FLUX_TUPLE_DELETED |
												FLUX_TUPLE_LOCKED |
												FLUX_TUPLE_UNCOMMITTED |
												FLUX_TUPLE_HAS_OVERFLOW |
												FLUX_TUPLE_SPECULATIVE)) &&
					cas_target_size == ItemIdGetLength(itemid))
				{
					uint32		expected = 0;

					if (FluxTupleWriterTryLock(old_tuple_hdr, &expected))
					{
						/*
						 * We now own this tuple exclusively via t_writer,
						 * which -- not the pre-lock probe -- is the real
						 * serialization point.  Between our pre-lock
						 * eligibility check and acquiring t_writer, a
						 * competing CAS writer may have completed its own
						 * in-place overwrite (it stamps
						 * FLUX_TUPLE_UNCOMMITTED into the new page image and
						 * resets on-page t_writer to 0, which lets our CAS
						 * succeed), or committed and left a conflict marker.
						 * Acting on the stale pre-lock decision would
						 * silently clobber that update (lost update).
						 * Re-validate the flags and the write-write probe
						 * under the lock; if the tuple is disqualified now,
						 * release t_writer and fall through to the exclusive
						 * path, which blocks on the in-progress writer or
						 * returns TM_Updated for EPQ.
						 */
						bool		cas_revalidated;

						cas_revalidated =
							!(old_tuple_hdr->t_flags & (FLUX_TUPLE_DELETED |
														FLUX_TUPLE_LOCKED |
														FLUX_TUPLE_UNCOMMITTED |
														FLUX_TUPLE_HAS_OVERFLOW |
														FLUX_TUPLE_SPECULATIVE)) &&
							cas_target_size == ItemIdGetLength(itemid);
						if (cas_revalidated &&
							snapshot != NULL && IsMVCCSnapshot(snapshot))
						{
							RelUndoRecPtr head_verptr;
							TransactionId head_xid;
							bool		head_inprogress = false;

							if (FluxTupleHasCommittedUpdateAfter(relation,
																  old_tuple_hdr,
																  ItemIdGetLength(itemid),
																  snapshot,
																  GetCurrentTransactionIdIfAny(),
																  &head_verptr,
																  &head_xid,
																  &head_inprogress) &&
								!FluxEpqReconcileMatches(snapshot,
														  RelationGetRelid(relation),
														  otid,
														  head_verptr,
														  head_xid))
								cas_revalidated = false;
							/*
							 * Commit-window conflict: the head committer is between PRE_COMMIT
							 * (marker + UNCOMMITTED cleared) and CLOG commit.  Its sLog marker
							 * is gone, so the dirty-xid probe below cannot see it.  Abandon
							 * the CAS fast path and fall to the slow path, which waits on the
							 * in-flight writer; otherwise the just-committed update is lost.
							 */
							else if (head_inprogress)
								cas_revalidated = false;
						}

						/*
						 * Re-check for a concurrent in-progress writer (see
						 * above)
						 */
						if (cas_revalidated)
						{
							bool		reval_is_insert = false;
							TransactionId reval_dirty_xid =
								SLogTupleGetDirtyXid(RelationGetRelid(relation),
													 otid, &reval_is_insert);

							if (TransactionIdIsValid(reval_dirty_xid) &&
								!reval_is_insert)
								cas_revalidated = false;
						}

						if (!cas_revalidated)
						{
							/*
							 * Lost the race: release and fall to exclusive
							 * path
							 */
							FluxTupleWriterUnlock(old_tuple_hdr);
						}
						else
						{
							/*
							 * Fairness gate: funnel the CAS writer through
							 * LOCKTAG_TUPLE so lockers (flux_tuple_lock) and
							 * slow-path writers already queued on the same
							 * tag get FIFO service.  Without this, a stream
							 * of CAS writers can lap a locker sleeping in
							 * XactLockTableWait under FluxLockTuple,
							 * starving it past statement_timeout.  Must be
							 * dontWait: t_writer is held here, and blocking
							 * on a heavyweight sleep under a spinlock would
							 * deadlock.  If not granted, drop t_writer and
							 * fall through to the exclusive path, which
							 * takes LOCKTAG_TUPLE with wait=true and queues
							 * FIFO behind the waiter that beat us here.
							 */
							bool		cas_have_tuple_lock = false;

							if (!FluxLockTuple(relation, otid,
												LockTupleNoKeyExclusive,
												false, &cas_have_tuple_lock))
							{
								/*
								 * CANDIDATE A (surgical fell-through): a locker
								 * or updater is queued on LOCKTAG_TUPLE for this
								 * tid.  Drop t_writer and the page lock, then
								 * BLOCK on LOCKTAG_TUPLE so this writer queues
								 * FIFO behind the existing waiter.  Only after we
								 * acquire the tag (the locker ahead of us has
								 * been served) do we re-enter the update path via
								 * the slow route, which re-reads the page and
								 * re-classifies.  We must release t_writer and
								 * the buffer content lock BEFORE the blocking
								 * acquire: holding either across a heavyweight
								 * sleep would deadlock.  Set the OUTER
								 * have_tuple_lock so the slow path's
								 * dirty_xid-gated LockTuple sites (guarded by
								 * !have_tuple_lock) do not double-acquire and
								 * FLUX_RELEASE_TUPLOCK frees it on every exit.
								 */
								FluxTupleWriterUnlock(old_tuple_hdr);
								LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
								ReleaseBuffer(buffer);
								FluxFreeTuple(cas_new_tuple);
								if (!have_tuple_lock)
									FluxLockTuple(relation, otid,
												   LockTupleNoKeyExclusive,
												   true, &have_tuple_lock);
								goto flux_update_slow_path;
							}
							{
							/*
							 * We own this tuple exclusively under
							 * share-exclusive page lock.  No other writer can
							 * modify it until we release t_writer.
							 *
							 * Compute timestamps outside critical section to
							 * avoid memory allocation issues.
							 */
							uint64		cas_current_ts;
							uint16		cas_data_offset;
							uint16		cas_data_len;
							char	   *cas_old_bytes;
							char	   *cas_new_bytes;
							Size		cas_tuple_len;

							(void) FluxGetTransactionTimestamp();
							cas_current_ts = (uint64) FluxGetDmlTimestamp();

							/* Ensure we have an XID for WAL flush */
							(void) GetCurrentTransactionId();

							/*
							 * Heap-shaped: the in-place image is a FRESH version.
							 * Stamp t_xmax = Invalid (this is the newest, live
							 * version) and t_xmin = the updater below.  The
							 * pre-update image is preserved in the UNDO fork
							 * (the CAS path writes an undo record), so an older
							 * snapshot that cannot see the updater's xmin reads
							 * the before-image back via FluxReconstructVisible-
							 * Version.
							 *
							 * Torn-read note: the CAS path holds only
							 * BUFFER_LOCK_SHARE_EXCLUSIVE, so a concurrent SHARE
							 * reader can observe this image mid-overwrite.  It is
							 * marked UNCOMMITTED and its xmin is the still-
							 * in-progress updater, so FluxTupleSatisfiesMVCC
							 * hides it (XidInMVCCSnapshot(updater) == true) and
							 * the reader reconstructs the prior version -- the
							 * row never blinks out.
							 */
							cas_new_tuple->t_data->t_commit_ts = 0;
							cas_new_tuple->t_data->t_flags |= FLUX_TUPLE_UPDATED;

							/*
							 * Mark the in-place image UNCOMMITTED, mirroring
							 * the regular update path.  A second writer that
							 * hits this row sees UNCOMMITTED in the CAS
							 * eligibility gate, bails to the regular path,
							 * and blocks on the in-progress writer via
							 * XactLockTableWait -- without this flag the
							 * second CAS would silently overwrite an
							 * uncommitted update (lost update / no
							 * write-write blocking).  PRE_COMMIT clears the
							 * flag and stamps the real commit timestamp;
							 * readers self-heal a stale flag via the
							 * visibility path after a crash.
							 */
							cas_new_tuple->t_data->t_flags |= FLUX_TUPLE_UNCOMMITTED;
							/*
							 * Clear inherited CLOG hint bits: this image copied the
							 * old tuple's flags, but we are stamping a NEW t_xmin
							 * (the updater).  A stale XMIN_COMMITTED from the old
							 * inserter would wrongly assert the new xmin is committed.
							 */
							cas_new_tuple->t_data->t_flags &=
								~(FLUX_TUPLE_XMIN_COMMITTED | FLUX_TUPLE_XMAX_COMMITTED);
							cas_new_tuple->t_data->t_xmin = GetCurrentTransactionId();  /* subxid: heap-shaped, so savepoint rollback marks it aborted in CLOG */
							cas_new_tuple->t_data->t_writer = 0;	/* clear in new image */
							ItemPointerSet(&cas_new_tuple->t_data->t_ctid, blkno, offnum);

							/*
							 * Carry the index-identity generation forward from the
							 * t_gen is reserved/unused in FLUX (FLUX uses plain
							 * heap-TID index identity, no RowID/gen scheme).  Leave it
							 * at the palloc0 default of 0.
							 */
							cas_new_tuple->t_data->t_gen = 0;

							/*
							 * WS-PVS1: build the new-image buffer of
							 * cas_target_size (== cas_new_size; the
							 * version pointer is a header field, not a
							 * trailer).  We reserve the per-rel UNDO
							 * record first so its RelUndoRecPtr can be
							 * stamped into the new image BEFORE both the
							 * diff scan (so the diff includes the
							 * version-pointer change) and the page memcpy.
							 *
							 * RelUndoReserve may extend the fork and ereport
							 * -- neither is safe inside the critical
							 * section.  Lock order: data buffer then UNDO
							 * buffer.
							 */
							{
								uint32		cas_old_len = ItemIdGetLength(itemid);
								char	   *cas_old_copy = palloc(cas_old_len);
								char	   *cas_full_image;
								Buffer		cas_undo_buffer = InvalidBuffer;
								RelUndoRecPtr cas_undo_ptr = InvalidRelUndoRecPtr;
								RelUndoRecPtr cas_prev_undo = InvalidRelUndoRecPtr;
								Size		cas_undo_reserve;
								RelUndoStageResult cas_undo_staged;
								bool		cas_undo_staged_valid = false;

								memcpy(cas_old_copy, old_tuple_hdr, cas_old_len);

								/*
								 * Reserve worst-case UNDO size (full-tuple
								 * record) up front.  We commit to either
								 * delta or full-tuple AFTER stamping the
								 * urec_ptr into the new image and computing
								 * the final diff.  Over-reservation by a
								 * few bytes is benign -- the unused tail of
								 * the reserved space is wasted on that
								 * page, no worse than a SHRINK update.
								 */
								cas_undo_reserve = SizeOfRelUndoRecordHeader +
									sizeof(RelUndoUpdatePayload) + cas_old_len;
								if (smgrexists(RelationGetSmgr(relation), RELUNDO_FORKNUM))
								{
									cas_prev_undo =
										GetPerRelUndoPtr(RelationGetRelid(relation));
									cas_undo_ptr = RelUndoReserve(relation,
																  cas_undo_reserve,
																  &cas_undo_buffer);
								}

								/*
								 * Assemble the on-page image: header+data
								 * from the freshly formed new tuple plus
								 * the trailing version-pointer.  Every
								 * committed in-place UPDATE stamps the
								 * verptr, so the size check above forces
								 * this same-shape requirement; first-time
								 * stamping (item length grew from base to
								 * base+8) happens on the exclusive-lock
								 * path.  The trailing field is part of the
								 * on-disk slot, so the diff scan and the
								 * page memcpy both see this final layout.
								 * Stamp the freshly reserved cas_undo_ptr
								 * (NEVER preserve the prior pointer): the
								 * new diff reconstructs this update's
								 * before-image, and the header's
								 * urec_prevundorec already chains to the
								 * prior diff -- the row always points at
								 * the head of the chain.  If no UNDO fork
								 * exists (unlogged/temp), stamp
								 * InvalidRelUndoRecPtr to keep slot length
								 * stable; readers fall back to the on-page
								 * tuple when the head is invalid.
								 */
								cas_full_image = palloc(cas_target_size);
								memcpy(cas_full_image, cas_new_tuple->t_data, cas_new_size);
								{
									FluxTupleHeader *cas_full_hdr =
										(FluxTupleHeader *) cas_full_image;

									cas_full_hdr->t_flags |= FLUX_TUPLE_HAS_VERSION_PTR;
									FluxTupleSetVersionPtr(cas_full_hdr,
															cas_target_size,
															cas_undo_ptr);
								}

								/*
								 * Compute the diff region for WAL logging.
								 * We log only the bytes that actually
								 * changed within the unified image.  Read
								 * the old side from cas_old_copy (already
								 * memcpy'd from the page, identical bytes)
								 * rather than the live locked page: the
								 * scan is pure CPU work with no I/O, and
								 * reading a palloc'd copy instead of
								 * old_tuple_hdr means this loop no longer
								 * needs the data page to stay locked --
								 * cas_target_size == cas_old_len is
								 * guaranteed by the eligibility gate above
								 * (same-size CAS only), so cas_old_copy has
								 * exactly cas_tuple_len bytes available.
								 */
								cas_tuple_len = cas_target_size;
								cas_old_bytes = cas_old_copy;
								cas_new_bytes = cas_full_image;

								/* Find first differing byte */
								cas_data_offset = 0;
								while (cas_data_offset < cas_tuple_len &&
									   cas_old_bytes[cas_data_offset] == cas_new_bytes[cas_data_offset])
									cas_data_offset++;

								if (cas_data_offset < cas_tuple_len)
								{
									uint16		cas_end = (uint16) cas_tuple_len;

									/* Find last differing byte */
									while (cas_end > cas_data_offset &&
										   cas_old_bytes[cas_end - 1] == cas_new_bytes[cas_end - 1])
										cas_end--;

									cas_data_len = cas_end - cas_data_offset;
								}
								else
								{
									/*
									 * No actual data change -- release and
									 * return OK.  Cancel the UNDO
									 * reservation; we have not written
									 * anything to the page or the UNDO
									 * fork.
									 */
									if (RelUndoRecPtrIsValid(cas_undo_ptr))
										RelUndoCancel(relation, cas_undo_buffer, cas_undo_ptr);
									pfree(cas_full_image);
									pfree(cas_old_copy);
									FluxTupleWriterUnlock(old_tuple_hdr);
									LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
									ReleaseBuffer(buffer);
									FluxFreeTuple(cas_new_tuple);

									/* Set output TID */
									ItemPointerSet(&slot->tts_tid, blkno, offnum);
									slot->tts_tableOid = RelationGetRelid(relation);
									if (update_indexes)
										*update_indexes = TU_None;
									pgstat_count_heap_update(relation, false, false);
									FluxUnlockTuple(relation, otid,
													 LockTupleNoKeyExclusive);
									return TM_Ok;
								}

								/* SSI conflict check */
								CheckForSerializableConflictIn(relation, otid,
															   BufferGetBlockNumber(buffer));

								/*
								 * FOLD variant: stage the UNDO before-image
								 * BEFORE the critical section so the combined
								 * WAL record can carry both the main-fork redo
								 * byte-diff and the undo before-image in one
								 * XLogInsert (halving per-UPDATE WAL insert
								 * count vs the standalone RM_RELUNDO record).
								 *
								 * RelUndoReserve() already returned the undo
								 * buffer exclusively locked+pinned, and (for a
								 * new page) left the metapage locked; both stay
								 * held through the crit section below, so one
								 * XLogInsert may legally register buffers from
								 * both forks.  RelUndoStage() writes+dirties the
								 * undo page here (no WAL); the crit section emits
								 * the fold record and PageSetLSNs both pages.
								 *
								 * The tid the payloads reference is fully known
								 * now (blkno/offnum), so set it before staging.
								 */
								ItemPointerSet(&slot->tts_tid, blkno, offnum);
								slot->tts_tableOid = RelationGetRelid(relation);
								if (RelUndoRecPtrIsValid(cas_undo_ptr))
								{
									RelUndoRecordHeader cas_undo_hdr;
									char	   *cas_combined;
									Size		cas_payload_total;
									RelUndoUpdatePayload cas_undo_payload;

									cas_undo_payload.oldtid = slot->tts_tid;
									cas_undo_payload.newtid = slot->tts_tid;
									cas_undo_hdr.urec_xid = GetCurrentTransactionId();
									cas_undo_hdr.urec_prevundorec = cas_prev_undo;
									cas_undo_hdr.urec_type = RELUNDO_UPDATE;

									/*
									 * Full old-tuple before-image, always: FLUX matches ZHeap
									 * (whole old tuple to UNDO, no byte-diff/delta).
									 * [header][update-payload][old tuple].
									 */
									cas_undo_hdr.urec_len = (uint16)
										(SizeOfRelUndoRecordHeader +
										 sizeof(RelUndoUpdatePayload) + cas_old_len);
									cas_undo_hdr.info_flags = RELUNDO_INFO_HAS_TUPLE;
									cas_undo_hdr.tuple_len = (uint16) cas_old_len;

									cas_payload_total = sizeof(RelUndoUpdatePayload) +
										cas_old_len;
									cas_combined = palloc(cas_payload_total);
									memcpy(cas_combined, &cas_undo_payload,
										   sizeof(RelUndoUpdatePayload));
									memcpy(cas_combined + sizeof(RelUndoUpdatePayload),
										   cas_old_copy, cas_old_len);

									RelUndoStage(relation, cas_undo_buffer, cas_undo_ptr,
												 &cas_undo_hdr, cas_combined,
												 cas_payload_total, &cas_undo_staged);
									cas_undo_staged_valid = true;
									pfree(cas_combined);
								}

								/* Critical section: modify page + WAL */
								START_CRIT_SECTION();

								memcpy(old_tuple_hdr, cas_full_image, cas_target_size);

								/*
								 * Update page-level commit timestamp
								 * atomically
								 */
								{
									FluxPageOpaque cas_opaque = FluxPageGetOpaque(page);
									uint64		cas_old_ts_flags;
									uint64		cas_new_ts_flags;
									uint64		cur_ts;

									do
									{
										cas_old_ts_flags = cas_opaque->pd_commit_ts_and_flags;
										cur_ts = cas_old_ts_flags & FLUX_PAGE_TS_MASK;

										if (cas_current_ts <= cur_ts)
											break;
										cas_new_ts_flags = (cas_old_ts_flags & FLUX_PAGE_FLAG_MASK) |
											(cas_current_ts & FLUX_PAGE_TS_MASK);
									} while (!pg_atomic_compare_exchange_u64(
																			 (pg_atomic_uint64 *) &cas_opaque->pd_commit_ts_and_flags,
																			 &cas_old_ts_flags, cas_new_ts_flags));
								}

								MarkBufferDirtyShared(buffer);

								/*
								 * WAL log.  When we staged an UNDO before-image,
								 * emit the single combined fold record carrying
								 * both the main-fork redo diff and the undo
								 * bytes; it PageSetLSNs both pages (and the
								 * metapage on a new undo page).  Otherwise
								 * (no UNDO fork) emit the plain redo record.
								 */
								if (cas_undo_staged_valid)
								{
									FluxXLogCasUpdateUndo(relation, buffer, offnum,
														   cas_data_offset, cas_data_len,
														   cas_new_bytes + cas_data_offset,
														   0,	/* new version: t_xmax = Invalid */
														   &cas_undo_staged);
								}
								else if (RelationNeedsWAL(relation))
								{
									FluxXLogCasUpdate(relation, buffer, offnum,
													   cas_data_offset, cas_data_len,
													   cas_new_bytes + cas_data_offset,
													   0);	/* new version: t_xmax = Invalid */
								}

								END_CRIT_SECTION();

								/*
								 * Release the staged UNDO buffers now that the
								 * fold record has logged them (mirrors the
								 * release RelUndoFinish would have done), and
								 * register the record for rollback discovery.
								 */
								if (cas_undo_staged_valid)
								{
									RegisterPerRelUndo(RelationGetRelid(relation),
													   cas_undo_ptr);
									pfree(cas_undo_staged.wal_record_data);
									UnlockReleaseBuffer(cas_undo_staged.undo_buffer);
									if (BufferIsValid(cas_undo_staged.metabuf))
										UnlockReleaseBuffer(cas_undo_staged.metabuf);
								}

								/* Release tuple-level CAS lock */
								FluxTupleWriterUnlock(old_tuple_hdr);

								/*
								 * sLog registration BEFORE buffer release.
								 * Eliminates the race window where another
								 * backend reads the modified tuple but finds
								 * no sLog entry (causing visibility failures
								 * at high concurrency).  Safe: seqlock reads
								 * are wait-free, no deadlock with buffer
								 * lock.
								 */
								FluxEnsureSLogCallbacks();
								SLogTupleInsert(RelationGetRelid(relation),
												&slot->tts_tid,
												GetTopTransactionId(),
												SLOG_OP_UPDATE,
												GetCurrentSubTransactionId(),
												cid, cas_current_ts, 0,
												LockTupleNoKeyExclusive);

								/* Store before-image for rollback */
								SLogTupleStoreBeforeImage(
														  RelationGetRelid(relation),
														  &slot->tts_tid,
														  GetTopTransactionId(),
														  cas_old_copy, cas_old_len,
														  ((FluxTupleHeader *) cas_old_copy)->t_flags,
														  ((FluxTupleHeader *) cas_old_copy)->t_commit_ts,
														  relation->rd_locator,
														  relation->rd_rel->relpersistence);

								LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

								/*
								 * No index maintenance for the CAS in-place path.  A
								 * CAS update is same-size and only reaches here when no
								 * indexed column changed (key changes are routed out of
								 * place at the top of flux_tuple_update, TU_All).  The TID
								 * is unchanged, so every (key, TID) entry still points at
								 * the live tuple; *update_indexes is set TU_None below.
								 */

								pfree(cas_old_copy);
								pfree(cas_full_image);

								/*
								 * Clear VM all-visible/all-frozen bits.
								 */
								FluxVMClear(relation, blkno, buffer, FLUX_VM_VALID_BITS);

								ReleaseBuffer(buffer);

								/* Track in-place update */
								flux_stat_in_place_updates++;

								/* Mark this block dirty for the scan-path sLog bypass */
								FluxDirtyMapMark(RelationGetRelid(relation), blkno);

								FluxFreeTuple(cas_new_tuple);

								/*
								 * Drive retained-marker cleanup now that all
								 * buffer locks are released.  Throttled
								 * internally; never runs the global sweep under
								 * a page lock (which would convoy all writers
								 * to this hot row).
								 */
								SLogTupleMaybeCleanupRetained();

								/*
								 * Same reasoning for the per-relation UNDO fork:
								 * this in-place AM can correctly report near-zero
								 * dead tuples, so autovacuum may never launch and
								 * RelUndoVacuum() (VACUUM-only) may never run.
								 * RelUndoMaybeVacuum() is the throttled backstop.
								 */
								RelUndoMaybeVacuum(relation);
								pgstat_count_heap_update(relation, false, false);
								FluxUnlockTuple(relation, otid,
												 LockTupleNoKeyExclusive);
								return TM_Ok;
							}	/* end block (tuple-lock granted) */
							}	/* end else (revalidated) */
						}
					}
					/* CAS failed -- another writer has this tuple */
				}
				/* Not eligible for CAS fast path */
			}
			/* ItemId not normal or new tuple too large */
		}
		/* Offset out of range */

		/* Release shared lock, fall through to exclusive path */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);
		FluxFreeTuple(cas_new_tuple);
	}

	/*
	 * Lock the buffer exclusively.  The exclusive buffer lock prevents
	 * concurrent modification while we hold it, but it is dropped while we
	 * XactLockTableWait on a conflicting in-progress writer (the wait sites
	 * below).  To stop two backends that each see the other's in-progress
	 * write from mutually waiting and deadlocking, an updater first takes a
	 * heavyweight tuple lock (have_tuple_lock) that serializes them into a
	 * FIFO queue, held continuously through the recheck/update and released
	 * on every exit via FLUX_RELEASE_TUPLOCK().  This matches heap_update.
	 *
	 * CANDIDATE A: a CAS writer that found a locker queued on the tag jumps
	 * here (flux_update_slow_path) already holding have_tuple_lock, so do
	 * NOT reset it -- clobbering it would drop the lock we just queued for.
	 * The buffer was released before the goto, so the re-read below re-pins
	 * it; the label must precede ReadBuffer.
	 */
flux_update_slow_path:
	/* Read the page containing the old tuple */
	buffer = ReadBuffer(relation, blkno);
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

	old_tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

	/* Check if old tuple has overflow chains to clean up later */
	old_has_overflow = (old_tuple_hdr->t_flags & FLUX_TUPLE_HAS_OVERFLOW) != 0;

	/*
	 * Check if tuple is already deleted.  As in the delete path, a DELETED
	 * flag may reflect an in-progress delete by a concurrent transaction
	 * (which also sets UNCOMMITTED).  If so, wait for that transaction and
	 * retry rather than reporting TM_Deleted immediately -- this matches
	 * heap, where an UPDATE blocks behind a concurrent in-progress DELETE.
	 */
	if (old_tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
	{
		TransactionId del_xid = InvalidTransactionId;
		bool		del_is_insert = false;

		if (old_tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED)
			del_xid = SLogTupleGetWriteConflictXid(RelationGetRelid(relation),
												   otid,
												   LockTupleNoKeyExclusive,
												   &del_is_insert);

		if (wait && TransactionIdIsValid(del_xid) &&
			!TransactionIdIsCurrentTransactionId(del_xid) &&
			!del_is_insert)
		{
			TransactionId wait_xid = del_xid;

			/*
			 * Acquire a heavyweight tuple lock before sleeping on the
			 * conflicting xid.  This serializes competing updaters of the
			 * same tuple into a FIFO queue; without it, two backends that
			 * each see the other's in-progress write would mutually
			 * XactLockTableWait and deadlock.  Matches heap_update.
			 */
			UnlockReleaseBuffer(buffer);
			if (!have_tuple_lock)
				FluxLockTuple(relation, otid, LockTupleNoKeyExclusive,
							   true, &have_tuple_lock);
			XactLockTableWait(wait_xid, relation, otid, XLTW_Update);

			buffer = ReadBuffer(relation, blkno);
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buffer);

			if (offnum < FirstOffsetNumber ||
				offnum > PageGetMaxOffsetNumber(page))
			{
				UnlockReleaseBuffer(buffer);
				FLUX_RELEASE_TUPLOCK();
				return TM_Invisible;
			}
			itemid = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(itemid))
			{
				UnlockReleaseBuffer(buffer);
				FLUX_RELEASE_TUPLOCK();
				return TM_Invisible;
			}
			old_tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

			/*
			 * Delete committed -> tombstone persists, report TM_Deleted for
			 * EPQ. Delete aborted -> before-image restored (DELETED cleared),
			 * fall through to perform the update.
			 */
			if (old_tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
			{
				if (tmfd)
				{
					tmfd->ctid = *otid;
					tmfd->xmax = wait_xid;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				FLUX_RELEASE_TUPLOCK();
				return TM_Deleted;
			}
		}
		else
		{
			if (tmfd)
			{
				tmfd->ctid = *otid;
				tmfd->xmax = GetCurrentTransactionId();
				tmfd->cmax = InvalidCommandId;
				tmfd->traversed = false;
			}
			UnlockReleaseBuffer(buffer);
			FLUX_RELEASE_TUPLOCK();
			return TM_Deleted;
		}
	}

	/*
	 * Handle LOCKED flag: if this tuple is locked by the current transaction
	 * (FOR SHARE/FOR KEY SHARE/FOR UPDATE), the lock is compatible with
	 * UPDATE (self-lock).  Clear the LOCKED flag since we're about to modify
	 * the tuple.  The sLog LOCK entry will be overwritten by the UPDATE entry
	 * or cleaned up at commit.
	 */
	if (old_tuple_hdr->t_flags & FLUX_TUPLE_LOCKED)
	{
		SLogTupleOp lock_entry;
		int			nfound;

		nfound = SLogTupleLookupFiltered(RelationGetRelid(relation), otid,
										 GetCurrentTransactionId(), &lock_entry, 1);
		if (nfound > 0 &&
			(lock_entry.op_type == SLOG_OP_LOCK_SHARE ||
			 lock_entry.op_type == SLOG_OP_LOCK_EXCL))
		{
			/* Our own lock - clear flag and proceed with update */
			old_tuple_hdr->t_flags &= ~FLUX_TUPLE_LOCKED;
		}

		/*
		 * If it's another transaction's lock, the existing concurrency
		 * control handles waiting via SLogTupleGetDirtyXid.
		 */
	}

	/*
	 * Fast-path: if UNCOMMITTED is set but no sLog entry exists, the previous
	 * transaction committed and its sLog cleanup already ran. Clear the stale
	 * flag now while we hold the buffer lock exclusively. This avoids the
	 * expensive sLog lookup inside the visibility check for the common case
	 * of UPDATing a recently-committed tuple.
	 *
	 * Only do this for tuples that are NOT deleted/updated (those flags
	 * indicate the tuple is being superseded, which requires the full
	 * visibility check to determine if the delete/update committed).
	 */
	if ((old_tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED) &&
		!(old_tuple_hdr->t_flags & (FLUX_TUPLE_DELETED | FLUX_TUPLE_UPDATED)))
	{
		if (!SLogTupleHasEntry(RelationGetRelid(relation), otid))
		{
			old_tuple_hdr->t_flags &= ~FLUX_TUPLE_UNCOMMITTED;
			/* Page will be dirtied by our upcoming update anyway */
		}
	}

	/*
	 * Check tuple visibility against snapshot and handle concurrent
	 * modifications.  Unlike a simple scan visibility check, UPDATE must
	 * distinguish between: - Truly invisible (another txn's uncommitted
	 * insert) → TM_Invisible - Concurrent update committed after our
	 * snapshot    → TM_Updated - In-progress modification by another txn           →
	 * wait, retry
	 */
	if (snapshot)
	{
		bool		visible;

		visible = FluxTupleVisibleToSnapshotDual(old_tuple_hdr, snapshot,
												  RelationGetRelid(relation),
												  buffer);

		if (!visible)
		{
			TransactionId dirty_xid;
			bool		is_insert_entry;

			/*
			 * Writer-only probe (seqlock read).  We must wait only
			 * on an in-progress INSERT/UPDATE/DELETE writer, never on a pure
			 * lock-only marker: a KeyShare locker (AccessShareLock tuplock) is
			 * compatible with our NoKeyExclusive update (ExclusiveLock tuplock)
			 * in the standard tuple-lock matrix, so blocking on its xid here --
			 * while it is queued behind us reporting a lock conflict -- forms a
			 * mutual XactLockTableWait cycle that heap avoids via
			 * HEAP_XMAX_IS_LOCKED_ONLY.  Real lock conflicts (Share/Exclusive
			 * lockers) are still serialized by the heavyweight tuplock below.
			 */
			dirty_xid = SLogTupleGetWriteConflictXid(RelationGetRelid(relation),
													 otid,
													 LockTupleNoKeyExclusive,
													 &is_insert_entry);

			/* Check if tuple was deleted by another transaction */
			if (old_tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
			{
				if (tmfd)
				{
					tmfd->ctid = *otid;
					tmfd->xmax = TransactionIdIsValid(dirty_xid) ?
						dirty_xid : GetCurrentTransactionId();
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				FLUX_RELEASE_TUPLOCK();
				return TM_Deleted;
			}

			/*
			 * Buffer lock was never released (wait-free sLog read), so the
			 * tuple cannot have changed.  Proceed with dirty_xid.
			 */
			{
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
						tmfd->cmax = InvalidCommandId;
						tmfd->traversed = false;
					}
					UnlockReleaseBuffer(buffer);
					FLUX_RELEASE_TUPLOCK();
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

						/*
						 * Serialize competing updaters via a heavyweight
						 * tuple lock before sleeping on the conflicting xid,
						 * so two backends racing on the same tuple queue
						 * instead of deadlocking.  Matches heap_update.
						 */
						UnlockReleaseBuffer(buffer);
						if (!have_tuple_lock)
							FluxLockTuple(relation, otid, LockTupleNoKeyExclusive,
										   true, &have_tuple_lock);
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
							FLUX_RELEASE_TUPLOCK();
							return TM_Invisible;
						}
						itemid = PageGetItemId(page, offnum);
						if (!ItemIdIsNormal(itemid))
						{
							UnlockReleaseBuffer(buffer);
							FLUX_RELEASE_TUPLOCK();
							return TM_Invisible;
						}
						old_tuple_hdr = (FluxTupleHeader *)
							PageGetItem(page, itemid);

						/* If it got deleted while we waited, report that */
						if (old_tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
						{
							if (tmfd)
							{
								tmfd->ctid = *otid;
								tmfd->xmax = wait_xid;
								tmfd->cmax = InvalidCommandId;
								tmfd->traversed = false;
							}
							UnlockReleaseBuffer(buffer);
							FLUX_RELEASE_TUPLOCK();
							return TM_Deleted;
						}

						/*
						 * Re-check visibility.  The tuple was modified by the
						 * now-committed txn; its commit_ts is now later than
						 * our snapshot -> TM_Updated so the executor can EPQ.
						 */
						visible = FluxTupleVisibleToSnapshotDual(old_tuple_hdr, snapshot,
																  RelationGetRelid(relation),
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
							 * 1. We return TM_Updated → executor EPQ 2.
							 * table_tuple_lock inserts LOCK_EXCL 3. Retry →
							 * another txn is in-progress → wait 4. Waited
							 * txn commits → still not visible 5. Return
							 * TM_Updated → goto 2 (infinite)
							 *
							 * Each iteration leaks per-query memory in the
							 * executor, eventually causing OOM.
							 */
							TransactionId myxid_postw =
								GetCurrentTransactionIdIfAny();

							if (TransactionIdIsValid(myxid_postw))
							{
								SLogTupleOp my_entry_postw;
								int			my_nfound_postw;

								my_nfound_postw = SLogTupleLookupFiltered(
																		  RelationGetRelid(relation),
																		  otid, myxid_postw,
																		  &my_entry_postw, 1);

								if (my_nfound_postw > 0)
								{
									/*
									 * Our LOCK entry from a prior EPQ cycle
									 * exists.  Fall through to perform the
									 * update.
									 */
								}
								else
								{
									if (tmfd)
									{
										tmfd->ctid = *otid;
										tmfd->xmax = wait_xid;
										tmfd->cmax = InvalidCommandId;
										tmfd->traversed = false;
									}
									UnlockReleaseBuffer(buffer);
									FLUX_RELEASE_TUPLOCK();
									return TM_Updated;
								}
							}
							else
							{
								if (tmfd)
								{
									tmfd->ctid = *otid;
									tmfd->xmax = wait_xid;
									tmfd->cmax = InvalidCommandId;
									tmfd->traversed = false;
								}
								UnlockReleaseBuffer(buffer);
								FLUX_RELEASE_TUPLOCK();
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
							tmfd->cmax = InvalidCommandId;
							tmfd->traversed = false;
						}
						UnlockReleaseBuffer(buffer);
						FLUX_RELEASE_TUPLOCK();
						return TM_WouldBlock;
					}
				}
				else
				{
					/*
					 * No in-progress sLog entry for another transaction. The
					 * modification has already committed.
					 *
					 * Check if our own transaction already has a sLog entry
					 * for this TID (e.g., LOCK_EXCL placed by
					 * table_tuple_lock during EvalPlanQual).  If so, EPQ
					 * already re-evaluated the WHERE clause and we should
					 * proceed with the update.
					 *
					 * Without this, we return TM_Updated endlessly: FLUX's
					 * in-place updates mean the tuple's commit_ts permanently
					 * exceeds the statement snapshot, so the executor's EPQ
					 * retry loop never terminates.
					 */
					TransactionId myxid_chk =
						GetCurrentTransactionIdIfAny();

					if (TransactionIdIsValid(myxid_chk))
					{
						SLogTupleOp my_entry;
						int			my_nfound;

						my_nfound = SLogTupleLookupFiltered(
															RelationGetRelid(relation),
															otid, myxid_chk, &my_entry, 1);
						if (my_nfound > 0)
						{
							/*
							 * Our own sLog entry exists (LOCK from EPQ path).
							 * Fall through to perform the update.
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
								tmfd->cmax = InvalidCommandId;
								tmfd->traversed = false;
							}
							UnlockReleaseBuffer(buffer);
							FLUX_RELEASE_TUPLOCK();
							return TM_Updated;
						}
					}
					else
					{
						if (tmfd)
						{
							tmfd->ctid = *otid;
							tmfd->xmax = InvalidTransactionId;
							tmfd->cmax = InvalidCommandId;
							tmfd->traversed = false;
						}
						UnlockReleaseBuffer(buffer);
						FLUX_RELEASE_TUPLOCK();
						return TM_Updated;
					}
				}
			}
			/* If now visible, fall through to perform the update */
		}

		/*
		 * Write-write conflict against an ALREADY-COMMITTED concurrent
		 * update.
		 *
		 * The visibility check above always returns "visible" for a committed
		 * in-place update, because at commit time t_commit_ts is rewound to
		 * the original insert timestamp (so mid-life readers still see the
		 * row). That rewind erases the "updated since you read it" signal
		 * heap keeps in xmax, so visibility alone can never detect a lost
		 * update.  Instead, the on-page tuple's trailing verptr (WS-PVS1)
		 * points at the UNDO-fork record produced by the last committed
		 * update; if that committer is invisible to our snapshot, taking the
		 * in-place UPDATE now would silently lose it.  Return TM_Updated so
		 * the executor re-evaluates via EvalPlanQual, matching heap.
		 *
		 * Converge like heap: bounce to EPQ once per DISTINCT committed
		 * update, not once per probe.  FluxEpqReconcileMatches suppresses
		 * re-firing on the identical (verptr, xid) marker we already
		 * bounced on this statement; FluxEpqReconcileMark records the
		 * marker before we return.  A strictly-newer committer stamps a
		 * fresh verptr, so identity dedup only suppresses the same marker
		 * and never masks a genuine new conflict.
		 */
		if (IsMVCCSnapshot(snapshot))
		{
			RelUndoRecPtr head_verptr;
			TransactionId head_xid;
			bool		head_inprogress = false;

			if (FluxTupleHasCommittedUpdateAfter(relation,
												  old_tuple_hdr,
												  ItemIdGetLength(itemid),
												  snapshot,
												  GetCurrentTransactionIdIfAny(),
												  &head_verptr,
												  &head_xid,
												  &head_inprogress) &&
				!FluxEpqReconcileMatches(snapshot,
										  RelationGetRelid(relation),
										  otid, head_verptr, head_xid))
			{
				FluxEpqReconcileMark(snapshot,
									  RelationGetRelid(relation), otid,
									  head_verptr, head_xid);
				if (tmfd)
				{
					tmfd->ctid = *otid;
					tmfd->xmax = InvalidTransactionId;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				FLUX_RELEASE_TUPLOCK();
				return TM_Updated;
			}
			else if (head_inprogress)
			{
				/*
				 * Commit-window conflict (see FluxTupleHasCommittedUpdateAfter):
				 * the head committer cleared its sLog marker + UNCOMMITTED flag
				 * at PRE_COMMIT but has not yet reached CLOG.  Neither this gate
				 * nor the sLog dirty-xid probe sees it, so we would clobber a
				 * just-committed update.  Wait on the in-flight writer, then retry
				 * from the top of the slow path where CLOG now resolves it to a
				 * committed conflict -> TM_Updated -> EPQ.  Heap-identical.
				 */
				if (wait)
				{
					TransactionId wait_xid = head_xid;

					/*
					 * Wait for the in-flight committer to reach CLOG or abort, then
					 * report TM_Updated so the executor re-fetches through
					 * EvalPlanQual.  EPQ's SnapshotAny fetch reconstructs the correct
					 * visible version (the committed value if head_xid committed, or
					 * the restored before-image if it aborted) and re-projects the
					 * update on top.  A blind update retry here would instead apply
					 * over the raw on-page bytes, which on abort are the
					 * not-yet-reverted value.
					 */
					UnlockReleaseBuffer(buffer);
					if (!have_tuple_lock)
						FluxLockTuple(relation, otid, LockTupleNoKeyExclusive,
									   true, &have_tuple_lock);
					XactLockTableWait(wait_xid, relation, otid, XLTW_Update);
					if (tmfd)
					{
						tmfd->ctid = *otid;
						tmfd->xmax = InvalidTransactionId;
						tmfd->cmax = InvalidCommandId;
						tmfd->traversed = false;
					}
					FLUX_RELEASE_TUPLOCK();
					return TM_Updated;
				}
			}
		}
	}

	/*
	 * Even when visibility returned "true", the tuple may have an in-progress
	 * modification by another transaction.  This happens when
	 * FluxTupleVisibleToSnapshotDual returns true for in-progress
	 * UPDATE/DELETE entries (to preserve tuple existence in scans).  We must
	 * still detect the write-write conflict and block.
	 *
	 * The in-progress writer is detected authoritatively via the sLog
	 * (SLogTupleGetDirtyXid filters by TransactionIdIsInProgress), NOT via
	 * the on-page FLUX_TUPLE_UNCOMMITTED flag.  The page flag and the sLog
	 * marker live in two domains (buffer locks vs. the sLog seqlock) that are not
	 * updated atomically, so a live writer's marker can exist while the page
	 * flag is transiently clear.  Gating this wait on the flag (as we once
	 * did) let such a writer slip past, silently clobbering its in-progress
	 * update. Consult the sLog unconditionally; only use the flag to
	 * opportunistically clear stale state when no writer is present.
	 */
	{
		TransactionId dirty_xid;
		bool		is_insert_entry;

		/*
		 * Lock-free.  Probe for a transaction that conflicts with our
		 * NoKeyExclusive update under the real heavyweight tuple-lock matrix:
		 * an in-progress INSERT/UPDATE/DELETE writer always conflicts, and a
		 * lock-only marker conflicts iff its recorded LockTupleMode does (FOR
		 * UPDATE/FOR SHARE block; a KeyShare FK locker is compatible and does
		 * not).  A pure writer-only probe would sail past a FOR UPDATE locker
		 * that left only a LOCK_EXCL marker and clobber the row it protects --
		 * a correctness failure.  We then acquire the same heavyweight
		 * LOCKTAG_TUPLE lock and XactLockTableWait on the conflicting xid,
		 * which serializes us into a FIFO queue rather than deadlocking.
		 */
		dirty_xid = SLogTupleGetWriteConflictXid(RelationGetRelid(relation),
												 otid,
												 LockTupleNoKeyExclusive,
												 &is_insert_entry);

		if (!TransactionIdIsValid(dirty_xid))
		{
			/*
			 * No active writer.  If the page still carries a stale
			 * UNCOMMITTED flag, clear it opportunistically.
			 */
			if (old_tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED)
			{
				old_tuple_hdr->t_flags &= ~FLUX_TUPLE_UNCOMMITTED;
				MarkBufferDirty(buffer);
			}
		}
		else if (TransactionIdIsValid(dirty_xid) &&
				 !TransactionIdIsCurrentTransactionId(dirty_xid) &&
				 !is_insert_entry)
		{
			/*
			 * Another transaction has an in-progress UPDATE/DELETE. Block
			 * until it finishes, then re-check.
			 */
			if (wait)
			{
				TransactionId wait_xid = dirty_xid;

				/*
				 * Serialize competing updaters via a heavyweight tuple lock
				 * before sleeping on the conflicting xid, so two backends
				 * racing on the same tuple queue instead of deadlocking.
				 * Matches heap_update.
				 */
				UnlockReleaseBuffer(buffer);
				if (!have_tuple_lock)
					FluxLockTuple(relation, otid, LockTupleNoKeyExclusive,
								   true, &have_tuple_lock);
				XactLockTableWait(wait_xid, relation, otid, XLTW_Update);

				/* Re-read page after waking */
				buffer = ReadBuffer(relation, blkno);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				page = BufferGetPage(buffer);

				if (offnum < FirstOffsetNumber ||
					offnum > PageGetMaxOffsetNumber(page))
				{
					UnlockReleaseBuffer(buffer);
					FLUX_RELEASE_TUPLOCK();
					return TM_Invisible;
				}
				itemid = PageGetItemId(page, offnum);
				if (!ItemIdIsNormal(itemid))
				{
					UnlockReleaseBuffer(buffer);
					FLUX_RELEASE_TUPLOCK();
					return TM_Invisible;
				}
				old_tuple_hdr = (FluxTupleHeader *)
					PageGetItem(page, itemid);

				/* If deleted while we waited, report that */
				if (old_tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
				{
					if (tmfd)
					{
						tmfd->ctid = *otid;
						tmfd->xmax = wait_xid;
						tmfd->cmax = InvalidCommandId;
						tmfd->traversed = false;
					}
					UnlockReleaseBuffer(buffer);
					FLUX_RELEASE_TUPLOCK();
					return TM_Deleted;
				}

				/*
				 * The waited-on txn finished (committed or aborted).  Return
				 * TM_Updated to force the executor to re-evaluate via EPQ
				 * against the now-stable page.
				 *
				 * This is required even on ABORT, and is where FLUX differs
				 * from heap.  FLUX updates in place, so a READ COMMITTED
				 * scan that ran while the other writer was in progress
				 * dirty-read that writer's uncommitted value as the base for
				 * the new tuple's expression (e.g. counter = counter + 10
				 * computed off the in-flight value).  Heap never sees this
				 * because its scan reads the prior committed version.  By the
				 * time we get here: - COMMIT: the page holds the other
				 * writer's committed value; EPQ recomputes on top of it. -
				 * ABORT: ApplyPerRelUndo() has already restored the
				 * pre-update image inline (before the conflicting XID left
				 * the proc array), so the page holds the original committed
				 * value; EPQ recomputes on top of that. Either way EPQ
				 * re-reads the correct base and recomputes, matching heap's
				 * final result.
				 */
				if (tmfd)
				{
					tmfd->ctid = *otid;
					tmfd->xmax = wait_xid;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				FLUX_RELEASE_TUPLOCK();
				return TM_Updated;
			}
			else
			{
				/* NOWAIT mode */
				if (tmfd)
				{
					tmfd->ctid = *otid;
					tmfd->xmax = dirty_xid;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				FLUX_RELEASE_TUPLOCK();
				return TM_WouldBlock;
			}
		}
		/* If dirty_xid is our own or invalid, proceed with update */
	}

	/*
	 * Final committed-update gate (authoritative).
	 *
	 * STAGE 2 above runs the committed-update detector once, before the
	 * in-progress-writer wait.  That check races: while the head writer W is
	 * still in progress, the detector declines (W's undo record is not yet
	 * committed), so control falls through to the writer-wait stage.  The
	 * buffer exclusive lock does NOT block W's commit -- W clears its
	 * in-progress sLog marker through the sLog seqlock domain, and because FLUX
	 * updates in place, the on-page value is already W's.  If W commits in the
	 * window between STAGE 2's read and the writer-wait probe, the wait stage
	 * sees NO in-progress writer and would fall through to apply our update
	 * over W's now-committed value -- silently losing W's update.
	 *
	 * Re-run the detector here, after the wait stage has resolved, as the last
	 * action before the update proper.  Every path that reaches the update
	 * passes through this point with the buffer exclusively locked, so a
	 * committer that landed anywhere upstream is caught.  Reconcile dedup makes
	 * this converge exactly like STAGE 2: we bounce to EvalPlanQual once per
	 * distinct committed marker; a repeat of the identical (verptr, xid) marker
	 * we already reconciled falls through and applies.
	 */
	if (snapshot && IsMVCCSnapshot(snapshot))
	{
		RelUndoRecPtr head_verptr;
		TransactionId head_xid;
		bool		head_inprogress = false;

		if (FluxTupleHasCommittedUpdateAfter(relation,
											  old_tuple_hdr,
											  ItemIdGetLength(itemid),
											  snapshot,
											  GetCurrentTransactionIdIfAny(),
											  &head_verptr,
											  &head_xid,
											  &head_inprogress))
		{
			if (!FluxEpqReconcileMatches(snapshot,
										  RelationGetRelid(relation),
										  otid, head_verptr, head_xid))
			{
				FluxEpqReconcileMark(snapshot,
									  RelationGetRelid(relation), otid,
									  head_verptr, head_xid);
				if (tmfd)
				{
					tmfd->ctid = *otid;
					tmfd->xmax = InvalidTransactionId;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				UnlockReleaseBuffer(buffer);
				FLUX_RELEASE_TUPLOCK();
				return TM_Updated;
			}
		}
		else if (head_inprogress)
		{
			/*
			 * Commit-window conflict (see FluxTupleHasCommittedUpdateAfter):
			 * the head committer cleared its sLog marker + UNCOMMITTED flag
			 * at PRE_COMMIT but has not yet reached CLOG.  Neither this gate
			 * nor the sLog dirty-xid probe sees it, so we would clobber a
			 * just-committed update.  Wait on the in-flight writer, then retry
			 * from the top of the slow path where CLOG now resolves it to a
			 * committed conflict -> TM_Updated -> EPQ.  Heap-identical.
			 */
			if (wait)
			{
				TransactionId wait_xid = head_xid;

				/*
				 * Wait for the in-flight committer to reach CLOG or abort, then
				 * report TM_Updated so the executor re-fetches through
				 * EvalPlanQual.  EPQ's SnapshotAny fetch reconstructs the correct
				 * visible version (the committed value if head_xid committed, or
				 * the restored before-image if it aborted) and re-projects the
				 * update on top.  A blind update retry here would instead apply
				 * over the raw on-page bytes, which on abort are the
				 * not-yet-reverted value.
				 */
				UnlockReleaseBuffer(buffer);
				if (!have_tuple_lock)
					FluxLockTuple(relation, otid, LockTupleNoKeyExclusive,
								   true, &have_tuple_lock);
				XactLockTableWait(wait_xid, relation, otid, XLTW_Update);
				if (tmfd)
				{
					tmfd->ctid = *otid;
					tmfd->xmax = InvalidTransactionId;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				FLUX_RELEASE_TUPLOCK();
				return TM_Updated;
			}
		}
	}

	/*
	 * Get transaction timestamp BEFORE critical section (initializes the
	 * per-transaction MVCC state FluxGetDmlTimestamp relies on).  Commit
	 * visibility comes from CLOG (heap-shaped xmin/xmax MVCC).
	 */
	(void) FluxGetTransactionTimestamp();
	current_ts = (uint64) FluxGetDmlTimestamp();

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
	 * release the buffer lock first.  FluxFormTuple may call
	 * FluxStoreOverflowColumn which acquires buffer locks on overflow pages.
	 * If overflow data lands on the same page we're updating, that would
	 * cause a buffer lock re-entry assertion failure.
	 */
	update_overflow_buffers.count = 0;
	{
		bool		buffer_unlocked = false;

		/*
		 * This in-place UPDATE path runs only when no indexed column changed
		 * (a key-changing UPDATE is routed to the out-of-place path), so the
		 * varlena columns are typically unchanged.  FLUX has no on-page
		 * overflow: TOAST any oversized varlena into the relation's standard
		 * heap TOAST table before forming, exactly like the insert path, then
		 * form the tuple from the (possibly externalized) datums.
		 */
		slot_getallattrs(slot);
		{
			int			upd_natts = RelationGetDescr(relation)->natts;

			memcpy(upd_toast_values, slot->tts_values, upd_natts * sizeof(Datum));
			memcpy(upd_toast_isnull, slot->tts_isnull, upd_natts * sizeof(bool));
			flux_toast_tuple(relation, upd_toast_values, upd_toast_isnull,
							 NULL, NULL, &upd_ttc, upd_toast_attr,
							 &upd_toasted, 0);

			new_tuple = FluxFormTuple(RelationGetDescr(relation),
									   upd_toast_values,
									   upd_toast_isnull,
									   NULL,	/* FLUX has no on-page overflow */
									   NULL);
		}

		/* Set MVCC fields for new tuple (heap-shaped: fresh version) */
		new_tuple->t_data->t_commit_ts = 0;	/* t_xmax = InvalidTransactionId */

		/*
		 * The new on-page image is the NEWEST version; stamp its inserter
		 * (t_xmin) with our XID.  Older snapshots that cannot see this XID
		 * read the pre-update image back from the UNDO fork via t_verptr.
		 */
		new_tuple->t_data->t_flags |= FLUX_TUPLE_UNCOMMITTED;
		new_tuple->t_data->t_xmin = GetCurrentTransactionId();  /* subxid: heap-shaped, so savepoint rollback marks it aborted in CLOG */
		new_tuple_size = new_tuple->t_len;

		/*
		 * WS-PVS1: the version-chain head lives in the fixed header field
		 * t_verptr, so new_tuple already carries it inside its header with no
		 * trailing growth.  The real urec_ptr is stamped in below after
		 * RelUndoReserve returns; until then t_verptr holds
		 * InvalidRelUndoRecPtr (consistent with the unlogged/temp path that
		 * never reserves).
		 */
		new_tuple->t_data->t_flags |= FLUX_TUPLE_HAS_VERSION_PTR;
		FluxTupleSetVersionPtr(new_tuple->t_data, new_tuple_size,
								InvalidRelUndoRecPtr);

		/*
		 * Pre-compute the oldest active timestamp before (re-)acquiring the
		 * buffer lock.  This avoids an O(MaxBackends) shared-memory scan
		 * while holding a page-level exclusive lock.  The value is used by
		 * the defrag estimation/execution path below.
		 */
		defrag_oldest_ts = FluxGetOldestActiveTimestamp();
		defrag_oldest_xmin = FluxGetOldestXminHorizon(relation);

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
				FluxFreeTuple(new_tuple);
				FLUX_RELEASE_TUPLOCK();
				return TM_Invisible;
			}
			old_tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);
		}
	}

	/*
	 * Determine if we can do an in-place update.
	 *
	 * In-place update is FLUX's primary advantage over heap: it avoids
	 * creating dead tuple versions and the associated index maintenance. We
	 * try several strategies in order of increasing cost:
	 *
	 * 1. Direct fit: new tuple fits within the old tuple's slot. 2. Page
	 * space fit: new tuple is larger but the extra bytes fit in the page's
	 * available free space. 3. Defrag fit: page defragmentation frees enough
	 * space for the new tuple to fit in-place.
	 *
	 */
retry_fit:
	if (new_tuple_size <= ItemIdGetLength(itemid))
	{
		/* Strategy 1: new tuple fits within old tuple's slot */
	}
	else if (new_tuple_size <= ItemIdGetLength(itemid) + PageGetFreeSpace(page))
	{
		/*
		 * Strategy 2: new tuple is larger but the difference fits in the
		 * page's free space.  We need to relocate the tuple data within the
		 * page, which PageRepairFragmentation can handle.
		 */
	}
	else
	{
		/*
		 * Strategy 3: try page defragmentation to reclaim dead tuple space.
		 * If the page has the defrag-needed flag and defragmentation would
		 * free enough space, do it now.
		 */
		FluxPageOpaque upd_opaque = FluxPageGetOpaque(page);
		Size		potential_free;

		(void) upd_opaque;		/* flag is only an optimization hint; see below */

		{
			/*
			 * Strategy 3: reclaim space occupied by dead-to-all superseded
			 * versions via defragmentation.
			 *
			 * We DO NOT gate this on FLUX_PAGE_DEFRAG_NEEDED.  That flag is an
			 * optimization hint set by the pruning paths, but in-place UPDATEs
			 * accumulate dead-to-all versions on a hot page (e.g. TPC-C
			 * district) without reliably setting it.  Trusting the flag as
			 * authoritative made a 2-byte tuple growth spuriously fail with
			 * "does not fit" on a page that was actually full of reclaimable
			 * dead versions (measured: ~34k such aborts/run, contigfree=0,
			 * maxoff 47-69 on a 500-row table).  Always scan; only error if a
			 * full defrag genuinely cannot free enough space.
			 */
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
					FluxTupleHeader *df_hdr;

					if (!ItemIdIsNormal(df_itemid))
					{
						if (ItemIdIsDead(df_itemid))
							potential_free += ItemIdGetLength(df_itemid) + sizeof(ItemIdData);
						continue;
					}

					if (FluxIsOverflowRecordInline(PageGetItem(page, df_itemid),
											  ItemIdGetLength(df_itemid)))
						continue;

					df_hdr = (FluxTupleHeader *) PageGetItem(page, df_itemid);

					if (FluxTupleDeadToAll(df_hdr, defrag_oldest_xmin))
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
				 * mark dead tuples LP_DEAD, then defragment.
				 *
				 * As in FluxPagePruneOpt, opportunistic pruning sets LP_DEAD
				 * (reclaiming storage) rather than LP_UNUSED.  The deleted
				 * tuples may still have index entries; reserving the line
				 * pointer until VACUUM removes those entries prevents the TID
				 * from being recycled and returning wrong index-scan results.
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
						FluxTupleHeader *prune_hdr;

						if (!ItemIdIsNormal(prune_itemid))
							continue;

						if (FluxIsOverflowRecordInline(PageGetItem(page, prune_itemid),
												  ItemIdGetLength(prune_itemid)))
							continue;

						prune_hdr = (FluxTupleHeader *) PageGetItem(page, prune_itemid);

						if (FluxTupleDeadToAll(prune_hdr, defrag_oldest_xmin))
						{
							ItemIdSetDead(prune_itemid);
						}
					}
				}
				FluxPageDefragment(page);
				MarkBufferDirty(buffer);

				if (RelationNeedsWAL(relation))
				{
					XLogRecPtr	df_lsn;

					df_lsn = FluxXLogDefrag(relation, buffer, NULL, 0, defrag_oldest_ts);
					PageSetLSN(page, df_lsn);
				}
				END_CRIT_SECTION();

				FluxRecordFreeSpace(relation, blkno, PageGetFreeSpace(page));

				/*
				 * Re-fetch the item after defragmentation since line pointers
				 * may have been reorganized.  The offset number should still
				 * be valid for surviving tuples.
				 */
				itemid = PageGetItemId(page, offnum);
				old_tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

				/* Check again if in-place update now fits */
				if (new_tuple_size <= ItemIdGetLength(itemid) + PageGetFreeSpace(page))
				{
					flux_stat_defrag_triggered_updates++;
				}
				else
				{
					if (!force_shrink_attempted &&
						update_overflow_buffers.count == 0)
						goto force_shrink_retry;
					flux_release_update_overflow_buffers(&update_overflow_buffers,
														  buffer);
					UnlockReleaseBuffer(buffer);
					FluxFreeTuple(new_tuple);
					ereport(ERROR,
							(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							 errmsg("updated flux tuple does not fit on page after defragmentation"),
							 errhint("FLUX tuples have stable TIDs and cannot move to another page. Lower the table's fillfactor to reserve room for in-place growth.")));
				}
			}
			else
			{
				/* Defrag wouldn't free enough space */
				if (!force_shrink_attempted &&
					update_overflow_buffers.count == 0)
					goto force_shrink_retry;
				flux_release_update_overflow_buffers(&update_overflow_buffers,
													  buffer);
				UnlockReleaseBuffer(buffer);
				FluxFreeTuple(new_tuple);
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("updated flux tuple does not fit on page"),
						 errdetail("The updated row is larger than its current slot, all variable-length columns are already stored off-page, and the page has no free space."),
						 errhint("FLUX tuples have stable TIDs and cannot move to another page. Lower the table's fillfactor to reserve room for in-place growth.")));
			}
		}
	}

	if (false)
	{
		/*
		 * Recovery path for an in-place-grown tuple that no longer fits in its
		 * slot, even after page defragmentation.  FLUX TIDs are stable, so the
		 * tuple cannot move to another page; the only way to shrink it is to
		 * push variable-length columns off-page.  Re-form the new tuple forcing
		 * every eligible varlena column into overflow with a zero inline
		 * prefix, which collapses the main tuple to its minimum footprint, then
		 * retry the in-place fit (it should now satisfy Strategy 1).
		 *
		 * This branch is only entered from the ERROR sites above, and only when
		 * the first form attempt took the no-overflow fast path (the main
		 * buffer is still locked and no overflow buffers were collected).
		 */
force_shrink_retry:
		{
			bool		relock_already_locked = false;

			FluxFreeTuple(new_tuple);

			/* Release the main buffer lock before re-forming with overflow. */
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

			new_tuple = FluxFormTupleForceShrink(RelationGetDescr(relation),
												  slot->tts_values,
												  slot->tts_isnull,
												  relation,
												  &update_overflow_buffers);

			new_tuple->t_data->t_commit_ts = 0;	/* t_xmax = InvalidTransactionId */
			new_tuple->t_data->t_flags |= FLUX_TUPLE_UNCOMMITTED;
			new_tuple->t_data->t_xmin = GetCurrentTransactionId();  /* subxid: heap-shaped, so savepoint rollback marks it aborted in CLOG */
			new_tuple_size = new_tuple->t_len;

			/*
			 * WS-PVS1: stamp InvalidRelUndoRecPtr into t_verptr, mirroring the
			 * main exclusive path, regardless of whether we reached this branch
			 * via fit cascade or force-shrink retry.
			 */
			new_tuple->t_data->t_flags |= FLUX_TUPLE_HAS_VERSION_PTR;
			FluxTupleSetVersionPtr(new_tuple->t_data, new_tuple_size,
									InvalidRelUndoRecPtr);

			/*
			 * Re-acquire the main buffer lock.  An overflow column may have
			 * landed on the page we are updating, in which case it is already
			 * locked as part of update_overflow_buffers.
			 */
			for (upd_i = 0; upd_i < update_overflow_buffers.count; upd_i++)
			{
				if (update_overflow_buffers.buffers[upd_i].buffer == buffer)
				{
					relock_already_locked = true;
					break;
				}
			}
			if (!relock_already_locked)
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

			page = BufferGetPage(buffer);

			/* Re-validate the line pointer after re-locking. */
			itemid = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(itemid))
			{
				flux_release_update_overflow_buffers(&update_overflow_buffers,
													  buffer);
				UnlockReleaseBuffer(buffer);
				FluxFreeTuple(new_tuple);
				FLUX_RELEASE_TUPLOCK();
				return TM_Invisible;
			}
			old_tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

			/*
			 * Re-validate visibility after the unlock/relock window.  The fit
			 * cascade ran conflict detection before forming the tuple, but
			 * re-forming with overflow drops the buffer lock again.  A
			 * concurrent committed delete that landed in that window must be
			 * reported as TM_Deleted rather than silently overwritten.
			 */
			if ((old_tuple_hdr->t_flags & FLUX_TUPLE_DELETED) &&
				!(old_tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED))
			{
				flux_release_update_overflow_buffers(&update_overflow_buffers,
													  buffer);
				UnlockReleaseBuffer(buffer);
				FluxFreeTuple(new_tuple);
				FLUX_RELEASE_TUPLOCK();
				return TM_Deleted;
			}

			force_shrink_attempted = true;
			goto retry_fit;
		}
	}

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
		uint32		old_len = ItemIdGetLength(itemid);
		char	   *old_copy;

		old_tuple_for_inplace_wal = palloc0(sizeof(FluxTupleData));
		old_copy = palloc(old_len);

		memcpy(old_copy, old_tuple_hdr, old_len);
		old_tuple_for_inplace_wal->t_len = old_len;
		old_tuple_for_inplace_wal->t_data = (FluxTupleHeader *) old_copy;
	}

	/*
	 * NOTE: The early UNDO record (pre-modification) was removed to avoid
	 * double UNDO records.  The deferred UNDO path below (upd_undo_ptr)
	 * handles UNDO recording for all in-place updates.  The deferred approach
	 * is safe because the WAL record includes both old and new tuple data,
	 * enabling crash recovery regardless of UNDO write order.
	 */


	/*
	 * Pre-allocate WAL buffer space BEFORE entering critical section. We may
	 * need to register the main buffer plus overflow buffers.
	 *
	 * rdata slots needed for UPDATE: MAX_OVERFLOW_BUFFERS (data per overflow
	 * record, no separate header) + 3 (xl_flux_update header + old tuple
	 * data + new tuple data)
	 *
	 * CRITICAL: XLogEnsureRecordSpace() may allocate memory, so it MUST be
	 * called outside the critical section.
	 */
	if (RelationNeedsWAL(relation))
		XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID, 4 + MAX_OVERFLOW_BUFFERS);

	/*
	 * Per-relation UNDO: reserve space for a full-tuple UPDATE record before
	 * the critical section.  The record stores the old tuple so a top-level
	 * ROLLBACK can restore it in place (RelUndoApplyUpdate handles same-size,
	 * grow, and shrink).  RelUndoReserve may extend the fork and ereport, so
	 * it must run outside the crit section.  Lock order: data buffer then
	 * UNDO buffer.
	 *
	 * If a force-shrink retry pushed columns off-page, update_overflow_buffers
	 * holds buffers that are pinned, content-locked, and marked dirty but not
	 * yet WAL-logged (overflow WAL is deferred to the atomic record below).
	 * RelUndoReserve and CheckForSerializableConflictIn can both ereport here,
	 * before the critical section makes the change durable.  Resource-owner
	 * cleanup would still release the pins and locks on abort, leaving only
	 * VACUUM-reclaimable dead overflow space, but release them explicitly so
	 * the abort path leaves no dirtied-yet-orphaned overflow pages behind.
	 */
	PG_TRY();
	{
		if (smgrexists(RelationGetSmgr(relation), RELUNDO_FORKNUM))
		{
			Size		upd_undo_reserve;

			/*
			 * WS-PVS1: reserve a worst-case (full-tuple) record BEFORE
			 * computing the diff so we can stamp the returned RelUndoRecPtr
			 * into new_tuple's trailing version slot AHEAD of the diff scan
			 * and the page memcpy.  The diff/full-tuple commit happens after
			 * stamping; over-reservation by a few bytes is wasted UNDO-fork
			 * space, not a correctness hazard.  Mirrors the CAS fast path
			 * (flux_operations.c:2018-2037).
			 */
			upd_undo_reserve = SizeOfRelUndoRecordHeader +
				sizeof(RelUndoUpdatePayload) +
				old_tuple_for_inplace_wal->t_len;

			upd_undo_ptr = RelUndoReserve(relation, upd_undo_reserve,
										  &upd_undo_buffer);

			/*
			 * Stamp the freshly reserved urec_ptr (never preserve a prior
			 * pointer): the new diff/full-tuple record reconstructs THIS
			 * update's before-image, and the header's urec_prevundorec
			 * already chains to the prior diff -- the row's verptr always
			 * points at the head of the chain.
			 */
			FluxTupleSetVersionPtr(new_tuple->t_data, new_tuple_size,
									upd_undo_ptr);
		}

		/*
		 * WS-PVS1: widen-then-narrow fixup (now a verptr no-op; retained for
		 * the flag-removal commit).  The version pointer lives in the fixed
		 * header field t_verptr, so FluxTupleGetVersionPtr/SetVersionPtr read
		 * and write it independently of the slot length -- widening the image
		 * to the old (larger) slot length no longer moves the pointer.  The
		 * in-place overwrite path below keeps the old line-pointer length when
		 * the new image is smaller; deform's self-describing t_natts lets the
		 * trailing slack be ignored by ordinary readers.  We still widen +
		 * zero-pad here so the memcpy below fills the whole slot (no stale
		 * trailing bytes).
		 *
		 * Done here, before the critical section, because repalloc()/pfree()
		 * are forbidden inside it.
		 */
		if (new_tuple_size <= ItemIdGetLength(itemid))
		{
			Size		slot_len = ItemIdGetLength(itemid);

			if (slot_len > new_tuple_size &&
				(new_tuple->t_data->t_flags & FLUX_TUPLE_HAS_VERSION_PTR))
			{
				RelUndoRecPtr verptr = FluxTupleGetVersionPtr(new_tuple->t_data,
															   new_tuple_size);

				new_tuple->t_data = (FluxTupleHeader *)
					repalloc(new_tuple->t_data, slot_len);
				MemSet((char *) new_tuple->t_data + new_tuple_size, 0,
					   slot_len - new_tuple_size);
				new_tuple->t_len = (uint32) slot_len;
				new_tuple_size = slot_len;
				FluxTupleSetVersionPtr(new_tuple->t_data, new_tuple_size, verptr);
			}
		}

		/*
		 * transaction read this tuple (holds a SIREAD lock on it), our update
		 * creates an rw-antidependency that may form a dangerous structure.
		 */
		CheckForSerializableConflictIn(relation, otid, BufferGetBlockNumber(buffer));

		/*
		 * Always set UNCOMMITTED so that visibility checks consult the sLog.
		 * Even though the tuple position hasn't moved (in-place update), the
		 * DATA has changed and other transactions must see the old data until
		 * this update commits.  The flag will be lazily cleared on the first
		 * visibility check after the updating transaction commits (since the
		 * sLog entry will have been removed at commit time).
		 *
		 * Also set FLUX_TUPLE_UPDATED to mark that this tuple has been
		 * updated in-place.  After commit, this flag persists and indicates
		 * that the tuple's t_commit_ts reflects the original INSERT commit
		 * time (not the UPDATE commit time).  This preserves visibility for
		 * readers whose snapshots predate the update.
		 */
		new_tuple->t_data->t_flags |= FLUX_TUPLE_UPDATED;

		/*
		 * t_gen is reserved/unused in FLUX (FLUX uses plain heap-TID index
		 * identity, no RowID/gen scheme).  Leave it at the palloc0 default.
		 */
		new_tuple->t_data->t_gen = 0;

		/*
		 * Build the heap-format logical-decoding images (old and new) BEFORE
		 * the critical section, since heap_form_tuple()/palloc() are forbidden
		 * inside it.  When the relation is not logically logged these leave
		 * data == NULL.  These calls palloc and can ereport on OOM, so they
		 * stay inside the PG_TRY: a throw here must also release the
		 * force-shrink overflow buffers, exactly like the reservation above.
		 */
		FluxXLogPrepareLogicalImage(relation, old_tuple_for_inplace_wal,
									 &update_old_img);
		FluxXLogPrepareLogicalImage(relation, new_tuple, &update_new_img);
	}
	PG_CATCH();
	{
		flux_release_update_overflow_buffers(&update_overflow_buffers, buffer);
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Start critical section for WAL logging */
	START_CRIT_SECTION();

	/*
	 * Set t_ctid on the in-memory new tuple BEFORE copying to the page. This
	 * ensures the WAL record's new_tuple image includes the correct t_ctid,
	 * so redo produces a page identical to the primary.  (We use blkno/offnum
	 * which is correct for both the "fits in existing slot" and
	 * "delete+re-add" strategies since we update offnum below if it changes.)
	 */
	ItemPointerSet(&new_tuple->t_data->t_ctid, blkno, offnum);

	if (new_tuple_size <= ItemIdGetLength(itemid))
	{
		/*
		 * New tuple fits within the old tuple's allocated space. Overwrite
		 * directly -- safe because we don't exceed the existing allocation.
		 * When the kept slot is larger than the new image, new_tuple was
		 * already widened to the full slot length before the critical section
		 * (see the WS-PVS1 widen block above), so new_tuple_size == slot_len
		 * here and the memcpy fills the whole slot, keeping any trailing
		 * slack zeroed.  The version pointer lives in the header field
		 * t_verptr, so it is copied by the memcpy regardless of slot width.
		 */
		memcpy(old_tuple_hdr, new_tuple->t_data, new_tuple_size);
	}
	else
	{
		/*
		 * New tuple is larger than the old one but fits on the page (Strategy
		 * 2 or 3).  We cannot memcpy in place because that would overwrite
		 * adjacent data.  Instead, remove the old line pointer entry, compact
		 * the page, and re-add the new tuple at the same offset.
		 *
		 * We use FluxPageIndexTupleDelete instead of the standard
		 * PageIndexTupleDelete because FLUX pages may contain LP_UNUSED
		 * items left by opportunistic defragmentation. PageIndexTupleDelete
		 * asserts all items are LP_NORMAL, which fails when LP_UNUSED items
		 * are present. FluxPageIndexTupleDelete skips LP_UNUSED items in the
		 * offset adjustment loop.
		 */
		FluxPageIndexTupleDelete(page, offnum);

		offnum = PageAddItem(page, new_tuple->t_data,
							 new_tuple_size,
							 offnum, false, false);

		if (offnum == InvalidOffsetNumber)
			elog(PANIC, "failed to re-add FLUX tuple after delete for growing update");

		/* Re-fetch itemid and header from the (same) location */
		itemid = PageGetItemId(page, offnum);
		old_tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

		ItemPointerSet(&new_tuple->t_data->t_ctid, blkno, offnum);
	}

	/* Set new TID to same location */
	ItemPointerSet(&slot->tts_tid, blkno, offnum);
	new_tuple->t_self = slot->tts_tid;
	/*
	 * t_ctid on the on-disk tuple is already correct from the memcpy or
	 * PageAddItem above, since we set it on new_tuple->t_data before copying.
	 */

	/* Track in-place update success */
	flux_stat_in_place_updates++;

	slot->tts_tableOid = RelationGetRelid(relation);

	/*
	 * Update page opaque header to track the latest commit timestamp and
	 * current free space.  This must happen before MarkBufferDirty and WAL
	 * logging so that full-page images capture the correct opaque state.  The
	 * redo function performs the same updates so WAL consistency checking
	 * passes.
	 */
	{
		FluxPageOpaque upd_phdr = FluxPageGetOpaque(page);

		FluxPageSetCommitTs(upd_phdr, Max(FluxPageGetCommitTs(upd_phdr), current_ts));
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
		FluxXLogUpdate(relation, buffer, offnum,
						old_tuple_for_inplace_wal, new_tuple,
						old_tuple_for_inplace_wal->t_data->t_commit_ts,
						0,		/* new version: t_xmax = Invalid (heap-shaped) */
						&update_overflow_buffers,
						InvalidBuffer,
						&update_old_img, &update_new_img);
	}

	END_CRIT_SECTION();

	FluxXLogReleaseLogicalImage(&update_old_img);
	FluxXLogReleaseLogicalImage(&update_new_img);

	/*
	 * Release all overflow buffers first — they were WAL-logged atomically
	 * above so they're safe to unlock now.
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
	 * Clear VM bits and capture free space while we still hold the main
	 * buffer lock.  Both need page access.
	 */
	FluxVMUpdateForUpdate(relation, buffer);
	{
		Size		update_free_space = PageGetFreeSpace(page);

		/*
		 * sLog registration BEFORE buffer release — eliminates the race
		 * window where another backend reads the modified tuple but finds no
		 * sLog entry.  Safe: seqlock reads take no lock, no deadlock.
		 */
		FluxEnsureSLogCallbacks();
		SLogTupleInsert(RelationGetRelid(relation), &slot->tts_tid,
						GetTopTransactionId(), SLOG_OP_UPDATE,
						GetCurrentSubTransactionId(), cid, current_ts, 0,
						LockTupleNoKeyExclusive);
		SLogTupleStoreBeforeImage(RelationGetRelid(relation), &slot->tts_tid,
								  GetTopTransactionId(),
								  (const char *) old_tuple_for_inplace_wal->t_data,
								  old_tuple_for_inplace_wal->t_len,
								  old_tuple_for_inplace_wal->t_data->t_flags,
								  old_tuple_for_inplace_wal->t_data->t_commit_ts,
								  relation->rd_locator,
								  relation->rd_rel->relpersistence);

		/* Release the main buffer lock — sLog registered, no race. */
		UnlockReleaseBuffer(buffer);

		FluxRecordFreeSpace(relation, blkno, update_free_space);
	}

	/*
	 * Finish the per-relation UNDO record now that the buffer lock is
	 * released.  This works from old_tuple_for_inplace_wal, a palloc'd copy
	 * taken before the page modification, so it does NOT need the page.
	 *
	 * Moving this out of the buffer-lock-held window significantly reduces
	 * contention at high concurrency (8+ clients on hot pages).
	 *
	 * Write a full-tuple RELUNDO_UPDATE record with old/new TID mapping and
	 * the old tuple data so that rollback can restore it.
	 */
	if (RelUndoRecPtrIsValid(upd_undo_ptr))
	{
		RelUndoRecordHeader upd_undo_hdr;
		RelUndoUpdatePayload upd_undo_payload;
		char	   *upd_combined;
		Size		upd_payload_total;

		upd_undo_payload.oldtid = *otid;
		upd_undo_payload.newtid = slot->tts_tid;

		upd_undo_hdr.urec_xid = GetCurrentTransactionId();
		upd_undo_hdr.urec_prevundorec =
			GetPerRelUndoPtr(RelationGetRelid(relation));

		/*
		 * Full-tuple UPDATE UNDO record: store the old tuple so rollback
		 * can restore it (RelUndoApplyUpdate handles same-size, grow, and
		 * shrink).  FLUX matches ZHeap: the whole old tuple to UNDO, no
		 * byte-diff/delta.  Layout written by RelUndoFinish:
		 * [header][payload][old tuple].
		 */
		upd_undo_hdr.urec_type = RELUNDO_UPDATE;
		upd_undo_hdr.urec_len = (uint16)
			(SizeOfRelUndoRecordHeader + sizeof(RelUndoUpdatePayload) +
			 old_tuple_for_inplace_wal->t_len);
		upd_undo_hdr.info_flags = RELUNDO_INFO_HAS_TUPLE;
		upd_undo_hdr.tuple_len = (uint16) old_tuple_for_inplace_wal->t_len;

		upd_payload_total = sizeof(RelUndoUpdatePayload) +
			old_tuple_for_inplace_wal->t_len;
		upd_combined = palloc(upd_payload_total);
		memcpy(upd_combined, &upd_undo_payload, sizeof(RelUndoUpdatePayload));
		memcpy(upd_combined + sizeof(RelUndoUpdatePayload),
			   old_tuple_for_inplace_wal->t_data,
			   old_tuple_for_inplace_wal->t_len);

		RelUndoFinish(relation, upd_undo_buffer, upd_undo_ptr,
					  &upd_undo_hdr, upd_combined, upd_payload_total);
		RegisterPerRelUndo(RelationGetRelid(relation), upd_undo_ptr);
		pfree(upd_combined);
	}

	/*
	 * No index maintenance for this path.  Key-changing UPDATEs are routed out
	 * of place at the top of flux_tuple_update (delete + insert at a new TID,
	 * *update_indexes = TU_All); this in-place path runs only for non-indexed
	 * changes, so the TID is unchanged and every secondary (key, TID) entry
	 * still points at the live tuple.  *update_indexes was defaulted to TU_None
	 * at the top of the function, so no index re-insert happens.
	 */
	{
		/*
		 * sLog registration was done above (before buffer release). The
		 * FluxEnsureSLogCallbacks + SLogTupleInsert +
		 * SLogTupleStoreBeforeImage calls were moved to eliminate the
		 * visibility race window at high concurrency.
		 */

		/* Free old_tuple copy now that before-image has been stored */
		FluxFreeTuple(old_tuple_for_inplace_wal);

		/* Mark this block dirty for the scan-path sLog bypass */
		FluxDirtyMapMark(RelationGetRelid(relation), blkno);

		/*
		 * NOTE: We do NOT immediately clean up overflow chains here.
		 * Immediate cleanup was: 1. Buggy (collected wrong overflow pointers
		 * after in-place modification) 2. Expensive on hot paths (extra
		 * buffer I/O + locking during UPDATE) 3. Complex to WAL-log correctly
		 *
		 * Instead, overflow cleanup is deferred to VACUUM.  When VACUUM prunes
		 * deleted tuples, it will also reclaim orphaned overflow pages.
		 *
		 * Future enhancement: Log overflow block/offset in WAL UPDATE record
		 * so UNDO log pruning can also clean up overflow chains.
		 */
		(void) old_has_overflow;	/* Suppress unused variable warning */
	}

	FluxFreeTuple(new_tuple);

	FLUX_RELEASE_TUPLOCK();

	/* Buffer + tuple locks released; safe to drive throttled cleanup. */
	SLogTupleMaybeCleanupRetained();
	RelUndoMaybeVacuum(relation);
	pgstat_count_heap_update(relation, false, false);
	return TM_Ok;
}

#undef FLUX_RELEASE_TUPLOCK



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
flux_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
				   CommandId cid, uint32 options, BulkInsertState bistate)
{
	FluxTuple *formed_tuples;
	bool	   *needs_single_insert;
	uint64		current_ts;
	TransactionId my_xid;
	int			i;
	int			ndone;

	if (ntuples <= 0)
		return;

	/*
	 * Get timestamps and XID outside the loop — these are per-transaction
	 * cached values, but calling them once avoids function call overhead.
	 */
	(void) FluxGetTransactionTimestamp();
	current_ts = (uint64) FluxGetDmlTimestamp();
	my_xid = GetCurrentTransactionId();	/* subxid: heap-shaped xmin */
	/* Ensure relation storage exists */
	RelationGetSmgr(relation);

	/*
	 * Phase 1: Pre-form all tuples without overflow handling. Passing NULL
	 * for rel and overflow_buffers skips the overflow path, keeping the tuple
	 * inline.  Tuples that exceed the page size will be detected below and
	 * routed to single-insert.
	 */
	formed_tuples = (FluxTuple *) palloc(ntuples * sizeof(FluxTuple));
	needs_single_insert = (bool *) palloc0(ntuples * sizeof(bool));

	for (i = 0; i < ntuples; i++)
	{
		slot_getallattrs(slots[i]);
		formed_tuples[i] = FluxFormTuple(RelationGetDescr(relation),
										  slots[i]->tts_values,
										  slots[i]->tts_isnull,
										  NULL, /* no overflow in batch */
										  NULL);

		/* Set MVCC fields (heap-shaped xmin/xmax) */
		formed_tuples[i]->t_data->t_commit_ts = 0;	/* t_xmax = Invalid */
		formed_tuples[i]->t_data->t_flags |= FLUX_TUPLE_UNCOMMITTED;
		formed_tuples[i]->t_data->t_xmin = my_xid;

		/*
		 * WS-PVS1: reserve the trailing version-pointer slot at INSERT time so
		 * the first UPDATE is a same-length overwrite (see flux_tuple_insert
		 * for the rationale).  Done before the size check below so oversize
		 * routing accounts for the base+8 on-page footprint.
		 */
		formed_tuples[i]->t_data->t_flags |= FLUX_TUPLE_HAS_VERSION_PTR;
		FluxTupleSetVersionPtr(formed_tuples[i]->t_data,
								formed_tuples[i]->t_len,
								InvalidRelUndoRecPtr);

		/* Mark tuples too large for batch insert */
		if (formed_tuples[i]->t_len > FLUX_MAX_TUPLE_SIZE)
			needs_single_insert[i] = true;
	}

	/*
	 * Phase 2: Batch insert page-at-a-time.
	 *
	 * For each page: lock once, insert all fitting tuples, WAL-log once,
	 * unlock.  This is much faster than per-tuple buffer operations.
	 *
	 * We register each inserted tuple in the backend-local tracked-key list
	 * (via SLogTupleTrackLocalOnly) so that FluxClearUncommittedFlags() can
	 * find and stamp them at commit time.  Without this, COPY-inserted tuples
	 * retain FLUX_TUPLE_UNCOMMITTED permanently and are invisible to all
	 * snapshots.
	 */
	FluxEnsureSLogCallbacks();
	ndone = 0;
	while (ndone < ntuples)
	{
		Buffer		buffer;
		Page		page;
		BlockNumber target_block;
		int			batch_start;
		int			batch_count;
		int			nfit;
		Size		saveFreeSpace;
		Size		avail;
		OffsetNumber *offnums = NULL;
		FluxLogicalImage *logical_imgs = NULL;
		FluxLogicalImage combined_image = {NULL, 0};
		bool		need_logical = RelationIsLogicallyLogged(relation);

		/* Skip tuples that need single-insert (overflow) */
		if (needs_single_insert[ndone])
		{
			flux_tuple_insert(relation, slots[ndone], cid, options, bistate);
			FluxFreeTuple(formed_tuples[ndone]);
			ndone++;
			continue;
		}

		/* Find a page with space for at least one tuple (fill-factor aware) */
		saveFreeSpace = RelationGetTargetPageFreeSpace(relation,
													   FLUX_DEFAULT_FILLFACTOR);
		target_block = FluxGetPageWithFreeSpace(relation,
												 formed_tuples[ndone]->t_len + saveFreeSpace);
		if (target_block == InvalidBlockNumber)
		{
			/* Fall back to single insert */
			flux_tuple_insert(relation, slots[ndone], cid, options, bistate);
			FluxFreeTuple(formed_tuples[ndone]);
			ndone++;
			continue;
		}

		buffer = ReadBuffer(relation, target_block);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);

		/*
		 * Determine how many tuples will fit on this page BEFORE entering the
		 * critical section.  Logical-decoding images and the offnums array must
		 * be allocated outside the crit section, so we need the count up front.
		 * This mirrors heap_multi_insert(): walk the pending tuples, subtracting
		 * each tuple's (line-pointer + aligned body) cost from the page's free
		 * space until the next one would not fit, honouring the fill-factor
		 * reserve and stopping at the first overflow tuple.
		 */
		avail = PageGetFreeSpace(page);
		/*
		 * Honour the fill-factor reserve while PACKING, not just while choosing
		 * the target block.  FLUX has stable TIDs and updates rows in place, so
		 * a page packed 100%% full leaves no room for later in-place growth and
		 * a growing UPDATE (e.g. an accumulating numeric) aborts with "does not
		 * fit".  Reserve saveFreeSpace bytes here so bulk load (COPY / HammerDB)
		 * leaves the same headroom the single-insert path does.  The "always
		 * take at least one tuple" rule below still guarantees forward progress
		 * even when a single tuple exceeds the post-reserve budget.
		 */
		if (avail > saveFreeSpace)
			avail -= saveFreeSpace;
		nfit = 0;
		while (ndone + nfit < ntuples &&
			   !needs_single_insert[ndone + nfit])
		{
			Size		need = sizeof(ItemIdData) +
				MAXALIGN(formed_tuples[ndone + nfit]->t_len);

			/* Always take at least one tuple; stop when the next won't fit */
			if (nfit > 0 && need > avail)
				break;
			avail -= need;
			nfit++;
		}
		Assert(nfit > 0);

		offnums = (OffsetNumber *) palloc(nfit * sizeof(OffsetNumber));
		if (need_logical)
		{
			Size		total = 0;
			char	   *p;

			logical_imgs = (FluxLogicalImage *)
				palloc(nfit * sizeof(FluxLogicalImage));
			for (i = 0; i < nfit; i++)
				FluxXLogPrepareLogicalImage(relation,
											 formed_tuples[ndone + i],
											 &logical_imgs[i]);

			/*
			 * Serialize every image into one contiguous blob, framed exactly as
			 * FluxXLogRegisterLogicalImage emits a single image: each frame is
			 * "[heap bytes][uint32 len]" in tuple order.  The multi-insert
			 * emitter registers this blob as a single rdata chunk, keeping the
			 * registered-data slot count constant regardless of batch size (the
			 * old per-tuple registration overflowed XLR_NORMAL_RDATAS on dense
			 * pages).  We size for all nfit images but register only the prefix
			 * covering the tuples actually inserted (see below), matching the
			 * ntuples trailing frames the redo/decode readers expect.
			 */
			for (i = 0; i < nfit; i++)
				if (logical_imgs[i].data != NULL)
					total += logical_imgs[i].len + sizeof(uint32);
			if (total > 0)
			{
				combined_image.data = (char *) palloc(total);
				p = combined_image.data;
				for (i = 0; i < nfit; i++)
				{
					if (logical_imgs[i].data == NULL)
						continue;
					memcpy(p, logical_imgs[i].data, logical_imgs[i].len);
					p += logical_imgs[i].len;
					memcpy(p, &logical_imgs[i].len, sizeof(uint32));
					p += sizeof(uint32);
				}
			}
		}

		/*
		 * Ensure the current transaction has an XID assigned BEFORE entering
		 * the critical section (GetCurrentTransactionId may lock and allocate,
		 * both forbidden in a crit section).  An assigned XID is required for
		 * correctness: a multi-insert WAL record without an attached xid cannot
		 * be decoded into a logical replication stream (ReorderBuffer groups
		 * changes by xid and asserts the xid is valid), and a WAL-emitting
		 * transaction would otherwise be treated as read-only.  This mirrors
		 * flux_tuple_insert() and heap_multi_insert().
		 */
		if (RelationNeedsWAL(relation))
			(void) GetCurrentTransactionId();

		/* Pre-allocate WAL space outside critical section */
		if (RelationNeedsWAL(relation))
			XLogEnsureRecordSpace(XLR_MAX_BLOCK_ID, 4);

		START_CRIT_SECTION();

		batch_start = ndone;
		batch_count = 0;

		/* Insert the tuples we sized for above */
		while (batch_count < nfit)
		{
			OffsetNumber offnum;
			ItemId		inserted_itemid;
			FluxTupleHeader *inserted_hdr;

			offnum = FluxPageAddTuple(page, formed_tuples[ndone],
									   formed_tuples[ndone]->t_len);
			if (offnum == InvalidOffsetNumber)
				break;			/* page is full, stop batching */

			/* Set TID in slot */
			ItemPointerSet(&slots[ndone]->tts_tid, target_block, offnum);
			formed_tuples[ndone]->t_self = slots[ndone]->tts_tid;
			slots[ndone]->tts_tableOid = RelationGetRelid(relation);
			/* Set t_ctid to self */
			inserted_itemid = PageGetItemId(page, offnum);
			inserted_hdr = (FluxTupleHeader *) PageGetItem(page, inserted_itemid);
			ItemPointerSet(&inserted_hdr->t_ctid, target_block, offnum);

			offnums[batch_count] = offnum;
			batch_count++;
			ndone++;
		}

		if (batch_count > 0)
		{
			/* Update page opaque fields */
			FluxPageOpaque phdr = FluxPageGetOpaque(page);

			FluxPageSetCommitTs(phdr, Max(FluxPageGetCommitTs(phdr), current_ts));

			MarkBufferDirty(buffer);

			/*
			 * WAL-log the whole batch in one record.  Every tuple body is
			 * logged individually (no forced full-page image), so recovery is
			 * crash-safe even with full_page_writes=off, and one logical-
			 * decoding image per tuple is appended for logically-logged
			 * relations so COPY'd rows replicate.
			 */
			if (RelationNeedsWAL(relation))
			{
				XLogRecPtr	recptr;
				FluxLogicalImage *emit_image = NULL;

				/*
				 * Register only the framed-image prefix covering the tuples we
				 * actually inserted.  batch_count can be < nfit if the page
				 * filled mid-loop, and the redo/decode readers walk exactly
				 * ntuples (== batch_count) trailing frames, so a longer blob
				 * would misalign their backward walk.
				 */
				if (combined_image.data != NULL)
				{
					Size		prefix = 0;

					for (i = 0; i < batch_count; i++)
						if (logical_imgs[i].data != NULL)
							prefix += logical_imgs[i].len + sizeof(uint32);
					combined_image.len = (uint32) prefix;
					if (prefix > 0)
						emit_image = &combined_image;
				}

				recptr = FluxXLogMultiInsert(relation, buffer,
											  offnums,
											  &formed_tuples[batch_start],
											  batch_count, current_ts,
											  emit_image);
				PageSetLSN(page, recptr);
			}
		}

		END_CRIT_SECTION();

		if (logical_imgs != NULL)
		{
			for (i = 0; i < nfit; i++)
				FluxXLogReleaseLogicalImage(&logical_imgs[i]);
			pfree(logical_imgs);
		}
		if (combined_image.data != NULL)
			pfree(combined_image.data);
		pfree(offnums);

		/*
		 * Register each inserted tuple in the backend-local tracked-key list.
		 * This is lightweight (no shared hash entry, no LWLock) — just a
		 * palloc'd linked-list node per tuple.  FluxClearUncommittedFlags()
		 * iterates this list at PRE_COMMIT to clear the UNCOMMITTED flag,
		 * making the tuples visible.
		 *
		 * We skip full sLog registration (shared hash) because COPY doesn't
		 * use SNAPSHOT_DIRTY or ON CONFLICT, and the local tracking is
		 * sufficient for commit-time processing.
		 */
		{
			TransactionId xid = GetTopTransactionId();
			TransactionId subxid = GetCurrentSubTransactionId();

			for (i = batch_start; i < batch_start + batch_count; i++)
			{
				SLogTupleTrackLocalOnly(RelationGetRelid(relation),
										&slots[i]->tts_tid,
										xid, subxid);
			}
		}

		/* Update FSM with remaining free space */
		FluxRecordFreeSpace(relation, target_block, PageGetFreeSpace(page));

		/* Update visibility map */
		FluxVMUpdateForInsert(relation, formed_tuples[batch_start]->t_data,
							   buffer);

		UnlockReleaseBuffer(buffer);

		/*
		 * Count the batched inserts here rather than once for all ntuples at
		 * the end: tuples routed to the single-insert fallback are already
		 * counted by their own flux_tuple_insert call, so counting the whole
		 * ntuples at the tail would double-count them.
		 */
		pgstat_count_heap_insert(relation, batch_count);

		/* Free formed tuples in this batch */
		for (i = batch_start; i < batch_start + batch_count; i++)
			FluxFreeTuple(formed_tuples[i]);
	}

	pfree(formed_tuples);
	pfree(needs_single_insert);
}

/*
 * FluxVacuumCrossPageDefrag - move live tuples from tail pages to front pages
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
 *      free space (via the FLUX free space map).
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
FluxVacuumCrossPageDefrag(Relation rel, BlockNumber nblocks,
						   BlockNumber *empty_end_pages_p,
						   int nindexes, Relation *indrels,
						   BufferAccessStrategy bstrategy)
{
	BlockNumber src_blkno;
	BlockNumber nonempty_limit;
	int			tuples_moved = 0;
	int			pages_emptied = 0;
	EState	   *estate = NULL;
	TupleTableSlot *slot = NULL;
	IndexInfo **indexInfoArray = NULL;

	/*
	 * A deferred index-maintenance record: an off-page copy of a moved tuple
	 * plus its new TID.  We must NOT call index_insert() while holding any
	 * table buffer content lock -- concurrent index bottom-up deletion locks
	 * the index leaf first and then the table page (flux_index_delete_tuples),
	 * so inserting into an index under the source/target page lock would form
	 * an AB-BA deadlock on buffer content locks, which the deadlock detector
	 * cannot see.  Instead we accumulate the moves for a source page and
	 * replay them after releasing that page's lock.
	 */
	typedef struct FluxDefragMove
	{
		char	   *tupcopy;
		Size		tuplen;
		ItemPointerData new_tid;
	}			FluxDefragMove;

	/*
	 * Cross-page defrag relocates a live tuple to a lower-numbered page,
	 * which changes its TID.  FLUX's whole design guarantees stable TIDs
	 * (UPDATE is performed in place, keeping the same TID); its index integration and the
	 * UPDATE path (which ereport()s rather than move a tuple that no longer
	 * fits -- see "FLUX tuples have stable TIDs and cannot move to another
	 * page") both depend on that invariant.  We can insert a new index entry
	 * for the new TID, but there is no cheap primitive to remove the OLD
	 * entry for the pre-move TID (btree reclaims dead entries only via
	 * ambulkdelete, not point-deletes).  Leaving the stale entry behind and
	 * then truncating the vacated tail page orphans it: an index scan hitting
	 * that entry fails with "could not read blocks N..N".  So when the
	 * relation has indexes we must not move tuples at all -- matching plain
	 * heap lazy VACUUM, which never relocates tuples precisely because it
	 * cannot cheaply update index pointers.  The tail pages simply are not
	 * truncated this pass; a later VACUUM (or a REINDEX) can reclaim them.
	 *
	 * ponytail: skip-the-move when indexes exist. The optimal fix (relocate
	 * and keep every index consistent) needs a moved-from-TID bulk-delete
	 * pass per index; add that if tail reclamation on indexed FLUX tables
	 * ever measurably matters.
	 */
	if (nindexes > 0)
		return 0;

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

	/*
	 * Cross-page defragmentation is an internal heuristic whose page-by-page
	 * progress depends on FSM search order and tuple packing, both of which
	 * vary across platforms and BLCKSZ.  Report it at DEBUG2 rather than INFO
	 * so that VACUUM VERBOSE output stays stable; the per-relation summary
	 * below (emitted at INFO) carries the user-meaningful result.
	 */
	ereport(DEBUG2,
			(errmsg_internal("table \"%s\": starting cross-page defragmentation from block %u",
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
		FluxDefragMove *deferred_moves = NULL;
		int			ndeferred = 0;

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
			FluxTupleHeader *tuple_hdr;
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

			tuple_hdr = (FluxTupleHeader *) PageGetItem(src_page, itemid);

			/* Skip pages with overflow records -- too complex to relocate */
			if (FluxIsOverflowRecordInline(tuple_hdr, ItemIdGetLength(itemid)))
			{
				skip_page = true;
				break;
			}

			/* Skip pages with tuples that have overflow pointers */
			if (tuple_hdr->t_flags & FLUX_TUPLE_HAS_OVERFLOW)
			{
				skip_page = true;
				break;
			}

			/* Skip pages with deleted-but-not-yet-removed tuples */
			if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
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
		 * Deferred index-maintenance list for this source page.  Index inserts
		 * are replayed only after src_buf is unlocked (see FluxDefragMove).
		 */
		if (nindexes > 0)
		{
			deferred_moves = (FluxDefragMove *)
				palloc(ntuples_on_page * sizeof(FluxDefragMove));
			ndeferred = 0;
		}

		/*
		 * Second pass: move each live tuple to a target page near the front
		 * of the relation.
		 */
		for (offnum = FirstOffsetNumber; offnum <= maxoff;
			 offnum = OffsetNumberNext(offnum))
		{
			ItemId		src_itemid;
			FluxTupleHeader *src_hdr;
			Size		tuple_len;
			Buffer		dst_buf;
			Page		dst_page;
			BlockNumber dst_blkno;
			OffsetNumber dst_offnum;
			ItemPointerData new_tid;

			src_itemid = PageGetItemId(src_page, offnum);
			if (!ItemIdIsNormal(src_itemid))
				continue;

			src_hdr = (FluxTupleHeader *) PageGetItem(src_page, src_itemid);
			tuple_len = ItemIdGetLength(src_itemid);

			/*
			 * Find a target page with enough free space.  Use the FSM
			 * directly (GetPageWithFreeSpace) instead of our wrapper
			 * FluxGetPageWithFreeSpace, because the wrapper reads and locks
			 * pages to verify free space, which would deadlock against the
			 * exclusive lock we already hold on the source page.  We verify
			 * the actual free space below, after locking the target page. The
			 * target must be before the source block to be useful for
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
				FluxRecordFreeSpace(rel, dst_blkno, actual_free);
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
				FluxTupleHeader *dst_hdr;

				dst_itemid = PageGetItemId(dst_page, dst_offnum);
				dst_hdr = (FluxTupleHeader *) PageGetItem(dst_page, dst_itemid);
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
				recptr = FluxXLogCrossPageDefrag(rel,
												  dst_buf, dst_offnum,
												  src_buf, offnum,
												  src_hdr, (uint32) tuple_len);
				PageSetLSN(dst_page, recptr);
				PageSetLSN(src_page, recptr);
			}

			END_CRIT_SECTION();

			/* Update FSM for the target page */
			FluxRecordFreeSpace(rel, dst_blkno,
								 PageGetFreeSpace(dst_page));

			/*
			 * If the relation has indexes, copy the moved tuple off-page now,
			 * while we still hold dst_buf, and defer the index insertion until
			 * after every table page lock for this source page is released.
			 * Calling index_insert() under a table content lock would invert
			 * the lock order used by concurrent index bottom-up deletion
			 * (flux_index_delete_tuples locks the index leaf, then the table
			 * page) and deadlock on buffer content locks.
			 */
			if (nindexes > 0)
			{
				ItemId		moved_itemid = PageGetItemId(dst_page, dst_offnum);
				FluxTupleHeader *moved_hdr =
					(FluxTupleHeader *) PageGetItem(dst_page, moved_itemid);
				FluxDefragMove *mv = &deferred_moves[ndeferred++];

				mv->tuplen = ItemIdGetLength(moved_itemid);
				mv->tupcopy = (char *) palloc(mv->tuplen);
				memcpy(mv->tupcopy, moved_hdr, mv->tuplen);
				mv->new_tid = new_tid;
			}

			UnlockReleaseBuffer(dst_buf);

			tuples_moved++;
		}

		/* Update FSM for the source page */
		FluxRecordFreeSpace(rel, src_blkno,
							 PageGetFreeSpace(src_page));

		UnlockReleaseBuffer(src_buf);

		/*
		 * Now that no table buffer content lock is held, replay the deferred
		 * index insertions for the tuples moved off this source page.  Working
		 * from off-page copies is safe: pages carrying overflow pointers were
		 * skipped in the first pass, so every copied tuple is self-contained.
		 */
		if (deferred_moves != NULL)
		{
			for (int m = 0; m < ndeferred; m++)
			{
				FluxDefragMove *mv = &deferred_moves[m];
				Datum		values[INDEX_MAX_KEYS];
				bool		isnull[INDEX_MAX_KEYS];

				ExecClearTuple(slot);
				FluxTupleToSlot((FluxTupleHeader *) mv->tupcopy, slot);
				slot->tts_tid = mv->new_tid;
				for (int i = 0; i < nindexes; i++)
				{
					FormIndexDatum(indexInfoArray[i], slot, estate,
								   values, isnull);

					/*
					 * Skip uniqueness check since we're relocating an existing
					 * tuple.
					 */
					index_insert(indrels[i], values, isnull, &mv->new_tid,
								 rel, UNIQUE_CHECK_NO, false,
								 indexInfoArray[i]);

					ResetPerTupleExprContext(estate);
				}

				pfree(mv->tupcopy);
			}

			pfree(deferred_moves);
			deferred_moves = NULL;
			ndeferred = 0;
		}

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

	if (tuples_moved > 0)
		ereport(DEBUG2,
				(errmsg_internal("table \"%s\": cross-page defrag moved %d tuples, emptied %d pages",
								  RelationGetRelationName(rel),
								  tuples_moved, pages_emptied)));

	return tuples_moved;
}

/*
 * Per-stream state for the Phase I VACUUM scan callback.
 */
typedef struct FluxVacScanState
{
	Relation	rel;
	BlockNumber current_block;	/* last block handed out (InvalidBlockNumber
								 * before the first call) */
	BlockNumber nblocks;
	bool		skip_all_frozen;	/* honor the VM ALL_FROZEN skip? */
}			FluxVacScanState;

/*
 * Read stream callback for VACUUM Phase I.
 *
 * Hands the read stream the next block that actually needs scanning so the
 * upcoming pages are prefetched under AIO, matching heapam's
 * heap_vac_scan_next_block().  Pages marked ALL_FROZEN in the visibility map
 * are skipped here (unless DISABLE_PAGE_SKIPPING was requested), so the
 * stream never issues I/O for pages VACUUM would immediately discard.
 */
static BlockNumber
flux_vac_scan_next_block(ReadStream *stream,
						  void *callback_private_data,
						  void *per_buffer_data)
{
	FluxVacScanState *state = callback_private_data;
	BlockNumber next_block = state->current_block + 1;

	for (; next_block < state->nblocks; next_block++)
	{
		CHECK_FOR_INTERRUPTS();

		if (state->skip_all_frozen &&
			FluxVMCheck(state->rel, next_block, FLUX_VM_ALL_FROZEN))
			continue;

		state->current_block = next_block;
		return next_block;
	}

	state->current_block = state->nblocks;
	return InvalidBlockNumber;
}

/* Truncation lock-acquisition tuning, mirroring heap's lazy_truncate_heap. */
#define FLUX_TRUNCATE_LOCK_WAIT_INTERVAL	50		/* ms */
#define FLUX_TRUNCATE_LOCK_TIMEOUT			5000	/* ms */

/*
 * FluxPageIsEmpty
 *
 * A FLUX page is truncatable-empty when it holds no line pointer in normal
 * state.  This deliberately treats a page whose line-pointer array is present
 * but consists entirely of LP_UNUSED (and/or LP_DEAD) slots as empty, matching
 * the emptiness definition used by cross-page defragmentation (page_emptied):
 * defrag relocates every NORMAL tuple forward and leaves the source ItemIds
 * LP_UNUSED without resetting pd_lower, so PageGetMaxOffsetNumber() still
 * reports a nonzero count.  Testing max-offset alone would wrongly classify
 * such a defragmented tail page as non-empty and decline to truncate it,
 * leaking the trailing pages defrag just emptied.
 */
static bool
FluxPageIsEmpty(Page page)
{
	OffsetNumber maxoff;

	if (PageIsNew(page))
		return true;

	maxoff = PageGetMaxOffsetNumber(page);
	for (OffsetNumber offnum = FirstOffsetNumber; offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);

		if (ItemIdIsNormal(itemid))
			return false;
	}

	return true;
}

/*
 * FluxCountNondeletablePages
 *
 * Scan backwards from the end of the relation and return the block number of
 * the first (lowest-numbered) trailing page that is NOT empty, i.e. the number
 * of blocks the relation should be truncated to.  A page counts as empty when
 * it has no line pointers in normal state.
 *
 * The caller MUST hold AccessExclusiveLock on the relation so that no other
 * backend can add a tuple to a trailing page between this recount and the
 * subsequent RelationTruncate().  This is the FLUX analogue of heap's
 * count_nondeletable_pages() and is *necessary*, not optional: the forward
 * scan that computed empty_end_pages ran under ShareUpdateExclusiveLock, so a
 * concurrent INSERT could have populated a page that was empty at scan time.
 */
static BlockNumber
FluxCountNondeletablePages(Relation onerel)
{
	BlockNumber blkno = RelationGetNumberOfBlocks(onerel);

	while (blkno > 0)
	{
		Buffer		buf;
		Page		page;
		bool		empty;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(onerel, blkno - 1);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		empty = FluxPageIsEmpty(page);
		UnlockReleaseBuffer(buf);

		if (!empty)
			break;

		blkno--;
	}

	return blkno;
}

/*
 * FluxTruncateRelation
 *
 * Truncate trailing empty pages off a FLUX relation, mirroring heap's
 * lazy_truncate_heap() locking protocol.  We acquire AccessExclusiveLock
 * conditionally (giving up rather than blocking or deadlocking against the
 * lower-grade ShareUpdateExclusiveLock we already hold), re-verify under that
 * lock that the trailing pages are still empty, then truncate and release.
 *
 * Doing the truncation under only ShareUpdateExclusiveLock (as the previous
 * code did) races concurrent INSERT/extension: a backend could compute a
 * target block, then find that block removed by our smgrtruncate, and
 * ReadBuffer() it past EOF ("unexpected data beyond EOF in block N"), or worse
 * silently lose a tuple written to a page we truncate away.
 */
static void
FluxTruncateRelation(Relation onerel, BlockNumber orig_nblocks,
					  BlockNumber desired_nblocks, bool verbose)
{
	BlockNumber new_nblocks;
	int			lock_retry = 0;

	/*
	 * Acquire AccessExclusiveLock, retrying with a bounded timeout.  If a
	 * conflicting lock request arrives we give up truncating rather than
	 * block other backends or deadlock.
	 */
	while (!ConditionalLockRelation(onerel, AccessExclusiveLock))
	{
		CHECK_FOR_INTERRUPTS();

		if (++lock_retry > (FLUX_TRUNCATE_LOCK_TIMEOUT /
							FLUX_TRUNCATE_LOCK_WAIT_INTERVAL))
		{
			ereport(verbose ? INFO : DEBUG2,
					(errmsg("\"%s\": stopping truncate due to conflicting lock request",
							RelationGetRelationName(onerel))));
			return;
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 FLUX_TRUNCATE_LOCK_WAIT_INTERVAL,
						 WAIT_EVENT_VACUUM_TRUNCATE);
		ResetLatch(MyLatch);
	}

	/*
	 * Under the exclusive lock, re-check that the relation hasn't grown since
	 * the vacuum scan.  If it has, the new pages presumably hold live tuples;
	 * give up.
	 */
	if (RelationGetNumberOfBlocks(onerel) != orig_nblocks)
	{
		UnlockRelation(onerel, AccessExclusiveLock);
		return;
	}

	/*
	 * Rescan the tail under the exclusive lock to confirm the pages we intend
	 * to drop are still empty.  A concurrent INSERT under the lower-grade lock
	 * could have repopulated them after the vacuum scan.
	 */
	new_nblocks = FluxCountNondeletablePages(onerel);
	if (new_nblocks < desired_nblocks)
		new_nblocks = desired_nblocks;

	if (new_nblocks >= orig_nblocks)
	{
		/* Nothing to truncate after re-verification. */
		UnlockRelation(onerel, AccessExclusiveLock);
		return;
	}

	RelationTruncate(onerel, new_nblocks);

	/*
	 * Release the exclusive lock as soon as the truncation is done.  Other
	 * backends process the smgr invalidation smgrtruncate sent out when they
	 * next acquire a lock on the relation.
	 */
	UnlockRelation(onerel, AccessExclusiveLock);

	if (verbose)
		ereport(INFO, (errmsg("truncated \"%s\" from %u to %u pages",
							  RelationGetRelationName(onerel),
							  orig_nblocks, new_nblocks)));
}

/*
 * Vacuum a FLUX relation
 *
 * This performs garbage collection on a FLUX table in multiple phases:
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
flux_relation_vacuum(Relation onerel, const VacuumParams *params,
					  BufferAccessStrategy bstrategy)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	Buffer		buf;
	Page		page;
	uint64		oldest_ts;
	TransactionId oldest_xmin_vac;
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
	 * Initialize FLUX transaction state so that VACUUM's own start timestamp
	 * is registered in xact_start_ts_slots.  Without this,
	 * FluxGetOldestActiveTimestamp() would see no active transactions and
	 * fall back to the current wall clock (GetCurrentTimestamp()), which for
	 * the VM all-visible hint would over-eagerly mark tuples all-visible.
	 * (The reclamation gate itself is the XID horizon below, not this ts.)
	 */
	(void) FluxGetTransactionTimestamp();

	/*
	 * Get the oldest active transaction's start timestamp.  Deleted tuples
	 * whose commit timestamp is older than this are no longer visible to any
	 * running transaction and can safely be removed.
	 *
	 * Previously this called FluxGetCommitTimestamp() which returns (and
	 * advances) the current wall-clock time.  That was wrong: it made VACUUM
	 * consider almost all deleted tuples as reclaimable, even those still
	 * needed by long-running concurrent transactions.
	 */
	oldest_ts = FluxGetOldestActiveTimestamp();
	oldest_xmin_vac = FluxGetOldestXminHorizon(onerel);
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
	{
	ReadStream *scan_stream;
	FluxVacScanState scan_state;

	scan_state.rel = onerel;
	scan_state.current_block = InvalidBlockNumber;
	scan_state.nblocks = nblocks;
	scan_state.skip_all_frozen =
		!(params->options & VACOPT_DISABLE_PAGE_SKIPPING);

	scan_stream = read_stream_begin_relation(READ_STREAM_MAINTENANCE |
											 READ_STREAM_USE_BATCHING,
											 bstrategy,
											 onerel,
											 MAIN_FORKNUM,
											 flux_vac_scan_next_block,
											 &scan_state,
											 0);

	while ((buf = read_stream_next_buffer(scan_stream, NULL)) != InvalidBuffer)
	{
		OffsetNumber offnum,
					maxoffnum;
		ItemId		itemid;
		OffsetNumber dead_offsets[MaxOffsetNumber];
		int			ndead_on_page = 0;

		CHECK_FOR_INTERRUPTS();

		blkno = BufferGetBlockNumber(buf);
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
			FluxTupleHeader *tuple_hdr;

			itemid = PageGetItemId(page, offnum);

			/*
			 * LP_DEAD line pointers were produced by opportunistic pruning
			 * (FluxPagePruneOpt / UPDATE defrag-fit), which reclaims a
			 * committed-deleted tuple's storage but reserves its TID so it is
			 * not recycled while index entries still reference it.  VACUUM must
			 * record these TIDs for index cleanup (Phase II) before Phase III
			 * converts them to LP_UNUSED.  Their storage is already gone, so
			 * there is no tuple header to inspect.
			 */
			if (ItemIdIsDead(itemid))
			{
				if (do_index_cleanup)
				{
					dead_offsets[ndead_on_page++] = offnum;
					dead_tuples++;
				}
				continue;
			}

			/* Skip if not a normal tuple */
			if (!ItemIdIsNormal(itemid))
				continue;

			tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

			/* Skip overflow records - they are managed by tuple lifecycle */
			if (FluxIsOverflowRecordInline(tuple_hdr, ItemIdGetLength(itemid)))
				continue;

			num_tuples++;

			/*
			 * Check if tuple is deleted and old enough to be removed.
			 *
			 * With the sLog-based MVCC model, a deleted tuple can be vacuumed
			 * if: - FLUX_TUPLE_DELETED is set - FLUX_TUPLE_UNCOMMITTED is
			 * NOT set (committed delete) - commit_ts is older than the oldest
			 * active snapshot
			 *
			 * If UNCOMMITTED is still set, the deleting transaction is still
			 * in progress (or aborted but not yet cleaned up by the sLog
			 * callback).  Skip it.
			 */
			if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
			{
				if (tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED)
				{
					/* Transaction still in progress -- skip */
					live_tuples++;
				}
				else if (FluxTupleDeadToAll(tuple_hdr, oldest_xmin_vac))
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

	read_stream_end(scan_stream);
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
	{
		ReadStream *stream;
		BlockRangeReadStreamPrivate stream_private;

		stream_private.current_blocknum = 0;
		stream_private.last_exclusive = nblocks;

		stream = read_stream_begin_relation(READ_STREAM_MAINTENANCE |
											READ_STREAM_USE_BATCHING,
											bstrategy,
											onerel,
											MAIN_FORKNUM,
											block_range_read_stream_cb,
											&stream_private,
											0);

		while ((buf = read_stream_next_buffer(stream, NULL)) != InvalidBuffer)
		{
			OffsetNumber offnum,
						maxoffnum;
			ItemId		itemid;
			bool		page_has_dead_tuples = false;
			bool		page_modified = false;

			CHECK_FOR_INTERRUPTS();

			blkno = BufferGetBlockNumber(buf);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);

			if (PageIsNew(page))
			{
				UnlockReleaseBuffer(buf);
				continue;
			}

			maxoffnum = PageGetMaxOffsetNumber(page);

			/*
			 * First pass: clean overflow chains for dead tuples.  We must do
			 * this BEFORE defragmenting, because FluxPageDefragment removes
			 * dead item pointers and after that we can no longer identify
			 * which tuples had overflow data.  We temporarily drop the
			 * exclusive lock since overflow chain deletion may need to read
			 * and lock other pages.
			 */
			for (offnum = FirstOffsetNumber; offnum <= maxoffnum; offnum++)
			{
				FluxTupleHeader *tuple_hdr;

				itemid = PageGetItemId(page, offnum);

				/*
				 * LP_DEAD line pointers were already pruned (storage reclaimed)
				 * by opportunistic pruning and their TIDs recorded in Phase I
				 * for index cleanup, which has now run.  They carry no storage
				 * and no overflow chain to clean; just flag the page so the
				 * defragment pass below converts them to LP_UNUSED.
				 */
				if (ItemIdIsDead(itemid))
				{
					page_has_dead_tuples = true;
					continue;
				}

				if (!ItemIdIsNormal(itemid))
					continue;

				tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);
				if (FluxIsOverflowRecordInline(tuple_hdr, ItemIdGetLength(itemid)))
					continue;

				if (FluxTupleDeadToAll(tuple_hdr, oldest_xmin_vac))
				{
					page_has_dead_tuples = true;

					/*
					 * Delete any external TOAST datums referenced by this dead
					 * tuple before it is removed by defragmentation.  FLUX has no
					 * on-page overflow chains; wide values live in the standard
					 * heap TOAST table and are reclaimed here exactly as heap
					 * VACUUM reclaims them.  Only DELETED tuples are dead-to-all.
					 */
					if (tuple_hdr->t_infomask & FLUX_INFOMASK_HASEXTERNAL)
					{
						TupleDesc	vtupdesc = RelationGetDescr(onerel);
						Datum		vvalues[MaxTupleAttributeNumber];
						bool		visnull[MaxTupleAttributeNumber];
						FluxTupleData vtup;

						vtup.t_len = ItemIdGetLength(itemid);
						vtup.t_data = tuple_hdr;
						ItemPointerSet(&vtup.t_self, blkno, offnum);
						FluxDeformTuple(onerel, &vtup, vtupdesc, vvalues, visnull);

						LockBuffer(buf, BUFFER_LOCK_UNLOCK);
						flux_toast_delete(onerel, vvalues, visnull, false);
						LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
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
				 * Mark dead tuples as unused before defragmenting. The scan
				 * above already identified them; now set LP_UNUSED.  Index
				 * cleanup (Phase II) has removed every index entry pointing at
				 * these TIDs, so it is now safe to free the line pointers for
				 * recycling -- both the still-materialized DELETED tuples and
				 * the already-pruned LP_DEAD placeholders.
				 */
				for (offnum = FirstOffsetNumber; offnum <= maxoffnum; offnum++)
				{
					FluxTupleHeader *vac_hdr;

					itemid = PageGetItemId(page, offnum);

					if (ItemIdIsDead(itemid))
					{
						ItemIdSetUnused(itemid);
						continue;
					}

					if (!ItemIdIsNormal(itemid))
						continue;

					if (FluxIsOverflowRecordInline(PageGetItem(page, itemid),
											  ItemIdGetLength(itemid)))
						continue;

					vac_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);
					if (FluxTupleDeadToAll(vac_hdr, oldest_xmin_vac))
					{
						ItemIdSetUnused(itemid);
					}
				}

				FluxPageDefragment(page);
				page_modified = true;
				pages_vacuumed++;

				MarkBufferDirty(buf);

				if (RelationNeedsWAL(onerel))
				{
					XLogRecPtr	recptr;

					recptr = FluxXLogDefrag(onerel, buf, NULL, 0, oldest_ts);
					PageSetLSN(page, recptr);
				}

				END_CRIT_SECTION();
			}

			/* Update FSM with accurate free space information */
			if (page_modified || PageGetFreeSpace(page) > 0)
			{
				FluxRecordFreeSpace(onerel, blkno, PageGetFreeSpace(page));
			}

			/*
			 * Update the Visibility Map for this page.
			 *
			 * After defragmentation, check whether all remaining tuples on
			 * the page are visible to all transactions and/or frozen.  If so,
			 * set the appropriate VM bits.  This enables index-only scans to
			 * skip heap fetches and future VACUUMs to skip this page
			 * entirely.
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
					FluxTupleHeader *vm_tuple_hdr;

					vm_itemid = PageGetItemId(page, vm_offnum);
					if (!ItemIdIsNormal(vm_itemid))
						continue;

					/* Skip overflow records */
					if (FluxIsOverflowRecordInline(PageGetItem(page, vm_itemid),
											  ItemIdGetLength(vm_itemid)))
						continue;

					vm_tuple_hdr = (FluxTupleHeader *) PageGetItem(page, vm_itemid);

					/*
					 * Dead tuples (deleted) that survived defrag are recently
					 * dead
					 */
					if (vm_tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
					{
						all_visible = false;
						all_frozen = false;
						break;
					}

					/* Speculative tuples are not visible to all */
					if (vm_tuple_hdr->t_flags & FLUX_TUPLE_SPECULATIVE)
					{
						all_visible = false;
						all_frozen = false;
						break;
					}

					/*
					 * A live (not deleted/speculative/uncommitted -- checked
					 * above) tuple is all-visible AND all-frozen to every possible
					 * snapshot iff its inserting XID (t_xmin) has committed and
					 * precedes the oldest-xmin horizon, so no snapshot can fail to
					 * see it.  (The former test compared the whole t_commit_ts word
					 * against a timestamp horizon -- meaningless in the heap-shaped
					 * model where that word holds an XID, not an HLC timestamp;
					 * mirrors FluxTupleDeadToAll's XID-horizon gate.)
					 */
					if ((vm_tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED) ||
						!TransactionIdIsValid(vm_tuple_hdr->t_xmin) ||
						!TransactionIdDidCommit(vm_tuple_hdr->t_xmin) ||
						!TransactionIdPrecedes(vm_tuple_hdr->t_xmin, oldest_xmin_vac))
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

				FluxVMVacuumPage(onerel, buf, all_visible, all_frozen);
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

		read_stream_end(stream);
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
	FluxVacuumCrossPageDefrag(onerel, nblocks,
							   &empty_end_pages,
							   nindexes, indrels,
							   bstrategy);

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
	/* FLUX has no on-page overflow records to vacuum (wide values use TOAST). */

	/*
	 * -----------------------------------------------------------------------
	 * Phase IV-D: per-relation UNDO fork discard
	 *
	 * Reclaim space in the relation's UNDO fork by discarding pages whose
	 * records are all older than the oldest transaction that could still need
	 * them for rollback.  A fork page is discardable iff its max_xid (the
	 * largest urec_xid on the page) precedes the cluster-wide removable
	 * horizon, so no in-progress transaction can roll back into it.  Run this
	 * before truncation so freed pages are returned to the fork's free list.
	 * -----------------------------------------------------------------------
	 */
	RelUndoVacuum(onerel, GetOldestNonRemovableTransactionId(onerel), false);

	/*
	 * -----------------------------------------------------------------------
	 * Phase V: Truncation and final cleanup
	 * -----------------------------------------------------------------------
	 */

	/* Truncate empty pages at the end of the relation */
	if (empty_end_pages > 0 && (params->options & VACOPT_DISABLE_PAGE_SKIPPING) == 0)
	{
		BlockNumber new_nblocks = nblocks - empty_end_pages;

		/*
		 * Truncate under AccessExclusiveLock with a tail re-verification, so
		 * we never remove a block a concurrent INSERT is targeting.  The
		 * helper may truncate to fewer pages than requested (never more) or
		 * decline entirely if it can't get the lock or the tail is no longer
		 * empty.
		 */
		FluxTruncateRelation(onerel, nblocks, new_nblocks, verbose);
	}

	/* Update FSM for the entire relation using the real post-truncate size */
	FluxVacuumFSM(onerel, RelationGetNumberOfBlocks(onerel));

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
				(errmsg("FLUX vacuum \"%s\": found %lld tuples (%lld live, %lld dead), "
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

/* ================================================================
 * sLog transaction callbacks for FLUX
 *
 * These handle FLUX-specific page operations (clearing UNCOMMITTED flags,
 * marking aborted tuples as DELETED) at transaction boundaries.  They call
 * the generic SLogTuple* functions for shared-hash cleanup.
 * ================================================================
 */

/*
 * Two-phase commit record for FLUX.
 *
 * One record per tracked tuple is saved at PREPARE time via
 * RegisterTwoPhaseRecord().  When COMMIT PREPARED fires, the postcommit
 * callback uses these to locate and clear UNCOMMITTED flags.  When
 * ROLLBACK PREPARED fires, the postabort callback marks tuples as aborted.
 */
typedef struct FluxTwoPhaseRecord
{
	Oid			relid;
	ItemPointerData tid;
	bool		local_only;		/* INSERT-only: no shared sLog entry */
	SLogOpType	op_type;		/* INSERT, DELETE, or UPDATE */
}			FluxTwoPhaseRecord;

/*
 * Two-phase info values distinguishing the record kinds saved under
 * TWOPHASE_RM_FLUX_ID.  info==FLUX_2PC_SLOG carries a FluxTwoPhaseRecord
 * (per-tuple sLog/visibility state); info==FLUX_2PC_RELUNDO carries a
 * FluxRelUndoTwoPhaseRecord (a per-relation UNDO chain head).
 */
#define FLUX_2PC_SLOG		0
#define FLUX_2PC_RELUNDO	1

/*
 * Two-phase commit record for a FLUX per-relation UNDO chain head.
 *
 * One record per relation touched by the prepared transaction.  On ROLLBACK
 * PREPARED, flux_twophase_postabort() replays this chain via
 * RelUndoApplyChain() to restore in-place before-images -- the physical
 * data-restore that the per-tuple sLog records (which only flip visibility
 * flags) do not perform.
 */
typedef struct FluxRelUndoTwoPhaseRecord
{
	Oid			relid;
	RelUndoRecPtr start_urec_ptr;
}			FluxRelUndoTwoPhaseRecord;

/*
 * FluxEnsureSLogCallbacks -- register xact/subxact callbacks once per backend.
 */
void
FluxEnsureSLogCallbacks(void)
{
	if (!flux_slog_callbacks_registered)
	{
		RegisterXactCallback(FluxSLogXactCallback, NULL);
		RegisterSubXactCallback(FluxSLogSubXactCallback, NULL);
		flux_slog_callbacks_registered = true;
	}
}

/*
 * Callback for FluxProcessAbortedEntries: mark aborted INSERT tuples as
 * DELETED and remove the ABORTED sLog entry.
 *
 * After marking DELETED on page, we must remove the shared ABORTED sLog entry.
 * Otherwise, post-commit readers would find SLOG_OP_ABORTED and interpret it
 * as "a delete was aborted" (tuple still alive), rather than "an INSERT was
 * aborted" (tuple is dead).  With the sLog entry removed, readers see
 * DELETED + slog_nfound==0 → "deletion committed" → invisible.
 */
static bool
flux_process_aborted_cb(const SLogTupleKey *key,
						 TransactionId xid, TransactionId subxid,
						 bool local_only, void *arg)
{
	SLogTupleOp ops[SLOG_MAX_TUPLE_OPS];
	int			nfound;
	int			i;
	bool		has_aborted = false;

	/* Check if this entry has an ABORTED op */
	nfound = SLogTupleLookupFiltered(key->relid, (ItemPointer) &key->tid,
									 xid, ops, SLOG_MAX_TUPLE_OPS);
	for (i = 0; i < nfound; i++)
	{
		if (ops[i].op_type == SLOG_OP_ABORTED)
		{
			has_aborted = true;
			break;
		}
	}

	if (has_aborted)
	{
		Buffer		buf;
		Page		page;
		ItemId		itemid;
		FluxTupleHeader *tuple_hdr;
		OffsetNumber offnum;
		Relation	rel;

		rel = try_relation_open(key->relid, AccessShareLock);
		if (rel == NULL)
			return true;		/* continue iteration */
		buf = ReadBuffer(rel, ItemPointerGetBlockNumber((ItemPointer) &key->tid));
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		offnum = ItemPointerGetOffsetNumber((ItemPointer) &key->tid);

		if (offnum <= PageGetMaxOffsetNumber(page))
		{
			itemid = PageGetItemId(page, offnum);
			if (ItemIdIsNormal(itemid))
			{
				tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

				if (tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED)
				{
					tuple_hdr->t_flags |= FLUX_TUPLE_DELETED;
					tuple_hdr->t_flags &= ~FLUX_TUPLE_UNCOMMITTED;
					MarkBufferDirty(buf);
				}
			}
		}

		UnlockReleaseBuffer(buf);
		relation_close(rel, AccessShareLock);

		/*
		 * Remove the ABORTED sLog entry now that the page is marked DELETED.
		 * This ensures post-commit readers see DELETED + no sLog entries,
		 * which the visibility function correctly interprets as "deletion
		 * committed" (tuple invisible).
		 */
		SLogTupleRemove(key->relid, (ItemPointer) &key->tid, xid);
	}

	return true;				/* continue iteration */
}

/*
 * FluxProcessAbortedEntries -- at COMMIT, mark tuples from rolled-back
 * subtransactions as DELETED on their pages.
 */
static void
FluxProcessAbortedEntries(TransactionId xid)
{
	SLogTupleIterateTrackedKeys(xid, flux_process_aborted_cb, NULL);
}

/* ----------------------------------------------------------------
 * Batched commit-time UNCOMMITTED-flag clearing for
 * FluxClearUncommittedFlags.
 *
 * Commit visibility comes from CLOG (heap-shaped xmin/xmax MVCC); commit
 * does NOT stamp any timestamp.  This path only clears the on-page
 * FLUX_TUPLE_UNCOMMITTED hint flag so later readers skip the sLog
 * fast-path lookup.  t_xmin/t_xmax written by the DML stay untouched.
 *
 * The batched approach collects tracked keys, sorts by (relid, blockno),
 * and processes them with minimal buffer I/O:
 *   - One try_relation_open() per distinct relid
 *   - One ReadBuffer() per distinct block
 *   - Local-only INSERTs skip the shared sLog lookup entirely
 * ----------------------------------------------------------------
 */
/*
 * flux_cmp_tracked_key_by_block -- qsort comparator for batch commit stamping.
 *
 * Sorts by (relid, blockno, offnum) to enable sequential I/O: one
 * try_relation_open per relation, one ReadBuffer per distinct block.
 */
static int
flux_cmp_tracked_key_by_block(const void *a, const void *b)
{
	const		SLogTrackedKeyInfo *ka = (const SLogTrackedKeyInfo *) a;
	const		SLogTrackedKeyInfo *kb = (const SLogTrackedKeyInfo *) b;

	if (ka->key.relid < kb->key.relid)
		return -1;
	if (ka->key.relid > kb->key.relid)
		return 1;

	{
		BlockNumber ba = ItemPointerGetBlockNumber((ItemPointer) &ka->key.tid);
		BlockNumber bb = ItemPointerGetBlockNumber((ItemPointer) &kb->key.tid);

		if (ba < bb)
			return -1;
		if (ba > bb)
			return 1;
	}

	{
		OffsetNumber oa = ItemPointerGetOffsetNumber((ItemPointer) &ka->key.tid);
		OffsetNumber ob = ItemPointerGetOffsetNumber((ItemPointer) &kb->key.tid);

		if (oa < ob)
			return -1;
		if (oa > ob)
			return 1;
	}

	return 0;
}

/*
 * flux_stamp_tuple_committed -- stamp a single tuple at commit time.
 *
 * Applies the appropriate timestamp and clears FLUX_TUPLE_UNCOMMITTED
 * based on the tracked operation type.  Called from the batched commit path
 * with the buffer already locked exclusive.
 *
 * For local-only INSERTs (the common case for single-row INSERT), we skip
 * the expensive SLogTupleLookupFiltered() call.  A local-only INSERT can
 * only be savepoint-aborted if SLogTupleRemoveBySubXid created a shared
 * ABORTED entry — in which case local_only would have been cleared on the
 * tracked key.  So if local_only is still true, this is a live INSERT.
 */
static void
flux_stamp_tuple_committed(Buffer buf, OffsetNumber offnum,
							const SLogTrackedKeyInfo * tk,
							TransactionId xid)
{
	Page		page = BufferGetPage(buf);
	ItemId		itemid;
	FluxTupleHeader *tuple_hdr;
	SLogOpType	found_op_type;

	if (offnum > PageGetMaxOffsetNumber(page))
		return;
	itemid = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(itemid))
		return;

	tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

	/*
	 * Determine the effective operation type for this tuple.
	 *
	 * A tracked key whose op_type was flipped to SLOG_OP_ABORTED by
	 * SLogTupleRemoveBySubXid (savepoint rollback) must be skipped: its tuple
	 * is logically discarded and a shared ABORTED sLog entry already exists
	 * to enforce invisibility.  Clearing UNCOMMITTED here would resurrect it.
	 * This check must precede the local_only fast path because savepoint-
	 * aborted INSERTs keep local_only == true.
	 *
	 * For local-only entries (INSERTs with no shared sLog entry), skip the
	 * shared hash lookup entirely — this is the key optimization for INSERT
	 * workloads.
	 */
	if (tk->op_type == SLOG_OP_ABORTED)
	{
		found_op_type = SLOG_OP_ABORTED;
	}
	else if (tk->local_only)
	{
		/* Fast path: live local-only INSERT, no shared sLog lookup needed */
		found_op_type = SLOG_OP_INSERT;
	}
	else if (tk->op_type == SLOG_OP_INSERT ||
			 tk->op_type == SLOG_OP_UPDATE ||
			 tk->op_type == SLOG_OP_DELETE)
	{
		/* op_type was tracked correctly at insert time -- skip shared lookup */
		found_op_type = tk->op_type;
	}
	else
	{
		/* Fallback: unknown op_type, do the expensive lookup */
		SLogTupleOp ops[SLOG_MAX_TUPLE_OPS];
		int			nfound;
		int			i;

		found_op_type = SLOG_ENTRY_ABORTED_TXN;
		nfound = SLogTupleLookupFiltered(tk->key.relid,
										 (ItemPointer) &tk->key.tid,
										 xid, ops, SLOG_MAX_TUPLE_OPS);
		for (i = 0; i < nfound; i++)
		{
			if (TransactionIdEquals(ops[i].xid, xid))
			{
				found_op_type = ops[i].op_type;
				break;
			}
		}
	}

	/*
	 * Skip entries that were aborted by a savepoint rollback.
	 */
	if (found_op_type == SLOG_OP_ABORTED)
		return;

	/*
	 * For SLOG_ENTRY_ABORTED_TXN with a non-local entry, this means no shared
	 * entry was found — shouldn't happen for non-local, but treat as skip.
	 * For unrecognized op types, also skip.
	 */
	if (!tk->local_only && found_op_type == SLOG_ENTRY_ABORTED_TXN)
		return;
	if (found_op_type != SLOG_OP_INSERT &&
		found_op_type != SLOG_OP_UPDATE &&
		found_op_type != SLOG_OP_DELETE)
		return;

	/*
	 * Clear the UNCOMMITTED flag (a hint bit) for INSERT/UPDATE/DELETE
	 * tuples.  In the heap-shaped xmin/xmax model, commit does NOT stamp any
	 * timestamp: t_xmin (inserter) and t_xmax (deleter/updater, in the
	 * t_commit_ts word) were written by the DML and stay untouched.  Overwriting
	 * t_commit_ts here would clobber t_xmax with a bogus value and corrupt
	 * visibility (a garbage low-32-bits would be read as an xmax XID).  CLOG is
	 * the authoritative commit oracle; clearing the flag is a pure optimization
	 * so later readers skip the sLog fast-path lookup.
	 */
	if (tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED)
		tuple_hdr->t_flags &= ~FLUX_TUPLE_UNCOMMITTED;
}

/*
 * flux_batch_clear_uncommitted -- batch-process tracked keys at commit time.
 *
 * Processes the pre-sorted array of tracked keys with sequential I/O:
 *   - One try_relation_open() per distinct relid
 *   - One ReadBuffer() per distinct block within each relation
 *   - All tuples on the same page are stamped while holding one buffer lock
 *
 * This replaces the per-tuple callback pattern which did O(n) ReadBuffer
 * calls even when multiple tuples shared the same page.
 */
static void
flux_batch_clear_uncommitted(SLogTrackedKeyInfo * keys, int nkeys,
							  TransactionId xid)
{
	Oid			cur_relid = InvalidOid;
	BlockNumber cur_blkno = InvalidBlockNumber;
	Relation	rel = NULL;
	Buffer		buf = InvalidBuffer;
	int			i;

	/*
	 * Prefetch: issue advisory read-ahead for the first few distinct blocks.
	 * Since keys are sorted by (relid, blockno), we can scan ahead cheaply.
	 * This overlaps I/O with the kernel readahead path.
	 */
	if (nkeys > 0)
	{
		Oid			pf_relid = keys[0].key.relid;
		Relation	pf_rel;
		BlockNumber pf_prev = InvalidBlockNumber;
		int			pf_count = 0;

		pf_rel = try_relation_open(pf_relid, AccessShareLock);
		if (pf_rel != NULL)
		{
			for (int j = 0; j < nkeys && pf_count < 8; j++)
			{
				BlockNumber pf_blk;

				/* Stop prefetching if we cross into a different relation */
				if (keys[j].key.relid != pf_relid)
					break;

				pf_blk = ItemPointerGetBlockNumber((ItemPointer) &keys[j].key.tid);
				if (pf_blk != pf_prev)
				{
					PrefetchBuffer(pf_rel, MAIN_FORKNUM, pf_blk);
					pf_prev = pf_blk;
					pf_count++;
				}
			}
			relation_close(pf_rel, AccessShareLock);
		}
	}

	for (i = 0; i < nkeys; i++)
	{
		SLogTrackedKeyInfo *tk = &keys[i];
		Oid			relid = tk->key.relid;
		BlockNumber blkno = ItemPointerGetBlockNumber((ItemPointer) &tk->key.tid);
		OffsetNumber offnum = ItemPointerGetOffsetNumber((ItemPointer) &tk->key.tid);

		/* Switch relation if needed (sorted order minimizes switches) */
		if (relid != cur_relid)
		{
			/* Mark outgoing page dirty (once per page, not per tuple) */
			if (BufferIsValid(buf))
			{
				MarkBufferDirtyHint(buf, true);
				UnlockReleaseBuffer(buf);
				buf = InvalidBuffer;
			}
			if (rel != NULL)
			{
				relation_close(rel, AccessShareLock);
				rel = NULL;
			}

			rel = try_relation_open(relid, AccessShareLock);
			if (rel == NULL)
			{
				cur_relid = relid;
				cur_blkno = InvalidBlockNumber;
				continue;
			}
			cur_relid = relid;
			cur_blkno = InvalidBlockNumber;
		}

		if (rel == NULL)
			continue;

		/*
		 * Switch block if needed — amortizes ReadBuffer across same-page
		 * tuples
		 */
		if (blkno != cur_blkno)
		{
			/* Mark outgoing page dirty (once per page, not per tuple) */
			if (BufferIsValid(buf))
			{
				MarkBufferDirtyHint(buf, true);
				UnlockReleaseBuffer(buf);
				buf = InvalidBuffer;
			}

			PG_TRY();
			{
				buf = ReadBuffer(rel, blkno);
				LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			}
			PG_CATCH();
			{
				/*
				 * If ReadBuffer fails (e.g., relation truncated
				 * concurrently), skip this block and continue with the next.
				 */
				buf = InvalidBuffer;
				EmitErrorReport();
				FlushErrorState();
				cur_blkno = blkno;
				continue;
			}
			PG_END_TRY();

			cur_blkno = blkno;
		}

		if (!BufferIsValid(buf))
			continue;

		/* Stamp this tuple */
		flux_stamp_tuple_committed(buf, offnum, tk, xid);
	}

	/* Mark final page dirty and release */
	if (BufferIsValid(buf))
	{
		MarkBufferDirtyHint(buf, true);
		UnlockReleaseBuffer(buf);
	}
	if (rel != NULL)
		relation_close(rel, AccessShareLock);
}

/*
 * FluxClearUncommittedFlags -- proactively clear FLUX_TUPLE_UNCOMMITTED on
 * all tuples modified by the current transaction at PRE_COMMIT time.
 *
 * Uses a batched approach: collects all tracked keys into an array, sorts
 * by (relid, blockno) for sequential I/O, then processes them with at most
 * one ReadBuffer per distinct block and one try_relation_open per relation.
 *
 * For local-only INSERTs (the common single-row INSERT case), the expensive
 * SLogTupleLookupFiltered() call is skipped entirely — a local-only entry
 * that hasn't been promoted to a shared ABORTED entry is guaranteed to be
 * a live INSERT.
 *
 * Clearing the flag is a pure hint: commit visibility comes from CLOG
 * (heap-shaped xmin/xmax MVCC), not from any stamped timestamp.
 */
static void
FluxClearUncommittedFlags(TransactionId xid)
{
	SLogTrackedKeyInfo *keys;
	int			nkeys;

	/*
	 * When lazy clear is enabled, skip the expensive batch page-visit loop.
	 * The UNCOMMITTED flags will be cleared lazily by readers via the
	 * visibility functions when they next access these tuples.
	 */
	if (flux_lazy_uncommitted_clear)
		return;

	/* Collect tracked keys into a sortable array */
	nkeys = SLogTupleCollectTrackedKeys(xid, &keys);
	if (nkeys == 0)
	{
		pfree(keys);
		return;
	}

	/* Sort by (relid, blockno, offnum) for sequential I/O */
	qsort(keys, nkeys, sizeof(SLogTrackedKeyInfo), flux_cmp_tracked_key_by_block);

	/* Batch-process: one ReadBuffer per distinct block */
	flux_batch_clear_uncommitted(keys, nkeys, xid);

	pfree(keys);
}

/*
 * FluxSLogXactCallback -- clean up sLog entries at transaction end.
 */
/*
 * flux_register_twophase_cb -- callback for SLogTupleIterateTrackedKeys
 * during PREPARE.  Saves each tracked tuple as a two-phase record so that
 * COMMIT PREPARED / ROLLBACK PREPARED can find them.
 *
 * For local-only entries (INSERTs), also creates a shared sLog entry so
 * that other backends can see the transaction is in-progress and not treat
 * the UNCOMMITTED flag as "stale committed."
 */
static bool
flux_register_twophase_cb(const SLogTupleKey *key,
						   TransactionId xid, TransactionId subxid,
						   bool local_only, void *arg)
{
	FluxTwoPhaseRecord rec;

	rec.relid = key->relid;
	ItemPointerCopy(&key->tid, &rec.tid);
	rec.local_only = local_only;

	/*
	 * Determine op_type: look up in shared sLog if not local-only. For
	 * local-only entries (INSERTs), we know it's SLOG_OP_INSERT.
	 */
	if (local_only)
	{
		rec.op_type = SLOG_OP_INSERT;

		/*
		 * Promote local-only INSERT to a shared sLog entry.  This is critical
		 * for 2PC correctness: after PREPARE, the originating backend's local
		 * tracking is gone, but the tuple still has FLUX_TUPLE_UNCOMMITTED
		 * set.  Without a shared sLog entry, other backends would see
		 * slog_nfound==0 and return invisible (correct), but COMMIT PREPARED
		 * needs the entry for its postcommit callback to locate and finalize
		 * the tuple.
		 *
		 * With the shared entry, other backends will also find the XID in
		 * sLog, call TransactionIdIsInProgress() → true (prepared XIDs are
		 * still "in progress"), and correctly hide the tuple.
		 */
		SLogTupleInsertRecovery(key->relid, (ItemPointer) &key->tid,
								xid, SLOG_OP_INSERT);
	}
	else
	{
		SLogTupleOp ops[SLOG_MAX_TUPLE_OPS];
		int			nfound;
		int			i;

		rec.op_type = SLOG_OP_INSERT;	/* fallback */
		nfound = SLogTupleLookupFiltered(key->relid, (ItemPointer) &key->tid,
										 xid, ops, SLOG_MAX_TUPLE_OPS);
		for (i = 0; i < nfound; i++)
		{
			if (TransactionIdEquals(ops[i].xid, xid))
			{
				rec.op_type = ops[i].op_type;
				break;
			}
		}
	}

	RegisterTwoPhaseRecord(TWOPHASE_RM_FLUX_ID, FLUX_2PC_SLOG,
						   &rec, sizeof(FluxTwoPhaseRecord));
	return true;				/* continue iteration */
}

/*
 * flux_register_relundo_cb -- IteratePerRelUndo callback.
 *
 * Serializes one per-relation UNDO chain head into the 2PC state file so
 * ROLLBACK PREPARED can restore in-place before-images.
 */
static void
flux_register_relundo_cb(Oid relid, RelUndoRecPtr start_urec_ptr, void *arg)
{
	FluxRelUndoTwoPhaseRecord rec;

	if (!RelUndoRecPtrIsValid(start_urec_ptr))
		return;

	rec.relid = relid;
	rec.start_urec_ptr = start_urec_ptr;
	RegisterTwoPhaseRecord(TWOPHASE_RM_FLUX_ID, FLUX_2PC_RELUNDO,
						   &rec, sizeof(FluxRelUndoTwoPhaseRecord));
}

/*
 * AtPrepare_Flux -- register two-phase records for FLUX tuples.
 *
 * Called from PrepareTransaction() between StartPrepare() and EndPrepare(),
 * where RegisterTwoPhaseRecord() is valid.  Saves each tracked tuple so
 * that COMMIT PREPARED / ROLLBACK PREPARED can locate and finalize them.
 */
void
AtPrepare_Flux(void)
{
	TransactionId xid = GetCurrentTransactionIdIfAny();

	if (!TransactionIdIsValid(xid))
		return;

	SLogTupleIterateTrackedKeys(xid, flux_register_twophase_cb, NULL);

	/*
	 * Serialize the per-relation UNDO chain heads too.  The sLog records
	 * above carry only per-tuple visibility state; the physical before-image
	 * of an in-place UPDATE lives in the per-relation UNDO chain and must be
	 * replayed by RelUndoApplyChain() on ROLLBACK PREPARED.
	 */
	IteratePerRelUndo(flux_register_relundo_cb, NULL);

	/*
	 * Relocate this xact's oldest-active-timestamp pin from this backend's
	 * proc slot onto the prepared xact's dummy-proc slot, so the pin survives
	 * this backend running new transactions or exiting entirely, and so the
	 * (possibly different) backend that later runs COMMIT/ROLLBACK PREPARED
	 * can find and clear it.  MarkAsPreparing() has already assigned the dummy
	 * proc, so its slot number is available here.
	 */
	{
		FullTransactionId prep_fxid = GetTopFullTransactionIdIfAny();

		if (FullTransactionIdIsValid(prep_fxid))
			FluxPrepareReassignSlot(TwoPhaseGetDummyProcNumber(prep_fxid, false));
	}
}

static void
FluxSLogXactCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
			{
				TransactionId xid = GetCurrentTransactionIdIfAny();

				if (TransactionIdIsValid(xid))
				{
					/*
					 * Ordering here is load-bearing for write-write
					 * correctness.  Three steps, in this exact order:
					 *
					 * 1. FluxProcessAbortedEntries -- mark pages DELETED for
					 * subxact-aborted ops and remove their ABORTED sLog
					 * entries.  MUST run before SLogTupleCommitByXid, whose
					 * COMMIT_XID apply removes every non-UPDATE op for the
					 * xid; running it first would erase the ABORTED marker
					 * this step needs and resurrect an aborted tuple.
					 *
					 * 2. SLogTupleCommitByXid -- finalize the xid's sLog ops.
					 *
					 * 3. FluxClearUncommittedFlags -- clear the on-page
					 * UNCOMMITTED hint flag.  Commit visibility itself comes
					 * from CLOG (heap-shaped xmin/xmax MVCC); clearing the
					 * flag is a pure hint so later readers skip the sLog
					 * fast-path lookup.
					 *
					 * All three run at PRE_COMMIT, before the point of no
					 * return: DSA allocation must stay in an abort-legal phase
					 * to avoid a post-commit OOM->PANIC.
					 */
					FluxProcessAbortedEntries(xid);
					SLogTupleCommitByXid(xid);
					FluxClearUncommittedFlags(xid);
				}
			}
			break;

		case XACT_EVENT_PRE_PREPARE:
			{
				TransactionId xid = GetCurrentTransactionIdIfAny();

				if (TransactionIdIsValid(xid))
				{
					/*
					 * At PREPARE, we must NOT clear UNCOMMITTED flags.  The
					 * transaction is not yet committed and another backend might
					 * ROLLBACK PREPARED.
					 *
					 * We still need to process any subtransaction-aborted
					 * entries (mark them DELETED on page) since those are
					 * definitively aborted regardless of PREPARE outcome.
					 *
					 * Two-phase record registration happens in
					 * AtPrepare_Flux(), called from PrepareTransaction()
					 * after StartPrepare().
					 */
					FluxProcessAbortedEntries(xid);
				}
			}
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			{
				/*
				 * The UNCOMMITTED-flag clear and before-image handling already
				 * happened at XACT_EVENT_PRE_COMMIT (see above).  Here we only
				 * release backend-local tracking state, which is safe in the
				 * post-commit no-abort region.  Commit visibility comes from
				 * CLOG (heap-shaped xmin/xmax MVCC); nothing further is written
				 * at COMMIT.
				 */
				SLogTupleResetTracking();
			}
			break;

		case XACT_EVENT_PREPARE:
			{
				/*
				 * At PREPARE completion, do NOT remove sLog entries or
				 * decrement dirty map counters.  The sLog entries must
				 * persist so that visibility checks can see the transaction
				 * is still in-progress (prepared).  Only discard the
				 * backend-local tracking list since this backend is done.
				 *
				 * The two-phase records registered during PRE_PREPARE will be
				 * used by COMMIT PREPARED / ROLLBACK PREPARED to perform the
				 * actual cleanup.
				 */
				SLogTupleResetTracking();
			}
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			{
				TransactionId xid = GetCurrentTransactionIdIfAny();

				if (TransactionIdIsValid(xid))
				{
					/*
					 * Mark this transaction's shared sLog ops ABORTED.  The
					 * DSA before-images are freed inside SLogTupleMarkAborted
					 * under the partition writer lock (single-owner), so we
					 * must not free them here: an unlocked free races the
					 * UNDO worker's SLogTupleRemoveByXidGlobal and corrupts
					 * the DSA heap.
					 */
					SLogTupleMarkAborted(xid);
				}

				/*
				 * The dirty map is grow-only and carries no per-transaction
				 * tracking, so there is nothing to undo at ABORT.  Leaving the
				 * page bit set is correct: the sLog entries are marked ABORTED
				 * (not removed), so a scanner must still probe the sLog to
				 * detect the aborted state and treat the tuples as still-live.
				 */
				SLogTupleResetTracking();
			}
			break;

		default:
			break;
	}
}

/*
 * flux_restore_before_image_cb -- callback for FluxRestoreBeforeImages.
 *
 * For each tracked key with a before-image in the rolled-back subtransaction,
 * physically restore the original tuple data on the page.
 */
static bool
flux_restore_before_image_cb(const SLogTupleKey *key,
							  TransactionId xid, TransactionId subxid,
							  bool local_only, void *arg)
{
	char	   *before_data;
	int			before_len;
	uint16		before_flags;
	uint64		before_commit_ts;
	RelFileLocator before_rlocator;
	char		before_relpersistence;

	/* Check if this tracked key has a before-image */
	if (!SLogTupleGetBeforeImage(key->relid, (ItemPointer) &key->tid,
								 xid, subxid,
								 &before_data, &before_len,
								 &before_flags, &before_commit_ts,
								 &before_rlocator, &before_relpersistence))
		return true;			/* No before-image (INSERT), continue */

	/*
	 * Restore the physical tuple on the page.
	 *
	 * This callback fires from SUBXACT_EVENT_ABORT_SUB, by which point the
	 * subtransaction is already in TRANS_ABORT state (AbortSubTransaction
	 * sets the state before invoking subxact callbacks).  That makes any
	 * relcache access -- relation_open and friends -- unsafe: it would trip
	 * the IsTransactionState() assertion in AssertCouldGetRelation.  We
	 * therefore go straight to the buffer manager using the RelFileLocator
	 * captured at store time, which needs no relcache and works regardless of
	 * transaction state (including proc_exit teardown).
	 */
	{
		Buffer		buf = InvalidBuffer;
		Page		page;
		ItemId		itemid;
		FluxTupleHeader *tuple_hdr;
		OffsetNumber offnum;
		bool		permanent = (before_relpersistence == RELPERSISTENCE_PERMANENT);

		PG_TRY();
		{
			buf = ReadBufferWithoutRelcache(before_rlocator, MAIN_FORKNUM,
											ItemPointerGetBlockNumber((ItemPointer) &key->tid),
											RBM_NORMAL, NULL, permanent);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);
			offnum = ItemPointerGetOffsetNumber((ItemPointer) &key->tid);

			if (offnum <= PageGetMaxOffsetNumber(page))
			{
				itemid = PageGetItemId(page, offnum);
				if (ItemIdIsNormal(itemid))
				{
					tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

					/*
					 * Restore the tuple to its pre-DML state.
					 *
					 * The before-image was the original occupant of this item
					 * slot.  For Strategy 1 in-place updates (new tuple
					 * smaller than old), the physical page space at this
					 * offset hasn't been reclaimed or compacted within the
					 * same subtransaction, so writing before_len bytes is
					 * safe even when before_len > ItemIdGetLength.
					 */
					memcpy(tuple_hdr, before_data, before_len);

					/* Update item length if size changed */
					if (before_len != (int) ItemIdGetLength(itemid))
						ItemIdSetNormal(itemid, ItemIdGetOffset(itemid),
										before_len);

					MarkBufferDirtyHint(buf, true);
				}
			}

			UnlockReleaseBuffer(buf);
			buf = InvalidBuffer;
		}
		PG_CATCH();
		{
			if (BufferIsValid(buf))
				UnlockReleaseBuffer(buf);
			EmitErrorReport();
			FlushErrorState();
		}
		PG_END_TRY();
	}

	return true;				/* continue iteration */
}

/*
 * FluxRestoreBeforeImages -- on savepoint rollback, restore physical tuples
 * that were modified by the rolled-back subtransaction.
 *
 * Iterates tracked keys for the given (xid, subxid) and for each one that
 * has a stashed before-image, reads the buffer and restores the tuple data.
 * This must be called BEFORE SLogTupleRemoveBySubXid (which marks sLog
 * entries as ABORTED) so that the tracked key list still has the subxid.
 */
static void
FluxRestoreBeforeImages(TransactionId xid, SubTransactionId subxid)
{
	SLogTupleIterateTrackedKeysForSubXid(xid, subxid,
										 flux_restore_before_image_cb,
										 NULL);
}

/*
 * FluxSLogSubXactCallback -- handle subtransaction events.
 */
static void
FluxSLogSubXactCallback(SubXactEvent event,
						 SubTransactionId mySubid,
						 SubTransactionId parentSubid,
						 void *arg)
{
	TransactionId xid;

	switch (event)
	{
		case SUBXACT_EVENT_ABORT_SUB:
			xid = GetTopTransactionIdIfAny();
			if (TransactionIdIsValid(xid))
			{
				/*
				 * Restore physical tuples from before-images FIRST, while the
				 * tracked key list still has entries for this subxid. Then
				 * mark sLog entries as ABORTED for visibility.
				 */
				FluxRestoreBeforeImages(xid, mySubid);
				SLogTupleRemoveBySubXid(xid, mySubid);
			}

			/*
			 * The dirty map is grow-only with no per-subtransaction tracking,
			 * so there is nothing to undo at subtransaction abort.  Leaving
			 * the page bit set is correct: SLogTupleRemoveBySubXid marks
			 * entries SLOG_OP_ABORTED (does not remove them), so a scanner
			 * must still probe the sLog to detect the aborted state.
			 */
			break;

		case SUBXACT_EVENT_COMMIT_SUB:
			xid = GetTopTransactionIdIfAny();
			if (TransactionIdIsValid(xid))
				SLogTupleUpdateSubXid(xid, mySubid, parentSubid);
			break;

		default:
			break;
	}
}

/* ================================================================
 * Two-phase commit callbacks for FLUX
 *
 * These are invoked by FinishPreparedTransaction() in the backend that
 * runs COMMIT PREPARED or ROLLBACK PREPARED.  They perform the tuple-level
 * cleanup that would normally happen at XACT_EVENT_PRE_COMMIT / COMMIT
 * or ABORT in the originating backend.
 * ================================================================
 */

/*
 * flux_twophase_postcommit -- called for each saved record when
 * COMMIT PREPARED resolves a prepared transaction.
 *
 * Clears FLUX_TUPLE_UNCOMMITTED and removes
 * shared sLog entries (for DELETE/UPDATE operations).
 */
void
flux_twophase_postcommit(FullTransactionId fxid, uint16 info,
						  void *recdata, uint32 len)
{
	FluxTwoPhaseRecord *rec = (FluxTwoPhaseRecord *) recdata;
	TransactionId xid = XidFromFullTransactionId(fxid);
	Buffer		buf = InvalidBuffer;
	Page		page;
	ItemId		itemid;
	FluxTupleHeader *tuple_hdr;
	OffsetNumber offnum;
	Relation	rel;

	/*
	 * Release this prepared xact's oldest-active-timestamp pin (moved onto the
	 * dummy-proc slot at PREPARE).  Idempotent, so running it for every record
	 * is harmless.  Runs under TwoPhaseStateLock held by
	 * FinishPreparedTransaction(), so look the dummy proc up with lock_held.
	 */
	FluxResolvePreparedSlot(TwoPhaseGetDummyProcNumber(fxid, true));

	/*
	 * Per-relation UNDO chain heads (FLUX_2PC_RELUNDO) are irrelevant on
	 * COMMIT PREPARED: the committed in-place data is already correct, the
	 * chain is simply discarded.  Only the per-tuple sLog records need work.
	 */
	if (info == FLUX_2PC_RELUNDO)
		return;

	Assert(len == sizeof(FluxTwoPhaseRecord));

	/*
	 * Heap-shaped COMMIT PREPARED: nothing to stamp.  t_xmin/t_xmax were
	 * written by the prepared DML and stay untouched; CLOG (updated by the
	 * 2PC machinery) makes the committed XID visible.  We only clear the
	 * UNCOMMITTED hint-bit flag below.
	 */

	rel = try_relation_open(rec->relid, AccessShareLock);
	if (rel == NULL)
		return;					/* relation dropped before COMMIT PREPARED */

	PG_TRY();
	{
		buf = ReadBuffer(rel, ItemPointerGetBlockNumber(&rec->tid));
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		offnum = ItemPointerGetOffsetNumber(&rec->tid);

		if (offnum <= PageGetMaxOffsetNumber(page))
		{
			itemid = PageGetItemId(page, offnum);
			if (ItemIdIsNormal(itemid))
			{
				tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

				/* Clear UNCOMMITTED flag (hint bit); do NOT touch t_commit_ts. */
				if (tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED)
					tuple_hdr->t_flags &= ~FLUX_TUPLE_UNCOMMITTED;

				MarkBufferDirtyHint(buf, true);
			}
		}

		UnlockReleaseBuffer(buf);
		buf = InvalidBuffer;
		relation_close(rel, AccessShareLock);
		rel = NULL;
	}
	PG_CATCH();
	{
		if (BufferIsValid(buf))
			UnlockReleaseBuffer(buf);
		if (rel != NULL)
			relation_close(rel, AccessShareLock);
		EmitErrorReport();
		FlushErrorState();
	}
	PG_END_TRY();

	/*
	 * Remove shared sLog entry for this tuple.  At PREPARE time, we promoted
	 * local-only entries to shared (via SLogTupleInsertRecovery), so ALL
	 * entries now have a shared sLog entry that needs cleanup.
	 */
	SLogTupleRemoveByXidSingle(rec->relid, &rec->tid, xid);
}

/*
 * flux_twophase_postabort -- called for each saved record when
 * ROLLBACK PREPARED resolves a prepared transaction.
 *
 * For INSERTs: marks the tuple as DELETED (the insert is rolled back).
 * For DELETEs/UPDATEs: marks the sLog entry as ABORTED (the operation
 * is undone, tuple remains/reverts to live).
 */
void
flux_twophase_postabort(FullTransactionId fxid, uint16 info,
						 void *recdata, uint32 len)
{
	FluxTwoPhaseRecord *rec = (FluxTwoPhaseRecord *) recdata;
	TransactionId xid = XidFromFullTransactionId(fxid);
	Buffer		buf = InvalidBuffer;
	Page		page;
	ItemId		itemid;
	FluxTupleHeader *tuple_hdr;
	OffsetNumber offnum;
	Relation	rel;

	/*
	 * Release this prepared xact's oldest-active-timestamp pin (moved onto the
	 * dummy-proc slot at PREPARE).  Idempotent and lock-held, as in
	 * flux_twophase_postcommit(); must precede the early returns below.
	 */
	FluxResolvePreparedSlot(TwoPhaseGetDummyProcNumber(fxid, true));

	/*
	 * Per-relation UNDO chain head (FLUX_2PC_RELUNDO): replay the chain to
	 * restore in-place before-images.  This is the physical data-restore for
	 * an aborted in-place UPDATE -- the per-tuple sLog records below only flip
	 * visibility flags.  We run in the finishing backend's own live
	 * transaction (COMMIT/ROLLBACK PREPARED), so relation_open and the chain
	 * walk are legal here without the TRANS_ABORT juggling ApplyPerRelUndo
	 * needs.
	 */
	if (info == FLUX_2PC_RELUNDO)
	{
		FluxRelUndoTwoPhaseRecord *urec =
			(FluxRelUndoTwoPhaseRecord *) recdata;
		Relation	urel;

		Assert(len == sizeof(FluxRelUndoTwoPhaseRecord));

		urel = try_relation_open(urec->relid, RowExclusiveLock);
		if (urel == NULL)
			return;				/* relation dropped */

		PG_TRY();
		{
			RelUndoApplyChain(urel, urec->start_urec_ptr);
		}
		PG_CATCH();
		{
			BufferLockReleaseAll();
			EmitErrorReport();
			FlushErrorState();
		}
		PG_END_TRY();

		relation_close(urel, RowExclusiveLock);
		return;
	}

	Assert(len == sizeof(FluxTwoPhaseRecord));

	rel = try_relation_open(rec->relid, AccessShareLock);
	if (rel == NULL)
		return;					/* relation dropped */

	PG_TRY();
	{
		buf = ReadBuffer(rel, ItemPointerGetBlockNumber(&rec->tid));
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		offnum = ItemPointerGetOffsetNumber(&rec->tid);

		if (offnum <= PageGetMaxOffsetNumber(page))
		{
			itemid = PageGetItemId(page, offnum);
			if (ItemIdIsNormal(itemid))
			{
				tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

				if (rec->op_type == SLOG_OP_INSERT)
				{
					/*
					 * Aborted INSERT: keep UNCOMMITTED set.  The shared sLog
					 * entry is marked ABORTED below, so the visibility code
					 * path at flux_mvcc.c:1008 will see UNCOMMITTED +
					 * SLOG_OP_ABORTED and goto not_visible.
					 *
					 * We do NOT clear UNCOMMITTED or set DELETED here,
					 * because that combination (committed-looking tuple with
					 * DELETED + ABORTED sLog) causes the deletion-check path
					 * to incorrectly reverse the deletion and make the tuple
					 * visible.
					 *
					 * The UNDO worker / VACUUM will eventually physically
					 * remove the dead tuple by recognizing the UNCOMMITTED +
					 * ABORTED pattern.
					 */
				}
				else
				{
					/*
					 * Aborted DELETE/UPDATE: the tuple reverts to its pre-DML
					 * state.  Clear any flags set by the aborted operation.
					 * For DELETE, remove the DELETED flag. For UPDATE, remove
					 * UPDATED flag and any UNCOMMITTED.
					 */
					if (rec->op_type == SLOG_OP_DELETE)
					{
						if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
						{
							tuple_hdr->t_flags &= ~FLUX_TUPLE_DELETED;
							MarkBufferDirtyHint(buf, true);
						}
					}
					else if (rec->op_type == SLOG_OP_UPDATE)
					{
						if (tuple_hdr->t_flags & FLUX_TUPLE_UPDATED)
						{
							tuple_hdr->t_flags &= ~FLUX_TUPLE_UPDATED;
							tuple_hdr->t_flags &= ~FLUX_TUPLE_UNCOMMITTED;
							MarkBufferDirtyHint(buf, true);
						}
					}
				}
			}
		}

		UnlockReleaseBuffer(buf);
		buf = InvalidBuffer;
		relation_close(rel, AccessShareLock);
		rel = NULL;
	}
	PG_CATCH();
	{
		if (BufferIsValid(buf))
			UnlockReleaseBuffer(buf);
		if (rel != NULL)
			relation_close(rel, AccessShareLock);
		EmitErrorReport();
		FlushErrorState();
	}
	PG_END_TRY();

	/*
	 * Mark shared sLog entry as ABORTED.  At PREPARE time, we promoted all
	 * local-only entries to shared, so every entry has a shared sLog entry.
	 * Marking it ABORTED ensures visibility code correctly hides the tuple
	 * until UNDO/VACUUM removes it.
	 */
	SLogTupleMarkAbortedSingle(rec->relid, &rec->tid, xid);
}

/*
 * flux_twophase_recover -- called during startup recovery for each
 * saved FLUX record in a prepared transaction's state file.
 *
 * During recovery, we don't need to do anything special: the tuples
 * are already in their prepared-but-uncommitted state on disk (with
 * UNCOMMITTED flag set for INSERTs, or sLog entries for DELETE/UPDATE).
 * The postcommit/postabort callbacks will handle cleanup when the
 * prepared transaction is eventually resolved.
 */
void
flux_twophase_recover(FullTransactionId fxid, uint16 info,
					   void *recdata, uint32 len)
{
	/* Nothing to do during recovery -- state is already consistent on disk */
}
