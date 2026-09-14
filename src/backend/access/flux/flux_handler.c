/*-------------------------------------------------------------------------
 *
 * flux_handler.c
 *	  FLUX table access method handler
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_handler.c
 *
 * NOTES
 *	  This file implements the FLUX table access method, which provides
 *	  time-based MVCC with in-place updates, overflow pages for large
 *	  attributes, compression, and advanced space management.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/detoast.h"
#include "access/flux.h"
#include "access/flux_dirtymap.h"
#include "access/flux_pbu.h"
#include "access/slog.h"
#include "access/flux_xlog.h"
#include "access/tableam.h"
#include "access/tableam_indexscan.h"
#include "access/undobuffer.h"
#include "access/tsmapi.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/progress.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/tidbitmap.h"
#include "utils/backend_progress.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/tuplesort.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/read_stream.h"
#include "storage/smgr.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * The shared TID-bitmap ceiling (TBM_MAX_TUPLES_PER_PAGE, nodes/tidbitmap.h)
 * is sized for the widest table AM's line-pointer packing.  FLUX does not
 * clamp offsets to MaxHeapTuplesPerPage (FluxPageAddTuple omits PAI_IS_HEAP),
 * so a FLUX bitmap index scan can feed offsets up to MaxFluxItemsPerPage into
 * tbm_add_tuples()/tbm_extract_page_tuple().  tidbitmap.h spells the FLUX
 * bound out from page geometry to avoid pulling access/flux.h into that
 * widely-included header; guard that literal bound against the real
 * MaxFluxItemsPerPage here so any geometry change is caught at compile time.
 */
StaticAssertDecl(MaxFluxItemsPerPage <= TBM_MAX_TUPLES_PER_PAGE,
				 "TBM_MAX_TUPLES_PER_PAGE must cover MaxFluxItemsPerPage");

/* Forward declarations */
static void flux_prepare_pagescan(FluxScanDesc scan, Buffer buffer);
static BlockNumber flux_scan_stream_read_next(ReadStream *stream,
											  void *callback_private_data,
											  void *per_buffer_data);
static BlockNumber flux_bitmap_stream_read_next(ReadStream *stream,
												void *callback_private_data,
												void *per_buffer_data);
static bool flux_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream);
static bool flux_scan_analyze_next_tuple(TableScanDesc scan,
										 double *liverows, double *deadrows,
										 TupleTableSlot *slot);
static void flux_scan_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
								   ItemPointer maxtid);
static bool flux_scan_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
										   TupleTableSlot *slot);
static bool flux_scan_bitmap_next_tuple(TableScanDesc scan,
										TupleTableSlot *slot,
										bool *recheck,
										uint64 *lossy_pages,
										uint64 *exact_pages);

/* Include operations from other modules */
extern void flux_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
							  uint32 options, BulkInsertState bistate);
extern TM_Result flux_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
								   uint32 options, Snapshot snapshot, Snapshot crosscheck,
								   bool wait, TM_FailureData *tmfd);
extern TM_Result flux_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
								   CommandId cid, uint32 options,
								   Snapshot snapshot, Snapshot crosscheck,
								   bool wait, TM_FailureData *tmfd,
								   LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes);
extern void flux_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
							  CommandId cid, uint32 options, BulkInsertState bistate);
extern void flux_relation_vacuum(Relation onerel, const VacuumParams *params,
								 BufferAccessStrategy bstrategy);

/*
 * Read stream callback for sequential scan prefetching.
 *
 * Returns the next block number to read ahead for the sequential scan.
 * The read_stream infrastructure will prefetch these blocks asynchronously,
 * reducing I/O wait time for cold data.
 *
 * Uses rs_prefetch_block (separate from rs_cblock) to track the prefetch
 * position independently of the scan's current position.
 */
static BlockNumber
flux_scan_stream_read_next(ReadStream *stream,
						   void *callback_private_data,
						   void *per_buffer_data)
{
	FluxScanDesc scan = (FluxScanDesc) callback_private_data;
	BlockNumber block;

	block = scan->rs_prefetch_block;
	if (block >= scan->rs_nblocks)
		return InvalidBlockNumber;

	scan->rs_prefetch_block = block + 1;
	return block;
}

/*
 * Read stream callback for bitmap heap scans.
 *
 * Pulls the next block from the TBM iterator and hands it to the read
 * stream so upcoming bitmap pages are prefetched asynchronously, matching
 * the HEAP bitmapheap_stream_read_next() behaviour.  The TBMIterateResult
 * for each block is stashed in per_buffer_data so the consumer can read
 * lossy/recheck flags and the exact tuple offsets without re-iterating.
 */
static BlockNumber
flux_bitmap_stream_read_next(ReadStream *stream,
							 void *callback_private_data,
							 void *per_buffer_data)
{
	FluxScanDesc scan = (FluxScanDesc) callback_private_data;
	TableScanDesc sscan = &scan->rs_base;
	TBMIterateResult *tbmres = per_buffer_data;

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/* no more entries in the bitmap */
		if (!tbm_iterate(&sscan->st.rs_tbmiterator, tbmres))
			return InvalidBlockNumber;

		/*
		 * Ignore any claimed entries past what we think is the end of the
		 * relation.  It may have been extended after the start of our scan.
		 * Skip this optimization under SERIALIZABLE, where all
		 * index-reachable tuples must be examined for conflict detection.
		 */
		if (!IsolationIsSerializable() &&
			tbmres->blockno >= scan->rs_nblocks)
			continue;

		return tbmres->blockno;
	}

	Assert(false);
	return InvalidBlockNumber;
}

/*
 * ------------------------------------------------------------------------
 * Slot related callbacks for FLUX AM
 * ------------------------------------------------------------------------
 */

/*
 * Return slot implementation suitable for storing FLUX tuples
 */
static const TupleTableSlotOps *
flux_slot_callbacks(Relation relation)
{
	(void) relation;
	return &TTSOpsFluxTuple;
}

/*
 * flux_begin_bulk_insert - Signal the start of a DML operation.
 *
 * Activates the Tier-2 UNDO write buffer for this relation.  When active,
 * per-tuple UNDO records are batched via UndoBufferAddRecord() and flushed
 * in larger XLOG_UNDO_BATCH WAL records (overflow path), reducing per-row
 * WAL overhead significantly for COPY and multi-insert.
 *
 * The overflow-flush path emits standalone XLOG_UNDO_BATCH records which
 * the revert-worker's UndoReadBatchFromWAL() can walk for any AM.
 */
static void
flux_begin_bulk_insert(Relation rel, uint32 options, int64 nrows)
{
	(void) options;

	UndoBufferBegin(rel, nrows);
}

/*
 * flux_finish_bulk_insert - Complete a DML operation.
 *
 * Flushes any pending UNDO records and deactivates the write buffer.
 */
static void
flux_finish_bulk_insert(Relation rel, uint32 options)
{
	(void) options;

	UndoBufferEnd(rel);
}

/*
 * ------------------------------------------------------------------------
 * Table scan callbacks for FLUX AM
 * ------------------------------------------------------------------------
 */

/*
 * Start a scan of the FLUX relation
 */
static TableScanDesc
flux_scan_begin(Relation relation, Snapshot snapshot,
				int nkeys, ScanKey key,
				ParallelTableScanDesc pscan,
				uint32 flags)
{
	FluxScanDesc scan;


	scan = (FluxScanDesc) palloc0(sizeof(FluxScanDescData));

	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_key = key;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = pscan;

	scan->rs_cbuf = InvalidBuffer;
	scan->rs_cblock = InvalidBlockNumber;
	scan->rs_nblocks = RelationGetNumberOfBlocks(relation);
	scan->rs_startblock = 0;
	scan->rs_coffset = FirstOffsetNumber;
	scan->rs_cindex = InvalidOffsetNumber;
	scan->rs_inited = false;
	scan->rs_ntuples = 0;
	scan->rs_vistuples = NULL;
	scan->rs_vm_buffer = InvalidBuffer;
	scan->rs_vm_blockno = InvalidBlockNumber;

	/*
	 * SSI: a seqscan (or samplescan) reads the whole relation, so under
	 * SERIALIZABLE it must take a relation-level SIREAD predicate lock, exactly
	 * as heap_beginscan does.  Per-tuple PredicateLockTID() in the page path
	 * only covers tuples that are actually returned, so without this a
	 * seqscan-based read left no trace for a concurrent writer to conflict
	 * against and FLUX silently missed write-skew anomalies that heap catches
	 * ("could not serialize access due to read/write dependencies among
	 * transactions").
	 *
	 * Predicate locking is a no-op outside SERIALIZABLE; the Assert mirrors
	 * heap's, so a missing snapshot is still caught in every isolation level.
	 */
	if (scan->rs_base.rs_flags & (SO_TYPE_SEQSCAN | SO_TYPE_SAMPLESCAN))
	{
		Assert(snapshot);
		PredicateLockRelation(relation, snapshot);
	}

	/* Allocate parallel scan worker data if doing a parallel scan */
	if (pscan != NULL)
		scan->rs_parallelworkerdata = palloc_object(ParallelBlockTableScanWorkerData);
	else
		scan->rs_parallelworkerdata = NULL;

	/* Set up MVCC bookkeeping timestamps (visibility uses xmin/xmax + CLOG) */
	scan->rs_snapshot_ts = GetCurrentTimestamp();
	scan->rs_xact_ts = GetCurrentTimestamp();

	/*
	 * Initialize read stream for sequential prefetching (non-parallel only).
	 * The read stream uses the kernel readahead and our callback to prefetch
	 * upcoming pages, reducing I/O wait time for cold sequential scans.
	 * Parallel scans use their own block coordination, so skip the stream.
	 */
	scan->rs_prefetch_block = 0;
	if (flags & SO_TYPE_BITMAPSCAN)
	{
		/*
		 * Bitmap scans drive the stream from the TBM iterator.  The iterator
		 * is attached by the executor after beginscan, so the callback only
		 * runs once read_stream_next_buffer() is first called.  Per-buffer
		 * data carries the TBMIterateResult for each prefetched block.
		 */
		scan->rs_read_stream = read_stream_begin_relation(READ_STREAM_DEFAULT,
														  NULL, /* bstrategy */
														  relation,
														  MAIN_FORKNUM,
														  flux_bitmap_stream_read_next,
														  scan,
														  sizeof(TBMIterateResult));
	}
	else if (pscan == NULL && scan->rs_nblocks > 0)
	{
		scan->rs_read_stream = read_stream_begin_relation(READ_STREAM_SEQUENTIAL |
														  READ_STREAM_USE_BATCHING,
														  NULL, /* bstrategy */
														  relation,
														  MAIN_FORKNUM,
														  flux_scan_stream_read_next,
														  scan,
														  0);
	}
	else
	{
		scan->rs_read_stream = NULL;
	}

	return (TableScanDesc) scan;
}

/*
 * End the scan and release resources
 */
static void
flux_scan_end(TableScanDesc sscan)
{
	FluxScanDesc scan = (FluxScanDesc) sscan;

	/*
	 * FLUX does not compress attributes, so there is no compression
	 * dictionary to retrain at ANALYZE scan end.
	 */

	/* End read stream before releasing buffers */
	if (scan->rs_read_stream != NULL)
	{
		read_stream_end(scan->rs_read_stream);
		scan->rs_read_stream = NULL;
	}

	/* Release buffer if held */
	if (BufferIsValid(scan->rs_cbuf))
	{
		ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = InvalidBuffer;
	}

	/* Release cached visibility map buffer */
	if (BufferIsValid(scan->rs_vm_buffer))
	{
		ReleaseBuffer(scan->rs_vm_buffer);
		scan->rs_vm_buffer = InvalidBuffer;
	}

	if (scan->rs_vistuples)
		pfree(scan->rs_vistuples);

	if (scan->rs_parallelworkerdata != NULL)
		pfree(scan->rs_parallelworkerdata);

	/*
	 * Unregister the snapshot if this scan owns it (SO_TEMP_SNAPSHOT).
	 * Without this, catalog scans and parallel worker scans leak snapshot
	 * references, causing "resource was not closed" warnings.
	 */
	if (scan->rs_base.rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(scan->rs_base.rs_snapshot);

	pfree(scan);
}

/*
 * Restart a relation scan
 */
static void
flux_scan_rescan(TableScanDesc sscan, ScanKey key,
				 bool set_params, bool allow_strat,
				 bool allow_sync, bool allow_pagemode)
{
	FluxScanDesc scan = (FluxScanDesc) sscan;

	/* Release current buffer */
	if (BufferIsValid(scan->rs_cbuf))
	{
		ReleaseBuffer(scan->rs_cbuf);
		scan->rs_cbuf = InvalidBuffer;
	}

	/* Release cached VM buffer on rescan (relation may have changed) */
	if (BufferIsValid(scan->rs_vm_buffer))
	{
		ReleaseBuffer(scan->rs_vm_buffer);
		scan->rs_vm_buffer = InvalidBuffer;
		scan->rs_vm_blockno = InvalidBlockNumber;
	}

	/* Reset read stream for rescan */
	if (scan->rs_read_stream != NULL)
	{
		read_stream_reset(scan->rs_read_stream);
		scan->rs_prefetch_block = 0;
	}

	/* Reset scan position to start of relation */
	scan->rs_cblock = InvalidBlockNumber;
	scan->rs_nblocks = RelationGetNumberOfBlocks(sscan->rs_rd);
	scan->rs_cindex = 0;
	scan->rs_coffset = FirstOffsetNumber;
	scan->rs_inited = false;
	scan->rs_ntuples = 0;

	/* Update scan key if provided */
	scan->rs_base.rs_nkeys = key ? scan->rs_base.rs_nkeys : 0;
	scan->rs_base.rs_key = key;
}

/*
 * Get the next block to scan.
 *
 * For serial scans, simply advances sequentially.  For parallel scans,
 * coordinates with other workers via table_block_parallelscan_nextpage()
 * so that each block is scanned by exactly one worker.
 *
 * Returns the next block number, or InvalidBlockNumber when finished.
 */
static BlockNumber
flux_scan_nextblock(FluxScanDesc scan)
{
	BlockNumber nblocks = scan->rs_nblocks;

	if (nblocks == 0)
		return InvalidBlockNumber;

	if (scan->rs_base.rs_parallel != NULL)
	{
		ParallelBlockTableScanDesc pbscan =
			(ParallelBlockTableScanDesc) scan->rs_base.rs_parallel;

		/* Initialize parallel worker state on first call */
		if (!scan->rs_inited)
		{
			table_block_parallelscan_startblock_init(scan->rs_base.rs_rd,
													 scan->rs_parallelworkerdata,
													 pbscan,
													 scan->rs_startblock,
													 InvalidBlockNumber);
			scan->rs_inited = true;
		}

		return table_block_parallelscan_nextpage(scan->rs_base.rs_rd,
												 scan->rs_parallelworkerdata,
												 pbscan);
	}
	else
	{
		/* Serial scan: advance to next block sequentially */
		if (scan->rs_cblock == InvalidBlockNumber)
			return 0;

		if (scan->rs_cblock + 1 >= nblocks)
			return InvalidBlockNumber;

		return scan->rs_cblock + 1;
	}
}

/*
 * flux_prepare_pagescan -- collect visible tuple offsets for page-mode scan
 *
 * Called once per page.  Locks SHARE, checks visibility for all tuples,
 * collects visible offsets into scan->rs_vistuples[], then unlocks.
 * The buffer remains pinned via scan->rs_cbuf.
 *
 * This is the FLUX equivalent of heapgetpage().  By doing all visibility
 * checks under a single SHARE lock acquisition per page, we avoid the
 * overhead of ReadBuffer+LockBuffer+ReleaseBuffer per tuple that the
 * original scan path had.
 *
 * The visibility map optimization (1D) is integrated here: if the page is
 * marked all-visible, per-tuple visibility checks are skipped entirely.
 */
static void
flux_prepare_pagescan(FluxScanDesc scan, Buffer buffer)
{
	Page		page;
	OffsetNumber maxoff;
	OffsetNumber offnum;
	int			ntup = 0;
	bool		all_visible;

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);

	/* Skip new/empty pages */
	if (PageIsNew(page))
	{
		scan->rs_ntuples = 0;
		scan->rs_cindex = 0;
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		return;
	}

	maxoff = PageGetMaxOffsetNumber(page);

	/* Allocate vistuples array if needed (shared with bitmap scan path) */
	if (scan->rs_vistuples == NULL)
	{
		scan->rs_vistuples = (OffsetNumber *)
			MemoryContextAlloc(TopMemoryContext,
							   MaxOffsetNumber * sizeof(OffsetNumber));
	}

	/*
	 * Check visibility: first check the in-page PD_ALL_VISIBLE flag (zero
	 * cost since the page is already pinned and locked).  Only fall through
	 * to the VM fork if the in-page flag is not set.  Use the cached VM
	 * buffer to avoid per-page ReadBufferExtended overhead.
	 */
	if (PageIsAllVisible(page))
		all_visible = true;
	else
		all_visible = FluxVMCheckCached(scan->rs_base.rs_rd, scan->rs_cblock,
										FLUX_VM_ALL_VISIBLE,
										&scan->rs_vm_buffer,
										&scan->rs_vm_blockno);

	for (offnum = FirstOffsetNumber; offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;
		FluxTupleHeader *tuple_header;

		itemid = PageGetItemId(page, offnum);
		if (!ItemIdIsNormal(itemid))
			continue;

		tuple_header = (FluxTupleHeader *) PageGetItem(page, itemid);

		/* Skip overflow records - they are not tuples */
		if (FluxIsOverflowRecordInline(tuple_header, ItemIdGetLength(itemid)))
			continue;

		/* Skip speculative tuples not yet confirmed */
		if (tuple_header->t_flags & FLUX_TUPLE_SPECULATIVE)
			continue;

		/*
		 * Check MVCC visibility.  If the page is marked all-visible in the
		 * visibility map, skip the expensive per-tuple check.  Otherwise,
		 * consult the commit timestamp and the sLog for in-progress
		 * transaction state.
		 *
		 * NOTE: Do NOT skip FLUX_TUPLE_DELETED tuples here.  The DELETED flag
		 * is set physically at DELETE time, but the delete may be in-progress
		 * or aborted.  The visibility function correctly consults the sLog to
		 * determine actual delete status.
		 */
		if (!all_visible &&
			scan->rs_base.rs_snapshot &&
			!FluxTupleVisibleToSnapshotDual(tuple_header,
											scan->rs_base.rs_snapshot,
											RelationGetRelid(scan->rs_base.rs_rd),
											buffer))
		{
			/*
			 * The on-page (newest) version is not visible to our snapshot.
			 * zheap read path: if this tuple was updated in place and still
			 * has a version chain in the UNDO log, an OLDER version may be
			 * visible -- keep it as a candidate so flux_scan_getnextslot can
			 * reconstruct and serve the before-image.  Only truly-invisible
			 * tuples (no history, or history exhausted) are dropped here.
			 */
			if ((tuple_header->t_flags & FLUX_TUPLE_UPDATED) &&
				IsMVCCSnapshot(scan->rs_base.rs_snapshot) &&
				UndoRecPtrIsValid(FluxTupleGetVersionPtr(tuple_header,
															ItemIdGetLength(itemid))))
			{
				/*
				 * We will serve an OLDER version of this row, so from SSI's
				 * point of view we are reading a version that the concurrent
				 * in-place updater is superseding -- heap's "visible
				 * RECENTLY_DEAD / DELETE_IN_PROGRESS" case, where it reports
				 * the updater's xid as a conflict out.  This branch used to
				 * `continue` straight past both the conflict-out check and the
				 * per-tuple SIREAD lock, so a reader that saw a before-image
				 * left no rw-dependency at all and write-skew anomalies heap
				 * rejects were silently accepted.
				 */
				if (IsolationIsSerializable())
				{
					ItemPointerData bi_tid;

					FluxCheckForSerializableConflictOut(scan->rs_base.rs_rd,
													   tuple_header,
													   buffer,
													   scan->rs_base.rs_snapshot);

					ItemPointerSet(&bi_tid, BufferGetBlockNumber(buffer), offnum);
					PredicateLockTID(scan->rs_base.rs_rd, &bi_tid,
									 scan->rs_base.rs_snapshot,
									 InvalidTransactionId);
				}

				scan->rs_vistuples[ntup++] = offnum;
				continue;
			}

			/*
			 * Tuple is not visible.  If we're in a serializable transaction,
			 * check for rw-conflict out: a concurrent writer modified this
			 * tuple after our snapshot.
			 */
			if (IsolationIsSerializable())
			{
				FluxCheckForSerializableConflictOut(scan->rs_base.rs_rd,
													tuple_header,
													buffer,
													scan->rs_base.rs_snapshot);
			}
			continue;
		}

		/*
		 * Tuple is visible.  Acquire SIREAD predicate lock for SSI so that
		 * concurrent writers can detect rw-antidependencies on this tuple.
		 *
		 * A VISIBLE tuple can still be a conflict-out source: if a concurrent
		 * transaction is deleting or updating it (visible to us, being written
		 * by them), we have read a version they are superseding, which is an
		 * rw-dependency they must know about.  Heap covers this by calling
		 * HeapCheckForSerializableConflictOut() unconditionally, which reports
		 * the updater's xid in the visible RECENTLY_DEAD /
		 * DELETE_IN_PROGRESS cases.  FLUX previously only reported conflicts
		 * for invisible tuples, so the reader never registered the incoming
		 * rw-conflict and a later writer was not recognised as a pivot --
		 * silently accepting write-skew anomalies heap rejects.
		 */
		if (IsolationIsSerializable())
		{
			ItemPointerData item_tid;

			if (tuple_header->t_flags & (FLUX_TUPLE_DELETED | FLUX_TUPLE_UPDATED))
				FluxCheckForSerializableConflictOut(scan->rs_base.rs_rd,
												   tuple_header,
												   buffer,
												   scan->rs_base.rs_snapshot);

			ItemPointerSet(&item_tid, BufferGetBlockNumber(buffer), offnum);
			PredicateLockTID(scan->rs_base.rs_rd, &item_tid,
							 scan->rs_base.rs_snapshot,
							 InvalidTransactionId);
		}

		scan->rs_vistuples[ntup++] = offnum;
	}

	scan->rs_ntuples = ntup;
	scan->rs_cindex = 0;

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
}

/*
 * Get next tuple from scan (page-mode)
 *
 * Uses page-mode scanning: for each page, flux_prepare_pagescan() collects
 * all visible tuple offsets under a single SHARE lock.  This function then
 * iterates through those offsets from the pinned-but-unlocked buffer.
 *
 * This eliminates the per-tuple ReadBuffer+LockBuffer+ReleaseBuffer overhead
 * of the original implementation (50K rows = 50K buffer ops → 1 per page).
 *
 * For parallel scans, block assignment is coordinated via
 * flux_scan_nextblock() which uses the parallel scan infrastructure.
 */
static bool
flux_scan_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	FluxScanDesc scan = (FluxScanDesc) sscan;

	/* If relation is empty, return false immediately */
	if (scan->rs_nblocks == 0)
	{
		ExecClearTuple(slot);
		return false;
	}

	for (;;)
	{
		Page		page;

		/*
		 * Try to return the next visible tuple from the current page. The
		 * buffer is pinned but NOT locked -- this matches heap's page-mode
		 * pattern.  FluxSlotStoreTuple acquires its own pin so the slot data
		 * remains valid after we move to the next page.
		 */
		while (scan->rs_cindex < scan->rs_ntuples)
		{
			OffsetNumber offnum;
			ItemId		itemid;
			FluxTupleHeader *tuple_header;

			offnum = scan->rs_vistuples[scan->rs_cindex];
			scan->rs_cindex++;

			page = BufferGetPage(scan->rs_cbuf);
			itemid = PageGetItemId(page, offnum);

			if (!ItemIdIsNormal(itemid))
				continue;

			tuple_header = (FluxTupleHeader *) PageGetItem(page, itemid);

			/*
			 * Before-image substitution for committed in-place UPDATEs (zheap
			 * read path).
			 *
			 * The on-page image is the NEWEST version, stamped with the
			 * updater's xmin.  If that updater is not visible to our MVCC
			 * snapshot, we must serve the older version reconstructed from
			 * the UNDO log (WS-PVS2):
			 * FluxReconstructVisibleVersion walks the t_verptr chain and
			 * stops at the version whose producing xid is visible
			 * (XidInMVCCSnapshot).  Visibility is decided purely by the xid
			 * snapshot + CLOG, matching FluxTupleSatisfiesMVCC.
			 *
			 * Determine on-page visibility once; only reconstruct when the
			 * newest version is NOT visible to us.  When it IS visible, serve
			 * on-page bytes directly.
			 */
			if ((tuple_header->t_flags & FLUX_TUPLE_UPDATED) &&
				scan->rs_base.rs_snapshot != NULL &&
				IsMVCCSnapshot(scan->rs_base.rs_snapshot) &&
				UndoRecPtrIsValid(FluxTupleGetVersionPtr(tuple_header,
															ItemIdGetLength(itemid))) &&
				!FluxTupleVisibleToSnapshotDual(tuple_header,
												scan->rs_base.rs_snapshot,
												RelationGetRelid(scan->rs_base.rs_rd),
												scan->rs_cbuf))
			{
				char	   *bi_data = NULL;
				int			bi_len = 0;
				ItemPointerData item_tid;

				ItemPointerSet(&item_tid, scan->rs_cblock, offnum);

				if (FluxReconstructVisibleVersion(
												  scan->rs_base.rs_rd,
												  &item_tid,
												  (const char *) tuple_header,
												  ItemIdGetLength(itemid),
												  scan->rs_base.rs_snapshot,
												  &bi_data, &bi_len))
				{
					FluxTupleHeader *bi_tuple = (FluxTupleHeader *) bi_data;

					FluxSlotStoreMaterializedTuple(slot, bi_tuple, bi_len);
					ItemPointerSet(&slot->tts_tid, scan->rs_cblock, offnum);
					return true;
				}

				/*
				 * No visible older version: the row is genuinely invisible to
				 * this snapshot (e.g. it was inserted-then-updated all after
				 * our snapshot).  Skip it.
				 */
				continue;
			}

			/* Normal: serve on-page data */
			FluxSlotStoreTuple(slot, tuple_header,
							   ItemIdGetLength(itemid), scan->rs_cbuf);
			ItemPointerSet(&slot->tts_tid, scan->rs_cblock, offnum);
			return true;
		}

		/*
		 * Exhausted all visible tuples on the current page. Release the
		 * buffer pin and advance to the next block.
		 */
		if (BufferIsValid(scan->rs_cbuf))
		{
			ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
		}

		/*
		 * Get the next page.  Use the read stream if available (prefetches
		 * upcoming pages for I/O efficiency), otherwise fall back to
		 * ReadBuffer for parallel scans.
		 */
		if (scan->rs_read_stream != NULL)
		{
			scan->rs_cbuf = read_stream_next_buffer(scan->rs_read_stream, NULL);
			if (!BufferIsValid(scan->rs_cbuf))
			{
				ExecClearTuple(slot);
				return false;
			}
			scan->rs_cblock = BufferGetBlockNumber(scan->rs_cbuf);
		}
		else
		{
			BlockNumber block = flux_scan_nextblock(scan);

			if (!BlockNumberIsValid(block))
			{
				ExecClearTuple(slot);
				return false;
			}

			scan->rs_cblock = block;
			scan->rs_cbuf = ReadBuffer(scan->rs_base.rs_rd, scan->rs_cblock);
		}

		/*
		 * Opportunistic cleanup: try to prune dead tuples before scanning.
		 * FluxPagePruneOpt() expects only a pin (no lock) and will
		 * non-blockingly try to get an exclusive lock.
		 */
		FluxPagePruneOpt(scan->rs_base.rs_rd, scan->rs_cbuf);

		/*
		 * Prepare the page scan: lock SHARE, collect all visible tuple
		 * offsets into rs_vistuples[], then unlock.  The buffer stays pinned
		 * for efficient tuple access without re-locking.
		 */
		flux_prepare_pagescan(scan, scan->rs_cbuf);
	}
}

/*
 * Set TID range for TID range scans
 *
 * This restricts the scan to only return tuples within the given TID range.
 * Used by TID range scans (WHERE ctid >= ... AND ctid < ...).
 *
 * Following the heap AM pattern, we store the effective min/max TIDs in the
 * base scan descriptor's st.tidrange fields and configure the scan start
 * position accordingly.
 */
static void
flux_scan_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
					   ItemPointer maxtid)
{
	FluxScanDesc scan = (FluxScanDesc) sscan;
	BlockNumber nblocks;
	ItemPointerData highestItem;
	ItemPointerData lowestItem;

	nblocks = RelationGetNumberOfBlocks(sscan->rs_rd);

	/*
	 * For relations without any pages, we can simply leave the TID range
	 * unset.  There will be no tuples to scan, therefore no tuples outside
	 * the given TID range.
	 */
	if (nblocks == 0)
		return;

	/*
	 * Set up ItemPointers which point to the first and last possible tuples
	 * in the relation.
	 */
	ItemPointerSet(&highestItem, nblocks - 1, MaxOffsetNumber);
	ItemPointerSet(&lowestItem, 0, FirstOffsetNumber);

	/*
	 * If the given maximum TID is below the highest possible TID in the
	 * relation, then restrict the range to that, otherwise we scan to the end
	 * of the relation.
	 */
	if (ItemPointerCompare(maxtid, &highestItem) < 0)
		ItemPointerCopy(maxtid, &highestItem);

	/*
	 * If the given minimum TID is above the lowest possible TID in the
	 * relation, then restrict the range to only scan for TIDs above that.
	 */
	if (ItemPointerCompare(mintid, &lowestItem) > 0)
		ItemPointerCopy(mintid, &lowestItem);

	/*
	 * Check for an empty range.
	 */
	if (ItemPointerCompare(&highestItem, &lowestItem) < 0)
	{
		/* Force an empty scan */
		scan->rs_cblock = nblocks;
		scan->rs_coffset = FirstOffsetNumber;
		ItemPointerSetInvalid(&sscan->st.tidrange.rs_mintid);
		ItemPointerSetInvalid(&sscan->st.tidrange.rs_maxtid);
		return;
	}

	/* Set scan start position to the first block in range */
	scan->rs_cblock = ItemPointerGetBlockNumberNoCheck(&lowestItem);
	scan->rs_coffset = FirstOffsetNumber;

	/* Store the effective TID range in the base scan descriptor */
	ItemPointerCopy(&lowestItem, &sscan->st.tidrange.rs_mintid);
	ItemPointerCopy(&highestItem, &sscan->st.tidrange.rs_maxtid);
}

/*
 * Get next tuple within the TID range set by flux_scan_set_tidrange.
 *
 * This delegates to the regular flux_scan_getnextslot for tuple fetching
 * and visibility, then filters to only return tuples within the TID range.
 * This keeps visibility semantics consistent between regular and TID range
 * scans.
 */
static bool
flux_scan_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
							   TupleTableSlot *slot)
{
	FluxScanDesc scan = (FluxScanDesc) sscan;
	ItemPointer mintid = &sscan->st.tidrange.rs_mintid;
	ItemPointer maxtid = &sscan->st.tidrange.rs_maxtid;
	BlockNumber nblocks;
	BlockNumber block;
	BlockNumber maxblock;
	Buffer		buffer;
	Page		page;
	OffsetNumber offnum;
	OffsetNumber maxoff;
	ItemId		itemid;
	FluxTupleHeader *tuple_header;

	/* If the range is invalid/empty, we're done */
	if (!ItemPointerIsValid(mintid) || !ItemPointerIsValid(maxtid))
	{
		ExecClearTuple(slot);
		return false;
	}

	maxblock = ItemPointerGetBlockNumber(maxtid);

	/* Clear the slot */
	ExecClearTuple(slot);

	nblocks = RelationGetNumberOfBlocks(sscan->rs_rd);
	if (nblocks == 0)
		return false;

	/* Scan pages within the TID range */
	for (block = scan->rs_cblock; block <= maxblock && block < nblocks; block++)
	{
		buffer = ReadBuffer(sscan->rs_rd, block);

		/*
		 * Opportunistic cleanup on first visit to page, consistent with the
		 * regular scan path.
		 */
		if (scan->rs_coffset == FirstOffsetNumber)
			FluxPagePruneOpt(sscan->rs_rd, buffer);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		/* Skip new/empty pages */
		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buffer);
			scan->rs_coffset = FirstOffsetNumber;
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);

		for (offnum = scan->rs_coffset; offnum <= maxoff; offnum++)
		{
			ItemPointerData curtid;

			ItemPointerSet(&curtid, block, offnum);

			/*
			 * Filter by TID range.  Skip tuples below mintid.
			 */
			if (ItemPointerCompare(&curtid, mintid) < 0)
				continue;

			/*
			 * If we've passed maxtid, we're done scanning.
			 */
			if (ItemPointerCompare(&curtid, maxtid) > 0)
			{
				UnlockReleaseBuffer(buffer);
				return false;
			}

			itemid = PageGetItemId(page, offnum);

			if (!ItemIdIsNormal(itemid))
				continue;

			tuple_header = (FluxTupleHeader *) PageGetItem(page, itemid);

			/* Skip overflow records - they are not tuples */
			if (FluxIsOverflowRecordInline(tuple_header,
										   ItemIdGetLength(itemid)))
				continue;

			/* Skip speculative tuples not yet confirmed */
			if (tuple_header->t_flags & FLUX_TUPLE_SPECULATIVE)
				continue;

			/* Check MVCC visibility (handles DELETED via sLog) */
			if (sscan->rs_snapshot &&
				!FluxTupleVisibleToSnapshotDual(tuple_header,
												sscan->rs_snapshot,
												RelationGetRelid(sscan->rs_rd),
												buffer))
				continue;

			/*
			 * Store the tuple into the slot with a buffer pin. Decompression
			 * happens lazily during deformation.
			 */
			FluxSlotStoreTuple(slot, tuple_header,
							   ItemIdGetLength(itemid), buffer);
			ItemPointerSet(&slot->tts_tid, block, offnum);
			/* Update scan position for next call */
			scan->rs_cblock = block;
			scan->rs_coffset = offnum + 1;

			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buffer);
			return true;
		}

		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buffer);
		scan->rs_coffset = FirstOffsetNumber;
	}

	/* Reached end of assigned range */
	return false;
}

/*
 * Bitmap heap scan: fetch next tuple from bitmap
 *
 * This is the scan_bitmap_next_tuple callback.  It iterates over TIDs from
 * the TBM (TID bitmap) built by a BitmapIndexScan, fetches and checks
 * visibility for each tuple, and returns visible ones in the slot.
 *
 * For each block indicated by the bitmap:
 * - If the block is "lossy" (the bitmap lost per-tuple precision for this
 *   block), we check every tuple on the page.
 * - If the block is "exact", we only check the specific offsets indicated.
 *
 * The scan descriptor was set up via table_beginscan_bm() and the TBM
 * iterator is stored in scan->st.rs_tbmiterator.
 */
static bool
flux_scan_bitmap_next_tuple(TableScanDesc scan,
							TupleTableSlot *slot,
							bool *recheck,
							uint64 *lossy_pages,
							uint64 *exact_pages)
{
	FluxScanDesc rscan = (FluxScanDesc) scan;

	Assert(rscan->rs_read_stream);

	for (;;)
	{
		void	   *per_buffer_data;
		TBMIterateResult *tbmres;

		/*
		 * If we have tuples remaining from a previously fetched page, try to
		 * return one.
		 */
		if (rscan->rs_ntuples > 0 &&
			rscan->rs_cindex < rscan->rs_ntuples)
		{
			OffsetNumber offnum;
			Page		page;
			ItemId		itemid;
			FluxTupleHeader *tuple_hdr;

			offnum = rscan->rs_vistuples[rscan->rs_cindex];
			rscan->rs_cindex++;

			/* Re-read the page (we released the lock after visibility check) */
			if (!BufferIsValid(rscan->rs_cbuf))
				rscan->rs_cbuf = ReadBuffer(scan->rs_rd, rscan->rs_cblock);

			LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(rscan->rs_cbuf);

			itemid = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(itemid))
			{
				LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
				continue;
			}

			tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

			/*
			 * NOTE: Do NOT skip FLUX_TUPLE_DELETED here.  This tuple passed
			 * the visibility check during page scan preparation. The DELETED
			 * flag may be set but the delete could be in-progress or aborted.
			 */

			/*
			 * FLUX updates in place keeping the same TID, so a changed
			 * indexed column leaves a stale (oldkey -> tid) secondary entry
			 * beside the new one.  A bitmap scan keeps only TIDs and discards
			 * the index tuple, so it cannot compare the stored key against
			 * the live tuple. Force the executor to re-evaluate
			 * bitmapqualorig against the live tuple whenever we return a
			 * committed in-place UPDATE: an exact (non-lossy) bitmap would
			 * otherwise trust the stale index entry and return a row whose
			 * live key no longer matches the scan key.
			 */
			if (tuple_hdr->t_flags & FLUX_TUPLE_UPDATED)
				*recheck = true;

			/*
			 * Before-image substitution for in-place UPDATEs (zheap read
			 * path), mirroring the sequential-scan and index-fetch paths.  A
			 * snapshot that cannot see the updater's xmin must observe the
			 * before-image, not the on-page (new) data.  Serve it from the
			 * UNDO log (WS-PVS2); FluxReconstructVisibleVersion
			 * walks the t_verptr chain and stops at the version whose
			 * producing xid is visible.  Visibility is ordinary heap-shaped
			 * xmin/xmax + CLOG + snapshot (FluxTupleSatisfiesMVCC).
			 */
			if ((tuple_hdr->t_flags & FLUX_TUPLE_UPDATED) &&
				scan->rs_snapshot != NULL &&
				IsMVCCSnapshot(scan->rs_snapshot) &&
				UndoRecPtrIsValid(FluxTupleGetVersionPtr(tuple_hdr,
															ItemIdGetLength(itemid))) &&
				!FluxTupleVisibleToSnapshotDual(tuple_hdr, scan->rs_snapshot,
												RelationGetRelid(scan->rs_rd),
												rscan->rs_cbuf))
			{
				char	   *bi_data = NULL;
				int			bi_len = 0;
				ItemPointerData item_tid;

				ItemPointerSet(&item_tid, rscan->rs_cblock, offnum);

				if (FluxReconstructVisibleVersion(
												  scan->rs_rd,
												  &item_tid,
												  (const char *) tuple_hdr,
												  ItemIdGetLength(itemid),
												  scan->rs_snapshot,
												  &bi_data, &bi_len))
				{
					FluxTupleHeader *bi_tuple = (FluxTupleHeader *) bi_data;

					FluxSlotStoreMaterializedTuple(slot, bi_tuple, bi_len);
					slot->tts_tableOid = RelationGetRelid(scan->rs_rd);
					ItemPointerSet(&slot->tts_tid, rscan->rs_cblock, offnum);
					LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

					return true;
				}

				/* No visible older version: skip this tuple. */
				LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
				continue;
			}

			/*
			 * Store the tuple with a buffer pin.  The slot gets its own pin
			 * via FluxSlotStoreTuple so the data stays valid.
			 */
			FluxSlotStoreTuple(slot, tuple_hdr,
							   ItemIdGetLength(itemid), rscan->rs_cbuf);
			slot->tts_tableOid = RelationGetRelid(scan->rs_rd);
			ItemPointerSet(&slot->tts_tid, rscan->rs_cblock, offnum);
			LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			return true;
		}

		/* Release buffer from previous block */
		if (BufferIsValid(rscan->rs_cbuf))
		{
			ReleaseBuffer(rscan->rs_cbuf);
			rscan->rs_cbuf = InvalidBuffer;
		}

		/*
		 * Advance to the next block in the bitmap.  The read stream pulls
		 * blocks from the TBM iterator (via flux_bitmap_stream_read_next),
		 * prefetching upcoming bitmap pages, and hands back the matching
		 * TBMIterateResult in per_buffer_data.  Out-of-range blocks were
		 * already filtered by the callback.
		 */
		rscan->rs_cbuf = read_stream_next_buffer(rscan->rs_read_stream,
												 &per_buffer_data);

		if (!BufferIsValid(rscan->rs_cbuf))
			return false;		/* bitmap exhausted */

		tbmres = per_buffer_data;

		Assert(BlockNumberIsValid(tbmres->blockno));
		Assert(BufferGetBlockNumber(rscan->rs_cbuf) == tbmres->blockno);

		*recheck = tbmres->recheck;

		rscan->rs_cblock = tbmres->blockno;

		LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_SHARE);
		{
			Page		page = BufferGetPage(rscan->rs_cbuf);
			int			ntup = 0;
			OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

			/* Allocate vistuples array if needed */
			if (rscan->rs_vistuples == NULL)
			{
				rscan->rs_vistuples = (OffsetNumber *)
					MemoryContextAlloc(TopMemoryContext,
									   MaxOffsetNumber * sizeof(OffsetNumber));
			}

			if (!tbmres->lossy)
			{
				/*
				 * Exact page: only examine offsets listed in the bitmap.
				 */
				OffsetNumber offsets[TBM_MAX_TUPLES_PER_PAGE];
				int			noffsets;

				noffsets = tbm_extract_page_tuple(tbmres, offsets,
												  TBM_MAX_TUPLES_PER_PAGE);

				for (int j = 0; j < noffsets; j++)
				{
					OffsetNumber offnum = offsets[j];
					ItemId		itemid;
					FluxTupleHeader *tuple_hdr;

					if (offnum < FirstOffsetNumber || offnum > maxoff)
						continue;

					itemid = PageGetItemId(page, offnum);
					if (!ItemIdIsNormal(itemid))
						continue;

					tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

					/* Skip overflow records */
					if (FluxIsOverflowRecordInline(tuple_hdr, ItemIdGetLength(itemid)))
						continue;

					/* Skip speculative tuples */
					if (tuple_hdr->t_flags & FLUX_TUPLE_SPECULATIVE)
						continue;

					/*
					 * Check visibility.  Keep an invisible-but-updated tuple
					 * that still has a version chain as a candidate so the
					 * tuple loop can reconstruct and serve the before-image
					 * (zheap read path).
					 */
					if (scan->rs_snapshot &&
						!FluxTupleVisibleToSnapshotDual(tuple_hdr, scan->rs_snapshot,
														RelationGetRelid(scan->rs_rd),
														rscan->rs_cbuf))
					{
						if (!((tuple_hdr->t_flags & FLUX_TUPLE_UPDATED) &&
							  IsMVCCSnapshot(scan->rs_snapshot) &&
							  UndoRecPtrIsValid(FluxTupleGetVersionPtr(tuple_hdr,
																		  ItemIdGetLength(itemid)))))
							continue;
					}

					rscan->rs_vistuples[ntup++] = offnum;
				}

				(*exact_pages)++;
			}
			else
			{
				/*
				 * Lossy page: examine every tuple on the page.  tbmres->lossy
				 * is true here.
				 */
				OffsetNumber offnum;

				for (offnum = FirstOffsetNumber; offnum <= maxoff;
					 offnum = OffsetNumberNext(offnum))
				{
					ItemId		itemid;
					FluxTupleHeader *tuple_hdr;

					itemid = PageGetItemId(page, offnum);
					if (!ItemIdIsNormal(itemid))
						continue;

					tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

					/* Skip overflow records */
					if (FluxIsOverflowRecordInline(tuple_hdr, ItemIdGetLength(itemid)))
						continue;

					/* Skip speculative tuples */
					if (tuple_hdr->t_flags & FLUX_TUPLE_SPECULATIVE)
						continue;

					/*
					 * Check visibility.  Keep an invisible-but-updated tuple
					 * that still has a version chain as a candidate so the
					 * tuple loop can reconstruct and serve the before-image
					 * (zheap read path).
					 */
					if (scan->rs_snapshot &&
						!FluxTupleVisibleToSnapshotDual(tuple_hdr, scan->rs_snapshot,
														RelationGetRelid(scan->rs_rd),
														rscan->rs_cbuf))
					{
						if (!((tuple_hdr->t_flags & FLUX_TUPLE_UPDATED) &&
							  IsMVCCSnapshot(scan->rs_snapshot) &&
							  UndoRecPtrIsValid(FluxTupleGetVersionPtr(tuple_hdr,
																		  ItemIdGetLength(itemid)))))
							continue;
					}

					rscan->rs_vistuples[ntup++] = offnum;
				}

				(*lossy_pages)++;
			}

			rscan->rs_ntuples = ntup;
			rscan->rs_cindex = 0;
		}
		LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

		/* Loop back to return the first visible tuple from this block */
	}
}

/*
 * ------------------------------------------------------------------------
 * Index scan callbacks for FLUX AM
 * ------------------------------------------------------------------------
 */

static bool flux_index_getnext_slot(IndexScanDesc scan,
									ScanDirection direction,
									TupleTableSlot *slot);
static bool flux_index_fetch_tuple(IndexScanFluxData *scan,
								   ItemPointer tid,
								   Snapshot snapshot,
								   TupleTableSlot *tts,
								   bool *call_again, bool *all_dead);

/*
 * Prepare table-AM state for a FLUX index scan (slot-based interface, added by
 * upstream ddce1da5b1b).  Allocates FLUX's private IndexScanFluxData into
 * scan->xs_table_opaque and resolves the getnext_slot implementation.
 *
 * FLUX enables in-place delete-marking (am_index_delete_marking = true), so a
 * plain index scan can reach a heap TID through an entry whose key no longer
 * matches the visible version (a delete-marked tombstone carrying an OLD key,
 * or a live NEW-key entry while an old-snapshot reader is served the
 * reconstructed before-image).  flux_index_getnext_slot rechecks the entry key
 * against the visible tuple and skips on mismatch (Phase 8c invariant I2), so
 * we ask the index AM to keep the index tuple (xs_want_itup) for the whole
 * scan.  The planner suppresses genuine index-only scans on a delete-marking
 * table (RelationSupportsDeleteMarking in get_relation_info), so xs_want_itup
 * here always means "plain scan that wants xs_itup for the recheck", never a
 * true index-only scan.
 */
static void
flux_index_scan_begin(IndexScanDesc scan, uint32 flags)
{
	IndexScanFluxData *fscan = palloc0_object(IndexScanFluxData);

	fscan->rel = scan->heapRelation;
	fscan->buffer = InvalidBuffer;

	/*
	 * On a delete-marking table, ask the index AM to keep the index tuple
	 * (xs_itup) so flux_index_getnext_slot can recheck the key against the
	 * visible version.  Use xs_want_itup_recheck, NOT xs_want_itup: this is a
	 * plain (buffer-slot) index scan, never a genuine index-only scan
	 * (planner-suppressed), and table_index_getnext_slot asserts that
	 * xs_want_itup implies a virtual slot.
	 */
	if (RelationSupportsDeleteMarking(scan->heapRelation))
		scan->xs_want_itup_recheck = true;

	scan->xs_getnext_slot = flux_index_getnext_slot;
	scan->xs_table_opaque = fscan;
}

static void
flux_index_scan_reset(IndexScanDesc scan)
{
	IndexScanFluxData *fscan = (IndexScanFluxData *) scan->xs_table_opaque;

	if (BufferIsValid(fscan->buffer))
	{
		ReleaseBuffer(fscan->buffer);
		fscan->buffer = InvalidBuffer;
	}
}

static void
flux_index_scan_end(IndexScanDesc scan)
{
	IndexScanFluxData *fscan = (IndexScanFluxData *) scan->xs_table_opaque;

	if (BufferIsValid(fscan->buffer))
		ReleaseBuffer(fscan->buffer);

	pfree(fscan);
}

/*
 * fetch_tid: TID-based visibility check for constraint enforcement code
 * (_bt_check_unique, unique_key_recheck), the special-purpose table-AM entry
 * point upstream ddce1da5b1b split out of the index-scan machinery.  These
 * callers pass a TID taken from an index but do not run an index scan, so they
 * need no scan state -- just "is any version reachable through this TID visible
 * to `snapshot`?".  FLUX has no HOT chains, so this is a single fetch through
 * the same visibility logic the index scan uses.
 */
static bool
flux_fetch_tid(Relation rel, ItemPointer tid, Snapshot snapshot,
			   bool *all_dead)
{
	IndexScanFluxData fscan;
	TupleTableSlot *slot;
	bool		call_again = false;
	bool		found;

	fscan.rel = rel;
	fscan.buffer = InvalidBuffer;

	slot = table_slot_create(rel, NULL);
	found = flux_index_fetch_tuple(&fscan, tid, snapshot, slot,
								   &call_again, all_dead);
	if (BufferIsValid(fscan.buffer))
		ReleaseBuffer(fscan.buffer);
	ExecDropSingleTupleTableSlot(slot);

	return found;
}

/*
 * Recheck a delete-marking index entry's key against the tuple just fetched
 * into `slot` for the scan's snapshot (Phase 8c invariant I2).
 *
 * On a delete-marking (in-place UPDATE) table an index scan can reach a heap
 * TID through an entry whose key differs from the VISIBLE version's key:
 *   - a delete-marked (tombstone) entry carries the OLD key while a new
 *     snapshot reader sees the NEW version; and
 *   - a LIVE (new-key) entry, while an OLD-snapshot reader is served the
 *     reconstructed before-image (OLD key) by the table AM.
 * Returning the tuple via such an entry would be wrong (a duplicate for a
 * range scan, or a phantom key for an old reader).  Re-form the visible
 * tuple's key for this index and compare to the entry (scan->xs_itup);
 * returns true on mismatch (caller must skip this entry).  The matching
 * version is always reachable through the entry whose key equals the visible
 * key.  Moved here from indexam.c index_fetch_heap() when upstream ddce1da5b1b
 * put the whole index-scan heap-fetch loop under the table AM.
 */
static bool
flux_index_entry_key_mismatch(IndexScanDesc scan, TupleTableSlot *slot)
{
	IndexInfo  *ii;
	EState	   *estate;
	ExprContext *econtext;
	Datum		vis_values[INDEX_MAX_KEYS];
	bool		vis_isnull[INDEX_MAX_KEYS];
	Datum		ent_values[INDEX_MAX_KEYS];
	bool		ent_isnull[INDEX_MAX_KEYS];
	TupleDesc	itupdesc = RelationGetDescr(scan->indexRelation);
	bool		mismatch = false;
	int			i;

	ii = BuildIndexInfo(scan->indexRelation);
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);

	econtext->ecxt_scantuple = slot;
	FormIndexDatum(ii, slot, estate, vis_values, vis_isnull);
	index_deform_tuple(scan->xs_itup, itupdesc, ent_values, ent_isnull);

	for (i = 0; i < ii->ii_NumIndexKeyAttrs; i++)
	{
		Form_pg_attribute att = TupleDescAttr(itupdesc, i);

		if (vis_isnull[i] != ent_isnull[i])
		{
			mismatch = true;
			break;
		}
		if (vis_isnull[i])
			continue;
		if (!datum_image_eq(vis_values[i], ent_values[i],
							att->attbyval, att->attlen))
		{
			mismatch = true;
			break;
		}
	}

	FreeExecutorState(estate);

	return mismatch;
}

/*
 * xs_getnext_slot callback for FLUX (slot-based table-AM index scan
 * interface).  This is the single entry point the executor reaches through
 * table_index_getnext_slot(): it drives the index AM (amgettuple via
 * tableam_index_getnext_tid) and, for each TID, fetches and visibility-checks
 * the FLUX tuple into the caller's slot, applying the delete-marking key
 * recheck.  Mirrors heapam_index_getnext_slot's structure, minus HOT chains
 * (FLUX does in-place, stable-TID updates and never sets call_again).
 */
static bool
flux_index_getnext_slot(IndexScanDesc scan, ScanDirection direction,
						TupleTableSlot *slot)
{
	IndexScanFluxData *fscan = (IndexScanFluxData *) scan->xs_table_opaque;

	Assert(TransactionIdIsValid(RecentXmin));

	for (;;)
	{
		bool		call_again = false;
		bool		all_dead = false;

		/* Get the next TID from the index */
		if (!tableam_index_getnext_tid(scan, direction))
			return false;

		Assert(ItemPointerIsValid(&scan->xs_heaptid));

		/* Fetch and visibility-check the FLUX tuple for this TID */
		if (!flux_index_fetch_tuple(fscan, &scan->xs_heaptid,
									scan->xs_snapshot, slot,
									&call_again, &all_dead))
		{
			/*
			 * No visible tuple for this entry.  Tell the index AM to kill its
			 * entry only when the TID is dead to all (never carry a sticky
			 * verdict), and never in recovery.
			 */
			if (all_dead && !scan->xactStartedInRecovery)
				scan->kill_prior_tuple = true;
			continue;			/* try next index entry */
		}

		pgstat_count_heap_fetch(scan->indexRelation);

		/* Phase 8c delete-marking key recheck (invariant I2) */
		if (scan->xs_itup != NULL &&
			RelationSupportsDeleteMarking(scan->heapRelation) &&
			flux_index_entry_key_mismatch(scan, slot))
		{
			/*
			 * The entry's key does not describe the visible version.  Do NOT
			 * let this count toward kill_prior_tuple (the entry is not dead --
			 * an older snapshot legitimately needs it).  Fetch the next TID.
			 */
			continue;
		}

		return true;
	}

	pg_unreachable();
	return false;
}

/*
 * Fetch the FLUX tuple at `tid` into `tts` after a visibility test according
 * to `snapshot`; returns true if a visible version was produced.  Called only
 * from flux_index_getnext_slot (the xs_getnext_slot callback).
 *
 * *call_again is always left false: FLUX does in-place, stable-TID updates, so
 * there is never more than one row version reachable via a single index entry
 * (no HOT-style chain to keep walking).  *all_dead, if non-NULL, is set true
 * only when this specific TID is dead to every snapshot; the caller uses it to
 * drive kill_prior_tuple.  It is strictly PER-TID -- never a sticky scan-level
 * verdict -- or a live entry could be wrongly marked LP_DEAD (silent data
 * loss).
 */
static bool
flux_index_fetch_tuple(IndexScanFluxData *scan,
					   ItemPointer tid,
					   Snapshot snapshot,
					   TupleTableSlot *tts,
					   bool *call_again, bool *all_dead)
{
	Relation	rel = scan->rel;
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);
	OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
	Buffer		buffer;
	Page		page;
	ItemId		itemid;
	FluxTupleHeader *tuple_hdr;
	bool		visible = false;

	/*
	 * Initialize output parameters.  all_dead is strictly PER-TID: it must
	 * reflect only whether *this* TID is dead to every snapshot, never a
	 * verdict carried over from a previously-fetched TID in the same scan.
	 * FLUX does in-place updates and never sets call_again, so each TID is
	 * fetched exactly once; a sticky scan-level all_dead would leak an
	 * earlier TID's dead verdict onto a later LIVE TID, making indexam.c set
	 * kill_prior_tuple and _bt_killitems mark the live entry LP_DEAD -- the
	 * silent-data-loss "kill LIVE entries" failure.  So always start false
	 * and set true only in the truly-dead branch below.
	 */
	*call_again = false;
	if (all_dead)
		*all_dead = false;

	/*
	 * FLUX secondary indexes hold plain 6-byte heap-style TIDs and FLUX does
	 * OUT-OF-PLACE updates for any key-changing UPDATE (delete + insert at a
	 * new TID), so a changed indexed column never leaves a stale (oldkey ->
	 * tid) entry pointing at a live in-place tuple.  There is therefore no
	 * in-place index staleness to recheck: this is a plain heap-style TID
	 * fetch that just checks visibility.
	 */

	/* Clear the slot */
	ExecClearTuple(tts);

	/*
	 * Release any buffer from a previous index_fetch_tuple call. This
	 * prevents buffer leaks when scanning multiple tuples.
	 */
	if (BufferIsValid(scan->buffer))
	{
		ReleaseBuffer(scan->buffer);
		scan->buffer = InvalidBuffer;
	}

	/* Read the page */
	buffer = ReadBuffer(rel, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buffer);

	/* Validate offset */
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
	{
		UnlockReleaseBuffer(buffer);
		return false;
	}

	/* Get the item */
	itemid = PageGetItemId(page, offnum);

	if (!ItemIdIsNormal(itemid))
	{
		UnlockReleaseBuffer(buffer);
		return false;
	}

	/* Get tuple header */
	tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

	/* Check visibility (heap-shaped xmin/xmax) */
	if (snapshot)
	{
		if (snapshot->snapshot_type == SNAPSHOT_DIRTY)
		{
			/*
			 * For SNAPSHOT_DIRTY (used by _bt_check_unique during ON
			 * CONFLICT), we must report the inserting/deleting xid through
			 * the snapshot struct so that SpeculativeInsertionWait can
			 * function correctly.  This mirrors HeapTupleSatisfiesDirty()
			 * behaviour.
			 *
			 * Uses t_xmin for INSERT visibility (fast path) and a single
			 * batched sLog lookup for DELETE/UPDATE/LOCK state.
			 */
			SLogTupleOp slog_entries[SLOG_MAX_TUPLE_OPS];
			int			slog_nfound = -1;	/* lazy: -1 = not yet fetched */

			snapshot->xmin = InvalidTransactionId;
			snapshot->xmax = InvalidTransactionId;
			snapshot->speculativeToken = 0;

			/*
			 * A speculative insert that lost its arbiter race is killed by
			 * flux_tuple_complete_speculative(succeeded=false), which marks it
			 * DELETED with t_xmax == t_xmin and leaves it on the page.  Such a
			 * tuple can never become live again (its inserter and deleter are
			 * the same xid, so they commit or roll back together), so it is
			 * dead to every snapshot -- the verdict heap_abort_speculative
			 * gets by zeroing xmin.  Without this, the killer's own next
			 * arbiter pass finds the tuple, reports its own top xid as xmin,
			 * and XactLockTableWait spins forever on itself
			 * (TransactionIdIsInProgress(own xid) is always true); other
			 * backends would wait on a speculative token already released.
			 */
			if ((tuple_hdr->t_flags & FLUX_TUPLE_DELETED) &&
				TransactionIdIsValid(FluxTupleGetXmin(tuple_hdr)) &&
				TransactionIdEquals(FluxTupleGetXmin(tuple_hdr),
									FluxTupleGetXmax(tuple_hdr)))
			{
				visible = false;
				goto visibility_done;
			}

			/*
			 * Resolve the inserter (t_xmin) unless it is ALREADY known committed
			 * via the FLUX_TUPLE_XMIN_COMMITTED hint.
			 *
			 * We must NOT gate this on FLUX_TUPLE_UNCOMMITTED: that flag is a
			 * pure hint cleared at PRE_COMMIT (FluxClearUncommittedFlags), which
			 * runs BEFORE the CLOG write / ProcArrayEndTransaction.  So an inserter
			 * that is still in progress -- past PRE_COMMIT but not yet committed --
			 * has UNCOMMITTED already clear, and gating on it would skip resolution
			 * and wrongly treat the row as a committed conflict.  In ON CONFLICT
			 * that surfaced as TM_Invisible -> slot_getsysattr on an empty slot ->
			 * Assert.  FLUX_TUPLE_XMIN_COMMITTED, by contrast, is set ONLY from a
			 * CLOG read (below and in FluxSetHintBits), so it can never precede the
			 * actual commit: gating on its absence resolves every not-yet-committed
			 * inserter correctly.  t_xmin is the inserter (heap xmin semantics);
			 * only speculative inserts additionally consult the sLog, for the token.
			 */
			if (!(tuple_hdr->t_flags & FLUX_TUPLE_XMIN_COMMITTED))
			{
				TransactionId hint_xid = FluxTupleGetXmin(tuple_hdr);

				if (TransactionIdIsValid(hint_xid) &&
					!TransactionIdIsCurrentTransactionId(hint_xid))
				{
					if (TransactionIdIsInProgress(hint_xid))
					{
						/*
						 * Another transaction is inserting this tuple. Report
						 * xmin for SpeculativeInsertionWait.
						 */
						snapshot->xmin = hint_xid;

						if (tuple_hdr->t_flags & FLUX_TUPLE_SPECULATIVE)
						{
							/* Fetch sLog for speculative token */
							slog_nfound = SLogTupleLookupFiltered(
																  RelationGetRelid(rel), tid,
																  InvalidTransactionId,
																  slog_entries, SLOG_MAX_TUPLE_OPS);
							{
								int			si;

								for (si = 0; si < slog_nfound; si++)
								{
									if (slog_entries[si].xid == hint_xid &&
										slog_entries[si].spec_token != 0)
									{
										snapshot->speculativeToken =
											slog_entries[si].spec_token;
										break;
									}
								}
							}
						}

						visible = true;
						goto visibility_done;
					}
					else if (!TransactionIdDidCommit(hint_xid))
					{
						/*
						 * Inserter aborted or crashed -- invisible.  Use
						 * !DidCommit rather than DidAbort: a crashed xid
						 * reports neither committed nor aborted, and with no
						 * INSERT undo record the aborted-inserter CLOG verdict
						 * is the only rollback signal (heap does the same).
						 */
						visible = false;
						goto visibility_done;
					}
					else
					{
						/*
						 * Inserter committed.  Cache the CLOG verdict as the
						 * FLUX_TUPLE_XMIN_COMMITTED hint (set ONLY from a CLOG
						 * read, so it can never precede commit) and clear the
						 * stale UNCOMMITTED hint.  BufferSetHintBits16 handles
						 * the lock upgrade.
						 */
						BufferSetHintBits16(&tuple_hdr->t_flags,
											(tuple_hdr->t_flags |
											 FLUX_TUPLE_XMIN_COMMITTED) &
											~FLUX_TUPLE_UNCOMMITTED,
											buffer);
					}
				}
				else if (TransactionIdIsValid(hint_xid) &&
						 TransactionIdIsCurrentTransactionId(hint_xid))
				{
					/*
					 * Our own insert.  Check sLog for our own delete or for
					 * an ABORTED entry from savepoint rollback.
					 */
					slog_nfound = SLogTupleLookupFiltered(
														  RelationGetRelid(rel), tid,
														  InvalidTransactionId,
														  slog_entries, SLOG_MAX_TUPLE_OPS);
					{
						int			si;
						bool		found_invisible = false;

						for (si = 0; si < slog_nfound; si++)
						{
							if (!TransactionIdEquals(slog_entries[si].xid, hint_xid))
								continue;
							if (slog_entries[si].op_type == SLOG_OP_DELETE ||
								slog_entries[si].op_type == SLOG_OP_ABORTED)
							{
								found_invisible = true;
								break;
							}
						}

						if (found_invisible)
						{
							visible = false;
							goto visibility_done;
						}
					}
					/* Our insert, not deleted/aborted by us -- fall through */
				}
				else
				{
					/*
					 * Invalid hint_xid -- fall back to sLog lookup. This
					 * handles pre-upgrade tuples.
					 */
					slog_nfound = SLogTupleLookupFiltered(
														  RelationGetRelid(rel), tid,
														  InvalidTransactionId,
														  slog_entries, SLOG_MAX_TUPLE_OPS);
					{
						int			si;
						bool		found_inserter = false;

						for (si = 0; si < slog_nfound; si++)
						{
							if (slog_entries[si].op_type == SLOG_OP_INSERT &&
								TransactionIdIsInProgress(slog_entries[si].xid))
							{
								snapshot->xmin = slog_entries[si].xid;
								found_inserter = true;
								break;
							}
							if (slog_entries[si].op_type == SLOG_OP_ABORTED)
							{
								visible = false;
								goto visibility_done;
							}
						}

						if (!found_inserter)
						{
							/* Stale flag, clear it */
							BufferSetHintBits16(&tuple_hdr->t_flags,
												tuple_hdr->t_flags & ~FLUX_TUPLE_UNCOMMITTED,
												buffer);
						}
						else
						{
							visible = true;
							goto visibility_done;
						}
					}
				}
			}

			/* Inserting xact is committed (or ours).  Check deletion. */
			if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
			{
				/*
				 * Deletion state is read from the on-page header, not the
				 * per-tuple sLog (per-page conflict tracking no longer writes
				 * a shared DELETE entry).  The deleter is t_xmax; classify it
				 * via current-xact / in-progress / CLOG, exactly as heap's
				 * HeapTupleSatisfiesDirty does for xmax.  This drives
				 * _bt_check_unique: an in-progress deleter sets snapshot->xmax
				 * (caller waits), our own or a committed delete makes the
				 * entry dead (visible=false -> not a uniqueness conflict), and
				 * an aborted delete leaves the row live.
				 */
				TransactionId del_xid = FluxTupleGetXmax(tuple_hdr);

				if (!TransactionIdIsValid(del_xid))
				{
					/*
					 * DELETED flag with no deleter (xmax invalid).  The
					 * tracked-key COMMIT sweep marks a savepoint-rolled-back
					 * INSERT as DELETED and clears UNCOMMITTED but stamps no
					 * deleter.  With no INSERT undo record, the inserter's
					 * t_xmin is the only rollback signal: if it never
					 * committed the row was never really there, so it is
					 * invisible.  Only a genuinely committed inserter with a
					 * stale DELETED hint is live.
					 */
					TransactionId ins_xid = FluxTupleGetXmin(tuple_hdr);

					if (TransactionIdIsValid(ins_xid) &&
						!TransactionIdIsCurrentTransactionId(ins_xid) &&
						!TransactionIdIsInProgress(ins_xid) &&
						!TransactionIdDidCommit(ins_xid))
					{
						visible = false;
						goto visibility_done;
					}

					BufferSetHintBits16(&tuple_hdr->t_flags,
										tuple_hdr->t_flags & ~FLUX_TUPLE_DELETED,
										buffer);
					/* fall through -- tuple is live */
				}
				else if (TransactionIdIsCurrentTransactionId(del_xid))
				{
					/* We deleted it ourselves. */
					visible = false;
					goto visibility_done;
				}
				else if (TransactionIdIsInProgress(del_xid))
				{
					/* Deleter still running: report xmax so the caller waits. */
					snapshot->xmax = del_xid;
					visible = true;
					goto visibility_done;
				}
				else if (TransactionIdDidCommit(del_xid))
				{
					/* Delete committed: the row (and its index entry) is dead. */
					visible = false;
					goto visibility_done;
				}
				else
				{
					/*
					 * Deleter aborted: the delete did not take effect.  Clear
					 * the stale DELETED hint and treat the tuple as live.
					 */
					BufferSetHintBits16(&tuple_hdr->t_flags,
										tuple_hdr->t_flags & ~FLUX_TUPLE_DELETED,
										buffer);
					/* fall through -- tuple is live */
				}
			}

			/*
			 * Check if this is the old version of an out-of-place update. The
			 * FLUX_TUPLE_UPDATED flag means this tuple has been superseded by
			 * a newer version at t_ctid.  However, the updater may have
			 * aborted — check sLog for ABORTED entries before declaring it
			 * invisible.
			 */
			if (tuple_hdr->t_flags & FLUX_TUPLE_UPDATED)
			{
				/*
				 * FLUX updates in place: an UPDATED tuple whose t_ctid still
				 * points at itself is the live current version, not a
				 * superseded old version.  Only a genuine out-of-place move
				 * (cross-page defrag) repoints t_ctid elsewhere.  Skip the
				 * supersession check for in-place updates so a retained
				 * committed UPDATE marker (kept for before-image serving)
				 * does not make the live row vanish from a dirty-snapshot
				 * probe -- e.g. the ON CONFLICT arbiter scan would otherwise
				 * miss the existing row after any prior UPDATE on it.  This
				 * mirrors the MVCC path's handling in
				 * FluxTupleVisibleToSnapshotDual.
				 */
				if (ItemPointerEquals(&tuple_hdr->t_ctid, tid))
				{
					/* In-place update: on-page tuple is live. */
					visible = true;
					goto visibility_done;
				}

				if (slog_nfound < 0)
					slog_nfound = SLogTupleLookupFiltered(
														  RelationGetRelid(rel), tid,
														  InvalidTransactionId,
														  slog_entries, SLOG_MAX_TUPLE_OPS);
				{
					int			si;
					bool		update_aborted = false;

					for (si = 0; si < slog_nfound; si++)
					{
						if (slog_entries[si].op_type == SLOG_OP_ABORTED)
						{
							update_aborted = true;
							break;
						}
						if ((slog_entries[si].op_type == SLOG_OP_UPDATE) &&
							!TransactionIdIsCurrentTransactionId(slog_entries[si].xid) &&
							TransactionIdDidAbort(slog_entries[si].xid))
						{
							update_aborted = true;
							break;
						}
					}

					if (update_aborted)
					{
						/*
						 * Updater aborted.  Clear stale UPDATED flag via
						 * hint-bits and treat as still-live tuple.
						 */
						BufferSetHintBits16(&tuple_hdr->t_flags,
											tuple_hdr->t_flags &
											~(FLUX_TUPLE_UPDATED |
											  FLUX_TUPLE_KEYS_UPDATED),
											buffer);
						/* Fall through to visible */
					}
					else if (slog_nfound == 0)
					{
						/*
						 * No sLog entries but UPDATED flag is set.  This
						 * means either: (a) the retained UPDATE entry was
						 * reclaimed by the per-TID oldest-entry eviction in
						 * flat_hash_apply_insert (hot row), or (b) the
						 * background worker cleaned up the entry.
						 *
						 * In both cases the update committed — the tuple on
						 * page IS the current version.  Clear the stale
						 * UPDATED flag and treat as visible (live tuple).
						 */
						BufferSetHintBits16(&tuple_hdr->t_flags,
											tuple_hdr->t_flags &
											~(FLUX_TUPLE_UPDATED |
											  FLUX_TUPLE_KEYS_UPDATED),
											buffer);
						/* Fall through to visible */
					}
					else
					{
						/*
						 * Check if updater is current, in-progress, or
						 * committed
						 */
						bool		updater_running = false;

						for (si = 0; si < slog_nfound; si++)
						{
							if (slog_entries[si].op_type != SLOG_OP_UPDATE)
								continue;

							if (TransactionIdIsCurrentTransactionId(slog_entries[si].xid))
							{
								/*
								 * We are the updater — old version is dead.
								 * This mirrors the DELETED handling above.
								 */
								visible = false;
								goto visibility_done;
							}

							if (TransactionIdIsInProgress(slog_entries[si].xid))
							{
								snapshot->xmax = slog_entries[si].xid;
								updater_running = true;
								break;
							}
						}

						if (!updater_running)
						{
							/* Updater committed — old version is dead */
							visible = false;
							goto visibility_done;
						}
						/* Updater still running — tuple visible for now */
					}
				}
			}

			/* LOCKED tuples are still visible (lock != delete) */
			if ((tuple_hdr->t_flags & FLUX_TUPLE_LOCKED) &&
				!(tuple_hdr->t_flags & (FLUX_TUPLE_DELETED | FLUX_TUPLE_UPDATED)))
			{
				visible = true;
				goto visibility_done;
			}

			visible = true;
		}
		else
		{
			visible = FluxTupleVisibleToSnapshotDual(tuple_hdr, snapshot,
													 RelationGetRelid(rel),
													 buffer);
		}
	}
	else
	{
		/*
		 * No snapshot means fetch unconditionally (e.g., system catalog
		 * scans, VACUUM FULL table rewrite).  Only truly deleted tuples are
		 * invisible.  UPDATED tuples are live (they contain the current
		 * version of the data after an in-place update).
		 */
		visible = !(tuple_hdr->t_flags & FLUX_TUPLE_DELETED);
	}
visibility_done:

	/*
	 * SSI bookkeeping for the index-fetch path, mirroring heap (see
	 * heapam_indexscan.c: PredicateLockTID on a visible tuple, and
	 * HeapCheckForSerializableConflictOut otherwise).  A visible tuple leaves a
	 * per-tuple SIREAD lock so a concurrent writer can detect the
	 * rw-antidependency; an invisible one means a concurrent writer already
	 * modified it, which is a conflict-out for our snapshot.
	 *
	 * Both are no-ops outside SERIALIZABLE.  Done here, while we still hold the
	 * buffer's share lock and before any of the before-image substitution paths
	 * below return, so every exit that reports this TID is covered.
	 */
	if (IsolationIsSerializable())
	{
		if (visible)
			PredicateLockTID(rel, tid, snapshot,
							 FluxTupleGetXmin(tuple_hdr));
		else
			FluxCheckForSerializableConflictOut(rel, tuple_hdr, buffer,
											   snapshot);
	}

	if (!visible)
	{
		/*
		 * zheap read path (index fetch): the on-page (newest) version is not
		 * visible to our snapshot, but if this tuple was updated in place and
		 * still has a version chain in the UNDO log, an OLDER version may be
		 * visible.  Reconstruct it and serve the before-image, mirroring the
		 * sequential-scan path.  This is what lets a REPEATABLE READ reader
		 * whose snapshot predates a committed UPDATE still find the row at
		 * its old indexed key.
		 */
		if ((tuple_hdr->t_flags & FLUX_TUPLE_UPDATED) &&
			snapshot != NULL && IsMVCCSnapshot(snapshot) &&
			UndoRecPtrIsValid(FluxTupleGetVersionPtr(tuple_hdr,
														ItemIdGetLength(itemid))))
		{
			char	   *bi_data = NULL;
			int			bi_len = 0;

			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			if (FluxReconstructVisibleVersion(rel, tid,
											  (const char *) tuple_hdr,
											  ItemIdGetLength(itemid),
											  snapshot,
											  &bi_data, &bi_len))
			{
				FluxTupleHeader *bi_tuple = (FluxTupleHeader *) bi_data;

				FluxSlotStoreMaterializedTuple(tts, bi_tuple, bi_len);
				tts->tts_tableOid = RelationGetRelid(rel);
				ItemPointerCopy(tid, &tts->tts_tid);
				ReleaseBuffer(buffer);
				return true;
			}
			ReleaseBuffer(buffer);
			return false;
		}

		/*
		 * Set the all_dead hint only if the tuple is committed-deleted AND
		 * its deleter (t_xmax) precedes the oldest-xmin horizon, meaning no
		 * running or future snapshot can still see it.  FluxTupleDeadToAll
		 * does the heap-shaped XID-horizon check (t_xmax committed and <
		 * oldest_xmin); this replaced the HLC-era FluxCanVacuumTimestamp,
		 * which compared the on-page word as a wall-clock timestamp -- WRONG
		 * post-pivot, since that word now packs the XID-based t_xmax.
		 *
		 * Setting all_dead prematurely would let the index AM remove the
		 * entry while concurrent transactions still need it.
		 */
		if (FluxTupleDeadToAll(tuple_hdr, FluxGetOldestXminHorizon(rel)))
		{
			if (all_dead)
				*all_dead = true;
		}

		/*
		 * Do NOT set call_again for UPDATED tuples.  For out-of-place updates
		 * the executor inserts new index entries (TU_All), so the index scan
		 * will naturally find the new version through its own index entry.
		 * Setting call_again=true here with the same tid causes an infinite
		 * loop because the caller retries with the unchanged tid parameter.
		 */

		UnlockReleaseBuffer(buffer);
		return false;
	}

	/*
	 * Unlock the buffer before materializing the slot. We keep the pin to
	 * ensure the page doesn't get evicted. We must unlock here because
	 * FluxTupleToSlotWithOverflow may need to fetch overflow data, and if
	 * that overflow is on the same page, it would try to lock an
	 * already-locked buffer causing an assertion failure.
	 */
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	/*
	 * Before-image substitution for in-place UPDATEs (zheap read path),
	 * mirroring the sequential-scan path (flux_getnextslot) and the
	 * fetch-by-TID path (flux_tuple_fetch_row_version).  If the reader can
	 * see the updater's xmin the on-page value is correct and the callee
	 * returns false (serve on-page); if not, an older visible version is
	 * reconstructed from the UNDO log (WS-PVS2) so a REPEATABLE
	 * READ / SERIALIZABLE reader whose snapshot cannot see the update
	 * observes the before-image through any index -- including one whose key
	 * the UPDATE never touched (e.g. a primary-key lookup).  Visibility is
	 * ordinary heap-shaped xmin/xmax + CLOG + snapshot (XidInMVCCSnapshot in
	 * the callee).
	 *
	 * No index-key recheck is needed: FLUX does in-place updates only for
	 * NON-key columns (key changes go out of place to a new TID), so the
	 * before-image reached through any index entry has the same key as the
	 * stored entry that led here.
	 */
	if ((tuple_hdr->t_flags & FLUX_TUPLE_UPDATED) &&
		!(tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED) &&
		snapshot != NULL && IsMVCCSnapshot(snapshot) &&
		FluxDirtyMapCheck(RelationGetRelid(rel),
						  ItemPointerGetBlockNumber(tid)))
	{
		char	   *bi_data = NULL;
		int			bi_len = 0;

		if (FluxReconstructVisibleVersion(rel, tid,
										  (const char *) tuple_hdr,
										  ItemIdGetLength(itemid),
										  snapshot,
										  &bi_data, &bi_len))
		{
			FluxTupleHeader *bi_tuple = (FluxTupleHeader *) bi_data;

			FluxSlotStoreMaterializedTuple(tts, bi_tuple, bi_len);
			tts->tts_tableOid = RelationGetRelid(rel);
			tts->tts_tid = *tid;

			scan->buffer = buffer;
			return true;
		}
	}

	/*
	 * Tuple is visible.  A committed-DELETED tuple can still be visible here:
	 * an RR/SERIALIZABLE reader whose snapshot predates the delete sees the
	 * row (FluxTupleVisibleToSnapshotDual already returned true because the
	 * deleter xid is not visible to this snapshot).
	 * FluxTupleToSlotWithOverflow refuses a DELETED tuple outright (it can't
	 * tell visible-to-this-snapshot from physically-dead), so serve the
	 * on-page image via FluxSlotStoreTuple -- the same virtual-slot store the
	 * sequential-scan path uses, which has no DELETED reject.  This mirrors
	 * seqscan and fixes a lost-row/wrong-result under REPEATABLE READ via an
	 * index path.
	 */
	if (visible && (tuple_hdr->t_flags & FLUX_TUPLE_DELETED))
	{
		FluxSlotStoreTuple(tts, tuple_hdr, ItemIdGetLength(itemid), buffer);
		tts->tts_tableOid = RelationGetRelid(rel);
		tts->tts_tid = *tid;

		/*
		 * Materialize while the page is still pinned so the returned varlena
		 * datums are copied into slot-owned memory and never dangle if the
		 * pinned buffer is released/reused before the caller consumes them.
		 */
		ExecMaterializeSlot(tts);
		ReleaseBuffer(buffer);
		return true;
	}

	/* Tuple is visible - convert to slot (with overflow fetch) */
	if (FluxTupleToSlotWithOverflow(tuple_hdr, tts, rel))
	{
		tts->tts_tableOid = RelationGetRelid(rel);
		tts->tts_tid = *tid;
		/* Slot is already marked valid by FluxTupleToSlotWithOverflow */
	}
	else
	{
		ReleaseBuffer(buffer);
		return false;
	}

	/*
	 * Materialize while the page is still pinned: FluxTupleToSlotWithOverflow
	 * stored pointers into the buffer page for non-overflow columns.  Copying
	 * them into slot-owned memory now means the returned datums do not depend
	 * on the buffer staying pinned, so a caller that reads them after the
	 * scan advances (releasing/reusing the buffer) cannot dangle.  This
	 * matches the lifetime guarantee heap gives once a fetched slot is handed
	 * upward.
	 */
	ExecMaterializeSlot(tts);
	ReleaseBuffer(buffer);

	return true;
}

/*
 * ------------------------------------------------------------------------
 * Tuple manipulation callbacks for FLUX AM
 * ------------------------------------------------------------------------
 */

/*
 * Fetch tuple at given TID
 */
static bool
flux_tuple_fetch_row_version(Relation relation,
							 ItemPointer tid,
							 Snapshot snapshot,
							 TupleTableSlot *slot)
{
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);
	OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
	Buffer		buffer;
	Page		page;
	ItemId		itemid;
	FluxTupleHeader *tuple_hdr;
	bool		visible = false;

	/* Clear the slot */
	ExecClearTuple(slot);

	/* Read the page */
	buffer = ReadBuffer(relation, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buffer);

	/* Validate offset */
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
	{
		UnlockReleaseBuffer(buffer);
		return false;
	}

	/* Get the item */
	itemid = PageGetItemId(page, offnum);

	if (!ItemIdIsNormal(itemid))
	{
		UnlockReleaseBuffer(buffer);
		return false;
	}

	/* Get tuple header */
	tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

	/*
	 * Check visibility (heap-shaped xmin/xmax). Special case: SnapshotAny is
	 * used by DELETE RETURNING to fetch the just-deleted tuple, so we must
	 * allow deleted tuples in that case.
	 */
	if (snapshot && snapshot != SnapshotAny)
	{
		/* For normal snapshots, check visibility */
		visible = FluxTupleVisibleToSnapshotDual(tuple_hdr, snapshot,
												 RelationGetRelid(relation),
												 buffer);

		/*
		 * If the visibility function says invisible, the tuple is not
		 * visible.  Do NOT additionally check FLUX_TUPLE_DELETED here — the
		 * visibility function already consulted the sLog to determine if the
		 * delete is committed/in-progress/aborted.
		 */
		if (!visible)
		{
			UnlockReleaseBuffer(buffer);
			return false;
		}
	}
	else if (snapshot == SnapshotAny)
	{
		/* SnapshotAny: fetch any tuple, even if deleted (for RETURNING) */
		visible = true;
	}
	else
	{
		/* No snapshot means fetch unconditionally if not deleted */
		if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
		{
			UnlockReleaseBuffer(buffer);
			return false;
		}
		visible = true;
	}

	if (!visible)
	{
		UnlockReleaseBuffer(buffer);
		return false;
	}

	/*
	 * Before-image substitution for committed in-place UPDATEs, mirroring the
	 * sequential-scan path.  An index fetch that lands on a tuple updated in
	 * place by a transaction not visible to our MVCC snapshot must serve the
	 * before-image, not the on-page (new) value -- otherwise an UPDATE driven
	 * by this fetch (e.g. UPDATE ... WHERE pk = const) would recompute on top
	 * of a concurrent committed update and silently lose it.  Serve from the
	 * UNDO log (WS-PVS2).  Visibility is ordinary heap-shaped
	 * xmin/xmax + CLOG + snapshot (XidInMVCCSnapshot in the callee): if the
	 * reader can see the updater the callee returns false and the on-page
	 * value is served.
	 */
	if ((tuple_hdr->t_flags & FLUX_TUPLE_UPDATED) &&
		!(tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED) &&
		snapshot != NULL && IsMVCCSnapshot(snapshot) &&
		FluxDirtyMapCheck(RelationGetRelid(relation),
						  ItemPointerGetBlockNumber(tid)))
	{
		char	   *bi_data = NULL;
		int			bi_len = 0;

		if (FluxReconstructVisibleVersion(relation, tid,
										  (const char *) tuple_hdr,
										  ItemIdGetLength(itemid),
										  snapshot,
										  &bi_data, &bi_len))
		{
			FluxTupleHeader *bi_tuple = (FluxTupleHeader *) bi_data;

			FluxSlotStoreMaterializedTuple(slot, bi_tuple, bi_len);
			slot->tts_tableOid = RelationGetRelid(relation);
			slot->tts_tid = *tid;
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buffer);
			return true;
		}
	}

	/* Store tuple into slot with buffer pin for safe access */
	FluxSlotStoreTuple(slot, tuple_hdr,
					   ItemIdGetLength(itemid), buffer);
	slot->tts_tableOid = RelationGetRelid(relation);
	slot->tts_tid = *tid;

	/*
	 * Materialize while the page is still pinned.  FluxSlotStoreTuple leaves
	 * the slot pointing at the on-page tuple, and this function's principal
	 * caller is the executor's "fetch the old tuple before updating it" step,
	 * which then performs an IN-PLACE update of that very tuple.  Without a
	 * copy, the slot's datums alias bytes that the update is about to
	 * overwrite, so the RETURNING projection reads the new row through the OLD
	 * slot.  Copying now also gives the slot the same lifetime guarantee the
	 * index-fetch path above provides (datums survive the buffer being
	 * unpinned/reused).
	 */
	ExecMaterializeSlot(slot);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);

	return true;
}

/*
 * Check if TID is valid for relation scan
 */
static bool
flux_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	BlockNumber nblocks = RelationGetNumberOfBlocks(scan->rs_rd);

	return ItemPointerIsValid(tid) &&
		ItemPointerGetBlockNumber(tid) < nblocks;
}

/*
 * Get latest version of tuple (for updates)
 */
static void
flux_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	/* FLUX uses in-place updates, so TID doesn't change */
	/* But we need to follow update chains if they exist */

	BlockNumber block = ItemPointerGetBlockNumber(tid);
	OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
	Buffer		buffer;
	Page		page;
	ItemId		itemid;
	FluxTupleHeader *tuple_hdr;

	buffer = ReadBuffer(scan->rs_rd, block);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buffer);
	itemid = PageGetItemId(page, offnum);

	if (ItemIdIsNormal(itemid))
	{
		tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

		/* Follow update chain if needed */
		if (tuple_hdr->t_flags & FLUX_TUPLE_UPDATED)
		{
			ItemPointerCopy(&tuple_hdr->t_ctid, tid);
		}
	}

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buffer);
}

/*
 * Check if tuple satisfies snapshot
 */
static bool
flux_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
							  Snapshot snapshot)
{
	Buffer		buffer;
	Page		page;
	ItemId		itemid;
	FluxTupleHeader *tuple_hdr;
	BlockNumber blkno;
	OffsetNumber offnum;
	bool		visible;

	/*
	 * Re-fetch the on-disk tuple header by TID so we can check the real
	 * commit timestamps and transaction status.  The previous implementation
	 * used flux_tuple_from_slot() which fabricated a new tuple with current
	 * timestamps, making the visibility check meaningless.
	 */
	if (!ItemPointerIsValid(&slot->tts_tid))
		return false;

	blkno = ItemPointerGetBlockNumber(&slot->tts_tid);
	offnum = ItemPointerGetOffsetNumber(&slot->tts_tid);

	buffer = ReadBuffer(rel, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);

	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
	{
		UnlockReleaseBuffer(buffer);
		return false;
	}

	itemid = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(itemid))
	{
		UnlockReleaseBuffer(buffer);
		return false;
	}

	tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

	/*
	 * For SNAPSHOT_DIRTY, we need to emulate HeapTupleSatisfiesDirty():
	 * return the inserting xid and speculative token through the snapshot so
	 * that callers like _bt_check_unique / SpeculativeInsertionWait can
	 * properly wait for or detect speculative insertions.
	 *
	 * With the sLog migration, t_xmin/t_xmax no longer exist in the tuple
	 * header.  We query the sLog for in-progress transaction state.
	 */
	if (snapshot->snapshot_type == SNAPSHOT_DIRTY)
	{
		ItemPointerData item_tid;

		snapshot->xmin = InvalidTransactionId;
		snapshot->xmax = InvalidTransactionId;
		snapshot->speculativeToken = 0;

		ItemPointerSet(&item_tid, blkno, offnum);

		/*
		 * Check if this tuple is uncommitted (inserted by a still-running
		 * transaction).
		 */
		if (tuple_hdr->t_flags & FLUX_TUPLE_UNCOMMITTED)
		{
			bool		is_insert = false;
			TransactionId dirty_xid;

			dirty_xid = SLogTupleGetDirtyXid(RelationGetRelid(rel),
											 &item_tid, &is_insert);

			if (TransactionIdIsValid(dirty_xid) && is_insert)
			{
				/*
				 * Another transaction is inserting this tuple. Report xmin
				 * for SpeculativeInsertionWait.
				 */
				snapshot->xmin = dirty_xid;

				if (tuple_hdr->t_flags & FLUX_TUPLE_SPECULATIVE)
				{
					snapshot->speculativeToken =
						ItemPointerGetBlockNumber(&tuple_hdr->t_ctid);
				}

				UnlockReleaseBuffer(buffer);
				return true;	/* visible for dirty snapshot purposes */
			}
			else if (!TransactionIdIsValid(dirty_xid))
			{
				/*
				 * No in-progress sLog entry found.  Check for aborted insert
				 * with pending UNDO.
				 */
				if (SLogTupleHasAbortedEntry(RelationGetRelid(rel),
											 &item_tid))
				{
					UnlockReleaseBuffer(buffer);
					return false;
				}

				/*
				 * The tuple's t_xmin is authoritative for the inserter; the
				 * sLog is consulted here for speculative-insert / command-id
				 * state on UNCOMMITTED tuples.
				 */
				{
					SLogTupleOp my_entry;
					int			nfound;
					TransactionId myxid = GetCurrentTransactionIdIfAny();

					if (TransactionIdIsValid(myxid))
					{
						nfound = SLogTupleLookupFiltered(RelationGetRelid(rel),
														 &item_tid, myxid,
														 &my_entry, 1);
						if (nfound > 0 &&
							my_entry.op_type != SLOG_OP_DELETE)
						{
							/* Our own insert or in-place update */
						}
						else if (nfound > 0)
						{
							/* Our own delete */
							UnlockReleaseBuffer(buffer);
							return false;
						}
						else
						{
							/*
							 * Stale UNCOMMITTED flag. Clear and fall through.
							 */
							if (BufferIsValid(buffer))
								BufferSetHintBits16(&tuple_hdr->t_flags,
													tuple_hdr->t_flags & ~FLUX_TUPLE_UNCOMMITTED,
													buffer);
							else
								tuple_hdr->t_flags &=
									~FLUX_TUPLE_UNCOMMITTED;
						}
					}
					else
					{
						/*
						 * No current transaction, stale flag.
						 */
						if (BufferIsValid(buffer))
							BufferSetHintBits16(&tuple_hdr->t_flags,
												tuple_hdr->t_flags & ~FLUX_TUPLE_UNCOMMITTED,
												buffer);
						else
							tuple_hdr->t_flags &=
								~FLUX_TUPLE_UNCOMMITTED;
					}
				}
			}
		}

		/* Tuple is committed (or ours).  Check if deleted. */
		if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
		{
			bool		is_insert = false;
			TransactionId del_xid;

			del_xid = SLogTupleGetDirtyXid(RelationGetRelid(rel),
										   &item_tid, &is_insert);

			if (TransactionIdIsValid(del_xid) && !is_insert)
			{
				/* Deleter still running */
				snapshot->xmax = del_xid;
				UnlockReleaseBuffer(buffer);
				return true;
			}
			else if (SLogTupleIsDeletedByMe(RelationGetRelid(rel),
											&item_tid))
			{
				/* We deleted it ourselves */
				UnlockReleaseBuffer(buffer);
				return false;
			}
			else
			{
				/*
				 * Tuple is marked DELETED with no in-progress deleter --
				 * deletion is committed.
				 */
				UnlockReleaseBuffer(buffer);
				return false;
			}
		}

		/*
		 * Check if tuple has been superseded by an out-of-place update. For
		 * cross-page updates, t_ctid points to the new version's TID
		 * (different from this tuple's position).  The old version is dead
		 * for index unique-check purposes.
		 *
		 * For in-place updates, t_ctid is self-referencing (points to the
		 * same TID as the tuple's own position).  These tuples are live.
		 */
		if ((tuple_hdr->t_flags & FLUX_TUPLE_UPDATED) &&
			!ItemPointerEquals(&tuple_hdr->t_ctid, &item_tid))
		{
			UnlockReleaseBuffer(buffer);
			return false;
		}

		/* Speculative but our own txn and not yet confirmed -- visible */
		UnlockReleaseBuffer(buffer);
		return true;
	}

	visible = FluxTupleVisibleToSnapshotDual(tuple_hdr, snapshot,
											 RelationGetRelid(rel), buffer);

	UnlockReleaseBuffer(buffer);

	return visible;
}

/*
 * Speculative tuple insertion for FLUX
 * This is used for INSERT ... ON CONFLICT operations
 */
static void
flux_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
							  CommandId cid, uint32 options,
							  BulkInsertState bistate, uint32 specToken)
{
	FluxTuple	tuple;
	Buffer		buf;
	Page		page;
	Size		tuple_size;
	BlockNumber target_block;
	OffsetNumber offnum;
	FluxTupleHeader *tuple_hdr;
	uint64		commit_ts;
	FluxOverflowBuffers overflow_buffers;
	int			i;

	slot_getallattrs(slot);

	/* Create FLUX tuple from slot with overflow support */
	overflow_buffers.count = 0;
	tuple = FluxFormTuple(RelationGetDescr(relation),
						  slot->tts_values, slot->tts_isnull,
						  relation, &overflow_buffers);
	tuple_size = tuple->t_len;

	/*
	 * Get timestamp BEFORE entering critical section, as this may allocate
	 * memory.
	 */
	commit_ts = FluxGetCommitTimestamp();

	/*
	 * Mark the tuple as uncommitted so that SNAPSHOT_DIRTY callers can detect
	 * in-progress insertions via the sLog.  The sLog entry is registered
	 * after the tuple is placed on the page (below) so that the TID is valid.
	 */
	tuple->t_data->t_flags |= FLUX_TUPLE_UNCOMMITTED;
	tuple->t_data->t_xmin = GetCurrentTransactionId();	/* subxid: heap-shaped,
														 * so savepoint rollback
														 * marks it aborted in
														 * CLOG */

	/*
	 * Use the FSM to find a page with enough free space, or extend the
	 * relation with a properly initialized new page.
	 */
	target_block = FluxGetPageWithFreeSpace(relation, tuple_size);
	if (target_block == InvalidBlockNumber)
	{
		/* Clean up overflow buffers before throwing error */
		for (i = 0; i < overflow_buffers.count; i++)
		{
			UnlockReleaseBuffer(overflow_buffers.buffers[i].buffer);
			pfree(overflow_buffers.buffers[i].record_data);
		}
		FluxFreeTuple(tuple);
		elog(ERROR, "FLUX failed to allocate page for speculative insertion");
	}

	buf = ReadBuffer(relation, target_block);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buf);

	/* Verify page has sufficient space */
	if (PageGetFreeSpace(page) < tuple_size)
	{
		/* FSM was stale, update and retry */
		FluxRecordFreeSpace(relation, target_block, PageGetFreeSpace(page));
		UnlockReleaseBuffer(buf);

		target_block = FluxGetPageWithFreeSpace(relation, tuple_size);
		if (target_block == InvalidBlockNumber)
		{
			/* Clean up overflow buffers before throwing error */
			for (i = 0; i < overflow_buffers.count; i++)
			{
				UnlockReleaseBuffer(overflow_buffers.buffers[i].buffer);
				pfree(overflow_buffers.buffers[i].record_data);
			}
			FluxFreeTuple(tuple);
			elog(ERROR, "FLUX failed to allocate page for speculative insertion after retry");
		}

		buf = ReadBuffer(relation, target_block);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);

		if (PageGetFreeSpace(page) < tuple_size)
		{
			/*
			 * Both FSM attempts returned stale pages.  Use P_NEW as final
			 * fallback — extend the relation to get a guaranteed- empty
			 * page.  Update FSM for accuracy on the bad page.
			 */
			FluxRecordFreeSpace(relation, BufferGetBlockNumber(buf),
								PageGetFreeSpace(page));
			UnlockReleaseBuffer(buf);

			buf = ReadBuffer(relation, P_NEW);
			LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(buf);
			FluxInitPage(page, BufferGetPageSize(buf));
		}
	}

	/*
	 * Try adding tuple before critical section.  If the page is too full (FSM
	 * was optimistic about alignment/line-pointer overhead), extend the
	 * relation and use a fresh page.
	 */
	offnum = PageAddItem(page, tuple->t_data, tuple_size,
						 InvalidOffsetNumber, false, false);
	if (offnum == InvalidOffsetNumber)
	{
		FluxRecordFreeSpace(relation, BufferGetBlockNumber(buf),
							PageGetFreeSpace(page));
		UnlockReleaseBuffer(buf);

		buf = ReadBuffer(relation, P_NEW);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		FluxInitPage(page, BufferGetPageSize(buf));

		offnum = PageAddItem(page, tuple->t_data, tuple_size,
							 InvalidOffsetNumber, false, false);
		if (offnum == InvalidOffsetNumber)
			elog(ERROR, "failed to add FLUX tuple to fresh page during "
				 "speculative insert (tuple_size=%zu)", tuple_size);
	}

	/* NO EREPORT(ERROR) from here till changes are logged */
	START_CRIT_SECTION();

	/* Mark as speculative insertion */
	tuple_hdr = (FluxTupleHeader *) PageGetItem(page, PageGetItemId(page, offnum));
	tuple_hdr->t_flags |= FLUX_TUPLE_SPECULATIVE;
	tuple_hdr->t_commit_ts = 0; /* heap-shaped: t_xmax = Invalid */

	/*
	 * Store the speculative insertion token in t_ctid, using the same
	 * encoding as heap: block number holds the token, offset is set to
	 * SpecTokenOffsetNumber so callers can distinguish a token from a real
	 * TID.
	 */
	ItemPointerSet(&tuple_hdr->t_ctid, specToken, SpecTokenOffsetNumber);

	/* Set TID in slot */
	ItemPointerSet(&slot->tts_tid, BufferGetBlockNumber(buf), offnum);
	MarkBufferDirty(buf);

	/* WAL logging */
	if (RelationNeedsWAL(relation))
	{
		XLogRecPtr	recptr;
		xl_flux_insert xlrec;

		xlrec.offnum = offnum;
		xlrec.flags = FLUX_TUPLE_SPECULATIVE;
		xlrec.tuple_len = (uint32) tuple_size;
		xlrec.commit_ts = 0;	/* heap-shaped: t_xmax = Invalid */

		/*
		 * Force a full-page image and append the tuple body to the main data
		 * channel (matching FluxXLogInsert's layout, which flux_xlog_insert_
		 * redo parses).  The redo handler restores the tuple from the FPI and
		 * never reaches the PageAddItem path for these records; the body is
		 * carried so logical decoding and any future non-FPI replay see a
		 * well-formed record.  Registering the body as block data instead
		 * (the historical behavior) left tuple_len uninitialized and made
		 * redo read past the record on a BLK_NEEDS_REDO replay.
		 */
		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD | REGBUF_FORCE_IMAGE);
		XLogRegisterData((char *) &xlrec, sizeof(xl_flux_insert));
		XLogRegisterData((char *) tuple->t_data, tuple_size);

		/* Register overflow buffers if any */
		if (overflow_buffers.count > 0)
		{
			for (i = 0; i < overflow_buffers.count; i++)
			{
				FluxOverflowBuffer *ovb = &overflow_buffers.buffers[i];

				/* Register the overflow buffer */
				XLogRegisterBuffer(i + 1, ovb->buffer, REGBUF_STANDARD);

				/* Register the overflow record data */
				XLogRegisterBufData(i + 1, ovb->record_data, ovb->record_len);
			}
		}

		recptr = XLogInsert(RM_FLUX_ID, XLOG_FLUX_INSERT);
		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/*
	 * Register the speculative insertion in the sLog so that SNAPSHOT_DIRTY
	 * callers can find the inserting xid via SLogTupleGetDirtyXid().
	 */
	FluxEnsureSLogCallbacks();
	SLogTupleInsert(RelationGetRelid(relation), &slot->tts_tid,
					GetTopTransactionId(), SLOG_OP_INSERT,
					GetCurrentSubTransactionId(), cid, commit_ts,
					specToken, LockTupleNoKeyExclusive);

	/* Update FSM with remaining free space */
	FluxRecordFreeSpace(relation, BufferGetBlockNumber(buf),
						PageGetFreeSpace(page));

	UnlockReleaseBuffer(buf);

	/* Release overflow buffers, deduplicating shared buffers */
	for (i = 0; i < overflow_buffers.count; i++)
	{
		Buffer		ovf_buf = overflow_buffers.buffers[i].buffer;
		bool		already_released = (ovf_buf == buf);
		int			j;

		for (j = 0; j < i && !already_released; j++)
		{
			if (overflow_buffers.buffers[j].buffer == ovf_buf)
				already_released = true;
		}

		if (!already_released)
			UnlockReleaseBuffer(ovf_buf);
		pfree(overflow_buffers.buffers[i].record_data);
	}

	FluxFreeTuple(tuple);
}

/*
 * Complete speculative insertion for FLUX
 */
static void
flux_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
								uint32 specToken, bool succeeded)
{
	ItemPointer tid = &slot->tts_tid;
	Buffer		buf;
	Page		page;
	ItemId		itemid;
	FluxTupleHeader *tuple_hdr;

	buf = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buf);

	itemid = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
	if (!ItemIdIsNormal(itemid))
	{
		UnlockReleaseBuffer(buf);
		return;
	}

	tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

	if (succeeded)
	{
		/*
		 * Speculative insertion succeeded.  Clear the speculative flag and
		 * restore t_ctid to point to the tuple itself (removing the
		 * speculative token), mirroring heap_finish_speculative().
		 *
		 * Dirty-read hole fix: this used to ALSO clear FLUX_TUPLE_UNCOMMITTED
		 * here, i.e. well before the inserting transaction actually commits.
		 * That made the row immediately visible to every concurrent snapshot
		 * the instant the speculative insert was confirmed (INSERT ... ON
		 * CONFLICT DO NOTHING/UPDATE), a straight dirty read -- HEAP never
		 * exposes an uncommitted speculative tuple this way.  The speculative
		 * INSERT already registered a real shared sLog entry (SLOG_OP_INSERT,
		 * see flux_tuple_insert_speculative), so leaving
		 * FLUX_TUPLE_UNCOMMITTED set here is safe and correct: the existing
		 * commit-time machinery (FluxClearUncommittedFlags /
		 * flux_stamp_tuple_committed, driven by the tracked sLog key) clears
		 * it and clears the UNCOMMITTED hint at actual transaction commit,
		 * exactly like a plain INSERT.
		 */
		START_CRIT_SECTION();

		tuple_hdr->t_flags &= ~FLUX_TUPLE_SPECULATIVE;
		ItemPointerSet(&tuple_hdr->t_ctid,
					   ItemPointerGetBlockNumber(tid),
					   ItemPointerGetOffsetNumber(tid));

		MarkBufferDirty(buf);

		/* WAL-log the confirmation */
		if (RelationNeedsWAL(relation))
		{
			XLogRecPtr	recptr;
			xl_flux_insert xlrec;

			xlrec.offnum = ItemPointerGetOffsetNumber(tid);
			xlrec.flags = 0;	/* cleared SPECULATIVE */
			xlrec.tuple_len = 0;	/* no body: confirm only flips flags */
			xlrec.commit_ts = tuple_hdr->t_commit_ts;

			/*
			 * Confirmation does not add a tuple; it clears the speculative
			 * flag on a tuple already present on the page.  Force a full-page
			 * image so redo restores the (already-updated) page rather than
			 * re-adding the tuple via PageAddItem.  tuple_len == 0 keeps the
			 * record body-less, which flux_xlog_insert_redo treats as
			 * "nothing to add" on the (unreachable, given the forced FPI)
			 * non-FPI path.
			 */
			XLogBeginInsert();
			XLogRegisterBuffer(0, buf, REGBUF_STANDARD | REGBUF_FORCE_IMAGE);
			XLogRegisterData((char *) &xlrec, sizeof(xl_flux_insert));

			recptr = XLogInsert(RM_FLUX_ID, XLOG_FLUX_INSERT);
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();
	}
	else
	{
		TransactionId abort_xid;

		/*
		 * Speculative abort: mark the tuple deleted by the same xid that
		 * inserted it (the current subxid stamped in t_xmin), so the
		 * inserter and deleter commit or roll back together and the tuple
		 * is dead to every snapshot from here on.  The transaction itself
		 * goes on to retry the arbiter check, so this must not rely on an
		 * abort; the DIRTY-snapshot fetch recognises t_xmin == t_xmax as
		 * dead, and VACUUM reclaims it once the xid is committed and past
		 * the horizon.
		 */
		abort_xid = GetCurrentTransactionId();
		Assert(TransactionIdEquals(abort_xid, FluxTupleGetXmin(tuple_hdr)));

		START_CRIT_SECTION();

		tuple_hdr->t_flags |= FLUX_TUPLE_DELETED;
		FluxTupleSetXmax(tuple_hdr, abort_xid);

		MarkBufferDirty(buf);

		/* WAL log the speculative abort as a delete */
		if (RelationNeedsWAL(relation))
		{
			XLogRecPtr	recptr;
			xl_flux_delete xlrec;

			xlrec.offnum = ItemPointerGetOffsetNumber(tid);
			xlrec.flags = 0;
			xlrec.tuple_len = ItemIdGetLength(itemid);
			xlrec.commit_ts = (uint64) abort_xid;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, sizeof(xl_flux_delete));
			XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
			XLogRegisterBufData(0, (char *) tuple_hdr, ItemIdGetLength(itemid));

			recptr = XLogInsert(RM_FLUX_ID, XLOG_FLUX_DELETE);
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();
	}

	UnlockReleaseBuffer(buf);
}

/*
 * Lock a tuple in FLUX table
 */
static TM_Result
flux_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
				TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
				LockWaitPolicy wait_policy, uint8 flags,
				TM_FailureData *tmfd)
{
	Buffer		buf;
	Page		page;
	ItemId		itemid;
	FluxTupleHeader *tuple_hdr;
	TM_Result	result = TM_Ok;
	TransactionId current_xid;
	bool		have_tuple_lock = false;
	char	   *keyintact_image = NULL;

	/*
	 * Get transaction XID BEFORE entering critical section, as this may
	 * allocate memory.
	 */
	current_xid = GetTopTransactionId();

reacquire:

	/*
	 * Any retry re-reads the header from scratch, so a before-image captured
	 * by the key-intact fast path on a previous pass is stale: drop it.  This
	 * is the single point that guarantees no skip decision (and no image) is
	 * ever carried across a buffer-lock gap.
	 */
	if (keyintact_image != NULL)
	{
		pfree(keyintact_image);
		keyintact_image = NULL;
	}

	buf = ReadBuffer(relation, ItemPointerGetBlockNumber(tid));
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buf);

	itemid = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
	if (!ItemIdIsNormal(itemid))
	{
		UnlockReleaseBuffer(buf);
		return TM_Invisible;
	}

	tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

	/*
	 * Already modified by our own transaction in this or a later command.
	 * heap_lock_tuple gets this verdict straight from
	 * HeapTupleSatisfiesUpdate(); FLUX must supply it explicitly.  MERGE's
	 * ExecMergeMatched() relies on it: when a BEFORE trigger modifies the row
	 * being merged, the lock must report TM_SelfModified so the executor
	 * raises "tuple to be updated or deleted was already modified ...".
	 * Without it, ExecMergeMatched loops back to lmerge_matched forever, which
	 * showed up as a backend growing to 60 GB until the OOM killer fired.
	 *
	 * Note the FLUX_TUPLE_DELETED handling below must come after this, so our
	 * own in-progress delete is not misreported as a concurrent TM_Deleted.
	 */
	{
		CommandId	self_cmax;

		if (FluxCheckSelfModified(relation, tid, tuple_hdr, cid, &self_cmax))
		{
			if (tmfd)
			{
				tmfd->ctid = *tid;
				tmfd->xmax = InvalidTransactionId;
				tmfd->cmax = self_cmax;
				tmfd->traversed = false;
			}
			UnlockReleaseBuffer(buf);
			return TM_SelfModified;
		}
	}

	/*
	 * Tuple is delete-marked.  FLUX_TUPLE_DELETED alone does NOT mean the row
	 * is gone -- it means *someone* stamped a delete, and that delete may
	 * still be in progress.  Reporting TM_Deleted straight off the flag made a
	 * locker treat an UNCOMMITTED delete as final: SELECT ... FOR SHARE / FOR
	 * KEY SHARE against a row with a pending DELETE returned the EMPTY set
	 * immediately, where heap blocks until the deleter commits or aborts
	 * (heap_delete stamps xmax with LockTupleExclusive, so heap_lock_tuple
	 * takes its ordinary xmax wait).  If the deleter then ROLLS BACK that is a
	 * WRONG RESULT -- the row is still live and lockable -- and for an FK
	 * parent probe (ri_LockPKTuple, LockTupleKeyShare) it is an integrity
	 * hole: the FK child is admitted against a parent row the locker was told
	 * does not exist.
	 *
	 * So resolve the deleter before answering, mirroring what
	 * flux_tuple_delete already does for a concurrent delete: wait out an
	 * in-progress non-self deleter, then jump back to 'reacquire' and decide
	 * on the re-read, post-wait header.  An aborted delete has had its
	 * before-image restored by UNDO (FLUX_TUPLE_DELETED cleared), so the retry
	 * finds a live row and locks it normally.  A committed delete, and our own
	 * delete, still report TM_Deleted.
	 *
	 * This check sits after the TM_SelfModified handling above so our own
	 * in-progress delete is never misreported as a concurrent one.
	 */
	if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
	{
		TransactionId del_xid;
		bool		del_is_insert = false;

		del_xid = flux_header_dirty_xid(tuple_hdr, &del_is_insert);

		if (TransactionIdIsValid(del_xid) && !del_is_insert)
		{
			/*
			 * In-progress DELETE by another transaction.  Honour the caller's
			 * wait policy exactly as the writer-wait branch below does, so
			 * NOWAIT/SKIP LOCKED keep their semantics.
			 */
			if (wait_policy == LockWaitBlock)
			{
				TransactionId wait_xid = del_xid;

				UnlockReleaseBuffer(buf);
				if (!have_tuple_lock)
					FluxLockTuple(relation, tid, mode, true, &have_tuple_lock);
				XactLockTableWait(wait_xid, relation, tid, XLTW_Lock);
				goto reacquire;
			}
			else if (wait_policy == LockWaitError)
			{
				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = del_xid;
					tmfd->cmax = InvalidCommandId;
				}
				result = TM_WouldBlock;
				goto out_unlock;
			}
			else				/* LockWaitSkip */
			{
				result = TM_WouldBlock;
				goto out_unlock;
			}
		}

		/* Deleter is committed, aborted, or ourselves: the mark stands. */
		if (tmfd)
		{
			tmfd->ctid = *tid;
			tmfd->xmax = InvalidTransactionId;
			tmfd->cmax = InvalidCommandId;
		}
		result = TM_Deleted;
		goto out_unlock;
	}

	/*
	 * Check visibility using timestamp-based MVCC and handle concurrent
	 * modifications.  Same pattern as UPDATE/DELETE: distinguish truly
	 * invisible tuples from concurrent modifications.
	 */
	if (!FluxTupleVisibleToSnapshotDual(tuple_hdr, snapshot,
										RelationGetRelid(relation),
										buf))
	{
		TransactionId dirty_xid;
		bool		is_insert_entry;

		/*
		 * Writer-only probe: this branch waits on an in-progress
		 * INSERT/UPDATE/DELETE writer.  Lock-only markers (LOCK_SHARE/
		 * LOCK_EXCL) are handled by the dedicated lock-conflict check below
		 * (SLogTupleHasLockConflict), which serializes lockers via the
		 * heavyweight LOCKTAG_TUPLE lock.  Waiting-as-reader on a pure
		 * locker's xid here lets two lockers each XactLockTableWait on the
		 * other and deadlock.
		 *
		 * The writer's xid is read from the on-page header (t_xmin inserter /
		 * in-place updater, t_xmax deleter), as the UPDATE and DELETE paths
		 * do.  The header stays authoritative through the writer's PRE_COMMIT
		 * window (sLog marker and UNCOMMITTED hint cleared before the CLOG
		 * write); the sLog probe goes blind there and would report the
		 * still-in-flight writer as "no writer".
		 */
		dirty_xid = flux_header_dirty_xid(tuple_hdr, &is_insert_entry);

		if (TransactionIdIsValid(dirty_xid) && is_insert_entry)
		{
			/* Another txn's in-progress INSERT - truly invisible */
			if (tmfd)
			{
				tmfd->ctid = *tid;
				tmfd->xmax = dirty_xid;
				tmfd->cmax = InvalidCommandId;
			}
			result = TM_Invisible;
			goto out_unlock;
		}

		/*
		 * KEY-INTACT FAST PATH -- heap's require_sleep = false for
		 * LockTupleKeyShare when HEAP_KEYS_UPDATED is clear (heapam.c, "If
		 * we're requesting KeyShare, and there's no update present, we don't
		 * need to wait.  Even if there is an update, we can still continue if
		 * the key hasn't been modified.").
		 *
		 * Without this, an FK check's parent-row probe (ri_LockPKTuple ->
		 * table_tuple_lock with LockTupleKeyShare) waited on ANY in-progress
		 * in-place updater, because the branch below reads the writer's xid
		 * from the on-page header and waits unconditionally, never looking at
		 * the requested LockTupleMode.  Two concurrent FK inserts referencing
		 * the same parent row then each waited on the other's xid with no
		 * heavyweight lock cycle for the deadlock detector to break, so the
		 * backend hung forever (isolation test fk-deadlock).  Heap treats
		 * KeyShare vs a non-key UPDATE as COMPATIBLE and never waits.
		 *
		 * LOCK ORDERING (this is what makes the skip safe):
		 *
		 * Both inputs to the decision -- FLUX_TUPLE_KEYS_UPDATED and the
		 * writer's xid -- are read from *tuple_hdr while this function holds
		 * the tuple's buffer content lock BUFFER_LOCK_EXCLUSIVE, taken at the
		 * 'reacquire' label above and never released anywhere between that
		 * LockBuffer() and this point.  Every path that stamps an in-place
		 * image holds a conflicting buffer content lock while doing so (the
		 * regular update path takes BUFFER_LOCK_EXCLUSIVE, the CAS fast path
		 * BUFFER_LOCK_SHARE_EXCLUSIVE), so no concurrent key-changing update
		 * can set the bit between our read of the flag and our read of the
		 * xid -- we observe one self-consistent header.  Any path that does
		 * release and re-take the buffer lock jumps back to 'reacquire',
		 * which re-reads the header from scratch, so a decision is never
		 * carried across a lock gap.  This mirrors heap, which re-checks the
		 * tuple after re-grabbing the buffer lock and only then clears
		 * require_sleep.
		 *
		 * We additionally require that the pre-update image actually be
		 * reconstructible from the UNDO log BEFORE committing to the skip.
		 * The on-page bytes here are the updater's uncommitted new image
		 * (this whole branch is the "not visible to our snapshot" case), so
		 * returning them in the caller's slot would leak uncommitted data.
		 * If the visible before-image cannot be produced we do NOT skip and
		 * fall through to the ordinary wait, which is always correct.
		 */
		if (TransactionIdIsValid(dirty_xid) && !is_insert_entry &&
			mode == LockTupleKeyShare &&
			!(tuple_hdr->t_flags & FLUX_TUPLE_KEYS_UPDATED) &&
			snapshot != NULL && IsMVCCSnapshot(snapshot))
		{
			char	   *bi_data = NULL;
			int			bi_len = 0;

			if (FluxReconstructVisibleVersion(relation, tid,
											  (const char *) tuple_hdr,
											  ItemIdGetLength(itemid),
											  snapshot,
											  &bi_data, &bi_len))
			{
				/*
				 * Key is intact and we hold the committed pre-update image:
				 * grant the KeyShare lock without waiting, and serve that
				 * image (never the updater's uncommitted bytes) to the
				 * caller.  bi_data is palloc'd in the caller's context, so it
				 * outlives the buffer we are about to release.
				 */
				keyintact_image = bi_data;

				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = InvalidTransactionId;
					tmfd->cmax = InvalidCommandId;
					tmfd->traversed = false;
				}
				goto lock_tuple;
			}
		}

		if (TransactionIdIsValid(dirty_xid) && !is_insert_entry)
		{
			/* Another txn's in-progress UPDATE/DELETE - wait and retry */
			if (wait_policy == LockWaitBlock)
			{
				TransactionId wait_xid = dirty_xid;

				UnlockReleaseBuffer(buf);
				if (!have_tuple_lock)
				{
					FluxLockTuple(relation, tid, mode, true,
								  &have_tuple_lock);
				}
				XactLockTableWait(wait_xid, relation, tid, XLTW_Lock);
				goto reacquire;
			}
			else if (wait_policy == LockWaitError)
			{
				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = dirty_xid;
					tmfd->cmax = InvalidCommandId;
				}
				result = TM_WouldBlock;
				goto out_unlock;
			}
			else				/* LockWaitSkip */
			{
				result = TM_WouldBlock;
				goto out_unlock;
			}
		}

		/*
		 * No in-progress writer.  The on-page inserter/updater (t_xmin) is
		 * therefore either committed, aborted, or our own transaction.
		 * Resolve it the way HeapTupleSatisfiesUpdate does -- against CLOG and
		 * the current transaction, NOT against the caller's snapshot.  A row
		 * whose inserter or in-place updater committed after this statement's
		 * snapshot was taken is invisible to the snapshot yet perfectly
		 * lockable; FLUX keeps the newest version at the same TID, so the
		 * on-page tuple is already "the latest version" heap would chase via
		 * t_ctid.  Reporting TM_Updated here instead sent ON CONFLICT DO
		 * UPDATE (ExecOnConflictLockRow, flags == 0) back to its arbiter scan,
		 * which found the same committed row, re-locked it under the same
		 * snapshot, got TM_Updated again, and so on forever.
		 *
		 * An in-progress or aborted inserter is TM_Invisible, as is our own
		 * inserter when the MVCC check rejected it (later command in this
		 * transaction, or a rolled-back savepoint).
		 */
		{
			TransactionId xmin = FluxTupleGetXmin(tuple_hdr);

			if (TransactionIdIsCurrentTransactionId(xmin) ||
				!TransactionIdDidCommit(xmin))
			{
				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = xmin;
					tmfd->cmax = InvalidCommandId;
				}
				result = TM_Invisible;
				goto out_unlock;
			}
		}

		if (tmfd)
		{
			tmfd->ctid = *tid;
			tmfd->xmax = InvalidTransactionId;
			tmfd->cmax = InvalidCommandId;
			tmfd->traversed = (flags & TUPLE_LOCK_FLAG_FIND_LAST_VERSION) != 0;
		}
		/* Fall through to lock the current (latest) version */
	}

lock_tuple:

	/*
	 * Check for lock conflicts using the sLog.  The sLog tracks all
	 * in-progress lock/delete/update operations, replacing the old
	 * t_xmax/MultiXact-based scheme.
	 *
	 * Pass the precise LockTupleMode so the real heavyweight matrix decides:
	 * a KeyShare FK probe must stay compatible with an in-progress
	 * NoKeyExclusive UPDATE, as it is in heap.
	 */
	{
		TransactionId xwait = InvalidTransactionId;

		if (SLogTupleGetLockConflictXid(RelationGetRelid(relation), tid,
										current_xid, mode,
										&xwait))
		{
			/* There's a conflict - check wait policy */
			if (wait_policy == LockWaitError)
			{
				if (tmfd)
				{
					tmfd->ctid = *tid;
					tmfd->xmax = InvalidTransactionId;
					tmfd->cmax = InvalidCommandId;
				}
				result = TM_WouldBlock;
				goto out_unlock;
			}
			else if (wait_policy == LockWaitSkip)
			{
				result = TM_WouldBlock;
				goto out_unlock;
			}
			else				/* LockWaitBlock */
			{
				/*
				 * Wait on the specific conflicting transaction identified by
				 * SLogTupleGetLockConflictXid -- never on an arbitrary
				 * in-progress peer, which could be a compatible locker queued
				 * behind us and would form a spurious mutual-wait cycle.
				 */

				/* Release buffer and wait */
				UnlockReleaseBuffer(buf);

				if (TransactionIdIsValid(xwait))
				{
					/* Acquire tuple-level lock to wait */
					if (!have_tuple_lock)
					{
						FluxLockTuple(relation, tid, mode, true,
									  &have_tuple_lock);
					}

					/* Wait for the conflicting transaction */
					XactLockTableWait(xwait, relation, tid, XLTW_Lock);
				}

				/* Re-acquire buffer and retry */
				goto reacquire;
			}
		}
	}

	/*
	 * Lock succeeded.
	 *
	 * tmfd->traversed means "an update chain was actually followed to reach a
	 * DIFFERENT tuple than the caller asked for" (see TM_FailureData in
	 * tableam.h; heapam_tuple_lock sets it only after *tid is replaced by
	 * tmfd->ctid).  FLUX updates in place with stable TIDs, so the tuple we
	 * locked is always the very one the caller named and no chain is ever
	 * walked: traversed is therefore always false.
	 *
	 * Reporting true for every FIND_LAST_VERSION caller told GetTupleForTrigger
	 * that a concurrent update had moved the row, so it re-ran EvalPlanQual and
	 * retried; with a BEFORE trigger that modifies its own target row, MERGE
	 * looped in ExecMergeMatched until the backend exhausted memory.
	 */
	if (tmfd)
	{
		tmfd->traversed = false;
		tmfd->ctid = *tid;
		tmfd->xmax = InvalidTransactionId;
		tmfd->cmax = InvalidCommandId;
	}

	tuple_hdr->t_flags |= FLUX_TUPLE_LOCKED;

	/*
	 * LOST-UPDATE FIX: register the sLog LOCK marker while STILL HOLDING the
	 * buffer EXCLUSIVE lock, atomically with setting FLUX_TUPLE_LOCKED.  The
	 * marker (not the ownerless page flag) is the authoritative lock owner
	 * that concurrent writers probe via SLogTupleGetWriteConflictXid.
	 *
	 * The previous code registered the marker only AFTER dropping the buffer
	 * lock (and only when slot != NULL), so two lockers racing in the
	 * probe->register gap both found no conflicting marker, both returned
	 * TM_Ok on the same tuple, and both proceeded to update in place -- a lost
	 * update.  Registering here closes the gap: because every writer's
	 * conflict probe runs under this same buffer lock, a locker is either
	 * fully visible (flag set + marker present) or not present at all to any
	 * concurrent writer.  This mirrors heap, which stamps xmax under the
	 * buffer content lock atomically with the lock.
	 *
	 * Must run BEFORE START_CRIT_SECTION: SLogTupleInsert may palloc, take the
	 * sLog partition LWLock, and (overflow path) ProcArrayLock -- all forbidden
	 * inside a critical section.  Taking those leaf locks under the buffer
	 * content lock is the standard, deadlock-free order (buffer content lock ->
	 * ProcArrayLock, exactly as heap's TransactionIdIsInProgress/CLOG probes).
	 */
	if (result == TM_Ok)
	{
		SLogOpType	lock_op;

		lock_op = (mode == LockTupleKeyShare || mode == LockTupleShare)
			? SLOG_OP_LOCK_SHARE : SLOG_OP_LOCK_EXCL;

		/*
		 * Record the precise LockTupleMode alongside the coarse lock_op so a
		 * concurrent updater can apply the real heavyweight conflict matrix:
		 * FOR KEY SHARE (AccessShareLock) stays compatible with a NoKeyExclusive
		 * UPDATE, while FOR SHARE/FOR UPDATE correctly block it.  See
		 * SLogTupleGetWriteConflictXid.
		 */
		FluxEnsureSLogCallbacks();
		SLogTupleInsert(RelationGetRelid(relation), tid,
						current_xid, lock_op,
						GetCurrentSubTransactionId(), cid, 0, 0, mode);

		/* Mark this block dirty for the scan-path sLog bypass */
		FluxDirtyMapMark(RelationGetRelid(relation),
						 ItemPointerGetBlockNumber(tid));
	}

	START_CRIT_SECTION();

	MarkBufferDirty(buf);

	/* Log the lock operation */
	if (RelationNeedsWAL(relation))
	{
		XLogRecPtr	recptr;
		xl_flux_lock xlrec;

		xlrec.offnum = ItemPointerGetOffsetNumber(tid);
		xlrec.flags = 0;
		xlrec.infomask = tuple_hdr->t_infomask;
		xlrec.lock_mode = (uint8) mode;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, sizeof(xl_flux_lock));
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);

		recptr = XLogInsert(RM_FLUX_ID, XLOG_FLUX_LOCK);
		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	/*
	 * Populate the slot with the locked tuple's data.  This must happen after
	 * END_CRIT_SECTION (since overflow fetch may do I/O and ereport). We must
	 * unlock the buffer (but keep the pin) before calling
	 * FluxTupleToSlotWithOverflow because it may fetch overflow data, and if
	 * that overflow is on the same page, it would try to lock an
	 * already-locked buffer causing an assertion failure.
	 *
	 * FK constraint triggers and other callers of table_tuple_lock() expect
	 * the slot to contain valid tuple data.
	 */
	if (slot && result == TM_Ok)
	{
		/*
		 * Unlock buffer but keep pin for slot materialization.  The sLog LOCK
		 * marker was already registered under the buffer lock above (LOST-UPDATE
		 * FIX), so nothing to register here.
		 */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		/*
		 * Key-intact fast path: the on-page bytes belong to a still
		 * in-progress updater and are invisible to our snapshot, so convert
		 * the committed pre-update image reconstructed above instead.  The
		 * caller's slot is an arbitrary ops (ri_LockPKTuple passes a plain
		 * heap/virtual slot), so it must go through the same conversion +
		 * ExecMaterializeSlot path as the on-page case below -- which also
		 * deep-copies it, leaving keyintact_image ours to free.
		 */
		if (!FluxTupleToSlotWithOverflow(keyintact_image != NULL ?
										 (FluxTupleHeader *) keyintact_image :
										 tuple_hdr, slot, relation))
		{
			/*
			 * Conversion failed (e.g., tuple was concurrently deleted).
			 * Return TM_Deleted rather than leaving the slot empty.
			 */
			ReleaseBuffer(buf);
			if (keyintact_image != NULL)
				pfree(keyintact_image);
			if (have_tuple_lock)
				FluxUnlockTuple(relation, tid, mode);
			return TM_Deleted;
		}
		slot->tts_tid = *tid;
		slot->tts_tableOid = RelationGetRelid(relation);

		/*
		 * FluxTupleToSlotWithOverflow leaves the slot virtual with
		 * tts_values[] pointing into the page buffer (and records no pin in
		 * the slot).  Deep-copy those pass-by-reference datums into
		 * slot-owned memory BEFORE dropping the pin, so a returned varlena
		 * (e.g. numeric via UPDATE ... RETURNING / FK / EPQ locking) does not
		 * alias a buffer that is about to be unpinned and reused.  This
		 * matches heap's slot discipline (ExecStorePinnedBufferHeapTuple
		 * keeps the pin in the slot).
		 */
		ExecMaterializeSlot(slot);

		/* Release the buffer pin now that slot is materialized */
		ReleaseBuffer(buf);

		if (keyintact_image != NULL)
			pfree(keyintact_image);

		/* Release tuple-level lock if we acquired it */
		if (have_tuple_lock)
			FluxUnlockTuple(relation, tid, mode);

		return result;
	}

out_unlock:

	/*
	 * TM_Invisible still has to leave the tuple in the caller's slot.
	 *
	 * heap does: heap_lock_tuple returns TM_Invisible for the ON CONFLICT DO
	 * UPDATE/DO SELECT case (heapam.c: "We return this value here rather than
	 * throwing an error in order to give that case the opportunity to throw a
	 * more specific error"), and heapam_tuple_lock then falls through to
	 * ExecStorePinnedBufferHeapTuple, so the slot is populated on that path
	 * too.  The executor relies on it: ExecOnConflictUpdate's TM_Invisible arm
	 * reads xmin off this very slot via slot_getsysattr() to recognise a
	 * self-conflict and raise "ON CONFLICT DO UPDATE command cannot affect row
	 * a second time" (nodeModifyTable.c).
	 *
	 * FLUX returned TM_Invisible with the slot left EMPTY, so that
	 * slot_getsysattr() hit the no-tuple path and raised "cannot retrieve a
	 * system column in this context" instead -- the wrong error, and a
	 * user-visible difference on plain INSERT ... ON CONFLICT (regression tests
	 * insert_conflict, update).
	 *
	 * Fill the slot here, at the single shared exit, so every TM_Invisible
	 * return is covered rather than just the two current ones.  The store is
	 * done while the buffer is still pinned and then materialized, matching the
	 * discipline of the success path below.
	 */
	/*
	 * The ItemPointerIsValid() test is not redundant: the FK fast-path batch
	 * flush (ri_LockPKTuple -> ri_FastPathFlushArray) can reach a locker with
	 * slot->tts_tid already invalidated, and ItemPointerGetOffsetNumber()
	 * asserts on an invalid pointer under cassert.  See the KNOWN-OPEN note on
	 * fk-crosstype-recheck in flux_default_triage.md.
	 */
	if (result == TM_Invisible && slot != NULL && BufferIsValid(buf) &&
		tid != NULL && ItemPointerIsValid(tid))
	{
		Page		ipage = BufferGetPage(buf);
		OffsetNumber ioff = ItemPointerGetOffsetNumber(tid);

		if (ioff >= FirstOffsetNumber && ioff <= PageGetMaxOffsetNumber(ipage))
		{
			ItemId		iid = PageGetItemId(ipage, ioff);

			if (ItemIdIsNormal(iid))
			{
				FluxTupleHeader *ihdr =
					(FluxTupleHeader *) PageGetItem(ipage, iid);

				/*
				 * Use the plain store (not FluxTupleToSlotWithOverflow): the
				 * caller only needs the system columns here, and the DELETED
				 * reject inside the overflow-aware path would refuse a
				 * delete-marked tuple outright.
				 */
				FluxSlotStoreTuple(slot, ihdr, ItemIdGetLength(iid), buf);
				slot->tts_tableOid = RelationGetRelid(relation);
				slot->tts_tid = *tid;
				ExecMaterializeSlot(slot);
			}
		}
	}

	UnlockReleaseBuffer(buf);

	if (keyintact_image != NULL)
		pfree(keyintact_image);

	/* Release tuple-level lock if we acquired it */
	if (have_tuple_lock)
		FluxUnlockTuple(relation, tid, mode);

	return result;
}

/*
 * Nontransactional truncate for FLUX relation
 *
 * This is called for TRUNCATE operations. We use RelationTruncate which
 * properly handles all forks (main, FSM, VM), WAL logging, shared buffer
 * invalidation, and cache coherency.
 */
static void
flux_relation_nontransactional_truncate(Relation rel)
{
	RelationTruncate(rel, 0);
}

/*
 * Set new filelocator for FLUX relation
 */
static void
flux_relation_set_new_filelocator(Relation rel,
								  const RelFileLocator *newrlocator,
								  char persistence,
								  TransactionId *freezeXid,
								  MultiXactId *minmulti)
{
	SMgrRelation srel;

	/* Set freeze XID to current transaction minimum */
	*freezeXid = RecentXmin;

	/* Set minimum multixact ID */
	*minmulti = GetOldestMultiXactId();

	/* Create the storage file (empty, no blocks yet) */
	srel = RelationCreateStorage(*newrlocator, persistence, true);

	/* WAL-log the file creation */
	if (persistence == RELPERSISTENCE_PERMANENT)
		log_smgrcreate(newrlocator, MAIN_FORKNUM);

	/*
	 * Note: We do not initialize block 0 here. Block 0 will be created
	 * on-demand during the first scan or insert operation via
	 * FluxGetPageWithFreeSpace() or the scan's RBM_ZERO_AND_LOCK logic.
	 */

	/* Set up init fork for unlogged tables if needed */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
	}

	smgrclose(srel);

	/*
	 * FLUX does not maintain a UNDO log.  Its DML UNDO goes to
	 * the per-backend UNDO engine (access/undo/perbackend), which owns its
	 * own log segments and discard worker, so there is nothing to create here.
	 */
}

/*
 * Check whether table tuples referenced by index entries are dead.
 *
 * This is called by index AMs during index tuple deletion (both simple
 * deletion during VACUUM and bottom-up deletion during retail inserts).
 * The index AM passes a list of TIDs and we check each one's liveness.
 * We set knowndeletable=true for entries whose table tuples are dead,
 * allowing the index AM to remove its entries.
 *
 * IMPORTANT: This function must NEVER modify table data.  It only reads
 * tuple headers to check visibility status.
 *
 * Modeled on heap_index_delete_tuples() but simplified for FLUX's
 * timestamp-based MVCC.
 */
static TransactionId
flux_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	TransactionId snapshotConflictHorizon = InvalidTransactionId;
	BlockNumber blkno = InvalidBlockNumber;
	Buffer		buf = InvalidBuffer;
	Page		page = NULL;
	OffsetNumber maxoff = InvalidOffsetNumber;
	int			finalndeltids = 0;

	Assert(delstate->ndeltids > 0);

	/* Iterate over deltids, determine which are deletable */
	for (int i = 0; i < delstate->ndeltids; i++)
	{
		TM_IndexDelete *ideltid = &delstate->deltids[i];
		TM_IndexStatus *istatus = delstate->status + ideltid->id;
		ItemPointer htid = &ideltid->tid;
		OffsetNumber offnum;

		/*
		 * Read buffer for this block if we haven't already.  Avoid refetching
		 * if it's the same block as the previous entry.
		 */
		if (blkno == InvalidBlockNumber ||
			ItemPointerGetBlockNumber(htid) != blkno)
		{
			if (BufferIsValid(buf))
				UnlockReleaseBuffer(buf);

			blkno = ItemPointerGetBlockNumber(htid);
			buf = ReadBuffer(rel, blkno);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			maxoff = PageGetMaxOffsetNumber(page);
		}

		offnum = ItemPointerGetOffsetNumber(htid);

		/* Sanity check: offset must be valid */
		if (offnum < FirstOffsetNumber || offnum > maxoff)
		{
			/*
			 * Index entry points to invalid offset.  Mark as deletable to
			 * clean up the corruption.
			 */
			istatus->knowndeletable = true;
			finalndeltids = i + 1;
			continue;
		}

		/* Already known to be deletable by the index AM? */
		if (istatus->knowndeletable)
		{
			Assert(!delstate->bottomup && !istatus->promising);
			finalndeltids = i + 1;
			continue;
		}

		{
			ItemId		lp = PageGetItemId(page, offnum);

			if (!ItemIdIsNormal(lp))
			{
				/*
				 * LP_DEAD, LP_UNUSED, or LP_REDIRECT: the tuple is gone. The
				 * index entry can be removed.
				 */
				istatus->knowndeletable = true;
			}
			else
			{
				FluxTupleHeader *tuple_hdr;

				tuple_hdr = (FluxTupleHeader *) PageGetItem(page, lp);

				/*
				 * For FLUX, a tuple is vacuumable (and its index entry
				 * deletable) if it is deleted AND old enough that no snapshot
				 * can see it.
				 */
				if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
				{
					if (FluxTupleDeadToAll(tuple_hdr, FluxGetOldestXminHorizon(rel)))
					{
						istatus->knowndeletable = true;
					}
					else
					{
						/* Recently dead -- cannot delete index entry yet */
						continue;
					}
				}
				else
				{
					/* Live tuple -- cannot delete index entry */
					continue;
				}
			}
		}

		/* Track progress for bottom-up deletion */
		if (delstate->bottomup && istatus->knowndeletable)
		{
			int			actualfreespace = 0;

			actualfreespace += istatus->freespace;
			if (actualfreespace >= delstate->bottomupfreespace)
			{
				/* Met the space target -- stop early */
				finalndeltids = i + 1;
				break;
			}
		}

		finalndeltids = i + 1;
	}

	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	/*
	 * Shrink deltids array to exclude non-deletable entries at the end.
	 */
	Assert(finalndeltids > 0 || delstate->bottomup);
	delstate->ndeltids = finalndeltids;

	return snapshotConflictHorizon;
}

/*
 * Copy data for FLUX relation (used by ALTER TABLE SET ACCESS METHOD, etc.)
 *
 * This performs a block-level copy of all storage forks from the old
 * relation files to new ones. Since we copy directly without examining
 * shared buffers, we must flush any dirty pages first. The old physical
 * files are scheduled for deletion.
 */
static void
flux_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	SMgrRelation dstrel;

	/*
	 * Since we copy the file directly without looking at the shared buffers,
	 * we'd better first flush out any pages of the source relation that are
	 * in shared buffers. We assume no new changes will be made while we are
	 * holding exclusive lock on the rel.
	 */
	FlushRelationBuffers(rel);

	/*
	 * Create and copy all forks of the relation, and schedule unlinking of
	 * old physical files.
	 *
	 * NOTE: any conflict in relfilenumber value will be caught in
	 * RelationCreateStorage().
	 */
	dstrel = RelationCreateStorage(*newrlocator, rel->rd_rel->relpersistence,
								   true);

	/* Copy main fork */
	RelationCopyStorage(RelationGetSmgr(rel), dstrel, MAIN_FORKNUM,
						rel->rd_rel->relpersistence);

	/* Copy any extra forks that exist (FSM, etc.) */
	for (ForkNumber forkNum = MAIN_FORKNUM + 1;
		 forkNum <= MAX_FORKNUM; forkNum++)
	{
		if (smgrexists(RelationGetSmgr(rel), forkNum))
		{
			smgrcreate(dstrel, forkNum, false);

			/*
			 * WAL log creation if the relation is persistent, or this is the
			 * init fork of an unlogged relation.
			 */
			if (RelationIsPermanent(rel) ||
				(rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED &&
				 forkNum == INIT_FORKNUM))
				log_smgrcreate(newrlocator, forkNum);
			RelationCopyStorage(RelationGetSmgr(rel), dstrel, forkNum,
								rel->rd_rel->relpersistence);
		}
	}

	/* Drop old relation storage, and close new one */
	RelationDropStorage(rel);
	smgrclose(dstrel);
}

/*
 * Copy data for cluster operation
 *
 * This is called by CLUSTER and VACUUM FULL to copy tuples from the old
 * table to the new one, optionally reordering by an index. We scan the
 * old table using SnapshotAny and perform our own MVCC visibility checks
 * to decide which tuples to keep, which to discard as dead, and which
 * are recently dead.
 *
 * For FLUX, visibility is determined by the tuple's timestamp-based MVCC
 * flags (deleted flag, commit timestamp, etc.) rather than heap-style xmin/xmax.
 */
static void
flux_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
							   Relation OldIndex, bool use_sort,
							   TransactionId OldestXmin,
							   Snapshot snapshot,
							   TransactionId *xid_cutoff,
							   MultiXactId *multi_cutoff,
							   double *num_tuples,
							   double *tups_vacuumed,
							   double *tups_recently_dead)
{
	TableScanDesc tableScan;
	IndexScanDesc indexScan;
	TupleTableSlot *slot;
	CommandId	mycid = GetCurrentCommandId(true);
	double		live_tuples = 0;
	double		dead_tuples = 0;
	double		recent_dead = 0;

	/* Initialize return values */
	*xid_cutoff = InvalidTransactionId;
	*multi_cutoff = InvalidMultiXactId;
	*num_tuples = 0;
	*tups_vacuumed = 0;
	*tups_recently_dead = 0;

	/*
	 * Valid smgr_targblock implies something already wrote to the relation.
	 * This may be harmless, but this function hasn't planned for it.
	 */
	Assert(RelationGetTargetBlock(NewTable) == InvalidBlockNumber);

	/*
	 * Set up the scan. If we have an index and are not doing a sort, use an
	 * index scan to get tuples in index order. Otherwise do a sequential scan
	 * (and optionally sort afterward).
	 */
	if (OldIndex != NULL && !use_sort)
	{
		pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
									 PROGRESS_REPACK_PHASE_INDEX_SCAN_HEAP);

		tableScan = NULL;
		indexScan = index_beginscan(OldTable, OldIndex, false, SnapshotAny,
									NULL, 0, 0, SO_NONE);
		index_rescan(indexScan, NULL, 0, NULL, 0);
	}
	else
	{
		pgstat_progress_update_param(PROGRESS_REPACK_PHASE,
									 PROGRESS_REPACK_PHASE_SEQ_SCAN_HEAP);

		tableScan = table_beginscan(OldTable, SnapshotAny, 0, NULL, 0);
		indexScan = NULL;
	}

	slot = table_slot_create(OldTable, NULL);

	/*
	 * Scan through the old table. For each tuple, check visibility using
	 * FLUX's timestamp-based MVCC and either copy it to the new table or skip
	 * it.
	 */
	for (;;)
	{
		bool		isdead = false;
		bool		is_tombstone = false;
		uint64		delete_ts = 0;

		CHECK_FOR_INTERRUPTS();

		if (indexScan != NULL)
		{
			if (!table_index_getnext_slot(indexScan, ForwardScanDirection,
									  slot))
				break;
		}
		else
		{
			if (!table_scan_getnextslot(tableScan, ForwardScanDirection, slot))
				break;
		}

		/*
		 * For FLUX, check tuple visibility using our page-level access. Read
		 * the tuple header from the page to check MVCC flags.
		 */
		{
			Buffer		buf;
			Page		page;
			ItemId		itemid;
			FluxTupleHeader *tuple_hdr;
			BlockNumber blkno = ItemPointerGetBlockNumber(&slot->tts_tid);
			OffsetNumber offnum = ItemPointerGetOffsetNumber(&slot->tts_tid);

			buf = ReadBuffer(OldTable, blkno);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			itemid = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(itemid))
			{
				/* Item pointer is dead or unused -- skip */
				UnlockReleaseBuffer(buf);
				dead_tuples++;
				continue;
			}

			tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

			if (tuple_hdr->t_flags & FLUX_TUPLE_UPDATED)
			{
				/*
				 * Distinguish cross-page (out-of-place) updates from in-place
				 * updates.  Cross-page updates have t_ctid pointing to a
				 * different TID (the new version's location). In-place
				 * updates have t_ctid pointing to self (same TID).
				 *
				 * Only cross-page old versions are dead; in-place updated
				 * tuples contain the current data and are live.
				 */
				ItemPointerData self_tid;

				ItemPointerSet(&self_tid, blkno, offnum);
				if (!ItemPointerEquals(&tuple_hdr->t_ctid, &self_tid))
					isdead = true;	/* Cross-page: old version is dead */
				/* else: in-place update, tuple is live — fall through */
			}

			if (!isdead && (tuple_hdr->t_flags & FLUX_TUPLE_DELETED))
			{
				/*
				 * Tuple has been deleted. Check whether it's old enough to be
				 * truly dead vs recently dead (still needed for MVCC
				 * snapshots).  XID-horizon gate (FluxTupleDeadToAll), not the
				 * removed HLC timestamp horizon: the on-page word now packs
				 * the XID-based t_xmax, not a wall-clock commit timestamp.
				 */
				if (FluxTupleDeadToAll(tuple_hdr, FluxGetOldestXminHorizon(OldTable)))
				{
					/* Definitely dead -- can discard */
					isdead = true;
				}
				else
				{
					/*
					 * Recently dead -- still needed by some snapshots.  Copy
					 * it to the new relation as a tombstone, preserving its
					 * original delete timestamp so old-snapshot readers still
					 * see the pre-delete version (matching heap's
					 * RECENTLY_DEAD retention during cluster rewrite).
					 */
					recent_dead++;
					isdead = false;
					is_tombstone = true;
					delete_ts = tuple_hdr->t_commit_ts;
				}
			}

			UnlockReleaseBuffer(buf);
		}

		if (isdead)
		{
			dead_tuples++;
			continue;
		}

		/* Live or recently-dead tuple -- copy to new table */
		table_tuple_insert(NewTable, slot, mycid, 0, NULL);

		if (is_tombstone)
		{
			/*
			 * The row was copied as a fresh, live INSERT.  Rewrite it in the
			 * new relation as a deleted tombstone carrying its original
			 * delete marker, and drop the INSERT's local sLog tracking so
			 * that commit-time bookkeeping does not clobber the tuple with
			 * this rewrite transaction's commit state (which would resurrect
			 * the row for readers whose snapshots predate the rewrite
			 * commit).
			 */
			Buffer		nbuf;
			Page		npage;
			ItemId		nitemid;
			FluxTupleHeader *ntuple_hdr;
			FluxPageOpaque nphdr;
			BlockNumber nblkno = ItemPointerGetBlockNumber(&slot->tts_tid);
			OffsetNumber noffnum = ItemPointerGetOffsetNumber(&slot->tts_tid);

			SLogTupleUntrackLocalOnly(RelationGetRelid(NewTable),
									  &slot->tts_tid);

			nbuf = ReadBuffer(NewTable, nblkno);
			LockBuffer(nbuf, BUFFER_LOCK_EXCLUSIVE);
			npage = BufferGetPage(nbuf);
			nitemid = PageGetItemId(npage, noffnum);

			START_CRIT_SECTION();

			ntuple_hdr = (FluxTupleHeader *) PageGetItem(npage, nitemid);
			ntuple_hdr->t_flags |= FLUX_TUPLE_DELETED;
			ntuple_hdr->t_flags &= ~FLUX_TUPLE_UNCOMMITTED;
			ntuple_hdr->t_commit_ts = delete_ts;

			nphdr = FluxPageGetOpaque(npage);
			FluxPageSetCommitTs(nphdr,
								Max(FluxPageGetCommitTs(nphdr), delete_ts));
			FluxPageSetFlag(nphdr, FLUX_PAGE_DEFRAG_NEEDED);

			MarkBufferDirty(nbuf);

			if (RelationNeedsWAL(NewTable))
			{
				XLogRecPtr	recptr;
				xl_flux_delete xlrec;

				xlrec.offnum = noffnum;
				xlrec.flags = 0;
				xlrec.tuple_len = ItemIdGetLength(nitemid);
				xlrec.commit_ts = delete_ts;

				XLogBeginInsert();
				XLogRegisterData((char *) &xlrec, sizeof(xl_flux_delete));
				XLogRegisterBuffer(0, nbuf, REGBUF_STANDARD);

				recptr = XLogInsert(RM_FLUX_ID, XLOG_FLUX_DELETE);
				PageSetLSN(npage, recptr);
			}

			END_CRIT_SECTION();

			UnlockReleaseBuffer(nbuf);
		}

		live_tuples++;

		pgstat_progress_update_param(PROGRESS_REPACK_HEAP_TUPLES_SCANNED,
									 live_tuples + dead_tuples + recent_dead);
	}

	/* Clean up scan resources */
	if (indexScan != NULL)
		index_endscan(indexScan);
	if (tableScan != NULL)
		table_endscan(tableScan);

	ExecDropSingleTupleTableSlot(slot);

	/* Return statistics to caller */
	*num_tuples = live_tuples;
	*tups_vacuumed = dead_tuples;
	*tups_recently_dead = recent_dead;
}

/*
 * Build range scan for index creation
 *
 * Scans the FLUX table and feeds tuples to the index AM's callback for
 * index building.  Handles partial indexes, expression indexes, uniqueness
 * checking, concurrent index builds, and proper visibility classification.
 *
 * Modeled on heapam_index_build_range_scan().
 */
static double
flux_index_build_range_scan(Relation tablerel, Relation indexrel,
							IndexInfo *indexInfo, bool allow_sync,
							bool anyvisible, bool progress,
							BlockNumber start_blockno, BlockNumber numblocks,
							IndexBuildCallback callback, void *callback_state,
							TableScanDesc scan)
{
	double		reltuples = 0;
	bool		checking_uniqueness PG_USED_FOR_ASSERTS_ONLY;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ExprState  *predicate;
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	Snapshot	snapshot;
	bool		need_unregister_snapshot = false;

	/* See whether we're verifying uniqueness/exclusion properties */
	checking_uniqueness = (indexInfo->ii_Unique ||
						   indexInfo->ii_ExclusionOps != NULL);

	/* "Any visible" mode is not compatible with uniqueness checks */
	Assert(!(anyvisible && checking_uniqueness));

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.  Also a slot to hold the current tuple.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(tablerel, NULL);

	/* Arrange for econtext's scan tuple to be the tuple under test */
	econtext->ecxt_scantuple = slot;

	/* Set up execution state for predicate, if any */
	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Prepare for scan.  Normal index build uses SnapshotAny (we do our own
	 * visibility checks).  Concurrent/bootstrap uses an MVCC snapshot.
	 */
	if (!scan)
	{
		if (IsBootstrapProcessingMode() || indexInfo->ii_Concurrent)
		{
			snapshot = RegisterSnapshot(GetTransactionSnapshot());
			need_unregister_snapshot = true;
		}
		else
			snapshot = SnapshotAny;

		scan = table_beginscan_strat(tablerel, snapshot, 0, NULL,
									 true, allow_sync);
	}
	else
	{
		snapshot = scan->rs_snapshot;
	}

	/* Scan all tuples in the base relation */
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bool		tupleIsAlive;

		CHECK_FOR_INTERRUPTS();

		if (snapshot == SnapshotAny)
		{
			/*
			 * Classify the tuple using FLUX's timestamp-based MVCC by
			 * re-reading the tuple header from the page.
			 */
			Buffer		buf;
			Page		page;
			ItemId		itemid;
			FluxTupleHeader *tuple_hdr;
			BlockNumber blkno = ItemPointerGetBlockNumber(&slot->tts_tid);
			OffsetNumber offnum = ItemPointerGetOffsetNumber(&slot->tts_tid);
			bool		indexIt;

			buf = ReadBuffer(tablerel, blkno);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);

			itemid = PageGetItemId(page, offnum);
			if (!ItemIdIsNormal(itemid))
			{
				UnlockReleaseBuffer(buf);
				continue;
			}

			tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

			if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
			{
				if (FluxTupleDeadToAll(tuple_hdr, FluxGetOldestXminHorizon(tablerel)))
				{
					/* Definitely dead -- skip */
					UnlockReleaseBuffer(buf);
					continue;
				}
				else
				{
					/* Recently dead -- index for MVCC, don't count */
					indexIt = true;
					tupleIsAlive = false;
				}
			}
			else if (tuple_hdr->t_flags & FLUX_TUPLE_SPECULATIVE)
			{
				/* Speculative insertion not yet confirmed -- skip */
				UnlockReleaseBuffer(buf);
				continue;
			}
			else
			{
				/* Live tuple -- index and count it */
				indexIt = true;
				tupleIsAlive = true;
				reltuples += 1;
			}

			UnlockReleaseBuffer(buf);

			if (!indexIt)
				continue;
		}
		else
		{
			/* MVCC snapshot already filtered for visibility */
			tupleIsAlive = true;
			reltuples += 1;
		}

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		/* In a partial index, discard tuples that don't satisfy predicate */
		if (predicate != NULL)
		{
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * Extract all indexed attributes.  This also evaluates any index
		 * expressions.
		 */
		FormIndexDatum(indexInfo, slot, estate, values, isnull);

		/*
		 * Call the AM's callback with the tuple's own TID.  FLUX secondary
		 * indexes hold plain 6-byte heap-style TIDs (no RowID/gen suffix), so
		 * this matches the stock heap index-build contract exactly.
		 */
		callback(indexrel, &slot->tts_tid,
				 values, isnull, tupleIsAlive, callback_state);
	}

	table_endscan(scan);

	if (need_unregister_snapshot)
		UnregisterSnapshot(snapshot);

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	/* These may have been pointing to the now-gone estate */
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;

	return reltuples;
}

/*
 * Validate scan for index
 */
static void
flux_index_validate_scan(Relation tablerel, Relation indexrel,
						 IndexInfo *indexInfo, Snapshot snapshot,
						 ValidateIndexState *state)
{
	TableScanDesc scan;
	TupleTableSlot *slot;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	ExprState  *predicate;
	EState	   *estate;
	ExprContext *econtext;
	ItemPointer indexcursor = NULL;
	ItemPointerData decoded;
	bool		tuplesort_empty = false;

	/*
	 * Need an EState for evaluation of index expressions and partial-index
	 * predicates.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	slot = table_slot_create(tablerel, NULL);
	econtext->ecxt_scantuple = slot;

	predicate = ExecPrepareQual(indexInfo->ii_Predicate, estate);

	/*
	 * Scan the table and the sorted output from tuplesort in parallel. For
	 * each table tuple, check if there's a matching index entry. Tuples that
	 * satisfy the predicate but have no index entry need to be inserted into
	 * the index.
	 */
	scan = table_beginscan_strat(tablerel, snapshot, 0, NULL, true, false);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		CHECK_FOR_INTERRUPTS();

		state->htups += 1;

		/*
		 * Skip tuples that don't satisfy the partial index predicate.
		 */
		if (predicate != NULL)
		{
			MemoryContextReset(econtext->ecxt_per_tuple_memory);
			if (!ExecQual(predicate, econtext))
				continue;
		}

		/*
		 * Advance the tuplesort cursor past any entries that are for TIDs
		 * earlier than the current table tuple.
		 */
		while (!tuplesort_empty &&
			   (!indexcursor ||
				ItemPointerCompare(indexcursor, &slot->tts_tid) < 0))
		{
			Datum		ts_val;
			bool		ts_isnull;

			tuplesort_empty = !tuplesort_getdatum(state->tuplesort,
												  true, false,
												  &ts_val, &ts_isnull,
												  NULL);
			Assert(tuplesort_empty || !ts_isnull);
			if (!tuplesort_empty)
			{
				itemptr_decode(&decoded, DatumGetInt64(ts_val));
				indexcursor = &decoded;
			}
			else
			{
				indexcursor = NULL;
			}
		}

		/*
		 * If the sorted cursor TID matches the current table tuple, the index
		 * already has this entry.  Otherwise, we need to add it.
		 */
		if (indexcursor != NULL &&
			ItemPointerCompare(indexcursor, &slot->tts_tid) == 0)
		{
			/* Already in the index -- skip */
			continue;
		}

		MemoryContextReset(econtext->ecxt_per_tuple_memory);

		FormIndexDatum(indexInfo, slot, estate, values, isnull);

		/*
		 * Insert the missing index entry using the tuple's own TID.
		 */
		index_insert(indexrel, values, isnull, &slot->tts_tid,
					 tablerel, indexInfo->ii_Unique ?
					 UNIQUE_CHECK_YES : UNIQUE_CHECK_NO,
					 false, indexInfo);

		state->tups_inserted += 1;
	}

	/*
	 * Release any AM-cached insert state (e.g. BRIN's pinned revmap buffer in
	 * ii_AmCache) before we finish.  See the note in
	 * flux_inplace_index_maintenance.
	 */
	index_insert_cleanup(indexrel, indexInfo);

	table_endscan(scan);

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_PredicateState = NULL;
}

/*
 * Get relation size information
 *
 * Returns the on-disk size in bytes for the specified fork of the relation.
 * This is used by pg_relation_size(), VACUUM, CLUSTER, and many other
 * operations that need to know the physical storage footprint.
 */
/*
 * Use table_block_relation_size() from tableam.c directly.  FLUX uses
 * standard BLCKSZ-width forks just like heap, so the generic
 * implementation is correct and efficient (no smgrexists() overhead).
 */

/*
 * Check if relation needs a TOAST table
 */
static bool
flux_relation_needs_toast_table(Relation rel)
{
	/*
	 * FLUX uses standard heap TOAST for wide values (it has no on-page
	 * overflow mechanism).  This mirrors heapam_relation_needs_toast_table: a
	 * TOAST table is needed iff there is a toastable attribute and the
	 * maximum tuple length could exceed TOAST_TUPLE_THRESHOLD.
	 */
	int32		data_length = 0;
	bool		maxlength_unknown = false;
	bool		has_toastable_attrs = false;
	TupleDesc	tupdesc = rel->rd_att;
	int32		tuple_length;
	int			i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;
		if (att->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
			continue;
		data_length = att_align_nominal(data_length, att->attalign);
		if (att->attlen > 0)
		{
			data_length += att->attlen;
		}
		else
		{
			int32		maxlen = type_maximum_size(att->atttypid,
												   att->atttypmod);

			if (maxlen < 0)
				maxlength_unknown = true;
			else
				data_length += maxlen;
			if (att->attstorage != TYPSTORAGE_PLAIN)
				has_toastable_attrs = true;
		}
	}
	if (!has_toastable_attrs)
		return false;			/* nothing to toast? */
	if (maxlength_unknown)
		return true;			/* any unlimited-length attrs? */
	tuple_length = MAXALIGN(FLUX_TUPLE_OVERHEAD +
							BITMAPLEN(tupdesc->natts)) +
		MAXALIGN(data_length);
	return (tuple_length > TOAST_TUPLE_THRESHOLD);
}

/*
 * TOAST tables for FLUX relations are ordinary heap relations (FLUX reuses
 * the standard heap TOAST machinery).
 */
static Oid
flux_relation_toast_am(Relation rel)
{
	return HEAP_TABLE_AM_OID;
}

/*
 * Estimate relation size
 *
 * Provides the planner with estimates of the number of pages, tuples,
 * and all-visible fraction for this relation. Uses the actual block count
 * from storage and estimates tuple density from the first non-empty page.
 */
static void
flux_relation_estimate_size(Relation rel, int32 *attr_widths,
							BlockNumber *pages, double *tuples,
							double *allvisfrac)
{
	BlockNumber nblocks;
	double		tuple_count;

	/* Get actual block count from storage */
	nblocks = smgrnblocks(RelationGetSmgr(rel), MAIN_FORKNUM);

	*pages = Max(nblocks, 1);

	if (nblocks == 0)
	{
		*tuples = 0;
		*allvisfrac = 0.0;
		return;
	}

	/*
	 * Estimate tuple count. If we have reltuples from pg_class, use that.
	 * Otherwise, sample the first block to estimate tuple density.
	 */
	if (rel->rd_rel->reltuples >= 0)
	{
		/*
		 * Scale reltuples by the ratio of current pages to relpages to
		 * account for growth or shrinkage since last ANALYZE.
		 */
		if (rel->rd_rel->relpages > 0)
			tuple_count = rel->rd_rel->reltuples *
				((double) nblocks / (double) rel->rd_rel->relpages);
		else
			tuple_count = rel->rd_rel->reltuples;
	}
	else
	{
		/*
		 * No statistics available.  Sample the first non-empty page to
		 * estimate tuple density.  If we can't find one, fall back to a
		 * conservative estimate.
		 */
		double		tuples_per_page = 0;
		BlockNumber probe;

		for (probe = 0; probe < Min(nblocks, 10); probe++)
		{
			Buffer		buf;
			Page		pg;
			OffsetNumber maxoff;
			OffsetNumber off;
			int			live = 0;

			buf = ReadBufferExtended(rel, MAIN_FORKNUM, probe,
									 RBM_NORMAL, NULL);
			LockBuffer(buf, BUFFER_LOCK_SHARE);
			pg = BufferGetPage(buf);

			if (PageIsNew(pg))
			{
				UnlockReleaseBuffer(buf);
				continue;
			}

			maxoff = PageGetMaxOffsetNumber(pg);
			for (off = FirstOffsetNumber; off <= maxoff; off++)
			{
				ItemId		iid = PageGetItemId(pg, off);

				if (!ItemIdIsNormal(iid))
					continue;

				/*
				 * Skip overflow records -- they are not user tuples and
				 * should not inflate the density estimate.
				 */
				if (FluxIsOverflowRecordInline(
											   (FluxTupleHeader *) PageGetItem(pg, iid),
											   ItemIdGetLength(iid)))
					continue;

				live++;
			}

			UnlockReleaseBuffer(buf);

			if (live > 0)
			{
				tuples_per_page = (double) live;
				break;
			}
		}

		/* Fallback if every sampled page was empty or new */
		if (tuples_per_page <= 0)
			tuples_per_page = (BLCKSZ - FLUX_PAGE_OVERHEAD) / 100.0;

		tuple_count = tuples_per_page * nblocks;
	}

	*tuples = Max(tuple_count, 0);

	/*
	 * Compute allvisfrac from pg_class.relallvisible, exactly as heap's
	 * table_block_relation_estimate_size() does.  relallvisible is maintained
	 * by VACUUM and is an O(1) catalog read -- no per-plan Visibility Map
	 * scan.  The previous implementation swept every VM page (plus a
	 * smgrexists()/smgrnblocks() probe of the VM fork) on every query plan,
	 * which showed up as ~9%% of CPU under high-concurrency pgbench.  A stale
	 * catalog value is acceptable here: the planner tolerates an approximate
	 * allvisfrac, and heap relies on the same value.
	 */
	if (rel->rd_rel->relpages > 0)
	{
		double		allvisible;

		allvisible = (double) rel->rd_rel->relallvisible /
			(double) rel->rd_rel->relpages;
		if (allvisible < 0.0)
			allvisible = 0.0;
		else if (allvisible > 1.0)
			allvisible = 1.0;
		*allvisfrac = allvisible;
	}
	else
		*allvisfrac = 0.0;
}

/*
 * Sample scan: get next block for sampling (TABLESAMPLE support)
 *
 * Called by the TABLESAMPLE executor to prepare the next block for tuple
 * extraction.  The TSM (Table Sample Method) decides which block to visit
 * via its NextSampleBlock callback, or, if that callback is NULL, we scan
 * sequentially starting from rs_startblock and wrapping around.
 *
 * We read the selected block into a buffer (pinned, not locked -- locking
 * is deferred to flux_scan_sample_next_tuple) and return true.  Returns
 * false when there are no more blocks to sample.
 */
static bool
flux_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	FluxScanDesc rscan = (FluxScanDesc) scan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	BlockNumber blockno;

	/* Return false immediately if relation is empty */
	if (rscan->rs_nblocks == 0)
		return false;

	/* Release previous buffer, if any */
	if (BufferIsValid(rscan->rs_cbuf))
	{
		ReleaseBuffer(rscan->rs_cbuf);
		rscan->rs_cbuf = InvalidBuffer;
	}

	if (tsm->NextSampleBlock)
	{
		/* TSM tells us which block to visit next */
		blockno = tsm->NextSampleBlock(scanstate, rscan->rs_nblocks);
	}
	else
	{
		/* No NextSampleBlock callback -- scan sequentially */
		if (rscan->rs_cblock == InvalidBlockNumber)
		{
			Assert(!rscan->rs_inited);
			blockno = rscan->rs_startblock;
		}
		else
		{
			Assert(rscan->rs_inited);

			blockno = rscan->rs_cblock + 1;

			if (blockno >= rscan->rs_nblocks)
			{
				/* Wrap to beginning of relation */
				blockno = 0;
			}

			if (blockno == rscan->rs_startblock)
			{
				/* Completed full cycle -- done */
				blockno = InvalidBlockNumber;
			}
		}
	}

	rscan->rs_cblock = blockno;

	if (!BlockNumberIsValid(blockno))
	{
		rscan->rs_inited = false;
		return false;
	}

	Assert(rscan->rs_cblock < rscan->rs_nblocks);

	CHECK_FOR_INTERRUPTS();

	/* Read the selected block -- comes back pinned but not locked */
	rscan->rs_cbuf = ReadBufferExtended(scan->rs_rd, MAIN_FORKNUM,
										blockno, RBM_NORMAL, NULL);

	rscan->rs_inited = true;
	return true;
}

/*
 * Sample scan: get next tuple from current block (TABLESAMPLE support)
 *
 * Called repeatedly for the block prepared by flux_scan_sample_next_block().
 * The TSM's NextSampleTuple callback decides which tuple offsets to examine.
 * We lock the buffer, check the tuple at the selected offset for visibility,
 * and either return it in the slot (true) or indicate end-of-page (false).
 *
 * Unlike the ANALYZE path which iterates all items sequentially, here the
 * TSM picks specific offsets, and we loop until it returns InvalidOffsetNumber
 * to signal that it is done with this block.
 */
static bool
flux_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
							TupleTableSlot *slot)
{
	FluxScanDesc rscan = (FluxScanDesc) scan;
	TsmRoutine *tsm = scanstate->tsmroutine;
	BlockNumber blockno = rscan->rs_cblock;
	Page		page;
	OffsetNumber maxoffset;

	/*
	 * Lock the buffer for visibility checks.  We hold the lock for the
	 * duration of this call and release before returning, matching the heap
	 * AM's non-pagemode pattern.
	 */
	LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_SHARE);

	page = BufferGetPage(rscan->rs_cbuf);
	maxoffset = PageGetMaxOffsetNumber(page);

	for (;;)
	{
		OffsetNumber tupoffset;
		ItemId		itemid;
		FluxTupleHeader *tuple_hdr;
		bool		visible;

		CHECK_FOR_INTERRUPTS();

		/* Ask the TSM which tuple to examine next on this page */
		tupoffset = tsm->NextSampleTuple(scanstate, blockno, maxoffset);

		if (OffsetNumberIsValid(tupoffset))
		{
			/* Skip invalid item pointers */
			itemid = PageGetItemId(page, tupoffset);
			if (!ItemIdIsNormal(itemid))
				continue;

			tuple_hdr = (FluxTupleHeader *) PageGetItem(page, itemid);

			/* Skip overflow records -- not user-visible tuples */
			if (FluxIsOverflowRecordInline(tuple_hdr, ItemIdGetLength(itemid)))
				continue;

			/*
			 * Determine visibility.  FLUX uses heap-shaped xmin/xmax MVCC via
			 * FluxTupleVisibleToSnapshotDual, which handles DELETED/UPDATED
			 * tuples via sLog consultation.
			 */
			if (tuple_hdr->t_flags & FLUX_TUPLE_SPECULATIVE)
				visible = false;
			else if (scan->rs_snapshot)
				visible = FluxTupleVisibleToSnapshotDual(tuple_hdr,
														 scan->rs_snapshot,
														 RelationGetRelid(scan->rs_rd),
														 rscan->rs_cbuf);
			else
				visible = !(tuple_hdr->t_flags & FLUX_TUPLE_DELETED);

			if (!visible)
				continue;

			/*
			 * Found a visible tuple.  Store it into the slot with a buffer
			 * pin so the data stays valid after we unlock.
			 */
			FluxSlotStoreTuple(slot, tuple_hdr,
							   ItemIdGetLength(itemid), rscan->rs_cbuf);
			slot->tts_tableOid = RelationGetRelid(scan->rs_rd);
			ItemPointerSet(&slot->tts_tid, blockno, tupoffset);
			LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			return true;
		}
		else
		{
			/*
			 * NextSampleTuple returned InvalidOffsetNumber -- done with this
			 * block.  Unlock, clear the slot, and tell the caller to move on.
			 */
			LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_UNLOCK);
			ExecClearTuple(slot);
			return false;
		}
	}

	/* unreachable */
	Assert(false);
}

/*
 * ------------------------------------------------------------------------
 * Main table AM routine structure for FLUX
 * ------------------------------------------------------------------------
 */
static const TableAmRoutine flux_methods = {
	.type = T_TableAmRoutine,

	/*
	 * FLUX table DML records UNDO into the PER-BACKEND UNDO engine
	 * (access/undo/perbackend) as of Phase 8b: flux_operations.c prepares one
	 * UnpackedUndoRecord per INSERT/UPDATE/DELETE via the flux_pbu.c shim
	 * (PrepareUndoInsert/InsertPreparedUndo), tagged uur_rmid = UNDO_RMID_FLUX,
	 * folding the undo page onto the FLUX forward WAL record as a full-page
	 * image for crash-safety.  On abort AbortTransaction() ->
	 * PbuAtAbort_ApplyUndo() -> execute_undo_actions() dispatches each record to
	 * flux_undo_apply() (the FLUX undo rmgr's rm_undo) which restores the
	 * before-image in place.
	 *
	 * Phase 8c: FLUX now declares am_undo_engine = UNDO_ENGINE_PERBACKEND and
	 * am_index_delete_marking = true.  An indexed-column UPDATE stays IN PLACE
	 * (stable TID): flux_tuple_update inserts the new (k_new,TID) index entry
	 * and delete-marks the old (k_old,TID) entry (index_delete_mark), reporting
	 * TU_None so the executor does not re-insert.  There is NO separate index
	 * UNDO: nbtree/hash suppress their index-UNDO writes for a delete-marking
	 * parent (RelationSupportsDeleteMarking) because FLUX's own table UNDO
	 * (flux_undo_apply, FLUX_UNDO_UPDATE) drives index cleanup on rollback --
	 * it clears the old delete-mark and removes the new (k_new,TID) entry,
	 * restoring the index to its exact pre-update state.  So flipping the
	 * engine to PERBACKEND does NOT route any index UNDO through the (still
	 * stubbed) nbtree/hash per-backend write arms: those arms are never
	 * reached for a delete-marking table.
	 */
	.am_supports_undo = true,
	/* updates in place at a stable TID; never reports traversed */
	.am_inplace_update_keeps_tid = true,
	.am_undo_engine = UNDO_ENGINE_PERBACKEND,
	.am_index_delete_marking = true,

	/*
	 * FLUX secondary indexes are plain 6-byte heap-TID nbtree entries with
	 * standard maintenance, like heap.  A key-changing UPDATE is heap-like
	 * (new TID, old version to UNDO, old index entry dies and is VACUUMed); a
	 * non-key UPDATE is in place.  The executor treats FLUX exactly like heap
	 * for indexing (the plain-TID index path; see indexam.c /
	 * execIndexing.c).
	 */

	/* Use minimal tuple slot */
	.slot_callbacks = flux_slot_callbacks,

	/* Use minimal scan functions - just return empty results */
	.scan_begin = flux_scan_begin,
	.scan_end = flux_scan_end,
	.scan_rescan = flux_scan_rescan,
	.scan_getnextslot = flux_scan_getnextslot,

	.scan_set_tidrange = flux_scan_set_tidrange,
	.scan_getnextslot_tidrange = flux_scan_getnextslot_tidrange,

	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	/* Use minimal index functions */
	.index_scan_begin = flux_index_scan_begin,
	.index_scan_reset = flux_index_scan_reset,
	.index_scan_end = flux_index_scan_end,

	/* Use minimal tuple functions */
	.tuple_insert = flux_tuple_insert,
	.tuple_insert_speculative = flux_tuple_insert_speculative,
	.tuple_complete_speculative = flux_tuple_complete_speculative,
	.multi_insert = flux_multi_insert,
	.tuple_delete = flux_tuple_delete,
	.tuple_update = flux_tuple_update,
	.tuple_lock = flux_tuple_lock,

	/* UNDO write-buffer activation / deactivation */
	.begin_bulk_insert = flux_begin_bulk_insert,
	.finish_bulk_insert = flux_finish_bulk_insert,

	.fetch_tid = flux_fetch_tid,
	.tuple_fetch_row_version = flux_tuple_fetch_row_version,
	.tuple_get_latest_tid = flux_tuple_get_latest_tid,
	.tuple_tid_valid = flux_tuple_tid_valid,
	.tuple_satisfies_snapshot = flux_tuple_satisfies_snapshot,
	.index_delete_tuples = flux_index_delete_tuples,

	/* Keep only essential relation functions */
	.relation_set_new_filelocator = flux_relation_set_new_filelocator,
	.relation_nontransactional_truncate = flux_relation_nontransactional_truncate,
	.relation_copy_data = flux_relation_copy_data,
	.relation_copy_for_cluster = flux_relation_copy_for_cluster,
	.relation_vacuum = flux_relation_vacuum,
	.scan_analyze_next_block = flux_scan_analyze_next_block,
	.scan_analyze_next_tuple = flux_scan_analyze_next_tuple,
	.index_build_range_scan = flux_index_build_range_scan,
	.index_validate_scan = flux_index_validate_scan,

	.relation_size = table_block_relation_size,
	.relation_needs_toast_table = flux_relation_needs_toast_table,
	.relation_toast_am = flux_relation_toast_am,
	.relation_fetch_toast_slice = heap_fetch_toast_slice,

	.relation_estimate_size = flux_relation_estimate_size,

	.scan_bitmap_next_tuple = flux_scan_bitmap_next_tuple,
	.scan_sample_next_block = flux_scan_sample_next_block,
	.scan_sample_next_tuple = flux_scan_sample_next_tuple,

	/*
	 * FLUX secondary indexes hold plain 6-byte heap-style TID entries and are
	 * maintained exactly like heap indexes.  A key-changing UPDATE stores the
	 * new version out of place (new TID) so the old (key, TID) index entry
	 * becomes dead and is reclaimed by VACUUM, never duplicated -- see
	 * flux_tuple_update().
	 */
};

/*
 * Return the FLUX table AM routine
 */
const TableAmRoutine *
GetFluxTableAmRoutine(void)
{
	return &flux_methods;
}

/*
 * Handler function for FLUX table access method
 */
PG_FUNCTION_INFO_V1(flux_tableam_handler);

Datum
flux_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&flux_methods);
}

/*
 * flux_analyze_accumulate_sample
 *
 * During an ANALYZE scan, capture the decompressed bytes of the relation's
 * first varlena column from the just-materialized slot.  These samples feed
 * FluxMaybeRefreshDict() at scan end so it can train a candidate compression
 * dictionary from data ANALYZE already sampled, with no extra table reads.
 *
 * Sample buffers are allocated lazily in the scan's own memory context and
 * are bounded by fixed byte/count caps.  Once a cap is hit we stop collecting
 * but let the ANALYZE scan continue normally.
 */
static void
flux_analyze_accumulate_sample(FluxScanDesc rscan, TupleTableSlot *slot)
{
	/*
	 * FLUX does not compress attributes and has no compression dictionary, so
	 * ANALYZE does not accumulate a training corpus.
	 */
	(void) rscan;
	(void) slot;
}

/*
 * ANALYZE support: select next block to sample
 *
 * Called by ANALYZE to prepare the next sampled block for tuple extraction.
 * The ReadStream provides buffers for blocks selected by the BlockSampler
 * in analyze.c -- we do not choose blocks ourselves.
 *
 * We acquire a buffer pin and shared lock here and hold them until
 * flux_scan_analyze_next_tuple() has returned false for this block,
 * preventing concurrent activity (e.g. pruning) from removing tuples
 * out from under us.
 */
static bool
flux_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	FluxScanDesc rscan = (FluxScanDesc) scan;

	/*
	 * Get the next buffer from the read stream.  The stream was set up by
	 * analyze.c with a BlockSampler callback, so it yields only the randomly
	 * selected sample blocks.  The buffer comes back already pinned.
	 */
	rscan->rs_cbuf = read_stream_next_buffer(stream, NULL);

	if (!BufferIsValid(rscan->rs_cbuf))
		return false;

	/*
	 * Don't lock the buffer here; flux_scan_analyze_next_tuple() manages its
	 * own lock/unlock cycle so it can release the lock before returning a
	 * sampled tuple, allowing FluxFetchOverflowColumn() to safely lock the
	 * same buffer for overflow data on this page.
	 */

	rscan->rs_cblock = BufferGetBlockNumber(rscan->rs_cbuf);
	rscan->rs_cindex = FirstOffsetNumber;

	return true;
}

/*
 * ANALYZE support: get next tuple from current block
 *
 * Extracts tuples one at a time from the block prepared by
 * flux_scan_analyze_next_block().  For each item pointer on the page we
 * classify the tuple as live, dead, or not-a-tuple (overflow record,
 * unused pointer) and update the caller's counters.
 *
 * When a live tuple suitable for sampling is found, it is materialized
 * into the slot and we return true.  When all items on the page have been
 * examined, we release the buffer and return false.
 *
 * The buffer remains pinned and locked for the entire duration of tuple
 * iteration on this block, matching the heap AM contract.
 */
static bool
flux_scan_analyze_next_tuple(TableScanDesc scan,
							 double *liverows, double *deadrows,
							 TupleTableSlot *slot)
{
	FluxScanDesc rscan = (FluxScanDesc) scan;
	Page		targpage;
	OffsetNumber maxoffset;

	Assert(BufferIsValid(rscan->rs_cbuf));

	/*
	 * Re-acquire the buffer content lock.  We release it before returning a
	 * sampled tuple (see below) so that the caller can safely deform the
	 * tuple -- FluxFetchOverflowColumn() may need to lock the same buffer to
	 * read overflow data stored on the same page.
	 */
	LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_SHARE);

	targpage = BufferGetPage(rscan->rs_cbuf);
	maxoffset = PageGetMaxOffsetNumber(targpage);

	/* Inner loop over items on the selected page */
	for (; rscan->rs_cindex <= maxoffset; rscan->rs_cindex++)
	{
		ItemId		itemid;
		FluxTupleHeader *tuple_hdr;
		bool		sample_it = false;
		UndoRecPtr proxy_verptr = InvalidUndoRecPtr;

		itemid = PageGetItemId(targpage, rscan->rs_cindex);

		/*
		 * Skip unused and dead line pointers.  Dead line pointers are counted
		 * as dead rows because vacuum needs to reclaim them.
		 */
		if (!ItemIdIsNormal(itemid))
		{
			if (ItemIdIsDead(itemid))
				*deadrows += 1;
			continue;
		}

		tuple_hdr = (FluxTupleHeader *) PageGetItem(targpage, itemid);

		/* Skip overflow records -- these are not user-visible tuples */
		if (FluxIsOverflowRecordInline(tuple_hdr, ItemIdGetLength(itemid)))
			continue;

		/*
		 * Classify the tuple for ANALYZE purposes.  FLUX uses timestamp-based
		 * MVCC rather than xmin/xmax, so we check the tuple flags and
		 * timestamps directly.
		 */
		if (tuple_hdr->t_flags & FLUX_TUPLE_DELETED)
		{
			/* Dead tuple (deleted) -- counted as dead for ANALYZE */
			*deadrows += 1;
		}
		else if (tuple_hdr->t_flags & FLUX_TUPLE_SPECULATIVE)
		{
			/*
			 * Speculative insertion not yet confirmed.  Don't count it; if
			 * the inserter commits it will be picked up by a future ANALYZE.
			 */
		}
		else
		{
			/*
			 * Tuple is live (or at least not deleted/speculative). Sample it
			 * for statistics.
			 */
			sample_it = true;
			*liverows += 1;

			/*
			 * Synthetic dead-tuple proxy for the UNDO log.  An
			 * in-place FLUX UPDATE creates no genuine dead tuple, so the
			 * standard dead-tuple autovacuum trigger would never fire and the
			 * UNDO log would grow unbounded.  A live tuple that carries a
			 * version pointer has a prior committed image retained in the
			 * fork; if that record has not yet been discarded it is
			 * reclaimable work, so we count it as a dead-tuple proxy.  The
			 * liveness probe touches a fork buffer, so it is deferred until
			 * after the content lock on the data page is released (below),
			 * matching the overflow-fetch discipline used for the dictionary
			 * sample.
			 */
			proxy_verptr = FluxTupleGetVersionPtr(tuple_hdr,
												  ItemIdGetLength(itemid));
		}

		if (sample_it)
		{
			/*
			 * Materialize the tuple into palloc'd memory rather than storing
			 * a buffer-pinned pointer.  This is necessary because slot
			 * deformation may call FluxFetchOverflowColumn(), which acquires
			 * buffer locks on overflow pages.  If the overflow data resides
			 * on the same page we are scanning, a buffer-pinned slot would
			 * cause a lock re-entry assertion failure in LockBuffer
			 * (bufmgr.c).
			 */
			Size		tuple_size = ItemIdGetLength(itemid);
			FluxTupleHeader *tuple_copy;

			tuple_copy = (FluxTupleHeader *) palloc(tuple_size);
			memcpy(tuple_copy, tuple_hdr, tuple_size);

			FluxSlotStoreMaterializedTuple(slot, tuple_copy, tuple_size);
			slot->tts_tableOid = RelationGetRelid(scan->rs_rd);
			ItemPointerSet(&slot->tts_tid, rscan->rs_cblock, rscan->rs_cindex);
			rscan->rs_cindex++;

			/*
			 * Release the content lock so the caller can safely deform the
			 * materialized tuple.  The buffer pin is kept so the page stays
			 * in the buffer pool.  We re-acquire the lock at the top of this
			 * function when called again.
			 */
			LockBuffer(rscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

			/*
			 * Opportunistically feed the dictionary-refresh corpus from the
			 * same sampled tuple.  Done after the unlock so any
			 * overflow-column fetch can lock this page safely.
			 */
			if (scan->rs_flags & SO_TYPE_ANALYZE)
				flux_analyze_accumulate_sample(rscan, slot);

			/*
			 * Count the per-backend UNDO version-chain proxy (see the
			 * live-tuple branch above).  Probing the record must happen with
			 * no data page content lock held.  FluxPbuRecordExists returns
			 * false once the record has been discarded, so the proxy resets
			 * after the undo log is reclaimed.
			 */
			if (UndoRecPtrIsValid(proxy_verptr))
			{
				if (FluxPbuRecordExists(proxy_verptr))
					*deadrows += 1;
			}

			return true;
		}
	}

	/*
	 * No more tuples on this page.  Release the buffer pin and lock that were
	 * acquired in flux_scan_analyze_next_block().
	 */
	UnlockReleaseBuffer(rscan->rs_cbuf);
	rscan->rs_cbuf = InvalidBuffer;

	/* Prevent stale slot contents from holding a pin */
	ExecClearTuple(slot);

	return false;
}
