/*-------------------------------------------------------------------------
 *
 * relundo_apply.c
 *	  Apply per-relation UNDO records for transaction rollback
 *
 * This module implements transaction rollback for per-relation UNDO.
 * It walks the UNDO chain backwards and applies each operation to restore
 * the database to its pre-transaction state.
 *
 * The rollback operations are:
 *   - INSERT: Mark inserted tuples as dead/unused
 *   - DELETE: Restore deleted tuple from UNDO record
 *   - UPDATE: Restore old tuple version from UNDO record
 *   - TUPLE_LOCK: Remove lock marker
 *
 * For crash safety, we write Compensation Log Records (CLRs) for each
 * UNDO application. If we crash during rollback, the CLRs prevent
 * double-application when recovery replays the UNDO chain.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/relundo_apply.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/relundo.h"
#include "access/relundo_xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/rel.h"

/*
 * Maximum number of distinct data pages a single UNDO-record apply may touch.
 * A per-relation UNDO consumer emits INSERT (1 page), in-place UPDATE (1),
 * out-of-place UPDATE/DELTA (2), and DELETE with ntids==1 (1).  The cap leaves
 * ample room below XLR_MAX_BLOCK_ID (32) for the fork page registered
 * alongside.
 */
#define RELUNDO_APPLY_MAX_DATA_BUFS		8

/*
 * Maximum number of consecutive UNDO records folded into one compensation log
 * record (CLR).  A bulk DELETE/UPDATE rollback emits one UNDO record per tuple,
 * and long runs of those records target the same data page and live on the same
 * UNDO-fork page (the chain is walked in reverse-insertion order).  Folding such
 * a run into a single CLR replaces N forced full-page images plus N XLogFlush
 * calls with one of each.  The cap bounds how long the batch holds the data
 * page's exclusive buffer lock; each per-record apply is an in-memory
 * memcpy/flag-set, so 128 is a small, bounded hold.
 */
#define RELUNDO_APPLY_MAX_BATCH			128

/*
 * Maximum number of distinct UNDO-fork pages whose records a single CLR may
 * cover.  Each contributes one forced full-page image and one exclusive buffer
 * lock held across the XLogInsert.  Together with the single batched data page,
 * the total registered blocks (1 + this) stays well under XLR_MAX_BLOCK_ID (32).
 */
#define RELUNDO_APPLY_MAX_FORK_BUFS		8

/* Forward declarations for internal functions */
static Page RelUndoTrackPage(Relation rel, Buffer *touched, int *ntouched,
							  BlockNumber blkno);
static bool RelUndoRecordSingleDataPage(const RelUndoRecordHeader *header,
										const void *payload, BlockNumber *blk);
static bool RelUndoForkTrack(BlockNumber *fork_blks, int *nfork,
							 BlockNumber blkno);
static void RelUndoApplyOneRecord(Relation rel, const RelUndoRecordHeader *header,
								  void *payload, RelUndoRecPtr current_ptr,
								  Buffer *touched, int *ntouched);
static void RelUndoApplyInsert(Relation rel, Page page, OffsetNumber offset);
static void RelUndoApplyUpdate(Relation rel, Page page, OffsetNumber offset,
							   char *tuple_data, uint32 tuple_len);
static void RelUndoApplyDelete(Relation rel, Page page, OffsetNumber offset,
							   char *tuple_data, uint32 tuple_len);
static void RelUndoApplyTupleLock(Relation rel, Page page, OffsetNumber offset);
static void RelUndoLogApplyCLR(Relation rel, const RelUndoRecPtr *urec_ptrs,
							   int nptrs, Buffer *touched, int ntouched);

/*
 * RelUndoApplyChain - Walk and apply per-relation UNDO chain for rollback
 *
 * This is the main entry point for transaction abort. We walk backwards
 * through the UNDO chain starting from start_ptr, applying each operation
 * until we reach an invalid pointer or the beginning of the chain.
 *
 * Each record type is handled self-contained: each case manages its own
 * buffer acquisition, apply, dirty marking, and buffer release.
 */
void
RelUndoApplyChain(Relation rel, RelUndoRecPtr start_ptr)
{
	RelUndoRecPtr current_ptr = start_ptr;
	RelUndoRecordHeader header;
	void	   *payload = NULL;
	Size		payload_size;

	if (!RelUndoRecPtrIsValid(current_ptr))
	{
		elog(DEBUG1, "RelUndoApplyChain: no valid UNDO pointer");
		return;
	}

	elog(DEBUG1, "RelUndoApplyChain: starting rollback at %lu",
		 (unsigned long) current_ptr);

	/*
	 * Walk backwards through the chain, applying each record.
	 *
	 * Each record's physical restoration is applied to its data page(s) while
	 * holding their buffers exclusively locked, then a single redoable
	 * compensation log record (XLOG_RELUNDO_APPLY) logs full-page images of
	 * every restored page plus the UNDO-fork page(s) (carrying the CLR_APPLIED
	 * flag).  The apply helpers may ereport(ERROR) on corruption, so they run
	 * BEFORE the critical section in RelUndoLogApplyCLR; an error there aborts
	 * the worker transaction and releases the buffer locks.
	 *
	 * Bulk DELETE/UPDATE rollback emits one UNDO record per tuple, and long
	 * runs of those records target the same data page (reverse-insertion
	 * order).  We fold such a run into one CLR: we keep that one data page's
	 * buffer exclusively locked and apply each record's in-memory restoration
	 * onto it, accumulating the records' urec_ptrs, then emit a single CLR
	 * (one forced data-page image, one flush) covering the whole batch.  A
	 * record that does not target exactly that single data page (out-of-place
	 * update, multi-TID delete, a different page) terminates the batch and is
	 * handled on its own with the unchanged singleton path.  Folding never
	 * widens the simultaneous buffer-lock footprint beyond one data page.
	 */
	while (RelUndoRecPtrIsValid(current_ptr))
	{
		Buffer		touched[RELUNDO_APPLY_MAX_DATA_BUFS];
		int			ntouched = 0;
		RelUndoRecPtr batch_ptrs[RELUNDO_APPLY_MAX_BATCH];
		int			nbatch = 0;
		BlockNumber batch_blkno = InvalidBlockNumber;
		BlockNumber fork_blks[RELUNDO_APPLY_MAX_FORK_BUFS];
		int			nfork = 0;
		int			i;

		if (!RelUndoReadRecord(rel, current_ptr, &header, &payload, &payload_size))
		{
			elog(WARNING, "RelUndoApplyChain: could not read UNDO record at %lu",
				 (unsigned long) current_ptr);
			break;
		}

		/* Skip already-applied records (CLR check for crash safety) */
		if (header.info_flags & RELUNDO_INFO_CLR_APPLIED)
		{
			elog(DEBUG1, "RelUndoApplyChain: skipping already-applied record at %lu",
				 (unsigned long) current_ptr);
			current_ptr = header.urec_prevundorec;
			if (payload)
			{
				pfree(payload);
				payload = NULL;
			}
			continue;
		}

		elog(DEBUG1, "RelUndoApplyChain: processing record type %d at %lu",
			 header.urec_type, (unsigned long) current_ptr);

		/* Apply this record; it pins+locks its data page(s) into touched[]. */
		RelUndoApplyOneRecord(rel, &header, payload, current_ptr,
							  touched, &ntouched);
		batch_ptrs[nbatch++] = current_ptr;
		(void) RelUndoRecordSingleDataPage(&header, payload, &batch_blkno);
		(void) RelUndoForkTrack(fork_blks, &nfork, RelUndoGetBlockNum(current_ptr));

		current_ptr = header.urec_prevundorec;
		if (payload)
		{
			pfree(payload);
			payload = NULL;
		}

		/*
		 * Extend the batch with following records that restore the very same
		 * single data page.  We only peek ahead while the current record was
		 * itself a single-page record (batch_blkno valid); a multi-page record
		 * leaves ntouched > 1 and ends the batch immediately.
		 */
		while (ntouched == 1 &&
			   BlockNumberIsValid(batch_blkno) &&
			   nbatch < RELUNDO_APPLY_MAX_BATCH &&
			   RelUndoRecPtrIsValid(current_ptr))
		{
			RelUndoRecordHeader peek_hdr;
			void	   *peek_payload = NULL;
			Size		peek_size;
			BlockNumber peek_blkno = InvalidBlockNumber;

			if (!RelUndoReadRecord(rel, current_ptr, &peek_hdr,
								   &peek_payload, &peek_size))
				break;

			if (peek_hdr.info_flags & RELUNDO_INFO_CLR_APPLIED)
			{
				/* Stop the batch; the skip is handled by the outer loop. */
				if (peek_payload)
					pfree(peek_payload);
				break;
			}

			if (!RelUndoRecordSingleDataPage(&peek_hdr, peek_payload, &peek_blkno) ||
				peek_blkno != batch_blkno)
			{
				/* Not foldable: leave current_ptr for the outer loop. */
				if (peek_payload)
					pfree(peek_payload);
				break;
			}

			/*
			 * The CLR will register one forced image per distinct fork page in
			 * the batch.  If folding this record would exceed the fork-page cap
			 * (and its fork page is not already in the batch), stop here.
			 */
			if (nfork >= RELUNDO_APPLY_MAX_FORK_BUFS)
			{
				BlockNumber peek_fork = RelUndoGetBlockNum(current_ptr);
				bool		seen = false;

				for (i = 0; i < nfork; i++)
				{
					if (fork_blks[i] == peek_fork)
					{
						seen = true;
						break;
					}
				}
				if (!seen)
				{
					if (peek_payload)
						pfree(peek_payload);
					break;
				}
			}

			/* Foldable: apply onto the already-locked data page. */
			RelUndoApplyOneRecord(rel, &peek_hdr, peek_payload, current_ptr,
								  touched, &ntouched);
			batch_ptrs[nbatch++] = current_ptr;
			(void) RelUndoForkTrack(fork_blks, &nfork,
									RelUndoGetBlockNum(current_ptr));

			current_ptr = peek_hdr.urec_prevundorec;
			if (peek_payload)
				pfree(peek_payload);
		}

		/*
		 * Log one redoable CLR carrying full-page images of every restored
		 * data page plus the fork page(s) for the batched records, then
		 * release the data buffers.  For non-WAL relations there is nothing to
		 * log; just mark dirty and release.
		 */
		if (RelationNeedsWAL(rel))
			RelUndoLogApplyCLR(rel, batch_ptrs, nbatch, touched, ntouched);
		else
		{
			for (i = 0; i < ntouched; i++)
			{
				MarkBufferDirty(touched[i]);
				UnlockReleaseBuffer(touched[i]);
			}
		}
	}

	elog(DEBUG1, "RelUndoApplyChain: rollback complete");
}

/*
 * RelUndoApplyRecordForRecovery - Reverse-apply one UNDO record during crash
 *		recovery, without writing a compensation log record (CLR).
 *
 * Crash recovery rolls back loser transactions after the redo pass.  At that
 * point WAL insertion is not yet permitted (LocalSetXLogInsertAllowed() runs
 * later in StartupXLOG), so this path mirrors the non-WAL branch of
 * RelUndoApplyChain: it restores the before-image into the data page(s) in
 * memory and marks them dirty, relying on the end-of-recovery checkpoint for
 * durability.  Re-application is harmless because redo always re-establishes
 * the post-modification page state before this reverse-apply runs, so a crash
 * mid-recovery simply replays redo and re-applies the before-image.
 *
 * Records already carrying RELUNDO_INFO_CLR_APPLIED (rolled back online before
 * the crash) are skipped.  The caller is responsible for driving this in
 * newest-first order for each loser transaction's tracked record pointers.
 *
 * Idempotency across a second crash that brackets the end-of-recovery
 * checkpoint: absolute records (full-tuple UPDATE/DELETE, INSERT) restore a
 * fixed before-image, so re-applying them is a no-op.  A DELTA record
 * reconstructs the before-image relative to the live page tuple, which is NOT
 * idempotent in isolation; the write path guarantees safety by emitting DELTA
 * only when overwriting a COMMITTED version, so at most one DELTA exists per
 * tid and every newer record for that tid is absolute.  Newest-first replay
 * therefore re-derives the DELTA's page anchor before it is reverse-applied,
 * and a lone DELTA reverse-apply (overwrite-with-old-bytes) is idempotent.
 */
void
RelUndoApplyRecordForRecovery(Relation rel, RelUndoRecPtr ptr)
{
	RelUndoRecordHeader header;
	void	   *payload = NULL;
	Size		payload_size;
	Buffer		touched[RELUNDO_APPLY_MAX_DATA_BUFS];
	int			ntouched = 0;
	int			i;

	if (!RelUndoRecPtrIsValid(ptr))
		return;

	if (!RelUndoReadRecord(rel, ptr, &header, &payload, &payload_size))
	{
		elog(WARNING, "RelUndoApplyRecordForRecovery: could not read UNDO record at %lu",
			 (unsigned long) ptr);
		return;
	}

	/* Already rolled back online before the crash: nothing to do. */
	if (header.info_flags & RELUNDO_INFO_CLR_APPLIED)
	{
		if (payload)
			pfree(payload);
		return;
	}

	RelUndoApplyOneRecord(rel, &header, payload, ptr, touched, &ntouched);

	for (i = 0; i < ntouched; i++)
	{
		MarkBufferDirty(touched[i]);
		UnlockReleaseBuffer(touched[i]);
	}

	if (payload)
		pfree(payload);
}

/*
 * RelUndoRecordSingleDataPage - Classify whether an UNDO record restores
 *		exactly one data page, and if so report that block number.
 *
 * Returns true and sets *blk when the record's restoration touches a single
 * data page (in-place UPDATE, single-TID DELETE, INSERT, TUPLE_LOCK).  Returns
 * false for records that may touch two pages (out-of-place UPDATE where oldtid
 * != newtid) or more than one TID (multi-TID DELETE); those are not eligible to
 * be folded into a same-page batch.  No buffers are touched.
 */
static bool
RelUndoRecordSingleDataPage(const RelUndoRecordHeader *header,
							const void *payload, BlockNumber *blk)
{
	*blk = InvalidBlockNumber;

	switch (header->urec_type)
	{
		case RELUNDO_INSERT:
			{
				const RelUndoInsertPayload *p = payload;

				*blk = ItemPointerGetBlockNumber(&p->firsttid);
				return true;
			}

		case RELUNDO_DELETE:
			{
				const RelUndoDeletePayload *p = payload;

				if (p->ntids != 1)
					return false;
				*blk = ItemPointerGetBlockNumber(&p->tids[0]);
				return true;
			}

		case RELUNDO_UPDATE:
			{
				const RelUndoUpdatePayload *p = payload;

				if (!ItemPointerEquals(&p->oldtid, &p->newtid))
					return false;
				*blk = ItemPointerGetBlockNumber(&p->oldtid);
				return true;
			}

		case RELUNDO_TUPLE_LOCK:
			{
				const RelUndoTupleLockPayload *p = payload;

				*blk = ItemPointerGetBlockNumber(&p->tid);
				return true;
			}

		default:
			return false;
	}
}

/*
 * RelUndoForkTrack - Add an UNDO-fork block to the batch's distinct-fork set.
 *
 * Returns true if blkno was newly added, false if it was already present or the
 * set is full (caller has already guaranteed room via the cap check).
 */
static bool
RelUndoForkTrack(BlockNumber *fork_blks, int *nfork, BlockNumber blkno)
{
	int			i;

	for (i = 0; i < *nfork; i++)
	{
		if (fork_blks[i] == blkno)
			return false;
	}

	Assert(*nfork < RELUNDO_APPLY_MAX_FORK_BUFS);
	fork_blks[(*nfork)++] = blkno;
	return true;
}

/*
 * RelUndoApplyOneRecord - Apply one UNDO record's physical restoration.
 *
 * Pins+locks the record's data page(s) into touched[] (via RelUndoTrackPage,
 * which deduplicates against pages already locked for this batch) and mutates
 * them in memory.  Does NOT log or release buffers; the caller batches one or
 * more applied records and emits a single CLR.  May ereport(ERROR) on
 * corruption, before any WAL is written.
 */
static void
RelUndoApplyOneRecord(Relation rel, const RelUndoRecordHeader *header,
					  void *payload, RelUndoRecPtr current_ptr,
					  Buffer *touched, int *ntouched)
{
	Page		page;
	BlockNumber target_blkno;
	OffsetNumber target_offset;
	int			i;

	switch (header->urec_type)
	{
		case RELUNDO_INSERT:
			{
				RelUndoInsertPayload *ins_payload = (RelUndoInsertPayload *) payload;

				target_blkno = ItemPointerGetBlockNumber(&ins_payload->firsttid);
				target_offset = ItemPointerGetOffsetNumber(&ins_payload->firsttid);

				page = RelUndoTrackPage(rel, touched, ntouched, target_blkno);
				RelUndoApplyInsert(rel, page, target_offset);
				break;
			}

		case RELUNDO_DELETE:
			{
				RelUndoDeletePayload *del_payload = (RelUndoDeletePayload *) payload;
				char	   *tuple_data_buf = NULL;
				uint32		tlen = 0;

				RelUndoReadRecordWithTuple(rel, current_ptr,
										   &tuple_data_buf, &tlen);

				for (i = 0; i < del_payload->ntids; i++)
				{
					target_blkno = ItemPointerGetBlockNumber(&del_payload->tids[i]);
					target_offset = ItemPointerGetOffsetNumber(&del_payload->tids[i]);

					page = RelUndoTrackPage(rel, touched, ntouched, target_blkno);

					if (tuple_data_buf && tlen > 0)
						RelUndoApplyDelete(rel, page, target_offset,
										   tuple_data_buf, tlen);
				}

				if (tuple_data_buf)
					pfree(tuple_data_buf);
				break;
			}

		case RELUNDO_UPDATE:
			{
				RelUndoUpdatePayload *upd_payload = (RelUndoUpdatePayload *) payload;
				char	   *tuple_data_buf = NULL;
				uint32		tlen = 0;

				RelUndoReadRecordWithTuple(rel, current_ptr,
										   &tuple_data_buf, &tlen);

				/* Restore old tuple at the old location */
				target_blkno = ItemPointerGetBlockNumber(&upd_payload->oldtid);
				target_offset = ItemPointerGetOffsetNumber(&upd_payload->oldtid);

				page = RelUndoTrackPage(rel, touched, ntouched, target_blkno);

				/*
				 * Partial-tuple (byte-diff) record: the trailing bytes are a
				 * RelUndoDiffRecord, not a full before-image.  Reconstruct the
				 * old tuple from the current on-page bytes plus the diff, then
				 * restore it exactly like a full-image update.  Reverse-apply is
				 * a self-describing byte splice, so no table-AM callback is
				 * needed.  Only reachable for an in-place update (oldtid ==
				 * newtid); the producing AM emits a delta only when the on-page
				 * slot length equals the length the diff was computed against.
				 */
				if ((header->info_flags & RELUNDO_INFO_PARTIAL_TUPLE) &&
					tuple_data_buf && tlen >= SizeOfRelUndoDiffRecord)
				{
					const RelUndoDiffRecord *diff =
						(const RelUndoDiffRecord *) tuple_data_buf;
					ItemId		dlp = PageGetItemId(page, target_offset);

					if (ItemIdIsNormal(dlp))
					{
						const char *cur = (const char *) PageGetItem(page, dlp);
						Size		cur_len = ItemIdGetLength(dlp);
						Size		recon_len = 0;
						char	   *recon = palloc(diff->old_total_len);

						if (RelUndoApplyDiffReverse(cur, cur_len, diff,
												recon, &recon_len))
							RelUndoApplyUpdate(rel, page, target_offset,
										   recon, (uint32) recon_len);
						else
							elog(ERROR, "RelUndoApplyChain: byte-diff reverse-apply "
								 "failed at (%u,%u)", target_blkno, target_offset);
						pfree(recon);
					}
					else
						elog(DEBUG2, "RelUndoApplyChain: delta target (%u,%u) not "
							 "normal, skipping", target_blkno, target_offset);
				}
				else if (tuple_data_buf && tlen > 0)
					RelUndoApplyUpdate(rel, page, target_offset,
									   tuple_data_buf, tlen);

				if (tuple_data_buf)
					pfree(tuple_data_buf);

				/*
				 * Mark the new tuple version as unused, but only for
				 * out-of-place updates where oldtid != newtid.  For in-place
				 * updates the old and new tuple share the same slot, so marking
				 * it unused would destroy the just-restored old tuple.
				 */
				if (!ItemPointerEquals(&upd_payload->oldtid,
									   &upd_payload->newtid))
				{
					BlockNumber new_blkno;
					OffsetNumber new_offset;
					Page		new_page;

					new_blkno = ItemPointerGetBlockNumber(&upd_payload->newtid);
					new_offset = ItemPointerGetOffsetNumber(&upd_payload->newtid);

					new_page = RelUndoTrackPage(rel, touched, ntouched, new_blkno);
					RelUndoApplyInsert(rel, new_page, new_offset);
				}
				break;
			}

		case RELUNDO_TUPLE_LOCK:
			{
				RelUndoTupleLockPayload *lock_payload = (RelUndoTupleLockPayload *) payload;

				target_blkno = ItemPointerGetBlockNumber(&lock_payload->tid);
				target_offset = ItemPointerGetOffsetNumber(&lock_payload->tid);

				page = RelUndoTrackPage(rel, touched, ntouched, target_blkno);
				RelUndoApplyTupleLock(rel, page, target_offset);
				break;
			}

		default:
			/* Release any tracked buffers before erroring out. */
			for (i = 0; i < *ntouched; i++)
				UnlockReleaseBuffer(touched[i]);
			*ntouched = 0;
			elog(ERROR, "RelUndoApplyChain: unknown UNDO record type %d",
				 header->urec_type);
	}
}

/*
 * RelUndoTrackPage - Read+pin+exclusive-lock a data page for the current
 *		apply, deduplicating against pages already locked for this record.
 *
 * Returns the page so the caller can mutate it.  The buffer is added to the
 * touched[] array (kept locked) the first time a block is requested; a repeat
 * request for the same block returns the already-locked page.  All tracked
 * buffers are released by RelUndoLogApplyCLR after the CLR is logged.
 */
static Page
RelUndoTrackPage(Relation rel, Buffer *touched, int *ntouched,
				 BlockNumber blkno)
{
	Buffer		buf;
	int			i;

	for (i = 0; i < *ntouched; i++)
	{
		if (BufferGetBlockNumber(touched[i]) == blkno)
			return BufferGetPage(touched[i]);
	}

	if (*ntouched >= RELUNDO_APPLY_MAX_DATA_BUFS)
	{
		/* Release locks before erroring so the worker can clean up. */
		for (i = 0; i < *ntouched; i++)
			UnlockReleaseBuffer(touched[i]);
		*ntouched = 0;
		elog(ERROR, "RelUndoApplyChain: too many data pages (%d) for one UNDO record",
			 RELUNDO_APPLY_MAX_DATA_BUFS + 1);
	}

	buf = ReadBuffer(rel, blkno);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	touched[*ntouched] = buf;
	(*ntouched)++;
	return BufferGetPage(buf);
}

/*
 * RelUndoApplyInsert - Undo an INSERT operation
 *
 * Mark the inserted tuple as dead/unused. For INSERT, we don't need the
 * original tuple data - we just mark the slot as available.
 */
static void
RelUndoApplyInsert(Relation rel, Page page, OffsetNumber offset)
{
	ItemId		lp;

	elog(DEBUG1, "RelUndoApplyInsert: page=%p, offset=%u", page, offset);

	/* Validate offset */
	if (offset == InvalidOffsetNumber || offset > PageGetMaxOffsetNumber(page))
		elog(ERROR, "RelUndoApplyInsert: invalid offset %u (max=%u)",
			 offset, PageGetMaxOffsetNumber(page));

	elog(DEBUG1, "RelUndoApplyInsert: calling PageGetItemId");
	lp = PageGetItemId(page, offset);

	elog(DEBUG1, "RelUndoApplyInsert: got ItemId %p", lp);

	if (!ItemIdIsNormal(lp))
		elog(WARNING, "RelUndoApplyInsert: tuple at offset %u is not normal", offset);

	/* Mark the line pointer as unused (LP_UNUSED) */
	elog(DEBUG1, "RelUndoApplyInsert: calling ItemIdSetUnused");
	ItemIdSetUnused(lp);

	elog(DEBUG1, "RelUndoApplyInsert: marked tuple at offset %u as unused", offset);
}

/*
 * RelUndoApplyDelete - Undo a DELETE operation
 *
 * Restore the deleted tuple from the UNDO record. The tuple data is stored
 * in the UNDO record and includes the full tuple (header + data).
 */
static void
RelUndoApplyDelete(Relation rel, Page page, OffsetNumber offset,
				   char *tuple_data, uint32 tuple_len)
{
	ItemId		lp;
	Size		aligned_len;

	/* Validate inputs */
	if (tuple_data == NULL || tuple_len == 0)
		elog(ERROR, "RelUndoApplyDelete: invalid tuple data");

	if (offset == InvalidOffsetNumber || offset > PageGetMaxOffsetNumber(page))
		elog(ERROR, "RelUndoApplyDelete: invalid offset %u", offset);

	lp = PageGetItemId(page, offset);

	aligned_len = MAXALIGN(tuple_len);

	/*
	 * Restore the tuple data. We use memcpy to copy the complete tuple
	 * including the header.
	 */
	if (ItemIdIsUsed(lp))
	{
		/*
		 * Tuple slot is still occupied -- the common case for an in-place AM
		 * whose DELETE is in place (it only flags the tuple deleted and leaves
		 * the full-length body on the page).  Restoring is an in-place
		 * overwrite of the same-length slot, so it needs no free space; do
		 * not consult PageGetFreeSpace here or a full page would spuriously
		 * fail the rollback.
		 */
		if (ItemIdGetLength(lp) != tuple_len)
			elog(ERROR, "RelUndoApplyDelete: tuple length mismatch");

		memcpy(PageGetItem(page, lp), tuple_data, tuple_len);
	}
	else
	{
		/* Need to allocate a new slot -- this path consumes free space. */
		OffsetNumber new_offset;

		if (PageGetFreeSpace(page) < aligned_len)
			elog(ERROR, "RelUndoApplyDelete: insufficient space on page to restore tuple");

		new_offset = PageAddItem(page, tuple_data, tuple_len,
								 offset, false, false);
		if (new_offset != offset)
			elog(ERROR, "RelUndoApplyDelete: could not restore tuple at expected offset");
	}

	/*
	 * Clear transient flags.  The restored tuple is the committed
	 * before-image of the DELETE, so it should not be marked deleted or
	 * uncommitted.
	 */
	if (RelUndoClearTransientFlags_hook)
	{
		lp = PageGetItemId(page, offset);
		RelUndoClearTransientFlags_hook((char *) PageGetItem(page, lp));
	}

	elog(DEBUG2, "RelUndoApplyDelete: restored tuple at offset %u (%u bytes)",
		 offset, tuple_len);
}

/*
 * RelUndoApplyUpdate - Undo an UPDATE operation
 *
 * Restore the old tuple version from the UNDO record.  The tuple data was
 * stored in the UNDO record and includes the full tuple (header + data).
 *
 * For an in-place update, the old tuple was physically overwritten at
 * the same offset.  We restore it by copying the saved data back.
 */
static void
RelUndoApplyUpdate(Relation rel, Page page, OffsetNumber offset,
				   char *tuple_data, uint32 tuple_len)
{
	ItemId		lp;

	/* Validate inputs */
	if (tuple_data == NULL || tuple_len == 0)
		elog(ERROR, "RelUndoApplyUpdate: invalid tuple data");

	if (offset == InvalidOffsetNumber || offset > PageGetMaxOffsetNumber(page))
		elog(ERROR, "RelUndoApplyUpdate: invalid offset %u", offset);

	lp = PageGetItemId(page, offset);

	if (!ItemIdIsNormal(lp))
		elog(ERROR, "RelUndoApplyUpdate: tuple at offset %u is not normal", offset);

	/*
	 * Restore the old tuple.  Handle size differences between the new tuple
	 * (currently on page) and the old tuple (from UNDO).
	 */
	if (tuple_len <= ItemIdGetLength(lp))
	{
		/*
		 * Old tuple is same size or smaller than the new one.  Simply
		 * overwrite in place and adjust the length.
		 */
		memcpy(PageGetItem(page, lp), tuple_data, tuple_len);
		if (tuple_len != ItemIdGetLength(lp))
			ItemIdSetNormal(lp, ItemIdGetOffset(lp), tuple_len);
	}
	else
	{
		/*
		 * Old tuple is larger than the new one.  Delete the current item
		 * and re-add the old tuple at the same offset.
		 */
		OffsetNumber restored_offset;

		PageIndexTupleDelete(page, offset);
		restored_offset = PageAddItem(page, tuple_data,
									  tuple_len, offset, false, false);
		if (restored_offset == InvalidOffsetNumber)
		{
			/*
			 * Try without specifying a target offset.  The page should have
			 * enough free space since we just removed the (smaller) new tuple.
			 */
			restored_offset = PageAddItem(page, tuple_data,
										  tuple_len, InvalidOffsetNumber,
										  false, false);
		}

		if (restored_offset == InvalidOffsetNumber)
			elog(ERROR, "RelUndoApplyUpdate: could not restore old tuple at offset %u (%u bytes)",
				 offset, tuple_len);
	}

	/*
	 * Clear transient flags on the restored tuple.  The UNDO record stores
	 * the before-image which was committed, so UNCOMMITTED should not be set.
	 * Clear it defensively in case lazy clearing hadn't run before the
	 * snapshot was taken, and also clear DELETED/UPDATED since the operation
	 * that set them is being rolled back.
	 */
	if (RelUndoClearTransientFlags_hook)
	{
		lp = PageGetItemId(page, offset);
		RelUndoClearTransientFlags_hook((char *) PageGetItem(page, lp));
	}

	elog(DEBUG2, "RelUndoApplyUpdate: restored old tuple at offset %u (%u bytes)",
		 offset, tuple_len);
}

/*
 * RelUndoApplyDiffReverse - reconstruct old tuple bytes from new bytes + diff.
 *
 * Generic (AM-neutral) reverse-apply of a byte-diff (RelUndoDiffRecord).  The
 * diff is a single splice against the new (current on-page) tuple: copy the
 * unchanged prefix of new_data, substitute the carried old bytes, then copy
 * the unchanged suffix, producing an old_total_len-byte old tuple.  Bounds are
 * validated against new_len and old_total_len so a corrupt record fails
 * cleanly rather than scribbling.  Returns true on success.
 *
 * FLUX's PVS read path mirrors this exact algorithm (FluxApplyDiffReverse) so
 * a single delta record serves both rollback and old-version reconstruction.
 */
bool
RelUndoApplyDiffReverse(const char *new_data, Size new_len,
						const RelUndoDiffRecord *diff,
						char *out_old_data, Size *out_old_len)
{
	Size		old_total_len;
	Size		tail;

	if (diff == NULL || new_data == NULL || out_old_data == NULL)
		return false;

	old_total_len = diff->old_total_len;

	/* The spliced region must lie within the new tuple. */
	if ((Size) diff->offset > new_len ||
		(Size) diff->offset + diff->del_len > new_len)
		return false;

	tail = new_len - diff->offset - diff->del_len;

	/* Reconstructed length must be prefix + ins + suffix. */
	if ((Size) diff->offset + diff->ins_len + tail != old_total_len)
		return false;

	/* prefix (unchanged new bytes) */
	memcpy(out_old_data, new_data, diff->offset);
	/* changed region: substitute the carried old bytes */
	memcpy(out_old_data + diff->offset,
		   (const char *) diff + SizeOfRelUndoDiffRecord,
		   diff->ins_len);
	/* suffix (unchanged new bytes) */
	memcpy(out_old_data + diff->offset + diff->ins_len,
		   new_data + diff->offset + diff->del_len,
		   tail);

	*out_old_len = old_total_len;
	return true;
}

/*
 * RelUndoApplyTupleLock - Undo a tuple lock operation
 *
 * Remove the lock marker from the tuple by clearing the lock-related
 * infomask bits and resetting xmax to InvalidTransactionId.
 */
static void
RelUndoApplyTupleLock(Relation rel, Page page, OffsetNumber offset)
{
	ItemId		lp;
	HeapTupleHeader htup;

	/* Validate offset */
	if (offset == InvalidOffsetNumber || offset > PageGetMaxOffsetNumber(page))
		elog(ERROR, "RelUndoApplyTupleLock: invalid offset %u", offset);

	lp = PageGetItemId(page, offset);

	if (!ItemIdIsNormal(lp))
		elog(ERROR, "RelUndoApplyTupleLock: tuple at offset %u is not normal", offset);

	htup = (HeapTupleHeader) PageGetItem(page, lp);

	/* Clear lock-related infomask bits */
	htup->t_infomask &= ~(HEAP_XMAX_LOCK_ONLY |
						  HEAP_XMAX_KEYSHR_LOCK |
						  HEAP_XMAX_SHR_LOCK |
						  HEAP_XMAX_EXCL_LOCK);
	htup->t_infomask2 &= ~HEAP_KEYS_UPDATED;

	/* Reset xmax to invalid */
	HeapTupleHeaderSetXmax(htup, InvalidTransactionId);

	elog(DEBUG2, "RelUndoApplyTupleLock: cleared lock from tuple at offset %u", offset);
}

/*
 * RelUndoLogApplyCLR - Write a redoable Compensation Log Record for a batch
 *
 * Logs a single XLOG_RELUNDO_APPLY record covering one or more consecutive
 * UNDO records (urec_ptrs[0..nptrs-1]) that were all applied to the same set of
 * data pages (passed in touched[], already mutated and held exclusively
 * locked).  Each record's RELUNDO_INFO_CLR_APPLIED flag is set in place on its
 * UNDO-fork page; the distinct fork pages are logged alongside the data pages.
 *
 * Because an in-place MVCC AM has no durable xmin, the rollback's
 * physical page changes are NOT reconstructable from any forward WAL record;
 * the forward record holds the *new* (aborted) value.  We therefore force a
 * full-page image of each restored page so crash redo reinstates the
 * before-image.  The CLR_APPLIED flag on each fork page makes a re-driven
 * RelUndoApplyChain idempotent (it skips already-applied records), preventing
 * double-application after a crash during rollback.
 *
 * relundo_redo_apply restores every registered block image and ignores the
 * record body, so a batched CLR replays identically to a sequence of
 * single-record CLRs -- only the WAL volume (one forced fork-page and data-page
 * image instead of N) and the flush count (one instead of N) shrink.
 *
 * All data buffers in touched[] are released here after logging.
 */
static void
RelUndoLogApplyCLR(Relation rel, const RelUndoRecPtr *urec_ptrs, int nptrs,
				   Buffer *touched, int ntouched)
{
	xl_relundo_apply xlrec;
	BlockNumber fork_blks[RELUNDO_APPLY_MAX_FORK_BUFS];
	Buffer		fork_bufs[RELUNDO_APPLY_MAX_FORK_BUFS];
	int			nfork = 0;
	XLogRecPtr	recptr;
	uint8		block_id;
	int			i;
	int			j;

	Assert(nptrs >= 1);
	Assert(ntouched >= 1);

	/*
	 * Collect the distinct fork pages this batch touches, in ascending block
	 * order so concurrent rollback appliers acquire fork-page locks in a
	 * consistent order (deadlock-free).  Data pages were already locked by the
	 * apply path before any fork page, so the global order is data-then-fork.
	 */
	for (i = 0; i < nptrs; i++)
	{
		BlockNumber blk = RelUndoGetBlockNum(urec_ptrs[i]);
		int			pos;

		for (pos = 0; pos < nfork; pos++)
		{
			if (fork_blks[pos] == blk)
				break;
		}
		if (pos < nfork)
			continue;			/* already collected */

		Assert(nfork < RELUNDO_APPLY_MAX_FORK_BUFS);
		/* insertion sort into ascending order */
		for (pos = nfork; pos > 0 && fork_blks[pos - 1] > blk; pos--)
			fork_blks[pos] = fork_blks[pos - 1];
		fork_blks[pos] = blk;
		nfork++;
	}

	xlrec.urec_ptr = urec_ptrs[0];
	xlrec.target_reloc = rel->rd_locator;

	/*
	 * Read and exclusive-lock each distinct fork page (ascending), before the
	 * critical section since ReadBuffer may perform I/O.
	 */
	for (i = 0; i < nfork; i++)
	{
		fork_bufs[i] = ReadBufferExtended(rel, RELUNDO_FORKNUM, fork_blks[i],
										  RBM_NORMAL, NULL);
		LockBuffer(fork_bufs[i], BUFFER_LOCK_EXCLUSIVE);
	}

	START_CRIT_SECTION();

	/*
	 * Set the CLR flags in place on every batched record's fork-page header.
	 */
	for (i = 0; i < nptrs; i++)
	{
		BlockNumber blk = RelUndoGetBlockNum(urec_ptrs[i]);
		uint16		off = RelUndoGetOffset(urec_ptrs[i]);
		char	   *contents;
		RelUndoRecordHeader *rec_hdr;

		for (j = 0; j < nfork; j++)
		{
			if (fork_blks[j] == blk)
				break;
		}
		Assert(j < nfork);

		contents = PageGetContents(BufferGetPage(fork_bufs[j]));
		rec_hdr = (RelUndoRecordHeader *) (contents + off);
		rec_hdr->info_flags |= (RELUNDO_INFO_HAS_CLR | RELUNDO_INFO_CLR_APPLIED);
	}

	for (i = 0; i < ntouched; i++)
		MarkBufferDirty(touched[i]);
	for (i = 0; i < nfork; i++)
		MarkBufferDirty(fork_bufs[i]);

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_relundo_apply));

	/*
	 * Force a full-page image of every restored data page then every fork
	 * page.  The restoration is an arbitrary in-place rewrite, so only an FPI
	 * can reproduce it during redo.
	 */
	block_id = 0;
	for (i = 0; i < ntouched; i++)
		XLogRegisterBuffer(block_id++, touched[i],
						   REGBUF_STANDARD | REGBUF_FORCE_IMAGE);
	for (i = 0; i < nfork; i++)
		XLogRegisterBuffer(block_id++, fork_bufs[i],
						   REGBUF_STANDARD | REGBUF_FORCE_IMAGE);

	recptr = XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_APPLY);

	for (i = 0; i < ntouched; i++)
		PageSetLSN(BufferGetPage(touched[i]), recptr);
	for (i = 0; i < nfork; i++)
		PageSetLSN(BufferGetPage(fork_bufs[i]), recptr);

	END_CRIT_SECTION();

	/*
	 * Force the CLR to durable storage before releasing the buffers.  Unlike
	 * heap, an in-place MVCC AM has no durable xmin/clog, so recovery cannot
	 * re-drive the per-relation fork undo for a loser transaction: the
	 * before-image we just restored exists only in these (still dirty) buffers and in this CLR.  If
	 * we crash before the CLR is flushed, redo replays only the forward
	 * (aborted) page change and the rollback is silently lost.  Flushing here
	 * makes the compensation durable so crash redo reinstates the before-image
	 * from the forced full-page images.  Batching amortizes this flush across
	 * every record folded into the CLR.
	 */
	XLogFlush(recptr);

	elog(DEBUG3, "RelUndoLogApplyCLR: CLR for %d UNDO record(s), %d data page(s), %d fork page(s)",
		 nptrs, ntouched, nfork);

	for (i = 0; i < nfork; i++)
		UnlockReleaseBuffer(fork_bufs[i]);
	for (i = 0; i < ntouched; i++)
		UnlockReleaseBuffer(touched[i]);
}

/*
 * RelUndoReadRecordWithTuple - Read UNDO record including tuple data
 *
 * This is like RelUndoReadRecord but also reads the tuple data that follows
 * the payload if RELUNDO_INFO_HAS_TUPLE is set.
 */
RelUndoRecordHeader *
RelUndoReadRecordWithTuple(Relation rel, RelUndoRecPtr ptr,
						   char **tuple_data_out, uint32 *tuple_len_out)
{
	RelUndoRecordHeader header_local;
	RelUndoRecordHeader *header;
	void	   *payload;
	Size		payload_size;
	bool		success;

	/* Initialize outputs */
	*tuple_data_out = NULL;
	*tuple_len_out = 0;

	/* Read the basic record (header + payload, no tuple data) */
	success = RelUndoReadRecord(rel, ptr, &header_local, &payload, &payload_size);
	if (!success)
		return NULL;

	/*
	 * Allocate combined buffer for header + payload. Tuple data will be
	 * allocated separately if present.
	 */
	header = (RelUndoRecordHeader *) palloc(SizeOfRelUndoRecordHeader + payload_size);
	memcpy(header, &header_local, SizeOfRelUndoRecordHeader);
	memcpy((char *) header + SizeOfRelUndoRecordHeader, payload, payload_size);

	/* Free the payload allocated by RelUndoReadRecord */
	pfree(payload);

	/*
	 * If tuple data is present, extract it from the combined payload.
	 *
	 * RelUndoReadRecord reads (urec_len - SizeOfRelUndoRecordHeader) bytes as
	 * "payload", which includes both the actual payload and the tuple data.
	 * The tuple data occupies the last tuple_len bytes of that region.
	 */
	if ((header->info_flags & RELUNDO_INFO_HAS_TUPLE) && header->tuple_len > 0)
	{
		uint32		tlen = header->tuple_len;
		Size		actual_payload_size;

		/*
		 * payload_size from RelUndoReadRecord includes both the real payload
		 * and the tuple data. The actual payload is the first part.
		 */
		if (payload_size < tlen)
		{
			elog(WARNING, "RelUndoReadRecordWithTuple: tuple_len %u exceeds payload_size %zu",
				 tlen, payload_size);
			return header;
		}

		actual_payload_size = payload_size - tlen;

		/*
		 * Allocate and copy the tuple data from the tail of the combined
		 * buffer
		 */
		*tuple_data_out = (char *) palloc(tlen);
		memcpy(*tuple_data_out,
			   (char *) header + SizeOfRelUndoRecordHeader + actual_payload_size,
			   tlen);
		*tuple_len_out = tlen;

		elog(DEBUG2, "RelUndoReadRecordWithTuple: read %u bytes of tuple data", tlen);
	}

	return header;
}
