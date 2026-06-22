/*-------------------------------------------------------------------------
 *
 * nbtree_undo.c
 *	  nbtree UNDO resource manager
 *
 * This module implements UNDO apply callbacks for the B-tree index AM.
 * When a transaction aborts, provisionally inserted index entries are
 * removed (or marked LP_DEAD) so that VACUUM is not required to clean
 * up after aborted transactions.
 *
 * Combined with heap UNDO, nbtree UNDO provides a "zero-VACUUM"
 * experience for aborted transactions: both heap tuples and their
 * index entries are cleaned up immediately during rollback.
 *
 * UNDO Subtypes:
 *   INSERT_LEAF:   Undo a leaf-page index tuple insertion
 *   INSERT_UPPER:  Undo an internal-page downlink insertion
 *   INSERT_POST:   Undo a posting list split
 *   DEDUP:         Undo a deduplication pass (restore pre-dedup page)
 *   DELETE:        Undo an ad-hoc deletion (re-insert deleted tuples)
 *
 * Structural operations (SPLIT, NEWROOT) and VACUUM operations are
 * logged for completeness but their undo-apply is handled by falling
 * back to per-entry LP_DEAD marking rather than reversing the
 * structural change, since concurrent readers may have already
 * observed the new structure.
 *
 * All hooks are guarded by RelationAmSupportsUndo(heaprel) -- nbtree
 * UNDO is controlled by the parent table AM's am_supports_undo declaration.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtree_undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/undobuffer.h"
#include "access/xact.h"
#include "access/nbtree.h"
#include "access/undo_xlog.h"
#include "access/undorecord.h"
#include "access/undormgr.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"
#include "storage/itemptr.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/*
 * nbtree UNDO subtypes (stored in urec_info)
 *
 * These correspond to the WAL-logged nbtree operations.
 */
#define NBTREE_UNDO_INSERT_LEAF		0x0001	/* leaf tuple insertion */
#define NBTREE_UNDO_INSERT_UPPER	0x0002	/* internal downlink insertion */
#define NBTREE_UNDO_INSERT_POST		0x0004	/* posting list split */
#define NBTREE_UNDO_DELETE			0x0005	/* ad-hoc tuple deletion */
#define NBTREE_UNDO_SPLIT_L		0x0006	/* page split (new item on left) */
#define NBTREE_UNDO_SPLIT_R		0x0007	/* page split (new item on right) */
#define NBTREE_UNDO_NEWROOT		0x0008	/* new root creation */
#define NBTREE_UNDO_DEDUP			0x0009	/* deduplication pass */
#define NBTREE_UNDO_VACUUM			0x000A	/* vacuum deletion (no-op undo) */

/*
 * NbtreeUndoInsertLeaf - Payload for leaf insert undo
 *
 * index_oid allows direct index open during rollback, eliminating
 * the O(N_indexes) scan through RelationGetIndexList().
 */
typedef struct NbtreeUndoInsertLeaf
{
	Oid			index_oid;		/* OID of the index relation */
	BlockNumber blkno;			/* Page where tuple was inserted */
	OffsetNumber offset;		/* Offset of the inserted tuple */
	Size		itup_sz;		/* Size of the index tuple */
	/* Followed by the IndexTupleData */
}			NbtreeUndoInsertLeaf;

#define SizeOfNbtreeUndoInsertLeaf	offsetof(NbtreeUndoInsertLeaf, itup_sz) + sizeof(Size)

/*
 * NbtreeUndoInsertUpper - Payload for internal insert undo
 */
typedef struct NbtreeUndoInsertUpper
{
	Oid			index_oid;		/* OID of the index relation */
	BlockNumber blkno;			/* Internal page */
	OffsetNumber offset;		/* Offset of downlink */
	BlockNumber child_blkno;	/* Child page whose downlink was added */
	Size		itup_sz;		/* Size of the downlink tuple */
	/* Followed by the IndexTupleData */
}			NbtreeUndoInsertUpper;

#define SizeOfNbtreeUndoInsertUpper	offsetof(NbtreeUndoInsertUpper, itup_sz) + sizeof(Size)

/*
 * NbtreeUndoDedup - Payload for dedup undo (full pre-dedup page image)
 */
typedef struct NbtreeUndoDedup
{
	Oid			index_oid;		/* OID of the index relation */
	BlockNumber blkno;			/* Page that was deduplicated */
	uint16		page_len;		/* Length of saved page image */
	/* Followed by the full page image (pre-dedup) */
}			NbtreeUndoDedup;

#define SizeOfNbtreeUndoDedup	offsetof(NbtreeUndoDedup, page_len) + sizeof(uint16)

/*
 * NbtreeUndoDelete - Payload for ad-hoc delete undo
 */
typedef struct NbtreeUndoDelete
{
	Oid			index_oid;		/* OID of the index relation */
	BlockNumber blkno;			/* Page from which tuples were deleted */
	uint16		ndeleted;		/* Number of deleted tuples */
	/* Followed by array of (OffsetNumber, IndexTupleData) pairs */
}			NbtreeUndoDelete;

#define SizeOfNbtreeUndoDelete	offsetof(NbtreeUndoDelete, ndeleted) + sizeof(uint16)

/* Forward declarations */
static UndoApplyResult nbtree_undo_apply(uint8 rmid, uint16 info,
										 TransactionId xid, Oid reloid,
										 const char *payload, Size payload_len,
										 UndoRecPtr urec_ptr);
static void nbtree_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
							 const char *payload, Size payload_len);

/* The nbtree UNDO RM registration entry */
static const UndoRmgrData nbtree_undo_rmgr = {
	.rm_name = "nbtree",
	.rm_undo = nbtree_undo_apply,
	.rm_desc = nbtree_undo_desc,
};

/*
 * NbtreeUndoRmgrInit - Register the nbtree UNDO resource manager
 */
void
NbtreeUndoRmgrInit(void)
{
	RegisterUndoRmgr(UNDO_RMID_NBTREE, &nbtree_undo_rmgr);
}

/*
 * NbtreeUndoLogInsert - Write UNDO record for a leaf index tuple insertion
 *
 * Called from _bt_insertonpg() after the insertion has been WAL-logged.
 * This records enough information to remove the inserted entry on abort.
 */
void
NbtreeUndoLogInsert(Relation rel, Relation heaprel, Buffer buf,
					IndexTuple itup, Size itemsz, OffsetNumber offset,
					bool isleaf)
{
	TransactionId xid = GetCurrentTransactionId();

	if (isleaf)
	{
		NbtreeUndoInsertLeaf hdr;

		hdr.index_oid = RelationGetRelid(rel);
		hdr.blkno = BufferGetBlockNumber(buf);
		hdr.offset = offset;
		hdr.itup_sz = itemsz;

		/*
		 * When the heap has an active UNDO write buffer, piggyback on it to
		 * avoid a separate UndoLogAllocate + WAL insert + pwrite per index
		 * entry.  The UndoRecordSet accepts mixed RM IDs.
		 */
		if (UndoBufferIsActive(heaprel))
		{
			UndoBufferAddRecordParts(heaprel,
									 UNDO_RMID_NBTREE,
									 NBTREE_UNDO_INSERT_LEAF,
									 (const char *) &hdr,
									 SizeOfNbtreeUndoInsertLeaf,
									 (const char *) itup,
									 itemsz);
		}
		else
		{
			UndoRecordSet *uset;

			uset = UndoRecordSetCreate(xid, GetCurrentTransactionUndoRecPtr());
			UndoRecordAddPayloadParts(uset,
									  UNDO_RMID_NBTREE,
									  NBTREE_UNDO_INSERT_LEAF,
									  RelationGetRelid(heaprel),
									  (const char *) &hdr,
									  SizeOfNbtreeUndoInsertLeaf,
									  (const char *) itup,
									  itemsz);
			UndoRecordSetInsert(uset);
			UndoRecordSetFree(uset);
		}
	}
	else
	{
		NbtreeUndoInsertUpper upper_hdr;

		upper_hdr.index_oid = RelationGetRelid(rel);
		upper_hdr.blkno = BufferGetBlockNumber(buf);
		upper_hdr.offset = offset;
		upper_hdr.child_blkno = BTreeTupleGetDownLink(itup);
		upper_hdr.itup_sz = itemsz;

		if (UndoBufferIsActive(heaprel))
		{
			UndoBufferAddRecordParts(heaprel,
									 UNDO_RMID_NBTREE,
									 NBTREE_UNDO_INSERT_UPPER,
									 (const char *) &upper_hdr,
									 SizeOfNbtreeUndoInsertUpper,
									 (const char *) itup,
									 itemsz);
		}
		else
		{
			UndoRecordSet *uset;

			uset = UndoRecordSetCreate(xid, GetCurrentTransactionUndoRecPtr());
			UndoRecordAddPayloadParts(uset,
									  UNDO_RMID_NBTREE,
									  NBTREE_UNDO_INSERT_UPPER,
									  RelationGetRelid(heaprel),
									  (const char *) &upper_hdr,
									  SizeOfNbtreeUndoInsertUpper,
									  (const char *) itup,
									  itemsz);
			UndoRecordSetInsert(uset);
			UndoRecordSetFree(uset);
		}
	}
}

/*
 * NbtreeUndoLogDedup - Write UNDO record before deduplication
 *
 * Called from _bt_dedup_pass() before the page is modified.
 * Saves a full page image so dedup can be reversed on abort.
 */
void
NbtreeUndoLogDedup(Relation rel, Relation heaprel, Buffer buf)
{
	NbtreeUndoDedup hdr;
	Page		page = BufferGetPage(buf);
	Size		page_size = PageGetPageSize(page);
	Size		payload_size;
	char	   *payload;
	UndoRecordSet *uset;
	TransactionId xid = GetCurrentTransactionId();

	payload_size = SizeOfNbtreeUndoDedup + page_size;
	payload = (char *) palloc(payload_size);

	hdr.index_oid = RelationGetRelid(rel);
	hdr.blkno = BufferGetBlockNumber(buf);
	hdr.page_len = (uint16) page_size;
	memcpy(payload, &hdr, SizeOfNbtreeUndoDedup);
	memcpy(payload + SizeOfNbtreeUndoDedup, page, page_size);

	uset = UndoRecordSetCreate(xid, GetCurrentTransactionUndoRecPtr());
	UndoRecordAddPayload(uset, UNDO_RMID_NBTREE, NBTREE_UNDO_DEDUP,
						 RelationGetRelid(heaprel), payload, payload_size);
	UndoRecordSetInsert(uset);
	UndoRecordSetFree(uset);
	pfree(payload);
}

/*
 * nbtree_undo_find_leaf_entry - Locate the live index entry to mark LP_DEAD
 *
 * The aborting transaction recorded the (blkno, offset) where it inserted
 * stored_itup, but concurrent inserts and leaf splits can shift entries: the
 * tuple now at the stored offset may be a *committed* entry belonging to some
 * other transaction.  Marking it LP_DEAD would silently drop committed rows
 * from the index.  We therefore identify the target entry by its heap TID
 * rather than by physical position.
 *
 * want_tid is the heap TID of the inserted entry (BTreeTupleGetHeapTID of
 * stored_itup).  On a match we return the leaf buffer (locked
 * BUFFER_LOCK_EXCLUSIVE, pinned) and set *out_offset to the matching offset;
 * the caller marks it dead and emits the CLR against that buffer/offset.
 *
 * If no live entry with want_tid is found, InvalidBuffer is returned: the
 * entry was already cleaned (e.g. by VACUUM or an earlier undo) and there is
 * nothing to do.  A posting-list tuple whose range covers want_tid is left
 * for VACUUM, since splitting a posting list during undo is out of scope.
 */
static Buffer
nbtree_undo_find_leaf_entry(Relation indexrel, BlockNumber stored_blkno,
							OffsetNumber stored_offset, IndexTuple stored_itup,
							ItemPointer want_tid, OffsetNumber *out_offset)
{
	Buffer		buffer;
	Page		page;
	BTPageOpaque opaque;
	BTScanInsert scankey;
	int32		cmpval;

	/*
	 * Fast path: the entry is still at the stored location.  This is the
	 * common case (no concurrent split/insert disturbed the offset) and lets
	 * us avoid a full tree descent.  Guard the stored blkno against the
	 * current relation size; the slow path re-descends and does not depend on
	 * it.
	 */
	if (RelationGetNumberOfBlocks(indexrel) > stored_blkno)
	{
		buffer = ReadBuffer(indexrel, stored_blkno);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);
		opaque = BTPageGetOpaque(page);

		if (P_ISLEAF(opaque) && stored_offset >= P_FIRSTDATAKEY(opaque) &&
			stored_offset <= PageGetMaxOffsetNumber(page))
		{
			ItemId		lp = PageGetItemId(page, stored_offset);

			if (ItemIdIsNormal(lp))
			{
				IndexTuple	onpage = (IndexTuple) PageGetItem(page, lp);
				ItemPointer onpage_tid = BTreeTupleGetHeapTID(onpage);

				if (onpage_tid != NULL &&
					!BTreeTupleIsPosting(onpage) &&
					ItemPointerEquals(onpage_tid, want_tid))
				{
					*out_offset = stored_offset;
					return buffer;
				}
			}
		}

		UnlockReleaseBuffer(buffer);
	}

	/*
	 * Slow path: the entry moved.  Re-descend the tree to the leaf page that
	 * should now hold the key, then walk right across split-out siblings while
	 * the key still belongs on the page, matching by heap TID.
	 *
	 * _bt_search needs heaprel only when access == BT_WRITE (to allocate a new
	 * root for an empty index).  We descend read-locked and upgrade the leaf
	 * lock to exclusive ourselves, so we pass heaprel == NULL with BT_READ.
	 * The upgrade and the right-walk are ordered left-to-right, matching
	 * nbtree's standard latch order, so they cannot deadlock against other
	 * descents.
	 */
	scankey = _bt_mkscankey(indexrel, stored_itup);
	(void) _bt_search(indexrel, NULL, scankey, &buffer, BT_READ, false);
	if (!BufferIsValid(buffer))
	{
		pfree(scankey);
		return InvalidBuffer;
	}

	/* Upgrade the landed leaf to an exclusive lock for in-place modification. */
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * _bt_compare against the high key tells us whether the key could live on
	 * a page to the right.  cmpval == 1 means "move right when scankey > high
	 * key"; this mirrors _bt_moveright's non-nextkey behavior.  We rescan each
	 * page in full because a concurrent insert or split may have shifted the
	 * entry since the descent.
	 */
	cmpval = 1;

	for (;;)
	{
		OffsetNumber off;
		OffsetNumber maxoff;

		page = BufferGetPage(buffer);
		opaque = BTPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		for (off = P_FIRSTDATAKEY(opaque); off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId		lp = PageGetItemId(page, off);
			IndexTuple	onpage;
			ItemPointer onpage_tid;

			if (!ItemIdIsNormal(lp))
				continue;

			onpage = (IndexTuple) PageGetItem(page, lp);

			/*
			 * A posting-list tuple covers a range of heap TIDs.  If want_tid
			 * falls in that range the entry was deduplicated after insertion;
			 * leave it for VACUUM rather than splitting the posting list here.
			 */
			if (BTreeTupleIsPosting(onpage))
				continue;

			onpage_tid = BTreeTupleGetHeapTID(onpage);
			if (onpage_tid != NULL && ItemPointerEquals(onpage_tid, want_tid))
			{
				*out_offset = off;
				pfree(scankey);
				return buffer;
			}
		}

		/*
		 * Not on this page.  Stop if this is the rightmost page or if the key
		 * is strictly less than this page's high key (so it cannot be further
		 * right).  Otherwise follow the right link, holding exclusive locks
		 * left-to-right.
		 */
		if (P_RIGHTMOST(opaque) ||
			_bt_compare(indexrel, scankey, page, P_HIKEY) < cmpval)
			break;

		buffer = _bt_relandgetbuf(indexrel, buffer, opaque->btpo_next, BT_WRITE);
	}

	_bt_relbuf(indexrel, buffer);
	pfree(scankey);
	return InvalidBuffer;
}

/*
 * nbtree_undo_apply - Apply a single nbtree UNDO record
 *
 * This is the rm_undo callback for the nbtree RM.
 */
static UndoApplyResult
nbtree_undo_apply(uint8 rmid, uint16 info, TransactionId xid, Oid reloid,
				  const char *payload, Size payload_len, UndoRecPtr urec_ptr)
{
	Assert(rmid == UNDO_RMID_NBTREE);

	/*
	 * During crash recovery, syscache may not be initialized yet when
	 * PerformUndoRecovery() runs.  try_relation_open() requires syscache to
	 * check if the relation exists, so we must defer UNDO application until
	 * after the system is fully initialized.
	 *
	 * Check if we're in recovery mode (InRecovery flag is still set). During
	 * crash recovery, UNDO phase runs before syscache is initialized, so we
	 * skip UNDO application and rely on the logical revert worker to handle
	 * it asynchronously after startup completes.
	 *
	 * This transaction will be tracked in the ATM (Aborted Transaction Map)
	 * so the background worker can pick it up later.
	 *
	 * Note: InRecovery is only true during startup/recovery; it's false
	 * during normal operation and during normal transaction abort, so this
	 * check only affects crash recovery.
	 */
	if (InRecovery)
	{
		ereport(DEBUG2,
				(errmsg("nbtree UNDO: deferring transaction %u to logical revert worker "
						"(in crash recovery, syscache not available)",
						xid)));
		return UNDO_APPLY_SKIPPED;
	}

	switch (info)
	{
		case NBTREE_UNDO_INSERT_LEAF:
			{
				NbtreeUndoInsertLeaf hdr;
				Relation	indexrel;
				Buffer		buffer;
				Page		page;
				IndexTuple	stored_itup;
				ItemPointer want_tid;
				OffsetNumber kill_offset;

				if (payload_len < SizeOfNbtreeUndoInsertLeaf)
					return UNDO_APPLY_ERROR;

				memcpy(&hdr, payload, SizeOfNbtreeUndoInsertLeaf);

				/* The original IndexTuple follows the fixed header. */
				if (payload_len < SizeOfNbtreeUndoInsertLeaf + hdr.itup_sz)
					return UNDO_APPLY_ERROR;

				stored_itup = (IndexTuple) (payload + SizeOfNbtreeUndoInsertLeaf);

				/*
				 * Open the index directly using the OID stored in the UNDO
				 * payload.  This avoids the O(N_indexes) scan through
				 * RelationGetIndexList().
				 */
				indexrel = try_relation_open(hdr.index_oid, RowExclusiveLock);
				if (indexrel == NULL)
				{
					ereport(DEBUG2,
							(errmsg("nbtree UNDO INSERT_LEAF: index %u no longer exists",
									hdr.index_oid)));
					return UNDO_APPLY_SKIPPED;
				}

				/*
				 * Identify the entry to remove by its heap TID, not by the
				 * stored physical offset: concurrent inserts and leaf splits
				 * can shift a committed entry onto that offset, and marking it
				 * LP_DEAD would silently drop committed rows.  A pivot tuple
				 * (no heap TID) cannot be the inserted leaf entry, so nothing
				 * to undo in that case.
				 */
				want_tid = BTreeTupleGetHeapTID(stored_itup);
				if (want_tid == NULL)
				{
					relation_close(indexrel, RowExclusiveLock);
					return UNDO_APPLY_SKIPPED;
				}

				buffer = nbtree_undo_find_leaf_entry(indexrel, hdr.blkno,
													 hdr.offset, stored_itup,
													 want_tid, &kill_offset);
				if (!BufferIsValid(buffer))
				{
					/* Entry already cleaned (or left to VACUUM): no change. */
					relation_close(indexrel, RowExclusiveLock);
					return UNDO_APPLY_SUCCESS;
				}

				page = BufferGetPage(buffer);

				START_CRIT_SECTION();

				ItemIdMarkDead(PageGetItemId(page, kill_offset));
				MarkBufferDirty(buffer);

				/*
				 * Emit a physiological CLR against the buffer we actually
				 * modified.  The redo handler re-applies ItemIdMarkDead at
				 * target_offset on block ref 0, so both must name the real
				 * killed location -- not the stale stored (blkno, offset).
				 */
				if (RelationNeedsWAL(indexrel))
				{
					XLogRecPtr	clr_lsn;
					xl_undo_apply xlrec;

					xlrec.urec_ptr = urec_ptr;
					xlrec.xid = xid;
					xlrec.target_locator = indexrel->rd_locator;
					xlrec.target_block = BufferGetBlockNumber(buffer);
					xlrec.target_offset = kill_offset;
					xlrec.operation_type = info;
					xlrec.clr_flags = UNDO_CLR_LP_DEAD;
					xlrec.tuple_len = 0;

					XLogBeginInsert();
					XLogRegisterData((char *) &xlrec,
									 SizeOfUndoApply);
					XLogRegisterBuffer(0, buffer,
									   REGBUF_STANDARD);
					clr_lsn = XLogInsert(RM_UNDO_ID,
										 XLOG_UNDO_APPLY_RECORD);
					PageSetLSN(page, clr_lsn);
				}

				END_CRIT_SECTION();

				UnlockReleaseBuffer(buffer);
				relation_close(indexrel, RowExclusiveLock);
				return UNDO_APPLY_SUCCESS;
			}

		case NBTREE_UNDO_INSERT_UPPER:
			{
				/*
				 * Undoing internal page insertions is complex and risky. The
				 * downlink is needed for tree navigation. Instead of removing
				 * it, we leave it in place. The child page (from a split that
				 * was part of the aborted transaction) will have its entries
				 * marked LP_DEAD by the leaf undo, and eventually the page
				 * will be recycled by VACUUM.
				 */
				return UNDO_APPLY_SKIPPED;
			}

		case NBTREE_UNDO_DEDUP:
			{
				NbtreeUndoDedup hdr;
				Relation	indexrel;
				Buffer		buffer;
				Page		page;

				if (payload_len < SizeOfNbtreeUndoDedup)
					return UNDO_APPLY_ERROR;

				memcpy(&hdr, payload, SizeOfNbtreeUndoDedup);

				/*
				 * Open the index directly using the OID stored in the UNDO
				 * payload.
				 */
				indexrel = try_relation_open(hdr.index_oid, RowExclusiveLock);
				if (indexrel == NULL)
				{
					ereport(DEBUG2,
							(errmsg("nbtree UNDO DEDUP: index %u no longer exists",
									hdr.index_oid)));
					return UNDO_APPLY_SKIPPED;
				}

				if (RelationGetNumberOfBlocks(indexrel) <= hdr.blkno)
				{
					ereport(DEBUG2,
							(errmsg("nbtree UNDO DEDUP: block %u beyond end of index %u",
									hdr.blkno, hdr.index_oid)));
					relation_close(indexrel, RowExclusiveLock);
					return UNDO_APPLY_SKIPPED;
				}

				buffer = ReadBuffer(indexrel, hdr.blkno);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				page = BufferGetPage(buffer);

				START_CRIT_SECTION();

				/* Restore the full pre-dedup page image */
				memcpy(page,
					   payload + SizeOfNbtreeUndoDedup,
					   hdr.page_len);

				MarkBufferDirty(buffer);

				if (RelationNeedsWAL(indexrel))
				{
					XLogRecPtr	clr_lsn;
					xl_undo_apply xlrec;

					xlrec.urec_ptr = urec_ptr;
					xlrec.xid = xid;
					xlrec.target_locator = indexrel->rd_locator;
					xlrec.target_block = hdr.blkno;
					xlrec.target_offset = 0;
					xlrec.operation_type = info;
					xlrec.clr_flags = UNDO_CLR_FULL_PAGE;
					xlrec.tuple_len = 0;

					XLogBeginInsert();
					XLogRegisterData((char *) &xlrec,
									 SizeOfUndoApply);
					XLogRegisterBuffer(0, buffer,
									   REGBUF_FORCE_IMAGE |
									   REGBUF_STANDARD);
					clr_lsn = XLogInsert(RM_UNDO_ID,
										 XLOG_UNDO_APPLY_RECORD);
					PageSetLSN(page, clr_lsn);
				}

				END_CRIT_SECTION();

				UnlockReleaseBuffer(buffer);
				relation_close(indexrel, RowExclusiveLock);
				return UNDO_APPLY_SUCCESS;
			}

		case NBTREE_UNDO_INSERT_POST:
		case NBTREE_UNDO_SPLIT_L:
		case NBTREE_UNDO_SPLIT_R:
		case NBTREE_UNDO_NEWROOT:

			/*
			 * Structural operations: attempting to reverse a split is too
			 * dangerous due to concurrent readers.  The individual leaf
			 * entries from the aborted transaction will be cleaned up by
			 * their own INSERT_LEAF undo records.  Structural artifacts
			 * (empty pages from splits) will be recycled by VACUUM.
			 */
			return UNDO_APPLY_SKIPPED;

		case NBTREE_UNDO_DELETE:

			/*
			 * Ad-hoc deletion undo: re-insert the deleted tuples. This is
			 * complex since we need to find the correct insertion point.  For
			 * now, skip and let the entries be re-created by the reverted
			 * heap operation.
			 */
			return UNDO_APPLY_SKIPPED;

		case NBTREE_UNDO_VACUUM:
			/* VACUUM runs in its own transaction -- undo is always no-op */
			return UNDO_APPLY_SKIPPED;

		default:
			ereport(WARNING,
					(errmsg("nbtree UNDO: unknown subtype %u", info)));
			return UNDO_APPLY_ERROR;
	}
}

/*
 * nbtree_undo_desc - Describe an nbtree UNDO record for debugging
 */
static void
nbtree_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
				 const char *payload, Size payload_len)
{
	const char *opname;

	switch (info)
	{
		case NBTREE_UNDO_INSERT_LEAF:
			opname = "INSERT_LEAF";
			break;
		case NBTREE_UNDO_INSERT_UPPER:
			opname = "INSERT_UPPER";
			break;
		case NBTREE_UNDO_INSERT_POST:
			opname = "INSERT_POST";
			break;
		case NBTREE_UNDO_DELETE:
			opname = "DELETE";
			break;
		case NBTREE_UNDO_SPLIT_L:
			opname = "SPLIT_L";
			break;
		case NBTREE_UNDO_SPLIT_R:
			opname = "SPLIT_R";
			break;
		case NBTREE_UNDO_NEWROOT:
			opname = "NEWROOT";
			break;
		case NBTREE_UNDO_DEDUP:
			opname = "DEDUP";
			break;
		case NBTREE_UNDO_VACUUM:
			opname = "VACUUM";
			break;
		default:
			opname = "UNKNOWN";
			break;
	}

	appendStringInfo(buf, "nbtree %s", opname);

	/* For types that have index_oid at the start of the payload, show it */
	if (payload_len >= sizeof(Oid) &&
		(info == NBTREE_UNDO_INSERT_LEAF ||
		 info == NBTREE_UNDO_INSERT_UPPER ||
		 info == NBTREE_UNDO_DEDUP ||
		 info == NBTREE_UNDO_DELETE))
	{
		Oid			index_oid;

		memcpy(&index_oid, payload, sizeof(Oid));
		appendStringInfo(buf, " index %u", index_oid);
	}
}
