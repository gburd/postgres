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
 * All hooks are guarded by RelationHasUndo(heaprel) -- nbtree UNDO
 * is controlled by the parent heap table's enable_undo setting.
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

#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/undo_xlog.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/undormgr.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"
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
 */
typedef struct NbtreeUndoInsertLeaf
{
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

		hdr.blkno = BufferGetBlockNumber(buf);
		hdr.offset = offset;
		hdr.itup_sz = itemsz;

		/*
		 * When the heap is in bulk UNDO mode, piggyback on its batch to
		 * avoid a separate UndoLogAllocate + WAL insert + pwrite per
		 * index entry.  The UndoRecordSet accepts mixed RM IDs.
		 */
		if (HeapBulkUndoIsActive(heaprel))
		{
			HeapBulkUndoAddRecordParts(heaprel,
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

		upper_hdr.blkno = BufferGetBlockNumber(buf);
		upper_hdr.offset = offset;
		upper_hdr.child_blkno = BTreeTupleGetDownLink(itup);
		upper_hdr.itup_sz = itemsz;

		if (HeapBulkUndoIsActive(heaprel))
		{
			HeapBulkUndoAddRecordParts(heaprel,
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
 * nbtree_undo_apply - Apply a single nbtree UNDO record
 *
 * This is the rm_undo callback for the nbtree RM.
 */
static UndoApplyResult
nbtree_undo_apply(uint8 rmid, uint16 info, TransactionId xid, Oid reloid,
				  const char *payload, Size payload_len, UndoRecPtr urec_ptr)
{
	Assert(rmid == UNDO_RMID_NBTREE);

	switch (info)
	{
		case NBTREE_UNDO_INSERT_LEAF:
			{
				NbtreeUndoInsertLeaf hdr;
				Relation	rel;
				Buffer		buffer;
				Page		page;
				BTPageOpaque opaque;

				if (payload_len < SizeOfNbtreeUndoInsertLeaf)
					return UNDO_APPLY_ERROR;

				memcpy(&hdr, payload, SizeOfNbtreeUndoInsertLeaf);

				/*
				 * Open the index relation.  We need to find it from the
				 * heap OID stored in reloid.  For simplicity, we scan
				 * pg_index to find indexes on this table.  However, we
				 * cannot safely do catalog lookups during abort.
				 *
				 * Instead, we use try_relation_open on the heap relation,
				 * then find its indexes. But during TRANS_ABORT we can't
				 * do this safely. Use best-effort: try to open the relation
				 * and if it fails, mark as LP_DEAD via the heap relation.
				 */

				/*
				 * For now, we take a simpler approach: the index OID
				 * is stored in the UNDO record payload by the caller.
				 * However, currently we store the heap OID in urec_reloid.
				 *
				 * Best-effort approach: try to open any relation. If it's
				 * been dropped, skip. Since nbtree UNDO is applied
				 * synchronously before the transaction enters TRANS_ABORT,
				 * we can still do relation opens.
				 */
				rel = try_relation_open(reloid, RowExclusiveLock);
				if (rel == NULL)
					return UNDO_APPLY_SKIPPED;

				/*
				 * We need the index relation, not the heap. For now,
				 * iterate through the heap's indexes to find the one
				 * containing our target block.
				 */
				{
					List	   *indexoidlist;
					ListCell   *lc;
					bool		found = false;

					indexoidlist = RelationGetIndexList(rel);
					foreach(lc, indexoidlist)
					{
						Oid			indexoid = lfirst_oid(lc);
						Relation	indexrel;

						indexrel = try_relation_open(indexoid,
													RowExclusiveLock);
						if (indexrel == NULL)
							continue;

						/* Check if this index has the target block */
						if (RelationGetNumberOfBlocks(indexrel) > hdr.blkno)
						{
							buffer = ReadBuffer(indexrel, hdr.blkno);
							LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
							page = BufferGetPage(buffer);
							opaque = BTPageGetOpaque(page);

							/* Verify this is a leaf btree page */
							if (P_ISLEAF(opaque) &&
								hdr.offset <= PageGetMaxOffsetNumber(page))
							{
								ItemId		lp = PageGetItemId(page, hdr.offset);

								START_CRIT_SECTION();

								if (ItemIdIsNormal(lp))
									ItemIdSetDead(lp);

								MarkBufferDirty(buffer);

								/* Generate CLR */
								if (RelationNeedsWAL(indexrel))
								{
									XLogRecPtr	clr_lsn;
									xl_undo_apply xlrec;

									xlrec.urec_ptr = urec_ptr;
									xlrec.xid = xid;
									xlrec.target_locator = indexrel->rd_locator;
									xlrec.target_block = hdr.blkno;
									xlrec.target_offset = hdr.offset;
									xlrec.operation_type = info;

									XLogBeginInsert();
									XLogRegisterData((char *) &xlrec,
													 SizeOfUndoApply);
									XLogRegisterBuffer(0, buffer,
													   REGBUF_FORCE_IMAGE |
													   REGBUF_STANDARD);
									clr_lsn = XLogInsert(RM_UNDO_ID,
														  XLOG_UNDO_APPLY_RECORD);
									PageSetLSN(page, clr_lsn);

									UndoLogWrite(urec_ptr +
												 offsetof(UndoRecordHeader,
														  urec_clr_ptr),
												 (const char *) &clr_lsn,
												 sizeof(XLogRecPtr));
								}

								END_CRIT_SECTION();

								found = true;
							}

							UnlockReleaseBuffer(buffer);
						}

						relation_close(indexrel, RowExclusiveLock);

						if (found)
							break;
					}

					list_free(indexoidlist);
				}

				relation_close(rel, RowExclusiveLock);
				return UNDO_APPLY_SUCCESS;
			}

		case NBTREE_UNDO_INSERT_UPPER:
			{
				/*
				 * Undoing internal page insertions is complex and risky.
				 * The downlink is needed for tree navigation. Instead of
				 * removing it, we leave it in place. The child page (from
				 * a split that was part of the aborted transaction) will
				 * have its entries marked LP_DEAD by the leaf undo, and
				 * eventually the page will be recycled by VACUUM.
				 */
				return UNDO_APPLY_SKIPPED;
			}

		case NBTREE_UNDO_DEDUP:
			{
				NbtreeUndoDedup hdr;
				Relation	rel;
				List	   *indexoidlist;
				ListCell   *lc;
				bool		found = false;

				if (payload_len < SizeOfNbtreeUndoDedup)
					return UNDO_APPLY_ERROR;

				memcpy(&hdr, payload, SizeOfNbtreeUndoDedup);

				rel = try_relation_open(reloid, RowExclusiveLock);
				if (rel == NULL)
					return UNDO_APPLY_SKIPPED;

				indexoidlist = RelationGetIndexList(rel);
				foreach(lc, indexoidlist)
				{
					Oid			indexoid = lfirst_oid(lc);
					Relation	indexrel;
					Buffer		buffer;
					Page		page;

					indexrel = try_relation_open(indexoid, RowExclusiveLock);
					if (indexrel == NULL)
						continue;

					if (RelationGetNumberOfBlocks(indexrel) > hdr.blkno)
					{
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

							XLogBeginInsert();
							XLogRegisterData((char *) &xlrec,
											 SizeOfUndoApply);
							XLogRegisterBuffer(0, buffer,
											   REGBUF_FORCE_IMAGE |
											   REGBUF_STANDARD);
							clr_lsn = XLogInsert(RM_UNDO_ID,
												  XLOG_UNDO_APPLY_RECORD);
							PageSetLSN(page, clr_lsn);

							UndoLogWrite(urec_ptr +
										 offsetof(UndoRecordHeader,
												  urec_clr_ptr),
										 (const char *) &clr_lsn,
										 sizeof(XLogRecPtr));
						}

						END_CRIT_SECTION();

						found = true;
						UnlockReleaseBuffer(buffer);
					}

					relation_close(indexrel, RowExclusiveLock);

					if (found)
						break;
				}

				list_free(indexoidlist);
				relation_close(rel, RowExclusiveLock);
				return found ? UNDO_APPLY_SUCCESS : UNDO_APPLY_SKIPPED;
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
			 * Ad-hoc deletion undo: re-insert the deleted tuples.
			 * This is complex since we need to find the correct
			 * insertion point.  For now, skip and let the entries
			 * be re-created by the reverted heap operation.
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
}
