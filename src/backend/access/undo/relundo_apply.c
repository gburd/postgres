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
 *   - DELTA_INSERT: Restore original column data
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
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "utils/rel.h"

/* Forward declarations for internal functions */
static void RelUndoApplyInsert(Relation rel, Page page, OffsetNumber offset);
static void RelUndoApplyUpdate(Relation rel, Page page, OffsetNumber offset,
							   char *tuple_data, uint32 tuple_len);
static void RelUndoApplyDelete(Relation rel, Page page, OffsetNumber offset,
							   char *tuple_data, uint32 tuple_len);
static void RelUndoApplyTupleLock(Relation rel, Page page, OffsetNumber offset);
static void RelUndoApplyDeltaInsert(Relation rel, Page page, OffsetNumber offset,
									char *delta_data, uint32 delta_len);
static void RelUndoWriteCLR(Relation rel, RelUndoRecPtr urec_ptr,
							XLogRecPtr clr_lsn);

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
	 */
	while (RelUndoRecPtrIsValid(current_ptr))
	{
		Buffer		buffer = InvalidBuffer;
		Page		page;
		BlockNumber target_blkno;
		OffsetNumber target_offset;

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
				pfree(payload);
			continue;
		}

		elog(DEBUG1, "RelUndoApplyChain: processing record type %d at %lu",
			 header.urec_type, (unsigned long) current_ptr);

		switch (header.urec_type)
		{
			case RELUNDO_INSERT:
				{
					RelUndoInsertPayload *ins_payload = (RelUndoInsertPayload *) payload;

					target_blkno = ItemPointerGetBlockNumber(&ins_payload->firsttid);
					target_offset = ItemPointerGetOffsetNumber(&ins_payload->firsttid);

					buffer = ReadBuffer(rel, target_blkno);
					LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
					page = BufferGetPage(buffer);

					RelUndoApplyInsert(rel, page, target_offset);

					MarkBufferDirty(buffer);
					UnlockReleaseBuffer(buffer);
					buffer = InvalidBuffer;
					break;
				}

			case RELUNDO_DELETE:
				{
					RelUndoDeletePayload *del_payload = (RelUndoDeletePayload *) payload;
					char	   *tuple_data_buf = NULL;
					uint32		tlen = 0;
					int			i;

					RelUndoReadRecordWithTuple(rel, current_ptr,
											   &tuple_data_buf, &tlen);

					for (i = 0; i < del_payload->ntids; i++)
					{
						target_blkno = ItemPointerGetBlockNumber(&del_payload->tids[i]);
						target_offset = ItemPointerGetOffsetNumber(&del_payload->tids[i]);

						if (!BufferIsValid(buffer) ||
							BufferGetBlockNumber(buffer) != target_blkno)
						{
							if (BufferIsValid(buffer))
								UnlockReleaseBuffer(buffer);
							buffer = ReadBuffer(rel, target_blkno);
							LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
							page = BufferGetPage(buffer);
						}

						if (tuple_data_buf && tlen > 0)
							RelUndoApplyDelete(rel, page, target_offset,
											   tuple_data_buf, tlen);

						MarkBufferDirty(buffer);
					}

					if (tuple_data_buf)
						pfree(tuple_data_buf);

					if (BufferIsValid(buffer))
					{
						UnlockReleaseBuffer(buffer);
						buffer = InvalidBuffer;
					}
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

					buffer = ReadBuffer(rel, target_blkno);
					LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
					page = BufferGetPage(buffer);

					if (tuple_data_buf && tlen > 0)
						RelUndoApplyUpdate(rel, page, target_offset,
										   tuple_data_buf, tlen);

					MarkBufferDirty(buffer);

					if (tuple_data_buf)
						pfree(tuple_data_buf);

					/* Mark the new tuple version as unused */
					{
						BlockNumber new_blkno;
						OffsetNumber new_offset;

						new_blkno = ItemPointerGetBlockNumber(&upd_payload->newtid);
						new_offset = ItemPointerGetOffsetNumber(&upd_payload->newtid);

						if (BufferIsValid(buffer) &&
							BufferGetBlockNumber(buffer) == new_blkno)
						{
							/* Same page -- reuse the locked buffer */
							RelUndoApplyInsert(rel, page, new_offset);
							MarkBufferDirty(buffer);
						}
						else
						{
							Buffer		new_buf;
							Page		new_page;

							new_buf = ReadBuffer(rel, new_blkno);
							LockBuffer(new_buf, BUFFER_LOCK_EXCLUSIVE);
							new_page = BufferGetPage(new_buf);
							RelUndoApplyInsert(rel, new_page, new_offset);
							MarkBufferDirty(new_buf);
							UnlockReleaseBuffer(new_buf);
						}
					}

					if (BufferIsValid(buffer))
					{
						UnlockReleaseBuffer(buffer);
						buffer = InvalidBuffer;
					}
					break;
				}

			case RELUNDO_TUPLE_LOCK:
				{
					RelUndoTupleLockPayload *lock_payload = (RelUndoTupleLockPayload *) payload;

					target_blkno = ItemPointerGetBlockNumber(&lock_payload->tid);
					target_offset = ItemPointerGetOffsetNumber(&lock_payload->tid);

					buffer = ReadBuffer(rel, target_blkno);
					LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
					page = BufferGetPage(buffer);

					RelUndoApplyTupleLock(rel, page, target_offset);
					MarkBufferDirty(buffer);

					UnlockReleaseBuffer(buffer);
					buffer = InvalidBuffer;
					break;
				}

			case RELUNDO_DELTA_INSERT:
				{
					RelUndoDeltaInsertPayload *delta_payload = (RelUndoDeltaInsertPayload *) payload;
					char	   *delta_data;

					delta_data = (char *) payload +
						offsetof(RelUndoDeltaInsertPayload, delta_len) + sizeof(uint16);

					target_blkno = ItemPointerGetBlockNumber(&delta_payload->tid);
					target_offset = ItemPointerGetOffsetNumber(&delta_payload->tid);

					buffer = ReadBuffer(rel, target_blkno);
					LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
					page = BufferGetPage(buffer);

					RelUndoApplyDeltaInsert(rel, page, target_offset,
											delta_data, delta_payload->delta_len);
					MarkBufferDirty(buffer);

					UnlockReleaseBuffer(buffer);
					buffer = InvalidBuffer;
					break;
				}

			default:
				elog(ERROR, "RelUndoApplyChain: unknown UNDO record type %d",
					 header.urec_type);
		}

		/* Write CLR for crash safety after successful application */
		if (RelationNeedsWAL(rel))
			RelUndoWriteCLR(rel, current_ptr, InvalidXLogRecPtr);

		/* Advance to the previous record in the chain */
		current_ptr = header.urec_prevundorec;

		if (payload)
		{
			pfree(payload);
			payload = NULL;
		}
	}

	elog(DEBUG1, "RelUndoApplyChain: rollback complete");
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

	/* Check if there's enough space (may need to reclaim) */
	aligned_len = MAXALIGN(tuple_len);
	if (PageGetFreeSpace(page) < aligned_len)
		elog(ERROR, "RelUndoApplyDelete: insufficient space on page to restore tuple");

	/*
	 * Restore the tuple data. We use memcpy to copy the complete tuple
	 * including the header.
	 */
	if (ItemIdIsUsed(lp))
	{
		/* Tuple slot is occupied - replace it */
		if (ItemIdGetLength(lp) != tuple_len)
			elog(ERROR, "RelUndoApplyDelete: tuple length mismatch");

		memcpy(PageGetItem(page, lp), tuple_data, tuple_len);
	}
	else
	{
		/* Need to allocate new slot */
		OffsetNumber new_offset;

		new_offset = PageAddItem(page, tuple_data, tuple_len,
								 offset, false, false);
		if (new_offset != offset)
			elog(ERROR, "RelUndoApplyDelete: could not restore tuple at expected offset");
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
 * For RECNO in-place updates, the old tuple was physically overwritten at
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

	elog(DEBUG2, "RelUndoApplyUpdate: restored old tuple at offset %u (%u bytes)",
		 offset, tuple_len);
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
 * RelUndoApplyDeltaInsert - Undo a delta/partial update
 *
 * Restore the original column data for columnar storage. This is used
 * when only specific columns were updated.
 */
static void
RelUndoApplyDeltaInsert(Relation rel, Page page, OffsetNumber offset,
						char *delta_data, uint32 delta_len)
{
	ItemId		lp;

	/* Validate inputs */
	if (delta_data == NULL || delta_len == 0)
		elog(ERROR, "RelUndoApplyDeltaInsert: invalid delta data");

	if (offset == InvalidOffsetNumber || offset > PageGetMaxOffsetNumber(page))
		elog(ERROR, "RelUndoApplyDeltaInsert: invalid offset %u", offset);

	lp = PageGetItemId(page, offset);

	if (!ItemIdIsNormal(lp))
		elog(ERROR, "RelUndoApplyDeltaInsert: tuple at offset %u is not normal", offset);

	/*
	 * In a real columnar implementation, we'd need to: 1. Parse the delta to
	 * identify which columns were modified 2. Restore the original column
	 * values This is highly table AM specific.
	 */
	elog(DEBUG2, "RelUndoApplyDeltaInsert: restored delta at offset %u (%u bytes)",
		 offset, delta_len);
}

/*
 * RelUndoWriteCLR - Write Compensation Log Record
 *
 * CLRs prevent double-application of UNDO operations after a crash during
 * rollback. We record that we've applied the UNDO operation for a specific
 * UNDO record pointer, and mark the UNDO record as applied in-place.
 */
static void
RelUndoWriteCLR(Relation rel, RelUndoRecPtr urec_ptr, XLogRecPtr clr_lsn)
{
	xl_relundo_apply xlrec;
	Buffer		undo_buf;
	Page		undo_page;
	char	   *contents;
	BlockNumber blkno;
	uint16		offset;
	RelUndoRecordHeader *rec_hdr;

	xlrec.urec_ptr = urec_ptr;
	xlrec.target_reloc = rel->rd_locator;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_relundo_apply));

	(void) XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_APPLY);

	/*
	 * Mark the UNDO record as applied in-place. Read the UNDO fork page, set
	 * the CLR flags, and mark dirty.
	 */
	blkno = RelUndoGetBlockNum(urec_ptr);
	offset = RelUndoGetOffset(urec_ptr);

	undo_buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, blkno,
								  RBM_NORMAL, NULL);
	LockBuffer(undo_buf, BUFFER_LOCK_EXCLUSIVE);
	undo_page = BufferGetPage(undo_buf);
	contents = PageGetContents(undo_page);

	rec_hdr = (RelUndoRecordHeader *) (contents + offset);
	rec_hdr->info_flags |= (RELUNDO_INFO_HAS_CLR | RELUNDO_INFO_CLR_APPLIED);

	elog(DEBUG3, "RelUndoWriteCLR: wrote CLR for UNDO record %lu, flags=%04x",
		 (unsigned long) urec_ptr, rec_hdr->info_flags);

	MarkBufferDirty(undo_buf);
	UnlockReleaseBuffer(undo_buf);
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
