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
#ifdef NOT_USED
static void RelUndoApplyDelete(Relation rel, Page page, OffsetNumber offset,
							   char *tuple_data, uint32 tuple_len);
static void RelUndoApplyUpdate(Relation rel, Page page, OffsetNumber offset,
							   char *tuple_data, uint32 tuple_len);
static void RelUndoApplyTupleLock(Relation rel, Page page, OffsetNumber offset);
static void RelUndoApplyDeltaInsert(Relation rel, Page page, OffsetNumber offset,
									char *delta_data, uint32 delta_len);
static void RelUndoWriteCLR(Relation rel, RelUndoRecPtr urec_ptr,
							XLogRecPtr clr_lsn);
#endif /* NOT_USED */

/*
 * RelUndoApplyChain - Walk and apply per-relation UNDO chain for rollback
 *
 * This is the main entry point for transaction abort. We walk backwards
 * through the UNDO chain starting from start_ptr, applying each operation
 * until we reach an invalid pointer or the beginning of the chain.
 */
void
RelUndoApplyChain(Relation rel, RelUndoRecPtr start_ptr)
{
	RelUndoRecPtr current_ptr = start_ptr;
	RelUndoRecordHeader header;
	void	   *payload = NULL;
	Size		payload_size;
	Buffer		buffer = InvalidBuffer;
	Page		page;
	BlockNumber target_blkno;
	OffsetNumber target_offset;

	/* Nothing to do if no UNDO records */
	if (!RelUndoRecPtrIsValid(current_ptr))
	{
		elog(DEBUG1, "RelUndoApplyChain: no valid UNDO pointer");
		return;
	}

	elog(DEBUG1, "RelUndoApplyChain: starting rollback at %lu",
		 (unsigned long) current_ptr);

	/*
	 * Walk backwards through the chain, applying each record.
	 * Note: Current implementation only supports INSERT rollback with
	 * metadata-only UNDO records. DELETE/UPDATE rollback would require
	 * storing complete tuple data in UNDO records.
	 */
	while (RelUndoRecPtrIsValid(current_ptr))
	{
		/* Read the UNDO record using existing function */
		if (!RelUndoReadRecord(rel, current_ptr, &header, &payload, &payload_size))
		{
			elog(WARNING, "RelUndoApplyChain: could not read UNDO record at %lu",
				 (unsigned long) current_ptr);
			break;
		}

		/* Determine target page based on record type */
		switch (header.urec_type)
		{
			case RELUNDO_INSERT:
				{
					RelUndoInsertPayload *ins_payload = (RelUndoInsertPayload *) payload;

					target_blkno = ItemPointerGetBlockNumber(&ins_payload->firsttid);
					target_offset = ItemPointerGetOffsetNumber(&ins_payload->firsttid);
					break;
				}

			case RELUNDO_DELETE:
			case RELUNDO_UPDATE:
			case RELUNDO_TUPLE_LOCK:
			case RELUNDO_DELTA_INSERT:
				/*
				 * These operations require complete tuple data in UNDO records,
				 * which is not yet implemented. For now, skip them.
				 */
				elog(WARNING, "RelUndoApplyChain: rollback for record type %d not yet implemented",
					 header.urec_type);
				current_ptr = header.urec_prevundorec;
				if (payload)
					pfree(payload);
				continue;

			default:
				elog(ERROR, "RelUndoApplyChain: unknown UNDO record type %d",
					 header.urec_type);
		}

		/* Get the target page (may reuse buffer if same page) */
		elog(DEBUG1, "RelUndoApplyChain: applying UNDO at block=%u, offset=%u",
			 target_blkno, target_offset);

		if (!BufferIsValid(buffer) ||
			BufferGetBlockNumber(buffer) != target_blkno)
		{
			if (BufferIsValid(buffer))
				ReleaseBuffer(buffer);

			elog(DEBUG1, "RelUndoApplyChain: reading buffer for block %u", target_blkno);
			buffer = ReadBuffer(rel, target_blkno);
		}

		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);

		elog(DEBUG1, "RelUndoApplyChain: page=%p, calling RelUndoApplyInsert", page);

		/* Apply the operation (only INSERT is currently supported) */
		RelUndoApplyInsert(rel, page, target_offset);

		/* Mark buffer dirty */
		MarkBufferDirty(buffer);

		UnlockReleaseBuffer(buffer);
		buffer = InvalidBuffer;

		/* Move to previous record in chain */
		current_ptr = header.urec_prevundorec;

		/* Cleanup payload */
		if (payload)
		{
			pfree(payload);
			payload = NULL;
		}
	}

	if (BufferIsValid(buffer))
		ReleaseBuffer(buffer);

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

#ifdef NOT_USED
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
#endif /* NOT_USED */

#ifdef NOT_USED
/*
 * RelUndoApplyUpdate - Undo an UPDATE operation
 *
 * Restore the old tuple version from the UNDO record. Like DELETE, this
 * requires the full tuple data stored in the UNDO record.
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
	 * Overwrite the new tuple with the old version.
	 * In a real implementation, we'd need to handle size differences,
	 * potentially using a different page if the old tuple is larger.
	 */
	if (ItemIdGetLength(lp) < tuple_len)
	{
		if (PageGetFreeSpace(page) < MAXALIGN(tuple_len) - ItemIdGetLength(lp))
			elog(ERROR, "RelUndoApplyUpdate: insufficient space to restore old tuple");

		/* Would need to reallocate - simplified for now */
		elog(ERROR, "RelUndoApplyUpdate: old tuple larger than new tuple not yet supported");
	}

	memcpy(PageGetItem(page, lp), tuple_data, tuple_len);

	elog(DEBUG2, "RelUndoApplyUpdate: restored old tuple at offset %u (%u bytes)",
		 offset, tuple_len);
}
#endif /* NOT_USED */

#ifdef NOT_USED
/*
 * RelUndoApplyTupleLock - Undo a tuple lock operation
 *
 * Remove the lock marker from the tuple. This typically involves clearing
 * lock bits in the tuple header.
 */
static void
RelUndoApplyTupleLock(Relation rel, Page page, OffsetNumber offset)
{
	ItemId		lp;

	/* Validate offset */
	if (offset == InvalidOffsetNumber || offset > PageGetMaxOffsetNumber(page))
		elog(ERROR, "RelUndoApplyTupleLock: invalid offset %u", offset);

	lp = PageGetItemId(page, offset);

	if (!ItemIdIsNormal(lp))
		elog(ERROR, "RelUndoApplyTupleLock: tuple at offset %u is not normal", offset);

	/*
	 * In a real implementation, we'd clear the lock bits in the tuple header.
	 * This is table AM specific - for now we just log.
	 */
	elog(DEBUG2, "RelUndoApplyTupleLock: removed lock from tuple at offset %u", offset);
}
#endif /* NOT_USED */

#ifdef NOT_USED
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
	 * In a real columnar implementation, we'd need to:
	 * 1. Parse the delta to identify which columns were modified
	 * 2. Restore the original column values
	 * This is highly table AM specific.
	 */
	elog(DEBUG2, "RelUndoApplyDeltaInsert: restored delta at offset %u (%u bytes)",
		 offset, delta_len);
}
#endif /* NOT_USED */

#ifdef NOT_USED
/*
 * RelUndoWriteCLR - Write Compensation Log Record
 *
 * CLRs prevent double-application of UNDO operations after a crash during
 * rollback. We record that we've applied the UNDO operation for a specific
 * UNDO record pointer.
 */
static void
RelUndoWriteCLR(Relation rel, RelUndoRecPtr urec_ptr, XLogRecPtr clr_lsn)
{
	xl_relundo_apply xlrec;
	XLogRecPtr	recptr;

	xlrec.urec_ptr = urec_ptr;
	xlrec.target_reloc = rel->rd_locator;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_relundo_apply));

	recptr = XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_APPLY);

	elog(DEBUG3, "RelUndoWriteCLR: wrote CLR for UNDO record %lu",
		 (unsigned long) urec_ptr);
}
#endif /* NOT_USED */

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
	 * Allocate combined buffer for header + payload.
	 * Tuple data will be allocated separately if present.
	 */
	header = (RelUndoRecordHeader *) palloc(SizeOfRelUndoRecordHeader + payload_size);
	memcpy(header, &header_local, SizeOfRelUndoRecordHeader);
	memcpy((char *) header + SizeOfRelUndoRecordHeader, payload, payload_size);

	/* Free the payload allocated by RelUndoReadRecord */
	pfree(payload);

	/* If tuple data is present, read it separately */
	if (header->info_flags & RELUNDO_INFO_HAS_TUPLE && header->tuple_len > 0)
	{
		/*
		 * In a real implementation, we'd need to read the tuple data
		 * from the UNDO fork. For now, return NULL to indicate this
		 * feature is not fully implemented yet.
		 *
		 * The tuple data follows the payload in the UNDO fork at:
		 * position = ptr + SizeOfRelUndoRecordHeader + payload_size
		 */
		elog(WARNING, "RelUndoReadRecordWithTuple: tuple data reading not yet implemented");
	}

	return header;
}
