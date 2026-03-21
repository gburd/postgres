/*-------------------------------------------------------------------------
 *
 * heapam_undo.c
 *	  Heap AM UNDO resource manager
 *
 * This module implements the UNDO apply callbacks for the heap access
 * method.  It handles physical reversal of INSERT, DELETE, UPDATE, and
 * INPLACE operations by directly manipulating page contents via memcpy.
 *
 * The approach follows ZHeap's physical undo model:
 *   - Read the target page into a shared buffer
 *   - Acquire an exclusive lock
 *   - Apply the reversal (mark dead, restore tuple, etc.)
 *   - Generate a CLR (Compensation Log Record) for crash safety
 *   - Release the buffer
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
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
 * Heap UNDO subtypes (stored in urec_info)
 */
#define HEAP_UNDO_INSERT	0x0001
#define HEAP_UNDO_DELETE	0x0002
#define HEAP_UNDO_UPDATE	0x0003
#define HEAP_UNDO_PRUNE		0x0004
#define HEAP_UNDO_INPLACE	0x0005

/*
 * Heap UNDO payload flags
 */
#define HEAP_UNDO_HAS_INDEX		0x0001	/* Relation has indexes */
#define HEAP_UNDO_HAS_TUPLE		0x0002	/* Payload contains tuple data */

/*
 * HeapUndoPayload - RM-specific payload for heap UNDO records
 *
 * This structure is serialized into the UNDO record's opaque payload.
 * It contains the target page location and optionally the full tuple
 * data needed for physical restoration.
 */
typedef struct HeapUndoPayload
{
	BlockNumber blkno;			/* Target block */
	OffsetNumber offset;		/* Item offset within page */
	uint16		flags;			/* HEAP_UNDO_HAS_INDEX etc. */
	uint32		tuple_len;		/* Length of tuple data (0 for INSERT) */
	/* Followed by tuple_len bytes of HeapTupleHeaderData + user data */
}			HeapUndoPayload;

#define SizeOfHeapUndoPayload	offsetof(HeapUndoPayload, tuple_len) + sizeof(uint32)

/* Forward declarations */
static UndoApplyResult heap_undo_apply(uint8 rmid, uint16 info,
									   TransactionId xid, Oid reloid,
									   const char *payload, Size payload_len,
									   UndoRecPtr urec_ptr);
static void heap_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
						   const char *payload, Size payload_len);

/* The heap UNDO RM registration entry */
static const UndoRmgrData heap_undo_rmgr = {
	.rm_name = "heap",
	.rm_undo = heap_undo_apply,
	.rm_desc = heap_undo_desc,
};

/*
 * HeapUndoRmgrInit - Register the heap UNDO resource manager
 *
 * Called during postmaster startup to register the heap RM's callbacks.
 */
void
HeapUndoRmgrInit(void)
{
	RegisterUndoRmgr(UNDO_RMID_HEAP, &heap_undo_rmgr);
}

/*
 * HeapUndoBuildPayload - Build a heap UNDO payload for a given operation
 *
 * This is the helper used by heapam.c to construct the payload that gets
 * stored in the UNDO record.  Returns the total payload size.
 */
Size
HeapUndoBuildPayload(char *dest, Size dest_size,
					 BlockNumber blkno, OffsetNumber offset,
					 bool relhasindex, const char *tuple_data,
					 uint32 tuple_len)
{
	HeapUndoPayload hdr;
	Size		total_size;

	total_size = SizeOfHeapUndoPayload + tuple_len;
	Assert(dest_size >= total_size);

	hdr.blkno = blkno;
	hdr.offset = offset;
	hdr.flags = 0;
	if (relhasindex)
		hdr.flags |= HEAP_UNDO_HAS_INDEX;
	if (tuple_len > 0)
		hdr.flags |= HEAP_UNDO_HAS_TUPLE;
	hdr.tuple_len = tuple_len;

	memcpy(dest, &hdr, SizeOfHeapUndoPayload);
	if (tuple_len > 0 && tuple_data != NULL)
		memcpy(dest + SizeOfHeapUndoPayload, tuple_data, tuple_len);

	return total_size;
}

/*
 * HeapUndoPayloadSize - Calculate payload size for a heap UNDO record
 */
Size
HeapUndoPayloadSize(uint32 tuple_len)
{
	return SizeOfHeapUndoPayload + tuple_len;
}

/*
 * HeapUndoBuildHeader - Fill in a HeapUndoPayloadHeader struct
 *
 * This builds just the fixed header portion of the UNDO payload.
 * Callers can pass this header and the raw tuple data separately to
 * scatter-gather UNDO APIs, avoiding an intermediate palloc + memcpy.
 */
void
HeapUndoBuildHeader(HeapUndoPayloadHeader *hdr,
					BlockNumber blkno, OffsetNumber offset,
					bool relhasindex, uint32 tuple_len)
{
	hdr->blkno = blkno;
	hdr->offset = offset;
	hdr->flags = 0;
	if (relhasindex)
		hdr->flags |= HEAP_UNDO_HAS_INDEX;
	if (tuple_len > 0)
		hdr->flags |= HEAP_UNDO_HAS_TUPLE;
	hdr->tuple_len = tuple_len;
}

/*
 * heap_undo_apply - Apply a single heap UNDO record
 *
 * This is the rm_undo callback for the heap RM.  It dispatches by
 * subtype (INSERT/DELETE/UPDATE/INPLACE) and applies the physical
 * page modification.
 */
static UndoApplyResult
heap_undo_apply(uint8 rmid, uint16 info, TransactionId xid, Oid reloid,
				const char *payload, Size payload_len, UndoRecPtr urec_ptr)
{
	HeapUndoPayload hdr;
	Relation	rel;
	Buffer		buffer;
	Page		page;
	const char *tuple_data;

	Assert(rmid == UNDO_RMID_HEAP);

	/* Deserialize the heap-specific payload header */
	if (payload_len < SizeOfHeapUndoPayload)
	{
		ereport(WARNING,
				(errmsg("heap UNDO: payload too short (%zu bytes) for record at %llu",
						payload_len, (unsigned long long) urec_ptr)));
		return UNDO_APPLY_ERROR;
	}

	memcpy(&hdr, payload, SizeOfHeapUndoPayload);
	tuple_data = (payload_len > SizeOfHeapUndoPayload) ?
		payload + SizeOfHeapUndoPayload : NULL;

	/*
	 * Try to open the relation. If it has been dropped, skip this record.
	 */
	rel = try_relation_open(reloid, RowExclusiveLock);
	if (rel == NULL)
	{
		ereport(DEBUG2,
				(errmsg("heap UNDO: relation %u no longer exists, skipping",
						reloid)));
		return UNDO_APPLY_SKIPPED;
	}

	/*
	 * Check if the block still exists (relation may have been truncated).
	 */
	if (RelationGetNumberOfBlocks(rel) <= hdr.blkno)
	{
		ereport(DEBUG2,
				(errmsg("heap UNDO: block %u beyond end of relation %u, skipping",
						hdr.blkno, reloid)));
		relation_close(rel, RowExclusiveLock);
		return UNDO_APPLY_SKIPPED;
	}

	/*
	 * Read the target page and acquire exclusive lock.
	 */
	buffer = ReadBuffer(rel, hdr.blkno);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);

	/*
	 * Apply the UNDO operation within a critical section.
	 */
	START_CRIT_SECTION();

	switch (info)
	{
		case HEAP_UNDO_INSERT:
			{
				ItemId		lp = PageGetItemId(page, hdr.offset);

				if (!ItemIdIsNormal(lp))
				{
					ereport(DEBUG2,
							(errmsg("heap UNDO INSERT: item (%u) already dead/unused",
									hdr.offset)));
				}
				else if (hdr.flags & HEAP_UNDO_HAS_INDEX)
				{
					ItemIdSetDead(lp);
				}
				else
				{
					ItemIdSetUnused(lp);
					PageSetHasFreeLinePointers(page);
				}
			}
			break;

		case HEAP_UNDO_DELETE:
			if (tuple_data != NULL && hdr.tuple_len > 0)
			{
				ItemId		lp = PageGetItemId(page, hdr.offset);

				if (!ItemIdIsUsed(lp) || !ItemIdHasStorage(lp))
				{
					ereport(WARNING,
							(errmsg("heap UNDO DELETE: item (%u) has no storage",
									hdr.offset)));
				}
				else
				{
					HeapTupleHeader page_htup =
						(HeapTupleHeader) PageGetItem(page, lp);

					ItemIdSetNormal(lp, ItemIdGetOffset(lp), hdr.tuple_len);
					memcpy(page_htup, tuple_data, hdr.tuple_len);
				}
			}
			break;

		case HEAP_UNDO_UPDATE:
			if (tuple_data != NULL && hdr.tuple_len > 0)
			{
				ItemId		lp = PageGetItemId(page, hdr.offset);

				if (!ItemIdIsUsed(lp) || !ItemIdHasStorage(lp))
				{
					ereport(WARNING,
							(errmsg("heap UNDO UPDATE: item (%u) has no storage",
									hdr.offset)));
				}
				else
				{
					HeapTupleHeader page_htup =
						(HeapTupleHeader) PageGetItem(page, lp);

					ItemIdSetNormal(lp, ItemIdGetOffset(lp), hdr.tuple_len);
					memcpy(page_htup, tuple_data, hdr.tuple_len);
				}
			}
			break;

		case HEAP_UNDO_PRUNE:
			/* PRUNE records are informational only -- no rollback action */
			break;

		case HEAP_UNDO_INPLACE:
			if (tuple_data != NULL && hdr.tuple_len > 0)
			{
				ItemId		lp = PageGetItemId(page, hdr.offset);

				if (!ItemIdIsNormal(lp))
				{
					ereport(WARNING,
							(errmsg("heap UNDO INPLACE: item (%u) not normal",
									hdr.offset)));
				}
				else
				{
					HeapTupleHeader page_htup =
						(HeapTupleHeader) PageGetItem(page, lp);

					lp->lp_len = hdr.tuple_len;
					memcpy(page_htup, tuple_data, hdr.tuple_len);
				}
			}
			break;

		default:
			ereport(WARNING,
					(errmsg("heap UNDO: unknown subtype %u", info)));
			break;
	}

	MarkBufferDirty(buffer);

	/*
	 * Generate CLR (Compensation Log Record) for crash safety.
	 */
	if (RelationNeedsWAL(rel))
	{
		XLogRecPtr	lsn;
		xl_undo_apply xlrec;

		xlrec.urec_ptr = urec_ptr;
		xlrec.xid = xid;
		xlrec.target_locator = rel->rd_locator;
		xlrec.target_block = hdr.blkno;
		xlrec.target_offset = hdr.offset;
		xlrec.operation_type = info;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfUndoApply);
		XLogRegisterBuffer(0, buffer, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

		lsn = XLogInsert(RM_UNDO_ID, XLOG_UNDO_APPLY_RECORD);
		PageSetLSN(page, lsn);

		/*
		 * Write the CLR pointer back into the UNDO record to mark it as
		 * already applied.
		 */
		UndoLogWrite(urec_ptr + offsetof(UndoRecordHeader, urec_clr_ptr),
					 (const char *) &lsn, sizeof(XLogRecPtr));

		{
			uint8		new_flags = UNDO_INFO_HAS_CLR | UNDO_INFO_XID_VALID;

			UndoLogWrite(urec_ptr + offsetof(UndoRecordHeader, urec_flags),
						 (const char *) &new_flags, sizeof(uint8));
		}
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);
	relation_close(rel, RowExclusiveLock);

	return UNDO_APPLY_SUCCESS;
}

/*
 * heap_undo_desc - Describe a heap UNDO record for debugging
 */
static void
heap_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
			   const char *payload, Size payload_len)
{
	HeapUndoPayload hdr;
	const char *opname;

	if (payload_len < SizeOfHeapUndoPayload)
	{
		appendStringInfo(buf, "heap UNDO: invalid payload");
		return;
	}

	memcpy(&hdr, payload, SizeOfHeapUndoPayload);

	switch (info)
	{
		case HEAP_UNDO_INSERT:
			opname = "INSERT";
			break;
		case HEAP_UNDO_DELETE:
			opname = "DELETE";
			break;
		case HEAP_UNDO_UPDATE:
			opname = "UPDATE";
			break;
		case HEAP_UNDO_PRUNE:
			opname = "PRUNE";
			break;
		case HEAP_UNDO_INPLACE:
			opname = "INPLACE";
			break;
		default:
			opname = "UNKNOWN";
			break;
	}

	appendStringInfo(buf, "heap %s: blk %u off %u tuple_len %u",
					 opname, hdr.blkno, hdr.offset, hdr.tuple_len);
}
