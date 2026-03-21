/*-------------------------------------------------------------------------
 *
 * undoapply.c
 *	  Apply UNDO records during transaction rollback using physical
 *	  page modifications
 *
 * When a transaction aborts, this module walks the UNDO chain backward
 * from the most recent record to the first, applying each record to
 * reverse the original operation via direct page manipulation:
 *
 *   UNDO_INSERT:  Mark the ItemId dead (if indexed) or unused
 *   UNDO_DELETE:  Restore the full old tuple via memcpy into the page
 *   UNDO_UPDATE:  Restore the old tuple version via memcpy + ItemId fixup
 *   UNDO_PRUNE:   (no rollback action - informational only)
 *   UNDO_INPLACE: Restore the old tuple data via memcpy in place
 *
 * Physical vs Logical UNDO Application
 * -------------------------------------
 * The previous implementation used logical operations (simple_heap_delete,
 * simple_heap_insert) which went through the full executor path, triggered
 * index updates, generated WAL, and could fail visibility checks.
 *
 * This rewrite follows the ZHeap approach: read the target page into a
 * shared buffer, acquire an exclusive lock, and directly memcpy the
 * stored tuple data back into the page.  This is:
 *
 *   - Faster: No executor overhead, no index maintenance during undo
 *   - Safer: No visibility check failures during abort
 *   - Simpler: Direct byte-level restore with minimal code paths
 *   - Atomic: Changes applied within a critical section
 *
 * Reference: ZHeap zundo.c RestoreTupleFromUndoRecord() and
 * zheap_undo_actions() for the physical application pattern.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undoapply.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/undo_xlog.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/itemid.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/* Forward declarations */
static bool ApplyOneUndoRecord(UndoRecordHeader * header, char *tuple_data,
							   UndoRecPtr urec_ptr);
static void UndoApplyInsert(Relation rel, Page page, OffsetNumber offset);
static void UndoApplyDelete(Page page, OffsetNumber offset,
							char *tuple_data, uint32 tuple_len);
static void UndoApplyUpdate(Page page, OffsetNumber offset,
							char *tuple_data, uint32 tuple_len);
static void UndoApplyInplace(Page page, OffsetNumber offset,
							 char *tuple_data, uint32 tuple_len);

/*
 * UndoApplyInsert - physically undo an INSERT by marking the ItemId
 *
 * Following ZHeap's undo_action_insert(): mark the line pointer as dead
 * if the relation has indexes (so index entries can find it for cleanup),
 * or as unused if there are no indexes.
 *
 * This replaces the old simple_heap_delete() call which went through
 * the full heap deletion path and could fail on visibility checks.
 */
static void
UndoApplyInsert(Relation rel, Page page, OffsetNumber offset)
{
	ItemId		lp;
	bool		relhasindex;

	lp = PageGetItemId(page, offset);

	if (!ItemIdIsNormal(lp))
	{
		/*
		 * Item is already dead or unused -- nothing to do.  This can happen
		 * if the page was already cleaned up by another mechanism.
		 */
		ereport(DEBUG2,
				(errmsg("UNDO apply INSERT: item (%u) already dead/unused, skipping",
						offset)));
		return;
	}

	relhasindex = RelationGetForm(rel)->relhasindex;

	if (relhasindex)
	{
		/*
		 * Mark dead rather than unused so that index scans can identify the
		 * dead tuple and trigger index cleanup (consistent with ZHeap
		 * approach: undo_action_insert).
		 */
		ItemIdSetDead(lp);
	}
	else
	{
		ItemIdSetUnused(lp);
		PageSetHasFreeLinePointers(page);
	}

	ereport(DEBUG2,
			(errmsg("UNDO apply INSERT: marked item (%u) as %s",
					offset, relhasindex ? "dead" : "unused")));
}

/*
 * UndoApplyDelete - physically undo a DELETE by restoring the old tuple
 *
 * The UNDO record contains the complete old tuple data.  We restore it
 * by memcpy into the page at the original location, following ZHeap's
 * RestoreTupleFromUndoRecord() pattern for UNDO_DELETE.
 *
 * The ItemId must still be present (possibly marked dead) and we restore
 * both the line pointer length and the tuple data.
 */
static void
UndoApplyDelete(Page page, OffsetNumber offset,
				char *tuple_data, uint32 tuple_len)
{
	ItemId		lp;
	HeapTupleHeader page_htup;

	lp = PageGetItemId(page, offset);

	/*
	 * The item slot should still exist.  During a DELETE, the standard heap
	 * marks the item dead via ItemIdMarkDead (which preserves lp_off and
	 * lp_len).  If VACUUM has already processed the item via ItemIdSetDead
	 * (which zeroes lp_off/lp_len), the storage is gone and we cannot
	 * restore.
	 */
	if (!ItemIdIsUsed(lp))
	{
		ereport(WARNING,
				(errmsg("UNDO apply DELETE: item (%u) is unused, cannot restore tuple",
						offset)));
		return;
	}

	if (!ItemIdHasStorage(lp))
	{
		ereport(WARNING,
				(errmsg("UNDO apply DELETE: item (%u) has no storage (vacuumed?), cannot restore",
						offset)));
		return;
	}

	page_htup = (HeapTupleHeader) PageGetItem(page, lp);

	/*
	 * Set the ItemId back to LP_NORMAL with the original offset and the
	 * restored tuple length.  This is critical because DELETE marks the item
	 * as dead.  Following ZHeap: ItemIdChangeLen(lp, undo_tup_len).
	 */
	ItemIdSetNormal(lp, ItemIdGetOffset(lp), tuple_len);

	/*
	 * Restore the complete tuple data (header + user data) via memcpy. This
	 * is the core physical UNDO operation: a direct byte-level restore.
	 */
	memcpy(page_htup, tuple_data, tuple_len);

	ereport(DEBUG2,
			(errmsg("UNDO apply DELETE: restored tuple (%u bytes) at offset %u",
					tuple_len, offset)));
}

/*
 * UndoApplyUpdate - physically undo an UPDATE by restoring the old tuple
 *
 * An UPDATE creates a new tuple version and marks the old one.  To undo,
 * we restore the old tuple data at the original location via memcpy.
 *
 * This replaces the old approach of simple_heap_delete (new version) +
 * simple_heap_insert (old version) with a single memcpy.
 *
 * Note: The new tuple version created by the UPDATE is left in place as
 * a dead item.  It will be cleaned up by normal page pruning.  This is
 * safe because the aborting transaction's xmin will fail visibility checks.
 */
static void
UndoApplyUpdate(Page page, OffsetNumber offset,
				char *tuple_data, uint32 tuple_len)
{
	ItemId		lp;
	HeapTupleHeader page_htup;

	lp = PageGetItemId(page, offset);

	if (!ItemIdIsUsed(lp))
	{
		ereport(WARNING,
				(errmsg("UNDO apply UPDATE: item (%u) is unused, cannot restore old tuple version",
						offset)));
		return;
	}

	if (!ItemIdHasStorage(lp))
	{
		ereport(WARNING,
				(errmsg("UNDO apply UPDATE: item (%u) has no storage (vacuumed?), cannot restore",
						offset)));
		return;
	}

	page_htup = (HeapTupleHeader) PageGetItem(page, lp);

	/*
	 * Restore the old tuple.  Set the ItemId to NORMAL with the correct
	 * length (the old and new tuple may differ in size), then memcpy the
	 * complete old tuple.  Follows ZHeap RestoreTupleFromUndoRecord() for
	 * UNDO_UPDATE.
	 */
	ItemIdSetNormal(lp, ItemIdGetOffset(lp), tuple_len);
	memcpy(page_htup, tuple_data, tuple_len);

	ereport(DEBUG2,
			(errmsg("UNDO apply UPDATE: restored old tuple (%u bytes) at offset %u",
					tuple_len, offset)));
}

/*
 * UndoApplyInplace - physically undo an in-place update
 *
 * In-place updates modify the tuple data without changing its location.
 * The UNDO record stores the original tuple bytes.  Restoration is a
 * simple memcpy back to the same location.  The tuple size should not
 * change for a true in-place update, but we handle it defensively.
 */
static void
UndoApplyInplace(Page page, OffsetNumber offset,
				 char *tuple_data, uint32 tuple_len)
{
	ItemId		lp;
	HeapTupleHeader page_htup;

	lp = PageGetItemId(page, offset);

	if (!ItemIdIsNormal(lp))
	{
		ereport(WARNING,
				(errmsg("UNDO apply INPLACE: item (%u) is not normal, cannot restore",
						offset)));
		return;
	}

	page_htup = (HeapTupleHeader) PageGetItem(page, lp);

	/* For true in-place updates, the length should match. */
	Assert(ItemIdGetLength(lp) == tuple_len);

	/*
	 * Restore the length via ItemIdSetNormal (preserving offset). For
	 * in-place updates the length should already be correct, but we set it
	 * defensively.
	 */
	lp->lp_len = tuple_len;

	/* Direct memcpy restore */
	memcpy(page_htup, tuple_data, tuple_len);

	ereport(DEBUG2,
			(errmsg("UNDO apply INPLACE: restored tuple (%u bytes) at offset %u",
					tuple_len, offset)));
}

/*
 * ApplyOneUndoRecord - Apply a single UNDO record using physical page ops
 *
 * This function reads the target page into a shared buffer, acquires an
 * exclusive lock, applies the UNDO operation within a critical section,
 * marks the buffer dirty, and releases the lock.
 *
 * The pattern follows ZHeap's zheap_undo_actions():
 *   1. Open relation with RowExclusiveLock
 *   2. ReadBuffer to get the target page
 *   3. LockBuffer(BUFFER_LOCK_EXCLUSIVE)
 *   4. START_CRIT_SECTION
 *   5. Physical modification (memcpy / ItemId manipulation)
 *   6. MarkBufferDirty
 *   7. Generate CLR via XLogInsert (full page image)
 *   8. END_CRIT_SECTION
 *   9. UnlockReleaseBuffer
 *
 * Returns true if successfully applied, false if skipped (e.g., relation
 * dropped or page truncated).
 */
static bool
ApplyOneUndoRecord(UndoRecordHeader * header, char *tuple_data,
				   UndoRecPtr urec_ptr)
{
	Relation	rel;
	Buffer		buffer;
	Page		page;
	BlockNumber blkno;
	OffsetNumber offset;

	/*
	 * If this UNDO record already has a CLR pointer, it was already applied
	 * during a previous rollback attempt (e.g., crash during rollback
	 * followed by recovery re-applying the UNDO chain).  Skip it to avoid
	 * double-application.
	 */
	if (XLogRecPtrIsValid(header->urec_clr_ptr))
	{
		ereport(DEBUG2,
				(errmsg("UNDO rollback: record at %llu already applied (CLR at %X/%X), skipping",
						(unsigned long long) urec_ptr,
						LSN_FORMAT_ARGS(header->urec_clr_ptr))));
		return false;
	}

	/*
	 * Try to open the relation. If it has been dropped, skip this record
	 * since the data is gone anyway.
	 */
	rel = try_relation_open(header->urec_reloid, RowExclusiveLock);
	if (rel == NULL)
	{
		ereport(DEBUG2,
				(errmsg("UNDO rollback: relation %u no longer exists, skipping",
						header->urec_reloid)));
		return false;
	}

	blkno = header->urec_blkno;
	offset = header->urec_offset;

	/*
	 * Check if the block still exists.  The relation may have been truncated
	 * between the original operation and the rollback.
	 */
	if (RelationGetNumberOfBlocks(rel) <= blkno)
	{
		ereport(DEBUG2,
				(errmsg("UNDO rollback: block %u beyond end of relation %u (truncated?), skipping",
						blkno, header->urec_reloid)));
		relation_close(rel, RowExclusiveLock);
		return false;
	}

	/*
	 * Read the target page into a shared buffer and acquire an exclusive
	 * lock.  This is the physical UNDO approach: we modify the page directly
	 * rather than going through the executor.
	 */
	buffer = ReadBuffer(rel, blkno);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buffer);

	/*
	 * Apply the UNDO operation within a critical section.  This ensures that
	 * if we crash mid-operation, WAL replay will handle recovery. Following
	 * ZHeap's pattern of START_CRIT_SECTION around physical page
	 * modifications.
	 */
	START_CRIT_SECTION();

	switch (header->urec_type)
	{
		case UNDO_INSERT:

			/*
			 * Undo INSERT: mark the inserted tuple's ItemId as dead (if
			 * relation has indexes) or unused (if no indexes).  No tuple data
			 * restoration needed -- the tuple is simply invalidated.
			 */
			UndoApplyInsert(rel, page, offset);
			break;

		case UNDO_DELETE:

			/*
			 * Undo DELETE: restore the complete old tuple from UNDO record.
			 * The tuple data is memcpy'd directly into the page.
			 */
			if (tuple_data != NULL && header->urec_tuple_len > 0)
			{
				UndoApplyDelete(page, offset,
								tuple_data, header->urec_tuple_len);
			}
			else
			{
				ereport(WARNING,
						(errmsg("UNDO rollback: DELETE record for relation %u has no tuple data",
								header->urec_reloid)));
			}
			break;

		case UNDO_UPDATE:

			/*
			 * Undo UPDATE: restore the old tuple version at the original
			 * location.  The new tuple version (at a potentially different
			 * location) is left for normal pruning to clean up.
			 */
			if (tuple_data != NULL && header->urec_tuple_len > 0)
			{
				UndoApplyUpdate(page, offset,
								tuple_data, header->urec_tuple_len);
			}
			else
			{
				ereport(WARNING,
						(errmsg("UNDO rollback: UPDATE record for relation %u has no tuple data",
								header->urec_reloid)));
			}
			break;

		case UNDO_PRUNE:

			/*
			 * PRUNE records are informational -- they record tuples that were
			 * pruned for recovery purposes.  During transaction rollback,
			 * prune operations cannot be undone because they are page-level
			 * maintenance operations.
			 */
			ereport(DEBUG2,
					(errmsg("UNDO rollback: skipping PRUNE record for relation %u",
							header->urec_reloid)));
			break;

		case UNDO_INPLACE:

			/*
			 * Undo in-place UPDATE: restore the original tuple bytes at the
			 * same page location via direct memcpy.
			 */
			if (tuple_data != NULL && header->urec_tuple_len > 0)
			{
				UndoApplyInplace(page, offset,
								 tuple_data, header->urec_tuple_len);
			}
			else
			{
				ereport(WARNING,
						(errmsg("UNDO rollback: INPLACE record for relation %u has no tuple data",
								header->urec_reloid)));
			}
			break;

		default:
			ereport(WARNING,
					(errmsg("UNDO rollback: unknown record type %u, skipping",
							header->urec_type)));
			break;
	}

	MarkBufferDirty(buffer);

	/*
	 * Generate a Compensation Log Record (CLR) for crash safety.
	 *
	 * We log a full page image (REGBUF_FORCE_IMAGE) so that recovery can
	 * restore the page to its post-undo state without needing the UNDO record
	 * data.  This follows ZHeap's approach in log_zheap_undo_actions which
	 * also uses REGBUF_FORCE_IMAGE for undo action WAL records.
	 *
	 * The xl_undo_apply metadata is included for debugging and pg_waldump
	 * output.  The actual page restoration during redo is handled entirely by
	 * the full page image.
	 *
	 * Skip WAL logging for unlogged relations (they don't need crash safety
	 * and are reset to empty on recovery anyway).
	 */
	if (RelationNeedsWAL(rel))
	{
		XLogRecPtr	lsn;
		xl_undo_apply xlrec;

		xlrec.urec_ptr = urec_ptr;
		xlrec.xid = header->urec_xid;
		xlrec.target_locator = rel->rd_locator;
		xlrec.target_block = blkno;
		xlrec.target_offset = offset;
		xlrec.operation_type = header->urec_type;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfUndoApply);
		XLogRegisterBuffer(0, buffer, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

		lsn = XLogInsert(RM_UNDO_ID, XLOG_UNDO_APPLY_RECORD);
		PageSetLSN(page, lsn);

		/*
		 * Write the CLR pointer back into the UNDO record.  This marks the
		 * record as "already applied" so that crash recovery (which may need
		 * to re-walk the UNDO chain) can skip it.  The write goes to the
		 * urec_clr_ptr field at a known offset within the serialized record.
		 */
		UndoLogWrite(urec_ptr + offsetof(UndoRecordHeader, urec_clr_ptr),
					 (const char *) &lsn, sizeof(XLogRecPtr));

		/*
		 * Also set UNDO_INFO_HAS_CLR in the record's urec_info flags so that
		 * readers can quickly determine this record has been applied without
		 * checking the full urec_clr_ptr field.
		 */
		{
			uint16		new_info = header->urec_info | UNDO_INFO_HAS_CLR;

			UndoLogWrite(urec_ptr + offsetof(UndoRecordHeader, urec_info),
						 (const char *) &new_info, sizeof(uint16));
		}
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);
	relation_close(rel, RowExclusiveLock);

	return true;
}

/*
 * ApplyUndoChain - Walk and apply an UNDO chain during transaction abort
 *
 * This function reads the UNDO chain starting from 'start_ptr' and applies
 * each record in order. Records are processed from the most recent to the
 * oldest (reverse chronological order), which is the natural order for
 * rollback.
 *
 * Each record is applied using physical page modifications: the target
 * page is read into a shared buffer, locked exclusively, modified via
 * memcpy, marked dirty, and released.
 *
 * On error, we emit a WARNING and continue processing remaining records.
 * This is a best-effort approach -- we do not want UNDO failures to prevent
 * transaction abort from completing.
 */
void
ApplyUndoChain(UndoRecPtr start_ptr)
{
	UndoRecPtr	current_ptr;
	char	   *read_buffer = NULL;
	Size		buffer_size = 0;
	int			records_applied = 0;
	int			records_skipped = 0;

	if (!UndoRecPtrIsValid(start_ptr))
		return;

	ereport(DEBUG1,
			(errmsg("applying UNDO chain starting at %llu",
					(unsigned long long) start_ptr)));

	current_ptr = start_ptr;

	/* Process each UNDO record in the chain */
	while (UndoRecPtrIsValid(current_ptr))
	{
		UndoRecordHeader header;
		char	   *tuple_data = NULL;
		Size		record_size;

		/*
		 * Read the fixed header first to determine the full record size.
		 */
		if (buffer_size < SizeOfUndoRecordHeader)
		{
			buffer_size = Max(SizeOfUndoRecordHeader + 8192, buffer_size * 2);
			if (read_buffer)
				pfree(read_buffer);
			read_buffer = (char *) palloc(buffer_size);
		}

		UndoLogRead(current_ptr, read_buffer, SizeOfUndoRecordHeader);
		memcpy(&header, read_buffer, SizeOfUndoRecordHeader);

		record_size = header.urec_len;

		/*
		 * Sanity check: record size should be at least the header size and
		 * not absurdly large.
		 */
		if (record_size < SizeOfUndoRecordHeader ||
			record_size > 1024 * 1024 * 1024)
		{
			ereport(WARNING,
					(errmsg("UNDO rollback: invalid record size %zu at %llu, stopping chain walk",
							record_size, (unsigned long long) current_ptr)));
			break;
		}

		/* Read the full record if it contains tuple data */
		if (record_size > SizeOfUndoRecordHeader)
		{
			if (buffer_size < record_size)
			{
				buffer_size = record_size;
				pfree(read_buffer);
				read_buffer = (char *) palloc(buffer_size);
			}

			UndoLogRead(current_ptr, read_buffer, record_size);

			/* Re-read header from full buffer */
			memcpy(&header, read_buffer, SizeOfUndoRecordHeader);

			/*
			 * Tuple data follows immediately after the fixed header in the
			 * serialized record.
			 */
			if (header.urec_tuple_len > 0)
				tuple_data = read_buffer + SizeOfUndoRecordHeader;
		}

		/* Apply this record using physical page modification */
		if (ApplyOneUndoRecord(&header, tuple_data, current_ptr))
			records_applied++;
		else
			records_skipped++;

		/*
		 * Follow the chain to the previous record.
		 */
		current_ptr = header.urec_prev;
	}

	if (read_buffer)
		pfree(read_buffer);

	/* Report results */
	if (records_skipped > 0)
	{
		ereport(WARNING,
				(errmsg("UNDO rollback: %d records applied, %d skipped",
						records_applied, records_skipped)));
	}
	else
	{
		ereport(DEBUG1,
				(errmsg("UNDO rollback complete: %d records applied",
						records_applied)));
	}
}
