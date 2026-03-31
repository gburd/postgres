/*-------------------------------------------------------------------------
 *
 * relundo.c
 *	  Per-relation UNDO core implementation
 *
 * This file implements the main API for per-relation UNDO logging used by
 * table access methods that need MVCC visibility via UNDO chain walking.
 *
 * The two-phase insert protocol works as follows:
 *
 *   1. RelUndoReserve() - Finds (or allocates) a page with enough space,
 *      pins and exclusively locks the buffer, advances pd_lower to reserve
 *      space, and returns an RelUndoRecPtr encoding the position.
 *
 *   2. Caller performs the DML operation.
 *
 *   3a. RelUndoFinish() - Writes the actual UNDO record into the reserved
 *       space, marks the buffer dirty, and releases it.
 *   3b. RelUndoCancel() - Releases the buffer without writing; the reserved
 *       space becomes a hole (zero-filled).
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/relundo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relundo.h"
#include "access/relundo_xlog.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "common/relpath.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

/*
 * RelUndoReserve
 *		Reserve space for an UNDO record (Phase 1 of 2-phase insert)
 *
 * Finds a page with enough free space for record_size bytes (which must
 * include the RelUndoRecordHeader).  If the current head page doesn't have
 * enough room, a new page is allocated and linked at the head.
 *
 * Returns an RelUndoRecPtr encoding (counter, blockno, offset).
 * The buffer is returned pinned and exclusively locked via *undo_buffer.
 */
RelUndoRecPtr
RelUndoReserve(Relation rel, Size record_size, Buffer *undo_buffer)
{
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	Buffer		databuf;
	Page		datapage;
	RelUndoPageHeader datahdr;
	BlockNumber blkno;
	uint16		offset;
	RelUndoRecPtr ptr;

	/*
	 * Sanity check: record must fit on an empty data page.  The usable space
	 * is the contents area minus our RelUndoPageHeaderData.
	 */
	{
		Size		max_record = BLCKSZ - MAXALIGN(SizeOfPageHeaderData)
			- SizeOfRelUndoPageHeaderData;

		if (record_size > max_record)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("UNDO record size %zu exceeds maximum %zu",
							record_size, max_record)));
	}

	/* Read the metapage with exclusive lock */
	metabuf = relundo_get_metapage(rel, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	elog(DEBUG1, "RelUndoReserve: record_size=%zu, head_blkno=%u",
		 record_size, meta->head_blkno);

	/*
	 * If there's a head page, check if it has enough space.
	 */
	if (BlockNumberIsValid(meta->head_blkno))
	{
		elog(DEBUG1, "RelUndoReserve: reading existing head page %u",
			 meta->head_blkno);

		databuf = ReadBufferExtended(rel, RELUNDO_FORKNUM, meta->head_blkno,
									 RBM_NORMAL, NULL);
		LockBuffer(databuf, BUFFER_LOCK_EXCLUSIVE);

		datapage = BufferGetPage(databuf);

		elog(DEBUG1, "RelUndoReserve: free_space=%zu",
			 relundo_get_free_space(datapage));

		if (relundo_get_free_space(datapage) >= record_size)
		{
			/* Enough space on current head page */
			blkno = meta->head_blkno;

			elog(DEBUG1, "RelUndoReserve: enough space, using block %u", blkno);

			/* Release the metapage -- we don't need to modify it */
			UnlockReleaseBuffer(metabuf);
			goto reserve;
		}

		/* Not enough space; release this page, allocate a new one */
		elog(DEBUG1, "RelUndoReserve: not enough space, allocating new page");
		UnlockReleaseBuffer(databuf);
	}

	/*
	 * Need a new page.  relundo_allocate_page handles free list / extend,
	 * links the new page as head, and marks both buffers dirty.
	 */
	blkno = relundo_allocate_page(rel, metabuf, &databuf);
	datapage = BufferGetPage(databuf);

	UnlockReleaseBuffer(metabuf);

reserve:
	/* Reserve space by advancing pd_lower */
	datahdr = (RelUndoPageHeader) PageGetContents(datapage);

	offset = datahdr->pd_lower;
	datahdr->pd_lower += record_size;

	/* Build the UNDO pointer */
	ptr = MakeRelUndoRecPtr(datahdr->counter, blkno, offset);

	*undo_buffer = databuf;
	return ptr;
}

/*
 * RelUndoFinish
 *		Complete UNDO record insertion (Phase 2 of 2-phase insert)
 *
 * Writes the header and payload into the space reserved by RelUndoReserve(),
 * marks the buffer dirty, and releases it.
 *
 * WAL logging is deferred to Phase 3 (WAL integration).
 */
void
RelUndoFinish(Relation rel, Buffer undo_buffer, RelUndoRecPtr ptr,
			 const RelUndoRecordHeader *header, const void *payload,
			 Size payload_size)
{
	Page		page;
	char	   *contents;
	uint16		offset;
	Size		total_record_size;
	xl_relundo_insert xlrec;
	char	   *record_data;
	RelUndoPageHeader datahdr;
	bool		is_new_page;
	uint8		info;
	Buffer		metabuf = InvalidBuffer;

	elog(DEBUG1, "RelUndoFinish: starting, ptr=%lu, payload_size=%zu",
		 (unsigned long) ptr, payload_size);

	elog(DEBUG1, "RelUndoFinish: calling BufferGetPage");
	page = BufferGetPage(undo_buffer);

	elog(DEBUG1, "RelUndoFinish: calling PageGetContents");
	contents = PageGetContents(page);

	elog(DEBUG1, "RelUndoFinish: calling RelUndoGetOffset");
	offset = RelUndoGetOffset(ptr);

	elog(DEBUG1, "RelUndoFinish: casting to RelUndoPageHeader");
	datahdr = (RelUndoPageHeader) contents;

	elog(DEBUG1, "RelUndoFinish: checking is_new_page, offset=%u", offset);
	/*
	 * Check if this is the first record on a newly allocated page. If the
	 * offset equals the header size, this is a new page.
	 */
	is_new_page = (offset == SizeOfRelUndoPageHeaderData);

	elog(DEBUG1, "RelUndoFinish: is_new_page=%d", is_new_page);

	/* Calculate total UNDO record size */
	total_record_size = SizeOfRelUndoRecordHeader + payload_size;

	elog(DEBUG1, "RelUndoFinish: writing header at offset %u", offset);
	/* Write the header */
	memcpy(contents + offset, header, SizeOfRelUndoRecordHeader);

	elog(DEBUG1, "RelUndoFinish: writing payload");
	/* Write the payload immediately after the header */
	if (payload_size > 0 && payload != NULL)
		memcpy(contents + offset + SizeOfRelUndoRecordHeader,
			   payload, payload_size);

	elog(DEBUG1, "RelUndoFinish: marking buffer dirty");
	/*
	 * Mark the buffer dirty now, before the critical section.
	 * XLogRegisterBuffer requires the buffer to be dirty when called.
	 */
	MarkBufferDirty(undo_buffer);

	elog(DEBUG1, "RelUndoFinish: checking if need metapage");
	/*
	 * If this is a new page, get the metapage lock BEFORE entering the
	 * critical section. We need to include the metapage in the WAL record
	 * since it was modified during page allocation.
	 *
	 * Note: We need EXCLUSIVE lock because XLogRegisterBuffer requires the
	 * buffer to be exclusively locked.
	 */
	if (is_new_page)
	{
		elog(DEBUG1, "RelUndoFinish: getting metapage");
		metabuf = relundo_get_metapage(rel, BUFFER_LOCK_EXCLUSIVE);
	}

	/*
	 * Allocate WAL record data buffer BEFORE entering critical section.
	 * Cannot call palloc() inside a critical section.
	 */
	elog(DEBUG1, "RelUndoFinish: allocating WAL record buffer, is_new_page=%d, total_record_size=%zu",
		 is_new_page, total_record_size);

	if (is_new_page)
	{
		Size		wal_data_size = SizeOfRelUndoPageHeaderData + total_record_size;

		elog(DEBUG1, "RelUndoFinish: new page, allocating %zu bytes", wal_data_size);
		record_data = (char *) palloc(wal_data_size);

		/* Copy page header */
		memcpy(record_data, datahdr, SizeOfRelUndoPageHeaderData);

		/* Copy UNDO record after the page header */
		memcpy(record_data + SizeOfRelUndoPageHeaderData,
			   header, SizeOfRelUndoRecordHeader);
		if (payload_size > 0 && payload != NULL)
			memcpy(record_data + SizeOfRelUndoPageHeaderData + SizeOfRelUndoRecordHeader,
				   payload, payload_size);
	}
	else
	{
		/* Normal case: just the UNDO record */
		elog(DEBUG1, "RelUndoFinish: existing page, allocating %zu bytes", total_record_size);
		record_data = (char *) palloc(total_record_size);
		elog(DEBUG1, "RelUndoFinish: palloc succeeded, record_data=%p", record_data);
		elog(DEBUG1, "RelUndoFinish: copying header, header=%p, size=%zu", header, SizeOfRelUndoRecordHeader);
		memcpy(record_data, header, SizeOfRelUndoRecordHeader);
		elog(DEBUG1, "RelUndoFinish: header copied");
		if (payload_size > 0 && payload != NULL)
		{
			elog(DEBUG1, "RelUndoFinish: copying payload, payload=%p, size=%zu", payload, payload_size);
			memcpy(record_data + SizeOfRelUndoRecordHeader, payload, payload_size);
			elog(DEBUG1, "RelUndoFinish: payload memcpy completed");
		}
		elog(DEBUG1, "RelUndoFinish: finished WAL buffer preparation");
	}

	elog(DEBUG1, "RelUndoFinish: about to START_CRIT_SECTION");
	/* WAL-log the insertion */
	START_CRIT_SECTION();

	xlrec.urec_type = header->urec_type;
	xlrec.urec_len = header->urec_len;
	xlrec.page_offset = MAXALIGN(SizeOfPageHeaderData) + offset;
	xlrec.new_pd_lower = datahdr->pd_lower;

	info = XLOG_RELUNDO_INSERT;
	if (is_new_page)
		info |= XLOG_RELUNDO_INIT_PAGE;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfRelundoInsert);

	/*
	 * Register the data page. We need to register the entire UNDO record
	 * (header + payload) as block data.
	 *
	 * For a new page, we also include the RelUndoPageHeaderData so that redo
	 * can reconstruct the page header fields (prev_blkno, counter).
	 * Use REGBUF_WILL_INIT to indicate the redo routine will initialize the page.
	 */
	if (is_new_page)
		XLogRegisterBuffer(0, undo_buffer, REGBUF_WILL_INIT);
	else
		XLogRegisterBuffer(0, undo_buffer, REGBUF_STANDARD);

	if (is_new_page)
	{
		Size		wal_data_size = SizeOfRelUndoPageHeaderData + total_record_size;

		XLogRegisterBufData(0, record_data, wal_data_size);

		/*
		 * When allocating a new page, the metapage was also updated
		 * (head_blkno). Register it as block 1 so the metapage state is
		 * preserved in WAL. Use REGBUF_STANDARD to get a full page image.
		 */
		XLogRegisterBuffer(1, metabuf, REGBUF_STANDARD);
	}
	else
	{
		/* Normal case: just the UNDO record */
		XLogRegisterBufData(0, record_data, total_record_size);
	}

	XLogInsert(RM_RELUNDO_ID, info);

	END_CRIT_SECTION();

	pfree(record_data);

	UnlockReleaseBuffer(undo_buffer);

	/* Release metapage if we locked it */
	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

/*
 * RelUndoCancel
 *		Cancel UNDO record reservation
 *
 * The reserved space is left as a zero-filled hole.  Readers will see
 * urec_type == 0 and skip it.  The buffer is released.
 */
void
RelUndoCancel(Relation rel, Buffer undo_buffer, RelUndoRecPtr ptr)
{
	/*
	 * The space was already zeroed by relundo_init_page().  pd_lower has been
	 * advanced past it, so it's just a hole.  Nothing to write.
	 */
	UnlockReleaseBuffer(undo_buffer);
}

/*
 * RelUndoReadRecord
 *		Read an UNDO record from the log
 *
 * Reads the header and payload from the location encoded in ptr.
 * Returns false if the pointer is invalid or the record has been discarded.
 * On success, *payload is palloc'd and must be pfree'd by the caller.
 */
bool
RelUndoReadRecord(Relation rel, RelUndoRecPtr ptr, RelUndoRecordHeader *header,
				 void **payload, Size *payload_size)
{
	BlockNumber blkno;
	uint16		offset;
	Buffer		buf;
	Page		page;
	char	   *contents;
	Size		psize;

	blkno = RelUndoGetBlockNum(ptr);
	offset = RelUndoGetOffset(ptr);

	if (!RelUndoRecPtrIsValid(ptr))
		return false;

	/* Check that the UNDO fork exists */
	if (!smgrexists(RelationGetSmgr(rel), RELUNDO_FORKNUM))
	{
		elog(DEBUG1, "RelUndoReadRecord: UNDO fork does not exist for relation %u", RelationGetRelid(rel));
		return false;
	}

	/*
	 * NOTE: We don't validate blkno < RelationGetNumberOfBlocksInFork() here
	 * because that function can return stale cached data. ExtendBufferedRel
	 * updates the fork size but doesn't immediately invalidate all caches.
	 * Instead, we rely on ReadBufferExtended to fail if the block doesn't exist.
	 */

	buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	page = BufferGetPage(buf);
	contents = PageGetContents(page);

	/* Validate page counter and offset */
	{
		RelUndoPageHeader hdr = (RelUndoPageHeader) contents;
		uint16		requested_counter = RelUndoGetCounter(ptr);

		/*
		 * Verify that the page's counter matches the pointer's counter.
		 * If they differ, the page has been discarded and reused with a new
		 * generation counter, meaning the old UNDO record is gone. This
		 * protects against stale pointers after page reuse.
		 */
		if (hdr->counter != requested_counter)
		{
			elog(DEBUG1, "RelUndoReadRecord: counter mismatch - page counter=%u, requested counter=%u (page reused)",
				 hdr->counter, requested_counter);
			UnlockReleaseBuffer(buf);
			return false;
		}

		/* Validate that offset is within the written portion of the page */
		if (offset < SizeOfRelUndoPageHeaderData || offset >= hdr->pd_lower)
		{
			elog(DEBUG1, "RelUndoReadRecord: offset validation failed - offset=%u, pd_lower=%u, pd_upper=%u, counter=%u",
				 offset, hdr->pd_lower, hdr->pd_upper, hdr->counter);
			UnlockReleaseBuffer(buf);
			return false;
		}
	}

	/* Copy the header */
	memcpy(header, contents + offset, SizeOfRelUndoRecordHeader);

	/* A zero urec_type means the slot was cancelled (hole) */
	if (header->urec_type == 0)
	{
		elog(DEBUG1, "RelUndoReadRecord: urec_type is 0 (cancelled/hole) at blk=%u offset=%u", blkno, offset);
		UnlockReleaseBuffer(buf);
		return false;
	}

	/* Calculate payload size and copy it */
	if (header->urec_len > SizeOfRelUndoRecordHeader)
	{
		psize = header->urec_len - SizeOfRelUndoRecordHeader;
		*payload = palloc(psize);
		memcpy(*payload, contents + offset + SizeOfRelUndoRecordHeader, psize);
		*payload_size = psize;
	}
	else
	{
		*payload = NULL;
		*payload_size = 0;
	}

	UnlockReleaseBuffer(buf);
	return true;
}

/*
 * RelUndoGetCurrentCounter
 *		Get current generation counter for a relation
 *
 * Reads the metapage and returns the current counter value.
 */
uint16
RelUndoGetCurrentCounter(Relation rel)
{
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	uint16		counter;

	metabuf = relundo_get_metapage(rel, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	counter = meta->counter;

	UnlockReleaseBuffer(metabuf);

	return counter;
}

/*
 * RelUndoInitRelation
 *		Initialize per-relation UNDO for a new relation
 *
 * Creates the UNDO fork and writes the initial metapage (block 0).
 * The chain starts empty (head_blkno = tail_blkno = InvalidBlockNumber).
 */
void
RelUndoInitRelation(Relation rel)
{
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	SMgrRelation srel;

	srel = RelationGetSmgr(rel);

	/*
	 * Create the physical fork file.  This is a no-op if it already exists
	 * (e.g., during recovery replay).
	 */
	smgrcreate(srel, RELUNDO_FORKNUM, false);

	/*
	 * Create the physical fork file and log it.
	 */
	if (!InRecovery)
		log_smgrcreate(&rel->rd_locator, RELUNDO_FORKNUM);

	/* Allocate the metapage (block 0) */
	metabuf = ExtendBufferedRel(BMR_REL(rel), RELUNDO_FORKNUM, NULL,
								EB_LOCK_FIRST);

	Assert(BufferGetBlockNumber(metabuf) == 0);

	metapage = BufferGetPage(metabuf);

	/* Initialize standard page header */
	PageInit(metapage, BLCKSZ, 0);

	/* Initialize the UNDO metapage fields */
	meta = (RelUndoMetaPage) PageGetContents(metapage);
	meta->magic = RELUNDO_METAPAGE_MAGIC;
	meta->version = RELUNDO_METAPAGE_VERSION;
	meta->counter = 1;			/* Start at 1 so 0 is clearly "no counter" */
	meta->head_blkno = InvalidBlockNumber;
	meta->tail_blkno = InvalidBlockNumber;
	meta->free_blkno = InvalidBlockNumber;
	meta->total_records = 0;
	meta->discarded_records = 0;

	MarkBufferDirty(metabuf);

	/*
	 * WAL-log the metapage initialization. This is critical for crash safety.
	 * If we crash after table creation but before the first INSERT, the
	 * metapage must be recoverable.
	 */
	if (!InRecovery)
	{
		xl_relundo_init xlrec;
		XLogRecPtr	recptr;

		xlrec.magic = RELUNDO_METAPAGE_MAGIC;
		xlrec.version = RELUNDO_METAPAGE_VERSION;
		xlrec.counter = 1;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfRelundoInit);
		XLogRegisterBuffer(0, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

		recptr = XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_INIT);

		PageSetLSN(metapage, recptr);
	}

	UnlockReleaseBuffer(metabuf);
}

/*
 * RelUndoDropRelation
 *		Drop per-relation UNDO when relation is dropped
 *
 * The UNDO fork is removed along with the relation's other forks by the
 * storage manager.  We just need to make sure we don't leave stale state.
 */
void
RelUndoDropRelation(Relation rel)
{
	SMgrRelation srel;

	srel = RelationGetSmgr(rel);

	/*
	 * If the UNDO fork doesn't exist, nothing to do.  This handles the case
	 * where the relation never had per-relation UNDO enabled.
	 */
	if (!smgrexists(srel, RELUNDO_FORKNUM))
		return;

	/*
	 * The actual file removal happens as part of the relation's overall drop
	 * via smgrdounlinkall().  We don't need to explicitly drop the fork here
	 * because the storage manager handles all forks together.
	 *
	 * If in the future we need explicit fork removal, we could truncate and
	 * unlink here.
	 */
}

/*
 * RelUndoVacuum
 *		Vacuum per-relation UNDO log
 *
 * Discards old UNDO records that are no longer needed for visibility
 * checks.  Currently we use a simple heuristic: the counter from the
 * metapage minus a safety margin gives the discard cutoff.
 *
 * A more sophisticated implementation would track the oldest active
 * snapshot's counter value.
 */
void
RelUndoVacuum(Relation rel, TransactionId oldest_xmin)
{
	Buffer		metabuf;
	Page		metapage;
	RelUndoMetaPage meta;
	uint16		current_counter;
	uint16		oldest_visible_counter;

	/* If no UNDO fork exists, nothing to vacuum */
	if (!smgrexists(RelationGetSmgr(rel), RELUNDO_FORKNUM))
		return;

	metabuf = relundo_get_metapage(rel, BUFFER_LOCK_SHARE);
	metapage = BufferGetPage(metabuf);
	meta = (RelUndoMetaPage) PageGetContents(metapage);

	current_counter = meta->counter;

	UnlockReleaseBuffer(metabuf);

	/*
	 * Simple heuristic: discard records more than 100 generations old. This
	 * is a conservative default; a real implementation would derive the
	 * cutoff from oldest_xmin and transaction-to-counter mappings.
	 */
	if (current_counter > 100)
		oldest_visible_counter = current_counter - 100;
	else
		oldest_visible_counter = 1;

	RelUndoDiscard(rel, oldest_visible_counter);
}
