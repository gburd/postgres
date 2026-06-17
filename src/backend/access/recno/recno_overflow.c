/*-------------------------------------------------------------------------
 *
 * recno_overflow.c
 *	  RECNO column-level overflow storage
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_overflow.c
 *
 * NOTES
 *	  This implements column-level overflow for large attribute values.
 *
 *	  Key design principles:
 *	  - Overflow data is stored as records on normal RECNO data pages,
 *	    not on dedicated overflow pages.  Pages can contain a mix of
 *	    normal tuples and overflow records.
 *	  - Each overflow record carries a lightweight header
 *	    (RecnoOverflowRecordHeader) without MVCC fields -- it shares
 *	    the visibility of the parent tuple.
 *	  - The main tuple stores a compact overflow pointer
 *	    (RecnoOverflowPtr) wrapped as a varlena, optionally followed
 *	    by an inline prefix of the original data.
 *	  - Overflow records chain via (BlockNumber, OffsetNumber) pairs.
 *	  - Free space management uses the same FSM as normal tuples.
 *	  - In-chain locality: consecutive overflow records in a chain are
 *	    placed on the same page when possible (tries prev_block first).
 *	  - Page reuse: RecnoFindOverflowPageForReuse() scans a head page
 *	    for existing overflow pages with free space before allocating new.
 *	  - VACUUM: RecnoVacuumOverflowRecords() uses a two-pass algorithm
 *	    to detect and remove orphaned overflow records.
 *
 *	  FUTURE ENHANCEMENTS (deferred):
 *
 *	  1. Lazy streaming pattern:
 *	     Currently RecnoFetchOverflowColumn() eagerly materializes the
 *	     entire column value into memory.  For very large BLOBs (hundreds
 *	     of MB), this is not viable.  A streaming interface would return a
 *	     custom varlena wrapper that fetches overflow pages on demand,
 *	     releasing page pins after each chunk is consumed.  This would
 *	     require integration with PostgreSQL's VARATT_EXTERNAL infrastructure
 *	     or a custom external varlena pointer type for RECNO.
 *
 *	  2. Row-level overflow:
 *	     When a row's total serialized size exceeds page capacity even after
 *	     column-level overflow, the row's fields could be split across multiple
 *	     pages using continuation pointers.  This would require a new flag
 *	     (RECNO_TUPLE_HAS_ROW_OVERFLOW), a RecnoRowOverflowPtr structure
 *	     with (next_block, next_offset, first_column), and changes to all
 *	     scan, update, and delete paths to follow row continuations.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/recno.h"
#include "access/recno_xlog.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/*
 * Overflow constants
 */
#define RECNO_MAX_OVERFLOW_CHAIN	1024	/* Safety limit on chain length */
#define RECNO_OVERFLOW_REUSE_SCAN	5	/* Max pages to check for reuse */

/*
 * GUC variable for inline prefix size
 */
int			recno_overflow_inline_prefix = RECNO_OVERFLOW_DEFAULT_PREFIX;

/*
 * RecnoFindOverflowPageForReuse
 *
 * Overflow page reuse strategy: before allocating a new page
 * for an overflow record, scan up to RECNO_OVERFLOW_REUSE_SCAN existing
 * overflow pages referenced by other tuples on the given head page.  If any
 * of those pages have enough free space, return its block number.
 *
 * This reduces the total number of pages in the relation by sharing overflow
 * space across multiple tuples from the same head page.
 *
 * Parameters:
 *   rel        - the relation
 *   head_page  - the head page whose tuples we scan for overflow pointers
 *   needed     - minimum PageGetFreeSpace() value required (MAXALIGN(record_size))
 *
 * Returns a suitable block number, or InvalidBlockNumber if no existing
 * overflow page has enough room.
 */
BlockNumber
RecnoFindOverflowPageForReuse(Relation rel, Page head_page, Size needed)
{
	OffsetNumber maxoff;
	OffsetNumber offnum;
	BlockNumber candidates[RECNO_OVERFLOW_REUSE_SCAN];
	int			ncandidates = 0;
	int			i;

	maxoff = PageGetMaxOffsetNumber(head_page);

	/*
	 * Scan the head page's slot table for tuples that have overflow pointers.
	 * Collect the first block of each overflow chain as a candidate for
	 * reuse.
	 */
	for (offnum = FirstOffsetNumber; offnum <= maxoff && ncandidates < RECNO_OVERFLOW_REUSE_SCAN; offnum++)
	{
		ItemId		itemid = PageGetItemId(head_page, offnum);
		RecnoTupleHeader *tuple_hdr;
		uint8	   *nulls_bitmap;
		char	   *data_ptr;
		Size		bitmap_len;
		TupleDesc	tupdesc;
		int			att_idx;
		bool		already_have;

		if (!ItemIdIsNormal(itemid))
			continue;

		tuple_hdr = (RecnoTupleHeader *) PageGetItem(head_page, itemid);

		/* Skip overflow records themselves */
		if (RecnoIsOverflowRecord(tuple_hdr, ItemIdGetLength(itemid)))
			continue;

		/* Only interested in tuples that have overflow pointers */
		if (!(tuple_hdr->t_flags & RECNO_TUPLE_HAS_OVERFLOW))
			continue;

		/*
		 * Walk the tuple's varlena attributes to find overflow pointers. We
		 * only need the first block of each chain as a candidate.
		 */
		tupdesc = RelationGetDescr(rel);
		bitmap_len = BITMAPLEN(tupdesc->natts);
		nulls_bitmap = (uint8 *) tuple_hdr->t_attrs_bitmap;
		data_ptr = (char *) tuple_hdr + RECNO_TUPLE_OVERHEAD + MAXALIGN(bitmap_len);

		for (att_idx = 0; att_idx < tupdesc->natts && ncandidates < RECNO_OVERFLOW_REUSE_SCAN; att_idx++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, att_idx);

			if (att->attisdropped)
				continue;

			if ((tuple_hdr->t_infomask & RECNO_INFOMASK_HASNULL) &&
				att_isnull(att_idx, nulls_bitmap))
				continue;

			data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);

			if (att->attlen == -1)
			{
				Size		attr_len = VARSIZE_ANY(data_ptr);

				if (RecnoIsOverflowPtr(data_ptr))
				{
					const RecnoOverflowPtr *ovp = RecnoGetOverflowPtr(data_ptr);
					BlockNumber cand = ovp->ov_first_block;

					/* Avoid duplicates */
					already_have = false;
					for (i = 0; i < ncandidates; i++)
					{
						if (candidates[i] == cand)
						{
							already_have = true;
							break;
						}
					}
					if (!already_have && cand != InvalidBlockNumber)
						candidates[ncandidates++] = cand;
				}
				data_ptr += attr_len;
			}
			else if (att->attlen > 0)
				data_ptr += att->attlen;
			else if (att->attlen == -2)
				data_ptr += strlen(data_ptr) + 1;
		}
	}

	/*
	 * Check each candidate page for sufficient free space.
	 */
	for (i = 0; i < ncandidates; i++)
	{
		Buffer		buf;
		Page		cand_page;
		Size		free_space;

		buf = ReadBuffer(rel, candidates[i]);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		cand_page = BufferGetPage(buf);
		free_space = PageGetFreeSpace(cand_page);
		UnlockReleaseBuffer(buf);

		if (free_space >= needed)
			return candidates[i];
	}

	return InvalidBlockNumber;
}

/*
 * Store a large column value in overflow records on normal data pages.
 *
 * Returns a varlena Datum containing [RecnoOverflowPtr][inline_prefix].
 * The caller replaces the original column value with this in the tuple.
 *
 * The inline_prefix_size parameter controls how many leading bytes of
 * the original value are kept inline for prefix-based operations (e.g.,
 * LIKE 'prefix%' or B-tree comparison without fetching overflow data).
 *
 * Key features:
 * - Overflow page reuse: tries existing overflow pages before FSM
 * - Abort-safe: tracks all overflow records so that on transaction
 *   abort, orphaned overflow records can be cleaned up by VACUUM
 */
Datum
RecnoStoreOverflowColumn(Relation rel, Datum value, int attnum,
						 Size inline_prefix_size,
						 RecnoOverflowBuffers *overflow_buffers)
{
	char	   *data_ptr;
	Size		data_len;
	Size		remaining;
	Size		prefix_len;
	BlockNumber first_block = InvalidBlockNumber;
	OffsetNumber first_offset = InvalidOffsetNumber;
	BlockNumber prev_block = InvalidBlockNumber;
	OffsetNumber prev_offset = InvalidOffsetNumber;
	int			chain_count = 0;
	char	   *result;
	Size		result_size;
	RecnoOverflowPtr *ovp;
	int			fsm_retry_count = 0;
	const int	MAX_FSM_RETRIES = 100;

	/* Extract raw data from the varlena */
	data_ptr = VARDATA_ANY(DatumGetPointer(value));
	data_len = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

	/* Determine actual inline prefix length */
	prefix_len = Min(inline_prefix_size, data_len);

	remaining = data_len;

	/*
	 * Store data across overflow records.  Each record is placed on a normal
	 * data page using PageAddItem, found via overflow page reuse or FSM.
	 *
	 * Track FSM retries to detect potential infinite loops caused by FSM
	 * corruption after crash recovery.  If we hit too many stale entries,
	 * error out with a diagnostic message.
	 */
	while (remaining > 0)
	{
		Buffer		buffer;
		Page		page;
		BlockNumber target_block;
		Size		chunk_size;
		Size		record_size;
		RecnoOverflowRecordHeader *rec_hdr;
		char	   *record_data;
		OffsetNumber offnum;
		int			i;

		/* Calculate how much data fits in one overflow record */
		chunk_size = Min(remaining, RECNO_OVERFLOW_MAX_CHUNK_SIZE);
		record_size = RECNO_OVERFLOW_RECORD_OVERHEAD + chunk_size;

		/*
		 * Spatial locality optimization: for subsequent overflow records in
		 * the same chain, try the same page as the previous record first.
		 * This packs overflow chain records onto fewer pages, improving
		 * sequential read performance when fetching the chain.
		 *
		 * For the first record in the chain, we go straight to the FSM.
		 */
		target_block = InvalidBlockNumber;

		/*
		 * The space needed for PageAddItem is MAXALIGN(record_size) for the
		 * tuple data plus sizeof(ItemIdData) for the line pointer.
		 * PageGetFreeSpace() already subtracts one sizeof(ItemIdData) from
		 * the raw free space, so we compare against MAXALIGN(record_size).
		 */
		if (prev_block != InvalidBlockNumber)
		{
			Buffer		prev_buf = InvalidBuffer;
			Page		prev_pg;
			Size		prev_free;
			bool		found_in_cache = false;
			int			j;

			/*
			 * Check if prev_block is already in overflow_buffers (locked
			 * EXCLUSIVE). If so, we can check its free space directly without
			 * locking again.
			 */
			if (overflow_buffers != NULL)
			{
				for (j = 0; j < overflow_buffers->count; j++)
				{
					if (BufferGetBlockNumber(overflow_buffers->buffers[j].buffer) == prev_block)
					{
						prev_buf = overflow_buffers->buffers[j].buffer;
						found_in_cache = true;
						break;
					}
				}
			}

			if (!found_in_cache)
			{
				/* Not in cache, need to read and lock it */
				prev_buf = ReadBuffer(rel, prev_block);
				LockBuffer(prev_buf, BUFFER_LOCK_SHARE);
			}

			prev_pg = BufferGetPage(prev_buf);
			prev_free = PageGetFreeSpace(prev_pg);

			if (!found_in_cache)
				UnlockReleaseBuffer(prev_buf);

			if (prev_free >= MAXALIGN(record_size))
				target_block = prev_block;
		}

		/* Fall back to FSM if no reuse candidate found */
		if (target_block == InvalidBlockNumber)
			target_block = RecnoGetPageWithFreeSpace(rel, MAXALIGN(record_size));

		if (target_block == InvalidBlockNumber)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("could not allocate space for overflow record")));

		/*
		 * Check if this page is already in overflow_buffers (from a previous
		 * iteration in this loop). If so, reuse that buffer to avoid
		 * double-locking.
		 */
		buffer = InvalidBuffer;
		if (overflow_buffers != NULL)
		{
			for (i = 0; i < overflow_buffers->count; i++)
			{
				if (BufferGetBlockNumber(overflow_buffers->buffers[i].buffer) == target_block)
				{
					buffer = overflow_buffers->buffers[i].buffer;
					break;
				}
			}
		}

		/* Read and lock the buffer if not already held */
		if (!BufferIsValid(buffer))
		{
			buffer = ReadBuffer(rel, target_block);
		}

		/*
		 * WORKAROUND: If target_block is 0 and this is the first overflow
		 * record, this is probably the main tuple's page. The main page might
		 * be pinned (but not locked) by the caller. Skip it and get a
		 * different page.
		 *
		 * We need to properly release the buffer we just read before getting
		 * a new one.
		 */
		if (target_block == 0 && overflow_buffers != NULL && overflow_buffers->count == 0)
		{
			/*
			 * Release the buffer we just acquired - it's pinned but not
			 * locked
			 */
			ReleaseBuffer(buffer);
			buffer = InvalidBuffer;

			/* Get a different page from FSM */
			target_block = RecnoGetPageWithFreeSpace(rel, MAXALIGN(record_size));

			if (target_block == InvalidBlockNumber || target_block == 0)
			{
				/* Extend the relation to get a new page */
				buffer = ReadBufferExtended(rel, MAIN_FORKNUM, P_NEW,
											RBM_NORMAL, NULL);
				target_block = BufferGetBlockNumber(buffer);
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
				RecnoInitPage(BufferGetPage(buffer), BufferGetPageSize(buffer));
			}
			else
			{
				buffer = ReadBuffer(rel, target_block);
			}
		}

		/* Now lock the buffer */
		if (!BufferIsLockedByMeInMode(buffer, BUFFER_LOCK_EXCLUSIVE))
		{
			LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		}
		page = BufferGetPage(buffer);

		/* Verify space is available */
		if (PageGetFreeSpace(page) < MAXALIGN(record_size))
		{
			/*
			 * FSM was stale, update and retry.
			 *
			 * Force FSM tree propagation via FreeSpaceMapVacuumRange so that
			 * the next GetPageWithFreeSpace call sees the corrected value.
			 * Without this, GetPageWithFreeSpace might return the same stale
			 * block repeatedly after multiple crash/recovery cycles when the
			 * FSM tree structure hasn't been updated.
			 */
			fsm_retry_count++;
			if (fsm_retry_count > MAX_FSM_RETRIES)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("too many FSM retry attempts (%d) while storing overflow data",
								fsm_retry_count),
						 errdetail("Overflow data size: %zu bytes, %d chunks processed, %zu bytes remaining",
								   data_len, chain_count, remaining),
						 errhint("FSM may be corrupted after crash recovery. Try VACUUM or REINDEX.")));

			RecnoRecordFreeSpace(rel, target_block, PageGetFreeSpace(page));
			UnlockReleaseBuffer(buffer);

			FreeSpaceMapVacuumRange(rel, target_block, target_block + 1);

			target_block = RecnoGetPageWithFreeSpace(rel, MAXALIGN(record_size));
			if (target_block == InvalidBlockNumber)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("could not allocate space for overflow record after retry")));

			/* Check if this page is already in overflow_buffers */
			buffer = InvalidBuffer;
			if (overflow_buffers != NULL)
			{
				for (i = 0; i < overflow_buffers->count; i++)
				{
					if (BufferGetBlockNumber(overflow_buffers->buffers[i].buffer) == target_block)
					{
						buffer = overflow_buffers->buffers[i].buffer;
						break;
					}
				}
			}

			/* Read and lock the buffer if not already held */
			if (!BufferIsValid(buffer))
			{
				buffer = ReadBuffer(rel, target_block);
			}

			/*
			 * Lock buffer only if we don't already hold the lock (retry
			 * path).
			 */
			if (!BufferIsLockedByMeInMode(buffer, BUFFER_LOCK_EXCLUSIVE))
			{
				LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
			}
			page = BufferGetPage(buffer);

			if (PageGetFreeSpace(page) < MAXALIGN(record_size))
				elog(ERROR, "page still has insufficient space for overflow record");
		}

		/* Build the overflow record in temporary memory */
		record_data = (char *) palloc0(record_size);
		rec_hdr = (RecnoOverflowRecordHeader *) record_data;
		rec_hdr->or_magic = RECNO_OVERFLOW_RECORD_MAGIC;
		rec_hdr->or_data_len = (uint32) chunk_size;
		rec_hdr->or_next_block = InvalidBlockNumber;
		rec_hdr->or_next_offset = InvalidOffsetNumber;
		rec_hdr->or_flags = 0;

		/* Copy chunk data after the header */
		memcpy(record_data + RECNO_OVERFLOW_RECORD_OVERHEAD,
			   data_ptr + (data_len - remaining),
			   chunk_size);

		/* Add the overflow record to the page */
		START_CRIT_SECTION();

		offnum = PageAddItem(page, record_data, record_size,
							 InvalidOffsetNumber, false, false);

		if (offnum == InvalidOffsetNumber)
		{
			END_CRIT_SECTION();
			pfree(record_data);
			UnlockReleaseBuffer(buffer);
			elog(ERROR, "failed to add overflow record to page");
		}

		/* Remember the first record's location */
		if (first_block == InvalidBlockNumber)
		{
			first_block = target_block;
			first_offset = offnum;
		}

		/*
		 * Link previous record to this one by updating the previous record's
		 * continuation pointer.
		 */
		if (prev_block != InvalidBlockNumber)
		{
			Buffer		prev_buffer;
			Page		prev_page;
			ItemId		prev_itemid;
			RecnoOverflowRecordHeader *prev_hdr;

			if (prev_block == target_block)
			{
				/* Same page - update in place */
				prev_itemid = PageGetItemId(page, prev_offset);
				prev_hdr = (RecnoOverflowRecordHeader *) PageGetItem(page, prev_itemid);
				prev_hdr->or_next_block = target_block;
				prev_hdr->or_next_offset = offnum;
				/* Page already dirty from our insert */
			}
			else
			{
				/*
				 * Different page - need to read and update.
				 *
				 * NOTE: prev_buffer should already be in overflow_buffers
				 * from the previous iteration. We just need to update the
				 * in-memory header. The caller will WAL-log all buffers
				 * atomically.
				 */
				bool		found = false;

				/* Find prev_buffer in overflow_buffers */
				if (overflow_buffers != NULL)
				{
					for (i = 0; i < overflow_buffers->count; i++)
					{
						if (BufferGetBlockNumber(overflow_buffers->buffers[i].buffer) == prev_block)
						{
							/* Update the cached record_data */
							RecnoOverflowRecordHeader *cached_hdr =
								(RecnoOverflowRecordHeader *) overflow_buffers->buffers[i].record_data;

							cached_hdr->or_next_block = target_block;
							cached_hdr->or_next_offset = offnum;

							/*
							 * Also update the on-page version. Buffer is
							 * still locked from when it was stored in
							 * overflow_buffers.
							 */
							prev_buffer = overflow_buffers->buffers[i].buffer;
							prev_page = BufferGetPage(prev_buffer);
							prev_itemid = PageGetItemId(prev_page, prev_offset);
							prev_hdr = (RecnoOverflowRecordHeader *) PageGetItem(prev_page, prev_itemid);
							prev_hdr->or_next_block = target_block;
							prev_hdr->or_next_offset = offnum;
							MarkBufferDirty(prev_buffer);

							found = true;
							break;
						}
					}
				}

				if (!found)
				{
					/*
					 * Fall back to immediate WAL logging if not tracking
					 * buffers. This shouldn't happen in normal operation.
					 */
					prev_buffer = ReadBuffer(rel, prev_block);
					LockBuffer(prev_buffer, BUFFER_LOCK_EXCLUSIVE);
					prev_page = BufferGetPage(prev_buffer);

					prev_itemid = PageGetItemId(prev_page, prev_offset);
					prev_hdr = (RecnoOverflowRecordHeader *) PageGetItem(prev_page, prev_itemid);
					prev_hdr->or_next_block = target_block;
					prev_hdr->or_next_offset = offnum;

					MarkBufferDirty(prev_buffer);

					if (RelationNeedsWAL(rel))
					{
						XLogRecPtr	recptr;

						recptr = RecnoXLogOverflowWrite(rel, prev_buffer, prev_offset,
														(char *) prev_hdr,
														sizeof(RecnoOverflowRecordHeader),
														RECNO_OVERFLOW_WAL_LINK_UPDATE,
														RecnoGetCommitTimestamp());
						PageSetLSN(prev_page, recptr);
					}

					UnlockReleaseBuffer(prev_buffer);
				}
			}
		}

		MarkBufferDirty(buffer);

		/*
		 * DO NOT WAL-log or release buffer here. Instead, collect buffer info
		 * for atomic logging by the caller. This ensures the main tuple
		 * UPDATE and all overflow records are logged in a single atomic WAL
		 * record, preventing orphaned overflow pages after crash recovery.
		 *
		 * IMPORTANT: Keep buffers LOCKED. XLogRegisterBuffer expects buffers
		 * to be locked, and XLogInsert will unlock them after WAL is written.
		 */
		if (overflow_buffers != NULL &&
			overflow_buffers->count < MAX_OVERFLOW_BUFFERS)
		{
			RecnoOverflowBuffer *ovb = &overflow_buffers->buffers[overflow_buffers->count];

			ovb->buffer = buffer;
			ovb->offset = offnum;
			ovb->record_data = record_data; /* Caller must pfree this later */
			ovb->record_len = record_size;
			ovb->flags = RECNO_OVERFLOW_WAL_NEW_RECORD;
			overflow_buffers->count++;

			/* Keep buffer pinned AND LOCKED for caller to WAL-log and release */
		}
		else
		{
			/* No overflow tracking - fall back to immediate WAL logging */
			if (RelationNeedsWAL(rel))
			{
				XLogRecPtr	recptr;

				recptr = RecnoXLogOverflowWrite(rel, buffer, offnum,
												record_data, record_size,
												RECNO_OVERFLOW_WAL_NEW_RECORD,
												RecnoGetCommitTimestamp());
				PageSetLSN(page, recptr);
			}
			UnlockReleaseBuffer(buffer);
			pfree(record_data);
		}

		END_CRIT_SECTION();

		/* Update FSM */
		RecnoRecordFreeSpace(rel, target_block, PageGetFreeSpace(page));

		/* Advance */
		prev_block = target_block;
		prev_offset = offnum;
		remaining -= chunk_size;
		chain_count++;

		/* Safety check */
		if (chain_count > RECNO_MAX_OVERFLOW_CHAIN)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("attribute value requires %d overflow records, exceeds maximum chain length",
							chain_count)));
	}

	/*
	 * Build the inline overflow pointer varlena:
	 * [VARHDRSZ][RecnoOverflowPtr][prefix_bytes...]
	 */
	result_size = VARHDRSZ + sizeof(RecnoOverflowPtr) + prefix_len;
	result = (char *) palloc0(result_size);
	SET_VARSIZE(result, result_size);

	ovp = (RecnoOverflowPtr *) VARDATA(result);
	ovp->ov_magic = RECNO_OVERFLOW_PTR_MAGIC;
	ovp->ov_first_block = first_block;
	ovp->ov_first_offset = first_offset;
	ovp->ov_padding = 0;
	ovp->ov_total_length = (uint32) data_len;
	ovp->ov_inline_prefix = (uint16) prefix_len;
	ovp->ov_flags = 0;

	/* Copy inline prefix data after the pointer struct */
	if (prefix_len > 0)
		memcpy((char *) ovp + sizeof(RecnoOverflowPtr), data_ptr, prefix_len);

	return PointerGetDatum(result);
}

/*
 * Fetch a column value from overflow records.
 *
 * Given a varlena containing a RecnoOverflowPtr, follow the overflow chain
 * and reconstruct the complete original varlena value.
 */
Datum
RecnoFetchOverflowColumn(Relation rel, const void *overflow_varlena)
{
	const RecnoOverflowPtr *ovp;
	char	   *result_data;
	char	   *result_ptr;
	Size		total_len;
	BlockNumber cur_block;
	OffsetNumber cur_offset;
	Size		bytes_read = 0;
	int			chain_count = 0;
	Buffer		current_buffer = InvalidBuffer;
	BlockNumber current_block = InvalidBlockNumber;

	if (!RecnoIsOverflowPtr(overflow_varlena))
		elog(ERROR, "RecnoFetchOverflowColumn called on non-overflow datum");

	ovp = RecnoGetOverflowPtr(overflow_varlena);
	total_len = ovp->ov_total_length;

	/* Allocate result buffer as a proper varlena */
	result_data = (char *) palloc(VARHDRSZ + total_len);
	SET_VARSIZE(result_data, VARHDRSZ + total_len);
	result_ptr = VARDATA(result_data);

	cur_block = ovp->ov_first_block;
	cur_offset = ovp->ov_first_offset;

	/*
	 * Follow the overflow chain.
	 *
	 * We keep the buffer pinned throughout the chain traversal to handle
	 * spatial locality where multiple overflow records can reside on the same
	 * page. Pattern: lock -> process -> unlock (keep pin) -> re-lock same or
	 * different page -> ... -> finally release pin.
	 */
	while (cur_block != InvalidBlockNumber && bytes_read < total_len)
	{
		Buffer		buffer;
		Page		page;
		ItemId		itemid;
		RecnoOverflowRecordHeader *rec_hdr;
		Size		copy_len;

		/*
		 * Check if we need to access a different page than what we currently
		 * have pinned. If same page, reuse the buffer; otherwise release the
		 * old buffer and read the new one.
		 */
		if (cur_block != current_block)
		{
			/* Different page - release previous buffer if any */
			if (BufferIsValid(current_buffer))
			{
				ReleaseBuffer(current_buffer);
				current_buffer = InvalidBuffer;
			}

			/* Read and pin the new buffer */
			buffer = ReadBuffer(rel, cur_block);
			current_buffer = buffer;
			current_block = cur_block;
		}
		else
		{
			/* Same page - reuse current buffer */
			buffer = current_buffer;
		}

		/* Lock the buffer for reading */
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);


		/* Validate offset */
		if (cur_offset < FirstOffsetNumber ||
			cur_offset > PageGetMaxOffsetNumber(page))
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			if (BufferIsValid(current_buffer))
				ReleaseBuffer(current_buffer);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid overflow record offset %u on block %u",
							cur_offset, cur_block)));
		}

		itemid = PageGetItemId(page, cur_offset);
		if (!ItemIdIsNormal(itemid))
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			if (BufferIsValid(current_buffer))
				ReleaseBuffer(current_buffer);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("overflow record at (%u,%u) is not a normal item",
							cur_block, cur_offset)));
		}

		rec_hdr = (RecnoOverflowRecordHeader *) PageGetItem(page, itemid);

		/* Validate it's an overflow record */
		if (rec_hdr->or_magic != RECNO_OVERFLOW_RECORD_MAGIC)
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			if (BufferIsValid(current_buffer))
				ReleaseBuffer(current_buffer);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("expected overflow record at (%u,%u), found magic 0x%08x",
							cur_block, cur_offset, rec_hdr->or_magic)));
		}

		/* Copy data chunk */
		copy_len = Min(rec_hdr->or_data_len, total_len - bytes_read);
		memcpy(result_ptr + bytes_read,
			   (char *) rec_hdr + RECNO_OVERFLOW_RECORD_OVERHEAD,
			   copy_len);
		bytes_read += copy_len;

		/* Follow chain */
		cur_block = rec_hdr->or_next_block;
		cur_offset = rec_hdr->or_next_offset;

		chain_count++;
		if (chain_count > RECNO_MAX_OVERFLOW_CHAIN)
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			if (BufferIsValid(current_buffer))
				ReleaseBuffer(current_buffer);
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("overflow chain exceeded maximum length")));
		}

		/*
		 * Unlock the buffer but keep it pinned. We may need it again if the
		 * next overflow record is on the same page (spatial locality
		 * optimization). The pin will be released when we move to a different
		 * page or finish the chain.
		 */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	}

	/* Release the final buffer */
	if (BufferIsValid(current_buffer))
		ReleaseBuffer(current_buffer);

	if (bytes_read != total_len)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("incomplete overflow read: expected %zu bytes, got %zu",
						total_len, bytes_read)));

	return PointerGetDatum(result_data);
}

/*
 * Delete an overflow chain starting at the given location.
 *
 * Follows the chain and removes each overflow record from its page,
 * freeing the space for reuse.
 */
void
RecnoDeleteOverflowChain(Relation rel, BlockNumber first_block,
						 OffsetNumber first_offset)
{
	BlockNumber cur_block = first_block;
	OffsetNumber cur_offset = first_offset;
	int			chain_count = 0;
	Buffer		current_buffer = InvalidBuffer;
	BlockNumber current_block = InvalidBlockNumber;

	/*
	 * Keep buffer pinned throughout chain traversal to handle spatial
	 * locality where multiple overflow records reside on same page.
	 */
	while (cur_block != InvalidBlockNumber)
	{
		Buffer		buffer;
		Page		page;
		ItemId		itemid;
		RecnoOverflowRecordHeader *rec_hdr;
		BlockNumber next_block;
		OffsetNumber next_offset;

		/*
		 * Check if we need a different page. If same page, reuse the buffer;
		 * otherwise release old buffer and read new one.
		 */
		if (cur_block != current_block)
		{
			/* Different page - release previous buffer if any */
			if (BufferIsValid(current_buffer))
			{
				ReleaseBuffer(current_buffer);
				current_buffer = InvalidBuffer;
			}

			/* Read and pin the new buffer */
			buffer = ReadBuffer(rel, cur_block);
			current_buffer = buffer;
			current_block = cur_block;
		}
		else
		{
			/* Same page - reuse current buffer */
			buffer = current_buffer;
		}

		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);

		if (cur_offset < FirstOffsetNumber ||
			cur_offset > PageGetMaxOffsetNumber(page))
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			break;
		}

		itemid = PageGetItemId(page, cur_offset);
		if (!ItemIdIsNormal(itemid))
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			break;
		}

		rec_hdr = (RecnoOverflowRecordHeader *) PageGetItem(page, itemid);

		if (rec_hdr->or_magic != RECNO_OVERFLOW_RECORD_MAGIC)
		{
			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
			break;
		}

		/* Save next pointers before we remove */
		next_block = rec_hdr->or_next_block;
		next_offset = rec_hdr->or_next_offset;

		/*
		 * Remove the item from the page.  Use RecnoPageIndexTupleDelete
		 * instead of PageIndexTupleDelete because the page may contain
		 * LP_UNUSED items from defragmentation.
		 */
		RecnoPageIndexTupleDelete(page, cur_offset);
		MarkBufferDirty(buffer);

		/*
		 * Note: We do NOT WAL-log individual overflow deletions.  Overflow
		 * cleanup is an idempotent operation that can be safely deferred. The
		 * parent tuple's modification is already WAL-logged, which ensures
		 * consistency.  If we crash before overflow cleanup completes, the
		 * orphaned overflow records will be cleaned up by VACUUM.
		 */

		/* Update FSM */
		RecnoRecordFreeSpace(rel, cur_block, PageGetFreeSpace(page));

		/*
		 * Unlock buffer but keep it pinned. May need it again if next
		 * overflow record is on the same page (spatial locality).
		 */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

		cur_block = next_block;
		cur_offset = next_offset;

		chain_count++;
		if (chain_count > RECNO_MAX_OVERFLOW_CHAIN)
			break;
	}

	/* Release the final buffer */
	if (BufferIsValid(current_buffer))
		ReleaseBuffer(current_buffer);
}

/*
 * Collect overflow chain starting locations from a tuple.
 *
 * Scans the tuple's varlena attributes looking for overflow pointers
 * and stores their (BlockNumber, OffsetNumber) pairs in the caller's
 * arrays.  Returns the number of overflow pointers found.
 *
 * This allows the caller to release any buffer lock before deleting
 * overflow chains, avoiding lock conflicts when an overflow chain
 * starts on the same page as the parent tuple.
 */
int
RecnoCollectOverflowPtrs(RecnoTupleHeader *tuple_hdr, TupleDesc tupdesc,
						 BlockNumber *blocks, OffsetNumber *offsets,
						 int max_ptrs)
{
	uint8	   *nulls_bitmap;
	char	   *data_ptr;
	Size		bitmap_len;
	int			natts;
	int			i;
	int			found = 0;

	/* Quick check: if tuple doesn't have overflow, nothing to do */
	if (!(tuple_hdr->t_flags & RECNO_TUPLE_HAS_OVERFLOW))
		return 0;

	bitmap_len = BITMAPLEN(tupdesc->natts);
	nulls_bitmap = (uint8 *) tuple_hdr->t_attrs_bitmap;
	data_ptr = (char *) tuple_hdr + RECNO_TUPLE_OVERHEAD + MAXALIGN(bitmap_len);
	natts = tupdesc->natts;

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		/* Check null bitmap */
		if ((tuple_hdr->t_infomask & RECNO_INFOMASK_HASNULL) &&
			att_isnull(i, nulls_bitmap))
			continue;

		/* Align */
		data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);

		if (att->attlen == -1)
		{
			/* Variable-length: check if it's an overflow pointer */
			Size		attr_len = VARSIZE_ANY(data_ptr);

			if (RecnoIsOverflowPtr(data_ptr) && found < max_ptrs)
			{
				const RecnoOverflowPtr *ovp = RecnoGetOverflowPtr(data_ptr);

				blocks[found] = ovp->ov_first_block;
				offsets[found] = ovp->ov_first_offset;
				found++;
			}
			data_ptr += attr_len;
		}
		else if (att->attlen > 0)
		{
			data_ptr += att->attlen;
		}
		else if (att->attlen == -2)
		{
			data_ptr += strlen(data_ptr) + 1;
		}
	}

	return found;
}

/*
 * Delete all overflow chains referenced by a tuple.
 *
 * Scans the tuple's varlena attributes looking for overflow pointers
 * and deletes each overflow chain found.
 *
 * WARNING: The caller must NOT hold a buffer lock on any page that
 * might contain overflow records for this tuple, because this function
 * acquires EXCLUSIVE locks on overflow pages internally.
 */
void
RecnoDeleteTupleOverflows(Relation rel, RecnoTupleHeader *tuple_hdr,
						  TupleDesc tupdesc)
{
	uint8	   *nulls_bitmap;
	char	   *data_ptr;
	Size		bitmap_len;
	int			i;

	/* Quick check: if tuple doesn't have overflow, nothing to do */
	if (!(tuple_hdr->t_flags & RECNO_TUPLE_HAS_OVERFLOW))
		return;

	bitmap_len = BITMAPLEN(tupdesc->natts);
	nulls_bitmap = (uint8 *) tuple_hdr->t_attrs_bitmap;
	data_ptr = (char *) tuple_hdr + RECNO_TUPLE_OVERHEAD + MAXALIGN(bitmap_len);

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;

		/* Check null bitmap */
		if ((tuple_hdr->t_infomask & RECNO_INFOMASK_HASNULL) &&
			att_isnull(i, nulls_bitmap))
			continue;

		/* Align */
		data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);

		if (att->attlen == -1)
		{
			/* Variable-length: check if it's an overflow pointer */
			Size		attr_len = VARSIZE_ANY(data_ptr);

			if (RecnoIsOverflowPtr(data_ptr))
			{
				const RecnoOverflowPtr *ovp = RecnoGetOverflowPtr(data_ptr);

				RecnoDeleteOverflowChain(rel, ovp->ov_first_block,
										 ovp->ov_first_offset);
			}
			data_ptr += attr_len;
		}
		else if (att->attlen > 0)
		{
			data_ptr += att->attlen;
		}
		else if (att->attlen == -2)
		{
			data_ptr += strlen(data_ptr) + 1;
		}
	}
}

/*
 * Check if a page item is an overflow record (not a normal tuple).
 *
 * Used by sequential scan to skip overflow records when scanning pages.
 */
bool
RecnoIsOverflowRecord(const void *item, Size item_len)
{
	const RecnoOverflowRecordHeader *hdr;

	if (item_len < sizeof(RecnoOverflowRecordHeader))
		return false;

	hdr = (const RecnoOverflowRecordHeader *) item;
	return hdr->or_magic == RECNO_OVERFLOW_RECORD_MAGIC;
}

/*
 * RecnoGetOverflowStats
 *
 * Scan the entire relation to count overflow records and compute overflow
 * space statistics.  This is a diagnostic/monitoring function that performs
 * a full sequential scan under shared buffer locks.
 *
 * Parameters (all are output):
 *   rel                    - open relation to examine
 *   total_overflow_records - total count of overflow record items found
 *   total_overflow_bytes   - total bytes consumed by overflow records
 *   avg_chain_length       - average chain length (overflow_records / chains,
 *                            or 0 if no overflow data exists)
 */
void
RecnoGetOverflowStats(Relation rel, int64 *total_overflow_records,
					  int64 *total_overflow_bytes, int64 *avg_chain_length)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	int64		overflow_records = 0;
	int64		overflow_bytes = 0;

	*total_overflow_records = 0;
	*total_overflow_bytes = 0;
	*avg_chain_length = 0;

	nblocks = RelationGetNumberOfBlocks(rel);

	for (blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber offnum;

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
									RBM_NORMAL, NULL);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buffer);
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);

		for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
		{
			ItemId		itemid = PageGetItemId(page, offnum);

			if (!ItemIdIsNormal(itemid))
				continue;

			if (RecnoIsOverflowRecord(PageGetItem(page, itemid),
									  ItemIdGetLength(itemid)))
			{
				RecnoOverflowRecordHeader *hdr =
					(RecnoOverflowRecordHeader *) PageGetItem(page, itemid);

				overflow_records++;
				overflow_bytes += hdr->or_data_len;
			}
		}

		UnlockReleaseBuffer(buffer);
	}

	*total_overflow_records = overflow_records;
	*total_overflow_bytes = overflow_bytes;

	/* Average chain length would require tracing chains; approximate */
	if (overflow_records > 0 && overflow_bytes > 0)
		*avg_chain_length = (overflow_bytes / RECNO_OVERFLOW_MAX_CHUNK_SIZE) + 1;
}

/*
 * RecnoVacuumOverflowRecords
 *
 * Remove orphaned overflow records that are not referenced by any live tuple.
 *
 * This implements a two-pass overflow chain cleanup approach:
 *
 * Pass 1: Scan all live tuples (non-deleted tuples with RECNO_TUPLE_HAS_OVERFLOW
 *         flag set) and collect the set of overflow record locations
 *         (block, offset) that are reachable from those tuples. We follow
 *         each overflow chain to collect all intermediate records too.
 *
 * Pass 2: Scan all pages for overflow records (identified by
 *         RECNO_OVERFLOW_RECORD_MAGIC). Any overflow record whose (block, offset)
 *         is NOT in the referenced set is an orphan and can be removed.
 *
 * This handles crash recovery scenarios where a tuple was deleted but the
 * overflow chain cleanup did not complete (e.g., crash between tuple deletion
 * and overflow chain deletion), as well as aborted transactions that left
 * overflow records behind.
 *
 * Parameters:
 *   rel - open relation (must hold appropriate lock)
 */
void
RecnoVacuumOverflowRecords(Relation rel)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	HTAB	   *referenced_overflow;
	HASHCTL		hashctl;
	int64		orphans_removed = 0;
	int64		overflow_records_found = 0;

	nblocks = RelationGetNumberOfBlocks(rel);
	if (nblocks == 0)
		return;

	/*
	 * Build a hash table of referenced overflow locations.  The key is a
	 * packed (BlockNumber, OffsetNumber) value.
	 */
	memset(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize = sizeof(uint64);
	hashctl.entrysize = sizeof(uint64); /* key-only, no payload */
	hashctl.hcxt = CurrentMemoryContext;

	referenced_overflow = hash_create("RecnoOverflowRefs",
									  256,	/* initial size estimate */
									  &hashctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/*
	 * Pass 1: Scan all pages to find live tuples with overflow pointers. For
	 * each such tuple, follow the overflow chain and record every overflow
	 * record location in the hash table.
	 */
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber offnum;

		CHECK_FOR_INTERRUPTS();

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
									RBM_NORMAL, NULL);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buffer);
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);

		for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
		{
			ItemId		itemid = PageGetItemId(page, offnum);
			RecnoTupleHeader *tuple_hdr;
			TupleDesc	tupdesc;
			uint8	   *nulls_bitmap;
			char	   *data_ptr;
			Size		bitmap_len;
			int			i;

			if (!ItemIdIsNormal(itemid))
				continue;

			tuple_hdr = (RecnoTupleHeader *) PageGetItem(page, itemid);

			/* Skip overflow records themselves */
			if (RecnoIsOverflowRecord(tuple_hdr, ItemIdGetLength(itemid)))
				continue;

			/* Skip deleted tuples - their overflow is orphaned */
			if (tuple_hdr->t_flags & RECNO_TUPLE_DELETED)
				continue;

			/* Only process tuples with overflow pointers */
			if (!(tuple_hdr->t_flags & RECNO_TUPLE_HAS_OVERFLOW))
				continue;

			/*
			 * Walk the tuple's varlena attributes to find overflow pointers,
			 * then follow each overflow chain and record all locations.
			 */
			tupdesc = RelationGetDescr(rel);
			bitmap_len = BITMAPLEN(tupdesc->natts);
			nulls_bitmap = (uint8 *) tuple_hdr->t_attrs_bitmap;
			data_ptr = (char *) tuple_hdr + RECNO_TUPLE_OVERHEAD + MAXALIGN(bitmap_len);

			for (i = 0; i < tupdesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupdesc, i);

				if (att->attisdropped)
					continue;

				if ((tuple_hdr->t_infomask & RECNO_INFOMASK_HASNULL) &&
					att_isnull(i, nulls_bitmap))
					continue;

				data_ptr = (char *) att_align_nominal(data_ptr, att->attalign);

				if (att->attlen == -1)
				{
					Size		attr_len = VARSIZE_ANY(data_ptr);

					if (RecnoIsOverflowPtr(data_ptr))
					{
						const RecnoOverflowPtr *ovp = RecnoGetOverflowPtr(data_ptr);
						BlockNumber cur_block = ovp->ov_first_block;
						OffsetNumber cur_offset = ovp->ov_first_offset;
						int			chain_len = 0;

						/*
						 * Follow the overflow chain and record every record
						 * location.
						 */
						while (cur_block != InvalidBlockNumber &&
							   chain_len < RECNO_MAX_OVERFLOW_CHAIN)
						{
							uint64		key;
							bool		found;
							Buffer		ovf_buf;
							Page		ovf_page;
							ItemId		ovf_itemid;
							RecnoOverflowRecordHeader *ovf_hdr;
							bool		same_page = false;

							/* Pack (block, offset) into a uint64 key */
							key = ((uint64) cur_block << 32) | (uint64) cur_offset;
							(void) hash_search(referenced_overflow, &key,
											   HASH_ENTER, &found);

							/*
							 * Follow the chain to get next pointer. Check if
							 * overflow is on the same page as the tuple to
							 * avoid double-locking the buffer.
							 */
							if (cur_block == blkno)
							{
								/* Overflow is on same page - reuse buffer */
								ovf_buf = buffer;
								ovf_page = page;
								same_page = true;
							}
							else
							{
								/*
								 * Overflow is on different page - need new
								 * buffer
								 */
								ovf_buf = ReadBuffer(rel, cur_block);
								LockBuffer(ovf_buf, BUFFER_LOCK_SHARE);
								ovf_page = BufferGetPage(ovf_buf);
							}

							if (cur_offset < FirstOffsetNumber ||
								cur_offset > PageGetMaxOffsetNumber(ovf_page))
							{
								if (!same_page)
									UnlockReleaseBuffer(ovf_buf);
								break;
							}

							ovf_itemid = PageGetItemId(ovf_page, cur_offset);
							if (!ItemIdIsNormal(ovf_itemid))
							{
								if (!same_page)
									UnlockReleaseBuffer(ovf_buf);
								break;
							}

							ovf_hdr = (RecnoOverflowRecordHeader *)
								PageGetItem(ovf_page, ovf_itemid);

							if (ovf_hdr->or_magic != RECNO_OVERFLOW_RECORD_MAGIC)
							{
								if (!same_page)
									UnlockReleaseBuffer(ovf_buf);
								break;
							}

							cur_block = ovf_hdr->or_next_block;
							cur_offset = ovf_hdr->or_next_offset;

							if (!same_page)
								UnlockReleaseBuffer(ovf_buf);
							chain_len++;
						}
					}
					data_ptr += attr_len;
				}
				else if (att->attlen > 0)
					data_ptr += att->attlen;
				else if (att->attlen == -2)
					data_ptr += strlen(data_ptr) + 1;
			}
		}

		UnlockReleaseBuffer(buffer);
	}

	/*
	 * Pass 2: Scan all pages for overflow records and remove any that are not
	 * in the referenced set.
	 */
	for (blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer		buffer;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber offnum;
		bool		page_modified = false;

		CHECK_FOR_INTERRUPTS();

		buffer = ReadBufferExtended(rel, MAIN_FORKNUM, blkno,
									RBM_NORMAL, NULL);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);

		if (PageIsNew(page))
		{
			UnlockReleaseBuffer(buffer);
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);

		/*
		 * Scan backwards so that RecnoPageIndexTupleDelete doesn't invalidate
		 * offsets we haven't checked yet.
		 */
		for (offnum = maxoff; offnum >= FirstOffsetNumber; offnum--)
		{
			ItemId		itemid = PageGetItemId(page, offnum);
			RecnoOverflowRecordHeader *hdr;
			uint64		key;
			bool		found;

			if (!ItemIdIsNormal(itemid))
				continue;

			hdr = (RecnoOverflowRecordHeader *) PageGetItem(page, itemid);

			/* Only process overflow records */
			if (!RecnoIsOverflowRecord(hdr, ItemIdGetLength(itemid)))
				continue;

			overflow_records_found++;

			/* Check if this overflow record is referenced */
			key = ((uint64) blkno << 32) | (uint64) offnum;
			(void) hash_search(referenced_overflow, &key,
							   HASH_FIND, &found);

			if (!found)
			{
				/* Orphaned overflow record - remove it */
				RecnoPageIndexTupleDelete(page, offnum);
				page_modified = true;
				orphans_removed++;
			}
		}

		if (page_modified)
		{
			MarkBufferDirty(buffer);

			/* WAL log the cleanup */
			if (RelationNeedsWAL(rel))
			{
				XLogRecPtr	recptr;

				recptr = RecnoXLogInitPage(rel, buffer, 0,
										   RecnoGetCommitTimestamp());
				PageSetLSN(page, recptr);
			}

			/* Update FSM */
			RecnoRecordFreeSpace(rel, blkno, PageGetFreeSpace(page));
		}

		UnlockReleaseBuffer(buffer);
	}

	hash_destroy(referenced_overflow);

	if (orphans_removed > 0)
		ereport(DEBUG1,
				(errmsg("RECNO overflow vacuum: found %lld overflow records, removed %lld orphans",
						(long long) overflow_records_found,
						(long long) orphans_removed)));
}
