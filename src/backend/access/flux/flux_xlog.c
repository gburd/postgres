/*-------------------------------------------------------------------------
 *
 * flux_xlog.c
 *	  FLUX WAL (Write-Ahead Logging) implementation
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_xlog.c
 *
 * NOTES
 *	  This implements WAL logging for FLUX operations, providing
 *	  UNDO/REDO functionality for crash recovery. Unlike heap,
 *	  FLUX uses in-place updates with before/after images.
 *
 *	  PANIC policy during redo
 *	  ------------------------
 *	  The per-opcode redo helpers below use elog(PANIC, ...) for any
 *	  invariant violation detected during WAL replay.  This is
 *	  deliberate: a mismatch between the WAL stream and the on-disk
 *	  state (truncated overflow payload, failure to add a tuple the
 *	  forward path just wrote, a page full the forward path just
 *	  defragmented, an unknown opcode) is not a recoverable condition.
 *	  Downgrading these sites to ERROR would promote silent divergence
 *	  between the primary and a standby, or between the on-disk
 *	  heap state and the WAL record that described it; PANIC forces
 *	  a postmaster-wide restart and, in the standby case, marks the
 *	  standby inconsistent.  Each PANIC site is therefore guarded by
 *	  logic that only fires on actually-corrupt input; fixing a PANIC
 *	  that fires in practice is a correctness bug in the forward
 *	  path, not a reason to soften the guard.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/flux.h"
#include "access/flux_xlog.h"
#include "access/bufmask.h"
#include "access/relundo.h"
#include "access/relundo_xlog.h"
#include "access/slog.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "miscadmin.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

/*
 * FluxXLogMaybeAppendLogicalTuple
 *		Append a heap-format image of `rtup` to the in-progress WAL record
 *		if `rel` is logically logged.  Returns true and sets
 *		FLUX_WAL_LOGICAL_TUPLE in `*flags` if the image was appended.
 *
 *	The heap image is what logical decoding consumes.  Physical REDO
 *	reads the FLUX-format tuple that precedes this region.  By writing
 *	both, we avoid the need for decode.c to call RelidByRelfilenumber /
 *	RelationIdGetRelation, which are unsafe before SetupHistoricSnapshot.
 *
 *	The image is appended at the END of the main WAL data channel with
 *	the length trailing the bytes:
 *
 *		... [heap bytes] [uint32 heap_len]
 *
 *	So the decoder can read heap_len from (end-4) and back up heap_len
 *	bytes to find the heap payload, regardless of what precedes it in
 *	the record (which may vary with compression / cross-page).
 *
 *	For UPDATE we append two back-to-back trailers (old then new); see
 *	FluxXLogUpdate.
 */
void
FluxXLogPrepareLogicalImage(Relation rel, FluxTuple rtup,
							FluxLogicalImage *img)
{
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *isnull;
	HeapTuple	heaptup;

	img->data = NULL;
	img->len = 0;

	if (rel == NULL || rtup == NULL || !RelationIsLogicallyLogged(rel))
		return;

	tupdesc = RelationGetDescr(rel);
	values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
	isnull = (bool *) palloc(tupdesc->natts * sizeof(bool));

	FluxDeformTuple(rel, rtup, tupdesc, values, isnull);
	heaptup = heap_form_tuple(tupdesc, values, isnull);

	/*
	 * Copy the heap tuple body into an image buffer that outlives the
	 * critical section.  XLogRegisterData() only records pointers, so the
	 * bytes must remain valid until XLogInsert() reads them.
	 */
	img->len = (uint32) heaptup->t_len;
	img->data = (char *) palloc(img->len);
	memcpy(img->data, heaptup->t_data, img->len);

	heap_freetuple(heaptup);
	pfree(values);
	pfree(isnull);
}

void
FluxXLogReleaseLogicalImage(FluxLogicalImage *img)
{
	if (img->data != NULL)
	{
		pfree(img->data);
		img->data = NULL;
	}
	img->len = 0;
}

/*
 * FluxXLogRegisterLogicalImage
 *		Register a previously prepared heap-format image onto the in-progress
 *		WAL record.  Allocation-free: safe to call inside a critical section.
 *		Appends "[heap bytes][uint32 heap_len]" to the main data channel and
 *		sets FLUX_WAL_LOGICAL_TUPLE in *flags.  No-op when img is NULL or
 *		empty (relation not logically logged).
 */
static void
FluxXLogRegisterLogicalImage(FluxLogicalImage *img, uint16 *flags)
{
	if (img == NULL || img->data == NULL)
		return;

	XLogRegisterData(img->data, img->len);
	XLogRegisterData((char *) &img->len, sizeof(uint32));

	*flags |= FLUX_WAL_LOGICAL_TUPLE;
}

/* ----------------------------------------------------------------
 *					WAL Record Logging Functions
 * ----------------------------------------------------------------
 */

/*
 * Log a tuple insert operation.
 */
XLogRecPtr
FluxXLogInsert(Relation rel, Buffer buffer, OffsetNumber offnum,
			   FluxTuple tuple, uint64 commit_ts,
			   FluxOverflowBuffers *overflow_buffers,
			   FluxLogicalImage *logical_img,
			   bool force_page_image)
{
	xl_flux_insert xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_FLUX_INSERT;
	uint8		main_buf_flags = REGBUF_STANDARD;
	int			i;
	xl_flux_overflow_write ovf_xlrecs[MAX_OVERFLOW_BUFFERS];

	/*
	 * The multi-insert (COPY/bulk) path adds many tuples to one page but
	 * emits a single INSERT record describing only the first tuple.  Under
	 * full_page_writes the page's initial touch captures the whole page, so
	 * redo restores every tuple.  With full_page_writes off there is no such
	 * image, and the unlogged tuples vanish on crash recovery, leaving later
	 * CAS_UPDATE redo to PANIC on a missing item.  Force a full-page image so
	 * the batch is crash-safe regardless of full_page_writes.
	 */
	if (force_page_image)
		main_buf_flags |= REGBUF_FORCE_IMAGE;

	/* Fill in the insert record */
	xlrec.offnum = offnum;
	xlrec.flags = 0;
	xlrec.tuple_len = tuple->t_len;
	xlrec.commit_ts = commit_ts;

	/*
	 * NOTE: XLogEnsureRecordSpace() has already been called by the caller
	 * (before entering the critical section) to pre-allocate space for the
	 * main buffer plus all overflow buffers.
	 */
	XLogBeginInsert();

	/* Register buffer FIRST, before any data */
	XLogRegisterBuffer(0, buffer, main_buf_flags);

	/*
	 * Register all overflow buffers (buffers 1..N) for atomic WAL logging.
	 * This ensures the main tuple and all overflow records are restored
	 * together during crash recovery, preventing orphaned overflow pages.
	 *
	 * IMPORTANT: Due to spatial locality optimization, multiple overflow
	 * records may reside on the same page. We must register each unique
	 * buffer only once, but register data for all overflow records.
	 *
	 * For each overflow buffer, we need to include: 1. The offset where the
	 * record should be placed 2. The actual overflow record data
	 */
	if (overflow_buffers != NULL)
	{
		int			registered_block_id = 1;	/* Start after main buffer */
		Buffer		registered_buffers[MAX_OVERFLOW_BUFFERS];
		int			registered_buffer_ids[MAX_OVERFLOW_BUFFERS];
		int			num_registered = 0;

		/*
		 * NOTE: ovf_xlrecs[] is declared at function scope (not here) so that
		 * the pointers registered via XLogRegisterBufData() remain valid
		 * until XLogInsert() is called after this block ends.
		 */

		/* Ensure we don't exceed PostgreSQL's hard limit */
		if (overflow_buffers->count > XLR_MAX_BLOCK_ID)
			elog(ERROR, "too many overflow records: %d (max %d)",
				 overflow_buffers->count, XLR_MAX_BLOCK_ID);

		for (i = 0; i < overflow_buffers->count; i++)
		{
			FluxOverflowBuffer *ovb = &overflow_buffers->buffers[i];
			int			block_id = -1;
			int			j;

			/*
			 * First check if this overflow buffer is the SAME as the main
			 * buffer. This can happen when spatial locality places an
			 * overflow record on the same page as the main tuple. In this
			 * case, reuse block_id=0.
			 */
			if (ovb->buffer == buffer)
			{
				block_id = 0;
				xlrec.flags |= FLUX_WAL_HAS_OVERFLOW_BLK0;
			}
			else
			{
				/*
				 * Check if this buffer was already registered among overflow
				 * buffers (spatial locality: multiple overflow records on
				 * same page). If so, reuse its block_id instead of
				 * registering again.
				 */
				for (j = 0; j < num_registered; j++)
				{
					if (registered_buffers[j] == ovb->buffer)
					{
						block_id = registered_buffer_ids[j];
						break;
					}
				}

				/* If not registered yet, register it now */
				if (block_id < 0)
				{
					block_id = registered_block_id++;

					/*
					 * Force a full-page image for overflow buffers. See
					 * FluxXLogUpdate for the rationale.
					 */
					XLogRegisterBuffer(block_id, ovb->buffer,
									   REGBUF_STANDARD | REGBUF_FORCE_IMAGE);

					/* Track this buffer so we don't register it again */
					registered_buffers[num_registered] = ovb->buffer;
					registered_buffer_ids[num_registered] = block_id;
					num_registered++;
				}
			}

			/*
			 * Create a proper xl_flux_overflow_write header with the offset.
			 * This tells the redo handler where to place the record. Each
			 * header is stored in a dedicated array slot so the pointer
			 * passed to XLogRegisterBufData remains valid until XLogInsert.
			 */
			ovf_xlrecs[i].offnum = ovb->offset;
			ovf_xlrecs[i].flags = ovb->flags;
			ovf_xlrecs[i].data_len = ovb->record_len;
			ovf_xlrecs[i].commit_ts = commit_ts;

			/* Register the header first, then the data */
			XLogRegisterBufData(block_id, (char *) &ovf_xlrecs[i], sizeof(xl_flux_overflow_write));
			XLogRegisterBufData(block_id, ovb->record_data, ovb->record_len);
		}
	}

	/* Now register the main data */
	XLogRegisterData((char *) &xlrec, sizeof(xl_flux_insert));
	XLogRegisterData((char *) tuple->t_data, tuple->t_len);

	FluxXLogRegisterLogicalImage(logical_img, &xlrec.flags);

	recptr = XLogInsert(RM_FLUX_ID, info);

	/* Set LSN on main page */
	PageSetLSN(page, recptr);

	/*
	 * Set LSN on all overflow pages. Due to spatial locality, some buffers
	 * may appear multiple times in overflow_buffers. PageSetLSN is idempotent
	 * (setting the same LSN multiple times is safe), so we can just iterate
	 * through all entries without checking for duplicates.
	 */
	if (overflow_buffers != NULL)
	{
		for (i = 0; i < overflow_buffers->count; i++)
		{
			Page		ovpage = BufferGetPage(overflow_buffers->buffers[i].buffer);

			PageSetLSN(ovpage, recptr);
		}
	}

	return recptr;
}

/*
 * Log a batched multi-tuple insert (COPY/bulk load) into one page.
 *
 * Unlike FluxXLogInsert, this records EVERY tuple body in the batch rather
 * than relying on a forced full-page image, so the batch replays correctly
 * regardless of full_page_writes.  No overflow buffers are involved: tuples
 * that need overflow are routed to the single-insert path by the caller.
 *
 * WAL main-data layout:
 *   [xl_flux_multi_insert header]
 *   ntuples * [xl_flux_multi_insert_tuple header][tuple t_data body]
 *   ntuples * [logical image bytes][uint32 len]   (only when logically logged)
 */
XLogRecPtr
FluxXLogMultiInsert(Relation rel, Buffer buffer,
					OffsetNumber *offnums, FluxTuple *tuples,
					int ntuples, uint64 commit_ts,
					FluxLogicalImage *logical_imgs)
{
	xl_flux_multi_insert xlrec;

	/*
	 * Serialize the whole per-tuple region into one scratch buffer and
	 * register it as a single rdata chunk.  XLogRegisterData() is limited to
	 * XLR_NORMAL_RDATAS (20) slots, and XLogEnsureRecordSpace() cannot grow
	 * that inside a critical section, so registering two chunks per tuple
	 * would overflow the slot array once a page packs more than a handful of
	 * rows. Mirrors heap_multi_insert()'s single-scratch-buffer pattern; the
	 * on-WAL bytes are identical to per-tuple registration because redo and
	 * logical decoding read the region as one contiguous XLogRecGetData()
	 * stream.
	 *
	 * The region is bounded by BLCKSZ: every tuple consumed
	 * sizeof(ItemIdData) + MAXALIGN(t_len) on the page, so the sum of
	 * SizeOfFluxMultiInsertTuple + t_len across the batch is strictly less
	 * than one page.
	 */
	PGAlignedBlock scratch;
	char	   *scratchptr = scratch.data;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_FLUX_MULTI_INSERT;
	int			i;

	Assert(ntuples > 0);
	Assert(ntuples <= MaxOffsetNumber);

	xlrec.ntuples = (uint16) ntuples;
	xlrec.flags = 0;
	xlrec.commit_ts = commit_ts;

	for (i = 0; i < ntuples; i++)
	{
		xl_flux_multi_insert_tuple *tuphdr;

		/*
		 * SHORTALIGN each per-tuple header so its uint16 fields land on an
		 * even offset, mirroring heap_multi_insert().  Readers (redo and
		 * logical decode) advance with the identical SHORTALIGN, so the
		 * on-WAL stride stays byte-for-byte in lockstep.
		 */
		scratchptr = (char *) SHORTALIGN(scratchptr);
		tuphdr = (xl_flux_multi_insert_tuple *) scratchptr;

		tuphdr->offnum = offnums[i];
		tuphdr->datalen = (uint16) tuples[i]->t_len;
		scratchptr += SizeOfFluxMultiInsertTuple;

		memcpy(scratchptr, (char *) tuples[i]->t_data, tuples[i]->t_len);
		scratchptr += tuples[i]->t_len;
	}
	Assert((scratchptr - scratch.data) < BLCKSZ);

	XLogBeginInsert();

	/*
	 * Register the page WITHOUT forcing a full-page image.  Crash safety
	 * comes from logging every tuple body above, not from an FPI.
	 */
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	XLogRegisterData((char *) &xlrec, SizeOfFluxMultiInsert);
	XLogRegisterData(scratch.data, (uint32) (scratchptr - scratch.data));

	/*
	 * Append one logical-decoding image per tuple (when logically logged).
	 * The caller pre-serialized all images into one contiguous blob before
	 * entering the critical section (logical_imgs[0].data), so a single
	 * registration covers the whole region and keeps the rdata slot count
	 * constant.
	 */
	if (logical_imgs != NULL && logical_imgs[0].data != NULL)
	{
		XLogRegisterData(logical_imgs[0].data, logical_imgs[0].len);
		xlrec.flags |= FLUX_WAL_LOGICAL_TUPLE;
	}

	recptr = XLogInsert(RM_FLUX_ID, info);

	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * Log a tuple update operation (in-place with before/after images)
 */
XLogRecPtr
FluxXLogUpdate(Relation rel, Buffer buffer, OffsetNumber offnum,
			   FluxTuple old_tuple, FluxTuple new_tuple,
			   uint64 old_commit_ts, uint64 new_commit_ts,
			   FluxOverflowBuffers *overflow_buffers,
			   Buffer new_buffer,
			   FluxLogicalImage *old_img,
			   FluxLogicalImage *new_img)
{
	xl_flux_update xlrec;
	xl_flux_prefix_suffix ps;
	XLogRecPtr	recptr;
	uint8		info = XLOG_FLUX_UPDATE_INPLACE;
	int			i;
	bool		is_cross_page = (BufferIsValid(new_buffer) &&
								 new_buffer != buffer);

	/* Fill in the update record */
	xlrec.offnum = offnum;
	xlrec.flags = 0;
	xlrec.old_commit_ts = old_commit_ts;
	xlrec.new_commit_ts = new_commit_ts;
	xlrec.old_tuple_len = (uint16) old_tuple->t_len;
	xlrec.new_tuple_len = (uint16) new_tuple->t_len;
	xlrec.dst_block_id = 0;
	memset(xlrec.pad, 0, sizeof(xlrec.pad));

	if (is_cross_page)
		xlrec.flags |= FLUX_WAL_CROSS_PAGE;

	/*
	 * NOTE: XLogEnsureRecordSpace() has already been called by the caller
	 * (before entering the critical section) to pre-allocate space for the
	 * main buffer plus all overflow buffers.
	 */
	XLogBeginInsert();

	/*
	 * Register the source buffer (block 0).
	 *
	 * A same-page update rewrites the tuple in place at the same offset: redo
	 * overwrites the slot with the new tuple data (or patches the
	 * prefix/suffix diff), so no full-page image is required.  An image is
	 * only needed when the new tuple is larger than the old one, because the
	 * write path then deletes and re-adds the line pointer (shifting page
	 * data), which redo cannot replay from the new tuple bytes alone.  The
	 * predicate new_tuple->t_len > old_tuple->t_len is a safe superset of
	 * that grow case: the on-page slot is always at least old_tuple->t_len,
	 * so any update that outgrows the slot also outgrows old_tuple->t_len and
	 * forces the image.
	 *
	 * For cross-page updates the redo handler marks the old tuple UPDATED
	 * directly (see FLUX_WAL_CROSS_PAGE handling in flux_redo) and the new
	 * tuple is restored from the destination page's image, so the same
	 * grow-only image rule applies to block 0.
	 */
	{
		uint8		buf_flags = REGBUF_STANDARD;

		if (new_tuple->t_len > old_tuple->t_len)
			buf_flags |= REGBUF_FORCE_IMAGE;

		XLogRegisterBuffer(0, buffer, buf_flags);
	}

	/*
	 * Register all overflow buffers (buffers 1..N) for atomic WAL logging.
	 * This ensures the main tuple UPDATE and all overflow records are
	 * restored together during crash recovery, preventing orphaned overflow
	 * pages.
	 *
	 * IMPORTANT: Due to spatial locality optimization, multiple overflow
	 * records may reside on the same page. We must register each unique
	 * buffer only once, but register data for all overflow records.
	 */
	{
		int			next_block_id = 1;	/* Start after main buffer (block 0) */

		if (overflow_buffers != NULL)
		{
			Buffer		registered_buffers[MAX_OVERFLOW_BUFFERS];
			int			registered_buffer_ids[MAX_OVERFLOW_BUFFERS];
			int			num_registered = 0;

			/* Ensure we don't exceed PostgreSQL's hard limit */
			if (overflow_buffers->count > XLR_MAX_BLOCK_ID)
				elog(ERROR, "too many overflow records: %d (max %d)",
					 overflow_buffers->count, XLR_MAX_BLOCK_ID);

			for (i = 0; i < overflow_buffers->count; i++)
			{
				FluxOverflowBuffer *ovb = &overflow_buffers->buffers[i];
				int			block_id = -1;
				int			j;

				/*
				 * First check if this overflow buffer is the SAME as the main
				 * buffer.  This can happen when spatial locality places an
				 * overflow record on the same page as the main tuple. In this
				 * case, reuse block_id=0.
				 */
				if (ovb->buffer == buffer)
				{
					block_id = 0;
					xlrec.flags |= FLUX_WAL_HAS_OVERFLOW_BLK0;
				}
				else
				{
					/*
					 * Check if this buffer was already registered among
					 * overflow buffers (spatial locality: multiple overflow
					 * records on same page). If so, reuse its block_id
					 * instead of registering again.
					 */
					for (j = 0; j < num_registered; j++)
					{
						if (registered_buffers[j] == ovb->buffer)
						{
							block_id = registered_buffer_ids[j];
							break;
						}
					}

					/* If not registered yet, register it now */
					if (block_id < 0)
					{
						block_id = next_block_id++;

						/*
						 * Force a full-page image for overflow buffers. The
						 * redo handler for overflow pages reconstructs items
						 * using PageAddItem, but the page layout can differ
						 * from the primary when the page already contains
						 * items from prior operations (e.g., free space
						 * fragmentation, item alignment). Using
						 * REGBUF_FORCE_IMAGE guarantees the page is restored
						 * exactly as the primary had it.
						 */
						XLogRegisterBuffer((uint8) block_id, ovb->buffer,
										   REGBUF_STANDARD | REGBUF_FORCE_IMAGE);

						/* Track this buffer so we don't register it again */
						registered_buffers[num_registered] = ovb->buffer;
						registered_buffer_ids[num_registered] = block_id;
						num_registered++;
					}
				}

				/* Register overflow record data for this buffer */
				XLogRegisterBufData((uint8) block_id, ovb->record_data,
									ovb->record_len);
			}
		}

		/*
		 * For cross-page out-of-place updates, register the destination
		 * buffer so both pages are crash-safe.  Force a full-page image so
		 * redo simply restores the page without needing replay logic.
		 */
		if (is_cross_page)
		{
			xlrec.dst_block_id = (uint8) next_block_id;
			XLogRegisterBuffer((uint8) next_block_id, new_buffer,
							   REGBUF_STANDARD | REGBUF_FORCE_IMAGE);
			next_block_id++;
		}
	}

	/* Now register the main data */
	XLogRegisterData((char *) &xlrec, sizeof(xl_flux_update));

	/*
	 * Log only new tuple for REDO.  Old tuple data is stored exclusively in
	 * the shared UNDO log (UNDO_RMID_FLUX record written via
	 * UndoBufferAddRecordParts) and is not needed during WAL replay:
	 *
	 * - Same-size/shrinking updates: redo overwrites the slot in place using
	 * only the new tuple data. - Growing updates: REGBUF_FORCE_IMAGE is set
	 * above, so redo restores the page from a full-page image and never
	 * enters BLK_NEEDS_REDO.
	 *
	 * Prefix/suffix compression: For same-size in-place updates, we compute
	 * the common prefix and suffix between old and new tuple data.  If the
	 * savings exceed sizeof(xl_flux_prefix_suffix) (4 bytes), we log only the
	 * changed bytes plus a small header.  The redo handler reconstructs the
	 * full new tuple from the existing page data + diff.
	 *
	 * This is only safe for same-size updates without cross-page moves.
	 * Growing updates use REGBUF_FORCE_IMAGE and never enter BLK_NEEDS_REDO.
	 */
	if (!is_cross_page &&
		old_tuple->t_len == new_tuple->t_len &&
		new_tuple->t_len > 0)
	{
		char	   *oldp = (char *) old_tuple->t_data;
		char	   *newp = (char *) new_tuple->t_data;
		int			len = new_tuple->t_len;
		int			difflen;

		/* Compute common prefix */
		for (ps.prefixlen = 0; ps.prefixlen < len; ps.prefixlen++)
			if (oldp[ps.prefixlen] != newp[ps.prefixlen])
				break;

		/* Compute common suffix (don't overlap with prefix) */
		for (ps.suffixlen = 0;
			 ps.suffixlen < len - ps.prefixlen;
			 ps.suffixlen++)
			if (oldp[len - 1 - ps.suffixlen] != newp[len - 1 - ps.suffixlen])
				break;

		difflen = len - ps.prefixlen - ps.suffixlen;

		/*
		 * Use compression only if the savings exceed the header overhead. The
		 * header is 4 bytes (two uint16s), so we need the prefix + suffix to
		 * save more than that.
		 */
		if (ps.prefixlen + ps.suffixlen > (int) sizeof(xl_flux_prefix_suffix) &&
			difflen >= 0)
		{
			xlrec.flags |= FLUX_WAL_PREFIX_SUFFIX;
			XLogRegisterData((char *) &ps, sizeof(xl_flux_prefix_suffix));
			if (difflen > 0)
				XLogRegisterData(newp + ps.prefixlen, difflen);
		}
		else
		{
			/* Not worth compressing, log full new tuple */
			XLogRegisterData((char *) new_tuple->t_data, new_tuple->t_len);
		}
	}
	else
	{
		/* Cross-page or size-changing: log full new tuple */
		XLogRegisterData((char *) new_tuple->t_data, new_tuple->t_len);
	}

	/*
	 * Append heap-format images of old + new tuples for logical decoding.
	 * Order: old first, then new.  Flag is set uniformly on both or neither.
	 * The images were prepared by the caller before the critical section.
	 */
	{
		uint16		tmpflag = 0;

		FluxXLogRegisterLogicalImage(old_img, &tmpflag);
		FluxXLogRegisterLogicalImage(new_img, &tmpflag);
		xlrec.flags |= tmpflag;
	}

	recptr = XLogInsert(RM_FLUX_ID, info);

	return recptr;
}

/*
 * Log a tuple delete operation
 */
XLogRecPtr
FluxXLogDelete(Relation rel, Buffer buffer, OffsetNumber offnum,
			   FluxTuple tuple, uint64 commit_ts,
			   FluxLogicalImage *logical_img)
{
	xl_flux_delete xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_FLUX_DELETE;

	/* Fill in the delete record */
	xlrec.offnum = offnum;
	xlrec.flags = 0;
	xlrec.tuple_len = tuple->t_len;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();

	/* Register buffer FIRST, before any data */
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	/*
	 * Register delete header only -- old tuple data is stored exclusively in
	 * the UNDO fork.  The redo handler only needs the offset and commit_ts to
	 * set FLUX_TUPLE_DELETED on the existing tuple.
	 */
	XLogRegisterData((char *) &xlrec, sizeof(xl_flux_delete));

	/*
	 * Append heap-format image of the deleted tuple for logical decoding.
	 * DELETE's REDO path doesn't need the old tuple image on-page (it just
	 * flips a flag), so this region is strictly for the decode side.  The
	 * image was prepared by the caller before the critical section.
	 */
	FluxXLogRegisterLogicalImage(logical_img, &xlrec.flags);

	recptr = XLogInsert(RM_FLUX_ID, info);

	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * Log page defragmentation
 */
XLogRecPtr
FluxXLogDefrag(Relation rel, Buffer buffer, FluxOffsetMapping *mappings,
			   int nmappings, uint64 commit_ts)
{
	xl_flux_defrag xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_FLUX_DEFRAG;

	/* Fill in the defrag record */
	xlrec.ntuples = nmappings;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_flux_defrag));
	XLogRegisterData((char *) mappings, sizeof(FluxOffsetMapping) * nmappings);

	/*
	 * Force a full-page image.  The caller may have removed dead tuples
	 * (ItemIdSetUnused) before compaction, and those removals are not encoded
	 * in the DEFRAG WAL record.  Without an FPI the redo handler would call
	 * PageRepairFragmentation() on a page that still contains the dead
	 * tuples, producing a page inconsistent with the primary.
	 */
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | REGBUF_FORCE_IMAGE);

	recptr = XLogInsert(RM_FLUX_ID, info);

	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * Log overflow record write.
 *
 * The caller must already hold an exclusive lock on the buffer and have
 * written the overflow record data to the page.  We log either a new
 * overflow record (header + data) or a link update (header only).
 *
 * buffer:      already-locked buffer containing the overflow record
 * offnum:      offset of the overflow record on the page
 * record_data: pointer to the record data to log (header, or header+data)
 * record_len:  length of data to log
 * flags:       FLUX_OVERFLOW_WAL_NEW_RECORD or FLUX_OVERFLOW_WAL_LINK_UPDATE
 * commit_ts:   commit timestamp
 */
XLogRecPtr
FluxXLogOverflowWrite(Relation rel, Buffer buffer, OffsetNumber offnum,
					  char *record_data, uint32 record_len, uint16 flags,
					  uint64 commit_ts)
{
	xl_flux_overflow_write xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_FLUX_OVERFLOW_WRITE;

	/* Fill in the overflow write record */
	xlrec.offnum = offnum;
	xlrec.flags = flags;
	xlrec.data_len = record_len;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_flux_overflow_write));
	XLogRegisterData(record_data, record_len);
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	recptr = XLogInsert(RM_FLUX_ID, info);

	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * Log attribute compression
 */
XLogRecPtr
FluxXLogCompress(Relation rel, Buffer buffer, OffsetNumber offnum,
				 uint16 attr_num, FluxCompressionType comp_type,
				 uint8 comp_level, char *comp_data,
				 uint32 orig_size, uint32 comp_size, uint64 commit_ts)
{
	xl_flux_compress xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_FLUX_COMPRESS;

	/* Fill in the compress record */
	xlrec.offnum = offnum;
	xlrec.attr_num = attr_num;
	xlrec.comp_type = comp_type;
	xlrec.comp_level = comp_level;
	xlrec.orig_size = orig_size;
	xlrec.comp_size = comp_size;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_flux_compress));
	XLogRegisterData(comp_data, comp_size);
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	recptr = XLogInsert(RM_FLUX_ID, info);

	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * Log page initialization
 */
XLogRecPtr
FluxXLogInitPage(Relation rel, Buffer buffer, uint32 flags, uint64 commit_ts)
{
	xl_flux_init_page xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_FLUX_INIT_PAGE;

	/* Fill in the init page record */
	xlrec.flags = flags;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();

	/* Register buffer FIRST, before any data */
	XLogRegisterBuffer(0, buffer, REGBUF_WILL_INIT | REGBUF_STANDARD);

	/* Now register the data */
	XLogRegisterData((char *) &xlrec, sizeof(xl_flux_init_page));

	recptr = XLogInsert(RM_FLUX_ID, info);

	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * Log a cross-page defragmentation move.
 *
 * This logs the move of a single tuple from a source page (block ref 1)
 * to a target page (block ref 0).  Both pages are registered so that
 * full-page images will be taken if needed.  The tuple data is included
 * in the record so that recovery can replay the move even without FPIs.
 */
XLogRecPtr
FluxXLogCrossPageDefrag(Relation rel,
						Buffer dst_buf, OffsetNumber dst_offnum,
						Buffer src_buf, OffsetNumber src_offnum,
						const void *tuple_data, uint32 tuple_len)
{
	xl_flux_cross_page_defrag xlrec;
	XLogRecPtr	recptr;

	xlrec.src_offnum = src_offnum;
	xlrec.dst_offnum = dst_offnum;
	xlrec.tuple_len = tuple_len;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_flux_cross_page_defrag));
	XLogRegisterData((char *) tuple_data, tuple_len);
	XLogRegisterBuffer(0, dst_buf, REGBUF_STANDARD | REGBUF_FORCE_IMAGE);
	XLogRegisterBuffer(1, src_buf, REGBUF_STANDARD);

	recptr = XLogInsert(RM_FLUX_ID, XLOG_FLUX_CROSS_PAGE_DEFRAG);

	return recptr;
}

/*
 * FluxXLogCasUpdate -- WAL record for same-size CAS in-place update.
 *
 * Logs only the changed byte range within the tuple.  This is the minimal
 * WAL record for the tuple-level CAS fast path where the entire tuple does
 * not need to be logged (same size, only data bytes changed).
 *
 * The caller holds BUFFER_LOCK_SHARE_EXCLUSIVE and the per-tuple t_writer CAS
 * lock.  We do NOT force a full-page image because:
 *   (a) the modification is confined to a single tuple's data bytes, and
 *   (b) the redo handler is idempotent (memcpy of fixed-length data at
 *       a fixed offset within the tuple).
 */
XLogRecPtr
FluxXLogCasUpdate(Relation rel, Buffer buffer, OffsetNumber offnum,
				  uint16 data_offset, uint16 data_len,
				  const char *new_data, uint64 new_commit_ts)
{
	xl_flux_cas_update xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);

	xlrec.offnum = offnum;
	xlrec.flags = 0;
	xlrec.data_offset = data_offset;
	xlrec.data_len = data_len;
	xlrec.new_commit_ts = new_commit_ts;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_flux_cas_update));
	XLogRegisterData(new_data, data_len);
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	recptr = XLogInsert(RM_FLUX_ID, XLOG_FLUX_CAS_UPDATE);
	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * FluxXLogCasUpdateUndo -- FOLD variant of FluxXLogCasUpdate.
 *
 * Emits ONE combined WAL record carrying both the main-fork redo byte-diff
 * (block 0, identical to FluxXLogCasUpdate) and the relundo-fork UNDO
 * before-image (block 1, identical to what RelUndoFinish would have logged in
 * a standalone RM_RELUNDO_ID record), plus the relundo metapage (block 2) when
 * the UNDO record started a fresh relundo page.
 *
 * The caller must hold BOTH the main-fork buffer and the staged undo buffer
 * (and the metapage buffer, if is_new_page) exclusively locked inside its
 * critical section.  RelUndoStage() has already written and dirtied the undo
 * page; this function only logs the change and stamps all page LSNs.  Returns
 * the record LSN.
 */
XLogRecPtr
FluxXLogCasUpdateUndo(Relation rel, Buffer buffer, OffsetNumber offnum,
					  uint16 data_offset, uint16 data_len,
					  const char *new_data, uint64 new_commit_ts,
					  const struct RelUndoStageResult *undo)
{
	xl_flux_cas_update_undo xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);

	/* redo half (block 0) */
	xlrec.offnum = offnum;
	xlrec.flags = 0;
	xlrec.data_offset = data_offset;
	xlrec.data_len = data_len;
	xlrec.new_commit_ts = new_commit_ts;

	/* undo half (block 1) */
	xlrec.urec_type = undo->urec_type;
	xlrec.is_new_page = undo->is_new_page ? 1 : 0;
	xlrec.urec_len = undo->urec_len;
	xlrec.page_offset = undo->page_offset;
	xlrec.new_pd_lower = undo->new_pd_lower;
	xlrec.max_xid = undo->max_xid;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfFluxCasUpdateUndo);
	XLogRegisterData(new_data, data_len);

	/* block 0: main-fork page, redo byte-diff */
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	/*
	 * block 1: relundo data page.  A freshly allocated page is registered
	 * WILL_INIT so redo reconstructs it from scratch (the redo handler keys
	 * off xlrec.is_new_page, not a record-level info bit, because the FLUX
	 * rmgr consumes the whole info upper-nibble as the opcode); the staged
	 * block data prepends the RelUndoPageHeaderData in that case
	 * (RelUndoStage built wal_record_data accordingly).  An existing page
	 * registers the record bytes at page_offset with flag 0 to keep the FPI
	 * faithful (relundo data pages are non-standard).
	 */
	if (undo->is_new_page)
		XLogRegisterBuffer(1, undo->undo_buffer, REGBUF_WILL_INIT);
	else
		XLogRegisterBuffer(1, undo->undo_buffer, 0);

	XLogRegisterBufData(1, undo->wal_record_data, undo->wal_record_size);

	/* block 2: relundo metapage, only when a new page was allocated */
	if (undo->is_new_page)
	{
		Assert(BufferIsValid(undo->metabuf));
		XLogRegisterBuffer(2, undo->metabuf, REGBUF_STANDARD);
	}

	recptr = XLogInsert(RM_FLUX_ID, XLOG_FLUX_CAS_UPDATE_UNDO);

	/* stamp all touched pages */
	PageSetLSN(page, recptr);
	PageSetLSN(BufferGetPage(undo->undo_buffer), recptr);
	if (undo->is_new_page)
		PageSetLSN(BufferGetPage(undo->metabuf), recptr);

	return recptr;
}

/*
 * FluxXLogWriteDict -- WAL-log a compression-dictionary fork page.
 *
 * The dictionary fork uses a non-standard page layout that the redo path
 * cannot rebuild from a logical delta, so we register the page as a forced
 * full-page image and the redo handler restores it verbatim.  The caller
 * must already hold the buffer locked and have dirtied it inside the same
 * critical section.
 */
XLogRecPtr
FluxXLogWriteDict(Relation rel, Buffer buffer)
{
	xl_flux_write_dict xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);

	xlrec.blkno = BufferGetBlockNumber(buffer);

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_flux_write_dict));
	XLogRegisterBuffer(0, buffer, REGBUF_FORCE_IMAGE);

	recptr = XLogInsert(RM_FLUX_ID, XLOG_FLUX_WRITE_DICT);
	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * REDO function for FLUX WAL records
 */
/* ----------------------------------------------------------------
 *		Per-opcode REDO handlers.
 *
 *	flux_redo() is the thin dispatcher; the real work for each
 *	XLOG_FLUX_* opcode lives in a dedicated static helper below.
 * ----------------------------------------------------------------
 */
static void flux_xlog_insert_redo(XLogReaderState *record);
static void flux_xlog_multi_insert_redo(XLogReaderState *record);
static void flux_xlog_update_inplace_redo(XLogReaderState *record);
static void flux_xlog_delete_redo(XLogReaderState *record);
static void flux_xlog_defrag_redo(XLogReaderState *record);
static void flux_xlog_overflow_write_redo(XLogReaderState *record);
static void flux_xlog_compress_redo(XLogReaderState *record);
static void flux_xlog_init_page_redo(XLogReaderState *record);
static void flux_xlog_cross_page_defrag_redo(XLogReaderState *record);
static void flux_xlog_vm_set_redo(XLogReaderState *record);
static void flux_xlog_vm_clear_redo(XLogReaderState *record);
static void flux_xlog_lock_redo(XLogReaderState *record);

/*
 * flux_xlog_insert_redo
 *		REDO handler for XLOG_FLUX_INSERT.
 */
static void
flux_xlog_insert_redo(XLogReaderState *record)
{
	RelFileLocator rlocator;
	BlockNumber blkno;
	Buffer		buffer;
	Page		page;

	XLogRecGetBlockTag(record, 0, &rlocator, NULL, &blkno);

	{
		xl_flux_insert *xlrec = (xl_flux_insert *) XLogRecGetData(record);
		char	   *tuple_data = (char *) xlrec + sizeof(xl_flux_insert);
		FluxTupleHeader *tuple_hdr = (FluxTupleHeader *) tuple_data;
		XLogRedoAction action;
		OffsetNumber final_offnum = InvalidOffsetNumber;
		bool		tuple_uncommitted = false;

		action = XLogReadBufferForRedo(record, 0, &buffer);

		/*
		 * For BLK_RESTORED (FPI), the page already has the tuple at
		 * xlrec->offnum
		 */
		if (action == BLK_RESTORED)
			final_offnum = xlrec->offnum;

		if (action == BLK_NEEDS_REDO)
		{
			FluxPageOpaque phdr;
			OffsetNumber inserted_offnum;
			char	   *ovf_data;
			Size		ovf_len;

			page = BufferGetPage(buffer);

			/*
			 * XLogInitBufferForRedo does standard PageInit for new pages, but
			 * doesn't set up FLUX opaque space. Initialize it here if needed.
			 */
			if (PageIsNew(page))
			{
				FluxInitPage(page, BufferGetPageSize(buffer));
			}

			/*
			 * CRITICAL: During normal operation, overflow records are
			 * inserted BEFORE the main tuple (via FluxStoreOverflowColumn
			 * called from FluxFormTuple, then FluxPageAddTuple for main).
			 * This means overflow records get lower offsets (1, 2, 3...) and
			 * the main tuple gets a higher offset (4, ...).
			 *
			 * We MUST replay in the same order. If there are overflow records
			 * on block_id=0 (same page as main tuple due to spatial
			 * locality), replay them FIRST before the main tuple.
			 */
			ovf_data = XLogRecGetBlockData(record, 0, &ovf_len);
			if (ovf_data != NULL && ovf_len > 0 &&
				(xlrec->flags & FLUX_WAL_HAS_OVERFLOW_BLK0))
			{
				char	   *ovf_ptr = ovf_data;
				Size		ovf_remaining = ovf_len;

				/*
				 * Block 0 has overflow data. Parse and replay all overflow
				 * records on this block before the main tuple. Each overflow
				 * record has format: [xl_flux_overflow_write header][actual
				 * record data]
				 */
				while (ovf_remaining > sizeof(xl_flux_overflow_write))
				{
					xl_flux_overflow_write *ovf_xlrec = (xl_flux_overflow_write *) ovf_ptr;
					char	   *actual_data = ovf_ptr + sizeof(xl_flux_overflow_write);
					Size		actual_len = ovf_xlrec->data_len;
					OffsetNumber ovf_offnum;

					if (ovf_remaining < sizeof(xl_flux_overflow_write) + actual_len)
						elog(PANIC, "FLUX INSERT redo: corrupt overflow data on block 0: "
							 "ovf_remaining=%zu, sizeof(hdr)=%zu, data_len=%u, "
							 "total_len=%zu, offnum=%u, flags=%u",
							 ovf_remaining, sizeof(xl_flux_overflow_write),
							 (unsigned) actual_len, ovf_len,
							 (unsigned) ovf_xlrec->offnum,
							 (unsigned) ovf_xlrec->flags);

					/*
					 * Use InvalidOffsetNumber to let PageAddItem choose the
					 * next available offset. This ensures sequential offsets
					 * matching the original insertion order.
					 */
					ovf_offnum = PageAddItem(page, actual_data, actual_len,
											 InvalidOffsetNumber, false, false);
					if (ovf_offnum == InvalidOffsetNumber)
					{
						elog(WARNING, "FLUX INSERT redo: failed to add overflow "
							 "record on block %u; skipping redo", blkno);
						goto insert_skip_tuple;
					}

					/* Advance to next overflow record in the block data */
					ovf_ptr += sizeof(xl_flux_overflow_write) + actual_len;
					ovf_remaining -= sizeof(xl_flux_overflow_write) + actual_len;
				}
			}

			/*
			 * Validate that the record actually carries a tuple body of the
			 * advertised length before dereferencing it.  The speculative
			 * INSERT writers (INSERT ... ON CONFLICT confirm/abort)
			 * historically emitted body-less records and relied solely on a
			 * full-page image; such a record must never reach this non-FPI
			 * path, but if it does (or tuple_len is otherwise inconsistent
			 * with the main data), reading PageAddItem(page, tuple_hdr,
			 * tuple_len) would walk off the end of the WAL record and
			 * SIGSEGV.  Treat a missing/short body as "nothing to replay
			 * here" rather than crashing recovery.
			 */
			if (xlrec->tuple_len == 0 ||
				XLogRecGetDataLen(record) <
				sizeof(xl_flux_insert) + xlrec->tuple_len)
			{
				elog(WARNING, "FLUX INSERT redo: record on block %u lacks a "
					 "tuple body (data_len=%u, need=%zu); skipping tuple redo",
					 blkno, XLogRecGetDataLen(record),
					 sizeof(xl_flux_insert) + (Size) xlrec->tuple_len);
				goto insert_skip_tuple;
			}

			/*
			 * Now replay the main tuple. Use InvalidOffsetNumber to let
			 * PageAddItem choose the next sequential offset after any
			 * overflow records we just added.
			 */
			inserted_offnum = PageAddItem(page, tuple_hdr, xlrec->tuple_len,
										  InvalidOffsetNumber, false, false);
			if (inserted_offnum == InvalidOffsetNumber)
			{
				/*
				 * PageAddItem can fail if the page was modified by a later
				 * operation (CLR from the UNDO subsystem, defrag, or prune)
				 * whose effects were checkpointed to disk before the crash.
				 * In that case this INSERT was already superseded and the
				 * page state is ahead of this WAL record.  Advance the page
				 * LSN so recovery doesn't retry, and skip tuple setup.
				 *
				 * PANICing here would make the server permanently
				 * unrecoverable after certain crash sequences involving the
				 * logical revert worker.
				 */
				elog(WARNING, "FLUX INSERT redo: failed to add tuple on "
					 "block %u (page may have been modified by a later "
					 "operation); skipping redo", blkno);
				goto insert_skip_tuple;
			}
			final_offnum = inserted_offnum;

			/*
			 * Fix the tuple's t_ctid to point to itself at the correct
			 * location. During normal operation, this is set in
			 * flux_tuple_insert after we know the final TID. During redo, we
			 * must fix it here.
			 *
			 * Defensive: validate the ItemId is LP_NORMAL *and* has storage
			 * before dereferencing via PageGetItem.  PageGetItem asserts
			 * ItemIdHasStorage (lp_len != 0), which ItemIdIsNormal does not
			 * imply: PageAddItem can produce an LP_NORMAL line pointer with
			 * lp_len == 0 for a zero-length payload, and after crash recovery
			 * involving the UNDO revert worker the slot could be in an
			 * unexpected state.  Skipping the t_ctid fixup for a zero-storage
			 * item is safe (there is no tuple body to point at).
			 */
			{
				ItemId		itemid = PageGetItemId(page, inserted_offnum);

				if (ItemIdIsNormal(itemid) && ItemIdHasStorage(itemid))
				{
					FluxTupleHeader *inserted_hdr =
						(FluxTupleHeader *) PageGetItem(page, itemid);

					/* blkno was already fetched at function entry */
					ItemPointerSet(&inserted_hdr->t_ctid, blkno, inserted_offnum);
				}
			}

			/*
			 * Update page header.  CRITICAL: Must replicate the exact logic
			 * from FluxPageAddTuple() so the page matches the Full Page
			 * Write.  FluxPageAddTuple sets the FLUX_PAGE_DEFRAG_NEEDED flag
			 * based on fragmentation heuristics, so we must do the same here.
			 */
			phdr = FluxPageGetOpaque(page);
			FluxPageSetCommitTs(phdr, Max(FluxPageGetCommitTs(phdr), xlrec->commit_ts));

			/*
			 * Mark page for defragmentation if fragmented. This matches the
			 * logic in FluxPageAddTuple() at flux_tuple.c:513-517.
			 */
			if (PageGetFreeSpace(page) >= xlrec->tuple_len * 2 &&
				PageGetMaxOffsetNumber(page) > FirstOffsetNumber + 5)
			{
				FluxPageSetFlag(phdr, FLUX_PAGE_DEFRAG_NEEDED);
			}

	insert_skip_tuple:
			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}

		/*
		 * Capture the UNCOMMITTED flag from the replayed page tuple while we
		 * still hold the buffer.  Reading it from the WAL main-data header is
		 * wrong for the speculative INSERT variants, which restore the tuple
		 * via a full-page image and carry no tuple body in main data.
		 */
		if (final_offnum != InvalidOffsetNumber && BufferIsValid(buffer))
		{
			Page		curpage = BufferGetPage(buffer);

			if (final_offnum <= PageGetMaxOffsetNumber(curpage))
			{
				ItemId		curiid = PageGetItemId(curpage, final_offnum);

				if (ItemIdIsNormal(curiid) && ItemIdHasStorage(curiid))
				{
					FluxTupleHeader *curhdr =
						(FluxTupleHeader *) PageGetItem(curpage, curiid);

					tuple_uncommitted =
						(curhdr->t_flags & FLUX_TUPLE_UNCOMMITTED) != 0;
				}
			}
		}

		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);

		/*
		 * Register UNCOMMITTED tuples in the per-tuple sLog during WAL
		 * replay.  On a hot standby, the sLog is never populated by normal
		 * INSERT operations (only the primary's transaction machinery does
		 * that).  Without this, the visibility check sees slog_nfound==0 and
		 * incorrectly assumes the inserter committed, making aborted tuples
		 * visible until the CLR arrives from the logical revert worker.
		 *
		 * This entry is cleaned up lazily: for committed transactions,
		 * SLogTupleEvictCommitted() reclaims the slot when the hash fills.
		 * For aborted transactions, the CLR sets DELETED, making the sLog
		 * entry irrelevant for visibility.
		 */
		if (final_offnum != InvalidOffsetNumber && tuple_uncommitted)
		{
			TransactionId redo_xid = XLogRecGetXid(record);

			if (TransactionIdIsValid(redo_xid))
			{
				ItemPointerData tid;

				ItemPointerSet(&tid, blkno, final_offnum);
				SLogTupleInsertRecovery(rlocator.relNumber, &tid,
										redo_xid, SLOG_OP_INSERT);
			}
		}

		/*
		 * Process overflow buffers on separate pages (buffers 1..N). Each
		 * overflow buffer contains an overflow record that was registered
		 * with XLogRegisterBufData during WAL logging.
		 *
		 * Note: Overflow records on block_id=0 were already handled above
		 * before the main tuple to preserve insertion order.
		 */
		for (int ovf_idx = 1; ovf_idx < XLR_MAX_BLOCK_ID; ovf_idx++)
		{
			Buffer		ovf_buffer;
			Page		ovf_page;
			XLogRedoAction ovf_action;

			if (!XLogRecHasBlockRef(record, ovf_idx))
				break;			/* No more overflow buffers */

			ovf_action = XLogReadBufferForRedo(record, (uint8) ovf_idx, &ovf_buffer);
			if (ovf_action == BLK_NEEDS_REDO)
			{
				char	   *ovf_data;
				Size		ovf_len;

				ovf_page = BufferGetPage(ovf_buffer);

				/* Initialize as FLUX page if new */
				if (PageIsNew(ovf_page))
				{
					FluxInitPage(ovf_page, BufferGetPageSize(ovf_buffer));
				}

				/* Get the overflow record data from WAL */
				ovf_data = XLogRecGetBlockData(record, (uint8) ovf_idx, &ovf_len);
				if (ovf_data != NULL && ovf_len > 0)
				{
					char	   *ovf_ptr = ovf_data;
					Size		ovf_remaining = ovf_len;

					/*
					 * Parse and replay all overflow records on this block.
					 * Multiple overflow records may be on the same page due
					 * to spatial locality optimization.
					 */
					while (ovf_remaining > sizeof(xl_flux_overflow_write))
					{
						xl_flux_overflow_write *ovf_xlrec = (xl_flux_overflow_write *) ovf_ptr;
						char	   *actual_data = ovf_ptr + sizeof(xl_flux_overflow_write);
						Size		actual_len = ovf_xlrec->data_len;
						OffsetNumber ovf_offnum;

						if (ovf_remaining < sizeof(xl_flux_overflow_write) + actual_len)
							elog(PANIC, "FLUX INSERT redo: corrupt overflow data on block %u",
								 BufferGetBlockNumber(ovf_buffer));

						/*
						 * Use the specific offset from WAL record. Overflow
						 * pointers reference these offsets. Use the specific
						 * offset from WAL record. Overflow pointers reference
						 * these offsets.
						 */
						ovf_offnum = PageAddItem(ovf_page, actual_data, actual_len,
												 ovf_xlrec->offnum, false, false);
						if (ovf_offnum == InvalidOffsetNumber)
						{
							/*
							 * Overflow page may have been modified by a later
							 * operation that was checkpointed.  Skip
							 * remaining overflow records on this page.
							 */
							elog(WARNING, "FLUX INSERT redo: failed to add "
								 "overflow record on block %u; skipping",
								 BufferGetBlockNumber(ovf_buffer));
							break;
						}

						/* Advance to next overflow record */
						ovf_ptr += sizeof(xl_flux_overflow_write) + actual_len;
						ovf_remaining -= sizeof(xl_flux_overflow_write) + actual_len;
					}
				}

				PageSetLSN(ovf_page, record->EndRecPtr);
				MarkBufferDirty(ovf_buffer);
			}
			if (BufferIsValid(ovf_buffer))
				UnlockReleaseBuffer(ovf_buffer);
		}
	}
}

/*
 * flux_xlog_multi_insert_redo
 *		REDO handler for XLOG_FLUX_MULTI_INSERT.
 *
 * Replays a page-at-a-time batch insert.  The WAL main data is:
 *		[xl_flux_multi_insert header]
 *		ntuples * { [xl_flux_multi_insert_tuple header][tuple t_data body] }
 *		ntuples * { [logical image bytes][uint32 len] }  (only if logical)
 *
 * Unlike single INSERT, the batch path never spills to overflow (large
 * tuples route to the single-insert path), so there is no overflow replay.
 */
static void
flux_xlog_multi_insert_redo(XLogReaderState *record)
{
	RelFileLocator rlocator;
	BlockNumber blkno;
	Buffer		buffer;
	Page		page;

	XLogRecGetBlockTag(record, 0, &rlocator, NULL, &blkno);

	{
		xl_flux_multi_insert *xlrec = (xl_flux_multi_insert *) XLogRecGetData(record);
		int			ntuples = xlrec->ntuples;
		char	   *cursor = (char *) xlrec + SizeOfFluxMultiInsert;
		XLogRedoAction action;
		OffsetNumber *final_offnums;
		bool	   *tuple_uncommitted;
		int			i;

		final_offnums = (OffsetNumber *) palloc0(ntuples * sizeof(OffsetNumber));
		tuple_uncommitted = (bool *) palloc0(ntuples * sizeof(bool));

		action = XLogReadBufferForRedo(record, 0, &buffer);

		if (action == BLK_RESTORED)
		{
			/*
			 * The full-page image already carries every tuple; recover the
			 * offsets from the per-tuple WAL headers so we can still register
			 * uncommitted tuples in the sLog below.
			 */
			char	   *c = cursor;

			for (i = 0; i < ntuples; i++)
			{
				xl_flux_multi_insert_tuple *thdr;

				c = (char *) SHORTALIGN(c);
				thdr = (xl_flux_multi_insert_tuple *) c;

				final_offnums[i] = thdr->offnum;
				c += SizeOfFluxMultiInsertTuple + thdr->datalen;
			}
		}

		if (action == BLK_NEEDS_REDO)
		{
			FluxPageOpaque phdr;
			Size		max_body = 0;

			page = BufferGetPage(buffer);

			if (PageIsNew(page))
				FluxInitPage(page, BufferGetPageSize(buffer));

			for (i = 0; i < ntuples; i++)
			{
				xl_flux_multi_insert_tuple *thdr;
				char	   *body;
				Size		datalen;
				OffsetNumber inserted_offnum;

				cursor = (char *) SHORTALIGN(cursor);
				thdr = (xl_flux_multi_insert_tuple *) cursor;
				body = cursor + SizeOfFluxMultiInsertTuple;
				datalen = thdr->datalen;

				cursor = body + datalen;

				if (datalen == 0)
				{
					elog(WARNING, "FLUX MULTI_INSERT redo: zero-length tuple "
						 "%d on block %u; skipping", i, blkno);
					continue;
				}

				/*
				 * Use InvalidOffsetNumber so PageAddItem packs sequentially,
				 * matching the original batch insertion order.  Skip on
				 * failure (the page may have been advanced past this record
				 * by a later checkpointed operation), exactly as the single
				 * INSERT redo path does.
				 */
				inserted_offnum = PageAddItem(page, body, datalen,
											  InvalidOffsetNumber, false, false);
				if (inserted_offnum == InvalidOffsetNumber)
				{
					elog(WARNING, "FLUX MULTI_INSERT redo: failed to add "
						 "tuple %d on block %u (page may have been modified by "
						 "a later operation); skipping", i, blkno);
					continue;
				}

				final_offnums[i] = inserted_offnum;
				if (datalen > max_body)
					max_body = datalen;

				/*
				 * Fix the tuple's t_ctid to point at itself.  Guard the
				 * PageGetItem dereference on ItemIdHasStorage as the single
				 * INSERT redo path does.
				 */
				{
					ItemId		itemid = PageGetItemId(page, inserted_offnum);

					if (ItemIdIsNormal(itemid) && ItemIdHasStorage(itemid))
					{
						FluxTupleHeader *inserted_hdr =
							(FluxTupleHeader *) PageGetItem(page, itemid);

						ItemPointerSet(&inserted_hdr->t_ctid, blkno, inserted_offnum);
					}
				}
			}

			phdr = FluxPageGetOpaque(page);
			FluxPageSetCommitTs(phdr, Max(FluxPageGetCommitTs(phdr), xlrec->commit_ts));

			/*
			 * Mark the page for defragmentation if fragmented, matching
			 * FluxPageAddTuple().  Use the largest tuple body in the batch as
			 * the heuristic threshold.
			 */
			if (max_body > 0 &&
				PageGetFreeSpace(page) >= max_body * 2 &&
				PageGetMaxOffsetNumber(page) > FirstOffsetNumber + 5)
			{
				FluxPageSetFlag(phdr, FLUX_PAGE_DEFRAG_NEEDED);
			}

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}

		/*
		 * Capture the UNCOMMITTED flag from each replayed tuple while we
		 * still hold the buffer (true for both BLK_NEEDS_REDO and
		 * BLK_RESTORED).
		 */
		if (BufferIsValid(buffer))
		{
			Page		curpage = BufferGetPage(buffer);
			OffsetNumber maxoff = PageGetMaxOffsetNumber(curpage);

			for (i = 0; i < ntuples; i++)
			{
				OffsetNumber off = final_offnums[i];
				ItemId		curiid;

				if (off == InvalidOffsetNumber || off > maxoff)
					continue;

				curiid = PageGetItemId(curpage, off);
				if (ItemIdIsNormal(curiid) && ItemIdHasStorage(curiid))
				{
					FluxTupleHeader *curhdr =
						(FluxTupleHeader *) PageGetItem(curpage, curiid);

					tuple_uncommitted[i] =
						(curhdr->t_flags & FLUX_TUPLE_UNCOMMITTED) != 0;
				}
			}
		}

		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);

		/*
		 * Register UNCOMMITTED tuples in the per-tuple sLog during replay, so
		 * a hot standby sees aborted batch rows as invisible until the CLR
		 * arrives.  Mirrors the single INSERT redo path.
		 */
		{
			TransactionId redo_xid = XLogRecGetXid(record);

			if (TransactionIdIsValid(redo_xid))
			{
				for (i = 0; i < ntuples; i++)
				{
					ItemPointerData tid;

					if (final_offnums[i] == InvalidOffsetNumber ||
						!tuple_uncommitted[i])
						continue;

					ItemPointerSet(&tid, blkno, final_offnums[i]);
					SLogTupleInsertRecovery(rlocator.relNumber, &tid,
											redo_xid, SLOG_OP_INSERT);
				}
			}
		}

		pfree(final_offnums);
		pfree(tuple_uncommitted);
	}
}

/*
 * flux_xlog_update_inplace_redo
 *		REDO handler for XLOG_FLUX_UPDATE_INPLACE.
 */
static void
flux_xlog_update_inplace_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_flux_update *xlrec = (xl_flux_update *) XLogRecGetData(record);
		XLogRedoAction action;

		/*
		 * WAL record layout depends on FLUX_WAL_PREFIX_SUFFIX flag:
		 *
		 * Without prefix/suffix: [xl_flux_update][full new tuple data] With
		 * prefix/suffix:    [xl_flux_update][xl_flux_prefix_suffix][diff
		 * bytes]
		 *
		 * Old tuple data is in the UNDO fork exclusively.
		 */
		char	   *after_header = (char *) xlrec + sizeof(xl_flux_update);
		bool		use_prefix_suffix = (xlrec->flags & FLUX_WAL_PREFIX_SUFFIX) != 0;
		xl_flux_prefix_suffix ps_info = {0, 0};
		char	   *diff_data = NULL;
		char	   *new_tuple_data = NULL;
		FluxTupleHeader *new_tuple_hdr = NULL;
		ItemId		itemid;
		FluxPageOpaque phdr;

		if (use_prefix_suffix)
		{
			memcpy(&ps_info, after_header, sizeof(xl_flux_prefix_suffix));
			diff_data = after_header + sizeof(xl_flux_prefix_suffix);
		}
		else
		{
			new_tuple_data = after_header;
			new_tuple_hdr = (FluxTupleHeader *) new_tuple_data;
		}

		action = XLogReadBufferForRedo(record, 0, &buffer);

		if (action == BLK_NEEDS_REDO)
		{
			char	   *blk0_ovf_data;
			Size		blk0_ovf_len;

			page = BufferGetPage(buffer);

			/*
			 * XLogInitBufferForRedo does standard PageInit for new pages, but
			 * doesn't set up FLUX opaque space. Initialize it here if needed.
			 */
			if (PageIsNew(page))
			{
				FluxInitPage(page, BufferGetPageSize(buffer));
			}

			/*
			 * Process overflow records on block 0 BEFORE the main tuple,
			 * matching the original insertion order.  During normal
			 * operation, overflow records stored on the same page as the main
			 * tuple (spatial locality) get lower offsets.  We must replay
			 * them first so the main tuple ends up at the correct offset.
			 */
			blk0_ovf_data = XLogRecGetBlockData(record, 0, &blk0_ovf_len);
			if (blk0_ovf_data != NULL && blk0_ovf_len > 0 &&
				(xlrec->flags & FLUX_WAL_HAS_OVERFLOW_BLK0))
			{
				char	   *ovf_ptr = blk0_ovf_data;
				Size		ovf_remaining = blk0_ovf_len;

				while (ovf_remaining > sizeof(xl_flux_overflow_write))
				{
					xl_flux_overflow_write *blk0_ovf_xlrec =
						(xl_flux_overflow_write *) ovf_ptr;
					char	   *actual_data = ovf_ptr + sizeof(xl_flux_overflow_write);
					Size		actual_len = blk0_ovf_xlrec->data_len;
					OffsetNumber ovf_offnum;

					if (ovf_remaining < sizeof(xl_flux_overflow_write) + actual_len)
						elog(PANIC, "FLUX UPDATE redo: corrupt overflow data on block 0");

					ovf_offnum = PageAddItem(page, actual_data, actual_len,
											 InvalidOffsetNumber, false, false);
					if (ovf_offnum == InvalidOffsetNumber)
						elog(PANIC, "FLUX UPDATE redo: failed to add overflow record on block 0");

					ovf_ptr += sizeof(xl_flux_overflow_write) + actual_len;
					ovf_remaining -= sizeof(xl_flux_overflow_write) + actual_len;
				}
			}

			/*
			 * Apply the update.  BLK_NEEDS_REDO is only returned when the
			 * page LSN < record LSN (no FPI).
			 *
			 * For cross-page out-of-place updates, the new tuple lives on the
			 * destination page (restored from its FPI).  Here we just mark
			 * the old tuple as UPDATED so visibility checks filter it
			 * correctly.
			 *
			 * Same-page out-of-place updates always force an FPI (see
			 * FluxXLogUpdate), so they get BLK_RESTORED and never reach this
			 * code path.
			 */
			itemid = PageGetItemId(page, xlrec->offnum);
			if (ItemIdIsNormal(itemid) && ItemIdHasStorage(itemid))
			{
				FluxTupleHeader *existing_tuple =
					(FluxTupleHeader *) PageGetItem(page, itemid);

				if (xlrec->flags & FLUX_WAL_CROSS_PAGE)
				{
					/*
					 * Cross-page out-of-place update: mark the old tuple as
					 * UPDATED.  The new version is on the destination page
					 * restored from its FPI.
					 */
					existing_tuple->t_flags |= FLUX_TUPLE_UPDATED;
					existing_tuple->t_flags &= ~FLUX_TUPLE_UNCOMMITTED;
					existing_tuple->t_commit_ts = xlrec->new_commit_ts;
				}
				else
				{
					Size		existing_len = ItemIdGetLength(itemid);

					if (use_prefix_suffix)
					{
						/*
						 * Prefix/suffix compressed update: reconstruct new
						 * tuple by patching the diff bytes into the existing
						 * tuple data on the page.
						 *
						 * The emitter computes prefix/suffix/difflen against
						 * new_tuple->t_len (a same-size update only), so redo
						 * must anchor on xlrec->new_tuple_len -- NOT the
						 * on-page slot length.  A prior non-shrinking shrink
						 * update can leave the slot larger than the tuple's
						 * logical length (see flux_operations.c), so using
						 * existing_len here would over-copy past the diff
						 * bytes and corrupt the tuple's suffix.
						 */
						int			difflen = (int) xlrec->new_tuple_len -
							ps_info.prefixlen - ps_info.suffixlen;

						if (difflen < 0 ||
							ps_info.prefixlen + ps_info.suffixlen > xlrec->new_tuple_len)
							elog(PANIC, "FLUX UPDATE REDO: invalid prefix/suffix "
								 "(prefix=%u, suffix=%u, tuple_len=%u)",
								 ps_info.prefixlen, ps_info.suffixlen,
								 xlrec->new_tuple_len);

						if (difflen > 0)
							memcpy((char *) existing_tuple + ps_info.prefixlen,
								   diff_data, difflen);
					}
					else if (xlrec->new_tuple_len <= existing_len)
					{
						/*
						 * Full new tuple: overwrite in place.  Do NOT shrink
						 * the line pointer; the write path keeps the original
						 * slot length so the undo before-image can be
						 * restored into it (see flux_operations.c).  Redo
						 * must produce the identical slot length or WAL
						 * consistency checking
						 * (wal_consistency_checking=flux) will diverge.
						 */
						memcpy(existing_tuple, new_tuple_hdr, xlrec->new_tuple_len);
					}
					else
					{
						/*
						 * Should not happen: growing updates force FPI via
						 * REGBUF_FORCE_IMAGE, so BLK_NEEDS_REDO is never
						 * returned for them.
						 */
						elog(PANIC, "FLUX UPDATE REDO: new tuple (%u) larger "
							 "than existing slot (%zu) without FPI",
							 xlrec->new_tuple_len, existing_len);
					}
				}
			}
			else
			{
				elog(DEBUG1, "FLUX UPDATE REDO: ItemId at offnum=%u is not normal", xlrec->offnum);
			}

			/* Update page header */
			phdr = FluxPageGetOpaque(page);
			FluxPageSetCommitTs(phdr, Max(FluxPageGetCommitTs(phdr), xlrec->new_commit_ts));

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);

		/*
		 * Process overflow buffers on separate pages (buffers 1..N) for
		 * UPDATE.  Multiple overflow records may share a single block due to
		 * spatial locality, so we loop through all records within each
		 * block's data (matching INSERT redo).
		 */
		for (int ovf_idx = 1; ovf_idx < XLR_MAX_BLOCK_ID; ovf_idx++)
		{
			Buffer		ovf_buffer;
			Page		ovf_page;
			XLogRedoAction ovf_action;

			if (!XLogRecHasBlockRef(record, ovf_idx))
				break;			/* No more overflow buffers */

			ovf_action = XLogReadBufferForRedo(record, (uint8) ovf_idx, &ovf_buffer);
			if (ovf_action == BLK_NEEDS_REDO)
			{
				char	   *ovf_data;
				Size		ovf_len;

				ovf_page = BufferGetPage(ovf_buffer);

				/* Initialize as FLUX page if new */
				if (PageIsNew(ovf_page))
				{
					FluxInitPage(ovf_page, BufferGetPageSize(ovf_buffer));
				}

				/* Get the overflow record data from WAL */
				ovf_data = XLogRecGetBlockData(record, (uint8) ovf_idx, &ovf_len);
				if (ovf_data != NULL && ovf_len > 0)
				{
					char	   *ovf_ptr = ovf_data;
					Size		ovf_remaining = ovf_len;

					/*
					 * Parse and replay all overflow records on this block.
					 * Multiple overflow records may share a page due to
					 * spatial locality.
					 */
					while (ovf_remaining > sizeof(xl_flux_overflow_write))
					{
						xl_flux_overflow_write *ovf_xlrec2 =
							(xl_flux_overflow_write *) ovf_ptr;
						char	   *actual_data = ovf_ptr + sizeof(xl_flux_overflow_write);
						Size		actual_len = ovf_xlrec2->data_len;
						OffsetNumber ovf_offnum;

						if (ovf_remaining < sizeof(xl_flux_overflow_write) + actual_len)
							elog(PANIC, "FLUX UPDATE redo: corrupt overflow data on block %u",
								 BufferGetBlockNumber(ovf_buffer));

						/*
						 * Use InvalidOffsetNumber to append sequentially,
						 * matching the original insertion order within this
						 * page.
						 */
						ovf_offnum = PageAddItem(ovf_page, actual_data, actual_len,
												 InvalidOffsetNumber, false, false);
						if (ovf_offnum == InvalidOffsetNumber)
							elog(PANIC, "FLUX UPDATE redo: failed to add overflow record on block %u",
								 BufferGetBlockNumber(ovf_buffer));

						ovf_ptr += sizeof(xl_flux_overflow_write) + actual_len;
						ovf_remaining -= sizeof(xl_flux_overflow_write) + actual_len;
					}
				}

				PageSetLSN(ovf_page, record->EndRecPtr);
				MarkBufferDirty(ovf_buffer);
			}
			if (BufferIsValid(ovf_buffer))
				UnlockReleaseBuffer(ovf_buffer);
		}
	}
}

/*
 * flux_xlog_delete_redo
 *		REDO handler for XLOG_FLUX_DELETE.
 */
static void
flux_xlog_delete_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_flux_delete *xlrec = (xl_flux_delete *) XLogRecGetData(record);
		XLogRedoAction action;
		ItemId		itemid;
		FluxPageOpaque phdr;

		/*
		 * WAL record contains only the delete header (offset + commit_ts).
		 * Old tuple data is stored exclusively in the UNDO fork for
		 * transaction rollback and is not needed here.
		 */

		action = XLogReadBufferForRedo(record, 0, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			/*
			 * XLogInitBufferForRedo does standard PageInit for new pages, but
			 * doesn't set up FLUX opaque space. Initialize it here if needed.
			 */
			if (PageIsNew(page))
			{
				FluxInitPage(page, BufferGetPageSize(buffer));
			}

			/* REDO: Mark tuple as deleted */
			itemid = PageGetItemId(page, xlrec->offnum);
			if (ItemIdIsNormal(itemid) && ItemIdHasStorage(itemid))
			{
				FluxTupleHeader *tuple =
					(FluxTupleHeader *) PageGetItem(page, itemid);

				tuple->t_flags |= FLUX_TUPLE_DELETED;
				tuple->t_commit_ts = xlrec->commit_ts;
			}

			/* Update page header */
			phdr = FluxPageGetOpaque(page);
			FluxPageSetCommitTs(phdr, Max(FluxPageGetCommitTs(phdr), xlrec->commit_ts));
			FluxPageSetFlag(phdr, FLUX_PAGE_DEFRAG_NEEDED);

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * flux_xlog_defrag_redo
 *		REDO handler for XLOG_FLUX_DEFRAG.
 */
static void
flux_xlog_defrag_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_flux_defrag *xlrec = (xl_flux_defrag *) XLogRecGetData(record);
		XLogRedoAction action;

		FluxPageOpaque phdr;

		action = XLogReadBufferForRedo(record, 0, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			/*
			 * XLogInitBufferForRedo does standard PageInit for new pages, but
			 * doesn't set up FLUX opaque space. Initialize it here if needed.
			 */
			if (PageIsNew(page))
			{
				FluxInitPage(page, BufferGetPageSize(buffer));
			}

			/* Defragment the page */
			PageRepairFragmentation(page);

			/* Update page header */
			phdr = FluxPageGetOpaque(page);
			FluxPageSetCommitTs(phdr, Max(FluxPageGetCommitTs(phdr), xlrec->commit_ts));
			FluxPageClearFlag(phdr, FLUX_PAGE_DEFRAG_NEEDED);

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * flux_xlog_overflow_write_redo
 *		REDO handler for XLOG_FLUX_OVERFLOW_WRITE.
 */
static void
flux_xlog_overflow_write_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_flux_overflow_write *xlrec =
			(xl_flux_overflow_write *) XLogRecGetData(record);
		char	   *record_data = (char *) xlrec + sizeof(xl_flux_overflow_write);
		XLogRedoAction action;

		action = XLogReadBufferForRedo(record, 0, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			/* Initialize as normal FLUX page if needed */
			if (PageIsNew(page))
			{
				FluxInitPage(page, BufferGetPageSize(buffer));
			}

			if (xlrec->flags & FLUX_OVERFLOW_WAL_LINK_UPDATE)
			{
				/*
				 * Link update: overwrite the existing overflow record header
				 * at the specified offset with updated chain pointers.
				 */
				ItemId		itemid;

				itemid = PageGetItemId(page, xlrec->offnum);
				if (ItemIdIsNormal(itemid) && ItemIdHasStorage(itemid))
				{
					FluxOverflowRecordHeader *existing_hdr =
						(FluxOverflowRecordHeader *) PageGetItem(page, itemid);

					memcpy(existing_hdr, record_data,
						   sizeof(FluxOverflowRecordHeader));
				}
			}
			else
			{
				/*
				 * New overflow record: the logged data is the complete record
				 * (FluxOverflowRecordHeader + chunk data). Add it to the page
				 * at the specified offset.
				 */
				OffsetNumber offnum;

				offnum = PageAddItem(page, record_data, xlrec->data_len,
									 xlrec->offnum, false, false);
				if (offnum == InvalidOffsetNumber)
					elog(ERROR, "failed to add overflow record to page during redo");
			}

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * flux_xlog_compress_redo
 *		REDO handler for XLOG_FLUX_COMPRESS.
 */
static void
flux_xlog_compress_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_flux_compress *xlrec = (xl_flux_compress *) XLogRecGetData(record);
		XLogRedoAction action;

		ItemId		itemid;
		FluxPageOpaque phdr;

		action = XLogReadBufferForRedo(record, 0, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			/*
			 * XLogInitBufferForRedo does standard PageInit for new pages, but
			 * doesn't set up FLUX opaque space. Initialize it here if needed.
			 */
			if (PageIsNew(page))
			{
				FluxInitPage(page, BufferGetPageSize(buffer));
			}

			/* Apply compression to the tuple attribute */
			itemid = PageGetItemId(page, xlrec->offnum);
			if (ItemIdIsNormal(itemid) && ItemIdHasStorage(itemid))
			{
				FluxTupleHeader *tuple =
					(FluxTupleHeader *) PageGetItem(page, itemid);

				/* Mark tuple as compressed */
				tuple->t_flags |= FLUX_TUPLE_COMPRESSED;
				tuple->t_infomask |= FLUX_INFOMASK_COMPRESSED;
				tuple->t_commit_ts = xlrec->commit_ts;
			}

			/* Update page header */
			phdr = FluxPageGetOpaque(page);
			FluxPageSetCommitTs(phdr, Max(FluxPageGetCommitTs(phdr), xlrec->commit_ts));

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * flux_xlog_init_page_redo
 *		REDO handler for XLOG_FLUX_INIT_PAGE.
 */
static void
flux_xlog_init_page_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_flux_init_page *xlrec = (xl_flux_init_page *) XLogRecGetData(record);
		XLogRedoAction action;

		action = XLogReadBufferForRedoExtended(record, 0, RBM_ZERO_AND_LOCK, false, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			FluxPageOpaque phdr;

			page = BufferGetPage(buffer);

			/* Initialize page with FLUX opaque space */
			FluxInitPage(page, BufferGetPageSize(buffer));

			/* Override commit_ts and flags from WAL record */
			phdr = FluxPageGetOpaque(page);
			phdr->pd_commit_ts_and_flags = ((uint64) (xlrec->commit_ts) & FLUX_PAGE_TS_MASK) | (uint64) (xlrec->flags);

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * flux_xlog_cross_page_defrag_redo
 *		REDO handler for XLOG_FLUX_CROSS_PAGE_DEFRAG.
 */
static void
flux_xlog_cross_page_defrag_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_flux_cross_page_defrag *xlrec =
			(xl_flux_cross_page_defrag *) XLogRecGetData(record);
		char	   *tuple_data = (char *) xlrec +
			sizeof(xl_flux_cross_page_defrag);
		XLogRedoAction dst_action;
		XLogRedoAction src_action;

		/*
		 * Redo the target page (block 0): insert the moved tuple.
		 * XLogReadBufferForRedo will skip replay if FPI is present.
		 */
		dst_action = XLogReadBufferForRedo(record, 0, &buffer);
		if (dst_action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			if (PageAddItem(page, tuple_data, xlrec->tuple_len,
							xlrec->dst_offnum, false, false)
				== InvalidOffsetNumber)
			{
				/*
				 * Defensive: with REGBUF_FORCE_IMAGE this path should be
				 * unreachable, but if it ever fires we must not PANIC —
				 * skip the move and let the source page processing proceed.
				 */
				elog(DEBUG1, "flux cross-page defrag: insufficient space on target page during redo");
				if (BufferIsValid(buffer))
					UnlockReleaseBuffer(buffer);
				goto process_source;
			}

			/* Update ctid in the new copy to point to itself */
			{
				ItemId		dst_itemid;
				FluxTupleHeader *dst_hdr;
				BlockNumber dst_blkno;

				XLogRecGetBlockTag(record, 0, NULL, NULL, &dst_blkno);
				dst_itemid = PageGetItemId(page, xlrec->dst_offnum);
				if (ItemIdIsNormal(dst_itemid) && ItemIdHasStorage(dst_itemid))
				{
					dst_hdr = (FluxTupleHeader *) PageGetItem(page, dst_itemid);
					ItemPointerSet(&dst_hdr->t_ctid, dst_blkno,
								   xlrec->dst_offnum);
				}
			}

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);

		/*
		 * Redo the source page (block 1): mark the old slot unused.
		 */
process_source:
		src_action = XLogReadBufferForRedo(record, 1, &buffer);
		if (src_action == BLK_NEEDS_REDO)
		{
			ItemId		src_itemid;

			page = BufferGetPage(buffer);
			src_itemid = PageGetItemId(page, xlrec->src_offnum);
			ItemIdSetUnused(src_itemid);

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * flux_xlog_vm_set_redo
 *		REDO handler for XLOG_FLUX_VM_SET.
 */
static void
flux_xlog_vm_set_redo(XLogReaderState *record)
{
	{
		xl_flux_vm_set *xlrec = (xl_flux_vm_set *) XLogRecGetData(record);
		Buffer		vmBuf;
		Page		vmPage;
		uint32		mapByte;
		uint8		mapOffset;
		uint8	   *map;

		/*
		 * Block 0 is the heap buffer, registered with REGBUF_NO_CHANGE.  We
		 * don't need to redo it since the heap page is not modified by VM
		 * operations.
		 */

		/* Redo VM buffer (block 1) */
		if (XLogReadBufferForRedo(record, 1, &vmBuf) == BLK_NEEDS_REDO)
		{
			vmPage = BufferGetPage(vmBuf);

			/* Calculate the VM byte and offset for this heap block */
			mapByte = (xlrec->heapBlk % ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) * 4)) / 4;
			mapOffset = (xlrec->heapBlk % ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) * 4)) % 4;

			map = (uint8 *) PageGetContents(vmPage);
			map[mapByte] |= (xlrec->flags << (mapOffset * 2));

			PageSetLSN(vmPage, record->EndRecPtr);
			MarkBufferDirty(vmBuf);
		}
		if (BufferIsValid(vmBuf))
			UnlockReleaseBuffer(vmBuf);
	}
}

/*
 * flux_xlog_vm_clear_redo
 *		REDO handler for XLOG_FLUX_VM_CLEAR.
 */
static void
flux_xlog_vm_clear_redo(XLogReaderState *record)
{
	{
		xl_flux_vm_clear *xlrec = (xl_flux_vm_clear *) XLogRecGetData(record);
		Buffer		vmBuf;
		Page		vmPage;
		uint32		mapByte;
		uint8		mapOffset;
		uint8	   *map;

		/*
		 * Block 0 is the heap buffer, registered with REGBUF_NO_CHANGE --
		 * skip it.
		 */

		/* Redo VM buffer (block 1) */
		if (XLogReadBufferForRedo(record, 1, &vmBuf) == BLK_NEEDS_REDO)
		{
			vmPage = BufferGetPage(vmBuf);

			/* Calculate the VM byte and offset for this heap block */
			mapByte = (xlrec->heapBlk % ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) * 4)) / 4;
			mapOffset = (xlrec->heapBlk % ((BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) * 4)) % 4;

			map = (uint8 *) PageGetContents(vmPage);
			map[mapByte] &= ~(xlrec->flags << (mapOffset * 2));

			PageSetLSN(vmPage, record->EndRecPtr);
			MarkBufferDirty(vmBuf);
		}
		if (BufferIsValid(vmBuf))
			UnlockReleaseBuffer(vmBuf);
	}
}

/*
 * flux_xlog_lock_redo
 *		REDO handler for XLOG_FLUX_LOCK.
 */
static void
flux_xlog_lock_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_flux_lock *xlrec = (xl_flux_lock *) XLogRecGetData(record);
		XLogRedoAction action;

		action = XLogReadBufferForRedo(record, 0, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			ItemId		itemid;

			page = BufferGetPage(buffer);

			if (PageIsNew(page))
			{
				FluxInitPage(page, BufferGetPageSize(buffer));
			}

			itemid = PageGetItemId(page, xlrec->offnum);
			if (ItemIdIsNormal(itemid) && ItemIdHasStorage(itemid))
			{
				FluxTupleHeader *tuple =
					(FluxTupleHeader *) PageGetItem(page, itemid);

				/* Apply the lock state from the WAL record */
				tuple->t_infomask = xlrec->infomask;
				tuple->t_flags |= FLUX_TUPLE_LOCKED;
			}

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * flux_xlog_cas_update_redo
 *		REDO handler for XLOG_FLUX_CAS_UPDATE.
 *
 * Patches a contiguous byte range within a tuple on the page.  The record
 * carries only the changed bytes (data_offset..data_offset+data_len) and
 * the new commit timestamp.  Idempotent memcpy; safe for replay.
 */
static void
flux_xlog_cas_update_redo(XLogReaderState *record)
{
	xl_flux_cas_update *xlrec = (xl_flux_cas_update *) XLogRecGetData(record);
	char	   *new_data = ((char *) xlrec) + sizeof(xl_flux_cas_update);
	Buffer		buffer;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);
		ItemId		itemid;
		FluxTupleHeader *tuple;

		itemid = PageGetItemId(page, xlrec->offnum);
		if (!ItemIdIsNormal(itemid) || !ItemIdHasStorage(itemid))
			elog(PANIC, "FLUX CAS_UPDATE redo: invalid item at offset %u",
				 xlrec->offnum);

		tuple = (FluxTupleHeader *) PageGetItem(page, itemid);

		/* Patch the changed data bytes */
		memcpy(((char *) tuple) + xlrec->data_offset, new_data, xlrec->data_len);

		/* Update commit timestamp */
		tuple->t_commit_ts = xlrec->new_commit_ts;

		/* Ensure t_writer is cleared (crash may have left it non-zero) */
		tuple->t_writer = 0;

		PageSetLSN(page, record->EndRecPtr);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

/*
 * flux_xlog_cas_update_undo_redo
 *		REDO handler for XLOG_FLUX_CAS_UPDATE_UNDO (FOLD variant).
 *
 * Replays two page changes from one record:
 *   block 0 - main-fork page: patch the tuple data bytes, set commit ts,
 *             clear t_writer (identical to flux_xlog_cas_update_redo).
 *   block 1 - relundo-fork data page: write the UNDO before-image
 *             (identical to relundo_redo_insert; new pages honor WILL_INIT
 *             via xlrec->is_new_page, existing pages memcpy at page_offset).
 *   block 2 - relundo metapage: FPI restore, present only when is_new_page.
 *
 * Both page redos are idempotent memcpys; safe for replay.
 */
static void
flux_xlog_cas_update_undo_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	xl_flux_cas_update_undo *xlrec =
		(xl_flux_cas_update_undo *) XLogRecGetData(record);
	char	   *new_data = ((char *) xlrec) + SizeOfFluxCasUpdateUndo;
	Buffer		buffer;
	XLogRedoAction action;
	bool		has_metapage = XLogRecHasBlockRef(record, 2);

	/* ---- block 0: main-fork redo byte-diff ---- */
	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);
		ItemId		itemid;
		FluxTupleHeader *tuple;

		itemid = PageGetItemId(page, xlrec->offnum);
		if (!ItemIdIsNormal(itemid) || !ItemIdHasStorage(itemid))
			elog(PANIC, "FLUX CAS_UPDATE_UNDO redo: invalid item at offset %u",
				 xlrec->offnum);

		tuple = (FluxTupleHeader *) PageGetItem(page, itemid);

		memcpy(((char *) tuple) + xlrec->data_offset, new_data, xlrec->data_len);
		tuple->t_commit_ts = xlrec->new_commit_ts;
		tuple->t_writer = 0;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* ---- block 1: relundo-fork UNDO before-image ---- */
	if (xlrec->urec_len < SizeOfRelUndoRecordHeader)
		elog(PANIC, "CAS_UPDATE_UNDO redo: invalid urec_len %u (min %zu)",
			 xlrec->urec_len, SizeOfRelUndoRecordHeader);
	if (xlrec->page_offset > BLCKSZ - sizeof(RelUndoPageHeaderData))
		elog(PANIC, "CAS_UPDATE_UNDO redo: invalid page offset %u",
			 xlrec->page_offset);
	if (xlrec->new_pd_lower > BLCKSZ)
		elog(PANIC, "CAS_UPDATE_UNDO redo: pd_lower %u exceeds page size",
			 xlrec->new_pd_lower);
	if ((uint32) xlrec->page_offset + (uint32) xlrec->urec_len > BLCKSZ)
		elog(PANIC, "CAS_UPDATE_UNDO redo: record extends past page end (offset %u + len %u > %u)",
			 xlrec->page_offset, xlrec->urec_len, (uint32) BLCKSZ);
	if (xlrec->new_pd_lower + MAXALIGN(SizeOfPageHeaderData) < xlrec->page_offset)
		elog(PANIC, "CAS_UPDATE_UNDO redo: new_pd_lower %u precedes page_offset %u",
			 xlrec->new_pd_lower, xlrec->page_offset);
	if (xlrec->urec_type < RELUNDO_INSERT || xlrec->urec_type > RELUNDO_UPDATE)
		elog(PANIC, "CAS_UPDATE_UNDO redo: invalid record type %u", xlrec->urec_type);

	if (xlrec->is_new_page)
	{
		buffer = XLogInitBufferForRedo(record, 1);
		action = BLK_NEEDS_REDO;
	}
	else
		action = XLogReadBufferForRedo(record, 1, &buffer);

	if (action == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);
		char	   *record_data;
		Size		record_len;

		record_data = XLogRecGetBlockData(record, 1, &record_len);
		if (record_data == NULL || record_len == 0)
			elog(PANIC, "CAS_UPDATE_UNDO redo: no block data for UNDO record");
		if (record_len > BLCKSZ)
			elog(PANIC, "CAS_UPDATE_UNDO redo: block data too large (%zu bytes)", record_len);

		if (xlrec->is_new_page)
		{
			char	   *contents;

			if (record_len < SizeOfRelUndoPageHeaderData)
				elog(PANIC, "CAS_UPDATE_UNDO redo: INIT_PAGE block data too small (%zu < %zu)",
					 record_len, SizeOfRelUndoPageHeaderData);
			if (record_len > BLCKSZ - MAXALIGN(SizeOfPageHeaderData))
				elog(PANIC, "CAS_UPDATE_UNDO redo: INIT_PAGE block data too large (%zu bytes)",
					 record_len);

			PageInit(page, BLCKSZ, 0);
			contents = PageGetContents(page);
			memcpy(contents, record_data, record_len);
		}
		else
		{
			RelUndoPageHeader undohdr = (RelUndoPageHeader) PageGetContents(page);

			if (undohdr->pd_lower > BLCKSZ)
				elog(PANIC, "CAS_UPDATE_UNDO redo: existing pd_lower %u exceeds page size",
					 undohdr->pd_lower);

			memcpy((char *) page + xlrec->page_offset, record_data, record_len);
			undohdr->pd_lower = xlrec->new_pd_lower;
			undohdr->max_xid = xlrec->max_xid;

			if (undohdr->pd_lower + MAXALIGN(SizeOfPageHeaderData) < xlrec->page_offset + record_len)
				elog(PANIC, "CAS_UPDATE_UNDO redo: pd_lower %u too small for offset %u + len %zu",
					 undohdr->pd_lower, xlrec->page_offset, record_len);
		}

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* ---- block 2: relundo metapage FPI ---- */
	if (has_metapage)
	{
		(void) XLogReadBufferForRedo(record, 2, &buffer);
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * flux_xlog_write_dict_redo
 *		REDO handler for XLOG_FLUX_WRITE_DICT.
 *
 * The record always carries a forced full-page image of the dictionary-fork
 * block, so XLogReadBufferForRedo restores the page directly and there is no
 * delta to replay.  We never reach BLK_NEEDS_REDO without the FPI present;
 * the handler simply restores and releases the buffer.
 */
static void
flux_xlog_write_dict_redo(XLogReaderState *record)
{
	Buffer		buffer;

	/*
	 * With REGBUF_FORCE_IMAGE the page is reconstructed from the FPI by
	 * XLogReadBufferForRedo, which returns BLK_RESTORED.  BLK_NEEDS_REDO is
	 * not expected, but if it ever occurs there is nothing to apply.
	 */
	(void) XLogReadBufferForRedo(record, 0, &buffer);
	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

/*
 * flux_redo
 *		Thin dispatcher for all XLOG_FLUX_* opcodes.  Each case is
 *	delegated to a dedicated per-opcode static helper above.
 */
void
flux_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;


	switch (info)
	{
		case XLOG_FLUX_INSERT:
			flux_xlog_insert_redo(record);
			break;

		case XLOG_FLUX_UPDATE_INPLACE:
			flux_xlog_update_inplace_redo(record);
			break;

		case XLOG_FLUX_DELETE:
			flux_xlog_delete_redo(record);
			break;

		case XLOG_FLUX_DEFRAG:
			flux_xlog_defrag_redo(record);
			break;

		case XLOG_FLUX_OVERFLOW_WRITE:
			flux_xlog_overflow_write_redo(record);
			break;

		case XLOG_FLUX_COMPRESS:
			flux_xlog_compress_redo(record);
			break;

		case XLOG_FLUX_INIT_PAGE:
			flux_xlog_init_page_redo(record);
			break;

		case XLOG_FLUX_CROSS_PAGE_DEFRAG:
			flux_xlog_cross_page_defrag_redo(record);
			break;

		case XLOG_FLUX_VM_SET:
			flux_xlog_vm_set_redo(record);
			break;

		case XLOG_FLUX_VM_CLEAR:
			flux_xlog_vm_clear_redo(record);
			break;

		case XLOG_FLUX_LOCK:
			flux_xlog_lock_redo(record);
			break;

		case XLOG_FLUX_CAS_UPDATE:
			flux_xlog_cas_update_redo(record);
			break;

		case XLOG_FLUX_CAS_UPDATE_UNDO:
			flux_xlog_cas_update_undo_redo(record);
			break;

		case XLOG_FLUX_WRITE_DICT:
			flux_xlog_write_dict_redo(record);
			break;

		case XLOG_FLUX_MULTI_INSERT:
			flux_xlog_multi_insert_redo(record);
			break;

		default:
			elog(PANIC, "flux_redo: unknown op code %u", info);
	}
}


/*
 * Mask function for FLUX pages (for consistency checking)
 */
void
flux_mask(char *page, BlockNumber blkno)
{
	Page		flux_page = (Page) page;
	FluxPageOpaque phdr;
	bool		is_overflow;
	OffsetNumber offnum;
	OffsetNumber maxoff;

	mask_page_lsn_and_checksum(flux_page);

	mask_page_hint_bits(flux_page);
	mask_unused_space(flux_page);

	/*
	 * Dictionary-fork pages (XLOG_FLUX_WRITE_DICT full-page images) have no
	 * opaque special area and no line-pointer array; they are initialized
	 * with PageInit(page, BLCKSZ, 0).  FluxPageGetOpaque would read 8 bytes
	 * past the page end and the line-pointer loop below would mis-mask dict
	 * payload, producing spurious "inconsistent pages" failures under
	 * wal_consistency_checking.  Detect them by their zero special size and
	 * leave the (already LSN/checksum/hint-masked) page untouched.
	 */
	if (PageGetSpecialSize(flux_page) != MAXALIGN(sizeof(FluxPageOpaqueData)))
		return;

	phdr = FluxPageGetOpaque(flux_page);

	/* Check page type before masking flags */
	is_overflow = (phdr->pd_commit_ts_and_flags & FLUX_PAGE_OVERFLOW) != 0;

	/*
	 * Mask the entire packed commit_ts_and_flags field.
	 *
	 * The timestamp uses Max(existing, new) during redo which can produce a
	 * different value if the page was concurrently modified.  Heuristic flags
	 * (e.g., FLUX_PAGE_DEFRAG_NEEDED) may be set by redo but not by the
	 * original operation, or vice versa.
	 */
	phdr->pd_commit_ts_and_flags = 0;

	/*
	 * Overflow pages contain FluxOverflowRecordHeader items, not regular
	 * tuples.  Their contents are fully determined by the WAL data, so no
	 * per-item masking is needed.
	 */
	if (is_overflow)
		return;

	/*
	 * Mask tuple-level fields that function as hint bits and are not
	 * faithfully reproduced by WAL redo.  The redo handlers only set the
	 * minimal fields needed for correctness (t_flags, t_commit_ts);
	 * transactional fields like infomask bits are set on the primary but not
	 * replayed.
	 */
	maxoff = PageGetMaxOffsetNumber(flux_page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		ItemId		itemid = PageGetItemId(flux_page, offnum);
		FluxTupleHeader *tuple_hdr;

		if (!ItemIdIsNormal(itemid) || !ItemIdHasStorage(itemid))
			continue;

		tuple_hdr = (FluxTupleHeader *) PageGetItem(flux_page, itemid);
		tuple_hdr->t_infomask = 0;
		tuple_hdr->t_flags = 0;
		tuple_hdr->t_commit_ts = 0;
		tuple_hdr->t_writer = 0;	/* transient CAS lock, not replayed */
		ItemPointerSetInvalid(&tuple_hdr->t_ctid);
	}
}
