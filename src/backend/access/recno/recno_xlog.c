/*-------------------------------------------------------------------------
 *
 * recno_xlog.c
 *	  RECNO WAL (Write-Ahead Logging) implementation
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_xlog.c
 *
 * NOTES
 *	  This implements WAL logging for RECNO operations, providing
 *	  UNDO/REDO functionality for crash recovery. Unlike heap,
 *	  RECNO uses in-place updates with before/after images.
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
#include "access/recno.h"
#include "access/recno_xlog.h"
#include "access/bufmask.h"
#include "access/slog.h"
#include "access/xloginsert.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "miscadmin.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

/*
 * RecnoXLogMaybeAppendLogicalTuple
 *		Append a heap-format image of `rtup` to the in-progress WAL record
 *		if `rel` is logically logged.  Returns true and sets
 *		RECNO_WAL_LOGICAL_TUPLE in `*flags` if the image was appended.
 *
 *	The heap image is what logical decoding consumes.  Physical REDO
 *	reads the RECNO-format tuple that precedes this region.  By writing
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
 *	the record (which may vary with compression / HLC / cross-page).
 *
 *	For UPDATE we append two back-to-back trailers (old then new); see
 *	RecnoXLogUpdate.
 */
static bool
RecnoXLogMaybeAppendLogicalTuple(Relation rel, RecnoTuple rtup, uint16 *flags)
{
	TupleDesc	tupdesc;
	Datum	   *values;
	bool	   *isnull;
	HeapTuple	heaptup;
	uint32		len;

	if (rel == NULL || rtup == NULL || !RelationIsLogicallyLogged(rel))
		return false;

	tupdesc = RelationGetDescr(rel);
	values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
	isnull = (bool *) palloc(tupdesc->natts * sizeof(bool));

	RecnoDeformTuple(rtup, tupdesc, values, isnull);
	heaptup = heap_form_tuple(tupdesc, values, isnull);

	len = (uint32) heaptup->t_len;
	XLogRegisterData((char *) heaptup->t_data, heaptup->t_len);
	XLogRegisterData((char *) &len, sizeof(uint32));

	heap_freetuple(heaptup);
	pfree(values);
	pfree(isnull);

	*flags |= RECNO_WAL_LOGICAL_TUPLE;
	return true;
}

/* ----------------------------------------------------------------
 *					HLC Uncertainty Handling
 *
 * These functions implement replica-side handling of HLC uncertainty
 * intervals.  When a replica applies a WAL record that carries HLC
 * data, it must ensure causal consistency by advancing its local HLC
 * past the commit timestamp.  If the replica's clock is within the
 * uncertainty window, it may optionally wait for the physical clock
 * to pass the window before serving reads.
 * ----------------------------------------------------------------
 */

/*
 * RecnoReplicaHandleUncertainty -- handle uncertainty on the replica side.
 *
 * When a replica applies a WAL record, the commit HLC may be in the
 * "future" relative to the replica's own clock.  If the replica's HLC
 * falls within the uncertainty window, we must either:
 *   (a) Advance the replica's HLC past the uncertainty upper bound, or
 *   (b) Wait until the physical clock passes the uncertainty window.
 *
 * The choice is controlled by the recno_uncertainty_wait GUC:
 *   - true:  sleep until physical clock >= commit_hlc + uncertainty_ms
 *   - false: immediately advance the local HLC past the window
 *
 * Either way, after this function returns, the replica's HLC is >= the
 * commit HLC, ensuring causal consistency for subsequent reads.
 */
void
RecnoReplicaHandleUncertainty(HLCTimestamp commit_hlc, int32 uncertainty_ms)
{
	uint64		commit_phys;
	uint64		upper_phys;

	if (!recno_use_hlc)
		return;

	commit_phys = HLCGetPhysical(commit_hlc);
	upper_phys = commit_phys + (uint64) uncertainty_ms;

	if (recno_uncertainty_wait)
	{
		/*
		 * Wait mode: spin until physical clock advances past the uncertainty
		 * window.  We use short sleeps to avoid busy-waiting.
		 *
		 * This ensures that when the replica serves reads after applying this
		 * WAL record, its physical clock is past the uncertainty window, so
		 * there is no ambiguity about ordering.
		 */
		for (;;)
		{
			TimestampTz now = GetCurrentTimestamp();
			uint64		now_ms = (uint64) now / 1000;

			if (now_ms >= upper_phys)
				break;

			CHECK_FOR_INTERRUPTS();

			{
				long		remaining_ms = (long) (upper_phys - now_ms);

				if (remaining_ms > 10)
					remaining_ms = 10;
				if (remaining_ms > 0)
					pg_usleep(remaining_ms * 1000);
			}
		}
	}

	/*
	 * Advance the local HLC to at least the commit_hlc.  This is done
	 * regardless of wait mode -- the replica's HLC must always move forward
	 * to respect causal ordering.
	 *
	 * HLCNow with msg_hlc = commit_hlc ensures the local HLC advances past
	 * the commit timestamp (the "receive" variant of the HLC algorithm).
	 */
	(void) HLCNow(commit_hlc);
}

/*
 * RecnoReplicaAdvanceHLC -- advance replica HLC to a specific target.
 *
 * Simple wrapper for HLCNow that ensures the replica's HLC moves
 * past the given target.  Used when the caller already knows the
 * exact target timestamp (e.g., the uncertainty upper bound).
 */
void
RecnoReplicaAdvanceHLC(HLCTimestamp target_hlc)
{
	if (!recno_use_hlc || target_hlc == InvalidHLCTimestamp)
		return;

	(void) HLCNow(target_hlc);
}

/*
 * recno_redo_handle_hlc -- extract and process HLC info during WAL redo.
 *
 * Called from the INSERT/UPDATE/DELETE redo handlers when the WAL record
 * has RECNO_WAL_HAS_HLC set.  Extracts the xl_recno_hlc_info from the
 * end of the record data, advances the local HLC, and handles
 * uncertainty for standby/replica.
 *
 * Returns a pointer to a static copy of the HLC info, or NULL if the
 * flag is not set or the record doesn't have enough data.
 */
static const xl_recno_hlc_info *
recno_redo_handle_hlc(XLogReaderState *record, uint16 flags)
{
	static xl_recno_hlc_info hlc_buf;
	Size		total_len;
	char	   *data;
	const xl_recno_hlc_info *hlc_info;

	if (!(flags & RECNO_WAL_HAS_HLC))
		return NULL;

	data = XLogRecGetData(record);
	total_len = XLogRecGetDataLen(record);

	if (total_len < SizeOfXlRecnoHlcInfo)
		return NULL;

	hlc_info = (const xl_recno_hlc_info *)
		(data + total_len - SizeOfXlRecnoHlcInfo);

	memcpy(&hlc_buf, hlc_info, SizeOfXlRecnoHlcInfo);

	/* Advance local HLC and handle uncertainty if this is a standby */
	if (hlc_buf.commit_hlc != InvalidHLCTimestamp)
	{
		int32		uncertainty_ms = 0;

		if (hlc_buf.uncertainty_upper != 0)
		{
			uint64		commit_phys = HLCGetPhysical(hlc_buf.commit_hlc);
			uint64		upper_phys = HLCGetPhysical(hlc_buf.uncertainty_upper);

			uncertainty_ms = (int32) (upper_phys - commit_phys);
		}

		RecnoReplicaHandleUncertainty(hlc_buf.commit_hlc, uncertainty_ms);
	}

	return &hlc_buf;
}

/* ----------------------------------------------------------------
 *					WAL Record Logging Functions
 * ----------------------------------------------------------------
 */

/*
 * Log a tuple insert operation.
 *
 * When recno_use_hlc is true, appends xl_recno_hlc_info with the commit
 * HLC and uncertainty interval.
 */
XLogRecPtr
RecnoXLogInsert(Relation rel, Buffer buffer, OffsetNumber offnum,
				RecnoTuple tuple, uint64 commit_ts,
				RecnoOverflowBuffers *overflow_buffers)
{
	xl_recno_insert xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_RECNO_INSERT;
	int			i;
	xl_recno_overflow_write ovf_xlrecs[MAX_OVERFLOW_BUFFERS];

	/* Fill in the insert record */
	xlrec.offnum = offnum;
	xlrec.flags = 0;
	xlrec.tuple_len = tuple->t_len;
	xlrec.commit_ts = commit_ts;

	/* Set HLC flag if running in HLC mode */
	if (recno_use_hlc)
		xlrec.flags |= RECNO_WAL_HAS_HLC;

	/*
	 * NOTE: XLogEnsureRecordSpace() has already been called by the caller
	 * (before entering the critical section) to pre-allocate space for the
	 * main buffer plus all overflow buffers.
	 */
	XLogBeginInsert();

	/* Register buffer FIRST, before any data */
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

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
			RecnoOverflowBuffer *ovb = &overflow_buffers->buffers[i];
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
				xlrec.flags |= RECNO_WAL_HAS_OVERFLOW_BLK0;
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
					 * RecnoXLogUpdate for the rationale.
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
			 * Create a proper xl_recno_overflow_write header with the offset.
			 * This tells the redo handler where to place the record. Each
			 * header is stored in a dedicated array slot so the pointer
			 * passed to XLogRegisterBufData remains valid until XLogInsert.
			 */
			ovf_xlrecs[i].offnum = ovb->offset;
			ovf_xlrecs[i].flags = ovb->flags;
			ovf_xlrecs[i].data_len = ovb->record_len;
			ovf_xlrecs[i].commit_ts = commit_ts;

			/* Register the header first, then the data */
			XLogRegisterBufData(block_id, (char *) &ovf_xlrecs[i], sizeof(xl_recno_overflow_write));
			XLogRegisterBufData(block_id, ovb->record_data, ovb->record_len);
		}
	}

	/* Now register the main data */
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_insert));
	XLogRegisterData((char *) tuple->t_data, tuple->t_len);

	/* Append HLC uncertainty info if enabled */
	if (recno_use_hlc)
	{
		xl_recno_hlc_info hlc_info;

		hlc_info.commit_hlc = commit_ts;	/* In HLC mode, commit_ts IS the
											 * HLC */
		HLCGetUncertaintyInterval((HLCTimestamp) commit_ts,
								  (HLCTimestamp *) &hlc_info.uncertainty_lower,
								  (HLCTimestamp *) &hlc_info.uncertainty_upper);
		XLogRegisterData((char *) &hlc_info, SizeOfXlRecnoHlcInfo);
	}

	(void) RecnoXLogMaybeAppendLogicalTuple(rel, tuple, &xlrec.flags);

	recptr = XLogInsert(RM_RECNO_ID, info);

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
 * Log a tuple update operation (in-place with before/after images)
 */
XLogRecPtr
RecnoXLogUpdate(Relation rel, Buffer buffer, OffsetNumber offnum,
				RecnoTuple old_tuple, RecnoTuple new_tuple,
				uint64 old_commit_ts, uint64 new_commit_ts,
				RecnoOverflowBuffers *overflow_buffers,
				Buffer new_buffer)
{
	xl_recno_update xlrec;
	XLogRecPtr	recptr;
	uint8		info = XLOG_RECNO_UPDATE_INPLACE;
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
		xlrec.flags |= RECNO_WAL_CROSS_PAGE;

	if (recno_use_hlc)
		xlrec.flags |= RECNO_WAL_HAS_HLC;

	/*
	 * NOTE: XLogEnsureRecordSpace() has already been called by the caller
	 * (before entering the critical section) to pre-allocate space for the
	 * main buffer plus all overflow buffers.
	 */
	XLogBeginInsert();

	/*
	 * Register the source buffer (block 0).
	 *
	 * For same-page out-of-place updates, force a full-page image.  Both the
	 * old tuple (marked RECNO_TUPLE_UPDATED) and the new tuple exist on this
	 * page at different offsets.  The redo handler cannot reconstruct this
	 * two-tuple state without an FPI because the new tuple's offset is not
	 * recorded in the WAL record.
	 *
	 * For cross-page updates, the redo handler marks the old tuple UPDATED
	 * directly (see RECNO_WAL_CROSS_PAGE handling in recno_redo), so an FPI
	 * is only needed when the new tuple is larger than the old (to avoid redo
	 * replay errors from differing free-space conditions).
	 */
	{
		uint8		buf_flags = REGBUF_STANDARD;

		if (!is_cross_page)
			buf_flags |= REGBUF_FORCE_IMAGE;
		else if (new_tuple->t_len > old_tuple->t_len)
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
				RecnoOverflowBuffer *ovb = &overflow_buffers->buffers[i];
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
					xlrec.flags |= RECNO_WAL_HAS_OVERFLOW_BLK0;
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
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_update));

	/*
	 * Log only new tuple for REDO.  Old tuple data is stored exclusively in
	 * the shared UNDO log (UNDO_RMID_RECNO record written via
	 * UndoBufferAddRecordParts) and is not needed during WAL replay:
	 *
	 * - Same-size/shrinking updates: redo overwrites the slot in place using
	 * only the new tuple data. - Growing updates: REGBUF_FORCE_IMAGE is set
	 * above, so redo restores the page from a full-page image and never
	 * enters BLK_NEEDS_REDO.
	 *
	 * Prefix/suffix compression: For same-size in-place updates, we compute
	 * the common prefix and suffix between old and new tuple data.  If the
	 * savings exceed sizeof(xl_recno_prefix_suffix) (4 bytes), we log only
	 * the changed bytes plus a small header.  The redo handler reconstructs
	 * the full new tuple from the existing page data + diff.
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
		xl_recno_prefix_suffix ps;
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
		if (ps.prefixlen + ps.suffixlen > (int) sizeof(xl_recno_prefix_suffix) &&
			difflen >= 0)
		{
			xlrec.flags |= RECNO_WAL_PREFIX_SUFFIX;
			XLogRegisterData((char *) &ps, sizeof(xl_recno_prefix_suffix));
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

	/* Append HLC uncertainty info if enabled */
	if (recno_use_hlc)
	{
		xl_recno_hlc_info hlc_info;

		hlc_info.commit_hlc = new_commit_ts;
		HLCGetUncertaintyInterval((HLCTimestamp) new_commit_ts,
								  (HLCTimestamp *) &hlc_info.uncertainty_lower,
								  (HLCTimestamp *) &hlc_info.uncertainty_upper);
		XLogRegisterData((char *) &hlc_info, SizeOfXlRecnoHlcInfo);
	}

	/*
	 * Append heap-format images of old + new tuples for logical decoding.
	 * Order: old first, then new.  Flag is set uniformly on both or neither.
	 */
	if (rel != NULL && RelationIsLogicallyLogged(rel))
	{
		uint16		tmpflag = 0;

		(void) RecnoXLogMaybeAppendLogicalTuple(rel, old_tuple, &tmpflag);
		(void) RecnoXLogMaybeAppendLogicalTuple(rel, new_tuple, &tmpflag);
		xlrec.flags |= tmpflag;
	}

	recptr = XLogInsert(RM_RECNO_ID, info);

	return recptr;
}

/*
 * Log a tuple delete operation
 */
XLogRecPtr
RecnoXLogDelete(Relation rel, Buffer buffer, OffsetNumber offnum,
				RecnoTuple tuple, uint64 commit_ts)
{
	xl_recno_delete xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_RECNO_DELETE;

	/* Fill in the delete record */
	xlrec.offnum = offnum;
	xlrec.flags = 0;
	xlrec.tuple_len = tuple->t_len;
	xlrec.commit_ts = commit_ts;

	if (recno_use_hlc)
		xlrec.flags |= RECNO_WAL_HAS_HLC;

	XLogBeginInsert();

	/* Register buffer FIRST, before any data */
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	/*
	 * Register delete header only -- old tuple data is stored exclusively in
	 * the UNDO fork.  The redo handler only needs the offset and commit_ts to
	 * set RECNO_TUPLE_DELETED on the existing tuple.
	 */
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_delete));

	/* Append HLC uncertainty info if enabled */
	if (recno_use_hlc)
	{
		xl_recno_hlc_info hlc_info;

		hlc_info.commit_hlc = commit_ts;
		HLCGetUncertaintyInterval((HLCTimestamp) commit_ts,
								  (HLCTimestamp *) &hlc_info.uncertainty_lower,
								  (HLCTimestamp *) &hlc_info.uncertainty_upper);
		XLogRegisterData((char *) &hlc_info, SizeOfXlRecnoHlcInfo);
	}

	/*
	 * Append heap-format image of the deleted tuple for logical decoding.
	 * DELETE's REDO path doesn't need the old tuple image on-page (it just
	 * flips a flag), so this region is strictly for the decode side.
	 */
	(void) RecnoXLogMaybeAppendLogicalTuple(rel, tuple, &xlrec.flags);

	recptr = XLogInsert(RM_RECNO_ID, info);

	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * Log page defragmentation
 */
XLogRecPtr
RecnoXLogDefrag(Relation rel, Buffer buffer, RecnoOffsetMapping *mappings,
				int nmappings, uint64 commit_ts)
{
	xl_recno_defrag xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_RECNO_DEFRAG;

	/* Fill in the defrag record */
	xlrec.ntuples = nmappings;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_defrag));
	XLogRegisterData((char *) mappings, sizeof(RecnoOffsetMapping) * nmappings);

	/*
	 * Force a full-page image.  The caller may have removed dead tuples
	 * (ItemIdSetUnused) before compaction, and those removals are not encoded
	 * in the DEFRAG WAL record.  Without an FPI the redo handler would call
	 * PageRepairFragmentation() on a page that still contains the dead
	 * tuples, producing a page inconsistent with the primary.
	 */
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD | REGBUF_FORCE_IMAGE);

	recptr = XLogInsert(RM_RECNO_ID, info);

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
 * flags:       RECNO_OVERFLOW_WAL_NEW_RECORD or RECNO_OVERFLOW_WAL_LINK_UPDATE
 * commit_ts:   commit timestamp
 */
XLogRecPtr
RecnoXLogOverflowWrite(Relation rel, Buffer buffer, OffsetNumber offnum,
					   char *record_data, uint32 record_len, uint16 flags,
					   uint64 commit_ts)
{
	xl_recno_overflow_write xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_RECNO_OVERFLOW_WRITE;

	/* Fill in the overflow write record */
	xlrec.offnum = offnum;
	xlrec.flags = flags;
	xlrec.data_len = record_len;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_overflow_write));
	XLogRegisterData(record_data, record_len);
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	recptr = XLogInsert(RM_RECNO_ID, info);

	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * Log attribute compression
 */
XLogRecPtr
RecnoXLogCompress(Relation rel, Buffer buffer, OffsetNumber offnum,
				  uint16 attr_num, RecnoCompressionType comp_type,
				  uint8 comp_level, char *comp_data,
				  uint32 orig_size, uint32 comp_size, uint64 commit_ts)
{
	xl_recno_compress xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_RECNO_COMPRESS;

	/* Fill in the compress record */
	xlrec.offnum = offnum;
	xlrec.attr_num = attr_num;
	xlrec.comp_type = comp_type;
	xlrec.comp_level = comp_level;
	xlrec.orig_size = orig_size;
	xlrec.comp_size = comp_size;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_compress));
	XLogRegisterData(comp_data, comp_size);
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	recptr = XLogInsert(RM_RECNO_ID, info);

	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * Log page initialization
 */
XLogRecPtr
RecnoXLogInitPage(Relation rel, Buffer buffer, uint32 flags, uint64 commit_ts)
{
	xl_recno_init_page xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_RECNO_INIT_PAGE;

	/* Fill in the init page record */
	xlrec.flags = flags;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();

	/* Register buffer FIRST, before any data */
	XLogRegisterBuffer(0, buffer, REGBUF_WILL_INIT | REGBUF_STANDARD);

	/* Now register the data */
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_init_page));

	recptr = XLogInsert(RM_RECNO_ID, info);

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
RecnoXLogCrossPageDefrag(Relation rel,
						 Buffer dst_buf, OffsetNumber dst_offnum,
						 Buffer src_buf, OffsetNumber src_offnum,
						 const void *tuple_data, uint32 tuple_len)
{
	xl_recno_cross_page_defrag xlrec;
	XLogRecPtr	recptr;

	xlrec.src_offnum = src_offnum;
	xlrec.dst_offnum = dst_offnum;
	xlrec.tuple_len = tuple_len;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_cross_page_defrag));
	XLogRegisterData((char *) tuple_data, tuple_len);
	XLogRegisterBuffer(0, dst_buf, REGBUF_STANDARD | REGBUF_FORCE_IMAGE);
	XLogRegisterBuffer(1, src_buf, REGBUF_STANDARD);

	recptr = XLogInsert(RM_RECNO_ID, XLOG_RECNO_CROSS_PAGE_DEFRAG);

	return recptr;
}

/*
 * RecnoXLogCasUpdate -- WAL record for same-size CAS in-place update.
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
RecnoXLogCasUpdate(Relation rel, Buffer buffer, OffsetNumber offnum,
				   uint16 data_offset, uint16 data_len,
				   const char *new_data, uint64 new_commit_ts)
{
	xl_recno_cas_update xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);

	xlrec.offnum = offnum;
	xlrec.flags = 0;
	xlrec.data_offset = data_offset;
	xlrec.data_len = data_len;
	xlrec.new_commit_ts = new_commit_ts;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_cas_update));
	XLogRegisterData(new_data, data_len);
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	recptr = XLogInsert(RM_RECNO_ID, XLOG_RECNO_CAS_UPDATE);
	PageSetLSN(page, recptr);

	return recptr;
}

/* ----------------------------------------------------------------
 *				HLC-Aware WAL Logging Functions
 *
 * These variants accept an explicit xl_recno_hlc_info, allowing
 * callers to pre-compute the HLC data (e.g., when the HLC is
 * obtained during transaction commit rather than at WAL-write time).
 * ----------------------------------------------------------------
 */

/*
 * RecnoFillHLCInfo -- populate an xl_recno_hlc_info from current state.
 *
 * Returns true if HLC mode is active and the struct was filled.
 * Returns false if recno_use_hlc is false (struct untouched).
 */
bool
RecnoFillHLCInfo(xl_recno_hlc_info *info)
{
	HLCTimestamp commit_hlc;

	if (!recno_use_hlc)
		return false;

	commit_hlc = HLCNow(InvalidHLCTimestamp);
	info->commit_hlc = (uint64) commit_hlc;
	HLCGetUncertaintyInterval(commit_hlc,
							  (HLCTimestamp *) &info->uncertainty_lower,
							  (HLCTimestamp *) &info->uncertainty_upper);
	return true;
}

/*
 * RecnoXLogInsertHLC -- insert WAL record with explicit HLC info.
 */
XLogRecPtr
RecnoXLogInsertHLC(Relation rel, Buffer buffer, OffsetNumber offnum,
				   RecnoTuple tuple, uint64 commit_ts,
				   const xl_recno_hlc_info *hlc_info)
{
	xl_recno_insert xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_RECNO_INSERT;

	xlrec.offnum = offnum;
	xlrec.flags = (hlc_info != NULL) ? RECNO_WAL_HAS_HLC : 0;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_insert));
	XLogRegisterData((char *) tuple->t_data, tuple->t_len);

	if (hlc_info != NULL)
		XLogRegisterData((char *) hlc_info, SizeOfXlRecnoHlcInfo);

	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	recptr = XLogInsert(RM_RECNO_ID, info);
	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * RecnoXLogUpdateHLC -- update WAL record with explicit HLC info.
 */
XLogRecPtr
RecnoXLogUpdateHLC(Relation rel, Buffer buffer, OffsetNumber offnum,
				   RecnoTuple old_tuple, RecnoTuple new_tuple,
				   uint64 old_commit_ts, uint64 new_commit_ts,
				   const xl_recno_hlc_info *hlc_info)
{
	xl_recno_update xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_RECNO_UPDATE_INPLACE;

	xlrec.offnum = offnum;
	xlrec.flags = (hlc_info != NULL) ? RECNO_WAL_HAS_HLC : 0;
	xlrec.old_commit_ts = old_commit_ts;
	xlrec.new_commit_ts = new_commit_ts;
	xlrec.old_tuple_len = (uint16) old_tuple->t_len;
	xlrec.new_tuple_len = (uint16) new_tuple->t_len;
	xlrec.dst_block_id = 0;
	memset(xlrec.pad, 0, sizeof(xlrec.pad));

	XLogBeginInsert();

	/*
	 * Force FPI for size-increasing updates (same rationale as
	 * RecnoXLogUpdate)
	 */
	{
		uint8		buf_flags = REGBUF_STANDARD;

		if (new_tuple->t_len > old_tuple->t_len)
			buf_flags |= REGBUF_FORCE_IMAGE;

		XLogRegisterBuffer(0, buffer, buf_flags);
	}

	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_update));

	/*
	 * Prefix/suffix compression for same-size in-place updates. Same logic as
	 * RecnoXLogUpdate but simpler since there's no cross-page.
	 */
	if (old_tuple->t_len == new_tuple->t_len && new_tuple->t_len > 0)
	{
		char	   *oldp = (char *) old_tuple->t_data;
		char	   *newp = (char *) new_tuple->t_data;
		int			len = new_tuple->t_len;
		xl_recno_prefix_suffix ps;
		int			difflen;

		for (ps.prefixlen = 0; ps.prefixlen < len; ps.prefixlen++)
			if (oldp[ps.prefixlen] != newp[ps.prefixlen])
				break;
		for (ps.suffixlen = 0;
			 ps.suffixlen < len - ps.prefixlen;
			 ps.suffixlen++)
			if (oldp[len - 1 - ps.suffixlen] != newp[len - 1 - ps.suffixlen])
				break;

		difflen = len - ps.prefixlen - ps.suffixlen;

		if (ps.prefixlen + ps.suffixlen > (int) sizeof(xl_recno_prefix_suffix) &&
			difflen >= 0)
		{
			xlrec.flags |= RECNO_WAL_PREFIX_SUFFIX;
			XLogRegisterData((char *) &ps, sizeof(xl_recno_prefix_suffix));
			if (difflen > 0)
				XLogRegisterData(newp + ps.prefixlen, difflen);
		}
		else
		{
			XLogRegisterData((char *) new_tuple->t_data, new_tuple->t_len);
		}
	}
	else
	{
		XLogRegisterData((char *) new_tuple->t_data, new_tuple->t_len);
	}

	if (hlc_info != NULL)
		XLogRegisterData((char *) hlc_info, SizeOfXlRecnoHlcInfo);

	recptr = XLogInsert(RM_RECNO_ID, info);
	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * RecnoXLogDeleteHLC -- delete WAL record with explicit HLC info.
 */
XLogRecPtr
RecnoXLogDeleteHLC(Relation rel, Buffer buffer, OffsetNumber offnum,
				   RecnoTuple tuple, uint64 commit_ts,
				   const xl_recno_hlc_info *hlc_info)
{
	xl_recno_delete xlrec;
	XLogRecPtr	recptr;
	Page		page = BufferGetPage(buffer);
	uint8		info = XLOG_RECNO_DELETE;

	xlrec.offnum = offnum;
	xlrec.flags = (hlc_info != NULL) ? RECNO_WAL_HAS_HLC : 0;
	xlrec.tuple_len = tuple->t_len;
	xlrec.commit_ts = commit_ts;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, sizeof(xl_recno_delete));
	/* Old tuple data is in UNDO fork exclusively */

	if (hlc_info != NULL)
		XLogRegisterData((char *) hlc_info, SizeOfXlRecnoHlcInfo);

	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	recptr = XLogInsert(RM_RECNO_ID, info);
	PageSetLSN(page, recptr);

	return recptr;
}

/*
 * REDO function for RECNO WAL records
 */
/* ----------------------------------------------------------------
 *		Per-opcode REDO handlers.
 *
 *	recno_redo() is the thin dispatcher; the real work for each
 *	XLOG_RECNO_* opcode lives in a dedicated static helper below.
 * ----------------------------------------------------------------
 */
static void recno_xlog_insert_redo(XLogReaderState *record);
static void recno_xlog_update_inplace_redo(XLogReaderState *record);
static void recno_xlog_delete_redo(XLogReaderState *record);
static void recno_xlog_defrag_redo(XLogReaderState *record);
static void recno_xlog_overflow_write_redo(XLogReaderState *record);
static void recno_xlog_compress_redo(XLogReaderState *record);
static void recno_xlog_init_page_redo(XLogReaderState *record);
static void recno_xlog_cross_page_defrag_redo(XLogReaderState *record);
static void recno_xlog_vm_set_redo(XLogReaderState *record);
static void recno_xlog_vm_clear_redo(XLogReaderState *record);
static void recno_xlog_lock_redo(XLogReaderState *record);

/*
 * recno_xlog_insert_redo
 *		REDO handler for XLOG_RECNO_INSERT.
 */
static void
recno_xlog_insert_redo(XLogReaderState *record)
{
	RelFileLocator rlocator;
	BlockNumber blkno;
	Buffer		buffer;
	Page		page;

	XLogRecGetBlockTag(record, 0, &rlocator, NULL, &blkno);

	{
		xl_recno_insert *xlrec = (xl_recno_insert *) XLogRecGetData(record);
		char	   *tuple_data = (char *) xlrec + sizeof(xl_recno_insert);
		RecnoTupleHeader *tuple_hdr = (RecnoTupleHeader *) tuple_data;
		XLogRedoAction action;
		OffsetNumber final_offnum = InvalidOffsetNumber;

		/* Process HLC uncertainty data on standby */
		recno_redo_handle_hlc(record, xlrec->flags);

		action = XLogReadBufferForRedo(record, 0, &buffer);

		/*
		 * For BLK_RESTORED (FPI), the page already has the tuple at
		 * xlrec->offnum
		 */
		if (action == BLK_RESTORED)
			final_offnum = xlrec->offnum;

		if (action == BLK_NEEDS_REDO)
		{
			RecnoPageOpaque phdr;
			OffsetNumber inserted_offnum;
			char	   *ovf_data;
			Size		ovf_len;

			page = BufferGetPage(buffer);

			/*
			 * XLogInitBufferForRedo does standard PageInit for new pages, but
			 * doesn't set up RECNO opaque space. Initialize it here if
			 * needed.
			 */
			if (PageIsNew(page))
			{
				RecnoInitPage(page, BufferGetPageSize(buffer));
			}

			/*
			 * CRITICAL: During normal operation, overflow records are
			 * inserted BEFORE the main tuple (via RecnoStoreOverflowColumn
			 * called from RecnoFormTuple, then RecnoPageAddTuple for main).
			 * This means overflow records get lower offsets (1, 2, 3...) and
			 * the main tuple gets a higher offset (4, ...).
			 *
			 * We MUST replay in the same order. If there are overflow records
			 * on block_id=0 (same page as main tuple due to spatial
			 * locality), replay them FIRST before the main tuple.
			 */
			ovf_data = XLogRecGetBlockData(record, 0, &ovf_len);
			if (ovf_data != NULL && ovf_len > 0 &&
				(xlrec->flags & RECNO_WAL_HAS_OVERFLOW_BLK0))
			{
				char	   *ovf_ptr = ovf_data;
				Size		ovf_remaining = ovf_len;

				/*
				 * Block 0 has overflow data. Parse and replay all overflow
				 * records on this block before the main tuple. Each overflow
				 * record has format: [xl_recno_overflow_write header][actual
				 * record data]
				 */
				while (ovf_remaining > sizeof(xl_recno_overflow_write))
				{
					xl_recno_overflow_write *ovf_xlrec = (xl_recno_overflow_write *) ovf_ptr;
					char	   *actual_data = ovf_ptr + sizeof(xl_recno_overflow_write);
					Size		actual_len = ovf_xlrec->data_len;
					OffsetNumber ovf_offnum;

					if (ovf_remaining < sizeof(xl_recno_overflow_write) + actual_len)
						elog(PANIC, "RECNO INSERT redo: corrupt overflow data on block 0: "
							 "ovf_remaining=%zu, sizeof(hdr)=%zu, data_len=%u, "
							 "total_len=%zu, offnum=%u, flags=%u",
							 ovf_remaining, sizeof(xl_recno_overflow_write),
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
						elog(WARNING, "RECNO INSERT redo: failed to add overflow "
							 "record on block %u; skipping redo", blkno);
						goto insert_skip_tuple;
					}

					/* Advance to next overflow record in the block data */
					ovf_ptr += sizeof(xl_recno_overflow_write) + actual_len;
					ovf_remaining -= sizeof(xl_recno_overflow_write) + actual_len;
				}
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
				elog(WARNING, "RECNO INSERT redo: failed to add tuple on "
					 "block %u (page may have been modified by a later "
					 "operation); skipping redo", blkno);
				goto insert_skip_tuple;
			}
			final_offnum = inserted_offnum;

			/*
			 * Fix the tuple's t_ctid to point to itself at the correct
			 * location. During normal operation, this is set in
			 * recno_tuple_insert after we know the final TID. During redo, we
			 * must fix it here.
			 *
			 * Defensive: validate ItemId is LP_NORMAL before dereferencing
			 * via PageGetItem.  After crash recovery involving the UNDO
			 * revert worker, the slot could be in an unexpected state.
			 */
			{
				ItemId		itemid = PageGetItemId(page, inserted_offnum);

				if (ItemIdIsNormal(itemid))
				{
					RecnoTupleHeader *inserted_hdr =
						(RecnoTupleHeader *) PageGetItem(page, itemid);

					/* blkno was already fetched at function entry */
					ItemPointerSet(&inserted_hdr->t_ctid, blkno, inserted_offnum);
				}
			}

			/*
			 * Update page header.  CRITICAL: Must replicate the exact logic
			 * from RecnoPageAddTuple() so the page matches the Full Page
			 * Write.  RecnoPageAddTuple sets the RECNO_PAGE_DEFRAG_NEEDED
			 * flag based on fragmentation heuristics, so we must do the same
			 * here.
			 */
			phdr = RecnoPageGetOpaque(page);
			RecnoPageSetCommitTs(phdr, Max(RecnoPageGetCommitTs(phdr), xlrec->commit_ts));

			/*
			 * Mark page for defragmentation if fragmented. This matches the
			 * logic in RecnoPageAddTuple() at recno_tuple.c:513-517.
			 */
			if (PageGetFreeSpace(page) >= xlrec->tuple_len * 2 &&
				PageGetMaxOffsetNumber(page) > FirstOffsetNumber + 5)
			{
				RecnoPageSetFlag(phdr, RECNO_PAGE_DEFRAG_NEEDED);
			}

	insert_skip_tuple:
			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);

		/*
		 * Register UNCOMMITTED tuples in the per-tuple sLog during WAL
		 * replay.  On a hot standby, the sLog is never populated by normal
		 * INSERT operations (only the primary's transaction machinery does
		 * that).  Without this, RecnoTupleVisibleHLC() sees slog_nfound==0
		 * and incorrectly assumes the inserter committed, making aborted
		 * tuples visible until the CLR arrives from the logical revert
		 * worker.
		 *
		 * This entry is cleaned up lazily: for committed transactions,
		 * SLogTupleEvictCommitted() reclaims the slot when the hash fills.
		 * For aborted transactions, the CLR sets DELETED, making the sLog
		 * entry irrelevant for visibility.
		 */
		if (final_offnum != InvalidOffsetNumber &&
			(tuple_hdr->t_flags & RECNO_TUPLE_UNCOMMITTED))
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

				/* Initialize as RECNO page if new */
				if (PageIsNew(ovf_page))
				{
					RecnoInitPage(ovf_page, BufferGetPageSize(ovf_buffer));
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
					while (ovf_remaining > sizeof(xl_recno_overflow_write))
					{
						xl_recno_overflow_write *ovf_xlrec = (xl_recno_overflow_write *) ovf_ptr;
						char	   *actual_data = ovf_ptr + sizeof(xl_recno_overflow_write);
						Size		actual_len = ovf_xlrec->data_len;
						OffsetNumber ovf_offnum;

						if (ovf_remaining < sizeof(xl_recno_overflow_write) + actual_len)
							elog(PANIC, "RECNO INSERT redo: corrupt overflow data on block %u",
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
							 * Overflow page may have been modified by a
							 * later operation that was checkpointed.  Skip
							 * remaining overflow records on this page.
							 */
							elog(WARNING, "RECNO INSERT redo: failed to add "
								 "overflow record on block %u; skipping",
								 BufferGetBlockNumber(ovf_buffer));
							break;
						}

						/* Advance to next overflow record */
						ovf_ptr += sizeof(xl_recno_overflow_write) + actual_len;
						ovf_remaining -= sizeof(xl_recno_overflow_write) + actual_len;
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
 * recno_xlog_update_inplace_redo
 *		REDO handler for XLOG_RECNO_UPDATE_INPLACE.
 */
static void
recno_xlog_update_inplace_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_recno_update *xlrec = (xl_recno_update *) XLogRecGetData(record);
		XLogRedoAction action;

		/*
		 * WAL record layout depends on RECNO_WAL_PREFIX_SUFFIX flag:
		 *
		 * Without prefix/suffix: [xl_recno_update][full new tuple data] With
		 * prefix/suffix:    [xl_recno_update][xl_recno_prefix_suffix][diff
		 * bytes]
		 *
		 * Old tuple data is in the UNDO fork exclusively.
		 */
		char	   *after_header = (char *) xlrec + sizeof(xl_recno_update);
		bool		use_prefix_suffix = (xlrec->flags & RECNO_WAL_PREFIX_SUFFIX) != 0;
		xl_recno_prefix_suffix ps_info = {0, 0};
		char	   *diff_data = NULL;
		char	   *new_tuple_data = NULL;
		RecnoTupleHeader *new_tuple_hdr = NULL;
		ItemId		itemid;
		RecnoPageOpaque phdr;

		if (use_prefix_suffix)
		{
			memcpy(&ps_info, after_header, sizeof(xl_recno_prefix_suffix));
			diff_data = after_header + sizeof(xl_recno_prefix_suffix);
		}
		else
		{
			new_tuple_data = after_header;
			new_tuple_hdr = (RecnoTupleHeader *) new_tuple_data;
		}

		/* Process HLC uncertainty data on standby */
		recno_redo_handle_hlc(record, xlrec->flags);

		action = XLogReadBufferForRedo(record, 0, &buffer);

		if (action == BLK_NEEDS_REDO)
		{
			char	   *blk0_ovf_data;
			Size		blk0_ovf_len;

			page = BufferGetPage(buffer);

			/*
			 * XLogInitBufferForRedo does standard PageInit for new pages, but
			 * doesn't set up RECNO opaque space. Initialize it here if
			 * needed.
			 */
			if (PageIsNew(page))
			{
				RecnoInitPage(page, BufferGetPageSize(buffer));
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
				(xlrec->flags & RECNO_WAL_HAS_OVERFLOW_BLK0))
			{
				char	   *ovf_ptr = blk0_ovf_data;
				Size		ovf_remaining = blk0_ovf_len;

				while (ovf_remaining > sizeof(xl_recno_overflow_write))
				{
					xl_recno_overflow_write *blk0_ovf_xlrec =
						(xl_recno_overflow_write *) ovf_ptr;
					char	   *actual_data = ovf_ptr + sizeof(xl_recno_overflow_write);
					Size		actual_len = blk0_ovf_xlrec->data_len;
					OffsetNumber ovf_offnum;

					if (ovf_remaining < sizeof(xl_recno_overflow_write) + actual_len)
						elog(PANIC, "RECNO UPDATE redo: corrupt overflow data on block 0");

					ovf_offnum = PageAddItem(page, actual_data, actual_len,
											 InvalidOffsetNumber, false, false);
					if (ovf_offnum == InvalidOffsetNumber)
						elog(PANIC, "RECNO UPDATE redo: failed to add overflow record on block 0");

					ovf_ptr += sizeof(xl_recno_overflow_write) + actual_len;
					ovf_remaining -= sizeof(xl_recno_overflow_write) + actual_len;
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
			 * RecnoXLogUpdate), so they get BLK_RESTORED and never reach this
			 * code path.
			 */
			itemid = PageGetItemId(page, xlrec->offnum);
			if (ItemIdIsNormal(itemid))
			{
				RecnoTupleHeader *existing_tuple =
					(RecnoTupleHeader *) PageGetItem(page, itemid);

				if (xlrec->flags & RECNO_WAL_CROSS_PAGE)
				{
					/*
					 * Cross-page out-of-place update: mark the old tuple as
					 * UPDATED.  The new version is on the destination page
					 * restored from its FPI.
					 */
					existing_tuple->t_flags |= RECNO_TUPLE_UPDATED;
					existing_tuple->t_flags &= ~RECNO_TUPLE_UNCOMMITTED;
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
						 * The existing tuple IS the old tuple (same size,
						 * same-size update only).  We overwrite the changed
						 * middle portion with the diff data from WAL.
						 */
						int			difflen = (int) existing_len -
							ps_info.prefixlen - ps_info.suffixlen;

						if (difflen < 0 ||
							ps_info.prefixlen + ps_info.suffixlen > existing_len)
							elog(PANIC, "RECNO UPDATE REDO: invalid prefix/suffix "
								 "(prefix=%u, suffix=%u, tuple_len=%zu)",
								 ps_info.prefixlen, ps_info.suffixlen,
								 existing_len);

						if (difflen > 0)
							memcpy((char *) existing_tuple + ps_info.prefixlen,
								   diff_data, difflen);
					}
					else if (xlrec->new_tuple_len <= existing_len)
					{
						/* Full new tuple: overwrite in place */
						memcpy(existing_tuple, new_tuple_hdr, xlrec->new_tuple_len);
						ItemIdSetNormal(itemid, ItemIdGetOffset(itemid),
										xlrec->new_tuple_len);
					}
					else
					{
						/*
						 * Should not happen: growing updates force FPI via
						 * REGBUF_FORCE_IMAGE, so BLK_NEEDS_REDO is never
						 * returned for them.
						 */
						elog(PANIC, "RECNO UPDATE REDO: new tuple (%u) larger "
							 "than existing slot (%zu) without FPI",
							 xlrec->new_tuple_len, existing_len);
					}
				}
			}
			else
			{
				elog(DEBUG1, "RECNO UPDATE REDO: ItemId at offnum=%u is not normal", xlrec->offnum);
			}

			/* Update page header */
			phdr = RecnoPageGetOpaque(page);
			RecnoPageSetCommitTs(phdr, Max(RecnoPageGetCommitTs(phdr), xlrec->new_commit_ts));

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

				/* Initialize as RECNO page if new */
				if (PageIsNew(ovf_page))
				{
					RecnoInitPage(ovf_page, BufferGetPageSize(ovf_buffer));
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
					while (ovf_remaining > sizeof(xl_recno_overflow_write))
					{
						xl_recno_overflow_write *ovf_xlrec2 =
							(xl_recno_overflow_write *) ovf_ptr;
						char	   *actual_data = ovf_ptr + sizeof(xl_recno_overflow_write);
						Size		actual_len = ovf_xlrec2->data_len;
						OffsetNumber ovf_offnum;

						if (ovf_remaining < sizeof(xl_recno_overflow_write) + actual_len)
							elog(PANIC, "RECNO UPDATE redo: corrupt overflow data on block %u",
								 BufferGetBlockNumber(ovf_buffer));

						/*
						 * Use InvalidOffsetNumber to append sequentially,
						 * matching the original insertion order within this
						 * page.
						 */
						ovf_offnum = PageAddItem(ovf_page, actual_data, actual_len,
												 InvalidOffsetNumber, false, false);
						if (ovf_offnum == InvalidOffsetNumber)
							elog(PANIC, "RECNO UPDATE redo: failed to add overflow record on block %u",
								 BufferGetBlockNumber(ovf_buffer));

						ovf_ptr += sizeof(xl_recno_overflow_write) + actual_len;
						ovf_remaining -= sizeof(xl_recno_overflow_write) + actual_len;
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
 * recno_xlog_delete_redo
 *		REDO handler for XLOG_RECNO_DELETE.
 */
static void
recno_xlog_delete_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_recno_delete *xlrec = (xl_recno_delete *) XLogRecGetData(record);
		XLogRedoAction action;
		ItemId		itemid;
		RecnoPageOpaque phdr;

		/*
		 * WAL record contains only the delete header (offset + commit_ts).
		 * Old tuple data is stored exclusively in the UNDO fork for
		 * transaction rollback and is not needed here.
		 */

		/* Process HLC uncertainty data on standby */
		recno_redo_handle_hlc(record, xlrec->flags);

		action = XLogReadBufferForRedo(record, 0, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			/*
			 * XLogInitBufferForRedo does standard PageInit for new pages, but
			 * doesn't set up RECNO opaque space. Initialize it here if
			 * needed.
			 */
			if (PageIsNew(page))
			{
				RecnoInitPage(page, BufferGetPageSize(buffer));
			}

			/* REDO: Mark tuple as deleted */
			itemid = PageGetItemId(page, xlrec->offnum);
			if (ItemIdIsNormal(itemid))
			{
				RecnoTupleHeader *tuple =
					(RecnoTupleHeader *) PageGetItem(page, itemid);

				tuple->t_flags |= RECNO_TUPLE_DELETED;
				tuple->t_commit_ts = xlrec->commit_ts;
			}

			/* Update page header */
			phdr = RecnoPageGetOpaque(page);
			RecnoPageSetCommitTs(phdr, Max(RecnoPageGetCommitTs(phdr), xlrec->commit_ts));
			RecnoPageSetFlag(phdr, RECNO_PAGE_DEFRAG_NEEDED);

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * recno_xlog_defrag_redo
 *		REDO handler for XLOG_RECNO_DEFRAG.
 */
static void
recno_xlog_defrag_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_recno_defrag *xlrec = (xl_recno_defrag *) XLogRecGetData(record);
		XLogRedoAction action;

		RecnoPageOpaque phdr;

		action = XLogReadBufferForRedo(record, 0, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			/*
			 * XLogInitBufferForRedo does standard PageInit for new pages, but
			 * doesn't set up RECNO opaque space. Initialize it here if
			 * needed.
			 */
			if (PageIsNew(page))
			{
				RecnoInitPage(page, BufferGetPageSize(buffer));
			}

			/* Defragment the page */
			PageRepairFragmentation(page);

			/* Update page header */
			phdr = RecnoPageGetOpaque(page);
			RecnoPageSetCommitTs(phdr, Max(RecnoPageGetCommitTs(phdr), xlrec->commit_ts));
			RecnoPageClearFlag(phdr, RECNO_PAGE_DEFRAG_NEEDED);

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * recno_xlog_overflow_write_redo
 *		REDO handler for XLOG_RECNO_OVERFLOW_WRITE.
 */
static void
recno_xlog_overflow_write_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_recno_overflow_write *xlrec =
			(xl_recno_overflow_write *) XLogRecGetData(record);
		char	   *record_data = (char *) xlrec + sizeof(xl_recno_overflow_write);
		XLogRedoAction action;

		action = XLogReadBufferForRedo(record, 0, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			/* Initialize as normal RECNO page if needed */
			if (PageIsNew(page))
			{
				RecnoInitPage(page, BufferGetPageSize(buffer));
			}

			if (xlrec->flags & RECNO_OVERFLOW_WAL_LINK_UPDATE)
			{
				/*
				 * Link update: overwrite the existing overflow record header
				 * at the specified offset with updated chain pointers.
				 */
				ItemId		itemid;

				itemid = PageGetItemId(page, xlrec->offnum);
				if (ItemIdIsNormal(itemid))
				{
					RecnoOverflowRecordHeader *existing_hdr =
						(RecnoOverflowRecordHeader *) PageGetItem(page, itemid);

					memcpy(existing_hdr, record_data,
						   sizeof(RecnoOverflowRecordHeader));
				}
			}
			else
			{
				/*
				 * New overflow record: the logged data is the complete record
				 * (RecnoOverflowRecordHeader + chunk data). Add it to the
				 * page at the specified offset.
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
 * recno_xlog_compress_redo
 *		REDO handler for XLOG_RECNO_COMPRESS.
 */
static void
recno_xlog_compress_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_recno_compress *xlrec = (xl_recno_compress *) XLogRecGetData(record);
		XLogRedoAction action;

		ItemId		itemid;
		RecnoPageOpaque phdr;

		action = XLogReadBufferForRedo(record, 0, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			page = BufferGetPage(buffer);

			/*
			 * XLogInitBufferForRedo does standard PageInit for new pages, but
			 * doesn't set up RECNO opaque space. Initialize it here if
			 * needed.
			 */
			if (PageIsNew(page))
			{
				RecnoInitPage(page, BufferGetPageSize(buffer));
			}

			/* Apply compression to the tuple attribute */
			itemid = PageGetItemId(page, xlrec->offnum);
			if (ItemIdIsNormal(itemid))
			{
				RecnoTupleHeader *tuple =
					(RecnoTupleHeader *) PageGetItem(page, itemid);

				/* Mark tuple as compressed */
				tuple->t_flags |= RECNO_TUPLE_COMPRESSED;
				tuple->t_infomask |= RECNO_INFOMASK_COMPRESSED;
				tuple->t_commit_ts = xlrec->commit_ts;
			}

			/* Update page header */
			phdr = RecnoPageGetOpaque(page);
			RecnoPageSetCommitTs(phdr, Max(RecnoPageGetCommitTs(phdr), xlrec->commit_ts));

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * recno_xlog_init_page_redo
 *		REDO handler for XLOG_RECNO_INIT_PAGE.
 */
static void
recno_xlog_init_page_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_recno_init_page *xlrec = (xl_recno_init_page *) XLogRecGetData(record);
		XLogRedoAction action;

		action = XLogReadBufferForRedoExtended(record, 0, RBM_ZERO_AND_LOCK, false, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			RecnoPageOpaque phdr;

			page = BufferGetPage(buffer);

			/* Initialize page with RECNO opaque space */
			RecnoInitPage(page, BufferGetPageSize(buffer));

			/* Override commit_ts and flags from WAL record */
			phdr = RecnoPageGetOpaque(page);
			phdr->pd_commit_ts_and_flags = ((uint64) (xlrec->commit_ts) & RECNO_PAGE_TS_MASK) | (uint64) (xlrec->flags);

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * recno_xlog_cross_page_defrag_redo
 *		REDO handler for XLOG_RECNO_CROSS_PAGE_DEFRAG.
 */
static void
recno_xlog_cross_page_defrag_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_recno_cross_page_defrag *xlrec =
			(xl_recno_cross_page_defrag *) XLogRecGetData(record);
		char	   *tuple_data = (char *) xlrec +
			sizeof(xl_recno_cross_page_defrag);
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
				elog(DEBUG1, "recno cross-page defrag: insufficient space on target page during redo");
				if (BufferIsValid(buffer))
					UnlockReleaseBuffer(buffer);
				goto process_source;
			}

			/* Update ctid in the new copy to point to itself */
			{
				ItemId		dst_itemid;
				RecnoTupleHeader *dst_hdr;
				BlockNumber dst_blkno;

				XLogRecGetBlockTag(record, 0, NULL, NULL, &dst_blkno);
				dst_itemid = PageGetItemId(page, xlrec->dst_offnum);
				dst_hdr = (RecnoTupleHeader *) PageGetItem(page, dst_itemid);
				ItemPointerSet(&dst_hdr->t_ctid, dst_blkno,
							   xlrec->dst_offnum);
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
 * recno_xlog_vm_set_redo
 *		REDO handler for XLOG_RECNO_VM_SET.
 */
static void
recno_xlog_vm_set_redo(XLogReaderState *record)
{
	{
		xl_recno_vm_set *xlrec = (xl_recno_vm_set *) XLogRecGetData(record);
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
 * recno_xlog_vm_clear_redo
 *		REDO handler for XLOG_RECNO_VM_CLEAR.
 */
static void
recno_xlog_vm_clear_redo(XLogReaderState *record)
{
	{
		xl_recno_vm_clear *xlrec = (xl_recno_vm_clear *) XLogRecGetData(record);
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
 * recno_xlog_lock_redo
 *		REDO handler for XLOG_RECNO_LOCK.
 */
static void
recno_xlog_lock_redo(XLogReaderState *record)
{
	Buffer		buffer;
	Page		page;

	{
		xl_recno_lock *xlrec = (xl_recno_lock *) XLogRecGetData(record);
		XLogRedoAction action;

		action = XLogReadBufferForRedo(record, 0, &buffer);
		if (action == BLK_NEEDS_REDO)
		{
			ItemId		itemid;

			page = BufferGetPage(buffer);

			if (PageIsNew(page))
			{
				RecnoInitPage(page, BufferGetPageSize(buffer));
			}

			itemid = PageGetItemId(page, xlrec->offnum);
			if (ItemIdIsNormal(itemid))
			{
				RecnoTupleHeader *tuple =
					(RecnoTupleHeader *) PageGetItem(page, itemid);

				/* Apply the lock state from the WAL record */
				tuple->t_infomask = xlrec->infomask;
				tuple->t_flags |= RECNO_TUPLE_LOCKED;
			}

			PageSetLSN(page, record->EndRecPtr);
			MarkBufferDirty(buffer);
		}
		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

/*
 * recno_xlog_cas_update_redo
 *		REDO handler for XLOG_RECNO_CAS_UPDATE.
 *
 * Patches a contiguous byte range within a tuple on the page.  The record
 * carries only the changed bytes (data_offset..data_offset+data_len) and
 * the new commit timestamp.  Idempotent memcpy; safe for replay.
 */
static void
recno_xlog_cas_update_redo(XLogReaderState *record)
{
	xl_recno_cas_update *xlrec = (xl_recno_cas_update *) XLogRecGetData(record);
	char	   *new_data = ((char *) xlrec) + sizeof(xl_recno_cas_update);
	Buffer		buffer;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(buffer);
		ItemId		itemid;
		RecnoTupleHeader *tuple;

		itemid = PageGetItemId(page, xlrec->offnum);
		if (!ItemIdIsNormal(itemid))
			elog(PANIC, "RECNO CAS_UPDATE redo: invalid item at offset %u",
				 xlrec->offnum);

		tuple = (RecnoTupleHeader *) PageGetItem(page, itemid);

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
 * recno_redo
 *		Thin dispatcher for all XLOG_RECNO_* opcodes.  Each case is
 *	delegated to a dedicated per-opcode static helper above.
 */
void
recno_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;


	switch (info)
	{
		case XLOG_RECNO_INSERT:
			recno_xlog_insert_redo(record);
			break;

		case XLOG_RECNO_UPDATE_INPLACE:
			recno_xlog_update_inplace_redo(record);
			break;

		case XLOG_RECNO_DELETE:
			recno_xlog_delete_redo(record);
			break;

		case XLOG_RECNO_DEFRAG:
			recno_xlog_defrag_redo(record);
			break;

		case XLOG_RECNO_OVERFLOW_WRITE:
			recno_xlog_overflow_write_redo(record);
			break;

		case XLOG_RECNO_COMPRESS:
			recno_xlog_compress_redo(record);
			break;

		case XLOG_RECNO_INIT_PAGE:
			recno_xlog_init_page_redo(record);
			break;

		case XLOG_RECNO_CROSS_PAGE_DEFRAG:
			recno_xlog_cross_page_defrag_redo(record);
			break;

		case XLOG_RECNO_VM_SET:
			recno_xlog_vm_set_redo(record);
			break;

		case XLOG_RECNO_VM_CLEAR:
			recno_xlog_vm_clear_redo(record);
			break;

		case XLOG_RECNO_LOCK:
			recno_xlog_lock_redo(record);
			break;

		case XLOG_RECNO_CAS_UPDATE:
			recno_xlog_cas_update_redo(record);
			break;

		default:
			elog(PANIC, "recno_redo: unknown op code %u", info);
	}
}


/*
 * Mask function for RECNO pages (for consistency checking)
 */
void
recno_mask(char *page, BlockNumber blkno)
{
	Page		recno_page = (Page) page;
	RecnoPageOpaque phdr;
	bool		is_overflow;
	OffsetNumber offnum;
	OffsetNumber maxoff;

	mask_page_lsn_and_checksum(recno_page);

	mask_page_hint_bits(recno_page);
	mask_unused_space(recno_page);

	phdr = RecnoPageGetOpaque(recno_page);

	/* Check page type before masking flags */
	is_overflow = (phdr->pd_commit_ts_and_flags & RECNO_PAGE_OVERFLOW) != 0;

	/*
	 * Mask the entire packed commit_ts_and_flags field.
	 *
	 * The timestamp uses Max(existing, new) during redo which can produce a
	 * different value if the page was concurrently modified.  Heuristic flags
	 * (e.g., RECNO_PAGE_DEFRAG_NEEDED) may be set by redo but not by the
	 * original operation, or vice versa.
	 */
	phdr->pd_commit_ts_and_flags = 0;

	/*
	 * Overflow pages contain RecnoOverflowRecordHeader items, not regular
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
	maxoff = PageGetMaxOffsetNumber(recno_page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum++)
	{
		ItemId		itemid = PageGetItemId(recno_page, offnum);
		RecnoTupleHeader *tuple_hdr;

		if (!ItemIdIsNormal(itemid))
			continue;

		tuple_hdr = (RecnoTupleHeader *) PageGetItem(recno_page, itemid);
		tuple_hdr->t_infomask = 0;
		tuple_hdr->t_flags = 0;
		tuple_hdr->t_commit_ts = 0;
		tuple_hdr->t_writer = 0;	/* transient CAS lock, not replayed */
		ItemPointerSetInvalid(&tuple_hdr->t_ctid);
	}
}
