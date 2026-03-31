/*
 * noxu_undostubs.c
 *		Stub implementations for deprecated bespoke UNDO functions
 *
 * These functions provide compatibility wrappers around the RelUndo API
 * for code that still references the old bespoke UNDO system. They should
 * be gradually eliminated as code is migrated to use RelUndo directly.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_undostubs.c
 */
#include "postgres.h"

#include "access/noxu_internal.h"
#include "access/relundo.h"
#include "access/relundo_xlog.h"
#include "access/undolog.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/procarray.h"
#include "utils/rel.h"

/*
 * nx_get_oldest_visible_undo_ptr
 *		Get the oldest UNDO record pointer still needed for visibility checks.
 *
 * Returns the oldest UNDO record that might still be visible to any snapshot.
 * Uses a simple heuristic: keep records from the last 100 generations, which
 * mirrors the logic in RelUndoVacuum(). Returns DeadRelUndoRecPtr if the
 * UNDO fork doesn't exist yet.
 */
RelUndoRecPtr
nx_get_oldest_visible_undo_ptr(Relation rel)
{
	uint16		current_counter;
	uint16		oldest_visible_counter;
	RelUndoRecPtr result;

	/* Return DeadRelUndoRecPtr if UNDO fork doesn't exist yet */
	if (!smgrexists(RelationGetSmgr(rel), RELUNDO_FORKNUM))
		return DeadRelUndoRecPtr;

	/*
	 * Use the same heuristic as RelUndoVacuum(): keep records from the last
	 * 100 generations. Get the current generation counter and subtract 100.
	 */
	current_counter = RelUndoGetCurrentCounter(rel);

	if (current_counter > 100)
		oldest_visible_counter = current_counter - 100;
	else
		oldest_visible_counter = 1;

	/*
	 * Build a RelUndoRecPtr with the oldest visible counter. Block and offset
	 * are set to 0 since we only use the counter for age comparisons.
	 */
	result = MakeRelUndoRecPtr(oldest_visible_counter, 0, 0);

	return result;
}

/*
 * nxundo_clear_speculative_token - Clear a speculative insertion token
 *
 * Clears the speculative_token field in a RelUndoInsertPayload stored in the
 * relation's per-relation UNDO fork.  Called when a speculative insertion is
 * confirmed (INSERT ... ON CONFLICT succeeds) so that concurrent
 * SnapshotDirty scans no longer see the tuple as speculative.
 *
 * The UNDO record is modified in place on the buffer page, marked dirty, and
 * WAL-logged so the change survives a crash.  This mirrors the approach used
 * by heap_finish_speculative() in the heap AM.
 */
void
nxundo_clear_speculative_token(Relation rel, RelUndoRecPtr undoptr)
{
	BlockNumber blkno;
	uint16		offset;
	Buffer		buf;
	Page		page;
	char	   *contents;
	RelUndoPageHeader datahdr;
	RelUndoRecordHeader *header;
	xl_relundo_insert xlrec;
	Size		total_record_size;

	if (!RelUndoRecPtrIsValid(undoptr))
		return;

	blkno = RelUndoGetBlockNum(undoptr);
	offset = RelUndoGetOffset(undoptr);

	buf = ReadBufferExtended(rel, RELUNDO_FORKNUM, blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	page = BufferGetPage(buf);
	contents = PageGetContents(page);
	datahdr = (RelUndoPageHeader) contents;

	/* Validate that offset falls within the written portion of the page */
	if (offset < SizeOfRelUndoPageHeaderData || offset >= datahdr->pd_lower)
	{
		UnlockReleaseBuffer(buf);
		elog(WARNING, "nxundo_clear_speculative_token: offset %u out of range [%u, %u)",
			 offset, (unsigned) SizeOfRelUndoPageHeaderData, datahdr->pd_lower);
		return;
	}

	/* Verify counter matches to guard against stale pointers */
	if (datahdr->counter != RelUndoGetCounter(undoptr))
	{
		UnlockReleaseBuffer(buf);
		return;
	}

	header = (RelUndoRecordHeader *) (contents + offset);

	/* Only INSERT and DELTA_INSERT records carry speculative tokens */
	if (!RELUNDO_TYPE_IS_INSERT(header->urec_type))
	{
		UnlockReleaseBuffer(buf);
		return;
	}

	/*
	 * Clear the speculative token in the payload.  For RELUNDO_INSERT the
	 * payload is a RelUndoInsertPayload; for RELUNDO_DELTA_INSERT it is an
	 * NXRelUndoDeltaInsertPayload.  Both have speculative_token at the same
	 * offset (inherited from the common layout).
	 */
	if (header->urec_type == RELUNDO_INSERT)
	{
		RelUndoInsertPayload *payload;

		payload = (RelUndoInsertPayload *) (contents + offset + SizeOfRelUndoRecordHeader);
		if (payload->speculative_token == INVALID_SPECULATIVE_TOKEN)
		{
			/* Already cleared -- nothing to do */
			UnlockReleaseBuffer(buf);
			return;
		}

		/* --- NO EREPORT(ERROR) from here until WAL is logged --- */
		START_CRIT_SECTION();

		payload->speculative_token = INVALID_SPECULATIVE_TOKEN;
		MarkBufferDirty(buf);

		/* WAL-log the modified record */
		total_record_size = header->urec_len;

		xlrec.urec_type = header->urec_type;
		xlrec.urec_len = header->urec_len;
		xlrec.page_offset = MAXALIGN(SizeOfPageHeaderData) + offset;
		xlrec.new_pd_lower = datahdr->pd_lower;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfRelundoInsert);
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterBufData(0, (char *) header, total_record_size);
		XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_INSERT);

		END_CRIT_SECTION();
	}
	else
	{
		/* RELUNDO_DELTA_INSERT */
		NXRelUndoDeltaInsertPayload *payload;

		payload = (NXRelUndoDeltaInsertPayload *) (contents + offset + SizeOfRelUndoRecordHeader);
		if (payload->speculative_token == INVALID_SPECULATIVE_TOKEN)
		{
			UnlockReleaseBuffer(buf);
			return;
		}

		START_CRIT_SECTION();

		payload->speculative_token = INVALID_SPECULATIVE_TOKEN;
		MarkBufferDirty(buf);

		total_record_size = header->urec_len;

		xlrec.urec_type = header->urec_type;
		xlrec.urec_len = header->urec_len;
		xlrec.page_offset = MAXALIGN(SizeOfPageHeaderData) + offset;
		xlrec.new_pd_lower = datahdr->pd_lower;

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfRelundoInsert);
		XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
		XLogRegisterBufData(0, (char *) header, total_record_size);
		XLogInsert(RM_RELUNDO_ID, XLOG_RELUNDO_INSERT);

		END_CRIT_SECTION();
	}

	UnlockReleaseBuffer(buf);
}

/*
 * nxundo_vacuum - VACUUM the per-relation UNDO log
 *
 * Discards old UNDO records that are no longer needed for visibility checks.
 * Determines the oldest XID still visible to any snapshot, calls
 * RelUndoVacuum() to discard expired records and reclaim space, then
 * updates the NX metapage's deprecated UNDO tracking fields so that
 * inspection tools (e.g. noxu_inspect) report consistent state.
 */
void
nxundo_vacuum(Relation rel, struct VacuumParams *params, BufferAccessStrategy bstrategy)
{
	TransactionId oldest_xmin;
	RelUndoRecPtr oldest_visible;

	/* If no UNDO fork exists, nothing to vacuum */
	if (!smgrexists(RelationGetSmgr(rel), RELUNDO_FORKNUM))
		return;

	/*
	 * Determine the oldest XID still visible to any running transaction.
	 * Any UNDO records from transactions older than this can be discarded.
	 */
	oldest_xmin = GetOldestNonRemovableTransactionId(rel);

	/* Discard old UNDO records through the RelUndo API */
	RelUndoVacuum(rel, oldest_xmin);

	/*
	 * Update the NX metapage's deprecated nx_undo_oldestptr field so that
	 * inspection tools report a consistent oldest-visible pointer.
	 *
	 * This field is deprecated and only used by noxu_inspect; it is
	 * recalculated on every VACUUM, so we intentionally skip WAL logging.
	 * After a crash the value reverts to whatever was last flushed, and the
	 * next VACUUM will correct it.
	 */
	oldest_visible = nx_get_oldest_visible_undo_ptr(rel);
	if (RelUndoRecPtrIsValid(oldest_visible))
	{
		Buffer		metabuf;
		Page		metapage;
		NXMetaPageOpaque *opaque;

		metabuf = ReadBufferExtended(rel, MAIN_FORKNUM, NX_META_BLK,
									 RBM_NORMAL, bstrategy);
		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

		metapage = BufferGetPage(metabuf);
		opaque = (NXMetaPageOpaque *) PageGetSpecialPointer(metapage);

		if (opaque->nx_undo_oldestptr != oldest_visible)
		{
			opaque->nx_undo_oldestptr = oldest_visible;
			MarkBufferDirty(metabuf);
		}

		UnlockReleaseBuffer(metabuf);
	}
}
