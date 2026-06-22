/*-------------------------------------------------------------------------
 *
 * recno_undo.c
 *	  RECNO UNDO resource manager
 *
 * RECNO writes one UNDO record per tuple INSERT, UPDATE and DELETE via
 * the shared UNDO-in-WAL infrastructure.  Records carry rmid
 * UNDO_RMID_RECNO and an info subtype (RECNO_UNDO_INSERT / UPDATE /
 * DELETE / DELTA_UPDATE); rollback is driven by undoapply.c which
 * dispatches to recno_undo_apply() based on rmid.
 *
 * Visibility of aborted rows is handled independently of physical
 * undo application: RECNO tuples carry a RECNO_TUPLE_UNCOMMITTED flag
 * whose MVCC-visibility path consults the sLog, so an aborted
 * transaction's tuples are invisible the moment the sLog entry
 * transitions to ABORTED (see recno_slog.c's XACT_EVENT_ABORT handler).
 * The physical page-mutation done here reclaims on-disk space so
 * VACUUM does not have to touch every aborted row.
 *
 * Crash safety is provided by emitting an xl_undo_apply CLR record
 * (XLOG_UNDO_APPLY_RECORD / RM_UNDO_ID) for every page modification.
 * The CLR carries the new tuple image (or the LP-state change) and is
 * replayed idempotently by the generic undo_xlog.c redo handler;
 * RECNO does not need its own redo routine for the undo-apply path.
 *
 * The callback mirrors heapam_undo.c's control flow:
 *
 *   1. Defer while in crash recovery or inside a transaction's abort
 *      path (BumpContext makes relation_close/pfree unsafe); the
 *      logical-revert worker re-drives the record from a clean top-
 *      level memory context.
 *   2. try_relation_open() the target; if the relation was dropped
 *      or truncated past the target block, return UNDO_APPLY_SKIPPED.
 *   3. Dispatch on info to a page-modification branch, emit a CLR,
 *      mark the buffer dirty, release locks, close.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/recno.h"
#include "access/recno_diff.h"
#include "access/recno_undo.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/undo_xlog.h"
#include "access/undormgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/relcache.h"


static UndoApplyResult recno_undo_apply(uint8 rmid, uint16 info,
										TransactionId xid, Oid reloid,
										const char *payload, Size payload_len,
										UndoRecPtr urec_ptr);
static void recno_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
							const char *payload, Size payload_len);


/* The RECNO UNDO RM registration entry */
static const UndoRmgrData recno_undo_rmgr = {
	.rm_name = "recno",
	.rm_undo = recno_undo_apply,
	.rm_desc = recno_undo_desc,
};


/*
 * RecnoUndoRmgrInit
 *		Register the RECNO UNDO resource manager.
 *
 * Called from InitializeUndoSubsystem() at postmaster startup, alongside
 * HeapUndoRmgrInit() and NbtreeUndoRmgrInit().
 */
void
RecnoUndoRmgrInit(void)
{
	RegisterUndoRmgr(UNDO_RMID_RECNO, &recno_undo_rmgr);

	/*
	 * Install the RECNO implementations of the AM-neutral per-relation UNDO
	 * hooks now, before crash recovery can replay a RELUNDO CLR that would
	 * dispatch through them.
	 */
	RecnoRelUndoInstallHooks();
}


/*
 * emit_recno_undo_clr
 *		Emit an XLOG_UNDO_APPLY_RECORD CLR for the page modification
 *		just performed.  Must be called inside the critical section,
 *		before END_CRIT_SECTION / UnlockReleaseBuffer.
 *
 * tuple_data is the image to replay into the target slot on redo
 * (NULL for LP_UNUSED cases).  tuple_len must match the on-page slot
 * length that should be installed.
 */
static void
emit_recno_undo_clr(Relation rel, Buffer buffer, UndoRecPtr urec_ptr,
					TransactionId xid, BlockNumber blkno, OffsetNumber offnum,
					uint16 info, uint16 clr_flags,
					const char *tuple_data, uint32 tuple_len)
{
	xl_undo_apply xlrec;
	XLogRecPtr	lsn;

	if (!RelationNeedsWAL(rel))
	{
		/*
		 * Unlogged / temp relations need no CLR: they do not survive a crash,
		 * so replay idempotency is irrelevant.
		 */
		PageSetLSN(BufferGetPage(buffer), GetXLogInsertRecPtr());
		return;
	}

	xlrec.urec_ptr = urec_ptr;
	xlrec.xid = xid;
	xlrec.target_locator = rel->rd_locator;
	xlrec.target_block = blkno;
	xlrec.target_offset = offnum;
	xlrec.operation_type = info;
	xlrec.clr_flags = clr_flags;
	xlrec.tuple_len = tuple_len;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfUndoApply);
	XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

	if ((clr_flags & UNDO_CLR_HAS_TUPLE) && tuple_data != NULL && tuple_len > 0)
		XLogRegisterBufData(0, tuple_data, tuple_len);

	lsn = XLogInsert(RM_UNDO_ID, XLOG_UNDO_APPLY_RECORD);
	PageSetLSN(BufferGetPage(buffer), lsn);
}


/*
 * apply_recno_undo_insert
 *		Undo an INSERT: mark the inserted tuple RECNO_TUPLE_DELETED so
 *		VACUUM can reclaim its space.  The sLog-driven visibility path
 *		already hides the row from readers once the transaction is
 *		marked ABORTED; this routine exists purely for physical
 *		space-reclaim.
 *
 * We do not use UNDO_CLR_LP_DEAD / UNDO_CLR_LP_UNUSED because those
 * drop the item entirely, whereas RECNO needs the tuple header to
 * stay intact (the page's commit_ts, overflow pointers, and the
 * DELETED bit itself are all read by VACUUM).
 */
static void
apply_recno_undo_insert(Relation rel, Buffer buffer, OffsetNumber offnum,
						BlockNumber blkno, UndoRecPtr urec_ptr,
						TransactionId xid)
{
	Page		page = BufferGetPage(buffer);
	ItemId		lp;
	RecnoTupleHeader hdr;
	Size		len;
	char	   *slot;

	lp = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(lp))
	{
		/*
		 * Already cleaned up (e.g. VACUUM ran between the abort and the
		 * logical-revert worker's pass).  Nothing to do.
		 */
		return;
	}

	len = ItemIdGetLength(lp);
	slot = (char *) PageGetItem(page, lp);

	START_CRIT_SECTION();

	/*
	 * Read, mutate, write back the tuple header in place.
	 *
	 * Set RECNO_TUPLE_DELETED so VACUUM can reclaim the space.  Crucially, we
	 * must NOT clear RECNO_TUPLE_UNCOMMITTED: the inserting transaction never
	 * committed, so the tuple has to remain on the UNCOMMITTED visibility path
	 * (recno_mvcc.c), where a SLOG_OP_ABORTED entry resolves the row to
	 * not-visible.  Clearing the flag would route the tuple to the
	 * post-commit deletion path, whose ABORTED handling resurrects aborted
	 * DELETEs and would therefore make this never-committed row visible.
	 */
	memcpy(&hdr, slot, sizeof(hdr));
	hdr.t_flags |= RECNO_TUPLE_DELETED;
	memcpy(slot, &hdr, sizeof(hdr));

	MarkBufferDirty(buffer);

	emit_recno_undo_clr(rel, buffer, urec_ptr, xid, blkno, offnum,
						RECNO_UNDO_INSERT, UNDO_CLR_HAS_TUPLE,
						slot, (uint32) len);

	END_CRIT_SECTION();
}


/*
 * apply_recno_undo_restore_tuple
 *		Shared helper for UPDATE, DELETE and DELTA_UPDATE undo: overwrite
 *		the current on-disk tuple with an in-memory before-image.  The
 *		caller is responsible for preparing the before-image (direct
 *		copy for DELETE/UPDATE, reverse-diff reconstruction for
 *		DELTA_UPDATE).
 *
 * For DELETE undo the before-image already carries the pre-delete
 * header, so the RECNO_TUPLE_DELETED bit will be cleared as a
 * side-effect of the overwrite.
 *
 * If the before-image is larger than the current on-page slot, the
 * undo is skipped (the slot was shrunk by a later in-place update and
 * cannot be safely grown from here).  The row will remain visible
 * per sLog until VACUUM reclaims it.
 */
static bool
apply_recno_undo_restore_tuple(Relation rel, Buffer buffer, OffsetNumber offnum,
							   BlockNumber blkno, UndoRecPtr urec_ptr,
							   TransactionId xid, uint16 info,
							   const char *old_image, uint32 old_len)
{
	Page		page = BufferGetPage(buffer);
	ItemId		lp;
	char	   *slot;

	Assert(old_image != NULL && old_len > 0);

	lp = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(lp))
	{
		ereport(DEBUG2,
				(errmsg_internal("RECNO UNDO: item (%u, %u) no longer normal, skipping",
								 blkno, offnum)));
		return false;
	}

	if (ItemIdGetLength(lp) < old_len)
	{
		ereport(WARNING,
				(errmsg_internal("RECNO UNDO: current slot at (%u, %u) is smaller "
								 "than before-image (%u < %u); rollback skipped, "
								 "row left under MVCC retention until VACUUM",
								 blkno, offnum,
								 (unsigned) ItemIdGetLength(lp),
								 (unsigned) old_len)));
		return false;
	}

	slot = (char *) PageGetItem(page, lp);

	START_CRIT_SECTION();

	memcpy(slot, old_image, old_len);
	if (ItemIdGetLength(lp) != old_len)
		ItemIdSetNormal(lp, ItemIdGetOffset(lp), old_len);

	MarkBufferDirty(buffer);

	emit_recno_undo_clr(rel, buffer, urec_ptr, xid, blkno, offnum,
						info, UNDO_CLR_HAS_TUPLE,
						old_image, old_len);

	END_CRIT_SECTION();
	return true;
}


/*
 * recno_undo_apply
 *		Apply a single RECNO UNDO record.
 *
 * Dispatched from undoapply.c for records tagged UNDO_RMID_RECNO.
 */
static UndoApplyResult
recno_undo_apply(uint8 rmid, uint16 info, TransactionId xid, Oid reloid,
				 const char *payload, Size payload_len, UndoRecPtr urec_ptr)
{
	RecnoUndoPayloadHeader hdr;
	const char *image_bytes;
	Size		image_len;
	Relation	rel;
	Buffer		buffer;
	BlockNumber blkno;
	OffsetNumber offnum;

	Assert(rmid == UNDO_RMID_RECNO);

	/*
	 * Defer during crash recovery (syscache may not be initialised) or during
	 * an aborting transaction (BumpContext makes relation_close() and pfree()
	 * unsafe).  The logical-revert worker will re-drive the record from a
	 * clean memory context.
	 */
	if (InRecovery || IsAbortedTransactionBlockState())
	{
		ereport(DEBUG2,
				(errmsg_internal("RECNO UNDO: deferring xid %u record at %llu "
								 "(in recovery or abort path)",
								 xid,
								 (unsigned long long) urec_ptr)));
		return UNDO_APPLY_SKIPPED;
	}

	/* Decode the common payload header */
	if (payload_len < SizeOfRecnoUndoPayloadHeader)
	{
		ereport(WARNING,
				(errmsg_internal("RECNO UNDO: payload too short (%zu bytes) "
								 "for record at %llu",
								 payload_len,
								 (unsigned long long) urec_ptr)));
		return UNDO_APPLY_ERROR;
	}
	memcpy(&hdr, payload, SizeOfRecnoUndoPayloadHeader);
	image_bytes = payload + SizeOfRecnoUndoPayloadHeader;
	image_len = payload_len - SizeOfRecnoUndoPayloadHeader;
	blkno = ItemPointerGetBlockNumber(&hdr.tid);
	offnum = ItemPointerGetOffsetNumber(&hdr.tid);

	/* Open the relation; skip if dropped */
	rel = try_relation_open(reloid, RowExclusiveLock);
	if (rel == NULL)
	{
		ereport(DEBUG2,
				(errmsg_internal("RECNO UNDO: relation %u no longer exists, "
								 "skipping record at %llu",
								 reloid,
								 (unsigned long long) urec_ptr)));
		return UNDO_APPLY_SKIPPED;
	}

	/* Skip if the target block was truncated away */
	if (RelationGetNumberOfBlocks(rel) <= blkno)
	{
		ereport(DEBUG2,
				(errmsg_internal("RECNO UNDO: block %u beyond end of "
								 "relation %u, skipping",
								 blkno, reloid)));
		relation_close(rel, RowExclusiveLock);
		return UNDO_APPLY_SKIPPED;
	}

	buffer = ReadBuffer(rel, blkno);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	switch (info)
	{
		case RECNO_UNDO_INSERT:
			apply_recno_undo_insert(rel, buffer, offnum, blkno,
									urec_ptr, xid);
			break;

		case RECNO_UNDO_UPDATE:
		case RECNO_UNDO_DELETE:
			if (!(hdr.flags & RECNO_UNDO_FLAG_HAS_TUPLE) || image_len == 0)
			{
				ereport(WARNING,
						(errmsg_internal("RECNO UNDO %s: missing before-image at %llu",
										 info == RECNO_UNDO_UPDATE ? "UPDATE" : "DELETE",
										 (unsigned long long) urec_ptr)));
				break;
			}
			apply_recno_undo_restore_tuple(rel, buffer, offnum, blkno,
										   urec_ptr, xid, info,
										   image_bytes, (uint32) image_len);
			break;

		case RECNO_UNDO_DELTA_UPDATE:
			{
				/*
				 * Reverse-apply the RecnoDiffRecord stored in the payload
				 * against the current (post-update) tuple to reconstruct the
				 * before-image, then restore.
				 */
				Page		page = BufferGetPage(buffer);
				ItemId		lp = PageGetItemId(page, offnum);
				const RecnoDiffRecord *diff;
				const char *cur_data;
				Size		cur_len;
				char	   *restored;

				if (!ItemIdIsNormal(lp))
				{
					ereport(DEBUG2,
							(errmsg_internal("RECNO UNDO DELTA_UPDATE: item "
											 "(%u, %u) not normal, skipping",
											 blkno, offnum)));
					break;
				}
				if (image_len < sizeof(RecnoDiffRecord))
				{
					ereport(WARNING,
							(errmsg_internal("RECNO UNDO DELTA_UPDATE: diff "
											 "truncated (%zu bytes) at %llu",
											 image_len,
											 (unsigned long long) urec_ptr)));
					break;
				}
				diff = (const RecnoDiffRecord *) image_bytes;
				cur_data = (const char *) PageGetItem(page, lp);
				cur_len = ItemIdGetLength(lp);

				/*
				 * Size the reconstruction buffer to the length the diff
				 * reconstructs to (old_total_len), not the current on-page
				 * length.  A length-shrinking update produces a larger
				 * before-image than the current tuple, so palloc(cur_len)
				 * would overflow when RecnoApplyDiffReverse writes it back.
				 */
				restored = palloc(diff->old_total_len);
				{
					Size		out_len = 0;

					if (!RecnoApplyDiffReverse(cur_data, cur_len, diff,
											   restored, &out_len))
					{
						ereport(WARNING,
								(errmsg_internal("RECNO UNDO DELTA_UPDATE: reverse-diff "
												 "failed at %llu; leaving tuple in place",
												 (unsigned long long) urec_ptr)));
						pfree(restored);
						break;
					}
					apply_recno_undo_restore_tuple(rel, buffer, offnum, blkno,
												   urec_ptr, xid,
												   RECNO_UNDO_DELTA_UPDATE,
												   restored, (uint32) out_len);
				}
				pfree(restored);
			}
			break;

		default:
			ereport(WARNING,
					(errmsg_internal("RECNO UNDO: unknown subtype 0x%x at %llu",
									 info, (unsigned long long) urec_ptr)));
			break;
	}

	UnlockReleaseBuffer(buffer);
	relation_close(rel, RowExclusiveLock);
	return UNDO_APPLY_SUCCESS;
}


/*
 * recno_undo_desc
 *		Describe a RECNO UNDO record for pg_waldump / debug logging.
 */
static void
recno_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
				const char *payload, Size payload_len)
{
	const char *subtype;
	RecnoUndoPayloadHeader hdr;

	switch (info)
	{
		case RECNO_UNDO_INSERT:
			subtype = "INSERT";
			break;
		case RECNO_UNDO_UPDATE:
			subtype = "UPDATE";
			break;
		case RECNO_UNDO_DELETE:
			subtype = "DELETE";
			break;
		case RECNO_UNDO_DELTA_UPDATE:
			subtype = "DELTA_UPDATE";
			break;
		default:
			subtype = "UNKNOWN";
			break;
	}

	if (payload_len >= SizeOfRecnoUndoPayloadHeader)
	{
		memcpy(&hdr, payload, SizeOfRecnoUndoPayloadHeader);
		appendStringInfo(buf,
						 "%s tid=(%u,%u) tuple_len=%u flags=0x%x",
						 subtype,
						 ItemPointerGetBlockNumber(&hdr.tid),
						 ItemPointerGetOffsetNumber(&hdr.tid),
						 hdr.tuple_len,
						 hdr.flags);
	}
	else
	{
		appendStringInfo(buf, "%s (truncated payload, %zu bytes)",
						 subtype, payload_len);
	}
}
