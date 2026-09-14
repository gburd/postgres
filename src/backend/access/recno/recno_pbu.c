/*-------------------------------------------------------------------------
 *
 * recno_pbu.c
 *	  RECNO per-backend UNDO shim (insert / fetch / redo side).
 *
 * Phase 10 retargets RECNO from the cluster-wide XactUndo engine
 * (access/undo/xactundo.c, UndoBufferAddRecord / PrepareXactUndoData) to the
 * per-backend UNDO engine (access/undo/perbackend,
 * PrepareUndoInsert/InsertPreparedUndo).  This is the ONLY RECNO file that
 * includes the per-backend engine's record/insert headers, because those
 * headers conflict with the rest of RECNO's includes.  Modeled exactly on
 * flux_pbu.c.
 *
 * WRITE PROTOCOL (one RECNO UNDO record per INSERT/UPDATE/DELETE):
 *   1. RecnoPbuPrepareInsert()       -- BEFORE the crit section: builds the
 *      UnpackedUndoRecord and calls PrepareUndoInsert(), reserving+pinning the
 *      undo buffers.  The payload is the caller's RecnoUndoPayloadHeader
 *      followed by the before-image bytes.
 *   2. RecnoPbuInsertUndo()          -- INSIDE the crit section, before the
 *      RECNO forward-op WAL record: WAL-logs undo-log meta then writes the
 *      record into the pinned buffers.
 *   3. RecnoPbuRegisterUndoBuffers() -- INSIDE the crit section, after the
 *      RECNO forward-op XLogRegisterBuffer/Data and before its XLogInsert():
 *      forces a full-page image of each undo page onto the forward record.
 *   4. RecnoPbuFinishUndo()          -- after XLogInsert(): stamps the record
 *      LSN onto the undo pages, releases them.
 *   RecnoPbuCancelUndo()             -- release the pinned undo buffers if the
 *      DML aborts before committing the record.
 *
 * The record is tagged uur_rmid = UNDO_RMID_RECNO and uur_type = the RECNO
 * undo subtype, so on abort execute_undo_actions() dispatches to
 * recno_undo_apply() (recno_undo.c) with the subtype in its "info" argument.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Author: Greg Burd <greg@burd.me>
 *
 * IDENTIFICATION
 *	  src/backend/access/recno/recno_pbu.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/perbackend/pbu_compat.h"
#include "access/perbackend/pbu_undoinsert.h"
#include "access/perbackend/pbu_undolog.h"
#include "access/perbackend/pbu_undorecord.h"
#include "access/recno_pbu.h"
#include "access/recno_undo.h"
#include "access/recno_xlog.h"
#include "access/undormgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "storage/bufmgr.h"
#include "storage/itemptr.h"

/*
 * One RECNO UNDO record is prepared/inserted at a time, entirely within its
 * own critical section, so a single static suffices (no re-entrancy).
 */
static UnpackedUndoRecord recno_pbu_uur;
static xl_undolog_meta recno_pbu_undometa;
static UndoRecPtr recno_pbu_urp = InvalidUndoRecPtr;
static xl_recno_undo recno_pbu_walhdr;
static bool recno_pbu_prepared = false;

/*
 * RecnoPbuPrepareInsert - reserve undo space for one RECNO UNDO record.
 *
 * Must be called BEFORE the caller's critical section.  subtype is a
 * RECNO_UNDO_* code; tid is the target tuple's TID; payload_hdr/len is the
 * fixed RecnoUndoPayloadHeader; image_bytes/len is the trailing before-image
 * (NULL/0 for INSERT records).  relpersistence maps to undo persistence.
 */
UndoRecPtr
RecnoPbuPrepareInsert(uint16 subtype, Oid reloid, ItemPointer tid,
					  const char *payload_hdr, uint32 payload_hdr_len,
					  const char *image_bytes, uint32 image_len,
					  char relpersistence)
{
	FullTransactionId fxid = GetTopFullTransactionId();
	UndoRecPtr	urp;

	Assert(!recno_pbu_prepared);

	MemSet(&recno_pbu_uur, 0, sizeof(recno_pbu_uur));
	recno_pbu_uur.uur_rmid = UNDO_RMID_RECNO;
	recno_pbu_uur.uur_type = (uint8) subtype;
	recno_pbu_uur.uur_info = 0;
	recno_pbu_uur.uur_reloid = reloid;
	recno_pbu_uur.uur_fork = MAIN_FORKNUM;
	recno_pbu_uur.uur_block = ItemPointerGetBlockNumber(tid);
	recno_pbu_uur.uur_offset = ItemPointerGetOffsetNumber(tid);
	recno_pbu_uur.uur_xid = XidFromFullTransactionId(fxid);
	recno_pbu_uur.uur_xidepoch = EpochFromFullTransactionId(fxid);

	initStringInfo(&recno_pbu_uur.uur_payload);
	if (payload_hdr != NULL && payload_hdr_len > 0)
		appendBinaryStringInfo(&recno_pbu_uur.uur_payload,
							   payload_hdr, payload_hdr_len);
	if (image_bytes != NULL && image_len > 0)
		appendBinaryStringInfo(&recno_pbu_uur.uur_payload,
							   image_bytes, image_len);

	MemSet(&recno_pbu_undometa, 0, sizeof(recno_pbu_undometa));

	urp = PrepareUndoInsert(&recno_pbu_uur, fxid,
							UndoPersistenceForRelPersistence(relpersistence),
							NULL, &recno_pbu_undometa);

	recno_pbu_prepared = true;
	recno_pbu_urp = urp;
	return urp;
}

/*
 * RecnoPbuInsertUndo - WAL-log the undo-log meta and write the prepared
 * record into the undo buffers.  Call INSIDE the crit section, before the
 * RECNO forward-op WAL record.
 */
void
RecnoPbuInsertUndo(void)
{
	Assert(recno_pbu_prepared);

	/*
	 * LogUndoMetaData() emits the xid->logno attach/meta record when this is
	 * the first undo insert after a checkpoint, which is what lets
	 * UndoLogAllocateInRecovery() resolve this xid's log during redo.  Because
	 * recno_redo re-runs this same insert (RecnoPbuRedoUndo), replaying the
	 * record re-derives the advanced insert pointer itself -- no post-insert
	 * meta record (CaptureCurrentUndoLogMeta/LogUndoMetaDataNow) is needed, and
	 * emitting one would be the FPI-producer protocol we are leaving behind.
	 */
	LogUndoMetaData(&recno_pbu_undometa);

	InsertPreparedUndo();
}

/*
 * RecnoPbuRegisterUndoBuffers - attach the pinned undo buffers, and the undo
 * record's own description, to the caller's forward-op WAL record.  Call INSIDE
 * the crit section, after the caller's XLogRegisterBuffer/Data and before its
 * XLogInsert().  first_block_id must be past any block id the RECNO forward-op
 * record already uses (block 0 = data page, plus any overflow blocks).
 *
 * No full-page image is forced.  Instead an xl_recno_undo header plus the undo
 * record's payload rides as block data on the first undo block, and recno_redo
 * re-runs the undo insert from it (RecnoPbuRedoUndo) to rebuild the undo page
 * content -- removing the two 8 KB undo-page images per in-place UPDATE that
 * dominated RECNO's UPDATE WAL volume.  Registering the buffers is still
 * required: it makes each undo page a block reference of this record, so the
 * page still gets an ordinary full-page image when one is needed (first touch
 * after a checkpoint, torn-page protection) and replay is verified under
 * wal_consistency_checking.
 */
void
RecnoPbuRegisterUndoBuffers(uint8 first_block_id)
{
	int			nblocks;

	Assert(recno_pbu_prepared);

	/*
	 * The header must outlive this call: XLogRegisterBufData only records a
	 * pointer, read at XLogInsert(); keep it in a static that stays valid
	 * until RecnoPbuFinishUndo(), not on the stack.
	 */
	recno_pbu_walhdr.urec_ptr = (uint64) recno_pbu_urp;
	recno_pbu_walhdr.reloid = recno_pbu_uur.uur_reloid;
	recno_pbu_walhdr.subtype = recno_pbu_uur.uur_type;
	recno_pbu_walhdr.payload_len = (uint16) recno_pbu_uur.uur_payload.len;
	/*
	 * uur_xid/uur_xidepoch were set from GetTopFullTransactionId() in
	 * RecnoPbuPrepareInsert, i.e. the xid the undo log is attached to.  REDO
	 * needs exactly that xid to resolve the log.
	 */
	recno_pbu_walhdr.top_xid = recno_pbu_uur.uur_xid;
	recno_pbu_walhdr.top_xid_epoch = recno_pbu_uur.uur_xidepoch;

	nblocks = RegisterUndoLogBuffersWithData(first_block_id,
											 (const char *) &recno_pbu_walhdr,
											 SizeOfRecnoUndo);

	/*
	 * Append the payload to the same block's data.  The prepared record's
	 * payload buffer stays alive until RecnoPbuFinishUndo(), past XLogInsert().
	 */
	if (nblocks > 0 && recno_pbu_uur.uur_payload.len > 0)
		XLogRegisterBufData(first_block_id, recno_pbu_uur.uur_payload.data,
							recno_pbu_uur.uur_payload.len);
}

/*
 * RecnoPbuUndoPending - true iff a RECNO undo record has been prepared and is
 * awaiting its FPI registration + LSN stamp.
 */
bool
RecnoPbuUndoPending(void)
{
	return recno_pbu_prepared;
}

/*
 * RecnoPbuFinishUndo - stamp the WAL LSN onto the undo pages, unlock/release
 * the undo buffers, reset state.  Call after the caller's XLogInsert().
 */
void
RecnoPbuFinishUndo(XLogRecPtr lsn)
{
	Assert(recno_pbu_prepared);

	UndoLogBuffersSetLSN(lsn);
	UnlockReleaseUndoBuffers();

	if (recno_pbu_uur.uur_payload.data != NULL)
	{
		pfree(recno_pbu_uur.uur_payload.data);
		recno_pbu_uur.uur_payload.data = NULL;
	}
	recno_pbu_prepared = false;
	recno_pbu_urp = InvalidUndoRecPtr;
}

/*
 * RecnoPbuCancelUndo - release the pinned undo buffers without writing the
 * record.  InsertPreparedUndo() has NOT been called, so no undo bytes were
 * written and the log's insert pointer was not advanced.
 */
void
RecnoPbuCancelUndo(void)
{
	if (!recno_pbu_prepared)
		return;

	UnlockReleaseUndoBuffers();

	if (recno_pbu_uur.uur_payload.data != NULL)
	{
		pfree(recno_pbu_uur.uur_payload.data);
		recno_pbu_uur.uur_payload.data = NULL;
	}
	recno_pbu_prepared = false;
	recno_pbu_urp = InvalidUndoRecPtr;
}

/*
 * RecnoPbuBlockIsUndo - true iff the given block reference of a WAL record is a
 * per-backend UNDO page rather than a RECNO relation page.  Undo pages live in
 * the reserved undo database OID (UndoLogDatabaseOid), so they are
 * distinguishable from any relation block regardless of block id.  The RECNO
 * redo handlers probe block ids past the forward-op blocks for the undo region,
 * and this keeps a forward-op block (main / overflow / cross-page destination)
 * from being mistaken for it.  Modeled on FluxPbuBlockIsUndo.
 */
bool
RecnoPbuBlockIsUndo(void *record, uint8 block_id)
{
	XLogReaderState *xlogrec = (XLogReaderState *) record;
	RelFileLocator rlocator;

	if (!XLogRecGetBlockTagExtended(xlogrec, block_id, &rlocator, NULL, NULL,
									NULL))
		return false;

	return rlocator.dbOid == UndoLogDatabaseOid;
}

/*
 * RecnoPbuRedoUndo - re-run the undo insert carried on a RECNO forward-op WAL
 * record, rebuilding the undo page content from the folded undo bytes rather
 * than from a forced full-page image.  Called from recno_redo() for every RECNO
 * opcode that can emit undo (INSERT/UPDATE/DELETE).  Modeled on FluxPbuRedoUndo.
 *
 * first_block_id is where the forward-op emitter started registering undo
 * buffers.  Undo blocks are the only ones in the reserved undo database, so the
 * search is unambiguous even for opcodes whose forward block count varies with
 * overflow / cross-page destinations.  A record that carries no undo simply has
 * no such block and this returns without doing anything.
 *
 * IDEMPOTENCE: the sequence of undo allocations during redo mirrors DO-time for
 * a given xid, so the record lands at the same offset in the same log and the
 * insert rewrites byte-identical content; the DO-time urec_ptr is re-derived
 * and a mismatch PANICs rather than silently misplacing the before-image.
 */
void
RecnoPbuRedoUndo(void *record, uint8 first_block_id)
{
	XLogReaderState *xlogrec = (XLogReaderState *) record;
	FullTransactionId fxid;
	UnpackedUndoRecord uur;
	xl_recno_undo undohdr;
	RecnoUndoPayloadHeader phdr;
	UndoRecPtr	urp;
	uint8		block_id;
	char	   *data = NULL;
	Size		datalen = 0;
	const char *payload;

	/*
	 * Locate the undo region: probe forward from first_block_id for a block
	 * that both lives in the undo database and carries block data (the region
	 * is attached to the FIRST undo block only).
	 */
	for (block_id = first_block_id;
		 block_id < first_block_id + RECNO_PBU_MAX_UNDO_BLOCKS;
		 block_id++)
	{
		if (!XLogRecHasBlockRef(xlogrec, block_id))
			break;				/* no more blocks at all */

		if (!RecnoPbuBlockIsUndo(xlogrec, block_id))
			continue;			/* a forward-op block, keep looking */

		data = XLogRecGetBlockData(xlogrec, block_id, &datalen);
		break;
	}

	/* No undo on this record. */
	if (data == NULL || datalen == 0)
		return;

	if (datalen < SizeOfRecnoUndo)
		elog(PANIC, "RECNO undo redo: undo region too short (%zu bytes, need %zu)",
			 datalen, SizeOfRecnoUndo);

	memcpy(&undohdr, data, SizeOfRecnoUndo);
	payload = data + SizeOfRecnoUndo;

	/*
	 * Resolve the undo log by the TOP-level xid recorded at DO time, not by
	 * this WAL record's own xid.  For a write inside a subtransaction the
	 * record is tagged with the subxact xid, but XLOG_UNDOLOG_ATTACH mapped
	 * the log under the top xid -- looking up the subxact xid reads an
	 * unpopulated (zero) slot in the xid->logno map, which is indistinguishable
	 * from log number 0 and so slips past the InvalidUndoLogNumber guard,
	 * failing later with "cannot find undo log number 0 for xid N" and
	 * aborting recovery.
	 */
	fxid = FullTransactionIdFromEpochAndXid(undohdr.top_xid_epoch,
											undohdr.top_xid);

	/*
	 * The before-image is the only copy of the pre-update bytes, so refuse to
	 * read past the WAL data rather than fabricate a record.
	 */
	if (datalen != SizeOfRecnoUndo + (Size) undohdr.payload_len ||
		undohdr.payload_len < SizeOfRecnoUndoPayloadHeader)
		elog(PANIC, "RECNO undo redo: inconsistent undo region "
			 "(region %zu bytes, payload_len %u)",
			 datalen, undohdr.payload_len);

	/*
	 * Rebuild the record EXACTLY as RecnoPbuPrepareInsert() built it.  Any
	 * difference in the fields UndoRecordSetInfo() keys on (fork, block,
	 * payload length) would change UndoRecordExpectedSize(), so this record --
	 * and every one after it in this log -- would land at a different offset
	 * than the original, and the version pointers already stamped into replayed
	 * tuples would dangle.
	 */
	MemSet(&uur, 0, sizeof(uur));
	uur.uur_rmid = UNDO_RMID_RECNO;
	uur.uur_type = (uint8) undohdr.subtype;
	uur.uur_info = 0;
	uur.uur_reloid = undohdr.reloid;
	uur.uur_fork = MAIN_FORKNUM;
	uur.uur_xid = XidFromFullTransactionId(fxid);
	uur.uur_xidepoch = EpochFromFullTransactionId(fxid);

	memcpy(&phdr, payload, SizeOfRecnoUndoPayloadHeader);
	uur.uur_block = ItemPointerGetBlockNumber(&phdr.tid);
	uur.uur_offset = ItemPointerGetOffsetNumber(&phdr.tid);

	/*
	 * Carry the payload verbatim (RecnoUndoPayloadHeader + before-image) so the
	 * replayed record is byte-identical to the original -- the version-chain
	 * reader and the abort path read it back unchanged.
	 */
	initStringInfo(&uur.uur_payload);
	appendBinaryStringInfo(&uur.uur_payload, payload, undohdr.payload_len);

	urp = PrepareUndoInsert(&uur, fxid, UNDO_PERMANENT, xlogrec, NULL);

	/*
	 * The replayed insert must land where the DO-time insert did.  A mismatch
	 * means the recovered undo-log metadata disagrees with the WAL; continuing
	 * would write this transaction's before-image at an offset the rollback
	 * walk will not find -- silently losing the ability to undo it.
	 */
	if (urp != (UndoRecPtr) undohdr.urec_ptr)
		elog(PANIC, "RECNO undo redo: undo record pointer mismatch "
			 "(replay " UINT64_FORMAT ", original " UINT64_FORMAT ")",
			 (uint64) urp, undohdr.urec_ptr);

	InsertPreparedUndo();

	/*
	 * Release the undo buffers.  The page LSNs are deliberately NOT stamped
	 * (matching zheap/FLUX redo): InsertPreparedUndo() dirtied the buffers, so
	 * the content reaches disk via the end-of-recovery checkpoint (or an
	 * intervening restartpoint), and a second replay simply re-inserts the
	 * identical bytes.
	 */
	UnlockReleaseUndoBuffers();

	pfree(uur.uur_payload.data);
}

/*
 * RecnoPbuFetchVersion - fetch one RECNO UNDO record for the version-chain
 * reader (RecnoReconstructVisibleVersion).
 *
 * Returns false if the record was discarded / unreadable.  On success:
 *   *out_subtype     = the RECNO undo subtype (uur_type).
 *   *out_xid         = the owning (top) xid that produced the CURRENT
 *                      candidate image (uur_xid); the reader tests this
 *                      against its snapshot.
 *   *out_payload     = a palloc'd copy of the record's uur_payload
 *                      (RecnoUndoPayloadHeader + before-image bytes); the
 *                      caller must pfree it.
 *   *out_payload_len = length of *out_payload.
 *
 * The payload is copied out (not aliased into the undo buffer) because
 * UndoRecordRelease frees the fetched record's buffers immediately.
 * Mirrors FluxPbuFetchVersion.
 */
bool
RecnoPbuFetchVersion(UndoRecPtr verptr, uint16 *out_subtype,
					 TransactionId *out_xid,
					 char **out_payload, uint32 *out_payload_len)
{
	UnpackedUndoRecord *urec;

	if (!UndoRecPtrIsValid(verptr))
		return false;

	urec = UndoFetchRecord(verptr, InvalidBlockNumber, InvalidOffsetNumber,
						   InvalidTransactionId, NULL, NULL);
	if (urec == NULL)
		return false;

	if (out_subtype != NULL)
		*out_subtype = urec->uur_type;

	if (out_xid != NULL)
		*out_xid = urec->uur_xid;

	if (out_payload != NULL)
	{
		uint32		len = (uint32) urec->uur_payload.len;

		if (len > 0)
		{
			char	   *buf = palloc(len);

			memcpy(buf, urec->uur_payload.data, len);
			*out_payload = buf;
		}
		else
			*out_payload = NULL;
		if (out_payload_len != NULL)
			*out_payload_len = len;
	}

	UndoRecordRelease(urec);
	return true;
}

/*
 * RecnoPbuFetchXid - header-only fetch: return the owning (top) xid of the
 * record at verptr (uur_xid).  Used by the version-chain reader to decide
 * whether the xid that produced the CURRENT candidate image is visible to the
 * reader's MVCC snapshot.  Returns false if the record was discarded /
 * unreadable.
 */
bool
RecnoPbuFetchXid(UndoRecPtr verptr, TransactionId *out_xid)
{
	UnpackedUndoRecord *urec;

	if (!UndoRecPtrIsValid(verptr))
		return false;

	urec = UndoFetchRecord(verptr, InvalidBlockNumber, InvalidOffsetNumber,
						   InvalidTransactionId, NULL, NULL);
	if (urec == NULL)
		return false;

	if (out_xid != NULL)
		*out_xid = urec->uur_xid;

	UndoRecordRelease(urec);
	return true;
}

/*
 * Subtransaction (SAVEPOINT) apply.  Set true while
 * RecnoPbuApplySubxactUndo() is reverse-applying a subxact's records so
 * recno_undo_apply() can scope its sLog cleanup correctly.
 */
static bool recno_pbu_in_subxact_apply = false;

bool
RecnoPbuInSubxactApply(void)
{
	return recno_pbu_in_subxact_apply;
}

UndoRecPtr
RecnoPbuGetCurrentUndoLatest(void)
{
	return (UndoRecPtr) GetCurrentTransactionPbuUndoLatest();
}

UndoRecPtr
RecnoPbuGetCurrentUndoStart(void)
{
	return (UndoRecPtr) GetCurrentTransactionPbuUndoStart();
}

/*
 * RecnoPbuApplySubxactUndo - reverse-apply an aborting subtransaction's
 * per-backend undo records, restoring pre-savepoint tuple images.  Modeled on
 * FluxPbuApplySubxactUndo.
 */
void
RecnoPbuApplySubxactUndo(UndoRecPtr from, UndoRecPtr stop_inclusive)
{
	const UndoRmgrData *rmgr = GetUndoRmgr(UNDO_RMID_RECNO);
	UndoRecPtr	cur = from;
	TransactionId xid;
	bool		done = false;

	if (rmgr == NULL || rmgr->rm_undo == NULL)
		return;
	if (!UndoRecPtrIsValid(cur) || !UndoRecPtrIsValid(stop_inclusive))
		return;

	xid = XidFromFullTransactionId(GetTopFullTransactionId());

	recno_pbu_in_subxact_apply = true;
	PG_TRY();
	{
		while (UndoRecPtrIsValid(cur) && !done)
		{
			UnpackedUndoRecord *urec;
			UndoRecPtr	prev;
			Buffer		buffer = InvalidBuffer;

			urec = UndoFetchRecord(cur, InvalidBlockNumber,
								   InvalidOffsetNumber,
								   InvalidTransactionId, NULL, NULL);
			if (urec == NULL)
				break;

			if (urec->uur_rmid == UNDO_RMID_RECNO)
				(void) rmgr->rm_undo(UNDO_RMID_RECNO, urec->uur_type, xid,
									 urec->uur_reloid,
									 urec->uur_payload.data,
									 urec->uur_payload.len, cur);

			if (cur == stop_inclusive)
				done = true;

			if (urec->uur_info & UREC_INFO_TRANSACTION)
				prev = InvalidUndoRecPtr;
			else
				prev = UndoGetPrevUndoRecptr(cur, urec->uur_prevurp, &buffer);

			UndoRecordRelease(urec);
			if (BufferIsValid(buffer))
				UnlockReleaseBuffer(buffer);

			cur = prev;
		}
	}
	PG_FINALLY();
	{
		recno_pbu_in_subxact_apply = false;
	}
	PG_END_TRY();
}
