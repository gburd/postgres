/*-------------------------------------------------------------------------
 *
 * flux_pbu.c
 *	  FLUX per-backend UNDO shim (insert / fetch / redo side).
 *
 * FLUX writes its DML before-images to the per-backend UNDO engine
 * (access/undo/perbackend, PrepareUndoInsert/UndoFetchRecord).  This
 * translation unit is the small "pbu shim" (mirroring fileops_pbu.c): it is
 * the ONLY FLUX file that CAN include the per-backend engine's record/insert
 * headers, because those headers redefine UndoRecordHeader / UndoLogControl
 * and still use the pre-RelFileLocator RelFileNode type, and so conflict with
 * the other UNDO headers that the rest of the FLUX access method pulls in.
 * The rest of FLUX reaches this engine only through the by-prototype entry
 * points declared in access/flux.h.
 *
 * WRITE PROTOCOL (one FLUX UNDO record per INSERT/UPDATE/DELETE before-image,
 * mirroring the FILEOPS crash-safe choreography):
 *   1. FluxPbuPrepareInsert()      -- BEFORE the critical section.  Builds the
 *      UnpackedUndoRecord and calls PrepareUndoInsert(), reserving+pinning the
 *      undo buffers; may palloc.  Returns the UndoRecPtr that becomes the
 *      tuple's version-chain head (t_verptr).
 *   2. FluxPbuInsertUndo()         -- INSIDE the crit section, before the FLUX
 *      forward-op WAL record: WAL-logs undo-log meta then writes the record
 *      into the pinned buffers (InsertPreparedUndo also chains the xact via
 *      SetCurrentUndoLocation).
 *   3. FluxPbuRegisterUndoBuffers() -- INSIDE the crit section, after the FLUX
 *      forward-op XLogRegisterBuffer()/XLogRegisterData() and before its
 *      XLogInsert(): attaches each pinned undo page to the FLUX forward WAL
 *      record.  No full-page image is forced: the forward record also carries
 *      the undo record's own bytes (xl_flux_undo, attached as block data on the
 *      first undo block), and flux_redo() RE-RUNS the undo insert (FluxPbuRedoUndo) to rebuild the
 *      undo page from those bytes.  Registering the buffers still matters --
 *      it makes the undo page a block reference of this record, so recovery
 *      restores it from an ordinary FPI when one was taken for other reasons
 *      (a page's first touch after a checkpoint, or wal_consistency_checking).
 *      THIS is what makes per-backend FLUX undo crash-safe: the undo bytes ride
 *      to disk on the same WAL record as the forward FLUX page mutation.
 *   4. FluxPbuFinishUndo()          -- after XLogInsert(): stamps the record
 *      LSN onto the undo pages, unlocks/releases them, resets state.
 *   FluxPbuCancelUndo()             -- release the pinned undo buffers if the
 *      DML aborts before it commits the record (e.g. a no-op CAS).
 *
 * The record is tagged uur_rmid = UNDO_RMID_FLUX and uur_type = the FLUX undo
 * subtype (FLUX_UNDO_INSERT/UPDATE/DELETE), so on abort execute_undo_actions()
 * -> GetUndoRmgr(UNDO_RMID_FLUX) -> rm_undo reaches flux_undo_apply()
 * (flux_undo.c) with the subtype in its "info" argument.  The whole
 * before-image payload -- FluxUndoPayloadHeader followed by the before-image
 * tuple bytes -- is carried in uur_payload so that (a) flux_undo_apply reads
 * it unchanged (execute_undo_actions_page passes rec->uur_payload to rm_undo),
 * and (b) FluxReconstructVisibleVersion reads the trailing tuple bytes back
 * exactly as before.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/flux/flux_pbu.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/perbackend/pbu_compat.h"
#include "access/perbackend/pbu_undoinsert.h"
#include "access/perbackend/pbu_undolog.h"
#include "access/perbackend/pbu_undorecord.h"
#include "access/flux_undo.h"
#include "access/flux_pbu.h"
#include "access/flux_xlog.h"
#include "access/undormgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "storage/bufmgr.h"
#include "storage/itemptr.h"

/*
 * One FLUX UNDO record is prepared/inserted at a time: each DML op emits
 * exactly one, entirely within its own critical section, before returning
 * (prepare -> insert -> register -> set-lsn -> release runs to completion with
 * no nesting), so a single static suffices and there is no re-entrancy.
 */
static UnpackedUndoRecord flux_pbu_uur;
static xl_undolog_meta flux_pbu_undometa;
static UndoRecPtr flux_pbu_urp = InvalidUndoRecPtr;
static xl_flux_undo flux_pbu_walhdr;
static bool flux_pbu_prepared = false;

/*
 * FluxPbuPrepareInsert - reserve undo space for one FLUX UNDO record and
 * return the UndoRecPtr that will be stamped into the tuple's t_verptr.
 *
 * Must be called BEFORE the caller's critical section (PrepareUndoInsert may
 * palloc / extend the undo log / ereport).  subtype is a FLUX_UNDO_* code;
 * tid is the target tuple's TID; tuple_bytes/tuple_len is the before-image (or
 * NULL/0 for INSERT records, which carry no image).  relpersistence is the
 * relation's relpersistence ('p'/'u'/'t'), mapped to the undo persistence.
 *
 * The uur_payload holds a FluxUndoPayloadHeader immediately followed by the
 * before-image tuple bytes -- the exact layout flux_undo_apply() and
 * FluxReconstructVisibleVersion() expect.
 */
UndoRecPtr
FluxPbuPrepareInsert(uint16 subtype, Oid reloid, ItemPointer tid,
					 const char *tuple_bytes, uint32 tuple_len,
					 char relpersistence)
{
	FullTransactionId fxid = GetTopFullTransactionId();
	FluxUndoPayloadHeader hdr;
	UndoRecPtr	urp;

	Assert(!flux_pbu_prepared);

	MemSet(&flux_pbu_uur, 0, sizeof(flux_pbu_uur));
	flux_pbu_uur.uur_rmid = UNDO_RMID_FLUX;
	flux_pbu_uur.uur_type = (uint8) subtype;
	flux_pbu_uur.uur_info = 0;
	flux_pbu_uur.uur_reloid = reloid;
	flux_pbu_uur.uur_fork = MAIN_FORKNUM;
	flux_pbu_uur.uur_block = ItemPointerGetBlockNumber(tid);
	flux_pbu_uur.uur_offset = ItemPointerGetOffsetNumber(tid);

	/*
	 * The record's xid/epoch identify the owning transaction; the abort path
	 * asserts the fetched record's (uur_xidepoch,uur_xid) equals the aborting
	 * full xid.  UndoRecordAllocate fills uur_xidepoch for the
	 * transaction-header record, but the caller must supply uur_xid.  This is
	 * also the xid FluxReconstructVisibleVersion reads back (the producer of
	 * the CURRENT candidate image) for its snapshot visibility test.
	 */
	flux_pbu_uur.uur_xid = XidFromFullTransactionId(fxid);
	flux_pbu_uur.uur_xidepoch = EpochFromFullTransactionId(fxid);

	/* Build the FLUX before-image payload: fixed header + tuple bytes. */
	MemSet(&hdr, 0, sizeof(hdr));
	ItemPointerCopy(tid, &hdr.tid);
	hdr.tuple_len = tuple_len;
	hdr.flags = (tuple_bytes != NULL && tuple_len > 0) ?
		FLUX_UNDO_FLAG_HAS_TUPLE : 0;

	/*
	 * writer_xid is the xid that produced this record's host image -- the
	 * CURRENT (possibly SUBXACT) xid, NOT the top xid.  The version-chain
	 * reader and the lost-update probe test THIS against the snapshot so that
	 * a SAVEPOINT rollback (which aborts a subxact while the top xact keeps
	 * running) resolves visibility correctly.  uur_xid below stays the owning
	 * TOP xid because the per-backend engine's abort path asserts the fetched
	 * record's uur_xid equals the aborting full xid.
	 */
	hdr.writer_xid = GetCurrentTransactionId();

	initStringInfo(&flux_pbu_uur.uur_payload);
	appendBinaryStringInfo(&flux_pbu_uur.uur_payload,
						   (char *) &hdr, SizeOfFluxUndoPayloadHeader);
	if (tuple_bytes != NULL && tuple_len > 0)
		appendBinaryStringInfo(&flux_pbu_uur.uur_payload,
							   tuple_bytes, tuple_len);

	MemSet(&flux_pbu_undometa, 0, sizeof(flux_pbu_undometa));

	urp = PrepareUndoInsert(&flux_pbu_uur, fxid,
							UndoPersistenceForRelPersistence(relpersistence),
							NULL, &flux_pbu_undometa);

	flux_pbu_prepared = true;
	flux_pbu_urp = urp;
	return urp;
}

/*
 * FluxPbuInsertUndo - WAL-log the undo-log meta and write the prepared record
 * into the undo buffers.  Call INSIDE the crit section, before the FLUX
 * forward-op WAL record.
 *
 * LogUndoMetaData() emits the xid->logno attach/meta record when this is the
 * first undo insert after a checkpoint, which is what lets
 * UndoLogAllocateInRecovery() resolve this xid's log during redo.  Because
 * flux_redo() re-runs this same insert (FluxPbuRedoUndo), replaying the record
 * re-derives the advanced insert pointer itself -- no post-insert meta record
 * is needed, unlike an FPI-based producer such as fileops_pbu.c.
 */
void
FluxPbuInsertUndo(void)
{
	Assert(flux_pbu_prepared);

	LogUndoMetaData(&flux_pbu_undometa);

	InsertPreparedUndo();
}

/*
 * FluxPbuRegisterUndoBuffers - attach the pinned undo buffers, and the undo
 * record's own description, to the caller's forward-op WAL record.
 *
 * Call INSIDE the crit section, after the caller's XLogRegisterBuffer()/
 * XLogRegisterData() and before its XLogInsert().  first_block_id is the WAL
 * block id at which to start registering the undo buffers -- it must be past
 * any block id the FLUX forward-op record already uses (block 0 = data page,
 * plus any overflow / cross-page destination blocks).  Returns the number of
 * undo blocks registered.
 *
 * No full-page image is forced.  Instead an xl_flux_undo header plus the undo
 * record's payload is attached as block data on the first undo block, and
 * flux_redo() re-runs the undo insert from it (FluxPbuRedoUndo).  That removes
 * the 8 KB undo-page image per undo record which dominated FLUX's UPDATE WAL
 * volume.  Registering the buffers is still required: it makes each undo page a
 * block reference of this record, so the page gets an ordinary full-page image
 * when it needs one (first touch after a checkpoint, i.e. torn-page protection)
 * and the replay is verified under wal_consistency_checking.
 */
int
FluxPbuRegisterUndoBuffers(uint8 first_block_id)
{
	int			nblocks;

	Assert(flux_pbu_prepared);

	/*
	 * The header must outlive this call: XLogRegisterBufData only records a
	 * pointer and the bytes are read at XLogInsert(), so it lives in a static
	 * that stays valid until FluxPbuFinishUndo(), not on the stack.
	 */
	flux_pbu_walhdr.urec_ptr = (uint64) flux_pbu_urp;
	flux_pbu_walhdr.reloid = flux_pbu_uur.uur_reloid;
	flux_pbu_walhdr.subtype = flux_pbu_uur.uur_type;
	flux_pbu_walhdr.payload_len = (uint16) flux_pbu_uur.uur_payload.len;

	nblocks = RegisterUndoLogBuffersWithData(first_block_id,
											 (const char *) &flux_pbu_walhdr,
											 SizeOfFluxUndo);

	/*
	 * Append the payload to the same block's data.  The prepared record's
	 * payload buffer stays alive until FluxPbuFinishUndo(), i.e. past
	 * XLogInsert().
	 */
	if (nblocks > 0 && flux_pbu_uur.uur_payload.len > 0)
		XLogRegisterBufData(first_block_id, flux_pbu_uur.uur_payload.data,
							flux_pbu_uur.uur_payload.len);

	return nblocks;
}

/*
 * FluxPbuUndoPending - true iff a FLUX undo record has been prepared+inserted
 * for the current op and is awaiting its WAL registration + LSN stamp.  The
 * FLUX forward-op WAL functions (flux_xlog.c) query this to decide whether to
 * fold the undo pages onto their record.
 */
bool
FluxPbuUndoPending(void)
{
	return flux_pbu_prepared;
}

/*
 * FluxPbuFinishUndo - stamp the WAL LSN onto the undo pages, then
 * unlock/release the undo buffers and reset state.  Call after the caller's
 * XLogInsert().  lsn is the LSN of the FLUX forward-op WAL record that carried
 * the undo buffers.
 */
void
FluxPbuFinishUndo(XLogRecPtr lsn)
{
	Assert(flux_pbu_prepared);

	UndoLogBuffersSetLSN(lsn);
	UnlockReleaseUndoBuffers();

	if (flux_pbu_uur.uur_payload.data != NULL)
	{
		pfree(flux_pbu_uur.uur_payload.data);
		flux_pbu_uur.uur_payload.data = NULL;
	}
	flux_pbu_prepared = false;
	flux_pbu_urp = InvalidUndoRecPtr;
}

/*
 * FluxPbuCancelUndo - release the pinned undo buffers without writing the
 * record.  Used when the DML op decides not to commit the reserved record
 * (e.g. a same-size CAS update that finds no byte actually changed).  Because
 * InsertPreparedUndo() has NOT been called, no undo bytes were written and the
 * log's insert pointer was not advanced; releasing the buffers leaves the
 * reserved space unused (identical to the FILEOPS no-op path).
 */
void
FluxPbuCancelUndo(void)
{
	if (!flux_pbu_prepared)
		return;

	UnlockReleaseUndoBuffers();

	if (flux_pbu_uur.uur_payload.data != NULL)
	{
		pfree(flux_pbu_uur.uur_payload.data);
		flux_pbu_uur.uur_payload.data = NULL;
	}
	flux_pbu_prepared = false;
	flux_pbu_urp = InvalidUndoRecPtr;
}

/*
 * FluxPbuBlockIsUndo - true iff the given block reference of a WAL record is a
 * per-backend UNDO page rather than a FLUX relation page.
 *
 * Undo pages live in the reserved undo database OID, so they are
 * distinguishable from any relation block regardless of which block id they
 * landed at.  The FLUX redo handlers' overflow-replay loops iterate block ids
 * 1..N looking for FLUX overflow pages, and the undo pages folded onto the same
 * record sit in that range; without this test such a loop would treat an undo
 * page as an overflow page -- FluxInitPage over it, parse undo bytes as overflow
 * records -- destroying the before-image the rollback needs.  Exposed here
 * because UndoLogDatabaseOid lives in the per-backend engine headers, which the
 * rest of FLUX cannot include.
 */
bool
FluxPbuBlockIsUndo(void *record, uint8 block_id)
{
	XLogReaderState *xlogrec = (XLogReaderState *) record;
	RelFileLocator rlocator;

	if (!XLogRecGetBlockTagExtended(xlogrec, block_id, &rlocator, NULL, NULL,
									NULL))
		return false;

	return rlocator.dbOid == UndoLogDatabaseOid;
}

/*
 * FluxPbuRedoUndo - re-run the undo insert carried on a FLUX forward-op WAL
 * record.
 *
 * Called from flux_redo() for every FLUX opcode that can emit undo, mirroring
 * zheap's redo routines (zheapamxlog.c): find the undo region, rebuild the
 * UnpackedUndoRecord from it, PrepareUndoInsert() (which in recovery resolves
 * the log from the xid->logno map that XLOG_UNDOLOG_ATTACH/META replay
 * maintains, and pins/locks the undo buffers), then InsertPreparedUndo() to
 * write the bytes.  The undo page content is thus reconstructed, not copied from
 * an 8 KB image.
 *
 * first_block_id is where the forward-op emitter started registering undo
 * buffers.  Undo blocks are always the LAST blocks of a FLUX record, and they
 * are the only ones in the reserved undo database (UndoLogDatabaseOid), so the
 * search is unambiguous even for opcodes whose forward block count varies with
 * overflow / cross-page destinations.  A record that carries no undo simply has
 * no such block and this returns without doing anything.
 *
 * IDEMPOTENCE: the sequence of undo allocations during redo mirrors the sequence
 * during DO for a given xid, so on replay the record lands at the same offset in
 * the same log and the insert rewrites byte-identical content.  Replaying a
 * record whose undo was already flushed therefore rewrites the same bytes rather
 * than double-applying, and a page that has since been discarded yields an
 * invalid buffer that InsertPreparedUndo() skips.
 */
void
FluxPbuRedoUndo(void *record, uint8 first_block_id)
{
	XLogReaderState *xlogrec = (XLogReaderState *) record;
	FullTransactionId fxid = XLogRecGetFullXid(xlogrec);
	UnpackedUndoRecord uur;
	xl_flux_undo undohdr;
	FluxUndoPayloadHeader phdr;
	UndoRecPtr	urp;
	uint8		block_id;
	char	   *data = NULL;
	Size		datalen = 0;
	const char *payload;

	/*
	 * Locate the undo region.  Probe forward from first_block_id for a block
	 * that both lives in the undo database and carries block data (the region
	 * is attached to the FIRST undo block only).
	 */
	for (block_id = first_block_id;
		 block_id < first_block_id + FLUX_PBU_MAX_UNDO_BLOCKS;
		 block_id++)
	{
		if (!XLogRecHasBlockRef(xlogrec, block_id))
			break;				/* no more blocks at all */

		if (!FluxPbuBlockIsUndo(xlogrec, block_id))
			continue;			/* a forward-op block, keep looking */

		data = XLogRecGetBlockData(xlogrec, block_id, &datalen);
		break;
	}

	/* No undo on this record. */
	if (data == NULL || datalen == 0)
		return;

	if (datalen < SizeOfFluxUndo)
		elog(PANIC, "FLUX undo redo: undo region too short (%zu bytes, need %zu)",
			 datalen, SizeOfFluxUndo);

	memcpy(&undohdr, data, SizeOfFluxUndo);
	payload = data + SizeOfFluxUndo;

	/*
	 * The before-image is the only copy of the pre-update bytes, so refuse to
	 * read past the WAL data rather than fabricate a record.
	 */
	if (datalen != SizeOfFluxUndo + (Size) undohdr.payload_len ||
		undohdr.payload_len < SizeOfFluxUndoPayloadHeader)
		elog(PANIC, "FLUX undo redo: inconsistent undo region "
			 "(region %zu bytes, payload_len %u)",
			 datalen, undohdr.payload_len);

	/*
	 * Rebuild the record EXACTLY as FluxPbuPrepareInsert() built it.  Any
	 * difference in the fields UndoRecordSetInfo() keys on (fork, block,
	 * payload length) would change UndoRecordExpectedSize(), so this record --
	 * and every record after it in this log -- would land at a different offset
	 * than the original, and the version pointers already stamped into replayed
	 * tuples would dangle.
	 */
	MemSet(&uur, 0, sizeof(uur));
	uur.uur_rmid = UNDO_RMID_FLUX;
	uur.uur_type = (uint8) undohdr.subtype;
	uur.uur_info = 0;
	uur.uur_reloid = undohdr.reloid;
	uur.uur_fork = MAIN_FORKNUM;
	uur.uur_xid = XidFromFullTransactionId(fxid);
	uur.uur_xidepoch = EpochFromFullTransactionId(fxid);

	memcpy(&phdr, payload, SizeOfFluxUndoPayloadHeader);
	uur.uur_block = ItemPointerGetBlockNumber(&phdr.tid);
	uur.uur_offset = ItemPointerGetOffsetNumber(&phdr.tid);

	/*
	 * The payload is carried verbatim (FluxUndoPayloadHeader + before-image
	 * bytes), so the replayed record is byte-identical to the original --
	 * including writer_xid, which the version-chain reader
	 * (FluxReconstructVisibleVersion) and the lost-update probe read back.
	 */
	initStringInfo(&uur.uur_payload);
	appendBinaryStringInfo(&uur.uur_payload, payload, undohdr.payload_len);

	urp = PrepareUndoInsert(&uur, fxid, UNDO_PERMANENT, xlogrec, NULL);

	/*
	 * The replayed insert must land where the DO-time insert did.  A mismatch
	 * means the recovered undo-log metadata disagrees with the WAL, and
	 * continuing would write this transaction's before-image at an offset the
	 * rollback walk will not find -- silently losing the ability to undo it.
	 */
	if (urp != (UndoRecPtr) undohdr.urec_ptr)
		elog(PANIC, "FLUX undo redo: undo record pointer mismatch "
			 "(replay " UINT64_FORMAT ", original " UINT64_FORMAT ")",
			 (uint64) urp, undohdr.urec_ptr);

	InsertPreparedUndo();

	/*
	 * Release the undo buffers.  The page LSNs are deliberately NOT stamped,
	 * matching zheap's redo routines: InsertPreparedUndo() has dirtied the
	 * buffers, so the content reaches disk via the end-of-recovery checkpoint
	 * (or an intervening restartpoint), and WAL replayed a second time simply
	 * re-inserts the identical bytes.
	 */
	UnlockReleaseUndoBuffers();

	pfree(uur.uur_payload.data);
}

/*
 * FluxPbuFetchVersion - fetch one FLUX UNDO record for the version-chain
 * reader (FluxReconstructVisibleVersion).
 *
 * Returns false if the record was discarded / unreadable.  On success:
 *   *out_xid       = the xid that produced the CURRENT candidate image
 *                    (the payload's writer_xid, a possibly-SUBXACT xid) --
 *                    the reader tests this against its snapshot.  This is NOT
 *                    uur_xid (which is the owning top xid).
 *   *out_subtype   = the FLUX undo subtype (uur_type).
 *   *out_payload   = a palloc'd copy of the record's uur_payload (the
 *                    FluxUndoPayloadHeader + before-image tuple bytes); the
 *                    caller must pfree it.
 *   *out_payload_len = length of *out_payload.
 *
 * The payload is copied out (not aliased into the undo buffer) because
 * UndoRecordRelease frees the fetched record's buffers immediately.
 */
bool
FluxPbuFetchVersion(UndoRecPtr verptr, TransactionId *out_xid,
					uint16 *out_subtype, char **out_payload,
					Size *out_payload_len)
{
	UnpackedUndoRecord *urec;

	if (!UndoRecPtrIsValid(verptr))
		return false;

	urec = UndoFetchRecord(verptr, InvalidBlockNumber, InvalidOffsetNumber,
						   InvalidTransactionId, NULL, NULL);
	if (urec == NULL)
		return false;

	if (out_xid != NULL)
	{
		/*
		 * The producing (possibly SUBXACT) xid lives in the payload's
		 * FluxUndoPayloadHeader.writer_xid, not in uur_xid (which is the owning
		 * top xid the engine keys on).  Fall back to uur_xid if the payload is
		 * somehow too short.
		 */
		if (urec->uur_payload.len >= (int) SizeOfFluxUndoPayloadHeader)
		{
			FluxUndoPayloadHeader phdr;

			memcpy(&phdr, urec->uur_payload.data, SizeOfFluxUndoPayloadHeader);
			*out_xid = phdr.writer_xid;
		}
		else
			*out_xid = urec->uur_xid;
	}
	if (out_subtype != NULL)
		*out_subtype = urec->uur_type;

	if (out_payload != NULL)
	{
		Size		len = urec->uur_payload.len;

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
 * FluxPbuFetchXid - header-only fetch: return just the producing xid of the
 * record at verptr (the payload's writer_xid, a possibly-SUBXACT xid).
 * Replaces the earlier fork-engine header probe on the lost-update conflict
 * probe (flux_mvcc.c) and the ANALYZE dead-tuple proxy (flux_handler.c).  Returns
 * false if the record was discarded / unreadable.
 */
bool
FluxPbuFetchXid(UndoRecPtr verptr, TransactionId *out_xid)
{
	UnpackedUndoRecord *urec;

	if (!UndoRecPtrIsValid(verptr))
		return false;

	urec = UndoFetchRecord(verptr, InvalidBlockNumber, InvalidOffsetNumber,
						   InvalidTransactionId, NULL, NULL);
	if (urec == NULL)
		return false;

	if (out_xid != NULL)
	{
		if (urec->uur_payload.len >= (int) SizeOfFluxUndoPayloadHeader)
		{
			FluxUndoPayloadHeader phdr;

			memcpy(&phdr, urec->uur_payload.data, SizeOfFluxUndoPayloadHeader);
			*out_xid = phdr.writer_xid;
		}
		else
			*out_xid = urec->uur_xid;
	}

	UndoRecordRelease(urec);
	return true;
}

/*
 * FluxPbuRecordExists - true iff the record at verptr has not been discarded.
 * Used by the ANALYZE dead-tuple proxy (flux_handler.c) as a cheap
 * "is there retained history?" probe.
 */
bool
FluxPbuRecordExists(UndoRecPtr verptr)
{
	UndoLogControl *log;
	bool		valid;

	if (!UndoRecPtrIsValid(verptr))
		return false;

	/*
	 * UndoRecordIsValid() asserts the caller holds log->discard_lock in shared
	 * mode (it protects the record from being discarded concurrently, and it
	 * reads log->oldest_data under that lock).  Acquire it here, matching every
	 * other UndoRecordIsValid() caller.  This path runs in autovacuum/ANALYZE
	 * workers concurrently with inserting backends and the discard worker.
	 *
	 * If the whole log has already been dropped, UndoLogGet() returns NULL and
	 * the record is certainly gone.
	 */
	log = UndoLogGet(UndoRecPtrGetLogNo(verptr));
	if (log == NULL)
		return false;

	LWLockAcquire(&log->discard_lock, LW_SHARED);
	valid = UndoRecordIsValid(verptr);

	/*
	 * UndoRecordIsValid() releases discard_lock itself when it returns false;
	 * on true it leaves the lock held for the caller to release.
	 */
	if (valid)
		LWLockRelease(&log->discard_lock);

	return valid;
}

/*
 * Set true while FluxPbuApplySubxactUndo() is reverse-applying a
 * subtransaction's records.  flux_undo_apply() reads it (via
 * FluxPbuInSubxactApply) to SKIP its top-xid sLog-marker cleanup: the sLog
 * markers are per-(xid,subxid) and the SUBXACT_EVENT_ABORT_SUB handler's
 * SLogTupleRemoveBySubXid does the correct subxact-scoped cleanup, whereas a
 * SLogTupleRemoveByXidSingle(top_xid) here would wrongly drop markers other
 * (sibling / parent) subtransactions still need.
 */
static bool flux_pbu_in_subxact_apply = false;

bool
FluxPbuInSubxactApply(void)
{
	return flux_pbu_in_subxact_apply;
}

/*
 * FluxPbuGetCurrentUndoStart / FluxPbuGetCurrentUndoLatest - the current
 * TransactionState's per-backend undo chain start / latest record pointers.
 * At SUBXACT_EVENT_ABORT_SUB the CurrentTransactionState is the aborting
 * subxact, so these bracket exactly that subxact's per-backend undo records
 * (both InvalidUndoRecPtr if the subxact wrote no undo).
 */
UndoRecPtr
FluxPbuGetCurrentUndoLatest(void)
{
	return (UndoRecPtr) GetCurrentTransactionPbuUndoLatest();
}

UndoRecPtr
FluxPbuGetCurrentUndoStart(void)
{
	return (UndoRecPtr) GetCurrentTransactionPbuUndoStart();
}

/*
 * FluxPbuApplySubxactUndo - reverse-apply an aborting subtransaction's
 * per-backend undo records, physically restoring the pre-savepoint tuple
 * images.
 *
 * from            = the subtransaction's latest undo record
 *                   (GetCurrentTransactionPbuUndoLatest while still in the
 *                   aborting subxact's TransactionState).
 * stop_inclusive  = the subtransaction's FIRST undo record
 *                   (GetCurrentTransactionPbuUndoStart in the same state).
 *                   pbuUndoStartPtr/pbuUndoLatestPtr are per-TransactionState,
 *                   so at SUBXACT_EVENT_ABORT_SUB they bracket exactly this
 *                   subxact's records.  We apply from `from` back to AND
 *                   INCLUDING stop_inclusive, i.e. only this subxact's
 *                   records; the parent's pre-savepoint records are left
 *                   intact.
 *
 * Walks the per-backend undo chain and dispatches each record to the FLUX undo
 * rmgr's rm_undo (flux_undo_apply), which restores the before-image in place
 * and emits its CLR.  Called from the FLUX SUBXACT_EVENT_ABORT_SUB handler
 * (wrapped in EnterInlineUndoApplyState so relation opens are legal).
 */
void
FluxPbuApplySubxactUndo(UndoRecPtr from, UndoRecPtr stop_inclusive)
{
	const UndoRmgrData *rmgr = GetUndoRmgr(UNDO_RMID_FLUX);
	UndoRecPtr	cur = from;
	TransactionId xid;
	bool		done = false;

	if (rmgr == NULL || rmgr->rm_undo == NULL)
		return;
	if (!UndoRecPtrIsValid(cur) || !UndoRecPtrIsValid(stop_inclusive))
		return;

	xid = XidFromFullTransactionId(GetTopFullTransactionId());

	flux_pbu_in_subxact_apply = true;
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
				break;			/* discarded -- nothing left to restore */

			/* Only reverse FLUX records (the xact writes only these). */
			if (urec->uur_rmid == UNDO_RMID_FLUX)
				(void) rmgr->rm_undo(UNDO_RMID_FLUX, urec->uur_type, xid,
									 urec->uur_reloid,
									 urec->uur_payload.data,
									 urec->uur_payload.len, cur);

			/* Stop after applying the subxact's oldest (start) record. */
			if (cur == stop_inclusive)
				done = true;

			/* Compute the previous record in this xact's chain. */
			if (urec->uur_info & UREC_INFO_TRANSACTION)
				prev = InvalidUndoRecPtr;	/* first record of the xact */
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
		flux_pbu_in_subxact_apply = false;
	}
	PG_END_TRY();
}
