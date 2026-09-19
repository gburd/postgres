/*-------------------------------------------------------------------------
 *
 * recno_pbu.h
 *	  By-prototype interface to the RECNO per-backend UNDO shim (recno_pbu.c).
 *
 * The rest of the RECNO access method includes access/recno.h and the various
 * recno_*.h headers and cannot include the per-backend UNDO engine headers
 * (they conflict, redefining UndoRecordHeader / RelFileNode etc.).  RECNO
 * therefore drives the per-backend insert / redo path through these entry
 * points, the only interface recno_pbu.c exposes.  Modeled on flux_pbu.h.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Author: Greg Burd <greg@burd.me>
 *
 * src/include/access/recno_pbu.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_PBU_H
#define RECNO_PBU_H

#include "access/undodefs.h"
#include "access/xlogdefs.h"
#include "storage/itemptr.h"

/*
 * Maximum number of undo pages one RECNO undo record can span; the number of
 * block ids to probe on redo.  XLogRecHasBlockRef breaks the probe loop at
 * the first missing block id.
 */
#define RECNO_PBU_MAX_UNDO_BLOCKS	4

/*
 * WRITE PROTOCOL (see recno_pbu.c).  One record per DML op:
 *   urp = RecnoPbuPrepareInsert()    -- before crit
 *   RecnoPbuInsertUndo()             -- in crit, before the RECNO forward WAL
 *   RecnoPbuRegisterUndoBuffers(bid) -- in crit, after XLogRegisterBuffer/Data
 *                                       and before XLogInsert (fold the undo
 *                                       record's bytes onto the RECNO forward
 *                                       record; redo re-runs the insert)
 *   RecnoPbuFinishUndo(lsn)          -- after XLogInsert (stamp LSN, release)
 * or RecnoPbuCancelUndo()            -- release without committing the record
 *
 * subtype is a RECNO_UNDO_* code; the payload is a RecnoUndoPayloadHeader
 * (recno_undo.h) followed by the before-image bytes (NULL/0 for INSERT).
 */
extern UndoRecPtr RecnoPbuPrepareInsert(uint16 subtype, Oid reloid,
										ItemPointer tid,
										const char *payload_hdr,
										uint32 payload_hdr_len,
										const char *image_bytes,
										uint32 image_len,
										char relpersistence);
extern void RecnoPbuInsertUndo(void);
extern void RecnoPbuRegisterUndoBuffers(uint8 first_block_id);
extern bool RecnoPbuUndoPending(void);
extern void RecnoPbuFinishUndo(XLogRecPtr lsn);
extern void RecnoPbuCancelUndo(void);

/*
 * REDO: re-run the per-backend undo insert folded onto a RECNO forward record,
 * rebuilding the undo page from its bytes (no forced full-page image).
 */
extern void RecnoPbuRedoUndo(void *record, uint8 first_block_id);
extern bool RecnoPbuBlockIsUndo(void *record, uint8 block_id);

/*
 * READ: fetch one RECNO UNDO record for the version-chain reader
 * (RecnoReconstructVisibleVersion).  Returns false if the record was
 * discarded/unreadable.  On success *out_subtype = the RECNO undo subtype,
 * *out_xid = the owning (top) xid that produced the candidate image, and
 * *out_payload = a palloc'd copy of the record payload (RecnoUndoPayloadHeader
 * + before-image bytes; caller frees), *out_payload_len its length.
 */
extern bool RecnoPbuFetchVersion(UndoRecPtr verptr, uint16 *out_subtype,
								 TransactionId *out_xid,
								 char **out_payload, uint32 *out_payload_len);

/*
 * READ: header-only fetch of the record's owning (top) xid at verptr.  Used by
 * the visibility path to decide whether a committed-but-UNCOMMITTED-flagged
 * in-place image's writer actually committed (vs a crash loser).  Returns
 * false if the record was discarded/unreadable.
 */
extern bool RecnoPbuFetchXid(UndoRecPtr verptr, TransactionId *out_xid);

/* Subtransaction (SAVEPOINT) rollback support. */
extern UndoRecPtr RecnoPbuGetCurrentUndoLatest(void);
extern UndoRecPtr RecnoPbuGetCurrentUndoStart(void);
extern void RecnoPbuApplySubxactUndo(UndoRecPtr from, UndoRecPtr stop_inclusive);
extern bool RecnoPbuInSubxactApply(void);

#endif							/* RECNO_PBU_H */
