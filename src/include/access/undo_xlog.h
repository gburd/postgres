/*-------------------------------------------------------------------------
 *
 * undo_xlog.h
 *	  UNDO resource manager WAL record definitions
 *
 * This file contains the WAL record format definitions for UNDO log
 * operations. These records are logged by the RM_UNDO_ID resource manager.
 *
 * Record types:
 *   XLOG_UNDO_BATCH          - A batch of a transaction's UNDO records
 *   XLOG_UNDO_INFLIGHT       - UNDO chains in flight at a checkpoint
 *   XLOG_UNDO_ROLLBACK       - A crash loser rolled back by recovery
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undo_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDO_XLOG_H
#define UNDO_XLOG_H

#include "access/transam.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"

/*
 * UndoRecPtr type definition.  We use undodefs.h which is lightweight
 * and can be included in both frontend and backend code.  If undodefs.h
 * has already been included (via undolog.h or directly), this is a no-op.
 */
#include "access/undodefs.h"

/*
 * WAL record types for UNDO operations
 *
 * These are the info codes for UNDO WAL records. The low 4 bits are used
 * for operation type, leaving the upper 4 bits for flags.
 */
#define XLOG_UNDO_BATCH				0x00	/* Batched UNDO records in WAL */
#define XLOG_UNDO_INFLIGHT			0x10	/* UNDO chains in flight at a
											 * checkpoint */
#define XLOG_UNDO_ROLLBACK			0x20	/* A crash loser rolled back by
											 * recovery */

/*
 * xl_undo_rollback - A crash loser that the end of recovery rolled back
 *
 * A transaction that a crash leaves in progress has no abort record.  At the
 * end of crash recovery, once WAL can be written again, the startup process
 * applies each such loser's UNDO records and then writes one of these,
 * carrying the records that applied, in the order it applied them (newest
 * first).  Redo applies them again, the same way, so that a standby, and a
 * later recovery that replays this record, end up where the primary did,
 * without reading the loser's UNDO chain from WAL.  Every UNDO resource
 * manager's apply function must therefore leave an object alone that no
 * longer shows the record's effect.
 *
 * The records follow the header as a packed stream of UndoRecordHeader and
 * payload, each urec_len bytes long, as in an XLOG_UNDO_BATCH record.
 */
typedef struct xl_undo_rollback
{
	TransactionId xid;			/* the loser */
	uint32		nrecords;		/* UNDO records that follow */
	/* followed by the UNDO records */
} xl_undo_rollback;

#define SizeOfUndoRollback	sizeof(xl_undo_rollback)

/* The records must start MAXALIGN'd, as they do in a batch. */
StaticAssertDecl(SizeOfUndoRollback % MAXIMUM_ALIGNOF == 0,
				 "xl_undo_rollback must keep the UNDO records after it aligned");

/*
 * xl_undo_batch - WAL record for batched UNDO data (XLOG_UNDO_BATCH)
 *
 * All UNDO records for a batch are serialized into a single WAL record.
 * The batch payload contains concatenated UndoRecordHeader+payload pairs
 * in their exact serialized format.
 *
 * The chain_prev field links this batch to the previous batch for the
 * same transaction.  During rollback, the UNDO chain is walked backward
 * by reading WAL records at successive chain_prev LSNs.
 *
 * Coalescing: The existing UndoRecordSet mechanism batches records
 * (flush at 256KB or 1000 records).  This batch becomes one WAL record.
 * A 1000-row INSERT produces ~1 WAL record containing 1000 UNDO records.
 */
typedef struct xl_undo_batch
{
	TransactionId xid;			/* Owning transaction */
	XLogRecPtr	chain_prev;		/* LSN of previous batch for this xact
								 * (InvalidXLogRecPtr if first batch) */
	uint32		nrecords;		/* Number of UNDO records in batch */
	uint32		total_len;		/* Total bytes of serialized UNDO data */
	Oid			primary_reloid; /* Relation OID (optimization for
								 * single-relation batches) */
	UndoPersistenceLevel persistence;	/* Persistence level of this batch */
	/* Followed by total_len bytes of serialized UndoRecordHeader+payload */
} xl_undo_batch;

#define SizeOfUndoBatch (offsetof(xl_undo_batch, persistence) + sizeof(UndoPersistenceLevel))

/*
 * xl_undo_inflight - UNDO chains of transactions in flight at a checkpoint
 *
 * Written by every online checkpoint after its redo pointer.  Crash recovery
 * starts redo there and so replays none of the batches written before it; a
 * transaction whose batches all precede the redo pointer is known to
 * recovery only through this record.
 */
typedef struct xl_undo_inflight_xact
{
	TransactionId xid;			/* top-level transaction */
	XLogRecPtr	last_batch_lsn[NUndoPersistenceLevels];
} xl_undo_inflight_xact;

typedef struct xl_undo_inflight
{
	uint32		nxacts;
	xl_undo_inflight_xact xacts[FLEXIBLE_ARRAY_MEMBER];
} xl_undo_inflight;

#define SizeOfUndoInflight offsetof(xl_undo_inflight, xacts)

/* Function declarations for WAL operations */
extern void undo_redo(XLogReaderState *record);
extern void undo_desc(StringInfo buf, XLogReaderState *record);
extern const char *undo_identify(uint8 info);

/*
 * UNDO batch reading from WAL for rollback and recovery.
 *
 * UndoReadBatchFromWAL reads a single XLOG_UNDO_BATCH record at the
 * given LSN and returns the header plus a pointer to the payload data.
 * The caller must pfree the returned data when done.
 */
typedef struct UndoBatchData
{
	xl_undo_batch header;		/* Batch header */
	char	   *payload;		/* Serialized UNDO records (palloc'd) */
	Size		payload_len;	/* Length of payload */
} UndoBatchData;

extern bool UndoValidateBatchLSN(XLogRecPtr batch_lsn);
extern UndoBatchData *UndoReadBatchFromWAL(XLogRecPtr batch_lsn);
extern void UndoFreeBatchData(UndoBatchData *batch);

/*
 * Recovery UNDO phase support.
 *
 * During WAL redo, XLOG_UNDO_BATCH and XLOG_UNDO_INFLIGHT records are
 * tracked, so that at the end of recovery the transactions a crash left in
 * progress can be found and rolled back (UndoRecoveryRollBack).
 */
extern void UndoRecoveryTrackBatch(TransactionId xid, XLogRecPtr batch_lsn,
								   XLogRecPtr chain_prev,
								   UndoPersistenceLevel persistence);
extern void UndoRecoveryRemoveXid(TransactionId xid);
extern void UndoRecoveryTrackInflight(const xl_undo_inflight_xact *x);
extern void UndoRecoveryRollBack(void);

/* True while recovery applies UNDO records outside redo; see there. */
extern PGDLLIMPORT bool UndoRecoveryRollingBack;

#endif							/* UNDO_XLOG_H */
