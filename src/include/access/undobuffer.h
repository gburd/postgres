/*-------------------------------------------------------------------------
 *
 * undobuffer.h
 *	  AM-agnostic Tier 2 UNDO write buffer
 *
 * The Tier 2 buffer accumulates serialized UNDO records for the current DML
 * operation in a per-backend byte buffer.  At WAL-write time, the buffer
 * contents are embedded directly inside the AM's DML WAL record via
 * XLogRegisterData(), eliminating a separate XLOG_UNDO_BATCH record for
 * single-tuple operations.
 *
 * If the buffer grows beyond the configured threshold before the DML WAL
 * record is written, the overflow path flushes it as a standalone
 * XLOG_UNDO_BATCH record (preserving bulk-operation semantics).
 *
 * The buffer is per-backend; only one relation can be active at a time.
 * This matches the executor's single-ModifyTable-node pattern.
 *
 * Any access method (table or index) can use this buffer.  The UNDO record
 * header format (UndoRecordHeader) is AM-agnostic: each record carries an
 * RM ID (urec_rmid) that identifies the resource manager responsible for
 * interpreting and applying the record during rollback.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undobuffer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDOBUFFER_H
#define UNDOBUFFER_H

#include "access/xlogdefs.h"
#include "utils/relcache.h"

/*
 * UndoBufferBegin - Activate the Tier 2 UNDO buffer for a relation.
 *
 * Only one relation can have an active buffer at a time.  If the buffer is
 * already active for a different relation, the previous buffer is flushed
 * and deactivated before switching.
 *
 * 'nrows' is the planner's estimate (0 if unknown); reserved for future
 * pre-sizing but not used currently.
 */
extern void UndoBufferBegin(Relation rel, int64 nrows);

/*
 * UndoBufferEnd - Deactivate the Tier 2 UNDO buffer.
 *
 * Any records accumulated since the last flush or WAL embedding will be
 * flushed as an overflow batch.
 */
extern void UndoBufferEnd(Relation rel);

/*
 * UndoBufferAddRecord - Add an UNDO record to the Tier 2 buffer.
 *
 * Auto-flushes via the overflow path if size/count thresholds are exceeded.
 */
extern void UndoBufferAddRecord(Relation rel, uint8 rmid, uint16 info,
								const char *payload, Size payload_len);

/*
 * UndoBufferAddRecordParts - Add an UNDO record with scatter-gather payload.
 *
 * Avoids an intermediate buffer for operations where the payload is a
 * fixed header struct + variable-length data (e.g., index tuple).
 */
extern void UndoBufferAddRecordParts(Relation rel, uint8 rmid, uint16 info,
									 const char *part1, Size part1_len,
									 const char *part2, Size part2_len);

/*
 * UndoBufferFlush - Overflow flush: emit a standalone XLOG_UNDO_BATCH.
 *
 * Used when the buffer grows too large before the DML WAL record is written
 * (bulk operations), or at UndoBufferEnd time.
 */
extern void UndoBufferFlush(void);

/*
 * UndoBufferIsActive - Check if the Tier 2 buffer is active for a relation.
 */
extern bool UndoBufferIsActive(Relation rel);

/*
 * UndoBufferHasPendingData - Return true if the buffer has records to embed.
 */
extern bool UndoBufferHasPendingData(void);

/*
 * UndoBufferTakePayload - Hand off buffer contents to the caller.
 *
 * Called from the DML WAL section before XLogInsert().  The caller embeds
 * the returned data via XLogRegisterData() to carry UNDO inside the DML
 * WAL record.  After XLogInsert(), the caller must invoke UndoBufferReset()
 * to release ownership and update chain tracking.
 */
extern void UndoBufferTakePayload(char **data_out, Size *len_out,
								  int *nrecords_out,
								  XLogRecPtr *chain_prev_out);

/*
 * UndoBufferReset - Reset after the DML WAL record has been written.
 *
 * Updates chain_prev to the LSN of the WAL record that embedded the UNDO,
 * then clears len/nrecords so the buffer can accept new records.
 */
extern void UndoBufferReset(XLogRecPtr embedded_lsn);

#endif							/* UNDOBUFFER_H */
