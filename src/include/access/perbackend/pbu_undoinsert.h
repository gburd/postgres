/*-------------------------------------------------------------------------
 *
 * undoinsert.h
 *	  entry points for inserting undo records
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undoinsert.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PBU_UNDOINSERT_H
#define PBU_UNDOINSERT_H

#include "access/perbackend/pbu_undolog.h"
#include "access/perbackend/pbu_undorecord.h"
#include "access/xlogdefs.h"
#include "catalog/pg_class.h"

/*
 * Typedef for callback function for UndoFetchRecord.
 *
 * This checks whether an undorecord satisfies the given conditions.
 */
typedef bool (*SatisfyUndoRecordCallback) (UnpackedUndoRecord *urec,
										   BlockNumber blkno,
										   OffsetNumber offset,
										   TransactionId xid);

extern UndoRecPtr PrepareUndoInsert(UnpackedUndoRecord *, FullTransactionId xid,
									UndoPersistence, XLogReaderState *xlog_record,
									xl_undolog_meta *);

extern void InsertPreparedUndo(void);
extern void RegisterUndoLogBuffers(uint8 first_block_id);
extern int	RegisterUndoLogBuffersWithData(uint8 first_block_id,
										   const char *data, uint32 datalen);
extern void RegisterUndoLogBuffersForImage(uint8 first_block_id);
extern void UndoLogBuffersSetLSN(XLogRecPtr recptr);
extern void UnlockReleaseUndoBuffers(void);

extern UnpackedUndoRecord *UndoFetchRecord(UndoRecPtr urp,
										   BlockNumber blkno, OffsetNumber offset,
										   TransactionId xid, UndoRecPtr *urec_ptr_out,
										   SatisfyUndoRecordCallback callback);
extern void UndoRecordRelease(UnpackedUndoRecord *urec);
extern void UndoRecordSetPrevUndoLen(uint16 len);
extern void UndoSetPrepareSize(UnpackedUndoRecord *undorecords, int nrecords,
							   FullTransactionId fxid, UndoPersistence upersistence,
							   XLogReaderState *xlog_record, xl_undolog_meta *undometa);

extern UndoRecPtr UndoGetPrevUndoRecptr(UndoRecPtr urp, UndoRecPtr prevurp,
										Buffer *buffer);

extern void UndoRecordOnUndoLogChange(UndoPersistence persistence);

extern bool PrepareUpdateUndoActionProgress(XLogReaderState *xlog_record,
											UndoRecPtr urecptr, int progress);
extern void UndoRecordUpdateTransInfo(int idx);

extern void UndoGetOneRecord(UnpackedUndoRecord *urec, UndoRecPtr urp,
							 RelFileNode rnode, UndoPersistence persistence,
							 bool keep_buffer);
extern void ResetUndoBuffers(void);
extern bool UndoRecordIsValid(UndoRecPtr urp);

#endif							/* PBU_UNDOINSERT_H */
