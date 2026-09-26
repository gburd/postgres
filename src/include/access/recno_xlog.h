/*-------------------------------------------------------------------------
 *
 * recno_xlog.h
 *	  RECNO WAL record definitions and redo interface.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Author: Greg Burd <greg@burd.me>
 *
 * src/include/access/recno_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_XLOG_H
#define RECNO_XLOG_H

#include "postgres.h"

#include "access/recno.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/buf.h"
#include "storage/bufpage.h"
#include "storage/off.h"
#include "utils/rel.h"

/* WAL opcodes (occupy the info byte; RM_RECNO_ID). */
#define XLOG_RECNO_INSERT				0x10
#define XLOG_RECNO_UPDATE_INPLACE		0x20
#define XLOG_RECNO_DELETE				0x30
#define XLOG_RECNO_INIT_PAGE			0x40
#define XLOG_RECNO_DEFRAG				0x50
#define XLOG_RECNO_OVERFLOW_WRITE		0x60
#define XLOG_RECNO_COMPRESS				0x70
#define XLOG_RECNO_LOCK					0x80
#define XLOG_RECNO_VM_SET				0x90
#define XLOG_RECNO_VM_CLEAR				0xA0
#define XLOG_RECNO_CROSS_PAGE_DEFRAG	0xB0

#define XLOG_RECNO_OPMASK				0xF0

/* xlrec.flags bits */
#define RECNO_WAL_HAS_HLC				0x0001
#define RECNO_WAL_HAS_OVERFLOW_BLK0		0x0002
#define RECNO_WAL_PREFIX_SUFFIX			0x0004
#define RECNO_WAL_CROSS_PAGE			0x0008
#define RECNO_WAL_LOGICAL_TUPLE			0x0010
#define RECNO_WAL_SPEC_CONFIRM			0x0020	/* XLOG_RECNO_INSERT: confirm speculative tuple, do not add */

/* HLC uncertainty side-data appended when recno_use_hlc. */
typedef struct xl_recno_hlc_info
{
	uint64		commit_hlc;
	uint64		uncertainty_lower;
	uint64		uncertainty_upper;
} xl_recno_hlc_info;

#define SizeOfXlRecnoHlcInfo	(sizeof(xl_recno_hlc_info))

typedef struct xl_recno_insert
{
	OffsetNumber offnum;
	uint16		flags;
	uint64		commit_ts;
	/* full tuple image follows as main data */
} xl_recno_insert;

typedef struct xl_recno_update
{
	OffsetNumber offnum;
	uint16		flags;
	uint64		old_commit_ts;
	uint64		new_commit_ts;
	uint32		old_tuple_len;
	uint8		dst_block_id;	/* block id of dest page for cross-page */
	uint8		pad;
	/* new tuple image (or prefix/suffix + delta) follows as main data */
} xl_recno_update;

typedef struct xl_recno_prefix_suffix
{
	uint16		prefixlen;
	uint16		suffixlen;
} xl_recno_prefix_suffix;

typedef struct xl_recno_delete
{
	OffsetNumber offnum;
	uint16		flags;
	uint64		commit_ts;
} xl_recno_delete;

/*
 * XLOG_RECNO_DEFRAG
 *
 * A RECNO page defragmentation.  Before compacting, the primary retires the
 * line pointers of reclaimable dead tuples with ItemIdSetUnused(); only then
 * does it call PageRepairFragmentation().  Redo MUST retire exactly the same
 * line pointers before it compacts, otherwise it compacts a different set of
 * items and the replayed page diverges from the primary's (different lp_off
 * values, different tuple data, different pd_upper).  So the offsets retired
 * are recorded here and replayed verbatim.
 */
typedef struct xl_recno_defrag
{
	uint16		nunused;		/* number of line pointers retired */
	uint64		commit_ts;
	/* OffsetNumber[nunused] follows as main data */
} xl_recno_defrag;

typedef struct xl_recno_overflow_write
{
	OffsetNumber offnum;
	uint16		flags;
	uint32		data_len;
	uint64		commit_ts;
	/* overflow record bytes follow */
} xl_recno_overflow_write;

/*
 * xl_recno_undo -- the per-backend UNDO record's own description, attached as
 * block data on the first undo block of a RECNO forward-op WAL record.
 *
 * Carrying the undo record's bytes (this header + its uur_payload verbatim)
 * lets recno_redo re-run the undo insert (RecnoPbuRedoUndo) and rebuild the
 * undo page content, so the undo pages no longer need a forced full-page
 * image on every INSERT/UPDATE/DELETE.  That FPI dominated RECNO's UPDATE WAL
 * volume (two 8 KB undo-page images per in-place update).  Modeled on
 * xl_flux_undo.
 *
 *   urec_ptr    - the undo location DO-time PrepareUndoInsert() returned; redo
 *                 re-derives it and PANICs on mismatch.
 *   reloid      - the undo record's uur_reloid.
 *   subtype     - the RECNO_UNDO_* code (uur_type).
 *   payload_len - bytes of uur_payload that follow (RecnoUndoPayloadHeader plus
 *                 the before-image; carried verbatim so the replayed record is
 *                 byte-identical, preserving the target TID and the before-image
 *                 the version chain / rollback read back).
 */
typedef struct xl_recno_undo
{
	uint64		urec_ptr;		/* DO-time undo record location */
	Oid			reloid;			/* relation OID of the undo record */
	uint16		subtype;		/* RECNO_UNDO_* code */
	uint16		payload_len;	/* bytes of payload that follow */
	TransactionId top_xid;		/* TOP-level xid that owns the undo log.
								 * The undo log is attached per top-level
								 * transaction (XLOG_UNDOLOG_ATTACH carries the
								 * top xid), but a write performed inside a
								 * subtransaction tags its forward WAL record
								 * with the SUBXACT xid.  REDO must resolve the
								 * log by this top xid, not by
								 * XLogRecGetFullXid(), or the xid->logno lookup
								 * misses and recovery dies. */
	uint32		top_xid_epoch;	/* epoch of top_xid */
	/* char payload[payload_len] follows */
} xl_recno_undo;

#define SizeOfRecnoUndo		sizeof(xl_recno_undo)

typedef struct xl_recno_compress
{
	OffsetNumber offnum;
	uint16		attr_num;
	uint8		comp_type;
	uint8		comp_level;
	uint32		orig_size;
	uint32		comp_size;
	uint64		commit_ts;
	/* compressed bytes follow */
} xl_recno_compress;

typedef struct xl_recno_init_page
{
	uint32		flags;
	uint64		commit_ts;
} xl_recno_init_page;

typedef struct xl_recno_cross_page_defrag
{
	BlockNumber dst_block;
	OffsetNumber src_offnum;
	OffsetNumber dst_offnum;
	uint32		tuple_len;
	uint64		commit_ts;
	/* tuple bytes follow */
} xl_recno_cross_page_defrag;

typedef struct xl_recno_lock
{
	OffsetNumber offnum;
	uint16		flags;
	uint8		infomask;
	uint8		lock_mode;
} xl_recno_lock;

typedef struct xl_recno_vm_set
{
	BlockNumber heapBlk;
	uint8		flags;
} xl_recno_vm_set;

typedef struct xl_recno_vm_clear
{
	BlockNumber heapBlk;
	uint8		flags;
} xl_recno_vm_clear;

/* WAL emit routines (recno_xlog.c). */
extern XLogRecPtr RecnoXLogInsert(Relation rel, Buffer buffer,
								  OffsetNumber offnum, RecnoTuple tuple,
								  uint64 commit_ts,
								  RecnoOverflowBuffers *overflow_buffers);
extern XLogRecPtr RecnoXLogUpdate(Relation rel, Buffer buffer,
								  OffsetNumber offnum, RecnoTuple old_tuple,
								  RecnoTuple new_tuple, uint64 old_commit_ts,
								  uint64 new_commit_ts,
								  RecnoOverflowBuffers *overflow_buffers,
								  Buffer new_buffer);
extern XLogRecPtr RecnoXLogDelete(Relation rel, Buffer buffer,
								  OffsetNumber offnum, RecnoTuple tuple,
								  uint64 commit_ts);
extern XLogRecPtr RecnoXLogDefrag(Relation rel, Buffer buffer,
								  OffsetNumber *unused_offsets, int nunused,
								  uint64 commit_ts);
extern XLogRecPtr RecnoXLogOverflowWrite(Relation rel, Buffer buffer,
										 OffsetNumber offnum, char *record_data,
										 uint32 record_len, uint16 flags,
										 uint64 commit_ts);
extern XLogRecPtr RecnoXLogCompress(Relation rel, Buffer buffer,
									OffsetNumber offnum, uint16 attr_num,
									RecnoCompressionType comp_type,
									uint8 comp_level, char *comp_data,
									uint32 orig_size, uint32 comp_size,
									uint64 commit_ts);
extern XLogRecPtr RecnoXLogInitPage(Relation rel, Buffer buffer, uint32 flags,
									uint64 commit_ts);
extern XLogRecPtr RecnoXLogCrossPageDefrag(Relation rel, Buffer dst_buf,
										   OffsetNumber dst_offnum,
										   Buffer src_buf,
										   OffsetNumber src_offnum,
										   const void *tuple_data,
										   uint32 tuple_len);

extern bool RecnoFillHLCInfo(xl_recno_hlc_info *info);
extern XLogRecPtr RecnoXLogInsertHLC(Relation rel, Buffer buffer,
									 OffsetNumber offnum, RecnoTuple tuple,
									 uint64 commit_ts,
									 const xl_recno_hlc_info *hlc_info);
extern XLogRecPtr RecnoXLogUpdateHLC(Relation rel, Buffer buffer,
									 OffsetNumber offnum, RecnoTuple old_tuple,
									 RecnoTuple new_tuple, uint64 old_commit_ts,
									 uint64 new_commit_ts,
									 const xl_recno_hlc_info *hlc_info);
extern XLogRecPtr RecnoXLogDeleteHLC(Relation rel, Buffer buffer,
									 OffsetNumber offnum, RecnoTuple tuple,
									 uint64 commit_ts,
									 const xl_recno_hlc_info *hlc_info);

extern void RecnoReplicaHandleUncertainty(HLCTimestamp commit_hlc,
										  int32 uncertainty_ms);
extern void RecnoReplicaAdvanceHLC(HLCTimestamp target_hlc);

/* rmgr callbacks. */
extern void recno_redo(XLogReaderState *record);
extern void recno_mask(char *page, BlockNumber blkno);
extern void recno_desc(StringInfo buf, XLogReaderState *record);
extern const char *recno_identify(uint8 info);

#endif							/* RECNO_XLOG_H */
