/*-------------------------------------------------------------------------
 *
 * recno_xlog.h
 *	  RECNO table access method WAL definitions
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/recno_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_XLOG_H
#define RECNO_XLOG_H

#include "postgres.h"

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/buf.h"
#include "storage/off.h"

/* Forward declarations */
typedef struct RecnoTupleData *RecnoTuple;
typedef enum RecnoCompressionType RecnoCompressionType;
typedef struct RelationData *Relation;
typedef struct RecnoOverflowBuffers RecnoOverflowBuffers;

/*
 * Heap-format tuple image for logical decoding.
 *
 * Logical decoding consumes a heap-format image of the RECNO tuple that the
 * write path appends to the end of the main WAL data channel as
 * "[heap bytes][uint32 heap_len]" (see decode.c).  Forming that image calls
 * heap_form_tuple()/palloc(), which are forbidden inside a WAL critical
 * section.  Callers therefore prepare the image with
 * RecnoXLogPrepareLogicalImage() BEFORE entering the critical section; the
 * WAL functions only register the prebuilt bytes (an allocation-free
 * operation) inside the section.  The image must stay alive until after
 * XLogInsert() returns; release it with RecnoXLogReleaseLogicalImage() after
 * END_CRIT_SECTION().
 *
 * When the relation is not logically logged, prepare leaves data == NULL and
 * the WAL functions skip the append.
 */
typedef struct RecnoLogicalImage
{
	char	   *data;			/* palloc'd heap tuple body, or NULL */
	uint32		len;			/* heap tuple t_len (also written trailing) */
} RecnoLogicalImage;

/*
 * WAL record types for RECNO
 */
/*
 * WAL record types for RECNO.
 *
 * Each opcode must be unique.  The info byte uses bits 0-7 with
 * XLR_INFO_MASK occupying the upper bits, so we have the lower
 * nibble(s) available for opcodes.
 */
#define XLOG_RECNO_INSERT			0x00
#define XLOG_RECNO_UPDATE_INPLACE	0x10
#define XLOG_RECNO_DELETE			0x20
#define XLOG_RECNO_DEFRAG			0x30	/* single-page defrag */
#define XLOG_RECNO_OVERFLOW_WRITE	0x40
#define XLOG_RECNO_COMPRESS			0x50
#define XLOG_RECNO_INIT_PAGE		0x60
#define XLOG_RECNO_CROSS_PAGE_DEFRAG 0x70	/* cross-page tuple move */
#define XLOG_RECNO_VM_SET			0x80	/* Set visibility map bits */
#define XLOG_RECNO_VM_CLEAR			0x90	/* Clear visibility map bits */
#define XLOG_RECNO_LOCK				0xA0	/* Tuple lock */
#define XLOG_RECNO_CAS_UPDATE		0xB0	/* Same-size CAS in-place update */
#define XLOG_RECNO_OPMASK			0xF0

/* Aliases for backward compatibility / clarity */
#define XLOG_RECNO_VACUUM			XLOG_RECNO_DEFRAG
#define XLOG_RECNO_UPDATE			XLOG_RECNO_UPDATE_INPLACE

/* Flags for xl_recno_overflow_write */
#define RECNO_OVERFLOW_WAL_NEW_RECORD	0x0000	/* New overflow record */
#define RECNO_OVERFLOW_WAL_LINK_UPDATE	0x0001	/* Link update only */

/*
 * Common WAL record flags.
 *
 * These appear in the 'flags' field of the DML WAL record structures
 * (xl_recno_insert, xl_recno_update, xl_recno_delete).
 */
#define RECNO_WAL_HAS_HLC		0x0001	/* HLC uncertainty info follows record */
#define RECNO_WAL_CROSS_PAGE	0x0002	/* Cross-page out-of-place update */
#define RECNO_WAL_HAS_OVERFLOW_BLK0	0x0004	/* Block 0 buf data has overflow
											 * records */
#define RECNO_WAL_PREFIX_SUFFIX	0x0008	/* Update uses prefix/suffix
										 * compression */
/*
 * Heap-format tuple image is appended to the WAL record for the benefit of
 * logical decoding.  Set when RelationIsLogicallyLogged(rel) at WAL-emit
 * time.  The layout of the appended region is:
 *
 *		uint32		logical_len				-- bytes of the heap-tuple payload
 *		bytes[logical_len]	HeapTuple t_data bytes
 *
 * For INSERT / DELETE the record contains exactly one heap-tuple payload.
 * For UPDATE it contains two back-to-back payloads (old, then new).
 * The payload is appended *after* any RECNO_WAL_HAS_HLC region so the
 * decoder only needs to know the flag order.
 */
#define RECNO_WAL_LOGICAL_TUPLE	0x0010

#ifndef FRONTEND
/*
 * HLC uncertainty information appended to WAL records when RECNO_WAL_HAS_HLC
 * is set.  This carries the full HLC timestamp and uncertainty interval for
 * use by logical replication subscribers and standby replicas.
 *
 * When a replica applies a WAL record containing this data, it can:
 *   1. Advance its local HLC to at least commit_hlc (causal consistency).
 *   2. Use the uncertainty interval to determine whether reads at the
 *      current time might see inconsistent ordering.
 *   3. Optionally wait until its local clock passes uncertainty_upper
 *      before serving reads at the committed timestamp.
 */
typedef struct xl_recno_hlc_info
{
	uint64		commit_hlc;		/* Commit HLC timestamp */
	uint64		uncertainty_lower;	/* Lower bound of uncertainty interval */
	uint64		uncertainty_upper;	/* Upper bound of uncertainty interval */
} xl_recno_hlc_info;

#define SizeOfXlRecnoHlcInfo	sizeof(xl_recno_hlc_info)
/*
 * WAL record data structures
 */
typedef struct xl_recno_insert
{
	OffsetNumber offnum;		/* Offset number */
	uint16		flags;			/* Flags (includes RECNO_WAL_HAS_HLC) */
	uint32		tuple_len;		/* Length of tuple data that follows */
	uint64		commit_ts;		/* Commit timestamp (HLC) */
	/* Tuple data follows */
	/* If flags & RECNO_WAL_HAS_HLC: xl_recno_hlc_info follows after tuple */
} xl_recno_insert;

typedef struct xl_recno_update
{
	OffsetNumber offnum;		/* Offset number on source page (block 0) */
	uint16		flags;			/* Flags (includes RECNO_WAL_HAS_HLC) */
	uint64		old_commit_ts;	/* Old commit timestamp */
	uint64		new_commit_ts;	/* New commit timestamp */
	uint16		old_tuple_len;	/* Length of old tuple */
	uint16		new_tuple_len;	/* Length of new tuple data that follows */
	uint8		dst_block_id;	/* Block ID of destination page for cross-page
								 * updates (only valid when
								 * RECNO_WAL_CROSS_PAGE is set in flags) */
	uint8		pad[3];			/* Padding for alignment */
	/* New tuple data follows (old tuple data is in UNDO fork only) */
	/* If flags & RECNO_WAL_HAS_HLC: xl_recno_hlc_info follows after tuple */
} xl_recno_update;

/*
 * Prefix/suffix compression header for in-place updates.
 *
 * When RECNO_WAL_PREFIX_SUFFIX is set in xl_recno_update.flags, this header
 * immediately follows the xl_recno_update struct and precedes the diff data.
 * Only the changed bytes (between prefixlen and len-suffixlen) are logged.
 *
 * The redo handler reconstructs the full new tuple by:
 *   1. Reading the existing tuple from the page (old data)
 *   2. Keeping old[0..prefixlen-1] as-is
 *   3. Copying the diff data into old[prefixlen..len-suffixlen-1]
 *   4. Keeping old[len-suffixlen..len-1] as-is
 */
typedef struct xl_recno_prefix_suffix
{
	uint16		prefixlen;		/* Bytes of common prefix */
	uint16		suffixlen;		/* Bytes of common suffix */
} xl_recno_prefix_suffix;

typedef struct xl_recno_delete
{
	OffsetNumber offnum;		/* Offset number */
	uint16		flags;			/* Flags (includes RECNO_WAL_HAS_HLC) */
	uint32		tuple_len;		/* Length of old tuple (for logical decoding) */
	uint64		commit_ts;		/* Commit timestamp (HLC) */
	/* Old tuple data is in UNDO fork only */
	/* If flags & RECNO_WAL_HAS_HLC: xl_recno_hlc_info follows */
} xl_recno_delete;

typedef struct xl_recno_lock
{
	OffsetNumber offnum;		/* Offset number */
	uint16		flags;			/* Flags */
	uint8		infomask;		/* Infomask bits (uint8) */
	uint8		lock_mode;		/* LockTupleMode */
} xl_recno_lock;

/*
 * WAL record for same-size CAS in-place update (XLOG_RECNO_CAS_UPDATE).
 *
 * This is a lightweight record logged by the tuple-level CAS update fast
 * path.  Only the changed portion of the tuple data is logged (the region
 * between data_offset and data_offset+data_len within the on-page tuple).
 * The redo handler patches these bytes directly into the tuple on the page.
 */
typedef struct xl_recno_cas_update
{
	OffsetNumber offnum;		/* Tuple offset on page */
	uint16		flags;			/* RECNO_WAL_HAS_HLC etc. */
	uint16		data_offset;	/* Byte offset within tuple for patch start */
	uint16		data_len;		/* Length of replacement data */
	uint64		new_commit_ts;	/* New commit timestamp for the tuple */
	/* char data[data_len] follows */
} xl_recno_cas_update;

typedef struct xl_recno_defrag
{
	uint16		ntuples;		/* Number of tuples moved */
	uint64		commit_ts;		/* Commit timestamp */
	/* Array of offset mappings follows */
} xl_recno_defrag;

typedef struct xl_recno_overflow_write
{
	OffsetNumber offnum;		/* Offset of overflow record on page */
	uint16		flags;			/* Flags (0 = new record, 1 = link update) */
	uint32		data_len;		/* Length of overflow data chunk */
	uint64		commit_ts;		/* Commit timestamp */
	/* RecnoOverflowRecordHeader + data follows for new records */
	/* RecnoOverflowRecordHeader follows for link updates */
} xl_recno_overflow_write;

typedef struct xl_recno_compress
{
	OffsetNumber offnum;		/* Offset number */
	uint16		attr_num;		/* Attribute number */
	uint8		comp_type;		/* Compression type */
	uint8		comp_level;		/* Compression level */
	uint32		orig_size;		/* Original size */
	uint32		comp_size;		/* Compressed size */
	uint64		commit_ts;		/* Commit timestamp */
	/* Compressed data follows */
} xl_recno_compress;

typedef struct xl_recno_vacuum
{
	uint32		ntuples;		/* Number of removed tuples */
} xl_recno_vacuum;

/*
 * Cross-page defragmentation: records moving a tuple from a source page
 * (block ref 1) to a target page (block ref 0).  The source line pointer
 * is set LP_UNUSED and the tuple data is added to the target page.
 *
 * If full-page images are present, recovery simply restores both pages.
 * Otherwise, recovery replays the move: adds the tuple to the target
 * and marks the source slot unused.
 */
typedef struct xl_recno_cross_page_defrag
{
	OffsetNumber src_offnum;	/* Source line pointer offset (on block 1) */
	OffsetNumber dst_offnum;	/* Target line pointer offset (on block 0) */
	uint32		tuple_len;		/* Length of moved tuple data */
	/* Tuple data follows */
} xl_recno_cross_page_defrag;

typedef struct xl_recno_init_page
{
	uint32		flags;			/* Page flags */
	uint64		commit_ts;		/* Initial commit timestamp */
} xl_recno_init_page;

/*
 * Visibility Map WAL records
 */
typedef struct xl_recno_vm_set
{
	BlockNumber heapBlk;		/* Heap block number */
	uint8		flags;			/* VM flags being set */
} xl_recno_vm_set;

typedef struct xl_recno_vm_clear
{
	BlockNumber heapBlk;		/* Heap block number */
	uint8		flags;			/* VM flags being cleared */
} xl_recno_vm_clear;

/*
 * Offset mapping for defragmentation
 */
typedef struct RecnoOffsetMapping
{
	OffsetNumber old_offnum;
	OffsetNumber new_offnum;
} RecnoOffsetMapping;
#else							/* FRONTEND */

/* Frontend-safe versions of WAL record structures */
typedef struct xl_recno_insert
{
	uint16		offnum;			/* Offset number */
	uint16		flags;			/* Flags */
	uint32		tuple_len;		/* Length of tuple data */
	uint64		commit_ts;		/* Commit timestamp */
} xl_recno_insert;

typedef struct xl_recno_delete
{
	uint16		offnum;			/* Offset number */
	uint16		flags;			/* Flags */
	uint32		tuple_len;		/* Length of old tuple */
	uint64		commit_ts;		/* Commit timestamp */
} xl_recno_delete;

typedef struct xl_recno_update
{
	uint16		offnum;			/* Offset number */
	uint16		flags;			/* Flags */
	uint64		old_commit_ts;	/* Old commit timestamp */
	uint64		new_commit_ts;	/* New commit timestamp */
	uint16		old_tuple_len;	/* Length of old tuple */
	uint16		new_tuple_len;	/* Length of new tuple */
} xl_recno_update;

typedef struct xl_recno_vacuum
{
	uint32		ntuples;		/* Number of removed tuples */
} xl_recno_vacuum;

typedef struct xl_recno_compress
{
	uint16		offnum;			/* Offset number */
	uint16		attr_num;		/* Attribute number */
	uint8		comp_type;		/* Compression type */
	uint8		comp_level;		/* Compression level */
	uint32		orig_size;		/* Original size */
	uint32		comp_size;		/* Compressed size */
	uint64		commit_ts;		/* Commit timestamp */
} xl_recno_compress;

#endif							/* !FRONTEND */

/*
 * Function prototypes
 */

/* Frontend-safe function prototypes (pg_waldump, etc.) */
extern void recno_desc(StringInfo buf, XLogReaderState *record);
extern const char *recno_identify(uint8 info);

#ifndef FRONTEND
/* WAL replay and logging functions - backend only */
extern void recno_redo(XLogReaderState *record);
extern void recno_mask(char *page, BlockNumber blkno);
/*
 * Prepare/release the heap-format logical-decoding image.  Call prepare
 * BEFORE START_CRIT_SECTION() and release AFTER END_CRIT_SECTION().  When rel
 * is not logically logged, prepare sets img->data = NULL and the WAL
 * functions emit no logical image.
 */
extern void RecnoXLogPrepareLogicalImage(Relation rel, RecnoTuple rtup,
										  RecnoLogicalImage *img);
extern void RecnoXLogReleaseLogicalImage(RecnoLogicalImage *img);

extern XLogRecPtr RecnoXLogInsert(Relation rel, Buffer buffer, OffsetNumber offnum,
								  RecnoTuple tuple, uint64 commit_ts,
								  RecnoOverflowBuffers *overflow_buffers,
								  RecnoLogicalImage *logical_img,
								  bool force_page_image);
extern XLogRecPtr RecnoXLogUpdate(Relation rel, Buffer buffer, OffsetNumber offnum,
								  RecnoTuple old_tuple, RecnoTuple new_tuple,
								  uint64 old_commit_ts, uint64 new_commit_ts,
								  RecnoOverflowBuffers *overflow_buffers,
								  Buffer new_buffer,
								  RecnoLogicalImage *old_img,
								  RecnoLogicalImage *new_img);
extern XLogRecPtr RecnoXLogDelete(Relation rel, Buffer buffer, OffsetNumber offnum,
								  RecnoTuple tuple, uint64 commit_ts,
								  RecnoLogicalImage *logical_img);

/*
 * HLC-aware WAL logging functions.
 *
 * These variants include HLC uncertainty information in the WAL record.
 * The hlc_info parameter may be NULL, in which case the record is written
 * without HLC data (equivalent to the non-HLC functions above).
 */
extern XLogRecPtr RecnoXLogInsertHLC(Relation rel, Buffer buffer,
									 OffsetNumber offnum, RecnoTuple tuple,
									 uint64 commit_ts,
									 const xl_recno_hlc_info *hlc_info);
extern XLogRecPtr RecnoXLogUpdateHLC(Relation rel, Buffer buffer,
									 OffsetNumber offnum,
									 RecnoTuple old_tuple, RecnoTuple new_tuple,
									 uint64 old_commit_ts, uint64 new_commit_ts,
									 const xl_recno_hlc_info *hlc_info);
extern XLogRecPtr RecnoXLogDeleteHLC(Relation rel, Buffer buffer,
									 OffsetNumber offnum, RecnoTuple tuple,
									 uint64 commit_ts,
									 const xl_recno_hlc_info *hlc_info);

extern XLogRecPtr RecnoXLogDefrag(Relation rel, Buffer buffer,
								  RecnoOffsetMapping *mappings, int nmappings, uint64 commit_ts);
extern XLogRecPtr RecnoXLogOverflowWrite(Relation rel, Buffer buffer,
										 OffsetNumber offnum, char *record_data,
										 uint32 record_len, uint16 flags,
										 uint64 commit_ts);
extern XLogRecPtr RecnoXLogCompress(Relation rel, Buffer buffer, OffsetNumber offnum,
									uint16 attr_num, RecnoCompressionType comp_type,
									uint8 comp_level, char *comp_data, uint32 orig_size, uint32 comp_size,
									uint64 commit_ts);
extern XLogRecPtr RecnoXLogInitPage(Relation rel, Buffer buffer, uint32 flags, uint64 commit_ts);
extern XLogRecPtr RecnoXLogCrossPageDefrag(Relation rel,
										   Buffer dst_buf, OffsetNumber dst_offnum,
										   Buffer src_buf, OffsetNumber src_offnum,
										   const void *tuple_data, uint32 tuple_len);
extern XLogRecPtr RecnoXLogCasUpdate(Relation rel, Buffer buffer,
									 OffsetNumber offnum,
									 uint16 data_offset, uint16 data_len,
									 const char *new_data,
									 uint64 new_commit_ts);

/*
 * Helper to fill in an xl_recno_hlc_info from current HLC state.
 * Populates commit_hlc and uncertainty bounds.
 * Returns false if HLC is not enabled (recno_use_hlc == false).
 */
extern bool RecnoFillHLCInfo(xl_recno_hlc_info *info);

/*
 * Locate the xl_recno_hlc_info within a RECNO WAL record's main data.
 *
 * When RECNO_WAL_LOGICAL_TUPLE is set, n_images self-delimiting tuple images
 * (each suffixed with a trailing uint32 length) are appended after the HLC
 * region; this walks backward past them to return the HLC struct.  Returns
 * NULL if the record carries no HLC info or the layout is malformed.
 */
extern const xl_recno_hlc_info *RecnoXLogLocateHLCInfo(const char *data,
													   Size total_len,
													   uint16 flags,
													   int n_images);

/*
 * Logical replication decode entry point for RECNO WAL records.
 */
struct LogicalDecodingContext;
struct XLogRecordBuffer;
extern void recno_decode(struct LogicalDecodingContext *ctx,
						 struct XLogRecordBuffer *buf);
#endif							/* !FRONTEND */
#endif							/* RECNO_XLOG_H */
