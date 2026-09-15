/*-------------------------------------------------------------------------
 *
 * flux_xlog.h
 *	  FLUX table access method WAL definitions
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/flux_xlog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FLUX_XLOG_H
#define FLUX_XLOG_H

#include "postgres.h"

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/buf.h"
#include "storage/off.h"

/* Forward declarations */
typedef struct FluxTupleData *FluxTuple;
typedef enum FluxCompressionType FluxCompressionType;
typedef struct RelationData *Relation;
typedef struct FluxOverflowBuffers FluxOverflowBuffers;
struct RelUndoStageResult;

/*
 * Heap-format tuple image for logical decoding.
 *
 * Logical decoding consumes a heap-format image of the FLUX tuple that the
 * write path appends to the end of the main WAL data channel as
 * "[heap bytes][uint32 heap_len]" (see decode.c).  Forming that image calls
 * heap_form_tuple()/palloc(), which are forbidden inside a WAL critical
 * section.  Callers therefore prepare the image with
 * FluxXLogPrepareLogicalImage() BEFORE entering the critical section; the
 * WAL functions only register the prebuilt bytes (an allocation-free
 * operation) inside the section.  The image must stay alive until after
 * XLogInsert() returns; release it with FluxXLogReleaseLogicalImage() after
 * END_CRIT_SECTION().
 *
 * When the relation is not logically logged, prepare leaves data == NULL and
 * the WAL functions skip the append.
 */
typedef struct FluxLogicalImage
{
	char	   *data;			/* palloc'd heap tuple body, or NULL */
	uint32		len;			/* heap tuple t_len (also written trailing) */
} FluxLogicalImage;

/*
 * WAL record types for FLUX
 */
/*
 * WAL record types for FLUX.
 *
 * Each opcode must be unique.  The info byte uses bits 0-7 with
 * XLR_INFO_MASK occupying the upper bits, so we have the lower
 * nibble(s) available for opcodes.
 */
#define XLOG_FLUX_INSERT			0x00
#define XLOG_FLUX_UPDATE_INPLACE	0x10
#define XLOG_FLUX_DELETE			0x20
#define XLOG_FLUX_DEFRAG			0x30	/* single-page defrag */
#define XLOG_FLUX_OVERFLOW_WRITE	0x40
#define XLOG_FLUX_COMPRESS			0x50
#define XLOG_FLUX_INIT_PAGE		0x60
#define XLOG_FLUX_CROSS_PAGE_DEFRAG 0x70	/* cross-page tuple move */
#define XLOG_FLUX_VM_SET			0x80	/* Set visibility map bits */
#define XLOG_FLUX_VM_CLEAR			0x90	/* Clear visibility map bits */
#define XLOG_FLUX_LOCK				0xA0	/* Tuple lock */
#define XLOG_FLUX_CAS_UPDATE		0xB0	/* Same-size CAS in-place update */
#define XLOG_FLUX_WRITE_DICT		0xC0	/* Compression-dictionary fork
											 * write */
#define XLOG_FLUX_MULTI_INSERT		0xD0	/* Batched multi-tuple insert */
#define XLOG_FLUX_CAS_UPDATE_UNDO	0xE0	/* CAS update folded with UNDO
											 * before-image */
#define XLOG_FLUX_OPMASK			0xF0

/* Aliases for backward compatibility / clarity */
#define XLOG_FLUX_VACUUM			XLOG_FLUX_DEFRAG
#define XLOG_FLUX_UPDATE			XLOG_FLUX_UPDATE_INPLACE

/* Flags for xl_flux_overflow_write */
#define FLUX_OVERFLOW_WAL_NEW_RECORD	0x0000	/* New overflow record */
#define FLUX_OVERFLOW_WAL_LINK_UPDATE	0x0001	/* Link update only */

/*
 * Common WAL record flags.
 *
 * These appear in the 'flags' field of the DML WAL record structures
 * (xl_flux_insert, xl_flux_update, xl_flux_delete).
 *
 * Note: bit 0x0001 is reserved (formerly FLUX_WAL_HAS_HLC) and unused.
 */
#define FLUX_WAL_CROSS_PAGE	0x0002	/* Cross-page out-of-place update */
#define FLUX_WAL_HAS_OVERFLOW_BLK0	0x0004	/* Block 0 buf data has overflow
											 * records */
#define FLUX_WAL_PREFIX_SUFFIX	0x0008	/* Update uses prefix/suffix
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
 */
#define FLUX_WAL_LOGICAL_TUPLE	0x0010

#ifndef FRONTEND
/*
 * WAL record data structures
 */
typedef struct xl_flux_insert
{
	OffsetNumber offnum;		/* Offset number */
	uint16		flags;			/* Flags (FLUX_WAL_* bits) */
	uint32		tuple_len;		/* Length of tuple data that follows */
	uint64		commit_ts;		/* Commit timestamp */
	/* Tuple data follows */
} xl_flux_insert;

typedef struct xl_flux_update
{
	OffsetNumber offnum;		/* Offset number on source page (block 0) */
	uint16		flags;			/* Flags (FLUX_WAL_* bits) */
	uint64		old_commit_ts;	/* Old commit timestamp */
	uint64		new_commit_ts;	/* New commit timestamp */
	uint16		old_tuple_len;	/* Length of old tuple */
	uint16		new_tuple_len;	/* Length of new tuple data that follows */
	uint8		dst_block_id;	/* Block ID of destination page for cross-page
								 * updates (only valid when
								 * FLUX_WAL_CROSS_PAGE is set in flags) */
	uint8		pad[3];			/* Padding for alignment */
	/* New tuple data follows (old tuple data is in UNDO fork only) */
} xl_flux_update;

/*
 * Prefix/suffix compression header for in-place updates.
 *
 * When FLUX_WAL_PREFIX_SUFFIX is set in xl_flux_update.flags, this header
 * immediately follows the xl_flux_update struct and precedes the diff data.
 * Only the changed bytes (between prefixlen and len-suffixlen) are logged.
 *
 * The redo handler reconstructs the full new tuple by:
 *   1. Reading the existing tuple from the page (old data)
 *   2. Keeping old[0..prefixlen-1] as-is
 *   3. Copying the diff data into old[prefixlen..len-suffixlen-1]
 *   4. Keeping old[len-suffixlen..len-1] as-is
 */
typedef struct xl_flux_prefix_suffix
{
	uint16		prefixlen;		/* Bytes of common prefix */
	uint16		suffixlen;		/* Bytes of common suffix */
} xl_flux_prefix_suffix;

typedef struct xl_flux_delete
{
	OffsetNumber offnum;		/* Offset number */
	uint16		flags;			/* Flags (FLUX_WAL_* bits) */
	uint32		tuple_len;		/* Length of old tuple (for logical decoding) */
	uint64		commit_ts;		/* Commit timestamp */
	/* Old tuple data is in UNDO fork only */
} xl_flux_delete;

/*
 * WAL record for a batched multi-tuple insert (XLOG_FLUX_MULTI_INSERT).
 *
 * flux_multi_insert packs many tuples onto a single page in one critical
 * section.  Rather than emit one xl_flux_insert per tuple (and force a
 * full-page image to stay crash-safe), we log every tuple body once in a
 * single record modelled on heap's xl_heap_multi_insert.
 *
 * Layout of the record's main data:
 *
 *		xl_flux_multi_insert						-- header (ntuples, flags, ts)
 *		repeated ntuples times:
 *			xl_flux_multi_insert_tuple				-- per-tuple header
 *			char body[datalen]						-- tuple t_data bytes
 *
 * If flags & FLUX_WAL_LOGICAL_TUPLE, ntuples heap-format logical images
 * follow (one per tuple) after the per-tuple region, each as [body][uint32 len].
 */
typedef struct xl_flux_multi_insert
{
	uint16		ntuples;		/* Number of tuples in this batch */
	uint16		flags;			/* Flags (FLUX_WAL_* bits) */
	uint64		commit_ts;		/* Shared commit timestamp */
	/* xl_flux_multi_insert_tuple entries follow */
} xl_flux_multi_insert;

#define SizeOfFluxMultiInsert	sizeof(xl_flux_multi_insert)

typedef struct xl_flux_multi_insert_tuple
{
	OffsetNumber offnum;		/* Target offset on the page */
	uint16		datalen;		/* Length of tuple t_data bytes that follow */
	/* char body[datalen] follows */
} xl_flux_multi_insert_tuple;

#define SizeOfFluxMultiInsertTuple	sizeof(xl_flux_multi_insert_tuple)

typedef struct xl_flux_lock
{
	OffsetNumber offnum;		/* Offset number */
	uint16		flags;			/* Flags */
	uint8		infomask;		/* Infomask bits (uint8) */
	uint8		lock_mode;		/* LockTupleMode */
} xl_flux_lock;

/*
 * WAL record for same-size CAS in-place update (XLOG_FLUX_CAS_UPDATE).
 *
 * This is a lightweight record logged by the tuple-level CAS update fast
 * path.  Only the changed portion of the tuple data is logged (the region
 * between data_offset and data_offset+data_len within the on-page tuple).
 * The redo handler patches these bytes directly into the tuple on the page.
 */
typedef struct xl_flux_cas_update
{
	OffsetNumber offnum;		/* Tuple offset on page */
	uint16		flags;			/* FLUX_WAL_* bits */
	uint16		data_offset;	/* Byte offset within tuple for patch start */
	uint16		data_len;		/* Length of replacement data */
	uint64		new_commit_ts;	/* New commit timestamp for the tuple */
	/* char data[data_len] follows */
} xl_flux_cas_update;

/*
 * WAL record for a CAS in-place update folded with its UNDO before-image
 * (XLOG_FLUX_CAS_UPDATE_UNDO).
 *
 * This is the FOLD variant of the CAS fast path: instead of emitting a
 * separate RM_FLUX_ID CAS-update record and a separate RM_RELUNDO_ID
 * before-image record, one record carries both.  Block 0 is the main-fork
 * page (same redo byte-diff as xl_flux_cas_update).  Block 1 is the
 * relundo-fork data page (the UNDO record bytes, replayed exactly like
 * relundo_redo_insert).  Block 2 is the relundo metapage, present only when
 * the UNDO record started a fresh relundo page (is_new_page).
 *
 * The redo handler replays block 0 like flux_xlog_cas_update_redo, then
 * block 1 like relundo_redo_insert (honoring the new-page INIT and the
 * metapage FPI).
 */
typedef struct xl_flux_cas_update_undo
{
	/* --- redo half (block 0): mirrors xl_flux_cas_update --- */
	OffsetNumber offnum;		/* Tuple offset on main-fork page */
	uint16		flags;			/* FLUX_WAL_* bits */
	uint16		data_offset;	/* Byte offset within tuple for patch start */
	uint16		data_len;		/* Length of replacement data */
	uint64		new_commit_ts;	/* New commit timestamp for the tuple */

	/* --- undo half (block 1): mirrors xl_relundo_insert --- */
	uint8		urec_type;		/* UNDO record type */
	uint8		is_new_page;	/* first record on a freshly allocated page */
	uint16		urec_len;		/* UNDO record length */
	uint16		page_offset;	/* page-absolute offset of the UNDO record */
	uint16		new_pd_lower;	/* shadow pd_lower after the UNDO write */
	TransactionId max_xid;		/* undo-page max_xid watermark after the bump */
	/* char redo_data[data_len] follows (block 0 byte-diff) */
} xl_flux_cas_update_undo;

#define SizeOfFluxCasUpdateUndo	sizeof(xl_flux_cas_update_undo)

typedef struct xl_flux_defrag
{
	uint16		ntuples;		/* Number of tuples moved */
	uint64		commit_ts;		/* Commit timestamp */
	/* Array of offset mappings follows */
} xl_flux_defrag;

typedef struct xl_flux_overflow_write
{
	OffsetNumber offnum;		/* Offset of overflow record on page */
	uint16		flags;			/* Flags (0 = new record, 1 = link update) */
	uint32		data_len;		/* Length of overflow data chunk */
	uint64		commit_ts;		/* Commit timestamp */
	/* FluxOverflowRecordHeader + data follows for new records */
	/* FluxOverflowRecordHeader follows for link updates */
} xl_flux_overflow_write;

typedef struct xl_flux_compress
{
	OffsetNumber offnum;		/* Offset number */
	uint16		attr_num;		/* Attribute number */
	uint8		comp_type;		/* Compression type */
	uint8		comp_level;		/* Compression level */
	uint32		orig_size;		/* Original size */
	uint32		comp_size;		/* Compressed size */
	uint64		commit_ts;		/* Commit timestamp */
	/* Compressed data follows */
} xl_flux_compress;

typedef struct xl_flux_vacuum
{
	uint32		ntuples;		/* Number of removed tuples */
}			xl_flux_vacuum;

/*
 * Cross-page defragmentation: records moving a tuple from a source page
 * (block ref 1) to a target page (block ref 0).  The source line pointer
 * is set LP_UNUSED and the tuple data is added to the target page.
 *
 * If full-page images are present, recovery simply restores both pages.
 * Otherwise, recovery replays the move: adds the tuple to the target
 * and marks the source slot unused.
 */
typedef struct xl_flux_cross_page_defrag
{
	OffsetNumber src_offnum;	/* Source line pointer offset (on block 1) */
	OffsetNumber dst_offnum;	/* Target line pointer offset (on block 0) */
	uint32		tuple_len;		/* Length of moved tuple data */
	/* Tuple data follows */
} xl_flux_cross_page_defrag;

typedef struct xl_flux_init_page
{
	uint32		flags;			/* Page flags */
	uint64		commit_ts;		/* Initial commit timestamp */
} xl_flux_init_page;

/*
 * Compression-dictionary fork write (XLOG_FLUX_WRITE_DICT).
 *
 * The dictionary fork (FLUX_DICT_FORKNUM) stores trained ZSTD/LZ4 dictionary
 * blobs append-only.  Its pages use a non-standard layout (the directory
 * metapage keeps FluxDictMeta above pd_lower; data pages keep payload above
 * pd_lower), so the redo path cannot reconstruct them from logical deltas.
 * Instead each dirtied dict-fork page is logged as a full-page image with
 * REGBUF_FORCE_IMAGE and the redo handler simply restores the registered
 * block.  No record-specific payload is required beyond the block image.
 */
typedef struct xl_flux_write_dict
{
	BlockNumber blkno;			/* Dictionary-fork block being written */
} xl_flux_write_dict;

/*
 * Visibility Map WAL records
 */
typedef struct xl_flux_vm_set
{
	BlockNumber heapBlk;		/* Heap block number */
	uint8		flags;			/* VM flags being set */
} xl_flux_vm_set;

typedef struct xl_flux_vm_clear
{
	BlockNumber heapBlk;		/* Heap block number */
	uint8		flags;			/* VM flags being cleared */
} xl_flux_vm_clear;

/*
 * Offset mapping for defragmentation
 */
typedef struct FluxOffsetMapping
{
	OffsetNumber old_offnum;
	OffsetNumber new_offnum;
} FluxOffsetMapping;
#else							/* FRONTEND */

/* Frontend-safe versions of WAL record structures */
typedef struct xl_flux_insert
{
	uint16		offnum;			/* Offset number */
	uint16		flags;			/* Flags */
	uint32		tuple_len;		/* Length of tuple data */
	uint64		commit_ts;		/* Commit timestamp */
} xl_flux_insert;

typedef struct xl_flux_delete
{
	uint16		offnum;			/* Offset number */
	uint16		flags;			/* Flags */
	uint32		tuple_len;		/* Length of old tuple */
	uint64		commit_ts;		/* Commit timestamp */
} xl_flux_delete;

typedef struct xl_flux_multi_insert
{
	uint16		ntuples;		/* Number of tuples in this batch */
	uint16		flags;			/* Flags */
	uint64		commit_ts;		/* Shared commit timestamp */
} xl_flux_multi_insert;

typedef struct xl_flux_multi_insert_tuple
{
	uint16		offnum;			/* Target offset on the page */
	uint16		datalen;		/* Length of tuple data that follows */
} xl_flux_multi_insert_tuple;

typedef struct xl_flux_update
{
	uint16		offnum;			/* Offset number */
	uint16		flags;			/* Flags */
	uint64		old_commit_ts;	/* Old commit timestamp */
	uint64		new_commit_ts;	/* New commit timestamp */
	uint16		old_tuple_len;	/* Length of old tuple */
	uint16		new_tuple_len;	/* Length of new tuple */
} xl_flux_update;

typedef struct xl_flux_vacuum
{
	uint32		ntuples;		/* Number of removed tuples */
}			xl_flux_vacuum;

typedef struct xl_flux_compress
{
	uint16		offnum;			/* Offset number */
	uint16		attr_num;		/* Attribute number */
	uint8		comp_type;		/* Compression type */
	uint8		comp_level;		/* Compression level */
	uint32		orig_size;		/* Original size */
	uint32		comp_size;		/* Compressed size */
	uint64		commit_ts;		/* Commit timestamp */
} xl_flux_compress;

#endif							/* !FRONTEND */

/*
 * Function prototypes
 */

/* Frontend-safe function prototypes (pg_waldump, etc.) */
extern void flux_desc(StringInfo buf, XLogReaderState *record);
extern const char *flux_identify(uint8 info);

#ifndef FRONTEND
/* WAL replay and logging functions - backend only */
extern void flux_redo(XLogReaderState *record);
extern void flux_mask(char *page, BlockNumber blkno);

/*
 * Prepare/release the heap-format logical-decoding image.  Call prepare
 * BEFORE START_CRIT_SECTION() and release AFTER END_CRIT_SECTION().  When rel
 * is not logically logged, prepare sets img->data = NULL and the WAL
 * functions emit no logical image.
 */
extern void FluxXLogPrepareLogicalImage(Relation rel, FluxTuple rtup,
										FluxLogicalImage *img);
extern void FluxXLogReleaseLogicalImage(FluxLogicalImage *img);

extern XLogRecPtr FluxXLogInsert(Relation rel, Buffer buffer, OffsetNumber offnum,
								 FluxTuple tuple, uint64 commit_ts,
								 FluxOverflowBuffers *overflow_buffers,
								 FluxLogicalImage *logical_img,
								 bool force_page_image);
extern XLogRecPtr FluxXLogUpdate(Relation rel, Buffer buffer, OffsetNumber offnum,
								 FluxTuple old_tuple, FluxTuple new_tuple,
								 uint64 old_commit_ts, uint64 new_commit_ts,
								 FluxOverflowBuffers *overflow_buffers,
								 Buffer new_buffer,
								 FluxLogicalImage *old_img,
								 FluxLogicalImage *new_img);
extern XLogRecPtr FluxXLogDelete(Relation rel, Buffer buffer, OffsetNumber offnum,
								 FluxTuple tuple, uint64 commit_ts,
								 FluxLogicalImage *logical_img);

/*
 * Log a batch of tuples inserted onto a single page by flux_multi_insert.
 * Every tuple body is logged once (no forced full-page image), modelled on
 * heap's xl_heap_multi_insert.  offnums[i] is the on-page offset assigned to
 * tuples[i]; logical_imgs may be NULL (no logical decoding) or an array of
 * ntuples prepared images.
 */
extern XLogRecPtr FluxXLogMultiInsert(Relation rel, Buffer buffer,
									  OffsetNumber *offnums, FluxTuple *tuples,
									  int ntuples, uint64 commit_ts,
									  FluxLogicalImage *logical_imgs);

extern XLogRecPtr FluxXLogDefrag(Relation rel, Buffer buffer,
								 FluxOffsetMapping *mappings, int nmappings, uint64 commit_ts);
extern XLogRecPtr FluxXLogOverflowWrite(Relation rel, Buffer buffer,
										OffsetNumber offnum, char *record_data,
										uint32 record_len, uint16 flags,
										uint64 commit_ts);
extern XLogRecPtr FluxXLogCompress(Relation rel, Buffer buffer, OffsetNumber offnum,
								   uint16 attr_num, FluxCompressionType comp_type,
								   uint8 comp_level, char *comp_data, uint32 orig_size, uint32 comp_size,
								   uint64 commit_ts);
extern XLogRecPtr FluxXLogInitPage(Relation rel, Buffer buffer, uint32 flags, uint64 commit_ts);
extern XLogRecPtr FluxXLogCrossPageDefrag(Relation rel,
										  Buffer dst_buf, OffsetNumber dst_offnum,
										  Buffer src_buf, OffsetNumber src_offnum,
										  const void *tuple_data, uint32 tuple_len);
extern XLogRecPtr FluxXLogCasUpdate(Relation rel, Buffer buffer,
									OffsetNumber offnum,
									uint16 data_offset, uint16 data_len,
									const char *new_data,
									uint64 new_commit_ts);
extern XLogRecPtr FluxXLogCasUpdateUndo(Relation rel, Buffer buffer,
										OffsetNumber offnum,
										uint16 data_offset, uint16 data_len,
										const char *new_data,
										uint64 new_commit_ts,
										const struct RelUndoStageResult *undo);

/*
 * Log a compression-dictionary fork page as a full-page image so crash
 * recovery and physical replicas reproduce the append-only dict fork.  The
 * caller must hold the buffer locked and have already modified it inside a
 * critical section; this registers the page with REGBUF_FORCE_IMAGE and sets
 * the page LSN.
 */
extern XLogRecPtr FluxXLogWriteDict(Relation rel, Buffer buffer);

/*
 * Logical replication decode entry point for FLUX WAL records.
 */
struct LogicalDecodingContext;
struct XLogRecordBuffer;
extern void flux_decode(struct LogicalDecodingContext *ctx,
						struct XLogRecordBuffer *buf);
#endif							/* !FRONTEND */
#endif							/* FLUX_XLOG_H */
