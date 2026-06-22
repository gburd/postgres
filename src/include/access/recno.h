/*-------------------------------------------------------------------------
 *
 * recno.h
 *	  RECNO table access method definitions
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/recno.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_H
#define RECNO_H

#include "postgres.h"

#include "access/heapam.h"
#include "access/relundo.h"
#include "access/rowid.h"
#include "storage/shmem.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "port/atomics.h"
#include "storage/buf.h"
#include "storage/bufpage.h"
#include "storage/procnumber.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/*
 * RECNO special space structure - stored in page special space (8 bytes)
 *
 * Packs the page-level commit timestamp and 3 flag bits into a single
 * uint64.  The top 3 bits (63-61) store page flags; the lower 61 bits
 * store the commit timestamp (sufficient for 73,000+ years of microseconds).
 *
 * pd_free_space was removed -- use PageGetFreeSpace() directly (same as
 * heap).  pd_flags was removed -- flags are packed into the timestamp word.
 */
typedef struct RecnoPageOpaqueData
{
	uint64		pd_commit_ts_and_flags; /* bits 63-61: flags, bits 60-0: ts */
} RecnoPageOpaqueData;

typedef RecnoPageOpaqueData *RecnoPageOpaque;

/* Page flags (stored in top 3 bits of pd_commit_ts_and_flags) */
#define RECNO_PAGE_FLAG_SHIFT		61
#define RECNO_PAGE_FLAG_MASK		(UINT64CONST(0x7) << RECNO_PAGE_FLAG_SHIFT)
#define RECNO_PAGE_TS_MASK			(~RECNO_PAGE_FLAG_MASK)

#define RECNO_PAGE_OVERFLOW			(UINT64CONST(1) << 61)
#define RECNO_PAGE_DEFRAG_NEEDED	(UINT64CONST(1) << 62)
#define RECNO_PAGE_FULL				(UINT64CONST(1) << 63)

/* Accessor macros for page opaque */
#define RecnoPageGetOpaque(page) \
	((RecnoPageOpaque) PageGetSpecialPointer(page))

#define RecnoPageGetCommitTs(opaque) \
	((opaque)->pd_commit_ts_and_flags & RECNO_PAGE_TS_MASK)

#define RecnoPageSetCommitTs(opaque, ts) \
	((opaque)->pd_commit_ts_and_flags = \
		((opaque)->pd_commit_ts_and_flags & RECNO_PAGE_FLAG_MASK) | \
		((uint64)(ts) & RECNO_PAGE_TS_MASK))

#define RecnoPageGetFlags(opaque) \
	((opaque)->pd_commit_ts_and_flags & RECNO_PAGE_FLAG_MASK)

#define RecnoPageSetFlag(opaque, flag) \
	((opaque)->pd_commit_ts_and_flags |= (flag))

#define RecnoPageClearFlag(opaque, flag) \
	((opaque)->pd_commit_ts_and_flags &= ~(flag))

/*
 * RECNO tuple header structure (v3 -- heap-compatible xmin/xmax MVCC)
 *
 * Visibility is ordinary heap-shaped xmin/xmax + CLOG + snapshot (the same
 * model HeapTupleSatisfiesMVCC uses), NOT the former HLC/sLog timestamp
 * scheme.  A tuple is visible to a snapshot iff its inserter (t_xmin) is
 * committed-and-visible-to-the-snapshot AND its deleter/updater (t_xmax) is
 * either invalid, not committed, or not visible to the snapshot.
 *
 *   - t_xmin: the inserting transaction's XID.  Always valid once the tuple
 *     exists on the page (it rides through WAL redo verbatim in the logged
 *     tuple body, so it needs no separate redo reconstruction).
 *   - t_xmax: the deleting/updating transaction's XID, or InvalidTransactionId
 *     for a live never-superseded tuple.  Physically stored in the low 32
 *     bits of the t_commit_ts word (see RecnoTupleGetXmax below) so the on-disk
 *     and WAL byte layout is unchanged from the HLC era; the DML paths and
 *     WAL now carry an XID in that slot instead of a timestamp.
 *
 * In-place UPDATE keeps the newest version on the page (new t_xmin) and pushes
 * the pre-update image to the per-relation UNDO fork via t_verptr (zheap
 * style).  A snapshot that predates the update reads the old version back from
 * the fork with RecnoReconstructVisibleVersion(), which walks t_verptr and
 * stops at the version whose producing xid is visible (XidInMVCCSnapshot).
 *
 * Transient operation state (who is inserting/deleting/locking concurrently)
 * is still tracked in the sLog for write-write conflict serialization and SSI,
 * but it is no longer consulted for read visibility -- CLOG is authoritative.
 *
 * t_writer: Per-tuple CAS writer lock for same-size updates under
 * BUFFER_LOCK_SHARE.  0 = unlocked; non-zero = (MyProcNumber + 1) of
 * the writer.  Operated on via atomic CAS through RecnoTupleWriter*
 * macros below.  It sits in the fixed header (see the field list and the
 * 40-byte total documented on the struct below); it is never read on the
 * visibility path, only CAS'd during an in-flight same-size update.
 */
typedef struct RecnoTupleHeader
{
	uint64		t_commit_ts;	/* 8B  low 32 bits = t_xmax (deleter/updater XID);
								 * high 32 bits reserved.  Accessed via
								 * RecnoTupleGetXmax/SetXmax.  Kept as a uint64
								 * field so the on-disk/WAL byte layout is
								 * unchanged from the HLC era. */
	RelUndoRecPtr t_verptr;		/* 8B  UNDO-fork version-chain head (WS-PVS1) */
	uint32		t_writer;		/* 4B  Per-tuple CAS writer lock (0=free) */

	/*
	 * t_gen: per-tuple index-identity generation.  Incremented on every
	 * in-place UPDATE that changes an indexed column, so that A->B->A
	 * recurrences of an indexed key value on this same physical tuple produce
	 * DISTINCT index RowIDs ((TID, gen)) -- the index never holds two
	 * identical (key, TID) entries, which nbtree's strict (key, rowid) order
	 * forbids.  The tuple stays physically in place across every UPDATE; only
	 * this counter and the index entries change.  t_gen is a real 4-byte
	 * header field (counted in the 40-byte total: 8+8+4+4+4+6+2+2+1+1 = 40);
	 * it is written into the on-disk tuple and the WAL-logged tuple body like
	 * any other header field.  Wraps at 2^32; VACUUM reclaims superseded
	 * old-gen index entries long before wraparound is a concern.
	 */
	uint32		t_gen;			/* 4B  index-identity generation */

	/*
	 * t_xmin: the inserting transaction's XID (heap xmin semantics).  Always
	 * valid once the tuple exists.  Visibility resolves it against CLOG and
	 * the reader's snapshot exactly like HeapTupleSatisfiesMVCC, so no HLC
	 * timestamp or sLog lookup is needed on the common read path.  The tuple
	 * body (including this field) is WAL-logged verbatim, so t_xmin survives
	 * redo without separate reconstruction.
	 *
	 * (Formerly t_xid_hint, which was "valid only while UNCOMMITTED"; the
	 * heap-shaped model makes it permanently authoritative as xmin.)
	 */
	TransactionId t_xmin;		/* 4B  Inserter XID (heap xmin) */

	/*
	 * t_verptr is the head of this tuple's persistent version chain in the
	 * UNDO fork (WS-PVS1/2).  It was formerly an unaligned 8-byte trailer at
	 * the end of the on-page item, located by item_len - 8 arithmetic and
	 * gated by RECNO_TUPLE_HAS_VERSION_PTR.  Since every tuple now reserves it
	 * from birth (born-with-flag), the trailer was always present, so it is a
	 * plain aligned header field: no growth on first UPDATE, no packed-page
	 * failure, no trailing-byte arithmetic.  InvalidRelUndoRecPtr means "never
	 * updated -- no history"; readers treat that as "on-page image is current".
	 *
	 * t_cid removed: command ID is now obtained from the sLog entry
	 * (RecnoSLogEntry.cid) when RECNO_TUPLE_UNCOMMITTED is set. This saves 4
	 * bytes per tuple. The sLog lookup is mandatory for uncommitted
	 * DELETE/UPDATE visibility anyway, so fetching the cid from there adds
	 * zero extra overhead.  (INSERT visibility no longer needs the sLog at
	 * all in the common case -- see t_xmin above.)
	 */
	ItemPointerData t_ctid;		/* 6B  Current TID / update chain */
	uint16		t_natts;		/* 2B  Number of attributes */
	uint16		t_flags;		/* 2B  Tuple flags */
	uint8		t_infomask;		/* 1B  HASNULL, HASVARWIDTH, etc. */
	uint8		t_pad;			/* 1B  padding so t_attrs_bitmap begins at a
								 * MAXALIGN boundary equal to RECNO_TUPLE_OVERHEAD.
								 * Without this the null bitmap (anchored at
								 * t_attrs_bitmap) and the column data (anchored at
								 * RECNO_TUPLE_OVERHEAD) disagree, corrupting reads.
								 * The whole header is still MAXALIGN'd to 40 bytes
								 * (8+8+4+4+4+6+2+2+1+1 = 40), so no footprint change. */
	uint8		t_attrs_bitmap[FLEXIBLE_ARRAY_MEMBER];
	/* Optional: RecnoInlineDiff after bitmap if HAS_INLINE_DIFF set */
} RecnoTupleHeader;

/* Fixed size: 40 bytes raw (MAXALIGN'd to 40 bytes): 8 t_commit_ts + 8 t_verptr
 * + 4 t_writer + 4 t_gen + 4 t_xmin + 6 t_ctid + 2 t_natts + 2 t_flags
 * + 1 t_infomask + 1 t_pad = 40, then the FLEXIBLE_ARRAY_MEMBER null bitmap. */

/*
 * Per-tuple CAS writer lock accessor macros.
 *
 * t_writer is a plain uint32 on disk (initialized to 0 by palloc0/memset).
 * At runtime we operate on it via pg_atomic_compare_exchange_u32 by casting
 * its address to (pg_atomic_uint32 *).  This is safe on all PostgreSQL
 * platforms because pg_atomic_uint32 is { volatile uint32 value; } with
 * identical size and alignment.
 *
 * RecnoTupleWriterTryLock: CAS 0 -> (MyProcNumber+1).  Returns true on success.
 * RecnoTupleWriterUnlock:  Atomic write 0 (release).
 * RecnoTupleWriterIsLocked: Non-zero check (relaxed read).
 */
#define RecnoTupleWriterTryLock(hdr, expected_ptr) \
	pg_atomic_compare_exchange_u32((pg_atomic_uint32 *) &(hdr)->t_writer, \
								   (expected_ptr), (uint32)(MyProcNumber + 1))

#define RecnoTupleWriterUnlock(hdr) \
	pg_atomic_write_u32((pg_atomic_uint32 *) &(hdr)->t_writer, 0)

#define RecnoTupleWriterIsLocked(hdr) \
	(pg_atomic_read_u32((pg_atomic_uint32 *) &(hdr)->t_writer) != 0)

/* Tuple flags (uint16) */
#define RECNO_TUPLE_COMPRESSED		0x0001
#define RECNO_TUPLE_HAS_OVERFLOW	0x0002
#define RECNO_TUPLE_DELETED			0x0004
#define RECNO_TUPLE_UPDATED			0x0008
#define RECNO_TUPLE_LOCKED			0x0010
#define RECNO_TUPLE_SPECULATIVE		0x0020
#define RECNO_TUPLE_HAS_INLINE_DIFF	0x0040	/* InlineDiff follows bitmap */
#define RECNO_TUPLE_UNCOMMITTED		0x0080	/* Inserted but not yet committed */
#define RECNO_TUPLE_HAS_VERSION_PTR	0x0100	/* trailing RelUndoRecPtr (version-chain head) follows column data */
#define RECNO_TUPLE_XMIN_COMMITTED	0x0200	/* t_xmin known-committed (CLOG hint, like heap HEAP_XMIN_COMMITTED) */
#define RECNO_TUPLE_XMAX_COMMITTED	0x0400	/* t_xmax known-committed (CLOG hint, like heap HEAP_XMAX_COMMITTED) */

/* Tuple infomask bits (uint8 -- reduced from uint16) */
#define RECNO_INFOMASK_HASNULL		0x01
#define RECNO_INFOMASK_HASVARWIDTH	0x02
#define RECNO_INFOMASK_HASEXTERNAL	0x04
#define RECNO_INFOMASK_COMPRESSED	0x08
#define RECNO_INFOMASK_HASOVERFLOW	0x10

/*
 * RECNO tuple structure
 */
typedef struct RecnoTupleData
{
	uint32		t_len;			/* Length of tuple */
	ItemPointerData t_self;		/* TID of this tuple */
	Oid			t_tableOid;		/* Table OID */
	RecnoTupleHeader *t_data;	/* Tuple header and data */
} RecnoTupleData;

typedef RecnoTupleData *RecnoTuple;

/*
 * Column-level overflow
 *
 * When an individual column value is too large to store inline in the main
 * tuple, it is stored as one or more "overflow records" on normal RECNO data
 * pages.  The main tuple stores a compact overflow pointer (RecnoOverflowPtr)
 * wrapped in a varlena, optionally preceded by an inline prefix of the
 * original data for efficient prefix matching.
 *
 * Overflow records use a lightweight header (RecnoOverflowRecordHeader)
 * without MVCC fields -- they share the visibility of the parent tuple.
 * Each overflow record holds a chunk of the column data and a continuation
 * pointer to the next chunk (or InvalidBlockNumber if this is the last).
 *
 * This approach stores overflow data on regular pages that can also hold
 * normal tuples, within the same relation -- no separate out-of-line
 * relation is created.
 */

/*
 * Overflow pointer stored inline in the main tuple (wrapped as varlena).
 *
 * On-disk layout of an overflowed column in the main tuple:
 *   [varlena header][RecnoOverflowPtr][inline_prefix_bytes...]
 *
 * The RECNO_OVERFLOW_PTR_MAGIC sentinel distinguishes this from a normal
 * varlena value during deform.
 */
#define RECNO_OVERFLOW_PTR_MAGIC	0x52564F50	/* "RVOP" */

typedef struct RecnoOverflowPtr
{
	uint32		ov_magic;		/* RECNO_OVERFLOW_PTR_MAGIC */
	BlockNumber ov_first_block; /* First overflow record's page */
	OffsetNumber ov_first_offset;	/* First overflow record's offset on page */
	uint16		ov_inline_prefix;	/* Bytes of inline prefix stored after ptr */
	uint32		ov_total_length;	/* Total uncompressed column data length */
	uint32		ov_content_hash;	/* 32-bit content-hash prefilter for COW */
} RecnoOverflowPtr;

/*
 * ov_content_hash reclaims the two formerly-reserved uint16 fields
 * (ov_padding, ov_flags), so the struct stays 20 bytes and
 * RECNO_OVERFLOW_PTR_SIZE is unchanged.  A wider (64-bit) hash would force
 * struct growth that inflates RECNO_OVERFLOW_PTR_SIZE and defeats the
 * force-shrink UPDATE recovery path, breaking in-place updates on full pages.
 *
 * The hash is only a cheap prefilter: every COW-reference candidate is
 * byte-verified against the fetched old chain before it is accepted, so a
 * 32-bit collision merely costs a wasted fetch and falls through to a normal
 * re-store.  It never produces an incorrect result.
 */

/* Minimum varlena size for an overflow pointer (no inline prefix) */
#define RECNO_OVERFLOW_PTR_SIZE		(VARHDRSZ + sizeof(RecnoOverflowPtr))

/* Default inline prefix size (configurable via GUC) */
#define RECNO_OVERFLOW_DEFAULT_PREFIX	128

/*
 * Check if a varlena datum is an overflow pointer.
 *
 * The check requires: correct size range, and magic value match.
 */
static inline bool
RecnoIsOverflowPtr(const void *ptr)
{
	Size		vsize;
	const RecnoOverflowPtr *ovp;

	if (ptr == NULL)
		return false;

	vsize = VARSIZE_ANY_EXHDR(ptr);
	if (vsize < sizeof(RecnoOverflowPtr))
		return false;

	ovp = (const RecnoOverflowPtr *) VARDATA_ANY(ptr);
	return ovp->ov_magic == RECNO_OVERFLOW_PTR_MAGIC;
}

/*
 * Extract overflow pointer from a varlena datum.
 */
static inline const RecnoOverflowPtr *
RecnoGetOverflowPtr(const void *ptr)
{
	return (const RecnoOverflowPtr *) VARDATA_ANY(ptr);
}

/*
 * Lightweight header for overflow records stored on normal data pages.
 *
 * Overflow records are stored via PageAddItem just like normal tuples, but
 * they carry this minimal header instead of a full RecnoTupleHeader.  The
 * ov_magic field lets us distinguish overflow records from normal tuples
 * during page scans (e.g., sequential scan must skip these).
 */
#define RECNO_OVERFLOW_RECORD_MAGIC		0x524F5643	/* "ROVC" */

typedef struct RecnoOverflowRecordHeader
{
	uint32		or_magic;		/* RECNO_OVERFLOW_RECORD_MAGIC */
	uint32		or_data_len;	/* Bytes of column data in this record */
	BlockNumber or_next_block;	/* Next overflow record's page, or Invalid */
	OffsetNumber or_next_offset;	/* Next overflow record's offset */
	uint16		or_flags;		/* Flags (reserved) */
	/* Column data follows immediately after this header */
} RecnoOverflowRecordHeader;

/* Maximum column data per overflow record */
#define RECNO_OVERFLOW_RECORD_OVERHEAD	MAXALIGN(sizeof(RecnoOverflowRecordHeader))
#define RECNO_OVERFLOW_MAX_CHUNK_SIZE \
	(RECNO_MAX_TUPLE_SIZE - RECNO_OVERFLOW_RECORD_OVERHEAD)

/*
 * Structure to track overflow buffers for atomic WAL logging.
 *
 * When creating overflow chains, we keep buffers pinned and collect them
 * here so the caller can register them all in a single WAL record with
 * the main tuple modification. This ensures atomicity during crash recovery.
 */
#define MAX_OVERFLOW_BUFFERS 32

typedef struct RecnoOverflowBuffer
{
	Buffer		buffer;			/* Pinned buffer containing overflow record */
	OffsetNumber offset;		/* Offset of overflow record on page */
	char	   *record_data;	/* RecnoOverflowRecordHeader + data */
	uint32		record_len;		/* Total record length */
	uint16		flags;			/* RECNO_OVERFLOW_WAL_NEW_RECORD or
								 * _LINK_UPDATE */
} RecnoOverflowBuffer;

typedef struct RecnoOverflowBuffers
{
	int			count;			/* Number of overflow buffers */
	RecnoOverflowBuffer buffers[MAX_OVERFLOW_BUFFERS];
} RecnoOverflowBuffers;

/*
 * Compression types
 */
typedef enum RecnoCompressionType
{
	RECNO_COMP_NONE,
	RECNO_COMP_LZ4,
	RECNO_COMP_ZSTD,
	RECNO_COMP_DELTA,			/* For numeric columns */
	RECNO_COMP_DICTIONARY		/* For text columns */
} RecnoCompressionType;

/*
 * Values for the recno_compression_algorithm GUC.  AUTO lets
 * RecnoChooseCompressionType() pick per attribute type; LZ4/ZSTD force that
 * codec for compressible varlena attributes; NONE disables compression.
 */
typedef enum RecnoCompressionAlgoGuc
{
	RECNO_COMP_ALGO_AUTO,
	RECNO_COMP_ALGO_LZ4,
	RECNO_COMP_ALGO_ZSTD,
	RECNO_COMP_ALGO_OFF
} RecnoCompressionAlgoGuc;

typedef struct RecnoCompressionHeader
{
	uint8		comp_type;
	uint8		comp_level;
	uint16		dict_id;		/* trained-dict id, 0 = RECNO_DICT_INVALID_ID */
	uint32		orig_size;
	uint32		comp_size;
} RecnoCompressionHeader;

/*
 * RECNO timestamp word.
 *
 * A uint64 wall-clock timestamp (microseconds since the PG epoch) used for
 * per-page commit-ts bookkeeping (RecnoPageSetCommitTs).  It is NOT a
 * visibility timestamp: commit visibility comes from CLOG via heap-shaped
 * xmin/xmax MVCC.
 */

/*
 * Tuple MVCC field accessors (heap-shaped xmin/xmax).
 *
 * t_xmax is physically the low 32 bits of the t_commit_ts word; the high 32
 * bits are reserved.  This preserves the on-disk/WAL byte layout while giving
 * the tuple a heap-compatible deleter/updater XID.  InvalidTransactionId (0)
 * means "live, never superseded".
 */
#define RecnoTupleGetXmax(tup)		((TransactionId) ((tup)->t_commit_ts & 0xFFFFFFFFULL))
#define RecnoTupleSetXmax(tup, xid) \
	((tup)->t_commit_ts = ((tup)->t_commit_ts & 0xFFFFFFFF00000000ULL) | \
						   ((uint64) (TransactionId) (xid)))
#define RecnoTupleGetXmin(tup)		((tup)->t_xmin)
#define RecnoTupleSetXmin(tup, xid)	((tup)->t_xmin = (TransactionId) (xid))

/*
 * Transaction state for uncertainty tracking.
 *
 * The full struct definition lives in recno_mvcc.c (private to that module).
 * External code should use the opaque forward declaration below.
 */
typedef struct RecnoTransactionState RecnoTransactionState;

/*
 * Free space management
 */
typedef struct RecnoFreeSpaceMap
{
	uint32		total_pages;
	uint32		pages_with_space;
	uint8	   *fsm_data;		/* Bitmap of page utilization */
	uint32	   *defrag_queue;	/* Pages needing defragmentation */
	uint32		defrag_queue_size;
} RecnoFreeSpaceMap;

/* Free space map levels */
#define RECNO_FSM_FULL			0
#define RECNO_FSM_75_PERCENT	1
#define RECNO_FSM_50_PERCENT	2
#define RECNO_FSM_25_PERCENT	3
#define RECNO_FSM_EMPTY			4

/*
 * Visibility Map support for RECNO
 *
 * The visibility map tracks two bits per page:
 * - ALL_VISIBLE: all tuples on page are visible to all transactions
 * - ALL_FROZEN: all tuples on page are frozen (no further VACUUM needed)
 *
 * This enables:
 * - Index-only scans (can skip heap fetch if page is all-visible)
 * - VACUUM optimization (can skip pages marked all-visible/frozen)
 */

/* Visibility map bits */
#define RECNO_VM_ALL_VISIBLE		0x01	/* All tuples visible to all xacts */
#define RECNO_VM_ALL_FROZEN			0x02	/* All tuples frozen */

/* Combined flags for convenience */
#define RECNO_VM_VALID_BITS			(RECNO_VM_ALL_VISIBLE | RECNO_VM_ALL_FROZEN)

/* Visibility map fork number (uses PostgreSQL's fork infrastructure) */
#define RECNO_VM_FORKNUM			VISIBILITYMAP_FORKNUM

/*
 * Scan descriptor for RECNO scans
 */
typedef struct RecnoScanDescData
{
	TableScanDescData rs_base;	/* Base scan descriptor */
	Buffer		rs_cbuf;		/* Current buffer */
	BlockNumber rs_cblock;		/* Current block */
	BlockNumber rs_nblocks;		/* Total blocks in relation (cached) */
	BlockNumber rs_startblock;	/* Starting block for sample scans */
	OffsetNumber rs_cindex;		/* Current offset in page */
	OffsetNumber rs_coffset;	/* Current offset number */
	bool		rs_inited;		/* True after first block is fetched */
	int			rs_ntuples;		/* Number of tuples on current page */
	OffsetNumber *rs_vistuples; /* Offset numbers of visible tuples */
	uint64		rs_snapshot_ts; /* Snapshot timestamp */
	uint64		rs_xact_ts;		/* Transaction timestamp */
	ParallelBlockTableScanWorkerData *rs_parallelworkerdata;	/* Parallel scan worker
																 * state */
	struct ReadStream *rs_read_stream;	/* Read stream for sequential
										 * prefetching */
	BlockNumber rs_prefetch_block;	/* Next block for read stream callback */

	/* Cached visibility map buffer to avoid per-page VM I/O */
	Buffer		rs_vm_buffer;	/* Pinned VM buffer (or InvalidBuffer) */
	BlockNumber rs_vm_blockno;	/* VM block number for rs_vm_buffer */

	/*
	 * ANALYZE dictionary-refresh sample accumulation.  During an ANALYZE scan
	 * the decompressed bytes of the first varlena column are gathered here so
	 * RecnoMaybeRefreshDict() can train a candidate dictionary at scan end.
	 * All fields stay zero/NULL on non-ANALYZE scans.
	 */
	char	   *rs_dict_samplebuf;	/* Concatenated sample bytes */
	size_t	   *rs_dict_sizes;	/* Per-sample lengths, in order */
	int			rs_dict_nsamples;	/* Number of accumulated samples */
	Size		rs_dict_total;	/* Total bytes in rs_dict_samplebuf */
	Size		rs_dict_cap;	/* Capacity of rs_dict_samplebuf */
	int			rs_dict_maxsamples; /* Capacity of rs_dict_sizes */
	int16		rs_dict_attnum; /* 1-based varlena attr sampled, 0 = none */
} RecnoScanDescData;

typedef RecnoScanDescData *RecnoScanDesc;

/*
 * Index fetch table data for RECNO
 */
typedef struct IndexFetchRecnoData
{
	IndexFetchTableData base;	/* AM independent part of the descriptor */

	Buffer		buffer;
} IndexFetchRecnoData;

/*
 * Constants
 */
#define RECNO_PAGE_OVERHEAD		(MAXALIGN(SizeOfPageHeaderData) + MAXALIGN(sizeof(RecnoPageOpaqueData)))
#define RECNO_TUPLE_OVERHEAD	(MAXALIGN(sizeof(RecnoTupleHeader)))

/*
 * The null bitmap is anchored at t_attrs_bitmap and the column data is
 * anchored at RECNO_TUPLE_OVERHEAD; the two MUST coincide or reads corrupt.
 * t_pad keeps t_attrs_bitmap on the MAXALIGN boundary that equals the
 * overhead.  Enforce it so a future header change cannot silently break it.
 */
StaticAssertDecl(offsetof(RecnoTupleHeader, t_attrs_bitmap) == RECNO_TUPLE_OVERHEAD,
				 "RECNO tuple null bitmap must begin at RECNO_TUPLE_OVERHEAD");
#define RECNO_MAX_TUPLE_SIZE	MAXALIGN_DOWN(BLCKSZ - RECNO_PAGE_OVERHEAD - sizeof(ItemIdData))
#define RECNO_OVERFLOW_THRESHOLD (RECNO_MAX_TUPLE_SIZE / 4)

/*
 * Hard ceiling on the number of line pointers a RECNO page can hold.  Unlike
 * heap, RECNO pages mix full tuples with small overflow-continuation records,
 * and RecnoPageAddTuple deliberately omits PAI_IS_HEAP so PageAddItemExtended
 * does not clamp offsets to MaxHeapTuplesPerPage.  The smallest storable item
 * is an overflow-record header, so the densest possible packing is bounded by
 * that item size plus its line pointer.  This is the RECNO-true analogue of
 * MaxHeapTuplesPerPage and must be used for dense TID encoding so that every
 * valid offset maps to a distinct index without aliasing into the next block.
 */
#define MaxRecnoItemsPerPage \
	((int) ((BLCKSZ - RECNO_PAGE_OVERHEAD) / \
			(RECNO_OVERFLOW_RECORD_OVERHEAD + sizeof(ItemIdData))))

/*
 * Fill factor support.  Unlike heap, RECNO has STABLE TIDs and updates rows
 * IN PLACE -- a row that grows (e.g. an accumulating numeric like TPC-C
 * w_ytd/d_ytd gaining a digit) cannot be relocated to another page the way
 * heap moves a grown tuple.  It must fit on its home page or the UPDATE fails
 * with "does not fit".  So RECNO must reserve per-page headroom by default;
 * packing pages 100%% full (heap's default) guarantees that any in-place
 * growth on a full page aborts.  The default reserves ~10%% of each page,
 * which comfortably covers digit-growth of numeric columns plus transient
 * line-pointer bloat from concurrent updates to a hot page.  Users can raise
 * it with WITH (fillfactor=N) for append-mostly tables that never grow rows.
 */
#define RECNO_MIN_FILLFACTOR		10
#define RECNO_DEFAULT_FILLFACTOR	90

/* Macros for tuple access */
#define RecnoTupleGetHeader(tuple) ((tuple)->t_data)
#define RecnoTupleGetData(tuple) \
	((char *) (tuple)->t_data + RECNO_TUPLE_OVERHEAD)

/*
 * Version-pointer accessors (WS-PVS1).
 *
 * The version-chain head now lives in the fixed header field t_verptr (it
 * was formerly an unaligned 8-byte trailer located by item_len - 8).  The
 * item_len parameter is retained for source compatibility with existing
 * call sites but is no longer used.
 */
static inline RelUndoRecPtr
RecnoTupleGetVersionPtr(const RecnoTupleHeader *hdr, Size item_len)
{
	(void) item_len;
	return hdr->t_verptr;
}

static inline void
RecnoTupleSetVersionPtr(RecnoTupleHeader *hdr, Size item_len, RelUndoRecPtr ptr)
{
	(void) item_len;
	hdr->t_verptr = ptr;
}

/*
 * RecnoReconstructVisibleVersion (WS-PVS2)
 *
 * Walk the per-tuple version chain in the UNDO fork to find the image
 * the reader's MVCC snapshot should see in place of the on-page (newer)
 * data.  Returns true and populates *out_data / *out_len with a palloc'd
 * reconstructed image when a step back was taken; returns false when the
 * on-page image is what the reader should see.
 *
 * See src/backend/access/recno/recno_pvs.c for the algorithm.
 */
extern bool RecnoReconstructVisibleVersion(Relation rel, ItemPointer tid,
										   const char *onpage_image,
										   Size onpage_len,
										   Snapshot snapshot,
										   char **out_data, int *out_len);

/*
 * Escrow / delta-accumulation for commutative columns (prototype).
 * See src/backend/access/recno/recno_escrow.c.
 */
#define RECNO_ESCROW_MAX_DELTA_IMAGE	1024

extern AttrNumber RecnoEscrowAttnum(Relation rel);
extern bool RecnoEscrowUpdateIsEligible(Relation rel, AttrNumber attnum,
										const char *old_image, uint32 old_len,
										TupleTableSlot *slot);
extern void RecnoEscrowComputeDelta(Relation rel, AttrNumber attnum,
									const Datum *old_values, const bool *old_isnull,
									const Datum *new_values, const bool *new_isnull,
									Datum *delta_out,
									char *delta_image, uint16 *delta_len,
									char *neg_delta_image, uint16 *neg_delta_len);
extern void RecnoEscrowComputeDeltaFromSlot(Relation rel, AttrNumber attnum,
											ItemPointer tid, Snapshot snapshot,
											const char *old_image, uint32 old_len,
											TupleTableSlot *slot,
											char *delta_image, uint16 *delta_len,
											char *neg_delta_image,
											uint16 *neg_delta_len);
extern void RecnoEscrowRollback(char *image, uint32 image_len,
								uint16 esc_off,
								const char *neg_delta, uint16 neg_delta_len,
								int32 typmod,
								const char *old_image, uint32 old_len);
extern uint16 RecnoEscrowAttrOffset(Relation rel, const char *image,
									uint32 image_len, AttrNumber attnum);
extern void RecnoEscrowSetOnpageSum(Relation rel, char *image, uint32 image_len,
									AttrNumber attnum,
									const char *onpage_image, uint32 onpage_len,
									const char *delta_image, uint16 delta_len);
extern void RecnoRelUndoInstallEscrowHook(void);

/* Slot operations for RECNO tuples */
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsRecnoTuple;
extern void RecnoSlotStoreTuple(TupleTableSlot *slot, RecnoTupleHeader *tuple,
								uint32 tuple_len, Buffer buffer);
extern void RecnoSlotStoreMaterializedTuple(TupleTableSlot *slot,
											RecnoTupleHeader *tuple,
											uint32 tuple_len);
extern void RecnoSlotSetRowID(TupleTableSlot *slot, const ItemPointerData *tid,
							  uint32 gen);

#define TTS_IS_RECNOTUPLE(slot) ((slot)->tts_ops == &TTSOpsRecnoTuple)

/* Function prototypes */
extern Size RecnoComputeDataSize(TupleDesc tupdesc, Datum *values, bool *isnull);
extern RecnoTuple RecnoFormTuple(TupleDesc tupdesc, Datum *values, bool *isnull,
								 Relation rel, RecnoOverflowBuffers *overflow_buffers);
extern RecnoTuple RecnoFormTupleForceShrink(TupleDesc tupdesc, Datum *values,
											bool *isnull, Relation rel,
											RecnoOverflowBuffers *overflow_buffers);
extern RecnoTuple RecnoFormTupleUpdate(TupleDesc tupdesc, Datum *values,
									   bool *isnull, Relation rel,
									   RecnoOverflowBuffers *overflow_buffers,
									   const RecnoOverflowPtr *old_ovptrs,
									   const bool *old_ovpresent);
extern void RecnoDeformTuple(Relation rel, RecnoTuple tuple, TupleDesc tupdesc, Datum *values, bool *isnull);
extern void RecnoFreeTuple(RecnoTuple tuple);
extern bool RecnoTupleToSlot(RecnoTupleHeader *tuple_header, TupleTableSlot *slot);
extern bool RecnoTupleToSlotWithOverflow(RecnoTupleHeader *tuple_header,
										 TupleTableSlot *slot, Relation rel);

/* Page management */
extern void RecnoInitPage(Page page, Size pageSize);
extern OffsetNumber RecnoPageAddTuple(Page page, RecnoTuple tuple, Size tuple_size);
extern bool RecnoPageUpdateTuple(Page page, OffsetNumber offnum, RecnoTuple new_tuple,
								 uint64 old_commit_ts, uint64 new_commit_ts);
extern int	RecnoPageGetLiveTuples(Page page, uint64 snapshot_ts);
extern void RecnoPageDefragment(Page page);
extern void RecnoPageIndexTupleDelete(Page page, OffsetNumber offnum);
extern int	RecnoPagePruneOpt(Relation rel, Buffer buffer);

/* Overflow handling - column-level overflow */
extern Datum RecnoStoreOverflowColumn(Relation rel, Datum value, int attnum,
									  Size inline_prefix_size,
									  RecnoOverflowBuffers *overflow_buffers);
extern Datum RecnoFetchOverflowColumn(Relation rel, const void *overflow_varlena);
extern void RecnoDeleteOverflowChain(Relation rel, BlockNumber first_block,
									 OffsetNumber first_offset);
extern int	RecnoCollectOverflowPtrs(RecnoTupleHeader *tuple_hdr,
									 TupleDesc tupdesc,
									 BlockNumber *blocks, OffsetNumber *offsets,
									 int max_ptrs);
extern void RecnoCollectOverflowPtrsByAttr(RecnoTupleHeader *tuple_hdr,
										   TupleDesc tupdesc,
										   RecnoOverflowPtr *out_ptrs,
										   bool *out_present, int natts);
extern void RecnoDeleteTupleOverflows(Relation rel, RecnoTupleHeader *tuple_hdr,
									  TupleDesc tupdesc);
extern bool RecnoIsOverflowRecord(const void *item, Size item_len);

/*
 * Inline version of RecnoIsOverflowRecord for hot scan paths.
 * Checks whether an item is an overflow continuation record by testing
 * the magic number in the header.
 */
static inline bool
RecnoIsOverflowRecordInline(const void *item, Size item_len)
{
	if (item_len < sizeof(RecnoOverflowRecordHeader))
		return false;
	return ((const RecnoOverflowRecordHeader *) item)->or_magic ==
		RECNO_OVERFLOW_RECORD_MAGIC;
}
extern void RecnoGetOverflowStats(Relation rel, int64 *total_overflow_records,
								  int64 *total_overflow_bytes, int64 *avg_chain_length);
extern void RecnoVacuumOverflowRecords(Relation rel);
extern BlockNumber RecnoFindOverflowPageForReuse(Relation rel, Page head_page,
												 Size needed);

/* Compression */
extern Datum RecnoCompressAttribute(Relation rel, Datum value, Oid typid, RecnoCompressionType comp_type);
extern Datum RecnoDecompressAttribute(Oid relid, Datum value, Oid typid, RecnoCompressionHeader *header);
extern void RecnoMaybeRefreshDict(Relation rel, const char *sample_buf,
								  const size_t *sample_sizes, int nsamples,
								  Size total);

/* Free space management */
extern BlockNumber RecnoGetPageWithFreeSpace(Relation rel, Size needed);
extern void RecnoRecordFreeSpace(Relation rel, BlockNumber page, Size freespace);
extern void RecnoVacuumFSM(Relation rel, BlockNumber new_nblocks);

/* Visibility Map management */
extern void RecnoVMInit(Relation rel);
extern void RecnoVMSet(Relation rel, BlockNumber heapBlk, Buffer heapBuf, uint8 flags);
extern void RecnoVMClear(Relation rel, BlockNumber heapBlk, Buffer heapBuf, uint8 flags);
extern bool RecnoVMCheck(Relation rel, BlockNumber heapBlk, uint8 flags);
extern bool RecnoVMCheckCached(Relation rel, BlockNumber heapBlk, uint8 flags,
							   Buffer *vmbuf, BlockNumber *vm_blockno);
extern void RecnoVMPinBuffer(Relation rel, BlockNumber heapBlk, Buffer *vmbuf);
extern void RecnoVMExtend(Relation rel, BlockNumber nheapblocks);
extern void RecnoVMTruncate(Relation rel, BlockNumber nheapblocks);
extern Size RecnoVMGetPageSize(void);
extern BlockNumber RecnoVMMapHeapToVM(BlockNumber heapBlk);
extern void RecnoVMUpdateForInsert(Relation rel, RecnoTupleHeader *tuple, Buffer buffer);
extern void RecnoVMUpdateForUpdate(Relation rel, Buffer buffer);
extern void RecnoVMUpdateForDelete(Relation rel, Buffer buffer);
extern void RecnoVMVacuumPage(Relation rel, Buffer buffer, bool all_visible, bool all_frozen);

/* MVCC functions */
extern uint64 RecnoGetCommitTimestamp(void);
extern uint64 RecnoGetTransactionTimestamp(void);
extern uint64 RecnoGetOldestActiveTimestamp(void);
extern Size RecnoMvccShmemSize(void);
extern void RecnoMvccShmemInit(void);
extern const ShmemCallbacks RecnoMvccShmemCallbacks;
extern void RecnoCommitTransaction(void);
extern void RecnoAbortTransaction(void);
extern uint64 RecnoGetSnapshotTimestamp(Snapshot snapshot);
extern bool RecnoTupleVisibleToSnapshot(RecnoTupleHeader *tuple, Snapshot snapshot,
										Oid relid, Buffer buffer);
extern void RecnoUpdateOldestActiveTimestamp(void);
extern void RecnoPrepareReassignSlot(int dummy_slot);
extern void RecnoResolvePreparedSlot(int dummy_slot);
extern void RecnoGetMvccStats(uint64 *current_ts, uint64 *oldest_ts, int *active_xacts);

/* SSI (Serializable Snapshot Isolation) via predicate.c integration */
extern void RecnoCheckForSerializableConflictOut(Relation relation,
												 RecnoTupleHeader *tuple,
												 Buffer buffer,
												 Snapshot snapshot);

/* MVCC timestamp helpers (page-level bookkeeping; visibility uses xmin/xmax) */
extern uint64 RecnoGetDmlTimestamp(void);

/*
 * WS-PVS3 lost-update conflict probe (fork-driven).  Reads the head verptr
 * from the on-page tuple, resolves it in the UNDO fork, and reports whether
 * that head record's committer is concurrent-or-later to snapshot (and not
 * exclude_xid).  If it is, the caller returns TM_Updated to drive EPQ.  EPQ
 * dedup lives in RecnoTransactionState: RecnoEpqReconcileMatches skips a
 * probe we already bounced on for this exact (relid, tid, cid, head verptr,
 * head xid); RecnoEpqReconcileMark stamps the identity we just bounced on.
 */
extern bool RecnoTupleHasCommittedUpdateAfter(Relation rel,
											  const RecnoTupleHeader *tuple,
											  Size tuple_len,
											  Snapshot snapshot,
											  TransactionId exclude_xid,
											  RelUndoRecPtr *out_head_verptr,
											  TransactionId *out_head_xid,
											  bool *out_inprogress);
extern bool RecnoEpqReconcileMatches(Snapshot snapshot, Oid relid,
									 ItemPointer tid,
									 RelUndoRecPtr head_verptr,
									 TransactionId head_xid);
extern void RecnoEpqReconcileMark(Snapshot snapshot, Oid relid,
								  ItemPointer tid,
								  RelUndoRecPtr head_verptr,
								  TransactionId head_xid);
extern bool RecnoTupleVisibleToSnapshotDual(RecnoTupleHeader *tuple,
											Snapshot snapshot,
											Oid relid, Buffer buffer);
extern TransactionId RecnoGetOldestXminHorizon(Relation rel);
extern bool RecnoTupleDeadToAll(RecnoTupleHeader *tuple,
								TransactionId oldest_xmin);

/*
 * MultiXact support has been removed.  Concurrent tuple locking is now
 * tracked via the sLog (recno_slog.c).
 */

/* Dirty block map (lock-free sLog bypass) */
extern Size RecnoDirtyMapShmemSize(void);
extern void RecnoDirtyMapShmemInit(void);
extern const ShmemCallbacks RecnoDirtyMapShmemCallbacks;

extern bool recno_lazy_uncommitted_clear;


/* Lock operations */
extern bool RecnoLockTuple(Relation rel, ItemPointer tid, LockTupleMode mode,
						   bool wait, bool *have_tuple_lock);
extern void RecnoUnlockTuple(Relation rel, ItemPointer tid, LockTupleMode mode);
extern void RecnoLockPage(Relation rel, BlockNumber blkno, LOCKMODE mode);
extern void RecnoUnlockPage(Relation rel, BlockNumber blkno, LOCKMODE mode);
extern bool RecnoLockMultipleTuples(Relation rel, ItemPointerData *tids, int ntids,
									LockTupleMode mode, bool wait);
extern void RecnoLockRelationForDDL(Relation rel, LOCKMODE lockmode);
extern bool RecnoHoldsTupleLock(Relation rel, ItemPointer tid, LockTupleMode mode);

/* Table operations */
extern void recno_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
							   uint32 options, BulkInsertState bistate);
extern TM_Result recno_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
									uint32 options, Snapshot snapshot, Snapshot crosscheck,
									bool wait, TM_FailureData *tmfd);
extern TM_Result recno_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
									CommandId cid, uint32 options,
									Snapshot snapshot, Snapshot crosscheck,
									bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode,
									Bitmapset **modified_attrs);
extern void recno_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
							   CommandId cid, uint32 options, BulkInsertState bistate);
extern void recno_relation_vacuum(Relation onerel, const VacuumParams *params,
								  BufferAccessStrategy bstrategy);
extern const TableAmRoutine *GetRecnoTableAmRoutine(void);

/*
 * recno_tableam_handler is declared via PG_FUNCTION_INFO_V1 in
 * recno_handler.c.  Do not redeclare it here: on Windows that emits a
 * __declspec(dllimport) prototype that conflicts with the implicit
 * dllexport from the V1 info macro.  Catalog references go through
 * pg_proc by name.
 */

/* Compression statistics and management */
extern void RecnoResetCompressionDict(void);

/* In-place update statistics */
extern void RecnoGetUpdateStats(int64 *in_place, int64 *out_of_place,
								int64 *defrag_triggered);

/*
 * RECNO-specific ANALYZE statistics
 *
 * These statistics capture properties unique to the RECNO storage format
 * and are collected during ANALYZE.  They are stored in the relation's
 * pg_class.reloptions and consumed by the planner to improve cost estimates.
 */
typedef struct RecnoRelationStats
{
	/* Compression effectiveness */
	double		compression_ratio;	/* avg uncompressed/compressed size */
	double		pct_compressed; /* fraction of tuples that are compressed */

	/* Overflow usage */
	double		pct_overflow;	/* fraction of tuples with overflow attrs */
	double		avg_overflow_chain_len; /* avg overflow records per overflow
										 * tuple */
	int64		total_overflow_bytes;	/* total bytes in overflow records */

	/* Space efficiency */
	double		avg_tuple_size; /* average on-disk tuple size (bytes) */
	double		avg_live_per_page;	/* average live tuples per page */
	double		free_space_frac;	/* average fraction of free space per page */
	double		bloat_factor;	/* allocated space / live data ratio */

	/* Page-level summary */
	int64		total_pages;	/* total pages in relation */
	int64		total_live_tuples;	/* total live tuples counted */
	int64		total_dead_tuples;	/* total dead tuples counted */

	/* Per-tuple commit-ts word distribution */
	bool		commit_ts_stats_valid;	/* true if commit-ts word fields are populated */
	uint64		commit_ts_min;		/* min commit-ts word seen */
	uint64		commit_ts_max;		/* max commit-ts word seen */
} RecnoRelationStats;

/* ANALYZE statistics collection (recno_stats.c) */
extern void RecnoCollectRelationStats(Relation rel, RecnoRelationStats *stats);
extern void RecnoLogRelationStats(Relation rel, const RecnoRelationStats *stats,
								  int elevel);

/* GUC variables */
extern int	recno_compression_level;
extern int	recno_compression_algorithm;
extern bool recno_enable_compression;
extern bool recno_analyze_refresh_dict;
extern double recno_compression_min_ratio;
extern int	recno_overflow_inline_prefix;

/* sLog transaction callbacks (recno_operations.c) */
extern void RecnoEnsureSLogCallbacks(void);

/* Two-phase commit support (recno_operations.c) */
extern void AtPrepare_Recno(void);
extern void recno_twophase_postcommit(FullTransactionId fxid, uint16 info,
									  void *recdata, uint32 len);
extern void recno_twophase_postabort(FullTransactionId fxid, uint16 info,
									 void *recdata, uint32 len);
extern void recno_twophase_recover(FullTransactionId fxid, uint16 info,
								   void *recdata, uint32 len);

/*
 * RECNO index-deduplication posting codec (recno_posting.c) and the RECNO
 * RowID comparator (recno_handler.c).  RecnoRowIDType.posting points at
 * RecnoPostingOps so nbtree can deduplicate wide (TID, gen) RowIDs.
 */
extern int32 recno_rowid_compare(const uint8 *a, const uint8 *b);
extern const RowIDPostingOps RecnoPostingOps;

#endif							/* RECNO_H */
