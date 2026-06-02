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
#include "access/recno_diff.h"
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
 * store the HLC timestamp (sufficient for 73,000+ years of microseconds).
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
 * RECNO tuple header structure (v2 -- sLog-based MVCC)
 *
 * Reduced from 64 bytes to 32 bytes (MAXALIGN'd) by removing:
 *   - t_xmin (4B) -- replaced by sLog self-visibility check
 *   - t_xmax (4B) -- replaced by sLog lock/delete tracking
 *   - t_xact_ts (8B) -- HLC is sole clock
 *   - t_infomask2 (2B) -- merged into t_flags
 *   - t_inline_diff (14B) -- moved to conditional position after bitmap
 *
 * The sole MVCC field is t_commit_ts (HLC timestamp).
 * Transient operation state (who is inserting/deleting/locking) is
 * tracked in the sLog, not in the tuple header.
 *
 * t_writer: Per-tuple CAS writer lock for same-size updates under
 * BUFFER_LOCK_SHARE.  0 = unlocked; non-zero = (MyProcNumber + 1) of
 * the writer.  Operated on via atomic CAS through RecnoTupleWriter*
 * macros below.  Placement after t_commit_ts (8B) keeps the total
 * at 8+4+6+2+2+1 = 23 bytes raw, still MAXALIGN'd to 24 bytes.
 */
typedef struct RecnoTupleHeader
{
	uint64		t_commit_ts;	/* 8B  HLC commit timestamp (sole MVCC field) */
	uint32		t_writer;		/* 4B  Per-tuple CAS writer lock (0=free) */

	/*
	 * t_cid removed: command ID is now obtained from the sLog entry
	 * (RecnoSLogEntry.cid) when RECNO_TUPLE_UNCOMMITTED is set. This saves 4
	 * bytes per tuple (28B -> 24B header, HEAP parity). The sLog lookup is
	 * mandatory for uncommitted visibility anyway, so fetching the cid from
	 * there adds zero extra overhead.
	 *
	 * t_xid_hint also removed: the inserter XID is now obtained from the sLog
	 * entry (RecnoSLogEntry.xid) when RECNO_TUPLE_UNCOMMITTED is set.
	 */
	ItemPointerData t_ctid;		/* 6B  Current TID / update chain */
	uint16		t_natts;		/* 2B  Number of attributes */
	uint16		t_flags;		/* 2B  Tuple flags */
	uint8		t_infomask;		/* 1B  HASNULL, HASVARWIDTH, etc. */
	uint8		t_attrs_bitmap[FLEXIBLE_ARRAY_MEMBER];
	/* Optional: RecnoInlineDiff after bitmap if HAS_INLINE_DIFF set */
} RecnoTupleHeader;

/* Fixed size: 23 bytes raw (MAXALIGN'd to 24 bytes). t_len removed — use ItemIdGetLength(itemid) for on-disk length. */

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
 * normal tuples, unlike TOAST which uses a separate relation.
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
	uint16		ov_padding;		/* Alignment padding */
	uint32		ov_total_length;	/* Total uncompressed column data length */
	uint16		ov_inline_prefix;	/* Bytes of inline prefix stored after ptr */
	uint16		ov_flags;		/* Overflow flags (reserved) */
} RecnoOverflowPtr;

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
 * Legacy overflow structures (kept for compatibility during transition)
 */
typedef struct RecnoOverflowRef
{
	uint32		overflow_page;	/* First overflow page */
	uint32		total_length;	/* Total attribute length */
	uint32		compression_info;	/* Compression metadata */
} RecnoOverflowRef;

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

typedef struct RecnoCompressionHeader
{
	uint8		comp_type;
	uint8		comp_level;
	uint16		_pad;
	uint32		orig_size;
	uint32		comp_size;
} RecnoCompressionHeader;

/*
 * Hybrid Logical Clock (HLC) timestamp.
 *
 * Packed into a single uint64:
 *   [63..16] 48-bit physical time (milliseconds since PG epoch)
 *   [15.. 0] 16-bit logical counter
 *
 * Simple uint64 comparison gives a correct total order that respects
 * causality (Kulkarni et al., 2014).
 */
typedef uint64 HLCTimestamp;

/* Invalid/zero HLC sentinel */
#define InvalidHLCTimestamp		((HLCTimestamp) 0)

/* HLC bit layout constants */
#define HLC_PHYSICAL_BITS		48
#define HLC_LOGICAL_BITS		16
#define HLC_LOGICAL_MASK		((UINT64CONST(1) << HLC_LOGICAL_BITS) - 1)
#define HLC_MAX_LOGICAL			0xFFFF	/* Maximum 16-bit logical counter */

/* HLC field extraction/construction macros */
#define HLC_GET_PHYSICAL(hlc)	((hlc) >> HLC_LOGICAL_BITS)
#define HLC_GET_LOGICAL(hlc)	((hlc) & HLC_LOGICAL_MASK)
#define HLC_MAKE(physical, logical) \
	(((uint64)(physical) << HLC_LOGICAL_BITS) | \
	 ((uint64)(logical) & HLC_LOGICAL_MASK))

/*
 * HLC comparison helpers.
 *
 * Because physical time occupies the high bits, standard uint64 comparison
 * gives correct causal ordering.  These are provided for readability.
 */
#define HLCBefore(a, b)			((a) < (b))
#define HLCAfterOrEqual(a, b)	((a) >= (b))

/*
 * Tuple header field accessors for HLC mode.
 */
#define RecnoTupleGetHLC(tup)		((HLCTimestamp)(tup)->t_commit_ts)
#define RecnoTupleSetHLC(tup, hlc)	((tup)->t_commit_ts = (uint64)(hlc))

/*
 * Pruning result for HLC-based pruning decisions.
 */
typedef enum RecnoPruneResult
{
	RECNO_PRUNE_KEEP,			/* Version must be kept */
	RECNO_PRUNE_DEAD,			/* Version is dead, can be removed */
	RECNO_PRUNE_DOMINATED,		/* Version is causally dominated */
	RECNO_PRUNE_RECENTLY_DEAD	/* Dead but might be needed by snapshot */
} RecnoPruneResult;

/*
 * HLC Uncertainty Interval.
 *
 * Represents the window [lower, upper] around a commit HLC where
 * clock skew may cause ambiguity in real-time ordering.  Used in
 * distributed scenarios and logged in WAL for replication.
 */
typedef struct HLCUncertaintyInterval
{
	HLCTimestamp lower;			/* commit_hlc - max_clock_offset */
	HLCTimestamp upper;			/* commit_hlc + max_clock_offset */
} HLCUncertaintyInterval;

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
	HLCTimestamp rs_snapshot_hlc;	/* Snapshot HLC */
	ParallelBlockTableScanWorkerData *rs_parallelworkerdata;	/* Parallel scan worker
																 * state */
	struct ReadStream *rs_read_stream;	/* Read stream for sequential
										 * prefetching */
	BlockNumber rs_prefetch_block;	/* Next block for read stream callback */

	/* Cached visibility map buffer to avoid per-page VM I/O */
	Buffer		rs_vm_buffer;	/* Pinned VM buffer (or InvalidBuffer) */
	BlockNumber rs_vm_blockno;	/* VM block number for rs_vm_buffer */
} RecnoScanDescData;

typedef RecnoScanDescData *RecnoScanDesc;

/*
 * Index fetch table data for RECNO
 */
typedef struct IndexFetchRecnoData
{
	IndexFetchTableData base;	/* AM independent part of the descriptor */

	Buffer		buffer;
	bool		all_dead;
} IndexFetchRecnoData;

/*
 * Constants
 */
#define RECNO_PAGE_OVERHEAD		(MAXALIGN(SizeOfPageHeaderData) + MAXALIGN(sizeof(RecnoPageOpaqueData)))
#define RECNO_TUPLE_OVERHEAD	(MAXALIGN(sizeof(RecnoTupleHeader)))
#define RECNO_MAX_TUPLE_SIZE	MAXALIGN_DOWN(BLCKSZ - RECNO_PAGE_OVERHEAD - sizeof(ItemIdData))
#define RECNO_OVERFLOW_THRESHOLD (RECNO_MAX_TUPLE_SIZE / 4)

/*
 * Fill factor support.  Default is 100 (pack pages fully), matching heap.
 * Lower values reserve space on each page for in-place updates.
 */
#define RECNO_MIN_FILLFACTOR		10
#define RECNO_DEFAULT_FILLFACTOR	100

/* Macros for tuple access */
#define RecnoTupleGetHeader(tuple) ((tuple)->t_data)
#define RecnoTupleGetData(tuple) \
	((char *) (tuple)->t_data + RECNO_TUPLE_OVERHEAD)
#define RecnoTupleIsVisible(tuple, snapshot_ts, xact_ts, relid, curcid, buf) \
	(RecnoTupleVisible(RecnoTupleGetHeader(tuple), snapshot_ts, xact_ts, relid, curcid, buf))

/* Slot operations for RECNO tuples */
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsRecnoTuple;
extern void RecnoSlotStoreTuple(TupleTableSlot *slot, RecnoTupleHeader *tuple,
								uint32 tuple_len, Buffer buffer);
extern void RecnoSlotStoreMaterializedTuple(TupleTableSlot *slot,
											RecnoTupleHeader *tuple,
											uint32 tuple_len);

#define TTS_IS_RECNOTUPLE(slot) ((slot)->tts_ops == &TTSOpsRecnoTuple)

/* Function prototypes */
extern bool RecnoTupleVisible(RecnoTupleHeader *tuple, uint64 snapshot_ts, uint64 xact_ts,
							  Oid relid, CommandId curcid, Buffer buffer);
extern Size RecnoComputeDataSize(TupleDesc tupdesc, Datum *values, bool *isnull);
extern RecnoTuple RecnoFormTuple(TupleDesc tupdesc, Datum *values, bool *isnull,
								 Relation rel, RecnoOverflowBuffers *overflow_buffers);
extern RecnoTuple RecnoFormTupleFromSlot(TupleTableSlot *slot);
extern Size RecnoComputeSlotSize(TupleTableSlot *slot);
extern void RecnoDeformTuple(RecnoTuple tuple, TupleDesc tupdesc, Datum *values, bool *isnull);
extern void RecnoFreeTuple(RecnoTuple tuple);
extern bool RecnoTupleToSlot(RecnoTupleHeader *tuple_header, TupleTableSlot *slot);
extern bool RecnoTupleToSlotWithOverflow(RecnoTupleHeader *tuple_header,
										 TupleTableSlot *slot, Relation rel);

/* Page management */
extern void RecnoInitPage(Page page, Size pageSize);
extern OffsetNumber RecnoPageAddTuple(Page page, RecnoTuple tuple, Size tuple_size);
extern void RecnoPageDeleteTuple(Page page, OffsetNumber offnum, uint64 commit_ts);
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

/* Legacy overflow interface (deprecated, for transition) */
extern RecnoOverflowRef *RecnoStoreOverflow(Relation rel, Datum value, int attnum);
extern Datum RecnoFetchOverflow(Relation rel, RecnoOverflowRef *ref);
extern void RecnoDeleteOverflow(Relation rel, RecnoOverflowRef *ref);

/* Compression */
extern Datum RecnoCompressAttribute(Datum value, Oid typid, RecnoCompressionType comp_type);
extern Datum RecnoDecompressAttribute(Datum value, Oid typid, RecnoCompressionHeader *header);

/* Free space management */
extern void RecnoInitFSM(Relation rel);
extern BlockNumber RecnoGetPageWithFreeSpace(Relation rel, Size needed);
extern void RecnoRecordFreeSpace(Relation rel, BlockNumber page, Size freespace);
extern void RecnoMarkPageForDefrag(Relation rel, BlockNumber page);
extern void RecnoOpportunisticDefrag(Relation rel);
extern void RecnoVacuumFSM(Relation rel, BlockNumber new_nblocks);
extern void RecnoGetFSMStats(Relation rel, int64 *total_pages, int64 *free_pages,
							 double *avg_free_space, int64 *defrag_needed);
extern void RecnoBatchDefrag(Relation rel, int max_pages);

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
extern void RecnoGetMvccStats(uint64 *current_ts, uint64 *oldest_ts, int *active_xacts);
extern bool RecnoCanVacuumTimestamp(uint64 vacuum_ts);

/* SSI (Serializable Snapshot Isolation) via predicate.c integration */
extern void RecnoCheckForSerializableConflictOut(Relation relation,
												 RecnoTupleHeader *tuple,
												 Buffer buffer,
												 Snapshot snapshot);

/* HLC MVCC functions (dual-mode wrappers) */
extern HLCTimestamp RecnoGetDmlTimestamp(void);
extern HLCTimestamp RecnoGetCommitHLC(HLCTimestamp msg_hlc);
extern HLCTimestamp RecnoGetTransactionHLC(void);
extern HLCTimestamp RecnoGetOldestActiveHLC(void);
extern uint64 RecnoGetOldestActiveSnapshotHLC(void);
extern bool RecnoHasActiveIsoReaders(void);
extern HLCTimestamp RecnoGetSnapshotHLC(Snapshot snapshot);
extern uint64 RecnoGetEpqReconcileFloor(Snapshot snapshot, Oid relid,
										ItemPointer tid);
extern void RecnoMarkEpqReconcile(Snapshot snapshot, Oid relid,
								  ItemPointer tid);
extern bool RecnoTupleVisibleHLC(RecnoTupleHeader *tuple,
								 HLCTimestamp snapshot_hlc,
								 Oid relid, CommandId curcid,
								 Buffer buffer);
extern bool RecnoTupleVisibleToSnapshotDual(RecnoTupleHeader *tuple,
											Snapshot snapshot,
											Oid relid, Buffer buffer);
extern bool RecnoCanPruneHLC(RecnoTupleHeader *tuple,
							 HLCTimestamp prune_horizon);
extern RecnoPruneResult RecnoPruneDecision(RecnoTupleHeader *tuple,
										   RecnoTupleHeader *newer_version,
										   HLCTimestamp prune_horizon);
extern bool RecnoTupleVisibleWithUncertainty(RecnoTupleHeader *tuple,
											 HLCTimestamp snapshot_hlc,
											 RecnoTransactionState *txn_state,
											 Oid relid);

/*
 * MultiXact support has been removed.  Concurrent tuple locking is now
 * tracked via the sLog (recno_slog.c).
 */

/* HLC (Hybrid Logical Clock) functions */
extern HLCTimestamp HLCNow(HLCTimestamp msg_hlc);
extern int	HLCCompare(HLCTimestamp a, HLCTimestamp b);
extern uint64 HLCGetPhysical(HLCTimestamp hlc);
extern uint16 HLCGetLogical(HLCTimestamp hlc);
extern HLCTimestamp HLCMake(uint64 physical_ms, uint16 logical);
extern TimestampTz HLCToTimestampTz(HLCTimestamp hlc);
extern HLCTimestamp HLCFromTimestampTz(TimestampTz ts);
extern HLCTimestamp HLCGetGlobal(void);
extern char *HLCToString(HLCTimestamp hlc);
extern void HLCGetDriftStats(uint64 *max_drift_ms,
							 uint64 *total_backward_jumps,
							 uint64 *total_overflow_events);
extern void HLCGetUncertaintyInterval(HLCTimestamp hlc,
									  HLCTimestamp *lower,
									  HLCTimestamp *upper);
extern bool HLCInUncertaintyWindow(HLCTimestamp reader_hlc,
								   HLCTimestamp commit_hlc);
extern Size RecnoHLCShmemSize(void);
extern void RecnoHLCShmemInit(void);
extern const ShmemCallbacks RecnoHLCShmemCallbacks;

/* Dirty block map (lock-free sLog bypass) */
extern Size RecnoDirtyMapShmemSize(void);
extern void RecnoDirtyMapShmemInit(void);
extern const ShmemCallbacks RecnoDirtyMapShmemCallbacks;

/* HLC GUC variables and hooks */
extern int	recno_node_id;
extern int	recno_max_clock_offset_ms;
extern bool recno_use_hlc;
extern bool recno_uncertainty_wait;
extern bool recno_lazy_uncommitted_clear;
extern void assign_recno_node_id(int newval, void *extra);
extern void assign_recno_max_clock_offset(int newval, void *extra);

/* Replica-side HLC uncertainty handling */
extern void RecnoReplicaHandleUncertainty(HLCTimestamp commit_hlc,
										  int32 uncertainty_ms);
extern void RecnoReplicaAdvanceHLC(HLCTimestamp target_hlc);

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
									TU_UpdateIndexes *update_indexes);
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

	/* HLC timestamp distribution (populated when HLC mode is enabled) */
	bool		hlc_stats_valid;	/* true if HLC fields are populated */
	uint64		hlc_min;		/* min HLC timestamp seen */
	uint64		hlc_max;		/* max HLC timestamp seen */
} RecnoRelationStats;

/* ANALYZE statistics collection (recno_stats.c) */
extern void RecnoCollectRelationStats(Relation rel, RecnoRelationStats *stats);
extern void RecnoLogRelationStats(Relation rel, const RecnoRelationStats *stats,
								  int elevel);

/* GUC variables */
extern int	recno_compression_level;
extern char *recno_compression_algorithm;
extern bool recno_enable_compression;
extern double recno_compression_min_ratio;
extern int	recno_overflow_inline_prefix;

/* Clock-bound integration structures and functions */

/*
 * RecnoTimestampBound - timestamp with error bounds from clock-bound
 *
 * Provides bounded timestamps for safe distributed MVCC. When clock-bound
 * is available, earliest_us and latest_us give tight bounds. Otherwise,
 * falls back to HLC +/- max_offset.
 */
typedef struct RecnoTimestampBound
{
	HLCTimestamp hlc;			/* Hybrid logical clock timestamp */
	int64		earliest_us;	/* Earliest possible time (microseconds) */
	int64		latest_us;		/* Latest possible time (microseconds) */
	uint64		error_bound_ms; /* Error bound in milliseconds */
	bool		bounds_valid;	/* True if bounds from clock-bound daemon */
} RecnoTimestampBound;

/*
 * RecnoClockStats - clock monitoring statistics
 */
typedef struct RecnoClockStats
{
	bool		clock_bound_available;	/* Clock-bound daemon accessible */
	uint64		max_observed_error_ms;	/* Maximum observed error bound */
	uint64		total_skew_warnings;	/* Count of skew warnings */
	uint64		total_fatal_checks; /* Count of fatal threshold hits */
	TimestampTz last_sync_time; /* Last successful NTP sync */
	TimestampTz last_check_time;	/* Last health check */
} RecnoClockStats;

/* Clock-bound functions (recno_clock.c) */
extern const ShmemCallbacks RecnoClockShmemCallbacks;
extern Size RecnoClockShmemSize(void);
extern void RecnoClockShmemInit(void);
extern void RecnoClockStartMonitor(void);
extern void RecnoClockMonitorMain(Datum main_arg);
extern RecnoTimestampBound RecnoGetTimestampBounds(void);
extern void RecnoWaitForClockBound(RecnoTimestampBound origin_bounds);
extern void RecnoClockGetStats(RecnoClockStats *stats);
extern void RecnoClockShutdown(void);

/* Clock-bound GUC variables */
extern bool recno_enable_clock_bound;
extern bool recno_fatal_on_clock_drift;
extern int	recno_clock_check_interval_ms;

/* GUC assign hooks */
extern void assign_recno_enable_clock_bound(bool newval, void *extra);
extern void assign_recno_fatal_on_clock_drift(bool newval, void *extra);
extern void assign_recno_clock_check_interval(int newval, void *extra);

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

#endif							/* RECNO_H */
