/*-------------------------------------------------------------------------
 *
 * recno.h
 *	  RECNO table access method definitions
 *
 * RECNO is an in-place-MVCC heap-replacement table AM with hybrid logical
 * clock (HLC) based visibility, overflow pages, per-tuple compression and a
 * per-backend UNDO version chain.  It is a sibling of the FLUX AM
 * (access/flux.h) and shares its structure; this header is the reconstructed
 * public surface for the vendored RECNO sources.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Author: Greg Burd <greg@burd.me>
 *
 * src/include/access/recno.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RECNO_H
#define RECNO_H

#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/undodefs.h"
#include "access/xact.h"
#include "executor/tuptable.h"
#include "port/atomics.h"
#include "storage/buf.h"
#include "storage/bufpage.h"
#include "storage/lockdefs.h"
#include "storage/read_stream.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/* ------------------------------------------------------------------------
 * Hybrid Logical Clock (HLC)
 *
 * HLCTimestamp is a packed uint64: high HLC_PHYSICAL_BITS bits are physical
 * milliseconds, low HLC_LOGICAL_BITS bits are the logical counter.
 * ------------------------------------------------------------------------ */
typedef uint64 HLCTimestamp;

#define HLC_LOGICAL_BITS		16
#define HLC_PHYSICAL_BITS		48
#define HLC_LOGICAL_MASK		((UINT64CONST(1) << HLC_LOGICAL_BITS) - 1)	/* 0xFFFF */
#define HLC_PHYSICAL_MASK		(~HLC_LOGICAL_MASK)
#define HLC_MAX_LOGICAL			HLC_LOGICAL_MASK
#define InvalidHLCTimestamp		((HLCTimestamp) 0)

/* Packed accessors (uppercase = macro). */
#define HLC_GET_PHYSICAL(hlc)	((uint64) ((hlc) >> HLC_LOGICAL_BITS))
#define HLC_GET_LOGICAL(hlc)	((uint16) ((hlc) & HLC_LOGICAL_MASK))
#define HLC_MAKE(phys, logical)	\
	((HLCTimestamp) (((uint64) (phys) << HLC_LOGICAL_BITS) | \
					 ((uint64) (logical) & HLC_LOGICAL_MASK)))

/* Ordering (HLCTimestamp is a monotone-packed uint64). */
#define HLCBefore(a, b)			((a) < (b))
#define HLCAfter(a, b)			((a) > (b))
#define HLCBeforeOrEqual(a, b)	((a) <= (b))
#define HLCAfterOrEqual(a, b)	((a) >= (b))
#define HLCEqual(a, b)			((a) == (b))

/* Uncertainty interval endpoints for one HLC timestamp. */
typedef struct RecnoHlcInfo
{
	HLCTimestamp commit_hlc;
	HLCTimestamp uncertainty_lower;
	HLCTimestamp uncertainty_upper;
} RecnoHlcInfo;

/* ------------------------------------------------------------------------
 * Page opaque (special space)
 * ------------------------------------------------------------------------ */
typedef struct RecnoPageOpaqueData
{
	uint64		pd_commit_ts;	/* page-level max commit timestamp (HLC) */
	uint32		pd_flags;		/* RECNO_PAGE_* */
	uint32		pd_free_space;	/* cached free-space hint */
} RecnoPageOpaqueData;

typedef RecnoPageOpaqueData *RecnoPageOpaque;

#define RECNO_PAGE_OVERFLOW			0x0001
#define RECNO_PAGE_DEFRAG_NEEDED	0x0002
#define RECNO_PAGE_FULL				0x0004

#define RECNO_PAGE_OVERHEAD			(MAXALIGN(sizeof(RecnoPageOpaqueData)))

/*
 * UPDATE HEADROOM (in-place growth reserve)
 *
 * RECNO updates in place at a stable TID: an UPDATE that makes the row even
 * one byte wider must find those bytes on the row's OWN page, because unlike
 * heap it cannot migrate the row to another page and leave a forwarding ctid.
 * A page packed to 100%% therefore makes every row on it permanently
 * un-updatable the first time any of them grows -- and rows DO grow without
 * any schema change: numeric(20,2) occupies 8 bytes at 30000.00 and 10 bytes
 * at 30001.00, because numeric stores base-10000 digit groups, so a plain
 * counter increment crossing a group boundary widens the datum.  That surfaced
 * as "updated recno tuple is too large for the page" on a TPROC-C d_ytd/w_ytd
 * counter with no concurrency involved at all.
 *
 * So the INSERT path holds back a fraction of each page as growth reserve, the
 * same role heap's fillfactor plays for HOT updates (heap merely gets slower
 * without it; RECNO gets a hard error, so RECNO reserves by default where heap
 * defaults to 100).  UPDATE deliberately ignores the reserve -- that is what it
 * is for.  An explicit fillfactor reloption overrides this default.
 */
#define RECNO_DEFAULT_FILLFACTOR	90

#define RecnoPageGetOpaque(page) \
	((RecnoPageOpaque) PageGetSpecialPointer(page))

/* ------------------------------------------------------------------------
 * Tuple header (fixed part) + wrapper
 * ------------------------------------------------------------------------ */
typedef struct RecnoTupleHeader
{
	uint64		t_commit_ts;	/* commit HLC timestamp (0 while uncommitted) */
	uint64		t_xact_ts;		/* transaction start HLC (reserved) */
	UndoRecPtr	t_verptr;		/* per-backend UNDO version-chain head */
	ItemPointerData t_ctid;		/* current TID / update chain link */
	ItemPointerData t_self;		/* this tuple's own TID (set on read) */
	Oid			t_tableOid;		/* owning relation OID (set on read) */
	TransactionId t_xmin;		/* inserting XID */
	TransactionId t_xmax;		/* deleting/updating XID (0 = live) */
	TransactionId t_xid_hint;	/* transient writer-XID hint */
	CommandId	t_cid;			/* command id */
	uint32		t_len;			/* total on-disk tuple length */
	uint16		t_natts;		/* number of attributes */
	uint16		t_flags;		/* RECNO_TUPLE_* */
	uint8		t_infomask;		/* RECNO_INFOMASK_* */
	uint8		t_pad[3];		/* pad so bitmap starts MAXALIGN'd */
	uint8		t_attrs_bitmap[FLEXIBLE_ARRAY_MEMBER];	/* null bitmap then data */
} RecnoTupleHeader;

#define RECNO_TUPLE_OVERHEAD	(offsetof(RecnoTupleHeader, t_attrs_bitmap))

/* In-memory tuple wrapper */
typedef struct RecnoTupleData
{
	uint32		t_len;			/* length of *t_data */
	ItemPointerData t_self;		/* SelfItemPointer */
	Oid			t_tableOid;		/* table the tuple came from */
	RecnoTupleHeader *t_data;	/* header + data */
} RecnoTupleData;

typedef RecnoTupleData *RecnoTuple;

/* Tuple flags (t_flags) */
#define RECNO_TUPLE_UNCOMMITTED		0x0001
#define RECNO_TUPLE_DELETED			0x0002
#define RECNO_TUPLE_UPDATED			0x0004
#define RECNO_TUPLE_LOCKED			0x0008
#define RECNO_TUPLE_COMPRESSED		0x0010
#define RECNO_TUPLE_HAS_OVERFLOW	0x0020
#define RECNO_TUPLE_HAS_ROW_OVERFLOW 0x0040
#define RECNO_TUPLE_HAS_INLINE_DIFF	0x0080
#define RECNO_TUPLE_SPECULATIVE		0x0100

/* Infomask bits (t_infomask) */
#define RECNO_INFOMASK_HASNULL		0x01
#define RECNO_INFOMASK_HASVARWIDTH	0x02
#define RECNO_INFOMASK_HASEXTERNAL	0x04
#define RECNO_INFOMASK_HASOVERFLOW	0x08
#define RECNO_INFOMASK_COMPRESSED	0x10

/* ------------------------------------------------------------------------
 * Compression
 * ------------------------------------------------------------------------ */
typedef enum RecnoCompressionType
{
	RECNO_COMP_NONE = 0,
	RECNO_COMP_LZ4,
	RECNO_COMP_ZSTD,
	RECNO_COMP_DICTIONARY,
	RECNO_COMP_DELTA
} RecnoCompressionType;

typedef struct RecnoCompressionHeader
{
	uint8		comp_type;		/* RecnoCompressionType */
	uint8		comp_level;
	uint16		_pad;
	uint32		orig_size;
	uint32		comp_size;
} RecnoCompressionHeader;

typedef struct RecnoCompressionDict RecnoCompressionDict;

/* ------------------------------------------------------------------------
 * Overflow
 * ------------------------------------------------------------------------ */
typedef struct RecnoOverflowPtr
{
	uint32		ov_magic;		/* RECNO_OVERFLOW_PTR_MAGIC */
	BlockNumber ov_first_block;	/* first overflow page */
	OffsetNumber ov_first_offset;	/* first overflow record offset */
	uint16		ov_flags;
	uint32		ov_total_length;	/* total stored length */
	uint16		ov_inline_prefix;	/* inline prefix bytes following */
	uint16		ov_padding;
} RecnoOverflowPtr;

#define RECNO_OVERFLOW_PTR_MAGIC		0x524F5650	/* "ROVP" */
#define RECNO_OVERFLOW_RECORD_MAGIC		0x524F5652	/* "ROVR" */
#define RECNO_OVERFLOW_DEFAULT_PREFIX	16
#define RECNO_OVERFLOW_MAX_CHUNK_SIZE	(BLCKSZ / 2)
#define RECNO_OVERFLOW_WAL_NEW_RECORD	0x01
#define RECNO_OVERFLOW_WAL_LINK_UPDATE	0x02

typedef struct RecnoOverflowRecordHeader
{
	uint32		or_magic;		/* RECNO_OVERFLOW_MAGIC */
	BlockNumber or_next_block;	/* next overflow page (or InvalidBlockNumber) */
	OffsetNumber or_next_offset;
	uint16		or_flags;
	uint32		or_data_len;	/* bytes of data in this record */
} RecnoOverflowRecordHeader;

#define RECNO_OVERFLOW_MAGIC		0x524F5646	/* "ROVF" */
#define RECNO_OVERFLOW_RECORD_OVERHEAD	(sizeof(RecnoOverflowRecordHeader))

typedef struct RecnoOverflowRef
{
	BlockNumber overflow_page;
	OffsetNumber overflow_offset;
	uint32		total_length;
	uint32		compression_info;
} RecnoOverflowRef;

typedef struct RecnoOverflowBuffer
{
	Buffer		buffer;
	OffsetNumber offset;
	uint16		flags;
	char	   *record_data;
	uint32		record_len;
} RecnoOverflowBuffer;

#define MAX_OVERFLOW_BUFFERS	32

typedef struct RecnoOverflowBuffers
{
	int			count;
	RecnoOverflowBuffer buffers[MAX_OVERFLOW_BUFFERS];
} RecnoOverflowBuffers;

/* ------------------------------------------------------------------------
 * Scan descriptor
 * ------------------------------------------------------------------------ */
typedef struct RecnoScanDescData
{
	TableScanDescData rs_base;	/* MUST be first */

	BlockNumber rs_nblocks;
	BlockNumber rs_startblock;
	BlockNumber rs_cblock;		/* current block */
	Buffer		rs_cbuf;		/* current buffer */
	OffsetNumber rs_coffset;	/* current offset within block */
	OffsetNumber rs_cindex;		/* current index into rs_vistuples */
	int			rs_ntuples;		/* # visible tuples on this page */
	OffsetNumber *rs_vistuples; /* their offsets */
	bool		rs_inited;

	/* MVCC timestamps for this scan */
	uint64		rs_snapshot_ts;
	uint64		rs_xact_ts;
	HLCTimestamp rs_snapshot_hlc;
	void	   *rs_mvcc;		/* RecnoMvccState* (opaque here) */

	/* prefetch / read stream */
	BlockNumber rs_prefetch_block;
	ReadStream *rs_read_stream;

	/* parallel */
	ParallelBlockTableScanWorkerData *rs_parallelworkerdata;
} RecnoScanDescData;

typedef struct RecnoScanDescData *RecnoScanDesc;

/* ------------------------------------------------------------------------
 * Slot
 * ------------------------------------------------------------------------ */
extern PGDLLIMPORT const TupleTableSlotOps TTSOpsRecnoTuple;

/* ------------------------------------------------------------------------
 * WAL / undo resource-manager ids (WAL id is positional in rmgrlist.h)
 * ------------------------------------------------------------------------ */

/* ------------------------------------------------------------------------
 * GUC variables (defined in various recno_*.c)
 * ------------------------------------------------------------------------ */
extern PGDLLIMPORT bool recno_use_hlc;
extern PGDLLIMPORT int recno_max_clock_offset_ms;
extern PGDLLIMPORT bool recno_uncertainty_wait;
extern PGDLLIMPORT int recno_node_id;
extern PGDLLIMPORT bool recno_enable_compression;
extern PGDLLIMPORT bool recno_enable_clock_bound;
extern PGDLLIMPORT bool recno_fatal_on_clock_drift;
extern PGDLLIMPORT int recno_clock_check_interval;
extern PGDLLIMPORT int recno_slog_entries_per_backend;
extern PGDLLIMPORT int recno_overflow_inline_prefix;

/* Attributes larger than this are pushed to overflow pages. */
#define RECNO_OVERFLOW_THRESHOLD	(BLCKSZ / 4)

/* Largest tuple that RECNO stores on a main page (rest goes to overflow). */
#define RECNO_MAX_TUPLE_SIZE		(BLCKSZ / 3)

/*
 * RECNO always records per-backend UNDO for its DML (am_undo_engine =
 * PERBACKEND), mirroring FLUX.  The historical enable_undo reloption gate is
 * retired: undo is unconditional so rollback and MVCC history always work.
 */
#define RelationHasUndo(rel)	(true)

/* Extract the commit HLC from a tuple header. */
static inline HLCTimestamp
RecnoTupleGetHLC(const RecnoTupleHeader *tuple)
{
	return (HLCTimestamp) tuple->t_commit_ts;
}

/* True iff a varlena datum is a RECNO overflow pointer. */
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

/* Extract the overflow pointer struct from a varlena datum. */
static inline const RecnoOverflowPtr *
RecnoGetOverflowPtr(const void *ptr)
{
	return (const RecnoOverflowPtr *) VARDATA_ANY(ptr);
}


/* ------------------------------------------------------------------------
 * Per-backend MVCC state (recno_handler.c)
 * ------------------------------------------------------------------------ */
typedef struct RecnoMvccData
{
	uint64		current_commit_ts;
	uint64		oldest_active_ts;
} RecnoMvccData;

/* Full per-backend transaction state lives in recno_mvcc.c. */
typedef struct RecnoTransactionState RecnoTransactionState;

/* HLC + clock-bound timestamp bundle (recno_clock.c). */
typedef struct RecnoTimestampBound
{
	HLCTimestamp hlc;
	uint64		earliest_us;
	uint64		latest_us;
	uint32		error_bound_ms;
	bool		bounds_valid;
} RecnoTimestampBound;

typedef struct RecnoClockStats
{
	bool		clock_bound_available;
	uint64		last_check_time;
	uint64		last_sync_time;
	uint64		max_observed_error_ms;
	uint64		total_fatal_checks;
	uint64		total_skew_warnings;
} RecnoClockStats;

/* HLC pruning decision. */
typedef enum RecnoPruneResult
{
	RECNO_PRUNE_KEEP = 0,
	RECNO_PRUNE_RECENTLY_DEAD,
	RECNO_PRUNE_DEAD
} RecnoPruneResult;

/* Relation-level statistics snapshot (recno_stats.c). */
typedef struct RecnoRelationStats
{
	int64		total_pages;
	int64		total_live_tuples;
	int64		total_dead_tuples;
	int64		total_overflow_bytes;
	double		avg_tuple_size;
	double		avg_live_per_page;
	double		avg_overflow_chain_len;
	double		bloat_factor;
	double		compression_ratio;
	double		free_space_frac;
	double		pct_compressed;
	double		pct_overflow;
	uint64		hlc_min;
	uint64		hlc_max;
	bool		hlc_stats_valid;
} RecnoRelationStats;

/*
 * Table AM private state for a RECNO index scan.
 *
 * Post-ddce1da5b1b (slot-based table-AM index scan interface): RECNO keeps its
 * own scan state in IndexScanDesc.xs_table_opaque, set up in
 * recno_index_scan_begin.  RECNO does in-place, stable-TID updates and never
 * walks a HOT-style chain, so it needs the relation, the pin held across
 * getnext_slot calls, and its pre-existing scan-level all_dead field (see
 * recno_index_fetch_tuple for its per-scan semantics).
 */
typedef struct IndexScanRecnoData
{
	Relation	rel;			/* the RECNO relation being scanned */
	Buffer		buffer;			/* pin held across getnext_slot calls */
	bool		all_dead;
} IndexScanRecnoData;

/* ------------------------------------------------------------------------
 * VM (visibility map) flag bits
 * ------------------------------------------------------------------------ */
#define RECNO_VM_ALL_VISIBLE	0x01
#define RECNO_VM_ALL_FROZEN		0x02
#define RECNO_VM_VALID_BITS		(RECNO_VM_ALL_VISIBLE | RECNO_VM_ALL_FROZEN)

/* ------------------------------------------------------------------------
 * Cross-file function prototypes (reconstructed from usage)
 * ------------------------------------------------------------------------ */
extern BlockNumber RecnoFindOverflowPageForReuse(Relation rel, Page head_page, Size needed);
extern BlockNumber RecnoGetPageWithFreeSpace(Relation rel, Size needed);
extern Size RecnoGetInsertFreeSpace(Relation rel, Size tuple_size);
extern BlockNumber RecnoVMMapHeapToVM(BlockNumber heapBlk);
extern bool HLCInUncertaintyWindow(HLCTimestamp reader_hlc, HLCTimestamp commit_hlc);
extern bool RecnoCanPruneHLC(RecnoTupleHeader *tuple, HLCTimestamp prune_horizon);
extern bool RecnoCanVacuumTimestamp(uint64 vacuum_ts);
extern bool RecnoCheckDeadlock(Relation rel, ItemPointer tid, LockTupleMode mode);
extern bool RecnoHoldsTupleLock(Relation rel, ItemPointer tid, LockTupleMode mode);
extern bool RecnoIsOverflowRecord(const void *item, Size item_len);
extern bool RecnoLockMultipleTuples(Relation rel, ItemPointerData *tids, int ntids, LockTupleMode mode, bool wait);
extern bool RecnoLockTuple(Relation rel, ItemPointer tid, LockTupleMode mode, bool wait, bool *have_tuple_lock);
extern void RecnoTrackSerializeLock(Oid dbid, RelFileNumber relnumber, BlockNumber blkno, OffsetNumber offnum);
extern void RecnoReleaseSerializeLocks(void);
extern void RecnoForgetSerializeLocks(void);
extern bool RecnoPageUpdateTuple(Page page, OffsetNumber offnum, RecnoTuple new_tuple, uint64 old_commit_ts, uint64 new_commit_ts);
extern bool RecnoTupleToSlot(RecnoTupleHeader *tuple_header, TupleTableSlot *slot);
extern bool RecnoTupleToSlotWithOverflow(RecnoTupleHeader *tuple_header, TupleTableSlot *slot, Relation rel);
extern bool RecnoTupleVisibleHLC(RecnoTupleHeader *tuple, HLCTimestamp snapshot_hlc, Oid relid, CommandId curcid, Buffer buffer);
extern bool RecnoTupleVisible(RecnoTupleHeader *tuple, uint64 snapshot_ts, uint64 xact_ts, Oid relid, CommandId curcid, Buffer buffer);
extern bool RecnoTupleVisibleToSnapshotDual(RecnoTupleHeader *tuple, Snapshot snapshot, Oid relid, Buffer buffer);
extern bool RecnoTupleHasCommittedUpdateAfter(Relation rel, const RecnoTupleHeader *tuple, Snapshot snapshot, TransactionId exclude_xid, TransactionId *out_head_xid, bool *out_inprogress);
extern bool RecnoTupleVisibleToSnapshot(RecnoTupleHeader *tuple, Snapshot snapshot, Oid relid, Buffer buffer);
extern bool RecnoReconstructVisibleVersion(Relation rel, ItemPointer tid,
										   const char *onpage_image, Size onpage_len,
										   Snapshot snapshot,
										   char **out_data, int *out_len);
extern bool RecnoTupleVisibleWithUncertainty(RecnoTupleHeader *tuple, HLCTimestamp snapshot_hlc, RecnoTransactionState * txn_state, Oid relid);
extern bool RecnoVMCheck(Relation rel, BlockNumber heapBlk, uint8 flags);
extern char * HLCToString(HLCTimestamp hlc);
extern Datum RecnoCompressAttribute(Datum value, Oid typid, RecnoCompressionType comp_type);
extern Datum RecnoDecompressAttribute(Datum value, Oid typid, RecnoCompressionHeader *header);
extern Datum RecnoFetchOverflowColumn(Relation rel, const void *overflow_varlena);
extern Datum RecnoFetchOverflow(Relation rel, RecnoOverflowRef *ref);
extern Datum RecnoStoreOverflowColumn(Relation rel, Datum value, int attnum, Size inline_prefix_size, RecnoOverflowBuffers * overflow_buffers);
extern HLCTimestamp HLCFromTimestampTz(TimestampTz ts);
extern HLCTimestamp HLCGetGlobal(void);
extern HLCTimestamp HLCMake(uint64 physical_ms, uint16 logical);
extern HLCTimestamp HLCNow(HLCTimestamp msg_hlc);
extern HLCTimestamp RecnoGetCommitHLC(HLCTimestamp msg_hlc);
extern HLCTimestamp RecnoGetDmlTimestamp(void);
extern HLCTimestamp RecnoGetOldestActiveHLC(void);
extern HLCTimestamp RecnoGetSnapshotHLC(Snapshot snapshot);
extern HLCTimestamp RecnoGetTransactionHLC(void);
extern int HLCCompare(HLCTimestamp a, HLCTimestamp b);
extern int RecnoCollectOverflowPtrs(RecnoTupleHeader *tuple_hdr, TupleDesc tupdesc, BlockNumber *blocks, OffsetNumber *offsets, int max_ptrs);
extern int RecnoPageGetLiveTuples(Page page, uint64 snapshot_ts);
extern int RecnoPagePruneOpt(Relation relation, Buffer buffer);
extern OffsetNumber RecnoPageAddTuple(Page page, RecnoTuple tuple, Size tuple_size);
extern RecnoOverflowRef * RecnoStoreOverflow(Relation rel, Datum value, int attnum);
extern RecnoPruneResult RecnoPruneDecision(RecnoTupleHeader *tuple, RecnoTupleHeader *newer_version, HLCTimestamp prune_horizon);
extern RecnoTimestampBound RecnoGetTimestampBounds(void);
extern RecnoTuple RecnoFormTupleFromSlot(TupleTableSlot *slot);
extern RecnoTuple RecnoFormTuple(TupleDesc tupdesc, Datum *values, bool *isnull, Relation rel, RecnoOverflowBuffers *overflow_buffers);
extern Size RecnoClockShmemSize(void);
extern Size RecnoComputeDataSize(TupleDesc tupdesc, Datum *values, bool *isnull);
extern Size RecnoComputeSlotSize(TupleTableSlot *slot);
extern Size RecnoHLCShmemSize(void);
extern Size RecnoMvccShmemSize(void);
extern Size RecnoVMGetPageSize(void);
extern TimestampTz HLCToTimestampTz(HLCTimestamp hlc);
extern TM_Result recno_tuple_delete(Relation relation, ItemPointer tid, CommandId cid, uint32 options, Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd);
extern TM_Result recno_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot, CommandId cid, uint32 options, Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd, LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes);
extern uint16 HLCGetLogical(HLCTimestamp hlc);
extern uint64 HLCGetPhysical(HLCTimestamp hlc);
extern uint64 RecnoGetCommitTimestamp(void);
extern uint64 RecnoGetOldestActiveTimestamp(void);
extern uint64 RecnoGetSnapshotTimestamp(Snapshot snapshot);
extern uint64 RecnoGetTransactionTimestamp(void);
extern void assign_recno_clock_check_interval(int newval, void *extra);
extern void assign_recno_enable_clock_bound(bool newval, void *extra);
extern void assign_recno_fatal_on_clock_drift(bool newval, void *extra);
extern void assign_recno_max_clock_offset(int newval, void *extra);
extern void assign_recno_node_id(int newval, void *extra);
extern void HLCGetDriftStats(uint64 *max_drift_ms, uint64 *total_backward_jumps, uint64 *total_overflow_events);
extern void HLCGetUncertaintyInterval(HLCTimestamp hlc, HLCTimestamp * lower, HLCTimestamp * upper);
extern void RecnoAbortTransaction(void);
extern void RecnoBatchDefrag(Relation rel, int max_pages);
extern void RecnoClockGetStats(RecnoClockStats * stats);
extern void RecnoClockMonitorMain(Datum main_arg);
extern void RecnoClockShmemInit(void);
extern void RecnoClockShutdown(void);
extern void RecnoClockStartMonitor(void);
extern void RecnoCollectRelationStats(Relation rel, RecnoRelationStats * stats);
extern void RecnoCheckForSerializableConflictOut(bool visible, Relation relation, RecnoTupleHeader *tuple, Buffer buffer, Snapshot snapshot);
extern void RecnoCommitTransaction(void);
extern void RecnoDeformTuple(RecnoTuple tuple, TupleDesc tupdesc, Datum *values, bool *isnull);
extern void RecnoDeleteOverflowChain(Relation rel, BlockNumber first_block, OffsetNumber first_offset);
extern void RecnoDeleteOverflow(Relation rel, RecnoOverflowRef *ref);
extern void RecnoDeleteTupleOverflows(Relation rel, RecnoTupleHeader *tuple_hdr, TupleDesc tupdesc);
extern void RecnoFreeTuple(RecnoTuple tuple);
extern void RecnoGetCompressionStats(int *dict_entries, int *total_compressed, double *avg_compression_ratio);
extern void RecnoGetFSMStats(Relation rel, int64 *total_pages, int64 *free_pages, double *avg_free_space, int64 *defrag_needed);
extern void RecnoGetMvccStats(uint64 *current_ts, uint64 *oldest_ts, int *active_xacts);
extern void RecnoGetOverflowStats(Relation rel, int64 *total_overflow_records, int64 *total_overflow_bytes, int64 *avg_chain_length);
extern void RecnoGetUpdateStats(int64 *in_place, int64 *out_of_place, int64 *defrag_triggered);
extern void RecnoHLCShmemInit(void);
extern void RecnoInitFSM(Relation rel);
extern void RecnoInitPage(Page page, Size pageSize);
extern void RecnoLockPage(Relation rel, BlockNumber blkno, LOCKMODE mode);
extern void RecnoLockRelationForDDL(Relation rel, LOCKMODE lockmode);
extern void RecnoLogRelationStats(Relation rel, const RecnoRelationStats * stats, int elevel);
extern void RecnoMarkPageForDefrag(Relation rel, BlockNumber page);
extern void recno_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples, CommandId cid, uint32 options, BulkInsertState bistate);
extern void RecnoMvccShmemInit(void);
extern void RecnoOpportunisticDefrag(Relation rel);
extern void RecnoPageDefragment(Page page);
extern void RecnoPageDeleteTuple(Page page, OffsetNumber offnum, uint64 commit_ts);
extern void RecnoPageIndexTupleDelete(Page page, OffsetNumber offnum);
extern void RecnoRecordFreeSpace(Relation rel, BlockNumber page, Size freespace);
extern void recno_relation_vacuum(Relation onerel, const VacuumParams *params, BufferAccessStrategy bstrategy);
extern void RecnoResetCompressionDict(void);
extern void RecnoSlotStoreMaterializedTuple(TupleTableSlot *slot, RecnoTupleHeader *tuple, uint32 tuple_len);
extern void RecnoSlotStoreTuple(TupleTableSlot *slot, RecnoTupleHeader *tuple, uint32 tuple_len, Buffer buffer);
extern void recno_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid, uint32 options, BulkInsertState bistate);
extern void RecnoUnlockPage(Relation rel, BlockNumber blkno, LOCKMODE mode);
extern void RecnoUnlockTuple(Relation rel, ItemPointer tid, LockTupleMode mode);
extern void RecnoUpdateOldestActiveTimestamp(void);
extern void RecnoVacuumFSM(Relation rel, BlockNumber new_nblocks);
extern void RecnoVacuumOverflowRecords(Relation rel);
extern void RecnoVMClear(Relation rel, BlockNumber heapBlk, Buffer heapBuf, uint8 flags);
extern void RecnoVMExtend(Relation rel, BlockNumber nheapblocks);
extern void RecnoVMInit(Relation rel);
extern void RecnoVMPinBuffer(Relation rel, BlockNumber heapBlk, Buffer *vmbuf);
extern void RecnoVMSet(Relation rel, BlockNumber heapBlk, Buffer heapBuf, uint8 flags);
extern void RecnoVMTruncate(Relation rel, BlockNumber nheapblocks);
extern void RecnoVMUpdateForDelete(Relation rel, Buffer buffer);
extern void RecnoVMUpdateForInsert(Relation rel, RecnoTupleHeader *tuple, Buffer buffer);
extern void RecnoVMUpdateForUpdate(Relation rel, Buffer buffer);
extern void RecnoVMVacuumPage(Relation rel, Buffer buffer, bool all_visible, bool all_frozen);
extern void RecnoWaitForClockBound(RecnoTimestampBound origin_bounds);

/* ------------------------------------------------------------------------
 * Handler
 * ------------------------------------------------------------------------ */
extern const TableAmRoutine *GetRecnoTableAmRoutine(void);

#endif							/* RECNO_H */
