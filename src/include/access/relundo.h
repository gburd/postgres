/*-------------------------------------------------------------------------
 *
 * relundo.h
 *	  Per-relation UNDO for MVCC visibility determination
 *
 * This subsystem provides per-relation UNDO logging for table access methods
 * that need to determine tuple visibility by walking UNDO chains.
 * This is complementary to the existing cluster-wide UNDO system which is used
 * for transaction rollback.
 *
 * ARCHITECTURE:
 * -------------
 * Per-relation UNDO stores operation metadata (INSERT/DELETE/UPDATE/LOCK) within
 * each relation's UNDO fork, enabling MVCC visibility checks via UNDO chain walking.
 * Each UNDO record contains minimal metadata needed for visibility determination.
 *
 * This differs from cluster-wide UNDO which stores complete tuple data in shared
 * log files for physical transaction rollback. The two systems coexist independently:
 *
 *   Cluster-Wide UNDO (existing):  Transaction rollback, crash recovery
 *   Per-Relation UNDO (this file): MVCC visibility determination
 *
 * UNDO POINTER FORMAT:
 * -------------------
 * RelUndoRecPtr is a 64-bit pointer with three fields:
 *   Bits 0-15:   Offset within page (16 bits, max 64KB pages)
 *   Bits 16-47:  Block number (32 bits, max 4 billion blocks)
 *   Bits 48-63:  Counter (16 bits, wraps every 65536 generations)
 *
 * The counter enables fast age comparison without reading UNDO pages.
 *
 * USAGE PATTERN:
 * -------------
 * Table AMs that need per-relation UNDO follow this pattern:
 *
 *   1. RelUndoReserve() - Reserve space, pin buffer
 *   2. Perform DML operation (may fail)
 *   3. RelUndoFinish() - Write UNDO record, release buffer
 *      OR RelUndoCancel() - Release reservation on error
 *
 * Example:
 *   Buffer undo_buf;
 *   RelUndoRecPtr ptr = RelUndoReserve(rel, record_size, &undo_buf);
 *
 *   // Perform DML (may error out safely)
 *   InsertTuple(rel, tid);
 *
 *   // Commit UNDO record
 *   RelUndoFinish(rel, undo_buf, ptr, &header, payload, payload_size);
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/relundo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELUNDO_H
#define RELUNDO_H

#include "access/transam.h"
#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "storage/bufpage.h"
#include "storage/itemptr.h"
#include "storage/relfilelocator.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/*
 * RelUndoRecPtr: 64-bit pointer for per-relation UNDO records
 *
 * Layout:
 *   [63:48] Counter (16 bits)  - Generation counter for age comparison
 *   [47:16] BlockNum (32 bits) - Block number in relation UNDO fork
 *   [15:0]  Offset (16 bits)   - Byte offset within page
 */
typedef uint64 RelUndoRecPtr;

/* Invalid UNDO pointer constant */
#define InvalidRelUndoRecPtr		((RelUndoRecPtr) 0)

/* Check if pointer is valid */
#define RelUndoRecPtrIsValid(ptr) \
	((ptr) != InvalidRelUndoRecPtr)

/* Extract counter field (bits 63:48) */
#define RelUndoGetCounter(ptr) \
	((uint16)(((ptr) >> 48) & 0xFFFF))

/* Extract block number field (bits 47:16) */
#define RelUndoGetBlockNum(ptr) \
	((BlockNumber)(((ptr) >> 16) & 0xFFFFFFFF))

/* Extract offset field (bits 15:0) */
#define RelUndoGetOffset(ptr) \
	((uint16)((ptr) & 0xFFFF))

/* Construct UNDO pointer from components */
#define MakeRelUndoRecPtr(counter, blkno, offset) \
	((((uint64)(counter)) << 48) | (((uint64)(blkno)) << 16) | ((uint64)(offset)))

/*
 * Per-relation UNDO record types
 *
 * These record the operations needed for MVCC visibility determination.
 * Unlike cluster-wide UNDO (which stores complete tuples for rollback),
 * per-relation UNDO stores only operation metadata.
 */
typedef enum RelUndoRecordType
{
	RELUNDO_INSERT = 1,			/* Insertion record with TID range */
	RELUNDO_DELETE = 2,			/* Deletion (batched up to 50 TIDs) */
	RELUNDO_UPDATE = 3,			/* Update with old/new TID link */
	RELUNDO_TUPLE_LOCK = 4		/* SELECT FOR UPDATE/SHARE */
} RelUndoRecordType;

/*
 * Common header for all per-relation UNDO records
 *
 * Every UNDO record starts with this fixed-size header, followed by
 * type-specific payload data.
 */
typedef struct RelUndoRecordHeader
{
	uint16		urec_type;		/* RelUndoRecordType */
	uint16		urec_len;		/* Total length including header */
	TransactionId urec_xid;		/* Creating transaction ID */
	RelUndoRecPtr urec_prevundorec; /* Previous record in chain */

	/* Rollback support fields */
	uint16		info_flags;		/* Information flags (see below) */
	uint16		tuple_len;		/* Length of tuple data (0 if none) */
	/* Followed by type-specific payload + optional tuple data */
} RelUndoRecordHeader;

/* Size of the common UNDO record header */
#define SizeOfRelUndoRecordHeader \
	sizeof(RelUndoRecordHeader)

/*
 * RelUndoRecordHeader info_flags values
 *
 * These flags indicate what additional data is stored with the UNDO record
 * to support transaction rollback.
 */
#define RELUNDO_INFO_HAS_TUPLE		0x0001	/* Record contains complete tuple */
#define RELUNDO_INFO_HAS_CLR		0x0002	/* CLR pointer is valid */
#define RELUNDO_INFO_CLR_APPLIED	0x0004	/* CLR has been applied */
#define RELUNDO_INFO_PARTIAL_TUPLE	0x0008	/* Delta/partial tuple only */

/*
 * RELUNDO_INSERT payload
 *
 * Records insertion of a range of consecutive TIDs.
 */
typedef struct RelUndoInsertPayload
{
	ItemPointerData firsttid;	/* First inserted TID */
	ItemPointerData endtid;		/* Last inserted TID (inclusive) */
} RelUndoInsertPayload;

/*
 * RELUNDO_DELETE payload
 *
 * Records deletion of up to 50 TIDs (batched for efficiency).
 */
#define RELUNDO_DELETE_MAX_TIDS 50

typedef struct RelUndoDeletePayload
{
	uint16		ntids;			/* Number of TIDs in this record */
	ItemPointerData tids[RELUNDO_DELETE_MAX_TIDS];
} RelUndoDeletePayload;

/*
 * RELUNDO_UPDATE payload
 *
 * Records update operation linking old and new tuple versions.
 */
typedef struct RelUndoUpdatePayload
{
	ItemPointerData oldtid;		/* Old tuple TID */
	ItemPointerData newtid;		/* New tuple TID */
	/* Optional: column bitmap for partial updates could be added here */
} RelUndoUpdatePayload;

/*
 * RELUNDO_TUPLE_LOCK payload
 *
 * Records tuple lock (SELECT FOR UPDATE/SHARE).
 */
typedef struct RelUndoTupleLockPayload
{
	ItemPointerData tid;		/* Locked tuple TID */
	uint16		lock_mode;		/* LockTupleMode */
} RelUndoTupleLockPayload;

/*
 * Per-relation UNDO metapage structure
 *
 * Stored at block 0 of the relation's UNDO fork. Tracks the head/tail
 * of the UNDO page chain and the current generation counter.
 *
 * The metapage is the root of all per-relation UNDO state. It is read
 * and updated during Reserve (to find the head page), Discard (to advance
 * the tail), and Init (to set up an empty chain). All metapage modifications
 * must be WAL-logged for crash safety.
 *
 * Memory layout is designed for 8-byte alignment of the 64-bit fields.
 */
/*
 * Number of independent append points ("head slots") in a relation's UNDO
 * fork.  Every committed CAS UPDATE appends its before-image to the head page
 * of one slot; a backend hashes to slot (MyProcNumber % RELUNDO_NUM_HEADS) so
 * that concurrent writers contend on RELUNDO_NUM_HEADS distinct tail pages
 * instead of one.
 *
 * Originally sized to match the common WAL's NUM_XLOGINSERT_LOCKS (8).
 * Raised to 16 after Plan B EC2 benchmarking showed cached-tpcb throughput
 * negative-scaling past ~64-96 concurrent writers on a 96-core host: at
 * c=192, MyProcNumber % 8 puts ~24 backends on each slot's exclusive
 * RelUndoReserve() buffer lock, cluster-wide (this is shared by every
 * in-place-update table's writers, not scoped per relation).  Doubling to 16 halves that
 * per-slot contention (~12 backends/slot at c=192) at a bounded cost:
 * RelUndoDiscard()/RelUndoDiscardSlot() walk all RELUNDO_NUM_HEADS slots
 * under the metapage's exclusive lock on every VACUUM and on every
 * RelUndoMaybeVacuum() throttled backstop call (relundo.c), so raising this
 * further multiplies that discard-side cost without EC2-scale concurrency
 * data to justify it; 16 was chosen as the largest change defensible from
 * local (20-thread) validation alone.  Revisit with real high-core-count A/B
 * data before raising further.
 *
 * The per-txn rollback chain threads through urec_prevundorec and the reader
 * keys off the physical (counter, blkno, offset) triple, so neither cares
 * which slot a record landed on; striping is transparent to both.
 *
 * Changing this value changes the on-disk RelUndoMetaPageData layout
 * (head_blkno[]/tail_blkno[] array size) -- see RELUNDO_METAPAGE_VERSION.
 */
#define RELUNDO_NUM_HEADS	16

typedef struct RelUndoMetaPageData
{
	uint32		magic;			/* RELUNDO_METAPAGE_MAGIC: validates that
								 * block 0 is actually a metapage */
	uint16		version;		/* Format version (currently 3); allows future
								 * on-disk format changes */
	uint16		counter;		/* Current generation counter; incremented
								 * when starting a new batch of records.
								 * Embedded in RelUndoRecPtr for O(1) age
								 * comparison. Wraps at 65536. */
	BlockNumber head_blkno[RELUNDO_NUM_HEADS];	/* Newest UNDO page per slot
												 * (where new records are
												 * appended). InvalidBlockNumber
												 * if that slot's chain is
												 * empty. */
	BlockNumber tail_blkno[RELUNDO_NUM_HEADS];	/* Oldest UNDO page per slot
												 * (first to be discarded).
												 * InvalidBlockNumber if that
												 * slot's chain is empty. */
	BlockNumber free_blkno;		/* Head of the free page list. Pages discarded
								 * by VACUUM are spliced here for reuse,
								 * avoiding fork extension. Shared across all
								 * slots. InvalidBlockNumber if no free pages. */
	uint64		total_records;	/* Cumulative count of all UNDO records ever
								 * created (monotonically increasing) */
	uint64		discarded_records;	/* Cumulative count of discarded records.
									 * (total - discarded) = live records. */
	BlockNumber system_alloc_watermark;	/* High-water mark of system-allocated
										 * pages. Tracks the highest block
										 * number allocated via system
										 * transaction, enabling efficient
										 * reclamation of unused pages. */
} RelUndoMetaPageData;

typedef RelUndoMetaPageData *RelUndoMetaPage;

/* Magic number for metapage validation */
#define RELUNDO_METAPAGE_MAGIC	0x4F56554D	/* "OVUM" */

/*
 * Block number of the metapage within the UNDO fork.  Data pages are block
 * >= 1; a chain link is InvalidBlockNumber at the tail and must NEVER be this
 * block.  Chain walkers use it to avoid re-locking the metapage (which the
 * caller typically already holds).
 */
#define RELUNDO_METAPAGE_BLKNO	((BlockNumber) 0)

/* Current metapage format version */
#define RELUNDO_METAPAGE_VERSION	4

/*
 * Advance a freshly PageInit'd metapage's pd_lower to cover the
 * RelUndoMetaPageData struct that lives in the page contents area.
 *
 * The metapage keeps all of its state in PageGetContents(page) rather than in
 * line pointers, so a bare PageInit leaves pd_lower at the empty-page value and
 * the entire meta struct sits inside the "hole" [pd_lower, pd_upper).  A
 * REGBUF_STANDARD full-page image elides that hole, so the FPI would carry a
 * valid header and zeroed contents; a standby (or crash recovery) restoring it
 * reconstructs a metapage with magic 0x0.  Growing pd_lower past the struct
 * makes the meta fields part of the FPI's recorded region.  Mirrors nbtree's
 * _bt_initmetapage.  Call after PageInit and after populating the fields.
 */
static inline void
RelUndoMetaPageSetPdLower(Page page)
{
	((PageHeader) page)->pd_lower =
		((char *) PageGetContents(page) + sizeof(RelUndoMetaPageData))
		- (char *) page;
}

/*
 * Per-relation UNDO data page header
 *
 * Each UNDO data page (block >= 1) starts with this header.
 * Pages are linked in a singly-linked chain from head to tail via prev_blkno.
 *
 * Records are appended starting at pd_lower and grow toward pd_upper.
 * Free space is [pd_lower, pd_upper). When pd_lower >= pd_upper, the page
 * is full and a new page must be allocated.
 *
 * The max_xid field tracks the largest urec_xid of any record on the page.
 * This drives page-granularity discard: a page is reclaimable once max_xid
 * precedes the relation's oldest non-removable xid (oldest_xmin), since no
 * active transaction can then need any record on the page for rollback.
 *
 * The counter field stamps the page with its generation at creation time.
 * It is retained for record-pointer addressing (RelUndoRecPtr) but is no
 * longer used for discard eligibility.
 */
typedef struct RelUndoPageHeaderData
{
	BlockNumber prev_blkno;		/* Previous page in chain (toward tail).
								 * InvalidBlockNumber for the oldest page in
								 * the chain (the tail). */
	TransactionId max_xid;		/* Largest urec_xid of any record on this page.
								 * InvalidTransactionId on an empty page. Used
								 * for discard eligibility checks. */
	uint16		counter;		/* Generation counter at page creation. Used
								 * for record-pointer addressing. */
	uint16		pd_lower;		/* Byte offset of next record insertion point
								 * (grows upward from header). */
	uint16		pd_upper;		/* Byte offset of end of usable space
								 * (typically BLCKSZ). */
} RelUndoPageHeaderData;

typedef RelUndoPageHeaderData *RelUndoPageHeader;

/* Size of UNDO page header */
#define SizeOfRelUndoPageHeaderData (sizeof(RelUndoPageHeaderData))

/* Maximum free space in an UNDO data page */
#define RelUndoPageMaxFreeSpace \
	(BLCKSZ - SizeOfRelUndoPageHeaderData)

/*
 * Internal page management functions (used by relundo.c and relundo_discard.c)
 * =============================================================================
 */

/* Read and pin the metapage (block 0) of the UNDO fork */
extern Buffer relundo_get_metapage(Relation rel, int mode);

/* Allocate a new data page at the head of the given slot's chain */
extern BlockNumber relundo_allocate_page(Relation rel, Buffer metabuf,
										 int slot, Buffer *newbuf);

/* Initialize an UNDO data page */
extern void relundo_init_page(Page page, BlockNumber prev_blkno,
							  uint16 counter);

/* Get free space on an UNDO data page */
extern Size relundo_get_free_space(Page page);

/*
 * Public API for table access methods
 * ====================================
 */

/*
 * RelUndoReserve - Reserve space for an UNDO record (Phase 1 of 2-phase insert)
 *
 * Reserves space in the relation's UNDO log and pins the buffer. The caller
 * should then perform the DML operation, and finally call RelUndoFinish() to
 * commit the UNDO record or RelUndoCancel() to release the reservation.
 *
 * Parameters:
 *   rel          - Relation to insert UNDO record into
 *   record_size  - Total size of UNDO record (header + payload)
 *   undo_buffer  - (output) Buffer containing the reserved space
 *
 * Returns:
 *   RelUndoRecPtr pointing to the reserved space
 *
 * The returned buffer is pinned and locked (exclusive). Caller must eventually
 * call RelUndoFinish() or RelUndoCancel().
 */
extern RelUndoRecPtr RelUndoReserve(Relation rel, Size record_size,
									Buffer *undo_buffer);

/*
 * RelUndoStageResult - facts produced by RelUndoStage() for a deferred WAL emit
 *
 * RelUndoStage() writes the UNDO record onto the reserved page and dirties the
 * buffers but performs NO WAL insert.  The caller then either lets RelUndoFinish
 * emit the standalone RM_RELUNDO_ID record, or folds these staged bytes into a
 * different resource manager's combined record (the caller's WAL-fold path).  The undo and
 * metapage buffers remain locked+pinned; the caller is responsible for
 * PageSetLSN on them under its critical section and for releasing them.
 */
typedef struct RelUndoStageResult
{
	Buffer		undo_buffer;	/* reserved data-page buffer (still locked) */
	Buffer		metabuf;		/* metapage buffer if is_new_page, else Invalid */
	bool		is_new_page;	/* first record on a freshly allocated page */
	uint8		urec_type;		/* header->urec_type (for the xlrec) */
	uint16		urec_len;		/* header->urec_len (for the xlrec) */
	uint16		page_offset;	/* page-absolute offset of the record */
	uint16		new_pd_lower;	/* shadow pd_lower after the write */
	TransactionId max_xid;		/* page max_xid watermark after the bump */
	char	   *wal_record_data;	/* palloc'd block-0 data (caller pfrees) */
	Size		wal_record_size;	/* size of wal_record_data */
} RelUndoStageResult;

/*
 * RelUndoStage - write an UNDO record onto its reserved page WITHOUT WAL.
 *
 * Performs every page mutation RelUndoFinish() does (header+payload memcpy,
 * max_xid bump, MarkBufferDirty on the data page and, for a new page, the
 * metapage) and builds the block-0 WAL data buffer, but does NOT open a
 * critical section, XLogInsert, PageSetLSN, or release any buffer.  The staged
 * facts are returned in *result so the caller can emit the WAL record itself.
 */
extern void RelUndoStage(Relation rel, Buffer undo_buffer, RelUndoRecPtr ptr,
						 const RelUndoRecordHeader *header,
						 const void *payload, Size payload_size,
						 RelUndoStageResult *result);

/*
 * RelUndoFinish - Complete UNDO record insertion (Phase 2 of 2-phase insert)
 *
 * Writes the UNDO record to the previously reserved space and releases the buffer.
 * This must be called after successful DML operation completion.
 *
 * Parameters:
 *   rel           - Relation containing the UNDO log
 *   undo_buffer   - Buffer from RelUndoReserve() (will be unlocked/unpinned)
 *   ptr           - RelUndoRecPtr from RelUndoReserve()
 *   header        - UNDO record header to write
 *   payload       - UNDO record payload data
 *   payload_size  - Size of payload data
 *
 * The buffer is marked dirty, WAL-logged, and released.
 */
extern void RelUndoFinish(Relation rel, Buffer undo_buffer,
						  RelUndoRecPtr ptr,
						  const RelUndoRecordHeader *header,
						  const void *payload, Size payload_size);

/*
 * RelUndoFinishWithTuple - Complete UNDO record insertion with tuple data
 *
 * Like RelUndoFinish(), but also writes tuple data after the payload for
 * operations that need to store the complete tuple (DELETE, UPDATE).
 *
 * Parameters:
 *   rel           - Relation containing the UNDO log
 *   undo_buffer   - Buffer from RelUndoReserve() (will be unlocked/unpinned)
 *   ptr           - RelUndoRecPtr from RelUndoReserve()
 *   header        - UNDO record header (must have RELUNDO_INFO_HAS_TUPLE set)
 *   payload       - UNDO record payload data
 *   payload_size  - Size of payload data
 *   tuple_data    - Complete tuple data to store
 *   tuple_len     - Length of tuple data
 *
 * The record layout on the UNDO page is:
 *   [RelUndoRecordHeader][payload][tuple_data]
 */
extern void RelUndoFinishWithTuple(Relation rel, Buffer undo_buffer,
								   RelUndoRecPtr ptr,
								   const RelUndoRecordHeader *header,
								   const void *payload, Size payload_size,
								   const char *tuple_data, uint32 tuple_len);

/*
 * RelUndoCancel - Cancel UNDO record reservation
 *
 * Releases a reservation made by RelUndoReserve() without writing an UNDO record.
 * Use this when the DML operation fails and needs to be rolled back.
 *
 * Parameters:
 *   rel          - Relation containing the UNDO log
 *   undo_buffer  - Buffer from RelUndoReserve() (will be unlocked/unpinned)
 *   ptr          - RelUndoRecPtr from RelUndoReserve()
 *
 * The reserved space is left as a "hole" that can be skipped during chain walking.
 */
extern void RelUndoCancel(Relation rel, Buffer undo_buffer, RelUndoRecPtr ptr);

/*
 * RelUndoReadRecord - Read an UNDO record
 *
 * Reads an UNDO record at the specified pointer and returns the header and payload.
 *
 * Parameters:
 *   rel           - Relation containing the UNDO log
 *   ptr           - RelUndoRecPtr to read from
 *   header        - (output) UNDO record header
 *   payload       - (output) Allocated payload buffer (caller must pfree)
 *   payload_size  - (output) Size of payload
 *
 * Returns:
 *   true if record was successfully read, false if pointer is invalid or
 *   record has been discarded
 *
 * If successful, *payload is allocated in CurrentMemoryContext and must be
 * freed by the caller.
 */
extern bool RelUndoReadRecord(Relation rel, RelUndoRecPtr ptr,
							  RelUndoRecordHeader *header,
							  void **payload, Size *payload_size);

/*
 * RelUndoReadRecordHeader - Read only the header of an UNDO record.
 *
 * Header-only variant that skips the payload palloc.  Used by hot probes
 * that need only urec_xid (e.g. the lost-update conflict probe).  Returns
 * false with the same semantics as RelUndoReadRecord.
 */
extern bool RelUndoReadRecordHeader(Relation rel, RelUndoRecPtr ptr,
									RelUndoRecordHeader *header);

/*
 * RelUndoDiscard - Discard old UNDO records
 *
 * Frees space occupied by UNDO records that no active transaction can still
 * need for rollback.  Called during VACUUM to reclaim space.
 *
 * Parameters:
 *   rel          - Relation to discard UNDO from
 *   oldest_xmin  - Oldest non-removable XID for this relation
 *
 * A page is discardable iff its max_xid precedes oldest_xmin, meaning every
 * record on the page belongs to a transaction that has already committed or
 * aborted and is older than any active snapshot.
 */
extern void RelUndoDiscard(Relation rel, TransactionId oldest_xmin, bool nowait);

/*
 * RelUndoHeadCacheInvalidate - drop the per-backend head page cache entry.
 *
 * Must be called by RelUndoDiscard() after it reclaims pages, since discard
 * can physically truncate the fork and leave the cached head block number
 * pointing past the new end of file.
 */
extern void RelUndoHeadCacheInvalidate(Oid relid);

/*
 * RelUndoInitRelation - Initialize per-relation UNDO for a new relation
 *
 * Creates the UNDO fork and initializes the metapage. Called during CREATE TABLE
 * for table AMs that use per-relation UNDO.
 *
 * Parameters:
 *   rel - Relation to initialize
 */
extern void RelUndoInitRelation(Relation rel);

/*
 * RelUndoDropRelation - Drop per-relation UNDO when relation is dropped
 *
 * Removes the UNDO fork. Called during DROP TABLE for table AMs that use
 * per-relation UNDO.
 *
 * Parameters:
 *   rel - Relation being dropped
 */
extern void RelUndoDropRelation(Relation rel);

/*
 * RelUndoVacuum - Vacuum per-relation UNDO log
 *
 * Performs maintenance on the UNDO log: discards old records, reclaims space,
 * and updates statistics. Called during VACUUM.
 *
 * Parameters:
 *   rel           - Relation to vacuum
 *   oldest_xmin   - Oldest XID still visible to any transaction
 */
extern void RelUndoVacuum(Relation rel, TransactionId oldest_xmin, bool nowait);

/*
 * RelUndoMaybeVacuum - Throttled, self-clocking per-relation UNDO fork
 * discard, decoupled from VACUUM.
 *
 * An in-place-update AM can correctly report near-zero dead tuples,
 * so autovacuum's dead-tuple/insert-count thresholds may never fire even
 * under sustained write churn, and RelUndoVacuum() above -- the only code
 * that discards the on-disk UNDO fork -- is normally reached exclusively via
 * VACUUM.  Call this from the owning AM's DML hot path (after releasing all
 * buffer/tuple locks -- see relundo.c for why) as a backstop so the fork
 * gets discarded on its own schedule.  A no-op most calls (throttled to once
 * every 5 seconds per backend, and skipped entirely below a minimum fork
 * size), so it is safe to call unconditionally on every DML operation.
 *
 * Parameters:
 *   rel - Relation whose UNDO fork may need discarding
 */
extern void RelUndoMaybeVacuum(Relation rel);

/*
 * =============================================================================
 * ROLLBACK API - Support for transaction abort via UNDO application
 * =============================================================================
 */

/*
 * RelUndoApplyChain - Walk and apply per-relation UNDO chain for rollback
 *
 * Walks backwards through the UNDO chain applying each operation to restore
 * the database state. Called during transaction abort.
 */
extern void RelUndoApplyChain(Relation rel, RelUndoRecPtr start_ptr);

/*
 * RelUndoApplyRecordForRecovery - Reverse-apply one UNDO record during crash
 * recovery, restoring the before-image in place without writing a CLR.
 */
extern void RelUndoApplyRecordForRecovery(Relation rel, RelUndoRecPtr ptr);

/* Read UNDO record including tuple data for rollback */
extern RelUndoRecordHeader *RelUndoReadRecordWithTuple(Relation rel,
													   RelUndoRecPtr ptr,
													   char **tuple_data_out,
													   uint32 *tuple_len_out);

/*
 * =============================================================================
 * CRASH-RECOVERY API - Reverse-apply per-relation UNDO for loser transactions
 * =============================================================================
 *
 * An in-place MVCC table AM overwrites the committed tuple
 * bytes on UPDATE, and the only durable copy of the prior committed version
 * is the before-image in the relation's UNDO fork.  If a transaction does an
 * in-place modification and the server crashes before it commits, redo
 * re-establishes the uncommitted page state, so a recovery-time driver must
 * reverse-apply the before-images of every incomplete (loser) transaction.
 *
 * After the redo pass finishes, CLOG is fully reconstructed, so
 * PerformRelUndoRecovery() scans every per-relation UNDO fork on disk and,
 * for each record whose creating transaction did not commit and is not a
 * prepared transaction, reverse-applies its before-image.  See
 * relundo_recovery.c for why an end-of-recovery fork scan is required rather
 * than tracking insertions during redo.
 */

/* Scan UNDO forks and reverse-apply all loser transactions' before-images. */
extern void PerformRelUndoRecovery(void);

/*
 * =============================================================================
 * AM-NEUTRAL HOOKS - decouple UNDO core from any specific table AM
 * =============================================================================
 *
 * The per-relation UNDO core (slog.c, relundo_apply.c, undoworker.c) is
 * format-agnostic: it stores and restores opaque tuple bytes without knowing
 * the in-place table AM's tuple layout.  The operations that do require
 * AM-specific knowledge are reached through these function pointers, which the
 * owning table AM installs at subsystem init.  When no in-place AM is
 * registered the pointers stay NULL and the core degrades gracefully (no
 * transient-flag clearing).
 *
 * RelUndoClearTransientFlags_hook: clear the AM's per-tuple transient state
 *   bits (uncommitted/deleted/updated) on a freshly restored before-image, so
 *   the restored committed version is not mistaken for an in-flight change.
 *
 * RelUndoAbortCleanup_hook: called after a rolled-back transaction's
 *   before-images have been physically restored (inline in xactundo.c, or by
 *   the background worker in relundo_worker.c), so the AM can drop any
 *   transient bookkeeping entries it kept ONLY for as long as the abort was
 *   pending (e.g. dirty-xid write-write-conflict markers).  xid is the
 *   transaction that was rolled back.  NULL is a valid no-op for an AM with
 *   no such transient state.
 *
 * RelUndoDiscardRetained_hook: called periodically by the UNDO discard worker
 *   (undoworker.c) so the AM can reclaim any retained-version bookkeeping
 *   (before-images, dirty markers) that has aged out.  The reclamation
 *   horizon is the AM's own xid horizon.  NULL is a valid no-op.
 */
extern void (*RelUndoClearTransientFlags_hook) (char *tuple_data);
extern void (*RelUndoAbortCleanup_hook) (TransactionId xid);
extern void (*RelUndoDiscardRetained_hook) (void);

#endif							/* RELUNDO_H */
