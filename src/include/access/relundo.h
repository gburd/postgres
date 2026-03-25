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
	RELUNDO_TUPLE_LOCK = 4,		/* SELECT FOR UPDATE/SHARE */
	RELUNDO_DELTA_INSERT = 5		/* Partial-column update (delta) */
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
	RelUndoRecPtr urec_prevundorec;	/* Previous record in chain */
} RelUndoRecordHeader;

/* Size of the common UNDO record header */
#define SizeOfRelUndoRecordHeader \
	offsetof(RelUndoRecordHeader, urec_prevundorec) + sizeof(RelUndoRecPtr)

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
 * RELUNDO_DELTA_INSERT payload
 *
 * Records partial-column update (delta). For columnar storage implementations.
 */
typedef struct RelUndoDeltaInsertPayload
{
	ItemPointerData tid;		/* Target tuple TID */
	uint16		attnum;			/* Modified attribute number */
	uint16		delta_len;		/* Length of delta data */
	/* Delta data follows (variable length) */
} RelUndoDeltaInsertPayload;

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
typedef struct RelUndoMetaPageData
{
	uint32		magic;			/* RELUNDO_METAPAGE_MAGIC: validates that block
								 * 0 is actually a metapage */
	uint16		version;		/* Format version (currently 1); allows future
								 * on-disk format changes */
	uint16		counter;		/* Current generation counter; incremented
								 * when starting a new batch of records.
								 * Embedded in RelUndoRecPtr for O(1) age
								 * comparison. Wraps at 65536. */
	BlockNumber head_blkno;		/* Newest UNDO page (where new records are
								 * appended). InvalidBlockNumber if the chain
								 * is empty. */
	BlockNumber tail_blkno;		/* Oldest UNDO page (first to be discarded).
								 * InvalidBlockNumber if the chain is empty. */
	BlockNumber free_blkno;		/* Head of the free page list. Discarded pages
								 * are added here for reuse, avoiding fork
								 * extension. InvalidBlockNumber if no free
								 * pages. */
	uint64		total_records;	/* Cumulative count of all UNDO records ever
								 * created (monotonically increasing) */
	uint64		discarded_records;	/* Cumulative count of discarded records.
									 * (total - discarded) = live records. */
} RelUndoMetaPageData;

typedef RelUndoMetaPageData *RelUndoMetaPage;

/* Magic number for metapage validation */
#define RELUNDO_METAPAGE_MAGIC	0x4F56554D	/* "OVUM" */

/* Current metapage format version */
#define RELUNDO_METAPAGE_VERSION	1

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
 * The counter field stamps the page with its generation at creation time.
 * This enables page-granularity discard: if a page's counter precedes the
 * oldest visible counter, all records on that page are safe to discard.
 */
typedef struct RelUndoPageHeaderData
{
	BlockNumber prev_blkno;		/* Previous page in chain (toward tail).
								 * InvalidBlockNumber for the oldest page in
								 * the chain (the tail). */
	uint16		counter;		/* Generation counter at page creation. Used
								 * for discard eligibility checks. */
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

/* Allocate a new data page at the head of the chain */
extern BlockNumber relundo_allocate_page(Relation rel, Buffer metabuf,
										 Buffer *newbuf);

/* Initialize an UNDO data page */
extern void relundo_init_page(Page page, BlockNumber prev_blkno,
							  uint16 counter);

/* Get free space on an UNDO data page */
extern Size relundo_get_free_space(Page page);

/* Compare two counter values handling wraparound */
extern bool relundo_counter_precedes(uint16 counter1, uint16 counter2);

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
 * RelUndoGetCurrentCounter - Get current generation counter for a relation
 *
 * Returns the current generation counter from the relation's UNDO metapage.
 * Used for age comparison when determining visibility.
 *
 * Parameters:
 *   rel - Relation to query
 *
 * Returns:
 *   Current generation counter value
 */
extern uint16 RelUndoGetCurrentCounter(Relation rel);

/*
 * RelUndoDiscard - Discard old UNDO records
 *
 * Frees space occupied by UNDO records older than the specified counter.
 * Called during VACUUM to reclaim space.
 *
 * Parameters:
 *   rel                     - Relation to discard UNDO from
 *   oldest_visible_counter  - Counter value of oldest visible transaction
 *
 * All records with counter < oldest_visible_counter are eligible for discard.
 */
extern void RelUndoDiscard(Relation rel, uint16 oldest_visible_counter);

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
extern void RelUndoVacuum(Relation rel, TransactionId oldest_xmin);

#endif							/* RELUNDO_H */
