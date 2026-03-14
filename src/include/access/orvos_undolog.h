/**
 * @file orvos_undolog.h
 * @brief UNDO log management for Orvos columnar storage.
 *
 * Orvos uses a per-relation UNDO log to implement MVCC.  The UNDO log
 * is stored as a singly-linked list of pages within the same relation
 * file.  Each UNDO record captures the state needed to undo or
 * determine the visibility of a DML operation.
 *
 * @par UNDO Record Lifecycle
 * 1. Space is reserved with ovundo_insert_reserve() (acquires UNDO page lock).
 * 2. The caller prepares the B-tree modifications (may abort if split needed).
 * 3. ovundo_insert_finish() writes the record into the reserved space.
 * 4. Old records are discarded by ovundo_discard() when no snapshot needs them.
 *
 * @par UNDO Pointer Format
 * An OVUndoRecPtr contains a monotonic counter, a block number, and an
 * offset within the page.  Two special values are defined:
 * - InvalidUndoPtr (counter=0): tuple visible to everyone.
 * - DeadUndoPtr (counter=1): tuple dead, not visible to anyone.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/access/orvos_undolog.h
 */
#ifndef ORVOS_UNDOLOG_H
#define ORVOS_UNDOLOG_H

#include "c.h"					/* for uint64, uint32, int32, etc. */

#include "access/xlogreader.h"
#include "storage/block.h"
#include "storage/buf.h"
#include "utils/relcache.h"

/**
 * @brief Maximum size of a single UNDO record.
 *
 * Set to BLCKSZ/2 to guarantee at least two records per page,
 * simplifying page management.  Most records are much smaller.
 */
#define MaxUndoRecordSize		(BLCKSZ / 2)

/**
 * @brief UNDO record pointer.
 *
 * Identifies an UNDO record by its physical location (blkno + offset)
 * and a monotonically increasing counter for age comparison.
 *
 * @param counter  Sequence number; higher = newer.  Used for age checks.
 * @param blkno    Physical block containing the UNDO record.
 * @param offset   Byte offset within the UNDO page.
 */
typedef struct
{
	uint64		counter;
	BlockNumber blkno;
	int32		offset;			/* int16 would suffice, but avoid padding */
} OVUndoRecPtr;

/** @brief Test whether two UNDO pointers refer to the same record. */
#define OVUndoRecPtrEquals(a, b) ((a).counter == (b).counter)

/**
 * @brief Opaque area for UNDO log pages.
 *
 * UNDO pages form a singly-linked list (head = oldest, tail = newest).
 *
 * @param next              Block number of the next UNDO page.
 * @param first_undorecptr  Pointer to the first record on this page.
 * @param last_undorecptr   Pointer to the last record on this page.
 * @param ov_page_id        Always OV_UNDO_PAGE_ID (0xF085).
 */
typedef struct
{
	BlockNumber next;
	OVUndoRecPtr first_undorecptr;	/* note: this is set even if the page is
									 * empty! */
	OVUndoRecPtr last_undorecptr;
	uint16		padding0;
	uint16		padding1;
	uint16		padding2;
	uint16		ov_page_id;		/* OV_UNDO_PAGE_ID */
} OVUndoPageOpaque;

/**
 * @brief Invalid UNDO pointer: counter=0, meaning tuple is visible to everyone.
 *
 * Compares less than any real UNDO pointer, so tuples with this pointer
 * are unconditionally visible.
 */
#define InvalidUndoPtr ((OVUndoRecPtr){.counter = 0, .blkno = InvalidBlockNumber, .offset = 0})

/**
 * @brief Dead UNDO pointer: counter=1, meaning tuple is not visible to anyone.
 *
 * Used in TID items to mark dead tuples awaiting VACUUM cleanup.
 */
#define DeadUndoPtr ((OVUndoRecPtr){.counter = 1, .blkno = InvalidBlockNumber, .offset = 0})

/**
 * @brief Check whether an UNDO pointer is valid (non-zero counter).
 * @param uptr  Pointer to the OVUndoRecPtr to check.
 * @return true if the pointer has a non-zero counter.
 */
static inline bool
IsOVUndoRecPtrValid(OVUndoRecPtr *uptr)
{
	return uptr->counter != 0;
}

/**
 * @brief Reservation for an UNDO record insertion.
 *
 * Created by ovundo_insert_reserve() to hold a locked UNDO page buffer
 * and the location where the new record will be written.  The caller
 * fills in the record and calls ovundo_insert_finish() to commit it.
 *
 * @param undobuf    Pinned and locked UNDO page buffer.
 * @param undorecptr Location of the reserved space.
 * @param length     Size of the reservation in bytes.
 * @param ptr        Direct pointer into the buffer's page content.
 */
typedef struct
{
	Buffer		undobuf;
	OVUndoRecPtr undorecptr;
	size_t		length;

	char	   *ptr;
}			ov_undo_reservation;

/**
 * @brief Reserve space for a new UNDO record.
 *
 * May extend the UNDO log by allocating a new page.  On return, the
 * reservation holds a locked buffer and a write pointer.
 *
 * @param rel            The Orvos relation.
 * @param size           Size of the UNDO record to insert.
 * @param reservation_p  Output: the reservation details.
 */
extern void ovundo_insert_reserve(Relation rel, size_t size, ov_undo_reservation * reservation_p);

/**
 * @brief Finalize an UNDO record insertion.
 *
 * Marks the reserved space as occupied and releases the UNDO page lock.
 *
 * @param reservation  The reservation from ovundo_insert_reserve().
 */
extern void ovundo_insert_finish(ov_undo_reservation * reservation);

/**
 * @brief Fetch an UNDO record by pointer.
 *
 * @param rel         The Orvos relation.
 * @param undoptr     The UNDO record pointer to fetch.
 * @param buf_p       Output: the buffer containing the record (locked).
 * @param lockmode    Buffer lock mode (BUFFER_LOCK_SHARE or BUFFER_LOCK_EXCLUSIVE).
 * @param missing_ok  If true, return NULL for expired records instead of ERROR.
 * @return Pointer to the record within the locked buffer, or NULL if missing.
 */
extern char *ovundo_fetch(Relation rel, OVUndoRecPtr undoptr, Buffer *buf_p, int lockmode, bool missing_ok);

/**
 * @brief Discard UNDO records older than @a oldest_undorecptr.
 *
 * Frees UNDO pages that contain only expired records, adding them to the
 * Free Page Map.  Updates the metapage's ov_undo_oldestptr.
 *
 * @param rel                The Orvos relation.
 * @param oldest_undorecptr  Oldest UNDO record still needed by any snapshot.
 */
extern void ovundo_discard(Relation rel, OVUndoRecPtr oldest_undorecptr);

/** @brief WAL redo handler for UNDO log page creation. */
extern void ovundo_newpage_redo(XLogReaderState *record);
/** @brief WAL redo handler for UNDO log discard. */
extern void ovundo_discard_redo(XLogReaderState *record);

#endif							/* ORVOS_UNDOLOG_H */
