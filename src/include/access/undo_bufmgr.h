/*-------------------------------------------------------------------------
 *
 * undo_bufmgr.h
 *	  UNDO log buffer manager using PostgreSQL's shared_buffers
 *
 * This module provides buffer management for UNDO log blocks by mapping
 * them into PostgreSQL's standard shared buffer pool using virtual
 * RelFileLocator entries.  This approach follows ZHeap's design where
 * undo data is "accessed through the buffer pool ... similar to regular
 * relation data" (ZHeap README).
 *
 * Each undo log is mapped to a virtual relation:
 *
 *   RelFileLocator = {
 *     spcOid   = UNDO_DEFAULT_TABLESPACE_OID (pg_default, 1663)
 *     dbOid    = UNDO_DB_OID (pseudo-database 9, following ZHeap)
 *     relNumber = log_number (undo log number as RelFileNumber)
 *   }
 *
 * Buffers are read/written via ReadBufferWithoutRelcache() using
 * MAIN_FORKNUM (following ZHeap's UndoLogForkNum convention), and
 * the standard buffer manager handles all caching, clock-sweep
 * eviction, dirty tracking, and checkpoint write-back.
 *
 * Undo buffers are distinguished from regular relation buffers by
 * the UNDO_DB_OID in the dbOid field of the RelFileLocator / BufferTag.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undo_bufmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDO_BUFMGR_H
#define UNDO_BUFMGR_H

#include "storage/block.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/relfilelocator.h"

/*
 * Pseudo-database OID used for undo log relations in the buffer pool.
 * This matches ZHeap's UndoLogDatabaseOid convention.  This OID must not
 * collide with any real database OID; value 9 is reserved for this purpose.
 */
#define UNDO_DB_OID				9

/*
 * Default tablespace OID for undo log buffers.  This matches the
 * pg_default tablespace (OID 1663 from pg_tablespace.dat).
 * Eventually per-tablespace undo logs may be supported, but for now
 * all undo data uses the default tablespace.
 */
#define UNDO_DEFAULT_TABLESPACE_OID		1663

/*
 * Fork number used for undo log buffers in the shared buffer pool.
 *
 * Following ZHeap's convention (UndoLogForkNum = MAIN_FORKNUM), we use
 * MAIN_FORKNUM for undo log buffer operations.  Undo buffers are
 * distinguished from regular relation data by the UNDO_DB_OID in the
 * dbOid field of the BufferTag, not by a special fork number.
 *
 * Using MAIN_FORKNUM is necessary because the smgr layer sizes internal
 * arrays to MAX_FORKNUM+1 entries.  A fork number beyond that range
 * would cause out-of-bounds accesses in smgr_cached_nblocks[] and
 * similar arrays.
 */
#define UndoLogForkNum	MAIN_FORKNUM

/*
 * UNDO_FORKNUM is reserved for future use when the smgr layer is
 * extended to support undo-specific file management (Task #5).
 * It is defined in buf_internals.h as a constant but not currently
 * used in buffer operations.
 */


/* ----------------------------------------------------------------
 *		Undo log to RelFileLocator mapping
 * ----------------------------------------------------------------
 */

/*
 * UndoLogGetRelFileLocator
 *		Build a virtual RelFileLocator for an undo log number.
 *
 * This mapping allows the standard buffer manager to identify undo log
 * blocks using its existing BufferTag infrastructure.  The resulting
 * RelFileLocator does not correspond to any entry in pg_class; it is
 * purely a buffer-pool-internal identifier.
 *
 * Parameters:
 *   log_number - the undo log number (0..16M)
 *   rlocator   - output RelFileLocator to populate
 */
static inline void
UndoLogGetRelFileLocator(uint32 log_number, RelFileLocator *rlocator)
{
	rlocator->spcOid = UNDO_DEFAULT_TABLESPACE_OID;
	rlocator->dbOid = UNDO_DB_OID;
	rlocator->relNumber = (RelFileNumber) log_number;
}

/*
 * IsUndoRelFileLocator
 *		Check whether a RelFileLocator refers to an undo log.
 *
 * This is useful for code that needs to distinguish undo log locators
 * from regular relation locators (e.g., in smgr dispatch, checkpoint
 * logic, or buffer tag inspection).
 */
static inline bool
IsUndoRelFileLocator(const RelFileLocator *rlocator)
{
	return (rlocator->dbOid == UNDO_DB_OID);
}

/*
 * UNDO page layout
 *
 * UNDO pages stored in shared_buffers use standard PostgreSQL page headers
 * (PageHeaderData) to support checksums, LSN tracking, and the buffer
 * manager's page verification.  UNDO record data starts immediately
 * after the page header.
 *
 * The usable bytes per page is BLCKSZ minus the page header size.
 * All UNDO byte offsets (in UndoRecPtr) are "logical" offsets — they
 * represent a contiguous byte stream of UNDO data.  The mapping from
 * logical offset to physical (block, page_offset) is handled by the
 * macros below.
 */
#define UNDO_USABLE_BYTES_PER_PAGE	(BLCKSZ - (int) SizeOfPageHeaderData)

/*
 * UndoRecPtrGetBlockNum
 *		Compute the block number for an undo log logical byte offset.
 *
 * Each page can hold UNDO_USABLE_BYTES_PER_PAGE bytes of UNDO data.
 * Logical offset L maps to block L / UNDO_USABLE_BYTES_PER_PAGE.
 */
#define UndoRecPtrGetBlockNum(offset) \
	((BlockNumber) ((offset) / UNDO_USABLE_BYTES_PER_PAGE))

/*
 * UndoRecPtrGetPageOffset
 *		Compute the offset within the page for an undo log logical byte offset.
 *
 * The page offset accounts for the PageHeaderData that precedes the
 * UNDO data in each page.
 */
#define UndoRecPtrGetPageOffset(offset) \
	((uint32) (SizeOfPageHeaderData + ((offset) % UNDO_USABLE_BYTES_PER_PAGE)))

/*
 * UndoLogicalToFileSize
 *		Compute the physical file size needed for a given logical byte count.
 *
 * This is the number of full pages needed (rounded up) times BLCKSZ.
 */
#define UndoLogicalToFileSize(logical_size) \
	((uint64) (UndoRecPtrGetBlockNum((logical_size) - 1) + 1) * BLCKSZ)


/* ----------------------------------------------------------------
 *		Buffer read/release API
 * ----------------------------------------------------------------
 */

/*
 * ReadUndoBuffer
 *		Read an undo log block into the shared buffer pool.
 *
 * This is the primary entry point for reading undo data.  It translates
 * the undo log number and block number into a virtual RelFileLocator and
 * calls ReadBufferWithoutRelcache() to obtain a shared buffer.
 *
 * The returned Buffer must be released with ReleaseUndoBuffer() when the
 * caller is done.  The caller may also need to lock the buffer (via
 * LockBuffer) depending on the access pattern.
 *
 * Parameters:
 *   log_number   - undo log number
 *   block_number - block within the undo log
 *   mode         - RBM_NORMAL, RBM_ZERO_AND_LOCK, etc.
 *
 * Returns: a valid Buffer handle.
 */
extern Buffer ReadUndoBuffer(uint32 log_number, BlockNumber block_number,
							 ReadBufferMode mode);

/*
 * ReadUndoBufferExtended
 *		Like ReadUndoBuffer but with explicit strategy control.
 *
 * Allows the caller to specify a buffer access strategy (e.g., for
 * sequential undo log scans during discard or recovery).
 */
extern Buffer ReadUndoBufferExtended(uint32 log_number,
									 BlockNumber block_number,
									 ReadBufferMode mode,
									 BufferAccessStrategy strategy);

/*
 * ReleaseUndoBuffer
 *		Release a previously read undo buffer.
 *
 * This is a thin wrapper around ReleaseBuffer() for API symmetry.
 * If the buffer was locked, it must be unlocked first (or use
 * UnlockReleaseUndoBuffer).
 */
extern void ReleaseUndoBuffer(Buffer buffer);

/*
 * UnlockReleaseUndoBuffer
 *		Unlock and release an undo buffer in one call.
 */
extern void UnlockReleaseUndoBuffer(Buffer buffer);

/*
 * MarkUndoBufferDirty
 *		Mark an undo buffer as dirty.
 *
 * This is a thin wrapper around MarkBufferDirty() for API consistency.
 */
extern void MarkUndoBufferDirty(Buffer buffer);


/* ----------------------------------------------------------------
 *		Buffer tag construction (requires buf_internals.h)
 * ----------------------------------------------------------------
 */

/*
 * UndoMakeBufferTag
 *		Initialize a BufferTag for an undo log block.
 *
 * This constructs the BufferTag that the shared buffer manager will use
 * to identify this undo block in its hash table.  It uses the virtual
 * RelFileLocator mapping and UndoLogForkNum.
 *
 * Callers must include storage/buf_internals.h before this header to
 * make these declarations visible.
 */
#ifdef BUFMGR_INTERNALS_H
extern void UndoMakeBufferTag(BufferTag *tag, uint32 log_number,
							  BlockNumber block_number);

/*
 * IsUndoBufferTag
 *		Check whether a BufferTag refers to an undo log buffer.
 *
 * Undo buffers are identified by the UNDO_DB_OID in the dbOid field
 * of the buffer tag.
 */
static inline bool
IsUndoBufferTag(const BufferTag *tag)
{
	return (tag->dbOid == UNDO_DB_OID);
}
#endif							/* BUFMGR_INTERNALS_H */


/* ----------------------------------------------------------------
 *		Invalidation
 * ----------------------------------------------------------------
 */

/*
 * InvalidateUndoBuffers
 *		Drop all shared buffers for a given undo log.
 *
 * Called when an undo log is discarded to remove stale entries from
 * the shared buffer pool.  This is analogous to DropRelationBuffers()
 * for regular relations.
 */
extern void InvalidateUndoBuffers(uint32 log_number);

/*
 * InvalidateUndoBufferRange
 *		Drop shared buffers for a range of blocks in an undo log.
 *
 * Called during undo log truncation/discard to invalidate only the
 * blocks that are being reclaimed.  Blocks starting from first_block
 * onward are invalidated.
 */
extern void InvalidateUndoBufferRange(uint32 log_number,
									  BlockNumber first_block,
									  BlockNumber last_block);

#endif							/* UNDO_BUFMGR_H */
