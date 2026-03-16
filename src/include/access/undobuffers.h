/*-------------------------------------------------------------------------
 *
 * undobuffers.h
 *	  UNDO log buffer cache for unified cache management
 *
 * This module provides a dedicated buffer cache for UNDO log blocks,
 * sitting alongside PostgreSQL's main shared buffer pool. Rather than
 * modifying the performance-critical BufferTag structure (which is kept
 * under 64 bytes for cache-line efficiency), we maintain a separate
 * cache with its own hash table and LRU eviction.
 *
 * Benefits:
 *   - Unified cache management with LRU eviction
 *   - Avoids repeated open/seek/read/close for UNDO record access
 *   - Dirty buffer tracking for checkpoint integration
 *   - Configurable size via undo_buffer_size GUC
 *
 * UNDO blocks are BLCKSZ (8KB) aligned chunks of UNDO log files.
 * A block is identified by (log_number, block_number), where
 * block_number = file_offset / BLCKSZ.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/undobuffers.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef UNDOBUFFERS_H
#define UNDOBUFFERS_H

#include "access/undolog.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

/*
 * Default number of UNDO buffer slots.
 * Each slot holds one BLCKSZ (8KB) block, so 128 slots = 1MB default.
 * Configurable via undo_buffer_size GUC (in KB).
 */
#define DEFAULT_UNDO_BUFFERS	128

/*
 * UndoBufferTag - Identifies an UNDO block in the cache
 *
 * Used as the hash key for the UNDO buffer lookup table.
 */
typedef struct UndoBufferTag
{
	uint32		log_number;		/* UNDO log number */
	uint32		block_number;	/* Block offset within log (offset / BLCKSZ) */
} UndoBufferTag;

/*
 * UndoBufferDesc - Descriptor for a cached UNDO block
 *
 * Maintains metadata about each buffer slot in the UNDO buffer cache.
 * The actual block data is stored in a separate contiguous array.
 */
typedef struct UndoBufferDesc
{
	UndoBufferTag tag;			/* Block identity */
	bool		is_valid;		/* Buffer contains valid data */
	bool		is_dirty;		/* Buffer has been modified */
	uint32		usage_count;	/* Clock-sweep usage counter */
	int			next_free;		/* Next free buffer in free list (-1 = end) */
	LWLock		content_lock;	/* Protects buffer contents */
} UndoBufferDesc;

/*
 * UndoBufferControl - Shared memory control for UNDO buffer cache
 *
 * Coordinates access to the UNDO buffer pool.
 */
typedef struct UndoBufferControl
{
	int			num_buffers;	/* Total buffer slots */
	int			first_free;		/* Head of free list (-1 = empty) */
	int			clock_hand;		/* Clock-sweep position for eviction */
	LWLock		manager_lock;	/* Protects free list and clock sweep */

	/* Statistics */
	pg_atomic_uint64 hits;		/* Cache hits */
	pg_atomic_uint64 misses;	/* Cache misses */
	pg_atomic_uint64 evictions;	/* Buffers evicted */
	pg_atomic_uint64 writes;	/* Dirty buffers written back */
} UndoBufferControl;

/* GUC parameter */
extern int	undo_buffer_size;

/* Global shared memory pointers */
extern UndoBufferControl *UndoBufCtl;
extern UndoBufferDesc *UndoBufDescriptors;
extern char *UndoBufBlocks;

/*
 * Public API
 */

/* Shared memory initialization */
extern Size UndoBuffersShmemSize(void);
extern void UndoBuffersShmemInit(void);

/*
 * UndoBufferRead - Read an UNDO block into the buffer cache
 *
 * Returns a pointer to the cached block data (BLCKSZ bytes).
 * The block is pinned in the cache until UndoBufferRelease() is called.
 * If the block is not cached, it is read from disk.
 *
 * The returned buffer index can be used with UndoBufferRelease()
 * and UndoBufferMarkDirty().
 */
extern char *UndoBufferRead(uint32 log_number, uint32 block_number,
							int *buffer_index);

/*
 * UndoBufferRelease - Release a pinned UNDO buffer
 *
 * Must be called after UndoBufferRead() when the caller is done
 * with the buffer contents.
 */
extern void UndoBufferRelease(int buffer_index);

/*
 * UndoBufferMarkDirty - Mark an UNDO buffer as dirty
 *
 * Called after modifying buffer contents. The buffer will be written
 * back to disk during checkpoint or eviction.
 */
extern void UndoBufferMarkDirty(int buffer_index);

/*
 * UndoBufferFlush - Write all dirty UNDO buffers to disk
 *
 * Called during checkpoint to ensure all modified UNDO blocks are
 * durably written. Also fsyncs the UNDO log files.
 */
extern void UndoBufferFlush(void);

/*
 * UndoBufferInvalidate - Invalidate cached blocks for a log
 *
 * Called when an UNDO log is discarded to remove stale entries
 * from the buffer cache.
 */
extern void UndoBufferInvalidate(uint32 log_number);

/*
 * UndoBufferGetStats - Get buffer cache statistics
 *
 * Returns current hit/miss/eviction counts for monitoring.
 */
extern void UndoBufferGetStats(uint64 *hits, uint64 *misses,
							   uint64 *evictions, uint64 *writes);

#endif							/* UNDOBUFFERS_H */
