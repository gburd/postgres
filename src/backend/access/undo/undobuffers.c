/*-------------------------------------------------------------------------
 *
 * undobuffers.c
 *	  UNDO log buffer cache for unified cache management
 *
 * This module provides a dedicated buffer cache for UNDO log blocks.
 * Instead of modifying PostgreSQL's main BufferTag (which must stay
 * under 64 bytes for cache-line efficiency), we maintain a separate
 * buffer pool with its own hash table and clock-sweep eviction.
 *
 * The cache stores BLCKSZ-aligned blocks from UNDO log files.
 * Blocks are identified by (log_number, block_number) pairs.
 *
 * Cache management:
 *   - Hash table for O(1) block lookup
 *   - Clock-sweep (approximation of LRU) for eviction
 *   - Free list for quick allocation of unused slots
 *   - Dirty buffer tracking for checkpoint write-back
 *
 * Concurrency:
 *   - Manager lock protects free list and clock-sweep state
 *   - Per-buffer content locks protect individual buffer contents
 *   - Hash table uses partition locks (via dynahash)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undobuffers.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "access/undobuffers.h"
#include "access/undolog.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

/* GUC parameter: UNDO buffer cache size in KB (default 1024 = 1MB) */
int			undo_buffer_size = 1024;

/* Shared memory structures */
UndoBufferControl *UndoBufCtl = NULL;
UndoBufferDesc *UndoBufDescriptors = NULL;
char	   *UndoBufBlocks = NULL;

/* Hash table for buffer lookup */
static HTAB *UndoBufHash = NULL;

/*
 * Hash table entry mapping UndoBufferTag -> buffer index
 */
typedef struct UndoBufHashEntry
{
	UndoBufferTag tag;			/* hash key (must be first) */
	int			buf_id;			/* index into UndoBufDescriptors */
} UndoBufHashEntry;

/*
 * Calculate the number of buffer slots from the GUC setting.
 * undo_buffer_size is in KB, each buffer is BLCKSZ bytes.
 */
static int
UndoBufferNumSlots(void)
{
	int			num = (undo_buffer_size * 1024) / BLCKSZ;

	if (num < 16)
		num = 16;				/* minimum 16 buffers */
	return num;
}

/*
 * UndoBuffersShmemSize
 *		Calculate shared memory needed for UNDO buffer cache
 */
Size
UndoBuffersShmemSize(void)
{
	Size		size = 0;
	int			num_buffers = UndoBufferNumSlots();

	/* Control structure */
	size = add_size(size, sizeof(UndoBufferControl));

	/* Buffer descriptors */
	size = add_size(size, mul_size(num_buffers, sizeof(UndoBufferDesc)));

	/* Buffer data blocks (BLCKSZ each, aligned) */
	size = add_size(size, mul_size(num_buffers, BLCKSZ));

	/* Hash table (estimated) */
	size = add_size(size, hash_estimate_size(num_buffers,
											 sizeof(UndoBufHashEntry)));

	return size;
}

/*
 * UndoBuffersShmemInit
 *		Initialize shared memory for UNDO buffer cache
 */
void
UndoBuffersShmemInit(void)
{
	bool		found;
	int			num_buffers;
	HASHCTL		hash_ctl;

	num_buffers = UndoBufferNumSlots();

	/* Allocate the control structure */
	UndoBufCtl = (UndoBufferControl *)
		ShmemInitStruct("UNDO Buffer Control",
						sizeof(UndoBufferControl),
						&found);

	if (!found)
	{
		UndoBufCtl->num_buffers = num_buffers;
		UndoBufCtl->first_free = 0;
		UndoBufCtl->clock_hand = 0;
		LWLockInitialize(&UndoBufCtl->manager_lock,
						 LWTRANCHE_UNDO_BUFFERS);
		pg_atomic_init_u64(&UndoBufCtl->hits, 0);
		pg_atomic_init_u64(&UndoBufCtl->misses, 0);
		pg_atomic_init_u64(&UndoBufCtl->evictions, 0);
		pg_atomic_init_u64(&UndoBufCtl->writes, 0);
	}

	/* Allocate buffer descriptors */
	UndoBufDescriptors = (UndoBufferDesc *)
		ShmemInitStruct("UNDO Buffer Descriptors",
						mul_size(num_buffers, sizeof(UndoBufferDesc)),
						&found);

	if (!found)
	{
		int			i;

		for (i = 0; i < num_buffers; i++)
		{
			UndoBufferDesc *desc = &UndoBufDescriptors[i];

			desc->tag.log_number = 0;
			desc->tag.block_number = 0;
			desc->is_valid = false;
			desc->is_dirty = false;
			desc->usage_count = 0;
			desc->next_free = (i < num_buffers - 1) ? i + 1 : -1;
			LWLockInitialize(&desc->content_lock,
							 LWTRANCHE_UNDO_BUFFERS);
		}
	}

	/* Allocate block data array */
	UndoBufBlocks = (char *)
		ShmemInitStruct("UNDO Buffer Blocks",
						mul_size(num_buffers, BLCKSZ),
						&found);

	if (!found)
		MemSet(UndoBufBlocks, 0, mul_size(num_buffers, BLCKSZ));

	/* Initialize hash table */
	hash_ctl.keysize = sizeof(UndoBufferTag);
	hash_ctl.entrysize = sizeof(UndoBufHashEntry);
	UndoBufHash = ShmemInitHash("UNDO Buffer Hash",
								num_buffers, num_buffers,
								&hash_ctl,
								HASH_ELEM | HASH_BLOBS);
}

/*
 * UndoBufferWriteBack - Write a dirty buffer to disk
 *
 * The caller must hold the content_lock in at least shared mode.
 */
static void
UndoBufferWriteBack(int buf_id)
{
	UndoBufferDesc *desc = &UndoBufDescriptors[buf_id];
	char	   *block = UndoBufBlocks + (Size) buf_id * BLCKSZ;
	char		path[MAXPGPATH];
	int			fd;
	off_t		file_offset;

	if (!desc->is_dirty)
		return;

	UndoLogPath(desc->tag.log_number, path);
	file_offset = (off_t) desc->tag.block_number * BLCKSZ;

	fd = BasicOpenFile(path, O_RDWR | O_CREAT | PG_BINARY);
	if (fd < 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not open UNDO log \"%s\" for buffer writeback: %m",
						path)));
		return;
	}

	if (pg_pwrite(fd, block, BLCKSZ, file_offset) != BLCKSZ)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not write UNDO buffer to \"%s\" at offset %lld: %m",
						path, (long long) file_offset)));
		return;
	}

	if (enableFsync && pg_fsync(fd) != 0)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not fsync UNDO log \"%s\": %m", path)));
		return;
	}

	close(fd);
	desc->is_dirty = false;
	pg_atomic_fetch_add_u64(&UndoBufCtl->writes, 1);
}

/*
 * UndoBufferReadFromDisk - Read a block from UNDO log file into buffer
 */
static bool
UndoBufferReadFromDisk(int buf_id, uint32 log_number, uint32 block_number)
{
	char	   *block = UndoBufBlocks + (Size) buf_id * BLCKSZ;
	char		path[MAXPGPATH];
	int			fd;
	off_t		file_offset;
	ssize_t		nread;

	UndoLogPath(log_number, path);
	file_offset = (off_t) block_number * BLCKSZ;

	fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
		{
			/* File doesn't exist yet, zero-fill the buffer */
			MemSet(block, 0, BLCKSZ);
			return true;
		}
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not open UNDO log \"%s\" for reading: %m",
						path)));
		return false;
	}

	nread = pg_pread(fd, block, BLCKSZ, file_offset);
	if (nread < 0)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not read UNDO block from \"%s\" at offset %lld: %m",
						path, (long long) file_offset)));
		return false;
	}

	/* If we read less than BLCKSZ, zero-fill the rest */
	if (nread < BLCKSZ)
		MemSet(block + nread, 0, BLCKSZ - nread);

	close(fd);
	return true;
}

/*
 * UndoBufferEvict - Find a victim buffer using clock-sweep
 *
 * Caller must hold manager_lock exclusively.
 * Returns the buffer index of the evicted buffer, with its hash
 * entry already removed.
 */
static int
UndoBufferEvict(void)
{
	int			num_buffers = UndoBufCtl->num_buffers;
	int			victim;
	int			loops = 0;

	/*
	 * Clock-sweep: scan buffers starting from clock_hand, decrementing
	 * usage_count. Pick the first buffer with usage_count == 0.
	 */
	for (;;)
	{
		UndoBufferDesc *desc;

		victim = UndoBufCtl->clock_hand;
		UndoBufCtl->clock_hand = (victim + 1) % num_buffers;

		desc = &UndoBufDescriptors[victim];

		if (!desc->is_valid)
		{
			/* Empty slot, use it directly */
			return victim;
		}

		if (desc->usage_count > 0)
		{
			desc->usage_count--;
			continue;
		}

		/* Found a victim with usage_count == 0 */

		/* Write back if dirty */
		if (desc->is_dirty)
		{
			LWLockAcquire(&desc->content_lock, LW_SHARED);
			UndoBufferWriteBack(victim);
			LWLockRelease(&desc->content_lock);
		}

		/* Remove from hash table */
		if (desc->is_valid)
		{
			bool		hash_found;

			hash_search(UndoBufHash, &desc->tag, HASH_REMOVE, &hash_found);
		}

		desc->is_valid = false;
		pg_atomic_fetch_add_u64(&UndoBufCtl->evictions, 1);
		return victim;

		/* Safety: avoid infinite loops in degenerate cases */
		loops++;
		if (loops > num_buffers * 3)
			elog(ERROR, "UNDO buffer cache eviction failed after %d loops", loops);
	}
}

/*
 * UndoBufferRead - Read an UNDO block into the buffer cache
 *
 * Returns a pointer to the cached block data. The block is
 * "pinned" by incrementing its usage_count. The caller must call
 * UndoBufferRelease() when done.
 */
char *
UndoBufferRead(uint32 log_number, uint32 block_number, int *buffer_index)
{
	UndoBufferTag tag;
	UndoBufHashEntry *entry;
	bool		hash_found;
	int			buf_id;
	UndoBufferDesc *desc;

	tag.log_number = log_number;
	tag.block_number = block_number;

	/* First, try to find the block in the hash table */
	LWLockAcquire(&UndoBufCtl->manager_lock, LW_SHARED);
	entry = (UndoBufHashEntry *) hash_search(UndoBufHash, &tag,
											 HASH_FIND, &hash_found);
	if (hash_found)
	{
		buf_id = entry->buf_id;
		desc = &UndoBufDescriptors[buf_id];

		/* Bump usage count (up to BM_MAX_USAGE_COUNT equivalent) */
		if (desc->usage_count < 5)
			desc->usage_count++;

		LWLockRelease(&UndoBufCtl->manager_lock);
		pg_atomic_fetch_add_u64(&UndoBufCtl->hits, 1);

		*buffer_index = buf_id;
		return UndoBufBlocks + (Size) buf_id * BLCKSZ;
	}
	LWLockRelease(&UndoBufCtl->manager_lock);

	/* Cache miss: need to load from disk */
	pg_atomic_fetch_add_u64(&UndoBufCtl->misses, 1);

	LWLockAcquire(&UndoBufCtl->manager_lock, LW_EXCLUSIVE);

	/* Re-check after acquiring exclusive lock (another backend may have loaded it) */
	entry = (UndoBufHashEntry *) hash_search(UndoBufHash, &tag,
											 HASH_FIND, &hash_found);
	if (hash_found)
	{
		buf_id = entry->buf_id;
		desc = &UndoBufDescriptors[buf_id];
		if (desc->usage_count < 5)
			desc->usage_count++;

		LWLockRelease(&UndoBufCtl->manager_lock);
		*buffer_index = buf_id;
		return UndoBufBlocks + (Size) buf_id * BLCKSZ;
	}

	/* Get a free buffer or evict one */
	if (UndoBufCtl->first_free >= 0)
	{
		buf_id = UndoBufCtl->first_free;
		UndoBufCtl->first_free = UndoBufDescriptors[buf_id].next_free;
	}
	else
	{
		buf_id = UndoBufferEvict();
	}

	/* Set up the descriptor */
	desc = &UndoBufDescriptors[buf_id];
	desc->tag = tag;
	desc->is_valid = true;
	desc->is_dirty = false;
	desc->usage_count = 1;
	desc->next_free = -1;

	/* Add to hash table */
	entry = (UndoBufHashEntry *) hash_search(UndoBufHash, &tag,
											 HASH_ENTER, &hash_found);
	Assert(!hash_found);
	entry->buf_id = buf_id;

	LWLockRelease(&UndoBufCtl->manager_lock);

	/* Read from disk outside the manager lock */
	LWLockAcquire(&desc->content_lock, LW_EXCLUSIVE);
	if (!UndoBufferReadFromDisk(buf_id, log_number, block_number))
	{
		/* Read failed, mark as invalid */
		LWLockRelease(&desc->content_lock);

		LWLockAcquire(&UndoBufCtl->manager_lock, LW_EXCLUSIVE);
		desc->is_valid = false;
		hash_search(UndoBufHash, &tag, HASH_REMOVE, NULL);
		desc->next_free = UndoBufCtl->first_free;
		UndoBufCtl->first_free = buf_id;
		LWLockRelease(&UndoBufCtl->manager_lock);

		ereport(ERROR,
				(errmsg("could not read UNDO block (log %u, block %u)",
						log_number, block_number)));
	}
	LWLockRelease(&desc->content_lock);

	*buffer_index = buf_id;
	return UndoBufBlocks + (Size) buf_id * BLCKSZ;
}

/*
 * UndoBufferRelease - Release a pinned UNDO buffer
 *
 * Currently a no-op since we use usage_count rather than pin counts,
 * but provided for API symmetry and future extension.
 */
void
UndoBufferRelease(int buffer_index)
{
	/* Nothing to do currently; usage_count handles eviction priority */
}

/*
 * UndoBufferMarkDirty - Mark an UNDO buffer as dirty
 */
void
UndoBufferMarkDirty(int buffer_index)
{
	Assert(buffer_index >= 0 && buffer_index < UndoBufCtl->num_buffers);
	UndoBufDescriptors[buffer_index].is_dirty = true;
}

/*
 * UndoBufferFlush - Write all dirty UNDO buffers to disk
 *
 * Called during checkpoint. Scans all buffers and writes back
 * any that are dirty.
 */
void
UndoBufferFlush(void)
{
	int			i;
	int			num_buffers;

	if (UndoBufCtl == NULL)
		return;

	num_buffers = UndoBufCtl->num_buffers;

	for (i = 0; i < num_buffers; i++)
	{
		UndoBufferDesc *desc = &UndoBufDescriptors[i];

		if (!desc->is_valid || !desc->is_dirty)
			continue;

		LWLockAcquire(&desc->content_lock, LW_SHARED);
		UndoBufferWriteBack(i);
		LWLockRelease(&desc->content_lock);
	}
}

/*
 * UndoBufferInvalidate - Invalidate cached blocks for a log
 *
 * Removes all cached blocks belonging to the specified log number.
 * Called when an UNDO log is discarded.
 */
void
UndoBufferInvalidate(uint32 log_number)
{
	int			i;
	int			num_buffers;

	if (UndoBufCtl == NULL)
		return;

	num_buffers = UndoBufCtl->num_buffers;

	LWLockAcquire(&UndoBufCtl->manager_lock, LW_EXCLUSIVE);

	for (i = 0; i < num_buffers; i++)
	{
		UndoBufferDesc *desc = &UndoBufDescriptors[i];

		if (!desc->is_valid || desc->tag.log_number != log_number)
			continue;

		/* Write back if dirty before invalidating */
		if (desc->is_dirty)
		{
			LWLockAcquire(&desc->content_lock, LW_SHARED);
			UndoBufferWriteBack(i);
			LWLockRelease(&desc->content_lock);
		}

		/* Remove from hash table */
		hash_search(UndoBufHash, &desc->tag, HASH_REMOVE, NULL);

		/* Return to free list */
		desc->is_valid = false;
		desc->is_dirty = false;
		desc->next_free = UndoBufCtl->first_free;
		UndoBufCtl->first_free = i;
	}

	LWLockRelease(&UndoBufCtl->manager_lock);
}

/*
 * UndoBufferGetStats - Get buffer cache statistics
 */
void
UndoBufferGetStats(uint64 *hits, uint64 *misses,
				   uint64 *evictions, uint64 *writes)
{
	if (UndoBufCtl == NULL)
	{
		*hits = *misses = *evictions = *writes = 0;
		return;
	}

	*hits = pg_atomic_read_u64(&UndoBufCtl->hits);
	*misses = pg_atomic_read_u64(&UndoBufCtl->misses);
	*evictions = pg_atomic_read_u64(&UndoBufCtl->evictions);
	*writes = pg_atomic_read_u64(&UndoBufCtl->writes);
}
