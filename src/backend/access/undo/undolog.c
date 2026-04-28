/*-------------------------------------------------------------------------
 *
 * undolog.c
 *	  PostgreSQL UNDO log manager implementation
 *
 * This file implements the core UNDO log file management:
 * - Log file creation, writing, and reading
 * - Space allocation using 64-bit UndoRecPtr
 * - Discard of old UNDO records
 *
 * UNDO logs are stored in $PGDATA/base/undo/ with names like:
 *   000000000001, 000000000002, etc. (12-digit zero-padded)
 *
 * Each log can grow up to 1TB (40-bit offset), with up to 16M logs (24-bit log number).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/undo/undolog.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/transam.h"
#include "access/undo_bufmgr.h"
#include "access/undolog.h"
<<<<<<< HEAD
#include "access/undorecord.h"
=======
>>>>>>> fb27c189aca (Add UNDO log segment rotation and lifecycle management)
#include "access/undoworker.h"
#include "access/undo_xlog.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "common/file_perm.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "utils/errcodes.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

/* GUC parameters */
bool		enable_undo = false;
int			undo_log_segment_size = UNDO_LOG_SEGMENT_SIZE;
int			max_undo_logs = MAX_UNDO_LOGS;
int			undo_retention_time = 60000;	/* 60 seconds */
int			undo_worker_naptime = 10000;	/* 10 seconds */
int			undo_buffer_size = 1024;	/* 1MB in KB */

/* Shared memory pointer */
UndoLogSharedData *UndoLogShared = NULL;

/* Directory for UNDO logs */
#define UNDO_LOG_DIR "base/undo"

/*
 * Per-backend cached file descriptor for UNDO log files.
 *
 * File descriptors are process-local, so this cache is backend-private
 * (not in shared memory).  We cache one fd per log slot to avoid the
 * overhead of open()/close() on every UndoLogWrite/UndoLogRead call.
 * The needs_fsync flag tracks whether any write has occurred since the
 * last sync, so UndoLogSync() at commit time only fsyncs dirty files.
 */
typedef struct UndoLogFdCacheEntry
{
	int			fd;				/* cached kernel fd, -1 if not open */
	uint32		log_number;		/* log number this fd belongs to */
	bool		needs_fsync;	/* dirty: written since last fsync */
	uint64		cached_size;	/* last known file size (monotonically grows) */
}			UndoLogFdCacheEntry;

static UndoLogFdCacheEntry undo_fd_cache[MAX_UNDO_LOGS];
static bool undo_fd_cache_initialized = false;

/*
 * Per-backend tracking of the highest UndoRecPtr written during the
 * current transaction.  Used by the UNDO flush daemon to know what
 * needs to be synced at commit time.
 */
static UndoRecPtr undo_max_write_ptr = InvalidUndoRecPtr;

/*
 * InitUndoFdCache
 *		Lazily initialize the per-backend fd cache.
 */
static void
InitUndoFdCache(void)
{
	int			i;

	if (undo_fd_cache_initialized)
		return;

	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		undo_fd_cache[i].fd = -1;
		undo_fd_cache[i].log_number = 0;
		undo_fd_cache[i].needs_fsync = false;
		undo_fd_cache[i].cached_size = 0;
	}
	undo_fd_cache_initialized = true;
}

/*
 * GetCachedUndoLogFdEntry
 *		Return the cache entry for the given log_number, opening if needed.
 *
 * The returned entry is owned by the cache -- callers must NOT close() the fd.
 */
static UndoLogFdCacheEntry *
GetCachedUndoLogFdEntry(uint32 log_number, int flags);

/* Forward declarations */
static int	OpenUndoLogFile(uint32 log_number, int flags);
static void CreateUndoLogFile(uint32 log_number);

/* ExtendUndoLogFile is declared in undolog.h */

/*
 * UndoLogShmemSize
 *		Calculate shared memory size for UNDO log management
 */
Size
UndoLogShmemSize(void)
{
	Size		size = 0;

	/* Space for UndoLogSharedData */
	size = add_size(size, sizeof(UndoLogSharedData));

	return size;
}

/*
 * UndoLogShmemInit
 *		Initialize shared memory for UNDO log management
 */
void
UndoLogShmemInit(void)
{
	bool		found;

	UndoLogShared = (UndoLogSharedData *)
		ShmemInitStruct("UNDO Log Control", UndoLogShmemSize(), &found);

	/*
	 * Ensure the base/<UNDO_DB_OID>/ directory exists.  UNDO log buffers
	 * use a virtual RelFileLocator with dbOid = UNDO_DB_OID (9), and the
	 * standard storage manager (md.c) resolves this to base/9/.  This
	 * directory must exist before recovery replays UNDO WAL records or
	 * the checkpointer flushes dirty UNDO buffers.
	 *
	 * We do this unconditionally (even if not !found) so that it's
	 * idempotent across restarts and crash recovery.
	 */
	if (enable_undo)
	{
		char		undo_db_path[MAXPGPATH];

		snprintf(undo_db_path, MAXPGPATH, "base/%u", UNDO_DB_OID);
		if (MakePGDirectory(undo_db_path) < 0 && errno != EEXIST)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not create directory \"%s\": %m",
							undo_db_path)));
	}

	if (!found)
	{
		int			i;

		/* Initialize all log control structures */
		for (i = 0; i < MAX_UNDO_LOGS; i++)
		{
			UndoLogControl *log = &UndoLogShared->logs[i];

			log->log_number = 0;
			pg_atomic_init_u64(&log->insert_ptr, InvalidUndoRecPtr);
			log->discard_ptr = InvalidUndoRecPtr;
			log->oldest_xid = InvalidTransactionId;
			LWLockInitialize(&log->lock, LWTRANCHE_UNDO_LOG);
			log->in_use = false;
			/* Lifecycle management fields */
			log->state = UNDO_LOG_FREE;
			pg_atomic_init_u64(&log->seal_ptr, InvalidUndoRecPtr);
			log->sealed_time = 0;
		}

		UndoLogShared->next_log_number = 1;
		LWLockInitialize(&UndoLogShared->allocation_lock, LWTRANCHE_UNDO_LOG);
		/* No active log initially */
		pg_atomic_init_u32(&UndoLogShared->active_log_idx, MAX_UNDO_LOGS);
		pg_atomic_init_u64(&UndoLogShared->total_allocated, 0);
		pg_atomic_init_u64(&UndoLogShared->total_discarded, 0);
	}
}

/*
 * UndoLogSealAndRotate
 *		Seal the current active UNDO log and activate a new one.
 *
 * This performs segment rotation: the current active log is frozen
 * (no more writes allowed) and a fresh log is created and activated.
 *
 * The trigger parameter records why the rotation occurred (capacity,
 * checkpoint, pressure, or manual) and is stored in the WAL record.
 *
 * Must be called WITHOUT holding allocation_lock -- this function
 * acquires it internally.
 */
void
UndoLogSealAndRotate(uint8 trigger)
{
	uint32		active_idx;
	uint32		old_log_number = 0;
	UndoRecPtr	old_seal_ptr = InvalidUndoRecPtr;
	uint32		new_log_number;
	int			new_slot = -1;
	int			i;

	LWLockAcquire(&UndoLogShared->allocation_lock, LW_EXCLUSIVE);

	/* Read active log index */
	active_idx = pg_atomic_read_u32(&UndoLogShared->active_log_idx);

	/* Seal the old log if one is active */
	if (active_idx < MAX_UNDO_LOGS)
	{
		UndoLogControl *old_log = &UndoLogShared->logs[active_idx];

		LWLockAcquire(&old_log->lock, LW_EXCLUSIVE);

		/* Double-check it's still ACTIVE (another backend may have rotated) */
		if (old_log->state == UNDO_LOG_ACTIVE)
		{
			old_log->state = UNDO_LOG_SEALED;
			old_seal_ptr = pg_atomic_read_u64(&old_log->insert_ptr);
			pg_atomic_write_u64(&old_log->seal_ptr, old_seal_ptr);
			old_log->sealed_time = GetCurrentTimestamp();
			old_log_number = old_log->log_number;
		}

		LWLockRelease(&old_log->lock);
	}

	/* Mark no active log while we allocate a new one */
	pg_atomic_write_u32(&UndoLogShared->active_log_idx, MAX_UNDO_LOGS);

	/* Find a free slot for the new log */
	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		if (!UndoLogShared->logs[i].in_use)
		{
			new_slot = i;
			break;
		}
	}

	if (new_slot < 0)
	{
		LWLockRelease(&UndoLogShared->allocation_lock);
		ereport(ERROR,
				(errmsg("too many UNDO logs active, cannot rotate"),
				 errhint("Increase max_undo_logs or wait for discard.")));
	}

	/* Allocate a new log number and initialize the slot */
	new_log_number = UndoLogShared->next_log_number++;

	{
		UndoLogControl *new_log = &UndoLogShared->logs[new_slot];

		LWLockAcquire(&new_log->lock, LW_EXCLUSIVE);
		new_log->log_number = new_log_number;
		pg_atomic_write_u64(&new_log->insert_ptr,
							MakeUndoRecPtr(new_log_number, 0));
		new_log->discard_ptr = MakeUndoRecPtr(new_log_number, 0);
		new_log->oldest_xid = InvalidTransactionId;
		new_log->in_use = true;
		new_log->state = UNDO_LOG_ACTIVE;
		pg_atomic_write_u64(&new_log->seal_ptr, InvalidUndoRecPtr);
		new_log->sealed_time = 0;
		LWLockRelease(&new_log->lock);
	}

	/* Create the segment file for the new log */
	CreateUndoLogFile(new_log_number);

	/* Update active log index to point to the new slot */
	pg_atomic_write_u32(&UndoLogShared->active_log_idx, new_slot);

	/* WAL-log the rotation so recovery can reconstruct state */
	{
		xl_undo_rotate xlrec;

		xlrec.old_log_number = old_log_number;
		xlrec.old_seal_ptr = old_seal_ptr;
		xlrec.new_log_number = new_log_number;
		xlrec.trigger = trigger;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfUndoRotate);
		XLogInsert(RM_UNDO_ID, XLOG_UNDO_ROTATE);
	}

	LWLockRelease(&UndoLogShared->allocation_lock);

	/* Notify the discard worker about the sealed log */
	WakeUndoDiscardWorker();

	ereport(LOG,
			(errmsg("UNDO log rotation: sealed log %u at offset %llu, "
					"activated log %u (trigger: %s)",
					old_log_number,
					(unsigned long long) UndoRecPtrGetOffset(old_seal_ptr),
					new_log_number,
					trigger == UNDO_ROTATE_CAPACITY ? "capacity" :
					trigger == UNDO_ROTATE_CHECKPOINT ? "checkpoint" :
					trigger == UNDO_ROTATE_PRESSURE ? "pressure" :
					trigger == UNDO_ROTATE_MANUAL ? "manual" : "unknown")));
}

/*
 * UndoLogDeleteSegmentFile
 *		Delete the on-disk segment file for a fully discarded UNDO log.
 *
 * Called by the discard worker after all records in a DISCARDABLE log
 * have been cleaned up. Silently succeeds if the file is already gone.
 */
void
UndoLogDeleteSegmentFile(uint32 log_number)
{
	char		path[MAXPGPATH];

	UndoLogPath(log_number, path);

	if (unlink(path) < 0 && errno != ENOENT)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not remove UNDO log file \"%s\": %m", path)));
	else
		ereport(DEBUG1,
				(errmsg("deleted UNDO log segment file: %s", path)));
}

/*
 * UndoLogTryPressureDiscard
 *		Attempt synchronous inline discard from the allocating backend.
 *
 * Called from UndoLogAllocate() when allocation pressure exceeds 95%.
 * Performs a mini discard pass without waiting for the background worker.
 *
 * Returns true if space was freed, false if long-running transactions
 * prevent discard.
 */
bool
UndoLogTryPressureDiscard(void)
{
	TransactionId oldest_xid;
	bool		freed = false;
	int			i;

	oldest_xid = UndoWorkerGetOldestXid();
	if (!TransactionIdIsValid(oldest_xid))
		oldest_xid = ReadNextTransactionId();

	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];

		if (!log->in_use)
			continue;

		LWLockAcquire(&log->lock, LW_EXCLUSIVE);

		if (TransactionIdIsValid(log->oldest_xid) &&
			TransactionIdPrecedes(log->oldest_xid, oldest_xid))
		{
			UndoRecPtr	insert_ptr = pg_atomic_read_u64(&log->insert_ptr);

			if (UndoRecPtrGetOffset(insert_ptr) >
				UndoRecPtrGetOffset(log->discard_ptr))
			{
				uint64		delta = UndoRecPtrGetOffset(insert_ptr) -
									UndoRecPtrGetOffset(log->discard_ptr);

				log->discard_ptr = insert_ptr;
				log->oldest_xid = oldest_xid;
				freed = true;

				/* Update cumulative discard counter */
				pg_atomic_fetch_add_u64(&UndoLogShared->total_discarded,
										delta);

				ereport(DEBUG2,
						(errmsg("UNDO pressure discard: log %u advanced to %llu",
								log->log_number,
								(unsigned long long) UndoRecPtrGetOffset(insert_ptr))));
			}
		}

		LWLockRelease(&log->lock);
	}

	/* Wake the background worker for follow-up cleanup */
	WakeUndoDiscardWorker();

	return freed;
}

/*
 * UndoLogPath
 *		Construct the file path for an UNDO log
 *
 * Path is stored in provided buffer (must be MAXPGPATH size).
 * Returns the buffer pointer for convenience.
 */
char *
UndoLogPath(uint32 log_number, char *path)
{
	snprintf(path, MAXPGPATH, "%s/%012u", UNDO_LOG_DIR, log_number);
	return path;
}

/*
 * CreateUndoLogFile
 *		Create a new UNDO log file
 */
static void
CreateUndoLogFile(uint32 log_number)
{
	char		path[MAXPGPATH];
	int			fd;

	/* Ensure directory exists */
	if (mkdir(UNDO_LOG_DIR, pg_dir_create_mode) < 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", UNDO_LOG_DIR)));

	/* Create the log file */
	UndoLogPath(log_number, path);
	fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create UNDO log file \"%s\": %m", path)));

	if (close(fd) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close UNDO log file \"%s\": %m", path)));

	ereport(DEBUG1,
			(errmsg("created UNDO log file: %s", path)));
}

/*
 * OpenUndoLogFile
 *		Open an UNDO log file for reading or writing
 *
 * Returns file descriptor. Caller must close it.
 */
static int
OpenUndoLogFile(uint32 log_number, int flags)
{
	char		path[MAXPGPATH];
	int			fd;

	UndoLogPath(log_number, path);
	fd = BasicOpenFile(path, flags | PG_BINARY);
	if (fd < 0)
	{
		/* If opening for read and file doesn't exist, create it first */
		if ((flags & O_CREAT) && errno == ENOENT)
		{
			CreateUndoLogFile(log_number);
			fd = BasicOpenFile(path, flags | PG_BINARY);
		}

		if (fd < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open UNDO log file \"%s\": %m", path)));
	}

	return fd;
}

/*
 * GetCachedUndoLogFdEntry
 *		Return the cache entry for the given log_number, opening if needed.
 *
 * Searches the backend-local fd cache for an entry matching log_number.
 * If not found, opens the file via OpenUndoLogFile() and caches the fd.
 * The caller must NOT close the returned fd -- it is owned by the cache.
 *
 * Returns a pointer to the UndoLogFdCacheEntry so callers can directly
 * access the fd and set needs_fsync without a second cache lookup.
 *
 * The flags parameter is passed to OpenUndoLogFile when opening a new fd.
 */
static UndoLogFdCacheEntry *
GetCachedUndoLogFdEntry(uint32 log_number, int flags)
{
	int			i;
	int			free_slot = -1;

	InitUndoFdCache();

	/* Search for existing cache entry */
	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		if (undo_fd_cache[i].fd >= 0 &&
			undo_fd_cache[i].log_number == log_number)
		{
			return &undo_fd_cache[i];
		}
		if (undo_fd_cache[i].fd < 0 && free_slot < 0)
			free_slot = i;
	}

	/* No cached entry found; need to open the file */
	if (free_slot < 0)
	{
		/*
		 * Cache is full.  Evict the first entry.  In practice the number of
		 * concurrently active UNDO logs per backend is small, so this should
		 * be rare.
		 */
		free_slot = 0;
		if (undo_fd_cache[free_slot].needs_fsync)
		{
			(void) pg_fdatasync(undo_fd_cache[free_slot].fd);
			undo_fd_cache[free_slot].needs_fsync = false;
		}
		close(undo_fd_cache[free_slot].fd);
		undo_fd_cache[free_slot].fd = -1;
		undo_fd_cache[free_slot].cached_size = 0;
	}

	undo_fd_cache[free_slot].fd = OpenUndoLogFile(log_number, flags);
	undo_fd_cache[free_slot].log_number = log_number;
	undo_fd_cache[free_slot].needs_fsync = false;
	undo_fd_cache[free_slot].cached_size = 0;	/* will be populated by
												 * ExtendUndoLogFile */

	return &undo_fd_cache[free_slot];
}

/*
 * ExtendUndoLogFile
 *		Extend an UNDO log file to cover at least logical_end bytes of data
 *
 * The logical_end parameter is in terms of UNDO data bytes (the same
 * units as UndoRecPtr offsets).  The physical file is extended to cover
 * the necessary number of BLCKSZ pages, accounting for PageHeaderData
 * overhead in each page.
 *
 * Uses the per-backend fd cache so the file is not opened and closed
 * on every call.  Also caches the file size in the fd cache entry to
 * avoid fstat() syscalls on every allocation.
 */
void
ExtendUndoLogFile(uint32 log_number, uint64 logical_end)
{
	UndoLogFdCacheEntry *entry;
	uint64		physical_size;

	if (logical_end == 0)
		return;

	/* Convert logical bytes to physical file size (accounting for headers) */
	physical_size = UndoLogicalToFileSize(logical_end);

	entry = GetCachedUndoLogFdEntry(log_number, O_RDWR | O_CREAT);

	/* Fast path: cached size already sufficient */
	if (physical_size <= entry->cached_size)
		return;

	/*
	 * Cache miss or first call for this entry.  Check actual file size via
	 * fstat only when the cached size is insufficient.
	 */
	if (entry->cached_size == 0)
	{
		struct stat statbuf;
		char		path[MAXPGPATH];

		if (fstat(entry->fd, &statbuf) < 0)
		{
			UndoLogPath(log_number, path);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat UNDO log file \"%s\": %m", path)));
		}
		entry->cached_size = statbuf.st_size;

		/* Re-check after populating cache */
		if (physical_size <= entry->cached_size)
			return;
	}

	/* Extend the file */
	{
		char		path[MAXPGPATH];

		if (ftruncate(entry->fd, physical_size) < 0)
		{
			UndoLogPath(log_number, path);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend UNDO log file \"%s\" to %llu bytes: %m",
							path, (unsigned long long) physical_size)));
		}

		ereport(DEBUG1,
				(errmsg("extended UNDO log %u from %llu to %llu bytes (logical end %llu)",
						log_number,
						(unsigned long long) entry->cached_size,
						(unsigned long long) physical_size,
						(unsigned long long) logical_end)));

		entry->cached_size = physical_size;
	}
}

/*
 * Per-backend flag: has this backend ensured that the base/<UNDO_DB_OID>/
 * directory exists?  Checked once per backend lifetime to avoid repeated
 * mkdir() syscalls.
 */
static bool undo_db_dir_ensured = false;

/*
 * Per-backend cache of the highest block number known to exist in each
 * UNDO log's smgr file.  This avoids repeated smgrnblocks() calls.
 */
static BlockNumber undo_smgr_nblocks_cache[MAX_UNDO_LOGS];
static bool undo_smgr_cache_initialized = false;

/*
 * EnsureUndoDbDirectory
 *		Create the base/<UNDO_DB_OID>/ directory if it doesn't exist.
 *
 * UNDO log buffers are mapped to virtual RelFileLocators with
 * dbOid = UNDO_DB_OID (9).  The standard storage manager (md.c) resolves
 * these to file paths under base/9/.  This directory must exist before
 * md.c can create or write UNDO log segment files during checkpoint
 * flush-back.
 */
static void
EnsureUndoDbDirectory(void)
{
	char		path[MAXPGPATH];

	if (undo_db_dir_ensured)
		return;

	snprintf(path, MAXPGPATH, "base/%u", UNDO_DB_OID);

	if (MakePGDirectory(path) < 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", path)));

	undo_db_dir_ensured = true;
}

/*
 * ExtendUndoLogSmgrFile
 *		Ensure the smgr-managed file for an UNDO log covers the required blocks.
 *
 * When UNDO I/O is routed through shared_buffers, dirty UNDO pages are
 * flushed to disk by the checkpointer via md.c.  md.c resolves the virtual
 * RelFileLocator {spcOid=1663, dbOid=9, relNumber=log_number} to file
 * path base/9/<log_number>.  This function ensures that file exists and
 * is extended to cover at least target_block.
 *
 * Uses a per-backend cache to avoid repeated smgrnblocks() calls.
 */
void
ExtendUndoLogSmgrFile(uint32 log_number, uint64 logical_end)
{
	RelFileLocator rlocator;
	SMgrRelation srel;
	BlockNumber target_block;
	BlockNumber current_nblocks;

	if (logical_end == 0)
		return;

	/*
	 * Compute the highest block number we need.  UndoRecPtrGetBlockNum
	 * maps logical byte offsets to block numbers.
	 */
	target_block = UndoRecPtrGetBlockNum(logical_end - 1);

	/* Initialize per-backend cache on first use */
	if (!undo_smgr_cache_initialized)
	{
		int			i;

		for (i = 0; i < MAX_UNDO_LOGS; i++)
			undo_smgr_nblocks_cache[i] = 0;
		undo_smgr_cache_initialized = true;
	}

	/* Fast path: cached nblocks already covers the target */
	if (undo_smgr_nblocks_cache[log_number % MAX_UNDO_LOGS] > target_block)
		return;

	/* Ensure the base/9/ directory exists */
	EnsureUndoDbDirectory();

	UndoLogGetRelFileLocator(log_number, &rlocator);
	srel = smgropen(rlocator, INVALID_PROC_NUMBER);

	/* Create the fork file if it doesn't exist yet */
	if (!smgrexists(srel, UndoLogForkNum))
		smgrcreate(srel, UndoLogForkNum, false);

	/* Extend the file to cover the target block */
	current_nblocks = smgrnblocks(srel, UndoLogForkNum);

	while (current_nblocks <= target_block)
	{
		PGIOAlignedBlock zbuffer;

		memset(zbuffer.data, 0, BLCKSZ);
		smgrextend(srel, UndoLogForkNum, current_nblocks, zbuffer.data, false);
		current_nblocks++;
	}

	/* Update the per-backend cache */
	undo_smgr_nblocks_cache[log_number % MAX_UNDO_LOGS] = current_nblocks;
}

/*
 * UndoLogAllocate
 *		Allocate space for an UNDO record
 *
 * Returns UndoRecPtr pointing to the allocated space.
 * Caller must write data using UndoLogWrite().
 *
 * This function implements segment rotation: when the active log approaches
 * capacity, it seals the current log and activates a new one.  Under extreme
 * pressure (>95%), it performs synchronous inline discard and applies
 * backpressure to smooth out allocation spikes.
 */
UndoRecPtr
UndoLogAllocate(Size size)
{
	UndoLogControl *log;
	UndoRecPtr	ptr;
	uint32		log_number;
	uint64		offset;
	uint32		active_idx;
	uint64		segment_size = (uint64) undo_log_segment_size;
	int			retries = 0;

	if (size == 0)
		ereport(ERROR,
				(errmsg("cannot allocate zero-size UNDO record")));

retry:
	if (retries++ > MAX_UNDO_LOGS * 2)
		ereport(ERROR,
				(errmsg("UNDO log allocation failed after %d retries", retries),
				 errhint("Check for long-running transactions blocking UNDO discard.")));

	/*
	 * Fast path: read active_log_idx atomically (no lock for read).
	 */
	active_idx = pg_atomic_read_u32(&UndoLogShared->active_log_idx);

	if (active_idx >= MAX_UNDO_LOGS)
	{
		/* No active log exists, create one via seal-and-rotate */
		UndoLogSealAndRotate(UNDO_ROTATE_CAPACITY);
		goto retry;
	}

	log = &UndoLogShared->logs[active_idx];

	/* Verify the log is still ACTIVE (another backend may have rotated) */
	if (log->state != UNDO_LOG_ACTIVE)
		goto retry;

	/* Atomically claim space in the UNDO log */
	{
		uint64		old_ptr;
		uint64		new_ptr;

		old_ptr = pg_atomic_read_u64(&log->insert_ptr);
		log_number = UndoRecPtrGetLogNo(old_ptr);
		offset = UndoRecPtrGetOffset(old_ptr);

		/*
		 * Check capacity thresholds before attempting CAS.
		 */
		if (offset + size > segment_size)
		{
			/* Would exceed segment -- must rotate */
			UndoLogSealAndRotate(UNDO_ROTATE_CAPACITY);
			goto retry;
		}

		if (offset + size > (segment_size * UNDO_PRESSURE_THRESHOLD_PCT) / 100)
		{
			/*
			 * Above 95%: attempt synchronous discard, apply backpressure,
			 * then rotate.
			 */
			(void) UndoLogTryPressureDiscard();

			/* Calculate proportional backpressure sleep */
			{
				uint64		pressure_pct = ((offset + size) * 100) / segment_size;
				long		sleep_us;

				sleep_us = UNDO_BACKPRESSURE_MIN_US +
					(long) (UNDO_BACKPRESSURE_MAX_US - UNDO_BACKPRESSURE_MIN_US) *
					(long) (pressure_pct - UNDO_PRESSURE_THRESHOLD_PCT) /
					(long) (100 - UNDO_PRESSURE_THRESHOLD_PCT);
				sleep_us = Min(sleep_us, UNDO_BACKPRESSURE_MAX_US);
				pg_usleep(sleep_us);
			}

			UndoLogSealAndRotate(UNDO_ROTATE_PRESSURE);
			goto retry;
		}

		if (offset + size > (segment_size * UNDO_ROTATE_THRESHOLD_PCT) / 100)
		{
			/* Above 85%: proactive rotation */
			UndoLogSealAndRotate(UNDO_ROTATE_CAPACITY);
			goto retry;
		}

		new_ptr = MakeUndoRecPtr(log_number, offset + size);

		/* CAS loop - retry if another backend allocated concurrently */
		while (!pg_atomic_compare_exchange_u64(&log->insert_ptr,
											   &old_ptr, new_ptr))
		{
			log_number = UndoRecPtrGetLogNo(old_ptr);
			offset = UndoRecPtrGetOffset(old_ptr);

			/* Re-check thresholds after CAS failure */
			if (offset + size > segment_size ||
				offset + size > (segment_size * UNDO_ROTATE_THRESHOLD_PCT) / 100)
			{
				/* Thresholds crossed during contention -- restart */
				goto retry;
			}

			new_ptr = MakeUndoRecPtr(log_number, offset + size);
		}

		ptr = old_ptr;			/* We got space starting at old_ptr */
	}

	/* Update cumulative allocation counter */
	pg_atomic_fetch_add_u64(&UndoLogShared->total_allocated, size);

	/* Extend file if necessary */
	ExtendUndoLogFile(log_number, offset + size);

	/* Extend the smgr-managed file for checkpoint write-back */
	ExtendUndoLogSmgrFile(log_number, offset + size);

	return ptr;
}

/*
 * UndoLogWrite
 *		Write data to UNDO log at specified pointer via shared_buffers
 *
 * Routes UNDO writes through PostgreSQL's shared buffer pool instead of
 * direct pwrite() syscalls.  This eliminates per-row pwrite() overhead
 * and per-commit fdatasync() — UNDO pages are flushed to disk by the
 * checkpointer, same as heap pages.
 *
 * The logical byte offset in the UndoRecPtr is mapped to physical pages
 * that include standard PageHeaderData, so the buffer manager's checksum
 * and LSN tracking work correctly.
 *
 * Records that span page boundaries are split across multiple buffer
 * pins.  Each modified page is WAL-logged with XLogRegisterBuffer for
 * full-page-image support during crash recovery.
 */
void
UndoLogWrite(UndoRecPtr ptr, const char *data, Size size)
{
	uint32		log_number = UndoRecPtrGetLogNo(ptr);
	uint64		logical_offset = UndoRecPtrGetOffset(ptr);
	Size		remaining = size;
	const char *src = data;

	if (!UndoRecPtrIsValid(ptr))
		ereport(ERROR,
				(errmsg("invalid UNDO record pointer")));

	if (size == 0)
		return;

	entry = GetCachedUndoLogFdEntry(log_number, O_RDWR | O_CREAT);

	/* Write data at the target offset without a separate lseek() call */
	written = pwrite(entry->fd, data, size, (off_t) offset);
	if (written != size)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to UNDO log %u: %m", log_number)));

	/* Mark the cache entry dirty so UndoLogSync() knows to fsync it */
	entry->needs_fsync = true;

	/* Track highest written pointer for the UNDO flush daemon */
	{
		UndoRecPtr	end_ptr = MakeUndoRecPtr(log_number, offset + size);

		if (end_ptr > undo_max_write_ptr)
			undo_max_write_ptr = end_ptr;
	}
}

/*
 * UndoLogRead
 *		Read data from UNDO log at specified pointer via shared_buffers
 *
 * Routes UNDO reads through PostgreSQL's shared buffer pool.  Hot UNDO
 * data (recently written, not yet evicted) is served directly from
 * shared_buffers without any I/O.  This is especially beneficial for
 * rollback reads, which access recently-written UNDO data.
 *
 * Reads that span page boundaries are handled by reading from multiple
 * buffer pins in sequence.
 */
void
UndoLogRead(UndoRecPtr ptr, char *buffer, Size size)
{
	uint32		log_number = UndoRecPtrGetLogNo(ptr);
	uint64		logical_offset = UndoRecPtrGetOffset(ptr);
	Size		remaining = size;
	char	   *dest = buffer;

	if (!UndoRecPtrIsValid(ptr))
		ereport(ERROR,
				(errmsg("invalid UNDO record pointer")));

	if (size == 0)
		return;

	while (remaining > 0)
	{
		BlockNumber blkno = UndoRecPtrGetBlockNum(logical_offset);
		uint32		page_off = UndoRecPtrGetPageOffset(logical_offset);
		Size		read_len = Min(remaining, (Size) (BLCKSZ - page_off));
		Buffer		buf;
		Page		page;

		buf = ReadUndoBuffer(log_number, blkno, RBM_NORMAL);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		memcpy(dest, (char *) page + page_off, read_len);

		UnlockReleaseUndoBuffer(buf);

		dest += read_len;
		logical_offset += read_len;
		remaining -= read_len;
	}
}

/*
 * UndoLogDiscard
 *		Discard UNDO records older than oldest_needed
 *
 * This is called by the UNDO worker to reclaim space.
 * For now, just update the discard pointer. Actual file truncation/deletion
 * will be implemented in later commits.
 */
void
UndoLogDiscard(UndoRecPtr oldest_needed)
{
	int			i;

	if (!UndoRecPtrIsValid(oldest_needed))
		return;

	/* Update discard pointers for all logs */
	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];

		if (!log->in_use)
			continue;

		LWLockAcquire(&log->lock, LW_EXCLUSIVE);

		/* Update discard pointer if this record is in this log */
		if (UndoRecPtrGetLogNo(oldest_needed) == log->log_number)
		{
			if (UndoRecPtrGetOffset(oldest_needed) > UndoRecPtrGetOffset(log->discard_ptr))
			{
				log->discard_ptr = oldest_needed;
				ereport(DEBUG2,
						(errmsg("UNDO log %u: discard pointer updated to offset %llu",
								log->log_number,
								(unsigned long long) UndoRecPtrGetOffset(oldest_needed))));
			}
		}

		LWLockRelease(&log->lock);
	}
}

/*
 * UndoLogGetInsertPtr
 *		Get the current insertion pointer for a log
 */
UndoRecPtr
UndoLogGetInsertPtr(uint32 log_number)
{
	int			i;
	UndoRecPtr	ptr = InvalidUndoRecPtr;

	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];

		if (log->in_use && log->log_number == log_number)
		{
			ptr = pg_atomic_read_u64(&log->insert_ptr);
			break;
		}
	}

	return ptr;
}

/*
 * UndoLogGetDiscardPtr
 *		Get the current discard pointer for a log
 */
UndoRecPtr
UndoLogGetDiscardPtr(uint32 log_number)
{
	int			i;
	UndoRecPtr	ptr = InvalidUndoRecPtr;

	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];

		if (log->in_use && log->log_number == log_number)
		{
			LWLockAcquire(&log->lock, LW_SHARED);
			ptr = log->discard_ptr;
			LWLockRelease(&log->lock);
			break;
		}
	}

	return ptr;
}

/*
 * Note: undo_redo() has been moved to undo_xlog.c which handles all UNDO
 * resource manager WAL record types including CLRs (XLOG_UNDO_APPLY_RECORD).
 */

/*
 * UndoLogGetOldestDiscardPtr
 *		Get the oldest UNDO discard pointer across all active logs
 *
 * This is used during checkpoint to record the oldest UNDO data that
 * might be needed for recovery.
 */
UndoRecPtr
UndoLogGetOldestDiscardPtr(void)
{
	UndoRecPtr	oldest = InvalidUndoRecPtr;
	int			i;

	/* Scan all active UNDO logs to find the oldest discard pointer */
	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];

		if (log->in_use)
		{
			if (!UndoRecPtrIsValid(oldest) ||
				log->discard_ptr < oldest)
				oldest = log->discard_ptr;
		}
	}

	return oldest;
}

/*
 * UndoLogSync
 *		Fsync all dirty UNDO log files in this backend's fd cache.
 *
 * Called at transaction commit to ensure all UNDO data written during
 * the transaction is durable on disk.  Only files that have been written
 * With shared_buffers routing (commit "Route UNDO I/O through
 * shared_buffers"), UNDO pages are managed by the buffer pool and
 * flushed to disk by the checkpointer, exactly like heap pages.
 * There is no longer a need for per-commit fdatasync of UNDO files.
 *
 * This function is now a no-op.  It is retained for backward compatibility
 * with callers that existed before the shared_buffers transition.
 */
void
UndoLogSync(void)
{
	int			i;

	if (!undo_fd_cache_initialized)
		return;

	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		if (undo_fd_cache[i].fd >= 0 && undo_fd_cache[i].needs_fsync)
		{
			if (pg_fdatasync(undo_fd_cache[i].fd) < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not fdatasync UNDO log %u: %m",
								undo_fd_cache[i].log_number)));

			undo_fd_cache[i].needs_fsync = false;
		}
	}
}

/*
 * UndoLogCloseFiles
 *		Close all cached UNDO log file descriptors for this backend.
 *
 * Called during transaction abort (when we don't need to fsync) and
 * at process exit to release file descriptors.  Any pending dirty
 * data is NOT fsynced -- the caller is responsible for ensuring
 * durability if needed (e.g., by calling UndoLogSync first).
 */
void
UndoLogCloseFiles(void)
{
	int			i;

	if (!undo_fd_cache_initialized)
		return;

	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		if (undo_fd_cache[i].fd >= 0)
		{
			close(undo_fd_cache[i].fd);
			undo_fd_cache[i].fd = -1;
			undo_fd_cache[i].log_number = 0;
			undo_fd_cache[i].needs_fsync = false;
			undo_fd_cache[i].cached_size = 0;
		}
	}
}

/*
 * UndoFlushGetMaxWritePtr
 *		Return this backend's highest written UndoRecPtr.
 *
 * Used by the commit path to tell the UNDO flush daemon what needs syncing.
 */
UndoRecPtr
UndoFlushGetMaxWritePtr(void)
{
	return undo_max_write_ptr;
}

/*
 * UndoFlushResetMaxWritePtr
 *		Reset this backend's max write pointer at transaction end.
 */
void
UndoFlushResetMaxWritePtr(void)
{
	undo_max_write_ptr = InvalidUndoRecPtr;
}

/*
 * CheckPointUndoLog
 *		Perform checkpoint processing for the UNDO log subsystem.
 *
 * This is called from CheckPointGuts() during each checkpoint.  It ensures
 * that UNDO log discard pointers are durably persisted so that crash recovery
 * knows which UNDO data is still needed, and optionally logs UNDO statistics
 * when log_checkpoints is enabled.
 *
 * UndoLogWrite() defers fsync to transaction commit (via UndoLogSync()).
 * The primary purpose of this function is to persist the discard pointer
 * state (the in-memory UndoLogControl structures are rebuilt from WAL
 * during recovery) and to provide checkpoint-time statistics for monitoring.
 *
 * Per-relation UNDO data flows through shared_buffers and is flushed by
 * CheckPointBuffers(), so it does not need separate handling here.
 */
void
CheckPointUndoLog(void)
{
	int			active_logs = 0;
	int			sealed_logs = 0;
	uint64		total_allocated = 0;
	uint64		total_discarded = 0;
	int			i;

	/* Nothing to do if UNDO is not enabled at the server level */
	if (UndoLogShared == NULL)
		return;

	/*
	 * Proactive rotation: if the active log is more than 50% full, seal it
	 * and start a fresh segment.  This ensures that at every checkpoint
	 * boundary, a moderately-full segment is closed, preventing unbounded
	 * accumulation within a single segment.
	 */
	{
		uint32		active_idx;
		uint64		segment_size = (uint64) undo_log_segment_size;

		active_idx = pg_atomic_read_u32(&UndoLogShared->active_log_idx);
		if (active_idx < MAX_UNDO_LOGS)
		{
			UndoLogControl *active_log = &UndoLogShared->logs[active_idx];
			uint64		insert_offset;

			insert_offset = UndoRecPtrGetOffset(
												pg_atomic_read_u64(&active_log->insert_ptr));

			if (insert_offset > (segment_size * UNDO_CHECKPOINT_ROTATE_PCT) / 100)
			{
				ereport(LOG,
						(errmsg("UNDO checkpoint: rotating active log %u "
								"at %llu bytes (%llu%% of segment)",
								active_log->log_number,
								(unsigned long long) insert_offset,
								(unsigned long long) ((insert_offset * 100) / segment_size))));

				UndoLogSealAndRotate(UNDO_ROTATE_CHECKPOINT);
			}
		}
	}

	/*
	 * Scan all active UNDO logs to gather statistics and verify discard
	 * pointer consistency.  The discard pointers themselves are WAL-logged
	 * (via XLOG_UNDO_DISCARD records) and will be replayed during recovery,
	 * so we don't need to write them to a separate file here.
	 *
	 * We take only shared locks since we are reading, not modifying.
	 */
	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		UndoLogControl *log = &UndoLogShared->logs[i];

		if (!log->in_use)
			continue;

		active_logs++;
		if (log->state == UNDO_LOG_SEALED || log->state == UNDO_LOG_DISCARDABLE)
			sealed_logs++;

		total_allocated += UndoRecPtrGetOffset(pg_atomic_read_u64(&log->insert_ptr));

		LWLockAcquire(&log->lock, LW_SHARED);
		total_discarded += UndoRecPtrGetOffset(log->discard_ptr);
		LWLockRelease(&log->lock);
	}

	/* Log UNDO statistics during checkpoint when log_checkpoints is on */
	if (log_checkpoints && active_logs > 0)
	{
		ereport(LOG,
				(errmsg("UNDO checkpoint: %d active log(s) (%d sealed/discardable), "
						"%llu bytes allocated, %llu bytes discarded, "
						"%llu bytes retained",
						active_logs, sealed_logs,
						(unsigned long long) total_allocated,
						(unsigned long long) total_discarded,
						(unsigned long long) (total_allocated - total_discarded))));
	}
}
