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
#include "access/undolog.h"
#include "access/undo_xlog.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/errcodes.h"
#include "utils/memutils.h"

/* GUC parameters (will be defined in later commits) */
int			undo_log_segment_size = UNDO_LOG_SEGMENT_SIZE;
int			max_undo_logs = MAX_UNDO_LOGS;

/* Shared memory pointer */
UndoLogSharedData *UndoLogShared = NULL;

/* Directory for UNDO logs */
#define UNDO_LOG_DIR "base/undo"

/* Forward declarations */
static uint32 AllocateUndoLog(void);
static int	OpenUndoLogFile(uint32 log_number, int flags);
static void CreateUndoLogFile(uint32 log_number);
static void ExtendUndoLogFile(uint32 log_number, uint64 new_size);

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

	if (!found)
	{
		int			i;

		/* Initialize all log control structures */
		for (i = 0; i < MAX_UNDO_LOGS; i++)
		{
			UndoLogControl *log = &UndoLogShared->logs[i];

			log->log_number = 0;
			log->insert_ptr = InvalidUndoRecPtr;
			log->discard_ptr = InvalidUndoRecPtr;
			log->oldest_xid = InvalidTransactionId;
			/* Note: LWLock tranche will be registered dynamically */
			LWLockInitialize(&log->lock, LWTRANCHE_UNDO_LOG);
			log->in_use = false;
		}

		UndoLogShared->next_log_number = 1;
		LWLockInitialize(&UndoLogShared->allocation_lock,
						 LWTRANCHE_UNDO_LOG);
	}
}

/*
 * AllocateUndoLog
 *		Allocate a new UNDO log number
 *
 * Returns the log number. Caller must create the file.
 */
static uint32
AllocateUndoLog(void)
{
	uint32		log_number;
	int			i;
	UndoLogControl *log = NULL;

	LWLockAcquire(&UndoLogShared->allocation_lock, LW_EXCLUSIVE);

	/* Find a free slot */
	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		if (!UndoLogShared->logs[i].in_use)
		{
			log = &UndoLogShared->logs[i];
			break;
		}
	}

	if (log == NULL)
		ereport(ERROR,
				(errmsg("too many UNDO logs active"),
				 errhint("Increase max_undo_logs configuration parameter.")));

	/* Allocate next log number */
	log_number = UndoLogShared->next_log_number++;

	/* Initialize the log control structure */
	LWLockAcquire(&log->lock, LW_EXCLUSIVE);
	log->log_number = log_number;
	log->insert_ptr = MakeUndoRecPtr(log_number, 0);
	log->discard_ptr = MakeUndoRecPtr(log_number, 0);
	log->oldest_xid = InvalidTransactionId;
	log->in_use = true;
	LWLockRelease(&log->lock);

	LWLockRelease(&UndoLogShared->allocation_lock);

	return log_number;
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
	if (mkdir(UNDO_LOG_DIR, S_IRWXU) < 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", UNDO_LOG_DIR)));

	/*
	 * Create the log file.  Use O_CREAT without O_EXCL so that this is
	 * idempotent -- the file may already exist from a previous server
	 * incarnation (after crash recovery, the shared-memory log control
	 * array is re-initialized, but the on-disk files survive).
	 */
	UndoLogPath(log_number, path);
	fd = BasicOpenFile(path, O_RDWR | O_CREAT | PG_BINARY);
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
 * ExtendUndoLogFile
 *		Extend an UNDO log file to at least new_size bytes
 */
static void
ExtendUndoLogFile(uint32 log_number, uint64 new_size)
{
	char		path[MAXPGPATH];
	int			fd;
	struct stat statbuf;
	uint64		current_size;

	UndoLogPath(log_number, path);
	fd = OpenUndoLogFile(log_number, O_RDWR | O_CREAT);

	/* Get current size */
	if (fstat(fd, &statbuf) < 0)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat UNDO log file \"%s\": %m", path)));
	}

	current_size = statbuf.st_size;

	/* Extend if needed */
	if (new_size > current_size)
	{
		if (ftruncate(fd, new_size) < 0)
		{
			int			save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not extend UNDO log file \"%s\" to %llu bytes: %m",
							path, (unsigned long long) new_size)));
		}

		ereport(DEBUG1,
				(errmsg("extended UNDO log %u from %llu to %llu bytes",
						log_number,
						(unsigned long long) current_size,
						(unsigned long long) new_size)));
	}

	close(fd);
}

/*
 * UndoLogAllocate
 *		Allocate space for an UNDO record
 *
 * Returns UndoRecPtr pointing to the allocated space.
 * Caller must write data using UndoLogWrite().
 */
UndoRecPtr
UndoLogAllocate(Size size)
{
	UndoLogControl *log;
	UndoRecPtr	ptr;
	uint32		log_number;
	uint64		offset;
	int			i;

	if (size == 0)
		ereport(ERROR,
				(errmsg("cannot allocate zero-size UNDO record")));

	/*
	 * Find or create an active log.
	 * For now, use a simple strategy: use the first in-use log,
	 * or allocate a new one if none exist.
	 */
	log = NULL;
	for (i = 0; i < MAX_UNDO_LOGS; i++)
	{
		if (UndoLogShared->logs[i].in_use)
		{
			log = &UndoLogShared->logs[i];
			break;
		}
	}

	if (log == NULL)
	{
		/* No active log, create one */
		log_number = AllocateUndoLog();
		CreateUndoLogFile(log_number);

		/* Find the log control structure we just allocated */
		for (i = 0; i < MAX_UNDO_LOGS; i++)
		{
			if (UndoLogShared->logs[i].log_number == log_number)
			{
				log = &UndoLogShared->logs[i];
				break;
			}
		}

		Assert(log != NULL);
	}

	/* Allocate space at end of log */
	LWLockAcquire(&log->lock, LW_EXCLUSIVE);

	ptr = log->insert_ptr;
	log_number = UndoRecPtrGetLogNo(ptr);
	offset = UndoRecPtrGetOffset(ptr);

	/* Check if we need to extend the file */
	if (offset + size > UNDO_LOG_SEGMENT_SIZE)
	{
		LWLockRelease(&log->lock);
		ereport(ERROR,
				(errmsg("UNDO log %u would exceed segment size", log_number),
				 errhint("UNDO log rotation not yet implemented")));
	}

	/* Update insert pointer */
	log->insert_ptr = MakeUndoRecPtr(log_number, offset + size);

	LWLockRelease(&log->lock);

	/* Extend file if necessary */
	ExtendUndoLogFile(log_number, offset + size);

	return ptr;
}

/*
 * UndoLogWrite
 *		Write data to UNDO log at specified pointer
 */
void
UndoLogWrite(UndoRecPtr ptr, const char *data, Size size)
{
	uint32		log_number = UndoRecPtrGetLogNo(ptr);
	uint64		offset = UndoRecPtrGetOffset(ptr);
	int			fd;
	ssize_t		written;

	if (!UndoRecPtrIsValid(ptr))
		ereport(ERROR,
				(errmsg("invalid UNDO record pointer")));

	if (size == 0)
		return;

	fd = OpenUndoLogFile(log_number, O_RDWR | O_CREAT);

	/* Seek to position */
	if (lseek(fd, offset, SEEK_SET) < 0)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek in UNDO log %u: %m", log_number)));
	}

	/* Write data */
	written = write(fd, data, size);
	if (written != size)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write to UNDO log %u: %m", log_number)));
	}

	/* Sync to disk (durability) */
	if (pg_fsync(fd) < 0)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync UNDO log %u: %m", log_number)));
	}

	close(fd);
}

/*
 * UndoLogRead
 *		Read data from UNDO log at specified pointer
 */
void
UndoLogRead(UndoRecPtr ptr, char *buffer, Size size)
{
	uint32		log_number = UndoRecPtrGetLogNo(ptr);
	uint64		offset = UndoRecPtrGetOffset(ptr);
	int			fd;
	ssize_t		nread;

	if (!UndoRecPtrIsValid(ptr))
		ereport(ERROR,
				(errmsg("invalid UNDO record pointer")));

	if (size == 0)
		return;

	fd = OpenUndoLogFile(log_number, O_RDONLY);

	/* Seek to position */
	if (lseek(fd, offset, SEEK_SET) < 0)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek in UNDO log %u: %m", log_number)));
	}

	/* Read data */
	nread = read(fd, buffer, size);
	if (nread != size)
	{
		int			save_errno = errno;

		close(fd);
		if (nread < 0)
			errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read from UNDO log %u: %m", log_number)));
	}

	close(fd);
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
			LWLockAcquire(&log->lock, LW_SHARED);
			ptr = log->insert_ptr;
			LWLockRelease(&log->lock);
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
 * undo_redo - Replay an UNDO WAL record
 *
 * This function is called during crash recovery to replay UNDO log
 * operations from the WAL.
 */
void
undo_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	char	   *rec = XLogRecGetData(record);

	switch (info)
	{
		case XLOG_UNDO_ALLOCATE:
			{
				xl_undo_allocate *xlrec = (xl_undo_allocate *) rec;
				UndoLogControl *log;
				int			i;

				/*
				 * Find or create the log control structure for this log
				 */
				log = NULL;
				for (i = 0; i < MAX_UNDO_LOGS; i++)
				{
					if (UndoLogShared->logs[i].in_use &&
						UndoLogShared->logs[i].log_number == xlrec->log_number)
					{
						log = &UndoLogShared->logs[i];
						break;
					}
				}

				if (log == NULL)
				{
					/* Log doesn't exist, create it */
					for (i = 0; i < MAX_UNDO_LOGS; i++)
					{
						if (\!UndoLogShared->logs[i].in_use)
						{
							log = &UndoLogShared->logs[i];
							log->log_number = xlrec->log_number;
							log->insert_ptr = xlrec->start_ptr;
							log->discard_ptr = MakeUndoRecPtr(xlrec->log_number, 0);
							log->oldest_xid = InvalidTransactionId;
							log->in_use = true;
							break;
						}
					}
				}

				if (log \!= NULL)
				{
					/*
					 * Update insert pointer to reflect this allocation
					 * No lock needed during recovery (single-threaded)
					 */
					log->insert_ptr = xlrec->start_ptr + xlrec->length;
				}
			}
			break;

		case XLOG_UNDO_DISCARD:
			{
				xl_undo_discard *xlrec = (xl_undo_discard *) rec;
				int			i;

				/* Find the log and update its discard pointer */
				for (i = 0; i < MAX_UNDO_LOGS; i++)
				{
					if (UndoLogShared->logs[i].in_use &&
						UndoLogShared->logs[i].log_number == xlrec->log_number)
					{
						UndoLogControl *log = &UndoLogShared->logs[i];

						log->discard_ptr = xlrec->discard_ptr;
						log->oldest_xid = xlrec->oldest_xid;
						break;
					}
				}
			}
			break;

		case XLOG_UNDO_EXTEND:
			{
				xl_undo_extend *xlrec = (xl_undo_extend *) rec;

				/*
				 * Extend the log file to the specified size
				 * File will be created if it doesn't exist
				 */
				ExtendUndoLogFile(xlrec->log_number, xlrec->new_size);
			}
			break;

		default:
			elog(PANIC, "undo_redo: unknown op code %u", info);
	}
}
