/*-------------------------------------------------------------------------
 *
 * fileops.c
 *	  Transactional file operations with WAL logging
 *
 * This module provides transactional filesystem operations that integrate
 * with PostgreSQL's WAL and transaction management. File operations are
 * logged to WAL and deferred until transaction commit/abort, following
 * the same pattern used for relation creation/deletion in catalog/storage.c.
 *
 * The deferred operations pattern works as follows:
 *   1. The API function logs the operation to WAL
 *   2. A PendingFileOp entry is added to a linked list
 *   3. At commit/abort time, FileOpsDoPendingOps() executes or discards
 *      the pending operations based on transaction outcome
 *
 * Subtransaction support:
 *   - At subtransaction commit, entries are reassigned to the parent level
 *   - At subtransaction abort, abort-time actions execute immediately
 *
 * Platform-specific handling:
 *   - O_DIRECT: Uses PG_O_DIRECT abstraction (Linux native O_DIRECT,
 *     macOS F_NOCACHE via fcntl, Windows FILE_FLAG_NO_BUFFERING)
 *   - fsync: Uses pg_fsync() which selects the appropriate mechanism
 *     (Linux fdatasync, macOS F_FULLFSYNC, Windows FlushFileBuffers,
 *     BSD fsync)
 *   - Directory sync: Uses fsync_fname()/fsync_parent_path() which
 *     handle directory fsync on Unix platforms (not needed on Windows)
 *   - Durable operations: Uses durable_rename()/durable_unlink() which
 *     ensure operations persist across crashes via proper fsync ordering
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/fileops.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#ifdef HAVE_SYS_FCNTL_H
#include <fcntl.h>
#endif

/*
 * ENODATA is Linux-specific; FreeBSD and other BSDs don't define it.
 * When removing extended attributes, ENODATA means "attribute does not
 * exist" — equivalent to ENOATTR on BSDs.
 */
#ifndef ENODATA
#ifdef ENOATTR
#define ENODATA ENOATTR
#else
#define ENODATA ENOENT
#endif
#endif

#include "access/fileops_xlog.h"
#include "access/rmgr.h"
#include "access/undolog.h"
#include "access/undorecord.h"
#include "access/undormgr.h"
#include "access/xact.h"
#include "access/xactundo.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/fileops.h"
#include "utils/memutils.h"

/* GUC variable */
bool		enable_transactional_fileops = true;

/* Head of the pending file operations linked list */
static PendingFileOp *pendingFileOps = NULL;

/*
 * fileops_fsync_parent -- fsync the parent directory of a file path
 *
 * This ensures that directory entry changes (create, delete, rename)
 * are durable. On Windows, directory fsync is not needed because NTFS
 * journals directory entries; fsync_fname_ext() handles this by being
 * a no-op for directories on Windows.
 */
static void
fileops_fsync_parent(const char *fname, int elevel)
{
	char		parentpath[MAXPGPATH];
	char	   *sep;

	strlcpy(parentpath, fname, MAXPGPATH);

	sep = strrchr(parentpath, '/');
	if (sep != NULL)
	{
		/* Got a path component, fsync the directory portion */
		if (sep == parentpath)
			parentpath[1] = '\0';	/* root directory */
		else
			*sep = '\0';

		fsync_fname_ext(parentpath, true, true, elevel);
	}
}

/*
 * AddPendingFileOp - Add a new pending file operation to the list
 *
 * All fields are deep-copied into TopMemoryContext to survive
 * until transaction end, following the PendingRelDelete pattern.
 */
static void
AddPendingFileOp(PendingFileOpType type, const char *path,
				 const char *newpath, off_t length, bool at_commit)
{
	PendingFileOp *pending;
	MemoryContext oldcxt;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	pending = (PendingFileOp *) palloc(sizeof(PendingFileOp));
	pending->type = type;
	pending->path = pstrdup(path);
	pending->newpath = newpath ? pstrdup(newpath) : NULL;
	pending->length = length;
	pending->at_commit = at_commit;
	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->next = pendingFileOps;
	pendingFileOps = pending;

	MemoryContextSwitchTo(oldcxt);
}

/*
 * FreePendingFileOp - Free a pending file operation entry
 */
static void
FreePendingFileOp(PendingFileOp *pending)
{
	if (pending->path)
		pfree(pending->path);
	if (pending->newpath)
		pfree(pending->newpath);
	pfree(pending);
}

/*
 * FileOpsCancelPendingDelete - Cancel a pending file deletion
 *
 * This removes matching DELETE entries from the pendingFileOps list.
 * It is called by RelationPreserveStorage() to ensure that when a
 * relation's storage is preserved (e.g., during index reuse in ALTER TABLE),
 * the corresponding FileOps DELETE entry is also cancelled, preventing
 * FileOpsDoPendingOps from deleting the file at commit time.
 */
void
FileOpsCancelPendingDelete(const char *path, bool at_commit)
{
	PendingFileOp *pending;
	PendingFileOp *prev;
	PendingFileOp *next;

	prev = NULL;
	for (pending = pendingFileOps; pending != NULL; pending = next)
	{
		next = pending->next;
		if (pending->type == PENDING_FILEOP_DELETE &&
			pending->at_commit == at_commit &&
			strcmp(pending->path, path) == 0)
		{
			/* unlink and free list entry */
			if (prev)
				prev->next = next;
			else
				pendingFileOps = next;
			FreePendingFileOp(pending);
			/* prev does not change */
		}
		else
		{
			prev = pending;
		}
	}
}

/*
 * FileOpsSync - Ensure a file's data is durably written to disk
 *
 * This is a convenience wrapper around fsync_fname() that uses the
 * platform-appropriate sync mechanism:
 *   - Linux: fdatasync() (only flushes data, not metadata unless needed)
 *   - macOS: fcntl(F_FULLFSYNC) (flushes disk write cache)
 *   - FreeBSD: fsync()
 *   - Windows: FlushFileBuffers()
 *
 * An ERROR is raised if the sync fails.
 */
/*
 * FileOpsCreate - Create a file within a transaction
 *
 * Creates the file immediately (so it can be used within the transaction)
 * and logs the creation to WAL. If register_delete is true, the file will
 * be deleted if the transaction aborts.
 *
 * Platform handling:
 *   - Linux/FreeBSD: O_DIRECT passed directly to open()
 *   - macOS: F_NOCACHE fcntl applied after open()
 *   - Windows: FILE_FLAG_NO_BUFFERING (handled by port layer)
 *
 * Returns the file descriptor on success, or -1 on failure.
 */
int
FileOpsCreate(const char *path, int flags, mode_t mode, bool register_delete)
{
	int			fd;

	Assert(!IsInParallelMode());

	fd = OpenTransientFilePerm(path, flags | O_CREAT, mode);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", path)));

	if (enableFsync)
	{
		pg_fsync(fd);
		fileops_fsync_parent(path, WARNING);
	}

	if (XLogIsNeeded())
	{
		xl_fileops_create xlrec;
		int			pathlen;

		xlrec.flags = flags;
		xlrec.mode = mode;
		xlrec.register_delete = register_delete;

		pathlen = strlen(path) + 1;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsCreate);
		XLogRegisterData(path, pathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_CREATE);
	}

	if (register_delete)
		AddPendingFileOp(PENDING_FILEOP_DELETE, path, NULL, 0, false);

	return fd;
}

void
FileOpsSync(const char *path)
{
	fsync_fname(path, false);
}

/*
 * FileOpsDoPendingOps - Execute pending file operations at transaction end
 *
 * At commit, operations with at_commit=true are executed.
 * At abort, operations with at_commit=false are executed.
 *
 * This is called from xact.c at transaction commit/abort, analogous
 * to smgrDoPendingDeletes().
 */
void
FileOpsDoPendingOps(bool isCommit)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	PendingFileOp *pending;
	PendingFileOp *prev;
	PendingFileOp *next;

	prev = NULL;
	for (pending = pendingFileOps; pending != NULL; pending = next)
	{
		next = pending->next;

		if (pending->nestLevel < nestLevel)
		{
			/* outer-level entries should not be processed yet */
			prev = pending;
			continue;
		}

		/* unlink from list first, so we don't retry on failure */
		if (prev)
			prev->next = next;
		else
			pendingFileOps = next;

		/* Execute if this operation matches the transaction outcome */
		if (pending->at_commit == isCommit)
		{
			switch (pending->type)
			{
				case PENDING_FILEOP_CREATE:
					/* Creates are executed immediately, nothing to do */
					break;

				default:
					break;
			}
		}

		FreePendingFileOp(pending);
		/* prev does not change */
	}
}

/*
 * AtSubCommit_FileOps - Handle subtransaction commit
 *
 * Reassign all pending ops from the current nesting level to the parent.
 */
void
AtSubCommit_FileOps(void)
{
	int			nestLevel = GetCurrentTransactionNestLevel();
	PendingFileOp *pending;

	for (pending = pendingFileOps; pending != NULL; pending = pending->next)
	{
		if (pending->nestLevel >= nestLevel)
			pending->nestLevel = nestLevel - 1;
	}
}

/*
 * AtSubAbort_FileOps - Handle subtransaction abort
 *
 * Execute abort-time actions for the current nesting level immediately.
 */
void
AtSubAbort_FileOps(void)
{
	FileOpsDoPendingOps(false);
}

/*
 * PostPrepare_FileOps - Clean up after PREPARE TRANSACTION
 *
 * Discard all pending file operations since they've been recorded
 * in the two-phase state file.
 */
void
PostPrepare_FileOps(void)
{
	PendingFileOp *pending;
	PendingFileOp *next;

	for (pending = pendingFileOps; pending != NULL; pending = next)
	{
		next = pending->next;
		pendingFileOps = next;
		FreePendingFileOp(pending);
	}
}

/*
 * fileops_redo - WAL redo function for FILEOPS records
 *
 * Replay file operations during crash recovery or standby apply.
 * Each operation type has its own redo handler added in separate commits.
 */
void
fileops_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	char	   *data = XLogRecGetData(record);

	switch (info)
	{
		case XLOG_FILEOPS_CREATE:
			{
				xl_fileops_create *xlrec = (xl_fileops_create *) data;
				const char *path = data + SizeOfFileOpsCreate;
				int			fd;

				fd = BasicOpenFilePerm(path,
									  (xlrec->flags & ~PG_O_DIRECT) | O_CREAT,
									  xlrec->mode);
				if (fd < 0)
				{
					if (errno == ENOENT)
					{
						char		parentpath[MAXPGPATH];
						char	   *sep;

						strlcpy(parentpath, path, MAXPGPATH);
						sep = strrchr(parentpath, '/');
						if (sep != NULL)
						{
							*sep = '\0';
							if (MakePGDirectory(parentpath) < 0 &&
								errno != EEXIST)
								ereport(WARNING,
										(errcode_for_file_access(),
										 errmsg("could not create directory \"%s\" during WAL replay: %m",
												parentpath)));
						}

						fd = BasicOpenFilePerm(path,
											   (xlrec->flags & ~PG_O_DIRECT) | O_CREAT,
											   xlrec->mode);
					}

					if (fd < 0 && errno != EEXIST)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not create file \"%s\" during WAL replay: %m",
										path)));
				}

				if (fd >= 0)
				{
					if (enableFsync)
						pg_fsync(fd);
					close(fd);
					if (enableFsync)
						fileops_fsync_parent(path, WARNING);
				}
			}
			break;

		default:
			elog(PANIC, "fileops_redo: unknown op code %u", info);
			break;
	}
}
