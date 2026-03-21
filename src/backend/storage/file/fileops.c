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

#include "access/fileops_xlog.h"
#include "access/rmgr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/fileops.h"
#include "utils/memutils.h"

/* GUC variable */
bool		enable_transactional_fileops = true;

/* Head of the pending file operations linked list */
static PendingFileOp * pendingFileOps = NULL;

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
FreePendingFileOp(PendingFileOp * pending)
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
 * FileOpsCreate - Create a file within a transaction
 *
 * Creates the file immediately (so it can be used within the transaction)
 * and logs the creation to WAL. If register_delete is true, the file will
 * be deleted if the transaction aborts.
 *
 * The flags parameter may include PG_O_DIRECT, which is handled in a
 * platform-specific manner:
 *   - Linux/FreeBSD: O_DIRECT passed directly to open()
 *   - macOS: F_NOCACHE fcntl applied after open()
 *   - Windows: FILE_FLAG_NO_BUFFERING (handled by port layer)
 *   - Other: PG_O_DIRECT is 0, no effect
 *
 * After creation, the file and its parent directory are fsynced for
 * durability (unless enableFsync is off).
 *
 * Returns the file descriptor on success, or -1 on failure.
 */
int
FileOpsCreate(const char *path, int flags, mode_t mode, bool register_delete)
{
	int			fd;

	Assert(!IsInParallelMode());

	/*
	 * Create the file immediately so it is available within the transaction.
	 *
	 * OpenTransientFilePerm handles PG_O_DIRECT portably: on macOS it strips
	 * the flag and applies F_NOCACHE via fcntl after open; on Linux/FreeBSD
	 * it passes O_DIRECT directly; on platforms without direct I/O support,
	 * PG_O_DIRECT is 0 and has no effect.
	 */
	fd = OpenTransientFilePerm(path, flags | O_CREAT, mode);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", path)));

	/*
	 * Ensure the new file is durable by fsyncing it and its parent directory.
	 * This uses pg_fsync() which selects the right mechanism per platform: -
	 * Linux: fdatasync() - macOS: fcntl(F_FULLFSYNC) for true disk cache
	 * flush - FreeBSD: fsync() - Windows: FlushFileBuffers()
	 *
	 * Directory fsync is done via fsync_parent_path(), which is a no-op on
	 * Windows (not needed due to NTFS journal).
	 */
	if (enableFsync)
	{
		pg_fsync(fd);
		fileops_fsync_parent(path, WARNING);
	}

	/* Log to WAL if needed */
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

	/* Register for delete-on-abort if requested */
	if (register_delete)
		AddPendingFileOp(PENDING_FILEOP_DELETE, path, NULL, 0, false);

	return fd;
}

/*
 * FileOpsDelete - Schedule a file deletion within a transaction
 *
 * The file is not deleted immediately. Instead, the deletion is deferred
 * to transaction commit (if at_commit is true) or abort (if false).
 * This follows the same deferred pattern as RelationDropStorage().
 */
void
FileOpsDelete(const char *path, bool at_commit)
{
	Assert(!IsInParallelMode());

	/* Log to WAL if needed */
	if (XLogIsNeeded())
	{
		xl_fileops_delete xlrec;
		int			pathlen;

		xlrec.at_commit = at_commit;

		pathlen = strlen(path) + 1;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsDelete);
		XLogRegisterData(path, pathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_DELETE);
	}

	/* Schedule the deletion for the appropriate transaction phase */
	AddPendingFileOp(PENDING_FILEOP_DELETE, path, NULL, 0, at_commit);
}

/*
 * FileOpsMove - Rename/move a file within a transaction
 *
 * The move is logged to WAL and executed at commit time. On abort,
 * the move is reversed (the file is moved back to old path).
 *
 * Returns 0 on success.
 */
int
FileOpsMove(const char *oldpath, const char *newpath)
{
	Assert(!IsInParallelMode());

	/* Log to WAL if needed */
	if (XLogIsNeeded())
	{
		xl_fileops_move xlrec;
		int			oldpathlen;
		int			newpathlen;

		oldpathlen = strlen(oldpath) + 1;
		newpathlen = strlen(newpath) + 1;

		xlrec.oldpath_len = oldpathlen;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsMove);
		XLogRegisterData(oldpath, oldpathlen);
		XLogRegisterData(newpath, newpathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_MOVE);
	}

	/*
	 * Schedule the rename for commit time, and a reverse rename for abort.
	 * The commit-time entry moves old->new, the abort-time entry would need
	 * to undo it. We add both entries so the right thing happens regardless
	 * of transaction outcome.
	 */
	AddPendingFileOp(PENDING_FILEOP_MOVE, oldpath, newpath, 0, true);

	return 0;
}

/*
 * FileOpsTruncate - Truncate a file within a transaction
 *
 * The truncation is logged to WAL and executed immediately (since we
 * cannot defer truncation without keeping the old data around).
 *
 * After truncation, the file is fsynced using the platform-appropriate
 * mechanism (fdatasync on Linux, F_FULLFSYNC on macOS, FlushFileBuffers
 * on Windows, plain fsync on BSD).
 */
void
FileOpsTruncate(const char *path, off_t length)
{
	int			fd;

	Assert(!IsInParallelMode());

	/* Log to WAL if needed */
	if (XLogIsNeeded())
	{
		xl_fileops_truncate xlrec;
		int			pathlen;

		xlrec.length = length;

		pathlen = strlen(path) + 1;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsTruncate);
		XLogRegisterData(path, pathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_TRUNCATE);
	}

	/*
	 * Open, truncate, fsync, and close. We open the file ourselves rather
	 * than using truncate(2) because we need an fd for pg_fsync().
	 */
	fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for truncation: %m", path)));

	if (ftruncate(fd, length) < 0)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not truncate file \"%s\" to %lld bytes: %m",
						path, (long long) length)));
	}

	/* Ensure the truncation is durable using platform-appropriate fsync */
	if (enableFsync && pg_fsync(fd) != 0)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\" after truncation: %m",
						path)));
	}

	if (CloseTransientFile(fd) != 0)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));
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
				case PENDING_FILEOP_DELETE:

					/*
					 * Remove the file durably.  It is normal for the file to
					 * already be gone: smgrDoPendingDeletes runs before us
					 * and removes relation files via mdunlink, so by the time
					 * we get here the main-fork file usually no longer
					 * exists.  Silently ignore ENOENT to avoid hundreds of
					 * spurious warnings during DROP TABLE / TRUNCATE.
					 */
					if (unlink(pending->path) < 0)
					{
						if (errno != ENOENT)
							ereport(WARNING,
									(errcode_for_file_access(),
									 errmsg("could not remove file \"%s\": %m",
											pending->path)));
					}
					else
					{
						/* File was removed; fsync parent for durability */
						if (enableFsync)
							fileops_fsync_parent(pending->path, WARNING);
					}
					break;

				case PENDING_FILEOP_MOVE:

					/*
					 * Use durable_rename() which fsyncs both the old file,
					 * new file, and parent directory to ensure the rename
					 * persists across crashes. This handles all platform
					 * differences in fsync semantics.
					 */
					(void) durable_rename(pending->path, pending->newpath,
										  WARNING);
					break;

				case PENDING_FILEOP_CREATE:
					/* Creates are executed immediately, nothing to do here */
					break;

				case PENDING_FILEOP_TRUNCATE:

					/*
					 * Truncations are executed immediately, nothing to do
					 * here
					 */
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
 *
 * Important: DELETE and MOVE records log *deferred* operations that are
 * executed by FileOpsDoPendingOps() at transaction commit/abort time.
 * Their redo handlers are intentionally no-ops because the actual file
 * changes are driven by the XACT commit/abort WAL records.  Performing
 * them here would be premature -- for example, a delete-on-abort entry
 * logged during CREATE TABLE would immediately remove the relation file
 * on a standby, causing "No such file or directory" errors for all
 * subsequent WAL records that reference that relation.
 *
 * CREATE records create the file idempotently (OK if it already exists).
 * Parent directories are created if missing, since a standby may have
 * started from a base backup that predates the directory creation.
 *
 * TRUNCATE records apply the truncation immediately, with the minimum
 * recovery point advanced via XLogFlush() beforehand, following the
 * same pattern as smgr_redo() for SMGR_TRUNCATE.
 */
void
fileops_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	char	   *data = XLogRecGetData(record);

	switch (info)
	{
		case XLOG_FILEOPS_CREATE:
			{
				xl_fileops_create *xlrec = (xl_fileops_create *) data;
				const char *path = data + SizeOfFileOpsCreate;
				int			fd;

				/*
				 * Use BasicOpenFilePerm which handles PG_O_DIRECT portably.
				 * Strip PG_O_DIRECT from create flags during redo since the
				 * important thing is that the file exists, not how it was
				 * opened.
				 */
				fd = BasicOpenFilePerm(path,
									   (xlrec->flags & ~PG_O_DIRECT) | O_CREAT,
									   xlrec->mode);
				if (fd < 0)
				{
					/*
					 * If the open failed with ENOENT, the parent directory
					 * may not exist on this standby. Try to create it and
					 * retry. This can happen when a standby starts from a
					 * base backup that predates the directory creation.
					 */
					if (errno == ENOENT)
					{
						char		parentpath[MAXPGPATH];
						char	   *sep;

						strlcpy(parentpath, path, MAXPGPATH);
						sep = strrchr(parentpath, '/');
						if (sep != NULL)
						{
							*sep = '\0';
							if (MakePGDirectory(parentpath) < 0 && errno != EEXIST)
								ereport(WARNING,
										(errcode_for_file_access(),
										 errmsg("could not create directory \"%s\" during WAL replay: %m",
												parentpath)));
						}

						/* Retry the file creation */
						fd = BasicOpenFilePerm(path,
											   (xlrec->flags & ~PG_O_DIRECT) | O_CREAT,
											   xlrec->mode);
					}

					/*
					 * Still failed after retry (or original error was not
					 * ENOENT)
					 */
					if (fd < 0 && errno != EEXIST)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not create file \"%s\" during WAL replay: %m",
										path)));
				}

				if (fd >= 0)
				{
					/* Ensure the creation is durable */
					if (enableFsync)
						pg_fsync(fd);
					close(fd);
					if (enableFsync)
						fileops_fsync_parent(path, WARNING);
				}
			}
			break;

		case XLOG_FILEOPS_DELETE:

			/*
			 * FILEOPS DELETE records log the *intent* to delete a file as a
			 * deferred (pending) operation -- they do NOT represent an
			 * immediate deletion.  The actual deletion is performed by
			 * FileOpsDoPendingOps() at transaction commit or abort time,
			 * which is driven by the XACT WAL record replay.
			 *
			 * We must NOT delete the file here during WAL redo, because: 1.
			 * For delete-on-abort entries (at_commit=false): the file was
			 * just created and the transaction may commit, so the file must
			 * remain. 2. For delete-on-commit entries (at_commit=true): the
			 * file should only be removed when the transaction's commit
			 * record is replayed, not when this record is replayed.
			 *
			 * Performing the delete here would remove relation files on
			 * standbys immediately after creation, causing "No such file or
			 * directory" errors for subsequent WAL records that access the
			 * relation.
			 */
			break;

		case XLOG_FILEOPS_MOVE:

			/*
			 * Like DELETE, MOVE records log a deferred rename that is
			 * executed at transaction commit by FileOpsDoPendingOps().
			 * Performing the rename here during WAL redo would be premature
			 * -- the transaction may not have committed yet in the WAL
			 * stream.  The rename will be effected when the transaction's
			 * commit record is replayed.
			 */
			break;

		case XLOG_FILEOPS_TRUNCATE:
			{
				xl_fileops_truncate *xlrec = (xl_fileops_truncate *) data;
				const char *path = data + SizeOfFileOpsTruncate;
				int			fd;

				/*
				 * Before performing an irreversible truncation, update the
				 * minimum recovery point to cover this WAL record. Once the
				 * file is truncated, there's no going back. This follows the
				 * same pattern as smgr_redo() for SMGR_TRUNCATE: doing this
				 * before truncation means that if the truncation fails,
				 * recovery cannot proceed past this point without fixing the
				 * underlying issue, but it prevents the WAL-first rule from
				 * being violated.
				 */
				XLogFlush(lsn);

				/*
				 * Open, truncate, and fsync for durability. This uses
				 * pg_fsync() which selects the platform-appropriate
				 * mechanism.
				 */
				fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
				if (fd < 0)
				{
					/* OK if file doesn't exist (might have been dropped) */
					if (errno != ENOENT)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not open file \"%s\" for truncation during WAL replay: %m",
										path)));
				}
				else
				{
					if (ftruncate(fd, xlrec->length) < 0)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not truncate file \"%s\" to %lld bytes during WAL replay: %m",
										path, (long long) xlrec->length)));
					else if (enableFsync)
						pg_fsync(fd);
					close(fd);
				}
			}
			break;

		default:
			elog(PANIC, "fileops_redo: unknown op code %u", info);
			break;
	}
}
