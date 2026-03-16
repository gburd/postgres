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

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/fileops.h"
#include "utils/memutils.h"

/* Head of the pending file operations linked list */
static PendingFileOp *pendingFileOps = NULL;

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
 * FileOpsCreate - Create a file within a transaction
 *
 * Creates the file immediately (so it can be used within the transaction)
 * and logs the creation to WAL. If register_delete is true, the file will
 * be deleted if the transaction aborts.
 *
 * Returns the file descriptor on success, or -1 on failure.
 */
int
FileOpsCreate(const char *path, int flags, mode_t mode, bool register_delete)
{
	int			fd;

	Assert(!IsInParallelMode());

	/* Create the file immediately so it is available within the transaction */
	fd = OpenTransientFilePerm(path, flags | O_CREAT, mode);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", path)));

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
		XLogRegisterData(unconstify(char *, path), pathlen);
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
		XLogRegisterData(unconstify(char *, path), pathlen);
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
		XLogRegisterData(unconstify(char *, oldpath), oldpathlen);
		XLogRegisterData(unconstify(char *, newpath), newpathlen);
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
 */
void
FileOpsTruncate(const char *path, off_t length)
{
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
		XLogRegisterData(unconstify(char *, path), pathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_TRUNCATE);
	}

	/* Execute truncation immediately - it cannot be deferred */
	if (truncate(path, length) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not truncate file \"%s\" to %lld bytes: %m",
						path, (long long) length)));
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
					/* Best effort - ignore errors (file might already be gone) */
					(void) unlink(pending->path);
					break;

				case PENDING_FILEOP_MOVE:
					if (rename(pending->path, pending->newpath) < 0)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not rename file \"%s\" to \"%s\": %m",
										pending->path, pending->newpath)));
					break;

				case PENDING_FILEOP_CREATE:
					/* Creates are executed immediately, nothing to do here */
					break;

				case PENDING_FILEOP_TRUNCATE:
					/* Truncations are executed immediately, nothing to do here */
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
 * Operations are designed to be idempotent:
 *   - CREATE: OK if file already exists
 *   - DELETE: OK if file is already missing
 *   - MOVE: Check destination existence before failing
 *   - TRUNCATE: Apply truncation unconditionally
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
									  xlrec->flags | O_CREAT,
									  xlrec->mode);
				if (fd < 0)
				{
					/* OK if the file already exists */
					if (errno != EEXIST)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not create file \"%s\" during WAL replay: %m",
										path)));
				}
				else
					close(fd);
			}
			break;

		case XLOG_FILEOPS_DELETE:
			{
				const char *path = data + SizeOfFileOpsDelete;

				/* OK if file is already missing */
				if (unlink(path) < 0 && errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not delete file \"%s\" during WAL replay: %m",
									path)));
			}
			break;

		case XLOG_FILEOPS_MOVE:
			{
				xl_fileops_move *xlrec = (xl_fileops_move *) data;
				const char *oldpath = data + SizeOfFileOpsMove;
				const char *newpath = oldpath + xlrec->oldpath_len;

				if (rename(oldpath, newpath) < 0)
				{
					/*
					 * If the source is missing but the destination exists,
					 * the move was already applied - that's OK.
					 */
					if (!(errno == ENOENT &&
						  access(newpath, F_OK) == 0))
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not rename file \"%s\" to \"%s\" during WAL replay: %m",
										oldpath, newpath)));
				}
			}
			break;

		case XLOG_FILEOPS_TRUNCATE:
			{
				xl_fileops_truncate *xlrec = (xl_fileops_truncate *) data;
				const char *path = data + SizeOfFileOpsTruncate;

				if (truncate(path, xlrec->length) < 0)
				{
					/* OK if file doesn't exist (might have been dropped) */
					if (errno != ENOENT)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not truncate file \"%s\" to %lld bytes during WAL replay: %m",
										path, (long long) xlrec->length)));
				}
			}
			break;

		default:
			elog(PANIC, "fileops_redo: unknown op code %u", info);
			break;
	}
}
