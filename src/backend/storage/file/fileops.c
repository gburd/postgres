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
				default:
					/* No operations registered yet */
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

	switch (info)
	{
		default:
			elog(PANIC, "fileops_redo: unknown op code %u", info);
			break;
	}
}
