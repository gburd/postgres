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
 * exist" -- equivalent to ENOATTR on BSDs.
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
#include "port/pg_xattr.h"
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

/*
 * FileOpsDelete - Schedule a file deletion within a transaction
 *
 * The file is not deleted immediately. Instead, the deletion is deferred
 * to transaction commit (if at_commit is true) or abort (if false).
 * On Unix: unlink() with parent dir fsync.
 * On Windows: pgunlink() with retry on EACCES.
 */
void
FileOpsDelete(const char *path, bool at_commit)
{
	Assert(!IsInParallelMode());

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

	AddPendingFileOp(PENDING_FILEOP_DELETE, path, NULL, 0, at_commit);
}

/*
 * FileOpsRename - Rename a file within a transaction
 *
 * The rename is deferred to commit time. Uses durable_rename() internally
 * which handles all platform differences:
 *   - Unix: rename() with fsync of both old and new parent dirs
 *   - Windows: MoveFileEx(MOVEFILE_REPLACE_EXISTING) with retry
 */
int
FileOpsRename(const char *oldpath, const char *newpath)
{
	Assert(!IsInParallelMode());

	if (XLogIsNeeded())
	{
		xl_fileops_rename xlrec;
		int			oldpathlen;
		int			newpathlen;

		oldpathlen = strlen(oldpath) + 1;
		newpathlen = strlen(newpath) + 1;

		xlrec.oldpath_len = oldpathlen;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsRename);
		XLogRegisterData(oldpath, oldpathlen);
		XLogRegisterData(newpath, newpathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_RENAME);
	}

	AddPendingFileOp(PENDING_FILEOP_RENAME, oldpath, newpath, 0, true);

	return 0;
}

/*
 * FileOpsWrite - Write data to a file at a specific offset
 *
 * Immediate execution, WAL-logged for crash recovery replay.
 * Uses pwrite() on POSIX. On Windows: SetFilePointerEx + WriteFile.
 *
 * Returns 0 on success, -1 on failure.
 */
int
FileOpsWrite(const char *path, off_t offset, const void *data, uint32 len)
{
	int			fd;
	ssize_t		nbytes;

	Assert(!IsInParallelMode());

	fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for writing: %m", path)));

	nbytes = pg_pwrite(fd, data, len, offset);
	if (nbytes != (ssize_t) len)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write %u bytes to file \"%s\" at offset %lld: %m",
						len, path, (long long) offset)));
	}

	if (enableFsync && pg_fsync(fd) != 0)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\" after write: %m", path)));
	}

	if (CloseTransientFile(fd) != 0)
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));

	if (XLogIsNeeded())
	{
		xl_fileops_write xlrec;
		int			pathlen;

		pathlen = strlen(path) + 1;

		xlrec.offset = offset;
		xlrec.len = len;
		xlrec.path_len = pathlen;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsWrite);
		XLogRegisterData(path, pathlen);
		XLogRegisterData(data, len);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_WRITE);
	}

	return 0;
}

/*
 * FileOpsTruncate - Truncate a file within a transaction
 *
 * Executed immediately and WAL-logged. Uses ftruncate() on POSIX,
 * SetEndOfFile() on Windows. File is fsynced after truncation.
 */
void
FileOpsTruncate(const char *path, off_t length)
{
	int			fd;

	Assert(!IsInParallelMode());

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
 * FileOpsChmod - Change file permissions within a transaction
 *
 * Immediate execution, WAL-logged.
 * On POSIX: chmod(). On Windows: _chmod() with limited mode bits
 * (only _S_IREAD/_S_IWRITE; no group/other). Logs WARNING for
 * unsupported mode bits on Windows.
 *
 * Returns 0 on success.
 */
int
FileOpsChmod(const char *path, mode_t mode)
{
	Assert(!IsInParallelMode());

	if (chmod(path, mode) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not chmod file \"%s\" to mode 0%o: %m",
						path, (unsigned int) mode)));

	if (XLogIsNeeded())
	{
		xl_fileops_chmod xlrec;
		int			pathlen;

		xlrec.mode = mode;
		pathlen = strlen(path) + 1;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsChmod);
		XLogRegisterData(path, pathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_CHMOD);
	}

	return 0;
}

/*
 * FileOpsChown - Change file ownership within a transaction
 *
 * Immediate execution, WAL-logged.
 * On POSIX: chown(). On Windows: no-op with WARNING (Windows uses
 * ACLs for ownership, not uid/gid).
 *
 * Returns 0 on success.
 */
int
FileOpsChown(const char *path, uid_t uid, gid_t gid)
{
	Assert(!IsInParallelMode());

#ifndef WIN32
	if (chown(path, uid, gid) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not chown file \"%s\" to %d:%d: %m",
						path, (int) uid, (int) gid)));
#else
	ereport(WARNING,
			(errmsg("chown is not supported on Windows, skipping for \"%s\"",
					path)));
#endif

	if (XLogIsNeeded())
	{
		xl_fileops_chown xlrec;
		int			pathlen;

		xlrec.uid = uid;
		xlrec.gid = gid;
		pathlen = strlen(path) + 1;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsChown);
		XLogRegisterData(path, pathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_CHOWN);
	}

	return 0;
}

void
FileOpsSync(const char *path)
{
	fsync_fname(path, false);
}

/*
 * FileOpsMkdir - Create a directory within a transaction
 *
 * Immediate execution. Optionally registers rmdir-on-abort.
 * Uses MakePGDirectory() pattern (mkdir with pg_dir_create_mode).
 * On Windows: _mkdir() (no mode parameter, permissions from parent).
 *
 * Returns 0 on success.
 */
int
FileOpsMkdir(const char *path, mode_t mode)
{
	Assert(!IsInParallelMode());

	if (MakePGDirectory(path) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create directory \"%s\": %m", path)));

	if (enableFsync)
		fileops_fsync_parent(path, WARNING);

	if (XLogIsNeeded())
	{
		xl_fileops_mkdir xlrec;
		int			pathlen;

		xlrec.mode = mode;
		pathlen = strlen(path) + 1;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsMkdir);
		XLogRegisterData(path, pathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_MKDIR);
	}

	/* Register rmdir-on-abort so directory is cleaned up on rollback */
	AddPendingFileOp(PENDING_FILEOP_RMDIR, path, NULL, 0, false);

	return 0;
}

/*
 * FileOpsRmdir - Remove a directory within a transaction
 *
 * Deferred to commit time (like DELETE). Uses rmdir() on all platforms.
 * On Windows: _rmdir().
 */
void
FileOpsRmdir(const char *path, bool at_commit)
{
	Assert(!IsInParallelMode());

	if (XLogIsNeeded())
	{
		xl_fileops_rmdir xlrec;
		int			pathlen;

		xlrec.at_commit = at_commit;
		pathlen = strlen(path) + 1;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsRmdir);
		XLogRegisterData(path, pathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_RMDIR);
	}

	AddPendingFileOp(PENDING_FILEOP_RMDIR, path, NULL, 0, at_commit);
}

/*
 * FileOpsSymlink - Create a symbolic link within a transaction
 *
 * Immediate execution. Registers delete-on-abort for cleanup.
 * On POSIX: symlink(). On Windows: pgsymlink() which creates NTFS
 * junction points via DeviceIoControl(). Note: junction points only
 * work for directories on Windows.
 *
 * Returns 0 on success.
 */
int
FileOpsSymlink(const char *target, const char *linkpath)
{
	Assert(!IsInParallelMode());

	if (symlink(target, linkpath) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create symbolic link \"%s\" -> \"%s\": %m",
						linkpath, target)));

	if (enableFsync)
		fileops_fsync_parent(linkpath, WARNING);

	if (XLogIsNeeded())
	{
		xl_fileops_symlink xlrec;
		int			targetlen;
		int			linkpathlen;

		targetlen = strlen(target) + 1;
		linkpathlen = strlen(linkpath) + 1;
		xlrec.target_len = targetlen;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsSymlink);
		XLogRegisterData(target, targetlen);
		XLogRegisterData(linkpath, linkpathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_SYMLINK);
	}

	/* Register delete-on-abort to clean up the symlink on rollback */
	AddPendingFileOp(PENDING_FILEOP_DELETE, linkpath, NULL, 0, false);

	return 0;
}

/*
 * FileOpsLink - Create a hard link within a transaction
 *
 * Immediate execution. Registers delete-on-abort for cleanup.
 * On POSIX: link(). On Windows: CreateHardLinkA() (NTFS only).
 *
 * Returns 0 on success.
 */
int
FileOpsLink(const char *oldpath, const char *newpath)
{
	Assert(!IsInParallelMode());

	if (link(oldpath, newpath) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create hard link \"%s\" -> \"%s\": %m",
						newpath, oldpath)));

	if (enableFsync)
		fileops_fsync_parent(newpath, WARNING);

	if (XLogIsNeeded())
	{
		xl_fileops_link xlrec;
		int			oldpathlen;
		int			newpathlen;

		oldpathlen = strlen(oldpath) + 1;
		newpathlen = strlen(newpath) + 1;
		xlrec.oldpath_len = oldpathlen;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsLink);
		XLogRegisterData(oldpath, oldpathlen);
		XLogRegisterData(newpath, newpathlen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_LINK);
	}

	/* Register delete-on-abort to clean up the link on rollback */
	AddPendingFileOp(PENDING_FILEOP_DELETE, newpath, NULL, 0, false);

	return 0;
}

/*
 * FileOpsSetXattr - Set an extended attribute on a file
 *
 * Immediate execution, WAL-logged.
 * Uses pg_setxattr() portability layer which handles:
 *   - Linux: setxattr()
 *   - macOS: setxattr() with extra options parameter
 *   - FreeBSD: extattr_set_file()
 *   - Windows: NTFS Alternate Data Streams
 *   - Other: returns ENOTSUP
 *
 * Returns 0 on success.
 */
int
FileOpsSetXattr(const char *path, const char *name,
				const void *value, size_t len)
{
	Assert(!IsInParallelMode());

	if (pg_setxattr(path, name, value, len) < 0)
	{
		if (errno == ENOTSUP)
			ereport(WARNING,
					(errmsg("extended attributes not supported on this platform, skipping setxattr for \"%s\"",
							path)));
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not set extended attribute \"%s\" on \"%s\": %m",
							name, path)));
	}

	if (XLogIsNeeded())
	{
		xl_fileops_setxattr xlrec;
		int			pathlen;
		int			namelen;

		pathlen = strlen(path) + 1;
		namelen = strlen(name) + 1;

		xlrec.path_len = (uint16) pathlen;
		xlrec.name_len = (uint16) namelen;
		xlrec.value_len = (uint32) len;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsSetxattr);
		XLogRegisterData(path, pathlen);
		XLogRegisterData(name, namelen);
		XLogRegisterData(value, (uint32) len);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_SETXATTR);
	}

	return 0;
}

/*
 * FileOpsRemoveXattr - Remove an extended attribute from a file
 *
 * Immediate execution, WAL-logged.
 * Uses pg_removexattr() portability layer.
 *
 * Returns 0 on success.
 */
int
FileOpsRemoveXattr(const char *path, const char *name)
{
	Assert(!IsInParallelMode());

	if (pg_removexattr(path, name) < 0)
	{
		if (errno == ENOTSUP)
			ereport(WARNING,
					(errmsg("extended attributes not supported on this platform, skipping removexattr for \"%s\"",
							path)));
		else if (errno != PG_ENOATTR)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not remove extended attribute \"%s\" from \"%s\": %m",
							name, path)));
	}

	if (XLogIsNeeded())
	{
		xl_fileops_removexattr xlrec;
		int			pathlen;
		int			namelen;

		pathlen = strlen(path) + 1;
		namelen = strlen(name) + 1;

		xlrec.path_len = (uint16) pathlen;
		xlrec.name_len = (uint16) namelen;

		XLogBeginInsert();
		XLogRegisterData(&xlrec, SizeOfFileOpsRemovexattr);
		XLogRegisterData(path, pathlen);
		XLogRegisterData(name, namelen);
		XLogInsert(RM_FILEOPS_ID, XLOG_FILEOPS_REMOVEXATTR);
	}

	return 0;
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

				case PENDING_FILEOP_RENAME:
					(void) durable_rename(pending->path, pending->newpath,
										  WARNING);
					break;

				case PENDING_FILEOP_DELETE:
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
						if (enableFsync)
							fileops_fsync_parent(pending->path, WARNING);
					}
					break;

				case PENDING_FILEOP_RMDIR:
					if (rmdir(pending->path) < 0)
					{
						if (errno != ENOENT)
							ereport(WARNING,
									(errcode_for_file_access(),
									 errmsg("could not remove directory \"%s\": %m",
											pending->path)));
					}
					else
					{
						if (enableFsync)
							fileops_fsync_parent(pending->path, WARNING);
					}
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

		case XLOG_FILEOPS_WRITE:
			{
				xl_fileops_write *xlrec = (xl_fileops_write *) data;
				const char *path = data + SizeOfFileOpsWrite;
				const char *wdata = path + xlrec->path_len;
				int			fd;

				fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
				if (fd < 0)
				{
					if (errno != ENOENT)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not open file \"%s\" for write during WAL replay: %m",
										path)));
				}
				else
				{
					if (pg_pwrite(fd, wdata, xlrec->len, xlrec->offset) !=
						(ssize_t) xlrec->len)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("could not write to file \"%s\" during WAL replay: %m",
										path)));
					else if (enableFsync)
						pg_fsync(fd);
					close(fd);
				}
			}
			break;

		case XLOG_FILEOPS_RENAME:

			/*
			 * RENAME records log deferred renames executed by
			 * FileOpsDoPendingOps() at transaction commit. Intentional no-op
			 * during redo.
			 */
			break;

		case XLOG_FILEOPS_DELETE:

			/*
			 * DELETE records log deferred operations executed by
			 * FileOpsDoPendingOps() at transaction commit/abort. Intentional
			 * no-op during redo.
			 */
			break;

		case XLOG_FILEOPS_SYMLINK:
			{
				xl_fileops_symlink *xlrec = (xl_fileops_symlink *) data;
				const char *target = data + SizeOfFileOpsSymlink;
				const char *linkpath = target + xlrec->target_len;

				/* Remove existing link first for idempotent redo */
				unlink(linkpath);
				if (symlink(target, linkpath) < 0 && errno != EEXIST)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not create symbolic link \"%s\" during WAL replay: %m",
									linkpath)));
				else if (enableFsync)
					fileops_fsync_parent(linkpath, WARNING);
			}
			break;

		case XLOG_FILEOPS_LINK:
			{
				xl_fileops_link *xlrec = (xl_fileops_link *) data;
				const char *oldpath = data + SizeOfFileOpsLink;
				const char *newpath = oldpath + xlrec->oldpath_len;

				/* Remove existing link first for idempotent redo */
				unlink(newpath);
				if (link(oldpath, newpath) < 0 && errno != EEXIST)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not create hard link \"%s\" during WAL replay: %m",
									newpath)));
				else if (enableFsync)
					fileops_fsync_parent(newpath, WARNING);
			}
			break;

		case XLOG_FILEOPS_MKDIR:
			{
				xl_fileops_mkdir *xlrec pg_attribute_unused() =
					(xl_fileops_mkdir *) data;
				const char *path = data + SizeOfFileOpsMkdir;

				if (MakePGDirectory(path) < 0 && errno != EEXIST)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not create directory \"%s\" during WAL replay: %m",
									path)));
				else if (enableFsync)
					fileops_fsync_parent(path, WARNING);
			}
			break;

		case XLOG_FILEOPS_RMDIR:

			/*
			 * RMDIR records log deferred operations, like DELETE. Intentional
			 * no-op during redo.
			 */
			break;

		case XLOG_FILEOPS_CHMOD:
			{
				xl_fileops_chmod *xlrec = (xl_fileops_chmod *) data;
				const char *path = data + SizeOfFileOpsChmod;

				if (chmod(path, xlrec->mode) < 0 && errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not chmod file \"%s\" during WAL replay: %m",
									path)));
			}
			break;

		case XLOG_FILEOPS_CHOWN:
			{
				xl_fileops_chown *xlrec = (xl_fileops_chown *) data;
				const char *path = data + SizeOfFileOpsChown;

#ifndef WIN32
				if (chown(path, xlrec->uid, xlrec->gid) < 0 &&
					errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not chown file \"%s\" during WAL replay: %m",
									path)));
#endif
			}
			break;

		case XLOG_FILEOPS_TRUNCATE:
			{
				xl_fileops_truncate *xlrec = (xl_fileops_truncate *) data;
				const char *path = data + SizeOfFileOpsTruncate;
				int			fd;

				XLogFlush(record->EndRecPtr);

				fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
				if (fd < 0)
				{
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

		case XLOG_FILEOPS_SETXATTR:
			{
				xl_fileops_setxattr *xlrec = (xl_fileops_setxattr *) data;
				const char *path = data + SizeOfFileOpsSetxattr;
				const char *name = path + xlrec->path_len;
				const void *value = name + xlrec->name_len;

				if (pg_setxattr(path, name, value, xlrec->value_len) < 0 &&
					errno != ENOTSUP && errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not set extended attribute \"%s\" on \"%s\" during WAL replay: %m",
									name, path)));
			}
			break;

		case XLOG_FILEOPS_REMOVEXATTR:
			{
				xl_fileops_removexattr *xlrec =
					(xl_fileops_removexattr *) data;
				const char *path = data + SizeOfFileOpsRemovexattr;
				const char *name = path + xlrec->path_len;

				if (pg_removexattr(path, name) < 0 &&
					errno != ENOTSUP && errno != ENOENT &&
					errno != PG_ENOATTR)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not remove extended attribute \"%s\" from \"%s\" during WAL replay: %m",
									name, path)));
			}
			break;

		default:
			elog(PANIC, "fileops_redo: unknown op code %u", info);
			break;
	}
}
