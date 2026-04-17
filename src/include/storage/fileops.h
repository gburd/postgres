/*-------------------------------------------------------------------------
 *
 * fileops.h
 *	  Transactional file operations API
 *
 * This module provides transactional filesystem operations that are
 * WAL-logged and integrated with PostgreSQL's transaction management.
 * File operations are deferred until transaction commit/abort, ensuring
 * atomicity with the rest of the transaction.
 *
 * The RM_FILEOPS_ID resource manager handles WAL replay for these
 * operations, ensuring correct behavior during crash recovery and
 * standby replay.
 *
 * The operation set follows the Berkeley DB fileops.src model: each
 * filesystem operation is a composable unit with its own WAL record
 * type, redo handler, and descriptor.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/fileops.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FILEOPS_H
#define FILEOPS_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"

/*
 * WAL record types for FILEOPS operations.
 *
 * The high 4 bits of the info byte are used for record type,
 * leaving the low bits for flags (following PostgreSQL convention).
 *
 * Following the Berkeley DB fileops.src model, each filesystem
 * operation has its own WAL record type for independent redo.
 */
#define XLOG_FILEOPS_CREATE			0x00
#define XLOG_FILEOPS_DELETE			0x10
#define XLOG_FILEOPS_RENAME			0x20
#define XLOG_FILEOPS_WRITE			0x30
#define XLOG_FILEOPS_TRUNCATE		0x40
#define XLOG_FILEOPS_CHMOD			0x50
#define XLOG_FILEOPS_CHOWN			0x60
#define XLOG_FILEOPS_MKDIR			0x70
#define XLOG_FILEOPS_RMDIR			0x80
#define XLOG_FILEOPS_SYMLINK		0x90
#define XLOG_FILEOPS_LINK			0xA0
#define XLOG_FILEOPS_SETXATTR		0xB0
#define XLOG_FILEOPS_REMOVEXATTR	0xC0

/*
 * PendingFileOp - Deferred file operation entry
 *
 * File operations are collected in a linked list during a transaction
 * and executed at commit or abort time. This follows the same pattern
 * used by PendingRelDelete in catalog/storage.c.
 */
typedef enum PendingFileOpType
{
	PENDING_FILEOP_CREATE,
	PENDING_FILEOP_DELETE,
	PENDING_FILEOP_RENAME,
	PENDING_FILEOP_WRITE,
	PENDING_FILEOP_TRUNCATE,
	PENDING_FILEOP_CHMOD,
	PENDING_FILEOP_CHOWN,
	PENDING_FILEOP_MKDIR,
	PENDING_FILEOP_RMDIR,
	PENDING_FILEOP_SYMLINK,
	PENDING_FILEOP_LINK,
	PENDING_FILEOP_SETXATTR,
	PENDING_FILEOP_REMOVEXATTR
}			PendingFileOpType;

typedef struct PendingFileOp
{
	PendingFileOpType type;		/* operation type */
	char	   *path;			/* primary file path */
	char	   *newpath;		/* new path (RENAME/SYMLINK/LINK, else NULL) */
	off_t		length;			/* truncation length or write offset */
	bool		at_commit;		/* execute at commit (true) or abort (false) */
	int			nestLevel;		/* transaction nesting level */
	struct PendingFileOp *next; /* linked list link */
}			PendingFileOp;

/* GUC variable */
extern bool enable_transactional_fileops;

/*
 * Public API for transactional file operations
 *
 * These functions handle platform-specific differences automatically:
 *   - O_DIRECT: PG_O_DIRECT (Linux/FreeBSD native, macOS F_NOCACHE,
 *     Windows FILE_FLAG_NO_BUFFERING)
 *   - fsync: pg_fsync() (Linux fdatasync, macOS F_FULLFSYNC,
 *     BSD fsync, Windows FlushFileBuffers)
 *   - Directory sync: fsync_parent_path() (Unix only, no-op on Windows)
 *   - Durable ops: durable_rename()/durable_unlink() with proper
 *     fsync ordering for crash safety
 *
 * Operation-specific API functions are declared below their WAL
 * record structures in subsequent sections.
 */

/* Utility functions */
extern void FileOpsCancelPendingDelete(const char *path, bool at_commit);
extern void FileOpsSync(const char *path);

/* Transaction lifecycle hooks */
extern void FileOpsDoPendingOps(bool isCommit);
extern void AtSubCommit_FileOps(void);
extern void AtSubAbort_FileOps(void);
extern void PostPrepare_FileOps(void);

/*
 * xl_fileops_create - WAL record for file creation
 *
 * Records that a file was created within a transaction. If the transaction
 * aborts, the file will be deleted. The path is stored as variable-length
 * data following the fixed header.
 */
typedef struct xl_fileops_create
{
	int			flags;			/* open flags used for creation */
	mode_t		mode;			/* file permission mode */
	bool		register_delete;	/* register for delete-on-abort */
	/* variable-length path follows */
}			xl_fileops_create;

#define SizeOfFileOpsCreate (offsetof(xl_fileops_create, register_delete) + sizeof(bool))

/* File creation API */
extern int	FileOpsCreate(const char *path, int flags, mode_t mode,
						  bool register_delete);

/*
 * xl_fileops_delete - WAL record for file deletion
 *
 * Records that a file deletion was requested. The at_commit flag indicates
 * whether the deletion should happen at commit (true) or was registered
 * as a delete-on-abort from a prior create (false).
 */
typedef struct xl_fileops_delete
{
	bool		at_commit;		/* true = delete at commit, false = at abort */
	/* variable-length path follows */
}			xl_fileops_delete;

#define SizeOfFileOpsDelete (offsetof(xl_fileops_delete, at_commit) + sizeof(bool))

/* File deletion API */
extern void FileOpsDelete(const char *path, bool at_commit);

/*
 * xl_fileops_rename - WAL record for file rename
 *
 * Records that a file was renamed. Both old and new paths are stored
 * as variable-length data: oldpath_len bytes of old path, then the
 * new path follows.
 */
typedef struct xl_fileops_rename
{
	uint16		oldpath_len;	/* length of old path (including NUL) */
	/* variable-length old path follows, then new path */
}			xl_fileops_rename;

#define SizeOfFileOpsRename (offsetof(xl_fileops_rename, oldpath_len) + sizeof(uint16))

/* File rename API */
extern int	FileOpsRename(const char *oldpath, const char *newpath);

/*
 * xl_fileops_write - WAL record for file write at offset
 *
 * Records that data was written to a file at a specific offset.
 * The path and data are stored as variable-length data following
 * the fixed header.
 */
typedef struct xl_fileops_write
{
	off_t		offset;			/* write offset in file */
	uint32		len;			/* data length */
	uint16		path_len;		/* length of path (including NUL) */
	/* variable-length path follows, then data */
}			xl_fileops_write;

#define SizeOfFileOpsWrite (offsetof(xl_fileops_write, path_len) + sizeof(uint16))

/* File write API */
extern int	FileOpsWrite(const char *path, off_t offset,
						 const void *data, uint32 len);

/*
 * xl_fileops_truncate - WAL record for file truncation
 */
typedef struct xl_fileops_truncate
{
	off_t		length;			/* new file length */
	/* variable-length path follows */
}			xl_fileops_truncate;

#define SizeOfFileOpsTruncate (offsetof(xl_fileops_truncate, length) + sizeof(off_t))

/* File truncation API */
extern void FileOpsTruncate(const char *path, off_t length);

/*
 * xl_fileops_chmod - WAL record for file permission change
 */
typedef struct xl_fileops_chmod
{
	mode_t		mode;			/* new permission mode */
	/* variable-length path follows */
}			xl_fileops_chmod;

#define SizeOfFileOpsChmod (offsetof(xl_fileops_chmod, mode) + sizeof(mode_t))

/*
 * xl_fileops_chown - WAL record for file ownership change
 */
typedef struct xl_fileops_chown
{
	uid_t		uid;			/* new owner user id */
	gid_t		gid;			/* new owner group id */
	/* variable-length path follows */
}			xl_fileops_chown;

#define SizeOfFileOpsChown (offsetof(xl_fileops_chown, gid) + sizeof(gid_t))

/* File metadata API */
extern int	FileOpsChmod(const char *path, mode_t mode);
extern int	FileOpsChown(const char *path, uid_t uid, gid_t gid);

/*
 * xl_fileops_mkdir - WAL record for directory creation
 */
typedef struct xl_fileops_mkdir
{
	mode_t		mode;			/* directory permission mode */
	/* variable-length path follows */
}			xl_fileops_mkdir;

#define SizeOfFileOpsMkdir (offsetof(xl_fileops_mkdir, mode) + sizeof(mode_t))

/*
 * xl_fileops_rmdir - WAL record for directory removal
 */
typedef struct xl_fileops_rmdir
{
	bool		at_commit;		/* true = rmdir at commit, false = at abort */
	/* variable-length path follows */
}			xl_fileops_rmdir;

#define SizeOfFileOpsRmdir (offsetof(xl_fileops_rmdir, at_commit) + sizeof(bool))

/* Directory lifecycle API */
extern int	FileOpsMkdir(const char *path, mode_t mode);
extern void FileOpsRmdir(const char *path, bool at_commit);

/*
 * xl_fileops_symlink - WAL record for symbolic link creation
 */
typedef struct xl_fileops_symlink
{
	uint16		target_len;		/* length of target (including NUL) */
	/* variable-length target follows, then linkpath */
}			xl_fileops_symlink;

#define SizeOfFileOpsSymlink (offsetof(xl_fileops_symlink, target_len) + sizeof(uint16))

/*
 * xl_fileops_link - WAL record for hard link creation
 */
typedef struct xl_fileops_link
{
	uint16		oldpath_len;	/* length of old path (including NUL) */
	/* variable-length old path follows, then new path */
}			xl_fileops_link;

#define SizeOfFileOpsLink (offsetof(xl_fileops_link, oldpath_len) + sizeof(uint16))

/* Link operations API */
extern int	FileOpsSymlink(const char *target, const char *linkpath);
extern int	FileOpsLink(const char *oldpath, const char *newpath);

/* WAL redo and descriptor functions */
extern void fileops_redo(XLogReaderState *record);
extern void fileops_desc(StringInfo buf, XLogReaderState *record);
extern const char *fileops_identify(uint8 info);

/* FILEOPS UNDO RM (fileops_undo.c) */
extern void FileopsUndoRmgrInit(void);

/* FILEOPS UNDO subtypes (stored in urec_info) */
#define FILEOPS_UNDO_CREATE			0x0001
#define FILEOPS_UNDO_RENAME			0x0002
#define FILEOPS_UNDO_TRUNCATE		0x0003
#define FILEOPS_UNDO_CHMOD			0x0004
#define FILEOPS_UNDO_CHOWN			0x0005
#define FILEOPS_UNDO_MKDIR			0x0006
#define FILEOPS_UNDO_SYMLINK		0x0007
#define FILEOPS_UNDO_LINK			0x0008
#define FILEOPS_UNDO_SETXATTR		0x0009
#define FILEOPS_UNDO_REMOVEXATTR	0x000A

#endif							/* FILEOPS_H */
