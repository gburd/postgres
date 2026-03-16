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
 */
#define XLOG_FILEOPS_CREATE		0x00
#define XLOG_FILEOPS_DELETE		0x10
#define XLOG_FILEOPS_MOVE		0x20
#define XLOG_FILEOPS_TRUNCATE	0x30

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
	bool		register_delete; /* register for delete-on-abort */
	/* variable-length path follows */
} xl_fileops_create;

#define SizeOfFileOpsCreate (offsetof(xl_fileops_create, register_delete) + sizeof(bool))

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
} xl_fileops_delete;

#define SizeOfFileOpsDelete (offsetof(xl_fileops_delete, at_commit) + sizeof(bool))

/*
 * xl_fileops_move - WAL record for file rename/move
 *
 * Records that a file was renamed. Both old and new paths are stored
 * as variable-length data: oldpath_len bytes of old path, then the
 * new path follows.
 */
typedef struct xl_fileops_move
{
	uint16		oldpath_len;	/* length of old path (including NUL) */
	/* variable-length old path follows, then new path */
} xl_fileops_move;

#define SizeOfFileOpsMove (offsetof(xl_fileops_move, oldpath_len) + sizeof(uint16))

/*
 * xl_fileops_truncate - WAL record for file truncation
 *
 * Records that a file was truncated to a given length.
 */
typedef struct xl_fileops_truncate
{
	off_t		length;			/* new file length */
	/* variable-length path follows */
} xl_fileops_truncate;

#define SizeOfFileOpsTruncate (offsetof(xl_fileops_truncate, length) + sizeof(off_t))

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
	PENDING_FILEOP_MOVE,
	PENDING_FILEOP_TRUNCATE
} PendingFileOpType;

typedef struct PendingFileOp
{
	PendingFileOpType type;		/* operation type */
	char	   *path;			/* primary file path */
	char	   *newpath;		/* new path (for MOVE only, else NULL) */
	off_t		length;			/* truncation length (for TRUNCATE only) */
	bool		at_commit;		/* execute at commit (true) or abort (false) */
	int			nestLevel;		/* transaction nesting level */
	struct PendingFileOp *next;	/* linked list link */
} PendingFileOp;

/* Public API for transactional file operations */
extern int	FileOpsCreate(const char *path, int flags, mode_t mode,
						  bool register_delete);
extern void FileOpsDelete(const char *path, bool at_commit);
extern int	FileOpsMove(const char *oldpath, const char *newpath);
extern void FileOpsTruncate(const char *path, off_t length);

/* Transaction lifecycle hooks */
extern void FileOpsDoPendingOps(bool isCommit);
extern void AtSubCommit_FileOps(void);
extern void AtSubAbort_FileOps(void);
extern void PostPrepare_FileOps(void);

/* WAL redo and descriptor functions */
extern void fileops_redo(XLogReaderState *record);
extern void fileops_desc(StringInfo buf, XLogReaderState *record);
extern const char *fileops_identify(uint8 info);

#endif							/* FILEOPS_H */
