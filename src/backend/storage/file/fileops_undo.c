/*-------------------------------------------------------------------------
 *
 * fileops_undo.c
 *	  FILEOPS UNDO resource manager
 *
 * This module implements the UNDO apply callbacks for transactional file
 * operations.  UNDO covers structural filesystem operations (file existence,
 * permissions, directory structure).  File content recovery uses traditional
 * WAL (via FileOpsWrite), not FILEOPS UNDO.
 *
 * On transaction abort, each FILEOPS UNDO record reverses the structural
 * change made by the corresponding FileOps* function:
 *   - CREATE:     unlink the created file
 *   - RENAME:     rename back to the original path
 *   - TRUNCATE:   ftruncate to original length
 *   - CHMOD:      chmod to original mode
 *   - CHOWN:      chown to original uid/gid
 *   - MKDIR:      rmdir the created directory
 *   - SYMLINK:    unlink the symlink
 *   - LINK:       unlink the hard link
 *   - SETXATTR:   restore/remove the original xattr
 *   - REMOVEXATTR: restore the original xattr value
 *
 * DELETE and RMDIR are handled by PendingFileOps (deferred execution)
 * and do not need UNDO records since they execute only at commit time.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/file/fileops_undo.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/undormgr.h"
#include "port/pg_xattr.h"
#include "storage/fd.h"
#include "storage/fileops.h"

/*
 * FILEOPS UNDO subtypes (stored in urec_info)
 */
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

/*
 * FileopsUndoPayload - generic payload header
 *
 * Variable-length paths and data follow this fixed header.
 * The layout depends on the subtype.
 */

/* CREATE undo payload: just the path to unlink */
typedef struct FileopsUndoCreate
{
	uint16		path_len;		/* including NUL */
	/* followed by path */
}			FileopsUndoCreate;

/* RENAME undo payload: oldpath and newpath for reverse rename */
typedef struct FileopsUndoRename
{
	uint16		oldpath_len;
	uint16		newpath_len;
	/* followed by oldpath, then newpath */
}			FileopsUndoRename;

/* TRUNCATE undo payload: path + original length */
typedef struct FileopsUndoTruncate
{
	off_t		orig_length;
	uint16		path_len;
	/* followed by path */
}			FileopsUndoTruncate;

/* CHMOD undo payload: path + original mode */
typedef struct FileopsUndoChmod
{
	mode_t		orig_mode;
	uint16		path_len;
	/* followed by path */
}			FileopsUndoChmod;

/* CHOWN undo payload: path + original uid/gid */
typedef struct FileopsUndoChown
{
	uid_t		orig_uid;
	gid_t		orig_gid;
	uint16		path_len;
	/* followed by path */
}			FileopsUndoChown;

/* MKDIR undo payload: just the path to rmdir */
typedef struct FileopsUndoMkdir
{
	uint16		path_len;
	/* followed by path */
}			FileopsUndoMkdir;

/* SYMLINK undo payload: just the linkpath to unlink */
typedef struct FileopsUndoSymlink
{
	uint16		linkpath_len;
	/* followed by linkpath */
}			FileopsUndoSymlink;

/* LINK undo payload: just the newpath to unlink */
typedef struct FileopsUndoLink
{
	uint16		newpath_len;
	/* followed by newpath */
}			FileopsUndoLink;

/* SETXATTR undo payload: path + name + original value (or empty if new) */
typedef struct FileopsUndoSetxattr
{
	uint16		path_len;
	uint16		name_len;
	uint32		orig_value_len; /* 0 if xattr didn't exist before */
	bool		had_value;		/* true if xattr existed before setxattr */
	/* followed by path, name, original value (if had_value) */
}			FileopsUndoSetxattr;

/* REMOVEXATTR undo payload: path + name + removed value */
typedef struct FileopsUndoRemovexattr
{
	uint16		path_len;
	uint16		name_len;
	uint32		value_len;
	/* followed by path, name, value */
}			FileopsUndoRemovexattr;

/* Forward declarations */
static UndoApplyResult fileops_undo_apply(uint8 rmid, uint16 info,
										  TransactionId xid, Oid reloid,
										  const char *payload, Size payload_len,
										  UndoRecPtr urec_ptr);
static void fileops_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
							  const char *payload, Size payload_len);

/* The FILEOPS UNDO RM registration entry */
static const UndoRmgrData fileops_undo_rmgr = {
	.rm_name = "fileops",
	.rm_undo = fileops_undo_apply,
	.rm_desc = fileops_undo_desc,
};

/*
 * FileopsUndoRmgrInit - Register the FILEOPS UNDO resource manager
 */
void
FileopsUndoRmgrInit(void)
{
	RegisterUndoRmgr(UNDO_RMID_FILEOPS, &fileops_undo_rmgr);
}

/*
 * fileops_undo_apply - Apply a single FILEOPS UNDO record
 *
 * Reverses the structural filesystem operation.
 */
static UndoApplyResult
fileops_undo_apply(uint8 rmid, uint16 info, TransactionId xid, Oid reloid,
				   const char *payload, Size payload_len, UndoRecPtr urec_ptr)
{
	Assert(rmid == UNDO_RMID_FILEOPS);

	switch (info)
	{
		case FILEOPS_UNDO_CREATE:
			{
				FileopsUndoCreate hdr;
				const char *path;

				if (payload_len < sizeof(FileopsUndoCreate))
					return UNDO_APPLY_ERROR;
				memcpy(&hdr, payload, sizeof(FileopsUndoCreate));
				path = payload + sizeof(FileopsUndoCreate);

				if (unlink(path) < 0 && errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("FILEOPS UNDO CREATE: could not unlink \"%s\": %m",
									path)));
				else
					ereport(DEBUG2,
							(errmsg("FILEOPS UNDO CREATE: unlinked \"%s\"",
									path)));
			}
			break;

		case FILEOPS_UNDO_RENAME:
			{
				FileopsUndoRename hdr;
				const char *oldpath;
				const char *newpath;

				if (payload_len < sizeof(FileopsUndoRename))
					return UNDO_APPLY_ERROR;
				memcpy(&hdr, payload, sizeof(FileopsUndoRename));
				oldpath = payload + sizeof(FileopsUndoRename);
				newpath = oldpath + hdr.oldpath_len;

				/* Reverse the rename: newpath -> oldpath */
				if (rename(newpath, oldpath) < 0)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("FILEOPS UNDO RENAME: could not rename \"%s\" back to \"%s\": %m",
									newpath, oldpath)));
				else
					ereport(DEBUG2,
							(errmsg("FILEOPS UNDO RENAME: renamed \"%s\" back to \"%s\"",
									newpath, oldpath)));
			}
			break;

		case FILEOPS_UNDO_TRUNCATE:
			{
				FileopsUndoTruncate hdr;
				const char *path;
				int			fd;

				if (payload_len < sizeof(FileopsUndoTruncate))
					return UNDO_APPLY_ERROR;
				memcpy(&hdr, payload, sizeof(FileopsUndoTruncate));
				path = payload + sizeof(FileopsUndoTruncate);

				fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
				if (fd < 0)
				{
					if (errno != ENOENT)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("FILEOPS UNDO TRUNCATE: could not open \"%s\": %m",
										path)));
				}
				else
				{
					if (ftruncate(fd, hdr.orig_length) < 0)
						ereport(WARNING,
								(errcode_for_file_access(),
								 errmsg("FILEOPS UNDO TRUNCATE: could not restore length of \"%s\": %m",
										path)));
					close(fd);
				}
			}
			break;

		case FILEOPS_UNDO_CHMOD:
			{
				FileopsUndoChmod hdr;
				const char *path;

				if (payload_len < sizeof(FileopsUndoChmod))
					return UNDO_APPLY_ERROR;
				memcpy(&hdr, payload, sizeof(FileopsUndoChmod));
				path = payload + sizeof(FileopsUndoChmod);

				if (chmod(path, hdr.orig_mode) < 0 && errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("FILEOPS UNDO CHMOD: could not restore mode of \"%s\": %m",
									path)));
			}
			break;

		case FILEOPS_UNDO_CHOWN:
			{
#ifndef WIN32
				FileopsUndoChown hdr;
				const char *path;

				if (payload_len < sizeof(FileopsUndoChown))
					return UNDO_APPLY_ERROR;
				memcpy(&hdr, payload, sizeof(FileopsUndoChown));
				path = payload + sizeof(FileopsUndoChown);

				if (chown(path, hdr.orig_uid, hdr.orig_gid) < 0 &&
					errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("FILEOPS UNDO CHOWN: could not restore ownership of \"%s\": %m",
									path)));
#endif
			}
			break;

		case FILEOPS_UNDO_MKDIR:
			{
				FileopsUndoMkdir hdr;
				const char *path;

				if (payload_len < sizeof(FileopsUndoMkdir))
					return UNDO_APPLY_ERROR;
				memcpy(&hdr, payload, sizeof(FileopsUndoMkdir));
				path = payload + sizeof(FileopsUndoMkdir);

				if (rmdir(path) < 0 && errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("FILEOPS UNDO MKDIR: could not rmdir \"%s\": %m",
									path)));
			}
			break;

		case FILEOPS_UNDO_SYMLINK:
			{
				FileopsUndoSymlink hdr;
				const char *linkpath;

				if (payload_len < sizeof(FileopsUndoSymlink))
					return UNDO_APPLY_ERROR;
				memcpy(&hdr, payload, sizeof(FileopsUndoSymlink));
				linkpath = payload + sizeof(FileopsUndoSymlink);

				if (unlink(linkpath) < 0 && errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("FILEOPS UNDO SYMLINK: could not unlink \"%s\": %m",
									linkpath)));
			}
			break;

		case FILEOPS_UNDO_LINK:
			{
				FileopsUndoLink hdr;
				const char *newpath;

				if (payload_len < sizeof(FileopsUndoLink))
					return UNDO_APPLY_ERROR;
				memcpy(&hdr, payload, sizeof(FileopsUndoLink));
				newpath = payload + sizeof(FileopsUndoLink);

				if (unlink(newpath) < 0 && errno != ENOENT)
					ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("FILEOPS UNDO LINK: could not unlink \"%s\": %m",
									newpath)));
			}
			break;

		case FILEOPS_UNDO_SETXATTR:
			{
				FileopsUndoSetxattr hdr;
				const char *path;
				const char *name;

				if (payload_len < sizeof(FileopsUndoSetxattr))
					return UNDO_APPLY_ERROR;
				memcpy(&hdr, payload, sizeof(FileopsUndoSetxattr));
				path = payload + sizeof(FileopsUndoSetxattr);
				name = path + hdr.path_len;

				if (hdr.had_value)
				{
					/* Restore original value */
					const void *orig_value = name + hdr.name_len;

					pg_setxattr(path, name, orig_value, hdr.orig_value_len);
				}
				else
				{
					/* xattr didn't exist before, remove it */
					pg_removexattr(path, name);
				}
			}
			break;

		case FILEOPS_UNDO_REMOVEXATTR:
			{
				FileopsUndoRemovexattr hdr;
				const char *path;
				const char *name;
				const void *value;

				if (payload_len < sizeof(FileopsUndoRemovexattr))
					return UNDO_APPLY_ERROR;
				memcpy(&hdr, payload, sizeof(FileopsUndoRemovexattr));
				path = payload + sizeof(FileopsUndoRemovexattr);
				name = path + hdr.path_len;
				value = name + hdr.name_len;

				/* Restore the removed xattr */
				pg_setxattr(path, name, value, hdr.value_len);
			}
			break;

		default:
			ereport(WARNING,
					(errmsg("FILEOPS UNDO: unknown subtype %u", info)));
			return UNDO_APPLY_ERROR;
	}

	return UNDO_APPLY_SUCCESS;
}

/*
 * fileops_undo_desc - Describe a FILEOPS UNDO record for debugging
 */
static void
fileops_undo_desc(StringInfo buf, uint8 rmid, uint16 info,
				  const char *payload, Size payload_len)
{
	const char *opname;

	switch (info)
	{
		case FILEOPS_UNDO_CREATE:
			opname = "CREATE";
			break;
		case FILEOPS_UNDO_RENAME:
			opname = "RENAME";
			break;
		case FILEOPS_UNDO_TRUNCATE:
			opname = "TRUNCATE";
			break;
		case FILEOPS_UNDO_CHMOD:
			opname = "CHMOD";
			break;
		case FILEOPS_UNDO_CHOWN:
			opname = "CHOWN";
			break;
		case FILEOPS_UNDO_MKDIR:
			opname = "MKDIR";
			break;
		case FILEOPS_UNDO_SYMLINK:
			opname = "SYMLINK";
			break;
		case FILEOPS_UNDO_LINK:
			opname = "LINK";
			break;
		case FILEOPS_UNDO_SETXATTR:
			opname = "SETXATTR";
			break;
		case FILEOPS_UNDO_REMOVEXATTR:
			opname = "REMOVEXATTR";
			break;
		default:
			opname = "UNKNOWN";
			break;
	}

	appendStringInfo(buf, "fileops %s", opname);
}
