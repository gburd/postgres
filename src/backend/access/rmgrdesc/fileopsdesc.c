/*-------------------------------------------------------------------------
 *
 * fileopsdesc.c
 *	  rmgr descriptor routines for storage/file/fileops.c
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/fileopsdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/fileops.h"

void
fileops_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *data = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_FILEOPS_CREATE:
			{
				xl_fileops_create *xlrec = (xl_fileops_create *) data;
				const char *path = data + SizeOfFileOpsCreate;

				appendStringInfo(buf, "create \"%s\" flags 0x%x mode 0%o",
								 path, xlrec->flags, xlrec->mode);
			}
			break;

		case XLOG_FILEOPS_WRITE:
			{
				xl_fileops_write *xlrec = (xl_fileops_write *) data;
				const char *path = data + SizeOfFileOpsWrite;

				appendStringInfo(buf, "write \"%s\" offset %lld len %u",
								 path, (long long) xlrec->offset, xlrec->len);
			}
			break;

		case XLOG_FILEOPS_RENAME:
			{
				xl_fileops_rename *xlrec = (xl_fileops_rename *) data;
				const char *oldpath = data + SizeOfFileOpsRename;
				const char *newpath = oldpath + xlrec->oldpath_len;

				appendStringInfo(buf, "rename \"%s\" to \"%s\"",
								 oldpath, newpath);
			}
			break;

		case XLOG_FILEOPS_DELETE:
			{
				xl_fileops_delete *xlrec = (xl_fileops_delete *) data;
				const char *path = data + SizeOfFileOpsDelete;

				appendStringInfo(buf, "delete \"%s\" at_%s",
								 path,
								 xlrec->at_commit ? "commit" : "abort");
			}
			break;

		case XLOG_FILEOPS_TRUNCATE:
			{
				xl_fileops_truncate *xlrec = (xl_fileops_truncate *) data;
				const char *path = data + SizeOfFileOpsTruncate;

				appendStringInfo(buf, "truncate \"%s\" to %lld bytes",
								 path, (long long) xlrec->length);
			}
			break;

		default:
			appendStringInfo(buf, "unknown fileops op code %u", info);
			break;
	}
}

const char *
fileops_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_FILEOPS_CREATE:
			id = "CREATE";
			break;
		case XLOG_FILEOPS_DELETE:
			id = "DELETE";
			break;
		case XLOG_FILEOPS_RENAME:
			id = "RENAME";
			break;
		case XLOG_FILEOPS_WRITE:
			id = "WRITE";
			break;
		case XLOG_FILEOPS_TRUNCATE:
			id = "TRUNCATE";
			break;
	}

	return id;
}
