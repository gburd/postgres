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

		case XLOG_FILEOPS_DELETE:
			{
				xl_fileops_delete *xlrec = (xl_fileops_delete *) data;
				const char *path = data + SizeOfFileOpsDelete;

				appendStringInfo(buf, "delete \"%s\" at_%s",
								 path,
								 xlrec->at_commit ? "commit" : "abort");
			}
			break;

		case XLOG_FILEOPS_MOVE:
			{
				xl_fileops_move *xlrec = (xl_fileops_move *) data;
				const char *oldpath = data + SizeOfFileOpsMove;
				const char *newpath = oldpath + xlrec->oldpath_len;

				appendStringInfo(buf, "move \"%s\" to \"%s\"",
								 oldpath, newpath);
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
		case XLOG_FILEOPS_MOVE:
			id = "MOVE";
			break;
		case XLOG_FILEOPS_TRUNCATE:
			id = "TRUNCATE";
			break;
	}

	return id;
}
