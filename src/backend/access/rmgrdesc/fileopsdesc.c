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
	}

	return id;
}
