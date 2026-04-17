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
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
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
		/* Operation-specific cases added in subsequent commits */
	}

	return id;
}
