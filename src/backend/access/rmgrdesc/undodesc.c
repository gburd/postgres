/*-------------------------------------------------------------------------
 *
 * undodesc.c
 *	  rmgr descriptor routines for access/undo/undolog.c
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/undodesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/undo_xlog.h"
#include "access/xlogreader.h"

/*
 * undo_desc - Describe an UNDO WAL record for pg_waldump
 *
 * This function generates human-readable output for UNDO WAL records,
 * used by pg_waldump and other debugging tools.
 */
void
undo_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_UNDO_ALLOCATE:
			{
				xl_undo_allocate *xlrec = (xl_undo_allocate *) rec;

				appendStringInfo(buf, "log %u, start %llu, len %u, xid %u",
								 xlrec->log_number,
								 (unsigned long long) xlrec->start_ptr,
								 xlrec->length,
								 xlrec->xid);
			}
			break;

		case XLOG_UNDO_DISCARD:
			{
				xl_undo_discard *xlrec = (xl_undo_discard *) rec;

				appendStringInfo(buf, "log %u, discard_ptr %llu, oldest_xid %u",
								 xlrec->log_number,
								 (unsigned long long) xlrec->discard_ptr,
								 xlrec->oldest_xid);
			}
			break;

		case XLOG_UNDO_EXTEND:
			{
				xl_undo_extend *xlrec = (xl_undo_extend *) rec;

				appendStringInfo(buf, "log %u, new_size %llu",
								 xlrec->log_number,
								 (unsigned long long) xlrec->new_size);
			}
			break;
	}
}

/*
 * undo_identify - Identify an UNDO WAL record type
 *
 * Returns a string identifying the operation type for debugging output.
 */
const char *
undo_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_UNDO_ALLOCATE:
			id = "ALLOCATE";
			break;
		case XLOG_UNDO_DISCARD:
			id = "DISCARD";
			break;
		case XLOG_UNDO_EXTEND:
			id = "EXTEND";
			break;
	}

	return id;
}
