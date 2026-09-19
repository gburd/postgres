/*-------------------------------------------------------------------------
 *
 * recnodesc.c
 *	  Resource manager descriptor for RECNO.
 *
 * Provides desc/identify for pg_waldump and the WAL rmgr table.  The redo
 * and mask routines live in recno_xlog.c (backend only).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Author: Greg Burd <greg@burd.me>
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/recnodesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"

extern void recno_desc(StringInfo buf, XLogReaderState *record);
extern const char *recno_identify(uint8 info);

/* RECNO WAL record types - keep in sync with recno_xlog.h */
#define XLOG_RECNO_INSERT				0x10
#define XLOG_RECNO_UPDATE_INPLACE		0x20
#define XLOG_RECNO_DELETE				0x30
#define XLOG_RECNO_INIT_PAGE			0x40
#define XLOG_RECNO_DEFRAG				0x50
#define XLOG_RECNO_OVERFLOW_WRITE		0x60
#define XLOG_RECNO_COMPRESS				0x70
#define XLOG_RECNO_LOCK					0x80
#define XLOG_RECNO_VM_SET				0x90
#define XLOG_RECNO_VM_CLEAR				0xA0
#define XLOG_RECNO_CROSS_PAGE_DEFRAG	0xB0
#define XLOG_RECNO_OPMASK				0xF0

void
recno_desc(StringInfo buf, XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & XLOG_RECNO_OPMASK)
	{
		case XLOG_RECNO_INSERT:
			appendStringInfoString(buf, "INSERT");
			break;
		case XLOG_RECNO_UPDATE_INPLACE:
			appendStringInfoString(buf, "UPDATE_INPLACE");
			break;
		case XLOG_RECNO_DELETE:
			appendStringInfoString(buf, "DELETE");
			break;
		case XLOG_RECNO_INIT_PAGE:
			appendStringInfoString(buf, "INIT_PAGE");
			break;
		case XLOG_RECNO_DEFRAG:
			appendStringInfoString(buf, "DEFRAG");
			break;
		case XLOG_RECNO_OVERFLOW_WRITE:
			appendStringInfoString(buf, "OVERFLOW_WRITE");
			break;
		case XLOG_RECNO_COMPRESS:
			appendStringInfoString(buf, "COMPRESS");
			break;
		case XLOG_RECNO_LOCK:
			appendStringInfoString(buf, "LOCK");
			break;
		case XLOG_RECNO_VM_SET:
			appendStringInfoString(buf, "VM_SET");
			break;
		case XLOG_RECNO_VM_CLEAR:
			appendStringInfoString(buf, "VM_CLEAR");
			break;
		case XLOG_RECNO_CROSS_PAGE_DEFRAG:
			appendStringInfoString(buf, "CROSS_PAGE_DEFRAG");
			break;
		default:
			appendStringInfoString(buf, "UNKNOWN");
			break;
	}
}

const char *
recno_identify(uint8 info)
{
	switch (info & XLOG_RECNO_OPMASK)
	{
		case XLOG_RECNO_INSERT:
			return "INSERT";
		case XLOG_RECNO_UPDATE_INPLACE:
			return "UPDATE_INPLACE";
		case XLOG_RECNO_DELETE:
			return "DELETE";
		case XLOG_RECNO_INIT_PAGE:
			return "INIT_PAGE";
		case XLOG_RECNO_DEFRAG:
			return "DEFRAG";
		case XLOG_RECNO_OVERFLOW_WRITE:
			return "OVERFLOW_WRITE";
		case XLOG_RECNO_COMPRESS:
			return "COMPRESS";
		case XLOG_RECNO_LOCK:
			return "LOCK";
		case XLOG_RECNO_VM_SET:
			return "VM_SET";
		case XLOG_RECNO_VM_CLEAR:
			return "VM_CLEAR";
		case XLOG_RECNO_CROSS_PAGE_DEFRAG:
			return "CROSS_PAGE_DEFRAG";
		default:
			return NULL;
	}
}
