/*-------------------------------------------------------------------------
 *
 * relundodesc.c
 *	  rmgr descriptor routines for access/undo/relundo_xlog.c
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/relundodesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relundo_xlog.h"

/*
 * relundo_desc - Describe a per-relation UNDO WAL record for pg_waldump
 */
void
relundo_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *data = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info & ~XLOG_RELUNDO_INIT_PAGE)
	{
		case XLOG_RELUNDO_INIT:
			{
				xl_relundo_init *xlrec = (xl_relundo_init *) data;

				appendStringInfo(buf, "magic 0x%08X, version %u, counter %u",
								 xlrec->magic, xlrec->version,
								 xlrec->counter);
			}
			break;

		case XLOG_RELUNDO_INSERT:
			{
				xl_relundo_insert *xlrec = (xl_relundo_insert *) data;
				const char *type_name;

				switch (xlrec->urec_type)
				{
					case 1:
						type_name = "INSERT";
						break;
					case 2:
						type_name = "DELETE";
						break;
					case 3:
						type_name = "UPDATE";
						break;
					case 4:
						type_name = "TUPLE_LOCK";
						break;
					default:
						type_name = "UNKNOWN";
						break;
				}

				appendStringInfo(buf,
								 "type %s, len %u, offset %u, new_pd_lower %u, max_xid %u",
								 type_name, xlrec->urec_len,
								 xlrec->page_offset,
								 xlrec->new_pd_lower,
								 xlrec->max_xid);

				if (info & XLOG_RELUNDO_INIT_PAGE)
					appendStringInfoString(buf, " (init page)");
			}
			break;

		case XLOG_RELUNDO_DISCARD:
			{
				xl_relundo_discard *xlrec = (xl_relundo_discard *) data;

				appendStringInfo(buf,
								 "slot %u, old_tail %u, new_tail %u, discard_xid %u, "
								 "npages_freed %u",
								 xlrec->slot,
								 xlrec->old_tail_blkno,
								 xlrec->new_tail_blkno,
								 xlrec->discard_xid,
								 xlrec->npages_freed);
			}
			break;

		case XLOG_RELUNDO_APPLY:
			{
				xl_relundo_apply *xlrec = (xl_relundo_apply *) data;

				appendStringInfo(buf, "urec_ptr %lu",
								 (unsigned long) xlrec->urec_ptr);
			}
			break;

		case XLOG_RELUNDO_TRUNCATE:
			{
				xl_relundo_truncate *xlrec = (xl_relundo_truncate *) data;

				appendStringInfo(buf, "new_nblocks %u", xlrec->new_nblocks);
			}
			break;
	}
}

/*
 * relundo_identify - Identify a per-relation UNDO WAL record type
 */
const char *
relundo_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_RELUNDO_INIT:
			id = "INIT";
			break;
		case XLOG_RELUNDO_INSERT:
			id = "INSERT";
			break;
		case XLOG_RELUNDO_INSERT | XLOG_RELUNDO_INIT_PAGE:
			id = "INSERT+INIT";
			break;
		case XLOG_RELUNDO_DISCARD:
			id = "DISCARD";
			break;
		case XLOG_RELUNDO_TRUNCATE:
			id = "TRUNCATE";
			break;
		case XLOG_RELUNDO_APPLY:
			id = "APPLY";
			break;
	}

	return id;
}
