/*-------------------------------------------------------------------------
 *
 * undodesc.c
 *	  rmgr descriptor routines for access/undo
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
		case XLOG_UNDO_ROLLBACK:
			{
				xl_undo_rollback *xlrec = (xl_undo_rollback *) rec;

				appendStringInfo(buf, "xid %u, nrecords %u",
								 xlrec->xid, xlrec->nrecords);
			}
			break;

		case XLOG_UNDO_INFLIGHT:
			{
				xl_undo_inflight *xlrec = (xl_undo_inflight *) rec;

				appendStringInfo(buf, "nxacts %u", xlrec->nxacts);
				for (uint32 i = 0; i < xlrec->nxacts; i++)
				{
					xl_undo_inflight_xact *x = &xlrec->xacts[i];

					appendStringInfo(buf, "; xid %u", x->xid);
					for (int j = 0; j < NUndoPersistenceLevels; j++)
						if (XLogRecPtrIsValid(x->last_batch_lsn[j]))
							appendStringInfo(buf, " p%d %X/%08X", j,
											 LSN_FORMAT_ARGS(x->last_batch_lsn[j]));
				}
			}
			break;

		case XLOG_UNDO_BATCH:
			{
				xl_undo_batch *xlrec = (xl_undo_batch *) rec;

				appendStringInfo(buf,
								 "undo batch: xid %u, nrecords %u, "
								 "total_len %u, chain_prev %X/%08X, "
								 "primary_reloid %u, persistence %d",
								 xlrec->xid,
								 xlrec->nrecords,
								 xlrec->total_len,
								 LSN_FORMAT_ARGS(xlrec->chain_prev),
								 xlrec->primary_reloid,
								 xlrec->persistence);
			}
			break;

		default:
			appendStringInfo(buf, "unknown undo op code %u", info);
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
		case XLOG_UNDO_ROLLBACK:
			id = "ROLLBACK";
			break;
		case XLOG_UNDO_BATCH:
			id = "BATCH";
			break;
		case XLOG_UNDO_INFLIGHT:
			id = "INFLIGHT";
			break;
	}

	return id;
}
