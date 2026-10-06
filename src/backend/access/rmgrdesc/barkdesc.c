/*-------------------------------------------------------------------------
 *
 * barkdesc.c
 *	  rmgr descriptor routines for access/bark/barkxlog.c
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/barkdesc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/barkxlog.h"
#include "access/rmgrdesc_utils.h"

void
bark_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case XLOG_BARK_VACUUM:
			{
				xl_bark_vacuum *xlrec = (xl_bark_vacuum *) rec;

				appendStringInfo(buf, "ndeleted: %u, nupdated: %u",
								 xlrec->ndeleted, xlrec->nupdated);

				if (XLogRecHasBlockData(record, 0))
				{
					OffsetNumber *offsets = (OffsetNumber *)
						XLogRecGetBlockData(record, 0, NULL);

					appendStringInfoString(buf, ", deleted:");
					array_desc(buf, offsets, sizeof(OffsetNumber),
							   xlrec->ndeleted, &offset_elem_desc, NULL);
					appendStringInfoString(buf, ", updated:");
					array_desc(buf, offsets + xlrec->ndeleted,
							   sizeof(OffsetNumber), xlrec->nupdated,
							   &offset_elem_desc, NULL);
				}
				break;
			}
		case XLOG_BARK_UNLINK_PAGE:
			{
				xl_bark_unlink_page *xlrec = (xl_bark_unlink_page *) rec;

				appendStringInfo(buf, "left: %u, right: %u, safexid: %u:%u, poffset: %u",
								 xlrec->leftsib, xlrec->rightsib,
								 EpochFromFullTransactionId(xlrec->safexid),
								 XidFromFullTransactionId(xlrec->safexid),
								 xlrec->poffset);
				break;
			}
		case XLOG_BARK_REUSE_PAGE:
			{
				xl_bark_reuse_page *xlrec = (xl_bark_reuse_page *) rec;

				appendStringInfo(buf, "rel: %u/%u/%u, blk: %u, snapshotConflictHorizon: %u:%u, isCatalogRel: %c",
								 xlrec->locator.spcOid, xlrec->locator.dbOid,
								 xlrec->locator.relNumber, xlrec->block,
								 EpochFromFullTransactionId(xlrec->snapshotConflictHorizon),
								 XidFromFullTransactionId(xlrec->snapshotConflictHorizon),
								 xlrec->isCatalogRel ? 'T' : 'F');
				break;
			}
		case XLOG_BARK_MARK_DELETED:
			{
				xl_bark_mark_deleted *xlrec = (xl_bark_mark_deleted *) rec;

				appendStringInfo(buf, "next: %u, safexid: %u:%u",
								 xlrec->next,
								 EpochFromFullTransactionId(xlrec->safexid),
								 XidFromFullTransactionId(xlrec->safexid));
				break;
			}
		case XLOG_BARK_INSERT_LEAF:
		case XLOG_BARK_INSERT_UPPER:
			{
				xl_bark_insert *xlrec = (xl_bark_insert *) rec;

				appendStringInfo(buf, "off: %u", xlrec->offnum);
				break;
			}
		case XLOG_BARK_OVERWRITE:
			{
				xl_bark_overwrite *xlrec = (xl_bark_overwrite *) rec;

				appendStringInfo(buf, "off: %u", xlrec->offnum);
				break;
			}
	}
}

const char *
bark_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_BARK_VACUUM:
			id = "VACUUM";
			break;
		case XLOG_BARK_UNLINK_PAGE:
			id = "UNLINK_PAGE";
			break;
		case XLOG_BARK_REUSE_PAGE:
			id = "REUSE_PAGE";
			break;
		case XLOG_BARK_MARK_DELETED:
			id = "MARK_DELETED";
			break;
		case XLOG_BARK_INSERT_LEAF:
			id = "INSERT_LEAF";
			break;
		case XLOG_BARK_INSERT_UPPER:
			id = "INSERT_UPPER";
			break;
		case XLOG_BARK_OVERWRITE:
			id = "OVERWRITE";
			break;
	}

	return id;
}
