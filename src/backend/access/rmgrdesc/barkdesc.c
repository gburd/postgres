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

/*
 * The deleted and updated offsets at the start of the block data of a VACUUM
 * or DELETE record.
 */
static void
delitems_desc(StringInfo buf, XLogReaderState *record, uint16 ndeleted,
			  uint16 nupdated)
{
	OffsetNumber *offsets = (OffsetNumber *) XLogRecGetBlockData(record, 0,
																 NULL);

	appendStringInfoString(buf, ", deleted:");
	array_desc(buf, offsets, sizeof(OffsetNumber), ndeleted,
			   &offset_elem_desc, NULL);
	appendStringInfoString(buf, ", updated:");
	array_desc(buf, offsets + ndeleted, sizeof(OffsetNumber), nupdated,
			   &offset_elem_desc, NULL);
}

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
					delitems_desc(buf, record, xlrec->ndeleted,
								  xlrec->nupdated);
				break;
			}
		case XLOG_BARK_DELETE:
			{
				xl_bark_delete *xlrec = (xl_bark_delete *) rec;

				appendStringInfo(buf, "snapshotConflictHorizon: %u, ndeleted: %u, nupdated: %u, isCatalogRel: %c",
								 xlrec->snapshotConflictHorizon,
								 xlrec->ndeleted, xlrec->nupdated,
								 xlrec->isCatalogRel ? 'T' : 'F');

				if (XLogRecHasBlockData(record, 0))
					delitems_desc(buf, record, xlrec->ndeleted,
								  xlrec->nupdated);
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
		case XLOG_BARK_ADD_TID:
			{
				xl_bark_add_tid *xlrec = (xl_bark_add_tid *) rec;

				appendStringInfo(buf, "off: %u, tid: (%u,%u)", xlrec->offnum,
								 ItemPointerGetBlockNumber(&xlrec->tid),
								 ItemPointerGetOffsetNumber(&xlrec->tid));
				break;
			}
		case XLOG_BARK_SPLIT:
			{
				xl_bark_split *xlrec = (xl_bark_split *) rec;
				BlockNumber left = InvalidBlockNumber;
				BlockNumber right = InvalidBlockNumber;

				XLogRecGetBlockTagExtended(record, 0, NULL, NULL, &left, NULL);
				XLogRecGetBlockTagExtended(record, 1, NULL, NULL, &right, NULL);
				appendStringInfo(buf, "level: %u, leaf: %c, left: %u, right: %u, cycleid: %u, prefix: %c%c",
								 xlrec->level,
								 (xlrec->flags & XLH_BARK_SPLIT_LEAF) ? 'T' : 'F',
								 left, right, xlrec->cycleid,
								 (xlrec->flags & XLH_BARK_SPLIT_LPREFIX) ? 'L' : '-',
								 (xlrec->flags & XLH_BARK_SPLIT_RPREFIX) ? 'R' : '-');
				break;
			}
		case XLOG_BARK_NEWROOT:
			{
				xl_bark_newroot *xlrec = (xl_bark_newroot *) rec;

				appendStringInfo(buf, "root: %u, level: %u",
								 xlrec->rootblk, xlrec->level);
				break;
			}
		case XLOG_BARK_CREATE_ROOT:
			{
				BlockNumber root = InvalidBlockNumber;

				XLogRecGetBlockTagExtended(record, 0, NULL, NULL, &root, NULL);
				appendStringInfo(buf, "root: %u", root);
				break;
			}
		case XLOG_BARK_OVERFLOW:
			appendStringInfo(buf, "npages: %d", XLogRecMaxBlockId(record) + 1);
			break;
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
		case XLOG_BARK_DELETE:
			id = "DELETE";
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
		case XLOG_BARK_ADD_TID:
			id = "ADD_TID";
			break;
		case XLOG_BARK_SPLIT:
			id = "SPLIT";
			break;
		case XLOG_BARK_NEWROOT:
			id = "NEWROOT";
			break;
		case XLOG_BARK_CREATE_ROOT:
			id = "CREATE_ROOT";
			break;
		case XLOG_BARK_OVERFLOW:
			id = "OVERFLOW";
			break;
	}

	return id;
}
