/*
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/orvosdesc.c
 */
#include "postgres.h"

#include "access/xlogreader.h"
#include "access/orvos_tid.h"
#include "access/orvos_wal.h"
#include "lib/stringinfo.h"

void
orvos_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == WAL_ORVOS_INIT_METAPAGE)
	{
		wal_orvos_init_metapage *walrec = (wal_orvos_init_metapage *) rec;

		appendStringInfo(buf, "natts %d", walrec->natts);
	}
	else if (info == WAL_ORVOS_UNDO_NEWPAGE)
	{
		wal_orvos_undo_newpage *walrec = (wal_orvos_undo_newpage *) rec;

		appendStringInfo(buf, "first_counter " UINT64_FORMAT, walrec->first_counter);
	}
	else if (info == WAL_ORVOS_UNDO_DISCARD)
	{
		wal_orvos_undo_discard *walrec = (wal_orvos_undo_discard *) rec;

		appendStringInfo(buf, "oldest_undorecptr " UINT64_FORMAT ", oldest_undopage %u",
						 walrec->oldest_undorecptr.counter,
						 walrec->oldest_undopage);
	}
	else if (info == WAL_ORVOS_BTREE_NEW_ROOT)
	{
		wal_orvos_btree_new_root *walrec = (wal_orvos_btree_new_root *) rec;

		appendStringInfo(buf, "attno %d", walrec->attno);
	}
	else if (info == WAL_ORVOS_BTREE_ADD_LEAF_ITEMS)
	{
		wal_orvos_btree_leaf_items *walrec = (wal_orvos_btree_leaf_items *) rec;

		appendStringInfo(buf, "attno %d, %d items, off %d", walrec->attno, walrec->nitems, walrec->off);
	}
	else if (info == WAL_ORVOS_BTREE_REPLACE_LEAF_ITEM)
	{
		wal_orvos_btree_leaf_items *walrec = (wal_orvos_btree_leaf_items *) rec;

		appendStringInfo(buf, "attno %d, %d items, off %d", walrec->attno, walrec->nitems, walrec->off);
	}
	else if (info == WAL_ORVOS_TOAST_NEWPAGE)
	{
		wal_orvos_toast_newpage *walrec = (wal_orvos_toast_newpage *) rec;

		appendStringInfo(buf, "tid (%u/%d), attno %d, offset %d/%d",
						 OVTidGetBlockNumber(walrec->tid), OVTidGetOffsetNumber(walrec->tid),
						 walrec->attno, walrec->offset, walrec->total_size);
	}
	else if (info == WAL_ORVOS_FPM_DELETE)
	{
		wal_orvos_fpm_delete *walrec = (wal_orvos_fpm_delete *) rec;

		appendStringInfo(buf, "old_fpm_head %u", walrec->old_fpm_head);
	}
}

const char *
orvos_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case WAL_ORVOS_INIT_METAPAGE:
			id = "INIT_METAPAGE";
			break;
		case WAL_ORVOS_UNDO_NEWPAGE:
			id = "UNDO_NEWPAGE";
			break;
		case WAL_ORVOS_UNDO_DISCARD:
			id = "UNDO_DISCARD";
			break;
		case WAL_ORVOS_BTREE_NEW_ROOT:
			id = "BTREE_NEW_ROOT";
			break;
		case WAL_ORVOS_BTREE_ADD_LEAF_ITEMS:
			id = "BTREE_ADD_LEAF_ITEMS";
			break;
		case WAL_ORVOS_BTREE_REPLACE_LEAF_ITEM:
			id = "BTREE_REPLACE_LEAF_ITEM";
			break;
		case WAL_ORVOS_BTREE_REWRITE_PAGES:
			id = "BTREE_REWRITE_PAGES";
			break;
		case WAL_ORVOS_TOAST_NEWPAGE:
			id = "ORVOS_TOAST_NEWPAGE";
			break;
		case WAL_ORVOS_FPM_DELETE:
			id = "FPM_DELETE";
			break;
	}
	return id;
}
