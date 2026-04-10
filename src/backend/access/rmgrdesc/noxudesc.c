/*
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/rmgrdesc/noxudesc.c
 */
#include "postgres.h"

#include "access/xlogreader.h"
#include "access/noxu_tid.h"
#include "access/noxu_wal.h"
#include "lib/stringinfo.h"

void
noxu_desc(StringInfo buf, XLogReaderState *record)
{
	char	   *rec = XLogRecGetData(record);
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	if (info == WAL_NOXU_INIT_METAPAGE)
	{
		wal_noxu_init_metapage *walrec = (wal_noxu_init_metapage *) rec;

		appendStringInfo(buf, "natts %d", walrec->natts);
	}
	else if (info == WAL_NOXU_UNDO_NEWPAGE)
	{
		wal_noxu_undo_newpage *walrec = (wal_noxu_undo_newpage *) rec;

		appendStringInfo(buf, "first_counter " UINT64_FORMAT, walrec->first_counter);
	}
	else if (info == WAL_NOXU_UNDO_DISCARD)
	{
		wal_noxu_undo_discard *walrec = (wal_noxu_undo_discard *) rec;

		appendStringInfo(buf, "oldest_undorecptr " UINT64_FORMAT ", oldest_undopage %u",
						 walrec->oldest_undorecptr,
						 walrec->oldest_undopage);
	}
	else if (info == WAL_NOXU_BTREE_NEW_ROOT)
	{
		wal_noxu_btree_new_root *walrec = (wal_noxu_btree_new_root *) rec;

		appendStringInfo(buf, "attno %d", walrec->attno);
	}
	else if (info == WAL_NOXU_BTREE_ADD_LEAF_ITEMS)
	{
		wal_noxu_btree_leaf_items *walrec = (wal_noxu_btree_leaf_items *) rec;

		appendStringInfo(buf, "attno %d, %d items, off %d", walrec->attno, walrec->nitems, walrec->off);
	}
	else if (info == WAL_NOXU_BTREE_REPLACE_LEAF_ITEM)
	{
		wal_noxu_btree_leaf_items *walrec = (wal_noxu_btree_leaf_items *) rec;

		appendStringInfo(buf, "attno %d, %d items, off %d", walrec->attno, walrec->nitems, walrec->off);
	}
	else if (info == WAL_NOXU_BTREE_REWRITE_PAGES)
	{
		wal_noxu_btree_rewrite_pages *walrec = (wal_noxu_btree_rewrite_pages *) rec;

		appendStringInfo(buf, "attno %d, numpages %d, recycle_bitmap 0x%08x, old_fpm_head %u",
						 walrec->attno, walrec->numpages,
						 walrec->recycle_bitmap, walrec->old_fpm_head);
	}
	else if (info == WAL_NOXU_OVERFLOW_NEWPAGE)
	{
		wal_noxu_overflow_newpage *walrec = (wal_noxu_overflow_newpage *) rec;

		appendStringInfo(buf, "tid (%u/%d), attno %d, offset %d/%d",
						 NXTidGetBlockNumber(walrec->tid), NXTidGetOffsetNumber(walrec->tid),
						 walrec->attno, walrec->offset, walrec->total_size);
	}
	else if (info == WAL_NOXU_FPM_DELETE)
	{
		wal_noxu_fpm_delete *walrec = (wal_noxu_fpm_delete *) rec;

		appendStringInfo(buf, "old_fpm_head %u", walrec->old_fpm_head);
	}
	else if (info == WAL_NOXU_LSM_INIT_META)
	{
		wal_noxu_lsm_init_meta *walrec = (wal_noxu_lsm_init_meta *) rec;

		appendStringInfo(buf, "base_capacity %d", walrec->base_capacity);
	}
	else if (info == WAL_NOXU_LSM_UPDATE_META)
	{
		appendStringInfo(buf, "update lsm metadata (full-page image)");
	}
	else if (info == WAL_NOXU_LSM_ROW_PAGE)
	{
		wal_noxu_lsm_row_page *walrec = (wal_noxu_lsm_row_page *) rec;

		appendStringInfo(buf, "level %u, segment '%c'",
						 walrec->level_num, walrec->segment_id);
	}
}

const char *
noxu_identify(uint8 info)
{
	const char *id = NULL;

	switch (info & ~XLR_INFO_MASK)
	{
		case WAL_NOXU_INIT_METAPAGE:
			id = "INIT_METAPAGE";
			break;
		case WAL_NOXU_UNDO_NEWPAGE:
			id = "UNDO_NEWPAGE";
			break;
		case WAL_NOXU_UNDO_DISCARD:
			id = "UNDO_DISCARD";
			break;
		case WAL_NOXU_BTREE_NEW_ROOT:
			id = "BTREE_NEW_ROOT";
			break;
		case WAL_NOXU_BTREE_ADD_LEAF_ITEMS:
			id = "BTREE_ADD_LEAF_ITEMS";
			break;
		case WAL_NOXU_BTREE_REPLACE_LEAF_ITEM:
			id = "BTREE_REPLACE_LEAF_ITEM";
			break;
		case WAL_NOXU_BTREE_REWRITE_PAGES:
			id = "BTREE_REWRITE_PAGES";
			break;
		case WAL_NOXU_OVERFLOW_NEWPAGE:
			id = "NOXU_OVERFLOW_NEWPAGE";
			break;
		case WAL_NOXU_FPM_DELETE:
			id = "FPM_DELETE";
			break;
		case WAL_NOXU_LSM_INIT_META:
			id = "LSM_INIT_META";
			break;
		case WAL_NOXU_LSM_UPDATE_META:
			id = "LSM_UPDATE_META";
			break;
		case WAL_NOXU_LSM_ROW_PAGE:
			id = "LSM_ROW_PAGE";
			break;
	}
	return id;
}
