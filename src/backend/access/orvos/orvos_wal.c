/*
 * orvos_wal.c
 *		WAL-logging for orvos.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_wal.c
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/xlogreader.h"
#include "access/orvos_internal.h"
#include "access/orvos_wal.h"

void
orvos_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case WAL_ORVOS_INIT_METAPAGE:
			ovmeta_initmetapage_redo(record);
			break;
		case WAL_ORVOS_UNDO_NEWPAGE:
			ovundo_newpage_redo(record);
			break;
		case WAL_ORVOS_UNDO_DISCARD:
			ovundo_discard_redo(record);
			break;
		case WAL_ORVOS_BTREE_NEW_ROOT:
			ovmeta_new_btree_root_redo(record);
			break;
		case WAL_ORVOS_BTREE_ADD_LEAF_ITEMS:
			ovbt_leaf_items_redo(record, false);
			break;
		case WAL_ORVOS_BTREE_REPLACE_LEAF_ITEM:
			ovbt_leaf_items_redo(record, true);
			break;
		case WAL_ORVOS_BTREE_REWRITE_PAGES:
			ovbt_rewrite_pages_redo(record);
			break;
		case WAL_ORVOS_TOAST_NEWPAGE:
			ovtoast_newpage_redo(record);
			break;
		case WAL_ORVOS_FPM_DELETE:
			ovfpm_delete_redo(record);
			break;
		default:
			elog(PANIC, "orvos_redo: unknown op code %u", info);
	}
}

void
orvos_mask(char *pagedata, BlockNumber blkno)
{
	Page		page = (Page) pagedata;

	(void) blkno;

	mask_page_lsn_and_checksum(page);

	return;
}
