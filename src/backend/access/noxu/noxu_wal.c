/*
 * noxu_wal.c
 *		WAL-logging for noxu.
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_wal.c
 */
#include "postgres.h"

#include "access/bufmask.h"
#include "access/xlogreader.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/noxu_internal.h"
#include "access/noxu_wal.h"
#include "access/relundo.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"

void
noxu_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
		case WAL_NOXU_INIT_METAPAGE:
			nxmeta_initmetapage_redo(record);
			break;
		/*
		 * UNDO WAL records removed - per-relation UNDO handles WAL automatically.
		 * The bespoke UNDO files that generated these records have been deleted.
		 */
#if 0
		case WAL_NOXU_UNDO_NEWPAGE:
			nxundo_newpage_redo(record);
			break;
		case WAL_NOXU_UNDO_DISCARD:
			nxundo_discard_redo(record);
			break;
#endif
		case WAL_NOXU_BTREE_NEW_ROOT:
			nxmeta_new_btree_root_redo(record);
			break;
		case WAL_NOXU_BTREE_ADD_LEAF_ITEMS:
			nxbt_leaf_items_redo(record, false);
			break;
		case WAL_NOXU_BTREE_REPLACE_LEAF_ITEM:
			nxbt_leaf_items_redo(record, true);
			break;
		case WAL_NOXU_BTREE_REWRITE_PAGES:
			nxbt_rewrite_pages_redo(record);
			break;
		case WAL_NOXU_OVERFLOW_NEWPAGE:
			nxoverflow_newpage_redo(record);
			break;
		case WAL_NOXU_FPM_DELETE:
			nxfpm_delete_redo(record);
			break;
		case WAL_NOXU_LSM_INIT_META:
			nx_lsm_init_meta_redo(record);
			break;
		case WAL_NOXU_LSM_UPDATE_META:
			nx_lsm_update_meta_redo(record);
			break;
		case WAL_NOXU_LSM_ROW_PAGE:
			nx_lsm_row_page_redo(record);
			break;

		default:
			elog(PANIC, "noxu_redo: unknown op code %u", info);
	}
}

void
noxu_mask(char *pagedata, BlockNumber blkno)
{
	Page		page = (Page) pagedata;
	PageHeader	pagehdr = (PageHeader) page;

	mask_page_lsn_and_checksum(page);

	mask_page_hint_bits(page);
	mask_unused_space(page);

	/*
	 * The metapage has a lot of things that can change that don't need to
	 * match between the primary and the standby.
	 */
	if (blkno == NX_META_BLK)
		mask_page_content(page);

	if (pagehdr->pd_lower > SizeOfPageHeaderData)
		mask_lp_flags(page);
}

/*
 * XLogRegisterUndoOp - Register an UNDO operation for WAL logging
 *
 * This function registers an UNDO buffer and its associated data for WAL
 * logging. The UNDO operation is stored in the WAL record at the specified
 * block_id.
 *
 * Note: The UNDO data is managed by the RelUndo subsystem, which handles
 * its own WAL logging automatically through RelUndoReserve/RelUndoFinish.
 * However, Noxu bundles UNDO and B-tree changes into single atomic WAL
 * records, so we can't use RelUndoFinish() directly. Instead, we write
 * the UNDO data manually and register it with the WAL record.
 */
void
XLogRegisterUndoOp(uint8 block_id, nx_pending_undo_op *undo_op)
{
	nx_wal_undo_op xlrec;

	xlrec.undoptr = undo_op->reservation.undorecptr;
	xlrec.length = undo_op->reservation.length;
	xlrec.is_update = undo_op->is_update;

	XLogRegisterBuffer(block_id, undo_op->reservation.undobuf,
					   REGBUF_STANDARD);
	XLogRegisterBufData(block_id, (char *) &xlrec, SizeOfNXWalUndoOp);
	XLogRegisterBufData(block_id, (char *) undo_op->payload,
						undo_op->reservation.length);
}

/*
 * XLogRedoUndoOp - Replay an UNDO operation from WAL
 *
 * This function replays an UNDO operation during WAL recovery. It reads
 * the UNDO buffer and data from the WAL record and writes them to the
 * UNDO buffer.
 *
 * Returns the UNDO buffer (caller must release it).
 */
Buffer
XLogRedoUndoOp(XLogReaderState *record, uint8 block_id)
{
	Buffer		buffer;
	XLogRedoAction action;

	action = XLogReadBufferForRedo(record, block_id, &buffer);
	if (action == BLK_NEEDS_REDO)
	{
		nx_wal_undo_op xlrec;
		Size		len;
		char	   *p = XLogRecGetBlockData(record, block_id, &len);
		Page		page;
		char	   *undo_ptr;

		Assert(len >= SizeOfNXWalUndoOp);

		memcpy(&xlrec, p, SizeOfNXWalUndoOp);
		p += SizeOfNXWalUndoOp;
		len -= SizeOfNXWalUndoOp;
		Assert(xlrec.length == len);

		/* Write the UNDO data to the buffer */
		page = BufferGetPage(buffer);
		undo_ptr = PageGetContents(page) + RelUndoGetOffset(xlrec.undoptr);

		START_CRIT_SECTION();
		memcpy(undo_ptr, p, xlrec.length);
		MarkBufferDirty(buffer);
		END_CRIT_SECTION();

		PageSetLSN(page, record->EndRecPtr);
	}
	else if (action == BLK_RESTORED)
	{
		/* Page was restored from full page image, nothing to do */
	}

	return buffer;
}
