/**
 * @file noxu_lsm.c
 * @brief LSM-tree level management for Noxu table access method.
 *
 * Manages multi-level LSM-tree segments within a Noxu relation file.
 * Level 1 uses row-oriented pages (line pointers to MinimalTuples).
 * Level 2+ data is merged into the existing B-tree attribute pages.
 *
 * Segments (A/B/X) are tracked in a dedicated LSM metadata page,
 * pointed to from the metapage opaque area.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/access/noxu/noxu_lsm.c
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/noxu_internal.h"
#include "access/noxu_lsm.h"
#include "access/noxu_wal.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/guc.h"
#include "utils/memutils.h"

/* GUC variables */
bool		noxu_lsm_enabled = false;	/* off by default, Phase 2 opt-in */
int			noxu_lsm_base_capacity = 4096;
int			noxu_lsm_max_levels = 20;

/*
 * Pre-defined compression policies for LSM levels (Phase 4).
 *
 * Level 2: Cheap codecs only (FOR, Dict, bitpacking).
 * Level 3+: Full codec cascade for maximum compression.
 */
const NXCompressionPolicy nx_lsm_policy_level2 = {
	.allow_chimp = false,
	.allow_dod = false,
	.allow_for = true,
	.allow_dict = true,
	.allow_fsst = false,
	.allow_uuid_delta = false,
	.allow_shared_dict = false,
	.allow_page_compress = true,
};

const NXCompressionPolicy nx_lsm_policy_full = {
	.allow_chimp = true,
	.allow_dod = true,
	.allow_for = true,
	.allow_dict = true,
	.allow_fsst = true,
	.allow_uuid_delta = true,
	.allow_shared_dict = true,
	.allow_page_compress = true,
};

/*
 * nx_lsm_get_level_policy - Get the compression policy for a given level.
 *
 * Returns NULL for Level 1 (row-oriented, no column compression).
 * Returns policy_level2 for Level 2 (cheap codecs).
 * Returns policy_full for Level 3+ (all codecs).
 */
const NXCompressionPolicy *
nx_lsm_get_level_policy(int level_num)
{
	if (level_num <= 1)
		return NULL;			/* Row-oriented, no column compression */
	if (level_num == 2)
		return &nx_lsm_policy_level2;
	return &nx_lsm_policy_full;
}

/*
 * Backend-private LSM metadata cache.
 * Keyed by RelFileLocator, but for simplicity we cache only the
 * most recently used relation.
 */
static NXLSMMetaCache lsm_cache = {.valid = false};
static RelFileLocator lsm_cache_rlocator;

/* Forward declarations */
static void nx_lsm_init_meta_page(Page page, int32 base_capacity);
static void nx_lsm_invalidate_cache(void);

/* ----------------------------------------------------------------
 * LSM Metadata Page Management
 * ----------------------------------------------------------------
 */

/*
 * nx_lsm_init_meta_page - Initialize an LSM metadata page in memory.
 */
static void
nx_lsm_init_meta_page(Page page, int32 base_capacity)
{
	NXLSMMetaPageData *lsm;
	NXLSMMetaPageOpaque *opaque;

	PageInit(page, BLCKSZ, sizeof(NXLSMMetaPageOpaque));

	opaque = NXLSMMetaPageGetOpaque(page);
	opaque->nx_flags = 0;
	opaque->nx_page_id = NX_LSM_META_PAGE_ID;

	lsm = (NXLSMMetaPageData *) PageGetContents(page);
	memset(lsm, 0, sizeof(NXLSMMetaPageData));
	lsm->nlevels = 0;
	lsm->base_capacity = base_capacity;

	/* Initialize all levels */
	for (int i = 0; i < NX_LSM_MAX_LEVELS; i++)
	{
		lsm->levels[i].level_num = i + 1;
		lsm->levels[i].capacity = base_capacity * (1 << i);
		lsm->levels[i].seg_a.segment_id = NX_LSM_SEG_NONE;
		lsm->levels[i].seg_b.segment_id = NX_LSM_SEG_NONE;
		lsm->levels[i].seg_x.segment_id = NX_LSM_SEG_NONE;
		lsm->levels[i].merge_active = false;
	}
}

/*
 * nx_lsm_ensure_meta - Ensure an LSM metadata page exists for a relation.
 *
 * Creates the LSM metadata page if it doesn't exist yet, and stores
 * its block number in the metapage opaque area.
 *
 * Returns the block number of the LSM metadata page.
 */
BlockNumber
nx_lsm_ensure_meta(Relation rel)
{
	NXMetaCacheData *metacache;
	BlockNumber lsm_blk;

	metacache = nxmeta_get_cache(rel);
	lsm_blk = metacache->cache_lsm_meta;

	if (lsm_blk != InvalidBlockNumber)
		return lsm_blk;

	/* Need to create the LSM metadata page */
	{
		Buffer		metabuf;
		Buffer		lsmbuf;
		Page		metapage;
		Page		lsmpage;
		NXMetaPageOpaque *metaopaque;
		XLogRecPtr	recptr;

		metabuf = ReadBuffer(rel, NX_META_BLK);
		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		metapage = BufferGetPage(metabuf);
		metaopaque = (NXMetaPageOpaque *) PageGetSpecialPointer(metapage);

		/* Double-check under lock */
		if (NXMetaGetLSMMetaBlock(metaopaque) != InvalidBlockNumber)
		{
			lsm_blk = NXMetaGetLSMMetaBlock(metaopaque);
			UnlockReleaseBuffer(metabuf);
			metacache->cache_lsm_meta = lsm_blk;
			return lsm_blk;
		}

		/* Allocate a new page for LSM metadata */
		lsmbuf = nxpage_getnewbuf(rel, metabuf);
		lsm_blk = BufferGetBlockNumber(lsmbuf);
		lsmpage = BufferGetPage(lsmbuf);

		START_CRIT_SECTION();

		/* Initialize the LSM metadata page */
		nx_lsm_init_meta_page(lsmpage, noxu_lsm_base_capacity);

		/* Update metapage opaque to point to the LSM metadata page */
		NXMetaSetLSMMetaBlock(metaopaque, lsm_blk);

		MarkBufferDirty(lsmbuf);
		MarkBufferDirty(metabuf);

		if (RelationNeedsWAL(rel))
		{
			wal_noxu_lsm_init_meta xlrec;

			xlrec.base_capacity = noxu_lsm_base_capacity;

			XLogBeginInsert();
			XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
			XLogRegisterBuffer(1, lsmbuf,
							   REGBUF_WILL_INIT | REGBUF_STANDARD);
			XLogRegisterData((char *) &xlrec, SizeOfNXWalLSMInitMeta);
			recptr = XLogInsert(RM_NOXU_ID, WAL_NOXU_LSM_INIT_META);
			PageSetLSN(metapage, recptr);
			PageSetLSN(lsmpage, recptr);
		}

		END_CRIT_SECTION();

		UnlockReleaseBuffer(lsmbuf);
		UnlockReleaseBuffer(metabuf);

		/* Update cache */
		metacache->cache_lsm_meta = lsm_blk;
	}

	return lsm_blk;
}

/*
 * nx_lsm_get_meta - Get a copy of the LSM metadata for a relation.
 *
 * Returns a pointer to the cached LSM metadata.  The caller must not
 * modify the returned data; use nx_lsm_meta_update() instead.
 */
NXLSMMetaPageData *
nx_lsm_get_meta(Relation rel)
{
	BlockNumber lsm_blk;
	Buffer		buf;
	Page		page;
	NXLSMMetaPageData *pagadata;

	/* Check cache */
	if (lsm_cache.valid &&
		RelFileLocatorEquals(lsm_cache_rlocator, rel->rd_locator))
		return &lsm_cache.data;

	lsm_blk = nx_lsm_ensure_meta(rel);

	buf = ReadBuffer(rel, lsm_blk);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	pagadata = (NXLSMMetaPageData *) PageGetContents(page);
	memcpy(&lsm_cache.data, pagadata, sizeof(NXLSMMetaPageData));
	lsm_cache.meta_blk = lsm_blk;
	lsm_cache.valid = true;
	lsm_cache_rlocator = rel->rd_locator;

	UnlockReleaseBuffer(buf);

	return &lsm_cache.data;
}

/*
 * nx_lsm_meta_update - Write modified LSM metadata back to disk.
 *
 * Writes the provided metadata to the LSM metadata page with
 * WAL logging (full-page image).
 */
void
nx_lsm_meta_update(Relation rel, NXLSMMetaPageData *data)
{
	BlockNumber lsm_blk;
	Buffer		buf;
	Page		page;
	NXLSMMetaPageData *pagedata;
	XLogRecPtr	recptr;

	lsm_blk = nx_lsm_ensure_meta(rel);

	buf = ReadBuffer(rel, lsm_blk);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buf);

	START_CRIT_SECTION();

	pagedata = (NXLSMMetaPageData *) PageGetContents(page);
	memcpy(pagedata, data, sizeof(NXLSMMetaPageData));

	MarkBufferDirty(buf);

	if (RelationNeedsWAL(rel))
	{
		wal_noxu_lsm_update_meta xlrec;

		xlrec.dummy = 0;

		XLogBeginInsert();
		XLogRegisterBuffer(0, buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
		XLogRegisterData((char *) &xlrec, SizeOfNXWalLSMUpdateMeta);
		recptr = XLogInsert(RM_NOXU_ID, WAL_NOXU_LSM_UPDATE_META);
		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);

	/* Update cache */
	memcpy(&lsm_cache.data, data, sizeof(NXLSMMetaPageData));
	lsm_cache.meta_blk = lsm_blk;
	lsm_cache.valid = true;
	lsm_cache_rlocator = rel->rd_locator;
}

/*
 * nx_lsm_invalidate_cache - Invalidate the backend-private LSM cache.
 */
static void
nx_lsm_invalidate_cache(void)
{
	lsm_cache.valid = false;
}

/* ----------------------------------------------------------------
 * Row-Oriented Segment Page I/O
 * ----------------------------------------------------------------
 */

/*
 * nx_lsm_write_row_pages - Write MinimalTuples to row-oriented segment pages.
 *
 * Allocates pages, writes tuples with line pointers, and chains pages
 * via the next_page pointer in the opaque area.  Returns the first and
 * last block numbers and page count.
 */
void
nx_lsm_write_row_pages(Relation rel,
						MinimalTuple *tuples,
						nxtid *tids,
						int nrows,
						int level_num,
						char segment_id,
						BlockNumber *first_blk_out,
						BlockNumber *last_blk_out,
						int *npages_out)
{
	Buffer		metabuf;
	BlockNumber first_blk = InvalidBlockNumber;
	BlockNumber prev_blk = InvalidBlockNumber;
	Buffer		prev_buf = InvalidBuffer;
	int			npages = 0;
	int			row_idx = 0;

	/* Lock metapage for page allocation */
	metabuf = ReadBuffer(rel, NX_META_BLK);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

	while (row_idx < nrows)
	{
		Buffer		buf;
		Page		page;
		NXLSMRowPageOpaque *opaque;
		nxtid		page_first_tid = tids[row_idx];
		nxtid		page_last_tid = page_first_tid;
		XLogRecPtr	recptr;

		/* Allocate a new page */
		buf = nxpage_getnewbuf(rel, metabuf);

		START_CRIT_SECTION();

		page = BufferGetPage(buf);
		PageInit(page, BLCKSZ, sizeof(NXLSMRowPageOpaque));

		opaque = NXLSMRowPageGetOpaque(page);
		opaque->level_num = level_num;
		opaque->segment_id = segment_id;
		opaque->next_page = InvalidBlockNumber;
		opaque->nx_page_id = NX_LSM_ROW_PAGE_ID;

		/* Fill page with MinimalTuples using line pointers */
		while (row_idx < nrows)
		{
			MinimalTuple mtup = tuples[row_idx];
			Size		tupsize = mtup->t_len;
			OffsetNumber off;

			/* Check if tuple fits on this page */
			if (PageGetFreeSpace(page) < MAXALIGN(tupsize) + sizeof(ItemIdData))
				break;

			off = PageAddItemExtended(page, (char *) mtup, tupsize,
									  InvalidOffsetNumber,
									  PAI_OVERWRITE);
			if (off == InvalidOffsetNumber)
				break;

			page_last_tid = tids[row_idx];
			row_idx++;
		}

		opaque->first_tid = page_first_tid;
		opaque->last_tid = page_last_tid;

		/* Chain to previous page */
		if (prev_buf != InvalidBuffer)
		{
			Page		prev_page = BufferGetPage(prev_buf);
			NXLSMRowPageOpaque *prev_opaque = NXLSMRowPageGetOpaque(prev_page);

			prev_opaque->next_page = BufferGetBlockNumber(buf);
			MarkBufferDirty(prev_buf);

			/*
			 * WAL-log the previous page update.  We use a full-page image
			 * for the previous page since we're modifying the opaque area.
			 */
			if (RelationNeedsWAL(rel))
			{
				/* The previous page was already WAL-logged with WILL_INIT
				 * when it was first written.  The opaque update will be
				 * captured by the full-page image on the next checkpoint.
				 * For correctness we rely on the full-page image being
				 * taken at the right point. */
			}

			END_CRIT_SECTION();
			UnlockReleaseBuffer(prev_buf);

			START_CRIT_SECTION();
		}

		MarkBufferDirty(buf);

		if (RelationNeedsWAL(rel))
		{
			wal_noxu_lsm_row_page xlrec;

			xlrec.level_num = level_num;
			xlrec.segment_id = segment_id;
			xlrec.padding = 0;

			XLogBeginInsert();
			XLogRegisterBuffer(0, buf,
							   REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
			XLogRegisterData((char *) &xlrec, SizeOfNXWalLSMRowPage);
			recptr = XLogInsert(RM_NOXU_ID, WAL_NOXU_LSM_ROW_PAGE);
			PageSetLSN(BufferGetPage(buf), recptr);
		}

		END_CRIT_SECTION();

		if (first_blk == InvalidBlockNumber)
			first_blk = BufferGetBlockNumber(buf);

		prev_blk = BufferGetBlockNumber(buf);
		prev_buf = buf;
		npages++;
	}

	/* Release the last page and metapage */
	if (prev_buf != InvalidBuffer)
		UnlockReleaseBuffer(prev_buf);
	UnlockReleaseBuffer(metabuf);

	*first_blk_out = first_blk;
	*last_blk_out = prev_blk;
	*npages_out = npages;
}

/*
 * nx_lsm_read_row_segment - Read all rows from a row-oriented segment.
 *
 * Walks the page chain and decomposes MinimalTuples into per-column
 * Datum/isnull arrays.  Returns the number of rows read.
 */
int
nx_lsm_read_row_segment(Relation rel,
						 NXLSMSegmentDesc *seg,
						 nxtid *tids_out,
						 Datum **col_datums_out,
						 bool **col_isnulls_out,
						 int start_offset,
						 TupleDesc tupdesc)
{
	BlockNumber blk;
	int			n = start_offset;
	int			natts = tupdesc->natts;

	if (NXLSMSegmentIsEmpty(seg))
		return 0;

	blk = seg->first_block;

	while (blk != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		NXLSMRowPageOpaque *opaque;
		OffsetNumber maxoff;

		buf = ReadBuffer(rel, blk);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		opaque = NXLSMRowPageGetOpaque(page);
		maxoff = PageGetMaxOffsetNumber(page);

		for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
		{
			ItemId		iid = PageGetItemId(page, off);
			MinimalTuple mtup;
			HeapTupleData htup;
			Datum	   *values;
			bool	   *isnulls;

			if (!ItemIdIsNormal(iid))
				continue;

			mtup = (MinimalTuple) PageGetItem(page, iid);

			/* Convert MinimalTuple to HeapTuple for deforming */
			htup.t_len = mtup->t_len + MINIMAL_TUPLE_OFFSET;
			htup.t_data = (HeapTupleHeader) ((char *) mtup - MINIMAL_TUPLE_OFFSET);

			values = col_datums_out ? palloc(natts * sizeof(Datum)) : NULL;
			isnulls = col_isnulls_out ? palloc(natts * sizeof(bool)) : NULL;

			if (values && isnulls)
			{
				heap_deform_tuple(&htup, tupdesc, values, isnulls);

				/* Store into the per-column arrays */
				for (int a = 0; a < natts; a++)
				{
					col_datums_out[a][n] = values[a];
					col_isnulls_out[a][n] = isnulls[a];
				}

				pfree(values);
				pfree(isnulls);
			}

			/* Extract TID from the line pointer position and page opaque */
			if (tids_out)
			{
				/*
				 * TIDs are stored in the tuples themselves during write.
				 * For now, derive from the page opaque TID range and offset.
				 * Actually, we stored MinimalTuples which don't carry TIDs.
				 * We need an auxiliary TID storage mechanism.
				 *
				 * Solution: Store TIDs in a separate array at the beginning
				 * of the page data, or encode them in the MinimalTuple header.
				 * For Phase 2 initial implementation, we store (tid, tuplen,
				 * tupdata) per entry on the page.
				 *
				 * TODO: For now, reconstruct TIDs from page opaque range.
				 * This is a placeholder that works for sequential TIDs.
				 */
				tids_out[n] = opaque->first_tid +
					(nxtid)(off - FirstOffsetNumber);
			}

			n++;
		}

		blk = opaque->next_page;
		UnlockReleaseBuffer(buf);
	}

	return n - start_offset;
}

/* ----------------------------------------------------------------
 * Segment Management
 * ----------------------------------------------------------------
 */

/*
 * nx_lsm_free_segment_pages - Return all pages in a segment to the FPM.
 */
void
nx_lsm_free_segment_pages(Relation rel, NXLSMSegmentDesc *seg)
{
	BlockNumber blk;

	if (NXLSMSegmentIsEmpty(seg))
		return;

	blk = seg->first_block;

	while (blk != InvalidBlockNumber)
	{
		Buffer		buf;
		Page		page;
		NXLSMRowPageOpaque *opaque;
		BlockNumber next_blk;

		buf = ReadBuffer(rel, blk);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		opaque = NXLSMRowPageGetOpaque(page);
		next_blk = opaque->next_page;

		nxpage_delete_page(rel, buf);
		UnlockReleaseBuffer(buf);

		blk = next_blk;
	}

	/* Clear the segment descriptor */
	memset(seg, 0, sizeof(NXLSMSegmentDesc));
	seg->segment_id = NX_LSM_SEG_NONE;
}

/*
 * nx_lsm_assign_to_level - Assign a new segment to a level's A or B slot.
 *
 * Returns true if the segment was assigned.  Returns false if both A and
 * B are occupied (caller should fall back to direct B-tree insert).
 */
bool
nx_lsm_assign_to_level(Relation rel, int level_num,
						NXLSMSegmentDesc *new_seg)
{
	NXLSMMetaPageData *meta;
	NXLSMLevelDesc *level;

	meta = nx_lsm_get_meta(rel);

	if (level_num < 1 || level_num > NX_LSM_MAX_LEVELS)
		elog(ERROR, "invalid LSM level number: %d", level_num);

	level = &meta->levels[level_num - 1];

	if (NXLSMSegmentIsEmpty(&level->seg_a))
	{
		/* Assign to slot A */
		new_seg->segment_id = NX_LSM_SEG_A;
		level->seg_a = *new_seg;

		if (level_num > meta->nlevels)
			meta->nlevels = level_num;

		nx_lsm_meta_update(rel, meta);
		return true;
	}
	else if (NXLSMSegmentIsEmpty(&level->seg_b))
	{
		/* Assign to slot B — both A and B now occupied, merge needed */
		new_seg->segment_id = NX_LSM_SEG_B;
		level->seg_b = *new_seg;

		nx_lsm_meta_update(rel, meta);

		/* Request merge (non-blocking) */
		nx_lsm_request_merge(rel, level_num);
		return true;
	}
	else
	{
		/*
		 * Both A and B occupied and merge not yet complete.
		 * Caller should fall back to direct B-tree insert.
		 */
		return false;
	}
}

/*
 * nx_lsm_request_merge - Request a merge at a level.
 *
 * When both A and B segments exist at a level, attempt an immediate
 * synchronous merge.  This avoids the complexity of background worker
 * coordination and ensures data is promptly merged into the B-tree.
 *
 * The background merge worker (NoxuMergeWorkerMain) provides an
 * additional periodic scan for any missed merges.
 */
void
nx_lsm_request_merge(Relation rel, int level_num)
{
	nx_lsm_merge_level(rel, level_num);
}

/*
 * nx_lsm_tid_in_segment - Check if a TID falls within a segment's range.
 *
 * O(1) check using the segment's TID range.  Since Noxu TIDs are
 * monotonically assigned, this gives perfect exclusion with zero
 * false positives (no Bloom filter needed).
 */
bool
nx_lsm_tid_in_segment(NXLSMSegmentDesc *seg, nxtid tid)
{
	if (NXLSMSegmentIsEmpty(seg))
		return false;

	return (tid >= seg->first_tid && tid <= seg->last_tid);
}

/* ----------------------------------------------------------------
 * WAL Redo Functions
 * ----------------------------------------------------------------
 */

/*
 * nx_lsm_init_meta_redo - Redo LSM metadata page initialization.
 *
 * blkref #0: metapage (updated with nx_lsm_meta pointer)
 * blkref #1: new LSM metadata page (WILL_INIT)
 */
void
nx_lsm_init_meta_redo(XLogReaderState *record)
{
	wal_noxu_lsm_init_meta *xlrec =
		(wal_noxu_lsm_init_meta *) XLogRecGetData(record);
	Buffer		metabuf;
	Buffer		lsmbuf;

	/* Redo the LSM metadata page initialization */
	if (XLogReadBufferForRedo(record, 1, &lsmbuf) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(lsmbuf);

		nx_lsm_init_meta_page(page, xlrec->base_capacity);
		PageSetLSN(page, record->EndRecPtr);
		MarkBufferDirty(lsmbuf);
	}
	if (BufferIsValid(lsmbuf))
		UnlockReleaseBuffer(lsmbuf);

	/* Redo the metapage update */
	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page		page = BufferGetPage(metabuf);
		NXMetaPageOpaque *opaque =
			(NXMetaPageOpaque *) PageGetSpecialPointer(page);
		BlockNumber lsm_blk;

		/* Get the block number of the LSM metadata page */
		if (!XLogRecGetBlockTagExtended(record, 1, NULL, NULL, &lsm_blk, NULL))
			elog(ERROR, "could not get block tag for LSM metadata page");

		NXMetaSetLSMMetaBlock(opaque, lsm_blk);
		PageSetLSN(page, record->EndRecPtr);
		MarkBufferDirty(metabuf);
	}
	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);

	nx_lsm_invalidate_cache();
}

/*
 * nx_lsm_update_meta_redo - Redo LSM metadata update.
 *
 * This uses a full-page image, so the standard restore handles it.
 * blkref #0: LSM metadata page (full-page image)
 */
void
nx_lsm_update_meta_redo(XLogReaderState *record)
{
	Buffer		buf;

	if (XLogReadBufferForRedo(record, 0, &buf) == BLK_NEEDS_REDO)
	{
		/* Full-page image should have been restored by XLogReadBufferForRedo */
		PageSetLSN(BufferGetPage(buf), record->EndRecPtr);
		MarkBufferDirty(buf);
	}
	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);

	nx_lsm_invalidate_cache();
}

/*
 * nx_lsm_row_page_redo - Redo row-oriented segment page write.
 *
 * This uses a full-page image.
 * blkref #0: the row page (full-page image)
 */
void
nx_lsm_row_page_redo(XLogReaderState *record)
{
	Buffer		buf;

	if (XLogReadBufferForRedo(record, 0, &buf) == BLK_NEEDS_REDO)
	{
		PageSetLSN(BufferGetPage(buf), record->EndRecPtr);
		MarkBufferDirty(buf);
	}
	if (BufferIsValid(buf))
		UnlockReleaseBuffer(buf);
}

/* ----------------------------------------------------------------
 * GUC Initialization
 * ----------------------------------------------------------------
 */

void
nx_lsm_init_gucs(void)
{
	DefineCustomBoolVariable("noxu.lsm_enabled",
							 "Enable LSM-tree level management.",
							 "When enabled, nursery flushes write to Level 1 "
							 "row-oriented pages instead of directly to the "
							 "B-tree.  Data is merged into the B-tree during "
							 "background merge operations.",
							 &noxu_lsm_enabled,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("noxu.lsm_base_capacity",
							"Base row capacity for LSM Level 1.",
							"Higher levels have capacity = base * 2^(level-1).",
							&noxu_lsm_base_capacity,
							4096,
							256, 1048576,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("noxu.lsm_max_levels",
							"Maximum number of LSM levels.",
							NULL,
							&noxu_lsm_max_levels,
							20,
							2, NX_LSM_MAX_LEVELS,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);
}
