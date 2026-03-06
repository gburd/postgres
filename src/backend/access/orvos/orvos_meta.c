/*
 * orvos_meta.c
 *		Routines for handling Orvos metapage
 *
 * The metapage holds a directory of B-tree root block numbers, one for each
 * column.
 *
 * TODO:
 * - extend the root block dir to an overflow page if there are too many
 *   attributes to fit on one page
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/orvos/orvos_meta.c
 */
#include "postgres.h"

#include "access/itup.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/orvos_internal.h"
#include "access/orvos_wal.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

static void ovmeta_wal_log_metapage(Buffer buf, int natts);

static OVMetaCacheData *
ovmeta_populate_cache_from_metapage(Relation rel, Page page)
{
	OVMetaCacheData *cache;
	OVMetaPage *metapg;
	int			natts;

	if (rel->rd_amcache != NULL)
	{
		pfree(rel->rd_amcache);
		rel->rd_amcache = NULL;
	}

	metapg = (OVMetaPage *) PageGetContents(page);

	natts = metapg->nattributes;

	cache =
		MemoryContextAllocZero(CacheMemoryContext,
							   offsetof(OVMetaCacheData, cache_attrs[natts]));
	cache->cache_nattributes = natts;

	for (int i = 0; i < natts; i++)
	{
		cache->cache_attrs[i].root = metapg->tree_root_dir[i].root;
		cache->cache_attrs[i].rightmost = InvalidBlockNumber;
	}

	rel->rd_amcache = cache;
	return cache;
}

OVMetaCacheData *
ovmeta_populate_cache(Relation rel)
{
	OVMetaCacheData *cache;
	Buffer		metabuf;
	BlockNumber nblocks;

	RelationGetSmgr(rel);

	if (rel->rd_amcache != NULL)
	{
		pfree(rel->rd_amcache);
		rel->rd_amcache = NULL;
	}

	nblocks = RelationGetNumberOfBlocks(rel);
	RelationSetTargetBlock(rel, nblocks);
	if (nblocks == 0)
	{
		cache =
			MemoryContextAllocZero(CacheMemoryContext,
								   offsetof(OVMetaCacheData, cache_attrs));
		cache->cache_nattributes = 0;
		rel->rd_amcache = cache;
	}
	else
	{
		metabuf = ReadBuffer(rel, OV_META_BLK);
		LockBuffer(metabuf, BUFFER_LOCK_SHARE);
		cache = ovmeta_populate_cache_from_metapage(rel, BufferGetPage(metabuf));
		UnlockReleaseBuffer(metabuf);
	}

	return cache;
}

static void
ovmeta_expand_metapage_for_new_attributes(Relation rel)
{
	int			natts = RelationGetNumberOfAttributes(rel) + 1;
	Buffer		metabuf;
	Page		page;
	OVMetaPage *metapg;

	metabuf = ReadBuffer(rel, OV_META_BLK);

	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(metabuf);
	metapg = (OVMetaPage *) PageGetContents(page);

	if (natts > metapg->nattributes)
	{
		int			new_pd_lower;

		new_pd_lower = (char *) &metapg->tree_root_dir[natts] - (char *) page;
		if (new_pd_lower > ((PageHeader) page)->pd_upper)
		{
			/*
			 * The root block directory must fit on the metapage.
			 *
			 * TODO: We could extend this by overflowing to another page.
			 */
			elog(ERROR, "too many attributes for orvos");
		}

		START_CRIT_SECTION();

		/* Initialize the new attribute roots to InvalidBlockNumber */
		for (int i = metapg->nattributes; i < natts; i++)
			metapg->tree_root_dir[i].root = InvalidBlockNumber;

		metapg->nattributes = natts;
		((PageHeader) page)->pd_lower = new_pd_lower;

		MarkBufferDirty(metabuf);

		if (RelationNeedsWAL(rel))
			ovmeta_wal_log_metapage(metabuf, natts);

		END_CRIT_SECTION();
	}
	UnlockReleaseBuffer(metabuf);

	if (rel->rd_amcache != NULL)
	{
		pfree(rel->rd_amcache);
		rel->rd_amcache = NULL;
	}
}

static Page
ovmeta_initmetapage_internal(int natts)
{
	Page		page;
	OVMetaPageOpaque *opaque;
	OVMetaPage *metapg;
	int			new_pd_lower;

	/*
	 * It's possible that we error out when building the metapage, if there
	 * are too many attribute, so work on a temporary copy first, before
	 * actually allocating the buffer.
	 */
	page = palloc(BLCKSZ);
	PageInit(page, BLCKSZ, sizeof(OVMetaPageOpaque));

	opaque = (OVMetaPageOpaque *) PageGetSpecialPointer(page);
	opaque->ov_flags = 0;
	opaque->ov_page_id = OV_META_PAGE_ID;

	/* UNDO-related fields */
	opaque->ov_undo_oldestptr.counter = 2;	/* start at 2, so that 0 is always
											 * "old", and 1 means "dead" */
	opaque->ov_undo_head = InvalidBlockNumber;
	opaque->ov_undo_tail = InvalidBlockNumber;
	opaque->ov_undo_tail_first_counter = 2;

	opaque->ov_fpm_head = InvalidBlockNumber;

	metapg = (OVMetaPage *) PageGetContents(page);

	new_pd_lower = (char *) &metapg->tree_root_dir[natts] - (char *) page;
	if (new_pd_lower > ((PageHeader) page)->pd_upper)
	{
		/*
		 * The root block directory must fit on the metapage.
		 *
		 * TODO: We could extend this by overflowing to another page.
		 */
		elog(ERROR, "too many attributes for orvos");
	}

	metapg->nattributes = natts;
	for (int i = 0; i < natts; i++)
		metapg->tree_root_dir[i].root = InvalidBlockNumber;

	((PageHeader) page)->pd_lower = new_pd_lower;
	return page;
}

/*
 * Initialize the metapage for an empty relation.
 */
void
ovmeta_initmetapage(Relation rel)
{
	Buffer		buf;
	Page		page;
	int			natts = RelationGetNumberOfAttributes(rel) + 1;

	/*
	 * Extend the relation to create the metapage. Use the modern
	 * ExtendBufferedRel API which returns the buffer already locked.
	 */
	buf = ExtendBufferedRel(BMR_REL(rel),
							MAIN_FORKNUM,
							NULL,		/* strategy */
							EB_LOCK_FIRST);
	if (BufferGetBlockNumber(buf) != OV_META_BLK)
		elog(ERROR, "table is not empty");
	page = ovmeta_initmetapage_internal(natts);

	START_CRIT_SECTION();
	PageRestoreTempPage(page, BufferGetPage(buf));

	MarkBufferDirty(buf);

	if (RelationNeedsWAL(rel))
		ovmeta_wal_log_metapage(buf, natts);

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

static void
ovmeta_wal_log_metapage(Buffer buf, int natts)
{
	Page		page = BufferGetPage(buf);
	wal_orvos_init_metapage init_rec;
	XLogRecPtr	recptr;

	init_rec.natts = natts;

	XLogBeginInsert();
	XLogRegisterData((char *) &init_rec, SizeOfZSWalInitMetapage);
	XLogRegisterBuffer(0, buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

	recptr = XLogInsert(RM_ORVOS_ID, WAL_ORVOS_INIT_METAPAGE);

	PageSetLSN(page, recptr);
}

static void
ovmeta_wal_log_new_att_root(Buffer metabuf, Buffer rootbuf, AttrNumber attno)
{
	Page		metapage = BufferGetPage(metabuf);
	Page		rootpage = BufferGetPage(rootbuf);
	wal_orvos_btree_new_root xlrec;
	XLogRecPtr	recptr;

	xlrec.attno = attno;

	XLogBeginInsert();
	XLogRegisterData((char *) &xlrec, SizeOfZSWalBtreeNewRoot);
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
	XLogRegisterBuffer(1, rootbuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

	recptr = XLogInsert(RM_ORVOS_ID, WAL_ORVOS_BTREE_NEW_ROOT);

	PageSetLSN(metapage, recptr);
	PageSetLSN(rootpage, recptr);
}

void
ovmeta_initmetapage_redo(XLogReaderState *record)
{
	Buffer		buf;

	/*
	 * Metapage changes are so rare that we rely on full-page images for
	 * replay.
	 */
	if (XLogReadBufferForRedo(record, 0, &buf) != BLK_RESTORED)
		elog(ERROR, "orvos metapage init WAL record did not contain a full-page image");

	Assert(BufferGetBlockNumber(buf) == OV_META_BLK);
	UnlockReleaseBuffer(buf);
}

void
ovmeta_new_btree_root_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_orvos_btree_new_root *xlrec =
		(wal_orvos_btree_new_root *) XLogRecGetData(record);
	AttrNumber	attno = xlrec->attno;
	Buffer		metabuf;
	Buffer		rootbuf;
	Page		rootpage;
	BlockNumber rootblk;
	OVBtreePageOpaque *opaque;

	rootbuf = XLogInitBufferForRedo(record, 1);
	rootpage = (Page) BufferGetPage(rootbuf);
	rootblk = BufferGetBlockNumber(rootbuf);
	/* initialize the page to look like a root leaf */
	rootpage = BufferGetPage(rootbuf);
	PageInit(rootpage, BLCKSZ, sizeof(OVBtreePageOpaque));
	opaque = OVBtreePageGetOpaque(rootpage);
	opaque->ov_attno = attno;
	opaque->ov_next = InvalidBlockNumber;
	opaque->ov_lokey = MinOVTid;
	opaque->ov_hikey = MaxPlusOneOVTid;
	opaque->ov_level = 0;
	opaque->ov_flags = OVBT_ROOT;
	opaque->ov_page_id = OV_BTREE_PAGE_ID;

	PageSetLSN(rootpage, lsn);
	MarkBufferDirty(rootbuf);

	/* Update the metapage to point to it */
	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page		metapage = (Page) BufferGetPage(metabuf);
		OVMetaPage *metapg = (OVMetaPage *) PageGetContents(metapage);

		Assert(BufferGetBlockNumber(metabuf) == OV_META_BLK);
		Assert(metapg->tree_root_dir[attno].root == InvalidBlockNumber);

		metapg->tree_root_dir[attno].root = rootblk;

		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
	UnlockReleaseBuffer(rootbuf);
}

/*
 * Get the block number of the b-tree root for given attribute.
 *
 * If 'readonly' is true, and the root doesn't exist yet (ie. it's an empty
 * table), returns InvalidBlockNumber. Otherwise new root is allocated if
 * the root doesn't exist.
 */
BlockNumber
ovmeta_get_root_for_attribute(Relation rel, AttrNumber attno, bool readonly)
{
	Buffer		metabuf;
	OVMetaPage *metapg;
	BlockNumber rootblk;
	OVMetaCacheData *metacache;

	Assert(attno == OV_META_ATTRIBUTE_NUM || attno >= 1);

	metacache = ovmeta_get_cache(rel);

	if (RelationGetTargetBlock(rel) == 0 ||
		RelationGetTargetBlock(rel) == InvalidBlockNumber)
	{
		BlockNumber nblocks = RelationGetNumberOfBlocks(rel);

		if (nblocks != 0)
			metacache = ovmeta_populate_cache(rel);
		else if (readonly)
			return InvalidBlockNumber;
		else
		{
			LockRelationForExtension(rel, ExclusiveLock);

			/*
			 * Confirm number of blocks is still 0 after taking lock, before
			 * initializing a new metapage
			 */
			nblocks = RelationGetNumberOfBlocks(rel);
			if (nblocks == 0)
				ovmeta_initmetapage(rel);
			UnlockRelationForExtension(rel, ExclusiveLock);
			metacache = ovmeta_populate_cache(rel);
		}
	}

	/*
	 * file has less number of attributes stored compared to catalog. This
	 * happens due to add column default value storing value in catalog and
	 * absent in table. This attribute must be marked with atthasmissing.
	 */
	if (attno >= metacache->cache_nattributes)
	{
		if (readonly)
		{
			/* re-check */
			metacache = ovmeta_populate_cache(rel);
			if (attno >= metacache->cache_nattributes)
				return InvalidBlockNumber;
		}
		else
		{
			ovmeta_expand_metapage_for_new_attributes(rel);
			metacache = ovmeta_populate_cache(rel);
		}
	}

	rootblk = metacache->cache_attrs[attno].root;

	if (!readonly && rootblk == InvalidBlockNumber)
	{
		/* try to allocate one */
		Page		page;

		metabuf = ReadBuffer(rel, OV_META_BLK);

		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(metabuf);
		metapg = (OVMetaPage *) PageGetContents(page);

		/*
		 * Re-check that the root is still invalid, now that we have the
		 * metapage locked.
		 */
		rootblk = metapg->tree_root_dir[attno].root;
		if (rootblk == InvalidBlockNumber)
		{
			Buffer		rootbuf;
			Page		rootpage;
			OVBtreePageOpaque *opaque;

			/* TODO: release lock on metapage while we do I/O */
			rootbuf = ovpage_getnewbuf(rel, metabuf);
			rootblk = BufferGetBlockNumber(rootbuf);

			START_CRIT_SECTION();

			metapg->tree_root_dir[attno].root = rootblk;

			/* initialize the page to look like a root leaf */
			rootpage = BufferGetPage(rootbuf);
			PageInit(rootpage, BLCKSZ, sizeof(OVBtreePageOpaque));
			opaque = OVBtreePageGetOpaque(rootpage);
			opaque->ov_attno = attno;
			opaque->ov_next = InvalidBlockNumber;
			opaque->ov_lokey = MinOVTid;
			opaque->ov_hikey = MaxPlusOneOVTid;
			opaque->ov_level = 0;
			opaque->ov_flags = OVBT_ROOT;
			opaque->ov_page_id = OV_BTREE_PAGE_ID;

			MarkBufferDirty(rootbuf);
			MarkBufferDirty(metabuf);

			if (RelationNeedsWAL(rel))
				ovmeta_wal_log_new_att_root(metabuf, rootbuf, attno);

			END_CRIT_SECTION();

			UnlockReleaseBuffer(rootbuf);
		}
		UnlockReleaseBuffer(metabuf);

		metacache->cache_attrs[attno].root = rootblk;
	}

	return rootblk;
}
