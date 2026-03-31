/*
 * noxu_meta.c
 *		Routines for handling Noxu metapage
 *
 * The metapage holds a directory of B-tree root block numbers, one for each
 * column.
 *
 * The root block directory (tree_root_dir[]) is stored in the metapage body
 * between pd_lower and pd_upper.  Each entry is one NXRootDirItem (4 bytes),
 * so a standard 8 kB page can hold roughly 2000 attributes (including the
 * TID tree at index 0).  If the directory outgrows the metapage, the table
 * creation or ALTER TABLE ADD COLUMN will error out.
 *
 * Extension strategy: if overflow is ever needed, the metapage could store
 * a pointer to a continuation page that holds additional NXRootDirItem
 * entries.  nxmeta_get_root_for_attribute() would follow the overflow chain
 * transparently.  The in-memory NXMetaCacheData already uses a flexible
 * array and would need no structural change.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/noxu/noxu_meta.c
 */
#include "postgres.h"

#include "access/itup.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/noxu_internal.h"
#include "access/noxu_wal.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

static void nxmeta_wal_log_metapage(Buffer buf, int natts);

static NXMetaCacheData *
nxmeta_populate_cache_from_metapage(Relation rel, Page page)
{
	NXMetaCacheData *cache;
	NXMetaPage *metapg;
	int			natts;

	if (rel->rd_amcache != NULL)
	{
		pfree(rel->rd_amcache);
		rel->rd_amcache = NULL;
	}

	metapg = (NXMetaPage *) PageGetContents(page);

	natts = metapg->nattributes;

	cache =
		MemoryContextAllocZero(CacheMemoryContext,
							   offsetof(NXMetaCacheData, cache_attrs[natts]));
	cache->cache_nattributes = natts;

	for (int i = 0; i < natts; i++)
	{
		cache->cache_attrs[i].root = metapg->tree_root_dir[i].root;
		cache->cache_attrs[i].rightmost = InvalidBlockNumber;
		cache->cache_attrs[i].dict_page = metapg->tree_root_dir[i].dict_page;
		cache->cache_attrs[i].dict_generation = metapg->tree_root_dir[i].dict_generation;
	}

	rel->rd_amcache = cache;
	return cache;
}

NXMetaCacheData *
nxmeta_populate_cache(Relation rel)
{
	NXMetaCacheData *cache;
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
								   offsetof(NXMetaCacheData, cache_attrs));
		cache->cache_nattributes = 0;
		rel->rd_amcache = cache;
	}
	else
	{
		metabuf = ReadBuffer(rel, NX_META_BLK);
		LockBuffer(metabuf, BUFFER_LOCK_SHARE);
		cache = nxmeta_populate_cache_from_metapage(rel, BufferGetPage(metabuf));
		UnlockReleaseBuffer(metabuf);
	}

	return cache;
}

static void
nxmeta_expand_metapage_for_new_attributes(Relation rel)
{
	int			natts = RelationGetNumberOfAttributes(rel) + 1;
	Buffer		metabuf;
	Page		page;
	NXMetaPage *metapg;

	metabuf = ReadBuffer(rel, NX_META_BLK);

	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(metabuf);
	metapg = (NXMetaPage *) PageGetContents(page);

	if (natts > metapg->nattributes)
	{
		int			new_pd_lower;

		new_pd_lower = (char *) &metapg->tree_root_dir[natts] - (char *) page;
		if (new_pd_lower > ((PageHeader) page)->pd_upper)
		{
			/*
			 * The root block directory must fit on the metapage.  With 8 kB
			 * pages this allows ~2000 attributes, which is well above
			 * PostgreSQL's MaxHeapAttributeNumber (1600).  If overflow is
			 * ever needed, a continuation page could be chained from the
			 * metapage opaque area.
			 */
			elog(ERROR, "too many attributes for noxu");
		}

		START_CRIT_SECTION();

		/* Initialize the new attribute roots to InvalidBlockNumber */
		for (int i = metapg->nattributes; i < natts; i++)
		{
			metapg->tree_root_dir[i].root = InvalidBlockNumber;
			metapg->tree_root_dir[i].dict_page = InvalidBlockNumber;
			metapg->tree_root_dir[i].dict_generation = 0;
		}

		metapg->nattributes = natts;
		((PageHeader) page)->pd_lower = new_pd_lower;

		MarkBufferDirty(metabuf);

		if (RelationNeedsWAL(rel))
			nxmeta_wal_log_metapage(metabuf, natts);

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
nxmeta_initmetapage_internal(int natts)
{
	Page		page;
	NXMetaPageOpaque *opaque;
	NXMetaPage *metapg;
	int			new_pd_lower;

	/*
	 * It's possible that we error out when building the metapage, if there
	 * are too many attribute, so work on a temporary copy first, before
	 * actually allocating the buffer.
	 */
	page = palloc(BLCKSZ);
	PageInit(page, BLCKSZ, sizeof(NXMetaPageOpaque));

	opaque = (NXMetaPageOpaque *) PageGetSpecialPointer(page);
	opaque->nx_flags = 0;
	opaque->nx_page_id = NX_META_PAGE_ID;

	/*
	 * Deprecated UNDO-related fields: These are no longer used.
	 * Per-relation UNDO is now handled by the RelUndo subsystem in a
	 * separate UNDO fork. We initialize them to zero to avoid using
	 * uninitialized values.
	 */
	opaque->nx_undo_oldestptr = MakeRelUndoRecPtr(0, 0, 0);
	opaque->nx_undo_head = InvalidBlockNumber;
	opaque->nx_undo_tail = InvalidBlockNumber;
	opaque->nx_undo_tail_first_counter = 0;

	opaque->nx_fpm_head = InvalidBlockNumber;

	metapg = (NXMetaPage *) PageGetContents(page);

	new_pd_lower = (char *) &metapg->tree_root_dir[natts] - (char *) page;
	if (new_pd_lower > ((PageHeader) page)->pd_upper)
	{
		/*
		 * The root block directory must fit on the metapage.  With 8 kB
		 * pages this allows ~2000 attributes, which is well above
		 * PostgreSQL's MaxHeapAttributeNumber (1600).  If overflow is
		 * ever needed, a continuation page could be chained from the
		 * metapage opaque area.
		 */
		elog(ERROR, "too many attributes for noxu");
	}

	metapg->nattributes = natts;
	for (int i = 0; i < natts; i++)
	{
		metapg->tree_root_dir[i].root = InvalidBlockNumber;
		metapg->tree_root_dir[i].dict_page = InvalidBlockNumber;
		metapg->tree_root_dir[i].dict_generation = 0;
	}

	((PageHeader) page)->pd_lower = new_pd_lower;
	return page;
}

/*
 * Initialize the metapage for an empty relation.
 */
void
nxmeta_initmetapage(Relation rel)
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
	if (BufferGetBlockNumber(buf) != NX_META_BLK)
		elog(ERROR, "table is not empty");
	page = nxmeta_initmetapage_internal(natts);

	START_CRIT_SECTION();
	PageRestoreTempPage(page, BufferGetPage(buf));

	MarkBufferDirty(buf);

	if (RelationNeedsWAL(rel))
		nxmeta_wal_log_metapage(buf, natts);

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buf);
}

static void
nxmeta_wal_log_metapage(Buffer buf, int natts)
{
	Page		page = BufferGetPage(buf);
	wal_noxu_init_metapage init_rec;
	XLogRecPtr	recptr;

	init_rec.natts = natts;

	XLogBeginInsert();

	/* Register ALL buffers first, before any data */
	XLogRegisterBuffer(0, buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

	/* Now register data after buffers are registered */
	XLogRegisterData((char *) &init_rec, SizeOfNXWalInitMetapage);

	recptr = XLogInsert(RM_NOXU_ID, WAL_NOXU_INIT_METAPAGE);

	PageSetLSN(page, recptr);
}

static void
nxmeta_wal_log_new_att_root(Buffer metabuf, Buffer rootbuf, AttrNumber attno)
{
	Page		metapage = BufferGetPage(metabuf);
	Page		rootpage = BufferGetPage(rootbuf);
	wal_noxu_btree_new_root xlrec;
	XLogRecPtr	recptr;

	xlrec.attno = attno;

	XLogBeginInsert();

	/* Register ALL buffers first, before any data */
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
	XLogRegisterBuffer(1, rootbuf, REGBUF_WILL_INIT | REGBUF_STANDARD);

	/* Now register data after buffers are registered */
	XLogRegisterData((char *) &xlrec, SizeOfNXWalBtreeNewRoot);

	recptr = XLogInsert(RM_NOXU_ID, WAL_NOXU_BTREE_NEW_ROOT);

	PageSetLSN(metapage, recptr);
	PageSetLSN(rootpage, recptr);
}

void
nxmeta_initmetapage_redo(XLogReaderState *record)
{
	Buffer		buf;

	/*
	 * Metapage changes are so rare that we rely on full-page images for
	 * replay.
	 */
	if (XLogReadBufferForRedo(record, 0, &buf) != BLK_RESTORED)
		elog(ERROR, "noxu metapage init WAL record did not contain a full-page image");

	Assert(BufferGetBlockNumber(buf) == NX_META_BLK);
	UnlockReleaseBuffer(buf);
}

void
nxmeta_new_btree_root_redo(XLogReaderState *record)
{
	XLogRecPtr	lsn = record->EndRecPtr;
	wal_noxu_btree_new_root *xlrec =
		(wal_noxu_btree_new_root *) XLogRecGetData(record);
	AttrNumber	attno = xlrec->attno;
	Buffer		metabuf;
	Buffer		rootbuf;
	Page		rootpage;
	BlockNumber rootblk;
	NXBtreePageOpaque *opaque;

	rootbuf = XLogInitBufferForRedo(record, 1);
	rootpage = (Page) BufferGetPage(rootbuf);
	rootblk = BufferGetBlockNumber(rootbuf);
	/* initialize the page to look like a root leaf */
	rootpage = BufferGetPage(rootbuf);
	PageInit(rootpage, BLCKSZ, sizeof(NXBtreePageOpaque));
	opaque = NXBtreePageGetOpaque(rootpage);
	opaque->nx_attno = attno;
	opaque->nx_next = InvalidBlockNumber;
	opaque->nx_lokey = MinNXTid;
	opaque->nx_hikey = MaxPlusOneNXTid;
	opaque->nx_level = 0;
	opaque->nx_flags = NXBT_ROOT;
	opaque->nx_page_id = NX_BTREE_PAGE_ID;

	PageSetLSN(rootpage, lsn);
	MarkBufferDirty(rootbuf);

	/* Update the metapage to point to it */
	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page		metapage = (Page) BufferGetPage(metabuf);
		NXMetaPage *metapg = (NXMetaPage *) PageGetContents(metapage);

		Assert(BufferGetBlockNumber(metabuf) == NX_META_BLK);
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
nxmeta_get_root_for_attribute(Relation rel, AttrNumber attno, bool readonly)
{
	Buffer		metabuf;
	NXMetaPage *metapg;
	BlockNumber rootblk;
	NXMetaCacheData *metacache;

	Assert(attno == NX_META_ATTRIBUTE_NUM || attno >= 1);

	metacache = nxmeta_get_cache(rel);

	if (RelationGetTargetBlock(rel) == 0 ||
		RelationGetTargetBlock(rel) == InvalidBlockNumber)
	{
		BlockNumber nblocks = RelationGetNumberOfBlocks(rel);

		if (nblocks != 0)
			metacache = nxmeta_populate_cache(rel);
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
				nxmeta_initmetapage(rel);
			UnlockRelationForExtension(rel, ExclusiveLock);
			metacache = nxmeta_populate_cache(rel);
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
			metacache = nxmeta_populate_cache(rel);
			if (attno >= metacache->cache_nattributes)
				return InvalidBlockNumber;
		}
		else
		{
			nxmeta_expand_metapage_for_new_attributes(rel);
			metacache = nxmeta_populate_cache(rel);
		}
	}

	rootblk = metacache->cache_attrs[attno].root;

	if (!readonly && rootblk == InvalidBlockNumber)
	{
		/* try to allocate one */
		Page		page;

		metabuf = ReadBuffer(rel, NX_META_BLK);

		LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(metabuf);
		metapg = (NXMetaPage *) PageGetContents(page);

		/*
		 * Re-check that the root is still invalid, now that we have the
		 * metapage locked.
		 */
		rootblk = metapg->tree_root_dir[attno].root;
		if (rootblk == InvalidBlockNumber)
		{
			Buffer		rootbuf;
			Page		rootpage;
			NXBtreePageOpaque *opaque;

			/*
			 * We hold the metapage lock while allocating a new page. Releasing
			 * and re-acquiring the lock around the I/O would allow more
			 * concurrency, but this path is only taken once per attribute tree
			 * (when the root is first created), so the contention is minimal.
			 */
			rootbuf = nxpage_getnewbuf(rel, metabuf);
			rootblk = BufferGetBlockNumber(rootbuf);

			START_CRIT_SECTION();

			metapg->tree_root_dir[attno].root = rootblk;

			/* initialize the page to look like a root leaf */
			rootpage = BufferGetPage(rootbuf);
			PageInit(rootpage, BLCKSZ, sizeof(NXBtreePageOpaque));
			opaque = NXBtreePageGetOpaque(rootpage);
			opaque->nx_attno = attno;
			opaque->nx_next = InvalidBlockNumber;
			opaque->nx_lokey = MinNXTid;
			opaque->nx_hikey = MaxPlusOneNXTid;
			opaque->nx_level = 0;
			opaque->nx_flags = NXBT_ROOT;
			opaque->nx_page_id = NX_BTREE_PAGE_ID;

			MarkBufferDirty(rootbuf);
			MarkBufferDirty(metabuf);

			if (RelationNeedsWAL(rel))
				nxmeta_wal_log_new_att_root(metabuf, rootbuf, attno);

			END_CRIT_SECTION();

			UnlockReleaseBuffer(rootbuf);
		}
		UnlockReleaseBuffer(metabuf);

		metacache->cache_attrs[attno].root = rootblk;
	}

	return rootblk;
}
