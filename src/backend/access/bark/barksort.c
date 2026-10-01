/*-------------------------------------------------------------------------
 *
 * barksort.c
 *	  Index build (bulk load) for the BARK index access method.
 *
 * bark_build() scans the heap, forms a SINGLE-shape index tuple for each live
 * row, sorts them by key, and writes a complete BARK tree bottom-up: it fills
 * leaf pages left to right (linking right-siblings and recording each page's
 * high key), then builds internal levels from the per-page downlinks until a
 * single root remains, and finally writes the meta page pointing at that root.
 * The pages are written through smgr_bulk_write, which WAL-logs them at build
 * finish, so a BARK index built this way is crash-safe and replicatable
 * without any BARK-specific WAL record.
 *
 * This commit implements build only.  Every leaf entry is SINGLE (one heap
 * TID in t_tid, no alt-TID bit); internal downlinks are PIVOT tuples (see
 * bark.h).  Insert into an existing index, and scanning, arrive in later
 * commits; until then the cost estimator is prohibitive so the planner never
 * chooses a BARK index.
 *
 * The write follows nbtsort.c's model: block numbers are assigned in a single
 * increasing sequence as pages are handed to the writer, so a page's block is
 * known at the moment it is flushed, which is exactly when its downlink is
 * added to the parent level.  A per-level BarkPageState stack is carried up as
 * the tree grows.
 *
 * ponytail: the sort is an in-memory qsort of formed index tuples.  A build
 * larger than memory needs tuplesort (disk spill); that is a scale concern,
 * not a correctness one, and is deferred.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/bark/barksort.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bark.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "storage/bulk_write.h"
#include "utils/rel.h"

/*
 * One page under construction, per tree level.  A full page is flushed (its
 * block number is assigned from the writer's running counter) and a new page
 * started; the flushed page's low key is promoted to the parent as a downlink.
 */
typedef struct BarkPageState
{
	BulkWriteBuffer buf;		/* page being filled */
	BlockNumber blkno;			/* block number assigned to this page */
	OffsetNumber nextoff;		/* next free item offset */
	uint32		level;			/* tree level (0 = leaf) */
	IndexTuple	lowkey;			/* first key on this page (palloc'd copy) */
	BlockNumber prevblk;		/* previous page at this level (for right-link) */
	BulkWriteBuffer prevbuf;	/* previous page's buffer, awaiting its next-link */
	struct BarkPageState *parent;	/* next level up; created on demand */
} BarkPageState;

/* The whole build: the writer, the comparison state, and the level stack. */
typedef struct BarkBuildState
{
	Relation	heap;
	Relation	index;
	BarkKeyInfo *keyinfo;
	BlockNumber nblocks;		/* next block number to assign (after meta) */
	int			nkeyatts;		/* number of key attributes */

	/* collected, then sorted, index tuples */
	IndexTuple *tuples;
	int64		ntuples;
	int64		maxtuples;
	double		indtuples;		/* reported to the planner */
} BarkBuildState;

typedef struct BarkSortArg
{
	BarkKeyInfo *keyinfo;
	Relation	index;
} BarkSortArg;


static int
bark_sort_cmp(const void *a, const void *b, void *arg)
{
	BarkSortArg *sa = (BarkSortArg *) arg;

	return bark_compare_itups(sa->keyinfo, sa->index,
							  *(IndexTuple *) a, *(IndexTuple *) b);
}

/* table_index_build_scan callback: form and collect one index tuple. */
static void
bark_build_callback(Relation index, ItemPointer tid, Datum *values,
					bool *isnull, bool tupleIsAlive, void *state)
{
	BarkBuildState *bs = (BarkBuildState *) state;
	IndexTuple	itup;

	if (!tupleIsAlive)
		return;

	itup = index_form_tuple(RelationGetDescr(index), values, isnull);
	itup->t_tid = *tid;			/* SINGLE shape: heap TID locator in t_tid */

	if (bs->ntuples >= bs->maxtuples)
	{
		bs->maxtuples = bs->maxtuples ? bs->maxtuples * 2 : 1024;
		if (bs->tuples == NULL)
			bs->tuples = (IndexTuple *)
				palloc(bs->maxtuples * sizeof(IndexTuple));
		else
			bs->tuples = (IndexTuple *)
				repalloc(bs->tuples, bs->maxtuples * sizeof(IndexTuple));
	}
	bs->tuples[bs->ntuples++] = itup;
	bs->indtuples += 1;
}

/* Allocate and initialize a fresh page at the given level. */
static BarkPageState *
bark_pagestate(BarkBuildState *bs, BulkWriteState *bulk, uint32 level)
{
	BarkPageState *st = palloc0_object(BarkPageState);
	BarkPageOpaque opaque;

	st->buf = smgr_bulk_get_buf(bulk);
	PageInit((Page) st->buf, BLCKSZ, sizeof(BarkPageOpaqueData));
	st->blkno = bs->nblocks++;
	/*
	 * Data items are laid down contiguously from BARK_P_HIKEY (offset 1)
	 * while the page is being filled.  When the page gains a right sibling,
	 * bark_flush_page rebuilds it as [high key, data...] so the high key
	 * occupies offset 1 and data starts at BARK_P_FIRSTKEY -- the Lehman &
	 * Yao layout a non-rightmost page must have.  The rightmost page per
	 * level keeps data at offset 1 (BarkPageFirstDataKey) and needs no
	 * rebuild.
	 */
	st->nextoff = BARK_P_HIKEY;
	st->level = level;
	st->prevblk = BARK_P_NONE;
	st->prevbuf = NULL;

	opaque = BarkPageGetOpaque((Page) st->buf);
	opaque->bark_prev = BARK_P_NONE;
	opaque->bark_next = BARK_P_NONE;
	opaque->bark_level = level;
	opaque->bark_flags = (level == 0) ? BARK_LEAF : 0;
	opaque->bark_page_id = BARK_PAGE_ID;

	return st;
}

/* Pivot (downlink) tuple for a finished page: its low key + child block. */
static IndexTuple
bark_form_downlink(IndexTuple lowkey, BlockNumber child, int nkeyatts)
{
	IndexTuple	pivot = CopyIndexTuple(lowkey);

	BarkPivotSetNAtts(pivot, (uint16) nkeyatts);
	BarkPivotSetDownLink(pivot, child);
	return pivot;
}

/*
 * High-key tuple for a page that has gained a right sibling: a pivot copy of
 * the right sibling's first key, serving as the (inclusive upper) bound on
 * the keys the page may hold.  Suffix truncation of the high key is deferred
 * to a later commit (A03); for now the whole key is kept, with no heap TID in
 * t_tid (it carries the PIVOT natts/status instead).
 */
static IndexTuple
bark_form_hikey(IndexTuple firstright, int nkeyatts)
{
	IndexTuple	hikey = CopyIndexTuple(firstright);

	BarkPivotSetNAtts(hikey, (uint16) nkeyatts);
	/* A high key has no downlink; leave the block number as the sentinel. */
	BarkPivotSetDownLink(hikey, BARK_P_NONE);
	return hikey;
}

/* Does an item of the given size fit on this page? */
static bool
bark_page_has_room(BulkWriteBuffer buf, Size itemsz)
{
	return PageGetFreeSpace((Page) buf) >= MAXALIGN(itemsz);
}

static void bark_buildadd(BarkBuildState *bs, BulkWriteState *bulk,
						  BarkPageState *st, IndexTuple itup);

/*
 * Rebuild `page` as [high key, data...]: the high key goes to BARK_P_HIKEY
 * and the existing data items (currently at offsets 1..maxoff) move up to
 * BARK_P_FIRSTKEY and beyond.  Used at flush, when a page gains a right
 * sibling and so needs a high key.  `page` must have room for one more item;
 * bark_buildadd guarantees that by reserving a high key's worth of space.
 */
static void
bark_prepend_hikey(Page page, IndexTuple hikey)
{
	OffsetNumber maxoff = PageGetMaxOffsetNumber(page);
	int			n = maxoff;		/* data items currently at offsets 1..maxoff */
	IndexTuple *copies = (IndexTuple *) palloc(n * sizeof(IndexTuple));
	Size	   *sizes = (Size *) palloc(n * sizeof(Size));
	BarkPageOpaqueData saved = *BarkPageGetOpaque(page);

	for (int i = 0; i < n; i++)
	{
		ItemId		iid = PageGetItemId(page, BARK_P_HIKEY + i);

		copies[i] = CopyIndexTuple((IndexTuple) PageGetItem(page, iid));
		sizes[i] = ItemIdGetLength(iid);
	}

	PageInit(page, BLCKSZ, sizeof(BarkPageOpaqueData));
	*BarkPageGetOpaque(page) = saved;

	if (PageAddItem(page, (char *) hikey, IndexTupleSize(hikey),
					BARK_P_HIKEY, false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add high key to BARK page during build");
	for (int i = 0; i < n; i++)
	{
		if (PageAddItem(page, (char *) copies[i], sizes[i],
						BARK_P_FIRSTKEY + i, false, false) ==
			InvalidOffsetNumber)
			elog(ERROR, "failed to replace item on BARK page during build");
		pfree(copies[i]);
	}
	pfree(copies);
	pfree(sizes);
}

/*
 * Flush st's current page because `firstright` (the item that did not fit)
 * will start a new page: set this page's high key to firstright, write the
 * page, chain the right-link, promote the page's low key to the parent as a
 * downlink, and start a fresh page in st for firstright and what follows.
 */
static void
bark_flush_page(BarkBuildState *bs, BulkWriteState *bulk, BarkPageState *st,
				IndexTuple firstright)
{
	IndexTuple	downlink;
	IndexTuple	hikey;
	BlockNumber flushedblk = st->blkno;
	BulkWriteBuffer flushedbuf = st->buf;
	IndexTuple	flushedlow = st->lowkey;
	BarkPageState *parent = st->parent;
	BarkPageState *fresh;

	/* Rebuild the page as [high key, data...]; the high key bounds the page. */
	hikey = bark_form_hikey(firstright, bs->nkeyatts);
	bark_prepend_hikey((Page) flushedbuf, hikey);
	pfree(hikey);

	/* Promote the flushed page's low key as a downlink into the parent. */
	if (parent == NULL)
	{
		parent = bark_pagestate(bs, bulk, st->level + 1);
		st->parent = parent;
	}
	downlink = bark_form_downlink(flushedlow, flushedblk, bs->nkeyatts);
	bark_buildadd(bs, bulk, parent, downlink);
	pfree(downlink);

	/* Start a fresh page at this level and chain siblings both ways. */
	fresh = bark_pagestate(bs, bulk, st->level);
	BarkPageGetOpaque((Page) flushedbuf)->bark_next = fresh->blkno;
	BarkPageGetOpaque((Page) fresh->buf)->bark_prev = flushedblk;

	/* The flushed page is complete and immutable; write it now. */
	smgr_bulk_write(bulk, flushedblk, flushedbuf, true);

	/* Move the fresh page's state into st, preserving the parent link. */
	st->buf = fresh->buf;
	st->blkno = fresh->blkno;
	st->nextoff = fresh->nextoff;
	st->lowkey = NULL;
	st->prevblk = flushedblk;
	st->prevbuf = flushedbuf;
	st->parent = parent;
	pfree(fresh);
	if (flushedlow != NULL)
		pfree(flushedlow);
}

/*
 * Add itup to the page in st, flushing to a new page first if it does not
 * fit (passing itup as the finished page's high key).  Records the page's low
 * key the first time an item lands on it.  Data items occupy BARK_P_HIKEY and
 * up during the build; at flush a high key is prepended (shifting data to
 * BARK_P_FIRSTKEY), and the rightmost page per level keeps data at offset 1.
 */
static void
bark_buildadd(BarkBuildState *bs, BulkWriteState *bulk, BarkPageState *st,
			  IndexTuple itup)
{
	Size		itemsz = IndexTupleSize(itup);
	OffsetNumber off;

	/*
	 * Flush when the page already holds a data item and the new item plus a
	 * high key (worst case itup's own size) would not fit.  Requiring room
	 * for two items keeps at least one item per page and guarantees the high
	 * key prepended at flush fits.
	 */
	if (st->nextoff > BARK_P_HIKEY &&
		!bark_page_has_room(st->buf, itemsz + itemsz))
		bark_flush_page(bs, bulk, st, itup);

	off = st->nextoff;
	if (PageAddItem((Page) st->buf, (char *) itup, itemsz, off, false, false) ==
		InvalidOffsetNumber)
		elog(ERROR, "failed to add item to BARK page during build");
	st->nextoff = OffsetNumberNext(off);

	if (st->lowkey == NULL)
		st->lowkey = CopyIndexTuple(itup);
}

/*
 * Finish the build: write every level's final (rightmost) page, discovering
 * the root (the single page at the highest level), and write the meta page.
 */
static void
bark_finish(BarkBuildState *bs, BulkWriteState *bulk, BarkPageState *leaf)
{
	BarkPageState *st = leaf;
	BlockNumber rootblk = BARK_P_NONE;
	uint32		rootlevel = 0;
	BulkWriteBuffer metabuf;
	BarkMetaPageData *meta;

	/*
	 * Walk the level stack bottom to top, writing each level's last page.
	 * The highest level whose last page is the sole page at that level is the
	 * root.
	 */
	while (st != NULL)
	{
		BarkPageState *parent = st->parent;
		bool		is_root = (parent == NULL);

		if (is_root)
		{
			BarkPageGetOpaque((Page) st->buf)->bark_flags |= BARK_ROOT;
			rootblk = st->blkno;
			rootlevel = st->level;
		}
		else
		{
			/*
			 * Not the root: this level's last page still needs its low key
			 * promoted to the parent, exactly as a flush would do, so the
			 * parent's rightmost downlink exists.
			 */
			IndexTuple	downlink = bark_form_downlink(st->lowkey, st->blkno,
													  bs->nkeyatts);

			bark_buildadd(bs, bulk, parent, downlink);
			pfree(downlink);
		}

		/*
		 * This page is the rightmost at its level, so it has no high key;
		 * its data already starts at BARK_P_HIKEY (data is laid down from
		 * offset 1 and only shifted to offset 2 when a high key is prepended
		 * at flush, which never happens to a rightmost page).
		 */
		smgr_bulk_write(bulk, st->blkno, st->buf, true);
		st = parent;
	}

	/* Meta page (block 0): points at the root. */
	metabuf = smgr_bulk_get_buf(bulk);
	PageInit((Page) metabuf, BLCKSZ, sizeof(BarkPageOpaqueData));
	{
		BarkPageOpaque opaque = BarkPageGetOpaque((Page) metabuf);

		opaque->bark_prev = BARK_P_NONE;
		opaque->bark_next = BARK_P_NONE;
		opaque->bark_level = 0;
		opaque->bark_flags = BARK_META;
		opaque->bark_page_id = BARK_PAGE_ID;
	}
	meta = BarkPageGetMeta((Page) metabuf);
	meta->bark_magic = BARK_MAGIC;
	meta->bark_version = BARK_VERSION;
	meta->bark_root = rootblk;
	meta->bark_level = rootlevel;
	meta->bark_allequalimage = false;	/* dedup/equalimage is a later phase */
	((PageHeader) metabuf)->pd_lower =
		((char *) meta + sizeof(BarkMetaPageData)) - (char *) metabuf;

	smgr_bulk_write(bulk, BARK_METAPAGE, metabuf, true);
}

/*
 * ambuild: build a BARK index over the heap.
 */
IndexBuildResult *
bark_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	BarkBuildState bs;
	BulkWriteState *bulk;
	BarkPageState *leaf;
	IndexBuildResult *result;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	memset(&bs, 0, sizeof(bs));
	bs.heap = heap;
	bs.index = index;
	bs.keyinfo = bark_build_keyinfo(index);
	bs.nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	bs.nblocks = 1;				/* block 0 is reserved for the meta page */

	/* Scan the heap and collect index tuples. */
	table_index_build_scan(heap, index, indexInfo, true, true,
						   bark_build_callback, &bs, NULL);

	bulk = smgr_bulk_start_rel(index, MAIN_FORKNUM);

	/* Sort by key (in memory; see the ponytail note at the top). */
	if (bs.ntuples > 1)
	{
		BarkSortArg sa = {.keyinfo = bs.keyinfo,.index = index};

		qsort_arg(bs.tuples, bs.ntuples, sizeof(IndexTuple),
				  bark_sort_cmp, &sa);
	}

	/* Load the sorted tuples into leaf pages, growing the tree upward. */
	leaf = bark_pagestate(&bs, bulk, 0);
	for (int64 i = 0; i < bs.ntuples; i++)
		bark_buildadd(&bs, bulk, leaf, bs.tuples[i]);

	bark_finish(&bs, bulk, leaf);
	smgr_bulk_finish(bulk);

	result = palloc_object(IndexBuildResult);
	result->heap_tuples = bs.indtuples;
	result->index_tuples = bs.indtuples;
	return result;
}

/*
 * ambuildempty: write an empty BARK index to the init fork (for unlogged
 * relations).  An empty index is just a meta page with no root.
 */
void
bark_buildempty(Relation index)
{
	BulkWriteState *bulk;
	BulkWriteBuffer metabuf;
	BarkMetaPageData *meta;
	BarkPageOpaque opaque;

	bulk = smgr_bulk_start_rel(index, INIT_FORKNUM);
	metabuf = smgr_bulk_get_buf(bulk);
	PageInit((Page) metabuf, BLCKSZ, sizeof(BarkPageOpaqueData));

	opaque = BarkPageGetOpaque((Page) metabuf);
	opaque->bark_prev = BARK_P_NONE;
	opaque->bark_next = BARK_P_NONE;
	opaque->bark_level = 0;
	opaque->bark_flags = BARK_META;
	opaque->bark_page_id = BARK_PAGE_ID;

	meta = BarkPageGetMeta((Page) metabuf);
	meta->bark_magic = BARK_MAGIC;
	meta->bark_version = BARK_VERSION;
	meta->bark_root = BARK_P_NONE;
	meta->bark_level = 0;
	meta->bark_allequalimage = false;
	((PageHeader) metabuf)->pd_lower =
		((char *) meta + sizeof(BarkMetaPageData)) - (char *) metabuf;

	smgr_bulk_write(bulk, BARK_METAPAGE, metabuf, true);
	smgr_bulk_finish(bulk);
}
