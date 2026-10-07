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
 * Leaf entries are the ones insert would form.  Where insert coalesces equal
 * keys (bark_allequalimage, in a non-unique index), bark_load writes each run
 * of equal keys as LIST and POSTING entries, as nbtsort.c's _bt_load
 * deduplicates into posting lists; otherwise every row is a SINGLE (one heap
 * TID in t_tid, no alt-TID bit).  Internal downlinks are PIVOT tuples (see
 * bark.h).
 *
 * The write follows nbtsort.c's model: block numbers are assigned in a single
 * increasing sequence as pages are handed to the writer, so a page's block is
 * known at the moment it is flushed, which is exactly when its downlink is
 * added to the parent level.  A per-level BarkPageState stack is carried up as
 * the tree grows.
 *
 * As in nbtsort.c, pages are not packed full: then the first inserts after
 * the build would split nearly every page they touch, and the splits would
 * cascade up the tree.  Leaf pages are packed to the index's fillfactor
 * reloption (default 90%) and internal pages to BARK_NONLEAF_FILLFACTOR.
 *
 * The sort uses tuplesort.c with a btree-family ordering comparator.  BARK's
 * operator classes live in btree's operator families (ambtreeopfamilies), so
 * tuplesort_begin_index_btree() produces exactly the total order
 * bark_compare_itups() does: it reads only the index's ordering support proc,
 * collation, and ASC/DESC + NULLS options via the shared family, never a btree
 * meta page (that path of _bt_mkscankey, taken when it is handed a tuple, is
 * not reached here because we pass NULL).  Switching to tuplesort also lets a
 * build spill to disk, and makes a parallel build possible: multiple workers
 * each scan a slice of the heap into a shared parallel tuplesort, the leader
 * merges the sorted runs, then the single existing bottom-up loader writes the
 * tree -- only the leader ever writes pages.
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
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bulk_write.h"
#include "storage/condition_variable.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/rel.h"
#include "utils/tuplesort.h"
#include "utils/wait_event.h"

/* Magic numbers for parallel state sharing via the DSM table of contents. */
#define PARALLEL_KEY_BARK_SHARED		UINT64CONST(0xB42C000000000001)
#define PARALLEL_KEY_TUPLESORT			UINT64CONST(0xB42C000000000002)
#define PARALLEL_KEY_QUERY_TEXT			UINT64CONST(0xB42C000000000003)
#define PARALLEL_KEY_WAL_USAGE			UINT64CONST(0xB42C000000000004)
#define PARALLEL_KEY_BUFFER_USAGE		UINT64CONST(0xB42C000000000005)

/*
 * One page under construction, per tree level.  A full page is flushed (its
 * block number is assigned from the writer's running counter) and a new page
 * started; the flushed page's downlink is added to the parent.
 */
typedef struct BarkPageState
{
	BulkWriteBuffer buf;		/* page being filled */
	BlockNumber blkno;			/* block number assigned to this page */
	OffsetNumber nextoff;		/* next free item offset */
	uint32		level;			/* tree level (0 = leaf) */
	Size		full;			/* page is "full" below this much free space */
	IndexTuple	lowkey;			/* this page's downlink, without its block */
	BlockNumber prevblk;		/* previous page at this level (for right-link) */
	BulkWriteBuffer prevbuf;	/* previous page's buffer, awaiting its next-link */
	struct BarkPageState *parent;	/* next level up; created on demand */
} BarkPageState;

/*
 * The largest leaf entry CREATE INDEX forms from a run of equal keys.  As
 * nbtsort.c's _bt_load limits its posting lists (maxpostingsize), this is a
 * tenth of a page less one line pointer: the space a leaf packed to the
 * default fillfactor of 90 leaves free.  An entry of BarkMaxItemSize, a third
 * of a page, would leave room for only two of them beside the high key.
 * Retail insert may still grow a built entry up to BarkMaxItemSize.
 */
#define BARK_BUILD_MAX_ENTRY_SIZE \
	(MAXALIGN_DOWN(BLCKSZ * 10 / 100) - sizeof(ItemIdData))

StaticAssertDecl(BARK_BUILD_MAX_ENTRY_SIZE <= BarkMaxItemSize,
				 "BARK build entry limit exceeds the item ceiling");

/*
 * More heap TIDs than one built entry can hold.  A LIST is far smaller, and a
 * POSTING entry's removal bound (sbm_removal_bound) charges a stored 64-bit
 * vector for every vector that holds a member, so each member costs at least
 * one bit of the entry.
 */
#define BARK_BUILD_MAX_ENTRY_TIDS	(8 * (int) BARK_BUILD_MAX_ENTRY_SIZE)

/*
 * A run of equal keys bark_load is gathering: the run's first tuple, which
 * supplies the key of every entry formed from the run, and the heap TIDs of
 * the run's tuples not yet written, ascending.  The array holds two entries'
 * worth; when it fills, the entries that cannot get longer are written out,
 * so a key with any number of rows needs a bounded amount of memory.
 */
typedef struct BarkBuildRun
{
	IndexTuple	key;			/* first tuple of the run, or NULL */
	ItemPointer tids;			/* pending heap TIDs, ascending */
	int			ntids;
} BarkBuildRun;

#define BARK_BUILD_RUN_TIDS		(2 * BARK_BUILD_MAX_ENTRY_TIDS)

/* The whole build: the writer, the comparison state, and the level stack. */
typedef struct BarkBuildState
{
	Relation	heap;
	Relation	index;
	BarkKeyInfo *keyinfo;
	BlockNumber nblocks;		/* next block number to assign (after meta) */
	int			nkeyatts;		/* number of key attributes */
	bool		isunique;		/* enforce uniqueness during load */
	bool		allequalimage;	/* bark_allequalimage(index) */
	bool		has_oversized;	/* saw a key too large to sort/load inline */
	IndexInfo  *indexInfo;		/* for the oversized second-pass insert */

	/* the sort this build feeds; set up by the caller */
	Tuplesortstate *sortstate;
	double		indtuples;		/* reported to the planner */

	/* parallel coordination, NULL for a serial build */
	struct BarkLeader *barkleader;
} BarkBuildState;

/*
 * Status for an index build performed in parallel, living in a DSM segment.
 * Immutable fields let a worker reconstruct the leader's build state; the
 * mutable fields (under mutex) accumulate the per-worker scan results the
 * leader needs once every participant is done.  Modeled on nbtsort.c's
 * BTShared, minus the uniqueness second-spool machinery BARK does not need
 * (BARK enforces uniqueness in the load phase, not the sort).
 */
typedef struct BarkShared
{
	/* Immutable: lets a worker rebuild the leader's spool. */
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isconcurrent;
	int			scantuplesortstates;
	int64		queryid;

	/*
	 * workersdonecv signals the leader as each worker finishes; the leader
	 * waits on it before touching the mutable state below.
	 */
	ConditionVariable workersdonecv;

	/* mutex protects the mutable fields that follow. */
	slock_t		mutex;

	int			nparticipantsdone;
	double		reltuples;
	double		indtuples;
	bool		brokenhotchain;
	bool		has_oversized;	/* any worker saw an oversized key */

	/*
	 * A ParallelTableScanDescData follows, past the alignment padding; it
	 * cannot be embedded because its implementation may need stronger
	 * alignment than this struct.
	 */
} BarkShared;

/* The parallel table scan descriptor stored right after a BarkShared. */
#define ParallelTableScanFromBarkShared(shared) \
	(ParallelTableScanDesc) ((char *) (shared) + BUFFERALIGN(sizeof(BarkShared)))

/* Status for the leader of a parallel index build. */
typedef struct BarkLeader
{
	ParallelContext *pcxt;
	int			nparticipanttuplesorts; /* workers launched + leader */
	BarkShared *barkshared;
	Sharedsort *sharedsort;
	Snapshot	snapshot;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
} BarkLeader;

/* True when any key attribute of `itup` is NULL (NULLs are distinct in SQL). */
static bool
bark_itup_has_null_key(Relation index, int nkeyatts, IndexTuple itup)
{
	TupleDesc	tupdesc = RelationGetDescr(index);

	for (int i = 0; i < nkeyatts; i++)
	{
		bool		isnull;

		(void) index_getattr(itup, i + 1, tupdesc, &isnull);
		if (isnull)
			return true;
	}
	return false;
}

/* table_index_build_scan callback: spool one index tuple into the sort. */
static void
bark_build_callback(Relation index, ItemPointer tid, Datum *values,
					bool *isnull, bool tupleIsAlive, void *state)
{
	BarkBuildState *bs = (BarkBuildState *) state;
	IndexTuple	full;
	Size		fulllen;

	if (!tupleIsAlive)
		return;

	/*
	 * Classify the row by its formed size.  An oversized key cannot go through
	 * the sort (tuplesort re-forms via index_form_tuple, which caps a tuple at
	 * 8191 bytes), nor can the bottom-up loader place it inline; such rows are
	 * recorded and inserted after the tree is loaded, via the normal
	 * overflow-aware insert path (bark_build_oversized_pass).
	 *
	 * This forms every row once here to measure its size, then tuplesort forms
	 * the non-oversized ones again; a size-only estimator mirroring
	 * bark_form_full_tuple's TOAST decisions would avoid the second form.
	 */
	full = bark_form_full_tuple(RelationGetDescr(index), values, isnull,
							   &fulllen);
	if (bark_len_is_oversized(fulllen))
	{
		bs->has_oversized = true;
		bs->indtuples += 1;
		pfree(full);
		return;
	}
	pfree(full);

	/*
	 * Spool a SINGLE-shape entry: the formed index tuple with the heap TID in
	 * t_tid.  tuplesort_putindextuplevalues forms the tuple and stamps t_tid
	 * exactly as the serial build did by hand.
	 */
	tuplesort_putindextuplevalues(bs->sortstate, index, tid, values, isnull);
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
	if (level > 0)
		st->full = BLCKSZ * (100 - BARK_NONLEAF_FILLFACTOR) / 100;
	else
		st->full = BarkGetTargetPageFreeSpace(bs->index);
	st->prevblk = BARK_P_NONE;
	st->prevbuf = NULL;

	/*
	 * The first page of a level is reached through a minus-infinity downlink
	 * (no key attributes), as nbtree's and bark_new_root's are.  Later pages
	 * get the previous page's high key (bark_flush_page).
	 */
	st->lowkey = palloc0_object(IndexTupleData);
	st->lowkey->t_info = sizeof(IndexTupleData);
	BarkPivotSetNAtts(st->lowkey, 0);

	opaque = BarkPageGetOpaque((Page) st->buf);
	opaque->bark_prev = BARK_P_NONE;
	opaque->bark_next = BARK_P_NONE;
	opaque->bark_level = level;
	opaque->bark_cycleid = 0;
	opaque->bark_flags = (level == 0) ? BARK_LEAF : 0;
	opaque->bark_page_id = BARK_PAGE_ID;

	return st;
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

	/* The items move as stored (not through BarkPageGetItem): only their offsets change. */
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
 * The high key for st's current page when `firstright` starts the next page:
 * on a leaf, firstright truncated against the page's last item
 * (bark_truncate_pivot); on an internal page, whose items are pivots already,
 * a copy of firstright.
 */
static IndexTuple
bark_build_hikey(BarkBuildState *bs, BarkPageState *st, IndexTuple firstright)
{
	Page		page = (Page) st->buf;
	IndexTuple	hikey;

	if (st->level == 0)
	{
		BarkItemBuf ibuf;
		IndexTuple	lastleft = BarkPageGetItem(page, OffsetNumberPrev(st->nextoff),
											   &ibuf);

		Assert(BarkEntryGetShape(firstright) != BARK_SHAPE_OVERSIZED);
		hikey = bark_truncate_pivot(bs->index, bs->keyinfo, lastleft,
									firstright);
	}
	else
	{
		hikey = CopyIndexTuple(firstright);
		BarkPivotSetDownLink(hikey, BARK_P_NONE);
	}
	return hikey;
}

/*
 * Flush st's current page because `firstright` (the item that did not fit)
 * will start a new page: give this page its high key, write the page, chain
 * the right-link, add the page's downlink to the parent, and start a fresh
 * page in st for firstright and what follows.
 *
 * The high key comes from bark_build_hikey.  A copy of it is kept as the
 * new page's downlink, so the parent separates the two pages exactly as the
 * high key does.  This is how nbtsort.c's _bt_buildadd pairs them.
 */
static void
bark_flush_page(BarkBuildState *bs, BulkWriteState *bulk, BarkPageState *st,
				IndexTuple firstright)
{
	Page		page = (Page) st->buf;
	IndexTuple	hikey;
	IndexTuple	moved = NULL;
	BlockNumber flushedblk = st->blkno;
	BulkWriteBuffer flushedbuf = st->buf;
	BarkPageState *parent = st->parent;
	BarkPageState *fresh;

	hikey = bark_build_hikey(bs, st, firstright);

	/*
	 * bark_buildadd reserves room for a high key the size of the item it
	 * adds, but the high key comes from the next item, which can be wider.
	 * When it does not fit, move this page's last item to the next page, as
	 * nbtsort.c's _bt_buildadd always does, and bound the page by that item
	 * instead: its high key is no larger than the item, so it fits in the
	 * space the item leaves.  A page holding a single item always has room,
	 * since an item is at most BarkMaxItemSize, a third of a page.
	 */
	if (PageGetFreeSpace(page) < MAXALIGN(IndexTupleSize(hikey)))
	{
		OffsetNumber lastoff = OffsetNumberPrev(st->nextoff);
		BarkItemBuf ibuf;

		Assert(lastoff > BARK_P_HIKEY);
		moved = CopyIndexTuple(BarkPageGetItem(page, lastoff, &ibuf));
		PageIndexTupleDelete(page, lastoff);
		st->nextoff = lastoff;
		pfree(hikey);
		hikey = bark_build_hikey(bs, st, moved);
		Assert(PageGetFreeSpace(page) >= MAXALIGN(IndexTupleSize(hikey)));
	}

	/* Rebuild the page as [high key, data...]; the high key bounds the page. */
	bark_prepend_hikey(page, hikey);

	/* Add the flushed page's downlink to the parent. */
	if (parent == NULL)
	{
		parent = bark_pagestate(bs, bulk, st->level + 1);
		st->parent = parent;
	}
	BarkPivotSetDownLink(st->lowkey, flushedblk);
	bark_buildadd(bs, bulk, parent, st->lowkey);
	pfree(st->lowkey);

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
	st->lowkey = hikey;
	st->prevblk = flushedblk;
	st->prevbuf = flushedbuf;
	st->parent = parent;
	pfree(fresh->lowkey);
	pfree(fresh);

	/* The item moved off the flushed page leads the new one. */
	if (moved != NULL)
	{
		if (PageAddItem((Page) st->buf, (char *) moved, IndexTupleSize(moved),
						st->nextoff, false, false) == InvalidOffsetNumber)
			elog(ERROR, "failed to add item to BARK page during build");
		st->nextoff = OffsetNumberNext(st->nextoff);
		pfree(moved);
	}
}

/*
 * Add itup to the page in st, flushing to a new page first if it does not
 * fit (passing itup as the first item of the next page).  Data items occupy
 * BARK_P_HIKEY and up during the build; at flush a high key is prepended
 * (shifting data to BARK_P_FIRSTKEY), and the rightmost page per level keeps
 * data at offset 1.
 */
static void
bark_buildadd(BarkBuildState *bs, BulkWriteState *bulk, BarkPageState *st,
			  IndexTuple itup)
{
	Size		itemsz = IndexTupleSize(itup);
	Size		hikeysz = itemsz;
	OffsetNumber off;

	/*
	 * Flush when the page already holds a data item and the new item plus a
	 * high key would not fit.  The high key is formed from the next item and
	 * is no larger than it; the new item stands in for it here, and for a
	 * LIST or POSTING item only its key is counted, since that is all a high
	 * key keeps (bark_make_pivot strips the body).  When the high key turns
	 * out wider, bark_flush_page moves the page's last item to the next page.
	 * Requiring room for the item and a high key keeps at least one item per
	 * page.
	 *
	 * Also flush, as nbtsort.c does, once the page's free space has dropped
	 * below the fillfactor target, provided it already holds two data items;
	 * the minimum keeps a low fillfactor with wide keys from producing pages
	 * of a single entry.
	 */
	if (BarkEntryGetShape(itup) == BARK_SHAPE_LIST ||
		BarkEntryGetShape(itup) == BARK_SHAPE_POSTING)
		hikeysz = BarkEntryGetBodyOffset(itup);

	if (st->nextoff > BARK_P_HIKEY &&
		(!bark_page_has_room(st->buf, itemsz + hikeysz) ||
		 (PageGetFreeSpace((Page) st->buf) < st->full &&
		  st->nextoff > BARK_P_FIRSTKEY)))
		bark_flush_page(bs, bulk, st, itup);

	off = st->nextoff;
	if (PageAddItem((Page) st->buf, (char *) itup, itemsz, off, false, false) ==
		InvalidOffsetNumber)
		elog(ERROR, "failed to add item to BARK page during build");
	st->nextoff = OffsetNumberNext(off);
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
			 * Not the root: this level's last page still needs its downlink
			 * added to the parent, exactly as a flush would do, so the
			 * parent's rightmost downlink exists.
			 */
			BarkPivotSetDownLink(st->lowkey, st->blkno);
			bark_buildadd(bs, bulk, parent, st->lowkey);
		}
		pfree(st->lowkey);

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
		opaque->bark_cycleid = 0;
		opaque->bark_flags = BARK_META;
		opaque->bark_page_id = BARK_PAGE_ID;
	}
	meta = BarkPageGetMeta((Page) metabuf);
	meta->bark_magic = BARK_MAGIC;
	meta->bark_version = BARK_VERSION;
	meta->bark_root = rootblk;
	meta->bark_level = rootlevel;
	meta->bark_allequalimage = bs->allequalimage;
	((PageHeader) metabuf)->pd_lower =
		((char *) meta + sizeof(BarkMetaPageData)) - (char *) metabuf;

	smgr_bulk_write(bulk, BARK_METAPAGE, metabuf, true);
}

/*
 * bark_form_posting's result for the first ntids locators, or NULL when that
 * is not a POSTING entry within BARK_BUILD_MAX_ENTRY_SIZE.
 */
static IndexTuple
bark_build_posting(TupleDesc tupdesc, IndexTuple key, ItemPointer tids,
				   int ntids)
{
	IndexTuple	entry = bark_form_posting(tupdesc, key, tids, ntids);

	if (entry != NULL && IndexTupleSize(entry) > BARK_BUILD_MAX_ENTRY_SIZE)
	{
		pfree(entry);
		entry = NULL;
	}
	return entry;
}

/*
 * Form one leaf entry of `key` from a prefix of the ascending locators
 * tids[0..ntids) and set *nused to the length of that prefix.  The entry is
 * the smaller of POSTING and LIST for the prefix, as bark_coalesce_list
 * chooses on insert, and the prefix is the longest one whose entry fits in
 * BARK_BUILD_MAX_ENTRY_SIZE.  One locator, or a key too wide for a LIST of
 * two, makes a SINGLE.
 *
 * Both shapes grow with the prefix: a LIST by one locator per member, a
 * POSTING by its removal bound, which never shrinks as members are added.  So
 * the LIST limit is computed directly and the encodings are compared there.
 * When POSTING wins, it is tried with every locator an entry can hold, which
 * covers the whole run for most keys, and only when that does not fit is the
 * POSTING limit found by bisection.
 */
static IndexTuple
bark_build_form_entry(BarkBuildState *bs, IndexTuple key, ItemPointer tids,
					  int ntids, int *nused)
{
	TupleDesc	tupdesc = RelationGetDescr(bs->index);
	Size		keysz = MAXALIGN(IndexTupleSize(key));
	int			nlist = 0;
	int			n;
	IndexTuple	entry;

	if (keysz < BARK_BUILD_MAX_ENTRY_SIZE)
		nlist = Min(BARK_LIST_MAX_COUNT,
					(int) ((BARK_BUILD_MAX_ENTRY_SIZE - keysz) /
						   sizeof(ItemPointerData)));

	if (ntids == 1 || nlist < 2)
	{
		entry = CopyIndexTuple(key);
		entry->t_tid = tids[0];
		*nused = 1;
		return entry;
	}

	n = Min(ntids, nlist);
	entry = bark_build_posting(tupdesc, key, tids, n);
	if (entry == NULL)
	{
		*nused = n;
		return bark_form_list(tupdesc, key, tids, n);
	}

	if (ntids > n)
	{
		int			lo = n;		/* fits */
		int			hi = Min(ntids, BARK_BUILD_MAX_ENTRY_TIDS);
		IndexTuple	probe = bark_build_posting(tupdesc, key, tids, hi);

		if (probe != NULL)
		{
			pfree(entry);
			*nused = hi;
			return probe;
		}
		hi--;
		while (lo < hi)
		{
			int			mid = lo + (hi - lo + 1) / 2;

			probe = bark_build_posting(tupdesc, key, tids, mid);
			if (probe != NULL)
			{
				pfree(entry);
				entry = probe;
				lo = mid;
			}
			else
				hi = mid - 1;
		}
		n = lo;
	}
	*nused = n;
	return entry;
}

/*
 * Write out the entries of the pending run.  At the end of the run (`final`)
 * every locator is written.  Otherwise only the entries whose extent no later
 * locator can change are: an entry is cut from the longest prefix that fits,
 * and no entry holds more than BARK_BUILD_MAX_ENTRY_TIDS, so a cut made while
 * more than that many locators remain is the same cut the whole run would
 * get.  The rest move to the front of the array.
 */
static void
bark_build_flush_run(BarkBuildState *bs, BulkWriteState *bulk,
					 BarkPageState *leaf, BarkBuildRun *run, bool final)
{
	int			keep = final ? 0 : BARK_BUILD_MAX_ENTRY_TIDS;
	int			done = 0;

	while (run->ntids - done > keep)
	{
		int			nused;
		IndexTuple	entry = bark_build_form_entry(bs, run->key,
												  run->tids + done,
												  run->ntids - done, &nused);

		bark_buildadd(bs, bulk, leaf, entry);
		pfree(entry);
		done += nused;
	}

	run->ntids -= done;
	memmove(run->tids, run->tids + done, run->ntids * sizeof(ItemPointerData));
}

/*
 * Load the sorted spool into the tree: pull index tuples from the finished
 * tuplesort in key order, enforce uniqueness for a unique index, write leaf
 * pages left to right, and build the upper levels and meta page.  Only the
 * leader ever calls this -- workers only feed the shared sort.
 *
 * Where insert would coalesce equal keys (bark_insert: a non-unique index
 * whose equal keys have equal images; oversized keys never reach this
 * point), each run of equal keys becomes LIST and POSTING entries, as
 * nbtsort.c's _bt_load deduplicates into posting lists.  The sort breaks
 * ties on the heap TID, so a run's locators arrive ascending, as both shapes
 * store them.
 */
static void
bark_load(BarkBuildState *bs, Tuplesortstate *sortstate)
{
	BulkWriteState *bulk = smgr_bulk_start_rel(bs->index, MAIN_FORKNUM);
	BarkPageState *leaf = bark_pagestate(bs, bulk, 0);
	IndexTuple	itup;
	IndexTuple	prev = NULL;
	bool		coalesce = bs->allequalimage && !bs->isunique;
	BarkBuildRun run = {0};

	if (coalesce)
		run.tids = palloc_array(ItemPointerData, BARK_BUILD_RUN_TIDS);

	while ((itup = tuplesort_getindextuple(sortstate, true)) != NULL)
	{
		if (coalesce)
		{
			if (run.key != NULL &&
				bark_compare_itups(bs->keyinfo, bs->index, itup, run.key) == 0)
			{
				if (run.ntids == BARK_BUILD_RUN_TIDS)
					bark_build_flush_run(bs, bulk, leaf, &run, false);
				run.tids[run.ntids++] = itup->t_tid;
				continue;
			}
			if (run.key != NULL)
			{
				bark_build_flush_run(bs, bulk, leaf, &run, true);
				pfree(run.key);
			}
			run.key = CopyIndexTuple(itup);
			run.tids[0] = itup->t_tid;
			run.ntids = 1;
			continue;
		}

		/*
		 * A unique index must reject duplicate keys at build time too.  The
		 * sort placed equal keys adjacently (with a heap-TID tiebreak), so
		 * comparing each tuple with its predecessor catches every duplicate.
		 * NULLs are distinct in SQL, so a key containing any NULL never
		 * conflicts (bark_itup_has_null_key).
		 */
		if (bs->isunique && prev != NULL &&
			!bark_itup_has_null_key(bs->index, bs->nkeyatts, itup) &&
			!bark_itup_has_null_key(bs->index, bs->nkeyatts, prev) &&
			bark_compare_itups(bs->keyinfo, bs->index, itup, prev) == 0)
		{
			Datum		values[INDEX_MAX_KEYS];
			bool		isnull[INDEX_MAX_KEYS];
			char	   *key_desc;

			index_deform_tuple(itup, RelationGetDescr(bs->index),
							   values, isnull);
			key_desc = BuildIndexValueDescription(bs->index, values, isnull);
			ereport(ERROR,
					(errcode(ERRCODE_UNIQUE_VIOLATION),
					 errmsg("could not create unique index \"%s\"",
							RelationGetRelationName(bs->index)),
					 key_desc ? errdetail("Key %s is duplicated.", key_desc) :
					 errdetail("Duplicate keys exist."),
					 errtableconstraint(bs->heap,
										 RelationGetRelationName(bs->index))));
		}

		bark_buildadd(bs, bulk, leaf, itup);

		/*
		 * tuplesort_getindextuple returns a tuple in sort-managed memory that
		 * the next call may overwrite; keep our own copy to compare against
		 * the next one.
		 */
		if (prev != NULL)
			pfree(prev);
		prev = bs->isunique ? CopyIndexTuple(itup) : NULL;
	}
	if (prev != NULL)
		pfree(prev);
	if (run.key != NULL)
	{
		bark_build_flush_run(bs, bulk, leaf, &run, true);
		pfree(run.key);
	}
	if (run.tids != NULL)
		pfree(run.tids);

	bark_finish(bs, bulk, leaf);
	smgr_bulk_finish(bulk);
}

/*
 * Second-pass callback: insert one oversized row into the just-loaded tree via
 * the normal overflow-aware insert path.  Non-oversized rows are already in the
 * tree from bark_load, so they are skipped here.
 */
static void
bark_oversized_pass_callback(Relation index, ItemPointer tid, Datum *values,
							 bool *isnull, bool tupleIsAlive, void *state)
{
	BarkBuildState *bs = (BarkBuildState *) state;
	IndexTuple	full;
	Size		fulllen;
	IndexUniqueCheck checkUnique;

	if (!tupleIsAlive)
		return;

	full = bark_form_full_tuple(RelationGetDescr(index), values, isnull,
							   &fulllen);
	if (!bark_len_is_oversized(fulllen))
	{
		pfree(full);
		return;				/* already loaded inline */
	}
	pfree(full);

	/*
	 * Insert via the normal path, which writes the overflow chain and places
	 * an OVERSIZED entry.  A unique index checks uniqueness here (the heap
	 * tuples are visible during build), matching how bark_load would have
	 * rejected an inline duplicate.
	 */
	checkUnique = bs->isunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO;
	bark_insert(index, values, isnull, tid, bs->heap, checkUnique, false,
				bs->indexInfo);
}

/*
 * Insert the oversized rows the first scan deferred (bs->has_oversized) into
 * the loaded tree.  A full second heap scan identifies them again.
 *
 * A full second heap scan identifies the oversized rows again rather than
 * remembering their TIDs from the first scan -- the simplest approach for both
 * serial and parallel builds (parallel workers cannot share a palloc'd TID
 * list), and oversized-key builds are rare.  The oversized rows could be
 * folded into the first scan if such builds become common.
 */
static void
bark_build_oversized_pass(BarkBuildState *bs)
{
	if (!bs->has_oversized)
		return;

	(void) table_index_build_scan(bs->heap, bs->index, bs->indexInfo, true,
								  true, bark_oversized_pass_callback, bs, NULL);
}

/* ---------------------------------------------------------------------------
 * Parallel build (modeled on nbtsort.c: _bt_begin_parallel et al.)
 *
 * Workers each scan a slice of the heap (via a shared ParallelTableScanDesc)
 * into a shared parallel tuplesort; the leader merges the sorted runs and
 * writes the whole tree with bark_load.  Only the leader writes pages, so the
 * crash-safe bulk-write path is unchanged -- parallelism only speeds the scan
 * and sort.
 * ---------------------------------------------------------------------------
 */

/* Size of the shared build state plus its trailing parallel scan descriptor. */
static Size
bark_parallel_estimate_shared(Relation heap, Snapshot snapshot)
{
	return add_size(BUFFERALIGN(sizeof(BarkShared)),
					table_parallelscan_estimate(heap, snapshot));
}

/*
 * A worker's (or the leader-as-worker's) share: scan its slice of the heap
 * into a partial tuplesort and perform the sort, then report its counts.
 */
static void
bark_parallel_scan_and_sort(Relation heap, Relation index,
							BarkShared *barkshared, Sharedsort *sharedsort,
							int sortmem)
{
	SortCoordinate coordinate;
	BarkBuildState bs;
	TableScanDesc scan;
	Tuplesortstate *sortstate;
	IndexInfo  *indexInfo;
	double		reltuples;

	coordinate = palloc0_object(SortCoordinateData);
	coordinate->isWorker = true;
	coordinate->nParticipants = -1;
	coordinate->sharedsort = sharedsort;

	/* Begin a "partial" tuplesort that contributes to the shared sort. */
	sortstate = tuplesort_begin_index_btree(heap, index, false, false,
											sortmem, coordinate,
											TUPLESORT_NONE);

	memset(&bs, 0, sizeof(bs));
	bs.heap = heap;
	bs.index = index;
	bs.keyinfo = bark_build_keyinfo(index);
	bs.keyinfo->heaprel = heap;
	bs.nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	bs.isunique = false;		/* leader enforces uniqueness during load */
	bs.sortstate = sortstate;

	indexInfo = BuildIndexInfo(index);
	indexInfo->ii_Concurrent = barkshared->isconcurrent;
	scan = table_beginscan_parallel(heap,
									ParallelTableScanFromBarkShared(barkshared),
									SO_NONE);
	reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
									   bark_build_callback, &bs, scan);

	tuplesort_performsort(sortstate);

	/* Report this participant's results back to the leader. */
	SpinLockAcquire(&barkshared->mutex);
	barkshared->nparticipantsdone++;
	barkshared->reltuples += reltuples;
	barkshared->indtuples += bs.indtuples;
	if (indexInfo->ii_BrokenHotChain)
		barkshared->brokenhotchain = true;
	if (bs.has_oversized)
		barkshared->has_oversized = true;
	SpinLockRelease(&barkshared->mutex);

	ConditionVariableSignal(&barkshared->workersdonecv);

	tuplesort_end(sortstate);
}

/*
 * Parallel worker entry point.  Registered in parallel.c's internal worker
 * table under the name "_bark_parallel_build_main" (looked up by
 * CreateParallelContext("postgres", ...)).
 */
void
_bark_parallel_build_main(dsm_segment *seg, shm_toc *toc)
{
	char	   *sharedquery;
	BarkShared *barkshared;
	Sharedsort *sharedsort;
	Relation	heapRel;
	Relation	indexRel;
	LOCKMODE	heapLockmode;
	LOCKMODE	indexLockmode;
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	int			sortmem;

	/* Set debug_query_string for this worker. */
	sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true);
	debug_query_string = sharedquery;
	pgstat_report_activity(STATE_RUNNING, debug_query_string);

	barkshared = shm_toc_lookup(toc, PARALLEL_KEY_BARK_SHARED, false);

	/* Lock modes must match those index.c took when it opened the relations. */
	if (!barkshared->isconcurrent)
	{
		heapLockmode = ShareLock;
		indexLockmode = AccessExclusiveLock;
	}
	else
	{
		heapLockmode = ShareUpdateExclusiveLock;
		indexLockmode = RowExclusiveLock;
	}

	pgstat_report_query_id(barkshared->queryid, false);

	heapRel = table_open(barkshared->heaprelid, heapLockmode);
	indexRel = index_open(barkshared->indexrelid, indexLockmode);

	sharedsort = shm_toc_lookup(toc, PARALLEL_KEY_TUPLESORT, false);
	tuplesort_attach_shared(sharedsort, seg);

	InstrStartParallelQuery();

	sortmem = maintenance_work_mem / barkshared->scantuplesortstates;
	bark_parallel_scan_and_sort(heapRel, indexRel, barkshared, sharedsort,
								sortmem);

	bufferusage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false);
	walusage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false);
	InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber],
						  &walusage[ParallelWorkerNumber]);

	index_close(indexRel, indexLockmode);
	table_close(heapRel, heapLockmode);
}

/* The leader joins the parallel scan as one more participant. */
static void
bark_leader_participate_as_worker(BarkBuildState *bs)
{
	BarkLeader *barkleader = bs->barkleader;
	int			sortmem;

	sortmem = maintenance_work_mem / barkleader->nparticipanttuplesorts;
	bark_parallel_scan_and_sort(bs->heap, bs->index, barkleader->barkshared,
								barkleader->sharedsort, sortmem);
}

/*
 * Set up the DSM, shared sort, and parallel heap scan, and launch workers.
 * On success sets bs->barkleader; if not even one worker could start, leaves
 * it NULL and the caller falls back to a serial build.
 */
static void
bark_begin_parallel(BarkBuildState *bs, bool isconcurrent, int request)
{
	ParallelContext *pcxt;
	int			scantuplesortstates;
	Snapshot	snapshot;
	Size		estbarkshared;
	Size		estsort;
	BarkShared *barkshared;
	Sharedsort *sharedsort;
	BarkLeader *barkleader = palloc0_object(BarkLeader);
	WalUsage   *walusage;
	BufferUsage *bufferusage;
	bool		leaderparticipates = true;
	int			querylen;

	EnterParallelMode();
	Assert(request > 0);
	pcxt = CreateParallelContext("postgres", "_bark_parallel_build_main",
								 request);

	scantuplesortstates = leaderparticipates ? request + 1 : request;

	/*
	 * A normal build uses SnapshotAny (it must index RECENTLY_DEAD tuples and
	 * do its own time-qual checks); a concurrent build takes a regular MVCC
	 * snapshot and indexes what is live according to it.
	 */
	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	/* Estimate space for the shared build state and the shared sort. */
	estbarkshared = bark_parallel_estimate_shared(bs->heap, snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, estbarkshared);
	estsort = tuplesort_estimate_shared(scantuplesortstates);
	shm_toc_estimate_chunk(&pcxt->estimator, estsort);
	shm_toc_estimate_keys(&pcxt->estimator, 2);

	/* Space for each worker's WAL and buffer usage. */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Space for the query text workers report. */
	if (debug_query_string)
	{
		querylen = strlen(debug_query_string);
		shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	else
		querylen = 0;

	InitializeParallelDSM(pcxt);

	/* If no DSM segment was available, fall back to a serial build. */
	if (pcxt->seg == NULL)
	{
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return;
	}

	/* Store and initialize the shared build state. */
	barkshared = (BarkShared *) shm_toc_allocate(pcxt->toc, estbarkshared);
	barkshared->heaprelid = RelationGetRelid(bs->heap);
	barkshared->indexrelid = RelationGetRelid(bs->index);
	barkshared->isconcurrent = isconcurrent;
	barkshared->scantuplesortstates = scantuplesortstates;
	barkshared->queryid = pgstat_get_my_query_id();
	ConditionVariableInit(&barkshared->workersdonecv);
	SpinLockInit(&barkshared->mutex);
	barkshared->nparticipantsdone = 0;
	barkshared->reltuples = 0.0;
	barkshared->indtuples = 0.0;
	barkshared->brokenhotchain = false;
	barkshared->has_oversized = false;
	table_parallelscan_initialize(bs->heap,
								  ParallelTableScanFromBarkShared(barkshared),
								  snapshot);

	/* Store and initialize the shared tuplesort state. */
	sharedsort = (Sharedsort *) shm_toc_allocate(pcxt->toc, estsort);
	tuplesort_initialize_shared(sharedsort, scantuplesortstates, pcxt->seg);

	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BARK_SHARED, barkshared);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_TUPLESORT, sharedsort);

	if (debug_query_string)
	{
		char	   *sharedquery;

		sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
		memcpy(sharedquery, debug_query_string, querylen + 1);
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, sharedquery);
	}

	walusage = shm_toc_allocate(pcxt->toc,
								mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_WAL_USAGE, walusage);
	bufferusage = shm_toc_allocate(pcxt->toc,
								   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage);

	LaunchParallelWorkers(pcxt);
	barkleader->pcxt = pcxt;
	barkleader->nparticipanttuplesorts = pcxt->nworkers_launched;
	if (leaderparticipates)
		barkleader->nparticipanttuplesorts++;
	barkleader->barkshared = barkshared;
	barkleader->sharedsort = sharedsort;
	barkleader->snapshot = snapshot;
	barkleader->walusage = walusage;
	barkleader->bufferusage = bufferusage;

	/* If no workers started, back out and build serially. */
	if (pcxt->nworkers_launched == 0)
	{
		WaitForParallelWorkersToFinish(pcxt);
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return;
	}

	bs->barkleader = barkleader;

	/* The leader participates as a worker too. */
	if (leaderparticipates)
		bark_leader_participate_as_worker(bs);

	WaitForParallelWorkersToAttach(pcxt);
}

/* Shut down workers, accumulate usage, and leave parallel mode. */
static void
bark_end_parallel(BarkLeader *barkleader)
{
	WaitForParallelWorkersToFinish(barkleader->pcxt);

	for (int i = 0; i < barkleader->pcxt->nworkers_launched; i++)
		InstrAccumParallelQuery(&barkleader->bufferusage[i],
								&barkleader->walusage[i]);

	if (IsMVCCSnapshot(barkleader->snapshot))
		UnregisterSnapshot(barkleader->snapshot);
	DestroyParallelContext(barkleader->pcxt);
	ExitParallelMode();
}

/* Wait in the leader for every participant to finish its scan and sort. */
static double
bark_parallel_heapscan(BarkBuildState *bs)
{
	BarkShared *barkshared = bs->barkleader->barkshared;
	int			nparticipanttuplesorts;
	double		reltuples;

	nparticipanttuplesorts = bs->barkleader->nparticipanttuplesorts;
	for (;;)
	{
		SpinLockAcquire(&barkshared->mutex);
		if (barkshared->nparticipantsdone == nparticipanttuplesorts)
		{
			bs->indtuples = barkshared->indtuples;
			bs->has_oversized = barkshared->has_oversized;
			reltuples = barkshared->reltuples;
			SpinLockRelease(&barkshared->mutex);
			break;
		}
		SpinLockRelease(&barkshared->mutex);

		ConditionVariableSleep(&barkshared->workersdonecv,
							   WAIT_EVENT_PARALLEL_CREATE_INDEX_SCAN);
	}
	ConditionVariableCancelSleep();

	return reltuples;
}

/*
 * ambuild: build a BARK index over the heap.
 *
 * When the planner granted parallel workers (indexInfo->ii_ParallelWorkers,
 * set by index.c only when amcanbuildparallel is true), run a parallel build:
 * workers and the leader each scan a slice of the heap into a shared parallel
 * tuplesort, the leader merges the runs, then loads the tree.  Otherwise do a
 * serial scan into a private tuplesort and load the same way.
 */
IndexBuildResult *
bark_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	BarkBuildState bs;
	Tuplesortstate *leadersort;
	SortCoordinate coordinate = NULL;
	IndexBuildResult *result;
	double		reltuples;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	memset(&bs, 0, sizeof(bs));
	bs.heap = heap;
	bs.index = index;
	bs.keyinfo = bark_build_keyinfo(index);
	bs.keyinfo->heaprel = heap;
	bs.nkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
	bs.nblocks = 1;				/* block 0 is reserved for the meta page */
	bs.isunique = indexInfo->ii_Unique;
	bs.allequalimage = bark_allequalimage(index);
	bs.indexInfo = indexInfo;	/* for the oversized second-pass insert */

	/* Launch parallel workers when the planner asked for them. */
	if (indexInfo->ii_ParallelWorkers > 0)
		bark_begin_parallel(&bs, indexInfo->ii_Concurrent,
							indexInfo->ii_ParallelWorkers);

	/* Leader-side coordination state, set only when workers launched. */
	if (bs.barkleader)
	{
		coordinate = palloc0_object(SortCoordinateData);
		coordinate->isWorker = false;
		coordinate->nParticipants =
			bs.barkleader->nparticipanttuplesorts;
		coordinate->sharedsort = bs.barkleader->sharedsort;
	}

	/*
	 * Begin the leader (or serial) tuplesort.  enforceUnique is false: BARK
	 * checks for duplicates in the load phase, where it can emit a precise
	 * error with the offending key, rather than during the sort.
	 */
	leadersort = tuplesort_begin_index_btree(heap, index, false, false,
											 maintenance_work_mem, coordinate,
											 TUPLESORT_NONE);
	bs.sortstate = leadersort;

	/* Fill the sort: serial scan, or wait for the parallel participants. */
	if (!bs.barkleader)
		reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
										   bark_build_callback, &bs, NULL);
	else
		reltuples = bark_parallel_heapscan(&bs);

	tuplesort_performsort(leadersort);

	/* Merge the sorted runs into the tree (leader only writes pages). */
	bark_load(&bs, leadersort);
	tuplesort_end(leadersort);

	if (bs.barkleader)
		bark_end_parallel(bs.barkleader);

	/*
	 * Insert any oversized keys the sort/load path could not place inline, now
	 * that the tree exists.  Done after end_parallel so only the leader (which
	 * has the full IndexInfo and the heap open) performs it, via the normal
	 * overflow-aware insert path.
	 */
	bark_build_oversized_pass(&bs);

	result = palloc_object(IndexBuildResult);
	result->heap_tuples = reltuples;
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
	opaque->bark_cycleid = 0;
	opaque->bark_flags = BARK_META;
	opaque->bark_page_id = BARK_PAGE_ID;

	meta = BarkPageGetMeta((Page) metabuf);
	meta->bark_magic = BARK_MAGIC;
	meta->bark_version = BARK_VERSION;
	meta->bark_root = BARK_P_NONE;
	meta->bark_level = 0;
	meta->bark_allequalimage = bark_allequalimage(index);
	((PageHeader) metabuf)->pd_lower =
		((char *) meta + sizeof(BarkMetaPageData)) - (char *) metabuf;

	smgr_bulk_write(bulk, BARK_METAPAGE, metabuf, true);
	smgr_bulk_finish(bulk);
}
