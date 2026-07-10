/*-------------------------------------------------------------------------
 *
 * pg_fts_am.c
 *		The "bm25" index access method for pg_fts.
 *
 * A segmented inverted index over an ftsdoc column, answering the @@@ operator
 * (boolean / phrase / NEAR / prefix / fuzzy / regex) and the <=> ordering
 * operator (block-max WAND / MaxScore top-k), plus a fast fts_count() path.
 * It maintains the corpus statistics BM25 needs (document count N, sum of
 * document lengths, per-term document frequency) and scores index-only.
 *
 * On-disk layout (the Lucene/Tantivy-style segmented design):
 *
 *	 block 0            metapage: N, sum(doclen), a directory of segments, and
 *							the pending write buffer pointers
 *	 per segment        a term dictionary (+ sparse block index), FOR-packed
 *							128-doc posting blocks with per-block max-tf/min-|D|
 *							impacts, a trigram index, and a livedocs tombstone
 *							bitmap
 *	 pending pages      newly inserted docs stored verbatim, searched directly
 *							until folded into a new segment by a flush
 *
 * Inserts append to the pending buffer and are immediately visible; a flush
 * (fts_merge() or VACUUM cleanup) folds pending docs into a new segment, and a
 * size-tiered merge compacts segments (dropping tombstoned docs).  Deletes are
 * recorded as per-segment livedocs tombstones by ambulkdelete.  All page writes
 * go through GenericXLog, so the index is crash-safe and replicated without a
 * custom resource manager.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_am.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_fts.h"
#include "pg_fts_am.h"
#include "pg_fts_sm.h"			/* namespaced sparsemap (tombstones, trigrams) */
#include <math.h>
#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/parallel.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/visibilitymap.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "executor/instrument.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pathnodes.h"
#include "nodes/tidbitmap.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "catalog/storage.h"
#include "storage/condition_variable.h"
#include "storage/freespace.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/selfuncs.h"

PG_FUNCTION_INFO_V1(bm25handler);

/* ----- build: collect postings from the heap ----- */

typedef struct BuildTerm
{
	char	   *term;
	int			len;
	/* postings for this term */
	ItemPointerData *tids;
	uint32	   *tfs;
	uint32	   *doclens;
	int			nposts;
	int			maxposts;
	int			next;			/* next BuildTerm sharing the same hash key, or -1 */
} BuildTerm;

typedef struct BM25BuildState
{
	MemoryContext ctx;
	BuildTerm  *terms;			/* sorted-on-flush; kept in a simple array */
	int			nterms;
	int			maxterms;
	/* build-time term list: an unsorted array collected during the heap scan,
	 * sorted once before the dictionary is written */
	double		ndocs;
	double		sumdoclen;
} BM25BuildState;

static int
cmp_buildterm(const void *a, const void *b)
{
	const BuildTerm *ta = (const BuildTerm *) a;
	const BuildTerm *tb = (const BuildTerm *) b;
	int			min = Min(ta->len, tb->len);
	int			c = memcmp(ta->term, tb->term, min);

	if (c != 0)
		return c;
	return ta->len - tb->len;
}

/*
 * Find or create a BuildTerm for (term,len).  We use a dynahash keyed by a
 * fixed-size padded copy of the term to avoid an O(n^2) linear scan.  Terms
 * longer than the key buffer fall back to exact comparison via the stored
 * BuildTerm, which is correct though it may hash-collide slightly; term length
 * is bounded by MAXSTRLEN in practice.
 */
#include "utils/hsearch.h"

#define BM25_TERMKEYLEN 64

typedef struct TermKey
{
	char		key[BM25_TERMKEYLEN];
} TermKey;

typedef struct TermHashEntry
{
	TermKey		key;			/* must be first: dynahash key */
	int			termidx;		/* head of a chain of BuildTerms sharing this key */
} TermHashEntry;

static HTAB *build_ht;

/*
 * (Re)initialize the build hash table that maps a term key to its BuildTerm
 * index.  Created in bs->ctx so it is freed when that context is reset between
 * segment flushes during a large build.
 */
static void
bm25_build_ht_init(BM25BuildState *bs)
{
	HASHCTL		ctl;

	ctl.keysize = sizeof(TermKey);
	ctl.entrysize = sizeof(TermHashEntry);
	ctl.hcxt = bs->ctx;
	build_ht = hash_create("bm25 build terms", 1024, &ctl,
						   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
make_termkey(TermKey *k, const char *term, int len)
{
	memset(k, 0, sizeof(TermKey));
	memcpy(k->key, term, Min(len, BM25_TERMKEYLEN));
	/* fold length into the tail so different-length terms sharing a prefix do
	 * not collide on the key */
	if (len < BM25_TERMKEYLEN)
		k->key[len] = '\1';
}

static void
add_posting(BM25BuildState *bs, const char *term, int len,
			ItemPointer tid, uint32 tf, uint32 doclen)
{
	TermKey		key;
	TermHashEntry *entry;
	bool		found;
	BuildTerm  *bt = NULL;
	int			idx;

	make_termkey(&key, term, len);
	entry = (TermHashEntry *) hash_search(build_ht, &key, HASH_ENTER, &found);

	/*
	 * On a hash hit, walk the chain of BuildTerms sharing this key and pick the
	 * truly-equal one.  The key is a padded/length-folded prefix, so two DISTINCT
	 * terms >= BM25_TERMKEYLEN bytes sharing that prefix can land on the same
	 * key; chaining keeps them as separate BuildTerms instead of clobbering the
	 * entry (which previously fragmented a term's postings across unreachable
	 * dictionary entries).
	 */
	if (found)
	{
		for (idx = entry->termidx; idx >= 0; idx = bs->terms[idx].next)
		{
			BuildTerm  *cand = &bs->terms[idx];

			if (cand->len == len && memcmp(cand->term, term, len) == 0)
			{
				bt = cand;
				break;
			}
		}
	}

	if (bt == NULL)
	{
		if (bs->nterms >= bs->maxterms)
		{
			bs->maxterms = bs->maxterms ? bs->maxterms * 2 : 1024;
			if (bs->terms == NULL)
				bs->terms = (BuildTerm *) palloc(bs->maxterms * sizeof(BuildTerm));
			else
				bs->terms = (BuildTerm *) repalloc(bs->terms,
												   bs->maxterms * sizeof(BuildTerm));
		}
		bt = &bs->terms[bs->nterms];
		bt->term = (char *) palloc(len);
		memcpy(bt->term, term, len);
		bt->len = len;
		bt->maxposts = 4;
		bt->nposts = 0;
		bt->tids = (ItemPointerData *) palloc(bt->maxposts * sizeof(ItemPointerData));
		bt->tfs = (uint32 *) palloc(bt->maxposts * sizeof(uint32));
		bt->doclens = (uint32 *) palloc(bt->maxposts * sizeof(uint32));
		/* push onto the head of this key's chain (-1 = end of chain) */
		bt->next = found ? entry->termidx : -1;
		entry->termidx = bs->nterms;
		bs->nterms++;
	}

	if (bt->nposts >= bt->maxposts)
	{
		bt->maxposts *= 2;
		bt->tids = (ItemPointerData *) repalloc(bt->tids,
												bt->maxposts * sizeof(ItemPointerData));
		bt->tfs = (uint32 *) repalloc(bt->tfs, bt->maxposts * sizeof(uint32));
		bt->doclens = (uint32 *) repalloc(bt->doclens, bt->maxposts * sizeof(uint32));
	}
	bt->tids[bt->nposts] = *tid;
	bt->tfs[bt->nposts] = tf;
	bt->doclens[bt->nposts] = doclen;
	bt->nposts++;
}

/* forward decls: segment writers are defined later; the build flush uses them */
static void bm25_write_segment(Relation index, BM25BuildState *bs, BM25SegMeta *seg);
static void bm25_meta_add_segment(Relation index, const BM25SegMeta *seg);

/*
 * Memory budget for the in-memory build state before it is flushed to a
 * segment.  A very large CREATE INDEX would otherwise accumulate the whole
 * corpus's terms + postings in bs->ctx and exhaust memory; instead, once the
 * build context grows past this, we write the accumulated terms as a segment
 * and start fresh.  Derived from maintenance_work_mem (bounded so a small
 * setting still makes progress).  The later size-tiered merge compacts the
 * resulting segments.
 */
static Size
bm25_build_mem_budget(void)
{
	Size		budget = (Size) maintenance_work_mem * (Size) 1024;

	if (budget < (Size) 32 * 1024 * 1024)
		budget = (Size) 32 * 1024 * 1024;	/* floor: 32MB */
	return budget;
}

/*
 * Flush the current in-memory build state as one immutable segment and reset
 * the state (freeing bs->ctx) so the heap scan can continue within a bounded
 * memory footprint.  A document's terms are always fully accumulated before a
 * flush (we only flush between tuples), so no document is split across
 * segments and each segment's ndocs/sumdoclen are self-consistent.
 */
static void
bm25_build_flush_segment(Relation index, BM25BuildState *bs)
{
	BM25SegMeta seg;

	if (bs->nterms == 0)
		return;
	if (bs->nterms > 1)
		qsort(bs->terms, bs->nterms, sizeof(BuildTerm), cmp_buildterm);

	/*
	 * Serialize index-page writes across parallel-build participants.  During a
	 * parallel build several backends flush segments into the same index
	 * concurrently; pg_fts appends pages (bm25_new_buffer -> ReadBuffer(P_NEW)),
	 * and overlapping appenders race on relation extension ("unexpected data
	 * beyond EOF").  Holding the relation extension lock around the whole
	 * segment write makes each participant's page additions atomic w.r.t. the
	 * others.  The expensive part of the build -- heap scan + tsearch analysis
	 * -- runs fully parallel, and the segment write appends pages via
	 * bm25_new_buffer(), which now serializes only the P_NEW extension itself
	 * (per page) rather than the whole write -- so participants write
	 * concurrently.  In a serial build IsInParallelMode() is false and no
	 * extension lock is taken at all.
	 */
	bm25_write_segment(index, bs, &seg);
	bm25_meta_add_segment(index, &seg);

	/* reset: free everything in the build context and start a fresh segment */
	MemoryContextReset(bs->ctx);
	bs->terms = NULL;
	bs->nterms = 0;
	bs->maxterms = 0;
	bs->ndocs = 0;
	bs->sumdoclen = 0;
	bm25_build_ht_init(bs);
}

/* per-heap-tuple callback */
static void
bm25_build_callback(Relation index, ItemPointer tid, Datum *values,
					bool *isnull, bool tupleIsAlive, void *state)
{
	BM25BuildState *bs = (BM25BuildState *) state;
	FtsDoc		doc;
	FtsTermEntry *entries;
	uint32		i;
	MemoryContext old;

	if (isnull[0])
		return;

	/*
	 * Bound build memory: if the accumulated segment has grown past the budget,
	 * flush it as a segment and continue with a fresh build state.  Checked
	 * between tuples so a document's terms are never split across segments.
	 */
	if (bs->nterms > 0 &&
		MemoryContextMemAllocated(bs->ctx, false) >= bm25_build_mem_budget())
		bm25_build_flush_segment(index, bs);

	old = MemoryContextSwitchTo(bs->ctx);

	doc = (FtsDoc) PG_DETOAST_DATUM(values[0]);
	entries = FTS_DOC_ENTRIES(doc);

	for (i = 0; i < doc->nterms; i++)
		add_posting(bs, FTS_DOC_TERMTEXT(doc, &entries[i]), entries[i].len,
					tid, entries[i].tf, doc->doclen);

	bs->ndocs += 1.0;
	bs->sumdoclen += doc->doclen;

	MemoryContextSwitchTo(old);
}

/* ----- posting compression (delta + varint) ----- */

/*
 * Pack/unpack a heap TID into a monotonic 48-bit docid so that ascending TIDs
 * yield ascending docids and small gaps.  MaxHeapTuplesPerPage bounds the
 * offset, so block*factor+offset is monotonic in (block, offset).
 */
#define BM25_OFFSET_FACTOR ((uint64) MaxHeapTuplesPerPage)

static inline uint64
bm25_tid_to_docid(ItemPointer tid)
{
	return (uint64) ItemPointerGetBlockNumber(tid) * BM25_OFFSET_FACTOR +
		(uint64) ItemPointerGetOffsetNumber(tid);
}

static inline void
bm25_docid_to_tid(uint64 docid, ItemPointer tid)
{
	BlockNumber blk = (BlockNumber) (docid / BM25_OFFSET_FACTOR);
	OffsetNumber off = (OffsetNumber) (docid % BM25_OFFSET_FACTOR);

	ItemPointerSet(tid, blk, off);
}

/*
 * FOR (frame-of-reference) bit-packing of a block's three columns (docid gaps,
 * tfs, doclens) as Structure-of-Arrays.  Each column is [u8 bitwidth][packed
 * little-endian bitstream of `n` values].  Small, uniform values (the common
 * case for delta-coded docids and tf/doclen) pack to a few bits each instead of
 * a whole varint byte -- this both shrinks the index and cuts decode cost.
 * Width 0 means every value is 0 (no bits stored).
 */
static inline int
bm25_bitwidth(uint64 maxval)
{
	int			w = 0;

	while (maxval)
	{
		w++;
		maxval >>= 1;
	}
	return w;				/* 0 if maxval==0 */
}

/* pack n values at `width` bits each into buf starting at bit offset 0; returns
 * bytes written (including the leading width byte). */
static int
bm25_for_pack(const uint64 *vals, int n, unsigned char *buf)
{
	uint64		maxv = 0;
	int			width;
	int			i;
	int			bitpos;
	int			nbytes;

	for (i = 0; i < n; i++)
		if (vals[i] > maxv)
			maxv = vals[i];
	width = bm25_bitwidth(maxv);
	buf[0] = (unsigned char) width;
	if (width == 0)
		return 1;
	nbytes = 1 + (n * width + 7) / 8;
	memset(buf + 1, 0, nbytes - 1);
	bitpos = 0;
	for (i = 0; i < n; i++)
	{
		uint64		v = vals[i];
		int			b;

		for (b = 0; b < width; b++)
		{
			if (v & ((uint64) 1 << b))
			{
				int			abs = bitpos + b;

				buf[1 + (abs >> 3)] |= (unsigned char) (1 << (abs & 7));
			}
		}
		bitpos += width;
	}
	return nbytes;
}

/* unpack n values at the width stored in buf[0]; returns bytes consumed. */
static int
bm25_for_unpack(const unsigned char *buf, int n, uint64 *out)
{
	int			width = buf[0];
	int			i;
	int			bitpos;
	const unsigned char *bits;
	uint64		mask;

	if (width == 0)
	{
		for (i = 0; i < n; i++)
			out[i] = 0;
		return 1;
	}

	bits = buf + 1;
	mask = (width >= 64) ? ~UINT64CONST(0) : (((uint64) 1 << width) - 1);
	bitpos = 0;
	for (i = 0; i < n; i++)
	{
		int			byte = bitpos >> 3;
		int			shift = bitpos & 7;
		uint64		v;

		/*
		 * A value spans at most width+7 bits, so a single unaligned load of
		 * the covering bytes at `byte` plus shift/mask extracts it when
		 * shift+width <= 64.  For the rare wide case (shift+width > 64)
		 * assemble across a 9-byte window.  Replaces the per-bit inner loop
		 * that dominated posting decode.
		 */
		if (shift + width <= 64)
		{
			uint64		w = 0;
			int			nb = (shift + width + 7) >> 3;
			int			k;

			for (k = 0; k < nb; k++)
				w |= (uint64) bits[byte + k] << (k * 8);
			v = (w >> shift) & mask;
		}
		else
		{
			uint64		lo = 0,
						hi;
			int			k;

			for (k = 0; k < 8; k++)
				lo |= (uint64) bits[byte + k] << (k * 8);
			hi = bits[byte + 8];
			v = ((lo >> shift) | (hi << (64 - shift))) & mask;
		}
		out[i] = v;
		bitpos += width;
	}
	return 1 + (n * width + 7) / 8;
}

/* Byte length of a FOR column of n values, given its leading width byte -- used
 * to skip a column (e.g. tf/doclen) without unpacking any value. */
static inline int
bm25_for_bytelen(const unsigned char *buf, int n)
{
	int			width = buf[0];

	return (width == 0) ? 1 : 1 + (n * width + 7) / 8;
}

/* Random-access one value at index i from a FOR column (buf[0] = width).
 * O(width), no full-column unpack -- lets the WAND hot path decode a posting's
 * tf/doclen only when it is actually scored, skipping pruned blocks entirely. */
static inline uint64
bm25_for_get(const unsigned char *buf, int i)
{
	int			width = buf[0];
	int			bitpos;
	uint64		v = 0;
	int			b;

	if (width == 0)
		return 0;
	bitpos = i * width;
	for (b = 0; b < width; b++)
	{
		int			abs = bitpos + b;

		if (buf[1 + (abs >> 3)] & (1 << (abs & 7)))
			v |= (uint64) 1 << b;
	}
	return v;
}

/*
 * Decode exactly one term's postings from the shared posting chain: start at
 * (firstblk, firstoff) and decode consecutive blocks -- following nextblk
 * across pages -- until `df` postings have been read.  A term's blocks are
 * written contiguously, so its run is delimited purely by df.  Returns the
 * count (== df on a consistent index); *out (and *blockmax if non-NULL) are
 * palloc'd.  `off` on pages after the first is the contents start.
 */
static int
bm25_decode_term(Relation index, BlockNumber firstblk, uint32 firstoff,
				 uint32 df, BM25Posting **out, uint32 **blockmax)
{
	BM25Posting *posts;
	uint32	   *bmax = NULL;
	int			n = 0;
	BlockNumber blk = firstblk;
	uint32		off = firstoff;

	posts = (BM25Posting *) palloc(Max(df, 1u) * sizeof(BM25Posting));
	if (blockmax)
		bmax = (uint32 *) palloc(Max(df, 1u) * sizeof(uint32));

	while (blk != InvalidBlockNumber && n < (int) df)
	{
		Buffer		buf = ReadBuffer(index, blk);
		Page		page;
		char	   *p,
				   *pend;
		BlockNumber next;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		pend = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;
		p = (char *) page + off;
		while (p + sizeof(BM25BlockHdr) <= pend && n < (int) df)
		{
			BM25BlockHdr *bh = (BM25BlockHdr *) p;
			const unsigned char *stream = (const unsigned char *) (bh + 1);
			uint64		docid = ((uint64) bh->first_docid_hi << 32) | bh->first_docid_lo;
			uint64		gaps[BM25_BLOCK_SIZE];
			uint64		tfs[BM25_BLOCK_SIZE];
			uint64		dls[BM25_BLOCK_SIZE];
			int			cnt = (int) bh->count;
			int			pos = 0;
			int			i;

			if (cnt == 0)
				break;
			pos += bm25_for_unpack(stream + pos, cnt, gaps);
			pos += bm25_for_unpack(stream + pos, cnt, tfs);
			pos += bm25_for_unpack(stream + pos, cnt, dls);
			for (i = 0; i < cnt && n < (int) df; i++)
			{
				docid += gaps[i];
				bm25_docid_to_tid(docid, &posts[n].tid);
				posts[n].tf = (uint32) tfs[i];
				posts[n].doclen = (uint32) dls[i];
				if (bmax)
					bmax[n] = bh->max_tf;
				n++;
			}
			p = (char *) (bh + 1) + bh->bytelen;
			p = (char *) MAXALIGN(p);
		}
		UnlockReleaseBuffer(buf);
		blk = next;
		off = MAXALIGN(SizeOfPageHeaderData);	/* later pages: contents start */
	}
	*out = posts;
	if (blockmax)
		*blockmax = bmax;
	return n;
}

/* ----- writing the index pages ----- */

/*
 * Low-page-biased allocation context.  Normally bm25_new_buffer() hands out
 * whatever free page the FSM offers (unordered), then extends.  During a
 * space-reclaiming compaction we instead want to pack live pages toward the
 * FRONT of the file so the dead tail can be truncated.  bm25_alloc_begin()
 * gathers all currently-free blocks, sorts them ascending, and
 * bm25_new_buffer() hands them out low-first; when the low-free list is
 * exhausted it falls back to the ordinary FSM/extend path.  The context is a
 * single backend-scoped hint (compaction is single-writer), reset by
 * bm25_alloc_end().
 */
static BlockNumber *bm25_lowfree = NULL;
static int	bm25_lowfree_n = 0;
static int	bm25_lowfree_i = 0;

static int
cmp_blocknumber(const void *a, const void *b)
{
	BlockNumber x = *(const BlockNumber *) a;
	BlockNumber y = *(const BlockNumber *) b;

	return (x < y) ? -1 : (x > y) ? 1 : 0;
}

/*
 * Gather all free blocks (via a linear FSM probe) into an ascending array so
 * subsequent bm25_new_buffer() calls reuse the lowest blocks first.  Single
 * writer only.  Cheap relative to the segment rewrite it precedes.
 */
static void
bm25_alloc_begin(Relation index)
{
	BlockNumber nblocks = RelationGetNumberOfBlocks(index);
	BlockNumber blk;

	bm25_lowfree_i = 0;
	bm25_lowfree_n = 0;
	bm25_lowfree = NULL;
	if (nblocks <= 1)
		return;
	bm25_lowfree = (BlockNumber *) palloc(sizeof(BlockNumber) * nblocks);
	for (blk = 1; blk < nblocks; blk++)	/* block 0 = metapage, never free */
		if (GetRecordedFreeSpace(index, blk) >= BLCKSZ / 2)
			bm25_lowfree[bm25_lowfree_n++] = blk;
	if (bm25_lowfree_n > 1)
		qsort(bm25_lowfree, bm25_lowfree_n, sizeof(BlockNumber), cmp_blocknumber);
}

static void
bm25_alloc_end(void)
{
	if (bm25_lowfree)
		pfree(bm25_lowfree);
	bm25_lowfree = NULL;
	bm25_lowfree_n = 0;
	bm25_lowfree_i = 0;
}

static Buffer
bm25_new_buffer(Relation index)
{
	Buffer		buffer;

	/*
	 * Low-bias reuse: during a compaction, prefer the lowest free block so
	 * live pages pack at the front of the file.
	 */
	while (bm25_lowfree && bm25_lowfree_i < bm25_lowfree_n)
	{
		BlockNumber blk = bm25_lowfree[bm25_lowfree_i++];

		buffer = ReadBuffer(index, blk);
		if (ConditionalLockBuffer(buffer))
		{
			RecordUsedIndexPage(index, blk);
			return buffer;
		}
		ReleaseBuffer(buffer);
	}

	/* Try to reuse a page freed by a previous merge before extending. */
	for (;;)
	{
		BlockNumber blk = GetFreeIndexPage(index);

		if (blk == InvalidBlockNumber)
			break;				/* no free page; extend below */
		buffer = ReadBuffer(index, blk);
		if (ConditionalLockBuffer(buffer))
			return buffer;		/* got it */
		/* someone else is using it; try the next free page */
		ReleaseBuffer(buffer);
	}

	/*
	 * Extend the relation.  Concurrent appenders (parallel build/merge
	 * participants) would otherwise race on P_NEW and trip "unexpected data
	 * beyond EOF", so the extension itself is serialized with the relation
	 * extension lock -- but ONLY around the single P_NEW call, not around the
	 * whole segment write.  Holding it for the entire (multi-GB) write would
	 * serialize the participants' writes and defeat the parallel merge; a
	 * per-page extension lock lets them write concurrently.
	 */
	if (IsInParallelMode())
		LockRelationForExtension(index, ExclusiveLock);
	buffer = ReadBuffer(index, P_NEW);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
	if (IsInParallelMode())
		UnlockRelationForExtension(index, ExclusiveLock);
	return buffer;
}

static void
bm25_init_page(Page page, uint16 flags)
{
	BM25PageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(BM25PageOpaqueData));
	opaque = BM25PageGetOpaque(page);
	opaque->flags = flags;
	opaque->nextblk = InvalidBlockNumber;
	/* start item area at the (MAXALIGN'd) contents offset used by readers */
	((PageHeader) page)->pd_lower = (char *) PageGetContents(page) - (char *) page;
}

static void
bm25_init_metapage(Relation index)
{
	Buffer		buffer;
	GenericXLogState *state;
	Page		page;
	BM25MetaPageData *meta;

	buffer = bm25_new_buffer(index);
	Assert(BufferGetBlockNumber(buffer) == BM25_METAPAGE_BLKNO);

	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
	bm25_init_page(page, BM25_META);
	meta = BM25PageGetMeta(page);
	MemSet(meta, 0, sizeof(BM25MetaPageData));
	meta->magic = BM25_MAGIC;
	meta->version = BM25_VERSION;
	meta->ndocs = 0;
	meta->sumdoclen = 0;
	meta->nsegments = 0;
	meta->pendinghead = InvalidBlockNumber;
	meta->pendingtail = InvalidBlockNumber;
	meta->npending = 0;
	((PageHeader) page)->pd_lower =
		((char *) meta + sizeof(BM25MetaPageData)) - (char *) page;
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buffer);
}

/*
 * Write all postings for one term into the segment's shared posting-page chain
 * via a BM25PostWriter, returning the term's first block + byte offset.
 * Postings are docid-sorted and packed into 128-doc FOR blocks (BM25BlockHdr +
 * three frame-of-reference bit-packed columns: docid-gaps, tfs, doclens), which
 * compresses the common case of many clustered docids into a few bits each.
 */
typedef struct BM25PostingSort
{
	uint64		docid;
	uint32		tf;
	uint32		doclen;
	ItemPointerData tid;
}			BM25PostingSort;

static int
cmp_posting_docid(const void *a, const void *b)
{
	uint64		da = ((const BM25PostingSort *) a)->docid;
	uint64		db = ((const BM25PostingSort *) b)->docid;

	if (da < db)
		return -1;
	if (da > db)
		return 1;
	return 0;
}

/*
 * Shared posting-page writer.  All terms in a segment append their blocks into
 * ONE chain of posting pages, so a rare term (a handful of postings) costs a
 * few dozen bytes instead of a whole 8 KB page -- critical for a Zipfian
 * vocabulary where most terms are tiny.  Each term records its start (block +
 * byte offset); the reader decodes blocks from there, counting postings until
 * it has read the term's df, following nextblk across page boundaries.
 */
typedef struct BM25PostWriter
{
	Relation	index;
	Buffer		buffer;
	GenericXLogState *state;
	Page		page;
} BM25PostWriter;

static void
pw_begin(BM25PostWriter *pw, Relation index)
{
	pw->index = index;
	pw->buffer = InvalidBuffer;
	pw->state = NULL;
	pw->page = NULL;
}

static void
pw_finish(BM25PostWriter *pw)
{
	if (pw->buffer != InvalidBuffer)
	{
		GenericXLogFinish(pw->state);
		UnlockReleaseBuffer(pw->buffer);
		pw->buffer = InvalidBuffer;
	}
}

/*
 * Append one term's postings to the shared writer.  Returns the block and byte
 * offset where the term's first block begins (for its dictionary entry).
 */
static void
bm25_write_postings(BM25PostWriter *pw, BuildTerm *bt,
					BlockNumber *firstblk, uint32 *firstoff)
{
	Relation	index = pw->index;
	BM25PostingSort *sorted;
	int			i;
	bool		start_recorded = false;

	*firstblk = InvalidBlockNumber;
	*firstoff = 0;

	sorted = (BM25PostingSort *) palloc(Max(bt->nposts, 1) * sizeof(BM25PostingSort));
	for (i = 0; i < bt->nposts; i++)
	{
		sorted[i].docid = bm25_tid_to_docid(&bt->tids[i]);
		sorted[i].tf = bt->tfs[i];
		sorted[i].doclen = bt->doclens[i];
		sorted[i].tid = bt->tids[i];
	}
	if (bt->nposts > 1)
		qsort(sorted, bt->nposts, sizeof(BM25PostingSort), cmp_posting_docid);

	i = 0;
	while (i < bt->nposts)
	{
		uint64		gaps[BM25_BLOCK_SIZE];
		uint64		tfs[BM25_BLOCK_SIZE];
		uint64		dls[BM25_BLOCK_SIZE];
		unsigned char scratch[3 * (1 + (BM25_BLOCK_SIZE * 64 + 7) / 8)];
		int			sclen = 0;
		uint32		blk_max_tf = 0;
		uint32		blk_min_dl = UINT32_MAX;
		uint64		blk_first_docid = sorted[i].docid;
		uint64		prev_docid = sorted[i].docid;
		int			bcount = 0;
		char	   *pageend;
		Size		need;
		char	   *dst;
		BM25BlockHdr *bh;

		/* gather up to BM25_BLOCK_SIZE postings into columns (SoA) */
		while (i < bt->nposts && bcount < BM25_BLOCK_SIZE)
		{
			gaps[bcount] = sorted[i].docid - prev_docid;	/* first gap is 0 */
			tfs[bcount] = sorted[i].tf;
			dls[bcount] = sorted[i].doclen;
			if (sorted[i].tf > blk_max_tf)
				blk_max_tf = sorted[i].tf;
			if (sorted[i].doclen < blk_min_dl)
				blk_min_dl = sorted[i].doclen;
			prev_docid = sorted[i].docid;
			bcount++;
			i++;
		}

		sclen += bm25_for_pack(gaps, bcount, scratch + sclen);
		sclen += bm25_for_pack(tfs, bcount, scratch + sclen);
		sclen += bm25_for_pack(dls, bcount, scratch + sclen);

		need = MAXALIGN(sizeof(BM25BlockHdr) + sclen);

		/* need a page with room for this block? */
		if (pw->buffer != InvalidBuffer)
		{
			pageend = (char *) pw->page + BLCKSZ - MAXALIGN(sizeof(BM25PageOpaqueData));
			if ((char *) pw->page + ((PageHeader) pw->page)->pd_lower + need > pageend)
			{
				Buffer		next = bm25_new_buffer(index);
				BlockNumber nextblk = BufferGetBlockNumber(next);

				BM25PageGetOpaque(pw->page)->nextblk = nextblk;
				GenericXLogFinish(pw->state);
				UnlockReleaseBuffer(pw->buffer);
				pw->buffer = next;
				pw->state = GenericXLogStart(index);
				pw->page = GenericXLogRegisterBuffer(pw->state, pw->buffer, GENERIC_XLOG_FULL_IMAGE);
				bm25_init_page(pw->page, BM25_POSTING);
			}
		}
		if (pw->buffer == InvalidBuffer)
		{
			pw->buffer = bm25_new_buffer(index);
			pw->state = GenericXLogStart(index);
			pw->page = GenericXLogRegisterBuffer(pw->state, pw->buffer, GENERIC_XLOG_FULL_IMAGE);
			bm25_init_page(pw->page, BM25_POSTING);
		}

		/* record the term's start at its first block */
		if (!start_recorded)
		{
			*firstblk = BufferGetBlockNumber(pw->buffer);
			*firstoff = (uint32) ((PageHeader) pw->page)->pd_lower;
			start_recorded = true;
		}

		dst = (char *) pw->page + ((PageHeader) pw->page)->pd_lower;
		bh = (BM25BlockHdr *) dst;
		bh->count = (uint32) bcount;
		bh->max_tf = blk_max_tf;
		bh->min_doclen = (blk_min_dl == UINT32_MAX ? 0 : blk_min_dl);
		bh->first_docid_hi = (uint32) (blk_first_docid >> 32);
		bh->first_docid_lo = (uint32) (blk_first_docid & 0xFFFFFFFF);
		bh->bytelen = (uint32) sclen;
		memcpy((char *) (bh + 1), scratch, sclen);
		((PageHeader) pw->page)->pd_lower += need;
	}

	pfree(sorted);
}

/*
 * Write the dictionary: sorted (term, df, firstposting) entries packed into a
 * chain of dictionary pages.  Returns the first dictionary block, and via
 * *indexstart the first page of the sparse block index (Invalid if empty).
 */
static BlockNumber
bm25_write_dictionary(Relation index, BM25BuildState *bs,
					  BlockNumber *postings, uint32 *offsets,
					  BlockNumber *indexstart)
{
	BlockNumber first = InvalidBlockNumber;
	Buffer		buffer = InvalidBuffer;
	GenericXLogState *state = NULL;
	Page		page = NULL;
	int			i;

	/* collect (blk, first-term-index) of each dict page for the block index */
	BlockNumber *pgblk = NULL;
	int		   *pgterm = NULL;
	int			npages = 0;
	int			pgcap = 0;

	*indexstart = InvalidBlockNumber;

	for (i = 0; i < bs->nterms; i++)
	{
		BuildTerm  *bt = &bs->terms[i];
		Size		need = MAXALIGN(sizeof(BM25DictEntry) + bt->len);
		char	   *dst;
		bool		newpage = false;

		if (buffer == InvalidBuffer ||
			((PageHeader) page)->pd_lower + need >
			BLCKSZ - sizeof(BM25PageOpaqueData))
		{
			Buffer		next = bm25_new_buffer(index);
			BlockNumber nextblk = BufferGetBlockNumber(next);

			if (buffer != InvalidBuffer)
			{
				BM25PageGetOpaque(page)->nextblk = nextblk;
				GenericXLogFinish(state);
				UnlockReleaseBuffer(buffer);
			}
			else
				first = nextblk;

			buffer = next;
			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
			bm25_init_page(page, BM25_DICT);
			newpage = true;
		}

		if (newpage)
		{
			if (npages >= pgcap)
			{
				pgcap = Max(pgcap * 2, 64);
				pgblk = pgblk ? repalloc(pgblk, pgcap * sizeof(BlockNumber))
					: palloc(pgcap * sizeof(BlockNumber));
				pgterm = pgterm ? repalloc(pgterm, pgcap * sizeof(int))
					: palloc(pgcap * sizeof(int));
			}
			pgblk[npages] = BufferGetBlockNumber(buffer);
			pgterm[npages] = i;		/* this term is the page's first */
			npages++;
		}

		dst = (char *) page + ((PageHeader) page)->pd_lower;
		{
			BM25DictEntry *de = (BM25DictEntry *) dst;
			int			p;
			uint32		maxtf = 0;

			de->termlen = bt->len;
			de->df = bt->nposts;
			for (p = 0; p < bt->nposts; p++)
				if (bt->tfs[p] > maxtf)
					maxtf = bt->tfs[p];
			de->max_tf = maxtf;
			de->firstposting = postings[i];
			de->firstoffset = offsets[i];
			memcpy(de->term, bt->term, bt->len);
		}
		((PageHeader) page)->pd_lower += need;
	}

	if (buffer != InvalidBuffer)
	{
		GenericXLogFinish(state);
		UnlockReleaseBuffer(buffer);
	}

	/* write the sparse block index: one entry per dict page, in term order */
	if (npages > 0)
	{
		BlockNumber ifirst = InvalidBlockNumber;
		Buffer		ib = InvalidBuffer;
		Page		ip = NULL;
		GenericXLogState *istate = NULL;
		int			j;

		for (j = 0; j < npages; j++)
		{
			BuildTerm  *bt = &bs->terms[pgterm[j]];
			Size		need = MAXALIGN(offsetof(BM25DictIndexEntry, term) + bt->len);
			char	   *dst;
			BM25DictIndexEntry *ie;

			if (ib == InvalidBuffer ||
				((PageHeader) ip)->pd_lower + need >
				BLCKSZ - sizeof(BM25PageOpaqueData))
			{
				Buffer		next = bm25_new_buffer(index);
				BlockNumber nextblk = BufferGetBlockNumber(next);

				if (ib != InvalidBuffer)
				{
					BM25PageGetOpaque(ip)->nextblk = nextblk;
					GenericXLogFinish(istate);
					UnlockReleaseBuffer(ib);
				}
				else
					ifirst = nextblk;
				ib = next;
				istate = GenericXLogStart(index);
				ip = GenericXLogRegisterBuffer(istate, ib, GENERIC_XLOG_FULL_IMAGE);
				bm25_init_page(ip, BM25_DICTINDEX);
			}
			dst = (char *) ip + ((PageHeader) ip)->pd_lower;
			ie = (BM25DictIndexEntry *) dst;
			ie->blk = pgblk[j];
			ie->termlen = bt->len;
			memcpy(ie->term, bt->term, bt->len);
			((PageHeader) ip)->pd_lower += need;
		}
		if (ib != InvalidBuffer)
		{
			GenericXLogFinish(istate);
			UnlockReleaseBuffer(ib);
		}
		*indexstart = ifirst;
	}
	if (pgblk)
		pfree(pgblk);
	if (pgterm)
		pfree(pgterm);

	return first;
}

/* forward decl: trigram index writer (pg_fts_trgm_index.c, included below) */
static BlockNumber bm25_write_trigrams(Relation index, BM25BuildState *bs);
/* forward decls: blob read/write live in pg_fts_trgm_index.c (included below) */
static BlockNumber bm25_write_blob(Relation index, const uint8 *data, Size len);
static uint8 *bm25_read_blob(Relation index, BlockNumber blk, Size len);

/*
 * Write one immutable segment (dictionary + postings + trigram index) from a
 * populated build state, filling *seg.  The build state's terms must already
 * be sorted.  livedocs starts empty (no tombstones); segments share the global
 * docid space via heap TIDs.
 */
static void
bm25_write_segment(Relation index, BM25BuildState *bs, BM25SegMeta *seg)
{
	BlockNumber *postings;
	uint32	   *offsets;
	BM25PostWriter pw;
	int			i;

	postings = (BlockNumber *) palloc(Max(bs->nterms, 1) * sizeof(BlockNumber));
	offsets = (uint32 *) palloc(Max(bs->nterms, 1) * sizeof(uint32));
	pw_begin(&pw, index);
	for (i = 0; i < bs->nterms; i++)
		bm25_write_postings(&pw, &bs->terms[i], &postings[i], &offsets[i]);
	pw_finish(&pw);

	MemSet(seg, 0, sizeof(BM25SegMeta));
	seg->dictstart = bm25_write_dictionary(index, bs, postings, offsets, &seg->dictindexstart);
	seg->trgmstart = bm25_write_trigrams(index, bs);
	seg->livedocs = InvalidBlockNumber;
	seg->ndocs = bs->ndocs;
	seg->sumdoclen = bs->sumdoclen;
	seg->nterms = bs->nterms;
	seg->ndeleted = 0;
	seg->livedocslen = 0;
	pfree(postings);
	pfree(offsets);
}

/*
 * Append a segment descriptor to the metapage directory and fold its doc stats
 * into the corpus totals.  The directory is a fixed array in the metapage; the
 * size-tiered merge keeps the live segment count far below BM25_MAX_SEGMENTS,
 * so hitting the cap means merging is not keeping up -- we raise a clear error
 * (data is intact) rather than chain overflow directory pages, which would add
 * complexity to every reader for a case the merge policy makes unreachable.
 */
static void
bm25_meta_add_segment(Relation index, const BM25SegMeta *seg)
{
	Buffer		buf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
	GenericXLogState *state;
	Page		page;
	BM25MetaPageData *m;

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	m = BM25PageGetMeta(page);
	if (m->nsegments >= BM25_MAX_SEGMENTS)
	{
		GenericXLogAbort(state);
		UnlockReleaseBuffer(buf);
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("bm25 index \"%s\" reached the maximum of %d segments",
						RelationGetRelationName(index), BM25_MAX_SEGMENTS),
				 errhint("Run VACUUM to merge segments, or REINDEX to rebuild.")));
	}
	m->segs[m->nsegments] = *seg;
	m->nsegments++;
	m->ndocs += seg->ndocs;
	m->sumdoclen += seg->sumdoclen;
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/* Read one segment's dictionary + posting lists into a build state. */
static void
bm25_read_segment_into(Relation index, const BM25SegMeta *seg, BM25BuildState *bs)
{
	BlockNumber blk = seg->dictstart;
	uint8	   *tombbuf = NULL;
	sm_t		tomb;
	bool		hastomb = false;
	sm_cursor_cached_t tombcache = SM_CURSOR_CACHED_INIT;

	/* open this segment's tombstone bitmap so merge physically DROPS deleted
	 * docs (otherwise re-adding their postings would resurrect them) */
	if (seg->livedocs != InvalidBlockNumber && seg->livedocslen > 0)
	{
		tombbuf = bm25_read_blob(index, seg->livedocs, seg->livedocslen);
		sm_open(&tomb, (uint8_t *) tombbuf, seg->livedocslen);
		hastomb = true;
	}

	while (blk != InvalidBlockNumber)
	{
		Buffer		buffer = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;
		MemoryContext old = MemoryContextSwitchTo(bs->ctx);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;
		while (ptr < end)
		{
			BM25DictEntry *de = (BM25DictEntry *) ptr;
			Size		esize = MAXALIGN(offsetof(BM25DictEntry, term) + de->termlen);
			BM25Posting *post;
			int			np,
						k;

			np = bm25_decode_term(index, de->firstposting, de->firstoffset,
								  de->df, &post, NULL);
			for (k = 0; k < np; k++)
			{
				if (hastomb)
				{
					/* postings are docid-ascending within a term; a cached MRU
					 * chunk cache turns the per-posting membership test into
					 * O(1) hits over the hot chunks */
					if (sm_contains_cached(&tomb, bm25_tid_to_docid(&post[k].tid),
										   &tombcache))
						continue;	/* tombstoned: drop from the merged segment */
				}
				add_posting(bs, de->term, de->termlen,
							&post[k].tid, post[k].tf, post[k].doclen);
			}
			pfree(post);
			ptr += esize;
		}
		UnlockReleaseBuffer(buffer);
		MemoryContextSwitchTo(old);
		blk = next;
	}
	if (tombbuf)
		pfree(tombbuf);
	bs->ndocs += seg->ndocs - seg->ndeleted;
}

/* Recycle a chained page list (dict/trigram/posting/data) to the FSM. */
static void
bm25_free_chain(Relation index, BlockNumber blk)
{
	while (blk != InvalidBlockNumber)
	{
		Buffer		buf = ReadBuffer(index, blk);
		BlockNumber next;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		next = BM25PageGetOpaque(BufferGetPage(buf))->nextblk;
		UnlockReleaseBuffer(buf);
		RecordFreeIndexPage(index, blk);
		blk = next;
	}
}

/* Free all pages of a segment (dict + each term's postings + trigram dir+data). */
static void
bm25_free_segment(Relation index, const BM25SegMeta *seg)
{
	BlockNumber blk = seg->dictstart;
	BlockNumber postchain = InvalidBlockNumber;

	/* dictionary pages; capture the shared posting chain's first block */
	while (blk != InvalidBlockNumber)
	{
		Buffer		buf = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;
		while (ptr < end)
		{
			BM25DictEntry *de = (BM25DictEntry *) ptr;
			Size		esize = MAXALIGN(offsetof(BM25DictEntry, term) + de->termlen);

			/* all terms share ONE posting chain; the first term names its head */
			if (postchain == InvalidBlockNumber)
				postchain = de->firstposting;
			ptr += esize;
		}
		UnlockReleaseBuffer(buf);
		RecordFreeIndexPage(index, blk);
		blk = next;
	}
	if (postchain != InvalidBlockNumber)
		bm25_free_chain(index, postchain);	/* free the shared posting chain once */

	/* trigram directory pages (+ their data blobs) */
	blk = seg->trgmstart;
	while (blk != InvalidBlockNumber)
	{
		Buffer		buf = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;
		while (ptr < end)
		{
			BM25TrgmEntry *te = (BM25TrgmEntry *) ptr;

			bm25_free_chain(index, te->firstdata);
			ptr += MAXALIGN(sizeof(BM25TrgmEntry));
		}
		UnlockReleaseBuffer(buf);
		RecordFreeIndexPage(index, blk);
		blk = next;
	}

	if (seg->livedocs != InvalidBlockNumber)
		bm25_free_chain(index, seg->livedocs);
	if (seg->dictindexstart != InvalidBlockNumber)
		bm25_free_chain(index, seg->dictindexstart);
}

/*
 * Size-tiered segment merge (a Lucene TieredMergePolicy in miniature).
 *
 * Rather than merging the whole directory into one segment on every trigger
 * (O(index) write amplification under steady inserts), we merge only a RUN of
 * similarly-sized segments at a time: sort the live segments by size (live doc
 * count) and, if the smallest ones fall within a size factor of each other,
 * merge just those into one new segment.  Small flushes coalesce cheaply while
 * large segments are rarely rewritten.  We loop until no tier qualifies and the
 * count is within budget, so query cost stays O(nsegments) small.  Tombstoned
 * docs are dropped as segments are read.  Called after a flush, from build, and
 * from VACUUM.
 */
#define BM25_MERGE_THRESHOLD 8		/* keep the live segment count at or below this */
#define BM25_MERGE_TIER_MIN 4		/* merge when >= this many same-tier segments */
#define BM25_MERGE_SIZE_FACTOR 3.0	/* "same tier" = within this size ratio */

/* segment (index,size) pair for sorting merge candidates by size */
typedef struct MergeCand
{
	uint32		idx;
	double		size;
}			MergeCand;

static int
cmp_mergecand(const void *a, const void *b)
{
	double		sa = ((const MergeCand *) a)->size;
	double		sb = ((const MergeCand *) b)->size;

	return (sa < sb) ? -1 : (sa > sb) ? 1 : 0;
}

/*
 * Merge one selected set of segments (by directory index) into a single new
 * segment, rewrite the metapage directory to drop the merged ones (preserving
 * the order of the rest) and append the new segment, then recycle the merged
 * segments' pages.  Returns true on success, false if the directory changed
 * underneath (caller stops).
 */
/*
 * Merge a specific set of segment descriptors (by CONTENT, not directory index)
 * into one new segment, writing its pages but NOT touching the metapage
 * directory.  Returns the new descriptor in *out.  Safe to run concurrently
 * with other callers merging DISJOINT descriptor sets: page appends are
 * serialized by the relation extension lock (in bm25_build_flush_segment's
 * peer path we lock explicitly; here bm25_write_segment appends under the same
 * discipline when IsInParallelMode()).  The caller (leader) removes the
 * consumed descriptors and installs *out in a single metapage update.
 */
static void
bm25_merge_group_to_seg(Relation index, const BM25SegMeta *group, uint32 ngroup,
						BM25SegMeta *out)
{
	BM25BuildState bs;
	uint32		i;

	bs.ctx = AllocSetContextCreate(CurrentMemoryContext, "bm25 merge group",
								   ALLOCSET_DEFAULT_SIZES);
	bs.terms = NULL;
	bs.nterms = 0;
	bs.maxterms = 0;
	bs.ndocs = 0;
	bs.sumdoclen = 0;
	bm25_build_ht_init(&bs);

	for (i = 0; i < ngroup; i++)
	{
		bs.sumdoclen += group[i].sumdoclen;
		bm25_read_segment_into(index, &group[i], &bs);
	}
	if (bs.nterms > 1)
		qsort(bs.terms, bs.nterms, sizeof(BuildTerm), cmp_buildterm);

	/* page appends are serialized per-page inside bm25_new_buffer */
	bm25_write_segment(index, &bs, out);
	out->ndocs = bs.ndocs;
	out->sumdoclen = bs.sumdoclen;

	MemoryContextDelete(bs.ctx);
}

static bool
bm25_merge_selected(Relation index, const uint32 *sel, uint32 nsel)
{
	BM25MetaPageData meta;
	BM25BuildState bs;
	BM25SegMeta newseg;
	BM25SegMeta chosen[BM25_MAX_SEGMENTS];
	uint32		i;

	{
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);

		LockBuffer(mb, BUFFER_LOCK_SHARE);
		memcpy(&meta, BM25PageGetMeta(BufferGetPage(mb)), sizeof(meta));
		UnlockReleaseBuffer(mb);
	}
	for (i = 0; i < nsel; i++)
	{
		if (sel[i] >= meta.nsegments)
			return false;		/* directory changed under us */
		chosen[i] = meta.segs[sel[i]];
	}

	bs.ctx = AllocSetContextCreate(CurrentMemoryContext, "bm25 merge segs",
								   ALLOCSET_DEFAULT_SIZES);
	bs.terms = NULL;
	bs.nterms = 0;
	bs.maxterms = 0;
	bs.ndocs = 0;
	bs.sumdoclen = 0;
	bm25_build_ht_init(&bs);

	for (i = 0; i < nsel; i++)
	{
		bs.sumdoclen += chosen[i].sumdoclen;
		bm25_read_segment_into(index, &chosen[i], &bs);
	}
	if (bs.nterms > 1)
		qsort(bs.terms, bs.nterms, sizeof(BuildTerm), cmp_buildterm);

	bm25_write_segment(index, &bs, &newseg);
	newseg.ndocs = bs.ndocs;
	newseg.sumdoclen = bs.sumdoclen;

	{
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);
		GenericXLogState *state;
		Page		mp;
		BM25MetaPageData *m;
		bool		same = true;

		LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		mp = GenericXLogRegisterBuffer(state, mb, 0);
		m = BM25PageGetMeta(mp);

		/* single-writer (VACUUM/flush/build) context, but re-check the chosen
		 * descriptors still match before committing the rewrite */
		if (m->nsegments != meta.nsegments)
			same = false;
		for (i = 0; same && i < nsel; i++)
			if (memcmp(&m->segs[sel[i]], &chosen[i], sizeof(BM25SegMeta)) != 0)
				same = false;

		if (same)
		{
			BM25SegMeta kept[BM25_MAX_SEGMENTS];
			uint32		nkept = 0;
			uint32		j;
			bool		issel;

			/* keep every segment not in sel[], preserving order */
			for (i = 0; i < m->nsegments; i++)
			{
				issel = false;
				for (j = 0; j < nsel; j++)
					if (sel[j] == i)
					{
						issel = true;
						break;
					}
				if (!issel)
					kept[nkept++] = m->segs[i];
			}
			kept[nkept++] = newseg;	/* append the merged segment */
			memcpy(m->segs, kept, nkept * sizeof(BM25SegMeta));
			m->nsegments = nkept;
			/* corpus totals unchanged (same docs, tombstones already excluded) */
			GenericXLogFinish(state);
			UnlockReleaseBuffer(mb);
			for (i = 0; i < nsel; i++)
				bm25_free_segment(index, &chosen[i]);
			IndexFreeSpaceMapVacuum(index);
			MemoryContextDelete(bs.ctx);
			return true;
		}
		else
		{
			/* directory changed; abandon (new segment leaks until next merge/
			 * REINDEX -- rare, single-writer path) */
			GenericXLogAbort(state);
			UnlockReleaseBuffer(mb);
			MemoryContextDelete(bs.ctx);
			return false;
		}
	}
}

/*
 * Merge ALL live segments into a single segment (explicit full compaction).
 * Used by fts_merge() so an on-demand call actually produces an optimal,
 * single-segment index (the tiered bm25_merge_segments only coalesces
 * same-size tiers and may deliberately leave several segments).  Merges in
 * bounded batches (BM25_MAX_SEGMENTS worth of selection at a time is fine since
 * a build/merge never exceeds the cap) and loops until one segment remains.
 * Returns true if it changed anything.
 */
/* ---- parallel merge (compact many segments into few, in parallel) ----
 *
 * The leader partitions the live segments into W disjoint groups; each worker
 * merges ONE group into one new segment (bm25_merge_group_to_seg -- writes
 * pages only, no directory touch) and reports the new descriptor via DSM.  The
 * leader then performs a SINGLE metapage update: drop all the consumed source
 * descriptors and install the W new ones.  This confines the expensive
 * decode/re-encode to parallel workers and keeps the directory swap serial and
 * atomic (no concurrent-swap race).  Result: W segments; caller may run a
 * final (cheap, W-way) pass if it wants exactly one.
 *
 * ponytail: Level-2 could recurse the parallel merge (W -> W/2 -> ... -> 1) so
 * even the final combine parallelizes; deferred -- one parallel pass already
 * removes the dominant per-segment decode cost from the serial path.
 */
#define PARALLEL_KEY_BM25_MERGE		UINT64CONST(0xB250000000000010)

typedef struct BM25MergeShared
{
	Oid			heaprelid;
	Oid			indexrelid;
	int			ngroups;		/* number of worker groups */
	int			nsrc;			/* total source segments */
	slock_t		mutex;
	/* filled by workers: the merged-segment descriptor per group */
	BM25SegMeta outseg[BM25_MAX_SEGMENTS];
	bool		outvalid[BM25_MAX_SEGMENTS];
	/* group layout: src[groupoff[g] .. groupoff[g+1]) are group g's sources */
	int			groupoff[BM25_MAX_SEGMENTS + 1];
	BM25SegMeta src[BM25_MAX_SEGMENTS];
}			BM25MergeShared;

static void bm25_merge_one_group(Relation index, BM25MergeShared *ms, int g);

PGDLLEXPORT void bm25_parallel_merge_main(dsm_segment *seg, shm_toc *toc);

void
bm25_parallel_merge_main(dsm_segment *seg, shm_toc *toc)
{
	BM25MergeShared *ms;
	Relation	heap;
	Relation	index;

	ms = (BM25MergeShared *) shm_toc_lookup(toc, PARALLEL_KEY_BM25_MERGE, false);
	heap = table_open(ms->heaprelid, AccessShareLock);
	index = index_open(ms->indexrelid, RowExclusiveLock);

	/* worker N handles group (N+1); group 0 is the leader's */
	if (ParallelWorkerNumber + 1 < ms->ngroups)
		bm25_merge_one_group(index, ms, ParallelWorkerNumber + 1);

	index_close(index, RowExclusiveLock);
	table_close(heap, AccessShareLock);
}

/* merge group g's sources into one segment, store descriptor in shared state */
static void
bm25_merge_one_group(Relation index, BM25MergeShared *ms, int g)
{
	int			lo = ms->groupoff[g];
	int			hi = ms->groupoff[g + 1];
	BM25SegMeta out;

	if (hi - lo <= 0)
		return;
	if (hi - lo == 1)
	{
		/* singleton group: nothing to merge, keep the source as-is */
		ms->outseg[g] = ms->src[lo];
		ms->outvalid[g] = false;	/* signals "source kept, no new seg" */
		return;
	}
	bm25_merge_group_to_seg(index, &ms->src[lo], (uint32) (hi - lo), &out);
	SpinLockAcquire(&ms->mutex);
	ms->outseg[g] = out;
	ms->outvalid[g] = true;
	SpinLockRelease(&ms->mutex);
}

/*
 * Parallel merge-all: partition live segments into (workers+1) groups, each
 * participant merges its group into a new segment, then the leader installs the
 * results with a single metapage update.  Returns true if it ran (and did the
 * directory swap), false to signal the caller to fall back to serial.
 */
static bool
bm25_merge_all_parallel(Relation index, int request)
{
	ParallelContext *pcxt;
	BM25MergeShared *ms;
	BM25MetaPageData meta;
	Size		estms;
	int			ngroups;
	int			nsrc;
	int			g,
				i;
	Relation	heap;
	Oid			heaprelid;

	{
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);

		LockBuffer(mb, BUFFER_LOCK_SHARE);
		memcpy(&meta, BM25PageGetMeta(BufferGetPage(mb)), sizeof(meta));
		UnlockReleaseBuffer(mb);
	}
	if (meta.nsegments <= 2)
		return false;			/* not worth parallelizing; serial handles it */

	heaprelid = index->rd_index->indrelid;

	EnterParallelMode();
	pcxt = CreateParallelContext("pg_fts", "bm25_parallel_merge_main", request);
	estms = BUFFERALIGN(sizeof(BM25MergeShared));
	shm_toc_estimate_chunk(&pcxt->estimator, estms);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	InitializeParallelDSM(pcxt);

	if (pcxt->seg == NULL)
	{
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return false;
	}

	ms = (BM25MergeShared *) shm_toc_allocate(pcxt->toc, estms);
	ms->heaprelid = heaprelid;
	ms->indexrelid = RelationGetRelid(index);
	SpinLockInit(&ms->mutex);

	/* collect the live source segments */
	nsrc = 0;
	for (i = 0; i < (int) meta.nsegments; i++)
		if (meta.segs[i].dictstart != InvalidBlockNumber)
			ms->src[nsrc++] = meta.segs[i];
	ms->nsrc = nsrc;

	/* groups = min(participants, nsrc); participant 0 = leader */
	ngroups = request + 1;
	if (ngroups > nsrc)
		ngroups = nsrc;
	ms->ngroups = ngroups;

	/* even contiguous partition of the nsrc sources into ngroups */
	for (g = 0; g <= ngroups; g++)
		ms->groupoff[g] = (int) ((int64) g * nsrc / ngroups);
	for (g = 0; g < ngroups; g++)
		ms->outvalid[g] = false;

	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BM25_MERGE, ms);
	LaunchParallelWorkers(pcxt);

	if (pcxt->nworkers_launched == 0)
	{
		WaitForParallelWorkersToFinish(pcxt);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return false;			/* no workers; serial fallback */
	}

	/* leader merges group 0 itself while workers handle groups 1..n */
	heap = table_open(heaprelid, AccessShareLock);
	bm25_merge_one_group(index, ms, 0);
	table_close(heap, AccessShareLock);

	WaitForParallelWorkersToFinish(pcxt);

	/*
	 * Single atomic directory update: drop every consumed source descriptor
	 * (content-match) and install each group's merged descriptor.  Groups that
	 * did not actually merge (singleton) keep their one source, so we simply
	 * don't drop it.
	 */
	{
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);
		GenericXLogState *state;
		Page		mp;
		BM25MetaPageData *m;
		BM25SegMeta kept[BM25_MAX_SEGMENTS];
		uint32		nkept = 0;
		uint32		j;
		int			k;

		LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		mp = GenericXLogRegisterBuffer(state, mb, 0);
		m = BM25PageGetMeta(mp);

		/* keep any segment that is NOT a consumed source of a merged group */
		for (j = 0; j < m->nsegments; j++)
		{
			bool		consumed = false;

			for (g = 0; g < ngroups && !consumed; g++)
			{
				if (!ms->outvalid[g])
					continue;	/* singleton group merged nothing */
				for (k = ms->groupoff[g]; k < ms->groupoff[g + 1]; k++)
					if (memcmp(&m->segs[j], &ms->src[k], sizeof(BM25SegMeta)) == 0)
					{
						consumed = true;
						break;
					}
			}
			if (!consumed)
				kept[nkept++] = m->segs[j];
		}
		/* append each merged group's new segment */
		for (g = 0; g < ngroups; g++)
			if (ms->outvalid[g])
				kept[nkept++] = ms->outseg[g];

		memcpy(m->segs, kept, nkept * sizeof(BM25SegMeta));
		m->nsegments = nkept;
		GenericXLogFinish(state);
		UnlockReleaseBuffer(mb);

		/* recycle the consumed source segments' pages */
		for (g = 0; g < ngroups; g++)
			if (ms->outvalid[g])
				for (k = ms->groupoff[g]; k < ms->groupoff[g + 1]; k++)
					bm25_free_segment(index, &ms->src[k]);
	}

	DestroyParallelContext(pcxt);
	ExitParallelMode();
	IndexFreeSpaceMapVacuum(index);
	return true;
}

static bool
bm25_merge_all(Relation index)
{
	bool		didwork = false;
	int			guard;

	/*
	 * Try a parallel merge first (unless already inside a parallel operation,
	 * e.g. the parallel build leader -- no nested parallelism).  It compacts
	 * the sources into (workers+1) groups in one parallel pass; the serial
	 * loop below then finishes to a single segment.
	 *
	 * NB: iterating the parallel pass to one segment was measured WORSE at 2M
	 * (each pass rewrites data -> write amplification) and did not cut the
	 * tail: the final reduction is the write of ONE multi-GB output segment by
	 * a single backend, which no group-partition scheme parallelizes.  The
	 * merge tail is a single-output-write cost, not a parallelism-partition
	 * one -- see DEFERRED.md (codec / streamed-write direction).
	 */
	if (!IsInParallelMode() && max_parallel_maintenance_workers > 0)
	{
		int			request = Min(max_parallel_maintenance_workers,
								 max_parallel_workers);

		if (request > 0 && bm25_merge_all_parallel(index, request))
			didwork = true;
	}

	for (guard = 0; guard < BM25_MAX_SEGMENTS; guard++)
	{
		BM25MetaPageData meta;
		uint32		sel[BM25_MAX_SEGMENTS];
		uint32		nsel = 0;
		uint32		i;

		{
			Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);

			LockBuffer(mb, BUFFER_LOCK_SHARE);
			memcpy(&meta, BM25PageGetMeta(BufferGetPage(mb)), sizeof(meta));
			UnlockReleaseBuffer(mb);
		}
		if (meta.nsegments <= 1)
			break;				/* already optimal */

		/* select every populated segment */
		for (i = 0; i < meta.nsegments; i++)
			if (meta.segs[i].dictstart != InvalidBlockNumber)
				sel[nsel++] = i;
		if (nsel <= 1)
			break;

		if (!bm25_merge_selected(index, sel, nsel))
			break;				/* directory changed underneath; stop */
		didwork = true;
	}
	return didwork;
}

/*
 * Full-compaction with tail truncation, for VACUUM FULL / an explicit
 * fts_vacuum().  Merge every live segment into one, biasing allocation toward
 * the lowest free blocks so live pages pack at the front; then truncate the
 * contiguous run of free blocks at the end of the file back to the OS.  This
 * is what reclaims the physical bloat left by ordinary merges (which recycle
 * freed pages to the FSM for later reuse but never shrink the relation).
 *
 * Single-writer only (holds a lock that excludes concurrent writers, e.g.
 * VACUUM's ShareUpdateExclusiveLock or CIC's AccessExclusiveLock).
 */
static bool
bm25_vacuum_compact(Relation index)
{
	BlockNumber nblocks;
	BlockNumber blk;
	BlockNumber truncpoint;
	bool		didwork = false;

	/*
	 * Compact to a single segment, reusing the lowest free blocks first so the
	 * merged output lands at the front of the file.  Do NOT use the parallel
	 * merge here: the low-page allocator is a single backend-scoped hint, and
	 * VACUUM wants a deterministic front-packed layout.
	 */
	bm25_alloc_begin(index);
	PG_TRY();
	{
		int			guard;

		/*
		 * First, always REWRITE the current live segments through the low-page
		 * allocator -- even a single segment -- so their live pages relocate to
		 * the front of the file and the stale post-build/-merge pages become a
		 * contiguous free tail.  Without this, an index that is already nseg=1
		 * (e.g. straight after a build that merged to one) keeps its dead pages
		 * interleaved and nothing is truncatable.
		 */
		{
			BM25MetaPageData meta;
			uint32		sel[BM25_MAX_SEGMENTS];
			uint32		nsel = 0;
			uint32		i;
			Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);

			LockBuffer(mb, BUFFER_LOCK_SHARE);
			memcpy(&meta, BM25PageGetMeta(BufferGetPage(mb)), sizeof(meta));
			UnlockReleaseBuffer(mb);
			for (i = 0; i < meta.nsegments; i++)
				if (meta.segs[i].dictstart != InvalidBlockNumber)
					sel[nsel++] = i;
			if (nsel >= 1 && bm25_merge_selected(index, sel, nsel))
				didwork = true;
		}

		for (guard = 0; guard < BM25_MAX_SEGMENTS; guard++)
		{
			BM25MetaPageData meta;
			uint32		sel[BM25_MAX_SEGMENTS];
			uint32		nsel = 0;
			uint32		i;
			Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);

			LockBuffer(mb, BUFFER_LOCK_SHARE);
			memcpy(&meta, BM25PageGetMeta(BufferGetPage(mb)), sizeof(meta));
			UnlockReleaseBuffer(mb);
			if (meta.nsegments <= 1)
				break;
			for (i = 0; i < meta.nsegments; i++)
				if (meta.segs[i].dictstart != InvalidBlockNumber)
					sel[nsel++] = i;
			if (nsel <= 1)
				break;
			if (!bm25_merge_selected(index, sel, nsel))
				break;
			didwork = true;
		}
	}
	PG_FINALLY();
	{
		bm25_alloc_end();
	}
	PG_END_TRY();

	/* Make freed pages visible in the FSM, then find the free tail. */
	IndexFreeSpaceMapVacuum(index);
	nblocks = RelationGetNumberOfBlocks(index);
	truncpoint = nblocks;
	for (blk = nblocks; blk > 1; blk--)
	{
		if (GetRecordedFreeSpace(index, blk - 1) >= BLCKSZ / 2)
			truncpoint = blk - 1;	/* free -> part of the truncatable tail */
		else
			break;				/* first live block from the end; stop */
	}

	if (truncpoint < nblocks)
	{
		/* drop the FSM entries for the pages we are about to remove, then
		 * truncate the relation to release the space to the OS */
		FreeSpaceMapVacuumRange(index, truncpoint, nblocks);
		RelationTruncate(index, truncpoint);
		didwork = true;
	}
	return didwork;
}

static void
bm25_merge_segments(Relation index)
{
	int			guard;

	/*
	 * Repeatedly merge one qualifying size-tier until no tier has enough
	 * segments and the total count is within budget.  The guard bounds the loop
	 * (each successful merge reduces nsegments).
	 */
	for (guard = 0; guard < BM25_MAX_SEGMENTS; guard++)
	{
		BM25MetaPageData meta;
		MergeCand	cand[BM25_MAX_SEGMENTS];
		uint32		sel[BM25_MAX_SEGMENTS];
		uint32		nsel = 0;
		uint32		i;

		{
			Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);

			LockBuffer(mb, BUFFER_LOCK_SHARE);
			memcpy(&meta, BM25PageGetMeta(BufferGetPage(mb)), sizeof(meta));
			UnlockReleaseBuffer(mb);
		}
		if (meta.nsegments <= 1)
			return;

		/* candidates sorted by live size (ndocs - ndeleted) */
		for (i = 0; i < meta.nsegments; i++)
		{
			cand[i].idx = i;
			cand[i].size = meta.segs[i].ndocs - meta.segs[i].ndeleted;
			if (cand[i].size < 1)
				cand[i].size = 1;
		}
		qsort(cand, meta.nsegments, sizeof(MergeCand), cmp_mergecand);

		/* longest run of same-tier segments from the smallest: each within
		 * BM25_MERGE_SIZE_FACTOR of the run's smallest member */
		{
			double		base = cand[0].size;
			uint32		run = 0;

			for (i = 0; i < meta.nsegments; i++)
			{
				if (cand[i].size <= base * BM25_MERGE_SIZE_FACTOR)
					run++;
				else
					break;
			}
			/* merge this tier if large enough, or if we simply have too many
			 * segments overall (force progress toward the budget) */
			if (run >= BM25_MERGE_TIER_MIN ||
				(meta.nsegments > BM25_MERGE_THRESHOLD && run >= 2))
			{
				for (i = 0; i < run; i++)
					sel[nsel++] = cand[i].idx;
			}
		}

		if (nsel < 2)
			return;				/* no tier worth merging */
		if (!bm25_merge_selected(index, sel, nsel))
			return;				/* directory changed underneath */
	}
}

/* ---- parallel index build (level 1: parallel heap scan + per-worker segment
 * flush; the leader merges the workers' segments at the end) ---- */

#define PARALLEL_KEY_BM25_SHARED		UINT64CONST(0xB250000000000001)
#define PARALLEL_KEY_QUERY_TEXT			UINT64CONST(0xB250000000000002)
#define PARALLEL_KEY_WAL_USAGE			UINT64CONST(0xB250000000000003)
#define PARALLEL_KEY_BUFFER_USAGE		UINT64CONST(0xB250000000000004)

/*
 * Shared state for a parallel bm25 build, in the DSM segment.  Workers write
 * their own segments straight into the index (segments are self-contained and
 * appended to the metapage under its exclusive lock), so unlike a btree build
 * there is no central sort or result hand-off -- the only shared state is the
 * parallel table scan and a done-counter.
 */
typedef struct BM25Shared
{
	Oid			heaprelid;
	Oid			indexrelid;
	bool		isconcurrent;
	ConditionVariable workersdonecv;
	slock_t		mutex;
	int			nparticipantsdone;
	double		reltuples;
	/* ParallelTableScanDescData follows (alignment: allocated separately) */
}			BM25Shared;

#define ParallelTableScanFromBM25Shared(shared) \
	(ParallelTableScanDesc) ((char *) (shared) + BUFFERALIGN(sizeof(BM25Shared)))

typedef struct BM25Leader
{
	ParallelContext *pcxt;
	int			nparticipanttuplesorts;
	BM25Shared *bm25shared;
	Snapshot	snapshot;
	BufferUsage *bufferusage;
	WalUsage   *walusage;
}			BM25Leader;

/*
 * Run the heap scan (serial or, if pscan != NULL, a parallel slice) building
 * segments into `index`.  Returns the number of heap tuples this participant
 * saw.  Flushing the residual terms is left to the caller so the leader can
 * account the total before the final merge.
 */
static double
bm25_scan_and_build(Relation heap, Relation index, IndexInfo *indexInfo,
					BM25BuildState *bs, ParallelTableScanDesc pscan)
{
	TableScanDesc scan = NULL;

	if (pscan != NULL)
#if PG_VERSION_NUM >= 180000
		scan = table_beginscan_parallel(heap, pscan, SO_NONE);
#else
		scan = table_beginscan_parallel(heap, pscan);
#endif
	return table_index_build_scan(heap, index, indexInfo, true, true,
								  bm25_build_callback, (void *) bs, scan);
}

/*
 * Worker entry point (registered as "pg_fts"/"bm25_parallel_build_main").
 */
PGDLLEXPORT void bm25_parallel_build_main(dsm_segment *seg, shm_toc *toc);

void
bm25_parallel_build_main(dsm_segment *seg, shm_toc *toc)
{
	BM25Shared *bm25shared;
	Relation	heap;
	Relation	index;
	IndexInfo  *indexInfo;
	ParallelTableScanDesc pscan;
	BM25BuildState bs;
	LOCKMODE	heapLockmode;
	LOCKMODE	indexLockmode;
	double		reltuples;
	char	   *sharedquery;
	BufferUsage *bufferusage;
	WalUsage   *walusage;

	bm25shared = (BM25Shared *) shm_toc_lookup(toc, PARALLEL_KEY_BM25_SHARED, false);

	sharedquery = shm_toc_lookup(toc, PARALLEL_KEY_QUERY_TEXT, true);
	debug_query_string = sharedquery;

	if (!bm25shared->isconcurrent)
	{
		heapLockmode = ShareLock;
		indexLockmode = AccessExclusiveLock;
	}
	else
	{
		heapLockmode = ShareUpdateExclusiveLock;
		indexLockmode = RowExclusiveLock;
	}

	heap = table_open(bm25shared->heaprelid, heapLockmode);
	index = index_open(bm25shared->indexrelid, indexLockmode);
	indexInfo = BuildIndexInfo(index);
	indexInfo->ii_Concurrent = bm25shared->isconcurrent;

	/* report buffer/WAL usage so EXPLAIN ANALYZE etc. account worker I/O */
	bufferusage = shm_toc_lookup(toc, PARALLEL_KEY_BUFFER_USAGE, false);
	walusage = shm_toc_lookup(toc, PARALLEL_KEY_WAL_USAGE, false);
	InstrStartParallelQuery();

	pscan = ParallelTableScanFromBM25Shared(bm25shared);

	bs.ctx = AllocSetContextCreate(CurrentMemoryContext, "bm25 parallel worker",
								   ALLOCSET_DEFAULT_SIZES);
	bs.terms = NULL;
	bs.nterms = 0;
	bs.maxterms = 0;
	bs.ndocs = 0;
	bs.sumdoclen = 0;
	bm25_build_ht_init(&bs);
	reltuples = bm25_scan_and_build(heap, index, indexInfo, &bs, pscan);
	bm25_build_flush_segment(index, &bs);	/* worker's residual -> a segment */
	MemoryContextDelete(bs.ctx);

	InstrEndParallelQuery(&bufferusage[ParallelWorkerNumber],
						  &walusage[ParallelWorkerNumber]);

	/* report done + this worker's tuple count */
	SpinLockAcquire(&bm25shared->mutex);
	bm25shared->nparticipantsdone++;
	bm25shared->reltuples += reltuples;
	SpinLockRelease(&bm25shared->mutex);
	ConditionVariableSignal(&bm25shared->workersdonecv);

	index_close(index, indexLockmode);
	table_close(heap, heapLockmode);
}

/*
 * Set up the parallel context, DSM shared state, and launch workers.  Returns
 * the leader struct, or NULL if no workers could be launched (fall back to a
 * serial build).
 */
static BM25Leader *
bm25_begin_parallel(Relation heap, Relation index, bool isconcurrent,
					int request)
{
	ParallelContext *pcxt;
	Snapshot	snapshot;
	Size		estbm25shared;
	Size		estscan;
	BM25Shared *bm25shared;
	ParallelTableScanDesc pscan;
	BM25Leader *bm25leader;
	BufferUsage *bufferusage;
	WalUsage   *walusage;
	char	   *sharedquery;
	int			querylen;
	bool		leaderparticipates = true;

	EnterParallelMode();
	Assert(request > 0);
	pcxt = CreateParallelContext("pg_fts", "bm25_parallel_build_main", request);

	if (!isconcurrent)
		snapshot = SnapshotAny;
	else
		snapshot = RegisterSnapshot(GetTransactionSnapshot());

	estbm25shared = BUFFERALIGN(sizeof(BM25Shared));
	estscan = table_parallelscan_estimate(heap, snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, estbm25shared + estscan);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* query text for worker debug/reporting */
	if (debug_query_string)
	{
		querylen = strlen(debug_query_string);
		shm_toc_estimate_chunk(&pcxt->estimator, querylen + 1);
		shm_toc_estimate_keys(&pcxt->estimator, 1);
	}
	else
		querylen = 0;

	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	InitializeParallelDSM(pcxt);

	if (pcxt->seg == NULL)
	{
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return NULL;
	}

	bm25shared = (BM25Shared *) shm_toc_allocate(pcxt->toc,
												 estbm25shared + estscan);
	bm25shared->heaprelid = RelationGetRelid(heap);
	bm25shared->indexrelid = RelationGetRelid(index);
	bm25shared->isconcurrent = isconcurrent;
	bm25shared->nparticipantsdone = 0;
	bm25shared->reltuples = 0.0;
	ConditionVariableInit(&bm25shared->workersdonecv);
	SpinLockInit(&bm25shared->mutex);

	pscan = ParallelTableScanFromBM25Shared(bm25shared);
	table_parallelscan_initialize(heap, pscan, snapshot);
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BM25_SHARED, bm25shared);

	if (debug_query_string)
	{
		sharedquery = (char *) shm_toc_allocate(pcxt->toc, querylen + 1);
		memcpy(sharedquery, debug_query_string, querylen + 1);
		shm_toc_insert(pcxt->toc, PARALLEL_KEY_QUERY_TEXT, sharedquery);
	}

	bufferusage = shm_toc_allocate(pcxt->toc,
								   mul_size(sizeof(BufferUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_BUFFER_USAGE, bufferusage);
	walusage = shm_toc_allocate(pcxt->toc,
								mul_size(sizeof(WalUsage), pcxt->nworkers));
	shm_toc_insert(pcxt->toc, PARALLEL_KEY_WAL_USAGE, walusage);

	LaunchParallelWorkers(pcxt);

	if (pcxt->nworkers_launched == 0)
	{
		/* no workers actually started; caller will do a serial build */
		WaitForParallelWorkersToFinish(pcxt);
		if (IsMVCCSnapshot(snapshot))
			UnregisterSnapshot(snapshot);
		DestroyParallelContext(pcxt);
		ExitParallelMode();
		return NULL;
	}

	bm25leader = (BM25Leader *) palloc0(sizeof(BM25Leader));
	bm25leader->pcxt = pcxt;
	bm25leader->nparticipanttuplesorts = pcxt->nworkers_launched;
	if (leaderparticipates)
		bm25leader->nparticipanttuplesorts++;
	bm25leader->bm25shared = bm25shared;
	bm25leader->snapshot = snapshot;
	bm25leader->bufferusage = bufferusage;
	bm25leader->walusage = walusage;
	return bm25leader;
}

/*
 * Wait for all workers to finish, accumulate their I/O stats + tuple count,
 * and tear down.  Returns the total heap tuples the workers scanned (read from
 * the DSM before it is unmapped).
 */
static double
bm25_end_parallel(BM25Leader *bm25leader)
{
	int			i;
	double		worker_tuples;

	WaitForParallelWorkersToFinish(bm25leader->pcxt);

	for (i = 0; i < bm25leader->pcxt->nworkers_launched; i++)
		InstrAccumParallelQuery(&bm25leader->bufferusage[i], &bm25leader->walusage[i]);

	/* read the workers' accumulated tuple count while the DSM is still mapped */
	worker_tuples = bm25leader->bm25shared->reltuples;

	if (IsMVCCSnapshot(bm25leader->snapshot))
		UnregisterSnapshot(bm25leader->snapshot);
	DestroyParallelContext(bm25leader->pcxt);
	ExitParallelMode();
	return worker_tuples;
}

static IndexBuildResult *
bm25_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	BM25BuildState bs;
	double		reltuples;
	BM25Leader *bm25leader = NULL;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	/* metapage must be block 0 -- write it before workers or the scan touch it */
	bm25_init_metapage(index);

	/* Try a parallel build if the planner requested workers. */
	if (indexInfo->ii_ParallelWorkers > 0)
		bm25leader = bm25_begin_parallel(heap, index, indexInfo->ii_Concurrent,
										 indexInfo->ii_ParallelWorkers);

	bs.ctx = AllocSetContextCreate(CurrentMemoryContext, "bm25 build",
								   ALLOCSET_DEFAULT_SIZES);
	bs.terms = NULL;
	bs.nterms = 0;
	bs.maxterms = 0;
	bs.ndocs = 0;
	bs.sumdoclen = 0;
	bm25_build_ht_init(&bs);

	if (bm25leader != NULL)
	{
		/*
		 * Parallel build: the leader also scans a slice (leaderparticipates),
		 * using the same shared parallel scan the workers use, and flushes its
		 * residual as a segment.  Workers write their own segments directly.
		 */
		ParallelTableScanDesc pscan =
			ParallelTableScanFromBM25Shared(bm25leader->bm25shared);

		reltuples = bm25_scan_and_build(heap, index, indexInfo, &bs, pscan);
		bm25_build_flush_segment(index, &bs);

		/* add the workers' tuple counts BEFORE tearing down the DSM */
		reltuples += bm25_end_parallel(bm25leader);

		/*
		 * Compact the participants' segments into an optimal single segment.
		 * bm25_end_parallel() has exited parallel mode, so bm25_merge_all() can
		 * itself run the PARALLEL merge (workers merge disjoint segment groups),
		 * making the compaction fast rather than the single-threaded O(index)
		 * tail that a naive build-time merge would be.  This keeps a freshly
		 * built (or REINDEXed) index optimal at first query -- a multi-segment
		 * index makes ranked scans traverse every segment's postings, which
		 * regresses common-term ranked latency.
		 */
		bm25_merge_all(index);
	}
	else
	{
		/* Serial build. */
		reltuples = bm25_scan_and_build(heap, index, indexInfo, &bs, NULL);
		bm25_build_flush_segment(index, &bs);

		/*
		 * Compact to a single optimal segment.  A serial build makes few
		 * segments (budget-triggered flushes + the residual), and the tiered
		 * bm25_merge_segments deliberately leaves same-size tiers -- which would
		 * leave a multi-segment index and regress ranked scans.  bm25_merge_all
		 * finishes to one segment (and uses the parallel merge if workers are
		 * available, since we are not in parallel mode here).
		 */
		bm25_merge_all(index);
	}

	MemoryContextDelete(bs.ctx);

	result = (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = reltuples;
	return result;
}

static void
bm25_buildempty(Relation index)
{
	bm25_init_metapage(index);
}

/*
 * Index one oversized document (its analyzed ftsdoc does not fit on a single
 * pending page) directly as its own one-document segment, bypassing the
 * verbatim pending buffer.  Segment posting storage is a chain of FOR-packed
 * pages with no per-document size limit, so arbitrarily large documents (e.g.
 * long Wikipedia articles) can be indexed.  Rare, so building a whole segment
 * per such document is acceptable.
 */
static void
bm25_insert_oversized_as_segment(Relation index, FtsDoc doc, ItemPointer tid)
{
	BM25BuildState bs;
	FtsTermEntry *entries = FTS_DOC_ENTRIES(doc);
	uint32		j;

	bs.ctx = AllocSetContextCreate(CurrentMemoryContext, "bm25 oversized",
								   ALLOCSET_DEFAULT_SIZES);
	bs.terms = NULL;
	bs.nterms = 0;
	bs.maxterms = 0;
	bs.ndocs = 0;
	bs.sumdoclen = 0;
	bm25_build_ht_init(&bs);

	{
		MemoryContext old = MemoryContextSwitchTo(bs.ctx);

		for (j = 0; j < doc->nterms; j++)
			add_posting(&bs, FTS_DOC_TERMTEXT(doc, &entries[j]), entries[j].len,
						tid, entries[j].tf, doc->doclen);
		bs.ndocs = 1.0;
		bs.sumdoclen = doc->doclen;
		MemoryContextSwitchTo(old);
	}

	/* write the one-doc segment (updates corpus N/sumdoclen via add_segment) */
	bm25_build_flush_segment(index, &bs);
	MemoryContextDelete(bs.ctx);

	/*
	 * A bulk INSERT/UPDATE of many oversized documents would create one
	 * segment each and could approach BM25_MAX_SEGMENTS before the next VACUUM
	 * gets a chance to merge.  Coalesce eagerly once the count climbs, so the
	 * segment directory never overflows on a write-heavy oversized workload.
	 */
	{
		BM25MetaPageData meta;
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);

		LockBuffer(mb, BUFFER_LOCK_SHARE);
		memcpy(&meta, BM25PageGetMeta(BufferGetPage(mb)), sizeof(meta));
		UnlockReleaseBuffer(mb);
		if (meta.nsegments >= BM25_MAX_SEGMENTS - 16)
			bm25_merge_segments(index);
	}
}

/*
 * aminsert: append the new document to the pending list.
 *
 * The document is stored verbatim (its ftsdoc bytes) on a chain of pending
 * pages and is searched directly at scan time, so newly inserted rows are
 * immediately visible to @@@ without a REINDEX.  The metapage N and sum(doclen)
 * are updated so BM25 length-normalization stays correct; per-term df in the
 * dictionary is not updated until a merge (REINDEX), matching GIN fastupdate's
 * documented staleness.
 */
static bool
bm25_insert(Relation index, Datum *values, bool *isnull,
			ItemPointer ht_ctid, Relation heapRel,
			IndexUniqueCheck checkUnique, bool indexUnchanged,
			IndexInfo *indexInfo)
{
	FtsDoc		doc;
	Size		doclen;
	Size		need;
	Buffer		metabuf;
	GenericXLogState *state;
	Page		metapage;
	BM25MetaPageData *meta;
	BlockNumber tailblk;
	Buffer		tailbuf;
	Page		tailpage;
	bool		appended = false;

	if (isnull[0])
		return false;

	doc = (FtsDoc) PG_DETOAST_DATUM(values[0]);
	doclen = VARSIZE(doc);
	need = MAXALIGN(sizeof(BM25PendingItem) + doclen);

	if (need > BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - MAXALIGN(sizeof(BM25PageOpaqueData)))
	{
		/* Too large for the verbatim pending buffer: index it directly as its
		 * own one-document segment (no per-doc size limit there). */
		bm25_insert_oversized_as_segment(index, doc, ht_ctid);
		return true;
	}

	/* Lock the metapage for the whole append (serializes inserters; a
	 * per-inserter fast path is a later optimization). */
	metabuf = ReadBuffer(index, BM25_METAPAGE_BLKNO);
	LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);
	metapage = BufferGetPage(metabuf);
	meta = BM25PageGetMeta(metapage);
	tailblk = meta->pendingtail;

	/* Try to append to the current tail page. */
	if (tailblk != InvalidBlockNumber)
	{
		tailbuf = ReadBuffer(index, tailblk);
		LockBuffer(tailbuf, BUFFER_LOCK_EXCLUSIVE);
		tailpage = BufferGetPage(tailbuf);
		if (((PageHeader) tailpage)->pd_lower + need <=
			BLCKSZ - MAXALIGN(sizeof(BM25PageOpaqueData)))
		{
			BM25PendingItem *pi;

			state = GenericXLogStart(index);
			tailpage = GenericXLogRegisterBuffer(state, tailbuf, 0);
			pi = (BM25PendingItem *) ((char *) tailpage +
									 ((PageHeader) tailpage)->pd_lower);
			pi->tid = *ht_ctid;
			pi->doclen = doclen;
			memcpy((char *) pi + sizeof(BM25PendingItem), doc, doclen);
			((PageHeader) tailpage)->pd_lower += need;
			metapage = GenericXLogRegisterBuffer(state, metabuf, 0);
			meta = BM25PageGetMeta(metapage);
			meta->ndocs += 1.0;
			meta->sumdoclen += doc->doclen;
			meta->npending += 1;
			GenericXLogFinish(state);
			appended = true;
		}
		if (!appended)
			UnlockReleaseBuffer(tailbuf);	/* re-read below as oldtail */
	}

	/* Need a fresh pending page (either none yet, or the tail is full). */
	if (!appended)
	{
		Buffer		newbuf = bm25_new_buffer(index);
		BlockNumber newblk = BufferGetBlockNumber(newbuf);
		BM25PendingItem *pi;

		state = GenericXLogStart(index);
		{
			Page		np = GenericXLogRegisterBuffer(state, newbuf,
													   GENERIC_XLOG_FULL_IMAGE);

			bm25_init_page(np, BM25_PENDING);
			pi = (BM25PendingItem *) ((char *) np +
									 ((PageHeader) np)->pd_lower);
			pi->tid = *ht_ctid;
			pi->doclen = doclen;
			memcpy((char *) pi + sizeof(BM25PendingItem), doc, doclen);
			((PageHeader) np)->pd_lower += need;
		}

		/* link previous tail (if any) to the new page */
		if (tailblk != InvalidBlockNumber)
		{
			Buffer		oldtail = ReadBuffer(index, tailblk);
			Page		op;

			LockBuffer(oldtail, BUFFER_LOCK_EXCLUSIVE);
			op = GenericXLogRegisterBuffer(state, oldtail, 0);
			BM25PageGetOpaque(op)->nextblk = newblk;
			metapage = GenericXLogRegisterBuffer(state, metabuf, 0);
			meta = BM25PageGetMeta(metapage);
			meta->pendingtail = newblk;
			meta->ndocs += 1.0;
			meta->sumdoclen += doc->doclen;
			meta->npending += 1;
			GenericXLogFinish(state);
			UnlockReleaseBuffer(oldtail);
		}
		else
		{
			metapage = GenericXLogRegisterBuffer(state, metabuf, 0);
			meta = BM25PageGetMeta(metapage);
			meta->pendinghead = newblk;
			meta->pendingtail = newblk;
			meta->ndocs += 1.0;
			meta->sumdoclen += doc->doclen;
			meta->npending += 1;
			GenericXLogFinish(state);
		}
		UnlockReleaseBuffer(newbuf);
	}
	else
		UnlockReleaseBuffer(tailbuf);

	UnlockReleaseBuffer(metabuf);
	return true;
}

/* ----- scan ----- */

#include "pg_fts_lev.c"
#include "pg_fts_am_scan.c"
#include "pg_fts_trgm_index.c"

/* ----- vacuum / flush / merge / cost / options ----- */

/*
 * Flush the pending write buffer into a NEW immutable segment.
 *
 * O(pending), not O(index): only the pending documents are folded into a fresh
 * segment appended to the directory.  (The old monolithic design re-read and
 * rewrote the entire index on every merge -- O(index) and quadratic under
 * steady inserts.)  Pending pages are then recycled to the FSM.  Tiered
 * compaction of many small segments is a separate operation.  Returns true if
 * a flush happened.
 */
static bool
bm25_flush_pending(Relation index)
{
	BM25MetaPageData meta;
	BM25BuildState bs;
	BM25SegMeta seg;
	BlockNumber blk;

	{
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);

		LockBuffer(mb, BUFFER_LOCK_SHARE);
		memcpy(&meta, BM25PageGetMeta(BufferGetPage(mb)), sizeof(meta));
		UnlockReleaseBuffer(mb);
	}
	if (meta.npending == 0)
		return false;

	bs.ctx = AllocSetContextCreate(CurrentMemoryContext, "bm25 flush",
								   ALLOCSET_DEFAULT_SIZES);
	bs.terms = NULL;
	bs.nterms = 0;
	bs.maxterms = 0;
	bs.ndocs = 0;
	bs.sumdoclen = 0;
	{
		HASHCTL		ctl;

		ctl.keysize = sizeof(TermKey);
		ctl.entrysize = sizeof(TermHashEntry);
		ctl.hcxt = bs.ctx;
		build_ht = hash_create("bm25 flush terms", 1024, &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/* fold only the pending documents into the build state */
	blk = meta.pendinghead;
	while (blk != InvalidBlockNumber)
	{
		Buffer		buffer = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;
		MemoryContext old = MemoryContextSwitchTo(bs.ctx);

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;
		while (ptr < end)
		{
			BM25PendingItem *pi = (BM25PendingItem *) ptr;
			FtsDoc		pdoc = (FtsDoc) ((char *) pi + sizeof(BM25PendingItem));
			FtsTermEntry *entries = FTS_DOC_ENTRIES(pdoc);
			uint32		j;

			for (j = 0; j < pdoc->nterms; j++)
				add_posting(&bs, FTS_DOC_TERMTEXT(pdoc, &entries[j]),
							entries[j].len, &pi->tid, entries[j].tf,
							pdoc->doclen);
			bs.ndocs += 1.0;
			bs.sumdoclen += pdoc->doclen;
			ptr += MAXALIGN(sizeof(BM25PendingItem) + pi->doclen);
		}
		UnlockReleaseBuffer(buffer);
		MemoryContextSwitchTo(old);
		blk = next;
	}

	if (bs.nterms > 1)
		qsort(bs.terms, bs.nterms, sizeof(BuildTerm), cmp_buildterm);

	bm25_write_segment(index, &bs, &seg);
	bm25_meta_add_segment(index, &seg);

	/*
	 * Clear the pending list.  Pending docs were already counted into the
	 * corpus totals at insert time; add_segment counted them again, so subtract
	 * the segment's contribution to avoid a double count.
	 */
	{
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);
		GenericXLogState *state;
		Page		mp;
		BM25MetaPageData *m;

		LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		mp = GenericXLogRegisterBuffer(state, mb, 0);
		m = BM25PageGetMeta(mp);
		m->ndocs -= seg.ndocs;
		m->sumdoclen -= seg.sumdoclen;
		m->pendinghead = InvalidBlockNumber;
		m->pendingtail = InvalidBlockNumber;
		m->npending = 0;
		GenericXLogFinish(state);
		UnlockReleaseBuffer(mb);
	}

	/* recycle the old pending pages */
	blk = meta.pendinghead;
	while (blk != InvalidBlockNumber)
	{
		Buffer		buf = ReadBuffer(index, blk);
		BlockNumber next;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		next = BM25PageGetOpaque(BufferGetPage(buf))->nextblk;
		UnlockReleaseBuffer(buf);
		RecordFreeIndexPage(index, blk);
		blk = next;
	}
	IndexFreeSpaceMapVacuum(index);

	MemoryContextDelete(bs.ctx);

	/* keep the segment count bounded (query cost is O(nsegments) per term) */
	bm25_merge_segments(index);
	return true;
}

/*
 * Collect the distinct docids present in a segment into a sparsemap (the
 * segment's docid "universe").  Used by bulkdelete to enumerate the TIDs the
 * vacuum callback must be asked about.
 */
static sm_t *
bm25_segment_docids(Relation index, const BM25SegMeta *seg)
{
	sm_t	   *seen = sm_create(256);
	BlockNumber blk = seg->dictstart;

	if (seen == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory building bm25 tombstone map")));

	while (blk != InvalidBlockNumber)
	{
		Buffer		buffer = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;
		while (ptr < end)
		{
			BM25DictEntry *de = (BM25DictEntry *) ptr;
			Size		esize = MAXALIGN(offsetof(BM25DictEntry, term) + de->termlen);
			BM25Posting *post;
			int			np,
						k;

			np = bm25_decode_term(index, de->firstposting, de->firstoffset,
								  de->df, &post, NULL);
			for (k = 0; k < np; k++)
				sm_add_grow(&seen, bm25_tid_to_docid(&post[k].tid));
			pfree(post);
			ptr += esize;
		}
		UnlockReleaseBuffer(buffer);
		blk = next;
	}
	return seen;
}

/*
 * bm25_bulkdelete: VACUUM asks us, via `callback`, which of the TIDs in the
 * index refer to now-dead heap tuples.  Because postings live in immutable
 * segments, we cannot cheaply remove individual entries; instead we maintain a
 * per-segment livedocs TOMBSTONE bitmap (a docid sparsemap of deleted docs).
 * Scans and counts subtract tombstoned docids, and the tiered merge physically
 * drops them.  This is essential for correctness: the index-only
 * scan and fts_count paths trust the visibility map, so a vacuumed+reused heap
 * slot MUST NOT still be reported as a match -- the tombstone prevents that.
 */
static IndexBulkDeleteResult *
bm25_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				IndexBulkDeleteCallback callback, void *callback_state)
{
	Relation	index = info->index;
	BM25MetaPageData meta;
	uint32		s;
	int64		num_index_tuples = 0;
	int64		tuples_removed = 0;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	{
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);

		LockBuffer(mb, BUFFER_LOCK_SHARE);
		memcpy(&meta, BM25PageGetMeta(BufferGetPage(mb)), sizeof(meta));
		UnlockReleaseBuffer(mb);
	}

	for (s = 0; s < meta.nsegments; s++)
	{
		BM25SegMeta *sg = &meta.segs[s];
		sm_t	   *seen;
		sm_t	   *dead;
		sm_cursor_t cur = SM_CURSOR_INIT;
		uint64		v;
		uint32		ndead = 0;
		BlockNumber oldlivedocs;
		uint32		oldlen;

		if (sg->dictstart == InvalidBlockNumber)
			continue;

		seen = bm25_segment_docids(index, sg);
		dead = sm_create(256);
		if (dead == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory building bm25 tombstone map")));

		/* carry forward any docids already tombstoned in this segment */
		if (sg->livedocs != InvalidBlockNumber && sg->livedocslen > 0)
		{
			uint8	   *buf = bm25_read_blob(index, sg->livedocs, sg->livedocslen);
			sm_t		old;
			sm_cursor_t oc = SM_CURSOR_INIT;
			uint64		dv;

			sm_open(&old, (uint8_t *) buf, sg->livedocslen);
			for (dv = sm_next_member(&old, (uint64_t) -1, &oc);
				 dv != SM_IDX_MAX;
				 dv = sm_next_member(&old, dv, &oc))
			{
				sm_add_grow(&dead, dv);
				ndead++;
			}
			pfree(buf);
		}

		/* ask the callback about each live (not-yet-tombstoned) docid */
		for (v = sm_next_member(seen, (uint64_t) -1, &cur);
			 v != SM_IDX_MAX;
			 v = sm_next_member(seen, v, &cur))
		{
			ItemPointerData tid;
			sm_cursor_t ccur = SM_CURSOR_INIT;

			num_index_tuples++;
			if (sm_contains(dead, v, &ccur))
				continue;		/* already tombstoned */
			bm25_docid_to_tid(v, &tid);
			if (callback(&tid, callback_state))
			{
				sm_add_grow(&dead, v);
				ndead++;
				tuples_removed++;
			}
		}
		sm_free(seen);

		oldlivedocs = sg->livedocs;
		oldlen = sg->livedocslen;

		/* write the updated tombstone bitmap (if any) and patch the metapage */
		{
			BlockNumber newblk = InvalidBlockNumber;
			uint32		newlen = 0;

			if (ndead > 0)
			{
				newlen = (uint32) sm_get_size(dead);
				newblk = bm25_write_blob(index, (const uint8 *) sm_get_data(dead),
										 newlen);
			}
			sm_free(dead);


			{
				Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);
				GenericXLogState *st;
				Page		mp;
				BM25MetaPageData *m;

				LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
				st = GenericXLogStart(index);
				mp = GenericXLogRegisterBuffer(st, mb, 0);
				m = BM25PageGetMeta(mp);
				if (s < m->nsegments)
				{
					m->segs[s].livedocs = newblk;
					m->segs[s].livedocslen = newlen;
					m->segs[s].ndeleted = ndead;
				}
				GenericXLogFinish(st);
				UnlockReleaseBuffer(mb);
			}
		}

		/* recycle the previous tombstone blob pages */
		if (oldlivedocs != InvalidBlockNumber && oldlen > 0)
			bm25_free_chain(index, oldlivedocs);
	}

	/* refresh corpus N so IDF/avgdl reflect the deletions */
	if (tuples_removed > 0)
	{
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);
		GenericXLogState *st;
		Page		mp;
		BM25MetaPageData *m;
		uint32		i;
		double		nd = 0;

		LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
		st = GenericXLogStart(index);
		mp = GenericXLogRegisterBuffer(st, mb, 0);
		m = BM25PageGetMeta(mp);
		for (i = 0; i < m->nsegments; i++)
			nd += m->segs[i].ndocs - m->segs[i].ndeleted;
		m->ndocs = nd + m->npending;
		GenericXLogFinish(st);
		UnlockReleaseBuffer(mb);
	}

	stats->num_index_tuples = (double) (num_index_tuples - tuples_removed);
	stats->tuples_removed += (double) tuples_removed;
	stats->num_pages = RelationGetNumberOfBlocks(index);
	return stats;
}

static IndexBulkDeleteResult *
bm25_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/* Fold any pending documents into a new segment, then compact segments. */
	if (!info->analyze_only)
	{
		(void) bm25_flush_pending(info->index);
		bm25_merge_segments(info->index);

		/*
		 * If the relation carries substantial dead space (physical size well
		 * above the live pages), reclaim it: compact to one segment reusing
		 * low blocks, then truncate the free tail.  Gated so routine
		 * autovacuum does not pay a full rewrite every pass -- only when the
		 * free tail is a meaningful fraction of the file.
		 */
		{
			BlockNumber nblocks = RelationGetNumberOfBlocks(info->index);
			BlockNumber freeblks = 0;
			BlockNumber b;

			for (b = 1; b < nblocks; b++)
				if (GetRecordedFreeSpace(info->index, b) >= BLCKSZ / 2)
					freeblks++;
			/* reclaim when >= 25% of the file is free (bloated after merges) */
			if (nblocks > 16 && freeblks > nblocks / 4)
				(void) bm25_vacuum_compact(info->index);
		}
	}

	return stats;
}

PG_FUNCTION_INFO_V1(fts_merge);

/* fts_merge(regclass) -> bool : merge the pending list on demand */
Datum
fts_merge(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Relation	index;
	bool		done;

	index = index_open(indexoid, ShareUpdateExclusiveLock);
	if (index->rd_rel->relam != get_index_am_oid("bm25", true))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a bm25 index",
						RelationGetRelationName(index))));
	done = bm25_flush_pending(index);
	/*
	 * Also compact the segment directory to a single optimal segment.  This is
	 * what makes fts_merge() an explicit "optimize now": after a parallel build
	 * (which leaves the workers' segments unmerged for speed) or churn, one call
	 * yields a one-segment index.  The tiered auto-merge deliberately leaves
	 * several same-size segments, so it is not enough on its own here.
	 */
	if (bm25_merge_all(index))
		done = true;
	index_close(index, ShareUpdateExclusiveLock);

	PG_RETURN_BOOL(done);
}

PG_FUNCTION_INFO_V1(fts_vacuum);

/*
 * fts_vacuum(regclass) -> bool : on-demand full compaction with truncation.
 * Like fts_merge(), but after compacting to one segment it reclaims the dead
 * pages left by prior merges -- packing live pages at the front of the file
 * and truncating the free tail back to the OS.  Use this to shrink an index
 * that has grown physically larger than its live contents.
 */
Datum
fts_vacuum(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Relation	index;
	bool		done;

	index = index_open(indexoid, ShareUpdateExclusiveLock);
	if (index->rd_rel->relam != get_index_am_oid("bm25", true))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a bm25 index",
						RelationGetRelationName(index))));
	done = bm25_flush_pending(index);
	if (bm25_vacuum_compact(index))
		done = true;
	index_close(index, ShareUpdateExclusiveLock);

	PG_RETURN_BOOL(done);
}

static void
bm25_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				  Cost *indexStartupCost, Cost *indexTotalCost,
				  Selectivity *indexSelectivity, double *indexCorrelation,
				  double *indexPages)
{
	GenericCosts costs = {0};

	/* baseline: generic estimate gives selectivity, pages, and row counts */
	genericcostestimate(root, path, loop_count, &costs);

	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;

	if (path->indexorderbys != NIL)
	{
		/*
		 * Ordering scan (ORDER BY ftsdoc <=> ftsquery): the AM runs block-max
		 * WAND / MaxScore and, with a LIMIT pushed down, the executor pulls only
		 * about k best results -- work is sublinear in the match set, unlike a
		 * generic full index scan.  Price it as mostly a modest startup plus a
		 * small per-tuple cost, so the planner prefers the index (which honours
		 * the ORDER BY) over a seqscan + sort.  We deliberately keep this low
		 * but nonzero; the LIMIT is applied by the caller (limit_tuples), so a
		 * cheap-per-tuple total lets a small LIMIT win and a large one still
		 * scale.
		 */
		double		ntuples = costs.numIndexTuples;

		*indexStartupCost = costs.indexStartupCost + 2.0 * cpu_operator_cost;
		/* WAND touches ~log(N)*k blocks, not all matches: charge a fraction of
		 * a page fetch per matching tuple plus the per-tuple CPU */
		*indexTotalCost = *indexStartupCost +
			ntuples * (cpu_index_tuple_cost + cpu_operator_cost) +
			0.25 * costs.numIndexPages * costs.spc_random_page_cost;
	}
	else
	{
		/*
		 * Plain @@@ scan: the generic estimate (selectivity from clause
		 * selectivity, pages from the posting lists) is a reasonable model of
		 * decoding the matching TID sets, so use it as-is.
		 */
		*indexStartupCost = costs.indexStartupCost;
		*indexTotalCost = costs.indexTotalCost;
	}
}

static bytea *
bm25_options(Datum reloptions, bool validate)
{
	return NULL;
}

static bool
bm25_validate(Oid opclassoid)
{
	return true;
}

Datum
bm25handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 2;
	amroutine->amsupport = 0;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
#if PG_VERSION_NUM >= 180000
	amroutine->amcanhash = false;
	amroutine->amconsistentequality = false;
	amroutine->amconsistentordering = false;
#endif
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = false;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
#if PG_VERSION_NUM >= 170000
	amroutine->amcanbuildparallel = true;
#endif
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_NO_PARALLEL;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = bm25_build;
	amroutine->ambuildempty = bm25_buildempty;
	amroutine->aminsert = bm25_insert;
#if PG_VERSION_NUM >= 170000
	amroutine->aminsertcleanup = NULL;
#endif
	amroutine->ambulkdelete = bm25_bulkdelete;
	amroutine->amvacuumcleanup = bm25_vacuumcleanup;
	amroutine->amcanreturn = bm25_canreturn;
	amroutine->amcostestimate = bm25_costestimate;
#if PG_VERSION_NUM >= 180000
	amroutine->amgettreeheight = NULL;
#endif
	amroutine->amoptions = bm25_options;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = bm25_validate;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = bm25_beginscan;
	amroutine->amrescan = bm25_rescan;
	amroutine->amgettuple = bm25_gettuple;
	amroutine->amgetbitmap = bm25_getbitmap;
	amroutine->amendscan = bm25_endscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}
