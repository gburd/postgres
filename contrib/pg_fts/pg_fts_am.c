/*-------------------------------------------------------------------------
 *
 * pg_fts_am.c
 *		The "bm25" index access method for pg_fts.
 *
 * Stage 3 of pg_fts: a minimal but real inverted-index access method over an
 * ftsdoc column, answering the @@@ operator via a bitmap scan.  It also
 * maintains the corpus statistics BM25 needs (document count N, sum of
 * document lengths, per-term document frequency), so that later stages can
 * score index-only.
 *
 * On-disk layout (deliberately simple for the skeleton; the segmented,
 * merge-on-write design with block-max impacts described in the plan is a
 * later optimization):
 *
 *	 block 0            metapage: N, sum(doclen), nterms
 *	 dictionary pages   sorted (term -> first posting block, df) entries
 *	 posting pages      arrays of (ItemPointerData tid, uint32 tf), chained
 *
 * Because the structure is built once from a heap scan and is not updated in
 * place, aminsert triggers a note that a REINDEX is needed to reflect new
 * rows.  Incremental maintenance (a pending list + background merge) is a
 * later stage; this keeps the skeleton small and correct.  All page writes go
 * through GenericXLog, so the index is crash-safe and replicated without a
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
#include <math.h>
#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/pathnodes.h"
#include "nodes/tidbitmap.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
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
} BuildTerm;

typedef struct BM25BuildState
{
	MemoryContext ctx;
	BuildTerm  *terms;			/* sorted-on-flush; kept in a simple array */
	int			nterms;
	int			maxterms;
	/* term -> index lookup is linear-search-free via a sorted rebuild at end;
	 * for the skeleton we keep an unsorted array and sort once before writing */
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
	int			termidx;
} TermHashEntry;

static HTAB *build_ht;

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
	BuildTerm  *bt;

	make_termkey(&key, term, len);
	entry = (TermHashEntry *) hash_search(build_ht, &key, HASH_ENTER, &found);

	/* verify true equality against the stored BuildTerm on a hash hit */
	if (found)
	{
		bt = &bs->terms[entry->termidx];
		if (!(bt->len == len && memcmp(bt->term, term, len) == 0))
			found = false;		/* key collision on a truncated/padded key */
	}

	if (!found)
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

/* LEB128 unsigned varint encode; returns bytes written into buf */
static inline int
bm25_varint_encode(uint64 v, unsigned char *buf)
{
	int			n = 0;

	do
	{
		unsigned char byte = v & 0x7F;

		v >>= 7;
		if (v)
			byte |= 0x80;
		buf[n++] = byte;
	} while (v);
	return n;
}

/* decode one varint from buf, advancing *pos */
static inline uint64
bm25_varint_decode(const unsigned char *buf, int *pos)
{
	uint64		v = 0;
	int			shift = 0;
	unsigned char byte;

	do
	{
		byte = buf[(*pos)++];
		v |= (uint64) (byte & 0x7F) << shift;
		shift += 7;
	} while (byte & 0x80);
	return v;
}

/* worst-case encoded size of one posting (docid gap up to 48 bits + tf + doclen) */
#define BM25_MAX_POSTING_BYTES (10 + 5 + 5)

/*
 * Decode all postings on a page (across all its blocks) into a palloc'd array.
 * Returns the total count.  If blockmax != NULL, *blockmax is a palloc'd array
 * (one entry per posting) giving the max_tf of the 128-block that posting
 * belongs to -- this is what block-max WAND uses to skip whole blocks.
 */
static int
bm25_page_decode_bm(Page page, BM25Posting **out, uint32 **blockmax)
{
	char	   *pstart = (char *) PageGetContents(page);
	char	   *pend = (char *) page + ((PageHeader) page)->pd_lower;
	char	   *p = pstart;
	BM25Posting *posts = NULL;
	uint32	   *bmax = NULL;
	int			n = 0;
	int			cap = 0;

	while (p + sizeof(BM25BlockHdr) <= pend)
	{
		BM25BlockHdr *bh = (BM25BlockHdr *) p;
		const unsigned char *stream = (const unsigned char *) (bh + 1);
		uint64		docid = ((uint64) bh->first_docid_hi << 32) | bh->first_docid_lo;
		int			pos = 0;
		int			i;

		if (bh->count == 0)
			break;
		if (n + (int) bh->count > cap)
		{
			cap = Max(cap * 2, n + (int) bh->count);
			posts = posts ? repalloc(posts, cap * sizeof(BM25Posting))
				: palloc(cap * sizeof(BM25Posting));
			if (blockmax)
				bmax = bmax ? repalloc(bmax, cap * sizeof(uint32))
					: palloc(cap * sizeof(uint32));
		}
		for (i = 0; i < (int) bh->count; i++)
		{
			uint64		gap = bm25_varint_decode(stream, &pos);
			uint32		tf = (uint32) bm25_varint_decode(stream, &pos);
			uint32		doclen = (uint32) bm25_varint_decode(stream, &pos);

			docid += gap;		/* first posting has gap 0 from first_docid */
			bm25_docid_to_tid(docid, &posts[n].tid);
			posts[n].tf = tf;
			posts[n].doclen = doclen;
			if (bmax)
				bmax[n] = bh->max_tf;
			n++;
		}
		p = (char *) (bh + 1) + bh->bytelen;
		p = (char *) MAXALIGN(p);
	}
	if (posts == NULL)
		posts = palloc(sizeof(BM25Posting));
	*out = posts;
	if (blockmax)
		*blockmax = bmax ? bmax : palloc(sizeof(uint32));
	return n;
}

static int
bm25_page_decode(Page page, BM25Posting **out)
{
	return bm25_page_decode_bm(page, out, NULL);
}

/* ----- writing the index pages ----- */

static Buffer
bm25_new_buffer(Relation index)
{
	Buffer		buffer;

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

	buffer = ReadBuffer(index, P_NEW);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
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
 * Write all postings for one term into a chain of posting pages, returning the
 * first block.  Postings are sorted by docid and delta+varint encoded per page
 * (BM25PostingPageHdr + varint stream), which compresses the common case of
 * many clustered docids into a few bytes each.
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

static BlockNumber
bm25_write_postings(Relation index, BuildTerm *bt)
{
	BlockNumber first = InvalidBlockNumber;
	Buffer		buffer = InvalidBuffer;
	GenericXLogState *state = NULL;
	Page		page = NULL;
	BM25PostingSort *sorted;
	int			i;

	/* sort this term's postings by docid for delta encoding */
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

	/*
	 * Emit fixed-size blocks of up to BM25_BLOCK_SIZE postings.  Each block is
	 * encoded into a scratch buffer (so its bytelen is known), then placed on
	 * the current page if it fits, else a new page is chained.  A block never
	 * spans pages (worst case 128*20 bytes + header < BLCKSZ).
	 */
	i = 0;
	while (i < bt->nposts)
	{
		unsigned char scratch[BM25_BLOCK_SIZE * BM25_MAX_POSTING_BYTES];
		int			sclen = 0;
		uint32		blk_max_tf = 0;
		uint64		blk_first_docid = sorted[i].docid;
		uint64		prev_docid = sorted[i].docid;
		int			bcount = 0;
		char	   *pageend;
		Size		need;
		char	   *dst;
		BM25BlockHdr *bh;

		/* encode one block (first posting has gap 0 from blk_first_docid) */
		while (i < bt->nposts && bcount < BM25_BLOCK_SIZE)
		{
			uint64		gap = sorted[i].docid - prev_docid;

			sclen += bm25_varint_encode(gap, scratch + sclen);
			sclen += bm25_varint_encode((uint64) sorted[i].tf, scratch + sclen);
			sclen += bm25_varint_encode((uint64) sorted[i].doclen, scratch + sclen);
			if (sorted[i].tf > blk_max_tf)
				blk_max_tf = sorted[i].tf;
			prev_docid = sorted[i].docid;
			bcount++;
			i++;
		}

		need = MAXALIGN(sizeof(BM25BlockHdr) + sclen);

		/* need a page with room for this block? */
		if (buffer != InvalidBuffer)
		{
			pageend = (char *) page + BLCKSZ - MAXALIGN(sizeof(BM25PageOpaqueData));
			if ((char *) page + ((PageHeader) page)->pd_lower + need > pageend)
			{
				/* current page full: chain a new one */
				Buffer		next = bm25_new_buffer(index);
				BlockNumber nextblk = BufferGetBlockNumber(next);

				BM25PageGetOpaque(page)->nextblk = nextblk;
				GenericXLogFinish(state);
				UnlockReleaseBuffer(buffer);
				buffer = next;
				state = GenericXLogStart(index);
				page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
				bm25_init_page(page, BM25_POSTING);
			}
		}
		if (buffer == InvalidBuffer)
		{
			buffer = bm25_new_buffer(index);
			state = GenericXLogStart(index);
			page = GenericXLogRegisterBuffer(state, buffer, GENERIC_XLOG_FULL_IMAGE);
			bm25_init_page(page, BM25_POSTING);
			first = BufferGetBlockNumber(buffer);
		}

		/* place the block at pd_lower */
		dst = (char *) page + ((PageHeader) page)->pd_lower;
		bh = (BM25BlockHdr *) dst;
		bh->count = (uint32) bcount;
		bh->max_tf = blk_max_tf;
		bh->first_docid_hi = (uint32) (blk_first_docid >> 32);
		bh->first_docid_lo = (uint32) (blk_first_docid & 0xFFFFFFFF);
		bh->bytelen = (uint32) sclen;
		memcpy((char *) (bh + 1), scratch, sclen);
		((PageHeader) page)->pd_lower += need;
	}

	if (buffer != InvalidBuffer)
	{
		GenericXLogFinish(state);
		UnlockReleaseBuffer(buffer);
	}

	pfree(sorted);
	return first;
}

/*
 * Write the dictionary: sorted (term, df, firstposting) entries packed into a
 * chain of dictionary pages.  Returns the first dictionary block, and via
 * *indexstart the first page of the sparse block index (Invalid if empty).
 */
static BlockNumber
bm25_write_dictionary(Relation index, BM25BuildState *bs,
					  BlockNumber *postings, BlockNumber *indexstart)
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

/*
 * Write one immutable segment (dictionary + postings + trigram index) from a
 * populated build state, filling *seg.  The build state's terms must already
 * be sorted.  livedocs starts empty (no tombstones); basedocid is 0 (segments
 * share the global docid space via heap TIDs).
 */
static void
bm25_write_segment(Relation index, BM25BuildState *bs, BM25SegMeta *seg)
{
	BlockNumber *postings;
	int			i;

	postings = (BlockNumber *) palloc(Max(bs->nterms, 1) * sizeof(BlockNumber));
	for (i = 0; i < bs->nterms; i++)
		postings[i] = bm25_write_postings(index, &bs->terms[i]);

	MemSet(seg, 0, sizeof(BM25SegMeta));
	seg->dictstart = bm25_write_dictionary(index, bs, postings, &seg->dictindexstart);
	seg->trgmstart = bm25_write_trigrams(index, bs);
	seg->livedocs = InvalidBlockNumber;
	seg->ndocs = bs->ndocs;
	seg->sumdoclen = bs->sumdoclen;
	seg->nterms = bs->nterms;
	seg->ndeleted = 0;
	seg->basedocid = 0;
	pfree(postings);
}

/*
 * Append a segment descriptor to the metapage directory and fold its doc stats
 * into the corpus totals.  Errors if the directory is full (tiered merge keeps
 * the count small; a chained directory page is future work).
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
				 errmsg("bm25 index has too many segments; run VACUUM or REINDEX")));
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
			BlockNumber pblk = de->firstposting;

			while (pblk != InvalidBlockNumber)
			{
				Buffer		pb = ReadBuffer(index, pblk);
				Page		pp;
				BM25Posting *post;
				int			np,
							k;

				LockBuffer(pb, BUFFER_LOCK_SHARE);
				pp = BufferGetPage(pb);
				np = bm25_page_decode(pp, &post);
				for (k = 0; k < np; k++)
					add_posting(bs, de->term, de->termlen,
								&post[k].tid, post[k].tf, post[k].doclen);
				pfree(post);
				pblk = BM25PageGetOpaque(pp)->nextblk;
				UnlockReleaseBuffer(pb);
			}
			ptr += esize;
		}
		UnlockReleaseBuffer(buffer);
		MemoryContextSwitchTo(old);
		blk = next;
	}
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

	/* dictionary pages + each entry's posting chain */
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

			bm25_free_chain(index, de->firstposting);
			ptr += esize;
		}
		UnlockReleaseBuffer(buf);
		RecordFreeIndexPage(index, blk);
		blk = next;
	}

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
 * Size-tiered merge: repeatedly merge the smallest run of segments while the
 * total count exceeds a target, combining them into one new segment and
 * dropping the originals.  Bounds the segment count (query cost is O(nsegments)
 * per term), the Lucene TieredMergePolicy idea in miniature.  Called after a
 * flush and from VACUUM.  Merges the whole set into one when count > threshold;
 * a finer size-tiered policy is a later tuning knob.
 */
#define BM25_MERGE_THRESHOLD 8

static void
bm25_merge_segments(Relation index)
{
	BM25MetaPageData meta;
	BM25BuildState bs;
	BM25SegMeta newseg;
	BM25SegMeta oldsegs[BM25_MAX_SEGMENTS];
	uint32		nold;
	uint32		s;

	{
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);

		LockBuffer(mb, BUFFER_LOCK_SHARE);
		memcpy(&meta, BM25PageGetMeta(BufferGetPage(mb)), sizeof(meta));
		UnlockReleaseBuffer(mb);
	}
	if (meta.nsegments <= BM25_MERGE_THRESHOLD)
		return;

	memcpy(oldsegs, meta.segs, meta.nsegments * sizeof(BM25SegMeta));
	nold = meta.nsegments;

	bs.ctx = AllocSetContextCreate(CurrentMemoryContext, "bm25 merge segs",
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
		build_ht = hash_create("bm25 merge terms", 1024, &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	for (s = 0; s < nold; s++)
	{
		bs.sumdoclen += oldsegs[s].sumdoclen;
		bm25_read_segment_into(index, &oldsegs[s], &bs);
	}
	if (bs.nterms > 1)
		qsort(bs.terms, bs.nterms, sizeof(BuildTerm), cmp_buildterm);

	bm25_write_segment(index, &bs, &newseg);
	newseg.ndocs = bs.ndocs;
	newseg.sumdoclen = bs.sumdoclen;

	/* replace the whole directory with the single merged segment */
	{
		Buffer		mb = ReadBuffer(index, BM25_METAPAGE_BLKNO);
		GenericXLogState *state;
		Page		mp;
		BM25MetaPageData *m;

		LockBuffer(mb, BUFFER_LOCK_EXCLUSIVE);
		state = GenericXLogStart(index);
		mp = GenericXLogRegisterBuffer(state, mb, 0);
		m = BM25PageGetMeta(mp);
		/* only merge if no concurrent change grew the directory further; here
		 * we require the same nsegments we snapshotted (single-writer VACUUM/
		 * flush context makes this safe) */
		if (m->nsegments == nold)
		{
			m->segs[0] = newseg;
			m->nsegments = 1;
			/* corpus totals unchanged (same docs, minus tombstones already
			 * excluded via ndocs-ndeleted in read) */
			m->ndocs = newseg.ndocs;
			m->sumdoclen = newseg.sumdoclen;
			GenericXLogFinish(state);
			UnlockReleaseBuffer(mb);
			for (s = 0; s < nold; s++)
				bm25_free_segment(index, &oldsegs[s]);
			IndexFreeSpaceMapVacuum(index);
		}
		else
		{
			/* directory changed under us; abandon this merge (new segment leaks
			 * until next merge/REINDEX -- rare, single-writer path) */
			GenericXLogAbort(state);
			UnlockReleaseBuffer(mb);
		}
	}
	MemoryContextDelete(bs.ctx);
}

static IndexBuildResult *
bm25_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	BM25BuildState bs;
	double		reltuples;
	BM25SegMeta seg;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	bs.ctx = AllocSetContextCreate(CurrentMemoryContext, "bm25 build",
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
		build_ht = hash_create("bm25 build terms", 1024, &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/* metapage must be block 0 -- write it before any other page */
	bm25_init_metapage(index);

	reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
									   bm25_build_callback, (void *) &bs, NULL);

	/* sort terms so the dictionary is searchable */
	if (bs.nterms > 1)
		qsort(bs.terms, bs.nterms, sizeof(BuildTerm), cmp_buildterm);

	/* write the whole heap as one initial segment (a large CREATE INDEX could
	 * flush multiple segments to bound memory; that refinement is future work,
	 * but the segment plumbing is now in place for it). */
	if (bs.nterms > 0)
	{
		bm25_write_segment(index, &bs, &seg);
		bm25_meta_add_segment(index, &seg);
	}

	MemoryContextDelete(bs.ctx);

	result = (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = bs.nterms;
	return result;
}

static void
bm25_buildempty(Relation index)
{
	bm25_init_metapage(index);
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
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)),
				errmsg("ftsdoc too large for a bm25 pending page"));

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

static IndexBulkDeleteResult *
bm25_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				IndexBulkDeleteCallback callback, void *callback_state)
{
	/*
	 * Tombstoning of deleted TIDs is a later step; MVCC visibility on the heap
	 * recheck (@@@) and the fts_search fetch keeps results correct even though
	 * dead postings remain in segments until merge/REINDEX.
	 */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
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
	index_close(index, ShareUpdateExclusiveLock);

	PG_RETURN_BOOL(done);
}

static void
bm25_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				  Cost *indexStartupCost, Cost *indexTotalCost,
				  Selectivity *indexSelectivity, double *indexCorrelation,
				  double *indexPages)
{
	/* Delegate to the generic estimator; a WAND-aware estimate comes later. */
	GenericCosts costs = {0};

	genericcostestimate(root, path, loop_count, &costs);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
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
	amroutine->amcanhash = false;
	amroutine->amconsistentequality = false;
	amroutine->amconsistentordering = false;
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
	amroutine->amcanbuildparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_NO_PARALLEL;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = bm25_build;
	amroutine->ambuildempty = bm25_buildempty;
	amroutine->aminsert = bm25_insert;
	amroutine->aminsertcleanup = NULL;
	amroutine->ambulkdelete = bm25_bulkdelete;
	amroutine->amvacuumcleanup = bm25_vacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = bm25_costestimate;
	amroutine->amgettreeheight = NULL;
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
