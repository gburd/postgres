/*-------------------------------------------------------------------------
 *
 * pg_fts_am.h
 *		On-disk page layout for the bm25 index access method.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_am.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_FTS_AM_H
#define PG_FTS_AM_H

#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "storage/bufpage.h"
#include "storage/itemptr.h"

#define BM25_MAGIC			0x42324635	/* "B2F5" */
#define BM25_VERSION		2		/* v2: segmented layout */
#define BM25_METAPAGE_BLKNO	0

/* page opaque flags */
#define BM25_META			(1 << 0)
#define BM25_DICT			(1 << 1)
#define BM25_POSTING		(1 << 2)
#define BM25_PENDING		(1 << 3)
#define BM25_TRGM			(1 << 4)	/* trigram directory page */
#define BM25_TRGM_DATA		(1 << 5)	/* trigram sparsemap blob page */
#define BM25_LIVEDOCS		(1 << 6)	/* per-segment tombstone bitmap page */
#define BM25_DICTINDEX		(1 << 7)	/* sparse block index over dict pages */

typedef struct BM25PageOpaqueData
{
	uint16		flags;
	uint16		unused;
	BlockNumber nextblk;		/* next page in a dict/posting/pending chain */
} BM25PageOpaqueData;

typedef BM25PageOpaqueData *BM25PageOpaque;

#define BM25PageGetOpaque(page) \
	((BM25PageOpaque) PageGetSpecialPointer(page))

/*
 * A segment: an immutable, self-contained mini-index built from one flush of
 * the write buffer (or the merge of several segments).  The bm25 index is a set
 * of these plus a small pending write buffer.  Each segment has its own term
 * dictionary, posting lists, trigram index, and a live-docs tombstone bitmap;
 * deletes set a tombstone bit, and a background tiered merge rewrites groups of
 * segments dropping tombstoned docs.  This is the Lucene/Tantivy consensus
 * design; it replaces the old single monolithic structure whose in-memory
 * build OOMed and whose full-rewrite merge was O(index).
 */
typedef struct BM25SegMeta
{
	BlockNumber dictstart;		/* first dictionary page of this segment */
	BlockNumber trgmstart;		/* first trigram directory page, or Invalid */
	BlockNumber livedocs;		/* first live-docs tombstone page, or Invalid */
	double		ndocs;			/* documents in this segment (incl. tombstoned) */
	double		sumdoclen;		/* sum of doclen in this segment */
	uint32		nterms;			/* distinct terms in this segment */
	uint32		ndeleted;		/* tombstoned docs (for merge accounting) */
	uint32		livedocslen;	/* serialized size of the livedocs tombstone blob */
	BlockNumber dictindexstart; /* sparse block index over dict pages (Invalid = none) */
} BM25SegMeta;

#define BM25_MAX_SEGMENTS 128	/* fits the metapage (~6KB of ~8KB); the size-
										 * tiered merge keeps the live count far below
										 * this, so it is only a safety backstop */

typedef struct BM25MetaPageData
{
	uint32		magic;
	uint32		version;
	double		ndocs;			/* corpus N (all live segments + pending, minus tombstones) */
	double		sumdoclen;		/* corpus sum(doclen) -> avgdl */
	uint32		nsegments;		/* number of live segment descriptors */
	BlockNumber pendinghead;	/* first pending page, or InvalidBlockNumber */
	BlockNumber pendingtail;	/* last pending page, for O(1) append */
	uint32		npending;		/* number of pending (unmerged) documents */
	BM25SegMeta segs[BM25_MAX_SEGMENTS];
} BM25MetaPageData;

#define BM25PageGetMeta(page) \
	((BM25MetaPageData *) PageGetContents(page))

/* a dictionary entry; term text is inline, length termlen */
typedef struct BM25DictEntry
{
	uint32		termlen;
	uint32		df;				/* document frequency */
	uint32		max_tf;			/* max tf across postings (WAND impact bound) */
	uint32		firstoffset;	/* byte offset of the term's first block in firstposting */
	BlockNumber firstposting;	/* first posting page for this term */
	char		term[FLEXIBLE_ARRAY_MEMBER];
} BM25DictEntry;

/*
 * Sparse block index over a segment's dictionary pages: one entry per dict
 * page, recording that page's FIRST term and its block number.  Entries are in
 * term order (dict pages are written in term order), so a term lookup binary-
 * searches the (small) index to the one dict page that could hold the term,
 * then scans just that page -- O(log P + 1) instead of scanning all P dict
 * pages.  This is the same point-lookup complexity an FST gives; prefix/range
 * scans still walk the dict chain from the located page.
 */
typedef struct BM25DictIndexEntry
{
	BlockNumber blk;			/* dictionary page this entry points at */
	uint32		termlen;		/* length of that page's first term */
	char		term[FLEXIBLE_ARRAY_MEMBER];
} BM25DictIndexEntry;

/* a posting: which heap tuple, its term frequency, and the document length */
typedef struct BM25Posting
{
	ItemPointerData tid;
	uint32		tf;
	uint32		doclen;			/* |D|: total tokens in the document */
} BM25Posting;

/*
 * Posting pages hold one or more fixed-size BLOCKS of up to BM25_BLOCK_SIZE
 * postings (the Lucene/Tantivy 128-doc block design).  Each block is a
 * BM25BlockHdr followed by three FOR (frame-of-reference) bit-packed columns --
 * docid-gaps, tfs, doclens; docid gaps are relative to first_docid within the
 * block.  Per-block max_tf/min_doclen give block-max WAND a tight impact bound
 * (finer than per-page), and first_docid lets a cursor skip an entire block
 * whose docids are all below a target.  (The columns are already narrow -- tf
 * and doclen are small and docid gaps are tiny within a common term's dense
 * blocks -- so patched-FOR/PFOR outlier extraction was measured to save only
 * ~7% of the column bytes, under ~0.5% of the whole index, not worth the
 * hot-path decode complexity; plain FOR is kept.)
 */
#define BM25_BLOCK_SIZE 128

typedef struct BM25BlockHdr
{
	uint32		count;			/* postings in this block (<= BM25_BLOCK_SIZE) */
	uint32		max_tf;			/* max tf in this block (block-max WAND bound) */
	uint32		min_doclen;		/* min |D| in this block (tightens block-max WAND) */
	uint32		first_docid_hi;
	uint32		first_docid_lo;
	uint32		bytelen;		/* byte length of the varint stream that follows */
} BM25BlockHdr;

/*
 * A pending record: a not-yet-merged document stored verbatim on a pending
 * page.  The ftsdoc varlena follows the header inline (doclen bytes).  Pending
 * documents are searched directly at scan time and folded into a new segment by
 * a flush -- triggered by fts_merge() or automatically during VACUUM cleanup.
 */
typedef struct BM25PendingItem
{
	ItemPointerData tid;
	uint32		doclen;			/* byte length of the ftsdoc that follows */
	/* char ftsdoc[doclen] follows, MAXALIGN'd */
} BM25PendingItem;

/*
 * A trigram-index entry: a trigram hash and, inline, a serialized sparsemap of
 * the docids of documents containing at least one term with that trigram.  Used
 * to narrow fuzzy/regex candidates instead of scanning the whole index.
 *
 * The trigram index is inverted over the VOCABULARY, not the corpus: a trigram
 * maps to the set of dictionary term ordinals whose term contains it.  The
 * vocabulary is far smaller than the document set (Heaps' law), so these sets
 * are small and dense -- unlike docid sets, where a common trigram would cover
 * most of the corpus.  No trigrams are skipped, so the candidate union stays a
 * sound superset for fuzzy/regex.
 * Each entry is fixed-size; the term-ordinal sparsemap is a data-page blob.
 */
typedef struct BM25TrgmEntry
{
	uint32		trgm;			/* trigram hash */
	uint32		smlen;			/* serialized sparsemap length in bytes */
	BlockNumber firstdata;		/* first BM25_TRGM_DATA page of the term-ord set */
} BM25TrgmEntry;

/* scan functions (pg_fts_am_scan.c, #included into pg_fts_am.c) */
extern IndexScanDesc bm25_beginscan(Relation r, int nkeys, int norderbys);
extern void bm25_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
						ScanKey orderbys, int norderbys);
extern int64 bm25_getbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern bool bm25_gettuple(IndexScanDesc scan, ScanDirection dir);
extern bool bm25_canreturn(Relation index, int attno);
extern void bm25_endscan(IndexScanDesc scan);

#endif							/* PG_FTS_AM_H */
