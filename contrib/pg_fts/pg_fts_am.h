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
#define BM25_VERSION		1
#define BM25_METAPAGE_BLKNO	0

/* page opaque flags */
#define BM25_META			(1 << 0)
#define BM25_DICT			(1 << 1)
#define BM25_POSTING		(1 << 2)
#define BM25_PENDING		(1 << 3)

typedef struct BM25PageOpaqueData
{
	uint16		flags;
	uint16		unused;
	BlockNumber nextblk;		/* next page in a dict/posting/pending chain */
	uint32		block_max_tf;	/* max tf on this posting page (block-max WAND) */
	uint32		first_docid_hi; /* high 32 bits of first docid on page */
	uint32		first_docid_lo; /* low 32 bits (for skip decisions) */
} BM25PageOpaqueData;

typedef BM25PageOpaqueData *BM25PageOpaque;

#define BM25PageGetOpaque(page) \
	((BM25PageOpaque) PageGetSpecialPointer(page))

typedef struct BM25MetaPageData
{
	uint32		magic;
	uint32		version;
	double		ndocs;			/* N (built + pending) */
	double		sumdoclen;		/* sum of document lengths -> avgdl = /N */
	uint32		nterms;			/* number of distinct terms (dictionary size) */
	BlockNumber dictstart;		/* first dictionary page */
	BlockNumber pendinghead;	/* first pending page, or InvalidBlockNumber */
	BlockNumber pendingtail;	/* last pending page, for O(1) append */
	uint32		npending;		/* number of pending (unmerged) documents */
} BM25MetaPageData;

#define BM25PageGetMeta(page) \
	((BM25MetaPageData *) PageGetContents(page))

/* a dictionary entry; term text is inline, length termlen */
typedef struct BM25DictEntry
{
	uint32		termlen;
	uint32		df;				/* document frequency */
	uint32		max_tf;			/* max tf across postings (WAND impact bound) */
	BlockNumber firstposting;	/* first posting page for this term */
	char		term[FLEXIBLE_ARRAY_MEMBER];
} BM25DictEntry;

/* a posting: which heap tuple, and the term frequency there */
typedef struct BM25Posting
{
	ItemPointerData tid;
	uint32		tf;
} BM25Posting;

/*
 * Posting pages store postings delta+varint compressed, not as a raw
 * BM25Posting array.  The page contents begin with a uint32 count, followed by
 * a varint stream: for each posting, the docid gap (this docid - previous,
 * where docid = block*MaxHeapTuplesPerPage + offset) and the tf.  docids are
 * written in ascending order within a term so gaps are small.  Readers use
 * bm25_page_decode(); writers use the BM25PostingWriter below.  This is the
 * posting compression that keeps the index compact at scale.
 */
typedef struct BM25PostingPageHdr
{
	uint32		count;			/* number of postings encoded on this page */
	/* varint stream follows */
} BM25PostingPageHdr;

/*
 * A pending record: a not-yet-merged document stored verbatim on a pending
 * page.  The ftsdoc varlena follows the header inline (doclen bytes).  Pending
 * documents are searched directly at scan time and folded into the main
 * dictionary/postings by a merge (REINDEX for now).
 */
typedef struct BM25PendingItem
{
	ItemPointerData tid;
	uint32		doclen;			/* byte length of the ftsdoc that follows */
	/* char ftsdoc[doclen] follows, MAXALIGN'd */
} BM25PendingItem;

/* scan functions (pg_fts_am_scan.c, #included into pg_fts_am.c) */
extern IndexScanDesc bm25_beginscan(Relation r, int nkeys, int norderbys);
extern void bm25_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
						ScanKey orderbys, int norderbys);
extern int64 bm25_getbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void bm25_endscan(IndexScanDesc scan);

#endif							/* PG_FTS_AM_H */
