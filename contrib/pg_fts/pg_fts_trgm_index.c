/*-------------------------------------------------------------------------
 *
 * pg_fts_trgm_index.c
 *		On-disk trigram index for narrowing fuzzy/regex candidates.
 *
 * Included into pg_fts_am.c.  Maps every trigram of every indexed term to the
 * set of TERM ORDINALS (positions in the segment's sorted dictionary) whose
 * term contains that trigram, stored as a namespaced sparsemap (see
 * pg_fts_sm.h).  Keying on the vocabulary rather than the docid space keeps
 * each set small (bounded by the number of distinct terms, not documents).  A
 * serialized sparsemap can still span more than one page, so it is stored as a
 * byte stream across a chain of data pages -- NOT packed inline on one page
 * (that assumption caused a segfault at scale).  A directory (trgm -> first
 * data block + byte length) lets the query side find a trigram's stream,
 * reassemble the sparsemap, and iterate its term ordinals.
 *
 * At fuzzy/regex query time the candidate term set is the union of the query
 * pattern's trigram postings -- a sound superset -- so the scan probes a small
 * candidate set and the heap recheck applies the exact test.
 *
 * Layout (all pages WAL-logged via GenericXLog, one page per Xlog cycle):
 *   directory pages (BM25_TRGM):      fixed-size BM25TrgmEntry[]
 *   data pages      (BM25_TRGM_DATA): raw sparsemap bytes, chained by nextblk
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_trgm_index.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_fts_sm.h"

typedef struct TrgmAccum
{
	uint32		trgm;
	uint64	   *docids;
	int			ndocids;
	int			maxdocids;
}			TrgmAccum;

static int
cmp_uint64(const void *a, const void *b)
{
	uint64		x = *(const uint64 *) a,
				y = *(const uint64 *) b;

	return (x > y) - (x < y);
}

/* usable bytes for the raw stream on a data page */
#define BM25_TRGMDATA_CAP \
	(BLCKSZ - (int) MAXALIGN(SizeOfPageHeaderData) - (int) MAXALIGN(sizeof(BM25PageOpaqueData)))

/*
 * Write `len` bytes across a fresh chain of BM25_TRGM_DATA pages (one page per
 * GenericXLog cycle, so no page-count limit).  Returns the first block.
 */
static BlockNumber
bm25_write_blob(Relation index, const uint8 *data, Size len)
{
	BlockNumber first = InvalidBlockNumber;
	Buffer		prevbuf = InvalidBuffer;
	Page		prevpage = NULL;
	GenericXLogState *prevstate = NULL;
	Size		off = 0;

	do
	{
		Buffer		buf = bm25_new_buffer(index);
		BlockNumber blk = BufferGetBlockNumber(buf);
		GenericXLogState *state = GenericXLogStart(index);
		Page		page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
		Size		chunk = Min(len - off, (Size) BM25_TRGMDATA_CAP);

		bm25_init_page(page, BM25_TRGM_DATA);
		if (chunk > 0)
		{
			memcpy((char *) PageGetContents(page), data + off, chunk);
			((PageHeader) page)->pd_lower =
				((char *) PageGetContents(page) - (char *) page) + chunk;
		}

		if (prevbuf != InvalidBuffer)
		{
			BM25PageGetOpaque(prevpage)->nextblk = blk;
			GenericXLogFinish(prevstate);
			UnlockReleaseBuffer(prevbuf);
		}
		else
			first = blk;

		prevbuf = buf;
		prevpage = page;
		prevstate = state;
		off += chunk;
	} while (off < len);

	if (prevbuf != InvalidBuffer)
	{
		GenericXLogFinish(prevstate);
		UnlockReleaseBuffer(prevbuf);
	}
	return first;
}

/* Read `len` bytes starting at data block `blk` into a palloc'd buffer. */
static uint8 *
bm25_read_blob(Relation index, BlockNumber blk, Size len)
{
	uint8	   *buf = (uint8 *) palloc(len ? len : 1);
	Size		off = 0;

	while (blk != InvalidBlockNumber && off < len)
	{
		Buffer		b = ReadBuffer(index, blk);
		Page		page;
		Size		avail;

		LockBuffer(b, BUFFER_LOCK_SHARE);
		page = BufferGetPage(b);
		avail = ((PageHeader) page)->pd_lower -
			((char *) PageGetContents(page) - (char *) page);
		avail = Min(avail, len - off);
		memcpy(buf + off, PageGetContents(page), avail);
		off += avail;
		blk = BM25PageGetOpaque(page)->nextblk;
		UnlockReleaseBuffer(b);
	}
	return buf;
}

/*
 * Build trigram -> docid-set sparsemaps: each trigram's serialized sparsemap
 * as a data-page blob, plus a fixed-size directory entry (trgm, smlen,
 * firstdata) on directory pages.  Returns the first directory block.
 */
static BlockNumber
bm25_write_trigrams(Relation index, BM25BuildState *bs)
{
	HTAB	   *ht;
	TrgmAccum  *accs;
	int			naccs = 0;
	int			maxaccs = 1024;
	int			i;
	BlockNumber first = InvalidBlockNumber;
	Buffer		dbuf = InvalidBuffer;
	GenericXLogState *dstate = NULL;
	Page		dpage = NULL;

	{
		typedef struct
		{
			uint32		trgm;
			int			idx;
		} TE;
		HASHCTL		c2;

		c2.keysize = sizeof(uint32);
		c2.entrysize = sizeof(TE);
		c2.hcxt = CurrentMemoryContext;
		ht = hash_create("bm25 trgm build", 4096, &c2,
						 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		accs = (TrgmAccum *) palloc(maxaccs * sizeof(TrgmAccum));

		for (i = 0; i < bs->nterms; i++)
		{
			BuildTerm  *bt = &bs->terms[i];
			uint32		trg[FTS_MAX_TRIGRAMS];
			int			ntrg = fts_trigrams(bt->term, bt->len, trg, FTS_MAX_TRIGRAMS);
			int			g;

			for (g = 0; g < ntrg; g++)
			{
				TE		   *e;
				bool		found;
				TrgmAccum  *acc;

				e = (TE *) hash_search(ht, &trg[g], HASH_ENTER, &found);
				if (!found)
				{
					if (naccs >= maxaccs)
					{
						maxaccs *= 2;
						accs = (TrgmAccum *) repalloc(accs, maxaccs * sizeof(TrgmAccum));
					}
					e->idx = naccs;
					accs[naccs].trgm = trg[g];
					accs[naccs].docids = NULL;
					accs[naccs].ndocids = 0;
					accs[naccs].maxdocids = 0;
					naccs++;
				}
				acc = &accs[e->idx];
				/*
				 * Inverted over the VOCABULARY: record this term's ordinal (its
				 * dictionary position i), not its docids.  The vocabulary is
				 * small, so the set stays small and dense regardless of how many
				 * documents the term appears in.
				 */
				if (acc->ndocids >= acc->maxdocids)
				{
					acc->maxdocids = acc->maxdocids ? acc->maxdocids * 2 : 8;
					if (acc->docids == NULL)
						acc->docids = (uint64 *) palloc(acc->maxdocids * sizeof(uint64));
					else
						acc->docids = (uint64 *) repalloc(acc->docids,
															  acc->maxdocids * sizeof(uint64));
				}
				acc->docids[acc->ndocids++] = (uint64) i;
			}
		}
	}

	for (i = 0; i < naccs; i++)
	{
		TrgmAccum  *acc = &accs[i];
		sm_t	   *sm;
		size_t		smlen;
		BlockNumber datablk;
		int			d,
					w = 0;
		Size		need;

		if (acc->ndocids > 1)
		{
			qsort(acc->docids, acc->ndocids, sizeof(uint64), cmp_uint64);
			for (d = 1; d < acc->ndocids; d++)
				if (acc->docids[d] != acc->docids[w])
					acc->docids[++w] = acc->docids[d];
			acc->ndocids = w + 1;
		}

		/*
		 * We do NOT skip "popular" trigrams: the union of the pattern's trigram
		 * term-sets is only a sound superset of candidate terms if every pattern
		 * trigram contributes; dropping one silently loses terms that share only
		 * that trigram.  Because the sets are over the VOCABULARY (small), even a
		 * common trigram's term-set is bounded and cheap.
		 *
		 * Use a library-owned, auto-growing sparsemap (sm_create + sm_add_grow):
		 * a fixed wrap buffer with plain sm_add silently drops members on ENOSPC,
		 * which lost candidates for high-cardinality trigrams (the 112-vs-424
		 * bug).  sm_free releases the malloc'd buffer (not palloc).
		 */
		sm = sm_create(256);
		if (sm == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory building bm25 trigram map")));
		for (d = 0; d < acc->ndocids; d++)
			sm_add_grow(&sm, acc->docids[d]);
		smlen = sm_get_size(sm);

		/* store this trigram's term-ordinal set as a data-page blob */
		datablk = bm25_write_blob(index, (const uint8 *) sm_get_data(sm), smlen);
		sm_free(sm);

		/* append the fixed-size directory entry, chaining a page if needed */
		need = MAXALIGN(sizeof(BM25TrgmEntry));
		if (dbuf == InvalidBuffer ||
			((PageHeader) dpage)->pd_lower + need >
			BLCKSZ - MAXALIGN(sizeof(BM25PageOpaqueData)))
		{
			Buffer		next = bm25_new_buffer(index);
			BlockNumber nextblk = BufferGetBlockNumber(next);

			if (dbuf != InvalidBuffer)
			{
				BM25PageGetOpaque(dpage)->nextblk = nextblk;
				GenericXLogFinish(dstate);
				UnlockReleaseBuffer(dbuf);
			}
			else
				first = nextblk;
			dbuf = next;
			dstate = GenericXLogStart(index);
			dpage = GenericXLogRegisterBuffer(dstate, dbuf, GENERIC_XLOG_FULL_IMAGE);
			bm25_init_page(dpage, BM25_TRGM);
		}
		{
			BM25TrgmEntry *te = (BM25TrgmEntry *) ((char *) dpage +
												   ((PageHeader) dpage)->pd_lower);

			te->trgm = acc->trgm;
			te->smlen = (uint32) smlen;
			te->firstdata = datablk;
			((PageHeader) dpage)->pd_lower += need;
		}
	}

	if (dbuf != InvalidBuffer)
	{
		GenericXLogFinish(dstate);
		UnlockReleaseBuffer(dbuf);
	}
	hash_destroy(ht);
	return first;
}

/*
 * Gather candidate docids for a fuzzy/regex query term into a TidSet.
 *
 * Two-stage vocabulary funnel: (1) union the query pattern's trigram postings
 * to get a set of candidate TERM ORDINALS (small: bounded by the vocabulary);
 * (2) walk the dictionary once, and for each term whose ordinal is a candidate,
 * union its docid postings.  The heap recheck then applies the exact
 * fuzzy/regex test.  No trigrams are skipped at build time, so every query
 * trigram that has a directory entry constrains the candidate set; if the
 * pattern has too few usable trigrams (e.g. it is shorter than a trigram) we
 * return false and the caller falls back to a full scan (always correct).
 */
static bool
bm25_trgm_candidates(Relation index, BlockNumber trgmstart,
					 BlockNumber dictstart,
					 const char *term, int termlen, int min_trigrams,
					 bool is_regex, TidSet *out)
{
	uint32		qtrg[FTS_MAX_TRIGRAMS];
	int			nqtrg;
	int			g;
	uint64	   *ords = NULL;		/* candidate term ordinals (sorted, unique) */
	int			nords = 0,
				maxords = 0;
	int			matched_trg = 0;
	ItemPointerData *tids;
	int			cap = 64,
				n = 0;
	BlockNumber dblk;
	uint32		ordinal;
	int			oi;

	out->tids = NULL;
	out->n = 0;
	if (trgmstart == InvalidBlockNumber)
		return false;
	if (is_regex)
		nqtrg = fts_regex_trigrams(term, termlen, qtrg, FTS_MAX_TRIGRAMS);
	else
		nqtrg = fts_trigrams(term, termlen, qtrg, FTS_MAX_TRIGRAMS);
	if (nqtrg < min_trigrams)
		return false;

	/* stage 1: union the pattern trigrams' term-ordinal sets */
	for (g = 0; g < nqtrg; g++)
	{
		BlockNumber blk = trgmstart;
		bool		done = false;

		while (blk != InvalidBlockNumber && !done)
		{
			Buffer		buf = ReadBuffer(index, blk);
			Page		page;
			char	   *ptr,
					   *end;
			BlockNumber next;
			uint32		smlen = 0;
			BlockNumber firstdata = InvalidBlockNumber;
			bool		found = false;

			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			ptr = (char *) PageGetContents(page);
			end = (char *) page + ((PageHeader) page)->pd_lower;
			next = BM25PageGetOpaque(page)->nextblk;
			while (ptr < end)
			{
				BM25TrgmEntry *te = (BM25TrgmEntry *) ptr;

				if (te->trgm == qtrg[g])
				{
					smlen = te->smlen;
					firstdata = te->firstdata;
					found = true;
					break;
				}
				ptr += MAXALIGN(sizeof(BM25TrgmEntry));
			}
			UnlockReleaseBuffer(buf);

			if (found)
			{
				uint8	   *smbuf = bm25_read_blob(index, firstdata, smlen);
				sm_t		sm;
				sm_cursor_t cur = SM_CURSOR_INIT;
				uint64_t	v;

				sm_open(&sm, smbuf, smlen);
				for (v = sm_next_member(&sm, (uint64_t) -1, &cur);
					 v != SM_IDX_MAX;
					 v = sm_next_member(&sm, v, &cur))
				{
					if (nords >= maxords)
					{
						maxords = maxords ? maxords * 2 : 64;
						ords = ords ? repalloc(ords, maxords * sizeof(uint64))
							: palloc(maxords * sizeof(uint64));
					}
					ords[nords++] = v;
				}
				pfree(smbuf);
				matched_trg++;
				done = true;
				break;
			}
			blk = next;
		}
	}

	/* if no pattern trigram had a directory entry (all popular/skipped), we
	 * cannot prune -- fall back to a full scan */
	if (matched_trg == 0)
		return false;

	/* dedup candidate ordinals */
	if (nords > 1)
	{
		int			w = 0,
					d;

		qsort(ords, nords, sizeof(uint64), cmp_uint64);
		for (d = 1; d < nords; d++)
			if (ords[d] != ords[w])
				ords[++w] = ords[d];
		nords = w + 1;
	}

	/* stage 2: walk THIS SEGMENT's dictionary once; for each candidate ordinal,
	 * union its term's docid postings.  The trigram directory's ordinals index
	 * into the segment's own dictionary, written in ordinal order. */
	tids = (ItemPointerData *) palloc(cap * sizeof(ItemPointerData));
	ordinal = 0;
	oi = 0;
	dblk = dictstart;
	while (dblk != InvalidBlockNumber && oi < nords)
	{
		Buffer		buf = ReadBuffer(index, dblk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;
		while (ptr < end && oi < nords)
		{
			BM25DictEntry *de = (BM25DictEntry *) ptr;
			Size		esize = MAXALIGN(offsetof(BM25DictEntry, term) + de->termlen);

			if (ordinal == (uint32) ords[oi])
			{
				BM25Posting *post;
				int			np = bm25_decode_term(index, de->firstposting,
												  de->firstoffset, de->df,
												  &post, NULL);
				int			k;

				for (k = 0; k < np; k++)
				{
					if (n >= cap)
					{
						cap *= 2;
						tids = repalloc(tids, cap * sizeof(ItemPointerData));
					}
					tids[n++] = post[k].tid;
				}
				pfree(post);
				oi++;
			}
			ordinal++;
			ptr += esize;
		}
		UnlockReleaseBuffer(buf);
		dblk = next;
	}

	out->tids = tids;
	out->n = n;
	tidset_sort_uniq(out);
	return true;
}
