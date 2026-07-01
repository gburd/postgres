/*-------------------------------------------------------------------------
 *
 * pg_fts_trgm_index.c
 *		On-disk trigram index for narrowing fuzzy/regex candidates.
 *
 * Included into pg_fts_am.c.  During index build we map every trigram of every
 * indexed term to the set of docids whose document contains a term with that
 * trigram, stored as a namespaced sparsemap (see pg_fts_sm.h) serialized inline
 * on WAL-logged trigram pages.  At fuzzy/regex query time the candidate docid
 * set is the union of the query pattern's trigram postings -- a sound superset
 * of the true matches (pigeonhole: a term within k edits, or matching a regex
 * whose required trigrams are known, shares a trigram) -- so the scan probes a
 * small candidate set and the heap recheck refines it, instead of scanning the
 * whole index.  This is the pg_tre-style funnel; the AST-tiling trigram
 * extraction for arbitrary regexes is a further refinement (today we use the
 * pattern's literal trigrams, falling back to the full set when too few).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_trgm_index.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_fts_sm.h"

/*
 * Build trigram -> docid-set sparsemaps from the collected BuildTerms and write
 * them to a chain of trigram pages.  Returns the first trigram block.
 *
 * We accumulate, per distinct trigram, a growable docid array, then build one
 * sparsemap per trigram and pack (trgm, smlen, sparsemap-bytes) onto pages.
 */

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

static BlockNumber
bm25_write_trigrams(Relation index, BM25BuildState *bs)
{
	HTAB	   *ht;
	TrgmAccum  *accs;
	int			naccs = 0;
	int			maxaccs = 1024;
	int			i;
	BlockNumber first = InvalidBlockNumber;
	Buffer		buffer = InvalidBuffer;
	GenericXLogState *state = NULL;
	Page		page = NULL;

	/* map trigram hash -> index into accs[] */
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
			int			p;

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

				/* add this term's docids to the trigram's docid set */
				for (p = 0; p < bt->nposts; p++)
				{
					uint64		docid = bm25_tid_to_docid(&bt->tids[p]);

					if (acc->ndocids >= acc->maxdocids)
					{
						acc->maxdocids = acc->maxdocids ? acc->maxdocids * 2 : 8;
						if (acc->docids == NULL)
							acc->docids = (uint64 *) palloc(acc->maxdocids * sizeof(uint64));
						else
							acc->docids = (uint64 *) repalloc(acc->docids,
															  acc->maxdocids * sizeof(uint64));
					}
					acc->docids[acc->ndocids++] = docid;
				}
			}
		}
	}

	/* serialize each trigram's docid set as a sparsemap and pack onto pages */
	for (i = 0; i < naccs; i++)
	{
		TrgmAccum  *acc = &accs[i];
		sm_t		sm;
		size_t		bufsz;
		uint8	   *smbuf;
		size_t		smlen;
		Size		need;
		int			d,
					w = 0;

		/* dedup docids (multiple terms sharing a trigram can repeat a docid) */
		if (acc->ndocids > 1)
		{
			qsort(acc->docids, acc->ndocids, sizeof(uint64), cmp_uint64);
			for (d = 1; d < acc->ndocids; d++)
				if (acc->docids[d] != acc->docids[w])
					acc->docids[++w] = acc->docids[d];
			acc->ndocids = w + 1;
		}

		/* build the sparsemap in a generously sized caller buffer */
		bufsz = 128 + (size_t) acc->ndocids * 16;
		smbuf = (uint8 *) palloc0(bufsz);
		sm_init(&sm, smbuf, bufsz);
		for (d = 0; d < acc->ndocids; d++)
			sm_add(&sm, acc->docids[d]);
		smlen = sm_get_size(&sm);

		need = MAXALIGN(offsetof(BM25TrgmEntry, trgm) + sizeof(uint32) * 2 + smlen);

		if (buffer == InvalidBuffer ||
			((PageHeader) page)->pd_lower + need >
			BLCKSZ - MAXALIGN(sizeof(BM25PageOpaqueData)))
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
			bm25_init_page(page, BM25_TRGM);
		}

		{
			char	   *dst = (char *) page + ((PageHeader) page)->pd_lower;
			BM25TrgmEntry *te = (BM25TrgmEntry *) dst;

			te->trgm = acc->trgm;
			te->smlen = (uint32) smlen;
			memcpy((char *) te + offsetof(BM25TrgmEntry, trgm) + sizeof(uint32) * 2,
				   sm_get_data(&sm), smlen);
			((PageHeader) page)->pd_lower += need;
		}
	}

	if (buffer != InvalidBuffer)
	{
		GenericXLogFinish(state);
		UnlockReleaseBuffer(buffer);
	}

	hash_destroy(ht);
	return first;
}

/*
 * Gather candidate docids for a fuzzy/regex query term into a TidSet, using the
 * trigram index: the union of the query term's trigram postings.  Returns true
 * if the trigram index was usable (query had enough trigrams and trgmstart is
 * valid); false means the caller should fall back to the full universe.
 */
static bool
bm25_trgm_candidates(Relation index, BlockNumber trgmstart,
					 const char *term, int termlen, int min_trigrams,
					 bool is_regex, TidSet *out)
{
	uint32		qtrg[FTS_MAX_TRIGRAMS];
	int			nqtrg;
	int			cap = 64,
				n = 0;
	ItemPointerData *tids;
	int			g;

	out->tids = NULL;
	out->n = 0;

	if (trgmstart == InvalidBlockNumber)
		return false;
	if (is_regex)
		nqtrg = fts_regex_trigrams(term, termlen, qtrg, FTS_MAX_TRIGRAMS);
	else
		nqtrg = fts_trigrams(term, termlen, qtrg, FTS_MAX_TRIGRAMS);
	if (nqtrg < min_trigrams)
		return false;			/* too few trigrams to prune soundly */

	tids = (ItemPointerData *) palloc(cap * sizeof(ItemPointerData));

	/* union the docid sets of the query term's trigrams */
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

			LockBuffer(buf, BUFFER_LOCK_SHARE);
			page = BufferGetPage(buf);
			ptr = (char *) PageGetContents(page);
			end = (char *) page + ((PageHeader) page)->pd_lower;
			next = BM25PageGetOpaque(page)->nextblk;

			while (ptr < end)
			{
				BM25TrgmEntry *te = (BM25TrgmEntry *) ptr;
				Size		esize = MAXALIGN(offsetof(BM25TrgmEntry, trgm) +
											 sizeof(uint32) * 2 + te->smlen);

				if (te->trgm == qtrg[g])
				{
					sm_t		sm;
					sm_cursor_t cur = SM_CURSOR_INIT;
					uint64_t	v;

					sm_open(&sm, (uint8_t *) te +
							offsetof(BM25TrgmEntry, trgm) + sizeof(uint32) * 2,
							te->smlen);
					for (v = sm_next_member(&sm, (uint64_t) -1, &cur);
						 v != SM_IDX_MAX;
						 v = sm_next_member(&sm, v, &cur))
					{
						if (n >= cap)
						{
							cap *= 2;
							tids = repalloc(tids, cap * sizeof(ItemPointerData));
						}
						bm25_docid_to_tid((uint64) v, &tids[n++]);
					}
					done = true;
					break;
				}
				ptr += esize;
			}
			UnlockReleaseBuffer(buf);
			blk = next;
		}
	}

	out->tids = tids;
	out->n = n;
	tidset_sort_uniq(out);
	return true;
}
