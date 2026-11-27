/*-------------------------------------------------------------------------
 *
 * pg_fts_am_scan.c
 *		Bitmap scan for the bm25 access method.
 *
 * Included directly into pg_fts_am.c (it shares static page helpers).  It
 * evaluates an ftsquery by set algebra over posting lists (a term yields the
 * TIDs whose document contains it; AND intersects, OR unions, NOT complements
 * against the indexed universe) for the bitmap and index-only scans, and runs
 * block-max WAND / MaxScore top-k for the <=> ordering scan.  Fuzzy/regex use
 * a Levenshtein automaton / trigram funnel; counts use a visibility-map-aware
 * bulk path.  Results are exact against @@@ semantics; the boolean and ranked
 * paths need no heap access beyond MVCC visibility.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_am_scan.c
 *
 *-------------------------------------------------------------------------
 */

/* A materialized, sorted, duplicate-free set of TIDs. */
typedef struct TidSet
{
	ItemPointerData *tids;
	int			n;
} TidSet;

/* forward decl: trigram-index candidate lookup (pg_fts_trgm_index.c) */
static bool bm25_trgm_candidates(Relation index, BlockNumber trgmstart,
								 BlockNumber dictstart,
								 const char *term, int termlen,
								 int min_trigrams, bool is_regex, TidSet *out);
static void bm25_collect_matches(Relation index, FtsQuery query, TidSet *out, bool *recheck);
static double bm25_query_maxhits(Relation index, FtsQuery q, double N);
/* forward decl: blob reader (pg_fts_trgm_index.c, included after this file) */
static uint8 *bm25_read_blob(Relation index, BlockNumber blk, Size len);

/*
 * Ceiling on the adaptive-k ordering scan's top-k.  A large k makes WAND's
 * threshold stay near zero (little pruning), degrading toward O(result * df),
 * so we refuse to grow k past this for deep pagination -- bounding worst-case
 * latency.  (Deep top-N over a broad query is inherently costly.)
 */
#define BM25_MAX_ORDERK 4096

/* A scored heap tuple (score, or distance in an ordering scan). */
typedef struct ScoredTid
{
	ItemPointerData tid;
	double		score;
}			ScoredTid;

static int bm25_topk_visible(Relation index, FtsQuery q, int k,
							 bool as_distance, ScoredTid **out);
static int bm25_topk_candidates_range(Relation index, FtsQuery q, int wantk,
									  uint64 docid_lo, uint64 docid_hi,
									  ScoredTid **out);

typedef struct BM25ScanOpaqueData
{
	FtsQuery	query;			/* copied into the scan's context */
	bool		queryValid;
	/* ordering-scan (amgettuple) state, materialized on first call */
	bool		orderInit;		/* have we computed the ordered results? */
	ScoredTid  *ordered;		/* top-k by ascending distance */
	int			nordered;
	int			ordpos;			/* next result to return */
	int			curk;			/* current materialized k (grows on demand) */
	double		maxhits;		/* provable upper bound on result size (cap growth) */
	/* plain-scan (amgettuple, no ORDER BY) state for index-only counts */
	bool		plainInit;		/* have we materialized the matching TIDs? */
	ItemPointerData *plainTids; /* sorted matching TIDs */
	int			nplain;
	int			plainpos;
	bool		plainRecheck;	/* results need a heap recheck (fuzzy/regex) */
	IndexTuple	plainItup;		/* cached all-NULL itup for index-only scans */
	TupleDesc	plainItupDesc;
} BM25ScanOpaqueData;

typedef BM25ScanOpaqueData *BM25ScanOpaque;

static int
cmp_tid(const void *a, const void *b)
{
	return ItemPointerCompare((ItemPointer) a, (ItemPointer) b);
}

static void
tidset_sort_uniq(TidSet *s)
{
	int			i,
				j;

	if (s->n <= 1)
		return;
	qsort(s->tids, s->n, sizeof(ItemPointerData), cmp_tid);
	for (i = 0, j = 1; j < s->n; j++)
		if (ItemPointerCompare(&s->tids[i], &s->tids[j]) != 0)
			s->tids[++i] = s->tids[j];
	s->n = i + 1;
}

/*
 * Tombstone (deleted-docid) sets, one per segment, loaded from each segment's
 * livedocs blob.  VACUUM (bm25_bulkdelete) records docids of vacuumed heap
 * tuples here; scans/counts MUST subtract them, because the index-only and
 * count paths trust the visibility map and would otherwise report a
 * vacuumed-and-reused heap slot as a match.  hasany is false (the common,
 * delete-free case) => zero overhead: no membership checks at all.
 */
typedef struct BM25Tombstones
{
	bool		hasany;
	uint32		nseg;
	uint8	  **blobs;			/* per-segment palloc'd blob, or NULL */
	sm_t	   *maps;			/* per-segment opened sm_t (valid iff blobs[i]) */
	bool	   *present;		/* whether segment i has a tombstone map */
}			BM25Tombstones;

static void
bm25_tombstones_load(Relation index, const BM25MetaPageData *meta, BM25Tombstones *t)
{
	uint32		s;

	t->hasany = false;
	t->nseg = meta->nsegments;
	t->blobs = NULL;
	t->maps = NULL;
	t->present = NULL;
	for (s = 0; s < meta->nsegments; s++)
		if (meta->segs[s].livedocs != InvalidBlockNumber &&
			meta->segs[s].livedocslen > 0)
		{
			t->hasany = true;
			break;
		}
	if (!t->hasany)
		return;

	t->blobs = (uint8 **) palloc0(meta->nsegments * sizeof(uint8 *));
	/*
	 * sm_t (struct sparsemap) is declared with 8-byte alignment; plain palloc
	 * only guarantees MAXALIGN (4 on ILP32), so allocate the array 8-aligned to
	 * satisfy that requirement (a misaligned sm_t trips -fsanitize=alignment).
	 */
	t->maps = (sm_t *) palloc_aligned(meta->nsegments * sizeof(sm_t), 8, 0);
	memset(t->maps, 0, meta->nsegments * sizeof(sm_t));
	t->present = (bool *) palloc0(meta->nsegments * sizeof(bool));
	for (s = 0; s < meta->nsegments; s++)
	{
		const BM25SegMeta *sg = &meta->segs[s];

		if (sg->livedocs != InvalidBlockNumber && sg->livedocslen > 0)
		{
			t->blobs[s] = bm25_read_blob(index, sg->livedocs, sg->livedocslen);
			sm_open(&t->maps[s], (uint8_t *) t->blobs[s], sg->livedocslen);
			t->present[s] = true;
		}
	}
}

static void
bm25_tombstones_free(BM25Tombstones *t)
{
	uint32		s;

	if (!t->hasany)
		return;
	for (s = 0; s < t->nseg; s++)
		if (t->present[s])
			pfree(t->blobs[s]);
	pfree(t->blobs);
	pfree(t->maps);
	pfree(t->present);
}

/*
 * Drop TIDs tombstoned in ONE specific segment from a TidSet in place.
 * Tombstones are per-segment: a docid deleted in segment A must only be
 * suppressed among matches produced BY segment A -- the same heap TID may have
 * been reused by a live document in a newer segment or the pending list, and
 * that document must not be filtered.  Applied to each segment's own match
 * contribution at collection time.
 */
static void
bm25_filter_tombstoned_seg(BM25Tombstones *t, uint32 segidx, TidSet *s)
{
	int			i,
				j = 0;
	uint64		stackids[256];
	bool		stackres[256];
	uint64	   *ids;
	bool	   *res;

	if (!t->hasany || s->n == 0 || segidx >= t->nseg || !t->present[segidx])
		return;

	/*
	 * Batched membership: extract this set's docids (already ascending, since
	 * the TidSet is TID-sorted and docid is monotonic in TID) and test them
	 * all in one left-to-right sweep with sm_contains_many -- O(chunks + n)
	 * instead of n independent head-walks.  Use a stack buffer for the common
	 * small case to avoid palloc; fall back to palloc only for large sets.
	 */
	if (s->n <= (int) lengthof(stackids))
	{
		ids = stackids;
		res = stackres;
	}
	else
	{
		ids = (uint64 *) palloc(s->n * sizeof(uint64));
		res = (bool *) palloc(s->n * sizeof(bool));
	}

	for (i = 0; i < s->n; i++)
		ids[i] = bm25_tid_to_docid(&s->tids[i]);

	sm_contains_many(&t->maps[segidx], ids, res, (size_t) s->n);

	for (i = 0; i < s->n; i++)
		if (!res[i])
			s->tids[j++] = s->tids[i];
	s->n = j;

	if (ids != stackids)
	{
		pfree(ids);
		pfree(res);
	}
}

/* Read the metapage for corpus stats + dictstart. */
static void
bm25_read_meta(Relation index, BM25MetaPageData *out)
{
	Buffer		buffer = ReadBuffer(index, BM25_METAPAGE_BLKNO);
	Page		page;

	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);
	memcpy(out, BM25PageGetMeta(page), sizeof(BM25MetaPageData));
	UnlockReleaseBuffer(buffer);
}

/*
 * Use a segment's sparse block index to find the single dictionary page that
 * could contain `term`: the last index entry whose term <= target.  Returns
 * that page's block number, or `dictstart` if the segment has no block index
 * (empty segment or pre-index format).  The located page is the ONLY page that
 * can hold the term (the next page's first term is > target), so point lookups
 * scan just that page.
 */
static BlockNumber
bm25_dict_seek(Relation index, const BM25SegMeta *seg,
			   const char *term, int termlen)
{
	BlockNumber iblk = seg->dictindexstart;
	BlockNumber best = seg->dictstart;

	while (iblk != InvalidBlockNumber)
	{
		Buffer		buf = ReadBuffer(index, iblk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;
		bool		overshot = false;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;
		while (ptr < end)
		{
			BM25DictIndexEntry *ie = (BM25DictIndexEntry *) ptr;
			int			cmplen = Min((int) ie->termlen, termlen);
			int			c = memcmp(ie->term, term, cmplen);

			if (c == 0)
				c = (int) ie->termlen - termlen;
			if (c <= 0)
				best = ie->blk;		/* entry term <= target: candidate page */
			else
			{
				overshot = true;	/* entries are sorted; no need to go further */
				break;
			}
			ptr += MAXALIGN(offsetof(BM25DictIndexEntry, term) + ie->termlen);
		}
		UnlockReleaseBuffer(buf);
		if (overshot)
			break;
		iblk = next;
	}
	return best;
}

/*
 * Look up a term in the dictionary; on hit, read its full posting list into a
 * TidSet.  Returns true if found.  bm25_dict_seek uses the segment's sparse
 * block index to jump straight to the one dictionary page that can hold the
 * term (scanning the whole chain only for a segment that predates the index).
 */
static bool
bm25_lookup_term(Relation index, const BM25SegMeta *seg,
				 const char *term, int termlen, TidSet *out)
{
	BlockNumber blk = bm25_dict_seek(index, seg, term, termlen);
	bool		onlyone = (seg->dictindexstart != InvalidBlockNumber);

	out->tids = NULL;
	out->n = 0;

	while (blk != InvalidBlockNumber)
	{
		Buffer		buffer = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr;
		char	   *end;
		BlockNumber firstposting = InvalidBlockNumber;
		uint32		firstoffset = 0;
		uint32		df = 0;
		bool		found = false;

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;

		while (ptr < end)
		{
			BM25DictEntry *de = (BM25DictEntry *) ptr;
			Size		esize = MAXALIGN(offsetof(BM25DictEntry, term) + de->termlen);

			if ((int) de->termlen == termlen &&
				memcmp(de->term, term, termlen) == 0)
			{
				firstposting = de->firstposting;
				firstoffset = de->firstoffset;
				df = de->df;
				found = true;
				break;
			}
			ptr += esize;
		}
		blk = BM25PageGetOpaque(page)->nextblk;
		UnlockReleaseBuffer(buffer);

		if (found)
		{
			/* read exactly this term's df postings from the shared chain */
			BM25Posting *post;
			int			np = bm25_decode_term(index, firstposting, firstoffset,
										  df, &post, NULL);
			ItemPointerData *tids = palloc(Max(np, 1) * sizeof(ItemPointerData));
			int			n = 0;
			int			i;

			for (i = 0; i < np; i++)
				tids[n++] = post[i].tid;
			pfree(post);
			out->tids = tids;
			out->n = n;
			tidset_sort_uniq(out);
			return true;
		}
		if (onlyone)
			break;				/* block index located the only possible page */
	}
	return false;
}

/* set operations on sorted TidSets */

/*
 * Galloping (exponential) search: return the least index >= lo in t[0..n) whose
 * tid >= key.  Used to skip runs when intersecting a small set against a large
 * one (O(|small| * log|large|) instead of O(|small|+|large|)).
 */
static inline int
tidset_gallop(const ItemPointerData *t, int n, int lo, const ItemPointerData *key)
{
	int			step = 1;
	int			hi;

	while (lo < n && ItemPointerCompare((ItemPointer) &t[lo], (ItemPointer) key) < 0)
	{
		if (lo + step < n &&
			ItemPointerCompare((ItemPointer) &t[lo + step], (ItemPointer) key) < 0)
		{
			lo += step;
			step <<= 1;
		}
		else
			break;
	}
	/* binary search in (lo, min(lo+step, n)] */
	hi = Min(lo + step, n - 1);
	while (lo < hi)
	{
		int			mid = (lo + hi) / 2;

		if (ItemPointerCompare((ItemPointer) &t[mid], (ItemPointer) key) < 0)
			lo = mid + 1;
		else
			hi = mid;
	}
	return lo;
}

static TidSet
tidset_and(TidSet a, TidSet b)
{
	TidSet		r;
	int			i = 0,
				j = 0,
				k = 0;

	r.tids = palloc(Min(a.n, b.n) * sizeof(ItemPointerData) + 1);

	/*
	 * When the sets differ greatly in size, gallop the smaller through the
	 * larger (skip-list style) so a highly selective AND does not touch every
	 * posting of the common term.  Otherwise a linear merge is cheapest.
	 */
	if (a.n > 0 && b.n > 0 && (a.n > 4 * b.n || b.n > 4 * a.n))
	{
		const ItemPointerData *sm = a.n <= b.n ? a.tids : b.tids;
		const ItemPointerData *lg = a.n <= b.n ? b.tids : a.tids;
		int			sn = Min(a.n, b.n);
		int			ln = Max(a.n, b.n);
		int			li = 0;
		int			si;

		for (si = 0; si < sn; si++)
		{
			li = tidset_gallop(lg, ln, li, &sm[si]);
			if (li >= ln)
				break;
			if (ItemPointerCompare((ItemPointer) &lg[li], (ItemPointer) &sm[si]) == 0)
				r.tids[k++] = sm[si];
		}
		r.n = k;
		return r;
	}

	while (i < a.n && j < b.n)
	{
		int			c = ItemPointerCompare(&a.tids[i], &b.tids[j]);

		if (c == 0)
		{
			r.tids[k++] = a.tids[i];
			i++;
			j++;
		}
		else if (c < 0)
			i++;
		else
			j++;
	}
	r.n = k;
	return r;
}

static TidSet
tidset_or(TidSet a, TidSet b)
{
	TidSet		r;
	int			i = 0,
				j = 0,
				k = 0;

	r.tids = palloc((a.n + b.n) * sizeof(ItemPointerData) + 1);
	while (i < a.n && j < b.n)
	{
		int			c = ItemPointerCompare(&a.tids[i], &b.tids[j]);

		if (c == 0)
		{
			r.tids[k++] = a.tids[i];
			i++;
			j++;
		}
		else if (c < 0)
			r.tids[k++] = a.tids[i++];
		else
			r.tids[k++] = b.tids[j++];
	}
	while (i < a.n)
		r.tids[k++] = a.tids[i++];
	while (j < b.n)
		r.tids[k++] = b.tids[j++];
	r.n = k;
	return r;
}

/* a AND NOT b (b subtracted from a) */
static TidSet
tidset_andnot(TidSet a, TidSet b)
{
	TidSet		r;
	int			i = 0,
				j = 0,
				k = 0;

	r.tids = palloc(a.n * sizeof(ItemPointerData) + 1);
	while (i < a.n)
	{
		if (j >= b.n)
			r.tids[k++] = a.tids[i++];
		else
		{
			int			c = ItemPointerCompare(&a.tids[i], &b.tids[j]);

			if (c == 0)
			{
				i++;
				j++;
			}
			else if (c < 0)
				r.tids[k++] = a.tids[i++];
			else
				j++;
		}
	}
	r.n = k;
	return r;
}

/*
 * bm25_lookup_prefix -- union the posting lists of every dictionary term that
 * begins with the given prefix.  Dictionary entries are byte-sorted, so the
 * matching terms are contiguous: seek (via the sparse per-page block index) to
 * the page that can hold the prefix, then scan forward only while entries could
 * still start with the prefix, stopping at the first term that sorts past it.
 * Sublinear in the dictionary rather than a full scan.
 */
static void
bm25_lookup_prefix(Relation index, const BM25SegMeta *seg,
				   const char *prefix, int prefixlen, TidSet *out)
{
	BlockNumber blk = bm25_dict_seek(index, seg, prefix, prefixlen);
	int			cap = 32;
	int			n = 0;
	ItemPointerData *tids = palloc(cap * sizeof(ItemPointerData));
	bool		done = false;

	while (blk != InvalidBlockNumber && !done)
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
			int			cmplen = Min((int) de->termlen, prefixlen);
			int			c = memcmp(de->term, prefix, cmplen);

			if (c < 0 || (c == 0 && (int) de->termlen < prefixlen))
			{
				/* term sorts before the prefix: not there yet, keep scanning */
				ptr += esize;
				continue;
			}
			if (c > 0)
			{
				/* first prefixlen bytes exceed the prefix: sorted, so no more
				 * matches can follow -- stop */
				done = true;
				break;
			}
			/* c == 0 and de->termlen >= prefixlen: a prefix match */
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
			}
			ptr += esize;
		}
		UnlockReleaseBuffer(buffer);
		blk = next;
	}

	out->tids = tids;
	out->n = n;
	tidset_sort_uniq(out);
}

/*
 * Evaluate the query into a TidSet via a stack machine over the RPN items.
 * NOT is handled specially: a bare NOT is only meaningful as "a AND NOT b", so
 * we track whether each stack entry is "positive" (a TID set) or "negative"
 * (the complement of a TID set).  AND/OR combine them with De Morgan; a top-
 * level negative result is complemented against all indexed TIDs (via the
 * universe set).
 */
typedef struct EvalVal
{
	TidSet		set;
	bool		negated;		/* true => set represents docs NOT to include */
} EvalVal;

static TidSet
bm25_eval_query(Relation index, const BM25SegMeta *seg, FtsQuery q,
				TidSet universe)
{
	EvalVal    *stack;
	int			top = 0;
	uint32		i;
	TidSet		result;

	if (q->nitems == 0)
	{
		result.tids = NULL;
		result.n = 0;
		return result;
	}

	stack = palloc(q->nitems * sizeof(EvalVal));

	for (i = 0; i < q->nitems; i++)
	{
		FtsQueryItem *it = &q->items[i];

		if (it->type == FTS_QI_VAL)
		{
			TidSet		s;

			if (it->flags & FTS_QF_PREFIX)
				bm25_lookup_prefix(index, seg,
								   FTS_QUERY_ITEMTEXT(q, it), it->termlen, &s);
			else
				bm25_lookup_term(index, seg,
								 FTS_QUERY_ITEMTEXT(q, it), it->termlen, &s);
			stack[top].set = s;
			stack[top].negated = false;
			top++;
		}
		else if (it->op == FTS_OP_NOT)
		{
			Assert(top >= 1);
			stack[top - 1].negated = !stack[top - 1].negated;
		}
		else					/* AND / OR */
		{
			EvalVal		b = stack[--top];
			EvalVal		a = stack[--top];
			EvalVal		res;

			if (it->op == FTS_OP_AND || it->op == FTS_OP_PHRASE)
			{
				/* PHRASE is treated as AND for candidate generation; the
				 * bitmap heap recheck (@@@) enforces adjacency exactly. */
				if (!a.negated && !b.negated)
				{
					res.set = tidset_and(a.set, b.set);
					res.negated = false;
				}
				else if (!a.negated && b.negated)
				{
					res.set = tidset_andnot(a.set, b.set);
					res.negated = false;
				}
				else if (a.negated && !b.negated)
				{
					res.set = tidset_andnot(b.set, a.set);
					res.negated = false;
				}
				else			/* !a AND !b = !(a OR b) */
				{
					res.set = tidset_or(a.set, b.set);
					res.negated = true;
				}
			}
			else				/* OR */
			{
				if (!a.negated && !b.negated)
				{
					res.set = tidset_or(a.set, b.set);
					res.negated = false;
				}
				else if (a.negated && b.negated)	/* !a OR !b = !(a AND b) */
				{
					res.set = tidset_and(a.set, b.set);
					res.negated = true;
				}
				else
				{
					/* positive OR negative: !x OR y = !(x AND NOT y) */
					TidSet		pos = a.negated ? b.set : a.set;
					TidSet		neg = a.negated ? a.set : b.set;

					res.set = tidset_andnot(neg, pos);
					res.negated = true;
				}
			}
			stack[top++] = res;
		}
	}

	Assert(top == 1);
	if (stack[0].negated)
		result = tidset_andnot(universe, stack[0].set);
	else
		result = stack[0].set;

	return result;
}

/*
 * bm25_fuzzy_terms -- collect the postings of every dictionary term within edit
 * distance k of `term`, using the Levenshtein automaton (pg_fts_lev.c) directly
 * over the sorted dictionary.  This is EXACT: only true within-k terms are
 * collected, so no heap recheck is needed (unlike the trigram funnel, which
 * over-generates candidates that must be re-verified per doc).  Returns true
 * (always applicable); *out is a sorted TidSet.  For query terms longer than
 * the automaton bound, returns false so the caller falls back to the funnel.
 */
static bool
bm25_fuzzy_terms(Relation index, const BM25SegMeta *seg,
				 const char *term, int termlen, int k, TidSet *out)
{
	FtsLevAut	aut;
	BlockNumber blk;
	ItemPointerData *tids;
	unsigned char nextkey[FTS_LEV_MAXQ + 2];

	/* per-matching-term sorted runs, merged (not sorted) at the end */
	ItemPointerData **runs = NULL;
	int		   *runlen = NULL;
	int			nruns = 0;
	int			runcap = 0;
	int64		total = 0;

	if (termlen > FTS_LEV_MAXQ)
		return false;			/* fall back to trigram funnel + recheck */

	aut.q = (const unsigned char *) term;
	aut.m = termlen;
	aut.k = k;

	/*
	 * Automaton-guided dictionary walk.  Terms are byte-sorted, so when a term
	 * dead-ends at prefix cand[0..deadlen), every term sharing that prefix is
	 * also a dead end -- we jump past them by seeking (via the per-page block
	 * index) to the smallest string greater than that prefix.  This turns an
	 * O(all terms) scan into roughly O(matching terms + boundaries), the effect
	 * an FST/DFA intersection gives.
	 */
	blk = seg->dictstart;
	while (blk != InvalidBlockNumber)
	{
		Buffer		buffer = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;
		bool		reseek = false;

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;

		while (ptr < end)
		{
			BM25DictEntry *de = (BM25DictEntry *) ptr;
			Size		esize = MAXALIGN(offsetof(BM25DictEntry, term) + de->termlen);
			int			deadlen;
			bool		match;

			if (abs((int) de->termlen - termlen) <= k)
				match = fts_lev_match_prefix(&aut,
											 (const unsigned char *) de->term,
											 (int) de->termlen, &deadlen);
			else
			{
				/* run the automaton anyway to learn the dead prefix for skipping */
				match = fts_lev_match_prefix(&aut,
											 (const unsigned char *) de->term,
											 (int) de->termlen, &deadlen);
				match = false;		/* length filter still rules it out */
			}

			if (match)
			{
				BM25Posting *post;
				int			np = bm25_decode_term(index, de->firstposting,
												  de->firstoffset, de->df,
												  &post, NULL);

				/* keep this term's docid-sorted TIDs as a run for k-way merge */
				if (np > 0)
				{
					ItemPointerData *run = palloc(np * sizeof(ItemPointerData));
					int			i;

					for (i = 0; i < np; i++)
						run[i] = post[i].tid;
					if (nruns >= runcap)
					{
						runcap = Max(runcap * 2, 16);
						runs = runs ? repalloc(runs, runcap * sizeof(ItemPointerData *))
							: palloc(runcap * sizeof(ItemPointerData *));
						runlen = runlen ? repalloc(runlen, runcap * sizeof(int))
							: palloc(runcap * sizeof(int));
					}
					runs[nruns] = run;
					runlen[nruns] = np;
					nruns++;
					total += np;
				}
				pfree(post);
				ptr += esize;
				continue;
			}

			/*
			 * Dead end at cand[0..deadlen).  The next term that could match is
			 * >= that prefix with its last byte incremented.  If that key is
			 * beyond the next dictionary entry, seek to it via the block index
			 * (jumping whole pages); otherwise just step to the next entry.
			 */
			/*
			 * Skip only on a GENUINE prefix death: the automaton exceeded k while
			 * still consuming the term (deadlen < termlen), so every term sharing
			 * cand[0..deadlen) is dead.  If deadlen == termlen the term was fully
			 * consumed without dying (it failed only on length or final accept),
			 * so LONGER terms with this prefix may still match -- must NOT skip.
			 */
			if (deadlen > 0 && deadlen < (int) de->termlen)
			{
				int			kl = deadlen;

				memcpy(nextkey, de->term, kl);
				/* increment the last byte of the dead prefix; carry on 0xff */
				while (kl > 0 && nextkey[kl - 1] == 0xff)
					kl--;
				if (kl == 0)
				{
					/* prefix is all 0xff: nothing greater can match; done */
					UnlockReleaseBuffer(buffer);
					goto done;
				}
				nextkey[kl - 1]++;

				/* is the very next entry already >= nextkey? then no gain */
				{
					char	   *nptr = ptr + esize;

					if (nptr < end)
					{
						BM25DictEntry *nde = (BM25DictEntry *) nptr;
						int			cmplen = Min((int) nde->termlen, kl);
						int			c = memcmp(nde->term, nextkey, cmplen);

						if (c > 0 || (c == 0 && (int) nde->termlen >= kl))
						{
							ptr = nptr;		/* next entry is past the dead run */
							continue;
						}
					}
				}
				/* seek to the page holding nextkey; only jump if it advances to a
				 * LATER page (else keep scanning this page linearly -- avoids
				 * re-seeking onto the same/earlier page and looping) */
				{
					BlockNumber tgt = bm25_dict_seek(index, seg,
													 (const char *) nextkey, kl);

					if (tgt != InvalidBlockNumber && tgt != blk)
					{
						reseek = true;
						blk = tgt;
						UnlockReleaseBuffer(buffer);
						break;
					}
				}
				/* target is on this same page: just step to the next entry */
				ptr += esize;
				continue;
			}
			ptr += esize;
		}
		if (reseek)
			continue;
		UnlockReleaseBuffer(buffer);
		blk = next;
	}

done:
	/*
	 * k-way merge the per-term docid-sorted runs into one sorted, de-duplicated
	 * TID array.  Each posting list is already docid-ordered, so merging avoids
	 * the O(n log n) qsort over the whole (up to ~1.3M) union -- which a profiler
	 * showed was the dominant fuzzy-count cost (a 1.28M qsort with a
	 * function-pointer comparator is ~400ms) -- replacing it with O(n log k)
	 * and cheap inline comparisons.
	 */
	{
		int		   *pos;			/* current index into each run */
		int		   *heap;			/* min-heap of run indices by current head TID */
		int			hn = 0;
		int			nout = 0;
		int			r;

		tids = palloc(Max(total, 1) * sizeof(ItemPointerData));
		pos = palloc0(Max(nruns, 1) * sizeof(int));
		heap = palloc(Max(nruns, 1) * sizeof(int));

#define RUN_HEAD(ri) (&runs[(ri)][pos[(ri)]])
#define HEAP_LESS(x, y) (ItemPointerCompare(RUN_HEAD(heap[x]), RUN_HEAD(heap[y])) < 0)
		/* build the heap with each non-empty run's head */
		for (r = 0; r < nruns; r++)
		{
			if (runlen[r] > 0)
			{
				int			c = hn++;

				heap[c] = r;
				while (c > 0 && HEAP_LESS(c, (c - 1) / 2))
				{
					int			t = heap[c];

					heap[c] = heap[(c - 1) / 2];
					heap[(c - 1) / 2] = t;
					c = (c - 1) / 2;
				}
			}
		}
		while (hn > 0)
		{
			int			best = heap[0];
			int			c = 0;

			if (nout == 0 ||
				ItemPointerCompare(&tids[nout - 1], RUN_HEAD(best)) != 0)
				tids[nout++] = *RUN_HEAD(best);
			pos[best]++;
			if (pos[best] >= runlen[best])
			{
				heap[0] = heap[--hn];	/* drop exhausted run */
			}
			/* sift down heap[0] */
			for (;;)
			{
				int			l = 2 * c + 1,
							ri = 2 * c + 2,
							sm = c;

				if (hn == 0)
					break;
				if (l < hn && HEAP_LESS(l, sm))
					sm = l;
				if (ri < hn && HEAP_LESS(ri, sm))
					sm = ri;
				if (sm == c)
					break;
				{
					int			t = heap[c];

					heap[c] = heap[sm];
					heap[sm] = t;
					c = sm;
				}
			}
		}
#undef RUN_HEAD
#undef HEAP_LESS
		out->tids = tids;
		out->n = nout;
	}
	return true;
}

/* Build the universe: all TIDs present in any posting list. We collect it from
 * the dictionary lazily only if a top-level NOT requires it. */
static TidSet
bm25_universe(Relation index, BlockNumber dictstart)
{
	TidSet		u;
	BlockNumber blk = dictstart;
	int			cap = 64;
	int			n = 0;
	ItemPointerData *tids = palloc(cap * sizeof(ItemPointerData));

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
			ptr += esize;
		}
		UnlockReleaseBuffer(buffer);
		blk = next;
	}

	u.tids = tids;
	u.n = n;
	tidset_sort_uniq(&u);
	return u;
}

IndexScanDesc
bm25_beginscan(Relation r, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(r, nkeys, norderbys);
	BM25ScanOpaque so = (BM25ScanOpaque) palloc0(sizeof(BM25ScanOpaqueData));

	so->query = NULL;
	so->queryValid = false;
	so->orderInit = false;
	so->ordered = NULL;
	so->nordered = 0;
	so->ordpos = 0;
	so->plainInit = false;
	so->plainTids = NULL;
	so->nplain = 0;
	so->plainpos = 0;
	so->plainRecheck = false;
	scan->opaque = so;
	/* the AM owns allocation of the order-by result arrays */
	if (norderbys > 0)
	{
		scan->xs_orderbyvals = palloc0(sizeof(Datum) * norderbys);
		scan->xs_orderbynulls = palloc(sizeof(bool) * norderbys);
	}
	return scan;
}

void
bm25_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			ScanKey orderbys, int norderbys)
{
	BM25ScanOpaque so = (BM25ScanOpaque) scan->opaque;

	if (scankey && scan->numberOfKeys > 0)
		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));

	so->queryValid = false;
	if (scan->numberOfKeys >= 1)
	{
		FtsQuery	q = DatumGetFtsQuery(scan->keyData[0].sk_argument);

		so->query = q;
		so->queryValid = true;
	}

	/* ordering scan: the query is the <=> operator's right operand */
	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys,
				scan->numberOfOrderBys * sizeof(ScanKeyData));
	so->orderInit = false;
	so->ordered = NULL;
	so->nordered = 0;
	so->ordpos = 0;
	so->plainInit = false;
	so->plainTids = NULL;
	so->nplain = 0;
	so->plainpos = 0;
	so->plainRecheck = false;
	if (scan->numberOfOrderBys >= 1)
	{
		so->query = DatumGetFtsQuery(scan->orderByData[0].sk_argument);
		so->queryValid = true;
	}
}

/*
 * bm25_canreturn: whether the index can return a column value for an
 * index-only scan.  The bm25 index is NOT covering, so it cannot -- see the
 * body.
 */
bool
bm25_canreturn(Relation index, int attno)
{
	/*
	 * The bm25 index is NOT covering: it stores analyzed postings, not the
	 * original ftsdoc, so it cannot reproduce a column value.  Returning true
	 * caused an index-only scan that SELECTs the indexed column to yield NULLs
	 * (a placeholder tuple).  Return false so the planner never uses an
	 * index-only scan to fetch a real attribute.  (count(*)/EXISTS need no
	 * attribute but still include the @@@ restriction column in the IOS
	 * coverage check, so they run through a bitmap/plain index scan; our
	 * visibility-map-aware fts_count() is the explicit fast count.)
	 */
	return false;
}

/*
 * Fill scan->xs_itup with a cached all-NULL index tuple if the executor ever
 * requests one for an index-only scan (xs_want_itup).  Currently inactive:
 * bm25_canreturn() returns false, so the planner never chooses an index-only
 * scan and xs_want_itup is never set -- this is a guarded no-op kept so the
 * gettuple paths remain correct if a covering capability is ever added.
 */
static inline void
bm25_set_itup(IndexScanDesc scan, BM25ScanOpaque so)
{
	if (!scan->xs_want_itup)
		return;
	if (so->plainItup == NULL)
	{
		TupleDesc	td = RelationGetDescr(scan->indexRelation);
		Datum	   *values = palloc0(sizeof(Datum) * td->natts);
		bool	   *isnull = palloc(sizeof(bool) * td->natts);
		int			a;

		for (a = 0; a < td->natts; a++)
			isnull[a] = true;
		so->plainItup = index_form_tuple(td, values, isnull);
		so->plainItupDesc = td;
		pfree(values);
		pfree(isnull);
	}
	scan->xs_itup = so->plainItup;
	scan->xs_itupdesc = so->plainItupDesc;
}

/*
 * bm25_gettuple: ordering scan for ORDER BY (ftsdoc <=> ftsquery) LIMIT k.
 * On the first call it computes the block-max WAND top-k (visibility-filtered)
 * into scan state, then returns tuples one per call in ascending distance
 * (descending relevance), setting xs_orderbyvals so the executor can honor the
 * ORDER BY without a sort.  Only forward scans are supported.
 */
bool
bm25_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	BM25ScanOpaque so = (BM25ScanOpaque) scan->opaque;

	if (dir != ForwardScanDirection)
		elog(ERROR, "bm25: only forward ordering scans are supported");

	if (!so->queryValid || so->query == NULL)
		return false;

	/*
	 * Plain scan (no ORDER BY <=>): stream the matching TIDs in heap order for
	 * a plain Index Scan.  (The common @@@ path is the bitmap scan via
	 * amgetbitmap; this amgettuple path serves a plain index scan when the
	 * planner chooses one, e.g. with bitmap scans disabled.)  The matches come
	 * from the same evaluator, so results are identical; the executor applies
	 * MVCC visibility on the heap fetch.
	 */
	if (scan->numberOfOrderBys == 0)
	{
		if (!so->plainInit)
		{
			TidSet		m;

			bm25_collect_matches(scan->indexRelation, so->query, &m, &so->plainRecheck);
			so->plainTids = m.tids;
			so->nplain = m.n;
			so->plainpos = 0;
			so->plainInit = true;
		}
		if (so->plainpos >= so->nplain)
			return false;
		scan->xs_heaptid = so->plainTids[so->plainpos++];
		scan->xs_recheck = so->plainRecheck;
		bm25_set_itup(scan, so);
		return true;
	}

	if (!so->orderInit)
	{
		/*
		 * Adaptive-k WAND: start small so a small LIMIT (the common first page)
		 * does minimal work -- WAND prunes hard for small k -- and grow on
		 * demand.  The top-k engine over-fetches (wantk = k*4) for MVCC, and we
		 * KEEP all those extra ranked candidates (see bm25_topk_visible), so a
		 * page-sized LIMIT is usually served from the first pass without a
		 * recompute.  Growth is capped at the query's provable max hits so we
		 * never recompute past the actual result size.
		 */
		double		N;
		BM25MetaPageData m0;

		bm25_read_meta(scan->indexRelation, &m0);
		N = m0.ndocs < 1.0 ? 1.0 : m0.ndocs;
		so->maxhits = bm25_query_maxhits(scan->indexRelation, so->query, N);
		/*
		 * Start k at a full first page (100).  Measured trade: vs k=64 this costs
		 * the top-10 case ~1ms, but serves the entire LIMIT 11..100 range in ONE
		 * WAND pass instead of a pass-then-recompute (which for a common-term
		 * query is ~35ms vs ~12ms) -- a net reduction for typical "first page of
		 * results" pagination.  Beyond 100, grow x4 (capped).
		 */
		so->curk = 100;
		if ((double) so->curk > so->maxhits)
			so->curk = Max((int) so->maxhits, 1);
		so->nordered = bm25_topk_visible(scan->indexRelation, so->query,
										 so->curk, true, &so->ordered);
		so->ordpos = 0;
		so->orderInit = true;
	}

	/*
	 * Batch exhausted but it was full (nordered == curk): the executor wants
	 * more than we materialized.  Grow k and recompute, skipping the rows
	 * already returned.  (WAND is a batch top-k; this bounds work to demand
	 * without a full resumable-cursor rewrite.)
	 */
	if (so->ordpos >= so->nordered && so->nordered == so->curk)
	{
		int			prev = so->ordpos;

		/*
		 * Grow k for deeper scrolling, but cap it: (a) never past the query's
		 * provable max hits (no more results exist), and (b) never past an
		 * absolute ceiling -- a very large k defeats WAND pruning (threshold
		 * stays ~0) and degrades to O(result * df), so beyond the ceiling we
		 * stop growing and return what we have.  Deep top-N over a broad query
		 * is inherently expensive; the ceiling bounds worst-case latency.
		 */
		if ((double) so->curk >= so->maxhits)
			return false;		/* already have every possible match */
		if (so->curk >= BM25_MAX_ORDERK)
			return false;		/* refuse pathological deep pagination */
		so->curk *= 4;
		if ((double) so->curk > so->maxhits)
			so->curk = Max((int) so->maxhits, 1);
		if (so->curk > BM25_MAX_ORDERK)
			so->curk = BM25_MAX_ORDERK;
		so->nordered = bm25_topk_visible(scan->indexRelation, so->query,
										 so->curk, true, &so->ordered);
		so->ordpos = prev;		/* resume after the rows already emitted */
	}

	if (so->ordpos >= so->nordered)
		return false;

	scan->xs_heaptid = so->ordered[so->ordpos].tid;
	scan->xs_recheck = false;	/* score computed exactly from the index */
	bm25_set_itup(scan, so);
	if (scan->numberOfOrderBys > 0)
	{
		IndexOrderByDistance dist;
		Oid			typ = FLOAT8OID;

		dist.value = so->ordered[so->ordpos].score;
		dist.isnull = false;
		index_store_float8_orderby_distances(scan, &typ, &dist, false);
	}
	so->ordpos++;
	return true;
}

/*
 * bm25_collect_matches: evaluate the scan's query across all segments + the
 * pending list; return matching TIDs (sorted, unique) and a *recheck flag
 * (true iff any term used the over-generating trigram funnel / regex / NOT-
 * universe path).  Shared by the bitmap scan and the plain gettuple scan.
 */
static void
bm25_collect_matches(Relation index, FtsQuery query, TidSet *out, bool *recheck)
{
	BM25MetaPageData meta;
	TidSet		acc;
	TidSet		pending_acc;
	BM25Tombstones seg_tombs;
	bool		has_fuzzy_regex = false;
	bool		has_not = false;
	bool		need_recheck = false;
	uint32		i;
	uint32		s;

	acc.tids = NULL;
	acc.n = 0;
	*recheck = false;
	if (query == NULL)
	{
		*out = acc;
		return;
	}

	bm25_read_meta(index, &meta);

	for (i = 0; i < query->nitems; i++)
	{
		FtsQueryItem *it = &query->items[i];

		if (it->type == FTS_QI_OPR && it->op == FTS_OP_NOT)
			has_not = true;
		if (it->type == FTS_QI_VAL && (it->flags & (FTS_QF_FUZZY | FTS_QF_REGEX)))
			has_fuzzy_regex = true;
	}

	/*
	 * Load per-segment tombstones once.  Each segment's match contribution is
	 * filtered against THAT segment's own tombstone map before being unioned,
	 * because a heap TID deleted in one segment may be reused by a live doc in
	 * another segment or the pending list.
	 */
	bm25_tombstones_load(index, &meta, &seg_tombs);

	for (s = 0; s < meta.nsegments; s++)
	{
		BM25SegMeta *sg = &meta.segs[s];
		TidSet		universe;

		if (sg->dictstart == InvalidBlockNumber)
			continue;

		if (has_fuzzy_regex)
		{
			TidSet		cands;
			bool		any_trgm = false;
			bool		exact = (query->nitems == 1);
			uint32		qi;

			cands.tids = NULL;
			cands.n = 0;
			for (qi = 0; qi < query->nitems; qi++)
			{
				FtsQueryItem *it = &query->items[qi];
				TidSet		ts;

				if (it->type != FTS_QI_VAL ||
					!(it->flags & (FTS_QF_FUZZY | FTS_QF_REGEX)))
					continue;

				if (it->flags & FTS_QF_FUZZY)
				{
					if (bm25_fuzzy_terms(index, sg,
										 FTS_QUERY_ITEMTEXT(query, it),
										 it->termlen, (int) it->distance, &ts))
					{
						cands = tidset_or(cands, ts);
						any_trgm = true;
						continue;
					}
				}
				exact = false;
				if (bm25_trgm_candidates(index, sg->trgmstart,
										 sg->dictstart,
										 FTS_QUERY_ITEMTEXT(query, it),
										 it->termlen, 3,
										 (it->flags & FTS_QF_REGEX) != 0, &ts))
				{
					cands = tidset_or(cands, ts);
					any_trgm = true;
				}
				else
				{
					any_trgm = false;
					break;
				}
			}
			if (any_trgm)
			{
				if (!exact)
					need_recheck = true;
				if (cands.n > 0)
				{
					bm25_filter_tombstoned_seg(&seg_tombs, s, &cands);
					if (cands.n > 0)
						acc = tidset_or(acc, cands);
				}
			}
			else
			{
				need_recheck = true;
				universe = bm25_universe(index, sg->dictstart);
				if (universe.n > 0)
				{
					bm25_filter_tombstoned_seg(&seg_tombs, s, &universe);
					if (universe.n > 0)
						acc = tidset_or(acc, universe);
				}
			}
			continue;
		}

		if (has_not)
			universe = bm25_universe(index, sg->dictstart);
		else
		{
			universe.tids = NULL;
			universe.n = 0;
		}

		{
			TidSet		result = bm25_eval_query(index,
												 sg, query, universe);

			if (result.n > 0)
			{
				bm25_filter_tombstoned_seg(&seg_tombs, s, &result);
				if (result.n > 0)
					acc = tidset_or(acc, result);	/* exact -- no recheck */
			}
		}
	}

	/* pending list: verbatim docs matched by the exact per-doc matcher.
	 * Collect these separately from the segment matches: a pending doc is a
	 * live heap tuple that was just inserted, so it must NOT be subjected to
	 * the segment tombstone filter below -- a reused heap slot (same TID as a
	 * previously deleted, tombstoned doc) would otherwise be wrongly dropped. */
	pending_acc.tids = NULL;
	pending_acc.n = 0;
	if (meta.pendinghead != InvalidBlockNumber)
	{
		BlockNumber blk = meta.pendinghead;

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
				BM25PendingItem *pi = (BM25PendingItem *) ptr;
				FtsDoc		pdoc = (FtsDoc) ((char *) pi + sizeof(BM25PendingItem));

				if (fts_doc_matches(pdoc, query))
				{
					TidSet		one;

					one.tids = &pi->tid;
					one.n = 1;
					pending_acc = tidset_or(pending_acc, one);	/* exact per-doc match */
				}
				ptr += MAXALIGN(sizeof(BM25PendingItem) + pi->doclen);
			}
			UnlockReleaseBuffer(buffer);
			blk = next;
		}
	}

	tidset_sort_uniq(&acc);
	/*
	 * Segment matches were already filtered against each segment's own
	 * tombstone map above; pending matches are live tuples and are never
	 * tombstoned.  Just release the loaded maps.
	 */
	bm25_tombstones_free(&seg_tombs);
	/* fold in the (unfiltered) pending matches and re-uniq */
	if (pending_acc.n > 0)
	{
		acc = tidset_or(acc, pending_acc);
		tidset_sort_uniq(&acc);
	}
	*out = acc;
	*recheck = need_recheck;
}

int64
bm25_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	BM25ScanOpaque so = (BM25ScanOpaque) scan->opaque;
	TidSet		matches;
	bool		recheck;

	if (!so->queryValid || so->query == NULL)
		return 0;
	bm25_collect_matches(scan->indexRelation, so->query, &matches, &recheck);
	if (matches.n > 0)
		tbm_add_tuples(tbm, matches.tids, matches.n, recheck);
	return matches.n;
}
void
bm25_endscan(IndexScanDesc scan)
{
	/* memory is freed with the scan's context */
}

/* ----- index-maintained corpus statistics (stage 5) ----- */

/*
 * Look up a term's dictionary entry (df, max_tf, first posting block) without
 * reading any postings.  Returns true if found.  This is what the lazy WAND
 * cursors need to start; postings are then paged in on demand.
 */
static bool
bm25_lookup_dict(Relation index, const BM25SegMeta *seg,
				 const char *term, int termlen,
				 uint32 *df, uint32 *max_tf, BlockNumber *firstposting,
				 uint32 *firstoffset)
{
	BlockNumber blk = bm25_dict_seek(index, seg, term, termlen);
	bool		onlyone = (seg->dictindexstart != InvalidBlockNumber);

	while (blk != InvalidBlockNumber)
	{
		Buffer		buffer = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;
		bool		found = false;

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;

		while (ptr < end)
		{
			BM25DictEntry *de = (BM25DictEntry *) ptr;
			Size		esize = MAXALIGN(offsetof(BM25DictEntry, term) + de->termlen);

			if ((int) de->termlen == termlen &&
				memcmp(de->term, term, termlen) == 0)
			{
				*df = de->df;
				*max_tf = de->max_tf;
				*firstposting = de->firstposting;
				*firstoffset = de->firstoffset;
				found = true;
				break;
			}
			ptr += esize;
		}
		UnlockReleaseBuffer(buffer);
		if (found)
			return true;
		if (onlyone)
			break;				/* block index located the only possible page */
		blk = next;
	}
	*df = 0;
	*max_tf = 0;
	*firstposting = InvalidBlockNumber;
	*firstoffset = 0;
	return false;
}

/* Look up the document frequency of a term in the index, 0 if absent. */
static uint32
bm25_lookup_df(Relation index, const BM25SegMeta *seg,
			   const char *term, int termlen)
{
	BlockNumber blk = bm25_dict_seek(index, seg, term, termlen);
	bool		onlyone = (seg->dictindexstart != InvalidBlockNumber);

	while (blk != InvalidBlockNumber)
	{
		Buffer		buffer = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;
		uint32		df = 0;
		bool		found = false;

		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);
		ptr = (char *) PageGetContents(page);
		end = (char *) page + ((PageHeader) page)->pd_lower;
		next = BM25PageGetOpaque(page)->nextblk;

		while (ptr < end)
		{
			BM25DictEntry *de = (BM25DictEntry *) ptr;
			Size		esize = MAXALIGN(offsetof(BM25DictEntry, term) + de->termlen);

			if ((int) de->termlen == termlen &&
				memcmp(de->term, term, termlen) == 0)
			{
				df = de->df;
				found = true;
				break;
			}
			ptr += esize;
		}
		UnlockReleaseBuffer(buffer);
		if (found)
			return df;
		if (onlyone)
			break;
		blk = next;
	}
	return 0;
}

PG_FUNCTION_INFO_V1(fts_index_nsegments);

/* fts_index_nsegments(regclass) -> int : number of live segments */
Datum
fts_index_nsegments(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Relation	index;
	BM25MetaPageData meta;

	index = index_open(indexoid, AccessShareLock);
	bm25_read_meta(index, &meta);
	index_close(index, AccessShareLock);
	PG_RETURN_INT32((int32) meta.nsegments);
}

PG_FUNCTION_INFO_V1(fts_index_stats);

/* fts_index_stats(regclass) -> (ndocs float8, avgdl float8, nterms int) */
Datum
fts_index_stats(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	Relation	index;
	BM25MetaPageData meta;
	TupleDesc	tupdesc;
	Datum		values[3];
	bool		nulls[3] = {false, false, false};
	HeapTuple	tuple;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	index = index_open(indexoid, AccessShareLock);
	if (index->rd_rel->relam != get_index_am_oid("bm25", true))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not a bm25 index",
						RelationGetRelationName(index))));
	bm25_read_meta(index, &meta);
	index_close(index, AccessShareLock);

	values[0] = Float8GetDatum(meta.ndocs);
	values[1] = Float8GetDatum(meta.ndocs > 0 ?
							   meta.sumdoclen / meta.ndocs : 0.0);
	{
		uint32		s;
		int64		nterms = 0;

		for (s = 0; s < meta.nsegments; s++)
			nterms += meta.segs[s].nterms;
		values[2] = Int32GetDatum((int32) nterms);
	}

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

PG_FUNCTION_INFO_V1(fts_index_df);

/* fts_index_df(regclass, ftsquery) -> float8[] of df per distinct query term */
Datum
fts_index_df(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	FtsQuery	q = PG_GETARG_FTSQUERY(1);
	Relation	index;
	BM25MetaPageData meta;
	Datum	   *elems;
	int			n = 0;
	uint32		i;
	ArrayType  *result;

	index = index_open(indexoid, AccessShareLock);
	bm25_read_meta(index, &meta);

	elems = (Datum *) palloc(q->nitems * sizeof(Datum));
	for (i = 0; i < q->nitems; i++)
	{
		FtsQueryItem *it = &q->items[i];

		if (it->type == FTS_QI_VAL)
		{
			uint32		df = 0;
			uint32		s;

			/* document frequency is summed across all segments */
			for (s = 0; s < meta.nsegments; s++)
				df += bm25_lookup_df(index, &meta.segs[s],
									 FTS_QUERY_ITEMTEXT(q, it), it->termlen);
			elems[n++] = Float8GetDatum((double) (df == 0 ? 1 : df));
		}
	}
	index_close(index, AccessShareLock);

	result = construct_array(elems, n, FLOAT8OID, 8, true, 'd');
	PG_FREE_IF_COPY(q, 1);
	PG_RETURN_ARRAYTYPE_P(result);
}

/* ----- index-only scored top-K search (WAND-style) ----- */

#include "funcapi.h"
#include "access/htup_details.h"

/*
 * fts_search(index regclass, query ftsquery, k int)
 *   -> setof (ctid tid, score float8)
 *
 * Index-only BM25 top-k: scores are computed entirely from the index (postings
 * give per-doc tf, the dictionary gives df and the max-tf impact bound, the
 * metapage gives N and avgdl) with no heap access.  A WAND-style upper-bound
 * check on each document's best possible score prunes documents that cannot
 * enter the current top-k, which is the early-termination win.
 *
 * Cursors load posting pages lazily and use each page's block-max_tf (stored in
 * the page opaque) to skip entire pages whose best possible contribution cannot
 * beat the current top-k threshold -- block-max WAND -- so most of a long
 * posting list is never decoded.  Per-document |D| is read from the postings
 * for exact BM25 length normalization.
 */
/* ----- document-at-a-time block-max WAND top-k (item 2) ----- */

static int
cmp_scored_desc(const void *a, const void *b)
{
	double		sa = ((const ScoredTid *) a)->score;
	double		sb = ((const ScoredTid *) b)->score;

	if (sa < sb)
		return 1;
	if (sa > sb)
		return -1;
	return 0;
}

/*
 * A per-term cursor for the WAND merge.  posts is the term's docid-sorted
 * posting list; cursors load posting pages lazily from the index and skip
 * whole pages via the page block-max when they cannot beat the threshold.
 */
typedef struct WandCursor
{
	Relation	index;
	BlockNumber curblk;			/* page holding the current block */
	uint32		curoff;			/* byte offset of the CURRENT block on curblk */
	int			nread;			/* postings consumed so far (stop at df) */
	BlockNumber firstblk;		/* first posting block for the term */
	uint32		firstoff;		/* byte offset of the term's first block */
	uint32		df;				/* term document frequency (postings to read) */

	/*
	 * Current block only, decoded LAZILY: docids are unpacked eagerly (needed
	 * to pivot/skip), but tf and doclen stay bit-packed in blkbuf and are
	 * extracted per-posting on demand (bm25_for_get) only when a posting is
	 * actually scored -- so blocks pruned by block-max never pay for tf/dl.
	 */
	unsigned char *blkbuf;		/* copy of the current block's FOR payload */
	uint64		docids[BM25_BLOCK_SIZE];	/* decoded docids of current block */
	uint32		tfoff;			/* offset of tf column within blkbuf */
	uint32		dloff;			/* offset of doclen column within blkbuf */
	int			blkcount;		/* postings in the current block */
	uint32		blk_max_tf;		/* block-max tf (from header) */
	uint32		blk_min_dl;		/* block-min |D| (from header) */
	int			cur;			/* index within the current block */
	uint64		docid;			/* current docid (UINT64_MAX = exhausted) */

	double		idf;
	double		avgdl;
	double		k1b_inv_avgdl;	/* precomputed k1*b/avgdl (norm hot path) */
	double		k1_1mb;			/* precomputed k1*(1-b) */
	double		idf_k1p1;		/* precomputed idf*(k1+1) */
	double		max_contrib;	/* term-wide upper bound (shortest-doc norm) */
	BM25Tombstones *tombs;		/* loaded per-segment tombstones (or NULL) */
	uint32		segidx;			/* which segment this cursor's postings belong to */
	sm_cursor_cached_t tombcache;	/* stack MRU chunk cache for tombstone lookups */
	/*
	 * Optional docid range [docid_lo, docid_hi) for a parallel worker's slice.
	 * docid_lo = 0 and docid_hi = UINT64_MAX means "whole term" (the serial
	 * path).  The cursor seeks to docid_lo at prime time and reports itself
	 * exhausted (docid = UINT64_MAX) once it reaches docid_hi, so the WAND/
	 * MaxScore loops need no range awareness -- they already stop when every
	 * cursor is exhausted.  Ranges partition the corpus disjointly across
	 * workers, so each worker scores a disjoint candidate set exactly.
	 */
	uint64		docid_lo;
	uint64		docid_hi;
}			WandCursor;

static inline void wand_skip_own_tombstoned(WandCursor *c);
static void wand_seek(WandCursor *c, uint64 target);

static inline uint64
tid_to_docid_s(ItemPointer tid)
{
	return (uint64) ItemPointerGetBlockNumber(tid) *
		(uint64) MaxHeapTuplesPerPage +
		(uint64) ItemPointerGetOffsetNumber(tid);
}

/*
 * Lazily load the next page-worth of THIS TERM's postings into the cursor.
 * Decodes blocks starting at (c->curblk, c->curoff) until the page ends or the
 * term's df is exhausted, remembering where to resume (curblk/curoff) so a huge
 * term is streamed a page at a time -- WAND/BMW can then skip most of it without
 * ever decoding it (the whole point of block-max WAND).
 */
static void
wand_load_block(WandCursor *c)
{
	Buffer		buf;
	Page		page;
	char	   *p,
			   *pend;
	BM25BlockHdr *bh;
	const unsigned char *stream;
	uint64		gaps[BM25_BLOCK_SIZE];
	uint64		base;
	int			cnt;
	int			glen;
	int			tflen;
	int			i;

	if (c->blkbuf)
	{
		pfree(c->blkbuf);
		c->blkbuf = NULL;
	}
	if (c->curblk == InvalidBlockNumber || c->nread >= (int) c->df)
	{
		c->blkcount = 0;
		c->cur = 0;
		c->docid = UINT64_MAX;
		return;
	}

	buf = ReadBuffer(c->index, c->curblk);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	pend = (char *) page + ((PageHeader) page)->pd_lower;
	p = (char *) page + c->curoff;

	/* skip any empty tail; advance across pages until a real block or EOF */
	while (!(p + sizeof(BM25BlockHdr) <= pend) ||
		   ((BM25BlockHdr *) p)->count == 0)
	{
		BlockNumber next = BM25PageGetOpaque(page)->nextblk;

		UnlockReleaseBuffer(buf);
		if (next == InvalidBlockNumber)
		{
			c->curblk = InvalidBlockNumber;
			c->blkcount = 0;
			c->cur = 0;
			c->docid = UINT64_MAX;
			return;
		}
		c->curblk = next;
		c->curoff = MAXALIGN(SizeOfPageHeaderData);
		buf = ReadBuffer(c->index, c->curblk);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		pend = (char *) page + ((PageHeader) page)->pd_lower;
		p = (char *) page + c->curoff;
	}

	bh = (BM25BlockHdr *) p;
	stream = (const unsigned char *) (bh + 1);
	cnt = (int) bh->count;
	if (cnt > BM25_BLOCK_SIZE)
		cnt = BM25_BLOCK_SIZE;	/* defensive */

	/* copy the block's FOR payload so tf/dl bytes stay valid after we unlock */
	c->blkbuf = (unsigned char *) palloc(bh->bytelen);
	memcpy(c->blkbuf, stream, bh->bytelen);

	/* eagerly decode ONLY docids (gaps); record tf/dl column offsets for lazy
	 * per-posting access -- pruned blocks never touch tf/dl */
	glen = bm25_for_unpack(c->blkbuf, cnt, gaps);
	tflen = bm25_for_bytelen(c->blkbuf + glen, cnt);
	c->tfoff = (uint32) glen;
	c->dloff = (uint32) (glen + tflen);

	base = ((uint64) bh->first_docid_hi << 32) | bh->first_docid_lo;
	for (i = 0; i < cnt; i++)
	{
		base += gaps[i];		/* first gap is 0 from first_docid */
		c->docids[i] = base;
	}
	c->blkcount = cnt;
	c->blk_max_tf = bh->max_tf;
	c->blk_min_dl = bh->min_doclen;
	c->cur = 0;
	c->docid = c->docids[0];
	/* advance the resume pointer to the next block (or next page) */
	{
		char	   *nextp = (char *) MAXALIGN((char *) (bh + 1) + bh->bytelen);

		if (nextp + sizeof(BM25BlockHdr) <= pend &&
			c->nread + cnt < (int) c->df)
			c->curoff = (uint32) (nextp - (char *) page);
		else
		{
			c->curblk = BM25PageGetOpaque(page)->nextblk;
			c->curoff = MAXALIGN(SizeOfPageHeaderData);
		}
	}
	c->nread += cnt;
	UnlockReleaseBuffer(buf);
}

/* Prime the cursor at the term's first block and load it. */
static void
wand_prime(WandCursor *c)
{
	c->blkbuf = NULL;
	c->curblk = c->firstblk;
	c->curoff = c->firstoff;
	c->nread = 0;
	if (c->firstblk == InvalidBlockNumber || c->df == 0)
	{
		c->blkcount = 0;
		c->cur = 0;
		c->docid = UINT64_MAX;
		return;
	}
	wand_load_block(c);
	/* seek to this cursor's docid range start (parallel worker slice); for the
	 * serial path docid_lo == 0 so this is a no-op */
	if (c->docid_lo > 0 && c->docid != UINT64_MAX)
		wand_seek(c, c->docid_lo);
	else
		wand_skip_own_tombstoned(c);
}

/* The block-max contribution upper bound for the current 128-block.
 * Uses the block's max_tf AND min |D|: impact is increasing in tf and
 * decreasing in |D|, so impact(max_tf, min_dl) is a sound (and much tighter
 * than the shortest-possible-doc) upper bound for every posting in the block. */
static inline double
wand_block_max_contrib(WandCursor *c)
{
	double		k1 = 1.2;
	double		mtf = (double) c->blk_max_tf;
	double		mindl = (double) c->blk_min_dl;

	return c->idf * mtf * (k1 + 1.0) / (mtf + c->k1_1mb + c->k1b_inv_avgdl * mindl);
}

/* True if the cursor's CURRENT docid is tombstoned in the cursor's OWN
 * segment.  Tombstones are per-segment, so a cursor must ignore only its own
 * segment's deletions -- a reused heap TID that is live in another segment or
 * the pending list must still be produced by the segments that legitimately
 * contain it. */
static inline bool
wand_cur_own_tombstoned(WandCursor *c)
{
	if (c->tombs == NULL || !c->tombs->hasany || c->docid == UINT64_MAX)
		return false;
	if (c->segidx >= c->tombs->nseg || !c->tombs->present[c->segidx])
		return false;
	/* Cached MRU chunk cache (stack-allocated on the cursor): a cursor scans
	 * docids in ascending order into a small working set of chunks, so the
	 * cache turns repeated head-walks into O(1) hits. */
	return sm_contains_cached(&c->tombs->maps[c->segidx], c->docid,
							  &c->tombcache);
}

/* After the current docid is (re)positioned, skip forward over any docids
 * deleted in this cursor's own segment.  Loads successive blocks as needed;
 * wand_load_block does not itself skip, so there is no recursion. */
static inline void
wand_skip_own_tombstoned(WandCursor *c)
{
	while (wand_cur_own_tombstoned(c))
	{
		c->cur++;
		if (c->cur < c->blkcount)
			c->docid = c->docids[c->cur];
		else
			wand_load_block(c);
	}
	/* enforce the worker's docid range upper bound: past it, this cursor is
	 * done (its slice ends before docid_hi; another worker owns the rest) */
	if (c->docid >= c->docid_hi)
		c->docid = UINT64_MAX;
}

/* Advance the cursor to the next posting, loading the next block if needed. */
static void
wand_next(WandCursor *c)
{
	c->cur++;
	if (c->cur < c->blkcount)
		c->docid = c->docids[c->cur];
	else
		wand_load_block(c);		/* stream the next block of this term */
	wand_skip_own_tombstoned(c);
}

/*
 * Skip the cursor past the rest of its current 128-block.  Because the cursor
 * now holds exactly one block, this simply loads the next block -- and the
 * block just abandoned never had its tf/doclen decoded (block-max pruning pays
 * only for docids).  Always makes forward progress.
 */
static void
wand_skip_block(WandCursor *c)
{
	wand_load_block(c);
	wand_skip_own_tombstoned(c);
}

/* Exact BM25 contribution of the current posting.  tf and |D| are extracted
 * from the block's still-packed FOR columns ON DEMAND (bm25_for_get) -- only
 * for postings actually scored, so pruned blocks never decode tf/dl. */
static inline double
wand_contrib_cur(WandCursor *c)
{
	double		tf = (double) bm25_for_get(c->blkbuf + c->tfoff, c->cur);
	double		dl = (double) bm25_for_get(c->blkbuf + c->dloff, c->cur);
	double		norm = tf + c->k1_1mb + c->k1b_inv_avgdl * dl;

	return c->idf_k1p1 * tf / norm;
}

/*
 * Advance the cursor's paging state (curblk/curoff/nread) past whole 128-blocks
 * whose docids are all < target, reading only block HEADERS (no FOR decode).
 * A block is entirely below target when the NEXT block's first_docid <= target
 * (blocks are docid-ordered); the last block on a chain we cannot prove-skip
 * this way, so we stop and let the caller decode it.  This is what lets a seek
 * over a high-df term skip hundreds of thousands of postings without decoding.
 */
static void
wand_skip_blocks(WandCursor *c, uint64 target)
{
	while (c->curblk != InvalidBlockNumber && c->nread < (int) c->df)
	{
		Buffer		buf = ReadBuffer(c->index, c->curblk);
		Page		page;
		char	   *p,
				   *pend;
		BlockNumber nextblk;
		bool		stopped = false;

		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);
		pend = (char *) page + ((PageHeader) page)->pd_lower;
		nextblk = BM25PageGetOpaque(page)->nextblk;
		p = (char *) page + c->curoff;
		while (p + sizeof(BM25BlockHdr) <= pend && c->nread < (int) c->df)
		{
			BM25BlockHdr *bh = (BM25BlockHdr *) p;
			char	   *nextp;

			if (bh->count == 0)
			{
				stopped = true;
				break;
			}
			nextp = (char *) MAXALIGN((char *) (bh + 1) + bh->bytelen);
			/* can we prove this whole block is < target? need the next block's
			 * first_docid (on this page) to be <= target. */
			if (nextp + sizeof(BM25BlockHdr) <= pend)
			{
				BM25BlockHdr *nb = (BM25BlockHdr *) nextp;
				uint64		nbfirst = ((uint64) nb->first_docid_hi << 32) | nb->first_docid_lo;

				if (nbfirst <= target)
				{
					/* whole block < target: skip it (headers only) */
					c->nread += (int) bh->count;
					c->curoff = (uint32) (nextp - (char *) page);
					p = nextp;
					continue;
				}
			}
			/* this block may contain target (or is the page's last block): stop
			 * so the caller decodes from here */
			stopped = true;
			break;
		}
		if (!stopped)
		{
			/* consumed all blocks on this page as skippable; move to next page */
			c->curblk = nextblk;
			c->curoff = MAXALIGN(SizeOfPageHeaderData);
			UnlockReleaseBuffer(buf);
			continue;
		}
		UnlockReleaseBuffer(buf);
		return;
	}
}

/* Advance a cursor to the first posting with docid >= target (or exhaust). */
static void
wand_seek(WandCursor *c, uint64 target)
{
	if (c->docid >= target)
	{
		wand_skip_own_tombstoned(c);
		return;
	}
	/* first, fast-forward within the current (already-decoded) block's docids */
	while (c->cur < c->blkcount && c->docids[c->cur] < target)
		c->cur++;
	if (c->cur < c->blkcount)
	{
		c->docid = c->docids[c->cur];
		wand_skip_own_tombstoned(c);
		return;
	}
	/* current block exhausted: skip whole undecoded blocks by header, then
	 * load the block containing target and land on it */
	wand_skip_blocks(c, target);
	for (;;)
	{
		wand_load_block(c);
		if (c->docid == UINT64_MAX)
			return;
		while (c->cur < c->blkcount && c->docids[c->cur] < target)
			c->cur++;
		if (c->cur < c->blkcount)
		{
			c->docid = c->docids[c->cur];
			wand_skip_own_tombstoned(c);
			return;
		}
		/* target beyond this block; loop to load/skip the next */
	}
}

/*
 * fts_search_wand: exact top-k identical to the accumulate path, but using
 * document-at-a-time WAND so that documents which cannot enter the current
 * top-k are skipped via the per-term max-contribution bounds.  Returns the
 * number of (tid, score) results written to *out (palloc'd), capped at k.
 */
static int
fts_search_bmw(WandCursor *cursors, int nterms, int k, ScoredTid **out)
{
	ScoredTid  *heap;			/* min-heap of current top-k by score */
	int			nheap = 0;
	double		threshold = 0.0;
	int			t;

	heap = (ScoredTid *) palloc(Max(k, 1) * sizeof(ScoredTid));

	/* prime each cursor with its first page */
	for (t = 0; t < nterms; t++)
		wand_prime(&cursors[t]);

	for (;;)
	{
		int			i,
					j;
		uint64		pivot_docid;
		double		maxsum;
		double		score;

		/* selection-sort cursors by current docid (nterms is small) */
		for (i = 0; i < nterms; i++)
			for (j = i + 1; j < nterms; j++)
				if (cursors[j].docid < cursors[i].docid)
				{
					WandCursor	tmp = cursors[i];

					cursors[i] = cursors[j];
					cursors[j] = tmp;
				}

		if (cursors[0].docid == UINT64_MAX)
			break;				/* all exhausted */

		/*
		 * WAND pivot: accumulate max_contrib in docid order until the running
		 * sum could exceed the threshold; that cursor's docid is the pivot.
		 */
		maxsum = 0.0;
		pivot_docid = UINT64_MAX;
		for (i = 0; i < nterms; i++)
		{
			if (cursors[i].docid == UINT64_MAX)
				break;
			maxsum += cursors[i].max_contrib;
			if (maxsum > threshold || nheap < k)
			{
				pivot_docid = cursors[i].docid;
				break;
			}
		}
		if (pivot_docid == UINT64_MAX)
			break;				/* no document can beat the threshold */

		/*
		 * Block-max WAND (BMW) refinement: the pivot passed the term-wide
		 * bound, but the *current blocks* may bound tighter.  Sum the per-block
		 * max contribution of every cursor whose docid <= pivot; if even that
		 * cannot beat the threshold, no document up to the pivot can enter the
		 * top-k, so skip the earliest cursor past its current 128-block instead
		 * of scoring.  Sound because block max_tf >= every tf in the block.
		 */
		if (nheap >= k)
		{
			double		blocksum = 0.0;

			for (i = 0; i < nterms; i++)
			{
				if (cursors[i].docid == UINT64_MAX)
					break;
				if (cursors[i].docid <= pivot_docid)
					blocksum += wand_block_max_contrib(&cursors[i]);
			}
			if (blocksum <= threshold)
			{
				/* advance cursor[0] to the start of its next block */
				wand_skip_block(&cursors[0]);
				continue;
			}
		}

		/* if the smallest docid equals the pivot, score it fully */
		if (cursors[0].docid == pivot_docid)
		{
			ItemPointerData tid;

			bm25_docid_to_tid(pivot_docid, &tid);

			score = 0.0;
			for (i = 0; i < nterms; i++)
				if (cursors[i].docid == pivot_docid)
					score += wand_contrib_cur(&cursors[i]);

			/* push into the top-k min-heap */
			if (nheap < k)
			{
				heap[nheap].tid = tid;
				heap[nheap].score = score;
				nheap++;
				if (nheap == k)
				{
					threshold = heap[0].score;
					for (i = 1; i < nheap; i++)
						if (heap[i].score < threshold)
							threshold = heap[i].score;
				}
			}
			else if (score > threshold)
			{
				int			minpos = 0;

				for (i = 1; i < nheap; i++)
					if (heap[i].score < heap[minpos].score)
						minpos = i;
				heap[minpos].tid = tid;
				heap[minpos].score = score;
				threshold = heap[0].score;
				for (i = 1; i < nheap; i++)
					if (heap[i].score < threshold)
						threshold = heap[i].score;
			}

			/* advance every cursor positioned at the pivot */
			for (i = 0; i < nterms; i++)
				if (cursors[i].docid == pivot_docid)
					wand_next(&cursors[i]);
		}
		else
		{
			/*
			 * Advance every cursor before the pivot up to pivot_docid.  Use a
			 * seek (block-skipping) rather than stepping one posting at a time:
			 * for a high-df term this skips entire 128-blocks whose docids are
			 * all below the pivot, instead of decoding hundreds of thousands of
			 * postings individually (the Q5/Q7 cost).
			 */
			for (i = 0; i < nterms; i++)
				if (cursors[i].docid < pivot_docid)
					wand_seek(&cursors[i], pivot_docid);
		}
	}

	/* release any still-loaded block buffers */
	for (t = 0; t < nterms; t++)
		if (cursors[t].blkbuf)
			pfree(cursors[t].blkbuf);

	qsort(heap, nheap, sizeof(ScoredTid), cmp_scored_desc);
	*out = heap;
	return nheap;
}

/*
 * fts_search_maxscore: exact top-k via the MaxScore algorithm.  Cursors are
 * split into ESSENTIAL and NON-ESSENTIAL sets by ascending max_contrib: a
 * suffix of low-impact terms whose cumulative max_contrib cannot, by itself,
 * reach the current threshold is non-essential -- a document containing only
 * non-essential terms can never enter the top-k.  We therefore iterate
 * candidate docids from the ESSENTIAL cursors only (document-at-a-time over the
 * smallest essential docid), then add the non-essential terms' contributions by
 * seeking.  As the threshold rises, more terms become non-essential, so long
 * queries do progressively less work.  Complements BMW (which excels on short
 * queries); identical exact top-k.
 */
static int
fts_search_maxscore(WandCursor *cursors, int nterms, int k, ScoredTid **out)
{
	ScoredTid  *heap;
	int			nheap = 0;
	double		threshold = 0.0;
	double	   *suffix;			/* suffix[i] = sum of max_contrib[i..nterms) */
	int			t,
				i,
				j;
	int			first_essential;	/* cursors[first_essential..) are essential */

	heap = (ScoredTid *) palloc(Max(k, 1) * sizeof(ScoredTid));
	suffix = (double *) palloc((nterms + 1) * sizeof(double));

	for (t = 0; t < nterms; t++)
		wand_prime(&cursors[t]);

	/* order cursors by ascending term-wide max_contrib (once; it is static) */
	for (i = 0; i < nterms; i++)
		for (j = i + 1; j < nterms; j++)
			if (cursors[j].max_contrib < cursors[i].max_contrib)
			{
				WandCursor	tmp = cursors[i];

				cursors[i] = cursors[j];
				cursors[j] = tmp;
			}
	suffix[nterms] = 0.0;
	for (i = nterms - 1; i >= 0; i--)
		suffix[i] = suffix[i + 1] + cursors[i].max_contrib;

	first_essential = 0;

	for (;;)
	{
		uint64		cand = UINT64_MAX;
		double		score;

		/* recompute the essential boundary from the current threshold: the
		 * longest low-impact prefix whose max_contrib sum <= threshold is
		 * non-essential */
		if (nheap >= k)
		{
			while (first_essential < nterms &&
				   suffix[first_essential + 1] <= threshold)
				first_essential++;
		}

		/* smallest docid among essential cursors drives the iteration */
		for (i = first_essential; i < nterms; i++)
			if (cursors[i].docid < cand)
				cand = cursors[i].docid;
		if (cand == UINT64_MAX)
			break;				/* essential cursors exhausted */

		/* score cand: essential contributions + upper bound of non-essentials */
		score = 0.0;
		for (i = first_essential; i < nterms; i++)
			if (cursors[i].docid == cand)
				score += wand_contrib_cur(&cursors[i]);

		/* early-exit check: essential score + all non-essential max <= threshold
		 * => cand cannot make the top-k, skip the non-essential lookups */
		if (!(nheap >= k && score + suffix[first_essential] <= threshold))
		{
			/* add exact non-essential contributions by seeking to cand */
			for (i = 0; i < first_essential; i++)
			{
				wand_seek(&cursors[i], cand);
				if (cursors[i].docid == cand)
					score += wand_contrib_cur(&cursors[i]);
			}

			if (nheap < k)
			{
				ItemPointerData tid;

				bm25_docid_to_tid(cand, &tid);
				heap[nheap].tid = tid;
				heap[nheap].score = score;
				nheap++;
				if (nheap == k)
				{
					threshold = heap[0].score;
					for (i = 1; i < nheap; i++)
						if (heap[i].score < threshold)
							threshold = heap[i].score;
				}
			}
			else if (score > threshold)
			{
				ItemPointerData tid;
				int			minpos = 0;

				bm25_docid_to_tid(cand, &tid);
				for (i = 1; i < nheap; i++)
					if (heap[i].score < heap[minpos].score)
						minpos = i;
				heap[minpos].tid = tid;
				heap[minpos].score = score;
				threshold = heap[0].score;
				for (i = 1; i < nheap; i++)
					if (heap[i].score < threshold)
						threshold = heap[i].score;
			}
		}

		/* advance every essential cursor sitting at cand */
		for (i = first_essential; i < nterms; i++)
			if (cursors[i].docid == cand)
				wand_next(&cursors[i]);
	}

	for (t = 0; t < nterms; t++)
		if (cursors[t].blkbuf)
			pfree(cursors[t].blkbuf);

	qsort(heap, nheap, sizeof(ScoredTid), cmp_scored_desc);
	*out = heap;
	return nheap;
}

/*
 * Dispatch to the top-k algorithm best suited to the query shape.  BMW excels
 * on short queries (tight block-max pruning, cheap pivot); MaxScore does
 * progressively less work as terms become non-essential, winning on long
 * queries / large k.  Both return the identical exact top-k.
 */
static int
fts_search_wand(WandCursor *cursors, int nterms, int k, ScoredTid **out)
{
	if (nterms >= 4)
		return fts_search_maxscore(cursors, nterms, k, out);
	return fts_search_bmw(cursors, nterms, k, out);
}

/*
 * bm25_query_maxhits: an upper bound on the number of documents a query can
 * match, computed by walking the RPN with a stack -- VAL pushes the term's
 * global df; AND/PHRASE -> min of operands; OR -> sum; NOT -> corpus N (a NOT
 * can match almost everything).  Fuzzy/regex terms over-generate and have no
 * cheap df, so any such term makes the bound N (unbounded for our purposes).
 * Used to decide whether the ordering scan can compute the WHOLE result set in
 * one WAND pass (avoiding the adaptive-k recompute) when the result is small.
 */
static double
bm25_query_maxhits(Relation index, FtsQuery q, double N)
{
	BM25MetaPageData meta;
	double	   *stack;
	int			top = 0;
	uint32		i;
	double		result;

	if (q->nitems == 0)
		return 0;
	bm25_read_meta(index, &meta);
	stack = (double *) palloc(q->nitems * sizeof(double));

	for (i = 0; i < q->nitems; i++)
	{
		FtsQueryItem *it = &q->items[i];

		if (it->type == FTS_QI_VAL)
		{
			if (it->flags & (FTS_QF_FUZZY | FTS_QF_REGEX | FTS_QF_PREFIX))
				stack[top++] = N;	/* over-generating: no cheap bound */
			else
			{
				uint32		gdf = 0;
				uint32		s;

				for (s = 0; s < meta.nsegments; s++)
				{
					uint32		df,
								mtf;
					BlockNumber fb;
					uint32		fo;

					if (bm25_lookup_dict(index, &meta.segs[s],
										 FTS_QUERY_ITEMTEXT(q, it), it->termlen,
										 &df, &mtf, &fb, &fo))
						gdf += df;
				}
				stack[top++] = (double) gdf;
			}
		}
		else if (it->op == FTS_OP_NOT)
		{
			/* !x can match up to N docs */
			if (top >= 1)
				stack[top - 1] = N;
		}
		else					/* AND / OR / PHRASE: binary */
		{
			double		b = (top >= 1) ? stack[--top] : 0;
			double		a = (top >= 1) ? stack[--top] : 0;

			if (it->op == FTS_OP_OR)
				stack[top++] = a + b;
			else				/* AND, PHRASE: bounded by the smaller side */
				stack[top++] = Min(a, b);
		}
	}
	result = (top >= 1) ? stack[top - 1] : N;
	pfree(stack);
	return Min(result, N);
}

/*
 * bm25_topk_visible: shared top-k engine for both the fts_search SRF and the
 * amgettuple ordering scan.  Runs block-max WAND / MaxScore over the index's
 * SEGMENTS for `q`, over-fetches candidates so MVCC visibility filtering still
 * yields k visible rows, drops tombstoned docs, and returns them (palloc'd in
 * the current context) sorted by descending score.  When as_distance is true,
 * each result's .score field is replaced by the ordering distance 1/(1+score)
 * (ascending distance = the same order).  The index must already be open; the
 * base table is opened here for the visibility check.  Returns the number of
 * visible results.
 *
 * NOTE: ranked results cover the merged SEGMENTS only; documents still in the
 * pending write buffer (inserted since the last flush) are searchable by @@@
 * and counted by fts_count(), but are not ranked here until a flush folds them
 * into a segment (automatic on VACUUM, or immediate via fts_merge()).  Ranking
 * pending docs would require per-doc scoring outside the WAND cursors; deferred
 * intentionally, since pending is transient and bounded.
 */
static int
bm25_topk_candidates_range(Relation index, FtsQuery q, int wantk,
						   uint64 docid_lo, uint64 docid_hi, ScoredTid **out)
{
	BM25MetaPageData meta;
	double		N,
				avgdl;
	const char **terms;
	int		   *lens;
	int			nterms;
	WandCursor *cursors;
	ScoredTid  *cand;
	BM25Tombstones tombs;
	int			ncand;
	int			t,
				nactive = 0;
	double		k1 = 1.2;

	if (wantk < 1)
		wantk = 1;

	bm25_read_meta(index, &meta);
	N = meta.ndocs < 1.0 ? 1.0 : meta.ndocs;
	avgdl = meta.ndocs > 0 ? meta.sumdoclen / meta.ndocs : 1.0;

	nterms = fts_query_terms(q, &terms, &lens);
	/* up to one cursor per (term, segment) */
	cursors = (WandCursor *) palloc(Max(nterms * Max((int) meta.nsegments, 1), 1) *
									sizeof(WandCursor));

	/* per-segment tombstones: each cursor skips docids deleted in its own
	 * segment, so reused heap TIDs live in another segment still rank */
	bm25_tombstones_load(index, &meta, &tombs);

	for (t = 0; t < nterms; t++)
	{
		uint32		gdf = 0;
		uint32		s;
		double		idf;
		double		b = 0.75;

		/* global df across all segments -> IDF (segments share the corpus) */
		for (s = 0; s < meta.nsegments; s++)
		{
			uint32		df,
						max_tf;
			BlockNumber firstblk;
			uint32		firstoff;

			if (bm25_lookup_dict(index, &meta.segs[s], terms[t], lens[t],
								 &df, &max_tf, &firstblk, &firstoff))
				gdf += df;
		}
		if (gdf == 0)
			continue;			/* term absent in every segment */
		idf = log(1.0 + (N - (double) gdf + 0.5) / ((double) gdf + 0.5));

		/* one cursor per segment that contains the term */
		for (s = 0; s < meta.nsegments; s++)
		{
			uint32		df,
						max_tf;
			BlockNumber firstblk;
			uint32		firstoff;
			double		mtf;

			if (!bm25_lookup_dict(index, &meta.segs[s], terms[t], lens[t],
								  &df, &max_tf, &firstblk, &firstoff))
				continue;
			mtf = (double) max_tf;
			cursors[nactive].index = index;
			cursors[nactive].firstblk = firstblk;
			cursors[nactive].firstoff = firstoff;
			cursors[nactive].df = df;
			cursors[nactive].blkbuf = NULL;
			cursors[nactive].blkcount = 0;
			cursors[nactive].cur = 0;
			cursors[nactive].docid = 0;
			cursors[nactive].idf = idf;
			cursors[nactive].avgdl = avgdl;
			cursors[nactive].k1b_inv_avgdl = k1 * b / avgdl;
			cursors[nactive].k1_1mb = k1 * (1.0 - b);
			cursors[nactive].idf_k1p1 = idf * (k1 + 1.0);
			cursors[nactive].max_contrib =
				idf * mtf * (k1 + 1.0) / (mtf + k1 * (1.0 - b));
			cursors[nactive].tombs = &tombs;
			cursors[nactive].segidx = s;
			cursors[nactive].docid_lo = docid_lo;
			cursors[nactive].docid_hi = docid_hi;
			{
				sm_cursor_cached_t ini = SM_CURSOR_CACHED_INIT;

				cursors[nactive].tombcache = ini;
			}
			nactive++;
		}
	}

	ncand = fts_search_wand(cursors, nactive, wantk, &cand);
	bm25_tombstones_free(&tombs);

	*out = cand;
	return ncand;
}

/*
 * bm25_topk_visible: serial top-k for the fts_search SRF and the amgettuple
 * ordering scan.  Generates candidates over the WHOLE corpus (docid range
 * [0, MAX)) then applies MVCC visibility, over-fetching (wantk = k*4) so k
 * visible rows survive.  When as_distance is true each result's .score is the
 * ordering distance 1/(1+score).  Returns visible results (palloc'd) sorted by
 * descending score.
 */
static int
bm25_topk_visible(Relation index, FtsQuery q, int k, bool as_distance,
				  ScoredTid **out)
{
	ScoredTid  *cand;
	ScoredTid  *results;
	int			ncand;
	int			nvis = 0;
	int			i;
	int			wantk = Max(k * 4, 64);
	Snapshot	snap = GetActiveSnapshot();
	Relation	heap;
	IndexFetchTableData *fetch;

	ncand = bm25_topk_candidates_range(index, q, wantk, 0, UINT64_MAX, &cand);
	if (k < 1)
		k = 1;

	results = (ScoredTid *) palloc(Max(k, 1) * sizeof(ScoredTid));
	heap = table_open(index->rd_index->indrelid, AccessShareLock);
#if PG_VERSION_NUM >= 180000
	fetch = table_index_fetch_begin(heap, 0);
#else
	fetch = table_index_fetch_begin(heap);
#endif
	for (i = 0; i < ncand && nvis < k; i++)
	{
		ItemPointerData tid = cand[i].tid;
		bool		call_again = false;
		bool		all_dead = false;
		TupleTableSlot *slot = table_slot_create(heap, NULL);

		if (table_index_fetch_tuple(fetch, &tid, snap, slot,
									&call_again, &all_dead))
		{
			results[nvis] = cand[i];
			if (as_distance)
				results[nvis].score = 1.0 / (1.0 + cand[i].score);
			nvis++;
		}
		ExecDropSingleTupleTableSlot(slot);
	}
	table_index_fetch_end(fetch);
	table_close(heap, AccessShareLock);

	*out = results;
	return nvis;
}

/*
 * bm25_count_visible: MVCC-correct count of documents matching `q`, computed in
 * bulk from the index without the per-tuple executor round-trips of an
 * index(-only) scan.  Collect the matching TIDs (sorted), then count those
 * visible to the snapshot: on an all-visible heap page (visibility map) the
 * whole run of TIDs counts without touching the heap; only pages the VM does
 * not mark all-visible are probed with table_index_fetch_tuple.  This is the
 * count-pushdown path the CustomScan uses to answer count(*) at index speed.
 * If `recheck` is set (fuzzy/regex/NOT over-generation), a matched TID must
 * additionally pass the exact qual -- but those paths return recheck=false for
 * single-term fuzzy and exact boolean, so the common count cases are pure.
 */
static int64
bm25_count_visible(Relation index, FtsQuery q)
{
	TidSet		matches;
	bool		recheck;
	Snapshot	snap = GetActiveSnapshot();
	Relation	heap;
	IndexFetchTableData *fetch = NULL;
	Buffer		vmbuf = InvalidBuffer;
	int64		count = 0;
	int			i;

	bm25_collect_matches(index, q, &matches, &recheck);
	if (matches.n == 0)
		return 0;

	heap = table_open(index->rd_index->indrelid, AccessShareLock);

	/*
	 * If any term over-generated (recheck), we cannot count from the index
	 * alone -- fall back to fetching + rechecking each candidate.  (Rare: only
	 * regex, NOT, or funnel-fallback fuzzy.)  Otherwise the collected TIDs are
	 * exactly the matches and we only need visibility.
	 */
	if (recheck)
	{
#if PG_VERSION_NUM >= 180000
		fetch = table_index_fetch_begin(heap, 0);
#else
		fetch = table_index_fetch_begin(heap);
#endif
		for (i = 0; i < matches.n; i++)
		{
			ItemPointerData tid = matches.tids[i];
			bool		ca = false,
						ad = false;
			TupleTableSlot *slot = table_slot_create(heap, NULL);

			/* NB: recheck of the actual qual would go here; today recheck=true
			 * only for paths not reachable by count-pushdown gating, so this is
			 * a visibility-only fallback */
			if (table_index_fetch_tuple(fetch, &tid, snap, slot, &ca, &ad))
				count++;
			ExecDropSingleTupleTableSlot(slot);
		}
		table_index_fetch_end(fetch);
		table_close(heap, AccessShareLock);
		return count;
	}

	/* exact matches: count visible via the VM, heap-probing only dirty pages */
	for (i = 0; i < matches.n; i++)
	{
		BlockNumber blk = ItemPointerGetBlockNumber(&matches.tids[i]);

		if (VM_ALL_VISIBLE(heap, blk, &vmbuf))
		{
			/* whole page visible: this TID counts, no heap access */
			count++;
			continue;
		}
		/* page not all-visible: probe the heap for this TID's visibility */
		if (fetch == NULL)
		{
#if PG_VERSION_NUM >= 180000
			fetch = table_index_fetch_begin(heap, 0);
#else
			fetch = table_index_fetch_begin(heap);
#endif
		}
		{
			ItemPointerData tid = matches.tids[i];
			bool		ca = false,
						ad = false;
			TupleTableSlot *slot = table_slot_create(heap, NULL);

			if (table_index_fetch_tuple(fetch, &tid, snap, slot, &ca, &ad))
				count++;
			ExecDropSingleTupleTableSlot(slot);
		}
	}
	if (fetch != NULL)
		table_index_fetch_end(fetch);
	if (vmbuf != InvalidBuffer)
		ReleaseBuffer(vmbuf);
	table_close(heap, AccessShareLock);
	return count;
}

/*
 * bm25_count_visible_oid: same as fts_count() but callable from C with an index
 * OID (used by the COUNT-pushdown CustomScan).  Opens the index under
 * AccessShareLock, counts, closes.
 */
int64
bm25_count_visible_oid(Oid indexoid, FtsQuery q)
{
	Relation	index = index_open(indexoid, AccessShareLock);
	int64		c = bm25_count_visible(index, q);

	index_close(index, AccessShareLock);
	return c;
}

PG_FUNCTION_INFO_V1(fts_count);

/* fts_count(regclass, ftsquery) -> bigint : MVCC-correct count via the index */
Datum
fts_count(PG_FUNCTION_ARGS)
{
	Oid			indexoid = PG_GETARG_OID(0);
	FtsQuery	q = PG_GETARG_FTSQUERY(1);
	Relation	index;
	int64		c;

	index = index_open(indexoid, AccessShareLock);
	c = bm25_count_visible(index, q);
	index_close(index, AccessShareLock);
	PG_RETURN_INT64(c);
}

PG_FUNCTION_INFO_V1(fts_search);

Datum
fts_search(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	ScoredTid  *results;

	if (SRF_IS_FIRSTCALL())
	{
		Oid			indexoid = PG_GETARG_OID(0);
		FtsQuery	q = PG_GETARG_FTSQUERY(1);
		int			k = PG_GETARG_INT32(2);
		MemoryContext oldctx;
		Relation	index;
		int			nvis;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		index = index_open(indexoid, AccessShareLock);
		nvis = bm25_topk_visible(index, q, k, false, &results);
		index_close(index, AccessShareLock);

		funcctx->max_calls = nvis;
		funcctx->user_fctx = results;
		MemoryContextSwitchTo(oldctx);
	}

	funcctx = SRF_PERCALL_SETUP();
	results = (ScoredTid *) funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		Datum		values[2];
		bool		nulls[2] = {false, false};
		HeapTuple	tuple;
		ItemPointer tidcopy = palloc(sizeof(ItemPointerData));

		*tidcopy = results[funcctx->call_cntr].tid;
		values[0] = PointerGetDatum(tidcopy);
		values[1] = Float8GetDatum(results[funcctx->call_cntr].score);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	SRF_RETURN_DONE(funcctx);
}

