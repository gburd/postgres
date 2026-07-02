/*-------------------------------------------------------------------------
 *
 * pg_fts_am_scan.c
 *		Bitmap scan for the bm25 access method.
 *
 * Included directly into pg_fts_am.c (it shares static page helpers).  The
 * scan evaluates an ftsquery by set algebra over posting lists: a term yields
 * the set of TIDs whose document contains it; AND intersects, OR unions, and
 * NOT complements against the set of all indexed TIDs.  The result is added to
 * the caller's TIDBitmap.  This matches the @@@ semantics exactly and needs no
 * heap access.
 *
 * The skeleton materializes TID sets as sorted arrays.  A later stage replaces
 * this with a streaming WAND top-K when scoring is pushed into the AM.
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
static void bm25_collect_matches(IndexScanDesc scan, TidSet *out, bool *recheck);

/* A scored heap tuple (score, or distance in an ordering scan). */
typedef struct ScoredTid
{
	ItemPointerData tid;
	double		score;
}			ScoredTid;

static int bm25_topk_visible(Relation index, FtsQuery q, int k,
							 bool as_distance, ScoredTid **out);

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
 * TidSet.  Returns true if found.  Dictionary pages are scanned linearly
 * within the chain (entries are sorted, but variable-length, so a linear walk
 * is simplest for the skeleton).
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
 * begins with the given prefix.  Dictionary entries are sorted, but a simple
 * full scan is used here (the skeleton's dictionary is small); an FST or
 * front-coded prefix index is a later optimization.
 */
static void
bm25_lookup_prefix(Relation index, BlockNumber dictstart,
				   const char *prefix, int prefixlen, TidSet *out)
{
	BlockNumber blk = dictstart;
	int			cap = 32;
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

			if ((int) de->termlen >= prefixlen &&
				memcmp(de->term, prefix, prefixlen) == 0)
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
	BlockNumber dictstart = seg->dictstart;
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
				bm25_lookup_prefix(index, dictstart,
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
	BlockNumber blk = seg->dictstart;
	int			cap = 64;
	int			n = 0;
	ItemPointerData *tids;

	if (termlen > FTS_LEV_MAXQ)
		return false;			/* fall back to trigram funnel + recheck */

	aut.q = (const unsigned char *) term;
	aut.m = termlen;
	aut.k = k;
	tids = palloc(cap * sizeof(ItemPointerData));

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

			/* length filter: a within-k match needs ||cand|-|q|| <= k */
			if (abs((int) de->termlen - termlen) <= k &&
				fts_lev_match(&aut, (const unsigned char *) de->term,
							  (int) de->termlen))
			{
				BM25Posting *post;
				int			np = bm25_decode_term(index, de->firstposting,
												  de->firstoffset, de->df,
												  &post, NULL);
				int			i;

				for (i = 0; i < np; i++)
				{
					if (n >= cap)
					{
						cap *= 2;
						tids = repalloc(tids, cap * sizeof(ItemPointerData));
					}
					tids[n++] = post[i].tid;
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
 * bm25_canreturn: report that the index can return tuples for an index-only
 * scan.  The bm25 index is not covering (it stores analyzed postings, not the
 * original ftsdoc), so it cannot reproduce a column value -- but count(*) and
 * EXISTS reference no column, and for those the executor's index-only scan just
 * needs a TID stream plus the visibility-map check.  We therefore claim IOS
 * support unconditionally; the plain gettuple path returns an all-NULL itup
 * whose attributes are never read for a no-column scan.
 */
bool
bm25_canreturn(Relation index, int attno)
{
	return true;
}

/*
 * Fill scan->xs_itup with a cached all-NULL index tuple when the executor runs
 * an index-only scan (xs_want_itup).  The bm25 index is not covering, but
 * count(*)/EXISTS reference no column, so the attribute values are never read.
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
	 * Plain scan (no ORDER BY <=>): stream matching TIDs in heap order.  This
	 * enables the executor's index-only scan path for count(*)/existence
	 * queries: for each TID the executor consults the visibility map and skips
	 * the heap entirely on all-visible pages -- the same mechanism (and same
	 * MVCC guarantees) as a btree index-only scan, so a count over a VACUUMed
	 * table does no heap fetches.
	 */
	if (scan->numberOfOrderBys == 0)
	{
		if (!so->plainInit)
		{
			TidSet		m;

			bm25_collect_matches(scan, &m, &so->plainRecheck);
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
		 * does minimal work -- WAND prunes hard for small k -- and grow x8 on
		 * demand so deeper scrolls need at most one or two recomputes.  The
		 * top-k array + lazy cursors keep memory bounded.
		 */
		so->curk = 64;
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

		so->curk *= 4;
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
bm25_collect_matches(IndexScanDesc scan, TidSet *out, bool *recheck)
{
	BM25ScanOpaque so = (BM25ScanOpaque) scan->opaque;
	BM25MetaPageData meta;
	TidSet		acc;
	bool		has_fuzzy_regex = false;
	bool		has_not = false;
	bool		need_recheck = false;
	uint32		i;
	uint32		s;

	acc.tids = NULL;
	acc.n = 0;
	*recheck = false;
	if (!so->queryValid || so->query == NULL)
	{
		*out = acc;
		return;
	}

	bm25_read_meta(scan->indexRelation, &meta);

	for (i = 0; i < so->query->nitems; i++)
	{
		FtsQueryItem *it = &so->query->items[i];

		if (it->type == FTS_QI_OPR && it->op == FTS_OP_NOT)
			has_not = true;
		if (it->type == FTS_QI_VAL && (it->flags & (FTS_QF_FUZZY | FTS_QF_REGEX)))
			has_fuzzy_regex = true;
	}

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
			bool		exact = (so->query->nitems == 1);
			uint32		qi;

			cands.tids = NULL;
			cands.n = 0;
			for (qi = 0; qi < so->query->nitems; qi++)
			{
				FtsQueryItem *it = &so->query->items[qi];
				TidSet		ts;

				if (it->type != FTS_QI_VAL ||
					!(it->flags & (FTS_QF_FUZZY | FTS_QF_REGEX)))
					continue;

				if (it->flags & FTS_QF_FUZZY)
				{
					if (bm25_fuzzy_terms(scan->indexRelation, sg,
										 FTS_QUERY_ITEMTEXT(so->query, it),
										 it->termlen, (int) it->distance, &ts))
					{
						cands = tidset_or(cands, ts);
						any_trgm = true;
						continue;
					}
				}
				exact = false;
				if (bm25_trgm_candidates(scan->indexRelation, sg->trgmstart,
										 sg->dictstart,
										 FTS_QUERY_ITEMTEXT(so->query, it),
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
					acc = tidset_or(acc, cands);
			}
			else
			{
				need_recheck = true;
				universe = bm25_universe(scan->indexRelation, sg->dictstart);
				if (universe.n > 0)
					acc = tidset_or(acc, universe);
			}
			continue;
		}

		if (has_not)
			universe = bm25_universe(scan->indexRelation, sg->dictstart);
		else
		{
			universe.tids = NULL;
			universe.n = 0;
		}

		{
			TidSet		result = bm25_eval_query(scan->indexRelation,
												 sg, so->query, universe);

			if (result.n > 0)
				acc = tidset_or(acc, result);	/* exact -- no recheck */
		}
	}

	/* pending list: verbatim docs matched by the exact per-doc matcher */
	if (meta.pendinghead != InvalidBlockNumber)
	{
		BlockNumber blk = meta.pendinghead;

		while (blk != InvalidBlockNumber)
		{
			Buffer		buffer = ReadBuffer(scan->indexRelation, blk);
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

				if (fts_doc_matches(pdoc, so->query))
				{
					TidSet		one;

					one.tids = &pi->tid;
					one.n = 1;
					acc = tidset_or(acc, one);	/* exact per-doc match */
				}
				ptr += MAXALIGN(sizeof(BM25PendingItem) + pi->doclen);
			}
			UnlockReleaseBuffer(buffer);
			blk = next;
		}
	}

	tidset_sort_uniq(&acc);
	*out = acc;
	*recheck = need_recheck;
}

int64
bm25_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	TidSet		matches;
	bool		recheck;

	bm25_collect_matches(scan, &matches, &recheck);
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
	BlockNumber curblk;			/* page currently being streamed (lazy) */
	uint32		curoff;			/* resume byte offset on curblk */
	int			nread;			/* postings streamed so far (stop at df) */
	BlockNumber firstblk;		/* first posting block for the term */
	uint32		firstoff;		/* byte offset of the term's first block */
	uint32		df;				/* term document frequency (postings to read) */
	BM25Posting *posts;			/* the term's full docid-sorted posting list */
	uint32	   *blockmax;		/* per-posting 128-block max_tf (block-max WAND) */
	uint32	   *blockmindl;		/* per-posting 128-block min |D| (tightens the bound) */
	int			nposts;			/* total postings for the term */
	int			cur;			/* index within posts */
	uint64		docid;			/* current docid (UINT64_MAX = exhausted) */
	uint32		block_max_tf;	/* unused (kept for ABI of readers) */
	double		idf;
	double		avgdl;
	double		k1b_inv_avgdl;	/* precomputed k1*b/avgdl (norm hot path) */
	double		k1_1mb;			/* precomputed k1*(1-b) */
	double		idf_k1p1;		/* precomputed idf*(k1+1) */
	double		max_contrib;	/* term-wide upper bound (shortest-doc norm) */
}			WandCursor;

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
wand_load_page(WandCursor *c)
{
	BM25Posting *posts;
	uint32	   *bmax;
	uint32	   *bmindl;
	int			cap;
	int			n = 0;
	Buffer		buf;
	Page		page;
	char	   *p,
			   *pend;

	if (c->posts)
	{
		pfree(c->posts);
		c->posts = NULL;
	}
	if (c->blockmax)
	{
		pfree(c->blockmax);
		c->blockmax = NULL;
	}
	if (c->blockmindl)
	{
		pfree(c->blockmindl);
		c->blockmindl = NULL;
	}
	if (c->curblk == InvalidBlockNumber || c->nread >= (int) c->df)
	{
		c->nposts = 0;
		c->cur = 0;
		c->docid = UINT64_MAX;
		return;
	}

	cap = Min((int) c->df - c->nread, BM25_BLOCK_SIZE * 64);
	cap = Max(cap, 1);
	posts = (BM25Posting *) palloc(cap * sizeof(BM25Posting));
	bmax = (uint32 *) palloc(cap * sizeof(uint32));
	bmindl = (uint32 *) palloc(cap * sizeof(uint32));

	buf = ReadBuffer(c->index, c->curblk);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	pend = (char *) page + ((PageHeader) page)->pd_lower;
	p = (char *) page + c->curoff;
	while (p + sizeof(BM25BlockHdr) <= pend && c->nread + n < (int) c->df)
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
		/* grow buffer if this block would overflow it */
		if (n + cnt > cap)
		{
			cap = n + cnt;
			posts = repalloc(posts, cap * sizeof(BM25Posting));
			bmax = repalloc(bmax, cap * sizeof(uint32));
			bmindl = repalloc(bmindl, cap * sizeof(uint32));
		}
		pos += bm25_for_unpack(stream + pos, cnt, gaps);
		pos += bm25_for_unpack(stream + pos, cnt, tfs);
		pos += bm25_for_unpack(stream + pos, cnt, dls);
		for (i = 0; i < cnt && c->nread + n < (int) c->df; i++)
		{
			docid += gaps[i];
			bm25_docid_to_tid(docid, &posts[n].tid);
			posts[n].tf = (uint32) tfs[i];
			posts[n].doclen = (uint32) dls[i];
			bmax[n] = bh->max_tf;
			bmindl[n] = bh->min_doclen;
			n++;
		}
		p = (char *) (bh + 1) + bh->bytelen;
		p = (char *) MAXALIGN(p);
	}
	/* remember where to resume: rest of this page, or the next page */
	if (p + sizeof(BM25BlockHdr) <= pend && c->nread + n < (int) c->df)
	{
		c->curoff = (uint32) ((char *) p - (char *) page);
		/* curblk stays */
	}
	else
	{
		c->curblk = BM25PageGetOpaque(page)->nextblk;
		c->curoff = MAXALIGN(SizeOfPageHeaderData);
	}
	UnlockReleaseBuffer(buf);

	c->posts = posts;
	c->blockmax = bmax;
	c->blockmindl = bmindl;
	c->nposts = n;
	c->nread += n;
	c->cur = 0;
	if (n > 0)
		c->docid = tid_to_docid_s(&c->posts[0].tid);
	else
		wand_load_page(c);		/* skipped an empty tail; try next page */
}

/* Prime the cursor at the term's first block/offset and load its first page. */
static void
wand_prime(WandCursor *c)
{
	c->posts = NULL;
	c->blockmax = NULL;
	c->blockmindl = NULL;
	c->curblk = c->firstblk;
	c->curoff = c->firstoff;
	c->nread = 0;
	if (c->firstblk == InvalidBlockNumber || c->df == 0)
	{
		c->nposts = 0;
		c->cur = 0;
		c->docid = UINT64_MAX;
		return;
	}
	wand_load_page(c);
}

/* The block-max contribution upper bound for the current posting's 128-block.
 * Uses the block's max_tf AND min |D|: impact is increasing in tf and
 * decreasing in |D|, so impact(max_tf, min_dl) is a sound (and much tighter
 * than the shortest-possible-doc) upper bound for every posting in the block. */
static inline double
wand_block_max_contrib(WandCursor *c)
{
	double		k1 = 1.2;
	double		mtf = (double) (c->cur < c->nposts ? c->blockmax[c->cur] : 0);
	double		mindl = (double) (c->cur < c->nposts ? c->blockmindl[c->cur] : 0);

	return c->idf * mtf * (k1 + 1.0) / (mtf + c->k1_1mb + c->k1b_inv_avgdl * mindl);
}

/* Advance the cursor to the next posting, loading the next page if needed. */
static void
wand_next(WandCursor *c)
{
	c->cur++;
	if (c->cur < c->nposts)
		c->docid = tid_to_docid_s(&c->posts[c->cur].tid);
	else
		wand_load_page(c);		/* stream the next page of this term */
}

/*
 * Skip the cursor past the rest of its current 128-block (the run of postings
 * sharing this block's max_tf).  Used by BMW when the current block cannot beat
 * the top-k threshold.  Always makes forward progress.
 */
static void
wand_skip_block(WandCursor *c)
{
	if (c->cur < c->nposts)
	{
		uint32		bm = c->blockmax[c->cur];
		int			start = c->cur;

		while (c->cur < c->nposts && c->blockmax[c->cur] == bm)
			c->cur++;
		if (c->cur == start)
			c->cur++;			/* guarantee progress */
	}
	if (c->cur < c->nposts)
		c->docid = tid_to_docid_s(&c->posts[c->cur].tid);
	else
		wand_load_page(c);
}

/* Exact BM25 contribution of the current posting, using stored per-doc |D|.
 * Norm constants (idf*(k1+1), k1*(1-b), k1*b/avgdl) are precomputed per cursor
 * so the hot path is multiplies, not divisions. */
static inline double
wand_contrib_cur(WandCursor *c)
{
	double		tf = (double) c->posts[c->cur].tf;
	double		dl = (double) c->posts[c->cur].doclen;
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
		return;
	/* first, fast-forward within the already-decoded page */
	while (c->cur < c->nposts &&
		   tid_to_docid_s(&c->posts[c->cur].tid) < target)
		c->cur++;
	if (c->cur < c->nposts)
	{
		c->docid = tid_to_docid_s(&c->posts[c->cur].tid);
		return;
	}
	/* decoded page exhausted: skip whole undecoded blocks by header, then
	 * decode the page containing target and land on it */
	wand_skip_blocks(c, target);
	for (;;)
	{
		wand_load_page(c);
		if (c->docid == UINT64_MAX)
			return;
		while (c->cur < c->nposts &&
			   tid_to_docid_s(&c->posts[c->cur].tid) < target)
			c->cur++;
		if (c->cur < c->nposts)
		{
			c->docid = tid_to_docid_s(&c->posts[c->cur].tid);
			return;
		}
		/* target beyond this decoded page; loop to load/skip the next */
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
			ItemPointerData tid = cursors[0].posts[cursors[0].cur].tid;

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

	/* release any still-loaded pages */
	for (t = 0; t < nterms; t++)
		if (cursors[t].posts)
			pfree(cursors[t].posts);

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
		if (cursors[t].posts)
			pfree(cursors[t].posts);

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
 * bm25_topk_visible: shared top-k engine for both the fts_search SRF and the
 * amgettuple ordering scan.  Runs block-max WAND over the index for `q`,
 * over-fetches candidates so MVCC visibility filtering still yields k visible
 * rows, and returns them (palloc'd in the current context) sorted by
 * descending score.  When as_distance is true, each result's .score field is
 * replaced by the ordering distance 1/(1+score) (ascending distance = the same
 * order).  The index must already be open; the base table is opened here for
 * the visibility check.  Returns the number of visible results.
 */
static int
bm25_topk_visible(Relation index, FtsQuery q, int k, bool as_distance,
				  ScoredTid **out)
{
	BM25MetaPageData meta;
	double		N,
				avgdl;
	const char **terms;
	int		   *lens;
	int			nterms;
	WandCursor *cursors;
	ScoredTid  *cand;
	ScoredTid  *results;
	int			ncand;
	int			wantk;
	Relation	heap;
	IndexFetchTableData *fetch;
	Snapshot	snap = GetActiveSnapshot();
	int			nvis = 0;
	int			i,
				t,
				nactive = 0;
	double		k1 = 1.2;

	if (k < 1)
		k = 1;
	wantk = Max(k * 4, 64);

	bm25_read_meta(index, &meta);
	N = meta.ndocs < 1.0 ? 1.0 : meta.ndocs;
	avgdl = meta.ndocs > 0 ? meta.sumdoclen / meta.ndocs : 1.0;

	nterms = fts_query_terms(q, &terms, &lens);
	/* up to one cursor per (term, segment) */
	cursors = (WandCursor *) palloc(Max(nterms * Max((int) meta.nsegments, 1), 1) *
									sizeof(WandCursor));

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
			cursors[nactive].posts = NULL;
			cursors[nactive].blockmax = NULL;
			cursors[nactive].blockmindl = NULL;
			cursors[nactive].nposts = 0;
			cursors[nactive].cur = 0;
			cursors[nactive].docid = 0;
			cursors[nactive].idf = idf;
			cursors[nactive].avgdl = avgdl;
			cursors[nactive].k1b_inv_avgdl = k1 * b / avgdl;
			cursors[nactive].k1_1mb = k1 * (1.0 - b);
			cursors[nactive].idf_k1p1 = idf * (k1 + 1.0);
			cursors[nactive].max_contrib =
				idf * mtf * (k1 + 1.0) / (mtf + k1 * (1.0 - b));
			nactive++;
		}
	}

	ncand = fts_search_wand(cursors, nactive, wantk, &cand);

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

