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

typedef struct BM25ScanOpaqueData
{
	FtsQuery	query;			/* copied into the scan's context */
	bool		queryValid;
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
 * Look up a term in the dictionary; on hit, read its full posting list into a
 * TidSet.  Returns true if found.  Dictionary pages are scanned linearly
 * within the chain (entries are sorted, but variable-length, so a linear walk
 * is simplest for the skeleton).
 */
static bool
bm25_lookup_term(Relation index, BlockNumber dictstart,
				 const char *term, int termlen, TidSet *out)
{
	BlockNumber blk = dictstart;

	out->tids = NULL;
	out->n = 0;

	while (blk != InvalidBlockNumber)
	{
		Buffer		buffer = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr;
		char	   *end;
		BlockNumber firstposting = InvalidBlockNumber;
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
				found = true;
				break;
			}
			ptr += esize;
		}
		blk = BM25PageGetOpaque(page)->nextblk;
		UnlockReleaseBuffer(buffer);

		if (found)
		{
			/* read the posting chain */
			BlockNumber pblk = firstposting;
			int			cap = 16;
			int			n = 0;
			ItemPointerData *tids = palloc(cap * sizeof(ItemPointerData));

			while (pblk != InvalidBlockNumber)
			{
				Buffer		pb = ReadBuffer(index, pblk);
				Page		pp;
				BM25Posting *post;
				int			np;
				int			i;

				LockBuffer(pb, BUFFER_LOCK_SHARE);
				pp = BufferGetPage(pb);
				post = (BM25Posting *) PageGetContents(pp);
				np = (((PageHeader) pp)->pd_lower -
					  ((char *) PageGetContents(pp) - (char *) pp)) /
					sizeof(BM25Posting);
				for (i = 0; i < np; i++)
				{
					if (n >= cap)
					{
						cap *= 2;
						tids = repalloc(tids, cap * sizeof(ItemPointerData));
					}
					tids[n++] = post[i].tid;
				}
				pblk = BM25PageGetOpaque(pp)->nextblk;
				UnlockReleaseBuffer(pb);
			}
			out->tids = tids;
			out->n = n;
			tidset_sort_uniq(out);
			return true;
		}
	}
	return false;
}

/* set operations on sorted TidSets */
static TidSet
tidset_and(TidSet a, TidSet b)
{
	TidSet		r;
	int			i = 0,
				j = 0,
				k = 0;

	r.tids = palloc(Min(a.n, b.n) * sizeof(ItemPointerData) + 1);
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
					post = (BM25Posting *) PageGetContents(pp);
					np = (((PageHeader) pp)->pd_lower -
						  ((char *) PageGetContents(pp) - (char *) pp)) /
						sizeof(BM25Posting);
					for (k = 0; k < np; k++)
					{
						if (n >= cap)
						{
							cap *= 2;
							tids = repalloc(tids, cap * sizeof(ItemPointerData));
						}
						tids[n++] = post[k].tid;
					}
					pblk = BM25PageGetOpaque(pp)->nextblk;
					UnlockReleaseBuffer(pb);
				}
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
bm25_eval_query(Relation index, BlockNumber dictstart, FtsQuery q,
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
				bm25_lookup_prefix(index, dictstart,
								   FTS_QUERY_ITEMTEXT(q, it), it->termlen, &s);
			else
				bm25_lookup_term(index, dictstart,
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

			if (it->op == FTS_OP_AND)
			{
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
				post = (BM25Posting *) PageGetContents(pp);
				np = (((PageHeader) pp)->pd_lower -
					  ((char *) PageGetContents(pp) - (char *) pp)) /
					sizeof(BM25Posting);
				for (k = 0; k < np; k++)
				{
					if (n >= cap)
					{
						cap *= 2;
						tids = repalloc(tids, cap * sizeof(ItemPointerData));
					}
					tids[n++] = post[k].tid;
				}
				pblk = BM25PageGetOpaque(pp)->nextblk;
				UnlockReleaseBuffer(pb);
			}
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
	scan->opaque = so;
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
}

int64
bm25_getbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	BM25ScanOpaque so = (BM25ScanOpaque) scan->opaque;
	BM25MetaPageData meta;
	TidSet		result;
	TidSet		universe;
	bool		need_universe;
	int64		ntids = 0;

	if (!so->queryValid || so->query == NULL)
		return 0;

	bm25_read_meta(scan->indexRelation, &meta);
	if (meta.dictstart == InvalidBlockNumber)
		return 0;

	/*
	 * A top-level NOT needs the universe.  We conservatively build it whenever
	 * the query contains any NOT operator; cheap queries without NOT skip it.
	 */
	need_universe = false;
	{
		uint32		i;

		for (i = 0; i < so->query->nitems; i++)
			if (so->query->items[i].type == FTS_QI_OPR &&
				so->query->items[i].op == FTS_OP_NOT)
			{
				need_universe = true;
				break;
			}
	}

	if (need_universe)
		universe = bm25_universe(scan->indexRelation, meta.dictstart);
	else
	{
		universe.tids = NULL;
		universe.n = 0;
	}

	result = bm25_eval_query(scan->indexRelation, meta.dictstart,
							 so->query, universe);

	if (result.n > 0)
	{
		tbm_add_tuples(tbm, result.tids, result.n, false);
		ntids = result.n;
	}

	return ntids;
}

void
bm25_endscan(IndexScanDesc scan)
{
	/* memory is freed with the scan's context */
}
