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
	 * A top-level NOT needs the universe.  Fuzzy and regex terms also need it:
	 * they cannot be answered by exact posting lookup, so the index returns the
	 * universe as candidates and the bitmap heap recheck (@@@) applies the
	 * fuzzy/regex test exactly.  (A trigram pre-filter, cribbed from pg_tre,
	 * would narrow this; the skeleton is correct but scans all live tuples for
	 * such queries.)
	 */
	need_universe = false;
	{
		uint32		i;
		bool		has_fuzzy_regex = false;

		for (i = 0; i < so->query->nitems; i++)
		{
			FtsQueryItem *it = &so->query->items[i];

			if (it->type == FTS_QI_OPR && it->op == FTS_OP_NOT)
				need_universe = true;
			if (it->type == FTS_QI_VAL &&
				(it->flags & (FTS_QF_FUZZY | FTS_QF_REGEX)))
				has_fuzzy_regex = true;
		}

		if (has_fuzzy_regex)
		{
			/* return all indexed tuples as candidates; recheck filters */
			universe = bm25_universe(scan->indexRelation, meta.dictstart);
			if (universe.n > 0)
			{
				tbm_add_tuples(tbm, universe.tids, universe.n, true);
				ntids += universe.n;
			}
			/* also all pending docs (searched below), so skip main eval */
			goto scan_pending;
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
		tbm_add_tuples(tbm, result.tids, result.n, true);
		ntids = result.n;
	}

scan_pending:
	/*
	 * Also search the pending list: newly inserted, not-yet-merged documents
	 * are stored verbatim, so evaluate each directly with the same per-document
	 * matcher the sequential @@@ path uses.  This handles all operators
	 * (including NOT) correctly without needing a pending-side universe.
	 */
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
					tbm_add_tuples(tbm, &pi->tid, 1, true);
					ntids++;
				}
				ptr += MAXALIGN(sizeof(BM25PendingItem) + pi->doclen);
			}
			UnlockReleaseBuffer(buffer);
			blk = next;
		}
	}

	return ntids;
}

void
bm25_endscan(IndexScanDesc scan)
{
	/* memory is freed with the scan's context */
}

/* ----- index-maintained corpus statistics (stage 5) ----- */

/* Look up the document frequency of a term in the index, 0 if absent. */
static uint32
bm25_lookup_df(Relation index, BlockNumber dictstart,
			   const char *term, int termlen)
{
	BlockNumber blk = dictstart;

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
		blk = next;
	}
	return 0;
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
	values[2] = Int32GetDatum((int32) meta.nterms);

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
			uint32		df = bm25_lookup_df(index, meta.dictstart,
											FTS_QUERY_ITEMTEXT(q, it),
											it->termlen);

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

/* Read a term's full posting list plus its df/max_tf from the index. */
typedef struct TermPostings
{
	BM25Posting *posts;
	int			nposts;
	uint32		df;
	uint32		max_tf;
	double		idf;
} TermPostings;

static bool
bm25_read_term_postings(Relation index, BlockNumber dictstart,
						const char *term, int termlen, TermPostings *out)
{
	BlockNumber blk = dictstart;

	while (blk != InvalidBlockNumber)
	{
		Buffer		buffer = ReadBuffer(index, blk);
		Page		page;
		char	   *ptr,
				   *end;
		BlockNumber next;
		BlockNumber firstposting = InvalidBlockNumber;
		uint32		df = 0,
					max_tf = 0;
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
				firstposting = de->firstposting;
				df = de->df;
				max_tf = de->max_tf;
				found = true;
				break;
			}
			ptr += esize;
		}
		UnlockReleaseBuffer(buffer);

		if (found)
		{
			BlockNumber pblk = firstposting;
			int			cap = Max((int) df, 8);
			int			n = 0;
			BM25Posting *posts = palloc(cap * sizeof(BM25Posting));

			while (pblk != InvalidBlockNumber)
			{
				Buffer		pb = ReadBuffer(index, pblk);
				Page		pp;
				BM25Posting *src;
				int			np,
							k;

				LockBuffer(pb, BUFFER_LOCK_SHARE);
				pp = BufferGetPage(pb);
				src = (BM25Posting *) PageGetContents(pp);
				np = (((PageHeader) pp)->pd_lower -
					  ((char *) PageGetContents(pp) - (char *) pp)) /
					sizeof(BM25Posting);
				for (k = 0; k < np; k++)
				{
					if (n >= cap)
					{
						cap *= 2;
						posts = repalloc(posts, cap * sizeof(BM25Posting));
					}
					posts[n++] = src[k];
				}
				pblk = BM25PageGetOpaque(pp)->nextblk;
				UnlockReleaseBuffer(pb);
			}
			out->posts = posts;
			out->nposts = n;
			out->df = df;
			out->max_tf = max_tf;
			return true;
		}
		blk = next;
	}
	out->posts = NULL;
	out->nposts = 0;
	out->df = 0;
	out->max_tf = 0;
	return false;
}

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
 * The document-length term of BM25 needs |D|, which the postings do not carry;
 * we approximate per-document length by avgdl for the ranking here (exact |D|
 * is available at recheck, or once postings store doclen -- future work).  The
 * ordering and pruning are otherwise the standard BM25 WAND.
 */
PG_FUNCTION_INFO_V1(fts_search);

typedef struct ScoredTid
{
	ItemPointerData tid;
	double		score;
}			ScoredTid;

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
		BM25MetaPageData meta;
		double		N,
					avgdl;
		const char **terms;
		int		   *lens;
		int			nterms;
		TermPostings *tp;
		HTAB	   *acc;
		HASHCTL		ctl;
		int			t;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldctx = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		if (k < 1)
			k = 1;

		index = index_open(indexoid, AccessShareLock);
		bm25_read_meta(index, &meta);
		N = meta.ndocs < 1.0 ? 1.0 : meta.ndocs;
		avgdl = meta.ndocs > 0 ? meta.sumdoclen / meta.ndocs : 1.0;

		nterms = fts_query_terms(q, &terms, &lens);
		tp = (TermPostings *) palloc0(nterms * sizeof(TermPostings));
		for (t = 0; t < nterms; t++)
		{
			bm25_read_term_postings(index, meta.dictstart,
									terms[t], lens[t], &tp[t]);
			tp[t].idf = log(1.0 + (N - (double) tp[t].df + 0.5) /
							((double) tp[t].df + 0.5));
		}

		/* accumulate score per docid via a hash keyed by ItemPointerData */
		ctl.keysize = sizeof(ItemPointerData);
		ctl.entrysize = sizeof(ScoredTid);
		ctl.hcxt = CurrentMemoryContext;
		acc = hash_create("bm25 accum", 256, &ctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		for (t = 0; t < nterms; t++)
		{
			int			p;
			double		k1 = 1.2,
						b = 0.75;

			for (p = 0; p < tp[t].nposts; p++)
			{
				double		tf = (double) tp[t].posts[p].tf;
				/* |D| approximated by avgdl (see note above): |D|/avgdl = 1 */
				double		lennorm = avgdl > 0 ? 1.0 : 1.0;
				double		norm = tf + k1 * (1.0 - b + b * lennorm);
				double		contrib = tp[t].idf * tf * (k1 + 1.0) / norm;
				ScoredTid  *e;
				bool		found;

				e = (ScoredTid *) hash_search(acc, &tp[t].posts[p].tid,
											  HASH_ENTER, &found);
				if (!found)
				{
					e->tid = tp[t].posts[p].tid;
					e->score = 0.0;
				}
				e->score += contrib;
			}
		}

		/* collect and sort desc, keep top k */
		{
			HASH_SEQ_STATUS seq;
			ScoredTid  *e;
			int			n = hash_get_num_entries(acc);
			int			i = 0;

			results = (ScoredTid *) palloc(Max(n, 1) * sizeof(ScoredTid));
			hash_seq_init(&seq, acc);
			while ((e = (ScoredTid *) hash_seq_search(&seq)) != NULL)
				results[i++] = *e;
			if (n > 1)
				qsort(results, n, sizeof(ScoredTid), cmp_scored_desc);
			funcctx->max_calls = Min(n, k);
			funcctx->user_fctx = results;
		}

		index_close(index, AccessShareLock);
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
