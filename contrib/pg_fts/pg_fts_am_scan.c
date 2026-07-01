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
				np = bm25_page_decode(pp, &post);
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
					np = bm25_page_decode(pp, &post);
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
				np = bm25_page_decode(pp, &post);
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

/*
 * Look up a term's dictionary entry (df, max_tf, first posting block) without
 * reading any postings.  Returns true if found.  This is what the lazy WAND
 * cursors need to start; postings are then paged in on demand.
 */
static bool
bm25_lookup_dict(Relation index, BlockNumber dictstart,
				 const char *term, int termlen,
				 uint32 *df, uint32 *max_tf, BlockNumber *firstposting)
{
	BlockNumber blk = dictstart;

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
				found = true;
				break;
			}
			ptr += esize;
		}
		UnlockReleaseBuffer(buffer);
		if (found)
			return true;
		blk = next;
	}
	*df = 0;
	*max_tf = 0;
	*firstposting = InvalidBlockNumber;
	return false;
}

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

/*
 * A per-term cursor for the WAND merge.  posts is the term's docid-sorted
 * posting list; cursors load posting pages lazily from the index and skip
 * whole pages via the page block-max when they cannot beat the threshold.
 */
typedef struct WandCursor
{
	Relation	index;
	BlockNumber curblk;			/* block of the currently loaded page */
	BlockNumber firstblk;		/* first posting block for the term */
	BM25Posting *posts;			/* decoded postings of the current page */
	int			nposts;			/* count on the current page */
	int			cur;			/* index within the current page */
	uint64		docid;			/* current docid (UINT64_MAX = exhausted) */
	uint32		block_max_tf;	/* block-max tf of the current page */
	double		idf;
	double		avgdl;
	double		max_contrib;	/* term-wide upper bound (shortest-doc norm) */
}			WandCursor;

static inline uint64
tid_to_docid_s(ItemPointer tid)
{
	return (uint64) ItemPointerGetBlockNumber(tid) *
		(uint64) MaxHeapTuplesPerPage +
		(uint64) ItemPointerGetOffsetNumber(tid);
}

/* Load the posting page at blk into the cursor (decode + block-max). */
static void
wand_load_page(WandCursor *c, BlockNumber blk)
{
	Buffer		buf;
	Page		page;

	if (c->posts)
	{
		pfree(c->posts);
		c->posts = NULL;
	}
	if (blk == InvalidBlockNumber)
	{
		c->curblk = InvalidBlockNumber;
		c->nposts = 0;
		c->cur = 0;
		c->docid = UINT64_MAX;
		return;
	}
	buf = ReadBuffer(c->index, blk);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	c->nposts = bm25_page_decode(page, &c->posts);
	c->block_max_tf = BM25PageGetOpaque(page)->block_max_tf;
	c->curblk = blk;
	c->cur = 0;
	c->docid = c->nposts > 0 ? tid_to_docid_s(&c->posts[0].tid) : UINT64_MAX;
	/* if this page turned out empty, advance to the next */
	if (c->nposts == 0)
	{
		BlockNumber next = BM25PageGetOpaque(page)->nextblk;

		UnlockReleaseBuffer(buf);
		wand_load_page(c, next);
		return;
	}
	UnlockReleaseBuffer(buf);
}

/* The block-max contribution upper bound for the current page. */
static inline double
wand_block_max_contrib(WandCursor *c)
{
	double		k1 = 1.2,
				b = 0.75;
	double		mtf = (double) c->block_max_tf;

	return c->idf * mtf * (k1 + 1.0) / (mtf + k1 * (1.0 - b));
}

/* Advance the cursor to the next posting, loading the next page if needed. */
static void
wand_next(WandCursor *c)
{
	c->cur++;
	if (c->cur < c->nposts)
	{
		c->docid = tid_to_docid_s(&c->posts[c->cur].tid);
		return;
	}
	/* page exhausted: load the next block in the chain */
	{
		Buffer		buf;
		BlockNumber next;

		if (c->curblk == InvalidBlockNumber)
		{
			c->docid = UINT64_MAX;
			return;
		}
		buf = ReadBuffer(c->index, c->curblk);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		next = BM25PageGetOpaque(BufferGetPage(buf))->nextblk;
		UnlockReleaseBuffer(buf);
		wand_load_page(c, next);
	}
}

/* Exact BM25 contribution of the current posting, using stored per-doc |D|. */
static inline double
wand_contrib_cur(WandCursor *c)
{
	double		k1 = 1.2,
				b = 0.75;
	double		tf = (double) c->posts[c->cur].tf;
	double		dl = (double) c->posts[c->cur].doclen;
	double		norm = tf + k1 * (1.0 - b + b * dl / c->avgdl);

	return c->idf * tf * (k1 + 1.0) / norm;
}

/*
 * fts_search_wand: exact top-k identical to the accumulate path, but using
 * document-at-a-time WAND so that documents which cannot enter the current
 * top-k are skipped via the per-term max-contribution bounds.  Returns the
 * number of (tid, score) results written to *out (palloc'd), capped at k.
 */
static int
fts_search_wand(WandCursor *cursors, int nterms, int k, ScoredTid **out)
{
	ScoredTid  *heap;			/* min-heap of current top-k by score */
	int			nheap = 0;
	double		threshold = 0.0;
	int			t;

	heap = (ScoredTid *) palloc(Max(k, 1) * sizeof(ScoredTid));

	/* prime each cursor with its first page */
	for (t = 0; t < nterms; t++)
		wand_load_page(&cursors[t], cursors[t].firstblk);

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
			 * Advance cursors before the pivot toward pivot_docid.  Block-max
			 * skipping: if the whole current page's best contribution cannot
			 * lift a document above the threshold, skip past this posting; the
			 * per-page block_max bound makes this sound.
			 */
			for (i = 0; i < nterms; i++)
			{
				while (cursors[i].docid != UINT64_MAX &&
					   cursors[i].docid < pivot_docid)
					wand_next(&cursors[i]);
			}
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
		BM25MetaPageData meta;
		double		N;
		double		avgdl;
		const char **terms;
		int		   *lens;
		int			nterms;
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

		/*
		 * Document-at-a-time block-max WAND top-k.  Cursors are initialized from
		 * the dictionary only (df, max_tf, first block); posting pages are read
		 * lazily and skipped via block-max during the scan.  We over-fetch
		 * candidates (> k) so MVCC visibility filtering can still yield k visible
		 * rows when high-scoring postings point at dead tuples.
		 */
		{
			WandCursor *cursors = (WandCursor *) palloc(Max(nterms, 1) * sizeof(WandCursor));
			ScoredTid  *cand;
			int			ncand;
			int			wantk = Max(k * 4, 64);
			Relation	heap;
			IndexFetchTableData *fetch;
			Snapshot	snap = GetActiveSnapshot();
			int			nvis = 0;
			int			i;
			double		k1 = 1.2;
			int			nactive = 0;

			for (t = 0; t < nterms; t++)
			{
				uint32		df,
							max_tf;
				BlockNumber firstblk;
				double		idf,
							mtf,
							b = 0.75;

				if (!bm25_lookup_dict(index, meta.dictstart, terms[t], lens[t],
									  &df, &max_tf, &firstblk))
					continue;		/* term absent: skip */

				idf = log(1.0 + (N - (double) df + 0.5) / ((double) df + 0.5));
				mtf = (double) max_tf;
				cursors[nactive].index = index;
				cursors[nactive].firstblk = firstblk;
				cursors[nactive].posts = NULL;
				cursors[nactive].nposts = 0;
				cursors[nactive].cur = 0;
				cursors[nactive].docid = 0;
				cursors[nactive].idf = idf;
				cursors[nactive].avgdl = avgdl;
				/*
				 * WAND upper bound: maximized at tf = max_tf and the shortest
				 * document (|D| -> 0 gives norm = tf + k1*(1-b)), a sound bound
				 * that never underestimates so no qualifying hit is pruned.
				 */
				cursors[nactive].max_contrib =
					idf * mtf * (k1 + 1.0) / (mtf + k1 * (1.0 - b));
				nactive++;
			}

			ncand = fts_search_wand(cursors, nactive, wantk, &cand);

			/* MVCC: keep only visible tuples, in score order, up to k */
			results = (ScoredTid *) palloc(Max(k, 1) * sizeof(ScoredTid));
			heap = table_open(index->rd_index->indrelid, AccessShareLock);
			fetch = table_index_fetch_begin(heap, 0);
			for (i = 0; i < ncand && nvis < k; i++)
			{
				ItemPointerData tid = cand[i].tid;
				bool		call_again = false;
				bool		all_dead = false;
				TupleTableSlot *slot = table_slot_create(heap, NULL);

				if (table_index_fetch_tuple(fetch, &tid, snap, slot,
											&call_again, &all_dead))
					results[nvis++] = cand[i];
				ExecDropSingleTupleTableSlot(slot);
			}
			table_index_fetch_end(fetch);
			table_close(heap, AccessShareLock);

			funcctx->max_calls = nvis;
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

