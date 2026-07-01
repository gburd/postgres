/*-------------------------------------------------------------------------
 *
 * pg_fts_rank.c
 *		BM25 / BM25F relevance scoring for pg_fts.
 *
 * Stage 4 of pg_fts.  Implements the Okapi BM25 score of a document against a
 * query.  BM25 needs corpus statistics that an ftsdoc alone does not carry:
 * the document count N, the average document length avgdl, and per-term
 * document frequency df.  Until the bm25 index access method maintains those
 * (a later stage), this file computes the score from statistics supplied by
 * the caller, which is enough to validate the scoring math by sequential scan
 * and to reproduce reference scores (Lucene/bm25s) for conformance testing.
 *
 * Score:
 *	 score(D,Q) = sum_t IDF(t) * ( f(t,D)*(k1+1) )
 *							  / ( f(t,D) + k1*(1 - b + b*|D|/avgdl) )
 * with the Lucene-style IDF:
 *	 IDF(t) = ln(1 + (N - df + 0.5) / (df + 0.5))
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_rank.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "pg_fts.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"

#define BM25_DEFAULT_K1		1.2
#define BM25_DEFAULT_B		0.75

/*
 * Collect the distinct term operands referenced by a query.  Operators and
 * duplicate terms are ignored -- BM25 sums over the query's terms present in
 * the document, regardless of the boolean structure (matching Lucene, which
 * scores the disjunction of query terms).
 */
static int
query_terms(FtsQuery q, const char ***terms_out, int **lens_out)
{
	const char **terms;
	int		   *lens;
	int			n = 0;
	uint32		i;

	terms = (const char **) palloc(q->nitems * sizeof(char *));
	lens = (int *) palloc(q->nitems * sizeof(int));

	for (i = 0; i < q->nitems; i++)
	{
		FtsQueryItem *it = &q->items[i];

		if (it->type == FTS_QI_VAL)
		{
			terms[n] = FTS_QUERY_ITEMTEXT(q, it);
			lens[n] = it->termlen;
			n++;
		}
	}
	*terms_out = terms;
	*lens_out = lens;
	return n;
}

/*
 * fts_bm25_score -- core scorer.
 *
 * dfs may be NULL, in which case every term is treated as having df = 1 (as if
 * it were rare); this yields a usable ranking when true df is unavailable.
 * When dfs is provided it must have one entry per distinct query term, in the
 * order query_terms() returns them.
 */
static double
fts_bm25_score(FtsDoc doc, FtsQuery q, double N, double avgdl,
			   const double *dfs, double k1, double b)
{
	const char **terms;
	int		   *lens;
	int			nterms;
	int			i;
	double		score = 0.0;
	double		dl = (double) doc->doclen;

	if (avgdl <= 0.0)
		avgdl = 1.0;

	nterms = query_terms(q, &terms, &lens);

	for (i = 0; i < nterms; i++)
	{
		FtsTermEntry *e = fts_doc_lookup(doc, terms[i], lens[i]);
		double		tf;
		double		df;
		double		idf;
		double		norm;

		if (e == NULL)
			continue;			/* term absent: contributes nothing */

		tf = (double) e->tf;
		df = (dfs != NULL) ? dfs[i] : 1.0;
		if (df < 1.0)
			df = 1.0;
		if (df > N)
			df = N;

		/* Lucene-style IDF, always >= 0 */
		idf = log(1.0 + (N - df + 0.5) / (df + 0.5));

		norm = tf + k1 * (1.0 - b + b * dl / avgdl);
		if (norm <= 0.0)
			continue;
		score += idf * (tf * (k1 + 1.0)) / norm;
	}

	pfree(terms);
	pfree(lens);
	return score;
}

/*
 * SQL: fts_bm25(doc, query, n_docs float8, avgdl float8 [, dfs float8[]])
 * Returns the BM25 score.  k1/b use the standard defaults.
 */
PG_FUNCTION_INFO_V1(fts_bm25);

Datum
fts_bm25(PG_FUNCTION_ARGS)
{
	FtsDoc		doc;
	FtsQuery	q;
	double		N;
	double		avgdl;
	double	   *dfs = NULL;
	int			ndfs = 0;
	double		score;

	/* non-STRICT because dfs is optional; guard the required args */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) ||
		PG_ARGISNULL(2) || PG_ARGISNULL(3))
		PG_RETURN_NULL();

	doc = PG_GETARG_FTSDOC(0);
	q = PG_GETARG_FTSQUERY(1);
	N = PG_GETARG_FLOAT8(2);
	avgdl = PG_GETARG_FLOAT8(3);

	if (!PG_ARGISNULL(4))
	{
		ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(4);
		Datum	   *elems;
		bool	   *nulls;
		int			i;

		if (ARR_ELEMTYPE(arr) != FLOAT8OID)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("dfs array must be float8[]")));
		deconstruct_array(arr, FLOAT8OID, 8, true, 'd',
						  &elems, &nulls, &ndfs);
		dfs = (double *) palloc(ndfs * sizeof(double));
		for (i = 0; i < ndfs; i++)
			dfs[i] = nulls[i] ? 1.0 : DatumGetFloat8(elems[i]);
	}

	if (N < 1.0)
		N = 1.0;

	score = fts_bm25_score(doc, q, N, avgdl, dfs,
						   BM25_DEFAULT_K1, BM25_DEFAULT_B);

	PG_FREE_IF_COPY(doc, 0);
	PG_FREE_IF_COPY(q, 1);
	PG_RETURN_FLOAT8(score);
}
