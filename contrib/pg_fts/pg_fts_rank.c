/*-------------------------------------------------------------------------
 *
 * pg_fts_rank.c
 *		BM25 / BM25F relevance scoring for pg_fts.
 *
 * Stage 4 of pg_fts.  Implements the Okapi BM25 score of a document against a
 * query.  BM25 needs corpus statistics that an ftsdoc alone does not carry:
 * the document count N, the average document length avgdl, and per-term
 * document frequency df.  The bm25 index access method maintains these (in its
 * metapage and dictionary), and the index scan paths score index-only; this
 * file computes the score from statistics supplied by the caller, which also
 * validates the scoring math by sequential scan and reproduces reference
 * scores (Lucene/bm25s) for conformance testing.
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

/* IDF / scoring variants (matching common implementations) */
typedef enum BM25Variant
{
	BM25_LUCENE = 0,			/* ln(1 + (N-df+0.5)/(df+0.5)); always >= 0 */
	BM25_ROBERTSON,				/* classic ln((N-df+0.5)/(df+0.5)); can be < 0 */
	BM25_ATIRE,					/* ln(N/df) */
	BM25_BM25PLUS,				/* Lucene IDF + delta floor on tf term */
	BM25_BM25L					/* rank_bm25 BM25L: delta shift on the
								 * length-normalized tf (helps long docs) */
}			BM25Variant;

#define BM25PLUS_DELTA		1.0
#define BM25L_DELTA			0.5

static double
bm25_idf(BM25Variant v, double N, double df)
{
	if (df < 1.0)
		df = 1.0;
	if (df > N)
		df = N;

	switch (v)
	{
		case BM25_ROBERTSON:
			return log((N - df + 0.5) / (df + 0.5));
		case BM25_ATIRE:
			return log(N / df);
		case BM25_BM25L:
			/* rank_bm25 BM25L IDF: ln((N+1)/(df+0.5)) */
			return log((N + 1.0) / (df + 0.5));
		case BM25_LUCENE:
		case BM25_BM25PLUS:
		default:
			return log(1.0 + (N - df + 0.5) / (df + 0.5));
	}
}

/*
 * Collect the distinct term operands referenced by a query.  Operators and
 * duplicate terms are ignored -- BM25 sums over the query's terms present in
 * the document, regardless of the boolean structure (matching Lucene, which
 * scores the disjunction of query terms).
 */
int
fts_query_terms(FtsQuery q, const char ***terms_out, int **lens_out)
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
 * order fts_query_terms() returns them.
 */
static double
fts_bm25_score(FtsDoc doc, FtsQuery q, double N, double avgdl,
			   const double *dfs, double k1, double b, BM25Variant variant)
{
	const char **terms;
	int		   *lens;
	int			nterms;
	int			i;
	double		score = 0.0;
	double		dl = (double) doc->doclen;

	if (avgdl <= 0.0)
		avgdl = 1.0;

	nterms = fts_query_terms(q, &terms, &lens);

	for (i = 0; i < nterms; i++)
	{
		FtsTermEntry *e = fts_doc_lookup(doc, terms[i], lens[i]);
		double		tf;
		double		df;
		double		idf;
		double		norm;
		double		sat;

		if (e == NULL)
			continue;			/* term absent: contributes nothing */

		tf = (double) e->tf;
		df = (dfs != NULL) ? dfs[i] : 1.0;
		idf = bm25_idf(variant, N, df);

		if (variant == BM25_BM25L)
		{
			/*
			 * BM25L: shift the LENGTH-NORMALIZED tf (ctd) by a delta before
			 * saturation, so long documents are not over-penalized.
			 *   ctd = tf / (1 - b + b*dl/avgdl)
			 *   sat = (k1+1)*(ctd+delta) / (k1 + ctd + delta)
			 */
			double		lennorm = 1.0 - b + b * dl / avgdl;
			double		ctd;

			if (lennorm <= 0.0)
				continue;
			ctd = tf / lennorm;
			sat = (k1 + 1.0) * (ctd + BM25L_DELTA) /
				(k1 + ctd + BM25L_DELTA);
			score += idf * sat;
			continue;
		}

		norm = tf + k1 * (1.0 - b + b * dl / avgdl);
		if (norm <= 0.0)
			continue;
		sat = (tf * (k1 + 1.0)) / norm;
		if (variant == BM25_BM25PLUS)
			sat += BM25PLUS_DELTA;	/* lower-bound the tf saturation */
		score += idf * sat;
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
						   BM25_DEFAULT_K1, BM25_DEFAULT_B, BM25_LUCENE);

	PG_FREE_IF_COPY(doc, 0);
	PG_FREE_IF_COPY(q, 1);
	PG_RETURN_FLOAT8(score);
}

static BM25Variant
parse_variant(text *v)
{
	char	   *s = text_to_cstring(v);
	BM25Variant r = BM25_LUCENE;

	if (strcmp(s, "lucene") == 0)
		r = BM25_LUCENE;
	else if (strcmp(s, "robertson") == 0)
		r = BM25_ROBERTSON;
	else if (strcmp(s, "atire") == 0)
		r = BM25_ATIRE;
	else if (strcmp(s, "bm25+") == 0 || strcmp(s, "bm25plus") == 0)
		r = BM25_BM25PLUS;
	else if (strcmp(s, "bm25l") == 0 || strcmp(s, "l") == 0)
		r = BM25_BM25L;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unknown bm25 variant \"%s\"", s),
				 errhint("Valid variants: lucene, robertson, atire, bm25+, bm25l.")));
	pfree(s);
	return r;
}

/*
 * SQL: fts_bm25_opts(doc, query, n_docs, avgdl, k1, b, variant, dfs)
 * Full-control scorer for reproducing reference implementations.
 */
PG_FUNCTION_INFO_V1(fts_bm25_opts);

Datum
fts_bm25_opts(PG_FUNCTION_ARGS)
{
	FtsDoc		doc;
	FtsQuery	q;
	double		N,
				avgdl,
				k1,
				b;
	BM25Variant variant;
	double	   *dfs = NULL;
	int			ndfs = 0;
	double		score;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ||
		PG_ARGISNULL(3) || PG_ARGISNULL(4) || PG_ARGISNULL(5) ||
		PG_ARGISNULL(6))
		PG_RETURN_NULL();

	doc = PG_GETARG_FTSDOC(0);
	q = PG_GETARG_FTSQUERY(1);
	N = PG_GETARG_FLOAT8(2);
	avgdl = PG_GETARG_FLOAT8(3);
	k1 = PG_GETARG_FLOAT8(4);
	b = PG_GETARG_FLOAT8(5);
	variant = parse_variant(PG_GETARG_TEXT_PP(6));

	if (!PG_ARGISNULL(7))
	{
		ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(7);
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

	score = fts_bm25_score(doc, q, N, avgdl, dfs, k1, b, variant);

	PG_FREE_IF_COPY(doc, 0);
	PG_FREE_IF_COPY(q, 1);
	PG_RETURN_FLOAT8(score);
}

#include "utils/array.h"
#include "utils/lsyscache.h"

/*
 * BM25F: multi-field BM25.  Per-field term frequencies are length-normalized
 * per field and combined with per-field weights *before* the tf-saturation
 * step, which is the Robertson/Zaragoza BM25F formulation (not a naive sum of
 * per-field BM25 scores):
 *
 *   tf~(t)      = sum_f w_f * f(t,D,f) / (1 - b + b * |D|_f / avgdl_f)
 *   score(D,Q)  = sum_t IDF(t) * tf~(t) * (k1+1) / (k1 + tf~(t))
 *
 * Inputs are parallel arrays over fields: one ftsdoc per field, one weight per
 * field, one avgdl per field.  IDF uses the summed df across fields (dfs is one
 * value per distinct query term, as with fts_bm25).
 */
PG_FUNCTION_INFO_V1(fts_bm25f);

Datum
fts_bm25f(PG_FUNCTION_ARGS)
{
	ArrayType  *docsarr;
	FtsQuery	q;
	ArrayType  *warr;
	double		N;
	ArrayType  *avgdlarr;
	ArrayType  *dfsarr = NULL;
	Datum	   *docd;
	bool	   *docnull;
	int			nfields;
	Datum	   *wd;
	bool	   *wnull;
	int			nw;
	Datum	   *avgd;
	bool	   *avgnull;
	int			navg;
	double	   *dfs = NULL;
	int			ndfs = 0;
	const char **terms;
	int		   *lens;
	int			nterms;
	double		k1 = BM25_DEFAULT_K1;
	double		b = BM25_DEFAULT_B;
	double		score = 0.0;
	int			f,
				t;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) ||
		PG_ARGISNULL(3) || PG_ARGISNULL(4))
		PG_RETURN_NULL();

	docsarr = PG_GETARG_ARRAYTYPE_P(0);
	q = PG_GETARG_FTSQUERY(1);
	warr = PG_GETARG_ARRAYTYPE_P(2);
	N = PG_GETARG_FLOAT8(3);
	avgdlarr = PG_GETARG_ARRAYTYPE_P(4);

	if (N < 1.0)
		N = 1.0;

	deconstruct_array(docsarr, ARR_ELEMTYPE(docsarr),
					  -1, false, 'i', &docd, &docnull, &nfields);
	if (ARR_ELEMTYPE(warr) != FLOAT8OID || ARR_ELEMTYPE(avgdlarr) != FLOAT8OID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("weights and avgdls must be float8[]")));
	deconstruct_array(warr, FLOAT8OID, 8, true, 'd', &wd, &wnull, &nw);
	deconstruct_array(avgdlarr, FLOAT8OID, 8, true, 'd', &avgd, &avgnull, &navg);

	if (nw != nfields || navg != nfields)
		ereport(ERROR,
				(errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
				 errmsg("docs, weights and avgdls must have the same length")));

	if (!PG_ARGISNULL(5))
	{
		Datum	   *de;
		bool	   *dn;
		int			i;

		dfsarr = PG_GETARG_ARRAYTYPE_P(5);
		if (ARR_ELEMTYPE(dfsarr) != FLOAT8OID)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("dfs array must be float8[]")));
		deconstruct_array(dfsarr, FLOAT8OID, 8, true, 'd', &de, &dn, &ndfs);
		dfs = (double *) palloc(ndfs * sizeof(double));
		for (i = 0; i < ndfs; i++)
			dfs[i] = dn[i] ? 1.0 : DatumGetFloat8(de[i]);
	}

	nterms = fts_query_terms(q, &terms, &lens);

	for (t = 0; t < nterms; t++)
	{
		double		tfw = 0.0;	/* weighted, length-normalized tf across fields */
		double		df = (dfs != NULL && t < ndfs) ? dfs[t] : 1.0;
		double		idf;

		for (f = 0; f < nfields; f++)
		{
			FtsDoc		doc;
			FtsTermEntry *e;
			double		avgdl = avgnull[f] ? 1.0 : DatumGetFloat8(avgd[f]);
			double		w = wnull[f] ? 1.0 : DatumGetFloat8(wd[f]);
			double		dl;

			if (docnull[f])
				continue;
			doc = (FtsDoc) PG_DETOAST_DATUM(docd[f]);
			dl = (double) doc->doclen;
			if (avgdl <= 0.0)
				avgdl = 1.0;
			e = fts_doc_lookup(doc, terms[t], lens[t]);
			if (e == NULL)
				continue;
			tfw += w * (double) e->tf / (1.0 - b + b * dl / avgdl);
		}

		if (tfw <= 0.0)
			continue;
		idf = bm25_idf(BM25_LUCENE, N, df);
		score += idf * tfw * (k1 + 1.0) / (k1 + tfw);
	}

	pfree(terms);
	pfree(lens);
	PG_FREE_IF_COPY(q, 1);
	PG_RETURN_FLOAT8(score);
}

/*
 * fts_distance(ftsdoc, ftsquery) -> float8: a BM25 *distance* (smaller = more
 * relevant) for use as an ORDER BY operator.  Distance = 1/(1+score) maps the
 * unbounded BM25 score into (0,1], monotonically decreasing, so ascending
 * distance is descending relevance.  Corpus stats are unknown to a bare
 * operator call, so df is treated as 1 (rare) and avgdl as the doc's own
 * length; the bm25 index's ordering scan computes exact scores internally, and
 * this function is only the fallback the planner requires the operator to have.
 */
PG_FUNCTION_INFO_V1(fts_distance);

Datum
fts_distance(PG_FUNCTION_ARGS)
{
	FtsDoc		doc;
	FtsQuery	q;
	double		score;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	doc = PG_GETARG_FTSDOC(0);
	q = PG_GETARG_FTSQUERY(1);

	/* N and avgdl unknown here; use N=1, avgdl=|D| so length term is neutral */
	score = fts_bm25_score(doc, q, 1.0, (double) doc->doclen, NULL,
						   BM25_DEFAULT_K1, BM25_DEFAULT_B, BM25_LUCENE);

	PG_FREE_IF_COPY(doc, 0);
	PG_FREE_IF_COPY(q, 1);
	PG_RETURN_FLOAT8(1.0 / (1.0 + score));
}

PG_FUNCTION_INFO_V1(fts_distance_commutator);

/* ftsquery <=> ftsdoc (commutator) */
Datum
fts_distance_commutator(PG_FUNCTION_ARGS)
{
	FtsQuery	q;
	FtsDoc		doc;
	double		score;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	q = PG_GETARG_FTSQUERY(0);
	doc = PG_GETARG_FTSDOC(1);

	score = fts_bm25_score(doc, q, 1.0, (double) doc->doclen, NULL,
						   BM25_DEFAULT_K1, BM25_DEFAULT_B, BM25_LUCENE);

	PG_FREE_IF_COPY(q, 0);
	PG_FREE_IF_COPY(doc, 1);
	PG_RETURN_FLOAT8(1.0 / (1.0 + score));
}
