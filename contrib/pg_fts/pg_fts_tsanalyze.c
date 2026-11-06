/*-------------------------------------------------------------------------
 *
 * pg_fts_tsanalyze.c
 *		Analyzer that reuses PostgreSQL's existing text-search pipeline.
 *
 * Stage 2 of pg_fts.  Where pg_fts_analyze.c provides a minimal self-contained
 * tokenizer, this file makes the analyzer *pluggable* by binding an ftsdoc to
 * any installed text search configuration (pg_ts_config): the configured
 * parser and dictionary chain (snowball stemmers, ispell, synonyms, thesaurus,
 * stopwords) are run via parsetext(), and the resulting normalized lexemes are
 * folded into an ftsdoc.  No tokenizer or dictionary code is reimplemented --
 * this is the reuse the design calls for.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_tsanalyze.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_fts.h"
#include "catalog/pg_type.h"
#include "tsearch/ts_cache.h"
#include "tsearch/ts_utils.h"
#include "utils/builtins.h"

/* qsort_arg comparator: elements are indices into a ParsedWord array (arg) */
static int
cmp_word_idx(const void *a, const void *b, void *arg)
{
	ParsedWord *words = (ParsedWord *) arg;
	ParsedWord *wa = &words[*(const int *) a];
	ParsedWord *wb = &words[*(const int *) b];
	int			min = Min(wa->len, wb->len);
	int			c = memcmp(wa->word, wb->word, min);

	if (c != 0)
		return c;
	return wa->len - wb->len;
}

/*
 * Build an ftsdoc from the words produced by parsetext().  The words are not
 * sorted and may contain duplicates and multiple variants per position, so we
 * sort, deduplicate and count term frequency, exactly like the simple
 * analyzer.  doclen is the number of token positions, which parsetext tracks
 * in prs->pos.
 */
static FtsDoc
ftsdoc_from_parsed(ParsedText *prs)
{
	int			nw = prs->curwords;
	ParsedWord *words = prs->words;
	int		   *order;
	int			i;
	int			ndistinct = 0;
	Size		lexbytes = 0;
	FtsDoc		doc;
	Size		total;
	FtsTermEntry *entries;
	char	   *lexemes;
	uint32		off;

	if (nw == 0)
	{
		total = FTS_DOC_HDRSIZE;
		doc = (FtsDoc) palloc0(total);
		SET_VARSIZE(doc, total);
		doc->version = FTS_DOC_VERSION;
		doc->flags = 0;
		doc->nterms = 0;
		doc->doclen = 0;
		doc->lexbytes = 0;
		return doc;
	}

	/* index-sort words by (text, len) without disturbing the array */
	order = (int *) palloc(nw * sizeof(int));
	for (i = 0; i < nw; i++)
		order[i] = i;
	qsort_arg(order, nw, sizeof(int), cmp_word_idx, words);

	for (i = 0; i < nw;)
	{
		int			run = 1;
		ParsedWord *w = &words[order[i]];

		while (i + run < nw)
		{
			ParsedWord *n = &words[order[i + run]];
			int			min = Min(w->len, n->len);
			int			c = memcmp(w->word, n->word, min);

			if (c == 0)
				c = w->len - n->len;
			if (c != 0)
				break;
			run++;
		}
		lexbytes += w->len;
		ndistinct++;
		i += run;
	}

	total = FTS_DOC_HDRSIZE +
		(Size) ndistinct * sizeof(FtsTermEntry) + lexbytes;
	doc = (FtsDoc) palloc0(total);
	SET_VARSIZE(doc, total);
	doc->version = FTS_DOC_VERSION;
	doc->flags = 0;
	doc->nterms = ndistinct;
	doc->doclen = (uint32) prs->pos;
	doc->lexbytes = lexbytes;

	entries = FTS_DOC_ENTRIES(doc);
	lexemes = FTS_DOC_LEXEMES(doc);
	off = 0;
	ndistinct = 0;
	for (i = 0; i < nw;)
	{
		int			run = 1;
		ParsedWord *w = &words[order[i]];

		while (i + run < nw)
		{
			ParsedWord *n = &words[order[i + run]];
			int			min = Min(w->len, n->len);
			int			c = memcmp(w->word, n->word, min);

			if (c == 0)
				c = w->len - n->len;
			if (c != 0)
				break;
			run++;
		}
		entries[ndistinct].off = off;
		entries[ndistinct].len = w->len;
		entries[ndistinct].tf = run;
		memcpy(lexemes + off, w->word, w->len);
		off += w->len;
		ndistinct++;
		i += run;
	}

	return doc;
}

/*
 * fts_analyze_with_config -- analyze text using a specific TS configuration.
 */
FtsDoc
fts_analyze_with_config(Oid cfgId, const char *str, int len)
{
	ParsedText	prs;
	FtsDoc		doc;
	char	   *buf;

	prs.lenwords = Max(len / 6, 16);
	prs.curwords = 0;
	prs.pos = 0;
	prs.words = (ParsedWord *) palloc(sizeof(ParsedWord) * prs.lenwords);

	/* parsetext wants a writable buffer */
	buf = (char *) palloc(len + 1);
	memcpy(buf, str, len);
	buf[len] = '\0';

	parsetext(cfgId, &prs, buf, len);

	doc = ftsdoc_from_parsed(&prs);

	if (prs.words)
		pfree(prs.words);
	pfree(buf);

	return doc;
}

PG_FUNCTION_INFO_V1(to_ftsdoc_byid);

/* to_ftsdoc(regconfig, text) */
Datum
to_ftsdoc_byid(PG_FUNCTION_ARGS)
{
	Oid			cfgId = PG_GETARG_OID(0);
	text	   *in = PG_GETARG_TEXT_PP(1);
	FtsDoc		doc;

	doc = fts_analyze_with_config(cfgId,
								  VARDATA_ANY(in), VARSIZE_ANY_EXHDR(in));
	PG_FREE_IF_COPY(in, 1);
	PG_RETURN_FTSDOC(doc);
}

/*
 * fts_normalize_term -- run a single query term through a text-search config's
 * parser+dictionary pipeline and return its normalized lexeme (palloc'd), so a
 * query term matches the same stemmed/stopword-processed form the document
 * index stores.  Returns NULL and sets *outlen=0 if the term normalizes away
 * (e.g. it is a stopword), in which case the caller should drop it.  If the
 * term produces multiple lexemes only the first is used (query terms are single
 * words in stage-1 syntax).
 */
char *
fts_normalize_term(Oid cfgId, const char *term, int len, int *outlen)
{
	ParsedText	prs;
	char	   *buf;
	char	   *result = NULL;

	*outlen = 0;
	prs.lenwords = 4;
	prs.curwords = 0;
	prs.pos = 0;
	prs.words = (ParsedWord *) palloc(sizeof(ParsedWord) * prs.lenwords);

	buf = (char *) palloc(len + 1);
	memcpy(buf, term, len);
	buf[len] = '\0';
	parsetext(cfgId, &prs, buf, len);

	if (prs.curwords > 0)
	{
		result = (char *) palloc(prs.words[0].len);
		memcpy(result, prs.words[0].word, prs.words[0].len);
		*outlen = prs.words[0].len;
	}
	pfree(buf);
	if (prs.words)
		pfree(prs.words);
	return result;
}
