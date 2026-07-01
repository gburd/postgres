/*-------------------------------------------------------------------------
 *
 * pg_fts_analyze.c
 *		Stage-1 built-in tokenizer for pg_fts.
 *
 * Produces an ftsdoc from raw text.  The stage-1 analyzer is deliberately
 * simple and self-contained: fold ASCII letters to lowercase, split on any
 * non-alphanumeric byte, and collect the distinct terms with their term
 * frequencies.  It is enough to make ftsdoc real and testable end to end.
 *
 * The pluggable analyzer framework -- reusing PostgreSQL's existing text-search
 * parser and dictionary pipeline (ts_parse.c, the snowball/ispell dictionaries)
 * -- is a later stage.  Isolating tokenization behind fts_analyze_text() now
 * means that later stage swaps the implementation without touching the type,
 * the operator, or the on-disk format.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_analyze.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_fts.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

/* One collected token before we fold duplicates into the ftsdoc. */
typedef struct RawTerm
{
	char	   *term;
	int			len;
	uint32		tf;
} RawTerm;

/*
 * Case-fold a single ASCII byte.  Non-ASCII bytes are passed through
 * unchanged, so UTF-8 multibyte sequences survive intact (proper Unicode
 * case-folding arrives with the tsearch-backed analyzer stage).
 */
static inline char
fold_ascii(unsigned char c)
{
	if (c >= 'A' && c <= 'Z')
		return (char) (c - 'A' + 'a');
	return (char) c;
}

static inline bool
is_token_byte(unsigned char c)
{
	/* ASCII alphanumerics start a/continue a token; so do all non-ASCII bytes
	 * (so UTF-8 words are kept whole rather than split at every byte). */
	if (c >= 0x80)
		return true;
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9');
}

static int
cmp_rawterm(const void *a, const void *b)
{
	const RawTerm *ra = (const RawTerm *) a;
	const RawTerm *rb = (const RawTerm *) b;
	int			min = Min(ra->len, rb->len);
	int			c = memcmp(ra->term, rb->term, min);

	if (c != 0)
		return c;
	return ra->len - rb->len;
}

/*
 * fts_analyze_text -- tokenize raw text into an ftsdoc.
 *
 * Returns a palloc'd, fully formed FtsDoc varlena.  An empty input yields a
 * valid zero-term document (which matches nothing).
 */
FtsDoc
fts_analyze_text(const char *str, int len)
{
	RawTerm    *raw;
	int			nraw = 0;
	int			maxraw;
	int			i;
	uint32		doclen = 0;

	/* Upper bound on tokens: every other byte could start a token. */
	maxraw = (len / 2) + 1;
	raw = (RawTerm *) palloc(maxraw * sizeof(RawTerm));

	/* First pass: carve out folded tokens. */
	i = 0;
	while (i < len)
	{
		int			start;
		char	   *folded;
		int			flen;
		int			j;

		/* skip separators */
		while (i < len && !is_token_byte((unsigned char) str[i]))
			i++;
		if (i >= len)
			break;

		start = i;
		while (i < len && is_token_byte((unsigned char) str[i]))
			i++;
		flen = i - start;

		folded = (char *) palloc(flen);
		for (j = 0; j < flen; j++)
			folded[j] = fold_ascii((unsigned char) str[start + j]);

		Assert(nraw < maxraw);
		raw[nraw].term = folded;
		raw[nraw].len = flen;
		raw[nraw].tf = 1;
		nraw++;
		doclen++;
	}

	/* Sort so duplicates are adjacent. */
	if (nraw > 1)
		qsort(raw, nraw, sizeof(RawTerm), cmp_rawterm);

	/* Second pass: fold duplicates, counting term frequency. */
	{
		int			ndistinct = 0;
		Size		lexbytes = 0;
		FtsDoc		doc;
		Size		total;
		FtsTermEntry *entries;
		char	   *lexemes;
		uint32		off;

		for (i = 0; i < nraw;)
		{
			int			run = 1;

			while (i + run < nraw &&
				   cmp_rawterm(&raw[i], &raw[i + run]) == 0)
				run++;
			raw[i].tf = run;
			lexbytes += raw[i].len;
			/* compact the run down to its first element */
			if (ndistinct != i)
				raw[ndistinct] = raw[i];
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
		doc->doclen = doclen;

		entries = FTS_DOC_ENTRIES(doc);
		lexemes = FTS_DOC_LEXEMES(doc);
		off = 0;
		for (i = 0; i < ndistinct; i++)
		{
			entries[i].off = off;
			entries[i].len = raw[i].len;
			entries[i].tf = raw[i].tf;
			memcpy(lexemes + off, raw[i].term, raw[i].len);
			off += raw[i].len;
		}

		return doc;
	}
}

PG_FUNCTION_INFO_V1(to_ftsdoc);

Datum
to_ftsdoc(PG_FUNCTION_ARGS)
{
	text	   *in = PG_GETARG_TEXT_PP(0);
	FtsDoc		doc;

	doc = fts_analyze_text(VARDATA_ANY(in), VARSIZE_ANY_EXHDR(in));

	PG_FREE_IF_COPY(in, 0);
	PG_RETURN_FTSDOC(doc);
}
