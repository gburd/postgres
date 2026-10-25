/*-------------------------------------------------------------------------
 *
 * pg_fts_analyze.c
 *		Stage-1 built-in tokenizer for pg_fts.
 *
 * Produces an ftsdoc from raw text.  This is the simple, self-contained default
 * analyzer -- to_ftsdoc(text): fold ASCII letters to lowercase, split on any
 * non-alphanumeric byte, and collect the distinct terms with their term
 * frequencies.
 *
 * The configuration-driven analyzer that reuses PostgreSQL's text-search parser
 * and dictionary pipeline (parsetext(), the snowball/ispell dictionaries) lives
 * in pg_fts_tsanalyze.c as to_ftsdoc(regconfig, text).  Tokenization is isolated
 * behind fts_analyze_text() so either analyzer can be used without touching the
 * type, the operator, or the on-disk format.
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
	uint32		pos;			/* 1-based token position in the source */
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
	if (ra->len != rb->len)
		return ra->len - rb->len;
	/* same term: order by position so positions come out ascending */
	if (ra->pos < rb->pos)
		return -1;
	if (ra->pos > rb->pos)
		return 1;
	return 0;
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
		raw[nraw].pos = doclen + 1;	/* 1-based token position */
		nraw++;
		doclen++;
	}

	/* Sort by (term, pos) so duplicates are adjacent and positions ascend. */
	if (nraw > 1)
		qsort(raw, nraw, sizeof(RawTerm), cmp_rawterm);

	/* Second pass: fold duplicates, recording tf and positions. */
	{
		int			ndistinct = 0;
		Size		lexbytes = 0;
		int			npos = nraw;	/* one position per token */
		FtsDoc		doc;
		Size		posbase;
		Size		total;
		FtsTermEntry *entries;
		char	   *lexemes;
		uint32	   *positions;
		uint32		off;
		uint32		pidx;
		int		   *runlen;
		int		   *runstart;

		/* identify distinct-term runs (term-only equality) */
		runlen = (int *) palloc(Max(nraw, 1) * sizeof(int));
		runstart = (int *) palloc(Max(nraw, 1) * sizeof(int));
		for (i = 0; i < nraw;)
		{
			int			run = 1;

			while (i + run < nraw)
			{
				int			min = Min(raw[i].len, raw[i + run].len);

				if (raw[i].len != raw[i + run].len ||
					memcmp(raw[i].term, raw[i + run].term, min) != 0)
					break;
				run++;
			}
			runstart[ndistinct] = i;
			runlen[ndistinct] = run;
			lexbytes += raw[i].len;
			ndistinct++;
			i += run;
		}

		posbase = MAXALIGN(FTS_DOC_HDRSIZE +
						   (Size) ndistinct * sizeof(FtsTermEntry) + lexbytes);
		total = posbase + (Size) npos * sizeof(uint32);
		doc = (FtsDoc) palloc0(total);
		SET_VARSIZE(doc, total);
		doc->version = FTS_DOC_VERSION;
		doc->flags = FTS_DOCF_POSITIONS;
		doc->nterms = ndistinct;
		doc->doclen = doclen;
		doc->lexbytes = lexbytes;

		entries = FTS_DOC_ENTRIES(doc);
		lexemes = FTS_DOC_LEXEMES(doc);
		positions = FTS_DOC_POSITIONS(doc);
		off = 0;
		pidx = 0;
		for (i = 0; i < ndistinct; i++)
		{
			int			s = runstart[i];
			int			k;

			entries[i].off = off;
			entries[i].len = raw[s].len;
			entries[i].tf = runlen[i];
			entries[i].posoff = pidx;
			memcpy(lexemes + off, raw[s].term, raw[s].len);
			off += raw[s].len;
			for (k = 0; k < runlen[i]; k++)
				positions[pidx++] = raw[s + k].pos;
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
