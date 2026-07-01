/*-------------------------------------------------------------------------
 *
 * pg_fts_doc.c
 *		Input/output and support functions for the ftsdoc type.
 *
 * ftsdoc input accepts the same text as to_ftsdoc(): the input string is
 * analyzed by the stage-1 tokenizer.  Output renders the distinct terms with
 * their term frequencies in a stable, human-readable form:
 *
 *	  'brown':1 'fox':2 'quick':1
 *
 * This mirrors tsvector's rendering closely enough to be familiar while making
 * the (BM25-relevant) term frequency visible, which tsvector's output hides.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_doc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_fts.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(ftsdoc_in);
PG_FUNCTION_INFO_V1(ftsdoc_out);
PG_FUNCTION_INFO_V1(ftsdoc_recv);
PG_FUNCTION_INFO_V1(ftsdoc_send);
PG_FUNCTION_INFO_V1(ftsdoc_length);

Datum
ftsdoc_in(PG_FUNCTION_ARGS)
{
	char	   *in = PG_GETARG_CSTRING(0);

	PG_RETURN_FTSDOC(fts_analyze_text(in, strlen(in)));
}

static void
append_quoted_term(StringInfo buf, const char *term, int len)
{
	int			i;

	appendStringInfoChar(buf, '\'');
	for (i = 0; i < len; i++)
	{
		char		c = term[i];

		if (c == '\'' || c == '\\')
			appendStringInfoChar(buf, '\\');
		appendStringInfoChar(buf, c);
	}
	appendStringInfoChar(buf, '\'');
}

Datum
ftsdoc_out(PG_FUNCTION_ARGS)
{
	FtsDoc		doc = PG_GETARG_FTSDOC(0);
	FtsTermEntry *entries = FTS_DOC_ENTRIES(doc);
	StringInfoData buf;
	uint32		i;

	initStringInfo(&buf);
	for (i = 0; i < doc->nterms; i++)
	{
		if (i > 0)
			appendStringInfoChar(&buf, ' ');
		append_quoted_term(&buf, FTS_DOC_TERMTEXT(doc, &entries[i]),
						   entries[i].len);
		appendStringInfo(&buf, ":%u", entries[i].tf);
	}

	PG_FREE_IF_COPY(doc, 0);
	PG_RETURN_CSTRING(buf.data);
}

/*
 * Binary receive/send.  The wire format is version-tagged and
 * architecture-neutral (fixed-width big-endian integers via pq_*), so it is
 * safe across replication and pg_dump -Fc.
 */
Datum
ftsdoc_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	uint16		version;
	uint32		nterms;
	uint32		doclen;
	FtsDoc		doc;
	FtsTermEntry *entries;
	char	   *lexemes;
	Size		lexbytes = 0;
	uint32		off = 0;
	uint32		i;
	char	  **terms;
	int		   *lens;
	uint32	   *tfs;

	version = (uint16) pq_getmsgint(buf, 2);
	if (version != FTS_DOC_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("unsupported ftsdoc version number %u", version)));

	nterms = (uint32) pq_getmsgint(buf, 4);
	doclen = (uint32) pq_getmsgint(buf, 4);

	terms = (char **) palloc(nterms * sizeof(char *));
	lens = (int *) palloc(nterms * sizeof(int));
	tfs = (uint32 *) palloc(nterms * sizeof(uint32));

	for (i = 0; i < nterms; i++)
	{
		const char *t;

		lens[i] = pq_getmsgint(buf, 4);
		tfs[i] = (uint32) pq_getmsgint(buf, 4);
		if (lens[i] < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					 errmsg("invalid ftsdoc term length")));
		t = pq_getmsgbytes(buf, lens[i]);
		terms[i] = (char *) palloc(lens[i]);
		memcpy(terms[i], t, lens[i]);
		lexbytes += lens[i];

		/* enforce the invariant that terms are strictly ascending */
		if (i > 0)
		{
			int			min = Min(lens[i - 1], lens[i]);
			int			c = memcmp(terms[i - 1], terms[i], min);

			if (c > 0 || (c == 0 && lens[i - 1] >= lens[i]))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
						 errmsg("ftsdoc terms must be sorted and distinct")));
		}
	}

	{
		Size		total = FTS_DOC_HDRSIZE +
			(Size) nterms * sizeof(FtsTermEntry) + lexbytes;

		doc = (FtsDoc) palloc0(total);
		SET_VARSIZE(doc, total);
		doc->version = FTS_DOC_VERSION;
		doc->flags = 0;
		doc->nterms = nterms;
		doc->doclen = doclen;

		entries = FTS_DOC_ENTRIES(doc);
		lexemes = FTS_DOC_LEXEMES(doc);
		for (i = 0; i < nterms; i++)
		{
			entries[i].off = off;
			entries[i].len = lens[i];
			entries[i].tf = tfs[i];
			memcpy(lexemes + off, terms[i], lens[i]);
			off += lens[i];
		}
	}

	PG_RETURN_FTSDOC(doc);
}

Datum
ftsdoc_send(PG_FUNCTION_ARGS)
{
	FtsDoc		doc = PG_GETARG_FTSDOC(0);
	FtsTermEntry *entries = FTS_DOC_ENTRIES(doc);
	StringInfoData buf;
	uint32		i;

	pq_begintypsend(&buf);
	pq_sendint16(&buf, doc->version);
	pq_sendint32(&buf, doc->nterms);
	pq_sendint32(&buf, doc->doclen);
	for (i = 0; i < doc->nterms; i++)
	{
		pq_sendint32(&buf, entries[i].len);
		pq_sendint32(&buf, entries[i].tf);
		pq_sendbytes(&buf, FTS_DOC_TERMTEXT(doc, &entries[i]), entries[i].len);
	}

	PG_FREE_IF_COPY(doc, 0);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/* ftsdoc_length(ftsdoc) -> int : total token count (doclen). Useful for BM25
 * length normalization later and handy for testing now. */
Datum
ftsdoc_length(PG_FUNCTION_ARGS)
{
	FtsDoc		doc = PG_GETARG_FTSDOC(0);
	uint32		doclen = doc->doclen;

	PG_FREE_IF_COPY(doc, 0);
	PG_RETURN_INT32((int32) doclen);
}

/*
 * fts_doc_lookup -- binary search for a term in a doc.
 * Returns the matching entry, or NULL.  Shared by the match evaluator.
 */
FtsTermEntry *
fts_doc_lookup(FtsDoc doc, const char *term, int termlen)
{
	FtsTermEntry *entries = FTS_DOC_ENTRIES(doc);
	int			lo = 0;
	int			hi = (int) doc->nterms - 1;

	while (lo <= hi)
	{
		int			mid = (lo + hi) / 2;
		const char *mterm = FTS_DOC_TERMTEXT(doc, &entries[mid]);
		int			mlen = entries[mid].len;
		int			min = Min(mlen, termlen);
		int			c = memcmp(mterm, term, min);

		if (c == 0)
			c = mlen - termlen;

		if (c == 0)
			return &entries[mid];
		else if (c < 0)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return NULL;
}
