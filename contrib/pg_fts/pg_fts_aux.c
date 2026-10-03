/*-------------------------------------------------------------------------
 *
 * pg_fts_aux.c
 *		Auxiliary functions for pg_fts: highlight, snippet, score explain.
 *
 * Stage 8 of pg_fts.  These operate on the original text plus a query and are
 * independent of the index access method.  highlight() wraps matched query
 * terms in the source text; snippet() returns the best-matching window.
 *
 * Term matching folds the source the same way the default analyzer does, so a
 * query term matches a source word when their folded forms are equal.  (A
 * configuration-aware variant that stems both sides can follow.)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_aux.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_fts.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

static inline bool
is_token_byte(unsigned char c)
{
	if (c >= 0x80)
		return true;
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9');
}

static inline char
fold_ascii(unsigned char c)
{
	if (c >= 'A' && c <= 'Z')
		return (char) (c - 'A' + 'a');
	return (char) c;
}

/* Does the query contain a positive occurrence of (folded) term? */
static bool
query_has_term(FtsQuery q, const char *folded, int len)
{
	uint32		i;

	for (i = 0; i < q->nitems; i++)
	{
		FtsQueryItem *it = &q->items[i];

		if (it->type == FTS_QI_VAL &&
			(int) it->termlen == len &&
			memcmp(FTS_QUERY_ITEMTEXT(q, it), folded, len) == 0)
			return true;
	}
	return false;
}

/*
 * Walk the source text, invoking a callback for each token with its byte
 * range and whether its folded form is a query term.  Between-token text is
 * emitted verbatim by the callers.
 */
typedef void (*TokenSink) (void *ctx, const char *src, int start, int end,
						   bool matched);

static void
tokenize_and_mark(const char *src, int len, FtsQuery q,
				  TokenSink sink, void *ctx)
{
	int			i = 0;

	while (i < len)
	{
		int			sepstart = i;
		int			tokstart;
		int			toklen;
		char	   *folded;
		int			j;
		bool		matched;

		while (i < len && !is_token_byte((unsigned char) src[i]))
			i++;
		/* emit separator run as a non-matching "token" span */
		if (i > sepstart)
			sink(ctx, src, sepstart, i, false);
		if (i >= len)
			break;

		tokstart = i;
		while (i < len && is_token_byte((unsigned char) src[i]))
			i++;
		toklen = i - tokstart;

		folded = (char *) palloc(toklen);
		for (j = 0; j < toklen; j++)
			folded[j] = fold_ascii((unsigned char) src[tokstart + j]);
		matched = query_has_term(q, folded, toklen);
		pfree(folded);

		sink(ctx, src, tokstart, i, matched);
	}
}

/* --- highlight --- */

typedef struct HlCtx
{
	StringInfoData buf;
	const char *pre;
	const char *post;
}			HlCtx;

static void
hl_sink(void *c, const char *src, int start, int end, bool matched)
{
	HlCtx	   *ctx = (HlCtx *) c;

	if (matched)
		appendStringInfoString(&ctx->buf, ctx->pre);
	appendBinaryStringInfo(&ctx->buf, src + start, end - start);
	if (matched)
		appendStringInfoString(&ctx->buf, ctx->post);
}

PG_FUNCTION_INFO_V1(fts_highlight);

/* fts_highlight(text, ftsquery, pre text, post text) -> text */
Datum
fts_highlight(PG_FUNCTION_ARGS)
{
	text	   *doc = PG_GETARG_TEXT_PP(0);
	FtsQuery	q = PG_GETARG_FTSQUERY(1);
	text	   *pre = PG_GETARG_TEXT_PP(2);
	text	   *post = PG_GETARG_TEXT_PP(3);
	HlCtx		ctx;
	text	   *result;

	initStringInfo(&ctx.buf);
	ctx.pre = text_to_cstring(pre);
	ctx.post = text_to_cstring(post);

	tokenize_and_mark(VARDATA_ANY(doc), VARSIZE_ANY_EXHDR(doc), q,
					  hl_sink, &ctx);

	result = cstring_to_text_with_len(ctx.buf.data, ctx.buf.len);

	PG_FREE_IF_COPY(doc, 0);
	PG_FREE_IF_COPY(q, 1);
	PG_RETURN_TEXT_P(result);
}

/* --- snippet --- */

typedef struct Tok
{
	int			start;
	int			end;
	bool		matched;
}			Tok;

typedef struct SnipCtx
{
	Tok		   *toks;
	int			n;
	int			max;
}			SnipCtx;

static void
snip_sink(void *c, const char *src, int start, int end, bool matched)
{
	SnipCtx    *ctx = (SnipCtx *) c;

	/* record only real tokens (skip pure separators) for windowing */
	if (!matched && !is_token_byte((unsigned char) src[start]))
		return;
	if (ctx->n >= ctx->max)
	{
		ctx->max = ctx->max ? ctx->max * 2 : 64;
		if (ctx->toks == NULL)
			ctx->toks = (Tok *) palloc(ctx->max * sizeof(Tok));
		else
			ctx->toks = (Tok *) repalloc(ctx->toks, ctx->max * sizeof(Tok));
	}
	ctx->toks[ctx->n].start = start;
	ctx->toks[ctx->n].end = end;
	ctx->toks[ctx->n].matched = matched;
	ctx->n++;
}

PG_FUNCTION_INFO_V1(fts_snippet);

/*
 * fts_snippet(text, ftsquery, pre, post, ellipsis, max_tokens) -> text
 * Returns the window of up to max_tokens tokens containing the most query
 * matches, with matched terms wrapped and an ellipsis where text is trimmed.
 */
Datum
fts_snippet(PG_FUNCTION_ARGS)
{
	text	   *doc = PG_GETARG_TEXT_PP(0);
	FtsQuery	q = PG_GETARG_FTSQUERY(1);
	char	   *pre = text_to_cstring(PG_GETARG_TEXT_PP(2));
	char	   *post = text_to_cstring(PG_GETARG_TEXT_PP(3));
	char	   *ell = text_to_cstring(PG_GETARG_TEXT_PP(4));
	int			maxtok = PG_GETARG_INT32(5);
	const char *src = VARDATA_ANY(doc);
	SnipCtx		ctx;
	int			best_start = 0;
	int			best_hits = -1;
	int			i;
	StringInfoData out;
	text	   *result;

	if (maxtok < 1)
		maxtok = 1;

	ctx.toks = NULL;
	ctx.n = 0;
	ctx.max = 0;
	tokenize_and_mark(src, VARSIZE_ANY_EXHDR(doc), q, snip_sink, &ctx);

	if (ctx.n == 0)
	{
		PG_RETURN_TEXT_P(cstring_to_text(""));
	}

	/* slide a maxtok-token window, choosing the one with the most matches */
	for (i = 0; i < ctx.n; i++)
	{
		int			hits = 0;
		int			j;

		for (j = i; j < ctx.n && j < i + maxtok; j++)
			if (ctx.toks[j].matched)
				hits++;
		if (hits > best_hits)
		{
			best_hits = hits;
			best_start = i;
		}
		if (i + maxtok >= ctx.n)
			break;
	}

	initStringInfo(&out);
	if (best_start > 0)
		appendStringInfoString(&out, ell);
	{
		int			end = Min(best_start + maxtok, ctx.n);

		for (i = best_start; i < end; i++)
		{
			if (i > best_start)
				appendStringInfoChar(&out, ' ');
			if (ctx.toks[i].matched)
				appendStringInfoString(&out, pre);
			appendBinaryStringInfo(&out, src + ctx.toks[i].start,
								   ctx.toks[i].end - ctx.toks[i].start);
			if (ctx.toks[i].matched)
				appendStringInfoString(&out, post);
		}
		if (end < ctx.n)
			appendStringInfoString(&out, ell);
	}

	result = cstring_to_text_with_len(out.data, out.len);
	PG_FREE_IF_COPY(doc, 0);
	PG_FREE_IF_COPY(q, 1);
	PG_RETURN_TEXT_P(result);
}
