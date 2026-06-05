/*-------------------------------------------------------------------------
 *
 * segparse_driver.c
 *	  Parser+lexer driver for seg's input syntax.
 *
 * Wires Lime's push parser (generated from segparse.lime) to Lime's
 * lexer (generated from segscan.lex).  Replaces the flex-generated
 * seg_yylex / yylex_init / yylex_destroy plus seg_scanner_init /
 * seg_scanner_finish that lived in segscan.l.
 *
 * Public interface declared in segdata.h is unchanged:
 *	int seg_yyparse(SEG *result, struct Node *escontext, yyscan_t yyscanner);
 *	int seg_yylex(union YYSTYPE *yylval_param, yyscan_t yyscanner);
 *	void seg_yyerror(SEG *result, struct Node *escontext, yyscan_t yyscanner,
 *	                 const char *message);
 *	void seg_scanner_init(const char *str, yyscan_t *yyscannerp);
 *	void seg_scanner_finish(yyscan_t yyscanner);
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * contrib/seg/segparse_driver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/miscnodes.h"
#include "utils/memutils.h"

#include "seg_gram_yytype.h"
#include "segdata.h"
#include "segparse.h"
#include "segscan_lex.h"		/* SegLexer, SegLexAlloc, SegLexFeedBytes,
								 * SegLexFeedEOF, SegLexFree, SEG_LEX_OK */

/*
 * Layout matches the converter's emitted struct GramParseExtra body,
 * plus the aborted flag the YYABORT/YYERROR shims set.
 */
struct GramParseExtra
{
	SEG		   *result;
	struct Node *escontext;
	yyscan_t	yyscanner;
	bool		aborted;
};

/* Lime push parser entry points (%name seg_yy in segparse.lime). */
extern void *seg_yyAlloc(void *(*mallocProc) (size_t));
extern void seg_yyFree(void *p, void (*freeProc) (void *));
extern void seg_yy(void *yyp, int yymajor, YYSTYPE yyminor,
				   struct GramParseExtra *extra);

/*
 * Public yyscan_t handle.  Tracks the input cursor and the most
 * recent token's text for error messages.
 */
typedef struct SegYyScanner
{
	const char *input;
	Size		input_len;
	StringInfoData yytext;
	void	   *parser;			/* seg_yyAlloc handle */
} SegYyScanner;

int
seg_yylex(union YYSTYPE *yylval_param, yyscan_t yyscanner)
{
	/*
	 * seg_yylex is unreferenced in tree (callers go through seg_yyparse).
	 * Stub kept for source-level compatibility with segdata.h's declaration.
	 */
	memset(yylval_param, 0, sizeof(*yylval_param));
	return 0;
}

void
seg_yyerror(SEG *result, struct Node *escontext, yyscan_t yyscanner,
			const char *message)
{
	SegYyScanner *s = (SegYyScanner *) yyscanner;

	(void) result;

	if (SOFT_ERROR_OCCURRED(escontext))
		return;

	if (s == NULL || s->yytext.len == 0)
	{
		errsave(escontext,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("bad seg representation"),
				 errdetail("%s at end of input", message)));
	}
	else
	{
		errsave(escontext,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("bad seg representation"),
				 errdetail("%s at or near \"%s\"", message, s->yytext.data)));
	}
}

void
seg_scanner_init(const char *str, yyscan_t *yyscannerp)
{
	SegYyScanner *s = palloc0_object(SegYyScanner);

	s->input = str;
	s->input_len = strlen(str);
	initStringInfo(&s->yytext);
	*yyscannerp = (yyscan_t) s;
}

void
seg_scanner_finish(yyscan_t yyscanner)
{
	SegYyScanner *s = (SegYyScanner *) yyscanner;

	if (s == NULL)
		return;
	if (s->yytext.data != NULL)
		pfree(s->yytext.data);
	pfree(s);
}

struct EmitContext
{
	SegYyScanner *s;
	struct GramParseExtra *extra;
};

static void
seg_emit_cb(void *user, int token, const char *text, size_t len)
{
	struct EmitContext *ctx = user;
	SegYyScanner *s = ctx->s;
	YYSTYPE		yylval;
	char	   *literal;

	memset(&yylval, 0, sizeof(yylval));

	resetStringInfo(&s->yytext);
	appendBinaryStringInfo(&s->yytext, text, len);

	literal = palloc(len + 1);
	memcpy(literal, text, len);
	literal[len] = '\0';
	yylval.text = literal;

	seg_yy(s->parser, token, yylval, ctx->extra);
}

int
seg_yyparse(SEG *result, struct Node *escontext, yyscan_t yyscanner)
{
	SegYyScanner *s = (SegYyScanner *) yyscanner;
	SegLexer   *lex;
	struct GramParseExtra extra;
	struct EmitContext ctx;
	YYSTYPE		zero_yylval;

	memset(&zero_yylval, 0, sizeof(zero_yylval));

	extra.result = result;
	extra.escontext = escontext;
	extra.yyscanner = yyscanner;
	extra.aborted = false;

	s->parser = seg_yyAlloc(palloc);

	lex = SegLexAlloc(palloc);
	if (lex == NULL)
	{
		seg_yyFree(s->parser, pfree);
		return 1;
	}

	ctx.s = s;
	ctx.extra = &extra;

	if (SegLexFeedBytes(lex, s->input, s->input_len,
						seg_emit_cb, &ctx) != SEG_LEX_OK)
	{
		SegLexFree(lex, pfree);
		seg_yyFree(s->parser, pfree);
		seg_yyerror(result, escontext, yyscanner, "syntax error");
		return 1;
	}
	(void) SegLexFeedEOF(lex, seg_emit_cb, &ctx);
	SegLexFree(lex, pfree);

	seg_yy(s->parser, 0, zero_yylval, &extra);

	seg_yyFree(s->parser, pfree);
	return 0;
}
