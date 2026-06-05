/*-------------------------------------------------------------------------
 *
 * cubeparse_driver.c
 *	  Parser+lexer driver for cube's input syntax.
 *
 * Wires Lime's push parser (generated from cubeparse.lime) to Lime's
 * lexer (generated from cubescan.lex).  Replaces the bison/flex
 * generated cube_yyparse / cube_yylex / cube_yylex_init pair.
 *
 * Public interface (declared in cubedata.h):
 *
 *	int cube_yyparse(NDBOX **result, Size scanbuflen,
 *	                 struct Node *escontext, yyscan_t yyscanner);
 *	void cube_yyerror(NDBOX **result, Size scanbuflen,
 *	                  struct Node *escontext, yyscan_t yyscanner,
 *	                  const char *message);
 *
 * The yyscanner handle holds the input buffer and tracks the last
 * matched text for error messages.  cube_yylex no longer exists -- the
 * parser is fed by the driver's emit callback.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * contrib/cube/cubeparse_driver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cubedata.h"
#include "cubeparse.h"
#include "cubescan_lex.h"		/* CubeLexer, CubeLexAlloc, CubeLexFeedBytes,
								 * CubeLexFree, CUBE_LEX_OK */
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/miscnodes.h"
#include "utils/memutils.h"

/*
 * Layout matches the converter's emitted struct GramParseExtra body,
 * plus the `aborted` flag the YYABORT macro shim sets in the
 * %include block of cubeparse.lime.  Defined BEFORE the extern
 * declarations below so the struct type is identical at every
 * consumer site (forward-declaring it via `extern void cube_yy(...,
 * struct GramParseExtra *)` would create an anonymous-struct mismatch
 * with the same-named struct defined later in this TU).
 */
struct GramParseExtra
{
	NDBOX	  **result;
	Size		scanbuflen;
	struct Node *escontext;
	yyscan_t	yyscanner;
	bool		aborted;
};

/* Lime push parser entry points (%name cube_yy in cubeparse.lime). */
extern void *cube_yyAlloc(void *(*mallocProc) (size_t));
extern void cube_yyFree(void *p, void (*freeProc) (void *));
extern void cube_yy(void *yyp, int yymajor, YYSTYPE yyminor,
					struct GramParseExtra *extra);

/* ------------------------------------------------------------------------- */
/* Scanner state                                                             */
/* ------------------------------------------------------------------------- */

/*
 * Public yyscan_t handle.  yy_scan_bytes installs the input via a
 * pointer + length; the parser-driver loop then drives the Lime lexer
 * over that span.  yytext tracks the most recently emitted lexeme for
 * error messages.
 */
typedef struct CubeYyScanner
{
	const char *input;
	Size		input_len;
	StringInfoData yytext;
	void	   *parser;			/* cube_yyAlloc handle (parser side) */
} CubeYyScanner;

static int
cube_yylex_init(yyscan_t *yyscannerp)
{
	CubeYyScanner *s = palloc0_object(CubeYyScanner);

	initStringInfo(&s->yytext);
	*yyscannerp = (yyscan_t) s;
	return 0;
}

static int
cube_yylex_destroy(yyscan_t yyscanner)
{
	CubeYyScanner *s = (CubeYyScanner *) yyscanner;

	if (s->yytext.data)
		pfree(s->yytext.data);
	pfree(s);
	return 0;
}

/*
 * Install the input text and length into the scanner.  Replaces flex's
 * yy_scan_bytes / cube_scanner_init pair.  The caller (cube.c's
 * cube_in / cube_out / etc.) keeps the input string alive for the
 * lifetime of the parse, so we just retain the pointer.
 */
static void
cube_yy_scan_bytes(yyscan_t yyscanner, const char *str, Size len)
{
	CubeYyScanner *s = (CubeYyScanner *) yyscanner;

	s->input = str;
	s->input_len = len;
}

/*
 * Public entry point used by cube.c.  Allocates the scanner, installs
 * the input, and returns the length so callers (e.g. cube_yyerror)
 * can reuse it.  Mirrors the flex-era cube_scanner_init signature.
 */
void
cube_scanner_init(const char *str, Size *scanbuflen, yyscan_t *yyscannerp)
{
	Size		slen = strlen(str);

	cube_yylex_init(yyscannerp);
	cube_yy_scan_bytes(*yyscannerp, str, slen);
	*scanbuflen = slen;
}

void
cube_scanner_finish(yyscan_t yyscanner)
{
	cube_yylex_destroy(yyscanner);
}

/* ------------------------------------------------------------------------- */
/* Error reporting                                                           */
/* ------------------------------------------------------------------------- */

/*
 * Same signature as the bison-driven version.  cubescan.l's flex copy
 * of cube_yyerror was the public one; this re-exports it identically.
 */
void
cube_yyerror(NDBOX **result, Size scanbuflen,
			 struct Node *escontext, yyscan_t yyscanner,
			 const char *message)
{
	CubeYyScanner *s = (CubeYyScanner *) yyscanner;

	if (s->yytext.len == 0)
	{
		errsave(escontext,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for cube"),
				 errdetail("%s at end of input", message)));
	}
	else
	{
		errsave(escontext,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for cube"),
				 errdetail("%s at or near \"%s\"", message,
						   s->yytext.data)));
	}
}

/* ------------------------------------------------------------------------- */
/* Lexer -> parser bridge                                                    */
/* ------------------------------------------------------------------------- */

struct EmitContext
{
	CubeYyScanner *s;
	struct GramParseExtra *extra;
};

/*
 * Called by CubeLexFeedBytes for each matched rule.  token is the
 * value LEX_EMIT'd from cubescan.lex (CUBEFLOAT, O_BRACKET, etc.).
 * text/len point into the input buffer (no NUL terminator).
 *
 * For cube's grammar, every value-bearing token's payload is just a
 * pstrdup of the matched text -- the bison-era cubescan.l set
 * `*yylval = yytext` for floats and used static string literals
 * ("(", ")", ",") for the punctuation tokens.  We mirror that with
 * pstrdups; the per-line memory context (cube.c's parsing context)
 * cleans up on completion.
 */
static void
cube_emit_cb(void *user, int token, const char *text, size_t len)
{
	struct EmitContext *ctx = user;
	CubeYyScanner *s = ctx->s;
	YYSTYPE		yylval;
	char	   *literal;

	if (ctx->extra->aborted)
		return;

	/* Track the last lexeme for error messages. */
	resetStringInfo(&s->yytext);
	appendBinaryStringInfo(&s->yytext, text, len);

	literal = palloc(len + 1);
	memcpy(literal, text, len);
	literal[len] = '\0';
	yylval = literal;

	cube_yy(s->parser, token, yylval, ctx->extra);
}

/* ------------------------------------------------------------------------- */
/* Parser entry point                                                        */
/* ------------------------------------------------------------------------- */

int
cube_yyparse(NDBOX **result, Size scanbuflen,
			 struct Node *escontext, yyscan_t yyscanner)
{
	CubeYyScanner *s = (CubeYyScanner *) yyscanner;
	CubeLexer  *lex;
	struct GramParseExtra extra;
	struct EmitContext ctx;
	YYSTYPE		zero_yylval = NULL;

	extra.result = result;
	extra.scanbuflen = scanbuflen;
	extra.escontext = escontext;
	extra.yyscanner = yyscanner;
	extra.aborted = false;

	s->parser = cube_yyAlloc(palloc);

	lex = CubeLexAlloc(palloc);
	if (lex == NULL)
	{
		cube_yyFree(s->parser, pfree);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg_internal("CubeLexAlloc returned NULL")));
	}

	ctx.s = s;
	ctx.extra = &extra;

	if (CubeLexFeedBytes(lex, s->input, s->input_len,
						 cube_emit_cb, &ctx) != CUBE_LEX_OK)
	{
		CubeLexFree(lex, pfree);
		cube_yyFree(s->parser, pfree);
		cube_yyerror(result, scanbuflen, escontext, yyscanner,
					 "syntax error");
		return 1;
	}
	CubeLexFeedEOF(lex, cube_emit_cb, &ctx);
	CubeLexFree(lex, pfree);

	cube_yy(s->parser, 0, zero_yylval, &extra);

	cube_yyFree(s->parser, pfree);
	return 0;
}
