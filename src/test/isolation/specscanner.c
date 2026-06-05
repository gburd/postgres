/*-------------------------------------------------------------------------
 *
 * specscanner.c
 *	  Parser+lexer driver for the isolation test spec file format.
 *
 * Lime v0.2.2's lexer subsystem (compiled from specscanner.lex) does
 * the tokenizing; this file is the parser-driver shim that wires
 * SpecLexFeedBytes to the Lime parser generated from specparse.lime
 * via spec_yy().  The hand-rolled state machine that used to live
 * here (~370 lines of scanner internals) is gone.
 *
 * Public interface declared in isolationtester.h is unchanged:
 * spec_yyparse / spec_yylex / spec_yyerror keep their signatures and
 * behaviour.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/isolation/specscanner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "isolationtester.h"
#include "spec_gram_yytype.h"
#include "specparse.h"
#include "specscanner_lex.h"	/* SpecLexer, SpecLexAlloc, SpecLexFeedBytes,
								 * SpecLexFeedEOF, SpecLexFree, SPEC_LEX_OK */

/*
 * Lime-generated push parser entry points (from specparse.c,
 * %name spec_yy).
 */
extern void *spec_yyAlloc(void *(*mallocProc) (size_t));
extern void spec_yyFree(void *p, void (*freeProc) (void *));
extern void spec_yy(void *yyp, int yymajor, SpecYYSTYPE yyminor);

/*
 * Result of parsing.  Declared extern in isolationtester.h and populated
 * by the testspec production's semantic action in specparse.lime.
 */
TestSpec	parseresult;

/* ----------------------------------------------------------------
 * Scanner / driver state.  The line counter is a file-scope int
 * declared extern from specscanner.lex's %include block; the driver
 * resets it at the start of each parse and the .lex's newline rules
 * increment it.  (When Lime upstream P0-NEW-12 lands and threads the
 * %lexer_extra_argument binding through to action bodies, this can
 * collapse into the per-instance extra struct.)
 * ----------------------------------------------------------------
 */
int			spec_yyline;
static SpecYYSTYPE spec_yylval; /* For the public spec_yylex shim. */

/* ----------------------------------------------------------------
 * Allocator shims for Lime's XxxAlloc / XxxFree contracts.
 * ----------------------------------------------------------------
 */
static void *
spec_palloc(size_t n)
{
	return pg_malloc(n);
}

static void
spec_pfree(void *p)
{
	if (p != NULL)
		free(p);
}

/* ----------------------------------------------------------------
 * Error reporting.  Identical wire format to the retired flex scanner:
 * "<message> at line N\n" on stderr, exit(1).
 * ----------------------------------------------------------------
 */
void
spec_yyerror(const char *message)
{
	fprintf(stderr, "%s at line %d\n", message, spec_yyline);
	exit(1);
}

/* ----------------------------------------------------------------
 * Input slurping.  Reads stdin to EOF into a heap buffer; identical
 * shape to the retired hand-rolled scanner's slurp_stdin helper.
 * ----------------------------------------------------------------
 */
static char *
slurp_stdin(size_t *len_out)
{
	size_t		cap = 4096;
	size_t		len = 0;
	char	   *buf = pg_malloc(cap);

	for (;;)
	{
		size_t		n;

		if (len + 1 >= cap)
		{
			cap *= 2;
			buf = pg_realloc(buf, cap);
		}
		n = fread(buf + len, 1, cap - 1 - len, stdin);
		if (n == 0)
		{
			if (ferror(stdin))
			{
				fprintf(stderr, "could not read spec from stdin\n");
				exit(1);
			}
			break;
		}
		len += n;
	}
	buf[len] = '\0';
	*len_out = len;
	return buf;
}

/* ----------------------------------------------------------------
 * Lexer -> parser bridge
 * ----------------------------------------------------------------
 */

struct SpecEmitContext
{
	void	   *parser;
};

/*
 * Called by SpecLexFeedBytes for each emitted token.  text/len point
 * into the input buffer (or, for IDENTIFIER and SQLBLOCK from the
 * QIDENT/SQLBLK exclusive states, at the LEX_BUF_TAKE'd heap copy
 * which the action body has already free'd by the time we get here --
 * we receive only its length and bytes, so we must copy out
 * immediately).
 */
static void
spec_emit_cb(void *user, int token, const char *text, size_t len)
{
	struct SpecEmitContext *ctx = user;
	SpecYYSTYPE lval;

	memset(&lval, 0, sizeof(lval));

	switch (token)
	{
		case IDENTIFIER:
		case SQLBLOCK:
			{
				char	   *dup = pg_malloc(len + 1);

				memcpy(dup, text, len);
				dup[len] = '\0';
				lval.str = dup;
				break;
			}
		case INTEGER:
			{
				char		numbuf[32];
				size_t		n = (len < sizeof(numbuf)) ? len : sizeof(numbuf) - 1;

				memcpy(numbuf, text, n);
				numbuf[n] = '\0';
				lval.integer = atoi(numbuf);
				break;
			}
		default:
			/* Keywords + punctuation: no payload. */
			break;
	}

	spec_yy(ctx->parser, token, lval);
}

/* ----------------------------------------------------------------
 * Public spec_yylex shim.  Not called by any in-tree code, but
 * isolationtester.h declares it; we keep the symbol for source-level
 * compatibility.  Returns a single token at a time by running the
 * lexer over a held input slice; the file-scope spec_yylval mirrors
 * the retired flex scanner's behaviour.
 * ----------------------------------------------------------------
 */
int
spec_yylex(void)
{
	/*
	 * spec_yylex is unreferenced in tree (callers go through spec_yyparse).
	 * Keeping a non-functional stub avoids breaking the public surface; if a
	 * future caller appears we can wire it up against the lexer state at that
	 * point.
	 */
	memset(&spec_yylval, 0, sizeof(spec_yylval));
	return 0;
}

/* ----------------------------------------------------------------
 * Driver.  Replaces Bison's generated yyparse.  Reads stdin to EOF,
 * runs the Lime lexer over the whole buffer, and feeds emitted
 * tokens to the Lime parser via spec_yy.  Returns 0 on success;
 * spec_yyerror exits(1) on any error so a non-zero return is never
 * observed by the caller.
 * ----------------------------------------------------------------
 */
int
spec_yyparse(void)
{
	char	   *input;
	size_t		input_len;
	SpecLexer  *lex;
	struct SpecEmitContext ctx;
	int			lex_status;
	SpecYYSTYPE eof_lval;

	spec_yyline = 1;

	input = slurp_stdin(&input_len);

	lex = SpecLexAlloc(spec_palloc);
	if (lex == NULL)
	{
		fprintf(stderr, "could not allocate spec lexer\n");
		exit(1);
	}

	ctx.parser = spec_yyAlloc(spec_palloc);

	lex_status = SpecLexFeedBytes(lex, input, input_len,
								  spec_emit_cb, &ctx);
	if (lex_status != SPEC_LEX_OK)
	{
		const char *m = SpecLexErrorMessage(lex);

		spec_yyerror(m ? m : "syntax error");
		/* spec_yyerror exits, but for analyzers: */
		SpecLexFree(lex, spec_pfree);
		spec_yyFree(ctx.parser, spec_pfree);
		free(input);
		return 1;
	}
	(void) SpecLexFeedEOF(lex, spec_emit_cb, &ctx);

	memset(&eof_lval, 0, sizeof(eof_lval));
	spec_yy(ctx.parser, 0, eof_lval);

	spec_yyFree(ctx.parser, spec_pfree);
	SpecLexFree(lex, spec_pfree);
	free(input);
	return 0;
}
