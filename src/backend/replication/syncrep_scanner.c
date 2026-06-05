/*-------------------------------------------------------------------------
 *
 * syncrep_scanner.c
 *	  Parser+lexer driver for the synchronous_standby_names GUC.
 *
 * Lime v0.2.2's lexer subsystem (compiled from syncrep_scanner.lex)
 * replaces the hand-rolled tokenizer that used to live in this file.
 * What remains here:
 *
 *   - syncrep_scanner_init / syncrep_scanner_finish: lifecycle of the
 *     SyncRepYyScanner state owned by the GUC code in syncrep.c.
 *   - syncrep_yyparse: the parser-driver loop that calls
 *     SyncRepLexFeedBytes once over the entire input string and feeds
 *     emitted tokens into the Lime parser via syncrep_yy().
 *   - syncrep_yyerror: identical-shaped public error helper, called
 *     by the grammar's %syntax_error and %parse_failure blocks.
 *
 * The public interface declared in include/replication/syncrep.h is
 * unchanged; syncrep.c needs no source-level updates.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/replication/syncrep_scanner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "syncrep_gram.h"
#include "syncrep_parse.h"
#include "syncrep_scanner_lex.h"	/* SyncRepLexer, SyncRepLexAlloc,
									 * SyncRepLexFeedBytes, SyncRepLexFeedEOF,
									 * SyncRepLexFree, SYNCREP_LEX_OK */

/* ------------------------------------------------------------------------- */
/* Scanner state lifecycle                                                   */
/* ------------------------------------------------------------------------- */

void
syncrep_scanner_init(const char *str, yyscan_t *yyscannerp)
{
	SyncRepYyScanner *s = palloc0_object(SyncRepYyScanner);

	/*
	 * Take a private copy of the input so the lexer's matched-byte pointers
	 * (handed back through emit callbacks) remain valid for the lifetime of
	 * the parse.
	 */
	s->input = pstrdup(str);
	s->len = (int) strlen(s->input);
	s->pos = 0;

	initStringInfo(&s->yytext);

	*yyscannerp = (yyscan_t) s;
}

void
syncrep_scanner_finish(yyscan_t yyscanner)
{
	SyncRepYyScanner *s = (SyncRepYyScanner *) yyscanner;

	if (s == NULL)
		return;

	if (s->yytext.data != NULL)
		pfree(s->yytext.data);
	if (s->input != NULL)
		pfree(s->input);
	pfree(s);
}

/* ------------------------------------------------------------------------- */
/* Error reporting                                                           */
/* ------------------------------------------------------------------------- */

void
syncrep_yyerror(SyncRepConfigData **syncrep_parse_result_p,
				char **syncrep_parse_error_msg_p,
				yyscan_t yyscanner,
				const char *message)
{
	SyncRepYyScanner *s = (SyncRepYyScanner *) yyscanner;
	const char *yytext = (s != NULL && s->yytext.data != NULL) ? s->yytext.data : "";

	/* Report only the first error in a parse operation. */
	if (*syncrep_parse_error_msg_p)
		return;
	if (yytext[0])
		*syncrep_parse_error_msg_p = psprintf("%s at or near \"%s\"",
											  message, yytext);
	else
		*syncrep_parse_error_msg_p = psprintf("%s at end of input",
											  message);
}

/* ------------------------------------------------------------------------- */
/* Lexer -> parser bridge                                                    */
/* ------------------------------------------------------------------------- */

struct EmitContext
{
	SyncRepYyScanner *scanner;
	SyncRepParseCtx *parse_ctx;
	void	   *parser;
};

/*
 * Runs once per Lime-emitted token.  text/len point at the matched
 * span (or at a heap copy taken by LEX_BUF_TAKE for delimited
 * identifiers).  We pstrdup into yylval->str for value-bearing tokens
 * and feed the parser via syncrep_yy().  For the wildcard "*" rule
 * the original flex scanner used the literal "*" as the token text;
 * we pstrdup the matched span (which is "*") -- same observable
 * result.
 */
static void
syncrep_emit_cb(void *user, int token, const char *text, size_t len)
{
	struct EmitContext *ctx = user;
	SyncRepYyScanner *s = ctx->scanner;
	SyncRepToken tok = {0};

	/* Track the last lexeme for error messages. */
	resetStringInfo(&s->yytext);
	appendBinaryStringInfo(&s->yytext, text, len);

	/*
	 * Stop feeding the parser if a previous token already produced a
	 * scanner-level error (currently only the unterminated-quoted- identifier
	 * case).  syncrep_yyerror's first-error-wins guard makes this safe even
	 * if the parser would have generated its own %syntax_error too.
	 */
	if (*ctx->parse_ctx->error_msg_p != NULL)
		return;

	switch (token)
	{
		case NAME:
		case NUM:
			{
				char	   *dup = palloc(len + 1);

				memcpy(dup, text, len);
				dup[len] = '\0';
				tok.str = dup;
				break;
			}
		default:
			/* COMMA, LPAREN, RPAREN, ANY, FIRST, JUNK: no payload. */
			break;
	}

	syncrep_yy(ctx->parser, token, tok, ctx->parse_ctx);
}

/* ------------------------------------------------------------------------- */
/* Parser entry point                                                        */
/* ------------------------------------------------------------------------- */

int
syncrep_yyparse(SyncRepConfigData **syncrep_parse_result_p,
				char **syncrep_parse_error_msg_p,
				yyscan_t yyscanner)
{
	SyncRepYyScanner *s = (SyncRepYyScanner *) yyscanner;
	SyncRepLexer *lex;
	SyncRepParseCtx parse_ctx = {
		.result_p = syncrep_parse_result_p,
		.error_msg_p = syncrep_parse_error_msg_p,
		.scanner = yyscanner,
	};
	struct EmitContext ctx;
	int			lex_status;
	SyncRepToken eof_tok = {0};

	lex = SyncRepLexAlloc(palloc);
	if (lex == NULL)
	{
		*syncrep_parse_error_msg_p = pstrdup("out of memory in syncrep lexer");
		return 1;
	}

	ctx.scanner = s;
	ctx.parse_ctx = &parse_ctx;
	ctx.parser = syncrep_yyAlloc(palloc);

	lex_status = SyncRepLexFeedBytes(lex, s->input, s->len,
									 syncrep_emit_cb, &ctx);
	if (lex_status != SYNCREP_LEX_OK && *syncrep_parse_error_msg_p == NULL)
	{
		const char *m = SyncRepLexErrorMessage(lex);

		syncrep_yyerror(syncrep_parse_result_p, syncrep_parse_error_msg_p,
						yyscanner, m ? m : "syntax error");
	}
	else
	{
		/* Drain any pending EOF-time rules (e.g. xd state's <<EOF>>). */
		(void) SyncRepLexFeedEOF(lex, syncrep_emit_cb, &ctx);
	}

	/* Finalise the parse with an EOF token unless we already errored. */
	if (*syncrep_parse_error_msg_p == NULL)
		syncrep_yy(ctx.parser, 0, eof_tok, &parse_ctx);

	syncrep_yyFree(ctx.parser, pfree);
	SyncRepLexFree(lex, pfree);

	return (*syncrep_parse_error_msg_p != NULL) ? 1 : 0;
}
