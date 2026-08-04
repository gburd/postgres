/*-------------------------------------------------------------------------
 *
 * pgpa_parser_driver.c
 *	  Parser+lexer driver for plan advice.
 *
 * Wires Lime's push parser (generated from pgpa_parser.lime) to
 * Lime's lexer (generated from pgpa_scanner.lex).  Replaces the
 * flex-generated tokenizer that lived in pgpa_scanner.l.
 *
 * Public interface preserved (callers in pg_plan_advice.c et al.):
 *   pgpa_yyparse(List **result, char **err, yyscan_t scanner)
 *   pgpa_yylex(union YYSTYPE *lval, List **result, char **err,
 *              yyscan_t scanner)
 *   pgpa_yyerror(List **result, char **err, yyscan_t scanner,
 *                const char *message)
 *   pgpa_scanner_init(const char *str, yyscan_t *yyscannerp)
 *   pgpa_scanner_finish(yyscan_t yyscanner)
 *
 * Portions Copyright (c) 2000-2026, PostgreSQL Global Development Group
 *
 * contrib/pg_plan_advice/pgpa_parser_driver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "nodes/miscnodes.h"
#include "parser/scansup.h"
#include "utils/builtins.h"

#include "pgpa_ast.h"
#include "pgpa_parser_yytype.h"
#include "pgpa_parser.h"
#include "pgpa_scanner_lex.h"	/* PgpaLexer, PgpaLexAlloc, PgpaLexFeedBytes,
								 * PgpaLexFeedEOF, PgpaLexFree, PGPA_LEX_OK,
								 * PgpaLexErrorMessage */

/* Layout matches the converter's emitted struct GramParseExtra body. */
struct GramParseExtra
{
	List	  **result;
	char	  **parse_error_msg_p;
	void	   *yyscanner;
	bool		aborted;
};

/* Lime push parser entry points (%name pgpa_yy in pgpa_parser.lime). */
extern void *pgpa_yyAlloc(void *(*mallocProc) (size_t));
extern void pgpa_yyFree(void *p, void (*freeProc) (void *));
extern void pgpa_yy(void *yyp, int yymajor, YYSTYPE yyminor,
					struct GramParseExtra *extra);

/*
 * yyscan_t handle.  Holds the pre-scanned token FIFO + the most-recent
 * token text for error messages.
 */
typedef struct PgpaToken
{
	int			code;
	YYSTYPE		val;
} PgpaToken;

typedef struct PgpaYyScanner
{
	const char *input;
	int			input_len;
	PgpaToken  *tokens;
	int			ntokens;
	int			cap;
	int			next;
	StringInfoData yytext;

	/*
	 * A lex-time error (integer out of range, trailing junk after a numeric
	 * literal) is detected during the pre-scan, before the parser runs.  We
	 * stash its message + offending text here so pgpa_yyerror reports it
	 * verbatim ("<message> at or near \"<text>\"") rather than the generic
	 * parser-position "syntax error", matching the retired flex scanner.
	 */
	char	   *lex_errmsg;
	char	   *lex_errtext;
} PgpaYyScanner;

#define PGPA_TOK_QIDENT		1001	/* internal sentinel from xd_close */

static void *
pgpa_palloc(size_t n)
{
	return palloc(n);
}

static void
pgpa_pfree(void *p)
{
	if (p != NULL)
		pfree(p);
}

static void
pgpa_push_token(PgpaYyScanner *s, int code, YYSTYPE val)
{
	if (s->ntokens >= s->cap)
	{
		int			newcap = s->cap == 0 ? 16 : s->cap * 2;

		if (s->tokens == NULL)
			s->tokens = palloc(newcap * sizeof(PgpaToken));
		else
			s->tokens = repalloc(s->tokens, newcap * sizeof(PgpaToken));
		s->cap = newcap;
	}
	s->tokens[s->ntokens].code = code;
	s->tokens[s->ntokens].val = val;
	s->ntokens++;
}

struct EmitContext
{
	PgpaYyScanner *s;
	bool		had_error;
	StringInfoData errmsg;
};

static void
pgpa_emit_cb(void *user, int token, const char *text, size_t len)
{
	struct EmitContext *ctx = user;
	YYSTYPE		val;

	memset(&val, 0, sizeof(val));

	switch (token)
	{
		case TOK_IDENT:
			{
				char	   *str;
				bool		fail;
				pgpa_advice_tag_type tag;

				str = downcase_identifier(text, len, false, false);
				val.str = str;

				tag = pgpa_parse_advice_tag(str, &fail);
				if (fail)
				{
					/* Plain identifier; emit as TOK_IDENT. */
					token = TOK_IDENT;
				}
				else if (tag == PGPA_TAG_JOIN_ORDER)
					token = TOK_TAG_JOIN_ORDER;
				else if (tag == PGPA_TAG_INDEX_SCAN ||
						 tag == PGPA_TAG_INDEX_ONLY_SCAN)
					token = TOK_TAG_INDEX;
				else if (tag == PGPA_TAG_SEQ_SCAN ||
						 tag == PGPA_TAG_TID_SCAN ||
						 tag == PGPA_TAG_BITMAP_HEAP_SCAN ||
						 tag == PGPA_TAG_NO_GATHER ||
						 tag == PGPA_TAG_DO_NOT_SCAN)
					token = TOK_TAG_SIMPLE;
				else
					token = TOK_TAG_GENERIC;
				break;
			}
		case PGPA_TOK_QIDENT:
			{
				char	   *dup = palloc(len + 1);

				memcpy(dup, text, len);
				dup[len] = '\0';
				val.str = dup;
				token = TOK_IDENT;
				break;
			}
		case TOK_INTEGER:
			{
				char		buf[32];
				size_t		n = (len < sizeof(buf)) ? len : sizeof(buf) - 1;
				ErrorSaveContext escontext = {T_ErrorSaveContext};

				memcpy(buf, text, n);
				buf[n] = '\0';
				val.integer = pg_strtoint32_safe(buf, (Node *) &escontext);
				if (escontext.error_occurred)
				{
					ctx->had_error = true;
					if (ctx->errmsg.data == NULL)
						initStringInfo(&ctx->errmsg);
					resetStringInfo(&ctx->errmsg);
					appendStringInfoString(&ctx->errmsg, "integer out of range");
					ctx->s->lex_errmsg = pstrdup("integer out of range");
					ctx->s->lex_errtext = pnstrdup(text, len);
					return;
				}
				break;
			}
		default:
			/* Single-char punctuation: no payload. */
			break;
	}

	/* Track last lexeme for error messages. */
	resetStringInfo(&ctx->s->yytext);
	appendBinaryStringInfo(&ctx->s->yytext, text, len);

	pgpa_push_token(ctx->s, token, val);
}

int
pgpa_yylex(YYSTYPE *yylval_param, List **result, char **err,
		   void *yyscanner)
{
	PgpaYyScanner *s = (PgpaYyScanner *) yyscanner;

	(void) result;
	(void) err;

	if (s->next >= s->ntokens)
		return 0;

	*yylval_param = s->tokens[s->next].val;
	return s->tokens[s->next++].code;
}

void
pgpa_yyerror(List **result, char **parse_error_msg_p, void *yyscanner,
			 const char *message)
{
	PgpaYyScanner *s = (PgpaYyScanner *) yyscanner;
	const char *yytext = (s != NULL && s->yytext.data != NULL) ? s->yytext.data : "";

	(void) result;

	if (*parse_error_msg_p)
		return;

	/*
	 * A lex-time error (integer out of range / trailing junk after a numeric
	 * literal) takes precedence over the generic parser "syntax error": it
	 * pinpoints the real problem.  Report it verbatim, positioned at the
	 * offending lexeme when we captured one.
	 */
	if (s != NULL && s->lex_errmsg != NULL)
	{
		const char *bad = s->lex_errtext ? s->lex_errtext : yytext;

		if (bad[0])
			*parse_error_msg_p = psprintf("%s at or near \"%s\"",
										  s->lex_errmsg, bad);
		else
			*parse_error_msg_p = psprintf("%s", s->lex_errmsg);
		return;
	}

	if (yytext[0])
		*parse_error_msg_p = psprintf("%s at or near \"%s\"", message, yytext);
	else
		*parse_error_msg_p = psprintf("%s at end of input", message);
}

void
pgpa_scanner_init(const char *str, void **yyscannerp)
{
	PgpaYyScanner *s = palloc0_object(PgpaYyScanner);
	PgpaLexer  *lex;
	struct EmitContext ctx;
	int			lex_status;
	int			input_len = (int) strlen(str);

	s->input = str;
	s->input_len = input_len;
	s->tokens = NULL;
	s->ntokens = 0;
	s->cap = 0;
	s->next = 0;
	initStringInfo(&s->yytext);

	lex = PgpaLexAlloc(pgpa_palloc);
	if (lex == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	ctx.s = s;
	ctx.had_error = false;
	ctx.errmsg.data = NULL;

	lex_status = PgpaLexFeedBytes(lex, str, input_len, pgpa_emit_cb, &ctx);
	if (lex_status == PGPA_LEX_OK)
		(void) PgpaLexFeedEOF(lex, pgpa_emit_cb, &ctx);

	*yyscannerp = (void *) s;

	if (ctx.had_error)
	{
		/*
		 * A lex-time value error (integer out of range).  The message and
		 * offending text were stashed on the scanner (s->lex_errmsg /
		 * lex_errtext); pgpa_yyerror surfaces them when the parse fails.
		 */
	}
	if (lex_status != PGPA_LEX_OK)
	{
		const char *m = PgpaLexErrorMessage(lex);

		/*
		 * Surface the "trailing junk after numeric literal" diagnostic (from
		 * the integer_junk rule) verbatim, matching the retired flex scanner.
		 * Other lexer errors (unterminated/zero-length quoted identifier,
		 * unexpected character) keep the generic parser-position "syntax
		 * error" the expected output was written against, so we do not hijack
		 * their message here.
		 */
		if (m != NULL && s->lex_errmsg == NULL &&
			strcmp(m, "trailing junk after numeric literal") == 0)
			s->lex_errmsg = pstrdup(m);
	}

	PgpaLexFree(lex, pgpa_pfree);
}

void
pgpa_scanner_finish(void *yyscanner)
{
	PgpaYyScanner *s = (PgpaYyScanner *) yyscanner;

	if (s == NULL)
		return;

	if (s->tokens != NULL)
		pfree(s->tokens);
	if (s->yytext.data != NULL)
		pfree(s->yytext.data);
	pfree(s);
}

int
pgpa_yyparse(List **result, char **parse_error_msg_p, void *yyscanner)
{
	void	   *parser;
	YYSTYPE		yylval;
	int			token;
	struct GramParseExtra extra;

	extern void pgpa_yy_drain(void *yyp, struct GramParseExtra *extra);

	extra.result = result;
	extra.parse_error_msg_p = parse_error_msg_p;
	extra.yyscanner = yyscanner;
	extra.aborted = false;

	parser = pgpa_yyAlloc(pgpa_palloc);
	if (parser == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	for (;;)
	{
		memset(&yylval, 0, sizeof(yylval));
		token = pgpa_yylex(&yylval, result, parse_error_msg_p, yyscanner);
		if (token == 0 || extra.aborted)
		{
			memset(&yylval, 0, sizeof(yylval));
			pgpa_yy(parser, 0, yylval, &extra);
			break;
		}
		pgpa_yy(parser, token, yylval, &extra);

		/*
		 * Drain pending default reduces eagerly (Phase 2j/3 pattern). Without
		 * this, Lime defers the next reduction until the subsequent token
		 * arrives, which differs from Bison's pull model and causes spurious
		 * syntax errors when an action relies on identifier-then-rule
		 * reductions before the next shift point (here: the AT_SIGN-vs-empty
		 * default reduce on opt_plan_name fires only after the parser sees
		 * AT_SIGN as lookahead).
		 */
		pgpa_yy_drain(parser, &extra);
		if (*parse_error_msg_p != NULL)
			break;
	}

	pgpa_yyFree(parser, pgpa_pfree);
	return (*parse_error_msg_p != NULL) ? 1 : 0;
}
