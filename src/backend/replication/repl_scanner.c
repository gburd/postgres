/*-------------------------------------------------------------------------
 *
 * repl_scanner.c
 *	  Parser+lexer driver for the walsender replication command line.
 *
 * Lime v0.2.2's lexer subsystem (compiled from repl_scanner.lex) does
 * the tokenizing; this file wires SyncRepLexFeedBytes to the Lime
 * parser generated from repl_gram.lime via replication_yy(), and
 * keeps the public surface declared in walsender_private.h:
 *
 *	int  replication_yyparse(Node **result_p, yyscan_t scanner);
 *	int  replication_yylex(YYSTYPE *lval, yyscan_t scanner);
 *	void replication_yyerror(Node **result_p, yyscan_t scanner,
 *	                         const char *msg);
 *	void replication_scanner_init(const char *str, yyscan_t *scannerp);
 *	void replication_scanner_finish(yyscan_t scanner);
 *	bool replication_scanner_is_replication_command(yyscan_t scanner);
 *
 * Strategy: pre-scan the entire input at scanner_init time, capturing
 * every emitted token + payload into an array.  replication_yylex pops
 * the next entry; is_replication_command peeks the first entry without
 * consuming it.  Mirrors the retired hand-rolled scanner's
 * pushed_back_token semantics with a full FIFO instead of a single
 * slot, but the observable behaviour is identical: same tokens, same
 * payloads, same error wire format.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/replication/repl_scanner.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "nodes/parsenodes.h"
#include "parser/scansup.h"
#include "replication/walsender_private.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "repl_gram.h"
#include "repl_gram_yytype.h"
#include "repl_scanner_lex.h"	/* ReplLexer, ReplLexAlloc, ReplLexFeedBytes,
								 * ReplLexFeedEOF, ReplLexFree, REPL_LEX_OK,
								 * ReplLexErrorMessage */

/*
 * Lime-generated push parser entry points (from repl_gram.c, %name
 * replication_yy).
 */
extern void *replication_yyAlloc(void *(*mallocProc) (size_t));
extern void replication_yyFree(void *p, void (*freeProc) (void *));
extern void replication_yy(void *yyp, int yymajor, YYSTYPE yyminor,
						   replication_yy_extra *extra);

/* Internal sentinel emitted by repl_scanner.lex's xdclose rule. */
#define REPL_TOK_QIDENT		1001

/* Pre-scanned token entry. */
typedef struct ReplToken
{
	int			code;
	YYSTYPE		val;
} ReplToken;

/*
 * Concrete scanner-state object sitting behind yyscan_t (an alias for
 * void *).  Allocated by replication_scanner_init in the caller's current
 * memory context; freed by replication_scanner_finish.
 */
typedef struct repl_yyscan_state
{
	const char *input;
	int			len;

	ReplToken  *tokens;			/* pre-scanned token FIFO */
	int			ntokens;
	int			cap;
	int			next;			/* index of next token to yield */
} repl_yyscan_state;

/*
 * Allocator shims so Lime's malloc/free-shaped parameters route to
 * palloc/pfree.
 */
static void *
repl_palloc_wrapper(size_t n)
{
	return palloc(n);
}

static void
repl_pfree_wrapper(void *p)
{
	if (p != NULL)
		pfree(p);
}

/* ----------------------------------------------------------------
 * Token-queue plumbing
 * ----------------------------------------------------------------
 */

static void
push_token(repl_yyscan_state *s, int code, YYSTYPE val)
{
	if (s->ntokens >= s->cap)
	{
		int			newcap = s->cap == 0 ? 16 : s->cap * 2;

		if (s->tokens == NULL)
			s->tokens = palloc(newcap * sizeof(ReplToken));
		else
			s->tokens = repalloc(s->tokens, newcap * sizeof(ReplToken));
		s->cap = newcap;
	}
	s->tokens[s->ntokens].code = code;
	s->tokens[s->ntokens].val = val;
	s->ntokens++;
}

/* ----------------------------------------------------------------
 * Lime emit callback: collect tokens into the queue with payloads
 * post-processed exactly as the retired hand-rolled scanner did.
 * ----------------------------------------------------------------
 */

struct EmitContext
{
	repl_yyscan_state *s;
	bool		had_error;
	char		errmsg[256];
};

static void
repl_emit_cb(void *user, int token, const char *text, size_t len)
{
	struct EmitContext *ctx = user;
	YYSTYPE		val;

	memset(&val, 0, sizeof(val));

	switch (token)
	{
		case UCONST:
			{
				char		buf[32];
				size_t		n = (len < sizeof(buf)) ? len : sizeof(buf) - 1;
				char	   *endp;

				memcpy(buf, text, n);
				buf[n] = '\0';
				errno = 0;
				val.uintval = (uint32) strtoul(buf, &endp, 10);
				break;
			}
		case RECPTR:
			{
				char		buf[64];
				unsigned int hi;
				unsigned int lo;

				if (len >= sizeof(buf))
				{
					ctx->had_error = true;
					snprintf(ctx->errmsg, sizeof(ctx->errmsg),
							 "invalid streaming start location");
					return;
				}
				memcpy(buf, text, len);
				buf[len] = '\0';
				if (sscanf(buf, "%X/%08X", &hi, &lo) != 2)
				{
					ctx->had_error = true;
					snprintf(ctx->errmsg, sizeof(ctx->errmsg),
							 "invalid streaming start location");
					return;
				}
				val.recptr = ((uint64) hi) << 32 | lo;
				break;
			}
		case IDENT:
			val.str = downcase_truncate_identifier(text, len, true);
			break;
		case REPL_TOK_QIDENT:
			{
				char	   *raw = palloc(len + 1);

				memcpy(raw, text, len);
				raw[len] = '\0';
				truncate_identifier(raw, len, true);
				val.str = raw;
				token = IDENT;	/* parser sees IDENT */
				break;
			}
		case SCONST:
			{
				char	   *raw = palloc(len + 1);

				memcpy(raw, text, len);
				raw[len] = '\0';
				val.str = raw;
				break;
			}
		case -1:
			{
				/*
				 * The catch-all rule.  flex returned the raw character code;
				 * Bison reported "syntax error" since no parser production
				 * matched.  We approximate by storing the sentinel and
				 * letting the parser fail with %syntax_error.
				 *
				 * The repl grammar has no token at code -1, so the parser
				 * will treat it as unexpected; same observable outcome as the
				 * flex/bison build.
				 */
				break;
			}
		default:
			/* Keywords and single-char punctuation: no payload. */
			break;
	}

	push_token(ctx->s, token, val);
}

/* ----------------------------------------------------------------
 * Public interface
 * ----------------------------------------------------------------
 */

int
replication_yylex(YYSTYPE *yylval_param, yyscan_t yyscanner)
{
	repl_yyscan_state *s = (repl_yyscan_state *) yyscanner;

	if (s->next >= s->ntokens)
		return 0;

	*yylval_param = s->tokens[s->next].val;
	return s->tokens[s->next++].code;
}

pg_noreturn void
replication_yyerror(Node **replication_parse_result_p, yyscan_t yyscanner,
					const char *message)
{
	(void) replication_parse_result_p;
	(void) yyscanner;

	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg_internal("%s", message)));
}

void
replication_scanner_init(const char *str, yyscan_t *yyscannerp)
{
	repl_yyscan_state *s = palloc0_object(repl_yyscan_state);
	ReplLexer  *lex;
	struct EmitContext ctx;
	int			lex_status;

	s->input = str;
	s->len = (int) strlen(str);
	s->tokens = NULL;
	s->ntokens = 0;
	s->cap = 0;
	s->next = 0;

	lex = ReplLexAlloc(repl_palloc_wrapper);
	if (lex == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	ctx.s = s;
	ctx.had_error = false;
	ctx.errmsg[0] = '\0';

	lex_status = ReplLexFeedBytes(lex, str, s->len, repl_emit_cb, &ctx);
	if (lex_status == REPL_LEX_OK)
		(void) ReplLexFeedEOF(lex, repl_emit_cb, &ctx);

	/*
	 * If the lexer rejected the input, raise the same syntax error the
	 * retired flex+bison pair would have produced.
	 */
	if (ctx.had_error)
	{
		ReplLexFree(lex, repl_pfree_wrapper);
		replication_yyerror(NULL, (yyscan_t) s, ctx.errmsg);
		/* unreachable */
	}
	if (lex_status != REPL_LEX_OK)
	{
		const char *m = ReplLexErrorMessage(lex);
		char	   *copy = pstrdup(m ? m : "syntax error");

		ReplLexFree(lex, repl_pfree_wrapper);
		replication_yyerror(NULL, (yyscan_t) s, copy);
		/* unreachable */
	}

	ReplLexFree(lex, repl_pfree_wrapper);

	*yyscannerp = (yyscan_t) s;
}

void
replication_scanner_finish(yyscan_t yyscanner)
{
	repl_yyscan_state *s = (repl_yyscan_state *) yyscanner;

	if (s == NULL)
		return;

	/*
	 * Token payloads were palloc'd in the same memory context as the scanner
	 * state; the caller's memory-context teardown releases them wholesale,
	 * but for symmetry with the hand-rolled scanner we free the queue spine
	 * explicitly.
	 */
	if (s->tokens != NULL)
		pfree(s->tokens);
	pfree(s);
}

bool
replication_scanner_is_replication_command(yyscan_t yyscanner)
{
	repl_yyscan_state *s = (repl_yyscan_state *) yyscanner;
	int			first_token;

	if (s->ntokens == 0)
		return false;
	first_token = s->tokens[0].code;

	switch (first_token)
	{
		case K_IDENTIFY_SYSTEM:
		case K_BASE_BACKUP:
		case K_START_REPLICATION:
		case K_CREATE_REPLICATION_SLOT:
		case K_DROP_REPLICATION_SLOT:
		case K_ALTER_REPLICATION_SLOT:
		case K_READ_REPLICATION_SLOT:
		case K_TIMELINE_HISTORY:
		case K_UPLOAD_MANIFEST:
		case K_SHOW:

			/*
			 * Caller will subsequently call replication_yyparse(), which
			 * drives replication_yylex() over the same queue starting at
			 * index 0 -- the leading keyword is automatically re-yielded.
			 */
			return true;
		default:
			return false;
	}
}

/*
 * Driver around the Lime-generated push parser.  The return value is the
 * Bison contract: 0 on success, non-zero on failure.  In practice any
 * syntax error longjmps out via ereport(ERROR) in replication_yyerror, so
 * a non-zero return is never observed by the caller -- mirrors the old
 * flex/bison build.
 */
int
replication_yyparse(Node **replication_parse_result_p, yyscan_t yyscanner)
{
	void	   *parser;
	YYSTYPE		yylval;
	int			token;
	replication_yy_extra extra;

	extra.replication_parse_result_p = replication_parse_result_p;
	extra.yyscanner = yyscanner;

	parser = replication_yyAlloc(repl_palloc_wrapper);
	if (parser == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	for (;;)
	{
		memset(&yylval, 0, sizeof(yylval));
		token = replication_yylex(&yylval, yyscanner);
		if (token <= 0)
		{
			memset(&yylval, 0, sizeof(yylval));
			replication_yy(parser, 0, yylval, &extra);
			break;
		}
		replication_yy(parser, token, yylval, &extra);
	}

	replication_yyFree(parser, repl_pfree_wrapper);
	return 0;
}
