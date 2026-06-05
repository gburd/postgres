/*-------------------------------------------------------------------------
 *
 * exprscan.c
 *	  Parser+lexer driver for pgbench's expression syntax.
 *
 * Lime v0.2.2's lexer subsystem (compiled from exprscan.lex) does the
 * tokenizing for the EXPR state; this file wires ExprLexFeedBytes into
 * the Lime parser (exprparse.lime, %name expr_yy) and keeps the public
 * surface declared in pgbench.h:
 *
 *	int  expr_yyparse(PgBenchExpr **expr_parse_result_p, yyscan_t scanner);
 *	int  expr_yylex(union YYSTYPE *yylval, yyscan_t scanner);
 *	void expr_yyerror(PgBenchExpr **expr_parse_result_p, yyscan_t scanner,
 *	                  const char *msg);
 *	void expr_yyerror_more(yyscan_t scanner, const char *msg, const char *more);
 *	bool expr_lex_one_word(PsqlScanState state, PQExpBuffer word_buf, int *offset);
 *	yyscan_t expr_scanner_init(PsqlScanState state, const char *source,
 *	                           int lineno, int start_offset, const char *command);
 *	void expr_scanner_finish(yyscan_t scanner);
 *	char *expr_scanner_get_substring(PsqlScanState state, int start_offset, bool chomp);
 *
 * Strategy:
 *	- expr_scanner_init pre-scans every EXPR token from the current
 *	  cursor through the next newline (or end-of-buffer) using one
 *	  ExprLexFeedBytes call, capturing token codes + payloads in a
 *	  per-scan FIFO.  expr_yylex pops one token per call, advancing the
 *	  PsqlScanState cursor as it goes (so psql_scan_get_location stays
 *	  accurate during error reporting).
 *	- INITIAL-state lexing (expr_lex_one_word) stays hand-rolled; it is
 *	  a small word-splitter that writes one word per call into
 *	  PsqlScanState->output_buf, which doesn't fit Lime's pre-scan
 *	  emit-callback shape.
 *	- The grammar's parser-driver loop (expr_yyparse) and the
 *	  PgBenchExpr constructors (pgb_make_*, pgb_find_func) are
 *	  unchanged from the previous hand-rolled scanner.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/bin/pgbench/exprscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <string.h>

#include "common/string.h"
#include "fe_utils/psqlscan.h"
#include "fe_utils/psqlscan_int.h"
#include "pgbench.h"

#include "exprscan_internal.h"
#include "exprparse.h"			/* generated parser-side token codes */
#include "exprscan_lex.h"		/* ExprLexer, ExprLexAlloc, ExprLexFeedBytes,
								 * ExprLexFeedEOF, ExprLexFree, EXPR_LEX_OK,
								 * ExprLexErrorMessage */

/*
 * Internal sentinels emitted by exprscan.lex.  Keep in sync with the
 * %include block of exprscan.lex.
 */
#define EXPR_TOK_EOL			1000
#define EXPR_TOK_FUNC_OR_KW		1001
#define EXPR_TOK_UNEXPECTED		1002

/* Forward declarations -- definitions live below. */
PgBenchExpr *pgb_make_null_constant(void);
PgBenchExpr *pgb_make_integer_constant(int64 ival);
PgBenchExpr *pgb_make_double_constant(double dval);
PgBenchExpr *pgb_make_boolean_constant(bool bval);
PgBenchExpr *pgb_make_variable(char *varname);
PgBenchExpr *pgb_make_op(yyscan_t yyscanner, const char *op,
						 PgBenchExpr *l, PgBenchExpr *r);
PgBenchExpr *pgb_make_uop(yyscan_t yyscanner, const char *op,
						  PgBenchExpr *e);
int			pgb_find_func(yyscan_t yyscanner, const char *fname);
PgBenchExprList *pgb_make_elist(PgBenchExpr *expr, PgBenchExprList *list);
PgBenchExpr *pgb_make_func(yyscan_t yyscanner, int fnumber,
						   PgBenchExprList *args);
PgBenchExpr *pgb_make_case(yyscan_t yyscanner,
						   PgBenchExprList *when_then,
						   PgBenchExpr *else_part);
void		expr_yyerror_token(expr_yy_extra *extra, int yymajor,
							   const char *message);

/* ------------------------------------------------------------------------- */
/* Module-scope expression-context state.                                    */
/*                                                                            */
/* These are reset on each expr_scanner_init() and read by                   */
/* expr_yyerror_more() to reconstruct the source line for error display.    */
/* ------------------------------------------------------------------------- */

static const char *expr_source = NULL;
static int	expr_lineno = 0;
static int	expr_start_offset = 0;
static const char *expr_command = NULL;
static bool last_was_newline = false;

/* ------------------------------------------------------------------------- */
/* Token FIFO (populated at expr_scanner_init time, consumed by expr_yylex). */
/* ------------------------------------------------------------------------- */

typedef struct ExprToken
{
	int			code;			/* parser token code, or 0 for EOL */
	YYSTYPE		val;
	int			end_offset;		/* scanbuf offset just past this token */
} ExprToken;

static ExprToken *expr_tokens = NULL;
static int	expr_ntokens = 0;
static int	expr_tokens_cap = 0;
static int	expr_tokens_next = 0;

static void
expr_push_token(int code, YYSTYPE val, int end_offset)
{
	if (expr_ntokens >= expr_tokens_cap)
	{
		int			newcap = expr_tokens_cap == 0 ? 16 : expr_tokens_cap * 2;

		if (expr_tokens == NULL)
			expr_tokens = pg_malloc_array(ExprToken, newcap);
		else
			expr_tokens = pg_realloc(expr_tokens,
									 newcap * sizeof(ExprToken));
		expr_tokens_cap = newcap;
	}
	expr_tokens[expr_ntokens].code = code;
	expr_tokens[expr_ntokens].val = val;
	expr_tokens[expr_ntokens].end_offset = end_offset;
	expr_ntokens++;
}

static void
expr_clear_tokens(void)
{
	if (expr_tokens != NULL)
		pg_free(expr_tokens);
	expr_tokens = NULL;
	expr_ntokens = 0;
	expr_tokens_cap = 0;
	expr_tokens_next = 0;
}

/* ------------------------------------------------------------------------- */
/* Cursor helpers (mirror psqlscan.c / psqlscanslash.c).                     */
/* ------------------------------------------------------------------------- */

static inline const char *
cur_buf(PsqlScanState state)
{
	if (state->buffer_stack != NULL)
		return state->buffer_stack->bufstring;
	return state->scanbuf;
}

static inline int
cur_buf_len(PsqlScanState state)
{
	if (state->buffer_stack != NULL)
		return state->buffer_stack->buflen;
	return state->scanbuflen;
}

static inline int
cur_pos(PsqlScanState state)
{
	if (state->buffer_stack != NULL)
		return state->buffer_stack->pos;
	return state->scanbufpos;
}

static inline void
set_cur_pos(PsqlScanState state, int new_pos)
{
	if (state->buffer_stack != NULL)
		state->buffer_stack->pos = new_pos;
	else
		state->scanbufpos = new_pos;
}

/* ------------------------------------------------------------------------- */
/* Character-class predicates -- used only by INITIAL-mode word scanning.   */
/* ------------------------------------------------------------------------- */

static inline bool
is_space_ch(unsigned char c)
{
	/* {space} = [ \t\r\f\v]  (no newline) */
	return c == ' ' || c == '\t' || c == '\r' || c == '\f' || c == '\v';
}

static inline bool
is_nonspace_ch(unsigned char c)
{
	return c != '\0' && c != ' ' && c != '\t' && c != '\r' &&
		c != '\f' && c != '\v' && c != '\n';
}

static int
match_continuation(const char *p, int avail)
{
	int			i;

	if (avail == 0 || p[0] != '\\')
		return 0;
	i = 1;
	if (i < avail && p[i] == '\r')
		i++;
	if (i >= avail || p[i] != '\n')
		return 0;
	return i + 1;
}

/* ------------------------------------------------------------------------- */
/* Keyword table for case-insensitive lookup of EXPR_TOK_FUNC_OR_KW.         */
/* ------------------------------------------------------------------------- */

static const struct
{
	const char *kw;
	int			len;
	int			tok;
}			expr_keywords[] = {

	/*
	 * Order copied from the retired hand-rolled MATCH_KW block.  Lookup is
	 * exact-length-match so order is irrelevant for correctness; preserved
	 * for diff readability.
	 */
	{"isnull", 6, ISNULL_OP},
	{"notnull", 7, NOTNULL_OP},
	{"and", 3, AND_OP},
	{"or", 2, OR_OP},
	{"not", 3, NOT_OP},
	{"is", 2, IS_OP},
	{"case", 4, CASE_KW},
	{"when", 4, WHEN_KW},
	{"then", 4, THEN_KW},
	{"else", 4, ELSE_KW},
	{"end", 3, END_KW},
	{"null", 4, NULL_CONST},
	/* TRUE and FALSE are handled separately because they need bval payload. */
};

static const int n_expr_keywords =
(int) (sizeof(expr_keywords) / sizeof(expr_keywords[0]));

/* ------------------------------------------------------------------------- */
/* Lime emit callback.                                                       */
/* ------------------------------------------------------------------------- */

struct EmitContext
{
	const char *input_base;		/* pointer to the bytes fed to Lime */
	int			input_start_offset; /* scanbufpos when pre-scan began */
	bool		had_unexpected;
	char		bad_char[2];	/* first byte of the unexpected char + NUL */
	bool		had_overflow;	/* set on numeric overflow */
	char		overflow_msg[64];
	char	   *overflow_text;	/* palloc'd copy of the offending literal */
};

static void
expr_emit_cb(void *user, int token, const char *text, size_t len)
{
	struct EmitContext *ctx = user;
	YYSTYPE		val;
	int			end_offset;

	memset(&val, 0, sizeof(val));
	end_offset = ctx->input_start_offset +
		(int) ((text +len) -ctx->input_base);

	switch (token)
	{
		case EXPR_TOK_EOL:

			/*
			 * Push a 0-code sentinel so expr_yylex returns 0 from this slot.
			 * end_offset is just past the newline, so popping it leaves the
			 * cursor where the next pgbench command starts.
			 */
			expr_push_token(0, val, end_offset);
			return;

		case EXPR_TOK_UNEXPECTED:
			ctx->had_unexpected = true;
			ctx->bad_char[0] = (len > 0) ? text[0] : '\0';

			ctx->bad_char[1] = '\0';

			/*
			 * Push the cursor past the offending byte so error reporting
			 * points one past it (matching the retired scanner, which did
			 * set_cur_pos before yyerror).
			 */
			expr_push_token(0, val, end_offset);
			return;

		case VARIABLE:
			{
				/* Drop the leading colon from matched. */
				size_t		nlen = (len > 0) ? len - 1 : 0;
				char	   *s = pg_malloc_array(char, nlen + 1);

				if (nlen > 0)
					memcpy(s, text +1, nlen);
				s[nlen] = '\0';
				val.str = s;
				break;
			}

		case INTEGER_CONST:
			{
				static const char minabs[] = "9223372036854775808";
				char	   *tmp = pg_malloc_array(char, len + 1);

				memcpy(tmp, text, len);
				tmp[len] = '\0';

				if (len == sizeof(minabs) - 1 &&
					memcmp(tmp, minabs, sizeof(minabs) - 1) == 0)
				{
					/*
					 * Special MAXINT_PLUS_ONE token (only legal after unary
					 * minus; the parser handles that via [UNARY] precedence).
					 */
					token = MAXINT_PLUS_ONE_CONST;
					pg_free(tmp);
					break;
				}

				if (!strtoint64(tmp, true, &val.ival))
				{
					ctx->had_overflow = true;
					strlcpy(ctx->overflow_msg, "bigint constant overflow",
							sizeof(ctx->overflow_msg));
					if (ctx->overflow_text != NULL)
						pg_free(ctx->overflow_text);
					ctx->overflow_text = tmp;
				}
				else
				{
					pg_free(tmp);
				}
				break;
			}

		case DOUBLE_CONST:
			{
				char	   *tmp = pg_malloc_array(char, len + 1);

				memcpy(tmp, text, len);
				tmp[len] = '\0';

				if (!strtodouble(tmp, true, &val.dval))
				{
					ctx->had_overflow = true;
					strlcpy(ctx->overflow_msg, "double constant overflow",
							sizeof(ctx->overflow_msg));
					if (ctx->overflow_text != NULL)
						pg_free(ctx->overflow_text);
					ctx->overflow_text = tmp;
				}
				else
				{
					pg_free(tmp);
				}
				break;
			}

		case EXPR_TOK_FUNC_OR_KW:
			{
				/* Case-insensitive keyword lookup over the small table. */
				int			i;

				for (i = 0; i < n_expr_keywords; i++)
				{
					if ((int) len == expr_keywords[i].len &&
						pg_strncasecmp(text, expr_keywords[i].kw,
									   expr_keywords[i].len) == 0)
					{
						token = expr_keywords[i].tok;
						break;
					}
				}
				if (i < n_expr_keywords)
				{
					/* It's a payload-less keyword. */
					break;
				}

				/* Special-case TRUE / FALSE -- they emit BOOLEAN_CONST. */
				if (len == 4 && pg_strncasecmp(text, "true", 4) == 0)
				{
					token = BOOLEAN_CONST;
					val.bval = true;
					break;
				}
				if (len == 5 && pg_strncasecmp(text, "false", 5) == 0)
				{
					token = BOOLEAN_CONST;
					val.bval = false;
					break;
				}

				/* Otherwise it's a function name. */
				{
					char	   *s = pg_malloc_array(char, len + 1);

					memcpy(s, text, len);
					s[len] = '\0';
					val.str = s;
					token = FUNCTION;
				}
				break;
			}

		default:
			/* Single-char punct / two-char ops: no payload. */
			break;
	}

	expr_push_token(token, val, end_offset);
}

/* ------------------------------------------------------------------------- */
/* Pre-scan invocation.                                                      */
/* ------------------------------------------------------------------------- */

static void *
expr_palloc_wrapper(size_t n)
{
	return pg_malloc(n);
}

static void
expr_pfree_wrapper(void *p)
{
	if (p != NULL)
		pg_free(p);
}

/*
 * Run the Lime lexer over the bytes between the current cursor and the
 * next newline (inclusive of the newline) or end-of-buffer.  Captures
 * tokens in the FIFO; advances the cursor (when popping) to each
 * token's recorded end_offset.  Caller must have already cleared any
 * prior FIFO via expr_clear_tokens().
 */
static void
expr_pre_scan(PsqlScanState state, yyscan_t yyscanner)
{
	ExprLexer  *lex;
	struct EmitContext ctx;
	const char *buf;
	int			pos;
	int			buflen;
	int			lex_status;

	/*
	 * We assume buffer_stack is empty for pgbench expressions: the scanner's
	 * :variable handling emits VARIABLE tokens directly, it does not push
	 * expansion buffers.  Should anyone ever wire that up, this assertion
	 * will catch it.
	 */
	Assert(state->buffer_stack == NULL);

	buf = state->scanbuf;
	pos = state->scanbufpos;
	buflen = state->scanbuflen;

	ctx.input_base = buf + pos;
	ctx.input_start_offset = pos;
	ctx.had_unexpected = false;
	ctx.bad_char[0] = '\0';
	ctx.bad_char[1] = '\0';
	ctx.had_overflow = false;
	ctx.overflow_msg[0] = '\0';
	ctx.overflow_text = NULL;

	lex = ExprLexAlloc(expr_palloc_wrapper);
	if (lex == NULL)
		expr_yyerror_more(yyscanner, "out of memory", NULL);

	lex_status = ExprLexFeedBytes(lex, ctx.input_base,
								  (size_t) (buflen - pos),
								  expr_emit_cb, &ctx);
	if (lex_status == EXPR_LEX_OK)
		(void) ExprLexFeedEOF(lex, expr_emit_cb, &ctx);

	if (lex_status != EXPR_LEX_OK)
	{
		const char *m = ExprLexErrorMessage(lex);
		char	   *copy = pg_strdup(m ? m : "lexer error");

		ExprLexFree(lex, expr_pfree_wrapper);
		expr_yyerror_more(yyscanner, copy, NULL);
		/* unreachable */
	}

	ExprLexFree(lex, expr_pfree_wrapper);

	/*
	 * If the lexer found an unexpected byte, mirror the retired scanner:
	 * advance the cursor past it (via the FIFO entry's end_offset on pop) and
	 * longjmp via expr_yyerror_more.
	 */
	if (ctx.had_unexpected)
	{
		/*
		 * Drain prior tokens so the cursor advances to just past the
		 * offending byte, matching the hand-rolled scanner's "set_cur_pos
		 * before yyerror" behaviour.
		 */
		while (expr_tokens_next < expr_ntokens)
		{
			set_cur_pos(state,
						expr_tokens[expr_tokens_next].end_offset);
			expr_tokens_next++;
		}
		expr_yyerror_more(yyscanner, "unexpected character",
						  pg_strdup(ctx.bad_char));
		/* unreachable */
	}

	if (ctx.had_overflow)
	{
		/* Same drain so the cursor sits at the bad literal's end. */
		while (expr_tokens_next < expr_ntokens)
		{
			set_cur_pos(state,
						expr_tokens[expr_tokens_next].end_offset);
			expr_tokens_next++;
		}
		expr_yyerror_more(yyscanner, ctx.overflow_msg,
						  ctx.overflow_text);
		/* unreachable */
	}
}

/* ------------------------------------------------------------------------- */
/* INITIAL-state hand-rolled scanner (whitespace-separated word lex).       */
/* ------------------------------------------------------------------------- */

static int
expr_yylex_initial(PsqlScanState state)
{
	for (;;)
	{
		const char *buf = cur_buf(state);
		int			buflen = cur_buf_len(state);
		int			pos = cur_pos(state);
		int			avail;
		const char *p;
		int			cm;

		if (pos >= buflen)
		{
			if (state->buffer_stack == NULL)
				return 0;
			psqlscan_pop_buffer_stack(state);
			psqlscan_select_top_buffer(state);
			continue;
		}

		p = buf + pos;
		avail = buflen - pos;

		/* {newline} terminates the line. */
		if (p[0] == '\n')
		{
			last_was_newline = true;
			set_cur_pos(state, pos + 1);
			return 0;
		}

		/* {continuation} = \\\r?\n -- skip. */
		cm = match_continuation(p, avail);
		if (cm > 0)
		{
			set_cur_pos(state, pos + cm);
			continue;
		}

		/* {space}+ -- skip. */
		if (is_space_ch((unsigned char) p[0]))
		{
			int			i = 1;

			while (i < avail && is_space_ch((unsigned char) p[i]))
				i++;
			set_cur_pos(state, pos + i);
			continue;
		}

		/*
		 * {nonspace}+ -- emit a word.  flex's combined
		 * `{nonspace}+{continuation}` rule is captured here by detecting a
		 * trailing `\\` immediately before \r?\n and stripping it.
		 */
		if (is_nonspace_ch((unsigned char) p[0]))
		{
			int			i = 1;
			int			wordlen;

			while (i < avail && is_nonspace_ch((unsigned char) p[i]))
				i++;
			wordlen = i;

			if (wordlen >= 1 && p[wordlen - 1] == '\\' &&
				wordlen < avail)
			{
				int			after = wordlen;
				int			cm_skip = 0;

				if (p[after] == '\n')
					cm_skip = 1;
				else if (p[after] == '\r' &&
						 after + 1 < avail && p[after + 1] == '\n')
					cm_skip = 2;

				if (cm_skip > 0)
				{
					int			emit = wordlen - 1;

					psqlscan_emit(state, p, emit);
					set_cur_pos(state, pos + wordlen + cm_skip);
					return 1;
				}
			}

			psqlscan_emit(state, p, wordlen);
			set_cur_pos(state, pos + wordlen);
			return 1;
		}

		/* Anything else: emit one byte. */
		psqlscan_emit(state, p, 1);
		set_cur_pos(state, pos + 1);
		return 1;
	}
}

/* ------------------------------------------------------------------------- */
/* Public scanner entry points.                                              */
/* ------------------------------------------------------------------------- */

int
expr_yylex(union YYSTYPE *yylval, yyscan_t yyscanner)
{
	PsqlScanState state = (PsqlScanState) yyscanner;

	last_was_newline = false;

	if (state->start_state == ST_INITIAL)
		return expr_yylex_initial(state);

	/* EXPR mode: pop next pre-scanned token from the FIFO. */
	if (expr_tokens_next >= expr_ntokens)
	{
		last_was_newline = true;
		return 0;
	}

	*yylval = expr_tokens[expr_tokens_next].val;
	set_cur_pos(state, expr_tokens[expr_tokens_next].end_offset);
	{
		int			code = expr_tokens[expr_tokens_next].code;

		expr_tokens_next++;
		if (code == 0)
		{
			last_was_newline = true;
			return 0;
		}
		return code;
	}
}

void
expr_yyerror(PgBenchExpr **expr_parse_result_p, yyscan_t yyscanner,
			 const char *message)
{
	(void) expr_parse_result_p;
	expr_yyerror_more(yyscanner, message, NULL);
}

void
expr_yyerror_token(expr_yy_extra *extra, int yymajor, const char *message)
{
	(void) yymajor;				/* not currently used */
	expr_yyerror_more(extra->yyscanner, message, NULL);
}

void
expr_yyerror_more(yyscan_t yyscanner, const char *message, const char *more)
{
	PsqlScanState state = (PsqlScanState) yyscanner;
	int			lineno;
	int			error_detection_offset;
	YYSTYPE		lval;
	char	   *full_line;

	psql_scan_get_location(state, &lineno, &error_detection_offset);
	error_detection_offset--;

	/*
	 * While parsing an expression, we may not have collected the whole line
	 * yet from the input source.  Lex till EOL so we can report the whole
	 * line.
	 */
	if (!last_was_newline)
	{
		while (expr_yylex(&lval, yyscanner))
			 /* skip */ ;
	}

	full_line = expr_scanner_get_substring(state, expr_start_offset, true);

	syntax_error(expr_source, expr_lineno, full_line, expr_command,
				 message, more, error_detection_offset - expr_start_offset);
}

bool
expr_lex_one_word(PsqlScanState state, PQExpBuffer word_buf, int *offset)
{
	int			lexresult;
	YYSTYPE		lval;

	Assert(state->scanbuf != NULL);

	state->output_buf = word_buf;
	resetPQExpBuffer(word_buf);
	state->start_state = ST_INITIAL;

	lexresult = expr_yylex(&lval, (yyscan_t) state);

	if (lexresult)
	{
		int			lineno;
		int			end_offset;

		psql_scan_get_location(state, &lineno, &end_offset);
		*offset = end_offset - word_buf->len;
	}
	else
		*offset = -1;

	psql_scan_reselect_sql_lexer(state);

	return (bool) lexresult;
}

yyscan_t
expr_scanner_init(PsqlScanState state,
				  const char *source, int lineno, int start_offset,
				  const char *command)
{
	expr_source = source;
	expr_lineno = lineno;
	expr_start_offset = start_offset;
	expr_command = command;
	last_was_newline = false;

	Assert(state->scanbuf != NULL);

	state->output_buf = NULL;

	/*
	 * Flag dispatch in expr_yylex: any non-INITIAL value picks the EXPR code
	 * path.  The Lime pre-scan does the real work here.
	 */
	state->start_state = ST_XB; /* sentinel: anything != ST_INITIAL */

	expr_clear_tokens();
	expr_pre_scan(state, (yyscan_t) state);

	return (yyscan_t) state;
}

void
expr_scanner_finish(yyscan_t yyscanner)
{
	PsqlScanState state = (PsqlScanState) yyscanner;

	expr_clear_tokens();
	psql_scan_reselect_sql_lexer(state);
}

char *
expr_scanner_get_substring(PsqlScanState state,
						   int start_offset,
						   bool chomp)
{
	char	   *result;
	const char *scanptr = state->scanbuf + start_offset;
	size_t		slen = strlen(scanptr);

	if (chomp)
	{
		while (slen > 0 &&
			   (scanptr[slen - 1] == '\n' || scanptr[slen - 1] == '\r'))
			slen--;
	}

	result = (char *) pg_malloc(slen + 1);
	memcpy(result, scanptr, slen);
	result[slen] = '\0';

	return result;
}

/* ------------------------------------------------------------------------- */
/* Parser driver and constructors (verbatim from the retired exprparse.y).   */
/* ------------------------------------------------------------------------- */

#define PGBENCH_NARGS_VARIABLE	(-1)
#define PGBENCH_NARGS_CASE		(-2)
#define PGBENCH_NARGS_HASH		(-3)
#define PGBENCH_NARGS_PERMUTE	(-4)

PgBenchExpr *
pgb_make_null_constant(void)
{
	PgBenchExpr *expr = pg_malloc_object(PgBenchExpr);

	expr->etype = ENODE_CONSTANT;
	expr->u.constant.type = PGBT_NULL;
	expr->u.constant.u.ival = 0;
	return expr;
}

PgBenchExpr *
pgb_make_integer_constant(int64 ival)
{
	PgBenchExpr *expr = pg_malloc_object(PgBenchExpr);

	expr->etype = ENODE_CONSTANT;
	expr->u.constant.type = PGBT_INT;
	expr->u.constant.u.ival = ival;
	return expr;
}

PgBenchExpr *
pgb_make_double_constant(double dval)
{
	PgBenchExpr *expr = pg_malloc_object(PgBenchExpr);

	expr->etype = ENODE_CONSTANT;
	expr->u.constant.type = PGBT_DOUBLE;
	expr->u.constant.u.dval = dval;
	return expr;
}

PgBenchExpr *
pgb_make_boolean_constant(bool bval)
{
	PgBenchExpr *expr = pg_malloc_object(PgBenchExpr);

	expr->etype = ENODE_CONSTANT;
	expr->u.constant.type = PGBT_BOOLEAN;
	expr->u.constant.u.bval = bval;
	return expr;
}

PgBenchExpr *
pgb_make_variable(char *varname)
{
	PgBenchExpr *expr = pg_malloc_object(PgBenchExpr);

	expr->etype = ENODE_VARIABLE;
	expr->u.variable.varname = varname;
	return expr;
}

PgBenchExpr *
pgb_make_op(yyscan_t yyscanner, const char *op,
			PgBenchExpr *l, PgBenchExpr *r)
{
	return pgb_make_func(yyscanner, pgb_find_func(yyscanner, op),
						 pgb_make_elist(r, pgb_make_elist(l, NULL)));
}

PgBenchExpr *
pgb_make_uop(yyscan_t yyscanner, const char *op, PgBenchExpr *e)
{
	return pgb_make_func(yyscanner, pgb_find_func(yyscanner, op),
						 pgb_make_elist(e, NULL));
}

static const struct
{
	const char *fname;
	int			nargs;
	PgBenchFunction tag;
}			PGBENCH_FUNCTIONS[] = {
	{"+", 2, PGBENCH_ADD},
	{"-", 2, PGBENCH_SUB},
	{"*", 2, PGBENCH_MUL},
	{"/", 2, PGBENCH_DIV},
	{"mod", 2, PGBENCH_MOD},
	{"abs", 1, PGBENCH_ABS},
	{"least", PGBENCH_NARGS_VARIABLE, PGBENCH_LEAST},
	{"greatest", PGBENCH_NARGS_VARIABLE, PGBENCH_GREATEST},
	{"debug", 1, PGBENCH_DEBUG},
	{"pi", 0, PGBENCH_PI},
	{"sqrt", 1, PGBENCH_SQRT},
	{"ln", 1, PGBENCH_LN},
	{"exp", 1, PGBENCH_EXP},
	{"int", 1, PGBENCH_INT},
	{"double", 1, PGBENCH_DOUBLE},
	{"random", 2, PGBENCH_RANDOM},
	{"random_gaussian", 3, PGBENCH_RANDOM_GAUSSIAN},
	{"random_exponential", 3, PGBENCH_RANDOM_EXPONENTIAL},
	{"random_zipfian", 3, PGBENCH_RANDOM_ZIPFIAN},
	{"pow", 2, PGBENCH_POW},
	{"power", 2, PGBENCH_POW},
	{"!and", 2, PGBENCH_AND},
	{"!or", 2, PGBENCH_OR},
	{"!not", 1, PGBENCH_NOT},
	{"&", 2, PGBENCH_BITAND},
	{"|", 2, PGBENCH_BITOR},
	{"#", 2, PGBENCH_BITXOR},
	{"<<", 2, PGBENCH_LSHIFT},
	{">>", 2, PGBENCH_RSHIFT},
	{"=", 2, PGBENCH_EQ},
	{"<>", 2, PGBENCH_NE},
	{"<=", 2, PGBENCH_LE},
	{"<", 2, PGBENCH_LT},
	{"!is", 2, PGBENCH_IS},
	{"!case_end", PGBENCH_NARGS_CASE, PGBENCH_CASE},
	{"hash", PGBENCH_NARGS_HASH, PGBENCH_HASH_MURMUR2},
	{"hash_murmur2", PGBENCH_NARGS_HASH, PGBENCH_HASH_MURMUR2},
	{"hash_fnv1a", PGBENCH_NARGS_HASH, PGBENCH_HASH_FNV1A},
	{"permute", PGBENCH_NARGS_PERMUTE, PGBENCH_PERMUTE},
	{NULL, 0, 0}
};

int
pgb_find_func(yyscan_t yyscanner, const char *fname)
{
	int			i = 0;

	while (PGBENCH_FUNCTIONS[i].fname)
	{
		if (pg_strcasecmp(fname, PGBENCH_FUNCTIONS[i].fname) == 0)
			return i;
		i++;
	}

	expr_yyerror_more(yyscanner, "unexpected function name", fname);
	return -1;					/* not reached */
}

PgBenchExprList *
pgb_make_elist(PgBenchExpr *expr, PgBenchExprList *list)
{
	PgBenchExprLink *cons;

	if (list == NULL)
	{
		list = pg_malloc_object(PgBenchExprList);
		list->head = NULL;
		list->tail = NULL;
	}

	cons = pg_malloc_object(PgBenchExprLink);
	cons->expr = expr;
	cons->next = NULL;

	if (list->head == NULL)
		list->head = cons;
	else
		list->tail->next = cons;

	list->tail = cons;

	return list;
}

static int
elist_length(PgBenchExprList *list)
{
	PgBenchExprLink *link = list != NULL ? list->head : NULL;
	int			len = 0;

	for (; link != NULL; link = link->next)
		len++;
	return len;
}

PgBenchExpr *
pgb_make_func(yyscan_t yyscanner, int fnumber, PgBenchExprList *args)
{
	int			len = elist_length(args);
	PgBenchExpr *expr = pg_malloc_object(PgBenchExpr);

	Assert(fnumber >= 0);

	switch (PGBENCH_FUNCTIONS[fnumber].nargs)
	{
		case PGBENCH_NARGS_VARIABLE:
			if (len == 0)
				expr_yyerror_more(yyscanner, "at least one argument expected",
								  PGBENCH_FUNCTIONS[fnumber].fname);
			break;
		case PGBENCH_NARGS_CASE:
			if (len < 3 || len % 2 != 1)
				expr_yyerror_more(yyscanner,
								  "odd and >= 3 number of arguments expected",
								  "case control structure");
			break;
		case PGBENCH_NARGS_HASH:
			if (len < 1 || len > 2)
				expr_yyerror_more(yyscanner, "unexpected number of arguments",
								  PGBENCH_FUNCTIONS[fnumber].fname);
			if (len == 1)
			{
				PgBenchExpr *var = pgb_make_variable(pg_strdup("default_seed"));

				args = pgb_make_elist(var, args);
			}
			break;
		case PGBENCH_NARGS_PERMUTE:
			if (len < 2 || len > 3)
				expr_yyerror_more(yyscanner, "unexpected number of arguments",
								  PGBENCH_FUNCTIONS[fnumber].fname);
			if (len == 2)
			{
				PgBenchExpr *var = pgb_make_variable(pg_strdup("default_seed"));

				args = pgb_make_elist(var, args);
			}
			break;
		default:
			Assert(PGBENCH_FUNCTIONS[fnumber].nargs >= 0);
			if (PGBENCH_FUNCTIONS[fnumber].nargs != len)
				expr_yyerror_more(yyscanner, "unexpected number of arguments",
								  PGBENCH_FUNCTIONS[fnumber].fname);
	}

	expr->etype = ENODE_FUNCTION;
	expr->u.function.function = PGBENCH_FUNCTIONS[fnumber].tag;
	expr->u.function.args = args != NULL ? args->head : NULL;
	if (args)
		pg_free(args);

	return expr;
}

PgBenchExpr *
pgb_make_case(yyscan_t yyscanner, PgBenchExprList *when_then,
			  PgBenchExpr *else_part)
{
	return pgb_make_func(yyscanner,
						 pgb_find_func(yyscanner, "!case_end"),
						 pgb_make_elist(else_part, when_then));
}

/* ------------------------------------------------------------------------- */
/* The Lime push parser driver -- replaces Bison's expr_yyparse().           */
/* ------------------------------------------------------------------------- */

int
expr_yyparse(PgBenchExpr **expr_parse_result_p, yyscan_t yyscanner)
{
	expr_yy_extra extra;
	void	   *parser;
	YYSTYPE		lval;
	int			tok;

	extra.result = expr_parse_result_p;
	extra.yyscanner = yyscanner;
	extra.aborted = false;

	parser = expr_yyAlloc(pg_malloc);

	for (;;)
	{
		memset(&lval, 0, sizeof(lval));
		tok = expr_yylex(&lval, yyscanner);
		if (tok == 0)
		{
			memset(&lval, 0, sizeof(lval));
			expr_yy(parser, 0, lval, &extra);
			break;
		}
		expr_yy(parser, tok, lval, &extra);

		if (extra.aborted)
			break;
	}

	expr_yyFree(parser, pg_free);

	return 0;
}
