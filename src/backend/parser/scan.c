/*-------------------------------------------------------------------------
 *
 * scan.c
 *	  Driver shim around the Lime-generated lexer (scan.lex).
 *
 * The state machine moved to scan.lex (Lime v0.2.2's lexer subsystem).
 * This file holds:
 *
 *   - The public API (scanner_init, scanner_finish, core_yylex,
 *     scanner_errposition, scanner_yyerror, ScanKeywordTokens)
 *     declared in include/parser/scanner.h.  pl_scanner.c and
 *     gram.c (via base_yylex in parser.c) consume it unchanged.
 *
 *   - The pre-scan FIFO: scanner_init feeds the entire input through
 *     CoreLexFeedBytes once, capturing every emitted token plus its
 *     yylval payload into an array.  core_yylex pops from the array.
 *
 *   - The emit callback that translates internal sentinel codes
 *     (SCAN_TOK_*) into parser tokens (gram.h: IDENT, ICONST, FCONST,
 *     ...) with appropriate yylval shaping (downcase_truncate_identifier,
 *     ScanKeywordLookup, pg_strtoint32_safe, sscanf, etc.).
 *
 *   - The action-body helpers declared in scan_lex_internal.h.
 *
 * To preserve the original null-terminator contract that base_yylex
 * relies on (`scanbuf[*llocp + cur_token_length] == '\0'` after each
 * core_yylex), each pop rewrites scanbuf[end_of_current_token] to
 * '\0' and saves the displaced byte for the next pop to restore.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/parser/scan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <ctype.h>
#include <unistd.h>

#include "lib/stringinfo.h"
#include "parser/parser_extension.h"
#include "scan_lex_internal.h"
#include "scan_lex.h"

/*
 * GUC variable.  This is a DIRECT violation of the warning given at the
 * head of gram.y, ie scanner code must not depend on any GUC variables.
 * Used only by the xeescape handler to reject \' in client-only encodings.
 */
int			backslash_quote = BACKSLASH_QUOTE_SAFE_ENCODING;

/* Public keyword-token table.  Mirrors scan.l's exported array. */
#define PG_KEYWORD(kwname, value, category, collabel) value,

const uint16 ScanKeywordTokens[] = {
#include "parser/kwlist.h"
};

#undef PG_KEYWORD

/* Match flex's default 0-byte sentinel at end of scanbuf. */
#define YY_END_OF_BUFFER_CHAR	0

/* Forward decls. */
static void scan_emit_cb(void *user, int rule, const char *text, size_t len);

/* ------------------------------------------------------------------------- */
/* CoreScanner: opaque handle returned by scanner_init.                      */
/* ------------------------------------------------------------------------- */

typedef struct CoreScanner
{
	core_yy_extra_type *extra;	/* must be first; see comment below */
	ScanLexCtx *ctx;			/* the lex context (FIFO-bearing) */
	int			next;			/* index of next FIFO entry to yield */

	/* Null-stuffing state, mirrors the retired hand-rolled scanner. */
	bool		saved_char_set;
	int			saved_pos;
	char		saved_char;

	/* For pre-scan errors that route through scanner_yyerror. */
	YYLTYPE		err_yylloc;
	YYLTYPE    *cur_yylloc;		/* set per-call to the caller's *yylloc */
} CoreScanner;

/*
 * gramparse.h's pg_yyget_extra() reads *(base_yy_extra_type **) yyscanner;
 * the first field must be the pointer to the public extra struct.  This
 * is identical to the flex-era struct yyguts_t layout.
 */

/* ------------------------------------------------------------------------- */
/* Allocator wrappers so Lime's malloc/free-shaped parameters route to       */
/* palloc/pfree.                                                             */
/* ------------------------------------------------------------------------- */

static void *
scan_palloc_wrapper(size_t n)
{
	return palloc(n);
}

static void
scan_pfree_wrapper(void *p)
{
	if (p != NULL)
		pfree(p);
}

/* ------------------------------------------------------------------------- */
/* Char-class predicates used by the operator trimmer.                       */
/* ------------------------------------------------------------------------- */

static inline bool
is_self_ch(unsigned char c)
{
	switch (c)
	{
		case ',':
		case '(':
		case ')':
		case '[':
		case ']':
		case '.':
		case ';':
		case ':':
		case '|':
		case '+':
		case '-':
		case '*':
		case '/':
		case '%':
		case '^':
		case '<':
		case '>':
		case '=':
			return true;
		default:
			return false;
	}
}

/* ------------------------------------------------------------------------- */
/* C-side literal accumulator (replaces %literal_buffer).                    */
/* ------------------------------------------------------------------------- */

void
scan_lex_litbuf_start(void *user)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);

	ctx->extra->literallen = 0;
}

void
scan_lex_addlit(void *user, const char *text, size_t len)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);
	core_yy_extra_type *extra = ctx->extra;

	if ((size_t) extra->literallen + len >= (size_t) extra->literalalloc)
	{
		extra->literalalloc = pg_nextpower2_32(extra->literallen + len + 1);
		extra->literalbuf = (char *) repalloc(extra->literalbuf,
											  extra->literalalloc);
	}
	memcpy(extra->literalbuf + extra->literallen, text, len);
	extra->literallen += (int) len;
}

void
scan_lex_addlitchar(void *user, unsigned char c)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);
	core_yy_extra_type *extra = ctx->extra;

	if (extra->literallen + 1 >= extra->literalalloc)
	{
		extra->literalalloc *= 2;
		extra->literalbuf = (char *) repalloc(extra->literalbuf,
											  extra->literalalloc);
	}
	extra->literalbuf[extra->literallen++] = (char) c;
}

size_t
scan_lex_litbuf_len(void *user)
{
	return (size_t) SCAN_LEX_CTX(user)->extra->literallen;
}

char *
scan_lex_litbuf_take(void *user, size_t *out_len)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);
	core_yy_extra_type *extra = ctx->extra;
	int			llen = extra->literallen;
	char	   *result;

	result = palloc(llen + 1);
	memcpy(result, extra->literalbuf, llen);
	result[llen] = '\0';
	if (out_len != NULL)
		*out_len = (size_t) llen;
	extra->literallen = 0;
	return result;
}

void
scan_lex_set_compound_end(void *user, int start, int end)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);

	ctx->compound_start = start;
	ctx->compound_end = end;
	ctx->compound_pending = true;
}

/* ------------------------------------------------------------------------- */
/* Operator trim helper.                                                     */
/*                                                                           */
/* Returns the length to keep from [text..text+len) per scan.l's {operator}  */
/* rule: trim at the first embedded slash-star or dash-dash, and trim         */
/* trailing +/- sequences if no special chars (~!@#^&|`?%) are present.       */
/* ------------------------------------------------------------------------- */

size_t
scan_lex_op_keep(const char *text, size_t len)
{
	const char *slashstar;
	const char *dashdash;
	size_t		nchars = len;

	/* Trim at first embedded slash-star or dash-dash. */
	slashstar = NULL;
	dashdash = NULL;
	for (size_t i = 0; i + 1 < len; i++)
	{
		if (slashstar == NULL && text[i] == '/' && text[i + 1] == '*')
			slashstar = text +i;

		if (dashdash == NULL && text[i] == '-' && text[i + 1] == '-')
			dashdash = text +i;
	}
	if (slashstar && dashdash)
	{
		if (slashstar > dashdash)
			slashstar = dashdash;
	}
	else if (!slashstar)
		slashstar = dashdash;
	if (slashstar)
		nchars = slashstar - text;

	/* Trim trailing +/- if no "special" op-chars remain. */
	if (nchars > 1 &&
		(text[nchars - 1] == '+' || text[nchars - 1] == '-'))
	{
		int			ic;
		bool		has_special = false;

		for (ic = (int) nchars - 2; ic >= 0; ic--)
		{
			char		c = text[ic];

			if (c == '~' || c == '!' || c == '@' ||
				c == '#' || c == '^' || c == '&' ||
				c == '|' || c == '`' || c == '?' ||
				c == '%')
			{
				has_special = true;
				break;
			}
		}
		if (!has_special)
		{
			while (nchars > 1 &&
				   (text[nchars - 1] == '+' || text[nchars - 1] == '-'))
				nchars--;
		}
	}

	return nchars;
}

/* ------------------------------------------------------------------------- */
/* Unicode-escape helpers.                                                   */
/* ------------------------------------------------------------------------- */

/* Forward decl: PG-side error used inside the helpers. */
static void
scan_lex_soft_error(ScanLexCtx *ctx, int pos, int end, const char *msg)
{
	if (ctx->had_error)
		return;
	ctx->had_error = true;
	ctx->err_pos = pos;
	ctx->err_end = end;
	snprintf(ctx->err_msg, sizeof(ctx->err_msg), "%s", msg);
}

void
scan_lex_addunicode(void *user, int pos, char32_t c)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);
	char		buf[MAX_UNICODE_EQUIVALENT_STRING + 1];

	if (!is_valid_unicode_codepoint(c))
	{
		/* end position: pos + length of the \uXXXX or \UXXXXXXXX escape */
		/* The current matched text starts at `pos` and is either 6 or 10 */
		/* chars; we don't know which here, so use the conservative 10 if   */
		/* uppercase \U or 6 if lowercase \u.  scanbuf bounds-check it.    */
		int			end;

		if (pos >= 1 && pos <= ctx->scanbuflen - 2 &&
			ctx->scanbuf[pos] == '\\' && ctx->scanbuf[pos + 1] == 'U')
			end = pos + 10;
		else if (pos >= 0 && pos <= ctx->scanbuflen - 2 &&
				 ctx->scanbuf[pos] == '\\' && ctx->scanbuf[pos + 1] == 'u')
			end = pos + 6;
		else
			end = pos + 1;
		if (end > ctx->scanbuflen)
			end = ctx->scanbuflen;
		scan_lex_soft_error(ctx, pos, end, "invalid Unicode escape value");
		return;
	}

	pg_unicode_to_server(c, (unsigned char *) buf);
	scan_lex_addlit(user, buf, strlen(buf));
}

int
scan_lex_handle_unicode(void *user, int pos, char32_t c)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);

	if (is_utf16_surrogate_first(c))
	{
		ctx->utf16_first_part = c;
		return 1;				/* caller transitions to XEU */
	}
	else if (is_utf16_surrogate_second(c))
	{
		int			end;

		if (pos >= 1 && pos <= ctx->scanbuflen - 2 &&
			ctx->scanbuf[pos] == '\\' && ctx->scanbuf[pos + 1] == 'U')
			end = pos + 10;
		else if (pos >= 0 && pos <= ctx->scanbuflen - 2 &&
				 ctx->scanbuf[pos] == '\\' && ctx->scanbuf[pos + 1] == 'u')
			end = pos + 6;
		else
			end = pos + 1;
		if (end > ctx->scanbuflen)
			end = ctx->scanbuflen;
		scan_lex_soft_error(ctx, pos, end, "invalid Unicode surrogate pair");
		return -1;
	}

	scan_lex_addunicode(user, pos, c);
	return 0;
}

void
scan_lex_handle_xeu_second(void *user, int pos, char32_t c)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);

	if (!is_utf16_surrogate_second(c))
	{
		int			end;

		if (pos >= 1 && pos <= ctx->scanbuflen - 2 &&
			ctx->scanbuf[pos] == '\\' && ctx->scanbuf[pos + 1] == 'U')
			end = pos + 10;
		else if (pos >= 0 && pos <= ctx->scanbuflen - 2 &&
				 ctx->scanbuf[pos] == '\\' && ctx->scanbuf[pos + 1] == 'u')
			end = pos + 6;
		else
			end = pos + 1;
		if (end > ctx->scanbuflen)
			end = ctx->scanbuflen;
		scan_lex_soft_error(ctx, pos, end, "invalid Unicode surrogate pair");
		return;
	}

	c = surrogate_pair_to_codepoint(ctx->utf16_first_part, c);
	scan_lex_addunicode(user, pos, c);
}

void
scan_lex_handle_xeescape(void *user, int pos, unsigned char c)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);
	core_yy_extra_type *extra = ctx->extra;
	unsigned char ch;

	if (c == '\'')
	{
		if (extra->backslash_quote == BACKSLASH_QUOTE_OFF ||
			(extra->backslash_quote == BACKSLASH_QUOTE_SAFE_ENCODING &&
			 PG_ENCODING_IS_CLIENT_ONLY(pg_get_client_encoding())))
			ereport(ERROR,
					(errcode(ERRCODE_NONSTANDARD_USE_OF_ESCAPE_CHARACTER),
					 errmsg("unsafe use of \\' in a string literal"),
					 errhint("Use '' to write quotes in strings. \\' is insecure in client-only encodings."),
					 errposition(pg_mbstrlen_with_len(extra->scanbuf, pos) + 1)));
	}

	switch (c)
	{
		case 'b':
			ch = '\b';
			break;
		case 'f':
			ch = '\f';
			break;
		case 'n':
			ch = '\n';
			break;
		case 'r':
			ch = '\r';
			break;
		case 't':
			ch = '\t';
			break;
		case 'v':
			ch = '\v';
			break;
		default:
			ch = c;
			if (c == '\0' || IS_HIGHBIT_SET(c))
				extra->saw_non_ascii = true;
			break;
	}
	scan_lex_addlitchar(user, ch);
}

void
scan_lex_handle_xehexesc(void *user, const char *text, size_t len)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);
	char		buf[8];
	size_t		hexlen = len - 2;
	unsigned char ch;

	if (hexlen >= sizeof(buf))
		hexlen = sizeof(buf) - 1;
	memcpy(buf, text +2, hexlen);
	buf[hexlen] = '\0';
	ch = (unsigned char) strtoul(buf, NULL, 16);
	scan_lex_addlitchar(user, ch);
	if (ch == '\0' || IS_HIGHBIT_SET(ch))
		ctx->extra->saw_non_ascii = true;
}

void
scan_lex_handle_xeoctesc(void *user, const char *text, size_t len)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);
	char		buf[8];
	size_t		octlen = len - 1;
	unsigned char ch;

	if (octlen >= sizeof(buf))
		octlen = sizeof(buf) - 1;
	memcpy(buf, text +1, octlen);
	buf[octlen] = '\0';
	ch = (unsigned char) strtoul(buf, NULL, 8);
	scan_lex_addlitchar(user, ch);
	if (ch == '\0' || IS_HIGHBIT_SET(ch))
		ctx->extra->saw_non_ascii = true;
}

/* ------------------------------------------------------------------------- */
/* FIFO management                                                           */
/* ------------------------------------------------------------------------- */

static void
push_token(ScanLexCtx *ctx, int code, int start, int end, core_YYSTYPE val)
{
	if (ctx->ntokens >= ctx->cap)
	{
		int			newcap = ctx->cap == 0 ? 64 : ctx->cap * 2;

		if (ctx->tokens == NULL)
			ctx->tokens = palloc(newcap * sizeof(ScanToken));
		else
			ctx->tokens = repalloc(ctx->tokens, newcap * sizeof(ScanToken));
		ctx->cap = newcap;
	}

	ctx->tokens[ctx->ntokens].code = code;
	ctx->tokens[ctx->ntokens].start = start;
	ctx->tokens[ctx->ntokens].end = end;
	ctx->tokens[ctx->ntokens].val = val;
	ctx->ntokens++;
}

/* ------------------------------------------------------------------------- */
/* Emit callback                                                             */
/*                                                                           */
/* Translates (sentinel-or-parser-token, text, len) -> (parser token,         */
/* yylval) and pushes onto the FIFO with [start, end) byte offsets.          */
/* ------------------------------------------------------------------------- */

static void
scan_emit_cb(void *user, int code, const char *text, size_t len)
{
	ScanLexCtx *ctx = SCAN_LEX_CTX(user);
	core_yy_extra_type *extra = ctx->extra;
	core_YYSTYPE val;
	int			start;
	int			end;
	int			out_code = code;

	memset(&val, 0, sizeof(val));

	/*
	 * Once we've recorded a soft error, suppress all further emits to
	 * preserve the FIRST error's position and avoid corrupting the FIFO with
	 * downstream tokens that scanner_yyerror would never reach.
	 */
	if (ctx->had_error)
		return;

	/*
	 * Compute byte-offset range.  For "compound" tokens (strings, quoted
	 * idents, dollar-quoted) the action body sets compound_start/_end via
	 * scan_lex_set_compound_end before the emit; the text ptr in those cases
	 * points at a heap copy unrelated to scanbuf.
	 */
	if (ctx->compound_pending)
	{
		start = ctx->compound_start;
		end = ctx->compound_end;
		ctx->compound_pending = false;
	}
	else
	{
		start = (int) (text -ctx->scanbuf);
		end = start + (int) len;
	}

	switch (code)
	{
		case SCAN_TOK_RAW_CHAR:
			out_code = (unsigned char) text[0];

			break;

		case SCAN_TOK_TYPECAST:
			out_code = TYPECAST;
			break;
		case SCAN_TOK_DOT_DOT:
			out_code = DOT_DOT;
			break;
		case SCAN_TOK_COLON_EQUALS:
			out_code = COLON_EQUALS;
			break;

		case SCAN_TOK_OP:
			{
				/*
				 * len is already the trimmed length (the .lex action did
				 * LEX_PUSHBACK based on scan_lex_op_keep).  Decide whether to
				 * emit a single-char self token, one of the special LA
				 * tokens, or OP.
				 */
				if (len == 1 && is_self_ch((unsigned char) text[0]))
				{
					out_code = (unsigned char) text[0];
				}
				else if (len == 2)
				{
					if (text[0] == '=' && text[1] == '>')
						out_code = EQUALS_GREATER;
					else if (text[0] == '>' && text[1] == '=')
						out_code = GREATER_EQUALS;
					else if (text[0] == '<' && text[1] == '=')
						out_code = LESS_EQUALS;
					else if (text[0] == '<' && text[1] == '>')
						out_code = NOT_EQUALS;
					else if (text[0] == '!' && text[1] == '=')
						out_code = NOT_EQUALS;
					else if (text[0] == '-' && text[1] == '>')
						out_code = RIGHT_ARROW;
					else
					{
						char	   *opstr = palloc(len + 1);

						memcpy(opstr, text, len);
						opstr[len] = '\0';
						val.str = opstr;
						out_code = OP;
					}
				}
				else
				{
					char	   *opstr;

					if (len >= NAMEDATALEN)
					{
						/*
						 * Operator too long; mirror scan.l's yyerror.
						 * Position via err_yylloc so scanner_errposition
						 * picks the right offset.
						 */
						ctx->err_pos = start;
						ctx->err_end = end;
						ctx->had_error = true;
						snprintf(ctx->err_msg, sizeof(ctx->err_msg),
								 "operator too long");
						return;
					}
					opstr = palloc(len + 1);
					memcpy(opstr, text, len);
					opstr[len] = '\0';
					val.str = opstr;
					out_code = OP;
				}
			}
			break;

		case SCAN_TOK_PARAM:
			{
				ErrorSaveContext escontext = {T_ErrorSaveContext};
				char		buf[32];
				size_t		n = (len < sizeof(buf)) ? len : sizeof(buf) - 1;
				int32		v;

				memcpy(buf, text, n);
				buf[n] = '\0';
				/* skip leading $ */
				v = pg_strtoint32_safe(buf + 1, (Node *) &escontext);
				if (escontext.error_occurred)
				{
					ctx->err_pos = start;
					ctx->err_end = end;
					ctx->had_error = true;
					snprintf(ctx->err_msg, sizeof(ctx->err_msg),
							 "parameter number too large");
					return;
				}
				val.ival = v;
				out_code = PARAM;
			}
			break;

		case SCAN_TOK_PARAM_JUNK:
			ctx->err_pos = start;
			ctx->err_end = end;
			ctx->had_error = true;
			snprintf(ctx->err_msg, sizeof(ctx->err_msg),
					 "trailing junk after parameter");
			return;

		case SCAN_TOK_ICONST_DEC:
		case SCAN_TOK_ICONST_HEX:
		case SCAN_TOK_ICONST_OCT:
		case SCAN_TOK_ICONST_BIN:
			{
				ErrorSaveContext escontext = {T_ErrorSaveContext};
				char	   *buf = palloc(len + 1);
				int32		v;

				memcpy(buf, text, len);
				buf[len] = '\0';
				v = pg_strtoint32_safe(buf, (Node *) &escontext);
				if (escontext.error_occurred)
				{
					/* Doesn't fit in int32 (or has decimal point); FCONST. */
					val.str = buf;
					out_code = FCONST;
				}
				else
				{
					pfree(buf);
					val.ival = v;
					out_code = ICONST;
				}
			}
			break;

		case SCAN_TOK_HEXFAIL:
			ctx->err_pos = start;
			ctx->err_end = end;
			ctx->had_error = true;
			snprintf(ctx->err_msg, sizeof(ctx->err_msg),
					 "invalid hexadecimal integer");
			return;
		case SCAN_TOK_OCTFAIL:
			ctx->err_pos = start;
			ctx->err_end = end;
			ctx->had_error = true;
			snprintf(ctx->err_msg, sizeof(ctx->err_msg),
					 "invalid octal integer");
			return;
		case SCAN_TOK_BINFAIL:
			ctx->err_pos = start;
			ctx->err_end = end;
			ctx->had_error = true;
			snprintf(ctx->err_msg, sizeof(ctx->err_msg),
					 "invalid binary integer");
			return;

		case SCAN_TOK_TRAILING_JUNK_NUM:
			ctx->err_pos = start;
			ctx->err_end = end;
			ctx->had_error = true;
			snprintf(ctx->err_msg, sizeof(ctx->err_msg),
					 "trailing junk after numeric literal");
			return;

		case SCAN_TOK_FCONST_NUMERIC:
		case SCAN_TOK_FCONST_REAL:
			{
				char	   *s = palloc(len + 1);

				memcpy(s, text, len);
				s[len] = '\0';
				val.str = s;
				out_code = FCONST;
			}
			break;

		case SCAN_TOK_IDENT_RAW:
			{
				char	   *idbuf = palloc(len + 1);
				int			kwnum;

				memcpy(idbuf, text, len);
				idbuf[len] = '\0';
				kwnum = ScanKeywordLookup(idbuf, extra->keywordlist);
				if (kwnum >= 0)
				{
					val.keyword = GetScanKeyword(kwnum, extra->keywordlist);
					out_code = extra->keyword_tokens[kwnum];
					pfree(idbuf);
				}
				else if (pg_grammar_ext_keyword_hook != NULL)
				{
					/*
					 * Phase 4 Track B: extensions registered via
					 * parser_extension.h may have added keywords NOT present
					 * in the compile-time ScanKeywords table. If the base
					 * lookup missed, ask the extension registry whether this
					 * identifier matches one of those.  Returns -1 on miss;
					 * non-negative is the token code in the rebuilt parser.
					 * We cast the lowercased lexeme into val.keyword to keep
					 * the downstream contract (val.keyword points at a
					 * canonical lowercase form, not the raw input).
					 */
					char	   *lower = downcase_truncate_identifier(idbuf, (int) len, false);
					int			ext_code;

					ext_code = pg_grammar_ext_keyword_hook(lower);
					if (ext_code >= 0)
					{
						val.keyword = lower;
						out_code = ext_code;
						pfree(idbuf);
					}
					else
					{
						val.str = downcase_truncate_identifier(idbuf, (int) len, true);
						pfree(idbuf);
						pfree(lower);
						out_code = IDENT;
					}
				}
				else
				{
					val.str = downcase_truncate_identifier(idbuf, (int) len, true);
					pfree(idbuf);
					out_code = IDENT;
				}
			}
			break;

		case SCAN_TOK_IDENT_QUOTED:
			{
				/*
				 * text is the LEX_BUF_TAKE'd buffer; it's already a
				 * heap-allocated NUL-terminated string of length 'len'. We
				 * copy and pfree the input (the action body owns it).
				 */
				char	   *ident;

				if (len == 0)
				{
					ctx->err_pos = start;
					ctx->err_end = end;
					ctx->had_error = true;
					snprintf(ctx->err_msg, sizeof(ctx->err_msg),
							 "zero-length delimited identifier");
					return;
				}
				ident = palloc(len + 1);
				memcpy(ident, text, len);
				ident[len] = '\0';
				if ((int) len >= NAMEDATALEN)
					truncate_identifier(ident, (int) len, true);
				val.str = ident;
				out_code = IDENT;
			}
			break;

		case SCAN_TOK_NCHAR:
			{
				int			kwnum = ScanKeywordLookup("nchar", extra->keywordlist);

				if (kwnum >= 0)
				{
					val.keyword = GetScanKeyword(kwnum, extra->keywordlist);
					out_code = extra->keyword_tokens[kwnum];
				}
				else
				{
					val.str = pstrdup("n");
					out_code = IDENT;
				}
			}
			break;

		case SCAN_TOK_BCONST:
			{
				char	   *s = palloc(len + 1);

				memcpy(s, text, len);
				s[len] = '\0';
				val.str = s;
				out_code = BCONST;
			}
			break;
		case SCAN_TOK_XCONST:
			{
				char	   *s = palloc(len + 1);

				memcpy(s, text, len);
				s[len] = '\0';
				val.str = s;
				out_code = XCONST;
			}
			break;
		case SCAN_TOK_SCONST:
			{
				char	   *s = palloc(len + 1);

				memcpy(s, text, len);
				s[len] = '\0';
				if (extra->saw_non_ascii)
					pg_verifymbstr(s, (int) len, false);
				val.str = s;
				out_code = SCONST;
			}
			break;
		case SCAN_TOK_USCONST:
			{
				char	   *s = palloc(len + 1);

				memcpy(s, text, len);
				s[len] = '\0';
				val.str = s;
				out_code = USCONST;
			}
			break;
		case SCAN_TOK_UIDENT:
			{
				char	   *s;

				if (len == 0)
				{
					ctx->err_pos = start;
					ctx->err_end = end;
					ctx->had_error = true;
					snprintf(ctx->err_msg, sizeof(ctx->err_msg),
							 "zero-length delimited identifier");
					return;
				}
				s = palloc(len + 1);
				memcpy(s, text, len);
				s[len] = '\0';
				val.str = s;
				out_code = UIDENT;
			}
			break;

		case SCAN_TOK_BAD_UNICODE_ESCAPE:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
					 errmsg("invalid Unicode escape"),
					 errhint("Unicode escapes must be \\uXXXX or \\UXXXXXXXX."),
					 errposition(pg_mbstrlen_with_len(extra->scanbuf, start) + 1)));
			break;

		case SCAN_TOK_BAD_HEX_ESCAPE:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ESCAPE_SEQUENCE),
					 errmsg("invalid hexadecimal digit"),
					 errposition(pg_mbstrlen_with_len(extra->scanbuf, start) + 1)));
			break;

		case SCAN_TOK_BAD_SURROGATE:
			ctx->err_pos = start;
			ctx->err_end = end;
			ctx->had_error = true;
			snprintf(ctx->err_msg, sizeof(ctx->err_msg),
					 "invalid Unicode surrogate pair");
			return;

		default:
			/* Unknown sentinel; treat as literal char. */
			out_code = code;
			break;
	}

	push_token(ctx, out_code, start, end, val);
}

/* ------------------------------------------------------------------------- */
/* Public API                                                                */
/* ------------------------------------------------------------------------- */

core_yyscan_t
scanner_init(const char *str,
			 core_yy_extra_type *yyext,
			 const ScanKeywordList *keywordlist,
			 const uint16 *keyword_tokens)
{
	Size		slen = strlen(str);
	CoreScanner *s = palloc0_object(CoreScanner);
	ScanLexCtx *ctx;
	CoreLexer  *lex;
	CoreLexResult lex_status;

	s->extra = yyext;
	s->saved_char_set = false;

	yyext->keywordlist = keywordlist;
	yyext->keyword_tokens = keyword_tokens;
	yyext->backslash_quote = backslash_quote;

	yyext->scanbuf = (char *) palloc(slen + 2);
	yyext->scanbuflen = slen;
	memcpy(yyext->scanbuf, str, slen);
	yyext->scanbuf[slen] = yyext->scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;

	yyext->literalalloc = 1024;
	yyext->literalbuf = (char *) palloc(yyext->literalalloc);
	yyext->literallen = 0;

	yyext->dolqstart = NULL;
	yyext->utf16_first_part = 0;
	yyext->saw_non_ascii = false;
	yyext->state_before_str_stop = 0;
	yyext->xcdepth = 0;

	ctx = palloc0_object(ScanLexCtx);
	ctx->extra = yyext;
	ctx->scanbuf = yyext->scanbuf;
	ctx->scanbuflen = (int) slen;
	ctx->cur_loc = 0;
	ctx->litstart = 0;
	ctx->compound_pending = false;
	ctx->prev_state = 0;
	ctx->dolqstart = NULL;
	ctx->saw_non_ascii = false;
	ctx->utf16_first_part = 0;
	ctx->had_error = false;

	s->ctx = ctx;
	s->next = 0;
	s->cur_yylloc = NULL;

	lex = CoreLexAlloc(scan_palloc_wrapper);
	if (lex == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory in core lexer init")));

	/* Pre-scan the entire input. */
	lex_status = CoreLexFeedBytes(lex, yyext->scanbuf, slen,
								  scan_emit_cb, ctx);

	if (lex_status == CORE_LEX_OK)
		(void) CoreLexFeedEOF(lex, scan_emit_cb, ctx);

	/*
	 * Either kind of failure: reroute through scanner_yyerror.  Errors raised
	 * inside helpers (ereport ERROR) longjmp directly; here we pick up soft
	 * errors (ctx->had_error) and Lime-side LEX_ERROR_AT messages.
	 */
	if (ctx->had_error)
	{
		s->err_yylloc = ctx->err_pos;
		s->cur_yylloc = &s->err_yylloc;

		/*
		 * NUL-stuff at err_end so scanner_yyerror's printf truncates the "at
		 * or near" text to just the failing token's bytes.
		 */
		if (ctx->err_end > ctx->err_pos &&
			ctx->err_end <= s->extra->scanbuflen)
		{
			s->saved_pos = ctx->err_end;
			s->saved_char = s->extra->scanbuf[ctx->err_end];
			s->extra->scanbuf[ctx->err_end] = '\0';
			s->saved_char_set = true;
		}
		CoreLexFree(lex, scan_pfree_wrapper);
		scanner_yyerror(ctx->err_msg, (core_yyscan_t) s);
		/* unreachable */
	}
	if (lex_status != CORE_LEX_OK)
	{
		const char *m = CoreLexErrorMessage(lex);
		char	   *copy = pstrdup(m ? m : "syntax error");

		s->err_yylloc = ctx->cur_loc;
		s->cur_yylloc = &s->err_yylloc;
		CoreLexFree(lex, scan_pfree_wrapper);
		scanner_yyerror(copy, (core_yyscan_t) s);
		/* unreachable */
	}

	CoreLexFree(lex, scan_pfree_wrapper);

	return (core_yyscan_t) s;
}

void
scanner_finish(core_yyscan_t yyscanner)
{
	CoreScanner *s = (CoreScanner *) yyscanner;

	if (s->extra->scanbuflen >= 8192)
		pfree(s->extra->scanbuf);
	if (s->extra->literalalloc >= 8192)
		pfree(s->extra->literalbuf);

	if (s->ctx != NULL)
	{
		if (s->ctx->tokens != NULL)
			pfree(s->ctx->tokens);
		pfree(s->ctx);
	}
}

int
core_yylex(core_YYSTYPE *yylval_param, YYLTYPE *yylloc_param,
		   core_yyscan_t yyscanner)
{
	CoreScanner *s = (CoreScanner *) yyscanner;
	ScanLexCtx *ctx = s->ctx;
	ScanToken  *t;

	s->cur_yylloc = yylloc_param;

	/* Restore previously-stuffed byte. */
	if (s->saved_char_set)
	{
		s->extra->scanbuf[s->saved_pos] = s->saved_char;
		s->saved_char_set = false;
	}

	if (s->next >= ctx->ntokens)
	{
		*yylloc_param = (YYLTYPE) s->extra->scanbuflen;
		return 0;
	}

	t = &ctx->tokens[s->next++];
	*yylval_param = t->val;
	*yylloc_param = (YYLTYPE) t->start;

	/*
	 * For string-bearing tokens, copy val.str into CurrentMemoryContext so
	 * the parser stores a stable pointer in its parse tree (the original
	 * palloc happened during scanner_init, which may have run in a
	 * shorter-lived context than the parser's).  pstrdup is cheap and matches
	 * the lifetime semantics of the retired flex+scan.c, which palloc'd
	 * inside yylex itself (during the parse).
	 */
	switch (t->code)
	{
		case IDENT:
		case UIDENT:
		case SCONST:
		case USCONST:
		case BCONST:
		case XCONST:
		case FCONST:
			case OP:
			if			(t->val.str != NULL)
							yylval_param->str = pstrdup(t->val.str);

			break;
		default:
			break;
	}

	/*
	 * Stuff '\0' at the end of this token so base_yylex's lookahead +
	 * un-truncate dance, plus pl_scanner.c's strlen(scanbuf+lloc), see a
	 * NUL-terminated token.  Save the displaced byte to restore on the next
	 * entry to core_yylex.
	 */
	if (t->end >= 0 && t->end <= s->extra->scanbuflen)
	{
		s->saved_pos = t->end;
		s->saved_char = s->extra->scanbuf[t->end];
		s->extra->scanbuf[t->end] = '\0';
		s->saved_char_set = true;
	}

	return t->code;
}

int
scanner_errposition(int location, core_yyscan_t yyscanner)
{
	CoreScanner *s = (CoreScanner *) yyscanner;
	int			pos;

	if (location < 0)
		return 0;

	pos = pg_mbstrlen_with_len(s->extra->scanbuf, location) + 1;
	return errposition(pos);
}

static void
scb_error_callback(void *arg)
{
	ScannerCallbackState *scbstate = (ScannerCallbackState *) arg;

	if (geterrcode() != ERRCODE_QUERY_CANCELED)
		(void) scanner_errposition(scbstate->location, scbstate->yyscanner);
}

void
setup_scanner_errposition_callback(ScannerCallbackState *scbstate,
								   core_yyscan_t yyscanner,
								   int location)
{
	scbstate->yyscanner = yyscanner;
	scbstate->location = location;
	scbstate->errcallback.callback = scb_error_callback;
	scbstate->errcallback.arg = scbstate;
	scbstate->errcallback.previous = error_context_stack;
	error_context_stack = &scbstate->errcallback;
}

void
cancel_scanner_errposition_callback(ScannerCallbackState *scbstate)
{
	error_context_stack = scbstate->errcallback.previous;
}

void
scanner_yyerror(const char *message, core_yyscan_t yyscanner)
{
	CoreScanner *s = (CoreScanner *) yyscanner;
	YYLTYPE		loc_value = s->cur_yylloc ? *s->cur_yylloc : 0;
	const char *loc;

	if (loc_value < 0 || loc_value > s->extra->scanbuflen)
		loc_value = 0;
	loc = s->extra->scanbuf + loc_value;

	if (*loc == YY_END_OF_BUFFER_CHAR)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
		/* translator: %s is typically the translation of "syntax error" */
				 errmsg("%s at end of input", _(message)),
				 scanner_errposition(loc_value, yyscanner)));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
		/* translator: first %s is typically the translation of "syntax error" */
				 errmsg("%s at or near \"%s\"", _(message), loc),
				 scanner_errposition(loc_value, yyscanner)));
	}
}
