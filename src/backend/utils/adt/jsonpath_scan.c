/*-------------------------------------------------------------------------
 *
 * jsonpath_scan.c
 *	Parser+lexer driver for the jsonpath datatype.
 *
 * Lime v0.2.2's lexer subsystem (compiled from jsonpath_scan.lex)
 * replaces the hand-rolled state machine that used to live here.
 * What remains:
 *
 *	- The literal accumulator helpers (jp_lex_buf_init, jp_lex_addchar,
 *	  jp_lex_addstring, jp_lex_check_keyword, jp_lex_set_yytext, etc.)
 *	  consumed by the .lex action bodies.
 *	- parseUnicode / parseHexChar / hexval / addUnicode / addUnicodeChar
 *	  copied verbatim from the pre-port scanner so the .lex's
 *	  jp_lex_parse_unicode helper can drive them.
 *	- The keyword table and checkKeyword lookup, called from the
 *	  driver's emit callback when an unquoted identifier reaches
 *	  end-of-token.
 *	- The parser driver (jsonpath_yyparse) that wraps
 *	  JsonPathLexFeedBytes once over the input and feeds emitted
 *	  tokens into the Lime parser via jsonpath_yy().
 *	- jsonpath_yyerror / jsonpath_yyerror_token, identical-shaped
 *	  public error helpers (called by the grammar's %syntax_error
 *	  block and by the lexer helpers).
 *	- The JsonPathParseItem constructors used by grammar actions.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	src/backend/utils/adt/jsonpath_scan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_collation.h"
#include "fmgr.h"
#include "jsonpath_internal.h"
#include "jsonpath_scan_lex.h"	/* JsonPathLexer, JsonPathLexAlloc, ... */
#include "jsonpath_scan_lex_internal.h" /* JP_TOK_*, JsonPathScanCtx */
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/miscnodes.h"
#include "nodes/pg_list.h"
#include "regex/regex.h"
#include "utils/builtins.h"

/* ------------------------------------------------------------------------- */
/* Forward decls                                                              */
/* ------------------------------------------------------------------------- */

static void resizeString(bool init, int appendLen, JsonPathYyScanner *s);
static void addstring_internal(bool init, const char *src, int l,
							   JsonPathYyScanner *s);
static void addchar_internal(bool init, char c, JsonPathYyScanner *s);

static bool parseUnicode(const char *str, int l, struct Node *escontext,
						 JsonPathYyScanner *s);
static bool parseHexChar(const char *str, struct Node *escontext,
						 JsonPathYyScanner *s);
static bool hexval(char c, int *result, struct Node *escontext,
				   JsonPathYyScanner *s);
static bool addUnicodeChar(char32_t ch, struct Node *escontext,
						   JsonPathYyScanner *s);
static bool addUnicode(char32_t ch, int *hi_surrogate,
					   struct Node *escontext, JsonPathYyScanner *s);

/* ------------------------------------------------------------------------- */
/*
 * Keyword table (sorted by length, then alphabetical; matches the pre-port
 * checkKeyword() byte-for-byte).
 * -------------------------------------------------------------------------
 */

typedef struct JsonPathKeyword
{
	int16		len;
	bool		lowercase;
	int			val;
	const char *keyword;
} JsonPathKeyword;

static const JsonPathKeyword keywords[] = {
	{2, false, IS_P, "is"},
	{2, false, TO_P, "to"},
	{3, false, ABS_P, "abs"},
	{3, false, LAX_P, "lax"},
	{4, false, DATE_P, "date"},
	{4, false, FLAG_P, "flag"},
	{4, false, LAST_P, "last"},
	{4, true, NULL_P, "null"},
	{4, false, SIZE_P, "size"},
	{4, false, TIME_P, "time"},
	{4, true, TRUE_P, "true"},
	{4, false, TYPE_P, "type"},
	{4, false, WITH_P, "with"},
	{5, false, STR_BTRIM_P, "btrim"},
	{5, true, FALSE_P, "false"},
	{5, false, FLOOR_P, "floor"},
	{5, false, STR_LOWER_P, "lower"},
	{5, false, STR_LTRIM_P, "ltrim"},
	{5, false, STR_RTRIM_P, "rtrim"},
	{5, false, STR_UPPER_P, "upper"},
	{6, false, BIGINT_P, "bigint"},
	{6, false, DOUBLE_P, "double"},
	{6, false, EXISTS_P, "exists"},
	{6, false, NUMBER_P, "number"},
	{6, false, STARTS_P, "starts"},
	{6, false, STRICT_P, "strict"},
	{6, false, STRINGFUNC_P, "string"},
	{7, false, BOOLEAN_P, "boolean"},
	{7, false, CEILING_P, "ceiling"},
	{7, false, DECIMAL_P, "decimal"},
	{7, false, STR_INITCAP_P, "initcap"},
	{7, false, INTEGER_P, "integer"},
	{7, false, STR_REPLACE_P, "replace"},
	{7, false, TIME_TZ_P, "time_tz"},
	{7, false, UNKNOWN_P, "unknown"},
	{8, false, DATETIME_P, "datetime"},
	{8, false, KEYVALUE_P, "keyvalue"},
	{9, false, TIMESTAMP_P, "timestamp"},
	{10, false, LIKE_REGEX_P, "like_regex"},
	{10, false, STR_SPLIT_PART_P, "split_part"},
	{12, false, TIMESTAMP_TZ_P, "timestamp_tz"},
};

static int
checkKeyword_for(const JsonPathString *scanstring)
{
	int			res = IDENT_P;
	const JsonPathKeyword *StopLow = keywords;
	const JsonPathKeyword *StopHigh = keywords + lengthof(keywords);
	const JsonPathKeyword *StopMiddle;
	int			diff;

	if (scanstring->len > keywords[lengthof(keywords) - 1].len)
		return res;

	while (StopLow < StopHigh)
	{
		StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);

		if (StopMiddle->len == scanstring->len)
			diff = pg_strncasecmp(StopMiddle->keyword, scanstring->val,
								  scanstring->len);
		else
			diff = StopMiddle->len - scanstring->len;

		if (diff < 0)
			StopLow = StopMiddle + 1;
		else if (diff > 0)
			StopHigh = StopMiddle;
		else
		{
			if (StopMiddle->lowercase)
				diff = strncmp(StopMiddle->keyword, scanstring->val,
							   scanstring->len);
			if (diff == 0)
				res = StopMiddle->val;
			break;
		}
	}
	return res;
}

/* ------------------------------------------------------------------------- */
/*
 * StringInfo-style buffer management for the per-token scanstring.
 *
 * Identical semantics to the pre-port scanner: addchar(init=true, ...) and
 * addstring(init=true, ...) allocate a new buffer (so the previously-
 * emitted token's str pointer remains valid).  Subsequent init=false calls
 * grow the current buffer.
 * -------------------------------------------------------------------------
 */

static void
resizeString(bool init, int appendLen, JsonPathYyScanner *s)
{
	if (init)
	{
		s->scanstring.total = Max(32, appendLen);
		s->scanstring.val = (char *) palloc(s->scanstring.total);
		s->scanstring.len = 0;
	}
	else
	{
		if (s->scanstring.len + appendLen >= s->scanstring.total)
		{
			while (s->scanstring.len + appendLen >= s->scanstring.total)
				s->scanstring.total *= 2;
			s->scanstring.val = repalloc(s->scanstring.val, s->scanstring.total);
		}
	}
}

static void
addstring_internal(bool init, const char *src, int l, JsonPathYyScanner *s)
{
	resizeString(init, l + 1, s);
	memcpy(s->scanstring.val + s->scanstring.len, src, l);
	s->scanstring.len += l;
}

static void
addchar_internal(bool init, char c, JsonPathYyScanner *s)
{
	resizeString(init, 1, s);
	s->scanstring.val[s->scanstring.len] = c;
	if (c != '\0')
		s->scanstring.len++;
}

/* ------------------------------------------------------------------------- */
/* yytext snapshot (for error messages).                                     */
/* ------------------------------------------------------------------------- */

static void
set_yytext(JsonPathYyScanner *s, const char *p, int len)
{
	int			n = (len > (int) sizeof(s->yytext) - 1)
		? (int) sizeof(s->yytext) - 1 : len;

	memcpy(s->yytext, p, n);
	s->yytext[n] = '\0';
	s->yytext_len = n;
}

static void
set_yytext_from_scanstring(JsonPathYyScanner *s)
{
	if (s->scanstring.val == NULL || s->scanstring.len == 0)
	{
		s->yytext[0] = '\0';
		s->yytext_len = 0;
		return;
	}
	set_yytext(s, s->scanstring.val, s->scanstring.len);
}

/* ------------------------------------------------------------------------- */
/* Public helpers consumed by jsonpath_scan.lex's action bodies.             */
/* ------------------------------------------------------------------------- */

void
jp_lex_buf_init(void *user)
{
	JsonPathYyScanner *s = JP_SCANNER(user);

	addchar_internal(true, '\0', s);
}

void
jp_lex_addchar(void *user, char c)
{
	JsonPathYyScanner *s = JP_SCANNER(user);

	addchar_internal(false, c, s);
}

void
jp_lex_addstring(void *user, const char *src, size_t len)
{
	JsonPathYyScanner *s = JP_SCANNER(user);

	addstring_internal(false, src, (int) len, s);
}

void
jp_lex_set_yytext(void *user, const char *p, size_t len)
{
	set_yytext(JP_SCANNER(user), p, (int) len);
}

void
jp_lex_set_yytext_empty(void *user)
{
	JsonPathYyScanner *s = JP_SCANNER(user);

	s->yytext[0] = '\0';
	s->yytext_len = 0;
}

bool
jp_lex_parse_unicode(void *user, const char *text, size_t len)
{
	return parseUnicode(text, (int) len, JP_ESCONTEXT(user), JP_SCANNER(user));
}

bool
jp_lex_parse_hex_char(void *user, const char *text)
{
	return parseHexChar(text, JP_ESCONTEXT(user), JP_SCANNER(user));
}

int
jp_lex_check_keyword(void *user)
{
	JsonPathYyScanner *s = JP_SCANNER(user);

	addchar_internal(false, '\0', s);
	set_yytext_from_scanstring(s);
	return checkKeyword_for(&s->scanstring);
}

/* ------------------------------------------------------------------------- */
/* Unicode / hex char handling (verbatim from the pre-port scanner).         */
/* ------------------------------------------------------------------------- */

static bool
hexval(char c, int *result, struct Node *escontext, JsonPathYyScanner *s)
{
	if (c >= '0' && c <= '9')
	{
		*result = c - '0';
		return true;
	}
	if (c >= 'a' && c <= 'f')
	{
		*result = c - 'a' + 0xA;
		return true;
	}
	if (c >= 'A' && c <= 'F')
	{
		*result = c - 'A' + 0xA;
		return true;
	}
	jsonpath_yyerror(NULL, escontext, (yyscan_t) s, "invalid hexadecimal digit");
	return false;
}

static bool
addUnicodeChar(char32_t ch, struct Node *escontext, JsonPathYyScanner *s)
{
	if (ch == 0)
	{
		ereturn(escontext, false,
				(errcode(ERRCODE_UNTRANSLATABLE_CHARACTER),
				 errmsg("unsupported Unicode escape sequence"),
				 errdetail("\\u0000 cannot be converted to text.")));
	}
	else
	{
		char		cbuf[MAX_UNICODE_EQUIVALENT_STRING + 1];

		if (!escontext || !IsA(escontext, ErrorSaveContext))
			pg_unicode_to_server(ch, (unsigned char *) cbuf);
		else if (!pg_unicode_to_server_noerror(ch, (unsigned char *) cbuf))
			ereturn(escontext, false,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("could not convert Unicode to server encoding")));
		addstring_internal(false, cbuf, strlen(cbuf), s);
	}
	return true;
}

static bool
addUnicode(char32_t ch, int *hi_surrogate, struct Node *escontext,
		   JsonPathYyScanner *s)
{
	if (is_utf16_surrogate_first(ch))
	{
		if (*hi_surrogate != -1)
			ereturn(escontext, false,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "jsonpath"),
					 errdetail("Unicode high surrogate must not follow "
							   "a high surrogate.")));
		*hi_surrogate = ch;
		return true;
	}
	else if (is_utf16_surrogate_second(ch))
	{
		if (*hi_surrogate == -1)
			ereturn(escontext, false,
					(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
					 errmsg("invalid input syntax for type %s", "jsonpath"),
					 errdetail("Unicode low surrogate must follow a high "
							   "surrogate.")));
		ch = surrogate_pair_to_codepoint(*hi_surrogate, ch);
		*hi_surrogate = -1;
	}
	else if (*hi_surrogate != -1)
	{
		ereturn(escontext, false,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s", "jsonpath"),
				 errdetail("Unicode low surrogate must follow a high "
						   "surrogate.")));
	}

	return addUnicodeChar(ch, escontext, s);
}

static bool
parseUnicode(const char *str, int l, struct Node *escontext,
			 JsonPathYyScanner *s)
{
	int			i = 2;
	int			hi_surrogate = -1;

	for (i = 2; i < l; i += 2)	/* skip leading '\u' */
	{
		char32_t	ch = 0;
		int			j;
		int			si;

		if (str[i] == '{')
		{
			while (str[++i] != '}' && i < l)
			{
				if (!hexval(str[i], &si, escontext, s))
					return false;
				ch = (ch << 4) | si;
			}
			i++;				/* skip '}' */
		}
		else
		{
			for (j = 0; j < 4 && i < l; j++)
			{
				if (!hexval(str[i++], &si, escontext, s))
					return false;
				ch = (ch << 4) | si;
			}
		}

		if (!addUnicode(ch, &hi_surrogate, escontext, s))
			return false;
	}

	if (hi_surrogate != -1)
	{
		ereturn(escontext, false,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s", "jsonpath"),
				 errdetail("Unicode low surrogate must follow a high "
						   "surrogate.")));
	}
	return true;
}

static bool
parseHexChar(const char *str, struct Node *escontext, JsonPathYyScanner *s)
{
	int			s2;
	int			s3;
	int			ch;

	if (!hexval(str[2], &s2, escontext, s))
		return false;
	if (!hexval(str[3], &s3, escontext, s))
		return false;
	ch = (s2 << 4) | s3;
	return addUnicodeChar(ch, escontext, s);
}

/* ------------------------------------------------------------------------- */
/* yyerror entry points.                                                     */
/* ------------------------------------------------------------------------- */

static const char *
last_matched_text(JsonPathYyScanner *s)
{
	if (s == NULL || s->yytext_len == 0)
		return "";
	return s->yytext;
}

void
jsonpath_yyerror(JsonPathParseResult **result, struct Node *escontext,
				 yyscan_t yyscanner, const char *message)
{
	JsonPathYyScanner *s = (JsonPathYyScanner *) yyscanner;
	const char *yytext = last_matched_text(s);

	if (SOFT_ERROR_OCCURRED(escontext))
		return;

	if (yytext == NULL || yytext[0] == '\0')
		errsave(escontext,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s at end of jsonpath input", _(message))));
	else
		errsave(escontext,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s at or near \"%s\" of jsonpath input",
						_(message), yytext)));
}

void
jsonpath_yyerror_token(jsonpath_yy_extra *extra, int yymajor,
					   const char *message)
{
	JsonPathYyScanner *s = (JsonPathYyScanner *) extra->scanner;
	struct Node *escontext = extra->escontext;
	const char *yytext = last_matched_text(s);

	if (SOFT_ERROR_OCCURRED(escontext))
		return;

	if (yymajor == 0)
		errsave(escontext,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s at end of jsonpath input", _(message))));
	else
		errsave(escontext,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s at or near \"%s\" of jsonpath input",
						_(message),
						(yytext && yytext[0]) ? yytext : "?")));
}

/* ------------------------------------------------------------------------- */
/* Allocator wrappers.                                                       */
/* ------------------------------------------------------------------------- */

static void *
jp_palloc_wrapper(size_t n)
{
	return palloc(n);
}

static void
jp_pfree_wrapper(void *p)
{
	if (p != NULL)
		pfree(p);
}

/* ------------------------------------------------------------------------- */
/* Emit callback                                                             */
/*                                                                           */
/* Translates (sentinel-or-parser-token, text, len) -> (parser token, val)   */
/* and feeds the parser via jsonpath_yy().  Aborts further emits if the      */
/* parser set extra->aborted (the YYABORT-equivalent for LIKE_REGEX).        */
/* ------------------------------------------------------------------------- */

typedef struct JpEmitContext
{
	JsonPathScanCtx ctx;		/* must be first for JP_CTX cast */
	void	   *parser;
	jsonpath_yy_extra *extra;
} JpEmitContext;

static void
jp_emit_cb(void *user, int code, const char *text, size_t len)
{
	JpEmitContext *ec = (JpEmitContext *) user;
	JsonPathYyScanner *s = ec->ctx.s;
	YYSTYPE		val;
	int			out_code = code;

	memset(&val, 0, sizeof(val));

	if (ec->extra->aborted || SOFT_ERROR_OCCURRED(ec->extra->escontext))
		return;

	switch (code)
	{
		case JP_TOK_RAW_CHAR:
			{
				/* Map a single special-char byte to the named token. */
				unsigned char c = (unsigned char) text[0];

				switch (c)
				{
					case '$':
						out_code = DOLLAR;
						break;
					case '@':
						out_code = AT;
						break;
					case '.':
						out_code = DOT;
						break;
					case ',':
						out_code = COMMA;
						break;
					case '?':
						out_code = QUESTION;
						break;
					case ':':
						out_code = COLON;
						break;
					case '[':
						out_code = LBRACKET;
						break;
					case ']':
						out_code = RBRACKET;
						break;
					case '{':
						out_code = LBRACE;
						break;
					case '}':
						out_code = RBRACE;
						break;
					case '(':
						out_code = LPAREN;
						break;
					case ')':
						out_code = RPAREN;
						break;
					case '+':
						out_code = PLUS;
						break;
					case '-':
						out_code = MINUS;
						break;
					case '*':
						out_code = STAR;
						break;
					case '/':
						out_code = SLASH;
						break;
					case '%':
						out_code = PERCENT;
						break;
					case '<':
						out_code = LESS_P;
						break;
					case '>':
						out_code = GREATER_P;
						break;
					case '=':
						out_code = EQUAL_P;
						break;
					case '!':
						out_code = NOT_P;
						break;
					case '|':
						out_code = OR_P;
						break;
					case '&':
						out_code = AND_P;
						break;
					case '#':
					default:

						/*
						 * '#' has no parser token (the pre-port scanner
						 * returned 0 for '#').  Setting yytext lets the
						 * caller see the offending byte if a syntax error
						 * follows; we simply skip emission here.
						 */
						set_yytext(s, text, (int) len);
						return;
				}
				set_yytext(s, text, (int) len);
			}
			break;

		case JP_TOK_VARIABLE_BARE:
			{
				/*
				 * matched is "$other+"; payload is bytes [1..len). Mirror the
				 * pre-port scanner: addstring(true, +1, len-1),
				 * addchar(false, '\0').
				 */
				addstring_internal(true, text +1, (int) len - 1, s);
				addchar_internal(false, '\0', s);
				val.str = s->scanstring;
				set_yytext(s, text, (int) len);
				out_code = VARIABLE_P;
			}
			break;

		case JP_TOK_STRING_TAKE:
			val.str = s->scanstring;
			set_yytext(s, text, (int) len); /* yytext = closing `"` */
			out_code = STRING_P;
			break;

		case JP_TOK_VARIABLE_TAKE:
			val.str = s->scanstring;
			set_yytext(s, text, (int) len); /* yytext = closing `"` */
			out_code = VARIABLE_P;
			break;

		case JP_TOK_NUMERIC_TEXT:
			addstring_internal(true, text, (int) len, s);
			addchar_internal(false, '\0', s);
			val.str = s->scanstring;
			set_yytext(s, text, (int) len);
			out_code = NUMERIC_P;
			break;

		case JP_TOK_INT_TEXT:
			addstring_internal(true, text, (int) len, s);
			addchar_internal(false, '\0', s);
			val.str = s->scanstring;
			set_yytext(s, text, (int) len);
			out_code = INT_P;
			break;

		default:
			if (code >= JP_TOK_BASE)
			{
				/* Unknown sentinel; suppress. */
				return;
			}

			/*
			 * Otherwise it is a parser-side token code emitted directly (a
			 * keyword token from the xnq path, or one of AND_P / OR_P / NOT_P
			 * / ANY_P / LESSEQUAL_P / EQUAL_P / NOTEQUAL_P / GREATEREQUAL_P /
			 * LESS_P / GREATER_P).
			 *
			 * For tokens emitted from xnq paths (xnq_blank, xnq_xc,
			 * xnq_break, xnq_eof) the .lex's action body called
			 * jp_lex_check_keyword(), which terminated the accumulator with
			 * NUL and snapshotted yytext.  val.str carries the accumulated
			 * identifier.
			 *
			 * For the multi-char operator tokens emitted via LEX_EMIT
			 * directly (AND_P from "&&" etc.) the grammar does not read S.str
			 * so val.str's contents do not matter.
			 */
			val.str = s->scanstring;
			break;
	}

	jsonpath_yy(ec->parser, out_code, val, ec->extra);
}

/* ------------------------------------------------------------------------- */
/* Parser driver.                                                             */
/* ------------------------------------------------------------------------- */

int
jsonpath_yyparse(JsonPathParseResult **result, struct Node *escontext,
				 yyscan_t yyscanner)
{
	JsonPathYyScanner *s = (JsonPathYyScanner *) yyscanner;
	jsonpath_yy_extra extra;
	JpEmitContext ec;
	JsonPathLexer *lex;
	JsonPathLexResult lex_status;
	YYSTYPE		eof_val;

	extra.result = result;
	extra.escontext = escontext;
	extra.scanner = yyscanner;
	extra.aborted = false;

	ec.ctx.s = s;
	ec.ctx.escontext = escontext;
	ec.parser = jsonpath_yyAlloc(jp_palloc_wrapper);
	ec.extra = &extra;

	lex = JsonPathLexAlloc(jp_palloc_wrapper);
	if (lex == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory in jsonpath lexer init")));

	lex_status = JsonPathLexFeedBytes(lex, s->input, s->len,
									  jp_emit_cb, &ec);

	if (lex_status == JSONPATH_LEX_OK &&
		!extra.aborted && !SOFT_ERROR_OCCURRED(escontext))
		(void) JsonPathLexFeedEOF(lex, jp_emit_cb, &ec);

	if (lex_status != JSONPATH_LEX_OK &&
		!SOFT_ERROR_OCCURRED(escontext))
	{
		const char *m = JsonPathLexErrorMessage(lex);

		jsonpath_yyerror(NULL, escontext, yyscanner,
						 m ? m : "syntax error");
	}

	/* Push EOF unless we already errored or aborted. */
	if (!extra.aborted && !SOFT_ERROR_OCCURRED(escontext))
	{
		s->yytext[0] = '\0';
		s->yytext_len = 0;
		memset(&eof_val, 0, sizeof(eof_val));
		jsonpath_yy(ec.parser, 0, eof_val, &extra);
	}

	jsonpath_yyFree(ec.parser, jp_pfree_wrapper);
	JsonPathLexFree(lex, jp_pfree_wrapper);

	return SOFT_ERROR_OCCURRED(escontext) ? 1 : 0;
}

/* ------------------------------------------------------------------------- */
/* parsejsonpath: public entry point used by jsonpath.c                       */
/* ------------------------------------------------------------------------- */

JsonPathParseResult *
parsejsonpath(const char *str, int len, struct Node *escontext)
{
	JsonPathParseResult *parseresult = NULL;
	JsonPathYyScanner *s;

	if (len <= 0)
		len = strlen(str);

	s = palloc0_object(JsonPathYyScanner);
	s->input = str;
	s->pos = 0;
	s->len = len;
	s->scanstring.val = NULL;
	s->scanstring.len = 0;
	s->scanstring.total = 0;
	s->hi_surrogate = -1;

	if (jsonpath_yyparse(&parseresult, escontext, (yyscan_t) s) != 0 &&
		!SOFT_ERROR_OCCURRED(escontext))
		jsonpath_yyerror(NULL, escontext, (yyscan_t) s, "invalid input");

	return parseresult;
}

/* ------------------------------------------------------------------------- */
/* JsonPathParseItem constructors (verbatim from the pre-port scanner).      */
/* ------------------------------------------------------------------------- */

JsonPathParseItem *
jpMakeItemType(JsonPathItemType type)
{
	JsonPathParseItem *v = palloc_object(JsonPathParseItem);

	CHECK_FOR_INTERRUPTS();

	v->type = type;
	v->next = NULL;
	return v;
}

JsonPathParseItem *
jpMakeItemString(JsonPathString *s)
{
	JsonPathParseItem *v;

	if (s == NULL)
		v = jpMakeItemType(jpiNull);
	else
	{
		v = jpMakeItemType(jpiString);
		v->value.string.val = s->val;
		v->value.string.len = s->len;
	}
	return v;
}

JsonPathParseItem *
jpMakeItemVariable(JsonPathString *s)
{
	JsonPathParseItem *v = jpMakeItemType(jpiVariable);

	v->value.string.val = s->val;
	v->value.string.len = s->len;
	return v;
}

JsonPathParseItem *
jpMakeItemKey(JsonPathString *s)
{
	JsonPathParseItem *v = jpMakeItemString(s);

	v->type = jpiKey;
	return v;
}

JsonPathParseItem *
jpMakeItemNumeric(JsonPathString *s)
{
	JsonPathParseItem *v = jpMakeItemType(jpiNumeric);

	v->value.numeric =
		DatumGetNumeric(DirectFunctionCall3(numeric_in,
											CStringGetDatum(s->val),
											ObjectIdGetDatum(InvalidOid),
											Int32GetDatum(-1)));
	return v;
}

JsonPathParseItem *
jpMakeItemBool(bool val)
{
	JsonPathParseItem *v = jpMakeItemType(jpiBool);

	v->value.boolean = val;
	return v;
}

JsonPathParseItem *
jpMakeItemBinary(JsonPathItemType type, JsonPathParseItem *la,
				 JsonPathParseItem *ra)
{
	JsonPathParseItem *v = jpMakeItemType(type);

	v->value.args.left = la;
	v->value.args.right = ra;
	return v;
}

JsonPathParseItem *
jpMakeItemUnary(JsonPathItemType type, JsonPathParseItem *a)
{
	JsonPathParseItem *v;

	if (type == jpiPlus && a->type == jpiNumeric && !a->next)
		return a;

	if (type == jpiMinus && a->type == jpiNumeric && !a->next)
	{
		v = jpMakeItemType(jpiNumeric);
		v->value.numeric =
			DatumGetNumeric(DirectFunctionCall1(numeric_uminus,
												NumericGetDatum(a->value.numeric)));
		return v;
	}

	v = jpMakeItemType(type);
	v->value.arg = a;
	return v;
}

JsonPathParseItem *
jpMakeItemList(List *list)
{
	JsonPathParseItem *head;
	JsonPathParseItem *end;
	ListCell   *cell;

	head = end = (JsonPathParseItem *) linitial(list);

	if (list_length(list) == 1)
		return head;

	while (end->next)
		end = end->next;

	for_each_from(cell, list, 1)
	{
		JsonPathParseItem *c = (JsonPathParseItem *) lfirst(cell);

		end->next = c;
		end = c;
	}
	return head;
}

JsonPathParseItem *
jpMakeIndexArray(List *list)
{
	JsonPathParseItem *v = jpMakeItemType(jpiIndexArray);
	ListCell   *cell;
	int			i = 0;

	Assert(list_length(list) > 0);
	v->value.array.nelems = list_length(list);
	v->value.array.elems = palloc(sizeof(v->value.array.elems[0]) *
								  v->value.array.nelems);
	foreach(cell, list)
	{
		JsonPathParseItem *jpi = lfirst(cell);

		Assert(jpi->type == jpiSubscript);
		v->value.array.elems[i].from = jpi->value.args.left;
		v->value.array.elems[i++].to = jpi->value.args.right;
	}
	return v;
}

JsonPathParseItem *
jpMakeAny(int first, int last)
{
	JsonPathParseItem *v = jpMakeItemType(jpiAny);

	v->value.anybounds.first = (first >= 0) ? first : PG_UINT32_MAX;
	v->value.anybounds.last = (last >= 0) ? last : PG_UINT32_MAX;
	return v;
}

bool
jpMakeItemLikeRegex(JsonPathParseItem *expr, JsonPathString *pattern,
					JsonPathString *flags, JsonPathParseItem **result,
					struct Node *escontext)
{
	JsonPathParseItem *v = jpMakeItemType(jpiLikeRegex);
	int			i;
	int			cflags;

	v->value.like_regex.expr = expr;
	v->value.like_regex.pattern = pattern->val;
	v->value.like_regex.patternlen = pattern->len;

	v->value.like_regex.flags = 0;
	for (i = 0; flags && i < flags->len; i++)
	{
		switch (flags->val[i])
		{
			case 'i':
				v->value.like_regex.flags |= JSP_REGEX_ICASE;
				break;
			case 's':
				v->value.like_regex.flags |= JSP_REGEX_DOTALL;
				break;
			case 'm':
				v->value.like_regex.flags |= JSP_REGEX_MLINE;
				break;
			case 'x':
				v->value.like_regex.flags |= JSP_REGEX_WSPACE;
				break;
			case 'q':
				v->value.like_regex.flags |= JSP_REGEX_QUOTE;
				break;
			default:
				ereturn(escontext, false,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid input syntax for type %s", "jsonpath"),
						 errdetail("Unrecognized flag character \"%.*s\" in LIKE_REGEX predicate.",
								   pg_mblen_range(flags->val + i,
												  flags->val + flags->len),
								   flags->val + i)));
				break;
		}
	}

	if (!jspConvertRegexFlags(v->value.like_regex.flags, &cflags, escontext))
		return false;

	{
		regex_t		re_tmp;
		pg_wchar   *wpattern;
		int			wpattern_len;
		int			re_result;

		wpattern = (pg_wchar *) palloc((pattern->len + 1) * sizeof(pg_wchar));
		wpattern_len = pg_mb2wchar_with_len(pattern->val,
											wpattern,
											pattern->len);

		if ((re_result = pg_regcomp(&re_tmp, wpattern, wpattern_len, cflags,
									DEFAULT_COLLATION_OID)) != REG_OKAY)
		{
			char		errMsg[100];

			pg_regerror(re_result, &re_tmp, errMsg, sizeof(errMsg));
			ereturn(escontext, false,
					(errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
					 errmsg("invalid regular expression: %s", errMsg)));
		}

		pg_regfree(&re_tmp);
	}

	*result = v;

	return true;
}

bool
jspConvertRegexFlags(uint32 xflags, int *result, struct Node *escontext)
{
	int			cflags = REG_ADVANCED;

	if (xflags & JSP_REGEX_ICASE)
		cflags |= REG_ICASE;

	if (xflags & JSP_REGEX_QUOTE)
	{
		cflags &= ~REG_ADVANCED;
		cflags |= REG_QUOTE;
	}
	else
	{
		if (!(xflags & JSP_REGEX_DOTALL))
			cflags |= REG_NLSTOP;
		if (xflags & JSP_REGEX_MLINE)
			cflags |= REG_NLANCH;

		if (xflags & JSP_REGEX_WSPACE)
			ereturn(escontext, false,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("XQuery \"x\" flag (expanded regular expressions) is not implemented")));
	}

	*result = cflags;

	return true;
}
