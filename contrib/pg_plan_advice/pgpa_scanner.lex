/*-------------------------------------------------------------------------
 *
 * pgpa_scanner.lex
 *	  Lime lexer for plan advice.
 *
 * Replaces contrib/pg_plan_advice/pgpa_scanner.l (~290 lines flex)
 * with a declarative .lex source compiled by Lime v0.2.2.  The
 * accompanying driver in pgpa_scan.c is replaced by a parser-driver
 * shim that pre-scans the input into a token FIFO.
 *
 * Three exclusive states (mirroring the flex source): INITIAL,
 * xc (C-style comment), xd (double-quoted identifier with "" escape).
 *
 * Portions Copyright (c) 2000-2026, PostgreSQL Global Development Group
 *
 * contrib/pg_plan_advice/pgpa_scanner.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Pgpa.

%include {
#include "postgres.h"

#include "common/string.h"
#include "nodes/miscnodes.h"
#include "parser/scansup.h"

#include "pgpa_ast.h"
#include "pgpa_parser_yytype.h"
#include "pgpa_parser.h"		/* TOK_IDENT, TOK_INTEGER, TOK_TAG_* */
}

/* Accumulator for double-quoted identifiers (xd state). */
%literal_buffer scanid {
    type      char
    initial   64
    grow      "*2"
    alloc     palloc
    realloc   repalloc
    free      pfree
}.

%exclusive_state XC.
%exclusive_state XD.

/* ---- Pattern fragments ---- */
%pattern ident_start /[A-Za-z_\x80-\xff]/.
%pattern ident_cont  /[A-Za-z_0-9$\x80-\xff]/.
%pattern decdigit    /[0-9]/.

/* ===== Whitespace ===== */
rule ws matches /[ \t\n\r\f\v]+/ { LEX_SKIP(); }

/* ===== C-style comment open ===== */
rule xc_open matches /\/\*/ {
    LEX_TRANSITION(PGPA_STATE_XC);
    LEX_SKIP();
}

/* ===== Double-quoted identifier open ===== */
rule xd_open matches /"/ {
    LEX_BUF_START(scanid);
    LEX_TRANSITION(PGPA_STATE_XD);
    LEX_SKIP();
}

/* ===== Identifier =====
**
** The driver classifies the matched span via pgpa_parse_advice_tag
** and emits the appropriate TOK_* code.  We pass through with
** TOK_IDENT here; the driver re-maps as needed.  Use a sentinel
** above the parser's range to distinguish unquoted-identifier
** matches from quoted ones (which need pstrdup but no
** downcase_identifier).
*/
rule ident matches /{ident_start}{ident_cont}*/  { LEX_EMIT(TOK_IDENT); }

/* ===== Integer literal =====
**
** Decimal digits with optional underscore separators.  Driver runs
** strtoint; range overflow becomes a parser error.
*/
rule integer matches /{decdigit}(_?{decdigit})*/  { LEX_EMIT(TOK_INTEGER); }

/* ===== Trailing junk after a numeric literal =====
**
** e.g. "12abc": a decinteger immediately followed by identifier chars.
** Lime's longest-match arbitration prefers this over `integer` + `ident`,
** matching upstream flex's {integer_junk} rule (Daniel Gustafsson,
** "Fix parsing of underscores in pg_plan_advice occurrence numbers").
*/
rule integer_junk matches /{decdigit}(_?{decdigit})*{ident_start}{ident_cont}*/ {
    /*
     * Record the offending span as yytext (via a benign TOK_IDENT emit) so
     * the driver's error report can say "...at or near \"12abc\"", then raise
     * the lexer error.  The pushed token is never consumed -- the error
     * aborts the parse.
     */
    if (emit) emit(user, TOK_IDENT, matched, matched_len);
    LEX_ERROR_AT("trailing junk after numeric literal");
}

/* ===== Single-character punctuation =====
**
** flex's catch-all `.` rule returned yytext[0] (the raw character
** byte).  The converter mapped the parser-side single-char usages
** to named tokens (LPAREN, RPAREN, COMMA, DOT, HASH, SLASH,
** AT_SIGN, LBRACE, RBRACE).  Emit each named code explicitly;
** anything else falls through to a syntax error.
*/
rule lparen   matches /\(/  { LEX_EMIT(LPAREN); }
rule rparen   matches /\)/  { LEX_EMIT(RPAREN); }
rule dot      matches /\./  { LEX_EMIT(DOT); }
rule hash     matches /#/   { LEX_EMIT(HASH); }
rule slash    matches /\//  { LEX_EMIT(SLASH); }
rule at_sign  matches /@/   { LEX_EMIT(AT_SIGN); }
rule lbrace   matches /\{/  { LEX_EMIT(LBRACE); }
rule rbrace   matches /\}/  { LEX_EMIT(RBRACE); }

/* ===== Catch-all =====
**
** Any other character is unexpected; signal via LEX_ERROR_AT and
** the driver translates that into a parse-error path. */
rule unexpected matches /./ {
    LEX_ERROR_AT("unexpected character");
}

/* ===== xc state (C-style comment) ===== */
<XC> rule xc_close matches /\*+\// {
    LEX_TRANSITION(PGPA_STATE_INITIAL);
    LEX_SKIP();
}

<XC> rule xc_inside matches /[^*\/]+/ { LEX_SKIP(); }

<XC> rule xc_other matches /./ { LEX_SKIP(); }

<XC> rule xc_eof matches <<EOF>> {
    LEX_ERROR_AT("unterminated comment");
}

/* ===== xd state (double-quoted identifier) ===== */
<XD> rule xd_double matches /""/ {
    LEX_BUF_APPEND_CH(scanid, '"');
    LEX_SKIP();
}

<XD> rule xd_inside matches /[^"]+/ {
    LEX_BUF_APPEND(scanid, matched, matched_len);
    LEX_SKIP();
}

<XD> rule xd_close matches /"/ {
    size_t n = LEX_BUF_LEN(scanid);
    char *s = LEX_BUF_TAKE(scanid);
    if (s == NULL) {
        LEX_ERROR_AT("oom in literal buffer take");
    } else if (n == 0) {
        pfree(s);
        LEX_ERROR_AT("zero-length delimited identifier");
    } else {
        /* Sentinel 1001 distinguishes quoted IDENT from the regular
        ** TOK_IDENT path (driver runs pstrdup + no downcase). */
        if (emit) emit(user, 1001, s, n);
        pfree(s);
    }
    LEX_TRANSITION(PGPA_STATE_INITIAL);
    LEX_SKIP();
}

<XD> rule xd_eof matches <<EOF>> {
    LEX_ERROR_AT("unterminated quoted identifier");
}
