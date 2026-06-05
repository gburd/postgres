/*-------------------------------------------------------------------------
 *
 * jsonpath_scan.lex
 *	  Lime lexer for the jsonpath datatype.
 *
 * Replaces the hand-rolled scanner in jsonpath_scan.c (~1700 lines)
 * with a declarative .lex source compiled by Lime v0.2.2's lexer
 * subsystem.  The accompanying jsonpath_scan.c shrinks to a
 * parser-driver shim plus the JsonPathParseItem constructors and
 * the public yyerror entry points.
 *
 * The four exclusive states (xq, xnq, xvq, xc) mirror flex's
 * jsonpath_scan.l one-to-one.  Tokens emitted from action bodies
 * are a mix of:
 *	- parser-side token codes (jsonpath_gram.h: AND_P, OR_P, NOT_P,
 *	  ANY_P, EQUAL_P, NOTEQUAL_P, LESS_P, LESSEQUAL_P, GREATER_P,
 *	  GREATEREQUAL_P) for tokens with no payload, and
 *	- internal sentinel codes (>1000) for tokens that need
 *	  driver-side post-processing (keyword lookup for unquoted
 *	  identifiers, single-char self/other byte-to-token mapping,
 *	  numeric literal text capture, accumulator-take for STRING_P
 *	  and VARIABLE_P).
 *
 * Literal buffering goes through a parallel JsonPathString on the
 * caller's JsonPathYyScanner, mirroring scan.lex's choice -- the
 * Unicode-escape helpers (parseUnicode / parseHexChar) need to
 * append from outside an action body's scope, which %literal_buffer
 * cannot do.  See jp_lex_addchar / jp_lex_addstring in
 * jsonpath_scan_lex_internal.h.
 *
 * The numeric-literal regexes (real, decimal, decinteger, hex/oct/bin
 * integer plus their *fail and *_junk variants) are declared in the
 * same order flex used so that longest-match-then-declaration-order
 * tiebreak picks the proper token over any *_junk catch-all.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/utils/adt/jsonpath_scan.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix JsonPath.

%include {
#include "postgres.h"

#include "jsonpath_scan_lex_internal.h"
}

/* ----------------------------------------------------------------- */
/* Exclusive states                                                  */
/* ----------------------------------------------------------------- */

%exclusive_state XQ.
%exclusive_state XNQ.
%exclusive_state XVQ.
%exclusive_state XC.

/* ----------------------------------------------------------------- */
/* Pattern fragments (mirroring jsonpath_scan.l verbatim)            */
/* ----------------------------------------------------------------- */

%pattern special   /[?%$.\[\]{}()|&!=<>@#,*:\-+\/]/.
%pattern blank     /[ \t\n\r\f]/.
/* "other" = not special, not blank, not '\' or '"'.  Inside [^...] */
/* the metacharacters lose their special meaning, but Lime accepts */
/* escaped forms uniformly so we keep the same characters as the */
/* positive class above. */
%pattern other     /[^?%$.\[\]{}()|&!=<>@#,*:\-+\/\\" \t\n\r\f]/.

%pattern decdigit  /[0-9]/.
%pattern hexdigit  /[0-9A-Fa-f]/.
%pattern octdigit  /[0-7]/.
%pattern bindigit  /[01]/.

%pattern decinteger /(0|[1-9](_?{decdigit})*)/.
%pattern decdigits  /{decdigit}(_?{decdigit})*/.
%pattern hexinteger /0[xX]{hexdigit}(_?{hexdigit})*/.
%pattern octinteger /0[oO]{octdigit}(_?{octdigit})*/.
%pattern bininteger /0[bB]{bindigit}(_?{bindigit})*/.

%pattern decimal   /({decinteger}\.{decdigits}?|\.{decdigits})/.
%pattern real      /({decinteger}|{decimal})[Ee][-+]?{decdigits}/.
%pattern realfail  /({decinteger}|{decimal})[Ee][-+]/.

%pattern decinteger_junk /{decinteger}{other}/.
%pattern decimal_junk    /{decimal}{other}/.
%pattern real_junk       /{real}{other}/.

%pattern unicode     /\\u({hexdigit}{4}|\{{hexdigit}{1,6}\})/.
%pattern unicodefail /\\u({hexdigit}{0,3}|\{{hexdigit}{0,6})/.
%pattern hex_char    /\\x{hexdigit}{2}/.
%pattern hex_fail    /\\x{hexdigit}{0,1}/.

/* =================================================================== */
/* INITIAL state rules                                                 */
/* Declaration order matters where lengths tie; e.g. "==" must be      */
/* declared before "=" so the two-char operator wins on equal length.  */
/* =================================================================== */

/* Two-char operators (declared before one-char punctuation). */
rule and_op       matches /&&/  { LEX_EMIT(AND_P); }
rule or_op        matches /\|\|/ { LEX_EMIT(OR_P); }
rule any_op       matches /\*\*/ { LEX_EMIT(ANY_P); }
rule lessequal    matches /<=/ { LEX_EMIT(LESSEQUAL_P); }
rule equal_op     matches /==/ { LEX_EMIT(EQUAL_P); }
rule notequal_lt  matches /<>/ { LEX_EMIT(NOTEQUAL_P); }
rule notequal_bg  matches /!=/ { LEX_EMIT(NOTEQUAL_P); }
rule greaterequal matches />=/ { LEX_EMIT(GREATEREQUAL_P); }

/* One-char comparison ops with named tokens. */
rule less_op    matches /</ { LEX_EMIT(LESS_P); }
rule greater_op matches />/ { LEX_EMIT(GREATER_P); }
rule not_op     matches /!/ { LEX_EMIT(NOT_P); }

/* $"..."  -- enters XVQ with empty accumulator. */
rule xvq_open matches /\$"/ {
    jp_lex_buf_init(user);
    LEX_TRANSITION(JSONPATH_STATE_XVQ);
    LEX_SKIP();
}

/* $other+  -- bare variable, payload is matched bytes 1.. */
rule var_bare matches /\${other}+/ {
    emit(user, JP_TOK_VARIABLE_BARE, matched, matched_len);
    LEX_SKIP();
}

/* "  -- enters XQ with empty accumulator. */
rule xq_open matches /"/ {
    jp_lex_buf_init(user);
    LEX_TRANSITION(JSONPATH_STATE_XQ);
    LEX_SKIP();
}

/* /*  -- enters XC.  Initialise the accumulator the way the flex */
/* source did (addchar(true,'\0',...))  to keep behaviour identical */
/* across the comment-then-token transition. */
rule xc_open matches /\/\*/ {
    jp_lex_buf_init(user);
    LEX_TRANSITION(JSONPATH_STATE_XC);
    LEX_SKIP();
}

/* Numeric literals. */
rule real_num matches /{real}/ {
    emit(user, JP_TOK_NUMERIC_TEXT, matched, matched_len);
    LEX_SKIP();
}
rule decimal_num matches /{decimal}/ {
    emit(user, JP_TOK_NUMERIC_TEXT, matched, matched_len);
    LEX_SKIP();
}
rule decinteger_num matches /{decinteger}/ {
    emit(user, JP_TOK_INT_TEXT, matched, matched_len);
    LEX_SKIP();
}
rule hexinteger_num matches /{hexinteger}/ {
    emit(user, JP_TOK_INT_TEXT, matched, matched_len);
    LEX_SKIP();
}
rule octinteger_num matches /{octinteger}/ {
    emit(user, JP_TOK_INT_TEXT, matched, matched_len);
    LEX_SKIP();
}
rule bininteger_num matches /{bininteger}/ {
    emit(user, JP_TOK_INT_TEXT, matched, matched_len);
    LEX_SKIP();
}

/* Numeric failures. */
rule realfail_num matches /{realfail}/ {
    jp_lex_set_yytext(user, matched, matched_len);
    LEX_ERROR_AT("invalid numeric literal");
}
rule decinteger_junk_num matches /{decinteger_junk}/ {
    jp_lex_set_yytext(user, matched, matched_len);
    LEX_ERROR_AT("trailing junk after numeric literal");
}
rule decimal_junk_num matches /{decimal_junk}/ {
    jp_lex_set_yytext(user, matched, matched_len);
    LEX_ERROR_AT("trailing junk after numeric literal");
}
rule real_junk_num matches /{real_junk}/ {
    jp_lex_set_yytext(user, matched, matched_len);
    LEX_ERROR_AT("trailing junk after numeric literal");
}

/* Single-char self/other (returned as the byte value to drive */
/* driver-side mapping to DOLLAR / AT / LBRACKET / etc.). */
rule special_ch matches /{special}/ {
    emit(user, JP_TOK_RAW_CHAR, matched, 1);
    LEX_SKIP();
}

/* Whitespace -- skip silently. */
rule ws matches /{blank}+/ { LEX_SKIP(); }

/* Backslash at top level -- enter XNQ with the backslash thrown back. */
rule xinitial_backslash matches /\\/ {
    LEX_PUSHBACK(matched_len);
    jp_lex_buf_init(user);
    LEX_TRANSITION(JSONPATH_STATE_XNQ);
    LEX_SKIP();
}

/* {other}+ -- enter XNQ with the matched bytes seeded into the */
/* accumulator (the flex source did addstring(true, ...) here). */
rule xinitial_other matches /{other}+/ {
    jp_lex_buf_init(user);
    jp_lex_addstring(user, matched, matched_len);
    LEX_TRANSITION(JSONPATH_STATE_XNQ);
    LEX_SKIP();
}

/* =================================================================== */
/* XNQ state: unquoted-identifier accumulation                         */
/* =================================================================== */

/* A run of {other} chars stays inside XNQ, appending. */
<XNQ> rule xnq_other matches /{other}+/ {
    jp_lex_addstring(user, matched, matched_len);
    LEX_SKIP();
}

/* {blank}+ ends the identifier (consume blanks; do not push back). */
<XNQ> rule xnq_blank matches /{blank}+/ {
    int kw = jp_lex_check_keyword(user);
    emit(user, kw, NULL, 0);
    LEX_TRANSITION(JSONPATH_STATE_INITIAL);
    LEX_SKIP();
}

/* /* opens a comment after emitting the identifier. */
<XNQ> rule xnq_xc matches /\/\*/ {
    int kw = jp_lex_check_keyword(user);
    emit(user, kw, NULL, 0);
    jp_lex_buf_init(user);
    LEX_TRANSITION(JSONPATH_STATE_XC);
    LEX_SKIP();
}

/* {special}|"  -- emit identifier and push the breaker char back so */
/* INITIAL re-processes it. */
<XNQ> rule xnq_break matches /({special}|")/ {
    LEX_PUSHBACK(matched_len);
    {
        int kw = jp_lex_check_keyword(user);
        emit(user, kw, NULL, 0);
    }
    LEX_TRANSITION(JSONPATH_STATE_INITIAL);
    LEX_SKIP();
}

/* =================================================================== */
/* Combined XNQ/XQ/XVQ state rules: shared escape handling             */
/* =================================================================== */

<XNQ, XQ, XVQ> rule esc_b matches /\\b/ { jp_lex_addchar(user, '\b'); LEX_SKIP(); }
<XNQ, XQ, XVQ> rule esc_f matches /\\f/ { jp_lex_addchar(user, '\f'); LEX_SKIP(); }
<XNQ, XQ, XVQ> rule esc_n matches /\\n/ { jp_lex_addchar(user, '\n'); LEX_SKIP(); }
<XNQ, XQ, XVQ> rule esc_r matches /\\r/ { jp_lex_addchar(user, '\r'); LEX_SKIP(); }
<XNQ, XQ, XVQ> rule esc_t matches /\\t/ { jp_lex_addchar(user, '\t'); LEX_SKIP(); }
<XNQ, XQ, XVQ> rule esc_v matches /\\v/ { jp_lex_addchar(user, '\v'); LEX_SKIP(); }

/* {unicode}+\\  -- throw back the trailing backslash and treat the */
/* preceding run as a unicode escape sequence.  Declared before the */
/* bare {unicode}+ rule so longest-match picks it when applicable. */
<XNQ, XQ, XVQ> rule esc_unicode_run_bs matches /{unicode}+\\/ {
    LEX_PUSHBACK(1);
    if (!jp_lex_parse_unicode(user, matched, matched_len - 1))
        LEX_ERROR_AT("invalid Unicode escape sequence");
    LEX_SKIP();
}

/* One-or-more {unicode} blocks; parseUnicode walks the buffer. */
<XNQ, XQ, XVQ> rule esc_unicode_run matches /{unicode}+/ {
    if (!jp_lex_parse_unicode(user, matched, matched_len))
        LEX_ERROR_AT("invalid Unicode escape sequence");
    LEX_SKIP();
}

/* {hex_char}: single \xHH. */
<XNQ, XQ, XVQ> rule esc_hex matches /{hex_char}/ {
    if (!jp_lex_parse_hex_char(user, matched))
        LEX_ERROR_AT("invalid hexadecimal character sequence");
    LEX_SKIP();
}

/* {unicode}*{unicodefail}: a run of valid \uXXXX followed by an */
/* incomplete \u prefix. */
<XNQ, XQ, XVQ> rule esc_unicode_fail matches /{unicode}*{unicodefail}/ {
    jp_lex_set_yytext(user, matched, matched_len);
    LEX_ERROR_AT("invalid Unicode escape sequence");
}

/* {hex_fail}: \x with 0 or 1 hex digits. */
<XNQ, XQ, XVQ> rule esc_hex_fail matches /{hex_fail}/ {
    jp_lex_set_yytext(user, matched, matched_len);
    LEX_ERROR_AT("invalid hexadecimal character sequence");
}

/* \\.  -- generic 2-char escape (any other byte). */
<XNQ, XQ, XVQ> rule esc_any matches /\\[\x00-\xff]/ {
    jp_lex_addchar(user, matched[1]);
    LEX_SKIP();
}

/* A trailing backslash with no following byte is the unterminated */
/* escape diagnostic.  Note: in Lime, \\ at the end of input would */
/* be matched here only if no <<EOF>> rule fires first; the XNQ */
/* EOF rule below wins for that state.  In XQ/XVQ the EOF rule is */
/* the unterminated-string error which subsumes this case. */
<XNQ, XQ, XVQ> rule esc_lone matches /\\/ {
    jp_lex_set_yytext(user, matched, matched_len);
    LEX_ERROR_AT("unexpected end after backslash");
}

/* =================================================================== */
/* XNQ end-of-input: emit accumulated identifier (clean, no error).    */
/* =================================================================== */

<XNQ> rule xnq_eof matches <<EOF>> {
    int kw = jp_lex_check_keyword(user);
    emit(user, kw, NULL, 0);
    jp_lex_set_yytext_empty(user);
    LEX_TRANSITION(JSONPATH_STATE_INITIAL);
    LEX_SKIP();
}

/* =================================================================== */
/* XQ state: quoted string                                             */
/* =================================================================== */

<XQ> rule xq_close matches /"/ {
    emit(user, JP_TOK_STRING_TAKE, "\"", 1);
    LEX_TRANSITION(JSONPATH_STATE_INITIAL);
    LEX_SKIP();
}

<XQ> rule xq_inside matches /[^\\"]+/ {
    jp_lex_addstring(user, matched, matched_len);
    LEX_SKIP();
}

<XQ> rule xq_eof matches <<EOF>> {
    jp_lex_set_yytext_empty(user);
    LEX_ERROR_AT("unterminated quoted string");
}

/* =================================================================== */
/* XVQ state: $"..." quoted variable name                              */
/* =================================================================== */

<XVQ> rule xvq_close matches /"/ {
    emit(user, JP_TOK_VARIABLE_TAKE, "\"", 1);
    LEX_TRANSITION(JSONPATH_STATE_INITIAL);
    LEX_SKIP();
}

<XVQ> rule xvq_inside matches /[^\\"]+/ {
    jp_lex_addstring(user, matched, matched_len);
    LEX_SKIP();
}

<XVQ> rule xvq_eof matches <<EOF>> {
    jp_lex_set_yytext_empty(user);
    LEX_ERROR_AT("unterminated quoted string");
}

/* =================================================================== */
/* XC state: C-style slash-star comment                              */
/* =================================================================== */

<XC> rule xc_close matches /\*\// {
    LEX_TRANSITION(JSONPATH_STATE_INITIAL);
    LEX_SKIP();
}

<XC> rule xc_inside matches /[^*]+/ { LEX_SKIP(); }

<XC> rule xc_star matches /\*/ { LEX_SKIP(); }

<XC> rule xc_eof matches <<EOF>> {
    jp_lex_set_yytext_empty(user);
    LEX_ERROR_AT("unexpected end of comment");
}
