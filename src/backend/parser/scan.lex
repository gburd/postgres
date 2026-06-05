/*-------------------------------------------------------------------------
 *
 * scan.lex
 *	  Lime lexer for the backend SQL grammar.
 *
 * Replaces the hand-rolled state machine in scan.c (~2000 lines) with
 * a declarative .lex source compiled by Lime v0.2.2's lexer subsystem.
 * scan.c shrinks to a parser-driver shim that pre-scans the input via
 * CoreLexFeedBytes, capturing tokens into a FIFO, then yields them to
 * core_yylex callers (gram.c via base_yylex, and pl_scanner.c) one at
 * a time, restoring/stuffing NUL terminators on the scanbuf so the
 * lookahead/un-truncate dance in base_yylex continues to work.
 *
 * The .lex source mirrors flex scan.l's eleven exclusive states (xb,
 * xc, xd, xh, xq, xqs, xe, xdolq, xui, xus, xeu) and the rule
 * ordering inside INITIAL.  Tokens emitted from action bodies are a
 * mix of:
 *   - parser-side token codes (gram.h: IDENT, ICONST, FCONST, SCONST,
 *     PARAM, TYPECAST, ..., plus single-char ASCII for self chars),
 *   - internal sentinel codes (>1000) for tokens that need driver-side
 *     post-processing (keyword lookup, operator trimming, integer
 *     parsing, downcase_truncate_identifier, etc.).
 *
 * Literal buffering uses a parallel StringInfo in the user context
 * rather than %literal_buffer, because the Unicode-escape helpers
 * need to append from outside an action body's scope.  See
 * scan_lex_addlit / scan_lex_litbuf_take in scan_lex_internal.h.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/parser/scan.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Core.

%include {
#include "postgres.h"

#include "scan_lex_internal.h"
}

/* ----------------------------------------------------------------- */
/* Exclusive states                                                  */
/* ----------------------------------------------------------------- */

%exclusive_state XB.
%exclusive_state XC.
%exclusive_state XD.
%exclusive_state XH.
%exclusive_state XQ.
%exclusive_state XQS.
%exclusive_state XE.
%exclusive_state XDOLQ.
%exclusive_state XUI.
%exclusive_state XUS.
%exclusive_state XEU.

/* ----------------------------------------------------------------- */
/* Pattern fragments (from scan.l; identical regexes)                */
/* ----------------------------------------------------------------- */

%pattern space             /[ \t\n\r\f\v]/.
%pattern non_newline       /[^\n\r]/.
%pattern non_newline_space /[ \t\f\v]/.

%pattern comment   /--{non_newline}*/.

%pattern ws        /({space}+|{comment})/.
%pattern special_ws        /({space}+|{comment}\n|{comment}\r)/.
%pattern non_newline_ws    /({non_newline_space}|{comment})/.
%pattern ws_with_newline   /{non_newline_ws}*(\n|\r){special_ws}*/.

%pattern quote     /'/.
%pattern quotecontinue /{ws_with_newline}{quote}/.

%pattern xcstart /\/\*[\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]*/.
%pattern xcstop  /\*+\//.
%pattern xcinside /[^*\/]+/.

%pattern xbstart /[bB]'/.
%pattern xbinside /[^']+/.

%pattern xhstart /[xX]'/.
%pattern xhinside /[^']+/.

%pattern xnstart /[nN]'/.

%pattern xqstart /'/.
%pattern xqinside /[^'\\]+/.
%pattern xqdouble /''/.

%pattern xestart /[eE]'/.
%pattern xeinside /[^\\']+/.
%pattern xeunicode  /\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})/.
%pattern xeunicodefail /\\(u[0-9A-Fa-f]{0,3}|U[0-9A-Fa-f]{0,7})/.
%pattern xeescape /\\[^0-7uUx]/.
%pattern xehexesc /\\x[0-9A-Fa-f]{1,2}/.
%pattern xehexfail /\\x/.
%pattern xeoctesc  /\\[0-7]{1,3}/.

%pattern xusstart /[uU]&'/.
%pattern xuistart /[uU]&"/.
%pattern xufailed /[uU]&/.

%pattern dolq_start /[A-Za-z_\x80-\xff]/.
%pattern dolq_cont  /[A-Za-z_0-9\x80-\xff]/.
%pattern dolqdelim  /\$({dolq_start}{dolq_cont}*)?\$/.
%pattern dolqfailed /\${dolq_start}{dolq_cont}*/.
%pattern dolqinside /[^$]+/.

%pattern xdstart /"/.
%pattern xdinside /[^"]+/.
%pattern xddouble /""/.

%pattern ident_start /[A-Za-z_\x80-\xff]/.
%pattern ident_cont  /[A-Za-z_0-9\$\x80-\xff]/.
%pattern identifier  /{ident_start}{ident_cont}*/.

%pattern self     /[,()\[\]\.;:|+\-*\/%^<>=]/.
%pattern op_chars /[\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]/.
%pattern operator /{op_chars}+/.

%pattern decdigit /[0-9]/.
%pattern hexdigit /[0-9A-Fa-f]/.
%pattern octdigit /[0-7]/.
%pattern bindigit /[01]/.

%pattern decinteger /{decdigit}(_?{decdigit})*/.
%pattern hexinteger /0[xX](_?{hexdigit})+/.
%pattern octinteger /0[oO](_?{octdigit})+/.
%pattern bininteger /0[bB](_?{bindigit})+/.

%pattern hexfail /0[xX]_?/.
%pattern octfail /0[oO]_?/.
%pattern binfail /0[bB]_?/.

%pattern numeric     /(({decinteger}\.{decinteger}?)|(\.{decinteger}))/.
%pattern numericfail /{decinteger}\.\./.

%pattern real     /({decinteger}|{numeric})[Ee][-+]?{decinteger}/.
%pattern realfail /({decinteger}|{numeric})[Ee][-+]/.

%pattern integer_junk /{decinteger}{identifier}/.
%pattern numeric_junk /{numeric}{identifier}/.
%pattern real_junk    /{real}{identifier}/.
%pattern param        /\${decdigit}+/.
%pattern param_junk   /\${decdigit}+{identifier}/.

/* =================================================================== */
/* INITIAL state rules (declaration order = tie-break order)           */
/* =================================================================== */

/* ----- Whitespace and SQL line comments ----- */
rule ws_initial matches /{ws}+/ { LEX_SKIP(); }

/* ----- C-style nested comment opener ----- */
rule xc_open matches /{xcstart}/ {
    SCAN_LEX_SET_LOC(matched);
    SCAN_LEX_XCDEPTH(user) = 0;
    LEX_PUSHBACK(matched_len - 2);
    LEX_TRANSITION(CORE_STATE_XC);
    LEX_SKIP();
}

/* ----- xb' (bit string) ----- */
rule xb_open matches /{xbstart}/ {
    SCAN_LEX_SET_LOC(matched);
    SCAN_LEX_LITSTART(user) = SCAN_LEX_OFFSET(matched);
    scan_lex_litbuf_start(user);
    scan_lex_addlitchar(user, 'b');
    LEX_TRANSITION(CORE_STATE_XB);
    LEX_SKIP();
}

/* ----- xh' (hex string) ----- */
rule xh_open matches /{xhstart}/ {
    SCAN_LEX_SET_LOC(matched);
    SCAN_LEX_LITSTART(user) = SCAN_LEX_OFFSET(matched);
    scan_lex_litbuf_start(user);
    scan_lex_addlitchar(user, 'x');
    LEX_TRANSITION(CORE_STATE_XH);
    LEX_SKIP();
}

/* ----- xn' (national char): "n" plus pushed-back quote ----- */
rule xn_open matches /{xnstart}/ {
    SCAN_LEX_SET_LOC(matched);
    LEX_PUSHBACK(matched_len - 1);
    emit(user, SCAN_TOK_NCHAR, matched, 1);
    LEX_SKIP();
}

/* ----- xq' (single-quoted standard string) ----- */
rule xq_open matches /{xqstart}/ {
    SCAN_LEX_SET_LOC(matched);
    SCAN_LEX_LITSTART(user) = SCAN_LEX_OFFSET(matched);
    SCAN_LEX_SAW_NON_ASCII(user) = false;
    scan_lex_litbuf_start(user);
    LEX_TRANSITION(CORE_STATE_XQ);
    LEX_SKIP();
}

/* ----- xe' (extended escape string) ----- */
rule xe_open matches /{xestart}/ {
    SCAN_LEX_SET_LOC(matched);
    SCAN_LEX_LITSTART(user) = SCAN_LEX_OFFSET(matched);
    SCAN_LEX_SAW_NON_ASCII(user) = false;
    scan_lex_litbuf_start(user);
    LEX_TRANSITION(CORE_STATE_XE);
    LEX_SKIP();
}

/* ----- u&' (Unicode string) ----- */
rule xus_open matches /{xusstart}/ {
    SCAN_LEX_SET_LOC(matched);
    SCAN_LEX_LITSTART(user) = SCAN_LEX_OFFSET(matched);
    scan_lex_litbuf_start(user);
    LEX_TRANSITION(CORE_STATE_XUS);
    LEX_SKIP();
}

/* ----- u&" (Unicode delimited identifier) ----- */
rule xui_open matches /{xuistart}/ {
    SCAN_LEX_SET_LOC(matched);
    SCAN_LEX_LITSTART(user) = SCAN_LEX_OFFSET(matched);
    scan_lex_litbuf_start(user);
    LEX_TRANSITION(CORE_STATE_XUI);
    LEX_SKIP();
}

/* ----- u& with neither ' nor " : push back &, treat as ident "u" ----- */
rule xu_failed matches /{xufailed}/ {
    SCAN_LEX_SET_LOC(matched);
    LEX_PUSHBACK(matched_len - 1);
    emit(user, SCAN_TOK_IDENT_RAW, matched, 1);
    LEX_SKIP();
}

/* ----- $foo$ dollar-quote opener ----- */
rule xdolq_open matches /{dolqdelim}/ {
    SCAN_LEX_SET_LOC(matched);
    {
        char *tag = palloc(matched_len + 1);
        memcpy(tag, matched, matched_len);
        tag[matched_len] = '\0';
        SCAN_LEX_DOLQSTART(user) = tag;
    }
    SCAN_LEX_LITSTART(user) = SCAN_LEX_OFFSET(matched);
    scan_lex_litbuf_start(user);
    LEX_TRANSITION(CORE_STATE_XDOLQ);
    LEX_SKIP();
}

/* ----- $foo without closing $ : emit '$', push back rest ----- */
rule xdolq_failed matches /{dolqfailed}/ {
    SCAN_LEX_SET_LOC(matched);
    LEX_PUSHBACK(matched_len - 1);
    emit(user, SCAN_TOK_RAW_CHAR, matched, 1);
    LEX_SKIP();
}

/* ----- " (double-quote: open delimited identifier) ----- */
rule xd_open matches /{xdstart}/ {
    SCAN_LEX_SET_LOC(matched);
    SCAN_LEX_LITSTART(user) = SCAN_LEX_OFFSET(matched);
    scan_lex_litbuf_start(user);
    LEX_TRANSITION(CORE_STATE_XD);
    LEX_SKIP();
}

/* ----- Multi-character punctuation (declared before {operator}) ----- */
rule typecast matches /::/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_TYPECAST, matched, matched_len);
    LEX_SKIP();
}
rule dot_dot matches /\.\./ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_DOT_DOT, matched, matched_len);
    LEX_SKIP();
}
rule colon_eq matches /:=/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_COLON_EQUALS, matched, matched_len);
    LEX_SKIP();
}

/* ----- Numeric literals -----
** Order from scan.l: decinteger, hex/oct/bin integer, *fail, numeric,
** numericfail, real, realfail, junks last.  Length-tie tiebreak by
** declaration order picks the proper integer/numeric over the catch-all
** {decinteger}{identifier} junk match.
*/
rule decinteger matches /{decinteger}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_ICONST_DEC, matched, matched_len);
    LEX_SKIP();
}
rule hexinteger matches /{hexinteger}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_ICONST_HEX, matched, matched_len);
    LEX_SKIP();
}
rule octinteger matches /{octinteger}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_ICONST_OCT, matched, matched_len);
    LEX_SKIP();
}
rule bininteger matches /{bininteger}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_ICONST_BIN, matched, matched_len);
    LEX_SKIP();
}

rule hexfail matches /{hexfail}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_HEXFAIL, matched, matched_len);
    LEX_SKIP();
}
rule octfail matches /{octfail}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_OCTFAIL, matched, matched_len);
    LEX_SKIP();
}
rule binfail matches /{binfail}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_BINFAIL, matched, matched_len);
    LEX_SKIP();
}

rule numeric matches /{numeric}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_FCONST_NUMERIC, matched, matched_len);
    LEX_SKIP();
}
rule numericfail matches /{numericfail}/ {
    SCAN_LEX_SET_LOC(matched);
    LEX_PUSHBACK(2);
    emit(user, SCAN_TOK_ICONST_DEC, matched, matched_len - 2);
    LEX_SKIP();
}

rule real matches /{real}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_FCONST_REAL, matched, matched_len);
    LEX_SKIP();
}
rule realfail matches /{realfail}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_TRAILING_JUNK_NUM, matched, matched_len);
    LEX_SKIP();
}

rule integer_junk matches /{integer_junk}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_TRAILING_JUNK_NUM, matched, matched_len);
    LEX_SKIP();
}
rule numeric_junk matches /{numeric_junk}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_TRAILING_JUNK_NUM, matched, matched_len);
    LEX_SKIP();
}
rule real_junk matches /{real_junk}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_TRAILING_JUNK_NUM, matched, matched_len);
    LEX_SKIP();
}

/* ----- $N parameter marker ----- */
rule param_junk matches /{param_junk}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_PARAM_JUNK, matched, matched_len);
    LEX_SKIP();
}
rule param matches /{param}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_PARAM, matched, matched_len);
    LEX_SKIP();
}

/* ----- Operator (driver trims comments + trailing +/-) ----- */
rule op matches /{operator}/ {
    SCAN_LEX_SET_LOC(matched);
    {
        size_t keep = scan_lex_op_keep(matched, matched_len);
        if (keep < matched_len) {
            LEX_PUSHBACK(matched_len - keep);
            matched_len = keep;
        }
    }
    emit(user, SCAN_TOK_OP, matched, matched_len);
    LEX_SKIP();
}

/* ----- Identifier (driver does keyword lookup) ----- */
rule ident matches /{identifier}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_IDENT_RAW, matched, matched_len);
    LEX_SKIP();
}

/* ----- Single-char self/other (returned as raw ASCII byte) ----- */
rule self_ch matches /{self}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_RAW_CHAR, matched, matched_len);
    LEX_SKIP();
}
rule other matches /[\x00-\xff]/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_RAW_CHAR, matched, matched_len);
    LEX_SKIP();
}

/* =================================================================== */
/* XC state: nested C-style comment                                    */
/* =================================================================== */

<XC> rule xc_nested matches /{xcstart}/ {
    SCAN_LEX_XCDEPTH(user)++;
    LEX_PUSHBACK(matched_len - 2);
    LEX_SKIP();
}
<XC> rule xc_close matches /{xcstop}/ {
    if (SCAN_LEX_XCDEPTH(user) <= 0)
        LEX_TRANSITION(CORE_STATE_INITIAL);
    else
        SCAN_LEX_XCDEPTH(user)--;
    LEX_SKIP();
}
<XC> rule xc_inside matches /{xcinside}/ { LEX_SKIP(); }
<XC> rule xc_op_chars matches /{op_chars}+/ { LEX_SKIP(); }
<XC> rule xc_stars matches /\*+/ { LEX_SKIP(); }
<XC> rule xc_eof matches <<EOF>> { LEX_ERROR_AT("unterminated /* comment"); }

/* =================================================================== */
/* XB state: bit-string body                                           */
/* =================================================================== */

<XB> rule xb_inside matches /{xbinside}/ {
    scan_lex_addlit(user, matched, matched_len);
    LEX_SKIP();
}
<XB> rule xb_close matches /{quote}/ {
    SCAN_LEX_PREV_STATE(user) = CORE_STATE_XB;
    LEX_TRANSITION(CORE_STATE_XQS);
    LEX_SKIP();
}
<XB> rule xb_eof matches <<EOF>> { LEX_ERROR_AT("unterminated bit string literal"); }

/* =================================================================== */
/* XH state: hex-string body                                           */
/* =================================================================== */

<XH> rule xh_inside matches /{xhinside}/ {
    scan_lex_addlit(user, matched, matched_len);
    LEX_SKIP();
}
<XH> rule xh_close matches /{quote}/ {
    SCAN_LEX_PREV_STATE(user) = CORE_STATE_XH;
    LEX_TRANSITION(CORE_STATE_XQS);
    LEX_SKIP();
}
<XH> rule xh_eof matches <<EOF>> { LEX_ERROR_AT("unterminated hexadecimal string literal"); }

/* =================================================================== */
/* XQ state: standard quoted string body                               */
/* =================================================================== */

<XQ> rule xq_double matches /{xqdouble}/ {
    scan_lex_addlitchar(user, '\'');
    LEX_SKIP();
}
<XQ> rule xq_inside matches /[^']+/ {
    scan_lex_addlit(user, matched, matched_len);
    LEX_SKIP();
}
<XQ> rule xq_close matches /{quote}/ {
    SCAN_LEX_PREV_STATE(user) = CORE_STATE_XQ;
    LEX_TRANSITION(CORE_STATE_XQS);
    LEX_SKIP();
}
<XQ> rule xq_eof matches <<EOF>> { LEX_ERROR_AT("unterminated quoted string"); }

/* =================================================================== */
/* XE state: extended-escape quoted string body                        */
/* =================================================================== */

<XE> rule xe_double matches /{xqdouble}/ {
    scan_lex_addlitchar(user, '\'');
    LEX_SKIP();
}
<XE> rule xe_unicode matches /{xeunicode}/ {
    SCAN_LEX_SET_LOC(matched);
    {
        char        ubuf[12];
        size_t      hlen = matched_len - 2;
        char32_t    c;
        int         rc;

        if (hlen >= sizeof(ubuf)) hlen = sizeof(ubuf) - 1;
        memcpy(ubuf, matched + 2, hlen);
        ubuf[hlen] = '\0';
        c = (char32_t) strtoul(ubuf, NULL, 16);
        rc = scan_lex_handle_unicode(user,
                                          SCAN_LEX_OFFSET(matched),
                                          c);
        if (rc == 1)
            LEX_TRANSITION(CORE_STATE_XEU);
    }
    LEX_SKIP();
}
<XE> rule xe_unicode_fail matches /{xeunicodefail}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_BAD_UNICODE_ESCAPE, matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_escape matches /{xeescape}/ {
    SCAN_LEX_SET_LOC(matched);
    scan_lex_handle_xeescape(user, SCAN_LEX_OFFSET(matched),
                              (unsigned char) matched[1]);
    LEX_SKIP();
}
<XE> rule xe_hexesc matches /{xehexesc}/ {
    SCAN_LEX_SET_LOC(matched);
    scan_lex_handle_xehexesc(user, matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_hexfail matches /{xehexfail}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_BAD_HEX_ESCAPE, matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_octesc matches /{xeoctesc}/ {
    SCAN_LEX_SET_LOC(matched);
    scan_lex_handle_xeoctesc(user, matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_inside matches /{xeinside}/ {
    scan_lex_addlit(user, matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_lone_backslash matches /\\/ {
    scan_lex_addlitchar(user, '\\');
    LEX_SKIP();
}
<XE> rule xe_close matches /{quote}/ {
    SCAN_LEX_PREV_STATE(user) = CORE_STATE_XE;
    LEX_TRANSITION(CORE_STATE_XQS);
    LEX_SKIP();
}
<XE> rule xe_eof matches <<EOF>> { LEX_ERROR_AT("unterminated quoted string"); }

/* =================================================================== */
/* XEU state: half-way through a Unicode surrogate pair                */
/* =================================================================== */

<XEU> rule xeu_unicode matches /{xeunicode}/ {
    SCAN_LEX_SET_LOC(matched);
    {
        char        ubuf[12];
        size_t      hlen = matched_len - 2;
        char32_t    c;

        if (hlen >= sizeof(ubuf)) hlen = sizeof(ubuf) - 1;
        memcpy(ubuf, matched + 2, hlen);
        ubuf[hlen] = '\0';
        c = (char32_t) strtoul(ubuf, NULL, 16);
        scan_lex_handle_xeu_second(user,
                                    SCAN_LEX_OFFSET(matched), c);
    }
    LEX_TRANSITION(CORE_STATE_XE);
    LEX_SKIP();
}
<XEU> rule xeu_unicode_fail matches /{xeunicodefail}/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_BAD_UNICODE_ESCAPE, matched, matched_len);
    LEX_SKIP();
}
<XEU> rule xeu_other matches /[\x00-\xff]/ {
    SCAN_LEX_SET_LOC(matched);
    emit(user, SCAN_TOK_BAD_SURROGATE, matched, matched_len);
    LEX_SKIP();
}
<XEU> rule xeu_eof matches <<EOF>> {
    LEX_ERROR_AT("invalid Unicode surrogate pair");
}

/* =================================================================== */
/* XUS state: u&'...' Unicode-escape string body                       */
/* =================================================================== */

<XUS> rule xus_double matches /{xqdouble}/ {
    scan_lex_addlitchar(user, '\'');
    LEX_SKIP();
}
<XUS> rule xus_inside matches /[^']+/ {
    scan_lex_addlit(user, matched, matched_len);
    LEX_SKIP();
}
<XUS> rule xus_close matches /{quote}/ {
    SCAN_LEX_PREV_STATE(user) = CORE_STATE_XUS;
    LEX_TRANSITION(CORE_STATE_XQS);
    LEX_SKIP();
}
<XUS> rule xus_eof matches <<EOF>> { LEX_ERROR_AT("unterminated quoted string"); }

/* =================================================================== */
/* XQS state: quote-stop -- look for continuation                      */
/* =================================================================== */

<XQS> rule xqs_continue matches /{quotecontinue}/ {
    LEX_TRANSITION(SCAN_LEX_PREV_STATE(user));
    LEX_SKIP();
}
<XQS> rule xqs_other matches /[\x00-\xff]/ {
    LEX_PUSHBACK(matched_len);
    {
        int prev = SCAN_LEX_PREV_STATE(user);
        size_t n = scan_lex_litbuf_len(user);
        char *taken = scan_lex_litbuf_take(user, NULL);
        int code;

        switch (prev) {
            case CORE_STATE_XB:  code = SCAN_TOK_BCONST;  break;
            case CORE_STATE_XH:  code = SCAN_TOK_XCONST;  break;
            case CORE_STATE_XQ:  code = SCAN_TOK_SCONST;  break;
            case CORE_STATE_XE:  code = SCAN_TOK_SCONST;  break;
            case CORE_STATE_XUS: code = SCAN_TOK_USCONST; break;
            default:             code = SCAN_TOK_SCONST;  break;
        }
        scan_lex_set_compound_end(user, SCAN_LEX_LITSTART(user),
                                  SCAN_LEX_OFFSET(matched));
        emit(user, code, taken, n);
        if (taken) pfree(taken);
    }
    LEX_TRANSITION(CORE_STATE_INITIAL);
    LEX_SKIP();
}
<XQS> rule xqs_eof matches <<EOF>> {
    int prev = SCAN_LEX_PREV_STATE(user);
    size_t n = scan_lex_litbuf_len(user);
    char *taken = scan_lex_litbuf_take(user, NULL);
    int code;

    switch (prev) {
        case CORE_STATE_XB:  code = SCAN_TOK_BCONST;  break;
        case CORE_STATE_XH:  code = SCAN_TOK_XCONST;  break;
        case CORE_STATE_XQ:  code = SCAN_TOK_SCONST;  break;
        case CORE_STATE_XE:  code = SCAN_TOK_SCONST;  break;
        case CORE_STATE_XUS: code = SCAN_TOK_USCONST; break;
        default:             code = SCAN_TOK_SCONST;  break;
    }
    scan_lex_set_compound_end(user, SCAN_LEX_LITSTART(user),
                              SCAN_LEX_TOTAL(user));
    emit(user, code, taken, n);
    if (taken) pfree(taken);
    LEX_TRANSITION(CORE_STATE_INITIAL);
    LEX_SKIP();
}

/* =================================================================== */
/* XDOLQ state: dollar-quoted string body                              */
/* =================================================================== */

<XDOLQ> rule xdolq_close matches /{dolqdelim}/ {
    const char *tag = SCAN_LEX_DOLQSTART(user);
    if (tag != NULL && (size_t) strlen(tag) == matched_len &&
        memcmp(tag, matched, matched_len) == 0) {
        size_t n = scan_lex_litbuf_len(user);
        char *taken = scan_lex_litbuf_take(user, NULL);
        scan_lex_set_compound_end(user, SCAN_LEX_LITSTART(user),
                                   SCAN_LEX_OFFSET(matched) + (int) matched_len);
        emit(user, SCAN_TOK_SCONST, taken, n);
        if (taken) pfree(taken);
        pfree((char *) tag);
        SCAN_LEX_DOLQSTART(user) = NULL;
        LEX_TRANSITION(CORE_STATE_INITIAL);
    } else {
        scan_lex_addlit(user, matched, matched_len - 1);
        LEX_PUSHBACK(1);
    }
    LEX_SKIP();
}
<XDOLQ> rule xdolq_inside matches /{dolqinside}/ {
    scan_lex_addlit(user, matched, matched_len);
    LEX_SKIP();
}
<XDOLQ> rule xdolq_inner_failed matches /{dolqfailed}/ {
    scan_lex_addlit(user, matched, matched_len);
    LEX_SKIP();
}
<XDOLQ> rule xdolq_lone_dollar matches /\$/ {
    scan_lex_addlitchar(user, '$');
    LEX_SKIP();
}
<XDOLQ> rule xdolq_eof matches <<EOF>> {
    LEX_ERROR_AT("unterminated dollar-quoted string");
}

/* =================================================================== */
/* XD state: delimited identifier "..." body                           */
/* =================================================================== */

<XD> rule xd_double matches /{xddouble}/ {
    scan_lex_addlitchar(user, '"');
    LEX_SKIP();
}
<XD> rule xd_inside matches /{xdinside}/ {
    scan_lex_addlit(user, matched, matched_len);
    LEX_SKIP();
}
<XD> rule xd_close matches /{xdstart}/ {
    size_t n = scan_lex_litbuf_len(user);
    char *taken = scan_lex_litbuf_take(user, NULL);
    scan_lex_set_compound_end(user, SCAN_LEX_LITSTART(user),
                               SCAN_LEX_OFFSET(matched) + 1);
    emit(user, SCAN_TOK_IDENT_QUOTED, taken, n);
    if (taken) pfree(taken);
    LEX_TRANSITION(CORE_STATE_INITIAL);
    LEX_SKIP();
}
<XD> rule xd_eof matches <<EOF>> { LEX_ERROR_AT("unterminated quoted identifier"); }

/* =================================================================== */
/* XUI state: u&"..." delimited Unicode identifier body                */
/* =================================================================== */

<XUI> rule xui_double matches /{xddouble}/ {
    scan_lex_addlitchar(user, '"');
    LEX_SKIP();
}
<XUI> rule xui_inside matches /{xdinside}/ {
    scan_lex_addlit(user, matched, matched_len);
    LEX_SKIP();
}
<XUI> rule xui_close matches /{xdstart}/ {
    size_t n = scan_lex_litbuf_len(user);
    char *taken = scan_lex_litbuf_take(user, NULL);
    scan_lex_set_compound_end(user, SCAN_LEX_LITSTART(user),
                               SCAN_LEX_OFFSET(matched) + 1);
    emit(user, SCAN_TOK_UIDENT, taken, n);
    if (taken) pfree(taken);
    LEX_TRANSITION(CORE_STATE_INITIAL);
    LEX_SKIP();
}
<XUI> rule xui_eof matches <<EOF>> { LEX_ERROR_AT("unterminated quoted identifier"); }
