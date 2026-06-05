/*-------------------------------------------------------------------------
 *
 * psqlscan.lex
 *	  Lime lexer for psql's SQL-side input scanner.
 *
 * Replaces the hand-rolled state machine in psqlscan.c (~2200 lines)
 * with a declarative .lex source compiled by Lime v0.2.2's lexer
 * subsystem.  Action bodies invoke helpers in psqlscan.c via the
 * PsqlEmitCtx cookie passed as the LexFeedBytes() user argument;
 * stop points (semicolon at depth 0, backslash, variable expansion)
 * call LEX_TERMINATE() so the driver can consume the appropriate
 * number of bytes and return to its caller (psql / pgbench).
 *
 * Mirrors the ten exclusive states of the legacy flex source
 * (psqlscan.l, pre-Phase 2h): xb, xc, xd, xh, xq, xqs, xe, xdolq,
 * xui, xus.  Token codes are not used; all output is written into
 * PsqlScanState->output_buf via psqlscan_emit() (or directly via
 * appendPQExpBufferStr for variable substitution).  Behaviour must
 * remain byte-identical to psqlscan.c for every test fixture.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/fe_utils/psqlscan.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Psql.

%include {
#include "postgres_fe.h"

#include <string.h>

#include "common/logging.h"
#include "fe_utils/psqlscan.h"
#include "fe_utils/psqlscan_int.h"
#include "fe_utils/psqlscan_emit.h"
}

/* ----------------------------------------------------------------- */
/* Exclusive states (mirror psqlscan.l)                              */
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

/* ----------------------------------------------------------------- */
/* Pattern fragments (mirror scan.lex / psqlscan.l)                  */
/* ----------------------------------------------------------------- */

%pattern space             /[ \t\n\r\f\v]/.
%pattern non_newline       /[^\n\r]/.
%pattern non_newline_space /[ \t\f\v]/.
%pattern comment           /--{non_newline}*/.
%pattern ws                /({space}+|{comment})/.
%pattern special_ws        /({space}+|{comment}\n|{comment}\r)/.
%pattern non_newline_ws    /({non_newline_space}|{comment})/.
%pattern ws_with_newline   /{non_newline_ws}*(\n|\r){special_ws}*/.

%pattern quote /'/.
%pattern quotecontinue /{ws_with_newline}{quote}/.
%pattern quotecontinuefail /({space}*-?)/.

%pattern xcstart /\/\*[\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]*/.
%pattern xcstop  /\*+\//.
%pattern xcinside /[^*\/]+/.

%pattern xbstart  /[bB]'/.
%pattern xbinside /[^']+/.

%pattern xhstart  /[xX]'/.
%pattern xhinside /[^']+/.

%pattern xnstart  /[nN]'/.

%pattern xqstart  /'/.
%pattern xqdouble /''/.
%pattern xqinside /[^']+/.

%pattern xestart       /[eE]'/.
%pattern xeinside      /[^\\']+/.
%pattern xeunicode     /\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})/.
%pattern xeunicodefail /\\(u[0-9A-Fa-f]{0,3}|U[0-9A-Fa-f]{0,7})/.
%pattern xeescape      /\\[^0-7]/.
%pattern xehexesc      /\\x[0-9A-Fa-f]{1,2}/.
%pattern xeoctesc      /\\[0-7]{1,3}/.

%pattern xusstart /[uU]&'/.
%pattern xuistart /[uU]&"/.
%pattern xufailed /[uU]&/.

%pattern dolq_start /[A-Za-z_\x80-\xff]/.
%pattern dolq_cont  /[A-Za-z_0-9\x80-\xff]/.
%pattern dolqdelim  /\$({dolq_start}{dolq_cont}*)?\$/.
%pattern dolqfailed /\${dolq_start}{dolq_cont}*/.
%pattern dolqinside /[^$]+/.

%pattern xdstart  /"/.
%pattern xddouble /""/.
%pattern xdinside /[^"]+/.

%pattern ident_start /[A-Za-z_\x80-\xff]/.
%pattern ident_cont  /[A-Za-z_0-9\$\x80-\xff]/.
%pattern identifier  /{ident_start}{ident_cont}*/.

%pattern variable_char /[A-Za-z_0-9\x80-\xff]/.

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
%pattern real        /({decinteger}|{numeric})[Ee][-+]?{decinteger}/.
%pattern realfail    /({decinteger}|{numeric})[Ee][-+]/.

%pattern integer_junk /{decinteger}{identifier}/.
%pattern numeric_junk /{numeric}{identifier}/.
%pattern real_junk    /{real}{identifier}/.

%pattern param      /\${decdigit}+/.
%pattern param_junk /\${decdigit}+{identifier}/.

/* =================================================================== */
/* INITIAL state                                                       */
/* =================================================================== */

rule ws_initial matches /{ws}+/ {
    if (PSQL_OUTBUF(user)->len > 0)
        psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

rule xc_open matches /{xcstart}/ {
    PSQL_STATE(user)->xcdepth = 0;
    LEX_PUSHBACK(matched_len - 2);
    psqlscan_emit(PSQL_STATE(user), matched, 2);
    LEX_TRANSITION(PSQL_STATE_XC);
    LEX_SKIP();
}

rule xb_open matches /{xbstart}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_TRANSITION(PSQL_STATE_XB);
    LEX_SKIP();
}

rule xh_open matches /{xhstart}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_TRANSITION(PSQL_STATE_XH);
    LEX_SKIP();
}

/* {xnstart}: emit only the leading 'n', push the quote back so it
 * starts a regular xq state next iteration. */
rule xn_open matches /{xnstart}/ {
    LEX_PUSHBACK(matched_len - 1);
    psqlscan_emit(PSQL_STATE(user), matched, 1);
    LEX_SKIP();
}

rule xq_open matches /{xqstart}/ {
    if (PSQL_STATE(user)->std_strings)
        LEX_TRANSITION(PSQL_STATE_XQ);
    else
        LEX_TRANSITION(PSQL_STATE_XE);
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

rule xe_open matches /{xestart}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_TRANSITION(PSQL_STATE_XE);
    LEX_SKIP();
}

rule xus_open matches /{xusstart}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_TRANSITION(PSQL_STATE_XUS);
    LEX_SKIP();
}

rule xui_open matches /{xuistart}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_TRANSITION(PSQL_STATE_XUI);
    LEX_SKIP();
}

/* {xufailed}: throw back the &, emit only U/u as a regular char */
rule xu_failed matches /{xufailed}/ {
    LEX_PUSHBACK(matched_len - 1);
    psqlscan_emit(PSQL_STATE(user), matched, 1);
    LEX_SKIP();
}

/* {dolqdelim}: $foo$ enter xdolq */
rule xdolq_open matches /{dolqdelim}/ {
    PsqlScanState s = PSQL_STATE(user);
    s->dolqstart = pg_malloc_array(char, matched_len + 1);
    memcpy(s->dolqstart, matched, matched_len);
    s->dolqstart[matched_len] = '\0';
    psqlscan_emit(s, matched, matched_len);
    LEX_TRANSITION(PSQL_STATE_XDOLQ);
    LEX_SKIP();
}

/* {dolqfailed}: $foo without closing $ -- throw back all but $ */
rule xdolq_failed matches /{dolqfailed}/ {
    LEX_PUSHBACK(matched_len - 1);
    psqlscan_emit(PSQL_STATE(user), matched, 1);
    LEX_SKIP();
}

rule xd_open matches /{xdstart}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_TRANSITION(PSQL_STATE_XD);
    LEX_SKIP();
}

/* Multi-char punctuation declared before {operator} so longest-match
 * arbitration in declaration-order picks them. */
rule typecast       matches /::/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule dot_dot        matches /\.\./ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule colon_eq       matches /:=/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule equals_greater matches /=>/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule less_equals    matches /<=/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule greater_equals matches />=/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule less_greater   matches /<>/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule not_equals     matches /!=/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule right_arrow    matches /->/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }

rule lparen matches /\(/ {
    PSQL_STATE(user)->paren_depth++;
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

rule rparen matches /\)/ {
    if (PSQL_STATE(user)->paren_depth > 0)
        PSQL_STATE(user)->paren_depth--;
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

rule semi matches /;/ {
    PsqlScanState s = PSQL_STATE(user);
    psqlscan_emit(s, matched, matched_len);
    if (s->paren_depth == 0 && s->begin_depth == 0) {
        s->start_state = PSQL_STATE_INITIAL;
        s->init_idents_count = 0;
        PSQL_TERMINATE_AT(user, STOP_SEMI, matched_len);
        LEX_TERMINATE();
    }
    LEX_SKIP();
}

/* "\\;" / "\\:" : emit only the second char, keep scanning */
rule bslash_special matches /\\[;:]/ {
    /* Reset BEGIN/END tracking if semi at outer level */
    if (matched[1] == ';' &&
        PSQL_STATE(user)->paren_depth == 0 &&
        PSQL_STATE(user)->begin_depth == 0)
        PSQL_STATE(user)->init_idents_count = 0;
    psqlscan_emit(PSQL_STATE(user), matched + 1, 1);
    LEX_SKIP();
}

/* Backslash command: stop, hand off to slash lexer */
rule bslash matches /\\/ {
    PsqlScanState s = PSQL_STATE(user);
    s->start_state = PSQL_STATE_INITIAL;
    PSQL_TERMINATE_AT(user, STOP_BACKSLASH, matched_len);
    LEX_TERMINATE();
}

/* :varname (plain) -- driver may push the value as a new buffer */
rule var_plain matches /:{variable_char}+/ {
    if (psql_emit_var_plain(user, matched, matched_len)) {
        /* var_plain populated ctx with var_name (+ var_value or marked
         * recursive) and consumed-base; complete it with our matched_len. */
        PSQL_CTX(user)->consumed += matched_len;
        LEX_TERMINATE();
    }
    LEX_SKIP();
}

rule var_squote matches /:'{variable_char}+'/ {
    psql_emit_var_squote(user, matched, matched_len);
    LEX_SKIP();
}

rule var_dquote matches /:"{variable_char}+"/ {
    psql_emit_var_dquote(user, matched, matched_len);
    LEX_SKIP();
}

rule var_test matches /:\{\?{variable_char}+\}/ {
    psql_emit_var_test(user, matched, matched_len);
    LEX_SKIP();
}

/* Backup-avoidance: incomplete :' / :" / :{ -- echo only the colon */
rule colon_special_fail matches /:['"\{]/ {
    LEX_PUSHBACK(matched_len - 1);
    psqlscan_emit(PSQL_STATE(user), matched, 1);
    LEX_SKIP();
}

/* Numeric literals (declaration order matches scan.lex / psqlscan.l).
 * Lime's longest-match-then-declaration-order arbitration picks the
 * right rule when multiple match. */
rule decinteger matches /{decinteger}/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule hexinteger matches /{hexinteger}/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule octinteger matches /{octinteger}/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule bininteger matches /{bininteger}/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule hexfail    matches /{hexfail}/    { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule octfail    matches /{octfail}/    { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule binfail    matches /{binfail}/    { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }

rule numeric matches /{numeric}/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule numericfail matches /{numericfail}/ {
    /* Throw back the trailing ".." (yyless(yyleng-2) → PUSHBACK(2)). */
    LEX_PUSHBACK(2);
    psqlscan_emit(PSQL_STATE(user), matched, matched_len - 2);
    LEX_SKIP();
}
rule real     matches /{real}/     { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule realfail matches /{realfail}/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule integer_junk matches /{integer_junk}/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule numeric_junk matches /{numeric_junk}/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule real_junk    matches /{real_junk}/    { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }

/* $N parameter and trailing-junk variant */
rule param_junk matches /{param_junk}/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
rule param      matches /{param}/      { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }

/* Operator (with embedded slash-star and dash-dash trimming, and
 * trailing +/- trimming under SQL rules per psqlscan.c).  Helper
 * computes the 'keep' length; we PUSHBACK the difference and emit the
 * kept prefix. */
rule op matches /{operator}/ {
    int nchars = (int) matched_len;
    const char *slashstar = NULL;
    const char *dashdash = NULL;
    int ic;

    for (ic = 0; ic < nchars - 1; ic++)
        if (matched[ic] == '/' && matched[ic + 1] == '*') { slashstar = matched + ic; break; }
    for (ic = 0; ic < nchars - 1; ic++)
        if (matched[ic] == '-' && matched[ic + 1] == '-') { dashdash = matched + ic; break; }
    if (slashstar && dashdash) {
        if (slashstar > dashdash) slashstar = dashdash;
    } else if (!slashstar) {
        slashstar = dashdash;
    }
    if (slashstar) nchars = (int) (slashstar - matched);

    if (nchars > 1 && (matched[nchars - 1] == '+' || matched[nchars - 1] == '-')) {
        int cc;
        for (cc = nchars - 2; cc >= 0; cc--) {
            char c = matched[cc];
            if (c == '~' || c == '!' || c == '@' || c == '#' ||
                c == '^' || c == '&' || c == '|' || c == '`' ||
                c == '?' || c == '%')
                break;
        }
        if (cc < 0) {
            do { nchars--; }
            while (nchars > 1 &&
                   (matched[nchars - 1] == '+' || matched[nchars - 1] == '-'));
        }
    }

    if ((size_t) nchars < matched_len) {
        LEX_PUSHBACK(matched_len - (size_t) nchars);
    }
    psqlscan_emit(PSQL_STATE(user), matched, (size_t) nchars);
    LEX_SKIP();
}

/* Identifier with BEGIN/END tracking. */
rule ident matches /{identifier}/ {
    PsqlScanState s = PSQL_STATE(user);

    psqlscan_track_identifier(s, matched, matched_len);
    psqlscan_emit(s, matched, matched_len);
    LEX_SKIP();
}

/* {self}: single-character punctuation tokens */
rule self_ch matches /{self}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

/* {other}: any single byte not matched by anything above */
rule other matches /[\x00-\xff]/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

/* =================================================================== */
/* XC state: nested C-style comment                                    */
/* =================================================================== */

<XC> rule xc_nested matches /{xcstart}/ {
    PSQL_STATE(user)->xcdepth++;
    LEX_PUSHBACK(matched_len - 2);
    psqlscan_emit(PSQL_STATE(user), matched, 2);
    LEX_SKIP();
}

<XC> rule xc_close matches /{xcstop}/ {
    if (PSQL_STATE(user)->xcdepth <= 0)
        LEX_TRANSITION(PSQL_STATE_INITIAL);
    else
        PSQL_STATE(user)->xcdepth--;
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XC> rule xc_inside matches /{xcinside}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XC> rule xc_op_chars matches /{op_chars}+/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XC> rule xc_stars matches /\*+/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XC> rule xc_eof matches <<EOF>> { /* fall through, driver handles via state */ }

/* =================================================================== */
/* XB / XH / XQ / XUS body states (content + close)                    */
/*                                                                     */
/* In flex these were four separate state declarations but with the    */
/* same body rule; we collapse via multi-state qualifier.              */
/* =================================================================== */

<XB, XH, XQ, XUS> rule body_close matches /'/ {
    PsqlScanState s = PSQL_STATE(user);
    s->state_before_str_stop = s->start_state;
    psqlscan_emit(s, matched, matched_len);
    LEX_TRANSITION(PSQL_STATE_XQS);
    LEX_SKIP();
}

<XQ, XUS> rule body_xqdouble matches /''/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XB, XH, XQ, XUS> rule body_inside matches /[^']+/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XB> rule xb_eof matches <<EOF>> { /* fall through */ }
<XH> rule xh_eof matches <<EOF>> { /* fall through */ }
<XQ> rule xq_eof matches <<EOF>> { /* fall through */ }
<XUS> rule xus_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XE state: extended-escape quoted string                             */
/* =================================================================== */

<XE> rule xe_close matches /'/ {
    PsqlScanState s = PSQL_STATE(user);
    s->state_before_str_stop = s->start_state;
    psqlscan_emit(s, matched, matched_len);
    LEX_TRANSITION(PSQL_STATE_XQS);
    LEX_SKIP();
}

<XE> rule xe_double matches /''/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XE> rule xe_unicode     matches /{xeunicode}/     { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
<XE> rule xe_unicodefail matches /{xeunicodefail}/ { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
<XE> rule xe_hexesc      matches /{xehexesc}/      { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
<XE> rule xe_octesc      matches /{xeoctesc}/      { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
<XE> rule xe_escape      matches /{xeescape}/      { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
<XE> rule xe_inside      matches /{xeinside}/      { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
<XE> rule xe_lone_bs     matches /\\/             { psqlscan_emit(PSQL_STATE(user), matched, matched_len); LEX_SKIP(); }
<XE> rule xe_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XQS state: quote-stop continuation lookahead                        */
/* =================================================================== */

<XQS> rule xqs_continue matches /{quotecontinue}/ {
    PsqlScanState s = PSQL_STATE(user);
    state = s->state_before_str_stop;
    psqlscan_emit(s, matched, matched_len);
    LEX_SKIP();
}

/* {quotecontinuefail}|{other}: matches at least one byte; we rewind
 * everything and transition back to INITIAL. */
<XQS> rule xqs_fail matches /[\x00-\xff]/ {
    LEX_PUSHBACK(matched_len);
    state = PSQL_STATE_INITIAL;
    LEX_SKIP();
}

<XQS> rule xqs_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XDOLQ state: $foo$...$foo$                                          */
/* =================================================================== */

<XDOLQ> rule xdolq_close matches /{dolqdelim}/ {
    PsqlScanState s = PSQL_STATE(user);

    if (s->dolqstart != NULL &&
        (size_t) strlen(s->dolqstart) == matched_len &&
        memcmp(s->dolqstart, matched, matched_len) == 0) {
        free(s->dolqstart);
        s->dolqstart = NULL;
        psqlscan_emit(s, matched, matched_len);
        LEX_TRANSITION(PSQL_STATE_INITIAL);
        LEX_SKIP();
    } else {
        /* Not the matching close.  yyless(yyleng-1): put back final $. */
        LEX_PUSHBACK(1);
        psqlscan_emit(s, matched, matched_len - 1);
        LEX_SKIP();
    }
}

<XDOLQ> rule xdolq_inside matches /{dolqinside}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XDOLQ> rule xdolq_inside_failed matches /{dolqfailed}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XDOLQ> rule xdolq_dollar matches /\$/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XDOLQ> rule xdolq_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XD / XUI states: delimited identifier                               */
/* =================================================================== */

<XD, XUI> rule xd_close matches /"/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_TRANSITION(PSQL_STATE_INITIAL);
    LEX_SKIP();
}

<XD, XUI> rule xd_double matches /""/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XD, XUI> rule xd_inside matches /{xdinside}/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XD>  rule xd_eof  matches <<EOF>> { /* fall through */ }
<XUI> rule xui_eof matches <<EOF>> { /* fall through */ }
