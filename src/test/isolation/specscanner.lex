/*-------------------------------------------------------------------------
 *
 * specscanner.lex
 *	  Lime lexer for the isolation test spec file format.
 *
 * Replaces the hand-rolled tokenizer that lived in specscanner.c
 * (~370 lines of state machine + char-class helpers) with a
 * declarative .lex source compiled by Lime v0.2.2's lexer subsystem.
 * Tokens emitted match specparse.h's #defines so the existing parser
 * works unchanged.
 *
 * Behavioural deltas from the previous hand-rolled scanner: none
 * intentional; the line-counting machinery for spec_yyerror moves
 * from a file-scope `yyline` variable into the lexer's
 * %lexer_extra_argument struct (extra->yyline), updated by the
 * newline-emitting rules.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/test/isolation/specscanner.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Spec.

%include {
#include "postgres_fe.h"

#include "specparse.h"			/* SQLBLOCK, IDENTIFIER, INTEGER, NOTICES,
								 * PERMUTATION, SESSION, SETUP, STEP,
								 * TEARDOWN, COMMA, LPAREN, RPAREN, STAR */

/*
 * Line counter for spec_yyerror.  Lives as a file-scope int in
 * specscanner.c; the driver resets it to 1 at the start of each parse.
 *
 * %lexer_extra_argument {SpecLexExtra *extra} is the cleaner shape for
 * this state, but Lime v0.2.2 parses the directive without threading
 * the `extra` binding through to action bodies (the generated .c
 * references `extra` as an undeclared identifier).  Filed as
 * P0-NEW-12; until that lands, we use a file-scope static here.
 */
extern int	spec_yyline;
}

/* Accumulator for both quoted-identifier and SQL-block content. */
%literal_buffer scanstr {
    type      char
    initial   1024
    grow      "*2"
    alloc     palloc
    realloc   repalloc
    free      pfree
}.

%exclusive_state QIDENT.
%exclusive_state SQLBLK.

/* ---- Pattern fragments ----
**
** flex source's character classes:
**   space       [ \t\r\f]    (no \n -- handled separately so we
**                              can bump yyline)
**   ident_start [A-Za-z_\200-\377]
**   ident_cont  [A-Za-z_\200-\377_0-9$]
*/
%pattern space       /[ \t\r\f]/.
%pattern ident_start /[A-Za-z_\x80-\xff]/.
%pattern ident_cont  /[A-Za-z_0-9$\x80-\xff]/.

/* ===== Whitespace, newlines, comments (INITIAL state) ===== */
rule ws       matches /{space}+/   { LEX_SKIP(); }
rule newline  matches /\n/         { spec_yyline++; LEX_SKIP(); }
rule comment  matches /#[^\n]*/    { LEX_SKIP(); }

/* ===== Case-sensitive keywords =====
**
** Each keyword has its own rule.  Lime's longest-match-wins +
** declaration-order tiebreak ensures these win over the generic
** ident rule below for exact matches.
*/
rule kw_notices     matches /notices/      { LEX_EMIT(NOTICES); }
rule kw_permutation matches /permutation/  { LEX_EMIT(PERMUTATION); }
rule kw_session     matches /session/      { LEX_EMIT(SESSION); }
rule kw_setup       matches /setup/        { LEX_EMIT(SETUP); }
rule kw_step        matches /step/         { LEX_EMIT(STEP); }
rule kw_teardown    matches /teardown/     { LEX_EMIT(TEARDOWN); }

/* ===== Generic identifier =====
**
** The driver's emit callback pg_strdups the matched span into yylval->str.
*/
rule ident matches /{ident_start}{ident_cont}*/  { LEX_EMIT(IDENTIFIER); }

/* ===== Integer literal =====
**
** The driver's emit callback runs atoi() over the matched span.
*/
rule integer matches /[0-9]+/  { LEX_EMIT(INTEGER); }

/* ===== Single-character punctuation ===== */
rule comma  matches /,/  { LEX_EMIT(COMMA); }
rule lparen matches /\(/ { LEX_EMIT(LPAREN); }
rule rparen matches /\)/ { LEX_EMIT(RPAREN); }
rule star   matches /\*/ { LEX_EMIT(STAR); }

/* ===== Quoted identifier "..." =====
**
** Open quote enters QIDENT state with a fresh accumulator.  Inside
** QIDENT, "" appends a literal " (xddouble), any non-quote / non-
** newline char appends one byte, a single closing quote takes the
** buffer and emits IDENTIFIER, a raw newline is an error, and EOF
** before close is an error.
*/
rule qident_open matches /"/ {
    LEX_BUF_START(scanstr);
    LEX_TRANSITION(SPEC_STATE_QIDENT);
    LEX_SKIP();
}

<QIDENT> rule qident_double matches /""/ {
    LEX_BUF_APPEND_CH(scanstr, '"');
    LEX_SKIP();
}

<QIDENT> rule qident_char matches /[^"\n]/ {
    LEX_BUF_APPEND_CH(scanstr, matched[0]);
    LEX_SKIP();
}

<QIDENT> rule qident_close matches /"/ {
    size_t n = LEX_BUF_LEN(scanstr);
    char *s = LEX_BUF_TAKE(scanstr);
    if (s == NULL) {
        LEX_ERROR_AT("oom in literal buffer take");
    } else {
        if (emit) emit(user, IDENTIFIER, s, n);
        free(s);
    }
    LEX_TRANSITION(SPEC_STATE_INITIAL);
    LEX_SKIP();
}

<QIDENT> rule qident_nl matches /\n/ {
    LEX_ERROR_AT("unexpected newline in quoted identifier");
}

<QIDENT> rule qident_eof matches <<EOF>> {
    LEX_ERROR_AT("unterminated quoted identifier");
}

/* ===== SQL block { ... } =====
**
** Opens by matching `{` plus any leading [ \t\r\f]* whitespace, which
** is not part of the block content (mirrors flex's leading-space
** stripping).  Inside SQLBLK, trailing `[ \t\r\f]*\}` closes the block
** with whitespace stripped (longest-match-wins handles the run-of-
** whitespace case the same as flex's `<sql>{space}*"}"` rule).  Lone
** whitespace inside the block is literal content; newlines bump
** yyline AND append a newline to the buffer.
*/
rule sqlblk_open matches /\{{space}*/ {
    LEX_BUF_START(scanstr);
    LEX_TRANSITION(SPEC_STATE_SQLBLK);
    LEX_SKIP();
}

<SQLBLK> rule sqlblk_close matches /{space}*\}/ {
    size_t n = LEX_BUF_LEN(scanstr);
    char *s = LEX_BUF_TAKE(scanstr);
    if (s == NULL) {
        LEX_ERROR_AT("oom in literal buffer take");
    } else {
        if (emit) emit(user, SQLBLOCK, s, n);
        free(s);
    }
    LEX_TRANSITION(SPEC_STATE_INITIAL);
    LEX_SKIP();
}

<SQLBLK> rule sqlblk_newline matches /\n/ {
    spec_yyline++;
    LEX_BUF_APPEND_CH(scanstr, '\n');
    LEX_SKIP();
}

<SQLBLK> rule sqlblk_char matches /./ {
    LEX_BUF_APPEND_CH(scanstr, matched[0]);
    LEX_SKIP();
}

<SQLBLK> rule sqlblk_eof matches <<EOF>> {
    LEX_ERROR_AT("unterminated sql block");
}

/* ===== Catch-all =====
**
** flex's `.` rule printed "syntax error at line N: unexpected
** character X" and exit(1).  Same shape via LEX_ERROR_AT; the driver
** translates LEX_ERROR + LexErrorMessage into the formatted output.
*/
rule unexpected matches /./ {
    LEX_ERROR_AT("unexpected character");
}
