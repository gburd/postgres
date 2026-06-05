/*-------------------------------------------------------------------------
 *
 * exprscan.lex
 *	  Lime lexer for pgbench's simple expression syntax.
 *
 * Replaces the EXPR-state half of the hand-rolled tokenizer that lived
 * in exprscan.c (~280 lines of state machine + char-class helpers) with
 * a declarative .lex source compiled by Lime v0.2.2's lexer subsystem.
 * Tokens emitted match exprparse.h's #defines so the existing Lime
 * grammar (exprparse.lime) accepts them unchanged.
 *
 * INITIAL-state behaviour (whitespace-separated word lex used by
 * expr_lex_one_word) remains hand-rolled in exprscan.c -- it needs to
 * write into PsqlScanState->output_buf one word at a time, which is
 * a poor fit for Lime's pre-scan FIFO model.
 *
 * Quoted strings are NOT used by pgbench's expression syntax, so no
 * %literal_buffer is needed.  Case-insensitive keyword matching is
 * deferred to the driver: the funcname rule emits a sentinel and the
 * driver does the case-insensitive lookup against a small keyword
 * table, mirroring the retired hand-rolled MATCH_KW behaviour.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/pgbench/exprscan.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Expr.

%include {
#include "postgres_fe.h"

#include "exprparse.h"			/* parser-side token codes */

/*
 * Sentinels for lexer-internal tokens not in exprparse.h.  All values
 * are well above the parser's symbolic tokens (1..40 from exprparse.h).
 */
#define EXPR_TOK_EOL          1000
#define EXPR_TOK_FUNC_OR_KW   1001
#define EXPR_TOK_UNEXPECTED   1002
}

/* ---- Pattern fragments ---- */
%pattern alpha       /[A-Za-z_\x80-\xff]/.
%pattern alnum       /[A-Za-z_0-9\x80-\xff]/.
%pattern digit       /[0-9]/.
%pattern space       /[ \t\r\f\v]/.

/* ===== Whitespace and continuation =====
**
** flex source:
**   {space}+        -- ignore
**   \\{NL}          -- ignore (line continuation)
**   {newline}       -- yield 0 (signals end-of-expression to the driver)
*/
rule ws           matches /{space}+/    { LEX_SKIP(); }
rule continuation matches /\\\r?\n/     { LEX_SKIP(); }

/* Newline ends the expression.  Emit a sentinel and stop the lexer;
** the driver translates that into yylex returning 0 with
** last_was_newline = true. */
rule eol matches /\n/ {
    LEX_EMIT(EXPR_TOK_EOL);
    LEX_TERMINATE();
}

/* ===== Two-character operators =====
**
** Declared first so longest-match-wins picks them over the
** corresponding single-char rules (`<`, `>`, `!`, `=`).
*/
rule ne_op_a  matches /<>/ { LEX_EMIT(NE_OP); }
rule ne_op_b  matches /!=/ { LEX_EMIT(NE_OP); }
rule le_op    matches /<=/ { LEX_EMIT(LE_OP); }
rule ge_op    matches />=/ { LEX_EMIT(GE_OP); }
rule ls_op    matches /<</ { LEX_EMIT(LS_OP); }
rule rs_op    matches />>/ { LEX_EMIT(RS_OP); }

/* ===== Single-character operators and punctuation ===== */
rule plus     matches /\+/ { LEX_EMIT(PLUS); }
rule minus    matches /-/  { LEX_EMIT(MINUS); }
rule star     matches /\*/ { LEX_EMIT(STAR); }
rule slash    matches /\// { LEX_EMIT(SLASH); }
rule percent  matches /%/  { LEX_EMIT(PERCENT); }
rule eq       matches /=/  { LEX_EMIT(EQ); }
rule lt       matches /</  { LEX_EMIT(LT); }
rule gt       matches />/  { LEX_EMIT(GT); }
rule bitor_t  matches /\|/ { LEX_EMIT(BITOR); }
rule bitand_t matches /&/  { LEX_EMIT(BITAND); }
rule bitxor_t matches /#/  { LEX_EMIT(BITXOR); }
rule tilde_t  matches /~/  { LEX_EMIT(TILDE); }
rule lparen   matches /\(/ { LEX_EMIT(LPAREN); }
rule rparen   matches /\)/ { LEX_EMIT(RPAREN); }
rule comma    matches /,/  { LEX_EMIT(COMMA); }

/* ===== Variable: :name =====
**
** flex regex:  :{alnum}+
** Driver pstrdups the name without the leading colon.
*/
rule variable matches /:{alnum}+/ { LEX_EMIT(VARIABLE); }

/* ===== Numeric literals =====
**
** flex source emitted DOUBLE_CONST for any literal containing `.` or a
** valid `[eE]` exponent, INTEGER_CONST otherwise.  We split into four
** rules and rely on Lime's longest-match-wins arbitration: the DOUBLE
** rules will outscore INTEGER whenever a `.` or valid exponent is
** present.  All four are declared in the order of Bison's flex source
** for documentation; arbitration is by match length, not declaration
** order, in the absence of ties.
**
**   {digit}+\.{digit}*([eE][+-]?{digit}+)?  -- 1., 1.5, 1.5e6
**   \.{digit}+([eE][+-]?{digit}+)?           -- .5, .5e6
**   {digit}+[eE][+-]?{digit}+                -- 1e6
**   {digit}+                                 -- INTEGER
**
** MAXINT_PLUS_ONE_CONST detection (the special "9223372036854775808"
** literal) is handled in the driver: when an INTEGER_CONST text equals
** that exact 19-byte string, the driver remaps to MAXINT_PLUS_ONE_CONST.
*/
rule double1 matches /{digit}+\.{digit}*([eE][+-]?{digit}+)?/ { LEX_EMIT(DOUBLE_CONST); }
rule double2 matches /\.{digit}+([eE][+-]?{digit}+)?/         { LEX_EMIT(DOUBLE_CONST); }
rule double3 matches /{digit}+[eE][+-]?{digit}+/              { LEX_EMIT(DOUBLE_CONST); }
rule integer matches /{digit}+/                               { LEX_EMIT(INTEGER_CONST); }

/* ===== Identifier / keyword =====
**
** flex source had per-keyword rules (case-insensitive) ahead of a
** generic {alpha}{alnum}* function-name rule.  Lime's regex flavour
** has no case-insensitive flag, so we emit a single sentinel for any
** letter-initial identifier and let the driver's emit callback do the
** case-insensitive lookup against a fixed keyword table.  Behaviour
** is identical: longest-match-wins fires the same way (the keyword
** table is exact-length-match), and ties resolve to the keyword.
*/
rule funcname matches /{alpha}{alnum}*/ { LEX_EMIT(EXPR_TOK_FUNC_OR_KW); }

/* ===== Catch-all =====
**
** Any byte that didn't match something above is reported by the
** driver via expr_yyerror_more("unexpected character", ...).  We emit
** a sentinel here; the driver longjmps out of pre-scanning via
** ereport(ERROR), so no further tokens are observed.
*/
rule unexpected matches /./ { LEX_EMIT(EXPR_TOK_UNEXPECTED); }
