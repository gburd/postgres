/*-------------------------------------------------------------------------
 *
 * segscan.lex
 *	  Lime lexer for the seg data type's input syntax.
 *
 * Replaces contrib/seg/segscan.l (146 lines flex).  The pattern set
 * is small and stateless: numeric literals (integer/real/exponential
 * float), range markers (.. or ...), plumin markers ('+-' or (+-)),
 * single-char extensions (< > ~), whitespace skipping, and a
 * catch-all error.  No state machine, no buffer accumulation -- every
 * match is one token whose value is the matched text, mirroring the
 * original `yylval->text = yytext` flex actions.
 *
 * Each rule LEX_EMITs the bison-era token code from segparse.h.  The
 * driver in segparse_driver.c receives (token, text, len) callbacks
 * and pstrdups the text into yylval->text before pushing to the Lime
 * parser.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * contrib/seg/segscan.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Seg.

%include {
#include "postgres.h"

#include "seg_gram_yytype.h"
#include "segdata.h"
#include "segparse.h"		/* SEGFLOAT, RANGE, PLUMIN, EXTENSION */
}

/* ---- Pattern fragments mirroring segscan.l ---- */
%pattern integer  /[+-]?[0-9]+/.
%pattern real     /[+-]?[0-9]+\.[0-9]+/.
%pattern float_p  /({integer}|{real})([eE]{integer})?/.
%pattern range_p  /\.\.\.?/.
%pattern plumin_p /'\+-'|\(\+-\)/.

/* ===== Range and plumin markers (must precede float to avoid `..`
**       being eaten as a malformed real) ===== */
rule range  matches /{range_p}/  { LEX_EMIT(RANGE); }
rule plumin matches /{plumin_p}/ { LEX_EMIT(PLUMIN); }

/* ===== Numeric literals ===== */
rule float_lit matches /{float_p}/ { LEX_EMIT(SEGFLOAT); }

/* ===== Single-char extensions =====
**
** flex source emitted EXTENSION with yylval->text pointing at a
** static string literal "<", ">", "~".  We pstrdup the matched
** byte in the driver -- functionally identical, one extra palloc per
** seg input.
*/
rule lt    matches /</   { LEX_EMIT(EXTENSION); }
rule gt    matches />/   { LEX_EMIT(EXTENSION); }
rule tilde matches /~/   { LEX_EMIT(EXTENSION); }

/* ===== Whitespace ===== */
rule ws matches /[ \t\n\r\f\v]+/ { LEX_SKIP(); }

/* ===== Catch-all error =====
**
** flex's `.` rule returned `yytext[0]` (an unexpected single-char
** token) which the parser rejected via syntax_error.  Lime's
** LEX_ERROR_AT terminates the LexFeedBytes call with SEG_LEX_ERROR;
** the driver translates that into the same errsave path seg_yyerror
** takes.
*/
rule unexpected matches /./ {
    LEX_ERROR_AT("syntax error: unexpected character");
}
