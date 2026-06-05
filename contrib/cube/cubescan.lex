/*-------------------------------------------------------------------------
 *
 * cubescan.lex
 *	  Lime lexer for the cube data type's input syntax.
 *
 * Replaces contrib/cube/cubescan.l (~150 lines flex).  The pattern set
 * is small and stateless: numeric literals (integer/real/exponential
 * float/infinity/NaN), single-character punctuation, whitespace
 * skipping, and a catch-all error.  No state machine, no buffer
 * accumulation -- every match is one token whose value is the matched
 * text, mirroring the original `*yylval = yytext` flex actions.
 *
 * Each rule LEX_EMITs the corresponding bison-era token code from
 * cubeparse.h.  The driver in cubeparse_driver.c (see meson.build)
 * receives (token, text, len) callbacks and pstrdups the text into
 * yylval->str before pushing to the Lime parser.  YYSTYPE for cube is
 * `char *` (cubedata.h:64), so each token value is just a C string
 * holding the lexeme.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * contrib/cube/cubescan.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Cube.

%include {
#include "postgres.h"

#include "cubedata.h"
#include "cubeparse.h"		/* CUBEFLOAT, O_BRACKET, C_BRACKET,
							 * O_PAREN, C_PAREN, COMMA */
}

/* ---- Pattern fragments mirroring cubescan.l's character classes ---- */
%pattern n         /[0-9]+/.
%pattern integer   /[+-]?{n}/.
%pattern real      /[+-]?({n}\.{n}?|\.{n})/.
%pattern float_p   /({integer}|{real})([eE]{integer})?/.
%pattern infinity  /[+-]?[iI][nN][fF]([iI][nN][iI][tT][yY])?/.
%pattern NaN       /[nN][aA][nN]/.

/* ===== Numeric literals (all three flex CUBEFLOAT rules) ===== */
rule float_lit matches /{float_p}/  { LEX_EMIT(CUBEFLOAT); }
rule infinity  matches /{infinity}/ { LEX_EMIT(CUBEFLOAT); }
rule nan       matches /{NaN}/      { LEX_EMIT(CUBEFLOAT); }

/* ===== Bracket-style cube delimiters =====
**
** Original flex source maps both \[/\] (square) and \(/\) (round) to
** distinct tokens, with the brackets standing in as outer cube
** delimiters and the parens as inner point delimiters.
*/
rule lbracket matches /\[/ { LEX_EMIT(O_BRACKET); }
rule rbracket matches /\]/ { LEX_EMIT(C_BRACKET); }
rule lparen   matches /\(/ { LEX_EMIT(O_PAREN); }
rule rparen   matches /\)/ { LEX_EMIT(C_PAREN); }
rule comma    matches /,/  { LEX_EMIT(COMMA); }

/* ===== Whitespace ===== */
rule ws matches /[ \t\n\r\f\v]+/ { LEX_SKIP(); }

/* ===== Catch-all error =====
**
** flex's `.` rule returned `yytext[0]` so the parser saw an
** unexpected single-char token and emitted "syntax error at or near
** ...".  Lime's LEX_ERROR_AT terminates the LexFeedBytes call with
** CUBE_LEX_ERROR; the driver translates that into the same errsave
** path cube_yyerror takes.
*/
rule unexpected matches /./ {
    LEX_ERROR_AT("syntax error: unexpected character");
}
