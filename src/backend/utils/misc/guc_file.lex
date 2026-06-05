/*-------------------------------------------------------------------------
 *
 * guc-file.lex
 *	  Lime lexer for the PostgreSQL configuration-file grammar.
 *
 * Replaces the hand-rolled tokenizer that lived in guc-file.c (~470
 * lines of state machine + char-class helpers + ad-hoc longest-match
 * arbitration) with a declarative .lex source compiled by Lime
 * v0.2.2's lexer subsystem.  guc-file.c keeps its driver half
 * (ProcessConfigFile / ParseConfigFile / ParseConfigFp /
 * ParseConfigDirectory) plus a parser-driver shim that consumes
 * tokens from the Lime-emitted FIFO.
 *
 * Tokens emitted match the existing internal enum in guc-file.c:
 *   GUC_ID=1 GUC_STRING=2 GUC_INTEGER=3 GUC_REAL=4 GUC_EQUALS=5
 *   GUC_UNQUOTED_STRING=6 GUC_QUALIFIED_ID=7 GUC_EOF=0 GUC_EOL=99
 *   GUC_ERROR=100
 *
 * Quoted strings are NOT decoded inside the lexer; the matched span
 * (including the surrounding single quotes and any \\. or '' escapes)
 * is what the existing ParseConfigFp expects, since it calls
 * DeescapeQuotedString on the raw text.  No %literal_buffer needed.
 *
 * Portions Copyright (c) 2000-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/misc/guc-file.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Guc.

%include {
#include "postgres.h"

/*
 * Token codes match the internal enum in guc-file.c.  Defined here in
 * the %include block so the .lex source has direct access to them
 * without going through a shared header (the enum is private to
 * guc-file.c).
 */
#define GUC_TOK_ID                  1
#define GUC_TOK_STRING              2
#define GUC_TOK_INTEGER             3
#define GUC_TOK_REAL                4
#define GUC_TOK_EQUALS              5
#define GUC_TOK_UNQUOTED_STRING     6
#define GUC_TOK_QUALIFIED_ID        7
#define GUC_TOK_EOL                 99
#define GUC_TOK_ERROR               100
}

/* ---- Pattern fragments mirroring the flex regexes ---- */
%pattern digit         /[0-9]/.
%pattern hexdigit      /[0-9A-Fa-f]/.
%pattern unit_letter   /[A-Za-z]/.
%pattern letter        /[A-Za-z_\x80-\xff]/.
%pattern letter_digit  /[A-Za-z_0-9\x80-\xff]/.

/* SIGN, EXPONENT, INTEGER, REAL composites. */
%pattern sign       /[+-]/.
%pattern exponent   /[Ee]{sign}?{digit}+/.

/* ===== Whitespace and comments =====
**
** flex source:
**   [ \t\r]+    -- ignore
**   #.*         -- comment
**   \n          ConfigFileLineno++; return GUC_EOL;
*/
rule ws       matches /[ \t\r]+/   { LEX_SKIP(); }
rule comment  matches /#[^\n]*/    { LEX_SKIP(); }
rule eol      matches /\n/         { LEX_EMIT(GUC_TOK_EOL); }

/* ===== Equals ===== */
rule equals   matches /=/  { LEX_EMIT(GUC_TOK_EQUALS); }

/* ===== Quoted string (single-quoted, with \\. and '' escapes) =====
**
** flex regex:  \'([^'\\\n]|\\.|\'\')*\'
**
** Lime's regex engine is byte-oriented like flex's; the regex
** translates directly.  The matched span includes the outer quotes
** and any escape sequences; ParseConfigFp's DeescapeQuotedString
** decodes after.
*/
rule sqstring matches /'([^'\\\n]|\\.|'')*'/ { LEX_EMIT(GUC_TOK_STRING); }

/* ===== Letter-initial: ID, QUALIFIED_ID, UNQUOTED_STRING =====
**
** flex source places these in the order ID > QUALIFIED_ID > STRING >
** UNQUOTED_STRING; longest-match-wins, tiebreak by declaration order.
** We declare in the same order so observable behaviour is identical.
**
**   ID              {LETTER}{LETTER_OR_DIGIT}*
**   QUALIFIED_ID    {ID}\.{ID}
**   UNQUOTED_STRING {LETTER}({LETTER_OR_DIGIT}|[-._:/])*
*/
rule id              matches /{letter}{letter_digit}*/                            { LEX_EMIT(GUC_TOK_ID); }
rule qualified_id    matches /{letter}{letter_digit}*\.{letter}{letter_digit}*/   { LEX_EMIT(GUC_TOK_QUALIFIED_ID); }
rule unquoted_string matches /{letter}({letter_digit}|[-._:\/])*/                 { LEX_EMIT(GUC_TOK_UNQUOTED_STRING); }

/* ===== Numbers: INTEGER and REAL =====
**
** flex source:
**   INTEGER {SIGN}?({DIGIT}+|0x{HEXDIGIT}+){UNIT_LETTER}*
**   REAL    {SIGN}?{DIGIT}*"."{DIGIT}*{EXPONENT}?
**
** INTEGER is listed before REAL so ties resolve to INTEGER.  In
** practice they cannot tie (INTEGER has no '.' and REAL requires one);
** the order is preserved for documentation.
*/
rule integer matches /{sign}?({digit}+|0[xX]{hexdigit}+){unit_letter}*/  { LEX_EMIT(GUC_TOK_INTEGER); }
rule real    matches /{sign}?{digit}*\.{digit}*{exponent}?/              { LEX_EMIT(GUC_TOK_REAL); }

/* ===== Catch-all =====
**
** flex's catch-all `.` rule emitted GUC_ERROR with single-character
** text.  Same shape via LEX_EMIT(GUC_TOK_ERROR) -- the driver formats
** the "syntax error in file ... near token X" message.
**
** Note: the unterminated-quoted-string case in the original
** hand-rolled scanner returned GUC_ERROR for the leading "'".  In
** the Lime port, an unterminated quote falls through to the
** catch-all rule (since the sqstring pattern requires a closing
** quote on the same line); same observable outcome.
*/
rule unexpected matches /./ { LEX_EMIT(GUC_TOK_ERROR); }
