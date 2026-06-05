/*-------------------------------------------------------------------------
 *
 * bootscanner.lex
 *	  Lime lexer for PostgreSQL bootstrap (BKI) input.
 *
 * Replaces hand-rolled bootscanner.c's tokenizer (~400 lines of state
 * machine) with a declarative .lex source compiled by Lime v0.2.1's
 * lexer subsystem.  bootscanner.c shrinks to just the parser-driver
 * shim (LexFeedBytes loop wrapping boot_yyAlloc / boot_yyLoc /
 * boot_yyFree).
 *
 * Tokens emitted match bootparse.h's bison-era #defines (OPEN,
 * XCLOSE, ID, COMMA, ...) so the existing parser works unchanged.
 * Each keyword rule explicitly LEX_EMITs its parser-side token code
 * rather than relying on auto-emit -- ordering by match-length plus
 * declaration order alone would suffice, but the explicit form
 * documents which scanner rule maps to which parser token.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/bootstrap/bootscanner.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Boot.

%include {
#include "postgres.h"

#include "bootparse.h"			/* COMMA, EQUALS, LPAREN, RPAREN, ID,
								 * NULLVAL, OPEN, XCLOSE, XCREATE, OBJ_ID,
								 * XBOOTSTRAP, XSHARED_RELATION,
								 * XROWTYPE_OID, INSERT_TUPLE, XDECLARE,
								 * XBUILD, INDICES, UNIQUE, INDEX, ON,
								 * USING, XTOAST, XFORCE, XNOT, XNULL */

extern char *DeescapeQuotedString(const char *s);
}

/* ---- Pattern fragments ---- */
%pattern id    /[-A-Za-z0-9_]+/.
%pattern sid   /'([^']|'')*'/.

/* ===== Whitespace and comments ===== */

rule ws       matches /[ \t\r\f\v]+/  { LEX_SKIP(); }
rule newline  matches /\n/            { LEX_SKIP(); }
rule comment  matches /#[^\n]*/       { LEX_SKIP(); }

/* ===== Single-character punctuation =====
**
** flex source:
**   ","  *yylval = ...; return COMMA;
**   "="  *yylval = ...; return EQUALS;
**   ...
*/

rule comma    matches /,/  { LEX_EMIT(COMMA); }
rule equals   matches /=/  { LEX_EMIT(EQUALS); }
rule lparen   matches /\(/ { LEX_EMIT(LPAREN); }
rule rparen   matches /\)/ { LEX_EMIT(RPAREN); }

/* ===== Reserved keywords =====
**
** Each keyword has its own rule.  Lime's longest-match-wins +
** declaration-order tiebreak ensures these win over the generic
** `ident` rule below for exact matches.
**
** _null_ is the only keyword whose token (NULLVAL) is a no-value
** marker; the rest set yylval->kw to the keyword's text in the
** emit callback (see bootscanner.c).
*/

rule kw_open            matches /open/             { LEX_EMIT(OPEN); }
rule kw_close           matches /close/            { LEX_EMIT(XCLOSE); }
rule kw_create          matches /create/           { LEX_EMIT(XCREATE); }
rule kw_OID             matches /OID/              { LEX_EMIT(OBJ_ID); }
rule kw_bootstrap       matches /bootstrap/        { LEX_EMIT(XBOOTSTRAP); }
rule kw_shared_relation matches /shared_relation/  { LEX_EMIT(XSHARED_RELATION); }
rule kw_rowtype_oid     matches /rowtype_oid/      { LEX_EMIT(XROWTYPE_OID); }
rule kw_insert          matches /insert/           { LEX_EMIT(INSERT_TUPLE); }
rule kw_declare         matches /declare/          { LEX_EMIT(XDECLARE); }
rule kw_build           matches /build/            { LEX_EMIT(XBUILD); }
rule kw_indices         matches /indices/          { LEX_EMIT(INDICES); }
rule kw_unique          matches /unique/           { LEX_EMIT(UNIQUE); }
rule kw_index           matches /index/            { LEX_EMIT(INDEX); }
rule kw_on              matches /on/               { LEX_EMIT(ON); }
rule kw_using           matches /using/            { LEX_EMIT(USING); }
rule kw_toast           matches /toast/            { LEX_EMIT(XTOAST); }
rule kw_FORCE           matches /FORCE/            { LEX_EMIT(XFORCE); }
rule kw_NOT             matches /NOT/              { LEX_EMIT(XNOT); }
rule kw_NULL            matches /NULL/             { LEX_EMIT(XNULL); }
rule kw_null_marker     matches /_null_/           { LEX_EMIT(NULLVAL); }

/* ===== Generic identifier =====
**
** Falls through to `ID`.  yylval->str gets the matched text via
** pstrdup; that's the driver's responsibility.
*/

rule ident matches /{id}/ { LEX_EMIT(ID); }

/* ===== Single-quoted string =====
**
** Outer quotes plus possibly-escaped contents (an embedded ''
** is the escape).  Driver runs DeescapeQuotedString on the
** matched span before passing to the parser.
*/

rule sqstring matches /{sid}/ { LEX_EMIT(ID); }

/* ===== Catch-all error =====
**
** flex's `.` rule matched any single char and called elog(ERROR).
** LEX_ERROR_AT terminates the LexFeedBytes call with BOOT_LEX_ERROR;
** the driver translates that to ereport.
*/

rule unexpected matches /./ {
    LEX_ERROR_AT("syntax error: unexpected character");
}
