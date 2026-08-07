/*-------------------------------------------------------------------------
 *
 * jsonpath_scan_lex_internal.h
 *	  Private interface between jsonpath_scan.lex (the Lime-generated
 *	  lexer) and jsonpath_scan.c (the parser-driver shim).
 *
 * Action bodies inside jsonpath_scan.lex include this header (via the
 * %include block).  It defines:
 *	- JP_TOK_* sentinel codes the .lex emits to signal driver-side
 *	  post-processing,
 *	- the JsonPathScanCtx struct that the .lex's action bodies
 *	  read/write through accessor macros,
 *	- prototypes for helpers the action bodies invoke
 *	  (jp_lex_addchar, jp_lex_addstring, jp_lex_parse_unicode, ...).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/jsonpath_scan_lex_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef JSONPATH_SCAN_LEX_INTERNAL_H
#define JSONPATH_SCAN_LEX_INTERNAL_H

#include "jsonpath_internal.h"

/* ----------------------------------------------------------------- */
/* Sentinel token codes emitted by jsonpath_scan.lex.  These live    */
/* above 1000 so they do not collide with parser tokens (gram.h's    */
/* TO_P=12 ... STR_INITCAP_P=64 plus DOLLAR=65 ... RBRACE=74).       */
/*                                                                   */
/* The .lex emits parser-side codes directly for terminals that take */
/* no payload (AND_P, OR_P, NOT_P, the comparison operators, etc.).  */
/* It emits sentinels for tokens that need driver-side post-process: */
/*	- JP_TOK_STRING_TAKE: STRING_P from accumulated buffer.          */
/*	- JP_TOK_VARIABLE_TAKE: VARIABLE_P from accumulated buffer.      */
/*	- JP_TOK_NUMERIC_TEXT / JP_TOK_INT_TEXT: numeric literal whose   */
/*	  text is the matched span (no escapes); driver palloc-copies    */
/*	  into JsonPathString.                                           */
/*	- JP_TOK_RAW_CHAR: single-char self/other punctuation; driver    */
/*	  maps text[0] -> token code (DOLLAR, AT, LBRACKET, ...).        */
/*	- JP_TOK_VARIABLE_BARE: a `$other+` run; matched span is the     */
/*	  full run including leading `$`; driver palloc-copies bytes 1.. */
/* ----------------------------------------------------------------- */

#define JP_TOK_BASE                  1000
#define JP_TOK_STRING_TAKE           (JP_TOK_BASE + 2)
#define JP_TOK_VARIABLE_TAKE         (JP_TOK_BASE + 3)
#define JP_TOK_NUMERIC_TEXT          (JP_TOK_BASE + 4)
#define JP_TOK_INT_TEXT              (JP_TOK_BASE + 5)
#define JP_TOK_RAW_CHAR              (JP_TOK_BASE + 6)
#define JP_TOK_VARIABLE_BARE         (JP_TOK_BASE + 7)

/* ----------------------------------------------------------------- */
/* JsonPathScanCtx: passed as `user` to the Lime lexer.  Action      */
/* bodies read/write its fields via accessor macros.                 */
/* ----------------------------------------------------------------- */

typedef struct JsonPathScanCtx
{
	JsonPathYyScanner *s;		/* the public scanner state (yytext, etc.) */
	struct Node *escontext;		/* soft-error context for ereturn/errsave */
} JsonPathScanCtx;

/* ----------------------------------------------------------------- */
/* Action-body accessor macros.  user is the void * Lime threads in. */
/* ----------------------------------------------------------------- */

#define JP_CTX(u)         ((JsonPathScanCtx *) (u))
#define JP_SCANNER(u)     (JP_CTX(u)->s)
#define JP_ESCONTEXT(u)   (JP_CTX(u)->escontext)

/* ----------------------------------------------------------------- */
/* Helper prototypes used by .lex action bodies.                     */
/*                                                                   */
/* The accumulator (scanstring) lives on JsonPathYyScanner; helpers  */
/* manipulate it directly so the same buffer carries through across  */
/* state transitions.                                                */
/* ----------------------------------------------------------------- */

extern void jp_lex_buf_init(void *user);
extern void jp_lex_addchar(void *user, char c);
extern void jp_lex_addstring(void *user, const char *src, size_t len);
extern void jp_lex_set_yytext(void *user, const char *p, size_t len);
extern void jp_lex_set_yytext_empty(void *user);
extern bool jp_lex_parse_unicode(void *user, const char *text, size_t len);
extern bool jp_lex_parse_hex_char(void *user, const char *text);

/*
 * Driver-side keyword lookup.  Matches checkKeyword() from the
 * pre-port scanner: case-insensitive ASCII compare against the
 * sorted keyword table, returning the keyword's parser token code
 * (e.g. STRICT_P) if found, else IDENT_P.  Operates on the current
 * accumulator contents (terminates the accumulator with NUL first).
 */
extern int	jp_lex_check_keyword(void *user);

#endif							/* JSONPATH_SCAN_LEX_INTERNAL_H */
