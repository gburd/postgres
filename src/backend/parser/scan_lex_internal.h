/*-------------------------------------------------------------------------
 *
 * scan_lex_internal.h
 *	  Private interface between scan.lex (the Lime-generated lexer) and
 *	  scan.c (the parser-driver shim).
 *
 * Action bodies inside scan.lex include this header (via the
 * %include block).  It defines:
 *	- SCAN_TOK_* sentinel codes the .lex emits to signal driver-side
 *	  post-processing,
 *	- the EmitContext struct user pointer the .lex's action bodies
 *	  read/write through SCAN_LEX_* accessor macros,
 *	- prototypes for helpers the action bodies invoke
 *	  (scan_lex_op_keep, scan_lex_handle_unicode, ...).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/parser/scan_lex_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SCAN_LEX_INTERNAL_H
#define SCAN_LEX_INTERNAL_H

#include <stdint.h>

#include "common/string.h"
#include "gramparse.h"
#include "mb/pg_wchar.h"
#include "nodes/miscnodes.h"
#include "parser/parser.h"
#include "parser/scansup.h"
#include "port/pg_bitutils.h"
#include "utils/builtins.h"

/* ----------------------------------------------------------------- */
/* Sentinel token codes emitted by scan.lex.  These live above 1000  */
/* so they don't collide with parser tokens (gram.h tokens start at  */
/* %first_token 258).                                                */
/* ----------------------------------------------------------------- */

#define SCAN_TOK_BASE                  1000
#define SCAN_TOK_RAW_CHAR              (SCAN_TOK_BASE + 1)	/* single self/other
															 * char */
#define SCAN_TOK_TYPECAST              (SCAN_TOK_BASE + 2)	/* :: */
#define SCAN_TOK_DOT_DOT               (SCAN_TOK_BASE + 3)	/* .. */
#define SCAN_TOK_COLON_EQUALS          (SCAN_TOK_BASE + 4)	/* := */
#define SCAN_TOK_OP                    (SCAN_TOK_BASE + 5)	/* operator */
#define SCAN_TOK_PARAM                 (SCAN_TOK_BASE + 6)	/* $1 */
#define SCAN_TOK_PARAM_JUNK            (SCAN_TOK_BASE + 7)
#define SCAN_TOK_ICONST_DEC            (SCAN_TOK_BASE + 8)
#define SCAN_TOK_ICONST_HEX            (SCAN_TOK_BASE + 9)
#define SCAN_TOK_ICONST_OCT            (SCAN_TOK_BASE + 10)
#define SCAN_TOK_ICONST_BIN            (SCAN_TOK_BASE + 11)
#define SCAN_TOK_HEXFAIL               (SCAN_TOK_BASE + 12)
#define SCAN_TOK_OCTFAIL               (SCAN_TOK_BASE + 13)
#define SCAN_TOK_BINFAIL               (SCAN_TOK_BASE + 14)
#define SCAN_TOK_FCONST_NUMERIC        (SCAN_TOK_BASE + 15)
#define SCAN_TOK_FCONST_REAL           (SCAN_TOK_BASE + 16)
#define SCAN_TOK_TRAILING_JUNK_NUM     (SCAN_TOK_BASE + 17)
#define SCAN_TOK_IDENT_RAW             (SCAN_TOK_BASE + 18) /* unquoted ident;
															 * driver does kw lookup
															 * + downcase */
#define SCAN_TOK_IDENT_QUOTED          (SCAN_TOK_BASE + 19) /* xd: takes literal
															 * buffer */
#define SCAN_TOK_NCHAR                 (SCAN_TOK_BASE + 20) /* n'... -- look up
															 * "nchar" keyword, emit
															 * as ident "n"
															 * otherwise */
#define SCAN_TOK_BCONST                (SCAN_TOK_BASE + 21)
#define SCAN_TOK_XCONST                (SCAN_TOK_BASE + 22)
#define SCAN_TOK_SCONST                (SCAN_TOK_BASE + 23)
#define SCAN_TOK_USCONST               (SCAN_TOK_BASE + 24)
#define SCAN_TOK_UIDENT                (SCAN_TOK_BASE + 25)
#define SCAN_TOK_BAD_UNICODE_ESCAPE    (SCAN_TOK_BASE + 26)
#define SCAN_TOK_BAD_HEX_ESCAPE        (SCAN_TOK_BASE + 27)
#define SCAN_TOK_BAD_SURROGATE         (SCAN_TOK_BASE + 28)

/* ----------------------------------------------------------------- */
/* EmitContext: passed as `user` to the Lime lexer.  Action bodies   */
/* read/write its fields via SCAN_LEX_* accessor macros.             */
/* ----------------------------------------------------------------- */

typedef struct ScanLexCtx
{
	core_yy_extra_type *extra;	/* the public scanner extra */
	const char *scanbuf;		/* pointer to the bytes being lexed */
	int			scanbuflen;

	/* Last starting offset (set on every match by SCAN_LEX_SET_LOC). */
	int			cur_loc;

	/* Compound-token start (set on quote/dollar-quote/identifier open). */
	int			litstart;

	/* For compound tokens, end_pos written before emit. */
	int			compound_start;
	int			compound_end;
	bool		compound_pending;	/* true: emit cb should use compound_*  */

	/* xc state nesting depth (independent of literal buffer). */
	int			xcdepth;

	/* xqs state: which body state we returned from. */
	int			prev_state;

	/* Current open dollar-quote tag (palloc'd). */
	char	   *dolqstart;

	/* Fields shadowing core_yy_extra_type for action-body convenience. */
	bool		saw_non_ascii;
	int32		utf16_first_part;

	/* FIFO of pre-scanned tokens (built during scanner_init). */
	struct ScanToken *tokens;
	int			ntokens;
	int			cap;

	/* If an action body raised a soft error, the driver picks it up. */
	bool		had_error;
	int			err_pos;
	int			err_end;
	char		err_msg[256];
} ScanLexCtx;

/* ----------------------------------------------------------------- */
/* Per-token entry in the FIFO.                                       */
/* ----------------------------------------------------------------- */

typedef struct ScanToken
{
	int			code;			/* parser-side token code (gram.h) */
	int			start;			/* byte offset in scanbuf */
	int			end;			/* byte offset of token's end (where to stuff
								 * '\0') */
	core_YYSTYPE val;
} ScanToken;

/* ----------------------------------------------------------------- */
/* Action-body accessor macros.  user is the void * Lime threads in. */
/* ----------------------------------------------------------------- */

#define SCAN_LEX_CTX(u)              ((ScanLexCtx *) (u))
#define SCAN_LEX_OFFSET(p)           ((int) ((p) - SCAN_LEX_CTX(user)->scanbuf))
#define SCAN_LEX_SET_LOC(p)          (SCAN_LEX_CTX(user)->cur_loc = SCAN_LEX_OFFSET(p))
#define SCAN_LEX_TOTAL(u)            (SCAN_LEX_CTX(u)->scanbuflen)
#define SCAN_LEX_XCDEPTH(u)          (SCAN_LEX_CTX(u)->xcdepth)
#define SCAN_LEX_PREV_STATE(u)       (SCAN_LEX_CTX(u)->prev_state)
#define SCAN_LEX_LITSTART(u)         (SCAN_LEX_CTX(u)->litstart)
#define SCAN_LEX_DOLQSTART(u)        (SCAN_LEX_CTX(u)->dolqstart)
#define SCAN_LEX_SAW_NON_ASCII(u)    (SCAN_LEX_CTX(u)->saw_non_ascii)

/* ----------------------------------------------------------------- */
/* Helper prototypes used by .lex action bodies.                     */
/* ----------------------------------------------------------------- */

extern size_t scan_lex_op_keep(const char *text, size_t len);
extern void scan_lex_set_compound_end(void *user, int start, int end);

/*
 * Unicode-escape handling helpers.  All take the EmitContext (cast
 * via SCAN_LEX_CTX) and a position for error reporting.  They
 * mutate the literal buffer indirectly via the lexer's emit path
 * (helpers must use addlit-style routines exposed below since we
 * can't easily reach LEX_BUF_APPEND from outside an action body).
 */

/*
 * Returns 1 if a high surrogate was buffered (caller must transition to
 * XEU); 0 if a complete codepoint was appended; -1 on error (already
 * raised).
 */
extern int	scan_lex_handle_unicode(void *user, int pos, char32_t c);
extern void scan_lex_handle_xeu_second(void *user, int pos, char32_t c);
extern void scan_lex_handle_xeescape(void *user, int pos, unsigned char c);
extern void scan_lex_handle_xehexesc(void *user, const char *text, size_t len);
extern void scan_lex_handle_xeoctesc(void *user, const char *text, size_t len);

/*
 * The buffer-append helpers exposed for the helpers above so they can
 * indirectly add to the literal buffer.  Implemented in scan.c using
 * the Lime literal-buffer macros that are reachable only inside
 * action bodies; we approximate with a parallel C-side accumulator.
 */
extern void scan_lex_addlitchar(void *user, unsigned char c);
extern void scan_lex_addlit(void *user, const char *text, size_t len);

/*
 * Take the C-side accumulator's contents into a freshly-palloc'd string,
 * resetting the accumulator.
 */
extern char *scan_lex_litbuf_take(void *user, size_t *out_len);
extern void scan_lex_litbuf_start(void *user);
extern size_t scan_lex_litbuf_len(void *user);

/*
 * Used by xeu helpers to add a previously-decoded codepoint via
 * pg_unicode_to_server (with proper error positioning).
 */
extern void scan_lex_addunicode(void *user, int pos, char32_t c);

#endif							/* SCAN_LEX_INTERNAL_H */
