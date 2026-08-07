/*-------------------------------------------------------------------------
 *
 * psqlscan_emit.h
 *	  Private interface between psqlscan.lex / psqlscanslash.lex (the
 *	  Lime-generated lexers) and the psqlscan.c / psqlscanslash.c
 *	  driver shims.
 *
 * Action bodies inside the .lex sources include this header (via the
 * %include block).  It defines the EmitContext struct passed as the
 * `user` cookie to LexFeedBytes(), plus accessor macros and helper
 * prototypes used from action bodies.
 *
 * The Strategy-D driver model: each psql_scan() / psql_scan_slash_*
 * call allocates a fresh Psql_Lexer (or Slash_Lexer), sets its state
 * to PsqlScanState->start_state, feeds the remaining bytes of the
 * current buffer, and lets the lexer run until either:
 *   - all bytes are consumed (driver pops buffer_stack or returns EOL),
 *   - an action body calls LEX_TERMINATE() after setting ctx->stop_kind
 *     to STOP_SEMI / STOP_BACKSLASH / STOP_VAR_EXPAND / STOP_VAR_RECURSE
 *     / STOP_OK (slash scanner end-of-arg case).
 *
 * On LEX_TERMINATE, action bodies record `consumed` = (matched - buf) +
 * matched_len (- pushback if any) so the driver can advance the
 * PsqlScanState cursor.  Without LEX_TERMINATE, consumed = total bytes
 * fed (all consumed naturally).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/include/fe_utils/psqlscan_emit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PSQLSCAN_EMIT_H
#define PSQLSCAN_EMIT_H

#include "fe_utils/psqlscan.h"
#include "fe_utils/psqlscan_int.h"

/* ----------------------------------------------------------------- */
/* Stop-kind codes set by action bodies before LEX_TERMINATE().      */
/* ----------------------------------------------------------------- */

typedef enum PsqlEmitStop
{
	STOP_NONE = 0,				/* lexer ran to end of fed bytes */
	STOP_SEMI,					/* SQL: command-terminating ; found */
	STOP_BACKSLASH,				/* SQL: \\ found, slash command follows */
	STOP_VAR_EXPAND,			/* SQL/slash: :varname; driver pushes value */
	STOP_VAR_RECURSE,			/* SQL/slash: :varname currently expanding */
	STOP_SLASH_OK,				/* slash: end-of-arg / end-of-cmdname */
} PsqlEmitStop;

/* ----------------------------------------------------------------- */
/* EmitContext: passed as `user` cookie to LexFeedBytes().           */
/* ----------------------------------------------------------------- */

typedef struct PsqlEmitCtx
{
	PsqlScanState state;		/* the user's scan state */
	const char *buf;			/* bytes fed to LexFeedBytes (slice start) */
	size_t		buflen;			/* number of bytes fed */
	size_t		consumed;		/* on STOP_*: bytes consumed from buf */

	PsqlEmitStop stop_kind;

	/*
	 * For STOP_VAR_EXPAND: the variable's name and value.  Both are malloc'd;
	 * driver frees them after psqlscan_push_new_buffer.
	 */
	char	   *var_name;
	char	   *var_value;

	/*
	 * For STOP_VAR_RECURSE: the variable's name (malloc'd) and the raw `:foo`
	 * text + length to echo into output.
	 */
	const char *var_text;
	int			var_text_len;
} PsqlEmitCtx;

/* ----------------------------------------------------------------- */
/* Helper macros for action bodies.                                  */
/* ----------------------------------------------------------------- */

#define PSQL_CTX(u)         ((PsqlEmitCtx *) (u))
#define PSQL_STATE(u)       (PSQL_CTX(u)->state)
#define PSQL_OUTBUF(u)      (PSQL_STATE(u)->output_buf)
#define PSQL_TERMINATE_AT(u, kind, mlen)                              \
	do {                                                              \
		PSQL_CTX(u)->stop_kind = (kind);                              \
		PSQL_CTX(u)->consumed =                                       \
			(size_t) ((matched) - PSQL_CTX(u)->buf) + (mlen);         \
	} while (0)

/* ----------------------------------------------------------------- */
/* Helpers callable from .lex action bodies.                         */
/* ----------------------------------------------------------------- */

/*
 * Variable-substitution helpers.  Called from a .lex action body when
 * a :varname / :'varname' / :"varname" / :{?varname} pattern matches.
 * Each returns true when the action body should LEX_TERMINATE (driver
 * needs to push a new buffer or echo a recursion warning); false when
 * the variable was either inline-expanded into output_buf or the text
 * was emitted verbatim and the action body can LEX_SKIP and continue.
 *
 * For :varname (PQUOTE_PLAIN with variable substitution side effect),
 * the result is STOP_VAR_EXPAND or STOP_VAR_RECURSE.  For the other
 * three forms, output is always inline.
 */
extern bool psql_emit_var_plain(void *user, const char *p, size_t len);
extern void psql_emit_var_squote(void *user, const char *p, size_t len);
extern void psql_emit_var_dquote(void *user, const char *p, size_t len);
extern void psql_emit_var_test(void *user, const char *p, size_t len);

#endif							/* PSQLSCAN_EMIT_H */
