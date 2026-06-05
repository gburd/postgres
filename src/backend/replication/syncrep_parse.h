/*-------------------------------------------------------------------------
 *
 * syncrep_parse.h
 *	  Private definitions shared by the Lime-generated
 *	  synchronous_standby_names parser and the hand-rolled scanner/driver.
 *
 * This header is strictly internal to src/backend/replication.  It is not
 * installed and is not visible to other subsystems: the public interface to
 * the parser stays exactly as declared in include/replication/syncrep.h
 * (syncrep_scanner_init / syncrep_yyparse / syncrep_scanner_finish /
 * syncrep_yyerror).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/replication/syncrep_parse.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SYNCREP_PARSE_H
#define SYNCREP_PARSE_H

#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "replication/syncrep.h"

/*
 * Semantic value carried with each terminal/non-terminal in the Lime
 * parser.  The only payload the grammar cares about is an identifier or
 * numeric-literal string; a single-field struct keeps the Lime-generated
 * YYMINORTYPE layout trivial while still giving us room to grow.
 */
typedef struct SyncRepToken
{
	char	   *str;			/* token text, palloc'd (or a static literal
								 * for the wildcard "*") */
} SyncRepToken;

/*
 * Parse context passed to the Lime parser as %extra_argument.  Bundles
 * everything the grammar actions, the %syntax_error / %parse_failure
 * callbacks, and the driver loop need to see.
 *
 * Lime only supports a single extra argument, so the three Bison
 * %parse-param slots collapse into this struct.
 */
typedef struct SyncRepParseCtx
{
	SyncRepConfigData **result_p;	/* written by the result rule */
	char	  **error_msg_p;	/* populated by syncrep_yyerror */
	yyscan_t	scanner;		/* opaque handle for yyerror's yytext peek */
} SyncRepParseCtx;

/*
 * Opaque scanner state.  yyscan_t (public typedef: void *) points at one of
 * these.  The fields track the input cursor, the text of the last matched
 * flex-style rule (for yyerror's "at or near" message), and a staging
 * buffer for delimited-identifier collection.
 */
/*
 * Opaque scanner state.  yyscan_t (public typedef: void *) points at one of
 * these.  The fields track the input cursor and the text of the last
 * matched lexer rule (for yyerror's "at or near" message).  Delimited-
 * identifier accumulation lives entirely in the Lime lexer's
 * %literal_buffer (scanid in syncrep_scanner.lex), so no xdbuf field
 * is needed here.
 */
typedef struct SyncRepYyScanner
{
	char	   *input;			/* palloc'd copy of the input string */
	int			pos;			/* cursor into input[]; advisory only, the
								 * lexer drives the actual scan */
	int			len;			/* length of input (excluding NUL) */
	StringInfoData yytext;		/* text of the most recently emitted token,
								 * for yyerror's "at or near X" formatting */
} SyncRepYyScanner;

/*
 * Lime-generated parser entry points.  %name syncrep_yy makes these:
 *   syncrep_yyAlloc(allocator)  -- create a parser instance
 *   syncrep_yyFree(parser, fr)  -- destroy it
 *   syncrep_yy(parser, code, t, ctx) -- feed one token
 * A zero code means end-of-input.
 */
extern void *syncrep_yyAlloc(void *(*mallocProc) (size_t));
extern void syncrep_yyFree(void *p, void (*freeProc) (void *));
extern void syncrep_yy(void *yyp, int yymajor, SyncRepToken yyminor,
					   SyncRepParseCtx *ctx);

#endif							/* SYNCREP_PARSE_H */
