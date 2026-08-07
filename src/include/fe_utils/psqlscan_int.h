/*-------------------------------------------------------------------------
 *
 * psqlscan_int.h
 *	  lexical scanner internal declarations
 *
 * This file declares the PsqlScanStateData structure used by psqlscan.c
 * and shared by other lexers compatible with it, such as psqlscanslash.c
 * and src/bin/pgbench/exprscan.c.
 *
 * One difficult aspect of this code is that we need to work in multibyte
 * encodings that are not ASCII-safe.  A "safe" encoding is one in which each
 * byte of a multibyte character has the high bit set (it's >= 0x80).  Since
 * all our lexing rules treat all high-bit-set characters alike, we don't
 * really need to care whether such a byte is part of a sequence or not.
 * In an "unsafe" encoding, we still expect the first byte of a multibyte
 * sequence to be >= 0x80, but later bytes might not be.  If we scan such
 * a sequence as-is, the lexing rules could easily be fooled into matching
 * such bytes to ordinary ASCII characters.  Our solution for this is to
 * substitute 0xFF for each non-first byte within the data presented to the
 * lexer.  The lexer rules will then pass the FF's through unmolested.  The
 * psqlscan_emit() subroutine is responsible for looking back to the original
 * string and replacing FF's with the corresponding original bytes.
 *
 * Another interesting thing we do here is scan different parts of the same
 * input with physically separate lexers (psqlscan.c and psqlscanslash.c).
 * This works because all lexer state lives in PsqlScanState rather than in
 * file-static variables, and both lexers agree on the scanner-state encoding
 * via ScanState below.  The lexer is recursion-safe because the entire input
 * cursor and buffer stack live in PsqlScanStateData.
 *
 * (Pre-Phase 2h, this file used flex's YY_BUFFER_STATE / yyscan_t machinery.
 * The hand-rolled scanners use plain pointers; flex is no longer involved.)
 *
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/fe_utils/psqlscan_int.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PSQLSCAN_INT_H
#define PSQLSCAN_INT_H

#include "fe_utils/psqlscan.h"


/*
 * Scanner state codes.  Values 0..ST_XUS_MAX are the SQL-side states; the
 * slash-command states (ST_XSLASHCMD..) are appended after.  Both lexers
 * share this enumeration so that PsqlScanStateData.start_state can encode
 * either lexer's state.
 *
 * The names mirror the original flex `%x` exclusive states from psqlscan.l
 * and psqlscanslash.l, except prefixed with ST_ to avoid macro collisions
 * with flex/Lemon-generated headers elsewhere in the tree.
 *
 * INITIAL must be 0 because legacy flex code in pgbench's exprscan.l uses
 * `state->start_state = INITIAL` (which expanded to 0) to reset; we keep
 * that compat by aliasing INITIAL to ST_INITIAL via a #define below.
 */
typedef enum
{
	ST_INITIAL = 0,				/* SQL: outside any quoted/comment context */
	ST_XB,						/* SQL: bit string literal               */
	ST_XC,						/* SQL: extended C-style comment         */
	ST_XD,						/* SQL: delimited identifier             */
	ST_XH,						/* SQL: hexadecimal byte string          */
	ST_XQ,						/* SQL: standard quoted string           */
	ST_XQS,						/* SQL: quote-stop (continuation lookahead) */
	ST_XE,						/* SQL: extended quoted string (\esc)    */
	ST_XDOLQ,					/* SQL: $foo$ quoted string              */
	ST_XUI,						/* SQL: U&"..." quoted identifier        */
	ST_XUS,						/* SQL: U&'...' quoted string            */

	ST_XSLASHCMD,				/* slash: scanning the command name      */
	ST_XSLASHARGSTART,			/* slash: skipping leading whitespace    */
	ST_XSLASHARG,				/* slash: scanning argument body         */
	ST_XSLASHQUOTE,				/* slash: inside '...' single-quoted arg */
	ST_XSLASHBACKQUOTE,			/* slash: inside `...` backquoted arg    */
	ST_XSLASHDQUOTE,			/* slash: inside "..." double-quoted arg */
	ST_XSLASHWHOLELINE,			/* slash: copy rest of line verbatim     */
	ST_XSLASHEND				/* slash: eat optional trailing \\\\     */
} ScanState;

/*
 * Compatibility names: the flex-era code referenced these as bare unprefixed
 * identifiers because flex generated `enum { INITIAL, xb, ... }` for us.
 * Other source files (notably pgbench's exprscan.l) embedded `INITIAL` into
 * their action blocks; keep the macro shim in place so they compile until
 * those files are ported in their respective phases.
 */
#define INITIAL ST_INITIAL
#define xb ST_XB
#define xc ST_XC
#define xd ST_XD
#define xh ST_XH
#define xq ST_XQ
#define xqs ST_XQS
#define xe ST_XE
#define xdolq ST_XDOLQ
#define xui ST_XUI
#define xus ST_XUS

/*
 * A stacked input buffer.  Used for psql variable substitution: when the
 * scanner sees `:varname`, the variable's value is pushed as a new
 * StackElem and the cursor walks through that string until it's exhausted,
 * then pops back to the underlying scanbuf.
 *
 * Pre-port these owned `YY_BUFFER_STATE buf` plus an externally-managed
 * yyscan_t.  Now we just track a plain owned buffer (`bufstring`) and a
 * cursor (`pos`) into it.
 */
typedef struct StackElem
{
	char	   *bufstring;		/* owned NUL-terminated input data        */
	int			buflen;			/* length not counting trailing NUL       */
	int			pos;			/* current scanning position in bufstring */
	char	   *origstring;		/* original (un-FF-mapped) data, if needed */
	char	   *varname;		/* variable name supplying this data, or NULL */
	struct StackElem *next;
} StackElem;

/*
 * All working state of the lexer must be stored in PsqlScanStateData
 * between calls.  This allows multiple open lexer operations, which is
 * needed for nested includes.  The lexer itself is not recursive, but it
 * must be reentrant-safe across PsqlScanState instances.
 */
typedef struct PsqlScanStateData
{
	PQExpBuffer output_buf;		/* current output buffer                  */

	StackElem  *buffer_stack;	/* stack of variable-expansion buffers    */

	/*
	 * Outer-level input buffer.  Owned by this struct; allocated by
	 * psql_scan_setup() and freed by psql_scan_finish().  Multibyte non-first
	 * bytes have been replaced with 0xFF when not in a "safe" encoding.
	 */
	char	   *scanbuf;		/* owned input buffer (with FF mapping)   */
	int			scanbuflen;		/* length of scanbuf data not counting NUL */
	int			scanbufpos;		/* current cursor into scanbuf            */
	const char *scanline;		/* original (un-FF-mapped) input data     */

	/*
	 * Length of the most recently matched token, used to honor yyless(N): the
	 * lexer rewinds the cursor by (yyleng - N) bytes after the action.
	 */
	int			yyleng;

	/* safe_encoding, curline, refline are used by emit() to replace FFs */
	int			encoding;		/* encoding being used now                */
	bool		safe_encoding;	/* is current encoding "safe"?            */
	bool		std_strings;	/* are string literals standard?          */
	const char *curline;		/* actual lexer input string for cur buf  */
	const char *refline;		/* original data for cur buffer           */

	/* status for psql_scan_get_location() */
	int			cur_line_no;	/* current line#, or 0 if no scan done    */
	const char *cur_line_ptr;	/* points into cur_line_no'th line in scanbuf */

	/*
	 * State that lives across successive input lines, until explicitly reset
	 * by psql_scan_reset.  start_state is consumed at scan entry and updated
	 * with the finishing state on exit.
	 */
	ScanState	start_state;
	ScanState	state_before_str_stop;	/* start cond. before end quote   */
	int			paren_depth;	/* depth of nesting in parentheses        */
	int			xcdepth;		/* depth of nesting in slash-star comments */
	char	   *dolqstart;		/* current $foo$ quote start string       */

	/*
	 * State to track BEGIN ... END boundaries inside CREATE FUNCTION /
	 * PROCEDURE / SCHEMA bodies, so that nested semicolons don't send the
	 * query early.  See the {identifier} rule in psqlscan.l.
	 */
	int			begin_depth;	/* depth of begin/end pairs */
	int			init_idents_count;	/* # identifiers since start of statement */
	char		init_idents[4]; /* records the first few identifiers */
	int			sub_idents_count;	/* # identifiers since start of a CREATE
									 * SCHEMA element */
	char		sub_idents[4];	/* records the first few of those identifiers */

	/*
	 * Callback functions provided by the program using the lexer, plus the
	 * void* passthrough they get.
	 */
	const PsqlScanCallbacks *callbacks;
	void	   *cb_passthrough;
} PsqlScanStateData;


/*
 * Functions exported by psqlscan.c, but only meant for use within
 * compatible lexers (psqlscanslash.c, pgbench's exprscan.c).
 */
extern void psqlscan_push_new_buffer(PsqlScanState state,
									 const char *newstr, const char *varname);
extern void psqlscan_pop_buffer_stack(PsqlScanState state);
extern void psqlscan_select_top_buffer(PsqlScanState state);
extern bool psqlscan_var_is_current_source(PsqlScanState state,
										   const char *varname);

/*
 * psqlscan_prepare_buffer kept the same name and contract as the flex era;
 * it allocates a buffer, does the FF mapping if needed, and returns a
 * `void *` handle that callers stored in StackElem.buf.  The handle is now
 * actually a pointer to char (the bufstring), but callers should treat it
 * as opaque.  The new contract: the returned pointer is the SAME pointer
 * stored in *txtcopy on output.
 */
extern void *psqlscan_prepare_buffer(PsqlScanState state,
									 const char *txt, int len,
									 char **txtcopy);
extern void psqlscan_emit(PsqlScanState state, const char *txt, int len);
extern void psqlscan_track_identifier(PsqlScanState state,
									 const char *identifier, int len);
extern char *psqlscan_extract_substring(PsqlScanState state,
										const char *txt, int len);
extern void psqlscan_escape_variable(PsqlScanState state,
									 const char *txt, int len,
									 PsqlScanQuoteType quote);
extern void psqlscan_test_variable(PsqlScanState state,
								   const char *txt, int len);

#endif							/* PSQLSCAN_INT_H */
