/*-------------------------------------------------------------------------
 *
 * syncrep_scanner.lex
 *	  Lime lexer for the synchronous_standby_names GUC.
 *
 * Replaces the hand-rolled tokenizer in syncrep_scanner.c (~390 lines
 * of state machine + char-class helpers) with a declarative .lex
 * source compiled by Lime v0.2.2's lexer subsystem.  The accompanying
 * syncrep_scanner.c shrinks to a parser-driver shim that wraps
 * SyncRepLexFeedBytes and forwards emitted tokens to the Lime parser
 * generated from syncrep_gram.lime.
 *
 * Tokens emitted match syncrep_gram.h's #defines (NAME, NUM, JUNK,
 * ANY, FIRST, COMMA, LPAREN, RPAREN).
 *
 * Behavioural deltas from the previous hand-rolled scanner:
 *
 *   - The ANY/FIRST keyword decision now happens via dedicated regex
 *     rules with case-insensitive character classes ([Aa][Nn][Yy] etc.)
 *     rather than reading the full identifier and post-classifying.
 *     Lime's longest-match-wins + declaration-order tiebreak places
 *     the keyword rules before the generic identifier rule, so
 *     "ANY"/"any" emit ANY (kw rule, 3 chars) but "ANYTHING" emits
 *     NAME (generic rule, 8 chars wins on length).  Identical
 *     observable behaviour to the prior scanner's ci_equal_ascii path.
 *
 *   - Delimited-identifier accumulation uses %literal_buffer (M3.7
 *     in Lime v0.2.2), replacing the StringInfo xdbuf field on
 *     SyncRepYyScanner.  The buffer's lifecycle is managed by Lime's
 *     runtime: LEX_BUF_START on `"`, LEX_BUF_APPEND_CH on `""`,
 *     LEX_BUF_APPEND on a run of non-quote bytes, LEX_BUF_TAKE on
 *     the closing `"`.  The driver's emit callback pstrdups the
 *     taken pointer into yylval->str and pfrees it.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/replication/syncrep_scanner.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix SyncRep.

%include {
#include "postgres.h"

#include "syncrep_gram.h"		/* NAME, NUM, JUNK, ANY, FIRST, COMMA,
								 * LPAREN, RPAREN */
#include "utils/palloc.h"
}

/* The xd state's accumulator for "..." identifiers. */
%literal_buffer scanid {
    type      char
    initial   64
    grow      "*2"
    alloc     palloc
    realloc   repalloc
    free      pfree
}.

%exclusive_state XD.

/* ---- Pattern fragments ----
**
** ident_start  = letter (ASCII or high-bit) or underscore.
** ident_cont   = ident_start | digit | $.
**
** Flex source's [\200-\377] (octal) maps to byte range 0x80-0xff.
*/
%pattern ident_start /[A-Za-z_\x80-\xff]/.
%pattern ident_cont  /[A-Za-z_0-9$\x80-\xff]/.

/* ===== Whitespace ===== */
rule ws matches /[ \t\n\r\f\v]+/ { LEX_SKIP(); }

/* ===== Case-insensitive keywords =====
**
** "any" and "first" are recognized case-insensitively; longer
** identifiers that happen to start with these letters fall through
** to the generic identifier rule below.
*/
rule kw_any   matches /[Aa][Nn][Yy]/                     { LEX_EMIT(ANY); }
rule kw_first matches /[Ff][Ii][Rr][Ss][Tt]/             { LEX_EMIT(FIRST); }

/* ===== Generic identifier =====
**
** The driver's emit callback pstrdups the matched span into
** yylval->str.
*/
rule ident matches /{ident_start}{ident_cont}*/          { LEX_EMIT(NAME); }

/* ===== Number ===== */
rule number matches /[0-9]+/                             { LEX_EMIT(NUM); }

/* ===== Wildcard / single-char tokens =====
**
** flex source emitted NAME with yylval = "*" for the wildcard rule.
** We pass the literal "*" through as the matched text; the driver
** pstrdups it like any other NAME.
*/
rule star    matches /\*/  { LEX_EMIT(NAME); }
rule comma   matches /,/   { LEX_EMIT(COMMA); }
rule lparen  matches /\(/  { LEX_EMIT(LPAREN); }
rule rparen  matches /\)/  { LEX_EMIT(RPAREN); }

/* ===== Delimited identifier "..." =====
**
** Open quote enters XD state with a fresh accumulator.  Inside XD,
** "" appends a single quote to the accumulator (the bison rule's
** xddouble), any run of non-quote bytes appends bytes (xdinside),
** and a single closing quote takes the accumulated buffer and
** emits NAME.  EOF inside XD is the unterminated-string error.
*/
rule xdstart matches /"/ {
    LEX_BUF_START(scanid);
    LEX_TRANSITION(SYNCREP_STATE_XD);
    LEX_SKIP();
}

<XD> rule xddouble matches /""/ {
    LEX_BUF_APPEND_CH(scanid, '"');
    LEX_SKIP();
}

<XD> rule xdinside matches /[^"]+/ {
    LEX_BUF_APPEND(scanid, matched, matched_len);
    LEX_SKIP();
}

<XD> rule xdstop matches /"/ {
    size_t n = LEX_BUF_LEN(scanid);
    char *s = LEX_BUF_TAKE(scanid);
    if (s == NULL) {
        LEX_ERROR_AT("oom in literal buffer take");
    } else {
        if (emit) emit(user, NAME, s, n);
        pfree(s);
    }
    LEX_TRANSITION(SYNCREP_STATE_INITIAL);
    LEX_SKIP();
}

<XD> rule xdeof matches <<EOF>> {
    LEX_ERROR_AT("unterminated quoted identifier");
}

/* ===== Catch-all =====
**
** flex's `.` rule emitted JUNK so the parser could format the
** "syntax error at or near ..." message.
*/
rule unexpected matches /./ { LEX_EMIT(JUNK); }
