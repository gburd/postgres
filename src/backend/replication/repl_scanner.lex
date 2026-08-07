/*-------------------------------------------------------------------------
 *
 * repl_scanner.lex
 *	  Lime lexer for the walsender replication command line.
 *
 * Replaces the hand-rolled tokenizer that lived in repl_scanner.c
 * (~570 lines of state machine + char-class helpers + literal buffer)
 * with a declarative .lex source compiled by Lime v0.2.2's lexer
 * subsystem.  Tokens emitted match repl_gram.h's #defines so the
 * existing parser works unchanged.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/replication/repl_scanner.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Repl.

%include {
#include "postgres.h"

#include "repl_gram.h"			/* SCONST, IDENT, UCONST, RECPTR,
								 * K_* keywords, LPAREN, RPAREN, COMMA,
								 * SEMI, DOT */
#include "utils/palloc.h"
}

/* Single accumulator reused for both XQ (single-quoted SCONST) and
** XD (double-quoted IDENT); the two states are mutually exclusive. */
%literal_buffer scanstr {
    type      char
    initial   1024
    grow      "*2"
    alloc     palloc
    realloc   repalloc
    free      pfree
}.

%exclusive_state XQ.
%exclusive_state XD.

/* ---- Pattern fragments ---- */
%pattern space       /[ \t\n\r\f\v]/.
%pattern digit       /[0-9]/.
%pattern hexdigit    /[0-9A-Fa-f]/.
%pattern ident_start /[A-Za-z_\x80-\xff]/.
%pattern ident_cont  /[A-Za-z_0-9$\x80-\xff]/.

/* ===== INITIAL state ===== */

/* Whitespace */
rule ws matches /{space}+/ { LEX_SKIP(); }

/* Case-sensitive keyword rules.  Each in its own rule; Lime's
** longest-match-wins + declaration-order tiebreak places them
** before the generic ident rule for exact matches.  Order copied
** from the retired flex scanner.
*/
rule kw_ALTER_REPLICATION_SLOT  matches /ALTER_REPLICATION_SLOT/  { LEX_EMIT(K_ALTER_REPLICATION_SLOT); }
rule kw_BASE_BACKUP             matches /BASE_BACKUP/             { LEX_EMIT(K_BASE_BACKUP); }
rule kw_CREATE_REPLICATION_SLOT matches /CREATE_REPLICATION_SLOT/ { LEX_EMIT(K_CREATE_REPLICATION_SLOT); }
rule kw_DROP_REPLICATION_SLOT   matches /DROP_REPLICATION_SLOT/   { LEX_EMIT(K_DROP_REPLICATION_SLOT); }
rule kw_EXPORT_SNAPSHOT         matches /EXPORT_SNAPSHOT/         { LEX_EMIT(K_EXPORT_SNAPSHOT); }
rule kw_IDENTIFY_SYSTEM         matches /IDENTIFY_SYSTEM/         { LEX_EMIT(K_IDENTIFY_SYSTEM); }
rule kw_LOGICAL                 matches /LOGICAL/                 { LEX_EMIT(K_LOGICAL); }
rule kw_NOEXPORT_SNAPSHOT       matches /NOEXPORT_SNAPSHOT/       { LEX_EMIT(K_NOEXPORT_SNAPSHOT); }
rule kw_PHYSICAL                matches /PHYSICAL/                { LEX_EMIT(K_PHYSICAL); }
rule kw_READ_REPLICATION_SLOT   matches /READ_REPLICATION_SLOT/   { LEX_EMIT(K_READ_REPLICATION_SLOT); }
rule kw_RESERVE_WAL             matches /RESERVE_WAL/             { LEX_EMIT(K_RESERVE_WAL); }
rule kw_SHOW                    matches /SHOW/                    { LEX_EMIT(K_SHOW); }
rule kw_SLOT                    matches /SLOT/                    { LEX_EMIT(K_SLOT); }
rule kw_START_REPLICATION       matches /START_REPLICATION/       { LEX_EMIT(K_START_REPLICATION); }
rule kw_TEMPORARY               matches /TEMPORARY/               { LEX_EMIT(K_TEMPORARY); }
rule kw_TIMELINE                matches /TIMELINE/                { LEX_EMIT(K_TIMELINE); }
rule kw_TIMELINE_HISTORY        matches /TIMELINE_HISTORY/        { LEX_EMIT(K_TIMELINE_HISTORY); }
rule kw_TWO_PHASE               matches /TWO_PHASE/               { LEX_EMIT(K_TWO_PHASE); }
rule kw_UPLOAD_MANIFEST         matches /UPLOAD_MANIFEST/         { LEX_EMIT(K_UPLOAD_MANIFEST); }
rule kw_USE_SNAPSHOT            matches /USE_SNAPSHOT/            { LEX_EMIT(K_USE_SNAPSHOT); }
rule kw_WAIT                    matches /WAIT/                    { LEX_EMIT(K_WAIT); }

/* Recovery-pointer literal: hex/hex (e.g. "1A2/F00").  The driver's
** emit callback sscanfs %X/%08X.  Declared BEFORE digits and idents
** so longest-match-wins fires correctly.
*/
rule recptr matches /{hexdigit}+\/{hexdigit}+/ { LEX_EMIT(RECPTR); }

/* Unsigned-integer constant.  Driver runs strtoul. */
rule uconst matches /{digit}+/ { LEX_EMIT(UCONST); }

/* Generic identifier.  Driver runs downcase_truncate_identifier. */
rule ident matches /{ident_start}{ident_cont}*/ { LEX_EMIT(IDENT); }

/* Single-character punctuation. */
rule lparen matches /\(/ { LEX_EMIT(LPAREN); }
rule rparen matches /\)/ { LEX_EMIT(RPAREN); }
rule comma  matches /,/  { LEX_EMIT(COMMA); }
rule semi   matches /;/  { LEX_EMIT(SEMI); }
rule dot    matches /\./ { LEX_EMIT(DOT); }

/* ===== Single-quoted string literal: '...' with '' -> ' escape ===== */

rule xqopen matches /'/ {
    LEX_BUF_START(scanstr);
    LEX_TRANSITION(REPL_STATE_XQ);
    LEX_SKIP();
}

<XQ> rule xqdouble matches /''/ {
    LEX_BUF_APPEND_CH(scanstr, '\'');
    LEX_SKIP();
}

<XQ> rule xqinside matches /[^']+/ {
    LEX_BUF_APPEND(scanstr, matched, matched_len);
    LEX_SKIP();
}

<XQ> rule xqclose matches /'/ {
    size_t n = LEX_BUF_LEN(scanstr);
    char *s = LEX_BUF_TAKE(scanstr);
    if (s == NULL) {
        LEX_ERROR_AT("oom in literal buffer take");
    } else {
        if (emit) emit(user, SCONST, s, n);
        pfree(s);
    }
    LEX_TRANSITION(REPL_STATE_INITIAL);
    LEX_SKIP();
}

<XQ> rule xqeof matches <<EOF>> {
    LEX_ERROR_AT("unterminated quoted string");
}

/* ===== Double-quoted identifier: "..." with "" -> " escape ===== */

rule xdopen matches /"/ {
    LEX_BUF_START(scanstr);
    LEX_TRANSITION(REPL_STATE_XD);
    LEX_SKIP();
}

<XD> rule xddouble matches /""/ {
    LEX_BUF_APPEND_CH(scanstr, '"');
    LEX_SKIP();
}

<XD> rule xdinside matches /[^"]+/ {
    LEX_BUF_APPEND(scanstr, matched, matched_len);
    LEX_SKIP();
}

<XD> rule xdclose matches /"/ {
    size_t n = LEX_BUF_LEN(scanstr);
    char *s = LEX_BUF_TAKE(scanstr);
    if (s == NULL) {
        LEX_ERROR_AT("oom in literal buffer take");
    } else {
        /* REPL_TOK_QIDENT is an internal sentinel (>1000, well
        ** above any repl_gram.h token); the driver's emit callback
        ** maps it to IDENT but skips the downcase step that
        ** unquoted idents go through. */
        if (emit) emit(user, 1001 /* REPL_TOK_QIDENT */, s, n);
        pfree(s);
    }
    LEX_TRANSITION(REPL_STATE_INITIAL);
    LEX_SKIP();
}

<XD> rule xdeof matches <<EOF>> {
    LEX_ERROR_AT("unterminated quoted string");
}

/* ===== Catch-all =====
**
** flex's `.` rule returned the raw character code, which Bison
** treated as an unknown token and reported as syntax error.  Same
** observable behaviour via emitting a sentinel JUNK code -- but the
** repl grammar has no JUNK token, so we use -1 to force the parser's
** default-error path.  Lime's emit callback in the driver translates
** -1 -> a 1-token syntax error.
*/
rule unexpected matches /./ { LEX_EMIT(-1); }
