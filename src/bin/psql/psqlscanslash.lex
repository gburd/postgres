/*-------------------------------------------------------------------------
 *
 * psqlscanslash.lex
 *	  Lime lexer for psql's backslash-command scanner.
 *
 * Replaces the hand-rolled state machine in psqlscanslash.c (~900
 * lines) with a declarative .lex source compiled by Lime v0.2.2's
 * lexer subsystem.  Action bodies invoke helpers in psqlscanslash.c
 * via the PsqlEmitCtx cookie; stop points (end-of-cmdname,
 * end-of-arg, end-of-line) call LEX_TERMINATE() so the driver returns
 * the accumulated argument string.
 *
 * Mirrors the eight exclusive states of the legacy flex source
 * (psqlscanslash.l, pre-Phase 2h): xslashcmd, xslashargstart,
 * xslasharg, xslashquote, xslashbackquote, xslashdquote,
 * xslashwholeline, xslashend.  Variable-substitution helpers are
 * shared with psqlscan.lex via psqlscan_emit.h.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/bin/psql/psqlscanslash.lex
 *
 *-------------------------------------------------------------------------
 */

%name_prefix Slash.

%include {
#include "postgres_fe.h"

#include <ctype.h>
#include <string.h>

#include "common.h"
#include "common/logging.h"
#include "fe_utils/conditional.h"
#include "fe_utils/psqlscan.h"
#include "fe_utils/psqlscan_emit.h"
#include "fe_utils/psqlscan_int.h"
#include "psqlscanslash.h"

/* File-scope state shared between psqlscanslash.c and the .lex action
 * bodies.  See psqlscanslash.c for definitions. */
extern enum slash_option_type slash_option_type;
extern char *slash_option_quote;
extern int  slash_unquoted_option_chars;
extern int  slash_backtick_start_offset;

extern void slash_evaluate_backtick(PsqlScanState state);
}

/* Default INITIAL state is unused (no INITIAL rules); we set the start
 * state from the caller (psql_scan_slash_command, ..._option,
 * ..._command_end). */

%exclusive_state XSLASHCMD.
%exclusive_state XSLASHARGSTART.
%exclusive_state XSLASHARG.
%exclusive_state XSLASHQUOTE.
%exclusive_state XSLASHBACKQUOTE.
%exclusive_state XSLASHDQUOTE.
%exclusive_state XSLASHWHOLELINE.
%exclusive_state XSLASHEND.

%pattern space         /[ \t\n\r\f\v]/.
%pattern variable_char /[A-Za-z_0-9\x80-\xff]/.
%pattern var_plain     /:{variable_char}+/.
%pattern var_squote    /:'{variable_char}+'/.
%pattern var_dquote    /:"{variable_char}+"/.
%pattern var_test      /:\{\?{variable_char}+\}/.

/* INITIAL: shouldn't be reached by the slash lexer (caller always sets
 * a start_state), but provide a fall-through that just emits one byte. */
rule initial_byte matches /[\x00-\xff]/ {
    psqlscan_emit(PSQL_STATE(user), matched, 1);
    LEX_SKIP();
}

/* =================================================================== */
/* XSLASHCMD: scanning the command name after "\"                      */
/* =================================================================== */

/* End of command name: any whitespace or another "\".  yyless(0)      */
/* (push back what we matched) and stop.                               */
<XSLASHCMD> rule cmd_end matches /[ \t\n\r\f\v\\]/ {
    LEX_PUSHBACK(matched_len);
    PSQL_TERMINATE_AT(user, STOP_SLASH_OK, 0);
    LEX_TERMINATE();
}

<XSLASHCMD> rule cmd_byte matches /[\x00-\xff]/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XSLASHCMD> rule cmd_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XSLASHARGSTART: skipping leading whitespace before next argument    */
/* =================================================================== */

<XSLASHARGSTART> rule argstart_ws matches /[ \t\n\r\f\v]+/ {
    LEX_SKIP();
}

/* "|" at start of arg is special only for OT_FILEPIPE.  Otherwise:
 * push it back and switch to XSLASHARG to lex it normally. */
<XSLASHARGSTART> rule argstart_pipe matches /\|/ {
    if (slash_option_type == OT_FILEPIPE) {
        psqlscan_emit(PSQL_STATE(user), matched, matched_len);
        state = SLASH_STATE_XSLASHWHOLELINE;
        LEX_SKIP();
    } else {
        LEX_PUSHBACK(matched_len);
        state = SLASH_STATE_XSLASHARG;
        LEX_SKIP();
    }
}

<XSLASHARGSTART> rule argstart_other matches /[\x00-\xff]/ {
    LEX_PUSHBACK(matched_len);
    state = SLASH_STATE_XSLASHARG;
    LEX_SKIP();
}

<XSLASHARGSTART> rule argstart_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XSLASHARG: scanning an unquoted argument                            */
/* =================================================================== */

/* End-of-arg: whitespace or backslash.  yyless(0) and stop. */
<XSLASHARG> rule arg_end matches /[ \t\n\r\f\v\\]/ {
    LEX_PUSHBACK(matched_len);
    PSQL_TERMINATE_AT(user, STOP_SLASH_OK, 0);
    LEX_TERMINATE();
}

<XSLASHARG> rule arg_squote matches /'/ {
    *slash_option_quote = '\'';
    slash_unquoted_option_chars = 0;
    state = SLASH_STATE_XSLASHQUOTE;
    LEX_SKIP();
}

<XSLASHARG> rule arg_backquote matches /`/ {
    slash_backtick_start_offset = PSQL_OUTBUF(user)->len;
    *slash_option_quote = '`';
    slash_unquoted_option_chars = 0;
    state = SLASH_STATE_XSLASHBACKQUOTE;
    LEX_SKIP();
}

<XSLASHARG> rule arg_dquote matches /"/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    *slash_option_quote = '"';
    slash_unquoted_option_chars = 0;
    state = SLASH_STATE_XSLASHDQUOTE;
    LEX_SKIP();
}

<XSLASHARG> rule arg_var_plain matches /{var_plain}/ {
    PsqlScanState s = PSQL_STATE(user);
    if (s->callbacks->get_variable == NULL) {
        psqlscan_emit(s, matched, matched_len);
    } else {
        char *varname;
        char *value;

        varname = psqlscan_extract_substring(s, matched + 1,
                                             (int) matched_len - 1);
        value = s->callbacks->get_variable(varname, PQUOTE_PLAIN,
                                            s->cb_passthrough);
        free(varname);
        if (value) {
            appendPQExpBufferStr(PSQL_OUTBUF(user), value);
            free(value);
        } else {
            psqlscan_emit(s, matched, matched_len);
        }
        *slash_option_quote = ':';
    }
    slash_unquoted_option_chars = 0;
    LEX_SKIP();
}

<XSLASHARG> rule arg_var_squote matches /{var_squote}/ {
    psqlscan_escape_variable(PSQL_STATE(user), matched, (int) matched_len,
                             PQUOTE_SQL_LITERAL);
    *slash_option_quote = ':';
    slash_unquoted_option_chars = 0;
    LEX_SKIP();
}

<XSLASHARG> rule arg_var_dquote matches /{var_dquote}/ {
    psqlscan_escape_variable(PSQL_STATE(user), matched, (int) matched_len,
                             PQUOTE_SQL_IDENT);
    *slash_option_quote = ':';
    slash_unquoted_option_chars = 0;
    LEX_SKIP();
}

<XSLASHARG> rule arg_var_test matches /{var_test}/ {
    psqlscan_test_variable(PSQL_STATE(user), matched, (int) matched_len);
    LEX_SKIP();
}

<XSLASHARG> rule arg_colon_fail matches /:['"\{]/ {
    LEX_PUSHBACK(matched_len - 1);
    slash_unquoted_option_chars++;
    psqlscan_emit(PSQL_STATE(user), matched, 1);
    LEX_SKIP();
}

<XSLASHARG> rule arg_other matches /[\x00-\xff]/ {
    slash_unquoted_option_chars++;
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XSLASHARG> rule arg_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XSLASHQUOTE: inside '...' single-quoted argument                    */
/* =================================================================== */

<XSLASHQUOTE> rule q_xqdouble matches /''/ {
    appendPQExpBufferChar(PSQL_OUTBUF(user), '\'');
    LEX_SKIP();
}

<XSLASHQUOTE> rule q_close matches /'/ {
    state = SLASH_STATE_XSLASHARG;
    LEX_SKIP();
}

<XSLASHQUOTE> rule q_escape_n matches /\\n/ { appendPQExpBufferChar(PSQL_OUTBUF(user), '\n'); LEX_SKIP(); }
<XSLASHQUOTE> rule q_escape_t matches /\\t/ { appendPQExpBufferChar(PSQL_OUTBUF(user), '\t'); LEX_SKIP(); }
<XSLASHQUOTE> rule q_escape_b matches /\\b/ { appendPQExpBufferChar(PSQL_OUTBUF(user), '\b'); LEX_SKIP(); }
<XSLASHQUOTE> rule q_escape_r matches /\\r/ { appendPQExpBufferChar(PSQL_OUTBUF(user), '\r'); LEX_SKIP(); }
<XSLASHQUOTE> rule q_escape_f matches /\\f/ { appendPQExpBufferChar(PSQL_OUTBUF(user), '\f'); LEX_SKIP(); }

<XSLASHQUOTE> rule q_octesc matches /\\[0-7]{1,3}/ {
    char buf[5];
    int n = (int) matched_len - 1;

    if (n > 3) n = 3;
    memcpy(buf, matched + 1, n);
    buf[n] = '\0';
    appendPQExpBufferChar(PSQL_OUTBUF(user),
                          (char) strtol(buf, NULL, 8));
    LEX_SKIP();
}

<XSLASHQUOTE> rule q_hexesc matches /\\x[0-9A-Fa-f]{1,2}/ {
    char buf[3];
    int n = (int) matched_len - 2;

    if (n > 2) n = 2;
    memcpy(buf, matched + 2, n);
    buf[n] = '\0';
    appendPQExpBufferChar(PSQL_OUTBUF(user),
                          (char) strtol(buf, NULL, 16));
    LEX_SKIP();
}

<XSLASHQUOTE> rule q_escape_other matches /\\[\x00-\xff]/ {
    psqlscan_emit(PSQL_STATE(user), matched + 1, 1);
    LEX_SKIP();
}

<XSLASHQUOTE> rule q_other matches /[\x00-\xff]/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XSLASHQUOTE> rule q_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XSLASHBACKQUOTE: inside `...` backticked argument                   */
/* =================================================================== */

<XSLASHBACKQUOTE> rule bq_close matches /`/ {
    PsqlScanState s = PSQL_STATE(user);
    if (s->cb_passthrough == NULL ||
        conditional_active((ConditionalStack) s->cb_passthrough))
        slash_evaluate_backtick(s);
    state = SLASH_STATE_XSLASHARG;
    LEX_SKIP();
}

<XSLASHBACKQUOTE> rule bq_var_plain matches /{var_plain}/ {
    PsqlScanState s = PSQL_STATE(user);
    if (s->callbacks->get_variable == NULL) {
        psqlscan_emit(s, matched, matched_len);
    } else {
        char *varname;
        char *value;

        varname = psqlscan_extract_substring(s, matched + 1,
                                             (int) matched_len - 1);
        value = s->callbacks->get_variable(varname, PQUOTE_PLAIN,
                                            s->cb_passthrough);
        free(varname);
        if (value) {
            appendPQExpBufferStr(PSQL_OUTBUF(user), value);
            free(value);
        } else {
            psqlscan_emit(s, matched, matched_len);
        }
    }
    LEX_SKIP();
}

<XSLASHBACKQUOTE> rule bq_var_squote matches /{var_squote}/ {
    psqlscan_escape_variable(PSQL_STATE(user), matched, (int) matched_len,
                             PQUOTE_SHELL_ARG);
    LEX_SKIP();
}

<XSLASHBACKQUOTE> rule bq_colon_fail matches /:'/ {
    LEX_PUSHBACK(matched_len - 1);
    psqlscan_emit(PSQL_STATE(user), matched, 1);
    LEX_SKIP();
}

<XSLASHBACKQUOTE> rule bq_other matches /[\x00-\xff]/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XSLASHBACKQUOTE> rule bq_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XSLASHDQUOTE: inside "..." double-quoted argument                   */
/* =================================================================== */

<XSLASHDQUOTE> rule dq_close matches /"/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    state = SLASH_STATE_XSLASHARG;
    LEX_SKIP();
}

<XSLASHDQUOTE> rule dq_other matches /[\x00-\xff]/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XSLASHDQUOTE> rule dq_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XSLASHWHOLELINE: copy rest of line verbatim                         */
/* =================================================================== */

<XSLASHWHOLELINE> rule wl_ws matches /[ \t\n\r\f\v]+/ {
    if (PSQL_OUTBUF(user)->len > 0)
        psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XSLASHWHOLELINE> rule wl_other matches /[\x00-\xff]/ {
    psqlscan_emit(PSQL_STATE(user), matched, matched_len);
    LEX_SKIP();
}

<XSLASHWHOLELINE> rule wl_eof matches <<EOF>> { /* fall through */ }

/* =================================================================== */
/* XSLASHEND: optional trailing "\\" eat                               */
/* =================================================================== */

<XSLASHEND> rule end_dbslash matches /\\\\/ {
    PSQL_TERMINATE_AT(user, STOP_SLASH_OK, matched_len);
    LEX_TERMINATE();
}

<XSLASHEND> rule end_other matches /[\x00-\xff]/ {
    LEX_PUSHBACK(matched_len);
    PSQL_TERMINATE_AT(user, STOP_SLASH_OK, 0);
    LEX_TERMINATE();
}

<XSLASHEND> rule end_eof matches <<EOF>> { /* fall through */ }
