/*
 * src/interfaces/ecpg/preproc/pgc.lex
 *
 * Lime declarative lexer for ecpg's SQL+ECPG-specific tokenizer.
 * Replaces the hand-rolled state machine in pgc.c (~3,238 lines).
 * pgc.c shrinks to a parser-driver shim that owns the InputBuffer
 * stack (EXEC SQL INCLUDE, EXEC SQL DEFINE expansion, INFORMIX-mode
 * $-prefixed directives) and runs a per-token LexFeedBytes loop:
 * each base_yylex() pull feeds the current buffer's remaining bytes,
 * the .lex's action body emits at most one token and calls
 * LEX_TERMINATE, then the driver advances the buffer cursor.
 *
 * Per-token feeding is required because ecpg's parser mutates
 * file-static globals read by the scanner (struct_level,
 * braces_open, parenths_open) inside reduce actions; a pure
 * pre-scan FIFO would observe stale values.  See AGENTS.md
 * "Phase 5 ecpg pgc port" for the design rationale.
 *
 * Lex-time fprintf to base_yyout (whitespace passthrough, /* * /
 * comment passthrough, #line directives emitted by parse_include)
 * happens directly inside an action body and is therefore
 * correctly interleaved with parse-time fprintf from preproc.lime
 * reduce actions, matching Bison's pull-mode timing.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/interfaces/ecpg/preproc/pgc.lex
 */

%name_prefix Pgc.

%include {
#include "postgres_fe.h"

#include <ctype.h>
#include <limits.h>

#include "common/string.h"
#include "preproc_extern.h"
#include "preproc_yytype.h"
#include "preproc.h"
#include "pgc_internal.h"
}

/*
 * ----------------------------------------------------------------- --
 * Sentinel rule codes emitted via the user callback.  Action bodies --
 * pass these to emit(); the driver shim translates to parser-side    --
 * token codes (preproc.h: SCONST, IDENT, ICONST, ...) and queues a   --
 * single FIFO entry per LexFeedBytes call.  All bodies that emit a   --
 * token also call LEX_TERMINATE so the driver retakes control after  --
 * exactly one token.                                                 --
 * ----------------------------------------------------------------- --
 */

/*
 * ----------------------------------------------------------------- --
 * Exclusive states (16 SQL-side + 5 ecpg-specific).                  --
 * ----------------------------------------------------------------- --
 */

%exclusive_state C.
%exclusive_state SQL.
%exclusive_state XB.
%exclusive_state XC.
%exclusive_state XD.
%exclusive_state XDC.
%exclusive_state XH.
%exclusive_state XN.
%exclusive_state XQ.
%exclusive_state XQS.
%exclusive_state XE.
%exclusive_state XQC.
%exclusive_state XDOLQ.
%exclusive_state XUI.
%exclusive_state XUS.
%exclusive_state XCOND.
%exclusive_state XSKIP.
%exclusive_state INCL.
%exclusive_state DEF.
%exclusive_state DEFI.
%exclusive_state UNDEF.

/*
 * ----------------------------------------------------------------- --
 * Pattern fragments.  Mirror the regex defs in the retired pgc.l.    --
 * ----------------------------------------------------------------- --
 */

%pattern space          /[ \t\n\r\f\v]/.
%pattern non_newline_space /[ \t\f\v]/.
%pattern newline        /[\n\r]/.
%pattern decdigit       /[0-9]/.
%pattern hexdigit       /[0-9A-Fa-f]/.
%pattern octdigit       /[0-7]/.
%pattern bindigit       /[01]/.
%pattern ident_start    /[A-Za-z_\x80-\xff]/.
%pattern ident_cont     /[A-Za-z_0-9$\x80-\xff]/.
%pattern dolq_start     /[A-Za-z_\x80-\xff]/.
%pattern dolq_cont      /[A-Za-z_0-9\x80-\xff]/.
%pattern op_chars       /[\~\!\@\#\^\&\|\`\?\+\-\*\/\%\<\>\=]/.
%pattern self           /[,()\[\]\.;:|+\-*\/%^<>=]/.
%pattern comment        /--[^\n\r]*/.
%pattern whitespace     /({space}+|{comment})/.
%pattern non_newline_ws /({non_newline_space}|{comment})/.
%pattern whitespace_with_newline /{non_newline_ws}*{newline}({non_newline_space}|{comment}|{newline})*/.
%pattern quotecontinue  /{whitespace_with_newline}'/.
%pattern decinteger     /{decdigit}(_?{decdigit})*/.
%pattern hexinteger     /0[xX](_?{hexdigit})+/.
%pattern octinteger     /0[oO](_?{octdigit})+/.
%pattern bininteger     /0[bB](_?{bindigit})+/.
%pattern hexfail        /0[xX]_?/.
%pattern octfail        /0[oO]_?/.
%pattern binfail        /0[bB]_?/.
%pattern numeric        /(({decinteger}\.{decinteger}?)|(\.{decinteger}))/.
%pattern numericfail    /{decinteger}\.\./.
%pattern real           /({decinteger}|{numeric})[Ee][-+]?{decinteger}/.
%pattern realfail       /({decinteger}|{numeric})[Ee][-+]/.
%pattern integer_junk   /{decinteger}{ident_start}/.
%pattern numeric_junk   /{numeric}{ident_start}/.
%pattern real_junk      /{real}{ident_start}/.
%pattern param          /\${decdigit}+/.
%pattern param_junk     /\${decdigit}+{ident_start}/.
%pattern identifier     /{ident_start}{ident_cont}*/.
%pattern cv_array       /({ident_cont}|{space}|[\[\]\+\-\*\%\/\(\)\>\.])*/.
%pattern cvariable      /:{identifier}((->|\.){identifier}|\[{cv_array}\])*/.
%pattern dolqdelim      /\$({dolq_start}{dolq_cont}*)?\$/.
%pattern dolqfailed     /\${dolq_start}{dolq_cont}*/.
%pattern dolqinside     /[^$]+/.
%pattern operator       /{op_chars}+/.
%pattern xcstart        /\/\*{op_chars}*/.
%pattern xcstop         /\*+\//.
%pattern xcinside       /[^*\/]+/.
%pattern ccomment       /\/\/[^\n]*\n/.
%pattern ip             /{decdigit}{1,3}\.{decdigit}{1,3}\.{decdigit}{1,3}\.{decdigit}{1,3}/.
%pattern exec_kw        /[eE][xX][eE][cC]/.
%pattern sql_kw         /[sS][qQ][lL]/.
%pattern exec_sql       /{exec_kw}{space}+{sql_kw}{space}*/.
%pattern ifdef_kw       /[iI][fF][dD][eE][fF]/.
%pattern ifndef_kw      /[iI][fF][nN][dD][eE][fF]/.
%pattern else_kw        /[eE][lL][sS][eE]/.
%pattern elif_kw        /[eE][lL][iI][fF]/.
%pattern endif_kw       /[eE][nN][dD][iI][fF]/.
%pattern define_kw      /[dD][eE][fF][iI][nN][eE]/.
%pattern undef_kw       /[uU][nN][dD][eE][fF]/.
%pattern include_kw     /[iI][nN][cC][lL][uU][dD][eE]/.
%pattern include_next_kw /[iI][nN][cC][lL][uU][dD][eE]_[nN][eE][xX][tT]/.
%pattern xbinside       /[^']+/.
%pattern xhinside       /[^']+/.
%pattern xqinside       /[^']+/.
%pattern xeinside       /[^\\']+/.
%pattern xeunicode      /\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})/.
%pattern xehexesc       /\\x[0-9A-Fa-f]{1,2}/.
%pattern xeoctesc       /\\[0-7]{1,3}/.
%pattern xqdouble       /''/.
%pattern xddouble       /""/.
%pattern xdinside       /[^"]+/.
%pattern xdcinside      /(\\[\\"]|[^"])/.

/*
 * ===================================================================
 * INITIAL state -- never reached at runtime.  pgc_lex_init transitions
 * to PGC_STATE_C immediately.  A catch-all rule keeps Lime happy.
 * ===================================================================
 */

rule initial_unreachable matches /[\x00-\xff]/ {
    LEX_ERROR_AT("internal error: pgc lexer in INITIAL state");
}

/*
 * ===================================================================
 * C state: C-language pass-through with EXEC SQL recognition.        --
 * XSKIP shares the EXEC SQL preprocessor-directive rules but ignores --
 * everything else (including bare EXEC SQL).  Per Phase 2i finding,  --
 * only ifdef/ifndef/elif/else/endif keywords match in <C, XSKIP>;    --
 * bare EXEC SQL is C-only.                                           --
 * ===================================================================
 */

/* ----- EXEC SQL DEFINE (C only; xskip ignores) ----- */
<C> rule c_exec_define matches /{exec_sql}{define_kw}{space}+/ {
    pgc_count_newlines(matched, matched_len);
    LEX_TRANSITION(PGC_STATE_DEFI);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<XSKIP> rule x_exec_define matches /{exec_sql}{define_kw}{space}+/ {
    pgc_count_newlines(matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- EXEC SQL INCLUDE_NEXT / INCLUDE ----- */
<C> rule c_exec_include_next matches /{exec_sql}{include_next_kw}{space}+/ {
    pgc_count_newlines(matched, matched_len);
    pgc_set_include_next(true);
    LEX_TRANSITION(PGC_STATE_INCL);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<XSKIP> rule x_exec_include_next matches /{exec_sql}{include_next_kw}{space}+/ {
    pgc_count_newlines(matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_exec_include matches /{exec_sql}{include_kw}{space}+/ {
    pgc_count_newlines(matched, matched_len);
    pgc_set_include_next(false);
    LEX_TRANSITION(PGC_STATE_INCL);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<XSKIP> rule x_exec_include matches /{exec_sql}{include_kw}{space}+/ {
    pgc_count_newlines(matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- EXEC SQL UNDEF (C only) ----- */
<C> rule c_exec_undef matches /{exec_sql}{undef_kw}{space}+/ {
    pgc_count_newlines(matched, matched_len);
    LEX_TRANSITION(PGC_STATE_UNDEF);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<XSKIP> rule x_exec_undef matches /{exec_sql}{undef_kw}{space}+/ {
    pgc_count_newlines(matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- EXEC SQL IFDEF / IFNDEF (C, XSKIP) ----- */
<C, XSKIP> rule cx_exec_ifdef matches /{exec_sql}{ifdef_kw}{space}+/ {
    pgc_count_newlines(matched, matched_len);
    if (pgc_push_if(true) < 0)
        LEX_ERROR_AT("too many nested EXEC SQL IFDEF conditions");
    LEX_TRANSITION(PGC_STATE_XCOND);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C, XSKIP> rule cx_exec_ifndef matches /{exec_sql}{ifndef_kw}{space}+/ {
    pgc_count_newlines(matched, matched_len);
    if (pgc_push_if(false) < 0)
        LEX_ERROR_AT("too many nested EXEC SQL IFDEF conditions");
    LEX_TRANSITION(PGC_STATE_XCOND);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- EXEC SQL ELIF ----- */
<C, XSKIP> rule cx_exec_elif matches /{exec_sql}{elif_kw}{space}+/ {
    int rc;
    pgc_count_newlines(matched, matched_len);
    rc = pgc_handle_elif();
    if (rc < 0)
        LEX_ERROR_AT("missing matching \"EXEC SQL IFDEF\" / \"EXEC SQL IFNDEF\"");
    if (rc == 1)
        LEX_ERROR_AT("missing \"EXEC SQL ENDIF;\"");
    LEX_TRANSITION(PGC_STATE_XCOND);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- EXEC SQL ELSE; ----- */
<C, XSKIP> rule cx_exec_else matches /{exec_sql}{else_kw}{space}*;/ {
    int rc;
    pgc_count_newlines(matched, matched_len);
    rc = pgc_handle_else();
    if (rc < 0)
        LEX_ERROR_AT("missing matching \"EXEC SQL IFDEF\" / \"EXEC SQL IFNDEF\"");
    if (rc == 1)
        LEX_ERROR_AT("more than one EXEC SQL ELSE");
    LEX_TRANSITION(pgc_active_state());
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- EXEC SQL ENDIF; ----- */
<C, XSKIP> rule cx_exec_endif matches /{exec_sql}{endif_kw}{space}*;/ {
    int rc;
    pgc_count_newlines(matched, matched_len);
    rc = pgc_handle_endif();
    if (rc < 0)
        LEX_ERROR_AT("unmatched EXEC SQL ENDIF");
    LEX_TRANSITION(pgc_active_state());
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- bare EXEC SQL (C only): emit SQL_START ----- */
<C> rule c_exec_sql matches /{exec_sql}/ {
    pgc_count_newlines(matched, matched_len);
    LEX_TRANSITION(PGC_STATE_SQL);
    emit(user, PGC_TOK_SQL_START, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/*
 * ===================================================================
 * INFORMIX-mode `$`-prefixed directives.  Parallel to EXEC SQL but   --
 * guarded inside the action body since INFORMIX_MODE is a runtime   --
 * enum.                                                              --
 * ===================================================================
 */

<C, XSKIP> rule cx_dollar_define matches /\${define_kw}{space}+/ {
    if (!INFORMIX_MODE) {
        LEX_PUSHBACK(matched_len - 1);
        emit(user, PGC_TOK_RAW_CHAR, matched, 1);
        pgc_terminate(user, matched, 1);
        LEX_SKIP();
        LEX_TERMINATE();
    } else {
    pgc_count_newlines(matched, matched_len);
    if (state == PGC_STATE_C) {
        LEX_TRANSITION(PGC_STATE_DEFI);
    }
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    }
}
<C, XSKIP> rule cx_dollar_include matches /\${include_kw}{space}+/ {
    if (!INFORMIX_MODE) {
        LEX_PUSHBACK(matched_len - 1);
        emit(user, PGC_TOK_RAW_CHAR, matched, 1);
        pgc_terminate(user, matched, 1);
        LEX_SKIP();
        LEX_TERMINATE();
    } else {
    pgc_count_newlines(matched, matched_len);
    if (state == PGC_STATE_C) {
        pgc_set_include_next(false);
        LEX_TRANSITION(PGC_STATE_INCL);
    }
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    }
}
<C, XSKIP> rule cx_dollar_undef matches /\${undef_kw}{space}+/ {
    if (!INFORMIX_MODE) {
        LEX_PUSHBACK(matched_len - 1);
        emit(user, PGC_TOK_RAW_CHAR, matched, 1);
        pgc_terminate(user, matched, 1);
        LEX_SKIP();
        LEX_TERMINATE();
    } else {
    pgc_count_newlines(matched, matched_len);
    if (state == PGC_STATE_C) {
        LEX_TRANSITION(PGC_STATE_UNDEF);
    }
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    }
}
<C, XSKIP> rule cx_dollar_ifdef matches /\${ifdef_kw}{space}+/ {
    if (!INFORMIX_MODE) {
        LEX_PUSHBACK(matched_len - 1);
        emit(user, PGC_TOK_RAW_CHAR, matched, 1);
        pgc_terminate(user, matched, 1);
        LEX_SKIP();
        LEX_TERMINATE();
    } else {
    pgc_count_newlines(matched, matched_len);
    if (pgc_push_if(true) < 0)
        LEX_ERROR_AT("too many nested EXEC SQL IFDEF conditions");
    LEX_TRANSITION(PGC_STATE_XCOND);
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    }
}
<C, XSKIP> rule cx_dollar_ifndef matches /\${ifndef_kw}{space}+/ {
    if (!INFORMIX_MODE) {
        LEX_PUSHBACK(matched_len - 1);
        emit(user, PGC_TOK_RAW_CHAR, matched, 1);
        pgc_terminate(user, matched, 1);
        LEX_SKIP();
        LEX_TERMINATE();
    } else {
    pgc_count_newlines(matched, matched_len);
    if (pgc_push_if(false) < 0)
        LEX_ERROR_AT("too many nested EXEC SQL IFDEF conditions");
    LEX_TRANSITION(PGC_STATE_XCOND);
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    }
}
<C, XSKIP> rule cx_dollar_elif matches /\${elif_kw}{space}+/ {
    int rc;
    if (!INFORMIX_MODE) {
        LEX_PUSHBACK(matched_len - 1);
        emit(user, PGC_TOK_RAW_CHAR, matched, 1);
        pgc_terminate(user, matched, 1);
        LEX_SKIP();
        LEX_TERMINATE();
    } else {
    pgc_count_newlines(matched, matched_len);
    rc = pgc_handle_elif();
    if (rc < 0)
        LEX_ERROR_AT("missing matching \"EXEC SQL IFDEF\" / \"EXEC SQL IFNDEF\"");
    if (rc == 1)
        LEX_ERROR_AT("missing \"EXEC SQL ENDIF;\"");
    LEX_TRANSITION(PGC_STATE_XCOND);
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    }
}
<C, XSKIP> rule cx_dollar_else matches /\${else_kw}{space}*;/ {
    int rc;
    if (!INFORMIX_MODE) {
        LEX_PUSHBACK(matched_len - 1);
        emit(user, PGC_TOK_RAW_CHAR, matched, 1);
        pgc_terminate(user, matched, 1);
        LEX_SKIP();
        LEX_TERMINATE();
    } else {
    pgc_count_newlines(matched, matched_len);
    rc = pgc_handle_else();
    if (rc < 0)
        LEX_ERROR_AT("missing matching \"EXEC SQL IFDEF\" / \"EXEC SQL IFNDEF\"");
    if (rc == 1)
        LEX_ERROR_AT("more than one EXEC SQL ELSE");
    LEX_TRANSITION(pgc_active_state());
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    }
}
<C, XSKIP> rule cx_dollar_endif matches /\${endif_kw}{space}*;/ {
    int rc;
    if (!INFORMIX_MODE) {
        LEX_PUSHBACK(matched_len - 1);
        emit(user, PGC_TOK_RAW_CHAR, matched, 1);
        pgc_terminate(user, matched, 1);
        LEX_SKIP();
        LEX_TERMINATE();
    } else {
    pgc_count_newlines(matched, matched_len);
    rc = pgc_handle_endif();
    if (rc < 0)
        LEX_ERROR_AT("unmatched EXEC SQL ENDIF");
    LEX_TRANSITION(pgc_active_state());
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    }
}

/* ----- bare $ in C (INFORMIX_MODE): enter SQL state ----- */
<C> rule c_dollar_bare matches /\$/ {
    if (INFORMIX_MODE) {
        LEX_TRANSITION(PGC_STATE_SQL);
        emit(user, PGC_TOK_SQL_START, matched, matched_len);
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
    }
    emit(user, PGC_TOK_RAW_CHAR, matched, 1);
    pgc_terminate(user, matched, 1);
    LEX_SKIP();
    LEX_TERMINATE();
}

/*
 * ===================================================================
 * C state: rest of lexing (comments, strings, idents, numerics, ops) --
 * ===================================================================
 */

/* ----- # cpp directives in C ----- */
<C> rule c_cppinclude_next matches /{space}*#{non_newline_space}*{include_next_kw}{non_newline_space}*/ {
    pgc_count_newlines(matched, matched_len);
    if (system_includes) {
        pgc_set_include_next(true);
        LEX_TRANSITION(PGC_STATE_INCL);
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    } else {
        emit(user, PGC_TOK_CPP_LINE, matched, matched_len);
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    }
}
<C> rule c_cppinclude matches /{space}*#{non_newline_space}*{include_kw}{non_newline_space}*/ {
    pgc_count_newlines(matched, matched_len);
    if (system_includes) {
        pgc_set_include_next(false);
        LEX_TRANSITION(PGC_STATE_INCL);
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    } else {
        emit(user, PGC_TOK_CPP_LINE, matched, matched_len);
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
        LEX_TERMINATE();
    }
}

/* C-state catch-all preprocessor line: #if/#ifdef/#ifndef/#define
 * /#undef/#pragma/#error/etc.  Pattern excludes #include and
 * #include_next (which the rules above match more specifically;
 * Lime's longest-match-wins favours those when they apply, but
 * the leading-letter exclusion below also rules out a stray match
 * here in case the input has a malformed include).  We use
 * non_newline_space to avoid eating preceding newlines. */
<C> rule c_cppline_other matches /{non_newline_space}*#{non_newline_space}*[^iI\n][A-Za-z_0-9]*([^\n\\]|\\[^\n]|\\{non_newline_space}*\n)*\n/ {
    pgc_count_newlines(matched, matched_len);
    emit(user, PGC_TOK_CPP_LINE, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_cppline_iif matches /{non_newline_space}*#{non_newline_space}*[iI][fFmMpP][A-Za-z_0-9]*([^\n\\]|\\[^\n]|\\{non_newline_space}*\n)*\n/ {
    pgc_count_newlines(matched, matched_len);
    emit(user, PGC_TOK_CPP_LINE, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- C-state ccomment // ... \n ----- */
<C, XSKIP> rule cx_ccomment matches /{ccomment}/ {
    pgc_count_newlines(matched, matched_len);
    pgc_echo(matched, matched_len);
    LEX_SKIP();
}

/* ----- /* extended block comment open ----- */
<C> rule c_xcstart matches /{xcstart}/ {
    pgc_count_newlines(matched, matched_len);
    pgc_state_before_str_start_set(PGC_STATE_C);
    pgc_xcdepth_set(0);
    LEX_PUSHBACK(matched_len - 2);
    pgc_echo("/*", 2);
    LEX_TRANSITION(PGC_STATE_XC);
    LEX_SKIP();
}

/* ----- C-state quoted strings ----- */
<C> rule c_xqc_open matches /'/ {
    pgc_state_before_str_start_set(PGC_STATE_C);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_XQC);
    LEX_SKIP();
}
<C> rule c_xdc_open matches /"/ {
    pgc_state_before_str_start_set(PGC_STATE_C);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_XDC);
    LEX_SKIP();
}

/* ----- nested * / outside comment is an error ----- */
<C> rule c_stray_xcstop matches /\*\// {
    pgc_emit_error("nested /* ... */ comments");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- Multi-char C operators (longest first) ----- */
<C> rule c_mempoint matches /->\*/ {
    emit(user, PGC_TOK_S_MEMPOINT, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_dotpoint matches /\.\*/ {
    emit(user, PGC_TOK_S_DOTPOINT, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_member matches /->/ {
    emit(user, PGC_TOK_S_MEMBER, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_rshift matches />>/ {
    emit(user, PGC_TOK_S_RSHIFT, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_lshift matches /<</ {
    emit(user, PGC_TOK_S_LSHIFT, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_or matches /\|\|/ {
    emit(user, PGC_TOK_S_OR, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_and matches /&&/ {
    emit(user, PGC_TOK_S_AND, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_inc matches /\+\+/ {
    emit(user, PGC_TOK_S_INC, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_dec matches /--/ {
    emit(user, PGC_TOK_S_DEC, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_eqeq matches /==/ {
    emit(user, PGC_TOK_S_EQUAL, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_neq matches /!=/ {
    emit(user, PGC_TOK_S_NEQUAL, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_addassign matches /\+=/ {
    emit(user, PGC_TOK_S_ADD, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_subassign matches /-=/ {
    emit(user, PGC_TOK_S_SUB, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_mulassign matches /\*=/ {
    emit(user, PGC_TOK_S_MUL, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_divassign matches /\/=/ {
    emit(user, PGC_TOK_S_DIV, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_modassign matches /%=/ {
    emit(user, PGC_TOK_S_MOD, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- C-state numerics (declaration order = priority on tie) ----- */
<C> rule c_real matches /{real}/ {
    emit(user, PGC_TOK_FCONST_REAL, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_realfail matches /{realfail}/ {
    LEX_PUSHBACK(2);
    emit(user, PGC_TOK_ICONST_DEC, matched, matched_len - 2);
    pgc_terminate(user, matched, matched_len - 2);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_numeric matches /{numeric}/ {
    emit(user, PGC_TOK_FCONST_NUMERIC, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_numericfail matches /{numericfail}/ {
    LEX_PUSHBACK(2);
    emit(user, PGC_TOK_ICONST_DEC, matched, matched_len - 2);
    pgc_terminate(user, matched, matched_len - 2);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_hexinteger matches /{hexinteger}/ {
    emit(user, PGC_TOK_ICONST_HEX, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_decinteger matches /{decinteger}/ {
    emit(user, PGC_TOK_ICONST_DEC, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- C-state identifier ----- */
<C> rule c_ident matches /{identifier}/ {
    pgc_track_function(matched, matched_len);
    if (pgc_handle_c_ident(matched, matched_len, user, lex)) {
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
    }
    /* Define expansion happened: terminate so the driver switches
     * to the just-pushed expansion buffer on next pull. */
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- C-state whitespace: ECHO ----- */
<C, XSKIP> rule cx_space matches /{space}+/ {
    pgc_count_newlines(matched, matched_len);
    pgc_echo(matched, matched_len);
    LEX_SKIP();
}

/* ----- XSKIP: ignore everything else (single bytes), no echo ----- */
<XSKIP> rule xskip_anything matches /[\x00-\xff]/ {
    LEX_SKIP();
}

/* ----- C-state single-char tokens ----- */
<C> rule c_lparen matches /\(/ {
    pgc_track_paren(1);
    emit(user, PGC_TOK_RAW_CHAR, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_rparen matches /\)/ {
    pgc_track_paren(-1);
    emit(user, PGC_TOK_RAW_CHAR, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<C> rule c_self matches /[:;,*%\/+\-{}\[\]=]/ {
    emit(user, PGC_TOK_RAW_CHAR, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- C-state catch-all: emit S_ANYTHING ----- */
<C> rule c_anything matches /[\x00-\xff]/ {
    emit(user, PGC_TOK_S_ANYTHING, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/*
 * ===================================================================
 * SQL state                                                          --
 * ===================================================================
 */

/* ----- SQL whitespace: ignore (NO ECHO) ----- */
<SQL> rule sql_ws matches /{whitespace}+/ {
    pgc_count_newlines(matched, matched_len);
    LEX_SKIP();
}

/* ----- SQL extended comment open ----- */
<SQL> rule sql_xcstart matches /{xcstart}/ {
    pgc_count_newlines(matched, matched_len);
    pgc_state_before_str_start_set(PGC_STATE_SQL);
    pgc_xcdepth_set(0);
    LEX_PUSHBACK(matched_len - 2);
    pgc_echo("/*", 2);
    LEX_TRANSITION(PGC_STATE_XC);
    LEX_SKIP();
}

/* ----- SQL bit / hex / national / extended / unicode strings ----- */
<SQL> rule sql_xb_open matches /[bB]'/ {
    pgc_state_before_str_start_set(PGC_STATE_SQL);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_XB);
    LEX_SKIP();
}
<SQL> rule sql_xh_open matches /[xX]'/ {
    pgc_state_before_str_start_set(PGC_STATE_SQL);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_XH);
    LEX_SKIP();
}
<SQL> rule sql_xn_open matches /[nN]'/ {
    pgc_state_before_str_start_set(PGC_STATE_SQL);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_XN);
    LEX_SKIP();
}
<SQL> rule sql_xe_open matches /[eE]'/ {
    pgc_state_before_str_start_set(PGC_STATE_SQL);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_XE);
    LEX_SKIP();
}
<SQL> rule sql_xus_open matches /[uU]&'/ {
    pgc_state_before_str_start_set(PGC_STATE_SQL);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_XUS);
    LEX_SKIP();
}
<SQL> rule sql_xui_open matches /[uU]&"/ {
    pgc_state_before_str_start_set(PGC_STATE_SQL);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_XUI);
    LEX_SKIP();
}
<SQL> rule sql_xq_open matches /'/ {
    pgc_state_before_str_start_set(PGC_STATE_SQL);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_XQ);
    LEX_SKIP();
}
<SQL> rule sql_xd_open matches /"/ {
    pgc_state_before_str_start_set(PGC_STATE_SQL);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_XD);
    LEX_SKIP();
}

/* ----- SQL dollar-quoted strings ----- */
<SQL> rule sql_dolq_open matches /{dolqdelim}/ {
    pgc_set_dolqstart(matched, matched_len);
    pgc_lit_start();
    pgc_addlit(matched, matched_len);
    LEX_TRANSITION(PGC_STATE_XDOLQ);
    LEX_SKIP();
}
<SQL> rule sql_dolq_failed matches /{dolqfailed}/ {
    LEX_PUSHBACK(matched_len - 1);
    emit(user, PGC_TOK_RAW_CHAR, matched, 1);
    pgc_terminate(user, matched, 1);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- SQL multi-char tokens ----- */
<SQL> rule sql_typecast matches /::/ {
    emit(user, PGC_TOK_TYPECAST, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_dot_dot matches /\.\./ {
    emit(user, PGC_TOK_DOT_DOT, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_colon_eq matches /:=/ {
    emit(user, PGC_TOK_COLON_EQUALS, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- SQL IP literal (declared before numerics so it wins ties) ----- */
<SQL> rule sql_ip matches /{ip}/ {
    emit(user, PGC_TOK_IP, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- SQL parameter ----- */
<SQL> rule sql_param_junk matches /{param_junk}/ {
    pgc_emit_error("trailing junk after parameter");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_param matches /{param}/ {
    emit(user, PGC_TOK_PARAM, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- INFORMIX bare $ in SQL state: emit as ':' ----- */
<SQL> rule sql_dollar matches /\$/ {
    if (INFORMIX_MODE) {
        emit(user, PGC_TOK_RAW_CHAR_COLON, matched, 1);
        pgc_terminate(user, matched, 1);
        LEX_SKIP();
    }
    emit(user, PGC_TOK_RAW_CHAR, matched, 1);
    pgc_terminate(user, matched, 1);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- SQL numerics ----- */
<SQL> rule sql_real matches /{real}/ {
    emit(user, PGC_TOK_FCONST_REAL, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_realfail matches /{realfail}/ {
    LEX_PUSHBACK(2);
    emit(user, PGC_TOK_ICONST_DEC, matched, matched_len - 2);
    pgc_terminate(user, matched, matched_len - 2);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_numeric matches /{numeric}/ {
    emit(user, PGC_TOK_FCONST_NUMERIC, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_numericfail matches /{numericfail}/ {
    LEX_PUSHBACK(2);
    emit(user, PGC_TOK_ICONST_DEC, matched, matched_len - 2);
    pgc_terminate(user, matched, matched_len - 2);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_hexinteger matches /{hexinteger}/ {
    emit(user, PGC_TOK_ICONST_HEX, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_octinteger matches /{octinteger}/ {
    emit(user, PGC_TOK_ICONST_OCT, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_bininteger matches /{bininteger}/ {
    emit(user, PGC_TOK_ICONST_BIN, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_decinteger matches /{decinteger}/ {
    emit(user, PGC_TOK_ICONST_DEC, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_hexfail matches /{hexfail}/ {
    pgc_emit_error("trailing junk after numeric literal");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_octfail matches /{octfail}/ {
    pgc_emit_error("trailing junk after numeric literal");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_binfail matches /{binfail}/ {
    pgc_emit_error("trailing junk after numeric literal");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_integer_junk matches /{integer_junk}/ {
    pgc_emit_error("trailing junk after numeric literal");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_numeric_junk matches /{numeric_junk}/ {
    pgc_emit_error("trailing junk after numeric literal");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<SQL> rule sql_real_junk matches /{real_junk}/ {
    pgc_emit_error("trailing junk after numeric literal");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/*
 * ----- SQL CVARIABLE: :ident with optional ->/./[...] chains -----
 * The full regex is complex; do prefix-match on `:ident` then pull
 * the rest in a driver helper that returns the consumed length.
 */
<SQL> rule sql_cvariable matches /{cvariable}/ {
    emit(user, PGC_TOK_CVARIABLE, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- SQL operator ----- */
<SQL> rule sql_operator matches /{operator}/ {
    size_t keep = pgc_op_keep(matched, matched_len);
    if (keep < matched_len) {
        LEX_PUSHBACK(matched_len - keep);
        matched_len = keep;
    }
    emit(user, PGC_TOK_OP, matched, matched_len);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- SQL identifier ----- */
<SQL> rule sql_ident matches /{identifier}/ {
    if (pgc_handle_sql_ident(matched, matched_len, user, lex)) {
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
    }
    /* Define expansion happened: terminate so the driver switches
     * to the just-pushed expansion buffer on next pull. */
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- SQL self-chars ----- */
<SQL> rule sql_self matches /{self}/ {
    if (matched[0] == ';' && struct_level == 0) {
        emit(user, PGC_TOK_RAW_CHAR, matched, 1);
        LEX_TRANSITION(PGC_STATE_C);
    } else {
        emit(user, PGC_TOK_RAW_CHAR, matched, 1);
    }
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}

/* ----- SQL catch-all: emit raw byte ----- */
<SQL> rule sql_other matches /[\x00-\xff]/ {
    emit(user, PGC_TOK_RAW_CHAR, matched, 1);
    pgc_terminate(user, matched, 1);
    LEX_SKIP();
    LEX_TERMINATE();
}

/*
 * ===================================================================
 * XB / XH: bit-string / hex-string body
 * ===================================================================
 */
<XB, XH> rule xbh_close matches /'/ {
    pgc_state_before_str_stop_set(state);
    LEX_TRANSITION(PGC_STATE_XQS);
    LEX_SKIP();
}
<XB, XH> rule xbh_inside matches /{xbinside}/ {
    pgc_addlit(matched, matched_len);
    pgc_count_newlines(matched, matched_len);
    LEX_SKIP();
}
<XB> rule xb_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unterminated bit string literal");
}
<XH> rule xh_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unterminated hexadecimal string literal");
}

/*
 * ===================================================================
 * XQ / XQC / XN / XUS: quoted string bodies
 * ===================================================================
 */
<XQ, XN, XUS> rule xqxnus_double matches /{xqdouble}/ {
    pgc_addlit(matched, matched_len);
    LEX_SKIP();
}
<XQ, XQC, XN, XUS> rule xq_close matches /'/ {
    pgc_state_before_str_stop_set(state);
    LEX_TRANSITION(PGC_STATE_XQS);
    LEX_SKIP();
}
<XQC> rule xqc_escape matches /\\'/ {
    pgc_addlit(matched, matched_len);
    LEX_SKIP();
}
<XQC> rule xqc_inside matches /[^']+/ {
    pgc_addlit(matched, matched_len);
    pgc_count_newlines(matched, matched_len);
    LEX_SKIP();
}
<XQ, XN, XUS> rule xqnus_inside matches /{xqinside}/ {
    pgc_addlit(matched, matched_len);
    pgc_count_newlines(matched, matched_len);
    LEX_SKIP();
}
<XQ, XQC, XN, XUS> rule xq_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unterminated quoted string");
}

/*
 * ===================================================================
 * XE: extended quoted string body with C-style escapes
 * ===================================================================
 */
<XE> rule xe_double matches /{xqdouble}/ {
    pgc_addlit(matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_unicode matches /{xeunicode}/ {
    pgc_addlit(matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_hexesc matches /{xehexesc}/ {
    pgc_addlit(matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_octesc matches /{xeoctesc}/ {
    pgc_addlit(matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_escape matches /\\[^0-7]/ {
    pgc_addlit(matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_inside matches /{xeinside}/ {
    pgc_addlit(matched, matched_len);
    pgc_count_newlines(matched, matched_len);
    LEX_SKIP();
}
<XE> rule xe_close matches /'/ {
    pgc_state_before_str_stop_set(state);
    LEX_TRANSITION(PGC_STATE_XQS);
    LEX_SKIP();
}
<XE> rule xe_lone_backslash matches /\\/ {
    pgc_addlitchar('\\');
    LEX_SKIP();
}
<XE> rule xe_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unterminated quoted string");
}

/*
 * ===================================================================
 * XQS: quote-stop continuation lookahead
 * ===================================================================
 */
<XQS> rule xqs_continue matches /{quotecontinue}/ {
    pgc_count_newlines(matched, matched_len);
    LEX_TRANSITION(pgc_state_before_str_stop());
    LEX_SKIP();
}
<XQS> rule xqs_other matches /[\x00-\xff]/ {
    LEX_PUSHBACK(matched_len);
    pgc_emit_string_token_for(user, lex);
    LEX_TRANSITION(pgc_state_before_str_start());
    pgc_terminate(user, matched, 0);
    LEX_SKIP();
    LEX_TERMINATE();
}
<XQS> rule xqs_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    pgc_emit_string_token_for(user, lex);
    LEX_TRANSITION(pgc_state_before_str_start());
    pgc_terminate(user, matched, 0);
    LEX_SKIP();
    LEX_TERMINATE();
}

/*
 * ===================================================================
 * XDOLQ: dollar-quoted string body
 * ===================================================================
 */
<XDOLQ> rule xdolq_close matches /{dolqdelim}/ {
    if (pgc_dolq_match(matched, matched_len)) {
        pgc_addlit(matched, matched_len);
        pgc_state_before_str_stop_set(state);
        pgc_emit_xdolq(user);
        LEX_TRANSITION(PGC_STATE_SQL);
        pgc_terminate(user, matched, matched_len);
        LEX_SKIP();
    }
    pgc_addlit(matched, matched_len - 1);
    LEX_PUSHBACK(1);
    LEX_SKIP();
}
<XDOLQ> rule xdolq_failed matches /{dolqfailed}/ {
    pgc_addlit(matched, matched_len);
    LEX_SKIP();
}
<XDOLQ> rule xdolq_inside matches /{dolqinside}/ {
    pgc_addlit(matched, matched_len);
    pgc_count_newlines(matched, matched_len);
    LEX_SKIP();
}
<XDOLQ> rule xdolq_lone_dollar matches /\$/ {
    pgc_addlitchar('$');
    LEX_SKIP();
}
<XDOLQ> rule xdolq_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unterminated dollar-quoted string");
}

/*
 * ===================================================================
 * XD / XUI: delimited identifier body
 * ===================================================================
 */
<XD, XUI> rule xd_double matches /{xddouble}/ {
    pgc_addlit(matched, matched_len);
    LEX_SKIP();
}
<XD, XUI> rule xd_close matches /"/ {
    int prev = state;
    LEX_TRANSITION(pgc_state_before_str_start());
    pgc_emit_xd_close(user, prev);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<XD, XUI> rule xd_inside matches /{xdinside}/ {
    pgc_addlit(matched, matched_len);
    pgc_count_newlines(matched, matched_len);
    LEX_SKIP();
}
<XD> rule xd_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unterminated quoted identifier");
}
<XUI> rule xui_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unterminated quoted identifier");
}

/*
 * ===================================================================
 * XDC: C double-quoted string body
 * ===================================================================
 */
<XDC> rule xdc_close matches /"/ {
    LEX_TRANSITION(pgc_state_before_str_start());
    pgc_emit_xdc(user);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<XDC> rule xdc_inside matches /{xdcinside}/ {
    pgc_addlit(matched, matched_len);
    pgc_count_newlines(matched, matched_len);
    LEX_SKIP();
}
<XDC> rule xdc_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unterminated quoted string");
}

/*
 * ===================================================================
 * XC: extended block comment with op_chars in start, nested via xcdepth
 * ===================================================================
 */
<XC> rule xc_open_inner matches /{xcstart}/ {
    if (pgc_state_before_str_start() == PGC_STATE_SQL) {
        pgc_xcdepth_inc();
        LEX_PUSHBACK(matched_len - 2);
        pgc_echo("/*", 2);
        LEX_SKIP();
    } else {
        pgc_echo(matched, matched_len);
        pgc_count_newlines(matched, matched_len);
        LEX_SKIP();
    }
}
<XC> rule xc_close matches /{xcstop}/ {
    if (pgc_state_before_str_start() == PGC_STATE_SQL) {
        if (pgc_xcdepth() <= 0) {
            pgc_echo(matched, matched_len);
            LEX_TRANSITION(PGC_STATE_SQL);
            LEX_SKIP();
        } else {
            pgc_xcdepth_dec();
            pgc_echo("*/", 2);
            LEX_SKIP();
        }
    } else {
        pgc_echo(matched, matched_len);
        LEX_TRANSITION(PGC_STATE_C);
        LEX_SKIP();
    }
}
<XC> rule xc_inside matches /{xcinside}/ {
    pgc_count_newlines(matched, matched_len);
    pgc_echo(matched, matched_len);
    LEX_SKIP();
}
<XC> rule xc_op_chars matches /{op_chars}/ {
    pgc_echo(matched, matched_len);
    LEX_SKIP();
}
<XC> rule xc_stars matches /\*+/ {
    pgc_echo(matched, matched_len);
    LEX_SKIP();
}
<XC> rule xc_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unterminated /* comment");
}

/*
 * ===================================================================
 * XCOND: identifier;<space>* lookup for IFDEF/IFNDEF condition
 * ===================================================================
 */
<XCOND> rule xcond_ident_semi matches /{identifier}{space}*;/ {
    pgc_handle_ifdef_ident(matched, matched_len);
    LEX_TRANSITION(pgc_active_state());
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<XCOND> rule xcond_other matches /[\x00-\xff]/ {
    pgc_emit_error("missing identifier in EXEC SQL IFDEF command");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<XCOND> rule xcond_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("missing identifier in EXEC SQL IFDEF command");
}

/*
 * ===================================================================
 * DEFI: capture symbol name for EXEC SQL DEFINE
 * ===================================================================
 */
<DEFI> rule defi_ident matches /{identifier}/ {
    pgc_set_def_symbol(matched, matched_len);
    pgc_lit_start();
    LEX_TRANSITION(PGC_STATE_DEF);
    LEX_SKIP();
}
<DEFI> rule defi_other matches /[\x00-\xff]/ {
    pgc_emit_error("missing identifier in EXEC SQL DEFINE command");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<DEFI> rule defi_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("missing identifier in EXEC SQL DEFINE command");
}

/*
 * ===================================================================
 * DEF: accumulate value bytes until ;
 * ===================================================================
 */
<DEF> rule def_semi matches /{space}*;/ {
    pgc_count_newlines(matched, matched_len);
    pgc_commit_def();
    LEX_TRANSITION(PGC_STATE_C);
    LEX_SKIP();
}
<DEF> rule def_byte matches /[^;]/ {
    pgc_addlit(matched, matched_len);
    pgc_count_newlines(matched, matched_len);
    LEX_SKIP();
}
<DEF> rule def_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unterminated EXEC SQL DEFINE");
}

/*
 * ===================================================================
 * UNDEF: identifier{space}*;
 * ===================================================================
 */
<UNDEF> rule undef_ident_semi matches /{identifier}{space}*;/ {
    pgc_handle_undef(matched, matched_len);
    LEX_TRANSITION(PGC_STATE_C);
    LEX_SKIP();
}
<UNDEF> rule undef_other matches /[\x00-\xff]/ {
    pgc_emit_error("missing identifier in EXEC SQL UNDEF command");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<UNDEF> rule undef_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("missing identifier in EXEC SQL UNDEF command");
}

/*
 * ===================================================================
 * INCL: <foo>; or "foo"; or bare;
 * ===================================================================
 */
<INCL> rule incl_angle matches /<[^>]+>{space}*;?/ {
    pgc_count_newlines(matched, matched_len);
    pgc_do_include(matched, matched_len, lex);
    LEX_TRANSITION(PGC_STATE_C);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<INCL> rule incl_dquote matches /"[^"]+"{space}*;?/ {
    pgc_count_newlines(matched, matched_len);
    pgc_do_include(matched, matched_len, lex);
    LEX_TRANSITION(PGC_STATE_C);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<INCL> rule incl_bare matches /[^;<>"\n]+;/ {
    pgc_count_newlines(matched, matched_len);
    pgc_do_include(matched, matched_len, lex);
    LEX_TRANSITION(PGC_STATE_C);
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<INCL> rule incl_other matches /[\x00-\xff]/ {
    pgc_emit_error("syntax error in EXEC SQL INCLUDE command");
    pgc_terminate(user, matched, matched_len);
    LEX_SKIP();
    LEX_TERMINATE();
}
<INCL> rule incl_eof matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) LEX_SKIP();
    LEX_ERROR_AT("unexpected end of input in EXEC SQL INCLUDE");
}

/*
 * ===================================================================
 * EOF rules for outer states.  When a frame pops (LexInclude), Lime
 * fires the per-state EOF rule for the frame being popped.  At
 * depth > 0 (still inside an include/define expansion), suppress
 * the error and let auto-pop continue.  At depth == 0 (top-level
 * buffer exhausted), the top-of-stack rule applies.
 * ===================================================================
 */
<C, SQL, XSKIP> rule eof_normal matches <<EOF>> {
    if (PgcLexIncludeDepth(lex) > 0) {
        pgc_handle_pop(lex);
        LEX_SKIP();
    } else {
        pgc_handle_top_eof();
        LEX_SKIP();
    }
}
