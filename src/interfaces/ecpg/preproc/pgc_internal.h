/*-------------------------------------------------------------------------
 *
 * pgc_internal.h
 *	  Private interface between pgc.lex and pgc.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/interfaces/ecpg/preproc/pgc_internal.h
 *-------------------------------------------------------------------------
 */
#ifndef PGC_INTERNAL_H
#define PGC_INTERNAL_H

#include <stdbool.h>
#include <stddef.h>

#define PGC_TOK_BASE                  1000
#define PGC_TOK_RAW_CHAR              (PGC_TOK_BASE + 1)
#define PGC_TOK_RAW_CHAR_COLON        (PGC_TOK_BASE + 2)
#define PGC_TOK_TYPECAST              (PGC_TOK_BASE + 3)
#define PGC_TOK_DOT_DOT               (PGC_TOK_BASE + 4)
#define PGC_TOK_COLON_EQUALS          (PGC_TOK_BASE + 5)
#define PGC_TOK_OP                    (PGC_TOK_BASE + 6)
#define PGC_TOK_PARAM                 (PGC_TOK_BASE + 7)
#define PGC_TOK_ICONST_DEC            (PGC_TOK_BASE + 8)
#define PGC_TOK_ICONST_HEX            (PGC_TOK_BASE + 9)
#define PGC_TOK_ICONST_OCT            (PGC_TOK_BASE + 10)
#define PGC_TOK_ICONST_BIN            (PGC_TOK_BASE + 11)
#define PGC_TOK_FCONST_NUMERIC        (PGC_TOK_BASE + 12)
#define PGC_TOK_FCONST_REAL           (PGC_TOK_BASE + 13)
#define PGC_TOK_IDENT                 (PGC_TOK_BASE + 14)
#define PGC_TOK_CSTRING               (PGC_TOK_BASE + 15)
#define PGC_TOK_UIDENT                (PGC_TOK_BASE + 16)
#define PGC_TOK_BCONST                (PGC_TOK_BASE + 17)
#define PGC_TOK_XCONST                (PGC_TOK_BASE + 18)
#define PGC_TOK_SCONST                (PGC_TOK_BASE + 19)
#define PGC_TOK_USCONST               (PGC_TOK_BASE + 20)
#define PGC_TOK_IP                    (PGC_TOK_BASE + 21)
#define PGC_TOK_CVARIABLE             (PGC_TOK_BASE + 22)
#define PGC_TOK_CPP_LINE              (PGC_TOK_BASE + 23)
#define PGC_TOK_SQL_START             (PGC_TOK_BASE + 24)
#define PGC_TOK_S_ANYTHING            (PGC_TOK_BASE + 25)
#define PGC_TOK_S_MEMBER              (PGC_TOK_BASE + 26)
#define PGC_TOK_S_MEMPOINT            (PGC_TOK_BASE + 27)
#define PGC_TOK_S_DOTPOINT            (PGC_TOK_BASE + 28)
#define PGC_TOK_S_RSHIFT              (PGC_TOK_BASE + 29)
#define PGC_TOK_S_LSHIFT              (PGC_TOK_BASE + 30)
#define PGC_TOK_S_OR                  (PGC_TOK_BASE + 31)
#define PGC_TOK_S_AND                 (PGC_TOK_BASE + 32)
#define PGC_TOK_S_INC                 (PGC_TOK_BASE + 33)
#define PGC_TOK_S_DEC                 (PGC_TOK_BASE + 34)
#define PGC_TOK_S_EQUAL               (PGC_TOK_BASE + 35)
#define PGC_TOK_S_NEQUAL              (PGC_TOK_BASE + 36)
#define PGC_TOK_S_ADD                 (PGC_TOK_BASE + 37)
#define PGC_TOK_S_SUB                 (PGC_TOK_BASE + 38)
#define PGC_TOK_S_MUL                 (PGC_TOK_BASE + 39)
#define PGC_TOK_S_DIV                 (PGC_TOK_BASE + 40)
#define PGC_TOK_S_MOD                 (PGC_TOK_BASE + 41)

extern void pgc_lit_start(void);
extern void pgc_addlit(const char *text, size_t len);
extern void pgc_addlitchar(unsigned char c);
extern char *pgc_lit_take(size_t *out_len);
extern size_t pgc_lit_len(void);
extern const char *pgc_lit_peek(void);

extern int	pgc_state_before_str_start(void);
extern void pgc_state_before_str_start_set(int s);
extern int	pgc_state_before_str_stop(void);
extern void pgc_state_before_str_stop_set(int s);
extern int	pgc_xcdepth(void);
extern void pgc_xcdepth_set(int d);
extern void pgc_xcdepth_inc(void);
extern void pgc_xcdepth_dec(void);
extern void pgc_set_dolqstart(const char *text, size_t len);
extern bool pgc_dolq_match(const char *text, size_t len);
extern void pgc_set_include_next(bool v);
extern void pgc_set_def_symbol(const char *text, size_t len);

extern void pgc_echo(const char *text, size_t len);
extern void pgc_count_newlines(const char *text, size_t len);
extern void pgc_track_paren(int delta);
extern void pgc_track_function(const char *text, size_t len);
extern size_t pgc_op_keep(const char *text, size_t len);

extern bool pgc_handle_c_ident(const char *text, size_t len, void *user, void *lex);
extern bool pgc_handle_sql_ident(const char *text, size_t len, void *user, void *lex);
extern size_t pgc_consume_cvariable_tail(const char *text, size_t len);

extern int	pgc_push_if(bool is_ifdef);
extern int	pgc_handle_elif(void);
extern int	pgc_handle_else(void);
extern int	pgc_handle_endif(void);
extern int	pgc_active_state(void);
extern void pgc_handle_ifdef_ident(const char *text, size_t len);
extern void pgc_commit_def(void);
extern void pgc_handle_undef(const char *text, size_t len);

extern void pgc_emit_string_token_for(void *user, void *lex);
extern void pgc_emit_xdolq(void *user);
extern void pgc_emit_xd_close(void *user, int prev_state);
extern void pgc_emit_xdc(void *user);
extern void pgc_emit_error(const char *msg);

extern void pgc_do_include(const char *text, size_t len, void *lex);
extern void pgc_handle_pop(void *lex);
extern void pgc_handle_top_eof(void);

extern void pgc_terminate(void *user, const char *match_end_ptr,
						  size_t match_consumed);

#endif
