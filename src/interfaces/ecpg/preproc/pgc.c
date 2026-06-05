/*-------------------------------------------------------------------------
 *
 * pgc.c
 *	  Driver shim around the Lime-generated lexer (pgc.lex).
 *
 * Phase 5 ecpg pgc port: the state machine moved to pgc.lex
 * (Lime v0.2.2's lexer subsystem).  This file is the public-API
 * shim that:
 *
 *   - Owns the InputBuffer stack (EXEC SQL INCLUDE files,
 *     EXEC SQL DEFINE expansion strings).  The legacy hand-rolled
 *     scanner used the same shape; we keep it because Lime's
 *     LexInclude pushes a frame that LEX_TERMINATE would drop
 *     together with the calling LexFeedBytes frame.
 *
 *   - Implements the public base_yylex API expected by parser.c:
 *     pull-mode, returns int token code, sets base_yytext /
 *     base_yyleng / base_yylineno / base_yylval.
 *
 *   - Runs a per-token LexFeedBytes loop: each base_yylex pull
 *     feeds the current InputBuffer's remaining bytes; .lex's
 *     action body emits at most one token via emit() and calls
 *     pgc_terminate + LEX_TERMINATE.  The driver advances the
 *     buffer cursor by the consumed-byte count, then returns.
 *
 *   - Provides every pgc_* helper called from pgc.lex action
 *     bodies (see pgc_internal.h for the contract).
 *
 * Per-token feeding (rather than full pre-scan FIFO) is required
 * because ecpg's parser mutates file-static globals read by the
 * scanner (struct_level, braces_open, parenths_open) inside its
 * reduce actions.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/interfaces/ecpg/preproc/pgc.c
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <ctype.h>
#include <limits.h>

#include "common/string.h"

#include "preproc_extern.h"
#include "preproc_yytype.h"
#include "preproc.h"
#include "pgc_internal.h"
#include "pgc_lex.h"

extern YYSTYPE base_yylval;

/* Public globals. */
char	   *base_yytext = NULL;
int			base_yyleng = 0;
int			base_yylineno = 1;
FILE	   *base_yyin = NULL;
FILE	   *base_yyout = NULL;
char	   *token_start = NULL;

/* Writable copy of yytext (some helpers mutate it in place). */
static char *yytext_buf = NULL;
static size_t yytext_alloc = 0;

static void
set_yytext(const char *src, int len)
{
	if ((size_t) len + 1 > yytext_alloc)
	{
		size_t		newsz = yytext_alloc ? yytext_alloc : 256;

		while (newsz < (size_t) len + 1)
			newsz *= 2;
		yytext_buf = (char *) realloc(yytext_buf, newsz);
		if (yytext_buf == NULL)
			mmfatal(OUT_OF_MEMORY, "out of memory");
		yytext_alloc = newsz;
	}
	if (len > 0)
		memcpy(yytext_buf, src, len);
	yytext_buf[len] = '\0';
	base_yytext = yytext_buf;
	base_yyleng = len;
}

/* Literal accumulator. */
static char *literalbuf = NULL;
static int	literallen;
static int	literalalloc;

void
pgc_lit_start(void)
{
	if (literalbuf == NULL)
	{
		literalalloc = 1024;
		literalbuf = (char *) mm_alloc(literalalloc);
	}
	literalbuf[0] = '\0';
	literallen = 0;
}

void
pgc_addlit(const char *text, size_t len)
{
	if (literalbuf == NULL)
		pgc_lit_start();
	if ((size_t) literallen + len >= (size_t) literalalloc)
	{
		do
			literalalloc *= 2;
		while ((size_t) literallen + len >= (size_t) literalalloc);
		literalbuf = (char *) realloc(literalbuf, literalalloc);
		if (literalbuf == NULL)
			mmfatal(OUT_OF_MEMORY, "out of memory");
	}
	memcpy(literalbuf + literallen, text, len);
	literallen += (int) len;
	literalbuf[literallen] = '\0';
}

void
pgc_addlitchar(unsigned char c)
{
	if (literalbuf == NULL)
		pgc_lit_start();
	if (literallen + 1 >= literalalloc)
	{
		literalalloc *= 2;
		literalbuf = (char *) realloc(literalbuf, literalalloc);
		if (literalbuf == NULL)
			mmfatal(OUT_OF_MEMORY, "out of memory");
	}
	literalbuf[literallen++] = (char) c;
	literalbuf[literallen] = '\0';
}

char *
pgc_lit_take(size_t *out_len)
{
	char	   *r;

	if (out_len)
		*out_len = (size_t) literallen;
	r = (char *) mm_alloc(literallen + 1);
	memcpy(r, literalbuf, literallen);
	r[literallen] = '\0';
	literallen = 0;
	if (literalbuf)
		literalbuf[0] = '\0';
	return r;
}

size_t
pgc_lit_len(void)
{
	return (size_t) literallen;
}

const char *
pgc_lit_peek(void)
{
	return literalbuf ? literalbuf : "";
}

/* File-static globals shared with .lex actions. */
static int	xcdepth_g = 0;
static char *dolqstart_g = NULL;
static int	parenths_open;
static bool include_next;
static int	state_before_str_start_g;
static int	state_before_str_stop_g;

#define MAX_NESTED_IF 128
static short preproc_tos;
static struct _if_value
{
	bool		active;
	bool		saw_active;
	bool		else_branch;
}			stacked_if_value[MAX_NESTED_IF];

static bool ifcond;

static char *newdefsymbol = NULL;

int
pgc_state_before_str_start(void)
{
	return state_before_str_start_g;
}
void
pgc_state_before_str_start_set(int s)
{
	state_before_str_start_g = s;
}
int
pgc_state_before_str_stop(void)
{
	return state_before_str_stop_g;
}
void
pgc_state_before_str_stop_set(int s)
{
	state_before_str_stop_g = s;
}
int
pgc_xcdepth(void)
{
	return xcdepth_g;
}
void
pgc_xcdepth_set(int d)
{
	xcdepth_g = d;
}
void
pgc_xcdepth_inc(void)
{
	xcdepth_g++;
}
void
pgc_xcdepth_dec(void)
{
	xcdepth_g--;
}
void
pgc_set_include_next(bool v)
{
	include_next = v;
}

void
pgc_set_dolqstart(const char *text, size_t len)
{
	free(dolqstart_g);
	dolqstart_g = (char *) malloc(len + 1);
	if (dolqstart_g == NULL)
		mmfatal(OUT_OF_MEMORY, "out of memory");
	memcpy(dolqstart_g, text, len);
	dolqstart_g[len] = '\0';
}

bool
pgc_dolq_match(const char *text, size_t len)
{
	return dolqstart_g != NULL &&
		strlen(dolqstart_g) == len &&
		memcmp(dolqstart_g, text, len) == 0;
}

void
pgc_set_def_symbol(const char *text, size_t len)
{
	free(newdefsymbol);
	newdefsymbol = (char *) mm_alloc(len + 1);
	memcpy(newdefsymbol, text, len);
	newdefsymbol[len] = '\0';
}

/* Echo + line tracking. */
void
pgc_echo(const char *text, size_t len)
{
	if (len > 0 && base_yyout != NULL)
		fwrite(text, 1, len, base_yyout);
}

void
pgc_count_newlines(const char *text, size_t len)
{
	for (size_t i = 0; i < len; i++)
		if (text[i] == '\n')
			base_yylineno++;
}

void
pgc_track_paren(int delta)
{
	parenths_open += delta;
}

void
pgc_track_function(const char *text, size_t len)
{
	if (braces_open == 0 && parenths_open == 0)
	{
		free(current_function);
		current_function = (char *) mm_alloc(len + 1);
		memcpy(current_function, text, len);
		current_function[len] = '\0';
	}
}

/* Operator length-trim helper. */
size_t
pgc_op_keep(const char *text, size_t len)
{
	const char *slashstar = NULL;
	const char *dashdash = NULL;
	size_t		nchars = len;

	for (size_t i = 0; i + 1 < len; i++)
	{
		if (slashstar == NULL && text[i] == '/' && text[i + 1] == '*')
			slashstar = text +i;

		if (dashdash == NULL && text[i] == '-' && text[i + 1] == '-')
			dashdash = text +i;
	}
	if (slashstar && dashdash)
	{
		if (slashstar > dashdash)
			slashstar = dashdash;
	}
	else if (!slashstar)
		slashstar = dashdash;
	if (slashstar)
		nchars = slashstar - text;

	if (nchars > 1 &&
		(text[nchars - 1] == '+' || text[nchars - 1] == '-'))
	{
		bool		has_special = false;

		for (int ic = (int) nchars - 2; ic >= 0; ic--)
		{
			char		c = text[ic];

			if (c == '~' || c == '!' || c == '@' || c == '#' ||
				c == '^' || c == '&' || c == '|' || c == '`' ||
				c == '?' || c == '%')
			{
				has_special = true;
				break;
			}
		}
		if (!has_special)
		{
			while (nchars > 1 &&
				   (text[nchars - 1] == '+' || text[nchars - 1] == '-'))
				nchars--;
		}
	}
	return nchars;
}

/* Buffer stack. */
typedef struct InputBuffer
{
	char	   *data;
	int			len;
	int			pos;
	int			lineno;
	char	   *filename;
	FILE	   *file;
	bool		from_string;
	bool		is_topmost;
	struct InputBuffer *next;
} InputBuffer;

static InputBuffer *cur_buffer = NULL;
static InputBuffer *buffer_stack = NULL;

static void
slurp_file_into(InputBuffer *ib, FILE *f)
{
	size_t		cap = 4096;
	size_t		used = 0;
	char	   *buf = (char *) malloc(cap);

	if (buf == NULL)
		mmfatal(OUT_OF_MEMORY, "out of memory");

	for (;;)
	{
		size_t		n;

		if (cap - used < 1024)
		{
			cap *= 2;
			buf = (char *) realloc(buf, cap);
			if (buf == NULL)
				mmfatal(OUT_OF_MEMORY, "out of memory");
		}
		n = fread(buf + used, 1, cap - used - 1, f);
		used += n;
		if (n == 0)
			break;
	}
	buf[used] = '\0';
	ib->data = buf;
	ib->len = (int) used;
}

static void
push_buffer_from_file(FILE *f, const char *filename)
{
	InputBuffer *ib;

	if (cur_buffer != NULL)
	{
		cur_buffer->lineno = base_yylineno;
		cur_buffer->next = buffer_stack;
		buffer_stack = cur_buffer;
	}

	ib = (InputBuffer *) calloc(1, sizeof(InputBuffer));
	if (ib == NULL)
		mmfatal(OUT_OF_MEMORY, "out of memory");
	slurp_file_into(ib, f);
	ib->pos = 0;
	ib->lineno = 1;
	ib->filename = filename ? mm_strdup(filename) : NULL;
	ib->file = f;
	ib->from_string = false;
	ib->is_topmost = false;

	cur_buffer = ib;
	base_yylineno = 1;
}

static void
push_buffer_from_string(const char *s)
{
	InputBuffer *ib;
	size_t		n = strlen(s);

	if (cur_buffer != NULL)
	{
		cur_buffer->lineno = base_yylineno;
		cur_buffer->next = buffer_stack;
		buffer_stack = cur_buffer;
	}

	ib = (InputBuffer *) calloc(1, sizeof(InputBuffer));
	if (ib == NULL)
		mmfatal(OUT_OF_MEMORY, "out of memory");
	ib->data = (char *) malloc(n + 1);
	if (ib->data == NULL)
		mmfatal(OUT_OF_MEMORY, "out of memory");
	memcpy(ib->data, s, n);
	ib->data[n] = '\0';
	ib->len = (int) n;
	ib->pos = 0;
	ib->lineno = base_yylineno;
	ib->filename = input_filename ? mm_strdup(input_filename) : NULL;
	ib->file = NULL;
	ib->from_string = true;
	ib->is_topmost = false;

	cur_buffer = ib;
}

static bool
pop_buffer(void)
{
	InputBuffer *gone = cur_buffer;
	InputBuffer *prev = buffer_stack;
	struct _defines *d;

	if (gone == NULL || prev == NULL)
		return false;

	for (d = defines; d != NULL; d = d->next)
	{
		if (d->used == gone)
		{
			d->used = NULL;
			break;
		}
	}

	buffer_stack = prev->next;
	prev->next = NULL;
	cur_buffer = prev;

	if (gone->file != NULL && !gone->is_topmost)
	{
		fclose(gone->file);
		gone->file = NULL;
	}

	base_yylineno = cur_buffer->lineno;

	{
		int			cmp = (input_filename && cur_buffer->filename) ?
			strcmp(input_filename, cur_buffer->filename) : 1;

		free(input_filename);
		input_filename = cur_buffer->filename ? mm_strdup(cur_buffer->filename) : NULL;

		if (gone->data)
			free(gone->data);
		if (gone->filename)
			free(gone->filename);
		free(gone);

		if (cmp != 0)
			output_line_number();
	}
	return true;
}

void
pgc_handle_pop(void *lex)
{
	(void) lex;
}

/* IFDEF stack. */
int
pgc_push_if(bool is_ifdef)
{
	if (preproc_tos >= MAX_NESTED_IF - 1)
		return -1;
	preproc_tos++;
	stacked_if_value[preproc_tos].active = false;
	stacked_if_value[preproc_tos].saw_active = false;
	stacked_if_value[preproc_tos].else_branch = false;
	ifcond = is_ifdef;
	return 0;
}

int
pgc_handle_elif(void)
{
	if (preproc_tos == 0)
		return -1;
	if (stacked_if_value[preproc_tos].else_branch)
		return 1;
	ifcond = true;
	return 0;
}

int
pgc_handle_else(void)
{
	if (preproc_tos == 0)
		return -1;
	if (stacked_if_value[preproc_tos].else_branch)
		return 1;
	stacked_if_value[preproc_tos].else_branch = true;
	stacked_if_value[preproc_tos].active =
		(stacked_if_value[preproc_tos - 1].active &&
		 !stacked_if_value[preproc_tos].saw_active);
	stacked_if_value[preproc_tos].saw_active = true;
	return 0;
}

int
pgc_handle_endif(void)
{
	if (preproc_tos == 0)
		return -1;
	preproc_tos--;
	return 0;
}

int
pgc_active_state(void)
{
	return stacked_if_value[preproc_tos].active ? PGC_STATE_C : PGC_STATE_XSKIP;
}

static bool
ecpg_isspace(char ch)
{
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' ||
		ch == '\f' || ch == '\v';
}

void
pgc_handle_ifdef_ident(const char *text, size_t len)
{
	struct _defines *defptr;
	bool		this_active;

	set_yytext(text, (int) len);
	{
		unsigned int j;

		for (j = strlen(base_yytext) - 1; j > 0 && ecpg_isspace(base_yytext[j]); j--)
			;
		if (base_yytext[j] == ';')
			j--;
		while (j > 0 && ecpg_isspace(base_yytext[j]))
			j--;
		base_yytext[j + 1] = '\0';
	}

	for (defptr = defines; defptr; defptr = defptr->next)
	{
		if (strcmp(base_yytext, defptr->name) == 0)
		{
			if (defptr->value == NULL)
				defptr = NULL;
			break;
		}
	}
	this_active = (defptr ? ifcond : !ifcond);
	stacked_if_value[preproc_tos].active =
		(stacked_if_value[preproc_tos - 1].active &&
		 !stacked_if_value[preproc_tos].saw_active &&
		 this_active);
	stacked_if_value[preproc_tos].saw_active |= this_active;
}

void
pgc_commit_def(void)
{
	struct _defines *ptr;

	for (ptr = defines; ptr != NULL; ptr = ptr->next)
	{
		if (strcmp(newdefsymbol, ptr->name) == 0)
		{
			free(ptr->value);
			ptr->value = mm_strdup(literalbuf ? literalbuf : "");
			free(newdefsymbol);
			newdefsymbol = NULL;
			break;
		}
	}
	if (ptr == NULL)
	{
		ptr = (struct _defines *) mm_alloc(sizeof(struct _defines));
		ptr->name = newdefsymbol;
		ptr->value = mm_strdup(literalbuf ? literalbuf : "");
		ptr->cmdvalue = NULL;
		ptr->used = NULL;
		ptr->next = defines;
		defines = ptr;
		newdefsymbol = NULL;
	}
	pgc_lit_start();
}

void
pgc_handle_undef(const char *text, size_t len)
{
	struct _defines *ptr,
			   *ptr2 = NULL;

	set_yytext(text, (int) len);
	{
		unsigned int j;

		for (j = strlen(base_yytext) - 1; j > 0 && ecpg_isspace(base_yytext[j]); j--)
			;
		if (base_yytext[j] == ';')
			j--;
		while (j > 0 && ecpg_isspace(base_yytext[j]))
			j--;
		base_yytext[j + 1] = '\0';
	}

	for (ptr = defines; ptr; ptr2 = ptr, ptr = ptr->next)
	{
		if (strcmp(base_yytext, ptr->name) == 0)
		{
			free(ptr->value);
			ptr->value = NULL;
			if (ptr->cmdvalue == NULL)
			{
				if (ptr2 == NULL)
					defines = ptr->next;
				else
					ptr2->next = ptr->next;
				free(ptr->name);
				free(ptr);
			}
			break;
		}
	}
}

/* Define-expansion + INFORMIX type alias. */
static bool
try_define_expand(const char *text, size_t len)
{
	struct _defines *ptr;
	char		buf[NAMEDATALEN];

	if (len >= NAMEDATALEN)
		return false;
	memcpy(buf, text, len);
	buf[len] = '\0';

	for (ptr = defines; ptr; ptr = ptr->next)
	{
		if (strcmp(buf, ptr->name) == 0 &&
			ptr->value != NULL &&
			ptr->used == NULL)
		{
			push_buffer_from_string(ptr->value);
			ptr->used = cur_buffer;
			return true;
		}
	}
	return false;
}

static bool
try_informix_define(const char *text, size_t len)
{
	const char *new = NULL;
	char		buf[NAMEDATALEN];

	if (len >= NAMEDATALEN)
		return false;
	memcpy(buf, text, len);
	buf[len] = '\0';

	if (strcmp(buf, "dec_t") == 0)
		new = "decimal";
	else if (strcmp(buf, "intrvl_t") == 0)
		new = "interval";
	else if (strcmp(buf, "dtime_t") == 0)
		new = "timestamp";

	if (new)
	{
		push_buffer_from_string(new);
		return true;
	}
	return false;
}

/* Per-call ephemeral state. */
static const char *cur_feed_start = NULL;
static InputBuffer *cur_feed_buffer = NULL;
static size_t cur_feed_consumed = 0;
static int	cur_feed_token_code = 0;
static const char *cur_feed_token_text = NULL;
static size_t cur_feed_token_len = 0;
static bool cur_feed_have_token = false;
static bool cur_feed_have_error = false;
static char cur_feed_err_msg[256];

void
pgc_terminate(void *user, const char *match_end_ptr, size_t match_consumed)
{
	(void) user;
	if (match_end_ptr != NULL && cur_feed_start != NULL &&
		match_end_ptr >= cur_feed_start)
	{
		size_t		off = (size_t) (match_end_ptr - cur_feed_start);
		size_t		consumed = off + match_consumed;

		if (consumed > cur_feed_consumed)
			cur_feed_consumed = consumed;
	}
}

void
pgc_emit_error(const char *msg)
{
	if (!cur_feed_have_error)
	{
		cur_feed_have_error = true;
		snprintf(cur_feed_err_msg, sizeof(cur_feed_err_msg), "%s", msg);
	}
}

/* Forward decl for emit dispatch. */
static void scan_emit_cb_dispatch(void *user, int rule, const char *text, size_t len);

bool
pgc_handle_c_ident(const char *text, size_t len, void *user, void *lex)
{
	char		buf[NAMEDATALEN];
	int			n = (int) (len < NAMEDATALEN - 1 ? len : NAMEDATALEN - 1);
	int			kw;

	memcpy(buf, text, n);
	buf[n] = '\0';

	if (INFORMIX_MODE && try_informix_define(text, len))
		return false;
	if (try_define_expand(text, len))
		return false;

	kw = ScanCKeywordLookup(buf);
	if (kw >= 0)
	{
		scan_emit_cb_dispatch(user, kw, text, len);
		(void) lex;
		return true;
	}
	scan_emit_cb_dispatch(user, PGC_TOK_IDENT, text, len);
	return true;
}

bool
pgc_handle_sql_ident(const char *text, size_t len, void *user, void *lex)
{
	char		buf[NAMEDATALEN];
	int			n = (int) (len < NAMEDATALEN - 1 ? len : NAMEDATALEN - 1);
	int			kw;

	memcpy(buf, text, n);
	buf[n] = '\0';

	if (try_define_expand(text, len))
		return false;
	if (get_typedef(buf, true) == NULL)
	{
		kw = ScanECPGKeywordLookup(buf);
		if (kw >= 0)
		{
			scan_emit_cb_dispatch(user, kw, text, len);
			(void) lex;
			return true;
		}
	}
	kw = ScanCKeywordLookup(buf);
	if (kw >= 0)
	{
		scan_emit_cb_dispatch(user, kw, text, len);
		return true;
	}
	scan_emit_cb_dispatch(user, PGC_TOK_IDENT, text, len);
	return true;
}

size_t
pgc_consume_cvariable_tail(const char *text, size_t len)
{
	(void) text;

	return len;
}

/* String-token emit helpers. */
static void
do_emit_buffered_string(void *user, int sentinel)
{
	size_t		n;
	char	   *s = pgc_lit_take(&n);

	scan_emit_cb_dispatch(user, sentinel, s, n);
	free(s);
}

void
pgc_emit_string_token_for(void *user, void *lex)
{
	(void) lex;
	switch (state_before_str_stop_g)
	{
		case PGC_STATE_XB:
			do_emit_buffered_string(user, PGC_TOK_BCONST);
			break;
		case PGC_STATE_XH:
			do_emit_buffered_string(user, PGC_TOK_XCONST);
			break;
		case PGC_STATE_XQ:
		case PGC_STATE_XQC:
		case PGC_STATE_XE:
		case PGC_STATE_XN:
			do_emit_buffered_string(user, PGC_TOK_SCONST);
			break;
		case PGC_STATE_XUS:
			do_emit_buffered_string(user, PGC_TOK_USCONST);
			break;
		default:
			do_emit_buffered_string(user, PGC_TOK_SCONST);
			break;
	}
}

void
pgc_emit_xdolq(void *user)
{
	size_t		n;
	char	   *s = pgc_lit_take(&n);

	scan_emit_cb_dispatch(user, PGC_TOK_SCONST, s, n);
	free(s);
	free(dolqstart_g);
	dolqstart_g = NULL;
}

void
pgc_emit_xd_close(void *user, int prev_state)
{
	size_t		n;
	char	   *s;

	if (literallen == 0)
		pgc_emit_error("zero-length delimited identifier");
	s = pgc_lit_take(&n);
	if (prev_state == PGC_STATE_XD)
		scan_emit_cb_dispatch(user, PGC_TOK_CSTRING, s, n);
	else
		scan_emit_cb_dispatch(user, PGC_TOK_UIDENT, s, n);
	free(s);
}

void
pgc_emit_xdc(void *user)
{
	size_t		n;
	char	   *s = pgc_lit_take(&n);

	scan_emit_cb_dispatch(user, PGC_TOK_CSTRING, s, n);
	free(s);
}

/* parse_include (legacy logic). */
void
pgc_do_include(const char *text, size_t len, void *lex)
{
	struct _include_path *ip;
	char		inc_file[MAXPGPATH];
	unsigned int i;
	FILE	   *f = NULL;

	(void) lex;
	set_yytext(text, (int) len);

	/*
	 * Strip trailing whitespace and an optional terminating ';'. The match
	 * may or may not include a ';' (for cpp-style `#include <foo.h>` it does
	 * NOT, for `EXEC SQL INCLUDE foo;` it does); guard the i-- with bounds so
	 * we don't eat the closing `>` or `"` when no ';' is present.
	 */
	i = strlen(base_yytext);
	if (i == 0)
	{
		mmfatal(PARSE_ERROR,
				"empty filename in EXEC SQL INCLUDE");
		return;
	}
	i--;
	while (i > 0 && ecpg_isspace(base_yytext[i]))
		i--;
	if (base_yytext[i] == ';' && i > 0)
	{
		i--;
		while (i > 0 && ecpg_isspace(base_yytext[i]))
			i--;
	}

	base_yytext[i + 1] = '\0';

	if (base_yytext[0] == '"' && base_yytext[i] == '"' &&
		((compat != ECPG_COMPAT_INFORMIX && compat != ECPG_COMPAT_INFORMIX_SE) ||
		 base_yytext[1] == '/'))
	{
		base_yytext[i] = '\0';
		memmove(base_yytext, base_yytext + 1, strlen(base_yytext));
		strlcpy(inc_file, base_yytext, sizeof(inc_file));
		f = fopen(inc_file, "r");
		if (!f)
		{
			if (strlen(inc_file) <= 2 ||
				strcmp(inc_file + strlen(inc_file) - 2, ".h") != 0)
			{
				strcat(inc_file, ".h");
				f = fopen(inc_file, "r");
			}
		}
	}
	else
	{
		if ((base_yytext[0] == '"' && base_yytext[i] == '"') ||
			(base_yytext[0] == '<' && base_yytext[i] == '>'))
		{
			base_yytext[i] = '\0';
			memmove(base_yytext, base_yytext + 1, strlen(base_yytext));
		}

		for (ip = include_paths; f == NULL && ip != NULL; ip = ip->next)
		{
			if (strlen(ip->path) + strlen(base_yytext) + 4 > MAXPGPATH)
			{
				fprintf(stderr,
						_("Error: include path \"%s/%s\" is too long on line %d, skipping\n"),
						ip->path, base_yytext, base_yylineno);
				continue;
			}
			snprintf(inc_file, sizeof(inc_file), "%s/%s", ip->path, base_yytext);
			f = fopen(inc_file, "r");
			if (!f)
			{
				if (strcmp(inc_file + strlen(inc_file) - 2, ".h") != 0)
				{
					strcat(inc_file, ".h");
					f = fopen(inc_file, "r");
				}
			}
			if (f && include_next)
			{
				fclose(f);
				f = NULL;
				include_next = false;
			}
		}
	}
	if (!f)
		mmfatal(NO_INCLUDE_FILE,
				"could not open include file \"%s\" on line %d",
				base_yytext, base_yylineno);

	push_buffer_from_file(f, inc_file);
	free(input_filename);
	input_filename = mm_strdup(inc_file);
	output_line_number();
}

void
pgc_handle_top_eof(void)
{
	if (preproc_tos > 0)
	{
		preproc_tos = 0;
		mmfatal(PARSE_ERROR, "missing \"EXEC SQL ENDIF;\"");
	}
}

/* Sentinel-to-parser-token translation. */
static int
process_integer_literal(const char *token, YYSTYPE *lval, int base)
{
	int			val;
	char	   *endptr;
	const char *s = base == 10 ? token : token + 2;

	errno = 0;
	val = strtoint(s, &endptr, base);
	if (*endptr != '\0' || errno == ERANGE)
	{
		lval->str = loc_strdup(token);
		return FCONST;
	}
	lval->ival = val;
	return ICONST;
}

static void
scan_emit_cb_dispatch(void *user, int rule, const char *text, size_t len)
{
	int			out_code = rule;
	YYSTYPE		val;
	char		buf[NAMEDATALEN];
	int			n;

	(void) user;
	memset(&val, 0, sizeof(val));

	switch (rule)
	{
		case PGC_TOK_RAW_CHAR:
			out_code = (unsigned char) text[0];

			break;

		case PGC_TOK_RAW_CHAR_COLON:
			out_code = ':';
			text = ":";

			len = 1;
			break;

		case PGC_TOK_TYPECAST:
			out_code = TYPECAST;
			break;
		case PGC_TOK_DOT_DOT:
			out_code = DOT_DOT;
			break;
		case PGC_TOK_COLON_EQUALS:
			out_code = COLON_EQUALS;
			break;

		case PGC_TOK_OP:
			if (len == 1 && strchr(",()[].;:|+-*/%^<>=", text[0]))
			{
				out_code = (unsigned char) text[0];
			}
			else if (len == 2)
			{
				if (text[0] == '=' && text[1] == '>')
					out_code = EQUALS_GREATER;
				else if (text[0] == '>' && text[1] == '=')
					out_code = GREATER_EQUALS;
				else if (text[0] == '<' && text[1] == '=')
					out_code = LESS_EQUALS;
				else if (text[0] == '<' && text[1] == '>')
					out_code = NOT_EQUALS;
				else if (text[0] == '!' && text[1] == '=')
					out_code = NOT_EQUALS;
				else if (text[0] == '-' && text[1] == '>')
					out_code = RIGHT_ARROW;
				else
				{
					char	   *s = (char *) mm_alloc(len + 1);

					memcpy(s, text, len);
					s[len] = '\0';
					val.str = loc_strdup(s);
					out_code = OP;
				}
			}
			else
			{
				char	   *s = (char *) mm_alloc(len + 1);

				memcpy(s, text, len);
				s[len] = '\0';
				val.str = loc_strdup(s);
				out_code = OP;
			}
			break;

		case PGC_TOK_PARAM:
			{
				char		nbuf[64];
				size_t		nl = len < sizeof(nbuf) ? len : sizeof(nbuf) - 1;

				memcpy(nbuf, text +1, nl - 1);
				nbuf[nl - 1] = '\0';
				errno = 0;
				val.ival = strtoint(nbuf, NULL, 10);
				if (errno == ERANGE)
				{
					pgc_emit_error("parameter number too large");
					return;
				}
				out_code = PARAM;
			}
			break;

		case PGC_TOK_ICONST_DEC:
		case PGC_TOK_ICONST_HEX:
		case PGC_TOK_ICONST_OCT:
		case PGC_TOK_ICONST_BIN:
			{
				char	   *s = (char *) mm_alloc(len + 1);
				int			b = (rule == PGC_TOK_ICONST_DEC) ? 10 :
					(rule == PGC_TOK_ICONST_HEX) ? 16 :
					(rule == PGC_TOK_ICONST_OCT) ? 8 : 2;

				memcpy(s, text, len);
				s[len] = '\0';
				out_code = process_integer_literal(s, &val, b);
				free(s);
			}
			break;

		case PGC_TOK_FCONST_NUMERIC:
		case PGC_TOK_FCONST_REAL:
			{
				char	   *s = (char *) mm_alloc(len + 1);

				memcpy(s, text, len);
				s[len] = '\0';
				val.str = loc_strdup(s);
				free(s);
				out_code = FCONST;
			}
			break;

		case PGC_TOK_IDENT:
			n = (int) (len < NAMEDATALEN - 1 ? len : NAMEDATALEN - 1);
			memcpy(buf, text, n);
			buf[n] = '\0';
			val.str = loc_strdup(buf);
			out_code = IDENT;
			break;

		case PGC_TOK_CSTRING:
			val.str = loc_strdup(text);
			out_code = CSTRING;
			break;
		case PGC_TOK_UIDENT:
			val.str = make3_str("U&\"", text, "\"");
			out_code = UIDENT;
			break;
		case PGC_TOK_BCONST:
			val.str = make3_str("b'", text, "'");
			out_code = BCONST;
			break;
		case PGC_TOK_XCONST:
			val.str = make3_str("x'", text, "'");
			out_code = XCONST;
			break;
		case PGC_TOK_SCONST:
			{
				int			s = state_before_str_stop_g;

				if (s == PGC_STATE_XE)
					val.str = make3_str("E'", text, "'");
				else if (s == PGC_STATE_XN)
					val.str = make3_str("N'", text, "'");
				else if (s == PGC_STATE_XDOLQ)
					val.str = loc_strdup(text);
				else
					val.str = make3_str("'", text, "'");
				out_code = SCONST;
			}
			break;
		case PGC_TOK_USCONST:
			val.str = make3_str("U&'", text, "'");
			out_code = USCONST;
			break;
		case PGC_TOK_IP:
			{
				char	   *s = (char *) mm_alloc(len + 1);

				memcpy(s, text, len);
				s[len] = '\0';
				val.str = loc_strdup(s);
				free(s);
			}
			out_code = IP;
			break;
		case PGC_TOK_CVARIABLE:
			{
				char	   *s = (char *) mm_alloc(len);

				memcpy(s, text +1, len - 1);
				s[len - 1] = '\0';
				val.str = loc_strdup(s);
				free(s);
			}
			out_code = CVARIABLE;
			break;
		case PGC_TOK_CPP_LINE:
			{
				char	   *s = (char *) mm_alloc(len + 1);

				memcpy(s, text, len);
				s[len] = '\0';
				val.str = loc_strdup(s);
				free(s);
			}
			out_code = CPP_LINE;
			break;
		case PGC_TOK_SQL_START:
			out_code = SQL_START;
			break;
		case PGC_TOK_S_ANYTHING:
			out_code = S_ANYTHING;
			break;
		case PGC_TOK_S_MEMBER:
			out_code = S_MEMBER;
			break;
		case PGC_TOK_S_MEMPOINT:
			out_code = S_MEMPOINT;
			break;
		case PGC_TOK_S_DOTPOINT:
			out_code = S_DOTPOINT;
			break;
		case PGC_TOK_S_RSHIFT:
			out_code = S_RSHIFT;
			break;
		case PGC_TOK_S_LSHIFT:
			out_code = S_LSHIFT;
			break;
		case PGC_TOK_S_OR:
			out_code = S_OR;
			break;
		case PGC_TOK_S_AND:
			out_code = S_AND;
			break;
		case PGC_TOK_S_INC:
			out_code = S_INC;
			break;
		case PGC_TOK_S_DEC:
			out_code = S_DEC;
			break;
		case PGC_TOK_S_EQUAL:
			out_code = S_EQUAL;
			break;
		case PGC_TOK_S_NEQUAL:
			out_code = S_NEQUAL;
			break;
		case PGC_TOK_S_ADD:
			out_code = S_ADD;
			break;
		case PGC_TOK_S_SUB:
			out_code = S_SUB;
			break;
		case PGC_TOK_S_MUL:
			out_code = S_MUL;
			break;
		case PGC_TOK_S_DIV:
			out_code = S_DIV;
			break;
		case PGC_TOK_S_MOD:
			out_code = S_MOD;
			break;

		default:
			/* Already a parser-side token (keyword lookup). */
			break;
	}

	if (cur_feed_have_token)
		return;
	cur_feed_have_token = true;
	cur_feed_token_code = out_code;
	cur_feed_token_text = text;

	cur_feed_token_len = len;
	base_yylval = val;
}

/* Lime emit callback wired in PgcLexFeedBytes. */
static void
scan_emit_cb_local(void *user, int rule, const char *text, size_t len)
{
	scan_emit_cb_dispatch(user, rule, text, len);
}

/* Lime lexer instance. */
static PgcLexer * pgc_lexer = NULL;

static void *
pgc_malloc_wrapper(size_t n)
{
	return malloc(n);
}

static void
pgc_free_wrapper(void *p)
{
	if (p)
		free(p);
}

void
lex_init(void)
{
	InputBuffer *ib;

	braces_open = 0;
	parenths_open = 0;
	current_function = NULL;
	include_next = false;

	base_yylineno = 1;

	preproc_tos = 0;
	stacked_if_value[preproc_tos].active = true;
	stacked_if_value[preproc_tos].saw_active = true;
	stacked_if_value[preproc_tos].else_branch = false;

	pgc_lit_start();

	while (cur_buffer != NULL)
	{
		InputBuffer *gone = cur_buffer;

		cur_buffer = buffer_stack;
		if (buffer_stack)
			buffer_stack = buffer_stack->next;

		if (gone->file != NULL && !gone->is_topmost)
			fclose(gone->file);
		if (gone->data)
			free(gone->data);
		if (gone->filename)
			free(gone->filename);
		free(gone);
	}
	while (buffer_stack != NULL)
	{
		InputBuffer *gone = buffer_stack;

		buffer_stack = gone->next;
		if (gone->file != NULL && !gone->is_topmost)
			fclose(gone->file);
		if (gone->data)
			free(gone->data);
		if (gone->filename)
			free(gone->filename);
		free(gone);
	}

	{
		struct _defines *d;

		for (d = defines; d != NULL; d = d->next)
			d->used = NULL;
	}

	ib = (InputBuffer *) calloc(1, sizeof(InputBuffer));
	if (ib == NULL)
		mmfatal(OUT_OF_MEMORY, "out of memory");
	if (base_yyin != NULL)
		slurp_file_into(ib, base_yyin);
	else
	{
		ib->data = (char *) malloc(1);
		if (ib->data == NULL)
			mmfatal(OUT_OF_MEMORY, "out of memory");
		ib->data[0] = '\0';
		ib->len = 0;
	}
	ib->pos = 0;
	ib->lineno = 1;
	ib->filename = input_filename ? mm_strdup(input_filename) : NULL;
	ib->file = base_yyin;
	ib->is_topmost = true;
	ib->from_string = false;
	ib->next = NULL;

	cur_buffer = ib;

	if (pgc_lexer != NULL)
	{
		PgcLexFree(pgc_lexer, pgc_free_wrapper);
		pgc_lexer = NULL;
	}
	pgc_lexer = PgcLexAlloc(pgc_malloc_wrapper);
	if (pgc_lexer == NULL)
		mmfatal(OUT_OF_MEMORY, "out of memory");
	PgcLexSetState(pgc_lexer, PGC_STATE_C);

	set_yytext("", 0);
	token_start = NULL;
}

int
base_yylex(void)
{
	token_start = NULL;

	for (;;)
	{
		const char *p;
		int			avail;
		PgcLexResult res;

		if (cur_buffer == NULL)
			return 0;

		avail = cur_buffer->len - cur_buffer->pos;
		if (avail <= 0)
		{
			if (buffer_stack == NULL)
			{
				pgc_handle_top_eof();
				return 0;
			}
			pop_buffer();
			continue;
		}

		p = cur_buffer->data + cur_buffer->pos;

		cur_feed_start = p;
		cur_feed_buffer = cur_buffer;
		cur_feed_consumed = 0;
		cur_feed_have_token = false;
		cur_feed_have_error = false;
		cur_feed_token_code = 0;
		cur_feed_token_text = NULL;
		cur_feed_token_len = 0;

		res = PgcLexFeedBytes(pgc_lexer, p, (size_t) avail,
							  scan_emit_cb_local, NULL);

		if (cur_feed_have_error)
		{
			mmfatal(PARSE_ERROR, "%s", cur_feed_err_msg);
			return 0;
		}

		if (cur_feed_consumed == 0 && !cur_feed_have_token)
		{
			cur_feed_buffer->pos = cur_feed_buffer->len;
			continue;
		}
		if (cur_feed_consumed > 0)
		{
			int			new_pos = cur_feed_buffer->pos + (int) cur_feed_consumed;

			if (new_pos > cur_feed_buffer->len)
				new_pos = cur_feed_buffer->len;
			cur_feed_buffer->pos = new_pos;
		}

		if (res == PGC_LEX_ERROR)
		{
			const char *msg = PgcLexErrorMessage(pgc_lexer);

			mmfatal(PARSE_ERROR, "%s", msg ? msg : "lexer error");
			return 0;
		}

		if (cur_feed_have_token)
		{
			set_yytext(cur_feed_token_text, (int) cur_feed_token_len);
			return cur_feed_token_code;
		}

		/* No token yet; loop. */
	}
}
