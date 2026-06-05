/*-------------------------------------------------------------------------
 *
 * psqlscan.c
 *	  Driver shim around the Lime-generated SQL-side lexer (psqlscan.lex).
 *
 * The state machine moved to psqlscan.lex (Lime v0.2.2's lexer
 * subsystem).  This file holds:
 *
 *	- The public API (psql_scan_create / psql_scan_setup / psql_scan
 *	  / psql_scan_finish / psql_scan_destroy / psql_scan_get_location
 *	  / psql_scan_in_quote / psql_scan_reset /
 *	  psql_scan_reselect_sql_lexer / psql_scan_set_passthrough)
 *	  declared in include/fe_utils/psqlscan.h.  psql, pg_dump,
 *	  pgbench, etc. consume it unchanged.
 *
 *	- The Strategy-D streaming driver: each psql_scan() call allocates
 *	  a fresh Psql_Lexer over the remaining bytes of the active buffer
 *	  (top of buffer_stack or scanbuf), sets its state to
 *	  state->start_state, and runs PsqlLexFeedBytes.  Action bodies do
 *	  the work directly through the PsqlEmitCtx cookie threaded as the
 *	  user pointer; stop points (semicolon at depth 0, backslash,
 *	  variable expansion) call LEX_TERMINATE() with the consumed-byte
 *	  count recorded in ctx.consumed.  When all bytes are consumed
 *	  without termination, the driver pops the buffer stack or returns
 *	  PSCAN_EOL/PSCAN_INCOMPLETE with the appropriate prompt.
 *
 *	- The variable-substitution helpers (psql_emit_var_*) called from
 *	  the .lex action bodies for the four :varname syntaxes.
 *
 *	- The buffer-stack management primitives shared with
 *	  psqlscanslash.c and pgbench's exprscan.c.
 *
 * The buffer_stack persists across psql_scan() calls; a `;` at depth
 * zero inside an expanded variable terminates the current call and
 * leaves the stack in place so the next call resumes at the correct
 * cursor inside the variable's value (matching psqlscan.l semantics).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/fe_utils/psqlscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "common/logging.h"
#include "fe_utils/psqlscan.h"
#include "fe_utils/psqlscan_emit.h"
#include "fe_utils/psqlscan_int.h"
#include "libpq-fe.h"
#include "psqlscan_lex.h"		/* PsqlLexer, PsqlLexAlloc, ... */


/* ------------------------------------------------------------------------- */
/* Allocator wrappers so Lime's malloc/free-shaped parameters route to       */
/* pg_malloc/free (which abort on OOM in frontend code).                     */
/* ------------------------------------------------------------------------- */

static void *
psql_lex_malloc(size_t n)
{
	return pg_malloc(n);
}

static void
psql_lex_free(void *p)
{
	if (p != NULL)
		free(p);
}

/* ------------------------------------------------------------------------- */
/* Cursor primitives shared with psqlscanslash.c and exprscan.c.             */
/* ------------------------------------------------------------------------- */

static inline const char *
cur_buf(PsqlScanState state)
{
	if (state->buffer_stack != NULL)
		return state->buffer_stack->bufstring;
	return state->scanbuf;
}

static inline int
cur_buf_len(PsqlScanState state)
{
	if (state->buffer_stack != NULL)
		return state->buffer_stack->buflen;
	return state->scanbuflen;
}

static inline int
cur_pos(PsqlScanState state)
{
	if (state->buffer_stack != NULL)
		return state->buffer_stack->pos;
	return state->scanbufpos;
}

static inline void
advance_cur_pos(PsqlScanState state, int n)
{
	if (state->buffer_stack != NULL)
		state->buffer_stack->pos += n;
	else
		state->scanbufpos += n;
}

/* ------------------------------------------------------------------------- */
/* Forward decls.                                                            */
/* ------------------------------------------------------------------------- */

static PsqlScanResult psql_classify_eol(PsqlScanState state,
										promptStatus_t *prompt);

/* ------------------------------------------------------------------------- */
/* Variable-substitution helpers called from psqlscan.lex action bodies.    */
/* ------------------------------------------------------------------------- */

bool
psql_emit_var_plain(void *user, const char *p, size_t len)
{
	PsqlEmitCtx *ctx = PSQL_CTX(user);
	PsqlScanState state = ctx->state;
	char	   *varname;
	char	   *value;

	varname = psqlscan_extract_substring(state, p + 1, (int) len - 1);
	if (state->callbacks->get_variable)
		value = state->callbacks->get_variable(varname, PQUOTE_PLAIN,
											   state->cb_passthrough);
	else
		value = NULL;

	if (value)
	{
		if (psqlscan_var_is_current_source(state, varname))
		{
			/* Recursion: emit a warning, echo the raw text, continue. */
			pg_log_warning("skipping recursive expansion of variable \"%s\"",
						   varname);
			free(value);
			free(varname);
			psqlscan_emit(state, p, (int) len);
			return false;
		}

		/* Set up the buffer push: driver does it after LEX_TERMINATE. */
		ctx->stop_kind = STOP_VAR_EXPAND;
		ctx->var_name = varname;
		ctx->var_value = value;

		/*
		 * We record only the offset of `matched`; the .lex action body adds
		 * matched_len to ctx->consumed before LEX_TERMINATE.
		 */
		ctx->consumed = (size_t) (p - ctx->buf);
		return true;
	}

	/* Variable undefined: emit raw text and continue. */
	free(varname);
	psqlscan_emit(state, p, (int) len);
	return false;
}

void
psql_emit_var_squote(void *user, const char *p, size_t len)
{
	psqlscan_escape_variable(PSQL_STATE(user), p, (int) len, PQUOTE_SQL_LITERAL);
}

void
psql_emit_var_dquote(void *user, const char *p, size_t len)
{
	psqlscan_escape_variable(PSQL_STATE(user), p, (int) len, PQUOTE_SQL_IDENT);
}

void
psql_emit_var_test(void *user, const char *p, size_t len)
{
	psqlscan_test_variable(PSQL_STATE(user), p, (int) len);
}

/* ------------------------------------------------------------------------- */
/* The Lime emit callback.  Strategy D leaves all the interesting work to    */
/* action bodies; the callback only fires for explicit LEX_EMIT, and our    */
/* psqlscan.lex doesn't use LEX_EMIT.  Provide a stub for the API.          */
/* ------------------------------------------------------------------------- */

static void
psql_lex_emit(void *user, int rule, const char *text, size_t len)
{
	(void) user;
	(void) rule;
	(void) text;

	(void) len;

	/*
	 * No-op; psqlscan.lex performs all emission via psqlscan_emit() in action
	 * bodies.
	 */
}

/* ------------------------------------------------------------------------- */
/* The streaming scan loop (Strategy D).                                     */
/* ------------------------------------------------------------------------- */

static int						/* 0 = EOL, 1 = SEMI, 2 = BACKSLASH */
psql_scan_run(PsqlScanState state)
{
	for (;;)
	{
		PsqlLexer  *lex;
		PsqlEmitCtx ctx;
		const char *buf;
		int			pos;
		int			len;
		int			feed_len;
		PsqlLexResult r;

		buf = cur_buf(state);
		pos = cur_pos(state);
		len = cur_buf_len(state);

		if (pos >= len)
		{
			/* End of current buffer */
			if (state->buffer_stack == NULL)
				return 0;		/* outer EOL */
			psqlscan_pop_buffer_stack(state);
			psqlscan_select_top_buffer(state);
			continue;
		}

		feed_len = len - pos;

		memset(&ctx, 0, sizeof(ctx));
		ctx.state = state;
		ctx.buf = buf + pos;
		ctx.buflen = (size_t) feed_len;
		ctx.stop_kind = STOP_NONE;

		lex = PsqlLexAlloc(psql_lex_malloc);
		PsqlLexSetState(lex, (int) state->start_state);

		r = PsqlLexFeedBytes(lex, buf + pos, (size_t) feed_len,
							 psql_lex_emit, &ctx);

		state->start_state = (ScanState) PsqlLexCurrentState(lex);
		PsqlLexFree(lex, psql_lex_free);

		if (r == PSQL_LEX_ERROR)
		{
			/*
			 * No psqlscan.lex action body raises LEX_ERROR_AT, so this
			 * indicates an unrecognised byte at the current cursor that no
			 * rule matched.  Treat it as the legacy "{other}: copy one byte"
			 * behaviour by emitting the byte, advancing, and looping.
			 */
			psqlscan_emit(state, buf + pos, 1);
			advance_cur_pos(state, 1);
			continue;
		}

		if (ctx.stop_kind == STOP_NONE)
		{
			/* Lexer consumed every byte fed and didn't hit a stop point. */
			advance_cur_pos(state, feed_len);
			continue;
		}

		/*
		 * Action body called LEX_TERMINATE; advance by exactly the recorded
		 * consumed-count.
		 */
		advance_cur_pos(state, (int) ctx.consumed);

		switch (ctx.stop_kind)
		{
			case STOP_SEMI:
				return 1;
			case STOP_BACKSLASH:
				return 2;
			case STOP_VAR_EXPAND:

				/*
				 * Push the variable's value onto the buffer stack and
				 * continue scanning (now over the value's bytes).
				 */
				psqlscan_push_new_buffer(state, ctx.var_value, ctx.var_name);
				free(ctx.var_value);
				free(ctx.var_name);
				ctx.var_value = NULL;
				ctx.var_name = NULL;
				continue;
			case STOP_VAR_RECURSE:
			case STOP_SLASH_OK:
			case STOP_NONE:
				/* Not used by SQL scanner. */
				continue;
		}
	}
}

/* ------------------------------------------------------------------------- */
/* Public API.                                                               */
/* ------------------------------------------------------------------------- */

PsqlScanState
psql_scan_create(const PsqlScanCallbacks *callbacks)
{
	PsqlScanState state;

	state = pg_malloc0_object(PsqlScanStateData);
	state->callbacks = callbacks;
	psql_scan_reset(state);
	return state;
}

void
psql_scan_destroy(PsqlScanState state)
{
	psql_scan_finish(state);
	psql_scan_reset(state);
	free(state);
}

void
psql_scan_set_passthrough(PsqlScanState state, void *passthrough)
{
	state->cb_passthrough = passthrough;
}

void
psql_scan_setup(PsqlScanState state,
				const char *line, int line_len,
				int encoding, bool std_strings)
{
	Assert(state->scanbuf == NULL);
	Assert(state->buffer_stack == NULL);

	state->encoding = encoding;
	state->safe_encoding = pg_valid_server_encoding_id(encoding);
	state->std_strings = std_strings;

	state->scanbuf = NULL;
	(void) psqlscan_prepare_buffer(state, line, line_len, &state->scanbuf);
	state->scanbuflen = line_len;
	state->scanbufpos = 0;
	state->scanline = line;

	state->curline = state->scanbuf;
	state->refline = state->scanline;

	state->cur_line_no = 0;
	state->cur_line_ptr = state->scanbuf;
}

PsqlScanResult
psql_scan(PsqlScanState state,
		  PQExpBuffer query_buf,
		  promptStatus_t *prompt)
{
	int			lexresult;

	Assert(state->scanbuf != NULL);

	state->output_buf = query_buf;

	lexresult = psql_scan_run(state);

	if (state->cur_line_no == 0)
		state->cur_line_no = 1;

	switch (lexresult)
	{
		case 0:
			return psql_classify_eol(state, prompt);
		case 1:
			*prompt = PROMPT_READY;
			return PSCAN_SEMICOLON;
		case 2:
			*prompt = PROMPT_READY;
			return PSCAN_BACKSLASH;
		default:
			fprintf(stderr, "invalid scan result\n");
			exit(1);
	}
}

static PsqlScanResult
psql_classify_eol(PsqlScanState state, promptStatus_t *prompt)
{
	PQExpBuffer query_buf = state->output_buf;

	switch (state->start_state)
	{
		case ST_INITIAL:
		case ST_XQS:
			if (state->paren_depth > 0)
			{
				*prompt = PROMPT_PAREN;
				return PSCAN_INCOMPLETE;
			}
			if (state->begin_depth > 0)
			{
				*prompt = PROMPT_CONTINUE;
				return PSCAN_INCOMPLETE;
			}
			if (query_buf->len > 0)
			{
				*prompt = PROMPT_CONTINUE;
				return PSCAN_EOL;
			}
			*prompt = PROMPT_READY;
			return PSCAN_INCOMPLETE;
		case ST_XB:
		case ST_XH:
		case ST_XE:
		case ST_XQ:
		case ST_XUS:
			*prompt = PROMPT_SINGLEQUOTE;
			return PSCAN_INCOMPLETE;
		case ST_XC:
			*prompt = PROMPT_COMMENT;
			return PSCAN_INCOMPLETE;
		case ST_XD:
		case ST_XUI:
			*prompt = PROMPT_DOUBLEQUOTE;
			return PSCAN_INCOMPLETE;
		case ST_XDOLQ:
			*prompt = PROMPT_DOLLARQUOTE;
			return PSCAN_INCOMPLETE;
		default:
			fprintf(stderr, "invalid scan state\n");
			exit(1);
	}
}

void
psql_scan_finish(PsqlScanState state)
{
	while (state->buffer_stack != NULL)
		psqlscan_pop_buffer_stack(state);

	if (state->scanbuf)
		free(state->scanbuf);
	state->scanbuf = NULL;
	state->scanbuflen = 0;
	state->scanbufpos = 0;
}

/*
 * Length-aware, case-insensitive keyword compare.  The Lime scanner hands
 * us `matched`/`len`, which is NOT NUL-terminated, so we cannot use the
 * flex-era pg_strcasecmp(yytext, ...) directly.  Returns true when the
 * `len`-byte lexeme equals `kw` (kw is a NUL-terminated literal).
 */
static bool
psqlscan_ident_is(const char *ident, int len, const char *kw)
{
	return (int) strlen(kw) == len &&
		pg_strncasecmp(ident, kw, len) == 0;
}

/*
 * Record the first few keywords/identifiers of a statement or CREATE SCHEMA
 * sub-statement in the idents[] array, of length idents_size.
 * *idents_count is the number of entries filled so far.
 *
 * We record the interesting keywords using their first character, which
 * works so long as those are all different.  Ported from psqlscan.l
 * (psqlscan_record_initial_keyword), adapted to the length-based lexeme.
 */
static void
psqlscan_record_initial_keyword(const char *identifier, int len,
								char *idents,
								int idents_size,
								int *idents_count)
{
	if (*idents_count < idents_size)
	{
		/*
		 * What we need to recognize is CREATE [OR REPLACE]
		 * FUNCTION/PROCEDURE and CREATE SCHEMA.  Checking for SCHEMA is
		 * useless but not harmful in the CREATE SCHEMA sub-statement case.
		 */
		if (psqlscan_ident_is(identifier, len, "create") ||
			psqlscan_ident_is(identifier, len, "function") ||
			psqlscan_ident_is(identifier, len, "procedure") ||
			psqlscan_ident_is(identifier, len, "or") ||
			psqlscan_ident_is(identifier, len, "replace") ||
			psqlscan_ident_is(identifier, len, "schema"))
			idents[*idents_count] = pg_tolower((unsigned char) identifier[0]);
		/* For other keywords or identifiers, leave '\0' in the array entry */
		(*idents_count)++;
	}
}

/*
 * Does the current input match CREATE [OR REPLACE] {FUNCTION|PROCEDURE}?
 */
static bool
psqlscan_is_create_routine(const char *idents)
{
	return idents[0] == 'c' &&
		(idents[1] == 'f' || idents[1] == 'p' ||
		 (idents[1] == 'o' && idents[2] == 'r' &&
		  (idents[3] == 'f' || idents[3] == 'p')));
}

/*
 * Track whether we are inside a BEGIN .. END block in a function
 * definition, so that semicolons contained therein don't terminate the
 * whole statement.  Short of writing a full parser here, this heuristic
 * should work.  Ported from psqlscan.l's psqlscan_track_identifier,
 * adapted to the length-based (non-NUL-terminated) Lime lexeme.
 */
void
psqlscan_track_identifier(PsqlScanState state, const char *identifier, int len)
{
	bool		is_create_schema;

	/* None of this needs to happen when we're inside parentheses */
	if (state->paren_depth != 0)
		return;

	/* Reset all my state at the start of each new statement */
	if (state->init_idents_count == 0)
	{
		memset(state->init_idents, 0, sizeof(state->init_idents));
		state->sub_idents_count = 0;
		memset(state->sub_idents, 0, sizeof(state->sub_idents));
	}

	/* Record initial keywords if init_idents_count is small enough */
	psqlscan_record_initial_keyword(identifier, len,
									state->init_idents,
									lengthof(state->init_idents),
									&state->init_idents_count);

	/*
	 * In CREATE SCHEMA, track identifiers from each top-level CREATE schema
	 * element separately, so that BEGIN/END tracking is enabled only within
	 * CREATE [OR REPLACE] {FUNCTION|PROCEDURE} clauses.
	 */
	is_create_schema = (state->init_idents[0] == 'c' &&
						state->init_idents[1] == 's');
	if (is_create_schema &&
		state->begin_depth == 0)
	{
		/* Reset sub-clause state at each top-level CREATE keyword */
		if (psqlscan_ident_is(identifier, len, "create"))
		{
			state->sub_idents_count = 0;
			memset(state->sub_idents, 0, sizeof(state->sub_idents));
		}
		/* ... and record the first few keywords following that */
		psqlscan_record_initial_keyword(identifier, len,
										state->sub_idents,
										lengthof(state->sub_idents),
										&state->sub_idents_count);
	}

	/*
	 * Track BEGIN/CASE/END only when within an appropriate (sub) statement.
	 */
	if (psqlscan_is_create_routine(state->init_idents) ||
		(is_create_schema &&
		 psqlscan_is_create_routine(state->sub_idents)))
	{
		if (psqlscan_ident_is(identifier, len, "begin"))
			state->begin_depth++;
		else if (psqlscan_ident_is(identifier, len, "case"))
		{
			/*
			 * CASE also ends with END.  We only need to track this if we are
			 * already inside a BEGIN.
			 */
			if (state->begin_depth >= 1)
				state->begin_depth++;
		}
		else if (psqlscan_ident_is(identifier, len, "end"))
		{
			if (state->begin_depth > 0)
				state->begin_depth--;
		}
	}
}

void
psql_scan_reset(PsqlScanState state)
{
	state->start_state = ST_INITIAL;
	state->paren_depth = 0;
	state->xcdepth = 0;
	if (state->dolqstart)
		free(state->dolqstart);
	state->dolqstart = NULL;
	state->begin_depth = 0;
	state->init_idents_count = 0;
}

void
psql_scan_reselect_sql_lexer(PsqlScanState state)
{
	state->start_state = ST_INITIAL;
}

bool
psql_scan_in_quote(PsqlScanState state)
{
	return state->start_state != ST_INITIAL &&
		state->start_state != ST_XQS;
}

void
psql_scan_get_location(PsqlScanState state, int *lineno, int *offset)
{
	const char *current_end;
	const char *line_end;

	if (state->cur_line_no == 0)
	{
		*lineno = 1;
		*offset = 0;
		return;
	}

	/*
	 * Walk forward from the last-known line pointer to scanbufpos, counting
	 * newlines and updating cur_line_ptr / cur_line_no.  We only ever look at
	 * the outer scanbuf here: while scanning a variable substitution the
	 * cursor in the substitution buffer doesn't advance scanbufpos, which
	 * preserves the historical behaviour from the flex-era scanner.
	 */
	current_end = state->scanbuf + state->scanbufpos;

	while (state->cur_line_ptr < current_end &&
		   (line_end = memchr(state->cur_line_ptr, '\n',
							  current_end - state->cur_line_ptr)) != NULL)
	{
		state->cur_line_no++;
		state->cur_line_ptr = line_end + 1;
	}
	state->cur_line_ptr = current_end;

	*lineno = state->cur_line_no;
	*offset = state->cur_line_ptr - state->scanbuf;
}

/* ------------------------------------------------------------------------- */
/* Internal helpers used by sibling lexers (psqlscanslash.c, exprscan.c).    */
/* ------------------------------------------------------------------------- */

void
psqlscan_push_new_buffer(PsqlScanState state, const char *newstr,
						 const char *varname)
{
	StackElem  *stackelem;

	stackelem = pg_malloc_object(StackElem);
	stackelem->varname = varname ? pg_strdup(varname) : NULL;

	(void) psqlscan_prepare_buffer(state, newstr, strlen(newstr),
								   &stackelem->bufstring);
	stackelem->buflen = strlen(newstr);
	stackelem->pos = 0;

	state->curline = stackelem->bufstring;
	if (state->safe_encoding)
	{
		stackelem->origstring = NULL;
		state->refline = stackelem->bufstring;
	}
	else
	{
		stackelem->origstring = pg_strdup(newstr);
		state->refline = stackelem->origstring;
	}
	stackelem->next = state->buffer_stack;
	state->buffer_stack = stackelem;
}

void
psqlscan_pop_buffer_stack(PsqlScanState state)
{
	StackElem  *stackelem = state->buffer_stack;

	state->buffer_stack = stackelem->next;
	free(stackelem->bufstring);
	if (stackelem->origstring)
		free(stackelem->origstring);
	if (stackelem->varname)
		free(stackelem->varname);
	free(stackelem);
}

void
psqlscan_select_top_buffer(PsqlScanState state)
{
	StackElem  *stackelem = state->buffer_stack;

	if (stackelem != NULL)
	{
		state->curline = stackelem->bufstring;
		state->refline = stackelem->origstring ?
			stackelem->origstring : stackelem->bufstring;
	}
	else
	{
		state->curline = state->scanbuf;
		state->refline = state->scanline;
	}
}

bool
psqlscan_var_is_current_source(PsqlScanState state, const char *varname)
{
	StackElem  *stackelem;

	for (stackelem = state->buffer_stack;
		 stackelem != NULL;
		 stackelem = stackelem->next)
	{
		if (stackelem->varname && strcmp(stackelem->varname, varname) == 0)
			return true;
	}
	return false;
}

void *
psqlscan_prepare_buffer(PsqlScanState state, const char *txt, int len,
						char **txtcopy)
{
	char	   *newtxt;

	newtxt = pg_malloc_array(char, (len + 1));
	*txtcopy = newtxt;

	if (state->safe_encoding)
		memcpy(newtxt, txt, len);
	else
	{
		int			i = 0;

		while (i < len)
		{
			int			thislen = PQmblen(txt + i, state->encoding);

			newtxt[i] = txt[i];
			i++;
			while (--thislen > 0 && i < len)
				newtxt[i++] = (char) 0xFF;
		}
	}
	newtxt[len] = '\0';

	return newtxt;
}

void
psqlscan_emit(PsqlScanState state, const char *txt, int len)
{
	PQExpBuffer output_buf = state->output_buf;

	if (state->safe_encoding)
		appendBinaryPQExpBuffer(output_buf, txt, len);
	else
	{
		const char *reference = state->refline;
		int			i;

		reference += (txt - state->curline);

		for (i = 0; i < len; i++)
		{
			char		ch = txt[i];

			if (ch == (char) 0xFF)
				ch = reference[i];
			appendPQExpBufferChar(output_buf, ch);
		}
	}
}

char *
psqlscan_extract_substring(PsqlScanState state, const char *txt, int len)
{
	char	   *result = pg_malloc_array(char, (len + 1));

	if (state->safe_encoding)
		memcpy(result, txt, len);
	else
	{
		const char *reference = state->refline;
		int			i;

		reference += (txt - state->curline);

		for (i = 0; i < len; i++)
		{
			char		ch = txt[i];

			if (ch == (char) 0xFF)
				ch = reference[i];
			result[i] = ch;
		}
	}
	result[len] = '\0';
	return result;
}

void
psqlscan_escape_variable(PsqlScanState state, const char *txt, int len,
						 PsqlScanQuoteType quote)
{
	char	   *varname;
	char	   *value;

	varname = psqlscan_extract_substring(state, txt + 2, len - 3);
	if (state->callbacks->get_variable)
		value = state->callbacks->get_variable(varname, quote,
											   state->cb_passthrough);
	else
		value = NULL;
	free(varname);

	if (value)
	{
		appendPQExpBufferStr(state->output_buf, value);
		free(value);
	}
	else
	{
		psqlscan_emit(state, txt, len);
	}
}

void
psqlscan_test_variable(PsqlScanState state, const char *txt, int len)
{
	char	   *varname;
	char	   *value;

	varname = psqlscan_extract_substring(state, txt + 3, len - 4);
	if (state->callbacks->get_variable)
		value = state->callbacks->get_variable(varname, PQUOTE_PLAIN,
											   state->cb_passthrough);
	else
		value = NULL;
	free(varname);

	if (value != NULL)
	{
		appendPQExpBufferStr(state->output_buf, "TRUE");
		free(value);
	}
	else
	{
		appendPQExpBufferStr(state->output_buf, "FALSE");
	}
}
