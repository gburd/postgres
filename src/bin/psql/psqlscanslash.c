/*-------------------------------------------------------------------------
 *
 * psqlscanslash.c
 *	  Driver shim around the Lime-generated slash-command lexer
 *	  (psqlscanslash.lex).
 *
 * The state machine moved to psqlscanslash.lex (Lime v0.2.2's lexer
 * subsystem).  This file holds:
 *
 *	- The public API (psql_scan_slash_command,
 *	  psql_scan_slash_option, psql_scan_slash_command_end,
 *	  psql_scan_get_paren_depth, psql_scan_set_paren_depth,
 *	  dequote_downcase_identifier) declared in psqlscanslash.h.
 *
 *	- The Strategy-D streaming driver: each public entry point sets
 *	  the start_state on the shared PsqlScanState and runs a loop
 *	  that allocates a fresh Slash_Lexer over the remaining bytes of
 *	  the active buffer, calls SlashLexFeedBytes, and either resumes
 *	  (variable expansion) or returns (end-of-cmdname, end-of-arg,
 *	  end-of-line).
 *
 *	- The file-scope state shared with psqlscanslash.lex's action
 *	  bodies (slash_option_type, slash_option_quote, etc.).
 *
 *	- The popen()-based backtick evaluator (slash_evaluate_backtick).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/bin/psql/psqlscanslash.c
 *
 *-------------------------------------------------------------------------
 */
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
#include "psqlscanslash_lex.h"	/* SlashLexer, SlashLexAlloc, ... */

#include "libpq-fe.h"

/* ------------------------------------------------------------------------- */
/* File-scope state.  Read/written by the .lex action bodies via the         */
/* `extern` declarations in psqlscanslash.lex's %include block.  The         */
/* matching extern declarations here satisfy -Wmissing-variable-declarations */
/* for this translation unit (the generated lexer is a separate unit and     */
/* gets its own extern from the .lex %include block).                        */
/* ------------------------------------------------------------------------- */

extern enum slash_option_type slash_option_type;
extern char *slash_option_quote;
extern int	slash_unquoted_option_chars;
extern int	slash_backtick_start_offset;

enum slash_option_type slash_option_type;
char	   *slash_option_quote;
int			slash_unquoted_option_chars;
int			slash_backtick_start_offset;

/* ------------------------------------------------------------------------- */
/* Allocator wrappers.                                                       */
/* ------------------------------------------------------------------------- */

static void *
slash_lex_malloc(size_t n)
{
	return pg_malloc(n);
}

static void
slash_lex_free(void *p)
{
	if (p != NULL)
		free(p);
}

/* ------------------------------------------------------------------------- */
/* Cursor primitives.                                                        */
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
/* The streaming slash-scan loop.                                            */
/* ------------------------------------------------------------------------- */

/*
 * Map ScanState codes to the Lime SLASH_STATE_* values emitted by the
 * Lime-generated header for psqlscanslash.lex.
 */
static int
slash_lime_state(ScanState ss)
{
	switch (ss)
	{
		case ST_XSLASHCMD:
			return SLASH_STATE_XSLASHCMD;
		case ST_XSLASHARGSTART:
			return SLASH_STATE_XSLASHARGSTART;
		case ST_XSLASHARG:
			return SLASH_STATE_XSLASHARG;
		case ST_XSLASHQUOTE:
			return SLASH_STATE_XSLASHQUOTE;
		case ST_XSLASHBACKQUOTE:
			return SLASH_STATE_XSLASHBACKQUOTE;
		case ST_XSLASHDQUOTE:
			return SLASH_STATE_XSLASHDQUOTE;
		case ST_XSLASHWHOLELINE:
			return SLASH_STATE_XSLASHWHOLELINE;
		case ST_XSLASHEND:
			return SLASH_STATE_XSLASHEND;
		default:
			return SLASH_STATE_INITIAL;
	}
}

static ScanState
slash_scan_state_from_lime(int code)
{
	if (code == SLASH_STATE_XSLASHCMD)
		return ST_XSLASHCMD;
	if (code == SLASH_STATE_XSLASHARGSTART)
		return ST_XSLASHARGSTART;
	if (code == SLASH_STATE_XSLASHARG)
		return ST_XSLASHARG;
	if (code == SLASH_STATE_XSLASHQUOTE)
		return ST_XSLASHQUOTE;
	if (code == SLASH_STATE_XSLASHBACKQUOTE)
		return ST_XSLASHBACKQUOTE;
	if (code == SLASH_STATE_XSLASHDQUOTE)
		return ST_XSLASHDQUOTE;
	if (code == SLASH_STATE_XSLASHWHOLELINE)
		return ST_XSLASHWHOLELINE;
	if (code == SLASH_STATE_XSLASHEND)
		return ST_XSLASHEND;
	return ST_INITIAL;
}

static void
slash_lex_emit(void *user, int rule, const char *text, size_t len)
{
	(void) user;
	(void) rule;
	(void) text;

	(void) len;
}

static void
slash_scan_run(PsqlScanState state)
{
	for (;;)
	{
		SlashLexer *lex;
		PsqlEmitCtx ctx;
		const char *buf;
		int			pos;
		int			len;
		int			feed_len;
		SlashLexResult r;

		buf = cur_buf(state);
		pos = cur_pos(state);
		len = cur_buf_len(state);

		if (pos >= len)
		{
			/* End of current buffer */
			if (state->buffer_stack == NULL)
				return;			/* outer EOL */
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

		lex = SlashLexAlloc(slash_lex_malloc);
		SlashLexSetState(lex, slash_lime_state(state->start_state));

		r = SlashLexFeedBytes(lex, buf + pos, (size_t) feed_len,
							  slash_lex_emit, &ctx);

		state->start_state =
			slash_scan_state_from_lime(SlashLexCurrentState(lex));
		SlashLexFree(lex, slash_lex_free);

		if (r == SLASH_LEX_ERROR)
		{
			/*
			 * Shouldn't happen with the catch-all rules in the .lex. Treat as
			 * a dead-letter byte: copy and advance.
			 */
			psqlscan_emit(state, buf + pos, 1);
			advance_cur_pos(state, 1);
			continue;
		}

		if (ctx.stop_kind == STOP_NONE)
		{
			advance_cur_pos(state, feed_len);
			continue;
		}

		advance_cur_pos(state, (int) ctx.consumed);

		switch (ctx.stop_kind)
		{
			case STOP_SLASH_OK:
				return;
			case STOP_VAR_EXPAND:
				psqlscan_push_new_buffer(state, ctx.var_value, ctx.var_name);
				free(ctx.var_value);
				free(ctx.var_name);
				ctx.var_value = NULL;
				ctx.var_name = NULL;
				continue;
			case STOP_NONE:
			case STOP_SEMI:
			case STOP_BACKSLASH:
			case STOP_VAR_RECURSE:
				/* Not used by slash scanner. */
				return;
		}
	}
}

/* ------------------------------------------------------------------------- */
/* Public API.                                                               */
/* ------------------------------------------------------------------------- */

char *
psql_scan_slash_command(PsqlScanState state)
{
	PQExpBufferData mybuf;

	Assert(state->scanbuf != NULL);

	initPQExpBuffer(&mybuf);
	state->output_buf = &mybuf;

	state->start_state = ST_XSLASHCMD;

	slash_scan_run(state);

	psql_scan_reselect_sql_lexer(state);

	return mybuf.data;
}

char *
psql_scan_slash_option(PsqlScanState state,
					   enum slash_option_type type,
					   char *quote,
					   bool semicolon)
{
	PQExpBufferData mybuf;
	ScanState	final_state;
	char		local_quote;

	Assert(state->scanbuf != NULL);

	if (quote == NULL)
		quote = &local_quote;
	*quote = 0;

	initPQExpBuffer(&mybuf);

	slash_option_type = type;
	slash_option_quote = quote;
	slash_unquoted_option_chars = 0;

	state->output_buf = &mybuf;

	if (type == OT_WHOLE_LINE)
		state->start_state = ST_XSLASHWHOLELINE;
	else
		state->start_state = ST_XSLASHARGSTART;

	slash_scan_run(state);

	final_state = state->start_state;

	psql_scan_reselect_sql_lexer(state);

	switch (final_state)
	{
		case ST_XSLASHARGSTART:
			break;
		case ST_XSLASHARG:
			if (semicolon)
			{
				while (slash_unquoted_option_chars-- > 0 &&
					   mybuf.len > 0 &&
					   mybuf.data[mybuf.len - 1] == ';')
				{
					mybuf.data[--mybuf.len] = '\0';
				}
			}
			if (type == OT_SQLID || type == OT_SQLIDHACK)
			{
				dequote_downcase_identifier(mybuf.data,
											(type != OT_SQLIDHACK),
											state->encoding);
				mybuf.len = strlen(mybuf.data);
			}
			break;
		case ST_XSLASHQUOTE:
		case ST_XSLASHBACKQUOTE:
		case ST_XSLASHDQUOTE:
			pg_log_error("unterminated quoted string");
			termPQExpBuffer(&mybuf);
			return NULL;
		case ST_XSLASHWHOLELINE:
			if (semicolon)
			{
				while (mybuf.len > 0 &&
					   (mybuf.data[mybuf.len - 1] == ';' ||
						(isascii((unsigned char) mybuf.data[mybuf.len - 1]) &&
						 isspace((unsigned char) mybuf.data[mybuf.len - 1]))))
				{
					mybuf.data[--mybuf.len] = '\0';
				}
			}
			break;
		default:
			fprintf(stderr, "invalid scan state\n");
			exit(1);
	}

	if (mybuf.len == 0 && *quote == 0)
	{
		termPQExpBuffer(&mybuf);
		return NULL;
	}

	return mybuf.data;
}

void
psql_scan_slash_command_end(PsqlScanState state)
{
	Assert(state->scanbuf != NULL);

	state->output_buf = NULL;
	state->start_state = ST_XSLASHEND;

	slash_scan_run(state);

	psql_scan_reselect_sql_lexer(state);
}

int
psql_scan_get_paren_depth(PsqlScanState state)
{
	return state->paren_depth;
}

void
psql_scan_set_paren_depth(PsqlScanState state, int depth)
{
	Assert(depth >= 0);
	state->paren_depth = depth;
}

void
dequote_downcase_identifier(char *str, bool downcase, int encoding)
{
	bool		inquotes = false;
	char	   *cp = str;

	while (*cp)
	{
		if (*cp == '"')
		{
			if (inquotes && cp[1] == '"')
				cp++;
			else
				inquotes = !inquotes;
			memmove(cp, cp + 1, strlen(cp));
		}
		else
		{
			if (downcase && !inquotes)
				*cp = pg_tolower((unsigned char) *cp);
			cp += PQmblenBounded(cp, encoding);
		}
	}
}

/*
 * Backtick evaluator.  Called from psqlscanslash.lex's bq_close action
 * body (and exposed via the `slash_evaluate_backtick` extern).
 */
extern void slash_evaluate_backtick(PsqlScanState state);
void
slash_evaluate_backtick(PsqlScanState state)
{
	PQExpBuffer output_buf = state->output_buf;
	char	   *cmd = output_buf->data + slash_backtick_start_offset;
	PQExpBufferData cmd_output;
	FILE	   *fd;
	bool		error = false;
	int			exit_code = 0;
	char		buf[512];
	size_t		result;

	initPQExpBuffer(&cmd_output);

	fflush(NULL);
	fd = popen(cmd, "r");
	if (!fd)
	{
		pg_log_error("%s: %m", cmd);
		error = true;
		exit_code = -1;
	}

	if (!error)
	{
		do
		{
			result = fread(buf, 1, sizeof(buf), fd);
			if (ferror(fd))
			{
				pg_log_error("%s: %m", cmd);
				error = true;
				break;
			}
			appendBinaryPQExpBuffer(&cmd_output, buf, result);
		} while (!feof(fd));
	}

	if (fd)
	{
		exit_code = pclose(fd);
		if (exit_code == -1)
		{
			pg_log_error("%s: %m", cmd);
			error = true;
		}
	}

	if (PQExpBufferDataBroken(cmd_output))
	{
		pg_log_error("%s: out of memory", cmd);
		error = true;
	}

	output_buf->len = slash_backtick_start_offset;
	output_buf->data[output_buf->len] = '\0';

	if (!error)
	{
		if (cmd_output.len > 0 &&
			cmd_output.data[cmd_output.len - 1] == '\n')
			cmd_output.len--;
		appendBinaryPQExpBuffer(output_buf, cmd_output.data, cmd_output.len);
	}

	SetShellResultVariables(exit_code);

	termPQExpBuffer(&cmd_output);
}
