/*-------------------------------------------------------------------------
 *
 * pg_fts_query.c
 *		Query-text parser and I/O for the ftsquery type.
 *
 * Stage-1 query grammar (recursive descent, no generator -- the grammar is
 * small and this keeps the extension self-contained):
 *
 *	  expr    := or_expr
 *	  or_expr := and_expr ( ('|' | 'OR') and_expr )*
 *	  and_expr:= unary ( ('&' | 'AND')? unary )*        -- implicit AND
 *	  unary   := ('!' | 'NOT' | '-') unary | primary
 *	  primary := '(' expr ')' | term
 *	  term    := run of token bytes (folded like the analyzer)
 *
 * The parser emits a postfix (RPN) item list, the same shape tsquery uses, so
 * evaluation is a simple stack machine.  Later stages add phrase ("..."),
 * NEAR, prefix (term*), field scoping (field:term) and boosts as new token
 * kinds and item kinds; the version field guards the on-disk format.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_query.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_fts.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"

/* An operand collected during the parse, before flattening to a varlena. */
typedef struct ParsedItem
{
	uint8		type;			/* FtsQueryItemType */
	uint8		op;				/* FtsQueryOp when type == FTS_QI_OPR */
	char	   *term;			/* palloc'd folded term when type == FTS_QI_VAL */
	int			termlen;
} ParsedItem;

/* Token kinds returned by the lexer. */
typedef enum
{
	TOK_EOF,
	TOK_TERM,
	TOK_AND,
	TOK_OR,
	TOK_NOT,
	TOK_LPAREN,
	TOK_RPAREN
} TokKind;

typedef struct Token
{
	TokKind		kind;
	char	   *term;			/* folded term text for TOK_TERM */
	int			termlen;
} Token;

typedef struct ParseState
{
	const char *buf;
	int			len;
	int			pos;
	ParsedItem *items;
	int			nitems;
	int			maxitems;
	bool		error;
	bool		have_peeked;	/* is peeked valid? */
	Token		peeked;			/* one-token lookahead cache */
} ParseState;

static void parse_or(ParseState *st);

static inline bool
is_token_byte(unsigned char c)
{
	if (c >= 0x80)
		return true;
	return (c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9');
}

static inline char
fold_ascii(unsigned char c)
{
	if (c >= 'A' && c <= 'Z')
		return (char) (c - 'A' + 'a');
	return (char) c;
}

static void
emit(ParseState *st, uint8 type, uint8 op, char *term, int termlen)
{
	if (st->nitems >= st->maxitems)
	{
		st->maxitems = st->maxitems ? st->maxitems * 2 : 16;
		if (st->items == NULL)
			st->items = (ParsedItem *) palloc(st->maxitems * sizeof(ParsedItem));
		else
			st->items = (ParsedItem *) repalloc(st->items,
												st->maxitems * sizeof(ParsedItem));
	}
	st->items[st->nitems].type = type;
	st->items[st->nitems].op = op;
	st->items[st->nitems].term = term;
	st->items[st->nitems].termlen = termlen;
	st->nitems++;
}

/*
 * Raw lexer.  Recognizes &, |, !, - and parentheses as punctuation; the
 * keywords AND/OR/NOT (case-insensitive) as operators; everything else is a
 * term.  A bare "and"/"or"/"not" is treated as an operator only when it stands
 * alone as a token, which is the standard, least-surprising behavior.
 *
 * Callers use next_token()/peek() rather than calling this directly, so that a
 * peeked token is lexed (and its term palloc'd) exactly once.
 */
static Token
lex_raw(ParseState *st)
{
	Token		tok = {TOK_EOF, NULL, 0};
	int			start;
	int			flen;
	int			i;
	char	   *folded;

	/* skip whitespace */
	while (st->pos < st->len &&
		   !is_token_byte((unsigned char) st->buf[st->pos]))
	{
		char		c = st->buf[st->pos];

		switch (c)
		{
			case '&':
				st->pos++;
				tok.kind = TOK_AND;
				return tok;
			case '|':
				st->pos++;
				tok.kind = TOK_OR;
				return tok;
			case '!':
			case '-':
				st->pos++;
				tok.kind = TOK_NOT;
				return tok;
			case '(':
				st->pos++;
				tok.kind = TOK_LPAREN;
				return tok;
			case ')':
				st->pos++;
				tok.kind = TOK_RPAREN;
				return tok;
			default:
				st->pos++;		/* ordinary separator */
				break;
		}
	}
	if (st->pos >= st->len)
		return tok;				/* TOK_EOF */

	/* a term: run of token bytes, folded */
	start = st->pos;
	while (st->pos < st->len &&
		   is_token_byte((unsigned char) st->buf[st->pos]))
		st->pos++;
	flen = st->pos - start;

	folded = (char *) palloc(flen);
	for (i = 0; i < flen; i++)
		folded[i] = fold_ascii((unsigned char) st->buf[start + i]);

	/* keyword recognition (ASCII, case already folded) */
	if (flen == 3 && memcmp(folded, "and", 3) == 0)
		tok.kind = TOK_AND;
	else if (flen == 2 && memcmp(folded, "or", 2) == 0)
		tok.kind = TOK_OR;
	else if (flen == 3 && memcmp(folded, "not", 3) == 0)
		tok.kind = TOK_NOT;
	else
	{
		tok.kind = TOK_TERM;
		tok.term = folded;
		tok.termlen = flen;
	}
	return tok;
}

/*
 * next_token -- consume and return the next token, using the one-token
 * lookahead cache if a peek() filled it.
 */
static Token
next_token(ParseState *st)
{
	if (st->have_peeked)
	{
		st->have_peeked = false;
		return st->peeked;
	}
	return lex_raw(st);
}

/* peek -- return the next token without consuming it (lexed at most once) */
static Token
peek(ParseState *st)
{
	if (!st->have_peeked)
	{
		st->peeked = lex_raw(st);
		st->have_peeked = true;
	}
	return st->peeked;
}

/* primary := '(' expr ')' | term */
static void
parse_primary(ParseState *st)
{
	Token		tok = next_token(st);

	if (tok.kind == TOK_LPAREN)
	{
		parse_or(st);
		tok = next_token(st);
		if (tok.kind != TOK_RPAREN)
			st->error = true;
	}
	else if (tok.kind == TOK_TERM)
	{
		emit(st, FTS_QI_VAL, 0, tok.term, tok.termlen);
	}
	else
	{
		st->error = true;
	}
}

/* unary := NOT unary | primary */
static void
parse_unary(ParseState *st)
{
	Token		tok = peek(st);

	if (tok.kind == TOK_NOT)
	{
		(void) next_token(st);
		parse_unary(st);
		emit(st, FTS_QI_OPR, FTS_OP_NOT, NULL, 0);
	}
	else
		parse_primary(st);
}

/* and_expr := unary ( AND? unary )*  (implicit AND between adjacent terms) */
static void
parse_and(ParseState *st)
{
	parse_unary(st);
	for (;;)
	{
		Token		tok = peek(st);

		if (tok.kind == TOK_AND)
		{
			(void) next_token(st);
			parse_unary(st);
			emit(st, FTS_QI_OPR, FTS_OP_AND, NULL, 0);
		}
		else if (tok.kind == TOK_TERM || tok.kind == TOK_NOT ||
				 tok.kind == TOK_LPAREN)
		{
			/* implicit AND */
			parse_unary(st);
			emit(st, FTS_QI_OPR, FTS_OP_AND, NULL, 0);
		}
		else
			break;
	}
}

/* or_expr := and_expr ( OR and_expr )* */
static void
parse_or(ParseState *st)
{
	parse_and(st);
	for (;;)
	{
		Token		tok = peek(st);

		if (tok.kind == TOK_OR)
		{
			(void) next_token(st);
			parse_and(st);
			emit(st, FTS_QI_OPR, FTS_OP_OR, NULL, 0);
		}
		else
			break;
	}
}

/*
 * fts_parse_query -- parse query text into an FtsQuery varlena.
 * Raises an error on malformed input.  An input with no terms yields a valid
 * empty query (matches nothing).
 */
FtsQuery
fts_parse_query(const char *str, int len)
{
	ParseState	st;
	FtsQuery	q;
	FtsQueryItem *items;
	char	   *textbase;
	Size		textbytes = 0;
	Size		total;
	uint32		off = 0;
	int			i;

	st.buf = str;
	st.len = len;
	st.pos = 0;
	st.items = NULL;
	st.nitems = 0;
	st.maxitems = 0;
	st.error = false;
	st.have_peeked = false;

	/* Only parse if there is at least one token; else empty query. */
	if (peek(&st).kind != TOK_EOF)
	{
		parse_or(&st);
		if (!st.error && peek(&st).kind != TOK_EOF)
			st.error = true;	/* trailing garbage */
	}

	if (st.error)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("syntax error in ftsquery: \"%.*s\"", len, str)));

	for (i = 0; i < st.nitems; i++)
		if (st.items[i].type == FTS_QI_VAL)
			textbytes += st.items[i].termlen;

	total = FTS_QUERY_HDRSIZE +
		(Size) st.nitems * sizeof(FtsQueryItem) + textbytes;
	q = (FtsQuery) palloc0(total);
	SET_VARSIZE(q, total);
	q->version = FTS_QUERY_VERSION;
	q->flags = 0;
	q->nitems = st.nitems;

	items = q->items;
	textbase = FTS_QUERY_TEXTBASE(q);
	for (i = 0; i < st.nitems; i++)
	{
		items[i].type = st.items[i].type;
		items[i].op = st.items[i].op;
		items[i].pad = 0;
		if (st.items[i].type == FTS_QI_VAL)
		{
			items[i].termoff = off;
			items[i].termlen = st.items[i].termlen;
			memcpy(textbase + off, st.items[i].term, st.items[i].termlen);
			off += st.items[i].termlen;
		}
		else
		{
			items[i].termoff = 0;
			items[i].termlen = 0;
		}
	}

	return q;
}

PG_FUNCTION_INFO_V1(ftsquery_in);
PG_FUNCTION_INFO_V1(ftsquery_out);
PG_FUNCTION_INFO_V1(ftsquery_recv);
PG_FUNCTION_INFO_V1(ftsquery_send);
PG_FUNCTION_INFO_V1(to_ftsquery);

Datum
ftsquery_in(PG_FUNCTION_ARGS)
{
	char	   *in = PG_GETARG_CSTRING(0);

	PG_RETURN_FTSQUERY(fts_parse_query(in, strlen(in)));
}

Datum
to_ftsquery(PG_FUNCTION_ARGS)
{
	text	   *in = PG_GETARG_TEXT_PP(0);
	FtsQuery	q;

	q = fts_parse_query(VARDATA_ANY(in), VARSIZE_ANY_EXHDR(in));
	PG_FREE_IF_COPY(in, 0);
	PG_RETURN_FTSQUERY(q);
}

/*
 * Render an ftsquery as fully parenthesised infix, so the output round-trips
 * back through the parser to an equivalent query.  Postfix RPN is walked with
 * a small string stack.
 */
Datum
ftsquery_out(PG_FUNCTION_ARGS)
{
	FtsQuery	q = PG_GETARG_FTSQUERY(0);
	FtsQueryItem *items = q->items;
	StringInfoData *stack;
	int			top = 0;
	uint32		i;
	StringInfoData result;

	if (q->nitems == 0)
	{
		PG_FREE_IF_COPY(q, 0);
		PG_RETURN_CSTRING(pstrdup(""));
	}

	stack = (StringInfoData *) palloc(q->nitems * sizeof(StringInfoData));

	for (i = 0; i < q->nitems; i++)
	{
		FtsQueryItem *it = &items[i];

		if (it->type == FTS_QI_VAL)
		{
			StringInfoData s;
			int			j;
			const char *t = FTS_QUERY_ITEMTEXT(q, it);

			initStringInfo(&s);
			appendStringInfoChar(&s, '\'');
			for (j = 0; j < (int) it->termlen; j++)
			{
				if (t[j] == '\'' || t[j] == '\\')
					appendStringInfoChar(&s, '\\');
				appendStringInfoChar(&s, t[j]);
			}
			appendStringInfoChar(&s, '\'');
			stack[top++] = s;
		}
		else if (it->op == FTS_OP_NOT)
		{
			StringInfoData s;

			Assert(top >= 1);
			initStringInfo(&s);
			appendStringInfoString(&s, "!");
			appendBinaryStringInfo(&s, stack[top - 1].data,
								   stack[top - 1].len);
			pfree(stack[top - 1].data);
			stack[top - 1] = s;
		}
		else
		{
			StringInfoData s;
			const char *opstr = (it->op == FTS_OP_AND) ? " & " : " | ";

			Assert(top >= 2);
			initStringInfo(&s);
			appendStringInfoChar(&s, '(');
			appendBinaryStringInfo(&s, stack[top - 2].data,
								   stack[top - 2].len);
			appendStringInfoString(&s, opstr);
			appendBinaryStringInfo(&s, stack[top - 1].data,
								   stack[top - 1].len);
			appendStringInfoChar(&s, ')');
			pfree(stack[top - 1].data);
			pfree(stack[top - 2].data);
			top -= 2;
			stack[top++] = s;
		}
	}

	Assert(top == 1);
	initStringInfo(&result);
	appendBinaryStringInfo(&result, stack[0].data, stack[0].len);
	pfree(stack[0].data);

	PG_FREE_IF_COPY(q, 0);
	PG_RETURN_CSTRING(result.data);
}

Datum
ftsquery_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	uint16		version;
	uint32		nitems;
	FtsQuery	q;
	FtsQueryItem *items;
	char	   *textbase;
	uint8	   *types;
	uint8	   *ops;
	char	  **terms;
	int		   *lens;
	Size		textbytes = 0;
	uint32		off = 0;
	uint32		i;

	version = (uint16) pq_getmsgint(buf, 2);
	if (version != FTS_QUERY_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
				 errmsg("unsupported ftsquery version number %u", version)));

	nitems = (uint32) pq_getmsgint(buf, 4);

	types = (uint8 *) palloc(nitems * sizeof(uint8));
	ops = (uint8 *) palloc(nitems * sizeof(uint8));
	terms = (char **) palloc(nitems * sizeof(char *));
	lens = (int *) palloc(nitems * sizeof(int));

	for (i = 0; i < nitems; i++)
	{
		types[i] = (uint8) pq_getmsgint(buf, 1);
		ops[i] = (uint8) pq_getmsgint(buf, 1);
		if (types[i] == FTS_QI_VAL)
		{
			const char *t;

			lens[i] = pq_getmsgint(buf, 4);
			if (lens[i] < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
						 errmsg("invalid ftsquery term length")));
			t = pq_getmsgbytes(buf, lens[i]);
			terms[i] = (char *) palloc(lens[i]);
			memcpy(terms[i], t, lens[i]);
			textbytes += lens[i];
		}
		else
		{
			if (types[i] != FTS_QI_OPR ||
				(ops[i] != FTS_OP_NOT && ops[i] != FTS_OP_AND &&
				 ops[i] != FTS_OP_OR))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
						 errmsg("invalid ftsquery item")));
			lens[i] = 0;
			terms[i] = NULL;
		}
	}

	{
		Size		total = FTS_QUERY_HDRSIZE +
			(Size) nitems * sizeof(FtsQueryItem) + textbytes;

		q = (FtsQuery) palloc0(total);
		SET_VARSIZE(q, total);
		q->version = FTS_QUERY_VERSION;
		q->flags = 0;
		q->nitems = nitems;

		items = q->items;
		textbase = FTS_QUERY_TEXTBASE(q);
		for (i = 0; i < nitems; i++)
		{
			items[i].type = types[i];
			items[i].op = ops[i];
			items[i].pad = 0;
			if (types[i] == FTS_QI_VAL)
			{
				items[i].termoff = off;
				items[i].termlen = lens[i];
				memcpy(textbase + off, terms[i], lens[i]);
				off += lens[i];
			}
		}
	}

	PG_RETURN_FTSQUERY(q);
}

Datum
ftsquery_send(PG_FUNCTION_ARGS)
{
	FtsQuery	q = PG_GETARG_FTSQUERY(0);
	FtsQueryItem *items = q->items;
	StringInfoData buf;
	uint32		i;

	pq_begintypsend(&buf);
	pq_sendint16(&buf, q->version);
	pq_sendint32(&buf, q->nitems);
	for (i = 0; i < q->nitems; i++)
	{
		pq_sendint8(&buf, items[i].type);
		pq_sendint8(&buf, items[i].op);
		if (items[i].type == FTS_QI_VAL)
		{
			pq_sendint32(&buf, items[i].termlen);
			pq_sendbytes(&buf, FTS_QUERY_ITEMTEXT(q, &items[i]),
						 items[i].termlen);
		}
	}

	PG_FREE_IF_COPY(q, 0);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}
