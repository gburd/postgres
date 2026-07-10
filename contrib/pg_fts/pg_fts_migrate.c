/*-------------------------------------------------------------------------
 *
 * pg_fts_migrate.c
 *		Migration helpers from the existing tsvector/tsquery stack to pg_fts.
 *
 * Stage 11 of pg_fts.  tsquery_to_ftsquery() mechanically converts a tsquery
 * into an ftsquery so existing queries port with minimal churn: & -> AND,
 * | -> OR, ! -> NOT, and the phrase operator <N> (OP_PHRASE) -> ftsquery
 * FTS_OP_PHRASE preserving the token gap, so adjacency is carried over
 * faithfully.
 *
 * tsquery is stored in prefix (Polish) order; ftsquery is postfix (RPN).  We
 * walk the tsquery tree recursively and emit postfix items.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_migrate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_fts.h"
#include "tsearch/ts_type.h"
#include "tsearch/ts_utils.h"
#include "utils/builtins.h"

/* An emitted ftsquery item, collected before flattening. */
typedef struct MigItem
{
	uint8		type;
	uint8		op;
	uint32		distance;		/* max token gap for FTS_OP_PHRASE (else unused) */
	char	   *term;			/* folded term (lowercased) for VAL items */
	int			termlen;
}			MigItem;

typedef struct MigState
{
	TSQuery		query;
	char	   *operands;		/* base of operand text */
	MigItem    *items;
	int			nitems;
	int			maxitems;
}			MigState;

static void
mig_emit(MigState *st, uint8 type, uint8 op, uint32 distance, char *term, int termlen)
{
	if (st->nitems >= st->maxitems)
	{
		st->maxitems = st->maxitems ? st->maxitems * 2 : 16;
		if (st->items == NULL)
			st->items = (MigItem *) palloc(st->maxitems * sizeof(MigItem));
		else
			st->items = (MigItem *) repalloc(st->items,
											 st->maxitems * sizeof(MigItem));
	}
	st->items[st->nitems].type = type;
	st->items[st->nitems].op = op;
	st->items[st->nitems].distance = distance;
	st->items[st->nitems].term = term;
	st->items[st->nitems].termlen = termlen;
	st->nitems++;
}

/* Recursively walk the tsquery item at index `pos`, emitting postfix. */
static void
mig_walk(MigState *st, QueryItem *item)
{
	if (item->type == QI_VAL)
	{
		QueryOperand *op = &item->qoperand;
		char	   *src = st->operands + op->distance;
		char	   *folded = (char *) palloc(op->length);
		int			i;

		/* tsquery lexemes are already normalized; copy verbatim (they are the
		 * dictionary output, so no further folding is applied). */
		for (i = 0; i < (int) op->length; i++)
			folded[i] = src[i];
		mig_emit(st, FTS_QI_VAL, 0, 0, folded, op->length);
	}
	else						/* QI_OPR */
	{
		QueryOperator *op = &item->qoperator;

		if (op->oper == OP_NOT)
		{
			/* NOT has a single (right) operand at item+1 */
			mig_walk(st, item + 1);
			mig_emit(st, FTS_QI_OPR, FTS_OP_NOT, 0, NULL, 0);
		}
		else
		{
			QueryItem  *left = item + op->left;
			QueryItem  *right = item + 1;
			uint8		ftop;
			uint32		dist = 1;

			mig_walk(st, left);
			mig_walk(st, right);

			switch (op->oper)
			{
				case OP_AND:
					ftop = FTS_OP_AND;
					break;
				case OP_OR:
					ftop = FTS_OP_OR;
					break;
				case OP_PHRASE:
					/* faithful: tsquery <N> -> ftsquery phrase with the same gap */
					ftop = FTS_OP_PHRASE;
					dist = op->distance;
					break;
				default:
					ftop = FTS_OP_AND;
					break;
			}
			mig_emit(st, FTS_QI_OPR, ftop, dist, NULL, 0);
		}
	}
}

PG_FUNCTION_INFO_V1(tsquery_to_ftsquery);

Datum
tsquery_to_ftsquery(PG_FUNCTION_ARGS)
{
	TSQuery		query = PG_GETARG_TSQUERY(0);
	MigState	st;
	FtsQuery	q;
	FtsQueryItem *items;
	char	   *textbase;
	Size		textbytes = 0;
	Size		total;
	uint32		off = 0;
	int			i;

	st.query = query;
	st.operands = GETOPERAND(query);
	st.items = NULL;
	st.nitems = 0;
	st.maxitems = 0;

	if (query->size > 0)
		mig_walk(&st, GETQUERY(query));

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
		items[i].flags = 0;
		items[i].distance = st.items[i].distance;
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

	PG_FREE_IF_COPY(query, 0);
	PG_RETURN_FTSQUERY(q);
}
