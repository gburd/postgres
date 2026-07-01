/*-------------------------------------------------------------------------
 *
 * pg_fts_migrate.c
 *		Migration helpers from the existing tsvector/tsquery stack to pg_fts.
 *
 * Stage 11 of pg_fts.  tsquery_to_ftsquery() mechanically converts a tsquery
 * into an ftsquery so existing queries port with minimal churn: & -> AND,
 * | -> OR, ! -> NOT.  The phrase operator <-> (OP_PHRASE) has no stage-1
 * ftsquery equivalent yet (phrase support is a later stage), so it is
 * converted to AND with a NOTICE, which preserves recall while losing the
 * adjacency constraint -- a safe, documented degradation for migration.
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
	bool		phrase_seen;
}			MigState;

static void
mig_emit(MigState *st, uint8 type, uint8 op, char *term, int termlen)
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
		mig_emit(st, FTS_QI_VAL, 0, folded, op->length);
	}
	else						/* QI_OPR */
	{
		QueryOperator *op = &item->qoperator;

		if (op->oper == OP_NOT)
		{
			/* NOT has a single (right) operand at item+1 */
			mig_walk(st, item + 1);
			mig_emit(st, FTS_QI_OPR, FTS_OP_NOT, NULL, 0);
		}
		else
		{
			QueryItem  *left = item + op->left;
			QueryItem  *right = item + 1;
			uint8		ftop;

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
					/* no phrase yet: degrade to AND (recall preserved) */
					st->phrase_seen = true;
					ftop = FTS_OP_AND;
					break;
				default:
					ftop = FTS_OP_AND;
					break;
			}
			mig_emit(st, FTS_QI_OPR, ftop, NULL, 0);
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
	st.phrase_seen = false;

	if (query->size > 0)
		mig_walk(&st, GETQUERY(query));

	if (st.phrase_seen)
		ereport(NOTICE,
				(errmsg("tsquery phrase operator (<->) converted to AND"),
				 errdetail("ftsquery does not support phrase search yet; "
						   "adjacency constraints were dropped.")));

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

	PG_FREE_IF_COPY(query, 0);
	PG_RETURN_FTSQUERY(q);
}
