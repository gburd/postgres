/*-------------------------------------------------------------------------
 *
 * pg_fts_match.c
 *		Match evaluation: does an ftsdoc satisfy an ftsquery?
 *
 * The query is a postfix (RPN) item list, so evaluation is a boolean stack
 * machine: a term operand pushes "does this doc contain the term", and each
 * operator pops its arguments and pushes the combined result.  This is the
 * same evaluation strategy tsquery uses (TS_execute), kept deliberately simple
 * for stage 1.  It is O(nitems * log nterms) with the binary-search lookup.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_match.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_fts.h"

bool
fts_doc_matches(FtsDoc doc, FtsQuery query)
{
	FtsQueryItem *items = query->items;
	bool	   *stack;
	int			top = 0;
	uint32		i;
	bool		result;

	/* An empty query matches nothing (there is no positive evidence). */
	if (query->nitems == 0)
		return false;

	stack = (bool *) palloc(query->nitems * sizeof(bool));

	for (i = 0; i < query->nitems; i++)
	{
		FtsQueryItem *it = &items[i];

		if (it->type == FTS_QI_VAL)
		{
			const char *term = FTS_QUERY_ITEMTEXT(query, it);
			bool		present;

			if (it->flags & FTS_QF_PREFIX)
				present = fts_doc_has_prefix(doc, term, it->termlen);
			else
				present = (fts_doc_lookup(doc, term, it->termlen) != NULL);

			stack[top++] = present;
		}
		else if (it->op == FTS_OP_NOT)
		{
			Assert(top >= 1);
			stack[top - 1] = !stack[top - 1];
		}
		else if (it->op == FTS_OP_AND)
		{
			Assert(top >= 2);
			stack[top - 2] = stack[top - 2] && stack[top - 1];
			top--;
		}
		else					/* FTS_OP_OR */
		{
			Assert(top >= 2);
			stack[top - 2] = stack[top - 2] || stack[top - 1];
			top--;
		}
	}

	Assert(top == 1);
	result = stack[0];
	pfree(stack);
	return result;
}

PG_FUNCTION_INFO_V1(fts_match);

/* ftsdoc @@@ ftsquery -> bool */
Datum
fts_match(PG_FUNCTION_ARGS)
{
	FtsDoc		doc = PG_GETARG_FTSDOC(0);
	FtsQuery	query = PG_GETARG_FTSQUERY(1);
	bool		res;

	res = fts_doc_matches(doc, query);

	PG_FREE_IF_COPY(doc, 0);
	PG_FREE_IF_COPY(query, 1);
	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1(fts_match_commutator);

/* ftsquery @@@ ftsdoc -> bool (commutator) */
Datum
fts_match_commutator(PG_FUNCTION_ARGS)
{
	FtsQuery	query = PG_GETARG_FTSQUERY(0);
	FtsDoc		doc = PG_GETARG_FTSDOC(1);
	bool		res;

	res = fts_doc_matches(doc, query);

	PG_FREE_IF_COPY(query, 0);
	PG_FREE_IF_COPY(doc, 1);
	PG_RETURN_BOOL(res);
}
