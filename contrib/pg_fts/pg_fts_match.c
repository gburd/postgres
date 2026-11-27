/*-------------------------------------------------------------------------
 *
 * pg_fts_match.c
 *		Match evaluation: does an ftsdoc satisfy an ftsquery?
 *
 * The query is a postfix (RPN) item list, so evaluation is a stack machine: a
 * term operand pushes "does this doc contain the term", and each operator pops
 * its arguments and pushes the combined result.  Beyond boolean AND/OR/NOT it
 * evaluates phrase and NEAR (using per-term positions), prefix, fuzzy (bounded
 * Levenshtein) and regex operands.  It mirrors tsquery's TS_execute strategy;
 * O(nitems * log nterms) with the binary-search term lookup.
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

/*
 * Each stack entry is a boolean presence plus, for term and phrase operands, a
 * position list (ascending token positions where the operand ends).  Phrase
 * evaluation intersects a left operand's positions with a right term's
 * positions offset by 1..distance, chaining across a multi-word phrase.
 * Boolean operators (AND/OR/NOT) collapse to presence and drop positions.
 */
typedef struct MatchVal
{
	bool		present;
	uint32	   *pos;			/* NULL if positions unavailable/irrelevant */
	int			npos;
}			MatchVal;

/*
 * Positions where a (possibly prefix) term occurs.  For an exact term this is
 * the stored position list; for a prefix term we merge the position lists of
 * all matching terms (rare, so a simple concat + sort).  If the doc carries no
 * positions, returns present-without-positions.
 */
static MatchVal
term_positions(FtsDoc doc, const char *term, int termlen, uint16 flags,
			   uint32 distance)
{
	MatchVal	v;

	v.present = false;
	v.pos = NULL;
	v.npos = 0;

	if (flags & FTS_QF_REGEX)
	{
		v.present = fts_doc_has_regex(doc, term, termlen);
		return v;
	}
	if (flags & FTS_QF_FUZZY)
	{
		v.present = fts_doc_has_fuzzy(doc, term, termlen, (int) distance);
		return v;
	}
	if (flags & FTS_QF_PREFIX)
	{
		/* presence only; phrase-with-prefix is not tracked positionally */
		v.present = fts_doc_has_prefix(doc, term, termlen);
		return v;
	}
	else
	{
		FtsTermEntry *e = fts_doc_lookup(doc, term, termlen);

		if (e == NULL)
			return v;
		v.present = true;
		if (FTS_DOC_HAS_POS(doc))
		{
			v.pos = FTS_DOC_TERMPOS(doc, e);
			v.npos = (int) e->tf;
		}
		return v;
	}
}

/*
 * Phrase step: given left positions (ends of the matched-so-far prefix) and
 * the right term's positions, return the right positions p such that some left
 * position L satisfies 0 < p - L <= distance.  Both inputs are ascending.
 */
static MatchVal
phrase_step(MatchVal left, MatchVal right, uint32 distance)
{
	MatchVal	r;
	int			li = 0,
				ri = 0,
				k = 0;

	r.present = false;
	r.pos = NULL;
	r.npos = 0;

	/* If either side lacks positions, fall back to presence-only AND: we
	 * cannot verify adjacency, so treat the phrase as a conjunction (recall
	 * preserved, precision degraded -- documented). */
	if (left.pos == NULL || right.pos == NULL)
	{
		r.present = left.present && right.present;
		return r;
	}

	r.pos = (uint32 *) palloc(right.npos * sizeof(uint32));
	for (ri = 0; ri < right.npos; ri++)
	{
		uint32		p = right.pos[ri];

		/* advance li to the first left position that could be in range */
		while (li < left.npos && left.pos[li] + distance < p)
			li++;
		/* any left position L with p-distance <= L < p works */
		if (li < left.npos && left.pos[li] < p &&
			p - left.pos[li] <= distance)
			r.pos[k++] = p;
	}
	r.npos = k;
	r.present = (k > 0);
	return r;
}

bool
fts_doc_matches(FtsDoc doc, FtsQuery query)
{
	FtsQueryItem *items = query->items;
	MatchVal   *stack;
	int			top = 0;
	uint32		i;
	bool		result;

	/* An empty query matches nothing (there is no positive evidence). */
	if (query->nitems == 0)
		return false;

	stack = (MatchVal *) palloc(query->nitems * sizeof(MatchVal));

	for (i = 0; i < query->nitems; i++)
	{
		FtsQueryItem *it = &items[i];

		if (it->type == FTS_QI_VAL)
		{
			stack[top++] = term_positions(doc, FTS_QUERY_ITEMTEXT(query, it),
										  it->termlen, it->flags,
										  it->distance);
		}
		else if (it->op == FTS_OP_NOT)
		{
			Assert(top >= 1);
			stack[top - 1].present = !stack[top - 1].present;
			stack[top - 1].pos = NULL;
			stack[top - 1].npos = 0;
		}
		else if (it->op == FTS_OP_PHRASE)
		{
			Assert(top >= 2);
			stack[top - 2] = phrase_step(stack[top - 2], stack[top - 1],
										 it->distance);
			top--;
		}
		else if (it->op == FTS_OP_AND)
		{
			Assert(top >= 2);
			stack[top - 2].present = stack[top - 2].present && stack[top - 1].present;
			stack[top - 2].pos = NULL;
			stack[top - 2].npos = 0;
			top--;
		}
		else					/* FTS_OP_OR */
		{
			Assert(top >= 2);
			stack[top - 2].present = stack[top - 2].present || stack[top - 1].present;
			stack[top - 2].pos = NULL;
			stack[top - 2].npos = 0;
			top--;
		}
	}

	Assert(top == 1);
	result = stack[0].present;
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
