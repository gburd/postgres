/*-------------------------------------------------------------------------
 *
 * pg_fts_trgm.c
 *		Trigram pre-filter for fuzzy/regex term matching at scale.
 *
 * Cribbed in spirit from pg_tre (the approximate-regex access method): a
 * pattern is reduced to a set of required trigrams, and only dictionary terms
 * sharing a trigram with the pattern are candidates for the expensive exact
 * test (Levenshtein for fuzzy, regex execution for regex).  This turns the
 * naive "test every term" scan into "test only trigram-overlapping terms",
 * which is the pruning that makes fuzzy/regex viable on a large vocabulary.
 *
 * For fuzzy matching with edit distance k, a term of t trigrams that matches
 * within k edits must share at least (t - k*3) ... in practice we use the
 * pigeonhole guarantee that at least one trigram of the query survives k edits
 * only when k is small; for correctness we require overlap of >= 1 trigram,
 * which is a sound filter for k below the term's trigram count and falls back
 * to a full scan otherwise (so results are always correct, only speed varies).
 *
 * This module implements the trigram extraction and the candidate-narrowing
 * used by the matcher; the persistent on-disk trigram posting index that the
 * bm25 AM uses at query time is in pg_fts_trgm_index.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_trgm.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_fts.h"
#include "common/hashfn.h"

/*
 * Extract the set of byte-trigrams from a term into caller-provided storage.
 * A term of length n < 3 yields a single padded trigram so short terms still
 * participate.  Returns the number of trigrams written (deduplicated).
 */
int
fts_trigrams(const char *s, int len, uint32 *out, int maxout)
{
	int			n = 0;
	int			i;

	if (len <= 0)
		return 0;

	if (len < 3)
	{
		char		pad[3] = {' ', ' ', ' '};
		int			j;

		for (j = 0; j < len; j++)
			pad[j] = s[j];
		if (n < maxout)
			out[n++] = hash_bytes((const unsigned char *) pad, 3);
		return n;
	}

	for (i = 0; i + 3 <= len; i++)
	{
		uint32		h = hash_bytes((const unsigned char *) (s + i), 3);
		int			k;
		bool		dup = false;

		for (k = 0; k < n; k++)
			if (out[k] == h)
			{
				dup = true;
				break;
			}
		if (!dup && n < maxout)
			out[n++] = h;
	}
	return n;
}

/*
 * Do two trigram sets share at least one trigram?  Both are small (bounded by
 * term length), so a nested scan is fine.
 */
bool
fts_trigrams_overlap(const uint32 *a, int na, const uint32 *b, int nb)
{
	int			i,
				j;

	for (i = 0; i < na; i++)
		for (j = 0; j < nb; j++)
			if (a[i] == b[j])
				return true;
	return false;
}

/*
 * fts_regex_trigrams -- extract required trigrams from a regular expression by
 * tiling its literal runs (a lightweight form of the Navarro/pg_tre approach).
 *
 * We scan the regex, tracking maximal runs of ordinary literal characters
 * (skipping over metacharacters and the constructs they introduce), and emit
 * the trigrams of each run.  A document matching the regex must contain some
 * literal run's text verbatim only when that run is *required* -- i.e. not
 * inside an alternation, optional, or repetition-zero construct.  To stay
 * sound we break a run (and drop trailing chars) at any operator that could
 * make the preceding character optional (*, ?, {0,...}) or introduce a branch
 * (|), and we only emit trigrams from runs that remain required.
 *
 * The result is a set of trigrams such that every matching string contains all
 * of them.  The caller UNIONs each trigram's posting set (a sound superset of
 * candidate terms) and then rechecks exactly -- so a term is a candidate if it
 * shares any required trigram; correctness comes from the exact recheck.  When
 * no required run yields a trigram (e.g. the regex is all alternation/optional),
 * we return 0 and the caller falls back to a full scan -- always correct.
 */
int
fts_regex_trigrams(const char *re, int relen, uint32 *out, int maxout)
{
	int			n = 0;
	int			i = 0;
	char		run[256];
	int			runlen = 0;

	/* flush the current literal run's trigrams, honoring a possible trailing
	 * quantifier that makes the LAST char optional (drop it in that case) */
	#define FLUSH_RUN(drop_last)										\
	do {															\
		int _rl = (runlen) - ((drop_last) ? 1 : 0);					\
		if (_rl >= 3)												\
		{															\
			int _t = fts_trigrams(run, _rl, out + n, maxout - n);	\
			n += _t;												\
		}															\
		runlen = 0;													\
	} while (0)

	while (i < relen && n < maxout)
	{
		char		c = re[i];

		switch (c)
		{
			case '\\':
				/* escaped literal: the next char is a literal */
				if (i + 1 < relen)
				{
					if (runlen < (int) sizeof(run))
						run[runlen++] = re[i + 1];
					i += 2;
				}
				else
					i++;
				break;
			case '*':
			case '?':
				/* previous char is optional -> drop it and flush */
				FLUSH_RUN(true);
				i++;
				break;
			case '+':
				/* previous char required (>=1) -> keep it, flush at boundary */
				FLUSH_RUN(false);
				i++;
				break;
			case '{':
				/* quantifier {m,n}: if it can be zero, the prev char is
				 * optional.  Conservatively drop the last char and skip to } */
				FLUSH_RUN(true);
				while (i < relen && re[i] != '}')
					i++;
				if (i < relen)
					i++;
				break;
			case '|':
				/* alternation: everything so far may not be required; drop the
				 * whole current run (soundness) */
				runlen = 0;
				i++;
				break;
			case '(':
			case ')':
				/* group boundary: flush what we have as required */
				FLUSH_RUN(false);
				i++;
				break;
			case '[':
				/* character class: not a fixed literal -> flush and skip it */
				FLUSH_RUN(false);
				i++;
				if (i < relen && re[i] == '^')
					i++;
				if (i < relen && re[i] == ']')	/* literal ] as first char */
					i++;
				while (i < relen && re[i] != ']')
					i++;
				if (i < relen)
					i++;
				break;
			case '.':
				/* wildcard: breaks the run */
				FLUSH_RUN(false);
				i++;
				break;
			case '^':
			case '$':
				/* anchors: run boundary, no char consumed into the run */
				FLUSH_RUN(false);
				i++;
				break;
			default:
				if (runlen < (int) sizeof(run))
					run[runlen++] = c;
				i++;
				break;
		}
	}
	FLUSH_RUN(false);
	#undef FLUSH_RUN

	return n;
}
