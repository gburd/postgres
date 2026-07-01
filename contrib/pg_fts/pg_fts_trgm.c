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
 * used by the matcher; wiring a persistent on-disk trigram posting index into
 * the bm25 AM (the full three-tier funnel) is the remaining scale work.
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
