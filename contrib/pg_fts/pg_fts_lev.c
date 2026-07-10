/*-------------------------------------------------------------------------
 *
 * pg_fts_lev.c
 *		Levenshtein automaton over a sorted term dictionary.
 *
 * A fuzzy query `term~k` matches every dictionary term within edit distance k
 * of `term`.  Instead of over-generating candidates with a trigram funnel and
 * rechecking each at the heap, we intersect a Levenshtein automaton directly
 * with the (sorted) dictionary: this yields EXACTLY the matching terms and
 * their posting lists, with no false positives and no heap recheck.
 *
 * The automaton is the standard bounded dynamic-programming row.  For a query
 * q[0..m) and a candidate prefix consumed so far, state is the DP row
 *   row[j] = edit distance between q[0..j) and the consumed prefix,
 * clamped to k+1 ("infinity").  Feeding a character c advances the row:
 *   next[0]      = prev[0] + 1
 *   next[j]      = min( prev[j] + 1,           // delete q[j-1]... (insert c)
 *                       next[j-1] + 1,         // insert
 *                       prev[j-1] + (q[j-1]!=c ? 1 : 0) )  // match/substitute
 * A prefix is a DEAD END when min(row) > k -- no extension can match, so the
 * whole subtree (and, on a sorted dictionary, a contiguous range of terms) can
 * be skipped.  A term MATCHES when row[m] <= k.
 *
 * This is exactly Lucene's FuzzyQuery automaton idea, realized directly over
 * the sorted on-disk dictionary (which already has a per-page block index for
 * seeking) rather than a separate FST -- the sorted order is what an FST would
 * have given us for the ordered walk, and point lookups are already O(log P).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  contrib/pg_fts/pg_fts_lev.c
 *
 *-------------------------------------------------------------------------
 */

/* Maximum query length the byte-wise automaton handles; longer falls back. */
#define FTS_LEV_MAXQ 255

typedef struct FtsLevAut
{
	const unsigned char *q;		/* query bytes */
	int			m;				/* query length */
	int			k;				/* max edits */
} FtsLevAut;

/*
 * A DP row is m+1 int16 cells.  We keep rows on the caller's stack (small: a
 * term is <= a few hundred bytes).  row[j] in [0, k+1].
 */
#define FTS_LEV_INF (aut->k + 1)

static inline void
fts_lev_start(const FtsLevAut *aut, int16 *row)
{
	int			j;

	for (j = 0; j <= aut->m; j++)
		row[j] = (int16) Min(j, aut->k + 1);
}

/* Advance `prev` by consuming byte c into `next`; returns min(next) for pruning. */
static inline int
fts_lev_step(const FtsLevAut *aut, const int16 *prev, unsigned char c, int16 *next)
{
	int			j;
	int			rowmin;

	next[0] = (int16) Min(prev[0] + 1, aut->k + 1);
	rowmin = next[0];
	for (j = 1; j <= aut->m; j++)
	{
		int			sub = prev[j - 1] + (aut->q[j - 1] == c ? 0 : 1);
		int			ins = next[j - 1] + 1;
		int			del = prev[j] + 1;
		int			v = Min(sub, Min(ins, del));

		if (v > aut->k + 1)
			v = aut->k + 1;
		next[j] = (int16) v;
		if (v < rowmin)
			rowmin = v;
	}
	return rowmin;
}

/* Does `row` accept (query within k edits of the consumed prefix)? */
static inline bool
fts_lev_accept(const FtsLevAut *aut, const int16 *row)
{
	return row[aut->m] <= aut->k;
}

/*
 * Match one candidate term against the automaton from scratch (no shared
 * prefix state).  Returns true if within k edits.  O(cand_len * m) but bounded:
 * the length filter |cand-m| <= k is applied by the caller.  This is the simple
 * per-term entry point; the dictionary walk below reuses row state across the
 * shared prefix of consecutive sorted terms for speed, but this is the
 * correctness reference.
 */
/*
 * Like fts_lev_match_prefix below but the simple form; retained as the
 * correctness reference is fts_lev_match_prefix, which also reports the dead
 * prefix.  (Plain fts_lev_match removed -- all callers use the prefix form.)
 */

/*
 * Match one candidate term and report the DEAD-PREFIX length: the smallest i
 * such that cand[0..i) already has row-min > k (no string with that prefix can
 * match).  Returns the match result; *deadlen is set to that i, or candlen if
 * the automaton never died while consuming cand.  Used to skip, in one jump,
 * every sorted dictionary term sharing a dead prefix.
 */
static bool
fts_lev_match_prefix(const FtsLevAut *aut, const unsigned char *cand,
					 int candlen, int *deadlen)
{
	int16		rowa[FTS_LEV_MAXQ + 2];
	int16		rowb[FTS_LEV_MAXQ + 2];
	int16	   *cur = rowa;
	int16	   *nxt = rowb;
	int			i;

	*deadlen = candlen;
	fts_lev_start(aut, cur);
	for (i = 0; i < candlen; i++)
	{
		int			rmin = fts_lev_step(aut, cur, cand[i], nxt);
		int16	   *t = cur;

		cur = nxt;
		nxt = t;
		if (rmin > aut->k)
		{
			*deadlen = i + 1;	/* prefix cand[0..i] is a dead end */
			return false;
		}
	}
	return fts_lev_accept(aut, cur);
}
