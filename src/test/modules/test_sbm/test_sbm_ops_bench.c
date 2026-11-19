/*
 * test_sbm_ops_bench: time the sbm read paths sparsemap 5.9 reworked, so a
 * port can be measured before and after on the same data.
 *
 * Builds one map of `n` members drawn from [0, universe) in the given
 * pattern (0 uniform, 1 runs of 64, 2 clustered: 32 members in each of a
 * few random 4096-bit windows), then times, per operation, `nq` queries:
 *
 *	 contains		random probes, no cursor
 *	 contains_asc	ascending probes with one cursor
 *	 next_from		sbm_next_member from a random position
 *	 iterate		a full forward iteration (per member)
 *	 rank			sbm_rank(0, x) at a random x
 *	 select			sbm_select(n) at a random n < cardinality
 *	 prev_iterate	a full reverse iteration (per member)
 *	 loc_contains	sbm_locator_contains, locator built once (untimed)
 *	 loc_rank		sbm_locator_rank
 *	 loc_select		sbm_locator_select
 *	 is_subset		sbm_is_subset(m, m) (per call)
 *	 remove_range	sbm_remove_range over a span of 4096 bits of a copy
 *					(per call); before 5.9 this removes bit by bit
 *
 * Each operation is repeated `reps` times and the minimum ns per query is
 * reported, since timing noise only adds delay.  Operations whose cost is
 * O(chunks) per query before the 5.9 port (rank, select, the locator
 * queries, is_subset, remove_range) run nq / 100 queries, at least 20, so
 * a baseline run finishes.
 */

#include "postgres.h"

#include "common/int.h"
#include "common/pg_prng.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/sbm.h"
#include "portability/instr_time.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

PG_FUNCTION_INFO_V1(test_sbm_ops_bench);

static int
cmp_u64(const void *a, const void *b)
{
	return pg_cmp_u64(*(const uint64 *) a, *(const uint64 *) b);
}

/* Sorted, distinct members; see the header comment for the patterns. */
static uint64 *
make_members(int64 n, int64 universe, int pattern, pg_prng_state *rng,
			 int64 *n_out)
{
	int64		cap = n + 64;
	uint64	   *v = palloc_array(uint64, cap);
	int64		got = 0;

	while (got < n)
	{
		int64		m = 0;

		while (got < cap - 64)
		{
			uint64		s = pg_prng_uint64(rng) % (uint64) universe;

			if (pattern == 0)
				v[got++] = s;
			else if (pattern == 1)
				for (int k = 0; k < 64 && s + k < (uint64) universe; k++)
					v[got++] = s + k;
			else
				for (int k = 0; k < 32; k++)
					v[got++] = (s & ~(uint64) 4095) +
						pg_prng_uint64(rng) % 4096;
		}
		qsort(v, got, sizeof(uint64), cmp_u64);
		for (int64 i = 0; i < got; i++)
			if (m == 0 || v[i] != v[m - 1])
				v[m++] = v[i];
		got = m;
	}
	*n_out = got;
	return v;
}

static double
elapsed_ns(instr_time start)
{
	instr_time	now;

	INSTR_TIME_SET_CURRENT(now);
	INSTR_TIME_SUBTRACT(now, start);
	return (double) INSTR_TIME_GET_NANOSEC(now);
}

typedef enum BenchOp
{
	B_CONTAINS, B_CONTAINS_ASC, B_NEXT_FROM, B_ITERATE, B_RANK, B_SELECT,
	B_PREV_ITERATE, B_LOC_CONTAINS, B_LOC_RANK, B_LOC_SELECT, B_IS_SUBSET,
	B_REMOVE_RANGE, NUM_BENCH_OPS
} BenchOp;

static const char *const bench_name[NUM_BENCH_OPS] = {
	"contains", "contains_asc", "next_from", "iterate", "rank", "select",
	"prev_iterate", "loc_contains", "loc_rank", "loc_select", "is_subset",
	"remove_range",
};

/* Run one timed pass of `op`; returns ns per query. */
/* The members, for building fresh copies in the remove_range pass. */
static const uint64 *members_for_range;

static double
run_op(BenchOp op, Sbm *m, SbmLocator *loc, const uint64 *q, int64 nq,
	   const uint64 *sorted_q, size_t card, int64 universe,
	   volatile uint64 *sink)
{
	instr_time	t;
	double		ns;
	int64		per = nq;

	switch (op)
	{
		case B_RANK:
		case B_SELECT:
		case B_LOC_RANK:
		case B_LOC_SELECT:
			nq = nq / 100 > 20 ? nq / 100 : (nq < 20 ? nq : 20);
			per = nq;
			break;
		default:
			break;
	}
	INSTR_TIME_SET_CURRENT(t);
	switch (op)
	{
		case B_CONTAINS:
			for (int64 i = 0; i < nq; i++)
				*sink += sbm_contains(m, q[i], NULL);
			break;
		case B_CONTAINS_ASC:
			{
				SbmCursor	c = SBM_CURSOR_INIT;

				for (int64 i = 0; i < nq; i++)
					*sink += sbm_contains(m, sorted_q[i], &c);
				break;
			}
		case B_NEXT_FROM:
			for (int64 i = 0; i < nq; i++)
				*sink += sbm_next_member(m, q[i], NULL);
			break;
		case B_ITERATE:
			{
				SbmCursor	c = SBM_CURSOR_INIT;
				uint64		x = SBM_IDX_MAX;

				while ((x = sbm_next_member(m, x, &c)) != SBM_IDX_MAX)
					*sink += x;
				per = (int64) card;
				break;
			}
		case B_RANK:
			for (int64 i = 0; i < nq; i++)
				*sink += sbm_rank(m, 0, q[i], true);
			break;
		case B_SELECT:
			for (int64 i = 0; i < nq; i++)
				*sink += sbm_select(m, q[i] % card, true);
			break;
		case B_PREV_ITERATE:
			{
				SbmCursor	c = SBM_CURSOR_INIT;
				uint64		x = SBM_IDX_MAX;

				while ((x = sbm_prev_member(m, x, &c)) != SBM_IDX_MAX)
				{
					*sink += x;
					if (x == 0)
						break;
				}
				per = (int64) card;
				break;
			}
		case B_LOC_CONTAINS:
			for (int64 i = 0; i < nq; i++)
				*sink += sbm_locator_contains(loc, q[i]);
			break;
		case B_LOC_RANK:
			for (int64 i = 0; i < nq; i++)
				*sink += sbm_locator_rank(loc, 0, q[i], true);
			break;
		case B_LOC_SELECT:
			for (int64 i = 0; i < nq; i++)
				*sink += sbm_locator_select(loc, q[i] % card, true);
			break;
		case B_IS_SUBSET:
			per = nq < 5 ? nq : 5;
			for (int64 i = 0; i < per; i++)
				*sink += sbm_is_subset(m, m);
			break;
		case B_REMOVE_RANGE:
			{
				uint64		lo = q[0] % (uint64) universe;
				uint64		span = 4096;

				per = nq < 5 ? nq : 5;
				for (int64 i = 0; i < per; i++)
				{
					/*
					 * Copy into a buffer with room for every run to split in
					 * two, so the call measures the rewrite, not ENOSPC.
					 */
					size_t		sz = sbm_get_size(m) * 2 + 4096;
					Sbm		   *c = sbm_create(sz);

					if (!sbm_add_many(c, members_for_range, card))
						elog(ERROR, "copy for remove_range failed");
					*sink += sbm_remove_range(c, lo, lo + span);
					sbm_free(c);
				}
				break;
			}
		case NUM_BENCH_OPS:
			break;
	}
	ns = elapsed_ns(t);
	return ns / (double) (per > 0 ? per : 1);
}

Datum
test_sbm_ops_bench(PG_FUNCTION_ARGS)
{
	int64		n = PG_GETARG_INT64(0);
	int64		universe = PG_GETARG_INT64(1);
	int			pattern = PG_GETARG_INT32(2);
	int64		nq = PG_GETARG_INT64(3);
	int			reps = PG_GETARG_INT32(4);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pg_prng_state rng;
	uint64	   *members;
	uint64	   *q;
	uint64	   *sorted_q;
	int64		nm;
	Sbm		   *m = NULL;
	SbmLocator *loc;
	SbmStats	st;
	size_t		card;
	volatile uint64 sink = 0;

	if (n < 1 || universe < n || nq < 1 || reps < 1 || pattern < 0 ||
		pattern > 2)
		elog(ERROR, "invalid argument");
	InitMaterializedSRF(fcinfo, 0);

	pg_prng_seed(&rng, 4242);
	members = make_members(n, universe, pattern, &rng, &nm);
	if (!sbm_add_many_grow(&m, members, (size_t) nm))
		elog(ERROR, "sbm_add_many_grow failed");
	card = sbm_cardinality(m);
	members_for_range = members;
	q = palloc_array(uint64, nq);
	sorted_q = palloc_array(uint64, nq);
	for (int64 i = 0; i < nq; i++)
		q[i] = pg_prng_uint64(&rng) % (uint64) universe;
	memcpy(sorted_q, q, sizeof(uint64) * nq);
	qsort(sorted_q, nq, sizeof(uint64), cmp_u64);
	loc = sbm_locator_build(m);
	memset(&st, 0, sizeof(st));
	sbm_statistics(m, &st);

	for (int op = 0; op < NUM_BENCH_OPS; op++)
	{
		double		best = -1;
		Datum		vals[5];
		bool		nulls[5] = {false};

		for (int r = 0; r < reps; r++)
		{
			double		ns = run_op(op, m, loc, q, nq, sorted_q, card,
									universe, &sink);

			if (best < 0 || ns < best)
				best = ns;
		}
		vals[0] = CStringGetTextDatum(bench_name[op]);
		vals[1] = Float8GetDatum(best);
		vals[2] = Int64GetDatum((int64) card);
		vals[3] = Int64GetDatum((int64) st.chunks_total);
		vals[4] = Int64GetDatum((int64) sbm_get_size(m));
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, vals, nulls);
	}
	sbm_locator_free(loc);
	sbm_free(m);
	if (sink == 42)
		elog(NOTICE, "unlikely");
	return (Datum) 0;
}
