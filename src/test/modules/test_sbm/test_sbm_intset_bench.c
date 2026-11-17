/*
 * test_sbm_intset_bench: compare sbm with IntegerSet (lib/integerset.c) on
 * the job GiST VACUUM gives IntegerSet.
 *
 * gistvacuum.c adds block numbers in ascending order to two sets as it scans
 * the index (internal pages, empty leaf pages), then iterates the first in
 * order and probes the second with block numbers taken from downlinks, in
 * no particular order.  This function times those three phases for both
 * structures on the same members and reports bytes held, so the comparison
 * covers what the caller actually does.
 *
 * Members are `nmembers` block numbers drawn from [0, nblocks): either
 * uniformly at random (pattern 0), or in runs of `runlen` consecutive blocks
 * at random starts (pattern 1, clustered deletions).  `nprobes` probes are
 * drawn half from the members and half uniformly from [0, nblocks).
 *
 * The sbm side builds with sbm_add_grow_cursor (one cursor threaded through
 * the ascending adds) and iterates with sbm_next_member and a cursor.  It
 * probes two ways: sbm_contains_cached, the variant for unordered probes,
 * and sbm_locator_contains after a one-off sbm_locator_build, whose cost is
 * added to that probe phase.  Each phase
 * repeats `reps` times and the minimum is reported, since timing noise only
 * adds delay.
 */

#include "postgres.h"

#include "common/int.h"
#include "common/pg_prng.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/integerset.h"
#include "lib/sbm.h"
#include "portability/instr_time.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"

PG_FUNCTION_INFO_V1(test_sbm_intset_bench);

static int
cmp_u64(const void *a, const void *b)
{
	return pg_cmp_u64(*(const uint64 *) a, *(const uint64 *) b);
}

/* Sorted, distinct members following `pattern`. */
static uint64 *
make_members(int64 nmembers, int64 nblocks, int pattern, int64 runlen,
			 pg_prng_state *rng, int64 *n_out)
{
	int64		cap = nmembers + runlen;
	uint64	   *v = palloc_array(uint64, cap);
	int64		n = 0;

	/*
	 * Draw with replacement, then sort and deduplicate; repeat until there
	 * are enough distinct members.  Building through an sbm one random
	 * member at a time would be quadratic (each insert walks the chunks).
	 */
	while (n < nmembers)
	{
		int64		m;

		while (n < cap - runlen)
		{
			uint64		start = pg_prng_uint64(rng) % (uint64) nblocks;
			int64		len = (pattern == 0) ? 1 : runlen;

			for (int64 k = 0; k < len && start + k < (uint64) nblocks; k++)
				v[n++] = start + k;
		}
		qsort(v, n, sizeof(uint64), cmp_u64);
		m = 0;
		for (int64 i = 0; i < n; i++)
			if (m == 0 || v[i] != v[m - 1])
				v[m++] = v[i];
		n = m;
	}
	*n_out = n;
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

Datum
test_sbm_intset_bench(PG_FUNCTION_ARGS)
{
	int64		nmembers = PG_GETARG_INT64(0);
	int64		nblocks = PG_GETARG_INT64(1);
	int			pattern = PG_GETARG_INT32(2);
	int64		runlen = PG_GETARG_INT64(3);
	int64		nprobes = PG_GETARG_INT64(4);
	int			reps = PG_GETARG_INT32(5);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	pg_prng_state rng;
	uint64	   *members;
	uint64	   *probes;
	int64		n;
	double		best[3][3];
	int64		bytes[3] = {0, 0, 0};
	int64		used[3] = {0, 0, 0};
	volatile uint64 sink = 0;
	MemoryContext work;

	if (nmembers < 1 || nblocks < nmembers || runlen < 1 || nprobes < 1 ||
		reps < 1 || pattern < 0 || pattern > 1)
		elog(ERROR, "invalid argument");
	InitMaterializedSRF(fcinfo, 0);

	pg_prng_seed(&rng, 42);
	members = make_members(nmembers, nblocks, pattern, runlen, &rng, &n);
	probes = palloc_array(uint64, nprobes);
	for (int64 i = 0; i < nprobes; i++)
		probes[i] = (i % 2 == 0) ? members[pg_prng_uint64(&rng) % n]
			: pg_prng_uint64(&rng) % (uint64) nblocks;

	for (int s = 0; s < 3; s++)
		for (int p = 0; p < 3; p++)
			best[s][p] = -1;

	/*
	 * IntegerSet, in a generation context as gistvacuum.c uses, so its
	 * footprint is the context's allocation.
	 */
	work = GenerationContextCreate(CurrentMemoryContext, "intset bench",
								   16 * 1024, 16 * 1024, 16 * 1024);
	for (int r = 0; r < reps; r++)
	{
		MemoryContext old = MemoryContextSwitchTo(work);
		IntegerSet *is;
		instr_time	t;
		double		ns;
		uint64		x;

		INSTR_TIME_SET_CURRENT(t);
		is = intset_create();
		for (int64 i = 0; i < n; i++)
			intset_add_member(is, members[i]);
		ns = elapsed_ns(t);
		if (best[0][0] < 0 || ns < best[0][0])
			best[0][0] = ns;

		INSTR_TIME_SET_CURRENT(t);
		intset_begin_iterate(is);
		while (intset_iterate_next(is, &x))
			sink += x;
		ns = elapsed_ns(t);
		if (best[0][1] < 0 || ns < best[0][1])
			best[0][1] = ns;

		INSTR_TIME_SET_CURRENT(t);
		for (int64 i = 0; i < nprobes; i++)
			sink += intset_is_member(is, probes[i]);
		ns = elapsed_ns(t);
		if (best[0][2] < 0 || ns < best[0][2])
			best[0][2] = ns;

		bytes[0] = (int64) MemoryContextMemAllocated(work, true);
		used[0] = (int64) intset_memory_usage(is);
		MemoryContextSwitchTo(old);
		MemoryContextReset(work);
	}

	/* sbm, palloc'd in the same kind of context. */
	for (int r = 0; r < reps; r++)
	{
		MemoryContext old = MemoryContextSwitchTo(work);
		Sbm		   *m = NULL;
		SbmCursor	cur = SBM_CURSOR_INIT;
		SbmCursorCached cache = SBM_CURSOR_CACHED_INIT;
		instr_time	t;
		double		ns;
		uint64		x;

		INSTR_TIME_SET_CURRENT(t);
		for (int64 i = 0; i < n; i++)
			sbm_add_grow_cursor(&m, members[i], &cur);
		ns = elapsed_ns(t);
		if (best[1][0] < 0 || ns < best[1][0])
			best[1][0] = ns;

		INSTR_TIME_SET_CURRENT(t);
		cur = (SbmCursor) SBM_CURSOR_INIT;
		x = SBM_IDX_MAX;
		while ((x = sbm_next_member(m, x, &cur)) != SBM_IDX_MAX)
			sink += x;
		ns = elapsed_ns(t);
		if (best[1][1] < 0 || ns < best[1][1])
			best[1][1] = ns;

		INSTR_TIME_SET_CURRENT(t);
		for (int64 i = 0; i < nprobes; i++)
			sink += sbm_contains_cached(m, probes[i], &cache);
		ns = elapsed_ns(t);
		if (best[1][2] < 0 || ns < best[1][2])
			best[1][2] = ns;

		bytes[1] = (int64) MemoryContextMemAllocated(work, true);
		used[1] = (int64) sbm_get_size(m);

		/* Locator probes: build the directory once, then probe. */
		{
			SbmLocator *loc;

			INSTR_TIME_SET_CURRENT(t);
			loc = sbm_locator_build(m);
			for (int64 i = 0; i < nprobes; i++)
				sink += sbm_locator_contains(loc, probes[i]);
			ns = elapsed_ns(t);
			if (best[2][2] < 0 || ns < best[2][2])
				best[2][2] = ns;
			best[2][0] = best[1][0];
			best[2][1] = best[1][1];
			bytes[2] = (int64) MemoryContextMemAllocated(work, true);
			used[2] = used[1];
			sbm_locator_free(loc);
		}
		if ((int64) sbm_cardinality(m) != n)
			elog(ERROR, "sbm holds %zu members, expected " INT64_FORMAT,
				 sbm_cardinality(m), n);
		MemoryContextSwitchTo(old);
		MemoryContextReset(work);
	}
	MemoryContextDelete(work);

	for (int s = 0; s < 3; s++)
	{
		static const char *const names[3] = {"intset", "sbm", "sbm+locator"};
		Datum		vals[7];
		bool		nulls[7] = {false};

		vals[0] = CStringGetTextDatum(names[s]);
		vals[1] = Int64GetDatum(n);
		vals[2] = Float8GetDatum(best[s][0] / (double) n);
		vals[3] = Float8GetDatum(best[s][1] / (double) n);
		vals[4] = Float8GetDatum(best[s][2] / (double) nprobes);
		vals[5] = Int64GetDatum(bytes[s]);
		vals[6] = Int64GetDatum(used[s]);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, vals, nulls);
	}
	if (sink == 42)
		elog(NOTICE, "unlikely");
	return (Datum) 0;
}
