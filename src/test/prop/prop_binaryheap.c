/*-------------------------------------------------------------------------
 *
 * prop_binaryheap.c
 *	  Property-based tests for src/common/binaryheap.c.
 *
 * binaryheap is the shared binary heap used by merge append, the executor's
 * bounded sorts, and more.  A heap has a crisp model: a priority queue.  The
 * highest-value first test for any data structure is a model test -- drive
 * the structure and a trusted reference (here, a sorted array) through the
 * same operations and require they agree.  These tests do exactly that for
 * the add / first / remove_first / build operations.
 *
 * Tier A: in-process, no server needed.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_binaryheap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "binaryheap"
#include "pg_prop.h"

#include "lib/binaryheap.h"

/*
 * In frontend builds bh_node_type is void *; we store int64 values cast
 * through intptr_t.  A min-heap comparator: returns >0 when a should come out
 * before b (binaryheap is a max-heap on the comparator, so to pop ascending
 * we make "smaller is greater").
 */
static int
cmp_min(bh_node_type a, bh_node_type b, void *arg)
{
	int64		x = (int64) (intptr_t) a;
	int64		y = (int64) (intptr_t) b;

	/* max-heap pops the comparator-largest first; invert for ascending pops */
	if (x < y)
		return 1;
	if (x > y)
		return -1;
	return 0;
}

static int
cmp_i64(const void *a, const void *b)
{
	int64		x = *(const int64 *) a;
	int64		y = *(const int64 *) b;

	return (x > y) - (x < y);
}

/*
 * Draw n values into a caller-provided array, bounded so they fit in intptr_t
 * on any platform (use int32 range, widened to int64).
 */
static int
draw_values(pg_prop_tc *tc, int64 *vals, int maxn)
{
	int			n = (int) pg_prop_draw_int(tc, 0, maxn);

	for (int i = 0; i < n; i++)
		vals[i] = pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);
	return n;
}

PG_PROP(pop_order_is_sorted)
{
	int64		vals[256];
	int64		sorted[256];
	int			n = draw_values(tc, vals, 200);
	binaryheap *heap = binaryheap_allocate(n > 0 ? n : 1, cmp_min, NULL);

	/* add every value, then pop them all: the pops must come out ascending */
	for (int i = 0; i < n; i++)
		binaryheap_add(heap, (bh_node_type) (intptr_t) vals[i]);

	memcpy(sorted, vals, sizeof(int64) * n);
	qsort(sorted, n, sizeof(int64), cmp_i64);

	for (int i = 0; i < n; i++)
	{
		int64		got;

		if (binaryheap_empty(heap))
			pg_prop_fail(tc, "heap empty after %d of %d pops", i, n);
		got = (int64) (intptr_t) binaryheap_remove_first(heap);
		if (got != sorted[i])
			pg_prop_fail(tc, "pop %d: got %lld, expected %lld (n=%d)",
						 i, (long long) got, (long long) sorted[i], n);
	}
	if (!binaryheap_empty(heap))
		pg_prop_fail(tc, "heap not empty after %d pops", n);

	binaryheap_free(heap);
}

PG_PROP(first_is_minimum)
{
	int64		vals[256];
	int			n;
	binaryheap *heap;
	int64		expected_min = 0;

	/* need at least one element for binaryheap_first */
	n = (int) pg_prop_draw_int(tc, 1, 200);
	vals[0] = 0;				/* silence -Wmaybe-uninitialized; n >= 1 fills it */
	for (int i = 0; i < n; i++)
		vals[i] = pg_prop_draw_int(tc, INT32_MIN, INT32_MAX);

	heap = binaryheap_allocate(n, cmp_min, NULL);
	for (int i = 0; i < n; i++)
		binaryheap_add(heap, (bh_node_type) (intptr_t) vals[i]);

	expected_min = vals[0];
	for (int i = 1; i < n; i++)
		if (vals[i] < expected_min)
			expected_min = vals[i];

	{
		int64		first = (int64) (intptr_t) binaryheap_first(heap);

		if (first != expected_min)
			pg_prop_fail(tc, "first = %lld, expected min %lld (n=%d)",
						 (long long) first, (long long) expected_min, n);
	}
	binaryheap_free(heap);
}

PG_PROP(build_matches_incremental_add)
{
	int64		vals[256];
	int			n = draw_values(tc, vals, 200);
	binaryheap *h1 = binaryheap_allocate(n > 0 ? n : 1, cmp_min, NULL);
	binaryheap *h2 = binaryheap_allocate(n > 0 ? n : 1, cmp_min, NULL);

	/*
	 * Two ways to construct the same heap: incremental add (sift-up each), and
	 * add_unordered followed by binaryheap_build (Floyd's bulk heapify).  They
	 * must pop in the same (sorted) order.
	 */
	for (int i = 0; i < n; i++)
		binaryheap_add(h1, (bh_node_type) (intptr_t) vals[i]);

	for (int i = 0; i < n; i++)
		binaryheap_add_unordered(h2, (bh_node_type) (intptr_t) vals[i]);
	binaryheap_build(h2);

	for (int i = 0; i < n; i++)
	{
		int64		a = (int64) (intptr_t) binaryheap_remove_first(h1);
		int64		b = (int64) (intptr_t) binaryheap_remove_first(h2);

		if (a != b)
			pg_prop_fail(tc, "pop %d: incremental=%lld build=%lld (n=%d)",
						 i, (long long) a, (long long) b, n);
	}
	binaryheap_free(h1);
	binaryheap_free(h2);
}
