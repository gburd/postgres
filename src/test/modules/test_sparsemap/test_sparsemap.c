/*--------------------------------------------------------------------------
 *
 * test_sparsemap.c
 *		Test module for compressed sparse bitmap (sparsemap).
 *
 * Copyright (c) 2024-2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_sparsemap/test_sparsemap.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/pg_prng.h"
#include "fmgr.h"
#include "lib/sparsemap.h"

PG_MODULE_MAGIC;

/* Convenient macros to test results */
#define EXPECT_TRUE(expr)	\
	do { \
		if (!(expr)) \
			elog(ERROR, \
				 "%s was unexpectedly false in file \"%s\" line %u", \
				 #expr, __FILE__, __LINE__); \
	} while (0)

#define EXPECT_FALSE(expr)	\
	do { \
		if (expr) \
			elog(ERROR, \
				 "%s was unexpectedly true in file \"%s\" line %u", \
				 #expr, __FILE__, __LINE__); \
	} while (0)

#define EXPECT_EQ_U64(result_expr, expected_expr)	\
	do { \
		uint64		_result = (result_expr); \
		uint64		_expected = (expected_expr); \
		if (_result != _expected) \
			elog(ERROR, \
				 "%s yielded %" PRIu64 ", expected %" PRIu64 " (%s) in file \"%s\" line %u", \
				 #result_expr, _result, _expected, #expected_expr, __FILE__, __LINE__); \
	} while (0)

#define EXPECT_EQ_SZ(result_expr, expected_expr)	\
	do { \
		size_t		_result = (result_expr); \
		size_t		_expected = (expected_expr); \
		if (_result != _expected) \
			elog(ERROR, \
				 "%s yielded %zu, expected %zu (%s) in file \"%s\" line %u", \
				 #result_expr, _result, _expected, #expected_expr, __FILE__, __LINE__); \
	} while (0)

/* -------------------------------------------------------------------
 * Helper: populate a map with consecutive set bits (creates RLE runs)
 * ------------------------------------------------------------------- */
static size_t
populate_map_rle(sparsemap_t * map, uint64 start, size_t count)
{
	size_t		i;

	for (i = 0; i < count; i++)
		sparsemap_add(map, start + i);
	return i;
}

/* -------------------------------------------------------------------
 * Test: lifecycle (create, init, open, copy, clear, free)
 * ------------------------------------------------------------------- */
static void
test_lifecycle(void)
{
	sparsemap_t *map;
	sparsemap_t *copy;
	sparsemap_t local;
	uint8	   *buf;

	elog(NOTICE, "testing lifecycle operations");

	/* create and free */
	map = sparsemap_create(1024);
	EXPECT_TRUE(map != NULL);
	EXPECT_EQ_SZ(sparsemap_get_capacity(map), 1024);
	/* empty-map size equals the per-chunk offset width (__sm_idx_t == uint64) */
	EXPECT_EQ_SZ(sparsemap_get_size(map), sizeof(uint64));
	sparsemap_free(map);

	/* init with caller-provided buffer */
	buf = palloc0(1024);
	sparsemap_init(&local, buf, 1024);
	EXPECT_EQ_SZ(sparsemap_get_capacity(&local), 1024);
	EXPECT_EQ_SZ(sparsemap_get_size(&local), sizeof(uint64));

	/* add some data, then clear */
	sparsemap_add(&local, 42);
	EXPECT_TRUE(sparsemap_contains(&local, 42));
	sparsemap_clear(&local);
	EXPECT_FALSE(sparsemap_contains(&local, 42));
	EXPECT_EQ_SZ(sparsemap_cardinality(&local), 0);

	/* populate, then open a second view */
	for (int i = 0; i < 100; i++)
		sparsemap_add(&local, i);
	{
		sparsemap_t view;

		sparsemap_open(&view, buf, 1024);
		for (int i = 0; i < 100; i++)
			EXPECT_TRUE(sparsemap_contains(&view, i));
	}

	/* copy */
	copy = sparsemap_copy(&local);
	EXPECT_TRUE(copy != NULL);
	EXPECT_EQ_SZ(sparsemap_cardinality(copy), sparsemap_cardinality(&local));
	for (int i = 0; i < 100; i++)
		EXPECT_TRUE(sparsemap_contains(copy, i));
	sparsemap_free(copy);

	pfree(buf);
}

/* -------------------------------------------------------------------
 * Test: capacity and resize
 * ------------------------------------------------------------------- */
static void
test_capacity(void)
{
	sparsemap_t *map;

	elog(NOTICE, "testing capacity and resize");

	map = sparsemap_create(1024);
	EXPECT_EQ_SZ(sparsemap_get_capacity(map), 1024);

	/* resize up */
	map = sparsemap_set_data_size(map, NULL, 2048);
	EXPECT_EQ_SZ(sparsemap_get_capacity(map), 2048);

	/* data survives resize */
	sparsemap_add(map, 42);
	EXPECT_TRUE(sparsemap_contains(map, 42));
	map = sparsemap_set_data_size(map, NULL, 4096);
	EXPECT_TRUE(sparsemap_contains(map, 42));

	/* capacity_remaining decreases as we fill */
	{
		double		cap_before;
		double		cap_after;

		cap_before = sparsemap_capacity_remaining(map);
		for (int i = 0; i < 50; i++)
			sparsemap_add(map, i * 3);	/* sparse pattern to use more space */
		cap_after = sparsemap_capacity_remaining(map);
		EXPECT_TRUE(cap_after < cap_before);
	}

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: single-bit operations (add, remove, contains, assign)
 * ------------------------------------------------------------------- */
static void
test_single_bit(void)
{
	sparsemap_t *map;

	elog(NOTICE, "testing single-bit operations");

	map = sparsemap_create(2048);

	/* empty map has nothing */
	EXPECT_FALSE(sparsemap_contains(map, 0));
	EXPECT_FALSE(sparsemap_contains(map, 1));
	EXPECT_FALSE(sparsemap_contains(map, 8192));

	/* add and verify */
	sparsemap_add(map, 1);
	sparsemap_add(map, 8192);
	EXPECT_TRUE(sparsemap_contains(map, 1));
	EXPECT_TRUE(sparsemap_contains(map, 8192));
	EXPECT_FALSE(sparsemap_contains(map, 0));
	EXPECT_FALSE(sparsemap_contains(map, 2));

	/* remove and verify */
	sparsemap_remove(map, 1);
	sparsemap_remove(map, 8192);
	EXPECT_FALSE(sparsemap_contains(map, 1));
	EXPECT_FALSE(sparsemap_contains(map, 8192));

	/* assign true then false */
	sparsemap_assign(map, 500, true);
	EXPECT_TRUE(sparsemap_contains(map, 500));
	sparsemap_assign(map, 500, false);
	EXPECT_FALSE(sparsemap_contains(map, 500));

	/* add is idempotent */
	sparsemap_add(map, 42);
	sparsemap_add(map, 42);
	EXPECT_TRUE(sparsemap_contains(map, 42));
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 1);

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: aggregate queries (cardinality, minimum, maximum, fill_factor)
 * ------------------------------------------------------------------- */
static void
test_aggregates(void)
{
	sparsemap_t *map;

	elog(NOTICE, "testing aggregate queries");

	map = sparsemap_create(4096);

	/* empty map: minimum/maximum return 0 when no chunks exist */
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 0);
	EXPECT_EQ_U64(sparsemap_minimum(map), 0);
	EXPECT_EQ_U64(sparsemap_maximum(map), 0);

	/* single element */
	sparsemap_add(map, 42);
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 1);
	EXPECT_EQ_U64(sparsemap_minimum(map), 42);
	EXPECT_EQ_U64(sparsemap_maximum(map), 42);

	/* more elements */
	sparsemap_add(map, 10);
	sparsemap_add(map, 8675309);
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 3);
	EXPECT_EQ_U64(sparsemap_minimum(map), 10);
	EXPECT_EQ_U64(sparsemap_maximum(map), 8675309);

	/* clear and recount */
	sparsemap_clear(map);
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 0);

	/* consecutive range */
	for (int i = 0; i < 512; i++)
		sparsemap_add(map, i + 13);
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 512);
	EXPECT_EQ_U64(sparsemap_minimum(map), 13);
	EXPECT_EQ_U64(sparsemap_maximum(map), 524);

	/* fill factor for dense range should be close to 1.0 */
	{
		double		ff;

		sparsemap_clear(map);
		for (int i = 0; i < 100; i++)
			sparsemap_add(map, i);
		ff = sparsemap_fill_factor(map);
		EXPECT_TRUE(ff > 0.5);
	}

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: minimum with rolling window
 * ------------------------------------------------------------------- */
static void
test_minimum_rolling(void)
{
	sparsemap_t *map;

	elog(NOTICE, "testing minimum with rolling window");

	map = sparsemap_create(10 * 1024);

	for (uint64 i = 0; i < 10 * 2048; i++)
	{
		sparsemap_add(map, i);
		if (i > 2047)
		{
			sparsemap_remove(map, i - 2048);
			EXPECT_EQ_U64(sparsemap_minimum(map), i - 2047);
		}
	}

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: rank and select
 * ------------------------------------------------------------------- */
static void
test_rank_select(void)
{
	sparsemap_t *map;
	uint8	   *buf;

	elog(NOTICE, "testing rank and select");

	buf = palloc0(4096);
	map = palloc(sizeof(sparsemap_t));
	sparsemap_init(map, buf, 4096);

	/* Insert known bits: 10, 20, 30, 40, 50 */
	sparsemap_add(map, 10);
	sparsemap_add(map, 20);
	sparsemap_add(map, 30);
	sparsemap_add(map, 40);
	sparsemap_add(map, 50);

	/* rank: count of set bits in range */
	EXPECT_EQ_SZ(sparsemap_rank(map, 0, 9, true), 0);
	EXPECT_EQ_SZ(sparsemap_rank(map, 0, 10, true), 1);
	EXPECT_EQ_SZ(sparsemap_rank(map, 0, 50, true), 5);
	EXPECT_EQ_SZ(sparsemap_rank(map, 10, 50, true), 5);
	EXPECT_EQ_SZ(sparsemap_rank(map, 11, 49, true), 3);

	/* select: position of n'th set bit (0-based) */
	EXPECT_EQ_U64(sparsemap_select(map, 0, true), 10);
	EXPECT_EQ_U64(sparsemap_select(map, 1, true), 20);
	EXPECT_EQ_U64(sparsemap_select(map, 4, true), 50);
	EXPECT_EQ_U64(sparsemap_select(map, 5, true), SPARSEMAP_IDX_MAX);

	/* select false: position of n'th unset bit */
	EXPECT_EQ_U64(sparsemap_select(map, 0, false), 0);
	EXPECT_EQ_U64(sparsemap_select(map, 10, false), 11);	/* 0..9 are unset, then
															 * 10 is set, 11 is 11th
															 * unset */

	pfree(map);
	pfree(buf);
}

/* -------------------------------------------------------------------
 * Test: span (find contiguous run)
 * ------------------------------------------------------------------- */
static void
test_span(void)
{
	sparsemap_t *map;

	elog(NOTICE, "testing span");

	map = sparsemap_create(4096);

	/* consecutive run 0..99 */
	for (int i = 0; i < 100; i++)
		sparsemap_add(map, i);

	/* find span of 10 set bits starting from 0 */
	EXPECT_EQ_U64(sparsemap_span(map, 0, 10, true), 0);
	/* find span starting from 50 */
	EXPECT_EQ_U64(sparsemap_span(map, 50, 10, true), 50);
	/* span too long */
	EXPECT_EQ_U64(sparsemap_span(map, 0, 101, true), SPARSEMAP_IDX_MAX);

	/* span of unset bits after the run */
	EXPECT_EQ_U64(sparsemap_span(map, 0, 10, false), 100);

	/* gap in middle: bits 0..49, 60..99 */
	sparsemap_clear(map);
	for (int i = 0; i < 50; i++)
		sparsemap_add(map, i);
	for (int i = 60; i < 100; i++)
		sparsemap_add(map, i);
	/* 10-bit unset span starting from 0: gap is at 50..59 */
	EXPECT_EQ_U64(sparsemap_span(map, 0, 10, false), 50);
	/* 11-bit unset span: gap 50..59 is only 10 bits, so next span at 100+ */
	EXPECT_EQ_U64(sparsemap_span(map, 0, 11, false), 100);

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: scan (iterate set bits)
 * ------------------------------------------------------------------- */
static size_t scan_count;
static uint64 scan_last_idx;

static void
scan_counter(uint64 v[], size_t n, void *aux)
{
	(void) aux;
	for (size_t i = 0; i < n; i++)
	{
		scan_count++;
		scan_last_idx = v[i];
	}
}

static void
test_scan(void)
{
	sparsemap_t *map;

	elog(NOTICE, "testing scan");

	map = sparsemap_create(4096);

	/* populate 0..99 */
	for (int i = 0; i < 100; i++)
		sparsemap_add(map, i);

	/* scan all */
	scan_count = 0;
	scan_last_idx = 0;
	sparsemap_scan(map, scan_counter, 0, NULL);
	EXPECT_EQ_SZ(scan_count, 100);
	EXPECT_EQ_U64(scan_last_idx, 99);

	/* scan with skip */
	scan_count = 0;
	scan_last_idx = 0;
	sparsemap_scan(map, scan_counter, 50, NULL);
	EXPECT_EQ_SZ(scan_count, 50);
	EXPECT_EQ_U64(scan_last_idx, 99);

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: RLE encoding (long consecutive runs)
 * ------------------------------------------------------------------- */
static void
test_rle(void)
{
	sparsemap_t *map;

	elog(NOTICE, "testing RLE encoding");

	map = sparsemap_create(8192);

	/* insert 5000 consecutive bits - should trigger RLE encoding */
	populate_map_rle(map, 0, 5000);
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 5000);
	EXPECT_EQ_U64(sparsemap_minimum(map), 0);
	EXPECT_EQ_U64(sparsemap_maximum(map), 4999);

	/* verify all bits are set */
	for (int i = 0; i < 5000; i++)
		EXPECT_TRUE(sparsemap_contains(map, i));
	EXPECT_FALSE(sparsemap_contains(map, 5000));

	/* RLE select: position of n'th set bit in consecutive run */
	EXPECT_EQ_U64(sparsemap_select(map, 0, true), 0);
	EXPECT_EQ_U64(sparsemap_select(map, 500, true), 500);
	EXPECT_EQ_U64(sparsemap_select(map, 4999, true), 4999);
	EXPECT_EQ_U64(sparsemap_select(map, 5000, true), SPARSEMAP_IDX_MAX);

	/* RLE select false: first unset bit is at 5000 */
	EXPECT_EQ_U64(sparsemap_select(map, 0, false), 5000);
	EXPECT_EQ_U64(sparsemap_select(map, 1, false), 5001);

	/* scan with skip on RLE */
	scan_count = 0;
	scan_last_idx = 0;
	sparsemap_scan(map, scan_counter, 4000, NULL);
	EXPECT_EQ_SZ(scan_count, 1000);
	EXPECT_EQ_U64(scan_last_idx, 4999);

	/* clear a bit in the middle of an RLE run */
	sparsemap_remove(map, 2500);
	EXPECT_FALSE(sparsemap_contains(map, 2500));
	EXPECT_TRUE(sparsemap_contains(map, 2499));
	EXPECT_TRUE(sparsemap_contains(map, 2501));
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 4999);

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: RLE edge cases
 * ------------------------------------------------------------------- */
static void
test_rle_edge_cases(void)
{
	sparsemap_t *map;

	elog(NOTICE, "testing RLE edge cases");

	map = sparsemap_create(32768);

	/* exact chunk boundary: 2048 consecutive bits */
	populate_map_rle(map, 0, 2048);
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 2048);
	EXPECT_EQ_U64(sparsemap_minimum(map), 0);
	EXPECT_EQ_U64(sparsemap_maximum(map), 2047);
	sparsemap_clear(map);

	/* cross chunk boundary: 0..2048 (one past boundary) */
	populate_map_rle(map, 0, 2049);
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 2049);
	EXPECT_TRUE(sparsemap_contains(map, 2048));
	sparsemap_clear(map);

	/* non-zero start crossing chunk boundary */
	populate_map_rle(map, 2000, 100);
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 100);
	EXPECT_EQ_U64(sparsemap_minimum(map), 2000);
	EXPECT_EQ_U64(sparsemap_maximum(map), 2099);
	EXPECT_TRUE(sparsemap_contains(map, 2047));
	EXPECT_TRUE(sparsemap_contains(map, 2048));
	sparsemap_clear(map);

	/* multiple disjoint RLE ranges */
	populate_map_rle(map, 0, 1000);
	populate_map_rle(map, 5000, 1000);
	populate_map_rle(map, 10000, 1000);
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 3000);
	EXPECT_FALSE(sparsemap_contains(map, 1000));
	EXPECT_FALSE(sparsemap_contains(map, 4999));
	EXPECT_TRUE(sparsemap_contains(map, 5000));

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: union (OR)
 * ------------------------------------------------------------------- */
static void
test_union(void)
{
	sparsemap_t *a;
	sparsemap_t *b;
	sparsemap_t *result;

	elog(NOTICE, "testing union");

	a = sparsemap_create(4096);
	b = sparsemap_create(4096);

	/* disjoint sets */
	sparsemap_add(a, 10);
	sparsemap_add(a, 20);
	sparsemap_add(b, 30);
	sparsemap_add(b, 40);
	result = sparsemap_union(a, b);
	EXPECT_TRUE(result != NULL);
	EXPECT_EQ_SZ(sparsemap_cardinality(result), 4);
	EXPECT_TRUE(sparsemap_contains(result, 10));
	EXPECT_TRUE(sparsemap_contains(result, 20));
	EXPECT_TRUE(sparsemap_contains(result, 30));
	EXPECT_TRUE(sparsemap_contains(result, 40));
	sparsemap_free(result);

	/* overlapping sets */
	sparsemap_clear(a);
	sparsemap_clear(b);
	sparsemap_add(a, 1);
	sparsemap_add(a, 2);
	sparsemap_add(b, 2);
	sparsemap_add(b, 3);
	result = sparsemap_union(a, b);
	EXPECT_TRUE(result != NULL);
	EXPECT_EQ_SZ(sparsemap_cardinality(result), 3);
	EXPECT_TRUE(sparsemap_contains(result, 1));
	EXPECT_TRUE(sparsemap_contains(result, 2));
	EXPECT_TRUE(sparsemap_contains(result, 3));
	sparsemap_free(result);

	/* cross-chunk union */
	sparsemap_clear(a);
	sparsemap_clear(b);
	sparsemap_add(a, 0);
	sparsemap_add(a, 2048);
	sparsemap_add(a, 8193);
	for (int i = 2049; i < 4096; i++)
		sparsemap_add(b, i);
	result = sparsemap_union(a, b);
	EXPECT_TRUE(result != NULL);
	EXPECT_TRUE(sparsemap_contains(result, 0));
	EXPECT_TRUE(sparsemap_contains(result, 2048));
	EXPECT_TRUE(sparsemap_contains(result, 8193));
	for (int i = 2049; i < 4096; i++)
		EXPECT_TRUE(sparsemap_contains(result, i));
	sparsemap_free(result);

	sparsemap_free(a);
	sparsemap_free(b);
}

/* -------------------------------------------------------------------
 * Test: intersection (AND)
 * ------------------------------------------------------------------- */
static void
test_intersection(void)
{
	sparsemap_t *a;
	sparsemap_t *b;
	sparsemap_t *result;

	elog(NOTICE, "testing intersection");

	a = sparsemap_create(4096);
	b = sparsemap_create(4096);

	/* disjoint sets */
	sparsemap_add(a, 10);
	sparsemap_add(a, 20);
	sparsemap_add(b, 30);
	sparsemap_add(b, 40);
	result = sparsemap_intersection(a, b);
	if (result != NULL)
	{
		EXPECT_EQ_SZ(sparsemap_cardinality(result), 0);
		sparsemap_free(result);
	}

	/* overlapping sets */
	sparsemap_clear(a);
	sparsemap_clear(b);
	for (int i = 0; i < 100; i++)
		sparsemap_add(a, i);
	for (int i = 50; i < 150; i++)
		sparsemap_add(b, i);
	result = sparsemap_intersection(a, b);
	EXPECT_TRUE(result != NULL);
	EXPECT_EQ_SZ(sparsemap_cardinality(result), 50);
	for (int i = 50; i < 100; i++)
		EXPECT_TRUE(sparsemap_contains(result, i));
	EXPECT_FALSE(sparsemap_contains(result, 49));
	EXPECT_FALSE(sparsemap_contains(result, 100));
	sparsemap_free(result);

	sparsemap_free(a);
	sparsemap_free(b);
}

/* -------------------------------------------------------------------
 * Test: difference (AND NOT)
 * ------------------------------------------------------------------- */
static void
test_difference(void)
{
	sparsemap_t *a;
	sparsemap_t *b;
	sparsemap_t *result;

	elog(NOTICE, "testing difference");

	a = sparsemap_create(4096);
	b = sparsemap_create(4096);

	for (int i = 0; i < 100; i++)
		sparsemap_add(a, i);
	for (int i = 50; i < 150; i++)
		sparsemap_add(b, i);

	result = sparsemap_difference(a, b);
	EXPECT_TRUE(result != NULL);
	EXPECT_EQ_SZ(sparsemap_cardinality(result), 50);
	for (int i = 0; i < 50; i++)
		EXPECT_TRUE(sparsemap_contains(result, i));
	for (int i = 50; i < 100; i++)
		EXPECT_FALSE(sparsemap_contains(result, i));
	sparsemap_free(result);

	sparsemap_free(a);
	sparsemap_free(b);
}

/* -------------------------------------------------------------------
 * Test: split
 * ------------------------------------------------------------------- */
static void
test_split(void)
{
	sparsemap_t *map;
	sparsemap_t portion;
	uint8		buf[4096];

	elog(NOTICE, "testing split");

	map = sparsemap_create(10 * 1024);
	memset(buf, 0, sizeof(buf));
	sparsemap_init(&portion, buf, sizeof(buf));

	/* insert 0..99, split at 50 */
	for (uint64 i = 0; i < 100; i++)
		sparsemap_add(map, i);

	sparsemap_split(map, 50, &portion);

	/* map should have 0..49, portion should have 50..99 */
	EXPECT_EQ_SZ(sparsemap_cardinality(map), 50);
	EXPECT_EQ_SZ(sparsemap_cardinality(&portion), 50);
	for (uint64 i = 0; i < 50; i++)
	{
		EXPECT_TRUE(sparsemap_contains(map, i));
		EXPECT_FALSE(sparsemap_contains(&portion, i));
	}
	for (uint64 i = 50; i < 100; i++)
	{
		EXPECT_FALSE(sparsemap_contains(map, i));
		EXPECT_TRUE(sparsemap_contains(&portion, i));
	}

	/* reunion via union should give the original */
	{
		sparsemap_t *merged = sparsemap_union(map, &portion);

		EXPECT_TRUE(merged != NULL);
		EXPECT_EQ_SZ(sparsemap_cardinality(merged), 100);
		for (uint64 i = 0; i < 100; i++)
			EXPECT_TRUE(sparsemap_contains(merged, i));
		sparsemap_free(merged);
	}

	/* split at SPARSEMAP_IDX_MAX */
	sparsemap_clear(map);
	sparsemap_clear(&portion);
	for (uint64 i = 0; i < 13; i++)
		sparsemap_add(map, i + 24);

	{
		uint64		offset;

		offset = sparsemap_split(map, SPARSEMAP_IDX_MAX, &portion);
		EXPECT_TRUE(sparsemap_maximum(map) < offset);
		EXPECT_TRUE(sparsemap_minimum(&portion) >= offset);
		EXPECT_TRUE(sparsemap_cardinality(map) + sparsemap_cardinality(&portion) == 13);
	}

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: offset (shift all bits)
 * ------------------------------------------------------------------- */
static void
test_offset(void)
{
	sparsemap_t *map;
	sparsemap_t *shifted;

	elog(NOTICE, "testing offset");

	map = sparsemap_create(10 * 1024);

	sparsemap_add(map, 10);
	sparsemap_add(map, 20);
	sparsemap_add(map, 30);

	/* offset == 0 returns a copy */
	shifted = sparsemap_offset(map, 0);
	EXPECT_TRUE(shifted != NULL);
	EXPECT_TRUE(sparsemap_contains(shifted, 10));
	EXPECT_TRUE(sparsemap_contains(shifted, 20));
	EXPECT_TRUE(sparsemap_contains(shifted, 30));
	EXPECT_EQ_SZ(sparsemap_cardinality(shifted), 3);
	sparsemap_free(shifted);

	/* positive offset */
	shifted = sparsemap_offset(map, 100);
	EXPECT_TRUE(shifted != NULL);
	EXPECT_FALSE(sparsemap_contains(shifted, 10));
	EXPECT_TRUE(sparsemap_contains(shifted, 110));
	EXPECT_TRUE(sparsemap_contains(shifted, 120));
	EXPECT_TRUE(sparsemap_contains(shifted, 130));
	EXPECT_EQ_SZ(sparsemap_cardinality(shifted), 3);
	sparsemap_free(shifted);

	/* negative offset, no bits dropped */
	shifted = sparsemap_offset(map, -5);
	EXPECT_TRUE(shifted != NULL);
	EXPECT_TRUE(sparsemap_contains(shifted, 5));
	EXPECT_TRUE(sparsemap_contains(shifted, 15));
	EXPECT_TRUE(sparsemap_contains(shifted, 25));
	EXPECT_EQ_SZ(sparsemap_cardinality(shifted), 3);
	sparsemap_free(shifted);

	/* negative offset, some bits dropped */
	shifted = sparsemap_offset(map, -15);
	EXPECT_TRUE(shifted != NULL);
	EXPECT_TRUE(sparsemap_contains(shifted, 5));	/* 20-15 */
	EXPECT_TRUE(sparsemap_contains(shifted, 15));	/* 30-15 */
	EXPECT_EQ_SZ(sparsemap_cardinality(shifted), 2);
	sparsemap_free(shifted);

	/* negative offset, all bits dropped */
	shifted = sparsemap_offset(map, -100);
	EXPECT_TRUE(shifted == NULL);

	/* empty map */
	sparsemap_clear(map);
	shifted = sparsemap_offset(map, 10);
	EXPECT_TRUE(shifted == NULL);

	/* large positive offset */
	sparsemap_add(map, 0);
	sparsemap_add(map, 1);
	sparsemap_add(map, 2);
	shifted = sparsemap_offset(map, 10000);
	EXPECT_TRUE(shifted != NULL);
	EXPECT_TRUE(sparsemap_contains(shifted, 10000));
	EXPECT_TRUE(sparsemap_contains(shifted, 10001));
	EXPECT_TRUE(sparsemap_contains(shifted, 10002));
	EXPECT_EQ_SZ(sparsemap_cardinality(shifted), 3);
	sparsemap_free(shifted);

	/* RLE range with positive offset */
	sparsemap_clear(map);
	for (int i = 0; i < 5000; i++)
		sparsemap_add(map, i);
	shifted = sparsemap_offset(map, 64);
	EXPECT_TRUE(shifted != NULL);
	EXPECT_FALSE(sparsemap_contains(shifted, 63));
	EXPECT_TRUE(sparsemap_contains(shifted, 64));
	EXPECT_TRUE(sparsemap_contains(shifted, 5063));
	EXPECT_FALSE(sparsemap_contains(shifted, 5064));
	EXPECT_EQ_SZ(sparsemap_cardinality(shifted), 5000);
	sparsemap_free(shifted);

	/* chunk boundary cross: bit 2047 shifted by +1 = bit 2048 */
	sparsemap_clear(map);
	sparsemap_add(map, 2047);
	shifted = sparsemap_offset(map, 1);
	EXPECT_TRUE(shifted != NULL);
	EXPECT_FALSE(sparsemap_contains(shifted, 2047));
	EXPECT_TRUE(sparsemap_contains(shifted, 2048));
	EXPECT_EQ_SZ(sparsemap_cardinality(shifted), 1);
	sparsemap_free(shifted);

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: get_data returns the raw buffer
 * ------------------------------------------------------------------- */
static void
test_get_data(void)
{
	sparsemap_t *map;

	elog(NOTICE, "testing get_data");

	map = sparsemap_create(1024);
	sparsemap_add(map, 42);
	EXPECT_TRUE(sparsemap_get_data(map) != NULL);
	EXPECT_EQ_SZ(sparsemap_get_capacity(map), 1024);
	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Test: multi-chunk sparse pattern
 * ------------------------------------------------------------------- */
static void
test_sparse_pattern(void)
{
	sparsemap_t *map;
	pg_prng_state state;

	elog(NOTICE, "testing sparse multi-chunk pattern");

	map = sparsemap_create(8192);
	pg_prng_seed(&state, 12345);

	/* insert 200 random bits across a wide range */
	for (int i = 0; i < 200; i++)
	{
		uint64		idx = pg_prng_uint64_range(&state, 0, 50000);

		sparsemap_add(map, idx);
	}

	/* verify cardinality (may be < 200 due to collisions) */
	{
		size_t		card = sparsemap_cardinality(map);

		EXPECT_TRUE(card > 0);
		EXPECT_TRUE(card <= 200);
	}

	/* verify minimum <= maximum */
	{
		uint64		min_val = sparsemap_minimum(map);
		uint64		max_val = sparsemap_maximum(map);

		EXPECT_TRUE(SPARSEMAP_FOUND(min_val));
		EXPECT_TRUE(SPARSEMAP_FOUND(max_val));
		EXPECT_TRUE(min_val <= max_val);
	}

	sparsemap_free(map);
}

/* -------------------------------------------------------------------
 * Entry point
 * ------------------------------------------------------------------- */
PG_FUNCTION_INFO_V1(test_sparsemap);

Datum
test_sparsemap(PG_FUNCTION_ARGS)
{
	test_lifecycle();
	test_capacity();
	test_single_bit();
	test_aggregates();
	test_minimum_rolling();
	test_rank_select();
	test_span();
	test_scan();
	test_rle();
	test_rle_edge_cases();
	test_union();
	test_intersection();
	test_difference();
	test_split();
	test_offset();
	test_get_data();
	test_sparse_pattern();

	PG_RETURN_VOID();
}
