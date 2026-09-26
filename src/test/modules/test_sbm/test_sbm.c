/*-------------------------------------------------------------------------
 *
 * test_sbm.c
 *	  Test the sparse bitmap set (sbm) data structure.
 *
 * This module publishes the sbm ADT (src/backend/lib/sbm.c) to the SQL layer
 * as thin wrappers, so its behavior can be exercised from a regression test,
 * and adds a randomized differential test that checks sbm against an
 * independent oracle (a sorted uint64 array).  The individual-function tests
 * mirror PostgreSQL's test_bitmapset module; the behaviors covered follow the
 * upstream sparsemap test suite (construction and the NULL-map contract,
 * membership, cardinality, min/max, rank/select/span, the set operations,
 * subset/compare, split/offset, range operations, serialization round-trip,
 * and structural validation).
 *
 * A set is transported across the SQL boundary as a bigint[] of its members.
 * SQL NULL means the empty set.  Members must be non-negative (they map to
 * uint64 indexes); a negative member is an error.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_sbm/test_sbm.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "common/pg_prng.h"
#include "fmgr.h"
#include "funcapi.h"
/* exercise sbm_init()/sbm_open() on caller-allocated (stack) maps */
#define SBM_EXPOSE_STRUCT
#include "lib/sbm.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/tidbitmap.h"
#include "port/pg_bitutils.h"
#include "storage/itemptr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"
#include "portability/instr_time.h"

PG_MODULE_MAGIC;

/* Construction / lifecycle exercised through the wrappers below */
PG_FUNCTION_INFO_V1(test_sbm_cardinality);
PG_FUNCTION_INFO_V1(test_sbm_contains);
PG_FUNCTION_INFO_V1(test_sbm_is_empty);
PG_FUNCTION_INFO_V1(test_sbm_minimum);
PG_FUNCTION_INFO_V1(test_sbm_maximum);
PG_FUNCTION_INFO_V1(test_sbm_add);
PG_FUNCTION_INFO_V1(test_sbm_remove);
PG_FUNCTION_INFO_V1(test_sbm_assign);
PG_FUNCTION_INFO_V1(test_sbm_add_range);
PG_FUNCTION_INFO_V1(test_sbm_remove_range);
PG_FUNCTION_INFO_V1(test_sbm_flip_range);
PG_FUNCTION_INFO_V1(test_sbm_rank);
PG_FUNCTION_INFO_V1(test_sbm_select);
PG_FUNCTION_INFO_V1(test_sbm_span);
PG_FUNCTION_INFO_V1(test_sbm_union);
PG_FUNCTION_INFO_V1(test_sbm_intersection);
PG_FUNCTION_INFO_V1(test_sbm_difference);
PG_FUNCTION_INFO_V1(test_sbm_xor);
PG_FUNCTION_INFO_V1(test_sbm_union_cardinality);
PG_FUNCTION_INFO_V1(test_sbm_intersection_cardinality);
PG_FUNCTION_INFO_V1(test_sbm_difference_cardinality);
PG_FUNCTION_INFO_V1(test_sbm_xor_cardinality);
PG_FUNCTION_INFO_V1(test_sbm_equals);
PG_FUNCTION_INFO_V1(test_sbm_compare);
PG_FUNCTION_INFO_V1(test_sbm_is_subset);
PG_FUNCTION_INFO_V1(test_sbm_is_superset);
PG_FUNCTION_INFO_V1(test_sbm_overlap);
PG_FUNCTION_INFO_V1(test_sbm_nonempty_difference);
PG_FUNCTION_INFO_V1(test_sbm_subset_compare);
PG_FUNCTION_INFO_V1(test_sbm_jaccard_index);
PG_FUNCTION_INFO_V1(test_sbm_membership);
PG_FUNCTION_INFO_V1(test_sbm_singleton_member);
PG_FUNCTION_INFO_V1(test_sbm_next_member);
PG_FUNCTION_INFO_V1(test_sbm_prev_member);
PG_FUNCTION_INFO_V1(test_sbm_pop_first);
PG_FUNCTION_INFO_V1(test_sbm_pop_last);
PG_FUNCTION_INFO_V1(test_sbm_offset);
PG_FUNCTION_INFO_V1(test_sbm_extract_range);
PG_FUNCTION_INFO_V1(test_sbm_hash);
PG_FUNCTION_INFO_V1(test_sbm_fill_factor);
PG_FUNCTION_INFO_V1(test_sbm_roundtrip);
PG_FUNCTION_INFO_V1(test_sbm_serialize_roundtrip);
PG_FUNCTION_INFO_V1(test_sbm_split);

/* Randomized differential test against a sorted-array oracle */
PG_FUNCTION_INFO_V1(test_sbm_random_operations);

/* Convenient macros to test results */
#define EXPECT_TRUE(expr)	\
	do { \
		if (!(expr)) \
			elog(ERROR, \
				 "%s was unexpectedly false in file \"%s\" line %u", \
				 #expr, __FILE__, __LINE__); \
	} while (0)

/* Initial byte capacity for a freshly built map. */
#define TEST_SBM_INITIAL_CAP	4096

/*
 * Build an sbm from a bigint[] of members.  A SQL NULL array is the empty set
 * (returned as NULL, which every sbm_* function accepts as an empty,
 * read-only map).  Negative members are rejected.  The result and all its
 * storage live in the current memory context.
 */
static Sbm *
array_to_sbm(ArrayType *arr)
{
	Sbm		   *map;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int16		typlen;
	bool		typbyval;
	char		typalign;

	if (arr == NULL)
		return NULL;

	get_typlenbyvalalign(INT8OID, &typlen, &typbyval, &typalign);
	deconstruct_array(arr, INT8OID, typlen, typbyval, typalign,
					  &elems, &nulls, &nelems);

	if (nelems == 0)
		return NULL;

	map = sbm_create(TEST_SBM_INITIAL_CAP);

	for (int i = 0; i < nelems; i++)
	{
		int64		v;

		if (nulls[i])
			elog(ERROR, "sbm member must not be NULL");
		v = DatumGetInt64(elems[i]);
		if (v < 0)
			elog(ERROR, "sbm member must be non-negative, got " INT64_FORMAT, v);

		/* grow the backing buffer as needed */
		while (sbm_add_grow(&map, (uint64) v) == SBM_IDX_MAX)
			elog(ERROR, "sbm_add_grow failed for member " INT64_FORMAT, v);
	}

	return map;
}

/* Fetch argument n as an sbm (SQL NULL -> empty set). */
#define PG_ARG_GETSBM(n) \
	(PG_ARGISNULL(n) ? NULL : array_to_sbm(PG_GETARG_ARRAYTYPE_P(n)))

/*
 * Convert an sbm to a bigint[] of its members in ascending order.  An empty
 * or NULL map becomes an empty (not NULL) array, matching how the SQL tests
 * compare results.
 */
static ArrayType *
sbm_members_to_array(const Sbm *map)
{
	size_t		card = sbm_cardinality((Sbm *) map);
	Datum	   *elems;
	uint64		idx;
	SbmCursor	cur = SBM_CURSOR_INIT;
	int			n = 0;

	if (card == 0)
		return construct_empty_array(INT8OID);

	elems = palloc_array(Datum, card);
	idx = SBM_IDX_MAX;			/* start-of-scan sentinel */
	while ((idx = sbm_next_member(map, idx, &cur)) != SBM_IDX_MAX)
	{
		if (n >= (int) card)
			elog(ERROR, "sbm cardinality/iteration mismatch");
		elems[n++] = Int64GetDatum((int64) idx);
	}
	if (n != (int) card)
		elog(ERROR, "sbm iteration returned %d of " UINT64_FORMAT " members",
			 n, (uint64) card);

	return construct_array_builtin(elems, n, INT8OID);
}

#define PG_RETURN_SBM_AS_ARRAY(map) \
	PG_RETURN_ARRAYTYPE_P(sbm_members_to_array(map))

/* --------------------------------------------------------------------------
 * Query wrappers
 * --------------------------------------------------------------------------
 */

Datum
test_sbm_cardinality(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);

	PG_RETURN_INT64((int64) sbm_cardinality(map));
}

Datum
test_sbm_contains(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		idx;

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	idx = PG_GETARG_INT64(1);
	if (idx < 0)
		PG_RETURN_BOOL(false);
	PG_RETURN_BOOL(sbm_contains(map, (uint64) idx, NULL));
}

Datum
test_sbm_is_empty(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);

	PG_RETURN_BOOL(sbm_is_empty(map));
}

Datum
test_sbm_minimum(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	uint64		m = sbm_minimum(map);

	if (sbm_is_empty(map))
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) m);
}

Datum
test_sbm_maximum(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	uint64		m = sbm_maximum(map);

	if (sbm_is_empty(map))
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) m);
}

Datum
test_sbm_fill_factor(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);

	PG_RETURN_FLOAT8(sbm_fill_factor(map));
}

/* --------------------------------------------------------------------------
 * Mutations: apply, then return the resulting set as an array
 * --------------------------------------------------------------------------
 */

Datum
test_sbm_add(PG_FUNCTION_ARGS)
{
	Sbm		   *map;
	int64		idx;

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	idx = PG_GETARG_INT64(1);
	if (idx < 0)
		elog(ERROR, "sbm member must be non-negative");

	map = PG_ARG_GETSBM(0);
	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);
	while (sbm_add_grow(&map, (uint64) idx) == SBM_IDX_MAX)
		elog(ERROR, "sbm_add_grow failed");
	PG_RETURN_SBM_AS_ARRAY(map);
}

/*
 * Prove the allocate-on-first-use contract: start from a genuinely NULL
 * Sbm *, add via all three grow entry points, and return the union.  Under
 * the old contract every add would no-op (returning SBM_IDX_MAX) and the
 * result would be empty.
 */
PG_FUNCTION_INFO_V1(test_sbm_add_grow_from_null);
Datum
test_sbm_add_grow_from_null(PG_FUNCTION_ARGS)
{
	Sbm		   *map = NULL;		/* deliberately NULL, not sbm_create() */
	SbmCursor	cur = SBM_CURSOR_INIT;
	const uint64 many[3] = {100, 200, 300};

	if (sbm_add_grow(&map, 1) == SBM_IDX_MAX)
		elog(ERROR, "sbm_add_grow from NULL failed");
	if (sbm_add_grow_cursor(&map, 2, &cur) == SBM_IDX_MAX)
		elog(ERROR, "sbm_add_grow_cursor from existing failed");
	if (!sbm_add_many_grow(&map, many, 3))
		elog(ERROR, "sbm_add_many_grow failed");
	PG_RETURN_SBM_AS_ARRAY(map);
}

Datum
test_sbm_remove(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		idx;

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	idx = PG_GETARG_INT64(1);
	if (idx < 0)
		elog(ERROR, "sbm member must be non-negative");
	if (map != NULL)
		(void) sbm_remove(map, (uint64) idx);
	PG_RETURN_SBM_AS_ARRAY(map);
}

Datum
test_sbm_assign(PG_FUNCTION_ARGS)
{
	Sbm		   *map;
	int64		idx;
	bool		value;

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();
	idx = PG_GETARG_INT64(1);
	value = PG_GETARG_BOOL(2);
	if (idx < 0)
		elog(ERROR, "sbm member must be non-negative");

	map = PG_ARG_GETSBM(0);
	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);
	/* sbm_assign can need to grow when setting a bit; ensure headroom. */
	while (value && sbm_add_grow(&map, (uint64) idx) == SBM_IDX_MAX)
		elog(ERROR, "sbm_add_grow failed");
	if (!value)
		(void) sbm_assign(map, (uint64) idx, false);
	PG_RETURN_SBM_AS_ARRAY(map);
}

Datum
test_sbm_add_range(PG_FUNCTION_ARGS)
{
	Sbm		   *map;
	int64		lo,
				hi;

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();
	lo = PG_GETARG_INT64(1);
	hi = PG_GETARG_INT64(2);
	if (lo < 0 || hi < 0)
		elog(ERROR, "sbm range bounds must be non-negative");
	if (hi < lo)
		elog(ERROR, "sbm range hi must be >= lo");

	map = PG_ARG_GETSBM(0);
	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);
	/* add_range may need a larger buffer; grow generously and retry. */
	while (!sbm_add_range(map, (uint64) lo, (uint64) hi))
	{
		Sbm		   *grown = sbm_set_data_size(map, NULL,
											  sbm_get_capacity(map) * 2 + 256);

		if (grown == NULL)
			elog(ERROR, "sbm_set_data_size failed");
		map = grown;
	}
	PG_RETURN_SBM_AS_ARRAY(map);
}

Datum
test_sbm_remove_range(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		lo,
				hi;

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();
	lo = PG_GETARG_INT64(1);
	hi = PG_GETARG_INT64(2);
	if (lo < 0 || hi < 0)
		elog(ERROR, "sbm range bounds must be non-negative");
	if (map != NULL)
		(void) sbm_remove_range(map, (uint64) lo, (uint64) hi);
	PG_RETURN_SBM_AS_ARRAY(map);
}

Datum
test_sbm_flip_range(PG_FUNCTION_ARGS)
{
	Sbm		   *map;
	int64		lo,
				hi;

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();
	lo = PG_GETARG_INT64(1);
	hi = PG_GETARG_INT64(2);
	if (lo < 0 || hi < 0)
		elog(ERROR, "sbm range bounds must be non-negative");

	map = PG_ARG_GETSBM(0);
	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);
	while (!sbm_flip_range(map, (uint64) lo, (uint64) hi))
	{
		Sbm		   *grown = sbm_set_data_size(map, NULL,
											  sbm_get_capacity(map) * 2 + 256);

		if (grown == NULL)
			elog(ERROR, "sbm_set_data_size failed");
		map = grown;
	}
	PG_RETURN_SBM_AS_ARRAY(map);
}

/* --------------------------------------------------------------------------
 * Rank / select / span
 * --------------------------------------------------------------------------
 */

Datum
test_sbm_rank(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		x,
				y;
	bool		value;

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
		PG_RETURN_NULL();
	x = PG_GETARG_INT64(1);
	y = PG_GETARG_INT64(2);
	value = PG_GETARG_BOOL(3);
	if (x < 0 || y < 0)
		elog(ERROR, "sbm rank bounds must be non-negative");
	PG_RETURN_INT64((int64) sbm_rank(map, (uint64) x, (uint64) y, value));
}

Datum
test_sbm_select(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		n;
	bool		value;
	uint64		res;

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();
	n = PG_GETARG_INT64(1);
	value = PG_GETARG_BOOL(2);
	if (n < 0)
		elog(ERROR, "sbm select rank must be non-negative");
	res = sbm_select(map, (uint64) n, value);
	if (res == SBM_IDX_MAX)
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) res);
}

Datum
test_sbm_span(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		start;
	int64		len;
	bool		value;
	uint64		res;

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
		PG_RETURN_NULL();
	start = PG_GETARG_INT64(1);
	len = PG_GETARG_INT64(2);
	value = PG_GETARG_BOOL(3);
	if (start < 0 || len < 0)
		elog(ERROR, "sbm span start/len must be non-negative");
	res = sbm_span(map, (uint64) start, (size_t) len, value);
	if (res == SBM_IDX_MAX)
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) res);
}

/* --------------------------------------------------------------------------
 * Set operations (result as array)
 * --------------------------------------------------------------------------
 */

Datum
test_sbm_union(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_SBM_AS_ARRAY(sbm_union(a, b));
}

Datum
test_sbm_intersection(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_SBM_AS_ARRAY(sbm_intersection(a, b));
}

Datum
test_sbm_difference(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_SBM_AS_ARRAY(sbm_difference(a, b));
}

Datum
test_sbm_xor(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_SBM_AS_ARRAY(sbm_xor(a, b));
}

Datum
test_sbm_union_cardinality(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_INT64((int64) sbm_union_cardinality(a, b));
}

Datum
test_sbm_intersection_cardinality(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_INT64((int64) sbm_intersection_cardinality(a, b));
}

Datum
test_sbm_difference_cardinality(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_INT64((int64) sbm_difference_cardinality(a, b));
}

Datum
test_sbm_xor_cardinality(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_INT64((int64) sbm_xor_cardinality(a, b));
}

/* --------------------------------------------------------------------------
 * Comparisons / relations
 * --------------------------------------------------------------------------
 */

Datum
test_sbm_equals(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_BOOL(sbm_equals(a, b));
}

Datum
test_sbm_compare(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_INT32(sbm_compare(a, b));
}

Datum
test_sbm_is_subset(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_BOOL(sbm_is_subset(a, b));
}

Datum
test_sbm_is_superset(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_BOOL(sbm_is_superset(a, b));
}

Datum
test_sbm_overlap(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_BOOL(sbm_overlap(a, b));
}

Datum
test_sbm_nonempty_difference(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_BOOL(sbm_nonempty_difference(a, b));
}

Datum
test_sbm_subset_compare(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_INT32((int32) sbm_subset_compare(a, b));
}

Datum
test_sbm_jaccard_index(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);

	PG_RETURN_FLOAT8(sbm_jaccard_index(a, b));
}

/* --------------------------------------------------------------------------
 * Membership classification and navigation
 * --------------------------------------------------------------------------
 */

Datum
test_sbm_membership(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);

	PG_RETURN_INT32((int32) sbm_membership(map));
}

Datum
test_sbm_singleton_member(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	uint64		m = sbm_singleton_member(map);

	if (m == SBM_IDX_MAX)
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) m);
}

Datum
test_sbm_next_member(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		prev;
	uint64		res;

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	prev = PG_GETARG_INT64(1);
	/* A negative "prev" means "start at the first member". */
	res = sbm_next_member(map,
						  prev < 0 ? SBM_IDX_MAX : (uint64) prev,
						  NULL);
	if (res == SBM_IDX_MAX)
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) res);
}

Datum
test_sbm_prev_member(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		prev;
	uint64		res;

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	prev = PG_GETARG_INT64(1);
	/* A negative "prev" means "start at the last member". */
	res = sbm_prev_member(map,
						  prev < 0 ? SBM_IDX_MAX : (uint64) prev,
						  NULL);
	if (res == SBM_IDX_MAX)
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) res);
}

Datum
test_sbm_pop_first(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	uint64		m;

	if (map == NULL)
		PG_RETURN_NULL();
	m = sbm_pop_first(map);
	if (m == SBM_IDX_MAX)
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) m);
}

Datum
test_sbm_pop_last(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	uint64		m;

	if (map == NULL)
		PG_RETURN_NULL();
	m = sbm_pop_last(map);
	if (m == SBM_IDX_MAX)
		PG_RETURN_NULL();
	PG_RETURN_INT64((int64) m);
}

/* --------------------------------------------------------------------------
 * Structural transforms
 * --------------------------------------------------------------------------
 */

Datum
test_sbm_offset(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		off;

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	off = PG_GETARG_INT64(1);
	PG_RETURN_SBM_AS_ARRAY(sbm_offset(map, (ssize_t) off));
}

Datum
test_sbm_extract_range(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		lo,
				hi;

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();
	lo = PG_GETARG_INT64(1);
	hi = PG_GETARG_INT64(2);
	if (lo < 0 || hi < 0)
		elog(ERROR, "sbm range bounds must be non-negative");
	PG_RETURN_SBM_AS_ARRAY(sbm_extract_range(map, (uint64) lo, (uint64) hi));
}

Datum
test_sbm_hash(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);

	PG_RETURN_INT64((int64) sbm_hash(map));
}

/*
 * Build a set, split it at idx into left (kept) and right (moved), and return
 * the two halves concatenated with a NULL separator would be awkward in SQL,
 * so instead we verify the split invariant internally and return the pair of
 * cardinalities as a 2-element array: {|left|, |right|}.  The SQL test checks
 * that left holds members < idx, right holds members >= idx, and the union of
 * the two equals the original.
 */
Datum
test_sbm_split(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	int64		idx;
	Sbm		   *other;
	size_t		orig_card;
	size_t		left_card;
	size_t		right_card;
	uint64		m;
	SbmCursor	cur = SBM_CURSOR_INIT;
	Datum		out[2];

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	idx = PG_GETARG_INT64(1);
	if (idx < 0)
		elog(ERROR, "sbm split index must be non-negative");
	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);

	orig_card = sbm_cardinality(map);
	other = sbm_create(TEST_SBM_INITIAL_CAP);
	(void) sbm_split(map, (uint64) idx, other);

	/* Invariant: left holds only members < idx. */
	m = SBM_IDX_MAX;
	while ((m = sbm_next_member(map, m, &cur)) != SBM_IDX_MAX)
		EXPECT_TRUE(m < (uint64) idx);

	/* Invariant: right holds only members >= idx. */
	cur = (SbmCursor) SBM_CURSOR_INIT;
	m = SBM_IDX_MAX;
	while ((m = sbm_next_member(other, m, &cur)) != SBM_IDX_MAX)
		EXPECT_TRUE(m >= (uint64) idx);

	left_card = sbm_cardinality(map);
	right_card = sbm_cardinality(other);

	/* Invariant: nothing lost, nothing duplicated. */
	EXPECT_TRUE(left_card + right_card == orig_card);

	out[0] = Int64GetDatum((int64) left_card);
	out[1] = Int64GetDatum((int64) right_card);
	PG_RETURN_ARRAYTYPE_P(construct_array_builtin(out, 2, INT8OID));
}

/* --------------------------------------------------------------------------
 * Round-trip checks
 * --------------------------------------------------------------------------
 */

/*
 * Build a set from the array, iterate it back out, and return the members.
 * A stable round-trip (array -> sbm -> array) is the most basic contract and
 * also exercises sbm_validate() on the constructed map.
 */
Datum
test_sbm_roundtrip(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);

	if (map != NULL)
		EXPECT_TRUE(sbm_validate(map));
	PG_RETURN_SBM_AS_ARRAY(map);
}

/*
 * Serialize the set to its stable wire format, deserialize it back, and
 * return the resulting members.  Exercises sbm_serialized_size /
 * sbm_serialize / sbm_deserialize and the validation performed on decode.
 */
Datum
test_sbm_serialize_roundtrip(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	size_t		need;
	uint8	   *buf;
	size_t		wrote;
	Sbm		   *back;
	ArrayType  *result;

	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);

	need = sbm_serialized_size(map);
	buf = (uint8 *) palloc(need);
	wrote = sbm_serialize(map, buf, need);
	EXPECT_TRUE(wrote == need);

	back = sbm_deserialize(buf, wrote);
	EXPECT_TRUE(back != NULL);
	EXPECT_TRUE(sbm_validate(back));
	EXPECT_TRUE(sbm_equals(map, back));

	result = sbm_members_to_array(back);
	PG_RETURN_ARRAYTYPE_P(result);
}

/* --------------------------------------------------------------------------
 * Randomized differential test against a sorted-array oracle
 * --------------------------------------------------------------------------
 */

/* Insert v into the sorted, duplicate-free oracle; returns new length. */
static int
oracle_add(uint64 *oracle, int n, uint64 v)
{
	int			lo = 0,
				hi = n;

	/* binary search for insertion point */
	while (lo < hi)
	{
		int			mid = (lo + hi) / 2;

		if (oracle[mid] < v)
			lo = mid + 1;
		else
			hi = mid;
	}
	if (lo < n && oracle[lo] == v)
		return n;				/* already present */
	memmove(&oracle[lo + 1], &oracle[lo], (size_t) (n - lo) * sizeof(uint64));
	oracle[lo] = v;
	return n + 1;
}

/* Remove v from the sorted oracle; returns new length. */
static int
oracle_remove(uint64 *oracle, int n, uint64 v)
{
	int			lo = 0,
				hi = n;

	while (lo < hi)
	{
		int			mid = (lo + hi) / 2;

		if (oracle[mid] < v)
			lo = mid + 1;
		else
			hi = mid;
	}
	if (lo >= n || oracle[lo] != v)
		return n;				/* not present */
	memmove(&oracle[lo], &oracle[lo + 1], (size_t) (n - lo - 1) * sizeof(uint64));
	return n - 1;
}

static bool
oracle_contains(const uint64 *oracle, int n, uint64 v)
{
	int			lo = 0,
				hi = n;

	while (lo < hi)
	{
		int			mid = (lo + hi) / 2;

		if (oracle[mid] < v)
			lo = mid + 1;
		else if (oracle[mid] > v)
			hi = mid;
		else
			return true;
	}
	return false;
}

/*
 * Drive a long sequence of random add/remove operations against both an sbm
 * and a sorted-array oracle, cross-checking membership, cardinality, min/max,
 * ordered iteration, rank/select, and a serialization round-trip along the
 * way.  Any divergence is reported with the seed so it can be replayed.
 *
 * Args: (seed bigint, num_ops int, min_value int, max_value int).  A NULL
 * seed uses the current timestamp.  Returns the final cardinality.
 */
Datum
test_sbm_random_operations(PG_FUNCTION_ARGS)
{
	pg_prng_state state;
	uint64		seed = (uint64) GetCurrentTimestamp();
	int			num_ops;
	int			min_value;
	int			max_value;
	uint32		range;
	Sbm		   *map;
	uint64	   *oracle;
	int			n = 0;

	if (!PG_ARGISNULL(0))
		seed = (uint64) PG_GETARG_INT64(0);
	if (PG_ARGISNULL(1) || PG_GETARG_INT32(1) <= 0)
		elog(ERROR, "invalid number of operations");
	if (PG_ARGISNULL(2) || PG_GETARG_INT32(2) < 0)
		elog(ERROR, "invalid minimum value");
	if (PG_ARGISNULL(3) || PG_GETARG_INT32(3) < 0)
		elog(ERROR, "invalid maximum value");

	num_ops = PG_GETARG_INT32(1);
	min_value = PG_GETARG_INT32(2);
	max_value = PG_GETARG_INT32(3);
	if (max_value < min_value)
		elog(ERROR, "maximum value must be >= minimum value");

	pg_prng_seed(&state, seed);
	range = (uint32) max_value - (uint32) min_value + 1;

	map = sbm_create(TEST_SBM_INITIAL_CAP);
	oracle = palloc_array(uint64, num_ops + 1);

	/* Phase 1: random add/remove, cross-checking membership as we go. */
	for (int i = 0; i < num_ops; i++)
	{
		uint64		v = (uint64) min_value + (pg_prng_uint32(&state) % range);
		bool		do_add = (pg_prng_uint32(&state) & 1) != 0 || n == 0;

		CHECK_FOR_INTERRUPTS();

		if (do_add)
		{
			while (sbm_add_grow(&map, v) == SBM_IDX_MAX)
				elog(ERROR, "sbm_add_grow failed, seed " UINT64_FORMAT, seed);
			n = oracle_add(oracle, n, v);
		}
		else
		{
			(void) sbm_remove(map, v);
			n = oracle_remove(oracle, n, v);
		}

		if (sbm_contains(map, v, NULL) != oracle_contains(oracle, n, v))
			elog(ERROR,
				 "membership mismatch for " UINT64_FORMAT " after %s, seed " UINT64_FORMAT,
				 v, do_add ? "add" : "remove", seed);
	}

	/* Cardinality must match the oracle. */
	if ((int) sbm_cardinality(map) != n)
		elog(ERROR, "cardinality mismatch: sbm=%d oracle=%d, seed " UINT64_FORMAT,
			 (int) sbm_cardinality(map), n, seed);

	/* min / max must match. */
	if (n > 0)
	{
		if (sbm_minimum(map) != oracle[0])
			elog(ERROR, "minimum mismatch, seed " UINT64_FORMAT, seed);
		if (sbm_maximum(map) != oracle[n - 1])
			elog(ERROR, "maximum mismatch, seed " UINT64_FORMAT, seed);
	}

	/*
	 * Ordered iteration must reproduce the oracle exactly, and rank/select
	 * must agree at every position.
	 */
	{
		SbmCursor	cur = SBM_CURSOR_INIT;
		uint64		idx = SBM_IDX_MAX;
		int			k = 0;

		while ((idx = sbm_next_member(map, idx, &cur)) != SBM_IDX_MAX)
		{
			if (k >= n)
				elog(ERROR, "iteration overran oracle, seed " UINT64_FORMAT, seed);
			if (idx != oracle[k])
				elog(ERROR,
					 "iteration mismatch at %d: sbm=" UINT64_FORMAT " oracle=" UINT64_FORMAT ", seed " UINT64_FORMAT,
					 k, idx, oracle[k], seed);
			/* rank of set bits in [0, idx] is k+1 */
			if ((int) sbm_rank(map, 0, idx, true) != k + 1)
				elog(ERROR, "rank mismatch at %d, seed " UINT64_FORMAT, k, seed);
			/* the k-th set bit (0-based) is oracle[k] */
			if (sbm_select(map, (uint64) k, true) != oracle[k])
				elog(ERROR, "select mismatch at %d, seed " UINT64_FORMAT, k, seed);
			k++;
		}
		if (k != n)
			elog(ERROR, "iteration returned %d of %d members, seed " UINT64_FORMAT,
				 k, n, seed);
	}

	/* Reverse iteration must reproduce the oracle in descending order. */
	if (n > 0)
	{
		uint64		idx = sbm_maximum(map);
		int			k = n - 1;

		while (true)
		{
			if (idx != oracle[k])
				elog(ERROR, "reverse iteration mismatch at %d, seed " UINT64_FORMAT,
					 k, seed);
			if (k == 0)
				break;
			idx = sbm_prev_member(map, idx, NULL);
			if (idx == SBM_IDX_MAX)
				elog(ERROR, "reverse iteration ended early, seed " UINT64_FORMAT, seed);
			k--;
		}
	}

	/* Structural validation and a serialization round-trip. */
	if (!sbm_validate(map))
		elog(ERROR, "sbm_validate failed, seed " UINT64_FORMAT, seed);
	{
		size_t		need = sbm_serialized_size(map);
		uint8	   *buf = (uint8 *) palloc(need);
		size_t		wrote = sbm_serialize(map, buf, need);
		Sbm		   *back = sbm_deserialize(buf, wrote);

		if (back == NULL || !sbm_equals(map, back))
			elog(ERROR, "serialization round-trip failed, seed " UINT64_FORMAT, seed);
	}

	PG_RETURN_INT32(n);
}

/* --------------------------------------------------------------------------
 * Additional coverage: constructors, bulk ops, aliases, in-place ops,
 * introspection, buffer lifecycle, scan, and the locator family.  These
 * exercise the public API paths (and the internal helpers they reach) that
 * the wrappers above do not.
 * --------------------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(test_sbm_or);
PG_FUNCTION_INFO_V1(test_sbm_and);
PG_FUNCTION_INFO_V1(test_sbm_andnot);
PG_FUNCTION_INFO_V1(test_sbm_union_inplace);
PG_FUNCTION_INFO_V1(test_sbm_intersection_inplace);
PG_FUNCTION_INFO_V1(test_sbm_difference_inplace);
PG_FUNCTION_INFO_V1(test_sbm_xor_inplace);
PG_FUNCTION_INFO_V1(test_sbm_create_singleton);
PG_FUNCTION_INFO_V1(test_sbm_create_from_range);
PG_FUNCTION_INFO_V1(test_sbm_create_from_array);
PG_FUNCTION_INFO_V1(test_sbm_add_many);
PG_FUNCTION_INFO_V1(test_sbm_to_array);
PG_FUNCTION_INFO_V1(test_sbm_contains_many);
PG_FUNCTION_INFO_V1(test_sbm_scan_cardinality);
PG_FUNCTION_INFO_V1(test_sbm_capacity_remaining);
PG_FUNCTION_INFO_V1(test_sbm_shrink_to_fit);
PG_FUNCTION_INFO_V1(test_sbm_statistics);
PG_FUNCTION_INFO_V1(test_sbm_add_grow_cursor_check);
PG_FUNCTION_INFO_V1(test_sbm_wrap_roundtrip);
PG_FUNCTION_INFO_V1(test_sbm_owned_copy_roundtrip);
PG_FUNCTION_INFO_V1(test_sbm_locator_check);

/*
 * sbm_or / sbm_and / sbm_andnot are aliases of union / intersection /
 * difference; verify each matches its primary and return the result.
 */
Datum
test_sbm_or(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);
	Sbm		   *viaor = sbm_or(a, b);
	Sbm		   *viaunion = sbm_union(a, b);

	EXPECT_TRUE(sbm_equals(viaor, viaunion));
	PG_RETURN_SBM_AS_ARRAY(viaor);
}

Datum
test_sbm_and(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);
	Sbm		   *viaand = sbm_and(a, b);

	EXPECT_TRUE(sbm_equals(viaand, sbm_intersection(a, b)));
	PG_RETURN_SBM_AS_ARRAY(viaand);
}

Datum
test_sbm_andnot(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);
	Sbm		   *viaandnot = sbm_andnot(a, b);

	EXPECT_TRUE(sbm_equals(viaandnot, sbm_difference(a, b)));
	PG_RETURN_SBM_AS_ARRAY(viaandnot);
}

/*
 * In-place set ops: mutate a private copy of the left operand and return it.
 * Each must equal the out-of-place result.
 */
Datum
test_sbm_union_inplace(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);
	Sbm		   *dst = (a == NULL) ? sbm_create(TEST_SBM_INITIAL_CAP) : sbm_copy(a);

	dst = sbm_union_inplace(dst, b);
	EXPECT_TRUE(sbm_equals(dst, sbm_union(a, b)));
	PG_RETURN_SBM_AS_ARRAY(dst);
}

Datum
test_sbm_intersection_inplace(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);
	Sbm		   *dst = (a == NULL) ? sbm_create(TEST_SBM_INITIAL_CAP) : sbm_copy(a);

	dst = sbm_intersection_inplace(dst, b);
	EXPECT_TRUE(sbm_equals(dst, sbm_intersection(a, b)));
	PG_RETURN_SBM_AS_ARRAY(dst);
}

Datum
test_sbm_difference_inplace(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);
	Sbm		   *dst = (a == NULL) ? sbm_create(TEST_SBM_INITIAL_CAP) : sbm_copy(a);

	dst = sbm_difference_inplace(dst, b);
	EXPECT_TRUE(sbm_equals(dst, sbm_difference(a, b)));
	PG_RETURN_SBM_AS_ARRAY(dst);
}

Datum
test_sbm_xor_inplace(PG_FUNCTION_ARGS)
{
	Sbm		   *a = PG_ARG_GETSBM(0);
	Sbm		   *b = PG_ARG_GETSBM(1);
	Sbm		   *dst = (a == NULL) ? sbm_create(TEST_SBM_INITIAL_CAP) : sbm_copy(a);

	dst = sbm_xor_inplace(dst, b);
	EXPECT_TRUE(sbm_equals(dst, sbm_xor(a, b)));
	PG_RETURN_SBM_AS_ARRAY(dst);
}

/* Constructors */
Datum
test_sbm_create_singleton(PG_FUNCTION_ARGS)
{
	int64		idx;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	idx = PG_GETARG_INT64(0);
	if (idx < 0)
		elog(ERROR, "sbm member must be non-negative");
	PG_RETURN_SBM_AS_ARRAY(sbm_create_singleton((uint64) idx));
}

Datum
test_sbm_create_from_range(PG_FUNCTION_ARGS)
{
	int64		lo,
				hi;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	lo = PG_GETARG_INT64(0);
	hi = PG_GETARG_INT64(1);
	if (lo < 0 || hi < 0)
		elog(ERROR, "sbm range bounds must be non-negative");
	PG_RETURN_SBM_AS_ARRAY(sbm_create_from_range((uint64) lo, (uint64) hi));
}

/*
 * sbm_create_from_array: pass the members through the library's own
 * array constructor (which sorts internally, exercising sbm_cmp_u64) rather
 * than through array_to_sbm's add loop.
 */
Datum
test_sbm_create_from_array(PG_FUNCTION_ARGS)
{
	ArrayType  *arr;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	uint64	   *vals;
	Sbm		   *map;

	if (PG_ARGISNULL(0))
		PG_RETURN_SBM_AS_ARRAY(sbm_create_from_array(NULL, 0));
	arr = PG_GETARG_ARRAYTYPE_P(0);
	get_typlenbyvalalign(INT8OID, &typlen, &typbyval, &typalign);
	deconstruct_array(arr, INT8OID, typlen, typbyval, typalign,
					  &elems, &nulls, &nelems);
	vals = palloc_array(uint64, Max(nelems, 1));
	for (int i = 0; i < nelems; i++)
	{
		int64		v;

		if (nulls[i])
			elog(ERROR, "sbm member must not be NULL");
		v = DatumGetInt64(elems[i]);
		if (v < 0)
			elog(ERROR, "sbm member must be non-negative");
		vals[i] = (uint64) v;
	}
	map = sbm_create_from_array(vals, (size_t) nelems);
	PG_RETURN_SBM_AS_ARRAY(map);
}

/* sbm_add_many / sbm_add_many_grow: add a batch, return the result. */
Datum
test_sbm_add_many(PG_FUNCTION_ARGS)
{
	ArrayType  *arr;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	uint64	   *vals;
	Sbm		   *map = sbm_create(TEST_SBM_INITIAL_CAP);

	if (PG_ARGISNULL(0))
		PG_RETURN_SBM_AS_ARRAY(map);
	arr = PG_GETARG_ARRAYTYPE_P(0);
	get_typlenbyvalalign(INT8OID, &typlen, &typbyval, &typalign);
	deconstruct_array(arr, INT8OID, typlen, typbyval, typalign,
					  &elems, &nulls, &nelems);
	vals = palloc_array(uint64, Max(nelems, 1));
	for (int i = 0; i < nelems; i++)
	{
		int64		v = DatumGetInt64(elems[i]);

		if (nulls[i] || v < 0)
			elog(ERROR, "sbm member must be non-negative and not NULL");
		vals[i] = (uint64) v;
	}
	/* add_many_grow reallocates as needed and covers add_many internally. */
	if (!sbm_add_many_grow(&map, vals, (size_t) nelems))
		elog(ERROR, "sbm_add_many_grow failed");
	PG_RETURN_SBM_AS_ARRAY(map);
}

/*
 * sbm_to_array: use the library's own extractor (distinct from the test
 * module's sbm_members_to_array helper) and return the members.
 */
Datum
test_sbm_to_array(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	size_t		card = sbm_cardinality(map);
	uint64	   *out;
	size_t		n;
	Datum	   *d;

	if (card == 0)
		PG_RETURN_ARRAYTYPE_P(construct_empty_array(INT8OID));
	out = palloc_array(uint64, card);
	n = card;
	sbm_to_array(map, out, &n);
	d = palloc_array(Datum, n);
	for (size_t i = 0; i < n; i++)
		d[i] = Int64GetDatum((int64) out[i]);
	PG_RETURN_ARRAYTYPE_P(construct_array_builtin(d, (int) n, INT8OID));
}

/*
 * sbm_contains_many: batch-probe a sorted set of indexes and return how many
 * are present; cross-check against per-index sbm_contains.
 */
Datum
test_sbm_contains_many(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	ArrayType  *arr;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	uint64	   *idxs;
	bool	   *results;
	int			present = 0;

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	arr = PG_GETARG_ARRAYTYPE_P(1);
	get_typlenbyvalalign(INT8OID, &typlen, &typbyval, &typalign);
	deconstruct_array(arr, INT8OID, typlen, typbyval, typalign,
					  &elems, &nulls, &nelems);
	if (nelems == 0)
		PG_RETURN_INT32(0);
	idxs = palloc_array(uint64, nelems);
	results = palloc_array(bool, nelems);
	for (int i = 0; i < nelems; i++)
	{
		int64		v = DatumGetInt64(elems[i]);

		if (nulls[i] || v < 0)
			elog(ERROR, "index must be non-negative and not NULL");
		/* contains_many requires ascending input */
		if (i > 0 && (uint64) v < idxs[i - 1])
			elog(ERROR, "test_sbm_contains_many requires ascending indexes");
		idxs[i] = (uint64) v;
	}
	sbm_contains_many(map, idxs, results, (size_t) nelems);
	for (int i = 0; i < nelems; i++)
	{
		if (results[i] != sbm_contains(map, idxs[i], NULL))
			elog(ERROR, "contains_many disagrees with contains at %d", i);
		if (results[i])
			present++;
	}
	PG_RETURN_INT32(present);
}

/* sbm_scan: count the delivered set-bit indices; must equal cardinality. */
static void
test_sbm_scan_cb(uint64 vec[], size_t n, void *aux)
{
	uint64	   *acc = (uint64 *) aux;

	(void) vec;					/* vec holds absolute set-bit indices */
	*acc += (uint64) n;
}

Datum
test_sbm_scan_cardinality(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	uint64		acc = 0;

	sbm_scan(map, test_sbm_scan_cb, 0, &acc);
	EXPECT_TRUE(acc == (uint64) sbm_cardinality(map));
	PG_RETURN_INT64((int64) acc);
}

/* sbm_capacity_remaining: 0..100 percent free. */
Datum
test_sbm_capacity_remaining(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);

	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);
	PG_RETURN_FLOAT8(sbm_capacity_remaining(map));
}

/* sbm_shrink_to_fit: shrink, then verify the set is unchanged. */
Datum
test_sbm_shrink_to_fit(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	Sbm		   *before;

	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);
	before = sbm_copy(map);
	map = sbm_shrink_to_fit(map);
	EXPECT_TRUE(sbm_equals(map, before));
	PG_RETURN_SBM_AS_ARRAY(map);
}

/*
 * sbm_statistics: return {chunks_total, bits_set} so the SQL test can check
 * bits_set == cardinality and chunks_total > 0 for a non-empty map.
 */
Datum
test_sbm_statistics(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	SbmStats	st;
	Datum		out[2];

	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);
	sbm_statistics(map, &st);
	EXPECT_TRUE(st.bits_set == (uint64) sbm_cardinality(map));
	out[0] = Int64GetDatum((int64) st.chunks_total);
	out[1] = Int64GetDatum((int64) st.bits_set);
	PG_RETURN_ARRAYTYPE_P(construct_array_builtin(out, 2, INT8OID));
}

/*
 * sbm_add_grow_cursor: add a batch of ascending indexes threading one cursor,
 * growing as needed; return the resulting set.
 */
Datum
test_sbm_add_grow_cursor_check(PG_FUNCTION_ARGS)
{
	ArrayType  *arr;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Sbm		   *map = sbm_create(TEST_SBM_INITIAL_CAP);
	SbmCursor	cur = SBM_CURSOR_INIT;

	if (PG_ARGISNULL(0))
		PG_RETURN_SBM_AS_ARRAY(map);
	arr = PG_GETARG_ARRAYTYPE_P(0);
	get_typlenbyvalalign(INT8OID, &typlen, &typbyval, &typalign);
	deconstruct_array(arr, INT8OID, typlen, typbyval, typalign,
					  &elems, &nulls, &nelems);
	for (int i = 0; i < nelems; i++)
	{
		int64		v = DatumGetInt64(elems[i]);

		if (nulls[i] || v < 0)
			elog(ERROR, "sbm member must be non-negative and not NULL");
		if (sbm_add_grow_cursor(&map, (uint64) v, &cur) == SBM_IDX_MAX)
			elog(ERROR, "sbm_add_grow_cursor failed");
	}
	PG_RETURN_SBM_AS_ARRAY(map);
}

/*
 * Buffer lifecycle: copy the map's raw internal buffer out (sbm_get_data /
 * sbm_get_size) and rebuild the map two ways over that buffer --
 * sbm_open_copy() (fresh owned map from the bytes) and sbm_anchor()+sbm_open()
 * (map over a caller-owned buffer) -- confirming both equal the original.
 * Covers sbm_get_data, sbm_anchor, sbm_open, and sbm_open_copy.
 */
Datum
test_sbm_wrap_roundtrip(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	size_t		n;
	const uint8 *data;
	Sbm		   *viacopy;
	Sbm		   *wrapped;
	uint8	   *body_buf;

	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);

	n = sbm_get_size(map);
	data = (const uint8 *) sbm_get_data(map);

	/* sbm_open_copy: allocate a fresh owned map from the raw bytes. */
	viacopy = sbm_open_copy(data, n, 0);
	EXPECT_TRUE(viacopy != NULL);
	EXPECT_TRUE(sbm_equals(map, viacopy));

	/* sbm_anchor + sbm_open over a caller-owned copy of the same bytes. */
	body_buf = (uint8 *) palloc(Max(n, 1));
	if (n > 0)
		memcpy(body_buf, data, n);
	wrapped = sbm_anchor(body_buf, n);
	EXPECT_TRUE(wrapped != NULL);
	sbm_open(wrapped, body_buf, n);
	EXPECT_TRUE(sbm_equals(map, wrapped));

	PG_RETURN_SBM_AS_ARRAY(viacopy);
}

/* sbm_owned_copy: normalize a map to an owned-contiguous copy; must be equal. */
Datum
test_sbm_owned_copy_roundtrip(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	Sbm		   *copy;

	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);
	copy = sbm_owned_copy(map);
	EXPECT_TRUE(copy != NULL);
	EXPECT_TRUE(sbm_equals(map, copy));
	PG_RETURN_SBM_AS_ARRAY(copy);
}

/*
 * Locator family: build an order-statistic locator over the set and verify
 * locator_contains / locator_rank / locator_select agree with the map's own
 * sbm_contains / sbm_rank / sbm_select at every member, then free it.
 * Returns the cardinality confirmed through the locator.
 */
Datum
test_sbm_locator_check(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	SbmLocator *loc;
	SbmCursor	cur = SBM_CURSOR_INIT;
	uint64		idx;
	int			k = 0;
	size_t		card;

	if (map == NULL)
		map = sbm_create(TEST_SBM_INITIAL_CAP);
	card = sbm_cardinality(map);
	loc = sbm_locator_build(map);

	/* An empty map has no locator (NULL by contract); nothing to check. */
	if (card == 0)
	{
		EXPECT_TRUE(loc == NULL);
		sbm_locator_free(loc);	/* NULL-safe */
		PG_RETURN_INT64(0);
	}
	EXPECT_TRUE(loc != NULL);

	idx = SBM_IDX_MAX;
	while ((idx = sbm_next_member(map, idx, &cur)) != SBM_IDX_MAX)
	{
		if (!sbm_locator_contains(loc, idx))
			elog(ERROR, "locator_contains missed member " UINT64_FORMAT, idx);
		/* rank over [0, idx] counts k+1 set bits */
		if (sbm_locator_rank(loc, 0, idx, true) != (size_t) (k + 1))
			elog(ERROR, "locator_rank mismatch at %d", k);
		if (sbm_locator_select(loc, (uint64) k, true) != idx)
			elog(ERROR, "locator_select mismatch at %d", k);
		k++;
	}
	/* a value not in the set is not contained */
	if (card > 0 && sbm_locator_contains(loc, sbm_maximum(map) + 1))
		elog(ERROR, "locator_contains false positive");
	sbm_locator_free(loc);
	PG_RETURN_INT64((int64) k);
}

PG_FUNCTION_INFO_V1(test_sbm_fill_factor_val);
PG_FUNCTION_INFO_V1(test_sbm_contains_cached);

/* sbm_fill_factor: fraction of the [min,max] span that is set, in [0,1]. */
Datum
test_sbm_fill_factor_val(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);

	PG_RETURN_FLOAT8(sbm_fill_factor(map));
}

/*
 * sbm_contains_cached: probe a batch of ascending indexes threading one
 * SbmCursorCached, and cross-check each against plain sbm_contains.  Returns
 * how many were present.
 */
Datum
test_sbm_contains_cached(PG_FUNCTION_ARGS)
{
	Sbm		   *map = PG_ARG_GETSBM(0);
	ArrayType  *arr;
	Datum	   *elems;
	bool	   *nulls;
	int			nelems;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	SbmCursorCached cache = SBM_CURSOR_CACHED_INIT;
	int			present = 0;

	if (PG_ARGISNULL(1))
		PG_RETURN_NULL();
	arr = PG_GETARG_ARRAYTYPE_P(1);
	get_typlenbyvalalign(INT8OID, &typlen, &typbyval, &typalign);
	deconstruct_array(arr, INT8OID, typlen, typbyval, typalign,
					  &elems, &nulls, &nelems);
	for (int i = 0; i < nelems; i++)
	{
		int64		v = DatumGetInt64(elems[i]);
		bool		hit;

		if (nulls[i] || v < 0)
			elog(ERROR, "index must be non-negative and not NULL");
		hit = sbm_contains_cached(map, (uint64) v, &cache);
		if (hit != sbm_contains(map, (uint64) v, NULL))
			elog(ERROR, "contains_cached disagrees with contains at %d", i);
		if (hit)
			present++;
	}
	PG_RETURN_INT32(present);
}

/*
 * Micro-benchmark: sbm vs PostgreSQL's Bitmapset and TIDBitmap.
 *
 * For a common workload of `n` distinct integer members drawn uniformly from
 * [0, universe) with a fixed seed, we time three structures on the same
 * indices and report, per structure, the build cost (ns/op inserted), the
 * full-scan cost (ns/op yielded), and the in-memory footprint in bytes.
 *
 * The three are not interchangeable, so the mapping is spelled out:
 *   - sbm:       idx -> sbm_add_grow; scan via sbm_scan; bytes = sbm_get_size.
 *   - Bitmapset: idx -> bms_add_member (idx must fit int); scan via
 *                bms_next_member; bytes from the word count (nwords * 8 + hdr).
 *   - TIDBitmap: idx -> ItemPointer(block = idx / MaxHeapTuplesPerPage,
 *                off = idx % MaxHeapTuplesPerPage + 1); build via
 *                tbm_add_tuples one TID at a time (mirrors the per-element
 *                insert the other two do); scan via tbm_iterate.  Given a
 *                generous memory budget so it stays exact (non-lossy).
 *
 * The point is the shape of the curves across density, not any single number:
 * Bitmapset is a flat word array (memory tracks the max index, unbeatable when
 * dense and small), sbm is sparse-chunked (wins as the set spreads out), and
 * TIDBitmap is a per-page hash keyed structure with a fixed per-page cost.
 */


/* --------------------------------------------------------------------------
 * Encoding-transition and lineage coverage
 *
 * The SQL-level wrappers above build every map from a bigint[] through
 * sbm_add_grow, which leaves most maps in whatever encoding the incremental
 * path happens to choose.  The functions below instead construct maps in a
 * specific encoding (small-set, sparse chunks, RLE chunks, or a mixture),
 * drive them through each transition between encodings, and after every step
 * compare the map against an independent oracle: a plain bool array covering
 * the test universe.  They also exercise every allocation lineage (owned,
 * wrapped, caller-initialized, wrapped-then-grown), the unaligned-buffer
 * paths, and the ENOSPC contract on caller-sized buffers.
 * --------------------------------------------------------------------------
 */

/* Oracle universe: comfortably more than a few RLE-capable chunk windows. */
#define ORACLE_BITS		(48 * 1024)

typedef struct SbmOracle
{
	bool		bit[ORACLE_BITS];
}			SbmOracle;

static void
oracle_set_range(SbmOracle * o, uint64 lo, uint64 hi, bool v)
{
	for (uint64 i = lo; i < hi && i < ORACLE_BITS; i++)
		o->bit[i] = v;
}

static size_t
oracle_count(const SbmOracle * o)
{
	size_t		n = 0;

	for (int i = 0; i < ORACLE_BITS; i++)
		n += o->bit[i];
	return n;
}

/*
 * Assert that map equals the oracle in every observable way: membership of
 * every index, cardinality (both the cached and the recomputed value), min,
 * max, forward and reverse iteration, a sample of rank/select values, and
 * structural validity.  `what` names the step for error messages.
 */
static void
check_against_oracle(const Sbm *map, const SbmOracle * o, const char *what)
{
	size_t		n = oracle_count(o);
	size_t		k = 0;
	uint64		idx = SBM_IDX_MAX;
	uint64		lo = SBM_IDX_MAX;
	uint64		hi = 0;
	int			next = 0;
	SbmCursor	cur = SBM_CURSOR_INIT;

	if (!sbm_validate(map))
	{
		const uint64 *w = (const uint64 *) sbm_get_data(map);
		StringInfoData buf;

		initStringInfo(&buf);
		for (size_t i = 0; i < sbm_get_size(map) / 8 && i < 16; i++)
			appendStringInfo(&buf, " %llx", (unsigned long long) w[i]);
		elog(ERROR, "%s: sbm_validate failed:%s", what, buf.data);
	}

	/*
	 * Forward iteration must visit exactly the oracle's members, in order;
	 * that alone pins down the whole set in O(N).
	 */
	while ((idx = sbm_next_member(map, idx, &cur)) != SBM_IDX_MAX)
	{
		while (next < ORACLE_BITS && !o->bit[next])
			next++;
		if (idx != (uint64) next)
			elog(ERROR, "%s: iteration produced " UINT64_FORMAT
				 ", expected %d", what, idx, next);
		if (lo == SBM_IDX_MAX)
			lo = idx;
		hi = idx;
		/* sample rank/select rather than paying O(n^2) */
		if (k % 499 == 0 || k + 1 == n)
		{
			if (sbm_rank(map, 0, idx, true) != k + 1)
				elog(ERROR, "%s: rank at " UINT64_FORMAT, what, idx);
			if (sbm_select(map, k, true) != idx)
				elog(ERROR, "%s: select(%zu)", what, k);
		}
		next++;
		k++;
	}
	if (k != n)
		elog(ERROR, "%s: forward iteration saw %zu of %zu", what, k, n);

	/* point lookups: a strided sample plus every chunk boundary +/- 1 */
	cur = (SbmCursor) SBM_CURSOR_INIT;
	for (int i = 0; i < ORACLE_BITS; i += 61)
		if (sbm_contains(map, i, &cur) != o->bit[i])
			elog(ERROR, "%s: membership of %d", what, i);
	for (int i = 2047; i < ORACLE_BITS; i += 2048)
		for (int d = 0; d < 3 && i + d < ORACLE_BITS; d++)
			if (sbm_contains(map, i + d, NULL) != o->bit[i + d])
				elog(ERROR, "%s: membership of %d", what, i + d);

	/* call twice: first fills the cardinality cache, second reads it */
	if (sbm_cardinality(map) != n || sbm_cardinality(map) != n)
		elog(ERROR, "%s: cardinality %zu, expected %zu", what,
			 sbm_cardinality(map), n);
	if (sbm_is_empty(map) != (n == 0))
		elog(ERROR, "%s: sbm_is_empty disagrees with cardinality", what);
	if (n > 0 && (sbm_minimum(map) != lo || sbm_maximum(map) != hi))
		elog(ERROR, "%s: min/max " UINT64_FORMAT "/" UINT64_FORMAT
			 ", expected " UINT64_FORMAT "/" UINT64_FORMAT, what,
			 sbm_minimum(map), sbm_maximum(map), lo, hi);

	/* reverse iteration: a bounded walk from the top */
	idx = SBM_IDX_MAX;
	k = 0;
	while (k < 64 && (idx = sbm_prev_member(map, idx, NULL)) != SBM_IDX_MAX)
	{
		if (idx >= ORACLE_BITS || !o->bit[idx])
			elog(ERROR, "%s: reverse iteration produced non-member "
				 UINT64_FORMAT, what, idx);
		k++;
	}
	if (k < Min(n, 64))
		elog(ERROR, "%s: reverse iteration ended early", what);
}

/* Build a map from an oracle using sbm_add_many_grow. */
static Sbm *
oracle_to_sbm(const SbmOracle * o)
{
	uint64	   *v = palloc_array(uint64, ORACLE_BITS);
	size_t		n = 0;
	Sbm		   *map = NULL;

	for (int i = 0; i < ORACLE_BITS; i++)
		if (o->bit[i])
			v[n++] = i;
	if (!sbm_add_many_grow(&map, v, n))
		elog(ERROR, "sbm_add_many_grow failed");
	pfree(v);
	return map;
}

/* Number of RLE chunks in map, via the public statistics API. */
static size_t
rle_chunks(const Sbm *map)
{
	SbmStats	st;

	sbm_statistics(map, &st);
	if (st.chunks_rle + st.chunks_sparse != st.chunks_total)
		elog(ERROR, "statistics: rle + sparse != total");
	if (st.bits_in_rle + st.bits_in_sparse != st.bits_set)
		elog(ERROR, "statistics: bits_in_rle + bits_in_sparse != bits_set");
	return st.chunks_rle;
}

/*
 * Small-set <-> chunk-mode transitions.  A map starts in small mode, is
 * promoted when a member leaves the small span, and demoted back when
 * removals bring it under the span again; read operations on a small map go
 * through a materialized chunk-mode view.
 */
static void
test_small_transitions(void)
{
	SbmOracle  *o = palloc0_object(SbmOracle);
	Sbm		   *map = sbm_create(0);
	SbmStats	st;

	/* small mode: indexes near zero */
	for (int i = 0; i < 1024; i += 7)
	{
		sbm_add(map, i);
		o->bit[i] = true;
	}
	check_against_oracle(map, o, "small: built");
	sbm_statistics(map, &st);
	if (st.bytes_used >= 256)
		elog(ERROR, "small: unexpectedly large footprint %zu", st.bytes_used);

	/* read-side operations that materialize a chunk view of a small map */
	if (sbm_span(map, 0, 1, true) != 0 || sbm_span(map, 1, 2, false) != 1)
		elog(ERROR, "small: span");
	{
		Sbm		   *shifted = sbm_offset(map, 3);

		if (!sbm_contains(shifted, 3, NULL) || sbm_contains(shifted, 0, NULL))
			elog(ERROR, "small: offset");
		sbm_free(shifted);
	}

	/* promote: a member beyond the small span */
	sbm_add(map, 5000);
	o->bit[5000] = true;
	check_against_oracle(map, o, "small: promoted");

	/* demote: remove it again */
	sbm_remove(map, 5000);
	o->bit[5000] = false;
	check_against_oracle(map, o, "small: demoted");

	/* a small run from zero promotes to a single RLE chunk */
	sbm_clear(map);
	memset(o, 0, sizeof(*o));
	sbm_add_range(map, 0, 1000);
	oracle_set_range(o, 0, 1000, true);
	sbm_add(map, 3000);
	o->bit[3000] = true;
	check_against_oracle(map, o, "small: run promoted");

	/* drain one member at a time through both ends */
	while (!sbm_is_empty(map))
	{
		uint64		f = sbm_pop_first(map);
		uint64		l = sbm_pop_last(map);

		if (f < ORACLE_BITS)
			o->bit[f] = false;
		if (l != SBM_IDX_MAX && l < ORACLE_BITS)
			o->bit[l] = false;
	}
	check_against_oracle(map, o, "small: drained");
	if (sbm_pop_first(map) != SBM_IDX_MAX || sbm_pop_last(map) != SBM_IDX_MAX)
		elog(ERROR, "pop on empty map");

	sbm_free(map);
	pfree(o);
}

/*
 * Sparse <-> RLE transitions.  Filling a chunk turns it into ones; extending
 * the run past the chunk boundary coalesces into RLE; clearing a bit at the
 * end, at the start, in the middle, and on each chunk boundary of an RLE run
 * separates it back into sparse and smaller RLE chunks.
 */
static void
test_rle_transitions(void)
{
	static const uint64 holes[] = {
		0, 1, 63, 64, 65, 2047, 2048, 2049, 4095, 4096, 6143, 6144, 9999,
		10000, 10001,
	};
	SbmOracle  *o = palloc0_object(SbmOracle);

	/* build a long run one bit at a time: sparse -> ones -> RLE coalescing */
	{
		Sbm		   *map = NULL;

		for (uint64 i = 0; i < 10002; i++)
			sbm_add_grow(&map, i);
		oracle_set_range(o, 0, 10002, true);
		check_against_oracle(map, o, "rle: incremental run");
		if (rle_chunks(map) == 0)
			elog(ERROR, "rle: incremental run did not produce an RLE chunk");
		sbm_free(map);
	}

	/* the same run built bottom-up from a non-zero, unaligned start */
	{
		Sbm		   *map = NULL;

		memset(o, 0, sizeof(*o));
		for (uint64 i = 9000; i > 100; i--)
			sbm_add_grow(&map, i);
		oracle_set_range(o, 101, 9001, true);
		check_against_oracle(map, o, "rle: descending run");
		sbm_free(map);
	}

	/* punch each hole individually into a fresh RLE run, then refill it */
	for (int h = 0; h < lengthof(holes); h++)
	{
		Sbm		   *map = sbm_create_from_range(0, 10002);
		char		what[64];

		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 0, 10002, true);
		if (rle_chunks(map) == 0)
			elog(ERROR, "rle: create_from_range did not use RLE");

		sbm_remove(map, holes[h]);
		o->bit[holes[h]] = false;
		snprintf(what, sizeof(what), "rle: hole at " UINT64_FORMAT, holes[h]);
		check_against_oracle(map, o, what);

		sbm_add_grow(&map, holes[h]);
		o->bit[holes[h]] = true;
		snprintf(what, sizeof(what), "rle: refill " UINT64_FORMAT, holes[h]);
		check_against_oracle(map, o, what);
		sbm_free(map);
	}

	/* a run of length one, and shrinking a run from its end to nothing */
	{
		Sbm		   *map = sbm_create_from_range(4096, 4097);

		memset(o, 0, sizeof(*o));
		o->bit[4096] = true;
		check_against_oracle(map, o, "rle: length-one run");
		sbm_remove(map, 4096);
		o->bit[4096] = false;
		check_against_oracle(map, o, "rle: length-one run removed");
		sbm_free(map);

		map = sbm_create_from_range(2048, 2048 + 3000);
		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 2048, 2048 + 3000, true);
		for (uint64 i = 2048 + 3000; i > 2048 + 1000; i--)
		{
			sbm_remove(map, i - 1);
			o->bit[i - 1] = false;
		}
		check_against_oracle(map, o, "rle: shrunk from the end");
		sbm_free(map);
	}

	/* two runs separated by one bit coalesce when the gap is filled */
	{
		Sbm		   *map = sbm_create_from_range(0, 3000);

		sbm_add_range(map, 3001, 7000);
		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 0, 7000, true);
		o->bit[3000] = false;
		check_against_oracle(map, o, "rle: two runs");
		sbm_add(map, 3000);
		o->bit[3000] = true;
		check_against_oracle(map, o, "rle: gap filled");
		sbm_free(map);
	}

	/* range removal / flip across RLE and sparse chunks */
	{
		Sbm		   *map = sbm_create_from_range(100, 9000);

		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 100, 9000, true);
		map = sbm_set_data_size(map, NULL, 64 * 1024);
		sbm_remove_range(map, 2000, 2100);
		oracle_set_range(o, 2000, 2100, false);
		check_against_oracle(map, o, "rle: remove_range");
		sbm_flip_range(map, 1990, 2110);
		for (int i = 1990; i < 2110; i++)
			o->bit[i] = !o->bit[i];
		check_against_oracle(map, o, "rle: flip_range");
		sbm_free(map);
	}

	pfree(o);
}

/*
 * Split at every interesting position of a mixed RLE/sparse map: before,
 * inside and after an RLE run, on chunk boundaries, inside a sparse chunk,
 * in a gap, and at the median.
 */
static void
test_split_positions(void)
{
	static const uint64 at[] = {
		0, 50, 100, 101, 2047, 2048, 3000, 4095, 4096, 5000, 6200,
		6300, 9000, 20000, SBM_IDX_MAX,
	};

	for (int k = 0; k < lengthof(at); k++)
	{
		SbmOracle  *o = palloc0_object(SbmOracle);
		SbmOracle  *lo = palloc0_object(SbmOracle);
		SbmOracle  *hi = palloc0_object(SbmOracle);
		Sbm		   *map = sbm_create(64 * 1024);
		Sbm		   *other = sbm_create(64 * 1024);
		uint64		split;
		char		what[64];

		sbm_add_range(map, 100, 6000);	/* RLE */
		oracle_set_range(o, 100, 6000, true);
		for (int i = 6100; i < 8000; i += 3)	/* sparse */
		{
			sbm_add(map, i);
			o->bit[i] = true;
		}

		split = sbm_split(map, at[k], other);
		if (at[k] != SBM_IDX_MAX && split != at[k])
			elog(ERROR, "split: returned " UINT64_FORMAT " for " UINT64_FORMAT,
				 split, at[k]);
		for (uint64 i = 0; i < ORACLE_BITS; i++)
		{
			lo->bit[i] = o->bit[i] && i < split;
			hi->bit[i] = o->bit[i] && i >= split;
		}
		snprintf(what, sizeof(what), "split low @" UINT64_FORMAT, at[k]);
		check_against_oracle(map, lo, what);
		snprintf(what, sizeof(what), "split high @" UINT64_FORMAT, at[k]);
		check_against_oracle(other, hi, what);

		sbm_free(map);
		sbm_free(other);
		pfree(o);
		pfree(lo);
		pfree(hi);
	}

	/* split argument checking and a too-small destination */
	{
		Sbm		   *map = sbm_create_from_range(0, 5000);
		uint8		small[64] pg_attribute_aligned(8);
		Sbm			tiny;

		errno = 0;
		if (sbm_split(NULL, 1, map) != SBM_IDX_MAX || errno != EINVAL)
			elog(ERROR, "split: NULL map");
		sbm_init(&tiny, small, sizeof(small));
		for (int i = 0; i < 64; i++)
			sbm_add(map, 6000 + i * 3);
		errno = 0;
		if (sbm_split(map, 2000, &tiny) != SBM_IDX_MAX || errno != ENOSPC)
			elog(ERROR, "split: expected ENOSPC on a tiny destination");
		/* a single-member map has no median */
		sbm_clear(map);
		sbm_add(map, 9);
		sbm_clear(&tiny);
		if (sbm_split(map, SBM_IDX_MAX, &tiny) != SBM_IDX_MAX)
			elog(ERROR, "split: median of a singleton");
		sbm_free(map);
	}
}

/*
 * Every allocation lineage, the grow/shrink/re-point paths of
 * sbm_set_data_size, ENOSPC on a caller-sized buffer, and unaligned buffers.
 */
static void
test_lineages(void)
{
	SbmOracle  *o = palloc0_object(SbmOracle);

	/* caller-initialized struct and buffer, filled until ENOSPC */
	{
		uint8		buf[256] pg_attribute_aligned(8);
		Sbm			m;
		int			added = 0;

		sbm_init(&m, buf, sizeof(buf));
		errno = 0;
		for (int i = 0; i < 4000; i += 37)
		{
			if (sbm_add(&m, i) == SBM_IDX_MAX)
				break;
			o->bit[i] = true;
			added++;
		}
		if (errno != ENOSPC)
			elog(ERROR, "lineage: expected ENOSPC filling a fixed buffer");
		if (added == 0)
			elog(ERROR, "lineage: fixed buffer accepted nothing");
		check_against_oracle(&m, o, "lineage: init");
		if (sbm_capacity_remaining(&m) < 0.0 ||
			sbm_capacity_remaining(&m) > 100.0)
			elog(ERROR, "lineage: capacity_remaining out of range");

		/* shrink_to_fit leaves a caller-owned buffer alone */
		if (sbm_shrink_to_fit(&m) != &m)
			elog(ERROR, "lineage: shrink_to_fit moved a wrapped map");

		/* open adopts the bytes: same contents */
		{
			uint8		copy[256] pg_attribute_aligned(8);
			Sbm			m2;

			memcpy(copy, buf, sizeof(copy));
			sbm_open(&m2, copy, sizeof(copy));
			check_against_oracle(&m2, o, "lineage: open");

			/* corrupt the chunk count: open resets to empty */
			memset(copy, 0xff, 8);
			sbm_open(&m2, copy, sizeof(copy));
			if (!sbm_is_empty(&m2))
				elog(ERROR, "lineage: corrupt buffer was not reset");
		}
	}

	/* wrapped, then grown past the caller's buffer (wrap -> owned-split) */
	{
		uint8	   *buf = palloc0(512);
		Sbm		   *m = sbm_anchor(buf, 512);
		Sbm		   *g;

		sbm_clear(m);
		memset(o, 0, sizeof(*o));
		for (int i = 0; i < 20000; i += 11)
		{
			while (sbm_add(m, i) == SBM_IDX_MAX)
			{
				g = sbm_set_data_size(m, NULL, sbm_get_capacity(m) * 2);
				m = g;
			}
			o->bit[i] = true;
		}
		check_against_oracle(m, o, "lineage: wrap grown");

		/* owned-split: grow again, shrink, and shrink_to_fit */
		m = sbm_set_data_size(m, NULL, sbm_get_capacity(m) * 2);
		m = sbm_set_data_size(m, NULL, sbm_get_size(m) + 64);
		m = sbm_shrink_to_fit(m);
		if (sbm_get_capacity(m) < sbm_get_size(m))
			elog(ERROR, "lineage: shrink below used size");
		check_against_oracle(m, o, "lineage: owned-split resized");

		/* owned_copy normalizes the lineage */
		g = sbm_owned_copy(m);
		check_against_oracle(g, o, "lineage: owned_copy");
		sbm_free(g);
		sbm_free(m);
		/* the caller's original buffer is still theirs */
		pfree(buf);
	}

	/* wrapped and shrunk in place; re-point at a caller buffer */
	{
		uint8	   *buf = palloc0(4096);
		uint8	   *buf2 = palloc0(4096);
		Sbm		   *m = sbm_anchor(buf, 4096);

		sbm_clear(m);
		sbm_add(m, 3);
		m = sbm_set_data_size(m, NULL, 2048);
		if (sbm_get_capacity(m) != 2048 || !sbm_contains(m, 3, NULL))
			elog(ERROR, "lineage: wrapped shrink");
		memcpy(buf2, buf, sbm_get_size(m));
		m = sbm_set_data_size(m, buf2, 4096);
		if (sbm_get_data(m) != buf2 || !sbm_contains(m, 3, NULL))
			elog(ERROR, "lineage: re-point");
		sbm_free(m);
		pfree(buf);
		pfree(buf2);
	}

	/* owned-contiguous: grow, no-op resize, shrink, shrink_to_fit */
	{
		Sbm		   *m = sbm_create(64);

		sbm_add(m, 1);
		m = sbm_set_data_size(m, NULL, 8192);
		m = sbm_set_data_size(m, NULL, 8192);
		m = sbm_shrink_to_fit(m);
		m = sbm_shrink_to_fit(m);
		if (!sbm_contains(m, 1, NULL))
			elog(ERROR, "lineage: owned resize lost data");
		sbm_free(m);
	}

	/* open_copy of valid, truncated, and garbage bytes */
	{
		Sbm		   *m = sbm_create_from_range(10, 5000);
		Sbm		   *c;

		c = sbm_open_copy(sbm_get_data(m), sbm_get_size(m), 128);
		if (!sbm_equals(m, c))
			elog(ERROR, "lineage: open_copy");
		sbm_free(c);
		c = sbm_open_copy(sbm_get_data(m), sbm_get_size(m) - 1, 0);
		if (c != NULL && !sbm_validate(c))
			elog(ERROR, "lineage: open_copy of truncated bytes");
		if (sbm_open_copy(NULL, 4, 0) != NULL)
			elog(ERROR, "lineage: open_copy(NULL)");
		c = sbm_open_copy(NULL, 0, 0);
		if (c == NULL || !sbm_is_empty(c))
			elog(ERROR, "lineage: open_copy of nothing");
		sbm_free(m);
	}

	/* unaligned caller buffer: every access is unaligned-safe */
	{
		uint8	   *raw = palloc0(8192 + 8);
		uint8	   *unaligned = raw + 1;
		Sbm		   *built = sbm_create_from_range(0, 3000);
		Sbm		   *u;

		sbm_add(built, 7777);
		memcpy(unaligned, sbm_get_data(built), sbm_get_size(built));
		u = sbm_open_copy(unaligned, sbm_get_size(built), 0);
		if (!sbm_equals(u, built))
			elog(ERROR, "lineage: unaligned open_copy");
		sbm_free(u);
		sbm_free(built);
		pfree(raw);
	}

	pfree(o);
}

/*
 * Differential test of every binary operation over operands in different
 * encodings: for each pair drawn from {empty, small, sparse, RLE, mixed,
 * far}, compare every result against the oracle-computed answer.
 */
static void
test_binary_ops(void)
{
#define NSHAPES 7
	SbmOracle  *shape = palloc0_array(SbmOracle, NSHAPES);
	Sbm		   *m[NSHAPES];
	SbmOracle  *r = palloc0_object(SbmOracle);

	/* 0: empty */
	/* 1: small-set mode */
	for (int i = 0; i < 1000; i += 3)
		shape[1].bit[i] = true;
	/* 2: sparse chunks */
	for (int i = 2000; i < 30000; i += 17)
		shape[2].bit[i] = true;
	/* 3: RLE runs, aligned and unaligned */
	oracle_set_range(&shape[3], 0, 6144, true);
	oracle_set_range(&shape[3], 10001, 20500, true);
	/* 4: mixture of everything, overlapping the others */
	oracle_set_range(&shape[4], 500, 2500, true);
	for (int i = 2500; i < 12000; i += 5)
		shape[4].bit[i] = true;
	oracle_set_range(&shape[4], 15000, 40000, true);
	/* 5: a single member */
	shape[5].bit[4096] = true;
	/* 6: dense alternating pattern across many chunks */
	for (int i = 0; i < ORACLE_BITS; i += 2)
		shape[6].bit[i] = true;

	for (int s = 0; s < NSHAPES; s++)
	{
		m[s] = (s == 0) ? NULL : oracle_to_sbm(&shape[s]);
		check_against_oracle(m[s], &shape[s], "shape");
	}

	for (int a = 0; a < NSHAPES; a++)
	{
		for (int b = 0; b < NSHAPES; b++)
		{
			const		SbmOracle *oa = &shape[a];
			const		SbmOracle *ob = &shape[b];
			size_t		n_and = 0,
						n_or = 0,
						n_andnot = 0,
						n_xor = 0;
			bool		subset = true,
						superset = true,
						overlap = false;
			char		what[64];
			Sbm		   *res;
			Sbm		   *inplace;

			for (int i = 0; i < ORACLE_BITS; i++)
			{
				n_and += oa->bit[i] && ob->bit[i];
				n_or += oa->bit[i] || ob->bit[i];
				n_andnot += oa->bit[i] && !ob->bit[i];
				n_xor += oa->bit[i] != ob->bit[i];
				if (oa->bit[i] && !ob->bit[i])
					subset = false;
				if (ob->bit[i] && !oa->bit[i])
					superset = false;
				if (oa->bit[i] && ob->bit[i])
					overlap = true;
			}

#define CHECK_OP(fn, inplacefn, expr, label) \
			do { \
				for (int i = 0; i < ORACLE_BITS; i++) \
					r->bit[i] = (expr); \
				snprintf(what, sizeof(what), "%s(%d,%d)", label, a, b); \
				res = fn(m[a], m[b]); \
				check_against_oracle(res, r, what); \
				sbm_free(res); \
				if (m[a] != NULL) \
				{ \
					inplace = inplacefn(sbm_copy(m[a]), m[b]); \
					check_against_oracle(inplace, r, what); \
					sbm_free(inplace); \
				} \
			} while (0)

			CHECK_OP(sbm_union, sbm_union_inplace,
					 oa->bit[i] || ob->bit[i], "union");
			CHECK_OP(sbm_intersection, sbm_intersection_inplace,
					 oa->bit[i] && ob->bit[i], "intersection");
			CHECK_OP(sbm_difference, sbm_difference_inplace,
					 oa->bit[i] && !ob->bit[i], "difference");
			CHECK_OP(sbm_xor, sbm_xor_inplace,
					 oa->bit[i] != ob->bit[i], "xor");
#undef CHECK_OP

			if (sbm_union_cardinality(m[a], m[b]) != n_or ||
				sbm_intersection_cardinality(m[a], m[b]) != n_and ||
				sbm_difference_cardinality(m[a], m[b]) != n_andnot ||
				sbm_xor_cardinality(m[a], m[b]) != n_xor)
				elog(ERROR, "cardinality op mismatch (%d,%d)", a, b);
			if (sbm_is_subset(m[a], m[b]) != subset ||
				sbm_is_superset(m[a], m[b]) != superset ||
				sbm_overlap(m[a], m[b]) != overlap ||
				sbm_nonempty_difference(m[a], m[b]) != (n_andnot > 0) ||
				sbm_equals(m[a], m[b]) != (subset && superset))
				elog(ERROR, "predicate mismatch (%d,%d)", a, b);
			{
				SbmSubsetRelation rel = sbm_subset_compare(m[a], m[b]);
				SbmSubsetRelation want =
					(subset && superset) ? SBM_REL_EQUAL :
					subset ? SBM_REL_SUBSET_A :
					superset ? SBM_REL_SUBSET_B : SBM_REL_DIFFERENT;

				if (rel != want)
					elog(ERROR, "subset_compare(%d,%d) = %d, want %d",
						 a, b, (int) rel, (int) want);
			}
			if ((sbm_compare(m[a], m[b]) == 0) != (subset && superset) ||
				sbm_compare(m[a], m[b]) != -sbm_compare(m[b], m[a]))
				elog(ERROR, "compare mismatch (%d,%d)", a, b);
			if (subset && superset && sbm_hash(m[a]) != sbm_hash(m[b]))
				elog(ERROR, "equal maps hash differently (%d,%d)", a, b);
			{
				double		j = sbm_jaccard_index(m[a], m[b]);
				double		want = n_or ? (double) n_and / n_or : 0.0;

				if (j < want - 1e-12 || j > want + 1e-12)
					elog(ERROR, "jaccard (%d,%d)", a, b);
			}
		}
	}

	/* per-shape unary operations over the same encodings */
	for (int s = 1; s < NSHAPES; s++)
	{
		static const ssize_t shifts[] = {1, 63, 64, 2047, 2048, 5000,
		-1, -64, -2048, -5000};
		char		what[64];

		for (int k = 0; k < lengthof(shifts); k++)
		{
			Sbm		   *sh = sbm_offset(m[s], shifts[k]);

			memset(r, 0, sizeof(*r));
			for (int64 i = 0; i < ORACLE_BITS; i++)
			{
				int64		j = i - shifts[k];

				if (j >= 0 && j < ORACLE_BITS)
					r->bit[i] = shape[s].bit[j];
			}
			/* members shifted past the oracle universe are not checked */
			if (shifts[k] > 0)
			{
				Sbm		   *clip = sbm_extract_range(sh, 0, ORACLE_BITS);

				sbm_free(sh);
				sh = clip;
			}
			snprintf(what, sizeof(what), "offset(%d, %zd)", s, shifts[k]);
			check_against_oracle(sh, r, what);
			sbm_free(sh);
		}

		/* extract_range at unaligned and aligned bounds */
		{
			Sbm		   *x = sbm_extract_range(m[s], 1000, 13000);

			memset(r, 0, sizeof(*r));
			for (int i = 1000; i < 13000; i++)
				r->bit[i] = shape[s].bit[i];
			snprintf(what, sizeof(what), "extract_range(%d)", s);
			check_against_oracle(x, r, what);
			sbm_free(x);
		}

		/* membership, cached lookups, batched lookups and the locator */
		{
			SbmLocator *loc = sbm_locator_build(m[s]);
			SbmCursorCached cache = SBM_CURSOR_CACHED_INIT;
			uint64	   *q = palloc_array(uint64, ORACLE_BITS / 7 + 1);
			bool	   *res = palloc_array(bool, ORACLE_BITS / 7 + 1);
			size_t		nq = 0;

			for (int i = 0; i < ORACLE_BITS; i += 7)
				q[nq++] = i;
			sbm_contains_many(m[s], q, res, nq);
			for (size_t i = 0; i < nq; i++)
				if (res[i] != shape[s].bit[q[i]])
					elog(ERROR, "contains_many(%d) at " UINT64_FORMAT, s, q[i]);
			for (int pass = 0; pass < 2; pass++)
				for (int i = ORACLE_BITS - 1; i >= 0; i -= 13)
					if (sbm_contains_cached(m[s], i, &cache) != shape[s].bit[i] ||
						sbm_locator_contains(loc, i) != shape[s].bit[i])
						elog(ERROR, "cached/locator contains(%d) at %d", s, i);
			for (size_t k = 0; k < sbm_cardinality(m[s]); k += 101)
			{
				uint64		v = sbm_select(m[s], k, true);

				if (sbm_locator_select(loc, k, true) != v ||
					sbm_locator_rank(loc, 0, v, true) != k + 1 ||
					sbm_locator_rank(loc, 0, v, false) !=
					sbm_rank(m[s], 0, v, false) ||
					sbm_locator_select(loc, k, false) !=
					sbm_select(m[s], k, false))
					elog(ERROR, "locator rank/select(%d) at %zu", s, k);
			}
			sbm_locator_free(loc);
			pfree(q);
			pfree(res);
		}

		/* unset-bit select/rank and span, against the oracle */
		{
			size_t		zeros = 0;

			for (int i = 0; i < ORACLE_BITS; i++)
			{
				if (!shape[s].bit[i])
				{
					if (zeros % 211 == 0 &&
						sbm_select(m[s], zeros, false) != (uint64) i)
						elog(ERROR, "select(false)(%d) at %d", s, i);
					zeros++;
				}
			}
			if (sbm_rank(m[s], 0, ORACLE_BITS - 1, false) != zeros)
				elog(ERROR, "rank(false)(%d)", s);

			/* span of set and of unset bits, from several starts */
			for (int start = 0; start < ORACLE_BITS; start += 4999)
			{
				for (size_t len = 1; len <= 3000; len = len * 7 + 1)
				{
					for (int val = 0; val <= 1; val++)
					{
						uint64		want = SBM_IDX_MAX;
						uint64		got = sbm_span(m[s], start, len, val);
						size_t		run = 0;

						for (int i = start; i < ORACLE_BITS; i++)
						{
							run = (shape[s].bit[i] == (bool) val) ? run + 1 : 0;
							if (run == len)
							{
								want = i + 1 - len;
								break;
							}
						}

						/*
						 * unset spans can also start at the oracle's end,
						 * past which every bit is unset
						 */
						if (want == SBM_IDX_MAX && !val)
							want = ORACLE_BITS - run;
						if (want != SBM_IDX_MAX && got != want)
							elog(ERROR, "span(%d, %d, %zu, %d) = " UINT64_FORMAT
								 ", want " UINT64_FORMAT,
								 s, start, len, val, got, want);
					}
				}
			}
		}

		/* serialization round trip and the error paths of deserialize */
		{
			size_t		need = sbm_serialized_size(m[s]);
			uint8	   *buf = palloc(need);
			Sbm		   *back;

			if (sbm_serialize(m[s], buf, need - 1) != 0)
				elog(ERROR, "serialize into a short buffer");
			if (sbm_serialize(m[s], buf, need) != need)
				elog(ERROR, "serialize size");
			back = sbm_deserialize(buf, need);
			if (!sbm_equals(back, m[s]))
				elog(ERROR, "deserialize(%d)", s);
			sbm_free(back);
			if (sbm_deserialize(buf, 8) != NULL)
				elog(ERROR, "deserialize of a short header");
			buf[0] ^= 0xff;
			if (sbm_deserialize(buf, need) != NULL)
				elog(ERROR, "deserialize with bad magic");
			buf[0] ^= 0xff;
			buf[4]++;
			if (sbm_deserialize(buf, need) != NULL)
				elog(ERROR, "deserialize with bad version");
			buf[4]--;
			buf[5] ^= 1;
			if (sbm_deserialize(buf, need) != NULL)
				elog(ERROR, "deserialize with the wrong byte order");
			pfree(buf);
		}

		/* to_array, including a too-small output buffer */
		{
			size_t		n = 0;
			uint64	   *out;

			sbm_to_array(m[s], NULL, &n);
			if (n != sbm_cardinality(m[s]))
				elog(ERROR, "to_array size query");
			out = palloc_array(uint64, n + 1);
			n = n / 2;
			sbm_to_array(m[s], out, &n);
			for (size_t i = 0; i < n; i++)
				if (out[i] != sbm_select(m[s], i, true))
					elog(ERROR, "to_array(%d) at %zu", s, i);
			pfree(out);
		}

		/* scan with skip delivers the tail of the member sequence */
		{
			uint64		acc = 0;
			size_t		card = sbm_cardinality(m[s]);

			sbm_scan(m[s], test_sbm_scan_cb, card / 3, &acc);
			if (acc != card - card / 3)
				elog(ERROR, "scan with skip (%d)", s);
		}
	}

	for (int s = 1; s < NSHAPES; s++)
		sbm_free(m[s]);
	pfree(shape);
	pfree(r);
#undef NSHAPES
}

/*
 * Maps built directly from hand-encoded chunk streams, to reach encodings
 * the incremental paths rarely produce: sparse chunks whose low vectors are
 * flagged "unused" (reduced capacity), and RLE chunks with a capacity below
 * one chunk window.  The layout is a uint64 chunk count followed by, per
 * chunk, a uint64 start index and the chunk body.
 */
static Sbm *
raw_map(const uint64 *words, size_t nwords)
{
	Sbm		   *m = sbm_open_copy((const uint8 *) words,
								  nwords * sizeof(uint64), 4096);

	if (m == NULL || !sbm_validate(m))
		elog(ERROR, "raw_map: encoding rejected");
	return m;
}

static void
test_raw_encodings(void)
{
	SbmOracle  *o = palloc0_object(SbmOracle);

	/*
	 * Sparse chunk at 0 whose vectors 0..3 are "unused" (flag 01) and whose
	 * vector 4 is all ones (flag 11): capacity 1792, members [256, 320).
	 * Adding a member in an unused vector must first restore the capacity.
	 */
	{
		const uint64 desc = UINT64CONST(0x55) | (UINT64CONST(3) << 8);
		const uint64 enc[] = {1, 0, desc};
		Sbm		   *m = raw_map(enc, lengthof(enc));

		oracle_set_range(o, 256, 320, true);
		check_against_oracle(m, o, "raw: reduced capacity");
		sbm_add(m, 5);
		o->bit[5] = true;
		check_against_oracle(m, o, "raw: capacity restored");
		sbm_remove(m, 300);
		o->bit[300] = false;
		check_against_oracle(m, o, "raw: reduced capacity remove");
		sbm_free(m);
	}

	/*
	 * RLE chunk at 4096 with capacity 100 and length 40 (members [4096,
	 * 4136)), followed by a sparse chunk at 6144 holding one mixed vector.
	 */
	{
		const uint64 rle = UINT64CONST(0x4000000000000000) |
			(UINT64CONST(100) << 31) | 40;
		const uint64 enc[] = {2, 4096, rle, 6144, 2, 0x5};
		Sbm		   *m = raw_map(enc, lengthof(enc));
		Sbm		   *x;

		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 4096, 4136, true);
		o->bit[6144] = o->bit[6146] = true;
		check_against_oracle(m, o, "raw: short RLE");

		/* set ops and rank/select across the short RLE's unused tail */
		if (sbm_rank(m, 4130, 4200, true) != 6 ||
			sbm_rank(m, 4130, 4200, false) != 65 ||
			sbm_select(m, 40, true) != 6144)
			elog(ERROR, "raw: short RLE rank/select");
		x = sbm_union(m, m);
		check_against_oracle(x, o, "raw: union with self");
		sbm_free(x);
		x = sbm_offset(m, 1);
		if (sbm_cardinality(x) != 42 || !sbm_contains(x, 4136, NULL))
			elog(ERROR, "raw: short RLE offset");
		sbm_free(x);

		/* grow the run, then extend past its capacity, then punch it */
		sbm_add(m, 4136);
		o->bit[4136] = true;
		check_against_oracle(m, o, "raw: short RLE extended");
		sbm_add(m, 4196);
		o->bit[4196] = true;
		check_against_oracle(m, o, "raw: short RLE past capacity");
		sbm_remove(m, 4100);
		o->bit[4100] = false;
		check_against_oracle(m, o, "raw: short RLE punched");
		sbm_free(m);
	}

	/*
	 * Union that appends an RLE run contiguous with an already-emitted
	 * all-ones sparse chunk, exercising the sparse-ones to RLE in-place
	 * rewrite in sbm_append_rle_chunk.  a fills window 0 densely (becomes a
	 * sparse all-ones chunk in the output); b is a long RLE run starting at
	 * 2048 that abuts it.
	 */
	{
		Sbm		   *a = sbm_create(64 * 1024);
		Sbm		   *b = sbm_create_from_range(2048, 2048 + 6000);
		Sbm		   *x;

		for (int i = 0; i < 2048; i++)
			sbm_add(a, i);		/* window 0, all ones, sparse */
		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 0, 2048 + 6000, true);
		x = sbm_union(a, b);
		check_against_oracle(x, o, "raw: sparse-ones + RLE union");
		sbm_free(x);
		sbm_free(a);
		sbm_free(b);
	}

	/*
	 * A sparse chunk with a leading all-ones vector, scanned with a skip that
	 * lands inside that vector: the sparse ONES partial-skip path.  flags[0]
	 * = ONES (bits [0,64)), flags[1] = MIXED (bits 64, 66).
	 */
	{
		const uint64 desc = UINT64CONST(3) | (UINT64CONST(2) << 2);
		const uint64 enc[] = {1, 0, desc, 0x5};
		Sbm		   *m = raw_map(enc, lengthof(enc));
		uint64		acc = 0;

		sbm_scan(m, test_sbm_scan_cb, 30, &acc);	/* skip into [0,64) */
		if (acc != sbm_cardinality(m) - 30)
			elog(ERROR, "raw: sparse ONES skip");
		sbm_free(m);
	}

	/*
	 * sbm_scan with skips that land past whole chunks and inside a chunk,
	 * over a map mixing a long RLE run with sparse chunks, so the scan's RLE
	 * skip and per-vector ONES/MIXED skip paths all fire.  The count
	 * delivered after skipping k must be cardinality - k.
	 */
	{
		Sbm		   *m = sbm_create_from_range(0, 9000); /* RLE */

		for (int i = 9000; i < 15000; i += 3)	/* sparse tail */
			sbm_add_grow(&m, i);
		{
			size_t		card = sbm_cardinality(m);
			static const size_t skips[] = {0, 1, 2048, 5000, 9000, 9001};

			for (int k = 0; k < lengthof(skips); k++)
			{
				uint64		acc = 0;

				if (skips[k] > card)
					continue;
				sbm_scan(m, test_sbm_scan_cb, skips[k], &acc);
				if (acc != card - skips[k])
					elog(ERROR, "scan skip %zu: got " UINT64_FORMAT
						 ", want %zu", skips[k], acc, card - skips[k]);
			}
		}
		sbm_free(m);
	}

	/*
	 * Union, difference and intersection of two maps whose RLE runs overlap
	 * only partially and at offset chunk boundaries, so a run is consumed in
	 * pieces (the mid-chunk-cursor emit paths): a long run in one map spans
	 * several of the other map's shorter runs and gaps.
	 */
	{
		Sbm		   *a = sbm_create_from_range(0, 20000);
		Sbm		   *b = sbm_create(64 * 1024);
		SbmOracle  *ob = palloc0_object(SbmOracle);
		SbmOracle  *r = palloc0_object(SbmOracle);
		Sbm		   *x;

		oracle_set_range(o, 0, 20000, true);
		sbm_add_range(b, 1000, 3000);
		sbm_add_range(b, 5000, 5100);
		sbm_add_range(b, 18000, 25000);
		oracle_set_range(ob, 1000, 3000, true);
		oracle_set_range(ob, 5000, 5100, true);
		oracle_set_range(ob, 18000, 25000, true);

		for (int i = 0; i < ORACLE_BITS; i++)
			r->bit[i] = o->bit[i] || ob->bit[i];
		x = sbm_union(a, b);
		check_against_oracle(x, r, "raw: partial-run union");
		sbm_free(x);
		x = sbm_union(b, a);	/* other operand order */
		check_against_oracle(x, r, "raw: partial-run union swapped");
		sbm_free(x);

		for (int i = 0; i < ORACLE_BITS; i++)
			r->bit[i] = o->bit[i] && !ob->bit[i];
		x = sbm_difference(a, b);
		check_against_oracle(x, r, "raw: partial-run difference");
		sbm_free(x);

		for (int i = 0; i < ORACLE_BITS; i++)
			r->bit[i] = ob->bit[i] && !o->bit[i];
		x = sbm_difference(b, a);
		check_against_oracle(x, r, "raw: partial-run difference swapped");
		sbm_free(x);

		for (int i = 0; i < ORACLE_BITS; i++)
			r->bit[i] = o->bit[i] && ob->bit[i];
		x = sbm_intersection(a, b);
		check_against_oracle(x, r, "raw: partial-run intersection");
		sbm_free(x);
		sbm_free(a);
		sbm_free(b);
		pfree(ob);
		pfree(r);
		memset(o, 0, sizeof(*o));
	}

	/*
	 * sbm_offset of a map that is a single long RLE run, by a chunk-aligned
	 * amount (pure-RLE emit), by an unaligned amount (head words + RLE body +
	 * tail), and far enough negative that the whole run is clipped away or
	 * starts below 0.
	 */
	{
		static const ssize_t shifts[] = {
			2048, 4096, 100,	/* up: aligned, aligned, unaligned */
			-2048, -100,		/* down: aligned, unaligned */
			-5000, -100000,		/* down: partial clip, full clip */
		};

		for (int k = 0; k < lengthof(shifts); k++)
		{
			Sbm		   *m = sbm_create_from_range(2048, 2048 + 5000);
			Sbm		   *sh = sbm_offset(m, shifts[k]);
			char		what[64];

			memset(o, 0, sizeof(*o));
			for (int64 i = 2048; i < 2048 + 5000; i++)
			{
				int64		j = i + shifts[k];

				if (j >= 0 && j < ORACLE_BITS)
					o->bit[j] = true;
			}
			if (sh != NULL)
			{
				Sbm		   *clip = sbm_extract_range(sh, 0, ORACLE_BITS);

				snprintf(what, sizeof(what), "raw: RLE offset %zd",
						 shifts[k]);
				check_against_oracle(clip ? clip : sh, o, what);
				if (clip)
					sbm_free(clip);
				sbm_free(sh);
			}
			else
				EXPECT_TRUE(oracle_count(o) == 0);
			sbm_free(m);
		}
	}

	/*
	 * Re-adding a bit inside an all-ones sparse vector, and clearing a bit
	 * inside one, exercise the ONES arms of the chunk set/clear kernels on a
	 * genuine sparse (not RLE) chunk.  A single all-ones 64-bit vector in an
	 * otherwise sparse chunk: flags[0] = ONES, flags[1] = MIXED.
	 */
	{
		const uint64 desc = UINT64CONST(3) | (UINT64CONST(2) << 2);
		const uint64 enc[] = {1, 0, desc, 0x5};
		Sbm		   *m = raw_map(enc, lengthof(enc));

		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 0, 64, true);	/* the all-ones vector */
		o->bit[64] = o->bit[66] = true; /* the mixed vector */
		check_against_oracle(m, o, "raw: ones vector");
		sbm_add(m, 20);			/* already set: ONES no-op */
		check_against_oracle(m, o, "raw: ones re-add");
		sbm_remove(m, 20);		/* clear inside the ones vector */
		o->bit[20] = false;
		check_against_oracle(m, o, "raw: ones clear");
		sbm_free(m);
	}

	/*
	 * Set operations whose output merges adjacent runs while it is being
	 * emitted: an all-ones sparse chunk followed by a contiguous RLE chunk,
	 * and two RLE chunks that abut, must each come out as a single RLE run.
	 */
	{
		const uint64 rle_2048_3000 = UINT64CONST(0x4000000000000000) |
			(UINT64CONST(4096) << 31) | 3000;
		const uint64 ones[] = {1, 0, ~UINT64CONST(0)};	/* [0, 2048) */
		const uint64 run[] = {1, 2048, rle_2048_3000};	/* [2048, 5048) */
		const uint64 rle_0_2048 = UINT64CONST(0x4000000000000000) |
			(UINT64CONST(2048) << 31) | 2048;
		const uint64 run0[] = {1, 0, rle_0_2048};	/* [0, 2048) as RLE */
		Sbm		   *a = raw_map(ones, lengthof(ones));
		Sbm		   *b = raw_map(run, lengthof(run));
		Sbm		   *c = raw_map(run0, lengthof(run0));
		Sbm		   *r;

		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 0, 5048, true);
		r = sbm_union(a, b);
		check_against_oracle(r, o, "raw: ones + RLE union");
		if (rle_chunks(r) != 1)
			elog(ERROR, "raw: ones + RLE union is not one RLE chunk");
		sbm_free(r);
		r = sbm_union(c, b);
		check_against_oracle(r, o, "raw: RLE + RLE union");
		sbm_free(r);
		r = sbm_xor(a, b);
		check_against_oracle(r, o, "raw: ones + RLE xor");
		sbm_free(r);

		/* intersection / difference of two RLE runs that only partly meet */
		memset(o, 0, sizeof(*o));
		r = sbm_intersection(c, b);
		check_against_oracle(r, o, "raw: disjoint RLE intersection");
		sbm_free(r);
		oracle_set_range(o, 0, 2048, true);
		r = sbm_difference(c, b);
		check_against_oracle(r, o, "raw: disjoint RLE difference");
		sbm_free(r);
		sbm_free(a);
		sbm_free(b);
		sbm_free(c);
	}

	/*
	 * An RLE run [0, 3000) whose capacity reaches three windows: setting a
	 * bit past the run inside the capacity separates the chunk, with the new
	 * bit in a later window (beyond the run's own last window) or in the
	 * run's last window, and both with and without room in the buffer.
	 */
	{
		const uint64 rle = UINT64CONST(0x4000000000000000) |
			(UINT64CONST(6144) << 31) | 3000;
		const uint64 enc[] = {1, 0, rle};
		static const uint64 at[] = {3000, 3001, 3100, 4095, 4096, 4097,
		5000, 6143};

		for (int k = 0; k < lengthof(at); k++)
		{
			Sbm		   *m = raw_map(enc, lengthof(enc));
			char		what[64];

			memset(o, 0, sizeof(*o));
			oracle_set_range(o, 0, 3000, true);
			sbm_add(m, at[k]);
			o->bit[at[k]] = true;
			snprintf(what, sizeof(what), "raw: set past run @" UINT64_FORMAT,
					 at[k]);
			check_against_oracle(m, o, what);
			sbm_free(m);
		}

		/* the same separations with no spare room: ENOSPC, map unchanged */
		{
			uint64		buf[lengthof(enc)];
			Sbm			m;

			memcpy(buf, enc, sizeof(enc));
			sbm_open(&m, (uint8 *) buf, sizeof(buf));
			memset(o, 0, sizeof(*o));
			oracle_set_range(o, 0, 3000, true);
			for (int k = 0; k < lengthof(at); k++)
			{
				errno = 0;
				if (sbm_add(&m, at[k]) != SBM_IDX_MAX)
				{
					/* extending the run in place needs no room */
					o->bit[at[k]] = true;
					continue;
				}
				EXPECT_TRUE(errno == ENOSPC);
			}
			check_against_oracle(&m, o, "raw: separation without room");
			errno = 0;
			EXPECT_TRUE(sbm_remove(&m, 1500) == SBM_IDX_MAX && errno == ENOSPC);
			check_against_oracle(&m, o, "raw: unset without room");
		}
	}

	/*
	 * Union where one operand's run extends into a later window of a shared
	 * capacity span while the other's does not, so the merge advances a
	 * cursor into the middle of a chunk and later emits the remainder (the
	 * cursor-mid-chunk emit and single-side overlap arms).  a and b both
	 * declare a 4096-bit capacity at 0 but a's run is 2500, b's is 100; the
	 * window [2048, 4096) then has set bits only in a.  Each is followed by a
	 * second disjoint chunk so neither is the final chunk.
	 */
	{
		const uint64 ra = UINT64CONST(0x4000000000000000) |
			(UINT64CONST(4096) << 31) | 2500;
		const uint64 rb = UINT64CONST(0x4000000000000000) |
			(UINT64CONST(4096) << 31) | 100;
		const uint64 tail = UINT64CONST(0x4000000000000000) |
			(UINT64CONST(2048) << 31) | 50;
		const uint64 ea[] = {2, 0, ra, 8192, tail};
		const uint64 eb[] = {2, 0, rb, 10240, tail};
		Sbm		   *a = raw_map(ea, lengthof(ea));
		Sbm		   *b = raw_map(eb, lengthof(eb));
		Sbm		   *x;

		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 0, 2500, true);
		oracle_set_range(o, 8192, 8242, true);
		oracle_set_range(o, 10240, 10290, true);
		x = sbm_union(a, b);
		check_against_oracle(x, o, "raw: interleaved RLE union a");
		sbm_free(x);
		x = sbm_union(b, a);
		check_against_oracle(x, o, "raw: interleaved RLE union b");
		sbm_free(x);
		sbm_free(a);
		sbm_free(b);
		memset(o, 0, sizeof(*o));
	}

	/*
	 * Union of two RLE chunks that share a wide capacity window but whose set
	 * runs have different lengths, so part of the overlap window has set bits
	 * in only one operand (the single-run a_has / b_has arms). a: cap 4096,
	 * run 100.  b: cap 4096, run 3000.  Both at start 0.
	 */
	{
		const uint64 ra = UINT64CONST(0x4000000000000000) |
			(UINT64CONST(4096) << 31) | 100;
		const uint64 rb = UINT64CONST(0x4000000000000000) |
			(UINT64CONST(4096) << 31) | 3000;
		const uint64 ea[] = {1, 0, ra};
		const uint64 eb[] = {1, 0, rb};
		Sbm		   *a = raw_map(ea, lengthof(ea));
		Sbm		   *b = raw_map(eb, lengthof(eb));
		Sbm		   *x;

		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 0, 3000, true);
		x = sbm_union(a, b);
		check_against_oracle(x, o, "raw: wide-cap RLE union a");
		sbm_free(x);
		x = sbm_union(b, a);
		check_against_oracle(x, o, "raw: wide-cap RLE union b");
		sbm_free(x);
		sbm_free(a);
		sbm_free(b);
		memset(o, 0, sizeof(*o));
	}

	/*
	 * Set operations and min/max over sparse chunks that carry NONE
	 * (reduced-capacity) slots.  A chunk whose low vectors are unused forces
	 * the "b has no capacity here" arms of difference/intersection and the
	 * leading-NONE path of sbm_minimum.  reduced = vectors 0..1 unused (flag
	 * 01), vector 2 all-ones: members [128, 192).
	 */
	{
		const uint64 reduced_desc = UINT64CONST(0x5) | (UINT64CONST(3) << 4);
		const uint64 reduced[] = {1, 0, reduced_desc};
		const uint64 bvals[] = {130, 5000};
		Sbm		   *a = raw_map(reduced, lengthof(reduced));
		Sbm		   *b = sbm_create_from_array(bvals, lengthof(bvals));
		Sbm		   *x;

		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 128, 192, true);
		check_against_oracle(a, o, "raw: reduced min/max");
		EXPECT_TRUE(sbm_minimum(a) == 128 && sbm_maximum(a) == 191);

		x = sbm_difference(a, b);	/* b covers one bit of a's window */
		memset(o, 0, sizeof(*o));
		oracle_set_range(o, 128, 192, true);
		o->bit[130] = false;
		check_against_oracle(x, o, "raw: reduced difference");
		sbm_free(x);

		x = sbm_intersection(a, b);
		memset(o, 0, sizeof(*o));
		o->bit[130] = true;
		check_against_oracle(x, o, "raw: reduced intersection");
		sbm_free(x);

		x = sbm_difference(b, a);	/* a's NONE slots: b bits survive */
		memset(o, 0, sizeof(*o));
		o->bit[5000] = true;
		check_against_oracle(x, o, "raw: reduced difference swapped");
		sbm_free(x);
		sbm_free(a);
		sbm_free(b);
		memset(o, 0, sizeof(*o));
	}

	/* structurally invalid streams are rejected (each a distinct check) */
	{
		const uint64 misaligned[] = {1, 5, 0x3};	/* start % 2048 != 0 */
		const uint64 descending[] = {2, 4096, 0x3, 2048, 0x3};
		const uint64 overrun[] = {1, 0, 0x2};	/* missing payload word */
		const uint64 rle_too_long[] = {1, 0,
		UINT64CONST(0x4000000000000000) | (UINT64CONST(10) << 31) | 20};
		const uint64 trailing[] = {1, 0, 0x3, 0};	/* slack past the data */

		EXPECT_TRUE(sbm_open_copy((const uint8 *) misaligned,
								  sizeof(misaligned), 0) == NULL);
		EXPECT_TRUE(sbm_open_copy((const uint8 *) descending,
								  sizeof(descending), 0) == NULL);
		EXPECT_TRUE(sbm_open_copy((const uint8 *) overrun,
								  sizeof(overrun), 0) == NULL);
		EXPECT_TRUE(sbm_open_copy((const uint8 *) rle_too_long,
								  sizeof(rle_too_long), 0) == NULL);
		{
			/* n is the buffer size; bytes past the encoding are ignored */
			Sbm		   *t = sbm_open_copy((const uint8 *) trailing,
										  sizeof(trailing), 0);

			EXPECT_TRUE(t != NULL && sbm_cardinality(t) == 64);
			sbm_free(t);
		}
	}

	pfree(o);
}

/*
 * ENOSPC on a caller-sized buffer: every mutator that can need more space
 * must fail with ENOSPC and leave the map exactly as it was, so the caller
 * can grow and retry.  Each case shrinks the buffer to the bytes in use and
 * then attempts an operation that needs more.
 */
static void
expect_enospc_unchanged(Sbm *m, const char *what, bool ok)
{
	if (ok || errno != ENOSPC)
		elog(ERROR, "%s: expected ENOSPC", what);
	if (!sbm_validate(m))
		elog(ERROR, "%s: map invalid after ENOSPC", what);
}

static Sbm *
tight_copy(const Sbm *src, uint8 **bufp)
{
	size_t		n = sbm_get_size(src);
	uint8	   *buf = palloc(n);
	Sbm		   *m;

	memcpy(buf, sbm_get_data(src), n);
	m = sbm_anchor(buf, n);
	sbm_open(m, buf, n);
	*bufp = buf;
	return m;
}

static void
test_enospc(void)
{
	Sbm		   *src;
	Sbm		   *m;
	uint8	   *buf;
	size_t		card;

	/* small-set mode: a new word does not fit */
	src = sbm_create(0);
	sbm_add(src, 3);
	m = tight_copy(src, &buf);
	errno = 0;
	expect_enospc_unchanged(m, "small add", sbm_add(m, 700) != SBM_IDX_MAX);
	EXPECT_TRUE(sbm_cardinality(m) == 1);
	/* ... nor does the promotion to chunk mode */
	expect_enospc_unchanged(m, "small promote",
							sbm_add(m, 100000) != SBM_IDX_MAX);
	sbm_free(m);
	pfree(buf);

	/* chunk mode: a new chunk, a new payload word, splitting an RLE run */
	sbm_clear(src);
	src = sbm_set_data_size(src, NULL, 8192);
	sbm_add_range(src, 0, 10000);
	sbm_add(src, 20001);
	card = sbm_cardinality(src);
	m = tight_copy(src, &buf);
	errno = 0;
	expect_enospc_unchanged(m, "new chunk", sbm_add(m, 50000) != SBM_IDX_MAX);
	expect_enospc_unchanged(m, "new word", sbm_add(m, 20100) != SBM_IDX_MAX);
	expect_enospc_unchanged(m, "RLE separate",
							sbm_remove(m, 5000) != SBM_IDX_MAX);
	expect_enospc_unchanged(m, "add_range",
							sbm_add_range(m, 30000, 30005));
	expect_enospc_unchanged(m, "remove_range",
							sbm_remove_range(m, 4000, 4002));
	expect_enospc_unchanged(m, "flip_range", sbm_flip_range(m, 4000, 4002));
	{
		const uint64 arr[] = {40000, 60000, 80000};

		expect_enospc_unchanged(m, "add_many",
								sbm_add_many(m, arr, lengthof(arr)));
	}
	{
		uint8		obuf[16] pg_attribute_aligned(8);
		Sbm			o;

		sbm_init(&o, obuf, sizeof(obuf));
		expect_enospc_unchanged(m, "split", sbm_split(m, 9000, &o) !=
								SBM_IDX_MAX);
	}
	EXPECT_TRUE(sbm_cardinality(m) == card);

	/* the grow variants recover from every one of those */
	{
		SbmCursor	cur = SBM_CURSOR_INIT;
		const uint64 arr[] = {40000, 60000, 80000};
		Sbm		   *g = sbm_owned_copy(m);

		sbm_free(m);
		pfree(buf);
		g = sbm_shrink_to_fit(g);
		EXPECT_TRUE(sbm_add_grow(&g, 50000) == 50000);
		g = sbm_shrink_to_fit(g);
		EXPECT_TRUE(sbm_add_grow_cursor(&g, 90000, &cur) == 90000);
		EXPECT_TRUE(sbm_add_grow_cursor(&g, 90001, &cur) == 90001);
		g = sbm_shrink_to_fit(g);
		EXPECT_TRUE(sbm_add_many_grow(&g, arr, lengthof(arr)));
		EXPECT_TRUE(sbm_cardinality(g) == card + 6);
		EXPECT_TRUE(sbm_validate(g));
		sbm_free(g);
	}

	/* create_from_range of a huge range takes the grow-and-retry path */
	{
		Sbm		   *r = sbm_create_from_range(5, UINT64CONST(1) << 33);

		EXPECT_TRUE(sbm_cardinality(r) == (UINT64CONST(1) << 33) - 5);
		EXPECT_TRUE(sbm_validate(r));
		sbm_free(r);
	}
	sbm_free(src);
}

/*
 * Empty and degenerate inputs, argument validation, locator staleness, and
 * every rejection branch of sbm_validate (driven with hand-built structs,
 * which SBM_EXPOSE_STRUCT makes possible).
 */
static void
test_edges(void)
{
	Sbm		   *empty = sbm_create(0);
	Sbm		   *chunked = sbm_create_from_range(5000, 9000);
	bool		res[2];
	uint64		q[2] = {1, 2};

	/* empty (non-NULL) maps, small and chunk mode */
	EXPECT_TRUE(sbm_membership(empty) == SBM_EMPTY);
	EXPECT_TRUE(sbm_singleton_member(empty) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_minimum(empty) == 0 && sbm_maximum(empty) == 0);
	EXPECT_TRUE(sbm_fill_factor(empty) == 0.0);
	EXPECT_TRUE(sbm_next_member(empty, SBM_IDX_MAX, NULL) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_prev_member(empty, SBM_IDX_MAX, NULL) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_pop_first(empty) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_pop_last(empty) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_rank(empty, 9, 3, true) == 0);	/* begin > end */
	EXPECT_TRUE(sbm_select(empty, 7, false) == 7);
	EXPECT_TRUE(sbm_span(empty, 3, 0, true) == 3);	/* zero length */
	EXPECT_TRUE(sbm_span(empty, 3, 5, false) == 3);
	EXPECT_TRUE(sbm_locator_build(empty) == NULL);
	{
		Sbm		   *small = sbm_create_singleton(3);
		SbmLocator *loc = sbm_locator_build(small); /* degenerate locator */

		EXPECT_TRUE(loc != NULL && sbm_locator_contains(loc, 3));
		EXPECT_TRUE(sbm_locator_select(loc, 0, true) == 3);
		sbm_locator_free(loc);
		sbm_free(small);
	}
	EXPECT_TRUE(sbm_serialized_size(empty) > 0);
	EXPECT_TRUE(sbm_offset(empty, 7) == NULL);
	EXPECT_TRUE(sbm_offset(chunked, 0) != NULL);	/* zero shift: a copy */
	EXPECT_TRUE(sbm_offset(chunked, -100000) == NULL);	/* all below 0 */
	{
		/* a member near the top of the index space cannot shift up */
		Sbm		   *top = sbm_create_singleton(SBM_IDX_MAX - 10);

		errno = 0;
		EXPECT_TRUE(sbm_offset(top, 20) == NULL && errno == ERANGE);
		sbm_free(top);
	}
	EXPECT_TRUE(sbm_intersection(empty, chunked) == NULL);
	EXPECT_TRUE(sbm_difference(empty, chunked) == NULL);
	EXPECT_TRUE(sbm_union_inplace(empty, NULL) == empty);
	EXPECT_TRUE(sbm_intersection_inplace(empty, chunked) == empty);
	EXPECT_TRUE(sbm_intersection_inplace(NULL, chunked) == NULL);
	EXPECT_TRUE(sbm_difference_inplace(NULL, chunked) == NULL);
	{
		Sbm		   *x = sbm_xor_inplace(sbm_create(0), chunked);

		EXPECT_TRUE(sbm_equals(x, chunked));
		sbm_free(x);
	}
	EXPECT_TRUE(sbm_flip_range(empty, 9, 9));
	EXPECT_TRUE(sbm_remove_range(empty, 9, 3));
	EXPECT_TRUE(sbm_add_many(empty, q, 1));
	EXPECT_TRUE(sbm_add_many_grow(&empty, q, 0));
	EXPECT_TRUE(sbm_create_from_array(NULL, 3) == NULL);
	EXPECT_TRUE(sbm_cardinality(sbm_create_from_array(q, 2)) == 2);

	/* chunk-mode emptiness: clear a promoted map */
	sbm_clear(chunked);
	EXPECT_TRUE(sbm_is_empty(chunked));
	EXPECT_TRUE(sbm_minimum(chunked) == 0 && sbm_maximum(chunked) == 0);
	EXPECT_TRUE(sbm_rank(chunked, 0, 9, false) == 10);
	EXPECT_TRUE(sbm_locator_build(chunked) == NULL);
	sbm_contains_many(chunked, q, res, 2);
	EXPECT_TRUE(!res[0] && !res[1]);

	/* argument validation */
	errno = 0;
	sbm_contains_many(empty, NULL, res, 2);
	EXPECT_TRUE(errno == EINVAL);
	sbm_contains_many(empty, q, res, 0);
	sbm_contains_many(NULL, q, res, 2);
	EXPECT_TRUE(!res[0] && !res[1]);
	errno = 0;
	sbm_init(NULL, NULL, 0);
	EXPECT_TRUE(errno == EINVAL);
	errno = 0;
	sbm_open(NULL, NULL, 0);
	EXPECT_TRUE(errno == EINVAL);
	EXPECT_TRUE(sbm_add_grow_cursor(NULL, 1, NULL) == SBM_IDX_MAX);
	{
		Sbm		   *g = NULL;

		EXPECT_TRUE(sbm_add_grow_cursor(&g, 7, NULL) == 7);
		sbm_free(g);
	}
	EXPECT_TRUE(sbm_serialize(empty, NULL, 0) == 0);
	EXPECT_TRUE(sbm_capacity_remaining(NULL) == 0.0);

	/* locator: stale after each kind of mutation, still correct */
	{
		Sbm		   *m = sbm_create_from_range(0, 50000);
		SbmLocator *loc;

		for (int i = 60000; i < 200000; i += 4096)
			sbm_add_grow(&m, i);
		loc = sbm_locator_build(m);
		sbm_add_grow(&m, 300000);	/* chunk count changes */
		EXPECT_TRUE(sbm_locator_contains(loc, 300000));
		sbm_locator_free(loc);

		loc = sbm_locator_build(m);
		sbm_remove(m, 0);		/* first chunk start may move */
		sbm_add(m, 0);
		EXPECT_TRUE(sbm_locator_rank(loc, 0, 100, true) ==
					sbm_rank(m, 0, 100, true));
		sbm_locator_free(loc);

		loc = sbm_locator_build(m);
		sbm_remove(m, 300000);	/* last chunk vanishes */
		sbm_add(m, 400000);
		EXPECT_TRUE(sbm_locator_select(loc, 3, true) ==
					sbm_select(m, 3, true));
		sbm_locator_free(loc);
		sbm_free(m);
	}

	/* hand-built structs for each sbm_validate rejection */
	{
		uint64		buf[8] = {0};
		Sbm			v;

		v.m_data = (uint8 *) buf;
		v.m_card_plus1 = 0;
		v.m_capacity = sizeof(buf);
		v.m_data_used = 0;
		EXPECT_TRUE(sbm_validate(&v));	/* never initialized */
		v.m_data_used = sizeof(buf) + 8;
		EXPECT_TRUE(!sbm_validate(&v)); /* used > capacity */
		v.m_data_used = 4;
		EXPECT_TRUE(!sbm_validate(&v)); /* shorter than the header */
		v.m_data = NULL;
		EXPECT_TRUE(!sbm_validate(&v)); /* no buffer */
		v.m_data = (uint8 *) buf;

		/* small mode: too many words, wrong size, trailing zero word */
		buf[0] = (UINT64CONST(1) << 63) | 99;
		v.m_data_used = 16;
		EXPECT_TRUE(!sbm_validate(&v));
		buf[0] = (UINT64CONST(1) << 63) | 1;
		v.m_data_used = 24;
		EXPECT_TRUE(!sbm_validate(&v));
		buf[1] = 0;
		v.m_data_used = 16;
		EXPECT_TRUE(!sbm_validate(&v));

		/* chunk mode: zero chunks but trailing bytes */
		buf[0] = 0;
		v.m_data_used = 16;
		EXPECT_TRUE(!sbm_validate(&v));

		/* a chunk whose header runs past the data */
		buf[0] = 1;
		v.m_data_used = 16;
		EXPECT_TRUE(!sbm_validate(&v));

		/* overlapping chunk spans: RLE with capacity past the next start */
		buf[0] = 2;
		buf[1] = 0;
		buf[2] = UINT64CONST(0x4000000000000000) | (UINT64CONST(4096) << 31) | 10;
		buf[3] = 2048;
		buf[4] = 3;
		v.m_data_used = 40;
		EXPECT_TRUE(!sbm_validate(&v));

		/* a chunk whose span would wrap the index space */
		buf[0] = 1;
		buf[1] = SBM_IDX_MAX - 2047;
		buf[2] = UINT64CONST(0x4000000000000000) | (UINT64CONST(4096) << 31) | 10;
		v.m_data_used = 24;
		EXPECT_TRUE(!sbm_validate(&v));

		/* sbm_open of each of these resets to the empty set */
		sbm_open(&v, (uint8 *) buf, sizeof(buf));
		EXPECT_TRUE(sbm_is_empty(&v) && sbm_validate(&v));
		buf[0] = (UINT64CONST(1) << 63) | 99;
		sbm_open(&v, (uint8 *) buf, sizeof(buf));
		EXPECT_TRUE(sbm_is_empty(&v) && sbm_validate(&v));
		EXPECT_TRUE(sbm_capacity_remaining(&v) > 0.0);
		v.m_data_used = sbm_get_capacity(&v);
		EXPECT_TRUE(sbm_capacity_remaining(&v) == 0.0);
	}

	/* deserialize: a valid header over a corrupt body */
	{
		Sbm		   *m = sbm_create_from_range(0, 3000);
		size_t		need = sbm_serialized_size(m);
		uint8	   *buf = palloc(need);

		sbm_serialize(m, buf, need);
		buf[need - 1] ^= 0x40;	/* flip a descriptor bit */
		buf[16] = 0x7f;			/* absurd chunk count */
		EXPECT_TRUE(sbm_deserialize(buf, need) == NULL);
		pfree(buf);
		sbm_free(m);
	}

	sbm_free(empty);
	sbm_free(chunked);
}

/* The NULL-map contract: every entry point accepts NULL as the empty set. */
static void
test_null_contract(void)
{
	SbmStats	st;
	size_t		n = 7;

	errno = 0;
	EXPECT_TRUE(sbm_add(NULL, 1) == SBM_IDX_MAX);
	EXPECT_TRUE(errno == EINVAL);
	EXPECT_TRUE(sbm_remove(NULL, 1) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_assign(NULL, 1, true) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_add_grow(NULL, 1) == SBM_IDX_MAX);
	EXPECT_TRUE(!sbm_add_many(NULL, NULL, 0));
	EXPECT_TRUE(!sbm_add_many_grow(NULL, NULL, 0));
	EXPECT_TRUE(sbm_pop_first(NULL) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_pop_last(NULL) == SBM_IDX_MAX);
	EXPECT_TRUE(!sbm_contains(NULL, 1, NULL));
	EXPECT_TRUE(!sbm_contains_cached(NULL, 1, NULL));
	EXPECT_TRUE(sbm_cardinality(NULL) == 0);
	EXPECT_TRUE(sbm_is_empty(NULL));
	EXPECT_TRUE(sbm_minimum(NULL) == 0);
	EXPECT_TRUE(sbm_maximum(NULL) == 0);
	EXPECT_TRUE(sbm_fill_factor(NULL) == 0.0);
	EXPECT_TRUE(sbm_get_size(NULL) == 0);
	EXPECT_TRUE(sbm_get_capacity(NULL) == 0);
	EXPECT_TRUE(sbm_get_data(NULL) == NULL);
	EXPECT_TRUE(sbm_membership(NULL) == SBM_EMPTY);
	EXPECT_TRUE(sbm_singleton_member(NULL) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_next_member(NULL, SBM_IDX_MAX, NULL) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_prev_member(NULL, SBM_IDX_MAX, NULL) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_rank(NULL, 0, 9, true) == 0);
	EXPECT_TRUE(sbm_rank(NULL, 0, 9, false) == 10);
	EXPECT_TRUE(sbm_select(NULL, 3, true) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_select(NULL, 3, false) == 3);
	EXPECT_TRUE(sbm_span(NULL, 0, 1, true) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_locator_build(NULL) == NULL);
	EXPECT_TRUE(!sbm_locator_contains(NULL, 1));
	EXPECT_TRUE(sbm_locator_rank(NULL, 0, 9, true) == 0);
	EXPECT_TRUE(sbm_locator_select(NULL, 0, true) == SBM_IDX_MAX);
	EXPECT_TRUE(sbm_copy(NULL) == NULL);
	EXPECT_TRUE(sbm_owned_copy(NULL) == NULL);
	EXPECT_TRUE(sbm_offset(NULL, 5) == NULL);
	EXPECT_TRUE(sbm_extract_range(NULL, 0, 9) == NULL);
	EXPECT_TRUE(sbm_set_data_size(NULL, NULL, 8) == NULL);
	EXPECT_TRUE(sbm_shrink_to_fit(NULL) == NULL);
	EXPECT_TRUE(sbm_union_inplace(NULL, NULL) == NULL);
	EXPECT_TRUE(sbm_xor_inplace(NULL, NULL) == NULL);
	EXPECT_TRUE(sbm_union(NULL, NULL) == NULL);
	EXPECT_TRUE(sbm_xor(NULL, NULL) == NULL);
	EXPECT_TRUE(sbm_equals(NULL, NULL));
	EXPECT_TRUE(sbm_compare(NULL, NULL) == 0);
	EXPECT_TRUE(sbm_jaccard_index(NULL, NULL) == 0.0);
	EXPECT_TRUE(sbm_validate(NULL));
	sbm_to_array(NULL, NULL, &n);
	if (n != 0)
		elog(ERROR, "NULL to_array");
	sbm_statistics(NULL, &st);
	sbm_statistics(NULL, NULL);
	sbm_scan(NULL, test_sbm_scan_cb, 0, NULL);
	sbm_clear(NULL);
	sbm_free(NULL);
	sbm_locator_free(NULL);
	if (sbm_hash(NULL) != sbm_hash(sbm_create(0)))
		elog(ERROR, "hash of NULL differs from hash of an empty map");
}

/*
 * Embedded-map API: sbm_anchor / sbm_reanchor and the *_embedded growing
 * entry points, driven exactly as Bitmapset will drive them -- an Sbm held
 * by value inside a palloc'd node, immediately followed by a flexible data[]
 * buffer that the map is anchored into.  Growth repallocs the whole node and
 * reanchors.  Every step is checked against the bool-array oracle.
 */
typedef struct EmbedNode
{
	uint32		tag;			/* stand-in for a NodeTag, to offset data[] */
	Sbm			sbm;
	uint8		data[FLEXIBLE_ARRAY_MEMBER];
}			EmbedNode;

#define EMBED_SBM_OFF	offsetof(EmbedNode, sbm)
#define EMBED_OFFSET	offsetof(EmbedNode, data)

static EmbedNode *
embed_new(size_t cap)
{
	EmbedNode  *n;

	/* an sbm needs room for at least its 8-byte header */
	if (cap < 8)
		cap = 8;
	n = (EmbedNode *) palloc0(EMBED_OFFSET + cap);
	n->tag = 0x5a5a5a5a;
	sbm_init(&n->sbm, n->data, cap);
	return n;
}

/* Build an EmbedNode holding exactly the oracle's members. */
static EmbedNode *
embed_from_oracle(const SbmOracle * o)
{
	EmbedNode  *n = embed_new(64);

	for (int i = 0; i < ORACLE_BITS; i++)
		if (o->bit[i])
			n = sbm_add_grow_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, (uint64) i);
	return n;
}

static void
test_embedded(void)
{
	SbmOracle  *o = palloc0_object(SbmOracle);
	SbmOracle  *ob = palloc0_object(SbmOracle);
	SbmOracle  *r = palloc0_object(SbmOracle);

	/* add_grow_embedded: build a wide set one member at a time */
	{
		EmbedNode  *n = embed_new(0);

		for (int i = 0; i < 20000; i += 7)
		{
			n = sbm_add_grow_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, (uint64) i);
			o->bit[i] = true;
		}
		/* adding an existing member must not change anything or grow */
		n = sbm_add_grow_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, 7);
		check_against_oracle(&n->sbm, o, "embedded: add_grow");
		Assert(n->tag == 0x5a5a5a5a);	/* node header survived every grow */

		/* reanchor after an unrelated move (simulate a node copy) */
		{
			size_t		sz = EMBED_OFFSET + sbm_get_capacity(&n->sbm);
			EmbedNode  *copy = (EmbedNode *) palloc(sz);

			memcpy(copy, n, sz);
			sbm_reanchor(&copy->sbm, copy->data);
			check_against_oracle(&copy->sbm, o, "embedded: reanchor");
			pfree(copy);
		}
		pfree(n);
	}

	/* add_range_embedded: a long run that forces several grows */
	{
		EmbedNode  *n = embed_new(0);

		memset(o, 0, sizeof(*o));
		n = sbm_add_range_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, 100, 9000);
		oracle_set_range(o, 100, 9000, true);
		check_against_oracle(&n->sbm, o, "embedded: add_range");
		/* a second, disjoint range */
		n = sbm_add_range_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, 12000, 12010);
		oracle_set_range(o, 12000, 12010, true);
		check_against_oracle(&n->sbm, o, "embedded: add_range 2");
		pfree(n);
	}

	/*
	 * The three *_into_embedded ops over operands in several encodings
	 * (small, sparse, RLE, mixed), each checked against the oracle.  dst is
	 * an embedded node; src is a plain owned sbm.
	 */
	{
		static const struct
		{
			int			lo,
						hi,
						step;
		}			shapes[] = {
			{0, 400, 3},		/* small-ish, sparse */
			{0, 9000, 1},		/* long run -> RLE */
			{5000, 30000, 13},	/* sparse, higher */
			{8000, 15000, 1},	/* run overlapping the above */
		};

		for (int ai = 0; ai < lengthof(shapes); ai++)
		{
			for (int bi = 0; bi < lengthof(shapes); bi++)
			{
				Sbm		   *src;
				EmbedNode  *n;

				memset(o, 0, sizeof(*o));
				memset(ob, 0, sizeof(*ob));
				for (int i = shapes[ai].lo; i < shapes[ai].hi; i += shapes[ai].step)
					o->bit[i] = true;
				for (int i = shapes[bi].lo; i < shapes[bi].hi; i += shapes[bi].step)
					ob->bit[i] = true;
				src = oracle_to_sbm(ob);

				/* union */
				n = embed_from_oracle(o);
				n = sbm_union_into_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, src);
				for (int i = 0; i < ORACLE_BITS; i++)
					r->bit[i] = o->bit[i] || ob->bit[i];
				check_against_oracle(&n->sbm, r, "embedded: union");
				pfree(n);

				/* intersection */
				n = embed_from_oracle(o);
				n = sbm_intersection_into_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, src);
				for (int i = 0; i < ORACLE_BITS; i++)
					r->bit[i] = o->bit[i] && ob->bit[i];
				check_against_oracle(&n->sbm, r, "embedded: intersection");
				pfree(n);

				/* difference */
				n = embed_from_oracle(o);
				n = sbm_difference_into_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, src);
				for (int i = 0; i < ORACLE_BITS; i++)
					r->bit[i] = o->bit[i] && !ob->bit[i];
				check_against_oracle(&n->sbm, r, "embedded: difference");
				pfree(n);

				sbm_free(src);
			}
		}
	}

	/* into ops with an empty src (union/difference no-op) and empty dst */
	{
		EmbedNode  *n = embed_from_oracle(o);
		Sbm		   *empty = sbm_create(0);

		memset(o, 0, sizeof(*o));
		for (int i = 100; i < 2000; i += 5)
			o->bit[i] = true;
		pfree(n);
		n = embed_from_oracle(o);
		n = sbm_union_into_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, empty);
		check_against_oracle(&n->sbm, o, "embedded: union empty src");
		n = sbm_difference_into_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, empty);
		check_against_oracle(&n->sbm, o, "embedded: difference empty src");
		/* intersection with empty src empties dst */
		n = sbm_intersection_into_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, empty);
		if (!sbm_is_empty(&n->sbm))
			elog(ERROR, "embedded: intersection with empty did not clear");
		sbm_free(empty);
		pfree(n);
	}

	/*
	 * Intersection and difference whose result is wholly empty, so the
	 * underlying sbm_* returns NULL (not an empty map): the result == NULL
	 * arm of the embedded replace path.
	 */
	{
		EmbedNode  *n;
		Sbm		   *disjoint;

		memset(o, 0, sizeof(*o));
		memset(ob, 0, sizeof(*ob));
		for (int i = 0; i < 2000; i += 3)
			o->bit[i] = true;
		for (int i = 1; i < 2000; i += 3)	/* disjoint from o */
			ob->bit[i] = true;
		disjoint = oracle_to_sbm(ob);

		n = embed_from_oracle(o);
		n = sbm_intersection_into_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET,
										   disjoint);
		if (!sbm_is_empty(&n->sbm))
			elog(ERROR, "embedded: disjoint intersection not empty");
		pfree(n);

		/* difference that removes everything (a minus a-superset) */
		n = embed_from_oracle(o);
		{
			Sbm		   *same = oracle_to_sbm(o);

			n = sbm_difference_into_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET,
											 same);
			sbm_free(same);
		}
		if (!sbm_is_empty(&n->sbm))
			elog(ERROR, "embedded: self-difference not empty");
		pfree(n);
		sbm_free(disjoint);
	}

	/*
	 * Intersection and difference with an empty (but stored) embedded dst:
	 * sbm_intersection / sbm_difference return NULL, exercising the result ==
	 * NULL arm of the embedded replace path.
	 */
	{
		EmbedNode  *n = embed_new(64);
		Sbm		   *src;

		memset(ob, 0, sizeof(*ob));
		for (int i = 0; i < 500; i += 2)
			ob->bit[i] = true;
		src = oracle_to_sbm(ob);
		n = sbm_intersection_into_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, src);
		if (!sbm_is_empty(&n->sbm))
			elog(ERROR, "embedded: empty-dst intersection not empty");
		/* empty dst minus non-empty src -> sbm_difference returns NULL */
		n = sbm_difference_into_embedded(n, EMBED_SBM_OFF, EMBED_OFFSET, src);
		if (!sbm_is_empty(&n->sbm))
			elog(ERROR, "embedded: empty-dst difference not empty");
		sbm_free(src);
		pfree(n);
	}

	/*
	 * Small/small comparator fast paths with operands spanning a different
	 * number of words, which exercises the "extra words in the longer map"
	 * tail loops of sbm_small_equals / sbm_small_subset_compare.  All three
	 * operands here stay in small (word-array) mode.
	 */
	{
		Sbm		   *lo = sbm_create_singleton(1);	/* word 0 only, nwords 1 */
		Sbm		   *hi = sbm_create_singleton(1);	/* grows to span word 1 */
		Sbm		   *lo2 = sbm_create_singleton(1);

		sbm_add(hi, 65);		/* bit 65 -> word 1, nwords 2 */

		/* equals: differing spans must be unequal, order-independent */
		if (sbm_equals(lo, hi) || sbm_equals(hi, lo))
			elog(ERROR, "embedded: small equals span mismatch");
		if (!sbm_equals(lo, lo2))
			elog(ERROR, "embedded: small equals same");

		/* subset: lo (just {1}) is a strict subset of hi ({1,200}) */
		if (sbm_subset_compare(lo, hi) != SBM_REL_SUBSET_A)
			elog(ERROR, "embedded: small subset a<b");
		if (sbm_subset_compare(hi, lo) != SBM_REL_SUBSET_B)
			elog(ERROR, "embedded: small subset b<a");
		if (sbm_subset_compare(lo, lo2) != SBM_REL_EQUAL)
			elog(ERROR, "embedded: small subset equal");

		/* overlap: they share bit 1 */
		if (!sbm_overlap(lo, hi))
			elog(ERROR, "embedded: small overlap");

		sbm_free(lo);
		sbm_free(hi);
		sbm_free(lo2);
	}

	pfree(o);
	pfree(ob);
	pfree(r);
}

/*
 * test_sbm_internals: run all of the above.  Returns true; any failure is
 * reported as an ERROR naming the step.
 */
PG_FUNCTION_INFO_V1(test_sbm_internals);
Datum
test_sbm_internals(PG_FUNCTION_ARGS)
{
	test_null_contract();
	test_small_transitions();
	test_rle_transitions();
	test_split_positions();
	test_raw_encodings();
	test_enospc();
	test_edges();
	test_embedded();
	test_lineages();
	test_binary_ops();
	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_sbm_benchmark);

/* Fill `out` with n distinct sorted indices in [0, universe), seeded. */
static uint64 *
bench_make_indices(int64 n, int64 universe, int64 seed, int64 *n_out)
{
	pg_prng_state rng;
	Sbm		   *seen = sbm_create(256); /* dedup via a throwaway sbm */
	uint64	   *idx;
	int64		got = 0;

	if (universe < n)
		n = universe;			/* cannot draw more distinct than universe */
	idx = (uint64 *) palloc(sizeof(uint64) * (n > 0 ? n : 1));
	pg_prng_seed(&rng, (uint64) seed);
	while (got < n)
	{
		uint64		v = (uint64) (pg_prng_uint64(&rng) % (uint64) universe);

		if (sbm_contains(seen, v, NULL))
			continue;
		sbm_add_grow(&seen, v);
		idx[got++] = v;
	}
	sbm_free(seen);
	*n_out = got;
	return idx;
}

/* sbm scan callback: count yielded set-bit indices into a volatile sink. */
static void
bench_sbm_scan_cb(uint64 vec[], size_t nn, void *aux)
{
	volatile uint64 *sink = (volatile uint64 *) aux;

	for (size_t i = 0; i < nn; i++)
		*sink += vec[i];
}

/*
 * Repetition count so the fast structures (sbm especially) run long enough
 * that the nanosecond clock's granularity is negligible.  Each rep rebuilds
 * from scratch, so build timing is honest.  Scaled down for large n so the
 * whole sweep stays interactive.
 */
#define BENCH_MIN_TOTAL_OPS 200000

Datum
test_sbm_benchmark(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int64		n = PG_ARGISNULL(0) ? 10000 : PG_GETARG_INT64(0);
	int64		universe = PG_ARGISNULL(1) ? 1000000 : PG_GETARG_INT64(1);
	int64		seed = PG_ARGISNULL(2) ? 42 : PG_GETARG_INT64(2);
	uint64	   *idx;
	int64		nn;
	int64		reps;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query;
	MemoryContext old;
	instr_time	t0,
				t1;
	volatile uint64 sink = 0;

	/* SRF boilerplate */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		(rsinfo->allowedModes & SFRM_Materialize) == 0)
		elog(ERROR, "set-valued function called in context that cannot accept a set");
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	per_query = rsinfo->econtext->ecxt_per_query_memory;
	old = MemoryContextSwitchTo(per_query);
	tupdesc = CreateTupleDescCopy(tupdesc);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(old);

	if (universe <= 0 || n < 0)
		elog(ERROR, "n must be >= 0 and universe > 0");
	if (universe > (int64) INT_MAX)
		elog(ERROR, "universe must fit int for the Bitmapset comparison");

	idx = bench_make_indices(n, universe, seed, &nn);
	reps = nn > 0 ? (BENCH_MIN_TOTAL_OPS / nn) : 1;
	if (reps < 1)
		reps = 1;

#define PER_OP(total_ns) \
	((nn > 0 && reps > 0) ? (double) (total_ns) / ((double) nn * (double) reps) : 0.0)
#define EMIT(structname, build_total_ns, scan_total_ns, bytes) \
	do { \
		Datum	vals[4]; \
		bool	nulls[4] = {false, false, false, false}; \
		HeapTuple tup; \
		vals[0] = CStringGetTextDatum(structname); \
		vals[1] = Float8GetDatum(PER_OP(build_total_ns)); \
		vals[2] = Float8GetDatum(PER_OP(scan_total_ns)); \
		vals[3] = Int64GetDatum((int64) (bytes)); \
		tup = heap_form_tuple(tupdesc, vals, nulls); \
		tuplestore_puttuple(tupstore, tup); \
	} while (0)

	/* ---- sbm ---- */
	{
		int64		build_ns = 0,
					scan_ns = 0;
		int64		bytes = 0;

		for (int64 r = 0; r < reps; r++)
		{
			Sbm		   *m = sbm_create(256);

			INSTR_TIME_SET_CURRENT(t0);
			for (int64 i = 0; i < nn; i++)
				sbm_add_grow(&m, idx[i]);
			INSTR_TIME_SET_CURRENT(t1);
			INSTR_TIME_SUBTRACT(t1, t0);
			build_ns += INSTR_TIME_GET_NANOSEC(t1);
			if (r == 0)
				bytes = m ? (int64) sbm_get_size(m) : 0;

			INSTR_TIME_SET_CURRENT(t0);
			sbm_scan(m, bench_sbm_scan_cb, 0, (void *) &sink);
			INSTR_TIME_SET_CURRENT(t1);
			INSTR_TIME_SUBTRACT(t1, t0);
			scan_ns += INSTR_TIME_GET_NANOSEC(t1);
			if (m)
				sbm_free(m);
		}
		EMIT("sbm", build_ns, scan_ns, bytes);
	}

	/* ---- sbm (bulk build via sbm_add_many_grow: sort + cursor, O(N log N)) ---- */
	{
		int64		build_ns = 0,
					scan_ns = 0;
		int64		bytes = 0;

		for (int64 r = 0; r < reps; r++)
		{
			Sbm		   *m = sbm_create(256);

			INSTR_TIME_SET_CURRENT(t0);
			sbm_add_many_grow(&m, idx, (size_t) nn);
			INSTR_TIME_SET_CURRENT(t1);
			INSTR_TIME_SUBTRACT(t1, t0);
			build_ns += INSTR_TIME_GET_NANOSEC(t1);
			if (r == 0)
				bytes = m ? (int64) sbm_get_size(m) : 0;

			INSTR_TIME_SET_CURRENT(t0);
			sbm_scan(m, bench_sbm_scan_cb, 0, (void *) &sink);
			INSTR_TIME_SET_CURRENT(t1);
			INSTR_TIME_SUBTRACT(t1, t0);
			scan_ns += INSTR_TIME_GET_NANOSEC(t1);
			if (m)
				sbm_free(m);
		}
		EMIT("sbm_bulk", build_ns, scan_ns, bytes);
	}

	/* ---- Bitmapset ---- */
	{
		int64		build_ns = 0,
					scan_ns = 0;
		int64		bytes = 0;

		for (int64 r = 0; r < reps; r++)
		{
			Bitmapset  *bms = NULL;
			int			x;

			INSTR_TIME_SET_CURRENT(t0);
			for (int64 i = 0; i < nn; i++)
				bms = bms_add_member(bms, (int) idx[i]);
			INSTR_TIME_SET_CURRENT(t1);
			INSTR_TIME_SUBTRACT(t1, t0);
			build_ns += INSTR_TIME_GET_NANOSEC(t1);
			if (r == 0)
			{
				/* header + word array; nwords = ceil((maxbit+1)/64) */
				int			hi = bms ? bms_prev_member(bms, -1) : -1;
				int64		nwords = hi < 0 ? 0 : (hi / 64 + 1);

				bytes = (int64) offsetof(Bitmapset, words) + nwords * 8;
			}

			INSTR_TIME_SET_CURRENT(t0);
			x = -1;
			while ((x = bms_next_member(bms, x)) >= 0)
				sink += (uint64) x;
			INSTR_TIME_SET_CURRENT(t1);
			INSTR_TIME_SUBTRACT(t1, t0);
			scan_ns += INSTR_TIME_GET_NANOSEC(t1);
			if (bms)
				bms_free(bms);
		}
		EMIT("bitmapset", build_ns, scan_ns, bytes);
	}

	/* ---- TIDBitmap ---- */
	{
		int64		build_ns = 0,
					scan_ns = 0;
		int			maxoff = MaxHeapTuplesPerPage;

		for (int64 r = 0; r < reps; r++)
		{
			TIDBitmap  *tbm;
			TBMIterator it;
			TBMIterateResult res;

			/* Generous budget so it never goes lossy for this workload. */
			tbm = tbm_create(1024L * 1024L * 1024L, NULL);

			INSTR_TIME_SET_CURRENT(t0);
			for (int64 i = 0; i < nn; i++)
			{
				ItemPointerData tid;
				BlockNumber blk = (BlockNumber) (idx[i] / (uint64) maxoff);
				OffsetNumber off = (OffsetNumber) (idx[i] % (uint64) maxoff) + 1;

				ItemPointerSet(&tid, blk, off);
				tbm_add_tuples(tbm, &tid, 1, false);
			}
			INSTR_TIME_SET_CURRENT(t1);
			INSTR_TIME_SUBTRACT(t1, t0);
			build_ns += INSTR_TIME_GET_NANOSEC(t1);

			INSTR_TIME_SET_CURRENT(t0);
			it = tbm_begin_iterate(tbm, NULL, InvalidDsaPointer);
			while (tbm_iterate(&it, &res))
				sink += (uint64) res.blockno;
			tbm_end_iterate(&it);
			INSTR_TIME_SET_CURRENT(t1);
			INSTR_TIME_SUBTRACT(t1, t0);
			scan_ns += INSTR_TIME_GET_NANOSEC(t1);
			tbm_free(tbm);
		}
		/* TIDBitmap has no public byte-size accessor; report -1. */
		EMIT("tidbitmap", build_ns, scan_ns, -1);
	}

	(void) sink;				/* keep the optimizer honest */
	pfree(idx);
	return (Datum) 0;
}
