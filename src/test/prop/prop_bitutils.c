/*-------------------------------------------------------------------------
 *
 * prop_bitutils.c
 *	  Property-based tests for src/port/pg_bitutils.c.
 *
 * The bit utilities -- population count, leftmost/rightmost set-bit position --
 * back the buffer manager, bitmapsets, visibility maps, and more.  Each has a
 * dispatch path (lookup table, intrinsic, or runtime-selected SIMD) that must
 * agree with a trivial reference loop for every input.  SIMD-vs-scalar
 * divergence is a classic bug class, which makes these worth pinning down
 * against an independent oracle.
 *
 * Tier A: in-process, no server needed.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_bitutils.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "bitutils"
#include "pg_prop.h"

#include "port/pg_bitutils.h"

/* Reference popcount: count set bits one at a time. */
static int
ref_popcount64(uint64 w)
{
	int			n = 0;

	while (w)
	{
		n += (int) (w & 1);
		w >>= 1;
	}
	return n;
}

PG_PROP(popcount32_matches_reference)
{
	uint32		w = (uint32) pg_prop_draw_int(tc, 0, UINT32_MAX);

	if ((int) pg_popcount32(w) != ref_popcount64(w))
		pg_prop_fail(tc, "pg_popcount32(%u) = %d, reference = %d",
					 w, (int) pg_popcount32(w), ref_popcount64(w));
}

PG_PROP(popcount64_matches_reference)
{
	uint64		w = (uint64) pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	if ((int) pg_popcount64(w) != ref_popcount64(w))
		pg_prop_fail(tc, "pg_popcount64(%llu) = %d, reference = %d",
					 (unsigned long long) w, (int) pg_popcount64(w),
					 ref_popcount64(w));
}

PG_PROP(popcount_buffer_matches_reference)
{
	size_t		len;
	unsigned char *buf = pg_prop_draw_bytes(tc, 0, 256, &len);
	uint64		got;
	int			ref = 0;

	/* exercise the multi-byte dispatch path (table / intrinsic / SIMD) */
	got = pg_popcount((const char *) buf, (int) len);
	for (size_t i = 0; i < len; i++)
		ref += ref_popcount64(buf[i]);

	if ((int) got != ref)
		pg_prop_fail(tc, "pg_popcount(len=%zu) = %llu, reference = %d",
					 len, (unsigned long long) got, ref);

	pg_free(buf);
}

PG_PROP(leftmost_one_pos32_matches_reference)
{
	uint32		w = (uint32) pg_prop_draw_int(tc, 1, UINT32_MAX);
	int			ref;
	int			got;

	/* pg_leftmost_one_pos32 is undefined for 0, so draw >= 1 */
	pg_prop_assume(tc, w != 0);

	/* reference: index of the highest set bit */
	ref = 0;
	for (int i = 31; i >= 0; i--)
	{
		if (w & (1u << i))
		{
			ref = i;
			break;
		}
	}
	got = pg_leftmost_one_pos32(w);
	if (got != ref)
		pg_prop_fail(tc, "pg_leftmost_one_pos32(%u) = %d, reference = %d",
					 w, got, ref);
}

PG_PROP(rightmost_one_pos64_matches_reference)
{
	uint64		w = (uint64) pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);
	int			ref;
	int			got;

	/* pg_rightmost_one_pos64 is undefined for 0 */
	pg_prop_assume(tc, w != 0);

	/* reference: index of the lowest set bit */
	ref = 0;
	for (int i = 0; i < 64; i++)
	{
		if (w & (UINT64CONST(1) << i))
		{
			ref = i;
			break;
		}
	}
	got = pg_rightmost_one_pos64(w);
	if (got != ref)
		pg_prop_fail(tc, "pg_rightmost_one_pos64(%llu) = %d, reference = %d",
					 (unsigned long long) w, got, ref);
}
