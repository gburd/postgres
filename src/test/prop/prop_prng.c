/*-------------------------------------------------------------------------
 *
 * prop_prng.c
 *	  Property-based tests for src/common/pg_prng.c.
 *
 * pg_prng is the shared pseudo-random generator (xoroshiro128**) behind
 * random(), sampling, and backend jitter.  Its range helpers carry explicit
 * contracts -- a value in [rmin, rmax], a double in [0, 1) -- that must hold
 * for every seed and every bound, including the degenerate rmin == rmax case
 * and the full-width range.  Those contracts are exactly what a property test
 * pins down.
 *
 * Tier A: in-process, no server needed.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/test/prop/prop_prng.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#define PG_PROP_AREA "prng"
#include "pg_prop.h"

#include "common/pg_prng.h"

/*
 * Seed a fresh generator from a drawn 64-bit value.  Seeding the PRNG from a
 * hegel-drawn integer is the right pattern here (unlike feeding a seed to code
 * that then makes its own random decisions): the property is about the range
 * helpers' output bounds, which must hold for every internal state, so the
 * seed is itself the input we want hegel to explore and shrink.
 */
static void
seed_from_draw(pg_prop_tc *tc, pg_prng_state *state)
{
	uint64		s = (uint64) pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	pg_prng_seed(state, s);
}

PG_PROP(uint64_range_within_bounds)
{
	pg_prng_state state;
	uint64		lo = (uint64) pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);
	uint64		hi = (uint64) pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);
	uint64		r;

	if (lo > hi)
	{
		uint64		tmp = lo;

		lo = hi;
		hi = tmp;
	}

	seed_from_draw(tc, &state);

	/* draw several values so one seed exercises more of the stream */
	for (int i = 0; i < 20; i++)
	{
		r = pg_prng_uint64_range(&state, lo, hi);
		if (r < lo || r > hi)
			pg_prop_fail(tc,
						 "uint64_range out of bounds: lo=%llu hi=%llu got=%llu",
						 (unsigned long long) lo, (unsigned long long) hi,
						 (unsigned long long) r);
	}
}

PG_PROP(int64_range_within_bounds)
{
	pg_prng_state state;
	int64		lo = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);
	int64		hi = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);
	int64		r;

	if (lo > hi)
	{
		int64		tmp = lo;

		lo = hi;
		hi = tmp;
	}

	seed_from_draw(tc, &state);

	for (int i = 0; i < 20; i++)
	{
		r = pg_prng_int64_range(&state, lo, hi);
		if (r < lo || r > hi)
			pg_prop_fail(tc,
						 "int64_range out of bounds: lo=%lld hi=%lld got=%lld",
						 (long long) lo, (long long) hi, (long long) r);
	}
}

PG_PROP(singleton_range_is_exact)
{
	pg_prng_state state;
	int64		v = pg_prop_draw_int(tc, INT64_MIN, INT64_MAX);

	seed_from_draw(tc, &state);

	/* a range of one value must always return that value */
	for (int i = 0; i < 8; i++)
	{
		int64		r = pg_prng_int64_range(&state, v, v);

		if (r != v)
			pg_prop_fail(tc, "singleton range [%lld,%lld] returned %lld",
						 (long long) v, (long long) v, (long long) r);
	}
}

PG_PROP(double_in_unit_interval)
{
	pg_prng_state state;

	seed_from_draw(tc, &state);

	/* pg_prng_double() is documented to return a value in [0.0, 1.0) */
	for (int i = 0; i < 50; i++)
	{
		double		d = pg_prng_double(&state);

		if (!(d >= 0.0 && d < 1.0))
			pg_prop_fail(tc, "double not in [0,1): got %.17g", d);
	}
}
