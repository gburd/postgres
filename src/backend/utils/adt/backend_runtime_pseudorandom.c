/*-------------------------------------------------------------------------
 *
 * backend_runtime_pseudorandom.c
 *	  Runtime bridge accessors for pseudorandom-function session state.
 *
 * These accessors keep SQL random-function compatibility globals mapped onto
 * the current PgSession while leaving runtime construction and early fallback
 * ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/adt/backend_runtime_pseudorandom.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../init/backend_runtime_internal.h"

pg_prng_state *
PgCurrentPseudoRandomStateRef(void)
{
	return &PgCurrentSessionRandomState()->prng_state;
}

bool *
PgCurrentPseudoRandomSeedSetRef(void)
{
	return &PgCurrentSessionRandomState()->prng_seed_set;
}
