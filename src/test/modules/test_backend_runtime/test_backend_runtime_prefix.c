/*-------------------------------------------------------------------------
 *
 * test_backend_runtime_prefix.c
 *		Reservation-only preload fixture, with no custom GUC descriptors.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/guc.h"

PG_MODULE_MAGIC_EXT(
					.name = "test_backend_runtime_prefix",
					.version = PG_VERSION,
					PG_MODULE_MAGIC_BACKEND_MODEL_POOLED_PROTOCOL_AFFINE
);

PGDLLEXPORT void _PG_init(void);

void
_PG_init(void)
{
	MarkGUCPrefixReserved("test_backend_runtime_prefix");
}
