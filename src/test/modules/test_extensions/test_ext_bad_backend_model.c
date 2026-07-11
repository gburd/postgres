/*
 * test_ext_bad_backend_model.c
 *
 * Extension module with intentionally invalid backend-model metadata.
 *
 * Portions Copyright (c) 2026, PostgreSQL Global Development Group
 */
#include "postgres.h"

#include "fmgr.h"

PG_MODULE_MAGIC_EXT(
					.name = "test_ext_bad_backend_model",
					.version = PG_VERSION,
					.backend_model = (PgBackendModel) 99
);
