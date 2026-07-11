/*-------------------------------------------------------------------------
 *
 * backend_runtime_resowner.c
 *	  Runtime bridge accessors for execution-owned resource-owner state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/resowner/backend_runtime_resowner.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "../init/backend_runtime_internal.h"

static ResourceOwner *PgCurrentResourceOwnerObjectRef(void);

PgExecutionResourceOwnerState *
PgCurrentExecutionResourceOwners(void)
{
	if (likely(CurrentPgExecutionResourceOwnerRuntimeState != NULL))
		return CurrentPgExecutionResourceOwnerRuntimeState;

	return &PgCurrentOrEarlyExecution()->resource_owners;
}

ResourceOwner *
PgCurrentResourceOwnerRef(void)
{
	return PG_RUNTIME_CURRENT_HOT_FIELD_REF(PgCurrentResourceOwnerHotRef,
											CurrentPgExecution,
											PgCurrentResourceOwnerObjectRef);
}

static ResourceOwner *
PgCurrentResourceOwnerObjectRef(void)
{
	return &PgCurrentExecutionResourceOwners()->current_owner;
}

ResourceOwner *
PgCurTransactionResourceOwnerRef(void)
{
	return &PgCurrentExecutionResourceOwners()->cur_transaction_owner;
}

ResourceOwner *
PgTopTransactionResourceOwnerRef(void)
{
	return &PgCurrentExecutionResourceOwners()->top_transaction_owner;
}

MemoryContext
PgCurrentResourceOwnerMemoryContext(void)
{
	PgExecutionResourceOwnerState *resource_owners;

	resource_owners = PgCurrentExecutionResourceOwners();
	return PgRuntimeGetOwnedMemoryContextWithSizes(&resource_owners->resource_owner_context,
												  "ResourceOwnerContext",
												  ALLOCSET_START_SMALL_SIZES);
}
