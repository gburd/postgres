/*-------------------------------------------------------------------------
 *
 * backend_runtime_nodes.c
 *	  Runtime bridge accessors for node read/write execution state.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/nodes/backend_runtime_nodes.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgExecutionNodeIOState *
PgCurrentExecutionNodeIOState(void)
{
	PG_RUNTIME_RETURN_CURRENT_EXECUTION_BUCKET(CurrentPgExecutionNodeIORuntimeState,
											   node_io);
}

bool *
PgCurrentNodeWriteLocationFieldsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionNodeIORuntimeState, PgCurrentExecutionNodeIOState)->write_location_fields;
}

const char **
PgCurrentNodeReadStrtokPtrRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionNodeIORuntimeState, PgCurrentExecutionNodeIOState)->strtok_ptr;
}

bool *
PgCurrentNodeRestoreLocationFieldsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionNodeIORuntimeState, PgCurrentExecutionNodeIOState)->restore_location_fields;
}
