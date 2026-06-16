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
#include "postgres.h"

#include "utils/backend_runtime.h"
#include "../utils/init/backend_runtime_internal.h"

PgExecutionNodeIOState *
PgCurrentExecutionNodeIOState(void)
{
	return &PgCurrentOrEarlyExecution()->node_io;
}

bool *
PgCurrentNodeWriteLocationFieldsRef(void)
{
	return &PgCurrentExecutionNodeIOState()->write_location_fields;
}

const char **
PgCurrentNodeReadStrtokPtrRef(void)
{
	return &PgCurrentExecutionNodeIOState()->strtok_ptr;
}

bool *
PgCurrentNodeRestoreLocationFieldsRef(void)
{
	return &PgCurrentExecutionNodeIOState()->restore_location_fields;
}
