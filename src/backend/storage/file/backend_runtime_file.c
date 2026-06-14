/*-------------------------------------------------------------------------
 *
 * backend_runtime_file.c
 *	  Runtime bridge accessors for backend-local file/storage state.
 *
 * These accessors keep storage and fd compatibility globals mapped onto the
 * current PgBackend while leaving runtime construction and early fallback
 * ownership in utils/init/backend_runtime.c.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/storage/file/backend_runtime_file.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/ilist.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "../../utils/init/backend_runtime_internal.h"

void **
PgCurrentVfdCacheRef(void)
{
	return &PgCurrentBackendStorageState()->vfd_cache;
}

Size *
PgCurrentSizeVfdCacheRef(void)
{
	return &PgCurrentBackendStorageState()->size_vfd_cache;
}

int *
PgCurrentNFileRef(void)
{
	return &PgCurrentBackendStorageState()->nfile;
}

bool *
PgCurrentTemporaryFilesAllowedRef(void)
{
	return &PgCurrentBackendStorageState()->temporary_files_allowed;
}

int *
PgCurrentNumAllocatedDescsRef(void)
{
	return &PgCurrentBackendStorageState()->num_allocated_descs;
}

int *
PgCurrentMaxAllocatedDescsRef(void)
{
	return &PgCurrentBackendStorageState()->max_allocated_descs;
}

void **
PgCurrentAllocatedDescsRef(void)
{
	return &PgCurrentBackendStorageState()->allocated_descs;
}

int *
PgCurrentNumExternalFDsRef(void)
{
	return &PgCurrentBackendStorageState()->num_external_fds;
}

HTAB **
PgCurrentSyncPendingOpsRef(void)
{
	return &PgCurrentBackendStorageState()->sync_pending_ops;
}

List **
PgCurrentSyncPendingUnlinksRef(void)
{
	return &PgCurrentBackendStorageState()->sync_pending_unlinks;
}

MemoryContext *
PgCurrentSyncPendingOpsContextRef(void)
{
	return &PgCurrentBackendStorageState()->sync_pending_ops_context;
}

uint16 *
PgCurrentSyncCycleCounterRef(void)
{
	return &PgCurrentBackendStorageState()->sync_cycle_counter;
}

uint16 *
PgCurrentSyncCheckpointCycleCounterRef(void)
{
	return &PgCurrentBackendStorageState()->sync_checkpoint_cycle_counter;
}

bool *
PgCurrentSyncInProgressRef(void)
{
	return &PgCurrentBackendStorageState()->sync_in_progress;
}

HTAB **
PgCurrentSMgrRelationHashRef(void)
{
	return &PgCurrentBackendStorageState()->smgr_relation_hash;
}

dlist_head *
PgCurrentSMgrUnpinnedRelationsRef(void)
{
	return &PgCurrentBackendStorageState()->smgr_unpinned_relations;
}

MemoryContext *
PgCurrentMdContextRef(void)
{
	return &PgCurrentBackendStorageState()->md_context;
}
