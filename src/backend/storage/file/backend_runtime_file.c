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

uint64 *
PgCurrentTemporaryFilesSizeRef(void)
{
	return &PgCurrentSessionTempFileState()->temporary_files_size;
}

long *
PgCurrentTempFileCounterRef(void)
{
	return &PgCurrentSessionTempFileState()->temp_file_counter;
}

Oid **
PgCurrentTempTableSpaceOidsRef(void)
{
	return &PgCurrentSessionTempFileState()->temp_table_spaces;
}

int *
PgCurrentNumTempTableSpacesRef(void)
{
	return &PgCurrentSessionTempFileState()->num_temp_table_spaces;
}

int *
PgCurrentNextTempTableSpaceRef(void)
{
	return &PgCurrentSessionTempFileState()->next_temp_table_space;
}

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

bool *
PgCurrentHaveXactTemporaryFilesRef(void)
{
	return &PgCurrentExecutionTransactionCleanupState()->have_xact_temporary_files;
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

void
PgBackendResetStorageClosedState(PgBackendStorageState *storage)
{
	Assert(storage != NULL);

	/*
	 * fd.c owns the private Vfd and AllocateDesc layouts.  By the time a
	 * logical backend reaches closed-state reset, normal transaction and
	 * proc-exit cleanup should have closed semantic file owners; this reclaim
	 * step clears the retained arrays and defensively closes any leftovers.
	 */
	PgBackendResetFileAccessClosedState(storage);

	PG_RUNTIME_DESTROY_HASH(storage->sync_pending_ops);
	PG_RUNTIME_LIST_FREE_DEEP(storage->sync_pending_unlinks);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(storage->sync_pending_ops_context);

	PG_RUNTIME_DESTROY_HASH(storage->smgr_relation_hash);
	dlist_init(&storage->smgr_unpinned_relations);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(storage->md_context);

	PgBackendInitializeStorageState(storage);
}
