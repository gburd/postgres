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
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
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
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->vfd_cache;
}

Size *
PgCurrentSizeVfdCacheRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->size_vfd_cache;
}

int *
PgCurrentNFileRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->nfile;
}

bool *
PgCurrentTemporaryFilesAllowedRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->temporary_files_allowed;
}

bool *
PgCurrentHaveXactTemporaryFilesRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgExecutionTransactionCleanupRuntimeState, PgCurrentExecutionTransactionCleanupState)->have_xact_temporary_files;
}

int *
PgCurrentNumAllocatedDescsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->num_allocated_descs;
}

int *
PgCurrentMaxAllocatedDescsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->max_allocated_descs;
}

void **
PgCurrentAllocatedDescsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->allocated_descs;
}

int *
PgCurrentNumExternalFDsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->num_external_fds;
}

HTAB **
PgCurrentSyncPendingOpsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->sync_pending_ops;
}

List **
PgCurrentSyncPendingUnlinksRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->sync_pending_unlinks;
}

MemoryContext *
PgCurrentSyncPendingOpsContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->sync_pending_ops_context;
}

uint16 *
PgCurrentSyncCycleCounterRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->sync_cycle_counter;
}

uint16 *
PgCurrentSyncCheckpointCycleCounterRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->sync_checkpoint_cycle_counter;
}

bool *
PgCurrentSyncInProgressRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->sync_in_progress;
}

HTAB **
PgCurrentSMgrRelationHashRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->smgr_relation_hash;
}

dlist_head *
PgCurrentSMgrUnpinnedRelationsRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->smgr_unpinned_relations;
}

MemoryContext *
PgCurrentMdContextRef(void)
{
	return &PG_RUNTIME_FAST_BUCKET_ACCESSOR(CurrentPgBackendStorageRuntimeState, PgCurrentBackendStorageState)->md_context;
}

void
PgBackendResetStorageClosedState(PgBackendStorageState *storage)
{
	int			saved_num_external_fds;

	Assert(storage != NULL);

	/*
	 * num_external_fds is owned by the ReserveExternalFD/ReleaseExternalFD
	 * protocol, not by this closed-state reclaim.  Reservations held by this
	 * backend's WaitEventSets (latch wait set, self-pipe, signalfd, FeBeWaitSet)
	 * are released LATER in the exit path -- the ipc bucket's
	 * PgBackendResetIPCClosedState() frees latch_wait_set and
	 * ShutdownWaitEventSupport() closes the per-carrier fds, each calling
	 * ReleaseExternalFD().  Neither PgBackendResetFileAccessClosedState() nor
	 * the PgBackendInitializeStorageState() MemSet below may zero the counter
	 * out from under those pending releases, or they underflow
	 * (Assert(numExternalFDs > 0) in fd.c -- the bootstrap-cassert teardown
	 * abort).  Preserve the live count across the whole reset; the explicit
	 * releases drive it to 0.
	 */
	saved_num_external_fds = storage->num_external_fds;

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
	storage->num_external_fds = saved_num_external_fds;
}
