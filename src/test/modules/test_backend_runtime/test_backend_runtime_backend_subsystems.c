/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_backend_subsystems.c
 *		Backend subsystem runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_backend_subsystems.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

PG_FUNCTION_INFO_V1(test_backend_parallel_state_is_backend_local);
Datum
test_backend_parallel_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;

	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	fake_backend1.parallel.worker_number = -1;
	fake_backend1.parallel.pq_mq_parallel_leader_proc_number = INVALID_PROC_NUMBER;
	fake_backend2.parallel.worker_number = -1;
	fake_backend2.parallel.pq_mq_parallel_leader_proc_number = INVALID_PROC_NUMBER;

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		ok = ok && ParallelWorkerNumber == -1;
		ok = ok && !ParallelMessagePending;
		ok = ok && !InitializingParallelWorker;
		ok = ok && *PgCurrentFixedParallelStateRef() == NULL;
		ok = ok && !*PgCurrentParallelContextListInitializedRef();
		ok = ok && *PgCurrentParallelLeaderPidRef() == 0;
		ok = ok && *PgCurrentPqMqHandleRef() == NULL;
		ok = ok && !*PgCurrentPqMqBusyRef();
		ok = ok && *PgCurrentPqMqParallelLeaderPidRef() == 0;
		ok = ok && *PgCurrentPqMqParallelLeaderProcNumberRef() == INVALID_PROC_NUMBER;

		ParallelWorkerNumber = 3;
		ParallelMessagePending = true;
		InitializingParallelWorker = true;
		*PgCurrentFixedParallelStateRef() = &fake_backend1;
		dlist_init(PgCurrentParallelContextListRef());
		*PgCurrentParallelContextListInitializedRef() = true;
		*PgCurrentParallelLeaderPidRef() = 111;
		*PgCurrentPqMqHandleRef() = &fake_backend1;
		*PgCurrentPqMqBusyRef() = true;
		*PgCurrentPqMqParallelLeaderPidRef() = 222;
		*PgCurrentPqMqParallelLeaderProcNumberRef() = 12;
		*PgCurrentParallelMessageContextRef() = (MemoryContext) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && ParallelWorkerNumber == -1;
		ok = ok && !ParallelMessagePending;
		ok = ok && !InitializingParallelWorker;
		ok = ok && *PgCurrentFixedParallelStateRef() == NULL;
		ok = ok && !*PgCurrentParallelContextListInitializedRef();
		ok = ok && *PgCurrentParallelLeaderPidRef() == 0;
		ok = ok && *PgCurrentPqMqHandleRef() == NULL;
		ok = ok && !*PgCurrentPqMqBusyRef();
		ok = ok && *PgCurrentPqMqParallelLeaderPidRef() == 0;
		ok = ok && *PgCurrentPqMqParallelLeaderProcNumberRef() == INVALID_PROC_NUMBER;
		ok = ok && *PgCurrentParallelMessageContextRef() == NULL;

		ParallelWorkerNumber = 4;
		ParallelMessagePending = false;
		InitializingParallelWorker = true;
		*PgCurrentFixedParallelStateRef() = &fake_backend2;
		dlist_init(PgCurrentParallelContextListRef());
		*PgCurrentParallelContextListInitializedRef() = true;
		*PgCurrentParallelLeaderPidRef() = 333;
		*PgCurrentPqMqHandleRef() = &fake_backend2;
		*PgCurrentPqMqBusyRef() = true;
		*PgCurrentPqMqParallelLeaderPidRef() = 444;
		*PgCurrentPqMqParallelLeaderProcNumberRef() = 34;
		*PgCurrentParallelMessageContextRef() = (MemoryContext) &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && ParallelWorkerNumber == 3;
		ok = ok && ParallelMessagePending;
		ok = ok && InitializingParallelWorker;
		ok = ok && *PgCurrentFixedParallelStateRef() == &fake_backend1;
		ok = ok && *PgCurrentParallelContextListInitializedRef();
		ok = ok && dlist_is_empty(PgCurrentParallelContextListRef());
		ok = ok && *PgCurrentParallelLeaderPidRef() == 111;
		ok = ok && *PgCurrentPqMqHandleRef() == &fake_backend1;
		ok = ok && *PgCurrentPqMqBusyRef();
		ok = ok && *PgCurrentPqMqParallelLeaderPidRef() == 222;
		ok = ok && *PgCurrentPqMqParallelLeaderProcNumberRef() == 12;
		ok = ok && *PgCurrentParallelMessageContextRef() ==
			(MemoryContext) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && ParallelWorkerNumber == 4;
		ok = ok && !ParallelMessagePending;
		ok = ok && InitializingParallelWorker;
		ok = ok && *PgCurrentFixedParallelStateRef() == &fake_backend2;
		ok = ok && *PgCurrentParallelContextListInitializedRef();
		ok = ok && dlist_is_empty(PgCurrentParallelContextListRef());
		ok = ok && *PgCurrentParallelLeaderPidRef() == 333;
		ok = ok && *PgCurrentPqMqHandleRef() == &fake_backend2;
		ok = ok && *PgCurrentPqMqBusyRef();
		ok = ok && *PgCurrentPqMqParallelLeaderPidRef() == 444;
		ok = ok && *PgCurrentPqMqParallelLeaderProcNumberRef() == 34;
		ok = ok && *PgCurrentParallelMessageContextRef() ==
			(MemoryContext) &fake_backend2;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend parallel state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_instrumentation_state_is_backend_local);
Datum
test_backend_instrumentation_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	BufferUsage saved_buffer_usage;
	BufferUsage saved_saved_buffer_usage;
	WalUsage	saved_wal_usage;
	WalUsage	saved_saved_wal_usage;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	saved_buffer_usage = pgBufferUsage;
	saved_saved_buffer_usage = save_pgBufferUsage;
	saved_wal_usage = pgWalUsage;
	saved_saved_wal_usage = save_pgWalUsage;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		pgBufferUsage.shared_blks_hit = 11;
		save_pgBufferUsage.shared_blks_hit = 12;
		pgWalUsage.wal_records = 13;
		save_pgWalUsage.wal_records = 14;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && pgBufferUsage.shared_blks_hit == 0;
		ok = ok && save_pgBufferUsage.shared_blks_hit == 0;
		ok = ok && pgWalUsage.wal_records == 0;
		ok = ok && save_pgWalUsage.wal_records == 0;

		pgBufferUsage.shared_blks_hit = 21;
		save_pgBufferUsage.shared_blks_hit = 22;
		pgWalUsage.wal_records = 23;
		save_pgWalUsage.wal_records = 24;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && pgBufferUsage.shared_blks_hit == 11;
		ok = ok && save_pgBufferUsage.shared_blks_hit == 12;
		ok = ok && pgWalUsage.wal_records == 13;
		ok = ok && save_pgWalUsage.wal_records == 14;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && pgBufferUsage.shared_blks_hit == 21;
		ok = ok && save_pgBufferUsage.shared_blks_hit == 22;
		ok = ok && pgWalUsage.wal_records == 23;
		ok = ok && save_pgWalUsage.wal_records == 24;

		PgSetCurrentBackend(saved_backend);
		pgBufferUsage = saved_buffer_usage;
		save_pgBufferUsage = saved_saved_buffer_usage;
		pgWalUsage = saved_wal_usage;
		save_pgWalUsage = saved_saved_wal_usage;
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		pgBufferUsage = saved_buffer_usage;
		save_pgBufferUsage = saved_saved_buffer_usage;
		pgWalUsage = saved_wal_usage;
		save_pgWalUsage = saved_saved_wal_usage;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend instrumentation state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_runtime_hot_bucket_cache_tracks_current_work);
Datum
test_runtime_hot_bucket_cache_tracks_current_work(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgExecution *saved_execution;
	PgBackend	fake_backend;
	PgExecution fake_execution;
	MemoryContext saved_current_memory_context;
	MemoryContext fake_memory_context;
	ResourceOwner saved_current_resource_owner;
	ResourceOwner fake_resource_owner;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	saved_execution = CurrentPgExecution;
	saved_current_memory_context = CurrentMemoryContext;
	saved_current_resource_owner = CurrentResourceOwner;
	MemSet(&fake_backend, 0, sizeof(fake_backend));
	MemSet(&fake_execution, 0, sizeof(fake_execution));
	fake_memory_context = (MemoryContext) &fake_execution;
	fake_resource_owner = (ResourceOwner) &fake_execution;

	PG_TRY();
	{
		PgSetCurrentSession(CurrentPgSession);

		if (CurrentPgBackend == NULL)
			elog(ERROR, "runtime hot bucket cache has no current backend");
		if (CurrentPgExecution == NULL)
			elog(ERROR, "runtime hot bucket cache has no current execution");
		if (CurrentPgBackendBufferRuntimeState != &CurrentPgBackend->buffers)
			elog(ERROR, "runtime hot bucket cache does not match current backend buffers");
		if (CurrentPgBackendLockRuntimeState != &CurrentPgBackend->locks)
			elog(ERROR, "runtime hot bucket cache does not match current backend locks");
		if (CurrentPgExecutionMemoryContextRuntimeState !=
			&CurrentPgExecution->memory_contexts)
			elog(ERROR, "runtime hot bucket cache does not match current execution memory contexts");
		if (CurrentPgExecutionResourceOwnerRuntimeState !=
			&CurrentPgExecution->resource_owners)
			elog(ERROR, "runtime hot bucket cache does not match current execution resource owners");

		/*
		 * Current-work setters are the cache invalidation boundary.  A hot
		 * bucket cache must follow those switches rather than retaining the
		 * previously-installed runtime work.
		 */
		PgSetCurrentBackend(&fake_backend);
		PgSetCurrentExecution(&fake_execution);

		*PgCurrentPrivateRefCountArrayKeysRef() = &fake_backend;
		*PgCurrentPrivateRefCountOverflowedRef() = 17;
		*PgCurrentNumHeldLWLocksRef() = 19;

		if (*PgCurrentPrivateRefCountArrayKeysRef() != &fake_backend)
			elog(ERROR, "runtime hot bucket cache did not track current work: private refcount keys");
		if (*PgCurrentPrivateRefCountOverflowedRef() != 17)
			elog(ERROR, "runtime hot bucket cache did not track current work: private refcount overflowed");
		if (*PgCurrentNumHeldLWLocksRef() != 19)
			elog(ERROR, "runtime hot bucket cache did not track current work: held lwlock count");
		if (CurrentPgBackendBufferRuntimeState != &fake_backend.buffers)
			elog(ERROR, "runtime hot bucket cache did not rebind: backend buffer bucket");
		if (CurrentPgBackendLockRuntimeState != &fake_backend.locks)
			elog(ERROR, "runtime hot bucket cache did not rebind: backend lock bucket");
		if (CurrentPgExecutionMemoryContextRuntimeState !=
			&fake_execution.memory_contexts)
			elog(ERROR, "runtime hot bucket cache did not rebind: execution memory bucket");
		if (CurrentPgExecutionResourceOwnerRuntimeState !=
			&fake_execution.resource_owners)
			elog(ERROR, "runtime hot bucket cache did not rebind: execution resource owner bucket");

		PgSetCurrentBackend(saved_backend);
		PgSetCurrentExecution(&fake_execution);
		CurrentMemoryContext = fake_memory_context;
		CurrentResourceOwner = fake_resource_owner;
		if (CurrentMemoryContext != fake_memory_context)
			elog(ERROR, "runtime hot mirror did not track explicit current execution switch: current memory context");
		if (CurrentResourceOwner != fake_resource_owner)
			elog(ERROR, "runtime hot mirror did not track explicit current execution switch: current resource owner");
		PgSetCurrentExecution(saved_execution);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentExecution(saved_execution);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "runtime hot bucket cache did not track current work");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_buffer_state_is_backend_local);
Datum
test_backend_buffer_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	WritebackContext *fake_backend1_writeback;
	WritebackContext *fake_backend2_writeback;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		*PgCurrentNLocBufferRef() = 101;
		*PgCurrentLocalBufferDescriptorsRef() = &fake_backend1;
		*PgCurrentLocalBufferBlockPointersRef() = &fake_backend1;
		*PgCurrentLocalRefCountRef() = (int32 *) &fake_backend1;
		*PgCurrentNextFreeLocalBufIdRef() = 102;
		*PgCurrentLocalBufHashRef() = (HTAB *) &fake_backend1;
		*PgCurrentNLocalPinnedBuffersRef() = 103;
		*PgCurrentLocalBufferCurBlockRef() = (char *) &fake_backend1;
		*PgCurrentLocalBufferNextBufInBlockRef() = 104;
		*PgCurrentLocalBufferNumBufsInBlockRef() = 105;
		*PgCurrentLocalBufferTotalBufsAllocatedRef() = 106;
		*PgCurrentLocalBufferContextRef() = (MemoryContext) &fake_backend1;
		*PgCurrentPinCountWaitBufRef() = (BufferDesc *) &fake_backend1;
		fake_backend1_writeback = PgCurrentBackendWritebackContextRef();
		ok = ok && fake_backend1_writeback != NULL;
		*PgCurrentPrivateRefCountArrayKeysRef() = &fake_backend1;
		*PgCurrentPrivateRefCountArrayRef() = &fake_backend1;
		*PgCurrentPrivateRefCountHashRef() = &fake_backend1;
		*PgCurrentPrivateRefCountOverflowedRef() = 107;
		*PgCurrentPrivateRefCountClockRef() = 108;
		*PgCurrentReservedRefCountSlotRef() = 109;
		*PgCurrentPrivateRefCountEntryLastRef() = 110;
		*PgCurrentMaxProportionalPinsRef() = 111;

		PgSetCurrentBackend(&fake_backend2);
		fake_backend2_writeback = PgCurrentBackendWritebackContextRef();
		ok = ok && *PgCurrentNLocBufferRef() == 0;
		ok = ok && *PgCurrentLocalBufferDescriptorsRef() == NULL;
		ok = ok && *PgCurrentLocalBufferBlockPointersRef() == NULL;
		ok = ok && *PgCurrentLocalRefCountRef() == NULL;
		ok = ok && *PgCurrentNextFreeLocalBufIdRef() == 0;
		ok = ok && *PgCurrentLocalBufHashRef() == NULL;
		ok = ok && *PgCurrentNLocalPinnedBuffersRef() == 0;
		ok = ok && *PgCurrentLocalBufferCurBlockRef() == NULL;
		ok = ok && *PgCurrentLocalBufferNextBufInBlockRef() == 0;
		ok = ok && *PgCurrentLocalBufferNumBufsInBlockRef() == 0;
		ok = ok && *PgCurrentLocalBufferTotalBufsAllocatedRef() == 0;
		ok = ok && *PgCurrentLocalBufferContextRef() == NULL;
		ok = ok && *PgCurrentPinCountWaitBufRef() == NULL;
		ok = ok && fake_backend2_writeback != NULL;
		ok = ok && fake_backend2_writeback != fake_backend1_writeback;
		ok = ok && *PgCurrentPrivateRefCountArrayKeysRef() == NULL;
		ok = ok && *PgCurrentPrivateRefCountArrayRef() == NULL;
		ok = ok && *PgCurrentPrivateRefCountHashRef() == NULL;
		ok = ok && *PgCurrentPrivateRefCountOverflowedRef() == 0;
		ok = ok && *PgCurrentPrivateRefCountClockRef() == 0;
		ok = ok && *PgCurrentReservedRefCountSlotRef() == 0;
		ok = ok && *PgCurrentPrivateRefCountEntryLastRef() == 0;
		ok = ok && *PgCurrentMaxProportionalPinsRef() == 0;

		*PgCurrentNLocBufferRef() = 201;
		*PgCurrentLocalBufferDescriptorsRef() = &fake_backend2;
		*PgCurrentLocalBufferBlockPointersRef() = &fake_backend2;
		*PgCurrentLocalRefCountRef() = (int32 *) &fake_backend2;
		*PgCurrentNextFreeLocalBufIdRef() = 202;
		*PgCurrentLocalBufHashRef() = (HTAB *) &fake_backend2;
		*PgCurrentNLocalPinnedBuffersRef() = 203;
		*PgCurrentLocalBufferCurBlockRef() = (char *) &fake_backend2;
		*PgCurrentLocalBufferNextBufInBlockRef() = 204;
		*PgCurrentLocalBufferNumBufsInBlockRef() = 205;
		*PgCurrentLocalBufferTotalBufsAllocatedRef() = 206;
		*PgCurrentLocalBufferContextRef() = (MemoryContext) &fake_backend2;
		*PgCurrentPinCountWaitBufRef() = (BufferDesc *) &fake_backend2;
		*PgCurrentPrivateRefCountArrayKeysRef() = &fake_backend2;
		*PgCurrentPrivateRefCountArrayRef() = &fake_backend2;
		*PgCurrentPrivateRefCountHashRef() = &fake_backend2;
		*PgCurrentPrivateRefCountOverflowedRef() = 207;
		*PgCurrentPrivateRefCountClockRef() = 208;
		*PgCurrentReservedRefCountSlotRef() = 209;
		*PgCurrentPrivateRefCountEntryLastRef() = 210;
		*PgCurrentMaxProportionalPinsRef() = 211;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && PgCurrentBackendWritebackContextRef() == fake_backend1_writeback;
		ok = ok && *PgCurrentNLocBufferRef() == 101;
		ok = ok && *PgCurrentLocalBufferDescriptorsRef() == &fake_backend1;
		ok = ok && *PgCurrentLocalBufferBlockPointersRef() == &fake_backend1;
		ok = ok && *PgCurrentLocalRefCountRef() == (int32 *) &fake_backend1;
		ok = ok && *PgCurrentNextFreeLocalBufIdRef() == 102;
		ok = ok && *PgCurrentLocalBufHashRef() == (HTAB *) &fake_backend1;
		ok = ok && *PgCurrentNLocalPinnedBuffersRef() == 103;
		ok = ok && *PgCurrentLocalBufferCurBlockRef() == (char *) &fake_backend1;
		ok = ok && *PgCurrentLocalBufferNextBufInBlockRef() == 104;
		ok = ok && *PgCurrentLocalBufferNumBufsInBlockRef() == 105;
		ok = ok && *PgCurrentLocalBufferTotalBufsAllocatedRef() == 106;
		ok = ok && *PgCurrentLocalBufferContextRef() == (MemoryContext) &fake_backend1;
		ok = ok && *PgCurrentPinCountWaitBufRef() == (BufferDesc *) &fake_backend1;
		ok = ok && *PgCurrentPrivateRefCountArrayKeysRef() == &fake_backend1;
		ok = ok && *PgCurrentPrivateRefCountArrayRef() == &fake_backend1;
		ok = ok && *PgCurrentPrivateRefCountHashRef() == &fake_backend1;
		ok = ok && *PgCurrentPrivateRefCountOverflowedRef() == 107;
		ok = ok && *PgCurrentPrivateRefCountClockRef() == 108;
		ok = ok && *PgCurrentReservedRefCountSlotRef() == 109;
		ok = ok && *PgCurrentPrivateRefCountEntryLastRef() == 110;
		ok = ok && *PgCurrentMaxProportionalPinsRef() == 111;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && PgCurrentBackendWritebackContextRef() == fake_backend2_writeback;
		ok = ok && *PgCurrentNLocBufferRef() == 201;
		ok = ok && *PgCurrentLocalBufferDescriptorsRef() == &fake_backend2;
		ok = ok && *PgCurrentLocalBufferBlockPointersRef() == &fake_backend2;
		ok = ok && *PgCurrentLocalRefCountRef() == (int32 *) &fake_backend2;
		ok = ok && *PgCurrentNextFreeLocalBufIdRef() == 202;
		ok = ok && *PgCurrentLocalBufHashRef() == (HTAB *) &fake_backend2;
		ok = ok && *PgCurrentNLocalPinnedBuffersRef() == 203;
		ok = ok && *PgCurrentLocalBufferCurBlockRef() == (char *) &fake_backend2;
		ok = ok && *PgCurrentLocalBufferNextBufInBlockRef() == 204;
		ok = ok && *PgCurrentLocalBufferNumBufsInBlockRef() == 205;
		ok = ok && *PgCurrentLocalBufferTotalBufsAllocatedRef() == 206;
		ok = ok && *PgCurrentLocalBufferContextRef() == (MemoryContext) &fake_backend2;
		ok = ok && *PgCurrentPinCountWaitBufRef() == (BufferDesc *) &fake_backend2;
		ok = ok && *PgCurrentPrivateRefCountArrayKeysRef() == &fake_backend2;
		ok = ok && *PgCurrentPrivateRefCountArrayRef() == &fake_backend2;
		ok = ok && *PgCurrentPrivateRefCountHashRef() == &fake_backend2;
		ok = ok && *PgCurrentPrivateRefCountOverflowedRef() == 207;
		ok = ok && *PgCurrentPrivateRefCountClockRef() == 208;
		ok = ok && *PgCurrentReservedRefCountSlotRef() == 209;
		ok = ok && *PgCurrentPrivateRefCountEntryLastRef() == 210;
		ok = ok && *PgCurrentMaxProportionalPinsRef() == 211;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend buffer state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_storage_state_is_backend_local);
Datum
test_backend_storage_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	dlist_init(&fake_backend1.storage.smgr_unpinned_relations);
	dlist_init(&fake_backend2.storage.smgr_unpinned_relations);

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		*PgCurrentVfdCacheRef() = &fake_backend1;
		*PgCurrentSizeVfdCacheRef() = 101;
		*PgCurrentNFileRef() = 102;
		*PgCurrentTemporaryFilesAllowedRef() = true;
		*PgCurrentNumAllocatedDescsRef() = 103;
		*PgCurrentMaxAllocatedDescsRef() = 104;
		*PgCurrentAllocatedDescsRef() = &fake_backend1;
		*PgCurrentNumExternalFDsRef() = 105;
		*PgCurrentSyncPendingOpsRef() = (HTAB *) &fake_backend1;
		*PgCurrentSyncPendingUnlinksRef() = (List *) &fake_backend1;
		*PgCurrentSyncPendingOpsContextRef() = (MemoryContext) &fake_backend1;
		*PgCurrentSyncCycleCounterRef() = 11;
		*PgCurrentSyncCheckpointCycleCounterRef() = 12;
		*PgCurrentSyncInProgressRef() = true;
		*PgCurrentSMgrRelationHashRef() = (HTAB *) &fake_backend1;
		*PgCurrentMdContextRef() = (MemoryContext) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentVfdCacheRef() == NULL;
		ok = ok && *PgCurrentSizeVfdCacheRef() == 0;
		ok = ok && *PgCurrentNFileRef() == 0;
		ok = ok && !*PgCurrentTemporaryFilesAllowedRef();
		ok = ok && *PgCurrentNumAllocatedDescsRef() == 0;
		ok = ok && *PgCurrentMaxAllocatedDescsRef() == 0;
		ok = ok && *PgCurrentAllocatedDescsRef() == NULL;
		ok = ok && *PgCurrentNumExternalFDsRef() == 0;
		ok = ok && *PgCurrentSyncPendingOpsRef() == NULL;
		ok = ok && *PgCurrentSyncPendingUnlinksRef() == NIL;
		ok = ok && *PgCurrentSyncPendingOpsContextRef() == NULL;
		ok = ok && *PgCurrentSyncCycleCounterRef() == 0;
		ok = ok && *PgCurrentSyncCheckpointCycleCounterRef() == 0;
		ok = ok && !*PgCurrentSyncInProgressRef();
		ok = ok && *PgCurrentSMgrRelationHashRef() == NULL;
		ok = ok && PgCurrentSMgrUnpinnedRelationsRef() ==
			&fake_backend2.storage.smgr_unpinned_relations;
		ok = ok && dlist_is_empty(PgCurrentSMgrUnpinnedRelationsRef());
		ok = ok && *PgCurrentMdContextRef() == NULL;

		*PgCurrentVfdCacheRef() = &fake_backend2;
		*PgCurrentSizeVfdCacheRef() = 201;
		*PgCurrentNFileRef() = 202;
		*PgCurrentTemporaryFilesAllowedRef() = true;
		*PgCurrentNumAllocatedDescsRef() = 203;
		*PgCurrentMaxAllocatedDescsRef() = 204;
		*PgCurrentAllocatedDescsRef() = &fake_backend2;
		*PgCurrentNumExternalFDsRef() = 205;
		*PgCurrentSyncPendingOpsRef() = (HTAB *) &fake_backend2;
		*PgCurrentSyncPendingUnlinksRef() = (List *) &fake_backend2;
		*PgCurrentSyncPendingOpsContextRef() = (MemoryContext) &fake_backend2;
		*PgCurrentSyncCycleCounterRef() = 21;
		*PgCurrentSyncCheckpointCycleCounterRef() = 22;
		*PgCurrentSyncInProgressRef() = true;
		*PgCurrentSMgrRelationHashRef() = (HTAB *) &fake_backend2;
		*PgCurrentMdContextRef() = (MemoryContext) &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && *PgCurrentVfdCacheRef() == &fake_backend1;
		ok = ok && *PgCurrentSizeVfdCacheRef() == 101;
		ok = ok && *PgCurrentNFileRef() == 102;
		ok = ok && *PgCurrentTemporaryFilesAllowedRef();
		ok = ok && *PgCurrentNumAllocatedDescsRef() == 103;
		ok = ok && *PgCurrentMaxAllocatedDescsRef() == 104;
		ok = ok && *PgCurrentAllocatedDescsRef() == &fake_backend1;
		ok = ok && *PgCurrentNumExternalFDsRef() == 105;
		ok = ok && *PgCurrentSyncPendingOpsRef() == (HTAB *) &fake_backend1;
		ok = ok && *PgCurrentSyncPendingUnlinksRef() == (List *) &fake_backend1;
		ok = ok && *PgCurrentSyncPendingOpsContextRef() == (MemoryContext) &fake_backend1;
		ok = ok && *PgCurrentSyncCycleCounterRef() == 11;
		ok = ok && *PgCurrentSyncCheckpointCycleCounterRef() == 12;
		ok = ok && *PgCurrentSyncInProgressRef();
		ok = ok && *PgCurrentSMgrRelationHashRef() == (HTAB *) &fake_backend1;
		ok = ok && PgCurrentSMgrUnpinnedRelationsRef() ==
			&fake_backend1.storage.smgr_unpinned_relations;
		ok = ok && dlist_is_empty(PgCurrentSMgrUnpinnedRelationsRef());
		ok = ok && *PgCurrentMdContextRef() == (MemoryContext) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentVfdCacheRef() == &fake_backend2;
		ok = ok && *PgCurrentSizeVfdCacheRef() == 201;
		ok = ok && *PgCurrentNFileRef() == 202;
		ok = ok && *PgCurrentTemporaryFilesAllowedRef();
		ok = ok && *PgCurrentNumAllocatedDescsRef() == 203;
		ok = ok && *PgCurrentMaxAllocatedDescsRef() == 204;
		ok = ok && *PgCurrentAllocatedDescsRef() == &fake_backend2;
		ok = ok && *PgCurrentNumExternalFDsRef() == 205;
		ok = ok && *PgCurrentSyncPendingOpsRef() == (HTAB *) &fake_backend2;
		ok = ok && *PgCurrentSyncPendingUnlinksRef() == (List *) &fake_backend2;
		ok = ok && *PgCurrentSyncPendingOpsContextRef() == (MemoryContext) &fake_backend2;
		ok = ok && *PgCurrentSyncCycleCounterRef() == 21;
		ok = ok && *PgCurrentSyncCheckpointCycleCounterRef() == 22;
		ok = ok && *PgCurrentSyncInProgressRef();
		ok = ok && *PgCurrentSMgrRelationHashRef() == (HTAB *) &fake_backend2;
		ok = ok && PgCurrentSMgrUnpinnedRelationsRef() ==
			&fake_backend2.storage.smgr_unpinned_relations;
		ok = ok && dlist_is_empty(PgCurrentSMgrUnpinnedRelationsRef());
		ok = ok && *PgCurrentMdContextRef() == (MemoryContext) &fake_backend2;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend storage state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_lock_state_is_backend_local);
Datum
test_backend_lock_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	int			fast_path_counts1[FP_LOCK_GROUPS_PER_BACKEND_MAX] = {0};
	int			fast_path_counts2[FP_LOCK_GROUPS_PER_BACKEND_MAX] = {0};
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		*PgCurrentNumHeldLWLocksRef() = 1;
		PgCurrentHeldLWLocks()[0].lock = (LWLock *) &fake_backend1;
		PgCurrentHeldLWLocks()[0].mode = LW_EXCLUSIVE;
		*PgCurrentLocalNumUserDefinedLWLockTranchesRef() = 11;
		*PgCurrentLWLockStatsHashRef() = (HTAB *) &fake_backend1;
		PgCurrentLWLockStatsDummy()->key.tranche = 12;
		PgCurrentLWLockStatsDummy()->key.instance = &fake_backend1;
		PgCurrentLWLockStatsDummy()->sh_acquire_count = 13;
		*PgCurrentLWLockStatsContextRef() = (MemoryContext) &fake_backend1;
		*PgCurrentLWLockStatsExitRegisteredRef() = true;
		*PgCurrentFastPathLocalUseCountsRef() = fast_path_counts1;
		*PgCurrentFastPathLocalUseCountsOwnedRef() = true;
		fast_path_counts1[0] = 101;
		*PgCurrentRelationExtensionLockHeldRef() = true;
		*PgCurrentLockMethodLocalHashRef() = (HTAB *) &fake_backend1;
		*PgCurrentStrongLockInProgressRef() = &fake_backend1;
		*PgCurrentAwaitedLockRef() = &fake_backend1;
		*PgCurrentAwaitedOwnerRef() = &fake_backend1;
		*PgCurrentDeadlockTimeoutPendingRef() = true;
		*PgCurrentConditionVariableSleepTargetRef() = &fake_backend1;
		*PgCurrentSpeculativeInsertionTokenRef() = 108;
		*PgCurrentDeadlockVisitedProcsRef() = &fake_backend1;
		*PgCurrentDeadlockNVisitedProcsRef() = 101;
		*PgCurrentDeadlockTopoProcsRef() = &fake_backend1;
		*PgCurrentDeadlockBeforeConstraintsRef() = &fake_backend1;
		*PgCurrentDeadlockAfterConstraintsRef() = &fake_backend1;
		*PgCurrentDeadlockWaitOrdersRef() = &fake_backend1;
		*PgCurrentDeadlockNWaitOrdersRef() = 102;
		*PgCurrentDeadlockWaitOrderProcsRef() = &fake_backend1;
		*PgCurrentDeadlockCurConstraintsRef() = &fake_backend1;
		*PgCurrentDeadlockNCurConstraintsRef() = 103;
		*PgCurrentDeadlockMaxCurConstraintsRef() = 104;
		*PgCurrentDeadlockPossibleConstraintsRef() = &fake_backend1;
		*PgCurrentDeadlockNPossibleConstraintsRef() = 105;
		*PgCurrentDeadlockMaxPossibleConstraintsRef() = 106;
		*PgCurrentDeadlockDetailsRef() = &fake_backend1;
		*PgCurrentDeadlockNDetailsRef() = 107;
		*PgCurrentDeadlockWorkspaceOwnedRef() = true;
		*PgCurrentBlockingAutovacuumProcRef() = &fake_backend1;
		*PgCurrentLocalPredicateLockHashRef() = (HTAB *) &fake_backend1;
		*PgCurrentMySerializableXactRef() = &fake_backend1;
		*PgCurrentMyXactDidWriteRef() = true;
		*PgCurrentSavedSerializableXactRef() = &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentNumHeldLWLocksRef() == 0;
		ok = ok && PgCurrentHeldLWLocks()[0].lock == NULL;
		ok = ok && PgCurrentHeldLWLocks()[0].mode == 0;
		ok = ok && *PgCurrentLocalNumUserDefinedLWLockTranchesRef() == 0;
		ok = ok && *PgCurrentLWLockStatsHashRef() == NULL;
		ok = ok && PgCurrentLWLockStatsDummy()->key.tranche == 0;
		ok = ok && PgCurrentLWLockStatsDummy()->key.instance == NULL;
		ok = ok && PgCurrentLWLockStatsDummy()->sh_acquire_count == 0;
		ok = ok && *PgCurrentLWLockStatsContextRef() == NULL;
		ok = ok && !*PgCurrentLWLockStatsExitRegisteredRef();
		ok = ok && *PgCurrentFastPathLocalUseCountsRef() == NULL;
		ok = ok && !*PgCurrentFastPathLocalUseCountsOwnedRef();
		ok = ok && !*PgCurrentRelationExtensionLockHeldRef();
		ok = ok && *PgCurrentLockMethodLocalHashRef() == NULL;
		ok = ok && *PgCurrentStrongLockInProgressRef() == NULL;
		ok = ok && *PgCurrentAwaitedLockRef() == NULL;
		ok = ok && *PgCurrentAwaitedOwnerRef() == NULL;
		ok = ok && !*PgCurrentDeadlockTimeoutPendingRef();
		ok = ok && *PgCurrentConditionVariableSleepTargetRef() == NULL;
		ok = ok && *PgCurrentSpeculativeInsertionTokenRef() == 0;
		ok = ok && *PgCurrentDeadlockVisitedProcsRef() == NULL;
		ok = ok && *PgCurrentDeadlockNVisitedProcsRef() == 0;
		ok = ok && *PgCurrentDeadlockTopoProcsRef() == NULL;
		ok = ok && *PgCurrentDeadlockBeforeConstraintsRef() == NULL;
		ok = ok && *PgCurrentDeadlockAfterConstraintsRef() == NULL;
		ok = ok && *PgCurrentDeadlockWaitOrdersRef() == NULL;
		ok = ok && *PgCurrentDeadlockNWaitOrdersRef() == 0;
		ok = ok && *PgCurrentDeadlockWaitOrderProcsRef() == NULL;
		ok = ok && *PgCurrentDeadlockCurConstraintsRef() == NULL;
		ok = ok && *PgCurrentDeadlockNCurConstraintsRef() == 0;
		ok = ok && *PgCurrentDeadlockMaxCurConstraintsRef() == 0;
		ok = ok && *PgCurrentDeadlockPossibleConstraintsRef() == NULL;
		ok = ok && *PgCurrentDeadlockNPossibleConstraintsRef() == 0;
		ok = ok && *PgCurrentDeadlockMaxPossibleConstraintsRef() == 0;
		ok = ok && *PgCurrentDeadlockDetailsRef() == NULL;
		ok = ok && *PgCurrentDeadlockNDetailsRef() == 0;
		ok = ok && !*PgCurrentDeadlockWorkspaceOwnedRef();
		ok = ok && *PgCurrentBlockingAutovacuumProcRef() == NULL;
		ok = ok && *PgCurrentLocalPredicateLockHashRef() == NULL;
		ok = ok && *PgCurrentMySerializableXactRef() == NULL;
		ok = ok && !*PgCurrentMyXactDidWriteRef();
		ok = ok && *PgCurrentSavedSerializableXactRef() == NULL;

		*PgCurrentNumHeldLWLocksRef() = 1;
		PgCurrentHeldLWLocks()[0].lock = (LWLock *) &fake_backend2;
		PgCurrentHeldLWLocks()[0].mode = LW_SHARED;
		*PgCurrentLocalNumUserDefinedLWLockTranchesRef() = 22;
		*PgCurrentLWLockStatsHashRef() = (HTAB *) &fake_backend2;
		PgCurrentLWLockStatsDummy()->key.tranche = 23;
		PgCurrentLWLockStatsDummy()->key.instance = &fake_backend2;
		PgCurrentLWLockStatsDummy()->sh_acquire_count = 24;
		*PgCurrentLWLockStatsContextRef() = (MemoryContext) &fake_backend2;
		*PgCurrentLWLockStatsExitRegisteredRef() = false;
		*PgCurrentFastPathLocalUseCountsRef() = fast_path_counts2;
		*PgCurrentFastPathLocalUseCountsOwnedRef() = false;
		fast_path_counts2[0] = 201;
		*PgCurrentRelationExtensionLockHeldRef() = false;
		*PgCurrentLockMethodLocalHashRef() = (HTAB *) &fake_backend2;
		*PgCurrentStrongLockInProgressRef() = &fake_backend2;
		*PgCurrentAwaitedLockRef() = &fake_backend2;
		*PgCurrentAwaitedOwnerRef() = &fake_backend2;
		*PgCurrentDeadlockTimeoutPendingRef() = false;
		*PgCurrentConditionVariableSleepTargetRef() = &fake_backend2;
		*PgCurrentSpeculativeInsertionTokenRef() = 208;
		*PgCurrentDeadlockVisitedProcsRef() = &fake_backend2;
		*PgCurrentDeadlockNVisitedProcsRef() = 201;
		*PgCurrentDeadlockTopoProcsRef() = &fake_backend2;
		*PgCurrentDeadlockBeforeConstraintsRef() = &fake_backend2;
		*PgCurrentDeadlockAfterConstraintsRef() = &fake_backend2;
		*PgCurrentDeadlockWaitOrdersRef() = &fake_backend2;
		*PgCurrentDeadlockNWaitOrdersRef() = 202;
		*PgCurrentDeadlockWaitOrderProcsRef() = &fake_backend2;
		*PgCurrentDeadlockCurConstraintsRef() = &fake_backend2;
		*PgCurrentDeadlockNCurConstraintsRef() = 203;
		*PgCurrentDeadlockMaxCurConstraintsRef() = 204;
		*PgCurrentDeadlockPossibleConstraintsRef() = &fake_backend2;
		*PgCurrentDeadlockNPossibleConstraintsRef() = 205;
		*PgCurrentDeadlockMaxPossibleConstraintsRef() = 206;
		*PgCurrentDeadlockDetailsRef() = &fake_backend2;
		*PgCurrentDeadlockNDetailsRef() = 207;
		*PgCurrentDeadlockWorkspaceOwnedRef() = false;
		*PgCurrentBlockingAutovacuumProcRef() = &fake_backend2;
		*PgCurrentLocalPredicateLockHashRef() = (HTAB *) &fake_backend2;
		*PgCurrentMySerializableXactRef() = &fake_backend2;
		*PgCurrentMyXactDidWriteRef() = false;
		*PgCurrentSavedSerializableXactRef() = &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && *PgCurrentNumHeldLWLocksRef() == 1;
		ok = ok && PgCurrentHeldLWLocks()[0].lock == (LWLock *) &fake_backend1;
		ok = ok && PgCurrentHeldLWLocks()[0].mode == LW_EXCLUSIVE;
		ok = ok && *PgCurrentLocalNumUserDefinedLWLockTranchesRef() == 11;
		ok = ok && *PgCurrentLWLockStatsHashRef() == (HTAB *) &fake_backend1;
		ok = ok && PgCurrentLWLockStatsDummy()->key.tranche == 12;
		ok = ok && PgCurrentLWLockStatsDummy()->key.instance == &fake_backend1;
		ok = ok && PgCurrentLWLockStatsDummy()->sh_acquire_count == 13;
		ok = ok && *PgCurrentLWLockStatsContextRef() == (MemoryContext) &fake_backend1;
		ok = ok && *PgCurrentLWLockStatsExitRegisteredRef();
		ok = ok && *PgCurrentFastPathLocalUseCountsRef() == fast_path_counts1;
		ok = ok && *PgCurrentFastPathLocalUseCountsOwnedRef();
		ok = ok && ((int *) *PgCurrentFastPathLocalUseCountsRef())[0] == 101;
		ok = ok && *PgCurrentRelationExtensionLockHeldRef();
		ok = ok && *PgCurrentLockMethodLocalHashRef() == (HTAB *) &fake_backend1;
		ok = ok && *PgCurrentStrongLockInProgressRef() == &fake_backend1;
		ok = ok && *PgCurrentAwaitedLockRef() == &fake_backend1;
		ok = ok && *PgCurrentAwaitedOwnerRef() == &fake_backend1;
		ok = ok && *PgCurrentDeadlockTimeoutPendingRef();
		ok = ok && *PgCurrentConditionVariableSleepTargetRef() == &fake_backend1;
		ok = ok && *PgCurrentSpeculativeInsertionTokenRef() == 108;
		ok = ok && *PgCurrentDeadlockVisitedProcsRef() == &fake_backend1;
		ok = ok && *PgCurrentDeadlockNVisitedProcsRef() == 101;
		ok = ok && *PgCurrentDeadlockTopoProcsRef() == &fake_backend1;
		ok = ok && *PgCurrentDeadlockBeforeConstraintsRef() == &fake_backend1;
		ok = ok && *PgCurrentDeadlockAfterConstraintsRef() == &fake_backend1;
		ok = ok && *PgCurrentDeadlockWaitOrdersRef() == &fake_backend1;
		ok = ok && *PgCurrentDeadlockNWaitOrdersRef() == 102;
		ok = ok && *PgCurrentDeadlockWaitOrderProcsRef() == &fake_backend1;
		ok = ok && *PgCurrentDeadlockCurConstraintsRef() == &fake_backend1;
		ok = ok && *PgCurrentDeadlockNCurConstraintsRef() == 103;
		ok = ok && *PgCurrentDeadlockMaxCurConstraintsRef() == 104;
		ok = ok && *PgCurrentDeadlockPossibleConstraintsRef() == &fake_backend1;
		ok = ok && *PgCurrentDeadlockNPossibleConstraintsRef() == 105;
		ok = ok && *PgCurrentDeadlockMaxPossibleConstraintsRef() == 106;
		ok = ok && *PgCurrentDeadlockDetailsRef() == &fake_backend1;
		ok = ok && *PgCurrentDeadlockNDetailsRef() == 107;
		ok = ok && *PgCurrentDeadlockWorkspaceOwnedRef();
		ok = ok && *PgCurrentBlockingAutovacuumProcRef() == &fake_backend1;
		ok = ok && *PgCurrentLocalPredicateLockHashRef() == (HTAB *) &fake_backend1;
		ok = ok && *PgCurrentMySerializableXactRef() == &fake_backend1;
		ok = ok && *PgCurrentMyXactDidWriteRef();
		ok = ok && *PgCurrentSavedSerializableXactRef() == &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentNumHeldLWLocksRef() == 1;
		ok = ok && PgCurrentHeldLWLocks()[0].lock == (LWLock *) &fake_backend2;
		ok = ok && PgCurrentHeldLWLocks()[0].mode == LW_SHARED;
		ok = ok && *PgCurrentLocalNumUserDefinedLWLockTranchesRef() == 22;
		ok = ok && *PgCurrentLWLockStatsHashRef() == (HTAB *) &fake_backend2;
		ok = ok && PgCurrentLWLockStatsDummy()->key.tranche == 23;
		ok = ok && PgCurrentLWLockStatsDummy()->key.instance == &fake_backend2;
		ok = ok && PgCurrentLWLockStatsDummy()->sh_acquire_count == 24;
		ok = ok && *PgCurrentLWLockStatsContextRef() == (MemoryContext) &fake_backend2;
		ok = ok && !*PgCurrentLWLockStatsExitRegisteredRef();
		ok = ok && *PgCurrentFastPathLocalUseCountsRef() == fast_path_counts2;
		ok = ok && !*PgCurrentFastPathLocalUseCountsOwnedRef();
		ok = ok && ((int *) *PgCurrentFastPathLocalUseCountsRef())[0] == 201;
		ok = ok && !*PgCurrentRelationExtensionLockHeldRef();
		ok = ok && *PgCurrentLockMethodLocalHashRef() == (HTAB *) &fake_backend2;
		ok = ok && *PgCurrentStrongLockInProgressRef() == &fake_backend2;
		ok = ok && *PgCurrentAwaitedLockRef() == &fake_backend2;
		ok = ok && *PgCurrentAwaitedOwnerRef() == &fake_backend2;
		ok = ok && !*PgCurrentDeadlockTimeoutPendingRef();
		ok = ok && *PgCurrentConditionVariableSleepTargetRef() == &fake_backend2;
		ok = ok && *PgCurrentSpeculativeInsertionTokenRef() == 208;
		ok = ok && *PgCurrentDeadlockVisitedProcsRef() == &fake_backend2;
		ok = ok && *PgCurrentDeadlockNVisitedProcsRef() == 201;
		ok = ok && *PgCurrentDeadlockTopoProcsRef() == &fake_backend2;
		ok = ok && *PgCurrentDeadlockBeforeConstraintsRef() == &fake_backend2;
		ok = ok && *PgCurrentDeadlockAfterConstraintsRef() == &fake_backend2;
		ok = ok && *PgCurrentDeadlockWaitOrdersRef() == &fake_backend2;
		ok = ok && *PgCurrentDeadlockNWaitOrdersRef() == 202;
		ok = ok && *PgCurrentDeadlockWaitOrderProcsRef() == &fake_backend2;
		ok = ok && *PgCurrentDeadlockCurConstraintsRef() == &fake_backend2;
		ok = ok && *PgCurrentDeadlockNCurConstraintsRef() == 203;
		ok = ok && *PgCurrentDeadlockMaxCurConstraintsRef() == 204;
		ok = ok && *PgCurrentDeadlockPossibleConstraintsRef() == &fake_backend2;
		ok = ok && *PgCurrentDeadlockNPossibleConstraintsRef() == 205;
		ok = ok && *PgCurrentDeadlockMaxPossibleConstraintsRef() == 206;
		ok = ok && *PgCurrentDeadlockDetailsRef() == &fake_backend2;
		ok = ok && *PgCurrentDeadlockNDetailsRef() == 207;
		ok = ok && !*PgCurrentDeadlockWorkspaceOwnedRef();
		ok = ok && *PgCurrentBlockingAutovacuumProcRef() == &fake_backend2;
		ok = ok && *PgCurrentLocalPredicateLockHashRef() == (HTAB *) &fake_backend2;
		ok = ok && *PgCurrentMySerializableXactRef() == &fake_backend2;
		ok = ok && !*PgCurrentMyXactDidWriteRef();
		ok = ok && *PgCurrentSavedSerializableXactRef() == &fake_backend2;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend lock state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_ipc_state_is_backend_local);
Datum
test_backend_ipc_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		*PgCurrentProcSignalSlotRef() = &fake_backend1;
		SharedInvalidMessageCounter = 101;
		catchupInterruptPending = true;
		*PgCurrentSharedInvalidationMessagesRef() = &fake_backend1;
		*PgCurrentSharedInvalidationNextMsgRef() = 102;
		*PgCurrentSharedInvalidationNumMsgsRef() = 103;
		*PgCurrentDsmInitDoneRef() = true;
		*PgCurrentDsmRegistryDsaRef() = &fake_backend1;
		*PgCurrentDsmRegistryTableRef() = &fake_backend1;
		*PgCurrentNextLocalTransactionIdRef() = 104;
		*PgCurrentLatchWaitSetRef() = (WaitEventSet *) &fake_backend1;
		PgCurrentLocalLatchData()->is_set = true;
		PgCurrentLocalLatchData()->owner_pid = 111;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentProcSignalSlotRef() == NULL;
		ok = ok && SharedInvalidMessageCounter == 0;
		ok = ok && !catchupInterruptPending;
		ok = ok && *PgCurrentSharedInvalidationMessagesRef() == NULL;
		ok = ok && *PgCurrentSharedInvalidationNextMsgRef() == 0;
		ok = ok && *PgCurrentSharedInvalidationNumMsgsRef() == 0;
		ok = ok && !*PgCurrentDsmInitDoneRef();
		ok = ok && *PgCurrentDsmRegistryDsaRef() == NULL;
		ok = ok && *PgCurrentDsmRegistryTableRef() == NULL;
		ok = ok && *PgCurrentNextLocalTransactionIdRef() == 0;
		ok = ok && *PgCurrentLatchWaitSetRef() == NULL;
		ok = ok && !PgCurrentLocalLatchData()->is_set;
		ok = ok && PgCurrentLocalLatchData()->owner_pid == 0;

		*PgCurrentProcSignalSlotRef() = &fake_backend2;
		SharedInvalidMessageCounter = 201;
		catchupInterruptPending = false;
		*PgCurrentSharedInvalidationMessagesRef() = &fake_backend2;
		*PgCurrentSharedInvalidationNextMsgRef() = 202;
		*PgCurrentSharedInvalidationNumMsgsRef() = 203;
		*PgCurrentDsmInitDoneRef() = false;
		*PgCurrentDsmRegistryDsaRef() = &fake_backend2;
		*PgCurrentDsmRegistryTableRef() = &fake_backend2;
		*PgCurrentNextLocalTransactionIdRef() = 204;
		*PgCurrentLatchWaitSetRef() = (WaitEventSet *) &fake_backend2;
		PgCurrentLocalLatchData()->is_set = false;
		PgCurrentLocalLatchData()->owner_pid = 222;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && *PgCurrentProcSignalSlotRef() == &fake_backend1;
		ok = ok && SharedInvalidMessageCounter == 101;
		ok = ok && catchupInterruptPending;
		ok = ok && *PgCurrentSharedInvalidationMessagesRef() == &fake_backend1;
		ok = ok && *PgCurrentSharedInvalidationNextMsgRef() == 102;
		ok = ok && *PgCurrentSharedInvalidationNumMsgsRef() == 103;
		ok = ok && *PgCurrentDsmInitDoneRef();
		ok = ok && *PgCurrentDsmRegistryDsaRef() == &fake_backend1;
		ok = ok && *PgCurrentDsmRegistryTableRef() == &fake_backend1;
		ok = ok && *PgCurrentNextLocalTransactionIdRef() == 104;
		ok = ok && *PgCurrentLatchWaitSetRef() == (WaitEventSet *) &fake_backend1;
		ok = ok && PgCurrentLocalLatchData()->is_set;
		ok = ok && PgCurrentLocalLatchData()->owner_pid == 111;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentProcSignalSlotRef() == &fake_backend2;
		ok = ok && SharedInvalidMessageCounter == 201;
		ok = ok && !catchupInterruptPending;
		ok = ok && *PgCurrentSharedInvalidationMessagesRef() == &fake_backend2;
		ok = ok && *PgCurrentSharedInvalidationNextMsgRef() == 202;
		ok = ok && *PgCurrentSharedInvalidationNumMsgsRef() == 203;
		ok = ok && !*PgCurrentDsmInitDoneRef();
		ok = ok && *PgCurrentDsmRegistryDsaRef() == &fake_backend2;
		ok = ok && *PgCurrentDsmRegistryTableRef() == &fake_backend2;
		ok = ok && *PgCurrentNextLocalTransactionIdRef() == 204;
		ok = ok && *PgCurrentLatchWaitSetRef() == (WaitEventSet *) &fake_backend2;
		ok = ok && !PgCurrentLocalLatchData()->is_set;
		ok = ok && PgCurrentLocalLatchData()->owner_pid == 222;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend IPC state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_wait_state_is_backend_local);
Datum
test_backend_wait_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	uint32		external_wait_event1 = 0;
	uint32		external_wait_event2 = 0;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		ok = ok && *PgCurrentMyWaitEventInfoRef() ==
			PgCurrentLocalWaitEventInfoRef();
		pgstat_report_wait_start(0x01000011);
		ok = ok && *PgCurrentLocalWaitEventInfoRef() == 0x01000011;
		pgstat_report_wait_end();
		ok = ok && *PgCurrentLocalWaitEventInfoRef() == 0;
		my_wait_event_info = &external_wait_event1;
		pgstat_report_wait_start(0x02000022);
		ok = ok && external_wait_event1 == 0x02000022;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentMyWaitEventInfoRef() ==
			PgCurrentLocalWaitEventInfoRef();
		ok = ok && external_wait_event2 == 0;
		my_wait_event_info = &external_wait_event2;
		pgstat_report_wait_start(0x03000033);
		ok = ok && external_wait_event2 == 0x03000033;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && *PgCurrentMyWaitEventInfoRef() == &external_wait_event1;
		ok = ok && external_wait_event1 == 0x02000022;
		pgstat_report_wait_end();
		ok = ok && external_wait_event1 == 0;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentMyWaitEventInfoRef() == &external_wait_event2;
		ok = ok && external_wait_event2 == 0x03000033;
		pgstat_reset_wait_event_storage();
		ok = ok && *PgCurrentMyWaitEventInfoRef() ==
			PgCurrentLocalWaitEventInfoRef();

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend wait state was not backend-local");

	PG_RETURN_BOOL(true);
}

typedef struct TestWaitCompletionContext
{
	PgBackend  *backend;
	PgSession  *session;
	PgExecution *execution;
	bool		saw_published_wait;
	bool		saw_cancel_interrupt;
	bool		saw_termination_interrupt;
	bool		saw_ready_wait;
} TestWaitCompletionContext;

typedef struct TestWaitCompletionPolicyContext
{
	PgBackend  *backend;
	bool		saw_published_wait;
	bool		saw_unpublished_wait;
} TestWaitCompletionPolicyContext;

static int
test_wait_completion_callback(void *callback_arg)
{
	TestWaitCompletionContext *context;
	PgWaitCompletion *completion;
	uint32		interrupt_events;

	context = (TestWaitCompletionContext *) callback_arg;
	completion = PgBackendCurrentWaitCompletion(context->backend);
	interrupt_events = pg_atomic_read_u32(&completion->interrupt_events);

	context->saw_published_wait =
		completion != NULL &&
		completion->backend == context->backend &&
		completion->session == context->session &&
		completion->execution == context->execution &&
		completion->spec.kind == PG_WAIT_KIND_EVENT_SET &&
		completion->spec.wait_event_info == 0x0A0B0C0D &&
		completion->spec.wake_events == (WL_LATCH_SET | WL_TIMEOUT) &&
		completion->spec.socket == 123 &&
		completion->spec.timeout == 42 &&
		context->backend->wait_state.spec.kind == PG_WAIT_KIND_EVENT_SET &&
		pg_atomic_read_u32(&context->backend->wait_state.waiting) == 1 &&
		pg_atomic_read_u32(&completion->state) == PG_WAIT_COMPLETION_WAITING &&
		pg_atomic_read_u32(&completion->ready_events) == 0 &&
		(interrupt_events & PG_WAIT_COMPLETION_INTERRUPT_CANCEL) != 0 &&
		(interrupt_events & PG_WAIT_COMPLETION_INTERRUPT_TERMINATE) == 0;

	SendInterrupt(context->backend, PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	SendInterrupt(context->backend, PG_BACKEND_INTERRUPT_PROC_DIE);
	interrupt_events = pg_atomic_read_u32(&completion->interrupt_events);
	context->saw_cancel_interrupt =
		(interrupt_events & PG_WAIT_COMPLETION_INTERRUPT_CANCEL) != 0;
	context->saw_termination_interrupt =
		(interrupt_events & PG_WAIT_COMPLETION_INTERRUPT_TERMINATE) != 0;

	context->saw_ready_wait =
		PgBackendWakeWaitCompletion(context->backend, WL_LATCH_SET) &&
		pg_atomic_read_u32(&completion->state) == PG_WAIT_COMPLETION_READY &&
		(pg_atomic_read_u32(&completion->ready_events) & WL_LATCH_SET) != 0;

	return 42;
}

static int
test_wait_completion_policy_callback(void *callback_arg)
{
	TestWaitCompletionPolicyContext *context;
	PgWaitCompletion *completion;

	context = (TestWaitCompletionPolicyContext *) callback_arg;
	completion = PgBackendCurrentWaitCompletion(context->backend);

	context->saw_published_wait =
		completion != NULL &&
		pg_atomic_read_u32(&context->backend->wait_state.waiting) == 1 &&
		pg_atomic_read_u32(&completion->state) == PG_WAIT_COMPLETION_WAITING &&
		completion->backend == context->backend &&
		completion->spec.kind == PG_WAIT_KIND_EVENT_SET;
	context->saw_unpublished_wait =
		completion != NULL &&
		pg_atomic_read_u32(&context->backend->wait_state.waiting) == 0 &&
		pg_atomic_read_u32(&completion->state) == PG_WAIT_COMPLETION_INACTIVE &&
		completion->backend == NULL &&
		completion->spec.kind == PG_WAIT_KIND_NONE;

	return 7;
}

PG_FUNCTION_INFO_V1(test_backend_wait_completion_publication);
Datum
test_backend_wait_completion_publication(PG_FUNCTION_ARGS)
{
#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgExecution *saved_execution;
	PgBackend	fake_backend;
	PgSession	fake_session;
	PgExecution fake_execution;
	PgWaitCompletion *completion;
	PgWaitSpec	wait_spec;
	TestWaitCompletionContext context;
	bool		saved_publication;
	bool		ok = true;
	int			result;

	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_execution = CurrentPgExecution;
	saved_publication = PgSetWaitCompletionPublication(true);

	MemSet(&fake_backend, 0, sizeof(fake_backend));
	MemSet(&fake_session, 0, sizeof(fake_session));
	MemSet(&fake_execution, 0, sizeof(fake_execution));
	pg_atomic_init_u32(&fake_backend.interrupts.pending_mask, 0);
	pg_atomic_write_u32(&fake_backend.interrupts.pending_mask,
						PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL));
	pg_atomic_init_u32(&fake_backend.wait_state.waiting, 0);
	pg_atomic_init_u32(&fake_backend.wait_state.completion.state,
					   PG_WAIT_COMPLETION_INACTIVE);
	pg_atomic_init_u32(&fake_backend.wait_state.completion.ready_events, 0);
	pg_atomic_init_u32(&fake_backend.wait_state.completion.interrupt_events, 0);
	fake_backend.wait_state.wait_event_info_ptr =
		&fake_backend.wait_state.local_wait_event_info;

	wait_spec.kind = PG_WAIT_KIND_EVENT_SET;
	wait_spec.wait_event_info = 0x0A0B0C0D;
	wait_spec.wake_events = WL_LATCH_SET | WL_TIMEOUT;
	wait_spec.socket = 123;
	wait_spec.timeout = 42;

	MemSet(&context, 0, sizeof(context));
	context.backend = &fake_backend;
	context.session = &fake_session;
	context.execution = &fake_execution;

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend);
		PgSetCurrentSession(&fake_session);
		PgSetCurrentExecution(&fake_execution);

		result = PgSuspend(&wait_spec, test_wait_completion_callback,
						   &context);
		completion = PgBackendCurrentWaitCompletion(&fake_backend);

		ok = ok && result == 42;
		ok = ok && context.saw_published_wait;
		ok = ok && context.saw_cancel_interrupt;
		ok = ok && context.saw_termination_interrupt;
		ok = ok && context.saw_ready_wait;
		ok = ok && completion != NULL;
		ok = ok && pg_atomic_read_u32(&fake_backend.wait_state.waiting) == 0;
		ok = ok && fake_backend.wait_state.spec.kind == PG_WAIT_KIND_NONE;
		ok = ok && fake_backend.wait_state.spec.wait_event_info == 0;
		ok = ok && fake_backend.wait_state.spec.wake_events == 0;
		ok = ok && fake_backend.wait_state.spec.socket == 0;
		ok = ok && fake_backend.wait_state.spec.timeout == 0;
		ok = ok && completion->backend == NULL;
		ok = ok && completion->session == NULL;
		ok = ok && completion->execution == NULL;
		ok = ok && completion->spec.kind == PG_WAIT_KIND_NONE;
		ok = ok && pg_atomic_read_u32(&completion->state) ==
			PG_WAIT_COMPLETION_INACTIVE;
		ok = ok && pg_atomic_read_u32(&completion->ready_events) == 0;
		ok = ok && pg_atomic_read_u32(&completion->interrupt_events) == 0;
		ok = ok && !PgBackendWakeWaitCompletion(&fake_backend, WL_LATCH_SET);

		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentExecution(saved_execution);
		PgSetWaitCompletionPublication(saved_publication);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentExecution(saved_execution);
		PgSetWaitCompletionPublication(saved_publication);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend wait completion publication failed");

	PG_RETURN_BOOL(true);
#else
	PG_RETURN_BOOL(true);
#endif
}

PG_FUNCTION_INFO_V1(test_backend_wait_completion_publication_policy);
Datum
test_backend_wait_completion_publication_policy(PG_FUNCTION_ARGS)
{
#ifdef PG_RUNTIME_ENABLE_WAIT_COMPLETION_PUBLICATION
	PgBackend  *saved_backend;
	PgRuntime	process_runtime;
	PgRuntime	thread_runtime;
	PgBackend	process_backend;
	PgBackend	thread_backend;
	PgWaitSpec	wait_spec;
	TestWaitCompletionPolicyContext context;
	bool		saved_publication;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	saved_publication = PgSetWaitCompletionPublication(false);

	MemSet(&process_runtime, 0, sizeof(process_runtime));
	process_runtime.kind = PG_RUNTIME_PROCESS;
	MemSet(&thread_runtime, 0, sizeof(thread_runtime));
	thread_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;

	MemSet(&process_backend, 0, sizeof(process_backend));
	process_backend.runtime = &process_runtime;
	process_backend.wait_state.wait_event_info_ptr =
		&process_backend.wait_state.local_wait_event_info;
	pg_atomic_init_u32(&process_backend.wait_state.waiting, 0);
	pg_atomic_init_u32(&process_backend.wait_state.completion.state,
					   PG_WAIT_COMPLETION_INACTIVE);
	pg_atomic_init_u32(&process_backend.wait_state.completion.ready_events, 0);
	pg_atomic_init_u32(&process_backend.wait_state.completion.interrupt_events, 0);

	MemSet(&thread_backend, 0, sizeof(thread_backend));
	thread_backend.runtime = &thread_runtime;
	thread_backend.wait_state.wait_event_info_ptr =
		&thread_backend.wait_state.local_wait_event_info;
	pg_atomic_init_u32(&thread_backend.wait_state.waiting, 0);
	pg_atomic_init_u32(&thread_backend.wait_state.completion.state,
					   PG_WAIT_COMPLETION_INACTIVE);
	pg_atomic_init_u32(&thread_backend.wait_state.completion.ready_events, 0);
	pg_atomic_init_u32(&thread_backend.wait_state.completion.interrupt_events, 0);

	wait_spec.kind = PG_WAIT_KIND_EVENT_SET;
	wait_spec.wait_event_info = 0x01020304;
	wait_spec.wake_events = WL_LATCH_SET;
	wait_spec.socket = PGINVALID_SOCKET;
	wait_spec.timeout = -1;

	PG_TRY();
	{
		MemSet(&context, 0, sizeof(context));
		context.backend = &process_backend;
		PgSetCurrentBackend(&process_backend);
		ok = ok && PgSuspend(&wait_spec,
							 test_wait_completion_policy_callback,
							 &context) == 7;
		ok = ok && context.saw_unpublished_wait;
		ok = ok && !context.saw_published_wait;

		MemSet(&context, 0, sizeof(context));
		context.backend = &thread_backend;
		PgSetCurrentBackend(&thread_backend);
		ok = ok && PgSuspend(&wait_spec,
							 test_wait_completion_policy_callback,
							 &context) == 7;
		ok = ok && context.saw_published_wait;
		ok = ok && !context.saw_unpublished_wait;
		ok = ok && pg_atomic_read_u32(&thread_backend.wait_state.waiting) == 0;
		ok = ok && pg_atomic_read_u32(&thread_backend.wait_state.completion.state) ==
			PG_WAIT_COMPLETION_INACTIVE;

		PgSetCurrentBackend(saved_backend);
		PgSetWaitCompletionPublication(saved_publication);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PgSetWaitCompletionPublication(saved_publication);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend wait completion publication policy failed");

	PG_RETURN_BOOL(true);
#else
	PG_RETURN_BOOL(true);
#endif
}

PG_FUNCTION_INFO_V1(test_backend_transaction_state_is_backend_local);
Datum
test_backend_transaction_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	FullTransactionId fxid1;
	FullTransactionId fxid2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	fxid1 = FullTransactionIdFromEpochAndXid(1, 101);
	fxid2 = FullTransactionIdFromEpochAndXid(2, 201);

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		*PgCurrentCachedFetchXidRef() = 101;
		*PgCurrentCachedFetchXidStatusRef() = 102;
		*PgCurrentCachedCommitLSNRef() = UINT64CONST(103);
		*PgCurrentTwoPhaseLockedGxactRef() = &fake_backend1;
		*PgCurrentTwoPhaseExitRegisteredRef() = true;
		*PgCurrentTwoPhaseCachedFxidRef() = fxid1;
		*PgCurrentTwoPhaseCachedGxactRef() = &fake_backend1;
		*PgCurrentSlruErrorCauseRef() = 104;
		*PgCurrentSlruErrnoRef() = 105;
		dclist_init(PgCurrentMultiXactCacheRef());
		*PgCurrentMultiXactCacheInitializedRef() = true;
		*PgCurrentMultiXactContextRef() = (MemoryContext) &fake_backend1;
		*PgCurrentMultiXactDebugStringRef() = (char *) "mxact-1";
		*PgCurrentProcArrayCachedXidNotInProgressRef() = 106;
		PgCurrentGlobalVisSharedRelsRef()->definitely_needed =
			FullTransactionIdFromEpochAndXid(3, 107);
		PgCurrentGlobalVisSharedRelsRef()->maybe_needed =
			FullTransactionIdFromEpochAndXid(3, 108);
		PgCurrentGlobalVisCatalogRelsRef()->definitely_needed =
			FullTransactionIdFromEpochAndXid(3, 109);
		PgCurrentGlobalVisCatalogRelsRef()->maybe_needed =
			FullTransactionIdFromEpochAndXid(3, 110);
		PgCurrentGlobalVisDataRelsRef()->definitely_needed =
			FullTransactionIdFromEpochAndXid(3, 111);
		PgCurrentGlobalVisDataRelsRef()->maybe_needed =
			FullTransactionIdFromEpochAndXid(3, 112);
		PgCurrentGlobalVisTempRelsRef()->definitely_needed =
			FullTransactionIdFromEpochAndXid(3, 113);
		PgCurrentGlobalVisTempRelsRef()->maybe_needed =
			FullTransactionIdFromEpochAndXid(3, 114);
		*PgCurrentComputeXidHorizonsResultLastXminRef() = 115;
		*PgCurrentXidCacheByRecentXminRef() = 116;
		*PgCurrentXidCacheByKnownXactRef() = 117;
		*PgCurrentXidCacheByMyXactRef() = 118;
		*PgCurrentXidCacheByLatestXidRef() = 119;
		*PgCurrentXidCacheByMainXidRef() = 120;
		*PgCurrentXidCacheByChildXidRef() = 121;
		*PgCurrentXidCacheByKnownAssignedRef() = 122;
		*PgCurrentXidCacheNoOverflowRef() = 123;
		*PgCurrentXidCacheSlowAnswerRef() = 124;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentCachedFetchXidRef() == InvalidTransactionId;
		ok = ok && *PgCurrentCachedFetchXidStatusRef() == 0;
		ok = ok && *PgCurrentCachedCommitLSNRef() == 0;
		ok = ok && *PgCurrentTwoPhaseLockedGxactRef() == NULL;
		ok = ok && !*PgCurrentTwoPhaseExitRegisteredRef();
		ok = ok && FullTransactionIdEquals(*PgCurrentTwoPhaseCachedFxidRef(),
											InvalidFullTransactionId);
		ok = ok && *PgCurrentTwoPhaseCachedGxactRef() == NULL;
		ok = ok && *PgCurrentSlruErrorCauseRef() == 0;
		ok = ok && *PgCurrentSlruErrnoRef() == 0;
		ok = ok && !*PgCurrentMultiXactCacheInitializedRef();
		ok = ok && *PgCurrentMultiXactContextRef() == NULL;
		ok = ok && *PgCurrentMultiXactDebugStringRef() == NULL;
		ok = ok && *PgCurrentProcArrayCachedXidNotInProgressRef() == InvalidTransactionId;
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisSharedRelsRef()->definitely_needed,
											InvalidFullTransactionId);
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisSharedRelsRef()->maybe_needed,
											InvalidFullTransactionId);
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisCatalogRelsRef()->definitely_needed,
											InvalidFullTransactionId);
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisCatalogRelsRef()->maybe_needed,
											InvalidFullTransactionId);
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisDataRelsRef()->definitely_needed,
											InvalidFullTransactionId);
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisDataRelsRef()->maybe_needed,
											InvalidFullTransactionId);
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisTempRelsRef()->definitely_needed,
											InvalidFullTransactionId);
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisTempRelsRef()->maybe_needed,
											InvalidFullTransactionId);
		ok = ok && *PgCurrentComputeXidHorizonsResultLastXminRef() == InvalidTransactionId;
		ok = ok && *PgCurrentXidCacheByRecentXminRef() == 0;
		ok = ok && *PgCurrentXidCacheByKnownXactRef() == 0;
		ok = ok && *PgCurrentXidCacheByMyXactRef() == 0;
		ok = ok && *PgCurrentXidCacheByLatestXidRef() == 0;
		ok = ok && *PgCurrentXidCacheByMainXidRef() == 0;
		ok = ok && *PgCurrentXidCacheByChildXidRef() == 0;
		ok = ok && *PgCurrentXidCacheByKnownAssignedRef() == 0;
		ok = ok && *PgCurrentXidCacheNoOverflowRef() == 0;
		ok = ok && *PgCurrentXidCacheSlowAnswerRef() == 0;

		*PgCurrentCachedFetchXidRef() = 201;
		*PgCurrentCachedFetchXidStatusRef() = 202;
		*PgCurrentCachedCommitLSNRef() = UINT64CONST(203);
		*PgCurrentTwoPhaseLockedGxactRef() = &fake_backend2;
		*PgCurrentTwoPhaseExitRegisteredRef() = false;
		*PgCurrentTwoPhaseCachedFxidRef() = fxid2;
		*PgCurrentTwoPhaseCachedGxactRef() = &fake_backend2;
		*PgCurrentSlruErrorCauseRef() = 204;
		*PgCurrentSlruErrnoRef() = 205;
		dclist_init(PgCurrentMultiXactCacheRef());
		*PgCurrentMultiXactCacheInitializedRef() = true;
		*PgCurrentMultiXactContextRef() = (MemoryContext) &fake_backend2;
		*PgCurrentMultiXactDebugStringRef() = (char *) "mxact-2";
		*PgCurrentProcArrayCachedXidNotInProgressRef() = 206;
		PgCurrentGlobalVisSharedRelsRef()->definitely_needed =
			FullTransactionIdFromEpochAndXid(4, 207);
		PgCurrentGlobalVisSharedRelsRef()->maybe_needed =
			FullTransactionIdFromEpochAndXid(4, 208);
		PgCurrentGlobalVisCatalogRelsRef()->definitely_needed =
			FullTransactionIdFromEpochAndXid(4, 209);
		PgCurrentGlobalVisCatalogRelsRef()->maybe_needed =
			FullTransactionIdFromEpochAndXid(4, 210);
		PgCurrentGlobalVisDataRelsRef()->definitely_needed =
			FullTransactionIdFromEpochAndXid(4, 211);
		PgCurrentGlobalVisDataRelsRef()->maybe_needed =
			FullTransactionIdFromEpochAndXid(4, 212);
		PgCurrentGlobalVisTempRelsRef()->definitely_needed =
			FullTransactionIdFromEpochAndXid(4, 213);
		PgCurrentGlobalVisTempRelsRef()->maybe_needed =
			FullTransactionIdFromEpochAndXid(4, 214);
		*PgCurrentComputeXidHorizonsResultLastXminRef() = 215;
		*PgCurrentXidCacheByRecentXminRef() = 216;
		*PgCurrentXidCacheByKnownXactRef() = 217;
		*PgCurrentXidCacheByMyXactRef() = 218;
		*PgCurrentXidCacheByLatestXidRef() = 219;
		*PgCurrentXidCacheByMainXidRef() = 220;
		*PgCurrentXidCacheByChildXidRef() = 221;
		*PgCurrentXidCacheByKnownAssignedRef() = 222;
		*PgCurrentXidCacheNoOverflowRef() = 223;
		*PgCurrentXidCacheSlowAnswerRef() = 224;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && *PgCurrentCachedFetchXidRef() == 101;
		ok = ok && *PgCurrentCachedFetchXidStatusRef() == 102;
		ok = ok && *PgCurrentCachedCommitLSNRef() == UINT64CONST(103);
		ok = ok && *PgCurrentTwoPhaseLockedGxactRef() == &fake_backend1;
		ok = ok && *PgCurrentTwoPhaseExitRegisteredRef();
		ok = ok && FullTransactionIdEquals(*PgCurrentTwoPhaseCachedFxidRef(),
											fxid1);
		ok = ok && *PgCurrentTwoPhaseCachedGxactRef() == &fake_backend1;
		ok = ok && *PgCurrentSlruErrorCauseRef() == 104;
		ok = ok && *PgCurrentSlruErrnoRef() == 105;
		ok = ok && *PgCurrentMultiXactCacheInitializedRef();
		ok = ok && dclist_is_empty(PgCurrentMultiXactCacheRef());
		ok = ok && *PgCurrentMultiXactContextRef() == (MemoryContext) &fake_backend1;
		ok = ok && strcmp(*PgCurrentMultiXactDebugStringRef(), "mxact-1") == 0;
		ok = ok && *PgCurrentProcArrayCachedXidNotInProgressRef() == 106;
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisSharedRelsRef()->definitely_needed,
											FullTransactionIdFromEpochAndXid(3, 107));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisSharedRelsRef()->maybe_needed,
											FullTransactionIdFromEpochAndXid(3, 108));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisCatalogRelsRef()->definitely_needed,
											FullTransactionIdFromEpochAndXid(3, 109));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisCatalogRelsRef()->maybe_needed,
											FullTransactionIdFromEpochAndXid(3, 110));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisDataRelsRef()->definitely_needed,
											FullTransactionIdFromEpochAndXid(3, 111));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisDataRelsRef()->maybe_needed,
											FullTransactionIdFromEpochAndXid(3, 112));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisTempRelsRef()->definitely_needed,
											FullTransactionIdFromEpochAndXid(3, 113));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisTempRelsRef()->maybe_needed,
											FullTransactionIdFromEpochAndXid(3, 114));
		ok = ok && *PgCurrentComputeXidHorizonsResultLastXminRef() == 115;
		ok = ok && *PgCurrentXidCacheByRecentXminRef() == 116;
		ok = ok && *PgCurrentXidCacheByKnownXactRef() == 117;
		ok = ok && *PgCurrentXidCacheByMyXactRef() == 118;
		ok = ok && *PgCurrentXidCacheByLatestXidRef() == 119;
		ok = ok && *PgCurrentXidCacheByMainXidRef() == 120;
		ok = ok && *PgCurrentXidCacheByChildXidRef() == 121;
		ok = ok && *PgCurrentXidCacheByKnownAssignedRef() == 122;
		ok = ok && *PgCurrentXidCacheNoOverflowRef() == 123;
		ok = ok && *PgCurrentXidCacheSlowAnswerRef() == 124;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentCachedFetchXidRef() == 201;
		ok = ok && *PgCurrentCachedFetchXidStatusRef() == 202;
		ok = ok && *PgCurrentCachedCommitLSNRef() == UINT64CONST(203);
		ok = ok && *PgCurrentTwoPhaseLockedGxactRef() == &fake_backend2;
		ok = ok && !*PgCurrentTwoPhaseExitRegisteredRef();
		ok = ok && FullTransactionIdEquals(*PgCurrentTwoPhaseCachedFxidRef(),
											fxid2);
		ok = ok && *PgCurrentTwoPhaseCachedGxactRef() == &fake_backend2;
		ok = ok && *PgCurrentSlruErrorCauseRef() == 204;
		ok = ok && *PgCurrentSlruErrnoRef() == 205;
		ok = ok && *PgCurrentMultiXactCacheInitializedRef();
		ok = ok && dclist_is_empty(PgCurrentMultiXactCacheRef());
		ok = ok && *PgCurrentMultiXactContextRef() == (MemoryContext) &fake_backend2;
		ok = ok && strcmp(*PgCurrentMultiXactDebugStringRef(), "mxact-2") == 0;
		ok = ok && *PgCurrentProcArrayCachedXidNotInProgressRef() == 206;
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisSharedRelsRef()->definitely_needed,
											FullTransactionIdFromEpochAndXid(4, 207));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisSharedRelsRef()->maybe_needed,
											FullTransactionIdFromEpochAndXid(4, 208));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisCatalogRelsRef()->definitely_needed,
											FullTransactionIdFromEpochAndXid(4, 209));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisCatalogRelsRef()->maybe_needed,
											FullTransactionIdFromEpochAndXid(4, 210));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisDataRelsRef()->definitely_needed,
											FullTransactionIdFromEpochAndXid(4, 211));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisDataRelsRef()->maybe_needed,
											FullTransactionIdFromEpochAndXid(4, 212));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisTempRelsRef()->definitely_needed,
											FullTransactionIdFromEpochAndXid(4, 213));
		ok = ok && FullTransactionIdEquals(PgCurrentGlobalVisTempRelsRef()->maybe_needed,
											FullTransactionIdFromEpochAndXid(4, 214));
		ok = ok && *PgCurrentComputeXidHorizonsResultLastXminRef() == 215;
		ok = ok && *PgCurrentXidCacheByRecentXminRef() == 216;
		ok = ok && *PgCurrentXidCacheByKnownXactRef() == 217;
		ok = ok && *PgCurrentXidCacheByMyXactRef() == 218;
		ok = ok && *PgCurrentXidCacheByLatestXidRef() == 219;
		ok = ok && *PgCurrentXidCacheByMainXidRef() == 220;
		ok = ok && *PgCurrentXidCacheByChildXidRef() == 221;
		ok = ok && *PgCurrentXidCacheByKnownAssignedRef() == 222;
		ok = ok && *PgCurrentXidCacheNoOverflowRef() == 223;
		ok = ok && *PgCurrentXidCacheSlowAnswerRef() == 224;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend transaction state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_timeout_state_is_backend_local);
Datum
test_backend_timeout_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackendTimeoutState *timeout1;
	PgBackendTimeoutState *timeout2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		timeout1 = PgCurrentTimeoutState();
		timeout1->all_timeouts_initialized = true;
		timeout1->all_timeouts[DEADLOCK_TIMEOUT].index = DEADLOCK_TIMEOUT;
		timeout1->all_timeouts[DEADLOCK_TIMEOUT].active = true;
		timeout1->all_timeouts[DEADLOCK_TIMEOUT].indicator = true;
		timeout1->all_timeouts[DEADLOCK_TIMEOUT].target_backend = &fake_backend1;
		timeout1->all_timeouts[DEADLOCK_TIMEOUT].target_execution =
			(PgExecution *) &fake_backend1;
		timeout1->all_timeouts[DEADLOCK_TIMEOUT].start_time = 101;
		timeout1->all_timeouts[DEADLOCK_TIMEOUT].fin_time = 102;
		timeout1->all_timeouts[DEADLOCK_TIMEOUT].interval_in_ms = 103;
		timeout1->num_active_timeouts = 1;
		timeout1->active_timeouts[0] =
			&timeout1->all_timeouts[DEADLOCK_TIMEOUT];
		timeout1->alarm_enabled = true;
		timeout1->signal_pending = true;
		timeout1->signal_due_at = 104;
		timeout1->firing_timeout_target = &fake_backend1;
		timeout1->firing_timeout_execution = (PgExecution *) &fake_backend1;
		timeout1->signal_delivery = true;

		PgSetCurrentBackend(&fake_backend2);
		timeout2 = PgCurrentTimeoutState();
		ok = ok && !timeout2->all_timeouts_initialized;
		ok = ok && timeout2->num_active_timeouts == 0;
		ok = ok && timeout2->active_timeouts[0] == NULL;
		ok = ok && !timeout2->alarm_enabled;
		ok = ok && !timeout2->signal_pending;
		ok = ok && timeout2->signal_due_at == 0;
		ok = ok && timeout2->firing_timeout_target == NULL;
		ok = ok && timeout2->firing_timeout_execution == NULL;
		ok = ok && !timeout2->signal_delivery;

		timeout2->all_timeouts_initialized = true;
		timeout2->all_timeouts[LOCK_TIMEOUT].index = LOCK_TIMEOUT;
		timeout2->all_timeouts[LOCK_TIMEOUT].active = true;
		timeout2->all_timeouts[LOCK_TIMEOUT].indicator = false;
		timeout2->all_timeouts[LOCK_TIMEOUT].target_backend = &fake_backend2;
		timeout2->all_timeouts[LOCK_TIMEOUT].target_execution =
			(PgExecution *) &fake_backend2;
		timeout2->all_timeouts[LOCK_TIMEOUT].start_time = 201;
		timeout2->all_timeouts[LOCK_TIMEOUT].fin_time = 202;
		timeout2->all_timeouts[LOCK_TIMEOUT].interval_in_ms = 203;
		timeout2->num_active_timeouts = 1;
		timeout2->active_timeouts[0] = &timeout2->all_timeouts[LOCK_TIMEOUT];
		timeout2->alarm_enabled = false;
		timeout2->signal_pending = true;
		timeout2->signal_due_at = 204;
		timeout2->firing_timeout_target = &fake_backend2;
		timeout2->firing_timeout_execution = (PgExecution *) &fake_backend2;
		timeout2->signal_delivery = false;

		PgSetCurrentBackend(&fake_backend1);
		timeout1 = PgCurrentTimeoutState();
		ok = ok && timeout1->all_timeouts_initialized;
		ok = ok && timeout1->num_active_timeouts == 1;
		ok = ok && timeout1->active_timeouts[0] ==
			&timeout1->all_timeouts[DEADLOCK_TIMEOUT];
		ok = ok && timeout1->all_timeouts[DEADLOCK_TIMEOUT].active;
		ok = ok && timeout1->all_timeouts[DEADLOCK_TIMEOUT].indicator;
		ok = ok && timeout1->all_timeouts[DEADLOCK_TIMEOUT].target_backend ==
			&fake_backend1;
		ok = ok && timeout1->all_timeouts[DEADLOCK_TIMEOUT].target_execution ==
			(PgExecution *) &fake_backend1;
		ok = ok && timeout1->all_timeouts[DEADLOCK_TIMEOUT].start_time == 101;
		ok = ok && timeout1->all_timeouts[DEADLOCK_TIMEOUT].fin_time == 102;
		ok = ok && timeout1->all_timeouts[DEADLOCK_TIMEOUT].interval_in_ms == 103;
		ok = ok && timeout1->alarm_enabled;
		ok = ok && timeout1->signal_pending;
		ok = ok && timeout1->signal_due_at == 104;
		ok = ok && timeout1->firing_timeout_target == &fake_backend1;
		ok = ok && timeout1->firing_timeout_execution ==
			(PgExecution *) &fake_backend1;
		ok = ok && timeout1->signal_delivery;

		PgSetCurrentBackend(&fake_backend2);
		timeout2 = PgCurrentTimeoutState();
		ok = ok && timeout2->all_timeouts_initialized;
		ok = ok && timeout2->num_active_timeouts == 1;
		ok = ok && timeout2->active_timeouts[0] ==
			&timeout2->all_timeouts[LOCK_TIMEOUT];
		ok = ok && timeout2->all_timeouts[LOCK_TIMEOUT].active;
		ok = ok && !timeout2->all_timeouts[LOCK_TIMEOUT].indicator;
		ok = ok && timeout2->all_timeouts[LOCK_TIMEOUT].target_backend ==
			&fake_backend2;
		ok = ok && timeout2->all_timeouts[LOCK_TIMEOUT].target_execution ==
			(PgExecution *) &fake_backend2;
		ok = ok && timeout2->all_timeouts[LOCK_TIMEOUT].start_time == 201;
		ok = ok && timeout2->all_timeouts[LOCK_TIMEOUT].fin_time == 202;
		ok = ok && timeout2->all_timeouts[LOCK_TIMEOUT].interval_in_ms == 203;
		ok = ok && !timeout2->alarm_enabled;
		ok = ok && timeout2->signal_pending;
		ok = ok && timeout2->signal_due_at == 204;
		ok = ok && timeout2->firing_timeout_target == &fake_backend2;
		ok = ok && timeout2->firing_timeout_execution ==
			(PgExecution *) &fake_backend2;
		ok = ok && !timeout2->signal_delivery;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend timeout state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_walsender_state_is_backend_local);
Datum
test_backend_walsender_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackendWalSenderState *walsender1;
	PgBackendWalSenderState *walsender2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		walsender1 = PgCurrentWalSenderState();
		walsender1->my_wal_snd = (WalSnd *) &fake_backend1;
		walsender1->is_walsender = true;
		walsender1->is_cascading_walsender = true;
		walsender1->is_db_walsender = true;
		walsender1->wake_requested = true;
		walsender1->xlogreader = (XLogReaderState *) &fake_backend1;
		walsender1->uploaded_manifest = (IncrementalBackupInfo *) &fake_backend1;
		walsender1->uploaded_manifest_mcxt = (MemoryContext) &fake_backend1;
		walsender1->send_time_line = 101;
		walsender1->send_time_line_next_tli = 102;
		walsender1->send_time_line_is_historic = true;
		walsender1->send_time_line_valid_upto = UINT64CONST(103);
		walsender1->sent_ptr = UINT64CONST(104);
		walsender1->output_message.maxlen = 105;
		walsender1->reply_message.maxlen = 106;
		walsender1->tmpbuf.maxlen = 107;
		walsender1->last_processing = 108;
		walsender1->last_reply_timestamp = 109;
		walsender1->waiting_for_ping_response = true;
		walsender1->shutdown_request_timestamp = 110;
		walsender1->shutdown_stream_done_queued = true;
		walsender1->streaming_done_sending = true;
		walsender1->streaming_done_receiving = true;
		walsender1->caught_up = true;
		walsender1->got_sigusr2 = true;
		walsender1->got_stopping = true;
		walsender1->replication_active = true;
		walsender1->logical_decoding_ctx =
			(LogicalDecodingContext *) &fake_backend1;
		walsender1->replication_cmd_context = (MemoryContext) &fake_backend1;
		walsender1->lag_tracker = (LagTracker *) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		walsender2 = PgCurrentWalSenderState();
		ok = ok && walsender2->my_wal_snd == NULL;
		ok = ok && !walsender2->is_walsender;
		ok = ok && !walsender2->is_cascading_walsender;
		ok = ok && !walsender2->is_db_walsender;
		ok = ok && !walsender2->wake_requested;
		ok = ok && walsender2->xlogreader == NULL;
		ok = ok && walsender2->uploaded_manifest == NULL;
		ok = ok && walsender2->uploaded_manifest_mcxt == NULL;
		ok = ok && walsender2->send_time_line == 0;
		ok = ok && walsender2->send_time_line_next_tli == 0;
		ok = ok && !walsender2->send_time_line_is_historic;
		ok = ok && walsender2->send_time_line_valid_upto == InvalidXLogRecPtr;
		ok = ok && walsender2->sent_ptr == InvalidXLogRecPtr;
		ok = ok && walsender2->output_message.maxlen == 0;
		ok = ok && walsender2->reply_message.maxlen == 0;
		ok = ok && walsender2->tmpbuf.maxlen == 0;
		ok = ok && walsender2->last_processing == 0;
		ok = ok && walsender2->last_reply_timestamp == 0;
		ok = ok && !walsender2->waiting_for_ping_response;
		ok = ok && walsender2->shutdown_request_timestamp == 0;
		ok = ok && !walsender2->shutdown_stream_done_queued;
		ok = ok && !walsender2->streaming_done_sending;
		ok = ok && !walsender2->streaming_done_receiving;
		ok = ok && !walsender2->caught_up;
		ok = ok && !walsender2->got_sigusr2;
		ok = ok && !walsender2->got_stopping;
		ok = ok && !walsender2->replication_active;
		ok = ok && walsender2->logical_decoding_ctx == NULL;
		ok = ok && walsender2->replication_cmd_context == NULL;
		ok = ok && walsender2->lag_tracker == NULL;

		walsender2->my_wal_snd = (WalSnd *) &fake_backend2;
		walsender2->wake_requested = true;
		walsender2->xlogreader = (XLogReaderState *) &fake_backend2;
		walsender2->uploaded_manifest = (IncrementalBackupInfo *) &fake_backend2;
		walsender2->uploaded_manifest_mcxt = (MemoryContext) &fake_backend2;
		walsender2->send_time_line = 201;
		walsender2->send_time_line_next_tli = 202;
		walsender2->send_time_line_valid_upto = UINT64CONST(203);
		walsender2->sent_ptr = UINT64CONST(204);
		walsender2->output_message.maxlen = 205;
		walsender2->reply_message.maxlen = 206;
		walsender2->tmpbuf.maxlen = 207;
		walsender2->last_processing = 208;
		walsender2->last_reply_timestamp = 209;
		walsender2->shutdown_request_timestamp = 210;
		walsender2->logical_decoding_ctx =
			(LogicalDecodingContext *) &fake_backend2;
		walsender2->replication_cmd_context = (MemoryContext) &fake_backend2;
		walsender2->lag_tracker = (LagTracker *) &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		walsender1 = PgCurrentWalSenderState();
		ok = ok && walsender1->my_wal_snd == (WalSnd *) &fake_backend1;
		ok = ok && walsender1->is_walsender;
		ok = ok && walsender1->is_cascading_walsender;
		ok = ok && walsender1->is_db_walsender;
		ok = ok && walsender1->wake_requested;
		ok = ok && walsender1->xlogreader == (XLogReaderState *) &fake_backend1;
		ok = ok && walsender1->uploaded_manifest ==
			(IncrementalBackupInfo *) &fake_backend1;
		ok = ok && walsender1->uploaded_manifest_mcxt ==
			(MemoryContext) &fake_backend1;
		ok = ok && walsender1->send_time_line == 101;
		ok = ok && walsender1->send_time_line_next_tli == 102;
		ok = ok && walsender1->send_time_line_is_historic;
		ok = ok && walsender1->send_time_line_valid_upto == UINT64CONST(103);
		ok = ok && walsender1->sent_ptr == UINT64CONST(104);
		ok = ok && walsender1->output_message.maxlen == 105;
		ok = ok && walsender1->reply_message.maxlen == 106;
		ok = ok && walsender1->tmpbuf.maxlen == 107;
		ok = ok && walsender1->last_processing == 108;
		ok = ok && walsender1->last_reply_timestamp == 109;
		ok = ok && walsender1->waiting_for_ping_response;
		ok = ok && walsender1->shutdown_request_timestamp == 110;
		ok = ok && walsender1->shutdown_stream_done_queued;
		ok = ok && walsender1->streaming_done_sending;
		ok = ok && walsender1->streaming_done_receiving;
		ok = ok && walsender1->caught_up;
		ok = ok && walsender1->got_sigusr2;
		ok = ok && walsender1->got_stopping;
		ok = ok && walsender1->replication_active;
		ok = ok && walsender1->logical_decoding_ctx ==
			(LogicalDecodingContext *) &fake_backend1;
		ok = ok && walsender1->replication_cmd_context ==
			(MemoryContext) &fake_backend1;
		ok = ok && walsender1->lag_tracker == (LagTracker *) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		walsender2 = PgCurrentWalSenderState();
		ok = ok && walsender2->my_wal_snd == (WalSnd *) &fake_backend2;
		ok = ok && !walsender2->is_walsender;
		ok = ok && !walsender2->is_cascading_walsender;
		ok = ok && !walsender2->is_db_walsender;
		ok = ok && walsender2->wake_requested;
		ok = ok && walsender2->xlogreader == (XLogReaderState *) &fake_backend2;
		ok = ok && walsender2->uploaded_manifest ==
			(IncrementalBackupInfo *) &fake_backend2;
		ok = ok && walsender2->uploaded_manifest_mcxt ==
			(MemoryContext) &fake_backend2;
		ok = ok && walsender2->send_time_line == 201;
		ok = ok && walsender2->send_time_line_next_tli == 202;
		ok = ok && !walsender2->send_time_line_is_historic;
		ok = ok && walsender2->send_time_line_valid_upto == UINT64CONST(203);
		ok = ok && walsender2->sent_ptr == UINT64CONST(204);
		ok = ok && walsender2->output_message.maxlen == 205;
		ok = ok && walsender2->reply_message.maxlen == 206;
		ok = ok && walsender2->tmpbuf.maxlen == 207;
		ok = ok && walsender2->last_processing == 208;
		ok = ok && walsender2->last_reply_timestamp == 209;
		ok = ok && !walsender2->waiting_for_ping_response;
		ok = ok && walsender2->shutdown_request_timestamp == 210;
		ok = ok && !walsender2->shutdown_stream_done_queued;
		ok = ok && !walsender2->streaming_done_sending;
		ok = ok && !walsender2->streaming_done_receiving;
		ok = ok && !walsender2->caught_up;
		ok = ok && !walsender2->got_sigusr2;
		ok = ok && !walsender2->got_stopping;
		ok = ok && !walsender2->replication_active;
		ok = ok && walsender2->logical_decoding_ctx ==
			(LogicalDecodingContext *) &fake_backend2;
		ok = ok && walsender2->replication_cmd_context ==
			(MemoryContext) &fake_backend2;
		ok = ok && walsender2->lag_tracker == (LagTracker *) &fake_backend2;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend WAL sender state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_replication_state_is_backend_local);
Datum
test_backend_replication_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackendReplicationState *replication1;
	PgBackendReplicationState *replication2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	fake_backend1.replication.sync_rep_wait_mode = SYNC_REP_NO_WAIT;
	fake_backend1.replication.walreceiver_recv_file = -1;
	fake_backend1.replication.walreceiver_primary_has_standby_xmin = true;
	fake_backend2.replication.sync_rep_wait_mode = SYNC_REP_NO_WAIT;
	fake_backend2.replication.walreceiver_recv_file = -1;
	fake_backend2.replication.walreceiver_primary_has_standby_xmin = true;

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		replication1 = PgCurrentReplicationState();
		replication1->my_replication_slot =
			(ReplicationSlot *) &fake_backend1;
		replication1->sync_rep_wait_mode = SYNC_REP_WAIT_FLUSH;
		replication1->walreceiver_conn = (WalReceiverConn *) &fake_backend1;
		replication1->walreceiver_recv_file = 101;
		replication1->walreceiver_recv_file_tli = 102;
		replication1->walreceiver_recv_seg_no = 103;
		replication1->walreceiver_logstream_result.Write = UINT64CONST(104);
		replication1->walreceiver_logstream_result.Flush = UINT64CONST(105);
		replication1->walreceiver_wakeup[0] = 106;
		replication1->walreceiver_reply_message.maxlen = 107;
		replication1->walreceiver_primary_has_standby_xmin = false;

		PgSetCurrentBackend(&fake_backend2);
		replication2 = PgCurrentReplicationState();
		ok = ok && replication2->my_replication_slot == NULL;
		ok = ok && replication2->sync_rep_wait_mode == SYNC_REP_NO_WAIT;
		ok = ok && replication2->walreceiver_conn == NULL;
		ok = ok && replication2->walreceiver_recv_file == -1;
		ok = ok && replication2->walreceiver_recv_file_tli == 0;
		ok = ok && replication2->walreceiver_recv_seg_no == 0;
		ok = ok && replication2->walreceiver_logstream_result.Write == 0;
		ok = ok && replication2->walreceiver_logstream_result.Flush == 0;
		ok = ok && replication2->walreceiver_wakeup[0] == 0;
		ok = ok && replication2->walreceiver_reply_message.maxlen == 0;
		ok = ok && replication2->walreceiver_primary_has_standby_xmin;

		replication2->my_replication_slot =
			(ReplicationSlot *) &fake_backend2;
		replication2->sync_rep_wait_mode = SYNC_REP_WAIT_APPLY;
		replication2->walreceiver_conn = (WalReceiverConn *) &fake_backend2;
		replication2->walreceiver_recv_file = 201;
		replication2->walreceiver_recv_file_tli = 202;
		replication2->walreceiver_recv_seg_no = 203;
		replication2->walreceiver_logstream_result.Write = UINT64CONST(204);
		replication2->walreceiver_logstream_result.Flush = UINT64CONST(205);
		replication2->walreceiver_wakeup[0] = 206;
		replication2->walreceiver_reply_message.maxlen = 207;
		replication2->walreceiver_primary_has_standby_xmin = true;

		PgSetCurrentBackend(&fake_backend1);
		replication1 = PgCurrentReplicationState();
		ok = ok && replication1->my_replication_slot ==
			(ReplicationSlot *) &fake_backend1;
		ok = ok && replication1->sync_rep_wait_mode == SYNC_REP_WAIT_FLUSH;
		ok = ok && replication1->walreceiver_conn ==
			(WalReceiverConn *) &fake_backend1;
		ok = ok && replication1->walreceiver_recv_file == 101;
		ok = ok && replication1->walreceiver_recv_file_tli == 102;
		ok = ok && replication1->walreceiver_recv_seg_no == 103;
		ok = ok && replication1->walreceiver_logstream_result.Write ==
			UINT64CONST(104);
		ok = ok && replication1->walreceiver_logstream_result.Flush ==
			UINT64CONST(105);
		ok = ok && replication1->walreceiver_wakeup[0] == 106;
		ok = ok && replication1->walreceiver_reply_message.maxlen == 107;
		ok = ok && !replication1->walreceiver_primary_has_standby_xmin;

		PgSetCurrentBackend(&fake_backend2);
		replication2 = PgCurrentReplicationState();
		ok = ok && replication2->my_replication_slot ==
			(ReplicationSlot *) &fake_backend2;
		ok = ok && replication2->sync_rep_wait_mode == SYNC_REP_WAIT_APPLY;
		ok = ok && replication2->walreceiver_conn ==
			(WalReceiverConn *) &fake_backend2;
		ok = ok && replication2->walreceiver_recv_file == 201;
		ok = ok && replication2->walreceiver_recv_file_tli == 202;
		ok = ok && replication2->walreceiver_recv_seg_no == 203;
		ok = ok && replication2->walreceiver_logstream_result.Write ==
			UINT64CONST(204);
		ok = ok && replication2->walreceiver_logstream_result.Flush ==
			UINT64CONST(205);
		ok = ok && replication2->walreceiver_wakeup[0] == 206;
		ok = ok && replication2->walreceiver_reply_message.maxlen == 207;
		ok = ok && replication2->walreceiver_primary_has_standby_xmin;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend replication state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_logical_replication_state_is_backend_local);
Datum
test_backend_logical_replication_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackendLogicalReplicationState *logical1;
	PgBackendLogicalReplicationState *logical2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	dlist_init(&fake_backend1.logical_replication.lsn_mapping);
	dlist_init(&fake_backend2.logical_replication.lsn_mapping);
	fake_backend1.logical_replication.apply_error_callback_arg.remote_attnum = -1;
	fake_backend1.logical_replication.apply_error_callback_arg.remote_xid =
		InvalidTransactionId;
	fake_backend1.logical_replication.apply_error_callback_arg.finish_lsn =
		InvalidXLogRecPtr;
	fake_backend1.logical_replication.subxact_data.subxact_last =
		InvalidTransactionId;
	fake_backend1.logical_replication.remote_final_lsn = InvalidXLogRecPtr;
	fake_backend1.logical_replication.stream_xid = InvalidTransactionId;
	fake_backend1.logical_replication.skip_xact_finish_lsn = InvalidXLogRecPtr;
	fake_backend1.logical_replication.last_flushpos = InvalidXLogRecPtr;
	fake_backend1.logical_replication.slotsync_sleep_ms =
		PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS;
	fake_backend2.logical_replication.apply_error_callback_arg.remote_attnum = -1;
	fake_backend2.logical_replication.apply_error_callback_arg.remote_xid =
		InvalidTransactionId;
	fake_backend2.logical_replication.apply_error_callback_arg.finish_lsn =
		InvalidXLogRecPtr;
	fake_backend2.logical_replication.subxact_data.subxact_last =
		InvalidTransactionId;
	fake_backend2.logical_replication.remote_final_lsn = InvalidXLogRecPtr;
	fake_backend2.logical_replication.stream_xid = InvalidTransactionId;
	fake_backend2.logical_replication.skip_xact_finish_lsn = InvalidXLogRecPtr;
	fake_backend2.logical_replication.last_flushpos = InvalidXLogRecPtr;
	fake_backend2.logical_replication.slotsync_sleep_ms =
		PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS;

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		logical1 = PgCurrentLogicalReplicationState();
		logical1->apply_error_callback_arg.command = LOGICAL_REP_MSG_INSERT;
		logical1->apply_error_callback_arg.rel =
			(struct LogicalRepRelMapEntry *) &fake_backend1;
		logical1->apply_error_callback_arg.remote_attnum = 11;
		logical1->apply_error_callback_arg.remote_xid = 12;
		logical1->apply_error_callback_arg.finish_lsn = UINT64CONST(13);
		logical1->apply_error_callback_arg.origin_name = (char *) &fake_backend1;
		logical1->subxact_data.nsubxacts = 14;
		logical1->subxact_data.nsubxacts_max = 15;
		logical1->subxact_data.subxact_last = 16;
		logical1->subxact_data.subxacts = (SubXactInfo *) &fake_backend1;
		logical1->apply_context = (MemoryContext) &fake_backend1;
		logical1->my_parallel_shared =
			(ParallelApplyWorkerShared *) &fake_backend1;
		logical1->parallel_apply_message_pending = true;
		logical1->logrep_worker_walrcv_conn =
			(WalReceiverConn *) &fake_backend1;
		logical1->my_subscription = (Subscription *) &fake_backend1;
		logical1->my_subscription_valid = true;
		logical1->my_logical_rep_worker =
			(LogicalRepWorker *) &fake_backend1;
		logical1->on_commit_wakeup_workers_subids = (List *) &fake_backend1;
		logical1->in_remote_transaction = true;
		logical1->remote_final_lsn = UINT64CONST(101);
		logical1->in_streamed_transaction = true;
		logical1->stream_xid = 102;
		logical1->parallel_stream_nchanges = 103;
		logical1->initializing_apply_worker = true;
		logical1->skip_xact_finish_lsn = UINT64CONST(104);
		logical1->stream_fd = (BufFile *) &fake_backend1;
		logical1->last_flushpos = UINT64CONST(105);
		logical1->table_states_not_ready = (List *) &fake_backend1;
		logical1->copybuf = (StringInfo) &fake_backend1;
		logical1->seqinfos = (List *) &fake_backend1;
		logical1->xlog_logical_info = true;
		logical1->xlog_logical_info_update_pending = true;
		logical1->slotsync_syncing_slots = true;
		logical1->slotsync_observed_primary_conninfo = (char *) &fake_backend1;
		logical1->slotsync_observed_primary_slotname = (char *) &fake_backend1;
		logical1->slotsync_observed_sync_replication_slots = true;
		logical1->slotsync_observed_hot_standby_feedback = true;
		logical1->slotsync_shutdown_pending = true;
		logical1->slotsync_sleep_ms = 106;
		logical1->launcher_last_start_times_dsa = (dsa_area *) &fake_backend1;
		logical1->launcher_last_start_times = (dshash_table *) &fake_backend1;
		logical1->launcher_on_commit_wakeup = true;
		logical1->parallel_apply_txn_hash = (HTAB *) &fake_backend1;
		logical1->parallel_apply_worker_pool = (List *) &fake_backend1;
		logical1->stream_apply_worker =
			(ParallelApplyWorkerInfo *) &fake_backend1;
		logical1->parallel_apply_subxactlist = (List *) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		logical2 = PgCurrentLogicalReplicationState();
		ok = ok && dlist_is_empty(&logical2->lsn_mapping);
		ok = ok && logical2->apply_error_callback_arg.command == 0;
		ok = ok && logical2->apply_error_callback_arg.rel == NULL;
		ok = ok && logical2->apply_error_callback_arg.remote_attnum == -1;
		ok = ok && logical2->apply_error_callback_arg.remote_xid ==
			InvalidTransactionId;
		ok = ok && logical2->apply_error_callback_arg.finish_lsn ==
			InvalidXLogRecPtr;
		ok = ok && logical2->apply_error_callback_arg.origin_name == NULL;
		ok = ok && logical2->subxact_data.nsubxacts == 0;
		ok = ok && logical2->subxact_data.nsubxacts_max == 0;
		ok = ok && logical2->subxact_data.subxact_last == InvalidTransactionId;
		ok = ok && logical2->subxact_data.subxacts == NULL;
		ok = ok && logical2->apply_context == NULL;
		ok = ok && logical2->my_parallel_shared == NULL;
		ok = ok && !logical2->parallel_apply_message_pending;
		ok = ok && logical2->logrep_worker_walrcv_conn == NULL;
		ok = ok && logical2->my_subscription == NULL;
		ok = ok && !logical2->my_subscription_valid;
		ok = ok && logical2->my_logical_rep_worker == NULL;
		ok = ok && logical2->on_commit_wakeup_workers_subids == NIL;
		ok = ok && !logical2->in_remote_transaction;
		ok = ok && logical2->remote_final_lsn == InvalidXLogRecPtr;
		ok = ok && !logical2->in_streamed_transaction;
		ok = ok && logical2->stream_xid == InvalidTransactionId;
		ok = ok && logical2->parallel_stream_nchanges == 0;
		ok = ok && !logical2->initializing_apply_worker;
		ok = ok && logical2->skip_xact_finish_lsn == InvalidXLogRecPtr;
		ok = ok && logical2->stream_fd == NULL;
		ok = ok && logical2->last_flushpos == InvalidXLogRecPtr;
		ok = ok && logical2->table_states_not_ready == NIL;
		ok = ok && logical2->copybuf == NULL;
		ok = ok && logical2->seqinfos == NIL;
		ok = ok && !logical2->xlog_logical_info;
		ok = ok && !logical2->xlog_logical_info_update_pending;
		ok = ok && !logical2->slotsync_syncing_slots;
		ok = ok && logical2->slotsync_observed_primary_conninfo == NULL;
		ok = ok && logical2->slotsync_observed_primary_slotname == NULL;
		ok = ok && !logical2->slotsync_observed_sync_replication_slots;
		ok = ok && !logical2->slotsync_observed_hot_standby_feedback;
		ok = ok && !logical2->slotsync_shutdown_pending;
		ok = ok && logical2->slotsync_sleep_ms ==
			PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS;
		ok = ok && logical2->launcher_last_start_times_dsa == NULL;
		ok = ok && logical2->launcher_last_start_times == NULL;
		ok = ok && !logical2->launcher_on_commit_wakeup;
		ok = ok && logical2->parallel_apply_txn_hash == NULL;
		ok = ok && logical2->parallel_apply_worker_pool == NIL;
		ok = ok && logical2->stream_apply_worker == NULL;
		ok = ok && logical2->parallel_apply_subxactlist == NIL;

		logical2->apply_context = (MemoryContext) &fake_backend2;
		logical2->apply_error_callback_arg.command = LOGICAL_REP_MSG_UPDATE;
		logical2->apply_error_callback_arg.rel =
			(struct LogicalRepRelMapEntry *) &fake_backend2;
		logical2->apply_error_callback_arg.remote_attnum = 21;
		logical2->apply_error_callback_arg.remote_xid = 22;
		logical2->apply_error_callback_arg.finish_lsn = UINT64CONST(23);
		logical2->apply_error_callback_arg.origin_name = (char *) &fake_backend2;
		logical2->subxact_data.nsubxacts = 24;
		logical2->subxact_data.nsubxacts_max = 25;
		logical2->subxact_data.subxact_last = 26;
		logical2->subxact_data.subxacts = (SubXactInfo *) &fake_backend2;
		logical2->my_parallel_shared =
			(ParallelApplyWorkerShared *) &fake_backend2;
		logical2->parallel_apply_message_pending = true;
		logical2->logrep_worker_walrcv_conn =
			(WalReceiverConn *) &fake_backend2;
		logical2->my_subscription = (Subscription *) &fake_backend2;
		logical2->my_subscription_valid = true;
		logical2->my_logical_rep_worker =
			(LogicalRepWorker *) &fake_backend2;
		logical2->on_commit_wakeup_workers_subids = (List *) &fake_backend2;
		logical2->in_remote_transaction = true;
		logical2->remote_final_lsn = UINT64CONST(201);
		logical2->in_streamed_transaction = true;
		logical2->stream_xid = 202;
		logical2->parallel_stream_nchanges = 203;
		logical2->initializing_apply_worker = true;
		logical2->skip_xact_finish_lsn = UINT64CONST(204);
		logical2->stream_fd = (BufFile *) &fake_backend2;
		logical2->last_flushpos = UINT64CONST(205);
		logical2->table_states_not_ready = (List *) &fake_backend2;
		logical2->copybuf = (StringInfo) &fake_backend2;
		logical2->seqinfos = (List *) &fake_backend2;
		logical2->xlog_logical_info = true;
		logical2->xlog_logical_info_update_pending = true;
		logical2->slotsync_syncing_slots = true;
		logical2->slotsync_observed_primary_conninfo = (char *) &fake_backend2;
		logical2->slotsync_observed_primary_slotname = (char *) &fake_backend2;
		logical2->slotsync_observed_sync_replication_slots = true;
		logical2->slotsync_observed_hot_standby_feedback = true;
		logical2->slotsync_shutdown_pending = true;
		logical2->slotsync_sleep_ms = 206;
		logical2->launcher_last_start_times_dsa = (dsa_area *) &fake_backend2;
		logical2->launcher_last_start_times = (dshash_table *) &fake_backend2;
		logical2->launcher_on_commit_wakeup = true;
		logical2->parallel_apply_txn_hash = (HTAB *) &fake_backend2;
		logical2->parallel_apply_worker_pool = (List *) &fake_backend2;
		logical2->stream_apply_worker =
			(ParallelApplyWorkerInfo *) &fake_backend2;
		logical2->parallel_apply_subxactlist = (List *) &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		logical1 = PgCurrentLogicalReplicationState();
		ok = ok && dlist_is_empty(&logical1->lsn_mapping);
		ok = ok && logical1->apply_error_callback_arg.command ==
			LOGICAL_REP_MSG_INSERT;
		ok = ok && logical1->apply_error_callback_arg.rel ==
			(struct LogicalRepRelMapEntry *) &fake_backend1;
		ok = ok && logical1->apply_error_callback_arg.remote_attnum == 11;
		ok = ok && logical1->apply_error_callback_arg.remote_xid == 12;
		ok = ok && logical1->apply_error_callback_arg.finish_lsn == UINT64CONST(13);
		ok = ok && logical1->apply_error_callback_arg.origin_name ==
			(char *) &fake_backend1;
		ok = ok && logical1->subxact_data.nsubxacts == 14;
		ok = ok && logical1->subxact_data.nsubxacts_max == 15;
		ok = ok && logical1->subxact_data.subxact_last == 16;
		ok = ok && logical1->subxact_data.subxacts ==
			(SubXactInfo *) &fake_backend1;
		ok = ok && logical1->apply_context == (MemoryContext) &fake_backend1;
		ok = ok && logical1->my_parallel_shared ==
			(ParallelApplyWorkerShared *) &fake_backend1;
		ok = ok && logical1->parallel_apply_message_pending;
		ok = ok && logical1->logrep_worker_walrcv_conn ==
			(WalReceiverConn *) &fake_backend1;
		ok = ok && logical1->my_subscription == (Subscription *) &fake_backend1;
		ok = ok && logical1->my_subscription_valid;
		ok = ok && logical1->my_logical_rep_worker ==
			(LogicalRepWorker *) &fake_backend1;
		ok = ok && logical1->on_commit_wakeup_workers_subids ==
			(List *) &fake_backend1;
		ok = ok && logical1->in_remote_transaction;
		ok = ok && logical1->remote_final_lsn == UINT64CONST(101);
		ok = ok && logical1->in_streamed_transaction;
		ok = ok && logical1->stream_xid == 102;
		ok = ok && logical1->parallel_stream_nchanges == 103;
		ok = ok && logical1->initializing_apply_worker;
		ok = ok && logical1->skip_xact_finish_lsn == UINT64CONST(104);
		ok = ok && logical1->stream_fd == (BufFile *) &fake_backend1;
		ok = ok && logical1->last_flushpos == UINT64CONST(105);
		ok = ok && logical1->table_states_not_ready == (List *) &fake_backend1;
		ok = ok && logical1->copybuf == (StringInfo) &fake_backend1;
		ok = ok && logical1->seqinfos == (List *) &fake_backend1;
		ok = ok && logical1->xlog_logical_info;
		ok = ok && logical1->xlog_logical_info_update_pending;
		ok = ok && logical1->slotsync_syncing_slots;
		ok = ok && logical1->slotsync_observed_primary_conninfo ==
			(char *) &fake_backend1;
		ok = ok && logical1->slotsync_observed_primary_slotname ==
			(char *) &fake_backend1;
		ok = ok && logical1->slotsync_observed_sync_replication_slots;
		ok = ok && logical1->slotsync_observed_hot_standby_feedback;
		ok = ok && logical1->slotsync_shutdown_pending;
		ok = ok && logical1->slotsync_sleep_ms == 106;
		ok = ok && logical1->launcher_last_start_times_dsa ==
			(dsa_area *) &fake_backend1;
		ok = ok && logical1->launcher_last_start_times ==
			(dshash_table *) &fake_backend1;
		ok = ok && logical1->launcher_on_commit_wakeup;
		ok = ok && logical1->parallel_apply_txn_hash == (HTAB *) &fake_backend1;
		ok = ok && logical1->parallel_apply_worker_pool == (List *) &fake_backend1;
		ok = ok && logical1->stream_apply_worker ==
			(ParallelApplyWorkerInfo *) &fake_backend1;
		ok = ok && logical1->parallel_apply_subxactlist == (List *) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		logical2 = PgCurrentLogicalReplicationState();
		ok = ok && dlist_is_empty(&logical2->lsn_mapping);
		ok = ok && logical2->apply_error_callback_arg.command ==
			LOGICAL_REP_MSG_UPDATE;
		ok = ok && logical2->apply_error_callback_arg.rel ==
			(struct LogicalRepRelMapEntry *) &fake_backend2;
		ok = ok && logical2->apply_error_callback_arg.remote_attnum == 21;
		ok = ok && logical2->apply_error_callback_arg.remote_xid == 22;
		ok = ok && logical2->apply_error_callback_arg.finish_lsn == UINT64CONST(23);
		ok = ok && logical2->apply_error_callback_arg.origin_name ==
			(char *) &fake_backend2;
		ok = ok && logical2->subxact_data.nsubxacts == 24;
		ok = ok && logical2->subxact_data.nsubxacts_max == 25;
		ok = ok && logical2->subxact_data.subxact_last == 26;
		ok = ok && logical2->subxact_data.subxacts ==
			(SubXactInfo *) &fake_backend2;
		ok = ok && logical2->apply_context == (MemoryContext) &fake_backend2;
		ok = ok && logical2->my_parallel_shared ==
			(ParallelApplyWorkerShared *) &fake_backend2;
		ok = ok && logical2->parallel_apply_message_pending;
		ok = ok && logical2->logrep_worker_walrcv_conn ==
			(WalReceiverConn *) &fake_backend2;
		ok = ok && logical2->my_subscription == (Subscription *) &fake_backend2;
		ok = ok && logical2->my_subscription_valid;
		ok = ok && logical2->my_logical_rep_worker ==
			(LogicalRepWorker *) &fake_backend2;
		ok = ok && logical2->on_commit_wakeup_workers_subids ==
			(List *) &fake_backend2;
		ok = ok && logical2->in_remote_transaction;
		ok = ok && logical2->remote_final_lsn == UINT64CONST(201);
		ok = ok && logical2->in_streamed_transaction;
		ok = ok && logical2->stream_xid == 202;
		ok = ok && logical2->parallel_stream_nchanges == 203;
		ok = ok && logical2->initializing_apply_worker;
		ok = ok && logical2->skip_xact_finish_lsn == UINT64CONST(204);
		ok = ok && logical2->stream_fd == (BufFile *) &fake_backend2;
		ok = ok && logical2->last_flushpos == UINT64CONST(205);
		ok = ok && logical2->table_states_not_ready == (List *) &fake_backend2;
		ok = ok && logical2->copybuf == (StringInfo) &fake_backend2;
		ok = ok && logical2->seqinfos == (List *) &fake_backend2;
		ok = ok && logical2->xlog_logical_info;
		ok = ok && logical2->xlog_logical_info_update_pending;
		ok = ok && logical2->slotsync_syncing_slots;
		ok = ok && logical2->slotsync_observed_primary_conninfo ==
			(char *) &fake_backend2;
		ok = ok && logical2->slotsync_observed_primary_slotname ==
			(char *) &fake_backend2;
		ok = ok && logical2->slotsync_observed_sync_replication_slots;
		ok = ok && logical2->slotsync_observed_hot_standby_feedback;
		ok = ok && logical2->slotsync_shutdown_pending;
		ok = ok && logical2->slotsync_sleep_ms == 206;
		ok = ok && logical2->launcher_last_start_times_dsa ==
			(dsa_area *) &fake_backend2;
		ok = ok && logical2->launcher_last_start_times ==
			(dshash_table *) &fake_backend2;
		ok = ok && logical2->launcher_on_commit_wakeup;
		ok = ok && logical2->parallel_apply_txn_hash == (HTAB *) &fake_backend2;
		ok = ok && logical2->parallel_apply_worker_pool == (List *) &fake_backend2;
		ok = ok && logical2->stream_apply_worker ==
			(ParallelApplyWorkerInfo *) &fake_backend2;
		ok = ok && logical2->parallel_apply_subxactlist == (List *) &fake_backend2;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend logical replication state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_xlog_state_is_backend_local);
Datum
test_backend_xlog_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackendXLogState *xlog1;
	PgBackendXLogState *xlog2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	fake_backend1.xlog.local_recovery_in_progress = true;
	fake_backend1.xlog.local_xlog_insert_allowed = -1;
	fake_backend1.xlog.proc_last_rec_ptr = InvalidXLogRecPtr;
	fake_backend1.xlog.xact_last_rec_end = InvalidXLogRecPtr;
	fake_backend1.xlog.xact_last_commit_end = InvalidXLogRecPtr;
	fake_backend1.xlog.redo_rec_ptr = InvalidXLogRecPtr;
	fake_backend1.xlog.open_log_file = -1;
	fake_backend1.xlog.local_min_recovery_point = InvalidXLogRecPtr;
	fake_backend1.xlog.update_min_recovery_point = true;
	fake_backend2.xlog.local_recovery_in_progress = true;
	fake_backend2.xlog.local_xlog_insert_allowed = -1;
	fake_backend2.xlog.proc_last_rec_ptr = InvalidXLogRecPtr;
	fake_backend2.xlog.xact_last_rec_end = InvalidXLogRecPtr;
	fake_backend2.xlog.xact_last_commit_end = InvalidXLogRecPtr;
	fake_backend2.xlog.redo_rec_ptr = InvalidXLogRecPtr;
	fake_backend2.xlog.open_log_file = -1;
	fake_backend2.xlog.local_min_recovery_point = InvalidXLogRecPtr;
	fake_backend2.xlog.update_min_recovery_point = true;

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		xlog1 = PgCurrentXLogState();
		xlog1->local_recovery_in_progress = false;
		xlog1->local_xlog_insert_allowed = 1;
		xlog1->proc_last_rec_ptr = UINT64CONST(101);
		xlog1->xact_last_rec_end = UINT64CONST(102);
		xlog1->xact_last_commit_end = UINT64CONST(103);
		xlog1->redo_rec_ptr = UINT64CONST(104);
		xlog1->do_page_writes = true;
		xlog1->logwrt_result.Write = UINT64CONST(105);
		xlog1->logwrt_result.Flush = UINT64CONST(106);
		xlog1->open_log_file = 107;
		xlog1->open_log_seg_no = 108;
		xlog1->open_log_tli = 109;
		xlog1->local_min_recovery_point = UINT64CONST(110);
		xlog1->local_min_recovery_point_tli = 111;
		xlog1->update_min_recovery_point = false;
		xlog1->local_data_checksum_state = PG_DATA_CHECKSUM_INPROGRESS_ON;
		xlog1->my_lock_no = 112;
		xlog1->holding_all_locks = true;
		xlog1->wal_debug_context = (MemoryContext) &fake_backend1;
		xlog1->btree_xlog_op_context = (MemoryContext) &fake_backend1;
		xlog1->gin_xlog_op_context = (MemoryContext) &fake_backend1;
		xlog1->gist_xlog_op_context = (MemoryContext) &fake_backend1;
		xlog1->spgist_xlog_op_context = (MemoryContext) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		xlog2 = PgCurrentXLogState();
		ok = ok && xlog2->local_recovery_in_progress;
		ok = ok && xlog2->local_xlog_insert_allowed == -1;
		ok = ok && xlog2->proc_last_rec_ptr == InvalidXLogRecPtr;
		ok = ok && xlog2->xact_last_rec_end == InvalidXLogRecPtr;
		ok = ok && xlog2->xact_last_commit_end == InvalidXLogRecPtr;
		ok = ok && xlog2->redo_rec_ptr == InvalidXLogRecPtr;
		ok = ok && !xlog2->do_page_writes;
		ok = ok && xlog2->logwrt_result.Write == 0;
		ok = ok && xlog2->logwrt_result.Flush == 0;
		ok = ok && xlog2->open_log_file == -1;
		ok = ok && xlog2->open_log_seg_no == 0;
		ok = ok && xlog2->open_log_tli == 0;
		ok = ok && xlog2->local_min_recovery_point == InvalidXLogRecPtr;
		ok = ok && xlog2->local_min_recovery_point_tli == 0;
		ok = ok && xlog2->update_min_recovery_point;
		ok = ok && xlog2->local_data_checksum_state == PG_DATA_CHECKSUM_OFF;
		ok = ok && xlog2->my_lock_no == 0;
		ok = ok && !xlog2->holding_all_locks;
		ok = ok && xlog2->wal_debug_context == NULL;
		ok = ok && xlog2->btree_xlog_op_context == NULL;
		ok = ok && xlog2->gin_xlog_op_context == NULL;
		ok = ok && xlog2->gist_xlog_op_context == NULL;
		ok = ok && xlog2->spgist_xlog_op_context == NULL;

		xlog2->local_recovery_in_progress = false;
		xlog2->local_xlog_insert_allowed = 0;
		xlog2->proc_last_rec_ptr = UINT64CONST(201);
		xlog2->xact_last_rec_end = UINT64CONST(202);
		xlog2->xact_last_commit_end = UINT64CONST(203);
		xlog2->redo_rec_ptr = UINT64CONST(204);
		xlog2->do_page_writes = true;
		xlog2->logwrt_result.Write = UINT64CONST(205);
		xlog2->logwrt_result.Flush = UINT64CONST(206);
		xlog2->open_log_file = 207;
		xlog2->open_log_seg_no = 208;
		xlog2->open_log_tli = 209;
		xlog2->local_min_recovery_point = UINT64CONST(210);
		xlog2->local_min_recovery_point_tli = 211;
		xlog2->update_min_recovery_point = false;
		xlog2->local_data_checksum_state = PG_DATA_CHECKSUM_INPROGRESS_OFF;
		xlog2->my_lock_no = 212;
		xlog2->holding_all_locks = true;
		xlog2->wal_debug_context = (MemoryContext) &fake_backend2;
		xlog2->btree_xlog_op_context = (MemoryContext) &fake_backend2;
		xlog2->gin_xlog_op_context = (MemoryContext) &fake_backend2;
		xlog2->gist_xlog_op_context = (MemoryContext) &fake_backend2;
		xlog2->spgist_xlog_op_context = (MemoryContext) &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		xlog1 = PgCurrentXLogState();
		ok = ok && !xlog1->local_recovery_in_progress;
		ok = ok && xlog1->local_xlog_insert_allowed == 1;
		ok = ok && xlog1->proc_last_rec_ptr == UINT64CONST(101);
		ok = ok && xlog1->xact_last_rec_end == UINT64CONST(102);
		ok = ok && xlog1->xact_last_commit_end == UINT64CONST(103);
		ok = ok && xlog1->redo_rec_ptr == UINT64CONST(104);
		ok = ok && xlog1->do_page_writes;
		ok = ok && xlog1->logwrt_result.Write == UINT64CONST(105);
		ok = ok && xlog1->logwrt_result.Flush == UINT64CONST(106);
		ok = ok && xlog1->open_log_file == 107;
		ok = ok && xlog1->open_log_seg_no == 108;
		ok = ok && xlog1->open_log_tli == 109;
		ok = ok && xlog1->local_min_recovery_point == UINT64CONST(110);
		ok = ok && xlog1->local_min_recovery_point_tli == 111;
		ok = ok && !xlog1->update_min_recovery_point;
		ok = ok && xlog1->local_data_checksum_state ==
			PG_DATA_CHECKSUM_INPROGRESS_ON;
		ok = ok && xlog1->my_lock_no == 112;
		ok = ok && xlog1->holding_all_locks;
		ok = ok && xlog1->wal_debug_context == (MemoryContext) &fake_backend1;
		ok = ok && xlog1->btree_xlog_op_context == (MemoryContext) &fake_backend1;
		ok = ok && xlog1->gin_xlog_op_context == (MemoryContext) &fake_backend1;
		ok = ok && xlog1->gist_xlog_op_context == (MemoryContext) &fake_backend1;
		ok = ok && xlog1->spgist_xlog_op_context == (MemoryContext) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		xlog2 = PgCurrentXLogState();
		ok = ok && !xlog2->local_recovery_in_progress;
		ok = ok && xlog2->local_xlog_insert_allowed == 0;
		ok = ok && xlog2->proc_last_rec_ptr == UINT64CONST(201);
		ok = ok && xlog2->xact_last_rec_end == UINT64CONST(202);
		ok = ok && xlog2->xact_last_commit_end == UINT64CONST(203);
		ok = ok && xlog2->redo_rec_ptr == UINT64CONST(204);
		ok = ok && xlog2->do_page_writes;
		ok = ok && xlog2->logwrt_result.Write == UINT64CONST(205);
		ok = ok && xlog2->logwrt_result.Flush == UINT64CONST(206);
		ok = ok && xlog2->open_log_file == 207;
		ok = ok && xlog2->open_log_seg_no == 208;
		ok = ok && xlog2->open_log_tli == 209;
		ok = ok && xlog2->local_min_recovery_point == UINT64CONST(210);
		ok = ok && xlog2->local_min_recovery_point_tli == 211;
		ok = ok && !xlog2->update_min_recovery_point;
		ok = ok && xlog2->local_data_checksum_state ==
			PG_DATA_CHECKSUM_INPROGRESS_OFF;
		ok = ok && xlog2->my_lock_no == 212;
		ok = ok && xlog2->holding_all_locks;
		ok = ok && xlog2->wal_debug_context == (MemoryContext) &fake_backend2;
		ok = ok && xlog2->btree_xlog_op_context == (MemoryContext) &fake_backend2;
		ok = ok && xlog2->gin_xlog_op_context == (MemoryContext) &fake_backend2;
		ok = ok && xlog2->gist_xlog_op_context == (MemoryContext) &fake_backend2;
		ok = ok && xlog2->spgist_xlog_op_context == (MemoryContext) &fake_backend2;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend XLog state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_recovery_state_is_backend_local);
Datum
test_backend_recovery_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackendRecoveryState *recovery1;
	PgBackendRecoveryState *recovery2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	fake_backend1.recovery.standby_wait_us = PG_BACKEND_STANDBY_INITIAL_WAIT_US;
	fake_backend2.recovery.standby_wait_us = PG_BACKEND_STANDBY_INITIAL_WAIT_US;

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		recovery1 = PgCurrentRecoveryState();
		recovery1->startup_got_sighup = true;
		recovery1->startup_shutdown_requested = true;
		recovery1->startup_promote_signaled = true;
		recovery1->startup_in_restore_command = true;
		recovery1->startup_progress_phase_start_time = 101;
		recovery1->startup_progress_timer_expired = true;
		recovery1->local_hot_standby_active = true;
		recovery1->local_promote_is_triggered = true;
		recovery1->recovery_lock_hash = (HTAB *) &fake_backend1;
		recovery1->recovery_lock_xid_hash = (HTAB *) &fake_backend1;
		recovery1->got_standby_deadlock_timeout = true;
		recovery1->got_standby_delay_timeout = true;
		recovery1->got_standby_lock_timeout = true;
		recovery1->standby_wait_us = 102;

		PgSetCurrentBackend(&fake_backend2);
		recovery2 = PgCurrentRecoveryState();
		ok = ok && !recovery2->startup_got_sighup;
		ok = ok && !recovery2->startup_shutdown_requested;
		ok = ok && !recovery2->startup_promote_signaled;
		ok = ok && !recovery2->startup_in_restore_command;
		ok = ok && recovery2->startup_progress_phase_start_time == 0;
		ok = ok && !recovery2->startup_progress_timer_expired;
		ok = ok && !recovery2->local_hot_standby_active;
		ok = ok && !recovery2->local_promote_is_triggered;
		ok = ok && recovery2->recovery_lock_hash == NULL;
		ok = ok && recovery2->recovery_lock_xid_hash == NULL;
		ok = ok && !recovery2->got_standby_deadlock_timeout;
		ok = ok && !recovery2->got_standby_delay_timeout;
		ok = ok && !recovery2->got_standby_lock_timeout;
		ok = ok && recovery2->standby_wait_us == PG_BACKEND_STANDBY_INITIAL_WAIT_US;

		recovery2->startup_got_sighup = true;
		recovery2->startup_shutdown_requested = true;
		recovery2->startup_promote_signaled = true;
		recovery2->startup_in_restore_command = true;
		recovery2->startup_progress_phase_start_time = 201;
		recovery2->startup_progress_timer_expired = true;
		recovery2->local_hot_standby_active = true;
		recovery2->local_promote_is_triggered = true;
		recovery2->recovery_lock_hash = (HTAB *) &fake_backend2;
		recovery2->recovery_lock_xid_hash = (HTAB *) &fake_backend2;
		recovery2->got_standby_deadlock_timeout = true;
		recovery2->got_standby_delay_timeout = true;
		recovery2->got_standby_lock_timeout = true;
		recovery2->standby_wait_us = 202;

		PgSetCurrentBackend(&fake_backend1);
		recovery1 = PgCurrentRecoveryState();
		ok = ok && recovery1->startup_got_sighup;
		ok = ok && recovery1->startup_shutdown_requested;
		ok = ok && recovery1->startup_promote_signaled;
		ok = ok && recovery1->startup_in_restore_command;
		ok = ok && recovery1->startup_progress_phase_start_time == 101;
		ok = ok && recovery1->startup_progress_timer_expired;
		ok = ok && recovery1->local_hot_standby_active;
		ok = ok && recovery1->local_promote_is_triggered;
		ok = ok && recovery1->recovery_lock_hash == (HTAB *) &fake_backend1;
		ok = ok && recovery1->recovery_lock_xid_hash == (HTAB *) &fake_backend1;
		ok = ok && recovery1->got_standby_deadlock_timeout;
		ok = ok && recovery1->got_standby_delay_timeout;
		ok = ok && recovery1->got_standby_lock_timeout;
		ok = ok && recovery1->standby_wait_us == 102;

		PgSetCurrentBackend(&fake_backend2);
		recovery2 = PgCurrentRecoveryState();
		ok = ok && recovery2->startup_got_sighup;
		ok = ok && recovery2->startup_shutdown_requested;
		ok = ok && recovery2->startup_promote_signaled;
		ok = ok && recovery2->startup_in_restore_command;
		ok = ok && recovery2->startup_progress_phase_start_time == 201;
		ok = ok && recovery2->startup_progress_timer_expired;
		ok = ok && recovery2->local_hot_standby_active;
		ok = ok && recovery2->local_promote_is_triggered;
		ok = ok && recovery2->recovery_lock_hash == (HTAB *) &fake_backend2;
		ok = ok && recovery2->recovery_lock_xid_hash == (HTAB *) &fake_backend2;
		ok = ok && recovery2->got_standby_deadlock_timeout;
		ok = ok && recovery2->got_standby_delay_timeout;
		ok = ok && recovery2->got_standby_lock_timeout;
		ok = ok && recovery2->standby_wait_us == 202;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend recovery state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_maintenance_worker_state_is_backend_local);
Datum
test_backend_maintenance_worker_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackendMaintenanceWorkerState *worker1;
	PgBackendMaintenanceWorkerState *worker2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	fake_backend1.maintenance_worker.bgwriter_last_snapshot_lsn =
		InvalidXLogRecPtr;
	fake_backend1.maintenance_worker.walsummarizer_sleep_quanta = 1;
	fake_backend1.maintenance_worker.walsummarizer_redo_pointer_at_last_summary_removal =
		InvalidXLogRecPtr;
	fake_backend2.maintenance_worker.bgwriter_last_snapshot_lsn =
		InvalidXLogRecPtr;
	fake_backend2.maintenance_worker.walsummarizer_sleep_quanta = 1;
	fake_backend2.maintenance_worker.walsummarizer_redo_pointer_at_last_summary_removal =
		InvalidXLogRecPtr;

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		worker1 = PgCurrentMaintenanceWorkerState();
		worker1->arch_module_errdetail_string = (char *) &fake_backend1;
		worker1->pgarch_last_sigterm_time = 101;
		worker1->archive_callbacks = (const struct ArchiveModuleCallbacks *) &fake_backend1;
		worker1->archive_module_state = (struct ArchiveModuleState *) &fake_backend1;
		worker1->archive_context = (MemoryContext) &fake_backend1;
		worker1->loaded_archive_library = (char *) &fake_backend1;
		worker1->pgarch_files = (struct arch_files_state *) &fake_backend1;
		worker1->bgwriter_context = (MemoryContext) &fake_backend1;
		worker1->walwriter_context = (MemoryContext) &fake_backend1;
		worker1->checkpointer_context = (MemoryContext) &fake_backend1;
		worker1->walsummarizer_context = (MemoryContext) &fake_backend1;
		worker1->pgarch_ready_to_stop = true;
		worker1->ckpt_active = true;
		worker1->ckpt_start_time = 102;
		worker1->ckpt_start_recptr = UINT64CONST(103);
		worker1->ckpt_cached_elapsed = 104.0;
		worker1->last_checkpoint_time = 105;
		worker1->last_xlog_switch_time = 106;
		worker1->bgwriter_last_snapshot_ts = 107;
		worker1->bgwriter_last_snapshot_lsn = UINT64CONST(108);
		worker1->walsummarizer_sleep_quanta = 109;
		worker1->walsummarizer_pages_read_since_last_sleep = 110;
		worker1->walsummarizer_redo_pointer_at_last_summary_removal =
			UINT64CONST(111);
		worker1->datachecksum_abort_requested = true;
		worker1->datachecksum_launcher_running = true;
		worker1->datachecksum_operation = 1;	/* DISABLE_DATACHECKSUMS */

		PgSetCurrentBackend(&fake_backend2);
		worker2 = PgCurrentMaintenanceWorkerState();
		ok = ok && worker2->arch_module_errdetail_string == NULL;
		ok = ok && worker2->pgarch_last_sigterm_time == 0;
		ok = ok && worker2->archive_callbacks == NULL;
		ok = ok && worker2->archive_module_state == NULL;
		ok = ok && worker2->archive_context == NULL;
		ok = ok && worker2->loaded_archive_library == NULL;
		ok = ok && worker2->pgarch_files == NULL;
		ok = ok && worker2->bgwriter_context == NULL;
		ok = ok && worker2->walwriter_context == NULL;
		ok = ok && worker2->checkpointer_context == NULL;
		ok = ok && worker2->walsummarizer_context == NULL;
		ok = ok && !worker2->pgarch_ready_to_stop;
		ok = ok && !worker2->ckpt_active;
		ok = ok && worker2->ckpt_start_time == 0;
		ok = ok && worker2->ckpt_start_recptr == 0;
		ok = ok && worker2->ckpt_cached_elapsed == 0;
		ok = ok && worker2->last_checkpoint_time == 0;
		ok = ok && worker2->last_xlog_switch_time == 0;
		ok = ok && worker2->bgwriter_last_snapshot_ts == 0;
		ok = ok && worker2->bgwriter_last_snapshot_lsn == InvalidXLogRecPtr;
		ok = ok && worker2->walsummarizer_sleep_quanta == 1;
		ok = ok && worker2->walsummarizer_pages_read_since_last_sleep == 0;
		ok = ok && worker2->walsummarizer_redo_pointer_at_last_summary_removal ==
			InvalidXLogRecPtr;
		ok = ok && !worker2->datachecksum_abort_requested;
		ok = ok && !worker2->datachecksum_launcher_running;
		ok = ok && worker2->datachecksum_operation == 0;	/* ENABLE_DATACHECKSUMS */

		worker2->arch_module_errdetail_string = (char *) &fake_backend2;
		worker2->pgarch_last_sigterm_time = 201;
		worker2->archive_callbacks = (const struct ArchiveModuleCallbacks *) &fake_backend2;
		worker2->archive_module_state = (struct ArchiveModuleState *) &fake_backend2;
		worker2->archive_context = (MemoryContext) &fake_backend2;
		worker2->loaded_archive_library = (char *) &fake_backend2;
		worker2->pgarch_files = (struct arch_files_state *) &fake_backend2;
		worker2->bgwriter_context = (MemoryContext) &fake_backend2;
		worker2->walwriter_context = (MemoryContext) &fake_backend2;
		worker2->checkpointer_context = (MemoryContext) &fake_backend2;
		worker2->walsummarizer_context = (MemoryContext) &fake_backend2;
		worker2->pgarch_ready_to_stop = true;
		worker2->ckpt_active = true;
		worker2->ckpt_start_time = 202;
		worker2->ckpt_start_recptr = UINT64CONST(203);
		worker2->ckpt_cached_elapsed = 204.0;
		worker2->last_checkpoint_time = 205;
		worker2->last_xlog_switch_time = 206;
		worker2->bgwriter_last_snapshot_ts = 207;
		worker2->bgwriter_last_snapshot_lsn = UINT64CONST(208);
		worker2->walsummarizer_sleep_quanta = 209;
		worker2->walsummarizer_pages_read_since_last_sleep = 210;
		worker2->walsummarizer_redo_pointer_at_last_summary_removal =
			UINT64CONST(211);
		worker2->datachecksum_abort_requested = true;
		worker2->datachecksum_launcher_running = true;
		worker2->datachecksum_operation = 1;	/* DISABLE_DATACHECKSUMS */

		PgSetCurrentBackend(&fake_backend1);
		worker1 = PgCurrentMaintenanceWorkerState();
		ok = ok && worker1->arch_module_errdetail_string ==
			(char *) &fake_backend1;
		ok = ok && worker1->pgarch_last_sigterm_time == 101;
		ok = ok && worker1->archive_callbacks ==
			(const struct ArchiveModuleCallbacks *) &fake_backend1;
		ok = ok && worker1->archive_module_state ==
			(struct ArchiveModuleState *) &fake_backend1;
		ok = ok && worker1->archive_context == (MemoryContext) &fake_backend1;
		ok = ok && worker1->loaded_archive_library == (char *) &fake_backend1;
		ok = ok && worker1->pgarch_files ==
			(struct arch_files_state *) &fake_backend1;
		ok = ok && worker1->bgwriter_context ==
			(MemoryContext) &fake_backend1;
		ok = ok && worker1->walwriter_context ==
			(MemoryContext) &fake_backend1;
		ok = ok && worker1->checkpointer_context ==
			(MemoryContext) &fake_backend1;
		ok = ok && worker1->walsummarizer_context ==
			(MemoryContext) &fake_backend1;
		ok = ok && worker1->pgarch_ready_to_stop;
		ok = ok && worker1->ckpt_active;
		ok = ok && worker1->ckpt_start_time == 102;
		ok = ok && worker1->ckpt_start_recptr == UINT64CONST(103);
		ok = ok && worker1->ckpt_cached_elapsed == 104.0;
		ok = ok && worker1->last_checkpoint_time == 105;
		ok = ok && worker1->last_xlog_switch_time == 106;
		ok = ok && worker1->bgwriter_last_snapshot_ts == 107;
		ok = ok && worker1->bgwriter_last_snapshot_lsn == UINT64CONST(108);
		ok = ok && worker1->walsummarizer_sleep_quanta == 109;
		ok = ok && worker1->walsummarizer_pages_read_since_last_sleep == 110;
		ok = ok && worker1->walsummarizer_redo_pointer_at_last_summary_removal ==
			UINT64CONST(111);
		ok = ok && worker1->datachecksum_abort_requested;
		ok = ok && worker1->datachecksum_launcher_running;
		ok = ok && worker1->datachecksum_operation == 1;	/* DISABLE_DATACHECKSUMS */

		PgSetCurrentBackend(&fake_backend2);
		worker2 = PgCurrentMaintenanceWorkerState();
		ok = ok && worker2->arch_module_errdetail_string ==
			(char *) &fake_backend2;
		ok = ok && worker2->pgarch_last_sigterm_time == 201;
		ok = ok && worker2->archive_callbacks ==
			(const struct ArchiveModuleCallbacks *) &fake_backend2;
		ok = ok && worker2->archive_module_state ==
			(struct ArchiveModuleState *) &fake_backend2;
		ok = ok && worker2->archive_context == (MemoryContext) &fake_backend2;
		ok = ok && worker2->loaded_archive_library == (char *) &fake_backend2;
		ok = ok && worker2->pgarch_files ==
			(struct arch_files_state *) &fake_backend2;
		ok = ok && worker2->bgwriter_context ==
			(MemoryContext) &fake_backend2;
		ok = ok && worker2->walwriter_context ==
			(MemoryContext) &fake_backend2;
		ok = ok && worker2->checkpointer_context ==
			(MemoryContext) &fake_backend2;
		ok = ok && worker2->walsummarizer_context ==
			(MemoryContext) &fake_backend2;
		ok = ok && worker2->pgarch_ready_to_stop;
		ok = ok && worker2->ckpt_active;
		ok = ok && worker2->ckpt_start_time == 202;
		ok = ok && worker2->ckpt_start_recptr == UINT64CONST(203);
		ok = ok && worker2->ckpt_cached_elapsed == 204.0;
		ok = ok && worker2->last_checkpoint_time == 205;
		ok = ok && worker2->last_xlog_switch_time == 206;
		ok = ok && worker2->bgwriter_last_snapshot_ts == 207;
		ok = ok && worker2->bgwriter_last_snapshot_lsn == UINT64CONST(208);
		ok = ok && worker2->walsummarizer_sleep_quanta == 209;
		ok = ok && worker2->walsummarizer_pages_read_since_last_sleep == 210;
		ok = ok && worker2->walsummarizer_redo_pointer_at_last_summary_removal ==
			UINT64CONST(211);
		ok = ok && worker2->datachecksum_abort_requested;
		ok = ok && worker2->datachecksum_launcher_running;
		ok = ok && worker2->datachecksum_operation == 1;	/* DISABLE_DATACHECKSUMS */

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend maintenance worker state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_autovacuum_state_is_backend_local);
Datum
test_backend_autovacuum_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackendAutovacuumState *av1;
	PgBackendAutovacuumState *av2;
	dlist_node	node1;
	dlist_node	node2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	fake_backend1.autovacuum.av_storage_param_cost_delay = -1;
	fake_backend1.autovacuum.av_storage_param_cost_limit = -1;
	dlist_init(&fake_backend1.autovacuum.database_list);
	fake_backend2.autovacuum.av_storage_param_cost_delay = -1;
	fake_backend2.autovacuum.av_storage_param_cost_limit = -1;
	dlist_init(&fake_backend2.autovacuum.database_list);
	dlist_node_init(&node1);
	dlist_node_init(&node2);

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		av1 = PgCurrentAutovacuumState();
		av1->av_storage_param_cost_delay = 1.5;
		av1->av_storage_param_cost_limit = 101;
		av1->got_sigusr2 = true;
		av1->recent_xid = 102;
		av1->recent_multi = 103;
		av1->default_freeze_min_age = 104;
		av1->default_freeze_table_age = 105;
		av1->default_multixact_freeze_min_age = 106;
		av1->default_multixact_freeze_table_age = 107;
		av1->autovac_mem_cxt = (MemoryContext) &fake_backend1;
		dlist_push_head(&av1->database_list, &node1);
		av1->database_list_cxt = (MemoryContext) &node1;
		av1->avl_dbase_array = (struct avl_dbase *) &fake_backend1;
		av1->my_worker_info = (struct WorkerInfoData *) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		av2 = PgCurrentAutovacuumState();
		ok = ok && av2->av_storage_param_cost_delay == -1;
		ok = ok && av2->av_storage_param_cost_limit == -1;
		ok = ok && !av2->got_sigusr2;
		ok = ok && av2->recent_xid == 0;
		ok = ok && av2->recent_multi == 0;
		ok = ok && av2->default_freeze_min_age == 0;
		ok = ok && av2->default_freeze_table_age == 0;
		ok = ok && av2->default_multixact_freeze_min_age == 0;
		ok = ok && av2->default_multixact_freeze_table_age == 0;
		ok = ok && av2->autovac_mem_cxt == NULL;
		ok = ok && dlist_is_empty(&av2->database_list);
		ok = ok && av2->database_list_cxt == NULL;
		ok = ok && av2->avl_dbase_array == NULL;
		ok = ok && av2->my_worker_info == NULL;

		av2->av_storage_param_cost_delay = 2.5;
		av2->av_storage_param_cost_limit = 201;
		av2->got_sigusr2 = true;
		av2->recent_xid = 202;
		av2->recent_multi = 203;
		av2->default_freeze_min_age = 204;
		av2->default_freeze_table_age = 205;
		av2->default_multixact_freeze_min_age = 206;
		av2->default_multixact_freeze_table_age = 207;
		av2->autovac_mem_cxt = (MemoryContext) &fake_backend2;
		dlist_push_head(&av2->database_list, &node2);
		av2->database_list_cxt = (MemoryContext) &node2;
		av2->avl_dbase_array = (struct avl_dbase *) &fake_backend2;
		av2->my_worker_info = (struct WorkerInfoData *) &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		av1 = PgCurrentAutovacuumState();
		ok = ok && av1->av_storage_param_cost_delay == 1.5;
		ok = ok && av1->av_storage_param_cost_limit == 101;
		ok = ok && av1->got_sigusr2;
		ok = ok && av1->recent_xid == 102;
		ok = ok && av1->recent_multi == 103;
		ok = ok && av1->default_freeze_min_age == 104;
		ok = ok && av1->default_freeze_table_age == 105;
		ok = ok && av1->default_multixact_freeze_min_age == 106;
		ok = ok && av1->default_multixact_freeze_table_age == 107;
		ok = ok && av1->autovac_mem_cxt == (MemoryContext) &fake_backend1;
		ok = ok && av1->database_list.head.next == &node1;
		ok = ok && av1->database_list_cxt == (MemoryContext) &node1;
		ok = ok && av1->avl_dbase_array ==
			(struct avl_dbase *) &fake_backend1;
		ok = ok && av1->my_worker_info ==
			(struct WorkerInfoData *) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		av2 = PgCurrentAutovacuumState();
		ok = ok && av2->av_storage_param_cost_delay == 2.5;
		ok = ok && av2->av_storage_param_cost_limit == 201;
		ok = ok && av2->got_sigusr2;
		ok = ok && av2->recent_xid == 202;
		ok = ok && av2->recent_multi == 203;
		ok = ok && av2->default_freeze_min_age == 204;
		ok = ok && av2->default_freeze_table_age == 205;
		ok = ok && av2->default_multixact_freeze_min_age == 206;
		ok = ok && av2->default_multixact_freeze_table_age == 207;
		ok = ok && av2->autovac_mem_cxt == (MemoryContext) &fake_backend2;
		ok = ok && av2->database_list.head.next == &node2;
		ok = ok && av2->database_list_cxt == (MemoryContext) &node2;
		ok = ok && av2->avl_dbase_array ==
			(struct avl_dbase *) &fake_backend2;
		ok = ok && av2->my_worker_info ==
			(struct WorkerInfoData *) &fake_backend2;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend autovacuum state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_repack_state_is_backend_local);
Datum
test_backend_repack_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackendRepackState *repack1;
	PgBackendRepackState *repack2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	fake_backend1.repack.repacked_rel_locator.relNumber = InvalidOid;
	fake_backend1.repack.repacked_rel_toast_locator.relNumber = InvalidOid;
	fake_backend2.repack.repacked_rel_locator.relNumber = InvalidOid;
	fake_backend2.repack.repacked_rel_toast_locator.relNumber = InvalidOid;

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		repack1 = PgCurrentRepackState();
		repack1->decoding_worker = (struct DecodingWorker *) &fake_backend1;
		RepackMessagePending = true;
		repack1->am_repack_worker = true;
		repack1->current_segment = 101;
		repack1->worker_dsm_segment = (dsm_segment *) &fake_backend1;
		repack1->repacked_rel_locator.spcOid = 102;
		repack1->repacked_rel_locator.dbOid = 103;
		repack1->repacked_rel_locator.relNumber = 104;
		repack1->repacked_rel_toast_locator.spcOid = 105;
		repack1->repacked_rel_toast_locator.dbOid = 106;
		repack1->repacked_rel_toast_locator.relNumber = 107;

		PgSetCurrentBackend(&fake_backend2);
		repack2 = PgCurrentRepackState();
		ok = ok && repack2->decoding_worker == NULL;
		ok = ok && !RepackMessagePending;
		ok = ok && !repack2->am_repack_worker;
		ok = ok && repack2->current_segment == 0;
		ok = ok && repack2->worker_dsm_segment == NULL;
		ok = ok && !OidIsValid(repack2->repacked_rel_locator.relNumber);
		ok = ok && !OidIsValid(repack2->repacked_rel_toast_locator.relNumber);

		repack2->decoding_worker = (struct DecodingWorker *) &fake_backend2;
		RepackMessagePending = true;
		repack2->am_repack_worker = true;
		repack2->current_segment = 201;
		repack2->worker_dsm_segment = (dsm_segment *) &fake_backend2;
		repack2->repacked_rel_locator.spcOid = 202;
		repack2->repacked_rel_locator.dbOid = 203;
		repack2->repacked_rel_locator.relNumber = 204;
		repack2->repacked_rel_toast_locator.spcOid = 205;
		repack2->repacked_rel_toast_locator.dbOid = 206;
		repack2->repacked_rel_toast_locator.relNumber = 207;

		PgSetCurrentBackend(&fake_backend1);
		repack1 = PgCurrentRepackState();
		ok = ok && repack1->decoding_worker ==
			(struct DecodingWorker *) &fake_backend1;
		ok = ok && RepackMessagePending;
		ok = ok && repack1->am_repack_worker;
		ok = ok && repack1->current_segment == 101;
		ok = ok && repack1->worker_dsm_segment ==
			(dsm_segment *) &fake_backend1;
		ok = ok && repack1->repacked_rel_locator.spcOid == 102;
		ok = ok && repack1->repacked_rel_locator.dbOid == 103;
		ok = ok && repack1->repacked_rel_locator.relNumber == 104;
		ok = ok && repack1->repacked_rel_toast_locator.spcOid == 105;
		ok = ok && repack1->repacked_rel_toast_locator.dbOid == 106;
		ok = ok && repack1->repacked_rel_toast_locator.relNumber == 107;

		PgSetCurrentBackend(&fake_backend2);
		repack2 = PgCurrentRepackState();
		ok = ok && repack2->decoding_worker ==
			(struct DecodingWorker *) &fake_backend2;
		ok = ok && RepackMessagePending;
		ok = ok && repack2->am_repack_worker;
		ok = ok && repack2->current_segment == 201;
		ok = ok && repack2->worker_dsm_segment ==
			(dsm_segment *) &fake_backend2;
		ok = ok && repack2->repacked_rel_locator.spcOid == 202;
		ok = ok && repack2->repacked_rel_locator.dbOid == 203;
		ok = ok && repack2->repacked_rel_locator.relNumber == 204;
		ok = ok && repack2->repacked_rel_toast_locator.spcOid == 205;
		ok = ok && repack2->repacked_rel_toast_locator.dbOid == 206;
		ok = ok && repack2->repacked_rel_toast_locator.relNumber == 207;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend repack state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_aio_state_is_backend_local);
Datum
test_backend_aio_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackendAioState *aio1;
	PgBackendAioState *aio2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	fake_backend1.aio.my_io_worker_id = -1;
	fake_backend2.aio.my_io_worker_id = -1;

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		aio1 = PgCurrentAioState();
		pgaio_my_backend = (PgAioBackend *) &fake_backend1;
		aio1->my_io_worker_id = 101;
		aio1->my_uring_context = (struct PgAioUringContext *) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		aio2 = PgCurrentAioState();
		ok = ok && pgaio_my_backend == NULL;
		ok = ok && aio2->my_io_worker_id == -1;
		ok = ok && aio2->my_uring_context == NULL;

		pgaio_my_backend = (PgAioBackend *) &fake_backend2;
		aio2->my_io_worker_id = 201;
		aio2->my_uring_context = (struct PgAioUringContext *) &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		aio1 = PgCurrentAioState();
		ok = ok && pgaio_my_backend == (PgAioBackend *) &fake_backend1;
		ok = ok && aio1->my_io_worker_id == 101;
		ok = ok && aio1->my_uring_context ==
			(struct PgAioUringContext *) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		aio2 = PgCurrentAioState();
		ok = ok && pgaio_my_backend == (PgAioBackend *) &fake_backend2;
		ok = ok && aio2->my_io_worker_id == 201;
		ok = ok && aio2->my_uring_context ==
			(struct PgAioUringContext *) &fake_backend2;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend AIO state was not backend-local");

	PG_RETURN_BOOL(true);
}

typedef struct TestBackendExtensionCleanupState
{
	bool	   *called;
} TestBackendExtensionCleanupState;

static void
test_backend_extension_private_state_cleanup(void *arg)
{
	TestBackendExtensionCleanupState *state =
		(TestBackendExtensionCleanupState *) arg;

	*state->called = true;
}

PG_FUNCTION_INFO_V1(test_backend_extension_module_state_is_backend_local);
Datum
test_backend_extension_module_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgBackend	fake_backend_reset;
	PgBackendExtensionModuleState *extension_modules;
	char		backend1_archive_directory[] = "backend1_archive";
	char		reset_archive_directory[] = "reset_archive";
	void	  **private_slot;
	TestBackendExtensionCleanupState *cleanup_state;
	const char *private_key = "test_backend_runtime.backend_private";
	const char *cleanup_key = "test_backend_runtime.backend_cleanup";
	bool		cleanup_called = false;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	MemSet(&fake_backend_reset, 0, sizeof(fake_backend_reset));

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		extension_modules = PgCurrentBackendExtensionModuleState();
		ok = ok && extension_modules->private_states == NIL;
		ok = ok && PgBackendGetExtensionPrivateState(private_key) == NULL;
		ok = ok && strcmp(*PgCurrentBasicArchiveDirectoryRef(), "") == 0;
		*PgCurrentBasicArchiveDirectoryRef() = backend1_archive_directory;
		private_slot = (void **)
			PgBackendEnsureExtensionPrivateState(private_key,
												 sizeof(void *),
												 NULL);
		*private_slot = &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		extension_modules = PgCurrentBackendExtensionModuleState();
		ok = ok && extension_modules->private_states == NIL;
		ok = ok && PgBackendGetExtensionPrivateState(private_key) == NULL;
		ok = ok && strcmp(*PgCurrentBasicArchiveDirectoryRef(), "") == 0;
		private_slot = (void **)
			PgBackendEnsureExtensionPrivateState(private_key,
												 sizeof(void *),
												 NULL);
		*private_slot = &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		private_slot = (void **) PgBackendGetExtensionPrivateState(private_key);
		ok = ok && private_slot != NULL && *private_slot == &fake_backend1;
		ok = ok && strcmp(*PgCurrentBasicArchiveDirectoryRef(),
						  "backend1_archive") == 0;

		PgSetCurrentBackend(&fake_backend2);
		private_slot = (void **) PgBackendGetExtensionPrivateState(private_key);
		ok = ok && private_slot != NULL && *private_slot == &fake_backend2;
		ok = ok && strcmp(*PgCurrentBasicArchiveDirectoryRef(), "") == 0;

		PgSetCurrentBackend(&fake_backend_reset);
		*PgCurrentBasicArchiveDirectoryRef() = reset_archive_directory;
		cleanup_state = (TestBackendExtensionCleanupState *)
			PgBackendEnsureExtensionPrivateState(cleanup_key,
												 sizeof(TestBackendExtensionCleanupState),
												 test_backend_extension_private_state_cleanup);
		cleanup_state->called = &cleanup_called;
		PgSetCurrentBackend(saved_backend);
		PgBackendResetClosedState(&fake_backend_reset);
		ok = ok && cleanup_called;
		ok = ok && fake_backend_reset.extension_modules.private_states == NIL;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend extension module state was not backend-local");

	PG_RETURN_BOOL(true);
}
