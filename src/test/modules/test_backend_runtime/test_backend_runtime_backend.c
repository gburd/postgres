/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_backend.c
 *		Backend-owned runtime state and PMChild tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_backend.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

PG_FUNCTION_INFO_V1(test_backend_pgstat_pending_state_is_backend_local);
Datum
test_backend_pgstat_pending_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgStat_BgWriterStats saved_bgwriter_stats;
	PgStat_CheckpointerStats saved_checkpointer_stats;
	PgStat_Counter saved_block_read_time;
	PgStat_Counter saved_block_write_time;
	PgStat_Counter saved_active_time;
	PgStat_Counter saved_transaction_idle_time;
	PgStat_PendingIO saved_io_stats;
	bool		saved_have_iostats;
	PgStat_SLRUStats saved_slru_stats;
	bool		saved_have_slrustats;
	PgStat_PendingLock saved_lock_stats;
	bool		saved_have_lockstats;
	PgStat_BackendPending saved_backend_stats;
	bool		saved_backend_has_iostats;
	PgStat_LocalState saved_local_state;
	MemoryContext saved_pending_context;
	WalUsage	saved_prev_backend_wal_usage;
	bool		saved_report_fixed;
	bool		saved_force_next_flush;
	bool		saved_force_snapshot_clear;
	void	   *saved_entry_ref_hash;
	int			saved_shared_ref_age;
	MemoryContext saved_shared_ref_context;
	MemoryContext saved_entry_ref_hash_context;
	bool		saved_pgstat_is_initialized;
	bool		saved_pgstat_is_shutdown;
	int			saved_xact_commit;
	int			saved_xact_rollback;
	instr_time	saved_func_total_time;
	WalUsage	saved_prev_wal_usage;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	saved_bgwriter_stats = PendingBgWriterStats;
	saved_checkpointer_stats = PendingCheckpointerStats;
	saved_block_read_time = pgStatBlockReadTime;
	saved_block_write_time = pgStatBlockWriteTime;
	saved_active_time = pgStatActiveTime;
	saved_transaction_idle_time = pgStatTransactionIdleTime;
	saved_io_stats = PendingIOStats;
	saved_have_iostats = have_iostats;
	saved_slru_stats = pending_SLRUStats[0];
	saved_have_slrustats = have_slrustats;
	saved_lock_stats = PendingLockStats;
	saved_have_lockstats = have_lockstats;
	saved_backend_stats = PendingBackendStats;
	saved_backend_has_iostats = backend_has_iostats;
	saved_local_state = pgStatLocal;
	saved_pending_context = *PgCurrentPgStatPendingContextRef();
	saved_prev_backend_wal_usage = prevBackendWalUsage;
	saved_report_fixed = pgstat_report_fixed;
	saved_force_next_flush = pgStatForceNextFlush;
	saved_force_snapshot_clear = force_stats_snapshot_clear;
	saved_entry_ref_hash = *PgCurrentPgStatEntryRefHashRef();
	saved_shared_ref_age = *PgCurrentPgStatSharedRefAgeRef();
	saved_shared_ref_context = *PgCurrentPgStatSharedRefContextRef();
	saved_entry_ref_hash_context = *PgCurrentPgStatEntryRefHashContextRef();
	saved_pgstat_is_initialized = pgstat_is_initialized;
	saved_pgstat_is_shutdown = pgstat_is_shutdown;
	saved_xact_commit = pgStatXactCommit;
	saved_xact_rollback = pgStatXactRollback;
	saved_func_total_time = total_func_time;
	saved_prev_wal_usage = prevWalUsage;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	dlist_init(&fake_backend1.pgstat_pending.pending);
	dlist_init(&fake_backend2.pgstat_pending.pending);

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		PendingBgWriterStats.buf_alloc = 11;
		PendingCheckpointerStats.num_requested = 12;
		pgStatBlockReadTime = 13;
		pgStatBlockWriteTime = 14;
		pgStatActiveTime = 15;
		pgStatTransactionIdleTime = 16;
		PendingIOStats.counts[IOOBJECT_RELATION][IOCONTEXT_NORMAL][IOOP_READ] = 17;
		have_iostats = true;
		pending_SLRUStats[0].blocks_hit = 18;
		have_slrustats = true;
		PendingLockStats.stats[LOCKTAG_RELATION].waits = 19;
		have_lockstats = true;
		PendingBackendStats.pending_io.counts[IOOBJECT_RELATION][IOCONTEXT_NORMAL][IOOP_WRITE] = 20;
		backend_has_iostats = true;
		pgStatLocal.shmem = (PgStat_ShmemControl *) &fake_backend1;
		pgStatLocal.dsa = (dsa_area *) &fake_backend1;
		pgStatLocal.shared_hash = (dshash_table *) &fake_backend1;
		pgStatSnapshot.mode = PGSTAT_FETCH_CONSISTENCY_CACHE;
		*PgCurrentPgStatPendingContextRef() = (MemoryContext) &fake_backend1;
		prevBackendWalUsage.wal_records = 21;
		pgstat_report_fixed = true;
		pgStatForceNextFlush = true;
		force_stats_snapshot_clear = true;
		*PgCurrentPgStatEntryRefHashRef() = &fake_backend1;
		*PgCurrentPgStatSharedRefAgeRef() = 26;
		*PgCurrentPgStatSharedRefContextRef() = (MemoryContext) &fake_backend1;
		*PgCurrentPgStatEntryRefHashContextRef() = (MemoryContext) &fake_backend1;
		pgstat_is_initialized = true;
		pgstat_is_shutdown = true;
		pgStatXactCommit = 22;
		pgStatXactRollback = 23;
		total_func_time.ticks = 24;
		prevWalUsage.wal_records = 25;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && PendingBgWriterStats.buf_alloc == 0;
		ok = ok && PendingCheckpointerStats.num_requested == 0;
		ok = ok && pgStatBlockReadTime == 0;
		ok = ok && pgStatBlockWriteTime == 0;
		ok = ok && pgStatActiveTime == 0;
		ok = ok && pgStatTransactionIdleTime == 0;
		ok = ok &&
			PendingIOStats.counts[IOOBJECT_RELATION][IOCONTEXT_NORMAL][IOOP_READ] == 0;
		ok = ok && !have_iostats;
		ok = ok && pending_SLRUStats[0].blocks_hit == 0;
		ok = ok && !have_slrustats;
		ok = ok && PendingLockStats.stats[LOCKTAG_RELATION].waits == 0;
		ok = ok && !have_lockstats;
		ok = ok &&
			PendingBackendStats.pending_io.counts[IOOBJECT_RELATION][IOCONTEXT_NORMAL][IOOP_WRITE] == 0;
		ok = ok && !backend_has_iostats;
		ok = ok && pgStatLocal.shmem == NULL;
		ok = ok && pgStatLocal.dsa == NULL;
		ok = ok && pgStatLocal.shared_hash == NULL;
		ok = ok && pgStatSnapshot.mode == PGSTAT_FETCH_CONSISTENCY_NONE;
		ok = ok && *PgCurrentPgStatPendingContextRef() == NULL;
		ok = ok && PgCurrentPgStatPendingListRef() == &fake_backend2.pgstat_pending.pending;
		ok = ok && dlist_is_empty(PgCurrentPgStatPendingListRef());
		ok = ok && prevBackendWalUsage.wal_records == 0;
		ok = ok && !pgstat_report_fixed;
		ok = ok && !pgStatForceNextFlush;
		ok = ok && !force_stats_snapshot_clear;
		ok = ok && *PgCurrentPgStatEntryRefHashRef() == NULL;
		ok = ok && *PgCurrentPgStatSharedRefAgeRef() == 0;
		ok = ok && *PgCurrentPgStatSharedRefContextRef() == NULL;
		ok = ok && *PgCurrentPgStatEntryRefHashContextRef() == NULL;
		ok = ok && !pgstat_is_initialized;
		ok = ok && !pgstat_is_shutdown;
		ok = ok && pgStatXactCommit == 0;
		ok = ok && pgStatXactRollback == 0;
		ok = ok && total_func_time.ticks == 0;
		ok = ok && prevWalUsage.wal_records == 0;

		PendingBgWriterStats.buf_alloc = 21;
		PendingCheckpointerStats.num_requested = 22;
		pgStatBlockReadTime = 23;
		pgStatBlockWriteTime = 24;
		pgStatActiveTime = 25;
		pgStatTransactionIdleTime = 26;
		PendingIOStats.counts[IOOBJECT_RELATION][IOCONTEXT_NORMAL][IOOP_READ] = 27;
		have_iostats = true;
		pending_SLRUStats[0].blocks_hit = 28;
		have_slrustats = true;
		PendingLockStats.stats[LOCKTAG_RELATION].waits = 29;
		have_lockstats = true;
		PendingBackendStats.pending_io.counts[IOOBJECT_RELATION][IOCONTEXT_NORMAL][IOOP_WRITE] = 30;
		backend_has_iostats = true;
		pgStatLocal.shmem = (PgStat_ShmemControl *) &fake_backend2;
		pgStatLocal.dsa = (dsa_area *) &fake_backend2;
		pgStatLocal.shared_hash = (dshash_table *) &fake_backend2;
		pgStatSnapshot.mode = PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
		*PgCurrentPgStatPendingContextRef() = (MemoryContext) &fake_backend2;
		prevBackendWalUsage.wal_records = 31;
		pgstat_report_fixed = true;
		pgStatForceNextFlush = true;
		force_stats_snapshot_clear = true;
		*PgCurrentPgStatEntryRefHashRef() = &fake_backend2;
		*PgCurrentPgStatSharedRefAgeRef() = 36;
		*PgCurrentPgStatSharedRefContextRef() = (MemoryContext) &fake_backend2;
		*PgCurrentPgStatEntryRefHashContextRef() = (MemoryContext) &fake_backend2;
		pgstat_is_initialized = true;
		pgstat_is_shutdown = true;
		pgStatXactCommit = 32;
		pgStatXactRollback = 33;
		total_func_time.ticks = 34;
		prevWalUsage.wal_records = 35;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && PendingBgWriterStats.buf_alloc == 11;
		ok = ok && PendingCheckpointerStats.num_requested == 12;
		ok = ok && pgStatBlockReadTime == 13;
		ok = ok && pgStatBlockWriteTime == 14;
		ok = ok && pgStatActiveTime == 15;
		ok = ok && pgStatTransactionIdleTime == 16;
		ok = ok &&
			PendingIOStats.counts[IOOBJECT_RELATION][IOCONTEXT_NORMAL][IOOP_READ] == 17;
		ok = ok && have_iostats;
		ok = ok && pending_SLRUStats[0].blocks_hit == 18;
		ok = ok && have_slrustats;
		ok = ok && PendingLockStats.stats[LOCKTAG_RELATION].waits == 19;
		ok = ok && have_lockstats;
		ok = ok &&
			PendingBackendStats.pending_io.counts[IOOBJECT_RELATION][IOCONTEXT_NORMAL][IOOP_WRITE] == 20;
		ok = ok && backend_has_iostats;
		ok = ok && pgStatLocal.shmem == (PgStat_ShmemControl *) &fake_backend1;
		ok = ok && pgStatLocal.dsa == (dsa_area *) &fake_backend1;
		ok = ok && pgStatLocal.shared_hash == (dshash_table *) &fake_backend1;
		ok = ok && pgStatSnapshot.mode == PGSTAT_FETCH_CONSISTENCY_CACHE;
		ok = ok && *PgCurrentPgStatPendingContextRef() == (MemoryContext) &fake_backend1;
		ok = ok && PgCurrentPgStatPendingListRef() == &fake_backend1.pgstat_pending.pending;
		ok = ok && dlist_is_empty(PgCurrentPgStatPendingListRef());
		ok = ok && prevBackendWalUsage.wal_records == 21;
		ok = ok && pgstat_report_fixed;
		ok = ok && pgStatForceNextFlush;
		ok = ok && force_stats_snapshot_clear;
		ok = ok && *PgCurrentPgStatEntryRefHashRef() == &fake_backend1;
		ok = ok && *PgCurrentPgStatSharedRefAgeRef() == 26;
		ok = ok && *PgCurrentPgStatSharedRefContextRef() == (MemoryContext) &fake_backend1;
		ok = ok && *PgCurrentPgStatEntryRefHashContextRef() == (MemoryContext) &fake_backend1;
		ok = ok && pgstat_is_initialized;
		ok = ok && pgstat_is_shutdown;
		ok = ok && pgStatXactCommit == 22;
		ok = ok && pgStatXactRollback == 23;
		ok = ok && total_func_time.ticks == 24;
		ok = ok && prevWalUsage.wal_records == 25;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && PendingBgWriterStats.buf_alloc == 21;
		ok = ok && PendingCheckpointerStats.num_requested == 22;
		ok = ok && pgStatBlockReadTime == 23;
		ok = ok && pgStatBlockWriteTime == 24;
		ok = ok && pgStatActiveTime == 25;
		ok = ok && pgStatTransactionIdleTime == 26;
		ok = ok &&
			PendingIOStats.counts[IOOBJECT_RELATION][IOCONTEXT_NORMAL][IOOP_READ] == 27;
		ok = ok && have_iostats;
		ok = ok && pending_SLRUStats[0].blocks_hit == 28;
		ok = ok && have_slrustats;
		ok = ok && PendingLockStats.stats[LOCKTAG_RELATION].waits == 29;
		ok = ok && have_lockstats;
		ok = ok &&
			PendingBackendStats.pending_io.counts[IOOBJECT_RELATION][IOCONTEXT_NORMAL][IOOP_WRITE] == 30;
		ok = ok && backend_has_iostats;
		ok = ok && pgStatLocal.shmem == (PgStat_ShmemControl *) &fake_backend2;
		ok = ok && pgStatLocal.dsa == (dsa_area *) &fake_backend2;
		ok = ok && pgStatLocal.shared_hash == (dshash_table *) &fake_backend2;
		ok = ok && pgStatSnapshot.mode == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
		ok = ok && *PgCurrentPgStatPendingContextRef() == (MemoryContext) &fake_backend2;
		ok = ok && PgCurrentPgStatPendingListRef() == &fake_backend2.pgstat_pending.pending;
		ok = ok && dlist_is_empty(PgCurrentPgStatPendingListRef());
		ok = ok && prevBackendWalUsage.wal_records == 31;
		ok = ok && pgstat_report_fixed;
		ok = ok && pgStatForceNextFlush;
		ok = ok && force_stats_snapshot_clear;
		ok = ok && *PgCurrentPgStatEntryRefHashRef() == &fake_backend2;
		ok = ok && *PgCurrentPgStatSharedRefAgeRef() == 36;
		ok = ok && *PgCurrentPgStatSharedRefContextRef() == (MemoryContext) &fake_backend2;
		ok = ok && *PgCurrentPgStatEntryRefHashContextRef() == (MemoryContext) &fake_backend2;
		ok = ok && pgstat_is_initialized;
		ok = ok && pgstat_is_shutdown;
		ok = ok && pgStatXactCommit == 32;
		ok = ok && pgStatXactRollback == 33;
		ok = ok && total_func_time.ticks == 34;
		ok = ok && prevWalUsage.wal_records == 35;

		PgSetCurrentBackend(saved_backend);
		PendingBgWriterStats = saved_bgwriter_stats;
		PendingCheckpointerStats = saved_checkpointer_stats;
		pgStatBlockReadTime = saved_block_read_time;
		pgStatBlockWriteTime = saved_block_write_time;
		pgStatActiveTime = saved_active_time;
		pgStatTransactionIdleTime = saved_transaction_idle_time;
		PendingIOStats = saved_io_stats;
		have_iostats = saved_have_iostats;
		pending_SLRUStats[0] = saved_slru_stats;
		have_slrustats = saved_have_slrustats;
		PendingLockStats = saved_lock_stats;
		have_lockstats = saved_have_lockstats;
		PendingBackendStats = saved_backend_stats;
		backend_has_iostats = saved_backend_has_iostats;
		pgStatLocal = saved_local_state;
		*PgCurrentPgStatPendingContextRef() = saved_pending_context;
		prevBackendWalUsage = saved_prev_backend_wal_usage;
		pgstat_report_fixed = saved_report_fixed;
		pgStatForceNextFlush = saved_force_next_flush;
		force_stats_snapshot_clear = saved_force_snapshot_clear;
		*PgCurrentPgStatEntryRefHashRef() = saved_entry_ref_hash;
		*PgCurrentPgStatSharedRefAgeRef() = saved_shared_ref_age;
		*PgCurrentPgStatSharedRefContextRef() = saved_shared_ref_context;
		*PgCurrentPgStatEntryRefHashContextRef() = saved_entry_ref_hash_context;
		pgstat_is_initialized = saved_pgstat_is_initialized;
		pgstat_is_shutdown = saved_pgstat_is_shutdown;
		pgStatXactCommit = saved_xact_commit;
		pgStatXactRollback = saved_xact_rollback;
		total_func_time = saved_func_total_time;
		prevWalUsage = saved_prev_wal_usage;
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PendingBgWriterStats = saved_bgwriter_stats;
		PendingCheckpointerStats = saved_checkpointer_stats;
		pgStatBlockReadTime = saved_block_read_time;
		pgStatBlockWriteTime = saved_block_write_time;
		pgStatActiveTime = saved_active_time;
		pgStatTransactionIdleTime = saved_transaction_idle_time;
		PendingIOStats = saved_io_stats;
		have_iostats = saved_have_iostats;
		pending_SLRUStats[0] = saved_slru_stats;
		have_slrustats = saved_have_slrustats;
		PendingLockStats = saved_lock_stats;
		have_lockstats = saved_have_lockstats;
		PendingBackendStats = saved_backend_stats;
		backend_has_iostats = saved_backend_has_iostats;
		pgStatLocal = saved_local_state;
		*PgCurrentPgStatPendingContextRef() = saved_pending_context;
		prevBackendWalUsage = saved_prev_backend_wal_usage;
		pgstat_report_fixed = saved_report_fixed;
		pgStatForceNextFlush = saved_force_next_flush;
		force_stats_snapshot_clear = saved_force_snapshot_clear;
		*PgCurrentPgStatEntryRefHashRef() = saved_entry_ref_hash;
		*PgCurrentPgStatSharedRefAgeRef() = saved_shared_ref_age;
		*PgCurrentPgStatSharedRefContextRef() = saved_shared_ref_context;
		*PgCurrentPgStatEntryRefHashContextRef() = saved_entry_ref_hash_context;
		pgstat_is_initialized = saved_pgstat_is_initialized;
		pgstat_is_shutdown = saved_pgstat_is_shutdown;
		pgStatXactCommit = saved_xact_commit;
		pgStatXactRollback = saved_xact_rollback;
		total_func_time = saved_func_total_time;
		prevWalUsage = saved_prev_wal_usage;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend pgstat pending state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_activity_state_is_backend_local);
Datum
test_backend_activity_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	LocalPgBackendStatus fake_status1;
	LocalPgBackendStatus fake_status2;
	LocalPgBackendStatus *saved_status_table;
	int			saved_num_backends;
	MemoryContext saved_status_context;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	saved_status_table = *PgCurrentLocalBackendStatusTableRef();
	saved_num_backends = *PgCurrentLocalNumBackendsRef();
	saved_status_context = *PgCurrentBackendStatusSnapContextRef();

	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	MemSet(&fake_status1, 0, sizeof(fake_status1));
	MemSet(&fake_status2, 0, sizeof(fake_status2));

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		*PgCurrentLocalBackendStatusTableRef() = &fake_status1;
		*PgCurrentLocalNumBackendsRef() = 11;
		*PgCurrentBackendStatusSnapContextRef() = (MemoryContext) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentLocalBackendStatusTableRef() == NULL;
		ok = ok && *PgCurrentLocalNumBackendsRef() == 0;
		ok = ok && *PgCurrentBackendStatusSnapContextRef() == NULL;

		*PgCurrentLocalBackendStatusTableRef() = &fake_status2;
		*PgCurrentLocalNumBackendsRef() = 22;
		*PgCurrentBackendStatusSnapContextRef() = (MemoryContext) &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && *PgCurrentLocalBackendStatusTableRef() == &fake_status1;
		ok = ok && *PgCurrentLocalNumBackendsRef() == 11;
		ok = ok && *PgCurrentBackendStatusSnapContextRef() == (MemoryContext) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && *PgCurrentLocalBackendStatusTableRef() == &fake_status2;
		ok = ok && *PgCurrentLocalNumBackendsRef() == 22;
		ok = ok && *PgCurrentBackendStatusSnapContextRef() == (MemoryContext) &fake_backend2;

		PgSetCurrentBackend(saved_backend);
		*PgCurrentLocalBackendStatusTableRef() = saved_status_table;
		*PgCurrentLocalNumBackendsRef() = saved_num_backends;
		*PgCurrentBackendStatusSnapContextRef() = saved_status_context;
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		*PgCurrentLocalBackendStatusTableRef() = saved_status_table;
		*PgCurrentLocalNumBackendsRef() = saved_num_backends;
		*PgCurrentBackendStatusSnapContextRef() = saved_status_context;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend activity state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_memory_manager_state_is_backend_local);
Datum
test_backend_memory_manager_state_is_backend_local(PG_FUNCTION_ARGS)
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
		PgCurrentAllocSetContextFreeLists()[0].num_free = 11;
		PgCurrentAllocSetContextFreeLists()[0].first_free =
			(struct AllocSetContext *) &fake_backend1;
		PgCurrentAllocSetContextFreeLists()[1].num_free = 12;
		PgCurrentAllocSetContextFreeLists()[1].first_free =
			(struct AllocSetContext *) &fake_backend1;
		*PgCurrentLogMemoryContextInProgressRef() = true;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && PgCurrentAllocSetContextFreeLists()[0].num_free == 0;
		ok = ok && PgCurrentAllocSetContextFreeLists()[0].first_free == NULL;
		ok = ok && PgCurrentAllocSetContextFreeLists()[1].num_free == 0;
		ok = ok && PgCurrentAllocSetContextFreeLists()[1].first_free == NULL;
		ok = ok && !*PgCurrentLogMemoryContextInProgressRef();

		PgCurrentAllocSetContextFreeLists()[0].num_free = 21;
		PgCurrentAllocSetContextFreeLists()[0].first_free =
			(struct AllocSetContext *) &fake_backend2;
		PgCurrentAllocSetContextFreeLists()[1].num_free = 22;
		PgCurrentAllocSetContextFreeLists()[1].first_free =
			(struct AllocSetContext *) &fake_backend2;
		*PgCurrentLogMemoryContextInProgressRef() = true;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && PgCurrentAllocSetContextFreeLists()[0].num_free == 11;
		ok = ok && PgCurrentAllocSetContextFreeLists()[0].first_free ==
			(struct AllocSetContext *) &fake_backend1;
		ok = ok && PgCurrentAllocSetContextFreeLists()[1].num_free == 12;
		ok = ok && PgCurrentAllocSetContextFreeLists()[1].first_free ==
			(struct AllocSetContext *) &fake_backend1;
		ok = ok && *PgCurrentLogMemoryContextInProgressRef();

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && PgCurrentAllocSetContextFreeLists()[0].num_free == 21;
		ok = ok && PgCurrentAllocSetContextFreeLists()[0].first_free ==
			(struct AllocSetContext *) &fake_backend2;
		ok = ok && PgCurrentAllocSetContextFreeLists()[1].num_free == 22;
		ok = ok && PgCurrentAllocSetContextFreeLists()[1].first_free ==
			(struct AllocSetContext *) &fake_backend2;
		ok = ok && *PgCurrentLogMemoryContextInProgressRef();

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	{
		PgBackendAllocSetFreeList *freelists;
		MemoryContext context;

		freelists = PgCurrentAllocSetContextFreeLists();
		context = AllocSetContextCreate(TopMemoryContext,
										"test backend memory manager freelist",
										ALLOCSET_SMALL_SIZES);
		MemoryContextDelete(context);
		ok = ok && freelists[1].num_free > 0;
		ok = ok && freelists[1].first_free != NULL;

		AllocSetFreeContextFreelists(freelists,
									 PG_BACKEND_ALLOCSET_NUM_FREELISTS);
		ok = ok && freelists[0].num_free == 0;
		ok = ok && freelists[0].first_free == NULL;
		ok = ok && freelists[1].num_free == 0;
		ok = ok && freelists[1].first_free == NULL;
	}

	if (!ok)
		elog(ERROR, "backend memory manager state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_utility_state_is_backend_local);
Datum
test_backend_utility_state_is_backend_local(PG_FUNCTION_ARGS)
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
		*PgCurrentNotifyInterruptPendingRef() = true;
		*PgCurrentAsyncUnlistenExitRegisteredRef() = true;
		*PgCurrentExtensionSiblingListRef() =
			(struct ExtensionSiblingCache *) &fake_backend1;
		*PgCurrentInjectionPointCacheRef() = (HTAB *) &fake_backend1;
		PgCurrentSamplingOldReservoirRef()->W = 1.25;
		*PgCurrentSamplingOldReservoirInitializedRef() = true;
		PgCurrentSeqScanTables()[0] = (HTAB *) &fake_backend1;
		PgCurrentSeqScanLevels()[0] = 11;
		*PgCurrentNumSeqScansRef() = 1;
		*PgCurrentSuperuserLastRoleIdRef() = 101;
		*PgCurrentSuperuserLastRoleIdIsSuperRef() = true;
		*PgCurrentSuperuserRoleIdCallbackRegisteredRef() = true;
		*PgCurrentResourceReleaseCallbacksRef() = &fake_backend1;
		PgCurrentDateTokenCache()[0] = &fake_backend1;
		PgCurrentDeltaTokenCache()[0] = &fake_backend1;
		*PgCurrentDegreeConstsSetRef() = true;
		*PgCurrentDegreeSin30Ref() = 0.5;
		*PgCurrentDegreeOneMinusCos60Ref() = 0.5;
		*PgCurrentDegreeAsin05Ref() = 30.0;
		*PgCurrentDegreeAcos05Ref() = 60.0;
		*PgCurrentDegreeAtan10Ref() = 45.0;
		*PgCurrentDegreeTan45Ref() = 1.0;
		*PgCurrentDegreeCot45Ref() = 1.0;
		PgCurrentDCHCache()[0] = &fake_backend1;
		*PgCurrentNumDCHCacheRef() = 1;
		*PgCurrentDCHCounterRef() = 11;
		PgCurrentNUMCache()[0] = &fake_backend1;
		*PgCurrentNumNUMCacheRef() = 1;
		*PgCurrentNUMCounterRef() = 12;
		*PgCurrentLibxmlContextRef() = (MemoryContext) &fake_backend1;
		*PgCurrentMissingAttrCacheRef() = (HTAB *) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && !*PgCurrentNotifyInterruptPendingRef();
		ok = ok && !*PgCurrentAsyncUnlistenExitRegisteredRef();
		ok = ok && *PgCurrentExtensionSiblingListRef() == NULL;
		ok = ok && *PgCurrentInjectionPointCacheRef() == NULL;
		ok = ok && PgCurrentSamplingOldReservoirRef()->W == 0.0;
		ok = ok && !*PgCurrentSamplingOldReservoirInitializedRef();
		ok = ok && PgCurrentSeqScanTables()[0] == NULL;
		ok = ok && PgCurrentSeqScanLevels()[0] == 0;
		ok = ok && *PgCurrentNumSeqScansRef() == 0;
		ok = ok && *PgCurrentSuperuserLastRoleIdRef() == InvalidOid;
		ok = ok && !*PgCurrentSuperuserLastRoleIdIsSuperRef();
		ok = ok && !*PgCurrentSuperuserRoleIdCallbackRegisteredRef();
		ok = ok && *PgCurrentResourceReleaseCallbacksRef() == NULL;
		ok = ok && PgCurrentDateTokenCache()[0] == NULL;
		ok = ok && PgCurrentDeltaTokenCache()[0] == NULL;
		ok = ok && !*PgCurrentDegreeConstsSetRef();
		ok = ok && *PgCurrentDegreeSin30Ref() == 0.0;
		ok = ok && *PgCurrentDegreeOneMinusCos60Ref() == 0.0;
		ok = ok && *PgCurrentDegreeAsin05Ref() == 0.0;
		ok = ok && *PgCurrentDegreeAcos05Ref() == 0.0;
		ok = ok && *PgCurrentDegreeAtan10Ref() == 0.0;
		ok = ok && *PgCurrentDegreeTan45Ref() == 0.0;
		ok = ok && *PgCurrentDegreeCot45Ref() == 0.0;
		ok = ok && PgCurrentDCHCache()[0] == NULL;
		ok = ok && *PgCurrentNumDCHCacheRef() == 0;
		ok = ok && *PgCurrentDCHCounterRef() == 0;
		ok = ok && PgCurrentNUMCache()[0] == NULL;
		ok = ok && *PgCurrentNumNUMCacheRef() == 0;
		ok = ok && *PgCurrentNUMCounterRef() == 0;
		ok = ok && *PgCurrentLibxmlContextRef() == NULL;
		ok = ok && *PgCurrentMissingAttrCacheRef() == NULL;

		*PgCurrentNotifyInterruptPendingRef() = false;
		*PgCurrentAsyncUnlistenExitRegisteredRef() = true;
		*PgCurrentExtensionSiblingListRef() =
			(struct ExtensionSiblingCache *) &fake_backend2;
		*PgCurrentInjectionPointCacheRef() = (HTAB *) &fake_backend2;
		PgCurrentSamplingOldReservoirRef()->W = 2.25;
		*PgCurrentSamplingOldReservoirInitializedRef() = true;
		PgCurrentSeqScanTables()[0] = (HTAB *) &fake_backend2;
		PgCurrentSeqScanLevels()[0] = 22;
		*PgCurrentNumSeqScansRef() = 1;
		*PgCurrentSuperuserLastRoleIdRef() = 202;
		*PgCurrentSuperuserLastRoleIdIsSuperRef() = false;
		*PgCurrentSuperuserRoleIdCallbackRegisteredRef() = true;
		*PgCurrentResourceReleaseCallbacksRef() = &fake_backend2;
		PgCurrentDateTokenCache()[0] = &fake_backend2;
		PgCurrentDeltaTokenCache()[0] = &fake_backend2;
		*PgCurrentDegreeConstsSetRef() = true;
		*PgCurrentDegreeSin30Ref() = 0.25;
		*PgCurrentDegreeOneMinusCos60Ref() = 0.75;
		*PgCurrentDegreeAsin05Ref() = 31.0;
		*PgCurrentDegreeAcos05Ref() = 61.0;
		*PgCurrentDegreeAtan10Ref() = 46.0;
		*PgCurrentDegreeTan45Ref() = 1.1;
		*PgCurrentDegreeCot45Ref() = 0.9;
		PgCurrentDCHCache()[0] = &fake_backend2;
		*PgCurrentNumDCHCacheRef() = 2;
		*PgCurrentDCHCounterRef() = 21;
		PgCurrentNUMCache()[0] = &fake_backend2;
		*PgCurrentNumNUMCacheRef() = 2;
		*PgCurrentNUMCounterRef() = 22;
		*PgCurrentLibxmlContextRef() = (MemoryContext) &fake_backend2;
		*PgCurrentMissingAttrCacheRef() = (HTAB *) &fake_backend2;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && *PgCurrentNotifyInterruptPendingRef();
		ok = ok && *PgCurrentAsyncUnlistenExitRegisteredRef();
		ok = ok && *PgCurrentExtensionSiblingListRef() ==
			(struct ExtensionSiblingCache *) &fake_backend1;
		ok = ok && *PgCurrentInjectionPointCacheRef() == (HTAB *) &fake_backend1;
		ok = ok && PgCurrentSamplingOldReservoirRef()->W == 1.25;
		ok = ok && *PgCurrentSamplingOldReservoirInitializedRef();
		ok = ok && PgCurrentSeqScanTables()[0] == (HTAB *) &fake_backend1;
		ok = ok && PgCurrentSeqScanLevels()[0] == 11;
		ok = ok && *PgCurrentNumSeqScansRef() == 1;
		ok = ok && *PgCurrentSuperuserLastRoleIdRef() == 101;
		ok = ok && *PgCurrentSuperuserLastRoleIdIsSuperRef();
		ok = ok && *PgCurrentSuperuserRoleIdCallbackRegisteredRef();
		ok = ok && *PgCurrentResourceReleaseCallbacksRef() == &fake_backend1;
		ok = ok && PgCurrentDateTokenCache()[0] == &fake_backend1;
		ok = ok && PgCurrentDeltaTokenCache()[0] == &fake_backend1;
		ok = ok && *PgCurrentDegreeConstsSetRef();
		ok = ok && *PgCurrentDegreeSin30Ref() == 0.5;
		ok = ok && *PgCurrentDegreeOneMinusCos60Ref() == 0.5;
		ok = ok && *PgCurrentDegreeAsin05Ref() == 30.0;
		ok = ok && *PgCurrentDegreeAcos05Ref() == 60.0;
		ok = ok && *PgCurrentDegreeAtan10Ref() == 45.0;
		ok = ok && *PgCurrentDegreeTan45Ref() == 1.0;
		ok = ok && *PgCurrentDegreeCot45Ref() == 1.0;
		ok = ok && PgCurrentDCHCache()[0] == &fake_backend1;
		ok = ok && *PgCurrentNumDCHCacheRef() == 1;
		ok = ok && *PgCurrentDCHCounterRef() == 11;
		ok = ok && PgCurrentNUMCache()[0] == &fake_backend1;
		ok = ok && *PgCurrentNumNUMCacheRef() == 1;
		ok = ok && *PgCurrentNUMCounterRef() == 12;
		ok = ok && *PgCurrentLibxmlContextRef() == (MemoryContext) &fake_backend1;
		ok = ok && *PgCurrentMissingAttrCacheRef() == (HTAB *) &fake_backend1;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && !*PgCurrentNotifyInterruptPendingRef();
		ok = ok && *PgCurrentAsyncUnlistenExitRegisteredRef();
		ok = ok && *PgCurrentExtensionSiblingListRef() ==
			(struct ExtensionSiblingCache *) &fake_backend2;
		ok = ok && *PgCurrentInjectionPointCacheRef() == (HTAB *) &fake_backend2;
		ok = ok && PgCurrentSamplingOldReservoirRef()->W == 2.25;
		ok = ok && *PgCurrentSamplingOldReservoirInitializedRef();
		ok = ok && PgCurrentSeqScanTables()[0] == (HTAB *) &fake_backend2;
		ok = ok && PgCurrentSeqScanLevels()[0] == 22;
		ok = ok && *PgCurrentNumSeqScansRef() == 1;
		ok = ok && *PgCurrentSuperuserLastRoleIdRef() == 202;
		ok = ok && !*PgCurrentSuperuserLastRoleIdIsSuperRef();
		ok = ok && *PgCurrentSuperuserRoleIdCallbackRegisteredRef();
		ok = ok && *PgCurrentResourceReleaseCallbacksRef() == &fake_backend2;
		ok = ok && PgCurrentDateTokenCache()[0] == &fake_backend2;
		ok = ok && PgCurrentDeltaTokenCache()[0] == &fake_backend2;
		ok = ok && *PgCurrentDegreeConstsSetRef();
		ok = ok && *PgCurrentDegreeSin30Ref() == 0.25;
		ok = ok && *PgCurrentDegreeOneMinusCos60Ref() == 0.75;
		ok = ok && *PgCurrentDegreeAsin05Ref() == 31.0;
		ok = ok && *PgCurrentDegreeAcos05Ref() == 61.0;
		ok = ok && *PgCurrentDegreeAtan10Ref() == 46.0;
		ok = ok && *PgCurrentDegreeTan45Ref() == 1.1;
		ok = ok && *PgCurrentDegreeCot45Ref() == 0.9;
		ok = ok && PgCurrentDCHCache()[0] == &fake_backend2;
		ok = ok && *PgCurrentNumDCHCacheRef() == 2;
		ok = ok && *PgCurrentDCHCounterRef() == 21;
		ok = ok && PgCurrentNUMCache()[0] == &fake_backend2;
		ok = ok && *PgCurrentNumNUMCacheRef() == 2;
		ok = ok && *PgCurrentNUMCounterRef() == 22;
		ok = ok && *PgCurrentLibxmlContextRef() == (MemoryContext) &fake_backend2;
		ok = ok && *PgCurrentMissingAttrCacheRef() == (HTAB *) &fake_backend2;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend utility state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_reset_closed_state);
static void
test_backend_runtime_resource_release_callback(ResourceReleasePhase phase,
											  bool isCommit,
											  bool isTopLevel,
											  void *arg)
{
}

static void
test_backend_runtime_timeout_handler(void)
{
}

Datum
test_backend_reset_closed_state(PG_FUNCTION_ARGS)
{
	PgBackend	fake_backend;
	PgBackendUtilityState *utility;
	PgBackendWalSenderState *walsender;
	PgBackendReplicationState *replication;
	PgBackendLogicalReplicationState *logical_replication;
	PgBackendXLogState *xlog;
	PgBackendMaintenanceWorkerState *maintenance_worker;
	PgBackendAutovacuumState *autovacuum;
	PgBackendAioState *aio;
	PgBackendLockState *locks;
	PgBackendActivityState *activity;
	PgBackendStorageState *storage;
	PgBackendTimeoutState *timeout;
	PgBackendParallelState *parallel;
	PgBackendBufferState *buffers;
	PgBackendIPCState *ipc;
	PgBackendTransactionState *transaction;
	PgBackendRecoveryState *recovery;
	PgBackendRepackState *repack;
	PgBackendPgStatPendingState *pgstat_pending;
	PgBackendWaitState *wait_state;
	PgBackend  *saved_backend;
	PgRuntime	fake_runtime;
	PgRuntime  *saved_runtime;
	bool		saved_proc_exit_active;
	HASHCTL		hash_ctl;
	bool		ok = true;

	MemSet(&fake_backend, 0, sizeof(fake_backend));
	MemSet(&fake_runtime, 0, sizeof(fake_runtime));
	fake_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
	fake_backend.runtime = &fake_runtime;
	fake_backend.backend_type = B_BACKEND;
	utility = &fake_backend.utility;
	walsender = &fake_backend.walsender;
	replication = &fake_backend.replication;
	logical_replication = &fake_backend.logical_replication;
	xlog = &fake_backend.xlog;
	maintenance_worker = &fake_backend.maintenance_worker;
	autovacuum = &fake_backend.autovacuum;
	aio = &fake_backend.aio;
	locks = &fake_backend.locks;
	activity = &fake_backend.activity;
	storage = &fake_backend.storage;
	timeout = &fake_backend.timeout;
	parallel = &fake_backend.parallel;
	buffers = &fake_backend.buffers;
	ipc = &fake_backend.ipc;
	transaction = &fake_backend.transaction;
	recovery = &fake_backend.recovery;
	repack = &fake_backend.repack;
	pgstat_pending = &fake_backend.pgstat_pending;
	wait_state = &fake_backend.wait_state;
	replication->walreceiver_recv_file = -1;
	xlog->open_log_file = -1;
	parallel->worker_number = -1;
	parallel->pq_mq_parallel_leader_proc_number = INVALID_PROC_NUMBER;
	buffers->reserved_ref_count_slot = -1;
	buffers->private_ref_count_entry_last = -1;
	transaction->cached_fetch_xid = InvalidTransactionId;
	transaction->two_phase_cached_fxid = InvalidFullTransactionId;
	transaction->procarray_cached_xid_not_in_progress = InvalidTransactionId;
	transaction->compute_xid_horizons_result_last_xmin = InvalidTransactionId;
	dclist_init(&transaction->multixact_cache);
	recovery->standby_wait_us = PG_BACKEND_STANDBY_INITIAL_WAIT_US;
	repack->repacked_rel_locator.relNumber = InvalidOid;
	repack->repacked_rel_toast_locator.relNumber = InvalidOid;
	dlist_init(&pgstat_pending->pending);
	pg_atomic_init_u32(&wait_state->waiting, 0);

	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(Oid);

	walsender->uploaded_manifest_mcxt =
		AllocSetContextCreate(TopMemoryContext,
							  "test uploaded manifest context",
							  ALLOCSET_SMALL_SIZES);
	{
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(walsender->uploaded_manifest_mcxt);
		walsender->uploaded_manifest = (IncrementalBackupInfo *) palloc(8);
		MemoryContextSwitchTo(oldcontext);
	}
	initStringInfo(&walsender->output_message);
	appendStringInfoString(&walsender->output_message, "output");
	initStringInfo(&walsender->reply_message);
	appendStringInfoString(&walsender->reply_message, "reply");
	initStringInfo(&walsender->tmpbuf);
	appendStringInfoString(&walsender->tmpbuf, "tmp");
	walsender->replication_cmd_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test replication command context",
							  ALLOCSET_SMALL_SIZES);
	walsender->lag_tracker = (LagTracker *) palloc0(8);

	initStringInfo(&replication->walreceiver_reply_message);
	appendStringInfoString(&replication->walreceiver_reply_message, "walrcv");

	logical_replication->subxact_data.nsubxacts = 1;
	logical_replication->subxact_data.nsubxacts_max = 1;
	logical_replication->subxact_data.subxact_last = FirstNormalTransactionId;
	logical_replication->subxact_data.subxacts = palloc(8);
	logical_replication->apply_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test apply context",
							  ALLOCSET_SMALL_SIZES);
	logical_replication->apply_error_callback_arg.rel =
		(struct LogicalRepRelMapEntry *) &fake_backend;
	logical_replication->apply_error_callback_arg.remote_attnum = 10;
	logical_replication->apply_error_callback_arg.remote_xid =
		FirstNormalTransactionId;
	logical_replication->apply_error_callback_arg.finish_lsn = 42;
	logical_replication->apply_error_callback_arg.origin_name =
		pstrdup("origin");
	logical_replication->my_parallel_shared =
		(ParallelApplyWorkerShared *) &fake_backend;
	logical_replication->my_subscription = (Subscription *) &fake_backend;
	logical_replication->my_subscription_valid = true;
	logical_replication->my_logical_rep_worker =
		(LogicalRepWorker *) &fake_backend;
	logical_replication->on_commit_wakeup_workers_subids = list_make1_int(1);
	logical_replication->copybuf = makeStringInfo();
	appendStringInfoString(logical_replication->copybuf, "copy");
	logical_replication->table_states_not_ready = list_make1_int(2);
	logical_replication->seqinfos = list_make1_int(3);
	logical_replication->slotsync_observed_primary_conninfo =
		pstrdup("conninfo");
	logical_replication->slotsync_observed_primary_slotname =
		pstrdup("slotname");
	logical_replication->parallel_apply_txn_hash =
		hash_create("test parallel apply txn hash", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	logical_replication->parallel_apply_worker_pool = list_make1_int(4);
	logical_replication->stream_apply_worker =
		(ParallelApplyWorkerInfo *) &fake_backend;
	logical_replication->parallel_apply_subxactlist = list_make1_int(5);
	logical_replication->parallel_apply_message_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test parallel apply message context",
							  ALLOCSET_SMALL_SIZES);

	xlog->wal_debug_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test wal debug context",
							  ALLOCSET_SMALL_SIZES);
	xlog->btree_xlog_op_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test btree xlog context",
							  ALLOCSET_SMALL_SIZES);
	xlog->gin_xlog_op_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test gin xlog context",
							  ALLOCSET_SMALL_SIZES);
	xlog->gist_xlog_op_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test gist xlog context",
							  ALLOCSET_SMALL_SIZES);
	xlog->spgist_xlog_op_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test spgist xlog context",
							  ALLOCSET_SMALL_SIZES);

	maintenance_worker->arch_module_errdetail_string =
		pstrdup("archive detail");
	maintenance_worker->archive_callbacks =
		(const struct ArchiveModuleCallbacks *) &fake_backend;
	maintenance_worker->archive_module_state =
		(struct ArchiveModuleState *) palloc0(8);
	maintenance_worker->archive_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test archive context",
							  ALLOCSET_SMALL_SIZES);
	maintenance_worker->loaded_archive_library = pstrdup("archive_library");
	maintenance_worker->pgarch_files = palloc0(8);
	maintenance_worker->bgwriter_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test background writer context",
							  ALLOCSET_SMALL_SIZES);
	maintenance_worker->walwriter_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test WAL writer context",
							  ALLOCSET_SMALL_SIZES);
	maintenance_worker->checkpointer_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test checkpointer context",
							  ALLOCSET_SMALL_SIZES);
	maintenance_worker->walsummarizer_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test WAL summarizer context",
							  ALLOCSET_SMALL_SIZES);

	autovacuum->autovac_mem_cxt =
		AllocSetContextCreate(TopMemoryContext,
							  "test autovacuum context",
							  ALLOCSET_SMALL_SIZES);
	autovacuum->database_list_cxt =
		AllocSetContextCreate(autovacuum->autovac_mem_cxt,
							  "test database list context",
							  ALLOCSET_SMALL_SIZES);
	dlist_init(&autovacuum->database_list);
	autovacuum->avl_dbase_array = (struct avl_dbase *) &fake_backend;
	autovacuum->my_worker_info = (struct WorkerInfoData *) &fake_backend;

	aio->my_backend = (struct PgAioBackend *) &fake_backend;
	aio->my_io_worker_id = 4;
	aio->my_uring_context = (struct PgAioUringContext *) &fake_backend;

	locks->fast_path_local_use_counts = palloc0(8);
	locks->fast_path_local_use_counts_owned = true;
	locks->strong_lock_in_progress = &fake_backend;
	locks->awaited_lock = &fake_backend;
	locks->awaited_owner = &fake_backend;
	locks->deadlock_visited_procs = palloc0(8);
	locks->deadlock_n_visited_procs = 1;
	locks->deadlock_topo_procs = locks->deadlock_visited_procs;
	locks->deadlock_before_constraints = palloc0(8);
	locks->deadlock_after_constraints = palloc0(8);
	locks->deadlock_wait_orders = palloc0(8);
	locks->deadlock_n_wait_orders = 2;
	locks->deadlock_wait_order_procs = palloc0(8);
	locks->deadlock_cur_constraints = palloc0(8);
	locks->deadlock_n_cur_constraints = 3;
	locks->deadlock_max_cur_constraints = 4;
	locks->deadlock_possible_constraints = palloc0(8);
	locks->deadlock_n_possible_constraints = 5;
	locks->deadlock_max_possible_constraints = 6;
	locks->deadlock_details = palloc0(8);
	locks->deadlock_n_details = 7;
	locks->blocking_autovacuum_proc = &fake_backend;
	locks->deadlock_workspace_owned = true;
	locks->lwlock_stats_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test LWLock stats context",
							  ALLOCSET_SMALL_SIZES);
	locks->lwlock_stats_htab = (HTAB *) &fake_backend;
	locks->lwlock_stats_exit_registered = true;

	activity->backend_status_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test backend status snapshot context",
							  ALLOCSET_SMALL_SIZES);
	activity->backend_status_table =
		MemoryContextAlloc(activity->backend_status_context,
						   sizeof(LocalPgBackendStatus));
	activity->num_backends = 1;

	storage->vfd_cache = malloc(8);
	storage->size_vfd_cache = 0;
	storage->nfile = 1;
	storage->temporary_files_allowed = true;
	storage->allocated_descs = malloc(8);
	storage->num_allocated_descs = 0;
	storage->max_allocated_descs = 1;
	storage->num_external_fds = 2;
	storage->sync_pending_ops_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test pending sync context",
							  ALLOCSET_SMALL_SIZES);
	hash_ctl.hcxt = storage->sync_pending_ops_context;
	storage->sync_pending_ops =
		hash_create("test pending sync hash", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	{
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(storage->sync_pending_ops_context);
		storage->sync_pending_unlinks = list_make1(pstrdup("pending-unlink"));
		MemoryContextSwitchTo(oldcontext);
	}
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(Oid);
	storage->smgr_relation_hash =
		hash_create("test smgr relation hash", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	dlist_init(&storage->smgr_unpinned_relations);
	storage->md_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test md context",
							  ALLOCSET_SMALL_SIZES);
	storage->sync_cycle_counter = 3;
	storage->sync_checkpoint_cycle_counter = 4;
	storage->sync_in_progress = true;

	timeout->all_timeouts_initialized = true;
	timeout->all_timeouts[DEADLOCK_TIMEOUT].index = DEADLOCK_TIMEOUT;
	timeout->all_timeouts[DEADLOCK_TIMEOUT].active = true;
	timeout->all_timeouts[DEADLOCK_TIMEOUT].indicator = true;
	timeout->all_timeouts[DEADLOCK_TIMEOUT].target_backend = &fake_backend;
	timeout->all_timeouts[DEADLOCK_TIMEOUT].target_execution =
		(PgExecution *) &fake_backend;
	timeout->all_timeouts[DEADLOCK_TIMEOUT].timeout_handler =
		test_backend_runtime_timeout_handler;
	timeout->all_timeouts[DEADLOCK_TIMEOUT].start_time = 1;
	timeout->all_timeouts[DEADLOCK_TIMEOUT].fin_time = 2;
	timeout->all_timeouts[DEADLOCK_TIMEOUT].interval_in_ms = 3;
	timeout->num_active_timeouts = 1;
	timeout->active_timeouts[0] = &timeout->all_timeouts[DEADLOCK_TIMEOUT];
	timeout->alarm_enabled = true;
	timeout->signal_pending = true;
	timeout->signal_due_at = 4;
	timeout->firing_timeout_target = &fake_backend;
	timeout->firing_timeout_execution = (PgExecution *) &fake_backend;
	timeout->signal_delivery = true;

	parallel->message_pending = true;
	parallel->initializing_worker = true;
	parallel->fixed_parallel_state = &fake_backend;
	dlist_init(&parallel->context_list);
	parallel->context_list_initialized = true;
	parallel->leader_pid = 11;
	parallel->pq_mq_busy = true;
	parallel->pq_mq_parallel_leader_pid = 12;
	parallel->pq_mq_parallel_leader_proc_number = 13;
	parallel->message_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test parallel message context",
							  ALLOCSET_SMALL_SIZES);

	buffers->pin_count_wait_buf = (BufferDesc *) &fake_backend;
	buffers->nlocbuffer = 2;
	buffers->local_buffer_descriptors = malloc(8);
	buffers->local_buffer_block_pointers = malloc(8);
	buffers->local_ref_count = malloc(8);
	buffers->next_free_local_buf_id = 1;
	buffers->local_buf_hash =
		hash_create("test local buffer hash", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	buffers->local_buffer_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test local buffer context",
							  ALLOCSET_SMALL_SIZES);
	buffers->local_buffer_cur_block =
		MemoryContextAlloc(buffers->local_buffer_context, 8);
	buffers->local_buffer_next_buf_in_block = 1;
	buffers->local_buffer_num_bufs_in_block = 2;
	buffers->local_buffer_total_bufs_allocated = 3;
	buffers->buffer_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test buffer context",
							  ALLOCSET_SMALL_SIZES);
	buffers->backend_writeback_context =
		MemoryContextAllocZero(buffers->buffer_context, 8);
	buffers->private_ref_count_array_keys =
		MemoryContextAllocZero(buffers->buffer_context, 8);
	buffers->private_ref_count_array =
		MemoryContextAllocZero(buffers->buffer_context, 8);
	buffers->private_ref_count_hash =
		hash_create("test private refcount hash", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	buffers->private_ref_count_clock = 4;
	buffers->reserved_ref_count_slot = 5;
	buffers->private_ref_count_entry_last = 6;
	buffers->max_proportional_pins = 7;

	ipc->proc_signal_slot = &fake_backend;
	ipc->shared_invalid_message_counter = 8;
	ipc->catchup_interrupt_pending = true;
	ipc->shared_invalidation_messages = &fake_backend;
	ipc->shared_invalidation_next_msg = 9;
	ipc->shared_invalidation_num_msgs = 10;
	ipc->dsm_init_done = true;
	ipc->next_local_transaction_id = 11;

	transaction->cached_fetch_xid = FirstNormalTransactionId;
	transaction->cached_fetch_xid_status = 1;
	transaction->cached_commit_lsn = 12;
	transaction->two_phase_locked_gxact = &fake_backend;
	transaction->two_phase_exit_registered = true;
	transaction->two_phase_cached_fxid =
		FullTransactionIdFromEpochAndXid(1, FirstNormalTransactionId);
	transaction->two_phase_cached_gxact = &fake_backend;
	transaction->slru_error_cause = 13;
	transaction->slru_errno_value = 14;
	transaction->multixact_cache_initialized = true;
	transaction->multixact_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test multixact context",
							  ALLOCSET_SMALL_SIZES);
	transaction->multixact_debug_string = pstrdup("mxact");
	transaction->procarray_cached_xid_not_in_progress =
		FirstNormalTransactionId;
	transaction->compute_xid_horizons_result_last_xmin =
		FirstNormalTransactionId;
	transaction->xidcache_by_recent_xmin = 1;
	transaction->xidcache_by_known_xact = 2;
	transaction->xidcache_by_my_xact = 3;
	transaction->xidcache_by_latest_xid = 4;
	transaction->xidcache_by_main_xid = 5;
	transaction->xidcache_by_child_xid = 6;
	transaction->xidcache_by_known_assigned = 7;
	transaction->xidcache_no_overflow = 8;
	transaction->xidcache_slow_answer = 9;

	recovery->startup_got_sighup = true;
	recovery->startup_shutdown_requested = true;
	recovery->startup_promote_signaled = true;
	recovery->startup_in_restore_command = true;
	recovery->startup_progress_phase_start_time = 15;
	recovery->startup_progress_timer_expired = true;
	recovery->local_hot_standby_active = true;
	recovery->local_promote_is_triggered = true;
	recovery->recovery_lock_hash =
		hash_create("test recovery lock hash", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	recovery->recovery_lock_xid_hash =
		hash_create("test recovery lock xid hash", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	recovery->got_standby_deadlock_timeout = true;
	recovery->got_standby_delay_timeout = true;
	recovery->got_standby_lock_timeout = true;
	recovery->standby_wait_us = 16;

	repack->message_pending = true;
	repack->am_repack_worker = true;
	repack->current_segment = 17;
	repack->repacked_rel_locator.spcOid = 18;
	repack->repacked_rel_locator.dbOid = 19;
	repack->repacked_rel_locator.relNumber = 20;
	repack->repacked_rel_toast_locator.spcOid = 21;
	repack->repacked_rel_toast_locator.dbOid = 22;
	repack->repacked_rel_toast_locator.relNumber = 23;
	repack->message_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test repack message context",
							  ALLOCSET_SMALL_SIZES);

	pgstat_pending->local = palloc0_object(PgStat_LocalState);
	pgstat_pending->local->snapshot = palloc0_object(PgStat_Snapshot);
	pgstat_pending->local->snapshot->context =
		AllocSetContextCreate(TopMemoryContext,
							  "test pgstat snapshot context",
							  ALLOCSET_SMALL_SIZES);
	pgstat_pending->local->snapshot->stats =
		(struct pgstat_snapshot_hash *) &fake_backend;
	pgstat_pending->local->snapshot->mode = PGSTAT_FETCH_CONSISTENCY_CACHE;
	pgstat_pending->shared_ref_age = 24;
	pgstat_pending->shared_ref_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test pgstat shared ref context",
							  ALLOCSET_SMALL_SIZES);
	pgstat_pending->entry_ref_hash_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test pgstat entry ref hash context",
							  ALLOCSET_SMALL_SIZES);
	pgstat_pending->pending_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test pgstat pending context",
							  ALLOCSET_SMALL_SIZES);
	pgstat_pending->cold = malloc(sizeof(PgBackendPgStatPendingColdState));
	if (pgstat_pending->cold == NULL)
		elog(ERROR, "out of memory allocating test pgstat pending cold state");
	MemSet(pgstat_pending->cold, 0, sizeof(PgBackendPgStatPendingColdState));
	pgstat_pending->cold->pending_bgwriter.buf_alloc = 25;
	pgstat_pending->cold->pending_checkpointer.num_requested = 26;
	pgstat_pending->io_stats_pending = true;
	pgstat_pending->slru_stats_pending = true;
	pgstat_pending->lock_stats_pending = true;
	pgstat_pending->backend_io_stats_pending = true;
	pgstat_pending->report_fixed = true;
	pgstat_pending->force_next_flush = true;
	pgstat_pending->force_snapshot_clear = true;
	pgstat_pending->is_initialized = true;
	pgstat_pending->is_shutdown = true;
	pgstat_pending->xact_commit = 27;
	pgstat_pending->xact_rollback = 28;
	pgstat_pending->block_read_time = 29;
	pgstat_pending->block_write_time = 30;
	pgstat_pending->active_time = 31;
	pgstat_pending->transaction_idle_time = 32;
	INSTR_TIME_SET_CURRENT(pgstat_pending->func_total_time);

	wait_state->spec.kind = PG_WAIT_KIND_EVENT_SET;
	wait_state->spec.wait_event_info = 0x01020304;
	wait_state->spec.wake_events = 33;
	wait_state->spec.socket = PGINVALID_SOCKET;
	wait_state->spec.timeout = 34;
	wait_state->local_wait_event_info = 0x05060708;
	pg_atomic_write_u32(&wait_state->waiting, 1);

	fake_backend.memory_manager.log_memory_context_in_progress = true;
	fake_backend.exit_state.retained_top_memory_context =
		(MemoryContext) &fake_backend;
	fake_backend.exit_state.proc_exit_done = true;

	utility->seq_scan_tables[0] = (HTAB *) &fake_backend;
	utility->seq_scan_tables[1] = (HTAB *) &fake_backend;
	utility->seq_scan_levels[0] = 1;
	utility->seq_scan_levels[1] = 2;
	utility->num_seq_scans = 2;
	RegisterResourceReleaseCallback(test_backend_runtime_resource_release_callback,
									NULL);
	utility->utility_cache_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test utility cache context",
							  ALLOCSET_SMALL_SIZES);
	hash_ctl.hcxt = utility->utility_cache_context;
	utility->injection_point_cache =
		hash_create("test injection point cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	ok = ok && MemoryContextGetParent(GetMemoryChunkContext(utility->injection_point_cache)) ==
		utility->utility_cache_context;
	utility->format_cache_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test format cache context",
							  ALLOCSET_SMALL_SIZES);
	utility->dch_cache[0] =
		MemoryContextAlloc(utility->format_cache_context, 8);
	utility->dch_cache[1] =
		MemoryContextAlloc(utility->format_cache_context, 8);
	utility->n_dch_cache = 2;
	utility->dch_counter = 11;
	utility->num_cache[0] =
		MemoryContextAlloc(utility->format_cache_context, 8);
	utility->num_cache[1] =
		MemoryContextAlloc(utility->format_cache_context, 8);
	utility->n_num_cache = 2;
	utility->num_counter = 12;
	ok = ok && GetMemoryChunkContext(utility->dch_cache[0]) ==
		utility->format_cache_context;
	ok = ok && GetMemoryChunkContext(utility->num_cache[0]) ==
		utility->format_cache_context;
	utility->libxml_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test libxml context",
							  ALLOCSET_SMALL_SIZES);
	utility->missing_attr_cache =
		hash_create("test missing attr cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	ok = ok && MemoryContextGetParent(GetMemoryChunkContext(utility->missing_attr_cache)) ==
		utility->utility_cache_context;

	saved_runtime = CurrentPgRuntime;
	saved_proc_exit_active = proc_exit_inprogress;
	saved_backend = CurrentPgBackend;
	PG_TRY();
	{
		PgSetCurrentRuntime(&fake_runtime);
		PgSetCurrentBackend(&fake_backend);
		notifyInterruptPending = true;
		proc_exit_inprogress = true;
		PgBackendResetClosedState(&fake_backend);
	}
	PG_CATCH();
	{
		proc_exit_inprogress = saved_proc_exit_active;
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentRuntime(saved_runtime);
		PG_RE_THROW();
	}
	PG_END_TRY();
	proc_exit_inprogress = saved_proc_exit_active;
	PgSetCurrentBackend(saved_backend);
	PgSetCurrentRuntime(saved_runtime);

	ok = ok && walsender->uploaded_manifest == NULL;
	ok = ok && walsender->uploaded_manifest_mcxt == NULL;
	ok = ok && walsender->output_message.data == NULL;
	ok = ok && walsender->reply_message.data == NULL;
	ok = ok && walsender->tmpbuf.data == NULL;
	ok = ok && walsender->replication_cmd_context == NULL;
	ok = ok && walsender->lag_tracker == NULL;
	ok = ok && replication->walreceiver_recv_file == -1;
	ok = ok && replication->walreceiver_reply_message.data == NULL;
	ok = ok && logical_replication->subxact_data.subxacts == NULL;
	ok = ok && logical_replication->subxact_data.nsubxacts == 0;
	ok = ok && logical_replication->subxact_data.nsubxacts_max == 0;
	ok = ok && logical_replication->subxact_data.subxact_last ==
		InvalidTransactionId;
	ok = ok && logical_replication->apply_context == NULL;
	ok = ok && logical_replication->apply_error_callback_arg.rel == NULL;
	ok = ok && logical_replication->apply_error_callback_arg.remote_attnum == -1;
	ok = ok && logical_replication->apply_error_callback_arg.remote_xid ==
		InvalidTransactionId;
	ok = ok && logical_replication->apply_error_callback_arg.finish_lsn ==
		InvalidXLogRecPtr;
	ok = ok && logical_replication->apply_error_callback_arg.origin_name ==
		NULL;
	ok = ok && logical_replication->my_parallel_shared == NULL;
	ok = ok && locks->fast_path_local_use_counts == NULL;
	ok = ok && !locks->fast_path_local_use_counts_owned;
	ok = ok && locks->strong_lock_in_progress == NULL;
	ok = ok && locks->awaited_lock == NULL;
	ok = ok && locks->awaited_owner == NULL;
	ok = ok && locks->deadlock_visited_procs == NULL;
	ok = ok && locks->deadlock_n_visited_procs == 0;
	ok = ok && locks->deadlock_topo_procs == NULL;
	ok = ok && locks->deadlock_before_constraints == NULL;
	ok = ok && locks->deadlock_after_constraints == NULL;
	ok = ok && locks->deadlock_wait_orders == NULL;
	ok = ok && locks->deadlock_n_wait_orders == 0;
	ok = ok && locks->deadlock_wait_order_procs == NULL;
	ok = ok && locks->deadlock_cur_constraints == NULL;
	ok = ok && locks->deadlock_n_cur_constraints == 0;
	ok = ok && locks->deadlock_max_cur_constraints == 0;
	ok = ok && locks->deadlock_possible_constraints == NULL;
	ok = ok && locks->deadlock_n_possible_constraints == 0;
	ok = ok && locks->deadlock_max_possible_constraints == 0;
	ok = ok && locks->deadlock_details == NULL;
	ok = ok && locks->deadlock_n_details == 0;
	ok = ok && locks->blocking_autovacuum_proc == NULL;
	ok = ok && !locks->deadlock_workspace_owned;
	ok = ok && locks->lwlock_stats_htab == NULL;
	ok = ok && locks->lwlock_stats_context == NULL;
	ok = ok && !locks->lwlock_stats_exit_registered;
	ok = ok && logical_replication->my_subscription == NULL;
	ok = ok && !logical_replication->my_subscription_valid;
	ok = ok && logical_replication->my_logical_rep_worker == NULL;
	ok = ok && logical_replication->on_commit_wakeup_workers_subids == NIL;
	ok = ok && logical_replication->copybuf == NULL;
	ok = ok && logical_replication->table_states_not_ready == NIL;
	ok = ok && logical_replication->seqinfos == NIL;
	ok = ok && logical_replication->slotsync_observed_primary_conninfo == NULL;
	ok = ok && logical_replication->slotsync_observed_primary_slotname == NULL;
	ok = ok && logical_replication->launcher_last_start_times_dsa == NULL;
	ok = ok && logical_replication->launcher_last_start_times == NULL;
	ok = ok && logical_replication->parallel_apply_txn_hash == NULL;
	ok = ok && logical_replication->parallel_apply_worker_pool == NIL;
	ok = ok && logical_replication->stream_apply_worker == NULL;
	ok = ok && logical_replication->parallel_apply_subxactlist == NIL;
	ok = ok && logical_replication->parallel_apply_message_context == NULL;
	ok = ok && xlog->open_log_file == -1;
	ok = ok && xlog->wal_debug_context == NULL;
	ok = ok && xlog->btree_xlog_op_context == NULL;
	ok = ok && xlog->gin_xlog_op_context == NULL;
	ok = ok && xlog->gist_xlog_op_context == NULL;
	ok = ok && xlog->spgist_xlog_op_context == NULL;
	ok = ok && maintenance_worker->arch_module_errdetail_string == NULL;
	ok = ok && maintenance_worker->archive_callbacks == NULL;
	ok = ok && maintenance_worker->archive_module_state == NULL;
	ok = ok && maintenance_worker->archive_context == NULL;
	ok = ok && maintenance_worker->loaded_archive_library == NULL;
	ok = ok && maintenance_worker->pgarch_files == NULL;
	ok = ok && maintenance_worker->bgwriter_context == NULL;
	ok = ok && maintenance_worker->walwriter_context == NULL;
	ok = ok && maintenance_worker->checkpointer_context == NULL;
	ok = ok && maintenance_worker->walsummarizer_context == NULL;
	ok = ok && autovacuum->autovac_mem_cxt == NULL;
	ok = ok && autovacuum->database_list_cxt == NULL;
	ok = ok && autovacuum->avl_dbase_array == NULL;
	ok = ok && autovacuum->my_worker_info == NULL;
	ok = ok && dlist_is_empty(&autovacuum->database_list);
	ok = ok && aio->my_backend == NULL;
	ok = ok && aio->my_io_worker_id == -1;
	ok = ok && aio->my_uring_context == NULL;
	ok = ok && activity->backend_status_table == NULL;
	ok = ok && activity->num_backends == 0;
	ok = ok && activity->backend_status_context == NULL;
	ok = ok && storage->vfd_cache == NULL;
	ok = ok && storage->size_vfd_cache == 0;
	ok = ok && storage->nfile == 0;
	ok = ok && !storage->temporary_files_allowed;
	ok = ok && storage->allocated_descs == NULL;
	ok = ok && storage->num_allocated_descs == 0;
	ok = ok && storage->max_allocated_descs == 0;
	/*
	 * num_external_fds is deliberately PRESERVED across the closed-state reset
	 * (owned by the ReserveExternalFD/ReleaseExternalFD protocol; the pending
	 * WaitEventSet fd releases later in the exit path drive it to 0 -- see
	 * PgBackendResetStorageClosedState).  The reset must not zero it out from
	 * under those releases, so verify it was PRESERVED at the fake backend's
	 * setup value (2), not zeroed.
	 */
	ok = ok && storage->num_external_fds == 2;
	ok = ok && storage->sync_pending_ops == NULL;
	ok = ok && storage->sync_pending_unlinks == NIL;
	ok = ok && storage->sync_pending_ops_context == NULL;
	ok = ok && storage->sync_cycle_counter == 0;
	ok = ok && storage->sync_checkpoint_cycle_counter == 0;
	ok = ok && !storage->sync_in_progress;
	ok = ok && storage->smgr_relation_hash == NULL;
	ok = ok && dlist_is_empty(&storage->smgr_unpinned_relations);
	ok = ok && storage->md_context == NULL;
	ok = ok && !timeout->all_timeouts_initialized;
	ok = ok && timeout->all_timeouts[DEADLOCK_TIMEOUT].index == 0;
	ok = ok && !timeout->all_timeouts[DEADLOCK_TIMEOUT].active;
	ok = ok && !timeout->all_timeouts[DEADLOCK_TIMEOUT].indicator;
	ok = ok && timeout->all_timeouts[DEADLOCK_TIMEOUT].target_backend == NULL;
	ok = ok && timeout->all_timeouts[DEADLOCK_TIMEOUT].target_execution == NULL;
	ok = ok && timeout->all_timeouts[DEADLOCK_TIMEOUT].timeout_handler == NULL;
	ok = ok && timeout->all_timeouts[DEADLOCK_TIMEOUT].start_time == 0;
	ok = ok && timeout->all_timeouts[DEADLOCK_TIMEOUT].fin_time == 0;
	ok = ok && timeout->all_timeouts[DEADLOCK_TIMEOUT].interval_in_ms == 0;
	ok = ok && timeout->num_active_timeouts == 0;
	ok = ok && timeout->active_timeouts[0] == NULL;
	ok = ok && !timeout->alarm_enabled;
	ok = ok && !timeout->signal_pending;
	ok = ok && timeout->signal_due_at == 0;
	ok = ok && timeout->firing_timeout_target == NULL;
	ok = ok && timeout->firing_timeout_execution == NULL;
	ok = ok && !timeout->signal_delivery;
	ok = ok && parallel->worker_number == -1;
	ok = ok && !parallel->message_pending;
	ok = ok && !parallel->initializing_worker;
	ok = ok && parallel->fixed_parallel_state == NULL;
	ok = ok && !parallel->context_list_initialized;
	ok = ok && parallel->leader_pid == 0;
	ok = ok && parallel->pq_mq_handle == NULL;
	ok = ok && !parallel->pq_mq_busy;
	ok = ok && parallel->pq_mq_parallel_leader_pid == 0;
	ok = ok && parallel->pq_mq_parallel_leader_proc_number ==
		INVALID_PROC_NUMBER;
	ok = ok && parallel->message_context == NULL;
	ok = ok && buffers->pin_count_wait_buf == NULL;
	ok = ok && buffers->nlocbuffer == 0;
	ok = ok && buffers->local_buffer_descriptors == NULL;
	ok = ok && buffers->local_buffer_block_pointers == NULL;
	ok = ok && buffers->local_ref_count == NULL;
	ok = ok && buffers->next_free_local_buf_id == 0;
	ok = ok && buffers->local_buf_hash == NULL;
	ok = ok && buffers->n_local_pinned_buffers == 0;
	ok = ok && buffers->local_buffer_cur_block == NULL;
	ok = ok && buffers->local_buffer_next_buf_in_block == 0;
	ok = ok && buffers->local_buffer_num_bufs_in_block == 0;
	ok = ok && buffers->local_buffer_total_bufs_allocated == 0;
	ok = ok && buffers->local_buffer_context == NULL;
	ok = ok && buffers->buffer_context == NULL;
	ok = ok && buffers->backend_writeback_context == NULL;
	ok = ok && buffers->private_ref_count_array_keys == NULL;
	ok = ok && buffers->private_ref_count_array == NULL;
	ok = ok && buffers->private_ref_count_hash == NULL;
	ok = ok && buffers->private_ref_count_overflowed == 0;
	ok = ok && buffers->private_ref_count_clock == 0;
	ok = ok && buffers->reserved_ref_count_slot == -1;
	ok = ok && buffers->private_ref_count_entry_last == -1;
	ok = ok && buffers->max_proportional_pins == 0;
	ok = ok && ipc->proc_signal_slot == NULL;
	ok = ok && ipc->shared_invalid_message_counter == 0;
	ok = ok && !ipc->catchup_interrupt_pending;
	ok = ok && ipc->shared_invalidation_messages == NULL;
	ok = ok && ipc->shared_invalidation_next_msg == 0;
	ok = ok && ipc->shared_invalidation_num_msgs == 0;
	ok = ok && !ipc->dsm_init_done;
	ok = ok && ipc->dsm_registry_dsa == NULL;
	ok = ok && ipc->dsm_registry_table == NULL;
	ok = ok && ipc->next_local_transaction_id == 0;
	ok = ok && ipc->latch_wait_set == NULL;
	ok = ok && transaction->cached_fetch_xid == InvalidTransactionId;
	ok = ok && transaction->cached_fetch_xid_status == 0;
	ok = ok && transaction->cached_commit_lsn == 0;
	ok = ok && transaction->two_phase_locked_gxact == NULL;
	ok = ok && !transaction->two_phase_exit_registered;
	ok = ok && FullTransactionIdEquals(transaction->two_phase_cached_fxid,
									   InvalidFullTransactionId);
	ok = ok && transaction->two_phase_cached_gxact == NULL;
	ok = ok && transaction->slru_error_cause == 0;
	ok = ok && transaction->slru_errno_value == 0;
	ok = ok && dclist_is_empty(&transaction->multixact_cache);
	ok = ok && !transaction->multixact_cache_initialized;
	ok = ok && transaction->multixact_context == NULL;
	ok = ok && transaction->multixact_debug_string == NULL;
	ok = ok && transaction->procarray_cached_xid_not_in_progress ==
		InvalidTransactionId;
	ok = ok && transaction->compute_xid_horizons_result_last_xmin ==
		InvalidTransactionId;
	ok = ok && transaction->xidcache_by_recent_xmin == 0;
	ok = ok && transaction->xidcache_by_known_xact == 0;
	ok = ok && transaction->xidcache_by_my_xact == 0;
	ok = ok && transaction->xidcache_by_latest_xid == 0;
	ok = ok && transaction->xidcache_by_main_xid == 0;
	ok = ok && transaction->xidcache_by_child_xid == 0;
	ok = ok && transaction->xidcache_by_known_assigned == 0;
	ok = ok && transaction->xidcache_no_overflow == 0;
	ok = ok && transaction->xidcache_slow_answer == 0;
	ok = ok && !recovery->startup_got_sighup;
	ok = ok && !recovery->startup_shutdown_requested;
	ok = ok && !recovery->startup_promote_signaled;
	ok = ok && !recovery->startup_in_restore_command;
	ok = ok && recovery->startup_progress_phase_start_time == 0;
	ok = ok && !recovery->startup_progress_timer_expired;
	ok = ok && !recovery->local_hot_standby_active;
	ok = ok && !recovery->local_promote_is_triggered;
	ok = ok && recovery->recovery_lock_hash == NULL;
	ok = ok && recovery->recovery_lock_xid_hash == NULL;
	ok = ok && !recovery->got_standby_deadlock_timeout;
	ok = ok && !recovery->got_standby_delay_timeout;
	ok = ok && !recovery->got_standby_lock_timeout;
	ok = ok && recovery->standby_wait_us == PG_BACKEND_STANDBY_INITIAL_WAIT_US;
	ok = ok && repack->decoding_worker == NULL;
	ok = ok && !repack->message_pending;
	ok = ok && !repack->am_repack_worker;
	ok = ok && repack->current_segment == 0;
	ok = ok && repack->worker_dsm_segment == NULL;
	ok = ok && !OidIsValid(repack->repacked_rel_locator.relNumber);
	ok = ok && !OidIsValid(repack->repacked_rel_toast_locator.relNumber);
	ok = ok && repack->message_context == NULL;
	ok = ok && pgstat_pending->local == NULL;
	ok = ok && pgstat_pending->entry_ref_hash == NULL;
	ok = ok && pgstat_pending->shared_ref_age == 0;
	ok = ok && pgstat_pending->shared_ref_context == NULL;
	ok = ok && pgstat_pending->entry_ref_hash_context == NULL;
	ok = ok && pgstat_pending->pending_context == NULL;
	ok = ok && dlist_is_empty(&pgstat_pending->pending);
	ok = ok && pgstat_pending->cold == NULL;
	ok = ok && !pgstat_pending->io_stats_pending;
	ok = ok && !pgstat_pending->slru_stats_pending;
	ok = ok && !pgstat_pending->lock_stats_pending;
	ok = ok && !pgstat_pending->backend_io_stats_pending;
	ok = ok && !pgstat_pending->report_fixed;
	ok = ok && !pgstat_pending->force_next_flush;
	ok = ok && !pgstat_pending->force_snapshot_clear;
	ok = ok && !pgstat_pending->is_initialized;
	ok = ok && !pgstat_pending->is_shutdown;
	ok = ok && pgstat_pending->xact_commit == 0;
	ok = ok && pgstat_pending->xact_rollback == 0;
	ok = ok && pgstat_pending->block_read_time == 0;
	ok = ok && pgstat_pending->block_write_time == 0;
	ok = ok && pgstat_pending->active_time == 0;
	ok = ok && pgstat_pending->transaction_idle_time == 0;
	ok = ok && wait_state->spec.kind == PG_WAIT_KIND_NONE;
	ok = ok && wait_state->spec.wait_event_info == 0;
	ok = ok && wait_state->spec.wake_events == 0;
	ok = ok && wait_state->spec.socket == 0;
	ok = ok && wait_state->spec.timeout == 0;
	ok = ok && wait_state->local_wait_event_info == 0;
	ok = ok && wait_state->wait_event_info_ptr ==
		&wait_state->local_wait_event_info;
	ok = ok && pg_atomic_read_u32(&wait_state->waiting) == 0;
	ok = ok && !fake_backend.memory_manager.log_memory_context_in_progress;
	ok = ok && !utility->notify_interrupt_pending;
	ok = ok && utility->seq_scan_tables[0] == NULL;
	ok = ok && utility->seq_scan_tables[1] == NULL;
	ok = ok && utility->seq_scan_levels[0] == 0;
	ok = ok && utility->seq_scan_levels[1] == 0;
	ok = ok && utility->num_seq_scans == 0;
	ok = ok && utility->resource_release_callbacks == NULL;
	ok = ok && utility->injection_point_cache == NULL;
	ok = ok && utility->utility_cache_context == NULL;
	ok = ok && utility->dch_cache[0] == NULL;
	ok = ok && utility->dch_cache[1] == NULL;
	ok = ok && utility->n_dch_cache == 0;
	ok = ok && utility->dch_counter == 0;
	ok = ok && utility->num_cache[0] == NULL;
	ok = ok && utility->num_cache[1] == NULL;
	ok = ok && utility->n_num_cache == 0;
	ok = ok && utility->num_counter == 0;
	ok = ok && utility->format_cache_context == NULL;
	ok = ok && utility->libxml_context == NULL;
	ok = ok && utility->missing_attr_cache == NULL;
	ok = ok && fake_backend.exit_state.retained_top_memory_context ==
		(MemoryContext) &fake_backend;
	ok = ok && fake_backend.exit_state.proc_exit_done;

	if (!ok)
		elog(ERROR, "closed backend runtime state was not reset: retained_top=%p expected_top=%p proc_exit_done=%d",
			 fake_backend.exit_state.retained_top_memory_context,
			 &fake_backend,
			 fake_backend.exit_state.proc_exit_done);

	PG_RETURN_BOOL(true);
}
