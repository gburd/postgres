/*-------------------------------------------------------------------------
 *
 * backend_runtime_teardown.c
 *	  Closed-backend/session/execution runtime teardown helpers.
 *
 * This file owns the ordered closed-state reset paths for runtime objects.
 * Keep root object construction, current-pointer installation, and early
 * fallback adoption in backend_runtime.c; move semantic destroy/reset logic
 * here or to a more specific subsystem-owned runtime file.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * src/backend/utils/init/backend_runtime_teardown.c
 *
 *-------------------------------------------------------------------------
 */
#define BACKEND_RUNTIME_NO_INLINE_BUCKET_ACCESSORS
#include "postgres.h"

#include "access/gin.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "archive/archive_module.h"
#include "commands/async.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/prepare.h"
#include "commands/trigger.h"
#include "lib/dshash.h"
#include "postmaster/pgarch.h"
#include "regex/regex.h"
#include "replication/reorderbuffer.h"
#include "replication/logical.h"
#include "replication/slotsync.h"
#include "replication/walreceiver.h"
#include "storage/buffile.h"
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/shm_mq.h"
#include "storage/waiteventset.h"
#include "tsearch/ts_cache.h"
#include "utils/backend_runtime.h"
#include "backend_runtime_internal.h"
#include "utils/dsa.h"
#include "utils/funccache.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/plancache.h"
#include "utils/resowner.h"
#include "utils/typcache.h"

static void
PgBackendResetStringInfo(StringInfoData *buf)
{
	if (buf == NULL)
		return;

	if (buf->data != NULL)
		pfree(buf->data);
	MemSet(buf, 0, sizeof(*buf));
}

static void
PgBackendResetExprInterpClosedState(PgBackendExprInterpState *expr_interp)
{
	Assert(expr_interp != NULL);

	if (expr_interp->reverse_dispatch_table != NULL)
		pfree(expr_interp->reverse_dispatch_table);
	MemSet(expr_interp, 0, sizeof(*expr_interp));
}

static void
PgBackendResetLockClosedState(PgBackendLockState *locks)
{
	Assert(locks != NULL);

	if (locks->held_lwlocks_array != NULL &&
		locks->held_lwlocks_array != locks->held_lwlocks_inline)
		pfree(locks->held_lwlocks_array);

	if (locks->fast_path_local_use_counts_owned &&
		locks->fast_path_local_use_counts != NULL)
		pfree(locks->fast_path_local_use_counts);

	if (locks->deadlock_workspace_owned)
	{
		if (locks->deadlock_visited_procs != NULL)
			pfree(locks->deadlock_visited_procs);
		if (locks->deadlock_before_constraints != NULL)
			pfree(locks->deadlock_before_constraints);
		if (locks->deadlock_after_constraints != NULL)
			pfree(locks->deadlock_after_constraints);
		if (locks->deadlock_wait_orders != NULL)
			pfree(locks->deadlock_wait_orders);
		if (locks->deadlock_wait_order_procs != NULL)
			pfree(locks->deadlock_wait_order_procs);
		if (locks->deadlock_cur_constraints != NULL)
			pfree(locks->deadlock_cur_constraints);
		if (locks->deadlock_possible_constraints != NULL)
			pfree(locks->deadlock_possible_constraints);
		if (locks->deadlock_details != NULL)
			pfree(locks->deadlock_details);
	}

	/*
	 * LWLOCK_STATS registers print_lwlock_stats() as a shmem-exit callback.
	 * proc_exit() drains that callback stack before closed-backend reset; this
	 * only reclaims any retained per-backend stats hash storage afterwards.
	 */
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(locks->lwlock_stats_context);
	locks->lwlock_stats_htab = NULL;
	locks->lwlock_stats_exit_registered = false;

	PgBackendInitializeLockState(locks);
}

static void
PgBackendResetExtensionModuleClosedState(PgBackendExtensionModuleState *extension_modules)
{
	Assert(extension_modules != NULL);

	foreach_ptr(PgBackendExtensionPrivateState, private_state,
				extension_modules->private_states)
	{
		if (private_state->cleanup != NULL &&
			private_state->state != NULL)
			private_state->cleanup(private_state->state);
	}

	foreach_ptr(PgBackendExtensionPrivateState, private_state,
				extension_modules->private_states)
	{
		if (private_state->state != NULL)
			pfree(private_state->state);
	}
	list_free_deep(extension_modules->private_states);

	PgBackendInitializeExtensionModuleState(extension_modules);
}

static void
PgBackendResetParallelClosedState(PgBackendParallelState *parallel)
{
	Assert(parallel != NULL);

	if (parallel->context_list_initialized)
		Assert(dlist_is_empty(&parallel->context_list));

	if (parallel->pq_mq_handle != NULL)
	{
		shm_mq_detach((shm_mq_handle *) parallel->pq_mq_handle);
		parallel->pq_mq_handle = NULL;
	}

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(parallel->message_context);

	PgBackendInitializeParallelState(parallel);
}

static void
PgBackendResetPgStatPendingClosedState(PgBackendPgStatPendingState *pgstat_pending)
{
	Assert(pgstat_pending != NULL);
	Assert(pgstat_pending->entry_ref_hash == NULL);
	Assert(dlist_is_empty(&pgstat_pending->pending));
	if (pgstat_pending->local != NULL)
	{
		Assert(pgstat_pending->local->shared_hash == NULL);
		Assert(pgstat_pending->local->dsa == NULL);
	}

	/*
	 * Normal pgstat shutdown owns flushing, shared-entry release, and DSA
	 * detach.  Closed-backend reset only reclaims retained local contexts and
	 * restores constructor defaults for reuse.
	 */
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(pgstat_pending->fixed_snapshot_context);
	if (pgstat_pending->local != NULL &&
		pgstat_pending->local->snapshot != NULL)
	{
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(pgstat_pending->local->snapshot->context);
		pfree(pgstat_pending->local->snapshot);
		pgstat_pending->local->snapshot = NULL;
	}
	if (pgstat_pending->local != NULL)
		pfree(pgstat_pending->local);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(pgstat_pending->shared_ref_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(pgstat_pending->entry_ref_hash_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(pgstat_pending->pending_context);
	if (pgstat_pending->cold != NULL)
		free(pgstat_pending->cold);

	PgBackendInitializePgStatPendingState(pgstat_pending);
}

static void
PgBackendResetWaitClosedState(PgBackendWaitState *wait_state)
{
	Assert(wait_state != NULL);

	PgBackendInitializeWaitState(wait_state);
}

static void
PgBackendResetBufferClosedState(PgBackendBufferState *buffers)
{
	Assert(buffers != NULL);
	Assert(buffers->n_local_pinned_buffers == 0);
	Assert(buffers->private_ref_count_overflowed == 0);

	/*
	 * During process-mode proc_exit(), normal buffer callbacks have already
	 * checked semantic cleanup, and some context-owned buffer helper storage
	 * can already be invalid.  Process exit will reclaim it.  Threaded client
	 * backend proc_exit() reuses this reset path without process teardown, so
	 * it must reclaim local-buffer arrays allocated with calloc(); context-
	 * owned buffer helper storage remains under the retained TopMemoryContext
	 * and is reclaimed when backend_thread_finish() deletes that root.
	 */
	if (PgBackendExitInProgress())
	{
		if (PgRuntimeIsThreadBacked(CurrentPgRuntime) &&
			CurrentPgBackend != NULL &&
			CurrentPgBackend->backend_type == B_BACKEND)
		{
			if (buffers->local_buffer_descriptors != NULL)
				free(buffers->local_buffer_descriptors);
			if (buffers->local_buffer_block_pointers != NULL)
				free(buffers->local_buffer_block_pointers);
			if (buffers->local_ref_count != NULL)
				free(buffers->local_ref_count);
		}

		PgBackendInitializeBufferState(buffers);
		return;
	}

	if (buffers->local_buffer_descriptors != NULL)
		free(buffers->local_buffer_descriptors);
	if (buffers->local_buffer_block_pointers != NULL)
		free(buffers->local_buffer_block_pointers);
	if (buffers->local_ref_count != NULL)
		free(buffers->local_ref_count);

	PG_RUNTIME_DESTROY_HASH(buffers->local_buf_hash);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(buffers->local_buffer_context);

	if (buffers->backend_writeback_context != NULL)
		pfree(buffers->backend_writeback_context);

	if (buffers->private_ref_count_array_keys != NULL)
		pfree(buffers->private_ref_count_array_keys);
	if (buffers->private_ref_count_array != NULL)
		pfree(buffers->private_ref_count_array);
	PG_RUNTIME_DESTROY_HASH(buffers->private_ref_count_hash);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(buffers->buffer_context);

	PgBackendInitializeBufferState(buffers);
}

static void
PgBackendResetIPCClosedState(PgBackendIPCState *ipc)
{
	Assert(ipc != NULL);

	if (ipc->dsm_registry_table != NULL)
		dshash_detach((dshash_table *) ipc->dsm_registry_table);
	if (ipc->dsm_registry_dsa != NULL)
		dsa_detach((dsa_area *) ipc->dsm_registry_dsa);
	if (ipc->latch_wait_set != NULL)
		FreeWaitEventSet(ipc->latch_wait_set);

	PgBackendInitializeIPCState(ipc);
}

static void
PgBackendResetTransactionClosedState(PgBackendTransactionState *transaction)
{
	Assert(transaction != NULL);
	Assert(!transaction->multixact_cache_initialized ||
		   dclist_is_empty(&transaction->multixact_cache));

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(transaction->multixact_context);
	if (transaction->multixact_debug_string != NULL)
		pfree(transaction->multixact_debug_string);

	PgBackendInitializeTransactionState(transaction);
}

static void
PgBackendResetRecoveryClosedState(PgBackendRecoveryState *recovery)
{
	Assert(recovery != NULL);

	PG_RUNTIME_DESTROY_HASH(recovery->recovery_lock_hash);
	PG_RUNTIME_DESTROY_HASH(recovery->recovery_lock_xid_hash);

	PgBackendInitializeRecoveryState(recovery);
}

static void
PgBackendResetRepackClosedState(PgBackendRepackState *repack)
{
	Assert(repack != NULL);
	Assert(repack->decoding_worker == NULL);

	if (repack->worker_dsm_segment != NULL)
		dsm_detach(repack->worker_dsm_segment);

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(repack->message_context);

	PgBackendInitializeRepackState(repack);
}

static void
PgExecutionResetErrorClosedState(PgExecutionErrorState *error)
{
	PgExecutionInitializeErrorState(error);
}

static void
PgExecutionResetResourceOwnersClosedState(PgExecutionResourceOwnerState
										  *resource_owners)
{
	bool		have_live_owner;
	MemoryContext resource_owner_context;

	Assert(resource_owners != NULL);

	have_live_owner = resource_owners->current_owner != NULL ||
		resource_owners->cur_transaction_owner != NULL ||
		resource_owners->top_transaction_owner != NULL;
	resource_owner_context = resource_owners->resource_owner_context;

	resource_owners->current_owner = NULL;
	resource_owners->cur_transaction_owner = NULL;
	resource_owners->top_transaction_owner = NULL;

	if (!have_live_owner && resource_owner_context != NULL)
	{
		MemoryContextDelete(resource_owner_context);
		resource_owners->resource_owner_context = NULL;
	}
}

static void
PgExecutionResetSPIClosedState(PgExecutionSPIState *spi)
{
	PgExecutionInitializeSPIState(spi);
}

static void
PgExecutionResetSnapshotDataArrays(SnapshotData *snapshot)
{
	Assert(snapshot != NULL);

	if (snapshot->xip != NULL)
		free(snapshot->xip);
	if (snapshot->subxip != NULL)
		free(snapshot->subxip);
	snapshot->xip = NULL;
	snapshot->subxip = NULL;
}

static void
PgExecutionResetSnapshotClosedState(PgExecutionSnapshotState *snapshot)
{
	Assert(snapshot != NULL);

	/*
	 * GetSnapshotData() mallocs xip/subxip arrays and relies on process exit
	 * to reclaim them because the historical SnapshotData objects are static.
	 * Threaded logical backends embed those SnapshotData objects in
	 * PgExecution, so connection churn must release the arrays explicitly.
	 */
	PgExecutionResetSnapshotDataArrays(&snapshot->current_snapshot_data);
	PgExecutionResetSnapshotDataArrays(&snapshot->secondary_snapshot_data);
	PgExecutionResetSnapshotDataArrays(&snapshot->catalog_snapshot_data);

	PgExecutionInitializeSnapshotState(snapshot);
}

static void
PgExecutionResetPortalClosedState(PgExecutionPortalState *portal)
{
	Assert(portal != NULL);

	MemSet(portal, 0, sizeof(*portal));
}

static void
PgExecutionResetReplicationScratchClosedState(PgExecutionReplicationScratchState
											  *replication_scratch)
{
	Assert(replication_scratch != NULL);

	EventTriggerResetQueryStateStack(&replication_scratch->event_trigger_query_state);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(replication_scratch->event_trigger_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(replication_scratch->apply_message_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(replication_scratch->logical_streaming_context);
	PgExecutionInitializeReplicationScratchState(replication_scratch);
}

static void
PgBackendResetWalSenderClosedState(PgBackendWalSenderState *walsender)
{
	if (walsender == NULL)
		return;

	/*
	 * The logical decoding context ("Logical decoding context") is a child of
	 * replication_cmd_context and owns the logical xlogreader.  FreeDecodingContext
	 * runs the output-plugin shutdown callback and MemoryContextDelete()s that
	 * child, so it must run before we delete the parent replication_cmd_context
	 * below.  Only the interrupted-mid-stream case reaches here with the cell
	 * still set; the normal logical exit path already freed it and NULLed the
	 * cell (StartLogicalReplication).
	 */
	if (walsender->logical_decoding_ctx != NULL)
	{
		FreeDecodingContext(walsender->logical_decoding_ctx);
		walsender->logical_decoding_ctx = NULL;
		walsender->xlogreader = NULL;
	}

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(walsender->uploaded_manifest_mcxt);
	walsender->uploaded_manifest = NULL;

	/*
	 * output_message/reply_message/tmpbuf (their .data buffers) and the physical
	 * xlogreader are all allocated inside replication_cmd_context: the StringInfos
	 * are initStringInfo'd after switching into it in exec_replication_command,
	 * and the physical reader is XLogReaderAllocate'd there in StartReplication.
	 * exec_replication_command MemoryContextReset()s that context at the end of
	 * every command, so by backend exit those chunks are already freed and the
	 * persistent cells dangle.  Deleting the context here is the sole owner-level
	 * free; individually pfree'ing the chunks (PgBackendResetStringInfo /
	 * XLogReaderFree) would double-free freed memory (heap corruption at exit).
	 * So delete the context first, then just clear the cells without freeing.
	 */
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(walsender->replication_cmd_context);

	walsender->xlogreader = NULL;
	MemSet(&walsender->output_message, 0, sizeof(walsender->output_message));
	MemSet(&walsender->reply_message, 0, sizeof(walsender->reply_message));
	MemSet(&walsender->tmpbuf, 0, sizeof(walsender->tmpbuf));

	if (walsender->lag_tracker != NULL)
	{
		pfree(walsender->lag_tracker);
		walsender->lag_tracker = NULL;
	}
}

static void
PgBackendResetReplicationClosedState(PgBackendReplicationState *replication)
{
	if (replication == NULL)
		return;

	if (replication->walreceiver_conn != NULL)
	{
		walrcv_disconnect(replication->walreceiver_conn);
		replication->walreceiver_conn = NULL;
	}

	if (replication->walreceiver_recv_file >= 0)
	{
		(void) close(replication->walreceiver_recv_file);
		replication->walreceiver_recv_file = -1;
	}

	PgBackendResetStringInfo(&replication->walreceiver_reply_message);
}

static void
PgBackendResetLogicalReplicationClosedState(PgBackendLogicalReplicationState *logical_replication)
{
	if (logical_replication == NULL)
		return;

	if (logical_replication->logrep_worker_walrcv_conn != NULL)
	{
		walrcv_disconnect(logical_replication->logrep_worker_walrcv_conn);
		logical_replication->logrep_worker_walrcv_conn = NULL;
	}

	if (logical_replication->stream_fd != NULL)
	{
		BufFileClose(logical_replication->stream_fd);
		logical_replication->stream_fd = NULL;
	}

	if (logical_replication->copybuf != NULL)
	{
		PgBackendResetStringInfo(logical_replication->copybuf);
		pfree(logical_replication->copybuf);
		logical_replication->copybuf = NULL;
	}

	if (logical_replication->subxact_data.subxacts != NULL)
	{
		pfree(logical_replication->subxact_data.subxacts);
		logical_replication->subxact_data.subxacts = NULL;
	}
	logical_replication->subxact_data.nsubxacts = 0;
	logical_replication->subxact_data.nsubxacts_max = 0;
	logical_replication->subxact_data.subxact_last = InvalidTransactionId;

	if (logical_replication->apply_error_callback_arg.origin_name != NULL)
	{
		pfree(logical_replication->apply_error_callback_arg.origin_name);
		logical_replication->apply_error_callback_arg.origin_name = NULL;
	}
	logical_replication->apply_error_callback_arg.rel = NULL;
	logical_replication->apply_error_callback_arg.remote_attnum = -1;
	logical_replication->apply_error_callback_arg.remote_xid = InvalidTransactionId;
	logical_replication->apply_error_callback_arg.finish_lsn = InvalidXLogRecPtr;

	if (logical_replication->slotsync_observed_primary_conninfo != NULL)
	{
		pfree(logical_replication->slotsync_observed_primary_conninfo);
		logical_replication->slotsync_observed_primary_conninfo = NULL;
	}
	if (logical_replication->slotsync_observed_primary_slotname != NULL)
	{
		pfree(logical_replication->slotsync_observed_primary_slotname);
		logical_replication->slotsync_observed_primary_slotname = NULL;
	}

	PG_RUNTIME_DESTROY_HASH(logical_replication->parallel_apply_txn_hash);

	PG_RUNTIME_LIST_FREE(logical_replication->on_commit_wakeup_workers_subids);
	PG_RUNTIME_LIST_FREE(logical_replication->table_states_not_ready);
	PG_RUNTIME_LIST_FREE(logical_replication->seqinfos);
	PG_RUNTIME_LIST_FREE(logical_replication->parallel_apply_worker_pool);
	PG_RUNTIME_LIST_FREE(logical_replication->parallel_apply_subxactlist);

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(logical_replication->apply_context);

	dlist_init(&logical_replication->lsn_mapping);
	logical_replication->my_parallel_shared = NULL;
	logical_replication->my_subscription = NULL;
	logical_replication->my_subscription_valid = false;
	logical_replication->my_logical_rep_worker = NULL;
	logical_replication->on_commit_wakeup_workers_subids = NIL;
	logical_replication->table_states_not_ready = NIL;
	logical_replication->seqinfos = NIL;
	if (logical_replication->launcher_last_start_times != NULL)
	{
		dshash_detach(logical_replication->launcher_last_start_times);
		logical_replication->launcher_last_start_times = NULL;
	}
	if (logical_replication->launcher_last_start_times_dsa != NULL)
	{
		dsa_detach(logical_replication->launcher_last_start_times_dsa);
		logical_replication->launcher_last_start_times_dsa = NULL;
	}
	logical_replication->parallel_apply_worker_pool = NIL;
	logical_replication->stream_apply_worker = NULL;
	logical_replication->parallel_apply_subxactlist = NIL;
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(
		logical_replication->parallel_apply_message_context);
}

static void
PgBackendResetXLogClosedState(PgBackendXLogState *xlog)
{
	if (xlog == NULL)
		return;

	if (xlog->open_log_file >= 0)
	{
		(void) close(xlog->open_log_file);
		xlog->open_log_file = -1;
	}

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(xlog->wal_debug_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(xlog->btree_xlog_op_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(xlog->gin_xlog_op_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(xlog->gist_xlog_op_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(xlog->spgist_xlog_op_context);
}

static void
PgBackendResetMaintenanceWorkerClosedState(PgBackendMaintenanceWorkerState *maintenance_worker)
{
	if (maintenance_worker == NULL)
		return;

	if (maintenance_worker->arch_module_errdetail_string != NULL)
	{
		pfree(maintenance_worker->arch_module_errdetail_string);
		maintenance_worker->arch_module_errdetail_string = NULL;
	}
	if (maintenance_worker->archive_module_state != NULL)
	{
		pfree(maintenance_worker->archive_module_state);
		maintenance_worker->archive_module_state = NULL;
	}
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(maintenance_worker->archive_context);
	if (maintenance_worker->loaded_archive_library != NULL)
	{
		pfree(maintenance_worker->loaded_archive_library);
		maintenance_worker->loaded_archive_library = NULL;
	}
	PgArchResetFilesState(&maintenance_worker->pgarch_files);

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(maintenance_worker->bgwriter_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(maintenance_worker->walwriter_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(maintenance_worker->checkpointer_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(maintenance_worker->walsummarizer_context);

	maintenance_worker->archive_callbacks = NULL;
}

static void
PgBackendResetAutovacuumClosedState(PgBackendAutovacuumState *autovacuum)
{
	if (autovacuum == NULL)
		return;

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(autovacuum->autovac_mem_cxt);
	autovacuum->database_list_cxt = NULL;
	autovacuum->avl_dbase_array = NULL;
	autovacuum->my_worker_info = NULL;
	dlist_init(&autovacuum->database_list);
}

static void
PgBackendResetAioClosedState(PgBackendAioState *aio)
{
	if (aio == NULL)
		return;

	aio->my_backend = NULL;
	aio->my_io_worker_id = -1;
	aio->my_uring_context = NULL;
}

static void
PgBackendResetMemoryManagerClosedState(PgBackendMemoryManagerState *memory_manager)
{
	if (memory_manager == NULL)
		return;

	/*
	 * The AllocSet freelist is tied to memory-context ownership, not this
	 * bookkeeping bucket.  Process exit lets the operating system reclaim it.
	 * Threaded logical exit, however, has already run session/connection
	 * cleanup before reaching the backend memory-manager bucket; those earlier
	 * MemoryContextDelete() calls can leave deleted keeper blocks on the
	 * backend-local freelists.  Free them before clearing the bookkeeping, or
	 * connection churn loses the only references and retains heap forever.
	 */
	if (PgBackendExitInProgress() &&
		CurrentPgRuntime != NULL &&
		PgRuntimeIsThreadBacked(CurrentPgRuntime))
		AllocSetFreeContextFreelists(memory_manager->context_freelists,
									 PG_BACKEND_ALLOCSET_NUM_FREELISTS);

	MemSet(memory_manager->context_freelists, 0,
		   sizeof(memory_manager->context_freelists));
	memory_manager->log_memory_context_in_progress = false;
}

static void
PgBackendResetUtilityClosedState(PgBackendUtilityState *utility)
{
	int			i;

	if (utility == NULL)
		return;

	utility->notify_interrupt_pending = false;

	if (utility->async_global_channel_table != NULL)
		dshash_detach(utility->async_global_channel_table);
	if (utility->async_global_channel_dsa != NULL)
		dsa_detach(utility->async_global_channel_dsa);
	utility->async_global_channel_table = NULL;
	utility->async_global_channel_dsa = NULL;

	ResetExtensionSiblingCache();

	PG_RUNTIME_DESTROY_HASH(utility->injection_point_cache);

	for (i = 0; i < utility->num_seq_scans; i++)
	{
		utility->seq_scan_tables[i] = NULL;
		utility->seq_scan_levels[i] = 0;
	}
	utility->num_seq_scans = 0;

	ResetResourceReleaseCallbacks();

	for (i = 0; i < utility->n_dch_cache; i++)
	{
		pfree(utility->dch_cache[i]);
		utility->dch_cache[i] = NULL;
	}
	utility->n_dch_cache = 0;
	utility->dch_counter = 0;

	for (i = 0; i < utility->n_num_cache; i++)
	{
		pfree(utility->num_cache[i]);
		utility->num_cache[i] = NULL;
	}
	utility->n_num_cache = 0;
	utility->num_counter = 0;

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(utility->format_cache_context);

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(utility->libxml_context);

	PG_RUNTIME_DESTROY_HASH(utility->missing_attr_cache);

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(utility->utility_cache_context);
}

void
PgBackendResetClosedState(PgBackend *backend)
{
	if (backend == NULL)
		return;

	PgBackendUnregisterThreadedBackend(backend);

#define PG_BACKEND_BUCKET(field, init, adopt, reset) \
	do { reset; } while (0);
#include "backend_runtime_backend_buckets.def"
#undef PG_BACKEND_BUCKET
}

static void
PgSessionResetTcopClosedState(PgSession *session)
{
	Assert(session != NULL);

	if (session->tcop.unnamed_stmt_psrc != NULL)
	{
		CachedPlanSource *psrc = session->tcop.unnamed_stmt_psrc;

		session->tcop.unnamed_stmt_psrc = NULL;
		DropCachedPlan(psrc);
	}
	if (session->tcop.row_description_context != NULL)
	{
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->tcop.row_description_context);
		MemSet(&session->tcop.row_description_buf, 0,
			   sizeof(session->tcop.row_description_buf));
	}
}

static void
PgSessionResetPreparedStatementClosedState(PgSession *session)
{
	PgSession  *saved_session;

	Assert(session != NULL);

	if (session->prepared_statement.prepared_queries != NULL)
	{
		saved_session = CurrentPgSession;
		PgSetCurrentSession(session);
		PG_TRY();
		{
			DropAllPreparedStatements();
			PgSetCurrentSession(saved_session);
		}
		PG_CATCH();
		{
			PgSetCurrentSession(saved_session);
			PG_RE_THROW();
		}
		PG_END_TRY();
		hash_destroy(session->prepared_statement.prepared_queries);
		session->prepared_statement.prepared_queries = NULL;
	}
}

static void
PgSessionResetXactCallbackClosedState(PgSession *session)
{
	PgSession  *saved_session;

	Assert(session != NULL);

	saved_session = CurrentPgSession;
	PgSetCurrentSession(session);
	ResetXactCallbackState();
	PgSetCurrentSession(saved_session);

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->xact_callbacks.xact_callback_context);
}

static void
PgSessionResetBackupClosedState(PgSession *session)
{
	PgSession  *saved_session;

	Assert(session != NULL);

	if (session->backup.session_backup_state != SESSION_BACKUP_NONE)
	{
		saved_session = CurrentPgSession;
		PgSetCurrentSession(session);
		PG_TRY();
		{
			do_pg_abort_backup(0, BoolGetDatum(false));
			PgSetCurrentSession(saved_session);
		}
		PG_CATCH();
		{
			PgSetCurrentSession(saved_session);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->backup.backup_context);
	session->backup.backup_state = NULL;
	session->backup.tablespace_map = NULL;
	session->backup.session_backup_state = SESSION_BACKUP_NONE;
}

static void
PgSessionResetAsyncClosedState(PgSession *session)
{
	Assert(session != NULL);

	PG_RUNTIME_DESTROY_HASH(session->async.local_channel_table);
	session->async.registered_listener = false;
}

static void
PgSessionResetFunctionManagerClosedState(PgSession *session)
{
	Assert(session != NULL);

	PG_RUNTIME_DESTROY_HASH(session->function_manager.c_func_hash);
	if (session->function_manager.cached_function_hash != NULL)
	{
		DestroyCachedFunctionHash(session->function_manager.cached_function_hash);
		session->function_manager.cached_function_hash = NULL;
	}
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(
		session->function_manager.function_manager_context);
}

static void
PgSessionResetExtensionModuleClosedState(PgSession *session)
{
	PgSession  *saved_session;

	Assert(session != NULL);

	saved_session = CurrentPgSession;
	PgSetCurrentSession(session);
	PG_TRY();
	{
		foreach_ptr(PgSessionResetCallbackItem, item,
					session->extension_modules.reset_callbacks)
			item->callback(item->arg);
		foreach_ptr(PgSessionExtensionPrivateState, private_state,
					session->extension_modules.private_states)
		{
			if (private_state->cleanup != NULL &&
				private_state->state != NULL)
				private_state->cleanup(private_state->state);
		}
		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	foreach_ptr(PgSessionExtensionPrivateState, private_state,
				session->extension_modules.private_states)
	{
		if (private_state->state != NULL)
			pfree(private_state->state);
	}
	list_free_deep(session->extension_modules.private_states);
	list_free_deep(session->extension_modules.reset_callbacks);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(
		session->extension_modules.plpython_memory_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(
		session->extension_modules.plperl_memory_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(
		session->extension_modules.pltcl_memory_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(
		session->extension_modules.plsample_memory_context);
	PgSessionInitializeExtensionModuleState(&session->extension_modules);
}

static void
PgSessionResetPgStatClosedState(PgSession *session)
{
	Assert(session != NULL);

	PgSessionInitializePgStatState(&session->pgstat);
}

static void
PgSessionResetEncodingClosedState(PgSession *session)
{
	Assert(session != NULL);

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->encoding.encoding_cache_context);

	PgSessionInitializeEncodingState(&session->encoding);
}

static void
PgSessionResetInvalidationCallbackClosedState(PgSession *session)
{
	Assert(session != NULL);

	PgSessionInitializeInvalidationCallbackState(&session->invalidation_callbacks);
}

static void
PgSessionResetRIGlobalsClosedState(PgSession *session)
{
	Assert(session != NULL);

	PG_RUNTIME_DESTROY_HASH(session->ri_globals.constraint_cache);
	PG_RUNTIME_DESTROY_HASH(session->ri_globals.query_cache);
	PG_RUNTIME_DESTROY_HASH(session->ri_globals.compare_cache);
	dclist_init(&session->ri_globals.constraint_cache_valid_list);
	session->ri_globals.fastpath_xact_callback_registered = false;
	session->ri_globals.debug_discard_caches_initialized = true;
	session->ri_globals.debug_discard_caches_value = DEFAULT_DEBUG_DISCARD_CACHES;
}

static void
PgSessionResetRelMapClosedState(PgSession *session)
{
	Assert(session != NULL);

	PgSessionInitializeRelMapState(&session->relmap);
}

static void
PgSessionResetGUCClosedState(PgSession *session)
{
	Assert(session != NULL);

	if (PgBackendExitInProgress())
		ResetGUCStateAtBackendExit();
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->guc.memory_context);
	PgSessionInitializeGUCState(&session->guc);
}

static void
PgSessionResetDateTimeClosedState(PgSession *session)
{
	Assert(session != NULL);

	if (!PgBackendExitInProgress())
		return;

	session->datetime.timezone_abbrev_table = NULL;
	MemSet(session->datetime.timezone_abbrev_cache, 0,
		   sizeof(session->datetime.timezone_abbrev_cache));
}

static void
PgSessionResetLogicalReplicationClosedState(PgSession *session)
{
	Assert(session != NULL);

	if (session->logical_replication.logical_rep_relmap_context != NULL)
	{
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->logical_replication.logical_rep_relmap_context);
		session->logical_replication.logical_rep_relmap = NULL;
	}
	else if (session->logical_replication.logical_rep_relmap != NULL)
	{
		PG_RUNTIME_DESTROY_HASH(session->logical_replication.logical_rep_relmap);
	}

	if (session->logical_replication.logical_rep_partmap_context != NULL)
	{
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->logical_replication.logical_rep_partmap_context);
		session->logical_replication.logical_rep_partmap = NULL;
	}
	else if (session->logical_replication.logical_rep_partmap != NULL)
	{
		PG_RUNTIME_DESTROY_HASH(session->logical_replication.logical_rep_partmap);
	}

	PG_RUNTIME_DESTROY_HASH(session->logical_replication.pgoutput_relation_sync_cache);
	session->logical_replication.pgoutput_publications_valid = false;
	session->logical_replication.syncing_relations_state = 0;
}

static void
PgSessionResetUserIdentityClosedState(PgSession *session)
{
	Assert(session != NULL);

	for (int i = 0; i < lengthof(session->user_identity.cached_roles); i++)
	{
		session->user_identity.cached_role[i] = InvalidOid;
		PG_RUNTIME_LIST_FREE(session->user_identity.cached_roles[i]);
	}
	if (session->user_identity.system_user_owned &&
		session->user_identity.system_user != NULL &&
		session->user_identity.system_user_context == NULL)
		pfree((void *) session->user_identity.system_user);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->user_identity.system_user_context);
	session->user_identity.system_user = NULL;
	session->user_identity.system_user_owned = false;
	session->user_identity.cached_db_hash = 0;
}

static void
PgSessionResetTextSearchClosedState(PgSession *session)
{
	Assert(session != NULL);

	PG_RUNTIME_DESTROY_HASH(session->text_search.parser_cache_hash);
	session->text_search.last_used_parser = NULL;

	if (session->text_search.dictionary_cache_hash != NULL)
	{
		HASH_SEQ_STATUS status;
		TSDictionaryCacheEntry *entry;

		hash_seq_init(&status, session->text_search.dictionary_cache_hash);
		while ((entry = (TSDictionaryCacheEntry *) hash_seq_search(&status)) != NULL)
		{
			if (entry->dictCtx != NULL)
			{
				MemoryContextDelete(entry->dictCtx);
				entry->dictCtx = NULL;
				entry->dictData = NULL;
			}
		}
		hash_destroy(session->text_search.dictionary_cache_hash);
		session->text_search.dictionary_cache_hash = NULL;
	}
	session->text_search.last_used_dictionary = NULL;

	if (session->text_search.config_cache_hash != NULL)
	{
		HASH_SEQ_STATUS status;
		TSConfigCacheEntry *entry;

		hash_seq_init(&status, session->text_search.config_cache_hash);
		while ((entry = (TSConfigCacheEntry *) hash_seq_search(&status)) != NULL)
		{
			if (entry->map != NULL)
			{
				for (int i = 0; i < entry->lenmap; i++)
				{
					if (entry->map[i].dictIds != NULL)
						pfree(entry->map[i].dictIds);
				}
				pfree(entry->map);
				entry->map = NULL;
				entry->lenmap = 0;
			}
		}
		hash_destroy(session->text_search.config_cache_hash);
		session->text_search.config_cache_hash = NULL;
	}
	session->text_search.last_used_config = NULL;
	session->text_search.current_config_cache = InvalidOid;
}

static void
PgSessionResetDatabaseClosedState(PgSession *session)
{
	Assert(session != NULL);

	if (session->database.database_path != NULL)
	{
		if (session->database.database_path_owned &&
			session->database.database_path_context == NULL)
			pfree(session->database.database_path);
		session->database.database_path = NULL;
	}
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->database.database_path_context);
	session->database.database_path_owned = false;
}

static void
PgSessionResetDynamicLibraryInitsClosedState(PgSession *session)
{
	Assert(session != NULL);

	if (session->dynamic_library_context == NULL &&
		session->dynamic_library_inits != NIL)
		list_free(session->dynamic_library_inits);

	session->dynamic_library_inits = NIL;
}

static void
PgSessionResetRegexClosedState(PgSession *session)
{
	Assert(session != NULL);

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->regex.regexp_cache_context);
	pg_free_regex_ctype_cache_list(session->regex.ctype_cache_list);
	PgSessionInitializeRegexState(&session->regex);
}

static void
PgSessionResetPortalManagerClosedState(PgSession *session)
{
	Assert(session != NULL);

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->portal_manager.top_portal_context);
	PgSessionInitializePortalManagerState(&session->portal_manager);
}

static void
PgSessionResetOptimizerClosedState(PgSession *session)
{
	Assert(session != NULL);

	if (session->optimizer.planner_extension_names != NULL)
	{
		pfree(session->optimizer.planner_extension_names);
		session->optimizer.planner_extension_names = NULL;
	}
	session->optimizer.planner_extension_names_assigned = 0;
	session->optimizer.planner_extension_names_allocated = 0;
	PG_RUNTIME_DESTROY_HASH(session->optimizer.opr_proof_cache_hash);
}

static void
PgSessionResetLocaleClosedState(PgSession *session)
{
	Assert(session != NULL);

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->locale.locale_time_context);
	PgSessionResetLocaleTime(&session->locale);

	PgSessionResetLocaleConv(&session->locale);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->locale.locale_conv_context);

	if (PgBackendExitInProgress() &&
		session->locale.default_locale != NULL)
	{
		pg_locale_release_external((pg_locale_t) session->locale.default_locale);
		session->locale.default_locale = NULL;
	}

	if (session->locale.collation_cache_context != NULL)
	{
		if (PgBackendExitInProgress())
			pg_locale_release_collation_cache_external(session->locale.collation_cache);
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->locale.collation_cache_context);
		session->locale.collation_cache = NULL;
		session->locale.last_collation_cache_oid = InvalidOid;
		session->locale.last_collation_cache_locale = NULL;
	}
	if (session->locale.icu_converter != NULL)
	{
		PgCloseIcuConverter(session->locale.icu_converter);
		session->locale.icu_converter = NULL;
	}
}

static void
PgSessionResetLegacySessionContextClosedState(PgSession *session)
{
	Assert(session != NULL);

	if (session->legacy_session_context != NULL)
	{
		if (CurrentPgSession == session &&
			CurrentSession == session->legacy_session)
			CurrentSession = NULL;
		PG_RUNTIME_DELETE_MEMORY_CONTEXT(session->legacy_session_context);
	}
}

static void
PgSessionResetLegacySessionClosedState(PgSession *session)
{
	Assert(session != NULL);

	if (CurrentPgSession == session &&
		CurrentSession == session->legacy_session)
		CurrentSession = NULL;
	session->legacy_session = NULL;
}

static void
PgSessionResetVacuumClosedState(PgSession *session)
{
	Assert(session != NULL);

	PgSessionInitializeVacuumState(&session->vacuum);
}

static void
PgSessionResetLockWaitClosedState(PgSession *session)
{
	Assert(session != NULL);

	PgSessionInitializeLockWaitState(&session->lock_wait);
}

static void
PgSessionResetLargeObjectClosedState(PgSession *session)
{
	Assert(session != NULL);
	Assert(session->large_object.heap_relation == NULL);
	Assert(session->large_object.index_relation == NULL);

	PgSessionInitializeLargeObjectState(&session->large_object);
}

static void
PgSessionResetTempFileClosedState(PgSession *session)
{
	Assert(session != NULL);

	PgSessionInitializeTempFileState(&session->temp_file);
}

static void
PgSessionResetPlanCacheClosedState(PgSession *session)
{
	Assert(session != NULL);
	if (!session->plan_cache.initialized)
	{
		PgSessionInitializePlanCacheState(&session->plan_cache);
		return;
	}

	Assert(dlist_is_empty(&session->plan_cache.saved_plan_list));
	Assert(dlist_is_empty(&session->plan_cache.cached_expression_list));

	PgSessionInitializePlanCacheState(&session->plan_cache);
}

static void
PgSessionResetNamespaceClosedState(PgSession *session)
{
	Assert(session != NULL);

	/*
	 * Normal namespace cleanup owns temp-namespace relation removal and GUC
	 * cleanup owns namespace_search_path_value.  Closed-session reset only
	 * releases the derived search-path storage/cache contexts and clears the
	 * remaining slots.
	 */
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(
		session->namespace_state.search_path_context);
	PG_RUNTIME_DELETE_MEMORY_CONTEXT(
		session->namespace_state.search_path_cache_context);
	PgSessionInitializeNamespaceState(&session->namespace_state);
}

void
PgSessionResetClosedState(PgSession *session)
{
	if (session == NULL)
		return;

#define PG_SESSION_RESET_BUCKET(field, reset) \
	do { reset; } while (0);
#include "backend_runtime_session_reset_buckets.def"
#undef PG_SESSION_RESET_BUCKET
}

static void
PgExecutionResetDebugClosedState(PgExecution *execution)
{
	Assert(execution != NULL);

	execution->debug.debug_query_string = NULL;
}

static void
PgExecutionResetMemoryContextsClosedState(PgExecution *execution)
{
	bool		preserve_error_context;
	MemoryContext error_context;

	Assert(execution != NULL);

	/*
	 * Threaded backend finish still has to publish logical exit and reclaim
	 * the retained TopMemoryContext after closed-state reset.  Keep the
	 * backend's ErrorContext address usable for any ereport() on that final
	 * physical-thread path, while clearing Top/CurrentMemoryContext so the
	 * retained root can be deleted deliberately by the carrier exit code.
	 */
	preserve_error_context =
		PgBackendExitInProgress() &&
		PgRuntimeIsThreadBacked(CurrentPgRuntime) &&
		execution == CurrentPgExecution;
	error_context = preserve_error_context ?
		execution->memory_contexts.error_context : NULL;

	PG_RUNTIME_DELETE_MEMORY_CONTEXT(execution->memory_contexts.message_context);

	MemSet(&execution->memory_contexts, 0,
		   sizeof(execution->memory_contexts));

	if (preserve_error_context)
		execution->memory_contexts.error_context = error_context;
}

static void
PgExecutionResetExtensionClosedState(PgExecutionExtensionState *extension)
{
	Assert(extension != NULL);

	foreach_ptr(PgExecutionExtensionPrivateState, private_state,
				extension->private_states)
	{
		if (private_state->cleanup != NULL &&
			private_state->state != NULL)
			private_state->cleanup(private_state->state);
	}

	foreach_ptr(PgExecutionExtensionPrivateState, private_state,
				extension->private_states)
	{
		if (private_state->state != NULL)
			pfree(private_state->state);
	}
	list_free_deep(extension->private_states);

	PgExecutionInitializeExtensionState(extension);
}

void
PgExecutionResetClosedState(PgExecution *execution)
{
	if (execution == NULL)
		return;

	if (execution == CurrentPgExecution)
		PgRuntimeFlushCurrentHotCells();
	if (execution == CurrentPgExecution)
		PgRuntimeFlushCurrentHotMirrors();

#define PG_EXECUTION_BUCKET(field, init, adopt, reset) \
	do { reset; } while (0);
#include "backend_runtime_execution_buckets.def"
#undef PG_EXECUTION_BUCKET

	if (execution == CurrentPgExecution)
		PgRuntimeReloadCurrentHotCells();
	if (execution == CurrentPgExecution)
		PgRuntimeReloadCurrentHotMirrors();
}
