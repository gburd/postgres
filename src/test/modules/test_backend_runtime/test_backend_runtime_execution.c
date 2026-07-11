/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_execution.c
 *		Execution-owned backend runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_execution.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

static void
test_backend_runtime_seed_execution_memory_contexts(PgExecution *execution)
{
	execution->memory_contexts.top_context = TopMemoryContext;
	execution->memory_contexts.current_context = CurrentMemoryContext;
	execution->memory_contexts.error_context = ErrorContext;
	execution->memory_contexts.message_context = MessageContext;
	execution->memory_contexts.top_transaction_context = TopTransactionContext;
	execution->memory_contexts.cur_transaction_context = CurTransactionContext;
	execution->memory_contexts.portal_context = PortalContext;
}

static void
test_backend_runtime_debug_handler1(const char *message)
{
}

static void
test_backend_runtime_debug_handler2(const char *message)
{
}

PG_FUNCTION_INFO_V1(test_execution_resource_owners_are_execution_local);
Datum
test_execution_resource_owners_are_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	ResourceOwner saved_current_resource_owner;
	ResourceOwner saved_cur_transaction_resource_owner;
	ResourceOwner saved_top_transaction_resource_owner;
	ResourceOwner fake_owner1 = (ResourceOwner) &fake_execution1;
	ResourceOwner fake_owner2 = (ResourceOwner) &fake_execution2;
	ResourceOwner fake_owner3 = (ResourceOwner) &saved_execution;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_current_resource_owner = CurrentResourceOwner;
	saved_cur_transaction_resource_owner = CurTransactionResourceOwner;
	saved_top_transaction_resource_owner = TopTransactionResourceOwner;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		CurrentResourceOwner = fake_owner1;
		CurTransactionResourceOwner = fake_owner2;
		TopTransactionResourceOwner = fake_owner3;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && CurrentResourceOwner == NULL;
		ok = ok && CurTransactionResourceOwner == NULL;
		ok = ok && TopTransactionResourceOwner == NULL;
		CurrentResourceOwner = fake_owner3;
		CurTransactionResourceOwner = fake_owner1;
		TopTransactionResourceOwner = fake_owner2;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && CurrentResourceOwner == fake_owner1;
		ok = ok && CurTransactionResourceOwner == fake_owner2;
		ok = ok && TopTransactionResourceOwner == fake_owner3;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && CurrentResourceOwner == fake_owner3;
		ok = ok && CurTransactionResourceOwner == fake_owner1;
		ok = ok && TopTransactionResourceOwner == fake_owner2;

		PgSetCurrentExecution(saved_execution);
		CurrentResourceOwner = saved_current_resource_owner;
		CurTransactionResourceOwner = saved_cur_transaction_resource_owner;
		TopTransactionResourceOwner = saved_top_transaction_resource_owner;
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		CurrentResourceOwner = saved_current_resource_owner;
		CurTransactionResourceOwner = saved_cur_transaction_resource_owner;
		TopTransactionResourceOwner = saved_top_transaction_resource_owner;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "execution resource owners were not execution-local");

	PG_RETURN_BOOL(true);
}
PG_FUNCTION_INFO_V1(test_execution_debug_query_string_is_execution_local);
Datum
test_execution_debug_query_string_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	const char *saved_debug_query_string;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_debug_query_string = debug_query_string;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		debug_query_string = "fake execution one";

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && debug_query_string == NULL;
		debug_query_string = "fake execution two";

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && strcmp(debug_query_string, "fake execution one") == 0;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && strcmp(debug_query_string, "fake execution two") == 0;
		debug_query_string = NULL;

		PgSetCurrentExecution(&fake_execution1);
		debug_query_string = "reset me";
		fake_execution1.memory_contexts.message_context = NULL;
		PgExecutionResetClosedState(&fake_execution1);
		ok = ok && debug_query_string == NULL;

		PgSetCurrentExecution(saved_execution);
		debug_query_string = saved_debug_query_string;
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		debug_query_string = saved_debug_query_string;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "debug_query_string was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_error_state_is_execution_local);
Datum
test_execution_error_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	ErrorContextCallback *saved_error_context_stack;
	sigjmp_buf *saved_exception_stack;
	ErrorContextCallback fake_error_context1;
	ErrorContextCallback fake_error_context2;
	sigjmp_buf fake_exception_stack1;
	sigjmp_buf fake_exception_stack2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_error_context_stack = error_context_stack;
	saved_exception_stack = PG_exception_stack;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);
	MemSet(&fake_error_context1, 0, sizeof(fake_error_context1));
	MemSet(&fake_error_context2, 0, sizeof(fake_error_context2));

	/*
	 * Do not wrap this in PG_TRY(): this test intentionally rewires
	 * PG_exception_stack to prove the compatibility lvalue is execution-local.
	 */
	PgSetCurrentExecution(&fake_execution1);
	error_context_stack = &fake_error_context1;
	PG_exception_stack = &fake_exception_stack1;

	PgSetCurrentExecution(&fake_execution2);
	ok = ok && error_context_stack == NULL;
	ok = ok && PG_exception_stack == NULL;
	error_context_stack = &fake_error_context2;
	PG_exception_stack = &fake_exception_stack2;

	PgSetCurrentExecution(&fake_execution1);
	ok = ok && error_context_stack == &fake_error_context1;
	ok = ok && PG_exception_stack == &fake_exception_stack1;

	PgSetCurrentExecution(&fake_execution2);
	ok = ok && error_context_stack == &fake_error_context2;
	ok = ok && PG_exception_stack == &fake_exception_stack2;

	PgSetCurrentExecution(saved_execution);
	error_context_stack = saved_error_context_stack;
	PG_exception_stack = saved_exception_stack;

	if (!ok)
		elog(ERROR, "execution error state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_memory_contexts_are_execution_local);
Datum
test_execution_memory_contexts_are_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	MemoryContext saved_top_memory_context;
	MemoryContext saved_current_memory_context;
	MemoryContext saved_error_context;
	MemoryContext saved_message_context;
	MemoryContext saved_top_transaction_context;
	MemoryContext saved_cur_transaction_context;
	MemoryContext saved_portal_context;
	MemoryContext reset_message_context;
	MemoryContext fake_context1 = (MemoryContext) &fake_execution1;
	MemoryContext fake_context2 = (MemoryContext) &fake_execution2;
	MemoryContext fake_context3 = (MemoryContext) &saved_execution;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_top_memory_context = TopMemoryContext;
	saved_current_memory_context = CurrentMemoryContext;
	saved_error_context = ErrorContext;
	saved_message_context = MessageContext;
	saved_top_transaction_context = TopTransactionContext;
	saved_cur_transaction_context = CurTransactionContext;
	saved_portal_context = PortalContext;
	reset_message_context =
		AllocSetContextCreate(saved_top_memory_context,
							  "test reset message context",
							  ALLOCSET_SMALL_SIZES);
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		TopMemoryContext = fake_context1;
		CurrentMemoryContext = fake_context1;
		ErrorContext = fake_context2;
		MessageContext = fake_context3;
		TopTransactionContext = fake_context1;
		CurTransactionContext = fake_context2;
		PortalContext = fake_context3;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && TopMemoryContext == NULL;
		ok = ok && CurrentMemoryContext == NULL;
		ok = ok && ErrorContext == NULL;
		ok = ok && MessageContext == NULL;
		ok = ok && TopTransactionContext == NULL;
		ok = ok && CurTransactionContext == NULL;
		ok = ok && PortalContext == NULL;
		TopMemoryContext = fake_context2;
		CurrentMemoryContext = fake_context3;
		ErrorContext = fake_context1;
		MessageContext = fake_context2;
		TopTransactionContext = fake_context3;
		CurTransactionContext = fake_context1;
		PortalContext = fake_context2;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && TopMemoryContext == fake_context1;
		ok = ok && CurrentMemoryContext == fake_context1;
		ok = ok && ErrorContext == fake_context2;
		ok = ok && MessageContext == fake_context3;
		ok = ok && TopTransactionContext == fake_context1;
		ok = ok && CurTransactionContext == fake_context2;
		ok = ok && PortalContext == fake_context3;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && TopMemoryContext == fake_context2;
		ok = ok && CurrentMemoryContext == fake_context3;
		ok = ok && ErrorContext == fake_context1;
		ok = ok && MessageContext == fake_context2;
		ok = ok && TopTransactionContext == fake_context3;
		ok = ok && CurTransactionContext == fake_context1;
		ok = ok && PortalContext == fake_context2;

		MessageContext = reset_message_context;
		PgExecutionResetClosedState(&fake_execution2);
		reset_message_context = NULL;
		ok = ok && TopMemoryContext == NULL;
		ok = ok && CurrentMemoryContext == NULL;
		ok = ok && ErrorContext == NULL;
		ok = ok && MessageContext == NULL;
		ok = ok && TopTransactionContext == NULL;
		ok = ok && CurTransactionContext == NULL;
		ok = ok && PortalContext == NULL;

		PgSetCurrentExecution(saved_execution);
		TopMemoryContext = saved_top_memory_context;
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
		MessageContext = saved_message_context;
		TopTransactionContext = saved_top_transaction_context;
		CurTransactionContext = saved_cur_transaction_context;
		PortalContext = saved_portal_context;
		if (reset_message_context != NULL)
			MemoryContextDelete(reset_message_context);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		TopMemoryContext = saved_top_memory_context;
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
		MessageContext = saved_message_context;
		TopTransactionContext = saved_top_transaction_context;
		CurTransactionContext = saved_cur_transaction_context;
		PortalContext = saved_portal_context;
		if (reset_message_context != NULL)
			MemoryContextDelete(reset_message_context);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "execution memory contexts were not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_spi_state_is_execution_local);
Datum
test_execution_spi_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	uint64		saved_spi_processed;
	SPITupleTable *saved_spi_tuptable;
	int			saved_spi_result;
	SPITupleTable fake_tuptable1;
	SPITupleTable fake_tuptable2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_spi_processed = SPI_processed;
	saved_spi_tuptable = SPI_tuptable;
	saved_spi_result = SPI_result;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);
	fake_execution1.spi.connected = -1;
	fake_execution2.spi.connected = -1;
	MemSet(&fake_tuptable1, 0, sizeof(fake_tuptable1));
	MemSet(&fake_tuptable2, 0, sizeof(fake_tuptable2));

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		SPI_processed = 111;
		SPI_tuptable = &fake_tuptable1;
		SPI_result = SPI_OK_SELECT;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && SPI_processed == 0;
		ok = ok && SPI_tuptable == NULL;
		ok = ok && SPI_result == 0;
		ok = ok && *PgCurrentSPIConnectedRef() == -1;
		SPI_processed = 222;
		SPI_tuptable = &fake_tuptable2;
		SPI_result = SPI_OK_INSERT;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && SPI_processed == 111;
		ok = ok && SPI_tuptable == &fake_tuptable1;
		ok = ok && SPI_result == SPI_OK_SELECT;
		ok = ok && *PgCurrentSPIConnectedRef() == -1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && SPI_processed == 222;
		ok = ok && SPI_tuptable == &fake_tuptable2;
		ok = ok && SPI_result == SPI_OK_INSERT;
		ok = ok && *PgCurrentSPIConnectedRef() == -1;

		PgSetCurrentExecution(saved_execution);
		SPI_processed = saved_spi_processed;
		SPI_tuptable = saved_spi_tuptable;
		SPI_result = saved_spi_result;
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		SPI_processed = saved_spi_processed;
		SPI_tuptable = saved_spi_tuptable;
		SPI_result = saved_spi_result;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "SPI state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_active_portal_is_execution_local);
Datum
test_execution_active_portal_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	Portal		saved_active_portal;
	PortalData	fake_portal1;
	PortalData	fake_portal2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_active_portal = ActivePortal;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);
	MemSet(&fake_portal1, 0, sizeof(fake_portal1));
	MemSet(&fake_portal2, 0, sizeof(fake_portal2));

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		ActivePortal = &fake_portal1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && ActivePortal == NULL;
		ActivePortal = &fake_portal2;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && ActivePortal == &fake_portal1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && ActivePortal == &fake_portal2;

		PgSetCurrentExecution(saved_execution);
		ActivePortal = saved_active_portal;
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		ActivePortal = saved_active_portal;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "active portal was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_reset_closed_state);
Datum
test_execution_reset_closed_state(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution;
	ErrorContextCallback fake_error_context;
	sigjmp_buf fake_exception_stack;
	SPITupleTable fake_tuptable;
	PortalData	fake_portal;
	ResourceOwner test_owner;
	MemoryContext resource_owner_context;
	MemoryContext message_context;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution, 0, sizeof(fake_execution));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution);
	message_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test execution message context",
							  ALLOCSET_SMALL_SIZES);
	fake_execution.memory_contexts.message_context = message_context;
	MemSet(&fake_error_context, 0, sizeof(fake_error_context));
	MemSet(&fake_tuptable, 0, sizeof(fake_tuptable));
	MemSet(&fake_portal, 0, sizeof(fake_portal));

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution);
		debug_query_string = "reset execution";
		fake_execution.error.context_stack = &fake_error_context;
		fake_execution.error.exception_stack = &fake_exception_stack;
		fake_execution.error.errordata_stack_depth = 3;
		fake_execution.error.recursion_depth = 2;
		fake_execution.error.saved_timeval_set = true;
		CurrentResourceOwner = (ResourceOwner) &fake_execution;
		CurTransactionResourceOwner = (ResourceOwner) saved_execution;
		TopTransactionResourceOwner = (ResourceOwner) &fake_error_context;
		test_owner = ResourceOwnerCreate(NULL, "test execution resource owner");
		resource_owner_context =
			fake_execution.resource_owners.resource_owner_context;
		ResourceOwnerDelete(test_owner);
		SPI_processed = 123;
		SPI_tuptable = &fake_tuptable;
		SPI_result = SPI_OK_SELECT;
		fake_execution.spi.stack = (_SPI_connection *) &fake_execution;
		fake_execution.spi.current = (_SPI_connection *) &fake_tuptable;
		fake_execution.spi.stack_depth = 2;
		fake_execution.spi.connected = 1;
		ActivePortal = &fake_portal;
		fake_execution.vacuum.in_vacuum = true;
		fake_execution.vacuum.cost_balance = 10;
		fake_execution.vacuum.cost_active = true;
		fake_execution.vacuum.shared_cost_balance =
			(pg_atomic_uint32 *) &fake_execution;
		fake_execution.vacuum.active_nworkers =
			(pg_atomic_uint32 *) &fake_error_context;
		fake_execution.vacuum.cost_balance_local = 11;
		fake_execution.vacuum.failsafe_active = true;
		fake_execution.vacuum.parallel_worker_delay_ns = 12;
		fake_execution.vacuum.parallel_shared_cost_params = &fake_tuptable;
		fake_execution.vacuum.parallel_shared_params_generation_local = 13;
		fake_execution.node_io.write_location_fields = true;
		fake_execution.node_io.strtok_ptr = "node";
		fake_execution.node_io.restore_location_fields = true;
		fake_execution.basebackup.backup_started_in_recovery = true;
		fake_execution.basebackup.total_checksum_failures = 7;
		fake_execution.basebackup.noverify_checksums = true;
		fake_execution.analyze.context =
			AllocSetContextCreate(TopMemoryContext,
								  "test analyze context",
								  ALLOCSET_SMALL_SIZES);
		fake_execution.analyze.strategy = (BufferAccessStrategy) &fake_tuptable;
		fake_execution.analyze.array_extra_data =
			MemoryContextAlloc(fake_execution.analyze.context, sizeof(int));
		fake_execution.extension.creating = true;
		fake_execution.extension.current_object = 1234;
		fake_execution.matview.maintenance_depth = 2;
		fake_execution.snapshot.current_snapshot = (Snapshot) &fake_execution;
		fake_execution.snapshot.transaction_xmin = 123;
		fake_execution.snapshot.recent_xmin = 456;
		fake_execution.snapshot.first_snapshot_set = true;
		fake_execution.snapshot.exported_snapshots = (List *) &fake_tuptable;
		fake_execution.combo_cid.hash = (HTAB *) &fake_execution;
		fake_execution.combo_cid.cids = &fake_error_context;
		fake_execution.combo_cid.used = 1;
		fake_execution.combo_cid.size = 2;
		fake_execution.xloginsert.context =
			AllocSetContextCreate(TopMemoryContext,
								  "test WAL construction context",
								  ALLOCSET_SMALL_SIZES);
		fake_execution.xloginsert.registered_buffers =
			MemoryContextAlloc(fake_execution.xloginsert.context, sizeof(int));
		fake_execution.xloginsert.max_registered_buffers = 4;
		fake_execution.xloginsert.mainrdata_head = (XLogRecData *) &fake_execution;
		fake_execution.xloginsert.mainrdata_last = (XLogRecData *) &fake_tuptable;
		fake_execution.xloginsert.mainrdata_len = 5;
		fake_execution.xloginsert.begininsert_called = true;
		fake_execution.xact.iso_level = XACT_SERIALIZABLE;
		fake_execution.xact.read_only = true;
		fake_execution.xact.check_xid_alive = 789;
		fake_execution.xact.top_full_transaction_id =
			FullTransactionIdFromEpochAndXid(1, 789);
		fake_execution.xact.parallel_current_xids =
			(TransactionId *) &fake_execution;
		fake_execution.xact.n_unreported_xids = 1;
		fake_execution.xact.current_command_id = 42;
		fake_execution.xact.prepare_gid = (char *) "gid";
		fake_execution.xact.transaction_abort_context =
			AllocSetContextCreate(TopMemoryContext,
								  "test transaction abort context",
								  ALLOCSET_SMALL_SIZES);
		fake_execution.transaction_cleanup.lo_cookies =
			NULL;
		fake_execution.transaction_cleanup.lo_cookies_size = 3;
		fake_execution.transaction_cleanup.lo_cleanup_needed = true;
		fake_execution.transaction_cleanup.lo_context =
			AllocSetContextCreate(TopMemoryContext,
								  "test large object context",
								  ALLOCSET_SMALL_SIZES);
		fake_execution.transaction_cleanup.lo_cookies =
			(LargeObjectDesc **)
			MemoryContextAlloc(fake_execution.transaction_cleanup.lo_context,
							   sizeof(LargeObjectDesc *));
		fake_execution.transaction_cleanup.have_xact_temporary_files = true;
		fake_execution.transaction_cleanup.pgstat_xact_stack =
			(PgStat_SubXactStatus *) &fake_tuptable;
		fake_execution.transaction_cleanup.ri_fastpath_cache =
			(HTAB *) &fake_portal;
		fake_execution.replication_scratch.replorigin_xact.origin = 1;
		fake_execution.replication_scratch.replorigin_xact.origin_lsn = 2;
		fake_execution.replication_scratch.replorigin_xact.origin_timestamp = 3;
		fake_execution.replication_scratch.apply_error_context_stack =
			&fake_error_context;
		fake_execution.replication_scratch.apply_message_context =
			AllocSetContextCreate(TopMemoryContext,
								  "test apply message context",
								  ALLOCSET_SMALL_SIZES);
		fake_execution.replication_scratch.logical_streaming_context =
			AllocSetContextCreate(TopMemoryContext,
								  "test logical streaming context",
								  ALLOCSET_SMALL_SIZES);
		fake_execution.guc_error.check_errcode_value = 1;
		fake_execution.guc_error.check_errmsg_string = (char *) "msg";
		fake_execution.guc_error.format_errnumber = 2;
		fake_execution.guc_error.format_domain = "domain";
		fake_execution.guc_error.flex_fatal_jmp = &fake_exception_stack;
		fake_execution.async.pending_actions = (struct ActionList *) &fake_execution;
		fake_execution.async.pending_listen_actions = (HTAB *) &fake_tuptable;
		fake_execution.async.pending_notifies =
			(struct NotificationList *) &fake_portal;
		fake_execution.async.signal_context =
			AllocSetContextCreate(TopMemoryContext,
								  "test async signal workspace",
								  ALLOCSET_SMALL_SIZES);
		fake_execution.async.signal_pids =
			MemoryContextAlloc(fake_execution.async.signal_context,
							   sizeof(int32));
		fake_execution.async.signal_procnos =
			MemoryContextAlloc(fake_execution.async.signal_context,
							   sizeof(ProcNumber));
		fake_execution.async.try_advance_tail = true;
		fake_execution.catalog.uncommitted_enum_types = (HTAB *) &fake_execution;
		fake_execution.catalog.currently_reindexed_heap = 555;
		fake_execution.catalog.currently_reindexed_index = 666;
		fake_execution.catalog.pending_reindexed_indexes = (List *) &fake_tuptable;
		fake_execution.catalog.pending_rel_deletes =
			(struct PendingRelDelete *) &fake_portal;
		fake_execution.catalog.pending_sync_hash = (HTAB *) &fake_error_context;
		fake_execution.catalog_cache.catcache_in_progress_stack =
			(CatCInProgress *) &fake_execution;
		fake_execution.catalog_cache.relcache_in_progress_list =
			(InProgressEnt *) &fake_tuptable;
		fake_execution.catalog_cache.relcache_eoxact_list_len = 1;
		fake_execution.catalog_cache.relcache_eoxact_tupledesc_array =
			(TupleDesc *) &fake_portal;
		fake_execution.relmap.active_shared_updates.magic = 1;
		fake_execution.relmap.pending_local_updates.num_mappings = 1;
		fake_execution.invalidation.message_arrays[0].msgs = &fake_execution;
		fake_execution.invalidation.message_arrays[0].maxmsgs = 1;
		fake_execution.invalidation.trans_info =
			(struct TransInvalidationInfo *) &fake_tuptable;
		fake_execution.invalidation.inplace_info =
			(struct InvalidationInfo *) &fake_portal;
		fake_execution.two_phase_records.head =
			(struct StateFileChunk *) &fake_execution;
		fake_execution.two_phase_records.tail =
			(struct StateFileChunk *) &fake_tuptable;
		fake_execution.two_phase_records.num_chunks = 2;
		fake_execution.trigger.depth = 3;
		fake_execution.trigger.after_triggers_context =
			AllocSetContextCreate(TopMemoryContext,
								  "test after trigger state",
								  ALLOCSET_SMALL_SIZES);
		fake_execution.trigger.after_triggers_data =
			MemoryContextAlloc(fake_execution.trigger.after_triggers_context,
							   sizeof(int));
		fake_execution.regex.regex_locale = &fake_tuptable;
		fake_execution.valgrind.old_error_count = 99;
		fake_execution.snapbuild.saved_resource_owner_during_export =
			(ResourceOwner) &fake_portal;
		fake_execution.snapbuild.export_in_progress = true;

		PgExecutionResetClosedState(&fake_execution);
		message_context = NULL;

		ok = ok && debug_query_string == NULL;
		ok = ok && error_context_stack == NULL;
		ok = ok && PG_exception_stack == NULL;
		ok = ok && fake_execution.error.errordata_stack_depth == -1;
		ok = ok && fake_execution.error.recursion_depth == 0;
		ok = ok && !fake_execution.error.saved_timeval_set;
		ok = ok && CurrentResourceOwner == NULL;
		ok = ok && CurTransactionResourceOwner == NULL;
		ok = ok && TopTransactionResourceOwner == NULL;
		ok = ok && fake_execution.resource_owners.resource_owner_context ==
			resource_owner_context;
		ok = ok && SPI_processed == 0;
		ok = ok && SPI_tuptable == NULL;
		ok = ok && SPI_result == 0;
		ok = ok && fake_execution.spi.stack == NULL;
		ok = ok && fake_execution.spi.current == NULL;
		ok = ok && fake_execution.spi.stack_depth == 0;
		ok = ok && fake_execution.spi.connected == -1;
		ok = ok && ActivePortal == NULL;
		ok = ok && fake_execution.memory_contexts.top_context == NULL;
		ok = ok && fake_execution.memory_contexts.current_context == NULL;
		ok = ok && fake_execution.memory_contexts.error_context == NULL;
		ok = ok && fake_execution.memory_contexts.message_context == NULL;
		ok = ok && fake_execution.memory_contexts.top_transaction_context == NULL;
		ok = ok && fake_execution.memory_contexts.cur_transaction_context == NULL;
		ok = ok && fake_execution.memory_contexts.portal_context == NULL;
		ok = ok && !fake_execution.vacuum.in_vacuum;
		ok = ok && fake_execution.vacuum.cost_balance == 0;
		ok = ok && !fake_execution.vacuum.cost_active;
		ok = ok && fake_execution.vacuum.shared_cost_balance == NULL;
		ok = ok && fake_execution.vacuum.active_nworkers == NULL;
		ok = ok && fake_execution.vacuum.cost_balance_local == 0;
		ok = ok && !fake_execution.vacuum.failsafe_active;
		ok = ok && fake_execution.vacuum.parallel_worker_delay_ns == 0;
		ok = ok && fake_execution.vacuum.parallel_shared_cost_params == NULL;
		ok = ok &&
			fake_execution.vacuum.parallel_shared_params_generation_local == 0;
		ok = ok && !fake_execution.node_io.write_location_fields;
		ok = ok && fake_execution.node_io.strtok_ptr == NULL;
		ok = ok && !fake_execution.node_io.restore_location_fields;
		ok = ok && !fake_execution.basebackup.backup_started_in_recovery;
		ok = ok && fake_execution.basebackup.total_checksum_failures == 0;
		ok = ok && !fake_execution.basebackup.noverify_checksums;
		ok = ok && fake_execution.analyze.context == NULL;
		ok = ok && fake_execution.analyze.strategy == NULL;
		ok = ok && fake_execution.analyze.array_extra_data == NULL;
		ok = ok && !fake_execution.extension.creating;
		ok = ok && fake_execution.extension.current_object == InvalidOid;
		ok = ok && fake_execution.matview.maintenance_depth == 0;
		ok = ok && fake_execution.snapshot.current_snapshot == NULL;
		ok = ok &&
			fake_execution.snapshot.transaction_xmin == FirstNormalTransactionId;
		ok = ok &&
			fake_execution.snapshot.recent_xmin == FirstNormalTransactionId;
		ok = ok && !fake_execution.snapshot.first_snapshot_set;
		ok = ok && fake_execution.snapshot.exported_snapshots == NIL;
		ok = ok && fake_execution.combo_cid.hash == NULL;
		ok = ok && fake_execution.combo_cid.cids == NULL;
		ok = ok && fake_execution.combo_cid.used == 0;
		ok = ok && fake_execution.combo_cid.size == 0;
		ok = ok && fake_execution.xloginsert.registered_buffers == NULL;
		ok = ok && fake_execution.xloginsert.max_registered_buffers == 0;
		ok = ok && fake_execution.xloginsert.mainrdata_head == NULL;
		ok = ok && fake_execution.xloginsert.mainrdata_last == NULL;
		ok = ok && fake_execution.xloginsert.mainrdata_len == 0;
		ok = ok && !fake_execution.xloginsert.begininsert_called;
		ok = ok && fake_execution.xloginsert.context == NULL;
		ok = ok && fake_execution.xact.iso_level == XACT_READ_COMMITTED;
		ok = ok && !fake_execution.xact.read_only;
		ok = ok && fake_execution.xact.check_xid_alive == InvalidTransactionId;
		ok = ok &&
			!FullTransactionIdIsValid(fake_execution.xact.top_full_transaction_id);
		ok = ok && fake_execution.xact.parallel_current_xids == NULL;
		ok = ok && fake_execution.xact.n_unreported_xids == 0;
		ok = ok && fake_execution.xact.current_command_id == 0;
		ok = ok && fake_execution.xact.prepare_gid == NULL;
		ok = ok && fake_execution.xact.transaction_abort_context == NULL;
		ok = ok && fake_execution.transaction_cleanup.lo_cookies == NULL;
		ok = ok && fake_execution.transaction_cleanup.lo_cookies_size == 0;
		ok = ok && !fake_execution.transaction_cleanup.lo_cleanup_needed;
		ok = ok && fake_execution.transaction_cleanup.lo_context == NULL;
		ok = ok &&
			!fake_execution.transaction_cleanup.have_xact_temporary_files;
		ok = ok && fake_execution.transaction_cleanup.pgstat_xact_stack == NULL;
		ok = ok && fake_execution.transaction_cleanup.ri_fastpath_cache == NULL;
		ok = ok &&
			fake_execution.replication_scratch.event_trigger_query_state == NULL;
		ok = ok &&
			fake_execution.replication_scratch.event_trigger_context == NULL;
		ok = ok &&
			fake_execution.replication_scratch.replorigin_xact.origin ==
			InvalidReplOriginId;
		ok = ok &&
			fake_execution.replication_scratch.replorigin_xact.origin_lsn ==
			InvalidXLogRecPtr;
		ok = ok &&
			fake_execution.replication_scratch.replorigin_xact.origin_timestamp == 0;
		ok = ok &&
			fake_execution.replication_scratch.apply_error_context_stack == NULL;
		ok = ok &&
			fake_execution.replication_scratch.apply_message_context == NULL;
		ok = ok &&
			fake_execution.replication_scratch.logical_streaming_context == NULL;
		ok = ok && fake_execution.guc_error.check_errcode_value == 0;
		ok = ok && fake_execution.guc_error.check_errmsg_string == NULL;
		ok = ok && fake_execution.guc_error.format_errnumber == 0;
		ok = ok && fake_execution.guc_error.format_domain == NULL;
		ok = ok && fake_execution.guc_error.flex_fatal_jmp == NULL;
		ok = ok && fake_execution.async.pending_actions == NULL;
		ok = ok && fake_execution.async.pending_listen_actions == NULL;
		ok = ok && fake_execution.async.pending_notifies == NULL;
		ok = ok && fake_execution.async.signal_context == NULL;
		ok = ok && fake_execution.async.signal_pids == NULL;
		ok = ok && fake_execution.async.signal_procnos == NULL;
		ok = ok && !fake_execution.async.try_advance_tail;
		ok = ok && fake_execution.catalog.uncommitted_enum_types == NULL;
		ok = ok && fake_execution.catalog.currently_reindexed_heap == InvalidOid;
		ok = ok && fake_execution.catalog.currently_reindexed_index == InvalidOid;
		ok = ok && fake_execution.catalog.pending_reindexed_indexes == NIL;
		ok = ok && fake_execution.catalog.pending_rel_deletes == NULL;
		ok = ok && fake_execution.catalog.pending_sync_hash == NULL;
		ok = ok &&
			fake_execution.catalog_cache.catcache_in_progress_stack == NULL;
		ok = ok &&
			fake_execution.catalog_cache.relcache_in_progress_list == NULL;
		ok = ok && fake_execution.catalog_cache.relcache_eoxact_list_len == 0;
		ok = ok &&
			fake_execution.catalog_cache.relcache_eoxact_tupledesc_array == NULL;
		ok = ok && fake_execution.relmap.active_shared_updates.magic == 0;
		ok = ok && fake_execution.relmap.pending_local_updates.num_mappings == 0;
		ok = ok && fake_execution.invalidation.message_arrays[0].msgs == NULL;
		ok = ok && fake_execution.invalidation.message_arrays[0].maxmsgs == 0;
		ok = ok && fake_execution.invalidation.trans_info == NULL;
		ok = ok && fake_execution.invalidation.inplace_info == NULL;
		ok = ok && fake_execution.two_phase_records.head == NULL;
		ok = ok && fake_execution.two_phase_records.tail == NULL;
		ok = ok && fake_execution.two_phase_records.num_chunks == 0;
		ok = ok && fake_execution.trigger.depth == 0;
		ok = ok && fake_execution.trigger.after_triggers_data == NULL;
		ok = ok && fake_execution.trigger.after_triggers_context == NULL;
		ok = ok && fake_execution.regex.regex_locale == NULL;
		ok = ok && fake_execution.valgrind.old_error_count == 0;
		ok = ok &&
			fake_execution.snapbuild.saved_resource_owner_during_export == NULL;
		ok = ok && !fake_execution.snapbuild.export_in_progress;

		PgExecutionResetClosedState(&fake_execution);
		ok = ok && fake_execution.resource_owners.resource_owner_context == NULL;

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		if (message_context != NULL)
			MemoryContextDelete(message_context);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "execution closed reset did not clear volatile state");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_event_trigger_query_state_reset);
Datum
test_execution_event_trigger_query_state_reset(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution;
	EventTriggerQueryState **statep;
	MemoryContext event_trigger_context = NULL;
	MemoryContext fake_event_trigger_context = NULL;
	bool		began = false;
	bool		ok = true;

	saved_execution = CurrentPgExecution;

	PG_TRY();
	{
		began = EventTriggerBeginCompleteQuery();
		if (!began)
			elog(ERROR, "event trigger complete-query state was not created");

		statep = PgCurrentEventTriggerQueryStateRef();
		ok = ok && *statep != NULL;
		event_trigger_context = *PgCurrentEventTriggerMemoryContextRef();
		ok = ok && event_trigger_context != NULL;
		ok = ok && event_trigger_context != TopMemoryContext;
		ok = ok && MemoryContextGetParent(event_trigger_context) ==
			TopMemoryContext;

		EventTriggerResetQueryStateStack(statep);

		ok = ok && *statep == NULL;
		ok = ok && *PgCurrentEventTriggerMemoryContextRef() ==
			event_trigger_context;
		MemoryContextDelete(event_trigger_context);
		*PgCurrentEventTriggerMemoryContextRef() = NULL;
		event_trigger_context = NULL;

		MemSet(&fake_execution, 0, sizeof(fake_execution));
		test_backend_runtime_seed_execution_memory_contexts(&fake_execution);
		fake_event_trigger_context =
			AllocSetContextCreate(TopMemoryContext,
								  "fake event trigger execution state",
								  ALLOCSET_SMALL_SIZES);
		fake_execution.replication_scratch.event_trigger_context =
			fake_event_trigger_context;
		PgSetCurrentExecution(&fake_execution);
		fake_execution.memory_contexts.message_context = NULL;
		PgExecutionResetClosedState(&fake_execution);
		fake_event_trigger_context = NULL;
		ok = ok && *PgCurrentEventTriggerMemoryContextRef() == NULL;
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		if (CurrentPgExecution == saved_execution && began)
			EventTriggerResetQueryStateStack(PgCurrentEventTriggerQueryStateRef());
		if (CurrentPgExecution == saved_execution &&
			event_trigger_context != NULL &&
			*PgCurrentEventTriggerMemoryContextRef() == event_trigger_context)
		{
			MemoryContextDelete(event_trigger_context);
			*PgCurrentEventTriggerMemoryContextRef() = NULL;
		}
		if (fake_event_trigger_context != NULL)
			MemoryContextDelete(fake_event_trigger_context);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "event trigger query state was not reset");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_vacuum_state_is_execution_local);
Datum
test_execution_vacuum_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	bool		saved_vacuum_in_progress;
	int			saved_vacuum_cost_balance;
	bool		saved_vacuum_cost_active;
	pg_atomic_uint32 *saved_vacuum_shared_cost_balance;
	pg_atomic_uint32 *saved_vacuum_active_nworkers;
	int			saved_vacuum_cost_balance_local;
	bool		saved_vacuum_failsafe_active;
	int64		saved_parallel_vacuum_worker_delay_ns;
	void	   *saved_parallel_vacuum_shared_cost_params;
	uint32		saved_parallel_vacuum_shared_params_generation_local;
	pg_atomic_uint32 shared_cost_balance1;
	pg_atomic_uint32 active_nworkers1;
	pg_atomic_uint32 shared_cost_balance2;
	pg_atomic_uint32 active_nworkers2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_vacuum_in_progress = *PgCurrentVacuumInProgressRef();
	saved_vacuum_cost_balance = VacuumCostBalance;
	saved_vacuum_cost_active = VacuumCostActive;
	saved_vacuum_shared_cost_balance = VacuumSharedCostBalance;
	saved_vacuum_active_nworkers = VacuumActiveNWorkers;
	saved_vacuum_cost_balance_local = VacuumCostBalanceLocal;
	saved_vacuum_failsafe_active = VacuumFailsafeActive;
	saved_parallel_vacuum_worker_delay_ns = parallel_vacuum_worker_delay_ns;
	saved_parallel_vacuum_shared_cost_params =
		*PgCurrentParallelVacuumSharedCostParamsRef();
	saved_parallel_vacuum_shared_params_generation_local =
		*PgCurrentParallelVacuumSharedParamsGenerationLocalRef();

	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);
	pg_atomic_init_u32(&shared_cost_balance1, 111);
	pg_atomic_init_u32(&active_nworkers1, 1);
	pg_atomic_init_u32(&shared_cost_balance2, 222);
	pg_atomic_init_u32(&active_nworkers2, 2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentVacuumInProgressRef() = true;
		VacuumCostBalance = 101;
		VacuumCostActive = true;
		VacuumSharedCostBalance = &shared_cost_balance1;
		VacuumActiveNWorkers = &active_nworkers1;
		VacuumCostBalanceLocal = 17;
		VacuumFailsafeActive = true;
		parallel_vacuum_worker_delay_ns = 1001;
		*PgCurrentParallelVacuumSharedCostParamsRef() = &shared_cost_balance1;
		*PgCurrentParallelVacuumSharedParamsGenerationLocalRef() = 13;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && !*PgCurrentVacuumInProgressRef();
		ok = ok && VacuumCostBalance == 0;
		ok = ok && !VacuumCostActive;
		ok = ok && VacuumSharedCostBalance == NULL;
		ok = ok && VacuumActiveNWorkers == NULL;
		ok = ok && VacuumCostBalanceLocal == 0;
		ok = ok && !VacuumFailsafeActive;
		ok = ok && parallel_vacuum_worker_delay_ns == 0;
		ok = ok && *PgCurrentParallelVacuumSharedCostParamsRef() == NULL;
		ok = ok && *PgCurrentParallelVacuumSharedParamsGenerationLocalRef() == 0;
		VacuumCostBalance = 202;
		VacuumSharedCostBalance = &shared_cost_balance2;
		VacuumActiveNWorkers = &active_nworkers2;
		VacuumCostBalanceLocal = 29;
		parallel_vacuum_worker_delay_ns = 2002;
		*PgCurrentParallelVacuumSharedCostParamsRef() = &shared_cost_balance2;
		*PgCurrentParallelVacuumSharedParamsGenerationLocalRef() = 31;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentVacuumInProgressRef();
		ok = ok && VacuumCostBalance == 101;
		ok = ok && VacuumCostActive;
		ok = ok && VacuumSharedCostBalance == &shared_cost_balance1;
		ok = ok && VacuumActiveNWorkers == &active_nworkers1;
		ok = ok && VacuumCostBalanceLocal == 17;
		ok = ok && VacuumFailsafeActive;
		ok = ok && parallel_vacuum_worker_delay_ns == 1001;
		ok = ok &&
			*PgCurrentParallelVacuumSharedCostParamsRef() == &shared_cost_balance1;
		ok = ok &&
			*PgCurrentParallelVacuumSharedParamsGenerationLocalRef() == 13;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && !*PgCurrentVacuumInProgressRef();
		ok = ok && VacuumCostBalance == 202;
		ok = ok && !VacuumCostActive;
		ok = ok && VacuumSharedCostBalance == &shared_cost_balance2;
		ok = ok && VacuumActiveNWorkers == &active_nworkers2;
		ok = ok && VacuumCostBalanceLocal == 29;
		ok = ok && !VacuumFailsafeActive;
		ok = ok && parallel_vacuum_worker_delay_ns == 2002;
		ok = ok &&
			*PgCurrentParallelVacuumSharedCostParamsRef() == &shared_cost_balance2;
		ok = ok &&
			*PgCurrentParallelVacuumSharedParamsGenerationLocalRef() == 31;

		PgSetCurrentExecution(saved_execution);
		*PgCurrentVacuumInProgressRef() = saved_vacuum_in_progress;
		VacuumCostBalance = saved_vacuum_cost_balance;
		VacuumCostActive = saved_vacuum_cost_active;
		VacuumSharedCostBalance = saved_vacuum_shared_cost_balance;
		VacuumActiveNWorkers = saved_vacuum_active_nworkers;
		VacuumCostBalanceLocal = saved_vacuum_cost_balance_local;
		VacuumFailsafeActive = saved_vacuum_failsafe_active;
		parallel_vacuum_worker_delay_ns = saved_parallel_vacuum_worker_delay_ns;
		*PgCurrentParallelVacuumSharedCostParamsRef() =
			saved_parallel_vacuum_shared_cost_params;
		*PgCurrentParallelVacuumSharedParamsGenerationLocalRef() =
			saved_parallel_vacuum_shared_params_generation_local;
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		*PgCurrentVacuumInProgressRef() = saved_vacuum_in_progress;
		VacuumCostBalance = saved_vacuum_cost_balance;
		VacuumCostActive = saved_vacuum_cost_active;
		VacuumSharedCostBalance = saved_vacuum_shared_cost_balance;
		VacuumActiveNWorkers = saved_vacuum_active_nworkers;
		VacuumCostBalanceLocal = saved_vacuum_cost_balance_local;
		VacuumFailsafeActive = saved_vacuum_failsafe_active;
		parallel_vacuum_worker_delay_ns = saved_parallel_vacuum_worker_delay_ns;
		*PgCurrentParallelVacuumSharedCostParamsRef() =
			saved_parallel_vacuum_shared_cost_params;
		*PgCurrentParallelVacuumSharedParamsGenerationLocalRef() =
			saved_parallel_vacuum_shared_params_generation_local;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "vacuum execution state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_node_io_state_is_execution_local);
Datum
test_execution_node_io_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	bool		saved_write_location_fields;
	const char *saved_strtok_ptr;
	bool		saved_restore_location_fields;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_write_location_fields = *PgCurrentNodeWriteLocationFieldsRef();
	saved_strtok_ptr = *PgCurrentNodeReadStrtokPtrRef();
	saved_restore_location_fields = *PgCurrentNodeRestoreLocationFieldsRef();
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentNodeWriteLocationFieldsRef() = true;
		*PgCurrentNodeReadStrtokPtrRef() = "node io one";
		*PgCurrentNodeRestoreLocationFieldsRef() = true;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && !*PgCurrentNodeWriteLocationFieldsRef();
		ok = ok && *PgCurrentNodeReadStrtokPtrRef() == NULL;
		ok = ok && !*PgCurrentNodeRestoreLocationFieldsRef();
		*PgCurrentNodeReadStrtokPtrRef() = "node io two";

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentNodeWriteLocationFieldsRef();
		ok = ok &&
			strcmp(*PgCurrentNodeReadStrtokPtrRef(), "node io one") == 0;
		ok = ok && *PgCurrentNodeRestoreLocationFieldsRef();

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && !*PgCurrentNodeWriteLocationFieldsRef();
		ok = ok &&
			strcmp(*PgCurrentNodeReadStrtokPtrRef(), "node io two") == 0;
		ok = ok && !*PgCurrentNodeRestoreLocationFieldsRef();

		PgSetCurrentExecution(saved_execution);
		*PgCurrentNodeWriteLocationFieldsRef() = saved_write_location_fields;
		*PgCurrentNodeReadStrtokPtrRef() = saved_strtok_ptr;
		*PgCurrentNodeRestoreLocationFieldsRef() = saved_restore_location_fields;
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		*PgCurrentNodeWriteLocationFieldsRef() = saved_write_location_fields;
		*PgCurrentNodeReadStrtokPtrRef() = saved_strtok_ptr;
		*PgCurrentNodeRestoreLocationFieldsRef() = saved_restore_location_fields;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "node I/O state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_basebackup_state_is_execution_local);
Datum
test_execution_basebackup_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	bool		saved_backup_started_in_recovery;
	long long int saved_total_checksum_failures;
	bool		saved_noverify_checksums;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_backup_started_in_recovery =
		*PgCurrentBaseBackupStartedInRecoveryRef();
	saved_total_checksum_failures =
		*PgCurrentBaseBackupTotalChecksumFailuresRef();
	saved_noverify_checksums = *PgCurrentBaseBackupNoVerifyChecksumsRef();
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentBaseBackupStartedInRecoveryRef() = true;
		*PgCurrentBaseBackupTotalChecksumFailuresRef() = 17;
		*PgCurrentBaseBackupNoVerifyChecksumsRef() = true;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && !*PgCurrentBaseBackupStartedInRecoveryRef();
		ok = ok && *PgCurrentBaseBackupTotalChecksumFailuresRef() == 0;
		ok = ok && !*PgCurrentBaseBackupNoVerifyChecksumsRef();
		*PgCurrentBaseBackupTotalChecksumFailuresRef() = 29;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentBaseBackupStartedInRecoveryRef();
		ok = ok && *PgCurrentBaseBackupTotalChecksumFailuresRef() == 17;
		ok = ok && *PgCurrentBaseBackupNoVerifyChecksumsRef();

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && !*PgCurrentBaseBackupStartedInRecoveryRef();
		ok = ok && *PgCurrentBaseBackupTotalChecksumFailuresRef() == 29;
		ok = ok && !*PgCurrentBaseBackupNoVerifyChecksumsRef();

		PgSetCurrentExecution(saved_execution);
		*PgCurrentBaseBackupStartedInRecoveryRef() =
			saved_backup_started_in_recovery;
		*PgCurrentBaseBackupTotalChecksumFailuresRef() =
			saved_total_checksum_failures;
		*PgCurrentBaseBackupNoVerifyChecksumsRef() = saved_noverify_checksums;
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		*PgCurrentBaseBackupStartedInRecoveryRef() =
			saved_backup_started_in_recovery;
		*PgCurrentBaseBackupTotalChecksumFailuresRef() =
			saved_total_checksum_failures;
		*PgCurrentBaseBackupNoVerifyChecksumsRef() = saved_noverify_checksums;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "basebackup state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_analyze_state_is_execution_local);
Datum
test_execution_analyze_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	MemoryContext saved_analyze_context;
	BufferAccessStrategy saved_analyze_strategy;
	MemoryContext fake_context1;
	MemoryContext fake_context2;
	BufferAccessStrategy fake_strategy1;
	BufferAccessStrategy fake_strategy2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_analyze_context = *PgCurrentAnalyzeContextRef();
	saved_analyze_strategy = *PgCurrentAnalyzeStrategyRef();
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);
	fake_context1 = (MemoryContext) &fake_execution1;
	fake_context2 = (MemoryContext) &fake_execution2;
	fake_strategy1 = (BufferAccessStrategy) &fake_execution1;
	fake_strategy2 = (BufferAccessStrategy) &fake_execution2;

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentAnalyzeContextRef() = fake_context1;
		*PgCurrentAnalyzeStrategyRef() = fake_strategy1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentAnalyzeContextRef() == NULL;
		ok = ok && *PgCurrentAnalyzeStrategyRef() == NULL;
		*PgCurrentAnalyzeContextRef() = fake_context2;
		*PgCurrentAnalyzeStrategyRef() = fake_strategy2;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentAnalyzeContextRef() == fake_context1;
		ok = ok && *PgCurrentAnalyzeStrategyRef() == fake_strategy1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentAnalyzeContextRef() == fake_context2;
		ok = ok && *PgCurrentAnalyzeStrategyRef() == fake_strategy2;

		PgSetCurrentExecution(saved_execution);
		*PgCurrentAnalyzeContextRef() = saved_analyze_context;
		*PgCurrentAnalyzeStrategyRef() = saved_analyze_strategy;
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		*PgCurrentAnalyzeContextRef() = saved_analyze_context;
		*PgCurrentAnalyzeStrategyRef() = saved_analyze_strategy;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "ANALYZE state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_extension_state_is_execution_local);
Datum
test_execution_extension_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	bool		saved_creating_extension;
	Oid			saved_current_extension_object;
	PgExecutionDebugHandler saved_pgcrypto_debug_handler;
	const char *private_key = "test_backend_runtime.execution_private";
	void	  **private_slot;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_creating_extension = creating_extension;
	saved_current_extension_object = CurrentExtensionObject;
	saved_pgcrypto_debug_handler = *PgCurrentPgcryptoDebugHandlerRef();
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		creating_extension = true;
		CurrentExtensionObject = 12345;
		ok = ok && PgExecutionGetExtensionPrivateState(private_key) == NULL;
		private_slot = (void **)
			PgExecutionEnsureExtensionPrivateState(private_key,
												   sizeof(void *),
												   NULL);
		*private_slot = &fake_execution1;
		*PgCurrentPgcryptoDebugHandlerRef() =
			test_backend_runtime_debug_handler1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && !creating_extension;
		ok = ok && CurrentExtensionObject == InvalidOid;
		ok = ok && PgExecutionGetExtensionPrivateState(private_key) == NULL;
		ok = ok && *PgCurrentPgcryptoDebugHandlerRef() == NULL;
		CurrentExtensionObject = 67890;
		private_slot = (void **)
			PgExecutionEnsureExtensionPrivateState(private_key,
												   sizeof(void *),
												   NULL);
		*private_slot = &fake_execution2;
		*PgCurrentPgcryptoDebugHandlerRef() =
			test_backend_runtime_debug_handler2;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && creating_extension;
		ok = ok && CurrentExtensionObject == 12345;
		private_slot = (void **) PgExecutionGetExtensionPrivateState(private_key);
		ok = ok && private_slot != NULL && *private_slot == &fake_execution1;
		ok = ok && *PgCurrentPgcryptoDebugHandlerRef() ==
			test_backend_runtime_debug_handler1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && !creating_extension;
		ok = ok && CurrentExtensionObject == 67890;
		private_slot = (void **) PgExecutionGetExtensionPrivateState(private_key);
		ok = ok && private_slot != NULL && *private_slot == &fake_execution2;
		ok = ok && *PgCurrentPgcryptoDebugHandlerRef() ==
			test_backend_runtime_debug_handler2;

		PgSetCurrentExecution(saved_execution);
		creating_extension = saved_creating_extension;
		CurrentExtensionObject = saved_current_extension_object;
		*PgCurrentPgcryptoDebugHandlerRef() = saved_pgcrypto_debug_handler;
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		creating_extension = saved_creating_extension;
		CurrentExtensionObject = saved_current_extension_object;
		*PgCurrentPgcryptoDebugHandlerRef() = saved_pgcrypto_debug_handler;
		PG_RE_THROW();
	}
	PG_END_TRY();

	PgExecutionInitializeExtensionState(&fake_execution2.extension);
	PgSetCurrentExecution(&fake_execution2);
	ok = ok && PgExecutionGetExtensionPrivateState(private_key) == NULL;
	ok = ok && *PgCurrentPgcryptoDebugHandlerRef() == NULL;
	PgSetCurrentExecution(saved_execution);

	if (!ok)
		elog(ERROR, "extension state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_matview_state_is_execution_local);
Datum
test_execution_matview_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	int			saved_maintenance_depth;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_maintenance_depth = *PgCurrentMatViewMaintenanceDepthRef();
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentMatViewMaintenanceDepthRef() = 2;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentMatViewMaintenanceDepthRef() == 0;
		*PgCurrentMatViewMaintenanceDepthRef() = 5;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentMatViewMaintenanceDepthRef() == 2;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentMatViewMaintenanceDepthRef() == 5;

		PgSetCurrentExecution(saved_execution);
		*PgCurrentMatViewMaintenanceDepthRef() = saved_maintenance_depth;
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		*PgCurrentMatViewMaintenanceDepthRef() = saved_maintenance_depth;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "materialized view state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_snapshot_combo_state_is_execution_local);
Datum
test_execution_snapshot_combo_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		PgCurrentSnapshotDataRef()->snapshot_type = SNAPSHOT_SELF;
		PgCurrentSecondarySnapshotDataRef()->snapshot_type = SNAPSHOT_ANY;
		PgCurrentCatalogSnapshotDataRef()->snapshot_type = SNAPSHOT_TOAST;
		*PgCurrentSnapshotRef() = (Snapshot) &fake_execution1;
		*PgCurrentSecondarySnapshotRef() = (Snapshot) &fake_execution1.session;
		*PgCurrentCatalogSnapshotRef() = (Snapshot) &fake_execution1.backend;
		*PgCurrentHistoricSnapshotRef() = (Snapshot) &fake_execution1.carrier;
		*PgCurrentTransactionXminRef() = 101;
		*PgCurrentRecentXminRef() = 102;
		*PgCurrentTupleCidDataRef() = (HTAB *) &fake_execution1;
		*PgCurrentActiveSnapshotRef() = &fake_execution1;
		PgCurrentRegisteredSnapshotsRef()->ph_arg = &fake_execution1;
		PgCurrentRegisteredSnapshotsRef()->ph_root =
			(pairingheap_node *) &fake_execution1;
		*PgCurrentFirstSnapshotSetRef() = true;
		*PgCurrentFirstXactSnapshotRef() = (Snapshot) &fake_execution1;
		*PgCurrentExportedSnapshotsRef() = (List *) &fake_execution1;
		*PgCurrentComboCidHashRef() = (HTAB *) &fake_execution1;
		*PgCurrentComboCidsRef() = &fake_execution1;
		*PgCurrentUsedComboCidsRef() = 103;
		*PgCurrentSizeComboCidsRef() = 104;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && PgCurrentSnapshotDataRef()->snapshot_type == SNAPSHOT_MVCC;
		ok = ok && PgCurrentSecondarySnapshotDataRef()->snapshot_type == SNAPSHOT_MVCC;
		ok = ok && PgCurrentCatalogSnapshotDataRef()->snapshot_type == SNAPSHOT_MVCC;
		ok = ok && *PgCurrentSnapshotRef() == NULL;
		ok = ok && *PgCurrentSecondarySnapshotRef() == NULL;
		ok = ok && *PgCurrentCatalogSnapshotRef() == NULL;
		ok = ok && *PgCurrentHistoricSnapshotRef() == NULL;
		ok = ok && *PgCurrentTransactionXminRef() == 0;
		ok = ok && *PgCurrentRecentXminRef() == 0;
		ok = ok && *PgCurrentTupleCidDataRef() == NULL;
		ok = ok && *PgCurrentActiveSnapshotRef() == NULL;
		ok = ok && PgCurrentRegisteredSnapshotsRef()->ph_arg == NULL;
		ok = ok && PgCurrentRegisteredSnapshotsRef()->ph_root == NULL;
		ok = ok && !*PgCurrentFirstSnapshotSetRef();
		ok = ok && *PgCurrentFirstXactSnapshotRef() == NULL;
		ok = ok && *PgCurrentExportedSnapshotsRef() == NIL;
		ok = ok && *PgCurrentComboCidHashRef() == NULL;
		ok = ok && *PgCurrentComboCidsRef() == NULL;
		ok = ok && *PgCurrentUsedComboCidsRef() == 0;
		ok = ok && *PgCurrentSizeComboCidsRef() == 0;

		PgCurrentSnapshotDataRef()->snapshot_type = SNAPSHOT_HISTORIC_MVCC;
		PgCurrentSecondarySnapshotDataRef()->snapshot_type = SNAPSHOT_NON_VACUUMABLE;
		PgCurrentCatalogSnapshotDataRef()->snapshot_type = SNAPSHOT_DIRTY;
		*PgCurrentSnapshotRef() = (Snapshot) &fake_execution2;
		*PgCurrentSecondarySnapshotRef() = (Snapshot) &fake_execution2.session;
		*PgCurrentCatalogSnapshotRef() = (Snapshot) &fake_execution2.backend;
		*PgCurrentHistoricSnapshotRef() = (Snapshot) &fake_execution2.carrier;
		*PgCurrentTransactionXminRef() = 201;
		*PgCurrentRecentXminRef() = 202;
		*PgCurrentTupleCidDataRef() = (HTAB *) &fake_execution2;
		*PgCurrentActiveSnapshotRef() = &fake_execution2;
		PgCurrentRegisteredSnapshotsRef()->ph_arg = &fake_execution2;
		PgCurrentRegisteredSnapshotsRef()->ph_root =
			(pairingheap_node *) &fake_execution2;
		*PgCurrentFirstSnapshotSetRef() = false;
		*PgCurrentFirstXactSnapshotRef() = (Snapshot) &fake_execution2;
		*PgCurrentExportedSnapshotsRef() = (List *) &fake_execution2;
		*PgCurrentComboCidHashRef() = (HTAB *) &fake_execution2;
		*PgCurrentComboCidsRef() = &fake_execution2;
		*PgCurrentUsedComboCidsRef() = 203;
		*PgCurrentSizeComboCidsRef() = 204;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && PgCurrentSnapshotDataRef()->snapshot_type == SNAPSHOT_SELF;
		ok = ok && PgCurrentSecondarySnapshotDataRef()->snapshot_type == SNAPSHOT_ANY;
		ok = ok && PgCurrentCatalogSnapshotDataRef()->snapshot_type == SNAPSHOT_TOAST;
		ok = ok && *PgCurrentSnapshotRef() == (Snapshot) &fake_execution1;
		ok = ok && *PgCurrentSecondarySnapshotRef() == (Snapshot) &fake_execution1.session;
		ok = ok && *PgCurrentCatalogSnapshotRef() == (Snapshot) &fake_execution1.backend;
		ok = ok && *PgCurrentHistoricSnapshotRef() == (Snapshot) &fake_execution1.carrier;
		ok = ok && *PgCurrentTransactionXminRef() == 101;
		ok = ok && *PgCurrentRecentXminRef() == 102;
		ok = ok && *PgCurrentTupleCidDataRef() == (HTAB *) &fake_execution1;
		ok = ok && *PgCurrentActiveSnapshotRef() == &fake_execution1;
		ok = ok && PgCurrentRegisteredSnapshotsRef()->ph_arg == &fake_execution1;
		ok = ok && PgCurrentRegisteredSnapshotsRef()->ph_root ==
			(pairingheap_node *) &fake_execution1;
		ok = ok && *PgCurrentFirstSnapshotSetRef();
		ok = ok && *PgCurrentFirstXactSnapshotRef() == (Snapshot) &fake_execution1;
		ok = ok && *PgCurrentExportedSnapshotsRef() == (List *) &fake_execution1;
		ok = ok && *PgCurrentComboCidHashRef() == (HTAB *) &fake_execution1;
		ok = ok && *PgCurrentComboCidsRef() == &fake_execution1;
		ok = ok && *PgCurrentUsedComboCidsRef() == 103;
		ok = ok && *PgCurrentSizeComboCidsRef() == 104;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && PgCurrentSnapshotDataRef()->snapshot_type == SNAPSHOT_HISTORIC_MVCC;
		ok = ok && PgCurrentSecondarySnapshotDataRef()->snapshot_type == SNAPSHOT_NON_VACUUMABLE;
		ok = ok && PgCurrentCatalogSnapshotDataRef()->snapshot_type == SNAPSHOT_DIRTY;
		ok = ok && *PgCurrentSnapshotRef() == (Snapshot) &fake_execution2;
		ok = ok && *PgCurrentSecondarySnapshotRef() == (Snapshot) &fake_execution2.session;
		ok = ok && *PgCurrentCatalogSnapshotRef() == (Snapshot) &fake_execution2.backend;
		ok = ok && *PgCurrentHistoricSnapshotRef() == (Snapshot) &fake_execution2.carrier;
		ok = ok && *PgCurrentTransactionXminRef() == 201;
		ok = ok && *PgCurrentRecentXminRef() == 202;
		ok = ok && *PgCurrentTupleCidDataRef() == (HTAB *) &fake_execution2;
		ok = ok && *PgCurrentActiveSnapshotRef() == &fake_execution2;
		ok = ok && PgCurrentRegisteredSnapshotsRef()->ph_arg == &fake_execution2;
		ok = ok && PgCurrentRegisteredSnapshotsRef()->ph_root ==
			(pairingheap_node *) &fake_execution2;
		ok = ok && !*PgCurrentFirstSnapshotSetRef();
		ok = ok && *PgCurrentFirstXactSnapshotRef() == (Snapshot) &fake_execution2;
		ok = ok && *PgCurrentExportedSnapshotsRef() == (List *) &fake_execution2;
		ok = ok && *PgCurrentComboCidHashRef() == (HTAB *) &fake_execution2;
		ok = ok && *PgCurrentComboCidsRef() == &fake_execution2;
		ok = ok && *PgCurrentUsedComboCidsRef() == 203;
		ok = ok && *PgCurrentSizeComboCidsRef() == 204;

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "execution snapshot/combo CID state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_xloginsert_state_is_execution_local);
Datum
test_execution_xloginsert_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	XLogRecData fake_rdata1;
	XLogRecData fake_rdata2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);
	MemSet(&fake_rdata1, 0, sizeof(fake_rdata1));
	MemSet(&fake_rdata2, 0, sizeof(fake_rdata2));

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentXLogInsertRegisteredBuffersRef() = &fake_execution1;
		*PgCurrentXLogInsertMaxRegisteredBuffersRef() = 101;
		*PgCurrentXLogInsertMaxRegisteredBlockIdRef() = 102;
		*PgCurrentXLogInsertMainRDataHeadRef() = &fake_rdata1;
		*PgCurrentXLogInsertMainRDataLastRef() = &fake_rdata1;
		*PgCurrentXLogInsertMainRDataLenRef() = UINT64CONST(103);
		*PgCurrentXLogInsertFlagsRef() = 104;
		PgCurrentXLogInsertHeaderRecordDataRef()->data = &fake_execution1;
		PgCurrentXLogInsertHeaderRecordDataRef()->len = 105;
		*PgCurrentXLogInsertHeaderScratchRef() = (char *) &fake_execution1;
		*PgCurrentXLogInsertRDatasRef() = &fake_rdata1;
		*PgCurrentXLogInsertNumRDatasRef() = 106;
		*PgCurrentXLogInsertMaxRDatasRef() = 107;
		*PgCurrentXLogInsertBeginCalledRef() = true;
		*PgCurrentXLogInsertContextRef() = (MemoryContext) &fake_execution1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentXLogInsertRegisteredBuffersRef() == NULL;
		ok = ok && *PgCurrentXLogInsertMaxRegisteredBuffersRef() == 0;
		ok = ok && *PgCurrentXLogInsertMaxRegisteredBlockIdRef() == 0;
		ok = ok && *PgCurrentXLogInsertMainRDataHeadRef() == NULL;
		ok = ok && *PgCurrentXLogInsertMainRDataLastRef() == NULL;
		ok = ok && *PgCurrentXLogInsertMainRDataLenRef() == 0;
		ok = ok && *PgCurrentXLogInsertFlagsRef() == 0;
		ok = ok && PgCurrentXLogInsertHeaderRecordDataRef()->data == NULL;
		ok = ok && PgCurrentXLogInsertHeaderRecordDataRef()->len == 0;
		ok = ok && *PgCurrentXLogInsertHeaderScratchRef() == NULL;
		ok = ok && *PgCurrentXLogInsertRDatasRef() == NULL;
		ok = ok && *PgCurrentXLogInsertNumRDatasRef() == 0;
		ok = ok && *PgCurrentXLogInsertMaxRDatasRef() == 0;
		ok = ok && !*PgCurrentXLogInsertBeginCalledRef();
		ok = ok && *PgCurrentXLogInsertContextRef() == NULL;

		*PgCurrentXLogInsertRegisteredBuffersRef() = &fake_execution2;
		*PgCurrentXLogInsertMaxRegisteredBuffersRef() = 201;
		*PgCurrentXLogInsertMaxRegisteredBlockIdRef() = 202;
		*PgCurrentXLogInsertMainRDataHeadRef() = &fake_rdata2;
		*PgCurrentXLogInsertMainRDataLastRef() = &fake_rdata2;
		*PgCurrentXLogInsertMainRDataLenRef() = UINT64CONST(203);
		*PgCurrentXLogInsertFlagsRef() = 204;
		PgCurrentXLogInsertHeaderRecordDataRef()->data = &fake_execution2;
		PgCurrentXLogInsertHeaderRecordDataRef()->len = 205;
		*PgCurrentXLogInsertHeaderScratchRef() = (char *) &fake_execution2;
		*PgCurrentXLogInsertRDatasRef() = &fake_rdata2;
		*PgCurrentXLogInsertNumRDatasRef() = 206;
		*PgCurrentXLogInsertMaxRDatasRef() = 207;
		*PgCurrentXLogInsertBeginCalledRef() = false;
		*PgCurrentXLogInsertContextRef() = (MemoryContext) &fake_execution2;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentXLogInsertRegisteredBuffersRef() == &fake_execution1;
		ok = ok && *PgCurrentXLogInsertMaxRegisteredBuffersRef() == 101;
		ok = ok && *PgCurrentXLogInsertMaxRegisteredBlockIdRef() == 102;
		ok = ok && *PgCurrentXLogInsertMainRDataHeadRef() == &fake_rdata1;
		ok = ok && *PgCurrentXLogInsertMainRDataLastRef() == &fake_rdata1;
		ok = ok && *PgCurrentXLogInsertMainRDataLenRef() == UINT64CONST(103);
		ok = ok && *PgCurrentXLogInsertFlagsRef() == 104;
		ok = ok && PgCurrentXLogInsertHeaderRecordDataRef()->data ==
			&fake_execution1;
		ok = ok && PgCurrentXLogInsertHeaderRecordDataRef()->len == 105;
		ok = ok && *PgCurrentXLogInsertHeaderScratchRef() ==
			(char *) &fake_execution1;
		ok = ok && *PgCurrentXLogInsertRDatasRef() == &fake_rdata1;
		ok = ok && *PgCurrentXLogInsertNumRDatasRef() == 106;
		ok = ok && *PgCurrentXLogInsertMaxRDatasRef() == 107;
		ok = ok && *PgCurrentXLogInsertBeginCalledRef();
		ok = ok && *PgCurrentXLogInsertContextRef() ==
			(MemoryContext) &fake_execution1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentXLogInsertRegisteredBuffersRef() == &fake_execution2;
		ok = ok && *PgCurrentXLogInsertMaxRegisteredBuffersRef() == 201;
		ok = ok && *PgCurrentXLogInsertMaxRegisteredBlockIdRef() == 202;
		ok = ok && *PgCurrentXLogInsertMainRDataHeadRef() == &fake_rdata2;
		ok = ok && *PgCurrentXLogInsertMainRDataLastRef() == &fake_rdata2;
		ok = ok && *PgCurrentXLogInsertMainRDataLenRef() == UINT64CONST(203);
		ok = ok && *PgCurrentXLogInsertFlagsRef() == 204;
		ok = ok && PgCurrentXLogInsertHeaderRecordDataRef()->data ==
			&fake_execution2;
		ok = ok && PgCurrentXLogInsertHeaderRecordDataRef()->len == 205;
		ok = ok && *PgCurrentXLogInsertHeaderScratchRef() ==
			(char *) &fake_execution2;
		ok = ok && *PgCurrentXLogInsertRDatasRef() == &fake_rdata2;
		ok = ok && *PgCurrentXLogInsertNumRDatasRef() == 206;
		ok = ok && *PgCurrentXLogInsertMaxRDatasRef() == 207;
		ok = ok && !*PgCurrentXLogInsertBeginCalledRef();
		ok = ok && *PgCurrentXLogInsertContextRef() ==
			(MemoryContext) &fake_execution2;

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "WAL insert construction state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_xact_state_is_execution_local);
Datum
test_execution_xact_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	TransactionId parallel_xids1[2] = {11, 12};
	TransactionId parallel_xids2[1] = {21};
	char		prepare_gid1[] = "gid-one";
	char		prepare_gid2[] = "gid-two";
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		XactIsoLevel = XACT_SERIALIZABLE;
		XactReadOnly = true;
		XactDeferrable = true;
		xact_is_sampled = true;
		CheckXidAlive = 101;
		bsysscan = true;
		MyXactFlags = XACT_FLAGS_ACCESSEDTEMPNAMESPACE |
			XACT_FLAGS_NEEDIMMEDIATECOMMIT;
		*PgCurrentXactTopFullTransactionIdRef() =
			FullTransactionIdFromEpochAndXid(1, 101);
		*PgCurrentNParallelCurrentXidsRef() = 2;
		*PgCurrentParallelCurrentXidsRef() = parallel_xids1;
		*PgCurrentNUnreportedXidsRef() = 2;
		PgCurrentUnreportedXids()[0] = 111;
		PgCurrentUnreportedXids()[1] = 112;
		*PgCurrentSubTransactionIdCounterRef() = 5;
		*PgCurrentCommandIdCounterRef() = 6;
		*PgCurrentCommandIdUsedRef() = true;
		*PgCurrentXactStartTimestampRef() = 1001;
		*PgCurrentStmtStartTimestampRef() = 1002;
		*PgCurrentXactStopTimestampRef() = 1003;
		*PgCurrentPrepareGIDRef() = prepare_gid1;
		*PgCurrentForceSyncCommitRef() = true;
		*PgCurrentTransactionAbortContextRef() =
			(MemoryContext) &fake_execution1;
		*PgCurrentTopTransactionStateDataRef() =
			(TransactionStateData *) &fake_execution1;
		*PgCurrentTransactionStateRef() =
			(TransactionStateData *) &fake_execution1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && XactIsoLevel == 0;
		ok = ok && !XactReadOnly;
		ok = ok && !XactDeferrable;
		ok = ok && !xact_is_sampled;
		ok = ok && CheckXidAlive == InvalidTransactionId;
		ok = ok && !bsysscan;
		ok = ok && MyXactFlags == 0;
		ok = ok &&
			FullTransactionIdEquals(*PgCurrentXactTopFullTransactionIdRef(),
									FullTransactionIdFromU64(0));
		ok = ok && *PgCurrentNParallelCurrentXidsRef() == 0;
		ok = ok && *PgCurrentParallelCurrentXidsRef() == NULL;
		ok = ok && *PgCurrentNUnreportedXidsRef() == 0;
		ok = ok && PgCurrentUnreportedXids()[0] == InvalidTransactionId;
		ok = ok && *PgCurrentSubTransactionIdCounterRef() == 0;
		ok = ok && *PgCurrentCommandIdCounterRef() == 0;
		ok = ok && !*PgCurrentCommandIdUsedRef();
		ok = ok && *PgCurrentXactStartTimestampRef() == 0;
		ok = ok && *PgCurrentStmtStartTimestampRef() == 0;
		ok = ok && *PgCurrentXactStopTimestampRef() == 0;
		ok = ok && *PgCurrentPrepareGIDRef() == NULL;
		ok = ok && !*PgCurrentForceSyncCommitRef();
		ok = ok && *PgCurrentTransactionAbortContextRef() == NULL;
		ok = ok && *PgCurrentTopTransactionStateDataRef() == NULL;
		ok = ok && *PgCurrentTransactionStateRef() == NULL;

		XactIsoLevel = XACT_REPEATABLE_READ;
		XactReadOnly = false;
		XactDeferrable = false;
		xact_is_sampled = false;
		CheckXidAlive = 201;
		bsysscan = false;
		MyXactFlags = XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK |
			XACT_FLAGS_PIPELINING;
		*PgCurrentXactTopFullTransactionIdRef() =
			FullTransactionIdFromEpochAndXid(2, 201);
		*PgCurrentNParallelCurrentXidsRef() = 1;
		*PgCurrentParallelCurrentXidsRef() = parallel_xids2;
		*PgCurrentNUnreportedXidsRef() = 1;
		PgCurrentUnreportedXids()[0] = 211;
		PgCurrentUnreportedXids()[1] = InvalidTransactionId;
		*PgCurrentSubTransactionIdCounterRef() = 7;
		*PgCurrentCommandIdCounterRef() = 8;
		*PgCurrentCommandIdUsedRef() = false;
		*PgCurrentXactStartTimestampRef() = 2001;
		*PgCurrentStmtStartTimestampRef() = 2002;
		*PgCurrentXactStopTimestampRef() = 2003;
		*PgCurrentPrepareGIDRef() = prepare_gid2;
		*PgCurrentForceSyncCommitRef() = false;
		*PgCurrentTransactionAbortContextRef() =
			(MemoryContext) &fake_execution2;
		*PgCurrentTopTransactionStateDataRef() =
			(TransactionStateData *) &fake_execution2;
		*PgCurrentTransactionStateRef() =
			(TransactionStateData *) &fake_execution2;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && XactIsoLevel == XACT_SERIALIZABLE;
		ok = ok && XactReadOnly;
		ok = ok && XactDeferrable;
		ok = ok && xact_is_sampled;
		ok = ok && CheckXidAlive == 101;
		ok = ok && bsysscan;
		ok = ok && MyXactFlags == (XACT_FLAGS_ACCESSEDTEMPNAMESPACE |
								   XACT_FLAGS_NEEDIMMEDIATECOMMIT);
		ok = ok &&
			FullTransactionIdEquals(*PgCurrentXactTopFullTransactionIdRef(),
									FullTransactionIdFromEpochAndXid(1, 101));
		ok = ok && *PgCurrentNParallelCurrentXidsRef() == 2;
		ok = ok && *PgCurrentParallelCurrentXidsRef() == parallel_xids1;
		ok = ok && *PgCurrentNUnreportedXidsRef() == 2;
		ok = ok && PgCurrentUnreportedXids()[0] == 111;
		ok = ok && PgCurrentUnreportedXids()[1] == 112;
		ok = ok && *PgCurrentSubTransactionIdCounterRef() == 5;
		ok = ok && *PgCurrentCommandIdCounterRef() == 6;
		ok = ok && *PgCurrentCommandIdUsedRef();
		ok = ok && *PgCurrentXactStartTimestampRef() == 1001;
		ok = ok && *PgCurrentStmtStartTimestampRef() == 1002;
		ok = ok && *PgCurrentXactStopTimestampRef() == 1003;
		ok = ok && *PgCurrentPrepareGIDRef() == prepare_gid1;
		ok = ok && *PgCurrentForceSyncCommitRef();
		ok = ok && *PgCurrentTransactionAbortContextRef() ==
			(MemoryContext) &fake_execution1;
		ok = ok && *PgCurrentTopTransactionStateDataRef() ==
			(TransactionStateData *) &fake_execution1;
		ok = ok && *PgCurrentTransactionStateRef() ==
			(TransactionStateData *) &fake_execution1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && XactIsoLevel == XACT_REPEATABLE_READ;
		ok = ok && !XactReadOnly;
		ok = ok && !XactDeferrable;
		ok = ok && !xact_is_sampled;
		ok = ok && CheckXidAlive == 201;
		ok = ok && !bsysscan;
		ok = ok && MyXactFlags == (XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK |
								   XACT_FLAGS_PIPELINING);
		ok = ok &&
			FullTransactionIdEquals(*PgCurrentXactTopFullTransactionIdRef(),
									FullTransactionIdFromEpochAndXid(2, 201));
		ok = ok && *PgCurrentNParallelCurrentXidsRef() == 1;
		ok = ok && *PgCurrentParallelCurrentXidsRef() == parallel_xids2;
		ok = ok && *PgCurrentNUnreportedXidsRef() == 1;
		ok = ok && PgCurrentUnreportedXids()[0] == 211;
		ok = ok && PgCurrentUnreportedXids()[1] == InvalidTransactionId;
		ok = ok && *PgCurrentSubTransactionIdCounterRef() == 7;
		ok = ok && *PgCurrentCommandIdCounterRef() == 8;
		ok = ok && !*PgCurrentCommandIdUsedRef();
		ok = ok && *PgCurrentXactStartTimestampRef() == 2001;
		ok = ok && *PgCurrentStmtStartTimestampRef() == 2002;
		ok = ok && *PgCurrentXactStopTimestampRef() == 2003;
		ok = ok && *PgCurrentPrepareGIDRef() == prepare_gid2;
		ok = ok && !*PgCurrentForceSyncCommitRef();
		ok = ok && *PgCurrentTransactionAbortContextRef() ==
			(MemoryContext) &fake_execution2;
		ok = ok && *PgCurrentTopTransactionStateDataRef() ==
			(TransactionStateData *) &fake_execution2;
		ok = ok && *PgCurrentTransactionStateRef() ==
			(TransactionStateData *) &fake_execution2;

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "transaction execution state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_transaction_cleanup_state_is_execution_local);
Datum
test_execution_transaction_cleanup_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentLargeObjectCookiesRef() =
			(LargeObjectDesc **) &fake_execution1;
		*PgCurrentLargeObjectCookiesSizeRef() = 101;
		*PgCurrentLargeObjectCleanupNeededRef() = true;
		*PgCurrentLargeObjectContextRef() = (MemoryContext) &fake_execution1;
		*PgCurrentHaveXactTemporaryFilesRef() = true;
		*PgCurrentPgStatXactStackRef() =
			(PgStat_SubXactStatus *) &fake_execution1;
		*PgCurrentRIFastPathCacheRef() = (HTAB *) &fake_execution1;
		*PgCurrentRIFastPathCallbackRegisteredRef() = true;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentLargeObjectCookiesRef() == NULL;
		ok = ok && *PgCurrentLargeObjectCookiesSizeRef() == 0;
		ok = ok && !*PgCurrentLargeObjectCleanupNeededRef();
		ok = ok && *PgCurrentLargeObjectContextRef() == NULL;
		ok = ok && !*PgCurrentHaveXactTemporaryFilesRef();
		ok = ok && *PgCurrentPgStatXactStackRef() == NULL;
		ok = ok && *PgCurrentRIFastPathCacheRef() == NULL;
		ok = ok && !*PgCurrentRIFastPathCallbackRegisteredRef();

		*PgCurrentLargeObjectCookiesRef() =
			(LargeObjectDesc **) &fake_execution2;
		*PgCurrentLargeObjectCookiesSizeRef() = 201;
		*PgCurrentLargeObjectCleanupNeededRef() = false;
		*PgCurrentLargeObjectContextRef() = (MemoryContext) &fake_execution2;
		*PgCurrentHaveXactTemporaryFilesRef() = false;
		*PgCurrentPgStatXactStackRef() =
			(PgStat_SubXactStatus *) &fake_execution2;
		*PgCurrentRIFastPathCacheRef() = (HTAB *) &fake_execution2;
		*PgCurrentRIFastPathCallbackRegisteredRef() = false;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentLargeObjectCookiesRef() ==
			(LargeObjectDesc **) &fake_execution1;
		ok = ok && *PgCurrentLargeObjectCookiesSizeRef() == 101;
		ok = ok && *PgCurrentLargeObjectCleanupNeededRef();
		ok = ok && *PgCurrentLargeObjectContextRef() ==
			(MemoryContext) &fake_execution1;
		ok = ok && *PgCurrentHaveXactTemporaryFilesRef();
		ok = ok && *PgCurrentPgStatXactStackRef() ==
			(PgStat_SubXactStatus *) &fake_execution1;
		ok = ok && *PgCurrentRIFastPathCacheRef() == (HTAB *) &fake_execution1;
		ok = ok && *PgCurrentRIFastPathCallbackRegisteredRef();

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentLargeObjectCookiesRef() ==
			(LargeObjectDesc **) &fake_execution2;
		ok = ok && *PgCurrentLargeObjectCookiesSizeRef() == 201;
		ok = ok && !*PgCurrentLargeObjectCleanupNeededRef();
		ok = ok && *PgCurrentLargeObjectContextRef() ==
			(MemoryContext) &fake_execution2;
		ok = ok && !*PgCurrentHaveXactTemporaryFilesRef();
		ok = ok && *PgCurrentPgStatXactStackRef() ==
			(PgStat_SubXactStatus *) &fake_execution2;
		ok = ok && *PgCurrentRIFastPathCacheRef() == (HTAB *) &fake_execution2;
		ok = ok && !*PgCurrentRIFastPathCallbackRegisteredRef();

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "transaction cleanup execution state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_reporting_replication_state_is_execution_local);
Datum
test_execution_reporting_replication_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	ErrorContextCallback callback1;
	ErrorContextCallback callback2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);
	MemSet(&callback1, 0, sizeof(callback1));
	MemSet(&callback2, 0, sizeof(callback2));
	fake_execution1.error.errordata_stack_depth = -1;
	fake_execution1.replication_scratch.replorigin_xact.origin =
		InvalidReplOriginId;
	fake_execution1.replication_scratch.replorigin_xact.origin_lsn =
		InvalidXLogRecPtr;
	fake_execution2.error.errordata_stack_depth = -1;
	fake_execution2.replication_scratch.replorigin_xact.origin =
		InvalidReplOriginId;
	fake_execution2.replication_scratch.replorigin_xact.origin_lsn =
		InvalidXLogRecPtr;

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentErrorDataStackDepthRef() = 1;
		*PgCurrentErrorRecursionDepthRef() = 2;
		PgCurrentErrorDataArray()[1].elevel = ERROR;
		PgCurrentSavedTimevalRef()->tv_sec = 101;
		PgCurrentSavedTimevalRef()->tv_usec = 102;
		*PgCurrentSavedTimevalSetRef() = true;
		strcpy(PgCurrentFormattedLogTime(), "time-one");
		*PgCurrentEventTriggerQueryStateRef() =
			(EventTriggerQueryState *) &fake_execution1;
		PgCurrentReplOriginXactStateRef()->origin = 11;
		PgCurrentReplOriginXactStateRef()->origin_lsn = 12;
		PgCurrentReplOriginXactStateRef()->origin_timestamp = 13;
		*PgCurrentApplyErrorContextStackRef() = &callback1;
		*PgCurrentApplyMessageContextRef() = (MemoryContext) &fake_execution1;
		*PgCurrentLogicalStreamingContextRef() = (MemoryContext) &fake_execution1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentErrorDataStackDepthRef() == -1;
		ok = ok && *PgCurrentErrorRecursionDepthRef() == 0;
		ok = ok && PgCurrentErrorDataArray()[0].elevel == 0;
		ok = ok && PgCurrentSavedTimevalRef()->tv_sec == 0;
		ok = ok && PgCurrentSavedTimevalRef()->tv_usec == 0;
		ok = ok && !*PgCurrentSavedTimevalSetRef();
		ok = ok && PgCurrentFormattedLogTime()[0] == '\0';
		ok = ok && *PgCurrentEventTriggerQueryStateRef() == NULL;
		ok = ok && PgCurrentReplOriginXactStateRef()->origin ==
			InvalidReplOriginId;
		ok = ok && PgCurrentReplOriginXactStateRef()->origin_lsn ==
			InvalidXLogRecPtr;
		ok = ok && PgCurrentReplOriginXactStateRef()->origin_timestamp == 0;
		ok = ok && *PgCurrentApplyErrorContextStackRef() == NULL;
		ok = ok && *PgCurrentApplyMessageContextRef() == NULL;
		ok = ok && *PgCurrentLogicalStreamingContextRef() == NULL;

		*PgCurrentErrorDataStackDepthRef() = 0;
		*PgCurrentErrorRecursionDepthRef() = 3;
		PgCurrentErrorDataArray()[0].elevel = WARNING;
		PgCurrentSavedTimevalRef()->tv_sec = 201;
		PgCurrentSavedTimevalRef()->tv_usec = 202;
		*PgCurrentSavedTimevalSetRef() = false;
		strcpy(PgCurrentFormattedLogTime(), "time-two");
		*PgCurrentEventTriggerQueryStateRef() =
			(EventTriggerQueryState *) &fake_execution2;
		PgCurrentReplOriginXactStateRef()->origin = 21;
		PgCurrentReplOriginXactStateRef()->origin_lsn = 22;
		PgCurrentReplOriginXactStateRef()->origin_timestamp = 23;
		*PgCurrentApplyErrorContextStackRef() = &callback2;
		*PgCurrentApplyMessageContextRef() = (MemoryContext) &fake_execution2;
		*PgCurrentLogicalStreamingContextRef() = (MemoryContext) &fake_execution2;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentErrorDataStackDepthRef() == 1;
		ok = ok && *PgCurrentErrorRecursionDepthRef() == 2;
		ok = ok && PgCurrentErrorDataArray()[1].elevel == ERROR;
		ok = ok && PgCurrentSavedTimevalRef()->tv_sec == 101;
		ok = ok && PgCurrentSavedTimevalRef()->tv_usec == 102;
		ok = ok && *PgCurrentSavedTimevalSetRef();
		ok = ok && strcmp(PgCurrentFormattedLogTime(), "time-one") == 0;
		ok = ok && *PgCurrentEventTriggerQueryStateRef() ==
			(EventTriggerQueryState *) &fake_execution1;
		ok = ok && PgCurrentReplOriginXactStateRef()->origin == 11;
		ok = ok && PgCurrentReplOriginXactStateRef()->origin_lsn == 12;
		ok = ok && PgCurrentReplOriginXactStateRef()->origin_timestamp == 13;
		ok = ok && *PgCurrentApplyErrorContextStackRef() == &callback1;
		ok = ok && *PgCurrentApplyMessageContextRef() ==
			(MemoryContext) &fake_execution1;
		ok = ok && *PgCurrentLogicalStreamingContextRef() ==
			(MemoryContext) &fake_execution1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentErrorDataStackDepthRef() == 0;
		ok = ok && *PgCurrentErrorRecursionDepthRef() == 3;
		ok = ok && PgCurrentErrorDataArray()[0].elevel == WARNING;
		ok = ok && PgCurrentSavedTimevalRef()->tv_sec == 201;
		ok = ok && PgCurrentSavedTimevalRef()->tv_usec == 202;
		ok = ok && !*PgCurrentSavedTimevalSetRef();
		ok = ok && strcmp(PgCurrentFormattedLogTime(), "time-two") == 0;
		ok = ok && *PgCurrentEventTriggerQueryStateRef() ==
			(EventTriggerQueryState *) &fake_execution2;
		ok = ok && PgCurrentReplOriginXactStateRef()->origin == 21;
		ok = ok && PgCurrentReplOriginXactStateRef()->origin_lsn == 22;
		ok = ok && PgCurrentReplOriginXactStateRef()->origin_timestamp == 23;
		ok = ok && *PgCurrentApplyErrorContextStackRef() == &callback2;
		ok = ok && *PgCurrentApplyMessageContextRef() ==
			(MemoryContext) &fake_execution2;
		ok = ok && *PgCurrentLogicalStreamingContextRef() ==
			(MemoryContext) &fake_execution2;

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "reporting/replication execution state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_guc_error_state_is_execution_local);
Datum
test_execution_guc_error_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentGUCCheckErrcodeValueRef() = 101;
		GUC_check_errmsg_string = "message one";
		GUC_check_errdetail_string = "detail one";
		GUC_check_errhint_string = "hint one";
		*PgCurrentFormatErrnumberRef() = 102;
		*PgCurrentFormatDomainRef() = "domain one";
		*PgCurrentConfigFileLinenoRef() = 103;
		*PgCurrentGUCFlexFatalErrmsgRef() = "fatal one";
		*PgCurrentGUCFlexFatalJmpRef() = (sigjmp_buf *) &fake_execution1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentGUCCheckErrcodeValueRef() == 0;
		ok = ok && GUC_check_errmsg_string == NULL;
		ok = ok && GUC_check_errdetail_string == NULL;
		ok = ok && GUC_check_errhint_string == NULL;
		ok = ok && *PgCurrentFormatErrnumberRef() == 0;
		ok = ok && *PgCurrentFormatDomainRef() == NULL;
		ok = ok && *PgCurrentConfigFileLinenoRef() == 0;
		ok = ok && *PgCurrentGUCFlexFatalErrmsgRef() == NULL;
		ok = ok && *PgCurrentGUCFlexFatalJmpRef() == NULL;

		*PgCurrentGUCCheckErrcodeValueRef() = 201;
		GUC_check_errmsg_string = "message two";
		GUC_check_errdetail_string = "detail two";
		GUC_check_errhint_string = "hint two";
		*PgCurrentFormatErrnumberRef() = 202;
		*PgCurrentFormatDomainRef() = "domain two";
		*PgCurrentConfigFileLinenoRef() = 203;
		*PgCurrentGUCFlexFatalErrmsgRef() = "fatal two";
		*PgCurrentGUCFlexFatalJmpRef() = (sigjmp_buf *) &fake_execution2;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentGUCCheckErrcodeValueRef() == 101;
		ok = ok && strcmp(GUC_check_errmsg_string, "message one") == 0;
		ok = ok && strcmp(GUC_check_errdetail_string, "detail one") == 0;
		ok = ok && strcmp(GUC_check_errhint_string, "hint one") == 0;
		ok = ok && *PgCurrentFormatErrnumberRef() == 102;
		ok = ok && strcmp(*PgCurrentFormatDomainRef(), "domain one") == 0;
		ok = ok && *PgCurrentConfigFileLinenoRef() == 103;
		ok = ok && strcmp(*PgCurrentGUCFlexFatalErrmsgRef(), "fatal one") == 0;
		ok = ok && *PgCurrentGUCFlexFatalJmpRef() ==
			(sigjmp_buf *) &fake_execution1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentGUCCheckErrcodeValueRef() == 201;
		ok = ok && strcmp(GUC_check_errmsg_string, "message two") == 0;
		ok = ok && strcmp(GUC_check_errdetail_string, "detail two") == 0;
		ok = ok && strcmp(GUC_check_errhint_string, "hint two") == 0;
		ok = ok && *PgCurrentFormatErrnumberRef() == 202;
		ok = ok && strcmp(*PgCurrentFormatDomainRef(), "domain two") == 0;
		ok = ok && *PgCurrentConfigFileLinenoRef() == 203;
		ok = ok && strcmp(*PgCurrentGUCFlexFatalErrmsgRef(), "fatal two") == 0;
		ok = ok && *PgCurrentGUCFlexFatalJmpRef() ==
			(sigjmp_buf *) &fake_execution2;

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "GUC error scratch state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_catalog_state_is_execution_local);
Datum
test_execution_catalog_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	MemoryContext oldcontext;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);
	fake_execution1.catalog.currently_reindexed_heap = InvalidOid;
	fake_execution1.catalog.currently_reindexed_index = InvalidOid;
	fake_execution2.catalog.currently_reindexed_heap = InvalidOid;
	fake_execution2.catalog.currently_reindexed_index = InvalidOid;

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentUncommittedEnumTypesRef() = (HTAB *) &fake_execution1;
		*PgCurrentUncommittedEnumValuesRef() = (HTAB *) &fake_execution1;
		*PgCurrentReindexedHeapRef() = 101;
		*PgCurrentReindexedIndexRef() = 102;
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		*PgCurrentPendingReindexedIndexesRef() =
			list_make1_oid(103);
		MemoryContextSwitchTo(oldcontext);
		*PgCurrentReindexingNestLevelRef() = 3;
		*PgCurrentPendingRelDeletesRef() =
			(struct PendingRelDelete *) &fake_execution1;
		*PgCurrentPendingSyncHashRef() = (HTAB *) &fake_execution1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentUncommittedEnumTypesRef() == NULL;
		ok = ok && *PgCurrentUncommittedEnumValuesRef() == NULL;
		ok = ok && *PgCurrentReindexedHeapRef() == InvalidOid;
		ok = ok && *PgCurrentReindexedIndexRef() == InvalidOid;
		ok = ok && *PgCurrentPendingReindexedIndexesRef() == NIL;
		ok = ok && *PgCurrentReindexingNestLevelRef() == 0;
		ok = ok && *PgCurrentPendingRelDeletesRef() == NULL;
		ok = ok && *PgCurrentPendingSyncHashRef() == NULL;

		*PgCurrentUncommittedEnumTypesRef() = (HTAB *) &fake_execution2;
		*PgCurrentUncommittedEnumValuesRef() = (HTAB *) &fake_execution2;
		*PgCurrentReindexedHeapRef() = 201;
		*PgCurrentReindexedIndexRef() = 202;
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		*PgCurrentPendingReindexedIndexesRef() =
			list_make1_oid(203);
		MemoryContextSwitchTo(oldcontext);
		*PgCurrentReindexingNestLevelRef() = 4;
		*PgCurrentPendingRelDeletesRef() =
			(struct PendingRelDelete *) &fake_execution2;
		*PgCurrentPendingSyncHashRef() = (HTAB *) &fake_execution2;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentUncommittedEnumTypesRef() ==
			(HTAB *) &fake_execution1;
		ok = ok && *PgCurrentUncommittedEnumValuesRef() ==
			(HTAB *) &fake_execution1;
		ok = ok && *PgCurrentReindexedHeapRef() == 101;
		ok = ok && *PgCurrentReindexedIndexRef() == 102;
		ok = ok && list_member_oid(*PgCurrentPendingReindexedIndexesRef(),
								   103);
		ok = ok && *PgCurrentReindexingNestLevelRef() == 3;
		ok = ok && *PgCurrentPendingRelDeletesRef() ==
			(struct PendingRelDelete *) &fake_execution1;
		ok = ok && *PgCurrentPendingSyncHashRef() ==
			(HTAB *) &fake_execution1;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentUncommittedEnumTypesRef() ==
			(HTAB *) &fake_execution2;
		ok = ok && *PgCurrentUncommittedEnumValuesRef() ==
			(HTAB *) &fake_execution2;
		ok = ok && *PgCurrentReindexedHeapRef() == 201;
		ok = ok && *PgCurrentReindexedIndexRef() == 202;
		ok = ok && list_member_oid(*PgCurrentPendingReindexedIndexesRef(),
								   203);
		ok = ok && *PgCurrentReindexingNestLevelRef() == 4;
		ok = ok && *PgCurrentPendingRelDeletesRef() ==
			(struct PendingRelDelete *) &fake_execution2;
		ok = ok && *PgCurrentPendingSyncHashRef() ==
			(HTAB *) &fake_execution2;

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "catalog execution state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_catalog_cache_state_is_execution_local);
Datum
test_execution_catalog_cache_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	TupleDesc	tupledesc_array1[1];
	TupleDesc	tupledesc_array2[1];
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);
	MemSet(tupledesc_array1, 0, sizeof(tupledesc_array1));
	MemSet(tupledesc_array2, 0, sizeof(tupledesc_array2));

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentCatCacheInProgressStackRef() =
			(CatCInProgress *) &fake_execution1;
		*PgCurrentRelcacheInProgressListRef() =
			(InProgressEnt *) &fake_execution1;
		*PgCurrentRelcacheInProgressListLenRef() = 1;
		*PgCurrentRelcacheInProgressListMaxLenRef() = 2;
		PgCurrentRelcacheEOXactList()[0] = 101;
		*PgCurrentRelcacheEOXactListLenRef() = 1;
		*PgCurrentRelcacheEOXactListOverflowedRef() = true;
		*PgCurrentRelcacheEOXactTupleDescArrayRef() = tupledesc_array1;
		*PgCurrentRelcacheNextEOXactTupleDescNumRef() = 3;
		*PgCurrentRelcacheEOXactTupleDescArrayLenRef() = 4;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentCatCacheInProgressStackRef() == NULL;
		ok = ok && *PgCurrentRelcacheInProgressListRef() == NULL;
		ok = ok && *PgCurrentRelcacheInProgressListLenRef() == 0;
		ok = ok && *PgCurrentRelcacheInProgressListMaxLenRef() == 0;
		ok = ok && PgCurrentRelcacheEOXactList()[0] == InvalidOid;
		ok = ok && *PgCurrentRelcacheEOXactListLenRef() == 0;
		ok = ok && !*PgCurrentRelcacheEOXactListOverflowedRef();
		ok = ok && *PgCurrentRelcacheEOXactTupleDescArrayRef() == NULL;
		ok = ok && *PgCurrentRelcacheNextEOXactTupleDescNumRef() == 0;
		ok = ok && *PgCurrentRelcacheEOXactTupleDescArrayLenRef() == 0;

		*PgCurrentCatCacheInProgressStackRef() =
			(CatCInProgress *) &fake_execution2;
		*PgCurrentRelcacheInProgressListRef() =
			(InProgressEnt *) &fake_execution2;
		*PgCurrentRelcacheInProgressListLenRef() = 5;
		*PgCurrentRelcacheInProgressListMaxLenRef() = 6;
		PgCurrentRelcacheEOXactList()[0] = 201;
		*PgCurrentRelcacheEOXactListLenRef() = 7;
		*PgCurrentRelcacheEOXactListOverflowedRef() = false;
		*PgCurrentRelcacheEOXactTupleDescArrayRef() = tupledesc_array2;
		*PgCurrentRelcacheNextEOXactTupleDescNumRef() = 8;
		*PgCurrentRelcacheEOXactTupleDescArrayLenRef() = 9;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentCatCacheInProgressStackRef() ==
			(CatCInProgress *) &fake_execution1;
		ok = ok && *PgCurrentRelcacheInProgressListRef() ==
			(InProgressEnt *) &fake_execution1;
		ok = ok && *PgCurrentRelcacheInProgressListLenRef() == 1;
		ok = ok && *PgCurrentRelcacheInProgressListMaxLenRef() == 2;
		ok = ok && PgCurrentRelcacheEOXactList()[0] == 101;
		ok = ok && *PgCurrentRelcacheEOXactListLenRef() == 1;
		ok = ok && *PgCurrentRelcacheEOXactListOverflowedRef();
		ok = ok && *PgCurrentRelcacheEOXactTupleDescArrayRef() ==
			tupledesc_array1;
		ok = ok && *PgCurrentRelcacheNextEOXactTupleDescNumRef() == 3;
		ok = ok && *PgCurrentRelcacheEOXactTupleDescArrayLenRef() == 4;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentCatCacheInProgressStackRef() ==
			(CatCInProgress *) &fake_execution2;
		ok = ok && *PgCurrentRelcacheInProgressListRef() ==
			(InProgressEnt *) &fake_execution2;
		ok = ok && *PgCurrentRelcacheInProgressListLenRef() == 5;
		ok = ok && *PgCurrentRelcacheInProgressListMaxLenRef() == 6;
		ok = ok && PgCurrentRelcacheEOXactList()[0] == 201;
		ok = ok && *PgCurrentRelcacheEOXactListLenRef() == 7;
		ok = ok && !*PgCurrentRelcacheEOXactListOverflowedRef();
		ok = ok && *PgCurrentRelcacheEOXactTupleDescArrayRef() ==
			tupledesc_array2;
		ok = ok && *PgCurrentRelcacheNextEOXactTupleDescNumRef() == 8;
		ok = ok && *PgCurrentRelcacheEOXactTupleDescArrayLenRef() == 9;

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "catalog cache execution state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_relmap_state_is_execution_local);
Datum
test_execution_relmap_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		PgCurrentRelMapActiveSharedUpdatesRef()->num_mappings = 1;
		PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapoid = 101;
		PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapfilenumber =
			102;
		PgCurrentRelMapActiveLocalUpdatesRef()->num_mappings = 2;
		PgCurrentRelMapPendingSharedUpdatesRef()->num_mappings = 3;
		PgCurrentRelMapPendingLocalUpdatesRef()->num_mappings = 4;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && PgCurrentRelMapActiveSharedUpdatesRef()->num_mappings == 0;
		ok = ok && PgCurrentRelMapActiveLocalUpdatesRef()->num_mappings == 0;
		ok = ok &&
			PgCurrentRelMapPendingSharedUpdatesRef()->num_mappings == 0;
		ok = ok && PgCurrentRelMapPendingLocalUpdatesRef()->num_mappings == 0;

		PgCurrentRelMapActiveSharedUpdatesRef()->num_mappings = 5;
		PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapoid = 201;
		PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapfilenumber =
			202;
		PgCurrentRelMapActiveLocalUpdatesRef()->num_mappings = 6;
		PgCurrentRelMapPendingSharedUpdatesRef()->num_mappings = 7;
		PgCurrentRelMapPendingLocalUpdatesRef()->num_mappings = 8;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && PgCurrentRelMapActiveSharedUpdatesRef()->num_mappings == 1;
		ok = ok &&
			PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapoid == 101;
		ok = ok &&
			PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapfilenumber ==
			102;
		ok = ok && PgCurrentRelMapActiveLocalUpdatesRef()->num_mappings == 2;
		ok = ok && PgCurrentRelMapPendingSharedUpdatesRef()->num_mappings == 3;
		ok = ok && PgCurrentRelMapPendingLocalUpdatesRef()->num_mappings == 4;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && PgCurrentRelMapActiveSharedUpdatesRef()->num_mappings == 5;
		ok = ok &&
			PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapoid == 201;
		ok = ok &&
			PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapfilenumber ==
			202;
		ok = ok && PgCurrentRelMapActiveLocalUpdatesRef()->num_mappings == 6;
		ok = ok && PgCurrentRelMapPendingSharedUpdatesRef()->num_mappings == 7;
		ok = ok && PgCurrentRelMapPendingLocalUpdatesRef()->num_mappings == 8;

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "relation mapper execution state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_inval_twophase_state_is_execution_local);
Datum
test_execution_inval_twophase_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	SharedInvalidationMessage invalmsg1;
	SharedInvalidationMessage invalmsg2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);
	MemSet(&invalmsg1, 0, sizeof(invalmsg1));
	MemSet(&invalmsg2, 0, sizeof(invalmsg2));

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		PgCurrentInvalMessageArrays()[0].msgs = &invalmsg1;
		PgCurrentInvalMessageArrays()[0].maxmsgs = 101;
		*PgCurrentTransInvalInfoRef() =
			(struct TransInvalidationInfo *) &fake_execution1;
		*PgCurrentInplaceInvalInfoRef() =
			(struct InvalidationInfo *) &fake_execution1;
		PgCurrentTwoPhaseRecordStateRef()->head =
			(struct StateFileChunk *) &fake_execution1;
		PgCurrentTwoPhaseRecordStateRef()->tail =
			(struct StateFileChunk *) &fake_execution1;
		PgCurrentTwoPhaseRecordStateRef()->num_chunks = 102;
		PgCurrentTwoPhaseRecordStateRef()->bytes_free = 103;
		PgCurrentTwoPhaseRecordStateRef()->total_len = 104;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && PgCurrentInvalMessageArrays()[0].msgs == NULL;
		ok = ok && PgCurrentInvalMessageArrays()[0].maxmsgs == 0;
		ok = ok && *PgCurrentTransInvalInfoRef() == NULL;
		ok = ok && *PgCurrentInplaceInvalInfoRef() == NULL;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->head == NULL;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->tail == NULL;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->num_chunks == 0;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->bytes_free == 0;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->total_len == 0;

		PgCurrentInvalMessageArrays()[1].msgs = &invalmsg2;
		PgCurrentInvalMessageArrays()[1].maxmsgs = 201;
		*PgCurrentTransInvalInfoRef() =
			(struct TransInvalidationInfo *) &fake_execution2;
		*PgCurrentInplaceInvalInfoRef() =
			(struct InvalidationInfo *) &fake_execution2;
		PgCurrentTwoPhaseRecordStateRef()->head =
			(struct StateFileChunk *) &fake_execution2;
		PgCurrentTwoPhaseRecordStateRef()->tail =
			(struct StateFileChunk *) &fake_execution2;
		PgCurrentTwoPhaseRecordStateRef()->num_chunks = 202;
		PgCurrentTwoPhaseRecordStateRef()->bytes_free = 203;
		PgCurrentTwoPhaseRecordStateRef()->total_len = 204;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && PgCurrentInvalMessageArrays()[0].msgs == &invalmsg1;
		ok = ok && PgCurrentInvalMessageArrays()[0].maxmsgs == 101;
		ok = ok && *PgCurrentTransInvalInfoRef() ==
			(struct TransInvalidationInfo *) &fake_execution1;
		ok = ok && *PgCurrentInplaceInvalInfoRef() ==
			(struct InvalidationInfo *) &fake_execution1;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->head ==
			(struct StateFileChunk *) &fake_execution1;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->tail ==
			(struct StateFileChunk *) &fake_execution1;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->num_chunks == 102;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->bytes_free == 103;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->total_len == 104;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && PgCurrentInvalMessageArrays()[1].msgs == &invalmsg2;
		ok = ok && PgCurrentInvalMessageArrays()[1].maxmsgs == 201;
		ok = ok && *PgCurrentTransInvalInfoRef() ==
			(struct TransInvalidationInfo *) &fake_execution2;
		ok = ok && *PgCurrentInplaceInvalInfoRef() ==
			(struct InvalidationInfo *) &fake_execution2;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->head ==
			(struct StateFileChunk *) &fake_execution2;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->tail ==
			(struct StateFileChunk *) &fake_execution2;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->num_chunks == 202;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->bytes_free == 203;
		ok = ok && PgCurrentTwoPhaseRecordStateRef()->total_len == 204;

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "invalidation/two-phase state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_async_state_is_execution_local);
Datum
test_execution_async_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentPendingActionsRef() = (struct ActionList *) &fake_execution1;
		*PgCurrentPendingListenActionsRef() = (HTAB *) &fake_execution1;
		*PgCurrentPendingNotifiesRef() =
			(struct NotificationList *) &fake_execution1;
		PgCurrentQueueHeadBeforeWriteRef()->page = 101;
		PgCurrentQueueHeadBeforeWriteRef()->offset = 102;
		PgCurrentQueueHeadAfterWriteRef()->page = 103;
		PgCurrentQueueHeadAfterWriteRef()->offset = 104;
		ok = ok &&
			PgCurrentAsyncSignalWorkspaceContext() ==
			fake_execution1.async.signal_context;
		*PgCurrentSignalPidsRef() = (int32 *) &fake_execution1;
		*PgCurrentSignalProcnosRef() = (ProcNumber *) &fake_execution1;
		*PgCurrentTryAdvanceTailRef() = true;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentPendingActionsRef() == NULL;
		ok = ok && *PgCurrentPendingListenActionsRef() == NULL;
		ok = ok && *PgCurrentPendingNotifiesRef() == NULL;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->page == 0;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->offset == 0;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->page == 0;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->offset == 0;
		ok = ok && fake_execution2.async.signal_context == NULL;
		ok = ok && *PgCurrentSignalPidsRef() == NULL;
		ok = ok && *PgCurrentSignalProcnosRef() == NULL;
		ok = ok && !*PgCurrentTryAdvanceTailRef();

		*PgCurrentPendingActionsRef() = (struct ActionList *) &fake_execution2;
		*PgCurrentPendingListenActionsRef() = (HTAB *) &fake_execution2;
		*PgCurrentPendingNotifiesRef() =
			(struct NotificationList *) &fake_execution2;
		PgCurrentQueueHeadBeforeWriteRef()->page = 201;
		PgCurrentQueueHeadBeforeWriteRef()->offset = 202;
		PgCurrentQueueHeadAfterWriteRef()->page = 203;
		PgCurrentQueueHeadAfterWriteRef()->offset = 204;
		ok = ok &&
			PgCurrentAsyncSignalWorkspaceContext() ==
			fake_execution2.async.signal_context;
		*PgCurrentSignalPidsRef() = (int32 *) &fake_execution2;
		*PgCurrentSignalProcnosRef() = (ProcNumber *) &fake_execution2;
		*PgCurrentTryAdvanceTailRef() = false;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentPendingActionsRef() ==
			(struct ActionList *) &fake_execution1;
		ok = ok && *PgCurrentPendingListenActionsRef() ==
			(HTAB *) &fake_execution1;
		ok = ok && *PgCurrentPendingNotifiesRef() ==
			(struct NotificationList *) &fake_execution1;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->page == 101;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->offset == 102;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->page == 103;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->offset == 104;
		ok = ok && fake_execution1.async.signal_context != NULL;
		ok = ok &&
			fake_execution1.async.signal_context !=
			fake_execution2.async.signal_context;
		ok = ok && *PgCurrentSignalPidsRef() == (int32 *) &fake_execution1;
		ok = ok && *PgCurrentSignalProcnosRef() ==
			(ProcNumber *) &fake_execution1;
		ok = ok && *PgCurrentTryAdvanceTailRef();

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentPendingActionsRef() ==
			(struct ActionList *) &fake_execution2;
		ok = ok && *PgCurrentPendingListenActionsRef() ==
			(HTAB *) &fake_execution2;
		ok = ok && *PgCurrentPendingNotifiesRef() ==
			(struct NotificationList *) &fake_execution2;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->page == 201;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->offset == 202;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->page == 203;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->offset == 204;
		ok = ok && fake_execution2.async.signal_context != NULL;
		ok = ok && *PgCurrentSignalPidsRef() == (int32 *) &fake_execution2;
		ok = ok && *PgCurrentSignalProcnosRef() ==
			(ProcNumber *) &fake_execution2;
		ok = ok && !*PgCurrentTryAdvanceTailRef();

		PgSetCurrentExecution(saved_execution);
		if (fake_execution1.async.signal_context != NULL)
			MemoryContextDelete(fake_execution1.async.signal_context);
		if (fake_execution2.async.signal_context != NULL)
			MemoryContextDelete(fake_execution2.async.signal_context);
	}
	PG_CATCH();
	{
		PgSetCurrentExecution(saved_execution);
		if (fake_execution1.async.signal_context != NULL)
			MemoryContextDelete(fake_execution1.async.signal_context);
		if (fake_execution2.async.signal_context != NULL)
			MemoryContextDelete(fake_execution2.async.signal_context);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "async execution state was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_execution_misc_scratch_state_is_execution_local);
Datum
test_execution_misc_scratch_state_is_execution_local(PG_FUNCTION_ARGS)
{
	PgExecution *saved_execution;
	PgExecution fake_execution1;
	PgExecution fake_execution2;
	MemoryContext after_triggers_context1;
	MemoryContext after_triggers_context2;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution1);
	test_backend_runtime_seed_execution_memory_contexts(&fake_execution2);

	PG_TRY();
	{
		PgSetCurrentExecution(&fake_execution1);
		*PgCurrentArrayAnalyzeExtraDataRef() = &fake_execution1;
		*PgCurrentTriggerDepthRef() = 101;
		after_triggers_context1 = PgCurrentAfterTriggersMemoryContext();
		*PgCurrentAfterTriggersDataRef() =
			MemoryContextAlloc(after_triggers_context1, sizeof(int));
		*PgCurrentRegexLocaleRef() = &fake_execution1;
		*PgCurrentValgrindOldErrorCountRef() = 101;
		*PgCurrentSnapBuildSavedResourceOwnerDuringExportRef() =
			(ResourceOwner) &fake_execution1;
		*PgCurrentSnapBuildExportInProgressRef() = true;

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentArrayAnalyzeExtraDataRef() == NULL;
		ok = ok && *PgCurrentTriggerDepthRef() == 0;
		ok = ok && *PgCurrentAfterTriggersDataRef() == NULL;
		ok = ok && *PgCurrentAfterTriggersMemoryContextRef() == NULL;
		ok = ok && *PgCurrentRegexLocaleRef() == NULL;
		ok = ok && *PgCurrentValgrindOldErrorCountRef() == 0;
		ok = ok && *PgCurrentSnapBuildSavedResourceOwnerDuringExportRef() ==
			NULL;
		ok = ok && !*PgCurrentSnapBuildExportInProgressRef();

		*PgCurrentArrayAnalyzeExtraDataRef() = &fake_execution2;
		*PgCurrentTriggerDepthRef() = 201;
		after_triggers_context2 = PgCurrentAfterTriggersMemoryContext();
		*PgCurrentAfterTriggersDataRef() =
			MemoryContextAlloc(after_triggers_context2, sizeof(int));
		*PgCurrentRegexLocaleRef() = &fake_execution2;
		*PgCurrentValgrindOldErrorCountRef() = 201;
		*PgCurrentSnapBuildSavedResourceOwnerDuringExportRef() =
			(ResourceOwner) &fake_execution2;
		*PgCurrentSnapBuildExportInProgressRef() = false;

		PgSetCurrentExecution(&fake_execution1);
		ok = ok && *PgCurrentArrayAnalyzeExtraDataRef() == &fake_execution1;
		ok = ok && *PgCurrentTriggerDepthRef() == 101;
		ok = ok && *PgCurrentAfterTriggersMemoryContextRef() ==
			after_triggers_context1;
		ok = ok && *PgCurrentAfterTriggersDataRef() != NULL;
		ok = ok && *PgCurrentRegexLocaleRef() == &fake_execution1;
		ok = ok && *PgCurrentValgrindOldErrorCountRef() == 101;
		ok = ok && *PgCurrentSnapBuildSavedResourceOwnerDuringExportRef() ==
			(ResourceOwner) &fake_execution1;
		ok = ok && *PgCurrentSnapBuildExportInProgressRef();

		PgSetCurrentExecution(&fake_execution2);
		ok = ok && *PgCurrentArrayAnalyzeExtraDataRef() == &fake_execution2;
		ok = ok && *PgCurrentTriggerDepthRef() == 201;
		ok = ok && *PgCurrentAfterTriggersMemoryContextRef() ==
			after_triggers_context2;
		ok = ok && *PgCurrentAfterTriggersDataRef() != NULL;
		ok = ok && *PgCurrentRegexLocaleRef() == &fake_execution2;
		ok = ok && *PgCurrentValgrindOldErrorCountRef() == 201;
		ok = ok && *PgCurrentSnapBuildSavedResourceOwnerDuringExportRef() ==
			(ResourceOwner) &fake_execution2;
		ok = ok && !*PgCurrentSnapBuildExportInProgressRef();

		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		if (fake_execution1.trigger.after_triggers_context != NULL)
			MemoryContextDelete(fake_execution1.trigger.after_triggers_context);
		if (fake_execution2.trigger.after_triggers_context != NULL)
			MemoryContextDelete(fake_execution2.trigger.after_triggers_context);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (fake_execution1.trigger.after_triggers_context != NULL)
		MemoryContextDelete(fake_execution1.trigger.after_triggers_context);
	if (fake_execution2.trigger.after_triggers_context != NULL)
		MemoryContextDelete(fake_execution2.trigger.after_triggers_context);

	if (!ok)
		elog(ERROR, "miscellaneous execution scratch state was not execution-local");

	PG_RETURN_BOOL(true);
}
