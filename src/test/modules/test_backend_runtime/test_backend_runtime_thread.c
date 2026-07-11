/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_thread.c
 *		Thread runtime tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_thread.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

static void test_pg_thread_routine(void *arg);
static void test_pg_thread_exit_routine(void *arg);

static void
test_pg_thread_routine(void *arg)
{
	pg_atomic_uint32 *ran = (pg_atomic_uint32 *) arg;

	pg_atomic_write_u32(ran, 1);
}

static void
test_pg_thread_exit_routine(void *arg)
{
	pg_atomic_uint32 *ran = (pg_atomic_uint32 *) arg;

	pg_atomic_write_u32(ran, 1);
	pg_thread_exit();
}

PG_FUNCTION_INFO_V1(test_backend_thread_create_join);
Datum
test_backend_thread_create_join(PG_FUNCTION_ARGS)
{
	PgThread	thread;
	pg_atomic_uint32 ran;
	int			rc;

	pg_atomic_init_u32(&ran, 0);
	rc = pg_thread_create(&thread, "pg test thread",
						  test_pg_thread_routine, &ran);
	if (rc != 0)
	{
		errno = rc;
		elog(ERROR, "pg_thread_create failed: %m");
	}

	rc = pg_thread_join(&thread);
	if (rc != 0)
	{
		errno = rc;
		elog(ERROR, "pg_thread_join failed: %m");
	}

	if (pg_atomic_read_u32(&ran) != 1)
		elog(ERROR, "thread routine did not run");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_thread_exit_join);
Datum
test_backend_thread_exit_join(PG_FUNCTION_ARGS)
{
	PgThread	thread;
	pg_atomic_uint32 ran;
	int			rc;

	pg_atomic_init_u32(&ran, 0);
	rc = pg_thread_create(&thread, "pg test thread exit",
						  test_pg_thread_exit_routine, &ran);
	if (rc != 0)
	{
		errno = rc;
		elog(ERROR, "pg_thread_create failed: %m");
	}

	rc = pg_thread_join(&thread);
	if (rc != 0)
	{
		errno = rc;
		elog(ERROR, "pg_thread_join failed: %m");
	}

	if (pg_atomic_read_u32(&ran) != 1)
		elog(ERROR, "thread exit routine did not run");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_thread_runtime_state);
Datum
test_backend_thread_runtime_state(PG_FUNCTION_ARGS)
{
#define CHECK_THREAD_RUNTIME_STATE(expr) \
	do { \
		if (!(expr)) \
			elog(ERROR, "thread backend runtime state check failed: %s", \
				 #expr); \
	} while (0)

	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	PgThreadBackendRuntimeState state;
	Latch		fake_latch;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;

	InitLatch(&fake_latch);

	PG_TRY();
	{
		InitializePgThreadRuntime(NULL);
		InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
											  &fake_latch);

		CHECK_THREAD_RUNTIME_STATE(state.logical.backend.runtime != NULL);
		CHECK_THREAD_RUNTIME_STATE(state.logical.backend.runtime->kind ==
								   PG_RUNTIME_THREAD_PER_SESSION);
		CHECK_THREAD_RUNTIME_STATE(!PgRuntimeKindIsThreadBacked(PG_RUNTIME_PROCESS));
		CHECK_THREAD_RUNTIME_STATE(PgRuntimeKindIsThreadBacked(
									   PG_RUNTIME_THREAD_PER_SESSION));
		CHECK_THREAD_RUNTIME_STATE(PgRuntimeKindIsThreadBacked(
									   PG_RUNTIME_POOLED_PROTOCOL));
		CHECK_THREAD_RUNTIME_STATE(!PgRuntimeIsThreadBacked(NULL));
		CHECK_THREAD_RUNTIME_STATE(PgRuntimeIsThreadBacked(state.logical.backend.runtime));
		CHECK_THREAD_RUNTIME_STATE(!PgRuntimeKindIsPooledProtocol(
									   PG_RUNTIME_PROCESS));
		CHECK_THREAD_RUNTIME_STATE(!PgRuntimeKindIsPooledProtocol(
									   PG_RUNTIME_THREAD_PER_SESSION));
		CHECK_THREAD_RUNTIME_STATE(PgRuntimeKindIsPooledProtocol(
									   PG_RUNTIME_POOLED_PROTOCOL));
		CHECK_THREAD_RUNTIME_STATE(!PgRuntimeIsPooledProtocol(NULL));
		CHECK_THREAD_RUNTIME_STATE(!PgRuntimeIsPooledProtocol(
									   state.logical.backend.runtime));
		CHECK_THREAD_RUNTIME_STATE(PgRuntimePooledProtocolCarrierLimit() ==
								   pooled_protocol_carriers);
		CHECK_THREAD_RUNTIME_STATE(PgRuntimePooledProtocolRequested() ==
								   (multithreaded &&
									pooled_protocol_carriers > 0));
		CHECK_THREAD_RUNTIME_STATE(state.logical.backend.runtime->extension_backend_model ==
								   PG_BACKEND_MODEL_THREAD_PER_SESSION);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.kind == PG_CARRIER_THREAD);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_backend == &state.logical.backend);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_session == &state.logical.session);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_execution == &state.logical.execution);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.backend_thread_start == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_waiting == false);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_signal_fd == -1);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_selfpipe_readfd == -1);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_selfpipe_writefd == -1);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_selfpipe_owner_pid == 0);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.stack_base_ptr == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.logical.backend.backend_type == B_BACKEND);
		CHECK_THREAD_RUNTIME_STATE(state.logical.backend.interrupt_latch == &fake_latch);
		CHECK_THREAD_RUNTIME_STATE(dlist_is_empty(&state.logical.backend.dsm_segment_list));
		CHECK_THREAD_RUNTIME_STATE(state.logical.backend.session == &state.logical.session);
		CHECK_THREAD_RUNTIME_STATE(state.logical.backend.connection == &state.logical.connection);
		CHECK_THREAD_RUNTIME_STATE(state.logical.backend.execution == &state.logical.execution);
		CHECK_THREAD_RUNTIME_STATE(state.logical.session.backend == &state.logical.backend);
		CHECK_THREAD_RUNTIME_STATE(state.logical.session.connection == &state.logical.connection);
		CHECK_THREAD_RUNTIME_STATE(state.logical.session.execution == &state.logical.execution);
		CHECK_THREAD_RUNTIME_STATE(state.logical.connection.backend == &state.logical.backend);
		CHECK_THREAD_RUNTIME_STATE(state.logical.connection.session == &state.logical.session);
		CHECK_THREAD_RUNTIME_STATE(state.logical.execution.backend == &state.logical.backend);
		CHECK_THREAD_RUNTIME_STATE(state.logical.execution.session == &state.logical.session);
		CHECK_THREAD_RUNTIME_STATE(state.logical.execution.carrier == &state.carrier);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgRuntime == saved_runtime);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgCarrier == saved_carrier);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgBackend == saved_backend);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgSession == saved_session);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgConnection == saved_connection);
		CHECK_THREAD_RUNTIME_STATE(CurrentPgExecution == saved_execution);

		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

#undef CHECK_THREAD_RUNTIME_STATE

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_thread_split_initializers);
Datum
test_backend_thread_split_initializers(PG_FUNCTION_ARGS)
{
#define CHECK_THREAD_SPLIT_INITIALIZER(expr) \
	do { \
		if (!(expr)) \
			elog(ERROR, "thread backend split initializer check failed: %s", \
				 #expr); \
	} while (0)

	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	PgCarrier	carrier;
	PgThreadBackendLogicalState logical_without_carrier;
	PgThreadBackendLogicalState logical_with_carrier;
	Latch		fake_latch1;
	Latch		fake_latch2;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;

	InitLatch(&fake_latch1);
	InitLatch(&fake_latch2);

	PG_TRY();
	{
		InitializePgThreadRuntime(NULL);
		InitializePgThreadCarrierRuntimeState(&carrier);

		CHECK_THREAD_SPLIT_INITIALIZER(carrier.kind == PG_CARRIER_THREAD);
		CHECK_THREAD_SPLIT_INITIALIZER(carrier.runtime != NULL);
		CHECK_THREAD_SPLIT_INITIALIZER(carrier.current_backend == NULL);
		CHECK_THREAD_SPLIT_INITIALIZER(carrier.current_session == NULL);
		CHECK_THREAD_SPLIT_INITIALIZER(carrier.current_execution == NULL);

		InitializePgThreadBackendLogicalState(&logical_without_carrier, NULL,
											  B_BACKEND, NULL, &fake_latch1);

		CHECK_THREAD_SPLIT_INITIALIZER(logical_without_carrier.backend.runtime ==
									   carrier.runtime);
		CHECK_THREAD_SPLIT_INITIALIZER(logical_without_carrier.backend.carrier ==
									   NULL);
		CHECK_THREAD_SPLIT_INITIALIZER(logical_without_carrier.backend.session ==
									   &logical_without_carrier.session);
		CHECK_THREAD_SPLIT_INITIALIZER(logical_without_carrier.backend.connection ==
									   &logical_without_carrier.connection);
		CHECK_THREAD_SPLIT_INITIALIZER(logical_without_carrier.backend.execution ==
									   &logical_without_carrier.execution);
		CHECK_THREAD_SPLIT_INITIALIZER(logical_without_carrier.session.backend ==
									   &logical_without_carrier.backend);
		CHECK_THREAD_SPLIT_INITIALIZER(logical_without_carrier.connection.backend ==
									   &logical_without_carrier.backend);
		CHECK_THREAD_SPLIT_INITIALIZER(logical_without_carrier.execution.backend ==
									   &logical_without_carrier.backend);
		CHECK_THREAD_SPLIT_INITIALIZER(logical_without_carrier.execution.carrier ==
									   NULL);

		InitializePgThreadBackendLogicalState(&logical_with_carrier, &carrier,
											  B_BACKEND, NULL, &fake_latch2);

		CHECK_THREAD_SPLIT_INITIALIZER(logical_with_carrier.backend.runtime ==
									   carrier.runtime);
		CHECK_THREAD_SPLIT_INITIALIZER(logical_with_carrier.backend.carrier ==
									   &carrier);
		CHECK_THREAD_SPLIT_INITIALIZER(logical_with_carrier.execution.carrier ==
									   &carrier);
		CHECK_THREAD_SPLIT_INITIALIZER(carrier.current_backend == NULL);
		CHECK_THREAD_SPLIT_INITIALIZER(carrier.current_session == NULL);
		CHECK_THREAD_SPLIT_INITIALIZER(carrier.current_execution == NULL);
		CHECK_THREAD_SPLIT_INITIALIZER(CurrentPgRuntime == saved_runtime);
		CHECK_THREAD_SPLIT_INITIALIZER(CurrentPgCarrier == saved_carrier);
		CHECK_THREAD_SPLIT_INITIALIZER(CurrentPgBackend == saved_backend);
		CHECK_THREAD_SPLIT_INITIALIZER(CurrentPgSession == saved_session);
		CHECK_THREAD_SPLIT_INITIALIZER(CurrentPgConnection == saved_connection);
		CHECK_THREAD_SPLIT_INITIALIZER(CurrentPgExecution == saved_execution);

		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

#undef CHECK_THREAD_SPLIT_INITIALIZER

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_pgproc_has_logical_id);
Datum
test_backend_pgproc_has_logical_id(PG_FUNCTION_ARGS)
{
	bool		ok;

	ok = MyProc != NULL;
	ok = ok && PgCurrentBackendId() != 0;
	ok = ok && MyProc->backendId != 0;
	ok = ok && MyProc->backendId == PgCurrentBackendId();
	ok = ok && MyProc->pid == MyProcPid;

	PG_RETURN_BOOL(ok);
}

PG_FUNCTION_INFO_V1(test_backend_thread_ids_are_logical);
Datum
test_backend_thread_ids_are_logical(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	PgThreadBackendRuntimeState state1;
	PgThreadBackendRuntimeState state2;
	Latch		fake_latch1;
	Latch		fake_latch2;
	PgBackendId current_backend_id;
	PgBackendId thread_backend_id1;
	PgBackendId thread_backend_id2;
	bool		ok = true;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;
	current_backend_id = PgCurrentBackendId();

	InitLatch(&fake_latch1);
	InitLatch(&fake_latch2);

	PG_TRY();
	{
		InitializePgThreadRuntime(NULL);
		InitializePgThreadBackendRuntimeState(&state1, B_BACKEND, NULL,
											  &fake_latch1);
		thread_backend_id1 = PgBackendGetId(&state1.logical.backend);

		InitializePgThreadBackendRuntimeState(&state2, B_BACKEND, NULL,
											  &fake_latch2);
		thread_backend_id2 = PgBackendGetId(&state2.logical.backend);

		ok = ok && current_backend_id != 0;
		ok = ok && thread_backend_id1 != 0;
		ok = ok && thread_backend_id2 != 0;
		ok = ok && thread_backend_id1 != current_backend_id;
		ok = ok && thread_backend_id2 != current_backend_id;
		ok = ok && thread_backend_id1 != thread_backend_id2;
		ok = ok && thread_backend_id1 == PgBackendGetId(&state1.logical.backend);
		ok = ok && thread_backend_id2 == PgBackendGetId(&state2.logical.backend);
		ok = ok && CurrentPgRuntime == saved_runtime;
		ok = ok && CurrentPgCarrier == saved_carrier;
		ok = ok && CurrentPgBackend == saved_backend;
		ok = ok && CurrentPgSession == saved_session;
		ok = ok && CurrentPgConnection == saved_connection;
		ok = ok && CurrentPgExecution == saved_execution;

		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
	}
	PG_CATCH();
	{
		PgSetCurrentRuntime(saved_runtime);
		PgSetCurrentCarrier(saved_carrier);
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
		PgSetCurrentConnection(saved_connection);
		PgSetCurrentExecution(saved_execution);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "thread backend ids were not distinct logical ids");

	PG_RETURN_BOOL(true);
}
