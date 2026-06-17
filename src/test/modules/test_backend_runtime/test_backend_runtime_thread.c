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

		CHECK_THREAD_RUNTIME_STATE(state.backend.runtime != NULL);
		CHECK_THREAD_RUNTIME_STATE(state.backend.runtime->kind ==
								   PG_RUNTIME_THREAD_PER_SESSION);
		CHECK_THREAD_RUNTIME_STATE(state.backend.runtime->extension_backend_model ==
								   PG_BACKEND_MODEL_THREAD_PER_SESSION);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.kind == PG_CARRIER_THREAD);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_backend == &state.backend);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_session == &state.session);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.current_execution == &state.execution);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.backend_thread_start == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_waiting == false);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_signal_fd == -1);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_selfpipe_readfd == -1);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_selfpipe_writefd == -1);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.wait_event_selfpipe_owner_pid == 0);
		CHECK_THREAD_RUNTIME_STATE(state.carrier.stack_base_ptr == NULL);
		CHECK_THREAD_RUNTIME_STATE(state.backend.backend_type == B_BACKEND);
		CHECK_THREAD_RUNTIME_STATE(state.backend.interrupt_latch == &fake_latch);
		CHECK_THREAD_RUNTIME_STATE(dlist_is_empty(&state.backend.dsm_segment_list));
		CHECK_THREAD_RUNTIME_STATE(state.backend.session == &state.session);
		CHECK_THREAD_RUNTIME_STATE(state.backend.connection == &state.connection);
		CHECK_THREAD_RUNTIME_STATE(state.backend.execution == &state.execution);
		CHECK_THREAD_RUNTIME_STATE(state.session.backend == &state.backend);
		CHECK_THREAD_RUNTIME_STATE(state.session.connection == &state.connection);
		CHECK_THREAD_RUNTIME_STATE(state.session.execution == &state.execution);
		CHECK_THREAD_RUNTIME_STATE(state.connection.backend == &state.backend);
		CHECK_THREAD_RUNTIME_STATE(state.connection.session == &state.session);
		CHECK_THREAD_RUNTIME_STATE(state.execution.backend == &state.backend);
		CHECK_THREAD_RUNTIME_STATE(state.execution.session == &state.session);
		CHECK_THREAD_RUNTIME_STATE(state.execution.carrier == &state.carrier);
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
		thread_backend_id1 = PgBackendGetId(&state1.backend);

		InitializePgThreadBackendRuntimeState(&state2, B_BACKEND, NULL,
											  &fake_latch2);
		thread_backend_id2 = PgBackendGetId(&state2.backend);

		ok = ok && current_backend_id != 0;
		ok = ok && thread_backend_id1 != 0;
		ok = ok && thread_backend_id2 != 0;
		ok = ok && thread_backend_id1 != current_backend_id;
		ok = ok && thread_backend_id2 != current_backend_id;
		ok = ok && thread_backend_id1 != thread_backend_id2;
		ok = ok && thread_backend_id1 == PgBackendGetId(&state1.backend);
		ok = ok && thread_backend_id2 == PgBackendGetId(&state2.backend);
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
