/*--------------------------------------------------------------------------
 *
 * test_backend_runtime.c
 *		Test backend runtime scaffolding
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>

#include "fmgr.h"
#include "libpq/libpq.h"
#include "port/atomics.h"
#include "port/pg_thread.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/backend_runtime.h"

PG_MODULE_MAGIC;

static sigjmp_buf exit_continuation_jmp;
static volatile bool exit_continuation_seen;
static volatile int exit_continuation_code;

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

static void
test_exit_backend(int code)
{
	exit_continuation_seen = true;
	exit_continuation_code = code;
	siglongjmp(exit_continuation_jmp, 1);
}

PG_FUNCTION_INFO_V1(test_backend_exit_runtime_continuation);
Datum
test_backend_exit_runtime_continuation(PG_FUNCTION_ARGS)
{
	PgBackendExitContinuation saved_exit_backend;
	volatile bool continued;

	if (CurrentPgRuntime == NULL)
		elog(ERROR, "current backend runtime is not initialized");

	saved_exit_backend = CurrentPgRuntime->exit_backend;
	exit_continuation_seen = false;
	exit_continuation_code = 0;
	continued = false;

	/*
	 * Test the post-cleanup runtime handoff directly.  Calling full
	 * PgBackendExit() here would run backend cleanup and then jump back into a
	 * backend stack that had already been torn down.
	 */
	CurrentPgRuntime->exit_backend = test_exit_backend;
	if (sigsetjmp(exit_continuation_jmp, 1) == 0)
		PgBackendExitComplete(17);
	else
		continued = true;
	CurrentPgRuntime->exit_backend = saved_exit_backend;

	if (!continued)
		elog(ERROR, "backend exit continuation did not transfer control");
	if (!exit_continuation_seen)
		elog(ERROR, "backend exit continuation was not called");
	if (exit_continuation_code != 17)
		elog(ERROR, "backend exit continuation saw code %d, expected 17",
			 exit_continuation_code);

	PG_RETURN_INT32(exit_continuation_code);
}

PG_FUNCTION_INFO_V1(test_backend_dsm_shutdown_is_backend_local);
Datum
test_backend_dsm_shutdown_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend_with_dsm;
	PgBackend	fake_backend_to_exit;
	dsm_segment *seg = NULL;
	dsm_handle	handle = DSM_HANDLE_INVALID;
	bool		found = false;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend_with_dsm, 0, sizeof(fake_backend_with_dsm));
	MemSet(&fake_backend_to_exit, 0, sizeof(fake_backend_to_exit));
	dlist_init(&fake_backend_with_dsm.dsm_segment_list);
	dlist_init(&fake_backend_to_exit.dsm_segment_list);

	PG_TRY();
	{
		/*
		 * Simulate two logical backends in one address space.  Only
		 * CurrentPgBackend is switched because this test isolates DSM mapping
		 * ownership; the rest of the current process runtime remains real.
		 */
		CurrentPgBackend = &fake_backend_with_dsm;
		seg = dsm_create(1024, 0);
		dsm_pin_mapping(seg);
		handle = dsm_segment_handle(seg);

		CurrentPgBackend = &fake_backend_to_exit;
		dsm_backend_shutdown();

		CurrentPgBackend = &fake_backend_with_dsm;
		found = (dsm_find_mapping(handle) == seg);
		dsm_detach(seg);
		seg = NULL;

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		if (seg != NULL)
		{
			CurrentPgBackend = &fake_backend_with_dsm;
			dsm_detach(seg);
		}
		CurrentPgBackend = saved_backend;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!found)
		elog(ERROR, "DSM shutdown for one backend detached another backend's mapping");

	PG_RETURN_BOOL(true);
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
	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	PgThreadBackendRuntimeState state;
	Latch		fake_latch;
	bool		ok = true;

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
		InitializePgThreadBackendRuntime(&state, B_BACKEND, NULL,
										 &fake_latch);

		ok = ok && CurrentPgRuntime != NULL;
		ok = ok && CurrentPgRuntime->kind == PG_RUNTIME_THREAD_PER_SESSION;
		ok = ok && CurrentPgRuntime->extension_backend_model ==
			PG_BACKEND_MODEL_THREAD_PER_SESSION;
		ok = ok && CurrentPgCarrier == &state.carrier;
		ok = ok && CurrentPgBackend == &state.backend;
		ok = ok && CurrentPgSession == &state.session;
		ok = ok && CurrentPgConnection == &state.connection;
		ok = ok && CurrentPgExecution == &state.execution;
		ok = ok && state.carrier.kind == PG_CARRIER_THREAD;
		ok = ok && state.backend.backend_type == B_BACKEND;
		ok = ok && state.backend.interrupt_latch == &fake_latch;
		ok = ok && dlist_is_empty(&state.backend.dsm_segment_list);

		CurrentPgRuntime = saved_runtime;
		CurrentPgCarrier = saved_carrier;
		CurrentPgBackend = saved_backend;
		CurrentPgSession = saved_session;
		CurrentPgConnection = saved_connection;
		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgRuntime = saved_runtime;
		CurrentPgCarrier = saved_carrier;
		CurrentPgBackend = saved_backend;
		CurrentPgSession = saved_session;
		CurrentPgConnection = saved_connection;
		CurrentPgExecution = saved_execution;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "thread backend runtime state was not initialized");

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
		InitializePgThreadBackendRuntime(&state1, B_BACKEND, NULL,
										 &fake_latch1);
		thread_backend_id1 = PgCurrentBackendId();

		InitializePgThreadBackendRuntime(&state2, B_BACKEND, NULL,
										 &fake_latch2);
		thread_backend_id2 = PgCurrentBackendId();

		ok = ok && current_backend_id != 0;
		ok = ok && thread_backend_id1 != 0;
		ok = ok && thread_backend_id2 != 0;
		ok = ok && thread_backend_id1 != current_backend_id;
		ok = ok && thread_backend_id2 != current_backend_id;
		ok = ok && thread_backend_id1 != thread_backend_id2;
		ok = ok && thread_backend_id1 == PgBackendGetId(&state1.backend);
		ok = ok && thread_backend_id2 == PgBackendGetId(&state2.backend);

		CurrentPgRuntime = saved_runtime;
		CurrentPgCarrier = saved_carrier;
		CurrentPgBackend = saved_backend;
		CurrentPgSession = saved_session;
		CurrentPgConnection = saved_connection;
		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgRuntime = saved_runtime;
		CurrentPgCarrier = saved_carrier;
		CurrentPgBackend = saved_backend;
		CurrentPgSession = saved_session;
		CurrentPgConnection = saved_connection;
		CurrentPgExecution = saved_execution;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "thread backend ids were not distinct logical ids");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_interrupt_holdoffs_are_backend_local);
Datum
test_backend_interrupt_holdoffs_are_backend_local(PG_FUNCTION_ARGS)
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
		CurrentPgBackend = &fake_backend1;
		HOLD_INTERRUPTS();
		HOLD_CANCEL_INTERRUPTS();
		START_CRIT_SECTION();

		CurrentPgBackend = &fake_backend2;
		ok = ok && InterruptHoldoffCount == 0;
		ok = ok && QueryCancelHoldoffCount == 0;
		ok = ok && CritSectionCount == 0;
		InterruptHoldoffCount = 3;
		QueryCancelHoldoffCount = 4;
		CritSectionCount = 5;

		CurrentPgBackend = &fake_backend1;
		ok = ok && InterruptHoldoffCount == 1;
		ok = ok && QueryCancelHoldoffCount == 1;
		ok = ok && CritSectionCount == 1;
		END_CRIT_SECTION();
		RESUME_CANCEL_INTERRUPTS();
		RESUME_INTERRUPTS();
		ok = ok && InterruptHoldoffCount == 0;
		ok = ok && QueryCancelHoldoffCount == 0;
		ok = ok && CritSectionCount == 0;

		CurrentPgBackend = &fake_backend2;
		ok = ok && InterruptHoldoffCount == 3;
		ok = ok && QueryCancelHoldoffCount == 4;
		ok = ok && CritSectionCount == 5;

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "interrupt holdoff counters were not backend-local");

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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		debug_query_string = "fake execution one";

		CurrentPgExecution = &fake_execution2;
		ok = ok && debug_query_string == NULL;
		debug_query_string = "fake execution two";

		CurrentPgExecution = &fake_execution1;
		ok = ok && strcmp(debug_query_string, "fake execution one") == 0;

		CurrentPgExecution = &fake_execution2;
		ok = ok && strcmp(debug_query_string, "fake execution two") == 0;
		debug_query_string = NULL;

		CurrentPgExecution = saved_execution;
		debug_query_string = saved_debug_query_string;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
		debug_query_string = saved_debug_query_string;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "debug_query_string was not execution-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_socket_io_is_connection_local);
Datum
test_connection_socket_io_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	PgConnectionSocketIOState *socket_io;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		CurrentPgConnection = &fake_connection1;
		socket_io = PgCurrentConnectionSocketIORef();
		socket_io->send_buffer = (char *) "fake connection one";
		socket_io->send_buffer_size = 11;
		socket_io->send_pointer = 7;
		socket_io->send_start = 3;
		socket_io->recv_pointer = 5;
		socket_io->recv_length = 9;
		socket_io->comm_busy = true;
		socket_io->comm_reading_msg = true;

		CurrentPgConnection = &fake_connection2;
		socket_io = PgCurrentConnectionSocketIORef();
		ok = ok && socket_io->send_buffer == NULL;
		ok = ok && socket_io->send_buffer_size == 0;
		ok = ok && socket_io->send_pointer == 0;
		ok = ok && socket_io->send_start == 0;
		ok = ok && socket_io->recv_pointer == 0;
		ok = ok && socket_io->recv_length == 0;
		ok = ok && !socket_io->comm_busy;
		ok = ok && !socket_io->comm_reading_msg;
		socket_io->send_buffer = (char *) "fake connection two";
		socket_io->comm_busy = true;

		CurrentPgConnection = &fake_connection1;
		socket_io = PgCurrentConnectionSocketIORef();
		ok = ok && strcmp(socket_io->send_buffer, "fake connection one") == 0;
		ok = ok && socket_io->send_buffer_size == 11;
		ok = ok && socket_io->send_pointer == 7;
		ok = ok && socket_io->send_start == 3;
		ok = ok && socket_io->recv_pointer == 5;
		ok = ok && socket_io->recv_length == 9;
		ok = ok && socket_io->comm_busy;
		ok = ok && socket_io->comm_reading_msg;

		CurrentPgConnection = &fake_connection2;
		socket_io = PgCurrentConnectionSocketIORef();
		ok = ok && strcmp(socket_io->send_buffer, "fake connection two") == 0;
		ok = ok && socket_io->comm_busy;
		ok = ok && !socket_io->comm_reading_msg;

		CurrentPgConnection = saved_connection;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection socket I/O state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_protocol_state_is_connection_local);
Datum
test_connection_protocol_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	const PQcommMethods *saved_comm_methods;
	WaitEventSet *saved_wait_set;
	const PQcommMethods methods1 = {0};
	const PQcommMethods methods2 = {0};
	WaitEventSet *wait_set1;
	WaitEventSet *wait_set2;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_comm_methods = PqCommMethods;
	saved_wait_set = FeBeWaitSet;
	wait_set1 = (WaitEventSet *) &fake_connection1;
	wait_set2 = (WaitEventSet *) &fake_connection2;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		CurrentPgConnection = &fake_connection1;
		PqCommMethods = &methods1;
		FeBeWaitSet = wait_set1;

		CurrentPgConnection = &fake_connection2;
		ok = ok && PqCommMethods == NULL;
		ok = ok && FeBeWaitSet == NULL;
		PqCommMethods = &methods2;
		FeBeWaitSet = wait_set2;

		CurrentPgConnection = &fake_connection1;
		ok = ok && PqCommMethods == &methods1;
		ok = ok && FeBeWaitSet == wait_set1;

		CurrentPgConnection = &fake_connection2;
		ok = ok && PqCommMethods == &methods2;
		ok = ok && FeBeWaitSet == wait_set2;

		CurrentPgConnection = saved_connection;
		PqCommMethods = saved_comm_methods;
		FeBeWaitSet = saved_wait_set;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		PqCommMethods = saved_comm_methods;
		FeBeWaitSet = saved_wait_set;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection protocol state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_identity_state_is_connection_local);
Datum
test_connection_identity_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	Port	   *saved_port;
	Port		fake_port1;
	Port		fake_port2;
	uint8		saved_cancel_key[PG_CONNECTION_CANCEL_KEY_LENGTH];
	int			saved_cancel_key_length;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_port = MyProcPort;
	saved_cancel_key_length = MyCancelKeyLength;
	memcpy(saved_cancel_key, MyCancelKey, sizeof(saved_cancel_key));
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));
	MemSet(&fake_port1, 0, sizeof(fake_port1));
	MemSet(&fake_port2, 0, sizeof(fake_port2));

	PG_TRY();
	{
		CurrentPgConnection = &fake_connection1;
		MyProcPort = &fake_port1;
		MyCancelKey[0] = 1;
		MyCancelKey[1] = 2;
		MyCancelKeyLength = 2;

		CurrentPgConnection = &fake_connection2;
		ok = ok && MyProcPort == NULL;
		ok = ok && MyCancelKeyLength == 0;
		MyProcPort = &fake_port2;
		MyCancelKey[0] = 7;
		MyCancelKey[1] = 8;
		MyCancelKey[2] = 9;
		MyCancelKeyLength = 3;

		CurrentPgConnection = &fake_connection1;
		ok = ok && MyProcPort == &fake_port1;
		ok = ok && MyCancelKeyLength == 2;
		ok = ok && MyCancelKey[0] == 1;
		ok = ok && MyCancelKey[1] == 2;

		CurrentPgConnection = &fake_connection2;
		ok = ok && MyProcPort == &fake_port2;
		ok = ok && MyCancelKeyLength == 3;
		ok = ok && MyCancelKey[0] == 7;
		ok = ok && MyCancelKey[1] == 8;
		ok = ok && MyCancelKey[2] == 9;

		CurrentPgConnection = saved_connection;
		MyProcPort = saved_port;
		memcpy(MyCancelKey, saved_cancel_key, sizeof(saved_cancel_key));
		MyCancelKeyLength = saved_cancel_key_length;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		MyProcPort = saved_port;
		memcpy(MyCancelKey, saved_cancel_key, sizeof(saved_cancel_key));
		MyCancelKeyLength = saved_cancel_key_length;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection identity state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_interrupt_wakes_target_latch);
Datum
test_backend_interrupt_wakes_target_latch(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend;
	Latch		fake_latch;
	bool		latch_set;
	bool		pending_seen;
	PgBackendInterruptMask pending;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend, 0, sizeof(fake_backend));
	InitLatch(&fake_latch);
	PgBackendInitializeInterrupts(&fake_backend);
	PgBackendSetInterruptLatch(&fake_backend, &fake_latch);

	PgBackendRaiseInterrupt(&fake_backend,
							PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	latch_set = fake_latch.is_set;

	CurrentPgBackend = &fake_backend;
	pending_seen = PgCurrentBackendHasPendingInterrupts();
	CurrentPgBackend = saved_backend;

	ResetLatch(&fake_latch);
	pending = PgBackendConsumeInterrupts(&fake_backend);

	if (!pending_seen)
		elog(ERROR, "current backend did not observe pending logical interrupt");
	if ((pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL)) == 0)
		elog(ERROR, "raised logical interrupt was not recorded");
	if (!latch_set)
		elog(ERROR, "raising interrupt did not set target backend latch");

	PG_RETURN_BOOL(true);
}
