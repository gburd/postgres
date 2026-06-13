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
#include "libpq/libpq-be.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "port/atomics.h"
#include "port/pg_thread.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/backend_runtime.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		CurrentResourceOwner = fake_owner1;
		CurTransactionResourceOwner = fake_owner2;
		TopTransactionResourceOwner = fake_owner3;

		CurrentPgExecution = &fake_execution2;
		ok = ok && CurrentResourceOwner == NULL;
		ok = ok && CurTransactionResourceOwner == NULL;
		ok = ok && TopTransactionResourceOwner == NULL;
		CurrentResourceOwner = fake_owner3;
		CurTransactionResourceOwner = fake_owner1;
		TopTransactionResourceOwner = fake_owner2;

		CurrentPgExecution = &fake_execution1;
		ok = ok && CurrentResourceOwner == fake_owner1;
		ok = ok && CurTransactionResourceOwner == fake_owner2;
		ok = ok && TopTransactionResourceOwner == fake_owner3;

		CurrentPgExecution = &fake_execution2;
		ok = ok && CurrentResourceOwner == fake_owner3;
		ok = ok && CurTransactionResourceOwner == fake_owner1;
		ok = ok && TopTransactionResourceOwner == fake_owner2;

		CurrentPgExecution = saved_execution;
		CurrentResourceOwner = saved_current_resource_owner;
		CurTransactionResourceOwner = saved_cur_transaction_resource_owner;
		TopTransactionResourceOwner = saved_top_transaction_resource_owner;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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

PG_FUNCTION_INFO_V1(test_session_database_state_is_session_local);
Datum
test_session_database_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	Oid			saved_database_id;
	Oid			saved_database_tablespace;
	bool		saved_database_has_login_event_triggers;
	char	   *saved_database_path;
	char	   *fake_path1 = "base/1";
	char	   *fake_path2 = "base/2";
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_database_id = MyDatabaseId;
	saved_database_tablespace = MyDatabaseTableSpace;
	saved_database_has_login_event_triggers =
		MyDatabaseHasLoginEventTriggers;
	saved_database_path = DatabasePath;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		CurrentPgSession = &fake_session1;
		MyDatabaseId = 1111;
		MyDatabaseTableSpace = 2222;
		MyDatabaseHasLoginEventTriggers = true;
		DatabasePath = fake_path1;

		CurrentPgSession = &fake_session2;
		ok = ok && MyDatabaseId == InvalidOid;
		ok = ok && MyDatabaseTableSpace == InvalidOid;
		ok = ok && !MyDatabaseHasLoginEventTriggers;
		ok = ok && DatabasePath == NULL;
		MyDatabaseId = 3333;
		MyDatabaseTableSpace = 4444;
		MyDatabaseHasLoginEventTriggers = false;
		DatabasePath = fake_path2;

		CurrentPgSession = &fake_session1;
		ok = ok && MyDatabaseId == 1111;
		ok = ok && MyDatabaseTableSpace == 2222;
		ok = ok && MyDatabaseHasLoginEventTriggers;
		ok = ok && DatabasePath == fake_path1;

		CurrentPgSession = &fake_session2;
		ok = ok && MyDatabaseId == 3333;
		ok = ok && MyDatabaseTableSpace == 4444;
		ok = ok && !MyDatabaseHasLoginEventTriggers;
		ok = ok && DatabasePath == fake_path2;

		CurrentPgSession = saved_session;
		MyDatabaseId = saved_database_id;
		MyDatabaseTableSpace = saved_database_tablespace;
		MyDatabaseHasLoginEventTriggers =
			saved_database_has_login_event_triggers;
		DatabasePath = saved_database_path;
	}
	PG_CATCH();
	{
		CurrentPgSession = saved_session;
		MyDatabaseId = saved_database_id;
		MyDatabaseTableSpace = saved_database_tablespace;
		MyDatabaseHasLoginEventTriggers =
			saved_database_has_login_event_triggers;
		DatabasePath = saved_database_path;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session database state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_datetime_state_is_session_local);
Datum
test_session_datetime_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	int			saved_date_style;
	int			saved_date_order;
	char	   *saved_interval_style;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_date_style = DateStyle;
	saved_date_order = DateOrder;
	saved_interval_style = pstrdup(GetConfigOption("IntervalStyle",
												   false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && DateStyle == USE_ISO_DATES;
		ok = ok && DateOrder == DATEORDER_MDY;
		ok = ok && IntervalStyle == INTSTYLE_POSTGRES;
		DateStyle = USE_SQL_DATES;
		DateOrder = DATEORDER_DMY;
		SetConfigOption("IntervalStyle", "sql_standard",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && IntervalStyle == INTSTYLE_SQL_STANDARD;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DateStyle == USE_ISO_DATES;
		ok = ok && DateOrder == DATEORDER_MDY;
		ok = ok && IntervalStyle == INTSTYLE_POSTGRES;
		DateStyle = USE_GERMAN_DATES;
		DateOrder = DATEORDER_YMD;
		SetConfigOption("IntervalStyle", "iso_8601",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && IntervalStyle == INTSTYLE_ISO_8601;

		PgSetCurrentSession(&fake_session1);
		ok = ok && DateStyle == USE_SQL_DATES;
		ok = ok && DateOrder == DATEORDER_DMY;
		ok = ok && IntervalStyle == INTSTYLE_SQL_STANDARD;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DateStyle == USE_GERMAN_DATES;
		ok = ok && DateOrder == DATEORDER_YMD;
		ok = ok && IntervalStyle == INTSTYLE_ISO_8601;

		PgSetCurrentSession(saved_session);
		DateStyle = saved_date_style;
		DateOrder = saved_date_order;
		SetConfigOption("IntervalStyle", saved_interval_style,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		DateStyle = saved_date_style;
		DateOrder = saved_date_order;
		SetConfigOption("IntervalStyle", saved_interval_style,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session date/time GUC state was not session-local");

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

PG_FUNCTION_INFO_V1(test_backend_pending_interrupts_are_backend_local);
Datum
test_backend_pending_interrupts_are_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	sig_atomic_t saved_interrupt_pending;
	sig_atomic_t saved_query_cancel_pending;
	sig_atomic_t saved_proc_die_pending;
	int			saved_proc_die_sender_pid;
	int			saved_proc_die_sender_uid;
	sig_atomic_t saved_idle_in_transaction_session_timeout_pending;
	sig_atomic_t saved_transaction_timeout_pending;
	sig_atomic_t saved_idle_session_timeout_pending;
	sig_atomic_t saved_proc_signal_barrier_pending;
	sig_atomic_t saved_log_memory_context_pending;
	sig_atomic_t saved_idle_stats_update_timeout_pending;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	saved_interrupt_pending = InterruptPending;
	saved_query_cancel_pending = QueryCancelPending;
	saved_proc_die_pending = ProcDiePending;
	saved_proc_die_sender_pid = ProcDieSenderPid;
	saved_proc_die_sender_uid = ProcDieSenderUid;
	saved_idle_in_transaction_session_timeout_pending =
		IdleInTransactionSessionTimeoutPending;
	saved_transaction_timeout_pending = TransactionTimeoutPending;
	saved_idle_session_timeout_pending = IdleSessionTimeoutPending;
	saved_proc_signal_barrier_pending = ProcSignalBarrierPending;
	saved_log_memory_context_pending = LogMemoryContextPending;
	saved_idle_stats_update_timeout_pending =
		IdleStatsUpdateTimeoutPending;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));

	PG_TRY();
	{
		CurrentPgBackend = &fake_backend1;
		InterruptPending = true;
		QueryCancelPending = true;
		ProcDiePending = true;
		ProcDieSenderPid = 101;
		ProcDieSenderUid = 202;
		IdleInTransactionSessionTimeoutPending = true;
		TransactionTimeoutPending = true;
		IdleSessionTimeoutPending = true;
		ProcSignalBarrierPending = true;
		LogMemoryContextPending = true;
		IdleStatsUpdateTimeoutPending = true;

		CurrentPgBackend = &fake_backend2;
		ok = ok && !InterruptPending;
		ok = ok && !QueryCancelPending;
		ok = ok && !ProcDiePending;
		ok = ok && ProcDieSenderPid == 0;
		ok = ok && ProcDieSenderUid == 0;
		ok = ok && !IdleInTransactionSessionTimeoutPending;
		ok = ok && !TransactionTimeoutPending;
		ok = ok && !IdleSessionTimeoutPending;
		ok = ok && !ProcSignalBarrierPending;
		ok = ok && !LogMemoryContextPending;
		ok = ok && !IdleStatsUpdateTimeoutPending;

		InterruptPending = false;
		QueryCancelPending = false;
		ProcDiePending = false;
		ProcDieSenderPid = 303;
		ProcDieSenderUid = 404;
		IdleInTransactionSessionTimeoutPending = false;
		TransactionTimeoutPending = false;
		IdleSessionTimeoutPending = false;
		ProcSignalBarrierPending = false;
		LogMemoryContextPending = false;
		IdleStatsUpdateTimeoutPending = false;

		CurrentPgBackend = &fake_backend1;
		ok = ok && InterruptPending;
		ok = ok && QueryCancelPending;
		ok = ok && ProcDiePending;
		ok = ok && ProcDieSenderPid == 101;
		ok = ok && ProcDieSenderUid == 202;
		ok = ok && IdleInTransactionSessionTimeoutPending;
		ok = ok && TransactionTimeoutPending;
		ok = ok && IdleSessionTimeoutPending;
		ok = ok && ProcSignalBarrierPending;
		ok = ok && LogMemoryContextPending;
		ok = ok && IdleStatsUpdateTimeoutPending;

		CurrentPgBackend = &fake_backend2;
		ok = ok && !InterruptPending;
		ok = ok && !QueryCancelPending;
		ok = ok && !ProcDiePending;
		ok = ok && ProcDieSenderPid == 303;
		ok = ok && ProcDieSenderUid == 404;
		ok = ok && !IdleInTransactionSessionTimeoutPending;
		ok = ok && !TransactionTimeoutPending;
		ok = ok && !IdleSessionTimeoutPending;
		ok = ok && !ProcSignalBarrierPending;
		ok = ok && !LogMemoryContextPending;
		ok = ok && !IdleStatsUpdateTimeoutPending;

		CurrentPgBackend = saved_backend;
		InterruptPending = saved_interrupt_pending;
		QueryCancelPending = saved_query_cancel_pending;
		ProcDiePending = saved_proc_die_pending;
		ProcDieSenderPid = saved_proc_die_sender_pid;
		ProcDieSenderUid = saved_proc_die_sender_uid;
		IdleInTransactionSessionTimeoutPending =
			saved_idle_in_transaction_session_timeout_pending;
		TransactionTimeoutPending = saved_transaction_timeout_pending;
		IdleSessionTimeoutPending = saved_idle_session_timeout_pending;
		ProcSignalBarrierPending = saved_proc_signal_barrier_pending;
		LogMemoryContextPending = saved_log_memory_context_pending;
		IdleStatsUpdateTimeoutPending =
			saved_idle_stats_update_timeout_pending;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		InterruptPending = saved_interrupt_pending;
		QueryCancelPending = saved_query_cancel_pending;
		ProcDiePending = saved_proc_die_pending;
		ProcDieSenderPid = saved_proc_die_sender_pid;
		ProcDieSenderUid = saved_proc_die_sender_uid;
		IdleInTransactionSessionTimeoutPending =
			saved_idle_in_transaction_session_timeout_pending;
		TransactionTimeoutPending = saved_transaction_timeout_pending;
		IdleSessionTimeoutPending = saved_idle_session_timeout_pending;
		ProcSignalBarrierPending = saved_proc_signal_barrier_pending;
		LogMemoryContextPending = saved_log_memory_context_pending;
		IdleStatsUpdateTimeoutPending =
			saved_idle_stats_update_timeout_pending;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "pending interrupt state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_core_state_is_backend_local);
Datum
test_backend_core_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	Latch	   *saved_latch;
	Latch		fake_latch1;
	Latch		fake_latch2;
	bool		saved_exit_on_any_error;
	int			saved_proc_pid;
	pg_time_t	saved_start_time;
	TimestampTz saved_start_timestamp;
	int			saved_pm_child_slot;
	char		saved_output_file_name[MAXPGPATH];
	BackendType saved_backend_type;
	ProcessingMode saved_mode;
	bool		saved_ignore_system_indexes;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	saved_exit_on_any_error = ExitOnAnyError;
	saved_proc_pid = MyProcPid;
	saved_start_time = MyStartTime;
	saved_start_timestamp = MyStartTimestamp;
	saved_latch = MyLatch;
	saved_pm_child_slot = MyPMChildSlot;
	strlcpy(saved_output_file_name, OutputFileName, sizeof(saved_output_file_name));
	saved_backend_type = MyBackendType;
	saved_mode = Mode;
	saved_ignore_system_indexes = IgnoreSystemIndexes;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	MemSet(&fake_latch1, 0, sizeof(fake_latch1));
	MemSet(&fake_latch2, 0, sizeof(fake_latch2));

	PG_TRY();
	{
		CurrentPgBackend = &fake_backend1;
		ExitOnAnyError = true;
		MyProcPid = 111;
		MyStartTime = 222;
		MyStartTimestamp = 333;
		MyLatch = &fake_latch1;
		MyPMChildSlot = 44;
		strlcpy(OutputFileName, "backend-one.log", MAXPGPATH);
		MyBackendType = B_BACKEND;
		Mode = NormalProcessing;
		IgnoreSystemIndexes = true;

		CurrentPgBackend = &fake_backend2;
		ok = ok && !ExitOnAnyError;
		ok = ok && MyProcPid == 0;
		ok = ok && MyStartTime == 0;
		ok = ok && MyStartTimestamp == 0;
		ok = ok && MyLatch == NULL;
		ok = ok && MyPMChildSlot == 0;
		ok = ok && OutputFileName[0] == '\0';
		ok = ok && MyBackendType == B_INVALID;
		ok = ok && Mode == BootstrapProcessing;
		ok = ok && !IgnoreSystemIndexes;

		ExitOnAnyError = false;
		MyProcPid = 555;
		MyStartTime = 666;
		MyStartTimestamp = 777;
		MyLatch = &fake_latch2;
		MyPMChildSlot = 88;
		strlcpy(OutputFileName, "backend-two.log", MAXPGPATH);
		MyBackendType = B_WAL_SENDER;
		Mode = InitProcessing;
		IgnoreSystemIndexes = false;

		CurrentPgBackend = &fake_backend1;
		ok = ok && ExitOnAnyError;
		ok = ok && MyProcPid == 111;
		ok = ok && MyStartTime == 222;
		ok = ok && MyStartTimestamp == 333;
		ok = ok && MyLatch == &fake_latch1;
		ok = ok && MyPMChildSlot == 44;
		ok = ok && strcmp(OutputFileName, "backend-one.log") == 0;
		ok = ok && MyBackendType == B_BACKEND;
		ok = ok && Mode == NormalProcessing;
		ok = ok && IgnoreSystemIndexes;

		CurrentPgBackend = &fake_backend2;
		ok = ok && !ExitOnAnyError;
		ok = ok && MyProcPid == 555;
		ok = ok && MyStartTime == 666;
		ok = ok && MyStartTimestamp == 777;
		ok = ok && MyLatch == &fake_latch2;
		ok = ok && MyPMChildSlot == 88;
		ok = ok && strcmp(OutputFileName, "backend-two.log") == 0;
		ok = ok && MyBackendType == B_WAL_SENDER;
		ok = ok && Mode == InitProcessing;
		ok = ok && !IgnoreSystemIndexes;

		CurrentPgBackend = saved_backend;
		ExitOnAnyError = saved_exit_on_any_error;
		MyProcPid = saved_proc_pid;
		MyStartTime = saved_start_time;
		MyStartTimestamp = saved_start_timestamp;
		MyLatch = saved_latch;
		MyPMChildSlot = saved_pm_child_slot;
		strlcpy(OutputFileName, saved_output_file_name, MAXPGPATH);
		MyBackendType = saved_backend_type;
		Mode = saved_mode;
		IgnoreSystemIndexes = saved_ignore_system_indexes;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		ExitOnAnyError = saved_exit_on_any_error;
		MyProcPid = saved_proc_pid;
		MyStartTime = saved_start_time;
		MyStartTimestamp = saved_start_timestamp;
		MyLatch = saved_latch;
		MyPMChildSlot = saved_pm_child_slot;
		strlcpy(OutputFileName, saved_output_file_name, MAXPGPATH);
		MyBackendType = saved_backend_type;
		Mode = saved_mode;
		IgnoreSystemIndexes = saved_ignore_system_indexes;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend core state was not backend-local");

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
	MemSet(&fake_error_context1, 0, sizeof(fake_error_context1));
	MemSet(&fake_error_context2, 0, sizeof(fake_error_context2));

	/*
	 * Do not wrap this in PG_TRY(): this test intentionally rewires
	 * PG_exception_stack to prove the compatibility lvalue is execution-local.
	 */
	CurrentPgExecution = &fake_execution1;
	error_context_stack = &fake_error_context1;
	PG_exception_stack = &fake_exception_stack1;

	CurrentPgExecution = &fake_execution2;
	ok = ok && error_context_stack == NULL;
	ok = ok && PG_exception_stack == NULL;
	error_context_stack = &fake_error_context2;
	PG_exception_stack = &fake_exception_stack2;

	CurrentPgExecution = &fake_execution1;
	ok = ok && error_context_stack == &fake_error_context1;
	ok = ok && PG_exception_stack == &fake_exception_stack1;

	CurrentPgExecution = &fake_execution2;
	ok = ok && error_context_stack == &fake_error_context2;
	ok = ok && PG_exception_stack == &fake_exception_stack2;

	CurrentPgExecution = saved_execution;
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
	MemoryContext saved_current_memory_context;
	MemoryContext saved_error_context;
	MemoryContext saved_message_context;
	MemoryContext saved_top_transaction_context;
	MemoryContext saved_cur_transaction_context;
	MemoryContext saved_portal_context;
	MemoryContext fake_context1 = (MemoryContext) &fake_execution1;
	MemoryContext fake_context2 = (MemoryContext) &fake_execution2;
	MemoryContext fake_context3 = (MemoryContext) &saved_execution;
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_current_memory_context = CurrentMemoryContext;
	saved_error_context = ErrorContext;
	saved_message_context = MessageContext;
	saved_top_transaction_context = TopTransactionContext;
	saved_cur_transaction_context = CurTransactionContext;
	saved_portal_context = PortalContext;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		CurrentMemoryContext = fake_context1;
		ErrorContext = fake_context2;
		MessageContext = fake_context3;
		TopTransactionContext = fake_context1;
		CurTransactionContext = fake_context2;
		PortalContext = fake_context3;

		CurrentPgExecution = &fake_execution2;
		ok = ok && CurrentMemoryContext == NULL;
		ok = ok && ErrorContext == NULL;
		ok = ok && MessageContext == NULL;
		ok = ok && TopTransactionContext == NULL;
		ok = ok && CurTransactionContext == NULL;
		ok = ok && PortalContext == NULL;
		CurrentMemoryContext = fake_context3;
		ErrorContext = fake_context1;
		MessageContext = fake_context2;
		TopTransactionContext = fake_context3;
		CurTransactionContext = fake_context1;
		PortalContext = fake_context2;

		CurrentPgExecution = &fake_execution1;
		ok = ok && CurrentMemoryContext == fake_context1;
		ok = ok && ErrorContext == fake_context2;
		ok = ok && MessageContext == fake_context3;
		ok = ok && TopTransactionContext == fake_context1;
		ok = ok && CurTransactionContext == fake_context2;
		ok = ok && PortalContext == fake_context3;

		CurrentPgExecution = &fake_execution2;
		ok = ok && CurrentMemoryContext == fake_context3;
		ok = ok && ErrorContext == fake_context1;
		ok = ok && MessageContext == fake_context2;
		ok = ok && TopTransactionContext == fake_context3;
		ok = ok && CurTransactionContext == fake_context1;
		ok = ok && PortalContext == fake_context2;

		CurrentPgExecution = saved_execution;
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
		MessageContext = saved_message_context;
		TopTransactionContext = saved_top_transaction_context;
		CurTransactionContext = saved_cur_transaction_context;
		PortalContext = saved_portal_context;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
		MessageContext = saved_message_context;
		TopTransactionContext = saved_top_transaction_context;
		CurTransactionContext = saved_cur_transaction_context;
		PortalContext = saved_portal_context;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "execution memory contexts were not execution-local");

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

PG_FUNCTION_INFO_V1(test_connection_interrupt_state_is_connection_local);
Datum
test_connection_interrupt_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	volatile sig_atomic_t saved_check_client_connection_pending;
	volatile sig_atomic_t saved_client_connection_lost;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_check_client_connection_pending = CheckClientConnectionPending;
	saved_client_connection_lost = ClientConnectionLost;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		CurrentPgConnection = &fake_connection1;
		CheckClientConnectionPending = true;
		ClientConnectionLost = false;

		CurrentPgConnection = &fake_connection2;
		ok = ok && !CheckClientConnectionPending;
		ok = ok && !ClientConnectionLost;
		CheckClientConnectionPending = false;
		ClientConnectionLost = true;

		CurrentPgConnection = &fake_connection1;
		ok = ok && CheckClientConnectionPending;
		ok = ok && !ClientConnectionLost;

		CurrentPgConnection = &fake_connection2;
		ok = ok && !CheckClientConnectionPending;
		ok = ok && ClientConnectionLost;

		CurrentPgConnection = saved_connection;
		CheckClientConnectionPending = saved_check_client_connection_pending;
		ClientConnectionLost = saved_client_connection_lost;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		CheckClientConnectionPending = saved_check_client_connection_pending;
		ClientConnectionLost = saved_client_connection_lost;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection interrupt state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_frontend_protocol_is_connection_local);
Datum
test_connection_frontend_protocol_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	ProtocolVersion saved_frontend_protocol;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_frontend_protocol = FrontendProtocol;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		CurrentPgConnection = &fake_connection1;
		FrontendProtocol = PG_PROTOCOL(3, 0);

		CurrentPgConnection = &fake_connection2;
		ok = ok && FrontendProtocol == 0;
		FrontendProtocol = PG_PROTOCOL(3, 2);

		CurrentPgConnection = &fake_connection1;
		ok = ok && FrontendProtocol == PG_PROTOCOL(3, 0);

		CurrentPgConnection = &fake_connection2;
		ok = ok && FrontendProtocol == PG_PROTOCOL(3, 2);

		CurrentPgConnection = saved_connection;
		FrontendProtocol = saved_frontend_protocol;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		FrontendProtocol = saved_frontend_protocol;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "frontend protocol state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_startup_state_is_connection_local);
Datum
test_connection_startup_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	struct ClientSocket *saved_client_socket;
	struct ClientSocket *fake_client_socket1;
	struct ClientSocket *fake_client_socket2;
	bool		saved_client_auth_in_progress;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_client_auth_in_progress = ClientAuthInProgress;
	saved_client_socket = MyClientSocket;
	fake_client_socket1 = (struct ClientSocket *) &fake_connection1;
	fake_client_socket2 = (struct ClientSocket *) &fake_connection2;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		CurrentPgConnection = &fake_connection1;
		ClientAuthInProgress = true;
		MyClientSocket = fake_client_socket1;

		CurrentPgConnection = &fake_connection2;
		ok = ok && !ClientAuthInProgress;
		ok = ok && MyClientSocket == NULL;
		ClientAuthInProgress = false;
		MyClientSocket = fake_client_socket2;

		CurrentPgConnection = &fake_connection1;
		ok = ok && ClientAuthInProgress;
		ok = ok && MyClientSocket == fake_client_socket1;

		CurrentPgConnection = &fake_connection2;
		ok = ok && !ClientAuthInProgress;
		ok = ok && MyClientSocket == fake_client_socket2;

		CurrentPgConnection = saved_connection;
		ClientAuthInProgress = saved_client_auth_in_progress;
		MyClientSocket = saved_client_socket;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		ClientAuthInProgress = saved_client_auth_in_progress;
		MyClientSocket = saved_client_socket;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection startup state was not connection-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_client_connection_info_is_connection_local);
Datum
test_client_connection_info_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	const char *saved_authn_id;
	UserAuth	saved_auth_method;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_authn_id = MyClientConnectionInfo.authn_id;
	saved_auth_method = MyClientConnectionInfo.auth_method;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		CurrentPgConnection = &fake_connection1;
		MyClientConnectionInfo.authn_id = "connection-one";
		MyClientConnectionInfo.auth_method = uaTrust;

		CurrentPgConnection = &fake_connection2;
		ok = ok && MyClientConnectionInfo.authn_id == NULL;
		MyClientConnectionInfo.authn_id = "connection-two";
		MyClientConnectionInfo.auth_method = uaSCRAM;

		CurrentPgConnection = &fake_connection1;
		ok = ok && strcmp(MyClientConnectionInfo.authn_id,
						  "connection-one") == 0;
		ok = ok && MyClientConnectionInfo.auth_method == uaTrust;

		CurrentPgConnection = &fake_connection2;
		ok = ok && strcmp(MyClientConnectionInfo.authn_id,
						  "connection-two") == 0;
		ok = ok && MyClientConnectionInfo.auth_method == uaSCRAM;

		CurrentPgConnection = saved_connection;
		MyClientConnectionInfo.authn_id = saved_authn_id;
		MyClientConnectionInfo.auth_method = saved_auth_method;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		MyClientConnectionInfo.authn_id = saved_authn_id;
		MyClientConnectionInfo.auth_method = saved_auth_method;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "client connection info was not connection-local");

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
