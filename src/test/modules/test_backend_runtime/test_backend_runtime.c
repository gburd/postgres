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

#include "access/gin.h"
#include "access/parallel.h"
#include "access/session.h"
#include "access/tableam.h"
#include "access/toast_compression.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/binary_upgrade.h"
#include "catalog/storage.h"
#include "common/pg_prng.h"
#include "commands/async.h"
#include "commands/event_trigger.h"
#include "commands/extension.h"
#include "commands/repack.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "commands/user.h"
#include "commands/vacuum.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "jit/jit.h"
#include "libpq/libpq-be.h"
#include "libpq/libpq.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/queryjumble.h"
#include "optimizer/cost.h"
#include "optimizer/extendplan.h"
#include "optimizer/geqo.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "parser/parser.h"
#include "parser/parse_expr.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "port/atomics.h"
#include "port/pg_thread.h"
#include "replication/reorderbuffer.h"
#include "replication/slot.h"
#include "replication/syncrep.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/aio_internal.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/copydir.h"
#include "storage/dsm.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/large_object.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/sinval.h"
#include "tcop/backend_startup.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tsearch/ts_cache.h"
#include "utils/backend_runtime.h"
#include "postmaster/datachecksum_state.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"
#include "utils/pgstat_internal.h"
#include "utils/plancache.h"
#include "utils/resowner.h"
#include "utils/rls.h"
#include "utils/wait_event.h"
#include "utils/xml.h"

PG_MODULE_MAGIC;

static sigjmp_buf exit_continuation_jmp;
static volatile bool exit_continuation_seen;
static volatile int exit_continuation_code;
static void test_backend_runtime_exit_callback(int code, Datum arg);
static void test_backend_runtime_syscache_callback(Datum arg,
												   SysCacheIdentifier cacheid,
												   uint32 hashvalue);
static void test_backend_runtime_relcache_callback(Datum arg, Oid relid);
static void test_backend_runtime_relsync_callback(Datum arg, Oid relid);

typedef struct TestBoolGUCSetting
{
	const char *name;
	bool	   *(*ref) (void);
	bool		default_value;
	const char *session1_value;
	bool		session1_expected;
	const char *session2_value;
	bool		session2_expected;
} TestBoolGUCSetting;

typedef struct TestIntGUCSetting
{
	const char *name;
	int		   *(*ref) (void);
	int			default_value;
	const char *session1_value;
	int			session1_expected;
	const char *session2_value;
	int			session2_expected;
} TestIntGUCSetting;

typedef struct TestRealGUCSetting
{
	const char *name;
	double	   *(*ref) (void);
	double		default_value;
	const char *session1_value;
	double		session1_expected;
	const char *session2_value;
	double		session2_expected;
} TestRealGUCSetting;

static void test_pg_thread_routine(void *arg);
static void test_pg_thread_exit_routine(void *arg);
static void test_pmchild_thread_backend_reader_routine(void *arg);
static void test_copy_current_user_identity(PgSession *session);

typedef struct TestPMChildThreadBackendRace
{
	PMChild    *pmchild;
	pg_atomic_uint32 start;
	pg_atomic_uint32 stop;
	pg_atomic_uint32 ready_count;
	pg_atomic_uint32 attempts;
	pg_atomic_uint32 hits;
	pg_atomic_uint32 saw_live_signal_pid;
} TestPMChildThreadBackendRace;

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
test_pmchild_thread_backend_reader_routine(void *arg)
{
	TestPMChildThreadBackendRace *state = (TestPMChildThreadBackendRace *) arg;

	pg_atomic_fetch_add_u32(&state->ready_count, 1);
	while (pg_atomic_read_u32(&state->start) == 0 &&
		   pg_atomic_read_u32(&state->stop) == 0)
		;

	while (pg_atomic_read_u32(&state->stop) == 0)
	{
		pg_atomic_fetch_add_u32(&state->attempts, 1);
		if (PostmasterChildSignalPid(state->pmchild) != 0)
			pg_atomic_fetch_add_u32(&state->saw_live_signal_pid, 1);
		if (PostmasterChildRaiseThreadInterrupt(state->pmchild,
												PG_BACKEND_INTERRUPT_QUERY_CANCEL))
			pg_atomic_fetch_add_u32(&state->hits, 1);
		(void) PostmasterChildWakeThreadBackend(state->pmchild);
	}
}

static void
test_backend_runtime_syscache_callback(Datum arg, SysCacheIdentifier cacheid,
									   uint32 hashvalue)
{
}

static void
test_backend_runtime_relcache_callback(Datum arg, Oid relid)
{
}

static void
test_backend_runtime_relsync_callback(Datum arg, Oid relid)
{
}

static void
test_copy_current_user_identity(PgSession *session)
{
	Assert(session != NULL);

	session->user_identity = *PgCurrentUserIdentityState();
	for (int i = 0; i < lengthof(session->user_identity.cached_roles); i++)
	{
		session->user_identity.cached_role[i] = InvalidOid;
		session->user_identity.cached_roles[i] = NIL;
	}
	session->user_identity.cached_db_hash = 0;
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
		pg_prng_seed(&pg_global_prng_state, 1);
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
		InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
											  &fake_latch);

		ok = ok && state.backend.runtime != NULL;
		ok = ok && state.backend.runtime->kind == PG_RUNTIME_THREAD_PER_SESSION;
		ok = ok && state.backend.runtime->extension_backend_model ==
			PG_BACKEND_MODEL_THREAD_PER_SESSION;
		ok = ok && state.carrier.kind == PG_CARRIER_THREAD;
		ok = ok && state.carrier.current_backend == &state.backend;
		ok = ok && state.carrier.current_session == &state.session;
		ok = ok && state.carrier.current_execution == &state.execution;
		ok = ok && state.backend.backend_type == B_BACKEND;
		ok = ok && state.backend.interrupt_latch == &fake_latch;
		ok = ok && dlist_is_empty(&state.backend.dsm_segment_list);
		ok = ok && state.backend.session == &state.session;
		ok = ok && state.backend.connection == &state.connection;
		ok = ok && state.backend.execution == &state.execution;
		ok = ok && state.session.backend == &state.backend;
		ok = ok && state.session.connection == &state.connection;
		ok = ok && state.session.execution == &state.execution;
		ok = ok && state.connection.backend == &state.backend;
		ok = ok && state.connection.session == &state.session;
		ok = ok && state.execution.backend == &state.backend;
		ok = ok && state.execution.session == &state.session;
		ok = ok && state.execution.carrier == &state.carrier;
		ok = ok && CurrentPgRuntime == saved_runtime;
		ok = ok && CurrentPgCarrier == saved_carrier;
		ok = ok && CurrentPgBackend == saved_backend;
		ok = ok && CurrentPgSession == saved_session;
		ok = ok && CurrentPgConnection == saved_connection;
		ok = ok && CurrentPgExecution == saved_execution;

		CurrentPgRuntime = saved_runtime;
		CurrentPgCarrier = saved_carrier;
		CurrentPgBackend = saved_backend;
		PgSetCurrentSession(saved_session);
		CurrentPgConnection = saved_connection;
		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgRuntime = saved_runtime;
		CurrentPgCarrier = saved_carrier;
		CurrentPgBackend = saved_backend;
		PgSetCurrentSession(saved_session);
		CurrentPgConnection = saved_connection;
		CurrentPgExecution = saved_execution;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "thread backend runtime state was not initialized");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_thread_install_adopts_backend_fallback_state);
Datum
test_thread_install_adopts_backend_fallback_state(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgThreadBackendRuntimeState state;
	Latch		fake_latch;
	bool		ok = true;

	saved_backend = CurrentPgBackend;

	InitLatch(&fake_latch);

	PG_TRY();
	{
		CurrentPgBackend = NULL;
		PgCurrentWalSenderState()->is_walsender = true;
		PgCurrentReplicationState()->sync_rep_wait_mode = 101;
		PgCurrentLogicalReplicationState()->slotsync_sleep_ms = 102;
		PgCurrentXLogState()->local_xlog_insert_allowed = 103;
		PgCurrentRecoveryState()->standby_wait_us = 104;
		PgCurrentMaintenanceWorkerState()->walsummarizer_sleep_quanta = 105;
		PgCurrentAutovacuumState()->av_storage_param_cost_limit = 106;
		PgCurrentRepackState()->current_segment = 107;
		PgCurrentAioState()->my_io_worker_id = 108;
		InterruptPending = true;
		InterruptHoldoffCount = 109;

		InitializePgThreadRuntime(NULL);
		InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
											  &fake_latch);
		PgBackendAdoptEarlyState(&state.backend);

		ok = ok && state.backend.walsender.is_walsender;
		ok = ok && state.backend.replication.sync_rep_wait_mode == 101;
		ok = ok && state.backend.logical_replication.slotsync_sleep_ms == 102;
		ok = ok && dlist_is_empty(&state.backend.logical_replication.lsn_mapping);
		ok = ok && state.backend.xlog.local_xlog_insert_allowed == 103;
		ok = ok && state.backend.recovery.standby_wait_us == 104;
		ok = ok &&
			state.backend.maintenance_worker.walsummarizer_sleep_quanta == 105;
		ok = ok && state.backend.autovacuum.av_storage_param_cost_limit == 106;
		ok = ok && dlist_is_empty(&state.backend.autovacuum.database_list);
		ok = ok && state.backend.repack.current_segment == 107;
		ok = ok && state.backend.aio.my_io_worker_id == 108;
		ok = ok && state.backend.pending_interrupts.interrupt_pending;
		ok = ok &&
			state.backend.interrupt_holdoffs.interrupt_holdoff_count == 109;

		ok = ok && !PgCurrentWalSenderState()->is_walsender;
		ok = ok && PgCurrentReplicationState()->sync_rep_wait_mode == -1;
		ok = ok && PgCurrentLogicalReplicationState()->slotsync_sleep_ms ==
			PG_BACKEND_SLOTSYNC_INITIAL_SLEEP_MS;
		ok = ok && dlist_is_empty(&PgCurrentLogicalReplicationState()->lsn_mapping);
		ok = ok && PgCurrentXLogState()->local_xlog_insert_allowed == -1;
		ok = ok && PgCurrentRecoveryState()->standby_wait_us ==
			PG_BACKEND_STANDBY_INITIAL_WAIT_US;
		ok = ok &&
			PgCurrentMaintenanceWorkerState()->walsummarizer_sleep_quanta == 1;
		ok = ok && PgCurrentAutovacuumState()->av_storage_param_cost_limit == -1;
		ok = ok && dlist_is_empty(&PgCurrentAutovacuumState()->database_list);
		ok = ok && PgCurrentRepackState()->current_segment == 0;
		ok = ok && PgCurrentAioState()->my_io_worker_id == -1;
		ok = ok && !InterruptPending;
		ok = ok && InterruptHoldoffCount == 0;

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "thread backend install did not adopt backend fallback state");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_thread_install_adopts_session_execution_fallback_state);
Datum
test_thread_install_adopts_session_execution_fallback_state(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgExecution *saved_execution;
	PgSession	session;
	PgExecution execution;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_execution = CurrentPgExecution;
	MemSet(&session, 0, sizeof(session));
	MemSet(&execution, 0, sizeof(execution));

	PG_TRY();
	{
		PgSetCurrentSession(NULL);
		CurrentPgExecution = NULL;
		*PgCurrentDoingCommandReadRef() = true;
		*PgCurrentClientEncodingRef() = &pg_enc2name_tbl[PG_UTF8];
		*PgCurrentPendingClientEncodingRef() = PG_UTF8;
		*PgCurrentPseudoRandomSeedSetRef() = true;
		*PgCurrentDebugQueryStringRef() = "aggregate execution fallback";
		*PgCurrentSPIConnectedRef() = 17;
		*PgCurrentXactIsoLevelRef() = XACT_SERIALIZABLE;
		*PgCurrentGUCCheckErrcodeValueRef() = 503;
		*PgCurrentPendingActionsRef() = (struct ActionList *) &execution;
		*PgCurrentPendingListenActionsRef() = (HTAB *) &execution;
		*PgCurrentPendingNotifiesRef() = (struct NotificationList *) &execution;
		PgCurrentQueueHeadBeforeWriteRef()->page = 11;
		PgCurrentQueueHeadBeforeWriteRef()->offset = 12;
		PgCurrentQueueHeadAfterWriteRef()->page = 13;
		PgCurrentQueueHeadAfterWriteRef()->offset = 14;
		*PgCurrentSignalPidsRef() = (int32 *) &execution;
		*PgCurrentSignalProcnosRef() = (ProcNumber *) &execution;
		*PgCurrentTryAdvanceTailRef() = true;
		*PgCurrentTriggerDepthRef() = 88;
		*PgCurrentAfterTriggersDataRef() = &execution;
		*PgCurrentValgrindOldErrorCountRef() = 77;

		PgSessionAdoptEarlyState(&session);
		PgExecutionAdoptEarlyState(&execution);

		ok = ok && session.loop_state.doing_command_read;
		ok = ok && session.encoding.client_encoding == &pg_enc2name_tbl[PG_UTF8];
		ok = ok && session.encoding.pending_client_encoding == PG_UTF8;
		ok = ok && session.random.prng_seed_set;
		ok = ok && strcmp(*PgExecutionDebugQueryStringRef(&execution),
						  "aggregate execution fallback") == 0;
		ok = ok && execution.spi.connected == 17;
		ok = ok && execution.xact.iso_level == XACT_SERIALIZABLE;
		ok = ok && execution.guc_error.check_errcode_value == 503;
		ok = ok && execution.async.pending_actions ==
			(struct ActionList *) &execution;
		ok = ok && execution.async.pending_listen_actions ==
			(HTAB *) &execution;
		ok = ok && execution.async.pending_notifies ==
			(struct NotificationList *) &execution;
		ok = ok && execution.async.queue_head_before_write.page == 11;
		ok = ok && execution.async.queue_head_before_write.offset == 12;
		ok = ok && execution.async.queue_head_after_write.page == 13;
		ok = ok && execution.async.queue_head_after_write.offset == 14;
		ok = ok && execution.async.signal_pids == (int32 *) &execution;
		ok = ok && execution.async.signal_procnos == (ProcNumber *) &execution;
		ok = ok && execution.async.try_advance_tail;
		ok = ok && execution.trigger.depth == 88;
		ok = ok && execution.trigger.after_triggers_data == &execution;
		ok = ok && execution.valgrind.old_error_count == 77;

		ok = ok && !*PgCurrentDoingCommandReadRef();
		ok = ok && *PgCurrentClientEncodingRef() ==
			&pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentPendingClientEncodingRef() == PG_SQL_ASCII;
		ok = ok && !*PgCurrentPseudoRandomSeedSetRef();
		ok = ok && *PgCurrentDebugQueryStringRef() == NULL;
		ok = ok && *PgCurrentSPIConnectedRef() == -1;
		ok = ok && *PgCurrentXactIsoLevelRef() == XACT_READ_COMMITTED;
		ok = ok && *PgCurrentGUCCheckErrcodeValueRef() == 0;
		ok = ok && *PgCurrentPendingActionsRef() == NULL;
		ok = ok && *PgCurrentPendingListenActionsRef() == NULL;
		ok = ok && *PgCurrentPendingNotifiesRef() == NULL;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->page == 0;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->offset == 0;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->page == 0;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->offset == 0;
		ok = ok && *PgCurrentSignalPidsRef() == NULL;
		ok = ok && *PgCurrentSignalProcnosRef() == NULL;
		ok = ok && !*PgCurrentTryAdvanceTailRef();
		ok = ok && *PgCurrentTriggerDepthRef() == 0;
		ok = ok && *PgCurrentAfterTriggersDataRef() == NULL;
		ok = ok && *PgCurrentValgrindOldErrorCountRef() == 0;

		PgSetCurrentSession(saved_session);
		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		CurrentPgExecution = saved_execution;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR,
			 "thread backend install did not adopt session/execution fallback state");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_thread_install_adopts_connection_fallback_state);
Datum
test_thread_install_adopts_connection_fallback_state(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection connection;
	Port		fallback_port;
	Port		preserved_port;
	const PQcommMethods methods = {0};
	WaitEventSet *fake_wait_set;
	PgConnectionSocketIOState *socket_io;
	PgConnectionSecurityState *security;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	MemSet(&connection, 0, sizeof(connection));
	MemSet(&fallback_port, 0, sizeof(fallback_port));
	MemSet(&preserved_port, 0, sizeof(preserved_port));
	fake_wait_set = (WaitEventSet *) &connection;

	PG_TRY();
	{
		CurrentPgConnection = NULL;
		MyProcPort = &fallback_port;
		MyCancelKey[0] = 11;
		MyCancelKeyLength = 1;
		socket_io = PgCurrentConnectionSocketIORef();
		socket_io->send_buffer = (char *) "connection fallback";
		socket_io->send_buffer_size = 32;
		PqCommMethods = &methods;
		FeBeWaitSet = fake_wait_set;
		FrontendProtocol = PG_PROTOCOL(3, 2);
		whereToSendOutput = DestRemote;
		client_connection_check_interval = 13;
		CheckClientConnectionPending = true;
		ClientConnectionLost = true;
		ClientAuthInProgress = true;
		MyClientSocket = (struct ClientSocket *) &fallback_port;
		conn_timing.socket_create = 21;
		conn_timing.ready_for_use = 22;
		MyClientConnectionInfo.authn_id = "fallback-authn";
		MyClientConnectionInfo.auth_method = uaSCRAM;
		security = PgCurrentConnectionSecurityStateRef();
		security->ssl_loaded_verify_locations = true;
		security->gss_send_buffer = (char *) "gss-send";
		security->gss_send_length = 31;
		security->pam_password = "pam-fallback";
		security->pam_port = &fallback_port;
		security->pam_no_password = true;

		PgConnectionAdoptEarlyState(&connection, &preserved_port);

		CurrentPgConnection = &connection;
		ok = ok && MyProcPort == &preserved_port;
		ok = ok && MyCancelKey[0] == 11;
		ok = ok && MyCancelKeyLength == 1;
		ok = ok && strcmp(PgCurrentConnectionSocketIORef()->send_buffer,
						  "connection fallback") == 0;
		ok = ok && PgCurrentConnectionSocketIORef()->send_buffer_size == 32;
		ok = ok && PqCommMethods == &methods;
		ok = ok && FeBeWaitSet == fake_wait_set;
		ok = ok && FrontendProtocol == PG_PROTOCOL(3, 2);
		ok = ok && whereToSendOutput == DestRemote;
		ok = ok && client_connection_check_interval == 13;
		ok = ok && CheckClientConnectionPending;
		ok = ok && ClientConnectionLost;
		ok = ok && ClientAuthInProgress;
		ok = ok && MyClientSocket == (struct ClientSocket *) &fallback_port;
		ok = ok && conn_timing.socket_create == 21;
		ok = ok && conn_timing.ready_for_use == 22;
		ok = ok && strcmp(MyClientConnectionInfo.authn_id,
						  "fallback-authn") == 0;
		ok = ok && MyClientConnectionInfo.auth_method == uaSCRAM;
		ok = ok && PgCurrentConnectionSecurityStateRef()->ssl_loaded_verify_locations;
		ok = ok && strcmp(PgCurrentConnectionSecurityStateRef()->gss_send_buffer,
						  "gss-send") == 0;
		ok = ok && PgCurrentConnectionSecurityStateRef()->gss_send_length == 31;
		ok = ok && strcmp(PgCurrentConnectionSecurityStateRef()->pam_password,
						  "pam-fallback") == 0;
		ok = ok && PgCurrentConnectionSecurityStateRef()->pam_port ==
			&fallback_port;
		ok = ok && PgCurrentConnectionSecurityStateRef()->pam_no_password;

		CurrentPgConnection = NULL;
		ok = ok && MyProcPort == NULL;
		ok = ok && MyCancelKeyLength == 0;
		ok = ok && PgCurrentConnectionSocketIORef()->send_buffer == NULL;
		ok = ok && PgCurrentConnectionSocketIORef()->send_buffer_size == 0;
		ok = ok && PqCommMethods == NULL;
		ok = ok && FeBeWaitSet == NULL;
		ok = ok && FrontendProtocol == 0;
		ok = ok && whereToSendOutput == DestDebug;
		ok = ok && client_connection_check_interval == 0;
		ok = ok && !CheckClientConnectionPending;
		ok = ok && !ClientConnectionLost;
		ok = ok && !ClientAuthInProgress;
		ok = ok && MyClientSocket == NULL;
		ok = ok && conn_timing.socket_create == 0;
		ok = ok && conn_timing.ready_for_use == TIMESTAMP_MINUS_INFINITY;
		ok = ok && MyClientConnectionInfo.authn_id == NULL;
		ok = ok && !PgCurrentConnectionSecurityStateRef()->ssl_loaded_verify_locations;
		ok = ok && PgCurrentConnectionSecurityStateRef()->gss_send_buffer == NULL;
		ok = ok && PgCurrentConnectionSecurityStateRef()->pam_password == NULL;

		CurrentPgConnection = saved_connection;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR,
			 "thread backend install did not adopt connection fallback state");

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

PG_FUNCTION_INFO_V1(test_session_loop_state_is_session_local);
Datum
test_session_loop_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		CurrentPgSession = &fake_session1;
		CurrentPgSession->loop_state.send_ready_for_query = true;
		CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled = true;
		CurrentPgSession->loop_state.doing_extended_query_message = true;
		CurrentPgSession->loop_state.transaction_started = true;

		CurrentPgSession = &fake_session2;
		ok = ok && !CurrentPgSession->loop_state.send_ready_for_query;
		ok = ok && !CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled;
		ok = ok && !CurrentPgSession->loop_state.doing_extended_query_message;
		ok = ok && !CurrentPgSession->loop_state.transaction_started;
		CurrentPgSession->loop_state.send_ready_for_query = true;
		CurrentPgSession->loop_state.idle_session_timeout_enabled = true;
		CurrentPgSession->loop_state.ignore_till_sync = true;
		CurrentPgSession->loop_state.step_error_boundary_active = true;

		CurrentPgSession = &fake_session1;
		ok = ok && CurrentPgSession->loop_state.send_ready_for_query;
		ok = ok && CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled;
		ok = ok && !CurrentPgSession->loop_state.idle_session_timeout_enabled;
		ok = ok && CurrentPgSession->loop_state.doing_extended_query_message;
		ok = ok && !CurrentPgSession->loop_state.ignore_till_sync;
		ok = ok && !CurrentPgSession->loop_state.step_error_boundary_active;
		ok = ok && CurrentPgSession->loop_state.transaction_started;

		CurrentPgSession = &fake_session2;
		ok = ok && CurrentPgSession->loop_state.send_ready_for_query;
		ok = ok && !CurrentPgSession->loop_state.idle_in_transaction_timeout_enabled;
		ok = ok && CurrentPgSession->loop_state.idle_session_timeout_enabled;
		ok = ok && !CurrentPgSession->loop_state.doing_extended_query_message;
		ok = ok && CurrentPgSession->loop_state.ignore_till_sync;
		ok = ok && CurrentPgSession->loop_state.step_error_boundary_active;
		ok = ok && !CurrentPgSession->loop_state.transaction_started;

		CurrentPgSession = saved_session;
	}
	PG_CATCH();
	{
		CurrentPgSession = saved_session;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session loop state was not session-local");

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
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

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

PG_FUNCTION_INFO_V1(test_session_tablespace_state_is_session_local);
Datum
test_session_tablespace_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_default_tablespace;
	char	   *saved_temp_tablespaces;
	char	   *saved_allow_in_place_tablespaces;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_default_tablespace =
		pstrdup(GetConfigOption("default_tablespace", false, false));
	saved_temp_tablespaces =
		pstrdup(GetConfigOption("temp_tablespaces", false, false));
	saved_allow_in_place_tablespaces =
		pstrdup(GetConfigOption("allow_in_place_tablespaces", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && default_tablespace == NULL;
		ok = ok && temp_tablespaces == NULL;
		ok = ok && !allow_in_place_tablespaces;
		ok = ok && binary_upgrade_next_pg_tablespace_oid == InvalidOid;
		SetConfigOption("default_tablespace", "pg_default",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_tablespaces", "pg_default",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_in_place_tablespaces", "on",
						PGC_SUSET, PGC_S_SESSION);
		binary_upgrade_next_pg_tablespace_oid = 12345;
		ok = ok && strcmp(default_tablespace, "pg_default") == 0;
		ok = ok && strcmp(temp_tablespaces, "pg_default") == 0;
		ok = ok && allow_in_place_tablespaces;
		ok = ok && binary_upgrade_next_pg_tablespace_oid == 12345;

		PgSetCurrentSession(&fake_session2);
		ok = ok && default_tablespace == NULL;
		ok = ok && temp_tablespaces == NULL;
		ok = ok && !allow_in_place_tablespaces;
		ok = ok && binary_upgrade_next_pg_tablespace_oid == InvalidOid;
		SetConfigOption("default_tablespace", "",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_tablespaces", "",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_in_place_tablespaces", "off",
						PGC_SUSET, PGC_S_SESSION);
		binary_upgrade_next_pg_tablespace_oid = 67890;
		ok = ok && default_tablespace != NULL;
		ok = ok && default_tablespace[0] == '\0';
		ok = ok && temp_tablespaces != NULL;
		ok = ok && temp_tablespaces[0] == '\0';
		ok = ok && !allow_in_place_tablespaces;
		ok = ok && binary_upgrade_next_pg_tablespace_oid == 67890;

		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(default_tablespace, "pg_default") == 0;
		ok = ok && strcmp(temp_tablespaces, "pg_default") == 0;
		ok = ok && allow_in_place_tablespaces;
		ok = ok && binary_upgrade_next_pg_tablespace_oid == 12345;

		PgSetCurrentSession(saved_session);
		SetConfigOption("default_tablespace", saved_default_tablespace,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_tablespaces", saved_temp_tablespaces,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_in_place_tablespaces",
						saved_allow_in_place_tablespaces,
						PGC_SUSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("default_tablespace", saved_default_tablespace,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_tablespaces", saved_temp_tablespaces,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_in_place_tablespaces",
						saved_allow_in_place_tablespaces,
						PGC_SUSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session tablespace state was not session-local");

PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_binary_upgrade_state_is_session_local);
Datum
test_session_binary_upgrade_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	Oid			saved_pg_type_oid;
	Oid			saved_array_pg_type_oid;
	Oid			saved_mrng_pg_type_oid;
	Oid			saved_mrng_array_pg_type_oid;
	Oid			saved_heap_pg_class_oid;
	RelFileNumber saved_heap_pg_class_relfilenumber;
	Oid			saved_index_pg_class_oid;
	RelFileNumber saved_index_pg_class_relfilenumber;
	Oid			saved_toast_pg_class_oid;
	RelFileNumber saved_toast_pg_class_relfilenumber;
	Oid			saved_pg_enum_oid;
	Oid			saved_pg_authid_oid;
	bool		saved_record_init_privs;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_pg_type_oid = binary_upgrade_next_pg_type_oid;
	saved_array_pg_type_oid = binary_upgrade_next_array_pg_type_oid;
	saved_mrng_pg_type_oid = binary_upgrade_next_mrng_pg_type_oid;
	saved_mrng_array_pg_type_oid = binary_upgrade_next_mrng_array_pg_type_oid;
	saved_heap_pg_class_oid = binary_upgrade_next_heap_pg_class_oid;
	saved_heap_pg_class_relfilenumber =
		binary_upgrade_next_heap_pg_class_relfilenumber;
	saved_index_pg_class_oid = binary_upgrade_next_index_pg_class_oid;
	saved_index_pg_class_relfilenumber =
		binary_upgrade_next_index_pg_class_relfilenumber;
	saved_toast_pg_class_oid = binary_upgrade_next_toast_pg_class_oid;
	saved_toast_pg_class_relfilenumber =
		binary_upgrade_next_toast_pg_class_relfilenumber;
	saved_pg_enum_oid = binary_upgrade_next_pg_enum_oid;
	saved_pg_authid_oid = binary_upgrade_next_pg_authid_oid;
	saved_record_init_privs = binary_upgrade_record_init_privs;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && binary_upgrade_next_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_array_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_mrng_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_mrng_array_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_heap_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_heap_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_index_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_index_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_toast_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_toast_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_pg_enum_oid == InvalidOid;
		ok = ok && binary_upgrade_next_pg_authid_oid == InvalidOid;
		ok = ok && !binary_upgrade_record_init_privs;
		binary_upgrade_next_pg_type_oid = 1001;
		binary_upgrade_next_array_pg_type_oid = 1002;
		binary_upgrade_next_mrng_pg_type_oid = 1003;
		binary_upgrade_next_mrng_array_pg_type_oid = 1004;
		binary_upgrade_next_heap_pg_class_oid = 1005;
		binary_upgrade_next_heap_pg_class_relfilenumber = 1006;
		binary_upgrade_next_index_pg_class_oid = 1007;
		binary_upgrade_next_index_pg_class_relfilenumber = 1008;
		binary_upgrade_next_toast_pg_class_oid = 1009;
		binary_upgrade_next_toast_pg_class_relfilenumber = 1010;
		binary_upgrade_next_pg_enum_oid = 1011;
		binary_upgrade_next_pg_authid_oid = 1012;
		binary_upgrade_record_init_privs = true;

		PgSetCurrentSession(&fake_session2);
		ok = ok && binary_upgrade_next_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_array_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_mrng_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_mrng_array_pg_type_oid == InvalidOid;
		ok = ok && binary_upgrade_next_heap_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_heap_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_index_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_index_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_toast_pg_class_oid == InvalidOid;
		ok = ok && binary_upgrade_next_toast_pg_class_relfilenumber ==
			InvalidRelFileNumber;
		ok = ok && binary_upgrade_next_pg_enum_oid == InvalidOid;
		ok = ok && binary_upgrade_next_pg_authid_oid == InvalidOid;
		ok = ok && !binary_upgrade_record_init_privs;
		binary_upgrade_next_pg_type_oid = 2001;
		binary_upgrade_next_array_pg_type_oid = 2002;
		binary_upgrade_next_mrng_pg_type_oid = 2003;
		binary_upgrade_next_mrng_array_pg_type_oid = 2004;
		binary_upgrade_next_heap_pg_class_oid = 2005;
		binary_upgrade_next_heap_pg_class_relfilenumber = 2006;
		binary_upgrade_next_index_pg_class_oid = 2007;
		binary_upgrade_next_index_pg_class_relfilenumber = 2008;
		binary_upgrade_next_toast_pg_class_oid = 2009;
		binary_upgrade_next_toast_pg_class_relfilenumber = 2010;
		binary_upgrade_next_pg_enum_oid = 2011;
		binary_upgrade_next_pg_authid_oid = 2012;
		binary_upgrade_record_init_privs = false;

		PgSetCurrentSession(&fake_session1);
		ok = ok && binary_upgrade_next_pg_type_oid == 1001;
		ok = ok && binary_upgrade_next_array_pg_type_oid == 1002;
		ok = ok && binary_upgrade_next_mrng_pg_type_oid == 1003;
		ok = ok && binary_upgrade_next_mrng_array_pg_type_oid == 1004;
		ok = ok && binary_upgrade_next_heap_pg_class_oid == 1005;
		ok = ok && binary_upgrade_next_heap_pg_class_relfilenumber == 1006;
		ok = ok && binary_upgrade_next_index_pg_class_oid == 1007;
		ok = ok && binary_upgrade_next_index_pg_class_relfilenumber == 1008;
		ok = ok && binary_upgrade_next_toast_pg_class_oid == 1009;
		ok = ok && binary_upgrade_next_toast_pg_class_relfilenumber == 1010;
		ok = ok && binary_upgrade_next_pg_enum_oid == 1011;
		ok = ok && binary_upgrade_next_pg_authid_oid == 1012;
		ok = ok && binary_upgrade_record_init_privs;

		PgSetCurrentSession(&fake_session2);
		ok = ok && binary_upgrade_next_pg_type_oid == 2001;
		ok = ok && binary_upgrade_next_array_pg_type_oid == 2002;
		ok = ok && binary_upgrade_next_mrng_pg_type_oid == 2003;
		ok = ok && binary_upgrade_next_mrng_array_pg_type_oid == 2004;
		ok = ok && binary_upgrade_next_heap_pg_class_oid == 2005;
		ok = ok && binary_upgrade_next_heap_pg_class_relfilenumber == 2006;
		ok = ok && binary_upgrade_next_index_pg_class_oid == 2007;
		ok = ok && binary_upgrade_next_index_pg_class_relfilenumber == 2008;
		ok = ok && binary_upgrade_next_toast_pg_class_oid == 2009;
		ok = ok && binary_upgrade_next_toast_pg_class_relfilenumber == 2010;
		ok = ok && binary_upgrade_next_pg_enum_oid == 2011;
		ok = ok && binary_upgrade_next_pg_authid_oid == 2012;
		ok = ok && !binary_upgrade_record_init_privs;

		PgSetCurrentSession(saved_session);
		binary_upgrade_next_pg_type_oid = saved_pg_type_oid;
		binary_upgrade_next_array_pg_type_oid = saved_array_pg_type_oid;
		binary_upgrade_next_mrng_pg_type_oid = saved_mrng_pg_type_oid;
		binary_upgrade_next_mrng_array_pg_type_oid =
			saved_mrng_array_pg_type_oid;
		binary_upgrade_next_heap_pg_class_oid = saved_heap_pg_class_oid;
		binary_upgrade_next_heap_pg_class_relfilenumber =
			saved_heap_pg_class_relfilenumber;
		binary_upgrade_next_index_pg_class_oid = saved_index_pg_class_oid;
		binary_upgrade_next_index_pg_class_relfilenumber =
			saved_index_pg_class_relfilenumber;
		binary_upgrade_next_toast_pg_class_oid = saved_toast_pg_class_oid;
		binary_upgrade_next_toast_pg_class_relfilenumber =
			saved_toast_pg_class_relfilenumber;
		binary_upgrade_next_pg_enum_oid = saved_pg_enum_oid;
		binary_upgrade_next_pg_authid_oid = saved_pg_authid_oid;
		binary_upgrade_record_init_privs = saved_record_init_privs;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		binary_upgrade_next_pg_type_oid = saved_pg_type_oid;
		binary_upgrade_next_array_pg_type_oid = saved_array_pg_type_oid;
		binary_upgrade_next_mrng_pg_type_oid = saved_mrng_pg_type_oid;
		binary_upgrade_next_mrng_array_pg_type_oid =
			saved_mrng_array_pg_type_oid;
		binary_upgrade_next_heap_pg_class_oid = saved_heap_pg_class_oid;
		binary_upgrade_next_heap_pg_class_relfilenumber =
			saved_heap_pg_class_relfilenumber;
		binary_upgrade_next_index_pg_class_oid = saved_index_pg_class_oid;
		binary_upgrade_next_index_pg_class_relfilenumber =
			saved_index_pg_class_relfilenumber;
		binary_upgrade_next_toast_pg_class_oid = saved_toast_pg_class_oid;
		binary_upgrade_next_toast_pg_class_relfilenumber =
			saved_toast_pg_class_relfilenumber;
		binary_upgrade_next_pg_enum_oid = saved_pg_enum_oid;
		binary_upgrade_next_pg_authid_oid = saved_pg_authid_oid;
		binary_upgrade_record_init_privs = saved_record_init_privs;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session binary-upgrade state was not session-local");

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
	char	   *saved_timezone;
	char	   *saved_log_timezone;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_date_style = DateStyle;
	saved_date_order = DateOrder;
	saved_interval_style = pstrdup(GetConfigOption("IntervalStyle",
												   false, false));
	saved_timezone = pstrdup(GetConfigOption("TimeZone", false, false));
	saved_log_timezone = pstrdup(GetConfigOption("log_timezone",
												 false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && DateStyle == USE_ISO_DATES;
		ok = ok && DateOrder == DATEORDER_MDY;
		ok = ok && IntervalStyle == INTSTYLE_POSTGRES;
		ok = ok && strcmp(*PgCurrentTimeZoneStringRef(), "GMT") == 0;
		ok = ok && strcmp(*PgCurrentLogTimeZoneStringRef(), "GMT") == 0;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "GMT") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "GMT") == 0;
		DateStyle = USE_SQL_DATES;
		DateOrder = DATEORDER_DMY;
		SetConfigOption("IntervalStyle", "sql_standard",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("TimeZone", "UTC",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_timezone", "UTC",
						PGC_SIGHUP, PGC_S_FILE);
		ok = ok && IntervalStyle == INTSTYLE_SQL_STANDARD;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "UTC") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "UTC") == 0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DateStyle == USE_ISO_DATES;
		ok = ok && DateOrder == DATEORDER_MDY;
		ok = ok && IntervalStyle == INTSTYLE_POSTGRES;
		ok = ok && strcmp(*PgCurrentTimeZoneStringRef(), "GMT") == 0;
		ok = ok && strcmp(*PgCurrentLogTimeZoneStringRef(), "GMT") == 0;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "GMT") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "GMT") == 0;
		DateStyle = USE_GERMAN_DATES;
		DateOrder = DATEORDER_YMD;
		SetConfigOption("IntervalStyle", "iso_8601",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("TimeZone", "Europe/London",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_timezone", "Europe/London",
						PGC_SIGHUP, PGC_S_FILE);
		ok = ok && IntervalStyle == INTSTYLE_ISO_8601;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "Europe/London") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "Europe/London") == 0;

		PgSetCurrentSession(&fake_session1);
		ok = ok && DateStyle == USE_SQL_DATES;
		ok = ok && DateOrder == DATEORDER_DMY;
		ok = ok && IntervalStyle == INTSTYLE_SQL_STANDARD;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "UTC") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "UTC") == 0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DateStyle == USE_GERMAN_DATES;
		ok = ok && DateOrder == DATEORDER_YMD;
		ok = ok && IntervalStyle == INTSTYLE_ISO_8601;
		ok = ok && session_timezone != NULL &&
			strcmp(pg_get_timezone_name(session_timezone), "Europe/London") == 0;
		ok = ok && log_timezone != NULL &&
			strcmp(pg_get_timezone_name(log_timezone), "Europe/London") == 0;

		PgSetCurrentSession(saved_session);
		DateStyle = saved_date_style;
		DateOrder = saved_date_order;
		SetConfigOption("IntervalStyle", saved_interval_style,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("TimeZone", saved_timezone,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_timezone", saved_log_timezone,
						PGC_SIGHUP, PGC_S_FILE);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		DateStyle = saved_date_style;
		DateOrder = saved_date_order;
		SetConfigOption("IntervalStyle", saved_interval_style,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("TimeZone", saved_timezone,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_timezone", saved_log_timezone,
						PGC_SIGHUP, PGC_S_FILE);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session date/time GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_text_search_state_is_session_local);
Datum
test_session_text_search_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_text_search_config;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_text_search_config =
		pstrdup(GetConfigOption("default_text_search_config", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.simple") == 0;
		ok = ok && !OidIsValid(*PgCurrentTSCurrentConfigCacheRef());
		SetConfigOption("default_text_search_config", "pg_catalog.english",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.english") == 0;
		ok = ok && !OidIsValid(*PgCurrentTSCurrentConfigCacheRef());
		*PgCurrentTSCurrentConfigCacheRef() = 12345;
		*PgCurrentTSParserCacheHashRef() = (HTAB *) &fake_session1;
		*PgCurrentTSLastUsedParserRef() =
			(TSParserCacheEntry *) &fake_session1;
		*PgCurrentTSDictionaryCacheHashRef() = (HTAB *) &fake_session1;
		*PgCurrentTSLastUsedDictionaryRef() =
			(TSDictionaryCacheEntry *) &fake_session1;
		*PgCurrentTSConfigCacheHashRef() = (HTAB *) &fake_session1;
		*PgCurrentTSLastUsedConfigRef() =
			(TSConfigCacheEntry *) &fake_session1;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.simple") == 0;
		ok = ok && !OidIsValid(*PgCurrentTSCurrentConfigCacheRef());
		ok = ok && *PgCurrentTSParserCacheHashRef() == NULL;
		ok = ok && *PgCurrentTSLastUsedParserRef() == NULL;
		ok = ok && *PgCurrentTSDictionaryCacheHashRef() == NULL;
		ok = ok && *PgCurrentTSLastUsedDictionaryRef() == NULL;
		ok = ok && *PgCurrentTSConfigCacheHashRef() == NULL;
		ok = ok && *PgCurrentTSLastUsedConfigRef() == NULL;
		SetConfigOption("default_text_search_config", "pg_catalog.simple",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.simple") == 0;
		*PgCurrentTSCurrentConfigCacheRef() = 67890;
		*PgCurrentTSParserCacheHashRef() = (HTAB *) &fake_session2;
		*PgCurrentTSLastUsedParserRef() =
			(TSParserCacheEntry *) &fake_session2;
		*PgCurrentTSDictionaryCacheHashRef() = (HTAB *) &fake_session2;
		*PgCurrentTSLastUsedDictionaryRef() =
			(TSDictionaryCacheEntry *) &fake_session2;
		*PgCurrentTSConfigCacheHashRef() = (HTAB *) &fake_session2;
		*PgCurrentTSLastUsedConfigRef() =
			(TSConfigCacheEntry *) &fake_session2;

		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.english") == 0;
		ok = ok && *PgCurrentTSCurrentConfigCacheRef() == 12345;
		ok = ok && *PgCurrentTSParserCacheHashRef() ==
			(HTAB *) &fake_session1;
		ok = ok && *PgCurrentTSLastUsedParserRef() ==
			(TSParserCacheEntry *) &fake_session1;
		ok = ok && *PgCurrentTSDictionaryCacheHashRef() ==
			(HTAB *) &fake_session1;
		ok = ok && *PgCurrentTSLastUsedDictionaryRef() ==
			(TSDictionaryCacheEntry *) &fake_session1;
		ok = ok && *PgCurrentTSConfigCacheHashRef() ==
			(HTAB *) &fake_session1;
		ok = ok && *PgCurrentTSLastUsedConfigRef() ==
			(TSConfigCacheEntry *) &fake_session1;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(TSCurrentConfig, "pg_catalog.simple") == 0;
		ok = ok && *PgCurrentTSCurrentConfigCacheRef() == 67890;
		ok = ok && *PgCurrentTSParserCacheHashRef() ==
			(HTAB *) &fake_session2;
		ok = ok && *PgCurrentTSLastUsedParserRef() ==
			(TSParserCacheEntry *) &fake_session2;
		ok = ok && *PgCurrentTSDictionaryCacheHashRef() ==
			(HTAB *) &fake_session2;
		ok = ok && *PgCurrentTSLastUsedDictionaryRef() ==
			(TSDictionaryCacheEntry *) &fake_session2;
		ok = ok && *PgCurrentTSConfigCacheHashRef() ==
			(HTAB *) &fake_session2;
		ok = ok && *PgCurrentTSLastUsedConfigRef() ==
			(TSConfigCacheEntry *) &fake_session2;

		PgSetCurrentSession(saved_session);
		SetConfigOption("default_text_search_config",
						saved_text_search_config,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("default_text_search_config",
						saved_text_search_config,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session text-search state was not session-local");

PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_on_commit_state_is_session_local);
Datum
test_session_on_commit_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	List	   *saved_on_commits;
	List	   *session1_marker;
	List	   *session2_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_on_commits = *PgCurrentOnCommitActionsRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_marker = (List *) &fake_session1;
	session2_marker = (List *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentOnCommitActionsRef() == NIL;
		*PgCurrentOnCommitActionsRef() = session1_marker;
		ok = ok && *PgCurrentOnCommitActionsRef() == session1_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentOnCommitActionsRef() == NIL;
		*PgCurrentOnCommitActionsRef() = session2_marker;
		ok = ok && *PgCurrentOnCommitActionsRef() == session2_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentOnCommitActionsRef() == session1_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentOnCommitActionsRef() == session2_marker;

		PgSetCurrentSession(saved_session);
		*PgCurrentOnCommitActionsRef() = saved_on_commits;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentOnCommitActionsRef() = saved_on_commits;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "ON COMMIT state was not session-local");

PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_sequence_state_is_session_local);
Datum
test_session_sequence_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	HTAB	   *saved_seqhashtab;
	struct SeqTableData *saved_last_used_seq;
	HTAB	   *session1_hash_marker;
	HTAB	   *session2_hash_marker;
	struct SeqTableData *session1_last_marker;
	struct SeqTableData *session2_last_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_seqhashtab = *PgCurrentSequenceHashTableRef();
	saved_last_used_seq = *PgCurrentLastUsedSequenceRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_hash_marker = (HTAB *) &fake_session1;
	session2_hash_marker = (HTAB *) &fake_session2;
	session1_last_marker = (struct SeqTableData *) &fake_session1;
	session2_last_marker = (struct SeqTableData *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentSequenceHashTableRef() == NULL;
		ok = ok && *PgCurrentLastUsedSequenceRef() == NULL;
		*PgCurrentSequenceHashTableRef() = session1_hash_marker;
		*PgCurrentLastUsedSequenceRef() = session1_last_marker;
		ok = ok && *PgCurrentSequenceHashTableRef() == session1_hash_marker;
		ok = ok && *PgCurrentLastUsedSequenceRef() == session1_last_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentSequenceHashTableRef() == NULL;
		ok = ok && *PgCurrentLastUsedSequenceRef() == NULL;
		*PgCurrentSequenceHashTableRef() = session2_hash_marker;
		*PgCurrentLastUsedSequenceRef() = session2_last_marker;
		ok = ok && *PgCurrentSequenceHashTableRef() == session2_hash_marker;
		ok = ok && *PgCurrentLastUsedSequenceRef() == session2_last_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentSequenceHashTableRef() == session1_hash_marker;
		ok = ok && *PgCurrentLastUsedSequenceRef() == session1_last_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentSequenceHashTableRef() == session2_hash_marker;
		ok = ok && *PgCurrentLastUsedSequenceRef() == session2_last_marker;

		PgSetCurrentSession(saved_session);
		*PgCurrentSequenceHashTableRef() = saved_seqhashtab;
		*PgCurrentLastUsedSequenceRef() = saved_last_used_seq;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentSequenceHashTableRef() = saved_seqhashtab;
		*PgCurrentLastUsedSequenceRef() = saved_last_used_seq;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "sequence state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_large_object_state_is_session_local);
Datum
test_session_large_object_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	Relation	saved_heap_relation;
	Relation	saved_index_relation;
	Relation	session1_heap_marker;
	Relation	session1_index_marker;
	Relation	session2_heap_marker;
	Relation	session2_index_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_heap_relation = *PgCurrentLargeObjectHeapRelationRef();
	saved_index_relation = *PgCurrentLargeObjectIndexRelationRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_heap_marker = (Relation) &fake_session1;
	session1_index_marker = (Relation) &fake_session2;
	session2_heap_marker = (Relation) &saved_session;
	session2_index_marker = (Relation) &saved_heap_relation;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == NULL;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == NULL;
		*PgCurrentLargeObjectHeapRelationRef() = session1_heap_marker;
		*PgCurrentLargeObjectIndexRelationRef() = session1_index_marker;
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == session1_heap_marker;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == session1_index_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == NULL;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == NULL;
		*PgCurrentLargeObjectHeapRelationRef() = session2_heap_marker;
		*PgCurrentLargeObjectIndexRelationRef() = session2_index_marker;
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == session2_heap_marker;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == session2_index_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == session1_heap_marker;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == session1_index_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentLargeObjectHeapRelationRef() == session2_heap_marker;
		ok = ok && *PgCurrentLargeObjectIndexRelationRef() == session2_index_marker;

		PgSetCurrentSession(saved_session);
		*PgCurrentLargeObjectHeapRelationRef() = saved_heap_relation;
		*PgCurrentLargeObjectIndexRelationRef() = saved_index_relation;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentLargeObjectHeapRelationRef() = saved_heap_relation;
		*PgCurrentLargeObjectIndexRelationRef() = saved_index_relation;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "large-object state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_async_state_is_session_local);
Datum
test_session_async_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	HTAB	   *saved_local_channel_table;
	bool		saved_registered_listener;
	HTAB	   *session1_table_marker;
	HTAB	   *session2_table_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_local_channel_table = *PgCurrentAsyncLocalChannelTableRef();
	saved_registered_listener = *PgCurrentAsyncRegisteredListenerRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_table_marker = (HTAB *) &fake_session1;
	session2_table_marker = (HTAB *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == NULL;
		ok = ok && !*PgCurrentAsyncRegisteredListenerRef();
		*PgCurrentAsyncLocalChannelTableRef() = session1_table_marker;
		*PgCurrentAsyncRegisteredListenerRef() = true;
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == session1_table_marker;
		ok = ok && *PgCurrentAsyncRegisteredListenerRef();

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == NULL;
		ok = ok && !*PgCurrentAsyncRegisteredListenerRef();
		*PgCurrentAsyncLocalChannelTableRef() = session2_table_marker;
		*PgCurrentAsyncRegisteredListenerRef() = false;
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == session2_table_marker;
		ok = ok && !*PgCurrentAsyncRegisteredListenerRef();

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == session1_table_marker;
		ok = ok && *PgCurrentAsyncRegisteredListenerRef();

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentAsyncLocalChannelTableRef() == session2_table_marker;
		ok = ok && !*PgCurrentAsyncRegisteredListenerRef();

		PgSetCurrentSession(saved_session);
		*PgCurrentAsyncLocalChannelTableRef() = saved_local_channel_table;
		*PgCurrentAsyncRegisteredListenerRef() = saved_registered_listener;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentAsyncLocalChannelTableRef() = saved_local_channel_table;
		*PgCurrentAsyncRegisteredListenerRef() = saved_registered_listener;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "async listener state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_encoding_state_is_session_local);
Datum
test_session_encoding_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	List	   *saved_conv_proc_list;
	FmgrInfo   *saved_to_server_conv_proc;
	FmgrInfo   *saved_to_client_conv_proc;
	FmgrInfo   *saved_utf8_to_server_conv_proc;
	const pg_enc2name *saved_client_encoding;
	const pg_enc2name *saved_database_encoding;
	const pg_enc2name *saved_message_encoding;
	bool		saved_startup_complete;
	int			saved_pending_client_encoding;
	List	   *session1_list_marker;
	List	   *session2_list_marker;
	FmgrInfo   *session1_to_server_marker;
	FmgrInfo   *session1_to_client_marker;
	FmgrInfo   *session1_utf8_marker;
	FmgrInfo   *session2_to_server_marker;
	FmgrInfo   *session2_to_client_marker;
	FmgrInfo   *session2_utf8_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_conv_proc_list = *PgCurrentEncodingConvProcListRef();
	saved_to_server_conv_proc = *PgCurrentToServerConvProcRef();
	saved_to_client_conv_proc = *PgCurrentToClientConvProcRef();
	saved_utf8_to_server_conv_proc = *PgCurrentUtf8ToServerConvProcRef();
	saved_client_encoding = *PgCurrentClientEncodingRef();
	saved_database_encoding = *PgCurrentDatabaseEncodingRef();
	saved_message_encoding = *PgCurrentMessageEncodingRef();
	saved_startup_complete = *PgCurrentEncodingStartupCompleteRef();
	saved_pending_client_encoding = *PgCurrentPendingClientEncodingRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_list_marker = (List *) &fake_session1;
	session2_list_marker = (List *) &fake_session2;
	session1_to_server_marker = (FmgrInfo *) &fake_session1;
	session1_to_client_marker = (FmgrInfo *) &fake_session2;
	session1_utf8_marker = (FmgrInfo *) &saved_session;
	session2_to_server_marker = (FmgrInfo *) &saved_conv_proc_list;
	session2_to_client_marker = (FmgrInfo *) &saved_to_server_conv_proc;
	session2_utf8_marker = (FmgrInfo *) &saved_to_client_conv_proc;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentEncodingConvProcListRef() == NIL;
		ok = ok && *PgCurrentToServerConvProcRef() == NULL;
		ok = ok && *PgCurrentToClientConvProcRef() == NULL;
		ok = ok && *PgCurrentUtf8ToServerConvProcRef() == NULL;
		ok = ok && *PgCurrentClientEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentDatabaseEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentMessageEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && !*PgCurrentEncodingStartupCompleteRef();
		ok = ok && *PgCurrentPendingClientEncodingRef() == PG_SQL_ASCII;
		*PgCurrentEncodingConvProcListRef() = session1_list_marker;
		*PgCurrentToServerConvProcRef() = session1_to_server_marker;
		*PgCurrentToClientConvProcRef() = session1_to_client_marker;
		*PgCurrentUtf8ToServerConvProcRef() = session1_utf8_marker;
		*PgCurrentClientEncodingRef() = &pg_enc2name_tbl[PG_UTF8];
		*PgCurrentDatabaseEncodingRef() = &pg_enc2name_tbl[PG_UTF8];
		*PgCurrentMessageEncodingRef() = &pg_enc2name_tbl[PG_UTF8];
		*PgCurrentEncodingStartupCompleteRef() = true;
		*PgCurrentPendingClientEncodingRef() = PG_UTF8;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentEncodingConvProcListRef() == NIL;
		ok = ok && *PgCurrentToServerConvProcRef() == NULL;
		ok = ok && *PgCurrentToClientConvProcRef() == NULL;
		ok = ok && *PgCurrentUtf8ToServerConvProcRef() == NULL;
		ok = ok && *PgCurrentClientEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentDatabaseEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentMessageEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && !*PgCurrentEncodingStartupCompleteRef();
		ok = ok && *PgCurrentPendingClientEncodingRef() == PG_SQL_ASCII;
		*PgCurrentEncodingConvProcListRef() = session2_list_marker;
		*PgCurrentToServerConvProcRef() = session2_to_server_marker;
		*PgCurrentToClientConvProcRef() = session2_to_client_marker;
		*PgCurrentUtf8ToServerConvProcRef() = session2_utf8_marker;
		*PgCurrentClientEncodingRef() = &pg_enc2name_tbl[PG_SQL_ASCII];
		*PgCurrentDatabaseEncodingRef() = &pg_enc2name_tbl[PG_SQL_ASCII];
		*PgCurrentMessageEncodingRef() = &pg_enc2name_tbl[PG_SQL_ASCII];
		*PgCurrentEncodingStartupCompleteRef() = false;
		*PgCurrentPendingClientEncodingRef() = PG_SQL_ASCII;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentEncodingConvProcListRef() == session1_list_marker;
		ok = ok && *PgCurrentToServerConvProcRef() == session1_to_server_marker;
		ok = ok && *PgCurrentToClientConvProcRef() == session1_to_client_marker;
		ok = ok && *PgCurrentUtf8ToServerConvProcRef() == session1_utf8_marker;
		ok = ok && *PgCurrentClientEncodingRef() == &pg_enc2name_tbl[PG_UTF8];
		ok = ok && *PgCurrentDatabaseEncodingRef() == &pg_enc2name_tbl[PG_UTF8];
		ok = ok && *PgCurrentMessageEncodingRef() == &pg_enc2name_tbl[PG_UTF8];
		ok = ok && *PgCurrentEncodingStartupCompleteRef();
		ok = ok && *PgCurrentPendingClientEncodingRef() == PG_UTF8;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentEncodingConvProcListRef() == session2_list_marker;
		ok = ok && *PgCurrentToServerConvProcRef() == session2_to_server_marker;
		ok = ok && *PgCurrentToClientConvProcRef() == session2_to_client_marker;
		ok = ok && *PgCurrentUtf8ToServerConvProcRef() == session2_utf8_marker;
		ok = ok && *PgCurrentClientEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentDatabaseEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && *PgCurrentMessageEncodingRef() == &pg_enc2name_tbl[PG_SQL_ASCII];
		ok = ok && !*PgCurrentEncodingStartupCompleteRef();
		ok = ok && *PgCurrentPendingClientEncodingRef() == PG_SQL_ASCII;

		PgSetCurrentSession(saved_session);
		*PgCurrentEncodingConvProcListRef() = saved_conv_proc_list;
		*PgCurrentToServerConvProcRef() = saved_to_server_conv_proc;
		*PgCurrentToClientConvProcRef() = saved_to_client_conv_proc;
		*PgCurrentUtf8ToServerConvProcRef() = saved_utf8_to_server_conv_proc;
		*PgCurrentClientEncodingRef() = saved_client_encoding;
		*PgCurrentDatabaseEncodingRef() = saved_database_encoding;
		*PgCurrentMessageEncodingRef() = saved_message_encoding;
		*PgCurrentEncodingStartupCompleteRef() = saved_startup_complete;
		*PgCurrentPendingClientEncodingRef() = saved_pending_client_encoding;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentEncodingConvProcListRef() = saved_conv_proc_list;
		*PgCurrentToServerConvProcRef() = saved_to_server_conv_proc;
		*PgCurrentToClientConvProcRef() = saved_to_client_conv_proc;
		*PgCurrentUtf8ToServerConvProcRef() = saved_utf8_to_server_conv_proc;
		*PgCurrentClientEncodingRef() = saved_client_encoding;
		*PgCurrentDatabaseEncodingRef() = saved_database_encoding;
		*PgCurrentMessageEncodingRef() = saved_message_encoding;
		*PgCurrentEncodingStartupCompleteRef() = saved_startup_complete;
		*PgCurrentPendingClientEncodingRef() = saved_pending_client_encoding;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "encoding state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_temp_file_state_is_session_local);
Datum
test_session_temp_file_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	uint64		saved_temporary_files_size;
	long		saved_temp_file_counter;
	Oid		   *saved_temp_table_spaces;
	int			saved_num_temp_table_spaces;
	int			saved_next_temp_table_space;
	Oid			session1_table_spaces[2] = {1111, 2222};
	Oid			session2_table_spaces[1] = {3333};
	Oid			copied_table_spaces[2];
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_temporary_files_size = *PgCurrentTemporaryFilesSizeRef();
	saved_temp_file_counter = *PgCurrentTempFileCounterRef();
	saved_temp_table_spaces = *PgCurrentTempTableSpaceOidsRef();
	saved_num_temp_table_spaces = *PgCurrentNumTempTableSpacesRef();
	saved_next_temp_table_space = *PgCurrentNextTempTableSpaceRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentTemporaryFilesSizeRef() == 0;
		ok = ok && *PgCurrentTempFileCounterRef() == 0;
		ok = ok && *PgCurrentTempTableSpaceOidsRef() == NULL;
		ok = ok && !TempTablespacesAreSet();
		ok = ok && *PgCurrentNextTempTableSpaceRef() == 0;
		*PgCurrentTemporaryFilesSizeRef() = 1234;
		*PgCurrentTempFileCounterRef() = 42;
		SetTempTablespaces(session1_table_spaces, lengthof(session1_table_spaces));
		ok = ok && TempTablespacesAreSet();
		ok = ok && GetTempTablespaces(copied_table_spaces,
									  lengthof(copied_table_spaces)) == 2;
		ok = ok && copied_table_spaces[0] == session1_table_spaces[0];
		ok = ok && copied_table_spaces[1] == session1_table_spaces[1];

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentTemporaryFilesSizeRef() == 0;
		ok = ok && *PgCurrentTempFileCounterRef() == 0;
		ok = ok && *PgCurrentTempTableSpaceOidsRef() == NULL;
		ok = ok && !TempTablespacesAreSet();
		ok = ok && *PgCurrentNextTempTableSpaceRef() == 0;
		*PgCurrentTemporaryFilesSizeRef() = 9876;
		*PgCurrentTempFileCounterRef() = 84;
		SetTempTablespaces(session2_table_spaces, lengthof(session2_table_spaces));
		ok = ok && TempTablespacesAreSet();
		ok = ok && GetTempTablespaces(copied_table_spaces,
									  lengthof(copied_table_spaces)) == 1;
		ok = ok && copied_table_spaces[0] == session2_table_spaces[0];

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentTemporaryFilesSizeRef() == 1234;
		ok = ok && *PgCurrentTempFileCounterRef() == 42;
		ok = ok && TempTablespacesAreSet();
		ok = ok && GetTempTablespaces(copied_table_spaces,
									  lengthof(copied_table_spaces)) == 2;
		ok = ok && copied_table_spaces[0] == session1_table_spaces[0];
		ok = ok && copied_table_spaces[1] == session1_table_spaces[1];

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentTemporaryFilesSizeRef() == 9876;
		ok = ok && *PgCurrentTempFileCounterRef() == 84;
		ok = ok && TempTablespacesAreSet();
		ok = ok && GetTempTablespaces(copied_table_spaces,
									  lengthof(copied_table_spaces)) == 1;
		ok = ok && copied_table_spaces[0] == session2_table_spaces[0];

		PgSetCurrentSession(saved_session);
		*PgCurrentTemporaryFilesSizeRef() = saved_temporary_files_size;
		*PgCurrentTempFileCounterRef() = saved_temp_file_counter;
		*PgCurrentTempTableSpaceOidsRef() = saved_temp_table_spaces;
		*PgCurrentNumTempTableSpacesRef() = saved_num_temp_table_spaces;
		*PgCurrentNextTempTableSpaceRef() = saved_next_temp_table_space;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentTemporaryFilesSizeRef() = saved_temporary_files_size;
		*PgCurrentTempFileCounterRef() = saved_temp_file_counter;
		*PgCurrentTempTableSpaceOidsRef() = saved_temp_table_spaces;
		*PgCurrentNumTempTableSpacesRef() = saved_num_temp_table_spaces;
		*PgCurrentNextTempTableSpaceRef() = saved_next_temp_table_space;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "temporary file state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_random_state_is_session_local);
Datum
test_session_random_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	pg_prng_state expected1;
	pg_prng_state expected2;
	float8		expected1_first;
	float8		expected1_second;
	float8		expected2_first;
	float8		expected2_second;
	float8		session1_first = 0;
	float8		session1_second = 0;
	float8		session2_first = 0;
	float8		session2_second = 0;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	pg_prng_fseed(&expected1, 0.25);
	expected1_first = pg_prng_double(&expected1);
	expected1_second = pg_prng_double(&expected1);
	pg_prng_fseed(&expected2, -0.5);
	expected2_first = pg_prng_double(&expected2);
	expected2_second = pg_prng_double(&expected2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !*PgCurrentPseudoRandomSeedSetRef();
		DirectFunctionCall1(setseed, Float8GetDatum(0.25));
		ok = ok && *PgCurrentPseudoRandomSeedSetRef();
		session1_first = DatumGetFloat8(OidFunctionCall0(F_RANDOM_));

		PgSetCurrentSession(&fake_session2);
		ok = ok && !*PgCurrentPseudoRandomSeedSetRef();
		DirectFunctionCall1(setseed, Float8GetDatum(-0.5));
		ok = ok && *PgCurrentPseudoRandomSeedSetRef();
		session2_first = DatumGetFloat8(OidFunctionCall0(F_RANDOM_));

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPseudoRandomSeedSetRef();
		session1_second = DatumGetFloat8(OidFunctionCall0(F_RANDOM_));

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPseudoRandomSeedSetRef();
		session2_second = DatumGetFloat8(OidFunctionCall0(F_RANDOM_));

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ok = ok && session1_first == expected1_first;
	ok = ok && session1_second == expected1_second;
	ok = ok && session2_first == expected2_first;
	ok = ok && session2_second == expected2_second;

	if (!ok)
		elog(ERROR, "random state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_optimizer_state_is_session_local);
Datum
test_session_optimizer_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	HTAB	   *session1_proof_marker;
	HTAB	   *session2_proof_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_proof_marker = (HTAB *) &fake_session1;
	session2_proof_marker = (HTAB *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPlannerExtensionNameArrayRef() == NULL;
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 0;
		ok = ok && *PgCurrentPlannerExtensionNamesAllocatedRef() == 0;
		ok = ok && GetPlannerExtensionId("phase12_optimizer_a") == 0;
		ok = ok && GetPlannerExtensionId("phase12_optimizer_b") == 1;
		ok = ok && GetPlannerExtensionId("phase12_optimizer_a") == 0;
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 2;
		ok = ok && *PgCurrentOprProofCacheHashRef() == NULL;
		*PgCurrentOprProofCacheHashRef() = session1_proof_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPlannerExtensionNameArrayRef() == NULL;
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 0;
		ok = ok && *PgCurrentPlannerExtensionNamesAllocatedRef() == 0;
		ok = ok && GetPlannerExtensionId("phase12_optimizer_b") == 0;
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 1;
		ok = ok && *PgCurrentOprProofCacheHashRef() == NULL;
		*PgCurrentOprProofCacheHashRef() = session2_proof_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 2;
		ok = ok && *PgCurrentOprProofCacheHashRef() == session1_proof_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPlannerExtensionNamesAssignedRef() == 1;
		ok = ok && *PgCurrentOprProofCacheHashRef() == session2_proof_marker;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "optimizer state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_plan_cache_state_is_session_local);
Datum
test_session_plan_cache_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	dlist_node	session1_saved_node;
	dlist_node	session1_expr_node;
	dlist_node	session2_saved_node;
	dlist_node	session2_expr_node;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	dlist_node_init(&session1_saved_node);
	dlist_node_init(&session1_expr_node);
	dlist_node_init(&session2_saved_node);
	dlist_node_init(&session2_expr_node);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && dlist_is_empty(PgCurrentSavedPlanListRef());
		ok = ok && dlist_is_empty(PgCurrentCachedExpressionListRef());
		dlist_push_tail(PgCurrentSavedPlanListRef(), &session1_saved_node);
		dlist_push_tail(PgCurrentCachedExpressionListRef(), &session1_expr_node);
		ok = ok && !dlist_is_empty(PgCurrentSavedPlanListRef());
		ok = ok && !dlist_is_empty(PgCurrentCachedExpressionListRef());

		PgSetCurrentSession(&fake_session2);
		ok = ok && dlist_is_empty(PgCurrentSavedPlanListRef());
		ok = ok && dlist_is_empty(PgCurrentCachedExpressionListRef());
		dlist_push_tail(PgCurrentSavedPlanListRef(), &session2_saved_node);
		dlist_push_tail(PgCurrentCachedExpressionListRef(), &session2_expr_node);
		ok = ok && !dlist_is_empty(PgCurrentSavedPlanListRef());
		ok = ok && !dlist_is_empty(PgCurrentCachedExpressionListRef());

		PgSetCurrentSession(&fake_session1);
		ok = ok && PgCurrentSavedPlanListRef()->head.next == &session1_saved_node;
		ok = ok && PgCurrentCachedExpressionListRef()->head.next == &session1_expr_node;

		PgSetCurrentSession(&fake_session2);
		ok = ok && PgCurrentSavedPlanListRef()->head.next == &session2_saved_node;
		ok = ok && PgCurrentCachedExpressionListRef()->head.next == &session2_expr_node;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "plan cache state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_namespace_state_is_session_local);
Datum
test_session_namespace_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	PgSessionNamespaceState *namespace_state;
	List	   *session1_path;
	List	   *session2_path;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_path = list_make2_oid(11, 12);
	session2_path = list_make1_oid(21);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		namespace_state = PgCurrentNamespaceState();
		ok = ok && namespace_state->initialized;
		ok = ok && namespace_state->active_path_generation == 1;
		ok = ok && namespace_state->base_search_path_valid;
		ok = ok && namespace_state->my_temp_namespace == InvalidOid;
		namespace_state->active_search_path = session1_path;
		namespace_state->active_creation_namespace = 11;
		namespace_state->active_temp_creation_pending = true;
		namespace_state->active_path_generation = 101;
		namespace_state->base_search_path = session1_path;
		namespace_state->base_creation_namespace = 12;
		namespace_state->base_temp_creation_pending = true;
		namespace_state->namespace_user = 10;
		namespace_state->base_search_path_valid = false;
		namespace_state->search_path_cache_valid = true;
		namespace_state->my_temp_namespace = 13;
		namespace_state->my_temp_toast_namespace = 14;
		namespace_state->my_temp_namespace_subid = 15;
		namespace_state->namespace_search_path_value = "phase12_namespace_a";
		namespace_state->search_path_cache = &fake_session1;
		namespace_state->last_search_path_cache_entry = &fake_session1;

		PgSetCurrentSession(&fake_session2);
		namespace_state = PgCurrentNamespaceState();
		ok = ok && namespace_state->initialized;
		ok = ok && namespace_state->active_search_path == NIL;
		ok = ok && namespace_state->active_path_generation == 1;
		ok = ok && namespace_state->base_search_path_valid;
		ok = ok && !namespace_state->search_path_cache_valid;
		ok = ok && namespace_state->my_temp_namespace == InvalidOid;
		ok = ok && namespace_state->namespace_search_path_value == NULL;
		namespace_state->active_search_path = session2_path;
		namespace_state->active_creation_namespace = 21;
		namespace_state->active_path_generation = 202;
		namespace_state->base_search_path = session2_path;
		namespace_state->base_creation_namespace = 22;
		namespace_state->namespace_user = 20;
		namespace_state->my_temp_namespace = 23;
		namespace_state->my_temp_toast_namespace = 24;
		namespace_state->namespace_search_path_value = "phase12_namespace_b";
		namespace_state->search_path_cache = &fake_session2;
		namespace_state->last_search_path_cache_entry = &fake_session2;

		PgSetCurrentSession(&fake_session1);
		namespace_state = PgCurrentNamespaceState();
		ok = ok && namespace_state->active_search_path == session1_path;
		ok = ok && namespace_state->active_creation_namespace == 11;
		ok = ok && namespace_state->active_temp_creation_pending;
		ok = ok && namespace_state->active_path_generation == 101;
		ok = ok && namespace_state->base_search_path == session1_path;
		ok = ok && namespace_state->base_creation_namespace == 12;
		ok = ok && namespace_state->base_temp_creation_pending;
		ok = ok && namespace_state->namespace_user == 10;
		ok = ok && !namespace_state->base_search_path_valid;
		ok = ok && namespace_state->search_path_cache_valid;
		ok = ok && namespace_state->my_temp_namespace == 13;
		ok = ok && namespace_state->my_temp_toast_namespace == 14;
		ok = ok && namespace_state->my_temp_namespace_subid == 15;
		ok = ok && strcmp(namespace_state->namespace_search_path_value,
						  "phase12_namespace_a") == 0;
		ok = ok && namespace_state->search_path_cache == &fake_session1;
		ok = ok && namespace_state->last_search_path_cache_entry == &fake_session1;

		PgSetCurrentSession(&fake_session2);
		namespace_state = PgCurrentNamespaceState();
		ok = ok && namespace_state->active_search_path == session2_path;
		ok = ok && namespace_state->active_creation_namespace == 21;
		ok = ok && namespace_state->active_path_generation == 202;
		ok = ok && namespace_state->base_search_path == session2_path;
		ok = ok && namespace_state->base_creation_namespace == 22;
		ok = ok && namespace_state->namespace_user == 20;
		ok = ok && namespace_state->my_temp_namespace == 23;
		ok = ok && namespace_state->my_temp_toast_namespace == 24;
		ok = ok && strcmp(namespace_state->namespace_search_path_value,
						  "phase12_namespace_b") == 0;
		ok = ok && namespace_state->search_path_cache == &fake_session2;
		ok = ok && namespace_state->last_search_path_cache_entry == &fake_session2;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "namespace state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_locale_state_is_session_local);
Datum
test_session_locale_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	PgSessionLocaleState *locale_state;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		locale_state = PgCurrentLocaleState();
		ok = ok && locale_state->initialized;
		ok = ok && locale_state->icu_validation_level_value == WARNING;
		ok = ok && locale_state->last_collation_cache_oid == InvalidOid;
		locale_state->locale_messages_value = "locale_messages_a";
		locale_state->locale_monetary_value = "locale_monetary_a";
		locale_state->locale_numeric_value = "locale_numeric_a";
		locale_state->locale_time_value = "locale_time_a";
		locale_state->icu_validation_level_value = ERROR;
		locale_state->localized_abbrev_days_values[0] = "SunA";
		locale_state->localized_full_days_values[0] = "SundayA";
		locale_state->localized_abbrev_months_values[0] = "JanA";
		locale_state->localized_full_months_values[0] = "JanuaryA";
		locale_state->locale_conv_valid = true;
		locale_state->locale_time_valid = true;
		locale_state->current_locale_conv = &fake_session1;
		locale_state->current_locale_conv_allocated = true;
		locale_state->collation_cache_context = (MemoryContext) &fake_session1;
		locale_state->collation_cache = &fake_session1;
		locale_state->last_collation_cache_oid = 111;
		locale_state->last_collation_cache_locale = &fake_session1;

		PgSetCurrentSession(&fake_session2);
		locale_state = PgCurrentLocaleState();
		ok = ok && locale_state->initialized;
		ok = ok && locale_state->locale_messages_value == NULL;
		ok = ok && locale_state->locale_monetary_value == NULL;
		ok = ok && locale_state->locale_numeric_value == NULL;
		ok = ok && locale_state->locale_time_value == NULL;
		ok = ok && locale_state->icu_validation_level_value == WARNING;
		ok = ok && !locale_state->locale_conv_valid;
		ok = ok && !locale_state->locale_time_valid;
		ok = ok && locale_state->last_collation_cache_oid == InvalidOid;
		locale_state->locale_messages_value = "locale_messages_b";
		locale_state->locale_monetary_value = "locale_monetary_b";
		locale_state->locale_numeric_value = "locale_numeric_b";
		locale_state->locale_time_value = "locale_time_b";
		locale_state->icu_validation_level_value = WARNING;
		locale_state->localized_abbrev_days_values[0] = "SunB";
		locale_state->localized_full_days_values[0] = "SundayB";
		locale_state->localized_abbrev_months_values[0] = "JanB";
		locale_state->localized_full_months_values[0] = "JanuaryB";
		locale_state->current_locale_conv = &fake_session2;
		locale_state->collation_cache_context = (MemoryContext) &fake_session2;
		locale_state->collation_cache = &fake_session2;
		locale_state->last_collation_cache_oid = 222;
		locale_state->last_collation_cache_locale = &fake_session2;

		PgSetCurrentSession(&fake_session1);
		locale_state = PgCurrentLocaleState();
		ok = ok && strcmp(locale_messages, "locale_messages_a") == 0;
		ok = ok && strcmp(locale_monetary, "locale_monetary_a") == 0;
		ok = ok && strcmp(locale_numeric, "locale_numeric_a") == 0;
		ok = ok && strcmp(locale_time, "locale_time_a") == 0;
		ok = ok && icu_validation_level == ERROR;
		ok = ok && strcmp(localized_abbrev_days[0], "SunA") == 0;
		ok = ok && strcmp(localized_full_days[0], "SundayA") == 0;
		ok = ok && strcmp(localized_abbrev_months[0], "JanA") == 0;
		ok = ok && strcmp(localized_full_months[0], "JanuaryA") == 0;
		ok = ok && locale_state->locale_conv_valid;
		ok = ok && locale_state->locale_time_valid;
		ok = ok && locale_state->current_locale_conv == &fake_session1;
		ok = ok && locale_state->current_locale_conv_allocated;
		ok = ok && locale_state->collation_cache_context == (MemoryContext) &fake_session1;
		ok = ok && locale_state->collation_cache == &fake_session1;
		ok = ok && locale_state->last_collation_cache_oid == 111;
		ok = ok && locale_state->last_collation_cache_locale == &fake_session1;

		PgSetCurrentSession(&fake_session2);
		locale_state = PgCurrentLocaleState();
		ok = ok && strcmp(locale_messages, "locale_messages_b") == 0;
		ok = ok && strcmp(locale_monetary, "locale_monetary_b") == 0;
		ok = ok && strcmp(locale_numeric, "locale_numeric_b") == 0;
		ok = ok && strcmp(locale_time, "locale_time_b") == 0;
		ok = ok && icu_validation_level == WARNING;
		ok = ok && strcmp(localized_abbrev_days[0], "SunB") == 0;
		ok = ok && strcmp(localized_full_days[0], "SundayB") == 0;
		ok = ok && strcmp(localized_abbrev_months[0], "JanB") == 0;
		ok = ok && strcmp(localized_full_months[0], "JanuaryB") == 0;
		ok = ok && locale_state->current_locale_conv == &fake_session2;
		ok = ok && locale_state->collation_cache_context == (MemoryContext) &fake_session2;
		ok = ok && locale_state->collation_cache == &fake_session2;
		ok = ok && locale_state->last_collation_cache_oid == 222;
		ok = ok && locale_state->last_collation_cache_locale == &fake_session2;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "locale state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_prepared_statement_state_is_session_local);
Datum
test_session_prepared_statement_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	HTAB	   *saved_prepared_queries;
	HTAB	   *saved_c_func_hash;
	HTAB	   *session1_marker;
	HTAB	   *session2_marker;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_prepared_queries = *PgCurrentPreparedQueriesRef();
	saved_c_func_hash = *PgCurrentCFuncHashRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_marker = (HTAB *) &fake_session1;
	session2_marker = (HTAB *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPreparedQueriesRef() == NULL;
		ok = ok && *PgCurrentCFuncHashRef() == NULL;
		*PgCurrentPreparedQueriesRef() = session1_marker;
		*PgCurrentCFuncHashRef() = session1_marker;
		ok = ok && *PgCurrentPreparedQueriesRef() == session1_marker;
		ok = ok && *PgCurrentCFuncHashRef() == session1_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPreparedQueriesRef() == NULL;
		ok = ok && *PgCurrentCFuncHashRef() == NULL;
		*PgCurrentPreparedQueriesRef() = session2_marker;
		*PgCurrentCFuncHashRef() = session2_marker;
		ok = ok && *PgCurrentPreparedQueriesRef() == session2_marker;
		ok = ok && *PgCurrentCFuncHashRef() == session2_marker;

		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentPreparedQueriesRef() == session1_marker;
		ok = ok && *PgCurrentCFuncHashRef() == session1_marker;

		PgSetCurrentSession(&fake_session2);
		ok = ok && *PgCurrentPreparedQueriesRef() == session2_marker;
		ok = ok && *PgCurrentCFuncHashRef() == session2_marker;

		PgSetCurrentSession(saved_session);
		*PgCurrentPreparedQueriesRef() = saved_prepared_queries;
		*PgCurrentCFuncHashRef() = saved_c_func_hash;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentPreparedQueriesRef() = saved_prepared_queries;
		*PgCurrentCFuncHashRef() = saved_c_func_hash;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "prepared statement state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_invalidation_callback_state_is_session_local);
Datum
test_session_invalidation_callback_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	PgSessionInvalidationCallbackState saved_invalidation_callbacks;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_invalidation_callbacks = *PgCurrentInvalidationCallbackState();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_count == 0;
		ok = ok && PgCurrentInvalidationCallbackState()->relcache_callback_count == 0;
		ok = ok && PgCurrentInvalidationCallbackState()->relsync_callback_count == 0;
		CacheRegisterSyscacheCallback(ATTNUM,
									  test_backend_runtime_syscache_callback,
									  UInt32GetDatum(11));
		CacheRegisterRelcacheCallback(test_backend_runtime_relcache_callback,
									  UInt32GetDatum(12));
		CacheRegisterRelSyncCallback(test_backend_runtime_relsync_callback,
									 UInt32GetDatum(13));
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_links[ATTNUM] == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_list[0].function ==
			test_backend_runtime_syscache_callback;
		ok = ok && DatumGetUInt32(PgCurrentInvalidationCallbackState()->syscache_callback_list[0].arg) == 11;
		ok = ok && PgCurrentInvalidationCallbackState()->relcache_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->relcache_callback_list[0].function ==
			test_backend_runtime_relcache_callback;
		ok = ok && DatumGetUInt32(PgCurrentInvalidationCallbackState()->relcache_callback_list[0].arg) == 12;
		ok = ok && PgCurrentInvalidationCallbackState()->relsync_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->relsync_callback_list[0].function ==
			test_backend_runtime_relsync_callback;
		ok = ok && DatumGetUInt32(PgCurrentInvalidationCallbackState()->relsync_callback_list[0].arg) == 13;

		PgSetCurrentSession(&fake_session2);
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_count == 0;
		ok = ok && PgCurrentInvalidationCallbackState()->relcache_callback_count == 0;
		ok = ok && PgCurrentInvalidationCallbackState()->relsync_callback_count == 0;
		CacheRegisterSyscacheCallback(PROCOID,
									  test_backend_runtime_syscache_callback,
									  UInt32GetDatum(21));
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_links[PROCOID] == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_links[ATTNUM] == 0;

		PgSetCurrentSession(&fake_session1);
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->syscache_callback_links[ATTNUM] == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->relcache_callback_count == 1;
		ok = ok && PgCurrentInvalidationCallbackState()->relsync_callback_count == 1;

		PgSetCurrentSession(saved_session);
		*PgCurrentInvalidationCallbackState() = saved_invalidation_callbacks;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentInvalidationCallbackState() = saved_invalidation_callbacks;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "invalidation callback state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_reset_closed_state);
Datum
test_session_reset_closed_state(PG_FUNCTION_ARGS)
{
	PgSession	fake_session;
	HASHCTL		hash_ctl;
	MemoryContext oldcontext;
	MemoryContext dynamic_library_context;
	Session    *legacy_session;
	TSParserCacheEntry *parser_entry;
	TSDictionaryCacheEntry *dictionary_entry;
	TSConfigCacheEntry *config_entry;
	Oid			test_key = BOOLOID;
	bool		found;
	bool		ok = true;

	MemSet(&fake_session, 0, sizeof(fake_session));
	MemSet(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(Oid);
	hash_ctl.entrysize = sizeof(Oid);

	fake_session.database.database_path = pstrdup("base/1");
	fake_session.prepared_statement.prepared_queries =
		hash_create("test prepared statement cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	fake_session.on_commit.on_commits = list_make1(palloc(8));
	fake_session.parser.operator_lookup_cache =
		hash_create("test operator lookup cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	fake_session.function_manager.c_func_hash =
		hash_create("test C function cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	fake_session.sequence.seqhashtab =
		hash_create("test sequence cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	fake_session.sequence.last_used_seq =
		(struct SeqTableData *) &fake_session;
	fake_session.async.local_channel_table =
		hash_create("test async channel cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	fake_session.async.registered_listener = true;
	fake_session.invalidation_callbacks.syscache_callback_count = 1;
	fake_session.invalidation_callbacks.syscache_callback_links[ATTNUM] = 1;
	fake_session.invalidation_callbacks.syscache_callback_list[0].id = ATTNUM;
	fake_session.invalidation_callbacks.syscache_callback_list[0].function =
		test_backend_runtime_syscache_callback;
	fake_session.invalidation_callbacks.relcache_callback_count = 1;
	fake_session.invalidation_callbacks.relcache_callback_list[0].function =
		test_backend_runtime_relcache_callback;
	fake_session.invalidation_callbacks.relsync_callback_count = 1;
	fake_session.invalidation_callbacks.relsync_callback_list[0].function =
		test_backend_runtime_relsync_callback;
	fake_session.user_identity.cached_role[0] = BOOLOID;
	fake_session.user_identity.cached_roles[0] = list_make1_oid(BOOLOID);
	fake_session.user_identity.cached_db_hash = 12345;

	hash_ctl.entrysize = sizeof(TSParserCacheEntry);
	fake_session.text_search.parser_cache_hash =
		hash_create("test text-search parser cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	parser_entry = hash_search(fake_session.text_search.parser_cache_hash,
							   &test_key, HASH_ENTER, &found);
	parser_entry->prsId = test_key;
	parser_entry->isvalid = true;
	fake_session.text_search.last_used_parser = parser_entry;

	hash_ctl.entrysize = sizeof(TSDictionaryCacheEntry);
	fake_session.text_search.dictionary_cache_hash =
		hash_create("test text-search dictionary cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	dictionary_entry =
		hash_search(fake_session.text_search.dictionary_cache_hash,
					&test_key, HASH_ENTER, &found);
	dictionary_entry->dictId = test_key;
	dictionary_entry->isvalid = true;
	dictionary_entry->dictCtx =
		AllocSetContextCreate(TopMemoryContext,
							  "test text-search dictionary",
							  ALLOCSET_SMALL_SIZES);
	dictionary_entry->dictData =
		MemoryContextAlloc(dictionary_entry->dictCtx, 8);
	fake_session.text_search.last_used_dictionary = dictionary_entry;

	hash_ctl.entrysize = sizeof(TSConfigCacheEntry);
	fake_session.text_search.config_cache_hash =
		hash_create("test text-search config cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	config_entry = hash_search(fake_session.text_search.config_cache_hash,
							   &test_key, HASH_ENTER, &found);
	config_entry->cfgId = test_key;
	config_entry->isvalid = true;
	config_entry->lenmap = 1;
	config_entry->map = palloc0(sizeof(ListDictionary));
	config_entry->map[0].len = 1;
	config_entry->map[0].dictIds = palloc(sizeof(Oid));
	config_entry->map[0].dictIds[0] = test_key;
	fake_session.text_search.last_used_config = config_entry;
	fake_session.text_search.current_config_cache = test_key;

	hash_ctl.entrysize = sizeof(Oid);
	fake_session.optimizer.planner_extension_names =
		(const char **) palloc(sizeof(char *));
	fake_session.optimizer.planner_extension_names[0] = "test";
	fake_session.optimizer.planner_extension_names_assigned = 1;
	fake_session.optimizer.planner_extension_names_allocated = 1;
	fake_session.optimizer.opr_proof_cache_hash =
		hash_create("test operator proof cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	fake_session.locale.collation_cache_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test collation cache",
							  ALLOCSET_SMALL_SIZES);
	fake_session.locale.collation_cache = &fake_session;
	fake_session.locale.last_collation_cache_oid = BOOLOID;
	fake_session.locale.last_collation_cache_locale = &fake_session;

	dynamic_library_context =
		PgSessionGetDynamicLibraryMemoryContext(&fake_session);
	ok = ok && dynamic_library_context != NULL;
	ok = ok && fake_session.dynamic_library_context ==
		dynamic_library_context;

	oldcontext = MemoryContextSwitchTo(dynamic_library_context);
	fake_session.dynamic_library_inits =
		lappend(fake_session.dynamic_library_inits, &fake_session);
	MemoryContextSwitchTo(oldcontext);

	ok = ok && fake_session.dynamic_library_inits != NIL;
	ok = ok && GetMemoryChunkContext(fake_session.dynamic_library_inits) ==
		dynamic_library_context;

	PgSessionResetClosedState(&fake_session);

	ok = ok && fake_session.dynamic_library_context == NULL;
	ok = ok && fake_session.dynamic_library_inits == NIL;
	ok = ok && fake_session.database.database_path == NULL;
	ok = ok && fake_session.prepared_statement.prepared_queries == NULL;
	ok = ok && fake_session.on_commit.on_commits == NIL;
	ok = ok && fake_session.parser.operator_lookup_cache == NULL;
	ok = ok && fake_session.function_manager.c_func_hash == NULL;
	ok = ok && fake_session.sequence.seqhashtab == NULL;
	ok = ok && fake_session.sequence.last_used_seq == NULL;
	ok = ok && fake_session.async.local_channel_table == NULL;
	ok = ok && !fake_session.async.registered_listener;
	ok = ok && fake_session.invalidation_callbacks.syscache_callback_count == 0;
	ok = ok && fake_session.invalidation_callbacks.syscache_callback_links[ATTNUM] == 0;
	ok = ok && fake_session.invalidation_callbacks.relcache_callback_count == 0;
	ok = ok && fake_session.invalidation_callbacks.relsync_callback_count == 0;
	ok = ok && fake_session.user_identity.cached_role[0] == InvalidOid;
	ok = ok && fake_session.user_identity.cached_roles[0] == NIL;
	ok = ok && fake_session.user_identity.cached_db_hash == 0;
	ok = ok && fake_session.text_search.parser_cache_hash == NULL;
	ok = ok && fake_session.text_search.last_used_parser == NULL;
	ok = ok && fake_session.text_search.dictionary_cache_hash == NULL;
	ok = ok && fake_session.text_search.last_used_dictionary == NULL;
	ok = ok && fake_session.text_search.config_cache_hash == NULL;
	ok = ok && fake_session.text_search.last_used_config == NULL;
	ok = ok && fake_session.text_search.current_config_cache == InvalidOid;
	ok = ok && fake_session.regex.ctype_cache_list == NULL;
	ok = ok && fake_session.optimizer.planner_extension_names == NULL;
	ok = ok && fake_session.optimizer.planner_extension_names_assigned == 0;
	ok = ok && fake_session.optimizer.planner_extension_names_allocated == 0;
	ok = ok && fake_session.optimizer.opr_proof_cache_hash == NULL;
	ok = ok && fake_session.locale.collation_cache_context == NULL;
	ok = ok && fake_session.locale.collation_cache == NULL;
	ok = ok && fake_session.locale.last_collation_cache_oid == InvalidOid;
	ok = ok && fake_session.locale.last_collation_cache_locale == NULL;

	legacy_session = PgSessionGetLegacySession(&fake_session);
	ok = ok && legacy_session != NULL;
	ok = ok && fake_session.legacy_session == legacy_session;
	ok = ok && fake_session.legacy_session_context != NULL;
	ok = ok && GetMemoryChunkContext(legacy_session) ==
		fake_session.legacy_session_context;

	legacy_session->segment = (dsm_segment *) &fake_session;
	legacy_session->area = (dsa_area *) &fake_session;

	/*
	 * Also cover the legacy fallback where a list exists before the dedicated
	 * session context has been created.
	 */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	fake_session.dynamic_library_inits =
		lappend(fake_session.dynamic_library_inits, &fake_session);
	MemoryContextSwitchTo(oldcontext);

	ok = ok && fake_session.dynamic_library_context == NULL;
	ok = ok && fake_session.dynamic_library_inits != NIL;

	PgSessionResetClosedState(&fake_session);

	ok = ok && fake_session.dynamic_library_context == NULL;
	ok = ok && fake_session.dynamic_library_inits == NIL;
	ok = ok && fake_session.legacy_session_context == NULL;
	ok = ok && fake_session.legacy_session == NULL;

	if (!ok)
		elog(ERROR, "closed session runtime state was not reset");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_runtime_server_guc_state_is_runtime_local);
Datum
test_runtime_server_guc_state_is_runtime_local(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgRuntime	fake_runtime1;
	PgRuntime	fake_runtime2;
	char	   *saved_cluster_name;
	char	   *saved_config_file_name;
	char	   *saved_hba_file_name;
	char	   *saved_ident_file_name;
	char	   *saved_hosts_file_name;
	char	   *saved_external_pid_file;
	const char *stage = "initial";
	bool		ok = true;

	saved_runtime = CurrentPgRuntime;
	saved_cluster_name = cluster_name ? pstrdup(cluster_name) : NULL;
	saved_config_file_name = ConfigFileName ? pstrdup(ConfigFileName) : NULL;
	saved_hba_file_name = HbaFileName ? pstrdup(HbaFileName) : NULL;
	saved_ident_file_name = IdentFileName ? pstrdup(IdentFileName) : NULL;
	saved_hosts_file_name = HostsFileName ? pstrdup(HostsFileName) : NULL;
	saved_external_pid_file =
		external_pid_file ? pstrdup(external_pid_file) : NULL;
	MemSet(&fake_runtime1, 0, sizeof(fake_runtime1));
	MemSet(&fake_runtime2, 0, sizeof(fake_runtime2));

	PG_TRY();
	{
		stage = "runtime1 default";
		CurrentPgRuntime = &fake_runtime1;
		RebindSessionGUCVariablePointers();
		ok = ok && strcmp(cluster_name, "") == 0;
		ok = ok && ConfigFileName == NULL;
		ok = ok && HbaFileName == NULL;
		ok = ok && IdentFileName == NULL;
		ok = ok && HostsFileName == NULL;
		ok = ok && external_pid_file == NULL;
		if (!ok)
			elog(ERROR,
				 "runtime server GUC state was not runtime-local at %s",
				 stage);

		stage = "runtime1 set";
		cluster_name = "phase12_runtime_one";
		ConfigFileName = "/tmp/phase12_runtime_one.conf";
		HbaFileName = "/tmp/phase12_runtime_one_hba.conf";
		IdentFileName = "/tmp/phase12_runtime_one_ident.conf";
		HostsFileName = "/tmp/phase12_runtime_one_hosts.conf";
		external_pid_file = "/tmp/phase12_runtime_one.pid";
		ok = ok && strcmp(cluster_name, "phase12_runtime_one") == 0;
		ok = ok && strcmp(ConfigFileName,
						  "/tmp/phase12_runtime_one.conf") == 0;
		ok = ok && strcmp(HbaFileName,
						  "/tmp/phase12_runtime_one_hba.conf") == 0;
		ok = ok && strcmp(IdentFileName,
						  "/tmp/phase12_runtime_one_ident.conf") == 0;
		ok = ok && strcmp(HostsFileName,
						  "/tmp/phase12_runtime_one_hosts.conf") == 0;
		ok = ok && strcmp(external_pid_file,
						  "/tmp/phase12_runtime_one.pid") == 0;
		ok = ok && strcmp(GetConfigOption("cluster_name", false, false),
						  "phase12_runtime_one") == 0;
		ok = ok && strcmp(GetConfigOption("config_file", false, false),
						  "/tmp/phase12_runtime_one.conf") == 0;
		if (!ok)
			elog(ERROR,
				 "runtime server GUC state was not runtime-local at %s",
				 stage);

		stage = "runtime2 default";
		CurrentPgRuntime = &fake_runtime2;
		RebindSessionGUCVariablePointers();
		ok = ok && strcmp(cluster_name, "") == 0;
		ok = ok && ConfigFileName == NULL;
		ok = ok && HbaFileName == NULL;
		ok = ok && IdentFileName == NULL;
		ok = ok && HostsFileName == NULL;
		ok = ok && external_pid_file == NULL;
		if (!ok)
			elog(ERROR,
				 "runtime server GUC state was not runtime-local at %s",
				 stage);
		stage = "runtime2 set";
		cluster_name = "phase12_runtime_two";
		ConfigFileName = "/tmp/phase12_runtime_two.conf";
		HbaFileName = "/tmp/phase12_runtime_two_hba.conf";
		IdentFileName = "/tmp/phase12_runtime_two_ident.conf";
		HostsFileName = "/tmp/phase12_runtime_two_hosts.conf";
		external_pid_file = "/tmp/phase12_runtime_two.pid";
		ok = ok && strcmp(cluster_name, "phase12_runtime_two") == 0;
		ok = ok && strcmp(ConfigFileName,
						  "/tmp/phase12_runtime_two.conf") == 0;
		ok = ok && strcmp(HbaFileName,
						  "/tmp/phase12_runtime_two_hba.conf") == 0;
		ok = ok && strcmp(IdentFileName,
						  "/tmp/phase12_runtime_two_ident.conf") == 0;
		ok = ok && strcmp(HostsFileName,
						  "/tmp/phase12_runtime_two_hosts.conf") == 0;
		ok = ok && strcmp(external_pid_file,
						  "/tmp/phase12_runtime_two.pid") == 0;
		ok = ok && strcmp(GetConfigOption("cluster_name", false, false),
						  "phase12_runtime_two") == 0;
		ok = ok && strcmp(GetConfigOption("config_file", false, false),
						  "/tmp/phase12_runtime_two.conf") == 0;
		if (!ok)
			elog(ERROR,
				 "runtime server GUC state was not runtime-local at %s",
				 stage);

		stage = "runtime1 restore";
		CurrentPgRuntime = &fake_runtime1;
		RebindSessionGUCVariablePointers();
		ok = ok && strcmp(cluster_name, "phase12_runtime_one") == 0;
		ok = ok && strcmp(ConfigFileName,
						  "/tmp/phase12_runtime_one.conf") == 0;
		ok = ok && strcmp(HbaFileName,
						  "/tmp/phase12_runtime_one_hba.conf") == 0;
		ok = ok && strcmp(IdentFileName,
						  "/tmp/phase12_runtime_one_ident.conf") == 0;
		ok = ok && strcmp(HostsFileName,
						  "/tmp/phase12_runtime_one_hosts.conf") == 0;
		ok = ok && strcmp(external_pid_file,
						  "/tmp/phase12_runtime_one.pid") == 0;
		ok = ok && strcmp(GetConfigOption("cluster_name", false, false),
						  "phase12_runtime_one") == 0;
		ok = ok && strcmp(GetConfigOption("config_file", false, false),
						  "/tmp/phase12_runtime_one.conf") == 0;
		if (!ok)
			elog(ERROR,
				 "runtime server GUC state was not runtime-local at %s: cluster=%s config=%s hba=%s ident=%s hosts=%s pid=%s",
				 stage,
				 cluster_name ? cluster_name : "<null>",
				 ConfigFileName ? ConfigFileName : "<null>",
				 HbaFileName ? HbaFileName : "<null>",
				 IdentFileName ? IdentFileName : "<null>",
				 HostsFileName ? HostsFileName : "<null>",
				 external_pid_file ? external_pid_file : "<null>");

		stage = "saved runtime restore";
		CurrentPgRuntime = saved_runtime;
		RebindSessionGUCVariablePointers();
		cluster_name = saved_cluster_name;
		ConfigFileName = saved_config_file_name;
		HbaFileName = saved_hba_file_name;
		IdentFileName = saved_ident_file_name;
		HostsFileName = saved_hosts_file_name;
		external_pid_file = saved_external_pid_file;
	}
	PG_CATCH();
	{
		CurrentPgRuntime = saved_runtime;
		RebindSessionGUCVariablePointers();
		cluster_name = saved_cluster_name;
		ConfigFileName = saved_config_file_name;
		HbaFileName = saved_hba_file_name;
		IdentFileName = saved_ident_file_name;
		HostsFileName = saved_hosts_file_name;
		external_pid_file = saved_external_pid_file;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "runtime server GUC state was not runtime-local at %s",
			 stage);

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_connection_guc_state_is_session_local);
Datum
test_session_connection_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_application_name;
	char	   *saved_log_disconnections;
	char	   *saved_log_statement;
	char	   *saved_post_auth_delay;
	char	   *saved_restrict_relation_kind;
	char	   *saved_tcp_keepalives_idle;
	char	   *saved_tcp_keepalives_interval;
	char	   *saved_tcp_keepalives_count;
	char	   *saved_tcp_user_timeout;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_application_name =
		pstrdup(GetConfigOption("application_name", false, false));
	saved_log_disconnections =
		pstrdup(GetConfigOption("log_disconnections", false, false));
	saved_log_statement =
		pstrdup(GetConfigOption("log_statement", false, false));
	saved_post_auth_delay =
		pstrdup(GetConfigOption("post_auth_delay", false, false));
	saved_restrict_relation_kind =
		pstrdup(GetConfigOption("restrict_nonsystem_relation_kind",
								false, false));
	saved_tcp_keepalives_idle =
		pstrdup(GetConfigOption("tcp_keepalives_idle", false, false));
	saved_tcp_keepalives_interval =
		pstrdup(GetConfigOption("tcp_keepalives_interval", false, false));
	saved_tcp_keepalives_count =
		pstrdup(GetConfigOption("tcp_keepalives_count", false, false));
	saved_tcp_user_timeout =
		pstrdup(GetConfigOption("tcp_user_timeout", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(application_name, "") == 0;
		ok = ok && tcp_keepalives_idle == 0;
		ok = ok && tcp_keepalives_interval == 0;
		ok = ok && tcp_keepalives_count == 0;
		ok = ok && tcp_user_timeout == 0;
		ok = ok && !Log_disconnections;
		ok = ok && log_statement == LOGSTMT_NONE;
		ok = ok && PostAuthDelay == 0;
		ok = ok && strcmp(*PgCurrentRestrictNonsystemRelationKindStringRef(),
						  "") == 0;
		ok = ok && restrict_nonsystem_relation_kind == 0;

		SetConfigOption("application_name", "phase12_conn_one",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_idle", "11",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_interval", "12",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_count", "13",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_user_timeout", "14",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_disconnections", "on",
						PGC_SU_BACKEND, PGC_S_CLIENT);
		SetConfigOption("log_statement", "ddl",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("post_auth_delay", "15",
						PGC_BACKEND, PGC_S_CLIENT);
		SetConfigOption("restrict_nonsystem_relation_kind", "view",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && strcmp(application_name, "phase12_conn_one") == 0;
		ok = ok && tcp_keepalives_idle == 11;
		ok = ok && tcp_keepalives_interval == 12;
		ok = ok && tcp_keepalives_count == 13;
		ok = ok && tcp_user_timeout == 14;
		ok = ok && Log_disconnections;
		ok = ok && log_statement == LOGSTMT_DDL;
		ok = ok && PostAuthDelay == 15;
		ok = ok && restrict_nonsystem_relation_kind == RESTRICT_RELKIND_VIEW;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(application_name, "") == 0;
		ok = ok && tcp_keepalives_idle == 0;
		ok = ok && tcp_keepalives_interval == 0;
		ok = ok && tcp_keepalives_count == 0;
		ok = ok && tcp_user_timeout == 0;
		ok = ok && !Log_disconnections;
		ok = ok && log_statement == LOGSTMT_NONE;
		ok = ok && PostAuthDelay == 0;
		ok = ok && restrict_nonsystem_relation_kind == 0;
		SetConfigOption("application_name", "phase12_conn_two",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_idle", "21",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_interval", "22",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_count", "23",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_user_timeout", "24",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_disconnections", "off",
						PGC_SU_BACKEND, PGC_S_CLIENT);
		SetConfigOption("log_statement", "all",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("post_auth_delay", "25",
						PGC_BACKEND, PGC_S_CLIENT);
		SetConfigOption("restrict_nonsystem_relation_kind",
						"view, foreign-table",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && strcmp(application_name, "phase12_conn_two") == 0;
		ok = ok && tcp_keepalives_idle == 21;
		ok = ok && tcp_keepalives_interval == 22;
		ok = ok && tcp_keepalives_count == 23;
		ok = ok && tcp_user_timeout == 24;
		ok = ok && !Log_disconnections;
		ok = ok && log_statement == LOGSTMT_ALL;
		ok = ok && PostAuthDelay == 25;
		ok = ok && restrict_nonsystem_relation_kind ==
			(RESTRICT_RELKIND_VIEW | RESTRICT_RELKIND_FOREIGN_TABLE);

		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(application_name, "phase12_conn_one") == 0;
		ok = ok && tcp_keepalives_idle == 11;
		ok = ok && tcp_keepalives_interval == 12;
		ok = ok && tcp_keepalives_count == 13;
		ok = ok && tcp_user_timeout == 14;
		ok = ok && Log_disconnections;
		ok = ok && log_statement == LOGSTMT_DDL;
		ok = ok && PostAuthDelay == 15;
		ok = ok && restrict_nonsystem_relation_kind == RESTRICT_RELKIND_VIEW;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(application_name, "phase12_conn_two") == 0;
		ok = ok && tcp_keepalives_idle == 21;
		ok = ok && tcp_keepalives_interval == 22;
		ok = ok && tcp_keepalives_count == 23;
		ok = ok && tcp_user_timeout == 24;
		ok = ok && !Log_disconnections;
		ok = ok && log_statement == LOGSTMT_ALL;
		ok = ok && PostAuthDelay == 25;
		ok = ok && restrict_nonsystem_relation_kind ==
			(RESTRICT_RELKIND_VIEW | RESTRICT_RELKIND_FOREIGN_TABLE);

		PgSetCurrentSession(saved_session);
		SetConfigOption("application_name", saved_application_name,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_disconnections", saved_log_disconnections,
						PGC_SU_BACKEND, PGC_S_CLIENT);
		SetConfigOption("log_statement", saved_log_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("post_auth_delay", saved_post_auth_delay,
						PGC_BACKEND, PGC_S_CLIENT);
		SetConfigOption("restrict_nonsystem_relation_kind",
						saved_restrict_relation_kind,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_idle", saved_tcp_keepalives_idle,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_interval",
						saved_tcp_keepalives_interval,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_count", saved_tcp_keepalives_count,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_user_timeout", saved_tcp_user_timeout,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("application_name", saved_application_name,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_disconnections", saved_log_disconnections,
						PGC_SU_BACKEND, PGC_S_CLIENT);
		SetConfigOption("log_statement", saved_log_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("post_auth_delay", saved_post_auth_delay,
						PGC_BACKEND, PGC_S_CLIENT);
		SetConfigOption("restrict_nonsystem_relation_kind",
						saved_restrict_relation_kind,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_idle", saved_tcp_keepalives_idle,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_interval",
						saved_tcp_keepalives_interval,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_keepalives_count", saved_tcp_keepalives_count,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("tcp_user_timeout", saved_tcp_user_timeout,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session connection GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_parser_state_is_session_local);
Datum
test_session_parser_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_backslash_quote;
	char	   *saved_transform_null_equals;
	HTAB	   *saved_operator_lookup_cache;
	HTAB	   *session1_operator_cache;
	HTAB	   *session2_operator_cache;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_backslash_quote =
		pstrdup(GetConfigOption("backslash_quote", false, false));
	saved_transform_null_equals =
		pstrdup(GetConfigOption("transform_null_equals", false, false));
	saved_operator_lookup_cache = *PgCurrentOperatorLookupCacheRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);
	session1_operator_cache = (HTAB *) &fake_session1;
	session2_operator_cache = (HTAB *) &fake_session2;

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && backslash_quote == BACKSLASH_QUOTE_SAFE_ENCODING;
		ok = ok && !Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() == NULL;
		SetConfigOption("backslash_quote", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transform_null_equals", "on",
						PGC_USERSET, PGC_S_SESSION);
		*PgCurrentOperatorLookupCacheRef() = session1_operator_cache;
		ok = ok && backslash_quote == BACKSLASH_QUOTE_ON;
		ok = ok && Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() ==
			session1_operator_cache;

		PgSetCurrentSession(&fake_session2);
		ok = ok && backslash_quote == BACKSLASH_QUOTE_SAFE_ENCODING;
		ok = ok && !Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() == NULL;
		SetConfigOption("backslash_quote", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transform_null_equals", "off",
						PGC_USERSET, PGC_S_SESSION);
		*PgCurrentOperatorLookupCacheRef() = session2_operator_cache;
		ok = ok && backslash_quote == BACKSLASH_QUOTE_OFF;
		ok = ok && !Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() ==
			session2_operator_cache;

		PgSetCurrentSession(&fake_session1);
		ok = ok && backslash_quote == BACKSLASH_QUOTE_ON;
		ok = ok && Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() ==
			session1_operator_cache;

		PgSetCurrentSession(&fake_session2);
		ok = ok && backslash_quote == BACKSLASH_QUOTE_OFF;
		ok = ok && !Transform_null_equals;
		ok = ok && *PgCurrentOperatorLookupCacheRef() ==
			session2_operator_cache;

		PgSetCurrentSession(saved_session);
		*PgCurrentOperatorLookupCacheRef() = saved_operator_lookup_cache;
		SetConfigOption("backslash_quote", saved_backslash_quote,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transform_null_equals",
						saved_transform_null_equals,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		*PgCurrentOperatorLookupCacheRef() = saved_operator_lookup_cache;
		SetConfigOption("backslash_quote", saved_backslash_quote,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transform_null_equals",
						saved_transform_null_equals,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session parser state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_vacuum_state_is_session_local);
Datum
test_session_vacuum_state_is_session_local(PG_FUNCTION_ARGS)
{
	enum
	{
		TEST_VACUUM_GUC_COUNT = 16
	};
	const char *guc_names[TEST_VACUUM_GUC_COUNT] = {
		"default_statistics_target",
		"track_cost_delay_timing",
		"vacuum_buffer_usage_limit",
		"vacuum_cost_delay",
		"vacuum_cost_limit",
		"vacuum_cost_page_dirty",
		"vacuum_cost_page_hit",
		"vacuum_cost_page_miss",
		"vacuum_failsafe_age",
		"vacuum_freeze_min_age",
		"vacuum_freeze_table_age",
		"vacuum_max_eager_freeze_failure_rate",
		"vacuum_multixact_failsafe_age",
		"vacuum_multixact_freeze_min_age",
		"vacuum_multixact_freeze_table_age",
		"vacuum_truncate"
	};
	const char *session1_values[TEST_VACUUM_GUC_COUNT] = {
		"101",
		"on",
		"4096",
		"2",
		"301",
		"31",
		"3",
		"5",
		"1700000000",
		"60000000",
		"160000000",
		"0.04",
		"1700000000",
		"6000000",
		"160000000",
		"off"
	};
	const char *session2_values[TEST_VACUUM_GUC_COUNT] = {
		"102",
		"off",
		"8192",
		"3",
		"302",
		"32",
		"4",
		"6",
		"1800000000",
		"70000000",
		"170000000",
		"0.05",
		"1800000000",
		"7000000",
		"170000000",
		"on"
	};
	char	   *saved_values[TEST_VACUUM_GUC_COUNT];
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;
	int			i;

	saved_session = CurrentPgSession;
	for (i = 0; i < TEST_VACUUM_GUC_COUNT; i++)
		saved_values[i] = pstrdup(GetConfigOption(guc_names[i], false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && default_statistics_target == 100;
		ok = ok && !track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 2048;
		ok = ok && VacuumCostDelay == 0;
		ok = ok && VacuumCostLimit == 200;
		ok = ok && VacuumCostPageDirty == 20;
		ok = ok && VacuumCostPageHit == 1;
		ok = ok && VacuumCostPageMiss == 2;
		ok = ok && vacuum_failsafe_age == 1600000000;
		ok = ok && vacuum_freeze_min_age == 50000000;
		ok = ok && vacuum_freeze_table_age == 150000000;
		ok = ok && vacuum_max_eager_freeze_failure_rate > 0.029;
		ok = ok && vacuum_max_eager_freeze_failure_rate < 0.031;
		ok = ok && vacuum_multixact_failsafe_age == 1600000000;
		ok = ok && vacuum_multixact_freeze_min_age == 5000000;
		ok = ok && vacuum_multixact_freeze_table_age == 150000000;
		ok = ok && vacuum_truncate;
		ok = ok && vacuum_cost_delay == 0;
		ok = ok && vacuum_cost_limit == 200;
		for (i = 0; i < TEST_VACUUM_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], session1_values[i],
							PGC_USERSET, PGC_S_SESSION);
		vacuum_cost_delay = 7.0;
		vacuum_cost_limit = 701;
		ok = ok && default_statistics_target == 101;
		ok = ok && track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 4096;
		ok = ok && VacuumCostDelay == 2.0;
		ok = ok && VacuumCostLimit == 301;
		ok = ok && VacuumCostPageDirty == 31;
		ok = ok && VacuumCostPageHit == 3;
		ok = ok && VacuumCostPageMiss == 5;
		ok = ok && vacuum_failsafe_age == 1700000000;
		ok = ok && vacuum_freeze_min_age == 60000000;
		ok = ok && vacuum_freeze_table_age == 160000000;
		ok = ok && vacuum_max_eager_freeze_failure_rate > 0.039;
		ok = ok && vacuum_max_eager_freeze_failure_rate < 0.041;
		ok = ok && vacuum_multixact_failsafe_age == 1700000000;
		ok = ok && vacuum_multixact_freeze_min_age == 6000000;
		ok = ok && vacuum_multixact_freeze_table_age == 160000000;
		ok = ok && !vacuum_truncate;
		ok = ok && vacuum_cost_delay == 7.0;
		ok = ok && vacuum_cost_limit == 701;

		PgSetCurrentSession(&fake_session2);
		ok = ok && default_statistics_target == 100;
		ok = ok && !track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 2048;
		ok = ok && VacuumCostDelay == 0;
		ok = ok && VacuumCostLimit == 200;
		ok = ok && VacuumCostPageDirty == 20;
		ok = ok && VacuumCostPageHit == 1;
		ok = ok && VacuumCostPageMiss == 2;
		ok = ok && vacuum_truncate;
		ok = ok && vacuum_cost_delay == 0;
		ok = ok && vacuum_cost_limit == 200;
		for (i = 0; i < TEST_VACUUM_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], session2_values[i],
							PGC_USERSET, PGC_S_SESSION);
		vacuum_cost_delay = 9.0;
		vacuum_cost_limit = 901;
		ok = ok && default_statistics_target == 102;
		ok = ok && !track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 8192;
		ok = ok && VacuumCostDelay == 3.0;
		ok = ok && VacuumCostLimit == 302;
		ok = ok && VacuumCostPageDirty == 32;
		ok = ok && VacuumCostPageHit == 4;
		ok = ok && VacuumCostPageMiss == 6;
		ok = ok && vacuum_failsafe_age == 1800000000;
		ok = ok && vacuum_freeze_min_age == 70000000;
		ok = ok && vacuum_freeze_table_age == 170000000;
		ok = ok && vacuum_max_eager_freeze_failure_rate > 0.049;
		ok = ok && vacuum_max_eager_freeze_failure_rate < 0.051;
		ok = ok && vacuum_multixact_failsafe_age == 1800000000;
		ok = ok && vacuum_multixact_freeze_min_age == 7000000;
		ok = ok && vacuum_multixact_freeze_table_age == 170000000;
		ok = ok && vacuum_truncate;
		ok = ok && vacuum_cost_delay == 9.0;
		ok = ok && vacuum_cost_limit == 901;

		PgSetCurrentSession(&fake_session1);
		ok = ok && default_statistics_target == 101;
		ok = ok && track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 4096;
		ok = ok && VacuumCostDelay == 2.0;
		ok = ok && VacuumCostLimit == 301;
		ok = ok && VacuumCostPageDirty == 31;
		ok = ok && VacuumCostPageHit == 3;
		ok = ok && VacuumCostPageMiss == 5;
		ok = ok && vacuum_failsafe_age == 1700000000;
		ok = ok && !vacuum_truncate;
		ok = ok && vacuum_cost_delay == 7.0;
		ok = ok && vacuum_cost_limit == 701;

		PgSetCurrentSession(&fake_session2);
		ok = ok && default_statistics_target == 102;
		ok = ok && !track_cost_delay_timing;
		ok = ok && VacuumBufferUsageLimit == 8192;
		ok = ok && VacuumCostDelay == 3.0;
		ok = ok && VacuumCostLimit == 302;
		ok = ok && VacuumCostPageDirty == 32;
		ok = ok && VacuumCostPageHit == 4;
		ok = ok && VacuumCostPageMiss == 6;
		ok = ok && vacuum_failsafe_age == 1800000000;
		ok = ok && vacuum_truncate;
		ok = ok && vacuum_cost_delay == 9.0;
		ok = ok && vacuum_cost_limit == 901;

		PgSetCurrentSession(saved_session);
		for (i = 0; i < TEST_VACUUM_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], saved_values[i],
							PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		for (i = 0; i < TEST_VACUUM_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], saved_values[i],
							PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session vacuum GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_buffer_io_state_is_session_local);
Datum
test_session_buffer_io_state_is_session_local(PG_FUNCTION_ARGS)
{
	enum
	{
		TEST_BUFFER_IO_GUC_COUNT = 6
	};
	const char *guc_names[TEST_BUFFER_IO_GUC_COUNT] = {
		"backend_flush_after",
		"effective_io_concurrency",
		"io_combine_limit",
		"maintenance_io_concurrency",
		"track_io_timing",
		"zero_damaged_pages"
	};
	const char *session1_values[TEST_BUFFER_IO_GUC_COUNT] = {
		"8",
		"32",
		"8",
		"24",
		"on",
		"on"
	};
	const char *session2_values[TEST_BUFFER_IO_GUC_COUNT] = {
		"4",
		"16",
		"4",
		"12",
		"off",
		"off"
	};
	char	   *saved_values[TEST_BUFFER_IO_GUC_COUNT];
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;
	int			i;

	saved_session = CurrentPgSession;
	for (i = 0; i < TEST_BUFFER_IO_GUC_COUNT; i++)
		saved_values[i] = pstrdup(GetConfigOption(guc_names[i], false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && backend_flush_after == DEFAULT_BACKEND_FLUSH_AFTER;
		ok = ok && effective_io_concurrency == DEFAULT_EFFECTIVE_IO_CONCURRENCY;
		ok = ok && io_combine_limit == DEFAULT_IO_COMBINE_LIMIT;
		ok = ok && io_combine_limit_guc == DEFAULT_IO_COMBINE_LIMIT;
		ok = ok && maintenance_io_concurrency == DEFAULT_MAINTENANCE_IO_CONCURRENCY;
		ok = ok && !track_io_timing;
		ok = ok && !zero_damaged_pages;
		for (i = 0; i < TEST_BUFFER_IO_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], session1_values[i],
							PGC_USERSET, PGC_S_SESSION);
		ok = ok && backend_flush_after == 8;
		ok = ok && effective_io_concurrency == 32;
		ok = ok && io_combine_limit == 8;
		ok = ok && io_combine_limit_guc == 8;
		ok = ok && maintenance_io_concurrency == 24;
		ok = ok && track_io_timing;
		ok = ok && zero_damaged_pages;

		PgSetCurrentSession(&fake_session2);
		ok = ok && backend_flush_after == DEFAULT_BACKEND_FLUSH_AFTER;
		ok = ok && effective_io_concurrency == DEFAULT_EFFECTIVE_IO_CONCURRENCY;
		ok = ok && io_combine_limit == DEFAULT_IO_COMBINE_LIMIT;
		ok = ok && io_combine_limit_guc == DEFAULT_IO_COMBINE_LIMIT;
		ok = ok && maintenance_io_concurrency == DEFAULT_MAINTENANCE_IO_CONCURRENCY;
		ok = ok && !track_io_timing;
		ok = ok && !zero_damaged_pages;
		for (i = 0; i < TEST_BUFFER_IO_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], session2_values[i],
							PGC_USERSET, PGC_S_SESSION);
		ok = ok && backend_flush_after == 4;
		ok = ok && effective_io_concurrency == 16;
		ok = ok && io_combine_limit == 4;
		ok = ok && io_combine_limit_guc == 4;
		ok = ok && maintenance_io_concurrency == 12;
		ok = ok && !track_io_timing;
		ok = ok && !zero_damaged_pages;

		PgSetCurrentSession(&fake_session1);
		ok = ok && backend_flush_after == 8;
		ok = ok && effective_io_concurrency == 32;
		ok = ok && io_combine_limit == 8;
		ok = ok && io_combine_limit_guc == 8;
		ok = ok && maintenance_io_concurrency == 24;
		ok = ok && track_io_timing;
		ok = ok && zero_damaged_pages;

		PgSetCurrentSession(&fake_session2);
		ok = ok && backend_flush_after == 4;
		ok = ok && effective_io_concurrency == 16;
		ok = ok && io_combine_limit == 4;
		ok = ok && io_combine_limit_guc == 4;
		ok = ok && maintenance_io_concurrency == 12;
		ok = ok && !track_io_timing;
		ok = ok && !zero_damaged_pages;

		PgSetCurrentSession(saved_session);
		for (i = 0; i < TEST_BUFFER_IO_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], saved_values[i],
							PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		for (i = 0; i < TEST_BUFFER_IO_GUC_COUNT; i++)
			SetConfigOption(guc_names[i], saved_values[i],
							PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session buffer I/O GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_xact_defaults_are_session_local);
Datum
test_session_xact_defaults_are_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_default_xact_deferrable;
	char	   *saved_default_xact_isolation;
	char	   *saved_default_xact_read_only;
	char	   *saved_synchronous_commit;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_default_xact_deferrable =
		pstrdup(GetConfigOption("default_transaction_deferrable", false,
								false));
	saved_default_xact_isolation =
		pstrdup(GetConfigOption("default_transaction_isolation", false,
								false));
	saved_default_xact_read_only =
		pstrdup(GetConfigOption("default_transaction_read_only", false,
								false));
	saved_synchronous_commit =
		pstrdup(GetConfigOption("synchronous_commit", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && DefaultXactIsoLevel == XACT_READ_COMMITTED;
		ok = ok && !DefaultXactReadOnly;
		ok = ok && !DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_ON;
		SetConfigOption("default_transaction_isolation", "serializable",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_read_only", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_deferrable", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("synchronous_commit", "remote_apply",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && DefaultXactIsoLevel == XACT_SERIALIZABLE;
		ok = ok && DefaultXactReadOnly;
		ok = ok && DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_REMOTE_APPLY;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DefaultXactIsoLevel == XACT_READ_COMMITTED;
		ok = ok && !DefaultXactReadOnly;
		ok = ok && !DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_ON;
		SetConfigOption("default_transaction_isolation", "repeatable read",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_read_only", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_deferrable", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("synchronous_commit", "local",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && DefaultXactIsoLevel == XACT_REPEATABLE_READ;
		ok = ok && !DefaultXactReadOnly;
		ok = ok && !DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_LOCAL_FLUSH;

		PgSetCurrentSession(&fake_session1);
		ok = ok && DefaultXactIsoLevel == XACT_SERIALIZABLE;
		ok = ok && DefaultXactReadOnly;
		ok = ok && DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_REMOTE_APPLY;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DefaultXactIsoLevel == XACT_REPEATABLE_READ;
		ok = ok && !DefaultXactReadOnly;
		ok = ok && !DefaultXactDeferrable;
		ok = ok && synchronous_commit == SYNCHRONOUS_COMMIT_LOCAL_FLUSH;

		PgSetCurrentSession(saved_session);
		SetConfigOption("default_transaction_deferrable",
						saved_default_xact_deferrable,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_isolation",
						saved_default_xact_isolation,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_read_only",
						saved_default_xact_read_only,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("synchronous_commit", saved_synchronous_commit,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("default_transaction_deferrable",
						saved_default_xact_deferrable,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_isolation",
						saved_default_xact_isolation,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("default_transaction_read_only",
						saved_default_xact_read_only,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("synchronous_commit", saved_synchronous_commit,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session transaction default GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_lock_wait_state_is_session_local);
Datum
test_session_lock_wait_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_deadlock_timeout;
	char	   *saved_statement_timeout;
	char	   *saved_lock_timeout;
	char	   *saved_idle_in_transaction_session_timeout;
	char	   *saved_transaction_timeout;
	char	   *saved_idle_session_timeout;
	char	   *saved_log_lock_waits;
	char	   *saved_log_lock_failures;
#ifdef LOCK_DEBUG
	char	   *saved_debug_deadlocks;
	char	   *saved_trace_lock_oidmin;
	char	   *saved_trace_lock_table;
	char	   *saved_trace_locks;
	char	   *saved_trace_lwlocks;
	char	   *saved_trace_userlocks;
#endif
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_deadlock_timeout =
		pstrdup(GetConfigOption("deadlock_timeout", false, false));
	saved_statement_timeout =
		pstrdup(GetConfigOption("statement_timeout", false, false));
	saved_lock_timeout =
		pstrdup(GetConfigOption("lock_timeout", false, false));
	saved_idle_in_transaction_session_timeout =
		pstrdup(GetConfigOption("idle_in_transaction_session_timeout",
								false, false));
	saved_transaction_timeout =
		pstrdup(GetConfigOption("transaction_timeout", false, false));
	saved_idle_session_timeout =
		pstrdup(GetConfigOption("idle_session_timeout", false, false));
	saved_log_lock_waits =
		pstrdup(GetConfigOption("log_lock_waits", false, false));
	saved_log_lock_failures =
		pstrdup(GetConfigOption("log_lock_failures", false, false));
#ifdef LOCK_DEBUG
	saved_debug_deadlocks =
		pstrdup(GetConfigOption("debug_deadlocks", false, false));
	saved_trace_lock_oidmin =
		pstrdup(GetConfigOption("trace_lock_oidmin", false, false));
	saved_trace_lock_table =
		pstrdup(GetConfigOption("trace_lock_table", false, false));
	saved_trace_locks =
		pstrdup(GetConfigOption("trace_locks", false, false));
	saved_trace_lwlocks =
		pstrdup(GetConfigOption("trace_lwlocks", false, false));
	saved_trace_userlocks =
		pstrdup(GetConfigOption("trace_userlocks", false, false));
#endif
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && DeadlockTimeout == 1000;
		ok = ok && StatementTimeout == 0;
		ok = ok && LockTimeout == 0;
		ok = ok && IdleInTransactionSessionTimeout == 0;
		ok = ok && TransactionTimeout == 0;
		ok = ok && IdleSessionTimeout == 0;
		ok = ok && log_lock_waits;
		ok = ok && !log_lock_failures;
		SetConfigOption("deadlock_timeout", "2000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("statement_timeout", "3000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lock_timeout", "4000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_in_transaction_session_timeout", "5000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transaction_timeout", "6000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_session_timeout", "7000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_lock_waits", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_lock_failures", "on",
						PGC_SUSET, PGC_S_SESSION);
#ifdef LOCK_DEBUG
		SetConfigOption("debug_deadlocks", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_oidmin", "20000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_table", "30000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_locks", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lwlocks", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_userlocks", "on",
						PGC_SUSET, PGC_S_SESSION);
#endif
		ok = ok && DeadlockTimeout == 2000;
		ok = ok && StatementTimeout == 3000;
		ok = ok && LockTimeout == 4000;
		ok = ok && IdleInTransactionSessionTimeout == 5000;
		ok = ok && TransactionTimeout == 6000;
		ok = ok && IdleSessionTimeout == 7000;
		ok = ok && !log_lock_waits;
		ok = ok && log_lock_failures;
#ifdef LOCK_DEBUG
		ok = ok && Debug_deadlocks;
		ok = ok && Trace_lock_oidmin == 20000;
		ok = ok && Trace_lock_table == 30000;
		ok = ok && Trace_locks;
		ok = ok && Trace_lwlocks;
		ok = ok && Trace_userlocks;
#endif

		PgSetCurrentSession(&fake_session2);
		ok = ok && DeadlockTimeout == 1000;
		ok = ok && StatementTimeout == 0;
		ok = ok && LockTimeout == 0;
		ok = ok && IdleInTransactionSessionTimeout == 0;
		ok = ok && TransactionTimeout == 0;
		ok = ok && IdleSessionTimeout == 0;
		ok = ok && log_lock_waits;
		ok = ok && !log_lock_failures;
		SetConfigOption("deadlock_timeout", "1100",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("statement_timeout", "1200",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lock_timeout", "1300",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_in_transaction_session_timeout", "1400",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transaction_timeout", "1500",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_session_timeout", "1600",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_lock_waits", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_lock_failures", "off",
						PGC_SUSET, PGC_S_SESSION);
#ifdef LOCK_DEBUG
		SetConfigOption("debug_deadlocks", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_oidmin", "10000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_table", "0",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_locks", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lwlocks", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_userlocks", "off",
						PGC_SUSET, PGC_S_SESSION);
#endif
		ok = ok && DeadlockTimeout == 1100;
		ok = ok && StatementTimeout == 1200;
		ok = ok && LockTimeout == 1300;
		ok = ok && IdleInTransactionSessionTimeout == 1400;
		ok = ok && TransactionTimeout == 1500;
		ok = ok && IdleSessionTimeout == 1600;
		ok = ok && log_lock_waits;
		ok = ok && !log_lock_failures;
#ifdef LOCK_DEBUG
		ok = ok && !Debug_deadlocks;
		ok = ok && Trace_lock_oidmin == 10000;
		ok = ok && Trace_lock_table == 0;
		ok = ok && !Trace_locks;
		ok = ok && !Trace_lwlocks;
		ok = ok && !Trace_userlocks;
#endif

		PgSetCurrentSession(&fake_session1);
		ok = ok && DeadlockTimeout == 2000;
		ok = ok && StatementTimeout == 3000;
		ok = ok && LockTimeout == 4000;
		ok = ok && IdleInTransactionSessionTimeout == 5000;
		ok = ok && TransactionTimeout == 6000;
		ok = ok && IdleSessionTimeout == 7000;
		ok = ok && !log_lock_waits;
		ok = ok && log_lock_failures;

		PgSetCurrentSession(&fake_session2);
		ok = ok && DeadlockTimeout == 1100;
		ok = ok && StatementTimeout == 1200;
		ok = ok && LockTimeout == 1300;
		ok = ok && IdleInTransactionSessionTimeout == 1400;
		ok = ok && TransactionTimeout == 1500;
		ok = ok && IdleSessionTimeout == 1600;
		ok = ok && log_lock_waits;
		ok = ok && !log_lock_failures;

		PgSetCurrentSession(saved_session);
		SetConfigOption("deadlock_timeout", saved_deadlock_timeout,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("statement_timeout", saved_statement_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lock_timeout", saved_lock_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_in_transaction_session_timeout",
						saved_idle_in_transaction_session_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transaction_timeout", saved_transaction_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_session_timeout", saved_idle_session_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_lock_waits", saved_log_lock_waits,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_lock_failures", saved_log_lock_failures,
						PGC_SUSET, PGC_S_SESSION);
#ifdef LOCK_DEBUG
		SetConfigOption("debug_deadlocks", saved_debug_deadlocks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_oidmin", saved_trace_lock_oidmin,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_table", saved_trace_lock_table,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_locks", saved_trace_locks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lwlocks", saved_trace_lwlocks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_userlocks", saved_trace_userlocks,
						PGC_SUSET, PGC_S_SESSION);
#endif
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("deadlock_timeout", saved_deadlock_timeout,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("statement_timeout", saved_statement_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lock_timeout", saved_lock_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_in_transaction_session_timeout",
						saved_idle_in_transaction_session_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("transaction_timeout", saved_transaction_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("idle_session_timeout", saved_idle_session_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_lock_waits", saved_log_lock_waits,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_lock_failures", saved_log_lock_failures,
						PGC_SUSET, PGC_S_SESSION);
#ifdef LOCK_DEBUG
		SetConfigOption("debug_deadlocks", saved_debug_deadlocks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_oidmin", saved_trace_lock_oidmin,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lock_table", saved_trace_lock_table,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_locks", saved_trace_locks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_lwlocks", saved_trace_lwlocks,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_userlocks", saved_trace_userlocks,
						PGC_SUSET, PGC_S_SESSION);
#endif
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session lock/wait GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_logging_state_is_session_local);
Datum
test_session_logging_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_debug_pretty_print;
	char	   *saved_debug_print_parse;
	char	   *saved_debug_print_plan;
	char	   *saved_debug_print_raw_parse;
	char	   *saved_debug_print_rewritten;
	char	   *saved_log_parser_stats;
	char	   *saved_log_planner_stats;
	char	   *saved_log_executor_stats;
	char	   *saved_log_statement_stats;
#ifdef BTREE_BUILD_STATS
	char	   *saved_log_btree_build_stats;
#endif
	char	   *saved_log_duration;
	char	   *saved_log_error_verbosity;
	char	   *saved_log_parameter_max_length;
	char	   *saved_log_parameter_max_length_on_error;
	char	   *saved_log_min_error_statement;
	char	   *saved_log_min_messages;
	char	   *saved_client_min_messages;
	char	   *saved_log_min_duration_sample;
	char	   *saved_log_min_duration_statement;
	char	   *saved_log_temp_files;
	char	   *saved_log_statement_sample_rate;
	char	   *saved_log_transaction_sample_rate;
	char	   *saved_backtrace_functions;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_debug_pretty_print =
		pstrdup(GetConfigOption("debug_pretty_print", false, false));
	saved_debug_print_parse =
		pstrdup(GetConfigOption("debug_print_parse", false, false));
	saved_debug_print_plan =
		pstrdup(GetConfigOption("debug_print_plan", false, false));
	saved_debug_print_raw_parse =
		pstrdup(GetConfigOption("debug_print_raw_parse", false, false));
	saved_debug_print_rewritten =
		pstrdup(GetConfigOption("debug_print_rewritten", false, false));
	saved_log_parser_stats =
		pstrdup(GetConfigOption("log_parser_stats", false, false));
	saved_log_planner_stats =
		pstrdup(GetConfigOption("log_planner_stats", false, false));
	saved_log_executor_stats =
		pstrdup(GetConfigOption("log_executor_stats", false, false));
	saved_log_statement_stats =
		pstrdup(GetConfigOption("log_statement_stats", false, false));
#ifdef BTREE_BUILD_STATS
	saved_log_btree_build_stats =
		pstrdup(GetConfigOption("log_btree_build_stats", false, false));
#endif
	saved_log_duration =
		pstrdup(GetConfigOption("log_duration", false, false));
	saved_log_error_verbosity =
		pstrdup(GetConfigOption("log_error_verbosity", false, false));
	saved_log_parameter_max_length =
		pstrdup(GetConfigOption("log_parameter_max_length", false, false));
	saved_log_parameter_max_length_on_error =
		pstrdup(GetConfigOption("log_parameter_max_length_on_error",
								false, false));
	saved_log_min_error_statement =
		pstrdup(GetConfigOption("log_min_error_statement", false, false));
	saved_log_min_messages =
		pstrdup(GetConfigOption("log_min_messages", false, false));
	saved_client_min_messages =
		pstrdup(GetConfigOption("client_min_messages", false, false));
	saved_log_min_duration_sample =
		pstrdup(GetConfigOption("log_min_duration_sample", false, false));
	saved_log_min_duration_statement =
		pstrdup(GetConfigOption("log_min_duration_statement", false, false));
	saved_log_temp_files =
		pstrdup(GetConfigOption("log_temp_files", false, false));
	saved_log_statement_sample_rate =
		pstrdup(GetConfigOption("log_statement_sample_rate", false, false));
	saved_log_transaction_sample_rate =
		pstrdup(GetConfigOption("log_transaction_sample_rate", false, false));
	saved_backtrace_functions =
		pstrdup(GetConfigOption("backtrace_functions", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !Debug_print_plan;
		ok = ok && !Debug_print_parse;
		ok = ok && !Debug_print_raw_parse;
		ok = ok && !Debug_print_rewritten;
		ok = ok && Debug_pretty_print;
		ok = ok && !log_parser_stats;
		ok = ok && !log_planner_stats;
		ok = ok && !log_executor_stats;
		ok = ok && !log_statement_stats;
#ifdef BTREE_BUILD_STATS
		ok = ok && !log_btree_build_stats;
#endif
		ok = ok && !log_duration;
		ok = ok && Log_error_verbosity == PGERROR_DEFAULT;
		ok = ok && log_parameter_max_length == -1;
		ok = ok && log_parameter_max_length_on_error == 0;
		ok = ok && log_min_error_statement == ERROR;
		ok = ok && log_min_messages[MyBackendType] == WARNING;
		ok = ok && client_min_messages == NOTICE;
		ok = ok && log_min_duration_sample == -1;
		ok = ok && log_min_duration_statement == -1;
		ok = ok && log_temp_files == -1;
		ok = ok && log_statement_sample_rate == 1.0;
		ok = ok && log_xact_sample_rate == 0;

		SetConfigOption("debug_pretty_print", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_parse", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_plan", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_raw_parse", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_rewritten", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_statement_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
#ifdef BTREE_BUILD_STATS
		SetConfigOption("log_btree_build_stats", "on",
						PGC_SUSET, PGC_S_SESSION);
#endif
		SetConfigOption("log_duration", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_error_verbosity", "verbose",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length", "128",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length_on_error", "256",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_error_statement", "fatal",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_messages", "error",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("client_min_messages", "warning",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_sample", "1000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_statement", "2000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_temp_files", "3000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_sample_rate", "0.25",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_transaction_sample_rate", "0.5",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("backtrace_functions", "errstart",
						PGC_SUSET, PGC_S_SESSION);
		ok = ok && !Debug_pretty_print;
		ok = ok && Debug_print_parse;
		ok = ok && Debug_print_plan;
		ok = ok && Debug_print_raw_parse;
		ok = ok && Debug_print_rewritten;
		ok = ok && log_parser_stats;
		ok = ok && !log_planner_stats;
		ok = ok && !log_executor_stats;
		ok = ok && !log_statement_stats;
#ifdef BTREE_BUILD_STATS
		ok = ok && log_btree_build_stats;
#endif
		ok = ok && log_duration;
		ok = ok && Log_error_verbosity == PGERROR_VERBOSE;
		ok = ok && log_parameter_max_length == 128;
		ok = ok && log_parameter_max_length_on_error == 256;
		ok = ok && log_min_error_statement == FATAL;
		ok = ok && log_min_messages[MyBackendType] == ERROR;
		ok = ok && client_min_messages == WARNING;
		ok = ok && log_min_duration_sample == 1000;
		ok = ok && log_min_duration_statement == 2000;
		ok = ok && log_temp_files == 3000;
		ok = ok && log_statement_sample_rate == 0.25;
		ok = ok && log_xact_sample_rate == 0.5;
		ok = ok && backtrace_functions != NULL &&
			strcmp(backtrace_functions, "errstart") == 0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !Debug_print_plan;
		ok = ok && !Debug_print_parse;
		ok = ok && !Debug_print_raw_parse;
		ok = ok && !Debug_print_rewritten;
		ok = ok && Debug_pretty_print;
		ok = ok && log_min_messages[MyBackendType] == WARNING;
		SetConfigOption("debug_pretty_print", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_parse", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_plan", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_raw_parse", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_rewritten", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_stats", "on",
						PGC_SUSET, PGC_S_SESSION);
#ifdef BTREE_BUILD_STATS
		SetConfigOption("log_btree_build_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
#endif
		SetConfigOption("log_duration", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_error_verbosity", "terse",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length", "64",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length_on_error", "32",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_error_statement", "panic",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_messages", "debug1",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("client_min_messages", "debug1",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_sample", "3000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_statement", "4000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_temp_files", "5000",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_sample_rate", "0.5",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_transaction_sample_rate", "0.25",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("backtrace_functions", "errmsg",
						PGC_SUSET, PGC_S_SESSION);
		ok = ok && Debug_pretty_print;
		ok = ok && !Debug_print_parse;
		ok = ok && !Debug_print_plan;
		ok = ok && !Debug_print_raw_parse;
		ok = ok && !Debug_print_rewritten;
		ok = ok && !log_parser_stats;
		ok = ok && !log_planner_stats;
		ok = ok && !log_executor_stats;
		ok = ok && log_statement_stats;
#ifdef BTREE_BUILD_STATS
		ok = ok && !log_btree_build_stats;
#endif
		ok = ok && !log_duration;
		ok = ok && Log_error_verbosity == PGERROR_TERSE;
		ok = ok && log_parameter_max_length == 64;
		ok = ok && log_parameter_max_length_on_error == 32;
		ok = ok && log_min_error_statement == PANIC;
		ok = ok && log_min_messages[MyBackendType] == DEBUG1;
		ok = ok && client_min_messages == DEBUG1;
		ok = ok && log_min_duration_sample == 3000;
		ok = ok && log_min_duration_statement == 4000;
		ok = ok && log_temp_files == 5000;
		ok = ok && log_statement_sample_rate == 0.5;
		ok = ok && log_xact_sample_rate == 0.25;
		ok = ok && backtrace_functions != NULL &&
			strcmp(backtrace_functions, "errmsg") == 0;

		PgSetCurrentSession(&fake_session1);
		ok = ok && !Debug_pretty_print;
		ok = ok && Debug_print_parse;
		ok = ok && Debug_print_plan;
		ok = ok && Debug_print_raw_parse;
		ok = ok && Debug_print_rewritten;
		ok = ok && log_parser_stats;
		ok = ok && !log_statement_stats;
		ok = ok && log_min_messages[MyBackendType] == ERROR;
		ok = ok && client_min_messages == WARNING;
		ok = ok && backtrace_functions != NULL &&
			strcmp(backtrace_functions, "errstart") == 0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && Debug_pretty_print;
		ok = ok && !Debug_print_parse;
		ok = ok && log_statement_stats;
		ok = ok && log_min_messages[MyBackendType] == DEBUG1;
		ok = ok && client_min_messages == DEBUG1;
		ok = ok && backtrace_functions != NULL &&
			strcmp(backtrace_functions, "errmsg") == 0;

		PgSetCurrentSession(saved_session);
		SetConfigOption("log_statement_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("debug_pretty_print", saved_debug_pretty_print,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_parse", saved_debug_print_parse,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_plan", saved_debug_print_plan,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_raw_parse", saved_debug_print_raw_parse,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_rewritten", saved_debug_print_rewritten,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", saved_log_parser_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", saved_log_planner_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", saved_log_executor_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_stats", saved_log_statement_stats,
						PGC_SUSET, PGC_S_SESSION);
#ifdef BTREE_BUILD_STATS
		SetConfigOption("log_btree_build_stats", saved_log_btree_build_stats,
						PGC_SUSET, PGC_S_SESSION);
#endif
		SetConfigOption("log_duration", saved_log_duration,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_error_verbosity", saved_log_error_verbosity,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length",
						saved_log_parameter_max_length,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length_on_error",
						saved_log_parameter_max_length_on_error,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_error_statement",
						saved_log_min_error_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_messages", saved_log_min_messages,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("client_min_messages", saved_client_min_messages,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_sample",
						saved_log_min_duration_sample,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_statement",
						saved_log_min_duration_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_temp_files", saved_log_temp_files,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_sample_rate",
						saved_log_statement_sample_rate,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_transaction_sample_rate",
						saved_log_transaction_sample_rate,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("backtrace_functions", saved_backtrace_functions,
						PGC_SUSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("log_statement_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("debug_pretty_print", saved_debug_pretty_print,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_parse", saved_debug_print_parse,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_plan", saved_debug_print_plan,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_raw_parse", saved_debug_print_raw_parse,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_print_rewritten", saved_debug_print_rewritten,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_parser_stats", saved_log_parser_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_planner_stats", saved_log_planner_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_executor_stats", saved_log_executor_stats,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_stats", saved_log_statement_stats,
						PGC_SUSET, PGC_S_SESSION);
#ifdef BTREE_BUILD_STATS
		SetConfigOption("log_btree_build_stats", saved_log_btree_build_stats,
						PGC_SUSET, PGC_S_SESSION);
#endif
		SetConfigOption("log_duration", saved_log_duration,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_error_verbosity", saved_log_error_verbosity,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length",
						saved_log_parameter_max_length,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_parameter_max_length_on_error",
						saved_log_parameter_max_length_on_error,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_error_statement",
						saved_log_min_error_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_messages", saved_log_min_messages,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("client_min_messages", saved_client_min_messages,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_sample",
						saved_log_min_duration_sample,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_min_duration_statement",
						saved_log_min_duration_statement,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_temp_files", saved_log_temp_files,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_statement_sample_rate",
						saved_log_statement_sample_rate,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("log_transaction_sample_rate",
						saved_log_transaction_sample_rate,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("backtrace_functions", saved_backtrace_functions,
						PGC_SUSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session logging GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_pgstat_state_is_session_local);
Datum
test_session_pgstat_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_stats_fetch_consistency;
	char	   *saved_track_activities;
	char	   *saved_track_counts;
	char	   *saved_track_functions;
	SessionEndType saved_session_end_cause;
	PgStat_Counter saved_last_session_report_time;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_stats_fetch_consistency =
		pstrdup(GetConfigOption("stats_fetch_consistency", false, false));
	saved_track_activities =
		pstrdup(GetConfigOption("track_activities", false, false));
	saved_track_counts =
		pstrdup(GetConfigOption("track_counts", false, false));
	saved_track_functions =
		pstrdup(GetConfigOption("track_functions", false, false));
	saved_session_end_cause = pgStatSessionEndCause;
	saved_last_session_report_time = *PgCurrentPgStatLastSessionReportTimeRef();
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && pgstat_track_counts;
		ok = ok && pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_OFF;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_CACHE;
		ok = ok && pgStatSessionEndCause == DISCONNECT_NORMAL;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 0;

		SetConfigOption("track_counts", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_activities", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_functions", "all",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("stats_fetch_consistency", "none",
						PGC_USERSET, PGC_S_SESSION);
		pgStatSessionEndCause = DISCONNECT_CLIENT_EOF;
		*PgCurrentPgStatLastSessionReportTimeRef() = 12345;
		ok = ok && !pgstat_track_counts;
		ok = ok && !pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_ALL;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_NONE;
		ok = ok && pgStatSessionEndCause == DISCONNECT_CLIENT_EOF;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 12345;

		PgSetCurrentSession(&fake_session2);
		ok = ok && pgstat_track_counts;
		ok = ok && pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_OFF;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_CACHE;
		ok = ok && pgStatSessionEndCause == DISCONNECT_NORMAL;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 0;
		SetConfigOption("track_counts", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_activities", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_functions", "pl",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("stats_fetch_consistency", "snapshot",
						PGC_USERSET, PGC_S_SESSION);
		pgStatSessionEndCause = DISCONNECT_KILLED;
		*PgCurrentPgStatLastSessionReportTimeRef() = 67890;
		ok = ok && pgstat_track_counts;
		ok = ok && pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_PL;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
		ok = ok && pgStatSessionEndCause == DISCONNECT_KILLED;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 67890;

		PgSetCurrentSession(&fake_session1);
		ok = ok && !pgstat_track_counts;
		ok = ok && !pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_ALL;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_NONE;
		ok = ok && pgStatSessionEndCause == DISCONNECT_CLIENT_EOF;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 12345;

		PgSetCurrentSession(&fake_session2);
		ok = ok && pgstat_track_counts;
		ok = ok && pgstat_track_activities;
		ok = ok && pgstat_track_functions == TRACK_FUNC_PL;
		ok = ok &&
			pgstat_fetch_consistency == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
		ok = ok && pgStatSessionEndCause == DISCONNECT_KILLED;
		ok = ok && *PgCurrentPgStatLastSessionReportTimeRef() == 67890;

		PgSetCurrentSession(saved_session);
		SetConfigOption("stats_fetch_consistency",
						saved_stats_fetch_consistency,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_activities", saved_track_activities,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_counts", saved_track_counts,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_functions", saved_track_functions,
						PGC_SUSET, PGC_S_SESSION);
		pgStatSessionEndCause = saved_session_end_cause;
		*PgCurrentPgStatLastSessionReportTimeRef() =
			saved_last_session_report_time;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("stats_fetch_consistency",
						saved_stats_fetch_consistency,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_activities", saved_track_activities,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_counts", saved_track_counts,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("track_functions", saved_track_functions,
						PGC_SUSET, PGC_S_SESSION);
		pgStatSessionEndCause = saved_session_end_cause;
		*PgCurrentPgStatLastSessionReportTimeRef() =
			saved_last_session_report_time;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session pgstat state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_query_id_state_is_session_local);
Datum
test_session_query_id_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_compute_query_id;
	bool		saved_query_id_enabled;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_compute_query_id =
		pstrdup(GetConfigOption("compute_query_id", false, false));
	saved_query_id_enabled = query_id_enabled;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_AUTO;
		ok = ok && !query_id_enabled;
		ok = ok && !IsQueryIdEnabled();

		SetConfigOption("compute_query_id", "off",
						PGC_SUSET, PGC_S_SESSION);
		query_id_enabled = true;
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_OFF;
		ok = ok && query_id_enabled;
		ok = ok && !IsQueryIdEnabled();

		PgSetCurrentSession(&fake_session2);
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_AUTO;
		ok = ok && !query_id_enabled;
		ok = ok && !IsQueryIdEnabled();

		SetConfigOption("compute_query_id", "auto",
						PGC_SUSET, PGC_S_SESSION);
		EnableQueryId();
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_AUTO;
		ok = ok && query_id_enabled;
		ok = ok && IsQueryIdEnabled();

		PgSetCurrentSession(&fake_session1);
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_OFF;
		ok = ok && query_id_enabled;
		ok = ok && !IsQueryIdEnabled();

		PgSetCurrentSession(&fake_session2);
		ok = ok && compute_query_id == COMPUTE_QUERY_ID_AUTO;
		ok = ok && query_id_enabled;
		ok = ok && IsQueryIdEnabled();

		PgSetCurrentSession(saved_session);
		SetConfigOption("compute_query_id", saved_compute_query_id,
						PGC_SUSET, PGC_S_SESSION);
		query_id_enabled = saved_query_id_enabled;
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("compute_query_id", saved_compute_query_id,
						PGC_SUSET, PGC_S_SESSION);
		query_id_enabled = saved_query_id_enabled;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session query ID state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_storage_guc_state_is_session_local);
Datum
test_session_storage_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_ignore_checksum_failure;
	char	   *saved_file_copy_method;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_ignore_checksum_failure =
		pstrdup(GetConfigOption("ignore_checksum_failure", false, false));
	saved_file_copy_method =
		pstrdup(GetConfigOption("file_copy_method", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_COPY;

		SetConfigOption("ignore_checksum_failure", "on",
						PGC_SUSET, PGC_S_SESSION);
		file_copy_method = FILE_COPY_METHOD_CLONE;
		ok = ok && ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_CLONE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_COPY;
		SetConfigOption("ignore_checksum_failure", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("file_copy_method", "copy",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && !ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_COPY;

		PgSetCurrentSession(&fake_session1);
		ok = ok && ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_CLONE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !ignore_checksum_failure;
		ok = ok && file_copy_method == FILE_COPY_METHOD_COPY;

		PgSetCurrentSession(saved_session);
		SetConfigOption("ignore_checksum_failure",
						saved_ignore_checksum_failure,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("file_copy_method", saved_file_copy_method,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("ignore_checksum_failure",
						saved_ignore_checksum_failure,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("file_copy_method", saved_file_copy_method,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session storage GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_user_guc_state_is_session_local);
Datum
test_session_user_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_password_encryption;
	char	   *saved_createrole_self_grant;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_password_encryption =
		pstrdup(GetConfigOption("password_encryption", false, false));
	saved_createrole_self_grant =
		pstrdup(GetConfigOption("createrole_self_grant", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && Password_encryption == PASSWORD_TYPE_SCRAM_SHA_256;
		ok = ok && strcmp(createrole_self_grant, "") == 0;
		ok = ok && !*PgCurrentCreateRoleSelfGrantEnabledRef();

		SetConfigOption("password_encryption", "md5",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("createrole_self_grant", "set, inherit",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && Password_encryption == PASSWORD_TYPE_MD5;
		ok = ok && strcmp(createrole_self_grant, "set, inherit") == 0;
		ok = ok && *PgCurrentCreateRoleSelfGrantEnabledRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsSpecifiedRef() != 0;
		ok = ok && !*PgCurrentCreateRoleSelfGrantOptionsAdminRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsInheritRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsSetRef();

		PgSetCurrentSession(&fake_session2);
		ok = ok && Password_encryption == PASSWORD_TYPE_SCRAM_SHA_256;
		ok = ok && strcmp(createrole_self_grant, "") == 0;
		ok = ok && !*PgCurrentCreateRoleSelfGrantEnabledRef();
		SetConfigOption("password_encryption", "scram-sha-256",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("createrole_self_grant", "set",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && Password_encryption == PASSWORD_TYPE_SCRAM_SHA_256;
		ok = ok && strcmp(createrole_self_grant, "set") == 0;
		ok = ok && *PgCurrentCreateRoleSelfGrantEnabledRef();
		ok = ok && !*PgCurrentCreateRoleSelfGrantOptionsInheritRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsSetRef();

		PgSetCurrentSession(&fake_session1);
		ok = ok && Password_encryption == PASSWORD_TYPE_MD5;
		ok = ok && strcmp(createrole_self_grant, "set, inherit") == 0;
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsInheritRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsSetRef();

		PgSetCurrentSession(&fake_session2);
		ok = ok && Password_encryption == PASSWORD_TYPE_SCRAM_SHA_256;
		ok = ok && strcmp(createrole_self_grant, "set") == 0;
		ok = ok && !*PgCurrentCreateRoleSelfGrantOptionsInheritRef();
		ok = ok && *PgCurrentCreateRoleSelfGrantOptionsSetRef();

		PgSetCurrentSession(saved_session);
		SetConfigOption("password_encryption", saved_password_encryption,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("createrole_self_grant",
						saved_createrole_self_grant,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("password_encryption", saved_password_encryption,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("createrole_self_grant",
						saved_createrole_self_grant,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session user GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_user_identity_state_is_session_local);
Datum
test_session_user_identity_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	PgSessionUserIdentityState *identity_state;
	Oid			userid;
	int			sec_context;
	bool		sec_def_context;
	bool		ok = true;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		identity_state = PgCurrentUserIdentityState();
		ok = ok && identity_state->initialized;
		ok = ok && identity_state->authenticated_user_id == InvalidOid;
		ok = ok && identity_state->session_user_id == InvalidOid;
		ok = ok && identity_state->outer_user_id == InvalidOid;
		ok = ok && identity_state->current_user_id == InvalidOid;
		ok = ok && identity_state->system_user == NULL;
		ok = ok && !identity_state->session_user_is_superuser;
		ok = ok && identity_state->security_restriction_context == 0;
		ok = ok && !identity_state->set_role_is_active;
		ok = ok && identity_state->cached_role[0] == InvalidOid;
		ok = ok && identity_state->cached_roles[0] == NIL;
		ok = ok && identity_state->cached_db_hash == 0;

		identity_state->authenticated_user_id = 10;
		identity_state->session_user_id = 11;
		identity_state->outer_user_id = 12;
		identity_state->current_user_id = 13;
		identity_state->system_user = "auth_method_a:authn_id_a";
		identity_state->session_user_is_superuser = true;
		identity_state->security_restriction_context =
			SECURITY_RESTRICTED_OPERATION | SECURITY_NOFORCE_RLS;
		identity_state->set_role_is_active = true;
		identity_state->cached_role[0] = 31;
		identity_state->cached_roles[0] = list_make1_oid(31);
		identity_state->cached_db_hash = 41;

		ok = ok && GetAuthenticatedUserId() == 10;
		ok = ok && GetSessionUserId() == 11;
		ok = ok && GetOuterUserId() == 12;
		ok = ok && GetUserId() == 13;
		ok = ok && GetSessionUserIsSuperuser();
		ok = ok && strcmp(GetSystemUser(), "auth_method_a:authn_id_a") == 0;
		ok = ok && GetCurrentRoleId() == 12;
		ok = ok && InSecurityRestrictedOperation();
		ok = ok && InNoForceRLSOperation();
		ok = ok && !InLocalUserIdChange();
		GetUserIdAndSecContext(&userid, &sec_context);
		ok = ok && userid == 13;
		ok = ok && sec_context == (SECURITY_RESTRICTED_OPERATION |
								   SECURITY_NOFORCE_RLS);

		SetUserIdAndSecContext(14, SECURITY_LOCAL_USERID_CHANGE);
		ok = ok && GetUserId() == 14;
		ok = ok && InLocalUserIdChange();
		GetUserIdAndContext(&userid, &sec_def_context);
		ok = ok && userid == 14;
		ok = ok && sec_def_context;
		SetUserIdAndContext(15, false);
		ok = ok && GetUserId() == 15;
		ok = ok && !InLocalUserIdChange();

		PgSetCurrentSession(&fake_session2);
		identity_state = PgCurrentUserIdentityState();
		ok = ok && identity_state->initialized;
		ok = ok && identity_state->authenticated_user_id == InvalidOid;
		ok = ok && identity_state->session_user_id == InvalidOid;
		ok = ok && identity_state->outer_user_id == InvalidOid;
		ok = ok && identity_state->current_user_id == InvalidOid;
		ok = ok && identity_state->system_user == NULL;
		ok = ok && !identity_state->session_user_is_superuser;
		ok = ok && identity_state->security_restriction_context == 0;
		ok = ok && !identity_state->set_role_is_active;
		ok = ok && identity_state->cached_role[0] == InvalidOid;
		ok = ok && identity_state->cached_roles[0] == NIL;
		ok = ok && identity_state->cached_db_hash == 0;

		identity_state->authenticated_user_id = 20;
		identity_state->session_user_id = 21;
		identity_state->outer_user_id = 22;
		identity_state->current_user_id = 23;
		identity_state->system_user = "auth_method_b:authn_id_b";
		identity_state->session_user_is_superuser = false;
		identity_state->set_role_is_active = false;
		identity_state->cached_role[0] = 32;
		identity_state->cached_roles[0] = list_make1_oid(32);
		identity_state->cached_db_hash = 42;

		ok = ok && GetAuthenticatedUserId() == 20;
		ok = ok && GetSessionUserId() == 21;
		ok = ok && GetOuterUserId() == 22;
		ok = ok && GetUserId() == 23;
		ok = ok && !GetSessionUserIsSuperuser();
		ok = ok && strcmp(GetSystemUser(), "auth_method_b:authn_id_b") == 0;
		ok = ok && GetCurrentRoleId() == InvalidOid;
		ok = ok && !InSecurityRestrictedOperation();
		ok = ok && !InNoForceRLSOperation();
		ok = ok && !InLocalUserIdChange();

		PgSetCurrentSession(&fake_session1);
		ok = ok && GetAuthenticatedUserId() == 10;
		ok = ok && GetSessionUserId() == 11;
		ok = ok && GetOuterUserId() == 12;
		ok = ok && GetUserId() == 15;
		ok = ok && GetSessionUserIsSuperuser();
		ok = ok && strcmp(GetSystemUser(), "auth_method_a:authn_id_a") == 0;
		ok = ok && GetCurrentRoleId() == 12;
		ok = ok && !InLocalUserIdChange();
		ok = ok && PgCurrentUserIdentityState()->cached_role[0] == 31;
		ok = ok && list_member_oid(PgCurrentUserIdentityState()->cached_roles[0],
								   31);
		ok = ok && PgCurrentUserIdentityState()->cached_db_hash == 41;

		PgSetCurrentSession(&fake_session2);
		ok = ok && PgCurrentUserIdentityState()->cached_role[0] == 32;
		ok = ok && list_member_oid(PgCurrentUserIdentityState()->cached_roles[0],
								   32);
		ok = ok && PgCurrentUserIdentityState()->cached_db_hash == 42;

		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "user identity state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_command_guc_state_is_session_local);
Datum
test_session_command_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_session_replication_role;
	char	   *saved_event_triggers;
	char	   *saved_trace_notify;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_session_replication_role =
		pstrdup(GetConfigOption("session_replication_role", false, false));
	saved_event_triggers =
		pstrdup(GetConfigOption("event_triggers", false, false));
	saved_trace_notify =
		pstrdup(GetConfigOption("trace_notify", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_ORIGIN;
		ok = ok && event_triggers;
		ok = ok && !Trace_notify;

		SetConfigOption("session_replication_role", "replica",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("event_triggers", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_notify", "on",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA;
		ok = ok && !event_triggers;
		ok = ok && Trace_notify;

		PgSetCurrentSession(&fake_session2);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_ORIGIN;
		ok = ok && event_triggers;
		ok = ok && !Trace_notify;
		SetConfigOption("session_replication_role", "local",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("event_triggers", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_notify", "off",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_LOCAL;
		ok = ok && event_triggers;
		ok = ok && !Trace_notify;

		PgSetCurrentSession(&fake_session1);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_REPLICA;
		ok = ok && !event_triggers;
		ok = ok && Trace_notify;

		PgSetCurrentSession(&fake_session2);
		ok = ok && SessionReplicationRole == SESSION_REPLICATION_ROLE_LOCAL;
		ok = ok && event_triggers;
		ok = ok && !Trace_notify;

		PgSetCurrentSession(saved_session);
		SetConfigOption("session_replication_role",
						saved_session_replication_role,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("event_triggers", saved_event_triggers,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_notify", saved_trace_notify,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("session_replication_role",
						saved_session_replication_role,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("event_triggers", saved_event_triggers,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("trace_notify", saved_trace_notify,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session command GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_replication_guc_state_is_session_local);
Datum
test_session_replication_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_wal_sender_timeout;
	char	   *saved_wal_sender_shutdown_timeout;
	char	   *saved_log_replication_commands;
	char	   *saved_wal_receiver_timeout;
	char	   *saved_logical_decoding_work_mem;
	char	   *saved_debug_logical_replication_streaming;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_wal_sender_timeout =
		pstrdup(GetConfigOption("wal_sender_timeout", false, false));
	saved_wal_sender_shutdown_timeout =
		pstrdup(GetConfigOption("wal_sender_shutdown_timeout", false, false));
	saved_log_replication_commands =
		pstrdup(GetConfigOption("log_replication_commands", false, false));
	saved_wal_receiver_timeout =
		pstrdup(GetConfigOption("wal_receiver_timeout", false, false));
	saved_logical_decoding_work_mem =
		pstrdup(GetConfigOption("logical_decoding_work_mem", false, false));
	saved_debug_logical_replication_streaming =
		pstrdup(GetConfigOption("debug_logical_replication_streaming",
								false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && wal_sender_timeout == 60 * 1000;
		ok = ok && wal_sender_shutdown_timeout == -1;
		ok = ok && !log_replication_commands;
		ok = ok && wal_receiver_timeout == 60 * 1000;
		ok = ok && logical_decoding_work_mem == 65536;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_BUFFERED;

		SetConfigOption("wal_sender_timeout", "7000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_shutdown_timeout", "8000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_replication_commands", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_receiver_timeout", "9000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("logical_decoding_work_mem", "128MB",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_logical_replication_streaming", "immediate",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && wal_sender_timeout == 7000;
		ok = ok && wal_sender_shutdown_timeout == 8000;
		ok = ok && log_replication_commands;
		ok = ok && wal_receiver_timeout == 9000;
		ok = ok && logical_decoding_work_mem == 131072;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && wal_sender_timeout == 60 * 1000;
		ok = ok && wal_sender_shutdown_timeout == -1;
		ok = ok && !log_replication_commands;
		ok = ok && wal_receiver_timeout == 60 * 1000;
		ok = ok && logical_decoding_work_mem == 65536;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_BUFFERED;
		SetConfigOption("wal_sender_timeout", "1000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_shutdown_timeout", "-1",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_replication_commands", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_receiver_timeout", "2000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("logical_decoding_work_mem", "64kB",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_logical_replication_streaming", "buffered",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && wal_sender_timeout == 1000;
		ok = ok && wal_sender_shutdown_timeout == -1;
		ok = ok && !log_replication_commands;
		ok = ok && wal_receiver_timeout == 2000;
		ok = ok && logical_decoding_work_mem == 64;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_BUFFERED;

		PgSetCurrentSession(&fake_session1);
		ok = ok && wal_sender_timeout == 7000;
		ok = ok && wal_sender_shutdown_timeout == 8000;
		ok = ok && log_replication_commands;
		ok = ok && wal_receiver_timeout == 9000;
		ok = ok && logical_decoding_work_mem == 131072;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_IMMEDIATE;

		PgSetCurrentSession(&fake_session2);
		ok = ok && wal_sender_timeout == 1000;
		ok = ok && wal_sender_shutdown_timeout == -1;
		ok = ok && !log_replication_commands;
		ok = ok && wal_receiver_timeout == 2000;
		ok = ok && logical_decoding_work_mem == 64;
		ok = ok && debug_logical_replication_streaming ==
			DEBUG_LOGICAL_REP_STREAMING_BUFFERED;

		PgSetCurrentSession(saved_session);
		SetConfigOption("debug_logical_replication_streaming",
						saved_debug_logical_replication_streaming,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("logical_decoding_work_mem",
						saved_logical_decoding_work_mem,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_replication_commands",
						saved_log_replication_commands,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_receiver_timeout", saved_wal_receiver_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_shutdown_timeout",
						saved_wal_sender_shutdown_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_timeout", saved_wal_sender_timeout,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("debug_logical_replication_streaming",
						saved_debug_logical_replication_streaming,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("logical_decoding_work_mem",
						saved_logical_decoding_work_mem,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("log_replication_commands",
						saved_log_replication_commands,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_receiver_timeout", saved_wal_receiver_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_shutdown_timeout",
						saved_wal_sender_shutdown_timeout,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_sender_timeout", saved_wal_sender_timeout,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session replication GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_general_guc_state_is_session_local);
Datum
test_session_general_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_allow_alter_system;
	char	   *saved_row_security;
	char	   *saved_check_function_bodies;
	char	   *saved_is_superuser;
	char	   *saved_temp_file_limit;
	char	   *saved_temp_buffers;
	char	   *saved_role;
	char	   *saved_lo_compat_privileges;
	char	   *saved_extra_float_digits;
	char	   *saved_array_nulls;
	char	   *saved_bytea_output;
	char	   *saved_xmlbinary;
	char	   *saved_xmloption;
	char	   *saved_quote_all_identifiers;
	char	   *saved_plan_cache_mode;
	char	   *saved_gin_fuzzy_search_limit;
	char	   *saved_gin_pending_list_limit;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_allow_alter_system =
		pstrdup(GetConfigOption("allow_alter_system", false, false));
	saved_row_security =
		pstrdup(GetConfigOption("row_security", false, false));
	saved_check_function_bodies =
		pstrdup(GetConfigOption("check_function_bodies", false, false));
	saved_is_superuser =
		pstrdup(GetConfigOption("is_superuser", false, false));
	saved_temp_file_limit =
		pstrdup(GetConfigOption("temp_file_limit", false, false));
	saved_temp_buffers =
		pstrdup(GetConfigOption("temp_buffers", false, false));
	saved_role = pstrdup(GetConfigOption("role", false, false));
	saved_lo_compat_privileges =
		pstrdup(GetConfigOption("lo_compat_privileges", false, false));
	saved_extra_float_digits =
		pstrdup(GetConfigOption("extra_float_digits", false, false));
	saved_array_nulls =
		pstrdup(GetConfigOption("array_nulls", false, false));
	saved_bytea_output =
		pstrdup(GetConfigOption("bytea_output", false, false));
	saved_xmlbinary =
		pstrdup(GetConfigOption("xmlbinary", false, false));
	saved_xmloption =
		pstrdup(GetConfigOption("xmloption", false, false));
	saved_quote_all_identifiers =
		pstrdup(GetConfigOption("quote_all_identifiers", false, false));
	saved_plan_cache_mode =
		pstrdup(GetConfigOption("plan_cache_mode", false, false));
	saved_gin_fuzzy_search_limit =
		pstrdup(GetConfigOption("gin_fuzzy_search_limit", false, false));
	saved_gin_pending_list_limit =
		pstrdup(GetConfigOption("gin_pending_list_limit", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && AllowAlterSystem;
		ok = ok && row_security;
		ok = ok && check_function_bodies;
		ok = ok && !current_role_is_superuser;
		ok = ok && temp_file_limit == -1;
		ok = ok && num_temp_buffers == 1024;
		ok = ok && strcmp(role_string, "none") == 0;
		ok = ok && !lo_compat_privileges;
		ok = ok && extra_float_digits == 1;
		ok = ok && Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_HEX;
		ok = ok && xmlbinary == XMLBINARY_BASE64;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_CONTENT;
		ok = ok && !quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_AUTO;
		ok = ok && GinFuzzySearchLimit == 0;
		ok = ok && gin_pending_list_limit == 0;

		SetConfigOption("allow_alter_system", "off",
						PGC_SIGHUP, PGC_S_SESSION);
		SetConfigOption("row_security", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("check_function_bodies", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("is_superuser", "on",
						PGC_INTERNAL, PGC_S_OVERRIDE);
		SetConfigOption("temp_file_limit", "64MB",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("temp_buffers", "800",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("role", "none",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lo_compat_privileges", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extra_float_digits", "2",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("array_nulls", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("bytea_output", "escape",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmlbinary", "hex",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmloption", "document",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("quote_all_identifiers", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("plan_cache_mode", "force_generic_plan",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_fuzzy_search_limit", "7",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_pending_list_limit", "8MB",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && !AllowAlterSystem;
		ok = ok && !row_security;
		ok = ok && !check_function_bodies;
		ok = ok && current_role_is_superuser;
		ok = ok && temp_file_limit == 65536;
		ok = ok && num_temp_buffers == 800;
		ok = ok && strcmp(role_string, "none") == 0;
		ok = ok && lo_compat_privileges;
		ok = ok && extra_float_digits == 2;
		ok = ok && !Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_ESCAPE;
		ok = ok && xmlbinary == XMLBINARY_HEX;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_DOCUMENT;
		ok = ok && quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_FORCE_GENERIC_PLAN;
		ok = ok && GinFuzzySearchLimit == 7;
		ok = ok && gin_pending_list_limit == 8192;

		PgSetCurrentSession(&fake_session2);
		ok = ok && AllowAlterSystem;
		ok = ok && row_security;
		ok = ok && check_function_bodies;
		ok = ok && !current_role_is_superuser;
		ok = ok && temp_file_limit == -1;
		ok = ok && num_temp_buffers == 1024;
		ok = ok && strcmp(role_string, "none") == 0;
		ok = ok && !lo_compat_privileges;
		ok = ok && extra_float_digits == 1;
		ok = ok && Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_HEX;
		ok = ok && xmlbinary == XMLBINARY_BASE64;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_CONTENT;
		ok = ok && !quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_AUTO;
		ok = ok && GinFuzzySearchLimit == 0;
		ok = ok && gin_pending_list_limit == 0;
		SetConfigOption("row_security", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("check_function_bodies", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("is_superuser", "off",
						PGC_INTERNAL, PGC_S_OVERRIDE);
		SetConfigOption("temp_file_limit", "128MB",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("temp_buffers", "900",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lo_compat_privileges", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extra_float_digits", "3",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("array_nulls", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("bytea_output", "hex",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmlbinary", "base64",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmloption", "content",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("quote_all_identifiers", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("plan_cache_mode", "force_custom_plan",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_fuzzy_search_limit", "11",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_pending_list_limit", "16MB",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && row_security;
		ok = ok && check_function_bodies;
		ok = ok && !current_role_is_superuser;
		ok = ok && temp_file_limit == 131072;
		ok = ok && num_temp_buffers == 900;
		ok = ok && !lo_compat_privileges;
		ok = ok && extra_float_digits == 3;
		ok = ok && Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_HEX;
		ok = ok && xmlbinary == XMLBINARY_BASE64;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_CONTENT;
		ok = ok && !quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN;
		ok = ok && GinFuzzySearchLimit == 11;
		ok = ok && gin_pending_list_limit == 16384;

		PgSetCurrentSession(&fake_session1);
		ok = ok && !AllowAlterSystem;
		ok = ok && !row_security;
		ok = ok && !check_function_bodies;
		ok = ok && current_role_is_superuser;
		ok = ok && temp_file_limit == 65536;
		ok = ok && num_temp_buffers == 800;
		ok = ok && lo_compat_privileges;
		ok = ok && extra_float_digits == 2;
		ok = ok && !Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_ESCAPE;
		ok = ok && xmlbinary == XMLBINARY_HEX;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_DOCUMENT;
		ok = ok && quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_FORCE_GENERIC_PLAN;
		ok = ok && GinFuzzySearchLimit == 7;
		ok = ok && gin_pending_list_limit == 8192;

		PgSetCurrentSession(&fake_session2);
		ok = ok && AllowAlterSystem;
		ok = ok && row_security;
		ok = ok && check_function_bodies;
		ok = ok && !current_role_is_superuser;
		ok = ok && temp_file_limit == 131072;
		ok = ok && num_temp_buffers == 900;
		ok = ok && !lo_compat_privileges;
		ok = ok && extra_float_digits == 3;
		ok = ok && Array_nulls;
		ok = ok && bytea_output == BYTEA_OUTPUT_HEX;
		ok = ok && xmlbinary == XMLBINARY_BASE64;
		ok = ok && *PgCurrentXmlOptionRef() == XMLOPTION_CONTENT;
		ok = ok && !quote_all_identifiers;
		ok = ok && plan_cache_mode == PLAN_CACHE_MODE_FORCE_CUSTOM_PLAN;
		ok = ok && GinFuzzySearchLimit == 11;
		ok = ok && gin_pending_list_limit == 16384;

		PgSetCurrentSession(saved_session);
		SetConfigOption("gin_pending_list_limit",
						saved_gin_pending_list_limit,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_fuzzy_search_limit",
						saved_gin_fuzzy_search_limit,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("plan_cache_mode", saved_plan_cache_mode,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("quote_all_identifiers",
						saved_quote_all_identifiers,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmlbinary", saved_xmlbinary,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("bytea_output", saved_bytea_output,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("array_nulls", saved_array_nulls,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("extra_float_digits", saved_extra_float_digits,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmloption", saved_xmloption,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lo_compat_privileges", saved_lo_compat_privileges,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("role", saved_role,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_buffers", saved_temp_buffers,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_file_limit", saved_temp_file_limit,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("is_superuser", saved_is_superuser,
						PGC_INTERNAL, PGC_S_OVERRIDE);
		SetConfigOption("check_function_bodies",
						saved_check_function_bodies,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("row_security", saved_row_security,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_alter_system", saved_allow_alter_system,
						PGC_SIGHUP, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("gin_pending_list_limit",
						saved_gin_pending_list_limit,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("gin_fuzzy_search_limit",
						saved_gin_fuzzy_search_limit,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("plan_cache_mode", saved_plan_cache_mode,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("quote_all_identifiers",
						saved_quote_all_identifiers,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmlbinary", saved_xmlbinary,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("bytea_output", saved_bytea_output,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("array_nulls", saved_array_nulls,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("extra_float_digits", saved_extra_float_digits,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("xmloption", saved_xmloption,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("lo_compat_privileges", saved_lo_compat_privileges,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("role", saved_role,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_buffers", saved_temp_buffers,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("temp_file_limit", saved_temp_file_limit,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("is_superuser", saved_is_superuser,
						PGC_INTERNAL, PGC_S_OVERRIDE);
		SetConfigOption("check_function_bodies",
						saved_check_function_bodies,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("row_security", saved_row_security,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("allow_alter_system", saved_allow_alter_system,
						PGC_SIGHUP, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session general GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_access_wal_guc_state_is_session_local);
Datum
test_session_access_wal_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_synchronize_seqscans;
	char	   *saved_wal_compression;
	char	   *saved_wal_init_zero;
	char	   *saved_wal_recycle;
	char	   *saved_wal_consistency_checking;
	char	   *saved_commit_delay;
	char	   *saved_commit_siblings;
	char	   *saved_track_wal_io_timing;
	char	   *saved_wal_skip_threshold;
	bool	   *fake1_wal_consistency_checking;
	bool	   *fake2_wal_consistency_checking;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_synchronize_seqscans =
		pstrdup(GetConfigOption("synchronize_seqscans", false, false));
	saved_wal_compression =
		pstrdup(GetConfigOption("wal_compression", false, false));
	saved_wal_init_zero =
		pstrdup(GetConfigOption("wal_init_zero", false, false));
	saved_wal_recycle =
		pstrdup(GetConfigOption("wal_recycle", false, false));
	saved_wal_consistency_checking =
		pstrdup(GetConfigOption("wal_consistency_checking", false, false));
	saved_commit_delay =
		pstrdup(GetConfigOption("commit_delay", false, false));
	saved_commit_siblings =
		pstrdup(GetConfigOption("commit_siblings", false, false));
	saved_track_wal_io_timing =
		pstrdup(GetConfigOption("track_wal_io_timing", false, false));
	saved_wal_skip_threshold =
		pstrdup(GetConfigOption("wal_skip_threshold", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(default_table_access_method,
						  DEFAULT_TABLE_ACCESS_METHOD) == 0;
		ok = ok && synchronize_seqscans;
		ok = ok && default_toast_compression == DEFAULT_TOAST_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_NONE;
		ok = ok && wal_init_zero;
		ok = ok && wal_recycle;
		ok = ok && wal_consistency_checking_string == NULL;
		ok = ok && wal_consistency_checking == NULL;
		ok = ok && CommitDelay == 0;
		ok = ok && CommitSiblings == 5;
		ok = ok && !track_wal_io_timing;
		ok = ok && wal_skip_threshold == 2048;

		default_table_access_method = "session1_tableam";
		default_toast_compression = TOAST_LZ4_COMPRESSION;
		SetConfigOption("synchronize_seqscans", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_compression", "pglz",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_init_zero", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_recycle", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_consistency_checking", "all",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_delay", "100",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_siblings", "8",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_wal_io_timing", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_skip_threshold", "3MB",
						PGC_USERSET, PGC_S_SESSION);
		fake1_wal_consistency_checking = wal_consistency_checking;
		ok = ok && strcmp(default_table_access_method,
						  "session1_tableam") == 0;
		ok = ok && !synchronize_seqscans;
		ok = ok && default_toast_compression == TOAST_LZ4_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_PGLZ;
		ok = ok && !wal_init_zero;
		ok = ok && !wal_recycle;
		ok = ok && strcmp(wal_consistency_checking_string, "all") == 0;
		ok = ok && fake1_wal_consistency_checking != NULL;
		ok = ok && CommitDelay == 100;
		ok = ok && CommitSiblings == 8;
		ok = ok && track_wal_io_timing;
		ok = ok && wal_skip_threshold == 3072;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(default_table_access_method,
						  DEFAULT_TABLE_ACCESS_METHOD) == 0;
		ok = ok && synchronize_seqscans;
		ok = ok && default_toast_compression == DEFAULT_TOAST_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_NONE;
		ok = ok && wal_init_zero;
		ok = ok && wal_recycle;
		ok = ok && wal_consistency_checking_string == NULL;
		ok = ok && wal_consistency_checking == NULL;
		ok = ok && CommitDelay == 0;
		ok = ok && CommitSiblings == 5;
		ok = ok && !track_wal_io_timing;
		ok = ok && wal_skip_threshold == 2048;

		default_table_access_method = "session2_tableam";
		default_toast_compression = TOAST_PGLZ_COMPRESSION;
		SetConfigOption("synchronize_seqscans", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("wal_compression", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_init_zero", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_recycle", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_consistency_checking", "",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_delay", "200",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_siblings", "9",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_wal_io_timing", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_skip_threshold", "4MB",
						PGC_USERSET, PGC_S_SESSION);
		fake2_wal_consistency_checking = wal_consistency_checking;
		ok = ok && strcmp(default_table_access_method,
						  "session2_tableam") == 0;
		ok = ok && synchronize_seqscans;
		ok = ok && default_toast_compression == TOAST_PGLZ_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_NONE;
		ok = ok && wal_init_zero;
		ok = ok && wal_recycle;
		ok = ok && strcmp(wal_consistency_checking_string, "") == 0;
		ok = ok && fake2_wal_consistency_checking != NULL;
		ok = ok && fake2_wal_consistency_checking !=
			fake1_wal_consistency_checking;
		ok = ok && CommitDelay == 200;
		ok = ok && CommitSiblings == 9;
		ok = ok && !track_wal_io_timing;
		ok = ok && wal_skip_threshold == 4096;

		PgSetCurrentSession(&fake_session1);
		ok = ok && strcmp(default_table_access_method,
						  "session1_tableam") == 0;
		ok = ok && !synchronize_seqscans;
		ok = ok && default_toast_compression == TOAST_LZ4_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_PGLZ;
		ok = ok && !wal_init_zero;
		ok = ok && !wal_recycle;
		ok = ok && strcmp(wal_consistency_checking_string, "all") == 0;
		ok = ok && wal_consistency_checking ==
			fake1_wal_consistency_checking;
		ok = ok && CommitDelay == 100;
		ok = ok && CommitSiblings == 8;
		ok = ok && track_wal_io_timing;
		ok = ok && wal_skip_threshold == 3072;

		PgSetCurrentSession(&fake_session2);
		ok = ok && strcmp(default_table_access_method,
						  "session2_tableam") == 0;
		ok = ok && synchronize_seqscans;
		ok = ok && default_toast_compression == TOAST_PGLZ_COMPRESSION;
		ok = ok && wal_compression == WAL_COMPRESSION_NONE;
		ok = ok && wal_init_zero;
		ok = ok && wal_recycle;
		ok = ok && strcmp(wal_consistency_checking_string, "") == 0;
		ok = ok && wal_consistency_checking ==
			fake2_wal_consistency_checking;
		ok = ok && CommitDelay == 200;
		ok = ok && CommitSiblings == 9;
		ok = ok && !track_wal_io_timing;
		ok = ok && wal_skip_threshold == 4096;

		PgSetCurrentSession(saved_session);
		SetConfigOption("wal_skip_threshold", saved_wal_skip_threshold,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_wal_io_timing", saved_track_wal_io_timing,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_siblings", saved_commit_siblings,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("commit_delay", saved_commit_delay,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_consistency_checking",
						saved_wal_consistency_checking,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_recycle", saved_wal_recycle,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_init_zero", saved_wal_init_zero,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_compression", saved_wal_compression,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("synchronize_seqscans", saved_synchronize_seqscans,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("wal_skip_threshold", saved_wal_skip_threshold,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("track_wal_io_timing", saved_track_wal_io_timing,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("commit_siblings", saved_commit_siblings,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("commit_delay", saved_commit_delay,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_consistency_checking",
						saved_wal_consistency_checking,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_recycle", saved_wal_recycle,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_init_zero", saved_wal_init_zero,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("wal_compression", saved_wal_compression,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("synchronize_seqscans", saved_synchronize_seqscans,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session access/WAL GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_misc_guc_state_is_session_local);
Datum
test_session_misc_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_allow_system_table_mods;
	char	   *saved_dynamic_library_path;
	char	   *saved_extension_control_path;
	char	   *saved_local_preload_libraries;
	char	   *saved_max_stack_depth;
	char	   *saved_session_preload_libraries;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_allow_system_table_mods =
		pstrdup(GetConfigOption("allow_system_table_mods", false, false));
	saved_dynamic_library_path =
		pstrdup(GetConfigOption("dynamic_library_path", false, false));
	saved_extension_control_path =
		pstrdup(GetConfigOption("extension_control_path", false, false));
	saved_local_preload_libraries =
		pstrdup(GetConfigOption("local_preload_libraries", false, false));
	saved_max_stack_depth =
		pstrdup(GetConfigOption("max_stack_depth", false, false));
	saved_session_preload_libraries =
		pstrdup(GetConfigOption("session_preload_libraries", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !allowSystemTableMods;
		ok = ok && max_stack_depth == 100;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 100 * (ssize_t) 1024;
		ok = ok && session_preload_libraries_string == NULL;
		ok = ok && local_preload_libraries_string == NULL;
		ok = ok && Dynamic_library_path == NULL;
		ok = ok && strcmp(Extension_control_path, "$system") == 0;

		SetConfigOption("allow_system_table_mods", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("max_stack_depth", "101",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("session_preload_libraries", "auto_explain",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("local_preload_libraries", "pg_stat_statements",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("dynamic_library_path", "$libdir/plugins",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extension_control_path", "$system:/tmp/session1",
						PGC_SUSET, PGC_S_SESSION);
		ok = ok && allowSystemTableMods;
		ok = ok && max_stack_depth == 101;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 101 * (ssize_t) 1024;
		ok = ok && session_preload_libraries_string != NULL &&
			strcmp(session_preload_libraries_string, "auto_explain") == 0;
		ok = ok && local_preload_libraries_string != NULL &&
			strcmp(local_preload_libraries_string, "pg_stat_statements") == 0;
		ok = ok && Dynamic_library_path != NULL &&
			strcmp(Dynamic_library_path, "$libdir/plugins") == 0;
		ok = ok && strcmp(Extension_control_path,
						  "$system:/tmp/session1") == 0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !allowSystemTableMods;
		ok = ok && max_stack_depth == 100;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 100 * (ssize_t) 1024;
		ok = ok && strcmp(Extension_control_path, "$system") == 0;
		SetConfigOption("allow_system_table_mods", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("max_stack_depth", "102",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("session_preload_libraries", "pg_prewarm",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("local_preload_libraries", "pg_trgm",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("dynamic_library_path", "$libdir",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extension_control_path", "$system:/tmp/session2",
						PGC_SUSET, PGC_S_SESSION);
		ok = ok && !allowSystemTableMods;
		ok = ok && max_stack_depth == 102;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 102 * (ssize_t) 1024;
		ok = ok && session_preload_libraries_string != NULL &&
			strcmp(session_preload_libraries_string, "pg_prewarm") == 0;
		ok = ok && local_preload_libraries_string != NULL &&
			strcmp(local_preload_libraries_string, "pg_trgm") == 0;
		ok = ok && Dynamic_library_path != NULL &&
			strcmp(Dynamic_library_path, "$libdir") == 0;
		ok = ok && strcmp(Extension_control_path,
						  "$system:/tmp/session2") == 0;

		PgSetCurrentSession(&fake_session1);
		ok = ok && allowSystemTableMods;
		ok = ok && max_stack_depth == 101;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 101 * (ssize_t) 1024;
		ok = ok && session_preload_libraries_string != NULL &&
			strcmp(session_preload_libraries_string, "auto_explain") == 0;
		ok = ok && local_preload_libraries_string != NULL &&
			strcmp(local_preload_libraries_string, "pg_stat_statements") == 0;
		ok = ok && Dynamic_library_path != NULL &&
			strcmp(Dynamic_library_path, "$libdir/plugins") == 0;
		ok = ok && strcmp(Extension_control_path,
						  "$system:/tmp/session1") == 0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !allowSystemTableMods;
		ok = ok && max_stack_depth == 102;
		ok = ok && *PgCurrentMaxStackDepthBytesRef() == 102 * (ssize_t) 1024;
		ok = ok && session_preload_libraries_string != NULL &&
			strcmp(session_preload_libraries_string, "pg_prewarm") == 0;
		ok = ok && local_preload_libraries_string != NULL &&
			strcmp(local_preload_libraries_string, "pg_trgm") == 0;
		ok = ok && Dynamic_library_path != NULL &&
			strcmp(Dynamic_library_path, "$libdir") == 0;
		ok = ok && strcmp(Extension_control_path,
						  "$system:/tmp/session2") == 0;

		PgSetCurrentSession(saved_session);
		SetConfigOption("allow_system_table_mods",
						saved_allow_system_table_mods,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("dynamic_library_path", saved_dynamic_library_path,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extension_control_path",
						saved_extension_control_path,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("local_preload_libraries",
						saved_local_preload_libraries,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_stack_depth", saved_max_stack_depth,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("session_preload_libraries",
						saved_session_preload_libraries,
						PGC_SUSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("allow_system_table_mods",
						saved_allow_system_table_mods,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("dynamic_library_path", saved_dynamic_library_path,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("extension_control_path",
						saved_extension_control_path,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("local_preload_libraries",
						saved_local_preload_libraries,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_stack_depth", saved_max_stack_depth,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("session_preload_libraries",
						saved_session_preload_libraries,
						PGC_SUSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session miscellaneous GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_sort_guc_state_is_session_local);
Datum
test_session_sort_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_trace_sort;
#ifdef DEBUG_BOUNDED_SORT
	char	   *saved_optimize_bounded_sort;
#endif
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_trace_sort = pstrdup(GetConfigOption("trace_sort", false, false));
#ifdef DEBUG_BOUNDED_SORT
	saved_optimize_bounded_sort =
		pstrdup(GetConfigOption("optimize_bounded_sort", false, false));
#endif
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && optimize_bounded_sort;
#endif
		SetConfigOption("trace_sort", "on",
						PGC_USERSET, PGC_S_SESSION);
#ifdef DEBUG_BOUNDED_SORT
		SetConfigOption("optimize_bounded_sort", "off",
						PGC_USERSET, PGC_S_SESSION);
#endif
		ok = ok && trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && !optimize_bounded_sort;
#endif

		PgSetCurrentSession(&fake_session2);
		ok = ok && !trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && optimize_bounded_sort;
#endif
		SetConfigOption("trace_sort", "off",
						PGC_USERSET, PGC_S_SESSION);
#ifdef DEBUG_BOUNDED_SORT
		SetConfigOption("optimize_bounded_sort", "on",
						PGC_USERSET, PGC_S_SESSION);
#endif
		ok = ok && !trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && optimize_bounded_sort;
#endif

		PgSetCurrentSession(&fake_session1);
		ok = ok && trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && !optimize_bounded_sort;
#endif

		PgSetCurrentSession(&fake_session2);
		ok = ok && !trace_sort;
#ifdef DEBUG_BOUNDED_SORT
		ok = ok && optimize_bounded_sort;
#endif

		PgSetCurrentSession(saved_session);
		SetConfigOption("trace_sort", saved_trace_sort,
						PGC_USERSET, PGC_S_SESSION);
#ifdef DEBUG_BOUNDED_SORT
		SetConfigOption("optimize_bounded_sort",
						saved_optimize_bounded_sort,
						PGC_USERSET, PGC_S_SESSION);
#endif
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("trace_sort", saved_trace_sort,
						PGC_USERSET, PGC_S_SESSION);
#ifdef DEBUG_BOUNDED_SORT
		SetConfigOption("optimize_bounded_sort",
						saved_optimize_bounded_sort,
						PGC_USERSET, PGC_S_SESSION);
#endif
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session sort GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_jit_guc_state_is_session_local);
Datum
test_session_jit_guc_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_jit;
	char	   *saved_jit_dump_bitcode;
	char	   *saved_jit_expressions;
	char	   *saved_jit_tuple_deforming;
	char	   *saved_jit_above_cost;
	char	   *saved_jit_inline_above_cost;
	char	   *saved_jit_optimize_above_cost;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_jit = pstrdup(GetConfigOption("jit", false, false));
	saved_jit_dump_bitcode =
		pstrdup(GetConfigOption("jit_dump_bitcode", false, false));
	saved_jit_expressions =
		pstrdup(GetConfigOption("jit_expressions", false, false));
	saved_jit_tuple_deforming =
		pstrdup(GetConfigOption("jit_tuple_deforming", false, false));
	saved_jit_above_cost =
		pstrdup(GetConfigOption("jit_above_cost", false, false));
	saved_jit_inline_above_cost =
		pstrdup(GetConfigOption("jit_inline_above_cost", false, false));
	saved_jit_optimize_above_cost =
		pstrdup(GetConfigOption("jit_optimize_above_cost", false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && !jit_enabled;
		ok = ok && strcmp(jit_provider, "llvmjit") == 0;
		ok = ok && !jit_debugging_support;
		ok = ok && !jit_dump_bitcode;
		ok = ok && jit_expressions;
		ok = ok && !jit_profiling_support;
		ok = ok && jit_tuple_deforming;
		ok = ok && jit_above_cost == 100000.0;
		ok = ok && jit_inline_above_cost == 500000.0;
		ok = ok && jit_optimize_above_cost == 500000.0;
		SetConfigOption("jit", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_dump_bitcode", "on",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("jit_expressions", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_tuple_deforming", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_above_cost", "11",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_inline_above_cost", "12",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_optimize_above_cost", "13",
						PGC_USERSET, PGC_S_SESSION);
		jit_provider = "session1_jit_provider";
		jit_debugging_support = true;
		jit_profiling_support = true;
		ok = ok && jit_enabled;
		ok = ok && strcmp(jit_provider, "session1_jit_provider") == 0;
		ok = ok && jit_debugging_support;
		ok = ok && jit_dump_bitcode;
		ok = ok && !jit_expressions;
		ok = ok && jit_profiling_support;
		ok = ok && !jit_tuple_deforming;
		ok = ok && jit_above_cost == 11.0;
		ok = ok && jit_inline_above_cost == 12.0;
		ok = ok && jit_optimize_above_cost == 13.0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !jit_enabled;
		ok = ok && strcmp(jit_provider, "llvmjit") == 0;
		ok = ok && !jit_debugging_support;
		ok = ok && !jit_dump_bitcode;
		ok = ok && jit_expressions;
		ok = ok && !jit_profiling_support;
		ok = ok && jit_tuple_deforming;
		ok = ok && jit_above_cost == 100000.0;
		ok = ok && jit_inline_above_cost == 500000.0;
		ok = ok && jit_optimize_above_cost == 500000.0;
		SetConfigOption("jit", "off",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_dump_bitcode", "off",
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("jit_expressions", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_tuple_deforming", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_above_cost", "21",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_inline_above_cost", "22",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_optimize_above_cost", "23",
						PGC_USERSET, PGC_S_SESSION);
		jit_provider = "session2_jit_provider";
		jit_debugging_support = false;
		jit_profiling_support = false;
		ok = ok && !jit_enabled;
		ok = ok && strcmp(jit_provider, "session2_jit_provider") == 0;
		ok = ok && !jit_debugging_support;
		ok = ok && !jit_dump_bitcode;
		ok = ok && jit_expressions;
		ok = ok && !jit_profiling_support;
		ok = ok && jit_tuple_deforming;
		ok = ok && jit_above_cost == 21.0;
		ok = ok && jit_inline_above_cost == 22.0;
		ok = ok && jit_optimize_above_cost == 23.0;

		PgSetCurrentSession(&fake_session1);
		ok = ok && jit_enabled;
		ok = ok && strcmp(jit_provider, "session1_jit_provider") == 0;
		ok = ok && jit_debugging_support;
		ok = ok && jit_dump_bitcode;
		ok = ok && !jit_expressions;
		ok = ok && jit_profiling_support;
		ok = ok && !jit_tuple_deforming;
		ok = ok && jit_above_cost == 11.0;
		ok = ok && jit_inline_above_cost == 12.0;
		ok = ok && jit_optimize_above_cost == 13.0;

		PgSetCurrentSession(&fake_session2);
		ok = ok && !jit_enabled;
		ok = ok && strcmp(jit_provider, "session2_jit_provider") == 0;
		ok = ok && !jit_debugging_support;
		ok = ok && !jit_dump_bitcode;
		ok = ok && jit_expressions;
		ok = ok && !jit_profiling_support;
		ok = ok && jit_tuple_deforming;
		ok = ok && jit_above_cost == 21.0;
		ok = ok && jit_inline_above_cost == 22.0;
		ok = ok && jit_optimize_above_cost == 23.0;

		PgSetCurrentSession(saved_session);
		SetConfigOption("jit_optimize_above_cost",
						saved_jit_optimize_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_inline_above_cost",
						saved_jit_inline_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_above_cost", saved_jit_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_tuple_deforming", saved_jit_tuple_deforming,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_expressions", saved_jit_expressions,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_dump_bitcode", saved_jit_dump_bitcode,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("jit", saved_jit,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("jit_optimize_above_cost",
						saved_jit_optimize_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_inline_above_cost",
						saved_jit_inline_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_above_cost", saved_jit_above_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_tuple_deforming", saved_jit_tuple_deforming,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_expressions", saved_jit_expressions,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("jit_dump_bitcode", saved_jit_dump_bitcode,
						PGC_SUSET, PGC_S_SESSION);
		SetConfigOption("jit", saved_jit,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session JIT GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_query_memory_state_is_session_local);
Datum
test_session_query_memory_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_work_mem;
	char	   *saved_hash_mem_multiplier;
	char	   *saved_maintenance_work_mem;
	char	   *saved_max_parallel_maintenance_workers;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_work_mem = pstrdup(GetConfigOption("work_mem", false, false));
	saved_hash_mem_multiplier =
		pstrdup(GetConfigOption("hash_mem_multiplier", false, false));
	saved_maintenance_work_mem =
		pstrdup(GetConfigOption("maintenance_work_mem", false, false));
	saved_max_parallel_maintenance_workers =
		pstrdup(GetConfigOption("max_parallel_maintenance_workers",
								false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && work_mem == 4096;
		ok = ok && hash_mem_multiplier == 2.0;
		ok = ok && maintenance_work_mem == 65536;
		ok = ok && max_parallel_maintenance_workers == 2;
		SetConfigOption("work_mem", "8MB", PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("hash_mem_multiplier", "3",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("maintenance_work_mem", "128MB",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_maintenance_workers", "3",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && work_mem == 8192;
		ok = ok && hash_mem_multiplier == 3.0;
		ok = ok && maintenance_work_mem == 131072;
		ok = ok && max_parallel_maintenance_workers == 3;

		PgSetCurrentSession(&fake_session2);
		ok = ok && work_mem == 4096;
		ok = ok && hash_mem_multiplier == 2.0;
		ok = ok && maintenance_work_mem == 65536;
		ok = ok && max_parallel_maintenance_workers == 2;
		SetConfigOption("work_mem", "9MB", PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("hash_mem_multiplier", "4",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("maintenance_work_mem", "96MB",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_maintenance_workers", "1",
						PGC_USERSET, PGC_S_SESSION);
		ok = ok && work_mem == 9216;
		ok = ok && hash_mem_multiplier == 4.0;
		ok = ok && maintenance_work_mem == 98304;
		ok = ok && max_parallel_maintenance_workers == 1;

		PgSetCurrentSession(&fake_session1);
		ok = ok && work_mem == 8192;
		ok = ok && hash_mem_multiplier == 3.0;
		ok = ok && maintenance_work_mem == 131072;
		ok = ok && max_parallel_maintenance_workers == 3;

		PgSetCurrentSession(&fake_session2);
		ok = ok && work_mem == 9216;
		ok = ok && hash_mem_multiplier == 4.0;
		ok = ok && maintenance_work_mem == 98304;
		ok = ok && max_parallel_maintenance_workers == 1;

		PgSetCurrentSession(saved_session);
		SetConfigOption("work_mem", saved_work_mem, PGC_USERSET,
						PGC_S_SESSION);
		SetConfigOption("hash_mem_multiplier", saved_hash_mem_multiplier,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("maintenance_work_mem", saved_maintenance_work_mem,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_maintenance_workers",
						saved_max_parallel_maintenance_workers,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("work_mem", saved_work_mem, PGC_USERSET,
						PGC_S_SESSION);
		SetConfigOption("hash_mem_multiplier", saved_hash_mem_multiplier,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("maintenance_work_mem", saved_maintenance_work_mem,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_maintenance_workers",
						saved_max_parallel_maintenance_workers,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session query memory GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_planner_cost_state_is_session_local);
Datum
test_session_planner_cost_state_is_session_local(PG_FUNCTION_ARGS)
{
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_seq_page_cost;
	char	   *saved_random_page_cost;
	char	   *saved_cpu_tuple_cost;
	char	   *saved_cpu_index_tuple_cost;
	char	   *saved_cpu_operator_cost;
	char	   *saved_parallel_tuple_cost;
	char	   *saved_parallel_setup_cost;
	char	   *saved_recursive_worktable_factor;
	char	   *saved_effective_cache_size;
	char	   *saved_max_parallel_workers_per_gather;
	char	   *saved_debug_parallel_query;
	char	   *saved_parallel_leader_participation;
	bool		ok = true;

	saved_session = CurrentPgSession;
	saved_seq_page_cost = pstrdup(GetConfigOption("seq_page_cost",
												  false, false));
	saved_random_page_cost = pstrdup(GetConfigOption("random_page_cost",
													 false, false));
	saved_cpu_tuple_cost = pstrdup(GetConfigOption("cpu_tuple_cost",
												   false, false));
	saved_cpu_index_tuple_cost =
		pstrdup(GetConfigOption("cpu_index_tuple_cost", false, false));
	saved_cpu_operator_cost =
		pstrdup(GetConfigOption("cpu_operator_cost", false, false));
	saved_parallel_tuple_cost =
		pstrdup(GetConfigOption("parallel_tuple_cost", false, false));
	saved_parallel_setup_cost =
		pstrdup(GetConfigOption("parallel_setup_cost", false, false));
	saved_recursive_worktable_factor =
		pstrdup(GetConfigOption("recursive_worktable_factor", false, false));
	saved_effective_cache_size =
		pstrdup(GetConfigOption("effective_cache_size", false, false));
	saved_max_parallel_workers_per_gather =
		pstrdup(GetConfigOption("max_parallel_workers_per_gather",
								false, false));
	saved_debug_parallel_query =
		pstrdup(GetConfigOption("debug_parallel_query", false, false));
	saved_parallel_leader_participation =
		pstrdup(GetConfigOption("parallel_leader_participation",
								false, false));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		ok = ok && seq_page_cost == DEFAULT_SEQ_PAGE_COST;
		ok = ok && random_page_cost == DEFAULT_RANDOM_PAGE_COST;
		ok = ok && cpu_tuple_cost == DEFAULT_CPU_TUPLE_COST;
		ok = ok && cpu_index_tuple_cost == DEFAULT_CPU_INDEX_TUPLE_COST;
		ok = ok && cpu_operator_cost == DEFAULT_CPU_OPERATOR_COST;
		ok = ok && parallel_tuple_cost == DEFAULT_PARALLEL_TUPLE_COST;
		ok = ok && parallel_setup_cost == DEFAULT_PARALLEL_SETUP_COST;
		ok = ok && recursive_worktable_factor ==
			DEFAULT_RECURSIVE_WORKTABLE_FACTOR;
		ok = ok && effective_cache_size == DEFAULT_EFFECTIVE_CACHE_SIZE;
		ok = ok && disable_cost == 1.0e10;
		ok = ok && max_parallel_workers_per_gather == 2;
		ok = ok && debug_parallel_query == DEBUG_PARALLEL_OFF;
		ok = ok && parallel_leader_participation;
		SetConfigOption("seq_page_cost", "1.25",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("random_page_cost", "3.5",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_tuple_cost", "0.02",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_index_tuple_cost", "0.01",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_operator_cost", "0.005",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_tuple_cost", "0.2",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_setup_cost", "2000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("recursive_worktable_factor", "12",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("effective_cache_size", "4096",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_workers_per_gather", "3",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_parallel_query", "regress",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_leader_participation", "off",
						PGC_USERSET, PGC_S_SESSION);
		disable_cost = 42.0;
		ok = ok && seq_page_cost == 1.25;
		ok = ok && random_page_cost == 3.5;
		ok = ok && cpu_tuple_cost == 0.02;
		ok = ok && cpu_index_tuple_cost == 0.01;
		ok = ok && cpu_operator_cost == 0.005;
		ok = ok && parallel_tuple_cost == 0.2;
		ok = ok && parallel_setup_cost == 2000.0;
		ok = ok && recursive_worktable_factor == 12.0;
		ok = ok && effective_cache_size == 4096;
		ok = ok && disable_cost == 42.0;
		ok = ok && max_parallel_workers_per_gather == 3;
		ok = ok && debug_parallel_query == DEBUG_PARALLEL_REGRESS;
		ok = ok && !parallel_leader_participation;

		PgSetCurrentSession(&fake_session2);
		ok = ok && seq_page_cost == DEFAULT_SEQ_PAGE_COST;
		ok = ok && random_page_cost == DEFAULT_RANDOM_PAGE_COST;
		ok = ok && cpu_tuple_cost == DEFAULT_CPU_TUPLE_COST;
		ok = ok && cpu_index_tuple_cost == DEFAULT_CPU_INDEX_TUPLE_COST;
		ok = ok && cpu_operator_cost == DEFAULT_CPU_OPERATOR_COST;
		ok = ok && parallel_tuple_cost == DEFAULT_PARALLEL_TUPLE_COST;
		ok = ok && parallel_setup_cost == DEFAULT_PARALLEL_SETUP_COST;
		ok = ok && recursive_worktable_factor ==
			DEFAULT_RECURSIVE_WORKTABLE_FACTOR;
		ok = ok && effective_cache_size == DEFAULT_EFFECTIVE_CACHE_SIZE;
		ok = ok && disable_cost == 1.0e10;
		ok = ok && max_parallel_workers_per_gather == 2;
		ok = ok && debug_parallel_query == DEBUG_PARALLEL_OFF;
		ok = ok && parallel_leader_participation;
		SetConfigOption("seq_page_cost", "1.5",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("random_page_cost", "2.5",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_tuple_cost", "0.03",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_index_tuple_cost", "0.015",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_operator_cost", "0.0075",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_tuple_cost", "0.3",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_setup_cost", "3000",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("recursive_worktable_factor", "13",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("effective_cache_size", "8192",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_workers_per_gather", "1",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_parallel_query", "on",
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_leader_participation", "on",
						PGC_USERSET, PGC_S_SESSION);
		disable_cost = 84.0;
		ok = ok && seq_page_cost == 1.5;
		ok = ok && random_page_cost == 2.5;
		ok = ok && cpu_tuple_cost == 0.03;
		ok = ok && cpu_index_tuple_cost == 0.015;
		ok = ok && cpu_operator_cost == 0.0075;
		ok = ok && parallel_tuple_cost == 0.3;
		ok = ok && parallel_setup_cost == 3000.0;
		ok = ok && recursive_worktable_factor == 13.0;
		ok = ok && effective_cache_size == 8192;
		ok = ok && disable_cost == 84.0;
		ok = ok && max_parallel_workers_per_gather == 1;
		ok = ok && debug_parallel_query == DEBUG_PARALLEL_ON;
		ok = ok && parallel_leader_participation;

		PgSetCurrentSession(&fake_session1);
		ok = ok && seq_page_cost == 1.25;
		ok = ok && random_page_cost == 3.5;
		ok = ok && cpu_tuple_cost == 0.02;
		ok = ok && cpu_index_tuple_cost == 0.01;
		ok = ok && cpu_operator_cost == 0.005;
		ok = ok && parallel_tuple_cost == 0.2;
		ok = ok && parallel_setup_cost == 2000.0;
		ok = ok && recursive_worktable_factor == 12.0;
		ok = ok && effective_cache_size == 4096;
		ok = ok && disable_cost == 42.0;
		ok = ok && max_parallel_workers_per_gather == 3;
		ok = ok && debug_parallel_query == DEBUG_PARALLEL_REGRESS;
		ok = ok && !parallel_leader_participation;

		PgSetCurrentSession(saved_session);
		SetConfigOption("seq_page_cost", saved_seq_page_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("random_page_cost", saved_random_page_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_tuple_cost", saved_cpu_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_index_tuple_cost", saved_cpu_index_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_operator_cost", saved_cpu_operator_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_tuple_cost", saved_parallel_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_setup_cost", saved_parallel_setup_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("recursive_worktable_factor",
						saved_recursive_worktable_factor,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("effective_cache_size", saved_effective_cache_size,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_workers_per_gather",
						saved_max_parallel_workers_per_gather,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_parallel_query", saved_debug_parallel_query,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_leader_participation",
						saved_parallel_leader_participation,
						PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		SetConfigOption("seq_page_cost", saved_seq_page_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("random_page_cost", saved_random_page_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_tuple_cost", saved_cpu_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_index_tuple_cost", saved_cpu_index_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("cpu_operator_cost", saved_cpu_operator_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_tuple_cost", saved_parallel_tuple_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_setup_cost", saved_parallel_setup_cost,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("recursive_worktable_factor",
						saved_recursive_worktable_factor,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("effective_cache_size", saved_effective_cache_size,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("max_parallel_workers_per_gather",
						saved_max_parallel_workers_per_gather,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("debug_parallel_query", saved_debug_parallel_query,
						PGC_USERSET, PGC_S_SESSION);
		SetConfigOption("parallel_leader_participation",
						saved_parallel_leader_participation,
						PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session planner cost GUC state was not session-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_session_planner_method_state_is_session_local);
Datum
test_session_planner_method_state_is_session_local(PG_FUNCTION_ARGS)
{
	static const TestBoolGUCSetting bool_settings[] = {
		{"enable_async_append", PgCurrentEnableAsyncAppendRef, true,
			"off", false, "on", true},
		{"enable_bitmapscan", PgCurrentEnableBitmapscanRef, true,
			"off", false, "on", true},
		{"enable_distinct_reordering", PgCurrentEnableDistinctReorderingRef,
			true, "off", false, "on", true},
		{"enable_eager_aggregate", PgCurrentEnableEagerAggregateRef, true,
			"off", false, "on", true},
		{"enable_gathermerge", PgCurrentEnableGathermergeRef, true,
			"off", false, "on", true},
		{"enable_group_by_reordering", PgCurrentEnableGroupByReorderingRef,
			true, "off", false, "on", true},
		{"enable_hashagg", PgCurrentEnableHashaggRef, true,
			"off", false, "on", true},
		{"enable_hashjoin", PgCurrentEnableHashjoinRef, true,
			"off", false, "on", true},
		{"enable_incremental_sort", PgCurrentEnableIncrementalSortRef, true,
			"off", false, "on", true},
		{"enable_indexonlyscan", PgCurrentEnableIndexonlyscanRef, true,
			"off", false, "on", true},
		{"enable_indexscan", PgCurrentEnableIndexscanRef, true,
			"off", false, "on", true},
		{"enable_material", PgCurrentEnableMaterialRef, true,
			"off", false, "on", true},
		{"enable_memoize", PgCurrentEnableMemoizeRef, true,
			"off", false, "on", true},
		{"enable_mergejoin", PgCurrentEnableMergejoinRef, true,
			"off", false, "on", true},
		{"enable_nestloop", PgCurrentEnableNestloopRef, true,
			"off", false, "on", true},
		{"enable_parallel_append", PgCurrentEnableParallelAppendRef, true,
			"off", false, "on", true},
		{"enable_parallel_hash", PgCurrentEnableParallelHashRef, true,
			"off", false, "on", true},
		{"enable_partition_pruning", PgCurrentEnablePartitionPruningRef, true,
			"off", false, "on", true},
		{"enable_partitionwise_aggregate",
			PgCurrentEnablePartitionwiseAggregateRef, false,
			"on", true, "off", false},
		{"enable_partitionwise_join", PgCurrentEnablePartitionwiseJoinRef,
			false, "on", true, "off", false},
		{"enable_presorted_aggregate", PgCurrentEnablePresortedAggregateRef,
			true, "off", false, "on", true},
		{"enable_self_join_elimination",
			PgCurrentEnableSelfJoinEliminationRef, true,
			"off", false, "on", true},
		{"enable_seqscan", PgCurrentEnableSeqscanRef, true,
			"off", false, "on", true},
		{"enable_sort", PgCurrentEnableSortRef, true,
			"off", false, "on", true},
		{"enable_tidscan", PgCurrentEnableTidscanRef, true,
			"off", false, "on", true},
		{"geqo", PgCurrentEnableGeqoRef, true,
			"off", false, "on", true}
	};
	static const TestIntGUCSetting int_settings[] = {
		{"constraint_exclusion", PgCurrentConstraintExclusionRef,
			CONSTRAINT_EXCLUSION_PARTITION,
			"off", CONSTRAINT_EXCLUSION_OFF, "on", CONSTRAINT_EXCLUSION_ON},
		{"from_collapse_limit", PgCurrentFromCollapseLimitRef, 8,
			"4", 4, "5", 5},
		{"geqo_effort", PgCurrentGeqoEffortRef, DEFAULT_GEQO_EFFORT,
			"6", 6, "7", 7},
		{"geqo_generations", PgCurrentGeqoGenerationsRef, 0,
			"20", 20, "22", 22},
		{"geqo_pool_size", PgCurrentGeqoPoolSizeRef, 0,
			"10", 10, "12", 12},
		{"geqo_threshold", PgCurrentGeqoThresholdRef, 12,
			"13", 13, "14", 14},
		{"join_collapse_limit", PgCurrentJoinCollapseLimitRef, 8,
			"6", 6, "7", 7},
		{"min_parallel_index_scan_size",
			PgCurrentMinParallelIndexScanSizeRef,
			(512 * 1024) / BLCKSZ, "32", 32, "64", 64},
		{"min_parallel_table_scan_size",
			PgCurrentMinParallelTableScanSizeRef,
			(8 * 1024 * 1024) / BLCKSZ, "64", 64, "128", 128}
	};
	static const TestRealGUCSetting real_settings[] = {
		{"cursor_tuple_fraction", PgCurrentCursorTupleFractionRef,
			DEFAULT_CURSOR_TUPLE_FRACTION, "0.25", 0.25, "0.75", 0.75},
		{"geqo_seed", PgCurrentGeqoSeedRef, 0.0, "0.11", 0.11,
			"0.22", 0.22},
		{"geqo_selection_bias", PgCurrentGeqoSelectionBiasRef,
			DEFAULT_GEQO_SELECTION_BIAS, "1.75", 1.75, "2.0", 2.0},
		{"min_eager_agg_group_size", PgCurrentMinEagerAggGroupSizeRef,
			8.0, "5.5", 5.5, "9.5", 9.5}
	};
	PgSession  *saved_session;
	PgSession	fake_session1;
	PgSession	fake_session2;
	char	   *saved_bool_values[lengthof(bool_settings)];
	char	   *saved_int_values[lengthof(int_settings)];
	char	   *saved_real_values[lengthof(real_settings)];
	bool		ok = true;
	int			i;

	saved_session = CurrentPgSession;
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));
	test_copy_current_user_identity(&fake_session1);
	test_copy_current_user_identity(&fake_session2);

	for (i = 0; i < lengthof(bool_settings); i++)
		saved_bool_values[i] =
			pstrdup(GetConfigOption(bool_settings[i].name, false, false));
	for (i = 0; i < lengthof(int_settings); i++)
		saved_int_values[i] =
			pstrdup(GetConfigOption(int_settings[i].name, false, false));
	for (i = 0; i < lengthof(real_settings); i++)
		saved_real_values[i] =
			pstrdup(GetConfigOption(real_settings[i].name, false, false));

	PG_TRY();
	{
		PgSetCurrentSession(&fake_session1);
		for (i = 0; i < lengthof(bool_settings); i++)
		{
			ok = ok && *bool_settings[i].ref() ==
				bool_settings[i].default_value;
			SetConfigOption(bool_settings[i].name,
							bool_settings[i].session1_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *bool_settings[i].ref() ==
				bool_settings[i].session1_expected;
		}
		for (i = 0; i < lengthof(int_settings); i++)
		{
			ok = ok && *int_settings[i].ref() ==
				int_settings[i].default_value;
			SetConfigOption(int_settings[i].name,
							int_settings[i].session1_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *int_settings[i].ref() ==
				int_settings[i].session1_expected;
		}
		for (i = 0; i < lengthof(real_settings); i++)
		{
			ok = ok && *real_settings[i].ref() ==
				real_settings[i].default_value;
			SetConfigOption(real_settings[i].name,
							real_settings[i].session1_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *real_settings[i].ref() ==
				real_settings[i].session1_expected;
		}
		Geqo_planner_extension_id = 17;
		ok = ok && Geqo_planner_extension_id == 17;

		PgSetCurrentSession(&fake_session2);
		for (i = 0; i < lengthof(bool_settings); i++)
		{
			ok = ok && *bool_settings[i].ref() ==
				bool_settings[i].default_value;
			SetConfigOption(bool_settings[i].name,
							bool_settings[i].session2_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *bool_settings[i].ref() ==
				bool_settings[i].session2_expected;
		}
		for (i = 0; i < lengthof(int_settings); i++)
		{
			ok = ok && *int_settings[i].ref() ==
				int_settings[i].default_value;
			SetConfigOption(int_settings[i].name,
							int_settings[i].session2_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *int_settings[i].ref() ==
				int_settings[i].session2_expected;
		}
		for (i = 0; i < lengthof(real_settings); i++)
		{
			ok = ok && *real_settings[i].ref() ==
				real_settings[i].default_value;
			SetConfigOption(real_settings[i].name,
							real_settings[i].session2_value,
							PGC_USERSET, PGC_S_SESSION);
			ok = ok && *real_settings[i].ref() ==
				real_settings[i].session2_expected;
		}
		Geqo_planner_extension_id = 23;
		ok = ok && Geqo_planner_extension_id == 23;

		PgSetCurrentSession(&fake_session1);
		for (i = 0; i < lengthof(bool_settings); i++)
			ok = ok && *bool_settings[i].ref() ==
				bool_settings[i].session1_expected;
		for (i = 0; i < lengthof(int_settings); i++)
			ok = ok && *int_settings[i].ref() ==
				int_settings[i].session1_expected;
		for (i = 0; i < lengthof(real_settings); i++)
			ok = ok && *real_settings[i].ref() ==
				real_settings[i].session1_expected;
		ok = ok && Geqo_planner_extension_id == 17;

		PgSetCurrentSession(saved_session);
		for (i = 0; i < lengthof(bool_settings); i++)
			SetConfigOption(bool_settings[i].name, saved_bool_values[i],
							PGC_USERSET, PGC_S_SESSION);
		for (i = 0; i < lengthof(int_settings); i++)
			SetConfigOption(int_settings[i].name, saved_int_values[i],
							PGC_USERSET, PGC_S_SESSION);
		for (i = 0; i < lengthof(real_settings); i++)
			SetConfigOption(real_settings[i].name, saved_real_values[i],
							PGC_USERSET, PGC_S_SESSION);
	}
	PG_CATCH();
	{
		PgSetCurrentSession(saved_session);
		for (i = 0; i < lengthof(bool_settings); i++)
			SetConfigOption(bool_settings[i].name, saved_bool_values[i],
							PGC_USERSET, PGC_S_SESSION);
		for (i = 0; i < lengthof(int_settings); i++)
			SetConfigOption(int_settings[i].name, saved_int_values[i],
							PGC_USERSET, PGC_S_SESSION);
		for (i = 0; i < lengthof(real_settings); i++)
			SetConfigOption(real_settings[i].name, saved_real_values[i],
							PGC_USERSET, PGC_S_SESSION);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "session planner method GUC state was not session-local");

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
	sig_atomic_t saved_config_reload_pending;
	sig_atomic_t saved_shutdown_request_pending;
	sig_atomic_t saved_wakeup_stop_pending;
	sig_atomic_t saved_autovac_launcher_pending;
	sig_atomic_t saved_checkpointer_shutdown_xlog_pending;
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
	saved_config_reload_pending = ConfigReloadPending;
	saved_shutdown_request_pending = ShutdownRequestPending;
	saved_wakeup_stop_pending = WakeupStopPending;
	saved_autovac_launcher_pending = AutoVacLauncherPending;
	saved_checkpointer_shutdown_xlog_pending =
		CheckpointerShutdownXLOGPending;
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
		ConfigReloadPending = true;
		ShutdownRequestPending = true;
		WakeupStopPending = true;
		AutoVacLauncherPending = true;
		CheckpointerShutdownXLOGPending = true;

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
		ok = ok && !ConfigReloadPending;
		ok = ok && !ShutdownRequestPending;
		ok = ok && !WakeupStopPending;
		ok = ok && !AutoVacLauncherPending;
		ok = ok && !CheckpointerShutdownXLOGPending;

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
		ConfigReloadPending = false;
		ShutdownRequestPending = false;
		WakeupStopPending = false;
		AutoVacLauncherPending = false;
		CheckpointerShutdownXLOGPending = false;

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
		ok = ok && ConfigReloadPending;
		ok = ok && ShutdownRequestPending;
		ok = ok && WakeupStopPending;
		ok = ok && AutoVacLauncherPending;
		ok = ok && CheckpointerShutdownXLOGPending;

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
		ok = ok && !ConfigReloadPending;
		ok = ok && !ShutdownRequestPending;
		ok = ok && !WakeupStopPending;
		ok = ok && !AutoVacLauncherPending;
		ok = ok && !CheckpointerShutdownXLOGPending;

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
		ConfigReloadPending = saved_config_reload_pending;
		ShutdownRequestPending = saved_shutdown_request_pending;
		WakeupStopPending = saved_wakeup_stop_pending;
		AutoVacLauncherPending = saved_autovac_launcher_pending;
		CheckpointerShutdownXLOGPending =
			saved_checkpointer_shutdown_xlog_pending;
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
		ConfigReloadPending = saved_config_reload_pending;
		ShutdownRequestPending = saved_shutdown_request_pending;
		WakeupStopPending = saved_wakeup_stop_pending;
		AutoVacLauncherPending = saved_autovac_launcher_pending;
		CheckpointerShutdownXLOGPending =
			saved_checkpointer_shutdown_xlog_pending;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "pending interrupt state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_exit_state_is_backend_local);
static void
test_backend_runtime_exit_callback(int code, Datum arg)
{
}

Datum
test_backend_exit_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	bool		saved_proc_exit_flag;
	bool		saved_shmem_exit_flag;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	saved_proc_exit_flag = proc_exit_inprogress;
	saved_shmem_exit_flag = shmem_exit_inprogress;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));

	PG_TRY();
	{
		CurrentPgBackend = &fake_backend1;
		proc_exit_inprogress = true;
		shmem_exit_inprogress = true;
		fake_backend1.exit_state.on_proc_exit_index = 1;
		fake_backend1.exit_state.on_shmem_exit_index = 1;
		fake_backend1.exit_state.before_shmem_exit_index = 1;
		fake_backend1.exit_state.proc_exit_active = true;
		fake_backend1.exit_state.shmem_exit_active = true;
		fake_backend1.exit_state.on_proc_exit_list[0].function =
			test_backend_runtime_exit_callback;
		fake_backend1.exit_state.on_proc_exit_list[0].arg =
			PointerGetDatum(&fake_backend1);

		CurrentPgBackend = &fake_backend2;
		ok = ok && !proc_exit_inprogress;
		ok = ok && !shmem_exit_inprogress;
		ok = ok && !PgBackendExitInProgress();
		ok = ok && !PgBackendShmemExitInProgress();

		proc_exit_inprogress = false;
		shmem_exit_inprogress = false;

		CurrentPgBackend = &fake_backend1;
		ok = ok && proc_exit_inprogress;
		ok = ok && shmem_exit_inprogress;
		ok = ok && PgBackendExitInProgress();
		ok = ok && PgBackendShmemExitInProgress();

		PgBackendInitializeExitState(&fake_backend1.exit_state);
		ok = ok && fake_backend1.exit_state.on_proc_exit_index == 0;
		ok = ok && fake_backend1.exit_state.on_shmem_exit_index == 0;
		ok = ok && fake_backend1.exit_state.before_shmem_exit_index == 0;
		ok = ok && !fake_backend1.exit_state.proc_exit_active;
		ok = ok && !fake_backend1.exit_state.shmem_exit_active;
		ok = ok && fake_backend1.exit_state.on_proc_exit_list[0].function == NULL;
		ok = ok && fake_backend1.exit_state.on_proc_exit_list[0].arg == 0;

		CurrentPgBackend = &fake_backend2;
		ok = ok && !proc_exit_inprogress;
		ok = ok && !shmem_exit_inprogress;

		CurrentPgBackend = saved_backend;
		proc_exit_inprogress = saved_proc_exit_flag;
		shmem_exit_inprogress = saved_shmem_exit_flag;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		proc_exit_inprogress = saved_proc_exit_flag;
		shmem_exit_inprogress = saved_shmem_exit_flag;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend exit state was not backend-local");

	PG_RETURN_BOOL(true);
}

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
		CurrentPgBackend = &fake_backend1;
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
		pgStatLocal.snapshot.mode = PGSTAT_FETCH_CONSISTENCY_CACHE;
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

		CurrentPgBackend = &fake_backend2;
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
		ok = ok && pgStatLocal.snapshot.mode == PGSTAT_FETCH_CONSISTENCY_NONE;
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
		pgStatLocal.snapshot.mode = PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
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

		CurrentPgBackend = &fake_backend1;
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
		ok = ok && pgStatLocal.snapshot.mode == PGSTAT_FETCH_CONSISTENCY_CACHE;
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

		CurrentPgBackend = &fake_backend2;
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
		ok = ok && pgStatLocal.snapshot.mode == PGSTAT_FETCH_CONSISTENCY_SNAPSHOT;
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

		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
		*PgCurrentLocalBackendStatusTableRef() = &fake_status1;
		*PgCurrentLocalNumBackendsRef() = 11;
		*PgCurrentBackendStatusSnapContextRef() = (MemoryContext) &fake_backend1;

		CurrentPgBackend = &fake_backend2;
		ok = ok && *PgCurrentLocalBackendStatusTableRef() == NULL;
		ok = ok && *PgCurrentLocalNumBackendsRef() == 0;
		ok = ok && *PgCurrentBackendStatusSnapContextRef() == NULL;

		*PgCurrentLocalBackendStatusTableRef() = &fake_status2;
		*PgCurrentLocalNumBackendsRef() = 22;
		*PgCurrentBackendStatusSnapContextRef() = (MemoryContext) &fake_backend2;

		CurrentPgBackend = &fake_backend1;
		ok = ok && *PgCurrentLocalBackendStatusTableRef() == &fake_status1;
		ok = ok && *PgCurrentLocalNumBackendsRef() == 11;
		ok = ok && *PgCurrentBackendStatusSnapContextRef() == (MemoryContext) &fake_backend1;

		CurrentPgBackend = &fake_backend2;
		ok = ok && *PgCurrentLocalBackendStatusTableRef() == &fake_status2;
		ok = ok && *PgCurrentLocalNumBackendsRef() == 22;
		ok = ok && *PgCurrentBackendStatusSnapContextRef() == (MemoryContext) &fake_backend2;

		CurrentPgBackend = saved_backend;
		*PgCurrentLocalBackendStatusTableRef() = saved_status_table;
		*PgCurrentLocalNumBackendsRef() = saved_num_backends;
		*PgCurrentBackendStatusSnapContextRef() = saved_status_context;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
		PgCurrentAllocSetContextFreeLists()[0].num_free = 11;
		PgCurrentAllocSetContextFreeLists()[0].first_free =
			(struct AllocSetContext *) &fake_backend1;
		PgCurrentAllocSetContextFreeLists()[1].num_free = 12;
		PgCurrentAllocSetContextFreeLists()[1].first_free =
			(struct AllocSetContext *) &fake_backend1;
		*PgCurrentLogMemoryContextInProgressRef() = true;

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
		ok = ok && PgCurrentAllocSetContextFreeLists()[0].num_free == 11;
		ok = ok && PgCurrentAllocSetContextFreeLists()[0].first_free ==
			(struct AllocSetContext *) &fake_backend1;
		ok = ok && PgCurrentAllocSetContextFreeLists()[1].num_free == 12;
		ok = ok && PgCurrentAllocSetContextFreeLists()[1].first_free ==
			(struct AllocSetContext *) &fake_backend1;
		ok = ok && *PgCurrentLogMemoryContextInProgressRef();

		CurrentPgBackend = &fake_backend2;
		ok = ok && PgCurrentAllocSetContextFreeLists()[0].num_free == 21;
		ok = ok && PgCurrentAllocSetContextFreeLists()[0].first_free ==
			(struct AllocSetContext *) &fake_backend2;
		ok = ok && PgCurrentAllocSetContextFreeLists()[1].num_free == 22;
		ok = ok && PgCurrentAllocSetContextFreeLists()[1].first_free ==
			(struct AllocSetContext *) &fake_backend2;
		ok = ok && *PgCurrentLogMemoryContextInProgressRef();

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
	HASHCTL		hash_ctl;
	bool		ok = true;

	MemSet(&fake_backend, 0, sizeof(fake_backend));
	utility = &fake_backend.utility;
	walsender = &fake_backend.walsender;
	replication = &fake_backend.replication;
	logical_replication = &fake_backend.logical_replication;
	xlog = &fake_backend.xlog;
	maintenance_worker = &fake_backend.maintenance_worker;
	autovacuum = &fake_backend.autovacuum;
	aio = &fake_backend.aio;
	replication->walreceiver_recv_file = -1;
	xlog->open_log_file = -1;

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

	fake_backend.memory_manager.log_memory_context_in_progress = true;

	utility->notify_interrupt_pending = true;
	utility->seq_scan_tables[0] = (HTAB *) &fake_backend;
	utility->seq_scan_tables[1] = (HTAB *) &fake_backend;
	utility->seq_scan_levels[0] = 1;
	utility->seq_scan_levels[1] = 2;
	utility->num_seq_scans = 2;
	RegisterResourceReleaseCallback(test_backend_runtime_resource_release_callback,
									NULL);
	utility->injection_point_cache =
		hash_create("test injection point cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);
	utility->dch_cache[0] = palloc(8);
	utility->dch_cache[1] = palloc(8);
	utility->n_dch_cache = 2;
	utility->dch_counter = 11;
	utility->num_cache[0] = palloc(8);
	utility->num_cache[1] = palloc(8);
	utility->n_num_cache = 2;
	utility->num_counter = 12;
	utility->libxml_context =
		AllocSetContextCreate(TopMemoryContext,
							  "test libxml context",
							  ALLOCSET_SMALL_SIZES);
	utility->missing_attr_cache =
		hash_create("test missing attr cache", 8, &hash_ctl,
					HASH_ELEM | HASH_BLOBS);

	PgBackendResetClosedState(&fake_backend);

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
	ok = ok && autovacuum->autovac_mem_cxt == NULL;
	ok = ok && autovacuum->database_list_cxt == NULL;
	ok = ok && autovacuum->avl_dbase_array == NULL;
	ok = ok && autovacuum->my_worker_info == NULL;
	ok = ok && dlist_is_empty(&autovacuum->database_list);
	ok = ok && aio->my_backend == NULL;
	ok = ok && aio->my_io_worker_id == -1;
	ok = ok && aio->my_uring_context == NULL;
	ok = ok && !fake_backend.memory_manager.log_memory_context_in_progress;
	ok = ok && utility->notify_interrupt_pending;
	ok = ok && utility->seq_scan_tables[0] == NULL;
	ok = ok && utility->seq_scan_tables[1] == NULL;
	ok = ok && utility->seq_scan_levels[0] == 0;
	ok = ok && utility->seq_scan_levels[1] == 0;
	ok = ok && utility->num_seq_scans == 0;
	ok = ok && utility->resource_release_callbacks == NULL;
	ok = ok && utility->injection_point_cache == NULL;
	ok = ok && utility->dch_cache[0] == NULL;
	ok = ok && utility->dch_cache[1] == NULL;
	ok = ok && utility->n_dch_cache == 0;
	ok = ok && utility->dch_counter == 0;
	ok = ok && utility->num_cache[0] == NULL;
	ok = ok && utility->num_cache[1] == NULL;
	ok = ok && utility->n_num_cache == 0;
	ok = ok && utility->num_counter == 0;
	ok = ok && utility->libxml_context == NULL;
	ok = ok && utility->missing_attr_cache == NULL;

	if (!ok)
		elog(ERROR, "closed backend runtime state was not reset");

	PG_RETURN_BOOL(true);
}

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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
		pgBufferUsage.shared_blks_hit = 11;
		save_pgBufferUsage.shared_blks_hit = 12;
		pgWalUsage.wal_records = 13;
		save_pgWalUsage.wal_records = 14;

		CurrentPgBackend = &fake_backend2;
		ok = ok && pgBufferUsage.shared_blks_hit == 0;
		ok = ok && save_pgBufferUsage.shared_blks_hit == 0;
		ok = ok && pgWalUsage.wal_records == 0;
		ok = ok && save_pgWalUsage.wal_records == 0;

		pgBufferUsage.shared_blks_hit = 21;
		save_pgBufferUsage.shared_blks_hit = 22;
		pgWalUsage.wal_records = 23;
		save_pgWalUsage.wal_records = 24;

		CurrentPgBackend = &fake_backend1;
		ok = ok && pgBufferUsage.shared_blks_hit == 11;
		ok = ok && save_pgBufferUsage.shared_blks_hit == 12;
		ok = ok && pgWalUsage.wal_records == 13;
		ok = ok && save_pgWalUsage.wal_records == 14;

		CurrentPgBackend = &fake_backend2;
		ok = ok && pgBufferUsage.shared_blks_hit == 21;
		ok = ok && save_pgBufferUsage.shared_blks_hit == 22;
		ok = ok && pgWalUsage.wal_records == 23;
		ok = ok && save_pgWalUsage.wal_records == 24;

		CurrentPgBackend = saved_backend;
		pgBufferUsage = saved_buffer_usage;
		save_pgBufferUsage = saved_saved_buffer_usage;
		pgWalUsage = saved_wal_usage;
		save_pgWalUsage = saved_saved_wal_usage;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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
		*PgCurrentBlockingAutovacuumProcRef() = &fake_backend1;
		*PgCurrentLocalPredicateLockHashRef() = (HTAB *) &fake_backend1;
		*PgCurrentMySerializableXactRef() = &fake_backend1;
		*PgCurrentMyXactDidWriteRef() = true;
		*PgCurrentSavedSerializableXactRef() = &fake_backend1;

		CurrentPgBackend = &fake_backend2;
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
		*PgCurrentBlockingAutovacuumProcRef() = &fake_backend2;
		*PgCurrentLocalPredicateLockHashRef() = (HTAB *) &fake_backend2;
		*PgCurrentMySerializableXactRef() = &fake_backend2;
		*PgCurrentMyXactDidWriteRef() = false;
		*PgCurrentSavedSerializableXactRef() = &fake_backend2;

		CurrentPgBackend = &fake_backend1;
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
		ok = ok && *PgCurrentBlockingAutovacuumProcRef() == &fake_backend1;
		ok = ok && *PgCurrentLocalPredicateLockHashRef() == (HTAB *) &fake_backend1;
		ok = ok && *PgCurrentMySerializableXactRef() == &fake_backend1;
		ok = ok && *PgCurrentMyXactDidWriteRef();
		ok = ok && *PgCurrentSavedSerializableXactRef() == &fake_backend1;

		CurrentPgBackend = &fake_backend2;
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
		ok = ok && *PgCurrentBlockingAutovacuumProcRef() == &fake_backend2;
		ok = ok && *PgCurrentLocalPredicateLockHashRef() == (HTAB *) &fake_backend2;
		ok = ok && *PgCurrentMySerializableXactRef() == &fake_backend2;
		ok = ok && !*PgCurrentMyXactDidWriteRef();
		ok = ok && *PgCurrentSavedSerializableXactRef() == &fake_backend2;

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
		ok = ok && *PgCurrentMyWaitEventInfoRef() ==
			PgCurrentLocalWaitEventInfoRef();
		pgstat_report_wait_start(0x01000011);
		ok = ok && *PgCurrentLocalWaitEventInfoRef() == 0x01000011;
		pgstat_report_wait_end();
		ok = ok && *PgCurrentLocalWaitEventInfoRef() == 0;
		my_wait_event_info = &external_wait_event1;
		pgstat_report_wait_start(0x02000022);
		ok = ok && external_wait_event1 == 0x02000022;

		CurrentPgBackend = &fake_backend2;
		ok = ok && *PgCurrentMyWaitEventInfoRef() ==
			PgCurrentLocalWaitEventInfoRef();
		ok = ok && external_wait_event2 == 0;
		my_wait_event_info = &external_wait_event2;
		pgstat_report_wait_start(0x03000033);
		ok = ok && external_wait_event2 == 0x03000033;

		CurrentPgBackend = &fake_backend1;
		ok = ok && *PgCurrentMyWaitEventInfoRef() == &external_wait_event1;
		ok = ok && external_wait_event1 == 0x02000022;
		pgstat_report_wait_end();
		ok = ok && external_wait_event1 == 0;

		CurrentPgBackend = &fake_backend2;
		ok = ok && *PgCurrentMyWaitEventInfoRef() == &external_wait_event2;
		ok = ok && external_wait_event2 == 0x03000033;
		pgstat_reset_wait_event_storage();
		ok = ok && *PgCurrentMyWaitEventInfoRef() ==
			PgCurrentLocalWaitEventInfoRef();

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend wait state was not backend-local");

	PG_RETURN_BOOL(true);
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
		worker1 = PgCurrentMaintenanceWorkerState();
		worker1->arch_module_errdetail_string = (char *) &fake_backend1;
		worker1->pgarch_last_sigterm_time = 101;
		worker1->archive_callbacks = (const struct ArchiveModuleCallbacks *) &fake_backend1;
		worker1->archive_module_state = (struct ArchiveModuleState *) &fake_backend1;
		worker1->archive_context = (MemoryContext) &fake_backend1;
		worker1->loaded_archive_library = (char *) &fake_backend1;
		worker1->pgarch_files = (struct arch_files_state *) &fake_backend1;
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
		worker1->datachecksum_operation = DISABLE_DATACHECKSUMS;

		CurrentPgBackend = &fake_backend2;
		worker2 = PgCurrentMaintenanceWorkerState();
		ok = ok && worker2->arch_module_errdetail_string == NULL;
		ok = ok && worker2->pgarch_last_sigterm_time == 0;
		ok = ok && worker2->archive_callbacks == NULL;
		ok = ok && worker2->archive_module_state == NULL;
		ok = ok && worker2->archive_context == NULL;
		ok = ok && worker2->loaded_archive_library == NULL;
		ok = ok && worker2->pgarch_files == NULL;
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
		ok = ok && worker2->datachecksum_operation == ENABLE_DATACHECKSUMS;

		worker2->arch_module_errdetail_string = (char *) &fake_backend2;
		worker2->pgarch_last_sigterm_time = 201;
		worker2->archive_callbacks = (const struct ArchiveModuleCallbacks *) &fake_backend2;
		worker2->archive_module_state = (struct ArchiveModuleState *) &fake_backend2;
		worker2->archive_context = (MemoryContext) &fake_backend2;
		worker2->loaded_archive_library = (char *) &fake_backend2;
		worker2->pgarch_files = (struct arch_files_state *) &fake_backend2;
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
		worker2->datachecksum_operation = DISABLE_DATACHECKSUMS;

		CurrentPgBackend = &fake_backend1;
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
		ok = ok && worker1->datachecksum_operation == DISABLE_DATACHECKSUMS;

		CurrentPgBackend = &fake_backend2;
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
		ok = ok && worker2->datachecksum_operation == DISABLE_DATACHECKSUMS;

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = &fake_backend1;
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

		CurrentPgBackend = &fake_backend2;
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

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
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
		CurrentPgBackend = &fake_backend1;
		aio1 = PgCurrentAioState();
		pgaio_my_backend = (PgAioBackend *) &fake_backend1;
		aio1->my_io_worker_id = 101;
		aio1->my_uring_context = (struct PgAioUringContext *) &fake_backend1;

		CurrentPgBackend = &fake_backend2;
		aio2 = PgCurrentAioState();
		ok = ok && pgaio_my_backend == NULL;
		ok = ok && aio2->my_io_worker_id == -1;
		ok = ok && aio2->my_uring_context == NULL;

		pgaio_my_backend = (PgAioBackend *) &fake_backend2;
		aio2->my_io_worker_id = 201;
		aio2->my_uring_context = (struct PgAioUringContext *) &fake_backend2;

		CurrentPgBackend = &fake_backend1;
		aio1 = PgCurrentAioState();
		ok = ok && pgaio_my_backend == (PgAioBackend *) &fake_backend1;
		ok = ok && aio1->my_io_worker_id == 101;
		ok = ok && aio1->my_uring_context ==
			(struct PgAioUringContext *) &fake_backend1;

		CurrentPgBackend = &fake_backend2;
		aio2 = PgCurrentAioState();
		ok = ok && pgaio_my_backend == (PgAioBackend *) &fake_backend2;
		ok = ok && aio2->my_io_worker_id == 201;
		ok = ok && aio2->my_uring_context ==
			(struct PgAioUringContext *) &fake_backend2;

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend AIO state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_pmchild_thread_backend_signal_api);
Datum
test_pmchild_thread_backend_signal_api(PG_FUNCTION_ARGS)
{
	PgRuntime	fake_runtime;
	PgBackend	fake_backend;
	PMChild		fake_pmchild;
	PgThread	fake_thread;
	Latch		fake_latch;
	PgBackendInterruptMask pending;
	int			exitstatus;
	pid_t		exit_signal_pid;
	Size		top_memory_allocated;
	bool		ok = true;

	MemSet(&fake_runtime, 0, sizeof(fake_runtime));
	MemSet(&fake_backend, 0, sizeof(fake_backend));
	MemSet(&fake_pmchild, 0, sizeof(fake_pmchild));
	MemSet(&fake_thread, 0, sizeof(fake_thread));

	fake_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
	fake_backend.id = 12345;
	fake_backend.runtime = &fake_runtime;
	PgBackendInitializeInterrupts(&fake_backend);
	fake_pmchild.signal_pid = 54321;
	fake_pmchild.thread_exitstatus = 99;
	fake_pmchild.thread_exit_top_memory_allocated = 16384;
	fake_pmchild.carrier_kind = PM_CHILD_CARRIER_THREAD;
	InitLatch(&fake_latch);

	PostmasterChildSetThread(&fake_pmchild, &fake_thread);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && !PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											   &top_memory_allocated,
											   &exit_signal_pid);

	PostmasterChildSetThreadBackend(&fake_pmchild, &fake_backend);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 12345;
	ok = ok && PostmasterChildRaiseThreadInterrupt(&fake_pmchild,
												   PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	pending = PgBackendConsumeInterrupts(&fake_backend);
	ok = ok && (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL));

	PostmasterChildPublishThreadExit(&fake_pmchild, 17, 8192, &fake_latch);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											  &top_memory_allocated,
											  &exit_signal_pid);
	ok = ok && exitstatus == 17;
	ok = ok && exit_signal_pid == 12345;
	ok = ok && top_memory_allocated == 8192;
	ok = ok && !PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											   &top_memory_allocated,
											   &exit_signal_pid);
	PostmasterChildRetryThreadExit(&fake_pmchild);
	ok = ok && PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											  &top_memory_allocated,
											  &exit_signal_pid);
	ok = ok && exitstatus == 17;
	ok = ok && exit_signal_pid == 12345;
	ok = ok && top_memory_allocated == 8192;
	ok = ok && !PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											   &top_memory_allocated,
											   &exit_signal_pid);
	ok = ok && !PostmasterChildRaiseThreadInterrupt(&fake_pmchild,
													PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	ok = ok && !PostmasterChildWakeThreadBackend(&fake_pmchild);

	PostmasterChildSetThread(&fake_pmchild, &fake_thread);
	PostmasterChildSetThreadBackend(&fake_pmchild, &fake_backend);
	PostmasterChildDetachThreadBackend(&fake_pmchild);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && !PostmasterChildRaiseThreadInterrupt(&fake_pmchild,
													PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	PostmasterChildPublishThreadExit(&fake_pmchild, 23, 4096, &fake_latch);
	ok = ok && PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											  &top_memory_allocated,
											  &exit_signal_pid);
	ok = ok && exitstatus == 23;
	ok = ok && exit_signal_pid == 12345;
	ok = ok && top_memory_allocated == 4096;

	if (!ok)
		elog(ERROR, "PMChild thread-backend signal API failed");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_pmchild_thread_backend_publication_race);
Datum
test_pmchild_thread_backend_publication_race(PG_FUNCTION_ARGS)
{
#define TEST_PMCHILD_READER_THREADS 4
#define TEST_PMCHILD_PUBLICATION_CYCLES 2000
	PgRuntime	fake_runtime;
	PgBackend	fake_backend;
	PMChild		fake_pmchild;
	PgThread	fake_pmthread;
	PgThread	reader_threads[TEST_PMCHILD_READER_THREADS];
	Latch		fake_latch;
	TestPMChildThreadBackendRace race_state;
	int			created_threads = 0;
	bool		ok = true;

	MemSet(&fake_runtime, 0, sizeof(fake_runtime));
	MemSet(&fake_backend, 0, sizeof(fake_backend));
	MemSet(&fake_pmchild, 0, sizeof(fake_pmchild));
	MemSet(&fake_pmthread, 0, sizeof(fake_pmthread));
	MemSet(&fake_latch, 0, sizeof(fake_latch));
	MemSet(&race_state, 0, sizeof(race_state));

	fake_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
	fake_backend.id = 12345;
	fake_backend.runtime = &fake_runtime;
	PgBackendInitializeInterrupts(&fake_backend);
	fake_pmchild.carrier_kind = PM_CHILD_CARRIER_THREAD;
	InitLatch(&fake_latch);

	race_state.pmchild = &fake_pmchild;
	pg_atomic_init_u32(&race_state.start, 0);
	pg_atomic_init_u32(&race_state.stop, 0);
	pg_atomic_init_u32(&race_state.ready_count, 0);
	pg_atomic_init_u32(&race_state.attempts, 0);
	pg_atomic_init_u32(&race_state.hits, 0);
	pg_atomic_init_u32(&race_state.saw_live_signal_pid, 0);

	PG_TRY();
	{
		int			exitstatus;
		pid_t		exit_signal_pid;
		Size		top_memory_allocated;

		PostmasterChildSetThread(&fake_pmchild, &fake_pmthread);

		for (int i = 0; i < TEST_PMCHILD_READER_THREADS; i++)
		{
			int			rc;

			rc = pg_thread_create(&reader_threads[i], "pmchild reader",
								  test_pmchild_thread_backend_reader_routine,
								  &race_state);
			if (rc != 0)
			{
				errno = rc;
				elog(ERROR, "pg_thread_create failed: %m");
			}
			created_threads++;
		}

		while (pg_atomic_read_u32(&race_state.ready_count) <
			   TEST_PMCHILD_READER_THREADS)
			pg_usleep(1000L);
		pg_atomic_write_u32(&race_state.start, 1);

		for (int i = 0; i < TEST_PMCHILD_PUBLICATION_CYCLES; i++)
		{
			PostmasterChildSetThreadBackend(&fake_pmchild, &fake_backend);
			/* Make the reader-side observation deterministic on fast runs. */
			if (pg_atomic_read_u32(&race_state.hits) == 0)
			{
				for (int spins = 0;
					 spins < 1000 &&
					 pg_atomic_read_u32(&race_state.hits) == 0;
					 spins++)
					pg_usleep(100L);
			}
			PostmasterChildDetachThreadBackend(&fake_pmchild);
			PostmasterChildPublishThreadExit(&fake_pmchild, i,
											 (Size) i * 16, &fake_latch);
			ok = ok && PostmasterChildHasExitedThread(&fake_pmchild,
													  &exitstatus,
													  &top_memory_allocated,
													  &exit_signal_pid);
			ok = ok && exitstatus == i;
			ok = ok && top_memory_allocated == (Size) i * 16;
			ok = ok && exit_signal_pid == 12345;
			(void) PgBackendConsumeInterrupts(&fake_backend);
			PostmasterChildSetThread(&fake_pmchild, &fake_pmthread);
		}

		pg_atomic_write_u32(&race_state.stop, 1);
		for (int i = 0; i < created_threads; i++)
		{
			int			rc;

			rc = pg_thread_join(&reader_threads[i]);
			if (rc != 0)
			{
				errno = rc;
				elog(ERROR, "pg_thread_join failed: %m");
			}
		}
		created_threads = 0;
	}
	PG_CATCH();
	{
		pg_atomic_write_u32(&race_state.stop, 1);
		for (int i = 0; i < created_threads; i++)
			(void) pg_thread_join(&reader_threads[i]);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ok = ok && pg_atomic_read_u32(&race_state.attempts) > 0;
	ok = ok && pg_atomic_read_u32(&race_state.hits) > 0;
	ok = ok && pg_atomic_read_u32(&race_state.saw_live_signal_pid) > 0;
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && !PostmasterChildRaiseThreadInterrupt(&fake_pmchild,
													PG_BACKEND_INTERRUPT_QUERY_CANCEL);

	if (!ok)
		elog(ERROR, "PMChild thread-backend publication race failed");

	PG_RETURN_BOOL(true);
#undef TEST_PMCHILD_READER_THREADS
#undef TEST_PMCHILD_PUBLICATION_CYCLES
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
	ProcNumber	saved_proc_number;
	ProcNumber	saved_parallel_leader_proc_number;
	PgBackendStatus *saved_beentry;
	PgBackendStatus fake_beentry1;
	PgBackendStatus fake_beentry2;
	BackgroundWorker *saved_bgworker_entry;
	BackgroundWorker fake_bgworker1;
	BackgroundWorker fake_bgworker2;
	pg_time_t	saved_start_time;
	TimestampTz saved_start_timestamp;
	int			saved_pm_child_slot;
	char		saved_output_file_name[MAXPGPATH];
	BackendType saved_backend_type;
	ProcessingMode saved_mode;
	bool		saved_ignore_system_indexes;
	pg_prng_state saved_global_prng_state;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	saved_exit_on_any_error = ExitOnAnyError;
	saved_proc_pid = MyProcPid;
	saved_proc_number = MyProcNumber;
	saved_parallel_leader_proc_number = ParallelLeaderProcNumber;
	saved_beentry = MyBEEntry;
	saved_bgworker_entry = MyBgworkerEntry;
	saved_start_time = MyStartTime;
	saved_start_timestamp = MyStartTimestamp;
	saved_latch = MyLatch;
	saved_pm_child_slot = MyPMChildSlot;
	strlcpy(saved_output_file_name, OutputFileName, sizeof(saved_output_file_name));
	saved_backend_type = MyBackendType;
	saved_mode = Mode;
	saved_ignore_system_indexes = IgnoreSystemIndexes;
	saved_global_prng_state = pg_global_prng_state;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	fake_backend1.my_proc_number = INVALID_PROC_NUMBER;
	fake_backend1.parallel_leader_proc_number = INVALID_PROC_NUMBER;
	fake_backend2.my_proc_number = INVALID_PROC_NUMBER;
	fake_backend2.parallel_leader_proc_number = INVALID_PROC_NUMBER;
	MemSet(&fake_latch1, 0, sizeof(fake_latch1));
	MemSet(&fake_latch2, 0, sizeof(fake_latch2));

	PG_TRY();
	{
		CurrentPgBackend = &fake_backend1;
		ExitOnAnyError = true;
		MyProcPid = 111;
		MyProcNumber = 12;
		ParallelLeaderProcNumber = 34;
		MyBEEntry = &fake_beentry1;
		MyBgworkerEntry = &fake_bgworker1;
		MyStartTime = 222;
		MyStartTimestamp = 333;
		MyLatch = &fake_latch1;
		MyPMChildSlot = 44;
		strlcpy(OutputFileName, "backend-one.log", MAXPGPATH);
		MyBackendType = B_BACKEND;
		Mode = NormalProcessing;
		IgnoreSystemIndexes = true;
		pg_global_prng_state.s0 = 1111;
		pg_global_prng_state.s1 = 2222;

		CurrentPgBackend = &fake_backend2;
		ok = ok && !ExitOnAnyError;
		ok = ok && MyProcPid == 0;
		ok = ok && MyProcNumber == INVALID_PROC_NUMBER;
		ok = ok && ParallelLeaderProcNumber == INVALID_PROC_NUMBER;
		ok = ok && MyBEEntry == NULL;
		ok = ok && MyBgworkerEntry == NULL;
		ok = ok && MyStartTime == 0;
		ok = ok && MyStartTimestamp == 0;
		ok = ok && MyLatch == NULL;
		ok = ok && MyPMChildSlot == 0;
		ok = ok && OutputFileName[0] == '\0';
		ok = ok && MyBackendType == B_INVALID;
		ok = ok && Mode == BootstrapProcessing;
		ok = ok && !IgnoreSystemIndexes;
		ok = ok && pg_global_prng_state.s0 == 0;
		ok = ok && pg_global_prng_state.s1 == 0;

		ExitOnAnyError = false;
		MyProcPid = 555;
		MyProcNumber = 56;
		ParallelLeaderProcNumber = 78;
		MyBEEntry = &fake_beentry2;
		MyBgworkerEntry = &fake_bgworker2;
		MyStartTime = 666;
		MyStartTimestamp = 777;
		MyLatch = &fake_latch2;
		MyPMChildSlot = 88;
		strlcpy(OutputFileName, "backend-two.log", MAXPGPATH);
		MyBackendType = B_WAL_SENDER;
		Mode = InitProcessing;
		IgnoreSystemIndexes = false;
		pg_global_prng_state.s0 = 5555;
		pg_global_prng_state.s1 = 6666;

		CurrentPgBackend = &fake_backend1;
		ok = ok && ExitOnAnyError;
		ok = ok && MyProcPid == 111;
		ok = ok && MyProcNumber == 12;
		ok = ok && ParallelLeaderProcNumber == 34;
		ok = ok && MyBEEntry == &fake_beentry1;
		ok = ok && MyBgworkerEntry == &fake_bgworker1;
		ok = ok && MyStartTime == 222;
		ok = ok && MyStartTimestamp == 333;
		ok = ok && MyLatch == &fake_latch1;
		ok = ok && MyPMChildSlot == 44;
		ok = ok && strcmp(OutputFileName, "backend-one.log") == 0;
		ok = ok && MyBackendType == B_BACKEND;
		ok = ok && Mode == NormalProcessing;
		ok = ok && IgnoreSystemIndexes;
		ok = ok && pg_global_prng_state.s0 == 1111;
		ok = ok && pg_global_prng_state.s1 == 2222;

		CurrentPgBackend = &fake_backend2;
		ok = ok && !ExitOnAnyError;
		ok = ok && MyProcPid == 555;
		ok = ok && MyProcNumber == 56;
		ok = ok && ParallelLeaderProcNumber == 78;
		ok = ok && MyBEEntry == &fake_beentry2;
		ok = ok && MyBgworkerEntry == &fake_bgworker2;
		ok = ok && MyStartTime == 666;
		ok = ok && MyStartTimestamp == 777;
		ok = ok && MyLatch == &fake_latch2;
		ok = ok && MyPMChildSlot == 88;
		ok = ok && strcmp(OutputFileName, "backend-two.log") == 0;
		ok = ok && MyBackendType == B_WAL_SENDER;
		ok = ok && Mode == InitProcessing;
		ok = ok && !IgnoreSystemIndexes;
		ok = ok && pg_global_prng_state.s0 == 5555;
		ok = ok && pg_global_prng_state.s1 == 6666;

		CurrentPgBackend = saved_backend;
		ExitOnAnyError = saved_exit_on_any_error;
		MyProcPid = saved_proc_pid;
		MyProcNumber = saved_proc_number;
		ParallelLeaderProcNumber = saved_parallel_leader_proc_number;
		MyBEEntry = saved_beentry;
		MyBgworkerEntry = saved_bgworker_entry;
		MyStartTime = saved_start_time;
		MyStartTimestamp = saved_start_timestamp;
		MyLatch = saved_latch;
		MyPMChildSlot = saved_pm_child_slot;
		strlcpy(OutputFileName, saved_output_file_name, MAXPGPATH);
		MyBackendType = saved_backend_type;
		Mode = saved_mode;
		IgnoreSystemIndexes = saved_ignore_system_indexes;
		pg_global_prng_state = saved_global_prng_state;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		ExitOnAnyError = saved_exit_on_any_error;
		MyProcPid = saved_proc_pid;
		MyProcNumber = saved_proc_number;
		ParallelLeaderProcNumber = saved_parallel_leader_proc_number;
		MyBEEntry = saved_beentry;
		MyBgworkerEntry = saved_bgworker_entry;
		MyStartTime = saved_start_time;
		MyStartTimestamp = saved_start_timestamp;
		MyLatch = saved_latch;
		MyPMChildSlot = saved_pm_child_slot;
		strlcpy(OutputFileName, saved_output_file_name, MAXPGPATH);
		MyBackendType = saved_backend_type;
		Mode = saved_mode;
		IgnoreSystemIndexes = saved_ignore_system_indexes;
		pg_global_prng_state = saved_global_prng_state;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend core state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_command_log_state_is_backend_local);
Datum
test_backend_command_log_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	PgSession	fake_session1;
	PgSession	fake_session2;
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	MemSet(&fake_session1, 0, sizeof(fake_session1));
	MemSet(&fake_session2, 0, sizeof(fake_session2));

	PG_TRY();
	{
		CurrentPgBackend = &fake_backend1;
		CurrentPgSession = &fake_session1;
		*PgCurrentDoingCommandReadRef() = true;
		*PgCurrentUserDOptionRef() = "data-one";
		PgCurrentUsageSaveRusageRef()->ru_inblock = 11;
		PgCurrentUsageSaveTimevalRef()->tv_sec = 12;
		strlcpy(PgCurrentFormattedStartTimeBuffer(), "start-one",
				PG_BACKEND_FORMATTED_TS_LEN);
		*PgCurrentLogLineNumberRef() = 13;
		*PgCurrentLogLinePidRef() = 14;

		CurrentPgBackend = &fake_backend2;
		CurrentPgSession = &fake_session2;
		ok = ok && !*PgCurrentDoingCommandReadRef();
		ok = ok && *PgCurrentUserDOptionRef() == NULL;
		ok = ok && PgCurrentUsageSaveRusageRef()->ru_inblock == 0;
		ok = ok && PgCurrentUsageSaveTimevalRef()->tv_sec == 0;
		ok = ok && PgCurrentFormattedStartTimeBuffer()[0] == '\0';
		ok = ok && *PgCurrentLogLineNumberRef() == 0;
		ok = ok && *PgCurrentLogLinePidRef() == 0;

		*PgCurrentDoingCommandReadRef() = false;
		*PgCurrentUserDOptionRef() = "data-two";
		PgCurrentUsageSaveRusageRef()->ru_inblock = 21;
		PgCurrentUsageSaveTimevalRef()->tv_sec = 22;
		strlcpy(PgCurrentFormattedStartTimeBuffer(), "start-two",
				PG_BACKEND_FORMATTED_TS_LEN);
		*PgCurrentLogLineNumberRef() = 23;
		*PgCurrentLogLinePidRef() = 24;

		CurrentPgBackend = &fake_backend1;
		CurrentPgSession = &fake_session1;
		ok = ok && *PgCurrentDoingCommandReadRef();
		ok = ok && strcmp(*PgCurrentUserDOptionRef(), "data-one") == 0;
		ok = ok && PgCurrentUsageSaveRusageRef()->ru_inblock == 11;
		ok = ok && PgCurrentUsageSaveTimevalRef()->tv_sec == 12;
		ok = ok && strcmp(PgCurrentFormattedStartTimeBuffer(), "start-one") == 0;
		ok = ok && *PgCurrentLogLineNumberRef() == 13;
		ok = ok && *PgCurrentLogLinePidRef() == 14;

		CurrentPgBackend = &fake_backend2;
		CurrentPgSession = &fake_session2;
		ok = ok && !*PgCurrentDoingCommandReadRef();
		ok = ok && strcmp(*PgCurrentUserDOptionRef(), "data-two") == 0;
		ok = ok && PgCurrentUsageSaveRusageRef()->ru_inblock == 21;
		ok = ok && PgCurrentUsageSaveTimevalRef()->tv_sec == 22;
		ok = ok && strcmp(PgCurrentFormattedStartTimeBuffer(), "start-two") == 0;
		ok = ok && *PgCurrentLogLineNumberRef() == 23;
		ok = ok && *PgCurrentLogLinePidRef() == 24;

		CurrentPgBackend = saved_backend;
		CurrentPgSession = saved_session;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		CurrentPgSession = saved_session;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend command/log state was not backend-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_backend_expr_interp_state_is_backend_local);
Datum
test_backend_expr_interp_state_is_backend_local(PG_FUNCTION_ARGS)
{
	PgBackend  *saved_backend;
	PgBackend	fake_backend1;
	PgBackend	fake_backend2;
	const void *dispatch_one[1];
	const void *dispatch_two[1];
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	dispatch_one[0] = &fake_backend1;
	dispatch_two[0] = &fake_backend2;

	PG_TRY();
	{
		CurrentPgBackend = &fake_backend1;
		PgCurrentExprInterpState()->dispatch_table = dispatch_one;
		PgCurrentExprInterpState()->reverse_dispatch_table[0].opcode = &fake_backend1;
		PgCurrentExprInterpState()->reverse_dispatch_table[0].op = 11;

		CurrentPgBackend = &fake_backend2;
		ok = ok && PgCurrentExprInterpState()->dispatch_table == NULL;
		ok = ok && PgCurrentExprInterpState()->reverse_dispatch_table[0].opcode == NULL;
		ok = ok && PgCurrentExprInterpState()->reverse_dispatch_table[0].op == 0;
		PgCurrentExprInterpState()->dispatch_table = dispatch_two;
		PgCurrentExprInterpState()->reverse_dispatch_table[0].opcode = &fake_backend2;
		PgCurrentExprInterpState()->reverse_dispatch_table[0].op = 22;

		CurrentPgBackend = &fake_backend1;
		ok = ok && PgCurrentExprInterpState()->dispatch_table == dispatch_one;
		ok = ok && PgCurrentExprInterpState()->reverse_dispatch_table[0].opcode == &fake_backend1;
		ok = ok && PgCurrentExprInterpState()->reverse_dispatch_table[0].op == 11;

		CurrentPgBackend = &fake_backend2;
		ok = ok && PgCurrentExprInterpState()->dispatch_table == dispatch_two;
		ok = ok && PgCurrentExprInterpState()->reverse_dispatch_table[0].opcode == &fake_backend2;
		ok = ok && PgCurrentExprInterpState()->reverse_dispatch_table[0].op == 22;

		CurrentPgBackend = saved_backend;
	}
	PG_CATCH();
	{
		CurrentPgBackend = saved_backend;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend expression interpreter state was not backend-local");

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

		CurrentPgExecution = &fake_execution1;
		debug_query_string = "reset me";
		PgExecutionResetClosedState(&fake_execution1);
		ok = ok && debug_query_string == NULL;

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

		PgExecutionResetClosedState(&fake_execution2);
		ok = ok && CurrentMemoryContext == NULL;
		ok = ok && ErrorContext == NULL;
		ok = ok && MessageContext == NULL;
		ok = ok && TopTransactionContext == NULL;
		ok = ok && CurTransactionContext == NULL;
		ok = ok && PortalContext == NULL;

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
	fake_execution1.spi.connected = -1;
	fake_execution2.spi.connected = -1;
	MemSet(&fake_tuptable1, 0, sizeof(fake_tuptable1));
	MemSet(&fake_tuptable2, 0, sizeof(fake_tuptable2));

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		SPI_processed = 111;
		SPI_tuptable = &fake_tuptable1;
		SPI_result = SPI_OK_SELECT;

		CurrentPgExecution = &fake_execution2;
		ok = ok && SPI_processed == 0;
		ok = ok && SPI_tuptable == NULL;
		ok = ok && SPI_result == 0;
		ok = ok && *PgCurrentSPIConnectedRef() == -1;
		SPI_processed = 222;
		SPI_tuptable = &fake_tuptable2;
		SPI_result = SPI_OK_INSERT;

		CurrentPgExecution = &fake_execution1;
		ok = ok && SPI_processed == 111;
		ok = ok && SPI_tuptable == &fake_tuptable1;
		ok = ok && SPI_result == SPI_OK_SELECT;
		ok = ok && *PgCurrentSPIConnectedRef() == -1;

		CurrentPgExecution = &fake_execution2;
		ok = ok && SPI_processed == 222;
		ok = ok && SPI_tuptable == &fake_tuptable2;
		ok = ok && SPI_result == SPI_OK_INSERT;
		ok = ok && *PgCurrentSPIConnectedRef() == -1;

		CurrentPgExecution = saved_execution;
		SPI_processed = saved_spi_processed;
		SPI_tuptable = saved_spi_tuptable;
		SPI_result = saved_spi_result;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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
	MemSet(&fake_portal1, 0, sizeof(fake_portal1));
	MemSet(&fake_portal2, 0, sizeof(fake_portal2));

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		ActivePortal = &fake_portal1;

		CurrentPgExecution = &fake_execution2;
		ok = ok && ActivePortal == NULL;
		ActivePortal = &fake_portal2;

		CurrentPgExecution = &fake_execution1;
		ok = ok && ActivePortal == &fake_portal1;

		CurrentPgExecution = &fake_execution2;
		ok = ok && ActivePortal == &fake_portal2;

		CurrentPgExecution = saved_execution;
		ActivePortal = saved_active_portal;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
		ActivePortal = saved_active_portal;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "active portal was not execution-local");

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
	pg_atomic_init_u32(&shared_cost_balance1, 111);
	pg_atomic_init_u32(&active_nworkers1, 1);
	pg_atomic_init_u32(&shared_cost_balance2, 222);
	pg_atomic_init_u32(&active_nworkers2, 2);

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = saved_execution;
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
		CurrentPgExecution = saved_execution;
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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		*PgCurrentNodeWriteLocationFieldsRef() = true;
		*PgCurrentNodeReadStrtokPtrRef() = "node io one";
		*PgCurrentNodeRestoreLocationFieldsRef() = true;

		CurrentPgExecution = &fake_execution2;
		ok = ok && !*PgCurrentNodeWriteLocationFieldsRef();
		ok = ok && *PgCurrentNodeReadStrtokPtrRef() == NULL;
		ok = ok && !*PgCurrentNodeRestoreLocationFieldsRef();
		*PgCurrentNodeReadStrtokPtrRef() = "node io two";

		CurrentPgExecution = &fake_execution1;
		ok = ok && *PgCurrentNodeWriteLocationFieldsRef();
		ok = ok &&
			strcmp(*PgCurrentNodeReadStrtokPtrRef(), "node io one") == 0;
		ok = ok && *PgCurrentNodeRestoreLocationFieldsRef();

		CurrentPgExecution = &fake_execution2;
		ok = ok && !*PgCurrentNodeWriteLocationFieldsRef();
		ok = ok &&
			strcmp(*PgCurrentNodeReadStrtokPtrRef(), "node io two") == 0;
		ok = ok && !*PgCurrentNodeRestoreLocationFieldsRef();

		CurrentPgExecution = saved_execution;
		*PgCurrentNodeWriteLocationFieldsRef() = saved_write_location_fields;
		*PgCurrentNodeReadStrtokPtrRef() = saved_strtok_ptr;
		*PgCurrentNodeRestoreLocationFieldsRef() = saved_restore_location_fields;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		*PgCurrentBaseBackupStartedInRecoveryRef() = true;
		*PgCurrentBaseBackupTotalChecksumFailuresRef() = 17;
		*PgCurrentBaseBackupNoVerifyChecksumsRef() = true;

		CurrentPgExecution = &fake_execution2;
		ok = ok && !*PgCurrentBaseBackupStartedInRecoveryRef();
		ok = ok && *PgCurrentBaseBackupTotalChecksumFailuresRef() == 0;
		ok = ok && !*PgCurrentBaseBackupNoVerifyChecksumsRef();
		*PgCurrentBaseBackupTotalChecksumFailuresRef() = 29;

		CurrentPgExecution = &fake_execution1;
		ok = ok && *PgCurrentBaseBackupStartedInRecoveryRef();
		ok = ok && *PgCurrentBaseBackupTotalChecksumFailuresRef() == 17;
		ok = ok && *PgCurrentBaseBackupNoVerifyChecksumsRef();

		CurrentPgExecution = &fake_execution2;
		ok = ok && !*PgCurrentBaseBackupStartedInRecoveryRef();
		ok = ok && *PgCurrentBaseBackupTotalChecksumFailuresRef() == 29;
		ok = ok && !*PgCurrentBaseBackupNoVerifyChecksumsRef();

		CurrentPgExecution = saved_execution;
		*PgCurrentBaseBackupStartedInRecoveryRef() =
			saved_backup_started_in_recovery;
		*PgCurrentBaseBackupTotalChecksumFailuresRef() =
			saved_total_checksum_failures;
		*PgCurrentBaseBackupNoVerifyChecksumsRef() = saved_noverify_checksums;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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
	fake_context1 = (MemoryContext) &fake_execution1;
	fake_context2 = (MemoryContext) &fake_execution2;
	fake_strategy1 = (BufferAccessStrategy) &fake_execution1;
	fake_strategy2 = (BufferAccessStrategy) &fake_execution2;

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		*PgCurrentAnalyzeContextRef() = fake_context1;
		*PgCurrentAnalyzeStrategyRef() = fake_strategy1;

		CurrentPgExecution = &fake_execution2;
		ok = ok && *PgCurrentAnalyzeContextRef() == NULL;
		ok = ok && *PgCurrentAnalyzeStrategyRef() == NULL;
		*PgCurrentAnalyzeContextRef() = fake_context2;
		*PgCurrentAnalyzeStrategyRef() = fake_strategy2;

		CurrentPgExecution = &fake_execution1;
		ok = ok && *PgCurrentAnalyzeContextRef() == fake_context1;
		ok = ok && *PgCurrentAnalyzeStrategyRef() == fake_strategy1;

		CurrentPgExecution = &fake_execution2;
		ok = ok && *PgCurrentAnalyzeContextRef() == fake_context2;
		ok = ok && *PgCurrentAnalyzeStrategyRef() == fake_strategy2;

		CurrentPgExecution = saved_execution;
		*PgCurrentAnalyzeContextRef() = saved_analyze_context;
		*PgCurrentAnalyzeStrategyRef() = saved_analyze_strategy;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	saved_creating_extension = creating_extension;
	saved_current_extension_object = CurrentExtensionObject;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		creating_extension = true;
		CurrentExtensionObject = 12345;

		CurrentPgExecution = &fake_execution2;
		ok = ok && !creating_extension;
		ok = ok && CurrentExtensionObject == InvalidOid;
		CurrentExtensionObject = 67890;

		CurrentPgExecution = &fake_execution1;
		ok = ok && creating_extension;
		ok = ok && CurrentExtensionObject == 12345;

		CurrentPgExecution = &fake_execution2;
		ok = ok && !creating_extension;
		ok = ok && CurrentExtensionObject == 67890;

		CurrentPgExecution = saved_execution;
		creating_extension = saved_creating_extension;
		CurrentExtensionObject = saved_current_extension_object;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
		creating_extension = saved_creating_extension;
		CurrentExtensionObject = saved_current_extension_object;
		PG_RE_THROW();
	}
	PG_END_TRY();

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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		*PgCurrentMatViewMaintenanceDepthRef() = 2;

		CurrentPgExecution = &fake_execution2;
		ok = ok && *PgCurrentMatViewMaintenanceDepthRef() == 0;
		*PgCurrentMatViewMaintenanceDepthRef() = 5;

		CurrentPgExecution = &fake_execution1;
		ok = ok && *PgCurrentMatViewMaintenanceDepthRef() == 2;

		CurrentPgExecution = &fake_execution2;
		ok = ok && *PgCurrentMatViewMaintenanceDepthRef() == 5;

		CurrentPgExecution = saved_execution;
		*PgCurrentMatViewMaintenanceDepthRef() = saved_maintenance_depth;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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
	MemSet(&fake_rdata1, 0, sizeof(fake_rdata1));
	MemSet(&fake_rdata2, 0, sizeof(fake_rdata2));

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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
		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		*PgCurrentGUCCheckErrcodeValueRef() = 101;
		GUC_check_errmsg_string = "message one";
		GUC_check_errdetail_string = "detail one";
		GUC_check_errhint_string = "hint one";
		*PgCurrentFormatErrnumberRef() = 102;
		*PgCurrentFormatDomainRef() = "domain one";
		*PgCurrentConfigFileLinenoRef() = 103;
		*PgCurrentGUCFlexFatalErrmsgRef() = "fatal one";
		*PgCurrentGUCFlexFatalJmpRef() = (sigjmp_buf *) &fake_execution1;

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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
	fake_execution1.catalog.currently_reindexed_heap = InvalidOid;
	fake_execution1.catalog.currently_reindexed_index = InvalidOid;
	fake_execution2.catalog.currently_reindexed_heap = InvalidOid;
	fake_execution2.catalog.currently_reindexed_index = InvalidOid;

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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
	MemSet(tupledesc_array1, 0, sizeof(tupledesc_array1));
	MemSet(tupledesc_array2, 0, sizeof(tupledesc_array2));

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		PgCurrentRelMapActiveSharedUpdatesRef()->num_mappings = 1;
		PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapoid = 101;
		PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapfilenumber =
			102;
		PgCurrentRelMapActiveLocalUpdatesRef()->num_mappings = 2;
		PgCurrentRelMapPendingSharedUpdatesRef()->num_mappings = 3;
		PgCurrentRelMapPendingLocalUpdatesRef()->num_mappings = 4;

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
		ok = ok && PgCurrentRelMapActiveSharedUpdatesRef()->num_mappings == 1;
		ok = ok &&
			PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapoid == 101;
		ok = ok &&
			PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapfilenumber ==
			102;
		ok = ok && PgCurrentRelMapActiveLocalUpdatesRef()->num_mappings == 2;
		ok = ok && PgCurrentRelMapPendingSharedUpdatesRef()->num_mappings == 3;
		ok = ok && PgCurrentRelMapPendingLocalUpdatesRef()->num_mappings == 4;

		CurrentPgExecution = &fake_execution2;
		ok = ok && PgCurrentRelMapActiveSharedUpdatesRef()->num_mappings == 5;
		ok = ok &&
			PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapoid == 201;
		ok = ok &&
			PgCurrentRelMapActiveSharedUpdatesRef()->mappings[0].mapfilenumber ==
			202;
		ok = ok && PgCurrentRelMapActiveLocalUpdatesRef()->num_mappings == 6;
		ok = ok && PgCurrentRelMapPendingSharedUpdatesRef()->num_mappings == 7;
		ok = ok && PgCurrentRelMapPendingLocalUpdatesRef()->num_mappings == 8;

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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
	MemSet(&invalmsg1, 0, sizeof(invalmsg1));
	MemSet(&invalmsg2, 0, sizeof(invalmsg2));

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = &fake_execution1;
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

		CurrentPgExecution = &fake_execution2;
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

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		*PgCurrentPendingActionsRef() = (struct ActionList *) &fake_execution1;
		*PgCurrentPendingListenActionsRef() = (HTAB *) &fake_execution1;
		*PgCurrentPendingNotifiesRef() =
			(struct NotificationList *) &fake_execution1;
		PgCurrentQueueHeadBeforeWriteRef()->page = 101;
		PgCurrentQueueHeadBeforeWriteRef()->offset = 102;
		PgCurrentQueueHeadAfterWriteRef()->page = 103;
		PgCurrentQueueHeadAfterWriteRef()->offset = 104;
		*PgCurrentSignalPidsRef() = (int32 *) &fake_execution1;
		*PgCurrentSignalProcnosRef() = (ProcNumber *) &fake_execution1;
		*PgCurrentTryAdvanceTailRef() = true;

		CurrentPgExecution = &fake_execution2;
		ok = ok && *PgCurrentPendingActionsRef() == NULL;
		ok = ok && *PgCurrentPendingListenActionsRef() == NULL;
		ok = ok && *PgCurrentPendingNotifiesRef() == NULL;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->page == 0;
		ok = ok && PgCurrentQueueHeadBeforeWriteRef()->offset == 0;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->page == 0;
		ok = ok && PgCurrentQueueHeadAfterWriteRef()->offset == 0;
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
		*PgCurrentSignalPidsRef() = (int32 *) &fake_execution2;
		*PgCurrentSignalProcnosRef() = (ProcNumber *) &fake_execution2;
		*PgCurrentTryAdvanceTailRef() = false;

		CurrentPgExecution = &fake_execution1;
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
		ok = ok && *PgCurrentSignalPidsRef() == (int32 *) &fake_execution1;
		ok = ok && *PgCurrentSignalProcnosRef() ==
			(ProcNumber *) &fake_execution1;
		ok = ok && *PgCurrentTryAdvanceTailRef();

		CurrentPgExecution = &fake_execution2;
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
		ok = ok && *PgCurrentSignalPidsRef() == (int32 *) &fake_execution2;
		ok = ok && *PgCurrentSignalProcnosRef() ==
			(ProcNumber *) &fake_execution2;
		ok = ok && !*PgCurrentTryAdvanceTailRef();

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
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
	bool		ok = true;

	saved_execution = CurrentPgExecution;
	MemSet(&fake_execution1, 0, sizeof(fake_execution1));
	MemSet(&fake_execution2, 0, sizeof(fake_execution2));

	PG_TRY();
	{
		CurrentPgExecution = &fake_execution1;
		*PgCurrentArrayAnalyzeExtraDataRef() = &fake_execution1;
		*PgCurrentTriggerDepthRef() = 101;
		*PgCurrentAfterTriggersDataRef() = &fake_execution1;
		*PgCurrentRegexLocaleRef() = &fake_execution1;
		*PgCurrentValgrindOldErrorCountRef() = 101;
		*PgCurrentSnapBuildSavedResourceOwnerDuringExportRef() =
			(ResourceOwner) &fake_execution1;
		*PgCurrentSnapBuildExportInProgressRef() = true;

		CurrentPgExecution = &fake_execution2;
		ok = ok && *PgCurrentArrayAnalyzeExtraDataRef() == NULL;
		ok = ok && *PgCurrentTriggerDepthRef() == 0;
		ok = ok && *PgCurrentAfterTriggersDataRef() == NULL;
		ok = ok && *PgCurrentRegexLocaleRef() == NULL;
		ok = ok && *PgCurrentValgrindOldErrorCountRef() == 0;
		ok = ok && *PgCurrentSnapBuildSavedResourceOwnerDuringExportRef() ==
			NULL;
		ok = ok && !*PgCurrentSnapBuildExportInProgressRef();

		*PgCurrentArrayAnalyzeExtraDataRef() = &fake_execution2;
		*PgCurrentTriggerDepthRef() = 201;
		*PgCurrentAfterTriggersDataRef() = &fake_execution2;
		*PgCurrentRegexLocaleRef() = &fake_execution2;
		*PgCurrentValgrindOldErrorCountRef() = 201;
		*PgCurrentSnapBuildSavedResourceOwnerDuringExportRef() =
			(ResourceOwner) &fake_execution2;
		*PgCurrentSnapBuildExportInProgressRef() = false;

		CurrentPgExecution = &fake_execution1;
		ok = ok && *PgCurrentArrayAnalyzeExtraDataRef() == &fake_execution1;
		ok = ok && *PgCurrentTriggerDepthRef() == 101;
		ok = ok && *PgCurrentAfterTriggersDataRef() == &fake_execution1;
		ok = ok && *PgCurrentRegexLocaleRef() == &fake_execution1;
		ok = ok && *PgCurrentValgrindOldErrorCountRef() == 101;
		ok = ok && *PgCurrentSnapBuildSavedResourceOwnerDuringExportRef() ==
			(ResourceOwner) &fake_execution1;
		ok = ok && *PgCurrentSnapBuildExportInProgressRef();

		CurrentPgExecution = &fake_execution2;
		ok = ok && *PgCurrentArrayAnalyzeExtraDataRef() == &fake_execution2;
		ok = ok && *PgCurrentTriggerDepthRef() == 201;
		ok = ok && *PgCurrentAfterTriggersDataRef() == &fake_execution2;
		ok = ok && *PgCurrentRegexLocaleRef() == &fake_execution2;
		ok = ok && *PgCurrentValgrindOldErrorCountRef() == 201;
		ok = ok && *PgCurrentSnapBuildSavedResourceOwnerDuringExportRef() ==
			(ResourceOwner) &fake_execution2;
		ok = ok && !*PgCurrentSnapBuildExportInProgressRef();

		CurrentPgExecution = saved_execution;
	}
	PG_CATCH();
	{
		CurrentPgExecution = saved_execution;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "miscellaneous execution scratch state was not execution-local");

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

PG_FUNCTION_INFO_V1(test_connection_reset_closed_state);
Datum
test_connection_reset_closed_state(PG_FUNCTION_ARGS)
{
	PgConnection connection;
	PgConnectionSocketIOState *socket_io;
	PgConnectionSecurityState *security;
	const PQcommMethods methods = {0};
	struct ClientSocket fake_client_socket;
	WaitEventSet *fake_wait_set;
	bool		ok = true;

	MemSet(&connection, 0, sizeof(connection));
	MemSet(&fake_client_socket, 0, sizeof(fake_client_socket));
	fake_wait_set = (WaitEventSet *) &connection;

	connection.identity.port = (struct Port *) &connection;
	MemSet(connection.identity.cancel_key, 0x7a,
		   sizeof(connection.identity.cancel_key));
	connection.identity.cancel_key_length =
		sizeof(connection.identity.cancel_key);

	socket_io = &connection.socket_io;
	socket_io->send_buffer = (char *) "released by socket_close";
	socket_io->send_buffer_size = 128;
	socket_io->send_pointer = 64;
	socket_io->send_start = 32;
	socket_io->recv_buffer[0] = 'x';
	socket_io->recv_pointer = 7;
	socket_io->recv_length = 9;
	socket_io->comm_busy = true;
	socket_io->comm_reading_msg = true;

	connection.protocol.comm_methods = &methods;
	connection.protocol.fe_be_wait_set = fake_wait_set;
	connection.protocol.frontend_protocol = PG_PROTOCOL(3, 2);
	connection.startup.client_auth_in_progress = true;
	connection.startup.client_socket = &fake_client_socket;

	security = &connection.security;
	security->ssl_loaded_verify_locations = true;
	security->gss_send_buffer = malloc(8);
	security->gss_send_length = 1;
	security->gss_send_next = 2;
	security->gss_send_consumed = 3;
	security->gss_recv_buffer = malloc(8);
	security->gss_recv_length = 4;
	security->gss_result_buffer = malloc(8);
	security->gss_result_length = 5;
	security->gss_result_next = 6;
	security->gss_max_packet_size = 7;
	security->pam_password = "borrowed";
	security->pam_port = (struct Port *) &connection;
	security->pam_no_password = true;

	if (security->gss_send_buffer == NULL ||
		security->gss_recv_buffer == NULL ||
		security->gss_result_buffer == NULL)
	{
		free(security->gss_send_buffer);
		free(security->gss_recv_buffer);
		free(security->gss_result_buffer);
		elog(ERROR, "out of memory");
	}

	PgConnectionResetClosedState(&connection);

	ok = ok && connection.identity.port == NULL;
	ok = ok && connection.identity.cancel_key[0] == 0;
	ok = ok && connection.identity.cancel_key_length == 0;

	socket_io = &connection.socket_io;
	ok = ok && socket_io->send_buffer == NULL;
	ok = ok && socket_io->send_buffer_size == 0;
	ok = ok && socket_io->send_pointer == 0;
	ok = ok && socket_io->send_start == 0;
	ok = ok && socket_io->recv_buffer[0] == '\0';
	ok = ok && socket_io->recv_pointer == 0;
	ok = ok && socket_io->recv_length == 0;
	ok = ok && !socket_io->comm_busy;
	ok = ok && !socket_io->comm_reading_msg;

	ok = ok && connection.protocol.comm_methods == NULL;
	ok = ok && connection.protocol.fe_be_wait_set == NULL;
	ok = ok && connection.protocol.frontend_protocol == 0;
	ok = ok && !connection.startup.client_auth_in_progress;
	ok = ok && connection.startup.client_socket == NULL;

	security = &connection.security;
	ok = ok && !security->ssl_loaded_verify_locations;
	ok = ok && security->gss_send_buffer == NULL;
	ok = ok && security->gss_send_length == 0;
	ok = ok && security->gss_send_next == 0;
	ok = ok && security->gss_send_consumed == 0;
	ok = ok && security->gss_recv_buffer == NULL;
	ok = ok && security->gss_recv_length == 0;
	ok = ok && security->gss_result_buffer == NULL;
	ok = ok && security->gss_result_length == 0;
	ok = ok && security->gss_result_next == 0;
	ok = ok && security->gss_max_packet_size == 0;
	ok = ok && security->pam_password == NULL;
	ok = ok && security->pam_port == NULL;
	ok = ok && !security->pam_no_password;

	if (!ok)
		elog(ERROR, "closed connection runtime state was not reset");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_connection_output_state_is_connection_local);
Datum
test_connection_output_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	CommandDest saved_where_to_send_output;
	int			saved_client_connection_check_interval;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_where_to_send_output = whereToSendOutput;
	saved_client_connection_check_interval = client_connection_check_interval;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));
	fake_connection1.output.where_to_send_output = DestDebug;
	fake_connection2.output.where_to_send_output = DestDebug;

	PG_TRY();
	{
		CurrentPgConnection = &fake_connection1;
		whereToSendOutput = DestRemote;
		client_connection_check_interval = 11;

		CurrentPgConnection = &fake_connection2;
		ok = ok && whereToSendOutput == DestDebug;
		ok = ok && client_connection_check_interval == 0;
		whereToSendOutput = DestNone;
		client_connection_check_interval = 22;

		CurrentPgConnection = &fake_connection1;
		ok = ok && whereToSendOutput == DestRemote;
		ok = ok && client_connection_check_interval == 11;

		CurrentPgConnection = &fake_connection2;
		ok = ok && whereToSendOutput == DestNone;
		ok = ok && client_connection_check_interval == 22;

		CurrentPgConnection = saved_connection;
		whereToSendOutput = saved_where_to_send_output;
		client_connection_check_interval = saved_client_connection_check_interval;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		whereToSendOutput = saved_where_to_send_output;
		client_connection_check_interval = saved_client_connection_check_interval;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection output state was not connection-local");

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
	ConnectionTiming saved_timing;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	saved_client_auth_in_progress = ClientAuthInProgress;
	saved_client_socket = MyClientSocket;
	saved_timing = conn_timing;
	fake_client_socket1 = (struct ClientSocket *) &fake_connection1;
	fake_client_socket2 = (struct ClientSocket *) &fake_connection2;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));
	fake_connection1.startup.timing.ready_for_use = TIMESTAMP_MINUS_INFINITY;
	fake_connection2.startup.timing.ready_for_use = TIMESTAMP_MINUS_INFINITY;

	PG_TRY();
	{
		CurrentPgConnection = &fake_connection1;
		ClientAuthInProgress = true;
		MyClientSocket = fake_client_socket1;
		conn_timing.socket_create = 11;
		conn_timing.ready_for_use = 12;
		conn_timing.fork_start = 13;
		conn_timing.fork_end = 14;
		conn_timing.auth_start = 15;
		conn_timing.auth_end = 16;

		CurrentPgConnection = &fake_connection2;
		ok = ok && !ClientAuthInProgress;
		ok = ok && MyClientSocket == NULL;
		ok = ok && conn_timing.socket_create == 0;
		ok = ok && conn_timing.ready_for_use == TIMESTAMP_MINUS_INFINITY;
		ok = ok && conn_timing.fork_start == 0;
		ok = ok && conn_timing.fork_end == 0;
		ok = ok && conn_timing.auth_start == 0;
		ok = ok && conn_timing.auth_end == 0;
		ClientAuthInProgress = false;
		MyClientSocket = fake_client_socket2;
		conn_timing.socket_create = 21;
		conn_timing.ready_for_use = 22;
		conn_timing.fork_start = 23;
		conn_timing.fork_end = 24;
		conn_timing.auth_start = 25;
		conn_timing.auth_end = 26;

		CurrentPgConnection = &fake_connection1;
		ok = ok && ClientAuthInProgress;
		ok = ok && MyClientSocket == fake_client_socket1;
		ok = ok && conn_timing.socket_create == 11;
		ok = ok && conn_timing.ready_for_use == 12;
		ok = ok && conn_timing.fork_start == 13;
		ok = ok && conn_timing.fork_end == 14;
		ok = ok && conn_timing.auth_start == 15;
		ok = ok && conn_timing.auth_end == 16;

		CurrentPgConnection = &fake_connection2;
		ok = ok && !ClientAuthInProgress;
		ok = ok && MyClientSocket == fake_client_socket2;
		ok = ok && conn_timing.socket_create == 21;
		ok = ok && conn_timing.ready_for_use == 22;
		ok = ok && conn_timing.fork_start == 23;
		ok = ok && conn_timing.fork_end == 24;
		ok = ok && conn_timing.auth_start == 25;
		ok = ok && conn_timing.auth_end == 26;

		CurrentPgConnection = saved_connection;
		ClientAuthInProgress = saved_client_auth_in_progress;
		MyClientSocket = saved_client_socket;
		conn_timing = saved_timing;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		ClientAuthInProgress = saved_client_auth_in_progress;
		MyClientSocket = saved_client_socket;
		conn_timing = saved_timing;
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

PG_FUNCTION_INFO_V1(test_connection_security_state_is_connection_local);
Datum
test_connection_security_state_is_connection_local(PG_FUNCTION_ARGS)
{
	PgConnection *saved_connection;
	PgConnection fake_connection1;
	PgConnection fake_connection2;
	PgConnectionSecurityState *security;
	bool		ok = true;

	saved_connection = CurrentPgConnection;
	MemSet(&fake_connection1, 0, sizeof(fake_connection1));
	MemSet(&fake_connection2, 0, sizeof(fake_connection2));

	PG_TRY();
	{
		CurrentPgConnection = &fake_connection1;
		security = PgCurrentConnectionSecurityStateRef();
		ok = ok && !security->ssl_loaded_verify_locations;
		ok = ok && security->gss_send_buffer == NULL;
		ok = ok && security->gss_send_length == 0;
		ok = ok && security->gss_recv_buffer == NULL;
		ok = ok && security->gss_result_buffer == NULL;
		ok = ok && security->gss_max_packet_size == 0;
		ok = ok && security->pam_password == NULL;
		ok = ok && security->pam_port == NULL;
		ok = ok && !security->pam_no_password;
		security->ssl_loaded_verify_locations = true;
		security->gss_send_buffer = (char *) &fake_connection1;
		security->gss_send_length = 11;
		security->gss_send_next = 12;
		security->gss_send_consumed = 13;
		security->gss_recv_buffer = (char *) &fake_connection2;
		security->gss_recv_length = 14;
		security->gss_result_buffer = (char *) &saved_connection;
		security->gss_result_length = 15;
		security->gss_result_next = 16;
		security->gss_max_packet_size = 17;
		security->pam_password = "pam-one";
		security->pam_port = (struct Port *) &fake_connection1;
		security->pam_no_password = true;

		CurrentPgConnection = &fake_connection2;
		security = PgCurrentConnectionSecurityStateRef();
		ok = ok && !security->ssl_loaded_verify_locations;
		ok = ok && security->gss_send_buffer == NULL;
		ok = ok && security->gss_send_length == 0;
		ok = ok && security->gss_recv_buffer == NULL;
		ok = ok && security->gss_result_buffer == NULL;
		ok = ok && security->gss_max_packet_size == 0;
		ok = ok && security->pam_password == NULL;
		ok = ok && security->pam_port == NULL;
		ok = ok && !security->pam_no_password;
		security->ssl_loaded_verify_locations = false;
		security->gss_send_buffer = (char *) &fake_connection2;
		security->gss_send_length = 21;
		security->gss_send_next = 22;
		security->gss_send_consumed = 23;
		security->gss_recv_buffer = (char *) &fake_connection1;
		security->gss_recv_length = 24;
		security->gss_result_buffer = (char *) &fake_connection2;
		security->gss_result_length = 25;
		security->gss_result_next = 26;
		security->gss_max_packet_size = 27;
		security->pam_password = "pam-two";
		security->pam_port = (struct Port *) &fake_connection2;
		security->pam_no_password = false;

		CurrentPgConnection = &fake_connection1;
		security = PgCurrentConnectionSecurityStateRef();
		ok = ok && security->ssl_loaded_verify_locations;
		ok = ok && security->gss_send_buffer == (char *) &fake_connection1;
		ok = ok && security->gss_send_length == 11;
		ok = ok && security->gss_send_next == 12;
		ok = ok && security->gss_send_consumed == 13;
		ok = ok && security->gss_recv_buffer == (char *) &fake_connection2;
		ok = ok && security->gss_recv_length == 14;
		ok = ok && security->gss_result_buffer == (char *) &saved_connection;
		ok = ok && security->gss_result_length == 15;
		ok = ok && security->gss_result_next == 16;
		ok = ok && security->gss_max_packet_size == 17;
		ok = ok && strcmp(security->pam_password, "pam-one") == 0;
		ok = ok && security->pam_port == (struct Port *) &fake_connection1;
		ok = ok && security->pam_no_password;

		CurrentPgConnection = &fake_connection2;
		security = PgCurrentConnectionSecurityStateRef();
		ok = ok && !security->ssl_loaded_verify_locations;
		ok = ok && security->gss_send_buffer == (char *) &fake_connection2;
		ok = ok && security->gss_send_length == 21;
		ok = ok && security->gss_send_next == 22;
		ok = ok && security->gss_send_consumed == 23;
		ok = ok && security->gss_recv_buffer == (char *) &fake_connection1;
		ok = ok && security->gss_recv_length == 24;
		ok = ok && security->gss_result_buffer == (char *) &fake_connection2;
		ok = ok && security->gss_result_length == 25;
		ok = ok && security->gss_result_next == 26;
		ok = ok && security->gss_max_packet_size == 27;
		ok = ok && strcmp(security->pam_password, "pam-two") == 0;
		ok = ok && security->pam_port == (struct Port *) &fake_connection2;
		ok = ok && !security->pam_no_password;

		CurrentPgConnection = saved_connection;
	}
	PG_CATCH();
	{
		CurrentPgConnection = saved_connection;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "connection security state was not connection-local");

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
