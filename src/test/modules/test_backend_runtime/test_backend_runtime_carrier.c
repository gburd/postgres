/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_carrier.c
 *		Carrier-owned runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_carrier.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

#ifdef USE_XTC_CARRIER
#include "postmaster/pg_xtc_carrier.h"
#include "xtc.h"
#include "xtc_loop.h"
#include "xtc_proc.h"

StaticAssertDecl(sizeof(((Latch *) 0)->owner_fiber_gen) == sizeof(((xtc_pid_t *) 0)->gen),
				 "latch fiber generation must preserve the libxtc generation");
StaticAssertDecl(sizeof(((PGPROC *) 0)->sem_fiber_gen) == sizeof(((xtc_pid_t *) 0)->gen),
				 "semaphore fiber generation must preserve the libxtc generation");

/* One private loop/thread: no PG roots or error stack in these callbacks. */
typedef struct FiberIdentityTest
{
	xtc_loop_t *loop;
	xtc_pid_t	supervisor;
	xtc_pid_t	backend;
	int			failures;
	int			completed;
	int			exited;
} FiberIdentityTest;

#define FIBER_IDENTITY_CHECK(test, expr) \
	do { \
		if (!(expr)) \
			(test)->failures++; \
	} while (0)

static void
fiber_identity_receive(FiberIdentityTest *test)
{
	void	   *msg = NULL;
	size_t		len = 0;

	FIBER_IDENTITY_CHECK(test, xtc_recv(&msg, &len, 1000000000) == XTC_OK);
	if (msg != NULL)
		xtc_free(msg);
}

static void
fiber_identity_at_exit(void *arg)
{
	FiberIdentityTest *test = arg;

	FIBER_IDENTITY_CHECK(test, !xtc_pg_in_backend_fiber());
	FIBER_IDENTITY_CHECK(test, !xtc_pid_is_none(xtc_self()));
	test->exited++;
}

static void
fiber_identity_sibling(void *arg)
{
	FiberIdentityTest *test = arg;
	PgCarrier	carrier = {0};

	FIBER_IDENTITY_CHECK(test, !xtc_pg_in_backend_fiber());
	FIBER_IDENTITY_CHECK(test, xtc_proc_set_userdata(&carrier) == XTC_OK);
	FIBER_IDENTITY_CHECK(test, xtc_pg_in_backend_fiber());
	FIBER_IDENTITY_CHECK(test, xtc_proc_at_exit(fiber_identity_at_exit, test) == XTC_OK);
	FIBER_IDENTITY_CHECK(test, xtc_proc_set_userdata(NULL) == XTC_OK);
	/* No yield: the sibling exits before the backend can consume this wake. */
	FIBER_IDENTITY_CHECK(test, xtc_send(test->backend, "wake", 5) == XTC_OK);
	xtc_exit_self(0);
}

static void
fiber_identity_backend(void *arg)
{
	FiberIdentityTest *test = arg;
	PgCarrier  *carrier = calloc(1, sizeof(PgCarrier));
	xtc_pid_t	self = xtc_self();

	FIBER_IDENTITY_CHECK(test, carrier != NULL);
	if (carrier == NULL)
		return;
	/* Identity must work before any PG current-work roots are installed. */
	FIBER_IDENTITY_CHECK(test, !xtc_pg_in_backend_fiber());
	carrier->migratable = true;
	FIBER_IDENTITY_CHECK(test, xtc_proc_set_userdata(carrier) == XTC_OK);
	FIBER_IDENTITY_CHECK(test, xtc_pg_in_backend_fiber());
	FIBER_IDENTITY_CHECK(test, xtc_pg_backend_fiber_is_migratable());
	FIBER_IDENTITY_CHECK(test, xtc_proc_at_exit(fiber_identity_at_exit, test) == XTC_OK);
	FIBER_IDENTITY_CHECK(test, xtc_send(test->supervisor, "ready", 6) == XTC_OK);
	fiber_identity_receive(test);
	/* A parked, supervisor resumed, B exited, then A resumed on this thread. */
	FIBER_IDENTITY_CHECK(test, test->exited == 1);
	FIBER_IDENTITY_CHECK(test, xtc_pid_eq(xtc_self(), self));
	FIBER_IDENTITY_CHECK(test, xtc_proc_userdata() == carrier);
	FIBER_IDENTITY_CHECK(test, xtc_pg_in_backend_fiber());
	FIBER_IDENTITY_CHECK(test, xtc_pg_backend_fiber_is_migratable());
	FIBER_IDENTITY_CHECK(test, xtc_proc_set_userdata(NULL) == XTC_OK);
	free(carrier);
	/* Final exit-only span: no owner, but still a libxtc proc. */
	FIBER_IDENTITY_CHECK(test, !xtc_pg_in_backend_fiber());
	FIBER_IDENTITY_CHECK(test, !xtc_pg_backend_fiber_is_migratable());
	test->completed++;
}

static void
fiber_identity_supervisor(void *arg)
{
	FiberIdentityTest *test = arg;
	xtc_proc_opts_t opts = XTC_PROC_OPTS_INIT;
	xtc_pid_t	child;
	uint64_t	ref;

	test->supervisor = xtc_self();
	FIBER_IDENTITY_CHECK(test, !xtc_pg_in_backend_fiber());
	FIBER_IDENTITY_CHECK(test, xtc_proc_spawn_monitor(test->loop, fiber_identity_backend,
													 test, &opts, &test->backend, &ref) == XTC_OK);
	fiber_identity_receive(test);
	FIBER_IDENTITY_CHECK(test, !xtc_pg_in_backend_fiber());
	FIBER_IDENTITY_CHECK(test, !xtc_pg_backend_fiber_is_migratable());
	FIBER_IDENTITY_CHECK(test, xtc_proc_spawn_monitor(test->loop, fiber_identity_sibling,
													 test, NULL, &child, &ref) == XTC_OK);
	/* Both monitored children must exit; each receive has a finite timeout. */
	for (int i = 0; i < 2; i++)
	{
		void	   *msg = NULL;
		size_t		len = 0;
		xtc_down_info_t di = XTC_DOWN_INFO_INIT;

		FIBER_IDENTITY_CHECK(test, xtc_recv(&msg, &len, 1000000000) == XTC_OK);
		if (msg != NULL)
		{
			if (xtc_down_decode_ex(msg, len, &di) == XTC_OK)
				FIBER_IDENTITY_CHECK(test, di.reason == 0);
			else
				test->failures++;
			xtc_free(msg);
		}
	}
	FIBER_IDENTITY_CHECK(test, !xtc_pg_in_backend_fiber());
	FIBER_IDENTITY_CHECK(test, xtc_loop_stop(test->loop) == XTC_OK);
}

static void *
fiber_identity_thread(void *arg)
{
	FiberIdentityTest *test = arg;

	FIBER_IDENTITY_CHECK(test, !xtc_pg_in_backend_fiber());
	if (xtc_loop_create(&test->loop) != XTC_OK)
	{
		test->failures++;
		return NULL;
	}
	if (xtc_proc_spawn(test->loop, fiber_identity_supervisor, test, NULL, NULL) == XTC_OK)
		FIBER_IDENTITY_CHECK(test, xtc_loop_run(test->loop) == XTC_OK);
	else
		test->failures++;
	FIBER_IDENTITY_CHECK(test, !xtc_pg_in_backend_fiber());
	FIBER_IDENTITY_CHECK(test, test->completed == 1 && test->exited == 2);
	xtc_loop_destroy(test->loop);
	return NULL;
}
#endif

PG_FUNCTION_INFO_V1(test_carrier_fiber_identity);
Datum
test_carrier_fiber_identity(PG_FUNCTION_ARGS)
{
#ifdef USE_XTC_CARRIER
	FiberIdentityTest test = {0};
	pthread_t	thread;
	int			rc;

	/* Do not nest a loop in the SQL caller's possibly active backend fiber. */
	rc = pthread_create(&thread, NULL, fiber_identity_thread, &test);
	if (rc != 0)
		elog(ERROR, "could not create fiber identity test thread: %d", rc);
	rc = pthread_join(thread, NULL);
	if (rc != 0)
		elog(ERROR, "could not join fiber identity test thread: %d", rc);
	if (test.failures != 0)
		elog(ERROR, "fiber identity checks failed: %d", test.failures);
#endif
	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_carrier_misc_state_is_carrier_local);
Datum
test_carrier_misc_state_is_carrier_local(PG_FUNCTION_ARGS)
{
	PgCarrier  *saved_carrier;
	PgCarrier	fake_carrier1;
	PgCarrier	fake_carrier2;
	char		stack_marker1;
	char		stack_marker2;
	void	   *thread_start1 = &fake_carrier1;
	void	   *thread_start2 = &fake_carrier2;
	bool		saved_is_under_postmaster;
	bool		ok = true;

	saved_carrier = CurrentPgCarrier;
	saved_is_under_postmaster = IsUnderPostmaster;
	MemSet(&fake_carrier1, 0, sizeof(fake_carrier1));
	MemSet(&fake_carrier2, 0, sizeof(fake_carrier2));
	fake_carrier1.kind = PG_CARRIER_THREAD;
	fake_carrier2.kind = PG_CARRIER_THREAD;
	fake_carrier1.wait_event_signal_fd = -1;
	fake_carrier1.wait_event_selfpipe_readfd = -1;
	fake_carrier1.wait_event_selfpipe_writefd = -1;
	fake_carrier2.wait_event_signal_fd = -1;
	fake_carrier2.wait_event_selfpipe_readfd = -1;
	fake_carrier2.wait_event_selfpipe_writefd = -1;

	PG_TRY();
	{
		PgSetCurrentCarrier(&fake_carrier1);
		*PgCurrentWaitEventWaitingRef() = true;
		*PgCurrentWaitEventSignalFdRef() = 11;
		*PgCurrentWaitEventSelfPipeReadFdRef() = 12;
		*PgCurrentWaitEventSelfPipeWriteFdRef() = 13;
		*PgCurrentWaitEventSelfPipeOwnerPidRef() = 14;
		*PgCurrentStackBasePtrRef() = &stack_marker1;
		*PgCurrentBackendThreadStartRef() = thread_start1;
		IsUnderPostmaster = true;

		PgSetCurrentCarrier(&fake_carrier2);
		ok = ok && *PgCurrentWaitEventWaitingRef() == false;
		ok = ok && *PgCurrentWaitEventSignalFdRef() == -1;
		ok = ok && *PgCurrentWaitEventSelfPipeReadFdRef() == -1;
		ok = ok && *PgCurrentWaitEventSelfPipeWriteFdRef() == -1;
		ok = ok && *PgCurrentWaitEventSelfPipeOwnerPidRef() == 0;
		ok = ok && *PgCurrentStackBasePtrRef() == NULL;
		ok = ok && *PgCurrentBackendThreadStartRef() == NULL;
		ok = ok && !IsUnderPostmaster;
		*PgCurrentWaitEventWaitingRef() = false;
		*PgCurrentWaitEventSignalFdRef() = 21;
		*PgCurrentWaitEventSelfPipeReadFdRef() = 22;
		*PgCurrentWaitEventSelfPipeWriteFdRef() = 23;
		*PgCurrentWaitEventSelfPipeOwnerPidRef() = 24;
		*PgCurrentStackBasePtrRef() = &stack_marker2;
		*PgCurrentBackendThreadStartRef() = thread_start2;
		IsUnderPostmaster = false;

		PgSetCurrentCarrier(&fake_carrier1);
		ok = ok && *PgCurrentWaitEventWaitingRef() == true;
		ok = ok && *PgCurrentWaitEventSignalFdRef() == 11;
		ok = ok && *PgCurrentWaitEventSelfPipeReadFdRef() == 12;
		ok = ok && *PgCurrentWaitEventSelfPipeWriteFdRef() == 13;
		ok = ok && *PgCurrentWaitEventSelfPipeOwnerPidRef() == 14;
		ok = ok && *PgCurrentStackBasePtrRef() == &stack_marker1;
		ok = ok && *PgCurrentBackendThreadStartRef() == thread_start1;
		ok = ok && IsUnderPostmaster;

		PgSetCurrentCarrier(&fake_carrier2);
		ok = ok && *PgCurrentWaitEventWaitingRef() == false;
		ok = ok && *PgCurrentWaitEventSignalFdRef() == 21;
		ok = ok && *PgCurrentWaitEventSelfPipeReadFdRef() == 22;
		ok = ok && *PgCurrentWaitEventSelfPipeWriteFdRef() == 23;
		ok = ok && *PgCurrentWaitEventSelfPipeOwnerPidRef() == 24;
		ok = ok && *PgCurrentStackBasePtrRef() == &stack_marker2;
		ok = ok && *PgCurrentBackendThreadStartRef() == thread_start2;
		ok = ok && !IsUnderPostmaster;

		PgSetCurrentCarrier(saved_carrier);
		IsUnderPostmaster = saved_is_under_postmaster;
	}
	PG_CATCH();
	{
		PgSetCurrentCarrier(saved_carrier);
		IsUnderPostmaster = saved_is_under_postmaster;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "carrier miscellaneous state was not carrier-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_carrier_threaded_guc_lock_depth_is_carrier_local);
Datum
test_carrier_threaded_guc_lock_depth_is_carrier_local(PG_FUNCTION_ARGS)
{
	PgCarrier  *saved_carrier;
	PgCarrier	fake_carrier1;
	PgCarrier	fake_carrier2;
	bool		ok = true;

	saved_carrier = CurrentPgCarrier;
	MemSet(&fake_carrier1, 0, sizeof(fake_carrier1));
	MemSet(&fake_carrier2, 0, sizeof(fake_carrier2));
	fake_carrier1.kind = PG_CARRIER_THREAD;
	fake_carrier2.kind = PG_CARRIER_THREAD;

	PG_TRY();
	{
		PgSetCurrentCarrier(&fake_carrier1);
		*PgCurrentThreadedGUCMutexDepthRef() = 1;
		PgSetCurrentCarrier(&fake_carrier2);
		ok = ok && *PgCurrentThreadedGUCMutexDepthRef() == 0;
		*PgCurrentThreadedGUCMutexDepthRef() = 2;
		PgSetCurrentCarrier(&fake_carrier1);
		ok = ok && *PgCurrentThreadedGUCMutexDepthRef() == 1;
		PgSetCurrentCarrier(&fake_carrier2);
		ok = ok && *PgCurrentThreadedGUCMutexDepthRef() == 2;

		PgSetCurrentCarrier(saved_carrier);
	}
	PG_CATCH();
	{
		PgSetCurrentCarrier(saved_carrier);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "threaded GUC mutex depth was not carrier-local");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_carrier_attach_detach_current_work);
Datum
test_carrier_attach_detach_current_work(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	MemoryContext saved_current_memory_context;
	ResourceOwner saved_current_resource_owner;
	PgThreadBackendRuntimeState state;
	PGPROC		fake_proc;
	Latch		fake_latch;
	WaitEventSet *fake_wait_set;
	MemoryContext fake_memory_context;
	MemoryContext saved_error_context;
	MemoryContext scheduler_memory_context;
	MemoryContext scheduler_error_context;
	ResourceOwner fake_resource_owner;
	ResourceOwner scheduler_resource_owner;
	bool		ok = true;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;
	saved_current_memory_context = CurrentMemoryContext;
	saved_error_context = ErrorContext;
	saved_current_resource_owner = CurrentResourceOwner;
	scheduler_memory_context = TopMemoryContext;
	scheduler_error_context = ErrorContext;
	scheduler_resource_owner = CurrentResourceOwner;

	InitializePgThreadRuntime(NULL);
	InitializePgThreadBackendRuntimeState(&state, B_BACKEND, NULL,
										  &fake_latch);
	PgCarrierDetachBackend(&state.carrier, &state.logical.backend);
	MemSet(&fake_proc, 0, sizeof(fake_proc));
	InitLatch(&fake_latch);
	fake_wait_set = (WaitEventSet *) &state.logical.connection;
	fake_memory_context = (MemoryContext) &state.logical.execution;
	fake_resource_owner = (ResourceOwner) &state.logical.execution;
	state.logical.backend.my_proc = &fake_proc;
	state.logical.backend.my_proc_number = 42;
	state.logical.backend.core.latch = &fake_latch;
	state.logical.backend.timeout.num_active_timeouts = 3;
	state.logical.connection.protocol.fe_be_wait_set = fake_wait_set;
	state.logical.execution.memory_contexts.current_context = fake_memory_context;
	state.logical.execution.resource_owners.current_owner =
		(struct ResourceOwnerData *) fake_resource_owner;
	ok = ok && state.carrier.scheduler_execution != NULL;
	if (state.carrier.scheduler_execution != NULL)
	{
		state.carrier.scheduler_execution->memory_contexts.top_context =
			TopMemoryContext;
		state.carrier.scheduler_execution->memory_contexts.current_context =
			scheduler_memory_context;
		state.carrier.scheduler_execution->memory_contexts.error_context =
			scheduler_error_context;
		state.carrier.scheduler_execution->resource_owners.current_owner =
			(struct ResourceOwnerData *) scheduler_resource_owner;
	}

	PG_TRY();
	{
		PgCarrierAttachBackend(&state.carrier, &state.logical.backend,
							   &state.logical.session, &state.logical.connection,
							   &state.logical.execution);

		ok = ok && CurrentPgRuntime == state.logical.backend.runtime;
		ok = ok && CurrentPgRuntime->current_carrier == &state.carrier;
		ok = ok && CurrentPgCarrier == &state.carrier;
		ok = ok && CurrentPgBackend == &state.logical.backend;
		ok = ok && CurrentPgSession == &state.logical.session;
		ok = ok && CurrentPgConnection == &state.logical.connection;
		ok = ok && CurrentPgExecution == &state.logical.execution;
		ok = ok && state.carrier.current_backend == &state.logical.backend;
		ok = ok && state.carrier.current_session == &state.logical.session;
		ok = ok && state.carrier.current_execution == &state.logical.execution;
		ok = ok && state.logical.backend.carrier == &state.carrier;
		ok = ok && state.logical.execution.carrier == &state.carrier;
		ok = ok && CurrentPgBackendTimeoutRuntimeState ==
			&state.logical.backend.timeout;
		ok = ok && CurrentPgConnectionProtocolRuntimeState ==
			&state.logical.connection.protocol;
		ok = ok && CurrentPgExecutionMemoryContextRuntimeState ==
			&state.logical.execution.memory_contexts;
		ok = ok && CurrentPgExecutionResourceOwnerRuntimeState ==
			&state.logical.execution.resource_owners;
		ok = ok && MyProc == &fake_proc;
		ok = ok && MyProcNumber == 42;
		ok = ok && MyLatch == &fake_latch;
		ok = ok && FeBeWaitSet == fake_wait_set;
		ok = ok && CurrentMemoryContext == fake_memory_context;
		ok = ok && CurrentResourceOwner == fake_resource_owner;
		ok = ok && PgCurrentTimeoutState() == &state.logical.backend.timeout;

		PgCarrierDetachBackend(&state.carrier, &state.logical.backend);

		ok = ok && CurrentPgRuntime == state.logical.backend.runtime;
		ok = ok && CurrentPgCarrier == &state.carrier;
		ok = ok && CurrentPgBackend == NULL;
		ok = ok && CurrentPgSession == NULL;
		ok = ok && CurrentPgConnection == NULL;
		ok = ok && CurrentPgExecution == NULL;
		ok = ok && state.carrier.current_backend == NULL;
		ok = ok && state.carrier.current_session == NULL;
		ok = ok && state.carrier.current_execution == NULL;
		ok = ok && state.logical.backend.carrier == NULL;
		ok = ok && state.logical.execution.carrier == NULL;
		ok = ok && CurrentPgBackendTimeoutRuntimeState == NULL;
		ok = ok && CurrentPgConnectionProtocolRuntimeState == NULL;
		ok = ok && CurrentPgExecutionMemoryContextRuntimeState == NULL;
		ok = ok && CurrentPgExecutionResourceOwnerRuntimeState == NULL;
		ok = ok && MyProc != &fake_proc;
		ok = ok && MyLatch != &fake_latch;
		ok = ok && FeBeWaitSet != fake_wait_set;
		ok = ok && CurrentMemoryContext == scheduler_memory_context;
		ok = ok && ErrorContext == scheduler_error_context;
		ok = ok && CurrentResourceOwner == scheduler_resource_owner;

		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
		CurrentResourceOwner = saved_current_resource_owner;
	}
	PG_CATCH();
	{
		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		ErrorContext = saved_error_context;
		CurrentResourceOwner = saved_current_resource_owner;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "carrier attach/detach did not refresh current work");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_current_work_snapshot_lazy_restore);
Datum
test_current_work_snapshot_lazy_restore(PG_FUNCTION_ARGS)
{
	PgRuntime  *saved_runtime;
	PgCarrier  *saved_carrier;
	PgBackend  *saved_backend;
	PgSession  *saved_session;
	PgConnection *saved_connection;
	PgExecution *saved_execution;
	MemoryContext saved_current_memory_context;
	ResourceOwner saved_current_resource_owner;
	PgThreadBackendRuntimeState state_a;
	PgThreadBackendRuntimeState state_b;
	PGPROC		fake_proc_a;
	PGPROC		fake_proc_b;
	Latch		fake_latch_a;
	Latch		fake_latch_b;
	PgCurrentWorkSnapshot snap;
	bool		ok = true;

	saved_runtime = CurrentPgRuntime;
	saved_carrier = CurrentPgCarrier;
	saved_backend = CurrentPgBackend;
	saved_session = CurrentPgSession;
	saved_connection = CurrentPgConnection;
	saved_execution = CurrentPgExecution;
	saved_current_memory_context = CurrentMemoryContext;
	saved_current_resource_owner = CurrentResourceOwner;

	InitializePgThreadRuntime(NULL);
	InitializePgThreadBackendRuntimeState(&state_a, B_BACKEND, NULL,
										  &fake_latch_a);
	InitializePgThreadBackendRuntimeState(&state_b, B_BACKEND, NULL,
										  &fake_latch_b);
	PgCarrierDetachBackend(&state_a.carrier, &state_a.logical.backend);
	PgCarrierDetachBackend(&state_b.carrier, &state_b.logical.backend);
	MemSet(&fake_proc_a, 0, sizeof(fake_proc_a));
	MemSet(&fake_proc_b, 0, sizeof(fake_proc_b));
	InitLatch(&fake_latch_a);
	InitLatch(&fake_latch_b);
	state_a.logical.backend.my_proc = &fake_proc_a;
	state_a.logical.backend.my_proc_number = 101;
	state_a.logical.backend.core.latch = &fake_latch_a;
	state_b.logical.backend.my_proc = &fake_proc_b;
	state_b.logical.backend.my_proc_number = 102;
	state_b.logical.backend.core.latch = &fake_latch_b;

	PG_TRY();
	{
		/* Fiber A becomes current. */
		PgCarrierAttachBackend(&state_a.carrier, &state_a.logical.backend,
							   &state_a.logical.session,
							   &state_a.logical.connection,
							   &state_a.logical.execution);
		ok = ok && CurrentPgBackend == &state_a.logical.backend;
		ok = ok && MyProc == &fake_proc_a;
		ok = ok && MyProcNumber == 101;
		ok = ok && MyLatch == &fake_latch_a;

		/*
		 * Fiber A is about to park: the chained hook's save captures A's six
		 * roots.  (We call the same public entry the hook uses.)
		 */
		PgRuntimeSaveCurrentWork(&snap);
		ok = ok && snap.backend == &state_a.logical.backend;
		ok = ok && snap.session == &state_a.logical.session;
		ok = ok && snap.carrier == &state_a.carrier;

		/*
		 * The scheduler runs sibling fiber B on this OS thread, clobbering the
		 * thread-local current-work bridge with B's roots.
		 */
		PgCarrierAttachBackend(&state_b.carrier, &state_b.logical.backend,
							   &state_b.logical.session,
							   &state_b.logical.connection,
							   &state_b.logical.execution);
		ok = ok && CurrentPgBackend == &state_b.logical.backend;
		ok = ok && CurrentPgSession == &state_b.logical.session;
		ok = ok && MyProc == &fake_proc_b;
		ok = ok && MyProcNumber == 102;
		ok = ok && MyLatch == &fake_latch_b;

		/*
		 * GUC-coherence check (the crux of the lazy restore): the session-
		 * rooted GUC bucket that every session-local GUC read resolves through
		 * (PgCurrentSessionGUCState / PgCurrentGUCVariablesRef) must now point
		 * at B's session GUC state, not A's -- even though no rebind ran.
		 */
		ok = ok && PgCurrentSessionGUCState() == &state_b.logical.session.guc;

		/*
		 * Fiber A resumes: the chained hook's restore reinstalls A's roots
		 * WITHOUT a GUC rebind.  Verify every root is A's again and that the
		 * compatibility globals derived from them track A.
		 */
		PgRuntimeRestoreCurrentWorkLazy(&snap);
		ok = ok && CurrentPgRuntime == snap.runtime;
		ok = ok && CurrentPgCarrier == &state_a.carrier;
		ok = ok && CurrentPgBackend == &state_a.logical.backend;
		ok = ok && CurrentPgSession == &state_a.logical.session;
		ok = ok && CurrentPgConnection == &state_a.logical.connection;
		ok = ok && CurrentPgExecution == &state_a.logical.execution;
		ok = ok && MyProc == &fake_proc_a;
		ok = ok && MyProcNumber == 101;
		ok = ok && MyLatch == &fake_latch_a;

		/*
		 * And the GUC read path is re-rooted back to A after the LAZY restore:
		 * the session-local GUC bucket resolves to A's session GUC state, so a
		 * session-local GUC read on the resumed fiber sees A's values without
		 * any per-switch rebind having run.  This is the direct GUC-coherence
		 * coverage for PgRuntimeRestoreCurrentWorkLazy itself (companion test
		 * test_session_connection_guc_state_is_session_local exercises real
		 * GUC *values* across the identical rebind=false refresh).
		 */
		ok = ok && PgCurrentSessionGUCState() == &state_a.logical.session.guc;

		/* Detach whichever backend is current and restore the real work. */
		if (CurrentPgCarrier == &state_a.carrier &&
			state_a.carrier.current_backend != NULL)
			PgCarrierDetachBackend(&state_a.carrier, &state_a.logical.backend);
		if (state_b.carrier.current_backend != NULL)
			PgCarrierDetachBackend(&state_b.carrier, &state_b.logical.backend);

		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
	}
	PG_CATCH();
	{
		PgRuntimeSetCurrentWork(saved_runtime, saved_carrier, saved_backend,
								saved_session, saved_connection,
								saved_execution, false);
		CurrentMemoryContext = saved_current_memory_context;
		CurrentResourceOwner = saved_current_resource_owner;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "current-work lazy restore did not preserve fiber-local roots");

	PG_RETURN_BOOL(true);
}
