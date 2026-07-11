/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_pmchild.c
 *		PMChild logical-backend publication tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_pmchild.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

typedef struct TestPMChildLogicalBackendRace
{
	PMChild    *pmchild;
	pg_atomic_uint32 start;
	pg_atomic_uint32 stop;
	pg_atomic_uint32 ready_count;
	pg_atomic_uint32 attempts;
	pg_atomic_uint32 hits;
	pg_atomic_uint32 saw_live_signal_pid;
} TestPMChildLogicalBackendRace;

static void
test_pmchild_install_stale_thread_payload(PMChild *pmchild,
										  PgBackend *backend)
{
	pmchild->logical_signal_pid = 44444;
	pmchild->logical_backend = backend;
	pmchild->thread_exitstatus = 99;
	pmchild->thread_exit_logical_signal_pid = 55555;
	pmchild->thread_exit_top_memory_allocated = 16384;
	pmchild->thread_exit_top_memory_reclaimed = 32768;
	pg_atomic_write_u32(&pmchild->thread_startup_complete, 1);
	pg_atomic_write_u32(&pmchild->thread_exited, 1);
}

static bool
test_pmchild_thread_payload_is_clear(PMChild *pmchild)
{
	int			exitstatus;
	pid_t		exit_signal_pid;
	Size		top_memory_allocated;
	Size		top_memory_reclaimed;

	if (pmchild->logical_backend != NULL)
		return false;
	if (pmchild->thread_exitstatus != 0)
		return false;
	if (pmchild->thread_exit_logical_signal_pid != 0)
		return false;
	if (pmchild->thread_exit_top_memory_allocated != 0)
		return false;
	if (pmchild->thread_exit_top_memory_reclaimed != 0)
		return false;
	if (PostmasterChildHasStartupComplete(pmchild))
		return false;
	if (PostmasterChildHasExitedThread(pmchild, &exitstatus,
									   &top_memory_allocated,
									   &top_memory_reclaimed,
									   &exit_signal_pid))
		return false;
	if (PostmasterChildHasExitedPooledLogical(pmchild, &exitstatus,
											 &top_memory_allocated,
											 &top_memory_reclaimed,
											 &exit_signal_pid))
		return false;

	return true;
}

static void
test_pmchild_logical_backend_reader_routine(void *arg)
{
	TestPMChildLogicalBackendRace *state = (TestPMChildLogicalBackendRace *) arg;

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
	Size		top_memory_reclaimed;
	bool		ok = true;

	MemSet(&fake_runtime, 0, sizeof(fake_runtime));
	MemSet(&fake_backend, 0, sizeof(fake_backend));
	MemSet(&fake_pmchild, 0, sizeof(fake_pmchild));
	MemSet(&fake_thread, 0, sizeof(fake_thread));

	fake_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
	fake_backend.id = 12345;
	fake_backend.runtime = &fake_runtime;
	PgBackendInitializeInterrupts(&fake_backend);
	fake_pmchild.logical_signal_pid = 54321;
	fake_pmchild.thread_exitstatus = 99;
	fake_pmchild.thread_exit_top_memory_allocated = 16384;
	fake_pmchild.thread_exit_top_memory_reclaimed = 32768;
	fake_pmchild.carrier_kind = PM_CHILD_CARRIER_THREAD;
	InitLatch(&fake_latch);

	PostmasterChildSetThread(&fake_pmchild, &fake_thread);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && !PostmasterChildHasStartupComplete(&fake_pmchild);
	PostmasterChildPublishThreadStartupComplete(&fake_pmchild, &fake_latch);
	ok = ok && PostmasterChildHasStartupComplete(&fake_pmchild);
	ok = ok && !PostmasterChildHasStartupComplete(&fake_pmchild);
	ok = ok && !PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											   &top_memory_allocated,
											   &top_memory_reclaimed,
											   &exit_signal_pid);

	PostmasterChildPublishLogicalBackend(&fake_pmchild, &fake_backend);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 12345;
	ok = ok && PostmasterChildRaiseThreadInterrupt(&fake_pmchild,
												   PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	pending = PgBackendConsumeInterrupts(&fake_backend);
	ok = ok && (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL));

	PostmasterChildPublishThreadExit(&fake_pmchild, 17, 8192, 32768,
									 &fake_latch);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											  &top_memory_allocated,
											  &top_memory_reclaimed,
											  &exit_signal_pid);
	ok = ok && exitstatus == 17;
	ok = ok && exit_signal_pid == 12345;
	ok = ok && top_memory_allocated == 8192;
	ok = ok && top_memory_reclaimed == 32768;
	ok = ok && !PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											   &top_memory_allocated,
											   &top_memory_reclaimed,
											   &exit_signal_pid);
	PostmasterChildRetryThreadExit(&fake_pmchild);
	ok = ok && PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											  &top_memory_allocated,
											  &top_memory_reclaimed,
											  &exit_signal_pid);
	ok = ok && exitstatus == 17;
	ok = ok && exit_signal_pid == 12345;
	ok = ok && top_memory_allocated == 8192;
	ok = ok && top_memory_reclaimed == 32768;
	ok = ok && !PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											   &top_memory_allocated,
											   &top_memory_reclaimed,
											   &exit_signal_pid);
	ok = ok && !PostmasterChildRaiseThreadInterrupt(&fake_pmchild,
													PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	ok = ok && !PostmasterChildWakeThreadBackend(&fake_pmchild);

	PostmasterChildSetThread(&fake_pmchild, &fake_thread);
	PostmasterChildPublishLogicalBackend(&fake_pmchild, &fake_backend);
	PostmasterChildUnpublishLogicalBackend(&fake_pmchild);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && fake_pmchild.thread_exit_logical_signal_pid == 12345;
	ok = ok && !PostmasterChildRaiseThreadInterrupt(&fake_pmchild,
													PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	PostmasterChildPublishThreadExit(&fake_pmchild, 23, 4096, 2048,
									 &fake_latch);
	ok = ok && PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											  &top_memory_allocated,
											  &top_memory_reclaimed,
											  &exit_signal_pid);
	ok = ok && exitstatus == 23;
	ok = ok && exit_signal_pid == 12345;
	ok = ok && top_memory_allocated == 4096;
	ok = ok && top_memory_reclaimed == 2048;

	if (!ok)
		elog(ERROR, "PMChild thread-backend signal API failed");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_pmchild_thread_backend_reset_api);
Datum
test_pmchild_thread_backend_reset_api(PG_FUNCTION_ARGS)
{
	PgRuntime	fake_runtime;
	PgBackend	fake_backend;
	PMChild		fake_pmchild;
	PgThread	fake_thread;
	Latch		fake_latch;
	int			exitstatus;
	pid_t		exit_signal_pid;
	Size		top_memory_allocated;
	Size		top_memory_reclaimed;
	bool		ok = true;

	MemSet(&fake_runtime, 0, sizeof(fake_runtime));
	MemSet(&fake_backend, 0, sizeof(fake_backend));
	MemSet(&fake_pmchild, 0, sizeof(fake_pmchild));
	MemSet(&fake_thread, 0, sizeof(fake_thread));

	fake_runtime.kind = PG_RUNTIME_THREAD_PER_SESSION;
	fake_backend.id = 12345;
	fake_backend.runtime = &fake_runtime;
	PgBackendInitializeInterrupts(&fake_backend);
	fake_pmchild.carrier_kind = PM_CHILD_CARRIER_THREAD;
	InitLatch(&fake_latch);

	test_pmchild_install_stale_thread_payload(&fake_pmchild, &fake_backend);
	PostmasterChildSetProcess(&fake_pmchild, 24680);
	ok = ok && PostmasterChildIsProcess(&fake_pmchild);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 24680;
	ok = ok && test_pmchild_thread_payload_is_clear(&fake_pmchild);

	test_pmchild_install_stale_thread_payload(&fake_pmchild, &fake_backend);
	PostmasterChildSetThread(&fake_pmchild, &fake_thread);
	ok = ok && PostmasterChildIsThread(&fake_pmchild);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && test_pmchild_thread_payload_is_clear(&fake_pmchild);

	PostmasterChildPublishLogicalBackend(&fake_pmchild, &fake_backend);
	PostmasterChildPublishThreadStartupComplete(&fake_pmchild, &fake_latch);
	PostmasterChildPublishThreadExit(&fake_pmchild, 17, 8192, 32768,
									 &fake_latch);
	ok = ok && PostmasterChildHasStartupComplete(&fake_pmchild);
	ok = ok && PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											  &top_memory_allocated,
											  &top_memory_reclaimed,
											  &exit_signal_pid);
	ok = ok && exitstatus == 17;
	ok = ok && exit_signal_pid == 12345;
	ok = ok && top_memory_allocated == 8192;
	ok = ok && top_memory_reclaimed == 32768;

	PostmasterChildSetThread(&fake_pmchild, &fake_thread);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && test_pmchild_thread_payload_is_clear(&fake_pmchild);

	if (!ok)
		elog(ERROR, "PMChild thread-backend reset API failed");

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(test_pmchild_pooled_logical_backend_signal_api);
Datum
test_pmchild_pooled_logical_backend_signal_api(PG_FUNCTION_ARGS)
{
	PgRuntime	fake_runtime;
	PgBackend	fake_backend;
	PMChild		fake_pmchild;
	Latch		fake_latch;
	PgBackendInterruptMask pending;
	int			exitstatus;
	pid_t		exit_signal_pid;
	Size		top_memory_allocated;
	Size		top_memory_reclaimed;
	bool		ok = true;

	MemSet(&fake_runtime, 0, sizeof(fake_runtime));
	MemSet(&fake_backend, 0, sizeof(fake_backend));
	MemSet(&fake_pmchild, 0, sizeof(fake_pmchild));
	MemSet(&fake_latch, 0, sizeof(fake_latch));

	fake_runtime.kind = PG_RUNTIME_POOLED_PROTOCOL;
	fake_backend.id = 23456;
	fake_backend.runtime = &fake_runtime;
	PgBackendInitializeInterrupts(&fake_backend);
	InitLatch(&fake_latch);

	test_pmchild_install_stale_thread_payload(&fake_pmchild, &fake_backend);
	PostmasterChildSetPooledLogical(&fake_pmchild);
	ok = ok && PostmasterChildIsPooledLogical(&fake_pmchild);
	ok = ok && PostmasterChildHasLogicalBackendPublication(&fake_pmchild);
	ok = ok && !PostmasterChildIsProcess(&fake_pmchild);
	ok = ok && !PostmasterChildIsThread(&fake_pmchild);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && test_pmchild_thread_payload_is_clear(&fake_pmchild);

	PostmasterChildPublishLogicalBackend(&fake_pmchild, &fake_backend);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 23456;
	ok = ok && PostmasterChildRaiseThreadInterrupt(&fake_pmchild,
												   PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	ok = ok && PostmasterChildWakeThreadBackend(&fake_pmchild);
	pending = PgBackendConsumeInterrupts(&fake_backend);
	ok = ok && (pending & PG_BACKEND_INTERRUPT_MASK(PG_BACKEND_INTERRUPT_QUERY_CANCEL));
	ok = ok && !PostmasterChildHasStartupComplete(&fake_pmchild);
	PostmasterChildPublishLogicalStartupComplete(&fake_pmchild, &fake_latch);
	ok = ok && PostmasterChildHasStartupComplete(&fake_pmchild);
	ok = ok && !PostmasterChildHasStartupComplete(&fake_pmchild);
	ok = ok && !PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											   &top_memory_allocated,
											   &top_memory_reclaimed,
											   &exit_signal_pid);
	ok = ok && !PostmasterChildHasExitedPooledLogical(&fake_pmchild,
													  &exitstatus,
													  &top_memory_allocated,
													  &top_memory_reclaimed,
													  &exit_signal_pid);

	PostmasterChildPublishPooledLogicalExit(&fake_pmchild, 19, 4096, 8192,
											&fake_latch);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && !PostmasterChildHasExitedThread(&fake_pmchild, &exitstatus,
											   &top_memory_allocated,
											   &top_memory_reclaimed,
											   &exit_signal_pid);
	ok = ok && PostmasterChildHasExitedPooledLogical(&fake_pmchild,
													 &exitstatus,
													 &top_memory_allocated,
													 &top_memory_reclaimed,
													 &exit_signal_pid);
	ok = ok && exitstatus == 19;
	ok = ok && exit_signal_pid == 23456;
	ok = ok && top_memory_allocated == 4096;
	ok = ok && top_memory_reclaimed == 8192;
	ok = ok && !PostmasterChildHasExitedPooledLogical(&fake_pmchild,
													  &exitstatus,
													  &top_memory_allocated,
													  &top_memory_reclaimed,
													  &exit_signal_pid);
	ok = ok && !PostmasterChildRaiseThreadInterrupt(&fake_pmchild,
													PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	ok = ok && !PostmasterChildWakeThreadBackend(&fake_pmchild);

	PostmasterChildSetPooledLogical(&fake_pmchild);
	PostmasterChildPublishLogicalBackend(&fake_pmchild, &fake_backend);
	PostmasterChildUnpublishLogicalBackend(&fake_pmchild);
	ok = ok && PostmasterChildSignalPid(&fake_pmchild) == 0;
	ok = ok && fake_pmchild.thread_exit_logical_signal_pid == 0;
	ok = ok && !PostmasterChildRaiseThreadInterrupt(&fake_pmchild,
													PG_BACKEND_INTERRUPT_QUERY_CANCEL);
	ok = ok && !PostmasterChildWakeThreadBackend(&fake_pmchild);

	if (!ok)
		elog(ERROR, "PMChild pooled logical backend signal API failed");

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
	TestPMChildLogicalBackendRace race_state;
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
		Size		top_memory_reclaimed;

		PostmasterChildSetThread(&fake_pmchild, &fake_pmthread);

		for (int i = 0; i < TEST_PMCHILD_READER_THREADS; i++)
		{
			int			rc;

			rc = pg_thread_create(&reader_threads[i], "pmchild reader",
								  test_pmchild_logical_backend_reader_routine,
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
			PostmasterChildPublishLogicalBackend(&fake_pmchild,
												 &fake_backend);
			/* Make the reader-side observation deterministic on fast runs. */
			if (pg_atomic_read_u32(&race_state.hits) == 0)
			{
				for (int spins = 0;
					 spins < 1000 &&
					 pg_atomic_read_u32(&race_state.hits) == 0;
					 spins++)
					pg_usleep(100L);
			}
			PostmasterChildUnpublishLogicalBackend(&fake_pmchild);
			PostmasterChildPublishThreadExit(&fake_pmchild, i,
											 (Size) i * 16,
											 (Size) i * 32, &fake_latch);
			ok = ok && PostmasterChildHasExitedThread(&fake_pmchild,
													  &exitstatus,
													  &top_memory_allocated,
													  &top_memory_reclaimed,
													  &exit_signal_pid);
			ok = ok && exitstatus == i;
			ok = ok && top_memory_allocated == (Size) i * 16;
			ok = ok && top_memory_reclaimed == (Size) i * 32;
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
