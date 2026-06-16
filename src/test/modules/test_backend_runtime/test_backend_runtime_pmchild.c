/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_pmchild.c
 *		PMChild thread-backend publication tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_pmchild.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

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
	ok = ok && !PostmasterChildHasStartupComplete(&fake_pmchild);
	PostmasterChildPublishThreadStartupComplete(&fake_pmchild, &fake_latch);
	ok = ok && PostmasterChildHasStartupComplete(&fake_pmchild);
	ok = ok && !PostmasterChildHasStartupComplete(&fake_pmchild);
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
