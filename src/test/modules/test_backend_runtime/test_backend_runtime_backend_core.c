/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_backend_core.c
 *		Core backend runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_backend_core.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

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
		PgSetCurrentBackend(&fake_backend1);
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

		PgSetCurrentBackend(&fake_backend2);
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

		PgSetCurrentBackend(&fake_backend1);
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

		PgSetCurrentBackend(&fake_backend2);
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

		PgSetCurrentBackend(saved_backend);
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
		PgSetCurrentBackend(saved_backend);
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
		PgSetCurrentBackend(&fake_backend1);
		PgSetCurrentSession(&fake_session1);
		*PgCurrentDoingCommandReadRef() = true;
		*PgCurrentUserDOptionRef() = "data-one";
		PgCurrentUsageSaveRusageRef()->ru_inblock = 11;
		PgCurrentUsageSaveTimevalRef()->tv_sec = 12;
		strlcpy(PgCurrentFormattedStartTimeBuffer(), "start-one",
				PG_BACKEND_FORMATTED_TS_LEN);
		*PgCurrentLogLineNumberRef() = 13;
		*PgCurrentLogLinePidRef() = 14;

		PgSetCurrentBackend(&fake_backend2);
		PgSetCurrentSession(&fake_session2);
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

		PgSetCurrentBackend(&fake_backend1);
		PgSetCurrentSession(&fake_session1);
		ok = ok && *PgCurrentDoingCommandReadRef();
		ok = ok && strcmp(*PgCurrentUserDOptionRef(), "data-one") == 0;
		ok = ok && PgCurrentUsageSaveRusageRef()->ru_inblock == 11;
		ok = ok && PgCurrentUsageSaveTimevalRef()->tv_sec == 12;
		ok = ok && strcmp(PgCurrentFormattedStartTimeBuffer(), "start-one") == 0;
		ok = ok && *PgCurrentLogLineNumberRef() == 13;
		ok = ok && *PgCurrentLogLinePidRef() == 14;

		PgSetCurrentBackend(&fake_backend2);
		PgSetCurrentSession(&fake_session2);
		ok = ok && !*PgCurrentDoingCommandReadRef();
		ok = ok && strcmp(*PgCurrentUserDOptionRef(), "data-two") == 0;
		ok = ok && PgCurrentUsageSaveRusageRef()->ru_inblock == 21;
		ok = ok && PgCurrentUsageSaveTimevalRef()->tv_sec == 22;
		ok = ok && strcmp(PgCurrentFormattedStartTimeBuffer(), "start-two") == 0;
		ok = ok && *PgCurrentLogLineNumberRef() == 23;
		ok = ok && *PgCurrentLogLinePidRef() == 24;

		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PgSetCurrentSession(saved_session);
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
	PgBackendExprEvalOpLookup reverse_dispatch_one[1];
	PgBackendExprEvalOpLookup reverse_dispatch_two[1];
	bool		ok = true;

	saved_backend = CurrentPgBackend;
	MemSet(&fake_backend1, 0, sizeof(fake_backend1));
	MemSet(&fake_backend2, 0, sizeof(fake_backend2));
	MemSet(reverse_dispatch_one, 0, sizeof(reverse_dispatch_one));
	MemSet(reverse_dispatch_two, 0, sizeof(reverse_dispatch_two));
	dispatch_one[0] = &fake_backend1;
	dispatch_two[0] = &fake_backend2;

	PG_TRY();
	{
		PgSetCurrentBackend(&fake_backend1);
		PgCurrentExprInterpState()->dispatch_table = dispatch_one;
		PgCurrentExprInterpState()->reverse_dispatch_table = reverse_dispatch_one;
		reverse_dispatch_one[0].opcode = &fake_backend1;
		reverse_dispatch_one[0].op = 11;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && PgCurrentExprInterpState()->dispatch_table == NULL;
		ok = ok && PgCurrentExprInterpState()->reverse_dispatch_table == NULL;
		PgCurrentExprInterpState()->dispatch_table = dispatch_two;
		PgCurrentExprInterpState()->reverse_dispatch_table = reverse_dispatch_two;
		reverse_dispatch_two[0].opcode = &fake_backend2;
		reverse_dispatch_two[0].op = 22;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && PgCurrentExprInterpState()->dispatch_table == dispatch_one;
		ok = ok && PgCurrentExprInterpState()->reverse_dispatch_table ==
			reverse_dispatch_one;
		ok = ok && reverse_dispatch_one[0].opcode == &fake_backend1;
		ok = ok && reverse_dispatch_one[0].op == 11;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && PgCurrentExprInterpState()->dispatch_table == dispatch_two;
		ok = ok && PgCurrentExprInterpState()->reverse_dispatch_table ==
			reverse_dispatch_two;
		ok = ok && reverse_dispatch_two[0].opcode == &fake_backend2;
		ok = ok && reverse_dispatch_two[0].op == 22;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend expression interpreter state was not backend-local");

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

	PgSetCurrentBackend(&fake_backend);
	pending_seen = PgCurrentBackendHasPendingInterrupts();
	PgSetCurrentBackend(saved_backend);

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
