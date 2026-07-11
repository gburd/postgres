/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_backend_interrupt.c
 *		Backend interrupt and exit-state runtime tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_backend_interrupt.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

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
		PgSetCurrentBackend(&fake_backend1);
		HOLD_INTERRUPTS();
		HOLD_CANCEL_INTERRUPTS();
		START_CRIT_SECTION();

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && InterruptHoldoffCount == 0;
		ok = ok && QueryCancelHoldoffCount == 0;
		ok = ok && CritSectionCount == 0;
		InterruptHoldoffCount = 3;
		QueryCancelHoldoffCount = 4;
		CritSectionCount = 5;

		PgSetCurrentBackend(&fake_backend1);
		ok = ok && InterruptHoldoffCount == 1;
		ok = ok && QueryCancelHoldoffCount == 1;
		ok = ok && CritSectionCount == 1;
		END_CRIT_SECTION();
		RESUME_CANCEL_INTERRUPTS();
		RESUME_INTERRUPTS();
		ok = ok && InterruptHoldoffCount == 0;
		ok = ok && QueryCancelHoldoffCount == 0;
		ok = ok && CritSectionCount == 0;

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && InterruptHoldoffCount == 3;
		ok = ok && QueryCancelHoldoffCount == 4;
		ok = ok && CritSectionCount == 5;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
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
		PgSetCurrentBackend(&fake_backend1);
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

		PgSetCurrentBackend(&fake_backend2);
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

		PgSetCurrentBackend(&fake_backend1);
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

		PgSetCurrentBackend(&fake_backend2);
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

		PgSetCurrentBackend(saved_backend);
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
		PgSetCurrentBackend(saved_backend);
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
		PgSetCurrentBackend(&fake_backend1);
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

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && !proc_exit_inprogress;
		ok = ok && !shmem_exit_inprogress;
		ok = ok && !PgBackendExitInProgress();
		ok = ok && !PgBackendShmemExitInProgress();

		proc_exit_inprogress = false;
		shmem_exit_inprogress = false;

		PgSetCurrentBackend(&fake_backend1);
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

		PgSetCurrentBackend(&fake_backend2);
		ok = ok && !proc_exit_inprogress;
		ok = ok && !shmem_exit_inprogress;

		PgSetCurrentBackend(saved_backend);
		proc_exit_inprogress = saved_proc_exit_flag;
		shmem_exit_inprogress = saved_shmem_exit_flag;
	}
	PG_CATCH();
	{
		PgSetCurrentBackend(saved_backend);
		proc_exit_inprogress = saved_proc_exit_flag;
		shmem_exit_inprogress = saved_shmem_exit_flag;
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!ok)
		elog(ERROR, "backend exit state was not backend-local");

	PG_RETURN_BOOL(true);
}

