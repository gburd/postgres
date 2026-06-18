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
