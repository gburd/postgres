/*--------------------------------------------------------------------------
 *
 * test_backend_runtime_dsm.c
 *		DSM-owned backend runtime state tests.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_backend_runtime/test_backend_runtime_dsm.c
 *
 * -------------------------------------------------------------------------
 */
#include "test_backend_runtime.h"

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
	/* DSM allocation reports wait events through backend-local wait state. */
	fake_backend_with_dsm.wait_state.wait_event_info_ptr =
		&fake_backend_with_dsm.wait_state.local_wait_event_info;
	fake_backend_to_exit.wait_state.wait_event_info_ptr =
		&fake_backend_to_exit.wait_state.local_wait_event_info;

	/*
	 * dsm_create()/dsm_backend_shutdown() take the shared
	 * DynamicSharedMemoryControlLock, and LWLockAcquire asserts that a
	 * backend running under the postmaster always carries a PGPROC
	 * (!(MyProc == NULL && IsUnderPostmaster)).  MyProc resolves through
	 * CurrentPgBackend, so the fake backends we install here must keep the
	 * real process backend's PGPROC -- this test isolates only DSM mapping
	 * ownership, not proc identity.  (In a non-assert build the immediate,
	 * uncontended lock acquisition never dereferences proc, which is why the
	 * release gate passed with a NULL my_proc; the assert correctly flags the
	 * missing invariant under cassert.)
	 */
	fake_backend_with_dsm.my_proc = MyProc;
	fake_backend_to_exit.my_proc = MyProc;

	PG_TRY();
	{
		/*
		 * Simulate two logical backends in one address space.  Only
		 * CurrentPgBackend is switched because this test isolates DSM mapping
		 * ownership; the rest of the current process runtime remains real.
		 */
		PgSetCurrentBackend(&fake_backend_with_dsm);
		pg_prng_seed(&pg_global_prng_state, 1);
		seg = dsm_create(1024, 0);
		dsm_pin_mapping(seg);
		handle = dsm_segment_handle(seg);

		PgSetCurrentBackend(&fake_backend_to_exit);
		dsm_backend_shutdown();

		PgSetCurrentBackend(&fake_backend_with_dsm);
		found = (dsm_find_mapping(handle) == seg);
		dsm_detach(seg);
		seg = NULL;

		PgSetCurrentBackend(saved_backend);
	}
	PG_CATCH();
	{
		if (seg != NULL)
		{
			PgSetCurrentBackend(&fake_backend_with_dsm);
			dsm_detach(seg);
		}
		PgSetCurrentBackend(saved_backend);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (!found)
		elog(ERROR, "DSM shutdown for one backend detached another backend's mapping");

	PG_RETURN_BOOL(true);
}
