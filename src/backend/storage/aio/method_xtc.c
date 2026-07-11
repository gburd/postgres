/*-------------------------------------------------------------------------
 *
 * method_xtc.c
 *    AIO - route data-file IO through libxtc's async file path when the
 *    issuing backend runs as an xtc fiber (USE_XTC_CARRIER).
 *
 * This is a "wrap" at the IoMethodOps vtable seam (item #6 of AGENTS_XTC.md;
 * see plan_docs/XTC_AIO_DESIGN.md).  It does not change any pgaio_io_* call
 * site.  When the issuer is an xtc backend fiber, xtc_aio_preadv/pwritev run
 * the IO on the xtc loop (parking the fiber, not blocking the carrier thread)
 * and return the total byte count or a negative errno -- the same iovec
 * signature and convention as pg_preadv/pg_pwritev.  The IO is completed by
 * the issuer before submit() returns, exactly like method_sync, so there is
 * nothing left in flight and no reordering.
 *
 * Off a fiber (process mode, aux backends, or any backend not on a carrier
 * loop) needs_synchronous_execution() returns true, so the executor uses the
 * existing synchronous fallback (pgaio_io_perform_synchronously()) and this
 * method's submit() is never reached -- process mode is byte-for-byte
 * unchanged.
 *
 * Because libxtc v1.2.0 provides vectored xtc_aio_preadv/pwritev (the async-
 * fiber analog of preadv(2)/pwritev(2)), submit() mirrors
 * pgaio_io_perform_synchronously() almost line for line -- the only change
 * from the synchronous path is which preadv/pwritev is called.  Issuer-async
 * reap is Step 3; WAL/fsync is Step 4 (see the design doc).
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/method_xtc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#ifdef USE_XTC_CARRIER

#include "postmaster/pg_xtc_carrier.h"	/* xtc_in_backend_fiber */
#include "storage/aio.h"
#include "storage/aio_internal.h"
#include "utils/backend_runtime.h"	/* PgCurrentWorkSnapshot, save/restore */
#include "utils/wait_event.h"

#include "xtc_aio.h"					/* xtc_aio_preadv / xtc_aio_pwritev */

static bool pgaio_xtc_needs_synchronous_execution(PgAioHandle *ioh);
static int	pgaio_xtc_submit(uint16 num_staged_ios, PgAioHandle **staged_ios);

const IoMethodOps pgaio_xtc_ops = {
	.needs_synchronous_execution = pgaio_xtc_needs_synchronous_execution,
	.submit = pgaio_xtc_submit,
};

/*
 * The xtc method handles an IO when the issuer is running as an xtc backend
 * fiber and the op is READV or WRITEV (any iovec length).  Off a fiber, or for
 * an invalid op, fall back to the synchronous method -- which preserves the
 * existing behavior exactly (process mode, aux backends).
 */
static bool
pgaio_xtc_needs_synchronous_execution(PgAioHandle *ioh)
{
	if (!xtc_in_backend_fiber)
		return true;

	switch ((PgAioOp) ioh->op)
	{
		case PGAIO_OP_READV:
		case PGAIO_OP_WRITEV:
			return false;
		case PGAIO_OP_INVALID:
			return true;
	}
	return true;
}

/*
 * Execute each staged IO on the xtc loop and complete it immediately.  Called
 * in a critical section (like every submit()).  Because the fiber parks on the
 * xtc loop for the duration of the IO rather than blocking the carrier thread,
 * sibling fibers on the same loop keep running -- but the issuer's own handle
 * is COMPLETED_* before this returns, so there is nothing in flight and
 * pgaio_wref_wait() sees it done with no possible reordering.
 *
 * This mirrors pgaio_io_perform_synchronously() (aio_io.c); the only
 * difference is the vectored call -- xtc_aio_preadv/pwritev instead of
 * pg_preadv/pg_pwritev -- so the iovec, offset, result, and completion
 * handling are identical to the synchronous method.
 */
static int
pgaio_xtc_submit(uint16 num_staged_ios, PgAioHandle **staged_ios)
{
	for (int i = 0; i < num_staged_ios; i++)
	{
		PgAioHandle *ioh = staged_ios[i];
		struct iovec *iov = &pgaio_ctl->iovecs[ioh->iovec_off];
		int			result = 0;

		/*
		 * needs_synchronous_execution() guarantees we only see READV/WRITEV
		 * issued from a backend fiber.
		 */
		Assert(xtc_in_backend_fiber);

		/*
		 * Advance the handle to SUBMITTED before running the IO, exactly like
		 * every other method's submit().  Skipping this leaves the handle IDLE
		 * and pgaio_io_process_completion() PANICs ("waiting for own IO in
		 * wrong state: IDLE").
		 */
		pgaio_io_prepare_submit(ioh);

		/*
		 * xtc_aio_preadv/pwritev PARK the issuing fiber for the duration of the
		 * IO (they yield to the xtc loop rather than blocking the carrier
		 * thread), so the loop may run OTHER backend fibers on this OS thread
		 * meanwhile.  That clobbers PG's per-backend current-work thread-locals
		 * (the hot-field refs behind CurrentPgBackend/Session/Execution --
		 * including PrivateRefCountArray).  Save them before parking and restore
		 * after, exactly as xtc_pg_wait_fd() does for the waiteventset park;
		 * without this a resumed fiber reads a sibling's PrivateRefCountArray and
		 * BufferLockAcquire()/GetPrivateRefCountEntry() dereference a bogus entry
		 * -> SIGSEGV (and short/garbage reads) under concurrent fiber IO.
		 */
		{
			PgCurrentWorkSnapshot snap;

			PgRuntimeSaveCurrentWork(&snap);

			switch ((PgAioOp) ioh->op)
			{
				case PGAIO_OP_READV:
					pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
					result = xtc_aio_preadv(ioh->op_data.read.fd, iov,
											ioh->op_data.read.iov_length,
											(int64) ioh->op_data.read.offset);
					pgstat_report_wait_end();
					break;
				case PGAIO_OP_WRITEV:
					pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
					result = xtc_aio_pwritev(ioh->op_data.write.fd, iov,
											 ioh->op_data.write.iov_length,
											 (int64) ioh->op_data.write.offset);
					pgstat_report_wait_end();
					break;
				case PGAIO_OP_INVALID:
					PgRuntimeRestoreCurrentWork(&snap);
					elog(ERROR, "trying to execute invalid IO operation");
			}

			PgRuntimeRestoreCurrentWork(&snap);
		}

		/*
		 * xtc_aio_p{read,write}v returns the total byte count (>= 0) or a
		 * negative errno -- the same convention pg_preadv/pg_pwritev report,
		 * so store it directly and complete the handle in-line (Invariant A:
		 * COMPLETED before submit() returns).
		 */
		ioh->result = result;
		pgaio_io_process_completion(ioh, ioh->result);
	}

	return num_staged_ios;
}

#endif							/* USE_XTC_CARRIER */
