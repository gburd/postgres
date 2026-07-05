/*-------------------------------------------------------------------------
 *
 * method_xtc.c
 *    AIO - route data-file IO through libxtc's async file path when the
 *    issuing backend runs as an xtc fiber (USE_XTC_CARRIER).
 *
 * This is a "wrap" at the IoMethodOps vtable seam (item #6 of AGENTS_XTC.md;
 * see plan_docs/XTC_AIO_DESIGN.md).  It does not change any pgaio_io_* call
 * site.  When the issuer is an xtc backend fiber, xtc_aio_pread/pwrite run the
 * IO on the xtc loop (parking the fiber, not blocking the carrier thread) and
 * return the byte count or a negative errno -- the same convention pg_preadv
 * uses.  The IO is completed by the issuer before submit() returns, exactly
 * like method_sync, so there is nothing left in flight and no reordering.
 *
 * Off a fiber (process mode, aux backends, or any backend not on a carrier
 * loop) needs_synchronous_execution() returns true, so the executor uses the
 * existing synchronous fallback (pgaio_io_perform_synchronously()) and this
 * method's submit() is never reached -- process mode is byte-for-byte
 * unchanged.
 *
 * Step 1 scope (smallest diff): single-iovec READV/WRITEV only.  Multi-element
 * iovecs are pushed to the synchronous fallback via
 * needs_synchronous_execution().  Multi-iovec support is Step 2; issuer-async
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
#include "utils/wait_event.h"

#include "xtc_aio.h"					/* xtc_aio_pread / xtc_aio_pwrite */

static bool pgaio_xtc_needs_synchronous_execution(PgAioHandle *ioh);
static int	pgaio_xtc_submit(uint16 num_staged_ios, PgAioHandle **staged_ios);

const IoMethodOps pgaio_xtc_ops = {
	.needs_synchronous_execution = pgaio_xtc_needs_synchronous_execution,
	.submit = pgaio_xtc_submit,
};

/*
 * The xtc method only handles an IO when the issuer is running as an xtc
 * backend fiber AND the IO is a single-iovec READV/WRITEV.  Everything else
 * falls back to the synchronous method, which preserves the existing behavior
 * exactly (process mode, aux backends, multi-iovec, invalid ops).
 */
static bool
pgaio_xtc_needs_synchronous_execution(PgAioHandle *ioh)
{
	if (!xtc_in_backend_fiber)
		return true;

	switch ((PgAioOp) ioh->op)
	{
		case PGAIO_OP_READV:
			return ioh->op_data.read.iov_length != 1;
		case PGAIO_OP_WRITEV:
			return ioh->op_data.write.iov_length != 1;
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
		 * needs_synchronous_execution() guarantees we only see single-iovec
		 * READV/WRITEV issued from a backend fiber.
		 */
		Assert(xtc_in_backend_fiber);

		switch ((PgAioOp) ioh->op)
		{
			case PGAIO_OP_READV:
				Assert(ioh->op_data.read.iov_length == 1);
				pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
				result = xtc_aio_pread(ioh->op_data.read.fd,
									   iov[0].iov_base,
									   (uint32) iov[0].iov_len,
									   (int64) ioh->op_data.read.offset);
				pgstat_report_wait_end();
				break;
			case PGAIO_OP_WRITEV:
				Assert(ioh->op_data.write.iov_length == 1);
				pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
				result = xtc_aio_pwrite(ioh->op_data.write.fd,
										iov[0].iov_base,
										(uint32) iov[0].iov_len,
										(int64) ioh->op_data.write.offset);
				pgstat_report_wait_end();
				break;
			case PGAIO_OP_INVALID:
				elog(ERROR, "trying to execute invalid IO operation");
		}

		/*
		 * xtc_aio_* returns the byte count (>= 0) or a negative errno -- the
		 * same convention pg_preadv/pg_pwritev report via
		 * (result < 0 ? -errno : result), so store it directly.  Assert the
		 * handle is completed before we return (Invariant A from the design).
		 */
		ioh->result = result;
		pgaio_io_process_completion(ioh, ioh->result);
	}

	return num_staged_ios;
}

#endif							/* USE_XTC_CARRIER */
