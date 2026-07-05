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
 * Step 2 scope: single- AND multi-iovec READV/WRITEV.  libxtc v1.1.0 exposes
 * only single-buffer xtc_aio_pread/pwrite (no readv/writev), so a multi-element
 * iovec is executed as a loop of per-element xtc_aio calls at increasing file
 * offsets -- equivalent to pg_preadv/pg_pwritev for the contiguous file region
 * PostgreSQL AIO describes.  Short-transfer and error semantics mirror the
 * synchronous method: accumulate transferred bytes, stop at the first short or
 * erroring element, and report a leading error as -errno.  Issuer-async reap
 * is Step 3; WAL/fsync is Step 4 (see the design doc).
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
 * Run one READV/WRITEV handle as a sequence of single-buffer xtc_aio calls
 * over its iovec, at increasing file offsets, and return the pg_preadv-style
 * result (total bytes transferred, or a negative errno if the first element
 * failed).  Stops at the first short or erroring element, matching the
 * synchronous method's partial-transfer semantics.
 */
static int
pgaio_xtc_run_vectored(PgAioHandle *ioh)
{
	struct iovec *iov = &pgaio_ctl->iovecs[ioh->iovec_off];
	bool		is_read = ((PgAioOp) ioh->op == PGAIO_OP_READV);
	int			fd = is_read ? ioh->op_data.read.fd : ioh->op_data.write.fd;
	int			iovcnt = is_read ? ioh->op_data.read.iov_length
								 : ioh->op_data.write.iov_length;
	int64		base_off = is_read ? (int64) ioh->op_data.read.offset
								 : (int64) ioh->op_data.write.offset;
	int64		done = 0;

	for (int k = 0; k < iovcnt; k++)
	{
		uint32		len = (uint32) iov[k].iov_len;
		int			r;

		if (len == 0)
			continue;

		if (is_read)
			r = xtc_aio_pread(fd, iov[k].iov_base, len, base_off + done);
		else
			r = xtc_aio_pwrite(fd, iov[k].iov_base, len, base_off + done);

		if (r < 0)
			return (done > 0) ? (int) done : r;	/* leading error -> -errno */

		done += r;
		if ((uint32) r < len)
			break;				/* short transfer: stop, like pg_preadv */
	}

	return (int) done;
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
		int			result;
		uint32		wait_event;

		/*
		 * needs_synchronous_execution() guarantees we only see READV/WRITEV
		 * issued from a backend fiber.
		 */
		Assert(xtc_in_backend_fiber);
		Assert((PgAioOp) ioh->op == PGAIO_OP_READV ||
			   (PgAioOp) ioh->op == PGAIO_OP_WRITEV);

		/*
		 * Advance the handle to SUBMITTED before running the IO, exactly like
		 * every other method's submit().  Skipping this leaves the handle IDLE
		 * and pgaio_io_process_completion() PANICs ("waiting for own IO in
		 * wrong state: IDLE").
		 */
		pgaio_io_prepare_submit(ioh);

		wait_event = ((PgAioOp) ioh->op == PGAIO_OP_READV)
			? WAIT_EVENT_DATA_FILE_READ : WAIT_EVENT_DATA_FILE_WRITE;
		pgstat_report_wait_start(wait_event);
		result = pgaio_xtc_run_vectored(ioh);
		pgstat_report_wait_end();

		/*
		 * xtc_aio_* returns the byte count (>= 0) or a negative errno -- the
		 * same convention pg_preadv/pg_pwritev report, so store it directly
		 * and complete the handle in-line (Invariant A: COMPLETED before
		 * submit() returns).
		 */
		ioh->result = result;
		pgaio_io_process_completion(ioh, ioh->result);
	}

	return num_staged_ios;
}

#endif							/* USE_XTC_CARRIER */
