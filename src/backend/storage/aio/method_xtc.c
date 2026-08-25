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

#ifndef WIN32
#include <sys/uio.h>					/* preadv2 / RWF_NOWAIT (Linux glibc) */
#endif

/*
 * Cached-read fast path: avoid a fiber park+resume (and thus a futex wait/wake
 * round trip) for a data-file read the OS page cache can satisfy immediately.
 * OLTP issues many tiny, mostly-cached reads per transaction; parking the fiber
 * for each one drives the carrier scheduler's wake path (F1 counters, F2 queue
 * notify, re-lease) into a futex storm under high concurrency -- measured at
 * 36% __x64_sys_futex with 192 VUs, mt collapsing to ~1% of fork.
 * preadv2(RWF_NOWAIT) returns the data with NO park when it is already cached,
 * and -EAGAIN when the read WOULD block -- only then do we fall through to the
 * async park path, where yielding the carrier is actually worth the wake cost.
 */
#if defined(RWF_NOWAIT)
#define XTC_AIO_HAVE_NOWAIT 1
#endif

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

#ifdef XTC_AIO_HAVE_NOWAIT
		/*
		 * Cached-read fast path.  For a READV, first try preadv2(RWF_NOWAIT):
		 * a plain synchronous syscall that returns immediately from the OS page
		 * cache when the data is resident, WITHOUT parking the fiber (so no
		 * carrier-scheduler wake, no futex).  This is a fiber-transparent op --
		 * no fiber switch, so no current-work save/restore is needed.  If it
		 * fully satisfies the read, complete the handle in-line and move on.
		 * On -EAGAIN (would block) or a partial read, fall through to the async
		 * park path below, which is where yielding the carrier actually pays.
		 */
		if ((PgAioOp) ioh->op == PGAIO_OP_READV)
		{
			ssize_t		want = 0;
			ssize_t		got;

			for (int k = 0; k < ioh->op_data.read.iov_length; k++)
				want += iov[k].iov_len;

			got = preadv2(ioh->op_data.read.fd, iov,
						  ioh->op_data.read.iov_length,
						  (int64) ioh->op_data.read.offset, RWF_NOWAIT);
			if (got == want)
			{
				/* fully served from cache -- no park, no futex */
				ioh->result = (int) got;
				pgaio_io_process_completion(ioh, ioh->result);
				continue;
			}
			/* EAGAIN / short / error -> fall through to the park path */
		}

		/*
		 * Non-blocking-write fast path.  For a WRITEV, try pwritev2(RWF_NOWAIT):
		 * when the write can be absorbed immediately (dirtying page-cache pages
		 * with no synchronous stable-storage pressure) it returns the byte count
		 * with NO fiber park -- avoiding the per-write carrier-scheduler wake
		 * that made io=xtc regress writes ~36% vs io=sync.  Only when the kernel
		 * would block (RWF_NOWAIT -> -EAGAIN, e.g. under dirty-page/O_DIRECT
		 * backpressure) do we fall through to the async park path, where
		 * yielding the carrier is worth the wake.  Mirrors the read fast path;
		 * fiber-transparent (no switch), so no current-work save/restore.
		 *
		 * Kernel-behavior caveat: buffered (non-O_DIRECT) pwritev2(RWF_NOWAIT)
		 * landed later and is less uniform than the read side -- on some kernels
		 * a buffered write may ALWAYS return -EAGAIN (never taking this fast
		 * path) rather than buffering.  That is not a correctness issue (we fall
		 * through to the parking xtc_aio_pwritev, which re-issues the FULL write
		 * from the original offset -- idempotent positional I/O, no double-write
		 * or torn durable state; durability is still deferred to fsync exactly as
		 * io=sync), but it means the write win may silently not materialize on
		 * such kernels.  Confirm the fast path actually fires (A/B) on the target
		 * kernel before relying on it.
		 */
		if ((PgAioOp) ioh->op == PGAIO_OP_WRITEV)
		{
			ssize_t		want = 0;
			ssize_t		put;

			for (int k = 0; k < ioh->op_data.write.iov_length; k++)
				want += iov[k].iov_len;

			put = pwritev2(ioh->op_data.write.fd, iov,
						   ioh->op_data.write.iov_length,
						   (int64) ioh->op_data.write.offset, RWF_NOWAIT);
			if (put == want)
			{
				/* fully absorbed without blocking -- no park, no futex */
				ioh->result = (int) put;
				pgaio_io_process_completion(ioh, ioh->result);
				continue;
			}
			/* EAGAIN / short / error -> fall through to the park path */
		}
#endif

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
			XtcPgVerifyCurrentWorkIsSelf();
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
