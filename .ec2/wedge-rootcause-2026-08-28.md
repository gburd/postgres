# ROOT CAUSE: pooled-carrier write wedge (XTC wedges where stock fork does not)

Date 2026-08-28 (mala). Debug build (symbols), libxtc v1.39 + xtc_loop_wake fix.
Repro: threaded pooled (carriers=-1 auto=32), pgbench scale-100, 64-client TPC-B
write load, fsync=on. Wedge in <8s: `select 1` hangs, CPU ~74% then pinned, never
recovers. Full all-thread gdb backtrace captured (/mnt/work/wr/bt.txt).

## The mechanism (from the symbolized backtrace)
Carrier client-backend stacks at the wedge:
  - HOLDER:  fdatasync > pg_fdatasync > issue_xlog_fsync > XLogWrite > XLogFlush >
             RecordTransactionCommit > CommitTransaction > ... > exec_simple_query
  - WAITERS: __futex_abstimed_wait > PGSemaphoreLock > ProcSemaphoreWaitCallback >
             LWLockAcquireOrWait > XLogFlush > RecordTransactionCommit > CommitTransaction
  - others in the protocol-read park (SocketBackendStickyIdleWait) / socket_flush.

## Root cause
The pooled carrier is a RAW PTHREAD (pg_thread_create, launch_backend.c:1372) that
runs many client sessions INLINE in a for(;;) loop
(PgSessionRunProtocolSchedulerUntilBoundary).  carrier_entry NEVER sets
xtc_in_backend_fiber (it stays false on the carrier pthread).  So at commit,
XLogFlush takes WALWriteLock and calls issue_xlog_fsync -> pg_fdatasync, which -- because
xtc_in_backend_fiber is FALSE -- takes the PLAIN BLOCKING fdatasync() path (the
xtc_aio_fdatasync fiber-park branch is skipped).

That blocking fdatasync freezes the carrier PTHREAD for the whole disk sync WHILE it
holds WALWriteLock.  Consequences, compounding under concurrency:
  1. All other committers block in LWLockAcquireOrWait(WALWriteLock) -> park on sems.
  2. EVERY OTHER SESSION multiplexed onto that same carrier pthread is frozen too (the
     carrier can't return to its for(;;) loop to serve them) -- so the pool loses
     effective workers exactly when commits pile up.
  3. Group-commit followers can't reach their commit point (their carriers are stuck),
     so the normal "one flush serves many" amortization breaks down.
At 64 write clients this starves/serializes the whole pool -> wedge.

## Why stock fork does NOT wedge
Each backend is an independent OS process.  A blocking fsync-under-WALWriteLock blocks
only that one process; the kernel schedules the other backends, and group commit means
the lock holder fsyncs for the whole cohort.  There is no "N sessions share one thread"
coupling, so one blocking syscall never freezes a cohort of sessions.  This is a
genuine PG/XTC bug: the pooled model multiplexes many sessions onto one pthread, and a
blocking syscall (fsync) on that pthread freezes all of them.

## The fix (design; implementation is a focused next task)
The carrier pthread must NOT block inline on WAL fsync while holding WALWriteLock and
starving its sibling sessions.  Options, best-first:
 A. OFFLOAD the WAL fsync off the carrier pthread so it can keep serving sibling
    sessions: run issue_xlog_fsync's pg_fdatasync via a dedicated fsync worker
    thread/pool (xtc_blocking_run with a real loop/pool context), returning the carrier
    to its loop while the sync is in flight; resume the committer on completion.
    Requires giving the carrier a libxtc loop/task context (today it is a bare pthread,
    so xtc_aio_* / xtc_blocking_run fall back to INLINE blocking -- aio.c aio_offload:
    "off a loop: run inline").
 B. Run pooled sessions as REAL libxtc fibers on an exec loop (not inline on a bare
    pthread), so pg_fdatasync's xtc_aio_fdatasync path parks the fiber and the loop
    serves siblings during the sync.  Bigger change; the "right" long-term shape.
 C. Shorten/again-amortize WALWriteLock: a carrier-aware group-commit that yields
    other sessions while the designated flusher syncs.
Any fix must keep process mode byte-for-byte and be A/B-gated (neutral-or-better on
read-S/CPU).  This is THE write-path blocker to the "outright win" bar.

## Note
This is distinct from (and beyond) the xtc_loop_wake nudge already landed (that fixed a
lost-wakeup for parked idle sessions).  This wedge is a blocking-syscall-freezes-the-
carrier problem inherent to inline session multiplexing on a bare pthread.
