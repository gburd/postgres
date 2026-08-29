# libxtc follow-up: v1.40.1 fixed the single-fiber strand, but a WALWriteLock-holder fiber is STILL stranded (unresumed) under CONCURRENT commit load

Date: 2026-08-29
libxtc: v1.40.1 (commit ebc430c) -- the idle-poll-dispatch fix. Confirmed compiled in.
Consumer: PostgreSQL "xtc" branch, sessions-as-fibers (pooled_protocol_carriers=0).
Re: your v1.40.1 reply, which said: "If you still see a wedge on v1.40.1 with a fiber
(not a bare pthread) suspended after its completion, capture the gdb (all loops in
xtc_io_poll with the completed fiber off-thread) plus which loop owns the fiber's park_fd,
and send it." Here it is.

--------------------------------------------------------------------------------
## v1.40.1 confirmed-fixed (thank you)
- Forced-offload (XTC_AIO_FORCE_OFFLOAD=1) scale-100 init: 100% wedge on v1.40.0 -> now
  completes.
- Native io_uring scale-100 init: 1/8 wedge on v1.40.0 -> 0/8 on v1.40.1.
The single-connection, single-parked-fiber strand is gone.

## But a DIFFERENT (or incompletely-fixed) resume wedge remains under CONCURRENT commit
Workload: pgbench -c 64 TPC-B write load (fsync=on, synchronous_commit=on), 32-vCPU box,
sessions-as-fibers on a 37-loop exec pool with migratable client fibers.
Symptom: pgbench runs ~1029 tps for ~2 s, then collapses to 0.0 tps FOREVER. Server-side
commits freeze (delta ~2 commits / 4 s). `select 1` from a fresh conn mostly recovers
(single-probe blips), so it is not a whole-runtime hang -- it is the COMMIT path that
starves.

### gdb at the freeze (the signature you asked for)
- WALWriteLock: p *(LWLock*)0x...b80 -> {tranche=8, state.value=0x80042000, waiters=
  {head=21,tail=236}}.  Bit 31 (LW_VAL_EXCLUSIVE) SET -> the lock is HELD EXCLUSIVE.
- The HOLDER is a client backend FIBER that is on NO OS thread and in NO frame: the whole
  backtrace has BackendMain=0, exec_simple_query=0, XLogWrite=0, issue_xlog_fsync=0,
  xtc_aio=0, blk_worker=0.  All 64 client fibers are libxtc-parked (invisible to gdb);
  one of them holds WALWriteLock and is never resumed.
- 31 of the 37 carrier loops are idle in xtc_io_poll / __xtc_loop_step_once /
  __xtc_exec_worker.
- Witness: the WAL writer fiber is blocked in
    xtc_carrier_proc -> WalWriterMain -> XLogBackgroundFlush ->
    LWLockAcquire(WALWriteLock, EXCLUSIVE) -> ProcSemaphoreWaitCallback -> PGSemaphoreLock
  waiting for the held lock.  (This one takes the blocking-sem path since it is a plain
  LWLockAcquire, not LWLockAcquireOrWait; it is only a witness, not the bug.)
- In a sibling snapshot the two client fibers that HAD reached commit were parked in
    exec_simple_query("END;") -> CommitTransaction -> XLogFlush ->
    LWLockAcquireOrWait(WALWriteLock, EXCLUSIVE) -> ProcSemaphoreWaitFiber ->
    xtc_pg_wait_fd(sem_wake_fd, READABLE, -1)
  i.e. parked on their per-proc eventfd waiting for the holder to flush; the holder never
  resumes, so they never wake.

So: a fiber that acquired WALWriteLock EXCLUSIVE (the flusher, mid-XLogWrite) parked and
was not resumed, all loops went idle, and every other committer is stuck behind it.
Identical END STATE to the v1.40.0 bug (stranded WALWriteLock holder, all loops idle in
xtc_io_poll), but it survives the v1.40.1 idle-poll-dispatch fix and appears only under
CONCURRENT commit (many migratable fibers), not the single-connection init.

## Why we think this is still a libxtc resume-side issue
- PG parks exactly as before (xtc_aio_fdatasync for the flush; xtc_pg_wait_fd on the
  sem_wake_fd for the LWLockAcquireOrWait waiters) and does nothing after parking. The
  holder's resume is libxtc's.
- The holder is off-thread with the lock held and ALL loops idle -- the same "completion
  reaped-and-dropped / task not re-enqueued" shape your v1.40.1 fix addressed for the idle
  poll, suggesting either (a) a SECOND poll/dispatch site still drops a completion under
  concurrency (e.g. the fairness-quantum poll, or a steal/rebalance transition), or (b)
  the idle-poll fix has a residual race when MANY migratable fibers are being stolen
  across loops at once (your root cause was a migratable fiber stolen to a peer loop; at
  64 concurrent fibers with eager rebalance the steal churn is high).
- It is concurrency-gated: single-fiber init is now 0/8; 64-fiber commit collapses after
  the first ~2 s burst every time.

## Hypotheses for you to check (in priority order)
1. Under high steal churn, can a completion for a fiber be reaped by a loop that is
   between __xtc_loop_step_once (idle) and the idle poll, or during a rebalance that
   moves the fiber's park registration, such that neither the losing nor gaining loop
   dispatches it?
2. Are there OTHER xtc_io_poll call sites (fairness quantum; a rebalance drain) that, like
   the pre-1.40.1 idle poll, reap events without calling __xtc_loop_dispatch_event?
3. Does a migratable fiber that parks on an eventfd (xtc_pg_wait_fd, our sem_wake_fd)
   registered on loop X, then gets its wake (foreign-thread write + xtc_proc_wake) while X
   is idle, reliably resume post-1.40.1? The LWLockAcquireOrWait waiters use this exact
   path and are among the stuck fibers.

## Reproduction
1. PG "xtc" branch on libxtc v1.40.1, multithreaded=on, pooled_protocol_carriers=0,
   fsync=on, synchronous_commit=on, PGDATA on local NVMe (xfs), shared_buffers ~40% RAM,
   32-vCPU (c6id.8xlarge).
2. pgbench -i -s 100 (now succeeds), then pgbench -c 64 -j 16 -T 60 -P 5.
3. Watch the -P 5 progress: ~1000 tps for the first 2 s, then 0.0 tps forever. gdb
   "thread apply all bt": WALWriteLock held exclusive by an off-thread fiber, WAL writer
   fiber blocked on it, all loops idle in xtc_io_poll, zero client backends running.
We can share bt_w64d.txt / bt_w64b.txt and a scripted repro. If you want the owning loop
of the holder's park_fd we can add targeted gdb (we could not enumerate libxtc's parked
procs from gdb without symbols for your proc table -- if you have a gdb helper for that,
send it and we'll capture the exact loop + park_fd).

## Not the issue
- Not the inline-blocking stackless pooled path (this is the fiber path).
- Not a PG post-park action (PG does nothing after parking).
- The v1.40.1 fix IS in and IS effective for the single-fiber case; this is the remaining
  concurrent-commit case.
